package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/prometheus/client_golang/prometheus"
)

// compile passing -ldflags "-X main.Build <build sha1>"
var Build string

const (
	VERSION = "0.01"
	NAME    = "lmd"
)

type Connection struct {
	Name       string
	Id         string
	Source     []string
	Auth       string
	RemoteName string
}

type Config struct {
	Listen           []string
	Updateinterval   int
	Connections      []Connection
	LogFile          string
	LogLevel         string
	NetTimeout       int
	ListenPrometheus string
}

var DataStore map[string]Peer
var DataStoreOrder []string

type configFiles []string

func (c *configFiles) String() string {
	return fmt.Sprintf("%s", *c)
}

func (c *configFiles) Set(value string) error {
	*c = append(*c, value)
	return nil
}

var GlobalConfig Config
var flagVerbose bool
var flagVeryVerbose bool
var flagTraceVerbose bool
var flagConfigFile configFiles
var flagVersion bool
var flagLogFile string
var flagPidfile string

var once sync.Once

func main() {
	flag.Var(&flagConfigFile, "c", "set location for config file, can be specified multiple times")
	flag.Var(&flagConfigFile, "config", "set location for config file, can be specified multiple times")
	flag.StringVar(&flagPidfile, "pidfile", "", "set path to pidfile")
	flag.StringVar(&flagLogFile, "logfile", "", "override logfile from the configuration file")
	flag.BoolVar(&flagVerbose, "v", false, "enable verbose output")
	flag.BoolVar(&flagVerbose, "verbose", false, "enable verbose output")
	flag.BoolVar(&flagVeryVerbose, "vv", false, "enable very verbose output")
	flag.BoolVar(&flagTraceVerbose, "vvv", false, "enable trace output")
	flag.BoolVar(&flagVersion, "version", false, "print version and exit")
	flag.Parse()
	if flagVersion {
		fmt.Printf("%s - version %s (Build: %s)\n", NAME, VERSION, Build)
		os.Exit(2)
	}

	if len(flagConfigFile) == 0 {
		fmt.Print("ERROR: no config files specified.\nSee --help for all options.\n")
		os.Exit(2)
	}

	if flagPidfile != "" {
		if _, err := os.Stat(flagPidfile); err == nil {
			dat, err := ioutil.ReadFile(flagPidfile)
			pid, err := strconv.ParseInt(strings.TrimSpace(string(dat)), 10, 64)
			process, err := os.FindProcess(int(pid))
			if err == nil {
				err = process.Signal(syscall.Signal(0))
				if err == nil {
					fmt.Fprintf(os.Stderr, "ERROR: pidfile '%s' does already exist (and process %d is still running)\n", flagPidfile, pid)
					os.Exit(2)
				}
			}
			fmt.Fprintf(os.Stderr, "WARNING: removing stale pidfile '%s'\n", flagPidfile)
			os.Remove(flagPidfile)
		}
	}

	// check for config errors
	GlobalConfig = ReadConfig(flagConfigFile)

	// write pidfile
	if flagPidfile != "" {
		err := ioutil.WriteFile(flagPidfile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0640)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: could not write pidfile '%s': %s\n", flagPidfile, err.Error())
			os.Exit(2)
		}
	}

	http.Handle("/metrics", prometheus.Handler())

	for {
		mainLoop()
	}
}

func mainLoop() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, os.Interrupt)
	shutdownChannel := make(chan bool)
	waitGroupListener := &sync.WaitGroup{}
	waitGroupPeers := &sync.WaitGroup{}

	GlobalConfig = ReadConfig(flagConfigFile)

	if flagVerbose {
		GlobalConfig.LogLevel = "Info"
	}
	if flagVeryVerbose {
		GlobalConfig.LogLevel = "Debug"
	}
	if flagTraceVerbose {
		GlobalConfig.LogLevel = "Trace"
	}
	setDefaults(&GlobalConfig)
	InitLogging(&GlobalConfig)

	if len(GlobalConfig.Connections) == 0 {
		log.Fatalf("no connections defined")
	}

	// Set the backends to be used.
	DataStore = make(map[string]Peer)
	InitObjects()

	prometheusListener := InitPrometheus()

	// start local listeners
	for _, listen := range GlobalConfig.Listen {
		go localListener(listen, waitGroupListener, shutdownChannel)
	}

	// start remote connections
	for _, c := range GlobalConfig.Connections {
		p := NewPeer(c, waitGroupPeers, shutdownChannel)
		_, Exists := DataStore[c.Id]
		if Exists {
			log.Fatalf("Duplicate id in connection list: %s", c.Id)
		}
		DataStore[c.Id] = *p
		p.Start()
		DataStoreOrder = append(DataStoreOrder, c.Id)
	}

	once.Do(PrintVersion)

	// just wait till someone hits ctrl+c or we have to reload
	for sig := range c {
		switch sig {
		case syscall.SIGTERM:
			log.Infof("got sigterm, quiting gracefully")
			shutdownChannel <- true
			close(shutdownChannel)
			if prometheusListener != nil {
				prometheusListener.Close()
			}
			waitGroupListener.Wait()
			waitGroupPeers.Wait()
			if flagPidfile != "" {
				os.Remove(flagPidfile)
			}
			os.Exit(0)
			break
		case os.Interrupt:
			shutdownChannel <- true
			close(shutdownChannel)
			if prometheusListener != nil {
				prometheusListener.Close()
			}
			log.Infof("got sigint, quiting")
			// wait one second which should be enough for the listeners
			waitTimeout(waitGroupListener, time.Second)
			if flagPidfile != "" {
				os.Remove(flagPidfile)
			}
			os.Exit(1)
			break
		case syscall.SIGHUP:
			log.Infof("got sighup, reloading configuration...")
			shutdownChannel <- true
			close(shutdownChannel)
			if prometheusListener != nil {
				prometheusListener.Close()
			}
			waitGroupListener.Wait()
			return
		default:
			log.Warnf("Signal not handled: %v", sig)
		}
	}
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func setDefaults(conf *Config) {
	if conf.NetTimeout <= 0 {
		conf.NetTimeout = 30
	}
	if conf.Updateinterval <= 0 {
		conf.Updateinterval = 2
	}
}

func PrintVersion() {
	fmt.Printf("%s - version %s (Build: %s) started with config %s\n", NAME, VERSION, Build, flagConfigFile)
}

func ReadConfig(files []string) (conf Config) {
	for _, configFile := range files {
		if _, err := os.Stat(configFile); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: could not load configuration from %s: %s\nuse --help to see all options.\n", configFile, err.Error())
			os.Exit(3)
		}
		if _, err := toml.DecodeFile(configFile, &conf); err != nil {
			panic(err)
		}
	}
	if flagLogFile != "" {
		conf.LogFile = flagLogFile
	}

	promPeerUpdateInterval.Set(float64(conf.Updateinterval))

	return
}
