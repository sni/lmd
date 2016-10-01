/*
LMD - Livestatus Multitool daemon
A livestatus in/out converter and caching daemon.

Help

Use `lmd -h` to get help on command line parameters.
*/
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/prometheus/client_golang/prometheus"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

const (
	// VERSION contains the actual lmd version
	VERSION = "0.0.3"
	// NAME defines the name of this project
	NAME = "lmd"
)

// Connection defines a single connection configuration.
type Connection struct {
	Name       string
	ID         string
	Source     []string
	Auth       string
	RemoteName string
}

// Config defines the available configuration options from supplied config files.
type Config struct {
	Listen              []string
	Updateinterval      int64
	FullUpdateInterval  int64
	Connections         []Connection
	LogFile             string
	LogLevel            string
	NetTimeout          int
	ListenPrometheus    string
	SkipSSLCheck        int
	IdleTimeout         int64
	IdleInterval        int64
	StaleBackendTimeout int
}

// DataStore contains a map of available remote peers.
var DataStore map[string]Peer

// DataStoreOrder contains the order of all remote peers as defined in the supplied config files.
var DataStoreOrder []string

type configFiles []string

// String returns the config files list as string.
func (c *configFiles) String() string {
	return fmt.Sprintf("%s", *c)
}

// Set appends a config file to the list of config files.
func (c *configFiles) Set(value string) error {
	*c = append(*c, value)
	return nil
}

// GlobalConfig contains the global configuration (after config files have been parsed)
var GlobalConfig Config
var flagVerbose bool
var flagVeryVerbose bool
var flagTraceVerbose bool
var flagConfigFile configFiles
var flagVersion bool
var flagLogFile string
var flagPidfile string
var flagProfile string

var once sync.Once
var netClient *http.Client
var mainSignalChannel chan os.Signal

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
	flag.StringVar(&flagProfile, "profiler", "", "start pprof profiler on this port, ex. :6060")
	flag.Parse()
	if flagVersion {
		fmt.Printf("%s - version %s (Build: %s)\n", NAME, VERSION, Build)
		os.Exit(2)
	}

	if flagProfile != "" {
		runtime.SetBlockProfileRate(10)
		go func() {
			http.ListenAndServe(flagProfile, http.DefaultServeMux)
		}()
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

	mainSignalChannel = make(chan os.Signal)
	for {
		exitCode := mainLoop(mainSignalChannel)
		if exitCode > 0 {
			os.Exit(exitCode)
		}
		if exitCode == 0 {
			break
		}
	}
}

func mainLoop(mainSignalChannel chan os.Signal) (exitCode int) {
	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, syscall.SIGHUP)
	signal.Notify(osSignalChannel, syscall.SIGTERM)
	signal.Notify(osSignalChannel, os.Interrupt)

	shutdownChannel := make(chan bool)
	waitGroupListener := &sync.WaitGroup{}
	waitGroupPeers := &sync.WaitGroup{}

	GlobalConfig = ReadConfig(flagConfigFile)

	setVerboseFlags(&GlobalConfig)
	setDefaults(&GlobalConfig)
	InitLogging(&GlobalConfig)

	if len(GlobalConfig.Connections) == 0 {
		log.Fatalf("no connections defined")
	}

	if flagProfile != "" {
		log.Warnf("pprof profiler listening at %s", flagProfile)
	}

	// Set the backends to be used.
	DataStore = make(map[string]Peer)
	DataStoreOrder = make([]string, 0)
	InitObjects()

	// initialize prometheus
	prometheusListener := initPrometheus()

	// initialize http client
	initializeHTTPClient()

	// start local listeners
	for _, listen := range GlobalConfig.Listen {
		go LocalListener(listen, waitGroupListener, shutdownChannel)
	}

	// start remote connections
	initializePeers(&GlobalConfig, waitGroupPeers, waitGroupListener, shutdownChannel)

	once.Do(PrintVersion)

	// just wait till someone hits ctrl+c or we have to reload
	select {
	case sig := <-osSignalChannel:
		return mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener)
	case sig := <-mainSignalChannel:
		return mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener)
	}
}

func initializePeers(GlobalConfig *Config, waitGroupPeers *sync.WaitGroup, waitGroupListener *sync.WaitGroup, shutdownChannel chan bool) {
	for _, c := range GlobalConfig.Connections {
		p := NewPeer(c, waitGroupPeers, shutdownChannel)
		_, Exists := DataStore[c.ID]
		if Exists {
			log.Fatalf("Duplicate id in connection list: %s", c.ID)
		}
		DataStore[c.ID] = *p
		p.Start()
		DataStoreOrder = append(DataStoreOrder, c.ID)
	}
}

func setVerboseFlags(GlobalConfig *Config) {
	if flagVerbose {
		GlobalConfig.LogLevel = "Info"
	}
	if flagVeryVerbose {
		GlobalConfig.LogLevel = "Debug"
	}
	if flagTraceVerbose {
		GlobalConfig.LogLevel = "Trace"
	}
}

func initializeHTTPClient() {
	insecure := false
	if GlobalConfig.SkipSSLCheck == 1 {
		insecure = true
	}
	tr := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: insecure},
	}
	netClient = &http.Client{
		Timeout:   time.Second * 30,
		Transport: tr,
	}
}

func mainSignalHandler(sig os.Signal, shutdownChannel chan bool, waitGroupPeers *sync.WaitGroup, waitGroupListener *sync.WaitGroup, prometheusListener net.Listener) (exitCode int) {
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
		return (0)
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
		return (1)
	case syscall.SIGHUP:
		log.Infof("got sighup, reloading configuration...")
		shutdownChannel <- true
		close(shutdownChannel)
		if prometheusListener != nil {
			prometheusListener.Close()
		}
		waitGroupListener.Wait()
		return (-1)
	default:
		log.Warnf("Signal not handled: %v", sig)
	}
	return (1)
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
		conf.Updateinterval = 5
	}
	if conf.FullUpdateInterval <= 0 {
		conf.FullUpdateInterval = 0
	}
	if conf.IdleInterval <= 0 {
		conf.IdleInterval = 1800
	}
	if conf.IdleTimeout <= 0 {
		conf.IdleTimeout = 120
	}
	if conf.StaleBackendTimeout <= 0 {
		conf.StaleBackendTimeout = 30
	}
}

// PrintVersion prints the version
func PrintVersion() {
	fmt.Printf("%s - version %s (Build: %s) started with config %s\n", NAME, VERSION, Build, flagConfigFile)
}

// ReadConfig reads all config files.
// It returns a Config object.
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
