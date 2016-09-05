package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

// compile passing -ldflags "-X main.Build <build sha1>"
var Build string

const (
	VERSION = "0.1"
	NAME    = "lampd"
)

type Connection struct {
	Name   string
	Id     string
	Source []string
}

type Config struct {
	Listen         []string
	Updateinterval int
	Connections    []Connection
	LogFile        string
	LogLevel       string
	NetTimeout     int
}

var DataStore map[string]Peer

var GlobalConfig Config
var flagVerbose bool
var flagVeryVerbose bool
var flagTraceVerbose bool
var flagConfigFile string
var flagVersion bool

func main() {
	flag.StringVar(&flagConfigFile, "c", "lampd.ini", "set location for config file")
	flag.StringVar(&flagConfigFile, "config", "lampd.ini", "set location for config file")
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

	if _, err := os.Stat(flagConfigFile); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: could not load configuration from %s: %s\nuse --help to see all options.\n", flagConfigFile, err)
		os.Exit(3)
	}
	if _, err := toml.DecodeFile(flagConfigFile, &GlobalConfig); err != nil {
		panic(err)
	}
	if flagVerbose {
		GlobalConfig.LogLevel = "Info"
	}
	if flagVeryVerbose {
		GlobalConfig.LogLevel = "Debug"
	}
	if flagTraceVerbose {
		GlobalConfig.LogLevel = "Trace"
	}
	InitLogging(&GlobalConfig)

	// Set the backends to be used.
	DataStore = make(map[string]Peer)
	InitObjects()

	// start local listeners
	for _, listen := range GlobalConfig.Listen {
		go localListener(listen, waitGroupListener, shutdownChannel)
	}

	// start remote connections
	for _, c := range GlobalConfig.Connections {
		p := NewPeer(&c, waitGroupPeers, shutdownChannel)
		_, Exists := DataStore[c.Id]
		if Exists {
			log.Fatalf("Duplicate id in connection list: %s", c.Id)
		}
		DataStore[c.Id] = *p
		p.Start()
	}

	fmt.Printf("%s - version %s (Build: %s) started\n", NAME, VERSION, Build)

	// just wait till someone hits ctrl+c or we have to reload
	for sig := range c {
		switch sig {
		case syscall.SIGTERM:
			log.Infof("got sigterm, quiting gracefully")
			shutdownChannel <- true
			close(shutdownChannel)
			waitGroupListener.Wait()
			waitGroupPeers.Wait()
			os.Exit(0)
			break
		case os.Interrupt:
			shutdownChannel <- true
			close(shutdownChannel)
			log.Infof("got sigint, quiting")
			// wait one second which should be enough for the listeners
			waitTimeout(waitGroupListener, time.Second)
			os.Exit(1)
			break
		case syscall.SIGHUP:
			log.Infof("got sighub, reloading configuration...")
			shutdownChannel <- true
			close(shutdownChannel)
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
