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
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

const (
	// VERSION contains the actual lmd version
	VERSION = "1.0.2"
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

// Equals checks if two connection objects are identical.
func (c *Connection) Equals(other *Connection) bool {
	equal := c.ID == other.ID
	equal = equal && c.Name == other.Name
	equal = equal && c.Auth == other.Auth
	equal = equal && c.RemoteName == other.RemoteName
	equal = equal && strings.Join(c.Source, ":") == strings.Join(other.Source, ":")
	return equal
}

// Config defines the available configuration options from supplied config files.
type Config struct {
	Listen              []string
	Nodes               []string
	TLSCertificate      string
	TLSKey              string
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
var DataStore map[string]*Peer

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

// nodeAccessor manages cluster nodes and starts/stops peers.
var nodeAccessor *Nodes

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
var lastMainRestart = time.Now().Unix()

// initialize objects structure
func init() {
	InitObjects()
	mainSignalChannel = make(chan os.Signal)
	DataStore = make(map[string]*Peer)
	DataStoreOrder = make([]string, 0)
}

func setFlags() {
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
}

func main() {
	// command line arguments
	setFlags()
	checkFlags()

	// make sure we log panics properly
	defer logPanicExit()

	for {
		exitCode := mainLoop(mainSignalChannel)
		if exitCode > 0 {
			os.Exit(exitCode)
		}
		// make it possible to call main() from tests without exiting the tests
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

	lastMainRestart = time.Now().Unix()
	shutdownChannel := make(chan bool)
	waitGroupInit := &sync.WaitGroup{}
	waitGroupListener := &sync.WaitGroup{}
	waitGroupPeers := &sync.WaitGroup{}

	GlobalConfig = ReadConfig(flagConfigFile)

	setVerboseFlags(&GlobalConfig)
	setDefaults(&GlobalConfig)
	InitLogging(&GlobalConfig)

	if len(GlobalConfig.Connections) == 0 {
		log.Fatalf("no connections defined")
	}

	if len(GlobalConfig.Listen) == 0 {
		log.Fatalf("no listeners defined")
	}

	if flagProfile != "" {
		log.Warnf("pprof profiler listening at %s", flagProfile)
	}

	// initialize prometheus
	prometheusListener := initPrometheus()

	// initialize http client
	initializeHTTPClient()

	// start local listeners
	waitGroupInit.Add(len(GlobalConfig.Listen))
	for _, listen := range GlobalConfig.Listen {
		go func(listen string) {
			// make sure we log panics properly
			defer logPanicExit()
			LocalListener(listen, waitGroupInit, waitGroupListener, shutdownChannel)
		}(listen)
	}

	// start remote connections
	initializePeers(&GlobalConfig, waitGroupPeers, waitGroupInit, waitGroupListener, shutdownChannel)

	once.Do(PrintVersion)

	// just wait till someone hits ctrl+c or we have to reload
	select {
	case sig := <-osSignalChannel:
		return mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener)
	case sig := <-mainSignalChannel:
		return mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener)
	}
}

func initializePeers(GlobalConfig *Config, waitGroupPeers *sync.WaitGroup, waitGroupInit *sync.WaitGroup, waitGroupListener *sync.WaitGroup, shutdownChannel chan bool) {
	// This node's http address (http://*:1234), to be used as address pattern
	var nodeListenAddress string
	rx := regexp.MustCompile("^(https?)://(.*?):(.*)")
	for _, listen := range GlobalConfig.Listen {
		parts := rx.FindStringSubmatch(listen)
		if len(parts) != 4 {
			continue
		}
		nodeListenAddress = listen
		break
	}

	// Get rid of obsolete peers (removed from config)
	for id := range DataStore {
		found := false // id exists
		for _, c := range GlobalConfig.Connections {
			if c.ID == id {
				found = true
			}
		}
		if !found {
			delete(DataStore, id)
		}
	}

	// Create/set Peer objects
	DataStoreOrder = nil
	var backends []string
	for _, c := range GlobalConfig.Connections {
		// Keep peer if connection settings unchanged
		var p *Peer
		if v, ok := DataStore[c.ID]; ok {
			if c.Equals(&v.Config) {
				p = v
				p.waitGroup = waitGroupPeers
				p.shutdownChannel = shutdownChannel
			}
		}

		// Create new peer otherwise
		if p == nil {
			p = NewPeer(c, waitGroupPeers, shutdownChannel)
		}

		// Check for duplicate id
		for _, b := range backends {
			if b == c.ID {
				log.Fatalf("Duplicate id in connection list: %s", c.ID)
			}
		}
		backends = append(backends, c.ID)

		// Put new or modified peer in map
		DataStore[c.ID] = p
		DataStoreOrder = append(DataStoreOrder, c.ID)
		// Peer started later in node redistribution routine
	}

	// Node accessor
	var nodeAddresses []string
	nodeAddresses = GlobalConfig.Nodes
	nodeAccessor = NewNodes(nodeAddresses, nodeListenAddress, waitGroupInit, shutdownChannel)
	nodeAccessor.Initialize() // starts peers in single mode
	nodeAccessor.Start()      // nodes loop starts/stops peers in cluster mode

}

func checkFlags() {
	flag.Parse()
	if flagVersion {
		fmt.Printf("%s - version %s (Build: %s)\n", NAME, VERSION, Build)
		os.Exit(2)
	}

	if flagProfile != "" {
		runtime.SetBlockProfileRate(10)
		go func() {
			// make sure we log panics properly
			defer logPanicExit()
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

	// write pidfile
	if flagPidfile != "" {
		err := ioutil.WriteFile(flagPidfile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0640)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: could not write pidfile '%s': %s\n", flagPidfile, err.Error())
			os.Exit(2)
		}
	}

	// initialize configuration and check for config errors
	GlobalConfig = ReadConfig(flagConfigFile)
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
		log.Infof("got sigint, quitting")
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
		waitGroupPeers.Wait()
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
	// combine listeners from all files
	var allListeners []string

	for _, configFile := range files {
		if _, err := os.Stat(configFile); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: could not load configuration from %s: %s\nuse --help to see all options.\n", configFile, err.Error())
			os.Exit(3)
		}
		if _, err := toml.DecodeFile(configFile, &conf); err != nil {
			panic(err)
		}
		allListeners = append(allListeners, conf.Listen...)
	}
	if flagLogFile != "" {
		conf.LogFile = flagLogFile
	}
	conf.Listen = allListeners

	promPeerUpdateInterval.Set(float64(conf.Updateinterval))

	return
}

func logPanicExit() {
	if r := recover(); r != nil {
		log.Errorf("Panic: %s", r)
		log.Errorf("%s", debug.Stack())
		os.Exit(1)
	}
}
