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
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/lkarlslund/stringdedup"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

const (
	// VERSION contains the actual lmd version
	VERSION = "1.7.0"
	// NAME defines the name of this project
	NAME = "lmd"
)

const loose = "loose"
const strict = "strict"

// https://github.com/golang/go/issues/8005#issuecomment-190753527
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Connection defines a single connection configuration.
type Connection struct {
	Name           string
	ID             string
	Source         []string
	Auth           string
	RemoteName     string `toml:"remote_name"`
	Section        string
	TLSCertificate string
	TLSKey         string
	TLSCA          string
	TLSSkipVerify  int
	Proxy          string
}

// Equals checks if two connection objects are identical.
func (c *Connection) Equals(other *Connection) bool {
	equal := c.ID == other.ID
	equal = equal && c.Name == other.Name
	equal = equal && c.Auth == other.Auth
	equal = equal && c.RemoteName == other.RemoteName
	equal = equal && c.Section == other.Section
	equal = equal && c.TLSCertificate == other.TLSCertificate
	equal = equal && c.TLSKey == other.TLSKey
	equal = equal && c.TLSCA == other.TLSCA
	equal = equal && c.TLSSkipVerify == other.TLSSkipVerify
	equal = equal && strings.Join(c.Source, ":") == strings.Join(other.Source, ":")
	return equal
}

// Config defines the available configuration options from supplied config files.
type Config struct {
	Listen               []string
	Nodes                []string
	TLSCertificate       string
	TLSKey               string
	TLSClientPems        []string
	Updateinterval       int64
	FullUpdateInterval   int64
	Connections          []Connection
	LogFile              string
	LogLevel             string
	ConnectTimeout       int
	NetTimeout           int
	ListenTimeout        int
	ListenPrometheus     string
	SkipSSLCheck         int
	IdleTimeout          int64
	IdleInterval         int64
	StaleBackendTimeout  int
	BackendKeepAlive     bool
	ServiceAuthorization string
	GroupAuthorization   string
}

// PeerMap contains a map of available remote peers.
var PeerMap map[string]*Peer

// PeerMapOrder contains the order of all remote peers as defined in the supplied config files.
var PeerMapOrder []string

// PeerMapLock is the lock for the PeerMap map
var PeerMapLock *LoggingLock

// Listeners stores if we started a listener
var Listeners map[string]*Listener

// ListenersLock is the lock for the Listeners map
var ListenersLock *LoggingLock

type configFiles []string

// String returns the config files list as string.
func (c *configFiles) String() string {
	return fmt.Sprintf("%s", *c)
}

// Set appends a config file to the list of config files.
func (c *configFiles) Set(value string) (err error) {
	_, err = os.Stat(value)
	if err != nil {
		return
	}
	*c = append(*c, value)
	return
}

// nodeAccessor manages cluster nodes and starts/stops peers.
var nodeAccessor *Nodes

var flagVerbose bool
var flagVeryVerbose bool
var flagTraceVerbose bool
var flagConfigFile configFiles
var flagVersion bool
var flagLogFile string
var flagPidfile string
var flagProfile string

var once sync.Once
var mainSignalChannel chan os.Signal
var lastMainRestart = time.Now().Unix()

var reHTTPHostPort *regexp.Regexp

// initialize objects structure
func init() {
	InitTableNames()
	InitObjects()
	mainSignalChannel = make(chan os.Signal)
	PeerMapLock = NewLoggingLock("PeerMapLock")
	PeerMap = make(map[string]*Peer)
	PeerMapOrder = make([]string, 0)
	Listeners = make(map[string]*Listener)
	ListenersLock = NewLoggingLock("ListenersLock")
	reHTTPHostPort = regexp.MustCompile("^(https?)://(.*?):(.*)")
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
		defer log.Debugf("lmd shutdown complete")
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
	localConfig := *(ReadConfig(flagConfigFile))
	setDefaults(&localConfig)
	setVerboseFlags(&localConfig)
	InitLogging(&localConfig)
	setServiceAuthorization(&localConfig)
	setGroupAuthorization(&localConfig)

	promPeerUpdateInterval.Set(float64(localConfig.Updateinterval))

	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, syscall.SIGHUP)
	signal.Notify(osSignalChannel, syscall.SIGTERM)
	signal.Notify(osSignalChannel, os.Interrupt)
	signal.Notify(osSignalChannel, syscall.SIGINT)

	osSignalUsrChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalUsrChannel, syscall.SIGUSR1)

	lastMainRestart = time.Now().Unix()
	shutdownChannel := make(chan bool)
	waitGroupInit := &sync.WaitGroup{}
	waitGroupListener := &sync.WaitGroup{}
	waitGroupPeers := &sync.WaitGroup{}

	if len(localConfig.Connections) == 0 {
		log.Fatalf("no connections defined")
	}

	if len(localConfig.Listen) == 0 {
		log.Fatalf("no listeners defined")
	}

	if flagProfile != "" {
		log.Warnf("pprof profiler listening at %s", flagProfile)
	}

	// initialize prometheus
	prometheusListener := initPrometheus(&localConfig)

	// start local listeners
	initializeListeners(&localConfig, waitGroupListener, waitGroupInit, shutdownChannel)

	// start remote connections
	initializePeers(&localConfig, waitGroupPeers, waitGroupInit, shutdownChannel)

	once.Do(PrintVersion)

	// just wait till someone hits ctrl+c or we have to reload
	statsTimer := time.NewTicker(30 * time.Second)
	for {
		select {
		case sig := <-osSignalChannel:
			return mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener)
		case sig := <-osSignalUsrChannel:
			mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener)
		case sig := <-mainSignalChannel:
			return mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener)
		case <-statsTimer.C:
			updateStatistics()
		}
	}
}

// Version returns the LMD version string
func Version() string {
	return fmt.Sprintf("%s (Build: %s)", VERSION, Build)
}

func initializeListeners(localConfig *Config, waitGroupListener *sync.WaitGroup, waitGroupInit *sync.WaitGroup, shutdownChannel chan bool) {
	ListenersNew := make(map[string]*Listener)

	// close all listeners which are no longer defined
	ListenersLock.Lock()
	for con, l := range Listeners {
		found := false
		for _, listen := range localConfig.Listen {
			if listen == con {
				found = true
				break
			}
		}
		if !found {
			delete(Listeners, con)
			l.connection.Close()
		}
	}

	// open new listeners
	for _, listen := range localConfig.Listen {
		if l, ok := Listeners[listen]; ok {
			l.shutdownChannel = shutdownChannel
			l.LocalConfig = localConfig
			ListenersNew[listen] = l
		} else {
			waitGroupInit.Add(1)
			l := NewListener(localConfig, listen, waitGroupInit, waitGroupListener, shutdownChannel)
			ListenersNew[listen] = l
		}
	}

	Listeners = ListenersNew
	ListenersLock.Unlock()
}

func initializePeers(localConfig *Config, waitGroupPeers *sync.WaitGroup, waitGroupInit *sync.WaitGroup, shutdownChannel chan bool) {
	// This node's http address (http://*:1234), to be used as address pattern
	var nodeListenAddress string
	for _, listen := range localConfig.Listen {
		parts := reHTTPHostPort.FindStringSubmatch(listen)
		if len(parts) != 4 {
			continue
		}
		nodeListenAddress = listen
		break
	}

	// Get rid of obsolete peers (removed from config)
	PeerMapLock.Lock()
	for id := range PeerMap {
		found := false // id exists
		for i := range localConfig.Connections {
			if localConfig.Connections[i].ID == id {
				found = true
			}
		}
		if !found {
			PeerMap[id].Stop()
			PeerMap[id].ClearLocked()
			PeerMapRemove(id)
		}
	}
	PeerMapLock.Unlock()

	// Create/set Peer objects
	PeerMapNew := make(map[string]*Peer)
	PeerMapOrderNew := make([]string, 0)
	backends := make([]string, len(localConfig.Connections))
	for i := range localConfig.Connections {
		c := localConfig.Connections[i]
		// Keep peer if connection settings unchanged
		var p *Peer
		PeerMapLock.RLock()
		if v, ok := PeerMap[c.ID]; ok {
			if c.Equals(v.Config) {
				p = v
				p.PeerLock.Lock()
				p.waitGroup = waitGroupPeers
				p.shutdownChannel = shutdownChannel
				p.LocalConfig = localConfig
				p.PeerLock.Unlock()
			}
		}
		PeerMapLock.RUnlock()

		// Create new peer otherwise
		if p == nil {
			p = NewPeer(localConfig, &c, waitGroupPeers, shutdownChannel)
		}

		// Check for duplicate id
		for _, b := range backends {
			if b == c.ID {
				log.Fatalf("Duplicate id in connection list: %s", c.ID)
			}
		}
		backends = append(backends, c.ID)

		// Put new or modified peer in map
		PeerMapNew[c.ID] = p
		PeerMapOrderNew = append(PeerMapOrderNew, c.ID)
		// Peer started later in node redistribution routine
	}

	PeerMapLock.Lock()
	PeerMapOrder = PeerMapOrderNew
	PeerMap = PeerMapNew
	PeerMapLock.Unlock()

	// Node accessor
	nodeAddresses := localConfig.Nodes
	nodeAccessor = NewNodes(localConfig, nodeAddresses, nodeListenAddress, waitGroupInit, shutdownChannel)
	nodeAccessor.Initialize() // starts peers in single mode
	nodeAccessor.Start()      // nodes loop starts/stops peers in cluster mode
}

func checkFlags() {
	flag.Parse()
	if flagVersion {
		fmt.Printf("%s - version %s\n", NAME, Version())
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

	createPidFile(flagPidfile)
}

func createPidFile(path string) {
	// write the pid id if file path is defined
	if path == "" {
		return
	}
	// check existing pid
	if dat, err := ioutil.ReadFile(path); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(dat))); err == nil {
			if process, err := os.FindProcess(pid); err == nil {
				if err := process.Signal(syscall.Signal(0)); err == nil {
					fmt.Fprintf(os.Stderr, "ERROR: lmd already running: %d\n", pid)
					os.Exit(2)
				}
			}
		}
		fmt.Fprintf(os.Stderr, "WARNING: removing stale pidfile %s\n", path)
	}
	err := ioutil.WriteFile(path, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0664)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Could not write pidfile: %s\n", err.Error())
		os.Exit(2)
	}
}

func deletePidFile(f string) {
	if f != "" {
		os.Remove(f)
	}
}

func setVerboseFlags(localConfig *Config) {
	if flagVerbose {
		localConfig.LogLevel = "Info"
	}
	if flagVeryVerbose {
		localConfig.LogLevel = "Debug"
	}
	if flagTraceVerbose {
		localConfig.LogLevel = "Trace"
	}
}

// NewLMDHTTPClient creates a http.Client with the given tls.Config
func NewLMDHTTPClient(tlsConfig *tls.Config, proxy string) *http.Client {
	tr := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfig,
	}
	if proxy != "" {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			log.Fatalf("ERROR: cannot parse proxy into url '%s': %s\n", proxy, err.Error())
		}
		tr.Proxy = http.ProxyURL(proxyURL)
	}
	netClient := &http.Client{
		Timeout:   time.Second * 30,
		Transport: tr,
	}
	return netClient
}

func mainSignalHandler(sig os.Signal, shutdownChannel chan bool, waitGroupPeers *sync.WaitGroup, waitGroupListener *sync.WaitGroup, prometheusListener io.Closer) (exitCode int) {
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
		deletePidFile(flagPidfile)
		return (0)
	case syscall.SIGINT:
		fallthrough
	case os.Interrupt:
		log.Infof("got sigint, quitting")
		shutdownChannel <- true
		close(shutdownChannel)
		if prometheusListener != nil {
			prometheusListener.Close()
		}
		// wait one second which should be enough for the listeners
		waitTimeout(waitGroupListener, time.Second)
		deletePidFile(flagPidfile)
		return (1)
	case syscall.SIGHUP:
		log.Infof("got sighup, reloading configuration...")
		if prometheusListener != nil {
			prometheusListener.Close()
		}
		return (-1)
	case syscall.SIGUSR1:
		log.Errorf("requested thread dump via signal %s", sig)
		logCurrentsLocks()
		logThreaddump()
		return (0)
	default:
		log.Warnf("Signal not handled: %v", sig)
	}
	return (1)
}

func logCurrentsLocks() {
	log.Errorf("*** current held locks:")
	PeerMapLock.RLock()
	for id := range PeerMap {
		p := PeerMap[id]
		if atomic.LoadInt32(&p.PeerLock.currentlyLocked) == 0 {
			log.Errorf("[%s] peer holding peer lock:", p.Name)
			LogCaller(log.Errorf, p.PeerLock.currentLockpointer.Load().(*[]uintptr))
		}
		if atomic.LoadInt32(&p.DataLock.currentlyLocked) == 0 {
			log.Errorf("[%s] peer holding data lock:", p.Name)
			LogCaller(log.Errorf, p.DataLock.currentLockpointer.Load().(*[]uintptr))
		}
	}
	PeerMapLock.RUnlock()
}

func logThreaddump() {
	log.Errorf("*** full thread dump:")
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	log.Errorf("%s", buf)
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	if timeout < time.Millisecond*10 {
		log.Panic("bogus timeout")
	}
	c := make(chan struct{})
	t := time.NewTimer(timeout)
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		t.Stop()
		return false // completed normally
	case <-t.C:
		return true // timed out
	}
}

func setDefaults(conf *Config) {
	if conf.NetTimeout <= 0 {
		conf.NetTimeout = 120
	}
	if conf.ConnectTimeout <= 0 {
		conf.ConnectTimeout = 30
	}
	if conf.ListenTimeout <= 0 {
		conf.ListenTimeout = 60
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

func setServiceAuthorization(conf *Config) {
	ServiceAuth := strings.ToLower(conf.ServiceAuthorization)
	switch {
	case ServiceAuth == loose, ServiceAuth == strict:
		conf.ServiceAuthorization = ServiceAuth
	case ServiceAuth != "":
		log.Warnf("Invalid ServiceAuthorization: %s, using loose", conf.ServiceAuthorization)
		conf.ServiceAuthorization = loose
	default:
		conf.ServiceAuthorization = loose
	}
}

func setGroupAuthorization(conf *Config) {
	GroupAuth := strings.ToLower(conf.GroupAuthorization)
	switch {
	case GroupAuth == loose, GroupAuth == strict:
		conf.GroupAuthorization = GroupAuth
	case GroupAuth != "":
		log.Warnf("Invalid GroupAuthorization: %s, using strict", conf.GroupAuthorization)
		conf.GroupAuthorization = strict
	default:
		conf.GroupAuthorization = strict
	}
}

// PrintVersion prints the version
func PrintVersion() {
	fmt.Printf("%s - version %s (Build: %s) started with config %s\n", NAME, VERSION, Build, flagConfigFile)
}

// ReadConfig reads all config files.
// It returns a Config object.
func ReadConfig(files []string) *Config {
	// combine listeners from all files
	allListeners := make([]string, 0)
	conf := &Config{BackendKeepAlive: true}
	for _, configFile := range files {
		if _, err := os.Stat(configFile); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: could not load configuration from %s: %s\nuse --help to see all options.\n", configFile, err.Error())
			os.Exit(3)
		}
		if _, err := toml.DecodeFile(configFile, &conf); err != nil {
			panic(err)
		}
		allListeners = append(allListeners, conf.Listen...)
		conf.Listen = []string{}
	}
	if flagLogFile != "" {
		conf.LogFile = flagLogFile
	}
	conf.Listen = allListeners

	for i := range conf.Connections {
		for j := range conf.Connections[i].Source {
			if strings.HasPrefix(conf.Connections[i].Source[j], "http") {
				conf.Connections[i].Source[j] = completePeerHTTPAddr(conf.Connections[i].Source[j])
			}
		}
	}

	return conf
}

func logPanicExit() {
	if r := recover(); r != nil {
		log.Errorf("Panic: %s", r)
		log.Errorf("Version: %s", Version())
		log.Errorf("%s", debug.Stack())
		deletePidFile(flagPidfile)
		os.Exit(1)
	}
}

func timeOrNever(timestamp int64) string {
	if timestamp > 0 {
		return (time.Unix(timestamp, 0).String())
	}
	return "never"
}

// PeerMapRemove deletes a peer from PeerMap and PeerMapOrder
func PeerMapRemove(peerID string) {
	// find id in order array
	for i, id := range PeerMapOrder {
		if id == peerID {
			PeerMapOrder = append(PeerMapOrder[:i], PeerMapOrder[i+1:]...)
			break
		}
	}
	delete(PeerMap, peerID)
}

// VersionNumeric converts a string version to a float number
func VersionNumeric(input string) (numeric float64) {
	pow := float64(0)
	for _, num := range strings.Split(input, ".") {
		f, _ := strconv.ParseFloat(num, 64)
		numeric += f * math.Pow(10, pow)
		pow -= 3
	}
	return numeric
}

// completePeerHTTPAddr returns autocompleted address for peer
// it appends /thruk/cgi-bin/remote.cgi or parts of it
func completePeerHTTPAddr(addr string) string {
	addr = strings.TrimSuffix(addr, "cgi-bin/remote.cgi")
	addr = strings.TrimSuffix(addr, "/")
	addr = strings.TrimSuffix(addr, "thruk")
	addr = strings.TrimSuffix(addr, "/")
	return addr + "/thruk/cgi-bin/remote.cgi"
}

func ByteCountBinary(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func updateStatistics() {
	size := stringdedup.Size()
	promStringDedupCount.Set(float64(size))
	promStringDedupBytes.Set(float64(stringdedup.ByteCount()))
	promStringDedupIndexBytes.Set(float64(32 * size))
}
