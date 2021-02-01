/*
LMD - Livestatus Multitool daemon
A livestatus in/out converter and caching daemon.

Help

Use `lmd -h` to get help on command line parameters.
*/
package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	jsoniter "github.com/json-iterator/go"
	"github.com/lkarlslund/stringdedup"
	"github.com/sasha-s/go-deadlock"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

const (
	// VERSION contains the actual lmd version
	VERSION = "1.9.5"
	// NAME defines the name of this project
	NAME = "lmd"

	// AuthLoose is used for loose authorization when host contacts are granted all services
	AuthLoose = "loose"

	// AuthStrict is used for strict authorization when host contacts are not granted all services
	AuthStrict = "strict"

	// ExitCritical is used to non-ok exits
	ExitCritical = 2

	// ExitUnknown is used as exit code for help
	ExitUnknown = 3

	// StatsTimerInterval sets the interval at which statistics will be updated
	StatsTimerInterval = 30 * time.Second

	// HTTPClientTimeout sets the default HTTP client timeout
	HTTPClientTimeout = 30 * time.Second

	// BlockProfileRateInterval sets the profiling interval when started with -profile
	BlockProfileRateInterval = 10
)

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
	Flags          []string
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
	equal = equal && strings.Join(c.Flags, ":") == strings.Join(other.Flags, ":")
	return equal
}

// Config defines the available configuration options from supplied config files.
type Config struct {
	Listen                 []string
	Nodes                  []string
	TLSCertificate         string
	TLSKey                 string
	TLSClientPems          []string
	Updateinterval         int64
	FullUpdateInterval     int64
	Connections            []Connection
	LogFile                string
	LogLevel               string
	LogSlowQueryThreshold  int
	LogHugeQueryThreshold  int
	ConnectTimeout         int
	NetTimeout             int
	ListenTimeout          int
	SaveTempRequests       bool
	ListenPrometheus       string
	SkipSSLCheck           int
	IdleTimeout            int64
	IdleInterval           int64
	StaleBackendTimeout    int
	BackendKeepAlive       bool
	ServiceAuthorization   string
	GroupAuthorization     string
	SyncIsExecuting        bool
	CompressionMinimumSize int
	CompressionLevel       int
	MaxClockDelta          float64
	UpdateOffset           int64
}

// PeerMap contains a map of available remote peers.
var PeerMap map[string]*Peer

// PeerMapOrder contains the order of all remote peers as defined in the supplied config files.
var PeerMapOrder []string

// PeerMapLock is the lock for the PeerMap map
var PeerMapLock *deadlock.RWMutex

// Listeners stores if we started a listener
var Listeners map[string]*Listener

// ListenersLock is the lock for the Listeners map
var ListenersLock *deadlock.RWMutex

type configFiles []string

// String returns the config files list as string.
func (c *configFiles) String() string {
	return fmt.Sprintf("%s", *c)
}

// Set appends a config file to the list of config files.
func (c *configFiles) Set(value string) (err error) {
	_, err = os.Stat(value)
	// check if the file exists but skip errors for file globs
	if err != nil && !strings.ContainsAny(value, "?*") {
		return
	}
	err = nil
	*c = append(*c, value)
	return
}

// nodeAccessor manages cluster nodes and starts/stops peers.
var nodeAccessor *Nodes

type arrayFlags struct {
	list []string
}

func (i *arrayFlags) String() string {
	return strings.Join(i.list, ", ")
}

func (i *arrayFlags) Set(value string) error {
	i.list = append(i.list, value)
	return nil
}

var flagVerbose bool
var flagVeryVerbose bool
var flagTraceVerbose bool
var flagConfigFile configFiles
var flagVersion bool
var flagLogFile string
var flagPidfile string
var flagProfile string
var flagDeadlock int
var flagCPUProfile string
var flagMemProfile string
var flagCfgOption arrayFlags

var cpuProfileHandler *os.File

var once sync.Once
var mainSignalChannel chan os.Signal
var lastMainRestart = time.Now().Unix()

var reHTTPHostPort *regexp.Regexp

// initialize objects structure
func init() {
	InitTableNames()
	InitObjects()
	mainSignalChannel = make(chan os.Signal)
	PeerMapLock = new(deadlock.RWMutex)
	PeerMap = make(map[string]*Peer)
	PeerMapOrder = make([]string, 0)
	Listeners = make(map[string]*Listener)
	ListenersLock = new(deadlock.RWMutex)
	reHTTPHostPort = regexp.MustCompile("^(https?)://(.*?):(.*)")
}

func setFlags() {
	flag.Var(&flagConfigFile, "c", "set location for config file, can be specified multiple times, can contain globs like lmd.ini.d/*.ini")
	flag.Var(&flagConfigFile, "config", "set location for config file, can be specified multiple times, can contain globs like lmd.ini.d/*.ini")
	flag.StringVar(&flagPidfile, "pidfile", "", "set path to pidfile")
	flag.StringVar(&flagLogFile, "logfile", "", "override logfile from the configuration file")
	flag.BoolVar(&flagVerbose, "v", false, "enable verbose output")
	flag.BoolVar(&flagVerbose, "verbose", false, "enable verbose output")
	flag.BoolVar(&flagVeryVerbose, "vv", false, "enable very verbose output")
	flag.BoolVar(&flagTraceVerbose, "vvv", false, "enable trace output")
	flag.BoolVar(&flagVersion, "version", false, "print version and exit")
	flag.StringVar(&flagProfile, "debug-profiler", "", "start pprof profiler on this port, ex. :6060")
	flag.StringVar(&flagCPUProfile, "cpuprofile", "", "write cpu profile to `file`")
	flag.StringVar(&flagMemProfile, "memprofile", "", "write memory profile to `file`")
	flag.IntVar(&flagDeadlock, "debug-deadlock", 0, "enable deadlock detection with given timeout")
	flag.Var(&flagCfgOption, "o", "write memory profile to `file`")
}

func main() {
	// command line arguments
	setFlags()
	checkFlags()

	// make sure we log panics properly
	defer logPanicExit()

	for {
		exitCode := mainLoop(mainSignalChannel, nil)
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

func mainLoop(mainSignalChannel chan os.Signal, initChannel chan bool) (exitCode int) {
	localConfig := *(ReadConfig(flagConfigFile))
	applyArgFlags(flagCfgOption, &localConfig)
	setDefaults(&localConfig)
	setVerboseFlags(&localConfig)
	InitLogging(&localConfig)
	setServiceAuthorization(&localConfig)
	setGroupAuthorization(&localConfig)

	CompressionLevel = localConfig.CompressionLevel
	CompressionMinimumSize = localConfig.CompressionMinimumSize

	// put some configuration settings into metrics
	promPeerUpdateInterval.Set(float64(localConfig.Updateinterval))
	promPeerFullUpdateInterval.Set(float64(localConfig.FullUpdateInterval))
	promCompressionLevel.Set(float64(CompressionLevel))
	promCompressionMinimumSize.Set(float64(CompressionMinimumSize))
	promSyncIsExecuting.Set(float64(interface2int(localConfig.SyncIsExecuting)))
	promSaveTempRequests.Set(float64(interface2int(localConfig.SaveTempRequests)))
	promBackendKeepAlive.Set(float64(interface2int(localConfig.BackendKeepAlive)))

	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, syscall.SIGHUP)
	signal.Notify(osSignalChannel, syscall.SIGTERM)
	signal.Notify(osSignalChannel, os.Interrupt)
	signal.Notify(osSignalChannel, syscall.SIGINT)

	osSignalUsrChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalUsrChannel, syscall.SIGUSR1)
	signal.Notify(osSignalUsrChannel, syscall.SIGUSR2)

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

	once.Do(PrintVersion)
	logConfig(&localConfig)

	// initialize prometheus
	prometheusListener := initPrometheus(&localConfig)

	// start local listeners
	initializeListeners(&localConfig, waitGroupListener, waitGroupInit, shutdownChannel)

	// start remote connections
	initializePeers(&localConfig, waitGroupPeers, waitGroupInit, shutdownChannel)

	if initChannel != nil {
		initChannel <- true
	}

	// just wait till someone hits ctrl+c or we have to reload
	statsTimer := time.NewTicker(StatsTimerInterval)
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
	return fmt.Sprintf("%s (Build: %s, %s)", VERSION, Build, runtime.Version())
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
			l.GlobalConfig = localConfig
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
			p := PeerMap[id]
			p.Stop()
			p.ClearData(true)
			PeerMapRemove(id)
		}
	}
	PeerMapLock.Unlock()

	// Create/set Peer objects
	PeerMapNew := make(map[string]*Peer)
	PeerMapOrderNew := make([]string, 0)
	backends := make([]string, 0, len(localConfig.Connections))
	for i := range localConfig.Connections {
		c := localConfig.Connections[i]
		// Keep peer if connection settings unchanged
		var p *Peer
		PeerMapLock.RLock()
		if v, ok := PeerMap[c.ID]; ok {
			if c.Equals(v.Config) {
				p = v
				p.Lock.Lock()
				p.waitGroup = waitGroupPeers
				p.shutdownChannel = shutdownChannel
				p.GlobalConfig = localConfig
				p.Lock.Unlock()
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
		os.Exit(ExitCritical)
	}

	if flagProfile != "" {
		if flagCPUProfile != "" || flagMemProfile != "" {
			fmt.Print("ERROR: either use -debug-profile or -cpu/memprofile, not both\n")
			os.Exit(ExitCritical)
		}
		runtime.SetBlockProfileRate(BlockProfileRateInterval)
		runtime.SetMutexProfileFraction(BlockProfileRateInterval)
		go func() {
			// make sure we log panics properly
			defer logPanicExit()
			err := http.ListenAndServe(flagProfile, http.DefaultServeMux)
			if err != nil {
				log.Debugf("http.ListenAndServe finished with: %e", err)
			}
		}()
	}

	if flagCPUProfile != "" {
		runtime.SetBlockProfileRate(BlockProfileRateInterval)
		cpuProfileHandler, err := os.Create(flagCPUProfile)
		if err != nil {
			fmt.Printf("ERROR: could not create CPU profile: %s", err.Error())
			os.Exit(ExitCritical)
		}
		if err := pprof.StartCPUProfile(cpuProfileHandler); err != nil {
			fmt.Printf("ERROR: could not start CPU profile: %s", err.Error())
			os.Exit(ExitCritical)
		}
	}

	if flagDeadlock <= 0 {
		deadlock.Opts.Disable = true
	} else {
		deadlock.Opts.Disable = false
		deadlock.Opts.DeadlockTimeout = time.Duration(flagDeadlock) * time.Second
		deadlock.Opts.LogBuf = NewLogWriter("Error")
	}

	if len(flagConfigFile) == 0 {
		fmt.Print("ERROR: no config files specified.\nSee --help for all options.\n")
		os.Exit(ExitCritical)
	}

	createPidFile(flagPidfile)
}

func createPidFile(path string) {
	// write the pid id if file path is defined
	if path == "" {
		return
	}
	// check existing pid
	if !checkPidFile(path) {
		fmt.Fprintf(os.Stderr, "WARNING: removing stale pidfile %s\n", path)
	}
	err := ioutil.WriteFile(path, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0664)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Could not write pidfile: %s\n", err.Error())
		os.Exit(ExitCritical)
	}
}

// checkPidFile returns false if pidfile is stale
func checkPidFile(path string) bool {
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return true
	}

	if pid, err := strconv.Atoi(strings.TrimSpace(string(dat))); err == nil {
		if process, err := os.FindProcess(pid); err == nil {
			if err := process.Signal(syscall.Signal(0)); err == nil {
				fmt.Fprintf(os.Stderr, "ERROR: lmd already running: %d\n", pid)
				os.Exit(ExitCritical)
			}
		}
	}

	return false
}

func deletePidFile(f string) {
	if f != "" {
		os.Remove(f)
	}
}

func onExit() {
	deletePidFile(flagPidfile)
	if flagCPUProfile != "" {
		pprof.StopCPUProfile()
		cpuProfileHandler.Close()
		log.Warnf("cpu profile written to: %s", flagCPUProfile)
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

func applyArgFlags(opts arrayFlags, localConfig *Config) {
	ps := reflect.ValueOf(localConfig)
	s := ps.Elem()
	typeOfS := s.Type()

	for _, opt := range opts.list {
		tmp := strings.SplitN(opt, "=", 2)
		if len(tmp) < 2 {
			log.Fatalf("ERROR: cannot parse option %s, syntax is '-o ConfigOption=Value'", opt)
		}
		optname := tmp[0]
		optvalue := tmp[1]
		found := false
		for i := 0; i < s.NumField(); i++ {
			cfgname := typeOfS.Field(i).Name
			if strings.EqualFold(cfgname, optname) {
				f := s.Field(i)
				if f.IsValid() && f.CanSet() {
					switch f.Kind() {
					case reflect.Int, reflect.Int64:
						f.SetInt(interface2int64(optvalue))
					case reflect.String:
						f.SetString(optvalue)
					case reflect.Bool:
						f.SetBool(interface2bool(optvalue))
					default:
						log.Fatalf("ERROR: cannot set option %s, type %s is not supported", cfgname, f.Kind())
					}
					found = true
					break
				}
				log.Fatalf("ERROR: cannot set option %s", cfgname)
			}
		}
		if !found {
			log.Fatalf("ERROR: no such option %s", optname)
		}
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
		Timeout:   HTTPClientTimeout,
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if strings.Contains(req.URL.RawQuery, "/omd/error.py%3fcode=500") {
				log.Warnf("HTTP request redirected to %s which indicates an internal error in the backend", req.URL.String())
				return http.ErrUseLastResponse
			}
			if strings.Contains(req.URL.Path, "/thruk/cgi-bin/login.cgi") {
				log.Warnf("HTTP request redirected to %s, this indicates an issue with authentication", req.URL.String())
				return http.ErrUseLastResponse
			}
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return nil
		},
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
		onExit()
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
		onExit()
		return (1)
	case syscall.SIGHUP:
		log.Infof("got sighup, reloading configuration...")
		if prometheusListener != nil {
			prometheusListener.Close()
		}
		return (-1)
	case syscall.SIGUSR1:
		log.Errorf("requested thread dump via signal %s", sig)
		logThreaddump()
		return (0)
	case syscall.SIGUSR2:
		if flagMemProfile == "" {
			log.Errorf("requested memory profile, but flag -memprofile missing")
			return (0)
		}
		f, err := os.Create(flagMemProfile)
		if err != nil {
			log.Errorf("could not create memory profile: %s", err.Error())
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Errorf("could not write memory profile: %s", err.Error())
		}
		log.Warnf("memory profile written to: %s", flagMemProfile)
		return (0)
	default:
		log.Warnf("Signal not handled: %v", sig)
	}
	return (1)
}

func logThreaddump() {
	log.Errorf("*** full thread dump:")
	buf := make([]byte, 1<<16)
	if n := runtime.Stack(buf, true); n < len(buf) {
		buf = buf[:n]
	}
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
		conf.Updateinterval = 7
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
	if conf.LogSlowQueryThreshold <= 0 {
		conf.LogSlowQueryThreshold = 5
	}
	if conf.LogHugeQueryThreshold <= 0 {
		conf.LogHugeQueryThreshold = 100
	}
	if conf.CompressionMinimumSize <= 0 {
		conf.CompressionMinimumSize = 500
	}
	if conf.MaxClockDelta < 0 {
		conf.MaxClockDelta = 10
	}
	if conf.UpdateOffset <= 0 {
		conf.UpdateOffset = 3
	}
}

func setServiceAuthorization(conf *Config) {
	ServiceAuth := strings.ToLower(conf.ServiceAuthorization)
	switch {
	case ServiceAuth == AuthLoose, ServiceAuth == AuthStrict:
		conf.ServiceAuthorization = ServiceAuth
	case ServiceAuth != "":
		log.Warnf("Invalid ServiceAuthorization: %s, using loose", conf.ServiceAuthorization)
		conf.ServiceAuthorization = AuthLoose
	default:
		conf.ServiceAuthorization = AuthLoose
	}
}

func setGroupAuthorization(conf *Config) {
	GroupAuth := strings.ToLower(conf.GroupAuthorization)
	switch {
	case GroupAuth == AuthLoose, GroupAuth == AuthStrict:
		conf.GroupAuthorization = GroupAuth
	case GroupAuth != "":
		log.Warnf("Invalid GroupAuthorization: %s, using strict", conf.GroupAuthorization)
		conf.GroupAuthorization = AuthStrict
	default:
		conf.GroupAuthorization = AuthStrict
	}
}

// PrintVersion prints the version
func PrintVersion() {
	fmt.Printf("%s - version %s started with config %s\n", NAME, Version(), flagConfigFile)
}

// ReadConfig reads all config files.
// It returns a Config object.
func ReadConfig(files []string) *Config {
	// combine listeners from all files
	allListeners := make([]string, 0)
	conf := &Config{
		BackendKeepAlive:       true,
		SaveTempRequests:       true,
		CompressionLevel:       -1,
		CompressionMinimumSize: DefaultCompressionMinimumSize,
	}
	for _, pattern := range files {
		configFiles, errGlob := filepath.Glob(pattern)
		if errGlob != nil {
			fmt.Fprintf(os.Stderr, "ERROR: config file pattern %s is invalid: %s\n", pattern, errGlob.Error())
			os.Exit(ExitUnknown)
		}
		if configFiles == nil {
			log.Debugf("config file pattern %s did not match any files", pattern)
			continue
		}
		for _, configFile := range configFiles {
			if _, err := os.Stat(configFile); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: could not load configuration from %s: %s\nuse --help to see all options.\n", configFile, err.Error())
				os.Exit(ExitUnknown)
			}
			if _, err := toml.DecodeFile(configFile, &conf); err != nil {
				panic(err)
			}
			allListeners = append(allListeners, conf.Listen...)
			conf.Listen = []string{}
		}
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

func logConfig(conf *Config) {
	// print command line arguments
	arg, _ := jsoniter.MarshalIndent(os.Args, "", "  ")
	cfg, _ := jsoniter.MarshalIndent(*conf, "", "  ")

	log.Debug("command line arguments:")
	for _, s := range strings.Split(string(arg), "\n") {
		log.Debugf("args: %s", s)
	}

	replaceAuth := regexp.MustCompile(`"Auth": ".*",`)
	log.Debug("effective configuration:")
	for _, s := range strings.Split(string(cfg), "\n") {
		s = replaceAuth.ReplaceAllString(s, `"Auth": "***",`)
		log.Debugf("conf: %s", s)
	}
}

func logPanicExit() {
	if r := recover(); r != nil {
		log.Errorf("Panic: %s", r)
		log.Errorf("Version: %s", Version())
		log.Errorf("%s", debug.Stack())
		deletePidFile(flagPidfile)
		os.Exit(ExitCritical)
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

func getMinimalTLSConfig() *tls.Config {
	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	return (config)
}
