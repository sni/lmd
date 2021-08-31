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

	"github.com/lkarlslund/stringdedup"
	"github.com/sasha-s/go-deadlock"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

const (
	// VERSION contains the actual lmd version
	VERSION = "2.0.2"
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
	StatsTimerInterval = 60 * time.Second

	// HTTPClientTimeout sets the default HTTP client timeout
	HTTPClientTimeout = 30 * time.Second

	// BlockProfileRateInterval sets the profiling interval when started with -profile
	BlockProfileRateInterval = 10

	// GCPercentage sets gc level like GOGC environment
	GCPercentage = 30

	// DefaultFilePerm set default permissions for new files
	DefaultFilePerm = 0644

	// DefaultDirPerm set default permissions for new folders
	DefaultDirPerm = 0755

	// ThrukMultiBackendMinVersion is the minimum required thruk version
	ThrukMultiBackendMinVersion = 2.23
)

// ContextKey is a key used as context key
type ContextKey string

// available ContextKeys
const (
	CtxPeer    ContextKey = "peer"
	CtxClient  ContextKey = "client"
	CtxRequest ContextKey = "request"
)

// AvailableContextKeys set list of available log prefix objects
var AvailableContextKeys = []ContextKey{CtxPeer, CtxClient, CtxRequest}

// https://github.com/golang/go/issues/8005#issuecomment-190753527
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

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
var flagExport string
var flagImport string

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
	flag.Var(&flagCfgOption, "o", "override settings, ex.: -o Listen=:3333 -o Connections=name,address")
	flag.StringVar(&flagExport, "export", "", "export/snapshot data to file.")
	flag.StringVar(&flagImport, "import", "", "start lmd from export/snapshot and do not contact backends.")
}

func main() {
	// command line arguments
	setFlags()
	checkFlags()

	// make sure we log panics properly
	defer logPanicExit()

	if flagExport != "" {
		mainExport()
		os.Exit(0)
	}

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
	localConfig := finalFlagsConfig(false)

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

	osSignalChannel := buildSignalChannel()

	osSignalUsrChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalUsrChannel, syscall.SIGUSR1)
	signal.Notify(osSignalUsrChannel, syscall.SIGUSR2)

	lastMainRestart = time.Now().Unix()
	shutdownChannel := make(chan bool)
	waitGroupInit := &sync.WaitGroup{}
	waitGroupListener := &sync.WaitGroup{}
	waitGroupPeers := &sync.WaitGroup{}

	if len(localConfig.Listen) == 0 {
		log.Fatalf("no listeners defined")
	}

	if flagProfile != "" {
		log.Warnf("pprof profiler listening at %s", flagProfile)
	}

	once.Do(PrintVersion)
	localConfig.LogConfig()

	// initialize prometheus
	prometheusListener := initPrometheus(localConfig)

	var qStat *QueryStats
	if localConfig.LogQueryStats {
		log.Debugf("query stats enabled")
		qStat = NewQueryStats()
	}

	// start local listeners
	initializeListeners(localConfig, waitGroupListener, waitGroupInit, qStat)

	// start remote connections
	if flagImport != "" {
		mainImport(localConfig, waitGroupPeers, waitGroupInit, shutdownChannel, flagImport, osSignalChannel)
	} else {
		if len(localConfig.Connections) == 0 {
			log.Fatalf("no connections defined")
		}
		initializePeers(localConfig, waitGroupPeers, waitGroupInit, shutdownChannel)
	}

	if initChannel != nil {
		initChannel <- true
	}

	// make garbagabe collectore more aggressive
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(GCPercentage)
	}

	// just wait till someone hits ctrl+c or we have to reload
	statsTimer := time.NewTicker(StatsTimerInterval)
	for {
		select {
		case sig := <-osSignalChannel:
			return mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener, qStat)
		case sig := <-osSignalUsrChannel:
			mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener, qStat)
		case sig := <-mainSignalChannel:
			return mainSignalHandler(sig, shutdownChannel, waitGroupPeers, waitGroupListener, prometheusListener, qStat)
		case <-statsTimer.C:
			updateStatistics(qStat)
		}
	}
}

func buildSignalChannel() chan os.Signal {
	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, syscall.SIGHUP)
	signal.Notify(osSignalChannel, syscall.SIGTERM)
	signal.Notify(osSignalChannel, os.Interrupt)
	signal.Notify(osSignalChannel, syscall.SIGINT)
	return osSignalChannel
}

func mainImport(localConfig *Config, waitGroupPeers *sync.WaitGroup, waitGroupInit *sync.WaitGroup, shutdownChannel chan bool, importFile string, osSignalChannel chan os.Signal) {
	go func() {
		sig := <-osSignalChannel
		mainSignalHandler(sig, shutdownChannel, waitGroupPeers, nil, nil, nil)
		os.Exit(1)
	}()
	err := initializePeersWithImport(localConfig, waitGroupPeers, waitGroupInit, shutdownChannel, importFile)
	if err != nil {
		log.Fatalf("%s", err)
	}
}

func mainExport() {
	osSignalChannel := buildSignalChannel()
	go func() {
		<-osSignalChannel
		os.Exit(1)
	}()
	err := exportData(flagExport)
	if err != nil {
		log.Fatalf("export failed: %s", err)
	}
	log.Infof("exported %d peers successfully", len(PeerMapOrder))
}

func ApplyFlags(conf *Config) {
	if flagLogFile != "" {
		conf.LogFile = flagLogFile
	}
	if flagVerbose {
		conf.LogLevel = "Info"
	}
	if flagVeryVerbose {
		conf.LogLevel = "Debug"
	}
	if flagTraceVerbose {
		conf.LogLevel = "Trace"
	}
}

// Version returns the LMD version string
func Version() string {
	return fmt.Sprintf("%s (Build: %s, %s)", VERSION, Build, runtime.Version())
}

func initializeListeners(localConfig *Config, waitGroupListener *sync.WaitGroup, waitGroupInit *sync.WaitGroup, qStat *QueryStats) {
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
			l.Connection.Close()
		}
	}

	// open new listeners
	for _, listen := range localConfig.Listen {
		if l, ok := Listeners[listen]; ok {
			l.Lock.Lock()
			l.GlobalConfig = localConfig
			l.Lock.Unlock()
			ListenersNew[listen] = l
		} else {
			waitGroupInit.Add(1)
			l := NewListener(localConfig, listen, waitGroupInit, waitGroupListener, qStat)
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
				p.SetHTTPClient()
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

	if flagImport != "" && flagExport != "" {
		fmt.Printf("ERROR: cannot use import and export at the same time.")
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
	err := ioutil.WriteFile(path, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644)
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

func onExit(qStat *QueryStats) {
	deletePidFile(flagPidfile)
	if qStat != nil {
		close(qStat.In)
		qStat = nil
	}
	if flagCPUProfile != "" {
		pprof.StopCPUProfile()
		cpuProfileHandler.Close()
		log.Warnf("cpu profile written to: %s", flagCPUProfile)
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
		if strings.EqualFold(optname, "connections") {
			conVal := strings.SplitN(optvalue, ",", 2)
			if len(conVal) < 2 {
				log.Fatalf("ERROR: cannot parse connection, syntax is '-o Connection=name,address'")
			}
			con := Connection{
				Name:   conVal[0],
				ID:     conVal[0],
				Source: []string{conVal[1]},
			}
			localConfig.Connections = append(localConfig.Connections, con)
			continue
		}
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
					case reflect.Slice:
						v := interface2string(optvalue)
						f.Set(reflect.Append(f, reflect.ValueOf(*v)))
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

func mainSignalHandler(sig os.Signal, shutdownChannel chan bool, waitGroupPeers *sync.WaitGroup, waitGroupListener *sync.WaitGroup, prometheusListener io.Closer, qStat *QueryStats) (exitCode int) {
	switch sig {
	case syscall.SIGTERM:
		log.Infof("got sigterm, quiting gracefully")
		close(shutdownChannel)
		ListenersLock.Lock()
		for con, l := range Listeners {
			delete(Listeners, con)
			l.Connection.Close()
		}
		ListenersLock.Unlock()
		if prometheusListener != nil {
			prometheusListener.Close()
		}
		waitGroupListener.Wait()
		waitGroupPeers.Wait()
		onExit(qStat)
		return (0)
	case syscall.SIGINT:
		fallthrough
	case os.Interrupt:
		log.Infof("got sigint, quitting")
		close(shutdownChannel)
		ListenersLock.Lock()
		for con, l := range Listeners {
			delete(Listeners, con)
			l.Connection.Close()
		}
		ListenersLock.Unlock()
		if prometheusListener != nil {
			prometheusListener.Close()
		}
		// wait one second which should be enough for the listeners
		if waitGroupListener != nil {
			waitTimeout(waitGroupListener, time.Second)
		}
		if qStat != nil {
			onExit(qStat)
		}
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

// PrintVersion prints the version
func PrintVersion() {
	fmt.Printf("%s - version %s started with config %s\n", NAME, Version(), flagConfigFile)
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

func updateStatistics(qStat *QueryStats) {
	size := stringdedup.Size()
	promStringDedupCount.Set(float64(size))
	promStringDedupBytes.Set(float64(stringdedup.ByteCount()))
	promStringDedupIndexBytes.Set(float64(32 * size))
	if qStat != nil {
		qStat.LogTrigger <- true
	}
}

func getMinimalTLSConfig(localConfig *Config) *tls.Config {
	config := &tls.Config{
		InsecureSkipVerify: localConfig.SkipSSLCheck > 0,
	}
	if localConfig.TLSMinVersion != "" {
		tlsMinVersion, err := parseTLSMinVersion(localConfig.TLSMinVersion)
		if err != nil {
			config.MinVersion = tlsMinVersion
		}
	}
	return (config)
}

func fmtHTTPerr(req *http.Request, err error) string {
	if req != nil {
		return (fmt.Sprintf("%s: %v", req.URL.String(), err))
	}
	return (fmt.Sprintf("%v", err))
}

func finalFlagsConfig(stdoutLogging bool) *Config {
	localConfig := NewConfig(flagConfigFile)
	if stdoutLogging {
		localConfig.LogLevel = "info"
		localConfig.LogFile = "stdout"
	}
	applyArgFlags(flagCfgOption, localConfig)
	localConfig.ValidateConfig()
	ApplyFlags(localConfig)
	InitLogging(localConfig)
	localConfig.SetServiceAuthorization()
	localConfig.SetGroupAuthorization()
	return localConfig
}
