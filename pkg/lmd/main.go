package lmd

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	hpprof "net/http/pprof"
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

	"github.com/OneOfOne/xxhash"
	"github.com/klauspost/compress/zstd"
	"github.com/lkarlslund/stringdedup"
	"github.com/sasha-s/go-deadlock"
)

const (
	// VERSION contains the actual lmd version.
	VERSION = "2.4.0"
	// NAME defines the name of this project.
	NAME = "lmd"

	// AuthLoose is used for loose authorization when host contacts are granted all services.
	AuthLoose = "loose"

	// AuthStrict is used for strict authorization when host contacts are not granted all services.
	AuthStrict = "strict"

	// ExitCritical is used to non-ok exits.
	ExitCritical = 2

	// ExitUnknown is used as exit code for help.
	ExitUnknown = 3

	// StatsTimerInterval sets the interval at which statistics will be updated.
	StatsTimerInterval = 60 * time.Second

	// HTTPClientTimeout sets the default HTTP client timeout.
	HTTPClientTimeout = 30 * time.Second

	// BlockProfileRateInterval sets the profiling interval when started with -profile.
	BlockProfileRateInterval = 10

	// GCPercentage sets gc level like GOGC environment.
	GCPercentage = 30

	// DefaultFilePerm set default permissions for new files.
	DefaultFilePerm = 0o644

	// DefaultDirPerm set default permissions for new folders.
	DefaultDirPerm = 0o755

	// DefaultMaxQueryFilter sets the default number of max query filters.
	DefaultMaxQueryFilter = 1000

	// ThrukMultiBackendMinVersion is the minimum required thruk version.
	ThrukMultiBackendMinVersion = 2.23
)

// ContextKey is a key used as context key.
type ContextKey string

// available ContextKeys.
const (
	CtxPeer    ContextKey = "peer"
	CtxClient  ContextKey = "client"
	CtxRequest ContextKey = "request"
)

// https://github.com/golang/go/issues/8005#issuecomment-190753527
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// ConnectionType contains the different connection types.
type ConnectionType uint8

// sets available connection types.
const (
	ConnTypeTCP ConnectionType = iota
	ConnTypeUnix
	ConnTypeTLS
	ConnTypeHTTP
)

func (c ConnectionType) String() string {
	switch c {
	case ConnTypeTCP:
		return "tcp"
	case ConnTypeUnix:
		return "unix"
	case ConnTypeTLS:
		return "tls"
	case ConnTypeHTTP:
		return "http"
	}
	log.Panicf("not implemented: %#v", c)

	return ""
}

var dedup = stringdedup.New(xxhash.Checksum32)

type Daemon struct {
	waitGroupListener *sync.WaitGroup
	Listeners         map[string]*Listener // Listeners stores if we started a listener
	Config            *Config              // reference to global config object
	PeerMapLock       *deadlock.RWMutex    // PeerMapLock is the lock for the PeerMap map
	waitGroupPeers    *sync.WaitGroup
	ListenersLock     *deadlock.RWMutex // ListenersLock is the lock for the Listeners map
	nodeAccessor      *Nodes            // nodeAccessor manages cluster nodes and starts/stops peers.
	shutdownChannel   chan bool
	cpuProfileHandler *os.File
	PeerMap           map[string]*Peer // PeerMap contains a map of available remote peers.
	waitGroupInit     *sync.WaitGroup
	initChannel       chan bool
	mainSignalChannel chan os.Signal
	PeerMapOrder      []string
	qStat             *QueryStats
	flags             struct {
		flagLogFile      string
		flagPidfile      string
		flagProfile      string
		flagCPUProfile   string
		flagMemProfile   string
		flagExport       string
		flagImport       string
		flagConfigFile   configFiles
		flagCfgOption    ArrayFlags
		flagDeadlock     int
		flagVerbose      bool
		flagVeryVerbose  bool
		flagTraceVerbose bool
		flagVersion      bool
	}
	lastMainRestart          float64
	defaultReqestParseOption ParseOptions
}

type ArrayFlags struct {
	list []string
}

func (i *ArrayFlags) String() string {
	return strings.Join(i.list, ", ")
}

func (i *ArrayFlags) Set(value string) error {
	i.list = append(i.list, value)

	return nil
}

func (i *ArrayFlags) Value() []string {
	return i.list
}

// Objects contains the static definition of all available tables and columns.
var Objects *ObjectsType

var (
	once           sync.Once
	reHTTPHostPort = regexp.MustCompile("^(https?)://(.*?):(.*)")
)

// initialize objects structure.
func init() {
	InitObjects()
}

func NewLMDInstance() (lmd *Daemon) {
	lmd = &Daemon{
		lastMainRestart:          currentUnixTime(),
		mainSignalChannel:        make(chan os.Signal),
		initChannel:              nil,
		PeerMapLock:              new(deadlock.RWMutex),
		PeerMap:                  make(map[string]*Peer),
		PeerMapOrder:             make([]string, 0),
		Listeners:                make(map[string]*Listener),
		ListenersLock:            new(deadlock.RWMutex),
		waitGroupInit:            &sync.WaitGroup{},
		waitGroupListener:        &sync.WaitGroup{},
		waitGroupPeers:           &sync.WaitGroup{},
		shutdownChannel:          make(chan bool),
		defaultReqestParseOption: ParseOptimize,
	}

	return
}

func (lmd *Daemon) setFlags() {
	flag.Var(&lmd.flags.flagConfigFile, "c", "set location for config file, can be specified multiple times, can contain globs like lmd.ini.d/*.ini")
	flag.Var(&lmd.flags.flagConfigFile, "config", "set location for config file, can be specified multiple times, can contain globs like lmd.ini.d/*.ini")
	flag.StringVar(&lmd.flags.flagPidfile, "pidfile", "", "set path to pidfile")
	flag.StringVar(&lmd.flags.flagLogFile, "logfile", "", "override logfile from the configuration file")
	flag.BoolVar(&lmd.flags.flagVerbose, "v", false, "enable verbose output")
	flag.BoolVar(&lmd.flags.flagVerbose, "verbose", false, "enable verbose output")
	flag.BoolVar(&lmd.flags.flagVeryVerbose, "vv", false, "enable very verbose output")
	flag.BoolVar(&lmd.flags.flagTraceVerbose, "vvv", false, "enable trace output")
	flag.BoolVar(&lmd.flags.flagVersion, "version", false, "print version and exit")
	flag.BoolVar(&lmd.flags.flagVersion, "V", false, "print version and exit")
	flag.StringVar(&lmd.flags.flagProfile, "debug-profiler", "", "start pprof profiler on this port, ex. :6060")
	flag.StringVar(&lmd.flags.flagCPUProfile, "cpuprofile", "", "write cpu profile to `file`")
	flag.StringVar(&lmd.flags.flagMemProfile, "memprofile", "", "write memory profile to `file`")
	flag.IntVar(&lmd.flags.flagDeadlock, "debug-deadlock", 0, "enable deadlock detection with given timeout")
	flag.Var(&lmd.flags.flagCfgOption, "o", "override settings, ex.: -o Listen=:3333 -o Connections=name,address")
	flag.StringVar(&lmd.flags.flagExport, "export", "", "export/snapshot data to file.")
	flag.StringVar(&lmd.flags.flagImport, "import", "", "start lmd from export/snapshot and do not contact backends.")
}

var Build string

func Main(build string) {
	Build = build

	lmd := NewLMDInstance()
	// command line arguments
	lmd.setFlags()
	lmd.checkFlags()
	lmd.initDebugOptions()

	// make sure we log panics properly
	defer lmd.logPanicExit()

	if lmd.flags.flagExport != "" {
		lmd.mainExport()
		os.Exit(0) //nolint:gocritic // ok, this defer is only relevant for panics
	}

	for {
		exitCode := lmd.mainLoop()
		if exitCode > 0 {
			log.Infof("lmd shutdown complete")
			os.Exit(exitCode)
		}
		// make it possible to call main() from tests without exiting the tests
		if exitCode == 0 {
			break
		}
	}
}

func (lmd *Daemon) mainLoop() (exitCode int) {
	localConfig := lmd.finalFlagsConfig(false)
	lmd.Config = localConfig

	CompressionLevel = zstd.EncoderLevelFromZstd(localConfig.CompressionLevel)
	CompressionMinimumSize = localConfig.CompressionMinimumSize

	// put some configuration settings into metrics
	promPeerUpdateInterval.Set(float64(localConfig.UpdateInterval))
	promPeerFullUpdateInterval.Set(float64(localConfig.FullUpdateInterval))
	promCompressionLevel.Set(float64(CompressionLevel))
	promCompressionMinimumSize.Set(float64(CompressionMinimumSize))
	promSyncIsExecuting.Set(float64(interface2int8(localConfig.SyncIsExecuting)))
	promSaveTempRequests.Set(float64(interface2int8(localConfig.SaveTempRequests)))
	promBackendKeepAlive.Set(float64(interface2int8(localConfig.BackendKeepAlive)))

	osSignalChannel := buildSignalChannel()

	osSignalUsrChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalUsrChannel, syscall.SIGUSR1)
	signal.Notify(osSignalUsrChannel, syscall.SIGUSR2)

	lmd.lastMainRestart = currentUnixTime()
	lmd.shutdownChannel = make(chan bool)
	lmd.waitGroupInit = &sync.WaitGroup{}
	lmd.waitGroupListener = &sync.WaitGroup{}
	lmd.waitGroupPeers = &sync.WaitGroup{}

	if len(localConfig.Listen) == 0 {
		lmd.cleanFatalf("no listeners defined")
	}

	if lmd.flags.flagProfile != "" {
		log.Warnf("pprof profiler listening at http://%s/debug/pprof/", lmd.flags.flagProfile)
	}

	once.Do(lmd.PrintVersion)
	log.Infof("%s - version %s started with config %s", NAME, Version(), lmd.flags.flagConfigFile)
	localConfig.LogConfig()
	ctx := context.Background()

	// initialize prometheus
	prometheusListener := initPrometheus(lmd)

	if localConfig.LogQueryStats {
		log.Debugf("query stats enabled")
		lmd.qStat = NewQueryStats()
	}

	// start local listeners
	lmd.initializeListeners()

	// start remote connections
	if lmd.flags.flagImport != "" {
		lmd.mainImport(lmd.flags.flagImport, osSignalChannel)
	} else {
		if len(localConfig.Connections) == 0 {
			lmd.cleanFatalf("no connections defined")
		}
		lmd.initializePeers(ctx)
	}

	if lmd.initChannel != nil {
		lmd.initChannel <- true
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
			return lmd.mainSignalHandler(sig, prometheusListener)
		case sig := <-osSignalUsrChannel:
			lmd.mainSignalHandler(sig, prometheusListener)
		case sig := <-lmd.mainSignalChannel:
			return lmd.mainSignalHandler(sig, prometheusListener)
		case <-statsTimer.C:
			updateStatistics(lmd.qStat)
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

func (lmd *Daemon) mainImport(importFile string, osSignalChannel chan os.Signal) {
	go func() {
		sig := <-osSignalChannel
		lmd.mainSignalHandler(sig, nil)
		os.Exit(1)
	}()
	err := initializePeersWithImport(lmd, importFile)
	if err != nil {
		lmd.cleanFatalf("%s", err)
	}
}

func (lmd *Daemon) mainExport() {
	osSignalChannel := buildSignalChannel()
	go func() {
		<-osSignalChannel
		os.Exit(1)
	}()
	err := exportData(lmd)
	if err != nil {
		lmd.cleanFatalf("export failed: %s", err)
	}
	log.Infof("exported %d peers successfully", len(lmd.PeerMapOrder))
}

func (lmd *Daemon) ApplyFlags(conf *Config) {
	if lmd.flags.flagLogFile != "" {
		conf.LogFile = lmd.flags.flagLogFile
	}
	if lmd.flags.flagVerbose {
		conf.LogLevel = "Info"
	}
	if lmd.flags.flagVeryVerbose {
		conf.LogLevel = "Debug"
	}
	if lmd.flags.flagTraceVerbose {
		conf.LogLevel = "Trace"
	}
}

// Version returns the LMD version string.
func Version() string {
	return fmt.Sprintf("%s (Build: %s, %s)", VERSION, Build, runtime.Version())
}

func (lmd *Daemon) initializeListeners() {
	ListenersNew := make(map[string]*Listener)

	// close all listeners which are no longer defined
	lmd.ListenersLock.Lock()
	for con, listen := range lmd.Listeners {
		found := false
		for _, listen := range lmd.Config.Listen {
			if listen == con {
				found = true

				break
			}
		}
		if !found {
			delete(lmd.Listeners, con)
			listen.Stop()
		}
	}

	// open new listeners
	for _, listen := range lmd.Config.Listen {
		if l, ok := lmd.Listeners[listen]; ok {
			ListenersNew[listen] = l
		} else {
			lmd.waitGroupInit.Add(1)
			l := NewListener(lmd, listen)
			ListenersNew[listen] = l
		}
	}

	lmd.Listeners = ListenersNew
	lmd.ListenersLock.Unlock()
}

func (lmd *Daemon) initializePeers(ctx context.Context) {
	// This node's http address (http://*:1234), to be used as address pattern
	var nodeListenAddress string
	for _, listen := range lmd.Config.Listen {
		parts := reHTTPHostPort.FindStringSubmatch(listen)
		if len(parts) != 4 {
			continue
		}
		nodeListenAddress = listen

		break
	}

	// Get rid of obsolete peers (removed from config)
	lmd.PeerMapLock.Lock()
	for peerKey, peer := range lmd.PeerMap {
		found := false // id exists
		for i := range lmd.Config.Connections {
			if lmd.Config.Connections[i].ID == peerKey {
				found = true
			}
		}
		if !found {
			peer.Stop()
			peer.data.Store(nil)
			lmd.PeerMapRemove(peerKey)
		}
	}
	lmd.PeerMapLock.Unlock()

	// Create/set Peer objects
	PeerMapNew := make(map[string]*Peer)
	PeerMapOrderNew := make([]string, 0)
	backends := make([]string, 0, len(lmd.Config.Connections))
	for i := range lmd.Config.Connections {
		conn := lmd.Config.Connections[i]
		// Keep peer if connection settings unchanged
		var peer *Peer
		lmd.PeerMapLock.RLock()
		v, ok := lmd.PeerMap[conn.ID]
		lmd.PeerMapLock.RUnlock()
		if ok {
			peer = v
			if conn.Equals(v.config) {
				peer.waitGroup = lmd.waitGroupPeers
				peer.shutdownChannel = lmd.shutdownChannel
				peer.SetHTTPClient()
			} else {
				peer.Stop()
				peer.data.Store(nil)
				lmd.PeerMapRemove(conn.ID)
			}
		}

		// Create new peer otherwise
		if peer == nil {
			peer = NewPeer(lmd, &conn)
		}

		// Check for duplicate id
		for _, b := range backends {
			if b == conn.ID {
				lmd.cleanFatalf("Duplicate id in connection list: %s", conn.ID)
			}
		}
		backends = append(backends, conn.ID)

		// Put new or modified peer in map
		PeerMapNew[conn.ID] = peer
		PeerMapOrderNew = append(PeerMapOrderNew, conn.ID)
		// Peer started later in node redistribution routine
	}

	lmd.PeerMapLock.Lock()
	lmd.PeerMapOrder = PeerMapOrderNew
	lmd.PeerMap = PeerMapNew
	lmd.PeerMapLock.Unlock()

	// Node accessor
	nodeAddresses := lmd.Config.Nodes
	lmd.nodeAccessor = NewNodes(lmd, nodeAddresses, nodeListenAddress)
	lmd.nodeAccessor.Initialize(ctx) // starts peers in single mode
	lmd.nodeAccessor.Start(ctx)      // nodes loop starts/stops peers in cluster mode
}

func (lmd *Daemon) checkFlags() {
	flag.Parse()
	if lmd.flags.flagVersion {
		fmt.Fprintf(os.Stdout, "%s - version %s\n", NAME, Version())
		os.Exit(ExitCritical)
	}

	if lmd.flags.flagDeadlock <= 0 {
		deadlock.Opts.Disable = true
	} else {
		deadlock.Opts.Disable = false
		deadlock.Opts.DeadlockTimeout = time.Duration(lmd.flags.flagDeadlock) * time.Second
		deadlock.Opts.LogBuf = NewLogWriter("Error")
	}

	if lmd.flags.flagImport != "" && lmd.flags.flagExport != "" {
		fmt.Fprintf(os.Stderr, "ERROR: cannot use import and export at the same time.")
		os.Exit(ExitCritical)
	}

	createPidFile(lmd.flags.flagPidfile)
}

func (lmd *Daemon) initDebugOptions() {
	if lmd.flags.flagProfile != "" {
		if lmd.flags.flagCPUProfile != "" || lmd.flags.flagMemProfile != "" {
			log.Errorf("ERROR: either use --debug-profile or --cpu/memprofile, not both")
			os.Exit(ExitCritical)
		}
		runtime.SetBlockProfileRate(BlockProfileRateInterval)
		runtime.SetMutexProfileFraction(BlockProfileRateInterval)
		go func() {
			// make sure we log panics properly
			defer lmd.logPanicExit()
			_ = hpprof.Handler("/debug/pprof/")
			err := http.ListenAndServe(lmd.flags.flagProfile, http.DefaultServeMux)
			if err != nil {
				log.Warnf("http.ListenAndServe finished with: %e", err)
			}
		}()

		log.Warnf("pprof profiler listening at http://%s/debug/pprof/", lmd.flags.flagProfile)
	}

	if lmd.flags.flagCPUProfile != "" {
		runtime.SetBlockProfileRate(BlockProfileRateInterval)
		cpuProfileHandler, err := os.Create(lmd.flags.flagCPUProfile)
		if err != nil {
			log.Errorf("ERROR: could not create CPU profile: %s", err.Error())
			os.Exit(ExitCritical)
		}
		if err := pprof.StartCPUProfile(cpuProfileHandler); err != nil {
			log.Errorf("ERROR: could not start CPU profile: %s", err.Error())
			os.Exit(ExitCritical)
		}
		lmd.cpuProfileHandler = cpuProfileHandler
	}
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
	err := os.WriteFile(path, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Could not write pidfile: %s\n", err.Error())
		os.Exit(ExitCritical)
	}
}

// checkPidFile returns false if pidfile is stale.
func checkPidFile(path string) bool {
	dat, err := os.ReadFile(path)
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

// wraps log.Fatalf but removes the pid file and such...
func (lmd *Daemon) cleanFatalf(format string, args ...interface{}) {
	lmd.onExit()
	log.Errorf(format, args...)

	os.Exit(ExitCritical)
}

func (lmd *Daemon) onExit() {
	deletePidFile(lmd.flags.flagPidfile)
	if lmd.qStat != nil {
		close(lmd.qStat.in)
		lmd.qStat = nil
	}
	if lmd.flags.flagCPUProfile != "" {
		pprof.StopCPUProfile()
		lmd.cpuProfileHandler.Close()
		log.Warnf("cpu profile written to: %s", lmd.flags.flagCPUProfile)
	}
}

func (lmd *Daemon) applyArgFlags(opts ArrayFlags, localConfig *Config) {
	ps := reflect.ValueOf(localConfig)
	val := ps.Elem()
	typeOfV := val.Type()

	for _, opt := range opts.list {
		tmp := strings.SplitN(opt, "=", 2)
		if len(tmp) < 2 {
			lmd.cleanFatalf("ERROR: cannot parse option %s, syntax is '-o ConfigOption=Value'", opt)
		}
		optname := tmp[0]
		optvalue := tmp[1]
		if strings.EqualFold(optname, "connections") {
			conVal := strings.SplitN(optvalue, ",", 2)
			if len(conVal) < 2 {
				lmd.cleanFatalf("ERROR: cannot parse connection, syntax is '-o Connection=name,address'")
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
		for i := range val.NumField() {
			cfgname := typeOfV.Field(i).Name
			if strings.EqualFold(cfgname, optname) {
				field := val.Field(i)
				if field.IsValid() && field.CanSet() {
					switch field.Kind() {
					case reflect.Int, reflect.Int64:
						field.SetInt(interface2int64(optvalue))
					case reflect.String:
						field.SetString(optvalue)
					case reflect.Bool:
						field.SetBool(interface2bool(optvalue))
					case reflect.Slice:
						v := interface2string(optvalue)
						field.Set(reflect.Append(field, reflect.ValueOf(*v)))
					default:
						lmd.cleanFatalf("ERROR: cannot set option %s, type %s is not supported", cfgname, field.Kind())
					}
					found = true

					break
				}
				lmd.cleanFatalf("ERROR: cannot set option %s", cfgname)
			}
		}
		if !found {
			lmd.cleanFatalf("ERROR: no such option %s", optname)
		}
	}
}

// NewLMDHTTPClient creates a http.Client with the given tls.Config.
func NewLMDHTTPClient(tlsConfig *tls.Config, proxy string) *http.Client {
	transport := &http.Transport{
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
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	netClient := &http.Client{
		Timeout:   HTTPClientTimeout,
		Transport: transport,
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

func (lmd *Daemon) mainSignalHandler(sig os.Signal, prometheusListener io.Closer) (exitCode int) {
	switch sig {
	case syscall.SIGTERM:
		log.Infof("got sigterm, quiting gracefully")
		close(lmd.shutdownChannel)
		lmd.ListenersLock.Lock()
		for con, l := range lmd.Listeners {
			delete(lmd.Listeners, con)
			l.Stop()
		}
		lmd.ListenersLock.Unlock()
		if prometheusListener != nil {
			prometheusListener.Close()
		}
		lmd.waitGroupListener.Wait()
		lmd.waitGroupPeers.Wait()
		lmd.onExit()

		return (0)
	case syscall.SIGINT, os.Interrupt:
		log.Infof("got sigint, quitting")
		close(lmd.shutdownChannel)
		lmd.ListenersLock.Lock()
		for con, l := range lmd.Listeners {
			delete(lmd.Listeners, con)
			l.Stop()
		}
		lmd.ListenersLock.Unlock()
		if prometheusListener != nil {
			prometheusListener.Close()
		}
		// wait one second which should be enough for the listeners
		if lmd.waitGroupListener != nil {
			waitTimeout(context.TODO(), lmd.waitGroupListener, time.Second)
		}
		if lmd.qStat != nil {
			lmd.onExit()
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
		if lmd.flags.flagMemProfile == "" {
			log.Errorf("requested memory profile, but flag -memprofile missing")

			return (0)
		}
		file, err := os.Create(lmd.flags.flagMemProfile)
		if err != nil {
			log.Errorf("could not create memory profile: %s", err.Error())
		}
		defer file.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(file); err != nil {
			log.Errorf("could not write memory profile: %s", err.Error())
		}
		log.Warnf("memory profile written to: %s", lmd.flags.flagMemProfile)

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
func waitTimeout(ctx context.Context, waitGroup *sync.WaitGroup, timeout time.Duration) bool {
	if timeout < time.Millisecond*10 {
		log.Panic("bogus timeout")
	}
	stopChan := make(chan struct{})
	timer := time.NewTimer(timeout)
	go func() {
		defer close(stopChan)
		waitGroup.Wait()
	}()
	select {
	case <-stopChan:
		timer.Stop()

		return false // completed normally
	case <-timer.C:
		return true // timed out
	case <-ctx.Done():
		return true // aborted
	}
}

// PrintVersion prints the version.
func (lmd *Daemon) PrintVersion() {
	fmt.Fprintf(os.Stdout, "%s - version %s started with config %s\n", NAME, Version(), lmd.flags.flagConfigFile)
}

func (lmd *Daemon) logPanicExit() {
	if r := recover(); r != nil {
		log.Errorf("Panic: %s", r)
		log.Errorf("Version: %s", Version())
		log.Errorf("%s", debug.Stack())
		deletePidFile(lmd.flags.flagPidfile)
		os.Exit(ExitCritical)
	}
}

func timeOrNever(timestamp float64) string {
	if timestamp > 0 {
		sec, dec := math.Modf(timestamp)

		return (time.Unix(int64(sec), int64(dec*float64(time.Second))).String())
	}

	return "never"
}

// PeerMapRemove deletes a peer from PeerMap and PeerMapOrder.
func (lmd *Daemon) PeerMapRemove(peerID string) {
	// find id in order array
	for i, id := range lmd.PeerMapOrder {
		if id == peerID {
			lmd.PeerMapOrder = append(lmd.PeerMapOrder[:i], lmd.PeerMapOrder[i+1:]...)

			break
		}
	}
	delete(lmd.PeerMap, peerID)
}

// completePeerHTTPAddr returns autocompleted address for peer
// it appends /thruk/cgi-bin/remote.cgi or parts of it.
func completePeerHTTPAddr(addr string) string {
	addr = strings.TrimSuffix(addr, "cgi-bin/remote.cgi")
	addr = strings.TrimSuffix(addr, "/")
	addr = strings.TrimSuffix(addr, "thruk")
	addr = strings.TrimSuffix(addr, "/")

	return addr + "/thruk/cgi-bin/remote.cgi"
}

// byteCountBinary returns human readable byte string.
func byteCountBinary(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func updateStatistics(qStat *QueryStats) {
	size := dedup.Size()
	promStringDedupCount.Set(float64(size))
	promStringDedupBytes.Set(float64(dedup.Statistics().BytesInMemory))
	promStringDedupIndexBytes.Set(float64(32 * size))
	if qStat != nil {
		qStat.logTrigger <- true
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

	return err.Error()
}

func (lmd *Daemon) finalFlagsConfig(stdoutLogging bool) *Config {
	localConfig := NewConfig(lmd.flags.flagConfigFile)
	if stdoutLogging {
		localConfig.LogLevel = "info"
		localConfig.LogFile = "stdout"
	}
	lmd.applyArgFlags(lmd.flags.flagCfgOption, localConfig)
	localConfig.ValidateConfig()
	lmd.ApplyFlags(localConfig)
	InitLogging(localConfig)
	localConfig.SetServiceAuthorization()
	localConfig.SetGroupAuthorization()

	return localConfig
}

// returns the current unix time with sub second precision.
func currentUnixTime() float64 {
	return float64(time.Now().UnixNano()) / float64(time.Second)
}
