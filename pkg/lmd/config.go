package lmd

import (
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	jsoniter "github.com/json-iterator/go"
)

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
	TLSServerName  string
	Proxy          string
	Flags          []string
	NoConfigTool   int `toml:"noconfigtool"` // skip adding config tool to sites query
}

// Equals checks if two connection objects are identical.
func (c *Connection) Equals(other *Connection) bool {
	equal := c.ID == other.ID
	equal = equal && c.Name == other.Name
	equal = equal && c.Auth == other.Auth
	equal = equal && c.RemoteName == other.RemoteName
	equal = equal && c.Section == other.Section
	equal = equal && c.TLSCertificate == other.TLSCertificate
	equal = equal && c.TLSServerName == other.TLSServerName
	equal = equal && c.TLSKey == other.TLSKey
	equal = equal && c.TLSCA == other.TLSCA
	equal = equal && c.TLSSkipVerify == other.TLSSkipVerify
	equal = equal && c.NoConfigTool == other.NoConfigTool
	equal = equal && strings.Join(c.Source, ":") == strings.Join(other.Source, ":")
	equal = equal && strings.Join(c.Flags, ":") == strings.Join(other.Flags, ":")

	return equal
}

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

// Config defines the available configuration options from supplied config files.
type Config struct {
	Listen                     []string     `toml:"Listen"`
	Nodes                      []string     `toml:"Nodes"`
	TLSCertificate             string       `toml:"TLSCertificate"`
	TLSKey                     string       `toml:"TLSKey"`
	TLSClientPems              []string     `toml:"TLSClientPems"`
	UpdateInterval             int64        `toml:"Updateinterval"`
	FullUpdateInterval         int64        `toml:"FullUpdateInterval"`
	Connections                []Connection `toml:"Connections"`
	LogFile                    string       `toml:"LogFile"`
	LogLevel                   string       `toml:"LogLevel"`
	LogSlowQueryThreshold      int          `toml:"LogSlowQueryThreshold"`
	LogHugeQueryThreshold      int          `toml:"LogHugeQueryThreshold"`
	LogQueryStats              bool         `toml:"LogQueryStats"`
	ConnectTimeout             int          `toml:"ConnectTimeout"`
	NetTimeout                 int          `toml:"NetTimeout"`
	ListenTimeout              int          `toml:"ListenTimeout"`
	SaveTempRequests           bool         `toml:"SaveTempRequests"`
	ListenPrometheus           string       `toml:"ListenPrometheus"`
	SkipSSLCheck               int          `toml:"SkipSSLCheck"`
	IdleTimeout                int64        `toml:"IdleTimeout"`
	IdleInterval               int64        `toml:"IdleInterval"`
	StaleBackendTimeout        int          `toml:"StaleBackendTimeout"`
	BackendKeepAlive           bool         `toml:"BackendKeepAlive"`
	ServiceAuthorization       string       `toml:"ServiceAuthorization"`
	GroupAuthorization         string       `toml:"GroupAuthorization"`
	SyncIsExecuting            bool         `toml:"SyncIsExecuting"`
	CompressionMinimumSize     int          `toml:"CompressionMinimumSize"`
	CompressionLevel           int          `toml:"CompressionLevel"`
	MaxClockDelta              float64      `toml:"MaxClockDelta"`
	UpdateOffset               int64        `toml:"UpdateOffset"`
	TLSMinVersion              string       `toml:"TLSMinVersion"`
	MaxParallelPeerConnections int          `toml:"MaxParallelPeerConnections"`
	MaxQueryFilter             int          `toml:"MaxQueryFilter"`
}

// NewConfig reads all config files.
// It returns a Config object.
func NewConfig(files []string) *Config {
	conf := Config{
		UpdateInterval:             7,
		FullUpdateInterval:         0,
		LogLevel:                   "Info",
		LogSlowQueryThreshold:      5,
		LogHugeQueryThreshold:      100,
		ConnectTimeout:             30,
		NetTimeout:                 120,
		ListenTimeout:              60,
		SaveTempRequests:           true,
		IdleTimeout:                120,
		IdleInterval:               1800,
		StaleBackendTimeout:        30,
		BackendKeepAlive:           true,
		ServiceAuthorization:       AuthLoose,
		GroupAuthorization:         AuthStrict,
		SyncIsExecuting:            true,
		CompressionMinimumSize:     DefaultCompressionMinimumSize,
		CompressionLevel:           -1,
		MaxClockDelta:              10,
		UpdateOffset:               3,
		TLSMinVersion:              "tls1.1",
		MaxParallelPeerConnections: 3,
		MaxQueryFilter:             DefaultMaxQueryFilter,
	}

	// combine listeners from all files
	allListeners := make([]string, 0)
	allConnections := make([]Connection, 0)
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
				fmt.Fprintf(os.Stderr, "ERROR: could not load configuration from %s: %s\n", configFile, err.Error())
				os.Exit(ExitUnknown)
			}
			allListeners = append(allListeners, conf.Listen...)
			conf.Listen = []string{}
			allConnections = append(allConnections, conf.Connections...)
			conf.Connections = []Connection{}
		}
	}
	conf.Listen = allListeners
	conf.Connections = allConnections

	for i := range conf.Connections {
		for j := range conf.Connections[i].Source {
			if strings.HasPrefix(conf.Connections[i].Source[j], "http") {
				conf.Connections[i].Source[j] = completePeerHTTPAddr(conf.Connections[i].Source[j])
			}
		}
	}

	return &conf
}

func (conf *Config) ValidateConfig() {
	DefaultConfig := NewConfig([]string{})
	if conf.NetTimeout <= 0 {
		log.Warnf("config: NetTimeout invalid, value must be greater than 0")
		conf.NetTimeout = DefaultConfig.NetTimeout
	}
	if conf.ConnectTimeout <= 0 {
		log.Warnf("config: ConnectTimeout invalid, value must be greater than 0")
		conf.ConnectTimeout = DefaultConfig.ConnectTimeout
	}
	if conf.ListenTimeout <= 0 {
		log.Warnf("config: ListenTimeout invalid, value must be greater than 0")
		conf.ListenTimeout = DefaultConfig.ListenTimeout
	}
	if conf.UpdateInterval <= 0 {
		log.Warnf("config: Updateinterval invalid, value must be greater than 0")
		conf.UpdateInterval = DefaultConfig.UpdateInterval
	}
	if conf.FullUpdateInterval < 0 {
		log.Warnf("config: FullUpdateInterval invalid, value must be greater than 0")
		conf.FullUpdateInterval = 0
	}
	if conf.IdleInterval <= 0 {
		log.Warnf("config: IdleInterval invalid, value must be greater than 0")
		conf.IdleInterval = DefaultConfig.IdleInterval
	}
	if conf.IdleTimeout <= 0 {
		log.Warnf("config: IdleTimeout invalid, value must be greater than 0")
		conf.IdleTimeout = DefaultConfig.IdleTimeout
	}
	if conf.StaleBackendTimeout <= 0 {
		log.Warnf("config: StaleBackendTimeout invalid, value must be greater than 0")
		conf.StaleBackendTimeout = DefaultConfig.StaleBackendTimeout
	}
	if conf.LogSlowQueryThreshold <= 0 {
		log.Warnf("config: LogSlowQueryThreshold invalid, value must be greater than 0")
		conf.LogSlowQueryThreshold = DefaultConfig.LogSlowQueryThreshold
	}
	if conf.LogHugeQueryThreshold <= 0 {
		log.Warnf("config: LogHugeQueryThreshold invalid, value must be greater than 0")
		conf.LogHugeQueryThreshold = DefaultConfig.LogHugeQueryThreshold
	}
	if conf.CompressionMinimumSize <= 0 {
		log.Warnf("config: CompressionMinimumSize invalid, value must be greater than 0")
		conf.CompressionMinimumSize = DefaultConfig.CompressionMinimumSize
	}
	if conf.MaxClockDelta < 0 {
		log.Warnf("config: MaxClockDelta invalid, value must be greater than 0")
		conf.MaxClockDelta = 10
	}
	if conf.UpdateOffset <= 0 {
		log.Warnf("config: UpdateOffset invalid, value must be greater than 0")
		conf.UpdateOffset = 3
	}
	_, err := parseTLSMinVersion(conf.TLSMinVersion)
	if err != nil {
		log.Warnf("%s", err)
	}
}

func (conf *Config) SetServiceAuthorization() {
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

func (conf *Config) SetGroupAuthorization() {
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

func (conf *Config) LogConfig() {
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

func parseTLSMinVersion(version string) (tlsminversion uint16, err error) {
	tlsminversion = 0
	switch strings.ToLower(version) {
	case "":
		tlsminversion = 0
	case "tls10", "tls1.0":
		tlsminversion = tls.VersionTLS10
	case "tls11", "tls1.1":
		tlsminversion = tls.VersionTLS11
	case "tls12", "tls1.2":
		tlsminversion = tls.VersionTLS12
	case "tls13", "tls1.3":
		tlsminversion = tls.VersionTLS13
	default:
		err = fmt.Errorf("cannot parse %s into tls version valid values are: tls1.0, tls1.1, tls1.2, tls1.3", version)
	}

	return
}
