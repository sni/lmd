package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
	"os/signal"
	"syscall"
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
	Source string
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
var Objects *ObjectsType

var GlobalConfig Config
var flagVerbose bool
var flagVeryVerbose bool
var flagConfigFile string
var flagVersion bool

func main() {
	flag.StringVar(&flagConfigFile, "c", "lampd.ini", "set location for config file")
	flag.StringVar(&flagConfigFile, "config", "lampd.ini", "set location for config file")
	flag.BoolVar(&flagVerbose, "v", false, "enable verbose output")
	flag.BoolVar(&flagVerbose, "verbose", false, "enable verbose output")
	flag.BoolVar(&flagVeryVerbose, "vv", false, "enable very verbose output")
	flag.BoolVar(&flagVersion, "version", false, "print version and exit")
	flag.Parse()
	if flagVersion {
		fmt.Printf("%s - version %s (Build: %s)\n", NAME, VERSION, Build)
		os.Exit(2)
	}
	for {
		MainLoop()
	}
}

func MainLoop() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)

	if _, err := toml.DecodeFile(flagConfigFile, &GlobalConfig); err != nil {
		panic(err)
	}
	if flagVerbose {
		GlobalConfig.LogLevel = "Info"
	}
	if flagVeryVerbose {
		GlobalConfig.LogLevel = "Debug"
	}
	InitLogging(&GlobalConfig)

	// Set the backends to be used.
	DataStore = make(map[string]Peer)
	Objects = &ObjectsType{}
	InitObjects()

	// start local listeners
	for _, listen := range GlobalConfig.Listen {
		go localListener(listen)
	}

	// start remote connections
	for _, c := range GlobalConfig.Connections {
		p := NewPeer(&c)
		_, Exists := DataStore[c.Id]
		if Exists {
			log.Panicf("Duplicate id in connection list: %s", c.Id)
		}
		DataStore[c.Id] = *p
		p.Start()
	}

	fmt.Printf("%s - version %s (Build: %s) started\n", NAME, VERSION, Build)

	// just wait till someone hits ctrl+c or we have to reload
	for _ = range c {
		log.Infof("Reloading Config...")
	}
}
