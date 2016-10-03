package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"regexp"
	"sync"
	"time"

	"syscall"

	"github.com/BurntSushi/toml"
)

func init() {
	// make tests faster if the listener does not wait that long to shutdown
	acceptInterval = 30 * time.Millisecond

	InitLogging(&Config{LogLevel: "Panic", LogFile: "stderr"})

	TestPeerWaitGroup = &sync.WaitGroup{}

	once.Do(PrintVersion)
}

const testConfig = `
Loglevel = "Panic"
Listen = ["test.sock"]
ListenPrometheus = ":50999"

[[Connections]]
name = "MockCon"
id   = "id1"
source = ["mock.sock"]
`

func assertEq(exp, got interface{}) error {
	if !reflect.DeepEqual(exp, got) {
		return fmt.Errorf("\nWanted \n%#v\nGot\n%#v", exp, got)
	}
	return nil
}

func assertLike(exp string, got string) error {
	regex, err := regexp.Compile(exp)
	if err != nil {
		panic(err.Error())
	}
	if !regex.MatchString(got) {
		return fmt.Errorf("\nWanted \n%#v\nGot\n%#v", exp, got)
	}
	return nil
}

func StartMockLivestatusSource() {
	startedChannel := make(chan bool)
	listen := "mock.sock"
	TestPeerWaitGroup.Add(1)
	go func() {
		os.Remove(listen)
		l, err := net.Listen("unix", listen)
		if err != nil {
			panic(err.Error())
		}
		defer func() {
			l.Close()
			TestPeerWaitGroup.Done()
			os.Remove(listen)
		}()
		startedChannel <- true
		for {
			conn, err := l.Accept()
			if err != nil {
				panic(err.Error())
			}

			req, err := ParseRequest(conn)
			if err != nil {
				panic(err.Error())
			}

			if req.Command != "" {
				conn.Close()
				if req.Command == "COMMAND [0] MOCK_EXIT" {
					return
				}
				break
			}

			if len(req.Filter) > 0 {
				conn.Write([]byte("200            3\n[]\n"))
				conn.Close()
				break
			}

			dat, err := ioutil.ReadFile("../t/data/" + req.Table + ".json")
			if err != nil {
				panic("could not read file: " + err.Error())
			}
			conn.Write([]byte(dat))
			conn.Close()
		}
	}()
	<-startedChannel
	return
}

var TestPeerWaitGroup *sync.WaitGroup

func StartMockMainLoop() {
	err := ioutil.WriteFile("test.ini", []byte(testConfig), 0644)
	if err != nil {
		panic(err.Error())
	}

	toml.DecodeFile("test.ini", &GlobalConfig)
	mainSignalChannel = make(chan os.Signal)
	startedChannel := make(chan bool)

	go func() {
		flagConfigFile = configFiles{"test.ini"}
		TestPeerWaitGroup.Add(1)
		startedChannel <- true
		mainLoop(mainSignalChannel)
		TestPeerWaitGroup.Done()
		os.Remove("test.ini")
	}()
	<-startedChannel
}

// StartTestPeer starts:
//  - a mock livestatus server which responds from status json
//  - a main loop which has the mock server as backend
// It returns a peer which the "mainloop" connection configured
func StartTestPeer() (peer *Peer) {
	StartMockLivestatusSource()
	StartMockMainLoop()

	testPeerShutdownChannel := make(chan bool)
	peer = NewPeer(Connection{Source: []string{"doesnotexist", "test.sock"}, Name: "Test", ID: "id0"}, TestPeerWaitGroup, testPeerShutdownChannel)
	peer.Start()

	// wait till backend is available
	retries := 0
	for {
		res, err := peer.QueryString("GET backends\nColumns: status last_error\nResponseHeader: fixed16\n\n")
		if err == nil && len(res) > 0 && len(res[0]) > 0 && res[0][0].(float64) == 0 {
			break
		}
		// recheck every 100ms
		time.Sleep(100 * time.Millisecond)
		retries++
		if retries > 300 {
			panic("backend never came online")
		}
	}

	return
}

func StopTestPeer(peer *Peer) {
	// stop the mainloop
	mainSignalChannel <- syscall.SIGTERM
	// stop the test peer
	peer.Stop()
	// stop the mock server
	peer.QueryString("COMMAND [0] MOCK_EXIT")
	// wait till all has stoped
	waitTimeout(TestPeerWaitGroup, 5*time.Second)
}
