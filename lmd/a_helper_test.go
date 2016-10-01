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

const testConfig = `
Loglevel = "Panic"
Listen = ["test.sock"]

[[Connections]]
name = "Test"
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
	go func() {
		os.Remove("mock.sock")
		l, err := net.Listen("unix", "mock.sock")
		if err != nil {
			panic(err.Error())
		}
		defer l.Close()
		startedChannel <- true
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			req, err := ParseRequest(conn)
			if err != nil {
				panic(err.Error())
			}

			if req.Command != "" {
				conn.Close()
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

var TestPeerShutdownChannel chan bool
var waitGroup *sync.WaitGroup
var mainStarted = false

func SetupMainLoop() {
	err := ioutil.WriteFile("test.ini", []byte(testConfig), 0644)
	if err != nil {
		panic(err.Error())
	}

	StartMockLivestatusSource()

	go func() {
		if mainStarted {
			waitGroup.Add(1)
			mainLoop(mainSignalChannel)
			waitGroup.Done()
		} else {
			mainStarted = true
			os.Args[1] = "-config=test.ini"
			main()
		}
	}()

	toml.DecodeFile("test.ini", &GlobalConfig)
}

func SetupTestPeer() (peer *Peer) {
	waitGroup = &sync.WaitGroup{}
	SetupMainLoop()

	TestPeerShutdownChannel = make(chan bool)
	peer = NewPeer(Connection{Source: []string{"doesnotexist", "test.sock"}, Name: "Test", ID: "id0"}, waitGroup, TestPeerShutdownChannel)
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
		if retries > 100 {
			panic("backend never came online")
		}
	}

	return
}

func StopTestPeer() {
	mainSignalChannel <- syscall.SIGTERM
	TestPeerShutdownChannel <- true
	close(TestPeerShutdownChannel)
	waitTimeout(waitGroup, time.Second)
	os.Remove("test.ini")
	os.Remove("test.sock")
	os.Remove("mock.sock")
}
