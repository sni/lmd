package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"regexp"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

var testLogLevel = "Error"

func init() {
	// make tests faster if the listener does not wait that long to shutdown
	acceptInterval = 30 * time.Millisecond

	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	TestPeerWaitGroup = &sync.WaitGroup{}

	once.Do(PrintVersion)
}

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

func StartMockLivestatusSource(nr int, numHosts int, numServices int) (listen string) {
	startedChannel := make(chan bool)
	listen = fmt.Sprintf("mock%d.sock", nr)
	TestPeerWaitGroup.Add(1)

	dataFolder := "../t/data"
	if numHosts > 0 || numServices > 0 {
		// prepare data files
		tempFolder := prepareTmpData(dataFolder, numHosts, numServices)
		defer os.Remove(tempFolder)
		dataFolder = tempFolder
	}

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
				continue
			}

			if len(req.Filter) > 0 {
				conn.Write([]byte("200            3\n[]\n"))
				conn.Close()
				continue
			}

			dat, err := ioutil.ReadFile(fmt.Sprintf("%s/%s.json", dataFolder, req.Table))
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

func prepareTmpData(dataFolder string, numHosts int, numServices int) (tempFolder string) {
	tempFolder, err := ioutil.TempDir("", "mockdata")
	if err != nil {
		panic("failed to create temp data folder: " + err.Error())
	}
	// read existing json files and extend hosts and services
	for name, table := range Objects.Tables {
		file, err := os.Create(fmt.Sprintf("%s/%s.json", tempFolder, name))
		if err != nil {
			panic("failed to create temp file: " + err.Error())
		}
		template, err := os.Open(fmt.Sprintf("%s/%s.json", dataFolder, name))
		if name == "hosts" || name == "services" {
			dat, _ := ioutil.ReadFile(fmt.Sprintf("%s/%s.json", dataFolder, name))
			removeFirstLine := regexp.MustCompile("^200.*")
			dat = removeFirstLine.ReplaceAll(dat, []byte{})
			var raw = [][]interface{}{}
			err = json.Unmarshal(dat, &raw)
			if err != nil {
				panic("failed to decode: " + err.Error())
			}
			num := len(raw)
			last := raw[num-1]
			newData := [][]interface{}{}
			if name == "hosts" {
				nameIndex := table.GetColumn("name").Index
				for x := 1; x < numHosts; x++ {
					newObj := make([]interface{}, len(last))
					copy(newObj, last)
					newObj[nameIndex] = fmt.Sprintf("%s_%d", "testhost", x)
					newData = append(newData, newObj)
				}
			}
			if name == "services" {
				nameIndex := table.GetColumn("host_name").Index
				descIndex := table.GetColumn("description").Index
				for x := 1; x < numHosts; x++ {
					for y := 1; y < numServices/numHosts; y++ {
						newObj := make([]interface{}, len(last))
						copy(newObj, last)
						newObj[nameIndex] = fmt.Sprintf("%s_%d", "testhost", x)
						newObj[descIndex] = fmt.Sprintf("%s_%d", "testsvc", y)
						newData = append(newData, newObj)
						if len(newData) == numServices {
							break
						}
					}
					if len(newData) == numServices {
						break
					}
				}
			}

			encoded, _ := json.Marshal(newData)
			err = file.Close()
			encoded = append([]byte(fmt.Sprintf("%d %11d\n", 200, len(encoded))), encoded...)
			ioutil.WriteFile(fmt.Sprintf("%s/%s.json", tempFolder, name), encoded, 0644)
		} else {
			io.Copy(file, template)
			err = file.Close()
		}
		if err != nil {
			panic("failed to create temp file: " + err.Error())
		}
	}
	return
}

var TestPeerWaitGroup *sync.WaitGroup

func StartMockMainLoop(sockets []string) {
	var testConfig = `
Loglevel = "` + testLogLevel + `"
Listen = ["test.sock"]
ListenPrometheus = ":50999"

`
	for i, socket := range sockets {
		testConfig += fmt.Sprintf("[[Connections]]\nname = \"MockCon\"\nid   = \"mockid%d\"\nsource = [\"%s\"]\n\n", i, socket)
	}

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
func StartTestPeer(numPeers int, numHosts int, numServices int) (peer *Peer) {
	sockets := []string{}
	for i := 0; i < numPeers; i++ {
		listen := StartMockLivestatusSource(i, numHosts, numServices)
		sockets = append(sockets, listen)
	}
	StartMockMainLoop(sockets)

	testPeerShutdownChannel := make(chan bool)
	peer = NewPeer(Connection{Source: []string{"doesnotexist", "test.sock"}, Name: "Test", ID: "testid"}, TestPeerWaitGroup, testPeerShutdownChannel)
	peer.InitAllTables()

	// wait till backend is available
	retries := 0
	for {
		res, err := peer.QueryString("GET backends\nColumns: status last_error\nFilter: status = 0\nResponseHeader: fixed16\n\n")
		if err == nil && len(res) == len(sockets) && len(res[0]) > 0 && res[0][0].(float64) == 0 {
			break
		}
		// recheck every 100ms
		time.Sleep(100 * time.Millisecond)
		retries++
		if retries > 30 {
			if err != nil {
				panic("backend never came online: " + err.Error())
			} else {
				panic("backend never came online")
			}
		}
	}

	return
}

func StopTestPeer(peer *Peer) {
	// stop the mainloop
	mainSignalChannel <- syscall.SIGTERM
	// stop the test peer
	peer.Stop()
	// stop the mock servers
	peer.QueryString("COMMAND [0] MOCK_EXIT")
	// wait till all has stoped
	waitTimeout(TestPeerWaitGroup, 5*time.Second)
}

func PauseTestPeers(peer *Peer) {
	peer.PauseUpdates()
	for _, p := range DataStore {
		p.PauseUpdates()
	}
}

func CheckOpenFilesLimit(b *testing.B, minimum uint64) {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		b.Skip("skipping test, cannot fetch open files limit.")
	}
	if rLimit.Cur < minimum {
		b.Skip(fmt.Sprintf("skipping test, open files limit too low, need at least %d, current: %d", minimum, rLimit.Cur))
	}
}
