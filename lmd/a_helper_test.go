package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

var testLogLevel = "Error"

// GlobalTestConfig contains the global configuration (after config files have been parsed)
var GlobalTestConfig Config

func init() {
	setDefaults(&GlobalTestConfig)
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
	listen = fmt.Sprintf("mock%d_%d.sock", nr, time.Now().Nanosecond())
	TestPeerWaitGroup.Add(1)

	// prepare data files
	dataFolder := prepareTmpData("../t/data", nr, numHosts, numServices)

	go func() {
		os.Remove(listen)
		l, err := net.Listen("unix", listen)
		if err != nil {
			panic(err.Error())
		}
		defer func() {
			os.Remove(listen)
			l.Close()
			os.RemoveAll(dataFolder)
			TestPeerWaitGroup.Done()
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
				switch req.Command {
				case "COMMAND [0] MOCK_EXIT":
					conn.Close()
					return
				case "COMMAND [0] test_ok":
				case "COMMAND [0] test_broken":
					conn.Write([]byte("400: command broken\n"))
				}
				conn.Close()
				continue
			}

			if len(req.Filter) > 0 || len(req.Stats) > 0 {
				conn.Write([]byte("200           3\n[]\n"))
				conn.Close()
				continue
			}

			dat, err := ioutil.ReadFile(fmt.Sprintf("%s/%s.json", dataFolder, req.Table))
			if err != nil {
				panic("could not read file: " + err.Error())
			}
			conn.Write([]byte(fmt.Sprintf("%d %11d\n", 200, len(dat))))
			conn.Write(dat)
			conn.Close()
		}
	}()
	<-startedChannel
	return
}

// prepareTmpData creates static json files which will be used to generate mocked backend response
// if numServices is  0, empty test data will be used
func prepareTmpData(dataFolder string, nr int, numHosts int, numServices int) (tempFolder string) {
	tempFolder, err := ioutil.TempDir("", fmt.Sprintf("mockdata%d_", nr))
	if err != nil {
		panic("failed to create temp data folder: " + err.Error())
	}
	// read existing json files and extend hosts and services
	for name, table := range Objects.Tables {
		if table.Virtual != nil {
			continue
		}
		file, err := os.Create(fmt.Sprintf("%s/%s.json", tempFolder, name))
		if err != nil {
			panic("failed to create temp file: " + err.Error())
		}
		template, err := os.Open(fmt.Sprintf("%s/%s.json", dataFolder, name))
		if err != nil {
			panic("failed to open temp file: " + err.Error())
		}
		switch {
		case numServices == 0 && name != "status":
			io.WriteString(file, "[]\n")
			err = file.Close()
		case name == "hosts" || name == "services":
			err = file.Close()
			prepareTmpDataHostService(dataFolder, tempFolder, table, numHosts, numServices)
		default:
			io.Copy(file, template)
			err = file.Close()
		}
		if err != nil {
			panic("failed to create temp file: " + err.Error())
		}
	}
	return
}

func prepareTmpDataHostService(dataFolder string, tempFolder string, table *Table, numHosts int, numServices int) {
	name := table.Name
	dat, _ := ioutil.ReadFile(fmt.Sprintf("%s/%s.json", dataFolder, name))
	var raw = [][]interface{}{}
	err := json.Unmarshal(dat, &raw)
	if err != nil {
		panic("failed to decode: " + err.Error())
	}
	num := len(raw)
	last := raw[num-1]

	// create hosts and services names
	serviceCount := 0
	hosts := make([]struct {
		hostname string
		services []string
	}, numHosts)
	for x := 0; x < numHosts; x++ {
		hosts[x].hostname = fmt.Sprintf("%s_%d", "testhost", x+1)
		hosts[x].services = make([]string, 0)
		for y := 1; y <= numServices/numHosts; y++ {
			service := fmt.Sprintf("%s_%d", "testsvc", y)
			hosts[x].services = append(hosts[x].services, service)
			serviceCount++
			if serviceCount == numServices {
				break
			}
		}
	}

	newData := [][]interface{}{}
	count := 0
	if name == "hosts" {
		nameIndex := GetTestColumnIndex(table, "name")
		servicesIndex := GetTestColumnIndex(table, "services")
		for x := range hosts {
			host := hosts[x]
			var newObj []interface{}
			count++
			if count > num {
				newObj = make([]interface{}, len(last))
				copy(newObj, last)
			} else {
				newObj = make([]interface{}, len(raw[count-1]))
				copy(newObj, raw[count-1])
			}
			newObj[nameIndex] = host.hostname
			newObj[servicesIndex] = host.services
			newData = append(newData, newObj)
		}
	}
	if name == "services" {
		nameIndex := GetTestColumnIndex(table, "host_name")
		descIndex := GetTestColumnIndex(table, "description")
		count := 0
		for x := range hosts {
			host := hosts[x]
			for y := range host.services {
				var newObj []interface{}
				count++
				if count >= num {
					newObj = make([]interface{}, len(last))
					copy(newObj, last)
				} else {
					newObj = make([]interface{}, len(raw[count]))
					copy(newObj, raw[count])
				}
				newObj[nameIndex] = host.hostname
				newObj[descIndex] = host.services[y]
				newData = append(newData, newObj)
			}
		}
	}

	buf := new(bytes.Buffer)
	buf.Write([]byte("["))
	for i, row := range newData {
		enc, _ := json.Marshal(row)
		buf.Write(enc)
		if i < len(newData)-1 {
			buf.Write([]byte(",\n"))
		}
	}
	buf.Write([]byte("]\n"))
	ioutil.WriteFile(fmt.Sprintf("%s/%s.json", tempFolder, name), buf.Bytes(), 0644)
}

var TestPeerWaitGroup *sync.WaitGroup

func StartMockMainLoop(sockets []string, extraConfig string) {
	var testConfig = `
Loglevel = "` + testLogLevel + `"

`
	testConfig += extraConfig
	if !strings.Contains(testConfig, "Listen ") {
		testConfig += `Listen = ["test.sock"]
		`
	}

	for i, socket := range sockets {
		testConfig += fmt.Sprintf("[[Connections]]\nname = \"MockCon-%s\"\nid   = \"mockid%d\"\nsource = [\"%s\"]\n\n", socket, i, socket)
	}

	err := ioutil.WriteFile("test.ini", []byte(testConfig), 0644)
	if err != nil {
		panic(err.Error())
	}

	toml.DecodeFile("test.ini", &GlobalTestConfig)
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

// StartTestPeer just call StartTestPeerExtra
// if numServices is  0, empty test data will be used
func StartTestPeer(numPeers int, numHosts int, numServices int) *Peer {
	return (StartTestPeerExtra(numPeers, numHosts, numServices, ""))
}

// StartTestPeerExtra starts:
//  - a mock livestatus server which responds from status json
//  - a main loop which has the mock server(s) as backend
// It returns a peer with the "mainloop" connection configured
// if numServices is  0, empty test data will be used
func StartTestPeerExtra(numPeers int, numHosts int, numServices int, extraConfig string) (peer *Peer) {
	sockets := []string{}
	for i := 0; i < numPeers; i++ {
		listen := StartMockLivestatusSource(i, numHosts, numServices)
		sockets = append(sockets, listen)
	}
	StartMockMainLoop(sockets, extraConfig)

	testPeerShutdownChannel := make(chan bool)
	peer = NewPeer(&GlobalTestConfig, &Connection{Source: []string{"doesnotexist", "test.sock"}, Name: "Test", ID: "testid"}, TestPeerWaitGroup, testPeerShutdownChannel)

	// wait till backend is available
	retries := 0
	for {
		res, _, err := peer.QueryString("GET backends\nColumns: status last_error\nFilter: status = 0\nResponseHeader: fixed16\n\n")
		if err == nil && len(*res) == len(sockets) && len((*res)[0]) > 0 && (*res)[0][0].(float64) == 0 && (*res)[0][1].(string) == "" {
			break
		}
		// recheck every 100ms
		time.Sleep(100 * time.Millisecond)
		retries++
		if retries > 100 {
			if err != nil {
				panic("backend never came online: " + err.Error())
			} else {
				panic("backend never came online")
			}
		}
	}

	// fill tables
	peer.InitAllTables()

	return
}

func StopTestPeer(peer *Peer) (err error) {
	// stop the mock servers
	peer.QueryString("COMMAND [0] MOCK_EXIT")
	// stop the mainloop
	mainSignalChannel <- syscall.SIGTERM
	// stop the test peer
	peer.Stop()
	// wait till all has stoped
	if waitTimeout(TestPeerWaitGroup, 10*time.Second) {
		err = fmt.Errorf("timeout while waiting for peers to stop")
	}
	return
}

func PauseTestPeers(peer *Peer) {
	peer.Stop()
	for _, p := range PeerMap {
		p.Stop()
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

func StartHTTPMockServer(t *testing.T) (*httptest.Server, func()) {
	var data struct {
		// Credential string  // unused
		Options struct {
			// Action string // unused
			Args []string
			Sub  string
		}
	}
	nr := 0
	numHosts := 5
	numServices := 10
	dataFolder := prepareTmpData("../t/data", nr, numHosts, numServices)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := json.Unmarshal([]byte(r.PostFormValue("data")), &data)
		if err != nil {
			t.Fatalf("failed to parse request: %s", err.Error())
		}
		if data.Options.Sub == "_raw_query" {
			req, _, err := NewRequest(bufio.NewReader(strings.NewReader(data.Options.Args[0])))
			if err != nil {
				t.Fatalf("failed to parse request: %s", err.Error())
			}
			switch {
			case req.Table != "":
				dat, err := ioutil.ReadFile(fmt.Sprintf("%s/%s.json", dataFolder, req.Table))
				if err != nil {
					panic("could not read file: " + err.Error())
				}
				fmt.Fprintf(w, "%d %11d\n", 200, len(dat))
				fmt.Fprint(w, string(dat))
				return
			case req.Command == "COMMAND [0] test_ok":
				if v, ok := r.Header["Accept"]; ok && v[0] == "application/livestatus" {
					fmt.Fprintln(w, "")
				} else {
					fmt.Fprintln(w, "{\"rc\":0,\"version \":\"2.20\",\"branch\":\"1\",\"output\":[null,0,\"\",null]}")
				}
				return
			case req.Command == "COMMAND [0] test_broken":
				if v, ok := r.Header["Accept"]; ok && v[0] == "application/livestatus" {
					fmt.Fprintln(w, "400: command broken")
				} else {
					fmt.Fprintln(w, "{\"rc\":0,\"version\":\"2.20\",\"branch\":\"1\",\"output\":[null,0,\"400: command broken\",null]}")
				}
				return
			}
		}
		if data.Options.Sub == "get_processinfo" {
			fmt.Fprintln(w, "{\"rc\":0, \"version\":\"2.20\", \"output\":[]}")
			return
		}
		t.Fatalf("unknown test request: %v", r)
	}))
	cleanup := func() {
		ts.Close()
		os.RemoveAll(dataFolder)
	}
	return ts, cleanup
}

func GetHTTPMockServerPeer(t *testing.T) (peer *Peer, cleanup func()) {
	ts, cleanup := StartHTTPMockServer(t)
	testPeerShutdownChannel := make(chan bool)
	peer = NewPeer(&GlobalTestConfig, &Connection{Source: []string{ts.URL}, Name: "Test", ID: "testid"}, TestPeerWaitGroup, testPeerShutdownChannel)
	return
}

func GetTestColumnIndex(table *Table, name string) int {
	for i := range table.Columns {
		if table.Columns[i].Name == name {
			return i
		}
	}
	panic(name + " not found")
}
