package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

const testLogLevel = "Error"   // set to Trace for debugging
const testLogTarget = "stderr" // set to /tmp/logfile...

func init() {
	lmd := createTestLMDInstance()
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: testLogTarget})

	once.Do(lmd.PrintVersion)

	// make ctrl+c work
	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, syscall.SIGTERM)
	signal.Notify(osSignalChannel, os.Interrupt)
	signal.Notify(osSignalChannel, syscall.SIGINT)
	go func() {
		<-osSignalChannel
		os.Exit(1)
	}()
}

func assertEq(exp, got interface{}) error {
	if !reflect.DeepEqual(exp, got) {
		return fmt.Errorf("\nWanted \n%#v\nGot\n%#v", exp, got)
	}
	return nil
}

func assertNeq(exp, got interface{}) error {
	if reflect.DeepEqual(exp, got) {
		return fmt.Errorf("\n%#v\nIs equal to\n%#v", exp, got)
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

func StartMockLivestatusSource(lmd *LMDInstance, nr int, numHosts int, numServices int) (listen string) {
	startedChannel := make(chan bool)
	listen = fmt.Sprintf("mock%d_%d.sock", nr, time.Now().Nanosecond())
	mockLog := logWith("mock_ls", listen)

	// prepare data files
	dataFolder := prepareTmpData("../t/data", nr, numHosts, numServices)

	go func() {
		mockLog.Debugf("starting listener on: %s", listen)
		os.Remove(listen)
		l, err := net.Listen("unix", listen)
		if err != nil {
			mockLog.Errorf("failed: %s", err)
			panic(err.Error())
		}
		clientConns := &sync.WaitGroup{}
		defer func() {
			if waitTimeout(clientConns, 5*time.Second) {
				log.Errorf("timeout while for mock connections to finish")
			}
			os.Remove(listen)
			mockLog.Debugf("shuting down")
			os.RemoveAll(dataFolder)
			mockLog.Debugf("shutdown complete")
		}()
		startedChannel <- true

		for {
			conn, err := l.Accept()
			if err != nil {
				// probably closed
				return
			}
			clientConns.Add(1)
			go func(conn net.Conn) {
				defer func() {
					clientConns.Done()
				}()
				if handleMockConnection(lmd, conn, dataFolder, mockLog) {
					l.Close()
					return
				}
			}(conn)
		}
	}()
	<-startedChannel
	mockLog.Debugf("ready")
	return
}

func handleMockConnection(lmd *LMDInstance, conn net.Conn, dataFolder string, mockLog *LogPrefixer) (closeServer bool) {
	closeServer = false
	req, err := ParseRequest(context.TODO(), lmd, conn)
	if err != nil {
		mockLog.Errorf("failed: %s", err)
		panic(err.Error())
	}

	if req.Command != "" {
		switch req.Command {
		case "COMMAND [0] MOCK_EXIT":
			mockLog.Debugf("got exit command")
			closeServer = true
		case "COMMAND [0] test_ok":
		case "COMMAND [0] test_broken":
			_checkErr2(conn.Write([]byte("400: command broken\n")))
		}
		_checkErr(conn.Close())
		return
	}

	mockLog.Debugf("request: %s", req.Table.String())
	if req.Table == TableColumns {
		// make the peer detect dependency columns
		b, err := json.Marshal(getTestDataColumns(dataFolder))
		if err != nil {
			panic(err)
		}
		_checkErr2(fmt.Fprintf(conn, "200 %11d\n", len(b)+1))
		_checkErr2(conn.Write(b))
		_checkErr2(conn.Write([]byte("\n")))
		_checkErr(conn.Close())
		return
	}

	if len(req.Filter) > 0 || len(req.Stats) > 0 {
		_checkErr2(conn.Write([]byte("200           3\n[]\n")))
		_checkErr(conn.Close())
		return
	}

	filename := fmt.Sprintf("%s/%s", dataFolder, req.Table.String())
	dat := readTestData(filename, req.Columns)
	_checkErr2(fmt.Fprintf(conn, "%d %11d\n", 200, len(dat)))
	_checkErr2(conn.Write(dat))
	_checkErr(conn.Close())
	mockLog.Debugf("%s response written: %d bytes", req.Table.String(), len(dat))
	return
}

// prepareTmpData creates static json files which will be used to generate mocked backend response
// if numServices is  0, empty test data will be used
func prepareTmpData(dataFolder string, nr int, numHosts int, numServices int) (tempFolder string) {
	tempFolder, err := os.MkdirTemp("", fmt.Sprintf("mockdata%d_", nr))
	if err != nil {
		panic("failed to create temp data folder: " + err.Error())
	}
	// read existing json files and extend hosts and services
	for name, table := range Objects.Tables {
		if table.Virtual != nil {
			continue
		}
		file, err := os.Create(fmt.Sprintf("%s/%s.map", tempFolder, name.String()))
		if err != nil {
			panic("failed to create temp file: " + err.Error())
		}
		template, err := os.Open(fmt.Sprintf("%s/%s.json", dataFolder, name.String()))
		if err != nil {
			panic("failed to open temp file: " + err.Error())
		}
		switch {
		case numServices == 0 && name != TableStatus:
			_checkErr2(file.WriteString("[]\n"))
			err = file.Close()
		case name == TableHosts || name == TableServices:
			err = file.Close()
			prepareTmpDataHostService(dataFolder, tempFolder, table, numHosts, numServices)
		default:
			_checkErr2(io.Copy(file, template))
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
	dat, _ := os.ReadFile(fmt.Sprintf("%s/%s.json", dataFolder, name.String()))
	var raw = make([]map[string]interface{}, 0)
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

	newData := []map[string]interface{}{}
	if name == TableHosts {
		count := 0
		for x := range hosts {
			host := hosts[x]
			count++
			src := last
			if count <= num {
				src = raw[count-1]
			}
			newObj := make(map[string]interface{}, len(src))
			for key := range src {
				newObj[key] = src[key]
			}
			newObj["name"] = host.hostname
			newObj["alias"] = host.hostname + "_ALIAS"
			newObj["services"] = host.services
			newData = append(newData, newObj)
		}
	}
	if name == TableServices {
		count := 0
		for x := range hosts {
			host := hosts[x]
			for y := range host.services {
				count++
				src := last
				if count <= num {
					src = raw[count-1]
				}
				newObj := make(map[string]interface{}, len(src))
				for key := range src {
					newObj[key] = src[key]
				}
				newObj["host_name"] = host.hostname
				newObj["description"] = host.services[y]
				newData = append(newData, newObj)
			}
		}
	}

	buf := new(bytes.Buffer)
	_checkErr2(buf.WriteString("["))
	for i, row := range newData {
		enc, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		_checkErr2(buf.Write(enc))
		if i < len(newData)-1 {
			_checkErr2(buf.WriteString(",\n"))
		}
	}
	_checkErr2(buf.WriteString("]\n"))
	_checkErr(os.WriteFile(fmt.Sprintf("%s/%s.map", tempFolder, name.String()), buf.Bytes(), 0o644))
}

func StartMockMainLoop(lmd *LMDInstance, sockets []string, extraConfig string) {
	var testConfig = `
Loglevel       = "` + testLogLevel + `"
LogFile        = "` + testLogTarget + `"

`
	testConfig += extraConfig
	if !strings.Contains(testConfig, "Listen ") {
		testConfig += `Listen = ["test.sock"]
		`
	}

	for i, socket := range sockets {
		testConfig += fmt.Sprintf("[[Connections]]\nname = \"MockCon-%s\"\nid   = \"mockid%d\"\nsource = [%q]\n\n", socket, i, socket)
	}

	err := os.WriteFile("test.ini", []byte(testConfig), 0o644)
	if err != nil {
		panic(err.Error())
	}

	_checkErr2(toml.DecodeFile("test.ini", lmd.Config))

	lmd.initChannel = make(chan bool)
	go func() {
		lmd.flags.flagConfigFile = configFiles{"test.ini"}
		lmd.mainLoop()
	}()
	<-lmd.initChannel
}

// StartTestPeer just call StartTestPeerExtra
// if numServices is  0, empty test data will be used
func StartTestPeer(numPeers int, numHosts int, numServices int) (*Peer, func() error, *LMDInstance) {
	return (StartTestPeerExtra(numPeers, numHosts, numServices, ""))
}

// StartTestPeerExtra starts:
//   - a mock livestatus server which responds from status json
//   - a main loop which has the mock server(s) as backend
//
// It returns a peer with the "mainloop" connection configured
// if numServices is  0, empty test data will be used
func StartTestPeerExtra(numPeers int, numHosts int, numServices int, extraConfig string) (peer *Peer, cleanup func() error, mocklmd *LMDInstance) {
	if testLogTarget != "stderr" {
		os.Remove(testLogTarget)
	}
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: testLogTarget})
	mocklmd = createTestLMDInstance()
	sockets := []string{}
	for i := 0; i < numPeers; i++ {
		sockets = append(sockets, StartMockLivestatusSource(mocklmd, i, numHosts, numServices))
	}
	StartMockMainLoop(mocklmd, sockets, extraConfig)

	lmd := createTestLMDInstance()
	peer = NewPeer(lmd, &Connection{Source: []string{"doesnotexist", "test.sock"}, Name: "TestPeer", ID: "testid"})

	// wait till backend is available
	waitUntil := time.Now().Add(10 * time.Second)
	for {
		err := peer.InitAllTables()
		if err == nil {
			peer.Lock.RLock()
			peer.data.Lock.RLock()
			gotPeers := len(peer.data.tables[TableStatus].Data)
			peer.data.Lock.RUnlock()
			peer.Lock.RUnlock()
			if gotPeers == numPeers {
				break
			}
		}
		// recheck every 50ms
		time.Sleep(50 * time.Millisecond)
		if time.Now().After(waitUntil) {
			panicFailedStartup(mocklmd, peer, numPeers, err)
		}
	}

	cleanup = func() (err error) {
		os.Remove("test.ini")

		// stop the mock servers
		_, _, err = peer.QueryString("COMMAND [0] MOCK_EXIT")
		if err != nil {
			// may fail if already stopped
			log.Debugf("send query failed: %e", err)
			err = nil
		}
		// stop the mainloop
		mocklmd.mainSignalChannel <- syscall.SIGTERM
		// stop the test peer
		peer.Stop()
		// wait till all has stoped
		if waitTimeout(peer.lmd.waitGroupPeers, 10*time.Second) {
			err = fmt.Errorf("timeout while waiting for peers to stop")
		}
		if waitTimeout(mocklmd.waitGroupPeers, 10*time.Second) {
			err = fmt.Errorf("timeout while waiting for mock peers to stop")
		}
		if waitTimeout(mocklmd.waitGroupListener, 10*time.Second) {
			err = fmt.Errorf("timeout while waiting for mock listenern to stop")
		}
		return
	}

	return
}

func PauseTestPeers(peer *Peer) {
	peer.Stop()
	for id := range peer.lmd.PeerMap {
		p := peer.lmd.PeerMap[id]
		p.Stop()
	}
}

func CheckOpenFilesLimit(b *testing.B, minimum uint64) {
	b.Helper()
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		b.Skip("skipping test, cannot fetch open files limit.")
	}
	if rLimit.Cur < minimum {
		b.Skipf("skipping test, open files limit too low, need at least %d, current: %d", minimum, rLimit.Cur)
	}
}

func StartHTTPMockServer(t *testing.T, lmd *LMDInstance) (*httptest.Server, func()) {
	t.Helper()
	nr := 0
	numHosts := 5
	numServices := 10
	dataFolder := prepareTmpData("../t/data", nr, numHosts, numServices)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { httpMockHandler(t, w, r, lmd, dataFolder) }))
	cleanup := func() {
		ts.Close()
		os.RemoveAll(dataFolder)
	}
	return ts, cleanup
}

func httpMockHandler(t *testing.T, w io.Writer, r *http.Request, lmd *LMDInstance, dataFolder string) {
	t.Helper()
	var data struct {
		// Credential string  // unused
		Options struct {
			// Action string // unused
			Args []string
			Sub  string
		}
	}
	err := json.Unmarshal([]byte(r.PostFormValue("data")), &data)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}
	if data.Options.Sub == "_raw_query" {
		req, _, err := NewRequest(context.TODO(), lmd, bufio.NewReader(strings.NewReader(data.Options.Args[0])), ParseDefault)
		if err != nil {
			t.Fatalf("failed to parse request: %s", err.Error())
		}
		httpMockHandlerRaw(t, w, r, dataFolder, req)
		return
	}
	if data.Options.Sub == "get_processinfo" {
		fmt.Fprintln(w, "{\"rc\":0, \"version\":\"2.20\", \"output\":[]}")
		return
	}
	t.Fatalf("unknown test request: %v", r)
}

func httpMockHandlerRaw(t *testing.T, w io.Writer, r *http.Request, dataFolder string, req *Request) {
	t.Helper()

	switch {
	case req.Table == TableColumns:
		// make the peer detect dependency columns
		b, err := json.Marshal(getTestDataColumns(dataFolder))
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(w, "%d %11d\n", 200, len(b))
		fmt.Fprint(w, string(b))
	case req.Table != TableNone:
		filename := fmt.Sprintf("%s/%s", dataFolder, req.Table.String())
		dat := readTestData(filename, req.Columns)
		fmt.Fprintf(w, "%d %11d\n", 200, len(dat))
		fmt.Fprint(w, string(dat))
	case req.Command == "COMMAND [0] test_ok":
		if v, ok := r.Header["Accept"]; ok && v[0] == "application/livestatus" {
			fmt.Fprintln(w, "")
		} else {
			fmt.Fprintln(w, "{\"rc\":0,\"version \":\"2.20\",\"branch\":\"1\",\"output\":[null,0,\"\",null]}")
		}
	case req.Command == "COMMAND [0] test_broken":
		if v, ok := r.Header["Accept"]; ok && v[0] == "application/livestatus" {
			fmt.Fprintln(w, "400: command broken")
		} else {
			fmt.Fprintln(w, "{\"rc\":0,\"version\":\"2.20\",\"branch\":\"1\",\"output\":[null,0,\"400: command broken\",null]}")
		}
	}
}

func GetHTTPMockServerPeer(t *testing.T, lmd *LMDInstance) (peer *Peer, cleanup func()) {
	t.Helper()
	ts, cleanup := StartHTTPMockServer(t, lmd)
	peer = NewPeer(lmd, &Connection{Source: []string{ts.URL}, Name: "TestPeer", ID: "testid"})
	return
}

func convertTestDataMapToList(filename string, columns []string) []byte {
	dat, err := os.ReadFile(filename + ".map")
	if err != nil {
		panic("could not read file: " + err.Error())
	}
	hashedData := make([]map[string]interface{}, 0)
	err = json.Unmarshal(dat, &hashedData)
	if err != nil {
		panic("could not parse file: " + err.Error())
	}

	result := make([]byte, 0)
	result = append(result, []byte("[")...)
	for k, rowIn := range hashedData {
		rowOut := make([]interface{}, len(columns))
		for i, col := range columns {
			val, ok := rowIn[col]
			if !ok {
				panic(fmt.Sprintf("missing column %s in testdata for file %s.map", col, filename))
			}
			rowOut[i] = val
		}
		rowBytes, err := json.Marshal(rowOut)
		if err != nil {
			panic("could not convert row to json: " + err.Error())
		}
		result = append(result, rowBytes...)
		if k < len(hashedData)-1 {
			result = append(result, []byte(",\n")...)
		}
	}
	result = append(result, []byte("]\n")...)

	file, err := os.Create(filename + ".json")
	if err != nil {
		panic("failed to write json back to file: " + err.Error())
	}
	_, err = file.Write(result)
	if err != nil {
		panic("failed to write json back to file: " + err.Error())
	}
	err = file.Close()
	if err != nil {
		panic("failed to write json back to file: " + err.Error())
	}

	return result
}

func readTestData(filename string, columns []string) []byte {
	dat, err := os.ReadFile(filename + ".json")
	if os.IsNotExist(err) {
		dat = convertTestDataMapToList(filename, columns)
	} else if err != nil {
		panic("could not read file: " + err.Error())
	}
	return dat
}

func getTestDataColumns(dataFolder string) (columns [][]string) {
	matches, _ := filepath.Glob(dataFolder + "/*.map")
	for _, match := range matches {
		tableName := strings.TrimSuffix(path.Base(match), ".map")
		raw, err := os.ReadFile(match)
		if err != nil {
			panic("failed to read json file: " + err.Error())
		}
		dat := make([]map[string]interface{}, 0)
		err = json.Unmarshal(raw, &dat)
		if err != nil {
			panic("failed to read json file: " + err.Error())
		}
		if len(dat) == 0 {
			continue
		}
		for col := range dat[0] {
			columns = append(columns, []string{tableName, col})
		}
	}
	return
}

func _checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func _checkErr2(_ interface{}, err error) {
	if err != nil {
		panic(err)
	}
}

func createTestLMDInstance() *LMDInstance {
	lmd := NewLMDInstance()
	lmd.Config = NewConfig([]string{})
	lmd.Config.ValidateConfig()
	lmd.flags.flagDeadlock = 15
	lmd.nodeAccessor = NewNodes(lmd, []string{}, "")
	return lmd
}

func panicFailedStartup(mocklmd *LMDInstance, peer *Peer, numPeers int, err error) {
	info := []string{fmt.Sprintf("backend(s) never came online, expected %d sites", numPeers)}
	if err != nil {
		info = append(info, err.Error())
	}

	data, _, err := peer.QueryString("GET sites\nColumns: name status last_error\n")
	if err != nil {
		info = append(info, err.Error())
		panic(strings.Join(info, "\n"))
	}
	info = append(info, fmt.Sprintf("got %d sites", len(data)))
	txt, err := json.Marshal(data)
	if err != nil {
		info = append(info, err.Error())
	} else {
		info = append(info, string(txt))
	}

	for _, peerKey := range mocklmd.PeerMapOrder {
		log.Errorf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> mock lmd")
		mocklmd.PeerMapLock.RLock()
		p := mocklmd.PeerMap[peerKey]
		mocklmd.PeerMapLock.RUnlock()
		p.logPeerStatus(log.Errorf)
	}

	panic(strings.Join(info, "\n"))
}

// make sure our test helpers are working correctly
func TestMock1(t *testing.T) {
	if testLogTarget != "stderr" {
		os.Remove(testLogTarget)
	}
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: testLogTarget})
	mocklmd := createTestLMDInstance()
	listen := StartMockLivestatusSource(mocklmd, 1, 1, 1)
	peer := NewPeer(mocklmd, &Connection{
		Source: []string{listen},
		Name:   "TestPeer",
	})

	err := peer.InitAllTables()
	if err != nil {
		t.Fatalf("init tables failed: %s", err.Error())
	}

	// tear down
	_, _, err = peer.QueryString("COMMAND [0] MOCK_EXIT")
	if err != nil {
		t.Fatalf("stopping mock livestatus source failed: %s", err.Error())
	}
}

func TestMock2(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 1, 1)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET status\nColumns: program_start\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}
