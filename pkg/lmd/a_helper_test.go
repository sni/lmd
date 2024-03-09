package lmd

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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

const (
	testLogLevel  = "Error"  // set to Trace for debugging
	testLogTarget = "stderr" // set to /tmp/logfile...
)

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

func StartMockLivestatusSource(lmd *Daemon, srcNum, numHosts, numServices int) (listen string) {
	startedChannel := make(chan bool)
	listen = fmt.Sprintf("mock%d_%d.sock", srcNum, time.Now().Nanosecond())
	mockLog := logWith("mock_ls", listen)

	// prepare data files
	dataFolder := prepareTmpData("../../t/data", srcNum, numHosts, numServices)

	go func() {
		mockLog.Debugf("starting listener on: %s", listen)
		os.Remove(listen)
		listener, err := net.Listen("unix", listen)
		if err != nil {
			mockLog.Errorf("failed: %s", err)
			panic(err.Error())
		}
		clientConns := &sync.WaitGroup{}
		defer func() {
			if waitTimeout(context.TODO(), clientConns, 5*time.Second) {
				log.Errorf("timeout while for mock connections to finish")
			}
			os.Remove(listen)
			mockLog.Debugf("shuting down")
			os.RemoveAll(dataFolder)
			mockLog.Debugf("shutdown complete")
		}()
		startedChannel <- true

		for {
			conn, err := listener.Accept()
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
					listener.Close()

					return
				}
			}(conn)
		}
	}()
	<-startedChannel
	mockLog.Debugf("ready")

	return listen
}

func handleMockConnection(lmd *Daemon, conn net.Conn, dataFolder string, mockLog *LogPrefixer) (closeServer bool) {
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

		return closeServer
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

		return closeServer
	}

	if len(req.Filter) > 0 || len(req.Stats) > 0 {
		_checkErr2(conn.Write([]byte("200           3\n[]\n")))
		_checkErr(conn.Close())

		return closeServer
	}

	filename := fmt.Sprintf("%s/%s", dataFolder, req.Table.String())
	dat := readTestData(filename, req.Columns)
	_checkErr2(fmt.Fprintf(conn, "%d %11d\n", 200, len(dat)))
	_checkErr2(conn.Write(dat))
	_checkErr(conn.Close())
	mockLog.Debugf("%s response written: %d bytes", req.Table.String(), len(dat))

	return closeServer
}

// prepareTmpData creates static json files which will be used to generate mocked backend response
// if numServices is  0, empty test data will be used.
func prepareTmpData(dataFolder string, nr, numHosts, numServices int) (tempFolder string) {
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

	return tempFolder
}

func prepareTmpDataHostService(dataFolder, tempFolder string, table *Table, numHosts, numServices int) {
	name := table.Name
	dat, _ := os.ReadFile(fmt.Sprintf("%s/%s.json", dataFolder, name.String()))
	raw := make([]map[string]interface{}, 0)
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
	for curNum := 0; curNum < numHosts; curNum++ {
		hosts[curNum].hostname = fmt.Sprintf("%s_%d", "testhost", curNum+1)
		hosts[curNum].services = make([]string, 0)
		if curNum == 2 {
			hosts[curNum].hostname = strings.ToUpper(hosts[curNum].hostname)
			hosts[curNum].hostname = strings.ReplaceAll(hosts[curNum].hostname, "TESTHOST", "UPPER")
		}
		for y := 1; y <= numServices/numHosts; y++ {
			service := fmt.Sprintf("%s_%d", "testsvc", y)
			hosts[curNum].services = append(hosts[curNum].services, service)
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
		for numH := range hosts {
			host := hosts[numH]
			for numS := range host.services {
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
				newObj["description"] = host.services[numS]
				newData = append(newData, newObj)
			}
		}
	}

	buf := new(bytes.Buffer)
	_checkErr2(buf.WriteString("["))
	for num, row := range newData {
		enc, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		_checkErr2(buf.Write(enc))
		if num < len(newData)-1 {
			_checkErr2(buf.WriteString(",\n"))
		}
	}
	_checkErr2(buf.WriteString("]\n"))
	_checkErr(os.WriteFile(fmt.Sprintf("%s/%s.map", tempFolder, name.String()), buf.Bytes(), 0o644))
}

func StartMockMainLoop(lmd *Daemon, sockets []string, extraConfig string) {
	testConfig := `
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
// if numServices is  0, empty test data will be used.
func StartTestPeer(numPeers, numHosts, numServices int) (*Peer, func() error, *Daemon) {
	return (StartTestPeerExtra(numPeers, numHosts, numServices, ""))
}

// StartTestPeerExtra starts:
//   - a mock livestatus server which responds from status json
//   - a main loop which has the mock server(s) as backend
//
// It returns a peer with the "mainloop" connection configured
// if numServices is  0, empty test data will be used.
func StartTestPeerExtra(numPeers, numHosts, numServices int, extraConfig string) (peer *Peer, cleanup func() error, mocklmd *Daemon) {
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
		err := peer.InitAllTables(context.TODO())
		if err == nil {
			peer.lock.RLock()
			peer.data.Lock.RLock()
			gotPeers := len(peer.data.tables[TableStatus].Data)
			peer.data.Lock.RUnlock()
			peer.lock.RUnlock()
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
		if waitTimeout(context.TODO(), peer.lmd.waitGroupPeers, 10*time.Second) {
			err = fmt.Errorf("timeout while waiting for peers to stop")
		}
		if waitTimeout(context.TODO(), mocklmd.waitGroupPeers, 10*time.Second) {
			err = fmt.Errorf("timeout while waiting for mock peers to stop")
		}
		if waitTimeout(context.TODO(), mocklmd.waitGroupListener, 10*time.Second) {
			err = fmt.Errorf("timeout while waiting for mock listenern to stop")
		}

		return
	}

	return peer, cleanup, mocklmd
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

func StartHTTPMockServer(t *testing.T, lmd *Daemon) (testserver *httptest.Server, cleanup func()) {
	t.Helper()
	nr := 0
	numHosts := 5
	numServices := 10
	dataFolder := prepareTmpData("../../t/data", nr, numHosts, numServices)
	testserver = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { httpMockHandler(r.Context(), t, w, r, lmd, dataFolder) }))
	cleanup = func() {
		testserver.Close()
		os.RemoveAll(dataFolder)
	}

	return testserver, cleanup
}

func httpMockHandler(ctx context.Context, t *testing.T, wrt io.Writer, rdr *http.Request, lmd *Daemon, dataFolder string) {
	t.Helper()
	var data struct {
		// Credential string  // unused
		Options struct {
			// Action string // unused
			Args []string `json:"args"`
			Sub  string   `json:"sub"`
		} `json:"options"`
	}
	err := json.Unmarshal([]byte(rdr.PostFormValue("data")), &data)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}
	if data.Options.Sub == "_raw_query" {
		req, _, err := NewRequest(ctx, lmd, bufio.NewReader(strings.NewReader(data.Options.Args[0])), ParseDefault)
		if err != nil {
			t.Fatalf("failed to parse request: %s", err.Error())
		}
		httpMockHandlerRaw(t, wrt, rdr, dataFolder, req)

		return
	}
	if data.Options.Sub == "get_processinfo" {
		fmt.Fprintln(wrt, "{\"rc\":0, \"version\":\"2.20\", \"output\":[]}")

		return
	}
	t.Fatalf("unknown test request: %v", rdr)
}

func httpMockHandlerRaw(t *testing.T, wrt io.Writer, rdr *http.Request, dataFolder string, req *Request) {
	t.Helper()

	switch {
	case req.Table == TableColumns:
		// make the peer detect dependency columns
		b, err := json.Marshal(getTestDataColumns(dataFolder))
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(wrt, "%d %11d\n", 200, len(b))
		fmt.Fprint(wrt, string(b))
	case req.Table != TableNone:
		filename := fmt.Sprintf("%s/%s", dataFolder, req.Table.String())
		dat := readTestData(filename, req.Columns)
		fmt.Fprintf(wrt, "%d %11d\n", 200, len(dat))
		fmt.Fprint(wrt, string(dat))
	case req.Command == "COMMAND [0] test_ok":
		if v, ok := rdr.Header["Accept"]; ok && v[0] == "application/livestatus" {
			fmt.Fprintln(wrt, "")
		} else {
			fmt.Fprintln(wrt, "{\"rc\":0,\"version \":\"2.20\",\"branch\":\"1\",\"output\":[null,0,\"\",null]}")
		}
	case req.Command == "COMMAND [0] test_broken":
		if v, ok := rdr.Header["Accept"]; ok && v[0] == "application/livestatus" {
			fmt.Fprintln(wrt, "400: command broken")
		} else {
			fmt.Fprintln(wrt, "{\"rc\":0,\"version\":\"2.20\",\"branch\":\"1\",\"output\":[null,0,\"400: command broken\",null]}")
		}
	}
}

func GetHTTPMockServerPeer(t *testing.T, lmd *Daemon) (peer *Peer, cleanup func()) {
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
	for rowNum, rowIn := range hashedData {
		rowOut := make([]interface{}, len(columns))
		for idx, col := range columns {
			val, ok := rowIn[col]
			if !ok {
				panic(fmt.Sprintf("missing column %s in testdata for file %s.map", col, filename))
			}
			rowOut[idx] = val
		}
		rowBytes, err2 := json.Marshal(rowOut)
		if err2 != nil {
			panic("could not convert row to json: " + err2.Error())
		}
		result = append(result, rowBytes...)
		if rowNum < len(hashedData)-1 {
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

func createTestLMDInstance() *Daemon {
	lmd := NewLMDInstance()
	lmd.Config = NewConfig([]string{})
	lmd.Config.ValidateConfig()
	lmd.flags.flagDeadlock = 15
	lmd.nodeAccessor = NewNodes(lmd, []string{}, "")

	return lmd
}

func panicFailedStartup(mocklmd *Daemon, peer *Peer, numPeers int, err error) {
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

// make sure our test helpers are working correctly.
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

	err := peer.InitAllTables(context.TODO())
	require.NoErrorf(t, err, "init tables failed")

	// tear down
	_, _, err = peer.QueryString("COMMAND [0] MOCK_EXIT")
	require.NoErrorf(t, err, "stopping mock livestatus source failed")
}

func TestMock2(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 1, 1)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET status\nColumns: program_start\n\n")
	require.NoErrorf(t, err, "query is successful")
	require.Lenf(t, res, 1, "result size is correct")

	err = cleanup()
	require.NoError(t, err)
}
