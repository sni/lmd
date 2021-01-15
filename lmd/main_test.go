package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestMainFunc(t *testing.T) {
	peer := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET backends\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("peer_key", (*res)[0][0]); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("mockid0", (*res)[1][0]); err != nil {
		t.Fatal(err)
	}

	testRequestStrings := []string{
		"GET backends\n\n",
		"GET backends\nResponseHeader: fixed16\n\n",
		"GET backends\nResponseHeader: fixed16\nOutputFormat: json\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key = mockid0\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key = mockid0\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key ~~ mockid0\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key =~ mockid0\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key !=\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key != id2\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key !=~ id2\n\n",
		"GET backends\nResponseHeader: fixed16\nSort: peer_key asc\n\n",
		"GET backends\nResponseHeader: fixed16\nSort: peer_key desc\n\n",
		"GET backends\nResponseHeader: fixed16\nSort: bytes_send asc\nSort: bytes_received desc\n\n",
	}
	for _, str := range testRequestStrings {
		res, _, err = peer.QueryString(str)
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEq("peer_key", (*res)[0][0]); err != nil {
			t.Fatal(err)
		}
		if err = assertEq("mockid0", (*res)[1][0]); err != nil {
			t.Fatal(err)
		}
	}

	// sort queries
	res, _, err = peer.QueryString("GET backends\nColumns: peer_key bytes_send bytes_received\nSort: bytes_send asc\nSort: bytes_received desc\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("mockid0", (*res)[0][0]); err != nil {
		t.Fatal(err)
	}

	// stats queries
	res, _, err = peer.QueryString("GET backends\nStats: bytes_send > 0\nStats: avg bytes_send\nStats: sum bytes_send\nStats: min bytes_send\nStats: max bytes_send\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1.0, (*res)[0][0]); err != nil {
		t.Fatal(err)
	}

	// send commands
	_, _, err = peer.QueryString("COMMAND [123456] TEST\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func TestMainReload(_ *testing.T) {
	StartMockMainLoop([]string{"mock0.sock"}, "")
	mainSignalChannel <- syscall.SIGHUP
	waitTimeout(TestPeerWaitGroup, 5*time.Second)
	// shutdown all peers
	for _, p := range PeerMap {
		p.Stop()
		p.shutdownChannel <- true
		PeerMapRemove(p.ID)
	}
	// shutdown all listeners
	ListenersLock.Lock()
	for _, l := range Listeners {
		l.connection.Close()
	}
	ListenersLock.Unlock()
	waitTimeout(TestPeerWaitGroup, 5*time.Second)
	retries := 0
	for {
		// recheck every 100ms
		time.Sleep(100 * time.Millisecond)
		retries++
		if retries > 100 {
			panic("listener/peers did not stop after reload")
		}
		ListenersLock.Lock()
		numListener := len(Listeners)
		ListenersLock.Unlock()
		if len(PeerMap)+numListener == 0 {
			break
		}
	}
}

func TestAllOps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping all ops test in short mode")
	}
	peer := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	ops := []string{"=", "!=", "=~", "!=~", "~", "!~", "~~", "!~~", "<", "<=", ">", ">=", "!>="}
	values := []string{"", " test", " 5", " 3.124", " {}"}

	res, _, err := peer.QueryString("GET columns\nColumns: table name description\n\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range *res {
		if row[2].(string) == "" {
			t.Fatalf("got no description for %s in %s", row[1].(string), row[0].(string))
		}
		tableName, _ := NewTableName(*interface2string(row[0]))
		col := Objects.Tables[tableName].GetColumn(row[1].(string))
		for _, op := range ops {
			if col.Optional != NoFlags {
				continue
			}
			if strings.HasSuffix(col.Name, "_lc") {
				continue
			}
			for _, value := range values {
				testqueryCol(t, peer, col.Table.Name, col.Name)
				testqueryFilter(t, peer, col.Table.Name, col.Name, op, value)
				testqueryGroup(t, peer, col.Table.Name, col.Name, op, value)
			}
		}
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func testqueryCol(t *testing.T, peer *Peer, table TableName, column string) {
	t.Helper()
	query := fmt.Sprintf("GET %s\nColumns: %s\nSort: %s asc\n\n",
		table.String(),
		column,
		column,
	)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			t.Fatalf("paniced for query:\n%s", query)
		}
	}()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(buf, ParseDefault)
	if err == nil {
		if err = assertEq(query, req.String()); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err = peer.QueryString(query)
	if err != nil {
		t.Fatal(err)
	}
}

func testqueryFilter(t *testing.T, peer *Peer, table TableName, column, op, value string) {
	t.Helper()
	query := fmt.Sprintf("GET %s\nColumns: %s\nFilter: %s %s%s\n\n",
		table.String(),
		column,
		column,
		op,
		value,
	)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			t.Fatalf("paniced for query:\n%s", query)
		}
	}()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(buf, ParseDefault)
	if err == nil {
		if err = assertEq(query, req.String()); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err = peer.QueryString(query)
	if err != nil {
		logDebugError(err)
	}
}

func testqueryGroup(t *testing.T, peer *Peer, table TableName, column, op, value string) {
	t.Helper()
	query := fmt.Sprintf("GET %s\nColumns: %s\nStats: %s %s%s\n\n",
		table.String(),
		column,
		column,
		op,
		value,
	)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			t.Fatalf("paniced for query:\n%s", query)
		}
	}()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(buf, ParseDefault)
	if err == nil {
		if err = assertEq(query, req.String()); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err = peer.QueryString(query)
	if err != nil {
		logDebugError(err)
	}
}

func TestAllTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping all ops test in short mode")
	}
	peer := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	for table := range Objects.Tables {
		if Objects.Tables[table].PassthroughOnly {
			continue
		}
		query := fmt.Sprintf("GET %s\n\n", table.String())
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in f", r)
				t.Fatalf("paniced for query:\n%s", query)
			}
		}()
		_, _, err := peer.QueryString(query)
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func TestMainConfig(t *testing.T) {
	testConfig := []string{
		`Loglevel = "Warn"`,
		"Listen = [\"test1.sock\"]\nSkipSSLCheck = 1",
		"Listen = [\"test2.sock\"]\nSkipSSLCheck = 0",
	}

	err := ioutil.WriteFile("test1.ini", []byte(testConfig[0]), 0644)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile("test2.ini", []byte(testConfig[1]), 0644)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile("test3.ini", []byte(testConfig[2]), 0644)
	if err != nil {
		t.Fatal(err)
	}

	conf := ReadConfig([]string{"test1.ini"})
	if err := assertEq(len(conf.Listen), 0); err != nil {
		t.Error(err)
	}

	conf = ReadConfig([]string{"test1.ini", "test2.ini"})
	if err := assertEq(len(conf.Listen), 1); err != nil {
		t.Error(err)
	}
	if err := assertEq(conf.SkipSSLCheck, 1); err != nil {
		t.Error(err)
	}

	conf = ReadConfig([]string{"test1.ini", "test2.ini", "test3.ini"})
	if err := assertEq(len(conf.Listen), 2); err != nil {
		t.Error(err)
	}
	if err := assertEq(conf.SkipSSLCheck, 0); err != nil {
		t.Error(err)
	}

	os.Remove("test1.ini")
	os.Remove("test2.ini")
	os.Remove("test3.ini")
}

func TestMainArgs(t *testing.T) {
	cfg := &Config{IdleTimeout: 10, SaveTempRequests: true}
	args := arrayFlags{list: []string{
		"idletimeout=300",
		"SaveTempRequests=false",
		"ListenPrometheus=/dev/null",
		"SkipSSLCheck=1",
	}}
	applyArgFlags(args, cfg)

	if err := assertEq(cfg.IdleTimeout, int64(300)); err != nil {
		t.Error(err)
	}

	if err := assertEq(cfg.SaveTempRequests, false); err != nil {
		t.Error(err)
	}

	if err := assertEq(cfg.ListenPrometheus, "/dev/null"); err != nil {
		t.Error(err)
	}

	if err := assertEq(cfg.SkipSSLCheck, int(1)); err != nil {
		t.Error(err)
	}
}

func TestMainWaitTimeout(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	t1 := time.Now()
	timeout := 50 * time.Millisecond
	waitTimeout(wg, timeout)
	duration := time.Since(t1)
	if duration < timeout {
		t.Errorf("timeout too small: %s", duration)
	}
}
