package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestMainFunc(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET backends\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("peer_key", res[0][0]); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("mockid0", res[1][0]); err != nil {
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
		if err = assertEq("peer_key", res[0][0]); err != nil {
			t.Fatal(err)
		}
		if err = assertEq("mockid0", res[1][0]); err != nil {
			t.Fatal(err)
		}
	}

	// sort queries
	res, _, err = peer.QueryString("GET backends\nColumns: peer_key bytes_send bytes_received\nSort: bytes_send asc\nSort: bytes_received desc\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("mockid0", res[0][0]); err != nil {
		t.Fatal(err)
	}

	// stats queries
	res, _, err = peer.QueryString("GET backends\nStats: bytes_send > 0\nStats: avg bytes_send\nStats: sum bytes_send\nStats: min bytes_send\nStats: max bytes_send\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1.0, res[0][0]); err != nil {
		t.Fatal(err)
	}

	// send commands
	_, _, err = peer.QueryString("COMMAND [123456] TEST\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestMainReload(t *testing.T) {
	lmd := createTestLMDInstance()
	StartMockMainLoop(lmd, []string{"mock0.sock"}, "")
	lmd.mainSignalChannel <- syscall.SIGHUP
	// shutdown all peers
	for id := range lmd.PeerMap {
		p := lmd.PeerMap[id]
		p.Stop()
		close(p.shutdownChannel)
		lmd.PeerMapRemove(p.ID)
	}
	// shutdown all listeners
	lmd.ListenersLock.Lock()
	for _, l := range lmd.Listeners {
		l.Stop()
	}
	lmd.ListenersLock.Unlock()
	if waitTimeout(lmd.waitGroupPeers, 5*time.Second) {
		t.Errorf("timeout while waiting for peers to stop")
	}
	retries := 0
	for {
		// recheck every 100ms
		time.Sleep(50 * time.Millisecond)
		retries++
		if retries > 200 {
			panic("listener/peers did not stop after reload")
		}
		lmd.ListenersLock.Lock()
		numListener := len(lmd.Listeners)
		lmd.ListenersLock.Unlock()
		if len(lmd.PeerMap)+numListener == 0 {
			break
		}
	}
}

func TestAllOps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping all ops test in short mode")
	}
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	ops := []string{"=", "!=", "=~", "!=~", "~", "!~", "~~", "!~~", "<", "<=", ">", ">=", "!>="}
	values := []string{"", " test", " 5", " 3.124", " {}"}

	res, _, err := peer.QueryString("GET columns\nColumns: table name description\n\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range res {
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

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func testqueryCol(t *testing.T, peer *Peer, table TableName, column string) {
	t.Helper()
	lmd := createTestLMDInstance()
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
	req, _, err := NewRequest(context.TODO(), lmd, buf, ParseDefault)
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
	lmd := createTestLMDInstance()
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
	req, _, err := NewRequest(context.TODO(), lmd, buf, ParseDefault)
	if err == nil {
		if err = assertEq(query, req.String()); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err = peer.QueryString(query)
	if err != nil {
		LogErrors(err)
	}
}

func testqueryGroup(t *testing.T, peer *Peer, table TableName, column, op, value string) {
	t.Helper()
	lmd := createTestLMDInstance()
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
	req, _, err := NewRequest(context.TODO(), lmd, buf, ParseDefault)
	if err == nil {
		if err = assertEq(query, req.String()); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err = peer.QueryString(query)
	if err != nil {
		LogErrors(err)
	}
}

func TestAllTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping all ops test in short mode")
	}
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	query := ""
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			t.Fatalf("paniced for query:\n%s", query)
		}
	}()
	for table := range Objects.Tables {
		if Objects.Tables[table].PassthroughOnly {
			continue
		}
		query = fmt.Sprintf("GET %s\n\n", table.String())
		_, _, err := peer.QueryString(query)
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestMainConfig(t *testing.T) {
	testConfig := []string{
		`Loglevel = "Warn"`,
		"Listen = [\"test1.sock\"]\nSkipSSLCheck = 1",
		"Listen = [\"test2.sock\"]\nSkipSSLCheck = 0",
	}

	err := os.WriteFile("test1.ini", []byte(testConfig[0]), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile("test2.ini", []byte(testConfig[1]), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile("test3.ini", []byte(testConfig[2]), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	conf := NewConfig([]string{"test1.ini"})
	if err := assertEq(len(conf.Listen), 0); err != nil {
		t.Error(err)
	}

	conf = NewConfig([]string{"test1.ini", "test2.ini"})
	if err := assertEq(len(conf.Listen), 1); err != nil {
		t.Error(err)
	}
	if err := assertEq(conf.SkipSSLCheck, 1); err != nil {
		t.Error(err)
	}

	conf = NewConfig([]string{"test1.ini", "test2.ini", "test3.ini"})
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
	if duration := time.Since(t1); duration < timeout {
		t.Errorf("timeout too small: %s", duration)
	}
}
