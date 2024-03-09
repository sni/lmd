package lmd

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMainFunc(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET backends\n\n")
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(res), 2, "expected at least 2 rows: %#v", res)
	require.Equal(t, "peer_key", res[0][0])
	require.Equal(t, "mockid0", res[1][0])

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
		require.NoError(t, err)
		require.Equal(t, "peer_key", res[0][0])
		require.Equal(t, "mockid0", res[1][0])
	}

	// sort queries
	res, _, err = peer.QueryString("GET backends\nColumns: peer_key bytes_send bytes_received\nSort: bytes_send asc\nSort: bytes_received desc\n\n")
	require.NoError(t, err)
	require.Equal(t, "mockid0", res[0][0])

	// stats queries
	res, _, err = peer.QueryString("GET backends\nStats: bytes_send > 0\nStats: avg bytes_send\nStats: sum bytes_send\nStats: min bytes_send\nStats: max bytes_send\n\n")
	require.NoError(t, err)
	require.InDelta(t, 1.0, res[0][0], 0)

	// send commands
	_, _, err = peer.QueryString("COMMAND [123456] TEST\n\n")
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
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
	if waitTimeout(context.TODO(), lmd.waitGroupPeers, 5*time.Second) {
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
	require.NoError(t, err)

	for _, row := range res {
		require.NotEmptyf(t, row[2], "got no description for %#v in %#v", row[1], row[0])
		tableName, _ := NewTableName(interface2stringNoDedup(row[0]))
		require.IsTypef(t, "", row[1], "this must be a string")
		col := Objects.Tables[tableName].GetColumn(interface2stringNoDedup(row[1]))
		for _, operator := range ops {
			if col.Optional != NoFlags {
				continue
			}
			if strings.HasSuffix(col.Name, "_lc") {
				continue
			}
			for _, value := range values {
				testqueryCol(t, peer, col.Table.Name, col.Name)
				testqueryFilter(t, peer, col.Table.Name, col.Name, operator, value)
				testqueryGroup(t, peer, col.Table.Name, col.Name, operator, value)
			}
		}
	}

	err = cleanup()
	require.NoError(t, err)
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
			t.Logf("Recovered in f: %v", r)
			t.Fatalf("panicked for query:\n%s", query)
		}
	}()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(context.TODO(), lmd, buf, ParseDefault)
	if err == nil {
		require.Equal(t, query, req.String())
	}
	_, _, err = peer.QueryString(query)
	require.NoError(t, err)
}

func testqueryFilter(t *testing.T, peer *Peer, table TableName, column, operator, value string) {
	t.Helper()
	lmd := createTestLMDInstance()
	query := fmt.Sprintf("GET %s\nColumns: %s\nFilter: %s %s%s\n\n",
		table.String(),
		column,
		column,
		operator,
		value,
	)
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Recovered in f: %v", r)
			t.Fatalf("panicked for query:\n%s", query)
		}
	}()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(context.TODO(), lmd, buf, ParseDefault)
	if err == nil {
		require.Equal(t, query, req.String())
	}
	_, _, err = peer.QueryString(query)
	if err != nil {
		LogErrors(err)
	}
}

func testqueryGroup(t *testing.T, peer *Peer, table TableName, column, operator, value string) {
	t.Helper()
	lmd := createTestLMDInstance()
	query := fmt.Sprintf("GET %s\nColumns: %s\nStats: %s %s%s\n\n",
		table.String(),
		column,
		column,
		operator,
		value,
	)
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Recovered in f: %v", r)
			t.Fatalf("panicked for query:\n%s", query)
		}
	}()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(context.TODO(), lmd, buf, ParseDefault)
	if err == nil {
		require.Equal(t, query, req.String())
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
			t.Logf("Recovered in f: %v", r)
			t.Fatalf("panicked for query:\n%s", query)
		}
	}()
	for table := range Objects.Tables {
		if Objects.Tables[table].PassthroughOnly {
			continue
		}
		query = fmt.Sprintf("GET %s\n\n", table.String())
		_, _, err := peer.QueryString(query)
		require.NoError(t, err)
	}

	err := cleanup()
	require.NoError(t, err)
}

func TestMainConfig(t *testing.T) {
	testConfig := []string{
		`Loglevel = "Warn"`,
		"Listen = [\"test1.sock\"]\nSkipSSLCheck = 1",
		"Listen = [\"test2.sock\"]\nSkipSSLCheck = 0",
	}

	err := os.WriteFile("test1.ini", []byte(testConfig[0]), 0o644)
	require.NoError(t, err)
	err = os.WriteFile("test2.ini", []byte(testConfig[1]), 0o644)
	require.NoError(t, err)
	err = os.WriteFile("test3.ini", []byte(testConfig[2]), 0o644)
	require.NoError(t, err)

	conf := NewConfig([]string{"test1.ini"})
	assert.Empty(t, conf.Listen, 0)

	conf = NewConfig([]string{"test1.ini", "test2.ini"})
	assert.Len(t, conf.Listen, 1)

	assert.Equal(t, 1, conf.SkipSSLCheck)

	conf = NewConfig([]string{"test1.ini", "test2.ini", "test3.ini"})
	assert.Len(t, conf.Listen, 2)
	assert.Equal(t, 0, conf.SkipSSLCheck)

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

	assert.Equal(t, int64(300), cfg.IdleTimeout)
	assert.False(t, cfg.SaveTempRequests)
	assert.Equal(t, "/dev/null", cfg.ListenPrometheus)
	assert.Equal(t, 1, cfg.SkipSSLCheck)
}

func TestMainWaitTimeout(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	t1 := time.Now()
	timeout := 50 * time.Millisecond
	waitTimeout(context.TODO(), wg, timeout)
	if duration := time.Since(t1); duration < timeout {
		t.Errorf("timeout too small: %s", duration)
	}
}
