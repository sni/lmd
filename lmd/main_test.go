package main

import (
	"bufio"
	"bytes"
	"fmt"
	"syscall"
	"testing"
)

func TestMainFunc(t *testing.T) {
	peer := SetupTestPeer()

	res, err := peer.QueryString("GET backends\n\n")
	if err = assertEq("peer_key", res[0][0]); err != nil {
		t.Fatal(err)
	}
	if err = assertEq("id1", res[1][0]); err != nil {
		t.Fatal(err)
	}

	testRequestStrings := []string{
		"GET backends\n\n",
		"GET backends\nResponseHeader: fixed16\n\n",
		"GET backends\nResponseHeader: fixed16\nOutputFormat: json\n\n",
		"GET backends\nResponseHeader: fixed16\nOutputFormat: wrapped_json\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key = id1\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key = id1\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key ~~ id1\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key =~ id1\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key !=\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key != id2\n\n",
		"GET backends\nResponseHeader: fixed16\nFilter: peer_key !=~ id2\n\n",
		"GET backends\nResponseHeader: fixed16\nSort: peer_key asc\n\n",
		"GET backends\nResponseHeader: fixed16\nSort: peer_key desc\n\n",
		"GET backends\nResponseHeader: fixed16\nSort: bytes_send asc\nSort: bytes_received desc\n\n",
	}
	for _, str := range testRequestStrings {
		res, err := peer.QueryString(str)
		if err != nil {
			t.Fatal(err)
		}
		if err = assertEq("peer_key", res[0][0]); err != nil {
			t.Fatal(err)
		}
		if err = assertEq("id1", res[1][0]); err != nil {
			t.Fatal(err)
		}
	}

	// sort querys
	res, err = peer.QueryString("GET backends\nColumns: peer_key bytes_send bytes_received\nSort: bytes_send asc\nSort: bytes_received desc\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("id1", res[0][0]); err != nil {
		t.Fatal(err)
	}

	// stats querys
	res, err = peer.QueryString("GET backends\nStats: bytes_send > 0\nStats: avg bytes_send\nStats: sum bytes_send\nStats: min bytes_send\nStats: max bytes_send\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1.0, res[0][0]); err != nil {
		t.Fatal(err)
	}

	// send commands
	_, err = peer.QueryString("COMMAND [123456] TEST\n\n")
	if err != nil {
		t.Fatal(err)
	}

	// test reload, makes the mainloop exit as well
	mainSignalChannel <- syscall.SIGHUP
}

func TestAllOps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping all ops test in short mode")
	}
	peer := SetupTestPeer()

	ops := []string{"=", "!=", "=~", "!=~", "~", "!~", "~~", "!~~", "<", "<=", ">", ">=", "!>="}
	values := []string{"", " test", " 5", " 3.124", "{}"}

	res, err := peer.QueryString("GET columns\nColumns: table name description\n\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range res {
		if row[2].(string) == "" {
			t.Fatalf("got no description for %s in %s", row[1].(string), row[0].(string))
		}
		for _, op := range ops {
			for _, value := range values {
				testquery(t, peer, row[0].(string), row[1].(string), op, value)
			}
		}
	}

	StopTestPeer(peer)
}

func testquery(t *testing.T, peer *Peer, table, column, op, value string) {
	query := fmt.Sprintf("GET %s\nColumns: %s\nFilter: %s %s%s\nSort: %s asc\n\n",
		table,
		column,
		column,
		op,
		value,
		column,
	)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			t.Fatalf("paniced for query:\n%s", query)
		}
	}()
	buf := bufio.NewReader(bytes.NewBufferString(query))
	req, _, err := NewRequest(buf)
	if err == nil {
		if err = assertEq(query, req.String()); err != nil {
			t.Fatal(err)
		}
	}
	peer.QueryString(query)
}
