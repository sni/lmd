package main

import "testing"

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
	res, err = peer.QueryString("COMMAND [123456] TEST\n\n")
	if err != nil {
		t.Fatal(err)
	}

	StopTestPeer()
}
