package main

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

const testConfig = `
Loglevel = "Error"
Listen = ["127.0.0.1:50051", "./test.sock"]

[[Connections]]
name = "Test"
id   = "id1"
source = ["127.0.0.1:50050"]
`

func TestMainFunc(t *testing.T) {
	err := ioutil.WriteFile("test.ini", []byte(testConfig), 0644)
	if err != nil {
		t.Fatal(err)
	}

	StartMockLivestatusSource("127.0.0.1:50050")

	go func() {
		os.Args[1] = "-config=test.ini"
		main()
	}()

	toml.DecodeFile("test.ini", &GlobalConfig)
	shutdownChannel := make(chan bool)
	waitGroup := &sync.WaitGroup{}
	peer := NewPeer(&Connection{Source: []string{"doesnotexist", "127.0.0.1:50051"}, Name: "Test", Id: "id0"}, waitGroup, shutdownChannel)
	peer.Start()

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
		"GET backends\nResponseHeader: fixed16\nSort: peer_bytes_send asc\nSort: peer_bytes_received desc\n\n",
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
	res, err = peer.QueryString("GET backends\nColumns: peer_key peer_bytes_send peer_bytes_received\nSort: peer_bytes_send asc\nSort: peer_bytes_received desc\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq("id1", res[0][0]); err != nil {
		t.Fatal(err)
	}

	// stats querys
	res, err = peer.QueryString("GET backends\nStats: peer_bytes_send > 0\nStats: avg peer_bytes_send\nStats: sum peer_bytes_send\nStats: min peer_bytes_send\nStats: max peer_bytes_send\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err = assertEq(1.0, res[0][0]); err != nil {
		t.Fatal(err)
	}

	// send querys
	res, err = peer.QueryString("COMMAND [123456] TEST\n\n")
	if err != nil {
		t.Fatal(err)
	}

	shutdownChannel <- true
	close(shutdownChannel)
	waitTimeout(waitGroup, time.Second)

	os.Remove("test.ini")
	os.Remove("test.sock")
}
