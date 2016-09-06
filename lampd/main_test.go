package main

import (
	"io/ioutil"
	"net"
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

	go func() {
		l, err := net.Listen("tcp", "127.0.0.1:50050")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			_, err = ParseRequest(conn)
			//req, err := ParseRequest(conn)
			if err != nil {
				log.Errorf("err: %v", err)
				t.Fatal(err)
				continue
			}
		}
	}()

	go func() {
		os.Args[1] = "-config=test.ini"
		main()
	}()

	time.Sleep(time.Duration(1) * time.Second)
	toml.DecodeFile("test.ini", &GlobalConfig)
	shutdownChannel := make(chan bool)
	waitGroup := &sync.WaitGroup{}
	peer := NewPeer(&Connection{Source: []string{"127.0.0.1:50051"}, Name: "Test", Id: "id0"}, waitGroup, shutdownChannel)

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

	os.Remove("test.ini")
	os.Remove("test.sock")
}
