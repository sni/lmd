package main

import (
	"bufio"
	"bytes"
	"sync"
	"testing"
)

func TestPeerSource(t *testing.T) {

	waitGroup := &sync.WaitGroup{}
	shutdownChannel := make(chan bool)
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/", "http://clusternode/test"}}
	peer := NewPeer(&Config{}, &connection, waitGroup, shutdownChannel)

	if err := assertEq("http://localhost/test", peer.Source[0]); err != nil {
		t.Error(err)
	}

	if err := assertEq("http://clusternode/test", peer.Source[1]); err != nil {
		t.Error(err)
	}
}

func TestPeerHTTPComplete(t *testing.T) {
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/cgi-bin/remote.cgi")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename/")); err != nil {
		t.Error(err)
	}
}

func TestParseResultJSON(t *testing.T) {

	waitGroup := &sync.WaitGroup{}
	shutdownChannel := make(chan bool)
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/", "http://clusternode/test"}}
	peer := NewPeer(&Config{}, &connection, waitGroup, shutdownChannel)

	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description\nOutputFormat: json\n")))
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`[
	 ["host1", "desc1"],
	 ["host2", "desc2"]
	]`)

	res, err := peer.parseResult(req, &data)

	if err := assertEq(2, len(res)); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(2, len(res[0])); err != nil {
		t.Error(err)
	}
	if err := assertEq("host2", res[1][0]); err != nil {
		t.Error(err)
	}
}

func TestParseResultWrappedJSON(t *testing.T) {

	waitGroup := &sync.WaitGroup{}
	shutdownChannel := make(chan bool)
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/", "http://clusternode/test"}}
	peer := NewPeer(&Config{}, &connection, waitGroup, shutdownChannel)

	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description\nOutputFormat: wrapped_json\n")))
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`{"data": [
	 ["host1", "desc1"],
	 ["host2", "desc2"]
	]}`)

	res, err := peer.parseResult(req, &data)

	if err := assertEq(2, len(res)); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(2, len(res[0])); err != nil {
		t.Error(err)
	}
	if err := assertEq("host2", res[1][0]); err != nil {
		t.Error(err)
	}
}

