package main

import (
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
