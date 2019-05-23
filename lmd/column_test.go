package main

import (
	"sync"
	"testing"
)

func TestColumnFlag(t *testing.T) {
	waitGroup := &sync.WaitGroup{}
	shutdownChannel := make(chan bool)
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/"}}
	peer := NewPeer(&Config{}, &connection, waitGroup, shutdownChannel)

	if err := assertEq(uint32(NoFlags), peer.Flags); err != nil {
		t.Error(err)
	}

	peer.SetFlag(Naemon)

	if err := assertEq(uint32(Naemon), peer.Flags); err != nil {
		t.Error(err)
	}
	if err := assertEq(true, peer.HasFlag(Naemon)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, peer.HasFlag(Naemon1_0_10)); err != nil {
		t.Error(err)
	}
	peer.SetFlag(Naemon1_0_10)
	if err := assertEq(true, peer.HasFlag(Naemon1_0_10)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, peer.HasFlag(MultiBackend)); err != nil {
		t.Error(err)
	}

	peer.ClearFlags()
	if err := assertEq(false, peer.HasFlag(Naemon)); err != nil {
		t.Error(err)
	}
	if err := assertEq(uint32(NoFlags), peer.Flags); err != nil {
		t.Error(err)
	}
}
