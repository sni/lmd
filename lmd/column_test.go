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

	if err := assertEq(NoFlags, peer.Flags); err != nil {
		t.Error(err)
	}

	peer.Flags.SetFlag(Naemon)

	if err := assertEq(Naemon, peer.Flags); err != nil {
		t.Error(err)
	}
	if err := assertEq(true, peer.Flags.HasFlag(Naemon)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, peer.Flags.HasFlag(Naemon1_0_10)); err != nil {
		t.Error(err)
	}
	peer.Flags.SetFlag(Naemon1_0_10)
	if err := assertEq(true, peer.Flags.HasFlag(Naemon1_0_10)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, peer.Flags.HasFlag(MultiBackend)); err != nil {
		t.Error(err)
	}

	peer.Flags.Clear()
	if err := assertEq(false, peer.Flags.HasFlag(Naemon)); err != nil {
		t.Error(err)
	}
	if err := assertEq(NoFlags, peer.Flags); err != nil {
		t.Error(err)
	}
}
