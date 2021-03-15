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
	if err := assertEq(false, peer.HasFlag(HasDependencyColumn)); err != nil {
		t.Error(err)
	}
	peer.SetFlag(HasDependencyColumn)
	if err := assertEq(true, peer.HasFlag(HasDependencyColumn)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, peer.HasFlag(MultiBackend)); err != nil {
		t.Error(err)
	}

	peer.ResetFlags()
	if err := assertEq(false, peer.HasFlag(Naemon)); err != nil {
		t.Error(err)
	}
	if err := assertEq(uint32(NoFlags), peer.Flags); err != nil {
		t.Error(err)
	}
}

func TestColumnList(t *testing.T) {
	cl := ColumnList{&Column{Name: "Test1"}, &Column{Name: "Test2"}}
	if err := assertEq("Test1, Test2", cl.String()); err != nil {
		t.Error(err)
	}
}

func TestColumnEmpty(t *testing.T) {
	c := Column{Name: "Test1", DataType: IntCol}
	if err := assertEq(-1, c.GetEmptyValue()); err != nil {
		t.Error(err)
	}

	c.DataType = StringCol
	if err := assertEq("", c.GetEmptyValue()); err != nil {
		t.Error(err)
	}
}
