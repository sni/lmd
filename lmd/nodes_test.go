package main

import (
	"syscall"
	"testing"
	"time"
)

func TestNodeManager(t *testing.T) {
	addresses := []string{"localhost:1234", "localhost:5678"}
	StartMockMainLoop(addresses, `
		Listen = ['test.sock', 'http://*:8901']
		Nodes = ['http://127.0.0.1:8901', 'http://127.0.0.2:8901']
	`)

	// wait till backend is available
	retries := 0
	for {
		if nodeAccessor != nil && nodeAccessor.thisNode != nil {
			break
		}
		// recheck every 50ms
		time.Sleep(50 * time.Millisecond)
		retries++
		if retries > 60 {
			panic("nodeAccessor never came online")
		}
	}

	if nodeAccessor == nil {
		t.Fatalf("nodeAccessor is nil")
	}
	if err := assertEq(nodeAccessor.IsClustered(), true); err != nil {
		t.Fatalf("Nodes are clustered")
	}
	if nodeAccessor.thisNode == nil {
		t.Fatalf("thisNode is nil")
	}
	if !(nodeAccessor.thisNode.HumanIdentifier() != "") {
		t.Fatalf("got a name")
	}

	// shutdown mainloop
	nodeAccessor.Stop()
	mainSignalChannel <- syscall.SIGTERM
	waitTimeout(TestPeerWaitGroup, 5*time.Second)
}
