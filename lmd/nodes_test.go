package main

import (
	"testing"
)

func TestNodeManager(t *testing.T) {
	extraConfig := `
		Listen = ['test.sock', 'http://127.0.0.1:8901']
		Nodes = ['http://127.0.0.1:8901', 'http://127.0.0.2:8902']
	`
	peer := StartTestPeerExtra(4, 10, 10, extraConfig)
	PauseTestPeers(peer)

	if nodeAccessor == nil {
		t.Fatalf("nodeAccessor should not be nil")
	}
	if err := assertEq(nodeAccessor.IsClustered(), true); err != nil {
		t.Fatalf("Nodes are not clustered")
	}
	if nodeAccessor.thisNode == nil {
		t.Fatalf("thisNode should not be nil")
	}
	if !(nodeAccessor.thisNode.HumanIdentifier() != "") {
		t.Fatalf("got a name")
	}

	// test host request
	res, err := peer.QueryString("GET hosts\nColumns: name peer_key state\n\n")
	if err != nil {
		t.Fatalf("request error should be nil")
	}

	if err = assertEq(len(res), 40); err != nil {
		t.Errorf("result contains 40 hosts")
	}

	// test host stats request
	res, err = peer.QueryString("GET hosts\nStats: name !=\nStats: avg latency\nStats: sum latency\n\n")
	if err != nil {
		t.Fatalf("request error should be nil")
	}
	if err = assertEq(int(res[0][0].(float64)), 40); err != nil {
		t.Errorf("host stats count")
	}
	if err = assertEq(res[0][1], 0.24065613746999998); err != nil {
		t.Errorf("host stats average")
	}
	if err = assertEq(res[0][2], 9.6262454988); err != nil {
		t.Errorf("host stats sum")
	}

	// test host grouped stats request
	res, err = peer.QueryString("GET hosts\nColumns: name alias\nStats: name !=\nStats: avg latency\nStats: sum latency\n\n")
	if err != nil {
		t.Fatalf("request error should be nil")
	}
	if err = assertEq(res[0][0], "testhost_10;tomcat"); err != nil {
		t.Errorf("host stats group name")
	}
	if err = assertEq(res[0][1], 4.0); err != nil {
		t.Errorf("host stats group avg")
	}
	if err = assertEq(res[0][2], 0.24065613747); err != nil {
		t.Errorf("host stats group sum")
	}

	StopTestPeer(peer)
}