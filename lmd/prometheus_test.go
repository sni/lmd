package main

import (
	"crypto/tls"
	"testing"
)

func TestPrometheus(t *testing.T) {
	extraConfig := `
        ListenPrometheus = "127.0.0.1:50999"
	`
	peer := StartTestPeerExtra(2, 10, 10, extraConfig)
	PauseTestPeers(peer)

	netClient := NewLMDHTTPClient(&tls.Config{}, "")
	response, err := netClient.Get("http://127.0.0.1:50999/metrics")
	if err != nil {
		t.Fatal(err)
	}
	contents, err := ExtractHTTPResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertLike("lmd_peer_update_interval", string(contents)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}
