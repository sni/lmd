package main

import (
	"context"
	"net/http"
	"testing"
)

func TestPrometheus(t *testing.T) {
	extraConfig := `
        ListenPrometheus = "127.0.0.1:50999"
	`
	peer, cleanup, _ := StartTestPeerExtra(2, 10, 10, extraConfig)
	PauseTestPeers(peer)

	ctx := context.Background()
	tlsconfig := getMinimalTLSConfig(peer.lmd.Config)
	netClient := NewLMDHTTPClient(tlsconfig, "")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:50999/metrics", http.NoBody)
	response, err := netClient.Do(req)
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

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}
