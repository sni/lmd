package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"
)

func TestPrometheus(t *testing.T) {
	extraConfig := `
        ListenPrometheus = "127.0.0.1:50999"
	`
	peer := StartTestPeerExtra(2, 10, 10, extraConfig)
	PauseTestPeers(peer)

	ctx := context.Background()
	netClient := NewLMDHTTPClient(&tls.Config{}, "")
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:50999/metrics", nil)
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

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}
