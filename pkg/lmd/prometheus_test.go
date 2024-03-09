package lmd

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	contents, err := ExtractHTTPResponse(response)
	require.NoError(t, err)
	assert.Contains(t, string(contents), "lmd_peer_update_interval")

	err = cleanup()
	require.NoError(t, err)
}
