package lmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeManager(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping nodes test in short mode")
	}
	extraConfig := `
		Listen = ['test.sock', 'http://127.0.0.1:8901']
		Nodes = ['http://127.0.0.1:8901', 'http://127.0.0.2:8902']
	`
	peer, cleanup, mocklmd := StartTestPeerExtra(4, 10, 10, extraConfig)
	PauseTestPeers(peer)

	require.NotNilf(t, mocklmd.nodeAccessor, "nodeAccessor must not be nil")
	require.Truef(t, mocklmd.nodeAccessor.IsClustered(), "nodes must not be clustered")
	require.NotNilf(t, mocklmd.nodeAccessor.thisNode, "thisNode should not be nil")
	require.NotEmptyf(t, mocklmd.nodeAccessor.thisNode.String(), "thisNode got a name")

	// test host request
	res, _, err := peer.QueryString("GET hosts\nColumns: name peer_key state\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 40, "result length")

	// test host stats request
	res, _, err = peer.QueryString("GET hosts\nStats: name !=\nStats: avg latency\nStats: sum latency\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 1, "result length")

	assert.InDeltaf(t, 40, res[0][0], 0.00001, "count is correct")
	assert.InDeltaf(t, 0.08365800231700002, res[0][1], 0.00001, "avg latency is correct")
	assert.InDeltaf(t, 3.346320092680001, res[0][2], 0.00001, "sum latency is correct")

	// test host grouped stats request
	res, _, err = peer.QueryString("GET hosts\nColumns: name alias\nStats: name !=\nStats: avg latency\nStats: sum latency\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 10, "result length")

	require.Lenf(t, res[1], 5, "result length")
	assert.Equalf(t, "testhost_1", res[1][0], "hostname matches")
	assert.Equalf(t, "testhost_1_ALIAS", res[1][1], "alias matches")
	assert.InDeltaf(t, 4.0, res[1][2], 0.00001, "count matches")
	assert.InDeltaf(t, 0.083658002317, res[1][3], 0.00001, "avg latency matches")

	// test host empty stats request
	res, _, err = peer.QueryString("GET hosts\nFilter: check_type = 15\nStats: sum percent_state_change\nStats: min percent_state_change\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 1, "result length")

	assert.InDeltaf(t, float64(0), res[0][0], 0.00001, "count is correct")

	err = cleanup()
	require.NoError(t, err)
}
