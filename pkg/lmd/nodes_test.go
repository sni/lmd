package lmd

import (
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeManager(t *testing.T) {
	extraConfig := `
		Listen = ['test.sock', 'http://127.0.0.1:8901']
		Nodes = ['http://127.0.0.1:8901', 'http://127.0.0.2:8902']
		ClusterAuth = 'testclustersecret'
	`
	peer, cleanup, mocklmd := StartTestPeerExtra(t, 4, 10, 10, extraConfig)
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

// A rogue node answering cluster pings without the shared cluster secret must be
// rejected: no identity registration, no backend assignment, no stopped backends.
func TestNodeManagerRejectsUnauthenticatedClusterPing(t *testing.T) {
	// rogue node serving on a free loopback port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	roguePings := int64(0)
	rogueNonce := ""
	rogue := httptest.NewUnstartedServer(http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/query" {
			wrt.WriteHeader(http.StatusNotFound)

			return
		}
		var requestData map[string]any
		_ = json.NewDecoder(req.Body).Decode(&requestData)
		rogueNonce = interface2stringNoDedup(requestData["authnonce"])
		atomic.AddInt64(&roguePings, 1)
		wrt.Header().Set("Content-Type", "application/json")
		// forged ping response: own identity + backend assignment, but no valid auth signature
		_, _ = fmt.Fprintf(wrt, `{"identifier":"SPOOFEDNODE","peers":["mockid0"],"version":%q}`, Version())
	}))
	rogue.Listener = listener
	rogue.Start()
	defer rogue.Close()

	extraConfig := `
		Listen = ['test.sock', 'http://127.0.0.1:8901']
		Nodes = ['http://127.0.0.1:8901', '` + rogue.URL + `']
		ClusterAuth = 'testclustersecret'
	`
	peer, cleanup, mocklmd := StartTestPeerExtra(t, 4, 10, 10, extraConfig)
	PauseTestPeers(peer)
	defer func() { _ = cleanup() }()

	nodes := mocklmd.nodeAccessor
	require.Truef(t, nodes.IsClustered(), "nodes must be clustered")
	assert.Positivef(t, atomic.LoadInt64(&roguePings), "rogue node must have received pings")

	// rogue identity must not be registered
	assert.NotContainsf(t, nodes.nodeBackends, "SPOOFEDNODE", "rogue node must not get a backend assignment")
	for _, node := range nodes.nodeAddresses {
		require.NotEqualf(t, "SPOOFEDNODE", node.id, "rogue identity must not be registered on node %s", node)
	}
	// own node must be identified (not broken by the rejection)
	require.NotNilf(t, nodes.thisNode, "thisNode should not be nil")
	// all backends stay online on the surviving node
	res, _, err := peer.QueryString("GET hosts\nColumns: name peer_key state\n\n")
	require.NoErrorf(t, err, "query successful")
	require.Lenf(t, res, 40, "all backend hosts must be served, none stopped")

	// the signature binds (secret, nonce, identifier, peers): changing any input
	// invalidates it, which is what the sendPing check relies on to reject forgeries.
	require.NotEmptyf(t, rogueNonce, "rogue node must have received a ping nonce")
	sig := clusterAuthSignature("testclustersecret", rogueNonce, "SPOOFEDNODE", []string{"mockid0"})
	assert.NotEqualf(t, sig, clusterAuthSignature("wrongsecret", rogueNonce, "SPOOFEDNODE", []string{"mockid0"}), "secret must be bound to the signature")
	assert.NotEqualf(t, sig, clusterAuthSignature("testclustersecret", rogueNonce+"x", "SPOOFEDNODE", []string{"mockid0"}), "nonce must be bound to the signature")
	assert.NotEqualf(t, sig, clusterAuthSignature("testclustersecret", rogueNonce, "OTHERNODE", []string{"mockid0"}), "identity must be bound to the signature")
	assert.NotEqualf(t, sig, clusterAuthSignature("testclustersecret", rogueNonce, "SPOOFEDNODE", []string{"mockid1"}), "peers must be bound to the signature")

	// round trip: a peer signing with the shared secret must pass the same check sendPing uses
	controller := &HTTPServerController{lmd: mocklmd}
	recorder := httptest.NewRecorder()
	pingRequestData := map[string]any{"identifier": nodes.ID, "peers": "", "authnonce": rogueNonce}
	controller.queryPing(recorder, pingRequestData)
	var response map[string]any
	require.NoErrorf(t, json.Unmarshal(recorder.Body.Bytes(), &response), "ping response must be json")
	responseIdentifier := interface2stringNoDedup(response["identifier"])
	responsePeers := make([]string, 0)
	for _, id := range interface2interfaceList(response["peers"]) {
		responsePeers = append(responsePeers, interface2stringNoDedup(id))
	}
	expected := clusterAuthSignature(mocklmd.Config.ClusterAuth, rogueNonce, responseIdentifier, responsePeers)
	assert.Truef(t, hmac.Equal([]byte(interface2stringNoDedup(response["authsig"])), []byte(expected)), "signed ping response must be accepted")
}
