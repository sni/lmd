package lmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerSource(t *testing.T) {
	lmd := createTestLMDInstance()
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/", "http://clusternode/test"}}
	peer := NewPeer(lmd, &connection)

	require.Len(t, peer.source, 2)
	assert.Equal(t, "http://localhost/test/", peer.source[0])
	assert.Equal(t, "http://clusternode/test", peer.source[1])
}

func TestPeerHTTPComplete(t *testing.T) {
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk"))
	assert.Equal(t, "http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/cgi-bin/remote.cgi"))
	assert.Equal(t, "http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename"))
	assert.Equal(t, "http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename/"))
}

func TestPeerDeltaUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 1, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := store.updateDelta(t.Context(), 0, 0)
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerUpdateResume(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 1, 10, 10)
	PauseTestPeers(peer)

	err := peer.resumeFromIdle(t.Context())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerInitSerial(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 1, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := store.initAllTablesSerial(t.Context())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestLMDPeerUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 3, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	peer.lastUpdate.Set(0)
	peer.setFlag(LMD)
	peer.setFlag(MultiBackend)
	store.setSyncStrategy()
	assert.IsTypef(t, &SyncStrategyLMD{}, store.sync, "expected sync strategy to be LMD")

	_, err := peer.tryUpdate(t.Context())
	require.NoError(t, err)

	peer.lastUpdate.Set(0)
	peer.resetFlags()
	peer.setFlag(MultiBackend)
	store.setSyncStrategy()
	assert.IsTypef(t, &SyncStrategyMultiBackend{}, store.sync, "expected sync strategy to be MultiBackend")
	_, err = peer.tryUpdate(t.Context())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestPeerLog(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(t, 1, 10, 10)
	PauseTestPeers(peer)

	peer.setBroken("test")
	peer.logPeerStatus(log.Debugf)
	err := peer.initTablesIfRestartRequiredError(t.Context(), fmt.Errorf("test"))
	require.Errorf(t, err, "got no error but expected broken peer")
	assert.Contains(t, err.Error(), "test")

	err = cleanup()
	require.NoError(t, err)
}
