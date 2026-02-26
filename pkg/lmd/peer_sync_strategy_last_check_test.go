package lmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerUpdateLastCheck(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	store := peer.data.Load()
	err := store.updateFull(t.Context())
	require.NoError(t, err)

	store.sync = &SyncStrategyLastCheck{}
	store.sync.Init(store)

	assert.IsTypef(t, &SyncStrategyLastCheck{}, store.sync, "expected sync strategy to be LastCheck")

	// fake some last_update entries
	data, _ := peer.getDataStoreSet()
	svcTbl, _ := peer.getDataStore(TableServices)
	lastCheckCol := svcTbl.GetColumn("last_check")
	for _, row := range svcTbl.data {
		row.dataInt64[lastCheckCol.Index] = 2
	}
	err = data.updateDelta(t.Context(), float64(5), float64(time.Now().Unix()+5))
	require.NoError(t, err)

	peer.lastUpdate.Set(0)
	updated, err := peer.tryUpdate(t.Context())
	require.NoError(t, err)
	assert.True(t, updated, "expected update to be successful")

	peer.lastUpdate.Set(0)
	peer.lastTimeperiodUpdateMinute.Store(0)
	updated, err = store.tryTimeperiodsUpdate(t.Context())
	require.NoError(t, err)
	assert.True(t, updated, "expected update to be successful")

	peer.lastUpdate.Set(0)
	peer.peerState.Set(PeerStatusWarning)
	updated, err = peer.tryUpdate(t.Context())
	require.NoError(t, err)
	assert.True(t, updated, "expected update to be successful")

	peer.lastUpdate.Set(0)
	peer.peerState.Set(PeerStatusDown)
	updated, err = peer.tryUpdate(t.Context())
	require.NoError(t, err)
	assert.True(t, updated, "expected update to be successful")

	cList := peer.data.Load().get(TableComments).table.GetColumns([]string{"id", "host_name", "service_description", "entry_time", "author", "comment", "persistent"})
	err = peer.data.Load().get(TableComments).AppendData(ResultSet([][]any{{"666", "test", "svc", 123456, "author", "comment", 0}}), cList)
	require.NoError(t, err)
	err = peer.data.Load().updateDelta(t.Context(), float64(time.Now().Unix())-60, float64(time.Now().Unix()))
	require.NoError(t, err)

	store = peer.data.Load()
	store.sync = &SyncStrategyLastCheck{}
	store.sync.Init(store)

	peer.lastUpdate.Set(0)
	peer.peerState.Set(PeerStatusBroken)
	updated, err = peer.tryUpdate(t.Context())
	require.Errorf(t, err, "got no error but expected broken peer")
	assert.Contains(t, err.Error(), "waiting for peer to recover")
	assert.False(t, updated, "expected update to be unsuccessful")

	err = cleanup()
	require.NoError(t, err)
}
