package lmd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComposeTimestamp1(t *testing.T) {
	timestamps := []int64{1, 3, 5}
	expect := []string{
		"Filter: last_update = 1\n",
		"Filter: last_update = 3\n",
		"Filter: last_update = 5\n",
		"Or: 3\n",
	}
	assert.Equal(t, expect, composeTimestampFilter(timestamps, "last_update"))
}

func TestComposeTimestamp2(t *testing.T) {
	ts := []int64{1, 2, 3}
	expect := []string{
		"Filter: last_check >= 1\nFilter: last_check <= 3\nAnd: 2\n",
	}
	assert.Equal(t, expect, composeTimestampFilter(ts, "last_check"))
}

func TestComposeTimestamp3(t *testing.T) {
	timestamps := []int64{1, 2, 3, 5, 7, 8, 9}
	expect := []string{
		"Filter: last_check >= 1\nFilter: last_check <= 3\nAnd: 2\n",
		"Filter: last_check = 5\n",
		"Filter: last_check >= 7\nFilter: last_check <= 9\nAnd: 2\n",
		"Or: 3\n",
	}
	assert.Equal(t, expect, composeTimestampFilter(timestamps, "last_check"))
}

func TestDSHasChanged(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	err := peer.data.reloadIfNumberOfObjectsChanged(context.TODO())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestDSFullUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	peer.statusSetLocked(LastUpdate, float64(0))
	peer.statusSetLocked(LastFullServiceUpdate, float64(0))
	err := peer.data.UpdateDeltaServices(context.TODO(), fmt.Sprintf("Filter: host_name = %s\nFilter: description = %s\n", "test", "test"), false, 0)
	require.NoError(t, err)

	peer.statusSetLocked(LastUpdate, float64(0))
	peer.statusSetLocked(LastFullServiceUpdate, float64(0))
	err = peer.data.UpdateDeltaServices(context.TODO(), fmt.Sprintf("Filter: host_name = %s\nFilter: description = %s\n", "test", "test"), true, time.Now().Unix())
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}

func TestDSDowntimesComments(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	err := peer.data.buildDowntimeCommentsList(TableComments)
	require.NoError(t, err)

	err = peer.data.buildDowntimeCommentsList(TableDowntimes)
	require.NoError(t, err)

	err = cleanup()
	require.NoError(t, err)
}
