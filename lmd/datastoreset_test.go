package main

import (
	"fmt"
	"testing"
)

func TestComposeTimestamp1(t *testing.T) {
	ts := []int64{1, 3, 5}
	expect := []string{
		"Filter: last_update = 1\n",
		"Filter: last_update = 3\n",
		"Filter: last_update = 5\n",
		"Or: 3\n",
	}
	if err := assertEq(expect, composeTimestampFilter(ts, "last_update")); err != nil {
		t.Error(err)
	}
}

func TestComposeTimestamp2(t *testing.T) {
	ts := []int64{1, 2, 3}
	expect := []string{
		"Filter: last_check >= 1\nFilter: last_check <= 3\nAnd: 2\n",
	}
	if err := assertEq(expect, composeTimestampFilter(ts, "last_check")); err != nil {
		t.Error(err)
	}
}

func TestComposeTimestamp3(t *testing.T) {
	ts := []int64{1, 2, 3, 5, 7, 8, 9}
	expect := []string{
		"Filter: last_check >= 1\nFilter: last_check <= 3\nAnd: 2\n",
		"Filter: last_check = 5\n",
		"Filter: last_check >= 7\nFilter: last_check <= 9\nAnd: 2\n",
		"Or: 3\n",
	}
	if err := assertEq(expect, composeTimestampFilter(ts, "last_check")); err != nil {
		t.Error(err)
	}
}

func TestDSHasChanged(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	err := peer.data.reloadIfNumberOfObjectsChanged()
	if err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestDSFullUpdate(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	peer.StatusSet(LastUpdate, int64(0))
	peer.StatusSet(LastFullServiceUpdate, int64(0))
	err := peer.data.UpdateDeltaServices(fmt.Sprintf("Filter: host_name = %s\nFilter: description = %s\n", "test", "test"), false)
	if err != nil {
		t.Error(err)
	}

	peer.StatusSet(LastUpdate, int64(0))
	peer.StatusSet(LastFullServiceUpdate, int64(0))
	err = peer.data.UpdateDeltaServices(fmt.Sprintf("Filter: host_name = %s\nFilter: description = %s\n", "test", "test"), true)
	if err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}

func TestDSDowntimesComments(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	err := peer.data.buildDowntimeCommentsList(TableComments)
	if err != nil {
		t.Error(err)
	}

	err = peer.data.buildDowntimeCommentsList(TableDowntimes)
	if err != nil {
		t.Error(err)
	}

	if err := cleanup(); err != nil {
		panic(err.Error())
	}
}
