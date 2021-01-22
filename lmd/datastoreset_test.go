package main

import (
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
