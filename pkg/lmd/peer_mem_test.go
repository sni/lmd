//go:build !race

package lmd

import (
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestLMDPeerInitPeakMemoryUsage(t *testing.T) {
	peak1, alloc1 := getGoMemStats(t, false)
	assert.LessOrEqualf(t, peak1, uint64(100), "peak allocated memory should be less than 100MB before the test: %dMB", peak1)

	peer, cleanup, _ := StartTestPeer(1, 1000, 15000)
	PauseTestPeers(peer)

	store := peer.data.Load()
	peer.lastUpdate.Set(0)
	peer.setFlag(LMD)
	store.setSyncStrategy()
	assert.IsTypef(t, &SyncStrategyLMD{}, store.sync, "expected sync strategy to be LMD")

	updated, err := peer.tryUpdate(t.Context())
	assert.True(t, updated, "expected update to be successful")
	require.NoError(t, err)

	err = cleanup()
	peak2, alloc2 := getGoMemStats(t, true)

	assert.LessOrEqualf(t, alloc2-alloc1, uint64(5), "allocated memory should be more or less free again (5mb tolerance): before: %dMB / after: %dMB", alloc1, alloc2)
	assert.LessOrEqualf(t, peak2, uint64(250), "peak allocated memory should be less than 250MB after the test: %dMB", peak2)

	require.NoError(t, err)
}

// returns current heap allocation and peak rss in MB.
func getGoMemStats(t *testing.T, doLog bool) (peakRSS, heapAlloc uint64) {
	t.Helper()

	debug.SetGCPercent(10)
	runtime.GC()
	debug.FreeOSMemory()

	var memStat runtime.MemStats
	runtime.ReadMemStats(&memStat)

	var rUse unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &rUse); err != nil {
		t.Fatalf("failed to get memory usage: %s", err.Error())
	}

	// Maxrss is in kilobytes on Linux.
	peakRSS = uint64(rUse.Maxrss) * 1024

	if doLog {
		t.Logf(
			"peak=%03dMB heap_alloc=%03dMB heap_sys=%03dMB stack_sys=%03dMB heap_inuse=%03dMB next_gc=%03dMB",
			peakRSS>>20,
			memStat.HeapAlloc>>20,
			memStat.HeapSys>>20,
			memStat.StackSys>>20,
			memStat.HeapInuse>>20,
			memStat.NextGC>>20,
		)
	}

	return peakRSS >> 20, memStat.HeapAlloc >> 20
}
