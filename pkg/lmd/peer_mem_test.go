//go:build !race

package lmd

import (
	"math"
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

	InitLogging(&Config{LogLevel: testLogLevel, LogFile: testLogTarget})

	mockLMD := createTestLMDInstance()
	sockets := StartMockLivestatusSource(mockLMD, 0, 1000, 15000)

	lmd := createTestLMDInstance()
	peer := NewPeer(lmd, &Connection{Source: []string{sockets}, Name: "TestPeer", ID: "testid"})

	err := peer.initAllTables(t.Context())
	require.NoError(t, err)
	getGoMemStats(t, true)

	// clean up again
	_, _, err = peer.QueryString("COMMAND [0] MOCK_EXIT")
	require.NoError(t, err)
	peer = nil //nolint:wastedassign // allow GC to free memory

	peak2, alloc2 := getGoMemStats(t, false)

	assert.LessOrEqualf(t, math.Abs(float64(alloc2-alloc1)), float64(5), "allocated memory should be more or less free again (5mb tolerance): before: %dMB / after: %dMB", alloc1, alloc2)
	assert.LessOrEqualf(t, peak2, uint64(250), "peak allocated memory should be less than 250MB after the test: %dMB", peak2)
}

// returns current heap allocation and peak rss in MB.
func getGoMemStats(tb testing.TB, doLog bool) (peakRSS, heapAlloc uint64) {
	tb.Helper()

	prev := debug.SetGCPercent(10)
	runtime.GC()
	debug.FreeOSMemory()
	debug.SetGCPercent(prev)

	var memStat runtime.MemStats
	runtime.ReadMemStats(&memStat)

	var rUse unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &rUse); err != nil {
		tb.Fatalf("failed to get memory usage: %s", err.Error())
	}

	// Maxrss is in kilobytes on Linux.
	peakRSS = uint64(rUse.Maxrss) * 1024

	if doLog {
		tb.Logf(
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
