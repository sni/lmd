package lmd

import (
	"fmt"
	"runtime"
)

const (
	byteNextLevel          = 1024
	nanosecondToMilisecond = 1_000_000
)

func getMemoryDetails() (details string) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	details = fmt.Sprintf(
		"memory stats after object fetching:\n"+
			"Alloc: %d (%.2f MB)\n"+
			"TotalAlloc: %d (%.2f MB)\n"+
			"Sys: %d (%.2f MB)\n"+
			"HeapAlloc: %d (%.2f MB)\n"+
			"HeapSys: %d (%.2f MB)\n"+
			"HeapIdle: %d (%.2f MB)\n"+
			"HeapInuse: %d (%.2f MB)\n"+
			"HeapReleased: %d (%.2f MB)\n"+
			"HeapObjects: %d\n"+
			"StackInuse: %d (%.2f MB)\n"+
			"StackSys: %d (%.2f MB)\n"+
			"Mallocs: %d\n"+
			"Frees: %d\n"+
			"NextGC: %d (%.2f MB)\n"+
			"LastGC: %d\n"+
			"PauseTotalNs: %d ns (%.2f ms)\n"+
			"NumGC: %d\n"+
			"NumForcedGC: %d\n"+
			"GCCPUFraction: %.4f\n"+
			"PauseNs (last 5): %v\n"+
			"PauseEnd (last 5): %v",
		memStats.Alloc, float64(memStats.Alloc)/byteNextLevel/byteNextLevel,
		memStats.TotalAlloc, float64(memStats.TotalAlloc)/byteNextLevel/byteNextLevel,
		memStats.Sys, float64(memStats.Sys)/byteNextLevel/byteNextLevel,
		memStats.HeapAlloc, float64(memStats.HeapAlloc)/byteNextLevel/byteNextLevel,
		memStats.HeapSys, float64(memStats.HeapSys)/byteNextLevel/byteNextLevel,
		memStats.HeapIdle, float64(memStats.HeapIdle)/byteNextLevel/byteNextLevel,
		memStats.HeapInuse, float64(memStats.HeapInuse)/byteNextLevel/byteNextLevel,
		memStats.HeapReleased, float64(memStats.HeapReleased)/byteNextLevel/byteNextLevel,
		memStats.HeapObjects,
		memStats.StackInuse, float64(memStats.StackInuse)/byteNextLevel/byteNextLevel,
		memStats.StackSys, float64(memStats.StackSys)/byteNextLevel/byteNextLevel,
		memStats.Mallocs,
		memStats.Frees,
		memStats.NextGC, float64(memStats.NextGC)/byteNextLevel/byteNextLevel,
		memStats.LastGC,
		memStats.PauseTotalNs, float64(memStats.PauseTotalNs)/nanosecondToMilisecond,
		memStats.NumGC,
		memStats.NumForcedGC,
		memStats.GCCPUFraction,
		// Show last few pause times
		func() []uint64 {
			pauses := make([]uint64, 0, 5)
			for i := 0; i < 5 && i < int(memStats.NumGC); i++ {
				idx := (memStats.NumGC - uint32(i)) % 256
				pauses = append(pauses, memStats.PauseNs[idx])
			}

			return pauses
		}(),
		// Show last few pause end times
		func() []uint64 {
			pauseEnds := make([]uint64, 0, 5)
			for i := 0; i < 5 && i < int(memStats.NumGC); i++ {
				idx := (memStats.NumGC - uint32(i)) % 256
				pauseEnds = append(pauseEnds, memStats.PauseEnd[idx])
			}

			return pauseEnds
		}(),
	)

	return details
}
