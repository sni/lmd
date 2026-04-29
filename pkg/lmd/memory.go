package lmd

import (
	"fmt"
	"runtime"
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
		memStats.Alloc, float64(memStats.Alloc)/1024/1024,
		memStats.TotalAlloc, float64(memStats.TotalAlloc)/1024/1024,
		memStats.Sys, float64(memStats.Sys)/1024/1024,
		memStats.HeapAlloc, float64(memStats.HeapAlloc)/1024/1024,
		memStats.HeapSys, float64(memStats.HeapSys)/1024/1024,
		memStats.HeapIdle, float64(memStats.HeapIdle)/1024/1024,
		memStats.HeapInuse, float64(memStats.HeapInuse)/1024/1024,
		memStats.HeapReleased, float64(memStats.HeapReleased)/1024/1024,
		memStats.HeapObjects,
		memStats.StackInuse, float64(memStats.StackInuse)/1024/1024,
		memStats.StackSys, float64(memStats.StackSys)/1024/1024,
		memStats.Mallocs,
		memStats.Frees,
		memStats.NextGC, float64(memStats.NextGC)/1024/1024,
		memStats.LastGC,
		memStats.PauseTotalNs, float64(memStats.PauseTotalNs)/1000000,
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
