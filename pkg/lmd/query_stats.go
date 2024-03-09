package lmd

import (
	"sort"
	"strings"
	"time"
)

// QueryStat is a single item of query statistics.
type QueryStat struct {
	Count         int64
	TotalDuration time.Duration
}

// QueryStatIn is the transport object for incoming count commands.
type QueryStatIn struct {
	Query    string
	Duration time.Duration
}

// QueryStats is the stats collector.
type QueryStats struct {
	Stats      map[string]*QueryStat
	In         chan QueryStatIn
	LogTrigger chan bool
}

// NewQueryStats creates a new query stats object.
func NewQueryStats() *QueryStats {
	stats := &QueryStats{
		Stats:      make(map[string]*QueryStat),
		In:         make(chan QueryStatIn),
		LogTrigger: make(chan bool),
	}

	go func() {
		for {
			select {
			case stat := <-stats.In:
				item, ok := stats.Stats[stat.Query]
				if !ok {
					item = &QueryStat{}
					stats.Stats[stat.Query] = item
				}
				item.Count++
				item.TotalDuration += stat.Duration
			case <-stats.LogTrigger:
				stats.LogStats()
			}
		}
	}()

	return stats
}

// LogStats returns the string result.
func (qs *QueryStats) LogStats() {
	if len(qs.Stats) == 0 {
		return
	}
	keys := make([]string, 0, len(qs.Stats))
	for key := range qs.Stats {
		// only show queries with more than a second total duration
		if qs.Stats[key].TotalDuration > time.Second {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return
	}
	// sort keys by total duration
	sort.Slice(keys, func(i, j int) bool { return qs.Stats[keys[i]].TotalDuration > qs.Stats[keys[j]].TotalDuration })

	log.Warnf("top 3 queries by duration:")
	for i, key := range keys {
		log.Warnf("%d) %9s / %3d#  %s", i+1, qs.Stats[key].TotalDuration.Truncate(time.Millisecond), qs.Stats[key].Count, strings.ReplaceAll(key, "\n", "\\n"))
		if i >= 2 {
			break
		}
	}

	// empty stats
	qs.Stats = make(map[string]*QueryStat)
}
