package lmd

import (
	"sort"
	"strings"
	"time"
)

const queryLogQueueSize = 100

// QueryStat is a single item of query statistics.
type QueryStat struct {
	count         int64
	totalDuration time.Duration
}

// QueryStatIn is the transport object for incoming count commands.
type QueryStatIn struct {
	query    string
	duration time.Duration
}

// QueryStats is the stats collector.
type QueryStats struct {
	stats      map[string]*QueryStat
	in         chan QueryStatIn
	logTrigger chan bool
	done       chan bool // channel to close the stats collector
}

// NewQueryStats creates a new query stats object.
func NewQueryStats() *QueryStats {
	qStat := &QueryStats{
		stats:      make(map[string]*QueryStat),
		in:         make(chan QueryStatIn, queryLogQueueSize),
		logTrigger: make(chan bool),
		done:       make(chan bool, 10),
	}

	go func() {
		for {
			select {
			case stat := <-qStat.in:
				item, ok := qStat.stats[stat.query]
				if !ok {
					item = &QueryStat{}
					qStat.stats[stat.query] = item
				}
				item.count++
				item.totalDuration += stat.duration
			case <-qStat.logTrigger:
				qStat.logStats()
			case <-qStat.done:
				log.Debugf("query stats collector finished")

				return
			}
		}
	}()

	return qStat
}

func (qs *QueryStats) stop() {
	if qs != nil {
		qs.done <- true
	}
}

// logStats logs the top 3 queries by duration and resets the stats.
func (qs *QueryStats) logStats() {
	if len(qs.stats) == 0 {
		return
	}

	keys := make([]string, 0, len(qs.stats))
	for key := range qs.stats {
		// only show queries with more than a second total duration
		if qs.stats[key].totalDuration > time.Second {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return
	}
	// sort keys by total duration
	sort.Slice(keys, func(i, j int) bool { return qs.stats[keys[i]].totalDuration > qs.stats[keys[j]].totalDuration })

	log.Warnf("top 3 queries by duration:")
	for i, key := range keys {
		log.Warnf("%d) %9s / %3d#  %s", i+1, qs.stats[key].totalDuration.Truncate(time.Millisecond), qs.stats[key].count, strings.ReplaceAll(key, "\n", "\\n"))
		if i >= 2 {
			break
		}
	}

	// empty stats
	qs.stats = make(map[string]*QueryStat)
}
