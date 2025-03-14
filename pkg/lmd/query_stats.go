package lmd

import (
	"sort"
	"strings"
	"time"

	"github.com/sasha-s/go-deadlock"
)

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
	lock       *deadlock.RWMutex // use when accessing stats
	in         chan QueryStatIn
	logTrigger chan bool
}

// NewQueryStats creates a new query stats object.
func NewQueryStats() *QueryStats {
	qStat := &QueryStats{
		lock:       new(deadlock.RWMutex),
		stats:      make(map[string]*QueryStat),
		in:         make(chan QueryStatIn),
		logTrigger: make(chan bool),
	}

	go func() {
		for {
			select {
			case stat := <-qStat.in:
				qStat.lock.RLock()
				item, ok := qStat.stats[stat.query]
				qStat.lock.RUnlock()
				qStat.lock.Lock()
				if !ok {
					item = &QueryStat{}
					qStat.stats[stat.query] = item
				}
				item.count++
				item.totalDuration += stat.duration
				qStat.lock.Unlock()
			case <-qStat.logTrigger:
				qStat.LogStats()
			}
		}
	}()

	return qStat
}

// LogStats returns the string result.
func (qs *QueryStats) LogStats() {
	qs.lock.RLock()
	if len(qs.stats) == 0 {
		qs.lock.RUnlock()

		return
	}
	qs.lock.RUnlock()
	qs.lock.Lock()
	defer qs.lock.Unlock()

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
