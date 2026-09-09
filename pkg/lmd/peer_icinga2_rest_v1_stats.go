package lmd

import (
	"slices"
	"strings"
)

type restv1ServiceStats struct {
	counts    map[string]int64
	names     []string
	worst     int8
	worstHard int8
}

func (s *restv1ServiceStats) add(row *DataRow) {
	if s.counts == nil {
		s.counts = make(map[string]int64)
	}
	s.names = append(s.names, row.GetStringByName("description"))
	s.counts["num_services"]++
	if row.GetInt8ByName("has_been_checked") == 0 {
		s.counts["num_services_pending"]++

		return
	}
	states := []string{"ok", "warn", "crit", "unknown"}
	state := row.GetInt8ByName("state")
	hard := row.GetInt8ByName("last_hard_state")
	if state >= 0 && int(state) < len(states) {
		s.counts["num_services_"+states[state]]++
		s.worst = restv1WorstState(s.worst, state)
	}
	if hard >= 0 && int(hard) < len(states) {
		s.counts["num_services_hard_"+states[hard]]++
		s.worstHard = restv1WorstState(s.worstHard, hard)
	}
}

func restv1WorstState(a, b int8) int8 {
	if a == 2 || b == 2 {
		return 2
	}

	return max(a, b)
}

func (s *restv1ServiceStats) apply(row *DataRow) {
	for name, col := range row.dataStore.table.columnsIndex {
		if col.DataType == Int64Col && (name == "num_services" || strings.HasPrefix(name, "num_services_")) {
			row.dataInt64[col.Index] = s.counts[name]
		}
	}
	row.dataInt[row.dataStore.table.GetColumn("worst_service_state").Index] = s.worst
	if col := row.dataStore.table.columnsIndex["worst_service_hard_state"]; col != nil {
		row.dataInt[col.Index] = s.worstHard
	}
}

func (ds *DataStoreSet) rebuildHostServiceStats() {
	hosts, services := ds.get(TableHosts), ds.get(TableServices)
	if hosts == nil || services == nil {
		return
	}
	byHost := make(map[string]*restv1ServiceStats)
	byGroup := make(map[string]*restv1ServiceStats)
	services.lock.RLock()
	for _, row := range services.data {
		host := row.GetStringByName("host_name")
		if byHost[host] == nil {
			byHost[host] = &restv1ServiceStats{}
		}
		byHost[host].add(row)
		for _, group := range row.GetStringListByName("groups") {
			if byGroup[group] == nil {
				byGroup[group] = &restv1ServiceStats{}
			}
			byGroup[group].add(row)
		}
	}
	services.lock.RUnlock()
	hosts.lock.Lock()
	for _, row := range hosts.data {
		stats := byHost[row.GetStringByName("name")]
		if stats == nil {
			stats = &restv1ServiceStats{}
		}
		stats.apply(row)
		slices.Sort(stats.names)
		row.dataStringList[hosts.table.GetColumn("services").Index] = stats.names
	}
	hosts.lock.Unlock()
	ds.restv1RebuildHostgroups(byHost)
	if groups := ds.get(TableServicegroups); groups != nil {
		groups.lock.Lock()
		for _, row := range groups.data {
			stats := byGroup[row.GetStringByName("name")]
			if stats == nil {
				stats = &restv1ServiceStats{}
			}
			stats.apply(row)
		}
		groups.lock.Unlock()
	}
}

func (ds *DataStoreSet) restv1RebuildHostgroups(byHost map[string]*restv1ServiceStats) {
	hosts := ds.get(TableHosts)
	hosts.lock.RLock()
	hostGroups := make(map[string]*restv1ServiceStats)
	hostCounts := make(map[string]map[string]int64)
	worstHosts := make(map[string]int8)
	for _, row := range hosts.data {
		for _, group := range row.GetStringListByName("groups") {
			if hostGroups[group] == nil {
				hostGroups[group] = &restv1ServiceStats{counts: make(map[string]int64)}
				hostCounts[group] = make(map[string]int64)
			}
			hostCounts[group]["num_hosts"]++
			state := row.GetInt8ByName("state")
			stateName := "pending"
			if row.GetInt8ByName("has_been_checked") != 0 && state >= 0 && state <= 2 {
				stateName = []string{"up", "down", "unreach"}[state]
				worstHosts[group] = max(worstHosts[group], state)
			}
			hostCounts[group]["num_hosts_"+stateName]++
			if stats := byHost[row.GetStringByName("name")]; stats != nil {
				for name, count := range stats.counts {
					hostGroups[group].counts[name] += count
				}
				hostGroups[group].worst = restv1WorstState(hostGroups[group].worst, stats.worst)
				hostGroups[group].worstHard = restv1WorstState(hostGroups[group].worstHard, stats.worstHard)
			}
		}
	}
	hosts.lock.RUnlock()
	if groups := ds.get(TableHostgroups); groups != nil {
		groups.lock.Lock()
		for _, row := range groups.data {
			name := row.GetStringByName("name")
			stats := hostGroups[name]
			if stats == nil {
				stats = &restv1ServiceStats{}
			}
			stats.apply(row)
			for _, colName := range []string{"num_hosts", "num_hosts_up", "num_hosts_down", "num_hosts_unreach", "num_hosts_pending"} {
				row.dataInt64[groups.table.GetColumn(colName).Index] = hostCounts[name][colName]
			}
			row.dataInt[groups.table.GetColumn("worst_host_state").Index] = worstHosts[name]
		}
		groups.lock.Unlock()
	}
}
