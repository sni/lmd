package lmd

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ensure we fully implement the SyncStrategy type.
var _ SyncStrategy = &SyncStrategyLastCheck{}

// SyncStrategyLastCheck synchronizes with peers based on the last_check time.
type SyncStrategyLastCheck struct {
	store                 *DataStoreSet
	lastFullHostUpdate    float64
	lastFullServiceUpdate float64
}

func (s *SyncStrategyLastCheck) Init(store *DataStoreSet) {
	s.store = store
}

// UpdateDelta runs a delta update on all status, hosts, services, comments and downtimes table.
// It returns true if the update was successful or false otherwise.
func (s *SyncStrategyLastCheck) UpdateDelta(ctx context.Context, lastUpdate, now float64) (err error) {
	store := s.store
	if store == nil {
		return nil
	}

	// update comments and downtimes
	err = store.updateCommentsAndDowntimes(ctx)
	if err != nil {
		return err
	}

	updateOffset := float64(store.peer.lmd.Config.UpdateOffset)
	filterStr := ""
	if lastUpdate > 0 {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\nFilter: last_check < %v\nAnd: 2\n",
			int64(lastUpdate-updateOffset), int64(now-updateOffset))
		if store.peer.lmd.Config.SyncIsExecuting && !store.peer.hasFlag(Shinken) {
			filterStr += "Filter: is_executing = 1\nOr: 2\n"
		}
	}

	hostStore := store.get(TableHosts)
	serviceStore := store.get(TableServices)
	updateThreshold := int64(lastUpdate - updateOffset)

	// no need to run full scan update directly after startup
	if s.lastFullHostUpdate == 0 {
		s.lastFullHostUpdate = currentUnixTime()
		s.lastFullServiceUpdate = s.lastFullHostUpdate
	}

	updated, err := s.updateFullScan(ctx, hostStore, &s.lastFullHostUpdate, filterStr, updateThreshold, store.updateDeltaHosts)
	if err != nil {
		return err
	}

	if !updated {
		_, _, err = store.updateDeltaHosts(ctx, filterStr)
		if err != nil {
			return err
		}
	}

	updated, err = s.updateFullScan(ctx, serviceStore, &s.lastFullServiceUpdate, filterStr, updateThreshold, store.updateDeltaServices)
	if err != nil {
		return err
	}

	if !updated {
		_, _, err = store.updateDeltaServices(ctx, filterStr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SyncStrategyLastCheck) UpdateTick(ctx context.Context) (err error) {
	store := s.store
	if store == nil {
		return nil
	}

	_, err = store.tryTimeperiodsUpdate(ctx)
	if err != nil {
		return err
	}

	err = store.tryOnDemandComDownUpdate(ctx)
	if err != nil {
		return err
	}

	return nil
}

type fullUpdateCb func(context.Context, string) (ResultSet, []*ResultPrepared, error)

// updateFullScan updates hosts and services tables by fetching some key indicator fields like last_check
// downtimes or acknowledged status. If an update is required, the last_check timestamp is used as filter for a
// delta update.
// The full scan just returns false without any update if the last update was less then MinFullScanInterval seconds ago.
// It returns true if an update was done and any error encountered.
//
//nolint:lll // it is what it is...
func (s *SyncStrategyLastCheck) updateFullScan(ctx context.Context, store *DataStore, lastUpdate *float64, filter string, updateThr int64, updateFn fullUpdateCb) (updated bool, err error) {
	peer := s.store.peer

	// do not do a full scan more often than every 60 seconds
	if *lastUpdate > float64(time.Now().Unix()-MinFullScanInterval) {
		return false, nil
	}

	now := currentUnixTime()

	scanColumns := []string{
		"last_check",
		"scheduled_downtime_depth",
		"acknowledged",
		"active_checks_enabled",
		"notifications_enabled",
		"modified_attributes",
	}

	// if update is based on last_check, we add next_check here which is not required when updating based on lmd_last_cache_update or last_update
	if strings.Contains(filter, "last_check") {
		scanColumns = append(scanColumns, "next_check")
	}

	// used to sort result later
	scanColumns = append(scanColumns, store.table.primaryKey...)
	req := &Request{
		Table:   store.table.name,
		Columns: scanColumns,
	}
	peer.setQueryOptions(req)
	res, _, err := peer.Query(ctx, req)
	if err != nil {
		return false, err
	}

	columns := make(ColumnList, len(scanColumns))
	for i, name := range scanColumns {
		col := store.table.columnsIndex[name]
		columns[i] = col
	}

	missing, err := s.getMissingTimestamps(ctx, store, res, columns, updateThr)
	if err != nil {
		return false, err
	}

	if len(missing) == 0 {
		logWith(store, req).Tracef("%s delta full scan did not find any timestamps", store.table.name.String())

		return false, nil
	}

	logWith(store, req).Debugf("%s delta full scan going to update %d timestamps", store.table.name.String(), len(missing))
	timestampFilter := s.composeTimestampFilter(missing, "last_check")
	if len(timestampFilter) > missedTimestampMaxFilter {
		msg := fmt.Sprintf("%s delta full scan timestamp filter too complex: %d", store.table.name.String(), len(timestampFilter))
		logWith(store, req).Debugf("%s", msg)
		// sync at least a few to get back on track
		missing = missing[0 : missedTimestampMaxFilter-1]
		timestampFilter = s.composeTimestampFilter(missing, "last_check")
	}

	filterList := []string{}
	if filter != "" {
		filterList = append(filterList, filter)
		if len(timestampFilter) > 0 {
			filterList = append(filterList, timestampFilter...)
			filterList = append(filterList, "Or: 2\n")
		}
	} else {
		filterList = append(filterList, timestampFilter...)
	}

	_, _, err = updateFn(ctx, strings.Join(filterList, ""))
	if err != nil {
		return false, err
	}

	*lastUpdate = now

	return true, nil
}

// getMissingTimestamps returns list of last_check dates which can be used to delta update.
func (s *SyncStrategyLastCheck) getMissingTimestamps(ctx context.Context, store *DataStore, res ResultSet, columns ColumnList, updateThr int64) (missing []int64, err error) {
	store.lock.RLock()
	peer := s.store.peer
	data := store.data
	if len(data) < len(res) {
		store.lock.RUnlock()
		if peer.hasFlag(Icinga2) || len(data) == 0 {
			err = s.reloadIfNumberOfObjectsChanged(ctx)

			return missing, err
		}

		err = &PeerError{msg: fmt.Sprintf("%s cache not ready, got %d entries but only have %d in cache",
			store.table.name.String(), len(res), len(data)), kind: ResponseError}
		logWith(peer).Warnf("%s", err.Error())
		peer.setBroken(fmt.Sprintf("got more %s than expected. Hint: check clients 'max_response_size' setting.", store.table.name.String()))

		return nil, err
	}

	missedUnique := make(map[int64]bool)
	for i, row := range res {
		ts := interface2int64(row[0])
		if ts >= updateThr {
			continue
		}
		if data[i].checkChangedIntValues(0, row, columns) {
			missedUnique[ts] = true
		}
	}
	store.lock.RUnlock()

	// return uniq sorted keys
	missing = make([]int64, len(missedUnique))
	i := 0
	for lastCheck := range missedUnique {
		missing[i] = lastCheck
		i++
	}
	slices.Sort(missing)

	return missing, nil
}

func (s *SyncStrategyLastCheck) reloadIfNumberOfObjectsChanged(ctx context.Context) (err error) {
	if s.hasChanged(ctx) {
		return (s.store.peer.initAllTables(ctx))
	}

	return err
}

func (s *SyncStrategyLastCheck) hasChanged(ctx context.Context) (changed bool) {
	peer := s.store.peer
	changed = false
	tablenames := []TableName{TableCommands, TableContactgroups, TableContacts, TableHostgroups, TableHosts, TableServicegroups, TableTimeperiods}
	for _, name := range tablenames {
		counter := peer.countQuery(ctx, name.String(), "name !=")
		table := s.store.get(name)
		table.lock.RLock()
		if counter >= 0 {
			changed = changed || (counter != len(table.data))
		}
		table.lock.RUnlock()
	}
	counter := peer.countQuery(ctx, "services", "host_name !=")
	table := s.store.get(TableServices)
	table.lock.RLock()
	if counter >= 0 {
		changed = changed || (counter != len(table.data))
	}
	table.lock.RUnlock()
	peer.clearLastRequest()

	return changed
}

func (s *SyncStrategyLastCheck) composeTimestampFilter(timestamps []int64, attribute string) []string {
	filter := []string{}
	block := struct {
		start int64
		end   int64
	}{-1, -1}
	for _, timestamp := range timestamps {
		if block.start == -1 {
			block.start = timestamp
			block.end = timestamp

			continue
		}
		if block.end == timestamp-1 {
			block.end = timestamp

			continue
		}

		if block.start != -1 {
			if block.start == block.end {
				filter = append(filter, fmt.Sprintf("Filter: %s = %d\n", attribute, block.start))
			} else {
				filter = append(filter, fmt.Sprintf("Filter: %s >= %d\nFilter: %s <= %d\nAnd: 2\n", attribute, block.start, attribute, block.end))
			}
		}

		block.start = timestamp
		block.end = timestamp
	}
	if block.start != -1 {
		if block.start == block.end {
			filter = append(filter, fmt.Sprintf("Filter: %s = %d\n", attribute, block.start))
		} else {
			filter = append(filter, fmt.Sprintf("Filter: %s >= %d\nFilter: %s <= %d\nAnd: 2\n", attribute, block.start, attribute, block.end))
		}
	}
	if len(filter) > 1 {
		filter = append(filter, fmt.Sprintf("Or: %d\n", len(filter)))
	}

	return filter
}
