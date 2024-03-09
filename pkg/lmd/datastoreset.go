package lmd

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sasha-s/go-deadlock"
)

const missedTimestampMaxFilter = 150

// DataStoreSet is the handle to a peers datastore.
type DataStoreSet struct {
	peer   *Peer
	Lock   *deadlock.RWMutex
	tables map[TableName]*DataStore
}

func NewDataStoreSet(peer *Peer) *DataStoreSet {
	dataset := DataStoreSet{
		Lock:   new(deadlock.RWMutex),
		tables: make(map[TableName]*DataStore),
		peer:   peer,
	}

	return &dataset
}

func (ds *DataStoreSet) Set(name TableName, store *DataStore) {
	ds.Lock.Lock()
	ds.tables[name] = store
	store.DataSet = ds
	ds.Lock.Unlock()
}

func (ds *DataStoreSet) Get(name TableName) *DataStore {
	ds.Lock.RLock()
	store := ds.tables[name]
	ds.Lock.RUnlock()

	return store
}

// CreateObjectByType fetches all static and dynamic data from the remote site and creates the initial table.
// It returns any error encountered.
func (ds *DataStoreSet) CreateObjectByType(ctx context.Context, table *Table) (store *DataStore, err error) {
	peer := ds.peer

	store = NewDataStore(table, peer)
	store.DataSet = ds
	keys, columns := store.GetInitialColumns()

	// fetch remote objects
	req := &Request{
		Table:   store.Table.Name,
		Columns: keys,
	}
	peer.setQueryOptions(req)
	res, resMeta, err := peer.Query(ctx, req)
	if err != nil {
		return nil, err
	}

	time1 := time.Now()

	// verify result set
	keyLen := len(keys)
	for i, row := range res {
		if len(row) != keyLen {
			err = fmt.Errorf("%s result set verification failed: len mismatch in row %d, expected %d columns and got %d", store.Table.Name.String(), i, keyLen, len(row))
			if peer.ErrorCount > 0 {
				// silently cancel, backend broke during initialization, should have been logged already
				log.Debugf("error during %s initialization, but backend is already failed: %s", &table.Name, err.Error())
			} else {
				log.Errorf("error during %s initialization: %s", &table.Name, err.Error())
			}

			return nil, err
		}
	}

	// make sure all backends are sorted the same way
	res = res.SortByPrimaryKey(table, req)

	durationPrepare := time.Since(time1).Truncate(time.Millisecond)
	time2 := time.Now()

	now := currentUnixTime()
	err = store.InsertData(res, columns, false)
	if err != nil {
		return nil, err
	}

	durationInsert := time.Since(time2).Truncate(time.Millisecond)

	time3 := time.Now()

	peer.lock.Lock()
	peer.LastUpdate = now
	peer.LastFullUpdate = now
	peer.lock.Unlock()
	durationLock := time.Since(time3).Truncate(time.Millisecond)

	tableName := table.Name.String()
	promObjectCount.WithLabelValues(peer.Name, tableName).Set(float64(len(res)))

	logWith(peer, req).Debugf("initial table: %15s - fetch: %9s - prepare: %9s - lock: %9s - insert: %9s - count: %8d - size: %8d kB",
		tableName, resMeta.Duration.Truncate(time.Millisecond), durationPrepare, durationLock, durationInsert, len(res), resMeta.Size/1024)

	return store, nil
}

// SetReferences creates reference entries for all tables.
func (ds *DataStoreSet) SetReferences() (err error) {
	for _, t := range ds.tables {
		t := t
		err = t.SetReferences()
		if err != nil {
			logWith(ds).Debugf("setting references on table %s failed: %s", t.Table.Name.String(), err.Error())

			return
		}
	}

	return
}

func (ds *DataStoreSet) hasChanged(ctx context.Context) (changed bool) {
	changed = false
	tablenames := []TableName{TableCommands, TableContactgroups, TableContacts, TableHostgroups, TableHosts, TableServicegroups, TableTimeperiods}
	for _, name := range tablenames {
		counter := ds.peer.countFromServer(ctx, name.String(), "name !=")
		ds.Lock.RLock()
		changed = changed || (counter != len(ds.tables[name].Data))
		ds.Lock.RUnlock()
	}
	counter := ds.peer.countFromServer(ctx, "services", "host_name !=")
	ds.Lock.RLock()
	changed = changed || (counter != len(ds.tables[TableServices].Data))
	ds.Lock.RUnlock()
	ds.peer.clearLastRequest()

	return
}

// UpdateFull runs a full update on all dynamic values for all tables which have dynamic updated columns.
// It returns any error occurred or nil if the update was successful.
func (ds *DataStoreSet) UpdateFull(ctx context.Context, tables []TableName) (err error) {
	time1 := time.Now()
	err = ds.UpdateFullTablesList(ctx, tables)
	if err != nil {
		return
	}
	peer := ds.peer
	duration := time.Since(time1)
	peer.lock.RLock()
	switch peer.PeerState {
	case PeerStatusUp, PeerStatusPending, PeerStatusSyncing:
	default:
		logWith(peer).Infof("site soft recovered from short outage (reason: %s - %s)", peer.PeerState.String(), peer.LastError)
	}
	peer.lock.RUnlock()
	peer.lock.Lock()
	peer.resetErrors()
	peer.ResponseTime = duration.Seconds()
	peer.LastUpdate = currentUnixTime()
	peer.LastFullUpdate = currentUnixTime()
	peer.lock.Unlock()
	logWith(peer).Debugf("full update complete in: %s", duration.String())
	promPeerUpdates.WithLabelValues(peer.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(peer.Name).Set(duration.Seconds())

	return
}

// UpdateFullTablesList updates list of tables and returns any error.
func (ds *DataStoreSet) UpdateFullTablesList(ctx context.Context, tables []TableName) (err error) {
	for i := range tables {
		name := tables[i]
		err = ds.UpdateFullTable(ctx, name)
		if err != nil {
			logWith(ds).Debugf("update failed: %s", err.Error())

			return
		}
	}

	return
}

// UpdateDelta runs a delta update on all status, hosts, services, comments and downtimes table.
// It returns true if the update was successful or false otherwise.
func (ds *DataStoreSet) UpdateDelta(ctx context.Context, from, until float64) (err error) {
	time1 := time.Now()

	err = ds.UpdateFullTablesList(ctx, Objects.StatusTables)
	if err != nil {
		return err
	}

	updateOffset := float64(ds.peer.lmd.Config.UpdateOffset)
	updateThreshold := int64(from - updateOffset)

	filterStr := ""
	if from > 0 {
		switch {
		case ds.peer.HasFlag(HasLMDLastCacheUpdateColumn):
			filterStr = fmt.Sprintf("Filter: lmd_last_cache_update >= %v\nFilter: lmd_last_cache_update < %v\nAnd: 2\n",
				int64(from-updateOffset), int64(until-updateOffset))
		case ds.peer.HasFlag(HasLastUpdateColumn):
			filterStr = fmt.Sprintf("Filter: last_update >= %v\nFilter: last_update < %v\nAnd: 2\n",
				int64(from-updateOffset), int64(until-updateOffset))
		default:
			filterStr = fmt.Sprintf("Filter: last_check >= %v\nFilter: last_check < %v\nAnd: 2\n",
				int64(from-updateOffset), int64(until-updateOffset))
			if ds.peer.lmd.Config.SyncIsExecuting && !ds.peer.HasFlag(Shinken) {
				filterStr += "Filter: is_executing = 1\nOr: 2\n"
			}
		}
	}
	err = ds.UpdateDeltaHosts(ctx, filterStr, true, updateThreshold)
	if err != nil {
		return err
	}
	err = ds.UpdateDeltaServices(ctx, filterStr, true, updateThreshold)
	if err != nil {
		return err
	}
	err = ds.UpdateDeltaCommentsOrDowntimes(ctx, TableComments)
	if err != nil {
		return err
	}
	err = ds.UpdateDeltaCommentsOrDowntimes(ctx, TableDowntimes)
	if err != nil {
		return err
	}

	peer := ds.peer
	duration := time.Since(time1)
	logWith(peer).Debugf("delta update complete in: %s", duration.Truncate(time.Millisecond).String())

	peer.lock.Lock()
	peerStatus := peer.PeerState
	peer.resetErrors()
	peer.LastUpdate = until
	peer.ResponseTime = duration.Seconds()
	peer.lock.Unlock()

	if peerStatus != PeerStatusUp && peerStatus != PeerStatusPending {
		logWith(peer).Infof("site soft recovered from short outage")
	}

	promPeerUpdates.WithLabelValues(peer.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(peer.Name).Set(duration.Seconds())

	return nil
}

// UpdateDeltaHosts update hosts by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional updateOffset seconds.
// It returns any error encountered.
func (ds *DataStoreSet) UpdateDeltaHosts(ctx context.Context, filterStr string, tryFullScan bool, updateThreshold int64) (err error) {
	return ds.updateDeltaHostsServices(ctx, TableHosts, filterStr, tryFullScan, updateThreshold)
}

// UpdateDeltaServices update services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional updateOffset seconds.
// It returns any error encountered.
func (ds *DataStoreSet) UpdateDeltaServices(ctx context.Context, filterStr string, tryFullScan bool, updateThreshold int64) (err error) {
	return ds.updateDeltaHostsServices(ctx, TableServices, filterStr, tryFullScan, updateThreshold)
}

// updateDeltaHostsServices update hosts / services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional updateOffset seconds.
// It returns any error encountered.
func (ds *DataStoreSet) updateDeltaHostsServices(ctx context.Context, tableName TableName, filterStr string, tryFullScan bool, updateThreshold int64) (err error) {
	// update changed services
	table := ds.Get(tableName)
	if table == nil {
		return fmt.Errorf("peer not ready, either not initialized or went offline recently")
	}
	peer := ds.peer

	if tryFullScan {
		// run regular delta update and lets check if all last_check dates match
		updated, uErr := ds.UpdateDeltaFullScanHostsServices(ctx, table, filterStr, updateThreshold)
		if updated || uErr != nil {
			return uErr
		}
	}
	req := &Request{
		Table:     tableName,
		Columns:   table.DynamicColumnNamesCache,
		FilterStr: filterStr,
	}
	peer.setQueryOptions(req)
	res, meta, err := peer.Query(ctx, req)
	if err != nil {
		return err
	}

	// make sure all backends are sorted the same way
	res = res.SortByPrimaryKey(table.Table, req)

	switch tableName {
	case TableHosts:
		hostDataOffset := 1

		return ds.insertDeltaDataResult(hostDataOffset, res, meta, table)
	case TableServices:
		serviceDataOffset := 2

		return ds.insertDeltaDataResult(serviceDataOffset, res, meta, table)
	default:
		logWith(peer, req).Panicf("not implemented for: %s", tableName.String())
	}

	return nil
}

func (ds *DataStoreSet) insertDeltaDataResult(dataOffset int, res ResultSet, resMeta *ResultMetaData, table *DataStore) (err error) {
	time1 := time.Now()
	updateSet, err := table.prepareDataUpdateSet(dataOffset, res, table.DynamicColumnCache)
	if err != nil {
		return err
	}
	durationPrepare := time.Since(time1).Truncate(time.Millisecond)

	now := currentUnixTime()
	time2 := time.Now()

	ds.Lock.Lock()
	durationLock := time.Since(time2).Truncate(time.Millisecond)
	time3 := time.Now()

	for _, update := range updateSet {
		if update.FullUpdate {
			err = update.DataRow.UpdateValues(dataOffset, update.ResultRow, table.DynamicColumnCache, now)
		} else {
			err = update.DataRow.UpdateValuesNumberOnly(dataOffset, update.ResultRow, table.DynamicColumnCache, now)
		}
		if err != nil {
			ds.Lock.Unlock()

			return err
		}
	}
	ds.Lock.Unlock()

	durationInsert := time.Since(time3).Truncate(time.Millisecond)

	p := ds.peer
	tableName := table.Table.Name.String()
	promObjectUpdate.WithLabelValues(p.Name, tableName).Add(float64(len(res)))
	logWith(p, resMeta.Request).Debugf("up delta table: %15s - fetch: %9s - prep: %9s - lock: %9s - insert: %9s - count: %8d - size: %8d kB",
		tableName, resMeta.Duration.Truncate(time.Millisecond), durationPrepare, durationLock, durationInsert, len(updateSet), resMeta.Size/1024)

	return nil
}

// UpdateDeltaFullScanHostsServices is a table independent wrapper for UpdateDeltaFullScan
// It returns true if an update was done and any error encountered.
func (ds *DataStoreSet) UpdateDeltaFullScanHostsServices(ctx context.Context, store *DataStore, filterStr string, updateThreshold int64) (updated bool, err error) {
	switch store.Table.Name {
	case TableServices:
		updated, err = ds.UpdateFullScan(ctx, store, LastFullServiceUpdate, filterStr, updateThreshold, ds.UpdateDeltaServices)
	case TableHosts:
		updated, err = ds.UpdateFullScan(ctx, store, LastFullHostUpdate, filterStr, updateThreshold, ds.UpdateDeltaHosts)
	default:
		p := ds.peer
		logWith(p).Panicf("not implemented for: " + store.Table.Name.String())
	}

	return
}

type fullUpdateCb func(context.Context, string, bool, int64) error

// UpdateFullScan updates hosts and services tables by fetching some key indicator fields like last_check
// downtimes or acknowledged status. If an update is required, the last_check timestamp is used as filter for a
// delta update.
// The full scan just returns false without any update if the last update was less then MinFullScanInterval seconds ago.
// It returns true if an update was done and any error encountered.
//
//nolint:lll // it is what it is...
func (ds *DataStoreSet) UpdateFullScan(ctx context.Context, store *DataStore, statusKey PeerStatusKey, filter string, updateThr int64, updateFn fullUpdateCb) (updated bool, err error) {
	peer := ds.peer
	lastUpdate, ok := peer.statusGetLocked(statusKey).(float64)
	if !ok {
		log.Panicf("status key %#v did not return a float64", statusKey)
	}

	// do not do a full scan more often than every 60 seconds
	if lastUpdate > float64(time.Now().Unix()-MinFullScanInterval) {
		return false, nil
	}

	scanColumns := []string{
		"last_check",
		"next_check",
		"scheduled_downtime_depth",
		"acknowledged",
		"active_checks_enabled",
		"notifications_enabled",
		"modified_attributes",
	}
	// used to sort result later
	scanColumns = append(scanColumns, store.Table.PrimaryKey...)
	req := &Request{
		Table:   store.Table.Name,
		Columns: scanColumns,
	}
	peer.setQueryOptions(req)
	res, _, err := peer.Query(ctx, req)
	if err != nil {
		return false, err
	}

	// make sure all backends are sorted the same way
	res = res.SortByPrimaryKey(store.Table, req)

	columns := make(ColumnList, len(scanColumns))
	for i, name := range scanColumns {
		col := store.Table.ColumnsIndex[name]
		columns[i] = col
	}

	missing, err := ds.getMissingTimestamps(ctx, store, res, columns, updateThr)
	if err != nil {
		return false, err
	}

	if len(missing) == 0 {
		logWith(ds, req).Tracef("%s delta scan did not find any timestamps", store.Table.Name.String())

		return false, nil
	}

	logWith(ds, req).Debugf("%s delta scan going to update %d timestamps", store.Table.Name.String(), len(missing))
	timestampFilter := composeTimestampFilter(missing, "last_check")
	if len(timestampFilter) > missedTimestampMaxFilter {
		msg := fmt.Sprintf("%s delta scan timestamp filter too complex: %d", store.Table.Name.String(), len(timestampFilter))
		logWith(ds, req).Debugf("%s", msg)
		// sync at least a few to get back on track
		missing = missing[0 : missedTimestampMaxFilter-1]
		timestampFilter = composeTimestampFilter(missing, "last_check")
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

	err = updateFn(ctx, strings.Join(filterList, ""), false, 0)
	if err != nil {
		return false, err
	}

	peer.statusSetLocked(statusKey, currentUnixTime())

	return true, nil
}

// getMissingTimestamps returns list of last_check dates which can be used to delta update.
func (ds *DataStoreSet) getMissingTimestamps(ctx context.Context, store *DataStore, res ResultSet, columns ColumnList, updateThreshold int64) (missing []int64, err error) {
	ds.Lock.RLock()
	peer := ds.peer
	data := store.Data
	if len(data) < len(res) {
		ds.Lock.RUnlock()
		if peer.HasFlag(Icinga2) || len(data) == 0 {
			err = ds.reloadIfNumberOfObjectsChanged(ctx)

			return missing, err
		}

		err = &PeerError{msg: fmt.Sprintf("%s cache not ready, got %d entries but only have %d in cache",
			store.Table.Name.String(), len(res), len(data)), kind: ResponseError}
		logWith(peer).Warnf("%s", err.Error())
		peer.setBroken(fmt.Sprintf("got more %s than expected. Hint: check clients 'max_response_size' setting.", store.Table.Name.String()))

		return nil, err
	}

	missedUnique := make(map[int64]bool)
	for i, row := range res {
		ts := interface2int64(row[0])
		if ts >= updateThreshold {
			continue
		}
		if data[i].CheckChangedIntValues(0, row, columns) {
			missedUnique[ts] = true
		}
	}
	ds.Lock.RUnlock()

	// return uniq sorted keys
	missing = make([]int64, len(missedUnique))
	i := 0
	for lastCheck := range missedUnique {
		missing[i] = lastCheck
		i++
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })

	return missing, nil
}

func (ds *DataStoreSet) reloadIfNumberOfObjectsChanged(ctx context.Context) (err error) {
	if ds.hasChanged(ctx) {
		return (ds.peer.InitAllTables(ctx))
	}

	return
}

// UpdateDeltaCommentsOrDowntimes update the comments or downtimes table. It fetches the number and highest id of
// the remote comments/downtimes. If an update is required, it then fetches all ids to check which are new and
// which have to be removed.
// It returns any error encountered.
func (ds *DataStoreSet) UpdateDeltaCommentsOrDowntimes(ctx context.Context, name TableName) (err error) {
	changed, err := ds.maxIDOrSizeChanged(ctx, name)
	if !changed || err != nil {
		return err
	}
	peer := ds.peer

	// fetch all ids to see which ones are missing or to be removed
	req := &Request{
		Table:   name,
		Columns: []string{"id"},
	}
	peer.setQueryOptions(req)
	res, _, err := peer.Query(ctx, req)
	if err != nil {
		return err
	}

	store := ds.Get(name)
	if store == nil {
		return nil
	}
	ds.Lock.Lock()
	idIndex := store.Index
	missingIDs := []int64{}
	resIndex := make(map[string]bool)
	for _, resRow := range res {
		comID := fmt.Sprintf("%d", interface2int64(resRow[0]))
		_, ok := idIndex[comID]
		if !ok {
			logWith(ds, req).Debugf("adding %s with id %s", name.String(), comID)
			id64 := interface2int64(resRow[0])
			missingIDs = append(missingIDs, id64)
		}
		resIndex[comID] = true
	}

	// remove old comments / downtimes
	for id := range idIndex {
		_, ok := resIndex[id]
		if !ok {
			logWith(ds, req).Debugf("removing %s with id %s", name.String(), id)
			tmp := idIndex[id]
			store.RemoveItem(tmp)
		}
	}
	ds.Lock.Unlock()

	if len(missingIDs) > 0 {
		keys, columns := store.GetInitialColumns()
		req2 := &Request{
			Table:     name,
			Columns:   keys,
			FilterStr: "",
		}
		for _, id := range missingIDs {
			req2.FilterStr += fmt.Sprintf("Filter: id = %d\n", id)
		}
		req2.FilterStr += fmt.Sprintf("Or: %d\n", len(missingIDs))
		peer.setQueryOptions(req2)
		res, _, err = peer.Query(ctx, req2)
		if err != nil {
			return err
		}

		nErr := store.AppendData(res, columns)
		if nErr != nil {
			return nErr
		}
	}

	// reset cache
	switch name {
	case TableComments:
		err = ds.RebuildCommentsList()
		if err != nil {
			return err
		}
	case TableDowntimes:
		err = ds.RebuildDowntimesList()
		if err != nil {
			return err
		}
	default:
		log.Panicf("supported table: %s", name.String())
	}

	logWith(peer, req).Debugf("updated %s", name.String())

	return nil
}

// maxIDOrSizeChanged returns true if table data changed in size or max id.
func (ds *DataStoreSet) maxIDOrSizeChanged(ctx context.Context, name TableName) (changed bool, err error) {
	peer := ds.peer
	// get number of entries and max id
	req := &Request{
		Table:     name,
		FilterStr: "Stats: id != -1\nStats: max id\n",
	}
	peer.setQueryOptions(req)
	res, _, err := peer.Query(ctx, req)
	if err != nil {
		return false, err
	}

	store := ds.Get(name)
	var maxID int64
	ds.Lock.RLock()
	entries := len(store.Data)
	if entries > 0 {
		maxID = store.Data[entries-1].GetInt64ByName("id")
	}
	ds.Lock.RUnlock()

	if len(res) == 0 || float64(entries) == interface2float64(res[0][0]) && (entries == 0 || interface2float64(res[0][1]) == float64(maxID)) {
		logWith(peer, req).Tracef("%s did not change", name.String())

		return false, nil
	}

	return true, nil
}

// UpdateFullTable updates a given table by requesting all dynamic columns from the remote peer.
// Assuming we get the objects always in the same order, we can just iterate over the index and update the fields.
// It returns a boolean flag whether the remote site has been restarted and any error encountered.
func (ds *DataStoreSet) UpdateFullTable(ctx context.Context, tableName TableName) (err error) {
	peer := ds.peer
	store := ds.Get(tableName)
	if store == nil {
		return fmt.Errorf("cannot update table %s, peer is down: %s", tableName.String(), peer.getError())
	}
	skip, err := ds.skipTableUpdate(store, tableName)
	if skip || err != nil {
		return err
	}

	columns := store.DynamicColumnNamesCache
	// primary keys are not required, we fetch everything anyway
	primaryKeysLen := len(store.Table.PrimaryKey)

	req := &Request{
		Table:   store.Table.Name,
		Columns: columns,
	}
	peer.setQueryOptions(req)
	res, resMeta, err := peer.Query(ctx, req)
	if err != nil {
		return err
	}

	// make sure all backends are sorted the same way
	t1 := time.Now()
	res = res.SortByPrimaryKey(store.Table, req)
	durationSort := time.Since(t1).Truncate(time.Millisecond)

	ds.Lock.RLock()
	data := store.Data
	ds.Lock.RUnlock()
	if len(res) != len(data) {
		err = fmt.Errorf("site returned different number of objects, assuming backend has been restarted, table: %s, expected: %d, received: %d",
			store.Table.Name.String(), len(data), len(res))
		logWith(peer).Debugf("%s", err.Error())

		return &PeerError{msg: err.Error(), kind: RestartRequiredError}
	}

	promObjectUpdate.WithLabelValues(peer.Name, tableName.String()).Add(float64(len(res)))

	var durationLock, durationInsert time.Duration
	switch tableName {
	case TableTimeperiods:
		// check for changed timeperiods, because we have to update the linked hosts and services as well
		err = ds.updateTimeperiodsData(ctx, primaryKeysLen, store, res, store.DynamicColumnCache)
		lastTimeperiodUpdateMinute, _ := strconv.Atoi(time.Now().Format("4"))
		peer.statusSetLocked(LastTimeperiodUpdateMinute, lastTimeperiodUpdateMinute)
		if err != nil {
			return err
		}
	case TableStatus:
		// check pid and date before updating tables.
		// Otherwise we and up with inconsistent state until the full refresh is finished
		err = peer.CheckBackendRestarted(primaryKeysLen, res, store.DynamicColumnCache)
		if err != nil {
			return err
		}
		// continue with normal update
		fallthrough
	default:
		err = ds.insertDeltaDataResult(primaryKeysLen, res, resMeta, store)
		if err != nil {
			return err
		}
	}

	if tableName == TableStatus {
		LogErrors(peer.checkStatusFlags(ctx, ds))
	}

	logWith(peer, req).Debugf("up full  table: %15s - fetch: %9s - sort: %9s - lock: %9s - insert: %9s - count: %8d - size: %8d kB",
		tableName.String(), resMeta.Duration.Truncate(time.Millisecond), durationSort, durationLock, durationInsert, len(res), resMeta.Size/1024)

	return nil
}

func (ds *DataStoreSet) skipTableUpdate(store *DataStore, table TableName) (bool, error) {
	if store == nil {
		return true, fmt.Errorf("skipped table %s update, store not initialized", table.String())
	}
	if store.Table.Virtual != nil {
		return true, nil
	}
	// no updates for passthrough tables, ex.: log
	if store.Table.PassthroughOnly {
		return true, nil
	}
	if ds.peer.HasFlag(MultiBackend) && store.Table.Name != TableStatus {
		return true, nil
	}
	if len(store.DynamicColumnNamesCache) == 0 {
		return true, nil
	}

	return false, nil
}

func (ds *DataStoreSet) updateTimeperiodsData(ctx context.Context, dataOffset int, store *DataStore, res ResultSet, columns ColumnList) (err error) {
	changedTimeperiods := make(map[string]bool)
	ds.Lock.Lock()
	data := store.Data
	now := currentUnixTime()
	nameCol := store.GetColumn("name")
	ds.Lock.Unlock()
	for i := range res {
		row := res[i]
		if data[i].CheckChangedIntValues(dataOffset, row, columns) {
			changedTimeperiods[data[i].GetString(nameCol)] = true
		}
		err = data[i].UpdateValues(dataOffset, row, columns, now)
		if err != nil {
			return err
		}
	}
	// Update hosts and services with those changed timeperiods
	for name, state := range changedTimeperiods {
		logWith(ds).Debugf("timeperiod %s has changed to %v, need to update affected hosts/services", name, state)
		err = ds.UpdateDeltaHosts(ctx, "Filter: check_period = "+name+"\nFilter: notification_period = "+name+"\nOr: 2\n", false, 0)
		if err != nil {
			return err
		}
		err = ds.UpdateDeltaServices(ctx, "Filter: check_period = "+name+"\nFilter: notification_period = "+name+"\nOr: 2\n", false, 0)
		if err != nil {
			return err
		}
	}

	return nil
}

// RebuildCommentsList updates the comments column of hosts/services based on the comments table ids.
func (ds *DataStoreSet) RebuildCommentsList() (err error) {
	time1 := time.Now()
	err = ds.buildDowntimeCommentsList(TableComments)
	if err != nil {
		return
	}

	duration := time.Since(time1)
	logWith(ds).Debugf("comments rebuild (%s)", duration.Truncate(time.Millisecond))

	return
}

// RebuildDowntimesList updates the downtimes column of hosts/services based on the downtimes table ids.
func (ds *DataStoreSet) RebuildDowntimesList() (err error) {
	time1 := time.Now()
	err = ds.buildDowntimeCommentsList(TableDowntimes)
	if err != nil {
		return
	}

	duration := time.Since(time1)
	logWith(ds).Debugf("downtimes rebuild (%s)", duration.Truncate(time.Millisecond))

	return
}

// buildDowntimeCommentsList updates the downtimes/comments id list for all hosts and services.
func (ds *DataStoreSet) buildDowntimeCommentsList(name TableName) (err error) {
	ds.Lock.Lock()
	defer ds.Lock.Unlock()

	store := ds.tables[name]
	if store == nil {
		return fmt.Errorf("cannot build id list, peer is down: %s", ds.peer.getError())
	}
	hostStore := ds.tables[TableHosts]
	if hostStore == nil {
		return fmt.Errorf("cannot build id list, peer is down: %s", ds.peer.getError())
	}
	hostIdx := hostStore.Table.GetColumn(name.String()).Index

	serviceStore := ds.tables[TableServices]
	if serviceStore == nil {
		return fmt.Errorf("cannot build id list, peer is down: %s", ds.peer.getError())
	}
	serviceIdx := serviceStore.Table.GetColumn(name.String()).Index

	hostResult := make(map[*DataRow][]int64)
	serviceResult := make(map[*DataRow][]int64)
	idIndex := store.Table.GetColumn("id").Index
	hostNameIndex := store.Table.GetColumn("host_name").Index
	serviceDescIndex := store.Table.GetColumn("service_description").Index
	hostIndex := hostStore.Index
	serviceIndex := serviceStore.Index2
	for i := range store.Data {
		row := store.Data[i]
		key := row.dataString[hostNameIndex]
		serviceName := row.dataString[serviceDescIndex]
		comID := row.dataInt64[idIndex]
		if serviceName != "" {
			if obj, ok := serviceIndex[key][serviceName]; ok {
				serviceResult[obj] = append(serviceResult[obj], comID)
			}
		} else {
			if obj, ok := hostIndex[key]; ok {
				hostResult[obj] = append(hostResult[obj], comID)
			}
		}
	}
	promObjectCount.WithLabelValues(ds.peer.Name, name.String()).Set(float64(len(store.Data)))

	// empty current lists
	for _, d := range hostStore.Data {
		d.dataInt64List[hostIdx] = emptyInt64List
	}
	for _, d := range serviceStore.Data {
		d.dataInt64List[serviceIdx] = emptyInt64List
	}

	// store updated lists
	for d, ids := range serviceResult {
		d.dataInt64List[serviceIdx] = ids
	}
	for d, ids := range hostResult {
		d.dataInt64List[hostIdx] = ids
	}

	return nil
}

func composeTimestampFilter(timestamps []int64, attribute string) []string {
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
