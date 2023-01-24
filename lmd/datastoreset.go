package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sasha-s/go-deadlock"
)

// DataStoreSet is the handle to a peers datastores
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
func (ds *DataStoreSet) CreateObjectByType(table *Table) (store *DataStore, err error) {
	// log table does not create objects
	if table.PassthroughOnly || table.Virtual != nil {
		return
	}
	p := ds.peer

	store = NewDataStore(table, p)
	store.DataSet = ds
	keys, columns := store.GetInitialColumns()

	// fetch remote objects
	req := &Request{
		Table:   store.Table.Name,
		Columns: keys,
	}
	p.setQueryOptions(req)
	res, resMeta, err := p.Query(req)

	if err != nil {
		return
	}

	t1 := time.Now()

	// verify result set
	keyLen := len(keys)
	for i, row := range res {
		if len(row) != keyLen {
			err = fmt.Errorf("%s result set verification failed: len mismatch in row %d, expected %d columns and got %d", store.Table.Name.String(), i, keyLen, len(row))
			if p.ErrorCount > 0 {
				// silently cancel, backend broke during initialization, should have been logged already
				log.Debugf("error during %s initialization, but backend is already failed: %s", &table.Name, err.Error())
			} else {
				log.Errorf("error during %s initialization: %s", &table.Name, err.Error())
			}
			return
		}
	}

	// make sure all backends are sorted the same way
	res = res.SortByPrimaryKey(table, req)

	now := currentUnixTime()
	err = store.InsertData(res, columns, false)
	if err != nil {
		return
	}

	p.Lock.Lock()
	p.Status[LastUpdate] = now
	p.Status[LastFullUpdate] = now
	p.Lock.Unlock()
	tableName := table.Name.String()
	promObjectCount.WithLabelValues(p.Name, tableName).Set(float64(len(res)))

	duration := time.Since(t1).Truncate(time.Millisecond)
	logWith(p, req).Debugf("updated table: %15s - fetch: %9s - insert: %9s - count: %8d - size: %8d kB", tableName, resMeta.Duration.Truncate(time.Microsecond), duration, len(res), resMeta.Size/1024)
	return
}

// SetReferences creates reference entries for all tables
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

func (ds *DataStoreSet) hasChanged() (changed bool) {
	changed = false
	tablenames := []TableName{TableCommands, TableContactgroups, TableContacts, TableHostgroups, TableHosts, TableServicegroups, TableTimeperiods}
	for _, name := range tablenames {
		counter := ds.peer.countFromServer(name.String(), "name !=")
		ds.Lock.RLock()
		changed = changed || (counter != len(ds.tables[name].Data))
		ds.Lock.RUnlock()
	}
	counter := ds.peer.countFromServer("services", "host_name !=")
	ds.Lock.RLock()
	changed = changed || (counter != len(ds.tables[TableServices].Data))
	ds.Lock.RUnlock()
	ds.peer.clearLastRequest()

	return
}

// UpdateFull runs a full update on all dynamic values for all tables which have dynamic updated columns.
// It returns any error occurred or nil if the update was successful.
func (ds *DataStoreSet) UpdateFull(tables []TableName) (err error) {
	t1 := time.Now()
	err = ds.UpdateFullTablesList(tables)
	if err != nil {
		return
	}
	p := ds.peer
	duration := time.Since(t1)
	peerStatus := p.StatusGet(PeerState).(PeerStatus)
	if peerStatus != PeerStatusUp && peerStatus != PeerStatusPending {
		logWith(p).Infof("site soft recovered from short outage (reason: %s - %s)", peerStatus.String(), p.StatusGet(LastError).(string))
	}
	p.Lock.Lock()
	p.resetErrors()
	p.Status[ResponseTime] = duration.Seconds()
	p.Status[LastUpdate] = currentUnixTime()
	p.Status[LastFullUpdate] = currentUnixTime()
	p.Lock.Unlock()
	logWith(p).Debugf("full update complete in: %s", duration.String())
	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return
}

// UpdateFullTablesList updates list of tables and returns any error
func (ds *DataStoreSet) UpdateFullTablesList(tables []TableName) (err error) {
	for i := range tables {
		name := tables[i]
		err = ds.UpdateFullTable(name)
		if err != nil {
			logWith(ds).Debugf("update failed: %s", err.Error())
			return
		}
	}
	return
}

// UpdateDelta runs a delta update on all status, hosts, services, comments and downtimes table.
// It returns true if the update was successful or false otherwise.
func (ds *DataStoreSet) UpdateDelta(from, to float64) (err error) {
	t1 := time.Now()

	err = ds.UpdateFullTablesList(Objects.StatusTables)
	if err != nil {
		return
	}

	updateOffset := float64(ds.peer.lmd.Config.UpdateOffset)

	filterStr := ""
	if from > 0 {
		switch {
		case ds.peer.HasFlag(HasLMDLastCacheUpdateColumn):
			filterStr = fmt.Sprintf("Filter: lmd_last_cache_update >= %v\nFilter: lmd_last_cache_update < %v\nAnd: 2\n", int64(from-updateOffset), int64(to-updateOffset))
		case ds.peer.HasFlag(HasLastUpdateColumn):
			filterStr = fmt.Sprintf("Filter: last_update >= %v\nFilter: last_update < %v\nAnd: 2\n", int64(from-updateOffset), int64(to-updateOffset))
		default:
			filterStr = fmt.Sprintf("Filter: last_check >= %v\nFilter: last_check < %v\nAnd: 2\n", int64(from-updateOffset), int64(to-updateOffset))
			if ds.peer.lmd.Config.SyncIsExecuting && !ds.peer.HasFlag(Shinken) {
				filterStr += "Filter: is_executing = 1\nOr: 2\n"
			}
		}
	}
	err = ds.UpdateDeltaHosts(filterStr, true)
	if err != nil {
		return err
	}
	err = ds.UpdateDeltaServices(filterStr, true)
	if err != nil {
		return err
	}
	err = ds.UpdateDeltaCommentsOrDowntimes(TableComments)
	if err != nil {
		return err
	}
	err = ds.UpdateDeltaCommentsOrDowntimes(TableDowntimes)
	if err != nil {
		return err
	}

	p := ds.peer
	duration := time.Since(t1)
	logWith(p).Debugf("delta update complete in: %s", duration.String())

	p.Lock.Lock()
	peerStatus := p.Status[PeerState].(PeerStatus)
	p.resetErrors()
	p.Status[LastUpdate] = to
	p.Status[ResponseTime] = duration.Seconds()
	p.Lock.Unlock()

	if peerStatus != PeerStatusUp && peerStatus != PeerStatusPending {
		logWith(p).Infof("site soft recovered from short outage")
	}

	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return
}

// UpdateDeltaHosts update hosts by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional updateOffset seconds.
// It returns any error encountered.
func (ds *DataStoreSet) UpdateDeltaHosts(filterStr string, tryFullScan bool) (err error) {
	return ds.updateDeltaHostsServices(TableHosts, filterStr, tryFullScan)
}

// UpdateDeltaServices update services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional updateOffset seconds.
// It returns any error encountered.
func (ds *DataStoreSet) UpdateDeltaServices(filterStr string, tryFullScan bool) (err error) {
	return ds.updateDeltaHostsServices(TableServices, filterStr, tryFullScan)
}

// updateDeltaHostsServices update hosts / services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional updateOffset seconds.
// It returns any error encountered.
func (ds *DataStoreSet) updateDeltaHostsServices(tableName TableName, filterStr string, tryFullScan bool) (err error) {
	// update changed services
	table := ds.Get(tableName)
	if table == nil {
		err = fmt.Errorf("peer not ready, either not initialized or went offline recently")
		return
	}
	p := ds.peer

	if tryFullScan {
		// run regular delta update and lets check if all last_check dates match
		updated, uErr := ds.UpdateDeltaFullScanHostsServices(table, filterStr)
		if updated || uErr != nil {
			return uErr
		}
	}
	req := &Request{
		Table:     tableName,
		Columns:   table.DynamicColumnNamesCache,
		FilterStr: filterStr,
	}
	p.setQueryOptions(req)
	res, meta, err := p.Query(req)
	if err != nil {
		return
	}
	switch tableName {
	case TableHosts:
		hostDataOffset := 1
		return ds.insertDeltaDataResult(hostDataOffset, res, meta, table)
	case TableServices:
		serviceDataOffset := 2
		return ds.insertDeltaDataResult(serviceDataOffset, res, meta, table)
	default:
		logWith(p, req).Panicf("not implemented for: %s", tableName.String())
	}
	return
}

func (ds *DataStoreSet) insertDeltaDataResult(dataOffset int, res ResultSet, resMeta *ResultMetaData, table *DataStore) (err error) {
	t1 := time.Now()
	updateSet, err := ds.prepareDataUpdateSet(dataOffset, res, table)
	if err != nil {
		return
	}
	now := currentUnixTime()

	ds.Lock.Lock()
	defer ds.Lock.Unlock()
	for i := range updateSet {
		update := updateSet[i]
		if update.FullUpdate {
			err = update.DataRow.UpdateValues(dataOffset, update.ResultRow, table.DynamicColumnCache, now)
		} else {
			err = update.DataRow.UpdateValuesNumberOnly(dataOffset, update.ResultRow, table.DynamicColumnCache, now)
		}
		if err != nil {
			return
		}
	}

	duration := time.Since(t1).Truncate(time.Millisecond)

	p := ds.peer
	tableName := table.Table.Name.String()
	promObjectUpdate.WithLabelValues(p.Name, tableName).Add(float64(len(res)))
	logWith(p, resMeta.Request).Debugf("updated table: %15s - fetch: %9s - insert: %9s - count: %8d - size: %8d kB", tableName, resMeta.Duration, duration, len(updateSet), resMeta.Size/1024)

	return
}

func (ds *DataStoreSet) prepareDataUpdateSet(dataOffset int, res ResultSet, table *DataStore) (updateSet []ResultPrepared, err error) {
	updateSet = make([]ResultPrepared, 0, len(res))

	// prepare list of large strings
	stringlistIndexes := make([]int, 0)
	for i := range table.DynamicColumnCache {
		col := table.DynamicColumnCache[i]
		if col.DataType == StringLargeCol {
			stringlistIndexes = append(stringlistIndexes, i+dataOffset)
		}
	}

	// compare last check date and only update large strings if the last check date has changed
	lastCheckCol := table.GetColumn("last_check")
	lastCheckDataIndex := lastCheckCol.Index
	lastCheckResIndex := table.DynamicColumnCache.GetColumnIndex("last_check") + dataOffset

	// prepare update
	ds.Lock.RLock()
	defer ds.Lock.RUnlock()
	nameIndex := table.Index
	nameIndex2 := table.Index2
	for i, resRow := range res {
		prepared := ResultPrepared{
			ResultRow:  resRow,
			FullUpdate: false,
		}
		if dataOffset == 0 {
			prepared.DataRow = table.Data[i]
		} else {
			switch table.Table.Name {
			case TableHosts:
				hostName := interface2stringNoDedup(resRow[0])
				dataRow := nameIndex[hostName]
				if dataRow == nil {
					return updateSet, fmt.Errorf("cannot update host, no host named '%s' found", hostName)
				}
				prepared.DataRow = dataRow
			case TableServices:
				hostName := interface2stringNoDedup(resRow[0])
				serviceName := interface2stringNoDedup(resRow[1])
				dataRow := nameIndex2[hostName][serviceName]
				if dataRow == nil {
					return updateSet, fmt.Errorf("cannot update service, no service named '%s' - '%s' found", interface2stringNoDedup(resRow[0]), interface2stringNoDedup(resRow[1]))
				}

				prepared.DataRow = dataRow
			}
		}

		// compare last check date and prepare large strings if the last check date has changed
		if interface2int64(resRow[lastCheckResIndex]) != prepared.DataRow.dataInt64[lastCheckDataIndex] {
			prepared.FullUpdate = true
			for _, j := range stringlistIndexes {
				res[i][j] = interface2stringlarge(res[i][j])
			}
		}
		updateSet = append(updateSet, prepared)
	}
	return updateSet, nil
}

// UpdateDeltaFullScanHostsServices is a table independent wrapper for UpdateDeltaFullScan
// It returns true if an update was done and any error encountered.
func (ds *DataStoreSet) UpdateDeltaFullScanHostsServices(store *DataStore, filterStr string) (updated bool, err error) {
	switch store.Table.Name {
	case TableServices:
		updated, err = ds.UpdateDeltaFullScan(store, LastFullServiceUpdate, filterStr, ds.UpdateDeltaServices)
	case TableHosts:
		updated, err = ds.UpdateDeltaFullScan(store, LastFullHostUpdate, filterStr, ds.UpdateDeltaHosts)
	default:
		p := ds.peer
		logWith(p).Panicf("not implemented for: " + store.Table.Name.String())
	}

	return
}

// UpdateDeltaFullScan updates hosts and services tables by fetching some key indicator fields like last_check
// downtimes or acknowledged status. If an update is required, the last_check timestamp is used as filter for a
// delta update.
// The full scan just returns false without any update if the last update was less then MinFullScanInterval seconds ago.
// It returns true if an update was done and any error encountered.
func (ds *DataStoreSet) UpdateDeltaFullScan(store *DataStore, statusKey PeerStatusKey, filterStr string, updateFn func(string, bool) error) (updated bool, err error) {
	p := ds.peer
	lastUpdate := p.StatusGet(statusKey).(float64)

	// do not do a full scan more often than every 60 seconds
	if lastUpdate > float64(time.Now().Unix()-MinFullScanInterval) {
		return
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
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}

	// make sure all backends are sorted the same way
	res = res.SortByPrimaryKey(store.Table, req)

	columns := make(ColumnList, len(scanColumns))
	for i, name := range scanColumns {
		col := store.Table.ColumnsIndex[name]
		columns[i] = col
	}

	missing, err := ds.getMissingTimestamps(store, res, columns)
	if err != nil {
		return
	}

	if len(missing) == 0 {
		logWith(ds, req).Debugf("%s delta scan did not find any timestamps", store.Table.Name.String())
		return
	}

	logWith(ds, req).Debugf("%s delta scan going to update %d timestamps", store.Table.Name.String(), len(missing))
	timestampFilter := composeTimestampFilter(missing, "last_check")
	if len(timestampFilter) > 100 {
		msg := fmt.Sprintf("%s delta scan timestamp filter too complex: %d", store.Table.Name.String(), len(timestampFilter))
		if p.HasFlag(HasLastUpdateColumn) {
			logWith(ds, req).Warnf("%s", msg)
		} else {
			logWith(ds, req).Debugf("%s", msg)
		}
		// sync at least a few to get back on track
		timestampFilter = timestampFilter[0:99]
	}

	filter := []string{}
	if filterStr != "" {
		filter = append(filter, filterStr)
		if len(timestampFilter) > 0 {
			filter = append(filter, timestampFilter...)
			filter = append(filter, "Or: 2\n")
		}
	} else {
		filter = append(filter, timestampFilter...)
	}

	err = updateFn(strings.Join(filter, ""), false)
	if err != nil {
		return
	}

	updated = true
	p.StatusSet(statusKey, currentUnixTime())

	return
}

// getMissingTimestamps returns list of last_check dates which can be used to delta update
func (ds *DataStoreSet) getMissingTimestamps(store *DataStore, res ResultSet, columns ColumnList) (missing []int64, err error) {
	ds.Lock.RLock()
	p := ds.peer
	data := store.Data
	if len(data) < len(res) {
		ds.Lock.RUnlock()
		if p.HasFlag(Icinga2) || len(data) == 0 {
			err = ds.reloadIfNumberOfObjectsChanged()
			return
		}
		err = &PeerError{msg: fmt.Sprintf("%s cache not ready, got %d entries but only have %d in cache", store.Table.Name.String(), len(res), len(data)), kind: ResponseError}
		logWith(p).Warnf("%s", err.Error())
		p.setBroken(fmt.Sprintf("got more %s than expected. Hint: check clients 'max_response_size' setting.", store.Table.Name.String()))
		return
	}

	missedUnique := make(map[int64]bool)
	updateThreshold := time.Now().Unix() - ds.peer.lmd.Config.UpdateOffset
	for i, row := range res {
		if data[i].CheckChangedIntValues(0, row, columns) {
			ts := interface2int64(row[0])
			if ts < updateThreshold {
				missedUnique[ts] = true
			}
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

	return
}

func (ds *DataStoreSet) reloadIfNumberOfObjectsChanged() (err error) {
	if ds.hasChanged() {
		return (ds.peer.InitAllTables())
	}
	return
}

// UpdateDeltaCommentsOrDowntimes update the comments or downtimes table. It fetches the number and highest id of
// the remote comments/downtimes. If an update is required, it then fetches all ids to check which are new and
// which have to be removed.
// It returns any error encountered.
func (ds *DataStoreSet) UpdateDeltaCommentsOrDowntimes(name TableName) (err error) {
	changed, err := ds.maxIDOrSizeChanged(name)
	if !changed || err != nil {
		return
	}
	p := ds.peer

	// fetch all ids to see which ones are missing or to be removed
	req := &Request{
		Table:   name,
		Columns: []string{"id"},
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}

	store := ds.Get(name)
	if store == nil {
		return
	}
	ds.Lock.Lock()
	idIndex := store.Index
	missingIds := []int64{}
	resIndex := make(map[string]bool)
	for _, resRow := range res {
		id := fmt.Sprintf("%d", interface2int64(resRow[0]))
		_, ok := idIndex[id]
		if !ok {
			logWith(ds, req).Debugf("adding %s with id %s", name.String(), id)
			id64 := int64(resRow[0].(float64))
			missingIds = append(missingIds, id64)
		}
		resIndex[id] = true
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

	if len(missingIds) > 0 {
		keys, columns := store.GetInitialColumns()
		req := &Request{
			Table:     name,
			Columns:   keys,
			FilterStr: "",
		}
		for _, id := range missingIds {
			req.FilterStr += fmt.Sprintf("Filter: id = %d\n", id)
		}
		req.FilterStr += fmt.Sprintf("Or: %d\n", len(missingIds))
		p.setQueryOptions(req)
		res, _, err = p.Query(req)
		if err != nil {
			return
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
			return
		}
	case TableDowntimes:
		err = ds.RebuildDowntimesList()
		if err != nil {
			return
		}
	}

	logWith(p, req).Debugf("updated %s", name.String())
	return
}

// maxIDOrSizeChanged returns true if table data changed in size or max id
func (ds *DataStoreSet) maxIDOrSizeChanged(name TableName) (changed bool, err error) {
	p := ds.peer
	// get number of entries and max id
	req := &Request{
		Table:     name,
		FilterStr: "Stats: id != -1\nStats: max id\n",
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
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
		logWith(p, req).Tracef("%s did not change", name.String())
		return
	}
	changed = true
	return
}

// UpdateFullTable updates a given table by requesting all dynamic columns from the remote peer.
// Assuming we get the objects always in the same order, we can just iterate over the index and update the fields.
// It returns a boolean flag whether the remote site has been restarted and any error encountered.
func (ds *DataStoreSet) UpdateFullTable(tableName TableName) (err error) {
	p := ds.peer
	store := ds.Get(tableName)
	if store == nil {
		return fmt.Errorf("cannot update table %s, peer is down: %s", tableName.String(), p.getError())
	}
	skip, err := ds.skipTableUpdate(store, tableName)
	if skip || err != nil {
		return
	}

	columns := store.DynamicColumnNamesCache
	// primary keys are not required, we fetch everything anyway
	primaryKeysLen := len(store.Table.PrimaryKey)

	req := &Request{
		Table:   store.Table.Name,
		Columns: columns,
	}
	p.setQueryOptions(req)
	res, resMeta, err := p.Query(req)
	if err != nil {
		return
	}

	// make sure all backends are sorted the same way
	res = res.SortByPrimaryKey(store.Table, req)

	ds.Lock.RLock()
	data := store.Data
	ds.Lock.RUnlock()
	if len(res) != len(data) {
		err = fmt.Errorf("site returned different number of objects, assuming backend has been restarted, table: %s, expected: %d, received: %d", store.Table.Name.String(), len(data), len(res))
		logWith(p).Debugf("%s", err.Error())
		return &PeerError{msg: err.Error(), kind: RestartRequiredError}
	}

	promObjectUpdate.WithLabelValues(p.Name, tableName.String()).Add(float64(len(res)))
	t1 := time.Now()

	switch tableName {
	case TableTimeperiods:
		// check for changed timeperiods, because we have to update the linked hosts and services as well
		err = ds.updateTimeperiodsData(primaryKeysLen, store, res, store.DynamicColumnCache)
		lastTimeperiodUpdateMinute, _ := strconv.Atoi(time.Now().Format("4"))
		p.StatusSet(LastTimeperiodUpdateMinute, lastTimeperiodUpdateMinute)
	case TableHosts, TableServices:
		err = ds.insertDeltaDataResult(primaryKeysLen, res, resMeta, store)
	case TableStatus:
		// check pid and date before updating tables.
		// Otherwise we and up with inconsistent state until the full refresh is finished
		err = p.CheckBackendRestarted(primaryKeysLen, res, store.DynamicColumnCache)
		if err != nil {
			return
		}
		// continue with normal update
		fallthrough
	default:
		ds.Lock.Lock()
		now := currentUnixTime()
		for i, row := range res {
			err = data[i].UpdateValues(primaryKeysLen, row, store.DynamicColumnCache, now)
			if err != nil {
				ds.Lock.Unlock()
				return
			}
		}
		ds.Lock.Unlock()
	}

	if tableName == TableStatus {
		LogErrors(p.checkStatusFlags(ds))
	}

	duration := time.Since(t1).Truncate(time.Millisecond)
	logWith(p, req).Debugf("updated table: %15s - fetch: %9s - insert: %9s - count: %8d - size: %8d kB", tableName.String(), resMeta.Duration.Truncate(time.Microsecond), duration, len(res), resMeta.Size/1024)
	return
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

func (ds *DataStoreSet) updateTimeperiodsData(dataOffset int, store *DataStore, res ResultSet, columns ColumnList) (err error) {
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
			return
		}
	}
	// Update hosts and services with those changed timeperiods
	for name, state := range changedTimeperiods {
		logWith(ds).Debugf("timeperiod %s has changed to %v, need to update affected hosts/services", name, state)
		err = ds.UpdateDeltaHosts("Filter: check_period = "+name+"\nFilter: notification_period = "+name+"\nOr: 2\n", false)
		if err != nil {
			return
		}
		err = ds.UpdateDeltaServices("Filter: check_period = "+name+"\nFilter: notification_period = "+name+"\nOr: 2\n", false)
		if err != nil {
			return
		}
	}
	return
}

// RebuildCommentsList updates the comments column of hosts/services based on the comments table ids
func (ds *DataStoreSet) RebuildCommentsList() (err error) {
	t1 := time.Now()
	err = ds.buildDowntimeCommentsList(TableComments)
	if err != nil {
		return
	}
	duration := time.Since(t1)
	logWith(ds).Debugf("comments rebuild (%s)", duration.Truncate(time.Microsecond))
	return
}

// RebuildDowntimesList updates the downtimes column of hosts/services based on the downtimes table ids
func (ds *DataStoreSet) RebuildDowntimesList() (err error) {
	t1 := time.Now()
	err = ds.buildDowntimeCommentsList(TableDowntimes)
	if err != nil {
		return
	}
	duration := time.Since(t1)
	logWith(ds).Debugf("downtimes rebuild (%s)", duration.Truncate(time.Microsecond))
	return
}

// buildDowntimeCommentsList updates the downtimes/comments id list for all hosts and services
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
		id := row.dataInt64[idIndex]
		if serviceName != "" {
			if obj, ok := serviceIndex[key][serviceName]; ok {
				serviceResult[obj] = append(serviceResult[obj], id)
			}
		} else {
			if obj, ok := hostIndex[key]; ok {
				hostResult[obj] = append(hostResult[obj], id)
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

	return
}

func composeTimestampFilter(timestamps []int64, attribute string) []string {
	filter := []string{}
	block := struct {
		start int64
		end   int64
	}{-1, -1}
	for _, ts := range timestamps {
		if block.start == -1 {
			block.start = ts
			block.end = ts
			continue
		}
		if block.end == ts-1 {
			block.end = ts
			continue
		}

		if block.start != -1 {
			if block.start == block.end {
				filter = append(filter, fmt.Sprintf("Filter: %s = %d\n", attribute, block.start))
			} else {
				filter = append(filter, fmt.Sprintf("Filter: %s >= %d\nFilter: %s <= %d\nAnd: 2\n", attribute, block.start, attribute, block.end))
			}
		}

		block.start = ts
		block.end = ts
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
