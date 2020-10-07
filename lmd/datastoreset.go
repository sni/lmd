package main

import (
	"fmt"
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
	cache  struct {
		comments  map[*DataRow][]int64 // caches hosts/services datarows to list of comments
		downtimes map[*DataRow][]int64 // caches hosts/services datarows to list of downtimes
	}
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

	// icinga2 returns hosts and services in random order but we assume ordered results later
	if p.HasFlag(Icinga2) {
		res = res.SortByPrimaryKey(table, req)
	}

	now := time.Now().Unix()
	err = store.InsertData(res, columns)
	if err != nil {
		return
	}

	p.Lock.Lock()
	p.Status[LastUpdate] = now
	p.Status[LastFullUpdate] = now
	p.Lock.Unlock()
	tableName := table.Name.String()
	promObjectCount.WithLabelValues(p.Name, tableName).Set(float64(len((*res))))

	duration := time.Since(t1).Truncate(time.Millisecond)
	log.Debugf("[%s] updated table: %15s - fetch: %8s - insert: %8s - count: %8d - size: %8d kB", p.Name, tableName, resMeta.Duration, duration, len(*res), resMeta.Size/1024)
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
		log.Infof("[%s] site soft recovered from short outage", p.Name)
	}
	p.Lock.Lock()
	p.resetErrors()
	p.Status[ResponseTime] = duration.Seconds()
	p.Status[LastUpdate] = time.Now().Unix()
	p.Status[LastFullUpdate] = time.Now().Unix()
	p.Lock.Unlock()
	log.Debugf("[%s] full update complete in: %s", p.Name, duration.String())
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
			log.Debugf("[%s] update failed: %s", ds.peer.Name, err.Error())
			return
		}
	}
	return
}

// UpdateDelta runs a delta update on all status, hosts, services, comments and downtimes table.
// It returns true if the update was successful or false otherwise.
func (ds *DataStoreSet) UpdateDelta(from, to int64) (err error) {
	t1 := time.Now()

	err = ds.UpdateFullTablesList(Objects.StatusTables)
	if err != nil {
		return
	}

	filterStr := ""
	if from > 0 {
		if ds.peer.HasFlag(HasLMDLastCacheUpdateColumn) {
			filterStr = fmt.Sprintf("Filter: lmd_last_cache_update >= %v\nFilter: lmd_last_cache_update < %v\nAnd: 2\n", from, to)
		} else if ds.peer.HasFlag(HasLastUpdateColumn) {
			filterStr = fmt.Sprintf("Filter: last_update >= %v\nFilter: last_update < %v\nAnd: 2\n", from, to)
		}
	}
	err = ds.UpdateDeltaHosts(filterStr)
	if err != nil {
		return err
	}
	err = ds.UpdateDeltaServices(filterStr)
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
	log.Debugf("[%s] delta update complete in: %s", p.Name, duration.String())

	p.Lock.Lock()
	peerStatus := p.Status[PeerState].(PeerStatus)
	p.resetErrors()
	p.Status[LastUpdate] = to
	p.Status[ResponseTime] = duration.Seconds()
	p.Lock.Unlock()

	if peerStatus != PeerStatusUp && peerStatus != PeerStatusPending {
		log.Infof("[%s] site soft recovered from short outage", p.Name)
	}

	promPeerUpdates.WithLabelValues(p.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(p.Name).Set(duration.Seconds())
	return
}

// UpdateDeltaHosts update hosts by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional UpdateAdditionalDelta seconds.
// It returns any error encountered.
func (ds *DataStoreSet) UpdateDeltaHosts(filterStr string) (err error) {
	// update changed hosts
	store := ds.Get(TableHosts)
	if store == nil {
		err = fmt.Errorf("peer not ready, either not initialized or went offline recently")
		return
	}
	p := ds.peer
	if filterStr == "" {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\n", (p.StatusGet(LastUpdate).(int64) - UpdateAdditionalDelta))
		if p.GlobalConfig.SyncIsExecuting {
			filterStr += "\nFilter: is_executing = 1\nOr: 2\n"
		}
		// no filter means regular delta update, so lets check if all last_check dates match
		ok, uErr := ds.UpdateDeltaFullScan(store, filterStr)
		if ok || uErr != nil {
			return uErr
		}
	}
	req := &Request{
		Table:     store.Table.Name,
		Columns:   store.DynamicColumnNamesCache,
		FilterStr: filterStr,
	}
	p.setQueryOptions(req)
	res, meta, err := p.Query(req)
	if err != nil {
		return
	}
	hostDataOffset := 1
	return ds.insertDeltaDataResult(hostDataOffset, res, meta, store)
}

// UpdateDeltaServices update services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional UpdateAdditionalDelta seconds.
// It returns any error encountered.
func (ds *DataStoreSet) UpdateDeltaServices(filterStr string) (err error) {
	// update changed services
	table := ds.Get(TableServices)
	if table == nil {
		err = fmt.Errorf("peer not ready, either not initialized or went offline recently")
		return
	}
	p := ds.peer
	if filterStr == "" {
		filterStr = fmt.Sprintf("Filter: last_check >= %v\n", (p.StatusGet(LastUpdate).(int64) - UpdateAdditionalDelta))
		if p.GlobalConfig.SyncIsExecuting {
			filterStr += "\nFilter: is_executing = 1\nOr: 2\n"
		}
		// no filter means regular delta update, so lets check if all last_check dates match
		ok, uErr := ds.UpdateDeltaFullScan(table, filterStr)
		if ok || uErr != nil {
			return uErr
		}
	}
	req := &Request{
		Table:     table.Table.Name,
		Columns:   table.DynamicColumnNamesCache,
		FilterStr: filterStr,
	}
	p.setQueryOptions(req)
	res, meta, err := p.Query(req)
	if err != nil {
		return
	}
	serviceDataOffset := 2
	return ds.insertDeltaDataResult(serviceDataOffset, res, meta, table)
}

func (ds *DataStoreSet) insertDeltaDataResult(dataOffset int, res *ResultSet, resMeta *ResultMetaData, table *DataStore) (err error) {
	t1 := time.Now()
	updateSet, err := ds.prepareDataUpdateSet(dataOffset, res, table)
	if err != nil {
		return
	}
	now := time.Now().Unix()

	ds.Lock.Lock()
	defer ds.Lock.Unlock()
	for i := range updateSet {
		update := updateSet[i]
		if update.FullUpdate {
			err = update.DataRow.UpdateValues(dataOffset, update.ResultRow, &table.DynamicColumnCache, now)
		} else {
			err = update.DataRow.UpdateValuesNumberOnly(dataOffset, update.ResultRow, &table.DynamicColumnCache, now)
		}
		if err != nil {
			return
		}
	}

	duration := time.Since(t1).Truncate(time.Millisecond)

	p := ds.peer
	tableName := table.Table.Name.String()
	promObjectUpdate.WithLabelValues(p.Name, tableName).Add(float64(len(*res)))
	log.Debugf("[%s] updated table: %15s - fetch: %8s - insert: %8s - count: %8d - size: %8d kB", p.Name, tableName, resMeta.Duration, duration, len(updateSet), resMeta.Size/1024)

	return
}

func (ds *DataStoreSet) prepareDataUpdateSet(dataOffset int, res *ResultSet, table *DataStore) (updateSet []ResultPrepared, err error) {
	updateSet = make([]ResultPrepared, len(*res))

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
	lastCheckIndex := table.DynamicColumnCache.GetColumnIndex("last_check") + dataOffset

	// prepare update
	ds.Lock.RLock()
	defer ds.Lock.RUnlock()
	nameIndex := table.Index
	nameIndex2 := table.Index2
	for i := range *res {
		resRow := &(*res)[i]
		prepared := ResultPrepared{
			ResultRow:  resRow,
			FullUpdate: false,
		}
		if dataOffset == 0 {
			prepared.DataRow = table.Data[i]
		} else {
			switch table.Table.Name {
			case TableHosts:
				hostName := interface2stringNoDedup((*resRow)[0])
				dataRow := nameIndex[*hostName]
				if dataRow == nil {
					return updateSet, fmt.Errorf("cannot update host, no host named '%s' found", *hostName)
				}
				prepared.DataRow = dataRow
			case TableServices:
				dataRow := nameIndex2[*(interface2stringNoDedup((*resRow)[0]))][*(interface2stringNoDedup((*resRow)[1]))]
				if dataRow == nil {
					return updateSet, fmt.Errorf("cannot update service, no service named '%s' - '%s' found", *(interface2stringNoDedup((*resRow)[0])), *(interface2stringNoDedup((*resRow)[1])))
				}

				prepared.DataRow = dataRow
			}
		}

		// compare last check date and prepare large strings if the last check date has changed
		if interface2int64((*resRow)[lastCheckIndex]) != prepared.DataRow.GetInt64(lastCheckCol) {
			prepared.FullUpdate = true
			for j := range stringlistIndexes {
				(*res)[i][j] = interface2stringlarge((*res)[i][j])
			}
		}
		updateSet[i] = prepared
	}
	return updateSet, nil
}

// UpdateDeltaFullScan updates hosts and services tables by fetching some key indicator fields like last_check
// downtimes or acknowledged status. If an update is required, the last_check timestamp is used as filter for a
// delta update.
// The full scan just returns false without any update if the last update was less then MinFullScanInterval seconds
// ago.
// It returns true if an update was done and any error encountered.
func (ds *DataStoreSet) UpdateDeltaFullScan(store *DataStore, filterStr string) (updated bool, err error) {
	updated = false
	p := ds.peer
	var lastUpdate int64
	switch store.Table.Name {
	case TableServices:
		lastUpdate = p.StatusGet(LastFullServiceUpdate).(int64)
	case TableHosts:
		lastUpdate = p.StatusGet(LastFullHostUpdate).(int64)
	default:
		log.Panicf("not implemented for: " + store.Table.Name.String())
	}

	// do not do a full scan more often than every 30 seconds
	if lastUpdate > time.Now().Unix()-MinFullScanInterval {
		return
	}

	scanColumns := []string{"last_check",
		"scheduled_downtime_depth",
		"acknowledged",
		"active_checks_enabled",
		"notifications_enabled",
	}
	req := &Request{
		Table:   store.Table.Name,
		Columns: scanColumns,
	}
	p.setQueryOptions(req)
	res, _, err := p.Query(req)
	if err != nil {
		return
	}

	columns := make(ColumnList, len(scanColumns))
	for i, name := range scanColumns {
		columns[i] = store.Table.ColumnsIndex[name]
	}

	missing, err := ds.getMissingTimestamps(store, res, &columns)
	if err != nil {
		return
	}

	if len(missing) > 0 {
		filter := []string{filterStr}
		for lastCheck := range missing {
			filter = append(filter, fmt.Sprintf("Filter: last_check = %d\n", lastCheck))
		}
		filter = append(filter, fmt.Sprintf("Or: %d\n", len(filter)))
		if store.Table.Name == TableServices {
			err = ds.UpdateDeltaServices(strings.Join(filter, ""))
		} else if store.Table.Name == TableHosts {
			err = ds.UpdateDeltaHosts(strings.Join(filter, ""))
		}
	}

	if store.Table.Name == TableServices {
		p.StatusSet(LastFullServiceUpdate, time.Now().Unix())
	} else if store.Table.Name == TableHosts {
		p.StatusSet(LastFullHostUpdate, time.Now().Unix())
	}
	updated = true
	return
}

// getMissingTimestamps returns list of last_check dates which can be used to delta update
func (ds *DataStoreSet) getMissingTimestamps(store *DataStore, res *ResultSet, columns *ColumnList) (missing map[int64]bool, err error) {
	missing = make(map[int64]bool)
	ds.Lock.RLock()
	p := ds.peer
	data := store.Data
	if len(data) < len(*res) {
		ds.Lock.RUnlock()
		if p.HasFlag(Icinga2) || len(data) == 0 {
			err = ds.reloadIfNumberOfObjectsChanged()
			return
		}
		err = &PeerError{msg: fmt.Sprintf("%s cache not ready, got %d entries but only have %d in cache", store.Table.Name.String(), len(*res), len(data)), kind: ResponseError}
		log.Warnf("[%s] %s", p.Name, err.Error())
		p.setBroken(fmt.Sprintf("got more %s than expected. Hint: check clients 'max_response_size' setting.", store.Table.Name.String()))
		return
	}
	for i := range *res {
		row := (*res)[i]
		if data[i].CheckChangedIntValues(&row, columns) {
			missing[interface2int64(row[0])] = true
		}
	}
	ds.Lock.RUnlock()
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
	for i := range *res {
		resRow := &(*res)[i]
		id := fmt.Sprintf("%d", interface2int64((*resRow)[0]))
		_, ok := idIndex[id]
		if !ok {
			log.Debugf("adding %s with id %s", name.String(), id)
			id64 := int64((*resRow)[0].(float64))
			missingIds = append(missingIds, id64)
		}
		resIndex[id] = true
	}

	// remove old comments / downtimes
	for id := range idIndex {
		_, ok := resIndex[id]
		if !ok {
			log.Debugf("removing %s with id %s", name.String(), id)
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
		err = ds.RebuildCommentsCache()
		if err != nil {
			return
		}
	case TableDowntimes:
		err = ds.RebuildDowntimesCache()
		if err != nil {
			return
		}
	}

	log.Debugf("[%s] updated %s", p.Name, name.String())
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

	if len(*res) == 0 || float64(entries) == interface2float64((*res)[0][0]) && (entries == 0 || interface2float64((*res)[0][1]) == float64(maxID)) {
		log.Tracef("[%s] %s did not change", p.Name, name.String())
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
	if primaryKeysLen > 0 {
		columns = columns[primaryKeysLen:]
	}

	req := &Request{
		Table:   store.Table.Name,
		Columns: columns,
	}
	p.setQueryOptions(req)
	res, resMeta, err := p.Query(req)
	if err != nil {
		return
	}

	// icinga2 returns hosts and services in random order but we assume ordered results later
	if p.HasFlag(Icinga2) {
		res = res.SortByPrimaryKey(store.Table, req)
	}

	ds.Lock.RLock()
	data := store.Data
	ds.Lock.RUnlock()
	if len(*res) != len(data) {
		err = fmt.Errorf("site returned different number of objects, assuming backend has been restarted, table: %s, expected: %d, received: %d", store.Table.Name.String(), len(data), len(*res))
		log.Debugf("[%s] %s", err.Error())
		return &PeerError{msg: err.Error(), kind: RestartRequiredError}
	}

	promObjectUpdate.WithLabelValues(p.Name, tableName.String()).Add(float64(len(*res)))
	t1 := time.Now()

	switch tableName {
	case TableTimeperiods:
		// check for changed timeperiods, because we have to update the linked hosts and services as well
		err = ds.updateTimeperiodsData(store, res, &store.DynamicColumnCache)
		lastTimeperiodUpdateMinute, _ := strconv.Atoi(time.Now().Format("4"))
		p.StatusSet(LastTimeperiodUpdateMinute, lastTimeperiodUpdateMinute)
	case TableHosts, TableServices:
		err = ds.insertDeltaDataResult(0, res, resMeta, store)
	default:
		ds.Lock.Lock()
		now := time.Now().Unix()
		for i := range *res {
			row := (*res)[i]
			err = data[i].UpdateValues(0, &row, &store.DynamicColumnCache, now)
			if err != nil {
				ds.Lock.Unlock()
				return
			}
		}
		ds.Lock.Unlock()
	}

	if tableName == TableStatus {
		logDebugError(p.checkStatusFlags(data))
		if !p.HasFlag(MultiBackend) && len(data) >= 1 {
			programStart := data[0].GetInt64ByName("program_start")
			corePid := data[0].GetIntByName("nagios_pid")
			if p.StatusGet(ProgramStart) != programStart || p.StatusGet(LastPid) != corePid {
				err = fmt.Errorf("site has been restarted, recreating objects (program_start: %d, pid: %d)", programStart, corePid)
				log.Infof("[%s] %s", p.Name, err.Error())
				return &PeerError{msg: err.Error(), kind: RestartRequiredError}
			}
		}
	}

	duration := time.Since(t1).Truncate(time.Millisecond)
	log.Debugf("[%s] updated table: %15s - fetch: %8s - insert: %8s - count: %8d - size: %8d kB", p.Name, tableName.String(), resMeta.Duration, duration, len(*res), resMeta.Size/1024)
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

func (ds *DataStoreSet) updateTimeperiodsData(store *DataStore, res *ResultSet, columns *ColumnList) (err error) {
	changedTimeperiods := make(map[string]bool)
	ds.Lock.Lock()
	data := store.Data
	now := time.Now().Unix()
	nameCol := store.GetColumn("name")
	ds.Lock.Unlock()
	for i := range *res {
		row := (*res)[i]
		if data[i].CheckChangedIntValues(&row, columns) {
			changedTimeperiods[*(data[i].GetString(nameCol))] = true
		}
		err = data[i].UpdateValues(0, &row, columns, now)
		if err != nil {
			return
		}
	}
	// Update hosts and services with those changed timeperiods
	for name, state := range changedTimeperiods {
		log.Debugf("[%s] timeperiod %s has changed to %v, need to update affected hosts/services", ds.peer.Name, name, state)
		err = ds.UpdateDeltaHosts("Filter: check_period = " + name + "\nFilter: notification_period = " + name + "\nOr: 2\n")
		if err != nil {
			return
		}
		err = ds.UpdateDeltaServices("Filter: check_period = " + name + "\nFilter: notification_period = " + name + "\nOr: 2\n")
		if err != nil {
			return
		}
	}
	return
}

// RebuildCommentsCache updates the comment cache
func (ds *DataStoreSet) RebuildCommentsCache() (err error) {
	t1 := time.Now()
	cache, err := ds.buildDowntimeCommentsCache(TableComments)
	if err != nil {
		return
	}
	ds.Lock.Lock()
	ds.cache.comments = cache
	ds.Lock.Unlock()
	duration := time.Since(t1).Truncate(time.Millisecond)
	log.Debugf("comments cache rebuild (%s)", duration)
	return
}

// RebuildDowntimesCache updates the comment cache
func (ds *DataStoreSet) RebuildDowntimesCache() (err error) {
	t1 := time.Now()
	cache, err := ds.buildDowntimeCommentsCache(TableDowntimes)
	if err != nil {
		return
	}
	ds.Lock.Lock()
	ds.cache.downtimes = cache
	ds.Lock.Unlock()
	duration := time.Since(t1).Truncate(time.Millisecond)
	log.Debugf("downtimes cache rebuild (%s)", duration)
	return
}

// buildDowntimesCache returns the downtimes/comments cache
func (ds *DataStoreSet) buildDowntimeCommentsCache(name TableName) (cache map[*DataRow][]int64, err error) {
	store := ds.Get(name)
	if store == nil {
		return nil, fmt.Errorf("cannot build cache, peer is down: %s", ds.peer.getError())
	}

	ds.Lock.RLock()
	defer ds.Lock.RUnlock()

	cache = make(map[*DataRow][]int64)
	idIndex := store.Table.GetColumn("id").Index
	hostNameIndex := store.Table.GetColumn("host_name").Index
	serviceDescIndex := store.Table.GetColumn("service_description").Index
	hostIndex := ds.tables[TableHosts].Index
	serviceIndex := ds.tables[TableServices].Index2
	for i := range store.Data {
		row := store.Data[i]
		key := row.dataString[hostNameIndex]
		serviceName := row.dataString[serviceDescIndex]
		var obj *DataRow
		if serviceName != "" {
			obj = serviceIndex[key][serviceName]
		} else {
			obj = hostIndex[key]
		}
		id := row.dataInt64[idIndex]
		cache[obj] = append(cache[obj], id)
	}
	promObjectCount.WithLabelValues(ds.peer.Name, name.String()).Set(float64(len(store.Data)))

	return cache, nil
}
