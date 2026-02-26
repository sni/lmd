package lmd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const missedTimestampMaxFilter = 150

// DataStoreSet is a collection of data stores.
type DataStoreSet struct {
	peer *Peer
	sync SyncStrategy

	tableCommands      atomic.Pointer[DataStore]
	tableComments      atomic.Pointer[DataStore]
	tableContactgroups atomic.Pointer[DataStore]
	tableContacts      atomic.Pointer[DataStore]
	tableDowntimes     atomic.Pointer[DataStore]
	tableHostgroups    atomic.Pointer[DataStore]
	tableHosts         atomic.Pointer[DataStore]
	tableServicegroups atomic.Pointer[DataStore]
	tableServices      atomic.Pointer[DataStore]
	tableStatus        atomic.Pointer[DataStore]
	tableTimeperiods   atomic.Pointer[DataStore]
}

func NewDataStoreSet(peer *Peer) *DataStoreSet {
	dataset := DataStoreSet{peer: peer}
	dataset.sync = NewSyncStrategy(&dataset)

	return &dataset
}

func (ds *DataStoreSet) setSyncStrategy() {
	peer := ds.peer
	switch {
	case peer.hasFlag(LMD):
		logWith(peer).Debugf("using sync strategy: LMD")
		ds.sync = &SyncStrategyLMD{}
	case peer.hasFlag(MultiBackend):
		logWith(peer).Debugf("using sync strategy: MultiBackend")
		ds.sync = &SyncStrategyMultiBackend{}
	case peer.hasFlag(HasLastUpdateColumn):
		logWith(peer).Debugf("using sync strategy: LastUpdate")
		ds.sync = &SyncStrategyLastUpdate{}
	default:
		logWith(peer).Debugf("using sync strategy: LastCheck")
		ds.sync = &SyncStrategyLastCheck{}
	}

	ds.sync.Init(ds)
}

// initAllTables creates all tables for this store.
// It returns nil if the import was successful or an error otherwise.
func (ds *DataStoreSet) initAllTables(ctx context.Context) (err error) {
	now := currentUnixTime()
	ds.peer.lastUpdate.Set(now)
	ds.peer.lastFullUpdate.Set(now)

	if ds.peer.lmd.Config.MaxParallelPeerConnections <= 1 {
		err = ds.initAllTablesSerial(ctx)
	} else {
		err = ds.initAllTablesParallel(ctx)
	}
	if err != nil {
		return err
	}

	if ds.peer.hasFlag(MultiBackend) {
		return nil
	}

	err = ds.setReferences()
	if err != nil {
		return err
	}

	err = ds.rebuildCommentsList()
	if err != nil {
		return err
	}

	err = ds.rebuildDowntimesList()
	if err != nil {
		return err
	}

	err = ds.rebuildContactsGroups()
	if err != nil {
		return err
	}

	return nil
}

// fetches all objects one at a time.
func (ds *DataStoreSet) initAllTablesSerial(ctx context.Context) (err error) {
	time1 := time.Now()

	// fetch one at a time
	for _, n := range Objects.UpdateTables {
		t := Objects.Tables[n]
		err = ds.initTable(ctx, t)
		if err != nil {
			logWith(ds.peer).Debugf("fetching %s objects failed: %s", t.name.String(), err.Error())

			return err
		}
	}

	logWith(ds.peer).Debugf("objects fetched serially in %s", time.Since(time1).String())

	return err
}

// initAllTablesParallel fetches all objects at once.
func (ds *DataStoreSet) initAllTablesParallel(ctx context.Context) (err error) {
	time1 := time.Now()

	// go with status table first
	err = ds.initTable(ctx, Objects.Tables[TableStatus])
	if err != nil {
		return err
	}

	// then fetch all others in parallel
	results := make(chan error, len(Objects.UpdateTables)-1)
	wait := &sync.WaitGroup{}
	for _, n := range Objects.UpdateTables {
		if n == TableStatus {
			continue
		}
		table := Objects.Tables[n]
		wait.Add(1)
		go func(table *Table) {
			// make sure we log panics properly
			defer logPanicExitPeer(ds.peer)
			defer func() {
				wait.Done()
			}()

			err2 := ds.initTable(ctx, table)
			results <- err2
			if err2 != nil {
				logWith(ds.peer).Debugf("fetching %s objects failed: %s", table.name.String(), err2.Error())

				return
			}
		}(table)
	}

	// wait till fetching all tables finished
	go func() {
		wait.Wait()
		close(results)
	}()

	// read results till channel is closed
	for e := range results {
		if e != nil {
			return e
		}
	}

	logWith(ds.peer).Debugf("objects fetched parallel in %s", time.Since(time1).String())

	return nil
}

// initTable initializes a single table for this peer.
func (ds *DataStoreSet) initTable(ctx context.Context, table *Table) (err error) {
	if ds.peer.hasFlag(MultiBackend) && table.name != TableStatus {
		// just create empty data pools
		// real data is handled by separate peers
		return nil
	}

	var store *DataStore
	if !table.passthroughOnly && table.virtual == nil {
		store, err = ds.createObjectByType(ctx, table)
		if err != nil {
			logWith(ds.peer).Debugf("creating initial objects failed in table %s: %s", table.name.String(), err.Error())

			return err
		}
		ds.setTable(table.name, store)
	}
	switch table.name {
	case TableStatus:
		err = ds.peer.updateInitialStatus(ctx, store)
		if err != nil {
			return err
		}
		// got an answer, remove last error and let clients know we are reconnecting
		state := ds.peer.peerState.Get()
		if state != PeerStatusPending && state != PeerStatusSyncing {
			ds.peer.peerState.Set(PeerStatusSyncing)
			ds.peer.lastError.Set("reconnecting...")
		}
	case TableTimeperiods:
		lastTimeperiodUpdateMinute := int32(interface2int8(time.Now().Format("4")))
		ds.peer.lastTimeperiodUpdateMinute.Store(lastTimeperiodUpdateMinute)
	default:
		// nothing special happens for the other tables
	}

	return nil
}

func (ds *DataStoreSet) setTable(name TableName, store *DataStore) {
	switch name {
	case TableCommands:
		ds.tableCommands.Store(store)
	case TableComments:
		ds.tableComments.Store(store)
	case TableContactgroups:
		ds.tableContactgroups.Store(store)
	case TableContacts:
		ds.tableContacts.Store(store)
	case TableDowntimes:
		ds.tableDowntimes.Store(store)
	case TableHostgroups:
		ds.tableHostgroups.Store(store)
	case TableHosts:
		ds.tableHosts.Store(store)
	case TableServicegroups:
		ds.tableServicegroups.Store(store)
	case TableServices:
		ds.tableServices.Store(store)
	case TableStatus:
		ds.tableStatus.Store(store)
	case TableTimeperiods:
		ds.tableTimeperiods.Store(store)
	default:
		log.Panicf("unsupported table name: %s", name.String())
	}
}

func (ds *DataStoreSet) get(name TableName) *DataStore {
	switch name {
	case TableCommands:
		return (ds.tableCommands.Load())
	case TableComments:
		return (ds.tableComments.Load())
	case TableContactgroups:
		return (ds.tableContactgroups.Load())
	case TableContacts:
		return (ds.tableContacts.Load())
	case TableDowntimes:
		return (ds.tableDowntimes.Load())
	case TableHostgroups:
		return (ds.tableHostgroups.Load())
	case TableHosts:
		return (ds.tableHosts.Load())
	case TableServicegroups:
		return (ds.tableServicegroups.Load())
	case TableServices:
		return (ds.tableServices.Load())
	case TableStatus:
		return (ds.tableStatus.Load())
	case TableTimeperiods:
		return (ds.tableTimeperiods.Load())
	default:
		log.Panicf("unsupported table name: %s", name.String())
	}

	return nil
}

// createObjectByType fetches all static and dynamic data from the remote site and creates the initial table.
// It returns any error encountered.
func (ds *DataStoreSet) createObjectByType(ctx context.Context, table *Table) (*DataStore, error) {
	peer := ds.peer

	store := NewDataStore(table, peer)
	store.dataSet = ds
	keys, columns := store.GetInitialColumns()

	// fetch remote objects in blocks
	limit := peer.lmd.Config.InitialSyncBlockSize
	if limit <= 0 {
		limit = DefaultInitialSyncBlockSize
	}

	var lastReq *Request
	offset := 0
	results := []*ResultSet{}
	metaData := []*ResultMetaData{}
	totalPrepTime := time.Duration(0)
	totalRowNum := 0
	tableName := table.name.String()

	for {
		req := &Request{
			Table:   store.table.name,
			Columns: keys,
			Limit:   &limit,
			Offset:  offset,
		}
		lastReq = req
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
				err := fmt.Errorf("%s result set verification failed: len mismatch in row %d, expected %d columns and got %d", store.table.name.String(), i, keyLen, len(row))
				if peer.errorCount.Load() > 0 {
					// silently cancel, backend broke during initialization, should have been logged already
					log.Debugf("error during %s initialization, but backend is already failed: %s", &table.name, err.Error())
				} else {
					log.Errorf("error during %s initialization: %s", &table.name, err.Error())
				}

				return nil, err
			}
		}

		metaData = append(metaData, resMeta)
		results = append(results, &res)

		totalPrepTime += time.Since(time1).Truncate(time.Millisecond)
		totalRowNum += len(res)

		if len(res) < limit {
			break
		}
		logWith(peer, lastReq).Debugf("initial table: %15s - fetching bulk: %d", tableName, offset)

		offset += limit
	}

	resMeta := ds.mergeResultMetas(metaData)

	time2 := time.Now()
	now := currentUnixTime()
	err := store.InsertDataMulti(results, columns, false)
	if err != nil {
		return nil, err
	}

	durationInsert := time.Since(time2).Truncate(time.Millisecond)

	time3 := time.Now()

	ds.peer.lastUpdate.Set(now)
	ds.peer.lastFullUpdate.Set(now)
	durationLock := time.Since(time3).Truncate(time.Millisecond)

	promObjectCount.WithLabelValues(peer.Name, tableName).Set(float64(totalRowNum))

	logWith(peer, lastReq).Debugf("initial table: %15s - fetch: %9s - prep: %9s - lock: %9s - insert: %9s - count: %8d - size: %8d kB",
		tableName, resMeta.Duration.Truncate(time.Millisecond), totalPrepTime, durationLock, durationInsert, totalRowNum, resMeta.Size/1024)

	return store, nil
}

// tryTimeperiodsUpdate updates timeperiods every full minute except when idling.
func (ds *DataStoreSet) tryTimeperiodsUpdate(ctx context.Context) (ok bool, err error) {
	lastTimeperiodUpdateMinute := ds.peer.lastTimeperiodUpdateMinute.Load()
	currentMinute := int32(interface2int8(time.Now().Format("4")))
	if lastTimeperiodUpdateMinute == currentMinute {
		return false, nil
	}

	peer := ds.peer
	idling := peer.idling.Load()
	if idling {
		return false, nil
	}

	ds.peer.lastTimeperiodUpdateMinute.Store(currentMinute)
	err = ds.timeperiodsUpdate(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (ds *DataStoreSet) timeperiodsUpdate(ctx context.Context) (err error) {
	peer := ds.peer
	t1 := time.Now()
	err = ds.updateFullTablesList(ctx, []TableName{TableTimeperiods, TableHostgroups, TableServicegroups})
	duration := time.Since(t1).Truncate(time.Millisecond)
	logWith(peer).Debugf("updating timeperiods and host/servicegroup statistics completed (%s)", duration)
	if err != nil {
		return err
	}
	if err2 := peer.requestLocaltime(ctx); err2 != nil {
		return err2
	}
	// this also sets the thruk version and checks the clock, so it should be called first
	if _, _, err = peer.fetchThrukExtras(ctx); err != nil {
		// log error, but this should not prevent accessing the backend
		log.Debugf("fetchThrukExtras: %s ", err.Error())

		return err
	}

	return err
}

// run on demand comments / downtimes update.
func (ds *DataStoreSet) tryOnDemandComDownUpdate(ctx context.Context) (err error) {
	if !ds.peer.forceComments.Load() {
		return nil
	}

	ds.peer.forceComments.Store(false)
	err = ds.updateCommentsAndDowntimes(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (ds *DataStoreSet) mergeResultMetas(metas []*ResultMetaData) (resMeta *ResultMetaData) {
	if len(metas) == 0 {
		return nil
	}

	if len(metas) == 1 {
		return metas[0]
	}

	resMeta = metas[0]
	for i, meta := range metas {
		if i == 0 {
			continue
		}
		resMeta.Size += meta.Size
		resMeta.Duration += meta.Duration
		resMeta.RowsScanned += meta.RowsScanned
		resMeta.Total += meta.Total
	}

	return resMeta
}

// setReferences creates reference entries for all tables.
func (ds *DataStoreSet) setReferences() (err error) {
	for tableName, table := range Objects.Tables {
		if table.virtual != nil {
			continue
		}
		if len(table.refTables) == 0 {
			continue
		}
		store := ds.get(tableName)
		err = store.SetReferences()
		if err != nil {
			logWith(ds).Debugf("setting references on table %s failed: %s", tableName.String(), err.Error())

			return err
		}
	}

	return err
}

// updateFull runs a full update on all dynamic values for all tables which have dynamic updated columns.
// It returns any error occurred or nil if the update was successful.
func (ds *DataStoreSet) updateFull(ctx context.Context) (err error) {
	time1 := time.Now()
	err = ds.updateFullTablesList(ctx, Objects.UpdateTables)
	if err != nil {
		return err
	}
	peer := ds.peer
	duration := time.Since(time1)
	peerState := peer.peerState.Get()
	switch peerState {
	case PeerStatusUp, PeerStatusPending, PeerStatusSyncing:
	default:
		logWith(peer).Infof("site soft recovered from short outage (reason: %s - %s)", peerState.String(), peer.lastError.Get())
	}
	now := currentUnixTime()
	peer.resetErrors()
	ds.peer.lastUpdate.Set(now)
	ds.peer.lastFullUpdate.Set(now)
	peer.responseTime.Set(duration.Seconds())
	logWith(peer).Debugf("full update complete in: %s", duration.String())
	promPeerUpdates.WithLabelValues(peer.Name).Inc()
	promPeerUpdateDuration.WithLabelValues(peer.Name).Set(duration.Seconds())

	return err
}

func (ds *DataStoreSet) updateDelta(ctx context.Context, lastUpdate, now float64) (err error) {
	ds.peer.lastUpdate.Set(now)
	ds.peer.forceDelta.Store(false)

	// update status table
	err = ds.updateFullTablesList(ctx, Objects.StatusTables)
	if err != nil {
		return err
	}

	return ds.sync.UpdateDelta(ctx, lastUpdate, now)
}

func (ds *DataStoreSet) resumeFromIdle(ctx context.Context) (err error) {
	err = ds.updateDelta(ctx, ds.peer.lastUpdate.Get(), currentUnixTime())
	if err != nil {
		return err
	}

	return nil
}

// updateFullTablesList updates list of tables and returns any error.
func (ds *DataStoreSet) updateFullTablesList(ctx context.Context, tables []TableName) (err error) {
	for i := range tables {
		name := tables[i]
		err = ds.updateFullTable(ctx, name)
		if err != nil {
			logWith(ds).Debugf("update failed: %s", err.Error())

			return err
		}
	}

	return err
}

// updateDeltaHosts update hosts by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional updateOffset seconds.
// It returns any error encountered.
func (ds *DataStoreSet) updateDeltaHosts(ctx context.Context, filterStr string) (res ResultSet, updateSet []*ResultPrepared, err error) {
	table := ds.get(TableHosts)
	if table == nil {
		return nil, nil, fmt.Errorf("peer not ready, either not initialized or went offline recently")
	}

	req := &Request{
		Table:     TableHosts,
		Columns:   table.dynamicColumnNamesCache,
		FilterStr: filterStr,
	}
	ds.peer.setQueryOptions(req)
	res, meta, err := ds.peer.Query(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	dataOffset := 1
	updateSet, err = ds.insertDeltaDataResult(dataOffset, res, meta, table)

	return res, updateSet, err
}

// updateDeltaServices update services by fetching all dynamic data with a last_check filter on the timestamp since
// the previous update with additional updateOffset seconds.
// It returns any error encountered.
func (ds *DataStoreSet) updateDeltaServices(ctx context.Context, filterStr string) (res ResultSet, updateSet []*ResultPrepared, err error) {
	table := ds.get(TableServices)
	if table == nil {
		return nil, nil, fmt.Errorf("peer not ready, either not initialized or went offline recently")
	}

	req := &Request{
		Table:     TableServices,
		Columns:   table.dynamicColumnNamesCache,
		FilterStr: filterStr,
	}
	ds.peer.setQueryOptions(req)
	res, meta, err := ds.peer.Query(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	dataOffset := 2

	updateSet, err = ds.insertDeltaDataResult(dataOffset, res, meta, table)

	return res, updateSet, err
}

func (ds *DataStoreSet) insertDeltaDataResult(dataOffset int, res ResultSet, resMeta *ResultMetaData, table *DataStore) (updateSet []*ResultPrepared, err error) {
	updateType := "delta"
	time1 := time.Now()
	updateSet, err = table.prepareDataUpdateSet(dataOffset, res, table.dynamicColumnCache)
	if err != nil {
		return nil, err
	}
	durationPrepare := time.Since(time1).Truncate(time.Millisecond)

	now := currentUnixTime()
	time2 := time.Now()

	if len(res) == len(table.data) {
		updateType = "full"
	}

	table.lock.Lock()
	durationLock := time.Since(time2).Truncate(time.Millisecond)
	time3 := time.Now()

	for _, update := range updateSet {
		if update.FullUpdate {
			err = update.DataRow.UpdateValues(dataOffset, update.ResultRow, table.dynamicColumnCache, now)
		} else {
			err = update.DataRow.UpdateValuesNumberOnly(dataOffset, update.ResultRow, table.dynamicColumnCache, now)
		}
		if err != nil {
			table.lock.Unlock()

			return nil, err
		}
	}
	table.lock.Unlock()

	durationInsert := time.Since(time3).Truncate(time.Millisecond)

	p := ds.peer
	tableName := table.table.name.String()
	promObjectUpdate.WithLabelValues(p.Name, tableName).Add(float64(len(res)))
	logWith(p, resMeta.Request).Debugf("up. %-5s table: %15s - fetch: %9s - prep: %9s - lock: %9s - insert: %9s - fetched: %8d - changed: %8d - size: %8d kB",
		updateType, tableName, resMeta.Duration.Truncate(time.Millisecond), durationPrepare, durationLock, durationInsert, len(res), len(updateSet), resMeta.Size/1024)

	return updateSet, nil
}

// updateFullTable updates a given table by requesting all dynamic columns from the remote peer.
// Assuming we get the objects always in the same order, we can just iterate over the index and update the fields.
// It returns a boolean flag whether the remote site has been restarted and any error encountered.
func (ds *DataStoreSet) updateFullTable(ctx context.Context, tableName TableName) (err error) {
	peer := ds.peer
	store := ds.get(tableName)
	if store == nil {
		return fmt.Errorf("cannot update table %s, peer is down: %s", tableName.String(), peer.getError())
	}
	if ds.skipTableUpdate(store) {
		return nil
	}

	columns := store.dynamicColumnNamesCache
	// primary keys are not required, we fetch everything anyway
	primaryKeysLen := len(store.table.primaryKey)

	req := &Request{
		Table:   store.table.name,
		Columns: columns,
	}
	peer.setQueryOptions(req)
	res, resMeta, err := peer.Query(ctx, req)
	if err != nil {
		return err
	}

	store.lock.RLock()
	data := store.data
	store.lock.RUnlock()
	if len(res) != len(data) {
		err = fmt.Errorf("site returned different number of objects, assuming backend has been restarted, table: %s, expected: %d, received: %d",
			store.table.name.String(), len(data), len(res))
		logWith(peer).Debugf("%s", err.Error())

		return &PeerError{msg: err.Error(), kind: RestartRequiredError}
	}

	promObjectUpdate.WithLabelValues(peer.Name, tableName.String()).Add(float64(len(res)))

	var durationLock, durationInsert time.Duration
	switch tableName {
	case TableTimeperiods:
		// check for changed timeperiods, because we have to update the linked hosts and services as well
		err = ds.updateTimeperiodsData(ctx, primaryKeysLen, store, res, store.dynamicColumnCache)
		peer.lastTimeperiodUpdateMinute.Store(int32(interface2int8(time.Now().Format("4"))))
		if err != nil {
			return err
		}
		logWith(peer, req).Debugf("up. full  table: %15s - fetch: %9s - prep:       --- - lock: %9s - insert: %9s - fetched: %8d - changed: %8d - size: %8d kB",
			tableName.String(), resMeta.Duration.Truncate(time.Millisecond), durationLock, durationInsert, len(res), len(res), resMeta.Size/1024)
	case TableStatus:
		// check pid and date before updating tables.
		// Otherwise we and up with inconsistent state until the full refresh is finished
		err = peer.checkBackendRestarted(primaryKeysLen, res, store.dynamicColumnCache)
		if err != nil {
			return err
		}
		// continue with normal update
		fallthrough
	default:
		_, err = ds.insertDeltaDataResult(primaryKeysLen, res, resMeta, store)
		if err != nil {
			return err
		}
	}

	if tableName == TableStatus {
		LogErrors(peer.checkStatusFlags(ctx, ds))
	}

	return nil
}

// update both comments and downtimes.
func (ds *DataStoreSet) updateCommentsAndDowntimes(ctx context.Context) (err error) {
	ds.peer.forceComments.Store(false)
	err = ds.updateDeltaCommentsOrDowntimes(ctx, TableComments)
	if err != nil {
		return err
	}
	err = ds.updateDeltaCommentsOrDowntimes(ctx, TableDowntimes)
	if err != nil {
		return err
	}

	return nil
}

// updateDeltaCommentsOrDowntimes update the comments or downtimes table. It fetches the number and highest id of
// the remote comments/downtimes. If an update is required, it then fetches all ids to check which are new and
// which have to be removed.
// It returns any error encountered.
func (ds *DataStoreSet) updateDeltaCommentsOrDowntimes(ctx context.Context, name TableName) (err error) {
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

	store := ds.get(name)
	if store == nil {
		return nil
	}
	store.lock.Lock()
	idIndex := store.index
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
	changed = false
	for id := range idIndex {
		_, ok := resIndex[id]
		if !ok {
			logWith(ds, req).Debugf("removing %s with id %s", name.String(), id)
			tmp := idIndex[id]
			store.RemoveItem(tmp)
			changed = true
		}
	}
	store.lock.Unlock()

	if len(missingIDs) > 0 {
		changed = true
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

	if !changed {
		logWith(peer, req).Debugf("updated %s (no changes)", name.String())

		return nil
	}

	// reset cache
	switch name {
	case TableComments:
		err = ds.rebuildCommentsList()
		if err != nil {
			return err
		}
	case TableDowntimes:
		err = ds.rebuildDowntimesList()
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

	store := ds.get(name)
	var maxID int64
	store.lock.RLock()
	entries := len(store.data)
	if entries > 0 {
		maxID = store.data[entries-1].GetInt64ByName("id")
	}
	store.lock.RUnlock()

	if len(res) == 0 || float64(entries) == interface2float64(res[0][0]) && (entries == 0 || interface2float64(res[0][1]) == float64(maxID)) {
		logWith(peer, req).Tracef("%s did not change", name.String())

		return false, nil
	}

	return true, nil
}

func (ds *DataStoreSet) skipTableUpdate(store *DataStore) bool {
	if store.table.virtual != nil {
		return true
	}
	// no updates for passthrough tables, ex.: log
	if store.table.passthroughOnly {
		return true
	}
	if ds.peer.hasFlag(MultiBackend) && store.table.name != TableStatus {
		return true
	}
	if len(store.dynamicColumnNamesCache) == 0 {
		return true
	}

	return false
}

func (ds *DataStoreSet) updateTimeperiodsData(ctx context.Context, dataOffset int, store *DataStore, res ResultSet, columns ColumnList) (err error) {
	if ds.skipTableUpdate(store) {
		return nil
	}
	changedTimeperiods := make(map[string]bool)

	store.lock.Lock()
	data := store.data
	now := currentUnixTime()
	nameCol := store.GetColumn("name")
	store.lock.Unlock()

	for i := range res {
		row := res[i]
		if data[i].checkChangedIntValues(dataOffset, row, columns) {
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
		_, _, err = ds.updateDeltaHosts(ctx, "Filter: check_period = "+name+"\nFilter: notification_period = "+name+"\nOr: 2\n")
		if err != nil {
			return err
		}
		_, _, err = ds.updateDeltaServices(ctx, "Filter: check_period = "+name+"\nFilter: notification_period = "+name+"\nOr: 2\n")
		if err != nil {
			return err
		}
	}

	return nil
}

// rebuildCommentsList updates the comments column of hosts/services based on the comments table ids.
func (ds *DataStoreSet) rebuildCommentsList() (err error) {
	time1 := time.Now()
	err = ds.buildDowntimeCommentsList(TableComments)
	if err != nil {
		return err
	}

	duration := time.Since(time1)
	logWith(ds).Debugf("comments rebuild (%s)", duration.Truncate(time.Millisecond))

	return err
}

// rebuildDowntimesList updates the downtimes column of hosts/services based on the downtimes table ids.
func (ds *DataStoreSet) rebuildDowntimesList() (err error) {
	time1 := time.Now()
	err = ds.buildDowntimeCommentsList(TableDowntimes)
	if err != nil {
		return err
	}

	duration := time.Since(time1)
	logWith(ds).Debugf("downtimes rebuild (%s)", duration.Truncate(time.Millisecond))

	return err
}

// buildDowntimeCommentsList updates the downtimes/comments id list for all hosts and services.
func (ds *DataStoreSet) buildDowntimeCommentsList(name TableName) (err error) {
	store := ds.get(name)
	if store == nil {
		return fmt.Errorf("cannot build id list, peer is down: %s", ds.peer.getError())
	}

	hostStore := ds.get(TableHosts)
	if hostStore == nil {
		return fmt.Errorf("cannot build id list, peer is down: %s", ds.peer.getError())
	}
	hostIdx := hostStore.table.GetColumn(name.String()).Index

	serviceStore := ds.get(TableServices)
	if serviceStore == nil {
		return fmt.Errorf("cannot build id list, peer is down: %s", ds.peer.getError())
	}
	serviceIdx := serviceStore.table.GetColumn(name.String()).Index

	hostResult := make(map[*DataRow][]int64)
	serviceResult := make(map[*DataRow][]int64)

	hostStore.lock.RLock()
	serviceStore.lock.RLock()
	store.lock.RLock()
	idIndex := store.table.GetColumn("id").Index
	hostNameIndex := store.table.GetColumn("host_name").Index
	serviceDescIndex := store.table.GetColumn("service_description").Index
	hostIndex := hostStore.index
	serviceIndex := serviceStore.index2
	for i := range store.data {
		row := store.data[i]
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
	hostStore.lock.RUnlock()
	serviceStore.lock.RUnlock()
	store.lock.RUnlock()
	promObjectCount.WithLabelValues(ds.peer.Name, name.String()).Set(float64(len(store.data)))

	// empty current lists
	hostStore.lock.Lock()
	for _, d := range hostStore.data {
		d.dataInt64List[hostIdx] = emptyInt64List
	}
	// store updated lists
	for d, ids := range hostResult {
		d.dataInt64List[hostIdx] = ids
	}
	hostStore.lock.Unlock()

	serviceStore.lock.Lock()
	for _, d := range serviceStore.data {
		d.dataInt64List[serviceIdx] = emptyInt64List
	}
	for d, ids := range serviceResult {
		d.dataInt64List[serviceIdx] = ids
	}
	serviceStore.lock.Unlock()

	return nil
}

// rebuildContactsGroups sets the contacts "groups" list from contactgroups members
// in case the backend does not support the contacts->groups attribute, ex.: icinga2.
func (ds *DataStoreSet) rebuildContactsGroups() (err error) {
	// directly supported, nothing to do
	if ds.peer.hasFlag(HasContactsGroupColumn) {
		return err
	}

	time1 := time.Now()
	err = ds.buildContactsGroupsList()
	if err != nil {
		return err
	}

	duration := time.Since(time1)
	logWith(ds).Debugf("contacts groups rebuild (%s)", duration.Truncate(time.Millisecond))

	return err
}

// buildContactsGroupsList updates the contacts->groups attribute from contactgroups->members.
func (ds *DataStoreSet) buildContactsGroupsList() (err error) {
	groupsStore := ds.get(TableContactgroups)
	if groupsStore == nil {
		return fmt.Errorf("cannot build groups list, peer is down: %s", ds.peer.getError())
	}
	groupNameIdx := groupsStore.table.GetColumn("name").Index
	membersIdx := groupsStore.table.GetColumn("members").Index

	contactsStore := ds.get(TableContacts)
	if contactsStore == nil {
		return fmt.Errorf("cannot build groups list, peer is down: %s", ds.peer.getError())
	}
	groupsIdx := contactsStore.table.GetColumn("groups").Index

	// build groups->members relation
	contactGroups := make(map[string][]string)
	groupsStore.lock.RLock()
	for _, row := range groupsStore.data {
		groupName := row.dataString[groupNameIdx]
		members := row.dataStringList[membersIdx]
		for _, member := range members {
			if _, ok := contactGroups[member]; !ok {
				contactGroups[member] = []string{groupName}
			} else {
				contactGroups[member] = append(contactGroups[member], groupName)
			}
		}
	}
	groupsStore.lock.RUnlock()

	// deduplicate data
	for i, row := range contactGroups {
		contactGroups[i] = dedupStringList(row, true)
	}

	// empty current lists
	contactsStore.lock.Lock()
	for _, d := range contactsStore.data {
		d.dataStringList[groupsIdx] = emptyStringList
	}
	// store updated groups
	for contactName, groups := range contactGroups {
		row, ok := contactsStore.index[contactName]
		if !ok {
			logWith(ds).Warnf("unknown contact encountered: %s", contactName)

			continue
		}
		row.dataStringList[groupsIdx] = groups
	}
	contactsStore.lock.Unlock()

	return nil
}
