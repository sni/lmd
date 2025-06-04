package lmd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/OneOfOne/xxhash"
	"github.com/sasha-s/go-deadlock"
)

// DataStore contains the actual data rows with a reference to the table and peer.
type DataStore struct {
	noCopy                  noCopy
	lock                    *deadlock.RWMutex
	index                   map[string]*DataRow            // access data rows from primary key, ex.: hostname or comment id
	index2                  map[string]map[string]*DataRow // access data rows from 2 primary keys, ex.: host and service
	indexLowerCase          map[string][]string            // access data rows from lower case primary key
	peer                    *Peer                          // reference to our peer
	lowerCaseColumns        map[int]int                    // list of string column indexes with their corresponding lower case index
	dupStringList           map[uint32][]string            // lookup pointer to other stringlists during initialization
	table                   *Table                         // reference to table definition
	dataSet                 *DataStoreSet                  // reference to parent DataSet
	dynamicColumnCache      ColumnList                     // contains list of keys used to run periodic update
	data                    []*DataRow                     // the actual data rows
	dynamicColumnNamesCache []string                       // contains list of keys used to run periodic update
}

// NewDataStore creates a new datastore with columns based on given flags.
func NewDataStore(table *Table, peer *Peer) (d *DataStore) {
	d = &DataStore{
		lock:                    new(deadlock.RWMutex),
		data:                    make([]*DataRow, 0),
		index:                   make(map[string]*DataRow),
		index2:                  make(map[string]map[string]*DataRow),
		indexLowerCase:          make(map[string][]string),
		dynamicColumnCache:      make(ColumnList, 0),
		dynamicColumnNamesCache: make([]string, 0),
		dupStringList:           make(map[uint32][]string),
		table:                   table,
		peer:                    peer,
		lowerCaseColumns:        make(map[int]int),
	}

	// create columns list
	table.lock.RLock()
	writeLocked := false
	dataSizes := table.dataSizes

	for i := range table.columns {
		col := table.columns[i]
		if col.Optional != NoFlags && !d.peer.HasFlag(col.Optional) {
			continue
		}
		if col.StorageType != LocalStore {
			continue
		}
		if col.Index == -1 {
			// require write lock and update table column
			if !writeLocked {
				table.lock.RUnlock()
				table.lock.Lock()
				writeLocked = true
			}
			// check index again, might have been updated meanwhile
			if col.Index == -1 {
				col.Index = dataSizes[col.DataType]
				dataSizes[col.DataType]++
			}
		}
		if col.FetchType == Dynamic {
			d.dynamicColumnCache = append(d.dynamicColumnCache, col)
		}
		if strings.HasSuffix(col.Name, "_lc") {
			refCol := table.GetColumn(strings.TrimSuffix(col.Name, "_lc"))
			d.lowerCaseColumns[refCol.Index] = col.Index
		}
	}

	if writeLocked {
		table.lock.Unlock()
	} else {
		table.lock.RUnlock()
	}

	// prepend primary keys to dynamic keys, since they are required to map the results back to specific items
	if len(d.dynamicColumnCache) > 0 {
		d.dynamicColumnNamesCache = append(d.dynamicColumnNamesCache, table.primaryKey...)
		for _, col := range d.dynamicColumnCache {
			d.dynamicColumnNamesCache = append(d.dynamicColumnNamesCache, col.Name)
		}
	}

	return d
}

// InsertData adds a resultSet and initializes the store table.
func (d *DataStore) InsertData(row ResultSet, columns ColumnList, setReferences bool) error {
	return d.InsertDataMulti([]*ResultSet{&row}, columns, setReferences)
}

// InsertDataMulti adds a list of resultSets and initializes the store table.
func (d *DataStore) InsertDataMulti(rowSet []*ResultSet, columns ColumnList, setReferences bool) error {
	totalNum := 0
	for _, rows := range rowSet {
		totalNum += len(*rows)
	}
	now := currentUnixTime()
	d.data = make([]*DataRow, totalNum)

	// do not set references for multi inserts
	if len(rowSet) > 1 {
		setReferences = false
	}

	num := 0
	for idx := range rowSet {
		rows := *rowSet[idx]
		for i, raw := range rows {
			row, err := NewDataRow(d, raw, columns, now, setReferences)
			rows[i] = nil // free memory
			if err != nil {
				log.Errorf("adding new %s failed: %s", d.table.name.String(), err.Error())

				return err
			}
			d.data[num] = row
			num++
		}
		*rowSet[idx] = nil // free memory
	}

	// reset after initial setup
	d.dupStringList = make(map[uint32][]string)

	// make sure all backends are sorted the same way
	d.sortByPrimaryKey()

	// create index
	d.rebuildIndex()

	return nil
}

// AppendData append a list of results and initializes the store table.
func (d *DataStore) AppendData(data ResultSet, columns ColumnList) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.table.lock.RLock()
	defer d.table.lock.RUnlock()
	if d.index == nil {
		// should not happen but might indicate a recent restart or backend issue
		return fmt.Errorf("index not ready, cannot append data")
	}
	for i := range data {
		resRow := data[i]
		row, nErr := NewDataRow(d, resRow, columns, 0, true)
		if nErr != nil {
			return nErr
		}
		d.AddItem(row)
	}

	return nil
}

// rebuildIndex refreshes the index for all data rows.
func (d *DataStore) rebuildIndex() {
	switch len(d.table.primaryKey) {
	case 0:
		return
	case 1:
		d.index = make(map[string]*DataRow, len(d.data))
		for i := range d.data {
			row := d.data[i]
			id := dedup.S(row.GetID())
			d.index[id] = row
			if d.table.name == TableHosts {
				idLower := dedup.S(strings.ToLower(id))
				if idLower != id {
					d.indexLowerCase[idLower] = append(d.indexLowerCase[idLower], id)
				}
			}
		}
	case 2:
		d.index2 = make(map[string]map[string]*DataRow)
		for i := range d.data {
			row := d.data[i]
			id1, id2 := row.GetID2()
			id1 = dedup.S(id1)
			id2 = dedup.S(id2)
			if _, ok := d.index2[id1]; !ok {
				d.index2[id1] = make(map[string]*DataRow)
			}
			d.index2[id1][id2] = row
		}
	default:
		panic("not supported number of primary keys")
	}
}

// AddItem adds an new DataRow to a DataStore.
func (d *DataStore) AddItem(row *DataRow) {
	d.data = append(d.data, row)
	switch len(d.table.primaryKey) {
	case 0:
	case 1:
		id := dedup.S(row.GetID())
		d.index[id] = row
		if d.table.name == TableHosts {
			idLower := dedup.S(strings.ToLower(id))
			if idLower != id {
				d.indexLowerCase[idLower] = append(d.indexLowerCase[idLower], id)
			}
		}
	case 2:
		id1, id2 := row.GetID2()
		id1 = dedup.S(id1)
		id2 = dedup.S(id2)
		if _, ok := d.index2[id1]; !ok {
			d.index2[id1] = make(map[string]*DataRow)
		}
		d.index2[id1][id2] = row
	default:
		panic("not supported number of primary keys")
	}
}

// RemoveItem removes a DataRow from a DataStore.
func (d *DataStore) RemoveItem(row *DataRow) {
	switch len(d.table.primaryKey) {
	case 0:
	case 1:
		delete(d.index, row.GetID())
		if d.table.name == TableHosts {
			panic("removing from hosts index is not supported")
		}
	default:
		panic("not supported number of primary keys")
	}
	for i := range d.data {
		if d.data[i] == row {
			d.data = append(d.data[:i], d.data[i+1:]...)

			return
		}
	}
	log.Panicf("element not found")
}

// SetReferences creates reference entries for this tables.
func (d *DataStore) SetReferences() (err error) {
	if len(d.table.refTables) == 0 {
		return
	}

	for _, row := range d.data {
		err = row.SetReferences()
		if err != nil {
			logWith(d).Debugf("setting references on table %s failed: %s", d.table.name.String(), err.Error())

			return
		}
	}

	return
}

// GetColumn returns column by name.
func (d *DataStore) GetColumn(name string) *Column {
	return d.table.columnsIndex[name]
}

// GetInitialColumns returns list of columns required to fill initial dataset.
func (d *DataStore) GetInitialColumns() ([]string, ColumnList) {
	columns := make(ColumnList, 0)
	keys := make([]string, 0)
	for i := range d.table.columns {
		col := d.table.columns[i]
		if d.peer != nil && !d.peer.HasFlag(col.Optional) {
			continue
		}
		if col.StorageType != LocalStore {
			continue
		}
		if col.FetchType == None {
			continue
		}
		columns = append(columns, col)
		keys = append(keys, col.Name)
	}

	return keys, columns
}

func (d *DataStore) GetWaitObject(req *Request) (*DataRow, bool) {
	if req.Table == TableServices {
		parts := strings.SplitN(req.WaitObject, ";", 2)
		obj, ok := d.index2[parts[0]][parts[1]]

		return obj, ok
	}

	obj, ok := d.index[req.WaitObject]

	return obj, ok
}

type getPreFilteredDataFilter func(map[string]bool, *Filter) bool

// GetPreFilteredData returns d.Data but try to return reduced dataset by using host / service index if table supports it.
func (d *DataStore) GetPreFilteredData(filter []*Filter) []*DataRow {
	if len(filter) == 0 {
		return d.data
	}

	switch {
	case d.table.name == TableHosts:
		return (d.tryFilterIndexData(filter, d.appendIndexHostsFromHostColumns))
	case d.table.name == TableServices:
		return (d.tryFilterIndexData(filter, d.appendIndexHostsFromServiceColumns))
	case len(d.table.primaryKey) == 1:
		return (d.tryFilterIndexData(filter, d.appendIndexFromPrimaryKey))
	default:
		// only hosts and services are supported
	}

	return d.data
}

func (d *DataStore) tryFilterIndexData(filter []*Filter, fn getPreFilteredDataFilter) []*DataRow {
	uniqRows := make(map[string]bool)
	ok := d.TryFilterIndex(uniqRows, filter, fn, false)
	if !ok {
		return d.data
	}
	// sort and return list of index names used
	primaryKeyList := []string{}
	for key := range uniqRows {
		primaryKeyList = append(primaryKeyList, key)
	}
	sort.Strings(primaryKeyList)
	indexedData := make([]*DataRow, 0)
	switch d.table.name {
	case TableServices:
		for _, name := range primaryKeyList {
			services, ok := d.index2[name]
			if ok {
				// Sort services by description, asc
				keys := make([]string, 0)
				for key := range services {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					row := services[key]
					if row != nil {
						indexedData = append(indexedData, row)
					}
				}
			}
		}
	default:
		for _, name := range primaryKeyList {
			row, ok := d.index[name]
			if ok {
				indexedData = append(indexedData, row)
			}
		}
	}
	log.Tracef("using indexed %s dataset of size: %d", d.table.name.String(), len(indexedData))

	return indexedData
}

func (d *DataStore) prepareDataUpdateSet(dataOffset int, res ResultSet, columns ColumnList) (updateSet []*ResultPrepared, err error) {
	updateSet = make([]*ResultPrepared, 0, len(res))

	// compare last check date and only update large strings if the last check date has changed
	lastCheckDataIdx, lastCheckResIdx := d.getUpdateColumn("last_check", dataOffset)
	lastUpdateDataIdx, lastUpdateResIdx := d.getUpdateColumn("last_update", dataOffset)

	useIndex := dataOffset == 0 || len(res) == len(d.data)

	if useIndex {
		// make sure all backends are sorted the same way
		res.sortByPrimaryKey(d.table, columns)
	}

	// prepare update
	nameIndex := d.index
	nameIndex2 := d.index2
	for rowNum := range res {
		resRow := res[rowNum]
		prep := &ResultPrepared{
			ResultRow:  resRow,
			FullUpdate: false,
		}
		if useIndex {
			prep.DataRow = d.data[rowNum]
		} else {
			switch d.table.name {
			case TableHosts:
				hostName := interface2stringNoDedup(resRow[0])
				dataRow := nameIndex[hostName]
				if dataRow == nil {
					return updateSet, fmt.Errorf("cannot update host, no host named '%s' found", hostName)
				}
				prep.DataRow = dataRow
			case TableServices:
				hostName := interface2stringNoDedup(resRow[0])
				serviceName := interface2stringNoDedup(resRow[1])
				dataRow := nameIndex2[hostName][serviceName]
				if dataRow == nil {
					return updateSet, fmt.Errorf("cannot update service, no service named '%s' - '%s' found", hostName, serviceName)
				}

				prep.DataRow = dataRow
			default:
				log.Panicf("table not supported: %s", d.table.name.String())
			}
		}

		switch {
		case lastUpdateResIdx >= 0 && lastCheckResIdx >= 0:
			switch {
			case interface2int64(resRow[lastUpdateResIdx]) != prep.DataRow.dataInt64[lastUpdateDataIdx]:
				// last_update has changed -> always do a full update
				prep.FullUpdate = true
			case interface2int64(resRow[lastCheckResIdx]) != prep.DataRow.dataInt64[lastCheckDataIdx]:
				// last_check has changed -> always do a full update
				prep.FullUpdate = true
			default:
				// check both, last_check and last_update to catch up very fast checks which finish within the same second
				continue
			}
		case lastUpdateResIdx >= 0:
			if interface2int64(resRow[lastUpdateResIdx]) == prep.DataRow.dataInt64[lastUpdateDataIdx] {
				// if there is only a last_update column, we simply trust the core if an update is required
				// skip update completely
				continue
			}
			// last_update has changed -> always do a full update
			prep.FullUpdate = true
		case lastCheckResIdx < 0:
			// no last_check column and no last_update -> always do a full update
			prep.FullUpdate = true
		case interface2int64(resRow[lastCheckResIdx]) != prep.DataRow.dataInt64[lastCheckDataIdx]:
			// compare last check date and do a full update only if last check has changed
			prep.FullUpdate = true
		}

		// prepare deduped strings if required
		if prep.FullUpdate {
			for i, col := range columns {
				res[rowNum][i+dataOffset] = cast2Type(res[rowNum][i+dataOffset], col, d)
			}
		}
		updateSet = append(updateSet, prep)
	}

	return updateSet, nil
}

// getUpdateColumn returns data and result index for given column name, it panics if the column is not type int64.
// the index will be -1 if the column is not available.
func (d *DataStore) getUpdateColumn(columnName string, dataOffset int) (dataIndex, resIndex int) {
	dataIndex = -1
	resIndex = -1
	checkCol := d.GetColumn(columnName)
	if checkCol == nil {
		return
	}
	// double  check last_update column
	if columnName == "last_update" && !d.peer.HasFlag(HasLastUpdateColumn) {
		return
	}

	dataIndex = checkCol.Index
	resIndex = d.dynamicColumnCache.GetColumnIndex(columnName) + dataOffset
	if checkCol.DataType != Int64Col {
		log.Panicf("%s: assumption about column type for %s is wrong, expected %s and got %s",
			d.table.name.String(), checkCol.Name, Int64Col.String(), checkCol.DataType.String())
	}

	return
}

// TryFilterIndex returns list of hostname which can be used to reduce the initial dataset.
func (d *DataStore) TryFilterIndex(uniqHosts map[string]bool, filter []*Filter, filterCb getPreFilteredDataFilter, breakOnNoneIndexableFilter bool) bool {
	filterFound := 0
	for _, fil := range filter {
		if fil.negate {
			// not supported
			return false
		}

		switch fil.groupOperator {
		case And:
			ok := d.TryFilterIndex(uniqHosts, fil.filter, filterCb, false)
			if !ok {
				return false
			}
			filterFound++
		case Or:
			ok := d.TryFilterIndex(uniqHosts, fil.filter, filterCb, true)
			if !ok {
				return false
			}
			filterFound++
		default:
			if filterCb(uniqHosts, fil) {
				filterFound++
			} else if breakOnNoneIndexableFilter {
				return false
			}
		}
	}

	// index can only be used if there is at least one useable filter found
	return filterFound > 0
}

func (d *DataStore) appendIndexHostsFromHostColumns(uniqHosts map[string]bool, fil *Filter) bool {
	switch fil.column.Name {
	case "name":
		switch fil.operator {
		// name == <value>
		case Equal:
			uniqHosts[fil.stringVal] = true

			return true

		// name =~ <value>
		case EqualNocase:
			uniqHosts[fil.stringVal] = true
			for _, key := range d.indexLowerCase[strings.ToLower(fil.stringVal)] {
				uniqHosts[key] = true
			}

			return true
		default:
			// other operators are not supported
		}
	case "name_lc":
		switch fil.operator {
		// name == <value>
		case Equal, EqualNocase:
			uniqHosts[fil.stringVal] = true
			for _, key := range d.indexLowerCase[strings.ToLower(fil.stringVal)] {
				uniqHosts[key] = true
			}

			return true
		default:
			// other operators are not supported
		}
	case "groups":
		// get hosts from host groups members
		switch fil.operator {
		// groups >= <value>
		case GreaterThan:
			store := d.dataSet.Get(TableHostgroups)
			group, ok := store.index[fil.stringVal]
			if ok {
				members := group.GetStringListByName("members")
				for _, m := range members {
					uniqHosts[m] = true
				}
			}

			return true
		case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
			store := d.dataSet.Get(TableHostgroups)
			for groupname, group := range store.index {
				if fil.MatchString(strings.ToLower(groupname)) {
					members := group.GetStringListByName("members")
					for _, m := range members {
						uniqHosts[m] = true
					}
				}
			}

			return true
		default:
			// other operator are not supported
		}
	}

	return false
}

func (d *DataStore) appendIndexHostsFromServiceColumns(uniqHosts map[string]bool, fil *Filter) bool {
	switch fil.column.Name {
	case "host_name":
		switch fil.operator {
		// host_name == <value>
		case Equal:
			uniqHosts[fil.stringVal] = true

			return true
		// host_name ~ <value>
		case RegexMatch, Contains:
			store := d.dataSet.Get(TableHosts)
			for hostname := range store.index {
				if fil.MatchString(hostname) {
					uniqHosts[hostname] = true
				}
			}

			return true
		default:
			// other operators are not supported
		}
	case "host_name_lc":
		switch fil.operator {
		// host_name ~~ <value>
		case RegexMatch, Contains, RegexNoCaseMatch, ContainsNoCase, EqualNocase, Equal:
			store := d.dataSet.Get(TableHosts)
			for hostname := range store.index {
				if fil.MatchString(strings.ToLower(hostname)) {
					uniqHosts[hostname] = true
				}
			}

			return true
		default:
			// other operators are not supported
		}
	case "host_groups":
		// get hosts from host groups members
		switch fil.operator {
		// groups >= <value>
		case GreaterThan:
			store := d.dataSet.Get(TableHostgroups)
			group, ok := store.index[fil.stringVal]
			if ok {
				members := group.GetStringListByName("members")
				for _, m := range members {
					uniqHosts[m] = true
				}
			}

			return true
		case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
			store := d.dataSet.Get(TableHostgroups)
			for groupname, group := range store.index {
				if fil.MatchString(strings.ToLower(groupname)) {
					members := group.GetStringListByName("members")
					for _, m := range members {
						uniqHosts[m] = true
					}
				}
			}

			return true
		default:
			// other operators are not supported
		}
	case "groups":
		// get hosts from services groups members
		switch fil.operator {
		// groups >= <value>
		case GreaterThan:
			store := d.dataSet.Get(TableServicegroups)
			group, ok := store.index[fil.stringVal]
			if ok {
				members := group.GetServiceMemberListByName("members")
				for i := range members {
					uniqHosts[members[i][0]] = true
				}
			}

			return true
		case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
			store := d.dataSet.Get(TableServicegroups)
			for groupname, group := range store.index {
				if fil.MatchString(groupname) {
					members := group.GetServiceMemberListByName("members")
					for i := range members {
						uniqHosts[members[i][0]] = true
					}
				}
			}

			return true
		default:
			// other operators are not supported
		}
	default:
		// other columns are not supported
	}

	return false
}

func (d *DataStore) appendIndexFromPrimaryKey(uniqRows map[string]bool, fil *Filter) bool {
	key := d.table.primaryKey[0]

	switch fil.column.Name {
	case key:
		switch fil.operator {
		// name == <value>
		case Equal:
			uniqRows[fil.stringVal] = true

			return true

		// name =~ <value>
		case EqualNocase:
			uniqRows[fil.stringVal] = true
			for _, key := range d.indexLowerCase[strings.ToLower(fil.stringVal)] {
				uniqRows[key] = true
			}

			return true
		default:
			// other operators are not supported
		}
	case key + "_lc":
		switch fil.operator {
		// name == <value>
		case Equal, EqualNocase:
			uniqRows[fil.stringVal] = true
			for _, key := range d.indexLowerCase[strings.ToLower(fil.stringVal)] {
				uniqRows[key] = true
			}

			return true
		default:
			// other operators are not supported
		}
	default:
		// other operator are not supported
	}

	return false
}

// deduplicateStringlist store duplicate string lists only once.
func (d *DataStore) deduplicateStringlist(list []string) []string {
	sum := xxhash.ChecksumString32(strings.Join(list, ListSepChar1))
	if l, ok := d.dupStringList[sum]; ok {
		return l
	}

	// adding new list, deduplicate strings as well
	dedupedList := make([]string, 0, len(list))
	for i := range list {
		dedupedList = append(dedupedList, dedup.S(list[i]))
	}

	if sum > 0 {
		d.dupStringList[sum] = dedupedList
	}

	return dedupedList
}

func (d *DataStore) sortByPrimaryKey() {
	if len(d.table.primaryKey) == 0 {
		return
	}

	sort.Sort(d)
}

// Len returns the result length used for sorting results.
func (d *DataStore) Len() int {
	return len(d.data)
}

// Less returns the sort result of two data rows.
func (d *DataStore) Less(idx1, idx2 int) bool {
	for _, colName := range d.table.primaryKey {
		col := d.table.columnsIndex[colName]
		switch col.DataType {
		case IntCol, Int64Col, FloatCol:
			valueA := d.data[idx1].GetFloat(col)
			valueB := d.data[idx2].GetFloat(col)
			if valueA == valueB {
				continue
			}

			return valueA < valueB
		case StringCol:
			str1 := d.data[idx1].GetString(col)
			str2 := d.data[idx2].GetString(col)
			if str1 == str2 {
				continue
			}

			return str1 < str2
		default:
			panic(fmt.Sprintf("sorting not implemented for type %s", col.DataType))
		}
	}

	return true
}

// Swap replaces two data rows while sorting.
func (d *DataStore) Swap(i, j int) {
	d.data[i], d.data[j] = d.data[j], d.data[i]
}
