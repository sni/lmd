package lmd

import (
	"fmt"
	"sort"
	"strings"
)

// DataStore contains the actual data rows with a reference to the table and peer.
type DataStore struct {
	noCopy                  noCopy
	Index                   map[string]*DataRow            // access data rows from primary key, ex.: hostname or comment id
	Index2                  map[string]map[string]*DataRow // access data rows from 2 primary keys, ex.: host and service
	IndexLowerCase          map[string][]string            // access data rows from lower case primary key
	Peer                    *Peer                          // reference to our peer
	LowerCaseColumns        map[int]int                    // list of string column indexes with their corresponding lower case index
	dupStringList           map[uint32][]string            // lookup pointer to other stringlists during initialization
	Table                   *Table                         // reference to table definition
	DataSet                 *DataStoreSet                  // reference to parent DataSet
	DynamicColumnCache      ColumnList                     // contains list of keys used to run periodic update
	Data                    []*DataRow                     // the actual data rows
	Columns                 ColumnList                     // reference to the used columns
	DynamicColumnNamesCache []string                       // contains list of keys used to run periodic update
}

// NewDataStore creates a new datastore with columns based on given flags.
func NewDataStore(table *Table, peer *Peer) (d *DataStore) {
	d = &DataStore{
		Data:                    make([]*DataRow, 0),
		Index:                   make(map[string]*DataRow),
		Index2:                  make(map[string]map[string]*DataRow),
		IndexLowerCase:          make(map[string][]string),
		DynamicColumnCache:      make(ColumnList, 0),
		DynamicColumnNamesCache: make([]string, 0),
		dupStringList:           make(map[uint32][]string),
		Table:                   table,
		Peer:                    peer,
		LowerCaseColumns:        make(map[int]int),
	}

	// create columns list
	table.Lock.RLock()
	writeLocked := false
	dataSizes := table.DataSizes

	for i := range table.Columns {
		col := table.Columns[i]
		if col.Optional != NoFlags && !d.Peer.HasFlag(col.Optional) {
			continue
		}
		if col.StorageType != LocalStore {
			continue
		}
		if col.Index == -1 {
			// require write lock and update table column
			if !writeLocked {
				table.Lock.RUnlock()
				table.Lock.Lock()
				writeLocked = true
			}
			col.Index = dataSizes[col.DataType]
			dataSizes[col.DataType]++
		}
		if col.FetchType == Dynamic {
			d.DynamicColumnCache = append(d.DynamicColumnCache, col)
		}
		if strings.HasSuffix(col.Name, "_lc") {
			refCol := table.GetColumn(strings.TrimSuffix(col.Name, "_lc"))
			d.LowerCaseColumns[refCol.Index] = col.Index
		}
	}

	if writeLocked {
		table.Lock.Unlock()
	} else {
		table.Lock.RUnlock()
	}

	// prepend primary keys to dynamic keys, since they are required to map the results back to specific items
	if len(d.DynamicColumnCache) > 0 {
		d.DynamicColumnNamesCache = append(d.DynamicColumnNamesCache, table.PrimaryKey...)
		for _, col := range d.DynamicColumnCache {
			d.DynamicColumnNamesCache = append(d.DynamicColumnNamesCache, col.Name)
		}
	}

	return d
}

// InsertData adds a list of results and initializes the store table.
func (d *DataStore) InsertData(rows ResultSet, columns ColumnList, setReferences bool) error {
	now := currentUnixTime()
	switch len(d.Table.PrimaryKey) {
	case 0:
	case 1:
		d.Index = make(map[string]*DataRow, len(rows))
	case 2:
		d.Index2 = make(map[string]map[string]*DataRow)
	default:
		panic("not supported number of primary keys")
	}

	d.Data = make([]*DataRow, len(rows))
	for idx, raw := range rows {
		row, err := NewDataRow(d, raw, columns, now, setReferences)
		if err != nil {
			log.Errorf("adding new %s failed: %s", d.Table.Name.String(), err.Error())

			return err
		}
		d.InsertItem(idx, row)
	}
	// only required during initial setup
	d.dupStringList = nil

	return nil
}

// AppendData append a list of results and initializes the store table.
func (d *DataStore) AppendData(data ResultSet, columns ColumnList) error {
	d.DataSet.lock.Lock()
	defer d.DataSet.lock.Unlock()
	d.Table.Lock.RLock()
	defer d.Table.Lock.RUnlock()
	if d.Index == nil {
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

// InsertItem adds an new DataRow to a DataStore at given Index.
func (d *DataStore) InsertItem(index int, row *DataRow) {
	d.Data[index] = row
	switch len(d.Table.PrimaryKey) {
	case 0:
	case 1:
		id := dedup.S(row.GetID())
		d.Index[id] = row
		if d.Table.Name == TableHosts {
			idLower := dedup.S(strings.ToLower(id))
			if idLower != id {
				d.IndexLowerCase[idLower] = append(d.IndexLowerCase[idLower], id)
			}
		}
	case 2:
		id1, id2 := row.GetID2()
		id1 = dedup.S(id1)
		id2 = dedup.S(id2)
		if _, ok := d.Index2[id1]; !ok {
			d.Index2[id1] = make(map[string]*DataRow)
		}
		d.Index2[id1][id2] = row
	default:
		panic("not supported number of primary keys")
	}
}

// AddItem adds an new DataRow to a DataStore.
func (d *DataStore) AddItem(row *DataRow) {
	d.Data = append(d.Data, row)
	switch len(d.Table.PrimaryKey) {
	case 0:
	case 1:
		id := dedup.S(row.GetID())
		d.Index[id] = row
		if d.Table.Name == TableHosts {
			idLower := dedup.S(strings.ToLower(id))
			if idLower != id {
				d.IndexLowerCase[idLower] = append(d.IndexLowerCase[idLower], id)
			}
		}
	case 2:
		id1, id2 := row.GetID2()
		id1 = dedup.S(id1)
		id2 = dedup.S(id2)
		if _, ok := d.Index2[id1]; !ok {
			d.Index2[id1] = make(map[string]*DataRow)
		}
		d.Index2[id1][id2] = row
	default:
		panic("not supported number of primary keys")
	}
}

// RemoveItem removes a DataRow from a DataStore.
func (d *DataStore) RemoveItem(row *DataRow) {
	switch len(d.Table.PrimaryKey) {
	case 0:
	case 1:
		delete(d.Index, row.GetID())
		if d.Table.Name == TableHosts {
			panic("removing from hosts index is not supported")
		}
	default:
		panic("not supported number of primary keys")
	}
	for i := range d.Data {
		if d.Data[i] == row {
			d.Data = append(d.Data[:i], d.Data[i+1:]...)

			return
		}
	}
	log.Panicf("element not found")
}

// SetReferences creates reference entries for this tables.
func (d *DataStore) SetReferences() (err error) {
	for _, row := range d.Data {
		err = row.SetReferences()
		if err != nil {
			logWith(d).Debugf("setting references on table %s failed: %s", d.Table.Name.String(), err.Error())

			return
		}
	}

	return
}

// GetColumn returns column by name.
func (d *DataStore) GetColumn(name string) *Column {
	return d.Table.ColumnsIndex[name]
}

// GetInitialColumns returns list of columns required to fill initial dataset.
func (d *DataStore) GetInitialColumns() ([]string, ColumnList) {
	columns := make(ColumnList, 0)
	keys := make([]string, 0)
	for i := range d.Table.Columns {
		col := d.Table.Columns[i]
		if d.Peer != nil && !d.Peer.HasFlag(col.Optional) {
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
		obj, ok := d.Index2[parts[0]][parts[1]]

		return obj, ok
	}

	obj, ok := d.Index[req.WaitObject]

	return obj, ok
}

type getPreFilteredDataFilter func(map[string]bool, *Filter) bool

// GetPreFilteredData returns d.Data but try to return reduced dataset by using host / service index if table supports it.
func (d *DataStore) GetPreFilteredData(filter []*Filter) []*DataRow {
	if len(filter) == 0 {
		return d.Data
	}

	switch {
	case d.Table.Name == TableHosts:
		return (d.tryFilterIndexData(filter, d.appendIndexHostsFromHostColumns))
	case d.Table.Name == TableServices:
		return (d.tryFilterIndexData(filter, d.appendIndexHostsFromServiceColumns))
	case len(d.Table.PrimaryKey) == 1:
		return (d.tryFilterIndexData(filter, d.appendIndexFromPrimaryKey))
	default:
		// only hosts and services are supported
	}

	return d.Data
}

func (d *DataStore) tryFilterIndexData(filter []*Filter, fn getPreFilteredDataFilter) []*DataRow {
	uniqRows := make(map[string]bool)
	ok := d.TryFilterIndex(uniqRows, filter, fn, false)
	if !ok {
		return d.Data
	}
	// sort and return list of index names used
	primaryKeyList := []string{}
	for key := range uniqRows {
		primaryKeyList = append(primaryKeyList, key)
	}
	sort.Strings(primaryKeyList)
	indexedData := make([]*DataRow, 0)
	switch d.Table.Name {
	case TableServices:
		for _, name := range primaryKeyList {
			services, ok := d.Index2[name]
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
			row, ok := d.Index[name]
			if ok {
				indexedData = append(indexedData, row)
			}
		}
	}
	log.Tracef("using indexed %s dataset of size: %d", d.Table.Name.String(), len(indexedData))

	return indexedData
}

func (d *DataStore) prepareDataUpdateSet(dataOffset int, res ResultSet, columns ColumnList) (updateSet []*ResultPrepared, err error) {
	updateSet = make([]*ResultPrepared, 0, len(res))

	// compare last check date and only update large strings if the last check date has changed
	lastCheckDataIdx, lastCheckResIdx := d.getUpdateColumn("last_check", dataOffset)
	lastUpdateDataIdx, lastUpdateResIdx := d.getUpdateColumn("last_update", dataOffset)

	useIndex := dataOffset == 0 || len(res) == len(d.Data)

	// prepare update
	nameIndex := d.Index
	nameIndex2 := d.Index2
	for rowNum := range res {
		resRow := res[rowNum]
		prep := &ResultPrepared{
			ResultRow:  resRow,
			FullUpdate: false,
		}
		if useIndex {
			prep.DataRow = d.Data[rowNum]
		} else {
			switch d.Table.Name {
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
				log.Panicf("table not supported: %s", d.Table.Name.String())
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
				res[rowNum][i+dataOffset] = cast2Type(res[rowNum][i+dataOffset], col)
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
	if columnName == "last_update" && !d.Peer.HasFlag(HasLastUpdateColumn) {
		return
	}

	dataIndex = checkCol.Index
	resIndex = d.DynamicColumnCache.GetColumnIndex(columnName) + dataOffset
	if checkCol.DataType != Int64Col {
		log.Panicf("%s: assumption about column type for %s is wrong, expected %s and got %s",
			d.Table.Name.String(), checkCol.Name, Int64Col.String(), checkCol.DataType.String())
	}

	return
}

// TryFilterIndex returns list of hostname which can be used to reduce the initial dataset.
func (d *DataStore) TryFilterIndex(uniqHosts map[string]bool, filter []*Filter, filterCb getPreFilteredDataFilter, breakOnNoneIndexableFilter bool) bool {
	filterFound := 0
	for _, fil := range filter {
		if fil.Negate {
			// not supported
			return false
		}

		switch fil.GroupOperator {
		case And:
			ok := d.TryFilterIndex(uniqHosts, fil.Filter, filterCb, false)
			if !ok {
				return false
			}
			filterFound++
		case Or:
			ok := d.TryFilterIndex(uniqHosts, fil.Filter, filterCb, true)
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
	switch fil.Column.Name {
	case "name":
		switch fil.Operator {
		// name == <value>
		case Equal:
			uniqHosts[fil.StrValue] = true

			return true

		// name =~ <value>
		case EqualNocase:
			uniqHosts[fil.StrValue] = true
			for _, key := range d.IndexLowerCase[strings.ToLower(fil.StrValue)] {
				uniqHosts[key] = true
			}

			return true
		default:
			// other operators are not supported
		}
	case "name_lc":
		switch fil.Operator {
		// name == <value>
		case Equal, EqualNocase:
			uniqHosts[fil.StrValue] = true
			for _, key := range d.IndexLowerCase[strings.ToLower(fil.StrValue)] {
				uniqHosts[key] = true
			}

			return true
		default:
			// other operators are not supported
		}
	case "groups":
		// get hosts from host groups members
		switch fil.Operator {
		// groups >= <value>
		case GreaterThan:
			store := d.DataSet.tables[TableHostgroups]
			group, ok := store.Index[fil.StrValue]
			if ok {
				members := group.GetStringListByName("members")
				for _, m := range members {
					uniqHosts[m] = true
				}
			}

			return true
		case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
			store := d.DataSet.tables[TableHostgroups]
			for groupname, group := range store.Index {
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
	switch fil.Column.Name {
	case "host_name":
		switch fil.Operator {
		// host_name == <value>
		case Equal:
			uniqHosts[fil.StrValue] = true

			return true
		// host_name ~ <value>
		case RegexMatch, Contains:
			store := d.DataSet.tables[TableHosts]
			for hostname := range store.Index {
				if fil.MatchString(hostname) {
					uniqHosts[hostname] = true
				}
			}

			return true
		default:
			// other operators are not supported
		}
	case "host_name_lc":
		switch fil.Operator {
		// host_name ~~ <value>
		case RegexMatch, Contains, RegexNoCaseMatch, ContainsNoCase, EqualNocase, Equal:
			store := d.DataSet.tables[TableHosts]
			for hostname := range store.Index {
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
		switch fil.Operator {
		// groups >= <value>
		case GreaterThan:
			store := d.DataSet.tables[TableHostgroups]
			group, ok := store.Index[fil.StrValue]
			if ok {
				members := group.GetStringListByName("members")
				for _, m := range members {
					uniqHosts[m] = true
				}
			}

			return true
		case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
			store := d.DataSet.tables[TableHostgroups]
			for groupname, group := range store.Index {
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
		switch fil.Operator {
		// groups >= <value>
		case GreaterThan:
			store := d.DataSet.tables[TableServicegroups]
			group, ok := store.Index[fil.StrValue]
			if ok {
				members := group.GetServiceMemberListByName("members")
				for i := range members {
					uniqHosts[members[i][0]] = true
				}
			}

			return true
		case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase:
			store := d.DataSet.tables[TableServicegroups]
			for groupname, group := range store.Index {
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
	key := d.Table.PrimaryKey[0]

	switch fil.Column.Name {
	case key:
		switch fil.Operator {
		// name == <value>
		case Equal:
			uniqRows[fil.StrValue] = true

			return true

		// name =~ <value>
		case EqualNocase:
			uniqRows[fil.StrValue] = true
			for _, key := range d.IndexLowerCase[strings.ToLower(fil.StrValue)] {
				uniqRows[key] = true
			}

			return true
		default:
			// other operators are not supported
		}
	case key + "_lc":
		switch fil.Operator {
		// name == <value>
		case Equal, EqualNocase:
			uniqRows[fil.StrValue] = true
			for _, key := range d.IndexLowerCase[strings.ToLower(fil.StrValue)] {
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
