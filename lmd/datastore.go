package main

import (
	"fmt"
	"sort"
	"strings"
)

// DataStore contains the actual data rows with a reference to the table and peer.
type DataStore struct {
	noCopy                  noCopy
	DynamicColumnCache      ColumnList                     // contains list of columns used to run periodic update
	DynamicColumnNamesCache []string                       // contains list of keys used to run periodic update
	Peer                    *Peer                          // reference to our peer
	PeerName                string                         // cached peer name
	PeerKey                 string                         // cached peer key
	PeerLockMode            PeerLockMode                   // flag wether datarow have to set PeerLock when accessing status
	DataSet                 *DataStoreSet                  // reference to parent DataSet
	Data                    []*DataRow                     // the actual data rows
	Index                   map[string]*DataRow            // access data rows from primary key, ex.: hostname or comment id
	Index2                  map[string]map[string]*DataRow // access data rows from 2 primary keys, ex.: host and service
	Table                   *Table                         // reference to table definition
	Columns                 ColumnList                     // reference to the used columns
	dupStringList           map[[32]byte][]string          // lookup pointer to other stringlists during initialization
	LowerCaseColumns        map[int]int                    // list of string column indexes with their coresponding lower case index
}

// NewDataStore creates a new datastore with columns based on given flags
func NewDataStore(table *Table, peer interface{}) (d *DataStore) {
	d = &DataStore{
		Data:                    make([]*DataRow, 0),
		Index:                   make(map[string]*DataRow),
		Index2:                  make(map[string]map[string]*DataRow),
		DynamicColumnCache:      make(ColumnList, 0),
		DynamicColumnNamesCache: make([]string, 0),
		dupStringList:           make(map[[32]byte][]string),
		Table:                   table,
		PeerLockMode:            table.PeerLockMode,
		LowerCaseColumns:        make(map[int]int),
	}

	if peer != nil {
		d.Peer = peer.(*Peer)
		d.Peer.Lock.RLock()
		d.PeerName = d.Peer.Name
		d.PeerKey = d.Peer.ID
		d.Peer.Lock.RUnlock()
	}

	// create columns list
	table.Lock.RLock()
	defer table.Lock.RUnlock()
	dataSizes := table.DataSizes

	for i := range table.Columns {
		col := table.Columns[i]
		if col.Optional != NoFlags && !d.Peer.HasFlag(col.Optional) {
			continue
		}
		if col.StorageType == LocalStore {
			if col.Index == -1 {
				// require write lock and update table column
				table.Lock.RUnlock()
				table.Lock.Lock()
				col.Index = dataSizes[col.DataType]
				dataSizes[col.DataType]++
				table.Lock.Unlock()
				table.Lock.RLock()
			}
			if col.FetchType == Dynamic {
				d.DynamicColumnNamesCache = append(d.DynamicColumnNamesCache, col.Name)
				d.DynamicColumnCache = append(d.DynamicColumnCache, col)
			}
			if strings.HasSuffix(col.Name, "_lc") {
				refCol := table.GetColumn(strings.TrimSuffix(col.Name, "_lc"))
				d.LowerCaseColumns[refCol.Index] = col.Index
			}
		}
	}

	// prepend primary keys to dynamic keys, since they are required to map the results back to specific items
	if len(d.DynamicColumnNamesCache) > 0 {
		d.DynamicColumnNamesCache = append(table.PrimaryKey, d.DynamicColumnNamesCache...)
	}
	return
}

// InsertData adds a list of results and initializes the store table
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
	d.Table.Lock.RLock()
	defer d.Table.Lock.RUnlock()
	d.Data = make([]*DataRow, len(rows))
	for i, data := range rows {
		row, err := NewDataRow(d, data, columns, now, setReferences)
		if err != nil {
			log.Errorf("adding new %s failed: %s", d.Table.Name.String(), err.Error())
			return err
		}
		d.InsertItem(i, row)
	}
	// only required during initial setup
	d.dupStringList = nil
	return nil
}

// AppendData append a list of results and initializes the store table
func (d *DataStore) AppendData(data ResultSet, columns ColumnList) error {
	d.DataSet.Lock.Lock()
	defer d.DataSet.Lock.Unlock()
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
		d.Index[row.GetID()] = row
	case 2:
		id1, id2 := row.GetID2()
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
		d.Index[row.GetID()] = row
	case 2:
		id1, id2 := row.GetID2()
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

// SetReferences creates reference entries for this tables
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

// GetColumn returns column by name
func (d *DataStore) GetColumn(name string) *Column {
	return d.Table.ColumnsIndex[name]
}

// GetInitialColumns returns list of columns required to fill initial dataset
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

type getPreFilteredDataFilter func(*DataStore, map[string]bool, *Filter) bool

// GetPreFilteredData returns d.Data but try to return reduced dataset by using host / service index if table supports it
func (d *DataStore) GetPreFilteredData(filter []*Filter) []*DataRow {
	if len(filter) == 0 {
		return d.Data
	}
	switch d.Table.Name {
	case TableHosts:
		return (d.tryFilterIndexData(filter, appendIndexHostsFromHostColumns))
	case TableServices:
		return (d.tryFilterIndexData(filter, appendIndexHostsFromServiceColumns))
	}
	return d.Data
}

func (d *DataStore) tryFilterIndexData(filter []*Filter, fn getPreFilteredDataFilter) []*DataRow {
	uniqHosts := make(map[string]bool)
	ok := d.TryFilterIndex(uniqHosts, filter, fn, false)
	if !ok {
		return d.Data
	}
	// sort and return list of index names used
	hostlist := []string{}
	for key := range uniqHosts {
		hostlist = append(hostlist, key)
	}
	sort.Strings(hostlist)
	if len(hostlist) == 0 {
		return d.Data
	}
	indexedData := make([]*DataRow, 0)
	switch d.Table.Name {
	case TableHosts:
		for _, name := range hostlist {
			row, ok := d.Index[name]
			if ok {
				indexedData = append(indexedData, row)
			}
		}
	case TableServices:
		for _, name := range hostlist {
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
	}
	log.Tracef("using indexed %s dataset of size: %d", d.Table.Name.String(), len(indexedData))
	return indexedData
}

// TryFilterIndex returns list of hostname which can be used to reduce the initial dataset
func (d *DataStore) TryFilterIndex(uniqHosts map[string]bool, filter []*Filter, fn getPreFilteredDataFilter, breakOnNoneIndexableFilter bool) bool {
	filterFound := 0
	for _, f := range filter {
		switch f.GroupOperator {
		case And:
			if f.Negate {
				// not supported
				return false
			}
			ok := d.TryFilterIndex(uniqHosts, f.Filter, fn, false)
			if !ok {
				return false
			}
			filterFound++
		case Or:
			if f.Negate {
				// not supported
				return false
			}
			ok := d.TryFilterIndex(uniqHosts, f.Filter, fn, true)
			if !ok {
				return false
			}
			filterFound++
		default:
			if f.Negate {
				// not supported
				return false
			}

			if fn(d, uniqHosts, f) {
				filterFound++
			} else if breakOnNoneIndexableFilter {
				return false
			}
		}
	}
	// index can only be used if there is at least one useable filter found
	return filterFound > 0
}

func appendIndexHostsFromHostColumns(d *DataStore, uniqHosts map[string]bool, f *Filter) bool {
	// trim lower case columns prefix, they are used internally only
	colName := strings.TrimSuffix(f.Column.Name, "_lc")
	switch colName {
	case "name":
		// name == <value>
		if f.Operator == Equal {
			uniqHosts[f.StrValue] = true
			return true
		}
	case "groups":
		// get hosts from host groups members
		switch f.Operator {
		// groups >= <value>
		case GreaterThan:
			store := d.DataSet.tables[TableHostgroups]
			group, ok := store.Index[f.StrValue]
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
				if f.MatchString(groupname) {
					members := group.GetStringListByName("members")
					for _, m := range members {
						uniqHosts[m] = true
					}
				}
			}
			return true
		}
	}
	return false
}

func appendIndexHostsFromServiceColumns(d *DataStore, uniqHosts map[string]bool, f *Filter) bool {
	// trim lower case columns prefix, they are used internally only
	colName := strings.TrimSuffix(f.Column.Name, "_lc")
	switch colName {
	case "host_name":
		switch f.Operator {
		// host_name == <value>
		case Equal:
			uniqHosts[f.StrValue] = true
			return true
		// host_name ~~ <value>
		case RegexMatch, RegexNoCaseMatch, Contains, ContainsNoCase, EqualNocase:
			store := d.DataSet.tables[TableHosts]
			for hostname := range store.Index {
				if f.MatchString(hostname) {
					uniqHosts[hostname] = true
				}
			}
			return true
		}
	case "host_groups":
		// get hosts from host groups members
		switch f.Operator {
		// groups >= <value>
		case GreaterThan:
			store := d.DataSet.tables[TableHostgroups]
			group, ok := store.Index[f.StrValue]
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
				if f.MatchString(groupname) {
					members := group.GetStringListByName("members")
					for _, m := range members {
						uniqHosts[m] = true
					}
				}
			}
			return true
		}
	case "groups":
		// get hosts from services groups members
		switch f.Operator {
		// groups >= <value>
		case GreaterThan:
			store := d.DataSet.tables[TableServicegroups]
			group, ok := store.Index[f.StrValue]
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
				if f.MatchString(groupname) {
					members := group.GetServiceMemberListByName("members")
					for i := range members {
						uniqHosts[members[i][0]] = true
					}
				}
			}
			return true
		}
	}
	return false
}
