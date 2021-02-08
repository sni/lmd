package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// DataStore contains the actual data rows with a reference to the table and peer.
type DataStore struct {
	noCopy                  noCopy
	DynamicColumnCache      ColumnList                     // contains list of columns used to run periodic update
	DynamicColumnNamesCache []string                       // contains list of keys used to run periodic update
	DataSizes               map[DataType]int               // contains the sizes for each data type
	Peer                    *Peer                          // reference to our peer
	PeerName                string                         // cached peer name
	PeerKey                 string                         // cached peer key
	DataSet                 *DataStoreSet                  // reference to parent DataSet
	Data                    []*DataRow                     // the actual data rows
	Index                   map[string]*DataRow            // access data rows from primary key, ex.: hostname or comment id
	Index2                  map[string]map[string]*DataRow // access data rows from 2 primary keys, ex.: host and service
	Table                   *Table                         // reference to table definition
	dupStringList           map[[32]byte][]string          // lookup pointer to other stringlists during initialization
	PeerLockMode            PeerLockMode                   // flag wether datarow have to set PeerLock when accessing status
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
	dataSizes := map[DataType]int{
		StringCol:     0,
		StringListCol: 0,
		IntCol:        0,
		Int64ListCol:  0,
		FloatCol:      0,
		CustomVarCol:  0,
	}
	for i := range table.Columns {
		col := table.Columns[i]
		if col.Optional != NoFlags && !d.Peer.HasFlag(col.Optional) {
			continue
		}
		if col.StorageType == LocalStore {
			dataSizes[col.DataType]++
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
	d.DataSizes = dataSizes
	// prepend primary keys to dynamic keys, since they are required to map the results back to specific items
	if len(d.DynamicColumnNamesCache) > 0 {
		d.DynamicColumnNamesCache = append(d.Table.PrimaryKey, d.DynamicColumnNamesCache...)
	}
	return
}

// InsertData adds a list of results and initializes the store table
func (d *DataStore) InsertData(data *ResultSet, columns *ColumnList) error {
	now := time.Now().Unix()
	switch len(d.Table.PrimaryKey) {
	case 0:
	case 1:
		d.Index = make(map[string]*DataRow, len(*data))
	case 2:
		d.Index2 = make(map[string]map[string]*DataRow)
	default:
		panic("not supported number of primary keys")
	}
	for i := range *data {
		row, err := NewDataRow(d, &(*data)[i], columns, now)
		if err != nil {
			log.Errorf("adding new %s failed: %s", d.Table.Name.String(), err.Error())
			return err
		}
		d.AddItem(row)
	}
	// only required during initial setup
	d.dupStringList = nil
	return nil
}

// AppendData append a list of results and initializes the store table
func (d *DataStore) AppendData(data *ResultSet, columns *ColumnList) error {
	d.DataSet.Lock.Lock()
	defer d.DataSet.Lock.Unlock()

	if d.Index == nil {
		// should not happen but might indicate a recent restart or backend issue
		return fmt.Errorf("index not ready, cannot append data")
	}
	for i := range *data {
		resRow := (*data)[i]
		row, nErr := NewDataRow(d, &resRow, columns, 0)
		if nErr != nil {
			return nErr
		}
		d.AddItem(row)
	}
	return nil
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

// GetColumn returns column by name
func (d *DataStore) GetColumn(name string) *Column {
	return d.Table.ColumnsIndex[name]
}

// GetInitialColumns returns list of columns required to fill initial dataset
func (d *DataStore) GetInitialColumns() ([]string, *ColumnList) {
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
	return keys, &columns
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

// GetPreFilteredData returns d.Data but try to return reduced dataset by using host / service index if table supports it
func (d *DataStore) GetPreFilteredData(filter *[]*Filter) []*DataRow {
	if len(*filter) == 0 {
		return d.Data
	}
	switch d.Table.Name {
	case TableHosts:
		hostlist := GetIndexableHostnames(filter, "name")
		if len(*hostlist) == 0 {
			return d.Data
		}
		indexedData := make([]*DataRow, 0)
		for _, name := range *hostlist {
			row, ok := d.Index[name]
			if ok {
				indexedData = append(indexedData, row)
			}
		}
		log.Tracef("using indexed %s dataset of size: %d", d.Table.Name.String(), len(indexedData))
		return indexedData
	case TableServices:
		hostlist := GetIndexableHostnames(filter, "host_name")
		if len(*hostlist) == 0 {
			return d.Data
		}
		indexedData := make([]*DataRow, 0)
		for _, name := range *hostlist {
			rows, ok := d.Index2[name]
			if ok {
				for _, row := range rows {
					indexedData = append(indexedData, row)
				}
			}
		}
		log.Tracef("using indexed %s dataset of size: %d", d.Table.Name.String(), len(indexedData))
		return indexedData
	}
	return d.Data
}

// GetIndexableHostnames returns list of hostname which can be used to reduce the initial dataset
func GetIndexableHostnames(filter *[]*Filter, column string) *[]string {
	hostlist := []string{}
	uniqHosts := make(map[string]bool)
	isUsable := func(f *Filter) bool {
		return f.Operator == Equal && f.Column.Name == column && !f.Negate
	}
	for _, f := range *filter {
		switch f.GroupOperator {
		case And:
			if f.Negate {
				return &hostlist
			}
			for _, f2 := range f.Filter {
				if isUsable(f2) {
					uniqHosts[f2.StrValue] = true
				}
			}
		case Or:
			if f.Negate {
				return &hostlist
			}
			for _, f2 := range f.Filter {
				if isUsable(f2) {
					uniqHosts[f2.StrValue] = true
				} else {
					// none-indexable or filter, must do full table scan
					return &hostlist
				}
			}
		default:
			// top lvl filter are combined by AND, so its safe to prefilter result data and skip the others
			if isUsable(f) {
				uniqHosts[f.StrValue] = true
			}
		}
	}

	// sort and return list of index names used
	for key := range uniqHosts {
		hostlist = append(hostlist, key)
	}
	sort.Strings(hostlist)
	return &hostlist
}
