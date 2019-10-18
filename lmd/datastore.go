package main

import (
	"time"
)

// DataStore contains the actual data rows with a reference to the table and peer.
type DataStore struct {
	noCopy                  noCopy
	DynamicColumnCache      ColumnList            // contains list of columns used to run periodic update
	DynamicColumnNamesCache []string              // contains list of keys used to run periodic update
	DataSizes               map[DataType]int      // contains the sizes for each data type
	Peer                    *Peer                 // reference to our peer
	Data                    []*DataRow            // the actual data rows
	Index                   map[string]*DataRow   // access data rows from primary key, ex.: hostname or comment id
	Table                   *Table                // reference to table definition
	dupStringList           map[[32]byte][]string // lookup pointer to other stringlists during initialisation
}

// NewDataStore creates a new datastore with columns based on given flags
func NewDataStore(table *Table, peer interface{}) (d *DataStore) {
	d = &DataStore{
		Data:                    make([]*DataRow, 0),
		Index:                   make(map[string]*DataRow),
		DynamicColumnCache:      make(ColumnList, 0),
		DynamicColumnNamesCache: make([]string, 0),
		dupStringList:           make(map[[32]byte][]string),
		Table:                   table,
	}

	if peer != nil {
		d.Peer = peer.(*Peer)
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
	if len(d.Table.PrimaryKey) > 0 {
		d.Index = make(map[string]*DataRow, len(*data))
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

// AddItem adds an new DataRow to a DataStore.
func (d *DataStore) AddItem(row *DataRow) {
	d.Data = append(d.Data, row)
	if len(d.Table.PrimaryKey) > 0 {
		d.Index[row.GetID()] = row
	}
}

// RemoveItem removes a DataRow from a DataStore.
func (d *DataStore) RemoveItem(row *DataRow) {
	if len(d.Table.PrimaryKey) > 0 {
		delete(d.Index, row.GetID())
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
		columns = append(columns, col)
		keys = append(keys, col.Name)
	}
	return keys, &columns
}
