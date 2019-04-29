package main

import (
	"fmt"
	"time"
)

// DataStore contains the actual data with a reference to the table and peer.
type DataStore struct {
	noCopy     noCopy
	Peer       *Peer
	Table      *Table
	Data       []*DataRow
	Index      map[string]*DataRow // access data rows from primary key, ex.: hostname or comment id
	ColumnsMap map[int]int         // maps table column indexes with data subindexes
}

// InsertData adds a list of results and initializes the store table
func (d *DataStore) InsertData(data *[][]interface{}) error {
	now := time.Now().Unix()
	if len(d.Table.PrimaryKey) > 0 {
		d.Index = make(map[string]*DataRow, len(*data))
	}
	for i := range *data {
		row, err := NewDataRow(d, &(*data)[i], now)
		if err != nil {
			return err
		}
		d.AddItem(row)
	}
	return nil
}

// AddItem adds an new DataRow to a DataStore.
func (d *DataStore) AddItem(row *DataRow) {
	d.Data = append(d.Data, row)
	if row.ID != "" {
		d.Index[row.ID] = row
	}
}

// RemoveItem removes a DataRow from a DataStore.
func (d *DataStore) RemoveItem(row *DataRow) {
	if row.ID != "" {
		delete(d.Index, row.ID)
	}
	for i := range d.Data {
		r := d.Data[i]
		if fmt.Sprintf("%p", r.RawData) == fmt.Sprintf("%p", row.RawData) {
			d.Data = append(d.Data[:i], d.Data[i+1:]...)
			return
		}
	}
	log.Panicf("element not found")
}
