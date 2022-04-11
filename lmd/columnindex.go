package main

// ColumnIndex defines a combination of a column with an index
type ColumnIndex struct {
	Column *Column
	Index  int
}

func NewColumnIndex(col *Column, ds *DataStore) ColumnIndex {
	return (ColumnIndex{
		Column: col,
		Index:  ds.ColumnsIndex[col],
	})
}
