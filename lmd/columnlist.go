package main

import (
	"strings"
)

// ColumnIndex defines a combination of a column with an index
type ColumnIndex struct {
	Column *Column
	Index  int
}

// ColumnIndexedList defines a set of Columns with its cached datastore index
type ColumnIndexedList []ColumnIndex

// ColumnList defines a set of Columns
type ColumnList []*Column

// String returns the string representation of a column list
func (cl *ColumnList) String() string {
	names := make([]string, 0, len(*cl))
	for i := range *cl {
		names = append(names, (*cl)[i].String())
	}
	return strings.Join(names, ", ")
}

func (cil *ColumnIndexedList) GetColumnIndex(name string) int {
	for i := range *cil {
		if (*cil)[i].Column.String() == name {
			return i
		}
	}
	return -1
}
