package main

// ColumnIndexedList defines a set of Columns with its cached datastore index
type ColumnIndexedList []ColumnIndex

func (cil *ColumnIndexedList) GetColumnIndex(name string) int {
	for i := range *cil {
		if (*cil)[i].Column.String() == name {
			return i
		}
	}
	return -1
}
