package lmd

import (
	"strings"
)

// ColumnList defines a set of Columns.
type ColumnList []*Column

// String returns the string representation of a column list.
func (cl *ColumnList) String() string {
	names := make([]string, 0, len(*cl))
	for i := range *cl {
		names = append(names, (*cl)[i].String())
	}

	return strings.Join(names, ", ")
}

func (cl *ColumnList) GetColumnIndex(name string) int {
	for i := range *cl {
		if (*cl)[i].String() == name {
			return i
		}
	}

	return -1
}
