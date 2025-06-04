package lmd

import (
	"fmt"
	"sort"
)

// ResultSet is a list of result rows.
type ResultSet [][]interface{}

// ResultSetStats contains a result from a stats query.
type ResultSetStats struct {
	Stats       map[string][]*Filter
	Total       int // total number of matched rows regardless of any limits or offsets
	RowsScanned int // total number of rows scanned to create result
}

func NewResultSetStats() *ResultSetStats {
	res := ResultSetStats{}
	res.Stats = make(map[string][]*Filter)

	return &res
}

// NewResultSet parses resultset from given bytes.
func NewResultSet(data []byte) (res ResultSet, err error) {
	res, remaining, err := parseJSONResult(data)

	if len(remaining) > 0 {
		return nil, fmt.Errorf("json parse error, stray data: %s", data)
	}

	return res, err
}

// sortByPrimaryKey sorts the resultset by their primary columns.
func (res *ResultSet) sortByPrimaryKey(table *Table, columns ColumnList) {
	if len(table.primaryKey) == 0 {
		return
	}
	sorted := ResultSetSorted{Data: *res}
	for _, name := range table.primaryKey {
		for x, col := range columns {
			if name == col.Name {
				sorted.Keys = append(sorted.Keys, x)
				sorted.Types = append(sorted.Types, col.DataType)
			}
		}
	}
	sort.Sort(&sorted)

	return
}

// result2Hash converts list result into hashes.
func (res *ResultSet) result2Hash(columns []string) []map[string]interface{} {
	hash := make([]map[string]interface{}, 0)
	for _, row := range *res {
		rowHash := make(map[string]interface{})
		for x, key := range columns {
			rowHash[key] = row[x]
		}
		hash = append(hash, rowHash)
	}

	return hash
}

// ResultSetSorted is a sorted list of result rows.
type ResultSetSorted struct {
	Data  ResultSet
	Keys  []int
	Types []DataType
}

// Len returns the result length used for sorting results.
func (res *ResultSetSorted) Len() int {
	return len(res.Data)
}

// Less returns the sort result of two data rows.
func (res *ResultSetSorted) Less(idx1, idx2 int) bool {
	for x := range res.Keys {
		dataIndex := res.Keys[x]
		sortType := res.Types[x]
		switch sortType {
		case IntCol, Int64Col, FloatCol:
			valueA := interface2float64(res.Data[idx1][dataIndex])
			valueB := interface2float64(res.Data[idx2][dataIndex])
			if valueA == valueB {
				continue
			}

			return valueA < valueB
		case StringCol:
			str1 := interface2stringNoDedup(res.Data[idx1][dataIndex])
			str2 := interface2stringNoDedup(res.Data[idx2][dataIndex])
			if str1 == str2 {
				continue
			}

			return str1 < str2
		default:
			panic(fmt.Sprintf("sorting not implemented for type %s", sortType))
		}
	}

	return true
}

// Swap replaces two data rows while sorting.
func (res *ResultSetSorted) Swap(i, j int) {
	res.Data[i], res.Data[j] = res.Data[j], res.Data[i]
}

// ResultPrepared is a list of result rows prepared to insert faster.
type ResultPrepared struct {
	DataRow    *DataRow
	ResultRow  []interface{}
	FullUpdate bool
}
