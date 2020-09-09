package main

import (
	"fmt"
	"sort"
)

// ResultSet is a list of result rows
type ResultSet [][]interface{}

// ResultSetStats contains a result from a stats query
type ResultSetStats map[string][]*Filter

// Precompress compresses large strings in result set to allow faster updates (compressing would happen during locked update loop otherwise)
func (res *ResultSet) Precompress(offset int, columns *ColumnList) {
	for i := range *columns {
		col := (*columns)[i]
		if col.DataType == StringLargeCol {
			replaceIndex := i + offset
			for j := range *res {
				(*res)[j][replaceIndex] = interface2stringlarge((*res)[j][replaceIndex])
			}
		}
	}
}

// Precompress compresses large strings in result set to allow faster updates (compressing would happen during locked update loop otherwise)
func (res *ResultSet) SortByPrimaryKey(table *Table, req *Request) *ResultSet {
	if len(table.PrimaryKey) == 0 {
		return res
	}
	sorted := &ResultSetSorted{Data: res}
	for _, name := range table.PrimaryKey {
		for x, col := range req.Columns {
			if name == col {
				sorted.Keys = append(sorted.Keys, x)
				sorted.Types = append(sorted.Types, table.GetColumn(col).DataType)
			}
		}
	}
	sort.Sort(sorted)
	return sorted.Data
}

// Result2Hash converts list result into hashes
func (res *ResultSet) Result2Hash(columns []string) []map[string]interface{} {
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

// ResultSetSorted is a sorted list of result rows
type ResultSetSorted struct {
	Data  *ResultSet
	Keys  []int
	Types []DataType
}

// Len returns the result length used for sorting results.
func (res *ResultSetSorted) Len() int {
	return len(*res.Data)
}

// Less returns the sort result of two data rows
func (res *ResultSetSorted) Less(i, j int) bool {
	for x := range res.Keys {
		dataIndex := res.Keys[x]
		sortType := res.Types[x]
		switch sortType {
		case IntCol:
			fallthrough
		case Int64Col:
			fallthrough
		case FloatCol:
			valueA := interface2float64((*res.Data)[i][dataIndex])
			valueB := interface2float64((*res.Data)[j][dataIndex])
			if valueA == valueB {
				continue
			}
			return valueA < valueB
		case StringCol:
			s1 := *interface2stringNoDedup((*res.Data)[i][dataIndex])
			s2 := *interface2stringNoDedup((*res.Data)[j][dataIndex])
			if s1 == s2 {
				continue
			}
			return s1 < s2
		}
		panic(fmt.Sprintf("sorting not implemented for type %s", sortType))
	}
	return true
}

// Swap replaces two data rows while sorting.
func (res *ResultSetSorted) Swap(i, j int) {
	(*res.Data)[i], (*res.Data)[j] = (*res.Data)[j], (*res.Data)[i]
}

// ResultPreparedSet is a list of result rows prepared to insert faster
type ResultPrepared struct {
	ResultRow  *[]interface{}
	DataRow    *DataRow
	FullUpdate bool
}
