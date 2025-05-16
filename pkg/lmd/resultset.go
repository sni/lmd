package lmd

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/a8m/djson"
	"github.com/buger/jsonparser"
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
	res = make(ResultSet, 0)
	rowNum := 0
	offset, jErr := jsonparser.ArrayEach(data, func(rowBytes []byte, _ jsonparser.ValueType, offset int, aErr error) {
		rowNum++
		if aErr != nil {
			err = fmt.Errorf("json parse error in row: %d at offset %d: %s", rowNum, offset, aErr.Error())

			return
		}
		row, dErr := djson.DecodeArray(rowBytes)
		if dErr != nil {
			row, dErr = tryRecoverJSON(rowBytes, dErr)
			// still failing
			if dErr != nil {
				syntaxErr := &djson.SyntaxError{}
				if errors.As(dErr, &syntaxErr) {
					err = fmt.Errorf("json parse error in row %d at offset %d: %s", rowNum, syntaxErr.Offset, dErr.Error())
				} else {
					err = fmt.Errorf("json parse error in row %d at offset %d: %s", rowNum, offset, dErr.Error())
				}

				return
			}
		}
		res = append(res, row)
	})
	// trailing comma error will be ignored
	if jErr != nil && offset < len(data)-3 {
		return nil, fmt.Errorf("json parse error: %w", jErr)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

func tryRecoverJSON(rowBytes []byte, dErr error) (row []interface{}, err error) {
	// try to fix invalid escape sequences and unknown utf8 characters
	if strings.Contains(dErr.Error(), "invalid character") {
		rowBytes = bytesToValidUTF8(rowBytes, []byte("\uFFFD"))
		row, dErr = djson.DecodeArray(rowBytes)
	}
	if dErr == nil {
		return row, nil
	}

	// try to fix invalid nan for numbers
	if strings.Contains(dErr.Error(), "invalid character 'a' in literal null") {
		rowBytes = bytes.ReplaceAll(rowBytes, []byte("nan,"), []byte("null,"))
		rowBytes = bytes.ReplaceAll(rowBytes, []byte("nan]"), []byte("null]"))
		row, dErr = djson.DecodeArray(rowBytes)
	}
	if dErr == nil {
		return row, nil
	}

	return row, dErr //nolint:wrapcheck // the djson error is required
}

// Precompress compresses large strings in result set to allow faster updates (compressing would happen during locked update loop otherwise).
func (res *ResultSet) Precompress(offset int, columns ColumnList) {
	for i := range columns {
		col := columns[i]
		if col.DataType == StringLargeCol {
			replaceIndex := i + offset
			for _, row := range *res {
				row[replaceIndex] = interface2stringlarge(row[replaceIndex])
			}
		}
	}
}

// SortByPrimaryKey sorts the resultset by their primary columns.
func (res *ResultSet) SortByPrimaryKey(table *Table, req *Request) ResultSet {
	if len(table.primaryKey) == 0 {
		return *res
	}
	sorted := ResultSetSorted{Data: *res}
	for _, name := range table.primaryKey {
		for x, col := range req.Columns {
			if name == col {
				sorted.Keys = append(sorted.Keys, x)
				sorted.Types = append(sorted.Types, table.GetColumn(col).DataType)
			}
		}
	}
	sort.Sort(&sorted)

	return sorted.Data
}

// Result2Hash converts list result into hashes.
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
