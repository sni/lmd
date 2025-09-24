package lmd

import (
	"fmt"
	"sort"
	"time"
)

// RawResultSet contains references to the result rows or stats objects.
type RawResultSet struct {
	noCopy      noCopy
	DataResult  []*DataRow     // references to the data rows required for the result
	StatsResult ResultSetStats // intermediate result of stats query
	Sort        []*SortField   // columns required for sorting
	Total       int            // total number of results for this set
	RowsScanned int            // total number of scanned rows for this set
}

// PostProcessing does all the post processing required for a request like sorting
// and cutting of limits, applying offsets.
func (raw *RawResultSet) PostProcessing(res *Response) {
	if len(res.request.Stats) > 0 {
		return
	}
	log.Tracef("PostProcessing")

	// offset outside
	if res.request.Offset > raw.Total {
		raw.DataResult = make([]*DataRow, 0)

		return
	}

	// sort our result
	if len(res.request.Sort) > 0 {
		// skip sorting if there is only one backend requested and we want the default sort order
		if len(res.request.BackendsMap) >= 1 || !res.request.IsDefaultSortOrder() {
			t1 := time.Now()
			sort.Sort(raw)
			duration := time.Since(t1)
			log.Debugf("sorting result took %s", duration.String())
		}
	}

	// apply request offset
	if res.request.Offset > 0 {
		raw.DataResult = raw.DataResult[res.request.Offset:]
	}

	// apply request limit
	if res.request.Limit != nil && *res.request.Limit >= 0 && *res.request.Limit < len(raw.DataResult) {
		raw.DataResult = raw.DataResult[0:*res.request.Limit]
	}
}

// Len returns the result length used for sorting results.
func (raw *RawResultSet) Len() int {
	return len(raw.DataResult)
}

// Less returns the sort result of two data rows.
func (raw *RawResultSet) Less(idx1, idx2 int) bool {
	for _, field := range raw.Sort {
		switch field.Column.DataType {
		case IntCol, Int64Col, FloatCol:
			valueA := raw.DataResult[idx1].GetFloat(field.Column)
			valueB := raw.DataResult[idx2].GetFloat(field.Column)
			if valueA == valueB {
				continue
			}
			if field.Direction == Asc {
				return valueA < valueB
			}

			return valueA > valueB
		case StringCol, StringLargeCol, StringListCol, ServiceMemberListCol, InterfaceListCol, JSONCol:
			str1 := raw.DataResult[idx1].GetString(field.Column)
			str2 := raw.DataResult[idx2].GetString(field.Column)
			if str1 == str2 {
				continue
			}
			if field.Direction == Asc {
				return str1 < str2
			}

			return str1 > str2
		case Int64ListCol:
			// join numbers to string
			str1 := raw.DataResult[idx1].GetString(field.Column)
			str2 := raw.DataResult[idx2].GetString(field.Column)
			if str1 == str2 {
				continue
			}
			if field.Direction == Asc {
				return str1 < str2
			}

			return str1 > str2
		case CustomVarCol:
			str1 := raw.DataResult[idx1].GetCustomVarValue(field.Column, field.Args)
			str2 := raw.DataResult[idx2].GetCustomVarValue(field.Column, field.Args)
			if str1 == str2 {
				continue
			}
			if field.Direction == Asc {
				// make empty vars appear last
				if str1 == "" {
					return false
				}
				if str2 == "" {
					return true
				}

				return str1 < str2
			}

			// make empty vars appear first
			if str1 == "" {
				return true
			}
			if str2 == "" {
				return false
			}

			return str1 > str2
		case StringListSortedCol:
			log.Panicf("sorted string list is a virtual column type and not directly used")
		}
		panic(fmt.Sprintf("sorting not implemented for type %s", field.Column.DataType))
	}

	return true
}

// Swap replaces two data rows while sorting.
func (raw *RawResultSet) Swap(i, j int) {
	raw.DataResult[i], raw.DataResult[j] = raw.DataResult[j], raw.DataResult[i]
}
