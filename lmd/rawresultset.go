package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// RawResultSet contains references to the result rows or stats objects
type RawResultSet struct {
	noCopy      noCopy
	Total       int            // total number of results for this set
	DataResult  []*DataRow     // references to the data rows required for the result
	StatsResult ResultSetStats // intermediate result of stats query
	Sort        *[]*SortField  // columns required for sorting
}

// PostProcessing does all the post processing required for a request like sorting
// and cutting of limits, applying offsets
func (raw *RawResultSet) PostProcessing(res *Response) {
	if len(res.Request.Stats) > 0 {
		return
	}
	log.Tracef("PostProcessing")

	// offset outside
	if res.Request.Offset > raw.Total {
		raw.DataResult = make([]*DataRow, 0)
	} else {
		// sort our result
		if len(res.Request.Sort) > 0 {
			// skip sorting if there is only one backend requested and we want the default sort order
			if len(res.Request.BackendsMap) >= 1 || !res.Request.IsDefaultSortOrder() {
				t1 := time.Now()
				sort.Sort(raw)
				duration := time.Since(t1)
				log.Debugf("sorting result took %s", duration.String())
			}
		}

		// apply request offset
		if res.Request.Offset > 0 {
			raw.DataResult = raw.DataResult[res.Request.Offset:]
		}
	}

	// apply request limit
	if res.Request.Limit != nil && *res.Request.Limit >= 0 && *res.Request.Limit < len(raw.DataResult) {
		raw.DataResult = raw.DataResult[0:*res.Request.Limit]
	}
}

// Len returns the result length used for sorting results.
func (raw *RawResultSet) Len() int {
	return len(raw.DataResult)
}

// Less returns the sort result of two data rows
func (raw *RawResultSet) Less(i, j int) bool {
	for k := range *raw.Sort {
		s := (*raw.Sort)[k]
		switch s.Column.DataType {
		case IntCol:
			fallthrough
		case Int64Col:
			fallthrough
		case FloatCol:
			valueA := raw.DataResult[i].GetFloat(s.Column)
			valueB := raw.DataResult[j].GetFloat(s.Column)
			if valueA == valueB {
				continue
			}
			if s.Direction == Asc {
				return valueA < valueB
			}
			return valueA > valueB
		case StringCol, StringLargeCol:
			s1 := raw.DataResult[i].GetString(s.Column)
			s2 := raw.DataResult[j].GetString(s.Column)
			if *s1 == *s2 {
				continue
			}
			if s.Direction == Asc {
				return *s1 < *s2
			}
			return *s1 > *s2
		case StringListCol:
			s1 := joinStringlist(raw.DataResult[i].GetStringList(s.Column), ";")
			s2 := joinStringlist(raw.DataResult[j].GetStringList(s.Column), ";")
			if *s1 == *s2 {
				continue
			}
			if s.Direction == Asc {
				return *s1 < *s2
			}
			return *s1 > *s2
		case IntListCol:
			// join numbers to string
			s1 := strings.Join(strings.Fields(fmt.Sprint(raw.DataResult[i].GetIntList(s.Column))), ";")
			s2 := strings.Join(strings.Fields(fmt.Sprint(raw.DataResult[j].GetIntList(s.Column))), ";")
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
		case HashMapCol:
			s1 := raw.DataResult[i].GetHashMap(s.Column)[s.Args]
			s2 := raw.DataResult[j].GetHashMap(s.Column)[s.Args]
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
		case ServiceMemberListCol:
			s1 := fmt.Sprintf("%v", raw.DataResult[i].GetServiceMemberList(s.Column))
			s2 := fmt.Sprintf("%v", raw.DataResult[j].GetServiceMemberList(s.Column))
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
		case InterfaceListCol:
			s1 := fmt.Sprintf("%v", raw.DataResult[i].GetInterfaceList(s.Column))
			s2 := fmt.Sprintf("%v", raw.DataResult[j].GetInterfaceList(s.Column))
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
		case CustomVarCol:
			s1 := fmt.Sprintf("%v", raw.DataResult[i].GetHashMap(s.Column))
			s2 := fmt.Sprintf("%v", raw.DataResult[j].GetHashMap(s.Column))
			if s1 == s2 {
				continue
			}
			if s.Direction == Asc {
				return s1 < s2
			}
			return s1 > s2
		}
		panic(fmt.Sprintf("sorting not implemented for type %s", s.Column.DataType))
	}
	return true
}

// Swap replaces two data rows while sorting.
func (raw *RawResultSet) Swap(i, j int) {
	raw.DataResult[i], raw.DataResult[j] = raw.DataResult[j], raw.DataResult[i]
}
