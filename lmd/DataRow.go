package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DataRow represents a single entry in a DataTable
type DataRow struct {
	noCopy     noCopy
	DataStore  *DataStore
	ID         string
	RawData    []interface{}
	Refs       map[string]*DataRow // contains references to other objects, ex.: hosts from the services table
	LastUpdate int64               // timestamp when this row has been updated
}

// NewDataRow creates a new DataRow
func NewDataRow(store *DataStore, raw *[]interface{}, timestamp int64) (row *DataRow, err error) {
	row = &DataRow{
		RawData:    *raw,
		LastUpdate: timestamp,
		DataStore:  store,
	}
	if !store.Table.PassthroughOnly {
		row.Refs = make(map[string]*DataRow, len(store.Table.RefColCacheIndexes))
	}
	if row.LastUpdate == 0 {
		row.LastUpdate = time.Now().Unix()
	}
	if len(store.Table.PrimaryKey) > 0 {
		var key strings.Builder
		for i, k := range store.Table.PrimaryKey {
			if i > 0 {
				key.WriteString(";")
			}
			key.WriteString(fmt.Sprintf("%v", row.GetValueByName(k)))
		}
		row.ID = key.String()
	}
	err = row.setReferences(store)
	return
}

// setReferences creates reference entries for cross referenced objects
func (d *DataRow) setReferences(store *DataStore) (err error) {
	for _, refNum := range store.Table.RefColCacheIndexes {
		refCol := store.Table.Columns[refNum]
		fieldName := refCol.Name
		refByName := store.Peer.Tables[fieldName].Index
		if refCol.Name == SERVICES {
			hostnameIndex := store.Table.GetColumn("host_name").Index
			if d.RawData[refCol.RefIndex] == nil || d.RawData[refCol.RefIndex].(string) == "" {
				// this may happen for optional reference columns, ex. services in comments
				d.Refs[fieldName] = nil
				continue
			}
			key := d.RawData[hostnameIndex].(string) + ";" + d.RawData[refCol.RefIndex].(string)
			d.Refs[fieldName] = refByName[key]
			if d.Refs[fieldName] == nil {
				err = fmt.Errorf("%s '%s' ref not found from table %s, refmap contains %d elements", refCol.Name, key, store.Table.Name, len(refByName))
				return
			}
		} else {
			d.Refs[fieldName] = refByName[d.RawData[refCol.RefIndex].(string)]
			if d.Refs[fieldName] == nil {
				err = fmt.Errorf("%s '%s' ref not found from table %s, refmap contains %d elements", refCol.Name, d.RawData[refCol.RefIndex].(string), store.Table.Name, len(refByName))
				return
			}
		}
	}
	return
}

// GetValueByName returns the raw value for given column name
func (d *DataRow) GetValueByName(name string) interface{} {
	table := d.DataStore.Table
	i := table.ColumnsIndex[name]
	if i >= len(d.RawData) {
		return d.GetValueByRequestColumn(table.GetRequestColumn(name))
	}
	return d.RawData[i]
}

// GetValueByRequestColumn returns the value for a given ResultColumn in a data row and resolves
// any virtual or reference column.
// The result is returned as interface.
func (d *DataRow) GetValueByRequestColumn(col *RequestColumn) interface{} {
	if col.Column.Index < len(d.RawData) {
		return d.RawData[col.Column.Index]
	}
	return d.indirectValueByRequestColumn(col)
}

func (d *DataRow) indirectValueByRequestColumn(col *RequestColumn) interface{} {
	if col.Type == VirtCol {
		return d.getVirtRowValue(col)
	}

	// this happens if we are requesting an optional column from the wrong backend
	// ex.: shinken specific columns from a non-shinken backend
	if col.Column.RefIndex == 0 {
		// return empty placeholder matching the column type
		return (col.Column.GetEmptyValue())
	}

	// reference columns
	refObj := d.Refs[d.DataStore.Table.Columns[col.Column.RefIndex].Name]
	if refObj == nil {
		return (col.Column.GetEmptyValue())
	}
	if len(refObj.RawData) > col.Column.RefColIndex {
		return refObj.RawData[col.Column.RefColIndex]
	}

	// this happens if we are requesting an optional column from the wrong backend
	// ex.: shinken specific columns from a non-shinken backend
	// -> return empty placeholder matching the column type
	return (col.Column.GetEmptyValue())
}

// getVirtRowValue returns the actual value for a virtual column.
func (d *DataRow) getVirtRowValue(col *RequestColumn) interface{} {
	ok := false
	var value interface{}
	if d.DataStore.Peer != nil {
		p := d.DataStore.Peer
		if p.Flags&LMDSub == LMDSub {
			val, ok2 := d.getVirtSubLMDValue(col)
			if ok2 {
				value = val
			}
		} else {
			p.PeerLock.RLock()
			value, ok = p.Status[col.Column.VirtMap.Key]
			p.PeerLock.RUnlock()
		}
	}
	if !ok {
		value = d.getVirtRowComputedValue(col)
	}
	colType := col.Column.VirtType
	switch colType {
	case IntCol:
		fallthrough
	case FloatCol:
		return numberToFloat(&value)
	case StringCol:
		return value
	case CustomVarCol:
		return value
	case HashMapCol:
		return value
	case StringListCol:
		return value
	case TimeCol:
		val := int64(numberToFloat(&value))
		if val < 0 {
			val = 0
		}
		return val
	default:
		log.Panicf("not implemented")
	}
	return nil
}

// getVirtRowComputedValue returns a computed virtual value for the given column.
func (d *DataRow) getVirtRowComputedValue(col *RequestColumn) (value interface{}) {
	switch col.Name {
	case EMPTY:
		// return empty string as placeholder for nonexisting columns
		value = ""
	case "lmd_last_cache_update":
		// return timestamp of last update for this data row
		value = d.LastUpdate
	case "lmd_version":
		// return lmd version
		value = fmt.Sprintf("%s-%s", NAME, Version())
	case "last_state_change_order":
		// return last_state_change or program_start
		lastStateChange := numberToFloat(&(d.RawData[d.DataStore.Table.ColumnsIndex["last_state_change"]]))
		if lastStateChange == 0 {
			value = d.DataStore.Peer.Status["ProgramStart"]
		} else {
			value = lastStateChange
		}
	case "state_order":
		// return 4 instead of 2, which makes critical come first
		// this way we can use this column to sort by state
		state := numberToFloat(&(d.RawData[d.DataStore.Table.ColumnsIndex["state"]]))
		if state == 2 {
			value = 4
		} else {
			value = state
		}
	case "has_long_plugin_output":
		// return 1 if there is long_plugin_output
		val := d.RawData[d.DataStore.Table.ColumnsIndex["long_plugin_output"]].(string)
		if val != "" {
			value = 1
		} else {
			value = 0
		}
	case "services_with_state":
		fallthrough
	case "services_with_info":
		// test
		servicesIndex := d.DataStore.Table.ColumnsIndex["services"]
		services := d.RawData[servicesIndex]
		hostnameIndex := d.DataStore.Table.ColumnsIndex["name"]
		hostName := d.RawData[hostnameIndex].(string)
		var res []interface{}
		for _, v := range services.([]interface{}) {
			var serviceValue []interface{}
			var serviceID strings.Builder

			serviceID.WriteString(hostName)
			serviceID.WriteString(";")
			serviceID.WriteString(v.(string))

			serviceInfo := d.DataStore.Peer.Tables["services"].Index[serviceID.String()].RawData
			stateIndex := d.DataStore.Peer.Tables["services"].Table.GetColumn("state").Index
			checkedIndex := d.DataStore.Peer.Tables["services"].Table.GetColumn("has_been_checked").Index

			serviceValue = append(serviceValue, v.(string), serviceInfo[stateIndex], serviceInfo[checkedIndex])
			if col.Name == "services_with_info" {
				outputIndex := d.DataStore.Peer.Tables["services"].Table.GetColumn("plugin_output").Index
				serviceValue = append(serviceValue, serviceInfo[outputIndex])
			}
			res = append(res, serviceValue)
		}
		if len(res) > 0 {
			value = res
		} else {
			value = []string{}
		}
	case "host_comments_with_info":
		fallthrough
	case "comments_with_info":
		var comments interface{}
		if col.Name == "host_comments_with_info" {
			hostNameIndex := d.DataStore.Table.ColumnsIndex["host_name"]
			hostName := d.RawData[hostNameIndex]
			host := d.DataStore.Peer.Tables["hosts"].Index[hostName.(string)].RawData
			commentsIndex := d.DataStore.Peer.Tables["hosts"].Table.GetColumn("comments").Index
			comments = host[commentsIndex]
		} else {
			commentsIndex := d.DataStore.Table.ColumnsIndex["comments"]
			comments = d.RawData[commentsIndex]
		}
		var res []interface{}
		for _, commentID := range comments.([]interface{}) {
			var commentWithInfo []interface{}

			commentIDStr := strconv.FormatFloat(commentID.(float64), 'f', 0, 64)
			comment := d.DataStore.Peer.Tables["comments"].Index[commentIDStr].RawData

			authorIndex := d.DataStore.Peer.Tables["comments"].Table.GetColumn("author").Index
			commentIndex := d.DataStore.Peer.Tables["comments"].Table.GetColumn("comment").Index

			commentWithInfo = append(commentWithInfo, commentID, comment[authorIndex], comment[commentIndex])
			res = append(res, commentWithInfo)
		}
		if len(res) > 0 {
			value = res
		} else {
			value = []string{}
		}
	case "configtool":
		if _, ok := d.DataStore.Peer.Status["ConfigTool"]; ok {
			value = d.DataStore.Peer.Status["ConfigTool"]
		} else {
			value = ""
		}
	case "federation_addr":
		if _, ok := d.DataStore.Peer.Status["SubAddr"]; ok {
			value = d.DataStore.Peer.Status["SubAddr"]
		} else {
			value = []string{}
		}
	case "federation_type":
		if _, ok := d.DataStore.Peer.Status["SubType"]; ok {
			value = d.DataStore.Peer.Status["SubType"]
		} else {
			value = []string{}
		}
	case "federation_name":
		if _, ok := d.DataStore.Peer.Status["SubName"]; ok {
			value = d.DataStore.Peer.Status["SubName"]
		} else {
			value = []string{}
		}
	case "federation_key":
		if _, ok := d.DataStore.Peer.Status["SubKey"]; ok {
			value = d.DataStore.Peer.Status["SubKey"]
		} else {
			value = []string{}
		}
	default:
		log.Panicf("cannot handle virtual column: %s", col.Name)
	}
	return
}

// getVirtSubLMDValue returns status values for LMDSub backends
func (d *DataRow) getVirtSubLMDValue(col *RequestColumn) (val interface{}, ok bool) {
	ok = true
	p := d.DataStore.Peer
	peerData := p.StatusGet("SubPeerStatus").(map[string]interface{})
	if peerData == nil {
		return nil, false
	}
	switch col.Name {
	case STATUS:
		// return worst state of LMD and LMDSubs state
		parentVal := p.StatusGet("PeerStatus").(PeerStatus)
		if parentVal != PeerStatusUp {
			val = parentVal
		} else {
			val, ok = peerData[col.Name]
		}
	case "last_error":
		// return worst state of LMD and LMDSubs state
		parentVal := p.StatusGet("LastError").(string)
		val, ok = peerData[col.Name]
		if parentVal != "" && (!ok || val.(string) == "") {
			val = parentVal
		}
	default:
		val, ok = peerData[col.Name]
	}
	return
}

// MatchFilter returns true if the given filter matches the given datarow.
func (d *DataRow) MatchFilter(filter *Filter) bool {
	// recursive group filter
	filterLength := len(filter.Filter)
	if filterLength > 0 {
		for _, f := range filter.Filter {
			subresult := d.MatchFilter(f)
			switch filter.GroupOperator {
			case And:
				// if all conditions must match and we failed already, exit early
				if !subresult {
					return false
				}
			case Or:
				// if only one condition must match and we got that already, exit early
				if subresult {
					return true
				}
			}
		}
		// if this is an AND filter and we did not return yet, this means all have matched.
		// else its an OR filter and none has matched so far.
		return filter.GroupOperator == And
	}

	// normal field filter
	if filter.Column.Column.Index < len(d.RawData) {
		// directly access the row value
		return (filter.MatchFilter(&(d.RawData[filter.Column.Column.Index])))
	}
	value := d.GetValueByRequestColumn(filter.Column)
	return (filter.MatchFilter(&value))
}

func (d *DataRow) getStatsKey(res *Response) string {
	keyValues := []string{}
	for i := range res.Request.RequestColumns {
		col := res.Request.RequestColumns[i]
		value := d.GetValueByRequestColumn(&col)
		keyValues = append(keyValues, fmt.Sprintf("%v", value))
	}
	return strings.Join(keyValues, ";")
}

// UpdateValues updates this datarow with new values
func (d *DataRow) UpdateValues(data *[]interface{}, indexes *[]int, timestamp int64) {
	for j, k := range *indexes {
		d.RawData[k] = (*data)[j]
	}
	d.LastUpdate = timestamp
}

// CheckChangedValues returns true if the given data results in an update
func (d *DataRow) CheckChangedValues(data *[]interface{}, indexes *[]int) bool {
	for j, k := range *indexes {
		if fmt.Sprintf("%v", d.RawData[k]) != fmt.Sprintf("%v", (*data)[j]) {
			return true
		}
	}
	return false
}
