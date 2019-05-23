package main

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

// DataRow represents a single entry in a DataTable
type DataRow struct {
	noCopy         noCopy              // we don't want to make copies, use references
	DataStore      *DataStore          // reference to the datastore itself
	ID             string              // uniq id
	Refs           map[string]*DataRow // contains references to other objects, ex.: hosts from the services table
	LastUpdate     int64               // timestamp when this row has been updated
	dataString     []*string           // stores string data
	dataInt        []int64             // stores integers
	dataFloat      []float64           // stores floats
	dataStringList []*[]string         // stores stringlists
	dataIntList    [][]int64           // stores lists of integers
	dataCustVar    []map[string]string // stores customn variables
}

// NewDataRow creates a new DataRow
func NewDataRow(store *DataStore, raw *[]interface{}, columns *ColumnList, timestamp int64) (d *DataRow, err error) {
	d = &DataRow{
		LastUpdate: timestamp,
		DataStore:  store,
	}
	if raw == nil {
		// virtual tables without data have no references or ids
		return
	}

	if !store.Table.PassthroughOnly {
		d.Refs = make(map[string]*DataRow, len(store.Table.RefTables))
	}

	err = d.SetData(raw, columns, timestamp)
	if err != nil {
		return
	}

	err = d.setID()
	if err != nil {
		return
	}

	err = d.setReferences(store)
	return
}

// setID calculates and set the ID field
func (d *DataRow) setID() (err error) {
	if len(d.DataStore.Table.PrimaryKey) == 0 {
		return
	}

	var key strings.Builder
	for i, k := range d.DataStore.Table.PrimaryKey {
		if i > 0 {
			key.WriteString(";")
		}
		key.WriteString(*(d.GetStringByName(k)))
	}
	d.ID = key.String()
	if d.ID == "" || d.ID == ";" {
		err = fmt.Errorf("id for %s is null", d.DataStore.Table.Name)
	}
	return
}

// setData creates initial data
func (d *DataRow) SetData(raw *[]interface{}, columns *ColumnList, timestamp int64) error {
	d.dataString = make([]*string, d.DataStore.DataSizes[StringCol])
	d.dataStringList = make([]*[]string, d.DataStore.DataSizes[StringListCol])
	d.dataInt = make([]int64, d.DataStore.DataSizes[IntCol])
	d.dataIntList = make([][]int64, d.DataStore.DataSizes[IntListCol])
	d.dataFloat = make([]float64, d.DataStore.DataSizes[FloatCol])
	d.dataCustVar = make([]map[string]string, d.DataStore.DataSizes[CustomVarCol])
	return d.UpdateValues(raw, columns, timestamp)
}

// setReferences creates reference entries for cross referenced objects
func (d *DataRow) setReferences(store *DataStore) (err error) {
	for i := range store.Table.RefTables {
		ref := store.Table.RefTables[i]
		tableName := ref.Table.Name
		refsByName := store.Peer.Tables[tableName].Index

		var key strings.Builder
		for i := range ref.Columns {
			if i > 0 {
				key.WriteString(";")
			}
			key.WriteString(*(d.GetString(ref.Columns[i])))
		}
		refValue := key.String()
		if refValue == "" {
			return fmt.Errorf("failed to create refValue in table %s", d.DataStore.Table.Name)
		}

		d.Refs[tableName] = refsByName[refValue]
		if d.Refs[tableName] == nil {
			if tableName == "services" && (store.Table.Name == "comments" || store.Table.Name == "downtimes") {
				// this may happen for optional reference columns, ex. services in comments
				continue
			}
			return fmt.Errorf("%s '%s' ref not found from table %s, refmap contains %d elements", tableName, refValue, store.Table.Name, len(refsByName))
		}
	}
	return
}

// GetColumn returns the column by name
func (d *DataRow) GetColumn(name string) *Column {
	return d.DataStore.Table.ColumnsIndex[name]
}

// GetString returns the string value for given column
func (d *DataRow) GetString(col *Column) *string {
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case StringCol:
			return d.dataString[col.Index]
		case IntCol:
			val := strconv.FormatInt(d.dataInt[col.Index], 10)
			return &val
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		return d.Refs[col.RefCol.Table.Name].GetString(col.RefCol)
	}
	return interface2string(d.getVirtRowValue(col))
}

// GetStringByName returns the string value for given column name
func (d *DataRow) GetStringByName(name string) *string {
	return d.GetString(d.DataStore.Table.ColumnsIndex[name])
}

// GetStringList returns the string list for given column
func (d *DataRow) GetStringList(col *Column) *[]string {
	switch col.StorageType {
	case LocalStore:
		if col.DataType == StringListCol {
			return d.dataStringList[col.Index]
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		return d.Refs[col.RefCol.Table.Name].GetStringList(col.RefCol)
	}
	return interface2stringlist(d.getVirtRowValue(col))
}

// GetStringListByName returns the string list for given column name
func (d *DataRow) GetStringListByName(name string) *[]string {
	return d.GetStringList(d.DataStore.Table.ColumnsIndex[name])
}

// GetFloat returns the float64 value for given column
func (d *DataRow) GetFloat(col *Column) float64 {
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case FloatCol:
			return d.dataFloat[col.Index]
		case IntCol:
			return float64(d.dataInt[col.Index])
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		return d.Refs[col.RefCol.Table.Name].GetFloat(col.RefCol)
	}
	return interface2float64(d.getVirtRowValue(col))
}

// GetInt returns the int64 value for given column
func (d *DataRow) GetInt(col *Column) int64 {
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case IntCol:
			return d.dataInt[col.Index]
		case FloatCol:
			return int64(d.dataFloat[col.Index])
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		return d.Refs[col.RefCol.Table.Name].GetInt(col.RefCol)
	}
	return interface2int64(d.getVirtRowValue(col))
}

// GetIntByName returns the int64 value for given column name
func (d *DataRow) GetIntByName(name string) int64 {
	return d.GetInt(d.DataStore.Table.ColumnsIndex[name])
}

// GetIntList returns the int64 list for given column
func (d *DataRow) GetIntList(col *Column) []int64 {
	switch col.StorageType {
	case LocalStore:
		if col.DataType == IntListCol {
			return d.dataIntList[col.Index]
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		return d.Refs[col.RefCol.Table.Name].GetIntList(col.RefCol)
	}
	return interface2int64list(d.getVirtRowValue(col))
}

// GetHashMap returns the hashmap for given column
func (d *DataRow) GetHashMap(col *Column) map[string]string {
	switch col.StorageType {
	case LocalStore:
		if col.DataType == HashMapCol || col.DataType == CustomVarCol {
			return d.dataCustVar[col.Index]
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		return d.Refs[col.RefCol.Table.Name].GetHashMap(col.RefCol)
	}
	return interface2hashmap(d.getVirtRowValue(col))
}

// GetValueByColumn returns the raw value for given column
func (d *DataRow) GetValueByColumn(col *Column) interface{} {
	if col.Optional != NoFlags && !d.DataStore.Peer.HasFlag(col.Optional) {
		return col.GetEmptyValue()
	}
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case StringCol:
			return d.dataString[col.Index]
		case StringListCol:
			return d.dataStringList[col.Index]
		case IntCol:
			return d.dataInt[col.Index]
		case IntListCol:
			return d.dataIntList[col.Index]
		case FloatCol:
			return d.dataFloat[col.Index]
		case CustomVarCol:
			return d.dataCustVar[col.Index]
		default:
			log.Panicf("unsupported column %s (type %d) in table %s", col.Name, col.DataType, d.DataStore.Table.Name)
		}
	case RefStore:
		return d.Refs[col.RefCol.Table.Name].GetValueByColumn(col.RefCol)
	case VirtStore:
		return d.getVirtRowValue(col)
	default:
		log.Panicf("unsupported column %s (type %d) in table %s", col.Name, col.DataType, d.DataStore.Table.Name)
	}
	return ""
}

// getVirtRowValue returns the actual value for a virtual column.
func (d *DataRow) getVirtRowValue(col *Column) interface{} {
	var value interface{}
	if col.VirtMap.StatusKey != "" {
		if d.DataStore.Peer == nil {
			log.Panicf("requesting column '%s' from table '%s' with peer", col.Name, d.DataStore.Table.Name)
		}
		p := d.DataStore.Peer
		ok := false
		if p.HasFlag(LMDSub) {
			value, ok = d.getVirtSubLMDValue(col)
		}
		if !ok {
			value = p.StatusGet(col.VirtMap.StatusKey)
		}
	} else {
		value = col.VirtMap.ResolvFunc(d, col)
	}
	return cast2Type(value, col)
}

// VirtColStateOrder returns sortable state
func VirtColLastStateChangeOrder(d *DataRow, col *Column) interface{} {
	// return last_state_change or program_start
	lastStateChange := d.GetIntByName("last_state_change")
	if lastStateChange == 0 {
		return d.DataStore.Peer.Status["ProgramStart"]
	}
	return lastStateChange
}

// VirtColStateOrder returns sortable state
func VirtColStateOrder(d *DataRow, col *Column) interface{} {
	// return 4 instead of 2, which makes critical come first
	// this way we can use this column to sort by state
	state := d.GetIntByName("state")
	if state == 2 {
		return 4
	}
	return state
}

// VirtColHasLongPluginOutput returns 1 if there is long plugin output, 0 if not
func VirtColHasLongPluginOutput(d *DataRow, col *Column) interface{} {
	val := d.GetStringByName("long_plugin_output")
	if *val != "" {
		return 1
	}
	return 0
}

// VirtColServicesWithInfo returns list of services with additional information
func VirtColServicesWithInfo(d *DataRow, col *Column) interface{} {
	services := d.GetStringListByName("services")
	hostName := d.GetStringByName("name")
	servicesStore := d.DataStore.Peer.Tables["services"]
	stateCol := servicesStore.Table.GetColumn("state")
	checkedCol := servicesStore.Table.GetColumn("has_been_checked")
	outputCol := servicesStore.Table.GetColumn("plugin_output")
	res := make([]interface{}, 0)
	for i := range *services {
		serviceID := *hostName + ";" + (*services)[i]
		service, ok := servicesStore.Index[serviceID]
		if !ok {
			log.Errorf("Could not find service: %s\n", serviceID)
			continue
		}
		serviceValue := []interface{}{(*services)[i], service.GetInt(stateCol), service.GetInt(checkedCol)}
		if col.Name == "services_with_info" {
			serviceValue = append(serviceValue, d.GetString(outputCol))
		}
		res = append(res, serviceValue)
	}
	return res
}

// VirtColComments returns list of comment IDs (with optional additional information)
func VirtColComments(d *DataRow, col *Column) interface{} {
	commentsStore := d.DataStore.Peer.Tables["comments"]
	commentsTable := commentsStore.Table
	authorCol := commentsTable.GetColumn("author")
	commentCol := commentsTable.GetColumn("comment")
	key := d.ID
	res := make([]interface{}, 0)
	comments, ok := d.DataStore.Peer.CommentsCache[key]
	if !ok {
		return res
	}
	if col.Name == "comments" {
		return comments
	}
	for i := range comments {
		commentID := strconv.FormatInt(comments[i], 10)
		comment, ok := commentsStore.Index[commentID]
		if !ok {
			log.Errorf("Could not find comment: %s\n", commentID)
			continue
		}
		commentWithInfo := []interface{}{comments[i], comment.GetString(authorCol), comment.GetString(commentCol)}
		res = append(res, commentWithInfo)
	}
	return res
}

// VirtColDowntimes returns list of downtimes IDs
func VirtColDowntimes(d *DataRow, col *Column) interface{} {
	key := d.ID
	downtimes, ok := d.DataStore.Peer.DowntimesCache[key]
	if !ok {
		return nil
	}
	return downtimes
}

// getVirtSubLMDValue returns status values for LMDSub backends
func (d *DataRow) getVirtSubLMDValue(col *Column) (val interface{}, ok bool) {
	ok = true
	p := d.DataStore.Peer
	peerData := p.StatusGet("SubPeerStatus").(map[string]interface{})
	if peerData == nil {
		return nil, false
	}
	switch col.Name {
	case "status":
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
		for i := range filter.Filter {
			f := filter.Filter[i]
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

	// if this is a optional column and we do not meet the requirements, match against an empty default column
	if filter.Column.Optional != NoFlags && !d.DataStore.Peer.HasFlag(filter.Column.Optional) {
		// duplicate filter, but use the empty column
		f := &Filter{
			Column:    d.DataStore.Table.GetEmptyColumn(),
			Operator:  filter.Operator,
			StrValue:  filter.StrValue,
			Regexp:    filter.Regexp,
			IsEmpty:   filter.IsEmpty,
			CustomTag: filter.CustomTag,
		}
		return f.Match(d)
	}

	return filter.Match(d)
}

func (d *DataRow) getStatsKey(res *Response) string {
	if len(res.Request.RequestColumns) == 0 {
		return ""
	}
	keyValues := []string{}
	for i := range res.Request.RequestColumns {
		keyValues = append(keyValues, *(d.GetString(res.Request.RequestColumns[i])))
	}
	return strings.Join(keyValues, ";")
}

// UpdateValues updates this datarow with new values
func (d *DataRow) UpdateValues(data *[]interface{}, columns *ColumnList, timestamp int64) error {
	if len(*columns) != len(*data) {
		return fmt.Errorf("table %s update failed, data size mismatch, expected %d columns and got %d", d.DataStore.Table.Name, len(*columns), len(*data))
	}
	for i := range *columns {
		col := (*columns)[i]
		switch col.DataType {
		case StringCol:
			if col.FetchType == Static {
				// deduplicate strings
				d.dataString[col.Index] = d.deduplicateString(interface2string((*data)[i]))
			} else {
				d.dataString[col.Index] = interface2string((*data)[i])
			}
		case StringListCol:
			if col.FetchType == Static {
				// deduplicate string lists
				d.dataStringList[col.Index] = d.deduplicateStringlist(interface2stringlist((*data)[i]))
			} else {
				d.dataStringList[col.Index] = interface2stringlist((*data)[i])
			}
		case IntCol:
			d.dataInt[col.Index] = interface2int64((*data)[i])
		case IntListCol:
			d.dataIntList[col.Index] = interface2int64list((*data)[i])
		case FloatCol:
			d.dataFloat[col.Index] = interface2float64((*data)[i])
		case CustomVarCol:
			d.dataCustVar[col.Index] = interface2customvar((*data)[i])
		default:
			log.Panicf("unsupported column %s (type %d) in table %s", col.Name, col.DataType, d.DataStore.Table.Name)
		}
	}
	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}
	d.LastUpdate = timestamp
	return nil
}

// CheckChangedIntValues returns true if the given data results in an update
func (d *DataRow) CheckChangedIntValues(data *[]interface{}, columns *ColumnList) bool {
	for j := range *columns {
		if interface2int64((*data)[j]) != d.dataInt[(*columns)[j].Index] {
			return true
		}
	}
	return false
}

func interface2float64(in interface{}) float64 {
	switch v := in.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case bool:
		if v {
			return 1
		}
	case string:
		val, _ := strconv.ParseFloat(v, 64)
		return val
	default:
		val, _ := strconv.ParseFloat(fmt.Sprintf("%v", v), 64)
		return val
	}
	return 0
}

func interface2int64(in interface{}) int64 {
	switch v := in.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case bool:
		if v {
			return 1
		}
	case string:
		val, _ := strconv.ParseInt(v, 10, 64)
		return val
	}
	val, _ := strconv.ParseInt(fmt.Sprintf("%v", in), 10, 64)
	return val
}

func interface2string(in interface{}) *string {
	switch v := in.(type) {
	case string:
		return &v
	case *string:
		return v
	case nil:
		val := ""
		return &val
	}
	str := fmt.Sprintf("%v", in)
	return &str
}

func interface2stringlist(in interface{}) *[]string {
	if list, ok := in.(*[]string); ok {
		return (list)
	}
	if list, ok := in.([]string); ok {
		return (&list)
	}
	if list, ok := in.([]interface{}); ok {
		val := make([]string, 0, len(list))
		for i := range list {
			val = append(val, *(interface2string(list[i])))
		}
		return &val
	}
	val := make([]string, 0)
	return &val
}

func interface2int64list(in interface{}) []int64 {
	if list, ok := in.([]int64); ok {
		return (list)
	}
	if list, ok := in.([]interface{}); ok {
		val := make([]int64, 0, len(list))
		for i := range list {
			val = append(val, interface2int64(list[i]))
		}
		return val
	}
	val := make([]int64, 0)
	return val
}

// interface2hashmap converts an interface to a hashmap
func interface2hashmap(in interface{}) map[string]string {
	if hashmap, ok := in.(map[string]string); ok {
		return (hashmap)
	}
	val := make(map[string]string)
	if list, ok := in.([]interface{}); ok {
		for _, tupelInterface := range list {
			if tupel, ok := tupelInterface.([]interface{}); ok {
				if len(tupel) == 2 {
					if s, ok := tupel[1].(string); ok {
						val[tupel[0].(string)] = s
					} else {
						val[tupel[0].(string)] = fmt.Sprintf("%v", tupel[1])
					}
				}
			}
		}
	}
	return val
}

// interface2customvar converts an interface to a custom var hashmap
//
// usually custom variables come in the form of a simple hash:
// {key: value}
// however icinga2 sends a list which can be any of:
// [], [null], [[key, value]]
func interface2customvar(in interface{}) map[string]string {
	custommap := interface2hashmap(in)
	for key := range custommap {
		upperKey := strings.ToUpper(key)
		if key != upperKey {
			custommap[upperKey] = custommap[key]
			delete(custommap, key)
		}
	}
	return custommap
}

// deduplicateString store duplicate strings only once
func (d *DataRow) deduplicateString(str *string) *string {
	if d.DataStore.dupString == nil {
		return str
	}
	if l, ok := d.DataStore.dupString[*str]; ok {
		return l
	}
	d.DataStore.dupString[*str] = str
	return str
}

// deduplicateStringlist store duplicate string lists only once
func (d *DataRow) deduplicateStringlist(list *[]string) *[]string {
	sum := sha256.Sum256([]byte(strings.Join(*list, ";")))
	if l, ok := d.DataStore.dupStringList[sum]; ok {
		return l
	}
	d.DataStore.dupStringList[sum] = list
	return list
}

func cast2Type(val interface{}, col *Column) interface{} {
	switch col.DataType {
	case StringCol:
		return (interface2string(val))
	case StringListCol:
		return (interface2stringlist(val))
	case IntCol:
		return (interface2int64(val))
	case IntListCol:
		return (interface2int64list(val))
	case FloatCol:
		return (interface2float64(val))
	case HashMapCol:
		return (interface2hashmap(val))
	case InterfaceListCol:
		return val
	}
	log.Panicf("unsupported type: %s", col.DataType)
	return nil
}

// WriteJSON store duplicate string lists only once
func (d *DataRow) WriteJSON(json *jsoniter.Stream, columns *[]*Column) {
	json.WriteRaw("[")
	for i := range *columns {
		col := (*columns)[i]
		if i > 0 {
			json.WriteRaw(",")
		}
		if col.Optional != NoFlags && !d.DataStore.Peer.HasFlag(col.Optional) {
			json.WriteVal(col.GetEmptyValue())
			continue
		}
		switch col.DataType {
		case StringCol:
			json.WriteString(*(d.GetString(col)))
		case StringListCol:
			json.WriteVal(d.GetStringList(col))
		case IntCol:
			json.WriteInt64(d.GetInt(col))
		case FloatCol:
			json.WriteFloat64(d.GetFloat(col))
		case IntListCol:
			json.WriteVal(d.GetIntList(col))
		case InterfaceListCol:
			fallthrough
		case CustomVarCol:
			fallthrough
		case HashMapCol:
			json.WriteVal(d.GetHashMap(col))
		default:
			log.Panicf("unsupported type: %s", col.DataType)
		}
	}
	json.WriteRaw("]\n")
}
