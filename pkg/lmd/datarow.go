package lmd

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"unique"

	jsoniter "github.com/json-iterator/go"
)

const ListSepChar1 = "\x00"

// DataRow represents a single entry in a DataTable.
type DataRow struct {
	noCopy                noCopy
	dataStore             *DataStore             // reference to the datastore itself
	refs                  map[TableName]*DataRow // contains references to other objects, ex.: hosts from the services table
	dataInt64List         [][]int64              // stores lists of integers
	dataString            []string               // stores string data
	dataInt               []int8                 // stores integers
	dataInt64             []int64                // stores large integers
	dataFloat             []float64              // stores floats
	dataStringList        [][]string             // stores string lists
	dataServiceMemberList [][]ServiceMember      // stores list of service members
	dataStringLarge       []StringContainer      // stores large strings
	dataInterfaceList     [][]interface{}        // stores anything else
	lastUpdate            float64                // timestamp of last update
}

// NewDataRow creates a new DataRow.
func NewDataRow(store *DataStore, raw []interface{}, columns ColumnList, timestamp float64, setReferences bool) (d *DataRow, err error) {
	d = &DataRow{
		lastUpdate: timestamp,
		dataStore:  store,
	}
	if raw == nil {
		// virtual tables without data have no references or ids
		return
	}

	if !store.table.passthroughOnly && len(store.table.refTables) > 0 {
		d.refs = make(map[TableName]*DataRow, len(store.table.refTables))
	}

	err = d.SetData(raw, columns, timestamp)
	if err != nil {
		return
	}

	d.setLowerCaseCache()

	if setReferences {
		err = d.SetReferences()
	}

	return
}

// GetID calculates and returns the ID value (nul byte concatenated primary key values).
func (d *DataRow) GetID() string {
	if len(d.dataStore.table.primaryKey) == 0 {
		return ""
	}
	if len(d.dataStore.table.primaryKey) == 1 {
		id := d.GetStringByName(d.dataStore.table.primaryKey[0])
		if id == "" {
			logWith(d).Errorf("id for %s is null", d.dataStore.table.name.String())
		}

		return id
	}

	var key strings.Builder
	for i, k := range d.dataStore.table.primaryKey {
		if i > 0 {
			key.WriteString(ListSepChar1)
		}
		key.WriteString(d.GetStringByName(k))
	}
	id := key.String()
	if id == "" || id == ListSepChar1 {
		logWith(d).Errorf("id for %s is null", d.dataStore.table.name.String())
	}

	return id
}

// GetID2 returns the 2 strings for tables with 2 primary keys.
func (d *DataRow) GetID2() (id1, id2 string) {
	id1 = d.GetStringByName(d.dataStore.table.primaryKey[0])
	if id1 == "" {
		logWith(d).Errorf("id1 for %s is null", d.dataStore.table.name.String())
	}
	id2 = d.GetStringByName(d.dataStore.table.primaryKey[1])
	if id2 == "" {
		logWith(d).Errorf("id2 for %s is null", d.dataStore.table.name.String())
	}

	return id1, id2
}

// SetData creates initial data.
func (d *DataRow) SetData(raw []interface{}, columns ColumnList, timestamp float64) error {
	for key, size := range d.dataStore.table.dataSizes {
		if size == 0 {
			continue
		}
		switch key {
		case StringCol:
			d.dataString = make([]string, size)
		case StringListCol:
			d.dataStringList = make([][]string, size)
		case IntCol:
			d.dataInt = make([]int8, size)
		case Int64Col:
			d.dataInt64 = make([]int64, size)
		case Int64ListCol:
			d.dataInt64List = make([][]int64, size)
		case FloatCol:
			d.dataFloat = make([]float64, size)
		case ServiceMemberListCol:
			d.dataServiceMemberList = make([][]ServiceMember, size)
		case InterfaceListCol:
			d.dataInterfaceList = make([][]interface{}, size)
		case StringLargeCol:
			d.dataStringLarge = make([]StringContainer, size)
		case JSONCol, CustomVarCol, StringListSortedCol:
			log.Panicf("not implemented: %#v", key)
		}
	}

	return d.UpdateValues(0, raw, columns, timestamp)
}

// setLowerCaseCache sets lowercase columns.
func (d *DataRow) setLowerCaseCache() {
	for from, to := range d.dataStore.lowerCaseColumns {
		d.dataString[to] = strings.ToLower(d.dataString[from])
	}
}

// SetReferences creates reference entries for cross referenced objects.
func (d *DataRow) SetReferences() (err error) {
	store := d.dataStore
	for i := range store.table.refTables {
		ref := &store.table.refTables[i]
		tableName := ref.Table.name
		refsByName := store.dataSet.Get(tableName).index
		refsByName2 := store.dataSet.Get(tableName).index2

		switch len(ref.Columns) {
		case 1:
			d.refs[tableName] = refsByName[d.GetString(ref.Columns[0])]
		case 2:
			d.refs[tableName] = refsByName2[d.GetString(ref.Columns[0])][d.GetString(ref.Columns[1])]
		}
		if _, ok := d.refs[tableName]; !ok {
			if tableName == TableServices && (store.table.name == TableComments || store.table.name == TableDowntimes) {
				// this may happen for optional reference columns, ex. services in comments
				continue
			}

			return fmt.Errorf("%s reference not found from table %s, refmap contains %d elements", tableName.String(), store.table.name.String(), len(refsByName))
		}
	}

	return
}

// GetString returns the string value for given column.
func (d *DataRow) GetString(col *Column) string {
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case StringCol:
			return d.dataString[col.Index]
		case IntCol:
			return fmt.Sprintf("%d", d.dataInt[col.Index])
		case Int64Col:
			return strconv.FormatInt(d.dataInt64[col.Index], 10)
		case FloatCol:
			return fmt.Sprintf("%v", d.dataFloat[col.Index])
		case StringLargeCol:
			return d.dataStringLarge[col.Index].String()
		case StringListCol:
			return strings.Join(d.dataStringList[col.Index], ListSepChar1)
		case ServiceMemberListCol:
			return fmt.Sprintf("%v", d.dataServiceMemberList[col.Index])
		case InterfaceListCol:
			return fmt.Sprintf("%v", d.dataInterfaceList[col.Index])
		case Int64ListCol:
			return strings.Join(strings.Fields(fmt.Sprint(d.dataInt64List[col.Index])), ListSepChar1)
		default:
			log.Panicf("unsupported type: %s", col.DataType)
		}
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref == nil {
			return interface2stringNoDedup(col.GetEmptyValue())
		}

		return ref.GetString(col.RefCol)
	case VirtualStore:
		return interface2stringNoDedup(d.getVirtualRowValue(col))
	}
	panic(fmt.Sprintf("unsupported type: %s", col.DataType))
}

// GetStringByName returns the string value for given column name.
func (d *DataRow) GetStringByName(name string) string {
	return d.GetString(d.dataStore.table.columnsIndex[name])
}

// GetStringList returns the string list for given column.
func (d *DataRow) GetStringList(col *Column) []string {
	switch col.StorageType {
	case LocalStore:
		if col.DataType == StringListCol {
			return d.dataStringList[col.Index]
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref == nil {
			return interface2stringListNoDedup(col.GetEmptyValue())
		}

		return ref.GetStringList(col.RefCol)
	case VirtualStore:
		return interface2stringListNoDedup(d.getVirtualRowValue(col))
	}
	panic(fmt.Sprintf("unsupported type: %s", col.DataType))
}

// GetStringListByName returns the string list for given column name.
func (d *DataRow) GetStringListByName(name string) []string {
	return d.GetStringList(d.dataStore.table.columnsIndex[name])
}

// GetFloat returns the float64 value for given column.
func (d *DataRow) GetFloat(col *Column) float64 {
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case FloatCol:
			return d.dataFloat[col.Index]
		case IntCol:
			return float64(d.dataInt[col.Index])
		case Int64Col:
			return float64(d.dataInt64[col.Index])
		default:
			log.Panicf("unsupported type: %s", col.DataType)
		}
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref == nil {
			return interface2float64(col.GetEmptyValue())
		}

		return ref.GetFloat(col.RefCol)
	case VirtualStore:
		return interface2float64(d.getVirtualRowValue(col))
	}
	panic(fmt.Sprintf("unsupported type: %s", col.DataType))
}

// GetInt8 returns the int8 value for given column.
func (d *DataRow) GetInt8(col *Column) int8 {
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case IntCol:
			return d.dataInt[col.Index]
		case FloatCol:
			return int8(d.dataFloat[col.Index])
		default:
			log.Panicf("unsupported type: %s", col.DataType)
		}
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref == nil {
			return interface2int8(col.GetEmptyValue())
		}

		return ref.GetInt8(col.RefCol)
	case VirtualStore:
		return interface2int8(d.getVirtualRowValue(col))
	}
	panic(fmt.Sprintf("unsupported type: %s", col.StorageType))
}

// GetInt64 returns the int64 value for given column.
func (d *DataRow) GetInt64(col *Column) int64 {
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case Int64Col:
			return d.dataInt64[col.Index]
		case IntCol:
			return int64(d.dataInt[col.Index])
		case FloatCol:
			return int64(d.dataFloat[col.Index])
		default:
			log.Panicf("unsupported type: %s", col.DataType)
		}
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref == nil {
			return interface2int64(col.GetEmptyValue())
		}

		return ref.GetInt64(col.RefCol)
	case VirtualStore:
		return interface2int64(d.getVirtualRowValue(col))
	}
	panic(fmt.Sprintf("unsupported type: %s", col.StorageType))
}

// GetIntByName returns the int value for given column name.
func (d *DataRow) GetInt8ByName(name string) int8 {
	return d.GetInt8(d.dataStore.table.columnsIndex[name])
}

// GetInt64ByName returns the int value for given column name.
func (d *DataRow) GetInt64ByName(name string) int64 {
	return d.GetInt64(d.dataStore.table.columnsIndex[name])
}

// GetInt64List returns the int64 list for given column.
func (d *DataRow) GetInt64List(col *Column) []int64 {
	switch col.StorageType {
	case LocalStore:
		if col.DataType == Int64ListCol {
			return d.dataInt64List[col.Index]
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref == nil {
			return interface2int64list(col.GetEmptyValue())
		}

		return ref.GetInt64List(col.RefCol)
	case VirtualStore:
		return interface2int64list(d.getVirtualRowValue(col))
	}
	panic(fmt.Sprintf("unsupported type: %s", col.StorageType))
}

// GetInt64ListByName returns the int64 list for given column name.
func (d *DataRow) GetInt64ListByName(name string) []int64 {
	return d.GetInt64List(d.dataStore.table.columnsIndex[name])
}

// GetServiceMemberList returns the a list of service members.
func (d *DataRow) GetServiceMemberList(col *Column) []ServiceMember {
	switch col.StorageType {
	case LocalStore:
		if col.DataType == ServiceMemberListCol {
			return d.dataServiceMemberList[col.Index]
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref == nil {
			return interface2serviceMemberList(col.GetEmptyValue())
		}

		return ref.GetServiceMemberList(col.RefCol)
	case VirtualStore:
		return interface2serviceMemberList(d.getVirtualRowValue(col))
	}
	panic(fmt.Sprintf("unsupported type: %s", col.StorageType))
}

// GetServiceMemberListByName returns the service member list for given column name.
func (d *DataRow) GetServiceMemberListByName(name string) []ServiceMember {
	return d.GetServiceMemberList(d.dataStore.table.columnsIndex[name])
}

// GetInterfaceList returns the a list of interfaces.
func (d *DataRow) GetInterfaceList(col *Column) []interface{} {
	switch col.StorageType {
	case LocalStore:
		if col.DataType == InterfaceListCol {
			return d.dataInterfaceList[col.Index]
		}
		log.Panicf("unsupported type: %s", col.DataType)
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref == nil {
			return interface2interfaceList(col.GetEmptyValue())
		}

		return ref.GetInterfaceList(col.RefCol)
	case VirtualStore:
		return interface2interfaceList(d.getVirtualRowValue(col))
	}
	panic(fmt.Sprintf("unsupported type: %s", col.StorageType))
}

// GetValueByColumn returns the raw value for given column.
func (d *DataRow) GetValueByColumn(col *Column) interface{} {
	if col.Optional != NoFlags && !d.dataStore.peer.HasFlag(col.Optional) {
		return col.GetEmptyValue()
	}
	switch col.StorageType {
	case LocalStore:
		switch col.DataType {
		case StringCol:
			return d.dataString[col.Index]
		case StringListCol:
			return d.dataStringList[col.Index]
		case StringLargeCol:
			return d.dataStringLarge[col.Index].StringRef()
		case IntCol:
			return d.dataInt[col.Index]
		case Int64ListCol:
			return d.dataInt64List[col.Index]
		case Int64Col:
			return d.dataInt64[col.Index]
		case FloatCol:
			return d.dataFloat[col.Index]
		case InterfaceListCol:
			return d.dataInterfaceList[col.Index]
		case ServiceMemberListCol:
			return d.dataServiceMemberList[col.Index]
		default:
			log.Panicf("unsupported column %s (type %s) in table %s", col.Name, col.DataType.String(), d.dataStore.table.name.String())
		}
	case RefStore:
		return d.refs[col.RefColTableName].GetValueByColumn(col.RefCol)
	case VirtualStore:
		return d.getVirtualRowValue(col)
	}
	panic(fmt.Sprintf("unsupported type: %s", col.StorageType))
}

// getVirtualRowValue returns the actual value for a virtual column.
func (d *DataRow) getVirtualRowValue(col *Column) interface{} {
	var value interface{}
	peer := d.dataStore.peer
	if col.VirtualMap.statusKey > 0 {
		if peer == nil {
			log.Panicf("requesting column '%s' from table '%s' with peer", col.Name, d.dataStore.table.name.String())
		}
		ok := false
		if peer.HasFlag(LMDSub) {
			value, ok = d.getVirtualSubLMDValue(peer, col)
		}
		if !ok {
			switch col.VirtualMap.statusKey {
			case PeerName:
				return &(peer.Name)
			case PeerKey:
				return &(peer.ID)
			case Section:
				return &(peer.section)
			case PeerParent:
				return &(peer.parentID)
			}
		}
	} else {
		value = col.VirtualMap.resolveFunc(peer, d, col)
	}

	return cast2Type(value, col)
}

// GetCustomVarValue returns custom variable value for given name.
func (d *DataRow) GetCustomVarValue(col *Column, name string) string {
	if col.StorageType == RefStore {
		ref := d.refs[col.RefColTableName]

		return ref.GetCustomVarValue(col.RefCol, name)
	}
	namesCol := d.dataStore.GetColumn("custom_variable_names")
	names := d.dataStringList[namesCol.Index]
	for idx, n := range names {
		if n != name {
			continue
		}
		valuesCol := d.dataStore.GetColumn("custom_variable_values")
		values := d.dataStringList[valuesCol.Index]
		if idx >= len(values) {
			return ""
		}

		return values[idx]
	}

	return ""
}

// VirtualColLocaltime returns current unix timestamp.
func VirtualColLocaltime(_ *Peer, _ *DataRow, _ *Column) interface{} {
	return currentUnixTime()
}

// VirtualColLastStateChangeOrder returns sortable state.
func VirtualColLastStateChangeOrder(p *Peer, d *DataRow, _ *Column) interface{} {
	// return last_state_change or program_start
	lastStateChange := d.GetInt64ByName("last_state_change")
	if lastStateChange == 0 {
		return p.programStart.Load()
	}

	return lastStateChange
}

// VirtualColStateOrder returns sortable state.
func VirtualColStateOrder(_ *Peer, d *DataRow, _ *Column) interface{} {
	// return 4 instead of 2, which makes critical come first
	// this way we can use this column to sort by state
	state := d.GetInt8ByName("state")
	if state == 2 {
		return 4
	}

	return state
}

// VirtualColHasLongPluginOutput returns 1 if there is long plugin output, 0 if not.
func VirtualColHasLongPluginOutput(_ *Peer, d *DataRow, _ *Column) interface{} {
	val := d.GetStringByName("long_plugin_output")
	if val != "" {
		return 1
	}

	return 0
}

// VirtualColServicesWithInfo returns list of services with additional information.
func VirtualColServicesWithInfo(_ *Peer, d *DataRow, col *Column) interface{} {
	services := d.GetStringListByName("services")
	hostName := d.GetStringByName("name")
	servicesStore := d.dataStore.dataSet.Get(TableServices)
	stateCol := servicesStore.table.GetColumn("state")
	checkedCol := servicesStore.table.GetColumn("has_been_checked")
	outputCol := servicesStore.table.GetColumn("plugin_output")
	res := make([]interface{}, len(services))
	for idx := range services {
		service, ok := servicesStore.index2[hostName][services[idx]]
		if !ok {
			log.Errorf("Could not find service: %s - %s\n", hostName, services[idx])

			continue
		}
		serviceValue := []interface{}{services[idx], service.GetInt8(stateCol), service.GetInt8(checkedCol)}
		if col.Name == "services_with_info" {
			serviceValue = append(serviceValue, service.GetString(outputCol))
		}
		res[idx] = serviceValue
	}

	return res
}

// VirtualColMembersWithState returns a list of hostgroup/servicegroup members with their states.
func VirtualColMembersWithState(_ *Peer, dRow *DataRow, _ *Column) interface{} {
	switch dRow.dataStore.table.name {
	case TableHostgroups:
		members := dRow.GetStringListByName("members")
		hostsStore := dRow.dataStore.dataSet.Get(TableHosts)
		stateCol := hostsStore.table.GetColumn("state")
		checkedCol := hostsStore.table.GetColumn("has_been_checked")

		res := make([]interface{}, len(members))
		for idx, hostName := range members {
			host, ok := hostsStore.index[hostName]
			if !ok {
				log.Errorf("Could not find host: %s\n", hostName)

				continue
			}
			res[idx] = []interface{}{hostName, host.GetInt8(stateCol), host.GetInt8(checkedCol)}
		}

		return res
	case TableServicegroups:
		membersCol := dRow.dataStore.GetColumn("members")
		members := dRow.GetServiceMemberList(membersCol)
		servicesStore := dRow.dataStore.dataSet.Get(TableServices)
		stateCol := servicesStore.table.GetColumn("state")
		checkedCol := servicesStore.table.GetColumn("has_been_checked")

		res := make([]interface{}, len(members))
		for idx := range members {
			hostName := members[idx][0]
			serviceDescription := members[idx][1]

			service, ok := servicesStore.index2[hostName][serviceDescription]
			if !ok {
				log.Errorf("Could not find service: %s - %s\n", hostName, serviceDescription)

				continue
			}
			res[idx] = []interface{}{hostName, serviceDescription, service.GetInt8(stateCol), service.GetInt8(checkedCol)}
		}

		return res
	default:
		log.Panicf("unsupported table: %s", dRow.dataStore.table.name.String())
	}

	return nil
}

// VirtualColCommentsWithInfo returns list of comment IDs with additional information.
func VirtualColCommentsWithInfo(_ *Peer, row *DataRow, _ *Column) interface{} {
	comments := row.GetInt64ListByName("comments")
	if len(comments) == 0 {
		return emptyInterfaceList
	}

	commentsStore := row.dataStore.dataSet.Get(TableComments)
	commentsTable := commentsStore.table
	authorCol := commentsTable.GetColumn("author")
	commentCol := commentsTable.GetColumn("comment")
	entryTimeCol := commentsTable.GetColumn("entry_time")
	entryTypeCol := commentsTable.GetColumn("entry_type")
	expiresCol := commentsTable.GetColumn("expires")
	expireTimeCol := commentsTable.GetColumn("expire_time")
	res := make([]interface{}, 0)
	for idx := range comments {
		commentID := fmt.Sprintf("%d", comments[idx])
		comment, ok := commentsStore.index[commentID]
		if !ok {
			log.Errorf("Could not find comment: %s\n", commentID)

			continue
		}
		commentWithInfo := []interface{}{
			comments[idx],
			comment.GetString(authorCol),
			comment.GetString(commentCol),
			comment.GetInt64(entryTimeCol),
			comment.GetInt8(entryTypeCol),
			comment.GetInt8(expiresCol),
			comment.GetInt64(expireTimeCol),
		}
		res = append(res, commentWithInfo)
	}

	return res
}

// VirtualColDowntimesWithInfo returns list of downtimes IDs with additional information.
func VirtualColDowntimesWithInfo(_ *Peer, row *DataRow, _ *Column) interface{} {
	downtimes := row.GetInt64ListByName("downtimes")
	if len(downtimes) == 0 {
		return emptyInterfaceList
	}

	downtimesStore := row.dataStore.dataSet.Get(TableDowntimes)
	downtimesTable := downtimesStore.table
	authorCol := downtimesTable.GetColumn("author")
	commentCol := downtimesTable.GetColumn("comment")
	entryTimeCol := downtimesTable.GetColumn("entry_time")
	startTimeCol := downtimesTable.GetColumn("start_time")
	endTimeCol := downtimesTable.GetColumn("end_time")
	fixedCol := downtimesTable.GetColumn("fixed")
	durationCol := downtimesTable.GetColumn("duration")
	triggeredCol := downtimesTable.GetColumn("triggered_by")
	res := make([]interface{}, 0)
	for idx := range downtimes {
		downtimeID := fmt.Sprintf("%d", downtimes[idx])
		downtime, ok := downtimesStore.index[downtimeID]
		if !ok {
			log.Errorf("Could not find downtime: %s\n", downtimeID)

			continue
		}
		downtimeWithInfo := []interface{}{
			downtimes[idx],
			downtime.GetString(authorCol),
			downtime.GetString(commentCol),
			downtime.GetInt64(entryTimeCol),
			downtime.GetInt64(startTimeCol),
			downtime.GetInt64(endTimeCol),
			downtime.GetInt64(fixedCol),
			downtime.GetInt64(durationCol),
			downtime.GetInt64(triggeredCol),
		}
		res = append(res, downtimeWithInfo)
	}

	return res
}

// VirtualColCustomVariables returns a custom variables hash.
func VirtualColCustomVariables(_ *Peer, row *DataRow, _ *Column) interface{} {
	namesCol := row.dataStore.GetColumn("custom_variable_names")
	valuesCol := row.dataStore.GetColumn("custom_variable_values")
	names := row.dataStringList[namesCol.Index]
	values := row.dataStringList[valuesCol.Index]
	res := make(map[string]string, len(names))
	for i := range names {
		res[names[i]] = values[i]
	}

	return res
}

// VirtualColTotalServices returns number of services.
func VirtualColTotalServices(_ *Peer, d *DataRow, _ *Column) interface{} {
	return d.GetInt64ByName("num_services")
}

// VirtualColFlags returns flags for peer.
func VirtualColFlags(p *Peer, _ *DataRow, _ *Column) interface{} {
	peerFlags := OptionalFlags(atomic.LoadUint32(&p.flags))

	return peerFlags.List()
}

// getVirtualSubLMDValue returns status values for LMDSub backends.
func (d *DataRow) getVirtualSubLMDValue(peer *Peer, col *Column) (val interface{}, ok bool) {
	peerState := peer.peerState.Get()
	peerData := peer.subPeerStatus.Load()

	if peerData == nil {
		return nil, false
	}
	switch col.Name {
	case "status":
		// return worst state of LMD and LMDSubs state
		if peerState != PeerStatusUp {
			return peerState, true
		}

		val, ok = (*peerData)[col.Name]

		return val, ok
	case "last_error":
		// return worst state of LMD and LMDSubs state
		if peer.lastError.Get() != "" {
			return peer.lastError.Get(), true
		}
		val, ok = (*peerData)[col.Name]

		return val, ok
	}

	val, ok = (*peerData)[col.Name]

	return val, ok
}

// MatchFilter returns true if the given filter matches the given data row.
// negate param is to force filter to be negated. Default is false.
func (d *DataRow) MatchFilter(filter *Filter, negate bool) bool {
	// recursive group filter
	groupOperator := filter.groupOperator
	negate = negate || filter.negate

	if negate {
		// Inverse the operation if negate is done at the GroupOperator
		switch groupOperator {
		case And:
			groupOperator = Or
		case Or:
			groupOperator = And
		}
	}

	switch groupOperator {
	case And:
		for _, f := range filter.filter {
			if !d.MatchFilter(f, negate) {
				return false
			}
		}

		return true
	case Or:
		for _, f := range filter.filter {
			if d.MatchFilter(f, negate) {
				return true
			}
		}

		return false
	}

	// if this is a optional column and we do not meet the requirements, match against an empty default column
	if filter.columnOptional != NoFlags && !d.dataStore.peer.HasFlag(filter.columnOptional) {
		// duplicate filter, but use the empty column
		dupFilter := &Filter{
			column:      d.dataStore.table.GetEmptyColumn(),
			operator:    filter.operator,
			stringVal:   filter.stringVal,
			regexp:      filter.regexp,
			isEmpty:     filter.isEmpty,
			customTag:   filter.customTag,
			negate:      negate,
			columnIndex: -1,
		}
		dupFilter.column.DataType = filter.column.DataType
		if dupFilter.negate {
			return !dupFilter.Match(d)
		}

		return dupFilter.Match(d)
	}
	if negate {
		return !filter.Match(d)
	}

	return filter.Match(d)
}

func (d *DataRow) getStatsKey(res *Response) string {
	if len(res.request.RequestColumns) == 0 {
		return ""
	}
	keyValues := []string{}
	for i := range res.request.RequestColumns {
		keyValues = append(keyValues, d.GetString(res.request.RequestColumns[i]))
	}

	return strings.Join(keyValues, ListSepChar1)
}

// UpdateValues updates this data row with new values.
func (d *DataRow) UpdateValues(dataOffset int, data []interface{}, columns ColumnList, timestamp float64) error {
	if len(columns) != len(data)-dataOffset {
		return fmt.Errorf("table %s update failed, data size mismatch, expected %d columns and got %d", d.dataStore.table.name.String(), len(columns), len(data))
	}

	for idx, col := range columns {
		localIndex := col.Index
		if col.StorageType != LocalStore {
			continue
		}
		if localIndex < 0 {
			log.Panicf("%s: tried to update bad column, index %d - %s", d.dataStore.table.name.String(), localIndex, col.Name)
		}
		resIndex := idx + dataOffset
		switch col.DataType {
		case StringCol:
			d.dataString[localIndex] = *(interface2string(data[resIndex]))
		case StringListCol:
			d.dataStringList[localIndex] = interface2stringList(data[resIndex], col.SortedList)
		case StringLargeCol:
			d.dataStringLarge[localIndex] = *interface2stringLarge(data[resIndex])
		case IntCol:
			d.dataInt[localIndex] = interface2int8(data[resIndex])
		case Int64Col:
			d.dataInt64[localIndex] = interface2int64(data[resIndex])
		case Int64ListCol:
			d.dataInt64List[localIndex] = interface2int64list(data[resIndex])
		case FloatCol:
			d.dataFloat[localIndex] = interface2float64(data[resIndex])
		case ServiceMemberListCol:
			d.dataServiceMemberList[localIndex] = interface2serviceMemberList(data[resIndex])
		case InterfaceListCol:
			d.dataInterfaceList[localIndex] = interface2interfaceList(data[resIndex])
		default:
			log.Panicf("unsupported column %s (type %d) in table %s", col.Name, col.DataType, d.dataStore.table.name.String())
		}
	}
	if timestamp == 0 {
		timestamp = currentUnixTime()
	}
	d.lastUpdate = timestamp

	return nil
}

// UpdateValuesNumberOnly updates this data row with new values but skips strings.
func (d *DataRow) UpdateValuesNumberOnly(dataOffset int, data []interface{}, columns ColumnList, timestamp float64) error {
	if len(columns) != len(data)-dataOffset {
		return fmt.Errorf("table %s update failed, data size mismatch, expected %d columns and got %d", d.dataStore.table.name.String(), len(columns), len(data))
	}
	for i, col := range columns {
		localIndex := col.Index
		resIndex := i + dataOffset
		switch col.DataType {
		case IntCol:
			d.dataInt[localIndex] = interface2int8(data[resIndex])
		case Int64Col:
			d.dataInt64[localIndex] = interface2int64(data[resIndex])
		case Int64ListCol:
			d.dataInt64List[localIndex] = interface2int64list(data[resIndex])
		case FloatCol:
			d.dataFloat[localIndex] = interface2float64(data[resIndex])
		case InterfaceListCol:
			d.dataInterfaceList[localIndex] = interface2interfaceList(data[resIndex])
		default:
			// skipping non-numerical columns
		}
	}
	d.lastUpdate = timestamp

	return nil
}

// checkChangedIntValues returns true if the given data results in an update.
func (d *DataRow) checkChangedIntValues(dataOffset int, data []interface{}, columns ColumnList) bool {
	for colNum, col := range columns {
		switch col.DataType {
		case IntCol:
			if interface2int8(data[colNum+dataOffset]) != d.dataInt[col.Index] {
				log.Tracef("checkChangedIntValues: int value %s changed: local: %d remote: %d", col.Name, d.dataInt[col.Index], interface2int8(data[colNum+dataOffset]))

				return true
			}
		case Int64Col:
			if interface2int64(data[colNum+dataOffset]) != d.dataInt64[col.Index] {
				log.Tracef("checkChangedIntValues: int64 value %s changed: local: %d remote: %d", col.Name, d.dataInt64[col.Index], interface2int64(data[colNum+dataOffset]))

				return true
			}
		default:
			// skipping non-int columns
		}
	}

	return false
}

func interface2float64(in interface{}) float64 {
	switch num := in.(type) {
	case float64:
		return num
	case int64:
		return float64(num)
	case int:
		return float64(num)
	case bool:
		if num {
			return 1
		}
	case string:
		val, _ := strconv.ParseFloat(num, 64)

		return val
	default:
		val, _ := strconv.ParseFloat(fmt.Sprintf("%v", num), 64)

		return val
	}

	return 0
}

func interface2int(raw interface{}) int {
	switch num := raw.(type) {
	case float64:
		return int(num)
	case int64:
		return int(num)
	case int:
		return num
	case int8:
		return int(num)
	case int32:
		return int(num)
	case bool:
		if num {
			return 1
		}

		return 0
	case string:
		val, _ := strconv.ParseInt(num, 10, 64)

		return int(val)
	}
	val, _ := strconv.ParseInt(fmt.Sprintf("%v", raw), 10, 64)

	return int(val)
}

func checkInt8Bounds(num int64) int8 {
	if num > int64(math.MaxInt8) || num < int64(math.MinInt8) {
		return 0
	}

	return int8(num)
}

func interface2int8(raw interface{}) int8 {
	switch num := raw.(type) {
	case float64:
		return checkInt8Bounds(int64(num))
	case int64:
		return checkInt8Bounds(num)
	case int:
		return checkInt8Bounds(int64(num))
	case int8:
		return num
	case int32:
		return checkInt8Bounds(int64(num))
	case bool:
		if num {
			return 1
		}

		return 0
	case string:
		val, _ := strconv.ParseInt(num, 10, 64)

		return checkInt8Bounds(val)
	}
	val, _ := strconv.ParseInt(fmt.Sprintf("%v", raw), 10, 64)

	return checkInt8Bounds(val)
}

func interface2int64(raw interface{}) int64 {
	switch num := raw.(type) {
	case float64:
		return int64(num)
	case int64:
		return num
	case int:
		return int64(num)
	case bool:
		if num {
			return 1
		}
	case string:
		val, _ := strconv.ParseInt(num, 10, 64)

		return val
	}
	val, _ := strconv.ParseInt(fmt.Sprintf("%v", raw), 10, 64)

	return val
}

func interface2string(raw interface{}) *string {
	switch str := raw.(type) {
	case string:
		dedupedstring := unique.Make(str).Value()

		return &dedupedstring
	case []byte:
		dedupedstring := unique.Make(string(str)).Value()

		return &dedupedstring
	case *[]byte:
		dedupedstring := unique.Make(string(*str)).Value()

		return &dedupedstring
	case *string:
		return str
	case nil:
		val := ""

		return &val
	}

	dedupedstring := unique.Make(fmt.Sprintf("%v", raw)).Value()

	return &dedupedstring
}

func interface2stringNoDedup(raw interface{}) string {
	switch str := raw.(type) {
	case string:
		return str
	case *string:
		return *str
	case nil:
		return ""
	}

	return fmt.Sprintf("%v", raw)
}

func interface2stringLarge(raw interface{}) *StringContainer {
	switch str := raw.(type) {
	case string:
		return NewStringContainer(&str)
	case *string:
		return NewStringContainer(str)
	case nil:
		val := ""

		return NewStringContainer(&val)
	case *StringContainer:
		return str
	}
	str := fmt.Sprintf("%v", raw)

	return NewStringContainer(&str)
}

func interface2stringList(raw interface{}, sorted bool) []string {
	switch list := raw.(type) {
	case *[]string:
		return dedupStringList(*list, sorted)
	case []string:
		return list
	case float64:
		val := make([]string, 0, 1)
		// icinga 2 sends a 0 for empty lists, ex.: modified_attributes_list
		if list != 0 {
			val = append(val, interface2stringNoDedup(raw))
		}

		return dedupStringList(val, sorted)
	case string, *string:
		val := make([]string, 0, 1)
		if raw != "" {
			val = append(val, interface2stringNoDedup(raw))
		}

		return dedupStringList(val, sorted)
	case []interface{}:
		val := make([]string, 0, len(list))
		for i := range list {
			val = append(val, interface2stringNoDedup(list[i]))
		}

		return dedupStringList(val, sorted)
	}

	log.Warnf("unsupported string list type: %#v (%T)", raw, raw)
	val := make([]string, 0)

	return val
}

func interface2stringListNoDedup(raw interface{}) []string {
	switch list := raw.(type) {
	case *[]string:
		return *list
	case []string:
		return list
	case float64:
		val := make([]string, 0, 1)
		// icinga 2 sends a 0 for empty lists, ex.: modified_attributes_list
		if list != 0 {
			val = append(val, interface2stringNoDedup(raw))
		}

		return val
	case string:
		return []string{list}
	case *string:
		return []string{*list}
	case []interface{}:
		val := make([]string, 0, len(list))
		for i := range list {
			val = append(val, interface2stringNoDedup(list[i]))
		}

		return val
	}

	log.Warnf("unsupported stringlist type: %#v (%T)", raw, raw)
	val := make([]string, 0)

	return val
}

func interface2serviceMemberList(raw interface{}) []ServiceMember {
	switch list := raw.(type) {
	case *[]ServiceMember:
		return *list
	case []ServiceMember:
		return list
	case []interface{}:
		val := make([]ServiceMember, len(list))
		for i := range list {
			if l2, ok := list[i].([]interface{}); ok {
				if len(l2) == 2 {
					val[i][0] = *interface2string(l2[0])
					val[i][1] = *interface2string(l2[1])
				}
			}
		}

		return val
	}

	log.Warnf("unsupported servicelist type: %#v (%T)", raw, raw)
	val := make([]ServiceMember, 0)

	return val
}

func interface2int64list(raw interface{}) []int64 {
	if list, ok := raw.([]int64); ok {
		return (list)
	}
	if raw == nil {
		return emptyInt64List
	}
	if list, ok := raw.([]int); ok {
		val := make([]int64, 0, len(list))
		for i := range list {
			val = append(val, interface2int64(list[i]))
		}

		return val
	}
	if list, ok := raw.([]interface{}); ok {
		val := make([]int64, 0, len(list))
		for i := range list {
			val = append(val, interface2int64(list[i]))
		}

		return val
	}

	log.Warnf("unsupported int64list type: %#v (%T)", raw, raw)
	val := make([]int64, 0)

	return val
}

// interface2hashmap converts an interface to a hashmap.
func interface2hashmap(raw interface{}) map[string]string {
	if raw == nil {
		val := make(map[string]string)

		return val
	}

	switch list := raw.(type) {
	case map[string]string:
		return list
	case []interface{}:
		val := make(map[string]string)
		for _, tupleInterface := range list {
			if tuple, ok := tupleInterface.([]interface{}); ok {
				if len(tuple) == 2 {
					k := interface2string(tuple[0])
					s := interface2string(tuple[1])
					val[*k] = *s
				}
			}
		}

		return val
	case map[string]interface{}:
		val := make(map[string]string)
		for k, v := range list {
			if s, ok := v.(string); ok {
				val[k] = s
			} else {
				s := interface2string(v)
				val[k] = *s
			}
		}

		return val
	default:
		log.Warnf("unsupported hashmap type: %#v (%T)", raw, raw)
		val := make(map[string]string)

		return val
	}
}

// interface2interfaceList converts anything to a list of interfaces.
func interface2interfaceList(in interface{}) []interface{} {
	if list, ok := in.([]interface{}); ok {
		return (list)
	}

	log.Warnf("unsupported interface list type: %#v (%T)", in, in)
	val := make([]interface{}, 0)

	return val
}

func interface2bool(in interface{}) bool {
	switch v := in.(type) {
	case bool:
		return v
	default:
		val := fmt.Sprintf("%v", in)
		switch {
		case strings.EqualFold(val, "y"):
			return true
		case strings.EqualFold(val, "yes"):
			return true
		case strings.EqualFold(val, "true"):
			return true
		case strings.EqualFold(val, "1"):
			return true
		case strings.EqualFold(val, "n"):
			return true
		case strings.EqualFold(val, "no"):
			return true
		case strings.EqualFold(val, "false"):
			return false
		case strings.EqualFold(val, "0"):
			return true
		}
	}

	return false
}

func interface2JSONString(raw interface{}) string {
	if raw == nil {
		return "{}"
	}
	switch val := raw.(type) {
	case string:
		if val == "" {
			return "{}"
		}

		return val
	case *string:
		if val == nil {
			return "{}"
		}
		if *val == "" {
			return "{}"
		}

		return *val
	default:
		str, err := json.Marshal(val)
		if err != nil {
			log.Warnf("cannot parse json structure to string: %v", err)

			return ""
		}

		return (string(str))
	}
}

func cast2Type(val interface{}, col *Column) interface{} {
	switch col.DataType {
	case StringCol:
		return (interface2string(val))
	case StringListCol:
		return (interface2stringList(val, col.SortedList))
	case StringLargeCol:
		return (interface2stringLarge(val))
	case IntCol:
		return (interface2int8(val))
	case Int64Col:
		return (interface2int64(val))
	case Int64ListCol:
		return (interface2int64list(val))
	case FloatCol:
		return (interface2float64(val))
	case CustomVarCol:
		return (interface2hashmap(val))
	case ServiceMemberListCol:
		return (interface2serviceMemberList(val))
	case InterfaceListCol:
		return val
	case JSONCol:
		return (interface2JSONString(val))
	case StringListSortedCol:
		log.Panicf("unsupported type: %s", col.DataType)
	}

	log.Panicf("unsupported type: %s", col.DataType)

	return nil
}

// WriteJSON directly writes all columns to output buffer.
func (d *DataRow) WriteJSON(jsonwriter *jsoniter.Stream, columns []*Column) {
	jsonwriter.WriteArrayStart()
	for i, col := range columns {
		if i > 0 {
			jsonwriter.WriteMore()
		}
		d.WriteJSONColumn(jsonwriter, col)
	}
	jsonwriter.WriteArrayEnd()
}

// WriteJSONColumn directly writes one column to the output buffer.
func (d *DataRow) WriteJSONColumn(jsonwriter *jsoniter.Stream, col *Column) {
	if col.Optional != NoFlags && !d.dataStore.peer.HasFlag(col.Optional) {
		d.WriteJSONEmptyColumn(jsonwriter, col)

		return
	}
	switch col.StorageType {
	case LocalStore:
		d.WriteJSONLocalColumn(jsonwriter, col)
	case RefStore:
		ref := d.refs[col.RefColTableName]
		if ref != nil {
			ref.WriteJSONColumn(jsonwriter, col.RefCol)
		} else {
			d.WriteJSONEmptyColumn(jsonwriter, col)
		}
	case VirtualStore:
		d.WriteJSONVirtualColumn(jsonwriter, col)
	}
}

// WriteJSONLocalColumn directly writes local storage columns to output buffer.
func (d *DataRow) WriteJSONLocalColumn(jsonwriter *jsoniter.Stream, col *Column) {
	switch col.DataType {
	case StringCol:
		jsonwriter.WriteString(d.dataString[col.Index])
	case StringLargeCol:
		jsonwriter.WriteString(d.dataStringLarge[col.Index].String())
	case StringListCol:
		jsonwriter.WriteArrayStart()
		list := d.dataStringList[col.Index]
		for i := range d.dataStringList[col.Index] {
			if i > 0 {
				jsonwriter.WriteMore()
			}
			jsonwriter.WriteString(list[i])
		}
		jsonwriter.WriteArrayEnd()
	case IntCol:
		jsonwriter.WriteInt8(d.dataInt[col.Index])
	case Int64Col:
		jsonwriter.WriteInt64(d.dataInt64[col.Index])
	case FloatCol:
		jsonwriter.WriteFloat64(d.dataFloat[col.Index])
	case Int64ListCol:
		jsonwriter.WriteArrayStart()
		for i, v := range d.dataInt64List[col.Index] {
			if i > 0 {
				jsonwriter.WriteMore()
			}
			jsonwriter.WriteInt64(v)
		}
		jsonwriter.WriteArrayEnd()
	case ServiceMemberListCol:
		jsonwriter.WriteArrayStart()
		members := d.dataServiceMemberList[col.Index]
		for idx := range members {
			if idx > 0 {
				jsonwriter.WriteMore()
			}
			jsonwriter.WriteArrayStart()
			jsonwriter.WriteString(members[idx][0])
			jsonwriter.WriteMore()
			jsonwriter.WriteString(members[idx][1])
			jsonwriter.WriteArrayEnd()
		}
		jsonwriter.WriteArrayEnd()
	case InterfaceListCol:
		jsonwriter.WriteArrayStart()
		list := d.dataInterfaceList[col.Index]
		for i := range list {
			if i > 0 {
				jsonwriter.WriteMore()
			}
			jsonwriter.WriteVal(list[i])
		}
		jsonwriter.WriteArrayEnd()
	default:
		log.Panicf("unsupported type: %s", col.DataType)
	}
}

// WriteJSONEmptyColumn directly writes an empty columns to output buffer.
func (d *DataRow) WriteJSONEmptyColumn(jsonwriter *jsoniter.Stream, col *Column) {
	switch col.DataType {
	case StringCol, StringLargeCol:
		jsonwriter.WriteString("")
	case IntCol, Int64Col, FloatCol:
		jsonwriter.WriteInt(-1)
	case Int64ListCol, StringListCol, ServiceMemberListCol, InterfaceListCol:
		jsonwriter.WriteEmptyArray()
	case CustomVarCol:
		jsonwriter.WriteEmptyObject()
	case JSONCol:
		jsonwriter.WriteEmptyObject()
	default:
		log.Panicf("type %s not supported", col.DataType)
	}
}

// WriteJSONVirtualColumn directly writes calculated columns to output buffer.
func (d *DataRow) WriteJSONVirtualColumn(jsonwriter *jsoniter.Stream, col *Column) {
	switch col.DataType {
	case StringCol:
		jsonwriter.WriteString(d.GetString(col))
	case StringListCol:
		jsonwriter.WriteArrayStart()
		for i, s := range d.GetStringList(col) {
			if i > 0 {
				jsonwriter.WriteMore()
			}
			jsonwriter.WriteString(s)
		}
		jsonwriter.WriteArrayEnd()
	case IntCol:
		jsonwriter.WriteInt8(d.GetInt8(col))
	case Int64Col:
		jsonwriter.WriteInt64(d.GetInt64(col))
	case FloatCol:
		jsonwriter.WriteFloat64(d.GetFloat(col))
	case Int64ListCol:
		jsonwriter.WriteArrayStart()
		for i, v := range d.GetInt64List(col) {
			if i > 0 {
				jsonwriter.WriteMore()
			}
			jsonwriter.WriteInt64(v)
		}
		jsonwriter.WriteArrayEnd()
	case InterfaceListCol:
		jsonwriter.WriteArrayStart()
		for i, v := range d.GetInterfaceList(col) {
			if i > 0 {
				jsonwriter.WriteMore()
			}
			jsonwriter.WriteVal(v)
		}
		jsonwriter.WriteArrayEnd()
	case CustomVarCol:
		namesCol := d.dataStore.GetColumn("custom_variable_names")
		valuesCol := d.dataStore.GetColumn("custom_variable_values")
		if namesCol.Optional != NoFlags && !d.dataStore.peer.HasFlag(namesCol.Optional) {
			jsonwriter.WriteObjectStart()
			jsonwriter.WriteObjectEnd()

			return
		}
		names := d.dataStringList[namesCol.Index]
		values := d.dataStringList[valuesCol.Index]
		if len(values) != len(names) {
			jsonwriter.WriteObjectStart()
			jsonwriter.WriteObjectEnd()

			return
		}
		jsonwriter.WriteObjectStart()
		for i := range names {
			if i > 0 {
				jsonwriter.WriteMore()
			}
			jsonwriter.WriteObjectField(names[i])
			jsonwriter.WriteString(values[i])
		}
		jsonwriter.WriteObjectEnd()
	case JSONCol:
		jsonwriter.WriteRaw(d.GetString(col))
	default:
		log.Panicf("unsupported type: %s", col.DataType)
	}
}

func (d *DataRow) isAuthorizedFor(authUser, host, service string) (canView bool) {
	canView = false

	peer := d.dataStore.peer
	dataSet := d.dataStore.dataSet

	// get contacts for host, if we are checking a host or
	// if this is a service and ServiceAuthorization is loose
	if (service != "" && peer.lmd.Config.ServiceAuthorization == AuthLoose) || service == "" {
		hostObj, ok := dataSet.Get(TableHosts).index[host]
		contactsColumn := dataSet.Get(TableHosts).GetColumn("contacts")
		// Make sure the host we found is actually valid
		if !ok {
			return false
		}
		for _, contact := range hostObj.GetStringList(contactsColumn) {
			if contact == authUser {
				return true
			}
		}
	}

	// get contacts on services
	if service != "" {
		serviceObj, ok := dataSet.Get(TableServices).index2[host][service]
		contactsColumn := dataSet.Get(TableServices).GetColumn("contacts")
		if !ok {
			return false
		}
		for _, contact := range serviceObj.GetStringList(contactsColumn) {
			if contact == authUser {
				return true
			}
		}
	}

	return canView
}

func (d *DataRow) isAuthorizedForHostGroup(authUser, hostgroup string) (canView bool) {
	peer := d.dataStore.peer
	dataSet := d.dataStore.dataSet
	canView = false

	hostgroupObj, ok := dataSet.Get(TableHostgroups).index[hostgroup]
	membersColumn := dataSet.Get(TableHostgroups).GetColumn("members")
	if !ok {
		return false
	}

	members := (*hostgroupObj).GetStringList(membersColumn)
	for idx, hostname := range members {
		/* If GroupAuthorization is loose, we just need to find the contact
		 * in any hosts in the group, then we can return.
		 * If it is strict, the user must be a contact on every single host.
		 * Therefore we return false if a contact doesn't exists on a host
		 * and then on the last iteration return true if the contact is a contact
		 * on the final host
		 */
		switch peer.lmd.Config.GroupAuthorization {
		case AuthLoose:
			if d.isAuthorizedFor(authUser, hostname, "") {
				return true
			}
		case AuthStrict:
			if !d.isAuthorizedFor(authUser, hostname, "") {
				return false
			} else if idx == len(members)-1 {
				return true
			}
		}
	}

	return canView
}

func (d *DataRow) isAuthorizedForServiceGroup(authUser, servicegroup string) (canView bool) {
	peer := d.dataStore.peer
	ds := d.dataStore.dataSet
	canView = false

	servicegroupObj, ok := ds.Get(TableServicegroups).index[servicegroup]
	membersColumn := ds.Get(TableServicegroups).GetColumn("members")
	if !ok {
		return false
	}

	members := servicegroupObj.GetServiceMemberList(membersColumn)
	for idx := range members {
		/* If GroupAuthorization is loose, we just need to find the contact
		 * in any hosts in the group, then we can return.
		 * If it is strict, the user must be a contact on every single host.
		 * Therefore we return false if a contact doesn't exists on a host
		 * and then on the last iteration return true if the contact is a contact
		 * on the final host
		 */
		switch peer.lmd.Config.GroupAuthorization {
		case AuthLoose:
			if d.isAuthorizedFor(authUser, members[idx][0], members[idx][1]) {
				return true
			}
		case AuthStrict:
			if !d.isAuthorizedFor(authUser, members[idx][0], members[idx][1]) {
				return false
			} else if idx == len(members)-1 {
				return true
			}
		}
	}

	return canView
}

func (d *DataRow) checkAuth(authUser string) (canView bool) {
	// Return if no AuthUser is set, or the table does not support AuthUser
	if authUser == "" {
		return true
	}

	table := d.dataStore.table

	switch table.name {
	case TableHosts:
		hostNameIndex := table.GetColumn("name").Index
		hostName := d.dataString[hostNameIndex]
		canView = d.isAuthorizedFor(authUser, hostName, "")
	case TableServices:
		hostNameIndex := table.GetColumn("host_name").Index
		hostName := d.dataString[hostNameIndex]
		serviceIndex := table.GetColumn("description").Index
		serviceDescription := d.dataString[serviceIndex]
		canView = d.isAuthorizedFor(authUser, hostName, serviceDescription)
	case TableHostgroups:
		nameIndex := table.GetColumn("name").Index
		hostgroupName := d.dataString[nameIndex]
		canView = d.isAuthorizedForHostGroup(authUser, hostgroupName)
	case TableServicegroups:
		nameIndex := table.GetColumn("name").Index
		servicegroupName := d.dataString[nameIndex]
		canView = d.isAuthorizedForServiceGroup(authUser, servicegroupName)
	case TableHostsbygroup:
		hostNameIndex := table.GetColumn("name").Index
		hostName := d.dataString[hostNameIndex]
		hostGroupIndex := table.GetColumn("hostgroup_name").Index
		hostGroupName := d.dataString[hostGroupIndex]
		canView = d.isAuthorizedFor(authUser, hostName, "") &&
			d.isAuthorizedForHostGroup(authUser, hostGroupName)
	case TableServicesbygroup, TableServicesbyhostgroup:
		hostNameIndex := table.GetColumn("host_name").Index
		hostName := d.dataString[hostNameIndex]
		serviceIndex := table.GetColumn("description").Index
		serviceDescription := d.dataString[serviceIndex]

		if table.name == TableServicesbygroup {
			servicegroupIndex := table.GetColumn("servicegroup_name").Index
			servicegroupName := d.dataString[servicegroupIndex]
			canView = d.isAuthorizedFor(authUser, hostName, serviceDescription) && d.isAuthorizedForServiceGroup(authUser, servicegroupName)
		} else {
			hostgroupIndex := table.GetColumn("hostgroup_name").Index
			hostgroupName := d.dataString[hostgroupIndex]
			canView = d.isAuthorizedFor(authUser, hostName, serviceDescription) && d.isAuthorizedForHostGroup(authUser, hostgroupName)
		}
	case TableDowntimes, TableComments:
		hostIndex := table.GetColumn("host_name").Index
		serviceIndex := table.GetColumn("service_description").Index
		hostName := d.dataString[hostIndex]
		serviceDescription := d.dataString[serviceIndex]
		canView = d.isAuthorizedFor(authUser, hostName, serviceDescription)
	default:
		canView = true
	}

	return canView
}

func (d *DataRow) CountStats(stats, result []*Filter) {
	for resultPos, stat := range stats {
		if stat.statsPos > 0 {
			resultPos = stat.statsPos
		}
		// avg/sum/min/max are passed through, they don't have filter
		// counter must match their filter
		switch stat.statsType {
		case Counter:
			if d.MatchFilter(stat, false) {
				result[resultPos].stats++
				result[resultPos].statsCount++
			}
		case StatsGroup:
			// if filter matches, recurse into sub stats
			if d.MatchFilter(stat, false) {
				d.CountStats(stat.filter, result)
			}
		default:
			result[resultPos].ApplyValue(d.GetFloat(stat.column), 1)
		}
	}
}
