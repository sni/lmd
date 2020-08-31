package main

import (
	"fmt"
	"strings"
)

// VirtColumnMapEntry is used to define the virtual key mapping in the VirtColumnMap
type VirtColumnResolveFunc func(d *DataRow, col *Column) interface{}

// VirtColumnMapEntry is used to define the virtual key mapping in the VirtColumnMap
type VirtColumnMapEntry struct {
	noCopy     noCopy
	Name       string
	StatusKey  PeerStatusKey
	ResolvFunc VirtColumnResolveFunc
}

// VirtColumnList maps the virtual columns with the peer status map entry.
// Must have either a StatusKey or a ResolvFunc set
var VirtColumnList = []VirtColumnMapEntry{
	// access things from the peer status by StatusKey
	{Name: "key", StatusKey: PeerKey},
	{Name: "name", StatusKey: PeerName},
	{Name: "addr", StatusKey: PeerAddr},
	{Name: "status", StatusKey: PeerState},
	{Name: "bytes_send", StatusKey: BytesSend},
	{Name: "bytes_received", StatusKey: BytesReceived},
	{Name: "queries", StatusKey: Querys},
	{Name: "last_error", StatusKey: LastError},
	{Name: "last_online", StatusKey: LastOnline},
	{Name: "last_update", StatusKey: LastUpdate},
	{Name: "response_time", StatusKey: ReponseTime},
	{Name: "idling", StatusKey: Idling},
	{Name: "last_query", StatusKey: LastQuery},
	{Name: "section", StatusKey: Section},
	{Name: "parent", StatusKey: PeerParent},
	{Name: "configtool", StatusKey: ConfigTool},
	{Name: "federation_key", StatusKey: SubKey},
	{Name: "federation_name", StatusKey: SubName},
	{Name: "federation_addr", StatusKey: SubAddr},
	{Name: "federation_type", StatusKey: SubType},

	// calculated columns by ResolvFunc
	{Name: "lmd_last_cache_update", ResolvFunc: func(d *DataRow, _ *Column) interface{} { return d.LastUpdate }},
	{Name: "lmd_version", ResolvFunc: func(_ *DataRow, _ *Column) interface{} { return fmt.Sprintf("%s-%s", NAME, Version()) }},
	{Name: "state_order", ResolvFunc: VirtColStateOrder},
	{Name: "last_state_change_order", ResolvFunc: VirtColLastStateChangeOrder},
	{Name: "has_long_plugin_output", ResolvFunc: VirtColHasLongPluginOutput},
	{Name: "services_with_state", ResolvFunc: VirtColServicesWithInfo},
	{Name: "services_with_info", ResolvFunc: VirtColServicesWithInfo},
	{Name: "comments", ResolvFunc: VirtColComments},
	{Name: "comments_with_info", ResolvFunc: VirtColCommentsWithInfo},
	{Name: "downtimes", ResolvFunc: VirtColDowntimes},
	{Name: "downtimes_with_info", ResolvFunc: VirtColDowntimesWithInfo},
	{Name: "members_with_state", ResolvFunc: VirtColMembersWithState},
	{Name: "custom_variables", ResolvFunc: VirtColCustomVariables},
	{Name: "total_services", ResolvFunc: VirtColTotalServices},
	{Name: "empty", ResolvFunc: func(_ *DataRow, _ *Column) interface{} { return "" }}, // return empty string as placeholder for nonexisting columns
}

// VirtColumnMap maps is the lookup map for the VirtColumnList
var VirtColumnMap = map[string]*VirtColumnMapEntry{}

// ServiceMember is a host_name / description pair
type ServiceMember [2]string

// FetchType defines if and how the column is updated.
//go:generate stringer -type=FetchType
type FetchType uint8

// placeholder to return in GetEmptyValue, no need to create empty lists over and over
var emptyInterfaceList = make([]interface{}, 0)
var emptyStringMap = make(map[string]string)
var emptyInt64List = []int64{}

const (
	// Static is used for all columns which are updated once at start.
	Static FetchType = iota + 1
	// Dynamic columns are updated periodically.
	Dynamic
	// None columns are never updated and either calculated on the fly
	None
)

// DataType defines the data type of a column.
//go:generate stringer -type=DataType
type DataType uint16

const (
	// StringCol is used for string columns.
	StringCol DataType = iota + 1
	// StringListCol is used for string list columns.
	StringListCol
	// IntCol is used for integer columns.
	IntCol
	// Int64Col is used for large integer columns.
	Int64Col
	// Int64ListCol is used for integer list columns.
	Int64ListCol
	// FloatCol is used for float columns.
	FloatCol
	// HashMapCol is used for generic hash map columns.
	HashMapCol
	// CustomVarCol is a list of custom variables
	CustomVarCol
	// ServiceMemberListCol is a list of host_name/servicename pairs
	ServiceMemberListCol
	// InterfaceListCol is a list of arbitrary data
	InterfaceListCol
	// StringLargeCol is used for large strings
	StringLargeCol
)

// StorageType defines how this column is stored
//go:generate stringer -type=StorageType
type StorageType uint8

const (
	// LocalStore columns are store in the DataRow.data* fields
	LocalStore StorageType = iota + 1
	// RefStore are referenced columns
	RefStore
	// VirtStore are calculated on the fly
	VirtStore
)

// OptionalFlags is used to set flags for optionial columns.
type OptionalFlags uint32

const (
	// NoFlags is set if there are no flags at all.
	NoFlags OptionalFlags = 0

	// LMD flag is set if the remote site is a LMD backend.
	LMD OptionalFlags = 1 << iota

	// MultiBackend flag is set if the remote connection returns more than one site
	MultiBackend

	// LMDSub is a sub peer from within a remote LMD connection
	LMDSub

	// HTTPSub is a sub peer from within a remote HTTP connection (MultiBackend)
	HTTPSub

	// Shinken flag is set if the remote site is a shinken installation.
	Shinken

	// Icinga2 flag is set if the remote site is a icinga 2 installation.
	Icinga2

	// Naemon flag is set if the remote site is a naemon installation.
	Naemon

	// HasDependencyColumn flag is set if the remote site has depends_exec and depends_notify columns
	HasDependencyColumn

	// HasLastUpdate flag is set if the remote site has a last_update column for hosts and services
	HasLastUpdateColumn

	// HasLMDLastUpdate flag is set if the remote site has a lmd_last_cache_update column for hosts and services
	HasLMDLastCacheUpdateColumn
)

var OptionalFlagsStrings = map[OptionalFlags]string{
	LMD:                         "LMD",
	MultiBackend:                "MultiBackend",
	LMDSub:                      "LMDSub",
	HTTPSub:                     "HTTPSub",
	Shinken:                     "Shinken",
	Icinga2:                     "Icinga2",
	Naemon:                      "Naemon",
	HasDependencyColumn:         "HasDependencyColumn",
	HasLastUpdateColumn:         "HasLastUpdateColumn",
	HasLMDLastCacheUpdateColumn: "HasLMDLastCacheUpdateColumn",
}

// String returns the string representation of used flags
func (f *OptionalFlags) String() string {
	if *f == NoFlags {
		return "[<none>]"
	}
	return ("[" + strings.Join(f.List(), ", ") + "]")
}

// List returns a string list of used flags
func (f *OptionalFlags) List() (list []string) {
	list = make([]string, 0)
	if *f == NoFlags {
		return
	}
	for fl, name := range OptionalFlagsStrings {
		if f.HasFlag(fl) {
			list = append(list, name)
		}
	}
	return
}

// HasFlag returns true if flags are present
func (f *OptionalFlags) HasFlag(flag OptionalFlags) bool {
	if flag == 0 {
		return true
	}
	if *f&flag != 0 {
		return true
	}
	return false
}

// SetFlag set a flag
func (f *OptionalFlags) SetFlag(flag OptionalFlags) {
	*f |= flag
}

// Clear removes all flags
func (f *OptionalFlags) Clear() {
	*f = NoFlags
}

// Column is the definition of a single column within a DataRow.
type Column struct {
	noCopy          noCopy
	Name            string              // name and primary key
	Description     string              // human description
	DataType        DataType            // Type of this column
	FetchType       FetchType           // flag wether this columns needs to be updated
	StorageType     StorageType         // flag how this column is stored
	Optional        OptionalFlags       // flags if this column is used for certain backends only
	Index           int                 // position in the DataRow data* fields
	RefCol          *Column             // reference to column in other table, ex.: host_alias
	RefColTableName TableName           // shortcut to Column.RefCol.Table.Name
	Table           *Table              // reference to the table holding this column
	VirtMap         *VirtColumnMapEntry // reference to resolver for virtual columns
}

// NewColumn adds a column object.
func NewColumn(table *Table, name string, storage StorageType, update FetchType, datatype DataType, restrict OptionalFlags, refCol *Column, description string) {
	col := &Column{
		Table:       table,
		Name:        name,
		Description: description,
		StorageType: storage,
		FetchType:   update,
		DataType:    datatype,
		RefCol:      refCol,
		Optional:    restrict,
	}
	if col.Table == nil {
		log.Panicf("missing table for %s", col.Name)
	}
	if col.StorageType == VirtStore {
		col.VirtMap = VirtColumnMap[name]
		if col.VirtMap == nil {
			log.Panicf("missing VirtMap for %s in %s", col.Name, table.Name)
		}
	}
	if col.StorageType == RefStore {
		if col.RefCol == nil {
			log.Panicf("missing RefCol for %s in %s", col.Name, table.Name)
		}
		col.RefColTableName = refCol.Table.Name
	}
	if table.ColumnsIndex == nil {
		table.ColumnsIndex = make(map[string]*Column)
	}
	table.ColumnsIndex[col.Name] = col
	table.Columns = append(table.Columns, col)
}

// String returns the string representation of a column list
func (c *Column) String() string {
	return c.Name
}

// GetEmptyValue returns an empty placeholder representation for the given column type
func (c *Column) GetEmptyValue() interface{} {
	switch c.DataType {
	case StringCol, StringLargeCol:
		return ""
	case IntCol, Int64Col, FloatCol:
		return -1
	case Int64ListCol:
		return emptyInt64List
	case StringListCol, ServiceMemberListCol, InterfaceListCol:
		return emptyInterfaceList
	case HashMapCol, CustomVarCol:
		return emptyStringMap
	default:
		log.Panicf("type %s not supported", c.DataType)
	}
	return ""
}
