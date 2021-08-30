package main

import (
	"fmt"
	"strings"
)

// VirtualColumnResolveFunc is used to define the virtual key mapping in the VirtualColumnMap
type VirtualColumnResolveFunc func(d *DataRow, col *Column) interface{}

// VirtualColumnMapEntry is used to define the virtual key mapping in the VirtualColumnMap
type VirtualColumnMapEntry struct {
	noCopy      noCopy
	Name        string
	StatusKey   PeerStatusKey
	ResolveFunc VirtualColumnResolveFunc
}

// VirtualColumnList maps the virtual columns with the peer status map entry.
// Must have either a StatusKey or a ResolveFunc set
var VirtualColumnList = []VirtualColumnMapEntry{
	// access things from the peer status by StatusKey
	{Name: "key", StatusKey: PeerKey},
	{Name: "name", StatusKey: PeerName},
	{Name: "addr", StatusKey: PeerAddr},
	{Name: "status", StatusKey: PeerState},
	{Name: "bytes_send", StatusKey: BytesSend},
	{Name: "bytes_received", StatusKey: BytesReceived},
	{Name: "queries", StatusKey: Queries},
	{Name: "last_error", StatusKey: LastError},
	{Name: "last_online", StatusKey: LastOnline},
	{Name: "last_update", StatusKey: LastUpdate},
	{Name: "response_time", StatusKey: ResponseTime},
	{Name: "idling", StatusKey: Idling},
	{Name: "last_query", StatusKey: LastQuery},
	{Name: "section", StatusKey: Section},
	{Name: "parent", StatusKey: PeerParent},
	{Name: "configtool", StatusKey: ConfigTool},
	{Name: "thruk", StatusKey: ThrukExtras},
	{Name: "federation_key", StatusKey: SubKey},
	{Name: "federation_name", StatusKey: SubName},
	{Name: "federation_addr", StatusKey: SubAddr},
	{Name: "federation_type", StatusKey: SubType},

	// calculated columns by ResolveFunc
	{Name: "lmd_last_cache_update", ResolveFunc: func(d *DataRow, _ *Column) interface{} { return d.LastUpdate }},
	{Name: "lmd_version", ResolveFunc: func(_ *DataRow, _ *Column) interface{} { return fmt.Sprintf("%s-%s", NAME, Version()) }},
	{Name: "state_order", ResolveFunc: VirtualColStateOrder},
	{Name: "last_state_change_order", ResolveFunc: VirtualColLastStateChangeOrder},
	{Name: "has_long_plugin_output", ResolveFunc: VirtualColHasLongPluginOutput},
	{Name: "services_with_state", ResolveFunc: VirtualColServicesWithInfo},
	{Name: "services_with_info", ResolveFunc: VirtualColServicesWithInfo},
	{Name: "comments_with_info", ResolveFunc: VirtualColCommentsWithInfo},
	{Name: "downtimes_with_info", ResolveFunc: VirtualColDowntimesWithInfo},
	{Name: "members_with_state", ResolveFunc: VirtualColMembersWithState},
	{Name: "custom_variables", ResolveFunc: VirtualColCustomVariables},
	{Name: "total_services", ResolveFunc: VirtualColTotalServices},
	{Name: "flags", ResolveFunc: VirtualColFlags},
	{Name: "localtime", ResolveFunc: VirtualColLocaltime},
	{Name: "empty", ResolveFunc: func(_ *DataRow, _ *Column) interface{} { return "" }}, // return empty string as placeholder for nonexisting columns
}

// VirtualColumnMap maps is the lookup map for the VirtualColumnList
var VirtualColumnMap = map[string]*VirtualColumnMapEntry{}

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
	// JSONCol is used for generic json data columns (handled as string).
	JSONCol
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
	// VirtualStore is calculated on the fly
	VirtualStore
)

// OptionalFlags is used to set flags for optional columns.
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

	// HasLastUpdateColumn flag is set if the remote site has a last_update column for hosts and services
	HasLastUpdateColumn

	// HasLMDLastCacheUpdateColumn flag is set if the remote site has a lmd_last_cache_update column for hosts and services
	HasLMDLastCacheUpdateColumn

	// HasLocaltimeColumn flag is set if the remote site has a localtime column
	HasLocaltimeColumn

	// HasCheckFreshnessColumn flag is set if the remote site has a check_freshness column for services
	HasCheckFreshnessColumn

	// HasEventHandlerColumn flag is set if the remote site has a event_handler column for hosts
	HasEventHandlerColumn

	// HasStalenessColumn flag is set if the remote site has a staleness column for hosts and services
	HasStalenessColumn

	// HasServiceParentsColumn flag is set if remote site support service parents column
	HasServiceParentsColumn
)

// OptionalFlagsStrings maps available backend flags to their string value
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
	HasLocaltimeColumn:          "HasLocaltimeColumn",
	HasCheckFreshnessColumn:     "HasCheckFreshnessColumn",
	HasEventHandlerColumn:       "HasEventHandlerColumn",
	HasStalenessColumn:          "HasStalenessColumn",
	HasServiceParentsColumn:     "HasServiceParentsColumn",
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
	Name            string                 // name and primary key
	Description     string                 // human description
	DataType        DataType               // Type of this column
	FetchType       FetchType              // flag wether this columns needs to be updated
	StorageType     StorageType            // flag how this column is stored
	Optional        OptionalFlags          // flags if this column is used for certain backends only
	Index           int                    // position in the DataRow data* fields
	RefCol          *Column                // reference to column in other table, ex.: host_alias
	RefColTableName TableName              // shortcut to Column.RefCol.Table.Name
	Table           *Table                 // reference to the table holding this column
	VirtualMap      *VirtualColumnMapEntry // reference to resolver for virtual columns
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
	if col.StorageType == VirtualStore {
		col.VirtualMap = VirtualColumnMap[name]
		if col.VirtualMap == nil {
			log.Panicf("missing VirtualMap for %s in %s", col.Name, table.Name)
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
	case CustomVarCol:
		return emptyStringMap
	case JSONCol:
		return "{}"
	default:
		log.Panicf("type %s not supported", c.DataType)
	}
	return ""
}
