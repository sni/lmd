package lmd

import (
	"fmt"
	"strings"
)

// VirtualColumnResolveFunc is used to define the virtual key mapping in the VirtualColumnMap.
type VirtualColumnResolveFunc func(p *Peer, d *DataRow, col *Column) interface{}

// VirtualColumnMapEntry is used to define the virtual key mapping in the VirtualColumnMap.
type VirtualColumnMapEntry struct {
	noCopy      noCopy
	ResolveFunc VirtualColumnResolveFunc
	Name        string
	StatusKey   PeerStatusKey
}

// VirtualColumnList maps the virtual columns with the peer status map entry.
// Must have either a StatusKey or a ResolveFunc set.
var VirtualColumnList = []VirtualColumnMapEntry{
	// access things from the peer status by StatusKey
	{Name: "key", StatusKey: PeerKey},
	{Name: "name", StatusKey: PeerName},
	{Name: "section", StatusKey: Section},
	{Name: "parent", StatusKey: PeerParent},

	// fetched on the fly
	{Name: "addr", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.PeerAddr.Get() }},
	{Name: "status", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.PeerState.Get() }},
	{Name: "bytes_send", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.BytesSend.Load() }},
	{Name: "bytes_received", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.BytesReceived.Load() }},
	{Name: "queries", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.Queries.Load() }},
	{Name: "last_error", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.LastError.Get() }},
	{Name: "last_online", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.LastOnline.Get() }},
	{Name: "last_update", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.LastUpdate.Get() }},
	{Name: "response_time", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.ResponseTime.Get() }},
	{Name: "idling", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.Idling.Load() }},
	{Name: "last_query", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.LastQuery.Get() }},
	{Name: "configtool", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.ConfigTool.Get() }},
	{Name: "thruk", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.ThrukExtras.Get() }},
	{Name: "federation_key", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.SubKey.Get() }},
	{Name: "federation_name", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.SubName.Get() }},
	{Name: "federation_addr", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.SubAddr.Get() }},
	{Name: "federation_type", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.SubType.Get() }},
	{Name: "federation_version", ResolveFunc: func(p *Peer, _ *DataRow, _ *Column) interface{} { return p.SubVersion.Get() }},

	// calculated columns by ResolveFunc
	{Name: "lmd_last_cache_update", ResolveFunc: func(_ *Peer, d *DataRow, _ *Column) interface{} { return d.LastUpdate }},
	{Name: "lmd_version", ResolveFunc: func(_ *Peer, _ *DataRow, _ *Column) interface{} { return fmt.Sprintf("%s-%s", NAME, Version()) }},
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
	{Name: "empty", ResolveFunc: func(_ *Peer, _ *DataRow, _ *Column) interface{} { return "" }}, // return empty string as placeholder for nonexisting columns
}

// VirtualColumnMap maps is the lookup map for the VirtualColumnList.
var VirtualColumnMap = map[string]*VirtualColumnMapEntry{}

// ServiceMember is a host_name / description pair.
type ServiceMember [2]string

// FetchType defines if and how the column is updated.
//
//go:generate stringer -type=FetchType
type FetchType uint8

// placeholder to return in GetEmptyValue, no need to create empty lists over and over.
var (
	emptyInterfaceList = make([]interface{}, 0)
	emptyStringMap     = make(map[string]string)
	emptyInt64List     = []int64{}
)

const (
	// Static is used for all columns which are updated once at start.
	Static FetchType = iota + 1
	// Dynamic columns are updated periodically.
	Dynamic
	// None columns are never updated and either calculated on the fly.
	None
)

// DataType defines the data type of a column.
//
//go:generate stringer -type=DataType
type DataType uint16

const (
	// StringCol is used for string columns.
	StringCol DataType = iota + 1
	// StringListCol is used for string list columns.
	StringListCol
	// IntCol is used for small integer columns.
	IntCol
	// Int64Col is used for large integer columns.
	Int64Col
	// Int64ListCol is used for integer list columns.
	Int64ListCol
	// FloatCol is used for float columns.
	FloatCol
	// JSONCol is used for generic json data columns (handled as string).
	JSONCol
	// CustomVarCol is a list of custom variables.
	CustomVarCol
	// ServiceMemberListCol is a list of host_name/servicename pairs.
	ServiceMemberListCol
	// InterfaceListCol is a list of arbitrary data.
	InterfaceListCol
	// StringLargeCol is used for large strings.
	StringLargeCol
)

// StorageType defines how this column is stored
//
//go:generate stringer -type=StorageType
type StorageType uint8

const (
	// LocalStore columns are store in the DataRow.data* fields.
	LocalStore StorageType = iota + 1
	// RefStore are referenced columns.
	RefStore
	// VirtualStore is calculated on the fly.
	VirtualStore
)

// OptionalFlags is used to set flags for optional columns.
type OptionalFlags uint32

const (
	// NoFlags is set if there are no flags at all.
	NoFlags OptionalFlags = 0

	// LMD flag is set if the remote site is a LMD backend.
	LMD OptionalFlags = 1 << iota

	// MultiBackend flag is set if the remote connection returns more than one site.
	MultiBackend

	// LMDSub is a sub peer from within a remote LMD connection.
	LMDSub

	// HTTPSub is a sub peer from within a remote HTTP connection (MultiBackend).
	HTTPSub

	// Shinken flag is set if the remote site is a shinken installation.
	Shinken

	// Icinga2 flag is set if the remote site is a icinga 2 installation.
	Icinga2

	// Naemon flag is set if the remote site is a naemon installation.
	Naemon

	// HasDependencyColumn flag is set if the remote site has depends_exec and depends_notify columns.
	HasDependencyColumn

	// HasLastUpdateColumn flag is set if the remote site has a last_update column for hosts and services.
	HasLastUpdateColumn

	// HasLMDLastCacheUpdateColumn flag is set if the remote site has a lmd_last_cache_update column for hosts and services.
	HasLMDLastCacheUpdateColumn

	// HasLocaltimeColumn flag is set if the remote site has a localtime column.
	HasLocaltimeColumn

	// HasCheckFreshnessColumn flag is set if the remote site has a check_freshness column for services.
	HasCheckFreshnessColumn

	// HasEventHandlerColumn flag is set if the remote site has a event_handler column for hosts.
	HasEventHandlerColumn

	// HasStalenessColumn flag is set if the remote site has a staleness column for hosts and services.
	HasStalenessColumn

	// HasServiceParentsColumn flag is set if remote site support service parents column.
	HasServiceParentsColumn

	// HasContactsGroupColumn flag is set if remote site support contacts groups column.
	HasContactsGroupColumn

	// HasContactsCommandsColumn flag is set if remote site support contacts notification commands column.
	HasContactsCommandsColumn
)

// OptionalFlagsStrings maps available backend flags to their string value.
var OptionalFlagsStrings = []struct { //nolint:govet // no need to align this struct, use only once
	flag OptionalFlags
	name string
}{
	{LMD, "LMD"},
	{MultiBackend, "MultiBackend"},
	{LMDSub, "LMDSub"},
	{HTTPSub, "HTTPSub"},
	{Shinken, "Shinken"},
	{Icinga2, "Icinga2"},
	{Naemon, "Naemon"},
	{HasDependencyColumn, "HasDependencyColumn"},
	{HasLastUpdateColumn, "HasLastUpdateColumn"},
	{HasLMDLastCacheUpdateColumn, "HasLMDLastCacheUpdateColumn"},
	{HasLocaltimeColumn, "HasLocaltimeColumn"},
	{HasCheckFreshnessColumn, "HasCheckFreshnessColumn"},
	{HasEventHandlerColumn, "HasEventHandlerColumn"},
	{HasStalenessColumn, "HasStalenessColumn"},
	{HasServiceParentsColumn, "HasServiceParentsColumn"},
	{HasContactsGroupColumn, "HasContactsGroupColumn"},
	{HasContactsCommandsColumn, "HasContactsCommandsColumn"},
}

// Load set flags from list of strings.
func (f *OptionalFlags) Load(list []string) {
	f.Clear()
	for _, flag := range list {
		for _, opt := range OptionalFlagsStrings {
			if strings.EqualFold(flag, opt.name) {
				f.SetFlag(opt.flag)

				break
			}
		}
	}
}

// String returns the string representation of used flags.
func (f *OptionalFlags) String() string {
	if *f == NoFlags {
		return "[<none>]"
	}

	return ("[" + strings.Join(f.List(), ", ") + "]")
}

// List returns a string list of used flags.
func (f *OptionalFlags) List() (list []string) {
	list = make([]string, 0)
	if *f == NoFlags {
		return
	}
	for _, fl := range OptionalFlagsStrings {
		if f.HasFlag(fl.flag) {
			list = append(list, fl.name)
		}
	}

	return
}

// HasFlag returns true if flags are present.
func (f *OptionalFlags) HasFlag(flag OptionalFlags) bool {
	if flag == 0 {
		return true
	}
	if *f&flag != 0 {
		return true
	}

	return false
}

// SetFlag sets a flag.
func (f *OptionalFlags) SetFlag(flag OptionalFlags) {
	*f |= flag
}

// Clear removes all flags.
func (f *OptionalFlags) Clear() {
	*f = NoFlags
}

// Column is the definition of a single column within a DataRow.
type Column struct {
	noCopy          noCopy
	RefCol          *Column                // reference to column in other table, ex.: host_alias
	Table           *Table                 // // reference to the table holding this column
	VirtualMap      *VirtualColumnMapEntry // reference to resolver for virtual columns
	Name            string                 // name and primary key
	Description     string                 // human description
	Index           int                    // position in datastore
	RefColTableName TableName              // shortcut to Column.RefCol.Table.Name
	Optional        OptionalFlags          // flags if this column is used for certain backends only
	DataType        DataType               // Type of this column
	FetchType       FetchType              // flag wether this columns needs to be updated
	StorageType     StorageType            // flag how this column is stored
}

// NewColumn adds a column object.
func NewColumn(table *Table, name string, storage StorageType, update FetchType, datatype DataType, restrict OptionalFlags, refCol *Column, descr string) {
	col := &Column{
		Table:       table,
		Name:        name,
		Index:       -1,
		Description: descr,
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
			log.Panicf("missing VirtualMap for %s in %s", col.Name, table.Name.String())
		}
	}
	if col.StorageType == RefStore {
		if col.RefCol == nil {
			log.Panicf("missing RefCol for %s in %s", col.Name, table.Name.String())
		}
		col.RefColTableName = refCol.Table.Name
	}
	if table.ColumnsIndex == nil {
		table.ColumnsIndex = make(map[string]*Column)
	}
	table.ColumnsIndex[col.Name] = col
	table.Columns = append(table.Columns, col)
}

// String returns the string representation of a column list.
func (c *Column) String() string {
	return c.Name
}

// GetEmptyValue returns an empty placeholder representation for the given column type.
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
