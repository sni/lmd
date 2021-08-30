package main

import (
	"fmt"
	"strings"
)

// TableRef contains data for referenced tables
type TableRef struct {
	noCopy  noCopy
	Table   *Table     // name of the table itself, ex.: hosts table
	Columns ColumnList // local column(s) which holds the values to determince the ID of the referenced item, ex.: host_name
}

// TableName contains all table names
type TableName int

// available table names
const (
	TableNone TableName = iota
	TableBackends
	TableSites
	TableColumns
	TableTables
	TableStatus
	TableTimeperiods
	TableContacts
	TableContactgroups
	TableCommands
	TableHosts
	TableHostgroups
	TableServices
	TableServicegroups
	TableComments
	TableDowntimes
	TableLog
	TableHostsbygroup
	TableServicesbygroup
	TableServicesbyhostgroup
)

// TableNameMapping contains TableName to string mapping
var TableNameMapping = map[TableName]string{
	TableBackends:            "backends",
	TableSites:               "sites",
	TableColumns:             "columns",
	TableTables:              "tables",
	TableStatus:              "status",
	TableTimeperiods:         "timeperiods",
	TableContacts:            "contacts",
	TableContactgroups:       "contactgroups",
	TableCommands:            "commands",
	TableHosts:               "hosts",
	TableHostgroups:          "hostgroups",
	TableServices:            "services",
	TableServicegroups:       "servicegroups",
	TableComments:            "comments",
	TableDowntimes:           "downtimes",
	TableLog:                 "log",
	TableHostsbygroup:        "hostsbygroup",
	TableServicesbygroup:     "servicesbygroup",
	TableServicesbyhostgroup: "servicesbyhostgroup",
}

// TableNameLookup is a hash map of string to Table object
var TableNameLookup = map[string]TableName{}

// PeerLockMode sets full or simple lock mode
type PeerLockMode int

const (
	// PeerLockModeSimple locks each peer.Status access separately
	PeerLockModeSimple PeerLockMode = iota

	// PeerLockModeFull locks peer once before createing the result
	PeerLockModeFull
)

// InitTableNames initializes the table name lookup map
func InitTableNames() {
	for t, s := range TableNameMapping {
		TableNameLookup[s] = t
	}
}

// NewTableName returns a table for given name or an error
func NewTableName(name string) (TableName, error) {
	if v, ok := TableNameLookup[strings.ToLower(name)]; ok {
		return v, nil
	}
	return TableNone, fmt.Errorf("table %s does not exist", name)
}

// String returns the name of this table as String
func (t *TableName) String() string {
	if s, ok := TableNameMapping[*t]; ok {
		return s
	}
	log.Panicf("unsupported tablename: %v", t)
	return ""
}

// Table defines available columns and table options
type Table struct {
	noCopy          noCopy
	Name            TableName
	Columns         ColumnList
	ColumnsIndex    map[string]*Column // access columns by name
	PassthroughOnly bool               // flag wether table will be cached or simply passed through to remote sites
	WorksUnlocked   bool               // flag wether locking the peer.DataLock can be skipped to answer the query
	PrimaryKey      []string
	RefTables       []TableRef // referenced tables
	Virtual         VirtualStoreResolveFunc
	DefaultSort     []string     // columns used to sort if nothing is specified
	PeerLockMode    PeerLockMode // should the peer be locked once for the complete result or on each access
}

// GetColumn returns a column for given name or nil if not found
func (t *Table) GetColumn(name string) *Column {
	return t.ColumnsIndex[name]
}

// GetColumnWithFallback returns a column for list of names, returns empty column as fallback
func (t *Table) GetColumnWithFallback(name string) *Column {
	col, ok := t.ColumnsIndex[name]
	if ok {
		return col
	}
	if !fixBrokenClientsRequestColumn(&name, t.Name) {
		return t.GetEmptyColumn()
	}
	return t.ColumnsIndex[name]
}

// GetColumns returns a column list for list of names
func (t *Table) GetColumns(names []string) ColumnList {
	columns := make(ColumnList, 0, len(names))
	for i := range names {
		columns = append(columns, t.ColumnsIndex[names[i]])
	}
	return columns
}

// GetEmptyColumn returns an empty column
func (t *Table) GetEmptyColumn() *Column {
	return &Column{
		Name:        "empty",
		Description: "placeholder for unknown columns",
		Table:       t,
		DataType:    StringCol,
		StorageType: VirtualStore,
		FetchType:   None,
		VirtualMap:  VirtualColumnMap["empty"],
	}
}

// AddColumn adds a new column
func (t *Table) AddColumn(name string, update FetchType, datatype DataType, description string) {
	NewColumn(t, name, LocalStore, update, datatype, NoFlags, nil, description)
}

// AddExtraColumn adds a new column with extra attributes
func (t *Table) AddExtraColumn(name string, storage StorageType, update FetchType, datatype DataType, restrict OptionalFlags, description string) {
	NewColumn(t, name, storage, update, datatype, restrict, nil, description)
}

// AddPeerInfoColumn adds a new column related to peer information
func (t *Table) AddPeerInfoColumn(name string, datatype DataType, description string) {
	NewColumn(t, name, VirtualStore, None, datatype, NoFlags, nil, description)
}

// AddRefColumns adds a reference column.
// tableName: name of the referenced table
// Prefix: column prefix for the added columns
// LocalName: column(s) which holds the reference value(s)
func (t *Table) AddRefColumns(tableName TableName, prefix string, localName []string) {
	refTable, Ok := Objects.Tables[tableName]
	if !Ok {
		log.Panicf("no such reference %s from column %s", tableName, strings.Join(localName, ","))
	}

	t.RefTables = append(t.RefTables, TableRef{Table: refTable, Columns: t.GetColumns(localName)})

	// add fake columns for all columns from the referenced table
	for i := range Objects.Tables[tableName].Columns {
		col := Objects.Tables[tableName].Columns[i]
		// skip peer_key and such things from ref table
		if col.StorageType == RefStore {
			continue
		}
		refColName := prefix + "_" + col.Name
		if prefix == "" {
			refColName = col.Name
		}
		if _, ok := t.ColumnsIndex[refColName]; ok {
			continue
		}
		NewColumn(t, refColName, RefStore, None, col.DataType, col.Optional, col, col.Description)
	}
}

// SetColumnIndex sets index for all columns
func (t *Table) SetColumnIndex() {
	flagCombos := make(map[OptionalFlags]bool)
	for i := range t.Columns {
		col := t.Columns[i]
		flagCombos[col.Optional] = true
	}
	for flags := range flagCombos {
		indexes := make(map[DataType]int)
		for i := range t.Columns {
			col := t.Columns[i]
			if col.Optional != NoFlags && !flags.HasFlag(col.Optional) {
				continue
			}
			if col.StorageType == LocalStore {
				_, ok := indexes[col.DataType]
				if !ok {
					indexes[col.DataType] = 0
				}
				if col.Index != indexes[col.DataType] && col.Index > 0 {
					// overlapping indexes would break data storage, make sure that columns for flags that include
					// other flags come last, ex.: first set columns for flag Naemon, then add columns for Naemon1_10
					log.Panicf("index overlap with flags in column %s of table %s: %v / %d != %d", col.Name, t.Name, flags.String(), col.Index, indexes[col.DataType])
				}
				col.Index = indexes[col.DataType]
				indexes[col.DataType]++
			}
		}
	}
}
