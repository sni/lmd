package lmd

import (
	"fmt"
	"strings"

	"github.com/sasha-s/go-deadlock"
)

// TableRef contains data for referenced tables.
type TableRef struct {
	noCopy  noCopy
	Table   *Table     // name of the table itself, ex.: hosts table
	Columns ColumnList // local column(s) which holds the values to determince the ID of the referenced item, ex.: host_name.
}

// TableName contains all table names.
type TableName int

// available table names.
const (
	TableNone TableName = iota
	TableBackends
	TableSites
	TableHosts
	TableServices
	TableHostgroups
	TableServicegroups
	TableColumns
	TableTables
	TableStatus
	TableTimeperiods
	TableContacts
	TableContactgroups
	TableCommands
	TableComments
	TableDowntimes
	TableLog
	TableHostsbygroup
	TableServicesbygroup
	TableServicesbyhostgroup
)

// NewTableName returns a table for given name or an error.
func NewTableName(name string) (TableName, error) {
	switch strings.ToLower(name) {
	case "backends":
		return TableBackends, nil
	case "sites":
		return TableSites, nil
	case "columns":
		return TableColumns, nil
	case "tables":
		return TableTables, nil
	case "status":
		return TableStatus, nil
	case "timeperiods":
		return TableTimeperiods, nil
	case "contacts":
		return TableContacts, nil
	case "contactgroups":
		return TableContactgroups, nil
	case "commands":
		return TableCommands, nil
	case "hosts":
		return TableHosts, nil
	case "hostgroups":
		return TableHostgroups, nil
	case "services":
		return TableServices, nil
	case "servicegroups":
		return TableServicegroups, nil
	case "comments":
		return TableComments, nil
	case "downtimes":
		return TableDowntimes, nil
	case "log":
		return TableLog, nil
	case "hostsbygroup":
		return TableHostsbygroup, nil
	case "servicesbygroup":
		return TableServicesbygroup, nil
	case "servicesbyhostgroup":
		return TableServicesbyhostgroup, nil
	}

	return TableNone, fmt.Errorf("table %s does not exist", name)
}

// String returns the name of this table as String.
func (t *TableName) String() string {
	switch *t {
	case TableBackends:
		return "backends"
	case TableSites:
		return "sites"
	case TableColumns:
		return "columns"
	case TableTables:
		return "tables"
	case TableStatus:
		return "status"
	case TableTimeperiods:
		return "timeperiods"
	case TableContacts:
		return "contacts"
	case TableContactgroups:
		return "contactgroups"
	case TableCommands:
		return "commands"
	case TableHosts:
		return "hosts"
	case TableHostgroups:
		return "hostgroups"
	case TableServices:
		return "services"
	case TableServicegroups:
		return "servicegroups"
	case TableComments:
		return "comments"
	case TableDowntimes:
		return "downtimes"
	case TableLog:
		return "log"
	case TableHostsbygroup:
		return "hostsbygroup"
	case TableServicesbygroup:
		return "servicesbygroup"
	case TableServicesbyhostgroup:
		return "servicesbyhostgroup"
	default:
		log.Panicf("unsupported tablename: %#v", t)
	}

	return ""
}

// Table defines available columns and table options.
type Table struct {
	noCopy          noCopy
	dataSizes       map[DataType]int   // contains size used for the datastore
	lock            *deadlock.RWMutex  // must be used for DataSizes access
	columnsIndex    map[string]*Column // access columns by name
	virtual         VirtualStoreResolveFunc
	primaryKey      []string
	refTables       []TableRef // referenced tables
	defaultSort     []string   // columns used to sort if nothing is specified
	columns         ColumnList
	name            TableName
	worksUnlocked   bool // flag wether locking the peer.DataLock can be skipped to answer the query
	passthroughOnly bool // flag wether table will be cached or simply passed through to remote sites
}

// GetColumn returns a column for given name or nil if not found.
func (t *Table) GetColumn(name string) *Column {
	return t.columnsIndex[name]
}

// GetColumnWithFallback returns a column for list of names, returns empty column as fallback.
func (t *Table) GetColumnWithFallback(name string) *Column {
	col, ok := t.columnsIndex[name]
	if ok {
		return col
	}
	if !fixBrokenClientsRequestColumn(&name, t.name) {
		return t.GetEmptyColumn()
	}

	return t.columnsIndex[name]
}

// GetColumns returns a column list for list of names.
func (t *Table) GetColumns(names []string) ColumnList {
	columns := make(ColumnList, 0, len(names))
	for i := range names {
		columns = append(columns, t.columnsIndex[names[i]])
	}

	return columns
}

// GetEmptyColumn returns an empty column.
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

// AddColumn adds a new column.
func (t *Table) AddColumn(name string, update FetchType, datatype DataType, description string) {
	NewColumn(t, name, LocalStore, update, datatype, NoFlags, nil, description)
}

// AddExtraColumn adds a new column with extra attributes.
func (t *Table) AddExtraColumn(name string, storage StorageType, update FetchType, datatype DataType, restrict OptionalFlags, description string) {
	NewColumn(t, name, storage, update, datatype, restrict, nil, description)
}

// AddPeerInfoColumn adds a new column related to peer information.
func (t *Table) AddPeerInfoColumn(name string, datatype DataType, description string) {
	NewColumn(t, name, VirtualStore, None, datatype, NoFlags, nil, description)
}

// AddRefColumns adds a reference column.
// tableName: name of the referenced table
// Prefix: column prefix for the added columns
// LocalName: column(s) which holds the reference value(s).
func (t *Table) AddRefColumns(tableName TableName, prefix string, localName []string) {
	refTable, Ok := Objects.Tables[tableName]
	if !Ok {
		log.Panicf("no such reference %s from column %s", tableName.String(), strings.Join(localName, ","))
	}

	t.refTables = append(t.refTables, TableRef{Table: refTable, Columns: t.GetColumns(localName)})

	// add fake columns for all columns from the referenced table
	for i := range Objects.Tables[tableName].columns {
		col := Objects.Tables[tableName].columns[i]
		// skip peer_key and such things from ref table
		if col.StorageType == RefStore {
			continue
		}
		refColName := prefix + "_" + col.Name
		if prefix == "" {
			refColName = col.Name
		}
		if _, ok := t.columnsIndex[refColName]; ok {
			continue
		}
		NewColumn(t, refColName, RefStore, None, col.DataType, col.Optional, col, col.Description)
	}
}
