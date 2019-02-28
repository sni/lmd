package main

// ObjectsType is a map of tables with a given order.
type ObjectsType struct {
	Tables map[string]*Table
	Order  []string
}

// Objects contains the static definition of all available tables and columns
var Objects *ObjectsType

// Table defines the livestatus table object.
type Table struct {
	noCopy                 noCopy
	Name                   string
	MaxIndex               int
	ColumnsIndex           map[string]int
	Columns                []*Column
	StaticColCacheNames    []string
	StaticColCacheIndexes  []int
	DynamicColCacheNames   []string
	DynamicColCacheIndexes []int
	RefColCacheNames       []string
	RefColCacheIndexes     []int
	PassthroughOnly        bool
	Virtual                bool
	GroupBy                bool
	WaitObject             []string
}

// UpdateType defines if and how the column is updated.
type UpdateType int

const (
	_ UpdateType = iota
	// StaticUpdate is used for all columns which are updated once at start.
	StaticUpdate
	// DynamicUpdate columns are updated periodically.
	DynamicUpdate
	// RefUpdate columns are static columns containing the key for a foreign table.
	RefUpdate
	// RefNoUpdate columns are referenced columns from other tables and not updated directly.
	RefNoUpdate
	// VirtUpdate columns are not updated directly and calculated on the fly.
	VirtUpdate
)

// ColumnType defines the data type of a column.
type ColumnType int

const (
	_ ColumnType = iota
	// StringCol is used for string columns.
	StringCol
	// StringListCol is used for string list columns.
	StringListCol
	// IntCol is used for integer columns.
	IntCol
	// IntListCol is used for integer list columns.
	IntListCol
	// FloatCol is used for float columns.
	FloatCol
	// RefCol is used for reference columns.
	RefCol
	// TimeCol is used for unix timestamp columns.
	TimeCol
	// CustomVarCol is used for custom variable map columns.
	CustomVarCol
	// HashMapCol is used for generic hash map columns.
	HashMapCol
	// VirtCol is used for virtual columns.
	VirtCol
	// StringFakeSortCol is used to sort grouped stats requests
	StringFakeSortCol
)

// Column is the definition of a single column within a table.
type Column struct {
	Name        string
	Type        ColumnType
	VirtType    ColumnType
	VirtMap     *VirtKeyMapTupel
	Index       int
	RefIndex    int
	RefColIndex int
	Update      UpdateType
	Optional    OptionalFlags
	Description string
}

// OptionalFlags is used to set flags for optionial columns.
type OptionalFlags byte

const (
	// NoFlags is set if there are no flags at all.
	NoFlags OptionalFlags = 0

	// LMD flag is set if the remote site is a LMD backend.
	LMD = 1 << iota

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
)

// GetEmptyValue returns an empty placeholder representation for the given column type
func (c *Column) GetEmptyValue() interface{} {
	switch c.Type {
	case IntCol:
		fallthrough
	case FloatCol:
		return -1
	case IntListCol:
		fallthrough
	case StringListCol:
		return (make([]interface{}, 0))
	}
	return ""
}

// GetTableColumnsData returns the virtual data used for the columns/table livestatus table.
func (o *ObjectsType) GetTableColumnsData() (data [][]interface{}) {
	for _, t := range o.Tables {
		for _, c := range t.Columns {
			colType := c.Type
			if c.Type == VirtCol {
				colType = c.VirtType
			}
			if c.Update == RefUpdate {
				continue
			}
			colTypeName := ""
			switch colType {
			case IntCol:
				colTypeName = "int"
			case StringCol:
				colTypeName = "string"
			case StringListCol:
				colTypeName = "list"
			case TimeCol:
				colTypeName = "int"
			case IntListCol:
				colTypeName = "list"
			case CustomVarCol:
				colTypeName = "list"
			case HashMapCol:
				colTypeName = "list"
			case FloatCol:
				colTypeName = "float"
			case RefCol:
				colTypeName = "string"
			default:
				log.Panicf("type not handled in table %s: %#v", t.Name, c)
			}
			row := make([]interface{}, 4)
			row[0] = c.Name
			row[1] = t.Name
			row[2] = colTypeName
			row[3] = c.Description
			data = append(data, row)
		}
	}
	return
}

// IsDefaultSortOrder returns true if the sortfield is the default for the given table.
func (t *Table) IsDefaultSortOrder(sort *[]*SortField) bool {
	if len(*sort) == 0 {
		return true
	}
	if len(*sort) == 2 {
		if t.Name == "services" {
			if (*sort)[0].Name == "host_name" && (*sort)[0].Direction == Asc && (*sort)[1].Name == "description" && (*sort)[1].Direction == Asc {
				return true
			}
		}
	} else if len(*sort) == 1 {
		if t.Name == "hosts" {
			if (*sort)[0].Name == "name" && (*sort)[0].Direction == Asc {
				return true
			}
		}
	}
	return false
}

// GetColumn returns a column by name.
func (t *Table) GetColumn(name string) *Column {
	return t.Columns[t.ColumnsIndex[name]]
}

// GetResultColumn returns a fake result column by name.
func (t *Table) GetResultColumn(name string) *ResultColumn {
	column := t.Columns[t.ColumnsIndex[name]]
	return &ResultColumn{Name: name, Type: column.Type, Column: column}
}

// GetInitialKeys returns the list of strings of all static and dynamic columns.
func (t *Table) GetInitialKeys(flags OptionalFlags) (keys []string) {
	for _, col := range t.Columns {
		if col.Update != RefUpdate && col.Update != RefNoUpdate && col.Type != VirtCol {
			if col.Optional == NoFlags || flags&col.Optional != 0 {
				keys = append(keys, col.Name)
			}
		}
	}
	return
}

// GetDynamicColumns returns a list of all dynamic columns along with their indexes.
func (t *Table) GetDynamicColumns(flags OptionalFlags) (keys []string, indexes []int) {
	offset := 0
	for _, col := range t.Columns {
		if col.Update == DynamicUpdate {
			if col.Optional == NoFlags || flags&col.Optional != 0 {
				keys = append(keys, col.Name)
				indexes = append(indexes, col.Index-offset)
			} else {
				offset++
			}
		}
	}
	return keys, indexes
}

// AddColumnObject adds a column object.
func (t *Table) AddColumnObject(col *Column) int {
	Index := t.MaxIndex
	t.MaxIndex++
	if t.ColumnsIndex == nil {
		t.ColumnsIndex = make(map[string]int)
	}
	if _, ok := t.ColumnsIndex[col.Name]; ok {
		log.Panicf("table %s trying to add column twice: %s", t.Name, col.Name)
	}
	t.ColumnsIndex[col.Name] = Index
	col.Index = Index
	switch col.Update {
	case StaticUpdate:
		t.StaticColCacheNames = append(t.StaticColCacheNames, col.Name)
		t.StaticColCacheIndexes = append(t.StaticColCacheIndexes, col.Index)
	case DynamicUpdate:
		t.DynamicColCacheNames = append(t.DynamicColCacheNames, col.Name)
		t.DynamicColCacheIndexes = append(t.DynamicColCacheIndexes, col.Index)
	case RefUpdate:
		t.RefColCacheNames = append(t.RefColCacheNames, col.Name)
		t.RefColCacheIndexes = append(t.RefColCacheIndexes, col.Index)
	}
	if t.Columns == nil {
		t.Columns = make([]*Column, 0)
	}
	t.Columns = append(t.Columns, col)
	return col.Index
}

// AddColumn adds a (normal) column.
func (t *Table) AddColumn(Name string, Update UpdateType, Type ColumnType, Description string) {
	column := &Column{
		Name:        Name,
		Type:        Type,
		Update:      Update,
		Description: Description,
	}
	if column.Type == VirtCol {
		virtMap, ok := VirtKeyMap[Name]
		if !ok {
			panic("no VirtKeyMap entry for " + Name)
		}
		column.VirtMap = &virtMap
		column.VirtType = column.VirtMap.Type
	}
	t.AddColumnObject(column)
}

// AddOptColumn adds a optional column.
func (t *Table) AddOptColumn(Name string, Update UpdateType, Type ColumnType, Restrict OptionalFlags, Description string) {
	column := &Column{
		Name:        Name,
		Type:        Type,
		Update:      Update,
		Description: Description,
		Optional:    Restrict,
	}
	t.AddColumnObject(column)
}

// AddRefColumn adds a reference column.
// Ref: name of the referenced table
// Prefix: column prefix for the added columns
// Name: column name in the referenced table
// LocalName: column name which holds the reference value
func (t *Table) AddRefColumn(Ref string, Prefix string, Name string, LocalName string) {
	_, Ok := Objects.Tables[Ref]
	if !Ok {
		panic("no such reference " + Ref + " from column " + LocalName)
	}

	// virtual column containing the information required to connect the referenced object
	RefColumn := Column{
		Name:        Ref, // type of reference, ex.: hosts
		Type:        RefCol,
		Update:      RefUpdate,
		RefIndex:    t.ColumnsIndex[LocalName],              // contains the index from the local column, ex: host_name in services
		RefColIndex: Objects.Tables[Ref].ColumnsIndex[Name], // contains the index from the remote column, ex: name in host
	}
	RefIndex := t.AddColumnObject(&RefColumn)

	// add fake columns for all columns from the referenced table
	for _, col := range Objects.Tables[Ref].Columns {
		if col.Name == Name {
			continue
		}
		// skip peer_key and such things from ref table
		if col.Type == VirtCol {
			continue
		}
		if col.Update == RefUpdate || col.Update == RefNoUpdate {
			continue
		}
		refColName := Prefix + "_" + col.Name
		if Prefix == "" {
			refColName = col.Name
		}
		if _, ok := t.ColumnsIndex[refColName]; ok {
			continue
		}
		column := &Column{
			Name:        refColName,
			Type:        col.Type,
			Update:      RefNoUpdate,
			RefIndex:    RefIndex,
			RefColIndex: col.Index,
			Description: col.Description,
		}
		t.AddColumnObject(column)
	}
}

// InitObjects creates the initial table object structures.
func InitObjects() {
	if Objects != nil {
		return
	}
	Objects = &ObjectsType{}

	// generate virtual keys with peer and host_peer prefix
	for name, dat := range VirtKeyMap {
		if dat.Key != "" {
			VirtKeyMap["peer_"+name] = dat
			VirtKeyMap["host_peer_"+name] = dat
		}
		if dat.Key == "" {
			VirtKeyMap["host_"+name] = dat
		}
	}

	Objects.Tables = make(map[string]*Table)
	// add complete virtual tables first
	Objects.AddTable("backends", NewBackendsTable("backends"))
	Objects.AddTable("sites", NewBackendsTable("sites"))
	Objects.AddTable("columns", NewColumnsTable("columns"))
	Objects.AddTable("tables", NewColumnsTable("tables"))

	Objects.AddTable("status", NewStatusTable())
	Objects.AddTable("timeperiods", NewTimeperiodsTable())
	Objects.AddTable("contacts", NewContactsTable())
	Objects.AddTable("contactgroups", NewContactgroupsTable())
	Objects.AddTable("commands", NewCommandsTable())
	Objects.AddTable("hosts", NewHostsTable())
	Objects.AddTable("hostgroups", NewHostgroupsTable())
	Objects.AddTable("services", NewServicesTable())
	Objects.AddTable("servicegroups", NewServicegroupsTable())
	Objects.AddTable("comments", NewCommentsTable())
	Objects.AddTable("downtimes", NewDowntimesTable())
	Objects.AddTable("log", NewLogTable())
	Objects.AddTable("hostsbygroup", NewHostsByGroupTable())
	Objects.AddTable("servicesbygroup", NewServicesByGroupTable())
	Objects.AddTable("servicesbyhostgroup", NewServicesByHostgroupTable())
}

// AddTable appends a table object to the Objects and verifies that no table is added twice.
func (o *ObjectsType) AddTable(name string, table *Table) {
	_, exists := o.Tables[name]
	if exists {
		log.Panicf("table %s has been added twice", name)
	}
	o.Tables[name] = table
	o.Order = append(o.Order, name)
}

// NewBackendsTable returns a new backends table
func NewBackendsTable(name string) (t *Table) {
	t = &Table{Name: name, Virtual: true, WaitObject: []string{"name"}}
	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")
	t.AddColumn("key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("name", RefNoUpdate, VirtCol, "Name of this peer")
	t.AddColumn("addr", RefNoUpdate, VirtCol, "Address of this peer")
	t.AddColumn("status", RefNoUpdate, VirtCol, "Status of this peer (0 - UP, 1 - Stale, 2 - Down, 4 - Pending)")
	t.AddColumn("bytes_send", RefNoUpdate, VirtCol, "Bytes send to this peer")
	t.AddColumn("bytes_received", RefNoUpdate, VirtCol, "Bytes received from this peer")
	t.AddColumn("queries", RefNoUpdate, VirtCol, "Number of queries sent to this peer")
	t.AddColumn("last_error", RefNoUpdate, VirtCol, "Last error message or empty if up")
	t.AddColumn("last_update", RefNoUpdate, VirtCol, "Timestamp of last update")
	t.AddColumn("last_online", RefNoUpdate, VirtCol, "Timestamp when peer was last online")
	t.AddColumn("response_time", RefNoUpdate, VirtCol, "Duration of last update in seconds")
	t.AddColumn("idling", RefNoUpdate, VirtCol, "Idle status of this backend (0 - Not idling, 1 - idling)")
	t.AddColumn("last_query", RefNoUpdate, VirtCol, "Timestamp of the last incoming request")
	t.AddColumn("section", RefNoUpdate, VirtCol, "Section information when having cascaded LMDs.")
	t.AddColumn("parent", RefNoUpdate, VirtCol, "Parent id when having cascaded LMDs.")
	t.AddColumn("lmd_version", RefNoUpdate, VirtCol, "LMD version string.")
	t.AddColumn("configtool", RefNoUpdate, VirtCol, "Thruks config tool configuration if available.")
	t.AddColumn("federation_key", RefNoUpdate, VirtCol, "original keys when using nested federation")
	t.AddColumn("federation_name", RefNoUpdate, VirtCol, "original names when using nested federation")
	t.AddColumn("federation_addr", RefNoUpdate, VirtCol, "original addresses when using nested federation")
	t.AddColumn("federation_type", RefNoUpdate, VirtCol, "original types when using nested federation")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewColumnsTable returns a new columns table
func NewColumnsTable(name string) (t *Table) {
	t = &Table{Name: name, Virtual: true}
	t.AddColumn("name", VirtUpdate, StringCol, "The name of the column within the table")
	t.AddColumn("table", VirtUpdate, StringCol, "The name of the table")
	t.AddColumn("type", VirtUpdate, StringCol, "The data type of the column (int, float, string, list)")
	t.AddColumn("description", VirtUpdate, StringCol, "A description of the column")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewStatusTable returns a new status table
func NewStatusTable() (t *Table) {
	t = &Table{Name: "status"}
	t.AddColumn("program_start", DynamicUpdate, IntCol, "The time of the last program start as UNIX timestamp")
	t.AddColumn("accept_passive_host_checks", DynamicUpdate, IntCol, "The number of host checks since program start")
	t.AddColumn("accept_passive_service_checks", DynamicUpdate, IntCol, "The number of completed service checks since program start")
	t.AddColumn("cached_log_messages", DynamicUpdate, IntCol, "The current number of log messages MK Livestatus keeps in memory")
	t.AddColumn("check_external_commands", DynamicUpdate, IntCol, "Whether the core checks for external commands at its command pipe (0/1)")
	t.AddColumn("check_host_freshness", DynamicUpdate, IntCol, "Whether host freshness checking is activated in general (0/1)")
	t.AddColumn("check_service_freshness", DynamicUpdate, IntCol, "Whether service freshness checking is activated in general (0/1)")
	t.AddColumn("connections", DynamicUpdate, IntCol, "The number of client connections to Livestatus since program start")
	t.AddColumn("connections_rate", DynamicUpdate, FloatCol, "The number of client connections to Livestatus since program start")
	t.AddColumn("enable_event_handlers", DynamicUpdate, IntCol, "Whether event handlers are activated in general (0/1)")
	t.AddColumn("enable_flap_detection", DynamicUpdate, IntCol, "Whether flap detection is activated in general (0/1)")
	t.AddColumn("enable_notifications", DynamicUpdate, IntCol, "Whether notifications are enabled in general (0/1)")
	t.AddColumn("execute_host_checks", DynamicUpdate, IntCol, "The number of host checks since program start")
	t.AddColumn("execute_service_checks", DynamicUpdate, IntCol, "The number of completed service checks since program start")
	t.AddColumn("forks", DynamicUpdate, IntCol, "The number of process creations since program start")
	t.AddColumn("forks_rate", DynamicUpdate, FloatCol, "The number of process creations since program start")
	t.AddColumn("host_checks", DynamicUpdate, IntCol, "The number of host checks since program start")
	t.AddColumn("host_checks_rate", DynamicUpdate, FloatCol, "The number of host checks since program start")
	t.AddColumn("interval_length", StaticUpdate, IntCol, "The default interval length from the core configuration")
	t.AddColumn("last_command_check", DynamicUpdate, IntCol, "The time of the last check for a command as UNIX timestamp")
	t.AddColumn("last_log_rotation", DynamicUpdate, IntCol, "Time time of the last log file rotation")
	t.AddColumn("livestatus_version", StaticUpdate, StringCol, "The version of the MK Livestatus module")
	t.AddColumn("log_messages", DynamicUpdate, IntCol, "The number of new log messages since program start")
	t.AddColumn("log_messages_rate", DynamicUpdate, FloatCol, "The number of new log messages since program start")
	t.AddColumn("nagios_pid", StaticUpdate, IntCol, "The process ID of the core main process")
	t.AddColumn("neb_callbacks", DynamicUpdate, IntCol, "The number of NEB call backs since program start")
	t.AddColumn("neb_callbacks_rate", DynamicUpdate, FloatCol, "The number of NEB call backs since program start")
	t.AddColumn("obsess_over_hosts", DynamicUpdate, IntCol, "Whether the core will obsess over host checks (0/1)")
	t.AddColumn("obsess_over_services", DynamicUpdate, IntCol, "Whether the core will obsess over service checks and run the ocsp_command (0/1)")
	t.AddColumn("process_performance_data", DynamicUpdate, IntCol, "Whether processing of performance data is activated in general (0/1)")
	t.AddColumn("program_version", StaticUpdate, StringCol, "The version of the monitoring daemon")
	t.AddColumn("requests", DynamicUpdate, FloatCol, "The number of requests to Livestatus since program start")
	t.AddColumn("requests_rate", DynamicUpdate, FloatCol, "The number of requests to Livestatus since program start")
	t.AddColumn("service_checks", DynamicUpdate, IntCol, "The number of completed service checks since program start")
	t.AddColumn("service_checks_rate", DynamicUpdate, FloatCol, "The number of completed service checks since program start")

	t.AddColumn("lmd_last_cache_update", RefNoUpdate, VirtCol, "Timestamp of the last LMD update of this object.")
	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")
	t.AddColumn("peer_addr", RefNoUpdate, VirtCol, "Address of this peer")
	t.AddColumn("peer_status", RefNoUpdate, VirtCol, "Status of this peer (0 - UP, 1 - Stale, 2 - Down, 4 - Pending)")
	t.AddColumn("peer_bytes_send", RefNoUpdate, VirtCol, "Bytes send to this peer")
	t.AddColumn("peer_bytes_received", RefNoUpdate, VirtCol, "Bytes received to this peer")
	t.AddColumn("peer_queries", RefNoUpdate, VirtCol, "Number of queries sent to this peer")
	t.AddColumn("peer_last_error", RefNoUpdate, VirtCol, "Last error message or empty if up")
	t.AddColumn("peer_last_update", RefNoUpdate, VirtCol, "Timestamp of last update")
	t.AddColumn("peer_last_online", RefNoUpdate, VirtCol, "Timestamp when peer was last online")
	t.AddColumn("peer_response_time", RefNoUpdate, VirtCol, "Duration of last update in seconds")
	t.AddColumn("configtool", RefNoUpdate, VirtCol, "Thruks config tool configuration if available.")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewTimeperiodsTable returns a new timeperiods table
func NewTimeperiodsTable() (t *Table) {
	t = &Table{Name: "timeperiods", WaitObject: []string{"name"}}
	t.AddColumn("alias", StaticUpdate, StringCol, "The alias of the timeperiod")
	t.AddColumn("name", StaticUpdate, StringCol, "The name of the timeperiod")
	t.AddColumn("in", DynamicUpdate, IntCol, "Wether we are currently in this period (0/1)")

	// naemon specific
	t.AddOptColumn("days", StaticUpdate, StringListCol, Naemon, "days")
	t.AddOptColumn("exceptions_calendar_dates", StaticUpdate, StringListCol, Naemon, "exceptions_calendar_dates")
	t.AddOptColumn("exceptions_month_date", StaticUpdate, StringListCol, Naemon, "exceptions_month_date")
	t.AddOptColumn("exceptions_month_day", StaticUpdate, StringListCol, Naemon, "exceptions_month_day")
	t.AddOptColumn("exceptions_month_week_day", StaticUpdate, StringListCol, Naemon, "exceptions_month_week_day")
	t.AddOptColumn("exceptions_week_day", StaticUpdate, StringListCol, Naemon, "exceptions_week_day")
	t.AddOptColumn("exclusions", StaticUpdate, StringListCol, Naemon, "exclusions")
	t.AddOptColumn("id", StaticUpdate, IntCol, Naemon, "The id of the timeperiods")

	t.AddColumn("lmd_last_cache_update", RefNoUpdate, VirtCol, "Timestamp of the last LMD update of this object.")
	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewContactsTable returns a new contacts table
func NewContactsTable() (t *Table) {
	t = &Table{Name: "contacts", WaitObject: []string{"name"}}
	t.AddColumn("alias", StaticUpdate, StringCol, "The full name of the contact")
	t.AddColumn("can_submit_commands", StaticUpdate, IntCol, "Wether the contact is allowed to submit commands (0/1)")
	t.AddColumn("email", StaticUpdate, StringCol, "The email address of the contact")
	t.AddColumn("host_notification_period", StaticUpdate, StringCol, "The time period in which the contact will be notified about host problems")
	t.AddColumn("host_notifications_enabled", StaticUpdate, IntCol, "Wether the contact will be notified about host problems in general (0/1)")
	t.AddColumn("name", StaticUpdate, StringCol, "The login name of the contact person")
	t.AddColumn("pager", StaticUpdate, StringCol, "The pager address of the contact")
	t.AddColumn("service_notification_period", StaticUpdate, StringCol, "The time period in which the contact will be notified about service problems")
	t.AddColumn("service_notifications_enabled", StaticUpdate, IntCol, "Wether the contact will be notified about service problems in general (0/1)")

	t.AddColumn("lmd_last_cache_update", RefNoUpdate, VirtCol, "Timestamp of the last LMD update of this object.")
	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewContactgroupsTable returns a new contactgroups table
func NewContactgroupsTable() (t *Table) {
	t = &Table{Name: "contactgroups", WaitObject: []string{"name"}}
	t.AddColumn("alias", StaticUpdate, StringCol, "The alias of the contactgroup")
	t.AddColumn("members", StaticUpdate, StringListCol, "A list of all members of this contactgroup")
	t.AddColumn("name", StaticUpdate, StringCol, "The name of the contactgroup")

	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewCommandsTable returns a new commands table
func NewCommandsTable() (t *Table) {
	t = &Table{Name: "commands", WaitObject: []string{"name"}}
	t.AddColumn("name", StaticUpdate, StringCol, "The name of the command")
	t.AddColumn("line", StaticUpdate, StringCol, "The shell command line")

	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewHostsTable returns a new hosts table
func NewHostsTable() (t *Table) {
	t = &Table{Name: "hosts", WaitObject: []string{"name"}}
	t.AddColumn("accept_passive_checks", DynamicUpdate, IntCol, "Whether passive host checks are accepted (0/1)")
	t.AddColumn("acknowledged", DynamicUpdate, IntCol, "Whether the current host problem has been acknowledged (0/1)")
	t.AddColumn("action_url", StaticUpdate, StringCol, "An optional URL to custom actions or information about this host")
	t.AddColumn("action_url_expanded", StaticUpdate, StringCol, "An optional URL to custom actions or information about this host")
	t.AddColumn("active_checks_enabled", DynamicUpdate, IntCol, "Whether active checks are enabled for the host (0/1)")
	t.AddColumn("address", StaticUpdate, StringCol, "IP address")
	t.AddColumn("alias", StaticUpdate, StringCol, "An alias name for the host")
	t.AddColumn("check_command", StaticUpdate, StringCol, "Nagios command for active host check of this host")
	t.AddColumn("check_freshness", DynamicUpdate, IntCol, "Whether freshness checks are activated (0/1)")
	t.AddColumn("check_interval", StaticUpdate, IntCol, "Number of basic interval lengths between two scheduled checks of the host")
	t.AddColumn("check_options", DynamicUpdate, IntCol, "The current check option, forced, normal, freshness... (0-2)")
	t.AddColumn("check_period", StaticUpdate, StringCol, "Time period in which this host will be checked. If empty then the host will always be checked.")
	t.AddColumn("check_type", DynamicUpdate, IntCol, "Type of check (0: active, 1: passive)")
	t.AddColumn("checks_enabled", DynamicUpdate, IntCol, "Whether checks of the host are enabled (0/1)")
	t.AddColumn("childs", StaticUpdate, StringListCol, "A list of all direct childs of the host")
	t.AddColumn("contacts", StaticUpdate, StringListCol, "A list of all contacts of this host, either direct or via a contact group")
	t.AddColumn("contact_groups", StaticUpdate, StringListCol, "A list of all contact groups this host is in")
	t.AddColumn("comments", DynamicUpdate, IntListCol, "A list of the ids of all comments of this host")
	t.AddColumn("current_attempt", DynamicUpdate, IntCol, "Number of the current check attempts")
	t.AddColumn("current_notification_number", DynamicUpdate, IntCol, "Number of the current notification")
	t.AddColumn("custom_variables", DynamicUpdate, CustomVarCol, "A dictionary of the custom variables")
	t.AddColumn("custom_variable_names", DynamicUpdate, StringListCol, "A list of the names of all custom variables")
	t.AddColumn("custom_variable_values", DynamicUpdate, StringListCol, "A list of the values of the custom variables")
	t.AddColumn("display_name", StaticUpdate, StringCol, "Optional display name of the host - not used by Nagios' web interface")
	t.AddColumn("downtimes", DynamicUpdate, IntListCol, "A list of the ids of all scheduled downtimes of this host")
	t.AddColumn("event_handler", StaticUpdate, StringCol, "Nagios command used as event handler")
	t.AddColumn("event_handler_enabled", DynamicUpdate, IntCol, "Nagios command used as event handler")
	t.AddColumn("execution_time", DynamicUpdate, FloatCol, "Time the host check needed for execution")
	t.AddColumn("first_notification_delay", StaticUpdate, IntCol, "Delay before the first notification")
	t.AddColumn("flap_detection_enabled", DynamicUpdate, IntCol, "Whether flap detection is enabled (0/1)")
	t.AddColumn("groups", StaticUpdate, StringListCol, "A list of all host groups this host is in")
	t.AddColumn("hard_state", DynamicUpdate, IntCol, "The effective hard state of the host (eliminates a problem in hard_state)")
	t.AddColumn("has_been_checked", DynamicUpdate, IntCol, "Whether the host has already been checked (0/1)")
	t.AddColumn("high_flap_threshold", StaticUpdate, IntCol, "High threshold of flap detection")
	t.AddColumn("icon_image", StaticUpdate, StringCol, "The name of an image file to be used in the web pages")
	t.AddColumn("icon_image_alt", StaticUpdate, StringCol, "The name of an image file to be used in the web pages")
	t.AddColumn("icon_image_expanded", StaticUpdate, StringCol, "The name of an image file to be used in the web pages")
	t.AddColumn("in_check_period", DynamicUpdate, IntCol, "Time period in which this host will be checked. If empty then the host will always be checked.")
	t.AddColumn("in_notification_period", DynamicUpdate, IntCol, "Time period in which problems of this host will be notified. If empty then notification will be always")
	t.AddColumn("is_executing", DynamicUpdate, IntCol, "is there a host check currently running... (0/1)")
	t.AddColumn("is_flapping", DynamicUpdate, IntCol, "Whether the host state is flapping (0/1)")
	t.AddColumn("last_check", DynamicUpdate, IntCol, "Time of the last check (Unix timestamp)")
	t.AddColumn("last_hard_state", DynamicUpdate, IntCol, "The effective hard state of the host (eliminates a problem in hard_state)")
	t.AddColumn("last_hard_state_change", DynamicUpdate, IntCol, "The effective hard state of the host (eliminates a problem in hard_state)")
	t.AddColumn("last_notification", DynamicUpdate, IntCol, "Time of the last notification (Unix timestamp)")
	t.AddColumn("last_state", DynamicUpdate, IntCol, "State before last state change")
	t.AddColumn("last_state_change", DynamicUpdate, IntCol, "State before last state change")
	t.AddColumn("last_time_down", DynamicUpdate, IntCol, "The last time the host was DOWN (Unix timestamp)")
	t.AddColumn("last_time_unreachable", DynamicUpdate, IntCol, "The last time the host was UNREACHABLE (Unix timestamp)")
	t.AddColumn("last_time_up", DynamicUpdate, IntCol, "The last time the host was UP (Unix timestamp)")
	t.AddColumn("latency", DynamicUpdate, FloatCol, "Time difference between scheduled check time and actual check time")
	t.AddColumn("long_plugin_output", DynamicUpdate, StringCol, "Complete output from check plugin")
	t.AddColumn("low_flap_threshold", StaticUpdate, IntCol, "Low threshold of flap detection")
	t.AddColumn("max_check_attempts", StaticUpdate, IntCol, "Max check attempts for active host checks")
	t.AddColumn("modified_attributes", DynamicUpdate, IntCol, "A bitmask specifying which attributes have been modified")
	t.AddColumn("modified_attributes_list", DynamicUpdate, StringListCol, "A bitmask specifying which attributes have been modified")
	t.AddColumn("name", StaticUpdate, StringCol, "Host name")
	t.AddColumn("next_check", DynamicUpdate, FloatCol, "Scheduled time for the next check (Unix timestamp)")
	t.AddColumn("next_notification", DynamicUpdate, IntCol, "Time of the next notification (Unix timestamp)")
	t.AddColumn("num_services", StaticUpdate, IntCol, "The total number of services of the host")
	t.AddColumn("num_services_crit", DynamicUpdate, IntCol, "The number of the host's services with the soft state CRIT")
	t.AddColumn("num_services_ok", DynamicUpdate, IntCol, "The number of the host's services with the soft state OK")
	t.AddColumn("num_services_pending", DynamicUpdate, IntCol, "The number of the host's services which have not been checked yet (pending)")
	t.AddColumn("num_services_unknown", DynamicUpdate, IntCol, "The number of the host's services with the soft state UNKNOWN")
	t.AddColumn("num_services_warn", DynamicUpdate, IntCol, "The number of the host's services with the soft state WARN")
	t.AddColumn("notes", StaticUpdate, StringCol, "Optional notes for this host")
	t.AddColumn("notes_expanded", StaticUpdate, StringCol, "Optional notes for this host")
	t.AddColumn("notes_url", StaticUpdate, StringCol, "Optional notes for this host")
	t.AddColumn("notes_url_expanded", StaticUpdate, StringCol, "Optional notes for this host")
	t.AddColumn("notification_interval", StaticUpdate, IntCol, "Interval of periodic notification or 0 if its off")
	t.AddColumn("notification_period", StaticUpdate, StringCol, "Time period in which problems of this host will be notified. If empty then notification will be always")
	t.AddColumn("notifications_enabled", DynamicUpdate, IntCol, "Whether notifications of the host are enabled (0/1)")
	t.AddColumn("obsess_over_host", DynamicUpdate, IntCol, "The current obsess_over_host setting... (0/1)")
	t.AddColumn("parents", StaticUpdate, StringListCol, "A list of all direct parents of the host")
	t.AddColumn("percent_state_change", DynamicUpdate, FloatCol, "Percent state change")
	t.AddColumn("perf_data", DynamicUpdate, StringCol, "Optional performance data of the last host check")
	t.AddColumn("plugin_output", DynamicUpdate, StringCol, "Output of the last host check")
	t.AddColumn("process_performance_data", DynamicUpdate, IntCol, "Whether processing of performance data is enabled (0/1)")
	t.AddColumn("retry_interval", StaticUpdate, IntCol, "Number of basic interval lengths between checks when retrying after a soft error")
	t.AddColumn("scheduled_downtime_depth", DynamicUpdate, IntCol, "The number of downtimes this host is currently in")
	t.AddColumn("services", StaticUpdate, StringListCol, "The services associated with the host")
	t.AddColumn("state", DynamicUpdate, IntCol, "The current state of the host (0: up, 1: down, 2: unreachable)")
	t.AddColumn("state_type", DynamicUpdate, IntCol, "The current state of the host (0: up, 1: down, 2: unreachable)")
	t.AddColumn("staleness", DynamicUpdate, FloatCol, "Staleness indicator for this host")
	t.AddColumn("pnpgraph_present", DynamicUpdate, IntCol, "The pnp graph presence (0/1)")

	// naemon specific
	t.AddOptColumn("obsess", DynamicUpdate, IntCol, Naemon, "The obsessing over host")
	t.AddOptColumn("depends_exec", StaticUpdate, StringListCol, Naemon, "List of hosts this hosts depends on for execution.")
	t.AddOptColumn("depends_notify", StaticUpdate, StringListCol, Naemon, "List of hosts this hosts depends on for notification.")

	// shinken specific
	t.AddOptColumn("is_impact", DynamicUpdate, IntCol, Shinken, "Whether the host state is an impact or not (0/1)")
	t.AddOptColumn("business_impact", StaticUpdate, IntCol, Shinken, "An importance level. From 0 (not important) to 5 (top for business)")
	t.AddOptColumn("source_problems", DynamicUpdate, StringListCol, Shinken, "The name of the source problems (host or service)")
	t.AddOptColumn("impacts", DynamicUpdate, StringListCol, Shinken, "List of what the source impact (list of hosts and services)")
	t.AddOptColumn("criticity", DynamicUpdate, IntCol, Shinken, "The importance we gave to this host between the minimum 0 and the maximum 5")
	t.AddOptColumn("is_problem", DynamicUpdate, IntCol, Shinken, "Whether the host state is a problem or not (0/1)")
	t.AddOptColumn("realm", DynamicUpdate, StringCol, Shinken, "Realm")
	t.AddOptColumn("poller_tag", DynamicUpdate, StringCol, Shinken, "Poller Tag")
	t.AddOptColumn("got_business_rule", DynamicUpdate, IntCol, Shinken, "Whether the host state is an business rule based host or not (0/1)")
	t.AddOptColumn("parent_dependencies", DynamicUpdate, StringCol, Shinken, "List of the dependencies (logical, network or business one) of this host.")

	t.AddColumn("services_with_info", RefNoUpdate, VirtCol, "The services, including info, that is associated with the host")
	t.AddColumn("services_with_state", RefNoUpdate, VirtCol, "The services, including state info, that is associated with the host")
	t.AddColumn("lmd_last_cache_update", RefNoUpdate, VirtCol, "Timestamp of the last LMD update of this object.")
	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")
	t.AddColumn("last_state_change_order", RefNoUpdate, VirtCol, "The last_state_change of this host suitable for sorting. Returns program_start from the core if host has been never checked.")
	t.AddColumn("has_long_plugin_output", RefNoUpdate, VirtCol, "Flag wether this host has long_plugin_output or not")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewHostgroupsTable returns a new hostgroups table
func NewHostgroupsTable() (t *Table) {
	t = &Table{Name: "hostgroups", WaitObject: []string{"name"}}
	t.AddColumn("action_url", StaticUpdate, StringCol, "An optional URL to custom actions or information about the hostgroup")
	t.AddColumn("alias", StaticUpdate, StringCol, "An alias of the hostgroup")
	t.AddColumn("members", StaticUpdate, StringListCol, "A list of all host names that are members of the hostgroup")
	t.AddColumn("name", StaticUpdate, StringCol, "Name of the hostgroup")
	t.AddColumn("notes", StaticUpdate, StringCol, "Optional notes to the hostgroup")
	t.AddColumn("notes_url", StaticUpdate, StringCol, "An optional URL with further information about the hostgroup")
	t.AddColumn("num_hosts", StaticUpdate, IntCol, "The total number of hosts of the hostgroup")
	t.AddColumn("num_hosts_up", DynamicUpdate, IntCol, "The total number of up hosts of the hostgroup")
	t.AddColumn("num_hosts_down", DynamicUpdate, IntCol, "The total number of down hosts of the hostgroup")
	t.AddColumn("num_hosts_unreach", DynamicUpdate, IntCol, "The total number of unreachable hosts of the hostgroup")
	t.AddColumn("num_hosts_pending", DynamicUpdate, IntCol, "The total number of down hosts of the hostgroup")
	t.AddColumn("num_services", StaticUpdate, IntCol, "The total number of services of the hostgroup")
	t.AddColumn("num_services_ok", DynamicUpdate, IntCol, "The total number of ok services of the hostgroup")
	t.AddColumn("num_services_warn", DynamicUpdate, IntCol, "The total number of warning services of the hostgroup")
	t.AddColumn("num_services_crit", DynamicUpdate, IntCol, "The total number of critical services of the hostgroup")
	t.AddColumn("num_services_unknown", DynamicUpdate, IntCol, "The total number of unknown services of the hostgroup")
	t.AddColumn("num_services_pending", DynamicUpdate, IntCol, "The total number of pending services of the hostgroup")
	t.AddColumn("worst_host_state", DynamicUpdate, IntCol, "The worst host state of the hostgroup")
	t.AddColumn("worst_service_state", DynamicUpdate, IntCol, "The worst service state of the hostgroup")

	t.AddColumn("lmd_last_cache_update", RefNoUpdate, VirtCol, "Timestamp of the last LMD update of this object.")
	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewServicesTable returns a new services table
func NewServicesTable() (t *Table) {
	t = &Table{Name: "services", WaitObject: []string{"host_name", "description"}}
	t.AddColumn("accept_passive_checks", DynamicUpdate, IntCol, "Whether the service accepts passive checks (0/1)")
	t.AddColumn("acknowledged", DynamicUpdate, IntCol, "Whether the current service problem has been acknowledged (0/1)")
	t.AddColumn("acknowledgement_type", DynamicUpdate, IntCol, "The type of the acknownledgement (0: none, 1: normal, 2: sticky)")
	t.AddColumn("action_url", StaticUpdate, StringCol, "An optional URL for actions or custom information about the service")
	t.AddColumn("action_url_expanded", StaticUpdate, StringCol, "An optional URL for actions or custom information about the service")
	t.AddColumn("active_checks_enabled", DynamicUpdate, IntCol, "Whether active checks are enabled for the service (0/1)")
	t.AddColumn("check_command", StaticUpdate, StringCol, "Nagios command used for active checks")
	t.AddColumn("check_interval", StaticUpdate, IntCol, "Number of basic interval lengths between two scheduled checks of the service")
	t.AddColumn("check_options", DynamicUpdate, IntCol, "The current check option, forced, normal, freshness... (0/1)")
	t.AddColumn("check_period", StaticUpdate, StringCol, "The name of the check period of the service. It this is empty, the service is always checked.")
	t.AddColumn("check_type", DynamicUpdate, IntCol, "The type of the last check (0: active, 1: passive)")
	t.AddColumn("checks_enabled", DynamicUpdate, IntCol, "Whether active checks are enabled for the service (0/1)")
	t.AddColumn("contacts", StaticUpdate, StringListCol, "A list of all contacts of the service, either direct or via a contact group")
	t.AddColumn("contact_groups", StaticUpdate, StringListCol, "A list of all contact groups this service is in")
	t.AddColumn("comments", DynamicUpdate, IntListCol, "A list of all comment ids of the service")
	t.AddColumn("current_attempt", DynamicUpdate, IntCol, "The number of the current check attempt")
	t.AddColumn("current_notification_number", DynamicUpdate, IntCol, "The number of the current notification")
	t.AddColumn("custom_variables", DynamicUpdate, CustomVarCol, "A dictionary of the custom variables")
	t.AddColumn("custom_variable_names", DynamicUpdate, StringListCol, "A list of the names of all custom variables of the service")
	t.AddColumn("custom_variable_values", DynamicUpdate, StringListCol, "A list of the values of all custom variable of the service")
	t.AddColumn("description", StaticUpdate, StringCol, "Description of the service (also used as key)")
	t.AddColumn("downtimes", DynamicUpdate, IntListCol, "A list of all downtime ids of the service")
	t.AddColumn("display_name", StaticUpdate, StringCol, "An optional display name (not used by Nagios standard web pages)")
	t.AddColumn("event_handler", StaticUpdate, StringCol, "Nagios command used as event handler")
	t.AddColumn("event_handler_enabled", DynamicUpdate, IntCol, "Nagios command used as event handler")
	t.AddColumn("execution_time", DynamicUpdate, FloatCol, "Time the service check needed for execution")
	t.AddColumn("first_notification_delay", DynamicUpdate, IntCol, "Delay before the first notification")
	t.AddColumn("flap_detection_enabled", DynamicUpdate, IntCol, "Whether flap detection is enabled for the service (0/1)")
	t.AddColumn("groups", StaticUpdate, StringListCol, "A list of all service groups the service is in")
	t.AddColumn("has_been_checked", DynamicUpdate, IntCol, "Whether the service already has been checked (0/1)")
	t.AddColumn("high_flap_threshold", StaticUpdate, IntCol, "High threshold of flap detection")
	t.AddColumn("icon_image", StaticUpdate, StringCol, "The name of an image to be used as icon in the web interface")
	t.AddColumn("icon_image_alt", StaticUpdate, StringCol, "The name of an image to be used as icon in the web interface")
	t.AddColumn("icon_image_expanded", StaticUpdate, StringCol, "The name of an image to be used as icon in the web interface")
	t.AddColumn("in_check_period", DynamicUpdate, IntCol, "The name of the check period of the service. It this is empty, the service is always checked.")
	t.AddColumn("in_notification_period", DynamicUpdate, IntCol, "The name of the notification period of the service. It this is empty, service problems are always notified.")
	t.AddColumn("initial_state", StaticUpdate, IntCol, "The initial state of the service")
	t.AddColumn("is_executing", DynamicUpdate, IntCol, "is there a service check currently running... (0/1)")
	t.AddColumn("is_flapping", DynamicUpdate, IntCol, "Whether the service is flapping (0/1)")
	t.AddColumn("last_check", DynamicUpdate, IntCol, "The time of the last check (Unix timestamp)")
	t.AddColumn("last_hard_state", DynamicUpdate, IntCol, "The last hard state of the service")
	t.AddColumn("last_hard_state_change", DynamicUpdate, IntCol, "The last hard state of the service")
	t.AddColumn("last_notification", DynamicUpdate, IntCol, "The time of the last notification (Unix timestamp)")
	t.AddColumn("last_state", DynamicUpdate, IntCol, "The last state of the service")
	t.AddColumn("last_state_change", DynamicUpdate, IntCol, "The last state of the service")
	t.AddColumn("last_time_critical", DynamicUpdate, IntCol, "The last time the service was CRITICAL (Unix timestamp)")
	t.AddColumn("last_time_warning", DynamicUpdate, IntCol, "The last time the service was in WARNING state (Unix timestamp)")
	t.AddColumn("last_time_ok", DynamicUpdate, IntCol, "The last time the service was OK (Unix timestamp)")
	t.AddColumn("last_time_unknown", DynamicUpdate, IntCol, "The last time the service was UNKNOWN (Unix timestamp)")
	t.AddColumn("latency", DynamicUpdate, FloatCol, "Time difference between scheduled check time and actual check time")
	t.AddColumn("long_plugin_output", DynamicUpdate, StringCol, "Unabbreviated output of the last check plugin")
	t.AddColumn("low_flap_threshold", DynamicUpdate, IntCol, "Low threshold of flap detection")
	t.AddColumn("max_check_attempts", StaticUpdate, IntCol, "The maximum number of check attempts")
	t.AddColumn("modified_attributes", DynamicUpdate, IntCol, "A bitmask specifying which attributes have been modified")
	t.AddColumn("modified_attributes_list", DynamicUpdate, StringListCol, "A bitmask specifying which attributes have been modified")
	t.AddColumn("next_check", DynamicUpdate, FloatCol, "The scheduled time of the next check (Unix timestamp)")
	t.AddColumn("next_notification", DynamicUpdate, IntCol, "The time of the next notification (Unix timestamp)")
	t.AddColumn("notes", StaticUpdate, StringCol, "Optional notes about the service")
	t.AddColumn("notes_expanded", StaticUpdate, StringCol, "Optional notes about the service")
	t.AddColumn("notes_url", StaticUpdate, StringCol, "Optional notes about the service")
	t.AddColumn("notes_url_expanded", StaticUpdate, StringCol, "Optional notes about the service")
	t.AddColumn("notification_interval", StaticUpdate, IntCol, "Interval of periodic notification or 0 if its off")
	t.AddColumn("notification_period", StaticUpdate, StringCol, "The name of the notification period of the service. It this is empty, service problems are always notified.")
	t.AddColumn("notifications_enabled", DynamicUpdate, IntCol, "Whether notifications are enabled for the service (0/1)")
	t.AddColumn("obsess_over_service", DynamicUpdate, IntCol, "Whether 'obsess_over_service' is enabled for the service (0/1)")
	t.AddColumn("percent_state_change", DynamicUpdate, FloatCol, "Percent state change")
	t.AddColumn("perf_data", DynamicUpdate, StringCol, "Performance data of the last check plugin")
	t.AddColumn("plugin_output", DynamicUpdate, StringCol, "Output of the last check plugin")
	t.AddColumn("process_performance_data", DynamicUpdate, IntCol, "Whether processing of performance data is enabled for the service (0/1)")
	t.AddColumn("retry_interval", StaticUpdate, IntCol, "Number of basic interval lengths between checks when retrying after a soft error")
	t.AddColumn("scheduled_downtime_depth", DynamicUpdate, IntCol, "The number of scheduled downtimes the service is currently in")
	t.AddColumn("state", DynamicUpdate, IntCol, "The current state of the service (0: OK, 1: WARN, 2: CRITICAL, 3: UNKNOWN)")
	t.AddColumn("state_type", DynamicUpdate, IntCol, "The current state of the service (0: OK, 1: WARN, 2: CRITICAL, 3: UNKNOWN)")
	t.AddColumn("host_name", StaticUpdate, StringCol, "Host name")
	t.AddColumn("staleness", DynamicUpdate, FloatCol, "Staleness indicator for this host")
	t.AddColumn("pnpgraph_present", DynamicUpdate, IntCol, "The pnp graph presence (0/1)")

	// naemon specific
	t.AddOptColumn("obsess", DynamicUpdate, IntCol, Naemon, "The obsessing over service")
	t.AddOptColumn("depends_exec", StaticUpdate, StringListCol, Naemon, "List of services this services depends on for execution.")
	t.AddOptColumn("depends_notify", StaticUpdate, StringListCol, Naemon, "List of services this services depends on for notification.")
	t.AddOptColumn("parents", StaticUpdate, StringListCol, Naemon, "List of services descriptions this services depends on.")

	// shinken specific
	t.AddOptColumn("is_impact", DynamicUpdate, IntCol, Shinken, "Whether the host state is an impact or not (0/1)")
	t.AddOptColumn("business_impact", StaticUpdate, IntCol, Shinken, "An importance level. From 0 (not important) to 5 (top for business)")
	t.AddOptColumn("source_problems", DynamicUpdate, StringListCol, Shinken, "The name of the source problems (host or service)")
	t.AddOptColumn("impacts", DynamicUpdate, StringListCol, Shinken, "List of what the source impact (list of hosts and services)")
	t.AddOptColumn("criticity", DynamicUpdate, IntCol, Shinken, "The importance we gave to this service between the minimum 0 and the maximum 5")
	t.AddOptColumn("is_problem", DynamicUpdate, IntCol, Shinken, "Whether the host state is a problem or not (0/1)")
	t.AddOptColumn("realm", DynamicUpdate, StringCol, Shinken, "Realm")
	t.AddOptColumn("poller_tag", DynamicUpdate, StringCol, Shinken, "Poller Tag")
	t.AddOptColumn("got_business_rule", DynamicUpdate, IntCol, Shinken, "Whether the service state is an business rule based host or not (0/1)")
	t.AddOptColumn("parent_dependencies", DynamicUpdate, StringCol, Shinken, "List of the dependencies (logical, network or business one) of this service.")

	t.AddRefColumn("hosts", "host", "name", "host_name")

	t.AddColumn("lmd_last_cache_update", RefNoUpdate, VirtCol, "Timestamp of the last LMD update of this object.")
	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")
	t.AddColumn("last_state_change_order", RefNoUpdate, VirtCol, "The last_state_change of this host suitable for sorting. Returns program_start from the core if host has been never checked.")
	t.AddColumn("state_order", RefNoUpdate, VirtCol, "The service state suitable for sorting. Unknown and Critical state are switched.")
	t.AddColumn("has_long_plugin_output", RefNoUpdate, VirtCol, "Flag wether this service has long_plugin_output or not")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewServicegroupsTable returns a new hostgroups table
func NewServicegroupsTable() (t *Table) {
	t = &Table{Name: "servicegroups", WaitObject: []string{"name"}}
	t.AddColumn("action_url", StaticUpdate, StringCol, "An optional URL to custom notes or actions on the service group")
	t.AddColumn("alias", StaticUpdate, StringCol, "An alias of the service group")
	t.AddColumn("members", StaticUpdate, StringListCol, "A list of all members of the service group as host/service pairs")
	t.AddColumn("name", StaticUpdate, StringCol, "The name of the service group")
	t.AddColumn("notes", StaticUpdate, StringCol, "Optional additional notes about the service group")
	t.AddColumn("notes_url", StaticUpdate, StringCol, "An optional URL to further notes on the service group")
	t.AddColumn("num_services", StaticUpdate, IntCol, "The total number of services of the service group")
	t.AddColumn("num_services_ok", DynamicUpdate, IntCol, "The total number of ok services of the service group")
	t.AddColumn("num_services_warn", DynamicUpdate, IntCol, "The total number of warning services of the service group")
	t.AddColumn("num_services_crit", DynamicUpdate, IntCol, "The total number of critical services of the service group")
	t.AddColumn("num_services_unknown", DynamicUpdate, IntCol, "The total number of unknown services of the service group")
	t.AddColumn("num_services_pending", DynamicUpdate, IntCol, "The total number of pending services of the service group")
	t.AddColumn("worst_service_state", DynamicUpdate, IntCol, "The worst service state of the service group")

	t.AddColumn("lmd_last_cache_update", RefNoUpdate, VirtCol, "Timestamp of the last LMD update of this object.")
	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewCommentsTable returns a new comments table
func NewCommentsTable() (t *Table) {
	t = &Table{Name: "comments", WaitObject: []string{"id"}}
	t.AddColumn("author", StaticUpdate, StringCol, "The contact that entered the comment")
	t.AddColumn("comment", StaticUpdate, StringCol, "A comment text")
	t.AddColumn("entry_time", StaticUpdate, IntCol, "The time the entry was made as UNIX timestamp")
	t.AddColumn("entry_type", StaticUpdate, IntCol, "The type of the comment: 1 is user, 2 is downtime, 3 is flap and 4 is acknowledgement")
	t.AddColumn("expires", StaticUpdate, IntCol, "Whether this comment expires")
	t.AddColumn("expire_time", StaticUpdate, IntCol, "The time of expiry of this comment as a UNIX timestamp")
	t.AddColumn("id", StaticUpdate, IntCol, "The id of the comment")
	t.AddColumn("is_service", StaticUpdate, IntCol, "0, if this entry is for a host, 1 if it is for a service")
	t.AddColumn("persistent", StaticUpdate, IntCol, "Whether this comment is persistent (0/1)")
	t.AddColumn("source", StaticUpdate, IntCol, "The source of the comment (0 is internal and 1 is external)")
	t.AddColumn("type", StaticUpdate, IntCol, "The type of the comment: 1 is host, 2 is service")
	t.AddColumn("host_name", StaticUpdate, StringCol, "Host name")
	t.AddColumn("service_description", StaticUpdate, StringCol, "Description of the service (also used as key)")

	t.AddRefColumn("hosts", "host", "name", "host_name")
	t.AddRefColumn("services", "service", "description", "service_description")

	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewDowntimesTable returns a new downtimes table
func NewDowntimesTable() (t *Table) {
	t = &Table{Name: "downtimes", WaitObject: []string{"id"}}
	t.AddColumn("author", StaticUpdate, StringCol, "The contact that scheduled the downtime")
	t.AddColumn("comment", StaticUpdate, StringCol, "A comment text")
	t.AddColumn("duration", StaticUpdate, IntCol, "The duration of the downtime in seconds")
	t.AddColumn("end_time", StaticUpdate, IntCol, "The end time of the downtime as UNIX timestamp")
	t.AddColumn("entry_time", StaticUpdate, IntCol, "The time the entry was made as UNIX timestamp")
	t.AddColumn("fixed", StaticUpdate, IntCol, "1 if the downtime is fixed, a 0 if it is flexible")
	t.AddColumn("id", StaticUpdate, IntCol, "The id of the downtime")
	t.AddColumn("is_service", StaticUpdate, IntCol, "0, if this entry is for a host, 1 if it is for a service")
	t.AddColumn("start_time", StaticUpdate, IntCol, "The start time of the downtime as UNIX timestamp")
	t.AddColumn("triggered_by", StaticUpdate, IntCol, "The id of the downtime this downtime was triggered by or 0 if it was not triggered by another downtime")
	t.AddColumn("type", StaticUpdate, IntCol, "The type of the downtime: 0 if it is active, 1 if it is pending")
	t.AddColumn("host_name", StaticUpdate, StringCol, "Host name")
	t.AddColumn("service_description", StaticUpdate, StringCol, "Description of the service (also used as key)")

	t.AddRefColumn("hosts", "host", "name", "host_name")
	t.AddRefColumn("services", "service", "description", "service_description")

	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewLogTable returns a new log table
func NewLogTable() (t *Table) {
	t = &Table{Name: "log", PassthroughOnly: true}

	t.AddColumn("attempt", StaticUpdate, IntCol, "The number of the check attempt")
	t.AddColumn("class", StaticUpdate, IntCol, "The class of the message as integer (0:info, 1:state, 2:program, 3:notification, 4:passive, 5:command)")
	t.AddColumn("contact_name", StaticUpdate, StringCol, "The name of the contact the log entry is about (might be empty)")
	t.AddColumn("host_name", StaticUpdate, StringCol, "The name of the host the log entry is about (might be empty)")
	t.AddColumn("lineno", StaticUpdate, IntCol, "The number of the line in the log file")
	t.AddColumn("message", StaticUpdate, StringCol, "The complete message line including the timestamp")
	t.AddColumn("options", StaticUpdate, StringCol, "The part of the message after the ':'")
	t.AddColumn("plugin_output", StaticUpdate, StringCol, "The output of the check, if any is associated with the message")
	t.AddColumn("service_description", StaticUpdate, StringCol, "The description of the service log entry is about (might be empty)")
	t.AddColumn("state", StaticUpdate, IntCol, "The state of the host or service in question")
	t.AddColumn("state_type", StaticUpdate, StringCol, "The type of the state (varies on different log classes)")
	t.AddColumn("time", StaticUpdate, IntCol, "Time of the log event (UNIX timestamp)")
	t.AddColumn("type", StaticUpdate, StringCol, "The type of the message (text before the colon), the message itself for info messages")
	t.AddColumn("command_name", StaticUpdate, StringCol, "The name of the command of the log entry (e.g. for notifications)")
	t.AddColumn("current_service_contacts", StaticUpdate, StringListCol, "A list of all contacts of the service, either direct or via a contact group")
	t.AddColumn("current_host_contacts", StaticUpdate, StringListCol, "A list of all contacts of this host, either direct or via a contact group")

	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewHostsByGroupTable returns a new hostsbygroup table
func NewHostsByGroupTable() (t *Table) {
	t = &Table{Name: "hostsbygroup", GroupBy: true}
	t.AddColumn("name", StaticUpdate, StringCol, "Host name")
	t.AddColumn("hostgroup_name", StaticUpdate, StringCol, "Host group name")

	t.AddRefColumn("hosts", "", "name", "name")
	t.AddRefColumn("hostgroups", "hostgroup", "name", "hostgroup_name")

	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewServicesByGroupTable returns a new servicesbygroup table
func NewServicesByGroupTable() (t *Table) {
	t = &Table{Name: "servicesbygroup", GroupBy: true}
	t.AddColumn("host_name", StaticUpdate, StringCol, "Host name")
	t.AddColumn("description", StaticUpdate, StringCol, "Service description")
	t.AddColumn("servicegroup_name", StaticUpdate, StringCol, "Service group name")

	t.AddRefColumn("hosts", "host", "name", "host_name")
	t.AddRefColumn("services", "", "description", "description")
	t.AddRefColumn("servicegroups", "servicegroup", "name", "servicegroup_name")

	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}

// NewServicesByHostgroupTable returns a new servicesbyhostgroup table
func NewServicesByHostgroupTable() (t *Table) {
	t = &Table{Name: "servicesbyhostgroup", GroupBy: true}
	t.AddColumn("host_name", StaticUpdate, StringCol, "Host name")
	t.AddColumn("description", StaticUpdate, StringCol, "Service description")
	t.AddColumn("hostgroup_name", StaticUpdate, StringCol, "Host group name")

	t.AddRefColumn("hosts", "host", "name", "host_name")
	t.AddRefColumn("services", "", "description", "description")
	t.AddRefColumn("hostgroups", "hostgroup", "name", "hostgroup_name")

	t.AddColumn("peer_key", RefNoUpdate, VirtCol, "Id of this peer")
	t.AddColumn("peer_name", RefNoUpdate, VirtCol, "Name of this peer")

	t.AddColumn("empty", VirtUpdate, VirtCol, "placeholder for unknown columns")
	return
}
