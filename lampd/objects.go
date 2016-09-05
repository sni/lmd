package main

type ObjectsType struct {
	Tables map[string]*Table
}

var Objects *ObjectsType

type Table struct {
	Name                   string
	MaxIndex               int
	ColumnsIndex           map[string]int
	Columns                []Column
	StaticColCacheNames    []string
	StaticColCacheIndexes  []int
	DynamicColCacheNames   []string
	DynamicColCacheIndexes []int
	RefColCacheNames       []string
	RefColCacheIndexes     []int
}

type UpdateType int

const (
	UnknownUpdate UpdateType = iota
	StaticUpdate
	DynamicUpdate
	RefUpdate
	RefNoUpdate
)

type ColumnType int

const (
	UnknownCol ColumnType = iota
	StringCol
	StringListCol
	IntCol
	IntListCol
	FloatCol
	RefCol
	TimeCol
	VirtCol
)

type Column struct {
	Name        string
	Type        ColumnType
	Index       int
	RefIndex    int
	RefColIndex int
	Update      UpdateType
}

func (t *Table) AddColumnObject(col *Column) int {
	Index := t.MaxIndex
	t.MaxIndex++
	if t.ColumnsIndex == nil {
		t.ColumnsIndex = make(map[string]int)
	}
	t.ColumnsIndex[col.Name] = Index
	col.Index = Index
	switch col.Update {
	case StaticUpdate:
		t.StaticColCacheNames = append(t.StaticColCacheNames, col.Name)
		t.StaticColCacheIndexes = append(t.StaticColCacheIndexes, col.Index)
		break
	case DynamicUpdate:
		t.DynamicColCacheNames = append(t.DynamicColCacheNames, col.Name)
		t.DynamicColCacheIndexes = append(t.DynamicColCacheIndexes, col.Index)
		break
	case RefUpdate:
		t.RefColCacheNames = append(t.RefColCacheNames, col.Name)
		t.RefColCacheIndexes = append(t.RefColCacheIndexes, col.Index)
		break
	}
	t.Columns = append(t.Columns, *col)
	return col.Index
}

func (t *Table) AddColumn(Name string, Update UpdateType, Type ColumnType) int {
	Column := Column{
		Name:   Name,
		Type:   Type,
		Update: Update,
	}
	return t.AddColumnObject(&Column)
}

func (t *Table) AddRefColumn(Ref string, Prefix string, Name string, Type ColumnType) (err error) {
	LocalColumn := Column{
		Name:   Prefix + "_" + Name,
		Type:   Type,
		Update: StaticUpdate,
	}
	LocalIndex := t.AddColumnObject(&LocalColumn)

	RefColumn := Column{
		Name:        Ref, // type of reference, ex.: hosts
		Type:        RefCol,
		Update:      RefUpdate,
		RefIndex:    LocalIndex,                             // contains the index from the local column, ex: host_name in services
		RefColIndex: Objects.Tables[Ref].ColumnsIndex[Name], // contains the index from the remote column, ex: name in host
	}
	RefIndex := t.AddColumnObject(&RefColumn)

	// expand reference columns
	_, Ok := Objects.Tables[Ref]
	if !Ok {
		panic("no such reference " + Ref + " from column " + Prefix + "_" + Name)
	}
	for _, col := range Objects.Tables[Ref].Columns {
		if col.Name != Name {
			Column := Column{
				Name:        Prefix + "_" + col.Name,
				Type:        col.Type,
				Update:      RefNoUpdate,
				RefIndex:    RefIndex,
				RefColIndex: col.Index,
			}
			t.AddColumnObject(&Column)
		}
	}
	return
}

// create all table structures
func InitObjects() (err error) {
	Objects = &ObjectsType{}

	Objects.Tables = make(map[string]*Table)
	Objects.Tables["backends"] = NewBackendsTable()
	Objects.Tables["status"] = NewStatusTable()
	Objects.Tables["timeperiods"] = NewTimeperiodsTable()
	Objects.Tables["contacts"] = NewContactsTable()
	Objects.Tables["contactgroups"] = NewContactgroupsTable()
	Objects.Tables["commands"] = NewCommandsTable()
	Objects.Tables["hosts"] = NewHostsTable()
	Objects.Tables["hostgroups"] = NewHostgroupsTable()
	Objects.Tables["services"] = NewServicesTable()
	Objects.Tables["servicegroups"] = NewServicegroupsTable()
	Objects.Tables["comments"] = NewCommentsTable()
	Objects.Tables["downtimes"] = NewDowntimesTable()
	Objects.Tables["log"] = NewLogTable()
	return
}

// add backends table definitions
func NewBackendsTable() (t *Table) {
	t = &Table{Name: "backends"}
	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	t.AddColumn("peer_name", RefNoUpdate, VirtCol)
	t.AddColumn("peer_addr", RefNoUpdate, VirtCol)
	t.AddColumn("peer_status", RefNoUpdate, VirtCol)
	t.AddColumn("peer_bytes_send", RefNoUpdate, VirtCol)
	t.AddColumn("peer_bytes_received", RefNoUpdate, VirtCol)
	t.AddColumn("peer_queries", RefNoUpdate, VirtCol)
	t.AddColumn("peer_last_error", RefNoUpdate, VirtCol)
	t.AddColumn("peer_last_update", RefNoUpdate, VirtCol)
	t.AddColumn("peer_last_online", RefNoUpdate, VirtCol)

	return
}

// add state table definitions
func NewStatusTable() (t *Table) {
	t = &Table{Name: "status"}
	t.AddColumn("accept_passive_host_checks", DynamicUpdate, IntCol)
	t.AddColumn("accept_passive_service_checks", DynamicUpdate, IntCol)
	t.AddColumn("cached_log_messages", DynamicUpdate, IntCol)
	t.AddColumn("check_external_commands", DynamicUpdate, IntCol)
	t.AddColumn("check_host_freshness", DynamicUpdate, IntCol)
	t.AddColumn("check_service_freshness", DynamicUpdate, IntCol)
	t.AddColumn("connections", DynamicUpdate, IntCol)
	t.AddColumn("connections_rate", DynamicUpdate, FloatCol)
	t.AddColumn("enable_event_handlers", DynamicUpdate, IntCol)
	t.AddColumn("enable_flap_detection", DynamicUpdate, IntCol)
	t.AddColumn("enable_notifications", DynamicUpdate, IntCol)
	t.AddColumn("execute_host_checks", DynamicUpdate, IntCol)
	t.AddColumn("execute_service_checks", DynamicUpdate, IntCol)
	t.AddColumn("forks", DynamicUpdate, IntCol)
	t.AddColumn("forks_rate", DynamicUpdate, FloatCol)
	t.AddColumn("host_checks", DynamicUpdate, IntCol)
	t.AddColumn("host_checks_rate", DynamicUpdate, FloatCol)
	t.AddColumn("interval_length", StaticUpdate, IntCol)
	t.AddColumn("last_command_check", DynamicUpdate, IntCol)
	t.AddColumn("last_log_rotation", DynamicUpdate, IntCol)
	t.AddColumn("livestatus_version", StaticUpdate, StringCol)
	t.AddColumn("log_messages", DynamicUpdate, IntCol)
	t.AddColumn("log_messages_rate", DynamicUpdate, FloatCol)
	t.AddColumn("nagios_pid", StaticUpdate, IntCol)
	t.AddColumn("neb_callbacks", DynamicUpdate, IntCol)
	t.AddColumn("neb_callbacks_rate", DynamicUpdate, FloatCol)
	t.AddColumn("obsess_over_hosts", DynamicUpdate, IntCol)
	t.AddColumn("obsess_over_services", DynamicUpdate, IntCol)
	t.AddColumn("process_performance_data", DynamicUpdate, IntCol)
	t.AddColumn("program_start", DynamicUpdate, IntCol)
	t.AddColumn("program_version", StaticUpdate, StringCol)
	t.AddColumn("requests", DynamicUpdate, FloatCol)
	t.AddColumn("requests_rate", DynamicUpdate, FloatCol)
	t.AddColumn("service_checks", DynamicUpdate, IntCol)
	t.AddColumn("service_checks_rate", DynamicUpdate, FloatCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	t.AddColumn("peer_name", RefNoUpdate, VirtCol)
	t.AddColumn("peer_addr", RefNoUpdate, VirtCol)
	t.AddColumn("peer_status", RefNoUpdate, VirtCol)
	t.AddColumn("peer_bytes_send", RefNoUpdate, VirtCol)
	t.AddColumn("peer_bytes_received", RefNoUpdate, VirtCol)
	t.AddColumn("peer_queries", RefNoUpdate, VirtCol)
	t.AddColumn("peer_last_error", RefNoUpdate, VirtCol)
	t.AddColumn("peer_last_update", RefNoUpdate, VirtCol)
	t.AddColumn("peer_last_online", RefNoUpdate, VirtCol)

	return
}

// add timeperiods table definitions
func NewTimeperiodsTable() (t *Table) {
	t = &Table{Name: "timeperiods"}
	t.AddColumn("alias", StaticUpdate, StringCol)
	t.AddColumn("name", StaticUpdate, StringCol)
	t.AddColumn("in", DynamicUpdate, IntCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add contacts table definitions
func NewContactsTable() (t *Table) {
	t = &Table{Name: "contacts"}
	t.AddColumn("alias", StaticUpdate, StringCol)
	t.AddColumn("can_submit_commands", StaticUpdate, IntCol)
	t.AddColumn("custom_variable_names", StaticUpdate, StringListCol)
	t.AddColumn("custom_variable_values", StaticUpdate, StringListCol)
	t.AddColumn("email", StaticUpdate, StringCol)
	t.AddColumn("host_notification_period", StaticUpdate, StringCol)
	t.AddColumn("host_notifications_enabled", StaticUpdate, IntCol)
	t.AddColumn("name", StaticUpdate, StringCol)
	t.AddColumn("pager", StaticUpdate, StringCol)
	t.AddColumn("service_notification_period", StaticUpdate, StringCol)
	t.AddColumn("service_notifications_enabled", StaticUpdate, IntCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add contactgroupstable definitions
func NewContactgroupsTable() (t *Table) {
	t = &Table{Name: "contacts"}
	t.AddColumn("alias", StaticUpdate, StringCol)
	t.AddColumn("members", StaticUpdate, StringListCol)
	t.AddColumn("name", StaticUpdate, StringCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add commands definitions
func NewCommandsTable() (t *Table) {
	t = &Table{Name: "commands"}
	t.AddColumn("name", StaticUpdate, StringCol)
	t.AddColumn("line", StaticUpdate, StringCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add hosts table definitions
func NewHostsTable() (t *Table) {
	t = &Table{Name: "hosts"}
	t.AddColumn("accept_passive_checks", DynamicUpdate, IntCol)
	t.AddColumn("acknowledged", DynamicUpdate, IntCol)
	t.AddColumn("action_url", StaticUpdate, StringCol)
	t.AddColumn("action_url_expanded", StaticUpdate, StringCol)
	t.AddColumn("active_checks_enabled", DynamicUpdate, IntCol)
	t.AddColumn("address", StaticUpdate, StringCol)
	t.AddColumn("alias", StaticUpdate, StringCol)
	t.AddColumn("check_command", StaticUpdate, StringCol)
	t.AddColumn("check_freshness", DynamicUpdate, IntCol)
	t.AddColumn("check_interval", StaticUpdate, IntCol)
	t.AddColumn("check_options", DynamicUpdate, IntCol)
	t.AddColumn("check_period", StaticUpdate, StringCol)
	t.AddColumn("check_type", DynamicUpdate, IntCol)
	t.AddColumn("checks_enabled", DynamicUpdate, IntCol)
	t.AddColumn("childs", StaticUpdate, StringListCol)
	t.AddColumn("contacts", StaticUpdate, StringListCol)
	t.AddColumn("contact_groups", StaticUpdate, StringListCol)
	t.AddColumn("comments", DynamicUpdate, IntListCol)
	t.AddColumn("current_attempt", DynamicUpdate, IntCol)
	t.AddColumn("current_notification_number", DynamicUpdate, IntCol)
	t.AddColumn("custom_variable_names", StaticUpdate, StringListCol)
	t.AddColumn("custom_variable_values", StaticUpdate, StringListCol)
	t.AddColumn("display_name", StaticUpdate, StringCol)
	t.AddColumn("downtimes", DynamicUpdate, IntListCol)
	t.AddColumn("event_handler", StaticUpdate, StringCol)
	t.AddColumn("event_handler_enabled", DynamicUpdate, IntCol)
	t.AddColumn("execution_time", DynamicUpdate, FloatCol)
	t.AddColumn("first_notification_delay", StaticUpdate, IntCol)
	t.AddColumn("flap_detection_enabled", DynamicUpdate, IntCol)
	t.AddColumn("groups", StaticUpdate, StringListCol)
	t.AddColumn("hard_state", DynamicUpdate, IntCol)
	t.AddColumn("has_been_checked", DynamicUpdate, IntCol)
	t.AddColumn("high_flap_threshold", StaticUpdate, IntCol)
	t.AddColumn("icon_image", StaticUpdate, StringCol)
	t.AddColumn("icon_image_alt", StaticUpdate, StringCol)
	t.AddColumn("icon_image_expanded", StaticUpdate, StringCol)
	t.AddColumn("in_check_period", DynamicUpdate, IntCol)
	t.AddColumn("in_notification_period", DynamicUpdate, IntCol)
	t.AddColumn("is_executing", DynamicUpdate, IntCol)
	t.AddColumn("is_flapping", DynamicUpdate, IntCol)
	t.AddColumn("last_check", DynamicUpdate, IntCol)
	t.AddColumn("last_hard_state", DynamicUpdate, IntCol)
	t.AddColumn("last_hard_state_change", DynamicUpdate, IntCol)
	t.AddColumn("last_notification", DynamicUpdate, IntCol)
	t.AddColumn("last_state", DynamicUpdate, IntCol)
	t.AddColumn("last_state_change", DynamicUpdate, IntCol)
	t.AddColumn("last_time_down", DynamicUpdate, IntCol)
	t.AddColumn("last_time_unreachable", DynamicUpdate, IntCol)
	t.AddColumn("last_time_up", DynamicUpdate, IntCol)
	t.AddColumn("latency", DynamicUpdate, FloatCol)
	t.AddColumn("long_plugin_output", DynamicUpdate, StringCol)
	t.AddColumn("low_flap_threshold", StaticUpdate, IntCol)
	t.AddColumn("max_check_attempts", StaticUpdate, IntCol)
	t.AddColumn("modified_attributes", DynamicUpdate, IntCol)
	t.AddColumn("modified_attributes_list", DynamicUpdate, StringListCol)
	t.AddColumn("name", StaticUpdate, StringCol)
	t.AddColumn("next_check", DynamicUpdate, IntCol)
	t.AddColumn("next_notification", DynamicUpdate, IntCol)
	t.AddColumn("num_services_crit", DynamicUpdate, IntCol)
	t.AddColumn("num_services_ok", DynamicUpdate, IntCol)
	t.AddColumn("num_services_pending", DynamicUpdate, IntCol)
	t.AddColumn("num_services_unknown", DynamicUpdate, IntCol)
	t.AddColumn("num_services_warn", DynamicUpdate, IntCol)
	t.AddColumn("num_services", StaticUpdate, IntCol)
	t.AddColumn("notes", StaticUpdate, StringCol)
	t.AddColumn("notes_expanded", StaticUpdate, StringCol)
	t.AddColumn("notes_url", StaticUpdate, StringCol)
	t.AddColumn("notes_url_expanded", StaticUpdate, StringCol)
	t.AddColumn("notification_interval", StaticUpdate, IntCol)
	t.AddColumn("notification_period", StaticUpdate, StringCol)
	t.AddColumn("notifications_enabled", DynamicUpdate, IntCol)
	t.AddColumn("obsess_over_host", DynamicUpdate, IntCol)
	t.AddColumn("parents", StaticUpdate, StringListCol)
	t.AddColumn("percent_state_change", DynamicUpdate, FloatCol)
	t.AddColumn("perf_data", DynamicUpdate, StringCol)
	t.AddColumn("plugin_output", DynamicUpdate, StringCol)
	t.AddColumn("process_performance_data", DynamicUpdate, IntCol)
	t.AddColumn("retry_interval", StaticUpdate, IntCol)
	t.AddColumn("scheduled_downtime_depth", DynamicUpdate, IntCol)
	t.AddColumn("state", DynamicUpdate, IntCol)
	t.AddColumn("state_type", DynamicUpdate, IntCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add hostgroups definitions
func NewHostgroupsTable() (t *Table) {
	t = &Table{Name: "hostgroups"}
	t.AddColumn("action_url", StaticUpdate, StringCol)
	t.AddColumn("alias", StaticUpdate, StringCol)
	t.AddColumn("members", StaticUpdate, StringListCol)
	t.AddColumn("name", StaticUpdate, StringCol)
	t.AddColumn("notes", StaticUpdate, StringCol)
	t.AddColumn("notes_url", StaticUpdate, StringCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add services table definitions
func NewServicesTable() (t *Table) {
	t = &Table{Name: "services"}
	t.AddColumn("accept_passive_checks", DynamicUpdate, IntCol)
	t.AddColumn("acknowledged", DynamicUpdate, IntCol)
	t.AddColumn("acknowledgement_type", DynamicUpdate, IntCol)
	t.AddColumn("action_url", StaticUpdate, StringCol)
	t.AddColumn("action_url_expanded", StaticUpdate, StringCol)
	t.AddColumn("active_checks_enabled", DynamicUpdate, IntCol)
	t.AddColumn("check_command", StaticUpdate, StringCol)
	t.AddColumn("check_interval", StaticUpdate, IntCol)
	t.AddColumn("check_options", DynamicUpdate, IntCol)
	t.AddColumn("check_period", StaticUpdate, StringCol)
	t.AddColumn("check_type", DynamicUpdate, IntCol)
	t.AddColumn("checks_enabled", DynamicUpdate, IntCol)
	t.AddColumn("contacts", StaticUpdate, StringListCol)
	t.AddColumn("contact_groups", StaticUpdate, StringListCol)
	t.AddColumn("comments", DynamicUpdate, IntListCol)
	t.AddColumn("current_attempt", DynamicUpdate, IntCol)
	t.AddColumn("current_notification_number", DynamicUpdate, IntCol)
	t.AddColumn("custom_variable_names", StaticUpdate, StringListCol)
	t.AddColumn("custom_variable_values", StaticUpdate, StringListCol)
	t.AddColumn("description", StaticUpdate, StringCol)
	t.AddColumn("downtimes", DynamicUpdate, IntListCol)
	t.AddColumn("display_name", StaticUpdate, StringCol)
	t.AddColumn("event_handler", StaticUpdate, StringCol)
	t.AddColumn("event_handler_enabled", DynamicUpdate, IntCol)
	t.AddColumn("execution_time", DynamicUpdate, FloatCol)
	t.AddColumn("first_notification_delay", DynamicUpdate, IntCol)
	t.AddColumn("flap_detection_enabled", DynamicUpdate, IntCol)
	t.AddColumn("groups", StaticUpdate, StringListCol)
	t.AddColumn("has_been_checked", DynamicUpdate, IntCol)
	t.AddColumn("high_flap_threshold", StaticUpdate, IntCol)
	t.AddColumn("icon_image", StaticUpdate, StringCol)
	t.AddColumn("icon_image_alt", StaticUpdate, StringCol)
	t.AddColumn("icon_image_expanded", StaticUpdate, StringCol)
	t.AddColumn("in_check_period", DynamicUpdate, IntCol)
	t.AddColumn("in_notification_period", DynamicUpdate, IntCol)
	t.AddColumn("initial_state", StaticUpdate, IntCol)
	t.AddColumn("is_executing", DynamicUpdate, IntCol)
	t.AddColumn("is_flapping", DynamicUpdate, IntCol)
	t.AddColumn("last_check", DynamicUpdate, IntCol)
	t.AddColumn("last_hard_state", DynamicUpdate, IntCol)
	t.AddColumn("last_hard_state_change", DynamicUpdate, IntCol)
	t.AddColumn("last_notification", DynamicUpdate, IntCol)
	t.AddColumn("last_state", DynamicUpdate, IntCol)
	t.AddColumn("last_state_change", DynamicUpdate, IntCol)
	t.AddColumn("last_time_critical", DynamicUpdate, IntCol)
	t.AddColumn("last_time_warning", DynamicUpdate, IntCol)
	t.AddColumn("last_time_ok", DynamicUpdate, IntCol)
	t.AddColumn("last_time_unknown", DynamicUpdate, IntCol)
	t.AddColumn("latency", DynamicUpdate, FloatCol)
	t.AddColumn("long_plugin_output", DynamicUpdate, StringCol)
	t.AddColumn("low_flap_threshold", DynamicUpdate, IntCol)
	t.AddColumn("max_check_attempts", StaticUpdate, IntCol)
	t.AddColumn("modified_attributes", DynamicUpdate, IntCol)
	t.AddColumn("modified_attributes_list", DynamicUpdate, StringListCol)
	t.AddColumn("next_check", DynamicUpdate, IntCol)
	t.AddColumn("next_notification", DynamicUpdate, IntCol)
	t.AddColumn("notes", StaticUpdate, StringCol)
	t.AddColumn("notes_expanded", StaticUpdate, StringCol)
	t.AddColumn("notes_url", StaticUpdate, StringCol)
	t.AddColumn("notes_url_expanded", StaticUpdate, StringCol)
	t.AddColumn("notification_interval", StaticUpdate, IntCol)
	t.AddColumn("notification_period", StaticUpdate, StringCol)
	t.AddColumn("notifications_enabled", DynamicUpdate, IntCol)
	t.AddColumn("obsess_over_service", DynamicUpdate, IntCol)
	t.AddColumn("percent_state_change", DynamicUpdate, FloatCol)
	t.AddColumn("perf_data", DynamicUpdate, StringCol)
	t.AddColumn("plugin_output", DynamicUpdate, StringCol)
	t.AddColumn("process_performance_data", DynamicUpdate, IntCol)
	t.AddColumn("retry_interval", StaticUpdate, IntCol)
	t.AddColumn("scheduled_downtime_depth", DynamicUpdate, IntCol)
	t.AddColumn("state", DynamicUpdate, IntCol)
	t.AddColumn("state_type", DynamicUpdate, IntCol)

	t.AddRefColumn("hosts", "host", "name", StringCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add hostgroups definitions
func NewServicegroupsTable() (t *Table) {
	t = &Table{Name: "servicegroups"}
	t.AddColumn("action_url", StaticUpdate, StringCol)
	t.AddColumn("alias", StaticUpdate, StringCol)
	t.AddColumn("members", StaticUpdate, StringListCol)
	t.AddColumn("name", StaticUpdate, StringCol)
	t.AddColumn("notes", StaticUpdate, StringCol)
	t.AddColumn("notes_url", StaticUpdate, StringCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add comments definitions
func NewCommentsTable() (t *Table) {
	t = &Table{Name: "comments"}
	t.AddColumn("author", StaticUpdate, StringCol)
	t.AddColumn("comment", StaticUpdate, StringCol)
	t.AddColumn("entry_time", StaticUpdate, IntCol)
	t.AddColumn("entry_type", StaticUpdate, IntCol)
	t.AddColumn("expires", StaticUpdate, IntCol)
	t.AddColumn("expire_time", StaticUpdate, IntCol)
	t.AddColumn("id", StaticUpdate, IntCol)
	t.AddColumn("is_service", StaticUpdate, IntCol)
	t.AddColumn("persistent", StaticUpdate, IntCol)
	t.AddColumn("source", StaticUpdate, IntCol)
	t.AddColumn("type", StaticUpdate, IntCol)
	t.AddColumn("host_name", StaticUpdate, StringCol)
	t.AddColumn("service_description", StaticUpdate, StringCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add downtimes definitions
func NewDowntimesTable() (t *Table) {
	t = &Table{Name: "downtimes"}
	t.AddColumn("author", StaticUpdate, StringCol)
	t.AddColumn("comment", StaticUpdate, StringCol)
	t.AddColumn("duration", StaticUpdate, IntCol)
	t.AddColumn("end_time", StaticUpdate, IntCol)
	t.AddColumn("entry_time", StaticUpdate, IntCol)
	t.AddColumn("fixed", StaticUpdate, IntCol)
	t.AddColumn("id", StaticUpdate, IntCol)
	t.AddColumn("is_service", StaticUpdate, IntCol)
	t.AddColumn("start_time", StaticUpdate, IntCol)
	t.AddColumn("triggered_by", StaticUpdate, IntCol)
	t.AddColumn("type", StaticUpdate, IntCol)

	t.AddColumn("host_name", StaticUpdate, StringCol)
	t.AddColumn("service_description", StaticUpdate, StringCol)

	t.AddColumn("peer_key", RefNoUpdate, VirtCol)
	return
}

// add log table definitions
func NewLogTable() (t *Table) {
	t = &Table{Name: "log"}

	t.AddColumn("attempt", StaticUpdate, IntCol)
	t.AddColumn("class", StaticUpdate, IntCol)
	t.AddColumn("contact_name", StaticUpdate, StringCol)
	t.AddColumn("host_name", StaticUpdate, StringCol)
	t.AddColumn("lineno", StaticUpdate, IntCol)
	t.AddColumn("message", StaticUpdate, StringCol)
	t.AddColumn("options", StaticUpdate, StringCol)
	t.AddColumn("plugin_output", StaticUpdate, StringCol)
	t.AddColumn("service_description", StaticUpdate, StringCol)
	t.AddColumn("state", StaticUpdate, IntCol)
	t.AddColumn("state_type", StaticUpdate, StringCol)
	t.AddColumn("time", StaticUpdate, IntCol)
	t.AddColumn("type", StaticUpdate, StringCol)

	return
}
