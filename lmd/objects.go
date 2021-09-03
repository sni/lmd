package main

// ObjectsType is a map of tables with a given order.
type ObjectsType struct {
	noCopy       noCopy
	Tables       map[TableName]*Table
	Order        []TableName
	UpdateTables []TableName // list of tables which need to be regularly updated
	StatusTables []TableName
}

// InitObjects creates the initial table object structures.
func InitObjects() {
	if Objects != nil {
		return
	}
	Objects = &ObjectsType{
		StatusTables: []TableName{TableStatus},
	}

	// generate virtual keys with peer and host_peer prefix
	for i := range VirtualColumnList {
		dat := &(VirtualColumnList[i])
		VirtualColumnMap[dat.Name] = dat
		if dat.StatusKey > 0 {
			VirtualColumnMap["peer_"+dat.Name] = dat
			VirtualColumnMap["host_peer_"+dat.Name] = dat
		} else {
			VirtualColumnMap["host_"+dat.Name] = dat
		}
	}

	Objects.Tables = make(map[TableName]*Table)
	// add complete virtual tables first
	Objects.AddTable(TableBackends, NewBackendsTable())
	Objects.AddTable(TableSites, NewBackendsTable())
	Objects.AddTable(TableColumns, NewColumnsTable())
	Objects.AddTable(TableTables, NewColumnsTable())

	// add remaining tables in an order where they can resolve the inter-table dependencies
	Objects.AddTable(TableStatus, NewStatusTable())
	Objects.AddTable(TableTimeperiods, NewTimeperiodsTable())
	Objects.AddTable(TableContacts, NewContactsTable())
	Objects.AddTable(TableContactgroups, NewContactgroupsTable())
	Objects.AddTable(TableCommands, NewCommandsTable())
	Objects.AddTable(TableHosts, NewHostsTable())
	Objects.AddTable(TableHostgroups, NewHostgroupsTable())
	Objects.AddTable(TableServices, NewServicesTable())
	Objects.AddTable(TableServicegroups, NewServicegroupsTable())
	Objects.AddTable(TableComments, NewCommentsTable())
	Objects.AddTable(TableDowntimes, NewDowntimesTable())
	Objects.AddTable(TableLog, NewLogTable())
	Objects.AddTable(TableHostsbygroup, NewHostsByGroupTable())
	Objects.AddTable(TableServicesbygroup, NewServicesByGroupTable())
	Objects.AddTable(TableServicesbyhostgroup, NewServicesByHostgroupTable())
}

// AddTable appends a table object to the Objects and verifies that no table is added twice.
func (o *ObjectsType) AddTable(name TableName, table *Table) {
	table.Name = name
	_, exists := o.Tables[name]
	if exists {
		log.Panicf("table %s has been added twice", name.String())
	}
	if table.PrimaryKey == nil {
		table.PrimaryKey = make([]string, 0)
	}
	table.SetColumnIndex()
	o.Tables[name] = table
	o.Order = append(o.Order, name)
	if !table.PassthroughOnly && table.Virtual == nil {
		o.UpdateTables = append(o.UpdateTables, name)
	}
}

// NewBackendsTable returns a new backends table
func NewBackendsTable() (t *Table) {
	t = &Table{Virtual: GetTableBackendsStore, WorksUnlocked: true, PeerLockMode: PeerLockModeFull}
	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	t.AddPeerInfoColumn("key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("name", StringCol, "Name of this peer")
	t.AddPeerInfoColumn("addr", StringCol, "Address of this peer")
	t.AddPeerInfoColumn("status", IntCol, "Status of this peer (0 - UP, 1 - Stale, 2 - Down, 4 - Pending)")
	t.AddPeerInfoColumn("bytes_send", Int64Col, "Bytes send to this peer")
	t.AddPeerInfoColumn("bytes_received", Int64Col, "Bytes received from this peer")
	t.AddPeerInfoColumn("queries", IntCol, "Number of queries sent to this peer")
	t.AddPeerInfoColumn("last_error", StringCol, "Last error message or empty if up")
	t.AddPeerInfoColumn("last_update", Int64Col, "Timestamp of last update")
	t.AddPeerInfoColumn("last_online", Int64Col, "Timestamp when peer was last online")
	t.AddPeerInfoColumn("response_time", FloatCol, "Duration of last update in seconds")
	t.AddPeerInfoColumn("idling", IntCol, "Idle status of this backend (0 - Not idling, 1 - idling)")
	t.AddPeerInfoColumn("last_query", Int64Col, "Timestamp of the last incoming request")
	t.AddPeerInfoColumn("section", StringCol, "Section information when having cascaded LMDs")
	t.AddPeerInfoColumn("parent", StringCol, "Parent id when having cascaded LMDs")
	t.AddPeerInfoColumn("lmd_version", StringCol, "LMD version string")
	t.AddPeerInfoColumn("flags", StringListCol, "peer flags")
	t.AddPeerInfoColumn("configtool", JSONCol, "Thruks config tool configuration if available")
	t.AddPeerInfoColumn("thruk", JSONCol, "Thruks extra data if available")
	t.AddPeerInfoColumn("federation_key", StringListCol, "original keys when using nested federation")
	t.AddPeerInfoColumn("federation_name", StringListCol, "original names when using nested federation")
	t.AddPeerInfoColumn("federation_addr", StringListCol, "original addresses when using nested federation")
	t.AddPeerInfoColumn("federation_type", StringListCol, "original types when using nested federation")
	t.AddExtraColumn("localtime", VirtualStore, None, FloatCol, NoFlags, "The unix timestamp of the local lmd host.")
	return
}

// NewColumnsTable returns a new columns table
func NewColumnsTable() (t *Table) {
	t = &Table{Virtual: GetTableColumnsStore, DefaultSort: []string{"table", "name"}, WorksUnlocked: true}
	t.AddExtraColumn("name", LocalStore, None, StringCol, NoFlags, "The name of the column within the table")
	t.AddExtraColumn("table", LocalStore, None, StringCol, NoFlags, "The name of the table")
	t.AddExtraColumn("type", LocalStore, None, StringCol, NoFlags, "The data type of the column (int, float, string, list)")
	t.AddExtraColumn("description", LocalStore, None, StringCol, NoFlags, "A description of the column")
	t.AddExtraColumn("lmd_update", LocalStore, None, StringCol, NoFlags, "The lmd update type")
	t.AddExtraColumn("lmd_datatype", LocalStore, None, StringCol, NoFlags, "The lmd column type")
	t.AddExtraColumn("lmd_storagetype", LocalStore, None, StringCol, NoFlags, "The lmd storage type")
	t.AddExtraColumn("lmd_flags", LocalStore, None, StringListCol, NoFlags, "The lmd flags for this column")
	return
}

// NewStatusTable returns a new status table
func NewStatusTable() (t *Table) {
	t = &Table{}
	t.AddColumn("program_start", Dynamic, Int64Col, "The time of the last program start as UNIX timestamp")
	t.AddColumn("accept_passive_host_checks", Dynamic, IntCol, "The number of host checks since program start")
	t.AddColumn("accept_passive_service_checks", Dynamic, IntCol, "The number of completed service checks since program start")
	t.AddColumn("cached_log_messages", Dynamic, Int64Col, "The current number of log messages MK Livestatus keeps in memory")
	t.AddColumn("check_external_commands", Dynamic, IntCol, "Whether the core checks for external commands at its command pipe (0/1)")
	t.AddColumn("check_host_freshness", Dynamic, IntCol, "Whether host freshness checking is activated in general (0/1)")
	t.AddColumn("check_service_freshness", Dynamic, IntCol, "Whether service freshness checking is activated in general (0/1)")
	t.AddColumn("connections", Dynamic, Int64Col, "The number of client connections to Livestatus since program start")
	t.AddColumn("connections_rate", Dynamic, FloatCol, "The number of client connections to Livestatus since program start")
	t.AddColumn("enable_event_handlers", Dynamic, IntCol, "Whether event handlers are activated in general (0/1)")
	t.AddColumn("enable_flap_detection", Dynamic, IntCol, "Whether flap detection is activated in general (0/1)")
	t.AddColumn("enable_notifications", Dynamic, IntCol, "Whether notifications are enabled in general (0/1)")
	t.AddColumn("execute_host_checks", Dynamic, IntCol, "The number of host checks since program start")
	t.AddColumn("execute_service_checks", Dynamic, IntCol, "The number of completed service checks since program start")
	t.AddColumn("forks", Dynamic, Int64Col, "The number of process creations since program start")
	t.AddColumn("forks_rate", Dynamic, FloatCol, "The number of process creations since program start")
	t.AddColumn("host_checks", Dynamic, Int64Col, "The number of host checks since program start")
	t.AddColumn("host_checks_rate", Dynamic, FloatCol, "The number of host checks since program start")
	t.AddColumn("interval_length", Static, IntCol, "The default interval length from the core configuration")
	t.AddColumn("last_command_check", Dynamic, Int64Col, "The time of the last check for a command as UNIX timestamp")
	t.AddColumn("last_log_rotation", Dynamic, Int64Col, "Time time of the last log file rotation")
	t.AddColumn("livestatus_version", Static, StringCol, "The version of the MK Livestatus module")
	t.AddColumn("log_messages", Dynamic, Int64Col, "The number of new log messages since program start")
	t.AddColumn("log_messages_rate", Dynamic, FloatCol, "The number of new log messages since program start")
	t.AddColumn("nagios_pid", Dynamic, IntCol, "The process ID of the core main process")
	t.AddColumn("neb_callbacks", Dynamic, Int64Col, "The number of NEB call backs since program start")
	t.AddColumn("neb_callbacks_rate", Dynamic, FloatCol, "The number of NEB call backs since program start")
	t.AddColumn("obsess_over_hosts", Dynamic, IntCol, "Whether the core will obsess over host checks (0/1)")
	t.AddColumn("obsess_over_services", Dynamic, IntCol, "Whether the core will obsess over service checks and run the ocsp_command (0/1)")
	t.AddColumn("process_performance_data", Dynamic, IntCol, "Whether processing of performance data is activated in general (0/1)")
	t.AddColumn("program_version", Static, StringCol, "The version of the monitoring daemon")
	t.AddColumn("requests", Dynamic, Int64Col, "The number of requests to Livestatus since program start")
	t.AddColumn("requests_rate", Dynamic, FloatCol, "The number of requests to Livestatus since program start")
	t.AddColumn("service_checks", Dynamic, Int64Col, "The number of completed service checks since program start")
	t.AddColumn("service_checks_rate", Dynamic, FloatCol, "The number of completed service checks since program start")

	t.AddPeerInfoColumn("lmd_last_cache_update", Int64Col, "Timestamp of the last LMD update of this object")
	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	t.AddPeerInfoColumn("peer_section", StringCol, "Section information when having cascaded LMDs")
	t.AddPeerInfoColumn("peer_addr", StringCol, "Address of this peer")
	t.AddPeerInfoColumn("peer_status", IntCol, "Status of this peer (0 - UP, 1 - Stale, 2 - Down, 4 - Pending)")
	t.AddPeerInfoColumn("peer_bytes_send", Int64Col, "Bytes send to this peer")
	t.AddPeerInfoColumn("peer_bytes_received", Int64Col, "Bytes received to this peer")
	t.AddPeerInfoColumn("peer_queries", Int64Col, "Number of queries sent to this peer")
	t.AddPeerInfoColumn("peer_last_error", StringCol, "Last error message or empty if up")
	t.AddPeerInfoColumn("peer_last_update", Int64Col, "Timestamp of last update")
	t.AddPeerInfoColumn("peer_last_online", Int64Col, "Timestamp when peer was last online")
	t.AddPeerInfoColumn("peer_response_time", FloatCol, "Duration of last update in seconds")
	t.AddPeerInfoColumn("configtool", JSONCol, "Thruks config tool configuration if available")
	t.AddPeerInfoColumn("thruk", JSONCol, "Thruks extra information if available")

	t.AddExtraColumn("localtime", VirtualStore, None, FloatCol, NoFlags, "The unix timestamp of the local lmd host.")
	return
}

// NewTimeperiodsTable returns a new timeperiods table
func NewTimeperiodsTable() (t *Table) {
	t = &Table{DefaultSort: []string{"name"}}
	t.AddColumn("alias", Static, StringCol, "The alias of the timeperiod")
	t.AddColumn("name", Static, StringCol, "The name of the timeperiod")
	t.AddColumn("in", Dynamic, IntCol, "Wether we are currently in this period (0/1)")

	// naemon specific
	t.AddExtraColumn("days", LocalStore, Static, InterfaceListCol, Naemon, "days")
	t.AddExtraColumn("exceptions_calendar_dates", LocalStore, Static, InterfaceListCol, Naemon, "exceptions_calendar_dates")
	t.AddExtraColumn("exceptions_month_date", LocalStore, Static, InterfaceListCol, Naemon, "exceptions_month_date")
	t.AddExtraColumn("exceptions_month_day", LocalStore, Static, InterfaceListCol, Naemon, "exceptions_month_day")
	t.AddExtraColumn("exceptions_month_week_day", LocalStore, Static, InterfaceListCol, Naemon, "exceptions_month_week_day")
	t.AddExtraColumn("exceptions_week_day", LocalStore, Static, InterfaceListCol, Naemon, "exceptions_week_day")
	t.AddExtraColumn("exclusions", LocalStore, Static, StringListCol, Naemon, "exclusions")
	t.AddExtraColumn("id", LocalStore, Static, IntCol, Naemon, "The id of the timeperiods")

	t.AddPeerInfoColumn("lmd_last_cache_update", Int64Col, "Timestamp of the last LMD update of this object")
	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewContactsTable returns a new contacts table
func NewContactsTable() (t *Table) {
	t = &Table{DefaultSort: []string{"name"}}
	t.AddColumn("alias", Static, StringCol, "The full name of the contact")
	t.AddColumn("can_submit_commands", Static, IntCol, "Wether the contact is allowed to submit commands (0/1)")
	t.AddColumn("email", Static, StringCol, "The email address of the contact")
	t.AddColumn("host_notification_period", Static, StringCol, "The time period in which the contact will be notified about host problems")
	t.AddColumn("host_notifications_enabled", Static, IntCol, "Wether the contact will be notified about host problems in general (0/1)")
	t.AddColumn("name", Static, StringCol, "The login name of the contact person")
	t.AddColumn("pager", Static, StringCol, "The pager address of the contact")
	t.AddColumn("service_notification_period", Static, StringCol, "The time period in which the contact will be notified about service problems")
	t.AddColumn("service_notifications_enabled", Static, IntCol, "Wether the contact will be notified about service problems in general (0/1)")

	t.AddExtraColumn("custom_variable_names", LocalStore, Dynamic, StringListCol, Naemon, "A list of all custom variables of the contact")
	t.AddExtraColumn("custom_variable_values", LocalStore, Dynamic, StringListCol, Naemon, "A list of the values of all custom variables of the contact")
	t.AddExtraColumn("custom_variables", VirtualStore, None, CustomVarCol, NoFlags, "A dictionary of the custom variables")

	t.AddPeerInfoColumn("lmd_last_cache_update", Int64Col, "Timestamp of the last LMD update of this object")
	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewContactgroupsTable returns a new contactgroups table
func NewContactgroupsTable() (t *Table) {
	t = &Table{DefaultSort: []string{"name"}}
	t.AddColumn("alias", Static, StringCol, "The alias of the contactgroup")
	t.AddColumn("members", Static, StringListCol, "A list of all members of this contactgroup")
	t.AddColumn("name", Static, StringCol, "The name of the contactgroup")

	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewCommandsTable returns a new commands table
func NewCommandsTable() (t *Table) {
	t = &Table{DefaultSort: []string{"name"}}
	t.AddColumn("name", Static, StringCol, "The name of the command")
	t.AddColumn("line", Static, StringCol, "The shell command line")

	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewHostsTable returns a new hosts table
func NewHostsTable() (t *Table) {
	t = &Table{PrimaryKey: []string{"name"}, DefaultSort: []string{"name"}}
	t.AddColumn("accept_passive_checks", Dynamic, IntCol, "Whether passive host checks are accepted (0/1)")
	t.AddColumn("acknowledged", Dynamic, IntCol, "Whether the current host problem has been acknowledged (0/1)")
	t.AddColumn("action_url", Static, StringCol, "An optional URL to custom actions or information about this host")
	t.AddColumn("action_url_expanded", Static, StringCol, "An optional URL to custom actions or information about this host")
	t.AddColumn("active_checks_enabled", Dynamic, IntCol, "Whether active checks are enabled for the host (0/1)")
	t.AddColumn("address", Static, StringCol, "IP address")
	t.AddColumn("alias", Static, StringCol, "An alias name for the host")
	t.AddColumn("check_command", Static, StringCol, "Naemon command for active host check of this host")
	t.AddColumn("check_freshness", Dynamic, IntCol, "Whether freshness checks are activated (0/1)")
	t.AddColumn("check_interval", Static, IntCol, "Number of basic interval lengths between two scheduled checks of the host")
	t.AddColumn("check_options", Dynamic, IntCol, "The current check option, forced, normal, freshness... (0-2)")
	t.AddColumn("check_period", Static, StringCol, "Time period in which this host will be checked. If empty then the host will always be checked")
	t.AddColumn("check_type", Dynamic, IntCol, "Type of check (0: active, 1: passive)")
	t.AddColumn("checks_enabled", Dynamic, IntCol, "Whether checks of the host are enabled (0/1)")
	t.AddColumn("childs", Static, StringListCol, "A list of all direct childs of the host")
	t.AddColumn("contacts", Static, StringListCol, "A list of all contacts of this host, either direct or via a contact group")
	t.AddColumn("contact_groups", Static, StringListCol, "A list of all contact groups this host is in")
	t.AddColumn("current_attempt", Dynamic, IntCol, "Number of the current check attempts")
	t.AddColumn("current_notification_number", Dynamic, IntCol, "Number of the current notification")
	t.AddColumn("custom_variable_names", Static, StringListCol, "A list of the names of all custom variables")
	t.AddColumn("custom_variable_values", Dynamic, StringListCol, "A list of the values of the custom variables")
	t.AddColumn("display_name", Static, StringCol, "Optional display name of the host")
	t.AddColumn("event_handler_enabled", Dynamic, IntCol, "Naemon command used as event handler")
	t.AddColumn("execution_time", Dynamic, FloatCol, "Time the host check needed for execution")
	t.AddColumn("first_notification_delay", Static, IntCol, "Delay before the first notification")
	t.AddColumn("flap_detection_enabled", Dynamic, IntCol, "Whether flap detection is enabled (0/1)")
	t.AddColumn("groups", Static, StringListCol, "A list of all host groups this host is in")
	t.AddColumn("hard_state", Dynamic, IntCol, "The effective hard state of the host (eliminates a problem in hard_state)")
	t.AddColumn("has_been_checked", Dynamic, IntCol, "Whether the host has already been checked (0/1)")
	t.AddColumn("high_flap_threshold", Static, IntCol, "High threshold of flap detection")
	t.AddColumn("icon_image", Static, StringCol, "The name of an image file to be used in the web pages")
	t.AddColumn("icon_image_alt", Static, StringCol, "The name of an image file to be used in the web pages")
	t.AddColumn("icon_image_expanded", Static, StringCol, "The name of an image file to be used in the web pages")
	t.AddColumn("in_check_period", Dynamic, IntCol, "Time period in which this host will be checked. If empty then the host will always be checked")
	t.AddColumn("in_notification_period", Dynamic, IntCol, "Time period in which problems of this host will be notified. If empty then notification will be always")
	t.AddColumn("initial_state", Static, IntCol, "Initial host state")
	t.AddColumn("is_executing", Dynamic, IntCol, "is there a host check currently running... (0/1)")
	t.AddColumn("is_flapping", Dynamic, IntCol, "Whether the host state is flapping (0/1)")
	t.AddColumn("last_check", Dynamic, Int64Col, "Time of the last check (Unix timestamp)")
	t.AddColumn("last_hard_state", Dynamic, IntCol, "The effective hard state of the host (eliminates a problem in hard_state)")
	t.AddColumn("last_hard_state_change", Dynamic, IntCol, "The effective hard state of the host (eliminates a problem in hard_state)")
	t.AddColumn("last_notification", Dynamic, Int64Col, "Time of the last notification (Unix timestamp)")
	t.AddColumn("last_state", Dynamic, IntCol, "State before last state change")
	t.AddColumn("last_state_change", Dynamic, IntCol, "State before last state change")
	t.AddColumn("last_time_down", Dynamic, Int64Col, "The last time the host was DOWN (Unix timestamp)")
	t.AddColumn("last_time_unreachable", Dynamic, Int64Col, "The last time the host was UNREACHABLE (Unix timestamp)")
	t.AddColumn("last_time_up", Dynamic, Int64Col, "The last time the host was UP (Unix timestamp)")
	t.AddColumn("latency", Dynamic, FloatCol, "Time difference between scheduled check time and actual check time")
	t.AddColumn("long_plugin_output", Dynamic, StringLargeCol, "Complete output from check plugin")
	t.AddColumn("low_flap_threshold", Static, IntCol, "Low threshold of flap detection")
	t.AddColumn("max_check_attempts", Static, IntCol, "Max check attempts for active host checks")
	t.AddColumn("modified_attributes", Dynamic, IntCol, "A bitmask specifying which attributes have been modified")
	t.AddColumn("modified_attributes_list", Dynamic, StringListCol, "A bitmask specifying which attributes have been modified")
	t.AddColumn("name", Static, StringCol, "Host name")
	t.AddColumn("next_check", Dynamic, Int64Col, "Scheduled time for the next check (Unix timestamp)")
	t.AddColumn("next_notification", Dynamic, Int64Col, "Time of the next notification (Unix timestamp)")
	t.AddColumn("num_services", Static, IntCol, "The total number of services of the host")
	t.AddColumn("num_services_crit", Dynamic, IntCol, "The number of the host's services with the soft state CRIT")
	t.AddColumn("num_services_ok", Dynamic, IntCol, "The number of the host's services with the soft state OK")
	t.AddColumn("num_services_pending", Dynamic, IntCol, "The number of the host's services which have not been checked yet (pending)")
	t.AddColumn("num_services_unknown", Dynamic, IntCol, "The number of the host's services with the soft state UNKNOWN")
	t.AddColumn("num_services_warn", Dynamic, IntCol, "The number of the host's services with the soft state WARN")
	t.AddColumn("num_services_hard_crit", Dynamic, IntCol, "The number of the host's services with the hard state CRIT")
	t.AddColumn("num_services_hard_ok", Dynamic, IntCol, "The number of the host's services with the hard state OK")
	t.AddColumn("num_services_hard_unknown", Dynamic, IntCol, "The number of the host's services with the hard state UNKNOWN")
	t.AddColumn("num_services_hard_warn", Dynamic, IntCol, "The number of the host's services with the hard state WARN")
	t.AddColumn("notes", Static, StringCol, "Optional notes for this host")
	t.AddColumn("notes_expanded", Static, StringCol, "Optional notes for this host")
	t.AddColumn("notes_url", Static, StringCol, "Optional notes for this host")
	t.AddColumn("notes_url_expanded", Static, StringCol, "Optional notes for this host")
	t.AddColumn("notification_interval", Static, IntCol, "Interval of periodic notification or 0 if its off")
	t.AddColumn("notification_period", Static, StringCol, "Time period in which problems of this host will be notified. If empty then notification will be always")
	t.AddColumn("notifications_enabled", Dynamic, IntCol, "Whether notifications of the host are enabled (0/1)")
	t.AddColumn("obsess_over_host", Dynamic, IntCol, "The current obsess_over_host setting... (0/1)")
	t.AddColumn("parents", Static, StringListCol, "A list of all direct parents of the host")
	t.AddColumn("percent_state_change", Dynamic, FloatCol, "Percent state change")
	t.AddColumn("perf_data", Dynamic, StringLargeCol, "Optional performance data of the last host check")
	t.AddColumn("plugin_output", Dynamic, StringLargeCol, "Output of the last host check")
	t.AddColumn("process_performance_data", Dynamic, IntCol, "Whether processing of performance data is enabled (0/1)")
	t.AddColumn("retry_interval", Static, IntCol, "Number of basic interval lengths between checks when retrying after a soft error")
	t.AddColumn("scheduled_downtime_depth", Dynamic, IntCol, "The number of downtimes this host is currently in")
	t.AddColumn("services", Static, StringListCol, "The services associated with the host")
	t.AddColumn("state", Dynamic, IntCol, "The current state of the host (0: up, 1: down, 2: unreachable)")
	t.AddColumn("state_type", Dynamic, IntCol, "The current state of the host (0: up, 1: down, 2: unreachable)")
	t.AddColumn("pnpgraph_present", Dynamic, IntCol, "The pnp graph presence (0/1)")
	t.AddColumn("worst_service_hard_state", Dynamic, IntCol, "The worst hard state of all of the host's services (OK <= WARN <= UNKNOWN <= CRIT)")
	t.AddColumn("worst_service_state", Dynamic, IntCol, "The worst soft state of all of the host's services (OK <= WARN <= UNKNOWN <= CRIT)")

	// will be updated from the comments/downtimes update, no need to fetch twice
	t.AddExtraColumn("comments", LocalStore, None, Int64ListCol, NoFlags, "A list of the ids of all comments of this host")
	t.AddExtraColumn("downtimes", LocalStore, None, Int64ListCol, NoFlags, "A list of the ids of all scheduled downtimes of this host")

	// lowercase columns are used to make case insensitive filter faster
	t.AddExtraColumn("name_lc", LocalStore, None, StringCol, NoFlags, "Host name (lowercase)")
	t.AddExtraColumn("alias_lc", LocalStore, None, StringCol, NoFlags, "An alias name for the host (lowercase)")
	t.AddExtraColumn("address_lc", LocalStore, None, StringCol, NoFlags, "IP address (lowercase)")
	t.AddExtraColumn("display_name_lc", LocalStore, None, StringCol, NoFlags, "Optional display name of the host (lowercase)")

	// backend specific
	t.AddExtraColumn("check_source", LocalStore, Dynamic, StringCol, Naemon|Icinga2, "Host check source address")

	// naemon specific
	t.AddExtraColumn("obsess", LocalStore, Dynamic, IntCol, Naemon, "The obsessing over host")
	t.AddExtraColumn("depends_exec", LocalStore, Static, StringListCol, HasDependencyColumn, "List of hosts this hosts depends on for execution")
	t.AddExtraColumn("depends_notify", LocalStore, Static, StringListCol, HasDependencyColumn, "List of hosts this hosts depends on for notification")
	t.AddExtraColumn("should_be_scheduled", LocalStore, Dynamic, IntCol, Naemon, "Whether Naemon still tries to run checks on this host (0/1)")
	t.AddExtraColumn("hourly_value", LocalStore, Static, IntCol, Naemon, "Hourly Value")
	t.AddExtraColumn("event_handler", LocalStore, Static, StringCol, HasEventHandlerColumn, "Naemon command used as event handler")
	t.AddExtraColumn("staleness", LocalStore, Dynamic, FloatCol, HasStalenessColumn, "Staleness indicator for this host")

	// shinken specific
	t.AddExtraColumn("is_impact", LocalStore, Dynamic, IntCol, Shinken, "Whether the host state is an impact or not (0/1)")
	t.AddExtraColumn("business_impact", LocalStore, Static, IntCol, Shinken, "An importance level. From 0 (not important) to 5 (top for business)")
	t.AddExtraColumn("source_problems", LocalStore, Dynamic, StringListCol, Shinken, "The name of the source problems (host or service)")
	t.AddExtraColumn("impacts", LocalStore, Dynamic, StringListCol, Shinken, "List of what the source impact (list of hosts and services)")
	t.AddExtraColumn("criticity", LocalStore, Dynamic, IntCol, Shinken, "The importance we gave to this host between the minimum 0 and the maximum 5")
	t.AddExtraColumn("is_problem", LocalStore, Dynamic, IntCol, Shinken, "Whether the host state is a problem or not (0/1)")
	t.AddExtraColumn("realm", LocalStore, Dynamic, StringCol, Shinken, "Realm")
	t.AddExtraColumn("poller_tag", LocalStore, Dynamic, StringCol, Shinken, "Poller Tag")
	t.AddExtraColumn("got_business_rule", LocalStore, Dynamic, IntCol, Shinken, "Whether the host state is an business rule based host or not (0/1)")
	t.AddExtraColumn("parent_dependencies", LocalStore, Dynamic, StringCol, Shinken, "List of the dependencies (logical, network or business one) of this host")

	// icinga2 specific
	t.AddExtraColumn("address6", LocalStore, Static, StringCol, Icinga2, "IPv6 address")

	t.AddExtraColumn("custom_variables", VirtualStore, None, CustomVarCol, NoFlags, "A dictionary of the custom variables")
	t.AddExtraColumn("services_with_info", VirtualStore, None, InterfaceListCol, NoFlags, "The services, including info, that is associated with the host")
	t.AddExtraColumn("services_with_state", VirtualStore, None, InterfaceListCol, NoFlags, "The services, including state info, that is associated with the host")
	t.AddExtraColumn("comments_with_info", VirtualStore, None, InterfaceListCol, NoFlags, "A list of all comments of the host with id, author and comment")
	t.AddExtraColumn("downtimes_with_info", VirtualStore, None, InterfaceListCol, NoFlags, "A list of all downtimes of the host with id, author and comment")
	t.AddPeerInfoColumn("lmd_last_cache_update", Int64Col, "Timestamp of the last LMD update of this object")
	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	t.AddExtraColumn("last_state_change_order", VirtualStore, None, Int64Col, NoFlags, "The last_state_change of this host suitable for sorting. Returns program_start from the core if host has been never checked")
	t.AddExtraColumn("has_long_plugin_output", VirtualStore, None, IntCol, NoFlags, "Flag wether this host has long_plugin_output or not")
	t.AddExtraColumn("total_services", VirtualStore, None, IntCol, NoFlags, "The total number of services of the host")
	return
}

// NewHostgroupsTable returns a new hostgroups table
func NewHostgroupsTable() (t *Table) {
	t = &Table{PrimaryKey: []string{"name"}, DefaultSort: []string{"name"}}
	t.AddColumn("action_url", Static, StringCol, "An optional URL to custom actions or information about the hostgroup")
	t.AddColumn("alias", Static, StringCol, "An alias of the hostgroup")
	t.AddColumn("members", Static, StringListCol, "A list of all host names that are members of the hostgroup")
	t.AddColumn("name", Static, StringCol, "Name of the hostgroup")
	t.AddColumn("notes", Static, StringCol, "Optional notes to the hostgroup")
	t.AddColumn("notes_url", Static, StringCol, "An optional URL with further information about the hostgroup")
	t.AddColumn("num_hosts", Static, IntCol, "The total number of hosts of the hostgroup")
	t.AddColumn("num_hosts_up", Dynamic, IntCol, "The total number of up hosts of the hostgroup")
	t.AddColumn("num_hosts_down", Dynamic, IntCol, "The total number of down hosts of the hostgroup")
	t.AddColumn("num_hosts_unreach", Dynamic, IntCol, "The total number of unreachable hosts of the hostgroup")
	t.AddColumn("num_hosts_pending", Dynamic, IntCol, "The total number of down hosts of the hostgroup")
	t.AddColumn("num_services", Static, IntCol, "The total number of services of the hostgroup")
	t.AddColumn("num_services_ok", Dynamic, IntCol, "The total number of ok services of the hostgroup")
	t.AddColumn("num_services_warn", Dynamic, IntCol, "The total number of warning services of the hostgroup")
	t.AddColumn("num_services_crit", Dynamic, IntCol, "The total number of critical services of the hostgroup")
	t.AddColumn("num_services_unknown", Dynamic, IntCol, "The total number of unknown services of the hostgroup")
	t.AddColumn("num_services_pending", Dynamic, IntCol, "The total number of pending services of the hostgroup")
	t.AddColumn("num_services_hard_crit", Dynamic, IntCol, "The total number of services with the state CRIT of hosts in this group")
	t.AddColumn("num_services_hard_ok", Dynamic, IntCol, "The total number of services with the state OK of hosts in this group")
	t.AddColumn("num_services_hard_unknown", Dynamic, IntCol, "The total number of services with the state UNKNOWN of hosts in this group")
	t.AddColumn("num_services_hard_warn", Dynamic, IntCol, "The total number of services with the state WARN of hosts in this group")
	t.AddColumn("worst_host_state", Dynamic, IntCol, "The worst host state of the hostgroup")
	t.AddColumn("worst_service_hard_state", Dynamic, IntCol, "The worst state of all services that belong to a host of this group (OK <= WARN <= UNKNOWN <= CRIT)")
	t.AddColumn("worst_service_state", Dynamic, IntCol, "The worst service state of the hostgroup")

	t.AddPeerInfoColumn("lmd_last_cache_update", Int64Col, "Timestamp of the last LMD update of this object")
	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")

	t.AddExtraColumn("members_with_state", VirtualStore, None, InterfaceListCol, NoFlags, "A list of all host names that are members of the hostgroup together with state and has_been_checked")
	return
}

// NewServicesTable returns a new services table
func NewServicesTable() (t *Table) {
	t = &Table{PrimaryKey: []string{"host_name", "description"}, DefaultSort: []string{"host_name", "description"}}
	t.AddColumn("accept_passive_checks", Dynamic, IntCol, "Whether the service accepts passive checks (0/1)")
	t.AddColumn("acknowledged", Dynamic, IntCol, "Whether the current service problem has been acknowledged (0/1)")
	t.AddColumn("acknowledgement_type", Dynamic, IntCol, "The type of the acknowledgement (0: none, 1: normal, 2: sticky)")
	t.AddColumn("action_url", Static, StringCol, "An optional URL for actions or custom information about the service")
	t.AddColumn("action_url_expanded", Static, StringCol, "An optional URL for actions or custom information about the service")
	t.AddColumn("active_checks_enabled", Dynamic, IntCol, "Whether active checks are enabled for the service (0/1)")
	t.AddColumn("check_command", Static, StringCol, "Naemon command used for active checks")
	t.AddColumn("check_interval", Static, IntCol, "Number of basic interval lengths between two scheduled checks of the service")
	t.AddColumn("check_options", Dynamic, IntCol, "The current check option, forced, normal, freshness... (0/1)")
	t.AddColumn("check_period", Static, StringCol, "The name of the check period of the service. It this is empty, the service is always checked")
	t.AddColumn("check_type", Dynamic, IntCol, "The type of the last check (0: active, 1: passive)")
	t.AddColumn("checks_enabled", Dynamic, IntCol, "Whether active checks are enabled for the service (0/1)")
	t.AddColumn("contacts", Static, StringListCol, "A list of all contacts of the service, either direct or via a contact group")
	t.AddColumn("contact_groups", Static, StringListCol, "A list of all contact groups this service is in")
	t.AddColumn("current_attempt", Dynamic, IntCol, "The number of the current check attempt")
	t.AddColumn("current_notification_number", Dynamic, IntCol, "The number of the current notification")
	t.AddColumn("custom_variable_names", Static, StringListCol, "A list of the names of all custom variables of the service")
	t.AddColumn("custom_variable_values", Dynamic, StringListCol, "A list of the values of all custom variable of the service")
	t.AddColumn("description", Static, StringCol, "Description of the service (also used as key)")
	t.AddColumn("display_name", Static, StringCol, "An optional display name")
	t.AddColumn("event_handler", Static, StringCol, "Naemon command used as event handler")
	t.AddColumn("event_handler_enabled", Dynamic, IntCol, "Naemon command used as event handler")
	t.AddColumn("execution_time", Dynamic, FloatCol, "Time the service check needed for execution")
	t.AddColumn("first_notification_delay", Dynamic, IntCol, "Delay before the first notification")
	t.AddColumn("flap_detection_enabled", Dynamic, IntCol, "Whether flap detection is enabled for the service (0/1)")
	t.AddColumn("groups", Static, StringListCol, "A list of all service groups the service is in")
	t.AddColumn("has_been_checked", Dynamic, IntCol, "Whether the service already has been checked (0/1)")
	t.AddColumn("high_flap_threshold", Static, IntCol, "High threshold of flap detection")
	t.AddColumn("icon_image", Static, StringCol, "The name of an image to be used as icon in the web interface")
	t.AddColumn("icon_image_alt", Static, StringCol, "The name of an image to be used as icon in the web interface")
	t.AddColumn("icon_image_expanded", Static, StringCol, "The name of an image to be used as icon in the web interface")
	t.AddColumn("in_check_period", Dynamic, IntCol, "The name of the check period of the service. It this is empty, the service is always checked")
	t.AddColumn("in_notification_period", Dynamic, IntCol, "The name of the notification period of the service. It this is empty, service problems are always notified")
	t.AddColumn("initial_state", Static, IntCol, "The initial state of the service")
	t.AddColumn("is_executing", Dynamic, IntCol, "is there a service check currently running... (0/1)")
	t.AddColumn("is_flapping", Dynamic, IntCol, "Whether the service is flapping (0/1)")
	t.AddColumn("last_check", Dynamic, Int64Col, "The time of the last check (Unix timestamp)")
	t.AddColumn("last_hard_state", Dynamic, IntCol, "The last hard state of the service")
	t.AddColumn("last_hard_state_change", Dynamic, IntCol, "The last hard state of the service")
	t.AddColumn("last_notification", Dynamic, Int64Col, "The time of the last notification (Unix timestamp)")
	t.AddColumn("last_state", Dynamic, IntCol, "The last state of the service")
	t.AddColumn("last_state_change", Dynamic, IntCol, "The last state of the service")
	t.AddColumn("last_time_critical", Dynamic, Int64Col, "The last time the service was CRITICAL (Unix timestamp)")
	t.AddColumn("last_time_warning", Dynamic, Int64Col, "The last time the service was in WARNING state (Unix timestamp)")
	t.AddColumn("last_time_ok", Dynamic, Int64Col, "The last time the service was OK (Unix timestamp)")
	t.AddColumn("last_time_unknown", Dynamic, Int64Col, "The last time the service was UNKNOWN (Unix timestamp)")
	t.AddColumn("latency", Dynamic, FloatCol, "Time difference between scheduled check time and actual check time")
	t.AddColumn("long_plugin_output", Dynamic, StringLargeCol, "Unabbreviated output of the last check plugin")
	t.AddColumn("low_flap_threshold", Dynamic, IntCol, "Low threshold of flap detection")
	t.AddColumn("max_check_attempts", Static, IntCol, "The maximum number of check attempts")
	t.AddColumn("modified_attributes", Dynamic, IntCol, "A bitmask specifying which attributes have been modified")
	t.AddColumn("modified_attributes_list", Dynamic, StringListCol, "A bitmask specifying which attributes have been modified")
	t.AddColumn("next_check", Dynamic, Int64Col, "The scheduled time of the next check (Unix timestamp)")
	t.AddColumn("next_notification", Dynamic, Int64Col, "The time of the next notification (Unix timestamp)")
	t.AddColumn("notes", Static, StringCol, "Optional notes about the service")
	t.AddColumn("notes_expanded", Static, StringCol, "Optional notes about the service")
	t.AddColumn("notes_url", Static, StringCol, "Optional notes about the service")
	t.AddColumn("notes_url_expanded", Static, StringCol, "Optional notes about the service")
	t.AddColumn("notification_interval", Static, IntCol, "Interval of periodic notification or 0 if its off")
	t.AddColumn("notification_period", Static, StringCol, "The name of the notification period of the service. It this is empty, service problems are always notified")
	t.AddColumn("notifications_enabled", Dynamic, IntCol, "Whether notifications are enabled for the service (0/1)")
	t.AddColumn("obsess_over_service", Dynamic, IntCol, "Whether 'obsess_over_service' is enabled for the service (0/1)")
	t.AddColumn("percent_state_change", Dynamic, FloatCol, "Percent state change")
	t.AddColumn("perf_data", Dynamic, StringLargeCol, "Performance data of the last check plugin")
	t.AddColumn("plugin_output", Dynamic, StringLargeCol, "Output of the last check plugin")
	t.AddColumn("process_performance_data", Dynamic, IntCol, "Whether processing of performance data is enabled for the service (0/1)")
	t.AddColumn("retry_interval", Static, IntCol, "Number of basic interval lengths between checks when retrying after a soft error")
	t.AddColumn("scheduled_downtime_depth", Dynamic, IntCol, "The number of scheduled downtimes the service is currently in")
	t.AddColumn("state", Dynamic, IntCol, "The current state of the service (0: OK, 1: WARN, 2: CRITICAL, 3: UNKNOWN)")
	t.AddColumn("state_type", Dynamic, IntCol, "The current state of the service (0: OK, 1: WARN, 2: CRITICAL, 3: UNKNOWN)")
	t.AddColumn("host_name", Static, StringCol, "Host name")
	t.AddColumn("pnpgraph_present", Dynamic, IntCol, "The pnp graph presence (0/1)")

	// will be updated from the comments/downtimes update, no need to fetch twice
	t.AddExtraColumn("comments", LocalStore, None, Int64ListCol, NoFlags, "A list of the ids of all comments of this service")
	t.AddExtraColumn("downtimes", LocalStore, None, Int64ListCol, NoFlags, "A list of the ids of all scheduled downtimes of this service")

	// lowercase columns are used to make case insensitive filter faster
	t.AddExtraColumn("host_name_lc", LocalStore, None, StringCol, NoFlags, "Host name (lowercase)")
	t.AddExtraColumn("description_lc", LocalStore, None, StringCol, NoFlags, "Description of the service (lowercase)")
	t.AddExtraColumn("display_name_lc", LocalStore, None, StringCol, NoFlags, "An optional display name (lowercase)")

	// backend specific
	t.AddExtraColumn("check_source", LocalStore, Dynamic, StringCol, Naemon|Icinga2, "Check source address")

	// naemon specific
	t.AddExtraColumn("obsess", LocalStore, Dynamic, IntCol, Naemon, "The obsessing over service")
	t.AddExtraColumn("depends_exec", LocalStore, Static, ServiceMemberListCol, HasDependencyColumn, "List of services this services depends on for execution")
	t.AddExtraColumn("depends_notify", LocalStore, Static, ServiceMemberListCol, HasDependencyColumn, "List of services this services depends on for notification")
	t.AddExtraColumn("parents", LocalStore, Static, StringListCol, HasServiceParentsColumn, "List of services descriptions this services depends on")
	t.AddExtraColumn("should_be_scheduled", LocalStore, Dynamic, IntCol, Naemon, "Whether Naemon still tries to run checks on this service (0/1)")
	t.AddExtraColumn("hourly_value", LocalStore, Static, IntCol, Naemon, "Hourly Value")
	t.AddExtraColumn("check_freshness", LocalStore, Dynamic, IntCol, HasCheckFreshnessColumn, "Whether freshness checks are activated (0/1)")
	t.AddExtraColumn("staleness", LocalStore, Dynamic, FloatCol, HasStalenessColumn, "Staleness indicator for this host")

	// shinken specific
	t.AddExtraColumn("is_impact", LocalStore, Dynamic, IntCol, Shinken, "Whether the host state is an impact or not (0/1)")
	t.AddExtraColumn("business_impact", LocalStore, Static, IntCol, Shinken, "An importance level. From 0 (not important) to 5 (top for business)")
	t.AddExtraColumn("source_problems", LocalStore, Dynamic, StringListCol, Shinken, "The name of the source problems (host or service)")
	t.AddExtraColumn("impacts", LocalStore, Dynamic, StringListCol, Shinken, "List of what the source impact (list of hosts and services)")
	t.AddExtraColumn("criticity", LocalStore, Dynamic, IntCol, Shinken, "The importance we gave to this service between the minimum 0 and the maximum 5")
	t.AddExtraColumn("is_problem", LocalStore, Dynamic, IntCol, Shinken, "Whether the host state is a problem or not (0/1)")
	t.AddExtraColumn("realm", LocalStore, Dynamic, StringCol, Shinken, "Realm")
	t.AddExtraColumn("poller_tag", LocalStore, Dynamic, StringCol, Shinken, "Poller Tag")
	t.AddExtraColumn("got_business_rule", LocalStore, Dynamic, IntCol, Shinken, "Whether the service state is an business rule based host or not (0/1)")
	t.AddExtraColumn("parent_dependencies", LocalStore, Dynamic, StringCol, Shinken, "List of the dependencies (logical, network or business one) of this service")

	t.AddRefColumns(TableHosts, "host", []string{"host_name"})

	t.AddExtraColumn("custom_variables", VirtualStore, None, CustomVarCol, NoFlags, "A dictionary of the custom variables")
	t.AddExtraColumn("comments_with_info", VirtualStore, None, InterfaceListCol, NoFlags, "A list of all comments of the host with id, author and comment")
	t.AddExtraColumn("downtimes_with_info", VirtualStore, None, InterfaceListCol, NoFlags, "A list of all downtimes of the service with id, author and comment")
	t.AddPeerInfoColumn("lmd_last_cache_update", Int64Col, "Timestamp of the last LMD update of this object")
	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	t.AddExtraColumn("last_state_change_order", VirtualStore, None, Int64Col, NoFlags, "The last_state_change of this host suitable for sorting. Returns program_start from the core if host has been never checked")
	t.AddExtraColumn("state_order", VirtualStore, None, IntCol, NoFlags, "The service state suitable for sorting. Unknown and Critical state are switched")
	t.AddExtraColumn("has_long_plugin_output", VirtualStore, None, IntCol, NoFlags, "Flag wether this service has long_plugin_output or not")
	return
}

// NewServicegroupsTable returns a new hostgroups table
func NewServicegroupsTable() (t *Table) {
	t = &Table{PrimaryKey: []string{"name"}, DefaultSort: []string{"name"}}
	t.AddColumn("action_url", Static, StringCol, "An optional URL to custom notes or actions on the service group")
	t.AddColumn("alias", Static, StringCol, "An alias of the service group")
	t.AddColumn("members", Static, ServiceMemberListCol, "A list of all members of the service group as host/service pairs")
	t.AddColumn("name", Static, StringCol, "The name of the service group")
	t.AddColumn("notes", Static, StringCol, "Optional additional notes about the service group")
	t.AddColumn("notes_url", Static, StringCol, "An optional URL to further notes on the service group")
	t.AddColumn("num_services", Static, IntCol, "The total number of services of the service group")
	t.AddColumn("num_services_ok", Dynamic, IntCol, "The total number of ok services of the service group")
	t.AddColumn("num_services_warn", Dynamic, IntCol, "The total number of warning services of the service group")
	t.AddColumn("num_services_crit", Dynamic, IntCol, "The total number of critical services of the service group")
	t.AddColumn("num_services_unknown", Dynamic, IntCol, "The total number of unknown services of the service group")
	t.AddColumn("num_services_pending", Dynamic, IntCol, "The total number of pending services of the service group")
	t.AddColumn("num_services_hard_crit", Dynamic, IntCol, "The number of services in the group that are CRIT")
	t.AddColumn("num_services_hard_ok", Dynamic, IntCol, "The number of services in the group that are OK")
	t.AddColumn("num_services_hard_unknown", Dynamic, IntCol, "The number of services in the group that are UNKNOWN")
	t.AddColumn("num_services_hard_warn", Dynamic, IntCol, "The number of services in the group that are WARN")
	t.AddColumn("worst_service_state", Dynamic, IntCol, "The worst service state of the service group")

	t.AddPeerInfoColumn("lmd_last_cache_update", Int64Col, "Timestamp of the last LMD update of this object")
	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")

	t.AddExtraColumn("members_with_state", VirtualStore, None, InterfaceListCol, NoFlags, "A list of all members of the service group with state and has_been_checked")
	return
}

// NewCommentsTable returns a new comments table
func NewCommentsTable() (t *Table) {
	t = &Table{PrimaryKey: []string{"id"}, DefaultSort: []string{"id"}}
	t.AddColumn("author", Static, StringCol, "The contact that entered the comment")
	t.AddColumn("comment", Static, StringCol, "A comment text")
	t.AddColumn("entry_time", Static, Int64Col, "The time the entry was made as UNIX timestamp")
	t.AddColumn("entry_type", Static, IntCol, "The type of the comment: 1 is user, 2 is downtime, 3 is flap and 4 is acknowledgement")
	t.AddColumn("expires", Static, IntCol, "Whether this comment expires")
	t.AddColumn("expire_time", Static, Int64Col, "The time of expiry of this comment as a UNIX timestamp")
	t.AddColumn("id", Static, Int64Col, "The id of the comment")
	t.AddColumn("is_service", Static, IntCol, "0, if this entry is for a host, 1 if it is for a service")
	t.AddColumn("persistent", Static, IntCol, "Whether this comment is persistent (0/1)")
	t.AddColumn("source", Static, IntCol, "The source of the comment (0 is internal and 1 is external)")
	t.AddColumn("type", Static, IntCol, "The type of the comment: 1 is host, 2 is service")
	t.AddColumn("host_name", Static, StringCol, "Host name")
	t.AddColumn("service_description", Static, StringCol, "Description of the service (also used as key)")

	t.AddRefColumns(TableHosts, "host", []string{"host_name"})
	t.AddRefColumns(TableServices, "service", []string{"host_name", "service_description"})

	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewDowntimesTable returns a new downtimes table
func NewDowntimesTable() (t *Table) {
	t = &Table{PrimaryKey: []string{"id"}, DefaultSort: []string{"id"}}
	t.AddColumn("author", Static, StringCol, "The contact that scheduled the downtime")
	t.AddColumn("comment", Static, StringCol, "A comment text")
	t.AddColumn("duration", Static, IntCol, "The duration of the downtime in seconds")
	t.AddColumn("end_time", Static, Int64Col, "The end time of the downtime as UNIX timestamp")
	t.AddColumn("entry_time", Static, Int64Col, "The time the entry was made as UNIX timestamp")
	t.AddColumn("fixed", Static, IntCol, "1 if the downtime is fixed, a 0 if it is flexible")
	t.AddColumn("id", Static, Int64Col, "The id of the downtime")
	t.AddColumn("is_service", Static, IntCol, "0, if this entry is for a host, 1 if it is for a service")
	t.AddColumn("start_time", Static, Int64Col, "The start time of the downtime as UNIX timestamp")
	t.AddColumn("triggered_by", Static, IntCol, "The id of the downtime this downtime was triggered by or 0 if it was not triggered by another downtime")
	t.AddColumn("type", Static, IntCol, "The type of the downtime: 0 if it is active, 1 if it is pending")
	t.AddColumn("host_name", Static, StringCol, "Host name")
	t.AddColumn("service_description", Static, StringCol, "Description of the service (also used as key)")

	t.AddRefColumns(TableHosts, "host", []string{"host_name"})
	t.AddRefColumns(TableServices, "service", []string{"host_name", "service_description"})

	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewLogTable returns a new log table
func NewLogTable() (t *Table) {
	t = &Table{PassthroughOnly: true, DefaultSort: []string{"time"}}

	t.AddColumn("attempt", Static, IntCol, "The number of the check attempt")
	t.AddColumn("class", Static, IntCol, "The class of the message as integer (0:info, 1:state, 2:program, 3:notification, 4:passive, 5:command)")
	t.AddColumn("contact_name", Static, StringCol, "The name of the contact the log entry is about (might be empty)")
	t.AddColumn("host_name", Static, StringCol, "The name of the host the log entry is about (might be empty)")
	t.AddColumn("lineno", Static, IntCol, "The number of the line in the log file")
	t.AddColumn("message", Static, StringCol, "The complete message line including the timestamp")
	t.AddColumn("options", Static, StringCol, "The part of the message after the ':'")
	t.AddColumn("plugin_output", Static, StringCol, "The output of the check, if any is associated with the message")
	t.AddColumn("service_description", Static, StringCol, "The description of the service log entry is about (might be empty)")
	t.AddColumn("state", Static, IntCol, "The state of the host or service in question")
	t.AddColumn("state_type", Static, StringCol, "The type of the state (varies on different log classes)")
	t.AddColumn("time", Static, Int64Col, "Time of the log event (UNIX timestamp)")
	t.AddColumn("type", Static, StringCol, "The type of the message (text before the colon), the message itself for info messages")
	t.AddColumn("command_name", Static, StringCol, "The name of the command of the log entry (e.g. for notifications)")
	t.AddColumn("current_service_contacts", Static, StringListCol, "A list of all contacts of the service, either direct or via a contact group")
	t.AddColumn("current_host_contacts", Static, StringListCol, "A list of all contacts of this host, either direct or via a contact group")

	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewHostsByGroupTable returns a new hostsbygroup table
func NewHostsByGroupTable() (t *Table) {
	t = &Table{Virtual: GetGroupByData}
	t.AddColumn("name", Static, StringCol, "Host name")
	t.AddColumn("hostgroup_name", Static, StringCol, "Host group name")

	t.AddRefColumns(TableHosts, "", []string{"name"})
	t.AddRefColumns(TableHostgroups, "hostgroup", []string{"hostgroup_name"})

	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewServicesByGroupTable returns a new servicesbygroup table
func NewServicesByGroupTable() (t *Table) {
	t = &Table{Virtual: GetGroupByData}
	t.AddColumn("host_name", Static, StringCol, "Host name")
	t.AddColumn("description", Static, StringCol, "Service description")
	t.AddColumn("servicegroup_name", Static, StringCol, "Service group name")

	t.AddRefColumns(TableHosts, "host", []string{"host_name"})
	t.AddRefColumns(TableServices, "", []string{"host_name", "description"})
	t.AddRefColumns(TableServicegroups, "servicegroup", []string{"servicegroup_name"})

	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}

// NewServicesByHostgroupTable returns a new servicesbyhostgroup table
func NewServicesByHostgroupTable() (t *Table) {
	t = &Table{Virtual: GetGroupByData}
	t.AddColumn("host_name", Static, StringCol, "Host name")
	t.AddColumn("description", Static, StringCol, "Service description")
	t.AddColumn("hostgroup_name", Static, StringCol, "Host group name")

	t.AddRefColumns(TableHosts, "host", []string{"host_name"})
	t.AddRefColumns(TableServices, "", []string{"host_name", "description"})
	t.AddRefColumns(TableHostgroups, "hostgroup", []string{"hostgroup_name"})

	t.AddPeerInfoColumn("peer_key", StringCol, "Id of this peer")
	t.AddPeerInfoColumn("peer_name", StringCol, "Name of this peer")
	return
}
