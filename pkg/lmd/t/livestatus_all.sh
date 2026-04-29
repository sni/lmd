#/bin/bash

# This script is intended to run on an OMD system
# It behaves like the startup table initialization of an LMD peer, doing the same livestatus queries.

# status
printf "GET status\nResponseHeader: fixed16\nOutputFormat: json\nColumns: program_start accept_passive_host_checks accept_passive_service_checks cached_log_messages check_external_commands check_host_freshness check_service_freshness connections connections_rate enable_event_handlers enable_flap_detection enable_notifications execute_host_checks execute_service_checks forks forks_rate host_checks host_checks_rate interval_length last_command_check last_log_rotation livestatus_version log_messages log_messages_rate nagios_pid neb_callbacks neb_callbacks_rate obsess_over_hosts obsess_over_services process_performance_data program_version requests requests_rate service_checks service_checks_rate\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/status.livestatus.response

# columns
printf "GET columns\nResponseHeader: fixed16\nOutputFormat: json\nColumns: table name\n" | unixcat ~/tmp/run/live > ~/columns.livestatus.response

# contactgroups
printf "GET contactgroups\nResponseHeader: fixed16\nOutputFormat: json\nColumns: alias members name\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/contactgroups.livestatus.response

# contacts
printf "GET contacts\nResponseHeader: fixed16\nOutputFormat: json\nColumns: alias name can_submit_commands email pager host_notification_period host_notifications_enabled service_notification_period service_notifications_enabled id modified_attributes modified_attributes_list in_service_notification_period in_host_notification_period address1 address2 address3 address4 address5 address6 custom_variable_names custom_variable_values groups host_notification_commands service_notification_commands\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/contacts.livestatus.response

# servicegroups
printf "GET servicegroups\nResponseHeader: fixed16\nOutputFormat: json\nColumns: action_url alias members name notes notes_url num_services num_services_ok num_services_warn num_services_crit num_services_unknown num_services_pending num_services_hard_crit num_services_hard_ok num_services_hard_unknown num_services_hard_warn worst_service_state\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/servicegroups.livestatus.response

# hosts
printf "GET hosts\nResponseHeader: fixed16\nOutputFormat: json\nColumns: accept_passive_checks acknowledged action_url action_url_expanded active_checks_enabled address alias check_command check_freshness check_interval check_options check_period check_type checks_enabled childs contacts contact_groups current_attempt current_notification_number custom_variable_names custom_variable_values display_name event_handler_enabled execution_time first_notification_delay flap_detection_enabled groups hard_state has_been_checked high_flap_threshold icon_image icon_image_alt icon_image_expanded in_check_period in_notification_period initial_state is_executing is_flapping last_check last_hard_state last_hard_state_change last_notification last_state last_state_change last_time_down last_time_unreachable last_time_up latency long_plugin_output low_flap_threshold max_check_attributes modified_attributes modified_attributes_list name next_check next_notification num_services num_services_crit num_services_ok num_services_pending num_services_unknown num_services_warn num_services_hard_crit num_services_hard_ok num_services_hard_unknown num_services_hard_warn obses_over_host percent_state_change perf_data plugin_output process_performance_data retry_interval scheduled_downtime_depth services state state_type pnpgraph_present worst_service_hard_state worst_service_state\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/hosts.livestatus.response

# services
printf "GET services\nResponseHeader: fixed16\nOutputFormat: json\nColumns: accept_passive_checks acknowledged action_url action_url_expanded active_checks_enabled check_command check_interval check_options check_period check_type checks_enabled comments current_attempt current_notification_number description event_handler event_handler_enabled custom_variable_names custom_variable_values execution_time first_notification_delay flap_detection_enabled groups has_been_checked high_flap_threshold host_acknowledged host_action_url_expanded host_active_checks_enabled host_address host_alias host_checks_enabled host_check_type host_latency host_plugin_output host_perf_data host_current_attempt host_check_command host_comments host_groups host_has_been_checked host_icon_image_expanded host_icon_image_alt host_is_executing host_is_flapping host_name host_notes_url_expanded host_notifications_enabled host_scheduled_downtime_depth host_state host_accept_passive_checks host_last_state_change icon_image icon_image_alt icon_image_expanded is_executing is_flapping last_check last_notification last_state_change latency long_plugin_output low_flap_threshold max_check_attempts next_check notes notes_expanded notes_url notes_url_expanded notification_interval notification_period notifications_enabled obsess_over_service percent_state_change perf_data plugin_output process_performance_data retry_interval scheduled_downtime_depth state state_type modified_attributes_list last_time_critical last_time_ok last_time_unknown last_time_warning display_name host_display_name host_custom_variable_names host_custom_variable_values in_check_period in_notification_period host_parents\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/services.livestatus.response

# commands
printf "GET commands\nResponseHeader: fixed16\nOutputFormat: json\nColumns: name line\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/commands.livestatus.response

# comments
printf "GET comments\nResponseHeader: fixed16\nOutputFormat: json\nColumns: author comment entry_time entry_type expires expire_time id is_service persistent source type host_name service_description\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/comments.livestatus.response

# hostgroups
printf "GET hostgroups\nResponseHeader: fixed16\nOutputFormat: json\nColumns: action_url alias members name notes notes_url num_hosts num_hosts_up num_hosts_down num_hosts_unreach num_hosts_pending num_services num_services_ok num_services_warn num_services_crit num_services_unknown num_services_pending num_services_hard_crit num_services_hard_ok num_services_hard_unknown num_services_hard_warn worst_host_state worst_service_hard_state worst_service_state\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/hostgroups.livestatus.response

# timeperiods
printf "GET timeperiods\nResponseHeader: fixed16\nOutputFormat: json\nColumns: alias name in days exceptions_calendar_dates exceptions_month_date exceptions_month_day exceptions_month_week_day exceptions_week_day exclusions id\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/timeperiods.livestatus.response

# downtimes
printf "GET downtimes\nResponseHeader: fixed16\nOutputFormat: json\nColumns: author comment duration end_time entry_time fixed id is_service start_time triggered_by type host_name service_description\nLimit: 500000\nKeepAlive: on\n" | unixcat ~/tmp/run/live > ~/downtimes.livestatus.response
