package main

import (
	"bufio"
	"bytes"
	"testing"
)

func BenchmarkQuery(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	testPeerShutdownChannel := make(chan bool)
	mockPeer := NewPeer(Connection{Source: []string{"mock0.sock"}, Name: "Mock", ID: "mock0id"}, TestPeerWaitGroup, testPeerShutdownChannel)
	req, _, _ := NewRequest(bufio.NewReader(bytes.NewBufferString("GET services\nResponseHeader: fixed16")))

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := mockPeer.query(req)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkSingleFilter(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 0, 0)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkSingleFilter_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkSingleFilter_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkMultiFilter(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 0, 0)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString("GET hosts\nColumns: name\nFilter: name != test\nFilter: state != 5\nFilter: latency != 2\nFilter: contact_groups !=\nFilter: custom_variables != TEST blah\n")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkSimpleStats(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 0, 0)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString("GET hosts\nStats: name != \nStats: avg latency\nStats: sum latency")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkTacStats(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 0, 0)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkTacStats_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkTacStats_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkTacStats_1k_svc_100Peer(b *testing.B) {
	CheckOpenFilesLimit(b, 2000)

	b.StopTimer()
	peer := StartTestPeer(100, 10, 10)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkTacStats_5k_svc_500Peer(b *testing.B) {
	CheckOpenFilesLimit(b, 2000)

	b.StopTimer()
	peer := StartTestPeer(500, 10, 10)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkServicelistLimit_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString(servicesPageQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkServicelistLimit_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, err := peer.QueryString(servicesPageQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	StopTestPeer(peer)
}

// Test queries
var tacPageStatsQuery = `GET services
Stats: description !=
Stats: check_type = 0
Stats: check_type = 1
Stats: has_been_checked = 0
Stats: has_been_checked = 0
Stats: scheduled_downtime_depth = 0
Stats: acknowledged = 0
StatsAnd: 3
Stats: has_been_checked = 0
Stats: active_checks_enabled = 0
StatsAnd: 2
Stats: has_been_checked = 0
Stats: scheduled_downtime_depth > 0
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 0
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 0
Stats: scheduled_downtime_depth = 0
Stats: acknowledged = 0
StatsAnd: 4
Stats: has_been_checked = 1
Stats: state = 0
Stats: scheduled_downtime_depth > 0
StatsAnd: 3
Stats: check_type = 0
Stats: has_been_checked = 1
Stats: state = 0
Stats: active_checks_enabled = 0
StatsAnd: 4
Stats: check_type = 1
Stats: has_been_checked = 1
Stats: state = 0
Stats: active_checks_enabled = 0
StatsAnd: 4
Stats: has_been_checked = 1
Stats: state = 1
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 1
Stats: scheduled_downtime_depth = 0
Stats: acknowledged = 0
StatsAnd: 4
Stats: has_been_checked = 1
Stats: state = 1
Stats: scheduled_downtime_depth > 0
StatsAnd: 3
Stats: check_type = 0
Stats: has_been_checked = 1
Stats: state = 1
Stats: active_checks_enabled = 0
StatsAnd: 4
Stats: check_type = 1
Stats: has_been_checked = 1
Stats: state = 1
Stats: active_checks_enabled = 0
StatsAnd: 4
Stats: has_been_checked = 1
Stats: state = 1
Stats: acknowledged = 1
StatsAnd: 3
Stats: has_been_checked = 1
Stats: state = 1
Stats: host_state != 0
StatsAnd: 3
Stats: has_been_checked = 1
Stats: state = 1
Stats: host_state = 0
Stats: active_checks_enabled = 1
Stats: acknowledged = 0
Stats: scheduled_downtime_depth = 0
StatsAnd: 6
Stats: has_been_checked = 1
Stats: state = 2
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 2
Stats: scheduled_downtime_depth = 0
Stats: acknowledged = 0
StatsAnd: 4
Stats: has_been_checked = 1
Stats: state = 2
Stats: scheduled_downtime_depth > 0
StatsAnd: 3
Stats: check_type = 0
Stats: has_been_checked = 1
Stats: state = 2
Stats: active_checks_enabled = 0
StatsAnd: 4
Stats: check_type = 1
Stats: has_been_checked = 1
Stats: state = 2
Stats: active_checks_enabled = 0
StatsAnd: 4
Stats: has_been_checked = 1
Stats: state = 2
Stats: acknowledged = 1
StatsAnd: 3
Stats: has_been_checked = 1
Stats: state = 2
Stats: host_state != 0
StatsAnd: 3
Stats: has_been_checked = 1
Stats: state = 2
Stats: host_state = 0
Stats: active_checks_enabled = 1
Stats: acknowledged = 0
Stats: scheduled_downtime_depth = 0
StatsAnd: 6
Stats: has_been_checked = 1
Stats: state = 3
StatsAnd: 2
Stats: has_been_checked = 1
Stats: state = 3
Stats: scheduled_downtime_depth = 0
Stats: acknowledged = 0
StatsAnd: 4
Stats: has_been_checked = 1
Stats: state = 3
Stats: scheduled_downtime_depth > 0
StatsAnd: 3
Stats: check_type = 0
Stats: has_been_checked = 1
Stats: state = 3
Stats: active_checks_enabled = 0
StatsAnd: 4
Stats: check_type = 1
Stats: has_been_checked = 1
Stats: state = 3
Stats: active_checks_enabled = 0
StatsAnd: 4
Stats: has_been_checked = 1
Stats: state = 3
Stats: acknowledged = 1
StatsAnd: 3
Stats: has_been_checked = 1
Stats: state = 3
Stats: host_state != 0
StatsAnd: 3
Stats: has_been_checked = 1
Stats: state = 3
Stats: host_state = 0
Stats: active_checks_enabled = 1
Stats: acknowledged = 0
Stats: scheduled_downtime_depth = 0
StatsAnd: 6
Stats: is_flapping = 1
Stats: flap_detection_enabled = 0
Stats: notifications_enabled = 0
Stats: event_handler_enabled = 0
Stats: check_type = 0
Stats: active_checks_enabled = 0
StatsAnd: 2
Stats: check_type = 1
Stats: active_checks_enabled = 0
StatsAnd: 2
Stats: accept_passive_checks = 0
OutputFormat: json
ResponseHeader: fixed16`

var servicesPageQuery = `GET services
Columns: accept_passive_checks acknowledged action_url action_url_expanded active_checks_enabled check_command check_interval check_options check_period check_type checks_enabled comments current_attempt current_notification_number description event_handler event_handler_enabled custom_variable_names custom_variable_values execution_time first_notification_delay flap_detection_enabled groups has_been_checked high_flap_threshold host_acknowledged host_action_url_expanded host_active_checks_enabled host_address host_alias host_checks_enabled host_check_type host_latency host_plugin_output host_perf_data host_current_attempt host_check_command host_comments host_groups host_has_been_checked host_icon_image_expanded host_icon_image_alt host_is_executing host_is_flapping host_name host_notes_url_expanded host_notifications_enabled host_scheduled_downtime_depth host_state host_accept_passive_checks host_last_state_change icon_image icon_image_alt icon_image_expanded is_executing is_flapping last_check last_notification last_state_change latency long_plugin_output low_flap_threshold max_check_attempts next_check notes notes_expanded notes_url notes_url_expanded notification_interval notification_period notifications_enabled obsess_over_service percent_state_change perf_data plugin_output process_performance_data retry_interval scheduled_downtime_depth state state_type modified_attributes_list last_time_critical last_time_ok last_time_unknown last_time_warning display_name host_display_name host_custom_variable_names host_custom_variable_values in_check_period in_notification_period host_parents
Limit: 100
OutputFormat: json
ResponseHeader: fixed16`
