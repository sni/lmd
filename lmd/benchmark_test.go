package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkParseResultJSON(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	columns := make([]string, 0)
	for i := range peer.Tables[TableServices].Table.Columns {
		col := peer.Tables[TableServices].Table.Columns[i]
		if col.StorageType == LocalStore && col.Optional == NoFlags {
			columns = append(columns, col.Name)
		}
	}
	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString(fmt.Sprintf("GET services\nOutputFormat: json\nColumns: %s\nColumnHeaders: on\n", strings.Join(columns, " ")))))
	if err != nil {
		panic(err.Error())
	}
	conn, connType, err := peer.GetConnection()
	if err != nil {
		panic(err.Error())
	}
	resBytes, err := peer.getQueryResponse(req, req.String(), peer.Status["PeerAddr"].(string), conn, connType)
	if err != nil {
		panic(err.Error())
	}

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res, _, err := req.parseResult(resBytes)
		if err != nil {
			panic(err.Error())
		}
		if len(*res) != 1001 {
			b.Fatalf("wrong result size, expected 1001, got %d", len(*res))
		}
		if len((*res)[0]) != len(columns) {
			b.Fatalf("wrong result size, expected %d, got %d", len(columns), len((*res)[0]))
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkParseResultWrappedJSON(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	columns := make([]string, 0)
	for i := range peer.Tables[TableServices].Table.Columns {
		col := peer.Tables[TableServices].Table.Columns[i]
		if col.StorageType == LocalStore && col.Optional == NoFlags {
			columns = append(columns, col.Name)
		}
	}
	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString(fmt.Sprintf("GET services\nOutputFormat: wrapped_json\nColumns: %s\nColumnHeaders: on\n", strings.Join(columns, " ")))))
	if err != nil {
		panic(err.Error())
	}
	conn, connType, err := peer.GetConnection()
	if err != nil {
		panic(err.Error())
	}
	resBytes, err := peer.getQueryResponse(req, req.String(), peer.Status["PeerAddr"].(string), conn, connType)
	if err != nil {
		panic(err.Error())
	}

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res, _, err := req.parseResult(resBytes)
		if err != nil {
			panic(err.Error())
		}
		if len(*res) != 1000 {
			b.Fatalf("wrong result size, expected 1000, got %d", len(*res))
		}
		if len((*res)[0]) != len(columns) {
			b.Fatalf("wrong result size, expected %d, got %d", len(columns), len((*res)[0]))
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkPeerUpdate(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 1000, 10000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res := peer.UpdateAllTables()
		if !res {
			panic("Update failed")
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkSingleFilter_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkSingleFilter_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkMultiFilter_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: name != test\nFilter: state != 5\nFilter: latency != 2\nFilter: contact_groups !=\nFilter: custom_variables != TEST blah\n")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkSimpleStats_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString("GET hosts\nStats: name != \nStats: avg latency\nStats: sum latency")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkTacStats_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkTacStats_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkTacStats_1k_svc_100Peer(b *testing.B) {
	CheckOpenFilesLimit(b, 2000)

	b.StopTimer()
	peer := StartTestPeer(100, 10, 10)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkTacStats_5k_svc_500Peer(b *testing.B) {
	CheckOpenFilesLimit(b, 2000)

	b.StopTimer()
	peer := StartTestPeer(500, 10, 10)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkServicelistLimit_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(servicesPageQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkServicelistLimit_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(servicesPageQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func BenchmarkRequestParser1(b *testing.B) {
	for n := 0; n < b.N; n++ {
		buf := bufio.NewReader(bytes.NewBufferString(servicesPageQuery))
		_, size, err := NewRequest(buf)
		if err != nil {
			panic(err.Error())
		}
		if size != 1681 {
			panic(fmt.Sprintf("got wrong size: %d", size))
		}
	}
}

func BenchmarkRequestParser2(b *testing.B) {
	for n := 0; n < b.N; n++ {
		buf := bufio.NewReader(bytes.NewBufferString(tacPageStatsQuery))
		_, size, err := NewRequest(buf)
		if err != nil {
			panic(err.Error())
		}
		if size != 3651 {
			panic(fmt.Sprintf("got wrong size: %d", size))
		}
	}
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
