package main

import (
	"testing"
)

func BenchmarkRequestsFilterSmall(b *testing.B) {
	peer := StartTestPeer(1, 0, 0)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsFilterSmall_1k_svc__1Peer(b *testing.B) {
	peer := StartTestPeer(1, 100, 1000)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsFilterSmall_1k_svc_10Peer(b *testing.B) {
	peer := StartTestPeer(10, 10, 100)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsFilterBig(b *testing.B) {
	peer := StartTestPeer(1, 0, 0)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString("GET hosts\nColumns: name\nFilter: name != test\nFilter: state != 5\nFilter: latency != 2\nFilter: contact_groups !=\nFilter: custom_variables != TEST blah\n")
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsStatsSmall(b *testing.B) {
	peer := StartTestPeer(1, 0, 0)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString("GET hosts\nStats: name != \nStats: avg latency\nStats: sum latency")
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsStatsBig(b *testing.B) {
	peer := StartTestPeer(1, 0, 0)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString(tacPageStatsQuery)
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsStatsBig_1k_svc__1Peer(b *testing.B) {
	peer := StartTestPeer(1, 100, 1000)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString(tacPageStatsQuery)
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsStatsBig_1k_svc_10Peer(b *testing.B) {
	peer := StartTestPeer(10, 10, 100)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString(tacPageStatsQuery)
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsServicelistLimit_1k_svc__1Peer(b *testing.B) {
	peer := StartTestPeer(1, 100, 1000)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString("GET services\nSort: host_name asc\nSort: description asc\nLimit: 100")
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkRequestsServicelistLimit_1k_svc_10Peer(b *testing.B) {
	peer := StartTestPeer(10, 10, 100)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		peer.QueryString("GET services\nSort: host_name asc\nSort: description asc\nLimit: 100")
	}
	b.StopTimer()

	StopTestPeer(peer)
}

func BenchmarkNumberToFloat(b *testing.B) {
	for n := 0; n < b.N; n++ {
		numberToFloat(123.1231123123)
		numberToFloat(1231123123)
		numberToFloat(true)
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
