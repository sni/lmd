package lmd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkParseResultJSON(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	columns := make([]string, 0)
	for i := range peer.data.tables[TableServices].Table.Columns {
		col := peer.data.tables[TableServices].Table.Columns[i]
		if col.StorageType == LocalStore && col.Optional == NoFlags {
			columns = append(columns, col.Name)
		}
	}
	req, _, err := NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(fmt.Sprintf("GET services\nOutputFormat: json\nColumns: %s\nColumnHeaders: on\n", strings.Join(columns, " ")))), ParseOptimize)
	require.NoError(b, err)

	conn, connType, err := peer.GetConnection(req)
	require.NoError(b, err)

	resBytes, _, err := peer.getQueryResponse(context.TODO(), req, req.String(), peer.PeerAddr, conn, connType)
	require.NoError(b, err)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res, _, err2 := req.parseResult(resBytes)
		if err2 != nil {
			panic(err2.Error())
		}
		if len(res) != 1001 {
			b.Fatalf("wrong result size, expected 1001, got %d", len(res))
		}
		if len(res[0]) != len(columns) {
			b.Fatalf("wrong result size, expected %d, got %d", len(columns), len(res[0]))
		}
	}
	b.StopTimer()

	err = cleanup()
	require.NoError(b, err)
}

func BenchmarkParseResultWrappedJSON(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	columns := make([]string, 0)
	for i := range peer.data.tables[TableServices].Table.Columns {
		col := peer.data.tables[TableServices].Table.Columns[i]
		if col.StorageType == LocalStore && col.Optional == NoFlags {
			columns = append(columns, col.Name)
		}
	}
	req, _, err := NewRequest(context.TODO(), peer.lmd, bufio.NewReader(bytes.NewBufferString(fmt.Sprintf("GET services\nOutputFormat: wrapped_json\nColumns: %s\nColumnHeaders: on\n", strings.Join(columns, " ")))), ParseOptimize)
	require.NoError(b, err)

	conn, connType, err := peer.GetConnection(req)
	require.NoError(b, err)

	resBytes, _, err := peer.getQueryResponse(context.TODO(), req, req.String(), peer.PeerAddr, conn, connType)
	require.NoError(b, err)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res, _, err2 := req.parseResult(resBytes)
		if err2 != nil {
			panic(err2.Error())
		}
		if len(res) != 1000 {
			b.Fatalf("wrong result size, expected 1000, got %d", len(res))
		}
		if len(res[0]) != len(columns) {
			b.Fatalf("wrong result size, expected %d, got %d", len(columns), len(res[0]))
		}
	}
	b.StopTimer()

	err = cleanup()
	require.NoError(b, err)
}

func BenchmarkPeerUpdate(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 1000, 10000)
	PauseTestPeers(peer)

	ctx := context.TODO()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		err := peer.data.UpdateFull(ctx, Objects.UpdateTables)
		if err != nil {
			panic("Update failed")
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkPeerUpdateServiceInsert(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 1000, 10000)
	PauseTestPeers(peer)

	table := peer.data.tables[TableServices]
	req := &Request{
		Table:           table.Table.Name,
		Columns:         table.DynamicColumnNamesCache,
		ResponseFixed16: true,
		OutputFormat:    OutputFormatJSON,
		FilterStr:       "Filter: host_name !=\n",
	}
	res, meta, err := peer.Query(context.TODO(), req)
	if err != nil {
		return
	}

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res := peer.data.insertDeltaDataResult(2, res, meta, table)
		if res != nil {
			panic("Update failed")
		}
	}
	b.StopTimer()

	err = cleanup()
	require.NoError(b, err)
}

func BenchmarkSingleFilter_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkSingleFilter_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: contact_groups >= demo\nSort: name asc")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkMultiFilter_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString("GET hosts\nColumns: name\nFilter: name != test\nFilter: state != 5\nFilter: latency != 2\nFilter: contact_groups !=\nFilter: custom_variables != TEST blah\n")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkSimpleStats_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString("GET hosts\nStats: name != \nStats: avg latency\nStats: sum latency")
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkTacStats_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkTacStats_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkTacStats_1k_svc_100Peer(b *testing.B) {
	CheckOpenFilesLimit(b, 2000)

	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(100, 10, 10)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkTacStats_5k_svc_500Peer(b *testing.B) {
	CheckOpenFilesLimit(b, 2000)

	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(500, 10, 10)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(tacPageStatsQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkServicelistLimit_1k_svc__1Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(1, 100, 1000)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(servicesPageQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkServicelistLimit_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(servicesPageQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

func BenchmarkRequestParser1(b *testing.B) {
	lmd := createTestLMDInstance()
	for n := 0; n < b.N; n++ {
		buf := bufio.NewReader(bytes.NewBufferString(servicesPageQuery))
		_, size, err := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
		if err != nil {
			panic(err.Error())
		}
		if size != 1681 {
			panic(fmt.Sprintf("got wrong size: %d", size))
		}
	}
}

func BenchmarkRequestParser2(b *testing.B) {
	lmd := createTestLMDInstance()
	for n := 0; n < b.N; n++ {
		buf := bufio.NewReader(bytes.NewBufferString(tacPageStatsQuery))
		_, size, err := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
		if err != nil {
			panic(err.Error())
		}
		if size != 3651 {
			panic(fmt.Sprintf("got wrong size: %d", size))
		}
	}
}

func BenchmarkServicesearchLimit_1k_svc_10Peer(b *testing.B) {
	b.StopTimer()
	peer, cleanup, _ := StartTestPeer(10, 10, 100)
	PauseTestPeers(peer)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := peer.QueryString(serviceSearchQuery)
		if err != nil {
			panic(err.Error())
		}
	}
	b.StopTimer()

	err := cleanup()
	require.NoError(b, err)
}

const tacPageStatsQuery = `GET services
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

const servicesPageQuery = `GET services
Columns: accept_passive_checks acknowledged action_url action_url_expanded active_checks_enabled check_command check_interval check_options check_period check_type checks_enabled comments current_attempt current_notification_number description event_handler event_handler_enabled custom_variable_names custom_variable_values execution_time first_notification_delay flap_detection_enabled groups has_been_checked high_flap_threshold host_acknowledged host_action_url_expanded host_active_checks_enabled host_address host_alias host_checks_enabled host_check_type host_latency host_plugin_output host_perf_data host_current_attempt host_check_command host_comments host_groups host_has_been_checked host_icon_image_expanded host_icon_image_alt host_is_executing host_is_flapping host_name host_notes_url_expanded host_notifications_enabled host_scheduled_downtime_depth host_state host_accept_passive_checks host_last_state_change icon_image icon_image_alt icon_image_expanded is_executing is_flapping last_check last_notification last_state_change latency long_plugin_output low_flap_threshold max_check_attempts next_check notes notes_expanded notes_url notes_url_expanded notification_interval notification_period notifications_enabled obsess_over_service percent_state_change perf_data plugin_output process_performance_data retry_interval scheduled_downtime_depth state state_type modified_attributes_list last_time_critical last_time_ok last_time_unknown last_time_warning display_name host_display_name host_custom_variable_names host_custom_variable_values in_check_period in_notification_period host_parents
Limit: 100
OutputFormat: json
ResponseHeader: fixed16`

const serviceSearchQuery = `GET services
ResponseHeader: fixed16
OutputFormat: json
Columns: host_has_been_checked host_name host_state host_scheduled_downtime_depth host_acknowledged has_been_checked state scheduled_downtime_depth acknowledged peer_key
Filter: host_name !~~ abc
Filter: host_alias !~~ abc
Filter: host_address !~~ abc
Filter: host_display_name !~~ abc
And: 4
Filter: host_groups !>= some-hostgroup
Filter: description !~~ cde
Filter: display_name !~~ cde
And: 2
Filter: description !~~ cde
Filter: display_name !~~ cde
And: 2
Filter: description !~~ xyz
Filter: display_name !~~ xyz
And: 2
Filter: host_name !~~ ab
Filter: host_alias !~~ ab
Filter: host_address !~~ ti
Filter: host_display_name !~~ ab
And: 4
Filter: description != some service
Filter: display_name != some service
And: 2
Filter: description != some other service
Filter: display_name != some other service
And: 2
And: 8
Filter: host_name ~~ 123
Filter: host_alias ~~ 123
Filter: host_address ~~ 123
Filter: host_display_name ~~ 123
Or: 4
Filter: host_name ~~ 234
Filter: host_alias ~~ 234
Filter: host_address ~~ 234
Filter: host_display_name ~~ 234
Or: 4
Filter: host_name ~~ 345
Filter: host_alias ~~ 345
Filter: host_address ~~ 345
Filter: host_display_name ~~ 345
Or: 4
Filter: host_name ~~ 456
Filter: host_alias ~~ 456
Filter: host_address ~~ 456
Filter: host_display_name ~~ 456
Or: 4
Filter: host_name !~~ not
Filter: host_alias !~~ not
Filter: host_address !~~ not
Filter: host_display_name !~~ not
And: 4
Or: 6
`
