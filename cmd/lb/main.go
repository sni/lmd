package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"pkg/lmd"
	"strings"
	"time"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

type result struct {
	name     string
	total    int64
	errors   int64
	bytesIn  int64
	bytesOut int64
}

type testquery struct {
	req   *lmd.Request
	name  string
	query string
}

type Cmd struct {
	daemon          *lmd.Daemon
	shutdownChannel chan bool
	resultChannel   chan *result
	queryChannel    chan *testquery
	flags           struct {
		flagConn     lmd.ArrayFlags
		flagDuration time.Duration
		flagParallel int64
		flagVersion  bool
		flagVerbose  bool
	}
}

const (
	outputLineLength = 80
)

var testQueries = []struct{ name, query string }{
	{"status", "GET status"},
	{"sites", "GET sites"},
	{"contact", "GET contacts\nFilter: name = test"},
	{"hosts", "GET hosts"},
	{"services", "GET services"},
	{"tac", tacPageStatsQuery},
	{"serviceList", servicesPageQuery},
	{"serviceSearch", serviceSearchQuery},
}

func main() {
	daemon := lmd.NewLMDInstance()
	daemon.Config = lmd.NewConfig([]string{})
	daemon.Config.ValidateConfig()

	cmd := &Cmd{
		shutdownChannel: make(chan bool, 5),
		resultChannel:   make(chan *result, 100),
		daemon:          daemon,
	}
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [arguments]\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "\nDescription:\n\n  lb is a benchmark tool for lmd which sends lots of requests to lmd.\n\nArguments:\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nExample:\n\n  lb -c localhost:6557 -d 5s -n 10\n\n")
	}
	flag.Int64Var(&cmd.flags.flagParallel, "n", 10, "set number of parallel requests")
	flag.Int64Var(&cmd.flags.flagParallel, "number", 10, "set number of parallel requests")
	flag.DurationVar(&cmd.flags.flagDuration, "d", 10*time.Second, "set run duration")
	flag.DurationVar(&cmd.flags.flagDuration, "duration", 10*time.Second, "set run duration")
	flag.Var(&cmd.flags.flagConn, "c", "set unix socket path or port to connect to")
	flag.Var(&cmd.flags.flagConn, "connection", "set unix socket path or port to connect to")
	flag.BoolVar(&cmd.flags.flagVerbose, "v", false, "enable verbose output")
	flag.BoolVar(&cmd.flags.flagVerbose, "verbose", false, "enable verbose output")
	flag.BoolVar(&cmd.flags.flagVersion, "V", false, "print version and exit")
	flag.BoolVar(&cmd.flags.flagVersion, "version", false, "print version and exit")
	flag.Parse()

	if cmd.flags.flagVersion {
		lmd.Build = Build
		fmt.Fprintf(os.Stdout, "lb - version %s\n", lmd.Version())
		os.Exit(0)
	}

	if len(cmd.flags.flagConn.Value()) == 0 {
		fmt.Fprintf(os.Stderr, "ERROR: must specify at least one connection. See -h/--help for all options.\n")

		return
	}

	cmd.queryChannel = make(chan *testquery, cmd.flags.flagParallel*2)
	cmd.fillQueries()

	for i := range cmd.flags.flagParallel {
		go cmd.runWorker(i)
	}

	final := cmd.collectResults()
	cmd.printResults(final)
}

func (cmd *Cmd) runWorker(i int64) {
	ctx := context.Background()
	name := fmt.Sprintf("w%02d", i)
	peer := lmd.NewPeer(cmd.daemon, &lmd.Connection{Source: cmd.flags.flagConn.Value(), Name: name, ID: name})
	for {
		select {
		case <-cmd.shutdownChannel:
			cmd.shutdownChannel <- true

			return
		default:
			query := <-cmd.queryChannel
			_, _, err := peer.Query(ctx, query.req)
			res := &result{
				name:  query.name,
				total: 1,
			}
			if err != nil {
				res.errors++
			}
			cmd.resultChannel <- res
			cmd.queryChannel <- query
		}
	}
}

func (cmd *Cmd) collectResults() map[string]*result {
	final := map[string]*result{}
	end := time.NewTimer(cmd.flags.flagDuration)
	keepRuning := true
	for keepRuning {
		select {
		case <-end.C:
			end.Stop()
			cmd.shutdownChannel <- true
			keepRuning = false
		case res := <-cmd.resultChannel:
			if _, ok := final[res.name]; !ok {
				final[res.name] = res
			} else {
				fin := final[res.name]
				fin.total += res.total
				fin.errors += res.errors
				fin.bytesIn += res.bytesIn
				fin.bytesOut += res.bytesOut
			}
		}
	}

	return final
}

func (cmd *Cmd) printResults(final map[string]*result) {
	total := &result{}
	totalRate := float64(0)
	for _, t := range testQueries {
		r := final[t.name]
		if r == nil {
			fmt.Fprintf(os.Stdout, "got no data for: %s\n", t.name)

			continue
		}
		rate := float64(r.total) / cmd.flags.flagDuration.Seconds()
		fmt.Fprintf(os.Stdout, "%15s: total: %10d | errors: %3d | rate: %9.2f req/s\n", r.name, r.total, r.errors, rate)
		totalRate += rate
		total.total += r.total
		total.errors += r.errors
	}
	fmt.Fprintf(os.Stdout, "%s\n", strings.Repeat("-", outputLineLength))
	fmt.Fprintf(os.Stdout, "%15s: total: %10d | errors: %3d | rate: %9.2f req/s\n", "total", total.total, total.errors, totalRate)
}

func (cmd *Cmd) fillQueries() {
	ctx := context.TODO()
	qIdx := 0
	for i := range cmd.flags.flagParallel * 2 {
		queryTxt := strings.TrimSpace(testQueries[qIdx].query) + "\n"
		if i >= cmd.flags.flagParallel {
			queryTxt += "OutputFormat: json\nResponseHeader: fixed16\n"
		}
		req, _, err := lmd.NewRequest(ctx, cmd.daemon, bufio.NewReader(bytes.NewBufferString(queryTxt)), lmd.ParseDefault)
		if err != nil {
			panic(err.Error())
		}
		cmd.queryChannel <- &testquery{
			name:  testQueries[qIdx].name,
			query: testQueries[qIdx].query,
			req:   req,
		}
		qIdx++
		if qIdx >= len(testQueries) {
			qIdx = 0
		}
	}
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
Stats: accept_passive_checks = 0`

const servicesPageQuery = `GET services
Columns: accept_passive_checks acknowledged action_url action_url_expanded active_checks_enabled check_command ` +
	`check_interval check_options check_period check_type checks_enabled comments current_attempt ` +
	`current_notification_number description event_handler event_handler_enabled custom_variable_names ` +
	`custom_variable_values execution_time first_notification_delay flap_detection_enabled groups has_been_checked ` +
	`high_flap_threshold host_acknowledged host_action_url_expanded host_active_checks_enabled host_address host_alias ` +
	`host_checks_enabled host_check_type host_latency host_plugin_output host_perf_data host_current_attempt ` +
	`host_check_command host_comments host_groups host_has_been_checked host_icon_image_expanded host_icon_image_alt ` +
	`host_is_executing host_is_flapping host_name host_notes_url_expanded host_notifications_enabled ` +
	`host_scheduled_downtime_depth host_state host_accept_passive_checks host_last_state_change icon_image ` +
	`icon_image_alt icon_image_expanded is_executing is_flapping last_check last_notification last_state_change ` +
	`latency long_plugin_output low_flap_threshold max_check_attempts next_check notes notes_expanded notes_url ` +
	`notes_url_expanded notification_interval notification_period notifications_enabled obsess_over_service ` +
	`percent_state_change perf_data plugin_output process_performance_data retry_interval scheduled_downtime_depth ` +
	`state state_type modified_attributes_list last_time_critical last_time_ok last_time_unknown last_time_warning ` +
	`display_name host_display_name host_custom_variable_names host_custom_variable_values in_check_period ` +
	`in_notification_period host_parents
Limit: 100`

const serviceSearchQuery = `GET services
Columns: host_has_been_checked host_name host_state host_scheduled_downtime_depth host_acknowledged has_been_checked ` +
	`state scheduled_downtime_depth acknowledged peer_key
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
