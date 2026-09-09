package lmd

// Icinga 2 REST v1 command support.
//
// Clients send Livestatus style commands ("COMMAND [ts] NAME;arg1;arg2...") which
// lmd normally passes through to the backend command file. Icinga 2 REST v1
// backends have no command file, so the commands are translated into REST API
// requests (actions and object modifications) instead.
//
// Supported commands:
//
//	SCHEDULE_FORCED_HOST_CHECK / SCHEDULE_FORCED_SVC_CHECK              reschedule a check
//	HOST_COMMENT / SVC_COMMENT                                          add a comment
//	DEL_HOST_COMMENT / DEL_SVC_COMMENT                                  delete a comment
//	ACKNOWLEDGE_HOST_PROBLEM / ACKNOWLEDGE_SVC_PROBLEM                  acknowledge a problem
//	STICKY_ACKNOWLEDGE_HOST_PROBLEM / STICKY_ACKNOWLEDGE_SVC_PROBLEM
//	REMOVE_HOST_ACKNOWLEDGEMENT / REMOVE_SVC_ACKNOWLEDGEMENT
//	HOST_DOWNTIME / SVC_DOWNTIME                                        schedule a downtime
//	DEL_HOST_DOWNTIME / DEL_SVC_DOWNTIME / DEL_DOWNTIME                 delete downtimes
//	ENABLE / DISABLE _{HOST,SVC}_CHECKS, _NOTIFICATIONS, _PASSIVE_CHECKS,
//	_FLAPPING, _EVENT_HANDLER and _PERFDATA                             toggle object attributes
//	Global notifications, event handlers, flap detection, check execution
//	and performance data commands                                    toggle application attributes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var reRestv1Command = regexp.MustCompile(`^COMMAND \[\d+\] +(.+)$`)

// restv1AttrCommand describes an ENABLE/DISABLE_* attribute command.
type restv1AttrCommand struct {
	objType string // "hosts", "services" or "icingaapplications"
	attr    string
	enabled bool
}

var restv1AttrCommands = map[string]restv1AttrCommand{
	"ENABLE_NOTIFICATIONS":             {"icingaapplications", "enable_notifications", true},
	"DISABLE_NOTIFICATIONS":            {"icingaapplications", "enable_notifications", false},
	"ENABLE_EVENT_HANDLERS":            {"icingaapplications", "enable_event_handlers", true},
	"DISABLE_EVENT_HANDLERS":           {"icingaapplications", "enable_event_handlers", false},
	"ENABLE_EVENT_HANDLER":             {"icingaapplications", "enable_event_handlers", true},
	"DISABLE_EVENT_HANDLER":            {"icingaapplications", "enable_event_handlers", false},
	"ENABLE_FLAP_DETECTION":            {"icingaapplications", "enable_flapping", true},
	"DISABLE_FLAP_DETECTION":           {"icingaapplications", "enable_flapping", false},
	"START_EXECUTING_HOST_CHECKS":      {"icingaapplications", "enable_host_checks", true},
	"STOP_EXECUTING_HOST_CHECKS":       {"icingaapplications", "enable_host_checks", false},
	"START_EXECUTING_SVC_CHECKS":       {"icingaapplications", "enable_service_checks", true},
	"STOP_EXECUTING_SVC_CHECKS":        {"icingaapplications", "enable_service_checks", false},
	"PROCESS_PERFORMANCE_DATA":         {"icingaapplications", "enable_perfdata", true},
	"STOP_PROCESSING_PERFORMANCE_DATA": {"icingaapplications", "enable_perfdata", false},
	"ENABLE_HOST_CHECKS":               {"hosts", "enable_active_checks", true},
	"DISABLE_HOST_CHECKS":              {"hosts", "enable_active_checks", false},
	"ENABLE_HOST_NOTIFICATIONS":        {"hosts", "enable_notifications", true},
	"DISABLE_HOST_NOTIFICATIONS":       {"hosts", "enable_notifications", false},
	"ENABLE_PASSIVE_HOST_CHECKS":       {"hosts", "enable_passive_checks", true},
	"DISABLE_PASSIVE_HOST_CHECKS":      {"hosts", "enable_passive_checks", false},
	"ENABLE_HOST_FLAPPING":             {"hosts", "enable_flapping", true},
	"DISABLE_HOST_FLAPPING":            {"hosts", "enable_flapping", false},
	"ENABLE_HOST_EVENT_HANDLER":        {"hosts", "enable_event_handler", true},
	"DISABLE_HOST_EVENT_HANDLER":       {"hosts", "enable_event_handler", false},
	"ENABLE_HOST_PERFDATA":             {"hosts", "enable_perfdata", true},
	"DISABLE_HOST_PERFDATA":            {"hosts", "enable_perfdata", false},
	"ENABLE_SVC_CHECKS":                {"services", "enable_active_checks", true},
	"DISABLE_SVC_CHECKS":               {"services", "enable_active_checks", false},
	"ENABLE_SVC_NOTIFICATIONS":         {"services", "enable_notifications", true},
	"DISABLE_SVC_NOTIFICATIONS":        {"services", "enable_notifications", false},
	"ENABLE_PASSIVE_SVC_CHECKS":        {"services", "enable_passive_checks", true},
	"DISABLE_PASSIVE_SVC_CHECKS":       {"services", "enable_passive_checks", false},
	"ENABLE_SVC_FLAPPING":              {"services", "enable_flapping", true},
	"DISABLE_SVC_FLAPPING":             {"services", "enable_flapping", false},
	"ENABLE_SVC_EVENT_HANDLER":         {"services", "enable_event_handler", true},
	"DISABLE_SVC_EVENT_HANDLER":        {"services", "enable_event_handler", false},
	"ENABLE_SVC_PERFDATA":              {"services", "enable_perfdata", true},
	"DISABLE_SVC_PERFDATA":             {"services", "enable_perfdata", false},
}

// icinga2RestV1SendCommands executes all commands from req.Command (one command
// per line, separated by a blank line) and returns the first error encountered.
func (p *Peer) icinga2RestV1SendCommands(ctx context.Context, req *Request) (ResultSet, *ResultMetaData, error) {
	t1 := time.Now()
	for line := range strings.SplitSeq(req.Command, "\n\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if err := p.restv1ExecuteCommand(ctx, line); err != nil {
			return nil, nil, err
		}
		p.restv1FullDelta.Store(true)
		p.forceComments.Store(true)
		logWith(p).Debugf("executed restv1 command: %s", line)
	}

	return ResultSet{}, p.restv1Meta(req, 0, 0, t1), nil
}

// restv1ExecuteCommand parses and executes a single command line.
func (p *Peer) restv1ExecuteCommand(ctx context.Context, line string) error {
	name, args, err := restv1ParseCommand(line)
	if err != nil {
		return p.badCommand(err)
	}
	switch name {
	case "ADD_HOST_COMMENT", "ADD_SVC_COMMENT":
		return p.restv1StandardComment(ctx, name, args)
	case "SCHEDULE_HOST_DOWNTIME", "SCHEDULE_SVC_DOWNTIME":
		return p.restv1StandardDowntime(ctx, name, args)
	case "SCHEDULE_FORCED_HOST_CHECK":
		return p.restv1ScheduleHostCheck(ctx, args)
	case "SCHEDULE_FORCED_SVC_CHECK":
		return p.restv1ScheduleServiceCheck(ctx, args)
	case "HOST_COMMENT":
		return p.restv1AddHostComment(ctx, args)
	case "SVC_COMMENT":
		return p.restv1AddServiceComment(ctx, args)
	case "DEL_HOST_COMMENT", "DEL_SVC_COMMENT":
		return p.restv1DeleteComment(ctx, name, args)
	case "ACKNOWLEDGE_HOST_PROBLEM", "ACKNOWLEDGE_SVC_PROBLEM":
		return p.restv1StandardAcknowledge(ctx, name, args)
	case "STICKY_ACKNOWLEDGE_HOST_PROBLEM":
		return p.restv1AcknowledgeHost(ctx, name, args)
	case "STICKY_ACKNOWLEDGE_SVC_PROBLEM":
		return p.restv1AcknowledgeService(ctx, name, args)
	case "REMOVE_HOST_ACKNOWLEDGEMENT":
		return p.restv1RemoveAcknowledgement(ctx, "Host", args)
	case "REMOVE_SVC_ACKNOWLEDGEMENT":
		return p.restv1RemoveAcknowledgement(ctx, "Service", args)
	case "HOST_DOWNTIME":
		return p.restv1ScheduleDowntime(ctx, "Host", "HOST_DOWNTIME", args)
	case "SVC_DOWNTIME":
		return p.restv1ScheduleDowntime(ctx, "Service", "SVC_DOWNTIME", args)
	case "DEL_HOST_DOWNTIME", "DEL_SVC_DOWNTIME":
		return p.restv1DeleteDowntime(ctx, name, args)
	case "DEL_DOWNTIME":
		return p.restv1DeleteDowntimeByID(ctx, args)
	}
	if cmd, ok := restv1AttrCommands[name]; ok {
		return p.restv1ToggleAttr(ctx, name, cmd, args)
	}

	return p.badCommand(fmt.Errorf("command %s is not supported for Icinga2 RESTv1 backends", name))
}

// restv1ParseCommand extracts the command name and arguments from a
// "COMMAND [ts] NAME;arg1;arg2..." line.
func restv1ParseCommand(line string) (command string, args []string, err error) {
	body := strings.TrimSpace(line)
	matched := reRestv1Command.FindStringSubmatch(body)
	if matched == nil {
		return "", nil, fmt.Errorf("malformed command: %s", body)
	}
	command, argsStr, hasArgs := strings.Cut(matched[1], ";")
	if command == "" {
		return "", nil, fmt.Errorf("malformed command: %s", body)
	}
	if !hasArgs {
		return command, nil, nil
	}

	return command, strings.Split(argsStr, ";"), nil
}

// badCommand wraps an error as a 400 command error.
func (p *Peer) badCommand(err error) error {
	return &PeerCommandError{code: ReturnCodeBadRequest, peer: p, err: err}
}

// restv1RequireArgs checks the minimum argument count of a command.
func restv1RequireArgs(name string, args []string, minArgs int) error {
	if len(args) < minArgs {
		return fmt.Errorf("command %s needs at least %d arguments", name, minArgs)
	}

	return nil
}

// restv1ParseTimestamp parses a unix timestamp argument, an empty argument is 0.
func restv1ParseTimestamp(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp %q", value)
	}

	return val, nil
}

// restv1ParseID parses a positive id argument (comment / downtime id).
func restv1ParseID(name, v string) (int64, error) {
	val, err := strconv.ParseInt(v, 10, 64)
	if err != nil || val <= 0 {
		return 0, fmt.Errorf("command %s: invalid id %q", name, v)
	}

	return val, nil
}

// restv1Quote quotes a string for use in a DSL filter expression.
func restv1Quote(v string) string {
	return strconv.Quote(v)
}

// restv1HostFilter builds the DSL filter for a single host object.
func restv1HostFilter(host string) string {
	return "host.name==" + restv1Quote(host)
}

// restv1ServiceFilter builds the DSL filter for a single service object.
func restv1ServiceFilter(host, service string) string {
	return "host.name==" + restv1Quote(host) + " && service.name==" + restv1Quote(service)
}

// restv1MiddleArg returns the argument at commentIdx. When the argument list is
// longer than commentIdx+1+numFixed, the surplus fields are re-joined with the
// argument separator, because the argument (a comment text) may contain it.
func restv1MiddleArg(args []string, commentIdx, numFixed int) string {
	if len(args) > commentIdx+1+numFixed {
		return strings.Join(args[commentIdx:len(args)-numFixed], ";")
	}

	return args[commentIdx]
}

// restv1TailArg returns the last argument (position idx); surplus fields are
// re-joined because the argument may contain the argument separator.
func restv1TailArg(args []string, idx int) string {
	if len(args) <= idx {
		return ""
	}

	return strings.Join(args[idx:], ";")
}

func restv1CommandTarget(name string, args []string) (objType, filter string, offset int) {
	if strings.Contains(name, "SVC") {
		return "Service", restv1ServiceFilter(args[0], args[1]), 1
	}

	return "Host", restv1HostFilter(args[0]), 0
}

func (p *Peer) restv1StandardComment(ctx context.Context, name string, args []string) error {
	minArgs := 4
	if strings.Contains(name, "SVC") {
		minArgs++
	}
	if err := restv1RequireArgs(name, args, minArgs); err != nil {
		return p.badCommand(err)
	}
	objType, filter, offset := restv1CommandTarget(name, args)

	return p.restv1AddComment(ctx, objType, filter, args[2+offset], restv1TailArg(args, 3+offset), 0)
}

func (p *Peer) restv1StandardAcknowledge(ctx context.Context, name string, args []string) error {
	minArgs := 6
	if strings.Contains(name, "SVC") {
		minArgs++
	}
	if err := restv1RequireArgs(name, args, minArgs); err != nil {
		return p.badCommand(err)
	}
	objType, filter, offset := restv1CommandTarget(name, args)
	body := map[string]any{
		"type": objType, "filter": filter,
		"sticky": args[1+offset] == "2", "notify": args[2+offset] == "1",
		"persistent": args[3+offset] == "1", "author": args[4+offset], "comment": restv1TailArg(args, 5+offset),
	}

	return p.restv1DoCommand(ctx, "/actions/acknowledge-problem", body, "no matching object found")
}

func (p *Peer) restv1StandardDowntime(ctx context.Context, name string, args []string) error {
	minArgs := 8
	if strings.Contains(name, "SVC") {
		minArgs++
	}
	if err := restv1RequireArgs(name, args, minArgs); err != nil {
		return p.badCommand(err)
	}
	objType, _, offset := restv1CommandTarget(name, args)
	converted := append([]string{}, args[:1+offset]...)
	converted = append(converted, args[1+offset], args[2+offset], args[6+offset], restv1TailArg(args, 7+offset), args[4+offset], args[5+offset], args[3+offset])

	return p.restv1ScheduleDowntime(ctx, objType, name, converted)
}

// restv1ScheduleHostCheck executes SCHEDULE_FORCED_HOST_CHECK;host;time.
func (p *Peer) restv1ScheduleHostCheck(ctx context.Context, args []string) error {
	if err := restv1RequireArgs("SCHEDULE_FORCED_HOST_CHECK", args, 2); err != nil {
		return p.badCommand(err)
	}
	when, err := restv1ParseTimestamp(args[1])
	if err != nil {
		return p.badCommand(err)
	}

	return p.restv1ScheduleCheck(ctx, "Host", restv1HostFilter(args[0]), when)
}

// restv1ScheduleServiceCheck executes SCHEDULE_FORCED_SVC_CHECK;host;service;time.
func (p *Peer) restv1ScheduleServiceCheck(ctx context.Context, args []string) error {
	if err := restv1RequireArgs("SCHEDULE_FORCED_SVC_CHECK", args, 3); err != nil {
		return p.badCommand(err)
	}
	when, err := restv1ParseTimestamp(args[2])
	if err != nil {
		return p.badCommand(err)
	}

	return p.restv1ScheduleCheck(ctx, "Service", restv1ServiceFilter(args[0], args[1]), when)
}

// restv1ScheduleCheck runs the reschedule-check action. A time in the future is
// used as next_check, otherwise the check runs immediately.
func (p *Peer) restv1ScheduleCheck(ctx context.Context, objType, filter string, when int64) error {
	body := map[string]any{
		"type":   objType,
		"filter": filter,
		"force":  true,
	}
	if when > time.Now().Unix() {
		body["next_check"] = when
	}

	return p.restv1DoCommand(ctx, "/actions/reschedule-check", body, "no matching object found")
}

// restv1AddHostComment executes HOST_COMMENT;host;author;comment;[persistent;sticky;notify;start;end;id].
func (p *Peer) restv1AddHostComment(ctx context.Context, args []string) error {
	if err := restv1RequireArgs("HOST_COMMENT", args, 3); err != nil {
		return p.badCommand(err)
	}
	comment, expiry := restv1CommentArgs(args, 2)

	return p.restv1AddComment(ctx, "Host", restv1HostFilter(args[0]), args[1], comment, expiry)
}

// restv1AddServiceComment executes SVC_COMMENT;host;service;author;comment;[persistent;sticky;notify;start;end;id].
func (p *Peer) restv1AddServiceComment(ctx context.Context, args []string) error {
	if err := restv1RequireArgs("SVC_COMMENT", args, 4); err != nil {
		return p.badCommand(err)
	}
	comment, expiry := restv1CommentArgs(args, 3)

	return p.restv1AddComment(ctx, "Service", restv1ServiceFilter(args[0], args[1]), args[2], comment, expiry)
}

// restv1CommentArgs extracts the comment text (position commentIdx) and the
// expiry timestamp (end_time) from the arguments of a comment command.
func restv1CommentArgs(args []string, commentIdx int) (comment string, expiry int64) {
	// the fixed arguments after the comment text: persistent, sticky, notify, start_time, end_time, comment_id
	const numFixed = 6
	comment = restv1MiddleArg(args, commentIdx, numFixed)
	endIdx := len(args) - 2
	if len(args) >= commentIdx+1+numFixed {
		expiry, _ = restv1ParseTimestamp(args[endIdx])
	}

	return comment, expiry
}

// restv1AddComment runs the add-comment action.
func (p *Peer) restv1AddComment(ctx context.Context, objType, filter, author, comment string, expiry int64) error {
	body := map[string]any{
		"type":    objType,
		"filter":  filter,
		"author":  author,
		"comment": comment,
	}
	if expiry > 0 {
		body["expiry"] = expiry
	}

	return p.restv1DoCommand(ctx, "/actions/add-comment", body, "no matching object found")
}

// restv1DeleteComment executes DEL_HOST_COMMENT;host;id / DEL_SVC_COMMENT;host;service;id.
func (p *Peer) restv1DeleteComment(ctx context.Context, name string, args []string) error {
	if err := restv1RequireArgs(name, args, 1); err != nil {
		return p.badCommand(err)
	}
	id, err := restv1ParseID(name, args[len(args)-1])
	if err != nil {
		return p.badCommand(err)
	}
	filter := "comment.legacy_id==" + strconv.FormatInt(id, 10)

	return p.restv1DoCommand(ctx, "/actions/remove-comment", map[string]any{"type": "Comment", "filter": filter}, "no comment found")
}

// restv1AcknowledgeHost executes ACKNOWLEDGE_HOST_PROBLEM;host;state;sticky;author;comment.
// The state argument is ignored, the current state is acknowledged.
func (p *Peer) restv1AcknowledgeHost(ctx context.Context, name string, args []string) error {
	if strings.HasPrefix(name, "STICKY_") {
		// the sticky variant only takes host and sticky
		if err := restv1RequireArgs(name, args, 2); err != nil {
			return p.badCommand(err)
		}

		return p.restv1Acknowledge(ctx, "Host", restv1HostFilter(args[0]), args[1] == "1", "", "")
	}
	if err := restv1RequireArgs(name, args, 5); err != nil {
		return p.badCommand(err)
	}

	return p.restv1Acknowledge(ctx, "Host", restv1HostFilter(args[0]), args[2] == "1", args[3], restv1TailArg(args, 4))
}

// restv1MinSvcAcknowledgeArgs is the minimum argument count of the plain service acknowledgement.
const restv1MinSvcAcknowledgeArgs = 6

// restv1AcknowledgeService executes ACKNOWLEDGE_SVC_PROBLEM;host;service;state;sticky;author;comment.
// The state argument is ignored, the current state is acknowledged.
func (p *Peer) restv1AcknowledgeService(ctx context.Context, name string, args []string) error {
	if strings.HasPrefix(name, "STICKY_") {
		// the sticky variant only takes host, service and sticky
		if err := restv1RequireArgs(name, args, 3); err != nil {
			return p.badCommand(err)
		}

		return p.restv1Acknowledge(ctx, "Service", restv1ServiceFilter(args[0], args[1]), args[2] == "1", "", "")
	}
	if err := restv1RequireArgs(name, args, restv1MinSvcAcknowledgeArgs); err != nil {
		return p.badCommand(err)
	}

	return p.restv1Acknowledge(ctx, "Service", restv1ServiceFilter(args[0], args[1]), args[3] == "1", args[4], restv1TailArg(args, 5))
}

// restv1Acknowledge runs the acknowledge-problem action.
func (p *Peer) restv1Acknowledge(ctx context.Context, objType, filter string, sticky bool, author, comment string) error {
	body := map[string]any{
		"type":    objType,
		"filter":  filter,
		"sticky":  sticky,
		"author":  author,
		"comment": comment,
	}

	return p.restv1DoCommand(ctx, "/actions/acknowledge-problem", body, "no matching object found")
}

// restv1RemoveAcknowledgement executes REMOVE_HOST_ACKNOWLEDGEMENT;host /
// REMOVE_SVC_ACKNOWLEDGEMENT;host;service.
func (p *Peer) restv1RemoveAcknowledgement(ctx context.Context, objType string, args []string) error {
	name := "REMOVE_" + objType + "_ACKNOWLEDGEMENT"
	var filter string
	if objType == "Service" {
		if err := restv1RequireArgs(name, args, 2); err != nil {
			return p.badCommand(err)
		}
		filter = restv1ServiceFilter(args[0], args[1])
	} else {
		if err := restv1RequireArgs(name, args, 1); err != nil {
			return p.badCommand(err)
		}
		filter = restv1HostFilter(args[0])
	}

	return p.restv1DoCommand(ctx, "/actions/remove-acknowledgement", map[string]any{"type": objType, "filter": filter}, "no matching object found")
}

// restv1ScheduleDowntime executes HOST_DOWNTIME;host;start;end;author;comment;[trigger;duration;fixed]
// and the service variant with host;service;start;end;author;comment;[trigger;duration;fixed].
func (p *Peer) restv1ScheduleDowntime(ctx context.Context, objType, cmd string, args []string) error {
	off := 0
	if objType == "Service" {
		off = 1
	}
	if err := restv1RequireArgs(cmd, args, 5+off); err != nil {
		return p.badCommand(err)
	}
	start, err := restv1ParseTimestamp(args[1+off])
	if err != nil {
		return p.badCommand(err)
	}
	end, err := restv1ParseTimestamp(args[2+off])
	if err != nil {
		return p.badCommand(err)
	}
	// duration (args[6]) and fixed flag (args[7]) are optional; without the
	// flag a zero duration means a fixed downtime window
	duration := int64(0)
	if len(args) > 6+off {
		duration, err = strconv.ParseInt(args[6+off], 10, 64)
		if err != nil {
			return p.badCommand(fmt.Errorf("command %s: invalid duration %q", cmd, args[6+off]))
		}
	}
	fixed := duration == 0
	if len(args) > 7+off && args[7+off] != "" {
		fixed = args[7+off] == "1"
	}
	if !fixed && duration <= 0 {
		return p.badCommand(fmt.Errorf("command %s: duration is required for a flexible downtime", cmd))
	}
	if end <= 0 {
		end = start + duration
	}

	var filter string
	if objType == "Service" {
		filter = restv1ServiceFilter(args[0], args[1])
	} else {
		filter = restv1HostFilter(args[0])
	}
	body := map[string]any{
		"type":       objType,
		"filter":     filter,
		"author":     args[3+off],
		"comment":    restv1MiddleArg(args, 4+off, 3),
		"start_time": start,
		"end_time":   end,
		"fixed":      fixed,
	}
	if !fixed {
		body["duration"] = duration
	}
	if len(args) > 5+off && args[5+off] != "" && args[5+off] != "0" {
		id, err := restv1ParseID(cmd, args[5+off])
		if err != nil {
			return p.badCommand(err)
		}
		results, _, err := p.restv1Do(ctx, "/objects/downtimes", []string{"legacy_id"}, "downtime.legacy_id=="+strconv.FormatInt(id, 10))
		if err != nil {
			return err
		}
		if len(results) != 1 {
			return p.badCommand(fmt.Errorf("trigger downtime %d not found", id))
		}
		body["trigger_name"] = results[0]["_object_name"]
	}

	return p.restv1DoCommand(ctx, "/actions/schedule-downtime", body, "no matching object found")
}

// restv1DeleteDowntime executes DEL_HOST_DOWNTIME;host;start;end;author;comment /
// DEL_SVC_DOWNTIME;host;service;start;end;author;comment. The comment argument
// is ignored, the downtime is matched by object, times and author.
func (p *Peer) restv1DeleteDowntime(ctx context.Context, name string, args []string) error {
	if len(args) == 1 {
		return p.restv1DeleteDowntimeByID(ctx, args)
	}
	isService := name == "DEL_SVC_DOWNTIME"
	off := 0
	minArgs := 4
	if isService {
		off = 1
		minArgs = 5
	}
	if err := restv1RequireArgs(name, args, minArgs); err != nil {
		return p.badCommand(err)
	}
	start, err := restv1ParseTimestamp(args[1+off])
	if err != nil {
		return p.badCommand(err)
	}
	end, err := restv1ParseTimestamp(args[2+off])
	if err != nil {
		return p.badCommand(err)
	}
	filter := "downtime.start_time==" + strconv.FormatInt(start, 10) + " && downtime.end_time==" + strconv.FormatInt(end, 10) +
		" && downtime.author==" + restv1Quote(args[3+off])
	what := "downtime for host " + args[0]
	if isService {
		filter = restv1ServiceFilter(args[0], args[1]) + " && " + filter
		what = fmt.Sprintf("downtime for service %s on host %s", args[1], args[0])
	} else {
		filter = restv1HostFilter(args[0]) + " && !service && " + filter
	}

	return p.restv1DoCommand(ctx, "/actions/remove-downtime", map[string]any{"type": "Downtime", "filter": filter}, "no "+what+" found")
}

// restv1DeleteDowntimeByID executes DEL_DOWNTIME;id.
func (p *Peer) restv1DeleteDowntimeByID(ctx context.Context, args []string) error {
	if err := restv1RequireArgs("DEL_DOWNTIME", args, 1); err != nil {
		return p.badCommand(err)
	}
	id, err := restv1ParseID("DEL_DOWNTIME", args[0])
	if err != nil {
		return p.badCommand(err)
	}
	filter := "downtime.legacy_id==" + strconv.FormatInt(id, 10)

	return p.restv1DoCommand(ctx, "/actions/remove-downtime", map[string]any{"type": "Downtime", "filter": filter}, "no downtime found")
}

// restv1ToggleAttr executes an ENABLE/DISABLE_* attribute command.
func (p *Peer) restv1ToggleAttr(ctx context.Context, name string, cmd restv1AttrCommand, args []string) error {
	if cmd.objType == "icingaapplications" {
		body := map[string]any{"attrs": map[string]any{cmd.attr: cmd.enabled}}

		return p.restv1DoCommand(ctx, "/objects/icingaapplications/app", body, "no IcingaApplication app found")
	}
	if cmd.objType == "services" {
		if err := restv1RequireArgs(name, args, 2); err != nil {
			return p.badCommand(err)
		}

		return p.restv1SetObjectAttr(ctx, "services", args[0], args[1], cmd.attr, cmd.enabled)
	}
	if err := restv1RequireArgs(name, args, 1); err != nil {
		return p.badCommand(err)
	}

	return p.restv1SetObjectAttr(ctx, "hosts", args[0], "", cmd.attr, cmd.enabled)
}

// restv1SetObjectAttr sets a single attribute on a host or service object.
func (p *Peer) restv1SetObjectAttr(ctx context.Context, objType, host, service, attr string, enabled bool) error {
	objName := url.PathEscape(host)
	what := "host " + host
	if service != "" {
		objName += "!" + url.PathEscape(service)
		what = fmt.Sprintf("service %s on host %s", service, host)
	}
	body := map[string]any{"attrs": map[string]any{attr: enabled}}

	return p.restv1DoCommand(ctx, "/objects/"+objType+"/"+objName, body, "no "+what+" found")
}

// restv1DoCommand executes a REST command (POST with a JSON body) and checks the
// response. It returns a PeerError for transport and authentication problems
// (so the caller may retry) and a PeerCommandError for everything else,
// including the notFound message for 404 answers.
func (p *Peer) restv1DoCommand(ctx context.Context, path string, body map[string]any, notFoundMsg string) error {
	base, err := p.restv1BaseURL()
	if err != nil {
		return &PeerError{msg: err.Error(), kind: ConnectionError}
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return &PeerError{msg: err.Error(), kind: ResponseError}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return &PeerError{msg: err.Error(), kind: ConnectionError}
	}
	if user, pass, hasAuth := p.restv1BasicAuth(); hasAuth {
		httpReq.SetBasicAuth(user, pass)
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := p.cache.HTTPClient.Do(httpReq)
	if err != nil {
		p.lastHTTPRequestSuccessful.Store(false)

		return &PeerError{msg: fmt.Sprintf("rest command request failed: %s", err.Error()), kind: ConnectionError, srcErr: err}
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return &PeerError{msg: fmt.Sprintf("rest command response read failed: %s", err.Error()), kind: ConnectionError, srcErr: err}
	}

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &PeerError{msg: fmt.Sprintf("rest authentication failed (status %d): %s", resp.StatusCode, restv1Truncate(respBytes)), kind: ConnectionError}
	case http.StatusNotFound:
		return &PeerCommandError{code: resp.StatusCode, peer: p, err: fmt.Errorf("%s", notFoundMsg)}
	}
	if resp.StatusCode != http.StatusOK {
		return &PeerCommandError{code: resp.StatusCode, peer: p, err: fmt.Errorf("%s", restv1StatusText(respBytes))}
	}
	p.lastHTTPRequestSuccessful.Store(true)

	var parsed struct {
		Results []struct {
			Code   any    `json:"code"`
			Status string `json:"status"`
		} `json:"results"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return &PeerCommandError{code: resp.StatusCode, peer: p, err: fmt.Errorf("rest command response parse error: %s", err.Error())}
	}
	if len(parsed.Results) == 0 {
		return &PeerCommandError{code: http.StatusNotFound, peer: p, err: fmt.Errorf("%s", notFoundMsg)}
	}
	for i := range parsed.Results {
		if code := interface2int(parsed.Results[i].Code); code < 200 || code >= 300 {
			return &PeerCommandError{code: code, peer: p, err: fmt.Errorf("%s", parsed.Results[i].Status)}
		}
	}

	return nil
}

// restv1StatusText extracts the error message from a REST error response body.
func restv1StatusText(body []byte) string {
	var parsed struct {
		Status  string   `json:"status"`
		Errors  []string `json:"errors"`
		Results []struct {
			Status string `json:"status"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil {
		if len(parsed.Errors) > 0 {
			return strings.Join(parsed.Errors, "; ")
		}
		// action failures report their status in the results array
		statuses := make([]string, 0, len(parsed.Results))
		for i := range parsed.Results {
			if parsed.Results[i].Status != "" {
				statuses = append(statuses, parsed.Results[i].Status)
			}
		}
		if len(statuses) > 0 {
			return strings.Join(statuses, "; ")
		}
		if parsed.Status != "" {
			return parsed.Status
		}
	}

	return restv1Truncate(body)
}
