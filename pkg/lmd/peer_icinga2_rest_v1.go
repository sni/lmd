package lmd

// Icinga 2 REST v1 backend.
//
// lmd normally talks to backends using the Livestatus protocol. Icinga 2 installations
// can instead be accessed through the REST v1 API (https://<host>:5665/v1/objects/...).
// This file implements a translation layer that turns the small, fixed set of sync
// requests lmd issues (initial full fetch, timestamp based delta, status) into REST
// calls and maps the JSON answer back into the []any rows (ResultSet) the rest of lmd
// expects. The incoming Livestatus queries are always answered from the local cache,
// so only the backend sync path needs this translation.
//

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// restv1Field maps a single lmd column to an Icinga 2 REST attribute.
// attr is the top level attribute to request ("" when the value is derived).
// resolve optionally computes the lmd value from the requested attributes; when nil
// the value of attrs[attr] is used as-is.
type restv1Field struct {
	resolve func(attrs map[string]any) any
	attr    string
}

// restv1TableSpec describes how to fetch one object type and map its columns.
type restv1TableSpec struct {
	columns   map[string]restv1Field
	restType  string // REST /v1/objects/<type>
	filterVar string // DSL filter variable (lower case singular type name)
}

// restv1BaseURL returns the API base url for this peer.
// a missing path defaults to /v1.
func (p *Peer) restv1BaseURL() (string, error) {
	parsed, err := url.Parse(p.peerAddr.Get())
	if err != nil {
		return "", fmt.Errorf("url parse error: %s", err.Error())
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}
	path := parsed.Path
	if path == "" || path == "/" {
		path = "/v1"
	}

	return parsed.Scheme + "://" + parsed.Host + strings.TrimSuffix(path, "/"), nil
}

// restv1BasicAuth returns the basic auth credentials for this peer.
// Credentials embedded in the url win, the configured auth string is the fallback.
func (p *Peer) restv1BasicAuth() (user, pass string, ok bool) {
	if u, err := url.Parse(p.peerAddr.Get()); err == nil && u.User != nil {
		pass, _ = u.User.Password()

		return u.User.Username(), pass, true
	}
	if p.config.Auth != "" {
		if i := strings.IndexByte(p.config.Auth, ':'); i >= 0 {
			return p.config.Auth[:i], p.config.Auth[i+1:], true
		}

		return p.config.Auth, "", true
	}

	return "", "", false
}

// restv1ObjectResult is a single result entry of an /objects query.
type restv1ObjectResult struct {
	Attrs map[string]any `json:"attrs"`
	Name  string         `json:"name"`
	Type  string         `json:"type"`
}

// restv1BuildRequest builds a REST query request. When filter is not empty a
// POST with X-Http-Method-Override: GET is used because the API does not
// reliably decode '+' in query strings.
func (p *Peer) restv1BuildRequest(ctx context.Context, path string, attrs []string, filter string) (*http.Request, error) {
	base, err := p.restv1BaseURL()
	if err != nil {
		return nil, &PeerError{msg: err.Error(), kind: ConnectionError}
	}
	var request *http.Request
	var newErr *PeerError
	if filter != "" {
		body, mErr := json.Marshal(map[string]any{"attrs": attrs, "filter": filter})
		if mErr != nil {
			return nil, &PeerError{msg: mErr.Error(), kind: ResponseError}
		}
		request, newErr = restv1NewRequest(ctx, http.MethodPost, base+path, bytes.NewReader(body))
		if newErr != nil {
			return nil, newErr
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Http-Method-Override", "GET")
	} else {
		request, newErr = restv1NewRequest(ctx, http.MethodGet, restv1AttrsURL(base+path, attrs), http.NoBody)
		if newErr != nil {
			return nil, newErr
		}
	}
	if user, pass, hasAuth := p.restv1BasicAuth(); hasAuth {
		request.SetBasicAuth(user, pass)
	}
	request.Header.Set("Accept", "application/json")

	return request, nil
}

// restv1NewRequest creates an HTTP request, wrapping transport errors as a PeerError.
func restv1NewRequest(ctx context.Context, method, target string, body io.Reader) (*http.Request, *PeerError) {
	request, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, &PeerError{msg: err.Error(), kind: ConnectionError}
	}

	return request, nil
}

// restv1AttrsURL appends the requested attribute list as a query string.
func restv1AttrsURL(base string, attrs []string) string {
	if len(attrs) == 0 {
		return base
	}
	query := url.Values{}
	for idx := range attrs {
		query.Add("attrs", attrs[idx])
	}

	return base + "?" + query.Encode()
}

// restv1Do issues a single REST call against /objects/<restType> (or a custom path).
// It returns the list of attribute dicts, the response size and any error. A 404
// "No objects found" answer is treated as an empty result.
func (p *Peer) restv1Do(ctx context.Context, path string, attrs []string, filter string) (results []map[string]any, size int, err error) {
	request, err := p.restv1BuildRequest(ctx, path, attrs, filter)
	if err != nil {
		return nil, 0, err
	}

	resp, err := p.cache.HTTPClient.Do(request)
	if err != nil {
		p.lastHTTPRequestSuccessful.Store(false)

		return nil, 0, &PeerError{msg: fmt.Sprintf("rest request failed: %s", err.Error()), kind: ConnectionError, srcErr: err}
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	size = len(respBytes)
	if err != nil {
		return nil, size, &PeerError{msg: fmt.Sprintf("rest response read failed: %s", err.Error()), kind: ConnectionError, srcErr: err}
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		// "No objects found." is a valid empty result for object queries
		if strings.Contains(restv1StatusText(respBytes), "No objects found") {
			p.lastHTTPRequestSuccessful.Store(true)

			return nil, size, nil
		}

		return nil, size, &PeerError{msg: fmt.Sprintf("rest endpoint not found: %s", restv1Truncate(respBytes)), kind: ResponseError}
	case http.StatusUnauthorized, http.StatusForbidden:

		return nil, size, &PeerError{msg: fmt.Sprintf("rest authentication failed (status %d): %s", resp.StatusCode, restv1Truncate(respBytes)), kind: ConnectionError}
	default:

		return nil, size, &PeerError{msg: fmt.Sprintf("rest site returned status %d: %s", resp.StatusCode, restv1Truncate(respBytes)), kind: ResponseError}
	}

	var parsed struct {
		Results []restv1ObjectResult `json:"results"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, size, &PeerError{msg: fmt.Sprintf("rest json parse error: %s", err.Error()), kind: ResponseError, srcErr: err}
	}
	results = make([]map[string]any, 0, len(parsed.Results))
	for i := range parsed.Results {
		if parsed.Results[i].Attrs == nil {
			return nil, size, &PeerError{msg: "rest object response is missing attributes", kind: ResponseError}
		}
		parsed.Results[i].Attrs["_object_name"] = parsed.Results[i].Name
		results = append(results, parsed.Results[i].Attrs)
	}
	p.lastHTTPRequestSuccessful.Store(true)

	return results, size, nil
}

// restv1Truncate limits the size of response bodies included in error messages.
const restv1TruncateSize = 512

func restv1Truncate(b []byte) string {
	if len(b) > restv1TruncateSize {
		return string(b[:restv1TruncateSize]) + "..."
	}

	return string(b)
}

// restv1Meta builds the ResultMetaData for a REST query.
func (p *Peer) restv1Meta(req *Request, size, total int, t1 time.Time) *ResultMetaData {
	return &ResultMetaData{
		Request:  req,
		Total:    int64(total),
		Size:     size,
		Duration: time.Since(t1),
	}
}

func restv1ColumnRequested(columns []string, name string) bool {
	return slices.Contains(columns, name)
}

// restv1AttrList returns the list of REST attributes required to satisfy the given lmd columns.
func restv1AttrList(spec restv1TableSpec, columns []string) []string {
	seen := make(map[string]bool, len(columns))
	attrs := make([]string, 0, len(columns))
	for idx := range columns {
		field, ok := spec.columns[columns[idx]]
		if !ok || field.attr == "" || seen[field.attr] {
			continue
		}
		seen[field.attr] = true
		attrs = append(attrs, field.attr)
	}
	slices.Sort(attrs)

	return attrs
}

// restv1Row builds a single lmd row (ordered by columns) from the REST attributes.
func restv1Row(spec restv1TableSpec, columns []string, attrs map[string]any) []any {
	row := make([]any, len(columns))
	for idx := range columns {
		field, ok := spec.columns[columns[idx]]
		if !ok {
			row[idx] = nil

			continue
		}
		if field.resolve != nil {
			row[idx] = field.resolve(attrs)

			continue
		}
		if field.attr == "" {
			row[idx] = nil

			continue
		}
		row[idx] = attrs[field.attr]
	}

	return row
}

// restv1TableSpecFor returns the spec for a given table or nil if unsupported.
func restv1TableSpecFor(table TableName) (spec restv1TableSpec, ok bool) {
	switch table {
	case TableHosts:

		return restv1HostSpec, true
	case TableServices:

		return restv1ServiceSpec, true
	case TableHostgroups:

		return restv1HostgroupsSpec, true
	case TableServicegroups:

		return restv1ServicegroupsSpec, true
	case TableTimeperiods:

		return restv1TimeperiodsSpec, true
	case TableCommands:

		return restv1CommandsSpec, true
	case TableContacts:

		return restv1ContactsSpec, true
	case TableContactgroups:

		return restv1ContactgroupsSpec, true
	case TableComments:

		return restv1CommentsSpec, true
	case TableDowntimes:

		return restv1DowntimesSpec, true
	default:

		return restv1TableSpec{}, false
	}
}

// restv1GroupTableFor returns the member object type used to derive the members of a group table.
func restv1GroupTableFor(table TableName) (memberType string, serviceMember, ok bool) {
	switch table {
	case TableHostgroups:

		return "hosts", false, true
	case TableServicegroups:

		return "services", true, true
	case TableContactgroups:

		return "users", false, true
	default:

		return "", false, false
	}
}

// restv1BuildMembers fetches all member objects of the given type and derives a
// group name -> members map from their groups attribute.
func (p *Peer) restv1BuildMembers(ctx context.Context, memberType string, serviceMember bool) (map[string][]any, error) {
	attrs := []string{"groups"}
	if serviceMember {
		attrs = append(attrs, "host_name", "name")
	} else {
		attrs = append(attrs, "name")
	}
	results, _, err := p.restv1Do(ctx, "/objects/"+memberType, attrs, "")
	if err != nil {
		return nil, err
	}
	members := make(map[string][]any, len(results))
	for idx := range results {
		item := results[idx]
		groups := restv1StringList(item["groups"])
		if len(groups) == 0 {
			continue
		}
		if serviceMember {
			host := restv1String(item["host_name"])
			name := restv1String(item["name"])
			for i := range groups {
				members[groups[i]] = append(members[groups[i]], []any{host, name})
			}

			continue
		}
		name := restv1String(item["name"])
		for i := range groups {
			members[groups[i]] = append(members[groups[i]], name)
		}
	}

	return members, nil
}

// icinga2RestV1Query handles a sync request for a REST v1 backend.
// It is the dispatch target of Peer.queryCB when the Icinga2RestV1 flag is set.
func (p *Peer) icinga2RestV1Query(ctx context.Context, req *Request, clb RowResultCB) (ResultSet, *ResultMetaData, error) {
	t1 := time.Now()

	// commands are translated into REST API requests
	if req.Command != "" {
		return p.icinga2RestV1SendCommands(ctx, req)
	}

	switch req.Table {
	case TableStatus:

		return p.restv1QueryStatus(ctx, req, t1)
	case TableLog:

		return nil, nil, &PeerError{msg: "the log table is not available for Icinga2 RESTv1 backends", kind: ResponseError}
	default:
		// every other table is resolved through the table spec below
	}

	spec, ok := restv1TableSpecFor(req.Table)
	if !ok {
		return nil, nil, &PeerError{msg: fmt.Sprintf("table %s is not supported for Icinga2 RESTv1 backends", req.Table.String()), kind: ResponseError}
	}

	// the REST API has no pagination: the first page (offset == 0) returns the full
	// (filtered) result set, any follow up page (offset > 0) returns nothing.
	if req.Offset > 0 {
		return ResultSet{}, p.restv1Meta(req, 0, 0, t1), nil
	}

	filter, err := restv1TranslateFilter(req, spec)
	if err != nil {
		// fall back to a full fetch, the delta is best effort anyway
		logWith(p, req).Debugf("restv1 filter translation failed, doing full fetch: %s", err.Error())
		filter = ""
	}

	attrs := restv1AttrList(spec, req.Columns)
	results, size, err := p.restv1Do(ctx, "/objects/"+spec.restType, attrs, filter)
	if err != nil {
		return nil, nil, err
	}

	// derive group members with a second call when requested
	var members map[string][]any
	var serviceMember bool
	if memberType, sm, isGroup := restv1GroupTableFor(req.Table); isGroup && restv1ColumnRequested(req.Columns, "members") {
		members, err = p.restv1BuildMembers(ctx, memberType, sm)
		if err != nil {
			return nil, nil, err
		}
		serviceMember = sm
	}

	res := make(ResultSet, 0, len(results))
	table := Objects.Tables[req.Table]
	for i := range results {
		row := restv1Row(spec, req.Columns, results[i])
		if members != nil {
			row = restv1ApplyMembers(row, req.Columns, results[i], members, serviceMember)
		}
		res = append(res, row)
	}

	// the API does not guarantee any order, lmd expects the rows sorted by primary key
	sorted := ResultSetSorted{Data: res}
	for _, name := range table.primaryKey {
		if idx := slices.Index(req.Columns, name); idx >= 0 {
			sorted.Keys = append(sorted.Keys, idx)
			sorted.Types = append(sorted.Types, table.GetColumn(name).DataType)
		}
	}
	sort.Sort(&sorted)

	if clb != nil {
		for i := range res {
			if err := clb(res[i], 0); err != nil {
				return nil, nil, err
			}
		}

		return nil, p.restv1Meta(req, size, len(res), t1), nil
	}

	return res, p.restv1Meta(req, size, len(res), t1), nil
}

// restv1ApplyMembers fills the members column of a group row from the derived members map.
func restv1ApplyMembers(row []any, columns []string, attrs map[string]any, members map[string][]any, serviceMember bool) []any {
	groupName := restv1String(attrs["name"])
	for idx := range columns {
		if columns[idx] != "members" {
			continue
		}
		if !serviceMember {
			vals := make([]string, 0, len(members[groupName]))
			for i := range members[groupName] {
				vals = append(vals, restv1String(members[groupName][i]))
			}
			slices.Sort(vals)
			row[idx] = vals
		} else {
			row[idx] = slices.Clone(members[groupName])
		}
	}

	return row
}

// restv1QueryStatus synthesizes the lmd status row from /v1/status.
func (p *Peer) restv1QueryStatus(ctx context.Context, req *Request, start time.Time) (ResultSet, *ResultMetaData, error) {
	base, err := p.restv1BaseURL()
	if err != nil {
		return nil, nil, &PeerError{msg: err.Error(), kind: ConnectionError}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/status", http.NoBody)
	if err != nil {
		return nil, nil, &PeerError{msg: err.Error(), kind: ConnectionError}
	}
	if user, pass, hasAuth := p.restv1BasicAuth(); hasAuth {
		httpReq.SetBasicAuth(user, pass)
	}
	httpReq.Header.Set("Accept", "application/json")
	resp, err := p.cache.HTTPClient.Do(httpReq)
	if err != nil {
		p.lastHTTPRequestSuccessful.Store(false)

		return nil, nil, &PeerError{msg: fmt.Sprintf("rest status request failed: %s", err.Error()), kind: ConnectionError, srcErr: err}
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	size := len(respBytes)
	if err != nil {
		return nil, nil, &PeerError{msg: fmt.Sprintf("rest status read failed: %s", err.Error()), kind: ConnectionError, srcErr: err}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, &PeerError{msg: fmt.Sprintf("rest status site returned %d: %s", resp.StatusCode, restv1Truncate(respBytes)), kind: ResponseError}
	}

	var parsed struct {
		Results []struct {
			Status map[string]any `json:"status"`
			Name   string         `json:"name"`
		} `json:"results"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, nil, &PeerError{msg: fmt.Sprintf("rest status json parse error: %s", err.Error()), kind: ResponseError, srcErr: err}
	}

	// the IcingaApplication entry carries program_start, pid and version
	var app map[string]any
	for i := range parsed.Results {
		ic, ok := parsed.Results[i].Status["icingaapplication"]
		if !ok {
			continue
		}
		if icMap, ok := ic.(map[string]any); ok {
			if a, ok := icMap["app"].(map[string]any); ok {
				app = a

				break
			}
		}
	}
	if app == nil {
		return nil, nil, &PeerError{msg: "rest status did not contain the icingaapplication data", kind: ResponseError}
	}

	row := make([]any, len(req.Columns))
	for i := range req.Columns {
		row[i] = restv1StatusValue(req.Columns[i], app)
	}
	res := ResultSet{row}
	p.lastHTTPRequestSuccessful.Store(true)

	return res, p.restv1Meta(req, size, 1, start), nil
}

// restv1StatusValue returns the lmd status column value from the icingaapplication data.
func restv1StatusValue(column string, app map[string]any) any {
	switch column {
	case "program_start":

		return interface2int64(app["program_start"])
	case "nagios_pid":

		return interface2int64(app["pid"])
	// the icinga version matches the version regexes used for flag detection
	case "program_version", "livestatus_version":

		return restv1String(app["version"])
	case "enable_event_handlers":

		return interface2int(app["enable_event_handlers"])
	case "enable_flap_detection":

		return interface2int(app["enable_flapping"])
	case "enable_notifications":

		return interface2int(app["enable_notifications"])
	case "execute_host_checks":

		return interface2int(app["enable_host_checks"])
	case "execute_service_checks":

		return interface2int(app["enable_service_checks"])
	case "process_performance_data":

		return interface2int(app["enable_perfdata"])
	default:

		return nil
	}
}

var reRestv1GroupOp = regexp.MustCompile(`^(And|Or):\s*(\d+)$`)

// restv1TranslateFilter converts the livestatus style filter (Filter:/And:/Or: lines)
// into an Icinga 2 DSL expression. Only the sync filter shapes (scalar comparisons)
// are supported; a parse problem results in an error which the caller turns into a
// full fetch.
func restv1TranslateFilter(req *Request, spec restv1TableSpec) (string, error) {
	filterStr := req.FilterStr
	if len(req.Filter) > 0 {
		// filters parsed from an incoming request are not supported for REST backends

		return "", fmt.Errorf("complex filters are not supported for REST backends")
	}
	if strings.TrimSpace(filterStr) == "" {
		return "", nil
	}

	stack := make([]string, 0)
	for line := range strings.SplitSeq(filterStr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if matched := reRestv1GroupOp.FindStringSubmatch(line); matched != nil {
			n, _ := strconv.Atoi(matched[2])
			if n < 1 || len(stack) < n {
				return "", fmt.Errorf("group operator %s needs %d clauses but only %d are available", line, n, len(stack))
			}
			parts := stack[len(stack)-n:]
			stack = stack[:len(stack)-n]
			joiner := " && "
			if matched[1] == "Or" {
				joiner = " || "
			}
			stack = append(stack, "("+strings.Join(parts, joiner)+")")

			continue
		}
		if raw, ok := strings.CutPrefix(line, "Filter:"); ok {
			expr := strings.TrimSpace(raw)
			dsl, err := restv1FilterExpr(spec, expr)
			if err != nil {
				return "", err
			}
			stack = append(stack, dsl)

			continue
		}

		return "", fmt.Errorf("unsupported filter line: %s", line)
	}
	if len(stack) == 0 {
		return "", nil
	}
	if len(stack) > 1 {
		return "(" + strings.Join(stack, " && ") + ")", nil
	}

	return stack[0], nil
}

// restv1FilterExpr translates a single "column op value" clause into a DSL expression.
func restv1FilterExpr(spec restv1TableSpec, expr string) (string, error) {
	fields := strings.Fields(expr)
	if len(fields) < 3 {
		return "", fmt.Errorf("bad filter clause: %s", expr)
	}
	col, oper, val := fields[0], fields[1], strings.Join(fields[2:], " ")
	field, ok := spec.columns[col]
	if !ok || field.attr == "" {
		return "", fmt.Errorf("filter column %s is not supported for REST backends", col)
	}
	dslOp, ok := restv1FilterOp(oper)
	if !ok {
		return "", fmt.Errorf("filter operator %s is not supported", oper)
	}

	return spec.filterVar + "." + field.attr + " " + dslOp + " " + restv1FilterValue(val), nil
}

func restv1FilterOp(op string) (string, bool) {
	switch op {
	case "=", "==":

		return "==", true
	case "!=":

		return "!=", true
	case "<":

		return "<", true
	case "<=":

		return "<=", true
	case ">":

		return ">", true
	case ">=":

		return ">=", true
	}

	return "", false
}

func restv1FilterValue(val string) string {
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return val
	}

	return strconv.Quote(val)
}

// --- value helpers ---

func restv1Map(v any) map[string]any {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}

	return nil
}

func restv1String(v any) string {
	switch val := v.(type) {
	case string:

		return val
	case nil:

		return ""
	default:

		return fmt.Sprintf("%v", val)
	}
}

func restv1StringList(v any) []string {
	out := make([]string, 0)
	switch val := v.(type) {
	case []any:
		for i := range val {
			out = append(out, restv1String(val[i]))
		}
	case string:
		if val != "" {
			out = append(out, val)
		}
	}

	return out
}

// restv1Stringify converts a custom variable value into its string representation.
func restv1Stringify(v any) string {
	switch val := v.(type) {
	case nil:

		return ""
	case string:

		return val
	case bool:
		if val {
			return "true"
		}

		return "false"
	case float64:
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10)
		}

		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		if data, err := json.Marshal(val); err == nil {
			return string(data)
		}

		return fmt.Sprintf("%v", val)
	}
}

// restv1CheckResult helpers read from the last_check_result attribute.
func restv1Lcr(attrs map[string]any) map[string]any {
	return restv1Map(attrs["last_check_result"])
}

func restv1LcrString(attrs map[string]any, key string) any {
	if lcr := restv1Lcr(attrs); lcr != nil {
		return restv1String(lcr[key])
	}

	return ""
}

func restv1PluginOutput(attrs map[string]any) any {
	output, _, _ := strings.Cut(restv1String(restv1Lcr(attrs)["output"]), "\n")

	return output
}

func restv1LongPluginOutput(attrs map[string]any) any {
	_, output, _ := strings.Cut(restv1String(restv1Lcr(attrs)["output"]), "\n")

	return output
}

func restv1CheckInterval(attrs map[string]any) any {
	return interface2float64(attrs["check_interval"]) / 60
}

func restv1RetryInterval(attrs map[string]any) any {
	return interface2float64(attrs["retry_interval"]) / 60
}

func restv1CheckSource(attrs map[string]any) any {
	return restv1LcrString(attrs, "check_source")
}

func restv1PerfData(attrs map[string]any) any {
	if lcr := restv1Lcr(attrs); lcr != nil {
		if list, ok := lcr["performance_data"].([]any); ok {
			vals := make([]string, 0, len(list))
			for i := range list {
				vals = append(vals, restv1String(list[i]))
			}

			return strings.Join(vals, " ")
		}
	}

	return ""
}

func restv1ExecutionTime(attrs map[string]any) any {
	if lcr := restv1Lcr(attrs); lcr != nil {
		return interface2float64(lcr["execution_end"]) - interface2float64(lcr["execution_start"])
	}

	return 0.0
}

func restv1Latency(attrs map[string]any) any {
	if lcr := restv1Lcr(attrs); lcr != nil {
		return interface2float64(lcr["execution_start"]) - interface2float64(lcr["schedule_start"])
	}

	return 0.0
}

func restv1CheckType(attrs map[string]any) any {
	if lcr := restv1Lcr(attrs); lcr != nil {
		if interface2bool(lcr["active"]) {
			return 0
		}

		return 1
	}

	return 0
}

func restv1HasBeenChecked(attrs map[string]any) any {
	if interface2int64(attrs["last_check"]) > 0 {
		return 1
	}

	return 0
}

func restv1Acknowledged(attrs map[string]any) any {
	if interface2int(attrs["acknowledgement"]) != 0 {
		return 1
	}

	return 0
}

func restv1CustomVarNames(attrs map[string]any) any {
	vals := make([]string, 0)
	if vars := restv1Map(attrs["vars"]); vars != nil {
		for key := range vars {
			vals = append(vals, key)
		}
		slices.Sort(vals)
	}

	return vals
}

func restv1CustomVarValues(attrs map[string]any) any {
	vars := restv1Map(attrs["vars"])
	keys := make([]string, 0, len(vars))
	for key := range vars {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	vals := make([]string, 0, len(keys))
	for i := range keys {
		vals = append(vals, restv1Stringify(vars[keys[i]]))
	}

	return vals
}

func restv1CommandLine(attrs map[string]any) any {
	switch cmd := attrs["command"].(type) {
	case nil:

		return ""
	case string:

		return cmd
	case []any:
		vals := make([]string, 0, len(cmd))
		for i := range cmd {
			vals = append(vals, restv1String(cmd[i]))
		}

		return strings.Join(vals, " ")
	default:

		return fmt.Sprintf("%v", cmd)
	}
}

func (ds *DataStoreSet) restv1UpdateAnnotations(ctx context.Context) error {
	for _, name := range []TableName{TableComments, TableDowntimes} {
		store, err := ds.createObjectByType(ctx, Objects.Tables[name])
		if err != nil {
			return err
		}
		if err := store.SetReferences(); err != nil {
			return err
		}
		ds.setTable(name, store)
	}
	if err := ds.rebuildCommentsList(); err != nil {
		return err
	}
	if err := ds.rebuildDowntimesList(); err != nil {
		return err
	}
	ds.peer.forceComments.Store(false)

	return nil
}

func restv1IsService(attrs map[string]any) any {
	return restv1String(attrs["service_name"]) != ""
}

func restv1CommentType(attrs map[string]any) any {
	if restv1String(attrs["service_name"]) != "" {
		return 2
	}

	return 1
}

// --- column maps ---

// restv1HostSpec maps the lmd hosts columns to Icinga 2 host attributes.
var restv1HostSpec = restv1TableSpec{
	restType:  "hosts",
	filterVar: "host",
	columns: map[string]restv1Field{
		"name":                     {attr: "name"},
		"display_name":             {attr: "display_name"},
		"alias":                    {attr: "display_name"},
		"address":                  {attr: "address"},
		"address6":                 {attr: "address6"},
		"check_command":            {attr: "check_command"},
		"check_interval":           {attr: "check_interval", resolve: restv1CheckInterval},
		"retry_interval":           {attr: "retry_interval", resolve: restv1RetryInterval},
		"max_check_attempts":       {attr: "max_check_attempts"},
		"check_period":             {attr: "check_period"},
		"notes":                    {attr: "notes"},
		"notes_url":                {attr: "notes_url"},
		"notes_expanded":           {attr: "notes"},
		"notes_url_expanded":       {attr: "notes_url"},
		"action_url":               {attr: "action_url"},
		"action_url_expanded":      {attr: "action_url"},
		"icon_image":               {attr: "icon_image"},
		"icon_image_alt":           {attr: "icon_image_alt"},
		"icon_image_expanded":      {attr: "icon_image"},
		"groups":                   {attr: "groups"},
		"state":                    {attr: "state"},
		"hard_state":               {attr: "last_hard_state"},
		"last_state":               {attr: "last_state"},
		"last_state_change":        {attr: "last_state_change"},
		"last_hard_state":          {attr: "last_hard_state"},
		"last_hard_state_change":   {attr: "last_hard_state_change"},
		"last_check":               {attr: "last_check"},
		"next_check":               {attr: "next_check"},
		"last_time_up":             {attr: "last_state_up"},
		"last_time_down":           {attr: "last_state_down"},
		"last_time_unreachable":    {attr: "last_state_unreachable"},
		"current_attempt":          {attr: "check_attempt"},
		"scheduled_downtime_depth": {attr: "downtime_depth"},
		"is_flapping":              {attr: "flapping"},
		"percent_state_change":     {attr: "flapping_current"},
		"flap_detection_enabled":   {attr: "enable_flapping"},
		"high_flap_threshold":      {attr: "flapping_threshold_high"},
		"low_flap_threshold":       {attr: "flapping_threshold_low"},
		"active_checks_enabled":    {attr: "enable_active_checks"},
		"accept_passive_checks":    {attr: "enable_passive_checks"},
		"event_handler_enabled":    {attr: "enable_event_handler"},
		"notifications_enabled":    {attr: "enable_notifications"},
		"process_performance_data": {attr: "enable_perfdata"},
		"state_type":               {attr: "state_type"},
		"checks_enabled":           {attr: "enable_active_checks"},
		"has_been_checked":         {attr: "last_check", resolve: restv1HasBeenChecked},
		"acknowledged":             {attr: "acknowledgement", resolve: restv1Acknowledged},
		"plugin_output":            {attr: "last_check_result", resolve: restv1PluginOutput},
		"long_plugin_output":       {attr: "last_check_result", resolve: restv1LongPluginOutput},
		"perf_data":                {attr: "last_check_result", resolve: restv1PerfData},
		"execution_time":           {attr: "last_check_result", resolve: restv1ExecutionTime},
		"latency":                  {attr: "last_check_result", resolve: restv1Latency},
		"check_type":               {attr: "last_check_result", resolve: restv1CheckType},
		"check_source":             {attr: "last_check_result", resolve: restv1CheckSource},
		"custom_variable_names":    {attr: "vars", resolve: restv1CustomVarNames},
		"custom_variable_values":   {attr: "vars", resolve: restv1CustomVarValues},
	},
}

// restv1ServiceSpec maps the lmd services columns to Icinga 2 service attributes.
var restv1ServiceSpec = restv1TableSpec{
	restType:  "services",
	filterVar: "service",
	columns: map[string]restv1Field{
		"host_name":                {attr: "host_name"},
		"description":              {attr: "name"},
		"display_name":             {attr: "display_name"},
		"check_command":            {attr: "check_command"},
		"check_interval":           {attr: "check_interval", resolve: restv1CheckInterval},
		"retry_interval":           {attr: "retry_interval", resolve: restv1RetryInterval},
		"max_check_attempts":       {attr: "max_check_attempts"},
		"check_period":             {attr: "check_period"},
		"notes":                    {attr: "notes"},
		"notes_url":                {attr: "notes_url"},
		"notes_expanded":           {attr: "notes"},
		"notes_url_expanded":       {attr: "notes_url"},
		"action_url":               {attr: "action_url"},
		"action_url_expanded":      {attr: "action_url"},
		"icon_image":               {attr: "icon_image"},
		"icon_image_alt":           {attr: "icon_image_alt"},
		"icon_image_expanded":      {attr: "icon_image"},
		"groups":                   {attr: "groups"},
		"event_handler":            {attr: "event_command"},
		"event_handler_enabled":    {attr: "enable_event_handler"},
		"state":                    {attr: "state"},
		"hard_state":               {attr: "last_hard_state"},
		"last_state":               {attr: "last_state"},
		"last_state_change":        {attr: "last_state_change"},
		"last_hard_state":          {attr: "last_hard_state"},
		"last_hard_state_change":   {attr: "last_hard_state_change"},
		"last_check":               {attr: "last_check"},
		"next_check":               {attr: "next_check"},
		"last_time_ok":             {attr: "last_state_ok"},
		"last_time_warning":        {attr: "last_state_warning"},
		"last_time_critical":       {attr: "last_state_critical"},
		"last_time_unknown":        {attr: "last_state_unknown"},
		"current_attempt":          {attr: "check_attempt"},
		"scheduled_downtime_depth": {attr: "downtime_depth"},
		"is_flapping":              {attr: "flapping"},
		"percent_state_change":     {attr: "flapping_current"},
		"flap_detection_enabled":   {attr: "enable_flapping"},
		"high_flap_threshold":      {attr: "flapping_threshold_high"},
		"low_flap_threshold":       {attr: "flapping_threshold_low"},
		"active_checks_enabled":    {attr: "enable_active_checks"},
		"accept_passive_checks":    {attr: "enable_passive_checks"},
		"notifications_enabled":    {attr: "enable_notifications"},
		"process_performance_data": {attr: "enable_perfdata"},
		"state_type":               {attr: "state_type"},
		"checks_enabled":           {attr: "enable_active_checks"},
		"has_been_checked":         {attr: "last_check", resolve: restv1HasBeenChecked},
		"acknowledged":             {attr: "acknowledgement", resolve: restv1Acknowledged},
		"acknowledgement_type":     {attr: "acknowledgement"},
		"plugin_output":            {attr: "last_check_result", resolve: restv1PluginOutput},
		"long_plugin_output":       {attr: "last_check_result", resolve: restv1LongPluginOutput},
		"perf_data":                {attr: "last_check_result", resolve: restv1PerfData},
		"execution_time":           {attr: "last_check_result", resolve: restv1ExecutionTime},
		"latency":                  {attr: "last_check_result", resolve: restv1Latency},
		"check_type":               {attr: "last_check_result", resolve: restv1CheckType},
		"check_source":             {attr: "last_check_result", resolve: restv1CheckSource},
		"custom_variable_names":    {attr: "vars", resolve: restv1CustomVarNames},
		"custom_variable_values":   {attr: "vars", resolve: restv1CustomVarValues},
	},
}

var restv1HostgroupsSpec = restv1TableSpec{
	restType:  "hostgroups",
	filterVar: "hostgroup",
	columns: map[string]restv1Field{
		"name":       {attr: "name"},
		"alias":      {attr: "display_name"},
		"action_url": {attr: "action_url"},
		"notes":      {attr: "notes"},
		"notes_url":  {attr: "notes_url"},
		"members":    {attr: ""},
	},
}

var restv1ServicegroupsSpec = restv1TableSpec{
	restType:  "servicegroups",
	filterVar: "servicegroup",
	columns: map[string]restv1Field{
		"name":       {attr: "name"},
		"alias":      {attr: "display_name"},
		"action_url": {attr: "action_url"},
		"notes":      {attr: "notes"},
		"notes_url":  {attr: "notes_url"},
		"members":    {attr: ""},
	},
}

var restv1TimeperiodsSpec = restv1TableSpec{
	restType:  "timeperiods",
	filterVar: "timeperiod",
	columns: map[string]restv1Field{
		"name":  {attr: "name"},
		"alias": {attr: "display_name"},
		"in":    {attr: "is_inside"},
	},
}

var restv1CommandsSpec = restv1TableSpec{
	restType:  "checkcommands",
	filterVar: "checkcommand",
	columns: map[string]restv1Field{
		"name": {attr: "name"},
		"line": {attr: "command", resolve: restv1CommandLine},
	},
}

var restv1ContactsSpec = restv1TableSpec{
	restType:  "users",
	filterVar: "user",
	columns: map[string]restv1Field{
		"name":                          {attr: "name"},
		"alias":                         {attr: "display_name"},
		"email":                         {attr: "email"},
		"pager":                         {attr: "pager"},
		"host_notification_period":      {attr: "period"},
		"service_notification_period":   {attr: "period"},
		"host_notifications_enabled":    {attr: "enable_notifications"},
		"service_notifications_enabled": {attr: "enable_notifications"},
	},
}

var restv1ContactgroupsSpec = restv1TableSpec{
	restType:  "usergroups",
	filterVar: "usergroup",
	columns: map[string]restv1Field{
		"name":    {attr: "name"},
		"alias":   {attr: "display_name"},
		"members": {attr: ""},
	},
}

var restv1CommentsSpec = restv1TableSpec{
	restType: "comments", filterVar: "comment",
	columns: map[string]restv1Field{
		"id":                  {attr: "legacy_id"},
		"host_name":           {attr: "host_name"},
		"service_description": {attr: "service_name"},
		"author":              {attr: "author"},
		"comment":             {attr: "text"},
		"entry_time":          {attr: "entry_time"},
		"entry_type":          {attr: "entry_type"},
		"expire_time":         {attr: "expire_time"},
		"expires":             {attr: "expire_time", resolve: func(attrs map[string]any) any { return interface2float64(attrs["expire_time"]) > 0 }},
		"persistent":          {attr: "persistent"},
		"source":              {resolve: func(map[string]any) any { return 1 }},
		"is_service":          {attr: "service_name", resolve: restv1IsService},
		"type":                {attr: "service_name", resolve: restv1CommentType},
	},
}

var restv1DowntimesSpec = restv1TableSpec{
	restType: "downtimes", filterVar: "downtime",
	columns: map[string]restv1Field{
		"id":                  {attr: "legacy_id"},
		"host_name":           {attr: "host_name"},
		"service_description": {attr: "service_name"},
		"author":              {attr: "author"},
		"comment":             {attr: "comment"},
		"entry_time":          {attr: "entry_time"},
		"start_time":          {attr: "start_time"},
		"end_time":            {attr: "end_time"},
		"duration":            {attr: "duration"},
		"fixed":               {attr: "fixed"},
		"is_service":          {attr: "service_name", resolve: restv1IsService},
		"type": {attr: "trigger_time", resolve: func(attrs map[string]any) any {
			if interface2float64(attrs["trigger_time"]) > 0 {
				return 0
			}

			return 1
		}},
	},
}
