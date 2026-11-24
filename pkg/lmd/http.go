package lmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/julienschmidt/httprouter"
)

// HTTPServerController is the container object for the rest interface's server.
type HTTPServerController struct {
	lmd *Daemon
}

func (c *HTTPServerController) errorOutput(err error, wrt http.ResponseWriter) {
	j := make(map[string]any)
	j["error"] = err.Error()
	wrt.Header().Set("Content-Type", "application/json")
	wrt.WriteHeader(http.StatusBadRequest)
	err = json.NewEncoder(wrt).Encode(j)
	if err != nil {
		log.Debugf("encoder failed: %e", err)
	}
}

func (c *HTTPServerController) index(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "LMD %s\n", VERSION)
}

func (c *HTTPServerController) queryTable(ctx context.Context, wrt http.ResponseWriter, requestData map[string]any) {
	wrt.Header().Set("Content-Type", "application/json")

	// Requested table (name)
	_, err := NewTableName(interface2stringNoDedup(requestData["table"]))
	// Check if table exists
	if err != nil {
		c.errorOutput(err, wrt)

		return
	}

	req, err := parseRequestDataToRequest(requestData, c.lmd.Config.MaxQuerySize)
	if err != nil {
		c.errorOutput(err, wrt)

		return
	}

	// Fetch backend data
	req.lmd = c.lmd
	req.ExpandRequestedBackends()

	var res *Response
	if d, exists := requestData["distributed"]; exists && interface2bool(d) {
		// force local answer to avoid recursion
		res, _, _, err = NewResponse(ctx, req, nil)
	} else {
		// Ask request object to send query, get response, might get distributed
		res, err = req.BuildResponse(ctx)
	}
	if err != nil {
		c.errorOutput(err, wrt)

		return
	}

	if res.result == nil {
		res.SetResultData()
	}

	// Send JSON
	buf, err := res.Buffer()
	if err != nil {
		c.errorOutput(err, wrt)

		return
	}
	_, err = buf.WriteTo(wrt)
	if err != nil {
		log.Debugf("writeto failed: %e", err)
	}
}

func (c *HTTPServerController) table(wrt http.ResponseWriter, request *http.Request, params httprouter.Params) {
	// Read request data
	requestData := make(map[string]any)
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&requestData); err != nil && !errors.Is(err, io.EOF) {
		log.Errorf("failed to parse request parameters: %s", err.Error())
		c.errorOutput(fmt.Errorf("request not understood"), wrt)

		return
	}

	// Use table name defined in rest request
	if tableName := params.ByName("name"); tableName != "" {
		requestData["table"] = tableName
	}

	c.queryTable(request.Context(), wrt, requestData)
}

func (c *HTTPServerController) ping(wrt http.ResponseWriter, request *http.Request, _ httprouter.Params) {
	// Read request data
	requestData := make(map[string]any)
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&requestData); err != nil && !errors.Is(err, io.EOF) {
		log.Errorf("failed to parse request parameters: %s", err.Error())
		c.errorOutput(fmt.Errorf("request not understood"), wrt)

		return
	}
	c.queryPing(wrt, requestData)
}

func (c *HTTPServerController) queryPing(wrt http.ResponseWriter, requestData map[string]any) {
	// Response data
	wrt.Header().Set("Content-Type", "application/json")
	id := c.lmd.nodeAccessor.ID
	jsonData := make(map[string]any)
	jsonData["identifier"] = id
	jsonData["peers"] = c.lmd.nodeAccessor.assignedBackends
	jsonData["version"] = Version()
	jsonData["authsig"] = clusterAuthSignature(c.lmd.Config.ClusterAuth,
		interface2stringNoDedup(requestData["authnonce"]), id, c.lmd.nodeAccessor.assignedBackends)

	// Send data
	err := json.NewEncoder(wrt).Encode(jsonData)
	if err != nil {
		log.Errorf("sending ping result failed: %s", err.Error())
	}
}

func (c *HTTPServerController) query(wrt http.ResponseWriter, request *http.Request, _ httprouter.Params) {
	// Read request data
	contentType := request.Header.Get("Content-Type")
	requestData := make(map[string]any)
	defer request.Body.Close()
	if contentType == "application/json" {
		decoder := json.NewDecoder(request.Body)
		err := decoder.Decode(&requestData)
		if err != nil && !errors.Is(err, io.EOF) {
			log.Errorf("failed to parse request parameters: %s", err.Error())
			c.errorOutput(fmt.Errorf("request not understood"), wrt)

			return
		}
	}

	// Request type (requested api function)
	requestedFunction, _ := requestData["_name"].(string)

	switch requestedFunction {
	case "ping":
		c.queryPing(wrt, requestData)
	case "table":
		c.queryTable(request.Context(), wrt, requestData)
	default:
		c.errorOutput(fmt.Errorf("unknown request: %s", requestedFunction), wrt)
	}
}

// rejectLineBreaks returns an error if value contains a line break. Such a
// value would end the current livestatus header line and allow appending
// further headers or a whole COMMAND request to the backend query.
func rejectLineBreaks(field, value string) error {
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("bad request: line break in %s value", field)
	}

	return nil
}

// requestValueList converts a JSON request list to validated strings,
// rejecting entries that carry line breaks.
func requestValueList(field string, val any) ([]string, error) {
	var values []string
	for _, item := range interface2interfaceList(val) {
		name := interface2stringNoDedup(item)
		if cerr := rejectLineBreaks(field, name); cerr != nil {
			return nil, cerr
		}
		values = append(values, name)
	}

	return values, nil
}

// parseRequestDataToRequest converts the request data into a request object.
// maxQuerySize limits the total bytes of filter lines, 0 disables the check.
func parseRequestDataToRequest(requestData map[string]any, maxQuerySize int) (req *Request, err error) {
	// New request object for specified table
	req = &Request{}
	table, err := NewTableName(interface2stringNoDedup(requestData["table"]))
	if err != nil {
		return nil, err
	}
	req.Table = table

	// Send header row by default
	req.ColumnsHeaders = true
	if val, ok := requestData["sendcolumnsheader"]; ok {
		req.ColumnsHeaders = interface2bool(val)
	}

	// Offset
	req.Offset = interface2int(requestData["offset"])

	// Limit
	if val, ok := requestData["limit"]; ok {
		req.Limit = new(int)
		*req.Limit = interface2int(val)
	}

	// Filter String in livestatus syntax
	if val, ok := requestData["filter"]; ok {
		err = parseHTTPFilterRequestData(req, val, "Filter", maxQuerySize)
		if err != nil {
			return req, err
		}
	}

	// Stats String in livestatus syntax
	if val, ok := requestData["stats"]; ok {
		err = parseHTTPFilterRequestData(req, val, "Stats", maxQuerySize)
		if err != nil {
			return req, err
		}
		req.SendStatsData = len(req.Stats) > 0
	}

	// Sort
	var requestDataSort []string
	if val, ok := requestData["sort"]; ok {
		requestDataSort, err = requestValueList("Sort", val)
		if err != nil {
			return req, err
		}
	}
	for _, line := range requestDataSort {
		err = parseSortHeader(&req.Sort, []byte(line))
		if err != nil {
			return req, err
		}
	}

	// Columns
	if val, ok := requestData["columns"]; ok {
		columns, cerr := requestValueList("Columns", val)
		if cerr != nil {
			return req, cerr
		}
		req.Columns = slices.DeleteFunc(columns, func(name string) bool { return name == "empty" })
	}
	req.SetRequestColumns()

	// Resolve the sort columns so the fields carry their column reference.
	// The socket path does this in ParseRequests; without it a sort field has
	// a nil column, which panics (and drops the connection) when the result is
	// sorted.
	if cerr := req.SetSortColumns(); cerr != nil {
		return req, cerr
	}

	// Format
	if val, ok := requestData["outputformat"]; ok {
		if cerr := parseOutputFormat(&req.OutputFormat, []byte(interface2stringNoDedup(val))); cerr != nil {
			return req, cerr
		}
	}

	// Backends
	var backends []string
	if val, ok := requestData["backends"]; ok {
		backends, err = requestValueList("Backends", val)
		if err != nil {
			return req, err
		}
	}
	req.Backends = backends

	// AuthUser forwarded by a distributing node for a distributed sub-request,
	// so the remote node filters its rows the same way.
	if val, ok := requestData["authuser"]; ok {
		authUser := interface2stringNoDedup(val)
		if err := rejectLineBreaks("AuthUser", authUser); err != nil {
			return req, err
		}
		req.AuthUser = authUser
	}

	return req, nil
}

// parseHTTPFilterRequestData parses and stores the given filter lines.
// maxQuerySize limits the total bytes of filter lines, 0 disables the check.
func parseHTTPFilterRequestData(req *Request, val any, prefix string, maxQuerySize int) (err error) {
	// Get filter lines, e.g., "Filter: col = val", "Or: 2"
	var filterLines []string
	if lines, ok := val.([]any); ok {
		for _, line := range lines {
			lineStr := interface2stringNoDedup(line)
			// a line break would add a new header line to the request
			if err2 := rejectLineBreaks(prefix, lineStr); err2 != nil {
				return err2
			}
			filterLines = append(filterLines, prefix+": "+lineStr+"\n")
		}
	}
	if strVal, ok := val.(string); ok {
		filterLines = strings.Split(strVal, "\n")
	}

	// Parse and store filter
	var size int
	for i := range filterLines {
		filterLine := filterLines[i]
		filterLine = strings.TrimSpace(filterLine)
		if filterLine == "" {
			continue
		}
		size += len(filterLine)
		if maxQuerySize > 0 && size > maxQuerySize {
			return fmt.Errorf("bad request: query size %d exceeds maximum of %d bytes", size, maxQuerySize)
		}
		if err2 := req.ParseRequestHeaderLine([]byte(filterLine), ParseOptimize); err2 != nil {
			return err2
		}
	}

	return err
}

func initializeHTTPRouter(lmd *Daemon) (handler http.Handler) {
	router := httprouter.New()

	// Controller
	controller := &HTTPServerController{
		lmd: lmd,
	}

	// Routes
	router.GET("/", controller.index)
	router.GET("/table/:name", controller.table)
	router.POST("/table/:name", controller.table)
	router.POST("/ping", controller.ping)
	router.POST("/query", controller.query)

	handler = router

	return handler
}
