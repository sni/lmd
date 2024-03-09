package lmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
)

// HTTPServerController is the container object for the rest interface's server.
type HTTPServerController struct {
	lmd *Daemon
}

func (c *HTTPServerController) errorOutput(err error, wrt http.ResponseWriter) {
	j := make(map[string]interface{})
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

func (c *HTTPServerController) queryTable(ctx context.Context, wrt http.ResponseWriter, requestData map[string]interface{}) {
	wrt.Header().Set("Content-Type", "application/json")

	// Requested table (name)
	_, err := NewTableName(interface2stringNoDedup(requestData["table"]))
	// Check if table exists
	if err != nil {
		c.errorOutput(err, wrt)

		return
	}

	req, err := parseRequestDataToRequest(requestData)
	if err != nil {
		c.errorOutput(err, wrt)

		return
	}

	// Fetch backend data
	err = req.ExpandRequestedBackends()
	if err != nil {
		c.errorOutput(err, wrt)

		return
	}

	var res *Response
	if d, exists := requestData["distributed"]; exists && interface2bool(d) {
		// force local answer to avoid recursion
		res, _, err = NewResponse(ctx, req, nil)
	} else {
		// Ask request object to send query, get response, might get distributed
		res, err = req.BuildResponse(ctx)
	}
	if err != nil {
		c.errorOutput(err, wrt)

		return
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
	requestData := make(map[string]interface{})
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&requestData); err != nil {
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
	requestData := make(map[string]interface{})
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&requestData); err != nil {
		c.errorOutput(fmt.Errorf("request not understood"), wrt)

		return
	}
	c.queryPing(wrt, requestData)
}

func (c *HTTPServerController) queryPing(wrt http.ResponseWriter, _ map[string]interface{}) {
	// Response data
	wrt.Header().Set("Content-Type", "application/json")
	id := c.lmd.nodeAccessor.ID
	jsonData := make(map[string]interface{})
	jsonData["identifier"] = id
	jsonData["peers"] = c.lmd.nodeAccessor.assignedBackends
	jsonData["version"] = Version()

	// Send data
	err := json.NewEncoder(wrt).Encode(jsonData)
	if err != nil {
		log.Debugf("sending ping result failed: %e", err)
	}
}

func (c *HTTPServerController) query(wrt http.ResponseWriter, request *http.Request, _ httprouter.Params) {
	// Read request data
	contentType := request.Header.Get("Content-Type")
	requestData := make(map[string]interface{})
	defer request.Body.Close()
	if contentType == "application/json" {
		decoder := json.NewDecoder(request.Body)
		err := decoder.Decode(&requestData)
		if err != nil {
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

func parseRequestDataToRequest(requestData map[string]interface{}) (req *Request, err error) {
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
		err = parseHTTPFilterRequestData(req, val, "Filter")
		if err != nil {
			return req, err
		}
	}

	// Stats String in livestatus syntax
	if val, ok := requestData["stats"]; ok {
		err = parseHTTPFilterRequestData(req, val, "Stats")
		if err != nil {
			return req, err
		}
		if len(req.Stats) > 0 {
			req.SendStatsData = true
		}
	}

	// Sort
	var requestDataSort []interface{}
	if val, ok := requestData["sort"]; ok {
		lines, ok := val.([]interface{})
		if ok {
			requestDataSort = lines
		}
	}
	for _, line := range requestDataSort {
		err := parseSortHeader(&req.Sort, []byte(interface2stringNoDedup(line)))
		if err != nil {
			return req, err
		}
	}

	// Columns
	var columns []string
	if val, ok := requestData["columns"]; ok {
		for _, column := range interface2interfacelist(val) {
			name := interface2stringNoDedup(column)
			if name != "empty" {
				columns = append(columns, name)
			}
		}
	}
	req.Columns = columns

	// Format
	if val, ok := requestData["outputformat"]; ok {
		err := parseOutputFormat(&req.OutputFormat, []byte(interface2stringNoDedup(val)))
		if err != nil {
			return req, err
		}
	}

	// Backends
	var backends []string
	if val, ok := requestData["backends"]; ok {
		for _, backend := range interface2interfacelist(val) {
			backends = append(backends, interface2stringNoDedup(backend))
		}
	}
	req.Backends = backends

	return req, nil
}

func parseHTTPFilterRequestData(req *Request, val interface{}, prefix string) (err error) {
	// Get filter lines, e.g., "Filter: col = val", "Or: 2"
	var filterLines []string
	if lines, ok := val.([]interface{}); ok {
		for _, line := range lines {
			filterLine := prefix + ": " + interface2stringNoDedup(line) + "\n"
			filterLines = append(filterLines, filterLine)
		}
	}
	if strVal, ok := val.(string); ok {
		filterLines = strings.Split(strVal, "\n")
	}

	// Parse and store filter
	for i := range filterLines {
		filterLine := filterLines[i]
		filterLine = strings.TrimSpace(filterLine)
		if filterLine == "" {
			continue
		}
		if err := req.ParseRequestHeaderLine([]byte(filterLine), ParseOptimize); err != nil {
			return err
		}
	}

	return
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

	return
}
