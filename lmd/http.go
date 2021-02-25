package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
)

// HTTPServerController is the container object for the rest interface's server.
type HTTPServerController struct {
}

func (c *HTTPServerController) errorOutput(err error, w http.ResponseWriter) {
	j := make(map[string]interface{})
	j["error"] = err.Error()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	err = json.NewEncoder(w).Encode(j)
	if err != nil {
		log.Debugf("encoder failed: %e", err)
	}
}

func (c *HTTPServerController) index(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "LMD %s\n", VERSION)
}

func (c *HTTPServerController) queryTable(w http.ResponseWriter, requestData map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")

	// Requested table (name)
	_, err := NewTableName(interface2stringNoDedup(requestData["table"]))

	// Check if table exists
	if err != nil {
		c.errorOutput(err, w)
		return
	}

	req, err := parseRequestDataToRequest(requestData)
	if err != nil {
		c.errorOutput(err, w)
		return
	}

	// Fetch backend data
	err = req.ExpandRequestedBackends()
	if err != nil {
		c.errorOutput(err, w)
		return
	}

	var res *Response
	if d, exists := requestData["distributed"]; exists && d.(bool) {
		// force local answer to avoid recursion
		res, err = NewResponse(req)
	} else {
		// Ask request object to send query, get response, might get distributed
		res, err = req.GetResponse()
	}
	if err != nil {
		c.errorOutput(err, w)
		return
	}

	// Send JSON
	buf, err := res.Buffer()
	if err != nil {
		c.errorOutput(err, w)
		return
	}
	_, err = buf.WriteTo(w)
	if err != nil {
		log.Debugf("writeto failed: %e", err)
	}
}

func (c *HTTPServerController) table(w http.ResponseWriter, request *http.Request, ps httprouter.Params) {
	// Read request data
	requestData := make(map[string]interface{})
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&requestData); err != nil {
		c.errorOutput(fmt.Errorf("request not understood"), w)
		return
	}

	// Use table name defined in rest request
	if tableName := ps.ByName("name"); tableName != "" {
		requestData["table"] = tableName
	}

	c.queryTable(w, requestData)
}

func (c *HTTPServerController) ping(w http.ResponseWriter, request *http.Request, _ httprouter.Params) {
	// Read request data
	requestData := make(map[string]interface{})
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&requestData); err != nil {
		c.errorOutput(fmt.Errorf("request not understood"), w)
		return
	}
	c.queryPing(w, requestData)
}

func (c *HTTPServerController) queryPing(w http.ResponseWriter, _ map[string]interface{}) {
	// Response data
	w.Header().Set("Content-Type", "application/json")
	id := nodeAccessor.ID
	j := make(map[string]interface{})
	j["identifier"] = id
	j["peers"] = nodeAccessor.assignedBackends
	j["version"] = Version()

	// Send data
	err := json.NewEncoder(w).Encode(j)
	if err != nil {
		log.Debugf("sending ping result failed: %e", err)
	}
}

func (c *HTTPServerController) query(w http.ResponseWriter, request *http.Request, _ httprouter.Params) {
	// Read request data
	contentType := request.Header.Get("Content-Type")
	requestData := make(map[string]interface{})
	defer request.Body.Close()
	if contentType == "application/json" {
		decoder := json.NewDecoder(request.Body)
		err := decoder.Decode(&requestData)
		if err != nil {
			c.errorOutput(fmt.Errorf("request not understood"), w)
			return
		}
	}

	// Request type (requested api function)
	requestedFunction, _ := requestData["_name"].(string)

	switch requestedFunction {
	case "ping":
		c.queryPing(w, requestData)
	case "table":
		c.queryTable(w, requestData)
	default:
		c.errorOutput(fmt.Errorf("unknown request: %s", requestedFunction), w)
	}
}

func parseRequestDataToRequest(requestData map[string]interface{}) (req *Request, err error) {
	// New request object for specified table
	req = &Request{}
	table, err := NewTableName(interface2stringNoDedup(requestData["table"]))
	if err != nil {
		return
	}
	req.Table = table

	// Send header row by default
	req.ColumnsHeaders = true
	if val, ok := requestData["sendcolumnsheader"]; ok {
		req.ColumnsHeaders = val.(bool)
	}

	// Offset
	req.Offset = interface2int(requestData["offset"])

	// Limit
	if val, ok := requestData["limit"]; ok {
		req.Limit = new(int)
		*req.Limit = int(val.(float64))
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
		err := parseSortHeader(&req.Sort, []byte(line.(string))) // request.go
		if err != nil {
			return req, err
		}
	}

	// Columns
	var columns []string
	if val, ok := requestData["columns"]; ok {
		for _, column := range val.([]interface{}) {
			name := column.(string)
			if name != "empty" {
				columns = append(columns, name)
			}
		}
	}
	req.Columns = columns

	// Format
	if val, ok := requestData["outputformat"]; ok {
		err := parseOutputFormat(&req.OutputFormat, []byte(val.(string)))
		if err != nil {
			return req, err
		}
	}

	// Backends
	var backends []string
	if val, ok := requestData["backends"]; ok {
		for _, backend := range val.([]interface{}) {
			backends = append(backends, backend.(string))
		}
	}
	req.Backends = backends
	return
}

func parseHTTPFilterRequestData(req *Request, val interface{}, prefix string) (err error) {
	// Get filter lines, e.g., "Filter: col = val", "Or: 2"
	var filterLines []string
	if lines, ok := val.([]interface{}); ok {
		for _, line := range lines {
			filterLine := prefix + ": " + line.(string) + "\n"
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

func initializeHTTPRouter() (handler http.Handler) {
	router := httprouter.New()

	// Controller
	controller := &HTTPServerController{}

	// Routes
	router.GET("/", controller.index)
	router.GET("/table/:name", controller.table)
	router.POST("/table/:name", controller.table)
	router.POST("/ping", controller.ping)
	router.POST("/query", controller.query)

	handler = router
	return
}
