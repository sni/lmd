package main

import (
	"encoding/json"
	"fmt"
	"net/http"

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
	json.NewEncoder(w).Encode(j)
}

func (c *HTTPServerController) index(w http.ResponseWriter, request *http.Request, ps httprouter.Params) {
	fmt.Fprintf(w, "LMD %s\n", VERSION)
}

func (c *HTTPServerController) queryTable(w http.ResponseWriter, requestData map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")

	// Requested table (name)
	tableName := requestData["table"].(string)

	// Check if table exists
	if _, exists := Objects.Tables[tableName]; !exists {
		c.errorOutput(fmt.Errorf("table not found: %s", tableName), w)
		return
	}

	// New request object for specified table
	req := &Request{}
	req.Table = tableName

	// Send header row by default
	req.SendColumnsHeader = true
	if val, ok := requestData["sendcolumnsheader"]; ok {
		if enable, ok := val.(bool); ok {
			req.SendColumnsHeader = enable
		}
	}

	// Offset
	if val, ok := requestData["offset"]; ok {
		req.Offset = int(val.(float64))
	}

	// Limit
	if val, ok := requestData["limit"]; ok {
		req.Limit = int(val.(float64))
	}

	// Filter
	var requestDataFilter []interface{}
	if val, ok := requestData["filter"]; ok {
		lines, ok := val.([]interface{})
		if ok {
			requestDataFilter = lines
		}
	}
	for _, line := range requestDataFilter {
		value := line.(string)
		err := ParseFilter(value, &value, tableName, &req.Filter) // filter.go
		if err != nil {
			c.errorOutput(err, w)
			return
		}
	}

	// Stats
	var requestDataStats []interface{}
	if val, ok := requestData["stats"]; ok {
		lines, ok := val.([]interface{})
		if ok {
			requestDataStats = lines
		}
	}
	for _, line := range requestDataStats {
		value := line.(string)
		err := ParseStats(value, &value, tableName, &req.Stats) // filter.go
		if err != nil {
			c.errorOutput(err, w)
			return
		}
	}
	if val, ok := requestData["sendstatsdata"]; ok {
		if enable, ok := val.(bool); ok {
			req.SendStatsData = enable
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
		value := line.(string)
		err := parseSortHeader(&req.Sort, value) // request.go
		if err != nil {
			c.errorOutput(err, w)
			return
		}
	}

	// Columns
	var columns []string
	if val, ok := requestData["columns"]; ok {
		for _, column := range val.([]interface{}) {
			columns = append(columns, column.(string))
		}
	}
	req.Columns = columns

	// Format
	if val, ok := requestData["outputformat"]; ok {
		if str, ok := val.(string); ok {
			err := parseOutputFormat(&req.OutputFormat, str)
			if err != nil {
				c.errorOutput(err, w)
				return
			}
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

	// Fetch backend data
	req.ExpandRequestedBackends() // ParseRequests()

	// Ask request object to send query, get response
	res, err := req.GetResponse()
	if err != nil {
		c.errorOutput(err, w)
		return
	}

	// Send JSON
	j, err := res.JSON()
	if err != nil {
		c.errorOutput(err, w)
		return
	}
	fmt.Fprintf(w, "%s", j)

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
	tableName := ps.ByName("name")
	if tableName != "" {
		requestData["table"] = tableName
	}

	c.queryTable(w, requestData)
}

func (c *HTTPServerController) ping(w http.ResponseWriter, request *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	// Response data
	id := nodeAccessor.ID
	j := make(map[string]interface{})
	j["identifier"] = id

	// Send data
	json.NewEncoder(w).Encode(j)
}

func (c *HTTPServerController) query(w http.ResponseWriter, request *http.Request, ps httprouter.Params) {
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
	requestedFunction := requestData["_name"].(string)

	switch requestedFunction {
	case "ping":
		c.ping(w, request, ps)
	case "table":
		c.queryTable(w, requestData)
	default:
		c.errorOutput(fmt.Errorf("unknown request: %s", requestedFunction), w)
	}
}

func initializeHTTPRouter() (handler http.Handler, err error) {
	router := httprouter.New()

	// Controller
	controller := &HTTPServerController{}

	// Routes
	router.GET("/v1", controller.index)
	router.GET("/v1/table/:name", controller.table)
	router.POST("/v1/table/:name", controller.table)
	router.POST("/v1/ping", controller.ping)
	router.POST("/v1/query", controller.query)

	handler = router
	return
}
