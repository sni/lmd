package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

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
	table_name := requestData["table"].(string)

	// Check if table exists
	if _, exists := Objects.Tables[table_name]; !exists {
		c.errorOutput(fmt.Errorf("table not found: %s", table_name), w)
		return
	}

	// New request object for specified table
	req := &Request{SendColumnsHeader: false}
	req.Table = table_name

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
	if val, ok := requestData["filterstr"]; ok {
		requestDataFilter = append(requestDataFilter, val.(string))
	}
	for _, line := range requestDataFilter {
		value := line.(string)
		err := ParseFilter(value, &value, table_name, &req.Filter) // filter.go
		if err != nil {
			c.errorOutput(err, w)
			return
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
	table_name := ps.ByName("name")
	if table_name != "" {
		requestData["table"] = table_name
	}

	c.queryTable(w, requestData)
}

func (c *HTTPServerController) ping(w http.ResponseWriter, request *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	// Response data
	id := NodeAccessor.ID
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
