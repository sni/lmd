package main

import (
	"fmt"
	"net/http"
    "encoding/json"
    "reflect"

    "github.com/julienschmidt/httprouter"
)

type HTTPRequest struct {
    Table string
    Filter []interface{}
    Offset int
    Limit int
    Sort []interface{}
    Columns []string
    Debug bool
}

type HTTPServerController struct {
}

func (c *HTTPServerController) errorOutput(text string, w http.ResponseWriter) {
    j := make(map[string]interface{})
    j["error"] = text //TODO err.Error() //TODO
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusBadRequest)
    json.NewEncoder(w).Encode(j)
}

func (c *HTTPServerController) index(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
    fmt.Fprintf(w, "LMD %s\n", VERSION)
}

func (c *HTTPServerController) table(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
    w.Header().Set("Content-Type", "application/json")
    
    //Check if table exists
    table_name := ps.ByName("name")
    if _, exists := Objects.Tables[table_name]; !exists {
        c.errorOutput(fmt.Sprintf("table not found: %s", table_name), w)
        return
    }
    
    //TODO passthru requests (?)
    
    //Parse additional query parameters
    decoder := json.NewDecoder(r.Body)
    httpRequest := &HTTPRequest{Table: table_name}
    if err := decoder.Decode(&httpRequest); err == nil {
        //Query parameters provided
    }

    //New request object for specified table
    req := &Request{SendColumnsHeader: false}
    req.Table = table_name
    
    //Offset
    if httpRequest.Offset != 0 {
        req.Offset = httpRequest.Offset
    }
    
    //Limit
    if httpRequest.Limit != 0 {
        req.Limit = httpRequest.Limit
    }
    
    //Filter
    if reflect.TypeOf(httpRequest.Filter).Kind() == reflect.Slice {
        for _, line := range httpRequest.Filter {
            if reflect.TypeOf(line).Kind() == reflect.String {
                value, _ := line.(string)
                err := ParseFilter(value, &value, httpRequest.Table, &req.Filter) //filter.go
                if err != nil {
                    c.errorOutput(err.Error(), w)
                    return
                }
            }
        }
    }
    
    //Sort
    if reflect.TypeOf(httpRequest.Sort).Kind() == reflect.Slice {
        for _, line := range httpRequest.Sort {
            fmt.Printf("checking sort <%s>...\n", line)
            if reflect.TypeOf(line).Kind() == reflect.String {
                value, _ := line.(string)
                err := parseSortHeader(&req.Sort, value) //request.go
                if err != nil {
                    c.errorOutput(err.Error(), w)
                    return
                }
            }
        }
    }
    
    //Columns
    if len(httpRequest.Columns) > 0 {
        req.Columns = httpRequest.Columns
    }

    //Fetch backend data
    req.ExpandRequestedBackends() //ParseRequests()
    
    //Debug output //TODO
    if httpRequest.Debug {
        if j, _ := json.Marshal(req); true {
            fmt.Fprintf(w, "%s", j) //TODO debugging only
            return
        }
    }
    
    //Ask request object to send query, get response
    res, err := req.GetResponse()
    if err != nil {
        c.errorOutput(err.Error(), w)
        return
    }
    
    //Send JSON
    j, err := res.JSON()
    if err != nil {
        c.errorOutput(err.Error(), w)
        return
    }
    fmt.Fprintf(w, "%s", j)

}

func initializeHTTPServer() (handler http.Handler, err error) {
    router := httprouter.New()
    
    //Controller
    controller := &HTTPServerController{}

    //Routes
    router.GET("/v1", controller.index)
    router.GET("/v1/table/:name", controller.table)
    router.POST("/v1/table/:name", controller.table)



    handler = router
    return
}


