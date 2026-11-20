package lmd

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPRouter(t *testing.T) {
	peer, cleanup, lmd := StartTestPeer(t, 3, 10, 10)
	PauseTestPeers(peer)

	baseURL := "http://localhost:9999"

	lmd.waitGroupInit.Add(1)
	listener := NewListener(lmd, baseURL)
	lmd.waitGroupInit.Wait()

	// index page
	resp, err := http.Get(baseURL + "/")
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body, _, err := parseResponseUndefinedSize(resp.Body, nil)
	require.NoError(t, err)
	assert.Contains(t, string(body), "LMD "+VERSION)
	resp.Body.Close()

	// ping page
	resp, err = http.Post(baseURL+"/ping", "application/json", nil)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body, _, err = parseResponseUndefinedSize(resp.Body, nil)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"identifier":`)
	assert.Contains(t, string(body), `"version":"`+VERSION)
	resp.Body.Close()

	// ping via query page
	resp, err = http.Post(baseURL+"/query", "application/json", strings.NewReader(`{"_name":"ping"}`))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body, _, err = parseResponseUndefinedSize(resp.Body, nil)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"version":"`+VERSION)
	resp.Body.Close()

	// unknown via query page
	resp, err = http.Post(baseURL+"/query", "application/json", strings.NewReader(`{"_name":"unknown"}`))
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
	body, _, err = parseResponseUndefinedSize(resp.Body, nil)
	require.NoError(t, err)
	assert.Contains(t, string(body), `unknown request:`)
	resp.Body.Close()

	// query page
	resp, err = http.Post(baseURL+"/query", "application/json", strings.NewReader(`{
		"_name":"table",
		 "table":"hosts",
		 "columns":["name", "peer_name", "status"],
		 "outputformat":"wrapped_json",
		 "filter":"Filter: name ~~ ^testhost",
		 "sort":"Sort: name asc",
		 "limit": 3
	}`))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body, _, err = parseResponseUndefinedSize(resp.Body, nil)
	require.NoError(t, err)
	assert.Contains(t, string(body), `testhost_`)
	resp.Body.Close()

	// table page (POST)
	resp, err = http.Post(baseURL+"/table/sites", "application/json", strings.NewReader(`{
		 "columns":["peer_name", "status"],
		 "outputformat":"wrapped_json",
		 "limit": 3
	}`))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body, _, err = parseResponseUndefinedSize(resp.Body, nil)
	require.NoError(t, err)
	assert.Contains(t, string(body), `MockCon-mock1`)
	resp.Body.Close()

	// table page (GET)
	resp, err = http.Get(baseURL + "/table/sites")
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	body, _, err = parseResponseUndefinedSize(resp.Body, nil)
	require.NoError(t, err)
	assert.Contains(t, string(body), `MockCon-mock1`)
	resp.Body.Close()

	// cleanup again
	err = listener.Connection.Close()
	require.NoError(t, err)

	// run cleanup
	err = cleanup()
	require.NoError(t, err)
}

// a value coming from the HTTP table API may contain line breaks which would
// end the current header line and append further headers or a whole COMMAND
// request to the backend query. Make sure such requests fail loudly.
func TestHTTPRequestValuesDoNotBreakHeaderLines(t *testing.T) {
	tests := []struct {
		field string
		value string
	}{
		{"filter", "name = alpha\nKeepAlive: on\n\nCOMMAND [1700000000] DISABLE_NOTIFICATIONS"},
		{"stats", "name = alpha\nKeepAlive: on\n\nCOMMAND [1700000000] DISABLE_NOTIFICATIONS"},
		{"columns", "name\nKeepAlive: on\n\nCOMMAND [1700000000] DISABLE_NOTIFICATIONS"},
		{"backends", "mock0\nKeepAlive: on\n\nCOMMAND [1700000000] DISABLE_NOTIFICATIONS"},
		{"sort", "name\nKeepAlive:on\n\nCOMMAND"},
		{"authuser", "admin\nKeepAlive: on\n\nCOMMAND"},
	}
	for _, test := range tests {
		t.Run(test.field, func(t *testing.T) {
			requestData := map[string]any{
				"table": "hosts",
			}
			if test.field == "authuser" {
				// authuser is a plain string, not a list
				requestData[test.field] = test.value
			} else {
				requestData[test.field] = []any{test.value}
			}
			_, err := parseRequestDataToRequest(requestData)
			assert.Error(t, err, "%s value must be rejected", test.field)
		})
	}
}
