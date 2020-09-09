package main

import (
	"bufio"
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestPeerSource(t *testing.T) {
	waitGroup := &sync.WaitGroup{}
	shutdownChannel := make(chan bool)
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/", "http://clusternode/test"}}
	peer := NewPeer(&Config{}, &connection, waitGroup, shutdownChannel)

	if err := assertEq("http://localhost/test/", peer.Source[0]); err != nil {
		t.Error(err)
	}

	if err := assertEq("http://clusternode/test", peer.Source[1]); err != nil {
		t.Error(err)
	}
}

func TestPeerHTTPComplete(t *testing.T) {
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/thruk/cgi-bin/remote.cgi")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename")); err != nil {
		t.Error(err)
	}
	if err := assertEq("http://localhost/sitename/thruk/cgi-bin/remote.cgi", completePeerHTTPAddr("http://localhost/sitename/")); err != nil {
		t.Error(err)
	}
}

func TestParseResultJSON(t *testing.T) {
	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1,2], {"a": 1}],
	]`)

	res, _, err := req.parseResult(&data)

	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(2, len(*res)); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(5, len((*res)[0])); err != nil {
		t.Error(err)
	}
	if err := assertEq("host2", (*res)[1][0]); err != nil {
		t.Error(err)
	}
	if err := assertEq(float64(1), (*res)[1][2]); err != nil {
		t.Error(err)
	}
}

func TestParseResultWrappedJSON(t *testing.T) {
	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: wrapped_json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`{"data": [
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1,2], {"a": 1}],
	],
	"total_count": 2}`)

	res, meta, err := req.parseResult(&data)

	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(2, len(*res)); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(5, len((*res)[0])); err != nil {
		t.Error(err)
	}
	if err := assertEq("host2", (*res)[1][0]); err != nil {
		t.Error(err)
	}
	if err := assertEq(float64(1), (*res)[1][2]); err != nil {
		t.Error(err)
	}
	if err := assertEq(int64(2), meta.Total); err != nil {
		t.Error(err)
	}
}

func TestParseResultJSONBroken(t *testing.T) {
	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(&data)
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	if err == nil {
		t.Errorf("got no error from broken json")
	}

	if res != nil {
		t.Errorf("got result for broken json")
	}
}

func TestParseResultJSONBroken2(t *testing.T) {
	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name description state list hash\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	data := []byte(`[
	 ["host1", "desc1", 0, [1,2], {"a": 1}],
	 ["host2", "desc2", 1, [1,2], {"a" 1}],
	]`)

	InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
	res, _, err := req.parseResult(&data)
	InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

	if err == nil {
		t.Errorf("got no error from broken json")
	}

	if res != nil {
		t.Errorf("got result for broken json")
	}
}

func TestParseResultJSONEscapeSequences(t *testing.T) {
	req, _, err := NewRequest(bufio.NewReader(bytes.NewBufferString("GET services\nColumns: host_name\nOutputFormat: json\n")), ParseOptimize)
	if err != nil {
		panic(err.Error())
	}
	for _, s := range []string{"\x00", "\x01", "\x02", "\x02", "\x06", "a\xc5z"} {
		data := []byte(fmt.Sprintf("[[\"null%s\"]]", s))

		InitLogging(&Config{LogLevel: "off", LogFile: "stderr"})
		res, _, err := req.parseResult(&data)
		InitLogging(&Config{LogLevel: testLogLevel, LogFile: "stderr"})

		if err != nil {
			t.Fatal(err)
		}
		if err := assertEq(1, len(*res)); err != nil {
			t.Error(err)
		}
		if err := assertLike("null", (*res)[0][0].(string)); err != nil {
			t.Error(err)
		}
	}
}

func TestPeerUpdate(t *testing.T) {
	peer := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	err := peer.data.UpdateFull(Objects.UpdateTables)
	if err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

func TestPeerDeltaUpdate(t *testing.T) {
	peer := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	err := peer.data.UpdateDelta(0, 0)
	if err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}
