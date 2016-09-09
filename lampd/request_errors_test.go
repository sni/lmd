package main

import "testing"

type ErrorRequest struct {
	Request string
	Error   string
}

func TestResponseErrorsFunc(t *testing.T) {
	peer := SetupTestPeer()

	testRequestStrings := []ErrorRequest{
		ErrorRequest{"", "bad request: empty request"},
		ErrorRequest{"NOE", "bad request: NOE"},
		ErrorRequest{"GET none\nColumns: none", "bad request: table none does not exist"},
		ErrorRequest{"GET backends\nColumns: status none", "bad request: table backends has no column none"},
		ErrorRequest{"GET hosts\nColumns: name\nFilter: none = 1", "bad request: unrecognized column from filter: none in Filter: none = 1"},
		ErrorRequest{"GET hosts\nBackends: none", "bad request: backend none does not exist"},
		ErrorRequest{"GET hosts\nnone", "bad request header: none"},
		ErrorRequest{"GET hosts\nNone: blah", "bad request: unrecognized header None: blah"},
		ErrorRequest{"GET hosts\nLimit: x", "bad request: limit must be a positive number"},
		ErrorRequest{"GET hosts\nLimit: -1", "bad request: limit must be a positive number"},
		ErrorRequest{"GET hosts\nOffset: x", "bad request: offset must be a positive number"},
		ErrorRequest{"GET hosts\nOffset: -1", "bad request: offset must be a positive number"},
		ErrorRequest{"GET hosts\nSort: 1", "bad request: invalid sort header, must be Sort: <field> <asc|desc>"},
		ErrorRequest{"GET hosts\nSort: name none", "bad request: unrecognized sort direction, must be asc or desc"},
		ErrorRequest{"GET hosts\nSort: name", "bad request: invalid sort header, must be Sort: <field> <asc|desc>"},
		ErrorRequest{"GET hosts\nColumns: name\nSort: state asc", "bad request: sort column state not in result set"},
		ErrorRequest{"GET hosts\nResponseheader: none", "bad request: unrecognized responseformat, only fixed16 is supported"},
		ErrorRequest{"GET hosts\nOutputFormat: csv: none", "bad request: unrecognized outputformat, only json and wrapped_json is supported"},
	}

	for _, er := range testRequestStrings {
		_, err := peer.QueryString(er.Request)
		if err = assertEq(er.Error, err.Error()); err != nil {
			t.Error("Request: " + er.Request)
			t.Error(err)
		}
	}

	StopTestPeer()
}
