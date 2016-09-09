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
		ErrorRequest{"GET backends\nColumns: status none", "bad request: table backends has no column none"},
	}

	for _, er := range testRequestStrings {
		_, err := peer.QueryString(er.Request)
		if err = assertEq(er.Error, err.Error()); err != nil {
			t.Fatal(err)
		}
	}

	StopTestPeer()
}
