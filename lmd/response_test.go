package main

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
)

func init() {
	InitLogging(&Config{LogLevel: "Panic", LogFile: "stderr"})
	InitObjects()
}

func TestRequestHeaderTableFail(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET none\n"))
	_, _, err := ParseRequestFromBuffer(buf)
	if err = assertEq(errors.New("bad request: table none does not exist"), err); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderColumnFail(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nCOlumns: test\n"))
	req, _, err := ParseRequestFromBuffer(buf)
	_, err = BuildResponse(req)
	if err = assertEq(errors.New("bad request: table hosts has no column test"), err); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderSort1Fail(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nCOlumns: state\nSort: name\n"))
	_, _, err := ParseRequestFromBuffer(buf)
	if err = assertEq(errors.New("bad request: invalid sort header, must be Sort: <field> <asc|desc>"), err); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHeaderSort2Fail(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET hosts\nCOlumns: state\nSort: name desc\n"))
	req, _, err := ParseRequestFromBuffer(buf)
	_, err = BuildResponse(req)
	if err = assertEq(errors.New("bad request: sort column name not in result set"), err); err != nil {
		t.Fatal(err)
	}
}
