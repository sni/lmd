package main

import (
	"fmt"
	"net"
	"reflect"
	"testing"
)

func assertEq(exp, got interface{}) error {
	if !reflect.DeepEqual(exp, got) {
		return fmt.Errorf("Wanted %#v; Got %#v", exp, got)
	}
	return nil
}

const (
	TEST_PORT = ":50001"
)

func TestConnection(t *testing.T) {
	InitLogging(&Config{LogLevel: "Off"})

	go func() {
		conn, _ := net.Dial("tcp", TEST_PORT)
		fmt.Fprintf(conn, "GET hosts\nLimit: 10\nOffset: 5\nColumns: name state\n")
		conn.Close()
	}()

	l, _ := net.Listen("tcp", TEST_PORT)
	conn, _ := l.Accept()

	res, _ := ParseRequest(conn)
	if err := assertEq("hosts", res.Table); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(10, res.Limit); err != nil {
		t.Fatal(err)
	}
	if err := assertEq(5, res.Offset); err != nil {
		t.Fatal(err)
	}
	if err := assertEq([]string{"name", "state"}, res.Columns); err != nil {
		t.Fatal(err)
	}
}
