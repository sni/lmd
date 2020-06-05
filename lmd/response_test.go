package main

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
)

func TestRequestHeaderTableFail(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString("GET none\n"))
	_, _, err := NewRequest(buf, ParseOptimize)
	if err = assertEq(errors.New("bad request: table none does not exist"), err); err != nil {
		t.Fatal(err)
	}
}
