package main

import (
	"testing"
)

func TestStringFilter(t *testing.T) {

	var valueA interface{}
	var valueB string

	// compare empty strings
	valueA = ""
	valueB = ""
	if err := assertEq(true, matchStringValueOperator(Equal, &valueA, &valueB, nil)); err != nil {
		t.Error(err)
	}

	// null json value should match empty string
	valueA = nil
	valueB = ""
	if err := assertEq(true, matchStringValueOperator(Equal, &valueA, &valueB, nil)); err != nil {
		t.Error(err)
	}
}
