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

func TestStringListFilter(t *testing.T) {

	var value interface{}
	assertEq("", value) // make gosimple happy

	value = []string{"abc", "def"}
	if err := assertEq(true, matchStringListFilter(&Filter{Operator: GreaterThan, StrValue: "def"}, &value)); err != nil {
		t.Error(err)
	}

	value = []float64{0}
	if err := assertEq(false, matchStringListFilter(&Filter{Operator: GreaterThan, StrValue: ""}, &value)); err != nil {
		t.Error(err)
	}

	// null json value should not panic
	value = nil
	if err := assertEq(false, matchStringListFilter(&Filter{Operator: GreaterThan, StrValue: ""}, &value)); err != nil {
		t.Error(err)
	}
}

func TestIntListFilter(t *testing.T) {

	var value interface{}
	assertEq("", value) // make gosimple happy

	value = []string{"abc", "def"}
	if err := assertEq(false, matchIntListFilter(&Filter{Operator: GreaterThan, FloatValue: 1}, &value)); err != nil {
		t.Error(err)
	}

	value = []float64{1, 2, 3, 4, 5}
	if err := assertEq(true, matchIntListFilter(&Filter{Operator: GreaterThan, FloatValue: 5}, &value)); err != nil {
		t.Error(err)
	}

	// null json value should not panic
	value = nil
	if err := assertEq(false, matchIntListFilter(&Filter{Operator: GreaterThan, FloatValue: 1}, &value)); err != nil {
		t.Error(err)
	}
}
