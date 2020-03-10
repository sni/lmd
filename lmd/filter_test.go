package main

import (
	"testing"
)

func TestStringFilter(t *testing.T) {
	// compare empty strings
	val := ""
	if err := assertEq(true, (&Filter{Operator: Equal, StrValue: ""}).MatchString(&val)); err != nil {
		t.Error(err)
	}
}

func TestStringListFilter(t *testing.T) {
	value := []string{"abc", "def"}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, StrValue: "def"}).MatchStringList(&value)); err != nil {
		t.Error(err)
	}
}

func TestInt64ListFilter(t *testing.T) {
	value := []int64{1, 2, 3, 4, 5}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, FloatValue: 5}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
}

func TestRegexpListFilter(t *testing.T) {
	value := []int64{1, 2, 3, 4, 5}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, FloatValue: 5}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
}
