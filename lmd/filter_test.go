package main

import (
	"regexp"
	"testing"
)

func TestStringFilter(t *testing.T) {
	// compare empty strings
	val := ""
	if err := assertEq(true, (&Filter{Operator: Equal, StrValue: ""}).MatchString(&val)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, (&Filter{Operator: Unequal, StrValue: ""}).MatchString(&val)); err != nil {
		t.Error(err)
	}
}

func TestStringListFilter(t *testing.T) {
	value := []string{"abc", "def"}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, StrValue: "def"}).MatchStringList(&value)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, (&Filter{Operator: GreaterThan, StrValue: "xyz"}).MatchStringList(&value)); err != nil {
		t.Error(err)
	}
}

func TestInt64ListFilter(t *testing.T) {
	value := []int64{1, 2, 3, 4, 5}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, FloatValue: 5}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, (&Filter{Operator: GreaterThan, FloatValue: 6}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
}

func TestRegexpStringFilter(t *testing.T) {
	value := "1"
	regex := regexp.MustCompile("[12]")
	if err := assertEq(true, (&Filter{Operator: RegexMatch, Regexp: regex}).MatchString(&value)); err != nil {
		t.Error(err)
	}
	regex = regexp.MustCompile("[02]")
	if err := assertEq(false, (&Filter{Operator: RegexMatch, Regexp: regex}).MatchString(&value)); err != nil {
		t.Error(err)
	}
}

func TestRegexpListFilter(t *testing.T) {
	value := []int64{1, 2, 3, 4, 5}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, FloatValue: 5}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, (&Filter{Operator: GreaterThan, FloatValue: 6}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
}
