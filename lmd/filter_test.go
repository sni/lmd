package main

import (
	"regexp"
	"testing"
)

func TestStringFilter(t *testing.T) {
	// compare empty strings
	val := ""
	if err := assertEq(true, (&Filter{Operator: Equal, StrValue: ""}).MatchString(val)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, (&Filter{Operator: Unequal, StrValue: ""}).MatchString(val)); err != nil {
		t.Error(err)
	}
}

func TestStringListFilter(t *testing.T) {
	value := []string{"abc", "def"}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, StrValue: "def"}).MatchStringList(value)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, (&Filter{Operator: GreaterThan, StrValue: "xyz"}).MatchStringList(value)); err != nil {
		t.Error(err)
	}
}

func TestStringListNegatedFilter(t *testing.T) {
	value := []string{"abc", "def"}
	if err := assertEq(false, (&Filter{Operator: ContainsNot, StrValue: "def"}).MatchStringList(value)); err != nil {
		t.Error(err)
	}
	if err := assertEq(true, (&Filter{Operator: GroupContainsNot, StrValue: "xyz"}).MatchStringList(value)); err != nil {
		t.Error(err)
	}

	value = []string{}
	if err := assertEq(true, (&Filter{Operator: ContainsNot, StrValue: "def"}).MatchStringList(value)); err != nil {
		t.Error(err)
	}
	if err := assertEq(true, (&Filter{Operator: GroupContainsNot, StrValue: "xyz"}).MatchStringList(value)); err != nil {
		t.Error(err)
	}
}

func TestInt64ListFilter(t *testing.T) {
	value := []int64{1, 2, 3, 4, 5}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, IntValue: 5}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, (&Filter{Operator: GreaterThan, IntValue: 6}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
}

func TestRegexpStringFilter(t *testing.T) {
	value := "1"
	regex := regexp.MustCompile("[12]")
	if err := assertEq(true, (&Filter{Operator: RegexMatch, Regexp: regex}).MatchString(value)); err != nil {
		t.Error(err)
	}
	regex = regexp.MustCompile("[02]")
	if err := assertEq(false, (&Filter{Operator: RegexMatch, Regexp: regex}).MatchString(value)); err != nil {
		t.Error(err)
	}
}

func TestRegexpListFilter(t *testing.T) {
	value := []int64{1, 2, 3, 4, 5}
	if err := assertEq(true, (&Filter{Operator: GreaterThan, IntValue: 5}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
	if err := assertEq(false, (&Filter{Operator: GreaterThan, IntValue: 6}).MatchInt64List(value)); err != nil {
		t.Error(err)
	}
}

func TestRegexpDetection(t *testing.T) {
	tests := map[string]bool{
		"":           false,
		"test":       false,
		"test.local": false,
		"test..de":   true,
		"test5.de":   false,
		"test.5de":   true, // domains/hostnames do not start with a number
		"srv..01":    true,
		"srv.{2}01":  true,
		"BAR .":      true,
		"t.t":        true,
		"test.":      true,
		"[a-z]":      true,
		".*":         true,
		`\d`:         true,
		"[0-9]+":     true,
		"test$":      true,
	}
	for str, exp := range tests {
		res := hasRegexpCharacters(str)
		if err := assertEq(exp, res); err != nil {
			t.Errorf("regex detection failed for test string '%s'\n%s", str, err)
		}
	}
}
