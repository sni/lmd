package lmd

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringFilter(t *testing.T) {
	// compare empty strings
	val := ""
	assert.True(t, (&Filter{operator: Equal, stringVal: ""}).MatchString(val))
	assert.False(t, (&Filter{operator: Unequal, stringVal: ""}).MatchString(val))
}

func TestStringListFilter(t *testing.T) {
	value := []string{"abc", "def"}
	assert.True(t, (&Filter{operator: GreaterThan, stringVal: "def"}).MatchStringList(value))
	assert.False(t, (&Filter{operator: GreaterThan, stringVal: "xyz"}).MatchStringList(value))
}

func TestStringListNegatedFilter(t *testing.T) {
	value := []string{"abc", "def"}
	assert.False(t, (&Filter{operator: ContainsNot, stringVal: "def"}).MatchStringList(value))
	assert.True(t, (&Filter{operator: GroupContainsNot, stringVal: "xyz"}).MatchStringList(value))

	value = []string{}
	assert.True(t, (&Filter{operator: ContainsNot, stringVal: "def"}).MatchStringList(value))
	assert.True(t, (&Filter{operator: GroupContainsNot, stringVal: "xyz"}).MatchStringList(value))
}

func TestInt64ListFilter(t *testing.T) {
	value := []int64{1, 2, 3, 4, 5}
	assert.True(t, (&Filter{operator: GreaterThan, intValue: 5}).MatchInt64List(value))
	assert.False(t, (&Filter{operator: GreaterThan, intValue: 6}).MatchInt64List(value))
}

func TestRegexpStringFilter(t *testing.T) {
	value := "1"
	regex := regexp.MustCompile("[12]")
	assert.True(t, (&Filter{operator: RegexMatch, regexp: regex}).MatchString(value))
	regex = regexp.MustCompile("[02]")
	assert.False(t, (&Filter{operator: RegexMatch, regexp: regex}).MatchString(value))
}

func TestRegexpListFilter(t *testing.T) {
	value := []int64{1, 2, 3, 4, 5}
	assert.True(t, (&Filter{operator: GreaterThan, intValue: 5}).MatchInt64List(value))
	assert.False(t, (&Filter{operator: GreaterThan, intValue: 6}).MatchInt64List(value))
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
		assert.Equalf(t, exp, res, "regex detection failed for test string '%s'", str)
	}
}

func TestServiceMemberListFilter(t *testing.T) {
	value := []ServiceMember{
		{"host1", "svcA"},
		{"host2", "svcB"},
		{"host3", "svcC"},
	}

	assertions := []struct {
		filter string
		expect bool
	}{
		{"parents =", false},
		{"parents !=", true},
		{"parents >= host1;svcA", true},
		{"parents <= host1;svcA", false},
		{"parents >= hostXY;svcA", false},
		{"parents <= hostXY;svcA", true},
		{"parents >= host", false},
		{"parents <= host", true},
		{"parents ~ ^h.*;^s.*A$", true},
		{"parents ~ ^H.*;^s.*A$", false},
		{"parents ~~ ^H.*;^s.*a$", true},
		{"parents !~ ^H.*;^s.*a$", true},
		{"parents !~~ ^H.*;^s.*a$", false},
	}

	for _, asrt := range assertions {
		stack := &[]*Filter{}
		err := ParseFilter([]byte(asrt.filter), TableServices, stack, ParseOptimize)
		require.NoError(t, err)
		require.Len(t, *stack, 1)

		if asrt.expect {
			assert.Truef(t, (*stack)[0].MatchServiceMemberList(value), "expected filter '%s' to match", asrt.filter)
		} else {
			assert.Falsef(t, (*stack)[0].MatchServiceMemberList(value), "expected filter '%s' to not match", asrt.filter)
		}
	}
}
