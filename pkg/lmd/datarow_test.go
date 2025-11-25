package lmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInterface2HashMap1(t *testing.T) {
	in := map[string]any{"key": 5}
	exp := map[string]string{"key": "5"}
	got := interface2hashmap(in)
	assert.Equal(t, exp, got)
}

func TestInterface2HashMap2(t *testing.T) {
	in := map[string]any{"key": nil}
	exp := map[string]string{"key": ""}
	got := interface2hashmap(in)
	assert.Equal(t, exp, got)
}

func TestInterface2HashMap3(t *testing.T) {
	in := []any{[]any{"key", ""}}
	exp := map[string]string{"key": ""}
	got := interface2hashmap(in)
	assert.Equal(t, exp, got)
}

func TestInterface2Stringlist1(t *testing.T) {
	in := ""
	exp := []string{}
	got := interface2stringList(in, false)
	assert.Equal(t, exp, got)
}

func TestInterface2Stringlist2(t *testing.T) {
	in := "test"
	exp := []string{"test"}
	got := interface2stringList(&in, false)
	assert.Equal(t, exp, got)
}
