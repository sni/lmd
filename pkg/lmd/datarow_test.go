package lmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDictColumnsSources(t *testing.T) {
	for _, table := range Objects.Tables {
		for _, col := range table.columns {
			if col.DataType != CustomVarCol || col.StorageType != VirtualStore {
				continue
			}
			src, ok := dictColumns[col.Name]
			assert.Truef(t, ok, "%s.%s missing in dictColumns", table.name.String(), col.Name)
			assert.NotNilf(t, table.GetColumn(src[0]), "%s.%s missing", table.name.String(), src[0])
			assert.NotNilf(t, table.GetColumn(src[1]), "%s.%s missing", table.name.String(), src[1])
		}
	}
}

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
