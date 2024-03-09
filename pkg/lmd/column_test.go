package lmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnFlag(t *testing.T) {
	lmd := createTestLMDInstance()
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/"}}
	peer := NewPeer(lmd, &connection)

	assert.Equal(t, uint32(NoFlags), peer.Flags)

	peer.SetFlag(Naemon)
	assert.Equal(t, uint32(Naemon), peer.Flags)
	assert.True(t, peer.HasFlag(Naemon))
	assert.False(t, peer.HasFlag(HasDependencyColumn))

	peer.SetFlag(HasDependencyColumn)
	assert.True(t, peer.HasFlag(HasDependencyColumn))
	assert.False(t, peer.HasFlag(MultiBackend))

	peer.ResetFlags()
	assert.False(t, peer.HasFlag(Naemon))
	assert.Equal(t, uint32(NoFlags), peer.Flags)
}

func TestColumnList(t *testing.T) {
	cl := ColumnList{&Column{Name: "Test1"}, &Column{Name: "Test2"}}
	assert.Equal(t, "Test1, Test2", cl.String())
}

func TestColumnEmpty(t *testing.T) {
	col := Column{Name: "Test1", DataType: IntCol}
	assert.Equal(t, -1, col.GetEmptyValue())

	col.DataType = StringCol
	assert.Empty(t, col.GetEmptyValue())
}
