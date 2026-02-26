package lmd

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnFlag(t *testing.T) {
	lmd := createTestLMDInstance()
	connection := Connection{Name: "Test", Source: []string{"http://localhost/test/"}}
	peer := NewPeer(lmd, &connection)

	assert.Equal(t, uint32(NoFlags), peer.flags)

	peer.setFlag(Naemon)
	assert.Equal(t, uint32(Naemon), peer.flags)
	assert.True(t, peer.hasFlag(Naemon))
	assert.False(t, peer.hasFlag(HasDependencyColumn))

	peer.setFlag(HasDependencyColumn)
	assert.True(t, peer.hasFlag(HasDependencyColumn))
	assert.False(t, peer.hasFlag(MultiBackend))

	peer.resetFlags()
	assert.False(t, peer.hasFlag(Naemon))
	assert.Equal(t, uint32(NoFlags), peer.flags)

	peer.resetFlags()
	flags := NoFlags
	assert.Equal(t, "[<none>]", flags.String())

	flags.Load([]string{"Naemon", "HasDependencyColumn"})
	atomic.StoreUint32(&peer.flags, uint32(flags))
	assert.True(t, peer.hasFlag(Naemon))
	assert.True(t, peer.hasFlag(HasDependencyColumn))
	assert.False(t, peer.hasFlag(MultiBackend))
	assert.Equal(t, "[Naemon, HasDependencyColumn]", flags.String())
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
