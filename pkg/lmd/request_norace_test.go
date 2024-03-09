//go:build !race
// +build !race

package lmd

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestStatsTac(t *testing.T) {
	peer, cleanup, lmd := StartTestPeer(4, 10, 10)
	PauseTestPeers(peer)

	assert.Len(t, lmd.PeerMap, 4)

	lmd.defaultReqestParseOption = ParseDefault
	query := strings.ReplaceAll(tacPageStatsQuery, "OutputFormat: json", "OutputFormat: wrapped_json")
	res, meta, err := peer.QueryString(query)
	require.NoError(t, err)
	assert.Equal(t, int64(1), meta.Total)
	assert.Equal(t, int64(40), meta.RowsScanned)
	assert.InDelta(t, 40, res[0][0], 0)
	assert.InDelta(t, 28, res[0][7], 0)
	assert.InDelta(t, 24, res[0][8], 0)
	assert.InDelta(t, 4, res[0][9], 0)

	lmd.defaultReqestParseOption = ParseOptimize
	res2, meta2, err2 := peer.QueryString(query)
	require.NoError(t, err2)

	assert.Equal(t, int64(1), meta2.Total)
	assert.Equal(t, int64(40), meta2.RowsScanned)
	assert.Equal(t, res, res2)

	err = cleanup()
	require.NoError(t, err)
}
