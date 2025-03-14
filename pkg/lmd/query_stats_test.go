package lmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryStats(t *testing.T) {
	extraConfig := `
LogQueryStats = true
`
	peer, cleanup, mockLMD := StartTestPeerExtra(1, 10, 10, extraConfig)

	res, _, err := peer.QueryString("GET services\n")
	require.NoError(t, err)
	assert.Equal(t, "accept_passive_checks", res[0][0])
	assert.InDelta(t, 1.0, res[1][0], 0)

	mockLMD.qStat.LogStats()

	err = cleanup()
	require.NoError(t, err)
}
