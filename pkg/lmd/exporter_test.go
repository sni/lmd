package lmd

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExporter(t *testing.T) {
	peer, cleanup, lmd := StartTestPeer(3, 10, 10)
	PauseTestPeers(peer)

	tempDir := t.TempDir()

	file := tempDir + "/exp.tar.gz"
	lmd.flags.flagExport = file
	lmd.peerMap = NewPeerMap() // reset peer map

	// hide warnings from settings flags
	peer.lmd.flags.flagImport = "Naemon"

	// overrule stdout logging of exporter
	t.Setenv("LMD_LOG_LEVEL", "warn")

	// run the export
	err := exportData(lmd)
	require.NoError(t, err)
	require.FileExists(t, file)

	// import the file again
	err = initializePeersWithImport(peer.lmd, file)
	require.NoError(t, err)
	assert.Lenf(t, peer.lmd.peerMap.Peers(), 3, "all 3 backends should have been imported")

	// unpack the tarball
	cmd := exec.CommandContext(t.Context(), "tar", "xzf", file)
	cmd.Dir = tempDir
	_, err = cmd.Output()
	require.NoError(t, err)

	// clear peers again
	for _, p := range peer.lmd.peerMap.Peers() {
		p.Stop()
		peer.lmd.peerMap.Remove(p.ID)
	}
	assert.Emptyf(t, peer.lmd.peerMap.Peers(), "peer map is empty now")

	err = initializePeersWithImport(peer.lmd, tempDir+"/sites/")
	require.NoError(t, err)
	assert.Lenf(t, peer.lmd.peerMap.Peers(), 3, "all 3 backends should have been imported")

	// run cleanup
	err = cleanup()
	require.NoError(t, err)
}
