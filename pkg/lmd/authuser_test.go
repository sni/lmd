package lmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/**
 * Tests that only one host entry is returned when setting the AuthUser.
 */
func TestAuthuserHost(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	// Authuser should only see it's own hosts
	res, _, err := peer.QueryString("GET hosts\nColumns: name state contacts\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that only two host stats are returned with AuthUser is set.
 */
func TestAuthuserHostStats(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hosts\nStats: state = 0\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.InDelta(t, 1.0, res[0][0], 0)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that only one service entries is returned when setting the AuthUser.
 */
func TestAuthuserService(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	// Authuser should only see it's own services
	res, _, err := peer.QueryString("GET services\nColumns: host_name description state\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that only two services stats are returned with AuthUser is set.
 */
func TestAuthuserServiceStats(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 10, 10)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET services\nStats: state = 0\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.InDelta(t, 7.0, res[0][0], 0)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that only one hostgroup entry is returned when setting the AuthUser.
 */
func TestAuthuserHostgroupsLoose(t *testing.T) {
	extraConfig := `
		GroupAuthorization = "loose"
	`
	peer, cleanup, _ := StartTestPeerExtra(1, 2, 2, extraConfig)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hostgroups\nColumns: name\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 2)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that only one hostgroup entry is returned when setting the AuthUser.
 */
func TestAuthuserHostgroupsStrict(t *testing.T) {
	extraConfig := `
		GroupAuthorization = "strict"
	`
	peer, cleanup, _ := StartTestPeerExtra(1, 2, 2, extraConfig)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET hostgroups\nColumns: name\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * We don't have any servicegroups that are valid with AuthUser but we still
 * test that when setting the AuthUser header, no servicegroups are returned.
 */
func TestAuthuserServicegroups(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET servicegroups\nColumns: name members\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that only one comment entry is returned when setting the AuthUser.
 */
func TestAuthuserComments(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET comments\nColumns: author comment\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that we get a correct number of services back when ServiceAuthorization is set to strict.
 */
func TestAuthuserServiceAuth(t *testing.T) {
	extraConfig := `
		ServiceAuthorization = "strict"
	`
	peer, cleanup, _ := StartTestPeerExtra(1, 2, 10, extraConfig)
	PauseTestPeers(peer)

	res, _, err := peer.QueryString("GET services\nColumns: host_name description state contacts\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 2)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that only one host entry is returned when setting the AuthUser.
 */
func TestAuthuserHostsbygroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	// Authuser should only see it's own hosts
	res, _, err := peer.QueryString("GET hostsbygroup\nColumns: name state\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * We don't have any servicegroups that are valid with AuthUser but we still
 * test that when setting the AuthUser header, no servicegroups are returned.
 */
func TestAuthuserServicesbygroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 10)
	PauseTestPeers(peer)

	// Authuser should only see it's own services
	res, _, err := peer.QueryString("GET servicesbygroup\nColumns: host_name description servicegroup_name\nAuthuser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 2)

	err = cleanup()
	require.NoError(t, err)
}

/**
 * Tests that only one service entries is returned when setting the AuthUser.
 */
func TestAuthuserServicesbyhostgroup(t *testing.T) {
	peer, cleanup, _ := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	// Authuser should only see it's own services
	res, _, err := peer.QueryString("GET servicesbyhostgroup\nColumns: host_name description state\nAuthUser: authuser\n\n")
	require.NoError(t, err)
	assert.Len(t, res, 1)

	err = cleanup()
	require.NoError(t, err)
}
