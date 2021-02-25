package main

import (
	"testing"
)

/**
 * Tests that only one host entry is returned when setting the AuthUser
 */
func TestAuthuserHost(t *testing.T) {
	peer := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	// Authuser should only see it's own hosts
	res, _, err := peer.QueryString("GET hosts\nColumns: name state\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that only two host stats are returned with AuthUser is set
 */
func TestAuthuserHostStats(t *testing.T) {
	peer := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	res, _, err := peer.QueryString("GET hosts\nStats: state = 0\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1.0, res[0][0]); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that only one service entries is returned when setting the AuthUser
 */
func TestAuthuserService(t *testing.T) {
	peer := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	// Authuser should only see it's own services
	res, _, err := peer.QueryString("GET services\nColumns: host_name description state\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that only two services stats are returned with AuthUser is set
 */
func TestAuthuserServiceStats(t *testing.T) {
	peer := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	res, _, err := peer.QueryString("GET services\nStats: state = 1\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1.0, res[0][0]); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that only one hostgroup entry is returned when setting the AuthUser
 */
func TestAuthuserHostgroupsLoose(t *testing.T) {
	extraConfig := `
		GroupAuthorization = "loose"
	`
	peer := StartTestPeerExtra(1, 2, 2, extraConfig)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	res, _, err := peer.QueryString("GET hostgroups\nColumns: name\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(2, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that only one hostgroup entry is returned when setting the AuthUser
 */
func TestAuthuserHostgroupsStrict(t *testing.T) {
	extraConfig := `
		GroupAuthorization = "strict"
	`
	peer := StartTestPeerExtra(1, 2, 2, extraConfig)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	res, _, err := peer.QueryString("GET hostgroups\nColumns: name\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * We don't have any servicegroups that are valid with AuthUser but we still
 * test that when setting the AuthUser header, no servicegroups are returned.
 */
func TestAuthuserServicegroups(t *testing.T) {
	peer := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	res, _, err := peer.QueryString("GET servicegroups\nColumns: name members\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that only one comment entry is returned when setting the AuthUser
 */
func TestAuthuserComments(t *testing.T) {
	peer := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	res, _, err := peer.QueryString("GET comments\nColumns: author comment\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that we get a correct number of services back when ServiceAuthorization is set to strict
 */
func TestAuthuserServiceAuth(t *testing.T) {
	extraConfig := `
		ServiceAuthorization = "strict"
	`
	peer := StartTestPeerExtra(1, 2, 9, extraConfig)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	res, _, err := peer.QueryString("GET services\nColumns: host_name description state\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that only one host entry is returned when setting the AuthUser
 */
func TestAuthuserHostsbygroup(t *testing.T) {
	peer := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	// Authuser should only see it's own hosts
	res, _, err := peer.QueryString("GET hostsbygroup\nColumns: name state\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1, len(res)); err != nil {
		t.Fatal(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * We don't have any servicegroups that are valid with AuthUser but we still
 * test that when setting the AuthUser header, no servicegroups are returned.
 */
func TestAuthuserServicesbygroup(t *testing.T) {
	peer := StartTestPeer(1, 2, 9)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	// Authuser should only see it's own services
	res, _, err := peer.QueryString("GET servicesbygroup\nColumns: host_name description servicegroup_name\nAuthuser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}

/**
 * Tests that only one service entries is returned when setting the AuthUser
 */
func TestAuthuserServicesbyhostgroup(t *testing.T) {
	peer := StartTestPeer(1, 2, 2)
	PauseTestPeers(peer)

	if err := assertEq(1, len(PeerMap)); err != nil {
		t.Error(err)
	}

	// Authuser should only see it's own services
	res, _, err := peer.QueryString("GET servicesbyhostgroup\nColumns: host_name description state\nAuthUser: authuser\n\n")
	if err != nil {
		t.Fatal(err)
	}

	if err := assertEq(1, len(res)); err != nil {
		t.Error(err)
	}

	if err := StopTestPeer(peer); err != nil {
		panic(err.Error())
	}
}
