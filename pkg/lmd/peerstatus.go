package lmd

// PeerStatusKey contains the different keys for the Peer.Status map.
type PeerStatusKey int8

// available keys for the peer status map.
const (
	_ PeerStatusKey = iota
	PeerKey
	PeerName
	Section
	PeerParent
)
