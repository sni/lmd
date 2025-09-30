package lmd

import (
	"slices"
	"sync/atomic"
)

// PeerMap is a concurrency-safe lock-free map of all peers.
// tuned for many reads and few writes.
type PeerMap struct {
	current atomic.Value
}

type peerMap struct {
	refs  map[string]*Peer // holds references of all available remote peers.
	peers []*Peer          // ordered peers
	keys  []string         // ordered peer IDs
}

func NewPeerMap() *PeerMap {
	current := &peerMap{
		refs:  make(map[string]*Peer),
		peers: make([]*Peer, 0),
		keys:  make([]string, 0),
	}
	p := &PeerMap{}
	p.current.Store(current)

	return p
}

// Peers returns ordered list of peers.
func (pm *PeerMap) Peers() []*Peer {
	m := pm.load()

	return m.peers
}

// Refs returns map of peers.
func (pm *PeerMap) Refs() map[string]*Peer {
	m := pm.load()

	return m.refs
}

// Get retrieves a peer by its ID.
func (pm *PeerMap) Get(peerID string) *Peer {
	m := pm.load()

	return m.refs[peerID]
}

// Add adds a peer to the map.
func (pm *PeerMap) Add(peers ...*Peer) {
	size := len(peers)

	if size == 0 {
		return
	}

	// create a new copy of the map
	old := pm.load()
	newPeers := make([]*Peer, 0, len(old.peers)+size)
	newPeers = append(newPeers, old.peers...)

	// add new entry(s)
	for _, peer := range peers {
		if _, exists := old.refs[peer.ID]; exists {
			log.Panicf("peer with ID %s already exists in peer map", peer.ID)
		}
		newPeers = append(newPeers, peer)
	}

	// store the new map
	pm.Replace(newPeers)
}

// Remove removes a peer from the map by its ID.
func (pm *PeerMap) Remove(ids ...string) {
	size := len(ids)

	if size == 0 {
		return
	}

	old := pm.load()
	newList := make([]*Peer, 0, len(old.keys)-size)
	for k, v := range old.refs {
		if !slices.Contains(ids, k) {
			newList = append(newList, v)
		}
	}

	// store the new map
	pm.Replace(newList)
}

// Replace replaces list of peer with new list.
func (pm *PeerMap) Replace(peers []*Peer) {
	size := len(peers)

	// create a new map
	newMap := &peerMap{
		refs:  make(map[string]*Peer, size),
		peers: make([]*Peer, 0, size),
		keys:  make([]string, 0, size),
	}

	for _, peer := range peers {
		newMap.refs[peer.ID] = peer
		newMap.peers = append(newMap.peers, peer)
		newMap.keys = append(newMap.keys, peer.ID)
	}

	// store the new map
	pm.current.Store(newMap)
}

// load returns the current map.
func (pm *PeerMap) load() *peerMap {
	current := pm.current.Load().(*peerMap) //nolint:forcetypeassert // type is always *peerMap

	return current
}
