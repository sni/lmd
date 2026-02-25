package lmd

import "context"

// ensure we fully implement the SyncStrategy type.
var _ SyncStrategy = &SyncStrategyMultiBackend{}

// SyncStrategyMultiBackend synchronizes with peers with multiple connections.
type SyncStrategyMultiBackend struct {
	store *DataStoreSet
}

func (s *SyncStrategyMultiBackend) Init(store *DataStoreSet) {
	s.store = store
}

// UpdateDelta runs the periodic updates from the update loop for LMD backends
// it fetches the sites table and creates and updates LMDSub backends for them.
func (s *SyncStrategyMultiBackend) UpdateDelta(ctx context.Context, _, _ float64) (err error) {
	store := s.store
	if store == nil {
		return nil
	}
	peer := store.peer

	sites, err := peer.fetchRemotePeers(ctx, s.store)
	if err != nil {
		logWith(peer).Infof("failed to fetch sites information: %s", err.Error())
		peer.errorLogged.Store(true)

		return err
	}

	// check if we need to start/stop peers
	logWith(peer).Debugf("checking for changed remote multi backends")
	existing := make(map[string]bool)
	for _, siteRow := range sites {
		var site map[string]any
		if s, ok2 := siteRow.(map[string]any); ok2 {
			site = s
		} else {
			continue
		}
		subID := peer.addSubPeer(ctx, HTTPSub, interface2stringNoDedup(site["id"]), interface2stringNoDedup(site["name"]), site)
		existing[subID] = true
	}

	peer.removeExceedingSubPeers(existing)

	return nil
}

func (s *SyncStrategyMultiBackend) UpdateTick(_ context.Context) error {
	return nil
}
