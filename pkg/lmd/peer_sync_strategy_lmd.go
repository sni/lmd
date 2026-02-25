package lmd

import "context"

// ensure we fully implement the SyncStrategy type.
var _ SyncStrategy = &SyncStrategyLMD{}

// SyncStrategyLMD synchronizes with peers with multiple connections from LMD.
type SyncStrategyLMD struct {
	store *DataStoreSet
}

func (s *SyncStrategyLMD) Init(store *DataStoreSet) {
	s.store = store
}

// UpdateDelta runs the periodic updates from the update loop for LMD backends
// it fetches the sites table and creates and updates LMDSub backends for them.
func (s *SyncStrategyLMD) UpdateDelta(ctx context.Context, _, _ float64) (err error) {
	store := s.store
	if store == nil {
		return nil
	}
	peer := store.peer

	columns := []string{"key", "name", "status", "addr", "last_error", "last_update", "last_online", "last_query", "idling"}
	req := &Request{
		Table:   TableSites,
		Columns: columns,
	}
	peer.setQueryOptions(req)
	res, _, err := peer.query(ctx, req)
	if err != nil {
		logWith(peer, req).Infof("failed to fetch sites information: %s", err.Error())

		return err
	}
	resHash := res.result2Hash(columns)

	// check if we need to start/stop peers
	logWith(peer).Debugf("checking for changed remote lmd backends")
	existing := make(map[string]bool)
	for _, rowHash := range resHash {
		subID := peer.addSubPeer(ctx, LMDSub, interface2stringNoDedup(rowHash["key"]), peer.Name+"/"+interface2stringNoDedup(rowHash["name"]), rowHash)
		existing[subID] = true
	}

	peer.removeExceedingSubPeers(existing)

	return nil
}

func (s *SyncStrategyLMD) UpdateTick(_ context.Context) error {
	return nil
}
