package lmd

import (
	"context"
	"fmt"
)

// ensure we fully implement the SyncStrategy type.
var _ SyncStrategy = &SyncStrategyLastUpdate{}

// SyncStrategyLastUpdate synchronizes with peers using the last update strategy.
type SyncStrategyLastUpdate struct {
	store             *DataStoreSet
	lastUpdateHost    float64
	lastUpdateService float64
}

func (s *SyncStrategyLastUpdate) Init(store *DataStoreSet) {
	s.store = store
}

// UpdateDelta runs the periodic updates from the update loop for LMD backends
// it fetches the sites table and creates and updates LMDSub backends for them.
func (s *SyncStrategyLastUpdate) UpdateDelta(ctx context.Context, _, _ float64) (err error) {
	store := s.store
	if store == nil {
		return nil
	}

	// update comments and downtimes
	err = store.updateCommentsAndDowntimes(ctx)
	if err != nil {
		return err
	}

	hostStore := store.get(TableHosts)
	serviceStore := store.get(TableServices)

	// fetch last_update value from data if not set yet, otherwise we would fetch all data twice on the first run
	if s.lastUpdateHost == 0 {
		s.lastUpdateHost = s.getMaxLastUpdateData(hostStore)
		s.lastUpdateService = s.getMaxLastUpdateData(serviceStore)
	}

	// update hosts and save highest last_update for update
	_, updateSet, err := store.updateDeltaHosts(ctx, fmt.Sprintf("Filter: last_update >= %f\n", s.lastUpdateHost))
	if err != nil {
		return err
	}
	s.lastUpdateHost = s.getMaxLastUpdateResult(hostStore, updateSet)

	// update services and save highest last_update for update
	_, updateSet, err = store.updateDeltaServices(ctx, fmt.Sprintf("Filter: last_update >= %f\n", s.lastUpdateService))
	if err != nil {
		return err
	}
	s.lastUpdateService = s.getMaxLastUpdateResult(serviceStore, updateSet)

	return nil
}

func (s *SyncStrategyLastUpdate) UpdateTick(ctx context.Context) (err error) {
	store := s.store
	if store == nil {
		return nil
	}

	_, err = store.tryTimeperiodsUpdate(ctx)
	if err != nil {
		return err
	}

	err = store.tryOnDemandComDownUpdate(ctx)
	if err != nil {
		return err
	}

	return nil
}

// getMaxLastUpdateResult returns the maximum last_update value from the given update set.
func (s *SyncStrategyLastUpdate) getMaxLastUpdateResult(store *DataStore, updateSet []*ResultPrepared) float64 {
	var maxValue float64

	dataOffset := 1
	if store.table.name == TableServices {
		dataOffset = 2
	}
	_, lastUpdateResIdx := store.getUpdateColumn("last_update", dataOffset)

	for _, update := range updateSet {
		lastUpdate := interface2float64(update.ResultRow[lastUpdateResIdx])
		if lastUpdate > maxValue {
			maxValue = lastUpdate
		}
	}

	return maxValue
}

// getMaxLastUpdateData returns the maximum last_update value for given table data.
func (s *SyncStrategyLastUpdate) getMaxLastUpdateData(store *DataStore) float64 {
	var maxValue float64

	idx := store.GetColumn("last_update").Index

	for _, row := range store.data {
		lastUpdate := row.dataFloat[idx]
		if lastUpdate > maxValue {
			maxValue = lastUpdate
		}
	}

	return maxValue
}
