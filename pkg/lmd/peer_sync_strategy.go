package lmd

import "context"

// SyncStrategy handles peer synchronization.
type SyncStrategy interface {
	Init(store *DataStoreSet)                                         // Initialize for the given datastore
	UpdateTick(ctx context.Context) (err error)                       // UpdateTick is called in the update loop every tick
	UpdateDelta(ctx context.Context, from, until float64) (err error) // UpdateDelta runs delta updates from the periodic update loop and returns whether an update was performed
}

func NewSyncStrategy(store *DataStoreSet) SyncStrategy {
	s := &SyncStrategyLastCheck{}
	s.Init(store)

	return s
}
