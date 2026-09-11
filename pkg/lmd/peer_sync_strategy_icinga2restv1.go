package lmd

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ensure we fully implement the SyncStrategy type.
var _ SyncStrategy = &SyncStrategyIcinga2RestV1{}

// restv1ObjectCheckInterval is the interval of the object change detection.
const restv1ObjectCheckInterval = 60 * time.Second

// SyncStrategyIcinga2RestV1 synchronizes with an Icinga 2 REST v1 backend.
//
// The Icinga 2 REST API has no "changed since" endpoint, so deltas are based on the
// last_check / last_state_change timestamps: every object whose last_check or
// last_state_change is at or after the previous sync time is re-fetched and merged.
// This is best effort (config edits that do not touch those timestamps may be missed)
// which is mitigated by the periodic full update (FullUpdateInterval), the object
// change detection below and the full delta that runs after a command.
type SyncStrategyIcinga2RestV1 struct {
	lastObjectCheck time.Time
	store           *DataStoreSet
	firstDelta      bool
}

func (s *SyncStrategyIcinga2RestV1) Init(store *DataStoreSet) {
	s.store = store
	s.firstDelta = true
}

// UpdateTick runs the periodic housekeeping (timeperiods and group statistics).
func (s *SyncStrategyIcinga2RestV1) UpdateTick(ctx context.Context) (err error) {
	store := s.store
	if store == nil {
		return nil
	}

	_, err = store.tryTimeperiodsUpdate(ctx)
	if err != nil {
		return err
	}
	if err := store.tryOnDemandComDownUpdate(ctx); err != nil {
		return err
	}

	return s.checkObjectsChanged(ctx)
}

// UpdateDelta runs a timestamp based delta update on the hosts and services table.
func (s *SyncStrategyIcinga2RestV1) UpdateDelta(ctx context.Context, from, _ float64) (err error) {
	store := s.store
	if store == nil {
		return nil
	}

	// the first delta after init runs without filter to populate the derived
	// host/group statistics, an executed command forces a full delta to pick up
	// state changes not covered by the timestamp filter
	fullDelta := s.firstDelta || store.peer.restv1FullDelta.Swap(false)
	s.firstDelta = true
	filterStr := ""
	if !fullDelta && from > 0 {
		from -= float64(store.peer.lmd.Config.UpdateOffset)
		filterStr = fmt.Sprintf("Filter: last_check >= %d\nFilter: last_state_change >= %d\nOr: 2\n", int64(from), int64(from))
	}

	if _, _, err = store.updateDeltaHosts(ctx, filterStr); err != nil {
		if s.isNewObjectError(err) {
			return s.fullResync(ctx)
		}

		return err
	}

	if _, _, err = store.updateDeltaServices(ctx, filterStr); err != nil {
		if s.isNewObjectError(err) {
			return s.fullResync(ctx)
		}

		return err
	}

	store.rebuildHostServiceStats()
	if err := store.updateCommentsAndDowntimes(ctx); err != nil {
		return err
	}
	s.firstDelta = false

	return nil
}

// checkObjectsChanged detects added or removed objects at a reduced rate and
// reinitializes the tables if the object set changed. The timestamp based delta
// only sees objects which ran a check or changed state, so removed objects and
// never checked new objects would not be picked up otherwise.
func (s *SyncStrategyIcinga2RestV1) checkObjectsChanged(ctx context.Context) (err error) {
	if time.Now().Before(s.lastObjectCheck) {
		return nil
	}
	s.lastObjectCheck = time.Now().Add(restv1ObjectCheckInterval)

	store := s.store
	peer := store.peer
	hostStore := store.get(TableHosts)
	serviceStore := store.get(TableServices)
	if hostStore == nil || serviceStore == nil {
		return nil
	}

	hostReq := &Request{Table: TableHosts, Columns: []string{"name"}}
	peer.setQueryOptions(hostReq)
	hostRes, _, err := peer.Query(ctx, hostReq)
	if err != nil {
		return err
	}
	hostSeen := make(map[string]bool, len(hostRes))
	for i := range hostRes {
		hostSeen[interface2stringNoDedup(hostRes[i][0])] = true
	}
	hostStore.lock.RLock()
	changed := len(hostSeen) != len(hostStore.index)
	for name := range hostSeen {
		if _, ok := hostStore.index[name]; !ok {
			changed = true

			break
		}
	}
	hostStore.lock.RUnlock()
	if changed {
		return s.fullResync(ctx)
	}

	serviceReq := &Request{Table: TableServices, Columns: []string{"host_name", "description"}}
	peer.setQueryOptions(serviceReq)
	serviceRes, _, err := peer.Query(ctx, serviceReq)
	if err != nil {
		return err
	}
	serviceSeen := make(map[string]bool, len(serviceRes))
	for i := range serviceRes {
		key := interface2stringNoDedup(serviceRes[i][0]) + "\x00" + interface2stringNoDedup(serviceRes[i][1])
		serviceSeen[key] = true
	}
	serviceStore.lock.RLock()
	changed = len(serviceSeen) != len(serviceStore.data)
	for key := range serviceSeen {
		host, service, _ := strings.Cut(key, "\x00")
		if serviceStore.index2[host][service] == nil {
			changed = true

			break
		}
	}
	serviceStore.lock.RUnlock()
	if changed {
		return s.fullResync(ctx)
	}

	return nil
}

// isNewObjectError detects the "object not in cache" errors raised by the delta merge
// when the backend has added a new object.
func (s *SyncStrategyIcinga2RestV1) isNewObjectError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()

	return strings.Contains(msg, "no host named") || strings.Contains(msg, "no service named")
}

// fullResync reinitializes all tables, used when the delta met unknown objects.
func (s *SyncStrategyIcinga2RestV1) fullResync(ctx context.Context) (err error) {
	peer := s.store.peer
	logWith(peer).Debugf("restv1 delta found unknown objects, reinitializing tables")

	return peer.initAllTables(ctx)
}
