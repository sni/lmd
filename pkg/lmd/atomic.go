package lmd

import "sync/atomic"

type atomicFloat64 struct {
	v atomic.Pointer[float64]
}

func (a *atomicFloat64) Set(val float64) {
	a.v.Store(&val)
}

func (a *atomicFloat64) Get() float64 {
	val := a.v.Load()
	if val != nil {
		return *val
	}

	return 0
}

type atomicString struct {
	v atomic.Pointer[string]
}

func (a *atomicString) Set(val string) {
	a.v.Store(&val)
}

func (a *atomicString) Get() string {
	val := a.v.Load()
	if val != nil {
		return *val
	}

	return ""
}

type atomicStringList struct {
	v atomic.Pointer[[]string]
}

func (a *atomicStringList) Set(val []string) {
	a.v.Store(&val)
}

func (a *atomicStringList) Get() []string {
	val := a.v.Load()
	if val != nil {
		return *val
	}

	return []string{}
}

type atomicPeerStatus struct {
	v atomic.Pointer[PeerStatus]
}

func (a *atomicPeerStatus) Set(val PeerStatus) {
	a.v.Store(&val)
}

func (a *atomicPeerStatus) Get() PeerStatus {
	val := a.v.Load()
	if val != nil {
		return *val
	}

	return PeerStatusPending
}
