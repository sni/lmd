//go:build !lock_debug

package lmd

import (
	"github.com/sasha-s/go-deadlock"
)

var deadlockOpts = deadlock.Opts

// An RWMutex is a drop-in replacement for sync.RWMutex.
type RWMutex struct {
	deadlock.RWMutex
}

func NewRWMutex(_ string) *RWMutex {
	return new(RWMutex)
}
