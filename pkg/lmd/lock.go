//go:build !lock_debug

package lmd

import (
	"github.com/sasha-s/go-deadlock"
)

var deadlockOpts = &deadlock.Opts

func init() {
	// start with deadlock disabled by default
	deadLockDisable()
}

func deadLockDisable() {
	deadlockOpts.Disable = true
	deadlockOpts.TimerPool = deadlock.TimerPoolDisabled
}

func deadLockEnable() {
	deadlockOpts.Disable = false
	deadlockOpts.TimerPool = deadlock.TimerPoolEnabled
	deadlockOpts.LogBuf = NewLogWriter("Error")
}

// An RWMutex is a drop-in replacement for sync.RWMutex.
type RWMutex struct {
	deadlock.RWMutex
}

func NewRWMutex(_ string) *RWMutex {
	return new(RWMutex)
}
