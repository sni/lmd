package lmd

import (
	"fmt"
	"time"
)

// TriggeredLock wraps a standard sync.RWMutex but has a way to use low priority write locks.
type TriggeredLock struct {
	lock         *RWMutex
	writeTrigger chan bool
	name         string
}

func NewTriggeredLock(name string) *TriggeredLock {
	lock := &TriggeredLock{
		name:         name,
		lock:         NewRWMutex(name),
		writeTrigger: make(chan bool, 3),
	}

	return lock
}

// Lock locks for writing.
func (l *TriggeredLock) Lock() {
	l.lock.Lock()
}

// Unlock unlocks a write lock.
func (l *TriggeredLock) Unlock() {
	l.lock.Unlock()
	l.drainTrigger()
	l.writeTrigger <- true
}

// RLock locks for reading.
func (l *TriggeredLock) RLock() {
	l.lock.RLock()
}

// RUnlock unlocks a read lock.
func (l *TriggeredLock) RUnlock() {
	l.lock.RUnlock()
	l.drainTrigger()
	l.writeTrigger <- true
}

// LockLowPriority issues a Lock() when there are no waiting RLock readers up to maxWait time.
func (l *TriggeredLock) LockLowPriority(maxWait time.Duration) {
	if l.lock.TryLock() {
		return
	}

	t1 := time.Now()

	// wait up to maxWait to get trigger by a finished unlock
	locked := false
	trying := true
	for trying {
		select {
		case <-l.writeTrigger:
			// signaled, but it doesn't mean we can get the lock, so try to acquire it
			if l.lock.TryLock() {
				locked = true
				trying = false
			}
		case <-time.After(maxWait):
			// timeout
			log.Warnf("unable to get a write slot in time (%s)", l.name)
			trying = false
		}
	}

	waitDuration := time.Since(t1)

	var blockDuration time.Duration
	if locked {
		blockDuration = 0
	} else {
		t2 := time.Now()
		l.lock.Lock()
		blockDuration = time.Since(t2)
	}

	l.drainTrigger()

	logMsg := fmt.Sprintf("waiting for %12s write lock took: wait: %8s | blocking: %8s",
		l.name,
		waitDuration.Truncate(time.Millisecond).String(),
		blockDuration.Truncate(time.Millisecond).String())
	if blockDuration > 10*time.Second {
		log.Warn(logMsg)
	} else {
		log.Debugf(logMsg)
	}
}

// drainTrigger removes all elements from the writeTrigger channel to avoid blocking future writes.
func (l *TriggeredLock) drainTrigger() {
	for len(l.writeTrigger) > 0 {
		<-l.writeTrigger
	}
}
