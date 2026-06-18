package lmd

import (
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
	select {
	case <-l.writeTrigger:
		// signaled
	case <-time.After(maxWait):
		log.Warnf("unable to get a write slot in time (%s)", l.name)
		// timeout
	}

	waitDuration := time.Since(t1)
	t2 := time.Now()
	l.lock.Lock()
	l.drainTrigger()

	blockDuration := time.Since(t2)
	if blockDuration > 3*time.Second {
		log.Warnf("waiting for %s write lock took: wait: %s | blocking: %s", l.name, waitDuration.String(), blockDuration.String())
	} else {
		log.Debugf("waiting for %s write lock took: wait: %s | blocking: %s", l.name, waitDuration.String(), blockDuration.String())
	}
}

// drainTrigger removes all elements from the writeTrigger channel to avoid blocking future writes.
func (l *TriggeredLock) drainTrigger() {
	for len(l.writeTrigger) > 0 {
		<-l.writeTrigger
	}
}
