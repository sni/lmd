package main

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// LoggingLock is a lock wrapper which traces the current holding caller
type LoggingLock struct {
	lock               *sync.RWMutex
	currentlyLocked    int32 // 0 not locked, 1 locked
	currentLockpointer atomic.Value
	name               string
}

// Lock just calls sync.RWMutex.Lock and stores the current caller
func (l *LoggingLock) Lock() {
	caller := make([]uintptr, 20)
	runtime.Callers(0, caller)
	waited := false
	if atomic.LoadInt32(&l.currentlyLocked) == 0 {
		l.lock.Lock()
	} else {
		timeout := time.Second * 3
		c := make(chan struct{})
		go func() {
			defer close(c)
			l.lock.Lock()
		}()
		select {
		case <-c:
			// got lock within timeout, no logging
			break
		case <-time.After(timeout):
			// still waiting...
			log.Warnf("[%s] waiting for write lock in:", l.name)
			LogCaller(log.Warnf, &caller)
			log.Warnf("[%s] current lock held by:", l.name)
			LogCaller(log.Warnf, l.currentLockpointer.Load().(*[]uintptr))
			waited = true
		}
	}

	l.currentLockpointer.Store(&caller)
	atomic.StoreInt32(&l.currentlyLocked, 1)
	if waited {
		log.Infof("[%s] got write lock in:", l.name)
		LogCaller(log.Infof, &caller)
	}
}

// RLock just calls sync.RWMutex.RLock
func (l *LoggingLock) RLock() {
	caller := make([]uintptr, 20)
	runtime.Callers(0, caller)
	waited := false

	if atomic.LoadInt32(&l.currentlyLocked) == 0 {
		l.lock.RLock()
	} else {
		timeout := time.Second * 3
		c := make(chan struct{})
		go func() {
			defer close(c)
			l.lock.RLock()
		}()
		select {
		case <-c:
			// got lock within timeout, no logging
			break
		case <-time.After(timeout):
			// still waiting...
			log.Warnf("[%s] waiting for read lock in:", l.name)
			LogCaller(log.Warnf, &caller)
			log.Warnf("[%s] current lock held by:", l.name)
			LogCaller(log.Warnf, l.currentLockpointer.Load().(*[]uintptr))
			waited = true
		}
	}
	if waited {
		log.Infof("[%s] got read lock in:", l.name)
		LogCaller(log.Infof, &caller)
	}
}

// Unlock just calls sync.RWMutex.Unlock
func (l *LoggingLock) Unlock() {
	atomic.StoreInt32(&l.currentlyLocked, 0)
	l.lock.Unlock()
}

// RUnlock just calls sync.RWMutex.RUnlock
func (l *LoggingLock) RUnlock() {
	l.lock.RUnlock()
}

// NewLoggingLock returns a new LoggingLock
func NewLoggingLock(name string) *LoggingLock {
	l := new(LoggingLock)
	l.lock = new(sync.RWMutex)
	l.name = name
	return l
}

func LogCaller(logger func(format string, v ...interface{}), caller *[]uintptr) {
	frames := runtime.CallersFrames(*caller)
	for {
		frame, more := frames.Next()
		if frame.Function == "" {
			break
		}
		logger("- %s:%d %s()", frame.File, frame.Line, frame.Function)
		if !more {
			break
		}
	}
}
