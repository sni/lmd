package main

import (
	"fmt"
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

// NewLoggingLock returns a new LoggingLock
func NewLoggingLock(name string) *LoggingLock {
	l := new(LoggingLock)
	l.lock = new(sync.RWMutex)
	l.name = name
	return l
}

// Lock just calls sync.RWMutex.Lock and stores the current caller
func (l *LoggingLock) Lock() {
	caller := make([]uintptr, 20)
	runtime.Callers(0, caller)
	waited := false

	if atomic.LoadInt32(&l.currentlyLocked) == 0 {
		l.lock.Lock()
		atomic.StoreInt32(&l.currentlyLocked, 1)
	} else {
		timeout := time.Second * time.Duration(LockTimeout)
		c := make(chan struct{})
		go func() {
			defer close(c)
			l.lock.Lock()
			atomic.StoreInt32(&l.currentlyLocked, 1)
		}()
		select {
		case <-c:
			// got lock within timeout, no logging
			break
		case <-time.After(timeout):
			// still waiting...
			l.logWaiting(&caller, fmt.Sprintf("[%s] waiting for write lock in:", l.name))
			waited = true
			<-c
		}
	}

	l.currentLockpointer.Store(&caller)
	if waited {
		log.Infof("[%s] finally got write lock", l.name)
	}
}

// RLock just calls sync.RWMutex.RLock
func (l *LoggingLock) RLock() {
	if atomic.LoadInt32(&l.currentlyLocked) == 0 {
		l.lock.RLock()
		return
	}

	waited := false
	timeout := time.Second * time.Duration(LockTimeout)
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
		caller := make([]uintptr, 20)
		runtime.Callers(0, caller)
		l.logWaiting(&caller, fmt.Sprintf("[%s] waiting for read lock in:", l.name))
		waited = true
		<-c
	}

	if waited {
		log.Infof("[%s] finally got read lock", l.name)
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

func (l *LoggingLock) logWaiting(caller *[]uintptr, message string) {
	log.Errorf("%s", message)
	LogCaller(log.Errorf, caller)
	log.Errorf("[%s] current lock held by:", l.name)
	LogCaller(log.Errorf, l.currentLockpointer.Load().(*[]uintptr))
	logThreaddump()
}
