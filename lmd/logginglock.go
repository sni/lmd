package main

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

// LoggingLock is a lock wrapper which traces the current holding caller
type LoggingLock struct {
	lock             *sync.RWMutex
	currentWriteLock atomic.Value
	name             string
}

// Lock just calls sync.RWMutex.Lock
func (l *LoggingLock) Lock() {
	l.LockN(2)
}

// LockN just calls sync.RWMutex.Lock and skips N caller functions to store current caller
func (l *LoggingLock) LockN(skip int) {
	_, file, line, _ := runtime.Caller(skip)
	file = strings.TrimPrefix(file[strings.LastIndex(file, "/"):], "/")
	waited := false
	currentWriteLock := l.currentWriteLock.Load().(string)
	if currentWriteLock != "" {
		log.Infof("[%s][%s:%d] waiting for write lock from %s", l.name, file, line, currentWriteLock)
		waited = true
	}
	l.lock.Lock()
	l.currentWriteLock.Store(fmt.Sprintf("%s:%d", file, line))
	if waited {
		log.Infof("[%s][%s:%d] got write lock", l.name, file, line)
	}
}

// RLock just calls sync.RWMutex.RLock
func (l *LoggingLock) RLock() {
	l.RLockN(2)
}

// RLockN just calls sync.RWMutex.RLock and skips N caller functions to store current caller
func (l *LoggingLock) RLockN(skip int) {
	_, file, line, _ := runtime.Caller(skip)
	file = strings.TrimPrefix(file[strings.LastIndex(file, "/"):], "/")
	waited := false
	currentWriteLock := l.currentWriteLock.Load().(string)
	if currentWriteLock != "" {
		log.Infof("[%s][%s:%d] waiting for read lock from %s", l.name, file, line, currentWriteLock)
		waited = true
	}
	l.lock.RLock()
	if waited {
		log.Infof("[%s][%s:%d] got read lock", l.name, file, line)
	}
}

// Unlock just calls sync.RWMutex.Unlock
func (l *LoggingLock) Unlock() {
	l.currentWriteLock.Store("")
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
	l.currentWriteLock.Store("")
	return l
}
