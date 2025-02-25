package lmd

import (
	"bytes"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"time"

	"github.com/sasha-s/go-deadlock"
)

var deadlockOpts = deadlock.Opts

const threshold = 3 * time.Millisecond

type lockMsg struct {
	direction string
	waited    time.Duration
	point     string
	name      string
	src       string
	gid       uint64
	timeIn    time.Time
}

var lockChan chan *lockMsg

func init() {
	lockChan = make(chan *lockMsg)
	go func() {
		locks := map[string]map[uint64]*lockMsg{}
		for {
			msg := <-lockChan
			switch msg.direction {
			case "write lock", "read lock":
				waitMsg := ""
				if msg.waited > threshold {
					waitMsg = fmt.Sprintf(" (%s)", msg.waited)
				}
				msg.timeIn = time.Now()
				if _, ok := locks[msg.point]; !ok {
					locks[msg.point] = make(map[uint64]*lockMsg)
				}
				locks[msg.point][msg.gid] = msg
				log.Infof("[%12s] %12s: %20s -> %20s%s", msg.point, msg.direction, msg.src, msg.name, waitMsg)
			case "write unlock", "read unlock":
				p, _ := locks[msg.point][msg.gid]
				duration := time.Since(p.timeIn)
				waitMsg := ""
				if duration > 1*time.Millisecond {
					waitMsg = fmt.Sprintf(" (%s)", duration)
				}
				delete(locks[msg.point], msg.gid)
				log.Infof("[%12s] %12s: %20s -> %20s%s", msg.point, msg.direction, msg.src, msg.name, waitMsg)
			default:
				log.Panicf("unknown direction: %s", msg.direction)
			}
		}
	}()
}

// An RWMutex is a drop-in replacement for sync.RWMutex.
type RWMutex struct {
	mu   deadlock.RWMutex
	name string
}

func NewRWMutex() *RWMutex {
	m := new(RWMutex)

	_, file, line, ok := runtime.Caller(1)
	if ok {
		m.name = fmt.Sprintf("%s:%d", path.Base(file), line)
	}

	return m
}

// Lock locks rw for writing.
func (m *RWMutex) Lock() {
	if m.mu.TryLock() {
		notifyLock(m, "write lock", 0)

		return
	}
	t1 := time.Now()
	m.mu.Lock()
	notifyLock(m, "write lock", time.Since(t1))
}

// Unlock unlocks the mutex for writing.
func (m *RWMutex) Unlock() {
	m.mu.Unlock()
	notifyLock(m, "write unlock", 0)
}

// RLock locks rw for writing.
func (m *RWMutex) RLock() {
	if m.mu.TryRLock() {
		notifyLock(m, "read lock", 0)

		return
	}
	t1 := time.Now()
	m.mu.RLock()
	notifyLock(m, "read lock", time.Since(t1))
}

// RUnlock locks rw for writing.
func (m *RWMutex) RUnlock() {
	m.mu.RUnlock()
	notifyLock(m, "read unlock", 0)
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func notifyLock(m *RWMutex, direction string, waited time.Duration) {
	_, file, line, ok := runtime.Caller(2)
	src := ""
	if ok {
		src = fmt.Sprintf("%s:%d", path.Base(file), line)
	}

	lockChan <- &lockMsg{
		direction: direction,
		waited:    waited,
		point:     fmt.Sprintf("%p", m),
		name:      m.name,
		src:       src,
		gid:       getGID(),
	}
}
