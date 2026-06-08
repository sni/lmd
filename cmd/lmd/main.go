package main

/*
 * LMD - Livestatus Multitool daemon
 * A livestatus in/out converter and caching daemon.
 *
 * # Help
 *
 * Use `lmd -h` to get help on command line parameters.
 */

import (
	"runtime/debug"
	"time"

	"pkg/lmd"
)

// free memory every minute.
func init() {
	go func() {
		defer lmd.LogPanicExit()
		t := time.Tick(1 * time.Minute)
		for {
			<-t
			debug.FreeOSMemory()
		}
	}()
}

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

func main() {
	lmd.Main(Build)
}
