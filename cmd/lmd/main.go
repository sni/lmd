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
	"pkg/lmd"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

func main() {
	lmd.Main(Build)
}
