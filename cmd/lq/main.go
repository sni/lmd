package main

/*
 * LQ - Livestatus Query Tool
 * Send queries to livestatus socket or tcp connection.
 *
 * # Help
 *
 * Use `lq -h` to get help on command line parameters.
 */

import (
	"pkg/lmd"
)

// Build contains the current git commit id
// compile passing -ldflags "-X main.Build <build sha1>" to set the id.
var Build string

func main() {
	lmd.MainLQ(Build)
}
