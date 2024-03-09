//go:build ignore
// +build ignore

package lmd

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"
)

// Dump displays arbitrary data
func Dump(v interface{}) {
	spew.Config.Indent = "\t"
	spew.Config.MaxDepth = 3
	spew.Config.DisableMethods = true
	fmt.Fprint(os.Stderr, spew.Sdump(v))
}

// SDump returns arbitrary data as string
func SDump(v interface{}) string {
	spew.Config.Indent = "\t"
	spew.Config.MaxDepth = 3
	spew.Config.DisableMethods = true

	return spew.Sdump(v)
}
