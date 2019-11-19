// +build ignore

package main

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
