// build with debug functions

package main

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"
)

func Dump(v interface{}) {
	spew.Config.Indent = "\t"
	spew.Config.MaxDepth = 20
	spew.Config.DisableMethods = true
	fmt.Fprintf(os.Stderr, spew.Sdump(v))
}
