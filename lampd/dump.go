// +build ignore

package main

import (
	"github.com/davecgh/go-spew/spew"
)

func Dump(v interface{}) {
	spew.Config.Indent = "\t"
	spew.Config.MaxDepth = 20
	spew.Config.DisableMethods = true
	spew.Dump(v)
}
