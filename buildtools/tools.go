// +build tools

package tools

/*
 * list of modules required to build and run all tests
 * see: https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module
 */

import (
	_ "github.com/davecgh/go-spew/spew"
	_ "golang.org/x/tools/cmd/goimports"
	_ "github.com/jmhodges/copyfighter"
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	_ "golang.org/x/tools/cmd/stringer"
)
