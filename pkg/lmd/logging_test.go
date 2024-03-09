package lmd

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	devnull := new(bytes.Buffer)
	log.SetOutput(devnull)
	defer log.SetOutput(os.Stdout)

	logWith(nil).Errorf("nil")

	p := &Peer{}
	logWith(p).Errorf("peer")

	var p2 *Peer
	logWith(p2).Errorf("nil peer")

	c := context.WithValue(context.Background(), CtxClient, "testclient")
	logWith(c).Errorf("context")

	assert.Contains(t, devnull.String(), "nil peer")
	assert.Contains(t, devnull.String(), "testclient")
}
