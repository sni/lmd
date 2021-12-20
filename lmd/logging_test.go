package main

import (
	"bytes"
	"context"
	"os"
	"testing"
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

	if err := assertLike("nil peer", devnull.String()); err != nil {
		t.Error(err)
	}

	if err := assertLike("testclient", devnull.String()); err != nil {
		t.Error(err)
	}
}
