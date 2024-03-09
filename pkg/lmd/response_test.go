package lmd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestHeaderTableFail(t *testing.T) {
	lmd := createTestLMDInstance()
	buf := bufio.NewReader(bytes.NewBufferString("GET none\n"))
	_, _, err := NewRequest(context.TODO(), lmd, buf, ParseOptimize)
	require.Equal(t, errors.New("bad request: table none does not exist"), err)
}
