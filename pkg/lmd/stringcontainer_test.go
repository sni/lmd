package lmd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringContainerCompression(t *testing.T) {
	var teststring strings.Builder
	for i := range [2000]int{} {
		teststring.WriteString(fmt.Sprintf("%d x", i))
	}
	str := teststring.String()
	cont := NewStringContainer(&str)

	assert.Empty(t, cont.stringData)

	require.NotNil(t, cont.compressedData)
	assert.Equal(t, str, cont.String())
}

func TestStringContainerNoCompression(t *testing.T) {
	var teststring strings.Builder
	for i := range [100]int{} {
		teststring.WriteString(fmt.Sprintf("%d x", i))
	}
	str := teststring.String()
	cont := NewStringContainer(&str)

	assert.Equal(t, str, cont.stringData)

	if cont.compressedData != nil {
		t.Fatalf("CompressedData should be nil")
	}
}
