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
	for i := range [1000]int{} {
		teststring.WriteString(fmt.Sprintf("%d x", i))
	}
	str := teststring.String()
	cont := NewStringContainer(&str)

	assert.Equal(t, "", cont.StringData)

	require.NotNil(t, cont.CompressedData)
	assert.Equal(t, str, cont.String())
}

func TestStringContainerNoCompression(t *testing.T) {
	var teststring strings.Builder
	for i := range [100]int{} {
		teststring.WriteString(fmt.Sprintf("%d x", i))
	}
	str := teststring.String()
	cont := NewStringContainer(&str)

	assert.Equal(t, str, cont.StringData)

	if cont.CompressedData != nil {
		t.Fatalf("CompressedData should be nil")
	}
}
