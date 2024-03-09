package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestStringContainerCompression(t *testing.T) {
	var teststring strings.Builder
	for i := range [1000]int{} {
		teststring.WriteString(fmt.Sprintf("%d x", i))
	}
	str := teststring.String()
	cont := NewStringContainer(&str)

	if err := assertEq("", cont.StringData); err != nil {
		t.Fatal(err)
	}

	if err := assertNeq(nil, cont.CompressedData); err != nil {
		t.Fatal(err)
	}

	if err := assertEq(str, cont.String()); err != nil {
		t.Fatal(err)
	}
}

func TestStringContainerNoCompression(t *testing.T) {
	var teststring strings.Builder
	for i := range [100]int{} {
		teststring.WriteString(fmt.Sprintf("%d x", i))
	}
	str := teststring.String()
	cont := NewStringContainer(&str)

	if err := assertEq(str, cont.StringData); err != nil {
		t.Fatal(err)
	}

	if cont.CompressedData != nil {
		t.Fatalf("CompressedData should be nil")
	}
}
