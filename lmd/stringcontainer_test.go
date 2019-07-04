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
	c := NewStringContainer(&str)

	if err := assertEq("", c.StringData); err != nil {
		t.Fatal(err)
	}

	if err := assertNeq(nil, c.CompressedData); err != nil {
		t.Fatal(err)
	}

	if err := assertEq(str, c.String()); err != nil {
		t.Fatal(err)
	}
}

func TestStringContainerNoCompression(t *testing.T) {
	var teststring strings.Builder
	for i := range [100]int{} {
		teststring.WriteString(fmt.Sprintf("%d x", i))
	}
	str := teststring.String()
	c := NewStringContainer(&str)

	if err := assertEq(str, c.StringData); err != nil {
		t.Fatal(err)
	}

	if c.CompressedData != nil {
		t.Fatalf("CompressedData should be nil")
	}
}
