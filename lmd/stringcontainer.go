package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

// StringContainer wraps large strings
type StringContainer struct {
	StringData     string
	CompressedData *[]byte
}

// NewStringContainer returns a new StringContainer
func NewStringContainer(data *string) *StringContainer {
	c := &StringContainer{}
	c.Set(data)
	return c
}

// Set sets the current string
func (s *StringContainer) Set(data *string) {
	// it only makes sense to compress larger strings
	if len(*data) < 1000 {
		s.StringData = *data
		s.CompressedData = nil
		return
	}
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	gz.Write([]byte(*data))
	gz.Close()
	s.StringData = ""
	by := b.Bytes()
	s.CompressedData = &by
	if log.IsV(2) {
		log.Tracef("compressed string from %d to %d (%.1f%%)", len(*data), len(s.StringData), 100-(float64(len(s.StringData))/float64(len(*data))*100))
	}
}

// String returns the string data
func (s *StringContainer) String() string {
	if s.CompressedData == nil {
		return s.StringData
	}
	rdata := bytes.NewReader(*s.CompressedData)
	r, _ := gzip.NewReader(rdata)
	b, _ := ioutil.ReadAll(r)
	str := string(b)
	return str
}

// StringRef returns the string data
func (s *StringContainer) StringRef() *string {
	if s.CompressedData == nil {
		return &s.StringData
	}
	rdata := bytes.NewReader(*s.CompressedData)
	r, _ := gzip.NewReader(rdata)
	b, _ := ioutil.ReadAll(r)
	str := string(b)
	return &str
}
