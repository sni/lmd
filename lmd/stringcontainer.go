package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

// StringContainer wraps large strings
type StringContainer struct {
	Data       string
	compressed bool
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
		s.Data = *data
		s.compressed = false
		return
	}
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	gz.Write([]byte(*data))
	gz.Close()
	s.Data = b.String()
	s.compressed = true
	if log.IsV(2) {
		log.Tracef("compressed string from %d to %d (%.1f%%)", len(*data), len(s.Data), 100-(float64(len(s.Data))/float64(len(*data))*100))
	}
}

// String returns the string data
func (s *StringContainer) String() string {
	if !s.compressed {
		return s.Data
	}
	rdata := bytes.NewReader([]byte(s.Data))
	r, _ := gzip.NewReader(rdata)
	b, _ := ioutil.ReadAll(r)
	str := string(b)
	return str
}

// StringRef returns the string data
func (s *StringContainer) StringRef() *string {
	if !s.compressed {
		return &s.Data
	}
	rdata := bytes.NewReader([]byte(s.Data))
	r, _ := gzip.NewReader(rdata)
	b, _ := ioutil.ReadAll(r)
	str := string(b)
	return &str
}
