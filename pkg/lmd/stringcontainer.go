package lmd

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

const DefaultCompressionMinimumSize = 512

// CompressionMinimumSize sets the minimum number of characters to use compression..
var CompressionMinimumSize = DefaultCompressionMinimumSize

// CompressionLevel sets the minimum number of characters to use compression.
var CompressionLevel = zstd.SpeedDefault

// CompressionThreshold is the minimum ratio threshold to keep compressed data.
var CompressionThreshold = 50.0

// StringContainer wraps large strings.
type StringContainer struct {
	stringData     string
	compressedData []byte
}

// NewStringContainer returns a new StringContainer.
func NewStringContainer(data *string) *StringContainer {
	c := &StringContainer{}
	c.Set(data)

	return c
}

// Set sets the current string.
func (s *StringContainer) Set(data *string) {
	// it only makes sense to compress larger strings
	if len(*data) < CompressionMinimumSize {
		s.stringData = dedup.S(*data)
		s.compressedData = nil

		return
	}
	s.stringData = ""
	compressed, err := compressWithZSTD([]byte(*data))
	if err != nil {
		log.Errorf("%s", err.Error())
		s.stringData = *data
		s.compressedData = nil

		return
	}

	dataLen := len(*data)
	compLen := len(compressed)
	ratio := 100 - (float64(compLen) / float64(dataLen) * 100)

	// only use compressed data if it actually safes space
	if log.IsV(2) {
		log.Tracef("compressed string from %d to %d (%.1f%%) mode:%s min:%d", dataLen, compLen, ratio, CompressionLevel.String(), CompressionMinimumSize)
	}
	if ratio < CompressionThreshold {
		s.stringData = dedup.S(*data)
		s.compressedData = nil
	} else {
		s.compressedData = compressed
	}
}

// String returns the string data.
func (s *StringContainer) String() string {
	if s.compressedData == nil {
		return s.stringData
	}

	return *s.StringRef()
}

// StringRef returns the string data.
func (s *StringContainer) StringRef() *string {
	if s.compressedData == nil {
		return &s.stringData
	}
	decoder, err := zstd.NewReader(bytes.NewReader(s.compressedData))
	if err != nil {
		log.Errorf("failed to create compressed reader: %s", err.Error())
		str := ""

		return &str
	}
	buf, err := io.ReadAll(decoder)
	decoder.Close()
	if err != nil {
		log.Errorf("failed to read compressed data: %s", err.Error())
		str := ""

		return &str
	}

	str := string(buf)

	return &str
}

func compressWithZSTD(input []byte) ([]byte, error) {
	var buf bytes.Buffer
	encoder, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(CompressionLevel))
	if err != nil {
		return nil, fmt.Errorf("compress error: %s", err.Error())
	}

	_, err = encoder.Write(input)
	if err != nil {
		_ = encoder.Close() // Close should be called even on errors

		return nil, fmt.Errorf("compress error: %s", err.Error())
	}

	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("compress error: %s", err.Error())
	}

	return buf.Bytes(), nil
}
