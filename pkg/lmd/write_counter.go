package lmd

import "io"

type WriteCounter struct {
	Writer io.Writer
	Count  int64
}

func NewWriteCounter(w io.Writer) *WriteCounter {
	return &WriteCounter{
		Writer: w,
	}
}

func (wc *WriteCounter) Write(p []byte) (written int, err error) {
	written, err = wc.Writer.Write(p)
	wc.Count += int64(written)

	return
}
