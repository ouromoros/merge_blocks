package main

import (
	"io"
)

type MemReader struct {
	i int
	s []int64
}

func (m *MemReader) ReadNext() (int64, error) {
	if m.i >= len(m.s) {
		return 0, io.EOF
	}
	v := m.s[m.i]
	m.i += 1
	return v, nil
}

func NewMemReader(s []int64) *MemReader {
	return &MemReader{i: 0, s: s}
}

type MemWriter struct {
	s []int64
}

func NewMemWriter() *MemWriter {
	s := make([]int64, 0)
	return &MemWriter{s: s}
}

func (m *MemWriter) WriteNext(i int64) {
	m.s = append(m.s, i)
}

func (m *MemWriter) Data() []int64 {
	return m.s
}
