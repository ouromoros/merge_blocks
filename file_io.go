package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type FileReader struct {
	f *os.File
	r *bufio.Reader
}

func NewFileReader(fname string, bufferSize int) *FileReader {
	f, err := os.Open(fname)
	if err != nil {
		panic("openfile error")
	}
	r := bufio.NewReaderSize(f, bufferSize)
	return &FileReader{f: f, r: r}
}

func (fr *FileReader) ReadNext() (int64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(fr.r, buf)
	if err != nil {
		return 0, err
	}
	return toInt64(buf), nil
}

func (fr *FileReader) ReadAll() []int64 {
	result := make([]int64, 0)
	buf := make([]byte, 8)
	for {
		_, err := io.ReadFull(fr.r, buf)
		if err == nil {
			result = append(result, toInt64(buf))
		} else if err == io.EOF {
			return result
		} else {
			panic("read error")
		}
	}
}

func (fr *FileReader) Close() {
	err := fr.f.Close()
	if err != nil {
		panic("closefile error")
	}
}

type FileWriter struct {
	f *os.File
	w *bufio.Writer
}

func NewFileWriter(fname string, bufferSize int) *FileWriter {
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic("openfile error")
	}
	w := bufio.NewWriterSize(f, bufferSize)
	return &FileWriter{f: f, w: w}
}

func (fw *FileWriter) WriteNext(n int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	_, err := fw.w.Write(buf)
	if err != nil {
		panic("write failed")
	}
}

func (fw *FileWriter) Close() {
	err := fw.w.Flush()
	if err != nil {
		panic("flush failed")
	}
	err = fw.f.Close()
	if err != nil {
		panic("close failed")
	}
}
