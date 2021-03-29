package main

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"io"
	"io/ioutil"
	"sync"
)

type Reader interface {
	ReadNext() (int64, error)
}

type Writer interface {
	WriteNext(n int64)
}

func writeSliceToFile(s []int64, fname string) {
	fw := NewFileWriter(fname, 1024 * 1024)
	for _, n := range s {
		fw.WriteNext(n)
	}
	fw.Close()
}

func mergeSmallFiles(fs []string, outFile string, numCores int) {
	arrays := make([][]int64, 0)
	for _, f := range fs {
		data, err := ioutil.ReadFile(f)
		if err != nil {
			panic("read file error")
		}
		arrays = append(arrays, toInt64Slice(data))
	}
	mergeResult := parallelMergeSort(arrays, numCores)
	writeSliceToFile(mergeResult, outFile)
}

func mergeLargeFiles(fs []string, outFile string, bufferSize int) {
	frs := make([]Reader, 0)
	for _, fname := range fs {
		fr := NewFileReader(fname, bufferSize)
		frs = append(frs, fr)
	}
	fw := NewFileWriter(outFile, bufferSize)
	doMerge(frs, fw)
	for _, fr := range frs {
		fr.(*FileReader).Close()
	}
	fw.Close()
}

func toInt64Slice(data []byte) []int64 {
	s := make([]int64, 0)
	for i := 0; i < len(data); i += 8 {
		v := toInt64(data[i : i+8])
		s = append(s, v)
	}
	return s
}

func toInt64(data []byte) int64 {
	var v int64
	_ = binary.Read(bytes.NewReader(data), binary.BigEndian, &v)
	return v
}

func getBatches(total int, num int) []int {
	batches := make([]int, 0)
	batchSize := total / num
	remain := total
	for i := 0; i < num-1; i++ {
		batches = append(batches, batchSize)
		remain -= batchSize
	}
	batches = append(batches, remain)
	return batches
}

func intMin(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func parallelMergeSort(sortedArrays [][]int64, parallelNum int) []int64 {
	offset := 0
	var wg sync.WaitGroup
	batchNum := intMin(len(sortedArrays), parallelNum)
	sortedBatches := make([][]int64, batchNum)
	for i, batchSize := range getBatches(len(sortedArrays), batchNum) {
		arrBatch := sortedArrays[offset : offset+batchSize]
		offset += batchSize
		wg.Add(1)
		go func(i int, arrBatch [][]int64) {
			sorted := sliceMergeSort(arrBatch)
			sortedBatches[i] = sorted
			defer wg.Done()
		}(i, arrBatch)
	}

	wg.Wait()
	if len(sortedBatches) == 1 {
		return sortedBatches[0]
	}
	finalResult := sliceMergeSort(sortedBatches)
	return finalResult
}

func sliceMergeSort(s [][]int64) []int64 {
	mrs := make([]Reader, 0)
	for i := range s {
		mrs = append(mrs, NewMemReader(s[i]))
	}
	w := NewMemWriter()
	doMerge(mrs, w)
	return w.Data()
}

func doMerge(readers []Reader, writer Writer) {
	initNodes := make([]Node, 0)
	for i, r := range readers {
		v, err := r.ReadNext()
		if err == nil {
			initNodes = append(initNodes, Node{index: i, value: v})
		} else if err == io.EOF {
		} else {
			panic("readNext error")
		}
	}
	h := &MergeHeap{data: initNodes}
	heap.Init(h)

	for h.Len() > 0 {
		next := (heap.Pop(h)).(Node)
		writer.WriteNext(next.value)
		r := readers[next.index]
		v, err := r.ReadNext()
		if err == nil {
			heap.Push(h, Node{index: next.index, value: v})
		} else if err == io.EOF {
		} else {
			panic("readNext error")
		}
	}
}
