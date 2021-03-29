package main

import (
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"
)

func sortArrays(arrays [][]int64) []int64 {
	merged := make([]int64, 0)
	for _, a := range arrays {
		merged = append(merged, a...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j]})
	return merged
}

func readSliceFromFile(f string) []int64 {
	r := NewFileReader(f, 1024 * 1024)
	return r.ReadAll()
}

func TestRun(t *testing.T) {
	s := rand.NewSource(42)
	r := rand.New(s)
	blockSize := 1024 * 8
	blockNum := 100
	bufferSize := 1024 * 8 * 20
	var maxBatchSize int64 = 1024 * 8 * 20

	arrays := make([][]int64, 0)
	inputFiles := make([]string, 0)
	inputDir, _ := ioutil.TempDir("", "test_input_dir")
	for i := 0; i < blockNum; i++ {
		a := generateSortedArray(r, blockSize / 8)
		tempInput, _ := ioutil.TempFile(inputDir, "test_input")
		tempInput.Close()
		writeSliceToFile(a, tempInput.Name())
		inputFiles = append(inputFiles, tempInput.Name())
		arrays = append(arrays, a)
	}
	tempOutput, _ := ioutil.TempFile(inputDir, "test_output")
	tempOutput.Close()
	outFile := tempOutput.Name()
	expected := sortArrays(arrays)

	RunTwoPass(inputDir, outFile, maxBatchSize, 8, bufferSize)

	result := readSliceFromFile(outFile)
	os.Remove(outFile)
	os.RemoveAll(inputDir)

	if len(expected) != len(result) {
		t.Errorf("length error")
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("index %v value %v not equal to %v", i, result[i], expected[i])
		}
	}
}

func BenchmarkRun(b *testing.B){
	s := rand.NewSource(42)
	r := rand.New(s)
	blockSize := 1024 * 1024 *  1
	blockNum := 100
	bufferSize := 1024 * 1024 * 1
	var maxBatchSize int64 = 1024 * 1024 * 4

	inputFiles := make([]string, 0)
	inputDir, _ := ioutil.TempDir("", "test_input_dir")
	for i := 0; i < blockNum; i++ {
		a := generateSortedArray(r, blockSize / 8)
		tempInput, _ := ioutil.TempFile(inputDir, "test_input")
		tempInput.Close()
		writeSliceToFile(a, tempInput.Name())
		inputFiles = append(inputFiles, tempInput.Name())
	}
	tempOutput, _ := ioutil.TempFile(inputDir, "test_output")
	tempOutput.Close()
	outFile := tempOutput.Name()

	for i := 0; i < b.N; i++ {
		RunTwoPass(inputDir, outFile, maxBatchSize, 8, bufferSize)
	}

	os.Remove(outFile)
	os.RemoveAll(inputDir)
}
