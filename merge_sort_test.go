package main

import (
	"math/rand"
	"testing"
)

func TestSliceMergeSort(t *testing.T) {
	testCase := [][]int64{
		{1 , 220, 1000},
		{-20, 23, 2999},
		{-100, 100, 23442, 325253, 325353},
		{},
	}
	expected := []int64{-100, -20, 1, 23, 100, 220, 1000, 2999, 23442, 325253, 325353}
	result := sliceMergeSort(testCase)
	if len(result) != 11 {
		t.Errorf("%v length error", result)
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("%v not equal to %v", result, expected)
		}
	}
}

func TestParallelSort(t *testing.T) {
	testCase := [][]int64{
		{1 , 220, 1000},
		{-20, 23, 2999},
		{-100, 100, 23442, 325253, 325353},
		{1},
	}
	expected := []int64{-100, -20, 1, 1, 23, 100, 220, 1000, 2999, 23442, 325253, 325353}
	result := parallelMergeSort(testCase, 2)
	if len(result) != 12 {
		t.Errorf("%v length error", result)
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("%v not equal to %v", result, expected)
		}
	}
}

func BenchmarkParalleMerge8Core(b *testing.B) {
	s := rand.NewSource(42)
	r := rand.New(s)
	arrayNum := 100
	arraySize := 100000
	arrays := make([][]int64, 0)
	for i := 0; i < arrayNum; i++ {
		a := generateSortedArray(r, arraySize)
		arrays = append(arrays, a)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parallelMergeSort(arrays, 8)
	}
}

func BenchmarkParallelMergeSingleCore(b *testing.B) {
	s := rand.NewSource(42)
	r := rand.New(s)
	arrayNum := 100
	arraySize := 100000
	arrays := make([][]int64, 0)
	for i := 0; i < arrayNum; i++ {
		a := generateSortedArray(r, arraySize)
		arrays = append(arrays, a)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parallelMergeSort(arrays, 1)
	}
}
