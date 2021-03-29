package main

import (
	"container/heap"
	"testing"
)

func TestMergeHeap(t *testing.T) {
	nodes := make([]Node, 0)
	h := &MergeHeap{data: nodes}

	for _, n := range []int64{5, 3, 1, -1, 6} {
		heap.Push(h, Node{index: int(n)%2, value: n})
	}

	expected := []int64{-1, 1, 3, 5, 6}
	result := make([]int64, 0)
	for i := 0; i < 5; i++ {
		n := heap.Pop(h).(Node)
		result = append(result, n.value)
	}

	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("%v not equal to %v", result, expected)
		}
	}
}
