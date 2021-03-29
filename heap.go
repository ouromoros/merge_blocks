package main

type Node struct {
	index int
	value int64
}

type MergeHeap struct {
	data []Node
}

func (h MergeHeap) Len() int {
	return len(h.data)
}

func (h MergeHeap) Less(i, j int) bool {
	return h.data[i].value < h.data[j].value
}

func (h MergeHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *MergeHeap) Push(x interface{}) {
	h.data = append(h.data, x.(Node))
}

func (h *MergeHeap) Pop() interface{} {
	n := len(h.data)
	ret := h.data[n-1]
	h.data = h.data[:n-1]
	return ret
}
