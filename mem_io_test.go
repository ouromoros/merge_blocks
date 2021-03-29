package main

import (
	"io"
	"testing"
)

func TestMemReader(t *testing.T) {
	testCases := [][]int64{
		{},
		{1, 2, 3, 4},
		{-1, -20, 12024, -2342},
		make([]int64, 1000),
	}
	for _, testSlice := range testCases {
		mr := NewMemReader(testSlice)
		result := make([]int64, 0)
		for {
			v, err := mr.ReadNext()
			if err == nil {
				result = append(result, v)
			} else if err == io.EOF {
				break
			} else {
				t.Errorf("unexpected error %v", err)
			}
		}
		if len(result) != len(testSlice) {
			t.Errorf("%v not equal to %v", result, testSlice)
		}
		for i := range result {
			if result[i] != testSlice[i] {
				t.Errorf("%v not equal to %v", result, testSlice)
			}
		}
	}
}

func TestMemWriter(t *testing.T) {
	testCases := [][]int64{
		{},
		{1, 2, 3, 4},
		{-1, -20, 12024, -2342},
		make([]int64, 1000),
	}
	for _, testSlice := range testCases {
		mw := NewMemWriter()
		for _, n := range testSlice {
			mw.WriteNext(n)
		}
		result := mw.Data()

		if len(result) != len(testSlice) {
			t.Errorf("%v not equal to %v", result, testSlice)
		}
		for i := range result {
			if result[i] != testSlice[i] {
				t.Errorf("%v not equal to %v", result, testSlice)
			}
		}
	}
}
