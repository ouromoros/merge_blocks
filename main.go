package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

func groupFiles(parentDir string, files []os.FileInfo, maxSize int64) [][]string {
	resultGroups := make([][]string, 0)
	group := make([]string, 0)
	var groupSize int64 = 0
	for _, f := range files {
		if groupSize+f.Size() > maxSize {
			resultGroups = append(resultGroups, group)
			group = make([]string, 0)
			groupSize = 0
		}
		path := filepath.Join(parentDir, f.Name())
		group = append(group, path)
		groupSize += f.Size()
	}
	if len(group) > 0 {
		resultGroups = append(resultGroups, group)
	}
	return resultGroups
}

func clearFiles(fs []string) {
	for _, f := range fs {
		err := os.Remove(f)
		if err != nil {
			fmt.Println("deleting ", f, " failed")
		}
	}
}

func getTempFile() string {
	tempFile, err := ioutil.TempFile("", "merge_temp")
	if err != nil {
		panic("create tempfile failed")
	}
	err = tempFile.Close()
	if err != nil {
		panic("close tempfile failed")
	}
	return tempFile.Name()
}

func RunTwoPass(inputDir string, outputPath string, maxBatchSize int64, numCores int, bufferSize int) {
	inFiles, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic("ReadDir failed")
	}

	grouped := groupFiles(inputDir, inFiles, maxBatchSize)
	tempResults := make([]string, 0)
	for _, fs := range grouped {
		tempFileName := getTempFile()
		mergeSmallFiles(fs, tempFileName, numCores)
		tempResults = append(tempResults, tempFileName)
	}

	mergeLargeFiles(tempResults, outputPath, bufferSize)

	clearFiles(tempResults)
}

func RunOnePass(inputDir string, outputPath string, bufferSize int) {
	inFiles, err := ioutil.ReadDir(inputDir)
	if err != nil {
		panic("ReadDir failed")
	}
	inPaths := make([]string, 0)
	for _, f := range inFiles {
		inPaths = append(inPaths, filepath.Join(inputDir, f.Name()))
	}

	mergeLargeFiles(inPaths, outputPath, bufferSize)
}


func generateSortedArray(r *rand.Rand,size int) []int64 {
	result := make([]int64, 0)
	for i := 0; i < size; i++ {
		result = append(result, r.Int63())
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j]})
	return result
}

func generateTestArrays(outputDir string, blockNum int, blockSize int) {
	if _, err := os.Stat(outputDir); !os.IsNotExist(err) {
		os.RemoveAll(outputDir)
	}
	os.Mkdir(outputDir, 0777)
	s := rand.NewSource(42)
	r := rand.New(s)
	for i := 0; i < blockNum; i++ {
		a := generateSortedArray(r, blockSize / 8)
		tempInput, _ := ioutil.TempFile(outputDir, "test_input")
		tempInput.Close()
		writeSliceToFile(a, tempInput.Name())
	}
}

func main() {
	if len(os.Args) < 4 {
		panic("input incorrect")
	}
	command := os.Args[1]
	startTime := time.Now()
	if command == "generate" {
		outputDir := os.Args[2]
		blockNum, err := strconv.Atoi(os.Args[3])
		if err != nil {
			panic("read blockNum error")
		}
		blockSize, err := strconv.Atoi(os.Args[4])
		if err != nil {
			panic("read blockSize error")
		}
		generateTestArrays(outputDir, blockNum, blockSize)
	} else if command == "onepass" {
		inputDir := os.Args[2]
		outFile := os.Args[3]
		bufferSize, _ := strconv.Atoi(os.Getenv("BUFFER_SIZE"))
		fmt.Printf("bufferSize=%v onepass\n", bufferSize)
		RunOnePass(inputDir, outFile, bufferSize)
	} else if command == "twopass" {
		inputDir := os.Args[2]
		outFile := os.Args[3]
		maxBatchSize, err := strconv.Atoi(os.Getenv("MAX_BATCH_SIZE"))
		if err != nil {
			panic("read MAX_BATCH_SIZE error")
		}
		bufferSize, err := strconv.Atoi(os.Getenv("BUFFER_SIZE"))
		if err != nil {
			panic("read BUFFER_SIZE error")
		}
		numCores, err := strconv.Atoi(os.Getenv("NUM_CORES"))
		if err != nil {
			panic("read NUM_CORES error")
		}
		fmt.Printf("maxBatchSize=%v bufferSize=%v numCores=%v twopass\n", maxBatchSize, bufferSize, numCores)
		RunTwoPass(inputDir, outFile, int64(maxBatchSize), numCores, bufferSize)
	} else {
		panic("unknown command")
	}

	elapsed := time.Since(startTime)
	fmt.Printf("finished. time elapsed: %v\n", elapsed)
}
