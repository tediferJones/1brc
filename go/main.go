package main

import (
	"fmt"
	"math"
	"os"
	"time"

	// "slices"
	"strconv"
	"strings"
)

type Record struct {
  max float64
  min float64
  total float64
  count int64
}

type MiniResult struct {
  First []byte
  Last []byte
  Result map[string]Record
}

func check(e error) {
  if e != nil {
    panic(e)
  }
}

func findFirst(findVal byte, bytes []byte) int {
  for i, val := range bytes {
    if val == findVal {
      return i
    }
  }
  return -1
}

func findLast(findVal byte, bytes []byte) int {
  for i := range bytes {
    index := len(bytes) - i - 1
    if bytes[index] == findVal {
      return index
    }
  }
  return -1
}

func processLine(val string, result map[string]Record) {
  splitLine := strings.Split(val, ";")
  item, ok := result[splitLine[0]]
  num, err := strconv.ParseFloat(splitLine[1], 64)
  check(err)
  if ok {
    if item.min > num {
      item.min = num
    }
    if item.max < num {
      item.max = num
    }
    item.total += num
    item.count += 1
  } else {
    result[splitLine[0]] = Record{
      min: num,
      max: num,
      total: num,
      count: 1,
    }
  }
}

func main() {
  start := time.Now()
  bufSize := int64(1 << 24)

  // file, err := os.ReadFile("../measurements.txt")
  file, err := os.Open("../measurements.txt")
  check(err)

  fileInfo, err := file.Stat()
  check(err)
  size := fileInfo.Size()
  chunkCount := int(math.Ceil(float64(size) / float64(bufSize)))
  fmt.Println(size, chunkCount)
  // miniResults := []map[string]Record{}
  result := map[string]Record{}
  prevLine := make([]byte, 128)
  currLine := make([]byte, 128)

  for i := 1; i <= chunkCount; i++ {
    fmt.Println("Scanning chunk", i)
    byteArray := make([]byte, bufSize)
    _, err2 := file.Read(byteArray)
    check(err2)
    file.Seek(int64(i * int(bufSize)), 0)

    endOfFirstLine := findFirst(byte(10), byteArray) + 1
    startOfLastLine := findLast(byte(10), byteArray)
    split := strings.Split(string(byteArray[endOfFirstLine:startOfLastLine]), "\n")

    // fmt.Println(endOfFirstLine, startOfLastLine)
    // fmt.Println("first:", split[0], "last:", split[len(split) - 1])

    currLine = append(currLine, byteArray[:endOfFirstLine]...)
    // fmt.Println(string(currLine), string(prevLine))
    // fullLine := string(append(currLine, prevLine[:len(prevLine) - 2]...))
    // fmt.Println(fullLine)
    // processLine(fullLine, result)

    prevLine = append(prevLine, byteArray[startOfLastLine:]...)

    // we need to do some resetting of firstLine and/or lastLine around here

    for _, val := range split {
      processLine(val, result)
    }
  }
  fmt.Println(time.Now().Sub(start))
}
