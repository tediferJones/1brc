package main

import (
	"fmt"
	"os"
  "math"
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

func main() {
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

  for i := 1; i <= chunkCount; i++ {
    fmt.Println("Scanning chunk", i)
    byteArray := make([]byte, bufSize)
    _, err2 := file.Read(byteArray)
    check(err2)

    endOfFirstLine := findFirst(byte(10), byteArray)
    startOfLastLine := findLast(byte(10), byteArray)
    fmt.Println(endOfFirstLine, startOfLastLine)

    // fmt.Println(string(byteArray))

    break
  }

  // byteArray := make([]byte, bufSize)
  // _, err2 := file.Read(byteArray)
  // check(err2)
  // fmt.Printf(string(byteArray) + "\n")
}
