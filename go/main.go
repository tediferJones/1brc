package main

import (
	"fmt"
	"math"
	"os"
	"sort"
	"time"
	"strconv"
	"strings"
)

type Record struct {
  max float64
  min float64
  total float64
  count int64
}

// type MiniResult struct {
//   First []byte
//   Last []byte
//   Result map[string]Record
// }

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
    item = Record{
      min: num,
      max: num,
      total: num,
      count: 1,
    }
  }
  result[splitLine[0]] = item
}

func getSortedKeys(result map[string]Record) []string {
  var keys []string
  for key := range result {
    keys = append(keys, key)
  }
  sort.Strings(keys)
  return keys
}

func roundFloat(num float64) float32 {
  return float32(math.Round(num * 10) / 10)
}

func prettyPrint(result map[string]Record) string {
  finalString := "{"
  for _, key := range getSortedKeys(result) {
    finalString += fmt.Sprintf(
      "%s=%.1f/%.1f/%.1f, ",
      key,
      roundFloat(result[key].min),
      roundFloat(result[key].total / float64(result[key].count)),
      roundFloat(result[key].max),
      )
  }
  return finalString[:len(finalString) - 2] + "}\n"
}

func main() {
  start := time.Now()
  bufSize := int64(1 << 24)

  file, err := os.Open("../measurements.txt")
  check(err)

  fileInfo, err := file.Stat()
  check(err)
  size := fileInfo.Size()
  chunkCount := int(math.Ceil(float64(size) / float64(bufSize)))
  fmt.Println(size, chunkCount)
  result := map[string]Record{}
  var prevLine []byte
  var currLine []byte

  for i := 1; i <= chunkCount; i++ {
    // if (i > 3) { break }
    fmt.Println("Scanning chunk", i)
    byteArray := make([]byte, bufSize)
    _, err2 := file.Read(byteArray)
    check(err2)
    file.Seek(int64(i * int(bufSize)), 0)

    endOfFirstLine := findFirst(byte(10), byteArray) + 1
    startOfLastLine := findLast(byte(10), byteArray)
    split := strings.Split(string(byteArray[endOfFirstLine:startOfLastLine]), "\n")

    currLine = append(currLine, byteArray[:endOfFirstLine]...)
    fullLine := string(append(prevLine, currLine...))
    processLine(fullLine[:len(fullLine) - 1], result)
    prevLine = byteArray[startOfLastLine + 1:]
    currLine = []byte{}

    for _, val := range split {
      processLine(val, result)
    }
  }

  myAnswer := prettyPrint(result)

  fmt.Print(myAnswer)
  fmt.Println(time.Now().Sub(start))

  content, err := os.ReadFile("../notes/answer.txt")
  check(err)
  fmt.Println(string(content) == (myAnswer + "\n"))
}
