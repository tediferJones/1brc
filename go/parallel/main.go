package main

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Record struct {
  max float64
  min float64
  total float64
  count int64
}

type Partial struct {
  first []byte
  last []byte
  index int
  miniResult map[string]Record
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
  item, ok := (result)[splitLine[0]]
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
  (result)[splitLine[0]] = item
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

func processChunk(
  filePath string,
  chunkIndex int,
  bufSize int,
  partialLines *[]Partial,
  wg *sync.WaitGroup,
  semaphore chan struct{},
  mutex *sync.Mutex,
) {
  // fmt.Println("Scanning chunk", chunkIndex)
  byteArray := make([]byte, bufSize)
  file, err := os.Open(filePath)
  check(err)
  file.Seek(int64(chunkIndex * int(bufSize)), 0)
  _, err2 := file.Read(byteArray)
  if err2 != nil {
    <- semaphore
    wg.Done()
    fmt.Println("Found read error:", err2)
    return
  }
  check(err2)

  endOfFirstLine := findFirst(byte(10), byteArray) + 1
  startOfLastLine := findLast(byte(10), byteArray) + 1
  split := strings.Split(string(byteArray[endOfFirstLine:startOfLastLine - 1]), "\n")
  miniResult := map[string]Record{}

  for _, val := range split {
    if len(val) > 0 {
      processLine(val, miniResult)
    }
  }

  var prevLine []byte
  var currLine []byte
  mutex.Lock()
  *partialLines = append(*partialLines, Partial{
    first: append(currLine, byteArray[:endOfFirstLine]...),
    last: append(prevLine, byteArray[startOfLastLine:]...),
    // first: byteArray[:endOfFirstLine],
    // last: byteArray[startOfLastLine:],
    index: chunkIndex,
    miniResult: miniResult,
  })
  mutex.Unlock()
  <- semaphore
  defer wg.Done()
  // defer fmt.Println("Done with", chunkIndex)
}

func mergeLines(partialLines []Partial, result map[string]Record) {
  for i := 0; i < len(partialLines); i++ {
    var last []byte
    if i - 1 >= 0 {
      last = partialLines[i - 1].last
    }
    mergedLine := string(append(last, partialLines[i].first...))
    processLine(mergedLine[:len(mergedLine) - 1], result)

    for key := range partialLines[i].miniResult {
      finalItem, finalOk := result[key]
      partialItem, partialOk := partialLines[i].miniResult[key]
      if partialOk == false {
        panic("partial not found")
      }
      if finalOk {
        if finalItem.min > partialItem.min {
          finalItem.min = partialItem.min
        }
        if finalItem.max < partialItem.max {
          finalItem.max = partialItem.max
        }
        finalItem.total += partialItem.total
        finalItem.count += partialItem.count
      } else {
        finalItem = partialItem
      }
      result[key] = finalItem
    }
  }
}

func main() {
  start := time.Now()
  bufSize := int64(1 << 24)
  filePath := "../measurements.txt"

  file, err := os.Open(filePath)
  check(err)

  fileInfo, err := file.Stat()
  check(err)
  size := fileInfo.Size()
  chunkCount := int(math.Ceil(float64(size) / float64(bufSize)))
  // chunkCount := 8
  fmt.Println(size, chunkCount)
  result := map[string]Record{}

  partialLines := []Partial{}
  var mutex sync.Mutex
  var wg sync.WaitGroup
  // maxGoRoutines := 16
  maxGoRoutines := 2
  semaphore := make(chan struct{}, maxGoRoutines)

  for i := 0; i < chunkCount; i++ {
    wg.Add(1)
    semaphore <- struct{}{}
    go processChunk(
      filePath,
      i,
      int(bufSize),
      &partialLines,
      &wg,
      semaphore,
      &mutex,
      )
  }
  fmt.Println("Waiting for go routines")
  wg.Wait()
  fmt.Println("All Go routines are done")

  sort.Slice(partialLines, func(i, j int) bool {
    return partialLines[i].index < partialLines[j].index
  })
  mergeLines(partialLines, result)

  myAnswer := prettyPrint(result)

  fmt.Println(time.Now().Sub(start))

  content, err := os.ReadFile("../notes/answer.txt")
  check(err)
  fmt.Println(string(content) == (myAnswer + "\n"))
}
