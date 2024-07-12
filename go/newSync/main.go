package main

import (
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

type Record struct {
  max float64
  min float64
  total float64
  count int64
}

func check(e error) {
  if e != nil {
    panic(e)
  }
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
  chunkNum int,
  filePath string,
  startIndex *int,
  result *map[string]Record,
) {
  // miniResult := map[string]Record{}
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
  startIndex := 0
  result := map[string]Record{}

  // We want to scan each chunk up to the last occurence of \n char
  // Then set the index of \n as the start value for the next chunk
  // This way we dont need worry about handling partial lines

  for i := 0; i < chunkCount; i++ {
    processChunk(i, filePath, &startIndex, &result)
  }

  myAnswer := prettyPrint(result)

  fmt.Println(time.Now().Sub(start))

  content, err := os.ReadFile("../notes/answer.txt")
  check(err)
  fmt.Println(string(content) == (myAnswer + "\n"))
}
