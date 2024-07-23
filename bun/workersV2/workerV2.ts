import { open } from 'fs/promises'

interface Result {
  [key: string]: {
    min: number,
    max: number,
    count: number,
    total: number,
  }
}

function processLine(line: string, result: Result) {
  const [city, temp] = line.split(';')
  const num = Number(temp)

  if (result[city]) {
    const rec = result[city];
    if (num < rec.min) rec.min = num;
    if (num > rec.max) rec.max = num;
    rec.total += num;
    rec.count += 1;
  } else {
    result[city] = {
      min: num,
      max: num,
      total: num,
      count: 1,
    }
  }
}

function processLineV2(city: string, temp: string, result: Result) {
  const num = Number(temp)

  if (result[city]) {
    const rec = result[city];
    if (num < rec.min) rec.min = num;
    if (num > rec.max) rec.max = num;
    rec.total += num;
    rec.count += 1;
  } else {
    result[city] = {
      min: num,
      max: num,
      total: num,
      count: 1,
    }
  }
}

self.onmessage = async (e) => {
  const { filePath, i, bufSize } = e.data;
  const file = await open(filePath)
  const { buffer } = await file.read(Buffer.alloc(bufSize), 0, bufSize, bufSize*i)
  const endOfFirstLine = buffer.indexOf(10) + 1;
  const startOfLastLine = buffer.lastIndexOf(10) + 1;
  const first = buffer.slice(0, endOfFirstLine)
  const last = buffer.slice(startOfLastLine)
  const result: Result = {};
  // buffer.slice(endOfFirstLine, startOfLastLine).toString('utf8').split('\n').forEach(line => {
  // buffer.toString('utf8', endOfFirstLine, startOfLastLine).split('\n').forEach(line => {
  //   if (!line) return
  //   processLine(line, result)
  // })

  let startIndex = endOfFirstLine;
  while (true) {
    const nextIndex = buffer.indexOf(10, startIndex)
    if (nextIndex < 0) break
    processLine(buffer.toString('utf8', startIndex, nextIndex), result)
    startIndex = nextIndex + 1
  }

  // let startIndex = endOfFirstLine;
  // while (true) {
  //   const nextIndex = buffer.indexOf(10, startIndex)
  //   const midIndex = buffer.indexOf(59, startIndex)
  //   if (nextIndex < 0) break
  //   processLineV2(
  //     buffer.toString('utf8', startIndex, midIndex),
  //     buffer.toString('utf8', midIndex + 1, nextIndex),
  //     result
  //   )
  //   startIndex = nextIndex + 1
  // }

  // [
  //   ...buffer.toString(
  //     'utf8',
  //     endOfFirstLine,
  //     startOfLastLine
  //   ).matchAll(/([^;]+);([^\n]+)\n/g)
  // ].forEach(match => {
  //     const city = match[1];
  //     const temp = match[2];
  //     const num = Number(temp)

  //     if (result[city]) {
  //       const rec = result[city];
  //       if (num < rec.min) rec.min = num;
  //       if (num > rec.max) rec.max = num;
  //       rec.total += num;
  //       rec.count += 1;
  //     } else {
  //       result[city] = {
  //         min: num,
  //         max: num,
  //         total: num,
  //         count: 1,
  //       }
  //     }
  // })
  
  self.postMessage({
    first,
    last,
    result,
    i,
  })
}
