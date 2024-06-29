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

self.onmessage = async (e) => {
  const { filePath, i, bufSize } = e.data;
  const file = await open(filePath)
  const { buffer } = await file.read(Buffer.alloc(bufSize), 0, bufSize, bufSize*i)
  const endOfFirstLine = buffer.indexOf(10) + 1;
  const startOfLastLine = buffer.lastIndexOf(10) + 1;
  const first = buffer.slice(0, endOfFirstLine)
  const last = buffer.slice(startOfLastLine)
  const result: Result = {}
  buffer.slice(endOfFirstLine, startOfLastLine).toString('utf8').split('\n').forEach(line => {
    if (!line) return
    processLine(line, result)
  })
  self.postMessage({
    first,
    last,
    result
  })
}
