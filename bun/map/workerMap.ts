import { open } from 'fs/promises';
import type { MapResult, WorkerReq, MiniMapResult } from './types';

function processLine(line: string, result: MapResult) {
  const [city, temp] = line.split(';')
  const num = Number(temp)
  const existing = result.get(city)

  if (existing) {
    if (num < existing.min) existing.min = num;
    if (num > existing.max) existing.max = num;
    existing.total += num;
    existing.count += 1;
  } else {
    result.set(city, {
      min: num,
      max: num,
      total: num,
      count: 1,
    })
  }
}

self.onmessage = async (e: { data: WorkerReq }) => {
  const { filePath, i, bufSize } = e.data;
  const file = await open(filePath)
  const { buffer } = await file.read(Buffer.alloc(bufSize), 0, bufSize, bufSize*i)
  const endOfFirstLine = buffer.indexOf(10) + 1;
  const startOfLastLine = buffer.lastIndexOf(10) + 1;
  const lastIndex = buffer.indexOf(0)
  const first = buffer.slice(0, endOfFirstLine)
  const last = buffer.slice(startOfLastLine, lastIndex > 0 ? lastIndex : undefined)
  const result: MapResult = new Map();

  let startIndex = endOfFirstLine;
  while (true) {
    const nextIndex = buffer.indexOf(10, startIndex)
    if (nextIndex < 0) break
    processLine(buffer.toString('utf8', startIndex, nextIndex), result)
    startIndex = nextIndex + 1
  }

  self.postMessage({
    first,
    last,
    result,
    i,
  } satisfies MiniMapResult)
}
