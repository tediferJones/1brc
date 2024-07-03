import { open } from 'fs/promises';

interface Record {
  min: number,
  max: number,
  count: number,
  total: number,
}

type MapResult = Map<string, Record>

interface MiniMapResult {
  first: Buffer,
  last: Buffer,
  result: MapResult,
}

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

function format(result: MapResult) {
  return `{${[...result.keys()].sort().reduce((str, key) => {
    const rec = result.get(key)
    if (!rec) throw Error('cant find key')
    return str + `${key}=${rec.min.toFixed(1)}/${(rec.total / rec.count).toFixed(1)}/${rec.max.toFixed(1)}, `
  }, '').slice(0, -2)}}\n`
}

const bufSize = 2**24;
const workerCount = 8;

class MyWorker {
  worker: Worker;
  promiseInfo?: {
    promise: Promise<MiniMapResult>,
    resolve: Function,
    reject: Function,
  };
  finalResult: MapResult;

  constructor(results: MiniMapResult[], finalResult: MapResult) {
    const worker = new Worker('./workerMap.ts')
    worker.onmessage = (e) => {
      results[e.data.i] = e.data
      this.promiseInfo?.resolve()
      this.promiseInfo = undefined;
    }
    worker.onerror = (err) => { throw err }
    this.worker = worker
    this.finalResult = finalResult
  }

  sendMsg(msgObj: { filePath: string, i: number, bufSize: number }) {
    if (this.promiseInfo) throw Error('promise already exists')
    this.worker.postMessage(msgObj)
    let resolve, reject;
    const promise = new Promise<MiniMapResult>((res, rej) => {
      [ resolve, reject ] = [ res, rej ];
    })
    if (!resolve || !reject) throw Error('cant find resolve and reject')
    this.promiseInfo = { promise, resolve, reject }
  }
}

function mergeResults(miniResult: MapResult, results: MapResult) {
  miniResult.forEach((val, key) => {
    const final = results.get(key)
    if (final) {
      if (val.min < final.min) final.min = val.min;
      if (val.max > final.max) final.max = val.max;
      final.count += val.count;
      final.total += val.total;
    } else {
      results.set(key, val);
    }
  })
}

export async function runMap(filePath: string) {
  const file = await open(filePath)
  const chunkCount = Math.ceil((await file.stat()).size / bufSize)
  const results: MiniMapResult[] = []
  const finalResult: MapResult = new Map();

  const myWorkers: MyWorker[] = [
    ...Array(workerCount).keys()
  ].map(() => new MyWorker(results, finalResult))

  for (let i = 0; i < chunkCount; i++) {
    let worker = myWorkers.find(worker => !worker.promiseInfo);
    if (!worker) {
      await Promise.race(myWorkers.map(worker => worker.promiseInfo?.promise));
      worker = myWorkers.find(worker => !worker.promiseInfo);
    }
    if (!worker) throw Error('no worker')
    worker.sendMsg({ filePath, i, bufSize })
  }

  await Promise.all(myWorkers.map(worker => worker.promiseInfo?.promise));
  myWorkers.forEach(worker => worker.worker.terminate());

  for (let i = 0; i < results.length; i++) {
    // const prev = Buffer.from(results[i - 1]?.last || '')
    // const current = Buffer.from(results[i].first)
    // const tempBuffer = Buffer.alloc((prev.length) + current.length);
    // prev.copy(tempBuffer)
    // current.copy(tempBuffer, prev.length)
    // processLine(tempBuffer.toString('utf8', 0, tempBuffer.length - 1), finalResult)
    // Object.keys(results[i].result).forEach(key => {
    //   const current = results[i].result;
    //   if (finalResult[key]) {
    //     if (current[key].min < finalResult[key].min) {
    //       finalResult[key].min = current[key].min
    //     }
    //     if (current[key].max > finalResult[key].max) {
    //       finalResult[key].max = current[key].max
    //     }
    //     finalResult[key].count += current[key].count
    //     finalResult[key].total += current[key].total
    //   } else {
    //     finalResult[key] = current[key]
    //   }
    // })

    processLine(
      Buffer.concat([
        results[i - 1]?.last || new Uint8Array(),
        results[i].first
      ]).toString('utf8'),
      finalResult,
    )
    mergeResults(results[i].result, finalResult)
  }
  return format(finalResult)
}

const start = Bun.nanoseconds();
const result = await runMap('../measurements.txt');
const end = Bun.nanoseconds();
console.log(`${(end - start) / 10**9}`)
console.log(`${result}\n` === (await Bun.file('../notes/answer.txt').text()))

// Best times:
// 93 seconds, 8 workers, 2**24 bufSize
// 94 seconds, 8 workers, 2**24 bufSize
