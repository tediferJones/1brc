import { open } from 'fs/promises';

interface Result {
  [key: string]: {
    min: number,
    max: number,
    count: number,
    total: number,
  }
}

interface MiniResult {
  first: Buffer,
  last: Buffer,
  result: Result,
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

function format(result: Result) {
  return `{${Object.keys(result).sort().reduce((str, key) => {
    const rec = result[key]
    return str + `${key}=${rec.min.toFixed(1)}/${(rec.total / rec.count).toFixed(1)}/${rec.max.toFixed(1)}, `
  }, '').slice(0, -2)}}\n`
}

// const bufSize = 2**31;
const bufSize = 2**24;
const workerCount = 8;

class MyWorker {
  worker: Worker;
  promiseInfo?: {
    promise: Promise<MiniResult>,
    resolve: Function,
    reject: Function,
  };
  finalResult: Result;

  constructor(results: MiniResult[], finalResult: Result) {
    const worker = new Worker('./workerV2.ts')
    worker.onmessage = (e) => {
      // process.stdout.clearLine(0);
      // process.stdout.cursorTo(0);
      // process.stdout.write(`${++isDone}`)
      results[e.data.i] = e.data
      // mergeResults(e.data.result, finalResult)
      // delete e.data.result
      // results[e.data.i] = e.data
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
    const promise = new Promise<MiniResult>((res, rej) => {
      [ resolve, reject ] = [ res, rej ];
    })
    if (!resolve || !reject) throw Error('cant find resolve and reject')
    this.promiseInfo = { promise, resolve, reject }
  }
}

function mergeResults(miniResult: Result, results: Result) {
  // const prev = Buffer.from(miniResult.last || '')
  // const current = Buffer.from(miniResult.first)
  // const tempBuffer = Buffer.alloc((prev.length) + current.length);
  // prev.copy(tempBuffer)
  // current.copy(tempBuffer, prev.length)
  // processLine(tempBuffer.toString('utf8', 0, tempBuffer.length - 1), results)

  Object.keys(miniResult).forEach(key => {
    const current = miniResult;
    if (results[key]) {
      if (current[key].min < results[key].min) {
        results[key].min = current[key].min
      }
      if (current[key].max > results[key].max) {
        results[key].max = current[key].max
      }
      results[key].count += current[key].count
      results[key].total += current[key].total
    } else {
      results[key] = current[key]
    }
  })
}

let isDone = 0;
export async function runV2(filePath: string) {
  const file = await open(filePath)
  const chunkCount = Math.ceil((await file.stat()).size / bufSize)
  const results: MiniResult[] = []
  const finalResult: Result = {};

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
const result = await runV2('../measurements.txt');
const end = Bun.nanoseconds();
console.log(`${(end - start) / 10**9}`)
console.log(`${result}\n` === (await Bun.file('../notes/answer.txt').text()))

// Best times:
// 97.8 seconds, 8 workers, 2**24 bufSize
//  - This score was achieved by merging all results after workers are done
// 109.8 seconds, 8 workers, 2**24 bufSize
// 111.1 seconds, 8 workers, 2**24 bufSize
// 112.5 seconds, 8 workers, 2**24 bufSize

// OLD
// for (let i = 0; i < chunkCount; i++) {
//   if (Object.keys(workers).length > 8) await Promise.race(Object.values(workers))
//   workers[i.toString()] = new Promise((resolve, reject) => {
//     const worker = new Worker('./workerV2.ts')
//     worker.postMessage({ filePath, i, bufSize })
//     worker.onmessage = (e) => {
//       process.stdout.clearLine(0);
//       process.stdout.cursorTo(0);
//       process.stdout.write(`${++isDone}/${chunkCount}`)
//       delete workers[i.toString()]
//       results[i] = e.data
//       resolve(e.data)
//       worker.terminate();
//     }
//     worker.onerror = (err) => {
//       reject(err)
//       throw err
//     }
//   })
// }
// await Promise.all(Object.values(workers))
