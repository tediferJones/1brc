import { open } from 'fs/promises';

interface Record {
  min: number,
  max: number,
  count: number,
  total: number,
}

interface Result {
  [key: string]: Record 
}

interface MiniResult {
  first: Buffer,
  last: Buffer,
  result: Result,
}

interface SomeWorker {
  worker: Worker,
  promiseInfo?: {
    promise: Promise<MiniResult>,
    resolve: Function,
    reject: Function,
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

function format(result: Result) {
  return `{${Object.keys(result).sort().reduce((str, key) => {
    const rec = result[key]
    return str + `${key}=${rec.min.toFixed(1)}/${(rec.total / rec.count).toFixed(1)}/${rec.max.toFixed(1)}, `
  }, '').slice(0, -2)}}\n`
}

// const bufSize = 2**31;
const bufSize = 2**24;
// const workerCount = 2;

async function run(filePath: string) {
  const file = await open(filePath)
  const chunkCount = Math.ceil((await file.stat()).size / bufSize)
  const workers: { [key: string]: Promise<MiniResult> } = {}
  const results: MiniResult[] = []
  const finalResult: Result = {};
  let isDone = 0;

  // for (let i = 0; i < chunkCount; i++) {
  //   // if (i > 4) break;
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

  class MyWorker {
    worker: Worker;
    promiseInfo?: {
      promise: Promise<MiniResult>,
      resolve: Function,
      reject: Function,
    }

    constructor() {
      const worker = new Worker('./workerV2.ts')
      worker.onmessage = (e) => {
        process.stdout.clearLine(0);
        process.stdout.cursorTo(0);
        process.stdout.write(`${++isDone}/${chunkCount}`)
        results[e.data.i] = e.data
        this.promiseInfo?.resolve()
        this.promiseInfo = undefined;
      }
      worker.onerror = (err) => { throw err }
      this.worker = worker
    }

    sendMsg(i: number) {
      if (this.promiseInfo) throw Error('promise already exists')
      this.worker.postMessage({ filePath, i, bufSize })
      let resolve, reject;
      const promise = new Promise<MiniResult>((res, rej) => {
        [ resolve, reject ] = [ res, rej ];
      })
      if (!resolve || !reject) throw Error('cant find resolve and reject')
      this.promiseInfo = { promise, resolve, reject }
    }
  }
  const myWorkers: MyWorker[] = [...Array(8).keys()].map(() => new MyWorker())

  for (let i = 0; i < chunkCount; i++) {
    let worker = myWorkers.find(worker => !worker.promiseInfo);
    if (!worker) {
      await Promise.race(myWorkers.map(worker => worker.promiseInfo?.promise));
      worker = myWorkers.find(worker => !worker.promiseInfo);
    }
    if (!worker) throw Error('no worker')
    worker.sendMsg(i)
  }

  await Promise.all(myWorkers.map(worker => worker.promiseInfo?.promise));
  myWorkers.forEach(worker => worker.worker.terminate());

  console.log('')
  for (let i = 0; i < results.length; i++) {
    const prev = Buffer.from(results[i - 1]?.last || '')
    const current = Buffer.from(results[i].first)
    const tempBuffer = Buffer.alloc((prev.length) + current.length);
    prev.copy(tempBuffer)
    current.copy(tempBuffer, prev.length)
    processLine(tempBuffer.toString('utf8', 0, tempBuffer.length - 1), finalResult)

    Object.keys(results[i].result).forEach(key => {
      const current = results[i].result;
      if (finalResult[key]) {
        if (current[key].min < finalResult[key].min) {
          finalResult[key].min = current[key].min
        }
        if (current[key].max > finalResult[key].max) {
          finalResult[key].max = current[key].max
        }
        finalResult[key].count += current[key].count
        finalResult[key].total += current[key].total
      } else {
        finalResult[key] = current[key]
      }
    })
  }
  console.log('done')
  return format(finalResult)
}

const start = Bun.nanoseconds();
const result = await run('../measurements.txt');
const end = Bun.nanoseconds();
// console.log(result)
console.log(`${(end - start) / 10**9}`)
console.log(`${result}\n` === (await Bun.file('../notes/answer.txt').text()))
// Best time: 112.5 seconds, 8 workers, 2*24 bufSize
