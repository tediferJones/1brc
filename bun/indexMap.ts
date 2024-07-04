import { open } from 'fs/promises';
import type {
  MapResult,
  MiniMapResult,
  WorkerReq,
} from './types';

class MyWorker {
  worker: Worker;
  promiseInfo?: {
    promise: Promise<MiniMapResult>,
    resolve: Function,
    reject: Function,
  };

  constructor(workerResults: MiniMapResult[]) {
    const worker = new Worker('./workerMap.ts')
    worker.onmessage = (e: { data: MiniMapResult }) => {
      workerResults[e.data.i] = e.data
      this.promiseInfo?.resolve()
      this.promiseInfo = undefined;
    }
    worker.onerror = (err) => { throw err }
    this.worker = worker
  }

  sendMsg(msgObj: WorkerReq) {
    if (this.promiseInfo) throw Error('promise already exists')
    this.worker.postMessage(msgObj)
    let resolve, reject;
    const promise = new Promise<MiniMapResult>(
      (res, rej) => [ resolve, reject ] = [ res, rej ]
    )
    if (!resolve || !reject) throw Error('cant find resolve and reject')
    this.promiseInfo = { promise, resolve, reject }
  }
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

const bufSize = 2**24;
const workerCount = 8;

export async function runMap(filePath: string) {
  const file = await open(filePath);
  const chunkCount = Math.ceil((await file.stat()).size / bufSize);
  const workerResults: MiniMapResult[] = [];
  const finalResult: MapResult = new Map();

  const myWorkers: MyWorker[] = [
    ...Array(workerCount).keys()
  ].map(() => new MyWorker(workerResults));

  for (let i = 0; i < chunkCount; i++) {
    let worker = myWorkers.find(worker => !worker.promiseInfo);
    if (!worker) {
      await Promise.race(myWorkers.map(worker => worker.promiseInfo?.promise));
      worker = myWorkers.find(worker => !worker.promiseInfo);
    }
    if (!worker) throw Error('no worker');
    worker.sendMsg({ filePath, i, bufSize });
  }

  await Promise.all(myWorkers.map(worker => worker.promiseInfo?.promise));
  myWorkers.forEach(worker => worker.worker.terminate());

  for (let i = 0; i < workerResults.length; i++) {
    processLine(
      Buffer.concat([
        workerResults[i - 1]?.last || new Uint8Array(),
        workerResults[i].first
      ]).toString('utf8'),
      finalResult,
    );
    mergeResults(workerResults[i].result, finalResult);
  }
  return format(finalResult);
}

const start = Bun.nanoseconds();
const result = await runMap('../measurements.txt');
const end = Bun.nanoseconds();
console.log(`${(end - start) / 10**9}`);
console.log(`${result}\n` === (await Bun.file('../notes/answer.txt').text()));

// Best times:
// 93 seconds, 8 workers, 2**24 bufSize
// 94 seconds, 8 workers, 2**24 bufSize
