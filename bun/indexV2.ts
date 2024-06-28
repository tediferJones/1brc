import { open } from 'fs/promises';

interface Result {
  [key: string]: {
    min: number,
    max: number,
    count: number,
    total: number,
  }
}

// const bufSize = 2**31;
const bufSize = 2**27;
// const workerCount = 2;

async function run(filePath: string) {
  const file = await open(filePath)
  const chunkCount = Math.ceil((await file.stat()).size / bufSize)
  // const workers: Promise<Result>[] = []
  const workers: { [key: string]: Promise<Result> } = {}
  const results: Result[] = []
  let isDone = 0;
  for (let i = 0; i < chunkCount; i++) {
    // workers.push(
    if (Object.keys(workers).length > 2) {
      await Promise.race(
        // Object.keys(workers).map(workerKey => workers[workerKey])
        Object.values(workers)
      )
    }
    workers[i.toString()] = new Promise((resolve, reject) => {
      const worker = new Worker('./workerV2.ts')
      worker.postMessage({ filePath, i, bufSize })
      worker.onmessage = (e) => {
        process.stdout.clearLine(0);
        process.stdout.cursorTo(0);
        process.stdout.write(`${++isDone}/${chunkCount}`)
        delete workers[i.toString()]
        results[i] = e.data
        resolve(e.data)
        worker.terminate();
      }
      worker.onerror = (err) => {
        reject(err)
        throw err
      }
    })
    // )
  }
  await Promise.all(Object.values(workers)).then(arr => {
    // console.log(arr)
  })
}

const start = Bun.nanoseconds();
const result = await run('../measurements.txt');
const end = Bun.nanoseconds();
console.log(result)
console.log(`${(end - start) / 10**9}`)
