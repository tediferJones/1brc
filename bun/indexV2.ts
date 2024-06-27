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
const bufSize = 2**28;
// const workerCount = 2;

async function run(filePath: string) {
  const file = await open(filePath)
  const chunkCount = Math.ceil((await file.stat()).size / bufSize)
  const workers: Promise<Result>[] = []
  let isDone = 0;
  for (let i = 0; i < chunkCount; i++) {
    workers.push(
      new Promise((resolve, reject) => {
        const worker = new Worker('./workerV2.ts')
        worker.postMessage({ filePath, i, bufSize })
        worker.onmessage = (e) => {
          process.stdout.clearLine(0);
          process.stdout.cursorTo(0);
          process.stdout.write(`${++isDone}/${chunkCount}`)
          resolve(e.data)
          worker.terminate();
        }
        worker.onerror = (err) => {
          reject(err)
          throw err
        }
      })
    )
  }
  await Promise.all(workers).then(arr => {
    // console.log(arr)
  })
}

const start = Bun.nanoseconds();
const result = await run('../measurements.txt');
const end = Bun.nanoseconds();
console.log(result)
console.log(`${(end - start) / 10**9}`)
