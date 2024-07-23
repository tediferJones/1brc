interface Result {
  [key: string]: {
    min: number,
    max: number,
    count: number,
    total: number,
  }
}

function format(result: Result) {
  return `{${Object.keys(result).sort().reduce((str, key) => {
    const rec = result[key]
    return str + `${key}=${rec.min.toFixed(1)}/${(rec.total / rec.count).toFixed(1)}/${rec.max.toFixed(1)}, `
  }, '').slice(0, -2)}}\n`
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

export async function easyMode(filePath: string) {
  const stream = Bun.file(filePath).stream()
  let lineCount = 0;
  const result: Result = {}
  const myWorkers: Promise<Result>[]  = []
  let chunkCount = 0;
  let totalLineCount = 0;
  let temp = 0;
  let chunks: Buffer[] = [];

  const decoder = new TextDecoder('utf8')
  let remains = Buffer.alloc(128);
  for await (const tempChunk of stream as any) {
    const chunk = Buffer.from(tempChunk)
    const endOfFristLine = chunk.indexOf(10) + 1;
    const startOfLastLine = chunk.lastIndexOf(10) + 1;

    chunk.copy(remains, remains.indexOf(0), 0, endOfFristLine)
    const toText = decoder.decode(remains.slice(0, remains.indexOf(10)))
    processLine(toText, result)
    remains.fill(0)
    if (startOfLastLine !== chunk.length) chunk.copy(remains, 0, startOfLastLine)

    if (myWorkers.length >= 32) {
      await Promise.all(myWorkers)
    }
    if (myWorkers.length >= 32) throw Error('didnt await')
    if (chunks.length === 256) {
      myWorkers.push(
        new Promise((resolve, reject) => {
          const worker = new Worker('workersV1/worker.js')
          worker.postMessage({
            // chunk: chunk.slice(endOfFristLine, startOfLastLine),
            chunk: chunks,
            id: myWorkers.length,
            result,
          })
          worker.onmessage = (e) => {
            myWorkers.splice(e.data.id, 1)
            totalLineCount += e.data.lineCount
            if (totalLineCount > temp) {
              console.log(totalLineCount.toLocaleString())
              temp += 1000000
            }

            resolve(e.data.result)
            worker.terminate();
          }
          worker.onerror = (err) => {
            throw Error('error in worker')
            // reject(err);
            // worker.terminate();
          }
        })
      )
      chunks = []
    } else {
      chunks.push(chunk.slice(endOfFristLine, startOfLastLine))
    }
    chunkCount++
  }
  console.log(myWorkers)
  console.log('PROMISE RESULT', await Promise.all(myWorkers))
  // console.log(result)
  
  return format(result)
}
const start = Bun.nanoseconds();
// console.log(await easyMode('./samples/measurements-10.txt'))
// console.log(await easyMode('./samples/measurements-1.txt'))
const res = await easyMode('../measurements.txt')
// console.log(res)
console.log(`Runtime: ${(Bun.nanoseconds() - start) / 10**9} seconds`)
console.log(`${res}\n` === (await Bun.file('../notes/answer.txt').text()))
