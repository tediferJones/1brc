// import { open } from 'fs/promises'
// 
// export default async function run(filePath: string) {
//   const file = await open(filePath)
// 
//   for await (const line of file.readLines()) {
//     console.log(line)
//   }
// }
// run('../measurements.txt')


// async function test() {
//   const file = Bun.file('../measurements.txt').stream();
//   // const reader = file.getReader();
//   let lineCount = 0;
// 
//   // console.log(await reader.read())
//   // @ts-ignore
//   for await (const chunk of file) {
//     const str = Buffer.from(chunk).toString();
//     lineCount += str.split(/\n/).length - 1
//     // console.log(lineCount)
//   }
// }
// 
// async function testV2() {
//   const file = Bun.file('../measurements.txt').stream();
//   const reader = file.getReader();
//   const encoder = new TextDecoder('utf-8')
//   let lineCount = 0;
//   // let chunk;
// 
//   while (true) {
//     const { value, done } = await reader.read()
//     if (done) break
//     lineCount += encoder.decode(value).split(/\n/).length - 1
//     // console.log(lineCount)
//     // console.log(encoder.decode(value))
//   }
// }

// const first = Bun.nanoseconds()
// await test()
// const second = Bun.nanoseconds()
// console.log(`End of file: ${(second - first) / 10**9} seconds`)
// await testV2()
const third = Bun.nanoseconds()
// console.log(`End of file: ${(third - second) / 10**9} seconds`)

import { open, read } from 'fs';

type State = 'key' | 'val';
type Test = {
  [key in State]: number[]
} & { get: 'key' | 'val', }

const searchState: Test = {
  // key: '',
  // val: '',
  key: [],
  val: [],
  get: 'key'
}

const decoder = new TextDecoder();
function readChunk(result: Result, fd: number, buf: Buffer, bufSize: number, count=0) {
  read(fd, buf, 0, bufSize, count*bufSize, (err, num) => {
    // console.log(buf, count)
    // for (let i = 0; i < buf.length; i++) {
    for (let i = 0; i < bufSize; i++) {
      if (buf[i] === ';'.charCodeAt(0)) {
        searchState.get = 'val';
      } else if (buf[i] === '\n'.charCodeAt(0)) {
        total++
        const num = Number(decoder.decode(new Uint8Array(searchState.val)))
        const city = decoder.decode(new Uint8Array(searchState.key))
        if (total % 1000000 === 0) console.log(total.toLocaleString())
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
        searchState.get = 'key'
        searchState.val = [];
        searchState.key = [];
      } else if (buf[i] === 0) {
        console.log('end of file');
        console.log(searchState)
        done = true;
        break
      } else {
        searchState[searchState.get].push(buf[i])
      }
    }

    if (!done) {
      readChunk(result, fd, Buffer.alloc(bufSize), bufSize, count + 1)
    }

    // console.log(buf)
    // const str = buf.toString('utf8', 0, num)
    // if (!str) {
    //   console.log(`End of file: ${(Bun.nanoseconds() - third) / 10**9} seconds`)
    //   done = true
    //   return 
    // }
    // total += str.split('\n').length - 1
    // console.log(str)
    // readChunk(result, fd, buf, bufSize, count + 1)
  })
}

interface Result {
  [key: string]: {
    min: number,
    max: number,
    count: number,
    total: number,
  }
}

let total = 0;
let done = false;

async function isDone(returnVal: Result): Promise<Result> {
  return new Promise((resolve, reject) => {
    const check = () => done ? resolve(returnVal) : setTimeout(check, 100)
    check()
  })
}

function format(result: Result) {
  return `{${Object.keys(result).sort().reduce((str, key) => {
    const rec = result[key]
    return str + `${key}=${rec.min.toFixed(1)}/${(rec.total / rec.count).toFixed(1)}/${rec.max.toFixed(1)}, `
  }, '').slice(0, -2)}}\n`
}

export default async function run(filePath: string) {
  // BEST SO FAR: 128000
  const bufSize = 128000;
  // const bufSize = 100;
  // console.log(`Buf size is: ${bufSize}`)
  let result: Result = {};
  open(filePath, (error, fd) => {
    if (error) return console.log(error.message)
    const buf = Buffer.alloc(bufSize)
    readChunk(result, fd, buf, bufSize)
  })

  const res = await isDone(result)
  // console.log(res)
  done = false;
  return format(res)
}

// console.log(Buffer.from('Ségou'))
// console.log(await run('./samples/measurements-10.txt'))
// console.log(await run('./samples/measurements-1.txt'))
const start = Bun.nanoseconds();
console.log(await run('../measurements.txt'))
console.log(`Runtime: ${(Bun.nanoseconds() - start) / 10**9} seconds`)
