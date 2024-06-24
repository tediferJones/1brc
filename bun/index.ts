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
// const third = Bun.nanoseconds()
// console.log(`End of file: ${(third - second) / 10**9} seconds`)

import { open, read } from 'fs';

type State = 'key' | 'val';
type Test = {
  [key in State]: number[]
} & { get: 'key' | 'val', }

const searchState: Test = {
  key: [],
  val: [],
  get: 'key'
}

const searchStateV2: {
  get: 59 | 10,
  key: string,
  val: string,
  remains: Buffer,
  // remains: string,
} = {
  get: 59,
  key: '',
  val: '',
  remains: Buffer.alloc(100),
  // remains: '',
}

const decoder = new TextDecoder();
function readChunk(result: Result, fd: number, buf: Buffer, bufSize: number, count=0) {
  read(fd, buf, 0, bufSize, count*bufSize, (err, num) => {
    // console.log(buf)
    // let start = 0;
    // console.log(searchStateV2)
    // console.log(buf)
    // console.log(buf.toString())
    // while (true) {
    //   const end = buf.indexOf(searchStateV2.get, start)
    //   if (end < 0) {
    //     if (buf[start + 1] === 0) {
    //       done = true
    //     }
    //     // buf.copy(searchStateV2.remains, 0, start)
    //     let final;
    //     if (buf.indexOf(0) === -1) {
    //       final = buf.length;
    //     } else {
    //       final = buf.indexOf(0);
    //     }
    //     // searchStateV2.remains = buf.toString('utf8', start, final)
    //     buf.copy(searchStateV2.remains, searchStateV2.remains.indexOf(0), start, final)
    //     break;
    //   }

    //   // const remainStr = searchStateV2.remains[0] === 0 ? '' : searchStateV2.remains.toString()
    //   // searchStateV2.remains = Buffer.alloc(100)

    //   // const remainStr = searchStateV2.remains
    //   // searchStateV2.remains = ''

    //   // if (searchStateV2.remains) {
    //   //   searchStateV2[searchStateV2.get === 59 ? 'key' : 'val'] += searchStateV2.remains
    //   //   searchStateV2.remains = ''
    //   // }

    //   if (searchStateV2.get === 10) {
    //     // console.log('end')
    //     // searchStateV2.val = buf.toString('utf8', start, end)
    //     buf.copy(searchStateV2.remains, searchStateV2.remains.indexOf(0))
    //     searchStateV2.val = searchStateV2.remains.toString('utf8', 0, searchStateV2.remains.indexOf(0)).slice(0, -1)
    //     searchStateV2.get = 59
    //     searchStateV2.remains = Buffer.alloc(100)
    //     total++
    //     // if (total % 10000000 === 0) {
    //     //   const newTemp = Bun.nanoseconds();
    //     //   console.log(total, (newTemp - tempTime) / 10**9)
    //     //   tempTime = newTemp
    //     // }

    //     // const city = remainStr + searchStateV2.key
    //     const city = searchStateV2.key
    //     // console.log(Buffer.from(city))
    //     const num = Number(searchStateV2.val)
    //     if (result[city]) {
    //       const rec = result[city];
    //       if (num < rec.min) rec.min = num;
    //       if (num > rec.max) rec.max = num;
    //       rec.total += num;
    //       rec.count += 1;
    //     } else {
    //       result[city] = {
    //         min: num,
    //         max: num,
    //         total: num,
    //         count: 1,
    //       }
    //     }
    //   } else {
    //     // searchStateV2.key = remainStr + buf.toString('utf8', start, end)
    //     // searchStateV2.key = buf.toString('utf8', start, end)
    //     buf.copy(searchStateV2.remains, searchStateV2.remains.indexOf(0))
    //     searchStateV2.key = searchStateV2.remains.toString('utf8', 0, searchStateV2.remains.indexOf(0)).slice(0, -1)
    //     searchStateV2.remains = Buffer.alloc(100)
    //     searchStateV2.get = 10
    //   }
    //   // console.log(buf.toString('utf8', start, end))
    //   // console.log(searchStateV2)
    //   start = end + 1
    // }
    
    for (let i = 0; i < bufSize; i++) {
      // if (buf[i] === ';'.charCodeAt(0)) {
      if (buf[i] === 59) {
        searchState.get = 'val';
      // } else if (buf[i] === '\n'.charCodeAt(0)) {
      } else if (buf[i] === 10) {
        total++
        const num = Number(decoder.decode(new Uint8Array(searchState.val)))
        const city = decoder.decode(new Uint8Array(searchState.key))
        if (total % 10000000 === 0) {
          const newTemp = Bun.nanoseconds();
          console.log(total.toLocaleString(), (newTemp - tempTime) / 10**9)
          tempTime = newTemp
        }
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
        // console.log('end of file');
        // console.log(searchState)
        done = true;
        break
      } else {
        searchState[searchState.get].push(buf[i])
      }

      // switch (buf[i]) {
      //   // case ';'.charCodeAt(0):
      //   case 59:
      //     searchState.get = 'val';
      //     break;
      //   // case '\n'.charCodeAt(0):
      //   case 10:
      //     total++
      //     const num = Number(decoder.decode(new Uint8Array(searchState.val)))
      //     const city = decoder.decode(new Uint8Array(searchState.key))
      //     if (total % 10000000 === 0) console.log(total.toLocaleString())
      //     if (result[city]) {
      //       const rec = result[city];
      //       if (num < rec.min) rec.min = num;
      //       if (num > rec.max) rec.max = num;
      //       rec.total += num;
      //       rec.count += 1;
      //     } else {
      //       result[city] = {
      //         min: num,
      //         max: num,
      //         total: num,
      //         count: 1,
      //       }
      //     }
      //     searchState.get = 'key'
      //     searchState.val = [];
      //     searchState.key = [];
      //     break;
      //   case 0:
      //     // console.log('end of file');
      //     // console.log(searchState)
      //     done = true;
      //     return
      //     // break
      //   default:
      //     searchState[searchState.get].push(buf[i])
      // }
    }

    if (!done) {
      readChunk(result, fd, Buffer.alloc(bufSize), bufSize, count + 1)
    }

    // // console.log(buf)
    // const str = buf.toString('utf8', 0, num)
    // if (!str) {
    //   console.log(`End of file: ${(Bun.nanoseconds() - third) / 10**9} seconds`)
    //   done = true
    //   return 
    // }
    // total += str.split('\n').length - 1
    // // console.log(str)
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

export async function run(filePath: string) {
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

export async function easyMode(filePath: string) {
  const testStream = Bun.file(filePath).stream()
  let lineCount = 0;
  const result: Result = {}

  const decoder = new TextDecoder('utf8')
  let remains = '';
  for await (const chunk of testStream as any) {
    // const lines = decoder.decode(chunk).split('\n')
    // console.log(lines[lines.length - 1])
    // lineCount += lines.length - 1
    // console.log(lineCount)
    decoder.decode(chunk).split('\n').forEach((line, i, arr) => {
      if (!line) return
      if (i === 0) {
        // console.log(remains)
        if (remains) {
          line = remains + line
          remains = ''
          // console.log('TOTAL', line)
        }
        // console.log('Part 2', line)
      }
      if (arr.length - 1 === i && !line.match(/\d+.\d$/)) {
        remains = line
        // console.log('Part 1', remains)
        return
      }
      lineCount++
      if (lineCount % 10000000 === 0) console.log(lineCount.toLocaleString())
      const [city, temp] = line.split(';')
      const num = Number(temp)
      if (isNaN(num)) console.log(num, temp, line)
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
    })
  }
  return format(result)
}
const start = Bun.nanoseconds();
console.log(await easyMode('../measurements.txt'))
// console.log(await easyMode('./samples/measurements-10.txt'))
console.log(`Runtime: ${(Bun.nanoseconds() - start) / 10**9} seconds`)

// const x = Buffer.from('Ségou;420\nPoop;69\n')
// console.log(x)
// console.log(x.indexOf(59, 7))
// console.log(x.indexOf(10))
// const test = Buffer.alloc(100)
// console.log(test)
// console.log(test.byteLength, test.indexOf(0))
// x.copyWithin()

// console.log(Buffer.from('Ségou').toString())
// console.log(await run('./samples/measurements-10.txt'))
// console.log(Buffer.from('Kunming;19.8\n'))
// console.log(await run('./samples/measurements-1.txt'))
// const start = Bun.nanoseconds();
// let tempTime = start;
// console.log(await run('../measurements.txt'))
// console.log(`Runtime: ${(Bun.nanoseconds() - start) / 10**9} seconds`)
