import { readdir } from "node:fs/promises";
import { expect, test } from "bun:test";
import run from "./index";

const dir = await readdir('samples/')
dir.filter(file => file.match(/\.txt$/)).toSorted().forEach(inputFile => {
  console.log(inputFile)
  test(inputFile, async () => {
    const myResult = await run(`./samples/${inputFile}`)
    const testResult = await Bun.file(
        `./samples/${inputFile.slice(0, inputFile.lastIndexOf('.')) + '.out'}`
      ).text()
    expect(myResult).toBe(testResult)
    // console.log(myResult === testResult)
  })
})


// test('test', async () => {
//   const myResult = await run('./samples/measurements-1.txt')
//   const testResult = await Bun.file(
//       './samples/measurements-1.out'
//     ).text()
//   console.log(myResult)
//   expect(myResult).toBe(testResult)
// })
