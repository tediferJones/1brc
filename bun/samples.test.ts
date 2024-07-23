import { readdir } from "node:fs/promises";
import { expect, test } from "bun:test";
import { run, easyMode } from "./base/index";
import { runV2 } from "./workersV2/indexV2";
import { runMap } from "./map/indexMap";

const dir = await readdir('samples/')
dir.filter(file => file.match(/\.txt$/)).toSorted().forEach(inputFile => {
  console.log(inputFile)
  test(inputFile, async () => {
    // const myResult = await run(`./samples/${inputFile}`)
    // const myResult = await easyMode(`./samples/${inputFile}`)
    // const myResult = await runV2(`./samples/${inputFile}`)
    const myResult = await runMap(`./samples/${inputFile}`)
    const testResult = await Bun.file(
        `./samples/${inputFile.slice(0, inputFile.lastIndexOf('.')) + '.out'}`
      ).text()
    expect(myResult).toBe(testResult)
    // console.log(myResult === testResult)
  })
})

// test('test', async () => {
//   // const myResult = await run('./samples/measurements-1.txt')
//   const myResult = await runMap('./samples/measurements-1.txt')
//   const testResult = await Bun.file(
//       './samples/measurements-1.out'
//     ).text()
//   console.log(myResult)
//   expect(myResult).toBe(testResult)
// })
