function processLine(line, result) {
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

self.onmessage = (e) => {
  // console.log('from worker', e)
  const { chunk, result, id } = e.data;
  const decoder = new TextDecoder('utf8')
  let lineCount = 0
  // decoder.decode(chunk).split('\n').forEach(line => {
  //   // console.log('doing stuff', line)
  //   if (!line) return
  //   lineCount++
  //   // if (lineCount % 10000000 === 0) console.log(lineCount.toLocaleString())
  //   processLine(line, result)
  // })

  chunk.forEach(chunk => {
    decoder.decode(chunk).split('\n').forEach(line => {
      // console.log('doing stuff', line)
      if (!line) return
      lineCount++
      // if (lineCount % 10000000 === 0) console.log(lineCount.toLocaleString())
      processLine(line, result)
    })
  })

  // console.log('worker is done')
  self.postMessage({ result, id, lineCount })

}
