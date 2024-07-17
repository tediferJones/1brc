def main():
    print('Main func');
    filePath = '../measurements.txt'
    file = open(filePath, 'rb')
    chunkSize = 1 << 24
    count = 0
    miniResults = []
    while True:
        chunk = file.read(chunkSize)
        count += 1
        print(count, type(chunk))
        endOfFirstLine = chunk.find(10)
        startOfLastLine = chunk.rfind(10)
        miniResult = {}
        # print(chunk[:endOfFirstLine])
        # print(chunk[startOfLastLine:])
        # print(len(chunk), print(startOfLastLine), print(endOfFirstLine))

        mainChunk = chunk[endOfFirstLine + 1:startOfLastLine]
        startIndex = 0
        while True:
            endOfNextLine = mainChunk.find(10, startIndex)
            if endOfNextLine < 0:
                break;
            if startIndex > len(mainChunk):
                print('IS DONE')
                break;
            # print(endOfNextLine)
            line = mainChunk[startIndex:endOfNextLine].decode('utf-8')
            startIndex = endOfNextLine + 1
            # print(line)
        print('DONE')

        miniResults.append({
            'first': chunk[:endOfFirstLine],
            'last': chunk[startOfLastLine:],
            # 'miniResult': chunk[startOfLastLine:endOfFirstLine],
        })
        if not chunk:
            break;

main();
