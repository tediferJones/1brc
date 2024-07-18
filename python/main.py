from typing import Dict, List, TypedDict
import time

class Record(TypedDict):
    min: float
    max: float
    total: float
    count: float

Result = Dict[str, Record]

class PartialResult(TypedDict):
    first: bytes
    last: bytes
    miniResult: Result

def mergeResults(partialResults: List[PartialResult]):
    finalResult: Result = {}
    for partialResult in partialResults:
        # We need to handle partial lines some where around here
        miniResult = partialResult['miniResult']
        for city in miniResult:
            if city in finalResult:
                if finalResult[city]['min'] > miniResult[city]['min']:
                    finalResult[city]['min'] = miniResult[city]['min']
                if finalResult[city]['max'] < miniResult[city]['max']:
                    finalResult[city]['max'] = miniResult[city]['max'] 
                finalResult[city]['total'] += miniResult[city]['total']
                finalResult[city]['count'] += miniResult[city]['count']
            else:
                finalResult[city] = {
                    'min': miniResult[city]['min'],
                    'max': miniResult[city]['max'],
                    'total': miniResult[city]['total'],
                    'count': miniResult[city]['count'],
                }
    return finalResult

def prettyPrint(result: Result):
    return ''

def main():
    print('Main func');
    start = time.time()
    filePath = '../measurements.txt'
    file = open(filePath, 'rb')
    chunkSize = 1 << 24
    count = 0
    miniResults: List[PartialResult] = []
    while True:
        chunk = file.read(chunkSize)
        if not chunk:
            break

        count += 1
        print(count)

        endOfFirstLine = chunk.find(10) + 1
        startOfLastLine = chunk.rfind(10) + 1
        miniResult: Result = {}
        # print(len(chunk), print(startOfLastLine), print(endOfFirstLine))

        mainChunk = chunk[endOfFirstLine:startOfLastLine]
        startIndex = 0
        while startIndex < len(mainChunk):
            endOfNextLine = mainChunk.find(10, startIndex)
            if (endOfNextLine == -1):
                endOfNextLine = len(mainChunk)

            line = mainChunk[startIndex:endOfNextLine].decode('utf-8')
            startIndex = endOfNextLine + 1
            city, num = line.split(';')
            temp = float(num)

            if city in miniResult:
                if miniResult[city]['min'] > temp:
                    miniResult[city]['min'] = temp
                if miniResult[city]['max'] < temp:
                    miniResult[city]['max'] = temp
                miniResult[city]['total'] += temp
                miniResult[city]['count'] += 1
            else:
                miniResult[city] = {
                    'min': temp,
                    'max': temp,
                    'total': temp,
                    'count': 1,
                }

        miniResults.append({
            'first': chunk[:endOfFirstLine],
            'last': chunk[startOfLastLine:],
            'miniResult': miniResult,
        })
        print('Chunk Time:', time.time() - start)

    finalResult = mergeResults(miniResults)
    prettyPrint(finalResult)
    print('Run Time:', time.time() - start)

main();
