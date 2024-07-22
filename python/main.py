import os
import math
import time
from multiprocessing import Pool
from typing import Dict, List, TypedDict

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
    # chunkCount: int

MiniResults = List[PartialResult]

def processLine(line: str, miniResult: Result):
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

def processChunk(filePath: str, chunkCount: int, chunkSize: int) -> PartialResult:
    file = open(filePath, 'rb')
    file.seek(chunkCount * chunkSize)
    chunk = file.read(chunkSize)

    endOfFirstLine = chunk.find(10) + 1
    startOfLastLine = chunk.rfind(10) + 1

    miniResult: Result = {}
    mainChunk = chunk[endOfFirstLine:startOfLastLine]
    startIndex = 0
    while startIndex < len(mainChunk):
        endOfNextLine = mainChunk.find(10, startIndex)
        if (endOfNextLine == -1):
            endOfNextLine = len(mainChunk)

        line = mainChunk[startIndex:endOfNextLine].decode('utf-8')
        startIndex = endOfNextLine + 1
        processLine(line, miniResult)

    print('DONE', chunkCount)
    return {
        'first': chunk[:endOfFirstLine],
        'last': chunk[startOfLastLine:],
        'miniResult': miniResult,
        # 'chunkCount': chunkCount
    }

def mergeResults(partialResults: MiniResults):
    finalResult: Result = {}
    for i, partialResult in enumerate(partialResults):
        line = (partialResults[i - 1]['last'] if i > 0 else b"") + partialResult['first']
        processLine(line.decode('utf-8'), finalResult)

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
    finalString = '{'
    for key in sorted(result.keys()):
        average = result[key]['total'] / result[key]['count']
        finalString += f"{key}={round(result[key]['min'], 1)}/{round(average, 1)}/{round(result[key]['max'], 1)}, "

    return finalString[:-2] + '}\n'

def main():
    filePath = '../measurements.txt'
    file = open(filePath, 'rb')
    chunkSize = 1 << 24
    chunkCount = math.ceil(os.fstat(file.fileno()).st_size / chunkSize)
    miniResults: MiniResults = []

    # Multiprocessor
    batchSize = 32
    chunks = [ (filePath, i, chunkSize) for i in range(chunkCount) ]
    with Pool(processes=batchSize) as pool:
        for i in range(0, len(chunks), batchSize):
            batch = chunks[i:i+batchSize]
            batchResults = pool.starmap(processChunk, batch)
            miniResults.extend(batchResults)

    # Single threaded
    # for i in range(chunkCount):
    #     miniResults.append(
    #         processChunk(filePath, i, chunkSize)
    #     )
    #     print(time.time() - start)

    finalResult = mergeResults(miniResults)
    return prettyPrint(finalResult)

if __name__ == '__main__':
    start = time.time()
    finalString = main()
    print('Run Time:', time.time() - start)

    with open('../notes/answer.txt') as file:
        answer = file.read()
        # print('THIS IS THE ANSWER\n', answer)
        # print('THIS IS THE RESULT\n', finalString)

        print(finalString + '\n' == answer)
