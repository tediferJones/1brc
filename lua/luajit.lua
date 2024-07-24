local startTime = os.clock()

local bit = require('bit')

local function findLastCharacter(str, char)
    local lastPos = nil
    local i = #str

    while i > 0 do
        local foundPos = string.find(str, char, i, true)
        if foundPos then
            lastPos = foundPos
            break
        end
        i = i - 1
    end

    if lastPos then
        return lastPos
    else
        return nil, "Character not found"
    end
end

local function processLine(line, miniResult)
  local splitIndex = line:find(';')
  local city = line:sub(0, splitIndex - 1)
  local temp = tonumber(line:sub(splitIndex + 1))
  -- print(city, temp, miniResult[city])

  if miniResult[city] == nil then
    miniResult[city] = {
      min = temp,
      max = temp,
      total = temp,
      count = 1,
    }
  else
    if miniResult[city].min > temp then
      miniResult[city].min = temp
    end
    if miniResult[city].max < temp then
      miniResult[city].max = temp
    end
    miniResult[city].total = miniResult[city].total + temp
    miniResult[city].count = miniResult[city].count + 1
  end
end

local function processChunk(filePath, chunkCount, chunkSize)
  local file = io.open(filePath, "rb")
  if not file then return end
  file:seek("set", chunkCount * chunkSize)
  local chunk = file:read(chunkSize)

  local endOfFirstLine = chunk:find('\n')
  local startOfLastLine = findLastCharacter(chunk, '\n')

  local first = chunk:sub(1, endOfFirstLine)
  local last = chunk:sub(startOfLastLine + 1, #chunk)
  print(chunkCount)
  -- print(chunkCount, first, #first, last, #last)

  local miniResult = {}

  local readIndex = 1;
  local mainChunk = chunk:sub(endOfFirstLine + 1, startOfLastLine - 1)
  while readIndex <= #mainChunk do
    local endOfLine = mainChunk:find('\n', readIndex)
    if not endOfLine then
      endOfLine = #mainChunk
    end
    local line = mainChunk:sub(readIndex, endOfLine)
    processLine(line, miniResult)
    readIndex = endOfLine + 1
    -- print(line)
  end
  MiniResults[#MiniResults + 1] = {
    record = miniResult,
    first = first,
    last = last,
  }
end

local function mergeResults(miniResults)
  local finalResult = {}
  for i = 1, #miniResults do
    local miniResult = miniResults[i]
    local first, records, last = miniResult.first, miniResult.record, ''
    if i > 1 then
      last = miniResults[i - 1].last
    end
    print(last..first)
    processLine(last..first, finalResult)
    for key, record in pairs(records) do
      if finalResult[key] == nil then
        finalResult[key] = record
      else
        if finalResult[key].min > record.min then
          finalResult[key].min = record.min
        end
        if finalResult[key].max < record.max then
          finalResult[key].max = record.max
        end
        finalResult[key].total = finalResult[key].total + record.total
        finalResult[key].count = finalResult[key].count + record.count
      end
    end
  end
  return finalResult
end

local function getSortedKeys(tab)
  local keys = {}
  for key in pairs(tab) do
    table.insert(keys, key)
  end

  table.sort(keys)
  print(keys)
  return keys
end

local function roundNum(num)
  return math.floor(num * 10 + 0.5) / 10
end

local function formatNum(num)
  return string.format("%.1f", num)
end

local function prettyPrint(result)
  local str = '{'
  local sortedKeys = getSortedKeys(result)
  for _, key in ipairs(sortedKeys) do
    str = str..key..'='..formatNum(result[key].min)..'/'..formatNum(roundNum(result[key].total / result[key].count))..'/'..formatNum(result[key].max)..', '
  end
  return str:sub(0, #str - 2)..'}\n'
end

local function compareStrings(str1, str2)
    -- Find the length of the longer string
    local maxLength = math.max(#str1, #str2)
    -- Iterate through each character of the strings
    for i = 1, maxLength do
        local char1 = str1:sub(i, i)
        local char2 = str2:sub(i, i)

        if char1 ~= char2 then
            print(string.format("Difference at position %d: '%s' vs '%s'", i, char1, char2))
        end
    end
end

local filePath = '../measurements.txt'
local chunkSize = bit.lshift(1, 24)
local file = io.open(filePath, "rb")

if not file then
  return
end

local fileSize = file:seek("end")
file:seek("set", 0)
local maxChunkCount = math.ceil(fileSize / chunkSize)

MiniResults = {}
local count = 0
while count < maxChunkCount do
  processChunk(filePath, count, chunkSize)
  count = count + 1
  -- if count > 4 then break end
end

local meregedResults = mergeResults(MiniResults)
local finalString = prettyPrint(meregedResults)
-- print(finalString)

local answer = io.open('../notes/answer.txt')
if not answer then return end
local answerString = answer:read('*a')
-- print(answerString)
print(finalString.."\n" == answerString)
-- compareStrings(finalString, answerString)

print('TIME:', os.clock() - startTime)
