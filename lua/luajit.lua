local startTime = os.clock()

print("hello world")
local bit = require('bit')

local chunkSize = bit.lshift(1, 24)
local file = io.open('../measurements.txt', "rb")

if not file then
  return
end

local count = 0
while true do
  count = count + 1
  local chunk = file:read(chunkSize)

  if not chunk then
    print('DONE')
    break
  end
end

print('TIME:', os.clock() - startTime)
