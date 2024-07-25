local ffi = require("ffi")

ffi.cdef[[
typedef unsigned long pthread_t;

typedef struct {
    long start;
    long end;
    double sum;
} thread_data_t;

int pthread_create(pthread_t *thread, const void *attr, void *(*start_routine)(void*), void *arg);
int pthread_join(pthread_t thread, void **retval);
]]

local C = ffi.C

local function sum_range(arg)
    local data = ffi.cast("thread_data_t*", arg)
    local sum = 0
    for i = data["start"], data["end"] do
        sum = sum + i
    end
    data.sum = sum
    return ffi.cast("void*", 0)
end

local function create_thread(func, data)
    local thread = ffi.new("pthread_t[1]")
    local arg = ffi.new("thread_data_t", data)
    local wrapped_func = ffi.cast("void* (*)(void*)", func)
    local res = C.pthread_create(thread, nil, wrapped_func, arg)
    if res ~= 0 then
        error("Failed to create thread")
    end
    return thread[0], arg
end

local function join_thread(thread)
    local retval = ffi.new("void*[1]")
    C.pthread_join(thread, retval)
end

local num_threads = 4
local range = 1e9
local range_per_thread = range / num_threads
local threads = {}
local results = {}

for i = 0, num_threads - 1 do
    local start = math.floor(i * range_per_thread)
    local stop = math.floor((i + 1) * range_per_thread - 1)
    if i == num_threads - 1 then
        stop = range - 1  -- Ensure the last thread covers up to range-1
    end
    local thread, data = create_thread(sum_range, {start = start, ["end"] = stop, sum = 0})
    table.insert(threads, {thread = thread, data = data})
end

local total_sum = 0
for _, t in ipairs(threads) do
    join_thread(t.thread)
    total_sum = total_sum + t.data.sum
end

print("Total sum: " .. total_sum)

