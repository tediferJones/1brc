import concurrent.futures
import time

def partial_sum(start, end):
    return sum(range(start, end))

maxCount = 1000000000
def main():
    total = 0
    num_threads = 4  # You can adjust the number of threads
    chunk_size = maxCount // num_threads

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(partial_sum, i * chunk_size, (i + 1) * chunk_size) for i in range(num_threads)]
        
        for future in concurrent.futures.as_completed(futures):
            print('DONE')
            total += future.result()
    
    print(total)

def mainSync():
    count = 0
    for i in range(maxCount):
        count += i
    print(count)

if __name__ == "__main__":
    start = time.time()
    main()
    # mainSync()
    print(time.time() - start)
