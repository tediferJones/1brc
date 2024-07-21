from multiprocessing import Pool
import time

def sum_range(start, end):
    """Compute the sum of integers in the range [start, end)."""
    return sum(range(start, end))

def chunk_ranges(total_range, num_chunks):
    """Divide the total range into smaller chunks."""
    chunk_size = total_range // num_chunks
    chunks = []
    for i in range(num_chunks):
        start = i * chunk_size
        end = start + chunk_size if i != num_chunks - 1 else total_range
        chunks.append((start, end))
    return chunks

if __name__ == "__main__":
    start = time.time()
    total_range = 1_000_000_000
    num_chunks = 10  # Number of chunks/processes to use

    # Create a pool of processes
    with Pool(processes=num_chunks) as pool:
        # Divide the range into chunks
        chunks = chunk_ranges(total_range, num_chunks)
        print(chunks)
        
        # Map the sum_range function to the chunks
        results = pool.starmap(sum_range, chunks)

    # Combine the results from all chunks
    print(results)
    total_sum = sum(results)
    print(time.time() - start)
    print(f"Total sum: {total_sum}")

