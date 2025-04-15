import struct
import time
import argparse
import mmap
import multiprocessing
from functools import partial


def simple_calculation(file_path):
    min_num = 2 ** 32
    max_num = 0
    total = 0

    with open(file_path, 'rb') as f:
        while True:
            data = f.read(4)
            if not data:
                break
            num = struct.unpack('>I', data)[0]

            if num < min_num:
                min_num = num
            if num > max_num:
                max_num = num
            total += num

    return total, min_num, max_num


def process_chunk(data_chunk):
    min_num = 2 ** 32
    max_num = 0
    total = 0

    for i in range(0, len(data_chunk), 4):
        num = struct.unpack('>I', data_chunk[i:i + 4])[0]

        if num < min_num:
            min_num = num
        if num > max_num:
            max_num = num
        total += num

    return total, min_num, max_num


def parallel_calculation(file_path):
    with open(file_path, 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            file_size = len(mm)
            num_processes = multiprocessing.cpu_count()
            chunk_size = (file_size // num_processes) & ~3  # 确保是4的倍数

            pool = multiprocessing.Pool(num_processes)
            results = []

            for i in range(num_processes):
                start = i * chunk_size
                end = start + chunk_size if i != num_processes - 1 else file_size
                results.append(pool.apply_async(process_chunk, (mm[start:end],)))

            pool.close()
            pool.join()

            # 合并结果
            total = 0
            min_num = 2 ** 32
            max_num = 0

            for res in results:
                chunk_total, chunk_min, chunk_max = res.get()
                total += chunk_total
                if chunk_min < min_num:
                    min_num = chunk_min
                if chunk_max > max_num:
                    max_num = chunk_max

            return total, min_num, max_num


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path", help="Input file path")
    parser.add_argument("--parallel", action="store_true", help="Use parallel processing")
    args = parser.parse_args()

    start_time = time.time()

    if args.parallel:
        total, min_num, max_num = parallel_calculation(args.file_path)
        print("Using parallel processing with memory-mapped files")
    else:
        total, min_num, max_num = simple_calculation(args.file_path)
        print("Using simple sequential reading")

    elapsed = time.time() - start_time

    print(f"Total: {total}")
    print(f"Min: {min_num}")
    print(f"Max: {max_num}")
    print(f"Time elapsed: {elapsed:.2f} seconds")