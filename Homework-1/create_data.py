import os
import struct
import random
import argparse


def create_binary_file(file_path, target_size_gb):
    target_size = target_size_gb * 1024 ** 3
    num_ints = target_size // 4

    with open(file_path, 'wb') as f:
        for _ in range(num_ints):
            # 生成随机无符号32位整数并使用大端序打包
            num = random.getrandbits(32)
            f.write(struct.pack('>I', num))

    actual_size = os.path.getsize(file_path)
    print(f"Created file {file_path} with size {actual_size / 1024 ** 3:.2f} GB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path", help="Output file path")
    parser.add_argument("--size", type=int, default=2, help="File size in GB (default: 2)")
    args = parser.parse_args()

    create_binary_file(args.file_path, args.size)