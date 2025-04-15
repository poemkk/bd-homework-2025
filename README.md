# bd-homework-2025
# Homework1程序说明
## 一、`create_data.py`
### （一）功能
创建一个至少2GB的二进制文件，文件内容由随机的32位无符号整数（大端序）组成。

### （二）使用方法
直接运行`create_data.py`，并在命令中指定输出文件路径和文件大小（可选，默认2GB）。
示例命令：`python create_data.py data.bin --size 2`

## 二、`calc_data.py`
### （一）功能
计算二进制文件中32位无符号整数的总和、找到最小值和最大值。提供两种处理模式：简单顺序读取和多进程结合内存映射文件的方式。

### （二）使用方法
1. **简单顺序读取模式**：
示例命令：`python calc_data.py data.bin`
2. **多进程结合内存映射文件模式**：
示例命令：`python calc_data.py data.bin --parallel` 
