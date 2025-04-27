import argparse
import polars as pl
import os


def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description="Построение свечных графиков из данных о сделках Binance.")
    parser.add_argument("--input", type=str, required=True, help="Входная папка или parquet-файл")
    parser.add_argument("--output", type=str, required=True, help="Путь для сохранения выходного parquet-файла")
    parser.add_argument("--interval", type=str, required=True,
                        choices=["500ms", "1s", "1m"],
                        help="Интервал свечей (500ms, 1s или 1m)")
    return parser.parse_args()


def interval_to_ms(interval):
    """Конвертация строкового интервала в миллисекунды"""
    if interval == "500ms":
        return 500
    elif interval == "1s":
        return 1000
    elif interval == "1m":
        return 60000  # 1 минута = 60,000 мс


def read_parquet(input_path):
    """Чтение parquet-файлов из указанного пути"""
    if os.path.isdir(input_path):
        # Если путь - директория, читаем все parquet-файлы в ней
        files = [os.path.join(input_path, f)
                 for f in os.listdir(input_path)
                 if f.endswith(".parquet")]
        if not files:
            raise ValueError(f"В директории {input_path} не найдено parquet-файлов")
        return pl.read_parquet(files)
    elif os.path.isfile(input_path) and input_path.endswith(".parquet"):
        # Если путь указывает на конкретный файл
        return pl.read_parquet(input_path)
    else:
        raise ValueError("Входной путь должен быть директорией или parquet-файлом")


def build_candlesticks(df: pl.DataFrame, interval_ms: int) -> pl.DataFrame:
    """Построение свечного графика из данных о сделках"""
    # Выравнивание временных меток по заданному интервалу
    df = df.with_columns(
        (pl.col("timestamp") // interval_ms * interval_ms).alias("open_time")
    )

    # Агрегация данных по интервалам
    candles = df.group_by("open_time").agg([
        pl.col("price").first().alias("open"),  # Цена открытия (первая сделка в интервале)
        pl.col("price").max().alias("high"),  # Максимальная цена
        pl.col("price").min().alias("low"),  # Минимальная цена
        pl.col("price").last().alias("close"),  # Цена закрытия (последняя сделка)
        pl.col("price").sort_by("_random_sort").first().alias("random"),  # Случайная цена в интервале
        pl.col("quantity").sum().alias("volume"),  # Общий объем
        pl.len().alias("num_trades")  # Количество сделок
    ]).sort("open_time")  # Сортировка по времени

    return candles


def main():
    """Основная функция выполнения скрипта"""
    args = parse_args()  # Получение аргументов
    df = read_parquet(args.input)  # Чтение входных данных
    interval_ms = interval_to_ms(args.interval)  # Конвертация интервала
    candles = build_candlesticks(df, interval_ms)  # Построение свечей
    candles.write_parquet(args.output)  # Сохранение результата


if __name__ == "__main__":
    main()