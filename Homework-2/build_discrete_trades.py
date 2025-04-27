import argparse
import polars as pl
import os


def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description="Построение дискретизированных сделок из данных Binance")
    # Входные данные (файл или папка с parquet-файлами)
    parser.add_argument("--input", type=str, required=True,
                        help="Путь к входному файлу .parquet или папке с файлами")
    # Путь для сохранения результата
    parser.add_argument("--output", type=str, required=True,
                        help="Путь для сохранения выходного файла .parquet")
    # Доступные интервалы агрегации
    parser.add_argument("--interval", type=str, required=True,
                        choices=["250ms", "500ms", "1s", "1h"],
                        help="Интервал агрегации сделок")
    return parser.parse_args()


def interval_to_ms(interval):
    """Конвертация строкового интервала в миллисекунды"""
    if interval == "250ms":
        return 250  # 250 миллисекунд
    elif interval == "500ms":
        return 500  # 500 миллисекунд
    elif interval == "1s":
        return 1000  # 1 секунда (1000 мс)
    elif interval == "1h":
        return 3600 * 1000  # 1 час (3,600,000 мс)


def read_parquet(input_path):
    """Чтение входных данных из parquet-файла(ов)"""
    if os.path.isdir(input_path):
        # Если путь ведет к папке - находим все parquet-файлы
        files = [os.path.join(input_path, f)
                 for f in os.listdir(input_path)
                 if f.endswith(".parquet")]
        if not files:
            raise ValueError(f"В директории {input_path} не найдено parquet-файлов")
        return pl.read_parquet(files)  # Чтение всех файлов
    elif os.path.isfile(input_path) and input_path.endswith(".parquet"):
        # Если путь ведет к конкретному файлу
        return pl.read_parquet(input_path)
    else:
        raise ValueError("Входные данные должны быть файлом .parquet или папкой с такими файлами")


def build_discrete_trades(df: pl.DataFrame, interval_ms: int) -> pl.DataFrame:
    """Построение дискретизированных сделок"""
    # Добавляем колонки:
    # 1. Время начала интервала (выровненное)
    # 2. Направление сделки (покупка/продажа)
    df = df.with_columns([
        (pl.col("timestamp") // interval_ms * interval_ms).alias("open_time"),
        pl.when(pl.col("is_buyer_maker")).then("sell").otherwise("buy").alias("side")
    ])

    # Группировка по времени и направлению сделки
    trades = df.group_by(["open_time", "side"]).agg([
        # Средневзвешенная цена (VWAP)
        (pl.col("price") * pl.col("quantity")).sum() / pl.col("quantity").sum(),
        # Суммарный объем в базовой валюте
        pl.col("quantity").sum(),
        # Суммарный объем в котируемой валюте
        pl.col("quote_qty").sum(),
        # Количество сделок в интервале
        pl.len()
    ]).rename({
        # Переименование колонок для понятного вывода
        "price": "vwap_price",
        "quantity": "total_quantity",
        "quote_qty": "total_quote_qty",
        "len": "num_trades"
    }).sort(["open_time", "side"])  # Сортировка по времени и типу сделки

    return trades


def main():
    """Основная функция выполнения скрипта"""
    # 1. Получаем параметры командной строки
    args = parse_args()

    # 2. Загружаем входные данные
    df = read_parquet(args.input)

    # 3. Конвертируем интервал в миллисекунды
    interval_ms = interval_to_ms(args.interval)

    # 4. Строим дискретизированные сделки
    discrete_trades = build_discrete_trades(df, interval_ms)

    # 5. Сохраняем результат
    discrete_trades.write_parquet(args.output)


if __name__ == "__main__":
    main()