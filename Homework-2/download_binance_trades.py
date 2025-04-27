import argparse
import os
import requests
import zipfile
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from urllib.parse import urljoin

BASE_URL = "https://data.binance.vision/data/spot/daily/trades/"


def download_file(symbol, date, output_dir):
    """
    下载指定日期和交易对的交易数据
    Скачать торговые данные за указанную дату и торговую пару
    """
    date_str = date.strftime("%Y-%m-%d")
    filename = f"{symbol.upper()}-trades-{date_str}.zip"
    url = urljoin(BASE_URL, f"{symbol.upper()}/{filename}")

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # 保存压缩文件
        # Сохранить архивный файл
        zip_path = os.path.join(output_dir, filename)
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # 解压并读取CSV
        # Распаковать и прочитать CSV
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # 假设压缩包内只有一个CSV文件
            # Предполагается, что в архиве только один CSV-файл
            csv_filename = zip_ref.namelist()[0]
            zip_ref.extractall(output_dir)
            csv_path = os.path.join(output_dir, csv_filename)

        # 转换为parquet
        # Конвертировать в parquet
        df = pd.read_csv(csv_path, header=None,
                         names=['trade_id', 'price', 'quantity', 'quote_qty',
                                'timestamp', 'is_buyer_maker', 'is_best_match'])

        parquet_path = os.path.join(output_dir, f"{symbol}-trades-{date_str}.parquet")
        df.to_parquet(parquet_path)

        # 清理临时文件
        # Очистить временные файлы
        os.remove(zip_path)
        os.remove(csv_path)

        print(f"Successfully downloaded and processed {filename}")
        # Успешно скачано и обработано {filename}
        return True
    except Exception as e:
        print(f"Failed to download {filename}: {str(e)}")
        # Не удалось скачать {filename}: {str(e)}
        return False


def download_trades(symbol, start_date, num_threads, output_dir="data"):
    """
    下载从指定日期开始的交易数据
    Скачать торговые данные, начиная с указанной даты
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    current_date = start_date
    today = datetime.now().date()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        while current_date <= today:
            futures.append(executor.submit(download_file, symbol, current_date, output_dir))
            current_date += timedelta(days=1)

        # 等待所有任务完成
        # Дождаться завершения всех задач
        results = [f.result() for f in futures]

    print(f"Download completed for {symbol}. Success: {sum(results)}/{len(results)}")
    # Загрузка завершена для {symbol}. Успешно: {sum(results)}/{len(results)}


def main():
    parser = argparse.ArgumentParser(description="Download Binance trade data")
    # Загрузить торговые данные Binance
    parser.add_argument("symbol", help="Trading pair symbol (e.g. btcusdt)")
    # Символ торговой пары (например, btcusdt)
    parser.add_argument("--threads", type=int, default=4, help="Number of download threads")
    # Количество потоков загрузки
    parser.add_argument("--start-date", default="2025-01-01",
                        help="Start date in YYYY-MM-DD format")
    # Дата начала в формате ГГГГ-ММ-ДД

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    download_trades(args.symbol, start_date, args.threads)


if __name__ == "__main__":
    main()