# bd-homework-2025
# 1.Homework-1 Инструкция по программе 
## I. `create_data.py`
### (I) Функция
Создает бинарный файл размером не менее 2 ГБ, состоящий из случайных 32 - битовых беззнаковых целых чисел в формате big endian.

### (II) Способ использования
Запустите `create_data.py` и укажите в команде путь к выходному файлу и размер файла (необязательно, по умолчанию 2 ГБ).
Пример команды: `python create_data.py data.bin --size 2`

## II. `calc_data.py`
### (I) Функция
Вычисляет сумму 32 - битовых беззнаковых целых чисел в бинарном файле, находит минимальное и максимальное значения. Предоставляет два режима обработки: простое последовательное чтение и использование `multiprocessing` совместно с memory - mapped файлами.

### (II) Способ использования
1. **Режим простого последовательного чтения**:
Пример команды: `python calc_data.py data.bin`
2. **Режим с использованием multiprocessing и memory - mapped файлов**:
Пример команды: `python calc_data.py data.bin --parallel`

### (III) Сравнение результатов
![image](https://github.com/user-attachments/assets/71936196-8e0d-4f7f-ab9c-5f6b50e4b020)

# 2. Homework-2 Инструменты для обработки торговых данных Binance

Этот проект включает три основных скрипта для загрузки и обработки торговых данных с Binance.

## Обзор функциональности

1. **Загрузка данных**: Скачивание сырых торговых данных с Binance
2. **Генерация свечей**: Преобразование сырых торговых данных в OHLC свечи
3. **Генерация дискретных сделок**: Агрегация сырых торговых данных по временным интервалам

## Установка зависимостей

```bash
pip install polars requests pandas pyarrow
```

## Инструкция по использованию

### 1. Загрузка торговых данных Binance (`download_binance_trades.py`)

Скачивание торговых данных для указанной торговой пары и сохранение в формате Parquet.

**Параметры**:
- `symbol`: Символ торговой пары (например, btcusdt)
- `--threads`: Количество потоков загрузки (по умолчанию: 4)
- `--start-date`: Дата начала (в формате ГГГГ-ММ-ДД, по умолчанию: 2025-01-01)

**Пример**:
```bash
python download_binance_trades.py btcusdt --threads 8 --start-date 2025-01-01
```

### 2. Генерация свечных данных (`build_candlesticks.py`)

Преобразование сырых торговых данных в свечные данные.

**Параметры**:
- `--input`: Входной файл или директория (формат Parquet)
- `--output`: Путь для сохранения результата
- `--interval`: Временной интервал (500ms, 1s, 1m)

**Пример**:
```bash
python build_candlesticks.py --input ./data/btcusdt-trades.parquet --output ./data/btcusdt-candles-1s.parquet --interval 1s
```

### 3. Генерация дискретных сделок (`build_discrete_trades.py`)

Агрегация сырых торговых данных по временным интервалам и направлению сделки.

**Параметры**:
- `--input`: Входной файл или директория (формат Parquet)
- `--output`: Путь для сохранения результата
- `--interval`: Временной интервал (250ms, 500ms, 1s, 1h)

**Пример**:
```bash
python build_discrete_trades.py --input ./data/btcusdt-trades.parquet --output ./data/btcusdt-discrete-1h.parquet --interval 1h
```

## Структура выходных данных

### Свечные данные (build_candlesticks.py)
| Поле        | Описание                          |
|-------------|-----------------------------------|
| open_time   | Время начала интервала            |
| open        | Цена открытия (первая сделка)     |
| high        | Максимальная цена                 |
| low         | Минимальная цена                  |
| close       | Цена закрытия (последняя сделка)  |
| random      | Случайно выбранная цена сделки    |
| volume      | Общий объем торгов                |
| num_trades  | Количество сделок                 |

### Дискретные сделки (build_discrete_trades.py)
| Поле           | Описание                          |
|----------------|-----------------------------------|
| open_time      | Время начала интервала            |
| side           | Направление сделки (buy/sell)     |
| vwap_price     | Средневзвешенная цена по объему   |
| total_quantity | Общий объем (базовая валюта)      |
| total_quote_qty| Общий объем (котируемая валюта)   |
| num_trades     | Количество сделок                 |

## Важные замечания

1. Убедитесь, что у вас достаточно места на диске для хранения данных
2. При использовании многопоточной загрузки соблюдайте ограничения Binance
3. Для обработки больших объемов данных может потребоваться значительный объем оперативной памяти

# 3. Homework-3 Анализ русскоязычных статей Википедии с использованием PySpark

Этот проект выполняет анализ текстов русскоязычных статей Википедии с использованием PySpark. Основные задачи включают поиск самого длинного слова, расчет средней длины слова, анализ латинских слов, выявление слов с частым использованием заглавных букв, поиск сокращений и извлечение имен.

## Структура проекта

Проект состоит из одного основного файла `4.py`, который содержит:
1. Инициализацию Spark-сессии с оптимизированными настройками
2. Функции для загрузки и обработки данных
3. Реализацию 6 задач анализа текста

## Задачи и их реализация

### Задача 1: Самое длинное русское слово
**Цель**: Найти самое длинное русское слово в статьях.

**Реализация**:
1. Фильтрация русских слов (с использованием регулярного выражения `RU_WORD_PATTERN`)
2. Исключение слов, содержащих точки (чтобы исключить сокращения)
3. Сортировка по длине слова в убывающем порядке
4. Выбор первого результата

**Результат**: Выводится самое длинное слово и его длина.

### Задача 2: Средняя длина слова
**Цель**: Рассчитать среднюю длину русских слов в статьях.

**Реализация**:
1. Фильтрация русских слов
2. Расчет средней длины с помощью агрегатной функции `avg()`

**Результат**: Выводится средняя длина слова с точностью до двух знаков после запятой.

### Задача 3: Самое частое латинское слово
**Цель**: Найти наиболее часто встречающееся слово, состоящее из латинских букв.

**Реализация**:
1. Фильтрация латинских слов (с использованием `LATIN_PATTERN`)
2. Группировка по словам и подсчет их частоты
3. Сортировка по частоте в убывающем порядке
4. Выбор первого результата

**Результат**: Выводится самое частое латинское слово и количество его вхождений.

### Задача 4: Слова с частым использованием заглавных букв
**Цель**: Найти слова, которые чаще пишутся с заглавной буквы (более чем в 50% случаев) и встречаются более 10 раз.

**Реализация**:
1. Фильтрация русских слов без точек
2. Группировка по нормализованному виду слова (в нижнем регистре)
3. Подсчет общего количества вхождений и количества вхождений с заглавной буквы
4. Фильтрация по условиям (>10 вхождений, >50% с заглавной буквы)
5. Сортировка по общему количеству вхождений

**Результат**: Выводится таблица с 20 самыми частыми словами, удовлетворяющими условиям.

### Задача 5: Сокращения вида "пр.", "др."
**Цель**: Найти часто встречающиеся русские сокращения длиной до 4 символов (включая точку).

**Реализация**:
1. Извлечение слов, содержащих только русские буквы и точки
2. Фильтрация по шаблону сокращений (`ABBREV1_PATTERN`)
3. Ограничение длины (<= 4 символа)
4. Группировка и подсчет частоты
5. Фильтрация (более 10 вхождений)
6. Сортировка по частоте

**Результат**: Выводится таблица с 20 самыми частыми сокращениями.

### Задача 7: Извлечение имен
**Цель**: Найти часто встречающиеся русские имена и фамилии.

**Реализация**:
1. Извлечение слов по шаблону имен (`NAME_PATTERN`)
2. Фильтрация:
   - Длина > 2 символов
   - Исключение слов из списка `NON_NAME_WORDS`
   - Принадлежность к списку русских имен/фамилий (`RUSSIAN_NAME_LIST`) ИЛИ соответствие шаблону "Имя Фамилия"
3. Группировка и подсчет частоты
4. Фильтрация (более 5 вхождений)
5. Сортировка по частоте

**Результат**: Выводится таблица с 20 самыми частыми именами.

## Использование

1. Убедитесь, что у вас установлены:
   - Python 3.x
   - PySpark
   - Файл с данными `wiki.txt` в той же директории

2. Запустите скрипт:
   ```
   spark-submit wiki_analyze.py
   ```

## Оптимизация

Проект включает несколько оптимизаций:
- Кэширование часто используемых DataFrame (`wiki_df` и `words_df`)
- Настройка параметров Spark для эффективной обработки:
  - Увеличение количества разделов для shuffle-операций
  - Увеличение памяти исполнителей
  - Включение адаптивного выполнения запросов

## Заключение

Этот проект демонстрирует возможности PySpark для анализа текстовых данных на русском языке, включая сложные фильтрации и агрегации. Реализованные задачи охватывают различные аспекты лингвистического анализа.
![image](https://github.com/user-attachments/assets/735764f6-e91c-4a2b-84f3-a920c1bcd6ce)
![image](https://github.com/user-attachments/assets/a6b21e5d-e378-497a-b85d-3dfa9aa3c427)
![image](https://github.com/user-attachments/assets/156baf74-fdca-4fa8-ab8d-4eea5cac2cc3)
![image](https://github.com/user-attachments/assets/cc471bfc-2d53-4e45-873c-4c1bb72b2b05)



