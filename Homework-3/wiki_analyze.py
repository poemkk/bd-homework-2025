# Импорт необходимых библиотек
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import re

# Инициализация Spark
spark = SparkSession.builder \
    .appName("RussianWikiAnalysis") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Определение регулярных выражений
RU_WORD_PATTERN = r"\b[а-яёА-ЯЁ][а-яёА-ЯЁ\-]*[а-яёА-ЯЁ]\b"  # Строгое соответствие русским словам
LATIN_PATTERN = r"\b[A-Za-z]+\b"  # Строгое соответствие словам с латинскими буквами (без дефисов)
ABBREV1_PATTERN = r"\b[а-яё]{1,3}\."  # Сокращения вида "пр.", "др."
NAME_PATTERN = r"\b[А-ЯЁ][а-яё]{1,20}(?:\s+[А-ЯЁ][а-яё]{1,20})?\b"  # Имена: одно имя или имя + фамилия

# Списки русских имен и фамилий (пример)
RUSSIAN_NAMES = [
    "Александр", "Алексей", "Андрей", "Анна", "Виктор", "Владимир", "Дмитрий", "Екатерина",
    "Елена", "Иван", "Мария", "Михаил", "Наталья", "Ольга", "Павел", "Петр", "Сергей",
    "Татьяна", "Юлия", "Игорь", "Николай", "Светлана"
]
RUSSIAN_SURNAMES = [
    "Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов", "Попов", "Васильев", "Соколов",
    "Михайлов", "Новиков", "Федоров", "Морозов", "Волков", "Алексеев", "Лебедев", "Семенов"
]
RUSSIAN_NAME_LIST = RUSSIAN_NAMES + RUSSIAN_SURNAMES

# Список распространенных несобственных имен
NON_NAME_WORDS = [
    "России", "Россия", "После", "Однако", "При", "Это", "Для", "Также", "Так",
    "Российской", "Кроме", "Согласно", "Германии", "Украины", "Как", "Если", "Его",
    "Федерации", "Франции", "Например", "Республики", "Совета", "Северной", "Москвы",
    "Москва", "Петербург", "СССР", "США", "Европы"
]

# 1. Загрузка и парсинг данных
def load_wiki_data(path):
    """Чтение файла wiki.txt и преобразование в структурированные данные"""
    raw = spark.read.text(path).withColumn("row_id", monotonically_increasing_id())

    # Извлечение статей (URL, заголовок, начало содержимого)
    articles = raw.filter(col("value").rlike(r"^https?://[^\s]+\t[^\t]+\t")) \
        .withColumn("article_id", monotonically_increasing_id()) \
        .withColumn("url", regexp_extract(col("value"), r"^(https?://[^\t]+)\t", 1)) \
        .withColumn("title", regexp_extract(col("value"), r"^https?://[^\t]+\t([^\t]+)\t", 1)) \
        .withColumn("content_start", regexp_extract(col("value"), r"^https?://[^\t]+\t[^\t]+\t(.*)", 1))

    # Назначение article_id для всех строк
    raw_with_article_id = raw.withColumn(
        "article_id",
        last(
            when(col("value").rlike(r"^https?://[^\s]+\t[^\t]+\t"), monotonically_increasing_id()),
            ignorenulls=True
        ).over(Window.orderBy("row_id"))
    )

    # Объединение оставшегося содержимого статей
    content = raw_with_article_id.filter(~col("value").rlike(r"^https?://[^\s]+\t[^\t]+\t")) \
        .filter(col("article_id").isNotNull()) \
        .groupBy("article_id") \
        .agg(concat_ws("\n", collect_list(col("value"))).alias("content_rest"))

    # Объединение данных и возврат результата
    return articles.join(content, "article_id", "left") \
        .withColumn("content", concat_ws("\n", col("content_start"), col("content_rest"))) \
        .select("url", "title", "content").cache()

# Загрузка данных
wiki_df = load_wiki_data("wiki.txt")
print(f"Загружено {wiki_df.count()} статей")

# 2. Обработка слов (общая для задач 1-4)
words_df = wiki_df.select(
    explode(
        split(
            regexp_replace(col("content"), r"[^а-яёА-ЯЁA-Za-z\s]", ""),  # Удаление всех символов, кроме букв и пробелов
            r"\s+"
        )
    ).alias("raw_word")
).filter(
    length(col("raw_word")) > 1  # Длина слова > 1
).withColumn(
    "word", regexp_replace(lower(col("raw_word")), "ё", "е")  # Нормализация: замена "ё" на "е"
).withColumn(
    "is_russian", col("raw_word").rlike(RU_WORD_PATTERN)
).withColumn(
    "is_latin", col("raw_word").rlike(LATIN_PATTERN)
).cache()

# Задача 1: Самое длинное русское слово
longest = words_df.filter(col("is_russian")) \
    .filter(~col("raw_word").contains(".")) \
    .orderBy(length(col("raw_word")).desc(), col("raw_word")) \
    .select("raw_word").first()
print(f"\n1. Самое длинное русское слово: {longest['raw_word'] if longest else 'нет'} (длина: {len(longest['raw_word']) if longest else 0})")

# Задача 2: Средняя длина слова
avg_len = words_df.filter(col("is_russian")) \
    .agg(avg(length(col("raw_word"))).alias("avg_length")) \
    .first()["avg_length"]
print(f"2. Средняя длина слова: {avg_len:.2f} символов")

# Задача 3: Самое частое латинское слово
top_latin = words_df.filter(col("is_latin")) \
    .groupBy("word").count() \
    .orderBy(col("count").desc(), col("word")) \
    .first()
print(f"3. Самое частое латинское слово: '{top_latin['word'] if top_latin else 'нет'}' (встречается {top_latin['count'] if top_latin else 0} раз)")

# Задача 4: Слова с частым использованием заглавных букв (более чем в половине случаев, встречаются > 10 раз)
uppercase_stats = words_df.filter(col("is_russian")) \
    .filter(~col("raw_word").rlike(r"\.")) \
    .groupBy(lower(col("raw_word")).alias("word")) \
    .agg(
        sum(when(col("raw_word").rlike("^[А-ЯЁ]"), 1).otherwise(0)).alias("uppercase_count"),
        count("*").alias("total_count")
    ) \
    .filter((col("uppercase_count") > 10) & (col("uppercase_count") / col("total_count") > 0.5)) \
    .orderBy(col("total_count").desc())

print("\n4. Слова с частым использованием заглавных букв (>10 раз, более чем в половине случаев):")
uppercase_stats.show(20, truncate=False)

# Задача 5: Сокращения вида "пр.", "др."
abbrev1_df = wiki_df.select(
    explode(
        split(
            regexp_replace(col("content"), r"[^\sа-яёА-ЯЁ.]", ""),  # Сохранение только русских букв и точек
            r"\s+"
        )
    ).alias("abbrev")
).filter(
    col("abbrev").rlike(ABBREV1_PATTERN) & (length(col("abbrev")) <= 4)
).groupBy("abbrev").count() \
    .filter(col("count") > 10) \
    .orderBy(col("count").desc())

print("\n5. Частые сокращения вида 'пр.', 'др.' (>10 раз):")
abbrev1_df.show(20, truncate=False)

# Задача 7: Извлечение имен
names_df = wiki_df.select(
    explode(
        split(
            regexp_replace(col("content"), r"[^а-яёА-ЯЁ\s]", ""),  # Сохранение только русских букв и пробелов
            r"\s+"
        )
    ).alias("name")
).filter(
    col("name").rlike(NAME_PATTERN) &
    (length(col("name")) > 2) &
    (~col("name").isin(NON_NAME_WORDS)) &
    (col("name").isin(RUSSIAN_NAME_LIST) | col("name").rlike(r"\b[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\b"))
).groupBy("name").count() \
    .filter(col("count") > 5) \
    .orderBy(col("count").desc())

print("\n7. Частые имена (>5 раз):")
names_df.show(20, truncate=False)

# Очистка кэша и завершение работы Spark
wiki_df.unpersist()
words_df.unpersist()
spark.stop()
