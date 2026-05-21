from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Инициализация Spark
spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# 2. Схема данных
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("client_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("merchant", StringType())
])

# 3. Чтение из Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Парсинг JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# 4. Логика обнаружения Fraud (Оконные функции)
# Ищем аномалии: > 3 транзакций за 1 минуту от одного клиента
windowed_df = parsed_df \
    .groupBy(window("event_time", "1 minute"), "client_id") \
    .agg(
        count("transaction_id").alias("tx_count"),
        sum("amount").alias("total_amount")
    ) \
    .filter((col("tx_count") > 3) | (col("total_amount") > 50000))

# 5. Вывод результатов в консоль
query = windowed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime='10 seconds') \
    .start()

print("⚡ Fraud Detection Stream started...")
query.awaitTermination()
