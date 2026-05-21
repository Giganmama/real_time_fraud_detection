# ️Real-Time Fraud Detection | Kafka + Spark Structured Streaming

Потоковый пайплайн для мониторинга транзакций в реальном времени. Обнаруживает аномалии (мошенничество) с использованием оконных агрегаций Spark и брокера сообщений Kafka.

## 🎯 Что решает

-  **Real-time Processing**: обработка транзакций с задержкой < 1 секунды
-  **Kafka Integration**: прием и отправка событий через Kafka Producer/Consumer
- 📊 **Windowed Aggregations**: выявление аномалий (например, > 5 транзакций за 1 минуту)
- 🐳 **Dockerized**: полный стек (Kafka, Spark, Zookeeper) в docker-compose
- 🧪 **Mock Data**: генератор тестовых транзакций для проверки логики

##  Технологический стек

- **Streaming:** Apache Spark (Structured Streaming), PySpark
- **Messaging:** Apache Kafka
- **Storage:** PostgreSQL (для хранения алертов)
- **Infrastructure:** Docker, Docker Compose
- **Language:** Python 3.10

##  Структура проекта
```
 real_time_fraud_detection/
├── docker-compose.yml # Инфраструктура (Kafka + Spark)
├── src/
│ ├── producer.py # Генератор транзакций
│ └── consumer.py # Spark Streaming Job
├── tests/
│ └── test_window_logic.py
── README.md
```

## 🚀 Быстрый старт

### 1. **Запусти инфраструктуру:**
```bash
docker-compose up -d
```

### 2. **Запусти генератор данных (Producer):**
```bash
python src/producer.py
```

### 3. **Запусти обработку (Spark Streaming):**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 src/consumer.py
```

## Логика обнаружения Fraud
Пайплайн использует Tumbling Windows (1 минута):  
- Считает количество транзакций от одного `client_id`.
- Считает сумму транзакций.
- Если `count` > 5 ИЛИ `sum` > 100,000 → триггер алерта.
