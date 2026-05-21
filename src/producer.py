import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Настройки
KAFKA_TOPIC = "transactions"
BOOTSTRAP_SERVERS = ['kafka:9092']

# Инициализация
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"🚀 Starting producer on topic {KAFKA_TOPIC}...")

def generate_transaction():
    return {
        "transaction_id": str(random.randint(100000, 999999)),
        "client_id": str(random.randint(1, 10)), # 10 клиентов для теста
        "amount": round(random.uniform(10.0, 50000.0), 2),
        "timestamp": datetime.now().isoformat(),
        "merchant": random.choice(["SuperMarket", "OnlineShop", "ATM", "Restaurant"])
    }

try:
    while True:
        transaction = generate_transaction()
        producer.send(KAFKA_TOPIC, value=transaction)
        print(f"Sent: {transaction['transaction_id']} | Client: {transaction['client_id']} | Amount: {transaction['amount']}")
        time.sleep(0.5)
except KeyboardInterrupt:
    print("Stopping producer...")
    producer.flush()
    producer.close()
