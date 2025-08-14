import os
import random
import requests
import json
import logging
import time

from confluent_kafka import Producer
from datetime import datetime, timezone
from confluent_kafka.serialization import StringSerializer
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import JsonMessageSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient as con

from faker import Faker

# Настройка логирования
logging.getLogger().setLevel(logging.INFO)

URL = "https://raw.githubusercontent.com/vfx-beavers/DE_libs/refs/heads/main/_data/kafka/product_img.json"
KAFKA_ORDERS_TOPIC = os.getenv("KAFKA_ORDERS_TOPIC", "orders")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka111:9093") #127.0.0.1:29092  127.0.0.1:9095
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081/")

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, 'orders_json_schema.json')
schema_registry_client = SchemaRegistryClient(url=SR_URL)
confluent_SR_client = con({"url": SR_URL})
subject = f'{KAFKA_ORDERS_TOPIC}-value'
SASL_USER = os.getenv("SASL_USER", "producer")
SASL_PASSWORD = os.getenv("SASL_PASSWORD", "producer-pwd")

conf = {
       "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
       "acks": "all",
       "retries": 3,
       "client.id": "items-producer",
       "security.protocol": "SASL_SSL",
       "ssl.ca.location": "/etc/kafka/secrets/ca.crt",  # Сертификат центра сертификации ca
       "ssl.certificate.location": "/etc/kafka/secrets/kafka111.crt",  # Сертификат клиента Kafka cert
       "ssl.key.location": "/etc/kafka/secrets/kafka111.key",  # Приватный ключ для клиента Kafka key
     # "ssl.endpoint.identification.algorithm": "none",
       "sasl.mechanism": "PLAIN",       # Используемый механизм SASL
       "sasl.username": SASL_USER,
       "sasl.password": SASL_PASSWORD,
       "client.id": "orders-producer"
}

producer = Producer(conf)
fake = Faker() # Синтетика 

# Чтение подготовленной json схемы
def load_schema(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        schema_file = f.read()
    return schema_file

# Сериализаторы
schema_file = load_schema(file_path)
json_orders_schema_str = schema.JsonSchema(schema_file)
json_orders_serializer = JsonMessageSerializer(schema_registry_client, subject, json_orders_schema_str)
string_serializer = StringSerializer('utf_8')

# Отслеживание статуса доставки
def delivery_report(err, msg):
    if err:
        print(f"Не взлетело: {err}")
    else:
        print(f"Отправлено в топик {msg.topic()} | offset {msg.offset()}")


def json_orders_codec():
    return json_orders_serializer


# Регистрация схемы 
def register_schema(schema_registry_client, subject, json_orders_schema_str):
    try:
        schema_id = schema_registry_client.register(subject, json_orders_schema_str) # subject "items-value"
        return schema_id
    except Exception as e:
        logging.error(f"Схема не зарегистрировалась: {e}")
        raise


# Чтение JSON-источника с товарами 
def read_git_file():
    try:
        r = requests.get(URL)
        r.raise_for_status()
        item_list = r.json()
        return item_list
    except FileNotFoundError:
        logging.error(f"Файл {URL} не найден.")
        return None
    except json.JSONDecodeError:
        logging.error("Ошибка декодирования JSON.")
        return None
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
        return None
    

# Генератор заказов
def order_faker():
    random_names = ["Alice", "Bob", "Charlie", "David", "Eva"]
    items = []
    cnt = random.randint(1, 5)
    order_date = fake.date_time_between(start_date='-1y', end_date='now', tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
    order_id = str(fake.unique.pyint(min_value=265298, max_value=272267))
    customer_id = random.choice(random_names)

    for _ in range(cnt):
        product = fake.random_element(elements=read_git_file())

        items.append({
            "product_id": product["product_id"],
            "name": product["name"],
            "sku": product["sku"],
            "brand": product["brand"],
            "category": product["category"],
            "index": product["index"],
            "store_id": product["store_id"],
            "description": product["description"]})

    yield {
        "order_id": order_id,
        "customer_id": customer_id,
        "items": items,
        "email": fake.email(),
        "order_date": order_date,
        "state": random.choice(["Cancelled", "Completed", "Pending"]),
        "timestamp": datetime.utcnow().isoformat()}

# Отправка в кафку
def create_order():
    try:
        order = next(order_faker())
        order_id = order['order_id']

        # Encode message 
        order_encoded = json_orders_serializer.encode_record_with_schema(subject, json_orders_schema_str, order) 
        # 5 bytes for the schema_id
        assert len(order_encoded) > 5
        assert isinstance(order_encoded, bytes)

        time.sleep(1)
        try:
           producer.produce(topic=KAFKA_ORDERS_TOPIC, key=string_serializer(order_id), value=order_encoded, callback=delivery_report)
           producer.poll(0)

           print(f"Отправлено: {order_id}")
        except Exception as e:
            print(f"Ошибка при отправке в Kafka: {e}")
        except KeyboardInterrupt:
            print("Прервано ручками. Ctrl+C")

        return {"message": "Отправлено в Kafka", "order_id": order["order_id"]}
    except Exception as e:
        logging.info(f"Ошибка при отправке: {str(e)}")


if __name__ == "__main__":
    try:
       latest = confluent_SR_client.get_latest_version(subject)
       logging.info(f"Схема уже зарегистрирована:{subject}")
    except Exception:
       schema_id = register_schema(schema_registry_client, subject, json_orders_schema_str)
       logging.info(f"Схема зарегистрирована: {subject} с id: {schema_id}")

    items = read_git_file()

    while True:
      if items:
        create_order()
        producer.flush()
        logging.info('Ушло по трубам')
        time.sleep(10)