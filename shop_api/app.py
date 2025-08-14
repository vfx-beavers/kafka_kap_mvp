import requests
import logging
import json
import time
import os

from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient as con
from confluent_kafka.serialization import StringSerializer
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import JsonMessageSerializer

# Настройка логирования
logging.getLogger().setLevel(logging.INFO)


URL = "https://raw.githubusercontent.com/vfx-beavers/DE_libs/refs/heads/main/_data/kafka/product_img.json"
KAFKA_ITEMS_TOPIC = os.getenv("KAFKA_ITEMS_TOPIC", "items")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") # 127.0.0.1:9095 127.0.0.1:29092
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081/")
SASL_USER = os.getenv("SASL_USER", "producer")
SASL_PASSWORD = os.getenv("SASL_PASSWORD", "producer-pwd")

subject = f'{KAFKA_ITEMS_TOPIC}-value'
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, 'items_schema_img.json')
schema_registry_client = SchemaRegistryClient(url=SR_URL)
confluent_SR_client = con({"url": SR_URL})

ssl_path = Path(__file__).resolve().parent.parent / 'ssl'
ca = f'{ssl_path}\ca.crt'
cert = f'{ssl_path}\kafka111.crt'
key = f'{ssl_path}\kafka111.key'

# Kafka конфиг
conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, # kafka111:9092,kafka222:9093,kafka333:9094 # 127.0.0.1:29093
    "acks": "all",
    "retries": 3,
    "client.id": "items-producer",
    "security.protocol": "SASL_SSL",  #SASL_SSL SASL_PLAINTEXT
    "ssl.ca.location": "/etc/kafka/secrets/ca.crt",  # Сертификат центра сертификации ca
    "ssl.certificate.location": "/etc/kafka/secrets/kafka111.crt",  # Сертификат клиента Kafka cert
    "ssl.key.location": "/etc/kafka/secrets/kafka111.key",  # Приватный ключ для клиента Kafka key
  # "ssl.endpoint.identification.algorithm": "none",
    "sasl.mechanism": "PLAIN",       # Используемый механизм SASL
    "sasl.username": SASL_USER,
    "sasl.password": SASL_PASSWORD
}
producer = Producer(conf)


# Чтение подготовленной json схемы
def load_schema(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        schema_file = f.read()
    return schema_file


schema_file = load_schema(file_path)
avro_value_schema_str = schema.JsonSchema(schema_file)
avro_message_serializer = JsonMessageSerializer(schema_registry_client, subject, avro_value_schema_str) # subject "items-value"
string_serializer = StringSerializer('utf_8')


def avro_value_codec():
    return avro_message_serializer


# Регистрация схемы 
def register_schema(schema_registry_client, subject, avro_value_schema_str):
    try:
        schema_id = schema_registry_client.register(subject, avro_value_schema_str) # subject "items-value"
        return schema_id
    except Exception as e:
        logging.error(f"Схема не зарегистрировалась: {e}")
        raise


# Отслеживание статуса доставки
def delivery_report(err, msg):
    if err:
        print(f"Не взлетело: {err}")
    else:
        print(f"Отправлено в топик {msg.topic()} | offset {msg.offset()}")


# Функция чтения JSON-источника. Имитация Api
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


# Функция отправки сообщений
def send_to_kafka(item_list):
    for item in item_list:
        product_id = item['product_id'] # для ключа

        # Encode message
        message_encoded = avro_message_serializer.encode_record_with_schema(subject, avro_value_schema_str, item) 
        # 5 bytes for the schema_id
        assert len(message_encoded) > 5
        assert isinstance(message_encoded, bytes)

        time.sleep(1)
        try:
            producer.produce(KAFKA_ITEMS_TOPIC, key=string_serializer(product_id), value=message_encoded,callback=delivery_report)
            producer.poll(0)

            print(f"Отправлено: {product_id}")
        except Exception as e:
            print(f"Ошибка при отправке в Kafka: {e}")
        except KeyboardInterrupt:
            print("Прервано ручками. Ctrl+C")
    

if __name__ == "__main__":
    try:
       latest = confluent_SR_client.get_latest_version(subject)
       logging.info(f"Схема уже зарегистрирована:{subject}")
    except Exception:
       schema_id = register_schema(schema_registry_client, subject, avro_value_schema_str)
       logging.info(f"Схема зарегистрирована: {subject} с id: {schema_id}")

    items = read_git_file()

    while True:
      if items:
        send_to_kafka(items)
        producer.flush()
        logging.info('Ушло по трубам')
        time.sleep(60)