import os
import faust
import logging
import json
import ssl
import time

from confluent_kafka import Producer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient as con
from confluent_kafka.serialization import StringSerializer
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import JsonMessageSerializer
from faust.types.auth import AuthProtocol


# Настройка логирования
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("main")


SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081/")
KAFKA_ITEMS_TOPIC = os.getenv("KAFKA_ITEMS_TOPIC", "items")
KAFKA_ITEMS_FILTERED_TOPIC = os.getenv("KAFKA_ITEMS_FILTERED_TOPIC", "filtered-items")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka111:9093") #localhost:9095
SASL_USERNAME= os.getenv("SASL_USERNAME", "admin")
SASL_PASSWORD= os.getenv("SASL_PASSWORD", "your-password")

confluent_SR_client = con({"url": SR_URL})
SR_client = SchemaRegistryClient(url=SR_URL)
subject = f'{KAFKA_ITEMS_TOPIC}-value'
f_subject = f'{KAFKA_ITEMS_FILTERED_TOPIC}-value'
latest = confluent_SR_client.get_latest_version(subject)
f_latest = confluent_SR_client.get_latest_version(f_subject)
schema_file = latest.schema.schema_str
f_schema_file = f_latest.schema.schema_str
json_value_schema_str = schema.JsonSchema(schema_file)
f_json_value_schema_str = schema.JsonSchema(f_schema_file)

json_message_serializer = JsonMessageSerializer(SR_client, f_subject, f_json_value_schema_str)
json_message_deserializer = JsonMessageSerializer(SR_client)
string_deserializer = StringDeserializer('utf_8')

file_path = "/tmp/app/lib/items_stop_list.json"
#file_path = "/mnt/e/DE/Kafka/Dev/local/stream_filter/lib/items_stop_list.json"


def json_value_codec():
    return json_message_serializer


# Kafka конфиг
out_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "client.id": "filtered-items-producer",
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "/etc/kafka/secrets/ca.crt",  # Сертификат центра сертификации ca
    "ssl.certificate.location": "/etc/kafka/secrets/kafka111.crt",  # Сертификат клиента Kafka cert
    "ssl.key.location": "/etc/kafka/secrets/kafka111.key",  # Приватный ключ для клиента Kafka key
    "sasl.mechanism": "PLAIN",       # Используемый механизм SASL
    "sasl.username": SASL_USERNAME,
    "sasl.password": SASL_PASSWORD
}

producer = Producer(out_conf)
ssl_context = ssl.create_default_context(cafile="/etc/kafka/secrets/ca.crt")
broker_credentials = faust.SASLCredentials(
    mechanism=faust.types.auth.SASLMechanism.PLAIN,
    ssl_context=ssl_context,
    username=SASL_USERNAME,
    password=SASL_PASSWORD,
)
broker_credentials.protocol = AuthProtocol.SASL_SSL

app = faust.App(
    'stream_filter',
    broker=KAFKA_BOOTSTRAP_SERVERS,
    broker_credentials=broker_credentials,
    value_serializer="raw",
    store='rocksdb://',
    topic_partitions=3
)

items_topic = app.topic(KAFKA_ITEMS_TOPIC)
filtered_items_topic = app.topic(KAFKA_ITEMS_FILTERED_TOPIC)

table = app.Table("blocked_table", default=list, partitions=3)
blocked_items = set()

@app.timer(interval=10.0)  # Обновление стоплиста каждые 10 секунд
async def stop_list_update():
    load_stop_list(file_path)
    logger.info(">>> Обновление списка")


def load_stop_list(file_path: str):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
               content = json.load(file)
               blocked_items.update(content['product_id'])
        logger.info(f">>> Список заблокированных товаров: {blocked_items}")
    except FileNotFoundError:
        logger.error(f">>> Список пуст: {file_path}.")


@app.command()
async def stop():
     product_id = input(">>> Введите Id продукта для блокировки (5 цифр, например 12356) : ")
     blocked_items.add(product_id)
     write_blocked_items()
     print(f">>> {product_id} Товар заблокирован : ")


def write_blocked_items():
    with open(file_path, 'w', encoding="utf-8") as file:
        json.dump({'product_id': list(blocked_items)}, file)


@app.agent(items_topic)
async def processor(stream):
     async for message in stream:
        try:
            message_decoded = json_message_deserializer.decode_message(message)
            #assert message_decoded == user_record
            if message_decoded['product_id'] not in blocked_items:
                 try:
                   # Encode message
                   message_encoded = json_message_serializer.encode_record_with_schema(subject, f_json_value_schema_str, message_decoded)
                   # 5 bytes for the schema_id
                   assert len(message_encoded) > 5
                   assert isinstance(message_encoded, bytes)

                   await filtered_items_topic.send(value=message_encoded) # key=key,

                   logger.info(f"product_id: {message_decoded['product_id']}, name: {message_decoded['name']}")

                 except:
                    logging.error(f"Не удалось доставить сообщение с товаром {message_decoded['product_id']}", exc_info=True)
            else:
                logging.warning(f"Товар запрещён к продаже: {message_decoded['name']}")
        except Exception as e:
            logging.error(f"Не удалось декодировать сообщение: {e}")
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    app.main()