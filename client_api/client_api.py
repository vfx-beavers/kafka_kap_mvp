import argparse
import random
import os
import time

from confluent_kafka import Producer
from datetime import datetime
from elasticsearch import Elasticsearch
from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import JsonMessageSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient as con


KAFKA_SEARCH_TOPIC = os.getenv("KAFKA_SEARCH_TOPIC", "client-search")
KAFKA_RECOMEND_TOPIC = os.getenv("KAFKA_RECOMEND_TOPIC", "recommendations")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka111:9093")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081/")
SASL_USER = os.getenv("SASL_USER", "producer")
SASL_PASSWORD = os.getenv("SASL_PASSWORD", "producer-pwd")

SR_client = SchemaRegistryClient(url=SR_URL)
confluent_SR_client = con({"url": SR_URL})
subject = f'{KAFKA_SEARCH_TOPIC}-value'
latest = confluent_SR_client.get_latest_version(subject)
schema_file = latest.schema.schema_str
json_value_schema_str = schema.JsonSchema(schema_file)
json_message_serializer = JsonMessageSerializer(SR_client, subject, json_value_schema_str) 


# function used to register the codec
def json_value_codec():
    return json_message_serializer

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "client.id": "client-search-producer",
    "security.protocol": "SASL_SSL",  #SASL_SSL SASL_PLAINTEXT
    "ssl.ca.location": "/etc/kafka/secrets/ca.crt",  # Сертификат центра сертификации ca
    "ssl.certificate.location": "/etc/kafka/secrets/kafka111.crt",  # Сертификат клиента Kafka cert
    "ssl.key.location": "/etc/kafka/secrets/kafka111.key",  # Приватный ключ для клиента Kafka key
    "sasl.mechanism": "PLAIN",       # Используемый механизм SASL
    "sasl.username": SASL_USER,
    "sasl.password": SASL_PASSWORD
}

# Отслеживание статуса доставки
def delivery_report(err, msg):
        if err:
           print(f"Не взлетело: {err}")
        else:
           print(f"Отправлено в топик {msg.topic()} | offset {msg.offset()}")

class ClientAPI:
    def __init__(self):
        self.es = Elasticsearch(["http://elasticsearch:9200"])
        self.producer = Producer(conf)

    

    # Поиск в ES
    def search_item(self, query):
        es_response = self.es.search(index="filtered-items", 
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["name^3", "description", "tags"]
                }
            }
        )

        random_names = ["Alice", "Bob", "Charlie", "David", "Eva"]
        random_user_id = random.choice(random_names)

        msg = {
            "type": "search",
            "user_id": random_user_id,
            "query": query,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Отправка запроса в Kafka
        # Encode message
        message_encoded = json_message_serializer.encode_record_with_schema(subject, json_value_schema_str, msg) 
        # 5 bytes for the schema_id
        assert len(message_encoded) > 5
        assert isinstance(message_encoded, bytes)

        time.sleep(1)
        try:
           self.producer.produce(KAFKA_SEARCH_TOPIC, value=message_encoded, callback=delivery_report)
           self.producer.poll(0)
           print(f"Запрос отправлен в kafka: {msg}")
        except Exception as e:
            print(f"Ошибка при отправке в Kafka: {e}")
        except KeyboardInterrupt:
            print("Прервано ручками. Ctrl+C")

        self.producer.flush()

        return es_response['hits']['hits']

    # Эмуляция рекомендаций
    def get_offer(self, user_id):
        es_recommend = self.es.search(index="recommendations", 
            query={
                "multi_match": {
                    "query": user_id,
                    "fields": ["user_id"]
                }
            }
        )

        print(f"Ожидание рекомендаций для: {user_id}")
        return es_recommend['hits']['hits']


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client API")
    subparsers = parser.add_subparsers(dest="command")

    search_parser = subparsers.add_parser("search")
    search_parser.add_argument("--query", required=True, help="Название товара для поиска")

    rec_parser = subparsers.add_parser("recommend")
    rec_parser.add_argument("--user_id", required=True, help="Идентификатор пользователя")

    args = parser.parse_args()
    api = ClientAPI()

    if args.command == "search":
        search_results = api.search_item(args.query)
        print(f"Нашлось {len(search_results)} товаров:")
        for item in search_results:
            print(f"Категория: {item['_source']['category']} -> {item['_source']['name']}")
    elif args.command == "recommend":
        offer_results = api.get_offer(args.user_id)
        for offer in offer_results:
            print(f"Рекомендуемые товары (id): {offer['_source']['product_id']}")
    else:
        parser.print_help()
