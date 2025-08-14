import logging
import os

from confluent_kafka.schema_registry import SchemaRegistryClient as con
from schema_registry.client import SchemaRegistryClient, schema

# Настройка логирования
logging.getLogger().setLevel(logging.INFO)


KAFKA_FILTERED_ITEMS_TOPIC = os.getenv("KAFKA_FILTERED_ITEMS_TOPIC", "filtered-items")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081/")
subject = f'{KAFKA_FILTERED_ITEMS_TOPIC}-value'
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, 'filtered_items_schema_img.json')
schema_registry_client = SchemaRegistryClient(url=SR_URL)
confluent_SR_client = con({"url": SR_URL})


# Чтение подготовленной json схемы
def load_schema(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        schema_file = f.read()
    return schema_file


schema_file = load_schema(file_path)
json_value_schema_str = schema.JsonSchema(schema_file)


# Регистрация схемы 
def register_schema(schema_registry_client, subject, json_value_schema_str):
    try:
        schema_id = schema_registry_client.register(subject, json_value_schema_str)
        return schema_id
    except Exception as e:
        logging.error(f"Схема не зарегистрировалась: {e}")
        raise


if __name__ == "__main__":
    try:
       latest = confluent_SR_client.get_latest_version(subject)
       logging.info(f"Схема уже зарегистрирована:{subject}")
    except Exception:
       schema_id = register_schema(schema_registry_client, subject, json_value_schema_str)
       logging.info(f"Схема зарегистрирована: {subject} с id: {schema_id}")