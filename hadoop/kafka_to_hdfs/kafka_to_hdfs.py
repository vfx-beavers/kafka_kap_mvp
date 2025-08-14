from kafka import KafkaConsumer
import time
import os
import pyhdfs
import logging

from schema_registry.client import SchemaRegistryClient, schema
from schema_registry.serializers import JsonMessageSerializer


# Настройка логирования
logging.getLogger().setLevel(logging.INFO)

SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081/")
KAFKA_ORDERS_TOPIC = os.getenv("KAFKA_ORDERS_TOPIC", "orders")
hdfs_client = pyhdfs.HdfsClient(hosts="hadoop-namenode:9870", user_name="root")
SR_client = SchemaRegistryClient(url=SR_URL)
json_deserializer = JsonMessageSerializer(SR_client)


# Создание папок в hdfs
userhomedir = hdfs_client.get_home_directory()
print(userhomedir)
availablenode = hdfs_client.get_active_namenode()
print(availablenode)
print(hdfs_client.listdir("/"))
hdfs_client.mkdirs('/data/orders')
print(hdfs_client.list_status('/data/orders'))


# Проверка и создание пустышек клиентов в hdfs
random_names = ["Alice", "Bob", "Charlie", "David", "Eva"]

for name in random_names:
     hdfs_json_files = f"/data/orders/{name}.json"

     if not hdfs_client.exists(f"/data/orders/{name}.json"):
        logging.info(f"Файл не существует → создаём новый: {name}")
        hdfs_client.create(hdfs_json_files, '')
     else:
        logging.info(f"Файл существует → пишем в него")


# Назначение прав в hdfs
hdfs_client.set_permission("/data", permission=777)
hdfs_client.set_permission("/data/orders", permission=777, recursive=True)


# Чтение и отправка в hdfs
def main():
  time.sleep(2)
  try:
      consumer = KafkaConsumer(
          'orders',
          bootstrap_servers=['kafka111-mirror:9192'],
          auto_offset_reset='earliest',
          enable_auto_commit=True,
          group_id='kafka-to-hdfs-consumer-group',
          #security_protocol='SASL_PLAINTEXT',
          ssl_cafile='/etc/kafka/secrets/ca.crt',
          sasl_mechanism='PLAIN',
          sasl_plain_username='admin',
          sasl_plain_password='your-password',
    )
  except:
      raise Exception('kafka connect error')

  for message in consumer:
      message = message.value
      message_decoded = json_deserializer.decode_message(message)
      customer_id = message_decoded.get('customer_id')
      print(customer_id)

      hdfs_json_file_path = f"/data/orders/{customer_id}.json"
      print(hdfs_json_file_path)
      
      hdfs_client.append(hdfs_json_file_path, (str(message_decoded) + "\n").encode('utf-8'))
      print(message_decoded)
    

if __name__ == "__main__":
    main()