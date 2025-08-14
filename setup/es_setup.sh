#!/bin/bash

# Ожидаем доступности Kafka Connect
while [ $(curl -s -o /dev/null -w %{http_code} http://kafka-connect:8083/connectors) -ne 200 ]
do
  echo "Waiting for Kafka Connect to be ready..."
  sleep 5
done

# Создаем коннектор для заказов
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "elasticsearch-sink-orders",
    "config": {
      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
      "tasks.max": "1",
      "topics": "orders",
      "insert.mode": "upsert",
      "connection.url": "http://elasticsearch:9200",
      "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
      "value.converter.schema.registry.url": "http://schema-registry:8081",
      "value.converter.schemas.enable": "true",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable": "false",
      "key.ignore": "true",
      "transforms": "extractTimestamp",
      "transforms.extractTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.extractTimestamp.timestamp.field": "@timestamp",
      "behavior.on.malformed.documents": "ignore",
      "behavior.on.null.values": "ignore",
      "producer.override.ssl.truststore.location": "/etc/kafka/secrets/kafka111.truststore.jks",
      "producer.override.ssl.truststore.password": "your-password",
      "consumer.override.ssl.truststore.location": "/etc/kafka/secrets/kafka111.truststore.jks",
      "consumer.override.ssl.truststore.password": "your-password"
    }
  }'
  

# Создаем коннектор для запросов клиентов
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "elasticsearch-sink-search",
    "config": {
      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
      "tasks.max": "1",
      "topics": "client-search",
      "connection.url": "http://elasticsearch:9200",
      "insert.mode": "upsert",
      "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
      "value.converter.schema.registry.url": "http://schema-registry:8081",
      "value.converter.schemas.enable": "true",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable": "false",
      "key.ignore": "true",
      "transforms": "extractTimestamp",
      "transforms.extractTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.extractTimestamp.timestamp.field": "@timestamp",
      "behavior.on.malformed.documents": "ignore",
      "behavior.on.null.values": "ignore",
      "producer.override.ssl.truststore.location": "/etc/kafka/secrets/kafka111.truststore.jks",
      "producer.override.ssl.truststore.password": "your-password",
      "consumer.override.ssl.truststore.location": "/etc/kafka/secrets/kafka111.truststore.jks",
      "consumer.override.ssl.truststore.password": "your-password"
	  }
  }'


# Создаем коннектор для фильтрованных товаров
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "elasticsearch-sink-filtered-items",
    "config": {
      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
      "tasks.max": "1",
      "topics": "filtered-items",
      "connection.url": "http://elasticsearch:9200",
      "insert.mode": "upsert",
      "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
      "value.converter.schema.registry.url": "http://schema-registry:8081",
      "value.converter.schemas.enable": "true",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable": "false",
      "key.ignore": "true",
      "transforms": "extractTimestamp",
      "transforms.extractTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
      "transforms.extractTimestamp.timestamp.field": "@timestamp",
      "behavior.on.malformed.documents": "ignore",
      "behavior.on.null.values": "ignore",
      "producer.override.ssl.truststore.location": "/etc/kafka/secrets/kafka111.truststore.jks",
      "producer.override.ssl.truststore.password": "your-password",
      "consumer.override.ssl.truststore.location": "/etc/kafka/secrets/kafka111.truststore.jks",
      "consumer.override.ssl.truststore.password": "your-password"
	  }
  }'
  
  
# Создаем коннектор для рекомендаций
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "elasticsearch-sink-recommendations",
    "config": {
	"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
	"behavior.on.null.values": "ignore",
	"key.converter.schemas.enable": "false",
	"tasks.max": "1",
	"topics": "recommendations",
	"value.converter.schemas.enable": "false",
	"name": "elasticsearch-sink-recommendations",
	"key.ignore": "true",
	"value.converter": "org.apache.kafka.connect.json.JsonConverter",
	"connection.url": "http://elasticsearch:9200",
	"schema.ignore": "true",
	"producer.override.ssl.truststore.location": "/etc/kafka/secrets/kafka111.truststore.jks",
	"producer.override.ssl.truststore.password": "your-password",
	"consumer.override.ssl.truststore.location": "/etc/kafka/secrets/kafka111.truststore.jks",
	"consumer.override.ssl.truststore.password": "your-password"
	}
  }'
echo "Elasticsearch connectors configured successfully"