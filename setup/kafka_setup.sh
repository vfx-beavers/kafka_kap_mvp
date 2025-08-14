#!/bin/bash

# Topics
kafka-topics --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --create --topic items --partitions 3 --replication-factor 2 --if-not-exists
kafka-topics --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --create --topic client-search --partitions 3 --replication-factor 2 --if-not-exists
kafka-topics --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --create --topic filtered-items --partitions 3 --replication-factor 2 --if-not-exists
kafka-topics --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --create --topic orders --partitions 3 --replication-factor 2 --if-not-exists
kafka-topics --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --create --topic recommendations --partitions 3 --replication-factor 2 --if-not-exists
kafka-topics --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --create --topic analytic-topic --partitions 3 --replication-factor 2 --if-not-exists

# ACL
kafka-acls --authorizer-properties zookeeper.connect=zookeeper-1:2181 --add --allow-principal User:admin --operation All --cluster
kafka-topics --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --list

kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Write --topic items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Read --topic items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Describe --topic items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Write --topic items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Read --topic items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Describe --topic items


kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Write --topic filtered-items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Read --topic filtered-items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Describe --topic filtered-items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Write --topic filtered-items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Read --topic filtered-items
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Describe --topic filtered-items


kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Write --topic client-search
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Read --topic client-search
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Describe --topic client-search
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Write --topic client-search
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Read --topic client-search
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Describe --topic client-search


kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Write --topic orders
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Read --topic orders
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Describe --topic orders
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Write --topic orders
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Read --topic orders
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Describe --topic orders


kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Write --topic recommendations
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Read --topic recommendations
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Describe --topic recommendations
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Write --topic recommendations
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Read --topic recommendations
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Describe --topic recommendations


kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Write --topic analytic-topic
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Read --topic analytic-topic
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation Describe --topic analytic-topic
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Write --topic analytic-topic
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Read --topic analytic-topic
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation Describe --topic analytic-topic


kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:producer --operation read --group "*"
kafka-acls --bootstrap-server kafka111:9093 --command-config /etc/kafka/secrets/admin.properties --add --allow-principal User:consumer --operation read --group "*"

echo "Kafka setup Ok"
