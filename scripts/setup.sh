#!/bin/bash

# Start Zookeeper
docker run -d --name zookeeper -p 2181:2181 zookeeper

# Start Kafka
docker run -d --name kafka -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka

# Start Cassandra
docker run -d --name cassandra -p 9042:9042 cassandra

# Start Redis
docker run -d --name redis -p 6379:6379 redis

# Wait for services to be up
echo "Waiting for services to start..."
sleep 30

# Create Kafka topic
docker exec kafka kafka-topics --create --topic url_frontier --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Set up Cassandra schema
docker exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS crawler_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE crawler_keyspace;
CREATE TABLE IF NOT EXISTS crawled_pages (
  url text PRIMARY KEY,
  timestamp timestamp,
  status_code int,
  headers text,
  html text,
  title text,
  description text,
  extracted_urls text
);"

# Run the crawler
echo "Starting the crawler..."
go run cmd/crawler/main.go
