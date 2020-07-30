#!/usr/bin/env bash

docker-compose up -d --force-recreate

until [ $(docker inspect --format='{{json .State.Health}}' connect | jq .Status) = "\"healthy\"" ]
do

  echo "Waiting for Kafka Connect to be ready..."
  sleep 10

done

echo "Kafka Connect is ready..."

sleep 3

curl --silent --output /dev/null -i -X POST -H "Accept:application/json" \
  -H  "Content-Type:application/json" http://localhost:8083/connectors/ \
  -d '{
    "name": "mongo-source",
    "config": {
      "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "connection.uri": "mongodb://conn-user:conn-pw@mongo:27017/?replicaSet=replica",
      "copy.existing": true,
      "database": "monitor",
      "collection": "to_kafka",
      "topic.prefix": "mongo",
      "key.converter.schema.registry.url": "http://schema-registry:8081",
      "value.converter.schema.registry.url": "http://schema-registry:8081"
    }
  }'

echo ""

sleep 3
curl -s localhost:8083/connectors/mongo-source/status | jq

sleep 2
curl --silent --output /dev/null -i -X POST -H "Accept:application/json" \
  -H  "Content-Type:application/json" http://localhost:8083/connectors/ \
  -d '{
    "name": "mongo-sink",
    "config": {
      "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
      "connection.uri": "mongodb://conn-user:conn-pw@mongo:27017/?replicaSet=replica",
      "database": "monitor",
      "collection": "from_kafka",
      "topics.regex": "mongo.monitor.wits"
    }
  }'

echo ""

sleep 3
curl -s localhost:8083/connectors/mongo-sink/status | jq

# curl -X DELETE http://localhost:8083/connectors/mongo-sink
# curl -X DELETE http://localhost:8083/connectors/mongo-source
