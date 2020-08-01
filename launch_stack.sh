#!/usr/bin/env bash

docker-compose pull

sudo pkill mysql
sudo pkill mysqld

docker-compose up -d --force-recreate

until [ $(docker inspect --format='{{json .State.Health}}' kafka-connect-onprem | jq .Status) = "\"healthy\"" ]
do

  status=$(docker inspect --format='{{json .State.Health}}' kafka-connect-onprem | jq .Status)
  python3 -c "print('Waiting for Kafka Connect to be ready...[' + '$status'[1:-1].upper() + ']')"
  sleep 10

done

echo "Kafka Connect is ready..."

sleep 3

curl -i -X POST -H "Accept:application/json" \
  -H  "Content-Type:application/json" http://localhost:18083/connectors/ \
  -d '{
    "name": "mysql-source-connector",
    "config": {
          "connector.class": "io.debezium.connector.mysql.MySqlConnector",
          "database.hostname": "mysql",
          "database.port": "3306",
          "database.user": "mysqluser",
          "database.password": "mysqlpw",
          "database.server.id": "12345",
          "database.server.name": "workshop",
          "database.whitelist": "orders",
          "table.blacklist": "orders.out_of_stock_events",
          "database.history.kafka.bootstrap.servers": "broker:29092",
          "database.history.kafka.topic": "debezium_dbhistory" ,
          "include.schema.changes": "false",
          "snapshot.mode": "when_needed",
          "transforms": "unwrap,TopicRename,extractKey",
          "transforms.unwrap.type": "io.debezium.transforms.UnwrapFromEnvelope",
          "transforms.TopicRename.type": "org.apache.kafka.connect.transforms.RegexRouter",
          "transforms.TopicRename.regex": "(.*)\\.(.*)\\.(.*)",
          "transforms.TopicRename.replacement": "$1_$3",
          "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
          "transforms.extractKey.field": "id",
          "key.converter": "org.apache.kafka.connect.converters.IntegerConverter"
      }
  }'


echo ""

sleep 3
curl -s localhost:18083/connectors/mysql-source-connector/status | jq

sleep 2
curl -i -X POST -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:18083/connectors/ \
    -d '{
        "name": "jdbc-mysql-sink",
        "config": {
          "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
          "topics": "out_of_stock_events",
          "connection.url": "jdbc:mysql://mysql:3306/orders",
          "connection.user": "mysqluser",
          "connection.password": "mysqlpw",
          "insert.mode": "INSERT",
          "batch.size": "3000",
          "auto.create": "true",
          "key.converter": "org.apache.kafka.connect.storage.StringConverter"
       }
    }'

echo ""

sleep 3
curl -s localhost:18083/connectors/jdbc-mysql-sink/status | jq

# curl -X DELETE http://localhost:8083/connectors/mongo-sink
# curl -X DELETE http://localhost:8083/connectors/mongo-source
