This command is used to start a Kafka console producer that sends messages to the "transaction-stream" topic on a Kafka broker running on localhost at port 9092. The command is executed inside a Docker container named "kafka".

- docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic transaction-stream --bootstrap-server localhost:9092 --from-beginning

## Deleting all serialized data from kafka

#### Option 1: Without deleting kafka topic & it's internal configurations

1. docker exec -it kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name transaction-stream --add-config retention.ms=1000 (Lower retention time to 1 second:)
2. wait for 1 - 2 min to complete internal kafka log cleaner to clean records.
3. docker exec -it kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name transaction-stream --delete-config retention.ms (Reset the topic back to default retention:)

#### Option 2: With deleting kafka topic

1. docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transaction-stream
2. docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transaction-stream --partitions 1 --replication-factor 1
