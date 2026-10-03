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

## Redis commands

#### Running Redis container

1. docker exec -it redis redis-cli

#### Inspecting the transaction feature store

Run these commands inside `redis-cli`. Transaction records are stored as JSON strings, not Redis hashes, so use `GET` rather than `HGETALL`.

1. Check the type of the time-window index:
   `TYPE fraud:features:window`
   Expected type: `zset`.
2. List indexed transaction keys and their ingestion timestamps (scores):
   `ZRANGE fraud:features:window 0 -1 WITHSCORES`
3. Find transaction keys by prefix:
   `SCAN 0 MATCH fraud:features:transaction:* COUNT 100`
   If Redis returns a non-zero cursor, use that cursor in the next `SCAN` command and continue until the cursor is `0`.
4. Print one transaction record (replace `<trans_num>` with the transaction number):
   `GET fraud:features:transaction:<trans_num>`
5. Check how many seconds remain before that transaction key expires:
   `TTL fraud:features:transaction:<trans_num>`
   With a 30-minute window, a newly written record should have a TTL no greater than 1800 seconds.
6. List all transaction JSON records in the current 30-minute ingestion-time window:
   `EVAL "local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000); local cutoff='('..tostring(now-1800000); local members=redis.call('ZRANGEBYSCORE',KEYS[1],cutoff,'+inf'); local records={}; for _,k in ipairs(members) do local v=redis.call('GET',k); if v then table.insert(records,v) end end; return records" 1 fraud:features:window`
   The `1800000` value is 30 minutes in milliseconds. Adjust it if `WINDOW_TIME` changes. This lists records by Redis ingestion time, not transaction event time.
