# Setup Kafka cluster

## Start servers
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

## Topic management
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic word-count-input --partitions 1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic word-count-output --partitions 1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --list

## Open Producer and push some messages
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic word-count-output --from-beginning

## Start the Application
    - Run the application StreamsStarterApp

## Start consumer to read message from output topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic word-count-output \
    --from-beginning --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
    

