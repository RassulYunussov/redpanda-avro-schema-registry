# Single Kafka Topic- Multiple data types using Schema Registry
A very minimal configuration applications to produce & consume multiple data types from Kafka topic using schema registry.

## Prerequisites:
- Kafka broker
- Topic "some-other-topic"
- Avro schemas uploaded to registry

## Guide
You can use Redpanda as a Kafka broker for that experiment.
All you need:
- download Redpanda [docker-compose.yml](https://docs.redpanda.com/current/get-started/quick-start/#start-streaming)
- Spin out: docker compose up
- Create a topic: docker exec -it redpanda-0 rpk topic create some-other-topic
- Create schemas in schema registry using Redpanda console
- Start applications
