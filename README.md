# Kafka Clients Demo

Simple Java examples demonstrating Apache Kafka client APIs using Confluent Cloud.

## Prerequisites

- Java 17+
- Maven
- Confluent Cloud cluster
- Topic named `orders`

## Configuration

Download the client configuration from Confluent Cloud and place it in:

```text
src/main/resources/client.properties
```

## Run

```bash
mvn compile exec:java
```

or run `ProducerDemo` from your IDE.

## Included Examples

- Kafka Producer
    - Asynchronous sends
    - Callbacks
    - Producer acknowledgements (`acks=all`)
    - Idempotent producer
    - Key-based partitioning

## Expected Outcome

The producer publishes sample order events and displays the assigned partition and offset for each record.