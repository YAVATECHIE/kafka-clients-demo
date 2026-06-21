# Kafka Clients Demo

Simple Java examples demonstrating Apache Kafka client APIs using Confluent Cloud.

## Prerequisites

* Java 17+
* Maven
* Confluent Cloud cluster
* Topic named `orders`

## Configuration

Download the client configuration from Confluent Cloud and place it in:

```text
src/main/resources/client.properties
```

## Build

```bash
mvn clean package
```

## Run

Run the desired example from your IDE.

## Included Examples

### ProducerDemo

Demonstrates:

* Asynchronous sends
* Producer callbacks
* Producer acknowledgements (`acks=all`)
* Idempotent producer
* Key-based partitioning

Expected observations:

* Records with the same key are routed to the same partition
* Producer callbacks provide delivery metadata
* Assigned partition and offset are displayed for each record

### SimpleConsumer

Demonstrates:

* Topic subscription
* Poll loop (`poll()`)
* Consumer groups
* Partition assignment
* Offsets
* Consumer restart behavior

Expected observations:

* Records are consumed from assigned partitions
* Key, value, partition, and offset information are displayed
* Restarting with the same consumer group resumes from the stored position
* Running multiple consumer instances with the same group triggers partition redistribution (rebalance)
