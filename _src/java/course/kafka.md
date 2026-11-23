Kafka is used in most modern event-driven backends. Learn the core model really well (partitions, offsets, consumer groups, delivery guarantees).

This track lists every topic to learn, in order, with mini projects to build and the best resources. Work through the topics with the resources below, then build at least two of the projects.

## Messaging fundamentals

### Point-to-point queues vs publish-subscribe

Learn this from the resources at the end of the section.

### Why event-driven architecture; events vs commands

Learn this from the resources at the end of the section.

### Kafka vs RabbitMQ vs ActiveMQ: when to use which

Learn this from the resources at the end of the section.


## Kafka core concepts

### Brokers, cluster, topics, partitions, offsets, segments

Learn this from the resources at the end of the section.

### Replication, leader/follower, ISR, replication factor, min.insync.replicas

Learn this from the resources at the end of the section.

### KRaft mode (Kafka 4.x, no ZooKeeper)

Learn this from the resources at the end of the section.

### Retention (time/size) and log compaction

Learn this from the resources at the end of the section.


## Producers

### Message keys & partitioning (ordering per key)

Learn this from the resources at the end of the section.

### acks (0, 1, all), retries, idempotent producer

Learn this from the resources at the end of the section.

### Batching, linger.ms, compression

Learn this from the resources at the end of the section.


## Consumers

### Consumer groups, partition assignment, rebalancing

Learn this from the resources at the end of the section.

### Offset commits: auto vs manual; at-most-once / at-least-once / exactly-once

Learn this from the resources at the end of the section.

### Consumer lag & monitoring

Learn this from the resources at the end of the section.

### Ordering guarantees and their limits

Learn this from the resources at the end of the section.

### Handling duplicates: idempotent consumer design

Learn this from the resources at the end of the section.


## Data & schemas

### Serialization: JSON, Avro, Protobuf

Learn this from the resources at the end of the section.

### Schema Registry & schema evolution (backward/forward compatibility)

Learn this from the resources at the end of the section.


## Spring for Apache Kafka

### KafkaTemplate & @KafkaListener; configuration via Spring Boot

Learn this from the resources at the end of the section.

### Error handling: DefaultErrorHandler, retry topics, dead-letter topics (DLT)

Learn this from the resources at the end of the section.

### Kafka transactions & exactly-once in Spring

Learn this from the resources at the end of the section.

### Testing with EmbeddedKafka or Testcontainers

Learn this from the resources at the end of the section.


## Advanced ecosystem

### Kafka Streams basics (stateless/stateful ops, KTable)

Learn this from the resources at the end of the section.

### Kafka Connect & CDC with Debezium

Learn this from the resources at the end of the section.

### Partition count choices, throughput tuning, monitoring tools

Learn this from the resources at the end of the section.

### Use cases: order events, notifications, activity tracking, log aggregation, real-time analytics

Learn this from the resources at the end of the section.

## Hands-on mini projects

Build at least two of these before moving on. Put each on GitHub with a short README.

### Kafka CLI warm-up (Beginner)

Run Kafka (KRaft) in Docker, create topics with several partitions, produce/consume from the CLI, test consumer groups and replay from offset 0.

**Practises:** Topics, partitions, offsets, consumer groups

### Real-time Notification Service (Intermediate)

Spring Boot producer publishes user events (signup, comment, like); a consumer sends emails, another pushes to the browser over WebSocket.

**Practises:** KafkaTemplate, @KafkaListener, WebSocket

### Order Events with Retries & DLT (Intermediate)

Order events where some messages fail on purpose: retry topics, a dead-letter topic, idempotent consumer (no duplicate processing), a re-processing endpoint.

**Practises:** Error handling, DLT, idempotency

### Clickstream Analytics (Advanced)

Simulate website clicks into Kafka; use Kafka Streams to compute top pages and active users per minute; show them live on a React chart.

**Practises:** Kafka Streams, windowing, real-time UI

### CDC Search Sync (Advanced)

Capture Postgres changes with Debezium into Kafka and keep an Elasticsearch/OpenSearch index in sync for product search.

**Practises:** Kafka Connect, Debezium, outbox/CDC

## Resources

::: resources
- [Apache Kafka Quickstart](https://kafka.apache.org/quickstart) · Run Kafka locally (KRaft)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/) · Official reference: design section is gold
- [Confluent Developer courses](https://developer.confluent.io/courses/) · Free Kafka 101, Spring + Kafka, Streams
- [Spring for Apache Kafka reference](https://docs.spring.io/spring-kafka/reference/) · KafkaTemplate, listeners, retries, DLT
- [Conduktor Kafkademy](https://learn.conduktor.io/kafka/) · Beginner-friendly Kafka lessons
- Book: [Kafka: The Definitive Guide (2nd ed.)](https://www.confluent.io/resources/kafka-the-definitive-guide-v2/) · Free e-book from Confluent
- [Debezium](https://debezium.io/documentation/) · Change data capture into Kafka (outbox)
- Video: [Kafka tutorials (videos)](https://www.youtube.com/results?search_query=Apache+Kafka+Spring+Boot+tutorial) · Spring Boot + Kafka walkthroughs
:::
