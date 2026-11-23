This section is about **streaming systems**: the logs and brokers that carry events, and the processing engines that compute on them while they are still arriving. It is written for data engineers who will build pipelines that must react in seconds, feed real-time OLAP engines and operational systems, or simply replace a nightly batch with something fresher. It assumes the batch and streaming distinction from Data engineering foundations, Spark and Structured Streaming from Apache Spark, Flink and Beam from Flink, Beam, Trino and other engines, and the real-time OLAP engines from the previous section, which consume the streams built here.

Apache Kafka, Spark Structured Streaming and the main streaming concepts already have a full, deep companion guide on this site, the **PySpark + Kafka guide**. Topics that overlap with it are complete, beginner-friendly articles here, with a link into the guide for internals, tuning and failure modes. Topics the guide does not cover (delivery guarantees across engines, backpressure, Lambda and Kappa, MirrorMaker 2, schema registries, Pulsar, Redpanda and streaming SQL) are written in full.

**What you will be able to do afterwards**

- Explain event time, processing time, watermarks and late data, and choose window types and triggers for a streaming aggregation.
- State precisely what at-most-once, at-least-once and exactly-once mean, and design an end-to-end pipeline that does not lose or double-count events.
- Describe Kafka's architecture (brokers, partitions, replicas, KRaft, tiered storage) and configure producers and consumers for durability and throughput.
- Explain how backpressure keeps a pipeline stable when one stage is slow.
- Choose between Lambda, Kappa and unified architectures, and between Kafka, Pulsar and Redpanda.
- Use a schema registry and compatibility rules so producers and consumers can evolve independently, and know where Kafka Streams, ksqlDB and streaming SQL engines such as Flink SQL, Materialize and RisingWave fit.

**The big picture.** A streaming platform has two halves. The first is a durable, ordered, replayable **log**: producers append events, the log keeps them for days or forever, and any number of consumers read at their own pace from positions (offsets) they control. Kafka is the standard; Pulsar, Redpanda and Google Cloud's Pub/Sub play the same role. The second half is **stream processing**: programs that read the log continuously and transform, join and aggregate it. Because events arrive late, out of order and sometimes twice, the hard problems are about **time** (when did it happen versus when did we see it?) and **correctness** (does every event count exactly once, even after a crash?). Every topic in this section is one of those two halves, or one of those two problems.

[[fig:streaming_platform]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Concepts

This group is engine-neutral. It gives you the vocabulary that every streaming engine shares: two kinds of time, windows, delivery guarantees and how they are achieved, how a pipeline protects itself when one stage is slower than the others, and the two classic architectures for combining batch and streaming.

Read it before the Kafka group. Kafka's idempotent producer and transactions only make sense once you know what "exactly once" promises, and Kafka Streams' windows are the same windows described here.

### Event Time vs. Processing Time

::: plain
A postcard is written on a beach on Monday (event time) and arrives on Friday (processing time). If you count "postcards written per day", you must file it under Monday, even though it arrived on Friday. And you must decide how long to wait for more of Monday's postcards before you announce Monday's total, knowing that one might still turn up next month.
:::

Every event has (at least) two timestamps:

- **Event time**: when the thing happened, recorded by the device or service that produced it.
- **Processing time**: when the stream processor sees it.

The gap between them (**skew** or **lag**) varies: a phone offline in a tunnel sends events minutes late, a retrying producer sends them seconds late, a backfill sends them days late. If you aggregate by processing time, results depend on network delays and on when the job happened to run; replaying the same data gives different answers. Aggregating by **event time** gives answers that describe the real world and are reproducible, but raises a question: when is a time period complete?

A **watermark** answers it. A watermark is the processor's estimate that "no more events with event time earlier than *W* are expected". Most engines compute it heuristically as the largest event time seen so far minus an allowed delay (Spark, Flink's bounded out-of-orderness strategy), or from source metadata (Dataflow estimates Pub/Sub watermarks from the oldest unacknowledged messages). When the watermark passes the end of a window, the window can produce its result.

Events that arrive after the watermark has passed their window are **late data**. Engines offer three answers:

1. **Drop** them (the default in many engines).
2. **Allowed lateness** (Beam, Flink): keep window state for an extra period after the watermark and emit an **updated** result when a late event arrives.
3. **Side outputs** (Flink `sideOutputLateData`): send too-late events to a separate stream, for example to a table that a daily batch job reconciles.

This simulation shows all three behaviours with 60-second windows, a 10-second watermark delay and 30 seconds of allowed lateness:

```python
# Event-time tumbling windows of 60 s with a watermark that trails the
# largest event time seen by 10 s, and 30 s of allowed lateness.
WINDOW, DELAY, LATENESS = 60, 10, 30
arrivals = [  # (event_time_seconds, value) in arrival order
    (5, 1), (20, 1), (64, 1), (50, 1), (75, 1), (58, 1), (130, 1), (40, 1),
]
counts, max_seen, fired = {}, 0, set()
for t, v in arrivals:
    start = t - t % WINDOW
    watermark = max_seen - DELAY
    if start + WINDOW + LATENESS <= watermark:
        print(f"t={t:>3} dropped (window {start} closed)")
        continue
    counts[start] = counts.get(start, 0) + v
    if start in fired:
        print(f"t={t:>3} late update: window {start} -> {counts[start]}")
    max_seen = max(max_seen, t)
    watermark = max_seen - DELAY
    for w in sorted(counts):
        if w + WINDOW <= watermark and w not in fired:
            fired.add(w)
            print(f"watermark {watermark}: window {w} fires with {counts[w]}")
# watermark 65: window 0 fires with 3
# t= 58 late update: window 0 -> 4
# watermark 120: window 60 fires with 2
# t= 40 dropped (window 0 closed)
```

The event at 50 s arrived after one from 64 s but before the watermark passed 60, so it counted normally. The event at 58 s was late but within allowed lateness, so window 0 was updated. The event at 40 s arrived when the watermark was 120, beyond 60 + 30, so it was dropped.

In Spark Structured Streaming the same idea is one line, `withWatermark("ts", "10 minutes")`; in Flink it is a `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))` or a `WATERMARK FOR ts AS ts - INTERVAL '10' SECOND` clause in Flink SQL.

> **Pitfall:** The watermark delay is a trade-off you must choose, not discover. Too short and you drop real data; too long and every result is delayed and state grows. Measure the actual event-time lag distribution (for example its 99th percentile) and set the delay from it.

> **Pitfall:** An idle source partition (no events) holds back the watermark in engines that take the minimum across partitions, so windows never close. Configure idleness detection (Flink `withIdleness`) or make sure every partition receives traffic.

::: guide
For watermarks, late data and windowing in Spark in depth, read [Event time, watermarks and windows](pyspark-streaming-kafka-topic-map.html#eventtime) and [Streaming fundamentals](pyspark-streaming-kafka-topic-map.html#ssbasics) in the PySpark + Kafka guide.
:::

::: check
?? Why does aggregating by processing time give non-reproducible results?
Processing time depends on network delays, retries and when the job ran, so replaying the same events puts them into different buckets.

?? What is a watermark?
The processor's estimate that no more events earlier than a given event time are expected. When it passes the end of a window, the window's result can be emitted.

?? Name three ways an engine can treat an event that arrives after the watermark passed its window.
Drop it; accept it within an allowed lateness period and emit an updated result; or send it to a side output for separate handling.
:::

::: resources
- Article: [Streaming 101: the world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/) · Tyler Akidau's classic introduction to event time, processing time and watermarks
- Article: [Streaming 102](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/) · watermarks, triggers and accumulation explained with animations
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · Akidau, Chernyak and Lax; the definitive book on event-time processing
- Docs: [Flink: timely stream processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/) · event time, watermarks and lateness in Flink
- Docs: [Dataflow streaming pipelines](https://docs.cloud.google.com/dataflow/docs/concepts/streaming-pipelines) · windows, watermarks and late data on Google Cloud
:::

### Window Functions in Streaming

::: plain
A stream never ends, so "total sales" has no final answer. Windows cut the endless stream into finite pieces you can count. **Tumbling** windows are like pages of a calendar, one per hour, no overlap. **Sliding** windows are like a moving spotlight, "the last 10 minutes, updated every minute". **Session** windows follow a person's visit: they last as long as the person keeps doing things and close after a quiet gap.
:::

A **window** assigns each event to one or more finite groups based on its timestamp (normally event time). The four standard types:

| Window | Definition | Each event belongs to | Typical use |
|---|---|---|---|
| Tumbling (fixed) | Fixed size, no overlap: [0, 60), [60, 120) | Exactly one window | Counts per minute, hourly totals |
| Sliding (hopping) | Fixed size, starting every *slide*: size 60, slide 20 | size / slide windows | Moving averages, "last 10 minutes" |
| Session | Per key; closes after a gap of inactivity | One session per key, which can grow and merge | User visits, device bursts |
| Global | One window for all time | The single window | Custom triggers, running totals |

This sketch computes the windows an event belongs to:

```python
# Which windows does an event belong to? Times are in seconds.
def tumbling(t, size):
    start = t - t % size
    return [(start, start + size)]

def sliding(t, size, slide):
    last_start = t - t % slide
    return [(s, s + size) for s in range(last_start, t - size, -slide) if s >= 0][::-1]

def sessions(times, gap):
    out, start, end = [], None, None
    for t in sorted(times):
        if start is None or t >= end:        # gap exceeded: close the session
            if start is not None:
                out.append((start, end))
            start = t
        end = t + gap
    out.append((start, end))
    return out

print(tumbling(125, 60))                      # [(120, 180)]
print(sliding(125, 60, 20))                   # [(80, 140), (100, 160), (120, 180)]
print(sessions([3, 10, 14, 50, 55, 200], 30))  # [(3, 44), (50, 85), (200, 230)]
```

Session windows are special: their boundaries depend on the data, and when a late event fills a gap, two sessions **merge** into one. That makes them more expensive to keep in state.

In Flink SQL, windows are **table-valued functions** (TVFs):

```sql
SELECT window_start, window_end, page, COUNT(*) AS views
FROM TABLE(
  HOP(TABLE page_views, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE))
GROUP BY window_start, window_end, page;
```

`TUMBLE`, `HOP`, `CUMULATE` (windows that grow in steps within a period, for "so far today" figures) and `SESSION` are available. Spark uses `window(ts, "10 minutes", "1 minute")` and `session_window(ts, "30 minutes")`.

**Triggers** decide *when* a window emits results. The default is "once, when the watermark passes the end of the window". Beam (and therefore Dataflow) lets you combine:

- **Early firings**: emit speculative partial results before the window is complete, for example every 30 seconds of processing time.
- **On-time firing**: when the watermark passes the window end.
- **Late firings**: emit an update for each late event within allowed lateness.

With several firings per window, the **accumulation mode** says what each firing contains: **accumulating** (the running total so far, so downstream overwrites) or **discarding** (only what is new since the last firing, so downstream adds). Flink offers custom `Trigger`s and **evictors**, which remove elements from a window before or after the window function runs (for example keep only the last 100 elements).

> **Pitfall:** A sliding window with size 1 hour and slide 1 second puts each event into 3,600 windows. State and computation multiply accordingly. Use a coarser slide, or compute per-second tumbling aggregates and combine them.

> **Pitfall:** Accumulating results written to a sink that adds (for example appending rows that a dashboard sums) double-count. Match the accumulation mode to the sink: accumulating with upsert sinks, discarding with additive sinks.

::: guide
For window operators, output modes and state cleanup in Spark, read [Event time, watermarks and windows](pyspark-streaming-kafka-topic-map.html#eventtime) and [Stateful processing](pyspark-streaming-kafka-topic-map.html#stateful) in the PySpark + Kafka guide.
:::

::: check
?? With size 10 minutes and slide 2 minutes, how many sliding windows contain each event?
Five (10 / 2).

?? Why are session windows more expensive than tumbling windows?
Their boundaries depend on the data, so the engine keeps state per key for open sessions and must merge sessions when a late event closes a gap.

?? What is the difference between accumulating and discarding modes?
Accumulating firings contain the whole result so far (downstream should overwrite); discarding firings contain only the change since the last firing (downstream should add).
:::

::: resources
- Docs: [Flink windows](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/) · window assigners, triggers, evictors and allowed lateness
- Docs: [Beam programming guide: windowing and triggers](https://beam.apache.org/documentation/programming-guide/) · the windowing and triggers chapters used by Dataflow
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · chapters 2 to 4 on the what, where, when and how of windowed results
- Article: [Streaming 102](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/) · triggers and accumulation modes with animated examples
- Paper: [The Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) · Akidau et al., VLDB 2015; the windowing and trigger model behind Beam
:::

### Stream Processing Guarantees

::: plain
Imagine a bank clerk processing a pile of cheques when the power fails. When the lights come back, the clerk can skip the cheque that was in their hand (maybe losing it), process it again (maybe paying it twice), or check the ledger to see whether it was already paid. Processing guarantees describe which of these a system promises, and "exactly once" means "the ledger will look as if every cheque was handled once", even though some were touched twice.
:::

Failures are normal in distributed systems: machines restart, networks drop acknowledgements, jobs are redeployed. Every streaming system must decide what happens to the events that were in flight. There are three guarantees:

| Guarantee | Promise | How it is achieved | Risk |
|---|---|---|---|
| At-most-once | Each event affects the result zero or one time | Commit the read position before processing | Data loss |
| At-least-once | Each event affects the result one or more times | Commit the read position after processing; retry on failure | Duplicates |
| Exactly-once | Each event affects the result exactly one time | At-least-once delivery plus deduplication, idempotence or transactions | Complexity and latency |

The key insight: **exactly-once is about effects, not deliveries.** Networks can always deliver a message twice. Systems achieve exactly-once *results* ("effectively once") by making repeated processing harmless.

**End to end, three parts must cooperate:**

1. **A replayable source.** After a failure, the processor must be able to re-read from a known position: Kafka offsets, Pub/Sub's unacknowledged messages, files.
2. **Consistent processing state.** The processor's state (counts, windows, join buffers) and its source positions must be saved together, atomically, so that after a restart they match. Flink does this with **checkpoints**: barriers flow through the dataflow with the events, and each operator snapshots its state when the barrier passes (an adaptation of the Chandy-Lamport snapshot algorithm, called asynchronous barrier snapshotting). Spark saves offsets and state per micro-batch in its checkpoint; Dataflow commits state and output per key.
3. **An idempotent or transactional sink.** Outputs written before the crash will be written again during replay. Either the write must be **idempotent** (writing the same thing twice equals writing it once: an upsert by key, a file with a deterministic name, a `MERGE`), or it must be **transactional** and committed together with the checkpoint.

This example shows why the sink matters. A job writes three records, crashes before committing its offset, and restarts from the beginning:

```python
# At-least-once delivery: the job crashes after writing offsets 0-2 but before
# committing them, so it restarts from offset 0 and replays.
log = [("p1", 10), ("p2", 5), ("p3", 7), ("p4", 3)]   # (payment_id, amount) at offsets 0..3

append_sink, upsert_sink = [], {}
def write(offset):
    pid, amount = log[offset]
    append_sink.append(amount)       # not idempotent: duplicates add up
    upsert_sink[pid] = amount        # idempotent: keyed write, replays overwrite

for offset in range(3):              # first run: writes 0, 1, 2, then crashes
    write(offset)
for offset in range(4):              # restart from the last committed offset (0)
    write(offset)

print(sum(append_sink), sum(upsert_sink.values()))   # 47 25
```

The true total is 25. The appending sink reports 47; the upsert sink is correct.

**Two-phase commit (2PC) sinks.** When the sink cannot be idempotent (for example appending to a Kafka topic), Flink uses a two-phase commit tied to checkpoints: during a checkpoint interval the sink writes into an open **transaction**; when the checkpoint barrier arrives it **pre-commits** (flushes) the transaction; when the whole checkpoint is complete, the job manager notifies the sinks, which **commit**. If the job fails before the checkpoint completes, the transaction is aborted and the data replayed. Consumers reading with `read_committed` never see aborted data. The price is latency: output becomes visible only at checkpoint intervals.

**On Google Cloud:**

- **Dataflow** provides exactly-once processing by default in streaming jobs, deduplicating by record IDs and committing state and output together; a cheaper **at-least-once streaming mode** is available for pipelines that tolerate duplicates.
- **Pub/Sub** delivers at least once by default; **exactly-once delivery** can be enabled on pull subscriptions, which guarantees that a successfully acknowledged message is not redelivered.
- The **BigQuery Storage Write API** supports exactly-once writes through application-created streams with offsets: a retry with the same offset is rejected instead of being appended twice.

> **Pitfall:** "Exactly-once" in a product description usually means *inside* that product. Kafka transactions do not make your database write exactly-once; Dataflow's guarantee does not cover an HTTP call your code makes. Check every external side effect, and make it idempotent with a key.

> **Tip:** At-least-once plus idempotent sinks is the most common and robust design. Give every event a unique ID at its source, carry it through the pipeline, and upsert or deduplicate by it at the end.

::: deeper
**Determinism.** Replay gives the same result only if processing is deterministic. Calls to `now()`, random numbers, or lookups against a changing external service can produce different outputs on replay, so an "exactly-once" pipeline can still write inconsistent results. Derive timestamps from events and make lookups versioned where it matters.

**Checkpoint interval trade-off.** Frequent checkpoints mean less replay after failures and lower latency for transactional sinks, but more overhead. Intervals from a few seconds to a few minutes are common; with large state, use incremental checkpoints (RocksDB in Flink) so each checkpoint uploads only changes.

**Idempotence beyond keys.** For aggregates, write results keyed by (window, key) so a replayed window overwrites itself. For files, use deterministic names (job, partition, checkpoint id) and atomic renames or table-format commits (Iceberg, Delta), which are transactional by design.

> **Interview:** "Is exactly-once delivery possible?" Not over an unreliable network, in general: an acknowledgement can always be lost. Exactly-once *processing* (each event affecting the final state once) is possible, by combining replay with idempotent or transactional state updates.
:::

::: check
?? A consumer commits its offset, then processes the message, then crashes. Which guarantee does it provide?
At-most-once: the message whose offset was committed but not processed is never seen again.

?? Name the three ingredients of end-to-end exactly-once.
A replayable source, processing state checkpointed atomically with source positions, and an idempotent or transactional sink.

?? Why does a Flink two-phase commit sink add latency?
Output is committed, and becomes visible to `read_committed` readers, only when a checkpoint completes, so results appear at checkpoint intervals.
:::

::: resources
- Blog: [An overview of end-to-end exactly-once processing in Apache Flink](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/) · checkpoints and the two-phase commit sink, step by step
- Paper: [Lightweight asynchronous snapshots for distributed dataflows](https://arxiv.org/abs/1506.08603) · Carbone et al.; the checkpointing algorithm behind Flink
- Docs: [Exactly-once in Dataflow](https://docs.cloud.google.com/dataflow/docs/concepts/exactly-once) · how Dataflow deduplicates and commits, and where the guarantee ends
- Docs: [Pub/Sub exactly-once delivery](https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery) · what the subscription setting guarantees and its limits
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · chapter 5 on exactly-once and side effects
- Docs: [BigQuery Storage Write API](https://docs.cloud.google.com/bigquery/docs/write-api) · stream types and exactly-once writes with offsets
:::

### Exactly-Once Semantics Internals

::: plain
Kafka's exactly-once is two tools. The first is a numbered envelope: every message a producer sends carries its sender ID and a sequence number, so the broker can throw away a copy it has already filed. The second is a sealed bundle: a group of messages to several topics, plus "I have finished reading up to here", is either all delivered together or not at all.
:::

**Idempotent producer.** Without it, a producer that sends a batch, loses the broker's acknowledgement and retries can write the batch twice. With idempotence, the broker assigns each producer a **producer ID (PID)** and an **epoch**, and the producer numbers its batches per partition with a **sequence number**. The broker remembers the last sequence numbers per PID and partition and rejects duplicates (and detects gaps). Since Kafka 3.0, `enable.idempotence=true` and `acks=all` are the producer defaults, so most modern producers are already idempotent within a session.

**Transactions** extend this across partitions and topics:

```python
from confluent_kafka import Producer, Consumer

producer = Producer({"bootstrap.servers": BOOTSTRAP, "transactional.id": "payments-enricher-1"})
consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "payments-enricher",
                     "isolation.level": "read_committed", "enable.auto.commit": False})
consumer.subscribe(["payments"])
producer.init_transactions()            # registers the transactional.id, fences older instances

while True:
    msgs = consumer.consume(num_messages=500, timeout=1.0)
    if not msgs:
        continue
    producer.begin_transaction()
    for m in msgs:
        producer.produce("payments-enriched", key=m.key(), value=enrich(m.value()))
    producer.send_offsets_to_transaction(consumer.position(consumer.assignment()),
                                         consumer.consumer_group_metadata())
    producer.commit_transaction()       # outputs and input offsets commit atomically
```

Under the hood:

1. A **transaction coordinator** (a broker) owns each `transactional.id` and logs transaction state in the internal `__transaction_state` topic.
2. Writes go to their partitions immediately, as normal records belonging to an open transaction.
3. On commit, the coordinator runs a two-phase commit: it records "prepare commit", then writes **commit markers** (control records) into every partition touched, including `__consumer_offsets` for the consumed offsets, then records "complete".
4. Consumers with `isolation.level=read_committed` read only up to the **last stable offset** (LSO), the first offset of any still-open transaction, and skip records of aborted transactions.

**Fencing.** If an instance hangs and a replacement starts with the same `transactional.id`, `init_transactions()` bumps the epoch; the old instance's later writes are rejected as coming from a **zombie**. This is why the `transactional.id` must be stable for a logical producer across restarts.

**Kafka Streams** wraps all of this in one setting, `processing.guarantee=exactly_once_v2`: each task's outputs, state store changelog writes and input offsets commit in one transaction.

> **Pitfall:** Transactions guarantee exactly-once only for read-process-write cycles *within Kafka*. A consumer that writes to a database, or a consumer left at the default `read_uncommitted`, does not get the guarantee.

> **Pitfall:** Long-open transactions hold back the LSO, so `read_committed` consumers stall. Keep transactions short (`transaction.timeout.ms` bounds them, 60 seconds by default).

::: guide
For the producer state machine, coordinator internals and transaction failure modes, read [Transactions and exactly-once](pyspark-streaming-kafka-topic-map.html#ktxn), [Producer internals](pyspark-streaming-kafka-topic-map.html#kprod) and [Checkpointing, fault tolerance and exactly-once](pyspark-streaming-kafka-topic-map.html#ft) in the PySpark + Kafka guide.
:::

::: check
?? How does the broker detect a duplicate batch from an idempotent producer?
It tracks the producer ID, epoch and last sequence numbers per partition; a batch with a sequence number it has already written is rejected as a duplicate.

?? What does a `read_committed` consumer do differently?
It reads only up to the last stable offset and filters out records from aborted transactions, so it never sees uncommitted or aborted data.

?? What problem does producer fencing solve?
A zombie instance (hung but still alive) with the same `transactional.id` as its replacement cannot write, because its epoch is older and the broker rejects it.
:::

::: resources
- Docs: [Kafka design: message delivery semantics](https://kafka.apache.org/documentation/#semantics) · the official explanation of guarantees, idempotence and transactions
- Blog: [Exactly-once semantics are possible: here's how Kafka does it](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/) · the motivation and design, from the team that built it
- Blog: [Transactions in Apache Kafka](https://www.confluent.io/blog/transactions-apache-kafka/) · coordinator, markers, LSO and fencing explained in detail
- Spec: [KIP-98: exactly once delivery and transactional messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) · the design document
- Spec: [KIP-447: producer scalability for exactly once semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) · the change behind `exactly_once_v2`
:::

### Backpressure & Flow Control

::: plain
On a factory assembly line, if the painting station slows down, the parts pile up in front of it. Without a rule, the pile grows until it falls over. **Backpressure** is the rule: when a station's tray is full, it signals the station before it to slow down, which signals the one before that, all the way back to the loading dock. The line runs at the speed of its slowest station, but nothing overflows.
:::

In a streaming pipeline, every stage runs at its own speed. If a source produces 100,000 events per second and a downstream operator can process only 60,000, the difference must go somewhere. There are only four options: **buffer** it (until memory runs out), **drop** it (lose data), **scale** the slow stage (more workers), or **slow down** the upstream stages. Backpressure is the last option, done automatically, and it protects the pipeline while scaling catches up.

**Push versus pull.** In **pull-based** systems the consumer asks for data when it is ready, so backpressure is natural. Kafka consumers are pull-based: if processing is slow, the consumer simply polls less often and its **lag** (the distance between the latest offset and its committed offset) grows in Kafka, which stores it safely on disk. In **push-based** systems (one operator sending to the next over the network) the receiver must tell the sender how much it can take.

**Reactive Streams** is a small specification (for the JVM, adopted in Java 9 as `java.util.concurrent.Flow`) that standardises this: a `Subscriber` calls `request(n)` on its `Subscription` to signal demand for *n* more elements, and a `Publisher` must never send more than requested. Libraries such as Project Reactor, RxJava and Apache Pekko Streams (the open-source fork of Akka Streams) implement it, so stages from different libraries can be connected safely.

**Flink: credit-based flow control.** Flink operators on different TaskManagers exchange data in **network buffers** (32 KB memory segments by default). Each receiving channel announces **credits** (how many free buffers it has) to the sender; the sender may only send as many buffers as it holds credits for. When the receiver is slow, its buffers fill, credits stop, the sender's output buffers fill, and the sender's own operator blocks, which in turn stops it consuming its input. Pressure propagates back to the source, which then reads more slowly from Kafka.

This simulation shows the sender held to the receiver's pace:

```python
# Credit-based flow control: the sender may only send as many buffers as the
# receiver has granted credits for, so a slow receiver slows the sender down.
from collections import deque

credits, inbox, sent, processed = 4, deque(), 0, 0
for tick in range(1, 9):
    while credits > 0 and sent < 20:          # sender: one buffer per credit
        inbox.append(sent); sent += 1; credits -= 1
    if tick % 2 == 0 and inbox:               # receiver: handles one buffer every 2 ticks
        inbox.popleft(); processed += 1; credits += 1   # frees a buffer, returns a credit
    print(f"tick {tick}: sent={sent} queued={len(inbox)} processed={processed}")
# tick 1: sent=4 queued=4 processed=0
# tick 2: sent=4 queued=3 processed=1
# tick 3: sent=5 queued=4 processed=1
# tick 4: sent=5 queued=3 processed=2
# tick 5: sent=6 queued=4 processed=2
# tick 6: sent=6 queued=3 processed=3
# tick 7: sent=7 queued=4 processed=3
# tick 8: sent=7 queued=3 processed=4
```

The queue never exceeds the four granted buffers, and after the first burst the sender sends exactly as fast as the receiver processes. Because flow control is per channel, one slow channel does not block the other channels sharing the same TCP connection, which was a problem before credit-based flow control was introduced in Flink 1.5.

**Finding the bottleneck in Flink.** The web UI shows, per subtask, the share of time each task is **busy**, **backpressured** (waiting for output buffers) and **idle**. The rule: the bottleneck is the *first* task (going downstream from the source) that is busy, not backpressured. Tasks before it are backpressured because of it; tasks after it are idle because they are waiting for it.

[[fig:streaming_backpressure]]

**Backpressure and checkpoints.** Checkpoint barriers travel with the data, so under heavy backpressure they queue behind full buffers and checkpoints take longer or time out. Flink offers **unaligned checkpoints** (barriers overtake buffered data, which is saved as part of the checkpoint) and **buffer debloating** (automatically shrinking in-flight buffers to a target time), both introduced to keep checkpoints fast under backpressure.

**Elsewhere:**

| System | Mechanism | Knobs |
|---|---|---|
| Kafka consumer | Pull; lag accumulates in Kafka | `max.poll.records`, `fetch.max.bytes`, `pause()`/`resume()` per partition |
| Spark Structured Streaming | Micro-batches sized at trigger time | `maxOffsetsPerTrigger` (Kafka), `maxFilesPerTrigger` |
| Pub/Sub client libraries | Flow control on outstanding messages | Max outstanding messages and bytes per subscriber |
| Dataflow | Streaming autoscaling on backlog and CPU | Min and max workers; backlog metrics |

> **Pitfall:** A slow consumer can be evicted from its group. If processing one batch of polled records takes longer than `max.poll.interval.ms` (5 minutes by default), the consumer is considered dead and its partitions are reassigned, which causes rebalances and more lag. Reduce `max.poll.records` or speed up processing.

> **Tip:** Alert on **consumer lag growth**, not lag size. A steady lag of a million records is fine for a big topic; a lag that keeps rising means the consumer cannot keep up and backpressure is only buying time.

::: deeper
**Backpressure is a symptom.** It tells you a stage is too slow; it does not fix the cause. Usual causes: a skewed key sending most records to one subtask, an external call (database, HTTP) per record, large state with slow RocksDB access, or too little parallelism. Batch external calls, use async I/O (Flink `AsyncDataStream`), rebalance keys, or scale the slow operator.

**Bounded buffers everywhere.** Unbounded in-memory queues turn backpressure into an out-of-memory crash. Every queue in a well-designed pipeline is bounded, and blocking on a full queue is the signal that propagates.

> **Interview:** "In the Flink UI, the source and the map are backpressured, the window operator is 100% busy and the sink is idle. Where is the problem?" The window operator: it is the first busy task downstream of the backpressured ones. Look for skew, expensive state access or insufficient parallelism there.
:::

::: check
?? Why is a Kafka consumer naturally protected from overload?
It pulls data when ready; if it falls behind, unread data waits in Kafka on disk and shows up as growing lag rather than overflowing the consumer's memory.

?? In credit-based flow control, what stops a fast sender from flooding a slow receiver?
The sender may only send as many buffers as the receiver has granted credits for, and credits come back only when the receiver frees buffers.

?? How do you find the bottleneck operator in Flink's UI?
Go downstream from the source: the first task that is busy (not backpressured) is the bottleneck; tasks upstream of it show backpressure and tasks downstream are idle.
:::

::: resources
- Docs: [Flink: monitoring back pressure](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/) · busy, backpressured and idle metrics in the web UI
- Blog: [How to identify the source of backpressure](https://flink.apache.org/2021/07/07/how-to-identify-the-source-of-backpressure/) · a practical method with metrics and flame graphs
- Blog: [A deep dive into Flink's network stack](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/) · network buffers and credit-based flow control explained
- Spec: [Reactive Streams](https://www.reactive-streams.org/) · the specification for asynchronous streams with non-blocking backpressure
- Docs: [Apache Pekko Streams basics](https://pekko.apache.org/docs/pekko/current/stream/stream-flows-and-basics.html) · backpressure in a Reactive Streams library
:::

### Lambda & Kappa Architectures

::: plain
Imagine a newspaper that prints a careful, fact-checked edition every night and also runs a live ticker during the day. The ticker is fast but sometimes wrong; the nightly edition corrects it. That is the **Lambda architecture**. The **Kappa architecture** says: build one newsroom good enough to be both fast and right, and when you need to fix old stories, rerun the same newsroom over the archive.
:::

**Lambda** was described by Nathan Marz around 2011, when stream processors were fast but approximate (Apache Storm offered at-least-once processing, and state was hard). It has three layers:

1. A **batch layer** stores the immutable master dataset of all raw events and periodically recomputes complete, accurate **batch views** (for example with Hadoop or Spark).
2. A **speed layer** processes recent events in real time to produce incremental, possibly approximate **real-time views** covering the gap since the last batch run.
3. A **serving layer** answers queries by merging batch views with real-time views.

The batch layer is the source of truth: any error in the speed layer disappears after the next batch run.

**The problem with Lambda** is that you implement the same business logic twice, in two different frameworks (say Spark SQL for batch and a streaming API for the speed layer), and keep them producing identical results. Every change is made twice, tested twice and debugged twice, and the two always drift.

**Kappa**, proposed by Jay Kreps in 2014 ("Questioning the Lambda Architecture"), removes the batch layer:

1. Keep the raw events in a **replayable log** (Kafka) with long enough retention, or forever.
2. Run **one** stream processing job that produces the serving views.
3. To change logic or fix a bug, start a **second instance** of the new job reading from the beginning of the log into a new output table; when it has caught up, switch readers to the new table and stop the old job.

[[fig:streaming_lambda_kappa]]

| | Lambda | Kappa |
|---|---|---|
| Code paths | Two (batch and stream) | One (stream) |
| Reprocessing | Batch recomputation | Replay the log through a new job version |
| Correctness model | Batch corrects the speed layer | Stream engine must be exactly-once and handle late data |
| Storage | Data lake plus stream | Long-retention log (or log plus lake) |
| Main weakness | Duplicate logic that drifts | Replaying months of data through a streaming job can be slow and costly |

**Why Kappa became practical.** Two things changed. Stream processors became correct: Flink, Dataflow and Spark offer exactly-once state, event-time windows and watermarks, which removed the reason for an accurate batch layer. And long retention became cheap: Kafka **tiered storage** keeps old segments in object storage such as Cloud Storage, so a log can hold years of data affordably.

**What teams actually do today** is usually a pragmatic mix:

- **Unified engines**: write logic once in a framework that runs both batch and streaming. Apache Beam (on Dataflow) uses the same pipeline code for bounded and unbounded data; Spark's DataFrame API is the same for `read` and `readStream`; Flink runs SQL in batch or streaming mode.
- **Streaming into the lakehouse**: stream events into Iceberg or Delta tables (or BigQuery through the Storage Write API), and run batch and incremental jobs over the same tables. Reprocessing reads the table, not the log, which is often faster than a log replay.
- **Lambda where it is still justified**: for example a real-time fraud score computed on the stream and a nightly batch that recomputes exact financial aggregates for reporting.

On Google Cloud, a common Kappa-style design is Pub/Sub or Google Managed Service for Apache Kafka as the log, one Dataflow pipeline as the processor, BigQuery as the serving store, and the raw events also archived to Cloud Storage (or a BigQuery raw table) so replays can come from there.

> **Pitfall:** Kappa's reprocessing step is easy to underestimate. Replaying 90 days through a streaming job takes time and capacity, and outputs must be written to a new table, not merged into the live one, or readers see a mix of old and new logic.

> **Tip:** Whatever the architecture, keep the raw, immutable events. They are what makes any recomputation possible.

::: deeper
**Event sourcing** in application design is the same idea as Kappa: the log of events is the source of truth and every view is derived by replaying it. The Event-driven architecture section covers it.

**Replay strategy details.** When replaying, the new job must process event time correctly (watermarks advance quickly during catch-up), sinks must be idempotent, and the job may need much more parallelism than in steady state. Many teams bootstrap state from a batch job over the lake and then switch to the stream from a known offset, a hybrid sometimes called "batch backfill, stream forward".

> **Interview:** "Why did Kappa replace Lambda in many companies?" Lambda required the same logic in two systems that drifted apart. Exactly-once stream processors and cheap long log retention made a single streaming code path both correct and able to reprocess history.
:::

::: check
?? What are the three layers of the Lambda architecture?
A batch layer that recomputes accurate views from all raw data, a speed layer that covers recent data in real time, and a serving layer that merges both for queries.

?? How does a Kappa architecture fix a bug in its processing logic?
It starts the corrected job as a new instance reading the log from the beginning into a new output, waits for it to catch up, switches readers over and stops the old job.

?? What changes made Kappa practical?
Stream processors with exactly-once state and event-time handling, and cheap long retention of the log (for example Kafka tiered storage on object storage).
:::

::: resources
- Article: [Questioning the Lambda Architecture](https://www.oreilly.com/radar/questioning-the-lambda-architecture/) · Jay Kreps's 2014 essay that proposed the Kappa approach
- Blog: [How to beat the CAP theorem](http://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html) · Nathan Marz's original description of the Lambda idea
- Book: [Big Data: principles and best practices of scalable realtime data systems](https://www.manning.com/books/big-data) · Marz and Warren; the full Lambda architecture
- Book: *Designing Data-Intensive Applications* by Martin Kleppmann · chapters 11 and 12 on stream processing, derived data and unifying batch and streaming
- Docs: [Beam programming guide](https://beam.apache.org/documentation/programming-guide/) · one pipeline model for batch and streaming, run by Dataflow
:::

## Apache Kafka

Kafka is the backbone of most streaming platforms, including on Google Cloud through Google Managed Service for Apache Kafka, so it earns a group of its own. The topics go from the inside out: how brokers store and replicate partitions, how producers and consumers talk to them, how the cluster manages its own metadata without ZooKeeper, how old data moves to object storage, how clusters replicate to each other, how to process streams inside the Kafka ecosystem, and how to keep message formats under control as they change.

If you use Pub/Sub instead, most ideas still apply: a durable log of messages, acknowledgements, ordering keys, subscriptions that read independently. What differs is that Pub/Sub hides partitions and brokers entirely.

### Apache Kafka — Architecture & Internals

::: plain
Kafka is a set of append-only notebooks kept by a team of librarians. Each subject (topic) is split into several notebooks (partitions) so many people can write at once. Every page is numbered (the offset), nothing is ever rewritten, and each notebook has photocopies (replicas) kept by other librarians. Readers keep a bookmark and read at their own speed; the notebook does not care how many readers there are.
:::

The core vocabulary:

- A **broker** is a Kafka server. A **cluster** is several brokers.
- A **topic** is a named stream of records, split into **partitions**. Each partition is an ordered, append-only **log**; each record gets a sequential **offset** within its partition. Ordering is guaranteed only within a partition.
- Each partition has **replicas** on different brokers (`replication.factor`, typically 3). One replica is the **leader**, which handles all writes (and by default reads); the others are **followers** that fetch from the leader.
- The **in-sync replicas (ISR)** are the replicas that are caught up with the leader. A write with `acks=all` is acknowledged only when every replica in the ISR has it, and `min.insync.replicas` (commonly 2 with replication factor 3) sets how many must be in the ISR for writes to be accepted at all.
- The **high watermark** is the offset up to which all ISR replicas have the data. Consumers only read below it, so they never see records that could be lost in a leader failure.

[[fig:streaming_kafka]]

**Leader election.** When a leader's broker fails, the **controller** (in KRaft mode, the active node of the controller quorum; see the KRaft topic) picks a new leader from the ISR, so no acknowledged write is lost. If no ISR member is alive, the partition stays offline unless **unclean leader election** is enabled (`unclean.leader.election.enable`, false by default), which trades possible data loss for availability.

**Storage.** Each partition replica is a directory of **log segments**. A segment is a `.log` file of record batches plus a `.index` (offset to file position) and `.timeindex` (timestamp to offset), both sparse. Only the newest (active) segment receives writes; a new segment starts when it reaches `segment.bytes` (1 GiB by default) or `segment.ms`. Old segments are deleted by **retention** (`retention.ms`, 7 days by default, or `retention.bytes`), or, for **compacted** topics (`cleanup.policy=compact`), cleaned so that only the latest record per key remains.

**Why Kafka is fast.** Appends are sequential disk writes, reads of recent data come from the operating system's page cache, records travel in compressed batches end to end (the broker does not decompress them), and consumers fetching from disk benefit from **zero-copy** transfer (`sendfile`), which sends file bytes to the network socket without copying them through the application. Zero-copy is not possible when TLS encryption is on, because the bytes must be encrypted in user space.

```bash
# Create a topic with 12 partitions and 3 replicas, and inspect it
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic orders \
  --partitions 12 --replication-factor 3 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic orders
# Topic: orders  Partition: 0  Leader: 2  Replicas: 2,0,1  Isr: 2,0,1  ...
```

On Google Cloud, **Google Managed Service for Apache Kafka** runs the brokers for you: you choose capacity (vCPUs and memory), it manages brokers, storage, upgrades and replication across zones, and clients connect with standard Kafka libraries using Google Cloud authentication.

> **Pitfall:** Choose the partition count with the future in mind. Increasing it later changes which partition a key maps to, which breaks per-key ordering for existing keys and upsert consumers that rely on key partitioning.

> **Pitfall:** `acks=all` with `min.insync.replicas=1` is not safe: if the ISR shrinks to the leader alone, writes are acknowledged with a single copy. Use `min.insync.replicas=2` with replication factor 3.

::: guide
For partitions, storage, replication and KRaft in depth, read [Kafka architecture fundamentals](pyspark-streaming-kafka-topic-map.html#kbasics), [Kafka storage internals](pyspark-streaming-kafka-topic-map.html#kstorage) and [Replication, durability and KRaft](pyspark-streaming-kafka-topic-map.html#krepl) in the PySpark + Kafka guide.
:::

::: check
?? What is the ISR, and why does leader election choose from it?
The in-sync replicas: those fully caught up with the leader. Electing a new leader from the ISR guarantees that every acknowledged (`acks=all`) write is present on the new leader.

?? Why do consumers read only below the high watermark?
Records above it are not yet on all in-sync replicas and could disappear if the leader failed, so exposing them could show data that later vanishes.

?? Why does TLS disable zero-copy transfer?
The bytes must be encrypted by the broker process before they are sent, so they cannot go straight from the page cache to the socket.
:::

::: resources
- Docs: [Kafka documentation: design](https://kafka.apache.org/documentation/#design) · persistence, efficiency, replication and log compaction from the source
- Book: [Kafka: The Definitive Guide, 2nd Edition](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/) · Shapira, Palino, Sivaram and Petty; the standard book
- Paper: [Kafka: a distributed messaging system for log processing](https://notes.stephenholiday.com/Kafka.pdf) · Kreps, Narkhede and Rao, NetDB 2011; the original design
- Course: [Confluent Developer: Kafka internals and architecture courses](https://developer.confluent.io/courses/) · free video courses on brokers, replication and storage
- Docs: [Google Managed Service for Apache Kafka](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/overview) · running Kafka on Google Cloud without managing brokers
:::

### Kafka Producers & Consumers — Deep Dive

::: plain
A producer is like a post room that collects letters for each destination in a tray and sends the tray when it is full or when a short timer rings, instead of running to the post box with every letter. Consumers are teams of readers who split the notebooks between them, each notebook read by exactly one member of a team, with each reader writing down how far they got.
:::

**Producers.** `producer.send()` does not send immediately. The record is serialized, assigned a partition, and added to a per-partition **batch** in an in-memory buffer. A background thread sends a batch when it reaches `batch.size` (16 KB by default) or after `linger.ms` (5 ms by default since Kafka 4.0, previously 0). Larger batches mean fewer requests and better **compression** (`compression.type`: `gzip`, `snappy`, `lz4` or `zstd`), at the cost of a few milliseconds of latency.

**Partitioning.** A record with a key goes to `hash(key) % partitions` (murmur2 hash), so all records with the same key are in order in one partition. A record without a key is spread using the **sticky** partitioner, which fills one batch for one partition before moving to the next, giving bigger batches than round-robin.

**Durability settings:**

| Setting | Default (Kafka 3.0+) | Meaning |
|---|---|---|
| `acks` | `all` | Wait for all in-sync replicas before acknowledging (`1` = leader only, `0` = no wait) |
| `enable.idempotence` | `true` | Sequence numbers prevent duplicates from retries |
| `retries` | Very large | Retry transient errors, bounded by `delivery.timeout.ms` (2 minutes) |
| `max.in.flight.requests.per.connection` | 5 | Up to 5 unacknowledged batches; idempotence keeps order |

```python
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20,
              "compression.type": "zstd", "acks": "all"})

def report(err, msg):
    if err:
        print("delivery failed:", err)       # log and alert; do not ignore

p.produce("orders", key="customer-42", value=b'{"order_id": 1}', on_delivery=report)
p.flush()                                     # block until all buffered records are delivered
```

**Consumers and groups.** Consumers with the same `group.id` form a **consumer group** and share a topic's partitions: each partition is read by exactly one member, so a group's parallelism is capped by the partition count. Different groups read independently. A broker acts as **group coordinator**, tracks members by heartbeats and runs **rebalances** when members join, leave or time out.

- **Eager** rebalancing (old default) revokes all partitions from everyone, then reassigns: the whole group stops.
- **Cooperative incremental** rebalancing (`CooperativeStickyAssignor`) moves only the partitions that must move.
- The **new consumer group protocol** (KIP-848, generally available in Kafka 4.0, `group.protocol=consumer`) moves assignment to the broker and makes rebalances incremental and much less disruptive.
- **Static membership** (`group.instance.id`) lets a restarting consumer rejoin without triggering a rebalance, useful for rolling deploys.

**Offsets.** Consumers store their position in the internal `__consumer_offsets` topic. By default `enable.auto.commit=true` commits every 5 seconds in the background, which can commit records not yet processed (at-most-once on a crash) or re-deliver records processed but not committed (duplicates). For control, disable auto-commit and commit after processing:

```python
from confluent_kafka import Consumer

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "order-loader",
              "enable.auto.commit": False, "auto.offset.reset": "earliest"})
c.subscribe(["orders"])
while True:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    upsert_order(msg.value())                 # idempotent write, keyed by order_id
    c.commit(message=msg, asynchronous=False) # at-least-once: commit after processing
```

`auto.offset.reset` decides where a group with no committed offset starts: `latest` (the default, only new records) or `earliest`.

> **Pitfall:** Ignoring delivery callbacks or never calling `flush()` before exit silently loses buffered records. Always handle delivery errors and flush on shutdown.

> **Pitfall:** More consumers than partitions leaves the extra consumers idle. Plan partition counts for peak consumer parallelism.

::: guide
For the producer's internals, the consumer protocol and rebalancing failure modes, read [Producer internals](pyspark-streaming-kafka-topic-map.html#kprod) and [Consumers, groups and rebalancing](pyspark-streaming-kafka-topic-map.html#kcons) in the PySpark + Kafka guide.
:::

::: check
?? Why does raising `linger.ms` from 5 to 20 often increase throughput?
The producer waits longer to fill batches, so it sends fewer, larger, better-compressed requests, at the cost of up to 20 ms extra latency.

?? A topic has 6 partitions and a consumer group has 8 members. What happens?
Six members each read one partition; two members are idle.

?? Why can auto-commit cause both lost and duplicated processing?
It commits on a timer, independently of processing: it may commit offsets of records not yet processed (lost on a crash) or not yet commit records already processed (re-delivered after a crash).
:::

::: resources
- Docs: [Kafka producer configs](https://kafka.apache.org/documentation/#producerconfigs) · every producer setting and its default
- Docs: [Kafka consumer configs](https://kafka.apache.org/documentation/#consumerconfigs) · every consumer setting and its default
- Spec: [KIP-848: the next generation of the consumer rebalance protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) · the broker-driven, incremental group protocol
- Book: [Kafka: The Definitive Guide, 2nd Edition](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/) · chapters 3 and 4 on producers and consumers
- Course: [Confluent Developer courses](https://developer.confluent.io/courses/) · free courses on producer and consumer behaviour with exercises
:::

### Kafka KRaft — ZooKeeper-Free Kafka

::: plain
For years Kafka kept its master list of who owns what (topics, partitions, leaders, settings) in a separate system, ZooKeeper, like a company storing its staff register at another company's office. **KRaft** moves the register in-house: a small group of Kafka nodes keeps it as its own Kafka-style log and agrees on every change by voting.
:::

Every Kafka cluster needs **metadata**: which brokers exist, which topics and partitions, which replica leads each partition, the ISR, configurations, ACLs. Until Kafka 2.8 this lived in **Apache ZooKeeper**, with one broker elected as **controller** to read and write it. That design had limits: a second distributed system to run and secure, slow controller failover (a new controller had to load all metadata from ZooKeeper), and practical limits around a few hundred thousand partitions.

**KRaft** (Kafka Raft, KIP-500) replaces ZooKeeper with a **metadata quorum** inside Kafka:

- A few nodes (3 or 5) run as **controllers** (`process.roles=controller`). They form a **Raft** quorum: one is the active controller (the Raft leader), the others are hot standbys.
- All metadata changes are records in an internal, replicated log, the `__cluster_metadata` topic. The active controller appends; the others replicate; a change is committed when a majority has it.
- **Brokers** (`process.roles=broker`) fetch the metadata log from the controllers and apply it to an in-memory image, much as consumers read a topic. Each broker knows exactly which metadata offset it has applied.
- Periodic **snapshots** of the metadata state let new or restarted nodes catch up without replaying the whole log.

```properties
# controller.properties (one of three dedicated controllers)
process.roles=controller
node.id=1
controller.quorum.bootstrap.servers=ctrl1:9093,ctrl2:9093,ctrl3:9093
listeners=CONTROLLER://:9093

# broker.properties
process.roles=broker
node.id=101
controller.quorum.bootstrap.servers=ctrl1:9093,ctrl2:9093,ctrl3:9093
```

```bash
kafka-storage.sh random-uuid                         # cluster ID
kafka-storage.sh format -t $CLUSTER_ID -c controller.properties
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status
```

**Benefits:** one system to run and secure; controller failover in a fraction of the time, because standbys already hold the metadata in memory; and far more partitions per cluster, since metadata propagates as a log instead of through ZooKeeper watches.

**Timeline.** KRaft became production-ready in Kafka 3.3. ZooKeeper mode was deprecated in 3.5, migration from ZooKeeper to KRaft was supported in the 3.x line (3.9 is the final "bridge" release), and **Kafka 4.0 removed ZooKeeper entirely**. To upgrade an old ZooKeeper cluster to 4.x you must first migrate it to KRaft on a 3.x release, then upgrade.

**Deployment choices:** in **combined mode** a node is both broker and controller (fine for development and small clusters); in production, run **dedicated controllers** so broker load cannot disturb the quorum. Use 3 controllers to tolerate one failure, 5 to tolerate two.

> **Pitfall:** A Raft quorum needs a majority. With 3 controllers, losing 2 stops all metadata changes (no leader elections, no topic creation), even though brokers keep serving existing partitions. Spread controllers across zones.

> **Tip:** Managed services such as Google Managed Service for Apache Kafka run the metadata layer for you; KRaft details matter when you run Kafka yourself on GKE or VMs.

::: guide
For the quorum protocol, controller failover and migration details, read [Replication, durability and KRaft](pyspark-streaming-kafka-topic-map.html#krepl) and [Kafka operations, security and sizing](pyspark-streaming-kafka-topic-map.html#kops) in the PySpark + Kafka guide.
:::

::: check
?? Where does a KRaft cluster store its metadata, and how do brokers learn about changes?
In the replicated `__cluster_metadata` log managed by the Raft controller quorum. Brokers fetch that log from the controllers and apply it to their in-memory metadata image.

?? Why is controller failover faster in KRaft than with ZooKeeper?
Standby controllers already replicate the metadata log and hold the state in memory, so a new active controller does not have to load everything from ZooKeeper.

?? How do you upgrade a ZooKeeper-based Kafka 3.6 cluster to Kafka 4.x?
Migrate it to KRaft on a 3.x release that supports migration (ideally 3.9), finish the migration, then upgrade to 4.x.
:::

::: resources
- Docs: [Kafka KRaft documentation](https://kafka.apache.org/documentation/#kraft) · configuration, deployment and operations of KRaft clusters
- Spec: [KIP-500: replace ZooKeeper with a self-managed metadata quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum) · the design and motivation
- Blog: [Why replace ZooKeeper with Kafka Raft: the log of all logs](https://www.confluent.io/blog/why-replace-zookeeper-with-kafka-raft-the-log-of-all-logs/) · the reasoning, explained for practitioners
- Docs: [The Raft consensus algorithm](https://raft.github.io/) · the paper, visualisations and talks behind the quorum
- Book: [Kafka: The Definitive Guide, 2nd Edition](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/) · cluster internals and the controller's role
:::

### Kafka Tiered Storage

::: plain
A shop keeps the most popular items on the shelves and the rest in a cheap warehouse across town. Customers can still order anything; old items just take a little longer to arrive. **Tiered storage** does this for Kafka: recent data stays on the brokers' fast local disks, older data moves to cheap object storage, and consumers can still read all of it.
:::

Without tiered storage, every byte a topic retains sits on broker disks, three times over with replication. Keeping 30 days of a 50 MB/s topic means about 130 TB of raw data, roughly 390 TB of disk with replication. That makes long retention expensive and makes brokers slow to replace, because a new broker must copy all its partitions' data from others.

**Tiered storage** (KIP-405; early access in Kafka 3.6, production-ready in 3.9) splits a partition's log into two tiers:

- **Local tier**: the active segment and recent closed segments on broker disks, bounded by `local.retention.ms` / `local.retention.bytes`.
- **Remote tier**: closed segments copied to remote storage (object storage such as Cloud Storage), kept for the topic's overall `retention.ms` / `retention.bytes`.

The flow: when a segment rolls (closes), the partition leader's **RemoteLogManager** copies it, with its indexes, to remote storage through a pluggable **RemoteStorageManager**, and records the upload in remote log metadata (by default kept in an internal topic, `__remote_log_metadata`). Once a segment is safely remote and older than local retention, it is deleted locally. A consumer asking for an old offset is served from the remote tier transparently; recent reads still come from local disk and the page cache.

```properties
# Broker
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=<plugin class for your object store>
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
```

```bash
# Topic: keep 1 day locally, 365 days in total
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic clicks \
  --partitions 24 --replication-factor 3 \
  --config remote.storage.enable=true \
  --config local.retention.ms=86400000 \
  --config retention.ms=31536000000
```

Apache Kafka defines the plugin interfaces but does not ship an object storage implementation. Open-source plugins such as Aiven's tiered storage plugin support Cloud Storage among other backends, and vendor distributions include their own.

**Benefits:** cheap long retention (which makes Kappa-style replays practical), faster broker replacement and rebalancing (only the local tier moves), and smaller, cheaper broker disks. **Costs:** reads of old data have higher latency and depend on object storage throughput, uploads use broker network and CPU, and object storage requests cost money.

> **Pitfall:** Tiered storage does not support **compacted** topics; it applies to topics with the delete cleanup policy. Changelog and state topics that rely on compaction stay fully local.

> **Pitfall:** A consumer replaying months of data reads from object storage, which can saturate broker network bandwidth and hurt producers. Throttle replays and size broker network for them.

> **Tip:** On Google Cloud, Google Managed Service for Apache Kafka manages broker storage for you; check its documentation for how retention and storage are billed before designing very long retention.

::: guide
For segments, indexes and retention mechanics that tiered storage builds on, read [Kafka storage internals](pyspark-streaming-kafka-topic-map.html#kstorage) and [Kafka operations, security and sizing](pyspark-streaming-kafka-topic-map.html#kops) in the PySpark + Kafka guide.
:::

::: check
?? With tiered storage enabled, which segments can be in remote storage?
Only closed (rolled) segments. The active segment is always local, and recent closed segments stay local until they exceed local retention.

?? What does `local.retention.ms=86400000` with `retention.ms` of one year mean?
Segments stay on broker disks for about one day after they are uploaded to remote storage, and the data is kept for one year in total, served from remote storage after the first day.

?? Why does tiered storage make adding a broker faster?
Rebalancing moves only the local tier of each partition, not the full history, which stays in object storage.
:::

::: resources
- Docs: [Kafka tiered storage](https://kafka.apache.org/documentation/#tieredstorage) · configuration, limitations and operations
- Spec: [KIP-405: Kafka tiered storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage) · the design, interfaces and metadata model
- Repo: [Tiered storage for Apache Kafka (Aiven)](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka) · an open-source RemoteStorageManager supporting Cloud Storage
- Blog: [Infinite storage in Confluent Platform](https://www.confluent.io/blog/infinite-kafka-storage-in-confluent-platform/) · the use cases and economics of tiered Kafka storage
- Docs: [Google Managed Service for Apache Kafka](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/overview) · how the managed service handles storage and retention
:::

### MirrorMaker 2 — Cross-Cluster Replication

::: plain
A company with offices in two cities keeps a copy of important files in each office, so if one office burns down, the other can carry on. **MirrorMaker 2** is the courier that keeps Kafka copies in sync between clusters: it copies the messages, the topic settings and even each reader's bookmark, translated so that a reader can move to the other office and continue where it left off.
:::

Kafka replication (ISR) works inside one cluster, across zones of one region. Replicating **between clusters** (between regions, between on-premises and cloud, or from many edge clusters into a central one) needs a separate tool. **MirrorMaker 2** (MM2, KIP-382, Kafka 2.4+) is Apache Kafka's built-in solution, implemented as a set of **Kafka Connect** connectors:

| Connector | Job |
|---|---|
| **MirrorSourceConnector** | Consumes topics from the source cluster and produces them to the target; also syncs topic configurations, partition counts and ACLs |
| **MirrorCheckpointConnector** | Translates consumer group offsets from source to target and emits them as checkpoints (optionally applies them to target groups) |
| **MirrorHeartbeatConnector** | Emits heartbeats so you can verify connectivity and measure replication latency |

**Topic naming.** With the default `DefaultReplicationPolicy`, a topic `orders` from cluster `eu` appears on the target as `eu.orders`. The prefix records where data came from and prevents loops in **active-active** setups (MM2 never replicates a topic back to the cluster it came from). The `IdentityReplicationPolicy` keeps names unchanged, which is convenient for migrations and **active-passive** disaster recovery, but is unsafe for active-active because nothing stops loops.

**Offset translation.** Offsets differ between clusters: offset 5,000 in `eu`'s `orders` partition 0 is not offset 5,000 in `us`'s `eu.orders` partition 0 (retention, compaction or a late start shift them). MM2's source connector periodically writes **offset syncs** (pairs of upstream and downstream offsets); the checkpoint connector uses them to translate each consumer group's committed offset into a target offset, written to a `<source>.checkpoints.internal` topic. With `sync.group.offsets.enabled=true`, MM2 writes translated offsets directly to the target cluster's consumer groups (when they are not active there), so consumers can fail over without tools.

```properties
# mm2.properties: active-passive replication from eu to us
clusters = eu, us
eu.bootstrap.servers = kafka-eu:9092
us.bootstrap.servers = kafka-us:9092

eu->us.enabled = true
eu->us.topics = orders, payments, clicks.*
eu->us.groups = .*
replication.factor = 3

sync.group.offsets.enabled = true
emit.checkpoints.interval.seconds = 30
sync.group.offsets.interval.seconds = 30
```

```bash
connect-mirror-maker.sh mm2.properties     # dedicated MM2 mode; or deploy the connectors on a Connect cluster
```

**Patterns:**

- **Active-passive (disaster recovery).** Producers and consumers use the primary; MM2 replicates to the standby. On failover, consumers start on the standby from translated offsets. Translation is approximate, so expect a few duplicates: consumers must be idempotent.
- **Active-active.** Each region produces locally and replicates to the other; consumers read both the local topic and the replicated `remote.topic` (for example with a regex subscription). Each event is written in one place, so there are no write conflicts, but global ordering across regions does not exist.
- **Aggregation.** Many edge or regional clusters replicate into a central analytics cluster.
- **Migration.** Replicate from an old cluster to a new one (for example to Google Managed Service for Apache Kafka), move consumers, then producers.

**Monitoring replication.** MM2 exposes metrics such as `replication-latency-ms` (time from the record's timestamp on the source to its write on the target) and `record-age-ms`, plus the consumer lag of its own source consumers. Alert when latency or lag grows: in a disaster, that gap is the data you would lose.

On Google Cloud, Google Managed Service for Apache Kafka offers managed Kafka Connect clusters that can run the MirrorMaker 2 connectors, for example to migrate from self-managed Kafka or to replicate between regions.

> **Pitfall:** Cross-cluster replication is asynchronous. In active-passive DR, data produced in the last seconds before the primary fails may never reach the standby. Your recovery point objective (RPO) is the replication lag, not zero.

> **Pitfall:** Consumers that fail over to a renamed topic (`eu.orders`) must subscribe to the new name. Plan names and subscriptions before the emergency, and rehearse failover.

::: deeper
**Exactly-once replication.** Since Kafka 3.5, the MirrorSourceConnector can run with Kafka Connect's exactly-once source support (KIP-618 provided the Connect side), which writes records and source offsets transactionally on the target. Offset translation for consumer groups remains approximate.

**Alternatives.** Confluent's **Cluster Linking** replicates at the broker level and preserves offsets byte for byte (no translation needed), but it is a Confluent feature. Uber's uReplicator was an earlier open-source alternative. For simple one-way migrations, some managed services provide their own replication tools.

**Ordering and partitions.** MM2 keeps the source partition of each record, so per-key ordering is preserved within a replicated topic, provided partition counts match (MM2 syncs them).

> **Interview:** "Why does MM2 rename topics by default?" To record the origin cluster and to prevent replication loops in active-active topologies: MM2 can see from the prefix that a topic came from the other cluster and will not send it back.
:::

::: check
?? Why does MM2 need offset translation?
Offsets of the same record differ between source and target clusters, so a consumer group's committed source offset must be mapped to the equivalent target offset before it can resume there.

?? When is `IdentityReplicationPolicy` appropriate, and when is it dangerous?
It is appropriate for one-way migrations and active-passive DR where names should stay the same. It is dangerous in active-active setups, because without prefixes MM2 cannot prevent topics from being replicated back and forth in a loop.

?? What is the recovery point objective of an active-passive MM2 setup?
Roughly the replication lag at the moment of failure: records produced but not yet replicated are lost.
:::

::: resources
- Docs: [Kafka geo-replication (MirrorMaker 2)](https://kafka.apache.org/documentation/#georeplication) · configuration, topologies and monitoring from the Kafka docs
- Spec: [KIP-382: MirrorMaker 2.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0) · the design: connectors, naming and offset translation
- Docs: [Create a MirrorMaker 2 connector on Google Cloud](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/connect-cluster/create-mirrormaker-connector) · running MM2 on managed Kafka Connect
- Article: [Kafka MirrorMaker explained](https://www.confluent.io/learn/kafka-mirrormaker/) · an overview of MM2 use cases and limits
- Book: [Kafka: The Definitive Guide, 2nd Edition](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/) · the multi-cluster architectures chapter
:::

### Kafka Streams & ksqlDB

::: plain
Kafka Streams is a toolkit that turns any ordinary Java application into a stream processor: no separate cluster, just a library that reads topics, keeps its working notes in a local database backed up to Kafka, and writes results back to topics. ksqlDB puts a SQL front desk on the same toolkit, so you can write `CREATE TABLE ... AS SELECT` instead of Java.
:::

**Kafka Streams** is a Java library (part of Apache Kafka). You scale it by running more instances of your app; they form a consumer group and split the input partitions into **tasks**. Its two core abstractions reflect the **stream-table duality**:

- A **KStream** is a stream of independent events ("Asha paid 10", "Asha paid 5").
- A **KTable** is a changelog seen as a table: the latest value per key ("Asha's balance is 15"). A stream of updates builds a table; a table's changes form a stream.
- A **GlobalKTable** is a table fully replicated to every instance, for joins against small reference data.

```java
StreamsBuilder b = new StreamsBuilder();
KStream<String, Payment> payments = b.stream("payments");
KTable<String, Customer> customers = b.table("customers");

payments
    .join(customers, (p, c) -> new Enriched(p, c.segment()))       // stream-table join by key
    .groupBy((k, e) -> e.segment())                                 // re-key: repartition topic
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
    .aggregate(() -> 0.0, (seg, e, total) -> total + e.amount())   // state in RocksDB
    .toStream()
    .to("revenue-by-segment-5m");
```

**State.** Aggregations, joins and windows keep **state stores** (RocksDB on local disk by default). Each store is backed by a compacted **changelog topic** in Kafka; if an instance dies, another rebuilds the store from the changelog (optionally from **standby replicas** kept warm on other instances). When you re-key data (`groupBy` on a new key), Streams writes an internal **repartition topic** so records with the new key meet in one task. **Interactive queries** let your app serve its local state directly, for example over a REST endpoint.

**Windows:** tumbling, hopping, sliding and session windows, with a **grace period** for late records (the same idea as allowed lateness). **Exactly-once** is one setting, `processing.guarantee=exactly_once_v2`, which uses Kafka transactions to commit outputs, changelog writes and offsets together.

**ksqlDB** is a Confluent project (under the Confluent Community License, not Apache) that runs Kafka Streams topologies defined in SQL on a ksqlDB server cluster:

```sql
CREATE STREAM payments (customer_id VARCHAR KEY, amount DOUBLE)
  WITH (KAFKA_TOPIC = 'payments', VALUE_FORMAT = 'AVRO');

CREATE TABLE spend_per_customer AS              -- a materialized view, kept up to date
  SELECT customer_id, SUM(amount) AS total, COUNT(*) AS payments
  FROM payments
  GROUP BY customer_id
  EMIT CHANGES;

SELECT * FROM spend_per_customer WHERE customer_id = 'c-42';   -- pull query: current value
```

A **persistent query** (`CREATE TABLE ... AS SELECT`) runs continuously and maintains a **materialized view** in state stores; **pull queries** read its current value by key; **push queries** (`EMIT CHANGES`) stream every change to the client.

Confluent now focuses its managed stream processing on Apache Flink (Flink SQL in Confluent Cloud), and ksqlDB sees less development. For new SQL-based streaming work, also consider the streaming SQL engines at the end of this section.

> **Pitfall:** Every re-keying creates a repartition topic and a network hop through Kafka. Chains of `groupBy` and joins on different keys can multiply topics, storage and latency. Design keys so that data is co-partitioned (same key, same partition count) where you join.

> **Pitfall:** Large state means long restores after failures or rebalances. Use standby replicas and static membership to avoid rebuilding stores from changelogs during routine restarts.

::: guide
For Kafka Streams, Connect and the wider ecosystem, read [Kafka Streams, Connect and the ecosystem](pyspark-streaming-kafka-topic-map.html#kstreams) and [Stateful processing](pyspark-streaming-kafka-topic-map.html#stateful) in the PySpark + Kafka guide.
:::

::: check
?? What is the difference between a KStream and a KTable?
A KStream is a sequence of independent events; a KTable is the latest value per key, built from a changelog stream of updates.

?? How does Kafka Streams recover a state store when an instance dies?
Another instance takes over the task and rebuilds the store by replaying its compacted changelog topic from Kafka (or promotes a warm standby replica).

?? What is the difference between a pull query and a push query in ksqlDB?
A pull query returns the current value of a materialized view and ends; a push query keeps running and streams every change to the client.
:::

::: resources
- Docs: [Kafka Streams documentation](https://kafka.apache.org/documentation/streams/) · the official guide: concepts, DSL, state and exactly-once
- Docs: [Confluent Kafka Streams guide](https://docs.confluent.io/platform/current/streams/index.html) · architecture, tutorials and operations
- Book: [Mastering Kafka Streams and ksqlDB](https://www.oreilly.com/library/view/mastering-kafka-streams/9781492062486/) · Mitch Seymour; stateful processing, windows and ksqlDB in depth
- Docs: [ksqlDB documentation](https://docs.ksqldb.io/) · streams, tables, persistent, pull and push queries
- Blog: [Streams and tables in Apache Kafka](https://www.confluent.io/blog/kafka-streams-tables-part-1-event-streaming/) · the stream-table duality explained step by step
:::

### Schema Registry & Schema Evolution

::: plain
Producers and consumers of a topic are like two companies exchanging forms. If one company adds a box to its form without telling the other, the other company misreads every form. A **schema registry** is a shared, versioned library of form designs with a rule book: before anyone may print a new version, the library checks that readers of the old version can still cope with it.
:::

Kafka stores bytes; it does not know what is in them. Without agreement on structure, one producer change (renaming a field, changing a type) can break every consumer, often at 3 a.m. A **schema** describes the structure of records, and a **schema registry** stores schemas centrally, gives each an ID, and enforces **compatibility rules** when a new version is registered.

**Formats.** Three are common:

| Format | Strengths | Evolution rules in short |
|---|---|---|
| **Avro** | Compact binary, schema needed to read, rich evolution rules | Add or remove fields with defaults; reader and writer schemas resolved at read time |
| **Protobuf** | Compact, code generation for many languages, widely used in services | Never reuse or change field numbers; add new fields; `reserved` for removed ones |
| **JSON Schema** | Human-readable JSON payloads | Validation rules; evolution is harder to make safe |

**How it works with Kafka (Confluent Schema Registry and compatible registries):**

1. The producer's serializer registers (or looks up) the schema under a **subject**, by default `<topic>-value` and `<topic>-key` (the topic name strategy), and receives a schema ID.
2. It writes each record in the **wire format**: a magic byte `0`, the 4-byte schema ID, then the encoded payload.
3. The consumer's deserializer reads the ID, fetches the schema once (and caches it), and decodes.

```python
# The Confluent wire format: magic byte 0, a 4-byte schema id, then the payload
import struct

def frame(schema_id, payload):
    return struct.pack(">bI", 0, schema_id) + payload

def unframe(message):
    magic, schema_id = struct.unpack(">bI", message[:5])
    assert magic == 0, "not Schema Registry framed"
    return schema_id, message[5:]

msg = frame(42, b"\x02\x06abc")
print(msg.hex())          # 000000002a0206616263
print(unframe(msg))       # (42, b'\x02\x06abc')
```

Only 5 bytes of overhead per record, instead of sending the schema with every message.

**Compatibility modes** (set per subject or globally):

| Mode | Guarantee | Who upgrades first | Typical allowed change (Avro) |
|---|---|---|---|
| `BACKWARD` (default) | New schema can read data written with the previous one | Consumers | Delete a field; add a field with a default |
| `FORWARD` | Previous schema can read data written with the new one | Producers | Add a field; delete a field that has a default |
| `FULL` | Both | Either | Add or delete fields that have defaults |
| `*_TRANSITIVE` | Same, checked against **all** earlier versions, not just the last | | |
| `NONE` | No checks | | Anything (dangerous) |

A simplified check shows the logic for added fields:

```python
# A simplified Avro record compatibility check (field names and defaults only)
def can_read(reader, writer):
    """Can a consumer with schema `reader` read data written with `writer`?"""
    written = {f["name"] for f in writer["fields"]}
    return all(f["name"] in written or "default" in f for f in reader["fields"])

v1 = {"fields": [{"name": "order_id"}, {"name": "amount"}]}
v2 = {"fields": [{"name": "order_id"}, {"name": "amount"},
                 {"name": "currency", "default": "EUR"}]}            # added with default
v3 = {"fields": [{"name": "order_id"}, {"name": "amount"},
                 {"name": "channel"}]}                               # added without default

print("v2 backward:", can_read(v2, v1), "forward:", can_read(v1, v2))  # v2 backward: True forward: True
print("v3 backward:", can_read(v3, v1), "forward:", can_read(v1, v3))  # v3 backward: False forward: True
```

`v3` is rejected under `BACKWARD`: a consumer on `v3` reading old `v1` data finds no `channel` value and no default to use. (Real Avro resolution also checks types, unions, aliases and enums.)

**Registering and checking from the command line:**

```bash
# Is this new schema compatible with the latest registered version?
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"EUR\"}]}"}' \
  $REGISTRY/compatibility/subjects/orders-value/versions/latest
# {"is_compatible":true}
```

Run this check in CI on every pull request that changes a schema, so incompatible changes are caught before deployment.

**Registries.** Confluent Schema Registry (Confluent Community License) is the de facto API. Open-source alternatives implementing a compatible API include **Apicurio Registry** and **Karapace**. On Google Cloud, Google Managed Service for Apache Kafka offers a schema registry with a Confluent-compatible API (check its current launch stage), and **Pub/Sub** has its own schemas (Avro or Protocol Buffers) with revisions, validated when messages are published.

> **Pitfall:** Changing a field's type, renaming a field (without aliases) or removing a field without a default breaks consumers in ways that compatibility checks catch only if the mode is right. `NONE` is not a neutral default: it allows everything.

> **Pitfall:** `BACKWARD` checks only against the latest version. If old data with older versions is still in the topic (long retention, replays), use `BACKWARD_TRANSITIVE` so the new schema can read every version still present.

::: deeper
**Schema references and subject strategies.** `RecordNameStrategy` and `TopicRecordNameStrategy` name subjects after the record type, which allows several event types in one topic, each evolving separately. Schemas can reference other schemas (shared types), which the registry tracks.

**Data contracts.** Newer registries attach metadata and rules to schemas (field-level tags such as PII, validation rules, migration rules), turning a schema into a **data contract** between producing and consuming teams. The Data governance section returns to contracts.

**Schemas at the warehouse edge.** The same schemas drive downstream tables: connectors that write Avro or Protobuf topics into BigQuery or Iceberg map schema versions to table schema changes, so compatible evolution in Kafka becomes compatible evolution in the lakehouse.

> **Interview:** "Under BACKWARD compatibility, who deploys first, producers or consumers?" Consumers. The new schema must be able to read old data, so consumers upgrade to it first, and producers start writing with it afterwards.
:::

::: check
?? What are the first five bytes of a Schema Registry framed Kafka message?
A magic byte `0` followed by the 4-byte (big-endian) schema ID.

?? Under `BACKWARD` compatibility, can you add a new field without a default? Why?
No. A consumer using the new schema could not read old records, which lack the field and offer no default value.

?? When should you use a `_TRANSITIVE` compatibility mode?
When records written with several older schema versions can still be read (long retention, replays, lake tables), so every new schema must be compatible with all earlier versions, not just the last one.
:::

::: resources
- Docs: [Confluent Schema Registry overview](https://docs.confluent.io/platform/current/schema-registry/index.html) · architecture, subjects, wire format and serializers
- Docs: [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) · every compatibility mode with allowed changes per format
- Spec: [Apache Avro specification](https://avro.apache.org/docs/1.12.0/specification/) · schema resolution rules between reader and writer schemas
- Docs: [Protocol Buffers language guide (proto3)](https://protobuf.dev/programming-guides/proto3/) · field numbers, updating messages and reserved fields
- Docs: [Pub/Sub schemas](https://docs.cloud.google.com/pubsub/docs/schemas) · Avro and Protocol Buffer schemas and revisions on Google Cloud
- Repo: [Apicurio Registry](https://www.apicur.io/registry/) · an open-source registry compatible with the Confluent API
:::

## Other brokers and streaming SQL

Kafka is the default, not the only choice. This last group looks at two alternatives that keep the log idea but change its architecture: Apache Pulsar, which separates serving from storage, and Redpanda, which reimplements the Kafka protocol in C++ with a different internal design. It ends with streaming SQL engines, which let you describe continuous computations as SQL queries and materialized views instead of code.

### Apache Pulsar

::: plain
In Kafka, each librarian both lends books and keeps them on their own shelves. In Pulsar, the librarians at the front desk (brokers) only lend and take returns; the books live in a separate, specialised warehouse (BookKeeper). If a librarian goes home, another can take over the desk at once, because no books need to move.
:::

**Apache Pulsar** was built at Yahoo, open-sourced in 2016 and became an Apache top-level project in 2018. It is a messaging and streaming platform with a layered architecture:

- **Brokers** are stateless serving nodes. They receive messages from producers, dispatch them to consumers, and cache recent data, but they do not own storage. Each topic is served by one broker at a time, and topic ownership can move between brokers in seconds.
- **Apache BookKeeper** stores the data. A topic's log is a sequence of **ledgers**; each ledger is split into entries written to several **bookies** (storage nodes) in parallel, with a write quorum and an acknowledgement quorum (for example write to 3, acknowledge after 2). Ledgers are striped across bookies, so one topic's data is spread over many disks.
- A **metadata store** (ZooKeeper, or alternatives such as etcd and Oxia in recent versions) holds cluster metadata and topic ownership.

[[fig:streaming_pulsar]]

Because storage is separate, scaling and recovery are different from Kafka. Adding a broker adds serving capacity immediately (no data to copy). Adding a bookie adds storage capacity, and new ledgers start using it at once (no rebalancing of old data). A failed bookie's entries are re-replicated in the background from the other copies.

**Topics and multi-tenancy.** Topic names have three levels, `persistent://tenant/namespace/topic`. **Tenants** and **namespaces** are first-class: authentication, quotas, retention, replication and other policies are set per namespace, so many teams can share one cluster safely. A **partitioned topic** is a group of internal topics, each with its own broker, for parallelism.

**Subscriptions.** Consumers attach to a topic through a named **subscription** with one of four types:

| Subscription type | Behaviour | Similar to |
|---|---|---|
| Exclusive | One consumer only | A single Kafka consumer |
| Failover | One active consumer, others on standby | Kafka group with one partition |
| Shared | Messages spread round-robin over all consumers, ordering not kept | A work queue (RabbitMQ, Pub/Sub) |
| Key_Shared | Spread across consumers, but all messages with the same key go to the same consumer | Kafka partitioning, without a partition count limit |

Pulsar tracks **individual acknowledgements** per message (not only a single offset), supports **negative acknowledgements** and redelivery, **dead letter topics** and **delayed delivery**. That makes it a good fit for queue-style workloads that Kafka handles awkwardly, such as many workers sharing one topic.

```bash
bin/pulsar-admin tenants create analytics
bin/pulsar-admin namespaces create analytics/clickstream
bin/pulsar-admin namespaces set-retention analytics/clickstream --size 500G --time 7d
bin/pulsar-client produce persistent://analytics/clickstream/clicks -m '{"page":"/home"}'
bin/pulsar-client consume persistent://analytics/clickstream/clicks -s loaders -t Shared -n 0
```

**Other features:** built-in **geo-replication** between clusters, configured per namespace (no separate tool like MirrorMaker 2); **tiered storage offload** of old ledgers to object storage (Cloud Storage among others); **Pulsar Functions** for lightweight processing; **Pulsar IO** connectors; a schema registry built into the brokers; and protocol handlers, including one for the Kafka protocol, which let Kafka clients talk to Pulsar.

| | Kafka | Pulsar |
|---|---|---|
| Storage | On brokers (plus tiered storage) | Separate BookKeeper layer (plus offload) |
| Scaling serving | Requires partition reassignment (data copy) | Move topic ownership, no data copy |
| Consumption models | Consumer groups over partitions | Exclusive, failover, shared, key_shared |
| Multi-tenancy | Through ACLs and quotas | Tenants and namespaces built in |
| Geo-replication | MirrorMaker 2 or vendor tools | Built in |
| Operational parts | Brokers + KRaft controllers | Brokers + bookies + metadata store |
| Ecosystem | Largest by far | Smaller |

On Google Cloud there is no first-party Pulsar service; teams run it on GKE (with the Pulsar Helm chart) or use a vendor such as StreamNative, which offers Pulsar (and the Kafka protocol) as a managed service.

> **Pitfall:** Pulsar has more moving parts than a KRaft Kafka cluster: brokers, bookies, a metadata store and often proxies. Each needs monitoring, capacity planning and upgrades. Make sure its features (multi-tenancy, queue semantics, built-in geo-replication) are worth that.

> **Tip:** If you need both a work queue and a replayable log, Pulsar's subscription types cover both in one system; with Kafka you would add a queue (or use Kafka's newer share groups, an early-access feature at the time of writing).

::: deeper
**Write path.** A producer sends to the topic's owning broker, which writes the entry to the current ledger on the write quorum of bookies; each bookie appends to its **journal** (a write-ahead log, ideally on a dedicated fast disk) and acknowledges, then flushes to **entry log** files with indexes. The broker acknowledges the producer when the acknowledgement quorum has confirmed. Only one writer may write a ledger; when ownership changes, the new broker fences the old ledger and opens a new one, which is how Pulsar avoids two brokers writing at once.

**Read path.** Tailing consumers read from the broker's cache; catch-up reads go to bookies, and offloaded data comes from object storage. This separation keeps catch-up reads from evicting the cache that tailing reads need, a common problem in Kafka during replays.

> **Interview:** "Why can Pulsar move a topic to another broker in seconds while Kafka partition reassignment can take hours?" Pulsar brokers hold no data: the new owner opens a new ledger and reads old data from BookKeeper. Kafka partition data lives on broker disks and must be copied to the new replicas.
:::

::: check
?? What is the role of BookKeeper in Pulsar?
It is the storage layer: topic data is written as ledgers replicated across bookies, while brokers stay stateless and only serve producers and consumers.

?? Which Pulsar subscription type spreads messages across many consumers while keeping per-key order?
Key_Shared.

?? Name two Pulsar features that Kafka handles with separate tools or not at all.
Built-in geo-replication per namespace (Kafka uses MirrorMaker 2), and multi-tenancy with tenants and namespaces; also individual message acknowledgements for queue-style consumption.
:::

::: resources
- Docs: [Apache Pulsar documentation](https://pulsar.apache.org/docs/) · concepts, deployment and administration
- Docs: [Pulsar architecture overview](https://pulsar.apache.org/docs/next/concepts-architecture-overview/) · brokers, BookKeeper, metadata store and geo-replication
- Docs: [Pulsar messaging concepts](https://pulsar.apache.org/docs/next/concepts-messaging/) · subscriptions, acknowledgements, retention and dead letter topics
- Docs: [BookKeeper concepts](https://bookkeeper.apache.org/docs/getting-started/concepts) · ledgers, bookies, quorums and the journal
- Book: [Apache Pulsar in Action](https://www.manning.com/books/apache-pulsar-in-action) · David Kjerrumgaard; a practical introduction with examples
:::

### Redpanda — Kafka-Compatible Streaming

::: plain
Redpanda is a different engine in the same car body. Your Kafka clients, tools and connectors (the steering wheel and pedals) work unchanged, because Redpanda speaks the Kafka protocol. Underneath, the engine was rebuilt from scratch in C++ to squeeze more out of each machine and to need fewer separate parts.
:::

**Redpanda** (first released in 2020) implements the **Kafka API** in C++, so Kafka producers, consumers, Kafka Connect, Kafka Streams and most admin tools work against it. Its design choices differ from Apache Kafka's:

- **Thread-per-core.** Built on the **Seastar** framework, Redpanda pins one thread to each CPU core and shards partitions across cores. Each core owns its memory and work, and cores communicate by message passing instead of locks, which avoids contention and context switches.
- **No JVM and no page cache dependence.** Redpanda manages its own memory and uses direct I/O to disk, so there is no garbage collector and its performance depends less on the operating system's page cache.
- **Raft per partition.** Each partition's replicas form their own **Raft** group, which handles leader election and replication. Writes are acknowledged after a majority has them, and Redpanda by default `fsync`s data before acknowledging.
- **Single binary.** Broker, metadata (also replicated with Raft, through an internal controller group), schema registry and an HTTP proxy run in one process, with no ZooKeeper and no separate controller nodes.

```bash
rpk cluster info
rpk topic create orders -p 12 -r 3
rpk topic produce orders --key customer-42 <<< '{"order_id": 1}'
rpk topic consume orders --num 1
```

**Durability semantics.** Kafka acknowledges `acks=all` writes when all in-sync replicas have them in memory (page cache), relying on replication rather than `fsync` for durability. Redpanda flushes to disk before acknowledging by default, which protects against simultaneous power loss of several replicas but costs latency. Both are reasonable; know which one you have when comparing benchmarks.

**Performance claims.** Redpanda publishes benchmarks showing lower tail latency and fewer nodes than Kafka for the same throughput. Independent analyses (for example Jack Vanlightly's 2023 series) found that results depend heavily on workload, configuration, hardware and test duration, and that Kafka matches or beats Redpanda in some scenarios. Treat any vendor benchmark as a hypothesis and test with your own message sizes, partition counts, durability settings and retention.

**Features beyond the Kafka API:**

- **Tiered storage** to object storage (Cloud Storage included), with features to restore topics and create read replicas from it.
- **Data transforms** written in languages that compile to **WebAssembly** (Wasm), run inside the broker for simple per-record work such as filtering, masking or format conversion, avoiding a round trip to a separate processor.
- A built-in **schema registry** and **HTTP proxy** (Kafka API over REST).
- **Redpanda Cloud**, including a **bring-your-own-cloud** (BYOC) option that runs the data plane in your own Google Cloud project.

**Licensing.** The core is source-available under the Business Source License (BSL), with some enterprise features under a commercial license. Check the license if you plan to offer it as a service yourself.

| | Apache Kafka | Redpanda |
|---|---|---|
| Language / runtime | Java/Scala on the JVM | C++ (Seastar), no JVM |
| Replication | ISR with KRaft for metadata | Raft per partition |
| Default flush | Relies on replication; no `fsync` per write | `fsync` before acknowledging |
| Processes | Brokers plus controllers | One binary per node |
| In-broker transforms | No | Wasm data transforms |
| License | Apache 2.0 | BSL core plus enterprise features |

**The wider "Kafka-compatible" landscape.** Several newer systems speak the Kafka protocol with very different storage: **WarpStream** (acquired by Confluent in 2024) and others write directly to object storage with stateless agents ("diskless" Kafka), trading higher latency for much lower cost and simpler operations. Apache Kafka itself is adding diskless designs through KIPs under discussion. The Kafka protocol has become a standard API with several implementations, and Google Managed Service for Apache Kafka is one more on Google Cloud.

> **Pitfall:** "Kafka-compatible" rarely means 100%. Check the specific APIs you rely on: transactions, exactly-once in Kafka Streams, specific admin APIs, quotas, ACL details and newer protocol features (such as the KIP-848 consumer protocol).

> **Tip:** Run your own benchmark with the OpenMessaging Benchmark framework, which both Kafka and Redpanda comparisons use, and include a multi-hour run with retention and catch-up reads, not just a short produce test.

::: deeper
**Why thread-per-core helps.** Lock contention and cross-core cache traffic grow with core count. By giving each core exclusive ownership of some partitions and their memory, Redpanda keeps data in the core's caches and avoids locks; work for another core is sent as a message to it. The cost is that a hot partition is limited to one core.

**Raft versus ISR.** Raft requires a majority to acknowledge and to elect a leader, so 3 replicas tolerate 1 failure for both writes and elections. Kafka's ISR can shrink to fewer replicas and keep accepting writes (down to `min.insync.replicas`), giving more availability options but requiring careful settings to avoid data loss.

> **Interview:** "Two vendors' benchmarks disagree about Kafka versus Redpanda. How do you decide?" Reproduce with your workload: message size, partition count, producer batching, `acks` and `fsync` settings, retention and consumer catch-up, on the hardware you would use, for hours not minutes, and compare cost per MB/s at your latency target.
:::

::: check
?? Why can existing Kafka clients talk to Redpanda?
Redpanda implements the Kafka wire protocol, so clients see it as a Kafka cluster.

?? What is the main durability difference between Redpanda's and Kafka's defaults?
Redpanda `fsync`s data to disk before acknowledging writes; Kafka acknowledges once in-sync replicas have the data (typically in the page cache) and relies on replication for durability.

?? What are Redpanda data transforms?
Small per-record processing functions compiled to WebAssembly that run inside the broker, for example to filter, mask or convert records between topics.
:::

::: resources
- Docs: [Redpanda documentation](https://docs.redpanda.com/) · installation, configuration, Kafka compatibility and operations
- Docs: [Redpanda architecture](https://docs.redpanda.com/current/get-started/architecture/) · thread-per-core, Raft per partition and tiered storage
- Docs: [Redpanda data transforms](https://docs.redpanda.com/current/develop/data-transforms/) · writing and deploying Wasm transforms
- Blog: [Kafka vs Redpanda performance: do the claims add up?](https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up) · an independent, detailed benchmark analysis
- Docs: [Seastar](https://seastar.io/) · the C++ framework behind Redpanda's thread-per-core design
:::

### Streaming SQL — Flink SQL, Materialize, RisingWave

::: plain
A normal SQL query is a photograph: it shows the data at one moment. A **streaming SQL** query is a live video: you write it once, and its result keeps changing as new data arrives. A materialized view in a streaming database is a scoreboard that updates itself every time a goal is scored, instead of someone recounting all the goals each time you look.
:::

Streaming SQL lets you write continuous computations (filters, joins, aggregations, windows) in SQL instead of Java or Python code. The core idea is the **dynamic table**: a stream is viewed as a table that keeps changing, a query over it produces another changing table, and that result is emitted as a **changelog** of inserts, updates and deletes.

```sql
-- Flink SQL: a Kafka topic as a table, with event time and a watermark
CREATE TABLE orders (
  order_id STRING,
  city     STRING,
  amount   DECIMAL(10, 2),
  ts       TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'bootstrap.events.europe-west1.managedkafka.my-proj.cloud.goog:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'https://registry.example.internal'
);

-- A continuous query: revenue per city per minute
SELECT window_start, city, SUM(amount) AS revenue
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, city;
```

A windowed aggregation emits each window once (an **append-only** result). A non-windowed `GROUP BY city` instead emits an **update** every time a city's total changes: Flink encodes these as changelog rows (`+I` insert, `-U` retract the old value, `+U` add the new value, `-D` delete), and the sink must handle updates (an upsert Kafka topic, a database table keyed by `city`).

**Streaming joins** are the hard part, because both sides are unbounded:

| Join type | Semantics | State kept |
|---|---|---|
| Regular join | Like batch SQL, over all history; updates as either side changes | Both sides forever (bound it with a state TTL) |
| Interval join | Rows join if their times are within a bound (`o.ts BETWEEN p.ts - INTERVAL '1' HOUR AND p.ts`) | Only rows inside the interval |
| Temporal join | Join each event to the version of a table **as of** the event's time (`FOR SYSTEM_TIME AS OF o.ts`) | Versions of the table side |
| Lookup join | Query an external table (database, key-value store) per event at processing time | None (or a cache) |

```sql
-- Temporal join: convert each order with the exchange rate valid at the order's time
SELECT o.order_id, o.amount * r.rate AS amount_eur
FROM orders AS o
JOIN currency_rates FOR SYSTEM_TIME AS OF o.ts AS r
  ON o.currency = r.currency;
```

**Three engines, three designs:**

- **Flink SQL** compiles SQL into a Flink dataflow job. It inherits Flink's checkpoints, exactly-once and connectors, scales to very large state (RocksDB, and newer disaggregated state backends), and runs in streaming or batch mode. Results go to sinks (Kafka, databases, lakehouse tables such as Iceberg and Paimon); Flink itself does not serve queries. It is the most widely used streaming SQL engine and is offered as a managed service by several vendors.
- **Materialize** is a streaming database built on **Timely Dataflow** and **Differential Dataflow**, which compute incremental updates to query results. You create materialized views with ordinary SQL (including complex joins and subqueries), and they stay up to date with **strong consistency**: all views reflect the same point in time, so a join of two views never shows a half-applied transaction. It speaks the PostgreSQL wire protocol, so you query views with any Postgres client, and it can ingest change data capture directly from PostgreSQL and MySQL.
- **RisingWave** is an open-source streaming database written in Rust, also PostgreSQL-compatible, that stores operator state in object storage (Cloud Storage included) with local caching. This makes state cheap and elastic and recovery fast. Like Materialize, it serves queries on its materialized views directly and can sink results to Kafka, databases and Iceberg.

```sql
-- Materialize or RisingWave: a view that is always up to date, queried like a table
CREATE MATERIALIZED VIEW revenue_by_city AS
SELECT city, SUM(amount) AS revenue, COUNT(*) AS orders
FROM orders
GROUP BY city;

SELECT * FROM revenue_by_city WHERE city = 'Pune';   -- answered from maintained state
```

| | Flink SQL | Materialize | RisingWave |
|---|---|---|---|
| Model | SQL compiled to a streaming job | Streaming database (incremental view maintenance) | Streaming database |
| Serves queries itself | No, writes to sinks | Yes (PostgreSQL protocol) | Yes (PostgreSQL protocol) |
| State | Local RocksDB plus checkpoints | Memory and local disk, persisted to object storage | Object storage with local cache |
| Consistency | Per-job, eventually consistent outputs | Strong consistency across views | Consistent snapshots per barrier |
| Best for | Large-scale pipelines, ETL, CDC into lakes | Operational views needing correctness, complex SQL | Postgres-style streaming apps with cheap elastic state |

**On Google Cloud:** Dataflow with Beam SQL or Beam's YAML pipelines covers SQL-like streaming on a managed service; **BigQuery continuous queries** run SQL continuously over rows as they arrive in a BigQuery table and write results to Pub/Sub, Bigtable or other BigQuery tables. Flink, Materialize and RisingWave can run on GKE or as vendor-managed services.

> **Pitfall:** A regular streaming join or an unbounded `GROUP BY` on a high-cardinality key keeps state forever. Set a state time-to-live (Flink `table.exec.state.ttl`) or use interval or temporal joins, or state will grow until the job fails.

> **Pitfall:** Sinks must match the result type. Writing an updating result (with retractions) to an append-only sink fails in Flink at planning time, or, in other systems, silently produces duplicates. Use upsert sinks keyed by the grouping columns.

::: deeper
**Incremental view maintenance.** Recomputing a view from scratch for each change is too slow. Streaming databases maintain views incrementally: for a `SUM`, add the new value; for a join, join the new row against the indexed other side. Differential Dataflow generalises this to iterative and nested queries by tracking changes as (data, time, diff) triples. The cost is state: every join input must be indexed, so memory and storage grow with the data the view depends on.

**Consistency matters for joins.** If a CDC transaction updates an order and its line items together, an eventually consistent streaming join may briefly show the new order with old items. Materialize's strict serializability, and RisingWave's barrier-aligned snapshots, prevent such transient inconsistencies; with Flink, design sinks and consumers to tolerate them.

**Where streaming SQL meets RTOLAP.** A streaming SQL engine maintains a fixed set of results; a real-time OLAP engine (previous section) answers ad hoc queries over fresh data. A common design uses both: Flink SQL cleans, joins and enriches the stream, and Pinot or Druid serves flexible queries over its output.

> **Interview:** "When would you choose a temporal join over a regular join?" When each event should be joined with the version of the other table that was valid at the event's time, such as exchange rates or product prices, and when you need bounded state: a regular join would keep both sides forever and would also update past results when the other table changes.
:::

::: check
?? Why does a non-windowed `GROUP BY` in Flink SQL produce update and retract rows?
Each new event changes the aggregate for its key, so the previous result must be retracted (`-U`) and the new one emitted (`+U`); the result is an updating table, not an append-only stream.

?? What is the main architectural difference between Flink SQL and Materialize or RisingWave?
Flink SQL compiles queries into jobs that write results to external sinks; Materialize and RisingWave are databases that maintain materialized views and serve queries on them directly over the PostgreSQL protocol.

?? How do you stop a regular streaming join from growing state forever?
Set a state TTL so old rows expire, or replace it with an interval join or a temporal join, which keep only bounded state.
:::

::: resources
- Docs: [Flink SQL overview](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/) · statements, queries and connectors
- Docs: [Flink dynamic tables](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/) · continuous queries and changelog streams explained
- Docs: [Flink SQL joins](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/) · regular, interval, temporal and lookup joins
- Docs: [Materialize documentation](https://materialize.com/docs/) · sources, materialized views, consistency and the PostgreSQL interface
- Docs: [RisingWave documentation](https://docs.risingwave.com/) · architecture, SQL and sinks for the Rust streaming database
- Repo: [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) · the incremental computation library behind Materialize
- Docs: [BigQuery continuous queries](https://docs.cloud.google.com/bigquery/docs/continuous-queries-introduction) · continuous SQL over new rows on Google Cloud
:::

## Wrap-up

### Cheat sheet

| Concept or task | Rule of thumb, setting or command |
|---|---|
| Aggregate by | Event time, with a watermark delay from the measured lag (p99) |
| Late data | Drop, allowed lateness (update results), or side output |
| Windows | Tumbling (one each), sliding (size/slide each), session (gap per key) |
| Accumulating vs discarding | Accumulating with upsert sinks; discarding with additive sinks |
| Exactly-once recipe | Replayable source + checkpointed state and offsets + idempotent or transactional sink |
| Most robust default | At-least-once plus idempotent writes keyed by a unique event ID |
| Safe Kafka producer | `acks=all`, idempotence on (defaults since 3.0), handle callbacks, `flush()` on exit |
| Safe Kafka topic | Replication factor 3, `min.insync.replicas=2`, unclean election off |
| Throughput | Raise `linger.ms`, use `zstd` or `lz4` compression |
| Consumer at-least-once | `enable.auto.commit=false`, commit after processing |
| Consumer parallelism | At most one consumer per partition in a group |
| Kafka exactly-once inside Kafka | Transactions + `read_committed`; Streams `exactly_once_v2` |
| KRaft | 3 or 5 dedicated controllers across zones; Kafka 4.0 has no ZooKeeper |
| Tiered storage | `remote.storage.enable=true`, `local.retention.ms` short, `retention.ms` long |
| MirrorMaker 2 | Default naming `source.topic`; offset translation; RPO = replication lag |
| Schema compatibility | `BACKWARD` (consumers first); `_TRANSITIVE` when old versions remain |
| Wire format | Magic byte 0 + 4-byte schema ID + payload |
| Backpressure bottleneck | First busy (not backpressured) task downstream from the source |
| Alert on | Consumer lag growth, checkpoint duration, replication latency |
| Unbounded join state | State TTL, interval join or temporal join |

### Practice

::: try
**1. Choose the watermark.** Mobile events reach your pipeline with these event-time delays: median 2 seconds, 99th percentile 45 seconds, 99.9th percentile 20 minutes (phones offline). The business wants per-minute counts within about a minute and exact daily totals. Design the watermark, lateness and late-data handling.
:::

?? Answer to 1
Use a watermark delay of about 45 to 60 seconds, so per-minute results appear about a minute after each window ends and include 99% of events. Add allowed lateness (or a side output) for the long tail: either allow, say, 30 minutes of lateness and emit updated per-minute results to an upsert sink, or send later events to a side output. For exact daily totals, run a daily batch job (or a late-firing window over the day) over the raw events stored in the lake, which includes everything that arrived by then.

::: try
**2. Spot the guarantee.** A consumer reads a batch from Kafka, writes each record with `INSERT` into a BigQuery table, then commits the offsets. The job is restarted twice a day. What guarantee does it provide, what goes wrong, and how would you fix it?
:::

?? Answer to 2
At-least-once: if the job dies after inserting but before committing, the batch is re-read and inserted again, so duplicates appear. Fix by making the write idempotent: use the Storage Write API with application-created streams and offsets, or stage the batch and `MERGE` into the table on a unique event ID, or deduplicate downstream by event ID. Alternatively use Dataflow's exactly-once mode with a BigQuery sink.

::: try
**3. Size a topic.** A topic receives 30 MB/s at peak. Each consumer instance can process 4 MB/s. You expect traffic to triple in two years. How many partitions would you create, and why does the future matter now?
:::

?? Answer to 3
Today you need at least 30 / 4 = 8 consumers; in two years, 90 / 4 = 23. Create around 24 to 32 partitions now. Adding partitions later changes the key-to-partition mapping, breaking per-key ordering and key-partitioned consumers (such as Pinot upserts), so choose for future parallelism, while avoiding far more partitions than needed (each adds overhead).

::: try
**4. Evolve a schema.** The `orders-value` subject uses `BACKWARD` compatibility and the current schema has `order_id` (string) and `amount` (double). Which of these changes will the registry accept, and in what order should you deploy producers and consumers? (a) add `currency` with default `"EUR"`; (b) add `channel` without a default; (c) remove `amount`; (d) change `amount` to string.
:::

?? Answer to 4
(a) Accepted: a new reader can fill the default for old records. (b) Rejected: new readers could not read old records lacking `channel`. (c) Accepted under `BACKWARD`: new readers ignore the field in old data (but any consumer still needing `amount` breaks, so check usage). (d) Rejected: double to string is not a permitted Avro type promotion. Under `BACKWARD`, upgrade consumers first, then producers.

::: try
**5. Pick the platform.** For each case, choose Kafka (Google Managed Service for Apache Kafka), Pub/Sub, Pulsar or Redpanda and justify briefly: (a) a Google Cloud team with Dataflow pipelines and no Kafka experience; (b) a company with hundreds of existing Kafka Streams and Connect applications moving to Google Cloud; (c) a multi-tenant SaaS needing per-customer isolation, queue-style consumption and built-in geo-replication; (d) a team that wants Kafka-compatible APIs with a single-binary deployment on its own GKE cluster.
:::

?? Answer to 5
(a) Pub/Sub: serverless, integrates natively with Dataflow and BigQuery, nothing to size. (b) Google Managed Service for Apache Kafka: keeps the Kafka API, Streams and Connect, with managed brokers. (c) Pulsar: tenants and namespaces, shared and key_shared subscriptions, built-in geo-replication (run on GKE or through a vendor). (d) Redpanda: Kafka API compatible, single binary, no JVM; verify compatibility of the features they rely on and the license.

### What comes next

You now know how events are carried, stored, replicated, described and processed with correct time and delivery guarantees. The next section, **Event-driven architecture**, moves from pipes to design: how services communicate through events instead of calls, event sourcing and CQRS, the outbox pattern for publishing changes reliably, sagas for workflows that span services, and how these patterns look on Google Cloud with Pub/Sub, Eventarc and managed Kafka.
