This section builds the moving parts around BigQuery: the services that carry events, transform data in streams and batches, copy database changes, run SQL pipelines and decide what runs when. It is written for data engineers who will build and operate pipelines on Google Cloud. It assumes the foundations from Google Cloud foundations for data (projects, IAM, service accounts, regions, VPC, Cloud Monitoring) and the warehouse knowledge from BigQuery in depth (load jobs, the Storage Write API, partitioning, slots). It also leans on the general ideas from earlier sections: logs and delivery guarantees from Streaming systems, Beam from Flink, Beam, Trino and other engines, Spark from Apache Spark, CDC from Data ingestion, and DAGs from Orchestration. This section is about how those ideas run as Google Cloud services, what they cost and where they break.

**What you will be able to do afterwards**

- Design Pub/Sub topics and subscriptions with the right delivery type, ordering, exactly-once and dead-letter settings, and choose between Pub/Sub and a managed Kafka service.
- Run Beam pipelines on Dataflow and Spark jobs on Managed Service for Apache Spark, size and autoscale them, and read their health from metrics.
- Build SQL pipelines in BigQuery with Dataform, and decide when a low-code tool such as Cloud Data Fusion is the right fit.
- Replicate MySQL, PostgreSQL, Oracle and SQL Server into BigQuery with Datastream, including backfill and freshness settings.
- Orchestrate with managed Airflow, Astronomer, Workflows, Cloud Scheduler and Eventarc, and run small jobs on Cloud Run, Cloud Run functions and Batch.

**The big picture.** A pipeline platform on Google Cloud has four kinds of parts. **Transport** carries data as it is created: Pub/Sub for events, Google Managed Service for Apache Kafka when you want the Kafka API, Datastream for database changes. **Processing** reshapes it: Dataflow for Beam code in streams and batches, Managed Service for Apache Spark for Spark, Dataform for SQL inside BigQuery, Cloud Data Fusion for visual pipelines, and Cloud Run or Batch for small custom jobs. **Control** decides when work runs and what happens on failure: Managed Service for Apache Airflow for data DAGs, Workflows for chains of API calls, Cloud Scheduler for cron, Eventarc for reacting to events. **Operations** watch it all through Cloud Monitoring and Cloud Logging. Most real platforms use only a few of these, and the skill this section teaches is choosing the fewest parts that meet the freshness, cost and team constraints.

[[fig:gcppipes_map]]

> **Note:** Several services here were renamed in 2025 and 2026. This section uses the current names and gives the former one once: **Managed Service for Apache Spark** (formerly Dataproc), with its **serverless** mode for Spark batches (formerly Dataproc Serverless); **Managed Service for Apache Airflow**, often shortened to Managed Airflow (formerly Cloud Composer), whose current generation is **Managed Airflow (Gen 3)** (formerly Composer 3); **Cloud Run functions** (formerly Cloud Functions); **Knowledge Catalog** (formerly Dataplex Universal Catalog); and **Gemini Enterprise Agent Platform** (formerly Vertex AI). Topic titles below keep the older names where the course outline uses them. APIs, `gcloud` command groups and IAM roles kept the old names, so you still type `gcloud dataproc` and `gcloud composer`.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Messaging and streaming

Every streaming pipeline starts with a durable place where producers can drop events and consumers can pick them up later, at their own pace. On Google Cloud you choose between two families. **Pub/Sub** is Google's own serverless messaging service: no partitions, no brokers, nothing to size. **Kafka** is the open-source log that much of the industry has standardised on, available as Google Managed Service for Apache Kafka or from vendors such as Confluent and Aiven running on Google Cloud.

The third topic in this group is **Dataflow**, the service that most often reads from those transports, transforms the events and writes them to BigQuery, Cloud Storage or Bigtable. Streaming systems explained the theory (event time, watermarks, exactly-once); this group shows how the Google Cloud services implement it and which settings matter in production.

### Pub/Sub in depth

::: plain
Pub/Sub is a post office for messages. A sender drops a letter into a named mailbox (a **topic**) and walks away. Each department that wants copies has its own pigeonhole (a **subscription**), and every pigeonhole gets its own copy of every letter. A clerk keeps a letter in the pigeonhole until the department signs for it (an **acknowledgement**); if nobody signs in time, the letter is offered again. You never hire more clerks or build more shelves: the post office grows by itself.
:::

**The model.** A **publisher** sends a **message** to a **topic**. A message has a `data` payload (up to 10 MB), up to 100 **attributes** (string key-value pairs that act like headers), an optional **ordering key**, and a server-assigned message ID and publish time. A **subscription** is attached to one topic and receives every message published after it was created. Several subscriptions on one topic give fan-out: the fraud service, the BigQuery loader and the email service each read independently, at their own speed, which is the Pub/Sub equivalent of Kafka consumer groups.

Inside a subscription, Pub/Sub **leases** each message to a subscriber client. The subscriber must **acknowledge** (ack) it before the **ack deadline** (10 seconds by default, up to 600 seconds); otherwise Pub/Sub redelivers it. Client libraries extend the deadline automatically while your code is still working. Unacknowledged messages are kept for the subscription's **retention** (7 days by default, from 10 minutes to 31 days). Topics can also retain messages (up to 31 days), which lets a new subscription **seek** back in time and replay history. Delivery is **at least once** by default: the same message can arrive twice.

[[fig:gcppipes_pubsub]]

**Subscription types.** The type decides who moves the message out of Pub/Sub:

| Type | How messages leave | Typical use |
|---|---|---|
| Pull (StreamingPull) | Your client holds a stream open and receives messages | Dataflow, GKE and Cloud Run consumers, high throughput |
| Push | Pub/Sub sends an HTTPS POST to an endpoint, with an OIDC token | Cloud Run services, webhooks; scale to zero |
| BigQuery | Pub/Sub writes each message as a row through the Storage Write API | Raw event tables with no code |
| Cloud Storage | Pub/Sub batches messages into files by time or size | Landing raw events in the lake as text or Avro files |

A realistic setup for an orders topic:

```bash
gcloud pubsub topics create orders --message-retention-duration=7d
gcloud pubsub topics create orders-dlq

# 1. A pull subscription for the fraud service: ordered per order, exactly-once, dead-lettered
gcloud pubsub subscriptions create orders-fraud \
  --topic=orders --ack-deadline=60 \
  --enable-message-ordering --enable-exactly-once-delivery \
  --dead-letter-topic=orders-dlq --max-delivery-attempts=10 \
  --min-retry-delay=10s --max-retry-delay=600s

# 2. Every message into BigQuery, with metadata columns (message_id, publish_time, attributes)
gcloud pubsub subscriptions create orders-to-bq \
  --topic=orders --bigquery-table=acme-prod:raw.orders_events \
  --use-table-schema --write-metadata

# 3. Raw Avro files in the lake, one file every 5 minutes or so
gcloud pubsub subscriptions create orders-to-gcs \
  --topic=orders --cloud-storage-bucket=acme-raw-events \
  --cloud-storage-file-prefix=orders/ --cloud-storage-max-duration=5m \
  --cloud-storage-output-format=avro
```

**Ordering keys.** By default Pub/Sub does not keep order: it spreads messages over many servers for throughput. If a publisher sets an **ordering key** (for example the order ID) and the subscription has message ordering enabled, messages with the same key published in the same region are delivered in publish order. Throughput per key is limited (1 MBps per ordering key), so use a key with many distinct values, never a constant.

```python
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient(
    publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
topic = publisher.topic_path("acme-prod", "orders")
future = publisher.publish(
    topic, b'{"order_id": "o-1001", "status": "PAID"}',
    ordering_key="o-1001",                 # same key, same order
    event_id="e-77f3", source="checkout")  # attributes
print(future.result())                     # the message ID Pub/Sub assigned
```

If a publish with an ordering key fails, the client pauses that key so later messages cannot overtake the failed one; your code must call `publisher.resume_publish(topic, "o-1001")` after handling the error.

**Exactly-once delivery.** With `--enable-exactly-once-delivery` on a pull subscription, Pub/Sub guarantees that a message whose ack **succeeded** is not delivered again, and the client can learn whether its ack succeeded (`ack_with_response` in Python). It applies only to pull subscriptions, only when subscribers connect in the same region, and it adds latency. It does not remove duplicates created by the **publisher**: a publisher that retries after a timeout can publish the same event twice with two different message IDs. That is why events should carry their own ID and consumers should be idempotent.

**Dead-letter topics.** After a message has failed `max-delivery-attempts` times (5 to 100, default 5), Pub/Sub forwards it to a dead-letter topic instead of retrying forever, so one poison message cannot block or waste the subscription. The Pub/Sub service agent needs permission to publish to the dead-letter topic and to subscribe to the source subscription; without those grants dead-lettering silently does nothing. Combine it with an **exponential backoff** retry policy (10 to 600 seconds) so a struggling downstream system is not hammered.

**Other features worth knowing.** **Filters** deliver only messages whose attributes match an expression (`attributes.source = "checkout"`); they cannot be changed after creation, and filtered-out messages are acknowledged automatically. **Schemas** (Avro or Protocol Buffers) validate messages at publish time. **Single message transforms** run a small JavaScript function on each message at the topic or subscription (up to 5 transforms per resource), for masking or reshaping without a Dataflow job.

> **Pitfall:** A push or BigQuery subscription with no dead-letter topic and a bad message. Pub/Sub keeps retrying it for the whole retention period, the backlog alert fires for days, and the real cause (one malformed record) is hidden. Always attach a dead-letter topic and a subscription on it that someone reads.

> **Pitfall:** Slow consumers with a short ack deadline. If processing takes 30 seconds and the deadline is 10 seconds with lease extension disabled, every message is redelivered while it is still being processed, and throughput collapses under duplicates. Let the client library manage leases, and set flow control (maximum outstanding messages and bytes) so a subscriber never takes more than it can finish.

Here is the idempotent-consumer pattern in plain Python. It runs as is and shows why "at least once plus deduplication" gives correct counts:

```python
deliveries = [  # (message_id, event_id, amount): a publisher retry and a redelivery
    ("m1", "e1", 10), ("m2", "e2", 5), ("m3", "e1", 10),   # m3: publisher retried e1
    ("m2", "e2", 5),                                        # m2 redelivered after a timeout
]
seen, total = set(), 0
for message_id, event_id, amount in deliveries:
    if event_id in seen:          # dedupe on the business event ID, not the message ID
        continue
    seen.add(event_id)
    total += amount
print(len(deliveries), len(seen), total)   # 4 2 15
```

In production the `seen` set is a table: a `MERGE` keyed on `event_id` in BigQuery, or `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY publish_time) = 1` in a downstream model.

::: deeper
**How Pub/Sub stores and serves messages.** Pub/Sub has a global control plane and regional data planes. Publishers connect to the nearest region (or a regional endpoint you choose), and a message is acknowledged to the publisher only after it is stored in more than one zone. A **message storage policy** on the topic restricts which regions may store messages, which matters for data residency. Subscribers in another region are served across regions, which adds latency and network cost, so keep heavy consumers in the publishing region.

**No partitions, by design.** Kafka parallelism is bounded by partitions, and ordering is per partition. Pub/Sub delivers messages individually, so a slow message does not block the others and adding subscriber instances scales immediately. The price is that ordering exists only per ordering key, and replay is by time or snapshot (`gcloud pubsub subscriptions seek`), not by offset.

**Billing.** Pub/Sub charges for throughput (bytes published and delivered, with a free monthly allowance), plus storage for retained messages and some subscription types. Each publish or pull request is billed for at least 1,000 bytes, so sending one tiny message per request wastes money; the client libraries batch for you (settings such as maximum messages, bytes and latency per batch).

**BigQuery subscriptions in detail.** They write through the Storage Write API and are at least once, so duplicates are possible; deduplicate downstream on an event ID or `message_id`. With a topic or table schema they map fields to columns; without one, the payload lands in a single `data` column. They also support CDC: if messages carry `_CHANGE_TYPE` (`UPSERT` or `DELETE`) and the table has a primary key, BigQuery applies upserts and deletes. Failed writes (for example a schema mismatch) are retried and then dead-lettered with an attribute explaining the error.

**Pub/Sub Lite** was a cheaper, zonal, partition-based variant. It has been retired; workloads that need partitions and offsets should use Google Managed Service for Apache Kafka.

> **Interview:** "Design delivery for payment events that must reach a ledger service exactly once, in order per account." Publish with the account ID as the ordering key and an event ID attribute; use a pull subscription with ordering and exactly-once delivery enabled, consumers in the same region, acks with response; make the ledger write idempotent on event ID anyway (publisher retries can still duplicate); attach a dead-letter topic with alerting; and monitor `oldest_unacked_message_age`.
:::

::: check
?? A topic has three subscriptions and 1,000 messages are published. How many deliveries happen, at minimum?
3,000: each subscription receives its own copy of every message. Redeliveries can make it more, never fewer.

?? Exactly-once delivery is enabled, yet the ledger shows a duplicate payment. Name a likely cause.
The publisher retried after a timeout and published the same event twice with two message IDs. Exactly-once covers redelivery of an acknowledged message, not publisher duplicates; dedupe on the event's own ID.

?? Why attach a dead-letter topic to every production subscription?
Without it, a message that always fails is retried for the whole retention period, wasting capacity and hiding the cause. After N attempts it moves aside where it can be inspected and replayed.
:::

::: resources
- Docs: [Pub/Sub overview](https://docs.cloud.google.com/pubsub/docs/overview) · the model, use cases and comparison with other messaging systems
- Docs: [Subscription properties](https://docs.cloud.google.com/pubsub/docs/subscription-properties) · ack deadlines, retention, retry policy, dead lettering, filters and ordering with their limits
- Docs: [Exactly-once delivery](https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery) · what the guarantee covers, client requirements and trade-offs
- Docs: [BigQuery subscriptions](https://docs.cloud.google.com/pubsub/docs/bigquery) · schema mapping, metadata, CDC and failure handling
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · Akidau, Chernyak and Lax; the delivery and correctness ideas behind Pub/Sub and Dataflow
- Docs: [Pub/Sub quotas and limits](https://docs.cloud.google.com/pubsub/quotas) · message sizes, throughput per region and per ordering key
:::

### Managed Kafka on Google Cloud: Google Managed Service for Apache Kafka, Confluent Cloud and Aiven

::: plain
Running Kafka yourself is like owning a fleet of delivery vans: you buy them, service them, hire drivers and handle breakdowns at 3 a.m. A managed Kafka service is a van-leasing company: you still decide the routes and how many vans you need, but someone else maintains them and replaces broken ones. Google, Confluent and Aiven all lease Kafka on Google Cloud; they differ in how much they manage, which extras they include and how they charge.
:::

**Why Kafka when Pub/Sub exists?** Pub/Sub is simpler, but some teams need the **Kafka API** itself: existing producers and consumers, Kafka Streams applications, Kafka Connect connectors, Flink jobs reading Kafka offsets, replay by offset, partition-level ordering, or one platform across several clouds and on-premises sites. The Kafka internals (partitions, replication, consumer groups) are in Streaming systems; here the question is who runs the brokers.

**Google Managed Service for Apache Kafka** runs open-source Apache Kafka clusters in KRaft mode (no ZooKeeper) inside Google's infrastructure, reachable from your VPCs through Private Service Connect. You do not choose brokers directly. You choose **total vCPUs** (at least 3) and **memory** (1 to 8 GiB per vCPU), and the service decides how many brokers to run and spreads them over three zones. Storage is **tiered**: recent segments sit on fast local disk, older data moves automatically to cheaper remote storage, so retention is limited by cost rather than disk size. Authentication uses **IAM** identities over SASL (each service account is a Kafka principal) or mutual TLS with client certificates; authorisation uses IAM roles or Kafka ACLs.

```bash
gcloud managed-kafka clusters create events \
  --location=europe-west1 --cpu=12 --memory=48GiB \
  --subnets=projects/acme-net/regions/europe-west1/subnetworks/data-subnet

gcloud managed-kafka topics create orders \
  --cluster=events --location=europe-west1 \
  --partitions=24 --replication-factor=3 \
  --configs=retention.ms=604800000          # 7 days
```

**Sizing it.** Google's sizing guide estimates about 20 MBps per vCPU at full utilisation and recommends planning for 50%. Reads cost less CPU than writes, so it counts a "write-equivalent" rate of writes plus a quarter of reads. A worked example that runs as is:

```python
import math

write_mbps = 60                 # producers write 60 MBps
read_mbps = 3 * write_mbps      # three consumer groups each read everything
write_equiv = write_mbps + read_mbps / 4
vcpus = max(3, math.ceil(write_equiv / 20 / 0.5))   # 20 MBps per vCPU, 50% target
memory_gib = vcpus * 4                               # 4 GiB per vCPU, the recommended ratio
print(write_equiv, vcpus, memory_gib)                # 105.0 11 44
```

**Kafka Connect clusters.** The service can also run managed **Connect clusters** attached to a Kafka cluster, with built-in connectors for BigQuery and Cloud Storage sinks, Pub/Sub source and sink, and MirrorMaker 2 for replication between clusters (useful for migrations). A **schema registry** compatible with the Confluent Schema Registry API (Avro and Protobuf) is available in preview at the time of writing.

**Confluent Cloud** is the fully managed platform from Kafka's original authors, deployable in Google Cloud regions and purchasable through Google Cloud Marketplace (so spend counts against Google commitments). It goes beyond brokers: cluster types from serverless Basic and Standard to Enterprise and Dedicated, a large catalogue of fully managed connectors, **Schema Registry** with Stream Governance (data contracts, lineage), managed **Apache Flink** for stream processing, and **Tableflow**, which materialises topics as Iceberg or Delta tables. Private networking to Google Cloud VPCs uses Private Service Connect or VPC peering.

**Aiven for Apache Kafka** runs open-source Kafka on Google Cloud (in Aiven's account or yours with bring-your-own-cloud), with the open-source **Karapace** schema registry, Kafka Connect and MirrorMaker 2. It appeals to teams that want a managed service while staying close to plain open-source components and to the same product on several clouds. **Redpanda** is a Kafka-API-compatible broker written in C++ without the JVM, offered as Redpanda Cloud (including bring-your-own-cloud on Google Cloud); clients work unchanged, but some Kafka features and ecosystem tools behave differently, so test.

| Option | You manage | Strengths | Watch out for |
|---|---|---|---|
| Self-managed on GKE (Strimzi operator) or VMs | Brokers, upgrades, disks, rebalancing, security | Full control, any version and plugin | Operations load, on-call, upgrades |
| Google Managed Service for Apache Kafka | Topics, sizing (vCPU and memory), clients | IAM, PSC, Google billing and support, tiered storage | Fewer extras than Confluent; schema registry in preview |
| Confluent Cloud | Topics, connectors, contracts | Richest ecosystem: connectors, Flink, governance, Tableflow | Price at high throughput, separate vendor console |
| Aiven | Topics, plan size | Open-source components, multi-cloud, BYOC | Smaller managed connector catalogue |
| Pub/Sub (not Kafka) | Topics and subscriptions only | Serverless, per-message delivery, native integrations | No Kafka API, no offsets or partitions |

> **Pitfall:** Choosing Kafka because "everyone uses Kafka" when every producer and consumer is on Google Cloud and nobody needs the Kafka API. Pub/Sub would remove all capacity planning. Choose Kafka for concrete reasons: existing clients, Connect or Streams, offset replay, or a multi-cloud backbone.

> **Tip:** Migrating from self-managed Kafka: create the managed cluster, replicate topics with MirrorMaker 2 (which also translates consumer offsets), move consumers first, then producers, then switch off replication. Rehearse with a non-critical topic.

::: deeper
**Network cost is the hidden bill.** Kafka replicates every write to brokers in two other zones, and consumers may read from leaders in other zones. On a self-managed cluster you pay inter-zone traffic for all of it; managed services fold replication into their price, but client traffic still crosses zones. **Fetch-from-follower** (KIP-392, `client.rack` on consumers plus a rack-aware replica selector on brokers) lets consumers read from a replica in their own zone, which matters at hundreds of MBps.

**Partition count still matters.** Even on a managed service, a partition is the unit of parallelism and ordering. Google's guide caps a cluster at 100,000 partition replicas and suggests about 4,000 per broker. Choose partitions for the consumer parallelism you need in a year (24 or 48 is common), not thousands "just in case": every partition adds open files, metadata and recovery time.

**Connecting clients to Google's service.** Java clients authenticate with a Google-provided login callback handler that exchanges application default credentials for tokens over SASL; other clients can use SASL/PLAIN with a service account's credentials. Pub/Sub and Dataflow integrate directly: Dataflow's Kafka IO reads managed clusters, and Pub/Sub **import topics** can ingest from external streaming sources without code.

**Cost model comparison.** Google's service bills provisioned vCPU, memory and local storage per hour plus remote storage and transfer; Confluent bills by its capacity units, throughput, storage and connectors; Aiven by plan size. A steady high-throughput workload often favours provisioned pricing; a spiky, small one favours serverless clusters or Pub/Sub.

> **Interview:** "Pub/Sub or Kafka for a new Google Cloud platform?" Default to Pub/Sub if everything is on Google Cloud and consumers are Dataflow, BigQuery and Cloud Run: no capacity planning, per-message scaling. Choose Google Managed Service for Apache Kafka when you need the Kafka API (existing clients, Streams, Connect, offset replay, Flink) or a backbone shared with other environments; choose Confluent Cloud when you also want managed connectors, governance and Flink from one vendor.
:::

::: check
?? Producers write 40 MBps and two consumer groups each read everything. Using Google's guidance (20 MBps per vCPU, 50% target, reads count a quarter), how many vCPUs should the cluster have?
Write-equivalent is 40 + 80/4 = 60 MBps; 60 / 20 / 0.5 = 6 vCPUs.

?? Name two reasons to choose a managed Kafka service over Pub/Sub.
Any two of: existing Kafka clients or Kafka Streams applications, Kafka Connect connectors, replay by offset and partition-level ordering, Flink or other engines that expect Kafka, one backbone shared across clouds or on-premises.

?? What does Confluent Cloud add beyond managed brokers?
Fully managed connectors, Schema Registry and governance features, managed Flink, and Tableflow to expose topics as Iceberg or Delta tables.
:::

::: resources
- Docs: [Google Managed Service for Apache Kafka overview](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/overview) · clusters, storage, security, networking and Connect
- Docs: [Plan your Kafka cluster size](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/plan-cluster-size) · the vCPU, memory, partition and disk formulas used above
- Docs: [Confluent Cloud documentation](https://docs.confluent.io/cloud/current/overview.html) · cluster types, connectors, Schema Registry, Flink and Tableflow
- Docs: [Aiven for Apache Kafka](https://aiven.io/docs/products/kafka) · Aiven's managed Kafka, Karapace and Connect
- Book: [Kafka: The Definitive Guide, 2nd edition](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/) · Shapira, Palino, Sivaram and Petty; the internals that matter even when someone else runs the brokers
- Docs: [Strimzi documentation](https://strimzi.io/documentation/) · the operator most teams use to self-manage Kafka on GKE, for comparison
:::

### Dataflow in depth

::: plain
Dataflow is a factory that builds itself around your recipe. You write the recipe once with Apache Beam ("read these orders, group them by minute, add them up, write the totals"). Dataflow reads the recipe, sets up a production line of machines, adds machines when orders pile up, removes them when it is quiet, and replaces any machine that breaks, without losing or double-counting anything. You pay for the machines while they run.
:::

**What Dataflow is.** Dataflow is Google's managed runner for **Apache Beam** pipelines, in batch and streaming. Beam itself (PCollections, transforms, windows, triggers, watermarks) is covered in Flink, Beam, Trino and other engines; this topic is about what happens after you press run.

**From code to job.** When you launch a pipeline with `--runner=DataflowRunner`, the SDK turns your code into a graph and sends it to the Dataflow service, which:

1. **Optimises the graph.** **Fusion** merges consecutive steps (parse, filter, map) into one stage so elements pass between them in memory. **Combiner lifting** moves partial aggregation before the shuffle, so `CombinePerKey(sum)` sends one partial sum per key per worker instead of every element.
2. **Starts workers.** Workers are Compute Engine VMs in your project (so your VPC, firewall rules and service account apply). With **Runner v2**, each worker runs your SDK code in a container next to the Dataflow worker process, which is what makes Python, Go and cross-language pipelines possible.
3. **Moves shuffle and state off the workers.** In batch, **Dataflow Shuffle** performs `GroupByKey` in a backend service instead of on worker disks. In streaming, **Streaming Engine** keeps shuffle and windowing state in the backend. Workers then hold less state, can be smaller, and can be added or removed quickly.
4. **Autoscales and rebalances.** Horizontal autoscaling picks the worker count from backlog, throughput and CPU. In batch, **dynamic work rebalancing** splits the remaining work of a slow worker and gives it to idle ones, which cures most straggler problems without your help.

[[fig:gcppipes_dataflow]]

A streaming pipeline from Pub/Sub to BigQuery, with per-minute revenue by country:

```python
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window

class ToRow(beam.DoFn):
    def process(self, kv, win=beam.DoFn.WindowParam):
        country, revenue = kv
        yield {"window_start": win.start.to_utc_datetime().isoformat(),
               "country": country, "revenue": revenue}

def run(argv=None):
    opts = PipelineOptions(argv, streaming=True)
    with beam.Pipeline(options=opts) as p:
        (p
         | "Read" >> beam.io.ReadFromPubSub(
               subscription="projects/acme-prod/subscriptions/orders-dataflow")
         | "Parse" >> beam.Map(json.loads)
         | "Key" >> beam.Map(lambda o: (o["country"], float(o["amount"])))
         | "Minute" >> beam.WindowInto(window.FixedWindows(60))
         | "Sum" >> beam.CombinePerKey(sum)
         | "Row" >> beam.ParDo(ToRow())
         | "Write" >> beam.io.WriteToBigQuery(
               "acme-prod:marts.revenue_per_minute",
               schema="window_start:TIMESTAMP,country:STRING,revenue:FLOAT",
               method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API))

if __name__ == "__main__":
    run()
```

```bash
python revenue_stream.py \
  --runner=DataflowRunner --project=acme-prod --region=europe-west1 \
  --temp_location=gs://acme-dataflow/tmp --job_name=revenue-stream \
  --service_account_email=df-revenue@acme-prod.iam.gserviceaccount.com \
  --machine_type=n2-standard-2 --max_num_workers=20 --no_use_public_ips
```

**Streaming modes.** By default Dataflow streaming is **exactly-once**: each record's effect on state and outputs happens once, even across retries, thanks to checkpointed state and deduplication in the backend. You can switch a job to **at-least-once** mode (`--dataflow_service_options=streaming_mode_at_least_once`), which is cheaper and has lower latency, for pipelines that tolerate duplicates or deduplicate downstream (for example a pass-through from Pub/Sub to BigQuery).

**Templates.** A template packages a pipeline so people and schedulers can launch it with parameters, without a development environment. **Flex templates** are recommended for new work: the pipeline is a container image in Artifact Registry plus a small spec file in Cloud Storage, and the job graph is built at launch from the parameters. Classic templates serialise a fixed graph and need `ValueProvider` parameters. Google also publishes open-source **Google-provided templates** for common jobs (Pub/Sub to BigQuery, Cloud Storage text to BigQuery, Datastream to BigQuery, JDBC to BigQuery and others), and the **job builder** in the console builds simple pipelines without code.

```bash
gcloud dataflow flex-template run "orders-backfill-$(date +%Y%m%d)" \
  --region=europe-west1 \
  --template-file-gcs-location=gs://acme-templates/orders_backfill.json \
  --parameters=start_date=2026-11-01,end_date=2026-11-22 \
  --additional-user-labels=team=orders,env=prod
```

**Monitoring.** Two numbers summarise a streaming job's health. **System lag** is how long the oldest unprocessed item has waited; **data freshness** (watermark age) is how far the output's event time trails real time. Rising system lag with low CPU usually means a slow external call or a hot key; rising lag with high CPU means the job needs more or bigger workers. The job page shows each stage's throughput, wall time and backlog; Cloud Monitoring exposes the same metrics (`job/system_lag`, `job/data_watermark_age`, `job/backlog_bytes`) for alerts.

> **Pitfall:** Cancelling a streaming job to deploy new code. Cancel stops immediately and discards in-flight data and state. Use `--update` with the same job name to replace the code in place (Dataflow checks that the new graph is compatible and carries state across), or **drain**, which stops reading new input and finishes the in-flight windows before stopping.

> **Pitfall:** A hot key. If one key (a default country, a null user ID, one huge customer) carries most of the traffic, the single worker that owns it becomes the bottleneck and autoscaling cannot help. Dataflow logs "hot key detected" warnings. Fix the data (filter nulls), add a random salt and combine in two steps, or use `CombinePerKey(...).with_hot_key_fanout(n)`.

::: deeper
**Autoscaling numbers.** Streaming autoscaling aims to keep backlog drainable within a target time. A worked example: a job processes 2,000 messages per second per worker and normally runs 5 workers for 10,000 messages per second. A deploy pauses input for 2 minutes, leaving 1.2 million messages. Draining that in 2 more minutes needs 10,000 extra messages per second, so the autoscaler should roughly double the workers to 10 for a few minutes and then scale back. Set `--max_num_workers` high enough for such catch-up, and note that Streaming Engine makes scaling faster because state does not have to move between worker disks.

**Breaking fusion.** Fusion is usually good, but if a step fans out (one file name becomes millions of rows), the fused stage cannot spread the expanded work. A `beam.Reshuffle()` after the fan-out forces a shuffle so the rows are redistributed across workers.

**Limits to know.** With Streaming Engine, the aggregated input for one key in open windows (buffered elements plus custom state) is capped at 60 GB; exceeding it stalls the pipeline. Large side inputs are broadcast to every worker, so keep them small or use a lookup service.

**Dataflow Prime and right fitting.** **Dataflow Prime** adds **vertical autoscaling** (it raises worker memory when it sees out-of-memory pressure) and bills in Data Compute Units; Google-provided templates run on Prime by default. **Right fitting** lets different stages ask for different resources (for example GPUs only for a model-inference step).

**Cost levers.** You pay for worker vCPU, memory and disk per second, plus Dataflow Shuffle or Streaming Engine usage. Use small machines with Streaming Engine, turn off public IPs (and Cloud NAT if not needed), use **FlexRS** (flexible resource scheduling, which can delay a batch job to use cheaper capacity) for batch jobs without deadlines, and use at-least-once mode where duplicates are acceptable.

> **Interview:** "A streaming Dataflow job's system lag grows every day at 9 a.m. CPU is 30%. What do you check?" Low CPU with rising lag points away from capacity: look for a hot key in the stage with the highest wall time, a slow external call (enrichment API, Bigtable lookups without batching), BigQuery write quota errors, or a step waiting on a large side input. Fix the bottleneck before raising worker counts.
:::

::: check
?? What does Streaming Engine change about a Dataflow job?
Shuffle and windowing state move from worker disks to the Dataflow backend, so workers can be smaller, autoscaling reacts faster, and service updates do not need redeploys.

?? You need to deploy a bug fix to a running streaming job without losing windows in progress. What do you do?
Launch the new code with `--update` and the same job name (with a transform name mapping if steps were renamed); if the change is incompatible, drain the old job and start the new one.

?? Why are Flex templates recommended over classic templates?
The pipeline ships as a container image and the graph is built at launch from parameters, so it supports any dependencies and dynamic graphs without `ValueProvider` plumbing.
:::

::: resources
- Docs: [Dataflow overview](https://docs.cloud.google.com/dataflow/docs/overview) · what Dataflow does and how it relates to Apache Beam
- Docs: [Use Streaming Engine for streaming jobs](https://docs.cloud.google.com/dataflow/docs/streaming-engine) · benefits, billing models and limits
- Docs: [Dataflow templates](https://docs.cloud.google.com/dataflow/docs/concepts/dataflow-templates) · Flex versus classic templates and Google-provided templates
- Docs: [Apache Beam programming guide](https://beam.apache.org/documentation/programming-guide/) · the model your Dataflow code is written in
- Paper: [The Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) · the design behind windows, triggers and watermarks in Dataflow
- Paper: [MillWheel: Fault-Tolerant Stream Processing at Internet Scale](https://research.google/pubs/millwheel-fault-tolerant-stream-processing-at-internet-scale/) · the exactly-once streaming system Dataflow's streaming backend grew from
:::

## Batch and transformation

Streams are only part of the work. Most data on a typical platform still moves in batches: nightly extracts, hourly file drops, models rebuilt after each load. This group covers the three Google Cloud ways to transform data in batch, from the most code to the least. **Managed Service for Apache Spark** runs Spark (and the Hadoop tools around it) on clusters you create or as serverless batches. **Dataform** runs SQL pipelines inside BigQuery, with dependencies and tests, much like dbt. **Cloud Data Fusion** builds pipelines visually and runs them on Spark for you.

The choice usually follows the team and the existing code more than the data. Spark suits engineers with existing PySpark or Scala jobs and work that does not fit SQL; Dataform suits SQL-fluent teams whose data already sits in BigQuery; Data Fusion suits integration teams who prefer configuring connectors to writing code.

### Managed Service for Apache Spark (formerly Dataproc) and serverless Spark

::: plain
If Spark is a powerful kitchen, Managed Service for Apache Spark offers it two ways. You can rent the whole kitchen (a **cluster**): you choose the ovens and the number of cooks, and you pay for every hour it is open, cooking or not. Or you can order a **serverless** batch: you hand over the recipe, Google finds a kitchen of the right size, cooks the meal and closes up, and you pay only for the cooking time.
:::

**Clusters.** A cluster is a set of Compute Engine VMs with Spark, Hadoop YARN and optional components (Jupyter, Trino, Flink, Hive, Iceberg and others) preinstalled by an **image version** (for example `2.2-debian12`). It has one master (or three for high availability), **primary workers** that run YARN and HDFS, and optional **secondary workers** that only compute, usually **Spot VMs** at a large discount. Clusters start in about 90 seconds and bill per second (one-minute minimum), with a service fee of 1 cent per vCPU per hour on top of the VMs and disks. Because data lives in Cloud Storage and BigQuery rather than HDFS, the best practice is **ephemeral clusters**: create a cluster for a job or a workflow, run it, delete it.

```bash
# An ephemeral cluster for the nightly ETL: small fixed core, Spot workers that autoscale
gcloud dataproc clusters create etl-nightly \
  --region=europe-west1 --image-version=2.2-debian12 \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-8 --num-workers=2 \
  --secondary-worker-type=spot --num-secondary-workers=0 \
  --autoscaling-policy=etl-policy \
  --max-idle=30m --no-address \
  --service-account=spark-etl@acme-prod.iam.gserviceaccount.com

gcloud dataproc jobs submit pyspark gs://acme-code/jobs/daily_orders.py \
  --cluster=etl-nightly --region=europe-west1 -- --run-date=2026-11-22
```

`--max-idle` deletes the cluster after 30 idle minutes, a cheap insurance against forgotten clusters. The **autoscaling policy** scales on YARN pending and available memory:

```yaml
# policy.yaml, imported with:
#   gcloud dataproc autoscaling-policies import etl-policy --source=policy.yaml --region=europe-west1
workerConfig:
  minInstances: 2
  maxInstances: 2          # primaries fixed: they hold shuffle data
secondaryWorkerConfig:
  minInstances: 0
  maxInstances: 50         # Spot workers come and go
basicAlgorithm:
  cooldownPeriod: 2m
  yarnConfig:
    scaleUpFactor: 1.0
    scaleDownFactor: 1.0
    gracefulDecommissionTimeout: 1h
```

**Serverless batches.** The serverless mode (formerly Dataproc Serverless) runs a Spark batch or an interactive session with no cluster at all. You submit the job with a **runtime version**, which pins Spark, Java, Python and Scala; the service provisions executors, autoscales them with Spark dynamic allocation, and bills for the resources used while the batch runs (in Data Compute Units, plus shuffle storage).

```bash
gcloud dataproc batches submit pyspark gs://acme-code/jobs/daily_orders.py \
  --region=europe-west1 --version=2.3 \
  --deps-bucket=gs://acme-spark-deps \
  --service-account=spark-etl@acme-prod.iam.gserviceaccount.com \
  --subnet=data-subnet \
  --properties=spark.dynamicAllocation.maxExecutors=40 \
  -- --run-date=2026-11-22
```

| | Clusters | Serverless batches |
|---|---|---|
| Start-up | About 90 seconds per cluster, then jobs start instantly | Some tens of seconds per batch |
| Control | Machine types, Spot, GPUs, optional components, init actions, YARN settings | Runtime version, Spark properties, container image |
| Billing | VMs and disks per second plus 1 cent per vCPU-hour, while the cluster exists | Resources used while the batch runs |
| Best for | Many jobs per run, long-lived notebooks, Hadoop tools (Hive, Trino), tuned workloads | Independent jobs, spiky schedules, teams that do not want to tune clusters |

There is also a **premium tier** that adds the **Lightning Engine**, a faster Spark execution engine with native (vectorised) query execution, for both serverless and cluster workloads, with the trade-off of a higher price per unit.

**Spark with BigQuery.** The open-source **Spark BigQuery connector** (preinstalled on recent images and runtimes) reads tables through the BigQuery Storage Read API, in parallel streams with column projection and filter pushdown, and writes either **directly** through the Storage Write API or **indirectly** by staging Parquet or Avro files in Cloud Storage and running a load job.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("daily_orders").getOrCreate()

orders = (spark.read.format("bigquery")
          .option("table", "acme-prod.raw.orders")
          .option("filter", "order_date = '2026-11-22'")   # pushed down to BigQuery
          .load()
          .select("order_id", "customer_id", "amount", "status"))

daily = (orders.where(F.col("status") == "PAID")
         .groupBy("customer_id")
         .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders")))

(daily.write.format("bigquery")
      .option("writeMethod", "direct")                    # Storage Write API
      .mode("append")
      .save("acme-prod.marts.daily_customer_revenue"))
```

BigQuery can also run Spark for you: **stored procedures for Apache Spark** and **BigQuery Studio notebooks** start serverless Spark sessions (through Spark Connect) from inside BigQuery, which suits SQL teams who need an occasional PySpark step.

> **Pitfall:** Putting HDFS-heavy work on Spot secondary workers or scaling primaries down aggressively. When a worker holding shuffle files disappears, Spark must recompute the lost stages, and a job can run for hours of retries. Keep primaries fixed, use secondaries only for compute, and enable a graceful decommission timeout; on serverless, this is handled for you.

> **Pitfall:** One long-lived "shared" cluster for every team. Library conflicts, noisy neighbours and an idle bill every night follow. Prefer ephemeral clusters per workflow (created by the orchestrator) or serverless batches, and keep persistent clusters for interactive work with autoscaling and `--max-idle`.

::: deeper
**Where the data is matters.** With data in Cloud Storage, Spark reads through the Cloud Storage connector. Object stores have no cheap rename, so use committers designed for them (the default output committers on recent images, or table formats such as Iceberg that commit through metadata). Many small files hurt here exactly as on any object store: aim for files of 128 MB to 1 GB.

**Shuffle on clusters versus serverless.** On clusters, shuffle files live on worker local disks, so disk type and size bound shuffle-heavy jobs (local SSDs help). On serverless, shuffle storage is separate from executors, which makes scale-down safe.

**Metastore and catalogs.** Spark needs a catalog for table names. Options are a Hive metastore (the managed **Dataproc Metastore** service, or one on the cluster), or the **Lakehouse runtime catalog** (formerly BigLake metastore) for Iceberg tables shared with BigQuery. A shared catalog lets BigQuery and Spark read and write the same Iceberg tables; see [Google Cloud Lakehouse and Apache Iceberg on BigQuery](#bigquery).

**Other deployment options.** **Workflow templates** describe a cluster plus a DAG of jobs and create and delete the cluster around them. **Spark on GKE** runs Spark executors as Kubernetes pods when a team already runs GKE. Orchestrators call all of these: Airflow's Google provider has operators to create clusters, submit jobs and run serverless batches.

**Versions.** Cluster image versions and serverless runtime versions each have support windows; new runtimes bring Spark 4. Pin the version in code and upgrade deliberately, testing jobs on the new runtime first.

> **Interview:** "Migrate 300 PySpark jobs from an on-premises Hadoop cluster to Google Cloud." Copy data to Cloud Storage (Storage Transfer Service), move tables to Iceberg or keep Hive tables with a managed metastore, change `hdfs://` paths to `gs://`, run jobs on ephemeral clusters or serverless batches created by the orchestrator, use Spot secondary workers for cost, and move simple SQL jobs into BigQuery later. Migrate first, modernise second.
:::

::: check
?? Why are ephemeral clusters the recommended pattern?
Data lives in Cloud Storage and BigQuery, not on the cluster, so a cluster can be created for a job and deleted afterwards. You pay only while work runs and avoid shared-cluster conflicts.

?? When would you prefer a cluster over a serverless batch?
When you need control the serverless mode does not offer (GPUs, specific machine types, optional components such as Trino or Hive, init actions), or when many jobs run back to back so one cluster start is amortised.

?? What is the difference between the connector's `direct` and `indirect` write methods?
Direct writes rows through the BigQuery Storage Write API. Indirect writes files to a Cloud Storage bucket and then runs a BigQuery load job.
:::

::: resources
- Docs: [Managed Service for Apache Spark overview](https://docs.cloud.google.com/dataproc/docs/concepts/overview) · clusters, billing and how the service fits with the rest of Google Cloud
- Docs: [Managed Service for Apache Spark serverless overview](https://docs.cloud.google.com/dataproc-serverless/docs/overview) · batches, interactive sessions and notebooks
- Docs: [Autoscaling clusters](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling) · how policies scale on YARN memory and how to decommission safely
- Repo: [Spark BigQuery connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) · read and write options, pushdown and write methods
- Book: [Learning Spark, 2nd edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/) · Damji, Wenig, Das and Lee; the Spark you run on any of these services
- Docs: [Lightning Engine for serverless Spark](https://docs.cloud.google.com/managed-spark/docs/guides/lightning-engine-serverless) · the premium-tier engine and how to enable it
:::

### Dataform and SQL pipelines on BigQuery

::: plain
A data team often has dozens of SQL queries that must run in the right order: first clean the raw orders, then join them to customers, then build the revenue table. Dataform is the recipe book and the head chef for those queries. You write each query in its own file and say which other tables it needs; Dataform works out the order, runs them in BigQuery, checks the results against your rules, and keeps everything in Git.
:::

**What Dataform is.** Dataform is BigQuery's built-in tool for developing, testing, versioning and scheduling SQL transformations (the T in ELT). It has two halves. **Dataform core** is an open-source compiler: it reads **SQLX** files (SQL plus a config block and small JavaScript expressions) and produces a dependency graph of BigQuery SQL statements. The **Dataform service** in the Google Cloud console hosts repositories, development workspaces, compilation, scheduled runs and logs. Dataform itself has no charge; you pay for the BigQuery queries it runs.

A model with an incremental table and assertions:

```sql
-- definitions/marts/fct_orders.sqlx
config {
  type: "incremental",
  schema: "marts",
  uniqueKey: ["order_id"],                       -- incremental runs become a MERGE
  bigquery: {
    partitionBy: "DATE(updated_at)",
    clusterBy: ["customer_id"]
  },
  assertions: {
    nonNull: ["order_id", "customer_id"],
    rowConditions: ["amount >= 0"]
  },
  description: "One row per order, latest state."
}

SELECT order_id, customer_id, amount, status, updated_at
FROM ${ref("stg_orders")}
${when(incremental(),
  `WHERE updated_at > (SELECT MAX(updated_at) FROM ${self()})`)}
```

`${ref("stg_orders")}` does two jobs: it resolves to the right fully qualified table name for the environment, and it records a dependency, so `stg_orders` always runs first. `${self()}` is this table; `incremental()` is true when the table exists and the run is not a full refresh. Assertions compile into queries that must return zero rows; a failure stops dependent actions.

Project settings live in `workflow_settings.yaml` (Dataform core 3):

```yaml
defaultProject: acme-prod
defaultLocation: EU
defaultDataset: dataform
defaultAssertionDataset: dataform_assertions
dataformCoreVersion: 3.0.0
```

**From code to schedule.** Developers work in **workspaces** (private Git branches in the console) or locally with the CLI (`dataform compile`, `dataform run`). A **release configuration** compiles a chosen Git commitish with settings such as the project, a schema suffix (`_dev`) or variables; a **workflow configuration** runs a release on a cron schedule, optionally only selected tags. For pipelines that must wait for loads, trigger Dataform from Managed Airflow (the Google provider has Dataform operators), from Workflows, or from Cloud Build.

How the dependency graph turns into an order is the classic topological sort; Python's standard library has one, and this runs as is:

```python
from graphlib import TopologicalSorter

refs = {                                   # model -> the models it ${ref()}s
    "stg_orders": set(), "stg_customers": set(),
    "fct_orders": {"stg_orders"},
    "dim_customers": {"stg_customers"},
    "mart_revenue": {"fct_orders", "dim_customers"},
}
ts = TopologicalSorter(refs)
ts.prepare()
wave = 1
while ts.is_active():
    ready = sorted(ts.get_ready())         # these can run in parallel
    print(wave, ready)
    ts.done(*ready)
    wave += 1
# 1 ['stg_customers', 'stg_orders']
# 2 ['dim_customers', 'fct_orders']
# 3 ['mart_revenue']
```

**Dataform compared with dbt.** Both compile templated SQL into a dependency graph with tests and documentation. The differences are about ecosystem and hosting:

| | Dataform | dbt |
|---|---|---|
| Templating | SQLX with JavaScript expressions and includes | SQL with Jinja macros |
| Warehouses | BigQuery only | Many adapters (BigQuery, Snowflake, Databricks, Postgres and more) |
| Hosting | Built into Google Cloud, no extra charge | dbt Core (open source, you host it) or dbt's commercial platform |
| Tests | Assertions (uniqueness, not null, row conditions, custom SQL) | Generic and singular data tests, unit tests, many packages |
| Ecosystem | Smaller | Large: packages, semantic layer, wide community |
| Fits when | BigQuery-only team wanting zero extra infrastructure | Multi-warehouse, large community packages, existing dbt skills |

> **Pitfall:** Incremental models without a `uniqueKey` and late-arriving updates. Without a key, incremental runs only `INSERT`, so an order updated after it was first loaded appears twice. Set `uniqueKey` so Dataform generates a `MERGE`, and filter the source with a small lookback window (for example `updated_at > MAX(updated_at) - INTERVAL 1 DAY`) to catch late rows.

> **Tip:** Give each environment its own schema suffix through release configurations (`marts_dev`, `marts_ci`) and run every pull request against a CI dataset with `dataform run --dry-run` or a sampled run. SQL deserves the same review and CI as Python.

::: deeper
**How compilation works.** Dataform core evaluates the JavaScript in your project (config blocks, `includes/` helper functions, `js` blocks that generate many actions in a loop) and outputs a compiled graph: a JSON document listing every action, its SQL and its dependencies. The service then runs actions in dependency order, in parallel where possible, as BigQuery jobs under the repository's service account. Because compilation is separate from execution, a failed compile never touches data.

**Actions beyond tables.** `type: "view"`, `"materialized"` (materialized views), `"operations"` (arbitrary SQL such as `CALL` or DDL), `"declaration"` (a source table not built by Dataform, so `ref` works on raw data), and pre and post operations around a table (grants, deleting a partition before reinserting it).

**Integration with Knowledge Catalog and lineage.** After successful actions Dataform updates table and column descriptions in Knowledge Catalog (formerly Dataplex Universal Catalog), and BigQuery jobs it runs appear in data lineage automatically.

**Security.** Repositories run as a custom service account that needs BigQuery job and data permissions; Git credentials for GitHub or GitLab are stored in Secret Manager. Strict act-as mode makes sure a user can only run workflows as service accounts they are allowed to use.

> **Interview:** "Dataform or dbt for a BigQuery-only team of six analytics engineers?" Both work. Dataform wins on zero infrastructure and native integration; dbt wins on ecosystem, packages, unit tests and portability if another warehouse might appear. If the team already knows dbt, keep dbt (Core on Cloud Run or the commercial platform); if starting fresh and BigQuery-only, Dataform is a sound default.
:::

::: check
?? What two things does `${ref("stg_orders")}` do?
It resolves to the correct table name for the current environment, and it declares a dependency so `stg_orders` runs before this model.

?? An incremental table shows duplicate orders after some orders were updated. What is missing?
A `uniqueKey`. Without it incremental runs append rows; with it Dataform generates a `MERGE` that updates existing rows.

?? What is the difference between a release configuration and a workflow configuration?
A release configuration compiles a Git version with settings (project, suffixes, variables). A workflow configuration schedules runs of a compiled release, optionally selecting tags or actions.
:::

::: resources
- Docs: [Dataform overview](https://docs.cloud.google.com/dataform/docs/overview) · concepts, SQLX, repositories, workspaces and scheduling
- Docs: [Test data quality with assertions](https://docs.cloud.google.com/dataform/docs/assertions) · built-in and manual assertions and how failures stop a run
- Docs: [Create tables in Dataform](https://docs.cloud.google.com/dataform/docs/create-tables) · table types including incremental tables, `uniqueKey` and partitioning options
- Repo: [Dataform core on GitHub](https://github.com/dataform-co/dataform) · the open-source compiler and CLI
- Docs: [What is dbt?](https://docs.getdbt.com/docs/introduction) · the main alternative, for an honest comparison
- Book: [Analytics Engineering with SQL and dbt](https://www.oreilly.com/library/view/analytics-engineering-with/9781098142377/) · Machado and Russa; modelling and testing practices that carry over to Dataform
:::

### Cloud Data Fusion

::: plain
Cloud Data Fusion is a set of building blocks for pipelines. Instead of writing code, you drag a "read from database" block onto a canvas, connect it to a "clean these columns" block, then to a "write to BigQuery" block, and press run. Behind the scenes it turns your drawing into a Spark job and runs it on a cluster it creates for you. It is like assembling furniture from a kit: fast for standard pieces, awkward for anything unusual.
:::

**What it is.** Cloud Data Fusion is a managed, visual data integration service built on the open-source **CDAP** project. You create an **instance** (in one of three editions: **Developer**, **Basic**, **Enterprise**), which hosts the web interface (Studio), the pipeline metadata and the plugin catalogue. Pipelines are graphs of **plugins**: sources (databases over JDBC, Cloud Storage, BigQuery, SaaS applications, Kafka, Pub/Sub), transforms (wrangle, join, group by, deduplicate, JavaScript or Python snippets) and sinks.

**How a run works.** When a batch pipeline runs, Data Fusion compiles the graph into a Spark program and submits it to a **compute profile**. The default profile creates an **ephemeral Managed Service for Apache Spark cluster** in your project, runs the pipeline and deletes the cluster; you can also point a profile at an existing cluster to avoid start-up time. So a Data Fusion run costs the instance (billed per hour while it exists) plus the Spark cluster while the pipeline runs.

**The main tools inside it:**

- **Studio**: the canvas for batch and realtime pipelines, with macros (`${run_date}`) for runtime arguments.
- **Wrangler**: an interactive, spreadsheet-like view of sample data where each cleaning step (parse a date, split a column, mask a field) is recorded as a directive and becomes a transform in the pipeline.
- **Replication**: continuous CDC from operational databases into BigQuery, in editions that support it; for new CDC work, Datastream (next group) is usually the simpler choice.
- **Hub**: downloadable plugins, connectors and pipeline templates.
- **Lineage**: dataset- and field-level lineage for what pipelines read and write.

Pipelines are stored as JSON and can be exported, kept in Git and deployed through the CDAP REST API, which makes CI/CD possible but less natural than for code-first tools. A deployed pipeline is started by its schedule, by a trigger on another pipeline's completion, or from an orchestrator (Airflow's Google provider includes Data Fusion operators).

**When a low-code tool fits.** It fits when an integration team moves many similar source-to-sink feeds (dozens of databases and SaaS tables into BigQuery), when the people maintaining them are not software engineers, and when visual lineage and a plugin catalogue matter more than custom logic. It fits poorly when transformations are complex (many joins, business rules, testing needs), when SQL in BigQuery could do the transformation after a plain load, or when cost is tight, because the instance runs continuously and every run pays cluster start-up.

| Need | Often a better fit than Data Fusion |
|---|---|
| Replicate databases into BigQuery | Datastream |
| Scheduled imports from Google and SaaS sources | BigQuery Data Transfer Service, or Fivetran and Airbyte |
| Transform data already in BigQuery | Dataform or dbt |
| Standard streaming jobs (Pub/Sub to BigQuery) | Pub/Sub BigQuery subscriptions or Google-provided Dataflow templates |
| Complex batch logic in code | Spark on Managed Service for Apache Spark, or Dataflow |

> **Pitfall:** Running dozens of tiny pipelines each on its own ephemeral cluster. Each run pays cluster start-up (a minute or two) and minimum resources, so 50 small hourly pipelines can cost more in clusters than in data processed. Group small feeds into fewer pipelines, or use a compute profile pointing at a shared, autoscaling cluster for frequent small runs.

> **Pitfall:** Forgetting that the instance itself costs money even when no pipeline runs. Delete development instances that are not in use, and use the Developer edition for experiments.

::: deeper
**CDAP underneath.** CDAP (Cask Data Application Platform) provides the pipeline engine, the plugin framework, metadata and lineage stores, and the REST APIs. Data Fusion runs CDAP's services in a Google-managed tenant project; your pipelines run in your project on Spark. Custom plugins are Java (JAR plus JSON descriptor) uploaded to the instance, which is the escape hatch when no plugin fits.

**Networking.** A **private instance** has no public IP and reaches sources through VPC peering with your network; the ephemeral Spark clusters run in your VPC, so they need routes and firewall rules to on-premises databases (Cloud VPN or Interconnect) and Private Google Access for Google APIs.

**Execution engine choices.** Pipelines can push down some transformations (joins, aggregations) into BigQuery with **Transformation Pushdown**, so BigQuery does the heavy work instead of Spark, which is useful when sources and sinks are both in BigQuery.

**Realtime pipelines** run as Spark Streaming jobs on a long-lived cluster. For most streaming needs on Google Cloud, Dataflow or Pub/Sub subscriptions are simpler and more robust.

> **Interview:** "When would you recommend a low-code ETL tool?" When the main problem is many standard connections maintained by people who are not engineers, with lineage and governance requirements, and the transformations are simple. Recommend code-first tools (SQL with Dataform or dbt, Spark, Beam) when logic is complex, testing and CI/CD matter, or cost is tight.
:::

::: check
?? What does a Data Fusion batch run cost, in two parts?
The instance, billed per hour while it exists (by edition), plus the Managed Service for Apache Spark cluster that runs the pipeline (usually ephemeral, so only while the run lasts).

?? What is Wrangler for?
Interactive data preparation on a sample: each cleaning step becomes a recorded directive, which turns into a transform in the pipeline.

?? A team wants to replicate 40 PostgreSQL tables into BigQuery continuously. Would you use Data Fusion?
Usually not: Datastream is a serverless service built for exactly this, with backfill, CDC and merge into BigQuery, and no instance or clusters to pay for.
:::

::: resources
- Docs: [Cloud Data Fusion overview](https://docs.cloud.google.com/data-fusion/docs/concepts/overview) · instances, pipelines, plugins and how execution works
- Docs: [Cloud Data Fusion pricing](https://cloud.google.com/data-fusion/pricing) · editions and what is billed for instances and pipelines
- Repo: [CDAP on GitHub](https://github.com/cdapio/cdap) · the open-source platform underneath Data Fusion
- Docs: [Wrangler](https://docs.cloud.google.com/data-fusion/docs/concepts/wrangler-overview) · interactive data preparation and directives
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley; how to weigh managed, low-code and code-first tools
:::

## Ingestion and orchestration

The last group covers the edges of the platform. **Datastream** brings data in from operational databases as a continuous stream of changes, so BigQuery holds a near-current copy without nightly dumps. The other three topics are about control: which service decides when each step runs, how failures are retried, and how a file landing or a timer firing starts work.

The options form a ladder of weight. At the light end, **Cloud Scheduler** fires a request on a cron schedule and **Eventarc** routes events to a handler. In the middle, **Workflows** chains API calls with retries and branching. At the heavy end, **Managed Service for Apache Airflow** (or Astronomer) gives a full data orchestrator with DAGs, backfills and run history. Under all of them, **Cloud Run**, **Cloud Run functions** and **Batch** run the small custom pieces of code that do not deserve a cluster. Orchestration explained the concepts; here you choose and operate the Google Cloud versions.

### Datastream: serverless CDC into BigQuery

::: plain
Imagine you need an up-to-date copy of a shop's stock ledger in your head office. You could photocopy the whole ledger every night, but it is slow and the copy is a day old. Datastream instead photocopies the ledger once, then reads the shop's diary of every change ("sold 2 of item 17, restocked item 9") as it is written and applies each change to your copy within minutes. The shop's staff barely notice.
:::

**What it does.** Datastream is a serverless **change data capture** (CDC) service: it reads a database's transaction log and streams inserts, updates and deletes to **BigQuery** (or to **Cloud Storage** as files, for other consumers). Data ingestion explained log-based CDC with Debezium; Datastream is the managed Google Cloud equivalent, with no Kafka, connectors or servers to run.

**The objects.** A **connection profile** describes how to reach a source or a destination (host, credentials, connectivity). A **stream** joins a source profile to a destination profile and lists which schemas and tables to include. Each stream does two things:

1. **Backfill**: an initial snapshot of the selected tables, read in chunks with ordinary queries (automatic for new tables, or triggered per table on demand).
2. **CDC**: continuous reading of the transaction log from the moment the stream started, so changes made during the backfill are not lost.

[[fig:gcppipes_datastream]]

**How each source is read:**

| Source | Log mechanism | Setup you must do |
|---|---|---|
| MySQL | Binary log in row format (by position or GTID) | `binlog_format=ROW`, enough binlog retention, a user with replication privileges |
| PostgreSQL | Logical decoding with the `pgoutput` plugin | `wal_level=logical`, a publication, a logical replication slot |
| Oracle | LogMiner or a binary log reader | Archive log mode, supplemental logging |
| SQL Server | Change tables (SQL Server CDC) or transaction log reading | CDC enabled on the database and tables |

Datastream also supports AlloyDB, MongoDB, Spanner and Salesforce sources, with further SaaS sources in preview. Connectivity can be an IP allowlist, a forward SSH tunnel, or **private connectivity** (a VPC peering with your network, which can reach on-premises databases over Cloud VPN or Interconnect).

PostgreSQL preparation, for example on Cloud SQL (where the flag `cloudsql.logical_decoding` turns on logical decoding):

```sql
CREATE PUBLICATION datastream_pub FOR TABLE public.orders, public.customers;
SELECT pg_create_logical_replication_slot('datastream_slot', 'pgoutput');

CREATE USER datastream WITH REPLICATION LOGIN PASSWORD 'stored-in-secret-manager';
GRANT USAGE ON SCHEMA public TO datastream;
GRANT SELECT ON public.orders, public.customers TO datastream;
```

**Writing to BigQuery.** Datastream creates one table per source table (in a dataset per schema, or one dataset with a prefix) and adds a `datastream_metadata` column with a change UUID and the source timestamp. You choose a **write mode** per stream:

- **Merge** (the default): the BigQuery table mirrors the source. Datastream writes changes through the Storage Write API using the source primary key, and BigQuery applies upserts and deletes in the background. The table's **data freshness** setting (BigQuery's `max_staleness` option) bounds how stale a query may be: lower is fresher but costs more background merging.
- **Append-only**: every change becomes a new row with `change_type` (`INSERT`, `UPDATE-INSERT`, `UPDATE-DELETE`, `DELETE`), sort keys and a change sequence number. You get the full history and build current state yourself, which suits audit trails and slowly changing dimensions.

Building current state from an append-only change log is a "latest row per key" query. Here it is on SQLite so it runs as is; the same `ROW_NUMBER` pattern works in BigQuery with `QUALIFY`:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE changes (id INT, status TEXT, source_ts INT, change_type TEXT)")
db.executemany("INSERT INTO changes VALUES (?, ?, ?, ?)", [
    (1, "NEW", 100, "INSERT"), (2, "NEW", 105, "INSERT"),
    (1, "PAID", 130, "UPDATE-INSERT"), (2, "NEW", 140, "DELETE"),
    (3, "NEW", 150, "INSERT"), (1, "SHIPPED", 120, "UPDATE-INSERT"),  # arrived late
])
rows = db.execute("""
    SELECT id, status FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_ts DESC) AS rn
      FROM changes)
    WHERE rn = 1 AND change_type <> 'DELETE'
    ORDER BY id""").fetchall()
print(rows)   # [(1, 'PAID'), (3, 'NEW')]
```

Order 1's late-arriving `SHIPPED` change has an older source timestamp than `PAID`, so the latest state is `PAID`; order 2 was deleted. Ordering by **source** time (and a sequence number for ties), never by arrival time, is what keeps a replica correct.

**Latency and cost.** Changes typically reach BigQuery within seconds of commit, and the merged view is current within the freshness bound you set (often 15 minutes to an hour for analytics). Datastream bills per GiB of data processed, with backfill and CDC priced separately; BigQuery bills the background merges and storage.

> **Pitfall:** A paused or deleted stream with its PostgreSQL replication slot still in place. The database keeps every WAL file the slot has not confirmed, and the disk fills until the primary stops accepting writes. Alert on retained WAL and drop slots you no longer use:

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;
```

> **Pitfall:** Tables without a primary key. Datastream cannot merge them, so they become append-only in BigQuery (with an `is_deleted` flag), and on PostgreSQL they need `REPLICA IDENTITY` set or only inserts are captured. Add keys at the source, or plan downstream deduplication.

::: deeper
**Schema changes.** Additive changes (a new nullable column) are picked up and added to BigQuery tables. Destructive or reordering changes (dropped columns, type changes, columns inserted in the middle) are not always detected and can produce errors or wrong values, so coordinate DDL with the data team and test it in a staging stream.

**Head-of-line blocking.** A stream reads one log (one replication slot on PostgreSQL), so one very busy table can delay changes for every other table in the stream. Split high-churn tables into their own stream.

**Limits worth knowing.** Events are limited to 20 MB; BigQuery allows four clustering columns, so for keys of more than four columns Datastream clusters on four of them; primary keys of unsupported types (for example `FLOAT`) prevent replication of that table. Generated and range-type columns on PostgreSQL are not supported.

**Datastream to Cloud Storage.** Writing Avro or JSON files to Cloud Storage lets other tools consume changes: the Google-provided **Datastream to BigQuery** or **Datastream to Spanner** Dataflow templates, Spark jobs merging into Iceberg tables, or anything that reads files. Use this path when you need transformations in flight or a lakehouse destination.

**Compared with Debezium.** Debezium on Kafka Connect gives you the change stream as Kafka topics that many consumers can share, more source options and full control, at the cost of running Connect and Kafka. Datastream is the choice when the main destination is BigQuery and nobody wants to operate connectors.

> **Interview:** "Replicate a 3 TB PostgreSQL orders database into BigQuery with 15-minute freshness and no impact on the primary." Use Datastream from a read replica (PostgreSQL 16 or later supports logical decoding from replicas) or the primary with a dedicated publication; private connectivity; merge mode with 15-minute freshness; separate streams for the hottest tables; alerts on slot lag and stream errors; and partitioning and clustering on the BigQuery tables for query cost.
:::

::: check
?? What are the two phases of a Datastream stream, and why must they overlap?
Backfill (a snapshot of existing rows) and CDC (changes from the log). CDC starts before the backfill finishes so that changes made during the snapshot are captured and applied in source-time order.

?? When would you choose append-only mode over merge mode?
When you need the full change history (audit, slowly changing dimensions, reprocessing), or when tables have no primary key. Merge mode keeps only the current state.

?? Why is an abandoned PostgreSQL replication slot dangerous?
PostgreSQL must keep all WAL the slot has not confirmed, so WAL accumulates until the disk fills and the database stops.
:::

::: resources
- Docs: [Datastream overview](https://docs.cloud.google.com/datastream/docs/overview) · sources, destinations, streams and connectivity
- Docs: [Datastream key concepts and features](https://docs.cloud.google.com/datastream/docs/behavior-overview) · backfill, CDC, ordering and event structure
- Docs: [BigQuery destination](https://docs.cloud.google.com/datastream/docs/destination-bigquery) · write modes, freshness, metadata columns and limits
- Docs: [PostgreSQL as a source](https://docs.cloud.google.com/datastream/docs/sources-postgresql) · logical decoding, publications, slots and WAL risks
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; chapter 11 explains change data capture and log-based integration
- Docs: [Debezium documentation](https://debezium.io/documentation/) · the open-source alternative, useful for understanding what Datastream automates
:::

### Managed orchestration: Managed Service for Apache Airflow (formerly Cloud Composer) and Astronomer

::: plain
Airflow is the conductor of a data orchestra: it tells each player when to start, notices when one stops, and restarts the piece from the right bar. But a conductor needs a concert hall, chairs, lighting and staff. Managed Airflow services rent you the whole hall: Google's Managed Service for Apache Airflow (formerly Cloud Composer) or Astronomer's Astro run the building, so your team only writes the music, which in Airflow is DAG files.
:::

**What "managed Airflow" takes off your hands.** An Airflow installation has a scheduler, a DAG processor, a triggerer for deferrable tasks, workers, a web server or API server, a metadata database, and storage for DAGs and logs (Orchestration covers what each one does). Running them yourself means upgrades, database backups, scaling, security patches and on-call. A managed service runs those components, and you supply DAGs, Python packages, connections and sizing.

**Managed Service for Apache Airflow (Gen 3).** The current generation (formerly Composer 3) supports Airflow 2 and Airflow 3 images, named like `composer-3-airflow-x.y.z-build.t`. Each **environment** has:

- an **environment bucket** in Cloud Storage with `dags/`, `plugins/`, `data/` and `logs/` folders; DAG files you copy into `dags/` are picked up automatically;
- Airflow components run by Google as containers, with **workers that autoscale** between a minimum and maximum count;
- a managed Airflow database, and an **environment size** preset that sizes the database and other infrastructure;
- integration with IAM (the environment's service account runs tasks), Secret Manager as a secrets backend, private networking, CMEK, Cloud Logging and Cloud Monitoring, and data lineage in Knowledge Catalog.

```bash
gcloud composer environments create orders-prod \
  --location=europe-west1 \
  --image-version="$COMPOSER_IMAGE" \
  --environment-size=small \
  --service-account=airflow-prod@acme-prod.iam.gserviceaccount.com \
  --min-workers=2 --max-workers=12 --worker-cpu=2 --worker-memory=7.5GB

# Deploy DAGs and install a provider (from CI, not by hand)
gcloud composer environments storage dags import \
  --environment=orders-prod --location=europe-west1 --source=dags/
gcloud composer environments update orders-prod --location=europe-west1 \
  --update-pypi-package="apache-airflow-providers-dbt-cloud"
```

`$COMPOSER_IMAGE` is a current `composer-3-airflow-...` image from the version list; Gen 3 builds are supported for 12 months after release, so plan an upgrade at least once a year.

**Sizing workers.** The number of tasks that can run at once is workers times concurrency per worker, capped by Airflow's `parallelism` and pool sizes. A quick calculation that runs as is:

```python
import math

peak_concurrent_tasks = 300   # e.g. the 2 a.m. fan-out of ingestion tasks
worker_concurrency = 12       # task slots per worker ([celery] worker_concurrency)
max_workers = math.ceil(peak_concurrent_tasks / worker_concurrency)
print(max_workers)            # 25
```

Most data-platform tasks only call other services (submit a BigQuery job, start a Dataflow template) and wait, so they use little CPU. Make them **deferrable** (they hand the waiting to the triggerer and free the worker slot), and the same work needs far fewer workers.

**Scaling limits you will meet.** The scheduler and DAG processor re-parse every DAG file regularly, so heavy top-level code (API calls, big imports, database queries outside tasks) slows the whole environment; keep top-level code trivial. Thousands of DAG files or very large dynamic DAGs lengthen parse loops. The metadata database grows with every task instance; clean old records on a schedule (`airflow db clean` through a maintenance DAG). Very large platforms split into several environments by domain, which also limits the blast radius of a bad package or upgrade.

**Astronomer (Astro).** Astronomer is the company behind much of Airflow's development, and **Astro** is its managed Airflow platform, available on Google Cloud as well as other clouds. Its strengths are developer workflow and Airflow expertise: the **Astro CLI** runs a local Airflow in containers (`astro dev start`) and deploys images or DAG-only updates (`astro deploy`); deployments choose executors, worker queues and sizes; new Airflow releases arrive quickly; and observability features track DAG and data health. With Airflow 3's remote execution, tasks can run in your own Google Cloud project while Astronomer runs the control plane.

**Self-managed Airflow on GKE** with the official Helm chart (Cloud SQL for the database, the KubernetesExecutor or CeleryExecutor) gives full control of versions and configuration, at the price of owning everything.

| | Managed Airflow (Gen 3) | Astronomer Astro | Self-managed on GKE |
|---|---|---|---|
| Who runs components | Google | Astronomer | You |
| Integration | Native IAM, VPC Service Controls, CMEK, Google billing | Connects to Google Cloud with service accounts and private networking | Whatever you build |
| Airflow versions | Google's images, after validation | Usually earliest support for new releases | Any version, any day |
| Developer experience | Console, gcloud, local development tooling | Astro CLI, deploy workflow, observability | Your own |
| Cost shape | Environment runs continuously, workers scale | Per deployment and usage, plus vendor contract | GKE nodes, Cloud SQL, people's time |
| Choose when | Google-centric platform, procurement through Google | Airflow-heavy teams wanting the best Airflow tooling or multi-cloud | Special requirements a managed service cannot meet |

> **Pitfall:** Treating the environment bucket as an IDE. Editing DAGs in place in production means no review, no tests and no record of what ran. Deploy DAGs only from Git through CI (DataOps and platform engineering shows the pipeline), and give humans read-only access to production buckets.

> **Pitfall:** Doing the heavy work inside Airflow workers (pandas on gigabytes, large file downloads). Workers are sized for orchestration; heavy tasks starve others and crash workers. Push the work to BigQuery, Dataflow, Spark or a Cloud Run job, and let Airflow submit and wait.

::: deeper
**Gen 3 architecture.** In the current generation, the Airflow components run in Google-managed infrastructure rather than in a GKE cluster in your project, and the environment connects to your VPC for private access to your resources. That reduces what you see and tune, and it removes a class of operational problems (node pools, cluster upgrades). **Highly resilient** environments spread components across zones and use a primary and standby database for zonal failures.

**Upgrades and disaster recovery.** Environment **snapshots** save DAGs, configuration and the Airflow database state, and can be loaded into a new environment, which is how you test an upgrade, move regions, or recover from a broken environment. For Airflow 2 to 3 upgrades, create a new Airflow 3 environment, run DAGs in parallel on a staging copy, then switch; DAG code changes are needed (see the Airflow 3 notes in Orchestration).

**Cost shape.** An environment bills for its components' compute, the database and storage while it exists, plus worker compute as it scales. That creates a floor cost even for a quiet environment: many small teams each with an environment pay several floors. Share environments by domain, delete development environments out of hours, or use Workflows for lightweight cases.

**Versions to watch.** Gen 2 versions 2.0.x and the legacy Gen 1 reach end of life in September 2026; anything still on them must move to Gen 3.

> **Interview:** "Your Airflow scheduler is slow: tasks sit in `scheduled` for minutes. What do you check?" DAG parse time and top-level code, number of DAG files, scheduler CPU and memory, database size and latency, pool and parallelism limits, and whether worker slots are full of sensors (convert them to deferrable operators). Then scale the right component, not just workers.
:::

::: check
?? Peak load is 180 concurrent tasks and each worker runs 12. How many workers do you need, and what change could cut that number sharply?
15 workers. Making waiting tasks deferrable (or reschedule-mode sensors) frees worker slots while they wait, so far fewer workers are needed.

?? Why should DAGs reach production only through CI?
So every change is reviewed, tested (import errors, DAG integrity) and versioned, and production always matches Git instead of manual edits in the bucket.

?? Name one reason to choose Astronomer over Google's managed Airflow, and one for the reverse.
Astronomer: faster access to new Airflow versions and strong developer tooling, or the same platform on several clouds. Google's service: native IAM, VPC Service Controls and CMEK integration with billing and support through Google.
:::

::: resources
- Docs: [Managed Airflow (Gen 3) overview](https://docs.cloud.google.com/composer/docs/composer-3/composer-overview) · architecture and what Google manages
- Docs: [Managed Airflow versioning overview](https://docs.cloud.google.com/composer/docs/composer-versioning-overview) · generations, image versions and support periods
- Docs: [Astro documentation](https://www.astronomer.io/docs/astro/) · Astronomer's managed Airflow, the Astro CLI and deployments
- Docs: [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html) · the scheduler, executors and best practices that apply on every platform
- Docs: [Helm chart for Apache Airflow](https://airflow.apache.org/docs/helm-chart/stable/index.html) · the official way to self-manage Airflow on Kubernetes
- Book: [Data Pipelines with Apache Airflow, 2nd edition](https://www.manning.com/books/data-pipelines-with-apache-airflow-second-edition) · Harenslak and de Ruiter; DAG design, testing and deployment
:::

### Workflows, Cloud Scheduler and Eventarc

::: plain
Not every job needs a conductor. Sometimes you need an alarm clock (**Cloud Scheduler**: "at 2 a.m., press this button"), a doorbell (**Eventarc**: "when a parcel arrives, ring the person who handles parcels"), or a written checklist that someone follows step by step, retrying a step that fails (**Workflows**). Together they cover most small automation without running an Airflow environment.
:::

**Cloud Scheduler** is managed cron. A job has a cron expression, a time zone and a target: an HTTP endpoint (with an OIDC or OAuth token from a service account), a Pub/Sub topic, or an App Engine route. It retries failed calls according to a retry configuration. It is priced per job per month, with a few free jobs per billing account, so it costs almost nothing.

**Eventarc** routes events to handlers. A **trigger** filters events by type and attributes and delivers them in the **CloudEvents** format to Cloud Run services, Cloud Run functions, Workflows, GKE services or internal HTTP endpoints. Sources include Cloud Storage object events, Pub/Sub messages, Cloud Audit Logs entries (for example "a BigQuery job completed") and third-party providers. Delivery is at least once, with retries for up to 24 hours and an optional Pub/Sub dead-letter topic. **Eventarc Advanced** adds a central message bus with enrollments and transformations for larger event-driven systems.

**Workflows** runs a sequence of steps written in YAML (or JSON): call an HTTP endpoint or a Google Cloud API through a **connector**, assign variables, branch with `switch`, loop with `for`, run branches in `parallel`, catch errors with `try` and `except`, and **retry** with backoff. Connectors handle authentication and wait for long-running operations (a BigQuery job, a Dataflow template) to finish. An execution can last up to a year and can pause on a **callback** until an external system calls back. Billing is per step executed (internal steps cheaper than external HTTP calls), with a free monthly allowance.

[[fig:gcppipes_events]]

A file-driven pipeline: when a partner drops a Parquet file, load it into BigQuery and announce it on Pub/Sub.

```yaml
# load_partner_orders.yaml
main:
  params: [event]
  steps:
    - init:
        assign:
          - project: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
          - bucket: ${event.data.bucket}
          - object: ${event.data.name}
    - only_parquet:
        switch:
          - condition: ${not text.match_regex(object, "\\.parquet$")}
            return: "ignored"
    - load:
        try:
          call: googleapis.bigquery.v2.jobs.insert     # waits for the job to finish
          args:
            projectId: ${project}
            body:
              configuration:
                load:
                  sourceUris:
                    - ${"gs://" + bucket + "/" + object}
                  sourceFormat: PARQUET
                  writeDisposition: WRITE_APPEND
                  destinationTable:
                    projectId: ${project}
                    datasetId: raw
                    tableId: partner_orders
          result: load_job
        retry: ${http.default_retry}
    - build_message:
        assign:
          - msg:
              object: ${object}
              rows: ${load_job.statistics.load.outputRows}
    - announce:
        call: googleapis.pubsub.v1.projects.topics.publish
        args:
          topic: ${"projects/" + project + "/topics/partner-orders-loaded"}
          body:
            messages:
              - data: ${base64.encode(json.encode(msg))}
    - done:
        return: ${load_job.jobReference.jobId}
```

```bash
gcloud workflows deploy load-partner-orders --location=europe-west1 \
  --source=load_partner_orders.yaml \
  --service-account=wf-loader@acme-prod.iam.gserviceaccount.com

gcloud eventarc triggers create partner-file-landed --location=europe-west1 \
  --destination-workflow=load-partner-orders \
  --destination-workflow-location=europe-west1 \
  --event-filters="type=google.cloud.storage.object.v1.finalized" \
  --event-filters="bucket=acme-partner-landing" \
  --service-account=wf-trigger@acme-prod.iam.gserviceaccount.com

# A nightly workflow on a schedule instead of an event
gcloud scheduler jobs create http finance-nightly --location=europe-west1 \
  --schedule="15 2 * * *" --time-zone="Europe/Berlin" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/acme-prod/locations/europe-west1/workflows/finance-nightly/executions" \
  --http-method=POST \
  --oauth-service-account-email=scheduler@acme-prod.iam.gserviceaccount.com
```

**Choosing between them and Airflow.**

| Need | Use |
|---|---|
| Run one job on a timetable | Cloud Scheduler calling the job (Cloud Run job, workflow, HTTP endpoint) |
| React to a file, message or audit event | Eventarc trigger to Cloud Run or Workflows |
| Chain a few API calls with retries and branches | Workflows |
| Many interdependent pipelines, backfills, data intervals, run history | Managed Airflow (or another data orchestrator) |
| Wait hours or days for a human approval or partner callback | Workflows with a callback |

> **Pitfall:** Assuming each event arrives exactly once. Eventarc and Cloud Storage notifications are at least once, so the same file can start two executions. Make handlers idempotent: check whether the object (bucket, name and **generation**) was already loaded, or load into a staging table and `MERGE`.

> **Pitfall:** Passing data through Workflows variables. Workflows has small limits on variable and argument sizes; pass references (URIs, table names, job IDs) and let BigQuery, Dataflow or Cloud Run move the data.

::: deeper
**Eventarc and Cloud Storage details.** The trigger's location must match the bucket's location (a multi-region bucket needs a multi-region trigger). Direct Cloud Storage events use Pub/Sub under the hood, so the Cloud Storage service agent needs permission to publish. Audit-log triggers depend on Data Access audit logs being enabled for that service, and they arrive with more delay than direct events.

**Workflows execution model.** Each step's result is checkpointed, so a long wait (a connector polling a long BigQuery job, a callback, `sys.sleep`) costs nothing while waiting. `parallel` branches share variables declared with `shared`, and parallel `for` loops have concurrency limits. Executions are independent: there is no built-in concurrency control across executions, so if two must not overlap, guard with a lock (for example a Firestore document or a BigQuery table state) or use Airflow.

**Cloud Scheduler retries and time zones.** A job has a retry count, minimum and maximum backoff, and an attempt deadline. Cron expressions are evaluated in the job's time zone, so daylight-saving changes can skip or repeat a run at 02:30 local time; schedule critical jobs outside the changeover hour or in UTC.

**Workflows versus Airflow, the real line.** Workflows has no notion of a data interval: it knows "run now with these arguments", not "build the partition for 2026-11-22 and backfill last month". When a pipeline needs catchup, backfills and a shared view of which partitions are built, that belongs in a data orchestrator.

> **Interview:** "Partners drop files at random times; each must be loaded within 10 minutes, and duplicates are unacceptable." Eventarc on object finalize to Workflows (or a Cloud Run function); load into a staging table keyed by file name and generation; `MERGE` into the target; record processed files in a control table to make retries idempotent; dead-letter failed events and alert; Cloud Scheduler runs a daily reconciliation that lists the bucket and reloads anything missed.
:::

::: check
?? A team needs to run one Cloud Run job every night at 3 a.m. What is the simplest setup?
A Cloud Scheduler job that calls the Cloud Run job's run endpoint on the schedule (or the Cloud Run job's own scheduler trigger), with alerting on failure. No orchestrator is needed.

?? Why can one file upload start two workflow executions, and how do you make that harmless?
Event delivery is at least once. Make the handler idempotent: track processed objects by name and generation, or load into staging and `MERGE`.

?? Which feature makes Workflows suitable for waiting on a human approval?
Callbacks: the workflow creates a callback endpoint, sends it out, and pauses (without cost) until something calls it or a timeout fires.
:::

::: resources
- Docs: [Workflows overview](https://docs.cloud.google.com/workflows/docs/overview) · steps, connectors, callbacks and use cases
- Docs: [Workflows syntax overview](https://docs.cloud.google.com/workflows/docs/reference/syntax) · steps, conditions, loops, parallel branches, retries and error handling
- Docs: [Cloud Scheduler overview](https://docs.cloud.google.com/scheduler/docs/overview) · targets, authentication and retries
- Docs: [Eventarc overview](https://docs.cloud.google.com/eventarc/docs/overview) · Standard and Advanced, sources, destinations and delivery guarantees
- Spec: [CloudEvents specification](https://cloudevents.io/) · the event format Eventarc delivers
- Book: [Data Pipelines Pocket Reference](https://www.oreilly.com/library/view/data-pipelines-pocket/9781492087823/) · Densmore; orchestration patterns and idempotent loading in a compact form
:::

### Serverless data processing on GCP: Cloud Run, Cloud Run functions and Batch

::: plain
Some jobs are too small for a factory but too big to do by hand: call an API every hour, convert a few files when they arrive, run a model over 200 images tonight. Serverless services are like hiring help by the hour: you describe the task, workers appear when there is work, and you stop paying when they leave. Google Cloud offers three kinds of help, for requests, for finished-when-done jobs, and for heavy batches on full machines.
:::

**Cloud Run services** run a container that answers HTTP requests (or gRPC), scaling from zero to many instances. In data engineering they are the natural target for **Pub/Sub push subscriptions** and Eventarc triggers, for small APIs that serve data, and for webhooks that receive SaaS events. Each instance handles several requests at once (concurrency, 80 by default), and requests can run up to 60 minutes.

**Cloud Run jobs** run a container to completion instead of serving requests: an ingestion connector, an export, a dbt Core run. A job execution runs up to 10,000 **tasks** in parallel (with a parallelism limit you set); each task can run up to 168 hours (12 hours with GPUs) and is retried up to 3 times by default. Every task receives `CLOUD_RUN_TASK_INDEX` and `CLOUD_RUN_TASK_COUNT`, so splitting work is simple. This runs as is and simulates three tasks:

```python
import os

files = [f"page-{i:04d}.json" for i in range(10)]

def my_share(files, index, count):
    return files[index::count]          # round-robin split, no coordination needed

index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))   # what one real task would use
count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))

for i in range(3):                      # simulate CLOUD_RUN_TASK_COUNT=3
    print(i, my_share(files, i, 3))
# 0 ['page-0000.json', 'page-0003.json', 'page-0006.json', 'page-0009.json']
# 1 ['page-0001.json', 'page-0004.json', 'page-0007.json']
# 2 ['page-0002.json', 'page-0005.json', 'page-0008.json']
```

```bash
gcloud run jobs deploy stripe-export \
  --image=europe-west1-docker.pkg.dev/acme-prod/jobs/stripe-export:2.3.0 \
  --region=europe-west1 --tasks=20 --parallelism=5 --max-retries=2 \
  --task-timeout=2h --set-secrets=STRIPE_KEY=stripe-api-key:latest \
  --service-account=ingest@acme-prod.iam.gserviceaccount.com
gcloud run jobs execute stripe-export --region=europe-west1
```

**Cloud Run functions** (formerly Cloud Functions) deploy a single function from source: Google builds the container and runs it on Cloud Run. They suit small event handlers, such as "a CSV landed, start a load job":

```python
import functions_framework
from google.cloud import bigquery

client = bigquery.Client()

@functions_framework.cloud_event
def load_file(cloud_event):
    data = cloud_event.data
    if not data["name"].endswith(".csv"):
        return
    uri = f"gs://{data['bucket']}/{data['name']}"
    config = bigquery.LoadJobConfig(source_format="CSV", skip_leading_rows=1)
    job = client.load_table_from_uri(uri, "acme-prod.raw.partner_orders", job_config=config)
    print(f"started {job.job_id} for {uri}")   # do not wait: the load runs in BigQuery
```

```bash
gcloud functions deploy load-partner-file --gen2 --region=europe-west1 \
  --runtime=python313 --source=. --entry-point=load_file \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=acme-partner-landing" \
  --service-account=loader@acme-prod.iam.gserviceaccount.com
```

**Batch** runs jobs on Compute Engine VMs that it creates and deletes for you: containers or scripts, thousands of tasks, any machine type, GPUs, Spot VMs, long run times. There is no charge for Batch itself; you pay for the VMs and disks. It suits heavy, embarrassingly parallel work such as image processing, simulation, geospatial enrichment or model scoring that needs specific hardware.

```json
{
  "taskGroups": [{
    "taskCount": 200,
    "parallelism": 50,
    "taskSpec": {
      "runnables": [{"container": {
        "imageUri": "europe-west1-docker.pkg.dev/acme-prod/jobs/geo-enrich:1.4",
        "commands": ["--date=2026-11-22"]}}],
      "computeResource": {"cpuMilli": 4000, "memoryMib": 16000},
      "maxRetryCount": 2,
      "maxRunDuration": "3600s"
    }
  }],
  "allocationPolicy": {"instances": [{"policy": {
    "machineType": "n2-standard-4", "provisioningModel": "SPOT"}}]},
  "logsPolicy": {"destination": "CLOUD_LOGGING"}
}
```

Submit it with `gcloud batch jobs submit geo-enrich-20261122 --location=europe-west1 --config=job.json`; each task reads `BATCH_TASK_INDEX` to pick its share.

**Choosing.**

| Workload | Best fit | Why |
|---|---|---|
| React to each message or file, seconds of work | Cloud Run functions or a Cloud Run service | Event triggers, scale to zero, billed per use |
| Scheduled container that runs and exits (API ingestion, exports, dbt Core) | Cloud Run jobs | Tasks, retries, secrets, no servers |
| Hundreds of heavy tasks, GPUs or special machines | Batch | Full VMs, Spot, long tasks, no service fee |
| Continuous streams, windows, joins, exactly-once | Dataflow | Built for unbounded data |
| Large distributed transforms over files or tables | BigQuery SQL, Spark or Dataflow | Distributed shuffle, not one container per task |

**Cost model.** Cloud Run bills vCPU-seconds and GiB-seconds while instances run (with request-based billing for services, CPU is charged only while handling requests) plus requests, with a monthly free tier. Batch bills VMs per second. That makes serverless cheap for spiky, small work and expensive for constant heavy load: a service busy 24 hours a day with minimum instances costs about as much as a VM, and Dataflow or Spark is cheaper per byte for large transforms because they shuffle efficiently.

> **Pitfall:** Building a distributed system out of functions: one function per file that calls another per row, fan-out through Pub/Sub, results joined in a database. It works at small scale and becomes hard to debug, retry and pay for. When the work needs shuffles, joins or ordering across many records, use Dataflow, Spark or BigQuery.

> **Pitfall:** Waiting inside a function for a long job. A function that blocks until a 20-minute load finishes pays for the wait and may hit its timeout (event-driven functions are limited to minutes). Start the job and return; let Eventarc (for example on the BigQuery job-completed audit event) or Workflows handle the next step.

::: deeper
**Cold starts and concurrency.** A new instance must start the container and initialise your code, which adds latency to the first requests. Set minimum instances for latency-sensitive services, keep start-up light (lazy-load large libraries), and create clients (BigQuery, Pub/Sub) once per instance, outside the handler. With concurrency above 1, code must be thread-safe.

**Push subscription throughput.** Pub/Sub adapts its push rate to your service: successful responses increase it and errors or slow responses back off. Return a 2xx only after the work is durable, and a 4xx or 5xx to trigger a retry. For very high throughput, a pull subscriber (a Cloud Run worker or GKE) gives more control than push.

**Networking and secrets.** Direct VPC egress lets Cloud Run reach private databases; secrets from Secret Manager mount as environment variables or files. Batch VMs follow your VPC settings and can run without external IPs.

**Idempotency again.** Cloud Run jobs retry failed tasks, Eventarc redelivers events and Batch retries tasks. Every task should be safe to run twice: write to a temporary location and rename, use deterministic output paths per input, or `MERGE` into targets.

> **Interview:** "Ingest 30 SaaS APIs hourly, each taking 1 to 15 minutes." Package each connector as a container, run them as Cloud Run jobs (one task per API or per account shard), store cursors in a control table or Firestore, secrets in Secret Manager, raw pages in Cloud Storage, then load into BigQuery. Schedule and chain them with Managed Airflow if dependencies and backfills matter, or Cloud Scheduler and Workflows if they are independent.
:::

::: check
?? A Cloud Run job runs with 4 tasks over 10 files using the round-robin split. Which files does task 1 process?
Files at positions 1, 5 and 9 (`files[1::4]`).

?? Why is Batch a better fit than Cloud Run jobs for 300 GPU-hours of image scoring?
Batch provisions full Compute Engine VMs of any type, including GPU and Spot machines, for long tasks, and charges only for those VMs; Cloud Run is built for container tasks with more limited hardware choices and run times.

?? A function waits 20 minutes for a BigQuery load to finish. What is wrong and what is the fix?
It pays for idle waiting and can hit the function timeout. Start the job and return, then react to completion with Eventarc (audit log event) or let Workflows wait on the job.
:::

::: resources
- Docs: [What is Cloud Run](https://docs.cloud.google.com/run/docs/overview/what-is-cloud-run) · services, jobs and functions on one platform
- Docs: [Create jobs](https://docs.cloud.google.com/run/docs/create-jobs) · tasks, parallelism, retries and timeouts for Cloud Run jobs
- Docs: [Cloud Run functions overview](https://docs.cloud.google.com/run/docs/functions/overview) · writing and deploying functions with event triggers
- Docs: [Batch overview](https://docs.cloud.google.com/batch/docs/get-started) · jobs, tasks, VMs, Spot provisioning and pricing
- Docs: [Cloud Run pricing](https://cloud.google.com/run/pricing) · instance-based and request-based billing and the free tier
- Book: [Building Serverless Applications on Google Cloud Run](https://www.oreilly.com/library/view/building-serverless-applications/9781492057086/) · Wietse Venema; containers, scaling and patterns on Cloud Run
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, setting or rule of thumb |
|---|---|
| Fan-out to several consumers | One topic, one subscription per consumer |
| Per-entity ordering in Pub/Sub | Publisher `ordering_key=<entity id>` plus `--enable-message-ordering` |
| No redelivery after a successful ack | `--enable-exactly-once-delivery` (pull only, same region); still dedupe on event ID |
| Park poison messages | `--dead-letter-topic=... --max-delivery-attempts=5..100` plus backoff retry policy |
| Events into BigQuery with no code | `--bigquery-table=proj:ds.table --use-table-schema --write-metadata` |
| Size Google Managed Service for Apache Kafka | vCPUs = (writes + reads / 4) / 20 MBps / 0.5; 4 GiB memory per vCPU |
| Pub/Sub or Kafka | Pub/Sub unless you need the Kafka API, offsets, Connect or a multi-cloud backbone |
| Deploy a fix to a streaming Dataflow job | `--update` with the same job name; drain if incompatible; never cancel |
| Package a Dataflow pipeline | Flex template: image in Artifact Registry, spec in Cloud Storage |
| Dataflow lag rising, CPU low | Hot key, slow external call or sink quota, not capacity |
| Cheap Spark clusters | Ephemeral, fixed primaries, Spot secondaries, `--max-idle` |
| Spark without clusters | `gcloud dataproc batches submit pyspark ... --version=<runtime>` |
| Spark reads and writes BigQuery | `format("bigquery")`, `writeMethod=direct` for the Storage Write API |
| Incremental model without duplicates | Dataform `type: "incremental"` with `uniqueKey` |
| Many standard feeds, non-engineers | Cloud Data Fusion; otherwise code-first tools |
| Database replica in BigQuery | Datastream, merge mode, freshness via `max_staleness` |
| PostgreSQL CDC hygiene | Dedicated publication, alert on retained WAL, drop unused slots |
| Managed Airflow worker count | Peak concurrent tasks / worker concurrency; make waits deferrable |
| React to a file landing | Eventarc trigger on `google.cloud.storage.object.v1.finalized`, idempotent handler |
| Run a container to completion | Cloud Run jobs (tasks, retries, `CLOUD_RUN_TASK_INDEX`); Batch for heavy VMs and GPUs |

### Practice

::: try
**1. Choose the transport and subscriptions.** An e-commerce company on Google Cloud publishes order events (about 3,000 per second at peak). Three consumers need them: a fraud service that must see each order's events in order, a raw events table in BigQuery, and hourly Avro files in Cloud Storage for the data lake. Nobody uses Kafka today. Choose the transport and describe each subscription's settings.
:::

?? A solution to 1
Pub/Sub, since everything is on Google Cloud and no consumer needs the Kafka API. Producers publish with `ordering_key` set to the order ID and an `event_id` attribute. The fraud service uses a pull subscription with message ordering, exactly-once delivery (consumers in the same region), a dead-letter topic with 5 to 10 attempts and exponential backoff, and an idempotent write on `event_id`. The raw table uses a BigQuery subscription with `--use-table-schema --write-metadata` and a dead-letter topic; a downstream model deduplicates on `event_id`. The lake uses a Cloud Storage subscription with Avro output and a maximum duration that produces files of reasonable size. Alert on `oldest_unacked_message_age` for each subscription.

::: try
**2. Size and choose Spark.** A nightly PySpark job reads 2 TB from BigQuery, aggregates it and writes 50 GB back. It runs once a night for about 40 minutes. Today it runs on a 20-node cluster that is never deleted. Propose a cheaper setup, with the commands or settings that matter.
:::

?? A solution to 2
Run it as a serverless batch (`gcloud dataproc batches submit pyspark ... --version=<runtime>`) with a dynamic allocation limit, or on an ephemeral cluster created by the orchestrator with fixed primaries, Spot secondary workers in an autoscaling policy, and `--max-idle=30m` as a safety net. Read with the Spark BigQuery connector using column projection and a pushed-down filter, write with `writeMethod=direct`. Either way you pay for about 40 minutes a night instead of 24 hours. If the logic is just joins and aggregations, consider moving it into BigQuery SQL with Dataform.

::: try
**3. Debug a replica.** A Datastream stream from PostgreSQL to BigQuery was paused for a week during a migration. The database team now reports that the primary's disk is 92% full and growing. Explain the cause, the immediate fix, and how to prevent it.
:::

?? A solution to 3
The stream's logical replication slot stayed in place while the stream was paused, so PostgreSQL retained all WAL since then. Immediate fix: resume the stream so it consumes and confirms the WAL, or, if the stream is no longer needed, drop the slot (`SELECT pg_drop_replication_slot('datastream_slot');`) and later re-create the stream with a fresh backfill. Prevention: alert on retained WAL per slot (`pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`), include slot cleanup in runbooks for pausing or deleting streams, and keep publications limited to the needed tables.

::: try
**4. Pick the orchestration.** For each case choose Cloud Scheduler, Eventarc, Workflows or Managed Airflow, and say why: (a) one Cloud Run job exporting a table every night; (b) partner files arriving at random times that must be loaded within minutes; (c) 70 interdependent pipelines with daily partitions and occasional month-long backfills; (d) a monthly close process that waits for a finance manager's approval before publishing.
:::

?? A solution to 4
(a) Cloud Scheduler calling the job: one timed call, nothing to orchestrate. (b) Eventarc on object finalize to Workflows or a Cloud Run function, with an idempotent load keyed on name and generation. (c) Managed Airflow (or another data orchestrator): dependencies, data intervals, catchup and backfills are exactly what it is for. (d) Workflows with a callback: it can pause for days at no cost until the approval arrives, then continue.

::: try
**5. Split the work.** A Cloud Run job must export 1,000 customer accounts from an API. Each account takes about 30 seconds. You want the export done in under 10 minutes. How many tasks and what parallelism do you set, and how does each task find its accounts?
:::

?? A solution to 5
1,000 accounts at 30 seconds is 30,000 task-seconds. To finish in about 10 minutes (600 seconds) you need at least 50 tasks running at once: set `--tasks=50 --parallelism=50` (or more tasks with the same parallelism for finer retries, for example `--tasks=100 --parallelism=50`, each doing 10 accounts in about 5 minutes). Each task takes `accounts[CLOUD_RUN_TASK_INDEX::CLOUD_RUN_TASK_COUNT]`. Check the API's rate limit first: 50 parallel callers may need throttling.

### What comes next

You can now move data through Google Cloud end to end: events through Pub/Sub or Kafka, streams and batches through Dataflow and Spark, SQL through Dataform, database changes through Datastream, and all of it scheduled and triggered by Airflow, Workflows, Cloud Scheduler and Eventarc. The next section, **Operational databases, AI and governance on Google Cloud**, covers what sits on either side of these pipelines: the operational databases they read from and serve (Cloud SQL, AlloyDB, Spanner, Bigtable, Firestore), the AI platform that consumes curated data, and the catalog, lineage and quality tools that keep the whole platform trustworthy.
