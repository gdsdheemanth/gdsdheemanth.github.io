This section covers what sits on either side of the pipelines you built in the last section: the operational databases they read from and serve, the catalog and sharing services that keep the platform trustworthy and open to partners, and the AI platform that consumes curated data. It is written for data engineers who must choose a database for an application team, replicate it into BigQuery, publish governed data to other teams and companies, and feed machine learning systems. It assumes Google Cloud foundations for data (projects, IAM, VPC Service Controls, CMEK), BigQuery in depth (storage, slots, security, BigQuery sharing basics) and Pipelines on Google Cloud (Datastream, Dataflow, Managed Airflow). It also builds on the general theory from earlier sections: B-trees, LSM trees, MVCC and distributed SQL from [Databases: OLTP, NoSQL and specialised](#databases), and catalogs, lineage and clean rooms from [Data governance, security and compliance](#governance), which comes later in the course but can be read in either order.

**What you will be able to do afterwards**

- Design Bigtable row keys that spread load, choose replication and app profiles, and explain when Bigtable beats BigQuery and when it does not.
- Explain how Spanner splits and replicates data, design keys and interleaved tables that avoid hotspots, and stream its changes to BigQuery.
- Choose between Cloud SQL, AlloyDB and Spanner for a relational workload, and get their data into BigQuery with federated queries or CDC.
- Run Knowledge Catalog (formerly Dataplex Universal Catalog) as a platform service: discovery, profiling, quality scans, lineage and business metadata.
- Publish data with BigQuery sharing (formerly Analytics Hub) and data clean rooms, and with open protocols such as Delta Sharing when the recipient is not on BigQuery.
- Supply Gemini Enterprise Agent Platform (formerly Vertex AI) with point-in-time-correct features, pipelines and batch predictions, and assemble all of it into Google Cloud's standard reference architectures.

**The big picture.** A data platform on Google Cloud has a warehouse in the middle (BigQuery) and pipelines around it. This section adds three rings. The **operational ring** is where applications live: Cloud SQL and AlloyDB for ordinary relational apps, Spanner for relational data that outgrows one machine or one region, Bigtable for huge key-value and time-series workloads, and Firestore for mobile and web documents. Data flows out of these stores into BigQuery through Datastream or change streams, and curated results flow back into them for low-latency serving. The **trust ring** is Knowledge Catalog, which knows what every asset is, where it came from and whether it is healthy, plus BigQuery sharing, clean rooms and open protocols that let other teams and companies use data without copies. The **AI ring** is Gemini Enterprise Agent Platform, which trains and serves models on features the data team produces. The last topic puts the rings together into reference architectures.

> **Note:** This section uses the current Google Cloud names and gives the former name once: Knowledge Catalog (formerly Dataplex Universal Catalog), BigQuery sharing (formerly Analytics Hub), Gemini Enterprise Agent Platform (formerly Vertex AI), Google Cloud Lakehouse (formerly BigLake) and Managed Service for Apache Airflow (formerly Cloud Composer). API names, `gcloud` groups and IAM roles kept the old names, so you still type `gcloud dataplex` and grant `roles/analyticshub.subscriber`.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Databases

Pipelines start and end in operational databases. Orders are written to Cloud SQL or AlloyDB, payments to Spanner, sensor readings and user profiles to Bigtable; Datastream and change streams copy those writes into BigQuery, and reverse ETL copies scores and aggregates back for applications to read in milliseconds. A data engineer is rarely the owner of these databases, but is often asked to choose one, to design the keys of a serving table, or to explain why the replica in BigQuery is late.

The general theory (B-trees and LSM trees, MVCC, Raft and Paxos, TrueTime) is in [Databases: OLTP, NoSQL and specialised](#databases). This group is about the Google Cloud services themselves: how they lay data out, where they break, and how they connect to the analytical side. A quick map before the details:

| Need | Service | Data model | Scale limit you will hit first |
|---|---|---|---|
| Ordinary app database, existing PostgreSQL, MySQL or SQL Server | Cloud SQL | Relational | One primary machine for writes |
| PostgreSQL with heavier reads and some analytics | AlloyDB | Relational (PostgreSQL) | One primary for writes, many read nodes |
| Relational, horizontally scalable, multi-region, strongly consistent | Spanner | Relational, graph, key-value | Practically none; cost and key design |
| Very high write rates, time series, key lookups at millisecond latency | Bigtable | Wide-column, sorted by row key | Row key design |
| Mobile and web app documents with offline sync | Firestore | Documents | Write rate per document and index limits |

### Cloud Bigtable in depth

::: plain
Bigtable is a gigantic phone book. Every entry is filed under one name, the **row key**, and the book is always kept in alphabetical order. Finding one name, or reading every name between "Smith" and "Smyth", is instant no matter how thick the book is. But you can only look things up by that one name: if you want everyone who lives on a certain street, you must read the whole book. So the art of Bigtable is choosing what to write as the name.
:::

**What it is.** Bigtable is Google's wide-column NoSQL database, the managed version of the system described in the 2006 Bigtable paper (which also inspired HBase and Cassandra). It is built for very large tables (terabytes to petabytes), very high read and write rates, and single-digit-millisecond latency. It powers time series, IoT telemetry, user profiles, ad and fraud features, and the online side of feature stores. It speaks the HBase API as well as its own client libraries.

**The data model.**

- A **table** holds **rows**. Each row has a **row key**: an arbitrary byte string up to 4 KB. Rows are stored **sorted by row key** in byte order, and the row key is the **only index**.
- Columns are grouped into **column families**, which you declare up front (keep it to a few; the recommended maximum is 100 per table). Inside a family, **column qualifiers** are created freely on write, so one row can have three columns and the next three thousand. Empty cells take no space: the table is **sparse**.
- Each cell can hold several **versions**, identified by a timestamp. A **garbage collection policy** per family keeps, for example, only the latest version or only the last 30 days.
- Writes are atomic **per row** only. There are no multi-row transactions and no joins. Single-row conditional writes (check-and-mutate) and atomic increments or appends (read-modify-write) exist.
- Size guidance: cells up to 10 MB recommended (100 MB hard limit), rows up to 100 MB recommended (256 MB hard limit).

**How it runs.** An **instance** contains one or more **clusters**, each in one zone, and each cluster has **nodes**. A table is split into **tablets**: contiguous ranges of row keys. Tablets are stored as SSTable files on Colossus, Google's distributed file system, and each node *serves* a set of tablets without *owning* their data. That separation is why Bigtable rebalances quickly: when one node is overloaded, moving a tablet to another node only moves a pointer, not the bytes. Adding nodes raises throughput almost linearly.

[[fig:gcpdata_bigtable]]

Google's own planning estimates for one SSD node (1 KB rows, read-only or write-only load) are up to about 17,000 reads per second, 14,000 writes per second, or 220 MBps of scans; HDD nodes are far slower for random reads (about 500 per second). Each SSD node can hold up to 5 TB, each HDD node 16 TB, so a cluster needs enough nodes for both throughput and storage. **Autoscaling** adds and removes nodes to keep CPU near a target you set.

```bash
# One instance, two clusters in different regions, autoscaling on CPU
gcloud bigtable instances create serving --display-name="Serving store" \
  --cluster-config=id=serving-ew1,zone=europe-west1-b,autoscaling-min-nodes=3,autoscaling-max-nodes=12,autoscaling-cpu-target=60
gcloud bigtable clusters create serving-ew4 --instance=serving --zone=europe-west4-a \
  --autoscaling-min-nodes=3 --autoscaling-max-nodes=12 --autoscaling-cpu-target=60

# A table with one column family that keeps 30 days of data
cbt -project=acme-prod -instance=serving createtable device_metrics
cbt -project=acme-prod -instance=serving createfamily device_metrics m
cbt -project=acme-prod -instance=serving setgcpolicy device_metrics m maxage=30d
```

**Row key design is the whole game.** Because rows are sorted and tablets are key ranges, the row key decides which node handles each read and write. Three rules cover most designs:

1. **Put the thing you query by first.** If the application asks "the last hour of readings for device 42", the key starts with the device: `dev-0042#...`. A query is then one short range scan.
2. **Never start with a value that only grows.** A key that starts with a timestamp or a sequence number sends every new write to the last tablet, so one node takes all the load while the others idle. This is **hotspotting**.
3. **Keep keys readable and fixed-width.** Pad numbers (`dev-0042`, not `dev-42`) so byte order matches numeric order, and use a delimiter such as `#`.

You can see rule 2 in plain Python. The script splits yesterday's keys into four tablets, then sends one minute of new writes from 100 devices:

```python
import bisect
from collections import Counter

def tablets_for(keys, n=4):                  # split points that give n equal tablets
    keys = sorted(keys)
    return [keys[len(keys) * i // n] for i in range(1, n)]

def load(keys, splits):                      # how many writes each tablet receives
    return Counter(bisect.bisect_right(splits, k) for k in keys)

devices = [f"dev-{d:04d}" for d in range(0, 1000, 10)]        # 100 devices
yesterday = range(1_700_000_000, 1_700_086_400, 3600)        # one reading per hour
today = range(1_700_086_400, 1_700_086_460)                  # the next minute

for name, key in [("time#device", lambda d, t: f"{t}#{d}"),
                  ("device#time", lambda d, t: f"{d}#{t}")]:
    splits = tablets_for([key(d, t) for d in devices for t in yesterday])
    new = [key(d, t) for d in devices for t in today]
    print(name, sorted(load(new, splits).items()))
# time#device [(3, 6000)]
# device#time [(0, 1500), (1, 1500), (2, 1500), (3, 1500)]
```

With the timestamp first, all 6,000 writes hit tablet 3. With the device first, each tablet takes a quarter. If the application usually wants the *newest* readings, store a **reverse timestamp** so the latest row sorts first and a scan with a limit of 10 returns the 10 most recent readings:

```python
MAX = 2**63 - 1
def row_key(device, ts_ms):
    return f"{device}#{MAX - ts_ms:019d}"    # zero-padded so text order = numeric order

for k in sorted(row_key("dev-0042", t) for t in (1764000000000, 1764000060000, 1764000120000)):
    print(k)
# dev-0042#9223370272854655807   <- newest reading (ts 1764000120000) first
# dev-0042#9223370272854715807
# dev-0042#9223370272854775807
```

Other patterns: **field promotion** (move a value you filter on into the key, `tenant#user#event_type#ts`), **salting** (prefix a small hash bucket, `07#dev-0042#...`, when one device itself is too hot; reads must then fan out over all buckets), and **tall and narrow** tables (one row per event) instead of ever-growing wide rows.

> **Pitfall:** Rows that grow forever. A key of just `dev-0042` with a new column per reading makes one row per device that grows past the 100 MB guidance, gets slow to read, and cannot be split, because a row never spans tablets. Put a time bucket in the key (`dev-0042#2026-11-23`) or use one row per reading.

**Replication and app profiles.** Adding a cluster in another zone or region starts replication automatically (an instance can span up to 8 regions). Replication is **eventually consistent** by default. Clients connect through an **app profile**, which sets the routing policy:

| Routing | Behaviour | Use for |
|---|---|---|
| Multi-cluster (`--route-any`) | Nearest available cluster, automatic failover | Online reads and idempotent writes that must survive a zone or region outage |
| Single-cluster (`--route-to=...`) | Always one cluster, read-your-writes | Single-row transactions (increments, check-and-mutate), batch jobs isolated from serving |

```bash
gcloud bigtable app-profiles create api --instance=serving --route-any
gcloud bigtable app-profiles create counters --instance=serving \
  --route-to=serving-ew1 --transactional-writes
```

**Analytics on Bigtable without hurting serving.** **Data Boost** is serverless compute for large reads: a Data Boost app profile sends a BigQuery external table query, a Dataflow job or a Spark job to separate compute, so the serving nodes do not see the scan. It is billed in serverless processing units only while used, supports single-cluster routing only, and does not guarantee that data written in the last 35 minutes is visible. Bigtable also supports **GoogleSQL queries** and, since April 2026, generally available **continuous materialized views**: aggregations that Bigtable keeps up to date in the background as data arrives. **Change streams** emit every mutation, so a Dataflow job can copy writes into BigQuery. The reverse direction, `EXPORT DATA` from BigQuery to Bigtable, is covered in [Analytics engineering and BI](#analytics).

**Bigtable or BigQuery?** They look similar on paper (both store huge tables) and are opposite in practice:

| | Bigtable | BigQuery |
|---|---|---|
| Access pattern | Point reads and short range scans by key | Large scans and aggregations over columns |
| Latency | Milliseconds | Seconds (sub-second with BI Engine) |
| Writes | Hundreds of thousands per second, row-level updates | Batch loads and streaming appends; DML is costly per row |
| Query language | Key lookups, filters, GoogleSQL with limits | Full SQL with joins and window functions |
| Pricing | Provisioned nodes plus storage | Bytes scanned or slots, plus storage |

The usual architecture uses both: events land in Bigtable for the application and in BigQuery for analysis, or BigQuery computes features and exports them to Bigtable for serving.

::: deeper
**LSM internals and garbage collection.** Each node buffers writes in memory and in a commit log, flushes them as SSTables, and merges SSTables in background **compactions**. Deletes and expired versions are removed only at compaction, so garbage collection is lazy: a read can still return a cell that is past its `maxage` until compaction runs. If correctness depends on age, filter by timestamp in the read as well. Many column families or qualifiers per row increase the work per read, because each read merges data from several SSTables.

**Hotspots and Key Visualizer.** Key Visualizer produces a heatmap of reads, writes and CPU by key range every 15 minutes. A bright horizontal band means a hot key range (a design problem); a bright diagonal moving upward means sequential keys. Bigtable can isolate a very hot tablet on its own node, but a single row is always served by one node, so a "celebrity" row needs a design change (salting, caching, or a counter split into shards).

**Sizing with replication.** With multi-cluster routing, each surviving cluster must absorb the whole load when another fails, so target a lower CPU per cluster than you would for a single cluster. Writes are applied in every cluster, so replication multiplies write cost by the number of clusters.

**Editions and tiers.** Bigtable is sold as **Enterprise** and **Enterprise Plus** editions with separate node prices. Enterprise Plus adds GoogleSQL through Data Boost and, in preview, an **in-memory tier** for sub-millisecond reads. **Tiered storage** (preview) moves cold data to a cheaper infrequent-access tier automatically. Check the editions page before you rely on either.

**Testing.** `gcloud beta emulators bigtable start` runs a local in-memory emulator for unit tests. Performance tests must run against a real cluster with realistic key distributions and enough data and time for Bigtable to split and balance tablets; a test with 1 GB for 30 seconds tells you nothing.

> **Interview:** "Design storage for one million IoT devices, each sending a reading every 10 seconds, with a dashboard showing any device's last 24 hours and a daily fleet report." That is 100,000 writes per second. Bigtable for the readings, key `device_id#reverse_ts` (or with a day bucket), one family with a 30-day GC policy, about 8 to 10 SSD nodes for writes plus headroom, autoscaling on CPU; multi-cluster routing for the dashboard API. The fleet report runs in BigQuery: a change stream or the ingest pipeline also writes to BigQuery, or a Data Boost query reads Bigtable directly once a day.
:::

::: check
?? Why is a row key that starts with the event timestamp a bad idea in Bigtable?
Rows are sorted by key and tablets are key ranges, so every new write has the largest key and lands in the last tablet. One node takes all writes (a hotspot) while the others idle.

?? An application increments a counter with read-modify-write, but the app profile uses multi-cluster routing. What goes wrong?
Single-row transactions need single-cluster routing. With multi-cluster routing, concurrent increments in different clusters could conflict, so Bigtable rejects them; use a single-cluster profile with transactional writes for that traffic.

?? Why can Bigtable rebalance load in seconds when a node is overloaded?
Nodes only serve tablets whose data lives in Colossus. Moving a tablet to another node moves metadata, not bytes.
:::

::: resources
- Docs: [Bigtable overview](https://docs.cloud.google.com/bigtable/docs/overview) · the data model, instances, clusters, nodes and how storage is separated from serving
- Docs: [Schema design best practices](https://docs.cloud.google.com/bigtable/docs/schema-design) · row keys, column families and patterns to avoid
- Docs: [Understand performance](https://docs.cloud.google.com/bigtable/docs/performance) · throughput per node, Key Visualizer and causes of slow clusters
- Docs: [Data Boost overview](https://docs.cloud.google.com/bigtable/docs/data-boost-overview) · serverless analytical reads, billing and limits
- Paper: [Bigtable: A Distributed Storage System for Structured Data](https://research.google/pubs/bigtable-a-distributed-storage-system-for-structured-data/) · the 2006 OSDI paper behind the service, HBase and Cassandra
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann's chapters on LSM trees, partitioning and replication explain Bigtable's design choices
:::

### Cloud Spanner in depth

::: plain
Imagine one company ledger that is copied by hand in three offices. Every entry must appear in the same order in all three copies, and anyone in any office must be able to read the latest page. Spanner keeps such a ledger for you, split into chapters that it moves between clerks as they get busy, and it uses extremely accurate clocks so the offices always agree on which entry came first. To you it looks like one ordinary SQL database.
:::

**What it is.** Spanner is Google's distributed relational database: SQL (GoogleSQL or a PostgreSQL dialect), ACID transactions, secondary indexes, and **external consistency** (the strongest form of serializability), while scaling horizontally across machines, zones and regions. The theory, TrueTime and commit wait, is explained in the NewSQL topic of [Databases: OLTP, NoSQL and specialised](#databases). This topic is about running it: instance configurations, splits, key design, interleaving, change streams and graph queries.

**Instances and capacity.** An **instance configuration** decides where replicas live. A **regional** configuration keeps three read-write replicas in three zones of one region (99.99% availability SLA). **Dual-region** and **multi-region** configurations spread replicas over regions, add **witness** replicas that vote but store no readable data, and offer a 99.999% SLA. Compute is bought in **processing units** (1,000 processing units = 1 node; below one node you buy in steps of 100). Each node supports up to 10 TiB of storage (1 TiB per 100 processing units for smaller instances). Spanner is sold in Standard, Enterprise and Enterprise Plus editions, and has a managed autoscaler.

```bash
gcloud spanner instances create orders --config=regional-europe-west1 \
  --edition=ENTERPRISE --processing-units=500 --description="Orders"
gcloud spanner databases create shop --instance=orders
```

**Splits.** Every table is stored sorted by its primary key. Spanner cuts the key space into **splits** (contiguous key ranges) and replicates each split with a **Paxos** group: one replica is the **leader**, which orders writes for that split, and the others follow. When a split grows large or busy, Spanner divides it (**load-based splitting**) and moves splits between servers. You never manage splits directly, though a **pre-splitting** API lets you create split points ahead of a known traffic spike such as a product launch.

[[fig:gcpdata_spanner]]

A transaction that touches one split commits with one Paxos round. A transaction across splits uses **two-phase commit**, with each split's Paxos group as a participant, which costs more round trips. Much of Spanner schema design is about keeping related rows in the same split.

**Key design.** Because splits are key ranges, monotonically increasing keys create a hotspot on the last split, exactly like Bigtable. Use a UUIDv4, a hash prefix, or a **bit-reversed sequence**, which turns 1, 2, 3 into values scattered over the key space:

```sql
CREATE SEQUENCE OrderIdSeq OPTIONS (sequence_kind = 'bit_reversed_positive');

CREATE TABLE Customers (
  CustomerId STRING(36) NOT NULL,          -- UUIDv4
  Name       STRING(MAX),
  Country    STRING(2),
) PRIMARY KEY (CustomerId);

CREATE TABLE Orders (
  CustomerId STRING(36) NOT NULL,
  OrderId    INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE OrderIdSeq)),
  OrderDate  DATE NOT NULL,
  Status     STRING(16),
  Total      NUMERIC,
) PRIMARY KEY (CustomerId, OrderId),
  INTERLEAVE IN PARENT Customers ON DELETE CASCADE;

CREATE INDEX OrdersByDate ON Orders(CustomerId, OrderDate DESC), INTERLEAVE IN Customers;
```

Why bit reversal works, in a few lines of Python (8 bits here; Spanner uses 63):

```python
def bit_reverse(n, bits=8):
    return int(format(n, f"0{bits}b")[::-1], 2)

ids = [1, 2, 3, 4, 5, 6]
print([bit_reverse(i) for i in ids])            # [128, 64, 192, 32, 160, 96]
split_of = lambda key: key * 4 // 256           # 4 equal key ranges
print([split_of(i) for i in ids])                # [0, 0, 0, 0, 0, 0]
print([split_of(bit_reverse(i)) for i in ids])   # [2, 1, 3, 0, 2, 1]
```

**Interleaving.** `INTERLEAVE IN PARENT` stores each customer's orders physically after the customer row, in the same split. Reading a customer with its orders is one range read, and a transaction on one customer and their orders stays in one Paxos group. Interleaving can be nested up to 7 levels. `ON DELETE CASCADE` deletes the children with the parent. An **interleaved index** (`INTERLEAVE IN Customers`) keeps index entries next to their parent too, which is ideal for "this customer's orders by date". A global index (no interleave) is itself a table distributed by its own key; an index whose first column is a timestamp is a hidden hotspot.

> **Pitfall:** Interleaving everything under one parent. If a single parent row (a large tenant, a popular product) has millions of children, that one key range cannot be split below the parent, so it becomes a hotspot of its own. Interleave only when children are bounded per parent and usually accessed with it.

**Transactions and reads.** **Locking read-write transactions** take locks and are retried on conflict (client libraries retry automatically, so keep the function free of side effects). **Read-only transactions** and single reads take no locks and read at a timestamp. A **strong read** sees all committed data; a **stale read** (exact or bounded staleness, for example 15 seconds) can be answered by the nearest replica without contacting the leader, which matters in multi-region instances. **Partitioned DML** runs large `UPDATE` or `DELETE` statements in parallel pieces, each its own transaction, to avoid the 80,000-mutation limit per commit.

```python
import datetime
from google.cloud import spanner

db = spanner.Client(project="acme-prod").instance("orders").database("shop")

with db.snapshot(exact_staleness=datetime.timedelta(seconds=15)) as snap:   # local replica read
    for row in snap.execute_sql("SELECT Status, COUNT(*) FROM Orders GROUP BY Status"):
        print(row)

rows = db.execute_partitioned_dml(
    "UPDATE Orders SET Status = 'ARCHIVED' WHERE OrderDate < '2024-01-01'")
print(rows, "rows archived")
```

**Change streams.** A change stream records every insert, update and delete for the tables or columns you choose, kept for 1 to 30 days (default 7):

```sql
CREATE CHANGE STREAM OrdersStream
  FOR Orders, Customers(Name, Country)
  OPTIONS (retention_period = '7d', value_capture_type = 'NEW_ROW');
```

Readers can be Dataflow (the Beam `SpannerIO` connector, with Google templates that write to BigQuery or Cloud Storage), Datastream (into BigQuery or Iceberg tables), a Debezium-based Kafka connector, or your own code using the API, which then must track **partition tokens** as the stream splits and merges with the data. `value_capture_type` decides what each record carries: old and new values of changed columns (the default), only new values, or the whole new row.

**Spanner Graph** defines a **property graph** over ordinary tables and queries it with **GQL**, the ISO graph query language, inside the same database and transactions:

```sql
CREATE PROPERTY GRAPH FinGraph
  NODE TABLES (Person, Account)
  EDGE TABLES (
    PersonOwnAccount
      SOURCE KEY (id) REFERENCES Person (id)
      DESTINATION KEY (account_id) REFERENCES Account (id)
      LABEL Owns,
    AccountTransferAccount
      SOURCE KEY (id) REFERENCES Account (id)
      DESTINATION KEY (to_id) REFERENCES Account (id)
      LABEL Transfers);

GRAPH FinGraph
MATCH (p:Person {name: "Dana"})-[:Owns]->(:Account)-[:Transfers]->{1,3}(dst:Account)
RETURN DISTINCT dst.id;
```

The query finds every account reachable from Dana's accounts in one to three transfers: a fraud-ring question that needs recursive self-joins in plain SQL. Graph modelling in general is covered in [Graph data engineering](#graph).

**Spanner and BigQuery.** BigQuery can query Spanner live with `EXTERNAL_QUERY` through a connection; enable **Data Boost** on the connection so analytical reads run on separate serverless compute and do not slow the application. The **columnar engine** for Spanner (generally available in 2026) keeps a columnar copy of data for analytical scans on live operational data. For continuous replication, use a change stream with Dataflow or Datastream; for the reverse direction, `EXPORT DATA` from BigQuery to Spanner.

```sql
SELECT * FROM EXTERNAL_QUERY('acme-prod.europe-west1.spanner-shop',
  'SELECT Status, COUNT(*) AS n FROM Orders GROUP BY Status');
```

::: deeper
**Reads in detail.** Any replica can serve a strong read: it asks the leader for the latest commit timestamp of the split and waits until it has applied everything up to it (its **safe time**). Stale reads skip the leader entirely. In a multi-region instance that is the difference between a cross-continent round trip and a local one, which is why read-heavy global apps use bounded staleness wherever a few seconds of lag is acceptable. Place the **default leader region** where most writes originate.

**Locking and retries.** Read-write transactions use pessimistic locking with **wound-wait**: an older transaction can abort a younger one that holds a conflicting lock. Long transactions that read many rows before writing are therefore aborted often under contention. Keep them short, read with precise key ranges, and never call external services inside the transaction function, because the client may run it several times.

**Version retention and recovery.** Spanner keeps old versions for a configurable `version_retention_period` (default 1 hour, up to 7 days), which enables **point-in-time recovery** and stale reads into the past. Backups and cross-region backup copies are separate features.

**Limits worth knowing.** Key size up to 8 KiB, 1,024 columns per table, 10 MiB per cell, 80,000 mutations and 100 MiB per commit (indexes and change stream records count). Every secondary index entry is a mutation, so a table with 10 indexes hits the mutation limit ten times sooner.

**Spanner Omni** (preview in 2026) is a downloadable edition that runs Spanner outside Google Cloud, on premises or in other clouds; it is not needed for ordinary Google Cloud workloads.

> **Interview:** "Spanner or AlloyDB for a new payments system?" If one region and one primary machine can handle peak writes for the foreseeable future, AlloyDB is cheaper and fully PostgreSQL. Choose Spanner when writes must scale horizontally, when you need multi-region strong consistency with automatic failover and no data loss, or when downtime for maintenance is unacceptable. Then design keys without monotonic prefixes, interleave accounts with their ledger entries, and keep transactions small.
:::

::: check
?? Why does Spanner recommend bit-reversed sequences or UUIDs instead of auto-increment keys?
Tables are split by primary-key ranges. Increasing keys send every insert to the last split, one Paxos leader takes all writes and the table cannot scale. Bit reversal or UUIDs spread inserts across splits.

?? What do you gain by interleaving `Orders` in `Customers`?
Each customer's orders are stored next to the customer row in the same split, so reading them together is one range read and a transaction on one customer avoids cross-split two-phase commit.

?? An analyst's `EXTERNAL_QUERY` against Spanner slows the checkout service. What do you change?
Enable Data Boost on the BigQuery connection so the query runs on separate serverless compute, or replicate to BigQuery with a change stream and query the copy.
:::

::: resources
- Docs: [Life of Spanner reads and writes](https://docs.cloud.google.com/spanner/docs/whitepapers/life-of-reads-and-writes) · how splits, leaders, Paxos and TrueTime handle each operation
- Docs: [Schema design best practices](https://docs.cloud.google.com/spanner/docs/schema-design) · keys, hotspots, interleaving and indexes
- Docs: [Change streams overview](https://docs.cloud.google.com/spanner/docs/change-streams) · scope, retention, value capture types and the ways to read them
- Docs: [Spanner Graph overview](https://docs.cloud.google.com/spanner/docs/graph/overview) · property graphs over tables and GQL queries
- Paper: [Spanner: Becoming a SQL System](https://research.google/pubs/spanner-becoming-a-sql-system/) · the SIGMOD 2017 paper on how Spanner's query processing, distribution and SQL layer work
- Docs: [Instance configurations](https://docs.cloud.google.com/spanner/docs/instance-configurations) · regional, dual-region and multi-region layouts, replica types and SLAs
:::

### AlloyDB and Cloud SQL

::: plain
Running your own database server is like owning a car: you choose it, but you also change the oil, fix the brakes and wake up when it breaks down at 3 a.m. Cloud SQL is a car with a full-time mechanic: the same familiar engines (PostgreSQL, MySQL, SQL Server), with backups, patches and failover handled for you. AlloyDB is a PostgreSQL car whose engine Google rebuilt for speed: it drives exactly like PostgreSQL, but carries more passengers (reads) and can also do some heavy hauling (analytics).
:::

**Cloud SQL** runs standard MySQL, PostgreSQL and SQL Server as a managed service. Google handles provisioning, patching, backups, point-in-time recovery, high availability and replicas; you still own the schema, the queries, the indexes and the connection count. It is the default answer when an application needs "a normal relational database" and the write load fits on one machine.

It comes in two **editions**:

| | Enterprise | Enterprise Plus |
|---|---|---|
| Availability SLA | 99.95% (maintenance excluded) | 99.99% (maintenance included) |
| Largest machine | 96 vCPU, 624 GB RAM | 192 vCPU, 1,512 GB RAM |
| Planned maintenance downtime | under 60 seconds | under 1 second |
| Point-in-time recovery logs | up to 7 days | up to 35 days |
| Data cache on local SSD | No | Yes |
| Read pools, managed connection pooling | No | Yes |

**High availability.** An HA (regional) instance has a primary in one zone and a standby in another, with writes replicated synchronously at the storage level. If the primary's zone fails, Cloud SQL promotes the standby and moves the instance's IP address, so clients reconnect to the same address. HA protects against zone failure, not against bad writes: a `DELETE` without `WHERE` is replicated instantly, which is what backups and point-in-time recovery are for.

**Read replicas** are asynchronous copies that serve reads and can live in other regions (**cross-region replicas**, which double as a disaster recovery plan: you promote one if the region fails). Point reporting, extracts and federated queries at a replica, never at the primary.

**AlloyDB for PostgreSQL** keeps the PostgreSQL front end (same SQL, extensions, drivers and tools) but replaces the storage layer. A **cluster** has one **primary instance** (with a standby node in another zone for HA) and any number of **read pool instances**, up to 20 nodes in total. All nodes share one regional, distributed **storage layer** instead of each keeping a full copy. The primary writes only its write-ahead log (WAL) to storage; storage services apply the WAL to data blocks in the background. Read pool nodes read the same storage and stream recent WAL to keep their caches fresh, so adding a read node takes minutes and needs no full data copy, and replica lag is usually small.

**The columnar engine** is AlloyDB's answer to "can I run analytics on my operational database?". It keeps selected columns in memory in a compressed columnar format (by default up to 30% of instance memory) and uses vectorised execution for scans, filters, aggregations and hash joins over them. **Auto-columnarization** watches the workload and chooses which columns to load; you can also add them yourself:

```sql
-- Instance flag (restarts the instance): google_columnar_engine.enabled = on
SELECT google_columnar_engine_add(relation => 'orders',
                                  columns  => 'order_date,status,country,total');

EXPLAIN (ANALYZE, COSTS OFF)
SELECT country, status, SUM(total) AS revenue
FROM orders
WHERE order_date >= DATE '2026-10-01'
GROUP BY country, status;
-- the plan shows a columnar scan node when the engine served the query
```

A query can use the columnar engine only if every column it scans is in the column store. Frequent updates invalidate columnar blocks until they are refreshed, so the engine suits tables that are appended to or updated moderately, and the optimizer may still prefer an index for selective lookups.

> **Tip:** The columnar engine makes "operational analytics" (a dashboard over today's orders, a support tool that aggregates a customer's history) fast without a pipeline. It does not replace BigQuery for history across many systems, for heavy joins over years of data, or for analysts who need to run anything they like without touching production. Put those workloads on a read pool or in BigQuery.

**Other AlloyDB features.** **Cross-region secondary clusters** replicate asynchronously to another region for disaster recovery. **AlloyDB AI** adds vector search (`pgvector` plus Google's ScaNN index) and SQL functions that call models. **AlloyDB Omni** is a downloadable edition for running the same engine on your own servers or other clouds.

**Which relational service?**

| Question | Cloud SQL | AlloyDB | Spanner |
|---|---|---|---|
| Engines | MySQL, PostgreSQL, SQL Server | PostgreSQL | GoogleSQL or PostgreSQL dialect |
| Write scaling | One primary | One primary | Horizontal |
| Read scaling | Replicas (copies) | Read pools on shared storage | Built in |
| Analytics on live data | Weak | Columnar engine | Data Boost, columnar engine |
| Multi-region writes with strong consistency | No | No | Yes |
| Typical fit | Most apps, lift-and-shift | Busy PostgreSQL apps, mixed workloads | Global or very large OLTP |

**Getting the data into BigQuery.** There are three ways, and they suit different jobs:

1. **Federated queries** (`EXTERNAL_QUERY`) push a SQL statement to Cloud SQL, AlloyDB or Spanner through a BigQuery **connection** and return the result. Good for small lookups and occasional reconciliation; bad for anything large or frequent, because the query runs on the operational database.

```bash
bq mk --connection --location=europe-west1 --project_id=acme-prod \
  --connection_type=CLOUD_SQL \
  --properties='{"instanceId":"acme-prod:europe-west1:orders-pg","database":"shop","type":"POSTGRES"}' \
  --connection_credential='{"username":"bq_reader","password":"<from Secret Manager>"}' \
  orders-pg
```

```sql
-- Point the connection at a read replica's instance ID, never at the primary
SELECT o.*, c.segment
FROM EXTERNAL_QUERY('acme-prod.europe-west1.orders-pg',
  '''SELECT order_id, customer_id, status, total
     FROM orders WHERE updated_at >= now() - interval '1 day' ''') AS o
JOIN marts.customers AS c USING (customer_id);
```

2. **Datastream** replicates continuously with change data capture from the database log into BigQuery (or Iceberg tables), with an initial backfill. This is the standard path for an analytical replica; the setup and the replication-slot pitfalls are in the Datastream topic of [Pipelines on Google Cloud](#gcppipes). On Cloud SQL for PostgreSQL enable the `cloudsql.logical_decoding` flag; on AlloyDB, `alloydb.logical_decoding`.
3. **Batch exports** (`gcloud sql export csv` to Cloud Storage, then a load job) are fine for small tables refreshed daily, and for one-off migrations. **Database Migration Service** is for moving databases *into* Cloud SQL or AlloyDB, not for analytics.

The reverse direction also exists: BigQuery `EXPORT DATA` can write query results into AlloyDB (on Enterprise editions), and a reverse ETL sync from BigQuery into AlloyDB is in preview.

> **Pitfall:** Connection exhaustion from serverless clients. Each Cloud Run instance opens its own pool, so 100 instances with 10 connections each need 1,000 connections, and a PostgreSQL instance with `max_connections` of a few hundred starts refusing them. Cap instances, keep pools small, and use managed connection pooling or PgBouncer. The Connection Pooling topic in [Databases: OLTP, NoSQL and specialised](#databases) has the arithmetic.

> **Pitfall:** Treating a read replica as consistent. A report that writes on the primary and immediately reads from a replica may not see its own write. Route read-your-writes paths to the primary, and monitor replica lag.

::: deeper
**Why AlloyDB writes less.** Standard PostgreSQL writes each change twice (WAL, then the data page at checkpoints), plus full-page images after each checkpoint to survive torn writes. AlloyDB's primary writes only WAL to a low-latency regional log; a fleet of log-processing workers materialises pages in the storage layer, which is replicated across zones. Checkpoints and full-page writes disappear from the primary, which frees I/O for queries. The storage layer also serves pages to any node, which is how read pools and fast failover share one copy of data.

**Caching tiers.** AlloyDB nodes keep hot pages in a buffer cache and a larger ultra-fast cache on local SSD, so a working set larger than memory still avoids most storage round trips. Cloud SQL Enterprise Plus has a similar **data cache** on local SSD. When sizing, compare the working set (not the database size) with memory plus cache.

**Cloud SQL storage and maintenance.** Cloud SQL storage can grow automatically but never shrinks; a one-off bloated table leaves you paying for the peak. Plan **maintenance windows** and **deny periods** (for example around Black Friday). Enterprise Plus does maintenance with sub-second downtime by switching to a pre-warmed replacement; Enterprise has up to about a minute of downtime.

**Security on both.** Private IP or Private Service Connect instead of public IPs; the **Cloud SQL Auth Proxy** or language connectors for TLS and IAM-based connection authorization; **IAM database authentication** so people and service accounts log in with Google identities instead of passwords; CMEK for disks and backups; and VPC Service Controls around the projects.

> **Interview:** "Our PostgreSQL primary is at 85% CPU and analysts run reports on it. What do you do?" First move reports off the primary: a read replica (Cloud SQL) or a read pool (AlloyDB), or replicate with Datastream to BigQuery for heavy analytics. Then look at the top queries (`pg_stat_statements`, Query Insights) and indexes. If writes alone saturate the largest machine, consider AlloyDB (more efficient writes) and, only after that, sharding or Spanner.
:::

::: check
?? What is the main architectural difference between AlloyDB read pool nodes and Cloud SQL read replicas?
Cloud SQL replicas each keep a full copy of the data and apply changes asynchronously. AlloyDB read pool nodes share the cluster's storage layer and only keep caches, so they are added quickly and lag little.

?? Why is `EXTERNAL_QUERY` a poor way to feed a daily 200 GB BigQuery model?
The query runs on the operational database and returns all rows through the connection: it loads production, is slow and is repeated every day. Use Datastream (CDC) for a continuously updated replica instead.

?? A query on AlloyDB does not use the columnar engine although the table is loaded. Name a likely reason.
One of the columns the query scans or joins on is not in the column store, or recent updates invalidated many columnar blocks, or the optimizer preferred an index for a selective lookup.
:::

::: resources
- Docs: [AlloyDB for PostgreSQL overview](https://docs.cloud.google.com/alloydb/docs/overview) · clusters, primary and read pool instances, storage, AlloyDB AI and Omni
- Docs: [About the AlloyDB columnar engine](https://docs.cloud.google.com/alloydb/docs/columnar-engine/about) · which queries benefit, memory sizing and auto-columnarization
- Docs: [Introduction to Cloud SQL editions](https://docs.cloud.google.com/sql/docs/editions-intro) · Enterprise versus Enterprise Plus features and SLAs
- Docs: [Cloud SQL federated queries](https://docs.cloud.google.com/bigquery/docs/cloud-sql-federated-queries) · connections, `EXTERNAL_QUERY` and type mapping
- Blog: [AlloyDB for PostgreSQL under the hood: intelligent, database-aware storage](https://cloud.google.com/blog/products/databases/alloydb-for-postgresql-intelligent-scalable-storage) · the log-processing storage design explained by Google's engineers
- Book: [PostgreSQL 14 Internals](https://postgrespro.com/community/books/internals) · Egor Rogov's free book on WAL, checkpoints, MVCC and buffers, the mechanisms AlloyDB changes
:::

## Governance and sharing

Once data flows from operational databases into BigQuery and the lake, two questions follow. Can people find the right data and trust it? And can other teams and other companies use it without copies that nobody controls? This group answers both on Google Cloud.

The concepts (catalogs, metadata models, lineage, data quality, clean rooms, differential privacy) are taught in [Data governance, security and compliance](#governance), and BigQuery's own access controls (IAM, row-level security, policy tags, masking) in [BigQuery in depth](#bigquery). Here the focus is the managed services: **Knowledge Catalog** for discovery, metadata, lineage and quality; **BigQuery sharing** and **data clean rooms** for sharing inside Google Cloud; and **Delta Sharing** and other open protocols for recipients on other platforms.

### Knowledge Catalog (formerly Dataplex Universal Catalog)

::: plain
A large library without a catalogue is a warehouse of paper: the books are there, but nobody can find them or knows whether they are any good. Knowledge Catalog is the catalogue, the librarian and the inspector for your data. It lists every table, file and topic automatically, lets owners write what each one means, shows where each one came from, and regularly checks that the contents are healthy, so a person (or an AI agent) can search for "monthly revenue by country" and land on the right, trustworthy table.
:::

**What it is.** Knowledge Catalog (formerly Dataplex Universal Catalog, and Data Catalog before that) is Google Cloud's managed catalog and governance service. It has grown from a metadata index into a "context" layer used by people, BI tools and AI agents. Its main capabilities:

| Capability | What it does | Effort for you |
|---|---|---|
| Automatic harvesting | Entries for BigQuery, Cloud SQL, AlloyDB, Spanner, Bigtable, Pub/Sub, Cloud Storage, Agent Platform models and more | None: on by default |
| Search | Keyword and natural-language semantic search over names, descriptions and metadata | None |
| Business metadata | Custom **aspects**, **glossaries** of business terms linked to columns | Define aspect types, assign stewards |
| Lineage | Table and column lineage from BigQuery jobs, Managed Airflow, Managed Spark, Dataflow and OpenLineage | Enable the Data Lineage API |
| Profiling scans | Column statistics: nulls, distinct values, ranges, top values | Create scans |
| Data quality scans | Rules with thresholds, results to the catalog and BigQuery | Write rules |
| Discovery scans | Turn files in Cloud Storage into BigQuery external or Lakehouse tables | Create scans |
| Data insights | Gemini drafts descriptions, suggested joins and example queries | Review and publish |
| Data products | Bundles of assets with owners, contracts and usage notes | Curate |

The metadata model (an **entry** per asset, in an **entry group**, with an **entry type**, carrying typed **aspects**) and lineage with OpenLineage are explained in [Data governance, security and compliance](#governance). This topic is about operating the service.

[[fig:gcpdata_catalog]]

**Harvesting and search.** Google Cloud sources are catalogued as soon as they exist, with technical metadata (schema, location, labels, sizes) refreshed automatically. Other systems (an on-premises Oracle database, a Collibra glossary, a Snowflake account) come in through **metadata import**: you or a connector produce metadata in Knowledge Catalog's import format, and a **metadata job** creates or updates the entries. Search respects IAM: a user sees an entry only if they can see the underlying asset, or if they hold catalog-level roles on the entry group. That is what makes it safe to turn search on for the whole company.

**Discovery scans for the lake.** Files that land in Cloud Storage are not tables until someone defines them. A **discovery scan** inspects a bucket, groups structured and semi-structured files (CSV, JSON, Parquet, Avro, ORC) into tables with schemas and partitions, and publishes them as BigQuery external tables or Lakehouse tables in a dataset named after the bucket; unstructured files (images, PDFs) become object tables.

```bash
gcloud dataplex datascans create data-discovery raw-landing-discovery \
  --location=europe-west1 \
  --data-source-resource="//storage.googleapis.com/projects/acme-prod/buckets/acme-raw-landing" \
  --bigquery-publishing-table-type=BIGLAKE \
  --bigquery-publishing-connection=projects/acme-prod/locations/europe-west1/connections/lake \
  --storage-exclude-patterns="**/_tmp/**" \
  --schedule="TZ=Europe/Berlin 0 * * * *"
```

**Profiling and quality scans.** A **data profile scan** computes per-column statistics and can publish them to the catalog, so anyone looking at a table sees its null share and value ranges next to its schema. A **data quality scan** checks rules (completeness, uniqueness, ranges, sets, regex, SQL expressions) against thresholds; the rule language is shown in the Data Quality Frameworks topic of [Data governance, security and compliance](#governance). In production you define scans in code, run them on new partitions only, and export results to BigQuery so they can be charted and alerted on:

```hcl
resource "google_dataplex_datascan" "orders_profile" {
  data_scan_id = "orders-profile"
  location     = "europe-west1"
  data {
    resource = "//bigquery.googleapis.com/projects/acme-prod/datasets/marts/tables/orders"
  }
  execution_spec {
    trigger {
      schedule { cron = "TZ=Europe/Berlin 30 6 * * *" }
    }
  }
  data_profile_spec {
    sampling_percent = 20
    row_filter       = "order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
    post_scan_actions {
      bigquery_export {
        results_table = "//bigquery.googleapis.com/projects/acme-prod/datasets/dq/tables/profile_results"
      }
    }
  }
}
```

The same scan from the command line is `gcloud dataplex datascans create data-profile orders-profile --location=europe-west1 --data-source-resource=... --sampling-percent=20 --row-filter=... --export-results-table=... --schedule=...`. Quality scans can also be triggered from an orchestrator after each load, so a Managed Airflow DAG fails (and does not publish) when a critical rule fails.

**Business metadata that stays correct.** Technical metadata is free; business metadata is work. Define a small number of **aspect types** that matter (for example `data-contract` with owner, tier, freshness SLO and PII flag), make them required for gold-tier datasets, and set them from the same code that creates the tables (Terraform or a CI job calling the API), not by hand in the console. Link columns to **glossary** terms so "active customer" has one definition. Use **data insights** to draft descriptions from query logs, but treat them as drafts a steward approves.

**Lineage on Google Cloud.** With the Data Lineage API enabled, every BigQuery job (queries, `CREATE TABLE AS SELECT`, `MERGE`) records table and column lineage automatically; Managed Airflow, Managed Service for Apache Spark and Dataflow report lineage through their integrations; anything else can send OpenLineage events. Lineage is regional and kept for 30 days of process history, so it answers "what feeds this table now?" well and "what fed it two years ago?" not at all.

> **Pitfall:** A catalog nobody curates. Turning on Knowledge Catalog gives you thousands of entries with no owners and no descriptions, and search returns ten tables called `orders`. Start with the 20 to 50 tables people actually query (find them in `INFORMATION_SCHEMA.JOBS`), give each an owner, a tier and a description, and mark everything else as uncurated.

> **Pitfall:** Full-table scans every hour. Each profile or quality scan is billed as Knowledge Catalog processing and reads the table. On a 10 TB table, scan the new partition with a row filter, sample for statistics, and run full scans weekly.

::: deeper
**Resources and IAM.** Scans, entry groups, aspect types and glossaries are regional or global resources in a project. Typical roles: `roles/dataplex.catalogViewer` for searchers, `roles/dataplex.catalogEditor` or entry-group-level roles for stewards who write aspects, `roles/dataplex.dataScanAdmin` for the platform team. Scans run as the Knowledge Catalog service agent (or a service account you choose), which needs read access to the data, and access to write the export table.

**Lakes and zones.** Older Dataplex setups organised storage into **lakes**, **zones** (raw and curated) and **assets** with automatic discovery and security propagation. That model still exists, but new designs use catalog-level features (entry groups, aspects, discovery scans, IAM on datasets and buckets) directly; you will meet lakes and zones mainly in existing estates.

**Context for AI.** The 2026 direction of the product is to serve metadata to AI agents: aspects, glossaries, golden queries (verified question and SQL pairs) and lineage are exposed through APIs and a remote MCP server so agents can ground their SQL in curated meaning. The practical consequence for data engineers is that business metadata quality now directly limits the quality of conversational analytics. Several of these capabilities are in preview; check the release notes before building on them.

**Sensitive data.** Knowledge Catalog does not classify PII by itself; **Sensitive Data Protection** discovery profiles BigQuery and Cloud Storage for sensitive data types and can write its findings as tags or aspects, which then drive policy tags and masking in BigQuery.

> **Interview:** "Our analysts don't trust the warehouse. What would you put in place on Google Cloud?" Ownership and tiers as required aspects on curated datasets; descriptions and glossary links for the most-queried tables; automatic lineage so impact and root cause are visible; profile and quality scans on gold tables, run after each load and exported to BigQuery with alerts; a published freshness SLO per table; and certified data products so search ranks trusted tables first.
:::

::: check
?? Files land daily in a Cloud Storage bucket and nobody has defined tables for them. Which Knowledge Catalog feature helps, and what does it produce?
A discovery scan. It infers schemas and partitions, groups files into tables, and publishes them as BigQuery external or Lakehouse tables in a dataset named after the bucket, with entries in the catalog.

?? Why should data quality scan results be exported to a BigQuery table?
So results can be queried over time, charted, joined with other metadata and used for alerts and SLO reporting, instead of living only as the latest status in the console.

?? A user searches the catalog and does not see a table that exists. Is that a bug?
Usually not. Search respects IAM, so users see only entries for assets they are allowed to see (or entry groups where they hold catalog roles).
:::

::: resources
- Docs: [Knowledge Catalog overview](https://docs.cloud.google.com/knowledge-catalog/docs/introduction) · capabilities, integrations and how the catalog serves people and agents
- Docs: [Discover and catalog Cloud Storage data](https://docs.cloud.google.com/knowledge-catalog/docs/cloud-storage-discovery) · discovery scans, published tables and required permissions
- Docs: [About data profiling](https://docs.cloud.google.com/knowledge-catalog/docs/data-profiling-overview) · what profile scans compute, sampling and export
- Docs: [About data lineage](https://docs.cloud.google.com/knowledge-catalog/docs/about-data-lineage) · automatic lineage sources, retention and limits
- Book: [Data Governance: The Definitive Guide](https://www.oreilly.com/library/view/data-governance-the/9781492063483/) · Eryurek, Gilad, Lakshmanan and others at Google on running governance as a programme, with Google Cloud examples
- Docs: [Terraform google_dataplex_datascan](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataplex_datascan) · defining profile, quality and discovery scans as code
:::

### BigQuery sharing (formerly Analytics Hub) and data clean rooms

::: plain
Copying data to a partner is like photocopying a book and posting it: the copy goes out of date at once, and you can never get it back. BigQuery sharing is a library card instead. The partner reads your live shelf from their own account, you decide which shelf, and you can cancel the card at any time. A **data clean room** goes one step further: it is a supervised reading room where two companies can put their data side by side and ask questions, but the supervisor only lets out answers about groups of people, never about one person.
:::

**The model.** BigQuery sharing (formerly Analytics Hub) has four objects:

- A **data exchange** is a container for listings with its own access control, private (your organization, named partners) or public.
- A **listing** points at one **shared dataset** (tables, views, authorized views, materialized views, routines, BigQuery ML models, external tables) or at a **Pub/Sub topic**, with a description, documentation and contact details.
- A **subscriber** with `roles/analyticshub.subscriber` on the listing **subscribes**, which creates a **linked dataset** in their own project: a read-only pointer to the shared dataset, with no copy. For a topic, subscribing creates a Pub/Sub subscription in their project.
- **Publishers** (`roles/analyticshub.publisher`) create listings and see usage; **admins** (`roles/analyticshub.admin`) manage exchanges.

Costs follow the pointer: the publisher keeps paying for storage (or Pub/Sub publishing), the subscriber pays for the queries they run in their own project, on their own reservation or on-demand. Nothing is charged for exchanges and listings themselves. The short version of this is already in the security topic of [BigQuery in depth](#bigquery); this topic covers designing, operating and extending it.

[[fig:gcpdata_cleanroom]]

**Publishing as code.** Treat an exchange like any other production interface: defined in Terraform, reviewed, versioned.

```hcl
resource "google_bigquery_analytics_hub_data_exchange" "partners" {
  location         = "EU"
  data_exchange_id = "acme_partners"
  display_name     = "Acme partner data"
}

resource "google_bigquery_analytics_hub_listing" "supplier_sales" {
  location         = "EU"
  data_exchange_id = google_bigquery_analytics_hub_data_exchange.partners.data_exchange_id
  listing_id       = "supplier_sales"
  display_name     = "Daily sales by supplier"
  bigquery_dataset {
    dataset = google_bigquery_dataset.share_supplier_sales.id   # a dataset built for sharing
  }
  restricted_export_config {          # egress controls
    enabled               = true      # no copy, export or snapshot of shared tables
    restrict_query_result = true      # no copy or export of query results either
  }
}

resource "google_bigquery_analytics_hub_listing_iam_member" "supplier_42" {
  location         = "EU"
  data_exchange_id = google_bigquery_analytics_hub_data_exchange.partners.data_exchange_id
  listing_id       = google_bigquery_analytics_hub_listing.supplier_sales.listing_id
  role             = "roles/analyticshub.subscriber"
  member           = "group:bi-team@supplier42.example"
}
```

**Designing what you share.**

1. **Share a dataset built for sharing**, containing views or curated tables, never your internal dataset. The shared dataset is a contract: consumers' queries break if you rename a column.
2. **One listing for many consumers** with row-level security: a row access policy per supplier group (`supplier_id = 42`) on the shared table lets one listing serve hundreds of suppliers, each seeing only their rows. Policy tags and masking apply to subscribers too.
3. **Egress controls** for licensed or sensitive data. With them, subscribers cannot copy, clone, export or snapshot tables, run `CREATE TABLE AS SELECT` or write results to tables, which keeps data inside the share.
4. **Monitor and revoke.** Publishers see subscriptions and usage (the `INFORMATION_SCHEMA.SHARED_DATASET_USAGE` view records consumer jobs on shared datasets) and can revoke a subscription, which removes the linked dataset's access at once.
5. **Location.** A linked dataset lives in the shared dataset's location. To serve consumers in another region, publish **linked dataset replicas** in additional regions, rather than letting each consumer copy the data.

**Stream sharing.** A listing can share a Pub/Sub topic: each subscriber gets their own subscription in their project and pays for their own delivery, while you keep one topic. This is how real-time feeds (prices, inventory, events) are shared without a custom API.

**Commercial data.** Public listings can be sold through Google Cloud Marketplace: the subscriber buys the listing and gets the linked dataset, and Google handles billing.

**Data clean rooms.** A clean room is a special exchange where several parties contribute data and **analysis rules** decide which queries are allowed. Roles: the **owner** manages membership, **contributors** add data, **subscribers** query. Contributors publish **views** with a privacy policy; tables shared without analysis rules give raw access, so always share rule-protected views. The rules:

| Analysis rule | What it enforces | Typical question |
|---|---|---|
| Aggregation threshold | Every output row must aggregate at least *k* distinct privacy units (for example users) | Spend by product category among users who saw a campaign |
| Differential privacy | Noise is added and a privacy budget (epsilon, delta) limits total queries | Statistics that must withstand repeated querying |
| List overlap | Only rows that match across parties can be returned after a join | Which of my customers also appear in your list |
| Join restriction (inside the others) | Joins only on allowed columns, and whether a join is required | Force joins on a hashed email, nothing else |

A retailer contributes purchases; an advertiser wants to measure a campaign:

```sql
-- Retailer (contributor): a view protected by an aggregation threshold of 50 users,
-- joinable only on hashed_email
CREATE OR REPLACE VIEW cleanroom_src.purchases_v
OPTIONS (privacy_policy = '{"aggregation_threshold_policy": {"threshold": 50, "privacy_unit_column": "hashed_email"}, "join_restriction_policy": {"join_condition": "JOIN_ALL", "join_allowed_columns": ["hashed_email"]}}')
AS SELECT hashed_email, product_category, spend, purchase_date FROM retail.purchases;

-- Advertiser (subscriber): only aggregates over 50 or more users come back
SELECT WITH AGGREGATION_THRESHOLD
  p.product_category,
  COUNT(DISTINCT p.hashed_email) AS buyers,
  SUM(p.spend) AS spend
FROM retail_cleanroom.purchases_v AS p
JOIN ads.exposed_users AS e USING (hashed_email)
GROUP BY p.product_category;
```

Groups with fewer than 50 distinct users are dropped silently. The logic in plain Python, with a threshold of 3:

```python
from collections import defaultdict

rows = [("u1", "saw_ad", 40), ("u2", "saw_ad", 25), ("u3", "saw_ad", 30),
        ("u4", "saw_ad", 55), ("u5", "saw_ad", 20), ("u6", "no_ad", 35),
        ("u7", "no_ad", 10), ("u8", "no_ad", 15), ("u9", "vip_ad", 900)]

def aggregate_with_threshold(rows, threshold):
    users, spend = defaultdict(set), defaultdict(int)
    for user, segment, amount in rows:
        users[segment].add(user)            # privacy unit = user
        spend[segment] += amount
    return {s: spend[s] for s in spend if len(users[s]) >= threshold}

print(aggregate_with_threshold(rows, threshold=3))   # {'saw_ad': 170, 'no_ad': 60}
print(aggregate_with_threshold(rows, threshold=1))   # {'saw_ad': 170, 'no_ad': 60, 'vip_ad': 900}
```

With no threshold, the `vip_ad` group reveals exactly what one person spent.

**Query templates** go further: the owner publishes predefined table-valued functions, and subscribers can only call those, with parameters. Google recommends them because analysis rules alone "aren't guaranteed to block every unauthorized query" designed to extract raw data. **Entity resolution** lets parties without a shared identifier match records through an identity provider they both choose.

> **Pitfall:** Sharing the internal dataset "for now". Every internal refactoring then breaks a partner, and columns you never meant to share (cost prices, internal notes) are visible. Build a sharing dataset of views from day one.

> **Pitfall:** Assuming hashing anonymises the join key. A hashed email is still personal data: anyone with a list of emails can hash them and match. Clean rooms protect the outputs; your contracts and data protection assessment must still treat the inputs as personal data.

::: deeper
**How linked datasets work.** A linked dataset is metadata in the subscriber's project that resolves to the publisher's storage at query time. The subscriber needs no IAM on the source dataset; access is granted by the listing. IAM can be set only at the linked dataset level, not per table. Limits worth knowing: up to 1,000 linked datasets per shared dataset, no materialized views or snapshots on linked tables, and very large joins over linked tables can fail.

**Sharing versus other options.** Inside one organization, plain IAM on a dataset (or an authorized view) is simpler and enough. Use sharing when you want a catalogue of data products with self-service subscription, usage tracking, egress controls, or partners in other organizations. Clean rooms are for when neither side may see the other's rows.

**Thresholds are not a proof.** Aggregation thresholds stop single-row outputs but are open to **differencing attacks**: query a group of 50, then the same group plus one person, and subtract. Query templates, join restrictions and, for strong guarantees, differential privacy with a budget address this. The budget is the hard part: once spent, the view returns errors until the owner resets it, so plan which questions are worth answering. The theory is in the Privacy Engineering topic of [Data governance, security and compliance](#governance).

**Perimeters.** VPC Service Controls apply to sharing: subscribing across a perimeter needs ingress and egress rules for the BigQuery and sharing APIs, otherwise subscriptions fail with permission errors that look like IAM problems.

> **Interview:** "A retailer wants to give 300 suppliers their own sales data daily." One sharing dataset with a sales view, a row access policy per supplier group, one private listing in a partner exchange with egress controls, subscriber role granted to each supplier's group, documentation and a schema contract on the listing, usage monitored from `SHARED_DATASET_USAGE`. Suppliers pay for their own queries; nothing is copied; revoking a supplier is one IAM change.
:::

::: check
?? A partner subscribes to your listing and runs 2 TB of queries a month. Who pays for what?
You pay for storing the shared data; the partner pays for the 2 TB of queries in their own project. Nothing is copied, so there is no second storage bill.

?? A clean room view has an aggregation threshold of 50 on `user_id`. A subscriber groups by `postcode` and one postcode has 12 users. What do they see?
That postcode's row is not returned. Only groups with at least 50 distinct users appear in the result.

?? Why are query templates recommended on top of analysis rules?
Rules restrict query shape but cannot anticipate every clever combination of allowed queries. Templates limit subscribers to a few reviewed queries with parameters, which closes most extraction tricks.
:::

::: resources
- Docs: [Introduction to BigQuery sharing](https://docs.cloud.google.com/bigquery/docs/analytics-hub-introduction) · exchanges, listings, linked datasets, roles, egress controls, limits and pricing
- Docs: [Share sensitive data with data clean rooms](https://docs.cloud.google.com/bigquery/docs/data-clean-rooms) · roles, query templates, entity resolution and limitations
- Docs: [Restrict data access using analysis rules](https://docs.cloud.google.com/bigquery/docs/analysis-rules) · aggregation threshold, differential privacy, list overlap and join restriction syntax
- Docs: [Terraform google_bigquery_analytics_hub_listing](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_analytics_hub_listing) · listings and egress controls as code
- Book: [The Algorithmic Foundations of Differential Privacy](https://www.cis.upenn.edu/~aaroth/Papers/privacybook.pdf) · Dwork and Roth's free book, the theory behind privacy budgets
- Docs: [Use differential privacy](https://docs.cloud.google.com/bigquery/docs/differential-privacy) · the BigQuery syntax, budgets and what the guarantees mean
:::

### Delta Sharing & Open Data Sharing Standards

::: plain
BigQuery sharing works beautifully when both sides use BigQuery. But your partner may use Databricks, Spark on their own servers, plain Python or a BI tool. An open sharing protocol is like a standard shipping container: whoever packs it, any port in the world has the crane to unload it. **Delta Sharing** is the most widely used such container for tables: the provider says "here are the files of this table, readable for the next hour", and any client that speaks the protocol reads them directly.
:::

**Why an open protocol.** Warehouse-native sharing (BigQuery sharing, Snowflake Secure Data Sharing) is zero-copy and governed, but only inside one platform. File drops (CSV over SFTP, a bucket) work with anything but create stale, uncontrolled copies. Delta Sharing, launched by Databricks in 2021 as an open-source part of the Linux Foundation's Delta Lake project, sits between them: live and governed like a warehouse share, but readable by any client that implements a small REST protocol.

**How the protocol works.** There are three parties: the **provider**, a **sharing server** that knows which tables are in which **share**, and the **recipient**. The recipient receives a **profile file** with the server endpoint and a credential:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing.acme.example/delta-sharing/",
  "bearerToken": "<token issued for this recipient>",
  "expirationTime": "2027-01-31T00:00:00.0Z"
}
```

The client then calls a handful of REST endpoints: list shares (`GET /shares`), schemas and tables, read a table's version and metadata, and **query** a table (`POST /shares/{share}/schemas/{schema}/tables/{table}/query`) with optional hints: a version or timestamp for time travel, a limit, and predicate hints for partition pruning. The response is newline-delimited JSON: a protocol line, a metadata line with the schema and partition columns, then one line per **data file**, each with a **short-lived pre-signed URL**. The client downloads the Parquet files straight from the provider's object storage. No data passes through the sharing server, which is why it scales to large tables. A `changes` endpoint returns the table's change data feed for incremental reads.

Here is a recipient processing a (shortened) query response in plain Python, pruning partitions on the client side:

```python
import json

response = """\
{"protocol":{"minReaderVersion":1}}
{"metaData":{"id":"f8d5c169","format":{"provider":"parquet"},"schemaString":"{...}","partitionColumns":["order_date"]}}
{"file":{"url":"https://storage.googleapis.com/acme-share/orders/part-0001.parquet?X-Goog-Signature=...","id":"a1","partitionValues":{"order_date":"2026-11-22"},"size":5242880}}
{"file":{"url":"https://storage.googleapis.com/acme-share/orders/part-0002.parquet?X-Goog-Signature=...","id":"a2","partitionValues":{"order_date":"2026-11-23"},"size":4194304}}"""

files = []
for line in response.splitlines():
    action = json.loads(line)
    if "metaData" in action:
        print("partitioned by", action["metaData"]["partitionColumns"])
    elif "file" in action:
        f = action["file"]
        files.append((f["partitionValues"]["order_date"], f["size"]))

wanted = [f for f in files if f[0] == "2026-11-23"]
print(len(files), "files,", sum(s for _, s in wanted) // 2**20, "MiB to read")
# partitioned by ['order_date']
# 2 files, 4 MiB to read
```

**Recipients.** The open-source connectors hide all of this:

```python
import delta_sharing   # pip install delta-sharing

profile = "acme.share"                                   # the profile file
client = delta_sharing.SharingClient(profile)
print(client.list_all_tables())

table_url = profile + "#sales_share.marts.orders"         # <profile>#<share>.<schema>.<table>
df = delta_sharing.load_as_pandas(table_url, limit=1000)  # pandas
sdf = delta_sharing.load_as_spark(table_url)              # needs the Spark connector
```

```python
# PySpark with the connector on the classpath (--packages io.delta:delta-sharing-spark_2.13:<version>)
orders = spark.read.format("deltaSharing").load("acme.share#sales_share.marts.orders")
changes = (spark.readStream.format("deltaSharing")
           .option("startingVersion", 120)
           .load("acme.share#sales_share.marts.orders"))   # incremental, if history is shared
```

BI tools such as Power BI and Tableau ship Delta Sharing connectors that take the same profile, so a business recipient needs no Spark at all.

**Providers.** Most providers use **Databricks with Unity Catalog** (on Google Cloud, Databricks stores the tables in Cloud Storage and signs URLs for them). Two modes exist:

- **Open sharing**: the recipient gets a token-based profile and can use any client. You control expiry and rotate tokens.
- **Databricks-to-Databricks sharing**: the recipient is identified by their Unity Catalog metastore ID, no token is exchanged, and they mount the share as a catalog. Shared views, notebooks and models are possible here.

```sql
-- Databricks SQL (provider)
CREATE SHARE supplier_sales;
ALTER SHARE supplier_sales ADD TABLE sales.marts.orders
  PARTITION (country = 'DE') AS marts.orders_de     -- share only one partition, under another name
  WITH HISTORY;                                     -- allow time travel and change data feed
CREATE RECIPIENT supplier_42;                       -- open sharing: returns an activation link
CREATE RECIPIENT partner_dbx USING ID 'gcp:europe-west1:<metastore-uuid>';   -- Databricks-to-Databricks
GRANT SELECT ON SHARE supplier_sales TO RECIPIENT supplier_42;
```

Without Databricks, the open-source **reference server** in the `delta-sharing` repository can serve Delta tables from Cloud Storage; it is useful for learning and small internal setups, but production providers need token management, audit and monitoring that a platform supplies.

**Snowflake Data Clean Rooms.** Snowflake's clean room product runs inside Snowflake accounts (including accounts hosted in Google Cloud regions): a provider defines **templates** (parameterised SQL with privacy controls such as minimum group sizes), a consumer runs approved analyses such as audience overlap and campaign measurement, and results can be activated to ad platforms. It is the Snowflake counterpart of BigQuery data clean rooms; Databricks has its own clean rooms built on Delta Sharing. In practice the clean room follows the platform where most of the sensitive data already lives.

**The landscape of open sharing.**

| Approach | Recipient needs | Zero-copy | Governance | Best for |
|---|---|---|---|---|
| Delta Sharing | Any Delta Sharing client (Spark, pandas, BI tools, Databricks) | Yes, reads provider storage | Shares, recipients, token expiry, audit on provider | Cross-platform table sharing |
| Iceberg REST catalog with credential vending | Any Iceberg engine (Spark, Trino, Flink, BigQuery) | Yes | Catalog grants and scoped, short-lived storage credentials | Several engines and organizations on one lakehouse |
| BigQuery sharing | A Google Cloud project | Yes | IAM, egress controls, row and column security | Partners on BigQuery |
| Snowflake Secure Data Sharing | A Snowflake account | Yes within a region | Shares and grants | Partners on Snowflake |
| Files plus signed URLs | Anything | No | None after download | One-off or very simple exchanges |

The **Iceberg REST catalog** specification is the main open alternative for tables: the catalog checks the caller's grants and **vends** short-lived credentials scoped to the table's files. On Google Cloud the Lakehouse runtime catalog (formerly BigLake metastore) exposes such an endpoint, and Apache Polaris and Unity Catalog implement it too. Delta tables can also publish Iceberg metadata (Delta UniForm), so one copy can be read through either ecosystem.

> **Pitfall:** Forgetting egress. Recipients read files from the provider's bucket, so the provider pays network egress when recipients are in another region or cloud. Large, frequently read shares to distant recipients can cost more than the storage. Replicate the share's tables to a bucket near the recipient, or charge for it.

> **Pitfall:** Long-lived bearer tokens in email. A profile file is a credential: anyone who has it can read the share until it expires. Send it through a secure channel, set short expiry, rotate tokens, and prefer identity federation where the provider supports it.

::: deeper
**What the protocol cannot do.** Row filters and column masks that the provider sets on the source are applied only if they are baked into what is shared (a partition filter, a view on platforms that support sharing views, a dedicated table). A raw-file protocol cannot enforce per-query policies on the recipient's side, unlike BigQuery sharing, where the provider's row access policies run inside every subscriber query.

**Versions and features.** Tables with newer Delta features (deletion vectors, column mapping) need a client that understands them; the protocol has a "delta" response format that ships the Delta log actions instead of plain file lists for such tables. Check connector versions on both sides before sharing a table with advanced features.

**Auditing and revocation.** Every query against the sharing server is logged with the recipient, so the provider knows who read what and when. Revocation stops new queries immediately, but pre-signed URLs already issued remain valid until they expire (minutes to an hour), and files already downloaded cannot be recalled.

**Choosing in a multi-platform company.** A common pattern on Google Cloud: curated tables in Apache Iceberg managed tables or Iceberg tables in Cloud Storage; BigQuery sharing for partners on BigQuery; the Iceberg REST endpoint for internal engines; Delta Sharing from Databricks for partners on other platforms. One copy of data, several doors, each with its own audit.

> **Interview:** "A partner on another cloud wants daily access to 2 TB of our order history, which lives in Databricks on Google Cloud." Delta Sharing with open sharing: a share with the needed tables and partitions, `WITH HISTORY` if they need incremental reads, a recipient with short token expiry sent securely. Estimate egress (2 TB a day across clouds is significant), and consider sharing only the daily increment through the change data feed instead of letting them reread everything.
:::

::: check
?? Where does the data travel when a Delta Sharing recipient reads a table?
From the provider's object storage directly to the recipient's client, through pre-signed URLs returned by the sharing server. The server sends only metadata and URLs.

?? What is the difference between open sharing and Databricks-to-Databricks sharing?
Open sharing gives a token-based profile file usable by any Delta Sharing client. Databricks-to-Databricks identifies the recipient by their Unity Catalog metastore, exchanges no token, and supports more asset types such as views and notebooks.

?? A partner uses BigQuery and you use BigQuery. Should you set up Delta Sharing?
No. BigQuery sharing is zero-copy, supports egress controls and applies your row and column policies inside the partner's queries. Open protocols are for recipients on other platforms.
:::

::: resources
- Spec: [Delta Sharing protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md) · the REST endpoints, profile file format and response format in full
- Docs: [Delta Sharing project site](https://delta.io/sharing/) · the open-source connectors and the list of supported clients
- Docs: [What is Delta Sharing? (Databricks on Google Cloud)](https://docs.databricks.com/gcp/en/delta-sharing/) · open versus Databricks-to-Databricks sharing, recipients, history and security
- Repo: [delta-io/delta-sharing](https://github.com/delta-io/delta-sharing) · the Python and Spark connectors and the reference server
- Blog: [Introducing Delta Sharing](https://www.databricks.com/blog/2021/05/26/introducing-delta-sharing-an-open-protocol-for-secure-data-sharing.html) · the original announcement and design goals
- Docs: [Snowflake Data Clean Rooms](https://docs.snowflake.com/en/user-guide/cleanrooms/introduction) · providers, consumers, templates and activation
- Spec: [Apache Iceberg REST catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) · the catalog API with credential vending, the main open alternative for table sharing
:::

## AI and architecture

The last group turns outward. Most data platforms exist at least partly to feed machine learning: churn scores, recommendations, fraud checks, forecasts, and now agents that answer questions over company data. On Google Cloud these run on **Gemini Enterprise Agent Platform** (formerly Vertex AI). The data engineer rarely trains the model, but owns almost everything that decides whether it works in production: correct training data, features that are the same at training and serving time, pipelines that retrain and score on schedule, and predictions written back where the business can use them.

The final topic assembles the whole Google Cloud part of the course (foundations, BigQuery, pipelines, operational databases, governance and AI) into the standard reference architectures you will see in design reviews and interviews. The general ML data topics (feature engineering, embeddings, RAG, evaluation) come later in [Data engineering for AI and ML](#aiml).

### Gemini Enterprise Agent Platform (formerly Vertex AI) for data engineers

::: plain
Think of a restaurant. The chefs (data scientists) create the dishes (models), but a dish is only as good as its ingredients, and a dish that tastes different every night is useless. The data engineer runs the pantry: ingredients arrive fresh and labelled, the recipe uses exactly the same ingredients in the test kitchen as in service, and the orders (predictions) go out to the tables on time. Gemini Enterprise Agent Platform is the kitchen equipment; this topic is about running the pantry.
:::

**What the platform is.** Gemini Enterprise Agent Platform (formerly Vertex AI) is Google Cloud's platform for building, training, serving and monitoring models and AI agents, including access to Gemini and partner models. API names, the Python package (`google-cloud-aiplatform`) and IAM roles (`roles/aiplatform.user`) kept the old name. The parts a data engineer touches:

| Component | What it does | Data engineer's job |
|---|---|---|
| Feature Store | Serves feature values from BigQuery to online applications | Build and refresh the feature tables; size the online store |
| Pipelines | Runs ML workflows as DAGs of containers (Kubeflow Pipelines) | Data preparation steps, scheduling, parameters, lineage |
| Datasets and training jobs | Managed training data references and custom training on CPUs, GPUs, TPUs | Point-in-time-correct training tables, snapshots |
| Model Registry | Versions of models, including BigQuery ML models | Promotion rules in CI/CD |
| Batch prediction | Scores a whole table and writes results back | Inputs and outputs in BigQuery, schedules, cost |
| Endpoints | Online prediction over HTTPS | Feature freshness, request logging to BigQuery |
| Model monitoring | Compares serving inputs with a training baseline | Baselines and alert routing |
| Vector Search, agents | Similarity search, agent runtime | Embedding pipelines, grounding data |

**Training data without leakage.** The most common data bug in ML is **leakage**: training rows contain information that was not available at prediction time, so the model looks brilliant offline and fails in production. For time-dependent features you need a **point-in-time join**: for each label at time *t*, take the latest feature value recorded at or before *t*, never the current value.

```sql
-- Labels: one row per customer per month-end, churned within 30 days (0/1)
-- Features: history of customer features with the time each value became known
CREATE OR REPLACE TABLE ml.training_churn_2026_11 AS
SELECT l.customer_id, l.label_ts, l.churned,
       f.orders_30d, f.avg_basket, f.days_since_last_order
FROM ml.churn_labels AS l
LEFT JOIN ml.customer_features_history AS f
  ON f.customer_id = l.customer_id
 AND f.feature_ts <= l.label_ts                      -- nothing from the future
WHERE l.label_ts BETWEEN TIMESTAMP '2024-01-01' AND TIMESTAMP '2026-09-30'
QUALIFY ROW_NUMBER() OVER (PARTITION BY l.customer_id, l.label_ts
                           ORDER BY f.feature_ts DESC) = 1;
```

The same idea in plain Python shows what goes wrong without it:

```python
import bisect

features = {"c1": [(1, 2), (5, 7), (9, 1)],     # (feature_ts, orders_30d)
            "c2": [(2, 0), (6, 4)]}
labels = [("c1", 4, 0), ("c1", 8, 1), ("c2", 7, 1)]   # (customer, label_ts, churned)

def as_of(customer, ts):                       # latest value known at ts
    hist = features[customer]
    i = bisect.bisect_right([t for t, _ in hist], ts)
    return hist[i - 1][1] if i else None

def latest(customer):                          # the leaky shortcut
    return features[customer][-1][1]

print([(c, t, as_of(c, t), y) for c, t, y in labels])   # [('c1', 4, 2, 0), ('c1', 8, 7, 1), ('c2', 7, 4, 1)]
print([(c, t, latest(c), y) for c, t, y in labels])     # [('c1', 4, 1, 0), ('c1', 8, 1, 1), ('c2', 7, 4, 1)]
```

With the leaky version, customer `c1` has `orders_30d = 1` at both label times: the value recorded at time 9, after they had already churned. The model learns "few recent orders means churn" from the future.

Freeze each training set (a dated table or a **table snapshot**) and record its name with the model, so any model can be retrained or audited on exactly the data it saw. BigQuery time travel only reaches back 7 days.

**Feature Store.** Agent Platform Feature Store keeps **BigQuery as the offline store**: your feature tables and views *are* the features, so there is no second copy for training. For low-latency serving, you create an **online store** and **feature views**, which sync the latest value per entity from a BigQuery table or view on a schedule. An optional **feature registry** (feature groups mapped to BigQuery tables, features mapped to columns) adds time-series handling, column selection and drift monitoring.

The recommended online store is **Bigtable online serving**, which handles terabytes and high request rates. **Optimized online serving** is deprecated: no new features since May 17, 2026, and its APIs will be shut down on February 17, 2027; embedding similarity search moves to Vector Search.

```hcl
resource "google_vertex_ai_feature_online_store" "serving" {
  name   = "customer_features"
  region = "europe-west1"                      # same location as the BigQuery source
  bigtable {
    auto_scaling {
      min_node_count         = 1
      max_node_count         = 3
      cpu_utilization_target = 60
    }
  }
}

resource "google_vertex_ai_feature_online_store_featureview" "churn" {
  name                 = "churn_features"
  region               = "europe-west1"
  feature_online_store = google_vertex_ai_feature_online_store.serving.name
  sync_config { cron = "0 * * * *" }            # hourly sync from BigQuery
  big_query_source {
    uri               = "bq://acme-prod.ml.customer_features_latest"
    entity_id_columns = ["customer_id"]
  }
}
```

The online application then fetches features by entity ID (`customer_id = "c-123"`) through the online serving API. Training uses point-in-time SQL on the history table, serving uses the synced latest values, and both come from the same BigQuery logic, which is what prevents **training-serving skew**.

**Pipelines.** Agent Platform Pipelines runs **Kubeflow Pipelines** (KFP) definitions: each step is a container, inputs and outputs are tracked as artifacts with lineage, and runs can be scheduled or triggered from Managed Airflow. A minimal retraining pipeline:

```python
from kfp import dsl, compiler
from google.cloud import aiplatform

@dsl.component(base_image="python:3.12", packages_to_install=["google-cloud-bigquery"])
def build_training_table(project: str, run_month: str) -> str:
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    client.query(f"CALL ml.build_training_churn('{run_month}')").result()   # the point-in-time SQL
    return f"{project}.ml.training_churn_{run_month}"

@dsl.component(base_image="python:3.12",
               packages_to_install=["google-cloud-bigquery[pandas]", "scikit-learn", "joblib"])
def train(project: str, table: str, model: dsl.Output[dsl.Model]):
    import joblib
    from google.cloud import bigquery
    from sklearn.linear_model import LogisticRegression
    df = bigquery.Client(project=project).query(f"SELECT * FROM `{table}`").to_dataframe()
    X = df[["orders_30d", "avg_basket", "days_since_last_order"]].fillna(0)
    clf = LogisticRegression(max_iter=1000).fit(X, df["churned"])
    joblib.dump(clf, model.path)

@dsl.pipeline(name="churn-training")
def churn_pipeline(project: str, run_month: str):
    t = build_training_table(project=project, run_month=run_month)
    train(project=project, table=t.output)

compiler.Compiler().compile(churn_pipeline, "churn_pipeline.yaml")
aiplatform.init(project="acme-prod", location="europe-west1")
aiplatform.PipelineJob(
    display_name="churn-training",
    template_path="churn_pipeline.yaml",
    pipeline_root="gs://acme-ml-pipelines/churn",
    parameter_values={"project": "acme-prod", "run_month": "2026_11"},
).submit(service_account="ml-pipelines@acme-prod.iam.gserviceaccount.com")
```

**Batch prediction from BigQuery.** Most business predictions are not real time: score every customer nightly and write the scores to a table that CRM, Looker and reverse ETL read. A batch prediction job reads a BigQuery table, spreads it across prediction workers, and writes predictions (and any errors) to BigQuery:

```python
model = aiplatform.Model("projects/acme-prod/locations/europe-west1/models/churn")
job = model.batch_predict(
    job_display_name="churn-scores-2026-11-23",
    bigquery_source="bq://acme-prod.ml.scoring_input_20261123",
    bigquery_destination_prefix="bq://acme-prod.ml_scores",
    instances_format="bigquery",
    predictions_format="bigquery",
    machine_type="n2-standard-8",
    starting_replica_count=2,
    max_replica_count=10,
)
```

Gemini models have their own **batch inference**: put one request per row in a BigQuery table, submit a batch job, and the responses land in an output table, at a lower price than online calls and without client-side rate-limit handling. For smaller jobs, the `AI.GENERATE` family of functions in BigQuery calls Gemini row by row from SQL (see [BigQuery in depth](#bigquery)). The input and output datasets must be in the job's region.

**BigQuery ML or Agent Platform?** Use **BigQuery ML** when the model fits its built-in types (regression, boosted trees, k-means, ARIMA_PLUS forecasting, matrix factorization; TimesFM forecasts through `AI.FORECAST`) and the team works in SQL: training and scoring stay next to the data. Register BigQuery ML models in the Model Registry (`OPTIONS (model_registry = 'VERTEX_AI')`) when you want one place to version and deploy models. Move to Agent Platform training for custom code, deep learning, GPUs, or when online serving needs an endpoint.

> **Pitfall:** Features computed twice. The data scientist computes `days_since_last_order` in a notebook with pandas for training; the application team recomputes it in Java for serving, with a different time zone. The model degrades and nobody knows why. Compute features once, in BigQuery, and serve them through the Feature Store or exported tables.

> **Pitfall:** Idle endpoints. An online prediction endpoint keeps at least one replica (a VM, sometimes with a GPU) running around the clock. If predictions are only needed daily, use batch prediction and undeploy the model.

::: deeper
**Feature freshness budgets.** A feature view synced hourly means features can be up to an hour old plus the BigQuery pipeline's own lag. Write down the freshness each feature needs: "orders in the last 30 days" tolerates a day; "items in the basket right now" does not belong in a batch feature table at all and should be computed by a streaming pipeline (Dataflow writing to Bigtable) or passed in the request.

**Monitoring needs data engineering.** Model monitoring compares the distribution of serving inputs with a training baseline (a BigQuery table) and alerts on drift. That only works if serving requests and responses are logged (endpoints can log to BigQuery) and if labels arrive later and are joined back to predictions to measure real accuracy. Build that join as a regular model in your warehouse.

**Reproducibility.** A model is reproducible only if you can name its code version, its pipeline parameters and its exact training data. Pipelines record artifacts and lineage in ML Metadata; your part is making the training table immutable (dated table, snapshot, or an Iceberg snapshot ID) and passing its name as a parameter, never "the latest data".

**Security and location.** Pipelines and training jobs run as service accounts; give them dataset-level BigQuery roles, not project-wide editor. Feature Store resources, batch prediction jobs and their BigQuery datasets must share a location. VPC Service Controls cover the platform's APIs; include them in the same perimeter as BigQuery and Cloud Storage.

> **Interview:** "Design the data side of a real-time fraud model on Google Cloud." Transactions arrive on Pub/Sub; Dataflow computes streaming features (counts per card in the last 5 minutes and hour) into Bigtable; batch features (customer tenure, historical chargeback rate) are built in BigQuery and synced to Feature Store with Bigtable online serving; the scoring service reads both and calls an endpoint; every request and decision is logged to BigQuery; labels (chargebacks) arrive weeks later and are joined back point-in-time for retraining in a scheduled pipeline; drift and feature freshness have alerts.
:::

::: check
?? What is a point-in-time join and why does training need one?
For each label at time t, it takes the latest feature value known at or before t. Without it, training rows include information from after the prediction moment (leakage), so offline accuracy is inflated and production accuracy collapses.

?? Where does Agent Platform Feature Store keep its offline data?
In BigQuery: feature groups and feature views point at BigQuery tables or views, so no separate offline store exists. Online stores sync the latest values from them.

?? A team scores 5 million customers once a night with an always-on endpoint called from a script. What would you change?
Use a batch prediction job that reads the input table from BigQuery and writes scores back to BigQuery, and undeploy the endpoint. It is cheaper, parallel and simpler to schedule.
:::

::: resources
- Docs: [About Feature Store on Gemini Enterprise Agent Platform](https://docs.cloud.google.com/vertex-ai/docs/featurestore/latest/overview) · BigQuery as the offline store, online store types, feature views and the deprecation timeline
- Docs: [Introduction to Agent Platform Pipelines](https://docs.cloud.google.com/vertex-ai/docs/pipelines/introduction) · KFP pipelines, artifacts, lineage and scheduling on the managed service
- Docs: [Get batch inferences from a custom trained model](https://docs.cloud.google.com/vertex-ai/docs/predictions/get-batch-predictions) · BigQuery input and output, machine types and replicas
- Docs: [Batch inference for BigQuery](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/capabilities/batch-prediction-from-bigquery) · request tables and output for Gemini batch jobs
- Book: [Machine Learning Design Patterns](https://www.oreilly.com/library/view/machine-learning-design/9781098115777/) · Lakshmanan, Robinson and Munn on feature stores, transform, and reproducibility patterns, with Google Cloud examples
- Paper: [Hidden Technical Debt in Machine Learning Systems](https://papers.nips.cc/paper_files/paper/2015/hash/86df7dcfd896fcaf2674f757a2463eba-Abstract.html) · the NeurIPS 2015 paper on why the data around a model is most of the work
:::

### Google Cloud reference architectures for data

::: plain
Architects do not design every house from nothing. They start from proven plans (a terraced house, a bungalow) and adapt them to the plot and the family. Data platforms are the same: a handful of standard plans cover most needs, and the skill is picking the right plan, knowing why each room is where it is, and knowing which walls you can move.
:::

Four plans cover most Google Cloud data platforms. Each is described as a flow, the services that implement it, the decisions that matter and where it usually breaks. Google publishes official versions in the Architecture Center; the versions here combine them with what the previous sections taught.

[[fig:gcpdata_refarch]]

**1. Batch lakehouse.** For daily or hourly analytics over many sources, with open formats so several engines can read the data.

```text
Sources (files, SaaS, DB exports) -> Cloud Storage landing bucket (raw, immutable)
  -> load or Spark (Managed Service for Apache Spark) -> Apache Iceberg tables (bronze, silver)
  -> BigQuery SQL with Dataform or dbt -> marts (gold) -> Looker, Data Studio, BigQuery sharing
Catalog: Lakehouse runtime catalog for Iceberg, Knowledge Catalog for discovery, lineage, quality
Control: Managed Airflow DAG per domain, data-interval aware, with quality scans as gates
```

Decisions: Iceberg managed tables (BigQuery writes, others read) or Iceberg tables written by Spark (several writers); partitioning by date and clustering by the main filter; one landing bucket per source with lifecycle rules. Where it breaks: small files from frequent loads (schedule compaction), schemas drifting at the source (contracts and quarantine), and backfills that overwrite good data (write-audit-publish). Details are in [Data lakes and lakehouses](#lakehouse) and [BigQuery in depth](#bigquery).

**2. Streaming analytics.** For dashboards and alerts that must be seconds to minutes fresh.

```text
Apps, devices -> Pub/Sub (or Managed Kafka) -> Dataflow (parse, dedupe on event ID, enrich, window)
  -> BigQuery via the Storage Write API (analytics)      -> Looker, BI Engine
  -> Bigtable (per-key serving, latest state)            -> APIs, feature serving
  -> Pub/Sub alerts topic -> Cloud Run handlers
Simplest variant: Pub/Sub BigQuery subscription plus SQL (continuous queries) with no Dataflow at all
```

Decisions: event-time windows and allowed lateness; exactly-once in Dataflow versus at-least-once plus dedupe in BigQuery; dead-letter topics; whether Bigtable is needed at all. Where it breaks: hot keys in Dataflow, schema changes in events, and unbounded state. See [Pipelines on Google Cloud](#gcppipes) and [Streaming systems](#streaming).

**3. CDC to BigQuery.** For an analytical replica of operational databases, minutes behind.

```text
Cloud SQL / AlloyDB / self-managed PostgreSQL, MySQL, Oracle, SQL Server
  -> Datastream (backfill + log-based CDC) -> BigQuery tables in merge mode (max_staleness controls cost)
     or append-only change tables -> Dataform builds SCD type 2 history and current views
Spanner -> change stream -> Dataflow template or Datastream -> BigQuery
Bigtable -> change stream -> Dataflow -> BigQuery
```

Decisions: merge mode (current state, cheap to query) versus append-only (full history, more work); `max_staleness` per table; which tables to include. Where it breaks: replication slots retaining WAL when a stream stops, unsupported DDL changes, large transactions, and primary keys missing on source tables.

**4. ML platform.** For models trained on warehouse data and served to applications.

```text
BigQuery (features as SQL, point-in-time training tables, snapshots)
  -> Agent Platform Pipelines (prepare, train, evaluate, register) -> Model Registry
  -> batch prediction -> BigQuery scores -> Looker, reverse ETL to CRM, Bigtable or AlloyDB
  -> or endpoint + Feature Store (Bigtable online serving) for real-time scoring
Monitoring: request logs to BigQuery, drift baselines, labels joined back for accuracy
```

Decisions: BigQuery ML versus custom training; batch versus online; feature freshness per feature. Where it breaks: leakage, training-serving skew, and unowned scoring tables that silently stop updating.

**What every plan shares.** The cross-cutting layer from [Google Cloud foundations for data](#gcpfound) is the same for all four:

| Concern | Standard choice |
|---|---|
| Projects | Separate projects per environment and per layer (landing, warehouse, pipelines, ML), in folders |
| Location | One home region or multi-region for all data services; DR region decided up front |
| Identity | Groups for people, one service account per pipeline, dataset-level grants |
| Perimeter and keys | VPC Service Controls around data projects, CMEK where required, Secret Manager |
| Governance | Knowledge Catalog with owners and tiers, lineage on, quality scans on gold tables |
| Sharing | Authorized views inside, BigQuery sharing outside, clean rooms for partner joins |
| Operations | Cloud Monitoring alerts on freshness, backlog and failures; budgets and quotas |
| Delivery | Terraform for infrastructure, CI/CD for SQL, DAGs and pipelines |

**Sizing before choosing.** A design review starts with numbers, not services. A quick estimate for a streaming design:

```python
events_per_sec = 20_000
avg_bytes = 1_200
mb_per_sec = events_per_sec * avg_bytes / 1e6
tb_per_day = events_per_sec * avg_bytes * 86_400 / 1e12
print(f"{mb_per_sec:.0f} MB/s, {tb_per_day:.2f} TB/day raw")          # 24 MB/s, 2.07 TB/day raw
print(f"about {tb_per_day * 365 / 8:.0f} TB/year at 8x compression")   # about 95 TB/year at 8x compression
```

24 MB/s is comfortable for Pub/Sub and a modest Dataflow job; 95 TB a year in BigQuery means partitioning and long-term storage pricing matter from day one, and a dashboard that scans the whole table on every refresh would be expensive.

> **Pitfall:** Choosing the most impressive plan. A team with 50 GB of data and daily reports does not need Pub/Sub, Dataflow, Bigtable and a feature store. BigQuery, load jobs, Dataform and one scheduler cover it. Add a component only when a requirement (freshness, latency, an existing engine) forces it.

> **Tip:** Write a one-page architecture decision record for each major choice: the context, the options considered, the decision, and what would make you revisit it. Future you, and the next interviewer, will ask "why Bigtable?".

::: deeper
**Disaster recovery.** BigQuery multi-region datasets protect against zone failures, not against a region-wide outage of the multi-region or against bad writes; **managed disaster recovery** (cross-region dataset replication with failover, on Enterprise Plus) and table snapshots cover those. For each plan, list every stateful component (buckets, datasets, Spanner, Bigtable, AlloyDB, Pub/Sub retention, Airflow metadata) and decide its recovery point and recovery time objectives. Stateless parts (Dataflow jobs, Cloud Run) are redeployed from code.

**Data mesh on Google Cloud.** In a mesh, domains own their data products. The mapping is direct: one project (or folder) per domain, curated datasets as products, Knowledge Catalog data products and aspects for ownership and contracts, BigQuery sharing for consumption across domains, and central guard rails (organization policies, VPC Service Controls, budgets) from a platform team. The hard parts are organisational, covered in [Data governance, security and compliance](#governance).

**Anti-patterns seen in reviews.** Cross-region joins by accident (datasets created in the default `US` location); a Spark cluster left running 24 hours a day for a 40-minute job; dashboards querying raw event tables; one service account with project owner for every pipeline; no dead-letter topics; Datastream streams with no alert on retained WAL; scoring tables with no freshness check.

> **Interview:** "Design analytics for a food delivery app: live order tracking for operations, daily finance reports, and a delivery-time prediction model." Streaming plan for orders and courier events (Pub/Sub, Dataflow, BigQuery for ops dashboards, Bigtable for live order state); CDC plan for the orders and payments databases (Datastream to BigQuery); batch lakehouse for finance marts built with Dataform, quality gates before month-end; ML plan for delivery-time prediction with point-in-time features and online serving through Feature Store; Knowledge Catalog lineage end to end; one region, separate projects, VPC Service Controls; numbers first: orders per second at peak, events per order, retention.
:::

::: check
?? A dashboard must show orders less than a minute after they are placed, and nothing else in the company needs streaming. What is the simplest architecture?
Publish order events to Pub/Sub with a BigQuery subscription writing straight into a table, and point the dashboard at a view over it (with BI Engine if needed). No Dataflow job is required unless events need complex transformation.

?? In the CDC plan, what is the trade-off between Datastream's merge mode and append-only mode?
Merge mode keeps a current-state table that is cheap to query but loses history. Append-only keeps every change, so you can build history (SCD type 2) and audits, but you must deduplicate and build current views yourself.

?? Name two things all four reference architectures share.
Any two of: a single home region, separate projects per environment and layer, service accounts per pipeline with dataset-level grants, VPC Service Controls, Knowledge Catalog governance, monitoring and budgets, infrastructure as code.
:::

::: resources
- Docs: [Architecture Center: big data and analytics](https://docs.cloud.google.com/architecture/big-data-analytics) · Google's reference architectures and jump-start solutions for data platforms
- Docs: [Analytics lakehouse](https://docs.cloud.google.com/architecture/big-data-analytics/analytics-lakehouse) · the batch lakehouse plan with Cloud Storage, BigQuery and Spark
- Docs: [MLOps: continuous delivery and automation pipelines in machine learning](https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning) · maturity levels for ML platforms and the pipelines behind them
- Docs: [Architecture and functions in a data mesh](https://docs.cloud.google.com/architecture/data-mesh) · domains, data products and the platform on Google Cloud
- Docs: [Google Cloud Well-Architected Framework](https://docs.cloud.google.com/architecture/framework) · the review checklist for reliability, security, cost and operations
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data-engineering/9781098108298/) · Reis and Housley's lifecycle view that these plans implement
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Bigtable row key | Query dimension first (`device#reverse_ts`); never a timestamp or sequence first |
| Bigtable failover vs transactions | `--route-any` for online reads; `--route-to=... --transactional-writes` for increments |
| Scan Bigtable without hurting serving | Data Boost app profile (single-cluster, data older than 35 minutes) |
| Bigtable or BigQuery | Key lookups in milliseconds: Bigtable; scans and joins: BigQuery |
| Spanner key without hotspots | UUIDv4 or `sequence_kind = 'bit_reversed_positive'` |
| Keep parent and children together | `INTERLEAVE IN PARENT Customers ON DELETE CASCADE` |
| Cheap global reads in Spanner | Stale reads with bounded or exact staleness |
| Spanner changes to BigQuery | `CREATE CHANGE STREAM ... FOR ...` then Dataflow or Datastream |
| Analytics on Spanner from BigQuery | `EXTERNAL_QUERY` with Data Boost on the connection |
| PostgreSQL: Cloud SQL or AlloyDB | Cloud SQL by default; AlloyDB for heavy reads, mixed workloads, columnar engine |
| Operational database into BigQuery | Datastream CDC; `EXTERNAL_QUERY` only for small lookups on a replica |
| Make lake files queryable | `gcloud dataplex datascans create data-discovery ...` |
| Profile a table daily | `gcloud dataplex datascans create data-profile ... --row-filter --export-results-table` |
| Share with a partner on BigQuery | Exchange, listing on a sharing dataset of views, egress controls, row policies |
| Partner joins without seeing rows | Clean room view with `aggregation_threshold_policy`; `SELECT WITH AGGREGATION_THRESHOLD` |
| Share with a partner not on BigQuery | Delta Sharing (open sharing, short token expiry) or an Iceberg REST catalog |
| Training data without leakage | Point-in-time join: latest `feature_ts <= label_ts`, frozen in a dated table |
| Online features | Feature Store with Bigtable online serving; feature view synced from BigQuery |
| Nightly scores | `model.batch_predict(bigquery_source=..., bigquery_destination_prefix=...)` |
| Picking an architecture | Numbers first (rate, size, freshness), then the simplest plan that meets them |

### Practice

::: try
**1. Design a Bigtable table.** A ride-hailing app stores the GPS position of each of 200,000 active drivers every 4 seconds. The app needs a driver's last 10 minutes of positions, and the operations team needs a daily heatmap of all positions. Propose the row key, column families, garbage collection policy, a rough node count, and how the daily heatmap is produced.
:::

?? A solution to 1
200,000 / 4 = 50,000 writes per second, so roughly 4 SSD nodes at the planning estimate of 14,000 writes per node, plus headroom: start with 6 to 8 nodes and autoscaling on CPU. Row key `driver_id#reverse_timestamp` (driver first, fixed width), so the last 10 minutes is a short prefix scan that returns the newest rows first. One column family `p` with `lat`, `lng`, `speed`, and a GC policy of `maxage=7d` (or whatever replay the app needs). Multi-cluster routing for the app's reads. The heatmap runs in BigQuery: either the ingest pipeline writes to BigQuery too, or a nightly Data Boost query through a BigQuery external table reads the day's data without loading the serving nodes.

::: try
**2. Fix a Spanner schema.** A team created `Events (EventId INT64 NOT NULL, CreatedAt TIMESTAMP, UserId STRING(36), Payload JSON) PRIMARY KEY (EventId)` with an application-side counter for `EventId`, plus a global index on `CreatedAt`. Writes plateau at a low rate no matter how many nodes they add. Explain why and propose a fix.
:::

?? A solution to 2
The increasing `EventId` sends every insert to the last split, and the index on `CreatedAt` is itself a table keyed by an increasing timestamp, a second hotspot. Adding nodes does not help because one split's leader takes all writes. Fix: generate `EventId` from a bit-reversed sequence (or use a UUIDv4), and if events are read per user, make the key `(UserId, EventId)` and interleave in a `Users` table. Replace the global `CreatedAt` index with an interleaved index `(UserId, CreatedAt DESC)`, or, for time-range analytics, read through a change stream into BigQuery instead of indexing by time in Spanner.

::: try
**3. Share and protect.** A bank wants to let 40 fintech partners on BigQuery see daily aggregates of their own customers' transactions, and wants one marketing partner to measure overlap between the bank's customers and the partner's app users without either side seeing the other's customer list. Design both.
:::

?? A solution to 3
For the 40 partners: a sharing dataset with a view of daily aggregates per partner, a row access policy per partner group on the underlying table, one private listing in a partner exchange with egress controls enabled, subscriber role granted to each partner's group, documentation and a schema contract on the listing, usage monitored from `SHARED_DATASET_USAGE`. For the overlap: a data clean room in which both parties contribute views of hashed identifiers, protected by a list overlap rule (join restriction `JOIN_ALL` on the hashed key) or an aggregation threshold so only counts of matched users over a minimum group size can be returned, ideally through a query template the bank approves. Both sides still treat the hashed identifiers as personal data in their contracts.

::: try
**4. Find the leak.** A churn model scores an AUC of 0.97 offline and 0.62 in production. The training table was built by joining labels from last year with the `customers` table as it is today. Explain the likely cause and fix it, with SQL.
:::

?? A solution to 4
Leakage: today's `customers` table contains values computed after the label dates (for example a low `orders_30d` that is low *because* the customer churned). Keep a history table of feature values with the time each became known, and join point in time:

```sql
CREATE OR REPLACE TABLE ml.training_churn_fixed AS
SELECT l.customer_id, l.label_ts, l.churned, f.orders_30d, f.avg_basket
FROM ml.churn_labels AS l
LEFT JOIN ml.customer_features_history AS f
  ON f.customer_id = l.customer_id AND f.feature_ts <= l.label_ts
WHERE l.label_ts >= TIMESTAMP '2025-01-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY l.customer_id, l.label_ts
                           ORDER BY f.feature_ts DESC) = 1;
```

Then freeze the result as a dated table, retrain, and expect a lower but honest offline score that matches production.

### What comes next

You can now choose and design the operational databases around a Google Cloud data platform, keep it discoverable and trustworthy with Knowledge Catalog, share it safely inside and outside BigQuery, feed machine learning on Gemini Enterprise Agent Platform, and assemble it all into reference architectures. Many of the services in this section (Spanner, Bigtable, Pub/Sub) are distributed systems whose guarantees you have used without proving. The next section, **Distributed systems**, explains the theory underneath: time and clocks, replication, consensus with Paxos and Raft, consistency models, partitioning and failure detection, so you can reason about why these systems behave as they do and what they cannot promise.
