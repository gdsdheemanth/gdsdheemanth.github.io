This section is about the first step of every pipeline: getting data out of the systems that create it and into your platform. It assumes you know what a pipeline, a warehouse and a lakehouse are (Data engineering foundations, Data warehousing, Data lakes and lakehouses), that you can read SQL and simple Python, and that you have met Kafka, Pub/Sub and event streams in Streaming systems and Event-driven architecture. Where a topic needs a database's internals (write-ahead logs, replication), it explains them again briefly.

Ingestion sounds simple ("copy the data") and is where most production incidents start. Sources change without warning, APIs throttle you, files arrive late or half-written, devices go offline and replay a day of readings at once, and databases delete rows that a nightly copy never sees. A good ingestion layer is boring because it has been designed for all of that.

**What you will be able to do afterwards**

- Choose between full snapshots, incremental extracts and change data capture (CDC) for a source, and explain what each one misses.
- Run log-based CDC from PostgreSQL, MySQL or Oracle with Debezium or Datastream, and land it as a correct, current table in BigQuery or a lakehouse.
- Explain how Kafka Connect workers, tasks, converters and transforms fit together, and operate a connector.
- Ingest from SaaS APIs, GraphQL, webhooks, files (including SFTP and EDI), logs, clickstreams, IoT devices and market data feeds, with retries, deduplication and replay.
- Size an ingestion system for peak load, handle back-pressure, and stream into Iceberg or Delta tables without drowning in small files.
- Put validation and anomaly gates at the edge, and decide when a "zero-ETL" integration can replace a pipeline.

**The big picture.** Picture a port. Ships (sources) arrive on very different schedules: container ships once a night (bulk files and snapshots), ferries every hour (incremental extracts), and a constant stream of small boats (events, CDC, sensors). The port has to unload all of them without losing or double-counting cargo, check what is inside, record where everything came from, and put it in the warehouse in a known place. The port does not decide what the cargo is used for; that is the job of the pipelines and transformations in the next section. But if the port loses a container or stamps the wrong date on it, nothing downstream can fix it. Every topic below is one kind of ship and the dock built for it.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Strategies

Before choosing a tool you choose a **strategy**: how you will find out what changed in a source since last time, and how strictly you will check the shape of what arrives. These two decisions drive everything else: how much load you put on the source, whether deletes ever reach the warehouse, how you recover from a bug, and how painful a schema change will be.

The group starts with the three basic ways to copy a table (full, incremental, CDC) and the two attitudes to schemas (check on write, interpret on read). It then goes deep on change data capture, because CDC is how most serious platforms copy operational databases today: the general idea, Debezium as the open-source standard, Kafka Connect as the framework it usually runs in, and the end-to-end patterns for replicating a whole database into a lake or warehouse.

### Bulk vs Incremental vs CDC Ingestion

::: plain
There are three ways to keep a copy of a table up to date. You can **photocopy the whole book** every night (a full snapshot). You can ask "**which pages changed since yesterday?**" by looking at the date written on each page (incremental extraction). Or you can **read the author's diary**, where every single edit is written down in order, including pages torn out (change data capture). The photocopy is simple but slow, the date check is cheap but misses torn-out pages, and the diary sees everything but needs special access.
:::

[[fig:ingestion_modes]]

**Full snapshot (bulk) ingestion** reads the entire source table every run and replaces the copy. It is the easiest strategy to get right: no state to remember, deletes handled automatically (a deleted row is simply absent from the new copy), and a bad run is fixed by running again. Its cost grows with the table, not with the change. A 2 GB dimension table copied nightly is fine; a 3 TB orders table copied hourly is not, both for your bill and for the source database, which has to scan everything while serving customers.

```sql
-- BigQuery: replace yesterday's copy atomically with today's snapshot
CREATE OR REPLACE TABLE raw.customers AS
SELECT * FROM staging.customers_snapshot_20260302;
```

**Incremental (watermark) extraction** fetches only rows changed since the last run, using a column that moves forward on every change, usually `updated_at` or an increasing ID. The pipeline stores the highest value it has seen, the **high watermark**, and asks for rows above it next time.

```python
import sqlite3

src = sqlite3.connect(":memory:")
src.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT, updated_at TEXT)")
src.executemany("INSERT INTO customers VALUES (?, ?, ?)", [
    (1, "a@x.com", "2026-03-01 09:00"),
    (2, "b@x.com", "2026-03-01 09:30"),
    (3, "c@x.com", "2026-03-01 10:00"),
])

def extract_since(watermark):
    rows = src.execute(
        "SELECT id, email, updated_at FROM customers WHERE updated_at > ? ORDER BY updated_at",
        (watermark,)).fetchall()
    new_wm = rows[-1][2] if rows else watermark
    return rows, new_wm

rows, wm = extract_since("1970-01-01 00:00")
print(len(rows), "rows, watermark", wm)      # 3 rows, watermark 2026-03-01 10:00

# Between runs: one update, one delete, and two updates of the same row
src.execute("UPDATE customers SET email='b2@x.com', updated_at='2026-03-01 11:00' WHERE id=2")
src.execute("UPDATE customers SET email='b3@x.com', updated_at='2026-03-01 11:05' WHERE id=2")
src.execute("DELETE FROM customers WHERE id=3")

rows, wm = extract_since(wm)
print(rows)        # [(2, 'b3@x.com', '2026-03-01 11:05')]
# The delete of id 3 and the intermediate email b2@x.com are invisible.
```

The example shows the two built-in blind spots of watermark extraction: **hard deletes** never appear (the row is gone, so no query can return it), and **intermediate states** are lost (you only see the latest version at extraction time). It also depends on the application always setting `updated_at`; a bulk fix run directly in SQL by a developer who forgets it is silently skipped.

**Change data capture (CDC)** reads the database's own record of changes instead of querying the tables. Every serious database writes each change to a log before applying it (PostgreSQL's write-ahead log or WAL, MySQL's binary log or binlog, Oracle's redo log), because it needs the log for crash recovery and replication. A CDC tool reads that log and emits one event per insert, update and delete, in commit order, with the old and new values. Nothing is missed, the load on the source is small (reading a log is far cheaper than scanning tables), and latency drops to seconds. The price is operational: you need privileges and configuration on the source, a running process that tracks its position in the log, and a pipeline that can apply a stream of changes correctly.

| | Full snapshot | Incremental (watermark) | Log-based CDC |
|---|---|---|---|
| Sees hard deletes | Yes (by absence) | No | Yes |
| Sees every intermediate change | No | No | Yes |
| Load on source | High: full scan | Low to medium: index range scan | Very low: reads the log |
| Typical latency | Hours | Minutes to hours | Seconds |
| State to keep | None | The watermark | Log position (offset) |
| Source changes needed | None | A reliable `updated_at` and an index on it | Log settings, privileges, sometimes a replication slot |
| Recovery from a bug | Rerun | Reset the watermark | Re-snapshot, then resume the log |
| Tools | `bq load`, Spark, Storage Transfer | Airflow plus SQL, Fivetran, Airbyte | Debezium, Datastream, GoldenGate |

The scope of this course uses specific names for log readers: **PostgreSQL logical decoding** with replication slots (often written "pg_logical" after the older pglogical extension), **MySQL binlog streaming** (the CDC tool connects as if it were a replica), and **Oracle GoldenGate** (Oracle's commercial replication product; LogMiner and XStream are the other Oracle options). Later topics cover each. On Google Cloud, **Datastream** is the managed CDC service for MySQL, PostgreSQL, Oracle, SQL Server, MongoDB and others into BigQuery and Cloud Storage; Pipelines on Google Cloud covers it in depth.

> **Pitfall:** A watermark query `WHERE updated_at > :last` can miss rows that commit late. A transaction that started at 10:59:58, set `updated_at` then, and committed at 11:00:03 becomes visible after a run at 11:00:00 already advanced the watermark to 10:59:59. Re-read a small overlap (for example the last 5 minutes) and deduplicate on the primary key, or use CDC.

> **Tip:** Many teams combine strategies: CDC for large, fast-changing tables, nightly snapshots for small reference tables, and a weekly full snapshot of the CDC tables to compare counts and catch drift.

::: deeper
**Watermark design details.** Use a column the database sets (a trigger or a default), not the application, if you can. Store the watermark only after the batch is durably loaded, so a crash re-reads rather than skips. Prefer `>=` with deduplication over `>` when timestamps can tie. Increasing integer IDs only catch inserts, never updates. Time zones and clock changes on the source can make `updated_at` go backwards; store and compare in UTC.

**Detecting deletes without CDC.** Options, from cheapest: ask the application to use **soft deletes** (`deleted_at` column, so deletes become updates); periodically extract only the primary keys and anti-join against your copy; or compare per-partition counts and hashes and re-copy the partitions that differ.

**Snapshot isolation matters.** A full extract that runs for 40 minutes must read a consistent snapshot (`REPEATABLE READ` in PostgreSQL, `--single-transaction` in `mysqldump`), otherwise rows updated during the extract can appear twice or not at all. CDC tools do the same for their initial snapshot and record the log position at which the snapshot was taken, so streaming can resume from exactly that point.

**Cost model.** Snapshot cost is proportional to table size times frequency; incremental cost to the change rate; CDC to the change rate plus a constant for the always-on reader. For a 500 GB table that changes 1 GB per day, an hourly snapshot reads 12 TB per day while CDC reads about 1 GB of changes.

> **Interview:** "How would you ingest a 2 TB PostgreSQL table that receives 5 million updates and some deletes per day, for analytics that needs 15-minute freshness?" Log-based CDC (Datastream or Debezium) after a consistent initial snapshot, landing in BigQuery with merge semantics; monitor replication slot lag; reconcile counts weekly. Explain why watermark extraction is wrong here: it misses deletes and puts index-scan load on the primary every 15 minutes.
:::

::: check
?? Why can a watermark-based incremental extract never see a hard delete?
It queries the current table for rows whose `updated_at` is past the watermark. A deleted row no longer exists in the table, so no query can return it. Only a snapshot (by absence), soft deletes or the change log can reveal it.

?? What does log-based CDC read, and why is it cheap for the source?
It reads the database's transaction log (WAL, binlog or redo log), which the database writes anyway for recovery and replication. Reading sequentially from the log costs far less than scanning or index-searching the tables.

?? A run at 11:00:00 used `updated_at > '10:59:00'` and saved 10:59:59 as the new watermark. How can a row be lost?
A transaction that set `updated_at` to 10:59:58 but committed after 11:00:00 was invisible to that run and is below the new watermark, so the next run skips it. Re-reading an overlap window with deduplication fixes it.
:::

::: resources
- Docs: [Debezium documentation](https://debezium.io/documentation/reference/stable/) · how log-based CDC works for each database, with the event format explained
- Docs: [Datastream overview](https://docs.cloud.google.com/datastream/docs/overview) · Google Cloud's managed CDC service, its sources and destinations
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley; chapter 7 compares ingestion patterns, push versus pull and full versus incremental
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; chapter 11 explains change data capture and logs as the source of truth
- Blog: [Change data capture: what it is and how it works](https://www.striim.com/blog/change-data-capture-cdc-what-it-is-and-how-it-works/) · a vendor overview comparing CDC methods with bulk loading
:::

### Schema-on-Read vs Schema-on-Write

::: plain
**Schema-on-write** is a strict receptionist: every parcel is checked against a list (this field is a number, that one is a date) before it is allowed into the building, and anything that does not match is sent back. **Schema-on-read** is a big storeroom where everything is accepted as it is, and the shape is decided later by whoever opens a parcel. The first keeps the inside tidy but turns away surprises; the second never loses anything but leaves the sorting to every reader.
:::

A **schema** is the description of the data's shape: field names, types, which fields may be missing, and nested structures. The question is *when* it is enforced.

**Schema-on-write** checks data against a declared schema at load time. Relational databases and BigQuery native tables work this way: a `bq load` with a declared schema fails if a column that should be `INT64` contains `"three"`. Readers can then trust types and query fast, because the storage format is typed and columnar. The cost is rigidity: when the source adds a field or changes a type, ingestion breaks until someone updates the schema.

**Schema-on-read** stores raw data as it arrived (JSON lines, CSV, logs) and applies a schema only when a query reads it. Data lakes made this popular: land everything in Cloud Storage first, decide later. Nothing is rejected, so you never lose data to a schema mismatch, and different readers can interpret the same raw files differently. The cost moves to every reader: each query must parse, cast and handle bad values, and two teams may parse the same field two different ways. This is sometimes called **late binding**: the meaning of the bytes is bound as late as possible.

```python
import json

raw = [
    '{"id": 1, "amount": 40, "country": "IN"}',
    '{"id": 2, "amount": "15.50", "country": "NO"}',                 # amount sent as text
    '{"id": 3, "amount": 22, "country": "IN", "coupon": "SPRING"}',  # new field
]

SCHEMA = {"id": int, "amount": float, "country": str}

def validate(rec):
    problems = []
    for name, typ in SCHEMA.items():
        value = rec.get(name)
        ok = isinstance(value, typ) or (typ is float and isinstance(value, int))
        if not ok:
            problems.append(f"{name} is not {typ.__name__}")
    extra = sorted(set(rec) - set(SCHEMA))
    if extra:
        problems.append(f"unknown fields {extra}")
    return problems

# Schema-on-write: check before storing, reject what does not fit
for line in raw:
    rec = json.loads(line)
    print(rec["id"], validate(rec) or "ok")
# 1 ok
# 2 ['amount is not float']
# 3 ["unknown fields ['coupon']"]

# Schema-on-read: store every line as is, apply types only when reading
def read_amount(line):
    try:
        return float(json.loads(line).get("amount"))
    except (TypeError, ValueError):
        return None

print(sum(read_amount(l) for l in raw))   # 77.5
```

In practice almost every platform uses **both**, in layers. The raw (bronze) layer is schema-on-read: keep the bytes exactly as received so you can always reprocess. The cleaned (silver) layer is schema-on-write: typed tables with enforced rules. The question is only where the boundary sits and what happens to records that fail.

In BigQuery the raw layer can be a table with a `JSON` column, and the typed layer is derived from it:

```sql
CREATE TABLE IF NOT EXISTS raw.orders_json (
  ingest_ts TIMESTAMP, source_file STRING, payload JSON
) PARTITION BY DATE(ingest_ts);

CREATE OR REPLACE TABLE clean.orders AS
SELECT
  SAFE_CAST(JSON_VALUE(payload, '$.id') AS INT64)        AS order_id,
  SAFE_CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC)  AS amount,   -- "15.50" and 15.5 both work
  JSON_VALUE(payload, '$.country')                       AS country,
  JSON_VALUE(payload, '$.coupon')                        AS coupon    -- NULL for old records
FROM raw.orders_json;
```

**Schema drift** is the name for unplanned changes in the shape of incoming data: new fields, removed fields, renamed fields, changed types, a field that is sometimes a string and sometimes an object. Strategies, from most permissive to strictest:

| Strategy | What happens on drift | Good for |
|---|---|---|
| Land raw, parse downstream | Nothing breaks at ingestion; parsers adapt later | Third-party APIs, logs, early exploration |
| Auto-evolve additive changes | New columns added automatically (`--schema_update_option=ALLOW_FIELD_ADDITION` on `bq load`, Delta `mergeSchema`, Iceberg schema evolution) | Internal sources that only add fields |
| Rescue column | Unknown or mistyped fields go into a JSON or map column instead of being dropped | Semi-trusted sources where you still want typed columns |
| Validate and quarantine | Bad records go to a dead-letter table with the error; good ones load | Critical tables with known contracts |
| Reject the batch | The whole load fails and alerts | Financial or regulatory feeds where partial data is worse than none |

**Schema inference** tools guess a schema from a sample: BigQuery `--autodetect`, Spark's `inferSchema`, DuckDB's `read_csv` sniffer, Knowledge Catalog (formerly Dataplex Universal Catalog) discovery of files in Cloud Storage. They are great for exploration and dangerous for production: a sample of 500 rows decides that a postcode column is an integer (losing leading zeros) or that a column that happens to be empty is a string.

**Schema registry at ingestion.** For event streams, the strongest form of schema-on-write is a **schema registry** (Confluent Schema Registry, Apicurio, or the schemas built into Pub/Sub). Producers serialise with a registered Avro, Protobuf or JSON Schema, the registry enforces a compatibility rule (backward, forward, full) on every new version, and consumers can always decode old and new messages. Pub/Sub topics can have a schema attached, and messages that do not match are rejected at publish time. Streaming systems covers registries in depth; Pipelines and transformation covers compatibility rules and data contracts.

> **Pitfall:** Turning on autodetect for a production CSV feed. The day a file arrives with a text value in a column that was inferred as `INT64`, the load fails at 3 a.m. Declare schemas explicitly for anything scheduled.

> **Tip:** Always keep the raw payload, even when you validate. When a parser bug is found six months later, you can rebuild the clean tables from raw instead of asking the source to resend history.

::: deeper
**Why typed storage is faster.** Columnar formats (Parquet, ORC, BigQuery's Capacitor) store each column with a known type, so they can use dictionary and run-length encoding, keep min and max statistics per block and skip blocks a query does not need. A JSON blob defeats all of that: every query decompresses and parses whole documents. BigQuery's `JSON` type helps by shredding fields into virtual columns internally, but a typed column is still cheaper to scan and easier to cluster on.

**Nested evolution is harder than flat evolution.** Adding a field inside a repeated record (`items[].discount`) is additive and safe in Avro, Parquet and BigQuery. Changing a field from a scalar to a record, or renaming a field, is a breaking change everywhere: in Avro it needs an alias, in Iceberg a rename is safe because columns are tracked by ID rather than name, while in Hive-style Parquet tables (matched by name) it silently reads as NULL.

**Type widening.** Safe widenings (`INT32` to `INT64`, `FLOAT` to `DOUBLE`, adding precision to a decimal) are supported by Iceberg and Delta (Delta's type widening feature); narrowing never is. BigQuery allows relaxing a column from `REQUIRED` to `NULLABLE` and adding columns, but changing a type needs a rewrite (`ALTER COLUMN ... SET DATA TYPE` supports only some coercions).

> **Interview:** "Schema-on-read or schema-on-write for a new data platform?" Both, layered: schema-on-read for the immutable raw landing zone, so nothing is ever lost and you can reprocess; schema-on-write with explicit schemas, validation and quarantine for the curated layer people query. Name the drift policy per source and who is alerted when it triggers.
:::

::: check
?? What is the main benefit of schema-on-read at the raw layer?
Nothing is rejected or lost when the source changes shape, and the original bytes are kept, so you can reprocess history with a corrected parser at any time.

?? Why is schema autodetection risky for a scheduled production load?
It guesses types from a sample. A later file can contain values that do not fit the guess (text in an inferred integer column, leading zeros in postcodes), causing failed loads or silently wrong values.

?? What is a "rescue column"?
A JSON or map column that collects fields the schema does not know or values that fail to cast, so the typed columns stay clean while no information is thrown away.
:::

::: resources
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley on schemas, schema evolution and the raw-to-curated layering
- Docs: [Specifying a schema in BigQuery](https://docs.cloud.google.com/bigquery/docs/schemas) · explicit schemas, autodetection and nested fields
- Docs: [Modifying table schemas](https://docs.cloud.google.com/bigquery/docs/managing-table-schemas) · which schema changes BigQuery allows in place and which need a rewrite
- Docs: [Pub/Sub schemas](https://docs.cloud.google.com/pubsub/docs/schemas) · enforcing Avro or Protocol Buffer schemas at publish time, with revisions
- Docs: [Iceberg schema evolution](https://iceberg.apache.org/docs/latest/evolution/) · why column IDs make adds, drops and renames safe
- Docs: [Schema Registry overview](https://docs.confluent.io/platform/current/schema-registry/index.html) · compatibility modes and how producers and consumers use a registry
:::

### Change Data Capture (CDC)

::: plain
Change data capture means **listening to a database instead of asking it questions**. Rather than querying "what changed?" every hour, you subscribe to the database's own change journal and receive a message every time a row is inserted, updated or deleted, a few seconds after it happens. It is like getting a text message for every transaction on your bank account instead of downloading the whole statement each night.
:::

CDC turns a database table into a **stream of change events**. Each event says which table and row changed, what kind of change it was, the values before and after, and where in the source's history it happened (a log position or transaction ID). Downstream systems apply those events to keep a copy current, or keep the events themselves as a history.

There are four ways to capture changes, and they differ a lot in what they see and what they cost:

| Method | How it works | Sees deletes | Source impact | Notes |
|---|---|---|---|---|
| Timestamp-based | Query rows with `updated_at` past a watermark | No | Medium (queries) | Really incremental extraction; called "query-based CDC" |
| Trigger-based | Database triggers copy every change into an audit table | Yes | High: extra write per change, inside the transaction | Works on any database with triggers |
| Log-based | Read the transaction log (WAL, binlog, redo) | Yes | Very low | The standard today; needs log configuration |
| Diff-based | Compare two snapshots | Yes | High (two full reads) | Last resort for sources without logs |

**Trigger-based CDC** is easy to understand and runs anywhere, so it is worth seeing once:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER);
CREATE TABLE accounts_changes (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,   -- order of changes
  op TEXT, id INTEGER, old_balance INTEGER, new_balance INTEGER
);
CREATE TRIGGER acc_ins AFTER INSERT ON accounts BEGIN
  INSERT INTO accounts_changes (op, id, new_balance) VALUES ('c', NEW.id, NEW.balance);
END;
CREATE TRIGGER acc_upd AFTER UPDATE ON accounts BEGIN
  INSERT INTO accounts_changes (op, id, old_balance, new_balance)
  VALUES ('u', NEW.id, OLD.balance, NEW.balance);
END;
CREATE TRIGGER acc_del AFTER DELETE ON accounts BEGIN
  INSERT INTO accounts_changes (op, id, old_balance) VALUES ('d', OLD.id, OLD.balance);
END;
""")
db.execute("INSERT INTO accounts VALUES (1, 100)")
db.execute("UPDATE accounts SET balance = 80 WHERE id = 1")
db.execute("UPDATE accounts SET balance = 95 WHERE id = 1")
db.execute("DELETE FROM accounts WHERE id = 1")

for row in db.execute("SELECT * FROM accounts_changes ORDER BY seq"):
    print(row)
# (1, 'c', 1, None, 100)
# (2, 'u', 1, 100, 80)
# (3, 'u', 1, 80, 95)
# (4, 'd', 1, 95, None)
```

Every change, including the delete and both intermediate balances, is captured. But each write to `accounts` now costs a second write, inside the same transaction, which slows the application, and the audit table must itself be extracted and pruned. That is why production systems moved to **log-based CDC**: the database already writes every committed change to its log, so a reader can follow the log with almost no extra cost.

**How log-based CDC works.** The CDC process connects to the database using the same protocol a replica would use:

- **PostgreSQL**: set `wal_level = logical`. A **replication slot** remembers how far the reader has consumed and stops the server from deleting WAL the reader still needs. A **logical decoding plugin** (`pgoutput`, built in since PostgreSQL 10) turns WAL records into row changes for the tables in a **publication**.
- **MySQL and MariaDB**: set `binlog_format = ROW` (the default since MySQL 8.0) and `binlog_row_image = FULL`. The reader registers as a replica and streams the binary log, tracking its position by file and offset or, better, by **GTID** (global transaction ID).
- **Oracle**: LogMiner (built in, reads redo and archive logs), XStream (lower latency, needs a GoldenGate licence) or GoldenGate itself.
- **SQL Server**: the built-in CDC feature, which a SQL Server Agent job fills from the transaction log into change tables.
- **MongoDB**: change streams, built on the replica set's oplog.

The output is a stream of events in commit order. Most tools publish them to Kafka or Pub/Sub, one topic per table, keyed by primary key so all changes to one row land in one partition and stay in order. **Kafka Connect** source connectors (next two topics) are the most common way to run this; Debezium is the open-source standard set of connectors.

A typical event, in Debezium's format:

```json
{
  "before": {"id": 1, "status": "new",  "amount": 40.00},
  "after":  {"id": 1, "status": "paid", "amount": 40.00},
  "source": {"connector": "postgresql", "db": "shop", "table": "orders",
             "lsn": 24023128, "txId": 5583, "ts_ms": 1772442061000},
  "op": "u",
  "ts_ms": 1772442061412
}
```

`op` is `c` (create), `u` (update), `d` (delete) or `r` (read, a row from the initial snapshot). `source.ts_ms` is when the change was committed in the database; the outer `ts_ms` is when the connector processed it, so the difference is your capture lag.

**What consumers do with the stream.** Three uses cover most cases: **replicate** (apply changes to keep a current copy in BigQuery or a lakehouse table), **keep history** (append every event to build slowly changing dimensions or audits), and **react** (invalidate a cache, update a search index, trigger a workflow). The Event-driven architecture section showed the **transactional outbox**, which uses CDC to publish domain events reliably.

> **Pitfall:** A CDC stream is not the same as domain events. A row change in `order_lines` says nothing about the business meaning ("order placed"), and it exposes the source's internal table design to every consumer. For integrations between services, publish intentional events through an outbox; use raw CDC for analytics replication.

> **Tip:** Measure **capture lag** (commit time to event time) and **apply lag** (event to queryable in the destination) separately. When freshness slips, you need to know which half is slow.

::: deeper
**Ordering and transactions.** Log-based CDC preserves commit order per source database, but most pipelines then split events by table and partition by key. Order is kept per key, not across tables, so a consumer can see an `order_lines` insert before the `orders` insert from the same transaction. If you need transactional consistency downstream, use the transaction metadata some tools emit (Debezium's `provide.transaction.metadata` adds transaction boundary events) or apply changes per transaction.

**Before images.** By default PostgreSQL logs only the primary key for the old row on update and delete. Set `ALTER TABLE orders REPLICA IDENTITY FULL` if consumers need full before images, at the cost of more WAL. Tables without a primary key cannot be replicated correctly at all without it.

**What CDC cannot see.** `TRUNCATE` is captured by some decoders and not others; DDL (schema changes) is handled very differently per database (MySQL logs DDL in the binlog, PostgreSQL logical decoding does not emit DDL); bulk loads that bypass logging (Oracle `NOLOGGING`, some `COPY` variants on unlogged tables) are invisible.

**Delivery guarantees.** Most CDC pipelines are **at-least-once**: after a crash the reader resumes from its last committed offset and re-emits some events. Design consumers to be idempotent, using the log position as a version: apply an event only if its position is higher than the one already applied for that key.

> **Interview:** "Why not just use triggers?" Triggers add a synchronous write to every transaction, run inside the application's latency budget, are easy to disable by accident, and must be maintained on every table. Log-based CDC reads what the database already writes, asynchronously, with no change to the application's transactions.
:::

::: check
?? Name two reasons log-based CDC replaced trigger-based CDC in most systems.
It adds almost no load to the source's transactions (the log is written anyway), and it needs no triggers on each table. Triggers add a synchronous extra write per change and are fragile to maintain.

?? What does a PostgreSQL replication slot do, and why can it be dangerous?
It records how far a logical reader has consumed and prevents the server from removing WAL the reader still needs. If the reader stops, WAL accumulates without limit and can fill the disk.

?? Why are CDC events usually keyed by primary key when published to Kafka or Pub/Sub?
So every change to the same row goes to the same partition (or ordering key) and is consumed in order, which lets the consumer apply them correctly.
:::

::: resources
- Docs: [Debezium documentation](https://debezium.io/documentation/reference/stable/) · connector-by-connector setup, event formats and guarantees
- Docs: [PostgreSQL logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) · replication slots, output plugins and the protocol CDC tools use
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; chapter 11 on change data capture, event sourcing and keeping systems in sync
- Article: [What is change data capture?](https://www.confluent.io/learn/change-data-capture/) · Confluent's overview of CDC methods and use cases with Kafka
- Paper: [DBLog: A Watermark Based Change-Data-Capture Framework](https://arxiv.org/abs/2010.12597) · Netflix's design for interleaving snapshots with log events without locking
- Docs: [Datastream: key concepts and features](https://docs.cloud.google.com/datastream/docs/behavior-overview) · how a managed CDC service handles snapshots, ordering and events
:::

### Debezium — CDC Deep Dive

::: plain
**Debezium** is a free, open-source set of programs that read the change logs of popular databases (PostgreSQL, MySQL, MongoDB, Oracle, SQL Server and more) and turn every change into a message. Think of it as a court stenographer sitting beside each database: it writes down every change in a standard format, remembers the last line it wrote so it can pick up after a break, and makes an initial copy of everything that was already there before it started.
:::

Debezium is a Red Hat-led open-source project (Apache 2.0 licence) and the de facto standard for open-source CDC. Its 3.x releases need Java 17 or later. It ships as:

- **Kafka Connect source connectors**, the classic deployment: Debezium runs inside a Kafka Connect cluster and writes to Kafka topics (next topic explains Connect).
- **Debezium Server**, a standalone application that runs the same connectors without Kafka and sends events to other systems, including **Google Cloud Pub/Sub**, Redis Streams, Apache Pulsar and plain HTTP endpoints. On Google Cloud this is a simple way to stream CDC into Pub/Sub from a container on GKE or Compute Engine.
- **Debezium Engine**, an embeddable Java library for applications that want to consume changes directly.

**Connector architecture.** Each connector has the same three jobs, implemented per database:

1. **Snapshot**: read the existing rows of the captured tables so the stream starts from a complete copy.
2. **Stream**: follow the log from the exact position the snapshot corresponds to, converting log records into change events.
3. **Track offsets**: periodically record the log position it has published up to (PostgreSQL LSN, MySQL binlog file and position or GTID set, Oracle SCN), so a restart resumes without gaps.

A PostgreSQL connector registered on Kafka Connect:

```json
{
  "name": "shop-pg",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "10.0.0.12",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${file:/secrets/shop-pg.properties:password}",
    "database.dbname": "shop",
    "topic.prefix": "shop",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_shop",
    "publication.name": "dbz_shop",
    "table.include.list": "public.orders,public.customers",
    "snapshot.mode": "initial",
    "heartbeat.interval.ms": "10000",
    "signal.data.collection": "public.debezium_signal"
  }
}
```

Events for `public.orders` go to the topic `shop.public.orders`. The password comes from a Kafka config provider rather than being written into the config (on Google Cloud, mount it from Secret Manager).

The same connector under Debezium Server, writing to Pub/Sub (`application.properties`):

```text
debezium.sink.type=pubsub
debezium.sink.pubsub.project.id=acme-prod
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.database.hostname=10.0.0.12
debezium.source.database.dbname=shop
debezium.source.topic.prefix=shop
debezium.source.plugin.name=pgoutput
debezium.source.table.include.list=public.orders,public.customers
```

Pub/Sub topics named `shop.public.orders` and `shop.public.customers` must exist. In a container, configure a durable offset store (Debezium supports file, Kafka, Redis and JDBC stores); a file inside an ephemeral container loses the position on restart.

**Snapshot modes.** `snapshot.mode` controls what happens on first start and on restart. The important values: `initial` (default: snapshot, then stream), `initial_only` (snapshot and stop), `no_data` (capture schema only and stream from now, for when history is loaded another way), `when_needed` (snapshot again if the stored offset is no longer available in the log), and `always`. Older releases called `no_data` `schema_only`.

**Incremental snapshots.** A classic snapshot reads whole tables at the start and blocks streaming until it finishes, which for a 2 TB table can take a day. **Incremental snapshots**, based on Netflix's DBLog watermark algorithm, read a table in primary-key chunks *while* streaming continues. For each chunk the connector writes a low watermark to a signal table, reads the chunk, writes a high watermark, and drops any chunk row that the log also changed between the two watermarks (the log event is newer). They can be paused, resumed and triggered on demand for one table, which is how you backfill a newly added table or repair a broken one:

```sql
-- PostgreSQL: ask the running connector to re-snapshot one table, in chunks
INSERT INTO debezium_signal (id, type, data)
VALUES ('resnap-orders-2026-03-02', 'execute-snapshot',
        '{"data-collections": ["public.orders"], "type": "incremental"}');
```

**Schema changes.** For PostgreSQL, each event carries the schema of the row as it was decoded, so added columns simply appear. For MySQL, the binlog contains DDL statements, and the connector keeps a **schema history topic** (configured with `schema.history.internal.kafka.topic`) recording every DDL so that after a restart it can interpret old binlog events with the schema that applied at that time. Losing that topic means the connector cannot restart without a new snapshot. With Avro or Protobuf and a schema registry, each table's event schema is versioned and compatibility rules apply to source DDL too.

**Applying events.** A consumer that keeps a current copy applies each event by key and skips anything older than what it already has:

```python
import json

# Debezium-style change events (envelope trimmed to the important fields)
events = [
    {"op": "r", "before": None, "after": {"id": 1, "status": "new"},     "source": {"lsn": 100}},
    {"op": "r", "before": None, "after": {"id": 2, "status": "new"},     "source": {"lsn": 100}},
    {"op": "u", "before": {"id": 1, "status": "new"}, "after": {"id": 1, "status": "paid"}, "source": {"lsn": 210}},
    {"op": "c", "before": None, "after": {"id": 3, "status": "new"},     "source": {"lsn": 220}},
    {"op": "d", "before": {"id": 2, "status": "new"}, "after": None,     "source": {"lsn": 230}},
    {"op": "u", "before": {"id": 1, "status": "new"}, "after": {"id": 1, "status": "paid"}, "source": {"lsn": 210}},  # redelivered
]

table, applied_lsn = {}, {}
for e in events:
    row = e["after"] or e["before"]
    key, lsn = row["id"], e["source"]["lsn"]
    if lsn < applied_lsn.get(key, -1):
        continue                       # older than what we already applied: skip
    applied_lsn[key] = lsn
    if e["op"] == "d":
        table.pop(key, None)
    else:                              # r (snapshot read), c (create), u (update)
        table[key] = e["after"]

print(json.dumps(table, sort_keys=True))
# {"1": {"id": 1, "status": "paid"}, "3": {"id": 3, "status": "new"}}
```

After a delete, Debezium also emits a **tombstone** (a message with the key and a null value) so Kafka log compaction can eventually remove the key entirely. Many sinks use the `ExtractNewRecordState` transform to flatten the envelope into just the `after` row plus metadata fields such as `__op` and `__source_ts_ms`.

> **Pitfall:** A PostgreSQL connector that is stopped for a long weekend keeps its replication slot, and the primary keeps every WAL segment since. Monitor slot lag, set `max_slot_wal_keep_size` (PostgreSQL 13+) as a safety valve, and drop slots of retired connectors.

> **Pitfall:** On a quiet table in a busy database, the connector receives no events for its tables, so it never confirms a newer LSN and the slot holds WAL anyway. Heartbeats (`heartbeat.interval.ms`, plus `heartbeat.action.query` writing to a small heartbeat table) keep the confirmed position moving.

::: deeper
**Offset semantics.** Debezium commits offsets periodically (`offset.flush.interval.ms` on Connect), after events are sent. After a crash it replays from the last flushed offset, so duplicates are normal and consumers must be idempotent. With Kafka 3.3+ and Connect's exactly-once source support (KIP-618) enabled, offsets and records are written in one Kafka transaction; Debezium supports this for several connectors in recent releases, which removes duplicates in Kafka (not in downstream sinks that are not transactional).

**Oracle specifics.** The LogMiner adapter mines redo and archive logs with SQL against `V$LOGMNR_CONTENTS`, which is CPU-heavy on busy databases and needs supplemental logging (`ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`). The XStream adapter has lower latency but requires a GoldenGate licence. Long-running transactions force the connector to buffer uncommitted changes; Debezium can buffer in memory or in an embedded cache.

**MongoDB** uses change streams, resumable by a resume token stored as the offset. Update events contain only the changed fields unless pre and post images are enabled (MongoDB 6.0+).

**Failover.** If a PostgreSQL primary fails over to a standby, the logical slot did not exist on the standby before PostgreSQL 17. PostgreSQL 17 added failover slots (`failover = true` when creating the slot, plus `sync_replication_slots` on the standby). On older versions, plan for a re-snapshot (`when_needed`) or recreate the slot and accept a gap check.

> **Interview:** "How do you add a new table to a running Debezium connector without re-snapshotting everything?" Add it to `table.include.list`, then send an `execute-snapshot` signal of type `incremental` for just that table. The connector backfills it in chunks while streaming continues, deduplicating against the log with the DBLog watermark method.
:::

::: check
?? What are the three jobs of every Debezium connector?
Take an initial snapshot of existing rows, stream changes from the log starting exactly where the snapshot ended, and record offsets (log positions) so it can resume after a restart.

?? What problem do incremental snapshots solve compared with the initial snapshot?
They read a table in chunks while streaming continues, can be paused and resumed, and can be triggered for a single table later, instead of blocking streaming for the whole duration of a huge one-off snapshot.

?? Why does the MySQL connector need a schema history topic?
Binlog row events do not carry full schema information, so after a restart the connector must know which DDL applied at each log position. The history topic records every DDL so old events can be decoded correctly.
:::

::: resources
- Docs: [Debezium reference documentation](https://debezium.io/documentation/reference/stable/) · per-connector configuration, snapshot modes, signals and transforms
- Docs: [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html) · running connectors without Kafka, including the Pub/Sub sink
- Repo: [debezium/debezium](https://github.com/debezium/debezium) · the source code, issues and release notes
- Blog: [Incremental snapshots in Debezium](https://debezium.io/blog/2021/10/07/incremental-snapshots/) · how chunked snapshots interleave with the log, with diagrams
- Paper: [DBLog: A Watermark Based Change-Data-Capture Framework](https://arxiv.org/abs/2010.12597) · the algorithm behind incremental snapshots
- Docs: [Debezium tutorial](https://debezium.io/documentation/reference/stable/tutorial.html) · a hands-on walk-through: start a connector and watch change events arrive
:::

### Apache Kafka Connect — Internals

::: plain
**Kafka Connect** is a ready-made machine for moving data in and out of Kafka without writing a program. You pick a **connector** (a plug-in that knows one system, such as PostgreSQL, Cloud Storage or BigQuery), give it a small configuration file, and Connect runs it for you: it splits the work into parallel tasks, restarts them when they fail, and remembers how far each one got. Think of it as a power strip with standard sockets, where each connector is a plug for a different appliance.
:::

[[fig:ingestion_connect]]

Kafka Connect ships with Apache Kafka. It is a runtime for **connectors**, and every connector is one of two kinds:

- A **source connector** reads from an external system (a database log, files, an API) and writes records into Kafka topics. Debezium connectors are source connectors.
- A **sink connector** reads from Kafka topics and writes to an external system (BigQuery, Cloud Storage, Elasticsearch, a database).

**The moving parts.**

- A **worker** is a JVM process running the Connect runtime. Workers with the same `group.id` form a **Connect cluster**.
- A **connector** instance is a configured use of a connector plug-in (for example "the `shop-pg` Debezium connector"). It does not move data itself; it decides how to split the work.
- **Tasks** do the actual copying. A connector creates up to `tasks.max` tasks: a JDBC source might give each task a subset of tables, a sink gives each task a subset of topic partitions (sink tasks are ordinary Kafka consumers in one consumer group). Debezium's relational connectors use a single task per database, because one log must be read in order.
- **Converters** translate between Kafka's bytes and Connect's internal typed records (`Struct` plus schema): `JsonConverter`, `StringConverter`, `ByteArrayConverter`, and the Avro, Protobuf and JSON Schema converters that talk to a schema registry. Converters are set per worker and can be overridden per connector.
- **Single Message Transforms (SMTs)** modify each record in flight: add or drop fields, mask values, rename topics, convert timestamps, filter records. They are chained in order.
- **Internal topics** hold the cluster's state: `config.storage.topic` (connector configurations, one partition, compacted), `offset.storage.topic` (source connector positions, compacted) and `status.storage.topic` (connector and task states). Sink connectors track progress as ordinary consumer group offsets.

The record flow for a source connector is: task `poll()` returns records, SMTs transform each one, the converter serialises key and value, the worker's producer sends them to Kafka, and the source offset is committed to the offsets topic periodically. For a sink: the worker's consumer fetches, the converter deserialises, SMTs transform, the task `put()` writes to the external system, and the consumer offset is committed once the task confirms the data is flushed.

**Distributed vs standalone mode.**

| | Standalone | Distributed |
|---|---|---|
| Workers | One process | Many processes sharing a `group.id` |
| Offsets stored in | A local file | The offsets topic in Kafka |
| Configuration | Properties files passed on the command line | REST API, stored in the config topic |
| Fault tolerance | None: the process is a single point of failure | Tasks are rebalanced to surviving workers |
| Use for | Development, a single edge agent | Everything in production |

A distributed worker's key settings:

```text
bootstrap.servers=bootstrap.kafka-prod.europe-west1.managedkafka.acme-prod.cloud.goog:9092
group.id=connect-prod
config.storage.topic=_connect-configs
offset.storage.topic=_connect-offsets
status.storage.topic=_connect-status
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Connectors are managed through the REST API (port 8083 by default):

```bash
curl -s -X PUT -H "Content-Type: application/json" \
  --data @shop-pg.json localhost:8083/connectors/shop-pg/config      # create or update
curl -s localhost:8083/connectors/shop-pg/status                      # RUNNING, FAILED, PAUSED per task
curl -s -X POST "localhost:8083/connectors/shop-pg/restart?includeTasks=true&onlyFailed=true"
curl -s -X PUT localhost:8083/connectors/shop-pg/pause
```

**SMTs in practice.** A common chain for CDC into a warehouse sink: unwrap the Debezium envelope, then route all tables to topics with a clean naming scheme.

```json
"transforms": "unwrap,route",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.add.fields": "op,source.ts_ms",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": "shop\\.public\\.(.*)",
"transforms.route.replacement": "cdc.shop.$1"
```

Built-in Kafka SMTs include `InsertField`, `ReplaceField` (include, exclude, rename), `MaskField`, `Cast`, `TimestampConverter`, `ValueToKey`, `ExtractField`, `RegexRouter`, `TimestampRouter`, `HoistField`, `Flatten` and `Filter`. Since Kafka 2.6, **predicates** (`TopicNameMatches`, `HasHeaderKey`, `RecordIsTombstone`) apply a transform only to some records.

> **Pitfall:** SMTs are for light, per-record changes. Joins, lookups, aggregations or anything that needs state belong in a stream processor (Flink, Kafka Streams, Dataflow). A chain of ten SMTs is a stream-processing job hiding in a config file.

**Error handling.** By default a task fails on the first bad record (`errors.tolerance=none`). For sinks you can tolerate errors and route failures to a **dead letter queue** topic:

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dlq.bigquery-sink",
"errors.deadletterqueue.context.headers.enable": "true",
"errors.log.enable": "true"
```

The DLQ catches failures in conversion and transformation; failures inside the external system's write call depend on the connector's own retry settings. Source connectors have no DLQ setting.

**On Google Cloud.** Google Managed Service for Apache Kafka can run **Connect clusters** for you (generally available since October 2025): a managed, distributed Kafka Connect deployment attached to a managed Kafka cluster, with pre-packaged connector plug-ins for common targets such as BigQuery, Cloud Storage and Pub/Sub, plus MirrorMaker 2. Alternatively, run Connect yourself on GKE (Strimzi is the common Kubernetes operator) or use Confluent Cloud's managed connectors on Google Cloud.

> **Tip:** Keep one connector per source database or per destination dataset rather than one giant connector. Failures, restarts and config changes are then isolated, and the REST status tells you exactly which flow is broken.

::: deeper
**Rebalancing.** When a worker joins or leaves, or a connector is added, the cluster reassigns connectors and tasks. Since Kafka 2.3 Connect uses **incremental cooperative rebalancing**: only the affected tasks move, and a departed worker's tasks wait for `scheduled.rebalance.max.delay.ms` (default 5 minutes) in case it comes back, instead of stopping every task in the cluster (the old "stop the world" behaviour).

**Exactly-once.** Source connectors: Kafka 3.3 added exactly-once support (KIP-618), enabled with `exactly.once.source.support=enabled` on workers; records and offsets are then committed in one Kafka transaction, if the connector supports it. Sink connectors achieve exactly-once only if the sink is idempotent (upserts by key) or stores Kafka offsets atomically with the data (some JDBC and lakehouse sinks do this), and then rewinds the consumer to its own stored offsets on start.

**Converters and schemas.** `JsonConverter` with `schemas.enable=true` embeds the full schema in every message, which can triple message size; with `false` there is no schema at all and sinks that need types (BigQuery, JDBC) cannot infer them. Avro or Protobuf with a registry sends a small schema ID per message instead. A mismatch between the converter a producer used and the one a sink expects is the most common "my connector fails on every record" bug: the bytes are fine, the decoder is wrong.

**Plugin isolation.** Each plug-in lives in its own directory on `plugin.path` and gets its own class loader, so two connectors can depend on different versions of the same library.

**Sizing.** Sink parallelism is capped by the number of topic partitions; source parallelism by how the connector can split the work. Watch per-task metrics such as `source-record-poll-rate`, `sink-record-send-rate`, `offset-commit-avg-time-ms` and consumer lag.

> **Interview:** "Your BigQuery sink connector is FAILED. Walk through debugging." Check `/status` for the task's stack trace; classify it (converter error, SMT error, destination auth or quota, schema mismatch); for bad records enable the DLQ and inspect its headers; for destination errors check the connector's retry settings and BigQuery quotas; fix, then restart only failed tasks. Mention verifying consumer lag returns to normal afterwards.
:::

::: check
?? What is the difference between a connector and a task?
A connector is the configured instance that decides how the work is split. Tasks are the units that actually copy data; a connector can create up to `tasks.max` tasks, which the cluster spreads across workers.

?? Where does a distributed Connect cluster store source connector offsets, and why does that matter?
In the compacted offsets topic in Kafka. Any worker can resume a task from there, so tasks survive the loss of the worker that ran them.

?? A sink connector fails because one message cannot be deserialised. What configuration lets it continue while keeping the bad message?
`errors.tolerance=all` plus `errors.deadletterqueue.topic.name`, so failing records go to a dead letter topic (optionally with error context headers) and the task carries on.
:::

::: resources
- Docs: [Kafka Connect in the Apache Kafka documentation](https://kafka.apache.org/documentation/#connect) · the reference for workers, configuration, transforms and the REST API
- Docs: [Kafka Connect on Managed Service for Apache Kafka](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/kafka-connect-overview) · Google's managed Connect clusters and their connectors
- Docs: [Kafka Connect user guide](https://docs.confluent.io/platform/current/connect/userguide.html) · Confluent's practical guide to deploying and configuring Connect
- Blog: [Kafka Connect deep dive: converters and serialization explained](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/) · why converter mismatches break pipelines, with examples
- Blog: [Kafka Connect deep dive: error handling and dead letter queues](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/) · the tolerance settings and DLQ headers in practice
- Book: [Kafka: The Definitive Guide, 2nd edition](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/) · Shapira and others; chapter 9 covers Connect architecture and operation
:::

### Database Replication to Lake — Patterns

::: plain
Replicating a database into a lake or warehouse is like moving house while still living in it. First you pack and ship everything already in the old house (the **snapshot**). While the truck is on the road, you keep a notebook of everything you buy or throw away (the **change stream**). At the new house you unpack, then replay the notebook in order, and from then on you keep the two houses in sync. The trick is making sure nothing bought during the move is lost and nothing is unpacked twice.
:::

[[fig:ingestion_replica]]

The previous topics gave you the parts. This topic assembles them into the end-to-end pattern almost every company uses to copy operational databases (PostgreSQL, MySQL, Oracle, SQL Server) into BigQuery or a lakehouse.

**Step 1: snapshot plus CDC, stitched at a log position.** A correct replica needs both a complete starting copy and every change after it, with no gap and no overlap. All the serious tools do the same thing: take a consistent snapshot, record the log position it corresponds to, and start streaming from exactly that position. Per database:

- **PostgreSQL**: create a logical replication slot, which exports a snapshot and its LSN; read tables inside that snapshot; stream from the slot. Monitor what the slot retains:

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;
```

- **MySQL**: read under a consistent snapshot (or a brief global read lock), record the binlog file and position or GTID set, then stream the binlog from there. Set binlog retention (`binlog_expire_logs_seconds`) longer than your longest possible outage.
- **Oracle**: record the SCN (system change number), run a flashback query `AS OF SCN`, then mine redo with LogMiner (or GoldenGate) from that SCN.

**Step 2: land the raw change log.** Write every change event, unchanged, to an append-only **changelog table** (or bronze files) with its metadata: operation, source commit timestamp, log position, ingestion time. This is your replayable history and your audit trail.

**Step 3: build the current-state table.** Derive the replica that people query by taking the latest event per primary key and dropping deleted keys. The logic is small enough to check in SQLite:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE orders_changelog (id INTEGER, op TEXT, status TEXT, lsn INTEGER);
INSERT INTO orders_changelog VALUES
  (1, 'r', 'new',     100),   -- initial snapshot
  (2, 'r', 'new',     100),
  (1, 'u', 'paid',    210),
  (3, 'c', 'new',     220),
  (2, 'd', NULL,      230),
  (1, 'u', 'shipped', 240);
""")

current = db.execute("""
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY lsn DESC) AS rn
  FROM orders_changelog
)
SELECT id, status FROM ranked
WHERE rn = 1 AND op != 'd'          -- latest change per key, deleted keys dropped
ORDER BY id
""").fetchall()
print(current)   # [(1, 'shipped'), (3, 'new')]
```

In BigQuery you apply only recent changes with a `MERGE`, guarded by the log position so late or replayed events never overwrite newer data:

```sql
MERGE lake.orders AS t
USING (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_lsn DESC) AS rn
    FROM raw.orders_changelog
    WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
  ) WHERE rn = 1
) AS s
ON t.id = s.id
WHEN MATCHED AND s.op = 'd' AND s.source_lsn > t.source_lsn THEN DELETE
WHEN MATCHED AND s.op != 'd' AND s.source_lsn > t.source_lsn THEN
  UPDATE SET status = s.status, amount = s.amount, source_lsn = s.source_lsn
WHEN NOT MATCHED AND s.op != 'd' THEN
  INSERT (id, status, amount, source_lsn) VALUES (s.id, s.status, s.amount, s.source_lsn);
```

Lakehouse tables do the same with `MERGE INTO` in Spark or Flink on Iceberg, Delta or Hudi. **Datastream** offers both modes natively for BigQuery: **merge** mode keeps a current replica (using BigQuery's CDC upsert support with a configurable staleness), **append-only** mode keeps every change as a row, which you can model into history yourself.

**Step 4: decide what to replicate.** **Table filtering** keeps the replica useful and cheap: include lists rather than exclude lists (so a new table with personal data is not replicated by accident), excluding columns (`column.exclude.list` in Debezium, column selection in Datastream) for secrets and large blobs, and separate streams for very hot tables so they cannot delay the others.

**Step 5: propagate schema changes.** Additive changes (new nullable column) should flow automatically: Datastream adds columns to BigQuery tables, Debezium emits a new schema version. Destructive changes (drop, rename, type change) need a policy: pause and alert, or keep the old column and add the new one. Agree with source teams that renames are done as add-copy-drop, and give data engineering a review step on migrations of replicated tables.

| Concern | Typical choice |
|---|---|
| Initial load of a huge table | Incremental (chunked) snapshot, or a bulk export plus CDC from the export's log position |
| Keep history | Append-only changelog, then SCD type 2 built from it |
| Current state for BI | Merge by key, guarded by log position |
| Deletes | Hard delete in the replica, or soft delete flag (`_is_deleted`) if consumers need to see them |
| Ordering | Per key by log position, never by ingestion time |
| Reconciliation | Daily counts and checksums per table, source versus replica |

> **Pitfall:** Ordering changes by ingestion time or by the event's `updated_at`. Retries and parallel writers deliver events out of order; only the source log position (LSN, binlog position or GTID, SCN) reliably says which change is newer.

> **Tip:** Keep the changelog much longer than the replica's history needs. If a merge bug corrupts the replica, you rebuild it from the changelog without touching the source.

::: deeper
**Snapshot strategies for very large tables.** A consistent snapshot of a 5 TB table holds a long transaction open on the source, which in PostgreSQL prevents vacuum from cleaning dead rows and in MySQL keeps undo logs growing. Options: run the snapshot against a read replica (Debezium can snapshot from a replica for MySQL, and Datastream can read from Cloud SQL replicas for some sources), use chunked incremental snapshots that hold no long transaction, or export with the database's own bulk tool and start CDC from the export's recorded position.

**LinkedIn's Brooklin and Databus.** LinkedIn built Databus (an early CDC system with a relay and bootstrap service for snapshots) and later Brooklin, a general streaming bridge that moves change streams between databases, Kafka and other systems. Both solved the same snapshot-plus-stream stitching problem described here, and their designs influenced today's tools.

**Merge cost.** A `MERGE` that scans the whole target table every 5 minutes gets expensive. Partition and cluster the target by the key or by a date that changes rarely, restrict the source side to recent changes, or let the platform's native CDC handle it: BigQuery tables with a primary key and `max_staleness` apply upserts in the background, and Iceberg or Hudi merge-on-read tables write delete files instead of rewriting data files.

**Consistency across tables.** Two tables merged on different schedules can be inconsistent with each other for a while (an order line without its order). For finance-grade consistency, merge all tables up to a common log position, publish a "consistent as of LSN X" marker, and let readers query only up to it.

> **Interview:** "Design replication of 300 MySQL tables into BigQuery with 10-minute freshness and full history." Datastream (or Debezium to Pub/Sub or Kafka) in append-only mode into a changelog dataset; scheduled `MERGE` or native merge mode for current-state tables; SCD2 history models from the changelog in Dataform or dbt; include lists, column exclusions for personal data; binlog retention of several days; daily count reconciliation; alerting on capture lag and on schema changes.
:::

::: check
?? Why must the snapshot and the change stream be stitched at a recorded log position?
Changes made while the snapshot is being read would otherwise be lost (if streaming starts later) or applied twice in an inconsistent order (if it starts earlier). Starting the stream exactly at the snapshot's position gives a gap-free, overlap-free history.

?? Why does the `MERGE` compare `source_lsn` before updating?
Events can arrive late or be replayed after a failure. Comparing log positions ensures an older change never overwrites a newer one, which makes the merge idempotent and order-safe.

?? What is the difference between Datastream's merge and append-only modes?
Merge keeps a current-state replica of each table in BigQuery. Append-only writes every change as a new row with change metadata, so the full history is kept and you build current state or SCD2 tables yourself.
:::

::: resources
- Docs: [Datastream: write modes for BigQuery](https://docs.cloud.google.com/datastream/docs/destination-bigquery) · merge versus append-only, staleness and how tables are created
- Docs: [PostgreSQL logical replication](https://www.postgresql.org/docs/current/logical-replication.html) · publications, slots and the initial table synchronisation
- Docs: [Debezium MySQL connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html) · binlog settings, GTIDs, snapshots and the schema history topic
- Repo: [linkedin/brooklin](https://github.com/linkedin/brooklin) · LinkedIn's open-source system for streaming change data between systems at scale
- Paper: [All Aboard the Databus!](https://dl.acm.org/doi/10.1145/2391229.2391247) · LinkedIn's CDC system design, including snapshot bootstrapping
- Docs: [BigQuery change data capture](https://docs.cloud.google.com/bigquery/docs/change-data-capture) · upserts and deletes with primary keys and `max_staleness`
:::

## Sources

The first group was about databases you can read at the log level. Most of the outside world does not give you that luxury. SaaS tools expose rate-limited APIs, partners push webhooks or drop files on an SFTP server, servers write logs, websites and apps emit click events, machines report over MQTT, and exchanges broadcast binary market data at millions of messages per second.

Each source type has its own failure modes, and the patterns in this group are the answers that have proven themselves: checkpointed pagination and backoff for APIs, fast acknowledgement and deduplication for webhooks, arrival detection and quarantine for files, buffering for logs, validation and identity stitching for clickstreams, store-and-forward for devices, and sequence-number gap detection for market data. One idea runs through all of them: **land the raw data durably first, acknowledge quickly, and process afterwards**.

### SaaS API Ingestion at Scale

::: plain
Pulling data from a SaaS tool (Salesforce, Stripe, HubSpot, Zendesk) is like borrowing books from a library with strict rules: you may only take a few books per visit, only so many visits per hour, and you must show your card each time. A good ingestion job takes books in order, writes down which shelf it reached (so it can continue after an interruption), waits politely when the librarian says "come back later", and renews its library card before it expires.
:::

Most SaaS products offer a REST API that returns JSON in **pages**. Ingesting from them at scale means solving the same five problems every time: pagination, incremental sync, rate limits, authentication and failure recovery. Tools such as Fivetran, Airbyte and Meltano exist largely because solving them for hundreds of APIs is tedious (Pipelines and transformation compares them). You still need to understand the patterns, both to build the connectors nobody sells and to debug the ones you buy.

**Pagination.** **Offset pagination** (`?offset=200&limit=100`) is simple but breaks when data changes during the read: an insert at the top shifts every row down, so you see a row twice or miss one, and deep offsets get slow. **Cursor pagination** returns an opaque token pointing at the next position (`"next_cursor": "eyJpZCI6..."`, Stripe's `starting_after=<last object id>`), which is stable under inserts and cheap for the server. **Keyset pagination** is the same idea with a visible key (`?created_after=2026-03-01T10:00:00Z&id_after=981`). Always prefer cursors or keysets, and always sort by a stable, unique key.

**Incremental sync.** Most APIs let you filter by modification time (`updated_since`, Salesforce `SystemModstamp`, HubSpot `lastmodifieddate`). The watermark rules from the first topic apply: overlap the window, deduplicate by ID, and remember that deletes are usually invisible. Some APIs expose an **events** feed instead (Stripe's `/v1/events` lists every object change for the last 30 days), which works like CDC for that SaaS.

**Rate limits.** APIs cap requests per second, per minute or per day, per user or per app, and reply `429 Too Many Requests` (sometimes `503`) with a `Retry-After` header when you exceed them. The client must retry with **exponential backoff and jitter**: wait roughly `base × 2^attempt`, capped, with randomness so a fleet of workers does not retry in lockstep (the "thundering herd").

```python
import random

class RateLimited(Exception):
    def __init__(self, retry_after):
        self.retry_after = retry_after

# A fake SaaS API: 7 records, pages of 3, and a 429 on two calls
RECORDS = [{"id": i} for i in range(1, 8)]
calls = 0
def fake_api(cursor=None, limit=3):
    global calls
    calls += 1
    if calls in (2, 3):
        raise RateLimited(retry_after=1)
    start = int(cursor or 0)
    page = RECORDS[start:start + limit]
    nxt = str(start + limit) if start + limit < len(RECORDS) else None
    return {"data": page, "next_cursor": nxt}

def backoff_delay(attempt, base=0.5, cap=30.0):
    """Full jitter: a random wait between 0 and base * 2**attempt, capped."""
    return random.uniform(0, min(cap, base * 2 ** attempt))

def fetch_all(sleep=lambda s: None, max_attempts=5):
    cursor, out, waits = None, [], []
    while True:
        for attempt in range(max_attempts):
            try:
                resp = fake_api(cursor)
                break
            except RateLimited as e:
                wait = max(e.retry_after, backoff_delay(attempt))
                waits.append(round(wait, 2))
                sleep(wait)
        else:
            raise RuntimeError("gave up after retries")
        out.extend(resp["data"])
        cursor = resp["next_cursor"]          # save this as the checkpoint
        if cursor is None:
            return out, waits

random.seed(7)
rows, waits = fetch_all()
print(len(rows), "records in", calls, "calls")   # 7 records in 5 calls
print("waits:", waits)                           # waits: [1, 1]  (Retry-After wins here)
```

(The `sleep` argument does nothing in the example so it runs instantly; pass `time.sleep` for real use.) Better still, stay under the limit in the first place with a client-side **token bucket**: a counter that refills at the allowed rate and that every request must take a token from. When many workers share one API quota, the bucket must be shared too (in Redis or Memorystore, or by funnelling calls through one service).

**Authentication.** Most SaaS APIs use **OAuth 2.0**. Server-to-server integrations use the **client credentials** flow (exchange a client ID and secret for a short-lived access token); integrations acting for a user use the **authorization code** flow once, then a long-lived **refresh token** to get new access tokens. Store secrets and refresh tokens in Secret Manager, refresh access tokens before they expire, and handle providers that **rotate** refresh tokens on every use (save the new one atomically, or two workers will invalidate each other).

**Bulk APIs.** For large volumes, row-by-row REST is the wrong tool. **Salesforce Bulk API 2.0** takes a SOQL query, runs it asynchronously, and lets you download results as CSV in chunks (following the `Sforce-Locator` header), using far fewer API calls than the REST query endpoint. Many other vendors have export jobs that write files you then download.

**Where to run it on Google Cloud.** A typical custom connector is a container run as a **Cloud Run job** (or a Managed Service for Apache Airflow (formerly Cloud Composer) task) on a schedule. It writes raw JSON pages to Cloud Storage (`gs://acme-raw/stripe/charges/dt=2026-03-02/page-000017.json`), stores its cursor or watermark in a small state table (Firestore, Cloud SQL or a BigQuery table), and a load job or external table brings the pages into BigQuery. The **BigQuery Data Transfer Service** has managed connectors for some sources (Salesforce, ServiceNow, Google Ads and others), and Datastream can now read Salesforce as a source.

> **Pitfall:** Saving the cursor before the page is durably written. If the job crashes between the two, the page is lost forever. Write data first, then the checkpoint; on restart, at worst a page is fetched twice and deduplicated.

> **Tip:** Log every API response's rate limit headers (`X-RateLimit-Remaining` or the vendor's equivalent). Graphing them shows you how close you are to the limit long before you hit it.

::: deeper
**Connector protocols.** The open-source **Singer** specification defines a tap (source) and target (destination) exchanging JSON lines on standard output: `SCHEMA`, `RECORD` and `STATE` messages, where `STATE` carries the bookmark the tap resumes from. The **Airbyte protocol** generalises this with commands (`spec`, `check`, `discover`, `read`) and message types including `RECORD`, `STATE`, `LOG` and `TRACE`, running each connector as a Docker container. Both separate "talk to the API" from "write to the warehouse", so one tap works with any target.

**Parallelism under a shared quota.** Splitting a backfill by date range across 20 workers only helps if the quota allows it. Compute the budget: a 100 requests per 10 seconds limit with 200 records per page caps you at 2,000 records per second, about 7.2 million per hour, regardless of worker count. Plan backfills of 500 million records accordingly (about 70 hours) or use the vendor's bulk export.

**Search endpoint caps.** Some APIs cap how many results a single search can page through (HubSpot's CRM search stops at 10,000 results per query). Split the query into smaller time windows so each stays under the cap.

**Idempotent writes.** Land pages under deterministic names (source, object, window, page number) so a rerun overwrites rather than duplicates, and deduplicate by object ID and `updated_at` when building the clean table.

> **Interview:** "Design ingestion of 200 million Salesforce records, then hourly updates." Initial load through Bulk API 2.0 query jobs split by `CreatedDate` ranges, results to Cloud Storage, then BigQuery load jobs; hourly incremental by `SystemModstamp` with an overlap window, deduplicated by `Id`; deletes from `queryAll` with `IsDeleted` or from Salesforce change events; quota monitoring; OAuth secrets in Secret Manager.
:::

::: check
?? Why is cursor pagination safer than offset pagination for ingestion?
Offsets shift when rows are inserted or deleted during the read, so you can skip or repeat records, and deep offsets get slow. A cursor points at a stable position (often the last ID seen), so concurrent changes do not move it.

?? What is jitter in exponential backoff for, and what happens without it?
It randomises the wait so many clients that failed at the same moment do not all retry at the same moment. Without it they retry in synchronised waves and keep hitting the rate limit together.

?? Your connector saves the cursor, then writes the page to Cloud Storage. What can go wrong?
A crash between the two steps loses that page permanently, because the next run resumes after it. Write the data first and save the checkpoint second, accepting a possible duplicate page.
:::

::: resources
- Docs: [Airbyte protocol](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol) · the message and command specification connectors implement
- Spec: [Singer specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md) · taps, targets and the SCHEMA, RECORD and STATE messages
- Docs: [Salesforce Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_intro.htm) · asynchronous query and ingest jobs for large volumes
- Docs: [Stripe API: pagination](https://docs.stripe.com/api/pagination) · a clean example of cursor pagination with `starting_after` and `has_more`
- Book: [Site Reliability Engineering: Addressing cascading failures](https://sre.google/sre-book/addressing-cascading-failures/) · Google's chapter on retries, backoff, jitter and retry budgets
- Docs: [Introduction to the BigQuery Data Transfer Service](https://docs.cloud.google.com/bigquery/docs/dts-introduction) · managed SaaS and Google-source connectors into BigQuery
:::

### REST APIs & GraphQL for Ingestion

::: plain
A **REST API** is like a shop with fixed counters: one counter for customers, one for orders, one for invoices, and each counter hands you a fixed bundle of information. **GraphQL** is like a shop with a single counter where you hand over a precise shopping list ("for each customer, only their name and their last three orders") and get exactly that back in one go. For ingestion, both raise the same questions: how do I get everything, a page at a time, without being throttled, and how do I find out what changed?
:::

**REST** (representational state transfer) organises an API around **resources** at URLs (`/v1/customers`, `/v1/customers/cus_123`) and HTTP methods (`GET` to read, `POST` to create). For ingestion you mostly `GET` list endpoints page by page. Pagination styles from the previous topic apply, plus one more you will meet often: the **`Link` header** (RFC 8288), used by GitHub and many others, which returns the URL of the next page instead of a token in the body.

```python
import re

def parse_link_header(value):
    """Parse an RFC 8288 Link header into {rel: url}."""
    links = {}
    for part in value.split(","):
        m = re.match(r'\s*<([^>]+)>\s*;\s*rel="([^"]+)"', part)
        if m:
            links[m.group(2)] = m.group(1)
    return links

header = ('<https://api.example.com/issues?page=3&per_page=100>; rel="next", '
          '<https://api.example.com/issues?page=9&per_page=100>; rel="last"')
links = parse_link_header(header)
print(links["next"])   # https://api.example.com/issues?page=3&per_page=100
print(sorted(links))   # ['last', 'next']
```

A robust REST extractor follows `next` until it is absent, never builds page URLs itself (servers change the scheme), and treats every response as data to land, not just to parse. Useful HTTP habits:

- Send `Accept-Encoding: gzip` and reuse connections (HTTP keep-alive); both cut time per page dramatically.
- Use conditional requests (`If-None-Match` with an `ETag`, or `If-Modified-Since`) for resources you poll: a `304 Not Modified` costs almost nothing and often does not count against quotas.
- Set timeouts on connect and read. A hung socket without a timeout stalls a job forever.
- Treat `5xx` and `429` as retryable, most `4xx` as permanent (fix the request), and `401` as "refresh the token, then retry once".

**GraphQL** exposes a single endpoint (usually `POST /graphql`) and a typed schema. The client sends a query naming exactly the fields it wants, including nested related objects, which avoids both over-fetching and the "N+1 calls" problem of REST (one call for the list, then one per item for its details). Pagination follows the **Relay connection** convention: lists are returned as `edges` with `node`s and a `pageInfo` block.

```text
query Orders($after: String, $since: DateTime!) {
  orders(first: 100, after: $after, filter: {updatedAt: {gt: $since}}) {
    edges {
      node { id status totalAmount updatedAt customer { id email } }
    }
    pageInfo { hasNextPage endCursor }
  }
}
```

The extractor loops, passing `endCursor` as `$after` until `hasNextPage` is false. Note that the filter syntax (`filter: {updatedAt: ...}`) is defined by each API's schema; GraphQL standardises the query language, not the filtering vocabulary.

**GraphQL rate limits are cost-based.** Because one query can ask for a lot, providers assign each query a **cost** computed from the number of objects it may return. GitHub's GraphQL API, for example, gives users 5,000 points per hour and lets you ask for the cost of a query in the response (`rateLimit { cost remaining resetAt }`). Ask for smaller pages or fewer nested connections when you approach the limit. Nested connections multiply: 100 orders × 50 lines each can count as 5,000 objects.

**GraphQL subscriptions** let a client keep a connection open (usually a WebSocket using the `graphql-ws` protocol) and receive events when data changes. They are useful for low-latency feeds, but for ingestion they share the weaknesses of any push channel: if your consumer is disconnected, events sent during the gap are lost unless the API offers replay. Treat subscriptions as a fast path and keep a periodic incremental query as the safety net that catches anything missed.

**From API to pipeline.** The robust shape, whatever the protocol:

1. **Extract**: a stateless worker (Cloud Run job) fetches pages for one window and writes each raw response to Cloud Storage with deterministic names and metadata (request URL, time, status).
2. **Checkpoint**: the worker records the window or cursor it completed in a state store.
3. **Load**: a BigQuery load job, or an external table over the raw files, brings pages in as `JSON` rows.
4. **Model**: SQL flattens nested JSON (`UNNEST` over arrays), types columns and deduplicates by ID.

> **Pitfall:** Treating `200 OK` as success. Some APIs return `200` with an error object in the body, or return a partial page with a warning. Validate the shape of every response (expected keys, page size) before advancing the checkpoint.

> **Tip:** Use idempotency keys when your ingestion *writes* back to an API (reverse ETL). Stripe's `Idempotency-Key` header lets you retry a `POST` safely: the server returns the original result instead of creating a second object.

::: deeper
**HTTP/2 and connection limits.** Many APIs support HTTP/2, which multiplexes concurrent requests over one connection. Concurrency still counts against the rate limit, so more parallelism only helps if you have spare quota.

**Snapshot consistency.** A paginated read is not a snapshot: records can change between page 1 and page 500. For exactness, pair the full read with an incremental pass covering the time the full read took.

**GraphQL query complexity.** Servers protect themselves with depth limits, complexity limits and persisted queries (only pre-registered query hashes are accepted). An extractor may need its queries registered in advance.

**OpenAPI.** Many REST APIs publish an OpenAPI (formerly Swagger) document describing endpoints, parameters and response schemas. Generating a typed client from it, or validating responses against it, catches upstream changes early.

> **Interview:** "REST or GraphQL for pulling data from a partner?" Whatever the partner offers; but with GraphQL you can request exactly the fields you need and nested objects in one call, at the cost of query-cost rate limits and more complex pagination of nested lists. With REST you get simpler caching and conditional requests. In both cases: cursor pagination, incremental filter, backoff, raw landing.
:::

::: check
?? What does a client do with `pageInfo { hasNextPage endCursor }`?
It keeps requesting the next page with `after: endCursor` until `hasNextPage` is false, which walks the whole connection in a stable order.

?? Why should a GraphQL subscription not be your only ingestion path?
If the consumer disconnects or crashes, events pushed during the gap are lost unless the API supports replay. A periodic incremental query is needed as a safety net.

?? Which HTTP status codes should an extractor retry, and which should it not?
Retry `429` and `5xx` (with backoff), refresh credentials once on `401`, and do not retry other `4xx` errors such as `400` or `404`, which mean the request itself is wrong.
:::

::: resources
- Spec: [GraphQL specification](https://spec.graphql.org/) · the language, type system and execution rules
- Docs: [GraphQL pagination](https://graphql.org/learn/pagination/) · cursors, connections and the Relay convention explained
- Docs: [GitHub GraphQL API rate limits](https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api) · a worked example of cost-based limits
- Spec: [RFC 8288: Web Linking](https://www.rfc-editor.org/rfc/rfc8288) · the `Link` header used for REST pagination
- Blog: [Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency) · Stripe on idempotency keys and safe retries
- Book: [Principles of Web API Design](https://www.oreilly.com/library/view/principles-of-web/9780137355754/) · James Higginbotham on resource design, pagination and API styles
:::

### Webhook Ingestion Systems

::: plain
A **webhook** is a doorbell. Instead of you calling a service every few minutes to ask "anything new?", the service rings your doorbell (sends an HTTP request to a URL you gave it) the moment something happens. The rules for answering a doorbell apply: answer quickly, check who it is, and do not open the door twice for the same delivery. If you are too slow, the courier assumes nobody is home and comes back later, so the same parcel may be delivered several times.
:::

[[fig:ingestion_webhook]]

With webhooks the source **pushes** events to your HTTPS endpoint. Stripe sends `invoice.paid`, GitHub sends `push`, Shopify sends `orders/create`. Latency is low and you make no polling calls, but you have inverted control: the sender decides when traffic arrives, how often it retries, and how long it waits for you. Your endpoint must be available, fast and safe at all times.

**The core design: acknowledge fast, process later.** The receiver should do as little as possible before returning `2xx`:

1. **Verify** the request is genuine (signature check).
2. **Persist** the raw body and headers durably (publish to Pub/Sub, or write to Cloud Storage).
3. **Return** `200` (or `202`) within a second or two.

Everything else (parsing, enrichment, writing to BigQuery, calling other services) happens asynchronously from the queue. Senders time out quickly (Stripe expects a response within seconds) and treat a timeout as failure, so slow processing in the request path turns into retries and duplicates.

**Signatures.** Anyone can send a POST to your URL, so senders sign each request with a shared secret, usually an HMAC-SHA256 over the raw body plus a timestamp. Verify on the **raw bytes** (not re-serialised JSON), compare in constant time, and reject timestamps outside a tolerance window to block replayed requests.

```python
import hashlib, hmac, json, time

SECRET = b"whsec_test_only"

def sign(body: bytes, ts: int) -> str:
    mac = hmac.new(SECRET, f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"

def verify(body: bytes, header: str, now: int, tolerance=300) -> bool:
    parts = dict(p.split("=", 1) for p in header.split(","))
    ts = int(parts["t"])
    if abs(now - ts) > tolerance:                 # reject old, replayed requests
        return False
    expected = hmac.new(SECRET, f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, parts["v1"])   # constant-time compare

seen = set()                                      # in production: a table keyed by event id
def handle(body: bytes, header: str, now: int) -> int:
    if not verify(body, header, now):
        return 400
    event = json.loads(body)
    if event["id"] in seen:
        return 200                                # duplicate: acknowledge, do nothing
    seen.add(event["id"])
    # publish to a queue here, then return quickly
    return 200

now = int(time.time())
body = json.dumps({"id": "evt_1", "type": "invoice.paid"}).encode()
h = sign(body, now)
print(handle(body, h, now))                       # 200  (processed)
print(handle(body, h, now))                       # 200  (duplicate, ignored)
print(handle(body.replace(b"paid", b"void"), h, now))   # 400  (tampered body)
print(handle(body, h, now + 3600))                # 400  (too old)
print(len(seen))                                  # 1
```

The header format above mirrors Stripe's `Stripe-Signature: t=...,v1=...`. The **Standard Webhooks** specification defines a common scheme (`webhook-id`, `webhook-timestamp`, `webhook-signature` headers) that a growing number of providers use.

**Idempotency and deduplication.** Senders deliver **at least once**: on a timeout, a `5xx` or a network error they retry, typically with exponential backoff for hours or days (Stripe retries live-mode events for up to three days). Every event carries a unique ID; deduplicate on it. Duplicates of a valid event should get `200` (so the sender stops), not an error. In the pipeline, deduplicate again downstream (`QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at) = 1`), because a crash between publish and acknowledgement can still let a duplicate in.

**Ordering.** Webhooks are not ordered. `invoice.paid` can arrive before `invoice.created`. Either process events using the timestamp or version in the payload (apply only if newer), or treat the webhook as a notification and fetch the current object from the API.

**Fan-out.** One incoming event often feeds several consumers: the warehouse, a notification service, a fraud model. Publish once to a Pub/Sub topic and give each consumer its own **subscription**; each gets every message, acknowledges independently and can fail without affecting the others.

**On Google Cloud** the receiver is usually a small **Cloud Run** service (or Cloud Run function) behind HTTPS, publishing to **Pub/Sub**. A **BigQuery subscription** on the topic can write every raw event into a table with no code, and other subscriptions push to processors. Pub/Sub topics can retain messages (up to 31 days) and subscriptions can **seek** back to a timestamp, which gives you replay.

**Replay and reconciliation.** Webhooks get lost: your endpoint was down longer than the sender's retry window, a deploy broke signature verification, the sender had an incident. Plan recovery from the start: use the sender's "resend" or event-list API (Stripe's events endpoint, GitHub's redelivery API) to backfill gaps, and run a daily reconciliation comparing counts against the API.

> **Pitfall:** Doing the real work inside the request handler. A slow database write makes responses take 10 seconds, the sender times out and retries, the retries add load, and the endpoint collapses under its own duplicates.

> **Pitfall:** Verifying the signature after a framework has parsed and re-serialised the JSON. Whitespace or key order changes, the HMAC no longer matches, and every event is rejected. Always verify against the exact raw request body.

::: deeper
**Endpoint hardening.** Allow-list sender IP ranges if published, cap body size, return `400` for invalid signatures quickly without detailed error messages, and rotate secrets with an overlap period in which both old and new secrets verify (Standard Webhooks allows several signatures in one header for this). Put the endpoint on its own Cloud Run service with minimum instances above zero so cold starts never cause timeouts.

**Thin versus fat events.** A **fat** payload contains the full object; a **thin** payload contains only the ID and type, and the consumer fetches the object from the API. Thin events avoid ordering and stale-data problems (you always fetch the latest) at the cost of an API call per event and rate limit pressure.

**Sending webhooks yourself.** If your platform emits webhooks to customers, the problems invert: per-endpoint queues so one slow customer does not block others, retries with backoff, a dead-letter state after the retry window, signing, and a delivery log customers can inspect. Open-source Svix and services such as Hookdeck implement this.

> **Interview:** "Design a system to ingest 2,000 webhook events per second from a payment provider into BigQuery, with no loss and no duplicates in reporting." Cloud Run receiver with signature verification that publishes the raw body to Pub/Sub and returns 200; BigQuery subscription writes raw events; a downstream model deduplicates by event ID; a second subscription feeds real-time consumers; topic retention and seek for replay; a daily reconciliation job against the provider's events API to backfill gaps.
:::

::: check
?? Why should a webhook handler return `200` before processing the event?
The sender has a short timeout and treats a slow response as failure, so it retries. Slow handlers cause duplicate deliveries and cascading load. Persisting to a queue and returning quickly keeps the sender happy; processing happens asynchronously.

?? A valid event arrives a second time. What status should the endpoint return?
`200`. The event was already received, so acknowledging it stops further retries. Returning an error would make the sender keep retrying.

?? How do you recover events your endpoint missed during a long outage?
Use the sender's event-list or redelivery API to fetch events for the outage window, deduplicate by event ID, and reconcile counts. That is why the design includes reconciliation against the API, not just the push channel.
:::

::: resources
- Docs: [Stripe webhooks](https://docs.stripe.com/webhooks) · signatures, retries, ordering and best practices from a large webhook sender
- Spec: [Standard Webhooks](https://www.standardwebhooks.com/) · a common, vendor-neutral signing and header scheme
- Docs: [Pub/Sub subscription overview](https://docs.cloud.google.com/pubsub/docs/subscription-overview) · pull, push, BigQuery and Cloud Storage subscriptions for fan-out
- Docs: [Replay and purge messages with seek](https://docs.cloud.google.com/pubsub/docs/replay-overview) · retention and seeking back in time
- Blog: [Building reliable webhook infrastructure](https://hookdeck.com/blog/reliable-webhook-infrastructure) · queueing, retries and observability for webhook ingestion
- Repo: [svix/svix-webhooks](https://github.com/svix/svix-webhooks) · an open-source webhook sending service, useful for seeing the sender side
:::

### File-Based Ingestion — SFTP, EDI, Flat Files

::: plain
Many companies still exchange data the old way: one side drops a file in a shared folder, the other side picks it up. It is like a pigeonhole in an office mailroom. The hard parts are knowing **when a letter has fully arrived** (not half-written), **reading handwriting you did not choose** (CSV with odd quoting, fixed-width text, XML, and EDI, a compact format used between retailers and suppliers), and deciding what to do with a letter that is partly illegible.
:::

Files are still the most common way to receive data from partners, banks, retailers, government agencies and legacy systems. A file feed needs four things: a **landing place**, **arrival detection**, **parsing and validation**, and **error handling** that never silently drops data.

**Landing: SFTP and Cloud Storage.** Partners usually push files over **SFTP** (file transfer over SSH). Google Cloud has no managed SFTP server, so common options are an SFTP server such as SFTPGo on GKE or Compute Engine backed by a Cloud Storage bucket, a partner-managed transfer service, or asking the partner to write directly to a bucket with a service account. Files moving from on-premises file systems can be copied with **Storage Transfer Service** agents. Whatever the route, files should end up in a **landing bucket** that is write-only for the sender and append-only by policy, with **object versioning** or a retention policy so nothing can be overwritten silently.

**Arrival detection.** Polling a folder every few minutes works but adds delay and listing costs. Event-driven detection is better:

```bash
# Publish a Pub/Sub message whenever an object is fully written to the bucket
gcloud storage buckets notifications create gs://acme-landing \
  --topic=landing-files --event-types=OBJECT_FINALIZE
```

`OBJECT_FINALIZE` fires only when an object is completely uploaded, so a consumer never sees half a file in Cloud Storage. (On a local Linux server, the equivalent is `inotify` watching for `IN_CLOSE_WRITE`.) An **Eventarc** trigger can start a Cloud Run service or Workflow directly from the same event. Multi-file deliveries need a **completeness signal**: a manifest file listing every file with its size and checksum, or a marker file (`_SUCCESS`, `batch.done`) written last. Process the batch only when the marker arrives and the manifest matches.

**Parsing flat files.** CSV is not one format but a family: delimiters (`,` `;` `|` tab), quoting rules, embedded newlines, encodings (UTF-8, Latin-1, UTF-16 with a byte order mark), header rows, trailing summary rows. **Fixed-width** files define each field by character positions in a spec document. Parse with a real CSV parser, never `line.split(",")`, and route bad rows to quarantine with their line number and error:

```python
import csv, io

landing = io.StringIO(
    "sku,qty,price\n"
    "A-1,12,4.50\n"
    "B-2,three,19.99\n"     # bad number
    "C-3,5\n"                # missing column
    '"D-4, large",2,7.00\n'  # comma inside quotes: valid
)
good, quarantine = [], []
reader = csv.reader(landing)
header = next(reader)
for lineno, row in enumerate(reader, start=2):
    try:
        if len(row) != len(header):
            raise ValueError(f"expected {len(header)} fields, got {len(row)}")
        sku, qty, price = row
        good.append((sku, int(qty), float(price)))
    except ValueError as e:
        quarantine.append({"line": lineno, "raw": row, "error": str(e)})

print(good)        # [('A-1', 12, 4.5), ('D-4, large', 2, 7.0)]
for q in quarantine:
    print(q["line"], q["error"])
# 3 invalid literal for int() with base 10: 'three'
# 4 expected 3 fields, got 2
```

At scale, let the engine do the parsing: BigQuery load jobs (`--max_bad_records`, `--allow_quoted_newlines`, explicit schemas), Spark with `mode=PERMISSIVE` and a `columnNameOfCorruptRecord` column, or DuckDB's CSV reader. **JSON** should arrive as newline-delimited JSON (one object per line) so it can be split across workers; a single 20 GB JSON array cannot. **XML** needs streaming parsers (Python's `iterparse`, Spark XML support) for large files, plus an XSD schema for validation.

**EDI.** **Electronic Data Interchange** is a family of compact formats for business documents between companies, still dominant in retail, logistics and healthcare. The two big standards are **ANSI X12** (North America) and **UN/EDIFACT** (international). Documents are identified by number: in X12, **850** is a purchase order, **855** an order acknowledgement, **856** an advance ship notice (what is in each shipment, down to pallets and cartons), **810** an invoice. A file is a sequence of **segments** (ended by a terminator such as `~`) made of **elements** (separated by a character such as `*`), wrapped in envelopes: `ISA`/`IEA` (interchange), `GS`/`GE` (functional group) and `ST`/`SE` (one transaction set).

```python
edi = ("ISA*00*          *00*          *ZZ*BUYERCO        *ZZ*SUPPLIER       "
       "*260302*0930*U*00401*000000123*0*P*>~"
       "GS*PO*BUYERCO*SUPPLIER*20260302*0930*123*X*004010~"
       "ST*850*0001~"
       "BEG*00*SA*PO-7781**20260302~"
       "PO1*1*12*EA*4.50**VP*WIDGET-A~"
       "PO1*2*3*CS*19.99**VP*GADGET-B~"
       "CTT*2~"
       "SE*6*0001~GE*1*123~IEA*1*000000123~")

element_sep = edi[3]                  # the 4th character of ISA defines the separator
segment_term = edi[105]               # the character right after the 106-character ISA
segments = [s.split(element_sep) for s in edi.split(segment_term) if s]

order = {"lines": []}
for seg in segments:
    if seg[0] == "BEG":
        order["po_number"], order["date"] = seg[3], seg[5]
    elif seg[0] == "PO1":
        order["lines"].append({"sku": seg[7], "qty": int(seg[2]), "unit": seg[3],
                               "price": float(seg[4])})
    elif seg[0] == "CTT":
        assert int(seg[1]) == len(order["lines"]), "line count mismatch"

print(order["po_number"], order["date"])          # PO-7781 20260302
for line in order["lines"]:
    print(line)
# {'sku': 'WIDGET-A', 'qty': 12, 'unit': 'EA', 'price': 4.5}
# {'sku': 'GADGET-B', 'qty': 3, 'unit': 'CS', 'price': 19.99}
```

The `ISA` segment has a fixed length, so its characters tell you the separators in use; never hard-code them. Real EDI also needs **acknowledgements** (the 997 or 999 functional acknowledgement tells the sender you received and could parse the file) and trading-partner-specific rules. Production systems use an EDI translator library or platform rather than hand-written parsers, but the data engineer still owns landing, mapping to tables and reconciliation.

> **Pitfall:** Processing a file the moment it appears on an SFTP server's disk. The upload may still be in progress, and you load half a file. Use finalize events in Cloud Storage, temporary names renamed on completion, or marker files.

> **Tip:** Record every file in a **file registry** table (name, size, checksum, received time, status, row counts loaded and rejected). It answers "did we get Tuesday's file?" in one query and prevents loading the same file twice.

::: deeper
**Exactly-once file loading.** Event notifications are at-least-once, and partners resend files. Use the registry with a unique key on (name, checksum): a second notification for the same object is ignored, and a resent file with a different checksum is flagged for a decision rather than loaded blindly.

**Encodings.** A file declared UTF-8 that contains Latin-1 bytes fails or, worse, loads with replacement characters. Detect encoding on arrival, convert to UTF-8 in a staging step, and keep the original. BigQuery load jobs accept `--encoding=ISO-8859-1` for Latin-1 CSV.

**Compression and splitting.** Gzipped CSV cannot be split, so one 30 GB `.csv.gz` is read by a single worker (and BigQuery limits compressed CSV and JSON files to 4 GB each). Ask partners for many smaller files or for Parquet, or decompress and split in a staging step.

**PGP.** Partners often encrypt files with PGP. Decrypt in a controlled step with keys held in Secret Manager or Cloud KMS, write the plaintext to a restricted bucket, and delete or shorten the retention of plaintext copies.

> **Interview:** "A retailer sends 400 store files nightly over SFTP; some are late, some duplicated, a few malformed. Design ingestion." SFTP into a landing bucket; finalize notifications to Pub/Sub; a registry for deduplication and completeness (expected 400 by 06:00, alert on missing stores); parsing with quarantine of bad rows; loads into a date-partitioned staging table; a completeness check before publishing the day's data; late files reprocess only their own partition.
:::

::: check
?? Why is `OBJECT_FINALIZE` a safe trigger for processing a file in Cloud Storage?
It fires only after the object is completely written and visible, so the consumer never reads a partial upload.

?? In X12 EDI, what are the 850 and 856 documents?
The 850 is a purchase order; the 856 is an advance ship notice describing what is in a shipment.

?? Why is a single large gzipped CSV file slow to load?
Gzip streams cannot be split, so only one worker can read the file from start to end, and large compressed files may also exceed per-file load limits.
:::

::: resources
- Docs: [Pub/Sub notifications for Cloud Storage](https://docs.cloud.google.com/storage/docs/pubsub-notifications) · event types, payloads and delivery guarantees for new objects
- Docs: [Loading CSV data into BigQuery](https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv) · options for quoting, encodings, bad records and limits
- Docs: [Storage Transfer Service overview](https://docs.cloud.google.com/storage-transfer/docs/overview) · moving files from file systems and other storage into Cloud Storage
- Spec: [X12 transaction sets](https://x12.org/products/transaction-sets) · the official catalogue of X12 document types such as 850, 856 and 810
- Repo: [SFTPGo](https://github.com/drakkan/sftpgo) · an open-source SFTP server that can store files directly in Cloud Storage
- Spec: [RFC 4180: Common format for CSV files](https://www.rfc-editor.org/rfc/rfc4180) · the closest thing CSV has to a standard, and where real files depart from it
:::

### Log Ingestion Pipelines

::: plain
Every server, container and application constantly writes a diary of what it is doing: requests served, errors hit, users logged in. A **log ingestion pipeline** collects those diaries from thousands of machines, tidies each line into a structured record, adds useful labels (which service, which region), and delivers them to places where people can search, count and alert on them. It is the postal service for machine diaries: small agents pick up letters at every house and sorting centres route them onwards.
:::

Logs are the highest-volume, lowest-value-per-byte data most companies have. A modest platform produces terabytes a day. Log pipelines therefore care about efficiency, buffering and graceful degradation more than most other ingestion.

**The shape of a log pipeline.**

1. **Collection** at the source: an **agent** on each host or a sidecar or DaemonSet in Kubernetes tails log files or reads from the container runtime, journald or syslog.
2. **Parsing and structuring**: turn text lines into fields (timestamp, level, service, message, status code). Applications that log **structured JSON** from the start skip most of this.
3. **Enrichment**: add metadata such as Kubernetes namespace and pod labels, host, region, version, and derived fields (GeoIP from client IP, parsed user agent).
4. **Buffering**: a durable buffer (Kafka, Pub/Sub, or the agent's disk buffer) absorbs bursts and outages downstream.
5. **Routing and storage**: errors to an alerting system, everything to cheap storage, a sample or aggregate to a search index, audit logs to long retention.

**The tools.**

| Tool | Written in | Strengths |
|---|---|---|
| Fluentd | Ruby (with C) | Mature, huge plug-in ecosystem, CNCF graduated |
| Fluent Bit | C | Tiny footprint, the default agent in many Kubernetes distributions |
| Logstash | JVM | Powerful parsing (grok), part of the Elastic stack, heavier |
| Vector | Rust | Fast, one binary as agent or aggregator, typed transform language (VRL) |
| OpenTelemetry Collector | Go | Vendor-neutral standard for logs, metrics and traces |

A Vector configuration that tails files, parses JSON, tags the environment and sends to Kafka:

```yaml
sources:
  app_logs:
    type: file
    include: ["/var/log/app/*.log"]
transforms:
  parse:
    type: remap
    inputs: ["app_logs"]
    source: |
      . = parse_json!(.message)
      .env = "prod"
sinks:
  to_kafka:
    type: kafka
    inputs: ["parse"]
    bootstrap_servers: "kafka-1:9092"
    topic: "app-logs"
    encoding:
      codec: json
```

**Parsing unstructured lines.** Old applications and web servers write text formats such as the combined access log. Parse with anchored regular expressions (or grok patterns, which are named regular expressions), count what fails, and never drop it silently:

```python
import json, re
from collections import Counter

LINE = re.compile(
    r'(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d{3}) (?P<bytes>\d+|-)')

raw = [
    '10.0.0.5 - - [02/Mar/2026:09:15:01 +0000] "GET /cart HTTP/1.1" 200 512',
    '10.0.0.9 - - [02/Mar/2026:09:15:02 +0000] "POST /pay HTTP/1.1" 502 0',
    'garbage line from a crashed process',
    '10.0.0.5 - - [02/Mar/2026:09:15:03 +0000] "GET /cart HTTP/1.1" 200 498',
]

parsed, unparsed = [], 0
for line in raw:
    m = LINE.match(line)
    if not m:
        unparsed += 1                       # keep a count, never drop silently
        continue
    rec = m.groupdict()
    rec["status"] = int(rec["status"])
    rec["bytes"] = 0 if rec["bytes"] == "-" else int(rec["bytes"])
    parsed.append(rec)

print(json.dumps(parsed[1]))
# {"ip": "10.0.0.9", "ts": "02/Mar/2026:09:15:02 +0000", "method": "POST", "path": "/pay", "status": 502, "bytes": 0}

# log-to-metric: count responses by status class
print(Counter(f"{r['status'] // 100}xx" for r in parsed), "unparsed:", unparsed)
# Counter({'2xx': 2, '5xx': 1}) unparsed: 1
```

The last line is **log-to-metric extraction**: turning many log lines into a few numbers (requests per status class per minute). Metrics are thousands of times cheaper to store and query than the logs they came from, so pipelines often compute them at the edge (Vector's `log_to_metric` transform, Cloud Logging's log-based metrics) and keep full logs for a shorter time.

**Kafka as the log buffer.** Between agents and storage, a Kafka (or Pub/Sub) layer decouples producers from consumers: storage can be down for an hour without agents dropping data, several consumers (search, warehouse, security tools) read the same stream, and replay is possible. Without a buffer, a slow sink pushes back on agents, whose local buffers fill until they drop logs.

**On Google Cloud.** **Cloud Logging** collects logs from Google Cloud services automatically. The **Ops Agent** (built on Fluent Bit for logs and the OpenTelemetry Collector for metrics) collects from Compute Engine VMs; GKE collects container logs by default. **Log sinks** route copies of selected logs to BigQuery, Cloud Storage or Pub/Sub:

```bash
gcloud logging sinks create errors-to-bq \
  bigquery.googleapis.com/projects/acme-prod/datasets/ops_logs \
  --log-filter='severity>=ERROR'
```

**Log Analytics** lets you query log buckets with SQL directly and link them to BigQuery as a read-only dataset, which avoids copying logs at all for many analyses.

> **Pitfall:** Logging personal data (emails, tokens, full request bodies) and shipping it to every downstream system. Redact at the agent (Vector `redact`, Fluent Bit filters) before logs leave the host; deleting it later from five systems is far harder.

> **Tip:** Ask application teams to log structured JSON with a consistent set of field names (OpenTelemetry semantic conventions are a good standard). It removes most parsing work and most parsing bugs.

::: deeper
**Back-pressure and loss policies.** Every agent has to decide what to do when the destination is slow: block (and risk the application blocking on stdout), buffer in memory (fast, lost on crash), buffer on disk (survives restarts, bounded by disk), or drop (with a counter). Vector and Fluent Bit let you configure disk buffers and the behaviour when full per sink. Choose per stream: drop debug logs, never drop audit logs.

**Multiline logs.** Stack traces span many lines; naive tailers turn one exception into 40 events. Configure multiline rules (a line starting with whitespace or "at " continues the previous event) at the agent.

**Cardinality.** Labels such as `user_id` or `request_id` turned into metric dimensions explode the number of time series. Keep high-cardinality values in logs and traces, not in metrics.

**Cost control.** Exclusion filters (drop health-check logs before ingestion), sampling of successful requests, shorter retention for verbose levels, and routing bulk logs to Cloud Storage or BigQuery rather than an expensive search index typically cut log costs by more than half.

> **Interview:** "Design log ingestion for 2,000 Kubernetes nodes producing 5 TB per day." DaemonSet agents (Fluent Bit or Vector) with disk buffers and redaction; a Kafka or Pub/Sub buffer; consumers writing errors and recent logs to a search system, all logs to Cloud Storage or BigQuery partitioned by day, metrics extracted at the edge; per-stream retention and drop policies; dashboards on agent drop counts and buffer usage.
:::

::: check
?? Why put Kafka or Pub/Sub between log agents and storage?
It absorbs bursts and downstream outages without agents dropping data, lets several consumers read the same logs independently, and allows replay.

?? What is log-to-metric extraction, and why is it valuable?
Turning many log lines into a few aggregated numbers (counts, rates, latency percentiles) at or near the source. Metrics are far cheaper to store and query, so you can keep them long and alert on them while keeping full logs for a shorter time.

?? Where should personal data be removed from logs, and why there?
At the agent on the source host, before logs are shipped, because once copied to several systems it must be found and deleted in every one.
:::

::: resources
- Docs: [Vector documentation](https://vector.dev/docs/) · sources, transforms, sinks, buffering and VRL in one tool
- Docs: [Fluent Bit documentation](https://docs.fluentbit.io/manual) · the lightweight agent behind many Kubernetes and Google Cloud log setups
- Docs: [Fluentd Kafka output plug-in](https://docs.fluentd.org/output/kafka) · sending logs to Kafka as a buffer
- Docs: [Cloud Logging routing and storage overview](https://docs.cloud.google.com/logging/docs/routing/overview) · sinks, log buckets and exclusion filters
- Docs: [Log Analytics](https://docs.cloud.google.com/logging/docs/log-analytics) · querying logs with SQL and linking them to BigQuery
- Docs: [OpenTelemetry logs](https://opentelemetry.io/docs/concepts/signals/logs/) · the vendor-neutral model for logs, with semantic conventions
:::

### Clickstream & Event Tracking Pipelines

::: plain
A **clickstream** is the trail of footprints a visitor leaves on a website or app: page viewed, button tapped, product added to the basket, checkout finished. Tracking pipelines collect those footprints from millions of browsers and phones, check them, attach context (which campaign, which device), and stitch them into stories: this visit, this person, this journey from advert to purchase. It is like reconstructing a shopper's route through a supermarket from the till receipts and the CCTV time stamps.
:::

Clickstream data powers product analytics, marketing attribution, personalisation and experiments. It is high volume, arrives from devices you do not control (with wrong clocks, ad blockers, flaky networks and bots), and is only useful once it is organised into **sessions** and **users**.

**The pipeline.**

1. **Trackers**: JavaScript, mobile and server-side SDKs that emit events (`page_view`, `add_to_cart`) with properties, an event ID, device timestamps and identifiers.
2. **Collector**: an HTTP endpoint that accepts batches of events fast and writes them unchanged to a stream. It must be highly available; it does no processing.
3. **Validation and enrichment**: events are checked against schemas, and enriched with parsed user agent, GeoIP, campaign parameters (UTM), and consent flags. Invalid events go to a "bad" stream with the reason, not to the bin.
4. **Loading**: to the warehouse (BigQuery) and often to real-time consumers.
5. **Modelling**: sessions, users, funnels and attribution, built in SQL.

**Snowplow** is the best-known open-source design of exactly this: trackers, a Scala collector writing raw events to a stream, an **Enrich** application validating against self-describing JSON schemas held in an **Iglu** schema registry, separate good and bad streams, and loaders. On Google Cloud the streams are Pub/Sub topics and the BigQuery Loader writes enriched events into BigQuery. **Segment** (Twilio) and **RudderStack** (open source) are customer data platforms with the same core flow plus many destinations; their tracking API has a small vocabulary (`track`, `identify`, `page`, `screen`, `group`, `alias`). **Google Analytics 4** can export raw events to BigQuery daily or continuously (`events_YYYYMMDD` tables).

**Event design.** Name events as object plus action (`product_viewed`, `order_completed`), version their schemas, and give every event: a unique `event_id` generated on the device, the device timestamp, the collector receive timestamp, an anonymous device ID, the user ID when known, and a session ID if the tracker manages sessions.

**Deduplication at ingestion.** Mobile SDKs retry when offline, collectors behind load balancers can receive the same batch twice, and some browser bots replay requests. Deduplicate on `event_id` within a window (natural duplicates arrive within minutes or hours), and investigate events that share an ID but differ in content: that usually means a broken ID generator.

**Session reconstruction.** If trackers do not assign sessions, or you want a consistent definition, rebuild them: a new session starts when the gap since the user's previous event exceeds a timeout (30 minutes is the common convention). This is a window-function job, checked here in SQLite:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE events (user_id TEXT, ts INTEGER, page TEXT);   -- ts in minutes
INSERT INTO events VALUES
  ('u1',  0, 'home'), ('u1',  5, 'search'), ('u1', 12, 'product'),
  ('u1', 70, 'home'), ('u1', 75, 'cart'),
  ('u2',  3, 'home');
""")
rows = db.execute("""
WITH marked AS (
  SELECT *,
    CASE WHEN ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) <= 30
         THEN 0 ELSE 1 END AS new_session          -- first event or a gap over 30 min
  FROM events
),
numbered AS (
  SELECT *, SUM(new_session) OVER (PARTITION BY user_id ORDER BY ts) AS session_no
  FROM marked
)
SELECT user_id, session_no, COUNT(*) AS events, MIN(ts) AS start, MAX(ts) AS finish
FROM numbered GROUP BY user_id, session_no ORDER BY user_id, session_no
""").fetchall()
for r in rows:
    print(r)
# ('u1', 1, 3, 0, 12)
# ('u1', 2, 2, 70, 75)
# ('u2', 1, 1, 3, 3)
```

The BigQuery version replaces the arithmetic with `TIMESTAMP_DIFF(event_ts, LAG(event_ts) OVER w, MINUTE)`.

**Identity stitching.** One person appears under many identifiers: an anonymous cookie on the laptop, another on the phone, a login user ID, an email in the CRM. **Identity stitching** links them. **Deterministic** stitching uses hard evidence: when a user logs in, the current anonymous ID is linked to the user ID, and earlier anonymous events can be attributed to that user. **Probabilistic** stitching guesses from signals such as IP address and device characteristics; it is less accurate and more sensitive under privacy law. The links form a graph (identifiers as nodes, observed co-occurrences as edges) and each **connected component** is one person; the Graph data engineering section shows the algorithm. Cap component size: a shared family tablet or a corporate proxy IP can merge thousands of people into one "user".

> **Pitfall:** Trusting device timestamps for ordering and sessions. Phone clocks drift and offline events arrive hours late. Keep both device and collector times; use device time for order within a batch, and correct obvious skew using the difference between the device's send time and the collector's receive time (Snowplow calls the result `derived_tstamp`).

> **Tip:** Filter bots early but keep them: mark known bots by user agent and IAB lists, mark suspicious behaviour (hundreds of events per second, no JavaScript execution), and exclude them in modelled tables rather than deleting them at ingestion.

::: deeper
**First-party collection.** Browsers increasingly block third-party trackers and limit cookie lifetimes. Serving the collector on your own domain (a subdomain such as `t.example.com` pointing at a Cloud Run or load balancer endpoint) and setting cookies server-side keeps data quality higher, within consent rules.

**Consent.** Under GDPR and similar laws, events must carry the user's consent state, and processing must respect it (no tracking before consent, no use for advertising without the right consent). Enforce it in enrichment and in the models, and keep the consent history itself.

**Late data and incremental models.** Sessions built daily get events that arrive after the day was processed. Common practice: rebuild the last two or three days of sessions on every run (a lookback window), and only treat older days as final.

**Volume and cost.** A site with 5 million daily users at 50 events each produces 250 million events per day. Partition by event date, cluster by user or event name, and keep wide raw events for a limited time, with modelled sessions and aggregates kept long.

> **Interview:** "Design a clickstream pipeline that supports real-time personalisation and daily analytics." Trackers to a first-party collector on Cloud Run; raw events to Pub/Sub; an enrichment and validation job (Dataflow or Snowplow Enrich) writing good and bad streams; a real-time consumer maintaining per-user features in Bigtable or Memorystore; BigQuery for raw and modelled data with sessionisation and identity stitching in Dataform or dbt with a lookback window; dedupe on event ID; consent flags enforced in enrichment.
:::

::: check
?? Why does the collector do no processing?
It must accept events from millions of devices fast and stay available. Writing raw events straight to a durable stream keeps it simple; validation and enrichment happen downstream, where failures do not lose data.

?? How does deterministic identity stitching work?
When a user logs in, the tracker records the current anonymous ID together with the known user ID. That observed link lets all events under the anonymous ID be attributed to the user.

?? How do you rebuild sessions with a 30-minute timeout in SQL?
Use `LAG` to get each user's previous event time, flag an event as a new session when the gap exceeds 30 minutes (or there is no previous event), then take a running `SUM` of the flags per user to number the sessions.
:::

::: resources
- Docs: [Snowplow documentation](https://docs.snowplow.io/docs/) · the open-source tracking pipeline: trackers, collector, enrich, schemas and loaders
- Docs: [Segment documentation](https://segment.com/docs/) · the tracking API spec and identity resolution in a commercial CDP
- Docs: [RudderStack documentation](https://www.rudderstack.com/docs/) · an open-source customer data platform with warehouse-first design
- Docs: [GA4 BigQuery export](https://support.google.com/analytics/answer/9358801) · how Google Analytics events land in BigQuery and their schema
- Docs: [Iglu schema registry](https://docs.snowplow.io/docs/api-reference/iglu/) · self-describing JSON schemas for event validation
- Book: [Event Streams in Action](https://www.manning.com/books/event-streams-in-action) · Alexander Dean (Snowplow's co-founder) on unified logs and event pipelines
:::

### IoT Data Ingestion Architectures

::: plain
IoT ingestion is collecting readings from thousands or millions of small devices (meters, pumps, vehicles, wearables) that live on weak networks and often go offline. Imagine a national weather service with stations on mountain tops: each station sends a short message every few seconds when the line works, keeps notes in a logbook when it does not, and sends the backlog when the line comes back. The central office has to accept a flood of tiny messages, sort out duplicates and late arrivals, and file every reading under the right station and the right time.
:::

IoT data is **many small messages from many unreliable senders**: a fleet of 200,000 devices sending one reading every 10 seconds is 20,000 messages per second, each a few hundred bytes. Devices have limited CPU, battery and bandwidth, connect over mobile or satellite links, and sit behind networks you do not control.

**MQTT.** The standard device protocol is **MQTT**, a lightweight publish-subscribe protocol over TCP (and TLS). Devices keep one long-lived connection to a **broker** and publish to hierarchical **topics** such as `plant/7/pump/42/telemetry`; subscribers use wildcards (`+` matches one level, `#` matches the rest: `plant/7/+/+/telemetry`). Versions 3.1.1 and 5.0 are in use; 5.0 adds reason codes, message expiry, shared subscriptions and user properties. Key features:

| Feature | What it does |
|---|---|
| QoS 0 (at most once) | Fire and forget; cheapest, can lose messages |
| QoS 1 (at least once) | The receiver acknowledges; the sender resends until acknowledged, so duplicates happen |
| QoS 2 (exactly once) | A four-step handshake guarantees one delivery between client and broker; slowest |
| Retained messages | The broker keeps the last message on a topic and gives it to new subscribers (for example the current device state) |
| Last will | A message the broker publishes if the device disconnects ungracefully ("device offline") |
| Persistent sessions | The broker queues QoS 1 and 2 messages for a disconnected subscriber |

QoS is per hop: QoS 2 between device and broker says nothing about the path from the broker to your warehouse. Most telemetry uses QoS 1 plus deduplication downstream.

**Brokers on Google Cloud.** Google retired its managed device service, Cloud IoT Core, in August 2023. Today the common patterns are: run an MQTT broker cluster such as **EMQX**, **HiveMQ**, **VerneMQ** or **Mosquitto** on GKE, or use a partner's managed broker on Google Cloud, then **bridge** messages into **Pub/Sub** (EMQX and HiveMQ ship Pub/Sub integrations). From Pub/Sub, **Dataflow** validates, deduplicates and aggregates, and writes to **BigQuery** for analytics and **Bigtable** for low-latency per-device lookups. Industrial sites often use **OPC UA** on the factory floor and **Sparkplug B** (a specification on top of MQTT with defined topics, payloads and device state) to bring data out.

**Edge processing.** Not everything should travel to the cloud. An **edge gateway** near the devices can filter (drop unchanged readings), aggregate (send a per-minute summary instead of 600 samples), run models locally (detect a vibration anomaly in milliseconds) and **store and forward**: buffer to local disk when the uplink fails and send the backlog later, oldest or newest first by policy. Google Distributed Cloud runs Kubernetes workloads on hardware at the edge for this.

**Duplicates, late data and time.** Store-and-forward and QoS 1 guarantee duplicates and late arrivals. Give each device a monotonically increasing **sequence number**, deduplicate on (device, sequence), and aggregate by **event time** (when the reading was taken), not arrival time:

```python
from collections import defaultdict

# (device, sequence number, event time in seconds, temperature)
readings = [
    ("pump-7", 1, 0,   61.0),
    ("pump-7", 2, 10,  61.4),
    ("pump-7", 2, 10,  61.4),   # QoS 1 redelivery: same sequence number
    ("pump-7", 4, 70,  64.9),   # arrived before 3 (reconnect after an outage)
    ("pump-7", 3, 20,  62.2),
    ("pump-7", 5, 80,  65.3),
]

seen = set()
per_minute = defaultdict(list)
for device, seq, t, temp in readings:
    if (device, seq) in seen:
        continue                            # drop the duplicate
    seen.add((device, seq))
    per_minute[(device, t // 60)].append(temp)   # bucket by event time, not arrival

for (device, minute), temps in sorted(per_minute.items()):
    print(device, minute, len(temps), round(sum(temps) / len(temps), 2), max(temps))
# pump-7 0 3 61.53 62.2
# pump-7 1 2 65.1 65.3
```

Gaps in sequence numbers also tell you exactly which readings are missing, which is far better than guessing from timestamps. Device clocks drift, so sync them (NTP) and record the gateway's receive time too.

**Time-series storage.** Two patterns dominate on Google Cloud. **Bigtable** for serving: a row key such as `pump-7#20260302T0915` (device first, so one device's readings are contiguous; never timestamp first, which sends all writes to one node) with recent readings read in milliseconds. **BigQuery** for analytics: tables partitioned by day and clustered by device ID. Dedicated time-series databases (TimescaleDB on PostgreSQL, InfluxDB, QuestDB) are alternatives when one engine must do both.

**Device fleet management.** Ingestion depends on knowing your devices: a **registry** of device IDs, owners, firmware versions and locations; per-device **X.509 certificates** for mutual TLS authentication (never one shared password for the fleet); certificate rotation; **over-the-air (OTA)** firmware updates; and monitoring of last-seen time per device. Join telemetry to the registry in the pipeline so every reading carries its device's current metadata.

> **Pitfall:** Synchronised reconnect storms. After a broker restart or regional network outage, every device reconnects and flushes its backlog at the same moment, producing many times normal load. Devices need randomised reconnect delays, and the pipeline needs headroom and autoscaling for backlog drains.

> **Tip:** Separate telemetry topics (high volume, loss-tolerant) from command and state topics (low volume, must be reliable). They need different QoS levels, retention and alerting.

::: deeper
**Shared subscriptions.** MQTT 5's shared subscriptions (`$share/group/topic`) spread messages across several consumers, like a consumer group, which is how bridges scale horizontally.

**Payload formats.** JSON is easy but verbose; on constrained links, Protocol Buffers, CBOR or the Sparkplug B protobuf payload cut size several times. Compress batches at the gateway, not single readings.

**Downsampling and retention.** Keep raw readings for days or weeks, per-minute aggregates for months, per-hour aggregates for years. Compute aggregates in the stream (Dataflow windows) so the raw tier can expire safely.

**Security.** Devices are physically accessible to attackers. Use hardware-backed keys where possible, least-privilege topic permissions per device (a device may publish only to its own topics), and treat device data as untrusted input: validate ranges and types before it reaches shared tables.

> **Interview:** "Ingest telemetry from 500,000 vehicles every 5 seconds, show live positions and support analytics." MQTT broker cluster on GKE with mutual TLS; bridge to Pub/Sub; Dataflow to deduplicate on (vehicle, sequence), window by event time with allowed lateness, write latest position per vehicle to Bigtable for the live map and all readings to BigQuery partitioned by day and clustered by vehicle; edge store-and-forward; reconnect jitter; per-vehicle last-seen monitoring. 100,000 messages per second at a few hundred bytes is about 2 to 3 TB per day before compression.
:::

::: check
?? What is the difference between MQTT QoS 1 and QoS 2?
QoS 1 guarantees at-least-once delivery (resend until acknowledged), so duplicates are possible. QoS 2 uses a four-step handshake to deliver exactly once, but only between one client and the broker, and it is slower.

?? Why give each device a sequence number?
It lets the pipeline deduplicate redelivered readings and detect exactly which readings are missing, independent of drifting device clocks.

?? Why should a Bigtable row key for telemetry start with the device ID rather than the timestamp?
Keys starting with the timestamp make all current writes land in one key range, overloading one node (hotspotting). Device first spreads writes across the key space and keeps each device's readings contiguous for fast reads.
:::

::: resources
- Article: [MQTT Essentials](https://www.hivemq.com/blog/mqtt-essentials-part-1-introducing-mqtt/) · HiveMQ's series on publish-subscribe, QoS, sessions and retained messages
- Spec: [MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html) · the OASIS standard
- Docs: [Bigtable schema design for time series data](https://docs.cloud.google.com/bigtable/docs/schema-design-time-series) · row keys, tall versus wide tables and hotspot avoidance
- Docs: [EMQX documentation](https://docs.emqx.com/en/emqx/latest/) · an open-source MQTT broker with clustering and Pub/Sub integration
- Spec: [Eclipse Sparkplug](https://sparkplug.eclipse.org/) · the industrial MQTT specification for topics, payloads and device state
- Docs: [Streaming pipelines with Dataflow](https://docs.cloud.google.com/dataflow/docs/concepts/streaming-pipelines) · windows, watermarks and late data for device streams
:::

### Financial Data Ingestion (Market Data)

::: plain
An exchange is a giant noticeboard where buyers pin "I will buy 100 shares at 10.00" and sellers pin "I will sell at 10.02", and notes are added, changed and torn down thousands of times a second. **Market data** is the exchange shouting every one of those changes, in order, through a megaphone. If you miss a single shout you no longer know what is on the board, so ingestion is about catching every message in sequence, noticing instantly when one is missing, and rebuilding the board exactly.
:::

Market data ingestion is the extreme end of the field: very high message rates (millions per second at peaks across a full feed), microsecond timing, strict ordering, and legal requirements to keep records. Even if you never build a trading system, it teaches lessons that apply everywhere: sequence numbers, gap recovery, replay and as-of joins.

**The protocols.**

- **FIX** (Financial Information eXchange) is the industry's text protocol for orders and executions between firms, made of `tag=value` fields separated by the SOH character (byte `0x01`): `8=FIX.4.4` (version), `35=D` (message type: new order), `55=ACME` (symbol), `54=1` (side: buy), `38=100` (quantity), `44=10.00` (price), ending with `10=` (checksum). FIX is common for order flow and drop copies (copies of your executions for risk and record keeping); it is rarely used for high-volume market data because text is slow to parse.
- **ITCH** is Nasdaq's binary market data protocol (TotalView-ITCH 5.0): fixed-layout messages for every order book event, such as `A` (add order), `E` (order executed), `X` (order cancelled, partially), `D` (order deleted), `U` (order replaced) and `P` (trade). It is sent over UDP multicast (MoldUDP64), with a separate retransmission service for gaps. Many exchanges have similar binary feeds.
- **OUCH** is Nasdaq's binary protocol for *entering* orders (the counterpart to ITCH for order entry), not for market data.

**Order book reconstruction.** A full-depth feed sends individual order events; the consumer maintains every live order and aggregates them into **price levels** to get the book. A simplified version:

```python
# ITCH-style messages: A = add order, E = executed, X = cancel (partial), D = delete
msgs = [
    ("A", 1, "B", 100, 10.00),   # order 1: buy 100 at 10.00
    ("A", 2, "B", 200, 10.01),
    ("A", 3, "S", 150, 10.03),
    ("A", 4, "S", 100, 10.02),
    ("E", 2, 50),                # 50 of order 2 traded
    ("X", 3, 50),                # 50 of order 3 cancelled
    ("D", 1),                    # order 1 removed
]

orders = {}                      # order id -> [side, shares, price]
for m in msgs:
    kind, oid = m[0], m[1]
    if kind == "A":
        orders[oid] = [m[2], m[3], m[4]]
    elif kind in ("E", "X"):
        orders[oid][1] -= m[2]
        if orders[oid][1] == 0:
            del orders[oid]
    elif kind == "D":
        del orders[oid]

def levels(side):
    book = {}
    for s, shares, price in orders.values():
        if s == side:
            book[price] = book.get(price, 0) + shares
    return sorted(book.items(), reverse=(side == "B"))

bids, asks = levels("B"), levels("S")
print("bids", bids)     # bids [(10.01, 150)]
print("asks", asks)     # asks [(10.02, 100), (10.03, 100)]
print("spread", round(asks[0][0] - bids[0][0], 2))   # spread 0.01
```

Real implementations use integer prices (ITCH sends prices as integers with four implied decimals, never floats), fixed-size arrays or trees per level, and are written in C++, Rust or Java with careful memory layout.

**Sequence numbers and gaps.** Every message carries a sequence number. A consumer that sees 1001 after 999 has lost 1000 and its book may be wrong. It must pause or flag the affected instruments, request retransmission of the gap, and apply messages in order. Feeds are often sent twice over independent networks (A and B feeds) and the consumer **arbitrates**: takes whichever copy of each sequence number arrives first.

**Tick data storage.** **Ticks** are individual trades and quotes. Storage patterns:

- **kdb+** (q language) has long been the standard for tick databases: in-memory today, on-disk history partitioned by date, columnar by symbol.
- **QuestDB**, **ClickHouse** and **TimescaleDB** are popular alternatives with SQL; QuestDB was designed for this workload.
- For research at scale, **Parquet partitioned by date and symbol** in Cloud Storage, queried from BigQuery, Spark or DuckDB.

Always store the **raw feed** (packet captures or decoded messages with sequence numbers) as well as derived tables. Regulators and quants both need exact replay.

**Time alignment.** Analysis joins trades with the quote in force at the moment of each trade: an **as-of join**, which matches each row to the most recent earlier row of another table. kdb+ (`aj`), QuestDB, ClickHouse and DuckDB support it natively:

```sql
-- DuckDB: each trade with the latest quote at or before it
SELECT t.symbol, t.ts, t.price, q.bid, q.ask
FROM trades t ASOF JOIN quotes q
  ON t.symbol = q.symbol AND t.ts >= q.ts;
```

In BigQuery, emulate it with a window over the union of trades and quotes (`LAST_VALUE(... IGNORE NULLS) OVER (PARTITION BY symbol ORDER BY ts)`).

**On Google Cloud.** Some exchanges distribute data through cloud services: CME Group's Smart Stream delivers market data via Google Cloud Pub/Sub, and many historical datasets are available through BigQuery sharing (formerly Analytics Hub). Latency-critical trading still runs in colocation next to exchange matching engines; the cloud is used for distribution to non-latency-sensitive consumers, research, risk and storage.

> **Pitfall:** Storing prices as binary floating point. `0.1 + 0.2` is not `0.3`, and accumulated errors produce wrong P&L. Use integers in the smallest unit (ticks or 1/10,000 of a currency unit) or `NUMERIC`/decimal types.

> **Tip:** Store at least three timestamps per message: the exchange's event time, the feed's send time if present, and your receive time. Their differences tell you your feed latency and expose clock problems.

::: deeper
**Clock synchronisation.** Ordering events across venues needs accurate clocks. Firms use PTP (Precision Time Protocol) with GPS-disciplined grandmaster clocks. In the EU, MiFID II (RTS 25) requires high-frequency trading systems to keep clocks within 100 microseconds of UTC, with timestamps at 1 microsecond granularity or finer.

**Kernel bypass.** At the highest rates, the operating system's network stack is too slow; feed handlers use kernel-bypass network cards and libraries and pin threads to CPU cores. This is far from typical data engineering but explains why market data ingestion is often a separate team.

**Corporate actions and reference data.** A tick history is unusable without reference data: symbol changes, splits and dividends (to adjust prices), trading calendars and halts. Ingest these as carefully as the ticks, with effective dates, and keep both raw and adjusted series.

**Volume.** A full US equities depth feed can exceed a billion messages on a busy day; options feeds are larger. Compression of columnar tick data is excellent (sorted timestamps and repeated symbols), often 10 times or more.

> **Interview:** "How would you detect and handle a gap in a market data feed?" Track the expected next sequence number per channel; on a jump, mark affected instruments as stale, request retransmission or read the redundant B feed, buffer later messages, apply the recovered messages in order, then resume; if recovery fails, re-snapshot the book from the venue's snapshot service. Log every gap for audit.
:::

::: check
?? What is the difference between ITCH and OUCH?
ITCH is a binary market data feed broadcasting every order book event. OUCH is a binary protocol for entering orders into the exchange.

?? Why must a consumer care about sequence numbers on a market data feed?
The order book is rebuilt incrementally from every event. A missing message leaves the book wrong, so gaps must be detected and filled (retransmission or the redundant feed) before the book can be trusted again.

?? What is an as-of join, and why is it needed for trade analysis?
It matches each row to the most recent earlier row in another table by key and time, such as each trade to the quote in force at that moment. Exact-time equality joins would almost never match.
:::

::: resources
- Spec: [Nasdaq TotalView-ITCH 5.0 specification](https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf) · the message layouts for a full-depth market data feed
- Spec: [FIX Trading Community: FIX standards](https://www.fixtrading.org/standards/) · the FIX protocol family and its message specifications
- Docs: [QuestDB documentation](https://questdb.com/docs/) · a time-series database built for tick data, with SQL and as-of joins
- Docs: [DuckDB ASOF joins](https://duckdb.org/docs/stable/guides/sql_features/asof_join) · as-of join semantics with worked examples
- Blog: [Scaling a trading bot with a time-series database](https://questdb.com/blog/scaling-trading-bot-with-time-series-database/) · a practical walk-through of storing and querying tick data
- Book: *Trading and Exchanges: Market Microstructure for Practitioners* by Larry Harris · how order books, quotes and trades actually work
:::

## At scale

The Sources group handled one source at a time. Real platforms ingest hundreds of feeds into shared infrastructure, and new problems appear at that scale: how many workers and partitions you need for peak load, what happens when consumers fall behind, how to stream into open table formats without creating millions of tiny files, how to stop bad data at the door before it spreads, and when a managed integration can replace a pipeline you would otherwise write and run.

These topics connect ingestion to the rest of the course. Throughput engineering uses the partitioning and consumer-group ideas from Streaming systems; streaming into a lakehouse builds on Data lakes and lakehouses; profiling at ingestion is the first line of the data quality work in Pipelines and transformation; and zero-ETL is the Google Cloud services from BigQuery in depth used as an alternative to building pipelines at all.

### Ingestion at Scale — Throughput Engineering

::: plain
Throughput engineering is planning a motorway for rush hour, not for 3 a.m. You measure how many cars arrive at the busiest moment, how many cars one lane can carry, and you build enough lanes with some spare. You also plan what happens when there is an accident: where cars queue (a buffer), how traffic lights upstream slow arrivals (back-pressure), and how quickly the jam clears once the road reopens.
:::

Ingestion systems fail at peaks: Black Friday, the end of the month, a mobile app release, a reconnect storm, a backfill someone started on a Monday. Throughput engineering is the discipline of measuring, sizing and protecting the system for those moments.

**Measure before you size.** You need four numbers per stream: **peak arrival rate** (events per second at the busiest minute, not the daily average), **event size**, **per-worker throughput** (measured under load with realistic data, not guessed), and the **latency target** (how far behind the source you are allowed to fall). Daily averages hide peaks: 864 million events a day is 10,000 per second on average, but the peak minute may be 40,000.

**Parallelism through partitioning.** Throughput scales by running many workers in parallel, and the unit of parallelism is the **partition**: a Kafka topic partition, a Pub/Sub subscription's streaming pull connections, a set of source tables, a date range of a backfill. Rules:

- In Kafka, at most one consumer in a group reads each partition, so the partition count caps consumer parallelism. Create more partitions than you need today (adding later changes key-to-partition mapping).
- Keys decide partitions. A skewed key (one huge customer, one hot device) puts most of the load on one partition, and that one consumer becomes the bottleneck no matter how many you add.
- Pub/Sub has no partitions to manage; it scales subscribers automatically, but **ordering keys** limit throughput per key (ordered delivery per key needs sequential processing).

**A worked sizing example**, using measured numbers and Little's law (the average number of items in a system equals the arrival rate times the average time each spends in it):

```python
import math

# Sizing an ingestion consumer group with Little's law: L = arrival rate x time in system
peak_events_per_s = 40_000
per_worker_events_per_s = 2_500      # measured, not guessed
headroom = 1.5                       # room for bursts, retries and a lost worker

workers = math.ceil(peak_events_per_s * headroom / per_worker_events_per_s)
print("workers needed:", workers)                # workers needed: 24

# A backlog after a 20-minute outage, drained with spare capacity
backlog = peak_events_per_s * 20 * 60
spare = workers * per_worker_events_per_s - peak_events_per_s
print("backlog:", backlog, "events")             # backlog: 48000000 events
print("catch-up minutes:", round(backlog / spare / 60, 1))   # catch-up minutes: 40.0

# Little's law: average events in flight when each spends 0.2 s in the pipeline
print("in flight:", int(peak_events_per_s * 0.2))   # in flight: 8000
```

So: at least 24 partitions (48 gives room to grow), and the honest statement that a 20-minute outage at peak costs 40 minutes of catch-up with 50% headroom. If the business needs faster recovery, it needs more headroom or autoscaling.

**Batching.** Per-message overhead (network round trips, request headers, commits) dominates small messages. Batch at every hop: Kafka producers with `linger.ms` and `batch.size`, Pub/Sub publisher batching settings, BigQuery Storage Write API appends of many rows per request, bulk inserts into databases. Batching trades a few milliseconds of latency for often 10 times the throughput.

**Back-pressure.** When a consumer cannot keep up, something must give. **Back-pressure** means the slow stage signals upstream to slow down instead of being overwhelmed. Mechanisms: bounded in-memory queues that block producers when full; Pub/Sub subscriber **flow control** (`max outstanding messages` and `bytes`) so a client never pulls more than it can process; Dataflow and Flink propagating back-pressure through their network buffers; Kafka's pull model, where consumers fetch only what they can handle and the backlog simply waits in the log. Without it, workers run out of memory and crash, which makes the backlog worse.

**Buffering for bursts.** A durable log (Kafka, Pub/Sub) in front of processing turns a burst into a temporary backlog instead of an outage. Size its retention for the longest outage you must survive (a Pub/Sub subscription keeps unacknowledged messages for 7 days by default, and topic retention can keep messages up to 31 days for replay; Kafka keeps whatever its topic retention allows), and size processing for peak plus catch-up.

**Monitoring the ingestion SLA.** Express the promise as a **freshness SLO** ("99% of events queryable within 5 minutes") and watch the signals that predict breaches:

| Signal | Kafka | Pub/Sub |
|---|---|---|
| Backlog size | Consumer group lag (messages) | `subscription/num_undelivered_messages` |
| Backlog age | Lag in time (via timestamps) | `subscription/oldest_unacked_message_age` |
| Throughput | Records consumed per second | Acknowledged messages per second |
| Errors | DLQ rate, task failures | Dead-letter topic rate, nack rate |

Age beats size: 1 million messages behind is fine at 100,000 per second and terrible at 1,000. Alert on oldest message age approaching your freshness target.

> **Pitfall:** Autoscaling on CPU for an I/O-bound consumer. Workers waiting on a slow sink show low CPU, so the autoscaler scales *down* while the backlog grows. Scale on backlog age or lag (Dataflow does this; for GKE use the Pub/Sub or Kafka lag metric with a custom or external metrics autoscaler).

> **Tip:** Load-test with replayed production traffic at 2 to 3 times peak before big events. Synthetic uniform data hides key skew and the size outliers that break real pipelines.

::: deeper
**Self-serve ingestion frameworks.** Large companies stop writing pipelines per source and build a framework: a source is described in configuration (connection, tables or endpoints, schedule, destination, owners), and the platform generates and runs the jobs with standard monitoring. LinkedIn described such a framework built on Apache Gobblin; most platforms now use a mix of managed connectors and an internal config-driven layer.

**Head-of-line blocking.** One poison message that crashes a consumer blocks its whole partition. Retry a bounded number of times, then route to a dead-letter queue and move on.

**Coordinated omission.** Measuring latency only for requests that completed hides the ones stuck in queues. Measure end-to-end latency with source timestamps carried in every event.

**Universal scalability law.** Adding workers does not scale linearly: contention (shared locks, a single sink) and coherence costs (coordination) make throughput flatten and even fall. Measure throughput at 1, 2, 4, 8, 16 workers to find where it bends before buying more.

> **Interview:** "Your ingestion is 2 hours behind after a traffic spike. What do you do?" Confirm where the lag is (source capture, buffer, processing or sink); check for skewed partitions and poison messages; scale consumers up to the partition count; if the sink is the bottleneck, increase batching or sink capacity; prioritise fresh data if the business allows (process new data on a separate path and backfill the backlog); afterwards, add headroom, lag-based autoscaling and alerts on backlog age.
:::

::: check
?? Why is the partition count an upper bound on consumer parallelism in Kafka?
Within a consumer group each partition is read by exactly one consumer, so consumers beyond the number of partitions sit idle.

?? Why alert on the age of the oldest unacknowledged message rather than the number of messages?
Age directly measures how stale data is relative to the freshness target. A message count means different things at different throughputs.

?? What is back-pressure, and what happens without it?
A slow stage signalling upstream stages to slow down (by blocking, limiting outstanding messages or pulling less). Without it, a fast producer overwhelms a slow consumer, which buffers until it runs out of memory and crashes.
:::

::: resources
- Docs: [Pub/Sub flow control](https://docs.cloud.google.com/pubsub/docs/flow-control-messages) · limiting outstanding messages and bytes in subscribers
- Docs: [Monitor Pub/Sub](https://docs.cloud.google.com/pubsub/docs/monitoring) · backlog, oldest unacked message age and alerting
- Docs: [Kafka producer configuration](https://kafka.apache.org/documentation/#producerconfigs) · batching, linger and compression settings that drive throughput
- Docs: [Apache Gobblin](https://gobblin.apache.org/) · the config-driven ingestion framework LinkedIn built and open-sourced, for very large scale
- Book: [Site Reliability Engineering: Handling overload](https://sre.google/sre-book/handling-overload/) · Google on load shedding, throttling and graceful degradation
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; chapter 1 on describing load and performance, chapter 11 on stream throughput
:::

### Streaming Ingestion to Lakehouse

::: plain
A lakehouse table is like a library where new books can only be added by publishing a new edition of the catalogue. Streaming into it means books arrive one by one all day, but you cannot print a new catalogue for every single book: you would drown in catalogues and leave a heap of thin booklets on the shelves. So you collect arrivals for a minute or so, shelve them as one batch with one new catalogue edition, and every night a librarian rebinds the thin booklets into proper volumes.
:::

[[fig:ingestion_lake]]

Open table formats (Apache Iceberg, Delta Lake, Apache Hudi, Apache Paimon) add transactions on top of files in object storage: each **commit** atomically adds and removes files and creates a new **snapshot** of the table. Streaming into them means turning a continuous flow of events into a sequence of commits, while keeping the number of files, snapshots and metadata under control. Data lakes and lakehouses explains the formats; this topic is about the ingestion side.

**The writers.**

| Writer | How it commits | Notes |
|---|---|---|
| Spark Structured Streaming | One commit per micro-batch (trigger interval) | Iceberg, Delta and Hudi sinks; exactly-once via checkpoint plus idempotent commits |
| Apache Flink | One commit per checkpoint | Iceberg and Paimon sinks; Paimon is designed for streaming upserts |
| Kafka Connect Iceberg sink | A coordinator commits for all tasks at an interval | Part of the Apache Iceberg project; good for raw topic landing |
| Managed services | Varies | Confluent Tableflow and Redpanda Iceberg Topics expose topics as Iceberg tables; BigQuery Apache Iceberg managed tables accept Storage Write API streaming |

A Spark job landing a Kafka topic into an Iceberg table every minute:

```python
(spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka-1:9092")
    .option("subscribe", "orders")
    .load()
    .selectExpr("CAST(key AS STRING) AS order_id", "CAST(value AS STRING) AS payload",
                "timestamp AS kafka_ts")
    .writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", "gs://acme-lake/checkpoints/orders_raw")
    .toTable("lake.raw.orders"))
```

The same in Flink SQL, with the Iceberg catalog served by a REST catalog such as Google Cloud Lakehouse's runtime catalog (formerly BigLake metastore):

```sql
INSERT INTO lake.raw.orders
SELECT order_id, payload, event_ts FROM kafka_orders;
-- Flink commits the files written since the last checkpoint each time a checkpoint completes
```

**Micro-batch versus continuous.** Every writer is micro-batch at the commit level: the question is only how often. Commit interval trades freshness against file and metadata overhead. A writer with 50 parallel tasks writing into 10 partitions with a one-minute interval can create up to 500 files per minute, 720,000 per day, each perhaps a few megabytes. Query engines must open every file, so reads slow down and planning time grows with the metadata. Typical commit intervals are 1 to 5 minutes for analytics; if you need seconds, use a system built for it (BigQuery Storage Write API, Paimon, or a real-time OLAP engine) rather than committing Iceberg every second.

**Compaction coordination.** Small files are inevitable, so a **compaction** job rewrites them into larger files (128 MB to 1 GB): Iceberg's `rewrite_data_files` procedure, Delta's `OPTIMIZE`, Hudi's clustering and compaction services. Compaction is itself a commit that replaces files, so it can **conflict** with the streaming writer under optimistic concurrency. Practices that avoid conflict: compact only partitions the stream no longer writes (for example yesterday's hour partitions), enable partial progress so a conflict loses only one file group, and run snapshot expiry and orphan file cleanup on a schedule so metadata does not grow forever.

```sql
-- Spark SQL on Iceberg: compact yesterday's partition, then expire old snapshots
CALL lake.system.rewrite_data_files(
  table => 'raw.orders',
  where => 'event_date = DATE ''2026-03-01''',
  options => map('partial-progress.enabled', 'true'));
CALL lake.system.expire_snapshots(table => 'raw.orders', older_than => TIMESTAMP '2026-02-23 00:00:00');
```

**Exactly-once lakehouse writes.** A crash between writing files and committing them must not cause duplicates or loss. Writers achieve this by tying commits to their source position: Spark stores Kafka offsets in its checkpoint and writes each micro-batch with an idempotent commit (Delta records the batch ID per stream with `txnAppId` and `txnVersion`; Iceberg stores the streaming query ID and batch ID in the snapshot summary and skips a batch already committed); Flink stores the pending commit in its checkpoint state and commits after the checkpoint completes (two-phase commit). Files written but never committed are orphans that cleanup removes later; they are never visible to readers.

**Streaming merge (upserts).** Appending raw events is easy. Applying CDC (updates and deletes) in a stream is harder: each micro-batch must `MERGE` into the table. Formats handle this differently: **copy-on-write** rewrites every data file that contains an updated row (expensive for frequent small updates), while **merge-on-read** writes small delete files (Iceberg position or equality deletes, Delta deletion vectors, Hudi log files) that readers apply until compaction folds them in. Streaming upserts need merge-on-read plus regular compaction, or a format designed for it (Hudi, Paimon), or BigQuery's native CDC upserts.

> **Pitfall:** A one-second trigger "for freshness". It creates tens of thousands of commits per day, the metadata tree explodes, and queries that took seconds take minutes. Agree on the real freshness need; most analytics is happy with one to five minutes.

> **Tip:** Land raw events append-only into a bronze table and run merges into the current-state table as a separate job. The append path stays simple and fast, and a merge bug never loses incoming data.

::: deeper
**Partitioning for streams.** Partition by event date or hour (with Iceberg's hidden partitioning, `days(event_ts)` or `hours(event_ts)`), not by ingestion time if you need event-time queries. Late events then write to older partitions, so compaction must also revisit partitions that received late data; track that from commit metadata.

**Fanout writers.** If each task writes to many partitions at once, it keeps many files open, which uses memory. Spark's Iceberg writer can require data clustered by partition before writing (`distribution-mode=hash`) to reduce open files, at the cost of a shuffle.

**Commit contention.** Many independent writers committing to one table serialise on the catalog's compare-and-swap of the metadata pointer and retry on conflict. Either have one writer per table, or route writers through a coordinator (the Kafka Connect Iceberg sink's design).

**Equality deletes.** Flink's Iceberg upsert mode writes equality deletes (delete rows where key = X), cheap to write but expensive to read because every read must check them; compaction becomes essential. Spark's merge-on-read writes position deletes, cheaper for readers.

> **Interview:** "Stream CDC for 200 tables from Kafka into Iceberg on Cloud Storage with 5-minute freshness." Append raw changes to bronze Iceberg tables via the Kafka Connect Iceberg sink or Flink, committing every few minutes; merge into silver current-state tables with merge-on-read deletes; scheduled compaction of cold partitions with partial progress, snapshot expiry and orphan cleanup; a REST catalog (Lakehouse runtime catalog) so BigQuery, Spark and Trino read the same tables; monitor files per partition and commit latency.
:::

::: check
?? Why does a short commit interval hurt a lakehouse table?
Each commit adds files and a new snapshot. Frequent commits from many tasks create huge numbers of small files and metadata entries, so query planning and reading slow down sharply.

?? How can compaction and a streaming writer conflict, and how do you avoid it?
Both commit changes to the same files through optimistic concurrency, so one may fail and retry. Compact only partitions the stream is no longer writing, and enable partial progress so conflicts lose little work.

?? How does Spark Structured Streaming avoid duplicating a micro-batch after a crash?
It stores source offsets and the batch ID in its checkpoint and commits each batch idempotently, recording the batch ID in the table's commit metadata, so a replayed batch that was already committed is skipped.
:::

::: resources
- Docs: [Iceberg Flink writes](https://iceberg.apache.org/docs/latest/flink-writes/) · streaming writes, upsert mode and commit behaviour from Flink
- Docs: [Iceberg Spark Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/) · streaming writes, triggers and maintenance advice
- Docs: [Delta Lake streaming reads and writes](https://docs.delta.io/latest/delta-streaming.html) · idempotent writes and exactly-once with Structured Streaming
- Docs: [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/) · compaction, snapshot expiry and orphan file removal
- Docs: [Iceberg Kafka Connect sink](https://iceberg.apache.org/docs/latest/kafka-connect/) · coordinated commits from Kafka topics into Iceberg tables
- Docs: [Apache Paimon](https://paimon.apache.org/docs/master/) · a table format designed for streaming upserts with Flink
:::

### Data Profiling & Anomaly Detection at Ingestion

::: plain
Profiling at ingestion is the quality inspector at the factory gate. Before a delivery goes onto the shelves, someone counts the boxes, opens a few, checks that the labels make sense, and compares the delivery with what normally arrives. If today's truck holds a third of the usual load or half the boxes are empty, the delivery is set aside (quarantined) and someone is called, instead of letting bad goods spread through the whole shop.
:::

Bad data is cheapest to catch at the door. Once a broken batch has been merged into curated tables, fed dashboards and trained a model, fixing it means finding and repairing every downstream copy. **Validation at ingestion** checks each batch (or micro-batch) as it arrives and decides: load, load with warnings, or quarantine.

**Profiling** means computing summary statistics of a dataset: row count, per-column null rate, distinct count, min, max, mean and quantiles for numbers, length and pattern statistics for strings, top values and their frequencies. A profile of the first deliveries tells you what "normal" looks like and suggests rules; profiles of every later batch are compared with that baseline.

Checks fall into three families:

| Family | Examples | Catches |
|---|---|---|
| Schema checks | Expected columns present, types parse, no unknown fields | Source changes, wrong file |
| Rule checks | `order_id` unique and not null, `amount >= 0`, `country` in a list, `end_date >= start_date` | Broken records, logic errors |
| Statistical (anomaly) checks | Row count within the usual range, null rate not jumping, mean amount stable, freshness | Partial loads, silent upstream bugs, duplicated batches |

Rule checks need someone to write the rules; statistical checks learn from history. A simple, robust detector compares today's value with recent history using the median and the **median absolute deviation (MAD)**, which, unlike mean and standard deviation, is not dragged around by the very outliers you are trying to find:

```python
import statistics

# Row counts for the same daily file over the last two weeks, then today's
history = [10_210, 10_480, 9_950, 10_300, 10_120, 7_400, 7_210,
           10_390, 10_050, 10_610, 10_220, 10_330, 7_350, 7_180]
today = 4_020

def robust_z(x, values):
    """Distance from the median in units of MAD (median absolute deviation)."""
    med = statistics.median(values)
    mad = statistics.median(abs(v - med) for v in values) or 1
    return (x - med) / (1.4826 * mad)

print(round(robust_z(today, history), 1))     # -18.8

# Compare with the same weekday only: weekends are naturally lower
same_weekday = history[5::7]                   # the same weekday in the last two weeks
print(same_weekday)                            # [7400, 7350]

def gate(batch_rows, null_rate, history):
    checks = {
        "volume": abs(robust_z(batch_rows, history)) < 4,
        "null_rate": null_rate <= 0.02,
    }
    return ("load" if all(checks.values()) else "quarantine"), checks

print(gate(today, 0.001, history))
# ('quarantine', {'volume': False, 'null_rate': True})
print(gate(10_100, 0.001, history))
# ('load', {'volume': True, 'null_rate': True})
```

The factor 1.4826 scales MAD so that it matches the standard deviation for normally distributed data. Real metrics have **seasonality** (weekends, month end, holidays); compare like with like (same weekday) or use a model that learns seasonality. In BigQuery, `ARIMA_PLUS` time-series models do this in SQL:

```sql
CREATE OR REPLACE MODEL ops.daily_rows_model
OPTIONS (model_type = 'ARIMA_PLUS',
         time_series_timestamp_col = 'day',
         time_series_data_col = 'row_count') AS
SELECT day, row_count FROM ops.ingest_daily_counts WHERE day < CURRENT_DATE();

SELECT day, row_count, is_anomaly, lower_bound, upper_bound
FROM ML.DETECT_ANOMALIES(
  MODEL ops.daily_rows_model,
  STRUCT(0.99 AS anomaly_prob_threshold),
  (SELECT day, row_count FROM ops.ingest_daily_counts WHERE day = CURRENT_DATE()));
```

**The quarantine pattern.** Never delete data that fails checks. Route it somewhere visible and recoverable:

- **Row-level quarantine**: bad rows go to a quarantine table with the error, source file and batch ID; good rows load. Right when individual records are bad but the batch is fine.
- **Batch-level quarantine**: the whole batch is held in a staging area and not published to the curated table. Right when the batch as a whole is suspect (half the usual volume, a duplicated file), because loading part of it would produce plausible but wrong totals.
- **Circuit breaker**: after a failed gate, downstream jobs that depend on the table do not run, so yesterday's correct data stays in place instead of being overwritten.

The decision of what blocks and what only warns is a business decision: a 5% drop in clickstream events may be fine, a single missing row in a payments file is not.

**Tools.** **Great Expectations** (GX Core, open source, Python) defines suites of expectations (`expect_column_values_to_not_be_null`, `expect_table_row_count_to_be_between`) and produces validation results and documentation. **Soda** (Soda Core, open source, plus Soda Cloud) uses a YAML check language. On Google Cloud, **Knowledge Catalog** (formerly Dataplex Universal Catalog) runs **data profile scans** and **data quality scans** on BigQuery tables, with rules you define or recommended from a profile, results in BigQuery and alerts. Observability platforms (Monte Carlo, Bigeye, Elementary for dbt) learn baselines for volume, freshness and schema across all tables automatically. Pipelines and transformation covers data quality frameworks and data contracts in depth.

> **Pitfall:** Alert fatigue. Hundreds of statistical checks with tight thresholds produce false alarms daily, people stop reading them, and the real incident is missed. Start with a few high-value checks (volume, freshness, keys, nulls in critical columns), tune thresholds on history, and give every alert an owner.

> **Tip:** Store every batch's profile (counts, null rates, min and max of key columns) in a small metrics table. It costs almost nothing, powers anomaly detection, and lets you answer "when did this start?" during an incident.

::: deeper
**Where to validate.** In the producer (best: bad data never leaves; requires source team buy-in, which is what data contracts formalise), in the stream (Dataflow or Flink validating each record with a dead-letter output), at load (BigQuery load options like `max_bad_records`, plus post-load SQL checks in staging), or after load (tests in Dataform or dbt). Defence in depth uses several, with cheap checks early and expensive ones later.

**Sampling.** Exact distinct counts and quantiles on billions of rows are expensive. Use approximate algorithms (HyperLogLog for distinct counts, as in BigQuery's `APPROX_COUNT_DISTINCT`; t-digest or KLL sketches for quantiles, as in `APPROX_QUANTILES`) and profile samples for heavy statistics.

**Distribution drift.** Beyond single numbers, compare distributions: population stability index (PSI), Kolmogorov-Smirnov tests for numeric columns, or chi-squared for categories. These catch problems like a currency switch where counts and nulls look normal but amounts are suddenly 100 times larger.

**Testing the gate.** Gates are code. Unit test them with recorded bad batches (empty file, duplicated file, shifted columns) so a refactor does not quietly disable them.

> **Interview:** "A partner's daily file silently arrives with half its usual rows. How would your platform catch it?" A volume check comparing the row count with the same weekday's history using a robust threshold (or an ARIMA_PLUS model); on failure the batch is quarantined in staging and not merged; a circuit breaker stops dependent jobs; an alert goes to the feed owner with the profile; after the partner resends, the batch is reprocessed idempotently.
:::

::: check
?? Why use the median and MAD instead of mean and standard deviation for anomaly thresholds?
They are robust: a few extreme values in history barely move them, whereas outliers inflate the mean and standard deviation and hide the next anomaly.

?? When is batch-level quarantine better than row-level quarantine?
When the batch as a whole is suspect, such as a partial or duplicated delivery. Loading only its "valid" rows would produce plausible but wrong totals, so the whole batch should be held.

?? What does a circuit breaker do after a failed ingestion gate?
It stops downstream jobs that depend on the table from running, so the last good data stays in place rather than being overwritten or aggregated with the bad batch.
:::

::: resources
- Docs: [Great Expectations documentation](https://docs.greatexpectations.io/docs/) · expectations, validation results and data docs in GX Core
- Docs: [Soda documentation](https://docs.soda.io/) · check language, anomaly detection and contracts
- Docs: [Data profiling in Knowledge Catalog](https://docs.cloud.google.com/dataplex/docs/data-profiling-overview) · automatic profiles of BigQuery tables on Google Cloud
- Docs: [Auto data quality overview](https://docs.cloud.google.com/dataplex/docs/auto-data-quality-overview) · rule-based data quality scans with results in BigQuery
- Docs: [ML.DETECT_ANOMALIES](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-detect-anomalies) · anomaly detection with ARIMA_PLUS and other models in BigQuery
- Book: [Data Quality Fundamentals](https://www.oreilly.com/library/view/data-quality-fundamentals/9781098112035/) · Moses, Gavish and Vorwerck on monitoring, anomaly detection and incident response for data
:::

### Zero-ETL Patterns

::: plain
**Zero-ETL** means getting data from where it is created to where it is analysed without writing or running your own pipeline. Instead of hiring a removal van every night, you either have the two buildings connected by a corridor (query the data where it lives), or the building management copies things across for you automatically (a managed replication built into the platform). There is still movement and processing underneath; you just do not build or operate it yourself.
:::

"Zero-ETL" is a marketing term, but it names a real shift: platforms increasingly offer **built-in integrations** that replace the most common custom pipelines, especially "copy my operational database into my warehouse" and "make this stream queryable". The work does not disappear; it moves into a managed service with less flexibility and less code.

**The three forms.**

1. **Managed replication**: the platform continuously copies changes from an operational store into an analytical one, with no pipeline code. You configure source, destination and tables.
2. **Federation (query in place)**: the analytical engine queries the operational store directly, at query time, with no copy.
3. **Shared storage**: both systems read the same physical data (open table formats, zero-copy sharing), so nothing needs to be copied.

**On Google Cloud.**

| Need | Zero-ETL option | How it works |
|---|---|---|
| Operational database to BigQuery | Datastream to BigQuery | Managed CDC; merge or append-only tables, no pipeline code |
| Query Cloud SQL, AlloyDB or Spanner from BigQuery | Federated queries (`EXTERNAL_QUERY`) | SQL pushed to the source at query time |
| Analytics on Spanner without load on serving | Spanner Data Boost | Federated reads run on separate, independent compute |
| Stream to warehouse | Pub/Sub BigQuery subscriptions | Pub/Sub writes messages into a table directly |
| Stream to files | Pub/Sub Cloud Storage subscriptions | Pub/Sub writes batches of messages as files |
| Share data across teams and companies | BigQuery sharing (formerly Analytics Hub) | Subscribers query the publisher's tables, no copy |
| One copy for many engines | Google Cloud Lakehouse with Iceberg | BigQuery, Spark, Trino and Flink share Iceberg tables in Cloud Storage |
| Warehouse back to applications (reverse ETL) | `EXPORT DATA` to Bigtable, Spanner or Pub/Sub; continuous queries | BigQuery pushes results to serving stores (some targets need Enterprise editions) |
| Transactional plus analytical in one database | AlloyDB columnar engine | An in-memory column store accelerates analytical queries on the operational database |

```sql
-- Federation: join live Cloud SQL data with warehouse history, no pipeline
SELECT h.customer_id, h.lifetime_value, live.open_tickets
FROM analytics.customer_history AS h
JOIN EXTERNAL_QUERY(
  'acme-prod.europe-west1.support-pg',
  'SELECT customer_id, COUNT(*) AS open_tickets FROM tickets WHERE status = ''open'' GROUP BY customer_id'
) AS live USING (customer_id);
```

```bash
# Streaming zero-ETL: every message published to the topic lands in a BigQuery table
gcloud pubsub subscriptions create clicks-to-bq \
  --topic=clicks --bigquery-table=acme-prod:raw.clicks --use-table-schema
```

**Beyond Google Cloud services.** Kafka to Iceberg without a pipeline is now offered by several platforms: Confluent's Tableflow materialises topics as Iceberg or Delta tables, Redpanda offers Iceberg Topics, and BigQuery can read the resulting Iceberg tables. Snowflake and Databricks both read and write Iceberg tables through shared catalogs, and Databricks Lakehouse Federation and Snowflake external tables query data in place. SaaS platforms increasingly share data directly with warehouses (for example Salesforce Data Cloud's zero-copy integration with BigQuery).

**When zero-ETL fits, and when it does not.**

| Good fit | Poor fit |
|---|---|
| Standard database-to-warehouse replication | Heavy transformation, joins or enrichment before landing |
| Small to moderate federated queries, freshness critical | Large scans against an OLTP database at peak hours |
| Teams without pipeline engineers | Complex ordering, deduplication or business rules |
| Raw landing that is then modelled in SQL (ELT) | Sources the integration does not support, or needed features it lacks |

Federation especially has costs: each query runs on the operational database, competes with production traffic, and is only as fast as the source. Use it for small, selective lookups and exploration, and replicate (Datastream) for anything heavy or frequent. Managed replication also brings constraints: limits on supported data types, DDL handling and table counts, and less control over exactly how and when changes are applied.

> **Pitfall:** Treating zero-ETL as zero work. You still own schema change policies, data quality checks, access control on the copies, cost monitoring and the modelling that turns raw replicas into useful tables.

> **Tip:** Use zero-ETL for the boring 80% (standard replication, raw stream landing) and spend engineering time on the 20% where custom logic earns its keep.

::: deeper
**Where the ETL went.** Datastream still runs a log reader, buffers in Cloud Storage and applies merges in BigQuery; a Pub/Sub BigQuery subscription still runs a writer using the Storage Write API. You pay for these through the service's pricing rather than through your own compute, and you debug them through their metrics and logs rather than your code.

**Consistency of federation.** A federated query reads the source at query time, so results change between runs and cannot be reproduced later. For audits and reproducible reports, query a replica with known freshness (or a snapshot), not a federated view.

**HTAP.** Hybrid transactional and analytical processing systems (AlloyDB's columnar engine, Spanner with Data Boost, TiDB, SingleStore) try to remove the boundary entirely. They work well for operational analytics on one database's data; they do not replace a warehouse that integrates hundreds of sources.

> **Interview:** "Our product team wants live dashboards over the orders database. Federation or replication?" If queries are few, small and need second-level freshness, federation (or a read replica) may do; if dashboards are frequent, heavy or join other sources, replicate with Datastream into BigQuery (minutes of latency, no load on the primary) and add BI Engine or materialized views for speed. Mention the cost and reproducibility trade-offs.
:::

::: check
?? What are the three forms of zero-ETL?
Managed replication (the platform copies changes for you), federation (query the data in place at query time), and shared storage (several engines read the same physical tables, such as Iceberg or zero-copy sharing).

?? Why is federation a poor choice for a heavy dashboard over an OLTP database?
Every query runs against the operational database, competing with production traffic and limited by its speed. Heavy or frequent queries should run on a replica in the warehouse instead.

?? Name two Google Cloud zero-ETL paths into BigQuery.
Datastream for CDC from operational databases, and Pub/Sub BigQuery subscriptions for streams. (Federated queries with `EXTERNAL_QUERY` and BigQuery sharing are others.)
:::

::: resources
- Docs: [Datastream overview](https://docs.cloud.google.com/datastream/docs/overview) · managed CDC from operational databases into BigQuery
- Docs: [Introduction to federated queries](https://docs.cloud.google.com/bigquery/docs/federated-queries-intro) · querying Cloud SQL, AlloyDB and Spanner from BigQuery
- Docs: [BigQuery subscriptions](https://docs.cloud.google.com/pubsub/docs/bigquery) · writing Pub/Sub messages directly into tables
- Docs: [Spanner Data Boost overview](https://docs.cloud.google.com/spanner/docs/databoost/databoost-overview) · independent compute for analytical reads on Spanner
- Docs: [Confluent Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) · Kafka topics materialised as Iceberg or Delta tables
- Blog: [Iceberg tables: powering open standards with Snowflake innovations](https://www.snowflake.com/en/blog/iceberg-tables-powering-open-standards-with-snowflake-innovations/) · how shared Iceberg tables let engines avoid copies
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Choose a strategy | Small tables: full snapshot; large and changing with deletes: log-based CDC; otherwise watermark with overlap |
| Watermark rule | Re-read an overlap window, deduplicate by key, save the watermark after the load |
| PostgreSQL CDC prerequisites | `wal_level = logical`, a replication slot, a publication, `pgoutput` |
| MySQL CDC prerequisites | `binlog_format = ROW`, `binlog_row_image = FULL`, GTIDs, retention longer than outages |
| Watch slot lag | `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` from `pg_replication_slots` |
| Re-snapshot one Debezium table | Insert an `execute-snapshot` signal of type `incremental` |
| Kafka Connect status | `curl localhost:8083/connectors/NAME/status` |
| Bad records in a sink | `errors.tolerance=all` plus `errors.deadletterqueue.topic.name` |
| Apply CDC correctly | Latest event per key by log position; delete when `op = 'd'`; never order by ingestion time |
| API pagination | Cursor or keyset, never offset; write data before saving the cursor |
| Rate limits | Exponential backoff with full jitter, honour `Retry-After`, shared token bucket |
| Webhooks | Verify HMAC on raw body, publish to Pub/Sub, return 200, dedupe on event ID |
| File arrival | `gcloud storage buckets notifications create gs://BUCKET --topic=T --event-types=OBJECT_FINALIZE` |
| Sessions in SQL | `LAG` gap > 30 min marks a new session; running `SUM` numbers them |
| IoT duplicates | Deduplicate on (device, sequence number), aggregate by event time |
| Market data | Sequence numbers, gap detection, integer prices, as-of joins |
| Size consumers | Peak rate × headroom ÷ measured per-worker rate; partitions ≥ workers |
| Ingestion SLO alert | Oldest unacknowledged message age, not message count |
| Lakehouse streaming | Commit every 1 to 5 minutes; compact cold partitions; expire snapshots |
| Anomaly gate | Robust z-score (median and MAD) or `ML.DETECT_ANOMALIES`; quarantine, do not delete |

### Practice

::: try
**1. Pick the strategy.** For each source choose full snapshot, watermark incremental or log-based CDC, and justify: (a) a 40,000-row `currencies` table in MySQL updated weekly; (b) a 1.5 TB PostgreSQL `payments` table with refunds that delete rows, needed in BigQuery within 10 minutes; (c) a partner's REST API offering `updated_since` and no delete information, needed daily.
:::

?? Answer to 1
(a) Full snapshot: tiny, rarely changes, and a snapshot handles deletes for free. (b) Log-based CDC (Datastream or Debezium): the table is too large to snapshot often, deletes must be captured, and freshness is minutes. (c) Watermark incremental on `updated_since` with an overlap window and deduplication by ID, plus a periodic full extract of IDs (or a weekly full snapshot) to detect deletes, since the API cannot report them.

::: try
**2. Fix the extractor.** A connector does this in a loop: fetch page with cursor, save the next cursor to Firestore, write the page to Cloud Storage. It crashes occasionally, and the warehouse has gaps. Explain why and fix the order. What else is needed so the fix does not create duplicates?
:::

?? A solution to 2
The cursor is saved before the data is durable, so a crash between the two steps skips that page forever. Write the page first (with a deterministic object name such as source, window and page number), then save the cursor. After a crash the page may be fetched again; the deterministic name makes the rewrite overwrite the same object, and the clean table deduplicates by record ID and `updated_at`.

::: try
**3. Apply a change log.** Given a BigQuery table `raw.customers_changelog(id, op, email, source_lsn, ingest_ts)` with `op` in `r`, `c`, `u`, `d`, write a query that returns the current state of every customer that still exists.
:::

?? A solution to 3

```sql
SELECT id, email
FROM raw.customers_changelog
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_lsn DESC) = 1
  AND op != 'd';
```

`QUALIFY` keeps the latest change per key by log position, and the second condition drops keys whose latest change is a delete. (`WHERE TRUE` is a harmless filter that keeps the query valid where a `WHERE`, `GROUP BY` or `HAVING` clause is required alongside `QUALIFY`.)

::: try
**4. Size it.** A clickstream peaks at 60,000 events per second. One consumer handles 4,000 per second. You want 50% headroom. How many consumers and Kafka partitions do you need, and how long would a 30-minute outage at peak take to drain?
:::

?? Answer to 4
Consumers: 60,000 × 1.5 / 4,000 = 22.5, so 23. Partitions: at least 23; choose more (for example 48) for growth and even spreading. Backlog after 30 minutes: 60,000 × 1,800 = 108 million events. Spare capacity: 23 × 4,000 − 60,000 = 32,000 per second, so draining takes 108,000,000 / 32,000 = 3,375 seconds, about 56 minutes (if traffic stays at peak meanwhile).

::: try
**5. Design a webhook ingest.** A payment provider sends 500 webhooks per second at peak, retries for three days, and sometimes sends events out of order. Sketch the components on Google Cloud and explain how you avoid both loss and double counting in revenue reports.
:::

?? A solution to 5
A Cloud Run receiver (minimum instances above zero) verifies the HMAC signature on the raw body, publishes the body and headers to a Pub/Sub topic and returns 200 immediately. A BigQuery subscription lands raw events; other subscriptions feed real-time consumers. Loss: Pub/Sub durability plus topic retention and seek for replay, the provider's retries, and a daily reconciliation against the provider's events API to backfill gaps. Double counting: deduplicate by event ID in the modelled table (`QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ...) = 1`), and resolve out-of-order events using the object's version or timestamp (or fetch the current object from the API) instead of arrival order.

### What comes next

You can now get data in: from databases through CDC, from APIs, webhooks, files, logs, clickstreams, devices and market feeds, at scale and with quality gates at the door. The next section, Pipelines and transformation, picks up where the raw data lands: ETL versus ELT, batch and micro-batch patterns, idempotent pipelines, dbt, managed connectors such as Fivetran and Airbyte, data quality and contracts, schema evolution, dead-letter handling, backfills and lineage.
