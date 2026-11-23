This section is about what happens after data lands: turning raw tables into clean, tested, trustworthy ones, on a schedule, again and again, without losing or duplicating anything. It is written for data engineers and analytics engineers who can already write SQL and simple Python. It builds on [Data ingestion](#ingestion), which covered getting data in (snapshots, incremental extraction, CDC, Kafka Connect, APIs and files); on [Data modeling](#modeling), which covered what the target tables should look like (grain, keys, history, contracts, schema migrations); and on [Streaming systems](#streaming) for delivery guarantees and schema registries. Where a topic touches one of those, it links back instead of repeating it. Examples use BigQuery GoogleSQL for warehouse SQL; every Python example runs with the standard library only, and most SQL examples also run in SQLite through Python's `sqlite3`.

**What you will be able to do afterwards**

- Choose between ETL, ELT and EtLT for a pipeline, and explain where each transformation should run and why.
- Pick the right load pattern (full refresh, append, merge, partition overwrite, lookback window) and make every run idempotent, so a rerun gives the same result.
- Parallelise pipelines safely, and choose between scheduled batches, micro-batches and true streaming.
- Build, test and structure a dbt project well beyond the basics: materializations, incremental strategies, snapshots, macros, packages, custom tests, hooks and multi-project setups.
- Decide when to buy a managed connector (Fivetran, Airbyte), when to run open-source ones (Singer and Meltano) and when a flow tool such as Apache NiFi fits.
- Put validation, contracts, schema-change handling, dead-letter queues, backfills and lineage into a pipeline so that failures are caught, contained and repairable.

**The big picture.** Think of a pipeline as a factory line. Raw material arrives at the loading dock (ingestion). Each station on the line does one job: clean, standardise, join, aggregate, publish. Between stations there are quality gates, and items that fail a gate go to a rework bin instead of jamming the line. A good line has three properties. It is **repeatable**: running the same shift twice produces the same output, not twice the output. It is **observable**: at any moment you can see what passed through each station, how long it took and where each finished item came from. It is **repairable**: when a station had a fault last Tuesday, you can rerun just last Tuesday through just that station. Every topic below adds one of these properties. The first group decides where the stations stand and how they process each batch. The second covers dbt, the most common tool for building the SQL stations. The third covers ready-made connectors that feed the line. The fourth covers reliability: quality gates, contracts, schema changes, rework bins, backfills and lineage. The next section, Orchestration, covers the control room that starts each station at the right time.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## ETL and ELT

Every pipeline answers two questions before anything else: **where does the transformation run** (on a separate processing server before loading, or inside the warehouse after loading), and **how much of the data does each run touch** (everything, only what changed, or a moving window). Get these right and most later problems, such as reruns, late data and backfills, become simple. Get them wrong and every incident turns into a manual cleanup.

This group starts with the ETL versus ELT decision, then covers batch load patterns, parallelism and micro-batches, the shapes pipelines take (fan-out, fan-in, medallion layers), and finally idempotency, the property that makes all of it safe to retry.

### ETL vs ELT Architecture Trade-offs

::: plain
ETL and ELT differ in where the cooking happens. In **ETL** (extract, transform, load) the ingredients are cleaned and cooked in a separate kitchen and only the finished dish is delivered to the restaurant. In **ELT** (extract, load, transform) the raw ingredients are delivered straight to the restaurant, which has a huge, powerful kitchen of its own, and the cooking happens there. Modern cloud warehouses such as BigQuery are that huge kitchen, which is why ELT became the default.
:::

**The three steps.** Every pipeline **extracts** data from a source (a database, an API, files), **transforms** it (fixes types, removes duplicates, joins, aggregates, applies business rules) and **loads** it into a target (a warehouse or lakehouse table). The acronym tells you the order.

**ETL** was the standard for decades. Warehouses such as Teradata and Oracle were expensive and their compute was sized for queries, not for heavy cleaning, so a separate ETL server (Informatica, IBM DataStage, SSIS, or hand-written code) read from sources, transformed in its own memory and loaded only the finished tables. The warehouse never saw raw data.

**ELT** reverses the last two steps. Cloud warehouses separated storage from compute and made both cheap and elastic, so it became simpler to load raw data as it is and transform it with SQL inside the warehouse:

```sql
-- 1. Load: copy raw JSON files from Cloud Storage into a raw table, unchanged.
LOAD DATA INTO raw.orders
FROM FILES (format = 'NEWLINE_DELIMITED_JSON',
            uris = ['gs://acme-landing/orders/2026-11-22/*.json']);

-- 2. Transform: build a typed, deduplicated staging table with SQL.
CREATE OR REPLACE TABLE staging.orders AS
SELECT
  SAFE_CAST(order_id AS INT64)          AS order_id,
  LOWER(TRIM(status))                   AS status,
  SAFE_CAST(amount AS NUMERIC)          AS amount,
  TIMESTAMP(ordered_at)                 AS ordered_at
FROM raw.orders
WHERE order_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1;
```

The second step is what dbt and Dataform manage at scale (the next group).

**Why ELT won.** Raw data is kept, so when a business rule changes you rerun the SQL over history instead of re-extracting from sources that may no longer have it. The transformation language is SQL, which analysts can read and write. The warehouse scales the compute for you. And extraction and loading become a commodity that connectors (Fivetran, Airbyte, Datastream) handle without custom code.

| Question | ETL | ELT |
|---|---|---|
| Where does transformation run? | A separate engine before loading (Spark, Dataflow, a Python service) | Inside the warehouse, after loading (SQL in BigQuery) |
| Is raw data kept in the target? | Usually not | Yes, in a raw layer |
| Re-processing history after a rule change | Re-extract from sources | Rerun SQL over the raw layer |
| Main language | Python, Java, Scala, a GUI tool | SQL (with Jinja templating in dbt) |
| Who can change it | Data engineers | Data and analytics engineers, analysts |
| Cost driver | The processing cluster | Warehouse compute (bytes scanned or slots) |
| Sensitive data in the warehouse | Can be removed before it arrives | Arrives raw unless removed in flight |

**When ETL still wins.** ELT is the default, not a law. Transform before loading when:

- **Sensitive data must never land.** If card numbers or health data must not exist in the warehouse at all, mask or drop them in flight. Hiding them later with views is not the same: the raw copy still exists in the table, in BigQuery time travel (7 days by default, configurable from 2 to 7) and in fail-safe storage for 7 more days.
- **The transformation is not SQL-shaped.** Parsing binary formats, image or audio processing, calling ML models, complex per-record logic: these are easier in Spark, Beam (Dataflow) or Python.
- **Volume should shrink before storage.** A fleet of sensors sending readings every 100 ms can be aggregated to one row per device per minute before loading, cutting storage and scan costs by orders of magnitude.
- **Latency is seconds.** A streaming job (Dataflow, Flink) that enriches and writes events is ETL by definition.

**EtLT.** Most real pipelines today are a mix sometimes written **EtLT**: a small "t" in flight (type casting, masking personal data, deduplication, adding load metadata), then load, then the big "T" in the warehouse. A minimal in-flight step in Python:

```python
import hashlib, json

SALT = b"rotate-me-via-secret-manager"   # in production, read it from Secret Manager

def light_transform(record):
    """Small 't': pseudonymise the email and add load metadata before loading."""
    out = dict(record)
    email = out.pop("email").strip().lower()
    out["email_hash"] = hashlib.sha256(SALT + email.encode()).hexdigest()[:16]
    out["_source"] = "shop_api"
    return out

raw = {"order_id": 7, "email": " Ada@Example.com ", "amount": "19.90"}
print(json.dumps(light_transform(raw), sort_keys=True))
# {"_source": "shop_api", "amount": "19.90", "email_hash": "420448d65f8c6b61", "order_id": 7}
```

Note that `amount` is still a string: type fixing belongs to the warehouse step, where it is visible and testable.

**Push versus pull.** A second axis is who starts the transfer. In a **pull** design the pipeline reaches into the source on its schedule (a nightly query, an API poll); it controls timing and load but only sees what the source exposes at that moment. In a **push** design the source sends data when it happens (events to Pub/Sub, webhooks, CDC streams); it is fresher and sees every change, but the pipeline must absorb bursts and duplicates. ELT works with both: pushed events land in a raw table just like pulled extracts. [Data ingestion](#ingestion) covers both sides in detail.

[[fig:pipelines_etl_elt]]

> **Pitfall:** "Load everything raw and figure it out later" turns into a swamp when nobody owns the later step. ELT moves the transformation into the warehouse; it does not remove the need for tests, documentation and owners. Treat the SQL as production code from day one.

> **Tip:** Keep the raw layer append-only and immutable (load metadata columns such as `_loaded_at` and `_source_file`). It is your replay buffer: every downstream table can be rebuilt from it.

::: deeper
**Cost model.** In ELT you pay twice for raw data: storage of the raw layer, and every scan of it. On BigQuery on-demand pricing (per TiB scanned; see the pricing page for current rates) a model that rescans a 4 TiB raw table every hour costs 96 TiB of scans a day. The fixes are structural: partition raw tables by load date, make transformations incremental, and cluster by the keys you filter on. With capacity pricing (BigQuery editions) the same waste shows up as slot contention instead of a bill.

**Where the engine runs is a spectrum.** "Transform in the warehouse" can mean BigQuery SQL, but also Spark on Managed Service for Apache Spark (formerly Dataproc) reading the same tables through the BigQuery Storage API, or Spark over Iceberg tables in Cloud Storage. The ETL/ELT distinction is really about whether the transformation reads from the governed, queryable copy of raw data (ELT) or from the source system directly (ETL).

**Reverse ETL** sends modeled data from the warehouse back to operational tools (CRM, ads, support); see [Analytics engineering and BI](#analytics). **Zero-ETL** integrations replicate operational databases into the warehouse without a pipeline you write; see [Data ingestion](#ingestion). Neither removes transformation; they move where it starts.

**Lineage and debugging.** ELT has an underrated advantage: every intermediate step is a table you can query. When a number is wrong, you can `SELECT` from staging and raw to find where it broke. In ETL the intermediate state lived in a server's memory and is gone.

> **Interview:** "Would you use ETL or ELT for a new analytics platform on BigQuery?" ELT by default: land raw data with a connector or load jobs, keep it immutable, transform with dbt or Dataform in SQL, test at each layer. Then name the exceptions: mask regulated fields in flight (EtLT), pre-aggregate very high-volume telemetry, and use Dataflow for low-latency streams or non-SQL logic. Mention cost control (partitioning, incremental models).
:::

::: check
?? What changed in the industry to make ELT the default?
Cloud warehouses separated storage from cheap, elastic compute, so loading raw data and transforming it with SQL in place became cheaper and simpler than running a separate transformation server, and it keeps raw data for reprocessing.

?? Your raw layer contains customer emails, which policy says must never be stored. Is a masking view enough?
No. The raw values are still stored in the table and in time travel and fail-safe storage. Mask or drop them before loading (the small "t" in EtLT).

?? Give two cases where transforming before loading is the better design.
Any two of: sensitive fields that must never land; non-SQL transformations such as parsing binaries or calling ML models; very high-volume data that should be aggregated first; low-latency streaming enrichment.
:::

::: resources
- Docs: [BigQuery LOAD DATA statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/load-statements) · loading files from Cloud Storage with SQL, the "L" of ELT on Google Cloud
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley; the data engineering lifecycle and where transformation sits in it
- Book: *The Data Warehouse ETL Toolkit* by Ralph Kimball and Joe Caserta · the classic ETL design subsystems; still the best checklist of what a pipeline must handle
- Blog: [ETL vs ELT](https://www.fivetran.com/blog/etl-vs-elt) · Fivetran's comparison from the ELT side, with the history of the shift
- Docs: [What is dbt?](https://docs.getdbt.com/docs/introduction) · the transformation layer of an ELT stack and how it uses the warehouse's compute
:::

### Batch Processing Patterns

::: plain
A batch pipeline runs on a schedule and processes a chunk of data each time, like a night shift that sorts the day's mail. The important choice is how much of the pile each shift touches: everything again (simple but slow), only the new letters (fast but easy to miss some), or the last few days again in case late letters arrived (a safe middle). Whatever the choice, running the same shift twice must leave the mail sorted the same way, not sorted twice.
:::

**The run and its interval.** A batch run is responsible for a **data interval**: "orders from 2026-11-22 00:00 to 2026-11-23 00:00 UTC". This is different from when the run happens (perhaps 02:00 on the 23rd, or three weeks later during a backfill). Orchestrators make this explicit: Airflow passes `data_interval_start` and `data_interval_end` to each run, Dagster has partitions, dbt's microbatch strategy has batches. The single most useful habit in batch engineering is to make every query depend on the interval it was given, never on the clock (`CURRENT_DATE()`). Then any run can be repeated for any interval and produce the same result.

**The load patterns.** These decide how a run's output is written to the target table:

| Pattern | What a run does | Good for | Watch out for |
|---|---|---|---|
| Full refresh | Rebuild the whole table from all source data, then replace it | Small and medium tables, dimensions, anything with complex logic | Cost grows with history, not with change |
| Append | Insert the interval's new rows | Immutable events, logs | A rerun duplicates rows unless the interval is deleted first |
| Merge (upsert) | Update rows whose key exists, insert the rest | Mutable entities (customers, orders) | Scans the target; needs a reliable key |
| Partition overwrite | Replace whole partitions (delete the interval, insert it again) | Facts partitioned by date | Must replace every affected partition, not just "today" |
| Lookback window | Partition overwrite or merge over the last N intervals, not just the latest | Late-arriving data | Pick N from measured lateness |
| Snapshot diff (delta) | Compare today's full extract with yesterday's to find inserts, updates and deletes | Sources with no timestamps or CDC | Two full copies to compare |
| CDC apply | Apply a stream of change events by key and log position | Replicas of operational databases | Covered in [Data ingestion](#ingestion) |

[[fig:pipelines_load_patterns]]

**Full refresh done safely.** Never `TRUNCATE` and then insert: between the two statements the table is empty, and if the insert fails it stays empty. Build the new version and swap it in one atomic step. In BigQuery, `CREATE OR REPLACE TABLE ... AS SELECT` does that: readers see the old table until the new one is complete.

**Merge.** The upsert pattern in BigQuery:

```sql
MERGE mart.customers AS t
USING (
  SELECT * FROM staging.customers
  WHERE _loaded_at >= @interval_start AND _loaded_at < @interval_end
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET email = s.email, country = s.country, updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, country, updated_at)
  VALUES (s.customer_id, s.email, s.country, s.updated_at);
```

The `s.updated_at > t.updated_at` guard makes the merge safe to rerun and safe against out-of-order batches: an older version never overwrites a newer one.

**Partition overwrite with a lookback window.** For event facts partitioned by date, the most robust daily pattern is: delete the last N days and insert them again, inside one transaction. Late events that arrived since the last run are picked up, and a rerun produces the same table. This runs as is in SQLite:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE raw_events (event_id TEXT, event_date TEXT, amount INT);
CREATE TABLE fct_daily (event_date TEXT PRIMARY KEY, events INT, amount INT);
INSERT INTO raw_events VALUES
  ('a', '2026-11-20', 10), ('b', '2026-11-21', 5), ('c', '2026-11-21', 7),
  ('d', '2026-11-22', 3);
""")

def run(db, run_date, lookback_days=2):
    """Rebuild the partitions from run_date - lookback_days to run_date."""
    with db:  # one transaction: readers never see the partitions missing
        db.execute("DELETE FROM fct_daily WHERE event_date BETWEEN date(?, ?) AND ?",
                   (run_date, f"-{lookback_days} days", run_date))
        db.execute("""INSERT INTO fct_daily
                      SELECT event_date, COUNT(DISTINCT event_id), SUM(amount)
                      FROM raw_events
                      WHERE event_date BETWEEN date(?, ?) AND ?
                      GROUP BY event_date""",
                   (run_date, f"-{lookback_days} days", run_date))

run(db, "2026-11-22")
run(db, "2026-11-22")                         # rerun: same result, no duplicates
print(db.execute("SELECT * FROM fct_daily ORDER BY 1").fetchall())
# [('2026-11-20', 1, 10), ('2026-11-21', 2, 12), ('2026-11-22', 1, 3)]

db.execute("INSERT INTO raw_events VALUES ('e', '2026-11-21', 100)")  # late event
run(db, "2026-11-23")                         # the lookback window catches it
print(db.execute("SELECT * FROM fct_daily ORDER BY 1").fetchall())
# [('2026-11-20', 1, 10), ('2026-11-21', 3, 112), ('2026-11-22', 1, 3)]
```

The same logic in BigQuery uses a multi-statement transaction:

```sql
DECLARE run_date DATE DEFAULT @run_date;
BEGIN TRANSACTION;
DELETE FROM mart.fct_daily
WHERE event_date BETWEEN DATE_SUB(run_date, INTERVAL 2 DAY) AND run_date;
INSERT INTO mart.fct_daily (event_date, events, amount)
SELECT event_date, COUNT(DISTINCT event_id), SUM(amount)
FROM staging.events
WHERE event_date BETWEEN DATE_SUB(run_date, INTERVAL 2 DAY) AND run_date
GROUP BY event_date;
COMMIT TRANSACTION;
```

dbt's `insert_overwrite` incremental strategy on BigQuery generates the equivalent for you (next group).

**Choosing the window.** Measure lateness instead of guessing: for each event, compute the gap between its event date and the date it was loaded, and look at the 99.9th percentile. If 99.9% of events arrive within 2 days, a 3-day lookback catches nearly all of them; a weekly job with a 30-day lookback can catch the rest.

> **Pitfall:** Appending without first deleting the interval. The first retry after a half-finished run doubles that day's revenue. Either delete-then-insert in one transaction, or merge on a key.

> **Pitfall:** Filtering the source by `_loaded_at` but partitioning the target by `event_date`. A late event loaded today for last Tuesday falls outside "today's partition" and is either lost or written without replacing last Tuesday's partition. Choose the window on the column the target is partitioned by.

::: deeper
**Snapshot diff in SQL.** When a source offers only full extracts, compute the delta yourself. In BigQuery, `EXCEPT DISTINCT` finds rows that are new or changed, and an anti-join on the key finds deletes:

```sql
-- new or changed rows
SELECT * FROM snap.customers_20261123
EXCEPT DISTINCT
SELECT * FROM snap.customers_20261122;
-- deleted keys
SELECT y.customer_id FROM snap.customers_20261122 y
LEFT JOIN snap.customers_20261123 t USING (customer_id)
WHERE t.customer_id IS NULL;
```

For wide tables, compare a hash per row (`FARM_FINGERPRINT(TO_JSON_STRING(t))`) instead of all columns.

**Merge cost on BigQuery.** A `MERGE` reads the target to find matches. If the target is partitioned by date and the `ON` clause only compares keys, BigQuery scans every partition. Add a constant filter on the target's partition column in the `ON` clause (for example `AND t.order_date >= DATE_SUB(@run_date, INTERVAL 7 DAY)`) when you know changes only touch recent partitions, and cluster the target by the merge key.

**The logical date trap.** In Airflow before 2.2, the run "for" Monday started on Tuesday and was called `execution_date`, which confused nearly everyone. Airflow now names the interval explicitly (`data_interval_start`, `data_interval_end`, `logical_date`). Whatever the tool, log the interval in every run's metadata.

**Determinism.** A rerun gives the same answer only if the logic is deterministic. Replace `CURRENT_TIMESTAMP()` with the interval end, avoid `ORDER BY` ties in `ROW_NUMBER()` (add a tie-breaker column), and pin reference data to a version or date.

> **Interview:** "A daily revenue table is sometimes too low and corrects itself a few days later. Why, and how do you fix it?" Late-arriving events: the job processes only the latest day, so events arriving after the run are missed until something else rewrites that day. Fix with a lookback window (rebuild the last N days each run, N from measured lateness), partition overwrite or merge so reruns are idempotent, and a freshness and completeness check so consumers know when a day is final.
:::

::: check
?? Why must a batch query depend on the run's data interval rather than on `CURRENT_DATE()`?
So that any run can be repeated or backfilled for any interval and give the same result. A query based on the clock produces different data depending on when it runs.

?? What is the danger of `TRUNCATE` followed by `INSERT` for a full refresh, and what is the safer alternative?
The table is empty between the two statements and stays empty if the insert fails. Build the new version and swap it atomically, for example with `CREATE OR REPLACE TABLE ... AS SELECT`.

?? Your target is partitioned by `event_date`. Why does a lookback window solve late data?
Each run rebuilds the last N date partitions from all data currently available, so events that arrived late for those dates are included the next time the window covers them.
:::

::: resources
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley on batch transformation patterns, full versus incremental, and update patterns
- Docs: [BigQuery MERGE statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement) · the upsert syntax, with partition-pruning notes
- Docs: [BigQuery multi-statement transactions](https://cloud.google.com/bigquery/docs/transactions) · how to delete and insert partitions atomically
- Article: [Functional Data Engineering: a modern paradigm for batch data processing](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) · Maxime Beauchemin on pure, idempotent tasks and partition overwrite
- Docs: [Airflow DAG runs, data intervals and catchup](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html) · how an orchestrator hands each run its interval
:::

### Parallel & Micro-Batch Processing Patterns

::: plain
If one person sorts the mail, it takes all night; ten people sorting different postcodes finish in an hour, as long as they do not all need the one stamp machine at the same time. Parallelism is splitting work so it runs side by side, with limits so shared resources are not overwhelmed. **Micro-batching** is the opposite trick: instead of sorting once a night, you sort a small pile every minute, which feels almost live without needing a completely different machine.
:::

**Three levels of parallelism.** A pipeline can run in parallel at three levels, and each has its own limits:

| Level | Example | What limits it |
|---|---|---|
| Between pipelines and runs | Ten DAGs running at once; three days of a backfill at once | Orchestrator settings, warehouse capacity |
| Between tasks in one run | Load 40 source tables side by side (fan-out) | Concurrency settings, source connections, API quotas |
| Inside a task | Spark processes 200 partitions on 50 cores; BigQuery uses many slots for one query | Engine parallelism: partitions, slots, workers |

The third level is the engine's job ([Apache Spark](#spark), [BigQuery in depth](#bigquery)). The first two are yours.

**Concurrency limits in an orchestrator.** Airflow, covered in depth in [Orchestration](#orchestration), shows the typical knobs:

- `parallelism` (scheduler-wide) caps how many task instances run at once across the whole installation.
- `max_active_tasks` on a DAG caps its concurrent tasks; `max_active_runs` caps how many runs of it (for example backfill days) run at once.
- **Pools** protect a shared resource. A pool has a number of **slots**; each task declares which pool it uses and how many slots it takes (`pool_slots`). A pool `crm_api` with 4 slots ensures that no more than 4 tasks call the CRM API at once, whichever DAG they belong to. `priority_weight` decides who goes first when slots are scarce.

```python
# Airflow (sketch): forty table loads, but never more than 4 queries on the source database.
for table in TABLES:
    SQLExecuteQueryOperator(
        task_id=f"extract_{table}",
        conn_id="orders_replica",
        sql=f"SELECT * FROM {table} WHERE updated_at >= '{{{{ data_interval_start }}}}'",
        pool="orders_replica",   # created with 4 slots
    )
```

Dagster has concurrency pools with the same purpose, and Prefect has concurrency limits.

**Bounded concurrency in plain Python.** The same idea without an orchestrator: a thread pool with a fixed number of workers. This runs as is:

```python
import threading, time
from concurrent.futures import ThreadPoolExecutor

active = peak = 0
lock = threading.Lock()

def load_partition(day):
    global active, peak
    with lock:
        active += 1
        peak = max(peak, active)
    time.sleep(0.05)                    # stands in for a 30-second load job
    with lock:
        active -= 1
    return f"{day}: ok"

days = [f"2026-11-{d:02d}" for d in range(1, 11)]
with ThreadPoolExecutor(max_workers=3) as pool:   # at most 3 loads at once
    results = list(pool.map(load_partition, days))
print(results[0], results[-1], "| peak concurrency:", peak)
# 2026-11-01: ok 2026-11-10: ok | peak concurrency: 3
```

Threads are right here because each task waits on I/O (a remote job). For CPU-heavy Python, use processes or push the work into an engine.

**How much does parallelism help?** **Amdahl's law** says the serial part limits the speed-up. If a nightly run takes 60 minutes, of which 50 are 40 independent table loads and 10 are a final serial build, then with 10 parallel loads the run takes about 5 + 10 = 15 minutes. Even with infinite parallelism it never goes below 10. Look for the serial tail before adding workers.

**Micro-batch versus true streaming.** Between "once a night" and "every event as it happens" sits **micro-batching**: run small batches very often.

| Approach | Typical latency | How it works | Examples |
|---|---|---|---|
| Scheduled batch | Hours | Run per interval | Airflow plus BigQuery SQL |
| Frequent batch | 5 to 15 minutes | Same, smaller intervals | dbt incremental every 15 minutes |
| Micro-batch streaming | Seconds to a minute | Engine loops: read new offsets, process, commit | Spark Structured Streaming |
| Record-at-a-time streaming | Milliseconds to seconds | Each event flows through operators continuously | Flink, Dataflow (Apache Beam) |

**Spark Structured Streaming triggers** make the choice explicit. The trigger decides when the next micro-batch starts:

```python
# PySpark (sketch). The query and checkpoint are the same; only the trigger changes.
q = (events.writeStream
     .format("parquet")
     .option("checkpointLocation", "gs://acme-chk/orders/")
     .trigger(processingTime="1 minute")   # a micro-batch every minute
     # .trigger(availableNow=True)         # process everything new, in batches, then stop
     # .trigger(realTime="5 minutes")      # Spark 4.1+ real-time mode: records stream through; the duration is a checkpoint interval
     .start("gs://acme-lake/orders/"))
```

With no trigger, Spark starts the next micro-batch as soon as the previous one finishes. **Real-time mode** (Spark 4.1 and later, `update` output mode only) processes records continuously instead of waiting for batch boundaries, for sub-second latency; it started with stateless queries, and Spark 4.3 added deduplication and aggregations. The older `continuous` trigger was an experimental predecessor with a similar goal. `availableNow` (Spark 3.3 and later) is the interesting middle ground: a scheduled job that uses the streaming engine's offset tracking to process exactly what is new since the last run and then stops. It replaces the older `once` trigger. You get incremental, exactly-once bookkeeping with batch costs. The [PySpark + Kafka guide](pyspark-streaming-kafka-topic-map.html#ssbasics) covers the streaming engine itself.

**Windowed batch jobs.** A batch job can still compute time windows: an hourly job that aggregates events into 5-minute tumbling windows for the hour that just closed, plus a lookback for late events. This is often cheaper and simpler than a streaming job when consumers can wait an hour.

> **Pitfall:** Raising parallelism until something breaks. Forty parallel extracts against one PostgreSQL primary can slow the application that customers use. Size the pool for the source, not for your cluster, and read from a replica.

> **Tip:** If the business asks for "real time", ask what decision the data drives and how fast that decision is made. Most dashboards are fine with 15 minutes, which a frequent batch or `availableNow` job delivers at a fraction of the operational cost of a 24/7 streaming job.

::: deeper
**Micro-batch internals in Spark.** Each micro-batch first writes the offsets it will read to the **offset log** in the checkpoint directory, processes them, writes output, then records completion in the **commit log**. After a crash, Spark rereads the last offsets that have no commit and reprocesses that batch; with an idempotent or transactional sink this gives exactly-once output. If a batch takes longer than the trigger interval, the next one starts late and the job falls behind; watch processing time against the interval and cap input with `maxOffsetsPerTrigger` (Kafka) or `maxFilesPerTrigger` (files).

**Parallelism is capped by the input's partitioning.** A Kafka topic with 6 partitions gives at most 6 parallel readers per consumer group; a single 20 GB gzip file is read by one task because gzip cannot be split. Design input layout for the parallelism you want.

**Dataflow** scales workers automatically for both batch and streaming. In streaming, Beam's windows and triggers decide when results are emitted; Dataflow can run in exactly-once mode (default) or a cheaper at-least-once streaming mode for idempotent sinks. [Flink, Beam, Trino and other engines](#engines) covers the model.

**Backpressure.** When a downstream stage is slower than the upstream one, a streaming system must slow the producer or buffer. Micro-batch engines handle it naturally (the next batch waits); record-at-a-time engines propagate backpressure through their network buffers. See [Streaming systems](#streaming).

> **Interview:** "When would you choose micro-batch over Flink-style streaming?" When seconds to a minute of latency is acceptable, the team knows Spark or SQL, and the job has heavy joins or aggregations that are simpler in batches. Choose record-at-a-time streaming for sub-second latency, complex event-time logic with large state, or per-event reactions. Mention `availableNow` as a way to run a streaming job on a schedule.
:::

::: check
?? What is an Airflow pool for?
To cap how many tasks use a shared resource at once, across all DAGs. A pool has a number of slots; each task using it takes one or more slots, and tasks wait when none are free.

?? A run spends 20 minutes in a serial final step and 40 minutes in 8 independent loads done one after another. What is the best time you can get by running loads in parallel?
About 20 + 5 = 25 minutes with all 8 loads in parallel (each load takes 5 minutes). The serial 20 minutes cannot be removed by parallelism.

?? What does Spark's `availableNow` trigger do?
It processes all data that is available when the query starts, in one or more micro-batches with normal checkpointing, and then stops. It lets you schedule a streaming query like a batch job while keeping incremental, exactly-once progress tracking.
:::

::: resources
- Docs: [Airflow pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html) · slots, pool_slots and how pools limit concurrency
- Docs: [Structured Streaming: APIs on DataFrames and Datasets](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html) · the triggers section lists every trigger type with examples
- Docs: [Dataflow streaming modes](https://cloud.google.com/dataflow/docs/guides/streaming-modes) · exactly-once versus at-least-once streaming on Google Cloud
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · Akidau, Chernyak and Lax; batch as a special case of streaming, windows and triggers
- Paper: [Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark](https://people.eecs.berkeley.edu/~matei/papers/2018/sigmod_structured_streaming.pdf) · the design of micro-batch execution, offsets and commit logs
:::

### Pipeline Design Patterns

::: plain
Pipelines are built from a few recurring shapes, just as buildings are built from walls, doors and stairs. **Fan-out** splits one job into many parallel ones, **fan-in** waits for them all and combines the results, a **branch** chooses one path based on a condition, and a **template** stamps out the same pipeline for many sources. The **medallion** pattern arranges tables in three floors, raw, cleaned and business-ready, so everyone knows what quality to expect on each floor.
:::

A pipeline is usually drawn as a **DAG**, a directed acyclic graph: boxes (tasks) and arrows (dependencies), with no cycles, so there is always an order in which every task can run after its inputs. The patterns below are shapes in that graph.

**Fan-out and fan-in.** One task produces work for many: list yesterday's 30 source files, then load each in parallel (fan-out). A later task waits for all of them and combines (fan-in): build the daily fact table once every source is loaded. Fan-in is where pipelines are most fragile: one slow or failed branch holds back the whole result. Decide explicitly what the join step does when a branch fails: fail the run (correctness first), or proceed with a flag (`completeness = 29/30`) for non-critical sources.

**Conditional branching.** Choose a path at run time: if the file is empty, skip the load and alert; if it is the first day of the month, also run the monthly close. Keep conditions in one place and make skipped branches visible in run history, not silently absent.

**Dynamic generation.** Real platforms have dozens or hundreds of similar pipelines. Rather than copying code, generate them from configuration:

```yaml
# sources.yaml: one entry per source table; the pipeline code never changes
- name: orders
  schedule: hourly
  key: order_id
  load: merge
- name: products
  schedule: daily
  key: product_id
  load: full_refresh
```

Two forms exist. **Parse-time generation** builds one DAG per config entry when the orchestrator reads the code. **Run-time mapping** creates tasks while the run executes, from data that only exists then (the list of files that actually arrived). Airflow's dynamic task mapping does the second:

```python
# Airflow 3 TaskFlow (sketch): one load task per file that actually arrived.
from airflow.sdk import dag, task

@dag(schedule="@daily")
def load_files():
    @task
    def list_files() -> list[str]:
        return ["gs://acme-landing/a.json", "gs://acme-landing/b.json"]

    @task
    def load(uri: str) -> int:
        ...  # load one file, return row count

    @task
    def summarise(counts: list[int]) -> None:
        print(sum(counts))

    summarise(load.expand(uri=list_files()))   # fan-out with expand, fan-in by passing the list

load_files()
```

**The template pattern.** Write the pipeline once as a function or class with parameters (source, key, load pattern), and keep the per-source differences in configuration. New sources become a config change reviewed in a pull request. This is the most effective way to keep a large platform consistent: retries, logging, quality checks and lineage are written once.

**Resolving the order yourself.** Python's standard library includes `graphlib`, which computes a valid order and the groups that can run in parallel. This runs as is and shows fan-out and fan-in:

```python
from graphlib import TopologicalSorter

deps = {                      # task: the tasks it depends on
    "load_orders":    {"list_files"},
    "load_payments":  {"list_files"},
    "load_customers": {"list_files"},
    "build_fact":     {"load_orders", "load_payments", "load_customers"},
    "publish":        {"build_fact"},
}
ts = TopologicalSorter(deps)
ts.prepare()
wave = 1
while ts.is_active():
    ready = sorted(ts.get_ready())          # everything here can run in parallel
    print(f"wave {wave}: {ready}")
    ts.done(*ready)
    wave += 1
# wave 1: ['list_files']
# wave 2: ['load_customers', 'load_orders', 'load_payments']
# wave 3: ['build_fact']
# wave 4: ['publish']
```

**The medallion pattern.** Databricks popularised names for three layers of tables:

| Layer | Also called | Contents | Rules |
|---|---|---|---|
| Bronze | raw, landing | Data as received, plus load metadata | Append-only, never edited, replayable |
| Silver | staging, cleaned, conformed | Typed, deduplicated, standardised, joined to keys | One row per entity or event, tested |
| Gold | marts, presentation | Business tables: facts, dimensions, aggregates, features | Owned, documented, contract-backed |

The dbt convention (staging, intermediate, marts) maps onto the same idea. The value is not the names but the promises: a consumer knows a gold table is tested and owned, and an engineer knows bronze can always rebuild silver.

[[fig:pipelines_dag_patterns]]

> **Pitfall:** Generating DAGs by querying a database or an API at parse time. Airflow re-parses DAG files continuously, so every parse hits the database, slows the scheduler and breaks all DAGs when the database is down. Generate from a file in the repository, or use run-time mapping.

> **Tip:** Prefer a few coarse tasks over many tiny ones. Each task has scheduling overhead (often seconds), its own logs and its own failure mode. A task should be the smallest unit you would want to retry on its own.

::: deeper
**Sensors and deferrable operators.** Waiting for an external event (a file, a partition, another team's table) is a design pattern of its own. A naive sensor occupies a worker slot while it polls. Deferrable operators in Airflow hand the waiting to a lightweight triggerer process, freeing the worker. Data-aware scheduling (Airflow **assets**, called datasets before Airflow 3; Dagster's asset-based scheduling) goes further: a pipeline runs when its input data is updated, not at a clock time. [Orchestration](#orchestration) covers these.

**Event-driven pipelines on Google Cloud.** A file landing in Cloud Storage can trigger an Eventarc event that starts a Cloud Run job or a Workflows execution, with no scheduler at all. This suits irregular arrivals; it needs idempotent processing because events can be delivered more than once. [Pipelines on Google Cloud](#gcppipes) covers the services.

**Criticisms of medallion.** Three layers is a convention, not a law. Large platforms often have more (raw, cleaned, conformed, business, serving), and some sources need fewer. The harmful version is "bronze, silver, gold" as folder names with no agreed promises; then gold tables are no more trustworthy than bronze ones.

**Idempotent units.** Every pattern above is safe only if each task is idempotent: retrying a branch of a fan-out must not double its output. That is the subject of the next topic.

> **Interview:** "Design a pipeline that loads 200 tables from a SaaS tool nightly." Configuration-driven: one YAML entry per table with key and load pattern; one templated task per table (or dynamic mapping) with a pool sized to the API's rate limit; each load idempotent (merge or partition overwrite) into bronze; dbt builds silver and gold after a fan-in; a completeness check counts which tables loaded and fails or flags the run. Mention retries with backoff, alerting per table, and adding a table as a one-line config change.
:::

::: check
?? What is the difference between parse-time DAG generation and run-time dynamic task mapping?
Parse-time generation builds tasks or DAGs from static configuration when the orchestrator reads the code. Run-time mapping creates tasks during a run from data that only exists then, such as the list of files that actually arrived.

?? What decision must you make explicitly at every fan-in point?
What happens when one or more upstream branches fail or are late: fail the whole run, wait, or continue with a recorded completeness flag.

?? What does a consumer gain from the medallion layers if they are implemented properly?
Known guarantees per layer: bronze is complete and replayable, silver is cleaned and tested, gold is owned, documented and contract-backed for business use.
:::

::: resources
- Docs: [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) · top-level code, idempotent tasks, dynamic DAGs and testing
- Docs: [Airflow dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) · `expand`, `partial` and fan-in over mapped results
- Blog: [What is a medallion architecture?](https://www.databricks.com/blog/what-is-medallion-architecture) · Databricks' definition of the bronze, silver and gold layers
- Docs: [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview) · staging, intermediate and marts as the dbt version of the layered pattern
- Docs: [graphlib: functionality to operate with graph-like structures](https://docs.python.org/3/library/graphlib.html) · topological sorting with parallel waves in the standard library
- Book: [Data Pipelines Pocket Reference](https://www.oreilly.com/library/view/data-pipelines-pocket/9781492087823/) · James Densmore; compact patterns for ingestion, transformation and orchestration
:::

### Pipeline Idempotency & Exactly-Once

::: plain
An operation is **idempotent** if doing it twice has the same effect as doing it once, like pressing a lift button: pressing it five times still calls one lift. Pipelines fail and retry all the time, and messages get delivered twice, so every step must be like the lift button. "Exactly-once" sounds like a promise that nothing is ever processed twice; in practice it means the **result** looks as if each record was processed once, because duplicates are made harmless.
:::

**Delivery guarantees.** Between any two systems, a message or batch can be:

| Guarantee | Meaning | How it happens |
|---|---|---|
| At-most-once | Each record is delivered zero or one time; losses possible | Send and never retry |
| At-least-once | Each record is delivered one or more times; duplicates possible | Retry until acknowledged |
| Exactly-once (effectively once) | The effect is as if each record were delivered once | At-least-once delivery plus idempotent or transactional processing |

The key insight: a network cannot guarantee exactly-once **delivery**, because when an acknowledgement is lost the sender cannot tell whether the receiver got the message. So every reliable system retries (at-least-once), and exactly-once is achieved at the **receiving end**, by making duplicates harmless. That is why "make the sink idempotent" is the answer to most exactly-once questions. [Streaming systems](#streaming) covers how Kafka and Flink implement this internally.

**Techniques, from simplest to strongest.**

1. **Overwrite by a deterministic unit.** Replace a whole partition, file or table for a given interval (previous topic). A rerun rewrites the same unit with the same content.
2. **Upsert by key.** `MERGE` on a primary key: a duplicate becomes an update with the same values.
3. **Version or sequence guard.** Apply an update only if its version (event timestamp, log position, sequence number) is newer than the stored one. This also handles out-of-order arrival.
4. **Deduplicate by an idempotency key.** Each record carries a unique ID set by the producer (an event UUID, a payment request ID). The sink remembers IDs it has applied, or deduplicates at read time with `ROW_NUMBER()`.
5. **Commit progress and output together.** Store the input offset in the same transaction as the output (a table of processed offsets, or a table format commit that records it). After a crash, restart from the committed offset; nothing was committed without its output.

A small sink that combines 2, 3 and 4 runs as is in SQLite. The input contains a duplicate delivery and an out-of-order update:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE orders (order_id TEXT PRIMARY KEY, status TEXT, version INT)")
db.execute("CREATE TABLE applied (event_id TEXT PRIMARY KEY)")

events = [  # (event_id, order_id, status, version)
    ("e1", "o1", "created", 1),
    ("e2", "o1", "paid",    2),
    ("e2", "o1", "paid",    2),   # duplicate delivery after a retry
    ("e4", "o1", "shipped", 4),
    ("e3", "o1", "packed",  3),   # arrives late, older than 'shipped'
]

for event_id, order_id, status, version in events:
    with db:  # event bookkeeping and the upsert commit together
        if db.execute("INSERT OR IGNORE INTO applied VALUES (?)", (event_id,)).rowcount == 0:
            continue  # already applied: skip the duplicate
        db.execute("""INSERT INTO orders VALUES (?, ?, ?)
                      ON CONFLICT(order_id) DO UPDATE SET status = excluded.status,
                                                          version = excluded.version
                      WHERE excluded.version > orders.version""",
                   (order_id, status, version))

print(db.execute("SELECT * FROM orders").fetchall())
print(db.execute("SELECT COUNT(*) FROM applied").fetchone()[0], "distinct events applied")
# [('o1', 'shipped', 4)]
# 4 distinct events applied
```

**Read-time deduplication in BigQuery.** When the raw layer is append-only and may contain duplicates, deduplicate once in staging with `QUALIFY`:

```sql
SELECT *
FROM raw.events
WHERE event_date = @run_date
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _loaded_at) = 1;
```

**What Google Cloud services guarantee.**

- **Pub/Sub** delivers at least once by default. **Exactly-once delivery** can be enabled on pull subscriptions: acknowledged messages are not redelivered, within the subscription's region. Duplicates published by the producer (a retried `publish`) are still separate messages, so carry an ID.
- **BigQuery Storage Write API**: the default stream is at-least-once; application-created **committed** streams accept an **offset** with each append and reject an append at an offset that was already written, which gives exactly-once writes when the writer tracks offsets.
- **Dataflow** streaming runs exactly-once by default (it deduplicates internally and checkpoints state); its at-least-once mode is cheaper for idempotent sinks.
- **Load jobs** are atomic: a failed `bq load` writes nothing. Give the job a deterministic job ID (`--job_id=load_orders_20261122`) and a retry of the same submission is rejected as a duplicate job instead of loading twice.

> **Pitfall:** Side effects outside the data. Sending an email, charging a card or calling a partner API from inside a retried task happens again on every retry. Record the side effect with an idempotency key first (or use the API's own idempotency key header) and check it before acting.

> **Pitfall:** Using `INSERT` with auto-generated IDs as the "key". Each retry creates a new ID, so the upsert never matches. The key must come from the data or the producer, never from the sink.

::: deeper
**Why delivery cannot be exactly-once.** This is the Two Generals problem: over an unreliable channel, no finite exchange of acknowledgements lets both sides be certain the other knows. Systems therefore choose between losing messages (at-most-once) and duplicating them (at-least-once), and remove duplicates' effects at the receiver.

**End-to-end means every hop.** A Kafka transaction makes read-process-write inside Kafka exactly-once, but if the next hop is a non-transactional database written with plain inserts, duplicates come back. Check each boundary: source to stream, stream to processor, processor to sink, sink to consumers. The [PySpark + Kafka guide](pyspark-streaming-kafka-topic-map.html#ft) shows Spark's checkpoint-based guarantees, and its [Kafka transactions](pyspark-streaming-kafka-topic-map.html#ktxn) section shows the producer side.

**Dedup windows.** Remembering every event ID forever is expensive. Most systems keep IDs for a window (Pub/Sub exactly-once and BigQuery's legacy `insertId` deduplication are best understood as windowed), so a duplicate that arrives after the window passes through. Pair a short dedup window in the stream with a full deduplication in the warehouse.

**Transactional outbox.** When an application must update its database and publish an event, doing both separately risks one without the other. The outbox pattern writes the event into a table in the same database transaction and lets CDC publish it; see [Event-driven architecture](#events).

**Determinism again.** Replay gives the same result only if the logic is deterministic: no `now()`, no unseeded randomness, no lookups against a changing service. A pipeline can be "exactly-once" and still produce different numbers on replay.

> **Interview:** "How do you make a Pub/Sub to BigQuery pipeline exactly-once?" Producers attach a unique event ID; the subscriber (Dataflow, or a BigQuery subscription with at-least-once semantics) writes to a raw table; the staging model deduplicates on event ID with `QUALIFY`; downstream marts are built with merge or partition overwrite so reruns are idempotent. Optionally enable Pub/Sub exactly-once delivery or Storage Write API committed streams with offsets, but still dedupe in the warehouse because producer retries create true duplicates.
:::

::: check
?? Why is exactly-once delivery impossible over a network, and how do real systems get exactly-once results?
A lost acknowledgement leaves the sender unable to tell whether the message arrived, so it must either retry (risking duplicates) or not (risking loss). Systems retry (at-least-once) and make the receiver idempotent or transactional so duplicates have no effect.

?? An event-processing task sends a welcome email and is retried after a timeout. What goes wrong and how do you prevent it?
The email is sent twice. Record the side effect under an idempotency key (for example the user ID plus event type) and check it before sending, or use the email provider's idempotency key.

?? Why does a version guard (`WHERE excluded.version > orders.version`) matter in addition to deduplication?
Deduplication removes repeated copies of the same event, but events can also arrive out of order. The version guard stops an older event from overwriting newer state.
:::

::: resources
- Blog: [Exactly-once semantics are possible: here's how Kafka does it](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/) · idempotent producers and transactions explained by Kafka's designers
- Docs: [Pub/Sub exactly-once delivery](https://cloud.google.com/pubsub/docs/exactly-once-delivery) · what it guarantees, its regional limits and how acknowledgements work
- Docs: [BigQuery Storage Write API: stream types and offsets](https://cloud.google.com/bigquery/docs/write-api) · committed streams, offsets and exactly-once writes
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · Kleppmann; chapters on transactions, and on stream processing and idempotence
- Article: [You cannot have exactly-once delivery](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/) · Tyler Treat's short, clear argument and what to do instead
:::

## dbt

The previous group described patterns; this group covers the tool most teams use to implement the SQL part of them. **dbt** (data build tool) is to warehouse SQL what a build system is to application code: you write `SELECT` statements, and dbt works out the order, wraps them in the right DDL and DML, runs them, tests the results and documents everything.

dbt already appeared twice in this course. [Analytics engineering and BI](#analytics) covered the analytics engineer's workflow, the staging, intermediate and marts layout, and CI for SQL. [Data modeling](#modeling) covered documentation-first modeling with dbt: YAML descriptions, model contracts, exposures, docs and the lineage graph. The two topics here go further into how dbt works inside and the features you need on a large project: incremental strategies, snapshots, source freshness, macros, packages, custom tests, hooks and multi-project setups. Dataform, the equivalent built into BigQuery, follows the same ideas and is covered in [Pipelines on Google Cloud](#gcppipes).

### dbt — Core to Advanced

::: plain
dbt is a recipe book for your warehouse. Each recipe (a **model**) is a `SELECT` statement that says which other recipes or raw ingredients it uses. dbt reads all the recipes, works out the order in which to cook them, cooks each one into a table or view, and then tastes the results with tests. You never write `CREATE TABLE` or `INSERT` yourself; you describe what the result should contain, and dbt handles how to build it.
:::

**A project in one picture.** A dbt project is a folder:

```text
acme_dbt/
  dbt_project.yml          project name, folder-level configs, variables
  models/
    staging/shop/          stg_shop__orders.sql, _shop__sources.yml
    marts/finance/         fct_orders.sql, _finance__models.yml
  snapshots/               history tables (type 2 slowly changing dimensions)
  seeds/                   small CSV files loaded as tables (country codes)
  tests/                   singular tests: SQL that returns failing rows
  macros/                  reusable Jinja functions
  packages.yml             third-party packages
```

Connection details live in `profiles.yml` (outside the repository) or in environment variables. For BigQuery, a profile names the project, the dataset dbt writes to, the location and how to authenticate (OAuth or a service account; on Google Cloud, prefer the runtime's attached service account over key files).

A model is a `SELECT` with references:

```sql
-- models/marts/finance/fct_orders.sql
select
    o.order_id,
    o.customer_id,
    o.ordered_at,
    sum(p.amount) as amount_paid
from {{ ref('stg_shop__orders') }} as o
left join {{ ref('stg_shop__payments') }} as p using (order_id)
group by 1, 2, 3
```

`ref('stg_shop__orders')` does two things: it resolves to the real table name for the current environment (`acme-dev.dbt_ada.stg_shop__orders` in development, `acme-prod.staging.stg_shop__orders` in production), and it records a dependency edge. `source('shop', 'orders')` does the same for raw tables that dbt did not build.

**What happens when you type `dbt build`.** dbt Core works in four stages:

1. **Parse.** Read every SQL and YAML file, render the Jinja just enough to find `ref`, `source` and `config` calls, and build the **manifest**: every node (model, test, seed, snapshot, source, exposure), its configuration and its edges. With partial parsing, only changed files are re-parsed.
2. **Select.** Apply node selection (`--select`, `--exclude`) to the graph.
3. **Compile.** Render each selected node's Jinja into plain SQL (written to `target/compiled/`).
4. **Execute.** Walk the graph in dependency order with a pool of `threads`, run each node's **materialization** (a macro that wraps the compiled `SELECT` in the right statements for the warehouse, through the **adapter**, here dbt-bigquery), and record results. With `dbt build`, each model's tests run right after it, and a failing test skips everything downstream.

The run leaves **artifacts** in `target/`: `manifest.json` (the whole project graph), `run_results.json` (status and timing of each node), `catalog.json` (column types from the warehouse, from `dbt docs generate`) and `sources.json` (freshness results). CI, orchestrators, catalogs and lineage tools all read these files.

**Materializations.** How each model is stored:

| Materialization | What dbt runs on BigQuery | Use for |
|---|---|---|
| `view` | `CREATE OR REPLACE VIEW` | Light staging models, always fresh |
| `table` | `CREATE OR REPLACE TABLE ... AS SELECT` | Marts that are rebuilt fully each run |
| `incremental` | First run: create table; later runs: merge or overwrite only new data | Large facts and events |
| `ephemeral` | Nothing; inlined as a CTE into models that use it | Small reusable steps |
| `materialized_view` | `CREATE MATERIALIZED VIEW` | Simple aggregates BigQuery can refresh itself |

**Incremental models** are where most of the engineering lives, because they implement the load patterns from Batch Processing Patterns. On BigQuery there are three strategies:

```sql
-- merge (default): upsert by unique_key
{{ config(materialized='incremental', unique_key='order_id',
          partition_by={'field': 'order_date', 'data_type': 'date'},
          cluster_by=['customer_id']) }}
select * from {{ ref('stg_shop__orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

```sql
-- insert_overwrite: replace whole partitions; the lookback window from earlier
{{ config(materialized='incremental', incremental_strategy='insert_overwrite',
          partition_by={'field': 'event_date', 'data_type': 'date'}) }}
select event_date, count(*) as events
from {{ ref('stg_events') }}
{% if is_incremental() %}
  where event_date >= date_sub(current_date(), interval 3 day)
{% endif %}
group by 1
```

```sql
-- microbatch (dbt 1.9 and later): dbt splits the work into time batches itself
{{ config(materialized='incremental', incremental_strategy='microbatch',
          event_time='event_ts', batch_size='day', begin='2024-01-01', lookback=2,
          partition_by={'field': 'event_ts', 'data_type': 'timestamp', 'granularity': 'day'}) }}
select * from {{ ref('stg_events') }}   -- no is_incremental() filter needed
```

`{{ this }}` is the model's own existing table. `is_incremental()` is true only when the table already exists, is a table, and the run is not a `--full-refresh`. With **microbatch**, dbt filters each upstream model that declares an `event_time` to one batch at a time, runs each batch as a separate, idempotent query, retries only failed batches with `dbt retry`, and backfills a range with `dbt run --event-time-start 2026-10-01 --event-time-end 2026-11-01`. Note that the `insert_overwrite` example uses `current_date()`, exactly the clock dependence warned against earlier; microbatch removes it.

**Snapshots** record history for tables that overwrite rows in place, producing a type 2 slowly changing dimension (see [Data modeling](#modeling) for SCD design). Since dbt 1.9 they are configured in YAML:

```yaml
snapshots:
  - name: snap_customers
    relation: source('shop', 'customers')
    config:
      schema: snapshots
      unique_key: customer_id
      strategy: timestamp        # or 'check' with check_cols when there is no updated_at
      updated_at: updated_at
      hard_deletes: new_record   # record deletions as a new row instead of ignoring them
```

Each run compares current rows with the snapshot table and closes changed rows by setting `dbt_valid_to`, inserting new versions with `dbt_valid_from`. A snapshot only sees the state at the moment it runs: two changes between runs collapse into one. If you need every change, use CDC from [Data ingestion](#ingestion).

**Sources and freshness.** Declaring sources gives lineage from raw tables and lets dbt check whether they are current:

```yaml
sources:
  - name: shop
    database: acme-raw
    schema: shop
    loaded_at_field: _loaded_at
    freshness:
      warn_after: {count: 6, period: hour}
      error_after: {count: 24, period: hour}
    tables:
      - name: orders
      - name: customers
```

`dbt source freshness` queries `max(_loaded_at)` per table and writes `sources.json`. Run it before the build and stop if a critical source is stale: building marts on yesterday's data and calling them fresh is worse than not building. Recent dbt versions also accept these keys under a `config:` block.

**Tests and selection.** Generic data tests (`unique`, `not_null`, `accepted_values`, `relationships`) are declared in YAML under `data_tests:`; singular tests are SQL files returning failing rows; unit tests (dbt 1.8 and later) check one model's logic against hand-written inputs. Node selection then lets you work on a slice of a large graph:

| Command | Meaning |
|---|---|
| `dbt build --select fct_orders` | Build and test one model |
| `dbt build --select +fct_orders` | It and everything upstream |
| `dbt build --select stg_shop__orders+` | It and everything downstream |
| `dbt build --select tag:hourly` | Every node tagged `hourly` |
| `dbt build --select state:modified+ --defer --state prod-artifacts/` | Only changed models and their children, reading unchanged parents from production |
| `dbt retry` | Rerun only what failed or was skipped last time |
| `dbt run --full-refresh --select fct_orders` | Rebuild an incremental model from scratch |
| `dbt show --select fct_orders --limit 10` | Preview a model's output without building it |

> **Pitfall:** The merge example filters on `max(updated_at)` from the target. If a run fails halfway through an upstream load, or rows arrive with older timestamps, they are skipped forever. Prefer a lookback (`updated_at > max(updated_at) - interval 3 day`) combined with `unique_key`, or microbatch.

> **Tip:** Run `dbt compile --select my_model` and read `target/compiled/.../my_model.sql` when Jinja confuses you. The compiled SQL is exactly what the warehouse receives.

::: deeper
**How `is_incremental()` and `{{ this }}` interact with environments.** In a fresh CI dataset the table does not exist, so incremental models build fully, which can be slow and expensive. Use `--defer --state` so unchanged parents are read from production, `--empty` (dbt 1.8 and later) to build zero-row versions that only validate SQL and schemas, or limit CI data with a variable in the model's filter.

**Merge cost.** The default merge scans the whole target table to find matches. `incremental_predicates` adds conditions to the merge's `ON` clause so BigQuery prunes target partitions, for example `["DBT_INTERNAL_DEST.order_date >= date_sub(current_date(), interval 7 day)"]`. With `insert_overwrite`, dbt computes the partitions to replace from the new data (dynamic mode) or from a list you give (static `partitions` config, cheaper because it avoids a query to find them).

**`on_schema_change`.** When an incremental model's columns change, `ignore` (default) keeps the old columns, `fail` stops, `append_new_columns` adds new ones, and `sync_all_columns` adds and removes. The Schema Evolution topic later in this section discusses when to use which.

**Python models.** A model can be a Python function returning a DataFrame. On BigQuery, dbt submits it to Managed Service for Apache Spark (its serverless batches or an existing cluster), so it is slower to start than SQL; use it for logic SQL cannot express.

**Engines.** dbt Core is the open-source Python engine (Apache 2.0). In 2025 dbt Labs announced the **dbt Fusion engine**, a rewrite in Rust that parses projects much faster and understands SQL (it can detect column errors before sending queries to the warehouse); check its current status and adapter support before adopting it. The commands and project format are the same.

> **Interview:** "Your incremental fact table slowly drifts away from the source. Why might that be?" Typical causes: the incremental filter uses `max(updated_at)` and skips late or back-dated rows; no `unique_key`, so updates append duplicates; a non-deterministic filter based on `current_date()`; or schema changes ignored by `on_schema_change`. Fixes: a lookback window with merge or `insert_overwrite`, microbatch with `lookback`, a periodic full refresh or reconciliation test against the source, and alerts on schema changes.
:::

::: check
?? What does dbt do in the parse stage, and which artifact does it produce?
It reads all SQL and YAML files, finds `ref`, `source` and `config` calls, and builds the project graph with every node's configuration. The result is `manifest.json`.

?? When is `is_incremental()` false even though the model is configured as incremental?
When the target table does not exist yet, when the existing relation is not a table (for example a view), or when the run uses `--full-refresh`.

?? Why can a snapshot miss changes, and what would you use instead?
It records only the state at the moment it runs, so several changes between two runs collapse into one. Use CDC to capture every change.
:::

::: resources
- Docs: [dbt documentation: introduction](https://docs.getdbt.com/docs/introduction) · the official guide to models, tests, sources and commands
- Docs: [About incremental strategy](https://docs.getdbt.com/docs/build/incremental-strategy) · merge, insert_overwrite, microbatch and which adapters support each
- Docs: [Snapshots](https://docs.getdbt.com/docs/build/snapshots) · YAML configuration, strategies and hard deletes
- Docs: [dbt artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts) · what manifest.json, run_results.json, catalog.json and sources.json contain
- Course: [dbt Learn](https://learn.getdbt.com/) · free official courses from fundamentals to advanced materializations
- Book: [Analytics Engineering with SQL and dbt](https://www.oreilly.com/library/view/analytics-engineering-with/9781098142377/) · Rui Machado and Hélder Russa; dbt projects end to end
:::

### dbt — Advanced Patterns

::: plain
Once a dbt project grows past a few dozen models, you start repeating yourself: the same date logic in twenty models, the same test written for every table, the same permission grant after every build. dbt's advanced features are about not repeating yourself. **Macros** are reusable snippets, **packages** are other people's macros you install, **custom tests** are your own reusable checks, **hooks** run extra statements around builds, and **dbt Mesh** lets several teams' projects reference each other safely, like libraries in a large codebase.
:::

**Jinja in one minute.** dbt renders every SQL and YAML file through **Jinja**, a templating language. `{{ ... }}` outputs a value, `{% ... %}` runs logic, `{# ... #}` is a comment. Useful context variables include `target` (the current environment: `target.name`, `target.schema`), `var('name')` (project variables), `env_var('NAME')` and `this`.

```sql
{% set payment_methods = ['card', 'bank_transfer', 'voucher'] %}
select
    order_id,
    {% for m in payment_methods %}
    sum(case when method = '{{ m }}' then amount else 0 end) as {{ m }}_amount
    {%- if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_shop__payments') }}
group by 1
```

This compiles to a `SELECT` with three `sum(case ...)` columns. Jinja runs before the warehouse sees anything; it only produces text.

**Macros.** A macro is a Jinja function in the `macros/` folder:

```sql
-- macros/cents_to_currency.sql
{% macro cents_to_currency(column, scale=2) %}
    round(cast({{ column }} as numeric) / 100, {{ scale }})
{% endmacro %}
```

Used as `{{ cents_to_currency('amount_cents') }} as amount`. Macros can also query the warehouse while compiling with `run_query`, for example to fetch the distinct payment methods instead of hard-coding them. Wrap such code in `{% if execute %}`, because during parsing no queries run and the result is empty.

**Overriding dbt's own macros.** Much of dbt's behaviour is implemented as macros you can replace. The best-known is `generate_schema_name`, which decides which dataset a model goes to. By default a model with `schema='finance'` lands in `<target_schema>_finance` (for example `dbt_ada_finance` in development). A common override keeps that in development but writes to exactly `finance` in production:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- elif custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

**Cross-database macros with `adapter.dispatch`.** A package that must work on BigQuery, Snowflake and Postgres defines a default implementation and adapter-specific ones (`bigquery__my_macro`); `adapter.dispatch('my_macro')` picks the right one at run time.

**Packages.** Installed from `packages.yml` with `dbt deps`, which writes a `package-lock.yml` pinning exact versions:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
  - package: dbt-labs/codegen
    version: [">=0.13.0", "<1.0.0"]
```

| Package | What it gives you |
|---|---|
| dbt-utils | `generate_surrogate_key`, `star` (all columns except some), `union_relations`, `date_spine`, `pivot`; tests such as `unique_combination_of_columns`, `expression_is_true`, `accepted_range`, `recency` |
| codegen | Generates source YAML and staging models from existing tables |
| audit_helper | Compares two relations row by row, for refactoring safely |
| dbt-expectations | Great Expectations-style tests written as dbt macros |
| dbt-project-evaluator | Lints the project itself: models without tests, direct source references in marts |

**Custom generic tests.** When the same rule appears on many columns, write it once. A generic test is a macro-like block that returns failing rows:

```sql
-- tests/generic/is_positive.sql
{% test is_positive(model, column_name, allow_zero=false) %}
select *
from {{ model }}
where {{ column_name }} {{ '<' if allow_zero else '<=' }} 0
{% endtest %}
```

and is used in YAML like a built-in, with configuration controlling severity and scope:

```yaml
models:
  - name: fct_orders
    columns:
      - name: amount_paid
        data_tests:
          - is_positive:
              allow_zero: true
              config:
                severity: error
                warn_if: ">0"
                error_if: ">100"          # tolerate a few, fail on many
                where: "order_date >= date_sub(current_date(), interval 7 day)"
                store_failures: true      # keep failing rows in an audit table
```

`where` restricts the test to recent partitions, which keeps test cost proportional to the change on BigQuery. `store_failures` writes the failing rows to a table (in a dataset ending `_dbt_test__audit` by default) so you can inspect them.

**Hooks.** Statements that run around builds:

| Hook | When it runs | Typical use |
|---|---|---|
| `pre-hook` / `post-hook` (model config) | Before or after one model | Write an audit row, set a table option |
| `on-run-start` / `on-run-end` (project) | Once per invocation | Create datasets, log the run, upload artifacts |

```yaml
# dbt_project.yml
on-run-end:
  - "{{ log_run_results(results) }}"   # a macro that inserts one row per node into an audit table
models:
  acme_dbt:
    marts:
      +grants:
        roles/bigquery.dataViewer: ["group:analysts@acme.example"]
```

Prefer the `grants` config over hand-written `GRANT` statements in hooks: dbt applies grants consistently after each build, and on BigQuery it maps them to IAM roles on the table.

**dbt Mesh: several projects, one graph.** A single project with 3,000 models and ten teams becomes slow to parse, hard to review and unclear to own. dbt Mesh splits it into projects per domain that reference each other through explicit interfaces:

- **Groups and access.** Each model belongs to a group with an owner and has `access: private` (only its group), `protected` (its project, the default) or `public` (other projects).
- **Contracts and versions** on public models, so consumers get a stable schema and time to migrate (covered in [Data modeling](#modeling)).
- **Cross-project refs.** The consuming project lists the producer in `dependencies.yml` and writes `{{ ref('finance', 'fct_revenue') }}`. It reads the producer's published model; it does not rebuild it.

Cross-project `ref` resolution is a dbt platform Enterprise feature, because it relies on the platform's metadata of the upstream project's production state. Teams on dbt Core use the community `dbt-loom` plugin or install the upstream project as a package (which rebuilds rather than references it).

**dbt Core, dbt platform and where to run it.**

| | dbt Core | dbt platform (formerly dbt Cloud) |
|---|---|---|
| Licence and cost | Open source, free | Commercial, per seat and usage |
| Development | Your editor and terminal (VS Code extension available) | Browser IDE plus a cloud-connected CLI |
| Scheduling | Your orchestrator (Managed Service for Apache Airflow (formerly Cloud Composer), Cloud Run jobs, GitHub Actions) | Built-in jobs, CI on pull requests, state-aware runs |
| Extras | Community tooling | Hosted docs and lineage, Semantic Layer API, cross-project refs |

On Google Cloud, a common dbt Core setup is a container image in Artifact Registry, run as a Cloud Run job (or a Composer task) with a service account that has BigQuery roles, triggered by Cloud Scheduler or the orchestrator. Dataform is the built-in alternative when the whole stack is BigQuery.

> **Pitfall:** Macro-heavy projects. When every model is three lines calling a macro that generates 200 lines, nobody can read or review the SQL. Use macros for genuinely repeated logic; keep business logic visible in the model.

> **Tip:** Install `dbt-project-evaluator` early and run it in CI. It turns the structure conventions (staging only reads sources, marts only read staging or intermediate, every model has a primary-key test) into failing checks.

::: deeper
**Custom materializations.** A materialization is itself a macro (`{% materialization my_mat, adapter='bigquery' %}`) that receives the compiled SQL and decides what statements to run. Teams write custom ones for patterns dbt lacks, such as write-audit-publish (build into a staging table, test, then swap). Copy the built-in `table` materialization as a starting point; the dbt guide on creating materializations walks through it.

**Parsing performance.** Parsing time grows with Jinja complexity and project size. Keep partial parsing on (`partial_parse.msgpack` in `target/`), avoid `run_query` at parse time, and split very large projects (Mesh). Large projects on dbt Core can take minutes just to parse; this was one of the motivations for the Rust-based Fusion engine.

**Testing macros.** Macros are code without types. Test them with unit tests on the models that use them, or with a small integration-test project that compiles and runs them against a test dataset in CI.

**Alternatives.** **SQLMesh** (open source; its company, Tobiko Data, was acquired by Fivetran in 2025) parses SQL into a syntax tree, offers column-level lineage, virtual development environments that avoid rebuilding unchanged tables, and plans that show the effect of a change before applying it. **Dataform** is free inside BigQuery and uses SQLX with similar concepts. In October 2025 Fivetran and dbt Labs announced a merger; dbt Core remains open source under Apache 2.0.

> **Interview:** "How would you organise dbt for 12 teams and 2,500 models?" Split by domain into projects (Mesh) with public, contracted and versioned interface models; groups and owners for every model; shared macros and conventions in an internal package; `dbt-project-evaluator` and slim CI (`state:modified+` with `--defer`) on every pull request; per-team schedules with tags or separate jobs; artifacts shipped to a catalog for lineage. Mention the cost angle: incremental models with partition pruning and tests restricted with `where`.
:::

::: check
?? Why should code that uses `run_query` in a macro be wrapped in `{% if execute %}`?
Because dbt renders Jinja during parsing too, when no queries are executed and `run_query` returns nothing. The `execute` flag is false during parsing and true during execution, so the guard avoids errors and wrong output.

?? What do `warn_if`, `error_if` and `where` on a test do?
`warn_if` and `error_if` set thresholds on the number of failing rows for each severity (for example warn on any, fail above 100). `where` restricts the test to a subset of rows, such as recent partitions, which reduces cost.

?? What does a model's `access: public` setting mean in dbt Mesh, and what usually accompanies it?
Other projects may reference the model with a cross-project `ref`. Public models usually have an enforced contract and versions so their schema stays stable for consumers.
:::

::: resources
- Docs: [Jinja and macros](https://docs.getdbt.com/docs/build/jinja-macros) · Jinja syntax, macros, `run_query` and the `execute` flag
- Repo: [dbt-utils](https://github.com/dbt-labs/dbt-utils) · the standard macro and test library with documentation for each macro
- Docs: [Writing custom generic data tests](https://docs.getdbt.com/best-practices/writing-custom-generic-tests) · test blocks, arguments and configuration
- Docs: [Project dependencies (dbt Mesh)](https://docs.getdbt.com/docs/mesh/govern/project-dependencies) · cross-project refs, `dependencies.yml` and their requirements
- Docs: [Hooks and operations](https://docs.getdbt.com/docs/build/hooks-operations) · pre-hooks, post-hooks, on-run-start and on-run-end
- Docs: [Create new materializations](https://docs.getdbt.com/guides/create-new-materializations) · how materialization macros work, for custom write patterns
- Repo: [dbt-project-evaluator](https://github.com/dbt-labs/dbt-project-evaluator) · lint your project against dbt Labs' best practices
:::

## Connectors

dbt transforms data that is already in the warehouse; something has to put it there. [Data ingestion](#ingestion) covered the hard, custom cases: CDC with Debezium and Datastream, Kafka Connect, APIs, webhooks and files. For the long tail of ordinary sources (a CRM, an ads platform, a billing tool, a support desk), most teams do not write extraction code at all. They use **connectors**: ready-made programs that know one source's API, its pagination, its rate limits and its quirks, and land the data in a standard shape.

This group compares the three main options: managed connector services (Fivetran, Airbyte Cloud), the open-source Singer ecosystem with Meltano, and Apache NiFi, a visual flow engine for routing and transforming data in motion. The question running through all three is build versus buy: what you get from a connector, what it costs, and what it still leaves for you to do.

### Fivetran & Airbyte — Managed Connectors

::: plain
Writing your own code to copy data from Salesforce, then from Stripe, then from Zendesk, is like building your own delivery van for every supplier. A managed connector service is a courier company: you tell it where to collect and where to deliver, and it handles the routes, the traffic and the breakdowns. **Fivetran** is a fully managed courier you pay per delivery. **Airbyte** offers the same service, but you can also run its vans yourself because the software is open.
:::

**What a connector actually does.** For each source a connector handles:

1. **Authentication**: OAuth flows, API keys, token refresh.
2. **Discovery**: list the available objects (streams) and their fields.
3. **Historical sync**: the first full copy, paginated and throttled.
4. **Incremental sync**: only what changed since the last run, using a **cursor** (an `updated_at` field, an event ID) or the source's change feed. For databases, log-based CDC.
5. **Rate limits and retries**: back off on HTTP 429, resume after failures from saved state.
6. **Schema drift**: new fields and tables appear; the connector decides what to do.
7. **Loading**: write to the destination (BigQuery, Snowflake, a lakehouse) with metadata columns, then commit its state.

Steps 3 to 6 are where custom scripts break. A vendor that runs the same connector for thousands of customers sees every API change first.

**Fivetran** is a fully managed service. You configure a connection in the UI, API or Terraform provider, choose the schema, tables and columns to sync and a sync frequency, and Fivetran runs it. Things worth knowing:

- **System columns.** Every synced table gets `_fivetran_synced` (when the row was last written). Where the source reports deletes, rows are soft-deleted with `_fivetran_deleted = true` rather than removed, so your models must filter them. Optional **history mode** keeps every version of a row with `_fivetran_start`, `_fivetran_end` and `_fivetran_active`, which gives you a type 2 history without snapshots.
- **Schema changes.** A connection-level policy decides what happens when the source adds tables or columns: allow all, allow new columns only, or block all (new items are excluded until someone enables them).
- **Pricing** is mainly usage-based on **monthly active rows (MAR)**: distinct primary keys inserted, updated or deleted in a month. A table where the same 100,000 rows change every day costs about 100,000 MAR a month, not 3 million. A full re-sync or a source that touches every row daily can multiply the bill.
- **Custom sources.** The **Connector SDK** lets you write a source in Python that Fivetran hosts and runs; database replication uses log-based CDC.

**Airbyte** is open source (self-managed with the `abctl` tool for a single machine or a Helm chart on Kubernetes, such as GKE) and also sold as Airbyte Cloud. Its design is open, which makes it a good way to see how connectors work:

- Each **connector is a container image** implementing a small protocol. A source answers `spec` (what configuration it needs), `check` (can I connect?), `discover` (what streams exist?) and `read` (emit records). A destination implements `write`.
- Connectors talk through the **Airbyte protocol**: JSON messages on standard output, of types `RECORD`, `STATE`, `LOG`, `TRACE`, `CATALOG` and others. `STATE` messages are checkpoints: the platform saves them only after the destination confirms that the preceding records are committed, which is what makes a sync resumable.
- **Sync modes** per stream:

| Sync mode | What happens in the destination |
|---|---|
| Full refresh, overwrite | Each sync replaces the table |
| Full refresh, append | Each sync appends a complete new copy |
| Incremental, append | New and changed records are appended (history with duplicates) |
| Incremental, append + deduped | Records appended to a raw history, and a final table keeps the latest per primary key |

- **Typing and deduping.** Destinations write records first as JSON to raw tables (in an internal dataset such as `airbyte_internal` on BigQuery), then build the final typed tables. A value that cannot be cast to its declared type becomes `NULL`, and the error is recorded in the row's `_airbyte_meta` column instead of failing the sync. Other metadata columns include `_airbyte_raw_id` and `_airbyte_extracted_at`. Newer destination versions can write typed tables more directly; check your destination's documentation.
- **Building connectors.** The **Connector Builder** is a UI that produces a declarative YAML connector for typical REST APIs (authentication, pagination, incremental cursors) without code; the Python CDK covers the rest; **PyAirbyte** runs connectors inside a Python script or notebook.

The typing-and-deduping step is worth understanding, because every connector tool does some version of it. A minimal version runs as is:

```python
import json

schema = {"id": int, "email": str, "plan": str, "seats": int}
raw = [  # what the connector wrote to the raw table, one JSON blob per record
    {"_raw_id": "r1", "_extracted_at": "2026-11-22T10:00", "data": '{"id": 1, "email": "a@x.io", "plan": "pro", "seats": 5}'},
    {"_raw_id": "r2", "_extracted_at": "2026-11-22T10:00", "data": '{"id": 2, "email": "b@x.io", "plan": "free", "seats": "ten"}'},
    {"_raw_id": "r3", "_extracted_at": "2026-11-23T10:00", "data": '{"id": 1, "email": "a@x.io", "plan": "team", "seats": 8}'},
]

def cast(value, typ):
    if value is None or isinstance(value, typ):
        return value
    return typ(value)                      # raises ValueError if impossible

final = {}
for rec in sorted(raw, key=lambda r: r["_extracted_at"]):
    data, row, errors = json.loads(rec["data"]), {}, []
    for col, typ in schema.items():
        try:
            row[col] = cast(data.get(col), typ)
        except (TypeError, ValueError):
            row[col] = None                # keep the row, null the bad field
            errors.append(f"{col}: cannot cast {data.get(col)!r} to {typ.__name__}")
    row["_meta"] = {"errors": errors}
    final[row["id"]] = row                 # dedupe: latest extraction per primary key wins

for row in final.values():
    print(row)
# {'id': 1, 'email': 'a@x.io', 'plan': 'team', 'seats': 8, '_meta': {'errors': []}}
# {'id': 2, 'email': 'b@x.io', 'plan': 'free', 'seats': None, '_meta': {'errors': ["seats: cannot cast 'ten' to int"]}}
```

**Choosing.**

| Option | You operate | Strengths | Weaknesses |
|---|---|---|---|
| Fivetran | Nothing | Breadth and reliability of connectors, little effort | Cost at high change volumes, closed source |
| Airbyte Cloud | Nothing | Open protocol, Connector Builder, capacity-based plans | Connector quality varies by support level |
| Airbyte self-managed | Kubernetes or a VM | Free software, data stays in your project | You run upgrades, scaling and monitoring |
| Datastream | Nothing | Native CDC from MySQL, PostgreSQL, Oracle, SQL Server into BigQuery | Databases only, no SaaS APIs |
| Custom code | Everything | Full control, unusual sources | Every API change is your incident |

> **Pitfall:** Treating connector output as clean. Connector tables mirror the source's quirks: soft-deleted rows, nested JSON, fields whose meaning changed in 2023. Always put staging models between connector tables and marts, and filter `_fivetran_deleted` or the equivalent there.

> **Pitfall:** Letting a connector sync "all tables, all columns". It costs more, copies personal data you did not need, and every new source table becomes a surprise in your warehouse. Select explicitly, and block new tables by default for sources with sensitive data.

::: deeper
**How incremental syncs go wrong in APIs.** Many SaaS APIs let records change without updating the field the connector uses as its cursor (a Salesforce formula field, a Stripe balance recomputed later), or return records out of order within a page. Connectors compensate with lookback windows (re-reading the last N hours each sync) and periodic re-syncs. When numbers drift, check which cursor the connector uses for that stream, and whether a historical re-sync fixes it.

**Deletes.** APIs often have no way to report deletions. Connectors then either never remove rows, or detect deletes by comparing full key lists on some schedule. Ask how each important stream handles deletes before trusting counts.

**Where the data flows.** With a fully managed service, data passes through the vendor's infrastructure on its way to your warehouse. For regulated data, look at the vendor's processing regions and at hybrid options where the vendor's control plane manages workers that run inside your own Google Cloud project, so records never leave it. Self-managed Airbyte on GKE keeps everything in your project by design.

**Lock-in.** Your staging models depend on vendor-specific metadata (`_fivetran_synced`, `_airbyte_meta`) and on each connector's table layout. Switching vendors means rewriting staging; keep that layer thin and consistent so a switch is contained.

**Industry consolidation.** Fivetran acquired Census (reverse ETL) and Tobiko Data (SQLMesh) in 2025 and announced a merger with dbt Labs in October 2025. Expect bundles across extraction, transformation and activation; evaluate each part on its merits.

> **Interview:** "Build or buy for 25 SaaS sources?" Buy for standard sources (Fivetran or Airbyte), because maintenance of API changes dominates the cost of custom code. Build only for sources without a good connector, with strict latency or volume needs, or where cost at high change volume outweighs engineering time. Mention MAR-based cost estimation from the source's change rate, staging models to isolate vendor quirks, explicit table and column selection for privacy, and monitoring sync freshness per connection.
:::

::: check
?? A table has 2 million rows; about 50,000 distinct rows change each day, but always the same 50,000. Roughly how many monthly active rows does it generate in Fivetran?
About 50,000, because MAR counts distinct primary keys changed in the month, not total change events.

?? In Airbyte, when is a `STATE` message saved, and why does that matter?
Only after the destination confirms that the records before it are committed. That makes syncs resumable without losing records: after a failure, the sync restarts from the last confirmed checkpoint (and may re-send some records, so loading must be idempotent).

?? Why should models never read connector tables directly in marts?
Connector tables carry source quirks and vendor metadata (soft deletes, nested JSON, type-cast errors). A staging layer cleans them once and isolates the rest of the project from vendor-specific details.
:::

::: resources
- Docs: [Fivetran documentation](https://fivetran.com/docs) · connectors, sync behaviour, system columns, history mode and the Connector SDK
- Docs: [Airbyte documentation](https://docs.airbyte.com/) · deployment, sync modes, Connector Builder and destinations
- Docs: [Airbyte protocol](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol) · the message types and commands every connector implements
- Docs: [Airbyte typing and deduping](https://docs.airbyte.com/using-airbyte/core-concepts/typing-deduping) · raw tables, final tables and `_airbyte_meta`
- Docs: [Datastream overview](https://cloud.google.com/datastream/docs/overview) · Google Cloud's managed CDC service, for comparison
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley on build versus buy and managed ingestion
:::

### Meltano & Singer Ecosystem

::: plain
**Singer** is an agreement about the shape of a plug. Any program that reads from a source (a **tap**) prints records in a standard text format, and any program that writes to a destination (a **target**) reads that format, so any tap can be connected to any target with a simple pipe, like any lamp fitting any socket. **Meltano** is the toolbox around those plugs: it installs taps and targets, stores their settings and progress, and runs them together with tools such as dbt.
:::

**The Singer specification.** Singer, created at Stitch around 2017, defines a tiny protocol. A tap writes JSON messages to standard output, one per line; a target reads them from standard input:

- `SCHEMA`: the stream's name, its JSON Schema and its key properties.
- `RECORD`: one row for a stream.
- `STATE`: a bookmark, such as the latest `updated_at` seen per stream. The target emits it after the preceding records are safely written; the runner stores it and passes it back to the tap next time.

```bash
tap-github --config tap.json --discover > catalog.json          # list streams and fields
tap-github --config tap.json --catalog catalog.json --state state.json \
  | target-bigquery --config target.json > new_state.json       # extract, load, save progress
```

The **catalog** produced by discovery lists every stream, and its metadata marks which are `selected` and their **replication method**: `FULL_TABLE`, `INCREMENTAL` (with a `replication-key`) or `LOG_BASED` (CDC).

The whole protocol fits in a short program. This tap and target run as is, connected by an in-memory pipe:

```python
import io, json

SOURCE = [  # pretend API rows
    {"id": 1, "title": "Fix login", "updated_at": "2026-11-20T09:00:00Z"},
    {"id": 2, "title": "Add export", "updated_at": "2026-11-21T14:30:00Z"},
    {"id": 3, "title": "Dark mode",  "updated_at": "2026-11-22T08:15:00Z"},
]

def tap(state, out):
    """A Singer tap: writes SCHEMA, RECORD and STATE messages, one JSON object per line."""
    bookmark = state.get("bookmarks", {}).get("issues", "")
    out.write(json.dumps({"type": "SCHEMA", "stream": "issues", "key_properties": ["id"],
                          "schema": {"properties": {"id": {"type": "integer"},
                                                    "title": {"type": "string"},
                                                    "updated_at": {"type": "string", "format": "date-time"}}}}) + "\n")
    for row in SOURCE:
        if row["updated_at"] > bookmark:
            out.write(json.dumps({"type": "RECORD", "stream": "issues", "record": row}) + "\n")
            bookmark = row["updated_at"]
    out.write(json.dumps({"type": "STATE", "value": {"bookmarks": {"issues": bookmark}}}) + "\n")

def target(lines, table):
    """A Singer target: reads messages, upserts records, returns the last state it saw."""
    state = None
    for line in lines:
        msg = json.loads(line)
        if msg["type"] == "RECORD":
            table[msg["record"]["id"]] = msg["record"]
        elif msg["type"] == "STATE":
            state = msg["value"]           # echo state only after records are stored
    return state

table, state = {}, {"bookmarks": {"issues": "2026-11-20T23:59:59Z"}}
pipe = io.StringIO()                       # stands in for: tap | target
tap(state, pipe)
new_state = target(pipe.getvalue().splitlines(), table)
print(sorted(table), new_state)
# [2, 3] {'bookmarks': {'issues': '2026-11-22T08:15:00Z'}}
```

Only issues 2 and 3 are newer than the saved bookmark, and the new bookmark is the latest `updated_at` that was written.

**Meltano** makes Singer practical. A Meltano project is a Git repository with a `meltano.yml`:

```yaml
version: 1
default_environment: dev
project_id: acme-elt
state_backend:
  uri: gs://acme-meltano-state/state     # keep bookmarks in Cloud Storage, not on a laptop
plugins:
  extractors:
    - name: tap-github
      variant: meltanolabs
      config:
        repositories: ["acme/shop"]
        start_date: "2026-01-01"
      select:
        - issues.*
        - pull_requests.*
  loaders:
    - name: target-bigquery
      variant: z3z1ma
      config:
        project: acme-raw
        dataset: github
  utilities:
    - name: dbt-bigquery
environments:
  - name: dev
  - name: prod
```

```bash
meltano add extractor tap-github --variant meltanolabs   # install into an isolated virtualenv
meltano config tap-github set auth_token "$GITHUB_TOKEN" # secrets go to .env or the environment
meltano select tap-github --list                         # what streams and fields are selected
meltano run tap-github target-bigquery dbt-bigquery:build
```

Each plugin is installed in its own virtual environment, so taps with conflicting dependencies coexist. `meltano run` chains extract-load blocks and other commands, and stores state per pipeline in the configured backend. **Meltano Hub** lists hundreds of taps and targets; many sources have several **variants** (forks by different maintainers), and the Hub shows which are maintained and which features they support.

**Building a tap with the Meltano SDK.** The `singer-sdk` Python package handles the protocol, state, catalog, pagination helpers and testing, so a REST tap is mostly declarations:

```python
# Sketch with the Meltano Singer SDK (pip install singer-sdk)
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

class IssuesStream(RESTStream):
    name = "issues"
    url_base = "https://api.example.com"
    path = "/v1/issues"
    primary_keys = ["id"]
    replication_key = "updated_at"           # the SDK keeps the bookmark in STATE
    records_jsonpath = "$.data[*]"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()

class TapExample(Tap):
    name = "tap-example"
    config_jsonschema = th.PropertiesList(
        th.Property("api_key", th.StringType, required=True, secret=True),
    ).to_dict()

    def discover_streams(self):
        return [IssuesStream(self)]
```

**Stream maps** (in the SDK and as Meltano mapper plugins) transform records in flight: drop or hash a column, rename streams, filter rows. That is the small "t" of EtLT.

**Where it fits.** Meltano suits teams that want code-first, version-controlled extract-load with no licence cost, are happy to operate it, and have a mix of standard and custom sources. On Google Cloud, a container image with the Meltano project runs well as a Cloud Run job, with secrets injected from Secret Manager and state in Cloud Storage; an orchestrator such as Managed Airflow or Dagster starts it.

> **Pitfall:** Assuming every tap of the same name behaves the same. Variants differ in streams, incremental support and bugs. Pin the variant and version (`pip_url` with a version or Git tag) and read its capabilities on Meltano Hub before production.

> **Tip:** Keep state outside the container. A Cloud Run job's file system disappears after each run; without a remote state backend every run is a full historical sync.

::: deeper
**State is at-least-once.** A target emits `STATE` after flushing the records before it, and Meltano saves the last state it receives. If the run crashes after records are written but before state is saved, the next run re-sends them. Targets should upsert on key properties (most do for warehouses) so duplicates are harmless.

**Schema changes mid-stream.** A tap may send a new `SCHEMA` message for a stream partway through a run. Targets differ in how they handle it: some add columns, some fail. Test the target you use with a schema change before relying on it.

**Batch messages.** Line-by-line JSON is slow for very large volumes. The Meltano SDK adds a `BATCH` message type: the tap writes records to compressed files and sends only a pointer, and targets that support it load the files in bulk.

**Singer versus Airbyte protocol.** Both are "JSON messages over a pipe with checkpoint state". Airbyte added explicit commands (`spec`, `check`, `discover`), containers as the packaging unit, typed destinations and more structured error reporting; Singer is lighter and runs as plain Python packages.

> **Interview:** "What does the STATE message do in Singer, and what guarantee does it give?" It carries the tap's bookmarks. The target emits it after the preceding records are durably written, and the runner saves it for the next run. That gives at-least-once delivery: after a crash, records since the last saved state are re-sent, so the target must upsert by key.
:::

::: check
?? Name the three core Singer message types and what each carries.
`SCHEMA` (a stream's JSON Schema and key properties), `RECORD` (one row for a stream) and `STATE` (bookmarks recording progress, passed back to the tap on the next run).

?? Why does Meltano install each plugin in its own virtual environment?
Taps, targets and utilities often depend on conflicting versions of the same libraries; isolation lets them coexist in one project.

?? Your Meltano pipeline runs as a Cloud Run job and re-syncs all history on every run. What is the most likely cause?
State is stored on the container's local file system (the default system database), which is discarded after each run. Configure a remote state backend such as Cloud Storage.
:::

::: resources
- Docs: [Meltano documentation](https://docs.meltano.com/) · projects, plugins, `meltano run`, environments and state backends
- Spec: [Singer specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md) · the message types, catalog, metadata and state
- Docs: [Meltano Singer SDK](https://sdk.meltano.com/) · building taps and targets, stream maps and batch messages
- Docs: [Meltano Hub](https://hub.meltano.com/) · the catalog of taps, targets and variants with their capabilities
- Repo: [Meltano on GitHub](https://github.com/meltano/meltano) · source code, issues and release notes
:::

### Apache NiFi

::: plain
Apache NiFi is a visual plumbing system for data. You drag boxes (**processors**) onto a canvas, each doing one job such as "fetch files", "convert CSV to JSON" or "send to Kafka", and connect them with pipes (**connections**) that hold a queue of items waiting for the next box. Every item that flows through, called a **FlowFile**, is tracked, so you can later ask "where did this file come from, what happened to it, and where did it go?"
:::

**What NiFi is for.** NiFi was created at the US National Security Agency (as "Niagarafiles") and open-sourced through Apache in 2014. It specialises in **moving and routing data in motion**: collecting files and messages from many systems, light per-record transformation, routing by content or attributes, and delivering to many destinations, with strong tracking. It is not a warehouse transformation tool (that is dbt's job) and not a stateful stream processor (that is Flink's or Dataflow's).

**The core concepts.**

| Concept | What it is |
|---|---|
| FlowFile | One unit of data: a map of **attributes** (key-value metadata such as `filename`, `mime.type`) plus a pointer to its **content** (the bytes) |
| Processor | A component that does one thing to FlowFiles: `ListGCSBucket`, `FetchGCSObject`, `ConvertRecord`, `RouteOnAttribute`, `PublishKafka`, `PutBigQuery` and hundreds more |
| Relationship | A named output of a processor, such as `success`, `failure`, `matched` |
| Connection | A queue between a relationship and the next processor, with back-pressure and prioritisation settings |
| Process group | A sub-canvas that groups processors into a reusable unit, with input and output ports |
| Controller service | Shared configuration used by processors: a database connection pool, a record reader or writer, credentials |
| Parameter context | Named values (bucket names, hostnames, secrets) referenced as `#{bucket}` so the same flow runs in dev and prod |

**Record-oriented processing.** Early NiFi flows split files into one FlowFile per record, which is slow. Modern flows use **record processors** with a **record reader** and **record writer** (CSV, JSON, Avro, Parquet) configured as controller services: `ConvertRecord` turns CSV into Avro, `QueryRecord` runs SQL over the records of a FlowFile, `UpdateRecord` changes fields, `PartitionRecord` groups by a field, all without splitting.

**Expression Language.** Processor properties can be computed from attributes: `${filename:substringBefore('.')}`, `${now():format('yyyy-MM-dd')}`, `${kafka.topic:equals('orders')}`. A typical routing rule in `RouteOnAttribute` is a property `large` with value `${fileSize:gt(104857600)}`, which sends files above 100 MiB to a `large` relationship.

**A typical flow.** Pick up partner files from Cloud Storage and publish them as records to Kafka, routing bad files aside:

```text
ListGCSBucket (new objects under partners/)            keeps its own listing state
  -> FetchGCSObject                                    downloads content into the FlowFile
  -> ConvertRecord (CSVReader -> AvroRecordSetWriter)  failure -> PutGCSObject (quarantine/)
  -> UpdateAttribute (kafka.key = ${partner.id})
  -> PublishKafka (topic: partner-orders)              failure -> retry, then to a dead-letter flow
```

**Back-pressure.** Each connection has two thresholds, an object count and a data size (by default 10,000 FlowFiles and 1 GB). When a queue reaches either, NiFi stops scheduling the processor that feeds it, so a slow destination slows the whole flow back to the source instead of exhausting memory or disk. The threshold is soft: a processor that was already running can finish its work and push the queue slightly over. A toy simulation shows the effect:

```python
from collections import deque

THRESHOLD = 3                    # back-pressure object threshold on the connection
queue, blocked, log = deque(), 0, []
for tick in range(1, 11):
    if len(queue) < THRESHOLD:   # upstream is scheduled only while the queue is below threshold
        queue.append(f"ff{tick}")
    else:
        blocked += 1             # upstream processor is not scheduled this tick
    if tick % 2 == 0:            # downstream is slower: one FlowFile every second tick
        queue.popleft()
    log.append(len(queue))
print("queue sizes:", log)
print("ticks upstream was paused:", blocked)
# queue sizes: [1, 1, 2, 2, 3, 2, 3, 2, 3, 2]
# ticks upstream was paused: 3
```

Connections also have **prioritisers** (first in first out, oldest first, newest first, by a `priority` attribute) and an optional **FlowFile expiration** for data that is worthless when late.

**Provenance.** NiFi records a **provenance event** for everything that happens to each FlowFile: `RECEIVE`, `FETCH`, `CREATE`, `ATTRIBUTES_MODIFIED`, `CONTENT_MODIFIED`, `ROUTE`, `FORK`, `JOIN`, `SEND`, `DROP` and others, with the attributes and a pointer to the content before and after. In the UI you can trace a single record's lineage across the flow and **replay** it from any point, as long as the content is still in the content repository.

**Writing your own processor.** NiFi 2.x supports processors written in Python, alongside Java:

```python
# Sketch: a NiFi 2.x Python processor that upper-cases text content
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class UppercaseText(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "0.1.0"
        description = "Upper-cases the text content of each FlowFile."

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowfile):
        text = flowfile.getContentsAsBytes().decode("utf-8")
        return FlowFileTransformResult(relationship="success", contents=text.upper(),
                                       attributes={"uppercased": "true"})
```

**Running NiFi on Google Cloud.** There is no Google-managed NiFi. Teams run it on GKE (with a Helm chart or a Kubernetes operator) or on Compute Engine VMs with persistent disks for the repositories, or use a vendor distribution. Use Cloud Storage, Pub/Sub, BigQuery and Kafka processors for the Google Cloud side, and Workload Identity rather than key files for credentials.

> **Pitfall:** Building heavy business logic in NiFi: joins across sources, aggregations, slowly changing dimensions. The canvas becomes unreadable and untestable. Use NiFi to move and route, then transform in the warehouse with dbt or in a stream processor.

> **Pitfall:** Ignoring the repositories' disks. When the content repository's disk fills, the whole node stops. Size disks for peak backlog (what queues hold when a destination is down for hours), and set back-pressure so queues stop growing before disks fill.

::: deeper
**Repositories.** A node keeps three on disk. The **FlowFile repository** is a write-ahead log of each FlowFile's current attributes and queue position; on restart NiFi replays it to rebuild queues. The **content repository** stores content in large append-only "claims" shared by many FlowFiles; content is immutable, so modifying a FlowFile writes new content and moves the pointer (copy on write), which keeps old versions available for provenance replay until they age out. The **provenance repository** indexes events for search. Putting the three on separate disks is the main performance tuning step.

**Clustering.** A NiFi cluster runs the same flow on every node, each processing its own share of data. One node is elected **cluster coordinator** (it manages membership and flow changes) and one is **primary node** (it runs processors configured to run on the primary only, such as a `List*` processor that must not list the same bucket on every node). Elections used ZooKeeper; NiFi 2.x can also use Kubernetes leases. A `List` processor on the primary node followed by a **load-balanced connection** (round robin or by attribute) spreads `Fetch` work across the cluster.

**Site-to-Site.** The protocol NiFi instances use to send FlowFiles to each other (over raw sockets or HTTP), through **remote process groups** and ports. It keeps attributes, compresses, load-balances across the receiving cluster and confirms delivery. A common pattern is many small edge instances (or **MiNiFi** agents on devices) sending to a central cluster.

**Versioning flows.** Flows are versioned in NiFi Registry, or in 2.x through registry clients backed by Git repositories, and promoted between environments with parameter contexts holding the environment-specific values. NiFi 2.0 (late 2024) required Java 21, added the Python processor API and removed older features such as templates and variables in favour of versioned flows and parameters.

> **Interview:** "When would you choose NiFi over Kafka Connect or Airbyte?" When the problem is routing many heterogeneous feeds (files, protocols like SFTP, HTTP and MQTT, message queues) with per-record decisions, visual operation by an operations team, and strong per-item provenance, especially at edges feeding a central system. Choose Kafka Connect when Kafka is the hub and connectors exist, Airbyte or Fivetran for SaaS-to-warehouse batch syncs, and Flink or Dataflow for stateful stream processing.
:::

::: check
?? What are the two parts of a FlowFile?
Attributes (a map of key-value metadata) and a pointer to its content (the bytes, stored in the content repository).

?? What happens when a connection reaches its back-pressure threshold?
NiFi stops scheduling the processor that feeds that connection until the queue drops below the threshold, so pressure propagates upstream instead of exhausting memory or disk.

?? Why should a `ListGCSBucket` processor in a cluster run on the primary node only?
Otherwise every node lists the same bucket and the same objects are fetched several times. List on the primary node, then distribute the fetches with a load-balanced connection.
:::

::: resources
- Docs: [Apache NiFi documentation](https://nifi.apache.org/documentation/) · user guide, administration guide and the processor reference
- Docs: [NiFi In Depth](https://nifi.apache.org/docs/nifi-docs/html/nifi-in-depth.html) · the FlowFile, content and provenance repositories and how they interact
- Docs: [NiFi Python developer's guide](https://nifi.apache.org/nifi-docs/python-developer-guide.html) · writing processors in Python for NiFi 2.x
- Repo: [Apache NiFi on GitHub](https://github.com/apache/nifi) · source code and release notes
- Course: [Apache NiFi: A Complete Guide](https://www.packtpub.com/en-us/product/apache-nifi-a-complete-guide-hortonworks-dataflow-hdf-9781839217739) · Packt course on flow design and clustering; written for NiFi 1.x, so check what changed in 2.x
- Docs: [NiFi Expression Language guide](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html) · every function available in processor properties
:::

## Reliability

The first three groups built pipelines that work on a good day. This group is about the other days: a source sends garbage, a producer renames a column, one message in a million crashes the consumer, a bug corrupted three weeks of a table, or someone asks "where does this number come from?" and nobody knows.

Each topic adds one defence. **Data quality and validation** puts gates on the line, and the write-audit-publish pattern makes sure consumers never see unchecked data. **Data contracts** move checks upstream to the producer. **Schema evolution** keeps pipelines running while shapes change. **Dead-letter queues** contain bad records without stopping good ones. **Backfills** repair history safely. **Observability and lineage** tell you what happened and who is affected. Several of these subjects also appear in [Data governance, security and compliance](#governance) (quality frameworks, lineage platforms) and [DataOps and platform engineering](#dataops) (testing, monitoring, incidents); here the focus is on how they are built into the pipeline itself.

### Data Quality & Validation

::: plain
A factory checks quality twice: at the loading dock, so bad material never enters the line, and before shipping, so a faulty product never reaches a customer. Pipelines need the same two gates. Rows that fail the first gate are set aside in a **quarantine** bin with a note saying why, and a batch that fails the second gate is never shown to users: the previous good version stays on the shelf until the problem is fixed.
:::

**Three kinds of check.** Rules differ by what they look at, and that decides where they can run:

| Kind | Looks at | Examples | Where it runs |
|---|---|---|---|
| Schema | The shape | Columns present, types, required fields | At ingestion, in the stream, in dbt contracts |
| Record | One row at a time | `amount >= 0`, valid email, known status | In flight (Dataflow, Python), or as SQL filters |
| Dataset | The whole batch | Unique keys, row count versus yesterday, sums reconcile with the source, freshness | After the batch is written, before it is published |

The frameworks are covered in [Data governance, security and compliance](#governance): dbt tests, Dataform assertions, Great Expectations (GX Core), Soda, and Knowledge Catalog (formerly Dataplex Universal Catalog) data quality scans, which run rules on BigQuery tables without infrastructure. For Python code, `pydantic` validates records and `pandera` validates DataFrames. This topic is about wiring checks into a pipeline: what happens when they fail.

**Four responses to a failed check.**

1. **Stop**: fail the task, alert, keep the previous output. For rules whose violation makes everything wrong (duplicate primary keys in a fact).
2. **Quarantine**: move failing rows to a side table with the reason, load the rest. For record-level problems in otherwise good batches.
3. **Warn**: load everything, alert someone to investigate. For rules that signal change rather than error (a new status value).
4. **Circuit breaker**: stop the pipeline when a metric is far from normal (row count down 80%), even if no individual rule failed.

Decide the response per rule, and write it down next to the rule. "Every failure pages someone" trains people to ignore pages; "every failure only warns" ships bad data.

**Write-audit-publish (WAP).** Here is the core problem with checks that run after a table is written: by the time the test fails, consumers may already have read the bad data. In dbt, for example, `dbt build` runs a model and then its tests; a failing test skips everything downstream, but the model's own table has already been replaced. Write-audit-publish fixes this by separating three steps:

1. **Write** the new data somewhere consumers cannot see: a candidate table, a staging dataset, an Iceberg branch.
2. **Audit** it with the dataset checks.
3. **Publish** atomically only if the audit passes; otherwise leave the old version in place and alert.

[[fig:pipelines_wap]]

A complete version runs as is in SQLite, combining quarantine (record checks during the write) with dataset audits and an atomic swap:

```python
import sqlite3

db = sqlite3.connect(":memory:", isolation_level=None)   # we manage transactions explicitly
db.executescript("""
CREATE TABLE fct_orders (order_id TEXT PRIMARY KEY, amount REAL);
INSERT INTO fct_orders VALUES ('o1', 10.0), ('o2', 25.0), ('o3', 8.0);
CREATE TABLE quarantine (order_id TEXT, amount REAL, reason TEXT);
""")

def publish(batch):
    db.execute("DROP TABLE IF EXISTS fct_orders_candidate")
    db.execute("CREATE TABLE fct_orders_candidate (order_id TEXT, amount REAL)")
    for oid, amt in batch:                                    # WRITE (rows failing record checks go aside)
        if oid is None or amt is None or amt < 0:
            db.execute("INSERT INTO quarantine VALUES (?, ?, 'bad key or amount')", (oid, amt))
        else:
            db.execute("INSERT INTO fct_orders_candidate VALUES (?, ?)", (oid, amt))
    audits = {                                                # AUDIT (dataset checks on the candidate)
        "duplicate keys": "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM fct_orders_candidate",
        "row count dropped >50%": """SELECT (SELECT COUNT(*) FROM fct_orders_candidate)
                                          < 0.5 * (SELECT COUNT(*) FROM fct_orders)""",
    }
    failed = [name for name, sql in audits.items() if db.execute(sql).fetchone()[0]]
    if failed:
        return f"blocked: {failed}"
    db.execute("BEGIN")                                       # PUBLISH (atomic swap)
    db.execute("DROP TABLE fct_orders")
    db.execute("ALTER TABLE fct_orders_candidate RENAME TO fct_orders")
    db.execute("COMMIT")
    return "published"

print(publish([("o1", 10.0), ("o2", 25.0), ("o3", 8.0), ("o4", 12.5), ("o5", -1.0)]))
print(publish([("o1", 10.0), ("o1", 10.0)]))                  # a broken upstream run
print(db.execute("SELECT COUNT(*), SUM(amount) FROM fct_orders").fetchone(),
      db.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0])
# published
# blocked: ['duplicate keys']
# (4, 55.5) 1
```

The second batch is blocked, so consumers still see the four good rows from the first.

**WAP in BigQuery.** Build the candidate, audit with `ASSERT` (which fails the script with a message when its condition is false), then publish:

```sql
CREATE OR REPLACE TABLE staging.fct_orders__candidate AS
SELECT * FROM staging.orders_enriched WHERE amount >= 0;

ASSERT (SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM staging.fct_orders__candidate) = 0
  AS 'fct_orders: duplicate order_id';
ASSERT (SELECT COUNT(*) FROM staging.fct_orders__candidate)
       >= 0.5 * (SELECT COUNT(*) FROM mart.fct_orders)
  AS 'fct_orders: row count dropped by more than half';

-- Only reached if both assertions pass.
CREATE OR REPLACE TABLE mart.fct_orders COPY staging.fct_orders__candidate;
```

For a single partition, publish with a transaction that deletes and re-inserts that partition from the candidate.

**WAP on a lakehouse.** Apache Iceberg supports it natively with **branches**: write to a branch, audit by querying the branch, then fast-forward `main` to it. In Spark SQL:

```sql
ALTER TABLE lake.sales.fct_orders SET TBLPROPERTIES ('write.wap.enabled' = 'true');
ALTER TABLE lake.sales.fct_orders CREATE BRANCH audit_20261123;
SET spark.wap.branch = audit_20261123;            -- writes now go to the branch
INSERT INTO lake.sales.fct_orders SELECT * FROM staging_orders;
SELECT COUNT(*) FROM lake.sales.fct_orders VERSION AS OF 'audit_20261123';   -- audit
CALL lake.system.fast_forward('sales.fct_orders', 'main', 'audit_20261123'); -- publish
```

Readers of `main` never see the branch's data until the fast-forward. Project Nessie and lakeFS offer the same idea across many tables at once.

> **Pitfall:** Running expensive dataset checks over the whole table every run. A uniqueness test on a 10 TB fact scans the key column each time. Audit only the partitions the run wrote, and run full-table checks weekly.

> **Tip:** Count quarantined rows per source and chart them. A quarantine rate that jumps from 0.01% to 4% is an incident even though the pipeline is green.

::: deeper
**dbt and WAP.** Options for dbt projects: build into a separate "blue" dataset and swap a view or the dataset contents after tests pass (blue-green, covered in [DataOps and platform engineering](#dataops)); a custom materialization that builds `model__candidate`, runs tests and then swaps; or run critical tests on the upstream staging models so bad data stops before it reaches the published model. Dataform assertions likewise run after the action they check, so the same reasoning applies.

**Reconciliation.** Some of the most valuable checks compare systems: order count in the warehouse versus the source database for yesterday, revenue in the mart versus the general ledger for closed months. They catch silent loss in connectors and joins that no per-table rule can see. Make the tolerance explicit (exact for finance, 0.1% for clickstream).

**Statistical thresholds.** Fixed thresholds ("at least 10,000 rows") break as the business grows. Compare with the same weekday over recent weeks and alert outside a band; [Data governance, security and compliance](#governance) covers anomaly detection and observability platforms.

**Validating in streams.** In Dataflow, validation is a `DoFn` with two outputs: valid records continue, invalid ones go to a dead-letter output with the reason (see the dead-letter topic below). Pub/Sub schemas reject malformed messages at publish time, which is the cheapest gate of all.

> **Interview:** "How do you stop a bad batch from reaching a dashboard?" Write-audit-publish: build into a candidate table or Iceberg branch, run dataset checks (key uniqueness, volume against history, reconciliation with the source), publish atomically only on success, otherwise keep the previous version and alert. Add record-level checks with quarantine at ingestion, and point out that dbt's build-then-test order alone does not protect the model itself.
:::

::: check
?? Why does `dbt build` not, on its own, protect consumers of a model whose test fails?
dbt materializes the model first and tests it afterwards. A failing test skips downstream models, but the model's own table has already been replaced with the bad data.

?? A batch has 0.2% of rows with a negative amount and is otherwise fine. Which response fits, and why?
Quarantine those rows with the reason and load the rest, then alert at a threshold. The problem is record-level, so stopping the whole batch would delay good data for every consumer.

?? What makes the publish step of write-audit-publish safe?
It is atomic: consumers see either the old version or the complete, audited new version, never a partial or unaudited one (a table swap, a transaction, or an Iceberg fast-forward).
:::

::: resources
- Docs: [BigQuery ASSERT statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging-statements) · failing a script with a message when a condition is false
- Docs: [Iceberg branching and tagging](https://iceberg.apache.org/docs/latest/branching/) · branches, write-audit-publish and fast-forward
- Docs: [Knowledge Catalog data quality scans](https://cloud.google.com/dataplex/docs/auto-data-quality-overview) · managed rules and thresholds on BigQuery tables
- Docs: [Great Expectations (GX Core)](https://docs.greatexpectations.io/docs/core/introduction/) · expectations, suites and checkpoints in Python pipelines
- Docs: [Soda documentation](https://docs.soda.io/) · checks as YAML run against the warehouse
- Docs: [Dataform assertions](https://cloud.google.com/dataform/docs/assertions) · built-in and manual assertions for BigQuery pipelines
:::

### Data Contracts

::: plain
A data contract is a promise from the team that produces data to the teams that use it, written down so a computer can check it. It is like the agreement between a bakery and a café: so many loaves, of these kinds, delivered by 7 a.m., with a week's notice before the recipe changes. The hard part is not writing the agreement but checking it automatically every time, at the right moment, and deciding what happens when it is broken.
:::

[Data modeling](#modeling) covered what goes into a contract (ownership, schema, semantics, quality rules, service levels, change policy), the open formats (the Open Data Contract Standard and the Data Contract Specification) and a checker that validates rows against a contract. This topic is about **enforcement**: where a pipeline checks a contract, and what happens on a breach.

**Three moments to enforce.**

| When | What is checked | Mechanism |
|---|---|---|
| Before a producer change ships (design time) | Is the proposed schema compatible with the published contract? | CI check in the producer's repository, schema registry compatibility check |
| When data is produced or arrives (run time) | Does each record or batch conform? | Pub/Sub schemas, schema registry serialisers, ingestion validation, dbt model contracts |
| Continuously (service levels) | Is the data fresh, complete and within agreed quality? | Freshness and volume monitors, SLO dashboards |

The first moment is the most valuable, because it stops breakage before any data is produced, and it is the one most teams skip.

**Design time: a compatibility check in the producer's CI.** The producer's pipeline compares the schema its next release would emit with the published contract and classifies each difference from the consumers' point of view. This runs as is:

```python
contract = {  # the published interface: column -> (type, required)
    "order_id": ("STRING", True), "customer_id": ("STRING", True),
    "amount": ("NUMERIC", True), "status": ("STRING", True), "coupon": ("STRING", False),
}
proposed = {  # what the producer's next release would emit
    "order_id": ("STRING", True), "customer_id": ("INT64", True),
    "amount": ("NUMERIC", False), "coupon": ("STRING", False), "channel": ("STRING", False),
}

def diff(old, new):
    """Classify changes from the consumers' point of view."""
    findings = []
    for col, (typ, required) in old.items():
        if col not in new:
            findings.append(("BREAKING", f"removed column {col}"))
        elif new[col][0] != typ:
            findings.append(("BREAKING", f"{col}: type {typ} -> {new[col][0]}"))
        elif required and not new[col][1]:
            findings.append(("BREAKING", f"{col}: no longer required, consumers may see NULL"))
    for col in new.keys() - old.keys():
        findings.append(("ok", f"added column {col}"))
    return findings

results = diff(contract, proposed)
for level, msg in results:
    print(f"{level:8} {msg}")
print("verdict:", "new major version and migration plan needed"
      if any(level == "BREAKING" for level, _ in results) else "compatible")
# BREAKING customer_id: type STRING -> INT64
# BREAKING amount: no longer required, consumers may see NULL
# BREAKING removed column status
# ok       added column channel
# verdict: new major version and migration plan needed
```

A failing check blocks the merge and points the producer at the contract's change policy: publish a new major version alongside the old one, notify registered consumers, and retire the old version after the agreed window. The open-source **Data Contract CLI** does this for real contract files (`datacontract lint`, `datacontract test` against a live dataset, `datacontract breaking` between two versions, and `datacontract export` to generate dbt models, SQL DDL or Avro schemas from the contract).

**Run time: let the platform reject bad data.** The strongest run-time checks are the ones you do not write:

```bash
# A Pub/Sub topic that only accepts messages matching an Avro schema
gcloud pubsub schemas create orders-v1 --type=avro --definition-file=orders.avsc
gcloud pubsub topics create orders --schema=orders-v1 --message-encoding=json
```

Publishing a message that does not match fails at the publisher, so the producer sees the error, not the consumer. Pub/Sub schemas support **revisions**: commit a new revision of the schema and let the topic accept a range of revisions during a migration. For Kafka, the schema registry plays the same role ([Streaming systems](#streaming)). Inside the warehouse, a dbt **model contract** (`contract: {enforced: true}`) fails the build if the model's columns or types differ from the YAML.

**Service levels: measure what you promised.** A contract that says "fresh by 07:00 UTC, 99% of days" needs a measurement: record when each daily partition was published (the run ledger in the observability topic) and compute the share of days it met the promise. Publish the number next to the dataset in the catalog. When a promise is missed, the contract says who is told and how fast the producer responds.

**What happens on a breach.** Write it into the contract, per clause:

| Clause broken | Typical response |
|---|---|
| Schema (design time) | Block the producer's merge |
| Schema (run time) | Reject at publish, or quarantine at ingestion and alert the producer |
| Quality rule | Quarantine rows or block publication (write-audit-publish), alert the producer |
| Freshness | Alert both sides; consumers' pipelines wait or run with a "stale" flag |

**Where contracts belong.** Put them at **ownership boundaries**: application team to data platform (the CDC stream or event topic is the contract), and data product to consumers (a public mart). Inside one team's pipeline, ordinary tests are enough. Contracts are an organisational tool as much as a technical one: they need an owner on the producer side who agrees to them.

> **Pitfall:** A contract nobody on the producing team knows about. If the data team writes the contract and checks it only in the warehouse, it is just another test that fails after the damage is done. The producer must own it and run the design-time check in their own CI.

> **Tip:** Start with one high-value boundary (the orders events that feed finance), a schema and a freshness clause. Expand once both teams have seen a breaking change caught before deployment; that one success sells contracts better than any presentation.

::: deeper
**Consumers in the contract.** A contract is more useful when it knows who depends on it. Lineage (the last topic in this group) can list consumers automatically; a producer's CI can then name the affected teams in the failing check, and a deprecation can notify them.

**Semantic changes.** The hardest breaking changes keep the schema identical: `amount` switches from including tax to excluding it, or `status = 'active'` starts including trial users. No schema check catches these. Contracts handle them with written semantics, versioning by meaning (a semantic change is a major version), and reconciliation tests that compare distributions before and after a release.

**Contracts and data mesh.** In a data mesh, every data product's contract is its public interface; platforms such as Knowledge Catalog and DataHub can show contracts, owners and service levels next to the data, and BigQuery sharing (formerly Analytics Hub) can expose contracted products to other organisations.

**Cost of strictness.** Rejecting every non-conforming record protects consumers but can lose data. For high-value events, prefer quarantine with replay over rejection, and alert on the quarantine rate.

> **Interview:** "A product team keeps breaking your pipelines with schema changes. What do you do?" Agree a data contract for the boundary they own (schema, semantics, freshness, change policy); add a design-time compatibility check to their CI so breaking changes fail their build; enforce at run time with Pub/Sub or registry schemas; version breaking changes with a deprecation window; show them lineage of who depends on the data. Frame it as protecting them from late-night pages about dashboards.
:::

::: check
?? Why is the design-time check the most valuable enforcement point?
It catches a breaking change before the producer deploys it, so no bad data is ever produced; later checks can only contain damage that has already happened.

?? Which change in the example is breaking even though no column was removed or retyped, and why?
`amount` becoming optional: consumers that relied on it never being NULL may now produce wrong sums or fail.

?? What kind of breaking change can no schema check detect, and how is it handled?
A semantic change with an identical schema, such as a column's meaning or units changing. It is handled with written semantics in the contract, versioning on meaning, and reconciliation or distribution tests.
:::

::: resources
- Docs: [Data Contract CLI](https://cli.datacontract.com/) · lint, test, breaking-change detection and export from contract files
- Spec: [Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/latest/) · the Linux Foundation Bitol project's contract format
- Docs: [Pub/Sub schemas](https://cloud.google.com/pubsub/docs/schemas) · Avro and Protocol Buffer schemas, revisions and validation at publish time
- Docs: [dbt model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) · enforcing a model's columns and types at build time
- Article: [An engineer's guide to data contracts](https://dataproducts.substack.com/p/an-engineers-guide-to-data-contracts) · Chad Sanderson and Adrian Kreuziger on implementing contracts end to end
- Book: *Data Contracts: Developing Production-Grade Pipelines at Scale* by Chad Sanderson and Mark Freeman · organisational and technical rollout of contracts
:::

### Schema Evolution & Compatibility

::: plain
Data shapes change all the time: a new field appears, an old one is retired, a number gets more decimal places. **Schema evolution** is changing a shape without breaking the programs that read or write it. **Compatibility** is the rule that says which changes are safe, like a plug adapter rule: a new socket that still accepts old plugs is backward compatible; an old socket that still accepts new plugs is forward compatible.
:::

Two earlier sections cover parts of this subject. [Data modeling](#modeling) lists the kinds of change (additive, widening, renaming, removing, semantic), the expand-and-contract method for renaming without downtime, migration tools and dbt model versions. [Streaming systems](#streaming) covers schema registries and their compatibility modes for Kafka. This topic connects them: how compatibility is defined precisely, and what each layer of a pipeline does when the shape of its input changes.

**Compatibility, precisely.** Name the two sides: the **writer** produces data with one schema, the **reader** consumes it with another.

| Mode | Guarantee | Safe changes (Avro) | Upgrade order |
|---|---|---|---|
| Backward | A reader with the new schema can read data written with the old one | Delete a field; add a field with a default | Upgrade consumers first |
| Forward | A reader with the old schema can read data written with the new one | Add a field; delete a field that had a default | Upgrade producers first |
| Full | Both | Add or delete only fields with defaults | Either order |
| None | No check | Anything | Coordinate everything by hand |

**Transitive** variants (backward transitive, and so on) check a new schema against all previous versions, not just the latest, which matters when old data is reread (replays, backfills).

**How a reader resolves a schema difference.** Avro's rules are the clearest model, and other formats behave similarly: a field present in both schemas is read; a field only the writer has is ignored; a field only the reader has takes its default, and if it has no default, reading fails. This runs as is:

```python
MISSING = object()

def resolve(record, writer_fields, reader_schema):
    """Read a record written with writer_fields using reader_schema (Avro-style rules)."""
    out = {}
    for name, spec in reader_schema.items():
        if name in writer_fields:
            out[name] = record[name]                # field in both: take the written value
        elif spec.get("default", MISSING) is not MISSING:
            out[name] = spec["default"]             # reader-only field: use its default
        else:
            raise ValueError(f"cannot read: {name!r} missing and has no default")
    return out                                      # writer-only fields are ignored

v1 = {"order_id": {}, "amount": {}}
v2 = {"order_id": {}, "amount": {}, "currency": {"default": "EUR"}}   # added with a default
v3 = {"order_id": {}, "amount": {}, "channel": {}}                   # added WITHOUT a default

old_record = {"order_id": "o1", "amount": 10}
new_record = {"order_id": "o2", "amount": 7, "currency": "USD"}

print(resolve(old_record, v1, v2))   # new reader, old data: backward compatible
print(resolve(new_record, v2, v1))   # old reader, new data: forward compatible
try:
    resolve(old_record, v1, v3)
except ValueError as e:
    print(e)
# {'order_id': 'o1', 'amount': 10, 'currency': 'EUR'}
# {'order_id': 'o2', 'amount': 7}
# cannot read: 'channel' missing and has no default
```

So adding `currency` with a default is fully compatible, while adding `channel` without a default breaks any reader using v3 on old data.

**What each layer does when the input changes.** A pipeline is a chain, and every link has its own setting. Know them all, because the strictest one decides what happens:

| Layer | Mechanism | Options |
|---|---|---|
| Event streams | Schema registry, Pub/Sub schemas | Compatibility mode per subject; Pub/Sub schema revisions |
| Connectors | Vendor policy | Fivetran: allow all, allow columns, block all; Airbyte: propagate all, propagate columns only, or detect and wait for approval; Datastream adds new columns |
| BigQuery load jobs | Schema update options | `ALLOW_FIELD_ADDITION`, `ALLOW_FIELD_RELAXATION` (REQUIRED to NULLABLE); otherwise a mismatch fails the load |
| BigQuery DDL | `ALTER TABLE` | `ADD COLUMN`, `RENAME COLUMN`, `DROP COLUMN`, `ALTER COLUMN DROP NOT NULL`, `ALTER COLUMN SET DATA TYPE` for some widening coercions such as INT64 to NUMERIC |
| Parquet files in a lake | Reader option | Spark's `mergeSchema` unions file schemas; columns are matched by name |
| Iceberg tables | Table metadata | Add, drop, rename, reorder and widen are metadata-only; columns are tracked by ID, so renames are safe |
| Delta tables | Write option | `mergeSchema` on write; column mapping mode for safe renames and drops |
| dbt incremental models | `on_schema_change` | `ignore` (default), `fail`, `append_new_columns`, `sync_all_columns` |

A load that accepts new fields:

```bash
bq load --source_format=NEWLINE_DELIMITED_JSON \
  --schema_update_option=ALLOW_FIELD_ADDITION \
  raw.orders gs://acme-landing/orders/2026-11-23/*.json ./orders_schema.json
```

**A pipeline-wide policy.** A sensible default for most platforms:

1. **Raw layer: accept additive changes automatically.** New columns land (connector "allow columns", `ALLOW_FIELD_ADDITION`). Raw is a record of what arrived.
2. **Staging layer: explicit column lists.** Staging models select named columns, never `SELECT *` from raw, so a new raw column does not flow anywhere until someone adds it on purpose. This is the **tolerant reader** pattern: read what you need, ignore the rest.
3. **Alert on every schema change**, additive or not, so the team knows. Removals and type changes stop the affected models (`on_schema_change: fail`) rather than producing NULL columns silently.
4. **Published marts change through expand and contract** with versioned models or views as the interface ([Data modeling](#modeling)).

> **Pitfall:** `SELECT *` from raw into staging, combined with `on_schema_change: sync_all_columns` downstream. A column the source drops is silently dropped all the way into the mart, and a dashboard shows blanks with no error anywhere.

> **Pitfall:** Renaming a field in a Parquet-based lake table that is matched by name. Old files have the old name, new files the new name, and readers see two half-empty columns. Use Iceberg (ID-based) or add the new column, backfill it, then drop the old one.

::: deeper
**Protocol Buffers rules.** Fields are identified by number, not name. Never reuse or change a field number; mark removed numbers and names `reserved`; adding a field is safe (old readers skip unknown numbers); changing a field's type is safe only between a few wire-compatible types. Names can change freely on the wire, but JSON encodings and generated code use names, so treat renames as breaking anyway.

**JSON Schema is harder.** Its compatibility depends on whether objects allow extra properties. A closed schema (`additionalProperties: false`) makes adding a field breaking for old readers; an open one makes removing a field unsafe to detect. Registries implement JSON Schema compatibility with these subtleties; many teams prefer Avro or Protobuf for contracts at boundaries.

**Pub/Sub BigQuery subscriptions** write messages straight into a table; with `use_topic_schema` they map the topic's schema to columns, so a new schema revision needs the table updated first (add the column, then commit the revision), or messages fail to write.

**Nested fields in BigQuery.** Adding a field inside a `RECORD` (including repeated records) is an allowed additive change, through the load option or by updating the schema with `bq update`. Changing a field from a scalar to a record is a rewrite.

**Upgrade order matters.** For backward-compatible changes, upgrade consumers first; for forward-compatible changes, producers first. Getting it wrong is how a "compatible" change still breaks production for a few hours.

> **Interview:** "A source adds a column, renames another and widens a third. Walk through what happens in your pipeline." The registry or connector policy admits the addition and widening and flags the rename; raw accepts the new column (`ALLOW_FIELD_ADDITION`), and the rename appears as a new column plus a column that stops being populated; staging's explicit column list is unaffected by the addition but its tests catch the renamed column going NULL; alerts fire on the schema change; you fix staging to coalesce old and new names, backfill, and agree with the source team to do renames as add-copy-drop in future.
:::

::: check
?? Which consumers should you upgrade first for a backward-compatible change, and why?
Consumers. Backward compatibility means a reader with the new schema can read data in the old schema, so new readers work with existing data before producers switch.

?? Why does adding a field without a default break backward compatibility in Avro?
A new reader reading old data finds no value for that field and has no default to use, so resolution fails.

?? Why should staging models list columns explicitly instead of using `SELECT *` from raw?
So schema drift in raw (new or removed columns) does not flow silently downstream; changes reach marts only when someone adds them deliberately, and removals fail loudly instead of becoming NULLs.
:::

::: resources
- Docs: [Confluent Schema Registry: schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) · compatibility modes with the allowed changes and upgrade order for each
- Spec: [Apache Avro specification: schema resolution](https://avro.apache.org/docs/1.12.0/specification/) · the exact reader and writer resolution rules
- Docs: [Modifying BigQuery table schemas](https://cloud.google.com/bigquery/docs/managing-table-schemas) · adding columns, relaxing modes and load-time schema updates
- Docs: [Iceberg schema evolution](https://iceberg.apache.org/docs/latest/evolution/) · why ID-based columns make renames and drops safe
- Docs: [dbt incremental models: on_schema_change](https://docs.getdbt.com/docs/build/incremental-models) · what each option does to an existing incremental table
- Article: [Schema evolution in Avro, Protocol Buffers and Thrift](https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html) · Martin Kleppmann's classic comparison of the three encodings
:::

### Error Handling & Dead Letter Queues

::: plain
When a post office finds a letter it cannot deliver (no address, torn envelope), it does not stop all deliveries for the day; it puts the letter in a "dead letter" office with a note, keeps the rest moving, and someone later fixes and resends it. A **dead-letter queue (DLQ)** does the same for data: a record that cannot be processed is set aside with the reason, the pipeline carries on, and the record can be **replayed** once the problem is fixed.
:::

**Classify errors first.** The right reaction depends on the kind of failure:

| Kind | Examples | Reaction |
|---|---|---|
| Transient | Timeout, HTTP 429 or 503, lost connection, quota briefly exceeded | Retry with exponential backoff and jitter, a bounded number of times |
| Permanent, one record | Malformed JSON, missing required field, value out of range, unknown enum | Do not retry; send to the DLQ with context and move on |
| Permanent, systemic | Expired credentials, wrong schema version deployed, a bug that fails every record | Stop the pipeline (circuit breaker) and alert; do not flood the DLQ |

A **poison message** is a record that fails every time it is processed. Without a DLQ it blocks its whole partition or subscription: the consumer crashes, restarts, reads the same message and crashes again.

**Retries done well.** Retry only transient errors, only idempotent operations, with **exponential backoff** (1 s, 2 s, 4 s ...) capped at a maximum, and **jitter** (a random part of the delay) so a thousand clients that failed together do not retry together. Google Cloud client libraries already do this for most calls; configure them rather than wrapping them in your own loops.

**What a dead letter must contain.** A dead letter that says only "error" is useless. Store the **original payload** unchanged, the **error class and message**, **where it came from** (topic, partition and offset, or file and line, or subscription and message ID), the **pipeline name and version**, the **attempt count** and a **timestamp**. That is enough to understand, fix and replay.

A pipeline step with all three reactions runs as is (sleeps are omitted to keep it fast):

```python
import json

class TransientError(Exception): pass     # timeouts, HTTP 429/503: worth retrying

calls = {"o2": 0}
def write_to_sink(order):
    if order["id"] == "o2" and calls["o2"] < 2:          # o2's sink call times out twice
        calls["o2"] += 1
        raise TransientError("timeout")
    return True

def process(raw, attempts=4):
    order = json.loads(raw)                               # JSONDecodeError (a ValueError) if malformed
    if order["amount"] < 0:
        raise ValueError(f"negative amount {order['amount']}")
    for attempt in range(1, attempts + 1):
        try:
            return write_to_sink(order), attempt
        except TransientError:
            if attempt == attempts:
                raise
            # real code: time.sleep(min(cap, base * 2 ** attempt) * random.random())

messages = ['{"id": "o1", "amount": 5}', '{"id": "o2", "amount": 9}',
            '{"id": "o3", "amount": -4}', '{"id": "o4", "amount": ']   # last one is truncated
ok, dead_letters = [], []
for offset, raw in enumerate(messages):
    try:
        _, attempts = process(raw)
        ok.append((offset, attempts))
    except (ValueError, KeyError) as e:                   # permanent: data problem, park it
        dead_letters.append({"offset": offset, "payload": raw,
                             "error": type(e).__name__, "detail": str(e)[:60]})
    if len(dead_letters) / (offset + 1) > 0.5 and offset >= 3:
        raise SystemExit("circuit breaker: most records failing, stop and page someone")

print("written (offset, attempts):", ok)
for d in dead_letters:
    print("DLQ:", d)
# written (offset, attempts): [(0, 1), (1, 3)]
# DLQ: {'offset': 2, 'payload': '{"id": "o3", "amount": -4}', 'error': 'ValueError', 'detail': 'negative amount -4'}
# DLQ: {'offset': 3, 'payload': '{"id": "o4", "amount": ', 'error': 'JSONDecodeError', 'detail': 'Expecting value: line 1 column 24 (char 23)'}
```

[[fig:pipelines_dlq]]

**Dead-letter topics in Pub/Sub.** A subscription can forward a message to a dead-letter topic after a number of failed delivery attempts (between 5 and 100), with a retry policy that spaces redeliveries out:

```bash
gcloud pubsub topics create orders-dlq
gcloud pubsub subscriptions create orders-dlq-sub --topic=orders-dlq   # keeps dead letters readable
gcloud pubsub subscriptions update orders-sub \
  --dead-letter-topic=orders-dlq --max-delivery-attempts=10 \
  --min-retry-delay=10s --max-retry-delay=600s
```

The Pub/Sub service agent needs permission to publish to the dead-letter topic and to subscribe to the source subscription; the console sets this up, with the CLI you grant it yourself. Forwarded messages carry attributes such as `CloudPubSubDeadLetterSourceDeliveryCount` and `CloudPubSubDeadLetterSourceSubscription`. Create a subscription on the dead-letter topic, or the dead letters are dropped because nobody subscribes to them.

**Dead letters in Dataflow (Apache Beam).** Inside a pipeline, the pattern is a transform with two outputs:

```python
# Sketch (Apache Beam, Python SDK)
import json
import apache_beam as beam
from apache_beam import pvalue

class ParseOrder(beam.DoFn):
    def process(self, msg: bytes):
        try:
            rec = json.loads(msg)
            if rec["amount"] < 0:
                raise ValueError("negative amount")
            yield rec
        except Exception as e:
            yield pvalue.TaggedOutput("dead", {"payload": msg.decode("utf-8", "replace"),
                                               "error": repr(e)})

parsed = messages | beam.ParDo(ParseOrder()).with_outputs("dead", main="ok")
parsed.ok   | "WriteOrders" >> beam.io.WriteToBigQuery("acme-prod:sales.orders")
parsed.dead | "WriteDead"   >> beam.io.WriteToBigQuery("acme-prod:ops.orders_dead_letter")
```

A BigQuery dead-letter table is often more practical than a topic: you can query it, group errors, and select exactly what to replay.

**Batch equivalents.** For file loads, BigQuery's `--max_bad_records=N` lets a load skip up to N unparseable rows (they are reported in the job's errors, not saved), and `--ignore_unknown_values` drops unexpected fields. Both lose data silently unless you record them; for important data, load to raw permissively and quarantine in SQL instead.

**Replay.** The DLQ is only half the pattern; the other half is getting records back:

1. Fix the cause (deploy the code fix, or get the producer to correct data).
2. Select the dead letters to replay (by error type, time range or source).
3. Republish them to the original input (topic, or a replay table the pipeline also reads), marked with a `replayed_at` attribute.
4. Rely on idempotent processing so records that partly succeeded before are not doubled.
5. Mark them replayed, and alert if they land in the DLQ again.

> **Pitfall:** A DLQ nobody watches. Records pile up for months and become a silent data-loss mechanism. Alert on DLQ inflow rate and on the age of the oldest dead letter, give the DLQ an owner, and set retention long enough to fix and replay.

> **Pitfall:** Retrying permanent errors. Retrying a malformed record five times with backoff wastes minutes per record and blocks the partition behind it. Classify, then retry only what can succeed.

::: deeper
**Retry topics with delays.** Kafka has no built-in delayed redelivery. A pattern popularised by Uber chains topics: a failure goes to `orders-retry-1` (consumed after 1 minute), then `orders-retry-2` (after 10 minutes), then `orders-dlq`. The main consumer never blocks, and transient downstream outages get time to recover. Kafka Connect has its own error tolerance and DLQ settings, covered in [Data ingestion](#ingestion).

**Ordering.** Moving a record to a DLQ breaks ordering for its key: later updates to the same entity can be applied before the failed one is replayed. For keyed state (account balances), either stop processing that key until the dead letter is resolved, or make replay version-aware so an older event cannot overwrite newer state (the version guard from the idempotency topic).

**Pub/Sub delivery attempts are approximate.** The delivery-attempt count is best-effort, so a message may be forwarded slightly earlier or later than the configured maximum. Exactly-once processing still needs idempotent sinks.

**Systemic failure detection.** A circuit breaker compares the failure rate in a window with a threshold (for example more than 50% of the last 1,000 records). When it trips, the pipeline pauses and pages instead of dead-lettering everything, which would turn a five-minute bug into a large replay job.

> **Interview:** "Design error handling for a Pub/Sub to BigQuery streaming pipeline." Classify errors; client-library retries with backoff for transient BigQuery errors; a Dataflow `DoFn` with a dead-letter output to a BigQuery table holding payload, error, message ID, pipeline version and time; a Pub/Sub dead-letter topic as a last resort for messages that crash the worker; a circuit breaker on failure rate; alerts on DLQ rate and oldest-age; and a replay job that republishes selected dead letters after a fix, relying on idempotent writes.
:::

::: check
?? Why should a dead letter store the original payload unchanged?
So it can be inspected and replayed exactly as received after the fix. A partially transformed or truncated payload may hide the problem and cannot be reprocessed by the normal pipeline.

?? What is wrong with sending every failure to the DLQ when a newly deployed bug makes all records fail?
The DLQ fills with good data that failed only because of the bug, the pipeline looks healthy, and recovery becomes a large replay. A circuit breaker should stop the pipeline and alert instead.

?? You created a Pub/Sub dead-letter topic but the dead letters are gone. What was probably missing?
A subscription on the dead-letter topic. Messages published to a topic with no subscriptions are not retained (unless message retention is enabled on the topic itself).
:::

::: resources
- Docs: [Pub/Sub dead-letter topics](https://cloud.google.com/pubsub/docs/dead-letter-topics) · retry policies, dead-letter configuration, permissions and attributes
- Blog: [Error handling patterns in Kafka](https://www.confluent.io/blog/error-handling-patterns-in-kafka/) · stop, dead letter, retry topics and ordering trade-offs
- Blog: [Building reliable reprocessing and dead letter queues with Apache Kafka](https://www.uber.com/blog/reliable-reprocessing/) · Uber's retry-topic design
- Docs: [Dataflow pipeline best practices: dead-letter queues](https://cloud.google.com/dataflow/docs/guides/pipeline-best-practices) · Google's guidance on error outputs in Beam pipelines
- Docs: [Retry strategy for Cloud Storage](https://cloud.google.com/storage/docs/retry-strategy) · exponential backoff, jitter and which operations are safe to retry
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · Kleppmann on faults, retries and idempotence in stream processing
:::

### Backfill Strategies & Historical Reprocessing

::: plain
A **backfill** is running a pipeline for dates in the past: to fill history for a new table, or to redo days that were computed wrongly. It is like reprinting last month's newspapers after finding a typo in the printing plate: you want to reprint only the affected days, without disturbing today's paper, without running out of paper halfway, and with a list of which days are already done in case the press breaks down.
:::

**Why backfills happen.**

- A **new pipeline** or a new column needs history, not just data from today onwards.
- A **bug** produced wrong results for a period.
- **Late or restated source data**: the source corrected last quarter's numbers.
- A **logic change** (a new definition of "active customer") must apply to all history.

**The vocabulary.**

| Term | Meaning |
|---|---|
| Backfill | Run the normal pipeline for past intervals, interval by interval |
| Reprocessing | Recompute existing outputs with new logic or corrected inputs |
| Full refresh | Rebuild the whole output from all inputs in one run |
| Replay | Re-read an input stream from an earlier position (an offset, a timestamp) |

**The precondition: idempotent, interval-scoped runs.** Everything earlier in this section pays off here. If each run rebuilds exactly its interval (partition overwrite or merge) and depends only on its interval, not on the clock, then a backfill is just "run the normal job for each past interval". If runs append, or read `max(updated_at)` from the target, backfills duplicate data or skip it.

**Strategies.**

| Strategy | How | Good for | Cost and risk |
|---|---|---|---|
| Partition by partition | Orchestrator runs the job once per past interval | Most fact tables | Many small runs; parallelism must be throttled |
| Full refresh | One run rebuilds everything | Small or medium tables, logic changes | One huge query; the table is replaced at once |
| Side-by-side rebuild | Build `fct_orders_v2` fully, compare with v1, then switch readers | Big logic changes on critical tables | Double storage for a while; safest for consumers |
| Event-time batches | dbt microbatch with `--event-time-start` and `--event-time-end` | dbt incremental models | Requires `event_time` configured upstream |
| Stream replay | Reset consumer offsets or seek a subscription to a past time | Streaming pipelines | Only as far back as the stream's retention |

**With an orchestrator.** In Airflow 3, backfills are run by the scheduler and created from the UI, the REST API or the CLI:

```bash
airflow backfill create --dag-id orders_daily \
  --from-date 2026-10-01 --to-date 2026-10-31 \
  --max-active-runs 4 --reprocess-behavior failed
```

`--max-active-runs` throttles how many days run at once, and `--reprocess-behavior` decides whether intervals that already have runs are rerun (`none`, `failed` or `completed`). Dagster launches backfills over a selection of **partitions** of an asset, either as one run per partition or, with a single-run backfill policy, as one run covering a range, which suits engines that process a range efficiently. [Orchestration](#orchestration) covers both tools.

**With dbt microbatch:**

```bash
dbt run --select fct_events --event-time-start "2026-10-01" --event-time-end "2026-11-01"
dbt retry     # rerun only the batches that failed
```

**Replaying streams.** A Pub/Sub subscription that retains acknowledged messages (or whose topic retains messages) can be rewound:

```bash
gcloud pubsub subscriptions seek orders-sub --time=2026-11-20T00:00:00Z
```

For Kafka, stop the consumer group and reset its offsets (`kafka-consumer-groups.sh --reset-offsets --to-datetime ... --execute`). Replay only works as far back as retention, which is why many teams also archive raw events to Cloud Storage or a bronze table for long-range reprocessing.

**A control table makes a long backfill resumable.** Record each partition's status; after a crash, the next run skips what is done. This runs as is:

```python
import sqlite3
from datetime import date, timedelta

db = sqlite3.connect(":memory:")
db.execute("""CREATE TABLE backfill_control (
                backfill_id TEXT, part TEXT, status TEXT, PRIMARY KEY (backfill_id, part))""")

def plan(backfill_id, start, end, newest_first=True):
    days = [start + timedelta(d) for d in range((end - start).days + 1)]
    with db:
        db.executemany("INSERT OR IGNORE INTO backfill_control VALUES (?, ?, 'pending')",
                       [(backfill_id, d.isoformat()) for d in days])
    order = "DESC" if newest_first else "ASC"
    return [p for (p,) in db.execute(
        f"SELECT part FROM backfill_control WHERE backfill_id = ? AND status != 'done' ORDER BY part {order}",
        (backfill_id,))]

def rebuild_partition(part, crash_on=None):
    if part == crash_on:
        raise RuntimeError(f"worker lost while rebuilding {part}")
    # real code: DELETE + INSERT the partition in one transaction (idempotent)

def run(backfill_id, start, end, crash_on=None):
    done = []
    for part in plan(backfill_id, start, end):
        rebuild_partition(part, crash_on)
        with db:
            db.execute("UPDATE backfill_control SET status = 'done' WHERE backfill_id = ? AND part = ?",
                       (backfill_id, part))
        done.append(part[5:])
    return done

try:
    run("fix-1123", date(2026, 11, 1), date(2026, 11, 7), crash_on="2026-11-04")
except RuntimeError as e:
    print(e)
print("resumed, rebuilt:", run("fix-1123", date(2026, 11, 1), date(2026, 11, 7)))
print(db.execute("SELECT status, COUNT(*) FROM backfill_control GROUP BY status").fetchall())
# worker lost while rebuilding 2026-11-04
# resumed, rebuilt: ['11-04', '11-03', '11-02', '11-01']
# [('done', 7)]
```

The first attempt finished 7, 6 and 5 November (newest first) before crashing; the second attempt did only the remaining four days.

**Newest first or oldest first?** Newest first restores the data people look at most, soonest. Oldest first is required when each interval depends on the previous one (running balances, cumulative metrics, type 2 history built incrementally). Know which kind your table is before you start.

**Plan the backfill like a release.**

1. **Scope**: which tables, which intervals, and everything downstream that must be rebuilt after them (lineage tells you).
2. **Capacity**: estimate bytes or slot-hours per interval times intervals; on BigQuery, run large backfills in a separate reservation or with a cap on concurrency so production jobs are not starved.
3. **Isolation**: write into a side table or branch and switch when done, or overwrite partitions in place if consumers can tolerate a mix of old and new for a while.
4. **Communication**: tell consumers which numbers will change and when; annotate dashboards.
5. **Verification**: compare row counts and key totals per interval before and after.

> **Pitfall:** Backfilling a snapshot-based history table. A type 2 dimension built by snapshots records only what it saw when it ran; you cannot backfill states you never observed. If history matters, capture it with CDC or keep raw daily extracts from day one.

> **Pitfall:** Forgetting downstream. Fixing `stg_orders` for October does nothing for `fct_revenue` and the finance dashboard until they are rebuilt for October too. Select the downstream graph (`dbt build --select stg_orders+`) or let the orchestrator propagate.

::: deeper
**State during backfills.** Incremental logic that reads its own target (`max(updated_at) from {{ this }}`) behaves differently when old intervals are rerun: the filter is based on the newest data, so old intervals select nothing. Backfill such models with `--full-refresh`, with microbatch, or with explicit interval variables passed to the model. Streaming jobs with large state (windows, joins) usually need a new job started from an earlier offset with fresh state, writing to a new output, then a switch: the **Kappa** approach to reprocessing ([Streaming systems](#streaming)).

**Backfill versus full refresh.** A full refresh is simpler and sees all data at once (necessary when logic spans intervals, such as "first order ever"), but is one large, all-or-nothing job. Partition backfills are resumable and throttleable but run the per-interval logic many times and can be slower overall because of per-run overhead. Use full refresh up to the size where it fits comfortably in your time window and capacity, partition backfills beyond that.

**Cost control on BigQuery.** A per-interval query that scans the whole source table (because the source is not partitioned on the filter column) multiplies cost by the number of intervals: 365 daily runs each scanning 2 TiB is 730 TiB. Check the bytes estimate of one interval with a dry run (`bq query --dry_run`) before launching a year.

**Airflow catchup.** When a DAG has a start date in the past and `catchup=True`, the scheduler creates runs for every missed interval automatically, which is an accidental backfill. Airflow 3 changed the default to not catching up; set it explicitly either way.

> **Interview:** "You found a bug that corrupted the revenue table for the last 90 days. How do you fix it?" Fix and test the logic; use lineage to list downstream tables and dashboards; estimate cost with a dry run; backfill the 90 days partition by partition (or rebuild side by side for a critical table), throttled and tracked in a control table, newest first unless there are cross-day dependencies; rebuild downstream; reconcile totals against the source; tell consumers which figures changed. Mention that this is only easy because runs are idempotent and interval-scoped.
:::

::: check
?? Why must an incremental model that filters on `max(updated_at)` from its own table be treated specially in a backfill?
When rerun for an old interval, the filter is still based on the newest data in the target, so it selects nothing for the old interval. Use a full refresh, microbatch, or explicit interval parameters.

?? When must a backfill run oldest first?
When each interval depends on the result of the previous one, such as running balances, cumulative metrics or incrementally built history.

?? What limits how far back you can replay a Pub/Sub subscription or a Kafka topic?
Their retention: acknowledged-message or topic retention in Pub/Sub, and the topic's retention settings in Kafka. Older data must come from an archive such as a bronze table or files in Cloud Storage.
:::

::: resources
- Docs: [Airflow backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html) · scheduler-managed backfills, reprocess behaviour and concurrency in Airflow 3
- Docs: [Dagster: backfilling data](https://docs.dagster.io/guides/build/partitions-and-backfills/backfilling-data) · partition backfills and backfill policies
- Docs: [dbt microbatch incremental models](https://docs.getdbt.com/docs/build/incremental-microbatch) · event-time batches, lookback and targeted backfills
- Docs: [Pub/Sub replay with seek](https://cloud.google.com/pubsub/docs/replay-overview) · snapshots, seek to a time and retention settings
- Article: [Functional Data Engineering](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) · why pure, partitioned tasks make reprocessing routine
:::

### Pipeline Observability & Lineage

::: plain
Observability means being able to answer questions about your pipelines that you did not plan to ask: why is this table late, why did yesterday's row count halve, what did this job cost. **Lineage** is the family tree of data: which tables and jobs a number came from, and which reports depend on a table. Together they turn "the dashboard is wrong" from a day of detective work into a few queries.
:::

[DataOps and platform engineering](#dataops) covers monitoring, alerting, logging and incident handling in general, and [Data governance, security and compliance](#governance) covers lineage platforms (OpenLineage, Marquez, DataHub, Knowledge Catalog lineage) and data observability tools. This topic is about the pipeline's side: what each run should record and emit, so that those tools have something to work with.

**The signals.** Data observability is usually summarised as five pillars:

| Pillar | Question | Cheap source |
|---|---|---|
| Freshness | When was the table last updated, and is that on time? | Table metadata, run records |
| Volume | How many rows arrived, compared with normal? | Run records, `INFORMATION_SCHEMA` partitions |
| Schema | Did columns or types change? | Table metadata, connector events |
| Distribution | Are values within normal ranges (nulls, averages, categories)? | Queries on the new partition |
| Lineage | Where did it come from, and who depends on it? | Lineage events, dbt manifest, query logs |

Add a sixth for pipelines specifically: **cost**, the bytes and slot time each job uses.

**A run ledger.** The most useful single thing a pipeline can do is write one row per run of each task to a table:

```sql
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
  run_id STRING NOT NULL,            -- unique per attempt
  pipeline STRING, task STRING,
  interval_start TIMESTAMP, interval_end TIMESTAMP,
  code_version STRING,               -- git commit of the deployed code
  started_at TIMESTAMP, finished_at TIMESTAMP,
  status STRING,                     -- success, failed, skipped
  rows_read INT64, rows_written INT64, rows_quarantined INT64,
  output_table STRING, error STRING
)
PARTITION BY DATE(started_at);
```

With it, freshness ("when did `fct_orders` last succeed?"), volume trends, duration trends, the share of on-time days (a contract's service level) and "which code version produced this partition?" are all simple SQL.

**Cost attribution with labels.** BigQuery jobs can carry labels; put the pipeline and task on every job, and cost per pipeline becomes a query over job history:

```sql
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key = 'pipeline') AS pipeline,
  COUNT(*) AS jobs,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4), 2) AS tib_billed,
  ROUND(SUM(total_slot_ms) / 1000 / 3600, 1) AS slot_hours
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND job_type = 'QUERY'
GROUP BY pipeline
ORDER BY tib_billed DESC;
```

Labels come from `bq query --label=pipeline:orders_daily`, from the `labels` argument of the client libraries, or in dbt-bigquery from the `query-comment` setting with `job-label: true` and per-model `labels` config.

**Lineage: where it comes from.** There are three ways to collect it, and mature platforms combine them:

1. **Declared**: the tool knows its own graph. dbt's `manifest.json` has every `ref` and `source`; Dataform has the same.
2. **Emitted at run time**: jobs send events describing their inputs and outputs. **OpenLineage** is the open standard: a run event with the job, the run ID and input and output datasets, plus facets such as schema and column lineage. Airflow, Spark, dbt and Flink have integrations; custom Python jobs can emit events with the OpenLineage client. Marquez, DataHub and Knowledge Catalog accept them.
3. **Inferred from query logs**: parse the SQL that ran. Knowledge Catalog lineage does this automatically for BigQuery jobs; SQL parsers such as `sqlglot` can derive table and column lineage from SQL text.

**Column-level lineage** goes from "table A feeds table B" to "`fct_revenue.amount` is computed from `stg_payments.amount_cents` and `stg_orders.currency`". It is what you need to answer "can I drop this column?" and to trace personal data for privacy work.

**Using lineage: impact analysis.** When a source is wrong or about to change, walk the graph downstream to find everything affected. This runs as is:

```python
from collections import deque

edges = {  # dataset -> datasets built from it (from lineage events or dbt's manifest)
    "raw.shop_orders":     ["stg.orders"],
    "raw.shop_payments":   ["stg.payments"],
    "stg.orders":          ["int.order_payments", "mart.dim_customer_orders"],
    "stg.payments":        ["int.order_payments"],
    "int.order_payments":  ["mart.fct_revenue"],
    "mart.fct_revenue":    ["dashboard:finance_daily", "ml:churn_features"],
    "mart.dim_customer_orders": [],
}

def downstream(node):
    """Breadth-first walk: everything affected if `node` is wrong, with its distance."""
    seen, queue, out = {node}, deque([(node, 0)]), []
    while queue:
        current, dist = queue.popleft()
        for child in edges.get(current, []):
            if child not in seen:
                seen.add(child)
                out.append((dist + 1, child))
                queue.append((child, dist + 1))
    return out

for dist, name in downstream("raw.shop_payments"):
    print(dist, name)
# 1 stg.payments
# 2 int.order_payments
# 3 mart.fct_revenue
# 4 dashboard:finance_daily
# 4 ml:churn_features
```

Walking the other way (upstream) is **root-cause analysis**: from a wrong dashboard back to the first table whose checks or run record look abnormal.

**Logs and traces.** Write structured logs (JSON with `pipeline`, `task`, `run_id`, `interval_start` fields) so Cloud Logging can filter and count them, and propagate the run ID into every query label and lineage event. One ID that links the orchestrator run, its logs, its BigQuery jobs and its lineage events is what makes an investigation fast.

> **Pitfall:** Monitoring only task success. A task can succeed while writing zero rows (an empty source, a wrong filter). Always alert on freshness and volume of the output table, not just on the job's exit status.

> **Tip:** Make the run ledger and lineage part of the pipeline template (Pipeline Design Patterns). If every new pipeline gets them for free, coverage reaches 100%; if each team must add them by hand, it never does.

::: deeper
**OpenLineage run lifecycle.** A job emits `START` when it begins and `COMPLETE` or `FAIL` when it ends (with `RUNNING` and `ABORT` also defined), each carrying the same run ID. Inputs and outputs can be reported on any event; facets attach schema, data quality metrics, row counts and SQL text. Because events are per run, lineage is versioned: you can see which run of which job, at which code version, wrote a given partition. Knowledge Catalog's Data Lineage API accepts OpenLineage events, so custom jobs on Cloud Run or Managed Service for Apache Spark can join the same graph as BigQuery and Managed Airflow.

**Lineage granularity trade-offs.** Table-level lineage is cheap and covers most impact analysis. Column-level lineage needs SQL parsing or engine integration and breaks on dynamic SQL, `SELECT *` and UDFs; treat it as best-effort. Row-level provenance (NiFi's provenance, or keeping source keys and run IDs on every row) is the most precise and the most expensive.

**Metadata is cheap, data is not.** Freshness, volume, schema and cost signals can all come from metadata (run ledger, `INFORMATION_SCHEMA`, job history) at almost no cost. Distribution checks query data; restrict them to new partitions and sample.

**Service level reporting.** From the run ledger compute, per table, the share of days published before the promised time and the mean time to recover after failures. Publishing these numbers in the catalog changes behaviour more than any alert.

> **Interview:** "Finance says yesterday's revenue looks low. Walk through your investigation." Check the run ledger for `fct_revenue`'s latest run: status, interval, rows written versus normal, code version. Walk lineage upstream: freshness and row counts of `int.order_payments`, `stg.payments`, `raw.shop_payments`; find the first abnormal node (for example the payments connector loaded 40% of the usual rows). Check its sync logs, fix or re-sync, backfill the day, rebuild downstream with lineage, and add a volume monitor at the raw layer so it is caught before finance next time.
:::

::: check
?? Name the five pillars of data observability and one cheap source for freshness.
Freshness, volume, schema, distribution and lineage. Freshness can come from table metadata or from the run ledger's last successful run per table.

?? Why is alerting only on task failure not enough?
A task can succeed while producing wrong or empty output. Freshness and volume checks on the output table catch problems the exit status cannot.

?? What is the difference between declared, emitted and inferred lineage?
Declared lineage comes from the tool's own graph (dbt's manifest); emitted lineage is sent by jobs at run time (OpenLineage events); inferred lineage is derived by parsing the queries that ran (query logs).
:::

::: resources
- Docs: [OpenLineage documentation](https://openlineage.io/docs/) · the run event model, facets and integrations for Airflow, Spark, dbt and Flink
- Docs: [Knowledge Catalog data lineage](https://cloud.google.com/dataplex/docs/about-data-lineage) · automatic lineage for BigQuery and Google Cloud services, and the lineage API
- Docs: [BigQuery INFORMATION_SCHEMA.JOBS view](https://cloud.google.com/bigquery/docs/information-schema-jobs) · job history with labels, bytes and slot time for cost attribution
- Docs: [Marquez](https://marquezproject.ai/) · the reference OpenLineage server and UI
- Docs: [DataHub documentation](https://docs.datahub.com/docs/) · metadata platform with lineage, impact analysis and observability features
- Book: [Data Quality Fundamentals](https://www.oreilly.com/library/view/data-quality-fundamentals/9781098112035/) · Moses, Gavish and Vorwerck; the observability pillars and lineage-driven incident response
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Default architecture on BigQuery | ELT: land raw, transform with SQL (dbt or Dataform); EtLT for masking in flight |
| Make a batch rerunnable | Depend on the run's data interval, never on `CURRENT_DATE()` |
| Full refresh safely | `CREATE OR REPLACE TABLE ... AS SELECT` (atomic), never `TRUNCATE` then `INSERT` |
| Upsert | `MERGE ... WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE ...` |
| Late data | Rebuild the last N partitions each run; N from measured lateness |
| Limit load on a shared source | Airflow pool with N slots; bounded worker pool in code |
| Streaming query run as a batch | Spark `trigger(availableNow=True)` |
| Exactly-once in practice | At-least-once delivery plus idempotent sink (keys, version guard, overwrite) |
| Deduplicate raw events | `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _loaded_at) = 1` |
| dbt: build a model and its parents | `dbt build --select +fct_orders` |
| dbt: slim CI | `dbt build --select state:modified+ --defer --state prod-artifacts/` |
| dbt: backfill a microbatch model | `dbt run --select m --event-time-start 2026-10-01 --event-time-end 2026-11-01` |
| dbt: cheaper tests | `config: where: "..."`, `warn_if`, `error_if`, `store_failures: true` |
| Managed connector cost | Fivetran MAR = distinct keys changed per month |
| Singer messages | `SCHEMA`, `RECORD`, `STATE`; state emitted after records are stored |
| NiFi back-pressure | Per connection: object and size thresholds (default 10,000 and 1 GB) |
| Never show unchecked data | Write-audit-publish: candidate, `ASSERT`, atomic publish |
| Accept new fields on load | `bq load --schema_update_option=ALLOW_FIELD_ADDITION` |
| Pub/Sub dead letters | `--dead-letter-topic=... --max-delivery-attempts=10` plus a subscription on it |
| Replay a subscription | `gcloud pubsub subscriptions seek SUB --time=...` |
| Airflow 3 backfill | `airflow backfill create --dag-id D --from-date ... --to-date ...` |
| Cost per pipeline | Job labels plus `INFORMATION_SCHEMA.JOBS` grouped by label |

### Practice

::: try
**1. Make it idempotent.** A daily job runs `INSERT INTO mart.daily_sales SELECT sale_date, SUM(amount) FROM staging.sales WHERE sale_date = CURRENT_DATE() - 1 GROUP BY 1`. List what goes wrong when it is retried, backfilled, or when sales arrive a day late, and rewrite it for BigQuery so all three work.
:::

?? A solution to 1
Retries append a second row for the same day; backfills compute the wrong day because the query depends on the clock; late sales are never counted because each day is computed once. Parameterise by the run date, rebuild a lookback window, and replace partitions atomically:

```sql
DECLARE run_date DATE DEFAULT @run_date;
BEGIN TRANSACTION;
DELETE FROM mart.daily_sales
WHERE sale_date BETWEEN DATE_SUB(run_date, INTERVAL 3 DAY) AND run_date;
INSERT INTO mart.daily_sales (sale_date, amount)
SELECT sale_date, SUM(amount)
FROM staging.sales
WHERE sale_date BETWEEN DATE_SUB(run_date, INTERVAL 3 DAY) AND run_date
GROUP BY sale_date;
COMMIT TRANSACTION;
```

::: try
**2. Dedupe and apply.** Extend the idempotent sink from the idempotency topic so that it also writes a run summary: how many events were applied, skipped as duplicates and ignored as out of date. Run it on the same five events and check the counts.
:::

?? A solution to 2

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE orders (order_id TEXT PRIMARY KEY, status TEXT, version INT)")
db.execute("CREATE TABLE applied (event_id TEXT PRIMARY KEY)")
events = [("e1", "o1", "created", 1), ("e2", "o1", "paid", 2), ("e2", "o1", "paid", 2),
          ("e4", "o1", "shipped", 4), ("e3", "o1", "packed", 3)]
summary = {"applied": 0, "duplicate": 0, "stale": 0}
for event_id, order_id, status, version in events:
    with db:
        if db.execute("INSERT OR IGNORE INTO applied VALUES (?)", (event_id,)).rowcount == 0:
            summary["duplicate"] += 1
            continue
        changed = db.execute("""INSERT INTO orders VALUES (?, ?, ?)
                                ON CONFLICT(order_id) DO UPDATE SET status = excluded.status,
                                                                    version = excluded.version
                                WHERE excluded.version > orders.version""",
                             (order_id, status, version)).rowcount
        summary["applied" if changed else "stale"] += 1
print(summary)
# {'applied': 3, 'duplicate': 1, 'stale': 1}
```

::: try
**3. Design a connector strategy.** A company has 30 SaaS sources (CRM, billing, support, ads), one PostgreSQL application database with 400 GB and heavy updates, and a partner who drops CSV files on SFTP nightly. The warehouse is BigQuery. Choose a tool for each and explain how schema changes and deletes are handled.
:::

?? Answer to 3
SaaS sources: a managed connector (Fivetran or Airbyte), with explicit table and column selection, "allow new columns" for raw, staging models that filter soft deletes, and freshness checks per connection; estimate cost from changed rows per month. PostgreSQL: log-based CDC with Datastream into BigQuery (or Debezium if you need Kafka), because heavy updates and deletes make snapshots expensive and incremental extraction misses deletes. Partner files: a small custom job or NiFi flow (SFTP to Cloud Storage, then a BigQuery load) with schema validation, quarantine of bad rows and an alert when the file is late. Everything lands in raw; dbt staging uses explicit column lists and alerts on schema changes.

::: try
**4. Write-audit-publish for a dbt mart.** Your `fct_orders` model sometimes publishes duplicates when an upstream connector double-loads. Describe two ways to make sure consumers never see a version that fails the uniqueness test.
:::

?? Answer to 4
First, move the uniqueness test upstream: test (and deduplicate) the staging model, so `dbt build` skips `fct_orders` when staging fails and the old mart stays in place. Second, implement write-audit-publish: build `fct_orders` into a candidate (a custom materialization that builds `fct_orders__candidate`, runs the tests, then swaps or copies it over the published table), or build the whole project into a blue dataset and switch a view or the published dataset only after all tests pass. Either way the publish step must be atomic, and a failure leaves the previous version in place and alerts.

::: try
**5. Plan a backfill.** A bug in the currency conversion corrupted `fct_revenue` (partitioned by day, about 400 GB per day scanned to rebuild) for the last 60 days. Downstream are `agg_revenue_monthly` and two dashboards. Write the plan: order, throttling, cost estimate method, verification and communication.
:::

?? Answer to 5
Fix and unit-test the conversion logic. Estimate cost with a dry run of one day (about 400 GB times 60, roughly 24 TB scanned) and run it in a separate reservation or with low concurrency (for example 4 days at a time) so production is not starved. Rebuild newest first, since days are independent, tracking each day in a control table so the job can resume. Then rebuild `agg_revenue_monthly` for the affected months (found by lineage) and refresh the dashboards. Verify by reconciling daily totals with the source or ledger, and compare before and after per day. Tell finance in advance which months will change, and annotate the dashboards.

### What comes next

You can now build pipelines that are repeatable, observable and repairable: ETL or ELT by design, idempotent load patterns, dbt projects that scale, connectors that feed them, and quality gates, contracts, schema-change policies, dead-letter queues, backfills and lineage around them. What is still missing is the control room that runs all of this on time, in the right order, with retries, dependencies across teams and a clear view of what is running. The next section, **Orchestration**, covers that in depth: DAG design and dependency management, scheduling, sensors and event-driven triggers, Apache Airflow's internals and production operations (including Managed Airflow), Dagster's asset-based model, Prefect, and how to choose between them.
