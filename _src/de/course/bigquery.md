This section is a deep tour of BigQuery, Google Cloud's serverless data warehouse and the centre of most data platforms built on Google Cloud. It is written for data engineers who will design tables, load data, pay the bills and answer for slow queries. It assumes you know SQL well (SQL for data engineering), understand warehouses, columnar storage and table formats (Data warehousing, Data lakes and lakehouses, Query engine internals), and have the Google Cloud basics from Google Cloud foundations for data: projects, IAM, regions and Cloud Storage.

**What you will be able to do afterwards**

- Explain what happens inside BigQuery when a query runs, from the API call to Capacitor files on Colossus and back.
- Design datasets and tables that are cheap to store and cheap to query: storage billing models, partitioning, clustering, views and materialized views.
- Choose the right way to get data in (load jobs, the Storage Write API, transfers, external and Iceberg tables) and explain the trade-offs.
- Choose between on-demand and capacity pricing, size reservations, read a query plan, and put guard rails on cost.
- Use BigQuery's advanced SQL, BigQuery ML and the generative AI functions, and serve dashboards through BI Engine, Looker and Connected Sheets.
- Lock data down with IAM, row-level security, policy tags, dynamic masking and controlled sharing.

**The big picture.** BigQuery splits a classic database into separate services that scale on their own. **Storage** keeps your tables as compressed column files in Colossus, Google's distributed file system. **Compute** is a large shared pool of workers (Dremel) that BigQuery hands to your query for as long as it runs, measured in **slots**. A very fast network (Jupiter) joins the two, and a cluster manager (Borg) places the work. Because storage and compute are separate, you pay for them separately: storage by the GiB per month, compute either by the bytes each query scans (on-demand) or by the slot capacity you reserve (editions). Almost every design decision in this section is about one of two things: reading fewer bytes, or using slots well.

[[fig:bigquery_arch]]

> **Note:** Google Cloud renamed several products in April 2026. BigLake is now **Google Cloud Lakehouse** and BigLake metastore is the **Lakehouse runtime catalog**; BigQuery tables for Apache Iceberg are **Apache Iceberg managed tables**; Dataplex Universal Catalog is **Knowledge Catalog**; Dataproc is **Managed Service for Apache Spark**; Vertex AI is part of **Gemini Enterprise Agent Platform**; Looker Studio is **Data Studio**. API, CLI and IAM names did not change, so you will still see `biglake` and `dataproc` in commands and role names. This section uses the new names and gives the old one on first mention.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Architecture

You can use BigQuery for years by typing SQL into a box, but you cannot run it well without a mental model of what sits behind the box. This group builds that model in three layers: the engine that executes queries, the storage that holds the bytes (and how you are billed for them), and the objects you create on top: datasets, tables, views, materialized views, snapshots and clones.

The ideas here come back in every later group. Partitioning only makes sense once you know that BigQuery reads column files block by block; slot pricing only makes sense once you know what a slot is; and security only makes sense once you know which object you are granting access to.

### BigQuery architecture: Dremel, Colossus, Jupiter and Borg

::: plain
Picture a huge restaurant kitchen shared by thousands of customers. The pantry (storage) is in one building, the cooks (compute) are in another, and a fast conveyor belt (the network) links them. When your order (a query) arrives, a head chef splits it into steps and borrows as many cooks as the job needs for a few seconds, then hands them to the next customer. You never own a stove; you pay either for the ingredients you use or for a guaranteed number of cooks.
:::

BigQuery is a **serverless** warehouse: there are no clusters to create, size or patch. You create datasets and tables, submit SQL, and Google runs it on infrastructure shared by many customers. Four internal systems explain most of its behaviour.

**Colossus** is Google's distributed file system, the successor to GFS. BigQuery stores every table as files in Colossus, in a columnar format called **Capacitor**: each column of each block of rows is stored, encoded and compressed separately. Colossus replicates the data across zones in the region, which is where BigQuery's durability comes from, and you never see disks or replicas.

**Dremel** is the query engine. It turns your SQL into a plan of **stages**, and each stage is executed in parallel by many workers. The unit of worker capacity is the **slot**: a virtual CPU with some memory and network attached. A small query may use a handful of slots for a second; a large join may use thousands for minutes. BigQuery decides how many slots each stage wants, and the scheduler gives it what is available.

**Jupiter** is Google's datacenter network. It provides petabit-scale bandwidth inside a region, which is what makes separating storage from compute practical: workers can read column files from Colossus almost as fast as from a local disk, so there is no need to keep data next to compute.

**Borg**, Google's cluster manager (Kubernetes descends from its ideas), places Dremel workers on machines, restarts them when machines fail and shares hardware among services.

Between stages sits a **distributed in-memory shuffle tier**. Stage 1 workers write their output (for example rows hash-partitioned by a join key) into shuffle; stage 2 workers read from it. Because intermediate data lives in shuffle rather than inside a worker, BigQuery can change the plan while it runs, add or remove workers, and retry a failed worker without restarting the query.

**What happens when you run a query:**

1. The client (console, `bq`, a client library, a BI tool over JDBC/ODBC) calls the API and creates a **job** in a **project**. The project that runs the job is the one billed for compute, even if the tables live in another project.
2. The query service parses and analyses the SQL against table metadata, checks IAM permissions, and optimises the plan. Here it uses partition and block statistics to **prune** data it does not need to read.
3. Dremel runs the stages. Leaf stages read only the referenced columns of the needed blocks from Colossus. Later stages join, aggregate and sort, passing data through shuffle.
4. The result is written to a temporary table and returned. Identical queries can be answered from this cached result for about 24 hours at no compute cost.

You can see the cost of step 3 before running anything with a **dry run**, which plans the query and reports the bytes it would scan:

```bash
bq query --use_legacy_sql=false --dry_run \
  'SELECT name, SUM(number) AS total
   FROM `bigquery-public-data.usa_names.usa_1910_2013`
   WHERE state = "TX"
   GROUP BY name'
# Query successfully validated. ... this query will process N bytes of data.
```

The bytes depend only on the columns you reference (here `name`, `number` and `state`), not on the `WHERE` clause, because this public table is neither partitioned nor clustered. That single fact (cost follows columns and pruned blocks, not rows returned) drives most of the design advice in this section.

**Resource model in one table:**

| Concept | What it is | Why it matters |
|---|---|---|
| Project | Billing and quota boundary | Jobs bill compute to the project that runs them |
| Dataset | Container of tables, views, routines, models | Has a fixed location; unit of most access grants |
| Table | Columnar data plus schema | Storage billed per GiB per month |
| Job | One query, load, export or copy | Has an ID, statistics and a plan you can inspect |
| Location | Region (`us-central1`) or multi-region (`US`, `EU`) | Data and the jobs that read it must be in the same location (global queries, in preview, relax this) |
| Slot | Unit of compute | What capacity pricing sells |

> **Pitfall:** `LIMIT 10` does not make a query cheaper on an unclustered table. BigQuery still reads every referenced column in full; the limit only trims the output. Use the free table preview in the console, or select fewer columns, when exploring.

> **Tip:** Write column lists, not `SELECT *`. In a columnar store each column you name is a separate set of files to read, so a 200-column table queried for 5 columns costs about 2.5% of the full scan.

The scope of this topic also touches features covered in depth later: partitioning and clustering (how pruning works), slot management (reservations and autoscaling) and BI Engine (an in-memory layer in front of the same storage). Keep the picture above in mind; each of those features plugs into one box of it.

::: deeper
**From the 2010 paper to today.** The original Dremel paper described a serving tree: a root server received the query, intermediate "mixers" combined partial results, and leaf servers scanned columnar data on local disks. It also introduced the encoding of nested, repeated records into columns using repetition and definition levels, the idea later reused by Parquet. The 2020 retrospective, "Dremel: A Decade of Interactive SQL Analysis at Web Scale", explains how the system moved to disaggregated storage on Colossus, replaced the fixed tree with a flexible graph of stages connected by a disaggregated in-memory shuffle, and added **dynamic query execution**: the plan can change mid-flight as BigQuery learns real data sizes, for example switching join strategy or repartitioning a skewed stage.

**Metadata at scale.** With petabyte tables, the list of blocks and their statistics is itself big data. The "Big Metadata" paper (VLDB 2021) describes how BigQuery stores block-level metadata (min and max values, null counts and so on) in columnar form and queries it with the same engine, which is how pruning stays fast for tables with millions of blocks.

**Streaming storage.** Rows written through the Storage Write API land first in a write-optimised storage engine (described in the Vortex paper, SIGMOD 2024) and are converted to Capacitor in the background, which is why streamed rows are queryable within seconds.

**Runtime.** Since March 2026 the **advanced runtime** (enhanced vectorisation and short-query optimisations) is the default for all projects. Short, simple queries can also run in **optional job creation mode** (`JOB_CREATION_OPTIONAL` in `jobs.query`), which skips creating a full job when it is not needed and cuts latency for dashboards and APIs.

**Fault tolerance.** A query can be retried internally up to three times, each attempt limited to 6 hours. Because stages hand data to shuffle, a lost worker only repeats its own unit of work.

> **Interview:** "Why is BigQuery fast at scanning a billion rows but a poor choice for fetching one row by key thousands of times per second?" Because it has no row index and every query is a distributed job with planning overhead measured in tens or hundreds of milliseconds. It is built for throughput on large scans, not for point lookups. Use Bigtable, Spanner, AlloyDB or Cloud SQL for serving, and BigQuery for analysis.

> **Interview:** "What does separation of storage and compute buy you?" Each scales independently, many workloads can read the same data without copies, compute can be shared and billed per use, and storage durability does not depend on compute nodes staying alive.
:::

::: check
?? Name the four Google systems behind BigQuery and the job each one does.
Colossus stores the Capacitor column files; Dremel plans and executes queries in stages on slots; Jupiter is the network that links compute and storage at very high bandwidth; Borg schedules the workers onto machines.

?? A team in project A queries a table owned by project B. Which project pays for the query, and which pays for the storage?
Project A pays for the query compute, because the job runs there. Project B pays for storing the table.

?? Why does adding `WHERE state = "TX"` not reduce the bytes billed on an unpartitioned, unclustered table?
BigQuery must still read the whole `state` column (and every other referenced column) to find the matching rows. Only partition or cluster pruning, or referencing fewer columns, reduces bytes scanned.
:::

::: resources
- Docs: [BigQuery overview](https://docs.cloud.google.com/bigquery/docs/introduction) · the official map of BigQuery's components, interfaces and features
- Paper: [Dremel: Interactive Analysis of Web-Scale Datasets](https://research.google/pubs/pub36632/) · the 2010 paper: serving trees and nested columnar encoding
- Paper: [Dremel: A Decade of Interactive SQL Analysis at Web Scale](https://research.google/pubs/dremel-a-decade-of-interactive-sql-analysis-at-web-scale/) · how Dremel became BigQuery: disaggregated storage, shuffle and dynamic execution
- Blog: [BigQuery under the hood](https://cloud.google.com/blog/products/bigquery/bigquery-under-the-hood) · Google's short tour of Dremel, Colossus, Jupiter and Borg
- Blog: [Separation of storage and compute in BigQuery](https://cloud.google.com/blog/products/bigquery/separation-of-storage-and-compute-in-bigquery) · why the split matters and what it enables
- Book: [Google BigQuery: The Definitive Guide](https://www.oreilly.com/library/view/google-bigquery-the/9781492044451/) · Lakshmanan and Tigani; the architecture chapters are still the best long-form explanation, though pricing details are dated
- Course: [Google Cloud data engineering and analytics training](https://cloud.google.com/learn/training/data-engineering-and-analytics) · official learning paths with hands-on BigQuery labs
:::

### BigQuery storage: Capacitor, table types and storage billing

::: plain
BigQuery stores a table the way a library might store a survey if every question had its own binder: all the answers to question 1 together, all the answers to question 2 together. Reading one question across a million forms then means opening one binder, not a million forms. You pay rent for the shelf space, and the rent drops by about half for binders nobody has changed in 90 days.
:::

**Capacitor**, BigQuery's columnar format, stores each column of a block of rows separately and picks an encoding per column: dictionary encoding for repeated strings, run-length encoding for long runs of equal values, and others. Before encoding, Capacitor may **reorder rows within a block** to make runs longer and compression better; since tables have no defined row order, that is allowed. Each block carries statistics (such as min and max per column) that the planner uses to skip blocks. Nested and repeated fields (`STRUCT`, `ARRAY`) are stored column by column too, using the Dremel encoding, so querying `order.items.sku` reads only that leaf.

**Table types.** "Table" covers several different objects in BigQuery:

| Type | Where the data lives | You pay BigQuery storage for |
|---|---|---|
| Standard (native) table | BigQuery managed storage (Capacitor on Colossus) | All bytes |
| Table snapshot / table clone | Shares storage with its base table | Only bytes that differ from the base |
| Materialized view | BigQuery managed storage | The precomputed result |
| External table, BigLake table | Cloud Storage, Bigtable, Google Drive and others | Nothing (the source system bills) |
| Apache Iceberg managed table | Your Cloud Storage bucket, managed by BigQuery | Nothing in BigQuery; Cloud Storage bills the files |
| Object table | Metadata over files (images, PDFs) in Cloud Storage | Nothing (metadata only) |
| Temporary tables | Cached results, session and script temp tables | Cached results free; session/script temp tables at active rates |

**Storage billing.** You choose per dataset between two models:

- **Logical** (the default) bills **uncompressed** bytes, computed from data types: an `INT64` counts 8 bytes, a `FLOAT64` 8, a `NUMERIC` 16, a `BOOL` 1, a `STRING` 2 bytes plus its UTF-8 length. Old versions kept by time travel and fail-safe are free.
- **Physical** bills **compressed** bytes as actually stored, at a higher price per GiB, and it **does** bill time travel and fail-safe bytes.

Each model has an **active** rate and a **long-term** rate. A table, or each partition of a partitioned table, moves to long-term automatically after **90 consecutive days without modification**, at about half the active price, with no difference in performance. Loads that append, DML, streaming and `CREATE OR REPLACE` reset the timer; querying, copying out and exporting do not. The first 10 GiB per month is free.

At the time of writing the us-central1 prices per GiB per month are about $0.023 active and $0.016 long-term for logical bytes, and $0.04 active and $0.02 long-term for physical bytes. Prices differ by region and change, so always check the pricing page. The worked example below shows why physical billing often wins:

```python
GIB_PER_TIB = 1024
# USD per GiB per month in us-central1 (check the pricing page for your region)
ACTIVE_LOGICAL, ACTIVE_PHYSICAL = 0.023, 0.04

logical_tib = 10.0      # uncompressed size BigQuery computes from the data types
physical_tib = 1.5      # the same data after Capacitor compression
time_travel_tib = 0.6   # old versions kept by time travel (a high-churn table)
fail_safe_tib = 0.6     # old versions kept by fail-safe

logical_bill = logical_tib * GIB_PER_TIB * ACTIVE_LOGICAL
physical_bill = (physical_tib + time_travel_tib + fail_safe_tib) * GIB_PER_TIB * ACTIVE_PHYSICAL
print(f"logical billing:  ${logical_bill:,.2f} per month")    # $235.52 per month
print(f"physical billing: ${physical_bill:,.2f} per month")   # $110.59 per month
print(f"compression ratio needed to break even: {ACTIVE_PHYSICAL / ACTIVE_LOGICAL:.2f}x")  # 1.74x
```

If your data compresses better than roughly 1.7x and you do not rewrite it constantly, physical billing is cheaper. Measure before switching:

```sql
SELECT
  table_schema AS dataset,
  SUM(total_logical_bytes) / POW(1024, 4)  AS logical_tib,
  SUM(total_physical_bytes) / POW(1024, 4) AS physical_tib,
  SUM(time_travel_physical_bytes + fail_safe_physical_bytes) / POW(1024, 4) AS history_tib
FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
GROUP BY dataset
ORDER BY logical_tib DESC;

-- then, for a dataset where it pays off:
ALTER SCHEMA analytics SET OPTIONS (storage_billing_model = 'PHYSICAL');
```

After a change you must wait 14 days before changing that dataset again, and the change takes about 24 hours to take effect.

**Time travel and fail-safe.** Every change keeps the previous versions of the affected data for the **time travel window**: 7 days by default, configurable per dataset from 2 to 7 days with `max_time_travel_hours` (48 to 168, in multiples of 24). Within it you can query the past, restore a table or undelete a dataset:

```sql
-- what did the table look like an hour ago?
SELECT * FROM sales.orders
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);

-- put yesterday's version back as a new table
CREATE TABLE sales.orders_restored AS
SELECT * FROM sales.orders
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);
```

After time travel, data sits in **fail-safe** for 7 more days. Fail-safe is not configurable and not self-service: only Google support can recover from it, at table level.

> **Pitfall:** A pipeline that runs `CREATE OR REPLACE TABLE` on a 2 TiB table every hour keeps up to a week of old copies in time travel. Under physical billing you pay for all of them. Shorten the time travel window for such datasets, or switch the pipeline to `MERGE` or partition overwrites that touch less data.

[[fig:bigquery_storage]]

::: deeper
**Why physical billing changes behaviour.** Under logical billing a `DELETE` of last year's data saves money immediately. Under physical billing the deleted bytes stay billable for the time travel window plus 7 days of fail-safe. Teams on physical billing therefore set short time travel windows on staging datasets (which can be rebuilt) and keep the full 7 days on curated datasets (which cannot).

**Long-term is per partition.** In a date-partitioned table, old partitions go long-term while today's stays active. This is one of the strongest arguments for partitioning append-mostly tables: an unpartitioned table that receives one new row a day never becomes long-term at all.

**Write path.** Load jobs write Capacitor files directly. Streaming through the Storage Write API writes to a write-optimised store first; background processes convert, compact and recluster it into Capacitor. You never manage compaction for native tables.

**Snapshots and history.** `CREATE SNAPSHOT TABLE ... FOR SYSTEM_TIME AS OF` turns a point within the time travel window into a durable, read-only table, which is the standard way to keep a recovery point longer than 7 days.

**Encryption.** All data is encrypted at rest by default with Google-managed keys; customer-managed keys (CMEK) through Cloud KMS are covered in the security topic.

> **Interview:** "Your BigQuery storage bill doubled after a schema change, though row counts did not." Possible causes: a large rewrite (for example `CREATE OR REPLACE` or a backfill) reset partitions from long-term to active, and under physical billing the old versions are now in time travel and fail-safe. `INFORMATION_SCHEMA.TABLE_STORAGE` shows active, long-term, time travel and fail-safe bytes separately.
:::

::: check
?? What resets a partition's 90-day long-term storage timer, and what does not?
Any modification resets it: appending loads, DML, streaming writes and replacing the table. Querying, exporting or copying the table out do not.

?? When is physical storage billing a bad idea?
When data compresses poorly (below about 1.7x at current prices) or when the dataset is rewritten so often that time travel and fail-safe bytes, which physical billing charges for, outweigh the compression savings.

?? A user dropped a table 10 days ago, and the dataset uses the default time travel window. Can you restore it yourself?
No. The 7-day time travel window has passed, so the data is in fail-safe for up to 7 more days, and only Google support can recover it from there.
:::

::: resources
- Docs: [Overview of BigQuery storage](https://docs.cloud.google.com/bigquery/docs/storage_overview) · table types, metadata and the storage layer explained
- Docs: [Data retention with time travel and fail-safe](https://docs.cloud.google.com/bigquery/docs/time-travel) · windows, restores and how retention interacts with billing
- Docs: [BigQuery pricing: storage](https://docs.cloud.google.com/bigquery/pricing) · current logical and physical rates per region
- Blog: [Inside Capacitor, BigQuery's next-generation columnar storage format](https://cloud.google.com/blog/products/bigquery/inside-capacitor-bigquerys-next-generation-columnar-storage-format) · encodings and row reordering from the team that built it
- Paper: [Big Metadata: When Metadata is Big Data](https://www.vldb.org/pvldb/vol14/p3083-edara.pdf) · how block-level metadata is stored and used for pruning
- Blog: [BigQuery Admin reference guide: Storage internals](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-admin-reference-guide-storage) · a practitioner's walk through storage, partitions and billing
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · Kleppmann; the column-oriented storage chapter explains the general ideas behind Capacitor
:::

### Datasets, tables, views and materialized views in BigQuery

::: plain
A dataset is a folder with a lock on the door and a fixed street address. Inside it, a table holds data, a view is a saved question that is answered fresh every time you ask, and a materialized view is a saved question whose answer is kept on a sticky note and updated whenever the data changes. Snapshots are photographs of a table; clones are photocopies you are allowed to write on, but you only pay for the pages you change.
:::

**Datasets** sit under projects (`project.dataset.table`) and are where most design decisions live:

- **Location** is set at creation and cannot change. Choose a region (`europe-west3`) for data residency and lower latency, or a multi-region (`US`, `EU`) for higher availability. Tables in different locations cannot normally be joined.
- **Defaults** apply to new tables: default table expiration, default partition expiration, default collation, case-insensitive names, storage billing model and time travel window.
- **Access**: IAM roles granted on a dataset apply to everything in it. Datasets are the natural unit for domains (`sales_raw`, `sales_curated`) and for environments.

```sql
CREATE SCHEMA IF NOT EXISTS analytics_curated
OPTIONS (
  location = 'EU',
  description = 'Curated sales marts',
  default_partition_expiration_days = 400,
  max_time_travel_hours = 168,
  labels = [('team', 'sales'), ('env', 'prod')]
);
```

**Tables** have a schema of typed columns, which can nest: `STRUCT` for records and `ARRAY` for repeated values. Denormalising one-to-many data into arrays (an order with an array of line items) is idiomatic in BigQuery because it avoids a join and the line items are stored next to their order.

```sql
CREATE TABLE analytics_curated.orders (
  order_id    STRING NOT NULL,
  customer_id INT64,
  region      STRING,
  order_ts    TIMESTAMP,
  items ARRAY<STRUCT<sku STRING, qty INT64, price NUMERIC>>
)
PARTITION BY DATE(order_ts)
CLUSTER BY customer_id
OPTIONS (description = 'One row per order, items nested');
```

**Logical views** store only a query. Every read re-runs it and bills the bytes of the underlying tables. Views give a stable interface, hide complexity and, with **authorized views**, control access: you add the view to the source dataset's list of authorized views, and anyone who can read the view gets the view's result without having any access to the source tables. Two related tools: an **authorized dataset** authorizes every view in a dataset at once, and an **authorized routine** does the same for a table function or UDF.

```sql
-- in dataset sales_shared (readers have access only here)
CREATE VIEW sales_shared.orders_eu AS
SELECT order_id, order_ts, customer_id
FROM analytics_curated.orders
WHERE region = 'EU';
-- then authorize sales_shared.orders_eu on dataset analytics_curated
-- (console: Sharing > Authorize views, or the datasets API)
```

**Materialized views** (MVs) store a precomputed result and keep it fresh automatically. Three mechanisms make them useful:

1. **Incremental refresh.** When base tables change, BigQuery attempts a refresh within about 5 minutes, but not more often than the refresh cap (`refresh_interval_minutes`, default 30). The refresh processes only the changed data where possible.
2. **Always fresh reads.** If the MV is behind, a query combines the stored result with the recent changes from the base table, so answers are correct even between refreshes. You can trade freshness for speed with `max_staleness`.
3. **Smart tuning.** A query written against the *base table* can be rewritten automatically to use an incremental MV that can answer it, so dashboards speed up without changes.

```sql
CREATE MATERIALIZED VIEW analytics_curated.daily_revenue
OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)
AS
SELECT DATE(order_ts) AS day, customer_id,
       SUM(item.qty * item.price) AS revenue, COUNT(*) AS item_lines
FROM analytics_curated.orders, UNNEST(items) AS item
GROUP BY day, customer_id;
```

Incremental MVs support a restricted SQL subset (aggregations, filters, some joins). For anything else, `allow_non_incremental_definition = true` creates a **non-incremental** MV that is fully recomputed on refresh and requires a `max_staleness`; it cannot use smart tuning.

**Snapshots, clones and copies** all make a second table from a first one, with different costs:

| Object | Writable? | Storage cost | Typical use |
|---|---|---|---|
| Table copy (`CREATE TABLE ... COPY`) | Yes | Full size | A real independent duplicate |
| Table snapshot (`CREATE SNAPSHOT TABLE`) | No (restore to write) | Only data later changed or deleted in the base | Recovery points, month-end freeze |
| Table clone (`CREATE TABLE ... CLONE`) | Yes | Only data that differs from the base | Dev and test copies of production |

```sql
CREATE SNAPSHOT TABLE backups.orders_2026_10_31
CLONE analytics_curated.orders
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-10-31 23:59:59+00'
OPTIONS (expiration_timestamp = TIMESTAMP '2027-11-01 00:00:00+00');

CREATE TABLE dev.orders CLONE analytics_curated.orders;  -- writable, near-free at first
```

> **Pitfall:** Views on views on views hide cost. A dashboard query on the top view bills every byte of every base table the chain touches, every time. Materialize the expensive layers (as tables or MVs) and keep views thin.

> **Pitfall:** An authorized view must not live in the same dataset as its source tables if you want to keep the tables private: anyone with access to that dataset could read the tables directly. Put shared views in their own dataset.

::: deeper
**How MV reads stay correct.** BigQuery tracks which base-table changes the MV has absorbed. At query time it reads the MV, then reads only the delta (rows appended since the last refresh) and merges them. Appends are cheap to merge; updates and deletes in the base table can invalidate parts of an incremental MV, and BigQuery then falls back to reading the base table until the next refresh. MVs therefore suit append-mostly fact tables best. MVs can also be built over tables with active change data capture (GA in 2026), over Spanner data (non-incremental only) and over metadata-cached BigLake tables.

**Editions matter.** In the Standard edition you can only query existing MVs; creating them, automatic refresh and smart tuning require Enterprise, Enterprise Plus or on-demand. Check feature tables before choosing an edition.

**Limits worth knowing.** A table can have up to 10,000 columns, including nested ones. A base table can be referenced by up to 500 MVs across the organization. A table snapshot can capture any point within the dataset's time travel window (at most 7 days back). Clones and snapshots are not supported for Iceberg managed tables.

**Views and security functions.** `SESSION_USER()` inside a view lets one view return different rows to different users, a lightweight alternative to row-level security for simple cases.

> **Interview:** "View, materialized view or table: how do you choose?" A view when logic must always reflect the latest data and the query is cheap; a materialized view when the same aggregation is read often over an append-mostly table and you want automatic incremental maintenance; a table built by a pipeline when the logic is complex, non-incremental or must be versioned and tested (for example with dbt or Dataform).
:::

::: check
?? Why can a materialized view return correct results even if its last refresh was 20 minutes ago?
At query time BigQuery combines the stored result with the base-table changes made since the last refresh (unless you set `max_staleness`, which allows serving the stale result).

?? What do you pay for right after creating a table clone of a 5 TiB table?
Almost nothing for storage: a clone is billed only for data that differs from its base table, so cost grows only as you modify the clone (or the base changes data the clone still references).

?? What must be true for an authorized view to work?
The view must be added to the source dataset's authorized views list (or its dataset authorized), and users need read access to the view's dataset, not to the source.
:::

::: resources
- Docs: [Introduction to datasets](https://docs.cloud.google.com/bigquery/docs/datasets-intro) · locations, defaults, billing models and access
- Docs: [Authorized views](https://docs.cloud.google.com/bigquery/docs/authorized-views) · step-by-step setup and the security model
- Docs: [Introduction to materialized views](https://docs.cloud.google.com/bigquery/docs/materialized-views-intro) · incremental vs non-incremental, smart tuning and limits
- Docs: [Introduction to table snapshots](https://docs.cloud.google.com/bigquery/docs/table-snapshots-intro) · how snapshot storage is billed and restored
- Docs: [Introduction to table clones](https://docs.cloud.google.com/bigquery/docs/table-clones-intro) · writable copies that bill only the differences
- Blog: [The BigQuery admin reference guide: Resource hierarchy](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-admin-reference-guide-resource-hierarchy) · how to lay out projects and datasets in a real organization
- Blog: [How to perform joins and data denormalization with nested and repeated fields](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-explained-working-joins-nested-repeated-data) · when to nest arrays instead of joining
:::

## Designing and loading

With the architecture in mind, the practical questions are how to lay data out so queries read little of it, and how to get data in. This group covers the two layout tools that matter most (partitioning and clustering), the ingestion paths from batch files to row-by-row streams, and the open-format path where data stays in your own bucket as Apache Iceberg tables that other engines can read and write.

These choices are hard to change later. Re-partitioning a 50 TiB table means rewriting it, and switching an ingestion path means changing producers. Getting them right at design time saves more money than any later tuning.

### Partitioning and clustering in BigQuery

::: plain
Imagine a filing cabinet for invoices. **Partitioning** gives each day its own drawer, so a question about 2 March only opens one drawer. **Clustering** sorts the folders inside each drawer by customer number, so finding customers 200 to 299 means pulling out one short run of folders instead of flipping through the whole drawer. Both tricks let BigQuery skip data it would otherwise have to read and bill.
:::

A **partitioned table** is split into segments, each stored and managed separately. Three kinds exist:

| Partitioning | Column | Granularities | Notes |
|---|---|---|---|
| Time-unit column | `DATE`, `TIMESTAMP` or `DATETIME` | Hour, day, month, year (`DATE`: day, month, year) | Most common; boundaries in UTC |
| Ingestion time | Pseudocolumn `_PARTITIONTIME` (and `_PARTITIONDATE`) | Hour, day, month, year | Row goes where its arrival time falls |
| Integer range | An `INT64` column with start, end and interval | Ranges | e.g. tenant or customer buckets |

Rows with a `NULL` partitioning value go to a special `__NULL__` partition; values outside the allowed range (for time columns, before 1960-01-01 or after 2159-12-31; for integer ranges, outside start to end) go to `__UNPARTITIONED__`.

```sql
CREATE TABLE events.page_views (
  event_ts     TIMESTAMP NOT NULL,
  customer_id  INT64,
  country      STRING,
  url          STRING,
  payload      JSON
)
PARTITION BY DATE(event_ts)
CLUSTER BY customer_id, country
OPTIONS (
  require_partition_filter = TRUE,   -- reject queries that would scan every partition
  partition_expiration_days = 730    -- drop partitions older than two years
);

-- integer-range partitioning: 100 partitions of 1,000 tenants each
CREATE TABLE saas.usage (tenant_id INT64, day DATE, units INT64)
PARTITION BY RANGE_BUCKET(tenant_id, GENERATE_ARRAY(0, 100000, 1000));
```

**Partition pruning** happens at planning time: if the `WHERE` clause constrains the partitioning column with constant expressions, BigQuery reads only the matching partitions, and a dry run reports the reduced byte count exactly. That is the main advantage of partitioning over clustering: a **guaranteed, predictable cost** before the query runs.

```sql
-- prunes: constant range on the partitioning column
SELECT customer_id, COUNT(*) AS views
FROM events.page_views
WHERE event_ts >= TIMESTAMP '2026-03-01' AND event_ts < TIMESTAMP '2026-03-08'
GROUP BY customer_id;
```

**Clustering** sorts the data inside each partition (or the whole table, if unpartitioned) by up to **four** columns, in the order you list them, and groups rows into storage blocks with min and max statistics. A filter on the clustering columns lets BigQuery skip blocks whose ranges cannot match. Clustering suits high-cardinality columns (customer IDs, SKUs, URLs) that would be terrible partition keys. As new data arrives, BigQuery **reclusters automatically** in the background, at no charge and without using your slots.

A small simulation shows why sorting matters. Each block remembers its min and max; a block is read only if its range overlaps the filter:

```python
import random

random.seed(7)
rows = [random.randrange(0, 400) for _ in range(40_000)]  # customer_id of each row
BLOCK = 1_000                                             # rows per storage block

def to_blocks(values):
    return [values[i:i + BLOCK] for i in range(0, len(values), BLOCK)]

def blocks_read(blocks, lo, hi):
    # a block must be read only if its [min, max] range overlaps the filter
    return sum(1 for b in blocks if min(b) <= hi and max(b) >= lo)

unclustered = to_blocks(rows)          # rows in arrival order
clustered = to_blocks(sorted(rows))    # rows sorted by customer_id
print("unclustered:", blocks_read(unclustered, 200, 299), "of", len(unclustered), "blocks")  # 40 of 40
print("clustered:  ", blocks_read(clustered, 200, 299), "of", len(clustered), "blocks")      # 11 of 40
```

Unsorted, every block contains some customer between 200 and 299, so nothing can be skipped. Sorted, a quarter of the IDs means about a quarter of the blocks (11 rather than 10 because the ranges at the edges overlap).

[[fig:bigquery_prune]]

**Choosing between them.** Google's guidance, condensed:

- Partition on the column that almost every query filters by, usually the event or business date, when each partition will hold a meaningful amount of data (on the order of 10 GB or more), and when you want exact cost estimates, partition-level expiration or partition-level writes.
- Prefer clustering alone when partitions would be tiny, when you would exceed the **10,000 partitions per table** limit (hourly partitions reach it in about 14 months), or when queries filter on several high-cardinality columns. Tables or partitions above about 64 MB usually benefit from clustering.
- Combine them for large fact tables: partition by date, cluster by the next most common filters.

> **Pitfall:** Wrapping the partitioning column in a function or comparing it to a value computed by a subquery can defeat pruning. `WHERE DATE(event_ts) = '2026-03-02'` prunes on a `DATE(event_ts)`-partitioned table, but `WHERE FORMAT_TIMESTAMP('%Y-%m', event_ts) = '2026-03'` does not. When the bound comes from another table, compute it first into a script variable (`DECLARE d DATE DEFAULT (SELECT MAX(day) FROM ...)`) and filter on the variable.

> **Pitfall:** Clustering order matters. With `CLUSTER BY customer_id, country`, a filter on `country` alone skips far fewer blocks than a filter on `customer_id`, because rows are sorted by `customer_id` first.

> **Tip:** Set `require_partition_filter = TRUE` on every large partitioned table. One forgotten filter in a notebook can scan years of data; the option turns that mistake into an error.

::: deeper
**Cost estimates and clustering.** A dry run cannot know which blocks a clustering filter will skip, so it reports the upper bound (the partitions after pruning). The bytes actually billed, shown after the query, can be much lower. This is why on-demand cost guards (`maximum_bytes_billed`) sometimes reject queries that would have been cheap.

**Writes.** A partition can be addressed directly with a decorator, for example `bq load 'events.page_views$20260302' ...` with `--replace`, to atomically replace one day. A single load or query job can modify at most **4,000 partitions**, which matters for backfills: split a 20-year daily backfill into several jobs. Ingestion-time partitioned tables also have a limit on partition modifications per day.

**Changing layout.** You cannot change a table's partitioning in place; create a new table with `CREATE TABLE ... PARTITION BY ... AS SELECT` and swap. You can change clustering columns on an existing table (for example `bq update --clustering_fields`); newly written data follows the new specification, while existing data is not rewritten immediately.

**Clustering details.** Clustering columns must be top-level, non-repeated columns of types such as `STRING`, `INT64`, `NUMERIC`, `DATE`, `DATETIME`, `TIMESTAMP`, `BOOL` or `GEOGRAPHY`. For `STRING`, only the first 1,024 characters are used for sorting. Clustering also speeds up aggregations and joins on the clustering keys, because matching rows sit together.

**Inspecting partitions.** `INFORMATION_SCHEMA.PARTITIONS` lists each partition's row count, size, last modification time and storage tier, which is how you check that old partitions really are in long-term storage.

> **Interview:** "Partition by `customer_id`?" Usually no: with millions of customers you hit the 10,000-partition limit and get tiny partitions. Partition by date and cluster by `customer_id`. Integer-range partitioning on a tenant ID only makes sense when tenants are few and large and queries almost always target one tenant.
:::

::: check
?? Why does partition pruning give an exact dry-run estimate but clustering does not?
Partition pruning is decided at planning time from the filter and partition metadata, so the bytes are known in advance. Block skipping from clustering happens during execution, so the dry run can only report the upper bound.

?? A table partitioned by hour has 3 years of data. What limit does it hit?
The 10,000 partitions per table limit (3 years is about 26,000 hours). Use daily partitions and cluster by the timestamp, or partition by day and keep fine-grained filtering for clustering.

?? Why might `CLUSTER BY country, customer_id` be worse than `CLUSTER BY customer_id, country` for a workload that mostly filters on one customer?
Rows are sorted by the first clustering column first. With `country` first, one customer's rows are spread across the blocks of every country they appear in, so a customer filter skips fewer blocks.
:::

::: resources
- Docs: [Introduction to partitioned tables](https://docs.cloud.google.com/bigquery/docs/partitioned-tables) · partition types, limits and when to choose clustering instead
- Docs: [Introduction to clustered tables](https://docs.cloud.google.com/bigquery/docs/clustered-tables) · block pruning, automatic reclustering and limitations
- Docs: [Query partitioned tables](https://docs.cloud.google.com/bigquery/docs/querying-partitioned-tables) · which filters prune and which do not
- Docs: [Quotas and limits](https://docs.cloud.google.com/bigquery/quotas) · partitions per table, partitions per job and modification limits
- Blog: [BigQuery Admin reference guide: Query optimization](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-admin-reference-guide-query-optimization) · partitioning and clustering in the context of real workloads
- Book: [Google BigQuery: The Definitive Guide](https://www.oreilly.com/library/view/google-bigquery-the/9781492044451/) · the performance chapter explains partitioning and clustering trade-offs in depth
:::

### Loading and streaming into BigQuery

::: plain
There are three ways to stock a warehouse. You can deliver goods by the truckload once a night (batch **load jobs**: cheap and simple). You can have a conveyor belt feeding items one by one all day (**streaming**: fresh, but you pay per item). Or you can leave goods in a partner's warehouse and just keep a catalogue entry that says where they are (**external tables**: no copy, but every lookup is a trip across town).
:::

**Batch load jobs** read files and write them into a table as one atomic operation: either all rows land or none do. Supported formats are Avro, Parquet, ORC, CSV, newline-delimited JSON, and Datastore and Firestore exports. Loads are **free** when they run on the shared slot pool (named `default-pipeline`); you pay only to store the result. The shared pool has no capacity guarantee, so production pipelines with deadlines assign a reservation with job type `PIPELINE`.

```bash
bq load \
  --source_format=PARQUET \
  --time_partitioning_field=event_ts \
  --clustering_fields=customer_id \
  events.page_views \
  'gs://acme-landing/page_views/dt=2026-03-02/*.parquet'
```

The same job in SQL, which is handy inside scripts and scheduled queries:

```sql
LOAD DATA INTO events.page_views
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://acme-landing/page_views/dt=2026-03-02/*.parquet']
);
```

Prefer self-describing formats. Parquet and Avro carry types and nested structure; CSV and JSON need a schema or autodetection, which guesses from a sample and can guess wrong (a ZIP code becomes `INT64` and loses leading zeros).

Key limits: a load job can read up to 15 TB and 10 million files (the size limit does not apply on a reservation); a project can run 100,000 load jobs per day; and each table accepts **1,500 modifications per day** (each appending or overwriting load, copy or query job counts as one).

**Streaming** uses the **Storage Write API**, a gRPC API that accepts batches of rows encoded as Protocol Buffers or Apache Arrow and makes them queryable within seconds. In 2026 Google renamed the two streaming paths: the high-throughput gRPC API is now **Storage Write API (gRPC)**, and the old `tabledata.insertAll` JSON method is now **Storage Write API (REST)**. New work should use the gRPC API, which is cheaper and has stronger guarantees.

The gRPC API offers several stream types:

| Stream type | Visibility | Delivery | Use for |
|---|---|---|---|
| Default stream | Immediately on write | At-least-once | Most streaming: logs, events, Pub/Sub pipelines |
| Committed (application-created) | Immediately on write | Exactly-once, using offsets | Streaming where duplicates are unacceptable |
| Pending | All at once when you commit | Atomic batch | A load-job alternative for row-producing apps |
| Buffered | When you flush | Row-level control | Advanced, mainly the Apache Beam connector |

You rarely call the API by hand. The usual producers already speak it: **Dataflow** (Beam's BigQuery sink with the Storage Write API method), **Pub/Sub BigQuery subscriptions** (Pub/Sub writes messages straight into a table with no code), **Datastream** for change data capture from databases, and connectors for Spark and Kafka.

```bash
# Pub/Sub writes every message on the topic into the table, using the table's schema
gcloud pubsub subscriptions create orders-to-bq \
  --topic=orders \
  --bigquery-table=acme-prod:raw.orders \
  --use-table-schema
```

**Pricing** at the time of writing (US): Storage Write API (gRPC) $0.025 per GiB with the first 2 TiB per month free; Storage Write API (REST) $0.01 per 200 MiB with a 1 KB minimum per row. Batch loads on the shared pool cost nothing.

**Transfers and external data.** The **BigQuery Data Transfer Service** (DTS) runs managed, scheduled imports: Cloud Storage files, Google services such as Google Ads, Google Analytics 4 and YouTube, SaaS sources such as Salesforce and ServiceNow, databases such as MySQL, PostgreSQL, Oracle and Teradata, plus dataset copies and **scheduled queries**. **External tables** leave data where it is and read it at query time:

```sql
CREATE EXTERNAL TABLE raw.clicks_ext
WITH PARTITION COLUMNS          -- infer dt=... folders as a partition column
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://acme-landing/clicks/*'],
  hive_partition_uri_prefix = 'gs://acme-landing/clicks',
  require_hive_partition_filter = TRUE
);

-- federated query: push SQL to Cloud SQL / AlloyDB through a connection
SELECT * FROM EXTERNAL_QUERY(
  'acme-prod.europe-west3.orders-pg',
  'SELECT id, status, updated_at FROM orders WHERE updated_at > now() - interval ''1 day''');
```

| Path | Latency | Cost driver | Best for |
|---|---|---|---|
| Load job | Minutes | Free (shared pool) or `PIPELINE` slots | Files landing in Cloud Storage, nightly or hourly batches |
| Storage Write API (gRPC) | Seconds | Per GiB ingested | Event streams, CDC, Dataflow and Pub/Sub sinks |
| DTS | Scheduled | Free for many Google sources; some connectors charge | SaaS and Google marketing data, scheduled queries |
| External / federated | At query time | Query bytes plus source costs | Rarely queried data, exploration, operational lookups |

> **Pitfall:** Loading one small file per minute into the same table runs into the 1,500 modifications per table per day limit (1,440 minutes per day). Batch files (for example every 15 minutes) or switch to the Storage Write API: streaming writes are not load, copy or query jobs, so they do not count toward that limit.

> **Pitfall:** The default stream is at-least-once. A producer that retries after a timeout can write the same rows twice. Either use committed streams with offsets, or carry a unique event ID and deduplicate downstream (for example with `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1`).

> **Pitfall:** `INSERT INTO ... VALUES` one row at a time is not streaming. Each statement is a DML job with job overhead and quota impact. Use the Storage Write API for row-level ingestion.

::: deeper
**Exactly-once with committed streams.** The writer creates a stream and sends `AppendRows` requests with an explicit **offset**. If a retry repeats an offset that was already written, the API rejects it as a duplicate instead of appending twice. Combined with a source that can replay from a known position (Pub/Sub with ordering, Kafka with committed offsets), this gives end-to-end exactly-once writes. Pending streams add atomic multi-stream commits (`BatchCommitWriteStreams`), useful when many workers must publish a batch all at once.

**CDC into BigQuery.** Tables with a declared (unenforced) primary key can accept upserts and deletes through the Storage Write API using the pseudocolumn `_CHANGE_TYPE` (`UPSERT` or `DELETE`). BigQuery applies the changes in the background within a staleness bound you set with the table's `max_staleness` option. This is how Datastream keeps replicas of operational databases current without running `MERGE` jobs.

**Where streamed data lives.** Streamed rows land in write-optimised storage (Vortex) first, then background jobs convert them into Capacitor and recluster. Queries read both transparently.

**Metadata caching for external tables.** External tables re-list files on every query, which is slow for many files. BigLake tables (next topic) can cache file listings and statistics, and external tables are usually a staging step rather than a serving layer.

**Exports.** The reverse direction matters too: `EXPORT DATA` writes query results to Cloud Storage (and, on Enterprise editions, to Bigtable, Spanner or AlloyDB), batch exports on the shared pool are free up to 50 TiB per day, and the **Storage Read API** streams table data to Spark, Beam or pandas in parallel, billed per TiB read beyond a free monthly allowance.

> **Interview:** "Design ingestion for 50,000 events per second that must be queryable within a minute, with no duplicates." Producers publish to Pub/Sub with an event ID; a Dataflow job (or a Pub/Sub BigQuery subscription if no transformation is needed) writes through the Storage Write API into a table partitioned by event date and clustered by the main filter keys. Deduplicate either with committed streams and offsets in Dataflow, or with a downstream `MERGE` or `QUALIFY` on the event ID. Watch the per-GiB ingestion cost and set `require_partition_filter`.
:::

::: check
?? Why are batch loads free but streaming is billed?
Loads run on a shared, best-effort slot pool and write large files efficiently, so Google does not charge for them. Streaming needs always-available write capacity and per-row bookkeeping, so it is billed per GiB ingested.

?? Which Storage Write API stream type would you use to replace a nightly load job written by a custom application, and why?
A pending stream: rows are buffered and become visible all at once when the stream is committed, giving the same all-or-nothing behaviour as a load job.

?? A team loads a file every 30 seconds into one table. What will go wrong?
They will exceed the 1,500 table modifications per day limit (2,880 loads per day), and loads will start failing with quota errors.
:::

::: resources
- Docs: [Introduction to loading data](https://docs.cloud.google.com/bigquery/docs/loading-data) · every ingestion option compared, with links to each format
- Docs: [Introduction to the Storage Write API (gRPC)](https://docs.cloud.google.com/bigquery/docs/write-api) · stream types, semantics, quotas and client libraries
- Docs: [Introduction to the BigQuery Data Transfer Service](https://docs.cloud.google.com/bigquery/docs/dts-introduction) · supported sources, scheduling and backfills
- Docs: [Introduction to external data sources](https://docs.cloud.google.com/bigquery/docs/external-data-sources) · external tables and federated queries, and when to use each
- Paper: [Vortex: A Stream-oriented Storage Engine For Big Data Analytics](https://research.google/pubs/vortex-a-stream-oriented-storage-engine-for-big-data-analytics/) · the storage engine behind BigQuery streaming
- Blog: [How to load, import, or ingest data into BigQuery for analysis](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-explained-data-ingestion) · a guided tour of the ingestion options
:::

### Google Cloud Lakehouse and Apache Iceberg on BigQuery

::: plain
Native BigQuery tables are like a hotel safe: very convenient, but only the hotel's staff can open it. Open table formats such as Apache Iceberg are like keeping your valuables in your own safe at home, with a shared logbook that says what is inside. Any trusted engine (BigQuery, Spark, Trino, Flink) can read the logbook and open the safe, so you keep one copy of the data and are free to change tools.
:::

**Why open formats.** Many organizations want one copy of data that several engines can use: BigQuery for SQL and BI, Spark for heavy transformation and ML feature work, Flink for streaming, Trino for federation. **Apache Iceberg** (covered in Data lakes and lakehouses) makes that possible: data sits as Parquet files in object storage, and a tree of metadata files (table metadata, manifest lists, manifests) records the schema, partitioning and the exact set of files in each snapshot. A **catalog** stores the pointer to the current metadata file and makes commits atomic.

Google Cloud's product for this is **Google Cloud Lakehouse** (called BigLake until April 2026). Its pieces, from least to most managed:

| Option | Who writes | Data location | BigQuery features | Typical use |
|---|---|---|---|---|
| External table | Anyone | Cloud Storage | Read only, users need bucket access | Quick exploration |
| BigLake table | Other engines | Cloud Storage | Read, fine-grained security, metadata caching | Govern existing Parquet/ORC/Avro/Delta data |
| Iceberg external table | Other engines | Cloud Storage | Read Iceberg snapshots, time travel over Iceberg history | Query Iceberg written by Spark or Flink |
| Iceberg tables in the Lakehouse runtime catalog | Spark, Flink, Trino; BigQuery (DML in preview) | Cloud Storage | Query from BigQuery by catalog name | Engine-neutral lakehouse |
| Apache Iceberg managed table | BigQuery (DML, streaming) | Your Cloud Storage bucket | Nearly full: DML, streaming, clustering, partitioning, security, time travel | BigQuery-centric, but files must be open |

**BigLake tables and access delegation.** An ordinary external table makes BigQuery read files *as the user*, so every analyst needs Cloud Storage permissions and you cannot hide rows or columns. A **BigLake table** is created `WITH CONNECTION`: a **Cloud resource connection** has its own service account, that service account reads the bucket, and users are granted access only to the table. Because BigQuery mediates every read, row-level security, column-level security and data masking work on BigLake tables. **Metadata caching** stores file listings and statistics so queries over many files plan quickly; you choose how stale the cache may be.

```bash
# one-time: a connection whose service account will read and write the bucket
bq mk --connection --location=EU --connection_type=CLOUD_RESOURCE lake_conn
bq show --connection EU.lake_conn        # note the serviceAccountId, then grant it
gcloud storage buckets add-iam-policy-binding gs://acme-lake-eu \
  --member=serviceAccount:SERVICE_ACCOUNT_FROM_ABOVE --role=roles/storage.objectUser
```

```sql
-- BigLake table over existing Parquet files, with cached metadata
CREATE EXTERNAL TABLE lake.clicks
WITH CONNECTION `acme-prod.eu.lake_conn`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://acme-lake-eu/clicks/*'],
  max_staleness = INTERVAL 4 HOUR,
  metadata_cache_mode = 'AUTOMATIC'
);
```

**Apache Iceberg managed tables** (formerly BigQuery tables for Apache Iceberg) give you the BigQuery experience with open files. BigQuery writes Parquet data and Iceberg metadata into a bucket you own, supports `INSERT`, `UPDATE`, `DELETE`, `MERGE`, high-throughput streaming with the Storage Write API, multi-statement transactions, partitioning, clustering, time travel and fine-grained security, and runs **automatic storage optimisation**: adaptive file sizing, compaction, reclustering and garbage collection of unreferenced files.

```sql
CREATE TABLE lake.orders_iceberg (
  order_id STRING, customer_id INT64, order_ts TIMESTAMP, amount NUMERIC
)
CLUSTER BY customer_id
WITH CONNECTION `acme-prod.eu.lake_conn`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://acme-lake-eu/iceberg/orders'
);

-- let other engines see the current state as an Iceberg snapshot
EXPORT TABLE METADATA FROM lake.orders_iceberg;
```

Storage for Iceberg managed tables is billed by Cloud Storage, not as BigQuery storage; BigQuery bills the queries and the background optimisation work.

**The Lakehouse runtime catalog** (formerly BigLake metastore) is a serverless catalog shared by all engines. It implements the standard **Apache Iceberg REST catalog** API, commits transactions, and can **vend credentials**: it hands each engine short-lived, table-scoped tokens, so Spark jobs do not need broad bucket permissions. Spark connects like this (the endpoint is the documented global one; regional endpoints also exist):

```python
from pyspark.sql import SparkSession

catalog = "lake"
spark = (SparkSession.builder.appName("orders-etl")
  .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
  .config(f"spark.sql.catalog.{catalog}.type", "rest")
  .config(f"spark.sql.catalog.{catalog}.uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
  .config(f"spark.sql.catalog.{catalog}.warehouse", "gs://acme-lake-eu")
  .config(f"spark.sql.catalog.{catalog}.header.x-goog-user-project", "acme-prod")
  .config(f"spark.sql.catalog.{catalog}.rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
  .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate())

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")
```

On Managed Service for Apache Spark a single cluster property can set all of this for you.

[[fig:bigquery_lake]]

> **Pitfall:** Never add, replace or delete files under an Iceberg managed table's prefix with other tools. BigQuery does not track files it did not write: new files are garbage-collected as "untracked", and modified files make the table fail its consistency check and become unreadable. Grant other tools read-only access to that prefix, and create each table in an empty, unique prefix.

> **Pitfall:** Put the bucket in the same region as the dataset. Cross-region reads add latency and data transfer charges. Google also recommends avoiding object versioning, object lock, bucket lock and customer-supplied encryption keys on these buckets.

::: deeper
**Choosing a pattern.** If BigQuery is the main writer and other engines mostly read, use Iceberg managed tables: you get BigQuery's DML, streaming and optimisation, and open files as an exit door. If Spark or Flink are the main writers, create the tables in the Lakehouse runtime catalog and let BigQuery query them by catalog name. If you only need to govern files that already exist, use BigLake tables. Keep native tables for the hottest, most BigQuery-specific data (search indexes, BI Engine, materialized views with smart tuning), because they still have the widest feature support.

**Limitations of Iceberg managed tables** at the time of writing: no rename, copy, clone or snapshot; some types (`BIGNUMERIC`, `INTERVAL`, `JSON`, `RANGE`, `GEOGRAPHY`) are not supported. Partitioning, multi-statement transactions and the advanced runtime became GA for them in 2026, as did change history (`APPENDS`) and continuous queries.

**Iceberg versions.** The REST catalog endpoint supports Iceberg V2 tables (GA) and V3 (preview); V1 tables must be upgraded. Iceberg external tables read V3 features such as deletion vectors since 2026.

**Federation.** The Lakehouse can federate other catalogs (for example Databricks Unity Catalog and Snowflake Horizon) so BigQuery and Spark on Google Cloud can query those Iceberg tables without copying them, and an Apache Hive catalog endpoint replaces a self-managed Hive metastore.

**Delta Lake and Hudi.** BigQuery can also read Delta Lake tables as BigLake tables, and Hudi tables through generated manifests, but Iceberg is where Google's write support and catalog investment are concentrated.

**The BigLake paper** (SIGMOD 2024) explains the design: a single governance and metadata layer over open files, with the BigQuery engine adapted to read Parquet efficiently using cached statistics.

> **Interview:** "Why not just put Parquet in a bucket and point everything at it?" Without a table format and catalog you get no atomic commits, no consistent snapshots, no safe concurrent writers, no schema evolution and no time travel, and every engine must list files itself. Iceberg plus a shared catalog gives you all of those while keeping files open.
:::

::: check
?? What does the connection's service account give you in a BigLake table that a plain external table lacks?
Access delegation: users need access only to the table, not to the bucket, and because BigQuery reads the files on their behalf it can enforce row-level and column-level security and masking.

?? Your Spark job writes new Parquet files directly into the folder of an Iceberg managed table. What happens?
BigQuery does not track those files, so its garbage collection deletes them as untracked; if existing files were modified, the table can fail its consistency check and become unreadable. Write through BigQuery or use a catalog-managed Iceberg table instead.

?? Who bills the storage of an Apache Iceberg managed table?
Cloud Storage, because the files live in your bucket. BigQuery bills the queries and the background storage optimisation.
:::

::: resources
- Docs: [What is borderless Lakehouse?](https://docs.cloud.google.com/biglake/docs/introduction) · Google Cloud Lakehouse overview: Iceberg, the runtime catalog and engines
- Docs: [Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/iceberg-tables) · features, bucket best practices, limitations and pricing
- Docs: [About the Lakehouse runtime catalog](https://docs.cloud.google.com/bigquery/docs/about-blms) · catalog, metadata and data layers, and the Iceberg REST API
- Docs: [Introduction to BigLake tables](https://docs.cloud.google.com/bigquery/docs/biglake-intro) · access delegation, security and metadata caching
- Paper: [BigLake: BigQuery's Evolution toward a Multi-Cloud Lakehouse](https://research.google/pubs/biglake-bigquerys-evolution-toward-a-multi-cloud-lakehouse/) · the design behind BigLake and managed Iceberg
- Spec: [Apache Iceberg table spec](https://iceberg.apache.org/spec/) · the metadata model every engine in the picture implements
- Book: [Apache Iceberg: The Definitive Guide](https://www.oreilly.com/library/view/apache-iceberg-the/9781098148614/) · Iceberg internals, catalogs and operations, engine by engine
:::

## Compute, cost and performance

The previous group was about laying data out so that queries read less of it. This group is about the other half of the bill and of the latency: compute. You will learn how BigQuery sells compute (bytes scanned versus slot-hours), how to divide capacity between teams, how to read a query plan to find out why a query is slow, and how to put guard rails on spending before a surprise invoice arrives.

Performance and cost are the same problem seen from two sides. A query that reads fewer bytes is cheaper on-demand; a query that uses fewer slot-seconds is cheaper on capacity pricing and leaves more slots for everyone else. The tools in this group tell you which of the two you are fighting.

### BigQuery compute: slots, editions, reservations and autoscaling

::: plain
You can pay for a taxi in two ways. On a meter, you pay for the distance travelled, however many taxis the company sends. On a contract, you rent a number of cars by the hour and use them as much as you like. BigQuery's **on-demand** pricing is the meter: you pay for the bytes each query reads. **Capacity** pricing is the rental: you pay for slots (units of compute) per hour, whatever your queries read.
:::

Every query runs on **slots**, BigQuery's unit of compute (roughly a virtual CPU with memory and network). The pricing model decides only how you pay for them and how many you can get.

**On-demand (per TiB).** You pay for the bytes processed by each query: $6.25 per TiB in the US at the time of writing, with the first 1 TiB per month free. Each query is billed at least 10 MB per referenced table and per query. Cached results and failed queries cost nothing. A project can generally use up to **2,000 concurrent slots**, shared by all its queries, with short bursts above that; under regional contention you may get fewer. New projects have a default **QueryUsagePerDay** quota of 200 TiB, a safety net you can lower (next topics).

**Capacity (per slot-hour) with editions.** You create **reservations** of slots in one of three **editions**:

| | Standard | Enterprise | Enterprise Plus |
|---|---|---|---|
| Pay-as-you-go price (us-central1) | $0.04 per slot-hour | $0.06 | $0.10 |
| Slot commitments (1 or 3 years) | Not available | 20% or 40% off | 20% or 40% off |
| Baseline slots | No (autoscaling only) | Yes | Yes |
| Maximum reservation size | 1,600 slots | Quota | Quota |
| Idle slot sharing, target concurrency | No | Yes | Yes |
| BigQuery ML, BI Engine, continuous queries | No | Yes | Yes |
| Row/column security, masking, CMEK, VPC Service Controls | No | Yes | Yes |
| Managed disaster recovery, Assured Workloads compliance | No | No | Yes |
| Monthly SLO | 99.9% | 99.99% | 99.99% |

Separately from slot commitments, spend-based **BigQuery committed use discounts** give about 10% (1 year) or 20% (3 years) off pay-as-you-go in every edition. Prices differ by region; check the pricing page.

Slots are billed **per second with a one-minute minimum** by default. Reservations can opt in to **fluid scaling** (GA since June 2026) for per-second billing with no minimum, which helps spiky workloads made of many short queries.

**Reservations, baseline and autoscaling.** A reservation lives in an **administration project** and has:

- **Baseline slots** (`slot_capacity`): always allocated and always billed, covered by commitments if you have them.
- **Autoscaling** up to `autoscale_max_slots` more: BigQuery adds slots in multiples of 50 when queries need them and releases them when they do not. You pay for what is allocated, not for what queries actually use.
- **Idle slot sharing**: unused baseline and committed slots in one reservation can be used by others in the same admin project and are reclaimed within milliseconds when the owner needs them (turn off per reservation with `ignore_idle_slots`). Autoscaled slots are never shared.

**Assignments** connect consumers to reservations: a project, folder or organization is assigned for a **job type**: `QUERY`, `PIPELINE` (loads, exports, copies), `ML_EXTERNAL`, `BACKGROUND` (for example search index maintenance) or `CONTINUOUS` (continuous queries). Anything without an assignment, direct or inherited, uses on-demand pricing.

```sql
-- run in the administration project
CREATE RESERVATION `acme-admin.region-us.etl`
OPTIONS (edition = 'ENTERPRISE', slot_capacity = 300, autoscale_max_slots = 500);

CREATE RESERVATION `acme-admin.region-us.bi`
OPTIONS (edition = 'ENTERPRISE', slot_capacity = 200, autoscale_max_slots = 200,
         target_job_concurrency = 40);

CREATE ASSIGNMENT `acme-admin.region-us.etl.data_eng_folder`
OPTIONS (assignee = 'folders/123456789', job_type = 'QUERY');

CREATE ASSIGNMENT `acme-admin.region-us.bi.dashboards`
OPTIONS (assignee = 'projects/acme-dashboards', job_type = 'QUERY');
```

[[fig:bigquery_slots]]

**Fair scheduling.** Inside a reservation, slots are divided equally between **projects** with running queries, then between the queries of each project. With 1,000 slots and two busy projects, each project gets 500 whether it runs 1 query or 20. If one critical job needs a guaranteed share, give it its own reservation.

**Choosing a model.** On-demand charges for bytes; capacity charges for compute time. Scan-heavy, compute-light work (big filters over wide tables) is expensive on-demand and cheap on slots; small, complex queries are the opposite. A rough monthly comparison:

```python
ON_DEMAND_PER_TIB, FREE_TIB = 6.25, 1      # USD, US pricing
ENTERPRISE_PAYG, ENTERPRISE_3YR = 0.06, 0.036  # USD per slot-hour, us-central1
HOURS_PER_MONTH = 730

def on_demand(tib_scanned):
    return max(tib_scanned - FREE_TIB, 0) * ON_DEMAND_PER_TIB

def autoscale(avg_slots, busy_hours_per_day, days=30):
    return avg_slots * busy_hours_per_day * days * ENTERPRISE_PAYG

def committed(slots):
    return slots * HOURS_PER_MONTH * ENTERPRISE_3YR

print(f"on-demand, 300 TiB scanned:     ${on_demand(300):,.2f}")    # $1,868.75
print(f"on-demand, 1,000 TiB scanned:   ${on_demand(1000):,.2f}")   # $6,243.75
print(f"autoscale, 120 slots x 10 h/day: ${autoscale(120, 10):,.2f}")  # $2,160.00
print(f"3-year commitment, 100 slots:   ${committed(100):,.2f}")    # $2,628.00
```

If the same workload (about 120 busy slots for 10 hours a day) scans 300 TiB a month, on-demand is cheaper; if it scans 1,000 TiB, autoscaling slots cost about a third as much. Measure your real slot usage (`total_slot_ms` in `INFORMATION_SCHEMA.JOBS`) before deciding, and remember that projects can mix models: keep ad hoc analysis on-demand with a quota and put steady pipelines on a reservation.

> **Pitfall:** A new project created by another team is not covered by your reservation unless the assignment is at the folder or organization level. It silently falls back to on-demand. Assign at the folder or organization, and explicitly assign exceptions to `None`.

> **Pitfall:** Choosing Standard edition to "save money" can remove features you rely on: BigQuery ML, BI Engine, row-level security, column-level security, masking, CMEK and VPC Service Controls are not available on Standard reservations.

::: deeper
**What a slot request means.** When a stage starts, BigQuery decides its ideal parallelism (say 2,000 units of work). If only 1,000 slots are available, the other units queue and are picked up as slots free; the query is slower, not more expensive. Being slow because work is queued is called **slot contention**, and it is the most common cause of "my query got slower but nothing changed".

**Capacity priority.** When a region is under pressure, BigQuery serves capacity in this order: Enterprise and Enterprise Plus baseline and committed slots, then Enterprise Plus autoscaled slots, then Enterprise autoscaled slots, and last Standard edition and on-demand. Critical workloads should run on baseline or committed slots.

**Newer workload controls.** Reservation groups (2026) let a set of reservations share idle slots among themselves first. Project caps (scheduling policies) limit how many slots or concurrent queries one project may use inside a shared reservation. Assignments can also route jobs by the identity running them (principal-based assignments). These controls are Enterprise and Enterprise Plus features.

**Commitments and edition changes.** Commitments are bought per edition and region in the admin project, and are shared by that project's reservations. You cannot change a reservation's edition in place; you delete and recreate it.

**Monitoring.** `INFORMATION_SCHEMA.RESERVATIONS_TIMELINE` shows per-second allocation, including autoscaling; `JOBS_TIMELINE` shows per-second slot usage by job. The **slot estimator** in the console models how performance would change with more or fewer slots, based on your history.

> **Interview:** "Your team pays $40,000 a month on-demand. How do you decide whether to move to editions?" Pull 30 to 90 days of `total_slot_ms` from `JOBS_BY_ORGANIZATION`, convert it to average and peak slots per hour, price that as baseline (covered by commitments) plus autoscaling, and compare. Check which features you need (BigQuery ML, BI Engine, masking) to choose the edition, keep exploratory projects on-demand with quotas, and pilot one reservation before committing.
:::

::: check
?? A reservation has 500 baseline slots and an autoscale maximum of 1,000. What is the most it can use, and what are you billed for?
Up to 1,500 slots (plus idle slots borrowed from other reservations, if allowed). You are billed for the 500 baseline slots all the time and for the autoscaled slots while they are allocated, in multiples of 50.

?? Why might a project on a reservation still receive an on-demand bill?
It has no assignment for that job type, direct or inherited from its folder or organization, so its jobs ran on on-demand pricing.

?? In a 1,000-slot reservation, project A runs 1 large query and project B runs 20. How are slots shared if all queries want as many as possible?
Equally by project first: about 500 slots to project A's single query and 500 shared among project B's 20 queries.
:::

::: resources
- Docs: [Understand BigQuery editions](https://docs.cloud.google.com/bigquery/docs/editions-intro) · the authoritative feature table for each edition
- Docs: [Understand slots](https://docs.cloud.google.com/bigquery/docs/slots) · slot economy, fair scheduling, idle slots and capacity priority
- Docs: [Understand reservations](https://docs.cloud.google.com/bigquery/docs/reservations-workload-management) · baseline, autoscaling, assignments and job types
- Docs: [BigQuery pricing](https://docs.cloud.google.com/bigquery/pricing) · on-demand rates, slot-hour rates and commitment discounts by region
- Docs: [Estimate slot capacity requirements](https://docs.cloud.google.com/bigquery/docs/slot-estimator) · using your own history to size reservations
- Blog: [BigQuery Admin reference guide: Query processing](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-admin-reference-guide-query-processing) · how slots, stages and shuffle fit together, for administrators
:::

### Reading BigQuery query plans and tuning queries

::: plain
A query plan is the recipe BigQuery wrote for your query: which ingredients to fetch, which steps run in parallel, and what is passed from one step to the next. When a dish takes too long, you do not guess; you look at which step of the recipe was slow. Was the kitchen short of cooks, was one cook given far more work than the others, or did a step produce far more food than anyone needed?
:::

Every query job records its **execution plan**: a graph of **stages**, each made of **steps**. You can see it in BigQuery Studio under **Execution details** and the **Execution graph** (which, since 2026, can overlay a heatmap on your SQL text showing which parts were most expensive), or fetch it from the API:

```bash
bq show --format=prettyjson -j bquxjob_12ab34cd_0189 \
  | jq '.statistics.query.queryPlan[]
        | {name, recordsRead, recordsWritten, waitMsAvg, computeMsAvg, computeMsMax,
           shuffleOutputBytes, shuffleOutputBytesSpilled}'
```

**Stages** have names such as `S00: Input`, `S01: Join+` or `S02: Aggregate` and are connected by edges that represent shuffled data. **Steps** inside a stage include `READ`, `FILTER`, `COMPUTE`, `JOIN`, `AGGREGATE`, `ANALYTIC_FUNCTION`, `SORT`, `LIMIT`, `USER_DEFINED_FUNCTION` and `WRITE`. A typical step list looks like this:

```text
READ      $30:l_orderkey, $31:l_quantity FROM lineitem
AGGREGATE GROUP BY $100 := $30  $70 := SUM($31)
WRITE     $100, $70 TO __stage00_output BY HASH($100)
```

The `WRITE ... BY HASH` line means the stage's output is partitioned by the group key and shuffled to the next stage.

**Per-stage statistics** and what they reveal:

| Statistic | Meaning | Red flag |
|---|---|---|
| `recordsRead`, `recordsWritten` | Rows in and out of the stage | A join writing far more rows than either input read: join explosion |
| `waitMs` avg and max | Time workers waited to be scheduled | High wait: slot contention |
| `readMs`, `computeMs`, `writeMs` avg and max | Time reading input, computing, writing output | Max much larger than avg: skew |
| `shuffleOutputBytes` | Bytes sent to the next stage | Huge shuffle: filter or aggregate earlier |
| `shuffleOutputBytesSpilled` | Shuffle that overflowed to disk | Spill: too much intermediate data for available shuffle quota |
| `parallelInputs` | Units of work in the stage | One unit doing all the work: skew or unsplittable input |

BigQuery also adds **query performance insights** to the graph: **slot contention** (the query waited for slots), **insufficient shuffle quota** (shuffle quota scales with the slots you have; exceeding it spills to disk), **data input scale change** (an input grew sharply compared with previous runs) and **partition skew** (one join partition far larger than the others, which BigQuery could not fully rebalance).

**A reading routine.** Start from the slowest stage. If wait time dominates, it is capacity, not SQL. If compute max is ten times compute average, find the skewed key (often `NULL`, an empty string, or one giant customer). If a join stage writes many more records than it reads, check join keys for duplicates. If early stages read many more bytes than expected, check pruning and `SELECT *`.

**Common anti-patterns and fixes:**

| Anti-pattern | Why it hurts | Fix |
|---|---|---|
| `SELECT *` on wide tables | Reads every column | List columns; use `SELECT * EXCEPT (...)` |
| Filtering after a join or in an outer query | Shuffles rows that are later discarded | Filter (and pre-aggregate) before joining |
| Self-join to find the latest row per key | Doubles the scan and shuffle | `QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) = 1` |
| `COUNT(DISTINCT x)` on billions of rows | Large shuffle of distinct values | `APPROX_COUNT_DISTINCT(x)` when a small statistical error is acceptable |
| `ORDER BY` a huge result without `LIMIT` | Final sort on one worker | Sort only what you return, or write to a table unsorted |
| CTE referenced several times | May be evaluated more than once | Materialize to a temp table in a script |
| Joins on long strings or expressions | More bytes to hash and shuffle | Join on `INT64` keys; precompute expressions |
| Many small `INSERT` statements | Job overhead, DML quotas | Batch loads or the Storage Write API |
| JavaScript UDF in a hot path | Slower than native SQL | Rewrite as SQL; use a SQL UDF |

A worked rewrite: the latest status per order.

```sql
-- before: self-join, reads order_events twice and shuffles both copies
SELECT e.order_id, e.status
FROM shop.order_events e
JOIN (SELECT order_id, MAX(event_ts) AS max_ts
      FROM shop.order_events GROUP BY order_id) m
  ON e.order_id = m.order_id AND e.event_ts = m.max_ts;

-- after: one scan, one shuffle by order_id
SELECT order_id, status
FROM shop.order_events
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)   -- prune partitions too
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts DESC) = 1;
```

> **Pitfall:** Judging a query by its elapsed time alone. Elapsed time depends on how many slots were free; `total_slot_ms` (slot-seconds consumed) is the stable measure of how much work the query does. Compare slot-ms before and after a rewrite.

> **Tip:** Test tuning with the cache off (`--nouse_cache` in `bq`, or the console query settings), or the second run will come back from cached results in milliseconds and teach you nothing.

::: deeper
**Join strategies.** BigQuery chooses between a **broadcast join** (the small side is sent to every worker reading the large side; no shuffle of the large side) and a **hash join** (both sides are shuffled by the join key). Small dimension tables joined to large facts should broadcast; the plan shows it as a join stage with one large and one small input. Dynamic execution can switch strategies mid-query once real sizes are known.

**Skew.** Hash partitioning sends all rows with the same key to the same worker. A key with 30% of the rows (often `NULL`) makes one worker do 30% of the work. Fixes: filter or separately handle the hot key, add a salt to spread it, or aggregate before joining. The partition skew insight points to the stage.

**Optimizer helpers.** Declaring **primary and foreign keys** (`PRIMARY KEY (id) NOT ENFORCED`) lets the optimizer eliminate unnecessary joins and simplify plans; BigQuery does not enforce them, so they must be true. **History-based optimizations** reuse what BigQuery learned from previous runs of similar queries (for example better join ordering or parallelism). Search indexes, materialized views with smart tuning and BI Engine accelerate specific patterns.

**Tools.** Google's open-source BigQuery anti-pattern recognition tool flags many of the patterns above in SQL text. `INFORMATION_SCHEMA.JOBS` exposes `job_stages`, `total_slot_ms` and `query_info` for fleet-wide analysis, for example to find the queries with the most spilled shuffle.

> **Interview:** "A daily query went from 5 to 40 minutes with no code change." Compare the plans of a fast and a slow run: if wait time grew, look at slot contention in the reservation (other workloads, autoscaling maximum reached); if records read grew, an input table grew or lost pruning (a partition filter changed, clustering degraded); if one stage's max compute exploded, look for a new skewed key. The data input scale change and partition skew insights point to two of these directly.
:::

::: check
?? A stage shows `computeMsAvg` of 2 seconds and `computeMsMax` of 90 seconds. What is the likely problem, and how do you confirm it?
Data skew: one worker received far more data than the others. Check the distribution of the stage's partitioning key (for example a join or group key) with a `GROUP BY key ORDER BY COUNT(*) DESC` query; a dominant value such as `NULL` confirms it.

?? Why is `total_slot_ms` a better measure of a rewrite's effect than elapsed time?
Elapsed time depends on how many slots were available at the moment; slot-ms measures the work the query itself required, so it is comparable across runs.

?? The plan shows large `shuffleOutputBytesSpilled`. Name two remedies.
Reduce the intermediate data (filter, pre-aggregate or select fewer columns before the join or aggregation) or give the query more slots (a larger reservation, since shuffle quota scales with slots).
:::

::: resources
- Docs: [Query plan and timeline](https://docs.cloud.google.com/bigquery/docs/query-plan-explanation) · every stage, step and statistic explained
- Docs: [Get query performance insights](https://docs.cloud.google.com/bigquery/docs/query-insights) · slot contention, shuffle quota, input scale change and partition skew
- Docs: [Optimize query computation](https://docs.cloud.google.com/bigquery/docs/best-practices-performance-compute) · Google's catalogue of SQL patterns to avoid and their replacements
- Repo: [BigQuery anti-pattern recognition](https://github.com/GoogleCloudPlatform/bigquery-antipattern-recognition) · a tool that scans SQL for common performance anti-patterns
- Blog: [In-memory query execution in Google BigQuery](https://cloud.google.com/blog/products/bigquery/in-memory-query-execution-in-google-bigquery) · how the shuffle tier works, which explains spill and repartitioning
- Blog: [BigQuery Admin reference guide: Query optimization](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-admin-reference-guide-query-optimization) · reading plans and fixing queries, step by step
:::

### BigQuery cost control

::: plain
Cost control in BigQuery works like managing a family credit card. You check prices before buying (dry runs), you set spending limits on each card (quotas and maximum bytes billed), you get text alerts when spending passes a threshold (budgets), and once a month you read the statement line by line to see who bought what (the jobs history). Each habit is cheap; skipping all of them is how teams get a five-figure surprise.
:::

Cost control has three layers: **prevent** expensive mistakes, **detect** spending early, and **analyse** where money goes so you can fix the causes.

**Prevent.**

- **Dry runs** estimate bytes before running. Every IDE and the console show the estimate; in code, run a dry run first and refuse to run anything above a threshold.
- **Maximum bytes billed** makes BigQuery fail a query, at no charge, if it would bill more than a limit:

```bash
bq query --use_legacy_sql=false --maximum_bytes_billed=50000000000 \
  'SELECT customer_id, SUM(amount) FROM sales.orders
   WHERE order_date >= "2026-10-01" GROUP BY customer_id'
# fails with "Query exceeded limit for bytes billed" if it would bill over 50 GB
```

```python
from google.cloud import bigquery

client = bigquery.Client()
sql = "SELECT customer_id, SUM(amount) FROM sales.orders WHERE order_date >= '2026-10-01' GROUP BY 1"
dry = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
print(f"would process {dry.total_bytes_processed / 2**30:.1f} GiB")
job = client.query(sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=50 * 10**9))
```

- **Custom quotas** cap on-demand usage per day: `QueryUsagePerDay` for a whole project and `QueryUsagePerUserPerDay` for each user. New projects start with a 200 TiB per day project limit; set your own, far lower for sandboxes. Quotas apply only to on-demand; for capacity pricing, the `autoscale_max_slots` of each reservation is your cap.
- **Table design**: `require_partition_filter`, partition and table expiration on staging data, and short time travel windows on rebuildable datasets.

**Detect.** Cloud Billing **budgets and alerts** notify you at thresholds (for example 50%, 90% and 100% of a monthly budget). Alerts do not stop spending by themselves; combine them with quotas, or connect a budget's Pub/Sub notification to automation that reacts.

**Analyse.** `INFORMATION_SCHEMA.JOBS` (per project), `JOBS_BY_USER` and `JOBS_BY_ORGANIZATION` record every job for 180 days. Typical questions:

```sql
-- the 20 most expensive on-demand queries of the last 7 days
SELECT
  user_email,
  job_id,
  ROUND(total_bytes_billed / POW(1024, 4), 3)        AS tib_billed,
  ROUND(total_bytes_billed / POW(1024, 4) * 6.25, 2) AS approx_usd,
  LEFT(query, 120)                                   AS query_start
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND job_type = 'QUERY'
  AND statement_type != 'SCRIPT'      -- scripts' child jobs already carry the bytes
  AND cache_hit IS NOT TRUE
ORDER BY total_bytes_billed DESC
LIMIT 20;

-- which repeated query shapes cost the most (literals ignored)
SELECT
  query_info.query_hashes.normalized_literals AS query_hash,
  COUNT(*)                                    AS runs,
  SUM(total_bytes_billed) / POW(1024, 4)      AS tib_billed,
  SUM(total_slot_ms) / 1000 / 3600            AS slot_hours,
  ANY_VALUE(LEFT(query, 120))                 AS example
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY' AND statement_type != 'SCRIPT'
GROUP BY query_hash
ORDER BY tib_billed DESC
LIMIT 20;
```

The second query is often the most useful one in a cost review: a single dashboard tile refreshing every 5 minutes with a full scan usually beats every analyst's ad hoc work combined.

**Attribute** costs with **labels**: set labels on jobs (`--label=team:growth` in `bq`, `labels` in client libraries, or `SET @@query_label = 'team:growth';` in a script or session) and on datasets and reservations. Labels flow into the Cloud Billing export, which you should send to BigQuery itself for dashboards. For capacity pricing, compute cost attribution (in preview at the time of writing) splits reservation charges across the projects that used them.

**Typical savings, roughly in order of payoff:**

1. Partition and cluster the largest tables, and enforce partition filters.
2. Replace full-scan dashboard queries with materialized views, aggregate tables or BI Engine.
3. Stop `SELECT *` in scheduled jobs and models (dbt and Dataform models included).
4. Move steady heavy workloads to reservations; keep ad hoc work on-demand with quotas.
5. Expire staging data, switch suitable datasets to physical storage billing, and let old partitions reach long-term rates.

> **Pitfall:** Summing `total_bytes_billed` over all jobs double-counts scripts: the parent `SCRIPT` job reports the total of its child statements, which are also listed. Filter `statement_type != 'SCRIPT'` (or count only parents).

> **Pitfall:** Estimating capacity cost from `total_slot_ms` underestimates the bill. Reservations bill allocated slots (baseline always, autoscaled slots in steps of 50 with a one-minute minimum unless fluid scaling is on), not the slot-ms your queries used. Use `RESERVATIONS_TIMELINE` or the billing export for real capacity costs.

::: deeper
**Where on-demand cost hides.** Each referenced table bills at least 10 MB, so a query joining 30 tiny tables bills at least 300 MB. Views bill their underlying tables. BigQuery ML training on-demand is billed at a higher per-TiB rate than queries for built-in model types. Remote model and AI function calls also incur charges from the model provider (Gemini Enterprise Agent Platform).

**Free operations.** Loading on the shared pool, copying tables, exporting up to 50 TiB per day on the shared pool, deleting, cached results and table previews. Queries against `INFORMATION_SCHEMA` views are billed like other queries (with the 10 MB minimum). The **BigQuery sandbox** gives a free tier without a billing account, with limits such as table expiration.

**Organization-wide views.** `JOBS_BY_ORGANIZATION` in an admin project gives a single place for FinOps dashboards. Pair it with `TABLE_STORAGE_BY_ORGANIZATION` for storage, and with the billing export for the authoritative numbers.

**Guard rails for tools.** BI tools and notebooks are the usual sources of runaway queries. Give them dedicated projects with their own quotas or reservations, so one tool cannot exhaust another team's capacity, and set `maximum_bytes_billed` in service code that generates SQL.

> **Interview:** "How would you prevent a junior analyst from accidentally spending $5,000 in one query?" Layer controls: a per-user daily quota and a project quota on the analysts' project, `require_partition_filter` on large tables, a default `maximum_bytes_billed` in shared tooling, budgets with alerts, and a weekly review of the top queries from `INFORMATION_SCHEMA.JOBS`. On capacity pricing, cap the analysts' reservation with `autoscale_max_slots` instead.
:::

::: check
?? Does a budget alert stop BigQuery from running queries when the budget is exceeded?
No. Budgets only notify. To stop spending you need quotas, `maximum_bytes_billed`, reservation caps, or automation triggered by the budget's notifications.

?? Why filter `statement_type != 'SCRIPT'` when summing bytes from `INFORMATION_SCHEMA.JOBS`?
The parent job of a multi-statement script reports the total of its child statements, which are also listed as separate jobs, so including both double-counts.

?? What does `maximum_bytes_billed` cost you if the query would exceed it?
Nothing: the query fails before running and is not billed.
:::

::: resources
- Docs: [Estimate and control costs](https://docs.cloud.google.com/bigquery/docs/best-practices-costs) · Google's checklist of cost controls for queries and storage
- Docs: [Create custom query quotas](https://docs.cloud.google.com/bigquery/docs/custom-quotas) · project and per-user daily limits for on-demand
- Docs: [JOBS view](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs) · every column you can analyse, with example queries
- Docs: [Estimate query costs](https://docs.cloud.google.com/bigquery/docs/estimate-costs) · dry runs, the pricing calculator and estimate limitations
- Blog: [BigQuery Admin reference guide: Monitoring](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-admin-reference-guide-monitoring) · building monitoring and cost dashboards from INFORMATION_SCHEMA
- Repo: [bigquery-utils](https://github.com/GoogleCloudPlatform/bigquery-utils) · Google's collection of useful scripts, views and dashboards for BigQuery administration
:::

## Features

The first three groups were about the engine: how data is stored, laid out, loaded and paid for. This group is about what you can build on it. BigQuery is far more than `SELECT`: it has a procedural language, several kinds of user-defined functions, native JSON and nested types, full-text search, machine learning and generative AI in SQL, an in-memory BI layer, and fine-grained security and sharing.

Most of these features use the same storage and slots you already understand, so their costs and limits follow the same rules. Where a feature needs a particular edition or has its own pricing, the topic says so.

### Advanced BigQuery SQL

::: plain
Plain SQL answers one question at a time. Advanced SQL lets you write a small program: remember values in variables, loop, handle errors, bundle steps into a procedure, and teach the database new functions, some written in JavaScript or Python, or even calling a web service. It also lets you work with data shaped like documents (JSON, lists inside rows) without flattening everything first.
:::

**`MERGE`** applies inserts, updates and deletes from a source to a target in one atomic statement, the core of incremental pipelines and slowly changing dimensions:

```sql
MERGE analytics.customers AS t
USING staging.customers_delta AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.is_deleted THEN DELETE
WHEN MATCHED THEN UPDATE SET email = s.email, tier = s.tier, updated_at = s.updated_at
WHEN NOT MATCHED BY TARGET THEN INSERT (customer_id, email, tier, updated_at)
  VALUES (s.customer_id, s.email, s.tier, s.updated_at);
```

Add a filter on the target's partitioning column inside the `ON` clause or a `WHEN` condition when the delta only touches recent partitions; otherwise the `MERGE` scans the whole target. Up to two mutating DML statements (`UPDATE`, `DELETE`, `MERGE`) run concurrently per table, and up to 20 more wait in a queue.

**Scripting and procedures.** A script is several statements run as one job, with variables, control flow, error handling and transactions:

```sql
DECLARE run_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
DECLARE rows_loaded INT64;

BEGIN
  BEGIN TRANSACTION;
  DELETE FROM marts.daily_sales WHERE sale_date = run_date;
  INSERT INTO marts.daily_sales
    SELECT sale_date, store_id, SUM(amount) FROM raw.sales
    WHERE sale_date = run_date GROUP BY sale_date, store_id;
  SET rows_loaded = @@row_count;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = FORMAT('daily_sales failed for %t: %s', run_date, @@error.message);
END;

SELECT FORMAT('loaded %d rows for %t', rows_loaded, run_date) AS status;
```

Wrap reusable scripts in **stored procedures** (`CREATE PROCEDURE marts.load_daily(run_date DATE) BEGIN ... END;` then `CALL marts.load_daily(DATE '2026-03-02');`). `EXECUTE IMMEDIATE` runs dynamically built SQL, useful for looping over tables; always pass values as query parameters (`USING`) rather than concatenating them. BigQuery can also run **Apache Spark stored procedures** written in PySpark, Java or Scala, billed through BigQuery.

**User-defined functions (UDFs).**

| Kind | Language | Runs | Good for |
|---|---|---|---|
| SQL UDF | GoogleSQL | Inline in the query plan | Reusable expressions; fastest |
| JavaScript UDF | JavaScript | Sandboxed in BigQuery | Logic awkward in SQL (string parsing, small algorithms) |
| Python UDF | Python 3.11 with PyPI packages | Managed containers, billed separately | Libraries you cannot rewrite in SQL; GA in 2026 |
| Remote function | Any, behind HTTPS | Cloud Run functions or Cloud Run | Calling services, private models, complex code |
| Table function (TVF) | GoogleSQL | Inline | Parameterised views |

```sql
-- SQL UDF: persistent, reusable
CREATE FUNCTION util.safe_ratio(a FLOAT64, b FLOAT64) AS (IF(b = 0, NULL, a / b));

-- Python UDF
CREATE FUNCTION util.normalize_phone(raw STRING) RETURNS STRING
LANGUAGE python
OPTIONS (runtime_version = 'python-3.11', entry_point = 'normalize',
         packages = ['phonenumbers'])
AS r'''
import phonenumbers
def normalize(raw):
    try:
        n = phonenumbers.parse(raw, "DE")
        return phonenumbers.format_number(n, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None
''';

-- Remote function backed by a Cloud Run service
CREATE FUNCTION util.risk_score(payload JSON) RETURNS FLOAT64
REMOTE WITH CONNECTION `acme-prod.eu.remote_conn`
OPTIONS (endpoint = 'https://risk-scorer-abc123-ew.a.run.app', max_batching_rows = 500);

-- Table function
CREATE TABLE FUNCTION marts.orders_for(cid INT64) AS
SELECT order_id, order_ts, amount FROM analytics.orders WHERE customer_id = cid;
SELECT * FROM marts.orders_for(42);
```

Remote functions receive rows in batches as JSON over HTTPS, so your service must be idempotent and fast. A project can run at most 10 concurrent queries that contain remote functions.

**Nested data and JSON.** Arrays and structs let one row hold a whole document. `UNNEST` turns an array into rows; `ARRAY_AGG` and `STRUCT` build them back:

```sql
SELECT o.order_id, item.sku, item.qty * item.price AS line_total
FROM analytics.orders AS o, UNNEST(o.items) AS item
WHERE item.qty > 1;

SELECT customer_id,
       ARRAY_AGG(STRUCT(order_id, order_ts) ORDER BY order_ts DESC LIMIT 3) AS last_orders
FROM analytics.orders
GROUP BY customer_id;
```

The native **`JSON` type** stores semi-structured data in a parsed, columnar-friendly form; you can address fields with dot notation and convert them with typed functions:

```sql
SELECT
  payload.user.id                    AS user_id_json,   -- JSON value
  LAX_INT64(payload.cart.item_count) AS item_count,     -- lenient conversion
  JSON_VALUE(payload, '$.device.os') AS os               -- STRING
FROM events.page_views
WHERE DATE(event_ts) = '2026-03-02';
```

Extract frequently filtered fields into real typed columns at load time; filters on typed, clustered columns prune far better than filters inside JSON.

**Search indexes** make needle-in-a-haystack lookups (an order ID, an IP address, an error code in logs) cheap on large text data. The index is maintained automatically; the `SEARCH` function uses it:

```sql
CREATE SEARCH INDEX app_logs_idx ON logs.app_logs (ALL COLUMNS)
OPTIONS (analyzer = 'LOG_ANALYZER');

SELECT log_ts, service, message
FROM logs.app_logs
WHERE SEARCH(app_logs, 'tx-7f3a9c')
  AND log_date >= '2026-03-01';
```

**Pipe syntax** (GA) writes a query as a top-down sequence of operations, which many people find easier to read and to build up step by step:

```sql
FROM analytics.orders
|> WHERE order_ts >= TIMESTAMP '2026-01-01'
|> AGGREGATE SUM(amount) AS revenue GROUP BY customer_id
|> ORDER BY revenue DESC
|> LIMIT 10;
```

> **Pitfall:** A JavaScript or Python UDF in the `WHERE` clause of a large query runs once per row and can dominate slot time. Use SQL expressions where you can, apply cheap filters before the UDF, and note that queries calling Python UDFs are not cached.

> **Tip:** `QUALIFY` filters on window functions without a subquery: `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1` is the standard deduplication idiom.

::: deeper
**Transactions.** Multi-statement transactions use snapshot isolation across tables in the same location. Concurrent transactions that modify the same table conflict, and the later one fails; design pipelines so each table has one writer at a time. DDL is limited inside transactions.

**DML internals.** DML in BigQuery rewrites the affected storage blocks rather than single rows, so many small updates are expensive and large batched ones are efficient. On partitioned tables, touching fewer partitions means rewriting fewer blocks. There is no daily limit on DML statements, but there are concurrency limits (two mutating statements running per table) and a rate at which `INSERT` statements queue.

**Search index details.** Indexes have text analyzers (`LOG_ANALYZER` for log-like text, `NO_OP_ANALYZER` for exact values, `PATTERN_ANALYZER` for custom tokenizing), can cover selected columns, and are built by background jobs that use either a shared pool (within an organization-wide size limit) or your `BACKGROUND` reservation. Search acceleration requires Enterprise, Enterprise Plus or on-demand; on Standard, `SEARCH` works without index acceleration.

**JSON and change history.** `JSON_FLATTEN` (2025) extracts nested values; the `APPENDS` and `CHANGES` table functions return rows appended or changed in a time range, enabling incremental processing without your own watermark columns.

**GoogleSQL is open source** (formerly ZetaSQL), so parsers and analyzers in tools such as linters can match BigQuery's grammar exactly.

> **Interview:** "Implement an SCD Type 2 dimension in BigQuery." A `MERGE` that, for changed keys, updates the current row's `valid_to` and `is_current`, and inserts a new current row. Because one `MERGE` cannot both update and insert for the same matched source row, the common trick is to union the delta with itself (one copy keyed for the update, one with a `NULL` merge key so it falls into `WHEN NOT MATCHED` and inserts), all in one atomic statement.
:::

::: check
?? Why can a `MERGE` into a large partitioned table be expensive even when the delta is tiny, and how do you fix it?
Without a filter on the target's partitioning column, BigQuery must scan the whole target to find matches. Add a partition predicate (for example on the date range present in the delta) to the `ON` clause or `WHEN` conditions.

?? When would you choose a remote function over a Python UDF?
When the logic must call private services or networks, needs a runtime or resources Python UDFs do not offer, or already exists as a service on Cloud Run; the cost is network latency per batch and a concurrency limit of 10 queries per project.

?? What does `UNNEST` do in `FROM analytics.orders AS o, UNNEST(o.items) AS item`?
It turns each order's `items` array into rows and joins them to their parent order row (a correlated cross join), so each output row is one item with its order's columns.
:::

::: resources
- Docs: [DML statements in GoogleSQL](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax) · `INSERT`, `UPDATE`, `DELETE` and `MERGE` with examples and limits
- Docs: [Procedural language](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language) · variables, loops, exceptions and transactions in scripts
- Docs: [User-defined functions](https://docs.cloud.google.com/bigquery/docs/user-defined-functions) · SQL and JavaScript UDFs, persistent and temporary
- Docs: [Work with remote functions](https://docs.cloud.google.com/bigquery/docs/remote-functions) · the request format, connections and limits
- Docs: [Working with JSON data](https://docs.cloud.google.com/bigquery/docs/json-data) · the JSON type, field access and conversion functions
- Docs: [Introduction to search in BigQuery](https://docs.cloud.google.com/bigquery/docs/search-intro) · search indexes, analyzers and when they help
- Paper: [SQL Has Problems. We Can Fix Them: Pipe Syntax In SQL](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/) · the design rationale behind BigQuery's pipe syntax
:::

### BigQuery ML and Gemini in BigQuery

::: plain
BigQuery ML lets you train and use machine learning models with SQL, right where the data already sits, instead of exporting data to a separate ML system. The generative AI functions go further: you can ask a large language model a question about every row of a table as part of a query, for example "is this review about a delivery problem?", and get the answer back as a column. Gemini in BigQuery is the assistant built into the console that helps you write and explain that SQL.
:::

**BigQuery ML (BQML)** trains models with `CREATE MODEL` and serves them with SQL functions. Models are objects in datasets, like tables. A churn model end to end:

```sql
CREATE OR REPLACE MODEL ml.churn_model
TRANSFORM (
  ML.STANDARD_SCALER(days_since_last_order) OVER () AS days_since_last_order,
  plan, country, support_tickets_90d, churned
)
OPTIONS (model_type = 'BOOSTED_TREE_CLASSIFIER', input_label_cols = ['churned'])
AS
SELECT days_since_last_order, plan, country, support_tickets_90d, churned
FROM ml.customer_features
WHERE snapshot_date < '2026-09-01';

SELECT * FROM ML.EVALUATE(MODEL ml.churn_model,
  (SELECT * FROM ml.customer_features WHERE snapshot_date >= '2026-09-01'));

SELECT customer_id, predicted_churned, predicted_churned_probs
FROM ML.PREDICT(MODEL ml.churn_model,
  (SELECT * FROM ml.customer_features WHERE snapshot_date = CURRENT_DATE()));
```

The `TRANSFORM` clause stores preprocessing inside the model, so prediction applies exactly the same scaling as training. Built-in model types include linear and logistic regression, boosted trees (XGBoost), random forests, deep neural networks, k-means, matrix factorization, PCA, autoencoders, ARIMA_PLUS time series and contribution analysis. `ML.EXPLAIN_PREDICT` and `ML.GLOBAL_EXPLAIN` give feature attributions. Models can be exported, or registered in **Gemini Enterprise Agent Platform** (formerly Vertex AI) for online serving.

**Forecasting without training.** `AI.FORECAST` uses Google's pre-trained **TimesFM** foundation model, so a forecast is one function call:

```sql
SELECT *
FROM AI.FORECAST(
  (SELECT day, store_id, revenue FROM marts.daily_store_revenue),
  data_col => 'revenue',
  timestamp_col => 'day',
  id_cols => ['store_id'],
  horizon => 28);
```

**Generative AI functions** send each row to a Gemini model through Gemini Enterprise Agent Platform and return the answer as a column. Two families exist:

- **General-purpose functions** give full control over model, prompt and parameters: `AI.GENERATE` (scalar; can return a `STRUCT` that follows a schema you give), `AI.GENERATE_TABLE` (a table with your schema), `AI.GENERATE_BOOL`, `AI.GENERATE_INT`, `AI.GENERATE_DOUBLE`, and `AI.GENERATE_TEXT` over a remote model.
- **Managed AI functions** have a short syntax and let BigQuery choose prompts and models: `AI.IF` (filter or join on a condition in natural language), `AI.SCORE` (rank), `AI.CLASSIFY` (assign categories) and `AI.AGG` (summarise many rows). Some are still in preview; check the status before relying on them in production.

```sql
-- structured extraction: one call per row, typed output
SELECT ticket_id,
       AI.GENERATE(('Extract the product and the problem from this support ticket: ', body),
                   output_schema => 'product STRING, problem STRING, urgent BOOL') AS extracted
FROM support.tickets
WHERE created_date = CURRENT_DATE();

-- managed functions: filter and classify with plain-language conditions
SELECT review_id, AI.CLASSIFY(review_text, categories => ['delivery', 'quality', 'price', 'other']) AS topic
FROM shop.reviews
WHERE AI.IF(('This review describes a damaged or late delivery: ', review_text))
  AND review_date >= '2026-11-01';
```

The functions also accept images, audio, video and PDFs stored in Cloud Storage, referenced through **object tables** or `ObjectRef` values, so you can classify product photos or extract fields from invoices in SQL.

**Embeddings and vector search.** An embedding is a vector of numbers that captures meaning; similar texts get nearby vectors. BigQuery generates them with a remote embedding model and searches them with `VECTOR_SEARCH`, optionally accelerated by a **vector index** (approximate nearest neighbours, either `IVF` or `TREE_AH`, the latter based on Google's ScaNN):

```sql
CREATE OR REPLACE TABLE kb.doc_embeddings AS
SELECT * FROM AI.GENERATE_EMBEDDING(
  MODEL kb.text_embedding_model,               -- remote model over an embedding endpoint
  (SELECT doc_id, body AS content FROM kb.documents));   -- adds an `embedding` column

CREATE VECTOR INDEX doc_idx ON kb.doc_embeddings (embedding)
OPTIONS (index_type = 'IVF', distance_type = 'COSINE');

SELECT base.doc_id, distance
FROM VECTOR_SEARCH(
  TABLE kb.doc_embeddings, 'embedding',
  (SELECT embedding FROM AI.GENERATE_EMBEDDING(MODEL kb.text_embedding_model,
     (SELECT 'How do I rotate a service account key?' AS content))),
  top_k => 5, distance_type => 'COSINE');
```

This is the retrieval half of retrieval-augmented generation (RAG) inside the warehouse. Newer options automate it: **autonomous embedding generation** keeps an embedding column up to date as source text changes, and `AI.SEARCH` and AI indexes (preview) hide the embedding steps entirely.

**Gemini in BigQuery** is the AI assistance in BigQuery Studio: SQL and Python generation and completion, explanations of existing queries, data canvas (a visual, prompt-driven exploration surface), data insights (suggested questions with generated queries), data preparation, and conversational analytics agents that answer business questions over chosen tables. It is a productivity layer; review generated SQL like any other code.

**Costs and editions.** BQML needs Enterprise, Enterprise Plus or on-demand (not Standard). On-demand training of built-in models is billed per TiB processed at a higher rate than ordinary queries ($312.50 per TiB for many model types at the time of writing); on reservations, training uses your slots. Generative AI and embedding calls are additionally billed by Gemini Enterprise Agent Platform per token or character; managed functions can run in an **optimized mode** that trains a small distilled model on a sample to cut tokens on large tables.

> **Pitfall:** An `AI.GENERATE` over a 50-million-row table is 50 million model calls. Always develop on a `LIMIT`ed sample, estimate tokens with `AI.COUNT_TOKENS`, filter with cheap SQL first, and store results in a table instead of recomputing them in a view.

::: deeper
**Remote models.** `CREATE MODEL ... REMOTE WITH CONNECTION ... OPTIONS (ENDPOINT = '...')` registers a hosted model (Gemini, partner models such as Anthropic Claude, Mistral and Llama, or open models you deploy) as a BigQuery model, so `AI.GENERATE_TEXT` and `AI.GENERATE_EMBEDDING` can call it. Calls go through a Cloud resource connection whose service account needs permission on Agent Platform, or use end-user credentials where supported.

**Reliability.** Model calls can fail per row (quota, safety filters, timeouts). General-purpose functions return a status column; check it and retry failed rows in a follow-up query rather than rerunning everything. Results are not deterministic, so persist them.

**Model quality.** BQML supports hyperparameter tuning (`NUM_TRIALS`), data split options and evaluation functions (`ML.EVALUATE`, `ML.ROC_CURVE`, `ML.CONFUSION_MATRIX`). For anything beyond tabular baselines, teams usually prototype in BQML and move to custom training on Agent Platform with features served from BigQuery.

**Vector index trade-offs.** `IVF` clusters vectors with k-means and searches the nearest clusters; `TREE_AH` uses ScaNN-style quantisation and suits large batch searches. Indexes are approximate: recall drops a little in exchange for speed. Very small tables are not indexed, and `VECTOR_INDEX.STATISTICS` tells you when data has drifted enough to rebuild.

> **Interview:** "Why do ML inside the warehouse at all?" It removes data movement and a second governance surface, lets analysts build baselines in SQL, and keeps predictions next to the data that BI tools read. The limits are model variety, fine control of training and low-latency online serving, which is why mature teams combine BQML for batch scoring with a dedicated ML platform for online models.
:::

::: check
?? What is the purpose of the `TRANSFORM` clause in `CREATE MODEL`?
It stores preprocessing (scaling, bucketing, feature crosses) inside the model, so `ML.PREDICT` applies exactly the same transformations to new data automatically.

?? You need a forecast for 2,000 stores by tomorrow and have no labelled training pipeline. Which BigQuery feature fits?
`AI.FORECAST` with the pre-trained TimesFM model and `id_cols => ['store_id']`: no training step, one query for all series.

?? Why should results of `AI.GENERATE` usually be written to a table rather than computed in a view?
Every read of the view would call the model again for every row, multiplying cost and latency, and the answers can differ between runs. Persisting them makes cost one-off and results reproducible.
:::

::: resources
- Docs: [Introduction to ML in BigQuery](https://docs.cloud.google.com/bigquery/docs/bqml-introduction) · model types, workflow and where BQML fits
- Docs: [Generative AI overview](https://docs.cloud.google.com/bigquery/docs/generative-ai-overview) · general-purpose, managed and task-specific AI functions
- Docs: [Introduction to embeddings and vector search](https://docs.cloud.google.com/bigquery/docs/vector-search-intro) · embedding functions, vector indexes and hybrid search
- Docs: [Gemini in BigQuery overview](https://docs.cloud.google.com/bigquery/docs/gemini-overview) · the assistive features in BigQuery Studio
- Repo: [GoogleCloudPlatform/generative-ai](https://github.com/GoogleCloudPlatform/generative-ai) · official notebooks, including BigQuery generative AI and RAG examples
- Book: [Machine Learning Design Patterns](https://www.oreilly.com/library/view/machine-learning-design/9781098115777/) · Lakshmanan, Robinson and Munn; many examples use BigQuery ML, and the patterns outlive any API
:::

### BI Engine, Looker and Connected Sheets

::: plain
Dashboards ask the same kinds of questions over and over, and people expect answers in a blink. **BI Engine** is a fast memory cache in front of BigQuery that answers those repeated questions from RAM. **Looker** is a shared dictionary that defines what "revenue" or "active customer" means once, so every chart uses the same definition. **Connected Sheets** lets people who live in spreadsheets explore billions of rows from Google Sheets, with BigQuery doing the heavy lifting.
:::

**BI Engine** is an in-memory, vectorised analysis service. You reserve memory per project and region; BigQuery then caches the columns and partitions your queries use most and runs eligible parts of queries against that memory. It works through the normal BigQuery API, so every tool benefits: Looker, Data Studio, Tableau, Power BI, custom apps over JDBC/ODBC and the console.

```sql
-- reserve 50 GiB in the US for this project and accelerate only two tables
ALTER BI_CAPACITY `acme-dashboards.region-us.default`
SET OPTIONS (
  size_gb = 50,
  preferred_tables = ['acme-prod.marts.daily_sales', 'acme-prod.marts.dim_store']
);
```

Key behaviours:

- **Preferred tables** restrict acceleration to a list, so ad hoc traffic does not evict your dashboard data. A query is accelerated only if *all* its tables are preferred; materialized views are accelerated only if both the view and its base tables are listed.
- BI Engine works best on **pre-joined or pre-aggregated** tables and star schemas with small dimensions. Pair it with materialized views or aggregate tables built by your pipeline.
- It does not accelerate wildcard tables, external tables, tables with row-level security, non-SQL UDFs or `JSON` columns; such queries fall back to normal slots (fully or partly).
- It is billed per GiB-hour of reserved memory ($0.0416 per GiB-hour in the US at the time of writing, roughly $30 per GiB per month), and it requires Enterprise, Enterprise Plus or on-demand; it is not available with Standard edition reservations.
- `INFORMATION_SCHEMA.JOBS` reports `bi_engine_statistics` (whether a query was fully, partly or not accelerated, and why), and `INFORMATION_SCHEMA.BI_CAPACITIES` shows reservations.

**Looker** is Google Cloud's enterprise BI platform, built around a semantic layer written in **LookML**. You describe tables as *views* with *dimensions* and *measures*, and join them into *explores*; Looker generates BigQuery SQL from users' clicks, so a metric is defined once and reused everywhere.

```text
view: orders {
  sql_table_name: `acme-prod.marts.orders` ;;

  dimension: order_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.order_id ;;
  }
  dimension_group: ordered {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.order_ts ;;
  }
  measure: revenue {
    type: sum
    sql: ${TABLE}.amount ;;
    value_format_name: usd
  }
}

explore: orders {}
```

LookML lives in Git, goes through code review and has development and production modes, which brings analytics engineering practice to BI. Looker can build **persistent derived tables** (PDTs) in BigQuery and use **aggregate awareness** to route queries to smaller pre-aggregated tables automatically. **Data Studio** (renamed from Looker Studio in April 2026) is the free, self-service dashboarding tool, with a native BigQuery connector; it suits lighter reporting without a governed semantic layer.

**Connected Sheets** connects Google Sheets to BigQuery. Users build pivot tables (up to 200,000 rows of output), charts, formulas and extracts over tables of any size; each operation runs as a BigQuery query under the user's identity, billed to a chosen project, and refreshes can be scheduled. It brings spreadsheet users onto governed warehouse data instead of CSV exports, and it can run TimesFM forecasts and anomaly detection directly from a sheet.

**A typical BI architecture on BigQuery:**

| Layer | Tool | Purpose |
|---|---|---|
| Curated marts | BigQuery tables built by dbt or Dataform | Clean star schemas and aggregates |
| Acceleration | Materialized views, BI Engine, cached results | Sub-second, low-cost repeated queries |
| Semantic layer | Looker (LookML) | One definition per metric, governed access |
| Consumption | Looker dashboards, Data Studio, Connected Sheets, other BI tools | Self-service analysis |

> **Pitfall:** Pointing a dashboard with 20 tiles at raw event tables, refreshed every few minutes by many viewers, is the classic BigQuery cost explosion. Build aggregate tables or materialized views for the dashboard grain, accelerate them with BI Engine, and set sensible cache and refresh intervals in the BI tool.

::: deeper
**How BI Engine executes.** BigQuery splits a query into subqueries; BI Engine is most effective at leaf stages that scan, filter and aggregate cached columns, and joins with a large fact and small dimensions. Unsupported operations run in BigQuery's regular engine and the job reports partial acceleration. Disable the query cache when benchmarking, or cached results will hide the effect.

**Sizing.** Start with the compressed size of the columns and partitions your dashboards actually touch (often a small fraction of the tables), add headroom, and watch eviction and acceleration rates in Cloud Monitoring. If memory is short, BI Engine evicts the least recently used partitions and columns.

**Looker and cost.** Every Looker query is a BigQuery job billed to the connection's project. Use datagroups to control caching, PDTs or aggregate awareness for heavy explores, and a dedicated project or reservation so BI load cannot starve pipelines. Labels on Looker's jobs make its share visible in `INFORMATION_SCHEMA.JOBS`.

**Semantic layers elsewhere.** dbt's semantic layer and other metric layers solve the same problem as LookML; whichever you use, define each metric once, version it in Git, and generate SQL rather than copying it between dashboards.

> **Interview:** "Dashboards on BigQuery are slow and expensive. What do you do?" Measure first in `INFORMATION_SCHEMA.JOBS` (which dashboards, which queries, bytes and slot-ms). Then: build aggregates at the dashboard grain (tables or materialized views), partition and cluster them on the dashboard filters, accelerate with BI Engine using preferred tables, tune BI-tool caching, and move BI traffic to its own reservation or project with a quota.
:::

::: check
?? A dashboard query joins a preferred table with a table that is not on the preferred list. Is it accelerated by BI Engine?
No. A query is accelerated only if all the tables it reads are preferred tables; this one runs on regular slots.

?? What problem does LookML solve that a collection of saved SQL queries does not?
It defines dimensions, measures and joins once in a version-controlled semantic model, so every dashboard and user computes "revenue" the same way, and SQL is generated rather than copied.

?? Who pays for the queries behind a Connected Sheets pivot table?
The billing project chosen for the connection, running under the user's identity; each refresh or pivot is a BigQuery query.
:::

::: resources
- Docs: [Introduction to BI Engine](https://docs.cloud.google.com/bigquery/docs/bi-engine-intro) · architecture, preferred tables, limitations and sizing
- Docs: [What is LookML?](https://docs.cloud.google.com/looker/docs/what-is-lookml) · the building blocks of Looker's semantic layer
- Docs: [Looker documentation](https://docs.cloud.google.com/looker/docs/intro) · administration, modelling and BigQuery connections
- Docs: [Using Connected Sheets](https://docs.cloud.google.com/bigquery/docs/connected-sheets) · setup, permissions, pivot limits and scheduled refresh
- Docs: [Analyze data with Data Studio](https://docs.cloud.google.com/bigquery/docs/visualize-looker-studio) · connecting Data Studio (formerly Looker Studio) to BigQuery
- Book: [The Data Warehouse Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) · Kimball and Ross; the star schemas that make BI Engine and Looker fast
:::

### BigQuery security and governance

::: plain
Think of a bank vault with several kinds of keys. One key lets you into the building (permission to run queries). Another opens a particular room (a dataset or table). Inside a room, some boxes are locked for everyone except a few people (column-level security), some drawers only show you your own branch's papers (row-level security), and some documents have their account numbers blacked out unless you have special clearance (data masking). When you lend papers to another bank, you hand over a viewing window, not the originals (sharing).
:::

**IAM basics.** BigQuery separates *running jobs* from *reading data*:

| Role | Typical grant level | Allows |
|---|---|---|
| `roles/bigquery.jobUser` | Project | Run jobs (queries, loads) billed to this project |
| `roles/bigquery.user` | Project | Run jobs, list datasets, create datasets |
| `roles/bigquery.dataViewer` | Dataset or table | Read data and metadata |
| `roles/bigquery.dataEditor` | Dataset or table | Read and modify data, create tables |
| `roles/bigquery.dataOwner` | Dataset | Full control, including sharing and row access policies |
| `roles/bigquery.admin` | Project | Everything, including reservations |

An analyst typically gets `jobUser` on a compute project and `dataViewer` on the datasets they need. Grant to **groups**, not individuals, and keep service accounts for pipelines separate from people. IAM **conditions** with tags and IAM **deny policies** (GA) add guard rails, for example denying deletes on production datasets to everyone except a pipeline account.

**Row-level security** filters rows per user or group with **row access policies**:

```sql
CREATE ROW ACCESS POLICY eu_sales ON marts.sales
GRANT TO ('group:sales-eu@acme.com')
FILTER USING (region = 'EU');

CREATE ROW ACCESS POLICY all_rows ON marts.sales
GRANT TO ('group:finance@acme.com', 'serviceAccount:etl@acme-prod.iam.gserviceaccount.com')
FILTER USING (TRUE);
```

As soon as one policy exists on a table, users not covered by any policy see **no rows**, so service accounts and teams that need everything require a `TRUE` policy. A user in several policies sees the union of their rows. Policies can also use `SESSION_USER()` to match rows to the querying user.

**Column-level security** uses **policy tags**: you create a **taxonomy** (for example `PII > High > ssn`), tag columns with it, enforce access control on the taxonomy, and grant the **Fine-Grained Reader** role (`roles/datacatalog.categoryFineGrainedReader`) on a tag to those allowed to read it. Others get an access error if they select the column (they can still run `SELECT * EXCEPT (ssn)`). Policy tags are managed by the Policy Tag Manager API, which still uses the `datacatalog` API name and roles; unlike the rest of Data Catalog, it is **not** deprecated. A newer alternative, **data governance tags** (preview), uses Resource Manager tags instead of taxonomies.

```json
[
  {"name": "customer_id", "type": "INT64"},
  {"name": "email", "type": "STRING",
   "policyTags": {"names": ["projects/acme-gov/locations/eu/taxonomies/1234/policyTags/5678"]}}
]
```

**Dynamic data masking** builds on column-level security: instead of denying access, it returns a masked value. You create a **data policy** with a masking rule and principals, attach it to a policy tag (or, since 2026, directly to a column), and grant the **Masked Reader** role (`roles/bigquerydatapolicy.maskedReader`). Rules include **nullify**, **default masking value** (0, empty string and so on by type), **hash (SHA-256)**, **random hash** (salted per query), **email mask**, **first four characters**, **last four characters**, **date year mask** and a **custom masking routine** (your own SQL UDF). Users with Fine-Grained Reader see real values, Masked Readers see masked values, everyone else is denied. Because masking happens before any other query operation, masked users cannot recover values by filtering or joining (except that deterministic hashes can still be joined, which is their purpose).

**Encryption and perimeter.**

- All data is encrypted at rest. With **CMEK** you protect datasets and tables with your own Cloud KMS key; disabling the key makes the data unreadable. For column-level encryption, the **AEAD** SQL functions encrypt individual values with keysets, which enables crypto-shredding (delete a customer's key to make their data unreadable).
- **VPC Service Controls** put BigQuery inside a service perimeter so data cannot be copied to projects outside it, even by users with valid credentials.
- **Cloud Audit Logs** record admin activity and (if enabled) data access; together with `INFORMATION_SCHEMA.JOBS` they answer "who read this table?".

**Sharing.** **BigQuery sharing** (formerly Analytics Hub) publishes datasets as **listings** in **exchanges**. A subscriber gets a **linked dataset**: a read-only pointer in their own project, with no copy and no storage cost to them, always current. Publishers can enable **egress controls** that block copying, exporting and snapshotting. **Data clean rooms** add **analysis rules** (such as aggregation thresholds, differential privacy and list overlap) and **query templates**, so partners can join their data with yours and see only aggregated, privacy-preserving results.

**Governance around BigQuery.** **Knowledge Catalog** (formerly Dataplex Universal Catalog) catalogs BigQuery assets, records lineage, profiles data and runs data quality scans; **Sensitive Data Protection** discovers which columns hold PII so you know what to tag. The section on governance later in the course covers these in depth.

> **Pitfall:** Granting `dataViewer` at the project level gives read access to every dataset in the project, including ones created later. Grant at the dataset level and use separate projects for different sensitivity levels.

> **Pitfall:** Row access policies apply to every access path, not just interactive queries. Service accounts that copy, export or otherwise need the full table must be covered by a `TRUE` policy, or they lose access to the rows like any other user.

::: deeper
**Editions and fine-grained security.** Row-level security, column-level security and masking are not available on Standard edition reservations; queries from a Standard reservation against protected tables fail. Plan editions with security in mind.

**Performance side effects.** Tables with row access policies cannot be accelerated by BI Engine, and the `job_stages` column in `INFORMATION_SCHEMA.JOBS` is empty for queries that read them (to avoid leaking filtered data through statistics). Filter predicates in policies should be simple and use clustered columns so they prune well.

**Authorized views versus row-level security.** Authorized views give a curated projection and work everywhere, but multiply objects (one view per audience). Row access policies keep one table and one set of queries, and are enforced for every access path. Large organizations usually combine both: row policies for tenant or region isolation, authorized views or shared datasets for curated external interfaces.

**Hashing is not anonymisation.** SHA-256 masking is deterministic and unkeyed, so low-entropy values (phone numbers, national IDs) can be brute-forced by hashing all possible inputs. Use random hash, nullify or keyed encryption (AEAD) for such columns, and treat masked data as still personal data under GDPR.

> **Interview:** "Analysts in three countries must see only their country's customers, and only the support team may see full email addresses." A row access policy per country group on the customer table (plus a `TRUE` policy for pipelines and finance); a policy tag on `email` with a masking data policy using the email mask for analysts (Masked Reader) and Fine-Grained Reader for support; all grants to groups; the project on Enterprise edition or higher; audit logs on.
:::

::: check
?? A new row access policy is created on a table that had none. What happens to a user who is not named in any policy?
They see zero rows: once a table has row access policies, access is denied by default to anyone not covered by a policy.

?? What is the difference between Fine-Grained Reader and Masked Reader on a policy-tagged column?
Fine-Grained Reader sees the real values; Masked Reader sees the values transformed by the data policy's masking rule; users with neither are denied access to the column.

?? A partner subscribes to your BigQuery sharing listing. Who pays to store the data, and is it copied?
You (the publisher) keep paying for storage; nothing is copied. The subscriber gets a read-only linked dataset and pays only for the queries they run.
:::

::: resources
- Docs: [Access control with IAM](https://docs.cloud.google.com/bigquery/docs/access-control) · every predefined role and permission, and where to grant them
- Docs: [Introduction to row-level security](https://docs.cloud.google.com/bigquery/docs/row-level-security-intro) · policies, the TRUE filter and interactions with other features
- Docs: [Introduction to column-level access control](https://docs.cloud.google.com/bigquery/docs/column-level-security-intro) · taxonomies, policy tags and enforcement
- Docs: [Introduction to data masking](https://docs.cloud.google.com/bigquery/docs/column-data-masking-intro) · masking rules, roles and how they interact
- Docs: [Introduction to BigQuery sharing](https://docs.cloud.google.com/bigquery/docs/analytics-hub-introduction) · exchanges, listings, linked datasets and egress controls
- Docs: [Share sensitive data with data clean rooms](https://docs.cloud.google.com/bigquery/docs/data-clean-rooms) · analysis rules and query templates for partner collaboration
- Blog: [BigQuery Admin reference guide: Data governance](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-admin-reference-guide-data-governance) · how the security features combine in a real setup
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Estimate a query's cost | `bq query --dry_run --use_legacy_sql=false '...'` |
| Hard cap on one query | `--maximum_bytes_billed=50000000000` (fails free if exceeded) |
| Cap daily on-demand spend | Custom quotas `QueryUsagePerDay` and `QueryUsagePerUserPerDay` |
| Cheaper reads | Name columns, never `SELECT *`; `LIMIT` does not cut bytes |
| Large fact table layout | `PARTITION BY DATE(ts) CLUSTER BY k1, k2` plus `require_partition_filter = TRUE` |
| Partition limits | 10,000 per table; 4,000 modified per job |
| Storage billing switch | `ALTER SCHEMA ds SET OPTIONS (storage_billing_model = 'PHYSICAL')` when compression beats about 1.7x |
| Query the past | `FROM t FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)` |
| Cheap dev copy of prod | `CREATE TABLE dev.t CLONE prod.t` |
| Batch ingestion | `bq load` or `LOAD DATA`; free on the shared pool; 1,500 modifications per table per day |
| Streaming ingestion | Storage Write API (gRPC): default stream at-least-once, committed stream with offsets for exactly-once |
| Open-format table written by BigQuery | `CREATE TABLE ... WITH CONNECTION ... OPTIONS (table_format = 'ICEBERG', ...)` |
| Capacity for a team | `CREATE RESERVATION` with `edition`, `slot_capacity`, `autoscale_max_slots`, then `CREATE ASSIGNMENT` |
| Slow query triage | Wait high: slots. Max much greater than avg: skew. Records out much greater than in: join explosion. Spill: shuffle |
| Most expensive queries | `INFORMATION_SCHEMA.JOBS` ordered by `total_bytes_billed`, excluding `SCRIPT` parents |
| Deduplicate rows | `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1` |
| Upsert | `MERGE ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` |
| Row filter per group | `CREATE ROW ACCESS POLICY ... GRANT TO (...) FILTER USING (...)` |
| Hide a column's values | Policy tag plus a masking data policy and the Masked Reader role |
| Speed up dashboards | Aggregates or materialized views, then `ALTER BI_CAPACITY ... preferred_tables` |

### Practice

::: try
**1. Pick a layout.** A `clickstream` table receives 400 GB per day, is kept for 3 years, and nearly every query filters on a date range and often on `user_id` or `page_type`. Write the `CREATE TABLE` options you would use and explain each choice, including any limit you are avoiding.
:::

?? A solution to 1

```sql
CREATE TABLE web.clickstream (
  event_ts TIMESTAMP NOT NULL, user_id INT64, page_type STRING, url STRING, props JSON
)
PARTITION BY DATE(event_ts)              -- ~400 GB per partition, ~1,100 partitions (< 10,000)
CLUSTER BY user_id, page_type            -- high-cardinality filters inside each day
OPTIONS (
  require_partition_filter = TRUE,       -- no accidental 3-year scans
  partition_expiration_days = 1095       -- drop data after 3 years automatically
);
```

Hourly partitions would exceed 10,000 partitions within about 14 months, so daily is right. Old partitions become long-term storage automatically because they are no longer modified.

::: try
**2. On-demand or reservation?** A project scanned 800 TiB last month. Its jobs used a total of 1,080,000,000,000 slot-milliseconds (`SUM(total_slot_ms)`). Estimate the on-demand bill and the cost of the same slot usage at Enterprise pay-as-you-go ($0.06 per slot-hour). What else must you check before switching?
:::

?? A solution to 2
On-demand: (800 - 1) TiB x $6.25 = $4,993.75. Slot usage: 1,080,000,000,000 ms / 1,000 / 3,600 = 300,000 slot-hours, x $0.06 = $18,000. Here on-demand is far cheaper: the workload is compute-heavy relative to the bytes it scans. Before switching you would also check that autoscaling allocation (multiples of 50, one-minute minimum) is higher than raw usage, which makes slots even more expensive here, and whether any feature you need (for example BigQuery ML or masking) rules out Standard edition.

::: try
**3. Read the plan.** A nightly join's slowest stage shows `recordsRead` of 2 billion and `recordsWritten` of 90 billion, `computeMsAvg` of 3 seconds and `computeMsMax` of 400 seconds. Name the two problems and how you would fix each.
:::

?? Answer to 3
Writing 45 times more rows than were read means a **join explosion**: the join key is not unique on at least one side (duplicates multiply rows). Deduplicate or aggregate the inputs before joining, or fix the join condition. The gap between average and maximum compute time is **skew**: one key (often `NULL` or a default value) holds a large share of the rows. Find it with a `GROUP BY key ORDER BY COUNT(*) DESC` query, then filter it out, handle it separately, or salt it.

::: try
**4. Secure a table.** The `marts.customers` table has `country`, `email` and `lifetime_value`. Analysts in the `analysts-de@acme.com` group may see only German customers and must not see real emails; the `support@acme.com` group may see all rows and real emails; the ETL service account needs everything. Describe the objects and grants.
:::

?? A solution to 4

```sql
CREATE ROW ACCESS POLICY de_only ON marts.customers
GRANT TO ('group:analysts-de@acme.com')
FILTER USING (country = 'DE');

CREATE ROW ACCESS POLICY everything ON marts.customers
GRANT TO ('group:support@acme.com',
          'serviceAccount:etl@acme-prod.iam.gserviceaccount.com')
FILTER USING (TRUE);
```

Then tag `email` with a policy tag from a PII taxonomy and enforce access control on the taxonomy. Create a data policy on that tag with the email mask rule and give the analysts group the Masked Reader role on it; give the support group and the ETL service account Fine-Grained Reader on the tag. Grant `dataViewer` on the dataset (not the project) to all three, and make sure the queries run on an Enterprise or Enterprise Plus reservation or on-demand, since Standard edition does not support these controls.

### What comes next

You now know BigQuery from its storage blocks to its security model: how to lay data out, get it in, pay for it, tune it and expose it safely. The next section, **Pipelines on Google Cloud**, builds the moving parts around it: Pub/Sub and managed Kafka for events, Dataflow and Managed Service for Apache Spark for processing, Datastream for change data capture, Dataform for SQL pipelines inside BigQuery, and Managed Service for Apache Airflow (formerly Cloud Composer) and Workflows to orchestrate them all.
