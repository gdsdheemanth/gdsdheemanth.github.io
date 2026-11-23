This section explains how analytical data warehouses are built, how they store and process data, and how you run one well. It is written for data engineers and analytics engineers who already load data into a warehouse and now need to understand why some queries take two seconds and others two hours, why one design costs ten times more than another, and how the main engines differ. It assumes Data engineering foundations (OLTP versus OLAP, the raw, staging and marts idea), SQL for data engineering (joins, `GROUP BY`, window functions, `MERGE`) and Data modeling (facts, dimensions, grain and slowly changing dimensions). Dimensional design itself lives in [Data modeling](#modeling) and BigQuery specifics live in [BigQuery in depth](#bigquery); this section links to them instead of repeating them. Examples use BigQuery GoogleSQL unless a topic is about another engine, and every Python example runs with the standard library only.

**What you will be able to do afterwards**

- Explain where data warehousing came from, what Kimball and Inmon disagreed about, and how a modern warehouse is split into layers and zones.
- Describe how massively parallel processing (MPP) and columnar storage make large scans fast, and what data skew does to them.
- Compare Snowflake, ClickHouse, DuckDB and MotherDuck with BigQuery: their architecture, their strengths and when each is the right tool.
- Speed up queries with zone maps, bitmap and bloom filter indexes, partitioning, clustering, materialized views and an understanding of the query optimizer.
- Serve consistent numbers through a semantic layer, and answer multi-dimensional questions with roll-up, drill-down, slice, dice and pivot.
- Test, automate and run a warehouse that many teams or customers share, including tenant isolation and cost attribution.

**The big picture.** A data warehouse is a database built for reading large amounts of history, not for recording individual transactions. Three ideas explain almost everything about how one behaves. First, **store by column**: analytical queries touch a few columns of many rows, so a warehouse keeps each column separately, encoded and compressed, and reads only the columns a query names. Second, **work in parallel**: the data is split across many workers that scan, filter and aggregate their share at the same time, then exchange partial results. Third, **skip what you can**: statistics about each block of data (minimum, maximum, distinct values) let the engine avoid reading blocks that cannot match, so the way data is sorted and partitioned decides how much is read. Every engine in this section (BigQuery, Snowflake, ClickHouse, DuckDB) applies the same three ideas with different trade-offs: where storage lives, who owns the compute, and how much you control the physical layout.

The section has four groups. Concepts and architecture covers history, layers, MPP, columnar storage and HTAP. Engines covers Snowflake, ClickHouse, DuckDB and MotherDuck. Performance covers indexing, partitioning, materialized views, the optimizer, the semantic layer and OLAP operations. Running a warehouse covers testing, automation and multi-tenancy.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Concepts and architecture

Before tuning a warehouse you need to know what one is for and how it is put together. This group starts with history, because the arguments of the 1990s (one integrated enterprise model or many business-process stars?) still shape how teams organise warehouses today. It then describes the layers data passes through on its way from source systems to dashboards.

The last three topics open the machine. MPP explains how work is split across many nodes and why one hot key can slow everything down. Columnar storage explains why reading three columns of a 200-column table is cheap. HTAP looks at systems that try to serve transactions and analytics from one database, and at when that is a better choice than a separate warehouse.

### Data Warehouse Concepts & History

::: plain
A data warehouse is the company's long-term memory for analysis. The shop tills, the website and the accounting system each remember only what they need to run today, in their own format. The warehouse copies their records into one place, cleans them, keeps history that the source systems throw away, and arranges it so that questions like "how did sales in each region change over five years?" can be answered quickly. Think of a library that collects the daily newspapers of every town, files them consistently and keeps them forever, so researchers do not have to visit each town.
:::

**Why warehouses exist.** Operational systems (OLTP: online transaction processing) are designed to record events fast and correctly: insert an order, update a stock level. They keep current state, use normalised schemas, and must answer in milliseconds. Analytical questions are the opposite: they read millions of rows, join data from several systems, and need history. Running them on the operational database slows the business down and still gives poor answers, because the data is scattered and the history is gone. A **data warehouse** is a separate database, loaded from the sources, for analysis (OLAP: online analytical processing).

Bill Inmon's classic definition (1992) names four properties, and they still describe a good warehouse:

| Property | Meaning | Example |
|---|---|---|
| Subject-oriented | Organised around business subjects, not applications | "Customer" and "Sales", not "CRM tables" and "till tables" |
| Integrated | Codes, units and keys made consistent across sources | One country code list, one currency, one customer key |
| Time-variant | Keeps history and records when things were true | A customer's segment in 2024 and in 2026 |
| Non-volatile | Loaded and read, not edited in place by users | Corrections arrive as new loads, not manual updates |

**A short history.**

1. **1980s: the idea.** Barry Devlin and Paul Murphy at IBM described a "business data warehouse" in 1988: one integrated store fed from operational systems. Teradata, founded in 1979, built some of the first parallel database machines for large decision-support workloads.
2. **1990s: two schools.** Inmon's *Building the Data Warehouse* (1992) and Ralph Kimball's *The Data Warehouse Toolkit* (1996) defined the two main design approaches described below. ETL tools (extract, transform, load) and OLAP cube servers grew up around them.
3. **2000s: appliances and column stores.** MPP appliances (Netezza, Greenplum, Teradata) and columnar databases (Vertica, which came out of the C-Store research project) made scans of billions of rows routine, but they were expensive boxes sized for peak load.
4. **2010s: cloud warehouses.** BigQuery (public in 2011), Snowflake (generally available in 2015) and others separated storage from compute and charged per use. Loading became cheap, so the pattern moved from ETL to **ELT**: load raw data first, transform it inside the warehouse with SQL, which tools such as dbt and Dataform organise.
5. **2020s: lakehouses and open formats.** Open table formats (Iceberg, Delta, Hudi) brought warehouse features to files in object storage, and warehouses learned to read and write those formats. The next section, Data lakes and lakehouses, covers this.

**Inmon versus Kimball.** Both build a warehouse; they disagree about the order and the shape.

- **Inmon (top-down).** First build an integrated, normalised (third normal form) **enterprise data warehouse** covering the whole business, the "Corporate Information Factory". Then derive **data marts** (smaller, often dimensional, per department) from it. Strength: one integrated, consistent core. Weakness: a long, expensive project before anyone sees a report.
- **Kimball (bottom-up).** Build the warehouse one **business process** at a time (orders, then shipments, then returns) as **star schemas**: fact tables surrounded by dimension tables. Consistency comes from **conformed dimensions** (the same customer and date dimensions shared by every star), planned with the **bus matrix**, a grid of business processes against the dimensions they use. Strength: value arrives in weeks. Weakness: without discipline, stars drift apart and conformance is lost.

The design techniques of the Kimball school (grain, conformed dimensions, slowly changing dimensions) are covered in [Dimensional Modeling (Kimball)](#modeling), and the hub-link-satellite alternative in Data Vault 2.0 Modeling in the same section.

**What most teams do today** is a hybrid. Raw data lands unchanged; a cleaned, integrated layer (sometimes normalised, sometimes Data Vault) sits in the middle; and Kimball-style stars or wide tables serve analysts at the top. Cheap storage and compute mean that "build the integrated core first" no longer blocks value for months, and dimensional marts remain the easiest shape for people and BI tools to query. The next topic describes those layers.

> **Pitfall:** Treating "Kimball versus Inmon" as a choice of tool or vendor. It is a choice of design order and where integration happens. You can implement either in BigQuery, Snowflake or a lakehouse.

> **Tip:** Write the bus matrix even for a small warehouse: rows are business processes, columns are dimensions, and a tick means the process uses the dimension. It shows in one page which dimensions must be conformed and which process to build next.

::: deeper
**Data warehouse versus data mart versus operational data store.** A **data mart** is a subject-specific subset (finance, marketing). In Inmon's design marts are derived from the warehouse ("dependent" marts); in Kimball's the warehouse *is* the set of conformed marts. An **operational data store** (ODS) is an integrated, near-current copy of operational data for operational reporting, with little history; today its role is often played by change data capture into the raw layer.

**What changed with the cloud.** Classic warehouses were sized for the peak (month-end close) and idle the rest of the time, so modeling was driven by saving space and pre-computing aggregates. Separating storage from compute removed most of that pressure: you can keep raw history cheaply, rebuild marts from scratch, and scale compute for the peak only. What did not change is the need for integration (one definition of customer) and history, which is why the design ideas outlived the hardware.

**Logical warehouse and federation.** Some platforms query data where it lives (federated queries into operational databases, external tables over files) instead of copying it. In BigQuery, `EXTERNAL_QUERY` reads Cloud SQL, AlloyDB and Spanner, and external and Iceberg tables read files in Cloud Storage. Federation is useful for small or rarely used data, but heavy, repeated analysis usually still benefits from loading into the warehouse's own optimised storage.

> **Interview:** "Kimball or Inmon?" A good answer explains both, then says that most modern platforms combine them: an integrated, history-keeping core (normalised or Data Vault) for robustness, and Kimball dimensional marts for consumption, with conformed dimensions as the glue.

> **Interview:** "What is the difference between ETL and ELT, and why did ELT win in the cloud?" ETL transforms data in a separate engine before loading; ELT loads raw data and transforms it inside the warehouse. Cloud warehouses made storage cheap and compute elastic, so keeping raw data and transforming with SQL became simpler, more auditable and easier to rerun.
:::

::: check
?? Name Inmon's four properties of a data warehouse.
Subject-oriented, integrated, time-variant and non-volatile.

?? What makes separate Kimball star schemas add up to one warehouse instead of disconnected marts?
Conformed dimensions: the same dimension tables (customer, product, date) shared by every star, planned with the bus matrix.

?? Why did ELT replace ETL as the default pattern in cloud warehouses?
Storage became cheap and compute elastic, so it became practical to load raw data first and transform it with SQL inside the warehouse, keeping raw history and making transformations easy to rerun and audit.
:::

::: resources
- Article: [Kimball Group design tips and resources](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/) · the archive of Kimball design tips, articles and the technique glossary
- Book: [The Data Warehouse Toolkit, 3rd edition](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) · Kimball and Ross; the reference for dimensional design and the bus matrix
- Book: *Building the Data Warehouse* by W. H. Inmon · the original top-down, enterprise data warehouse approach and the four defining properties
- Article: [Differences of opinion](https://www.kimballgroup.com/2004/03/differences-of-opinion/) · Kimball Group's own comparison of the Kimball and Inmon approaches
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley; places the warehouse in the wider data engineering lifecycle
- Docs: [What is a data warehouse?](https://cloud.google.com/learn/what-is-a-data-warehouse) · Google Cloud's short overview of warehouse concepts and cloud warehouses
:::

### Data Warehouse Layers & Zones

::: plain
Data in a warehouse moves through a series of rooms, like food through a restaurant kitchen. Deliveries arrive at the loading dock and are kept exactly as delivered. They are then unpacked and checked, washed and cut, combined into dishes, and finally plated for the customer. Each room has its own rules and its own staff, and if a dish tastes wrong you can walk back through the rooms to find where it went wrong. Warehouse layers are those rooms.
:::

[[fig:warehousing_layers]]

A **layer** (or **zone**) is a stage in the path from source systems to consumers, with a clear contract: what data it holds, how clean it is, who may read it, and how long it is kept. Names vary by team and vendor, but the stages are the same:

| Layer | Also called | What it holds | Typical rules |
|---|---|---|---|
| Landing | inbox, drop zone | Files or messages exactly as received | Short retention; no transformation; often in Cloud Storage |
| Raw | bronze, source | Loaded into tables, still in source shape, plus load metadata | Append-only; keep full history; never edited |
| Staging | cleansed, silver (first half) | One model per source table: renamed, typed, deduplicated | No joins across sources; light, mechanical cleaning |
| Integrated / intermediate | silver (second half), core | Sources joined, keys mapped, business rules applied | Reusable building blocks; not for end users |
| Curated / marts | gold, presentation | Facts and dimensions, wide tables, aggregates | Documented, tested, owned; what analysts query |
| Semantic / consumption | metrics layer, serving | Metric definitions, views and caches for tools | Stable names and definitions; access control |

The Databricks term **medallion architecture** names three of these (bronze, silver, gold). dbt's project conventions name them **staging**, **intermediate** and **marts**. They are the same idea with different words.

**Why layers help.**

- **Reprocessing.** Because raw keeps everything as received, a bug in a later layer can be fixed and the layers above rebuilt, without asking the source system for the data again.
- **Separation of concerns.** Staging handles source quirks (a column called `cust_nm`, dates stored as text). Marts handle business meaning. A change to a source touches one staging model, not fifty reports.
- **Access control.** Raw data often contains personal data in the clear; marts can expose only what consumers need. Granting by layer is simpler than granting table by table.
- **Debugging and lineage.** When a number is wrong, you compare layer by layer to find where it changed.

**A concrete layout on BigQuery.** A common pattern is one dataset per layer per domain (or per layer, for small warehouses), with IAM granted at dataset level:

```sql
-- Raw: source shape, append-only, with load metadata
CREATE TABLE raw_shop.orders (
  payload      JSON,             -- the record exactly as received
  _source_file STRING,
  _loaded_at   TIMESTAMP NOT NULL
)
PARTITION BY DATE(_loaded_at);

-- Staging: one clean row per source row, typed and renamed, latest version only
CREATE OR REPLACE VIEW stg_shop.orders AS
SELECT
  CAST(JSON_VALUE(payload, '$.id') AS INT64)              AS order_id,
  CAST(JSON_VALUE(payload, '$.cust') AS INT64)            AS customer_id,
  CAST(JSON_VALUE(payload, '$.total') AS NUMERIC)         AS order_total,
  TIMESTAMP(JSON_VALUE(payload, '$.created'))             AS created_at,
  _loaded_at
FROM raw_shop.orders
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.id')
                           ORDER BY _loaded_at DESC) = 1;
```

Marts are then built from staging (and intermediate models) as tables, usually by dbt or Dataform, and given descriptions, owners and tests. Analysts are granted `roles/bigquery.dataViewer` on mart datasets only; pipelines have write access to the layers they build.

**Retention differs by layer.** Landing files can expire after a few days once loaded (a Cloud Storage lifecycle rule). Raw is usually kept longest, because it is the replay source, but personal data may force limits; partition expiration handles that. Staging can be views (no storage at all). Marts are rebuilt from below and need only as much history as consumers ask for.

> **Pitfall:** Letting analysts and dashboards read staging or raw tables "just this once". Every report built on a lower layer bypasses the tests and definitions of the marts, and breaks the next time a source changes. Make the curated layer the only one consumers can see.

> **Pitfall:** Too many layers. Five hops for a simple source add latency, cost and confusion. Each layer must have a reason (a contract that differs from its neighbours); merge the ones that do not.

> **Tip:** Name objects so the layer is obvious: `raw_`, `stg_`, `int_`, `fct_`, `dim_` prefixes (or one dataset per layer). Anyone reading a query can then see at a glance whether it reads governed data.

::: deeper
**Layers and data contracts.** The boundary between source teams and the raw layer, and between marts and consumers, are the two places where a **data contract** (schema, meaning, freshness, owner) pays off most. Contracts are covered in [Data Contract Modeling](#modeling).

**Where the semantic layer fits.** The top layer is not always tables. It can be a set of governed views, a BI model (LookML in Looker) or a metrics layer (dbt Semantic Layer, Cube) that turns "revenue by month" into SQL against the marts. The topic Semantic Layer & Metrics Layer later in this section covers it.

**Zones in a lakehouse.** On a lakehouse the same layers are usually separate prefixes or buckets in Cloud Storage holding Iceberg or Delta tables, and the catalog (for example the Lakehouse runtime catalog, formerly BigLake metastore) registers them. Knowledge Catalog (formerly Dataplex Universal Catalog) can attach owners, quality scans and business glossary terms to each layer's assets.

**Streaming through layers.** For near-real-time data the layers stay but get thinner: events land in raw within seconds (Pub/Sub to BigQuery subscriptions, or the Storage Write API), staging is a view, and marts are incremental models or materialized views refreshed every few minutes. The trade-off is that every extra layer adds latency, so streaming paths often skip the intermediate layer.

> **Interview:** "Why keep a raw layer if you already have clean staging tables?" Because raw is the only copy of what the source actually sent. It lets you replay history after a bug, audit disputes ("what did the CRM say on 3 March?") and add new derived fields to old data, without depending on the source system keeping history.
:::

::: check
?? What is the main rule of the raw layer, and why?
Keep data exactly as received, append-only, with load metadata. It is the replay and audit source: later layers can be fixed and rebuilt from it without re-extracting from sources.

?? A dashboard reads a staging table directly. What can go wrong?
It bypasses the curated layer's tests, business rules and documented definitions, and it breaks or silently changes when the source or staging model changes.

?? How do the medallion names map to the layers in this topic?
Bronze is raw, silver covers staging and integrated models, and gold covers curated marts.
:::

::: resources
- Docs: [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview) · dbt Labs' guide to staging, intermediate and marts layers and their naming
- Article: [Medallion architecture](https://www.databricks.com/glossary/medallion-architecture) · Databricks' description of bronze, silver and gold layers
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley; the storage and transformation chapters discuss layering and its trade-offs
- Docs: [Introduction to BigQuery datasets](https://cloud.google.com/bigquery/docs/datasets-intro) · datasets as the unit of location, access control and default expiration
- Docs: [Cloud Storage object lifecycle management](https://cloud.google.com/storage/docs/lifecycle) · expiring landing-zone files automatically after they are loaded
:::

### MPP Architecture

::: plain
Imagine counting the votes of a whole country. One person counting alone would take years. Instead, every polling station counts its own ballots at the same time and phones in a total, and a central office adds up the totals. That is massively parallel processing: split the data into many pieces, give each piece to a separate worker with its own resources, let them all work at once, and combine the partial results. It is only as fast as the slowest polling station, which is why an unevenly split load is the main enemy.
:::

[[fig:warehousing_mpp]]

**MPP** (massively parallel processing) databases spread both data and work across many **nodes**, each with its own CPU, memory and (traditionally) disk. Nearly every analytical warehouse is MPP in some form.

**Three ways to share hardware.**

| Architecture | What is shared | Examples | Trade-off |
|---|---|---|---|
| Shared-everything | CPU, memory and disk on one machine | a single PostgreSQL or DuckDB server | Simple, but limited to one machine |
| Shared-disk | Nodes share storage, have private CPU and memory | Oracle RAC; cloud warehouses over object storage | Any node can read any data; storage bandwidth is the limit |
| Shared-nothing | Nothing: each node owns a slice of the data | Teradata, Greenplum, Vertica, ClickHouse clusters | Scales almost linearly; data placement matters a lot |

Classic MPP warehouses are **shared-nothing**: each node stores a partition of every large table on local disks and processes only that partition. Modern cloud warehouses (BigQuery, Snowflake) are a hybrid: data lives in shared, durable storage (Colossus or object storage), and compute nodes cache and process slices of it. That keeps the parallelism of shared-nothing while letting compute grow, shrink or restart without moving data.

**Leader and workers.** An MPP query has two roles:

1. A **leader** (also called coordinator, master or root) receives the SQL, parses and optimises it, and splits the plan into **fragments** (stages) that can run in parallel.
2. **Workers** (compute nodes, segments, slots) each run the fragments on their share of the data: scan, filter, partial aggregate.
3. When an operation needs rows with the same key on the same worker (a join or a `GROUP BY`), workers **exchange** data over the network: the **shuffle** or **redistribution** step.
4. Final results flow back to the leader, which merges them and returns the answer.

**How data is distributed.** In shared-nothing systems you choose a **distribution** for each table:

- **Hash distribution** on a column: `node = hash(customer_id) mod N`. Rows with the same key are always on the same node, so joins and aggregations on that key need no network exchange. Greenplum writes it as `DISTRIBUTED BY (customer_id)`.
- **Round-robin (random)**: rows are dealt out evenly. Perfect balance, but every join needs a shuffle.
- **Replicated (broadcast)**: a full copy of a small table on every node, so joins against it are always local. Good for small dimensions.

**Joins in parallel.** For `orders JOIN customers ON customer_id`, the planner has three options: if both tables are already hash-distributed on `customer_id`, join locally (**co-located join**); if one side is small, **broadcast** it to every node; otherwise **redistribute** (shuffle) one or both sides by the join key. Shuffles move data across the network and are often the most expensive part of a query.

**Data skew.** Parallel work finishes when the *slowest* worker finishes. If one key holds a large share of the rows, the node that owns it does most of the work while the others wait. A small simulation shows it:

```python
import zlib
from collections import Counter

NODES = 4

def node_for(key: str) -> int:
    return zlib.crc32(key.encode()) % NODES   # stable hash (str hash() changes per run)

# 10,000 orders: 600 ordinary customers, plus one customer with 4,000 orders
orders = [f"c{i % 600}" for i in range(6000)] + ["c_big"] * 4000

by_hash = Counter(node_for(k) for k in orders)
print(sorted(by_hash.items()))
print(f"skew: busiest node has {max(by_hash.values()) / (len(orders) / NODES):.2f}x the average")

# Fix: salt the hot key into 8 sub-keys so its rows spread over the nodes
salted = [f"{k}#{i % 8}" if k == "c_big" else k for i, k in enumerate(orders)]
by_salt = Counter(node_for(k) for k in salted)
print(sorted(by_salt.items()))
print(f"skew after salting: {max(by_salt.values()) / (len(orders) / NODES):.2f}x")
# [(0, 1490), (1, 1510), (2, 1490), (3, 5510)]
# skew: busiest node has 2.20x the average
# [(0, 2490), (1, 2510), (2, 2490), (3, 2510)]
# skew after salting: 1.00x
```

Node 3 does 2.2 times the average work, so the query takes about 2.2 times longer than a balanced one, however many nodes you add. **Salting** splits the hot key into sub-keys; for a join, the other side's matching rows must then be copied once per salt value. Choosing a distribution key with many distinct, evenly spread values (and no huge `NULL` or "unknown" group) avoids most skew.

> **Pitfall:** Distributing a table on a column with few distinct values (country, status) or with a dominant default value (`-1`, `NULL`, "guest"). All those rows go to one node, which becomes the bottleneck for every query on the table.

> **Tip:** In a serverless warehouse you do not choose distributions, but skew still exists inside joins and aggregations. In BigQuery, a stage where the maximum worker time is far above the average is the sign; [Reading BigQuery query plans and tuning queries](#bigquery) shows how to spot and fix it.

::: deeper
**Why shared-nothing scales.** With no shared resource to fight over, adding nodes adds scan bandwidth and CPU in proportion, the argument of Stonebraker's 1986 paper "The Case for Shared Nothing" and of DeWitt and Gray's 1992 survey of parallel databases. The limits are the parts that are not parallel: planning on the leader, the final merge, and exchanges whose cost grows with data moved (Amdahl's law). That is why MPP systems are excellent at large scans and poor at thousands of tiny queries per second.

**Resizing.** In a shared-nothing cluster, adding nodes means redistributing data (rehashing every table), which is slow and was a major operational pain of appliances. Consistent hashing and many small virtual partitions per node reduce the amount moved. Disaggregated designs avoid the problem: storage stays put and new compute nodes simply read their share.

**Exchange operators.** Inside the plan, shuffles appear as **exchange** (or **motion**) operators: *redistribute motion* (hash by key), *broadcast motion* (copy to all) and *gather motion* (send to the leader). Reading them in an `EXPLAIN` tells you where data crosses the network. Spark calls the same step an exchange and writes shuffle files; the [Shuffle internals](pyspark-streaming-kafka-topic-map.html#shuffle) section of the PySpark guide goes deep on it.

**Today's open-source MPP.** Greenplum (PostgreSQL-based) became closed source in 2024; its community continues as Apache Cloudberry, an incubating project. ClickHouse, StarRocks and Apache Doris are shared-nothing MPP engines, and Trino is an MPP query engine without its own storage.

> **Interview:** "A join that used to take 2 minutes now takes 40, and the data only grew 10%. What do you check?" Skew first: a new hot key (a bot account, a default value) that sends most rows to one worker. Look at per-worker or per-stage maximum versus average time and row counts, find the key with `GROUP BY key ORDER BY COUNT(*) DESC`, then filter it, handle it separately, or salt it.
:::

::: check
?? What is the difference between shared-nothing and shared-disk architectures?
In shared-nothing each node owns its slice of the data on its own disks; in shared-disk all nodes can read the same storage. Cloud warehouses combine shared durable storage with shared-nothing style parallel processing.

?? Which three join strategies can an MPP planner choose for a join between two large tables?
A co-located join if both are already distributed on the join key, a broadcast of the smaller side to every node, or a redistribution (shuffle) of one or both sides by the join key.

?? Why does adding more nodes not fix a skewed query?
The hot key's rows all go to one node, so that node's work does not shrink as nodes are added; the query still waits for it.
:::

::: resources
- Blog: [BigQuery under the hood](https://cloud.google.com/blog/products/bigquery/bigquery-under-the-hood) · how a serverless MPP warehouse splits work across Dremel workers and shuffle
- Paper: [The Case for Shared Nothing](https://dsf.berkeley.edu/papers/hpts85-nothing.pdf) · Stonebraker's short 1986 argument for shared-nothing parallel databases
- Paper: [Parallel Database Systems: The Future of High Performance Database Systems](https://dl.acm.org/doi/10.1145/129888.129894) · DeWitt and Gray's classic survey of partitioning, pipelining and parallel joins
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; the partitioning and batch processing chapters explain distribution and skew
- Docs: [Apache Cloudberry documentation](https://cloudberry.apache.org/docs/) · an open-source MPP database with explicit distribution keys and motion operators
- Course: [CMU 15-721 Advanced Database Systems](https://15721.courses.cs.cmu.edu/) · Andy Pavlo's lectures on parallel execution, joins and modern analytical engines
:::

### Columnar Storage Internals

::: plain
Picture a spreadsheet of a million orders with fifty columns. A row store keeps it the way you read a page: all fifty values of order 1, then all fifty of order 2. A column store cuts the sheet into fifty vertical strips and files each strip separately. To answer "total revenue by month" you pull out only the date and amount strips and leave the other forty-eight in the drawer. And because a strip holds values of one kind, often repeated ("paid, paid, paid"), it can be squeezed into much less space.
:::

**Row versus column layout.** A **row-oriented** store (PostgreSQL, MySQL) writes all columns of a row together, which is ideal for "fetch or update order 42". A **column-oriented** store writes each column separately, which is ideal for "scan three columns of a billion rows". A query that reads 3 of 50 equally sized columns reads about 6% of the data in a column store and 100% in a row store, before compression makes the gap even larger.

**How a columnar file or table is organised.** The details differ (Parquet and ORC files, BigQuery's Capacitor, Snowflake micro-partitions, ClickHouse parts), but the structure is the same:

1. Rows are grouped into large **row groups** (or blocks, stripes, granules), typically tens of thousands to millions of rows.
2. Within a row group, each column is stored as a separate **column chunk**, split into **pages**.
3. Each chunk is first **encoded** (exploiting the type and order of values), then **compressed** with a general-purpose codec.
4. **Statistics** (min, max, null count, sometimes distinct count) are kept for each chunk or page in metadata, so readers can skip data without decoding it.

**Encodings.** They turn typed values into fewer bits:

| Encoding | Idea | Works best on |
|---|---|---|
| Dictionary | Store each distinct value once; replace values by small integer codes | Low to medium cardinality strings: status, country |
| Run-length (RLE) | Store "value, repeat count" for runs of equal values | Sorted or slowly changing columns |
| Bit-packing | Store integers in only as many bits as the largest needs | Dictionary codes, small integers |
| Delta | Store differences between consecutive values | Timestamps, increasing IDs |
| Frame of reference | Store values as offsets from a block minimum | Numbers clustered in a narrow range |

```python
col = ["paid", "shipped", "cancelled", "paid"] * 250_000   # 1,000,000 values

def runs(values):
    """Run-length encode: list of (value, count)."""
    out = []
    for v in values:
        if out and out[-1][0] == v:
            out[-1][1] += 1
        else:
            out.append([v, 1])
    return out

raw_bytes = sum(len(v) for v in col)
dictionary = sorted(set(col))                  # ['cancelled', 'paid', 'shipped']
codes = [dictionary.index(v) for v in col[:8]]
print(raw_bytes, dictionary, codes)            # 6000000 ['cancelled', 'paid', 'shipped'] [1, 2, 0, 1, 1, 2, 0, 1]
bits = (len(dictionary) - 1).bit_length()      # 2 bits are enough for 3 codes
print(f"dictionary + bit-packing: {len(col) * bits // 8:,} bytes")   # 250,000 bytes
print(f"runs in load order: {len(runs(col)):,}")                     # 750,001
print(f"runs after sorting: {len(runs(sorted(col)))}")               # 3
```

Six million bytes of text become 250,000 bytes of 2-bit codes (plus a tiny dictionary), a 24-fold saving before any compression. RLE does nothing in load order (750,001 runs, because neighbours differ) but collapses the sorted column to three runs. That is why sort order is a physical design decision: it decides how well every encoding and every min/max statistic works. Encodings are covered in depth in [Data Encoding Techniques](#hardware) and the Parquet layout in the Parquet internals topic of [Data lakes and lakehouses](#lakehouse).

**Compression codecs** then squeeze the encoded bytes. **Snappy** and **LZ4** are very fast with moderate ratios; **Zstd** gives better ratios at similar decompression speed and is the common default for new Parquet data; **gzip** compresses well but decompresses slowly. Warehouses choose for you; in files you choose per column chunk. Compression algorithms are explained in [Compression Algorithms Deep Dive](#hardware).

**Predicate pushdown and pruning.** The reader pushes the `WHERE` clause down to the storage layer. For `WHERE order_date = '2026-11-20'`, it checks each row group's min and max for `order_date` and skips groups whose range excludes the value. Only the surviving groups are read, and only for the referenced columns. Pruning works well when data is sorted or clustered on the filtered column; on a randomly ordered column every group's min and max span nearly the whole range, and nothing is skipped (the Indexing topic in the Performance group simulates this).

**Vectorized execution.** Column stores process data in **batches** (vectors) of a few thousand values of one column at a time, rather than one row at a time. Tight loops over arrays of the same type use the CPU cache well, avoid per-row function calls and let the compiler use SIMD instructions. Some operations run directly on encoded data: a filter on a dictionary-encoded column compares small integer codes, and `SUM` over an RLE run is `value × count`.

**Late materialization.** The engine evaluates filters column by column, keeping a list (or bitmap) of matching row positions, and only fetches the other columns for rows that survive. A selective filter therefore avoids decoding most of the other columns at all.

> **Pitfall:** `SELECT *` on a wide columnar table. It reads and decodes every column, which removes the main benefit of the format and, in BigQuery on-demand pricing, bills every column.

> **Pitfall:** Many tiny files or row groups. Each has fixed metadata and open costs, and small row groups compress poorly and carry less useful statistics. Aim for files of hundreds of megabytes and row groups in the range your engine recommends.

::: deeper
**Where the ideas came from.** MonetDB (1990s) stored every column as a separate array; the MonetDB/X100 paper (CIDR 2005) introduced vectorized execution, which became VectorWise and influenced DuckDB, Snowflake and ClickHouse. C-Store (VLDB 2005) combined column storage, compression and sorted projections, and became Vertica. Google's Dremel paper (2010) showed how to store nested, repeated records in columns using repetition and definition levels, the idea Parquet adopted. Abadi, Boncz and colleagues' survey "The Design and Implementation of Modern Column-Oriented Database Systems" ties them together.

**Writes are the weak side.** Updating one row means rewriting compressed column chunks. Column stores therefore batch writes: new rows go to a write-optimised buffer or delta store (a row store, an LSM-style set of small parts, or a streaming buffer) and are merged into columnar form in the background. ClickHouse's merges, Snowflake's immutable micro-partitions and BigQuery's streaming storage are all variations on this.

**Nested data.** Columnar formats store a `STRUCT` as one column per leaf field, and arrays with repetition levels, so `SELECT order.customer.city` reads one leaf column, not the whole record. This is why BigQuery recommends nested and repeated fields over wide joins for data always read together.

**Choosing the sort order.** A good primary sort column is one that many queries filter on and that has moderate cardinality (date, tenant, event type). Sorting by a unique ID compresses other columns poorly and helps only point lookups. Multi-column orders favour the first column; Z-order and Hilbert curves balance several, see [Data Skipping, Z-Ordering & Liquid Clustering](#lakehouse).

> **Interview:** "Why are column stores fast for analytics?" Fewer bytes read (only referenced columns, skipped blocks), much better compression (similar values together, type-specific encodings), and faster CPU work (vectorized loops, operations on encoded data, late materialization). The price is slower single-row writes and updates.
:::

::: check
?? A query reads 4 columns of a 100-column table. Roughly what fraction of the data does a column store read, before compression and pruning?
About 4%, assuming similar column sizes; a row store would read all of it.

?? Why does sorting a column before writing make run-length encoding and min/max pruning more effective?
Sorting puts equal values next to each other, creating long runs for RLE, and gives each block a narrow min and max range, so filters can skip more blocks.

?? What is late materialization?
Evaluating filters first on the filter columns, tracking which row positions match, and only then reading and decoding the other columns for the matching rows.
:::

::: resources
- Paper: [Dremel: Interactive Analysis of Web-Scale Datasets](https://research.google/pubs/pub36632/) · Google's paper on columnar storage of nested data and serving-tree execution
- Paper: [The Design and Implementation of Modern Column-Oriented Database Systems](https://stratos.seas.harvard.edu/files/stratos/files/columnstoresfntdbs.pdf) · Abadi, Boncz, Harizopoulos, Idreos and Madden; the standard survey of column-store techniques
- Paper: [MonetDB/X100: Hyper-Pipelining Query Execution](https://www.cidrdb.org/cidr2005/papers/P19.pdf) · the paper that introduced vectorized execution
- Docs: [Apache Parquet file format](https://parquet.apache.org/docs/file-format/) · row groups, column chunks, pages, encodings and statistics
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; chapter 3 compares row and column storage and compression
:::

### HTAP — Hybrid Transactional/Analytical

::: plain
Normally a business runs two kitchens: a fast-food counter that serves individual orders in seconds (the transactional database) and a separate catering kitchen that cooks in bulk overnight (the warehouse), with a truck carrying ingredients between them. HTAP tries to run both from one kitchen, with one set of ingredients, so the bulk cooking always uses today's stock and no truck is needed. It works if the kitchen can keep the bulk cooking from getting in the way of the counter.
:::

**HTAP** (hybrid transactional/analytical processing) means one database system that runs OLTP transactions and analytical queries on the same, fresh data. The usual alternative is two systems: an operational database plus a warehouse fed by batch ETL or change data capture (CDC), with minutes to hours of delay.

**Why it is hard.** The two workloads want opposite things:

| Concern | OLTP | OLAP |
|---|---|---|
| Access pattern | Read or write a few rows by key | Scan millions of rows, few columns |
| Best layout | Row store with B-tree indexes | Column store with compression |
| Concurrency | Thousands of short transactions | A few long, heavy queries |
| Main risk | Latency spikes | Running out of memory or CPU |

A heavy analytical scan on the same CPUs and buffers as checkout transactions can slow checkout down. So every HTAP design has two parts: a way to keep a **columnar copy** of the data fresh, and a way to **isolate** analytical work from transactional work.

**Main designs.**

- **Row store plus in-memory column store, one node.** The database keeps the row tables as the source of truth and maintains columnar copies of selected tables or columns in memory. The optimizer sends scans to the columns and point lookups to the rows. **AlloyDB for PostgreSQL**'s **columnar engine** works this way: it can choose columns to load automatically based on the workload, and analytical queries on read pool instances keep the load away from the primary. Oracle Database In-Memory and SQL Server columnstore indexes are other examples.
- **Separate columnar replicas.** **TiDB** stores data in TiKV (a row store replicated with Raft) and can add **TiFlash** replicas: columnar copies that receive the same Raft log as non-voting learners, so they are consistent and fresh, and run on separate machines. The optimizer chooses TiKV or TiFlash per query, or per part of a query.
- **Unified storage.** **SingleStore** stores tables in a columnstore with a row-oriented in-memory segment for recent writes and secondary indexes, so one table serves both point lookups and scans.
- **Isolated compute over the same storage.** **Spanner Data Boost** runs analytical queries (including federated queries from BigQuery) on separate, Google-managed compute that reads Spanner's storage directly, so they do not use the instance's provisioned capacity.

An example of the AlloyDB columnar engine, configured with database flags and SQL:

```sql
-- After enabling the google_columnar_engine.enabled flag on the instance
SELECT google_columnar_engine_add('orders');   -- load a table into the column store

-- This scan can now run on columnar data; EXPLAIN shows a Custom Scan (columnar scan)
EXPLAIN (ANALYZE)
SELECT region, SUM(total) FROM orders
WHERE created_at >= now() - interval '1 day'
GROUP BY region;
```

**HTAP versus warehouse plus CDC.** HTAP gives seconds-fresh analytics without a pipeline, but only over the data that lives in that one database. A warehouse integrates many sources, keeps history the application deletes, and scales analytics separately. On Google Cloud the common middle path is **Datastream** CDC from Cloud SQL, AlloyDB or other databases into BigQuery, which gives low-latency replication (often seconds to minutes) while keeping the warehouse as the integrated store.

| Need | Better fit |
|---|---|
| Live operational dashboards over one application's data | HTAP (AlloyDB columnar engine, TiDB with TiFlash) |
| Analytics across many sources, long history | Warehouse fed by CDC (Datastream into BigQuery) |
| Occasional analysis of Spanner data from BigQuery | Federated query with Data Boost |
| Customer-facing analytics at high concurrency | Real-time OLAP engine (see the Real-time OLAP engines section) |

> **Pitfall:** Expecting HTAP to replace the warehouse. It answers "what is happening now in this application?" very well; it does not integrate your CRM, billing and web events, and it does not keep the history your application overwrites.

> **Pitfall:** Running analytics on the primary of an HTAP database without workload isolation. Use the read replicas, separate columnar nodes or resource groups the product provides, or transactional latency will suffer at the worst moment.

::: deeper
**Freshness and consistency.** In TiDB, a query that reads TiFlash waits until the learner replica has applied the Raft log up to the query's read timestamp, so analytical reads are consistent snapshots, not "eventually" fresh. In-memory column stores keep a delta of recent changes and merge it into the columnar form in the background; a query combines both. The engineering problem is the same as in any column store: absorb small writes cheaply, then reorganise.

**Research lineage.** HyPer (Kemper and Neumann, ICDE 2011) showed one main-memory system running both workloads, using virtual-memory snapshots for analytical queries. Gartner coined the term HTAP in 2014. The TiDB paper (VLDB 2020) describes the Raft learner approach.

**Postgres extensions.** Several projects add columnar analytics inside PostgreSQL itself, for example `pg_duckdb`, which embeds DuckDB's engine to run analytical queries from Postgres and read Parquet and Iceberg files. They bring fast scans to Postgres users but share the server's resources with the transactional workload.

**Zero-ETL.** "Zero-ETL" integrations replicate an operational database into an analytical store automatically. They are managed CDC, not HTAP: still two systems, but you do not run the pipeline.

> **Interview:** "Product wants live dashboards on order data. HTAP or warehouse?" Ask about scope and latency. If it is one application's data and seconds of freshness matter, an HTAP feature (columnar engine, columnar replicas) or a read replica may be enough. If it combines sources or needs history, stream CDC into the warehouse and accept a short delay; if many external users will hit it, consider a real-time OLAP engine.
:::

::: check
?? Why can't you simply run heavy analytical queries on the primary of an OLTP database?
They compete for CPU, memory and I/O with short transactions, causing latency spikes, and a row store with B-tree indexes scans large tables inefficiently.

?? How does TiDB keep its TiFlash columnar replicas consistent with TiKV?
TiFlash nodes join each region's Raft group as non-voting learners and apply the same log; a read waits until the replica has caught up to the query's timestamp.

?? Name two things a warehouse fed by CDC gives you that a single HTAP database does not.
Integration of many sources, and long history (including data the application overwrites or deletes), plus analytical compute that scales separately.
:::

::: resources
- Docs: [AlloyDB columnar engine overview](https://cloud.google.com/alloydb/docs/columnar-engine/about) · how AlloyDB keeps columnar copies in memory and chooses columns automatically
- Docs: [Spanner Data Boost overview](https://cloud.google.com/spanner/docs/databoost/databoost-overview) · isolated compute for analytical and federated queries on Spanner data
- Paper: [TiDB: A Raft-based HTAP Database](https://www.vldb.org/pvldb/vol13/p3072-huang.pdf) · the VLDB 2020 paper on row and columnar replicas kept in sync with Raft
- Docs: [TiFlash overview](https://docs.pingcap.com/tidb/stable/tiflash-overview) · columnar replicas, consistency and query routing in TiDB
- Blog: [What is HTAP?](https://www.singlestore.com/blog/what-is-htap/) · SingleStore's explanation of hybrid workloads and unified storage
- Docs: [Datastream overview](https://cloud.google.com/datastream/docs/overview) · the CDC alternative: replicating operational databases into BigQuery
:::

## Engines

The first group described ideas that every analytical engine shares. This group looks at four engines that apply them in very different ways, chosen because each represents a different point in the design space. **Snowflake** is a multi-cloud managed warehouse that separates storage, compute and services, and runs on Google Cloud as well as other clouds. **ClickHouse** is an open-source, shared-nothing column store built for very fast queries on event data, where you control the physical layout. **DuckDB** is an in-process engine, the "SQLite of analytics", that runs inside your Python process or command line. **MotherDuck** turns DuckDB into a shared cloud service while still running part of each query on your laptop.

BigQuery is the fifth point and the reference throughout this course; it has its own section, [BigQuery in depth](#bigquery). Read each topic here with the question "how does this differ from BigQuery, and when would I pick it instead?" in mind. A comparison table in the Snowflake topic and the Go deeper boxes answer that question directly.

### Snowflake — Architecture & Internals

::: plain
Snowflake works like a shared office building with rentable meeting rooms. All the company's files are kept in one central archive (storage). When a team needs to work, it books a meeting room of the size it needs (a virtual warehouse), works on copies of the files it fetched, and gives the room back when done, paying only for the minutes used. A reception desk (the services layer) keeps the register of files, checks badges and decides which room does what. Many teams can work at once in separate rooms without disturbing each other, because they share only the archive.
:::

[[fig:warehousing_snowflake]]

Snowflake is a managed, multi-cloud data warehouse. It runs on Google Cloud (as well as other clouds), in the cloud and region you choose when you create an account; storage and compute are provisioned by Snowflake on that cloud. Its architecture, described in the 2016 SIGMOD paper "The Snowflake Elastic Data Warehouse", has three layers.

**1. Storage.** Tables are stored in the cloud provider's object storage (Cloud Storage on Google Cloud) as **micro-partitions**: immutable files of about 50 to 500 MB of uncompressed data, columnar inside, created automatically in the order data is loaded. For every micro-partition and column, Snowflake keeps metadata: min and max values, number of distinct values, null count. Queries use it to **prune** micro-partitions that cannot match a filter. You never manage files; you see tables.

**2. Compute: virtual warehouses.** A **virtual warehouse** is a cluster of compute nodes that runs queries. Sizes go from X-Small upward, and each size step doubles both the nodes and the credits per hour (X-Small 1 credit per hour, Small 2, Medium 4, Large 8 and so on, for standard warehouses). Warehouses are billed per second while running, with a 60-second minimum each time one starts. They cache recently read micro-partitions on local SSD, so repeated queries on the same data get faster.

```sql
CREATE WAREHOUSE transform_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60            -- seconds idle before it suspends (stops billing)
  AUTO_RESUME = TRUE           -- starts again when a query arrives
  INITIALLY_SUSPENDED = TRUE;

-- Separate warehouses isolate workloads: BI never waits for the nightly load
CREATE WAREHOUSE bi_wh WAREHOUSE_SIZE = 'SMALL'
  MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 4   -- multi-cluster: scale out for concurrency
  SCALING_POLICY = 'STANDARD' AUTO_SUSPEND = 300;
```

**Scale up versus scale out.** A bigger size makes one heavy query faster (more nodes per query). More clusters (a **multi-cluster warehouse**, Enterprise edition and above) let more queries run at once without queuing. A worked example: a transformation that takes 10 minutes on Medium (4 credits per hour) costs 4 × 10 / 60 ≈ 0.67 credits. If it parallelises well, it takes about 5 minutes on Large (8 credits per hour), which costs the same 0.67 credits but finishes twice as fast. Scaling up is only wasteful when the query does not parallelise, or when it is so short that the 60-second minimum dominates.

**3. Cloud services.** A shared, always-on layer handles authentication and access control, the metadata store (tables, micro-partition statistics), query parsing and optimisation, and transactions. It also serves three caches: the **result cache** (an identical query on unchanged data returns the stored result, for 24 hours from last use), the **metadata cache** (`COUNT(*)` or `MIN(date)` can be answered from statistics without a warehouse), and the warehouse's local disk cache mentioned above. Cloud services usage is billed only when it exceeds 10% of the day's warehouse compute.

**Time Travel.** Because micro-partitions are immutable, an update writes new micro-partitions and marks the old ones as replaced, rather than changing them. Snowflake keeps the old versions for the table's **data retention period**, so you can query or restore the past:

```sql
SELECT * FROM orders AT(OFFSET => -60 * 30);               -- as of 30 minutes ago
SELECT * FROM orders BEFORE(STATEMENT => '<query_id>');    -- just before a bad UPDATE ran
UNDROP TABLE orders;                                       -- restore a dropped table
ALTER TABLE orders SET DATA_RETENTION_TIME_IN_DAYS = 30;   -- up to 90 on Enterprise edition
```

The default retention is 1 day. After Time Travel, permanent tables enter a 7-day **Fail-safe** period, during which only Snowflake support can recover data. **Transient** and temporary tables have no Fail-safe and at most 1 day of Time Travel, which makes them cheaper for staging data you can rebuild.

**Zero-copy cloning.** `CREATE TABLE orders_dev CLONE orders;` creates a new table that points at the same micro-partitions. It is a metadata operation, nearly instant and free in storage at first; only micro-partitions changed later in either table take new space. You can clone schemas and whole databases, and combine it with Time Travel (`CLONE orders AT(TIMESTAMP => ...)`), which makes full-size development and test environments cheap.

**Secure data sharing.** A provider account grants read access to objects through a **share**; consumer accounts create a read-only database from it. No data is copied; the consumer queries the provider's micro-partitions with its own warehouse, so the consumer pays for compute.

```sql
CREATE SHARE sales_share;
GRANT USAGE ON DATABASE sales TO SHARE sales_share;
GRANT USAGE ON SCHEMA sales.marts TO SHARE sales_share;
GRANT SELECT ON TABLE sales.marts.fct_orders TO SHARE sales_share;
ALTER SHARE sales_share ADD ACCOUNTS = partner_org.partner_account;
```

Direct shares work within a region; listings with automatic replication (and the Snowflake Marketplace) reach other regions and clouds. BigQuery's equivalent is BigQuery sharing (formerly Analytics Hub).

| | Snowflake | BigQuery |
|---|---|---|
| Compute model | Virtual warehouses you size, start and suspend | Serverless slots, on-demand or reserved (editions) |
| Billing | Credits per second of warehouse running time | Bytes scanned, or slot capacity |
| Physical layout | Automatic micro-partitions, optional clustering key | Explicit partitioning and clustering |
| History | Time Travel up to 90 days, Fail-safe 7 days | Time travel window up to 7 days, fail-safe 7 days |
| Copies | Zero-copy clone | Table clones and snapshots |
| Sharing | Shares, listings, Marketplace | BigQuery sharing exchanges and listings |

> **Pitfall:** Leaving `AUTO_SUSPEND` high or unset on warehouses used by BI tools. A dashboard that refreshes every 5 minutes can keep a warehouse running all day. Set short suspends, and check `WAREHOUSE_METERING_HISTORY` for warehouses that never sleep.

> **Pitfall:** Fixing slow queries by always sizing up. If a query spills to remote storage or scans every micro-partition, a bigger warehouse just pays more to do the same waste. Check the query profile for partitions scanned versus total and for spilling first.

::: deeper
**Inside a query.** The services layer compiles SQL into a plan and prunes micro-partitions using metadata, often removing most of the table before a warehouse sees it. The warehouse nodes split the remaining micro-partitions among themselves (with work stealing to balance load), read columns from object storage or local cache, and exchange data for joins and aggregations. Intermediate results that do not fit in memory **spill** to local disk, then to remote storage, which the query profile reports and which is a common cause of slow queries. The NSDI 2020 paper "Building An Elastic Query Engine on Disaggregated Storage" analyses these behaviours on real workloads.

**Clustering.** Micro-partitions follow load order, so a table loaded daily is naturally clustered by date. For very large tables queried on other columns, a **clustering key** makes a background service reorganise micro-partitions, consuming credits. `SYSTEM$CLUSTERING_INFORMATION('orders', '(customer_id)')` reports clustering depth (how many micro-partitions overlap for a value). The modeling side is in [Warehouse Modeling Playbooks (BigQuery, Snowflake)](#modeling).

**Beyond the warehouse.** Snowflake also offers **dynamic tables** (declarative tables refreshed incrementally to a target lag), streams and tasks (change tracking and scheduling), Snowpark (Python, Java and Scala DataFrame code executed inside Snowflake), Iceberg tables (data in open format in your own bucket, with Snowflake or an external catalog such as Snowflake Open Catalog, a managed Apache Polaris), and the search optimization and query acceleration services for selective lookups and outlier queries.

**Transactions.** Snowflake provides ACID transactions with snapshot isolation using the immutable micro-partitions: a commit atomically swaps the table's list of micro-partitions in the metadata store. Concurrent DML on the same table can block or fail, so warehouses work best with batch-style writes.

> **Interview:** "How does Snowflake's zero-copy clone work, and what does it cost?" A clone is a new table whose metadata points at the source's existing immutable micro-partitions, so it is instant and initially uses no extra storage. Later changes to either table write new micro-partitions, and only those are stored and billed separately.
:::

::: check
?? Name Snowflake's three architectural layers and what each does.
Storage (immutable columnar micro-partitions in object storage), compute (virtual warehouses that run queries and cache data) and cloud services (metadata, optimisation, security, transactions and the result cache).

?? A query takes 8 minutes on a Medium warehouse and parallelises perfectly. Roughly what would it cost and take on Large?
About 4 minutes at twice the rate per hour, so the same number of credits (about 0.53), finishing in half the time.

?? Why is a zero-copy clone nearly instant?
It copies only metadata: the clone points at the same immutable micro-partitions as the source, and new storage is used only for later changes.
:::

::: resources
- Docs: [Snowflake key concepts and architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts) · the official description of storage, compute and cloud services layers
- Paper: [The Snowflake Elastic Data Warehouse](https://dl.acm.org/doi/10.1145/2882903.2903741) · the SIGMOD 2016 paper on the multi-cluster, shared-data architecture
- Paper: [Building An Elastic Query Engine on Disaggregated Storage](https://www.usenix.org/conference/nsdi20/presentation/vuppalapati) · NSDI 2020 analysis of Snowflake's caching, elasticity and spilling on real workloads
- Docs: [Understanding and using Time Travel](https://docs.snowflake.com/en/user-guide/data-time-travel) · retention, AT and BEFORE clauses, UNDROP and Fail-safe
- Docs: [Cloning considerations](https://docs.snowflake.com/en/user-guide/object-clone) · how zero-copy cloning works for tables, schemas and databases
- Book: [Snowflake: The Definitive Guide](https://www.oreilly.com/library/view/snowflake-the-definitive/9781098103811/) · Joyce Kay Avila; a hands-on tour of architecture, security and cost
- Course: [Snowflake University](https://learn.snowflake.com/) · Snowflake's free official courses and hands-on badges
:::

### ClickHouse — Architecture & Use Cases

::: plain
ClickHouse is like a filing room where every drawer is kept sorted by the label you chose when you set it up, say "customer, then date". Each drawer has a small card at the front listing the first label in every bundle of folders. To find one customer's records for a week, a clerk glances at the cards, pulls out only the few bundles that could contain them, and ignores the rest of the room. New folders arrive in small sorted batches and are merged into the big drawers in the background. Choosing the right sort label is most of the work.
:::

**ClickHouse** is an open-source (Apache 2.0) column-oriented database for real-time analytics, first built at Yandex for web analytics and open-sourced in 2016. It is known for very fast aggregations over billions of rows, which makes it popular for observability (logs, metrics, traces), product analytics, ad tech and customer-facing dashboards. You can run it yourself (on Compute Engine or GKE) or use ClickHouse Cloud, which is available on Google Cloud.

**MergeTree: the core table engine.** Almost every table uses an engine from the **MergeTree** family:

```sql
CREATE TABLE events
(
    event_date  Date,
    event_time  DateTime,
    tenant_id   UInt32,
    event_type  LowCardinality(String),   -- dictionary-encoded string
    user_id     UInt64,
    duration_ms UInt32,
    INDEX idx_user user_id TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, event_type, event_time);
```

How it stores data:

1. Each `INSERT` writes a new immutable **part**: a directory with one compressed file per column, sorted by the `ORDER BY` key.
2. Background **merges** combine small parts into larger ones (hence "MergeTree"), keeping the sort order. Merging is where deduplication and pre-aggregation happen for the specialised engines below.
3. Inside a part, rows are grouped into **granules** of 8,192 rows (the `index_granularity` setting).
4. The **primary index** (`primary.idx`) stores the sorting-key values of the first row of every granule. It is **sparse**: one entry per granule, not per row, so a billion-row table needs about 1,000,000,000 / 8,192 ≈ 122,071 entries, which fit easily in memory.
5. **Mark files** record where each granule starts inside each compressed column file, so a reader can jump straight to it.

[[fig:warehousing_mergetree]]

For `WHERE tenant_id = 42 AND event_type = 'click'`, ClickHouse binary-searches the primary index to find the granule ranges that can contain those key values, then reads only those granules and only the referenced columns. A filter on the first key column prunes best; a filter on only the third column prunes much less, because rows with the same `event_time` are scattered across all tenants. `EXPLAIN indexes = 1 SELECT ...` shows how many parts and granules each index selected.

**Choosing `ORDER BY`.** Put columns that most queries filter on first, ordered from low to higher cardinality (tenant, then event type, then time). Unlike a relational primary key, the MergeTree key is **not unique**: duplicate key values are allowed. `PARTITION BY` is mainly for data management (dropping or moving a month), not speed; keep partitions coarse, usually monthly, because each insert creates at least one part per partition it touches.

**Data skipping indexes** add summaries per block of granules for columns not in the key: `minmax`, `set`, `bloom_filter`, and token or n-gram bloom filters for text search. In the table above, `idx_user` lets a query for one `user_id` skip blocks whose bloom filter says the user is absent.

**Specialised MergeTree engines** do work during merges:

| Engine | What happens on merge | Typical use |
|---|---|---|
| `ReplacingMergeTree(version)` | Keeps the latest row per sorting key | Upserts, CDC from OLTP |
| `SummingMergeTree` | Sums numeric columns per key | Simple counters |
| `AggregatingMergeTree` | Merges aggregate function states per key | Rollups with uniques, quantiles |
| `CollapsingMergeTree(sign)` | Cancels rows with sign +1 and -1 | Changing rows from event streams |

Merges happen at unpredictable times, so a query can see rows that are not yet merged. Use `FINAL` (`SELECT ... FROM t FINAL`) or aggregate in the query when exact results matter.

**Materialized views are insert triggers.** A ClickHouse materialized view runs its `SELECT` on each inserted block of the source table and writes the result into a target table. Combined with `AggregatingMergeTree`, that maintains rollups incrementally:

```sql
CREATE TABLE events_daily
(
    event_date Date,
    tenant_id  UInt32,
    event_type LowCardinality(String),
    events     SimpleAggregateFunction(sum, UInt64),
    users      AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (tenant_id, event_type, event_date);

CREATE MATERIALIZED VIEW events_daily_mv TO events_daily AS
SELECT event_date, tenant_id, event_type,
       count() AS events, uniqState(user_id) AS users
FROM events
GROUP BY event_date, tenant_id, event_type;

SELECT event_date, sum(events) AS events, uniqMerge(users) AS users
FROM events_daily
WHERE tenant_id = 42
GROUP BY event_date ORDER BY event_date;
```

The view only sees new inserts; existing rows must be backfilled with an `INSERT INTO events_daily SELECT ...`. **Refreshable materialized views**, by contrast, rerun their whole query on a schedule. **Projections** store an alternative sort order or a pre-aggregation inside each part of the same table, and the optimizer uses them automatically.

**Sharding and replication.** For high availability, `ReplicatedMergeTree` tables keep copies of each part on several replicas, coordinated through **ClickHouse Keeper** (a Raft-based replacement for ZooKeeper). For scale, data is split into **shards**, and a `Distributed` table fans a query out to all shards and merges the results. ClickHouse Cloud replaces this with `SharedMergeTree`, which keeps parts in object storage and lets stateless compute replicas scale separately.

> **Pitfall:** Inserting one row at a time. Each insert creates a part, and thousands of tiny parts per second overwhelm merges ("too many parts" errors). Batch inserts (thousands to hundreds of thousands of rows), or enable asynchronous inserts so the server batches for you.

> **Pitfall:** Treating `ReplacingMergeTree` as a unique key. Duplicates remain until a merge happens, which may be never for old parts. Query with `FINAL` or deduplicate in the query.

::: deeper
**Why it is fast.** Beyond columnar storage and the sparse index, ClickHouse uses vectorized execution with SIMD-optimised functions, many specialised hash table variants chosen by key type for `GROUP BY`, aggressive use of all CPU cores per query, and careful compression codecs per column (including `Delta`, `DoubleDelta`, `Gorilla` and `T64` for time series, combined with LZ4 or ZSTD). The VLDB 2024 paper "ClickHouse - Lightning Fast Analytics for Everyone" describes the design.

**Updates and deletes.** Classic `ALTER TABLE ... UPDATE/DELETE` **mutations** rewrite whole parts in the background and are heavy. **Lightweight deletes** (`DELETE FROM t WHERE ...`) mark rows as deleted with a hidden mask column and remove them during merges. Designs that append new versions and resolve them with `ReplacingMergeTree` remain the common pattern for frequently changing data.

**Joins.** ClickHouse supports standard joins with several algorithms (hash, parallel hash, grace hash for joins larger than memory, merge joins on sorted data), and join performance has improved a lot. Still, many ClickHouse schemas denormalise into wide tables and use **dictionaries** (in-memory key-value lookups loaded from another table or source) for dimension attributes.

**ClickHouse versus BigQuery.** ClickHouse gives sub-second latency at high concurrency when its sort key matches the queries, and you control layout and hardware. BigQuery handles arbitrary ad hoc SQL over petabytes with no tuning or servers, but each query has more overhead. Many platforms use both: BigQuery as the warehouse, ClickHouse (or another real-time OLAP engine) for serving fast, predictable queries. The Real-time OLAP engines section compares ClickHouse with Druid, Pinot and StarRocks.

> **Interview:** "Why is the ClickHouse primary index sparse, and what does that imply?" One entry per granule of 8,192 rows keeps the index small enough to stay in memory even for huge tables. The price is that a lookup reads at least one whole granule, so ClickHouse is excellent for range scans and aggregations but not designed for high-rate single-row lookups.
:::

::: check
?? What is a granule, and how does the primary index use it?
A block of rows (8,192 by default) within a part. The sparse primary index stores the sorting-key values of the first row of each granule, so queries find the granule ranges that can match and read only those.

?? Why should `PARTITION BY` usually be coarse, such as by month?
Partitions exist mainly for data management, every insert creates at least one part per partition touched, and too many partitions lead to too many small parts and slow merges. The sort key, not the partition, does most of the pruning.

?? How does a ClickHouse materialized view differ from a BigQuery materialized view?
A ClickHouse materialized view is an insert trigger that transforms each new block and writes it into a separate target table; it does not see existing data or later updates unless you backfill. A BigQuery materialized view is maintained by the service and kept consistent with its base table.
:::

::: resources
- Docs: [MergeTree table engine](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree) · parts, granules, sorting keys, partitions and skipping indexes
- Docs: [A practical introduction to primary indexes](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes) · how the sparse index and marks prune granules, with worked examples
- Paper: [ClickHouse - Lightning Fast Analytics for Everyone](https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf) · VLDB 2024 paper on storage, query processing and integrations
- Docs: [Data replication](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication) · ReplicatedMergeTree and ClickHouse Keeper
- Docs: [Materialized views](https://clickhouse.com/docs/materialized-views) · incremental and refreshable materialized views
- Video: [ClickHouse YouTube channel](https://www.youtube.com/c/ClickHouseDB) · release webinars and internals talks by the core team
:::

### DuckDB — Embedded OLAP

::: plain
Most databases are like a restaurant: a separate building you send orders to over the phone. DuckDB is like a very good kitchen knife: it lives inside your own program, there is nothing to install as a server or connect to, and it works on the data right where it is, whether that is a Parquet file on disk, a folder of CSVs or a pandas DataFrame in memory. It is SQLite's idea applied to analytics: one small library, very fast at scanning and aggregating.
:::

**DuckDB** is an open-source (MIT), in-process analytical database. "In-process" means it runs as a library inside the host program (Python, R, Java, Node.js, the `duckdb` command line, even a web browser through WebAssembly), with no server, no network and no separate installation. It reached version 1.0 in 2024 and is maintained by DuckDB Labs, with the DuckDB Foundation holding the intellectual property.

**SQL on files, directly.** DuckDB queries files without loading them first:

```sql
-- A folder of Parquet files written with Hive-style directories (dt=2026-11-20/...)
SELECT event_type, COUNT(*) AS n
FROM read_parquet('events/*/*.parquet', hive_partitioning = true)
WHERE dt >= '2026-11-01'
GROUP BY ALL            -- group by every non-aggregated column
ORDER BY n DESC;

FROM 'orders.csv' LIMIT 5;                          -- FROM-first shorthand, auto-detected CSV
DESCRIBE SELECT * FROM 'orders.parquet';            -- column names and types
COPY (SELECT * FROM 'orders.csv') TO 'orders.parquet' (FORMAT parquet, COMPRESSION zstd);
```

It pushes filters and column selection into the Parquet reader (reading only needed row groups and columns), and uses Hive partition values in paths to skip whole directories.

**The Python API and Arrow.** In Python, DuckDB can query DataFrames by their variable name and return results in several formats:

```python
import duckdb
import pandas as pd

orders = pd.DataFrame({"region": ["EU", "EU", "APAC"], "total": [40, 10, 25]})
rel = duckdb.sql("SELECT region, SUM(total) AS revenue FROM orders GROUP BY ALL ORDER BY region")
print(rel.fetchall())     # [('APAC', 25), ('EU', 50)]
df = rel.df()             # back to pandas; .pl() gives Polars, .arrow() an Arrow table
```

DuckDB reads Apache Arrow memory (and pandas and Polars frames backed by it) largely without copying, and returns Arrow results the same way, which makes it a fast "SQL engine" for the Python data stack.

**Inside the engine.**

- **Columnar, compressed storage** in a single database file (`analytics.duckdb`), organised in row groups of about 122,880 rows with per-column compression (dictionary, RLE, bit-packing, frame of reference, FSST for strings, ALP for floats).
- **Vectorized, push-based execution.** Operators process vectors of 2,048 values at a time, and pipelines push data from sources to sinks.
- **Morsel-driven parallelism.** Each pipeline's input is split into small chunks ("morsels") that worker threads take from a shared queue, so all cores stay busy and load balances automatically.
- **Out-of-core processing.** Hash joins, aggregations and sorts that exceed the memory limit spill to temporary files instead of failing, so DuckDB can process datasets larger than RAM on a laptop.
- **ACID transactions** with multi-version concurrency control (MVCC). One process may write a database file at a time; many can read it read-only.

**Extensions.** Much of DuckDB's reach comes from extensions, loaded with `INSTALL` and `LOAD` (many autoload when first needed): `httpfs` for HTTP and object storage, `iceberg` and `delta` for table formats, `postgres` and `mysql` to query or attach operational databases, `spatial` for geometry, `json`, `excel` and full-text search. Community extensions install with `INSTALL name FROM community`.

```sql
INSTALL httpfs; LOAD httpfs;
-- Cloud Storage through its XML API with an HMAC key (keep the values in Secret Manager)
CREATE SECRET gcs_reader (TYPE gcs, KEY_ID 'your-hmac-key-id', SECRET 'your-hmac-secret');
SELECT COUNT(*) FROM read_parquet('gs://my-bucket/events/dt=2026-11-20/*.parquet');
```

**Friendly SQL.** DuckDB adds conveniences on top of PostgreSQL-like SQL: `GROUP BY ALL`, `ORDER BY ALL`, `SELECT * EXCLUDE (col)` and `REPLACE`, `COLUMNS('regex')` to apply an expression to many columns, `SUMMARIZE table` for quick profiling, and `PIVOT` / `UNPIVOT`.

**Where it fits in data engineering.**

| Use | Why DuckDB |
|---|---|
| Exploring files locally | No setup; seconds on gigabytes of Parquet |
| Tests and CI for SQL logic | Runs in-process in a test suite; dbt has a DuckDB adapter |
| Single-node transformation jobs | A Cloud Run job or Batch task that reads Cloud Storage, transforms and writes Parquet |
| Embedded analytics in applications | A library, no server to operate; WebAssembly build runs in the browser |
| Replacing pandas for heavy transforms | Parallel, out-of-core, and usually much less memory |

> **Pitfall:** Opening the same database file for writing from several processes. DuckDB allows one writer process per file. For concurrent writers, use a server database, MotherDuck, or write separate files and combine them.

> **Pitfall:** Using DuckDB as a shared multi-user warehouse on a network drive. It is a single-node, single-writer engine; it shines for one user, one job or one application at a time.

::: deeper
**Why push-based.** DuckDB started with the classic pull-based (Volcano-style) iterator model and moved to push-based pipelines, which make parallelism and pipeline scheduling easier to control: each pipeline has a source, a chain of operators and a sink (a hash table build, an aggregation, the result), and the scheduler runs pipelines in dependency order. The morsel idea comes from Leis and colleagues' SIGMOD 2014 paper on HyPer. Execution models (Volcano versus push) are compared in the Query engine internals section.

**Larger than memory, carefully.** Spilling keeps queries alive, but very large joins can still be slow when they spill heavily. Set `SET memory_limit = '8GB'` and `SET temp_directory = '/tmp/duck'` explicitly in containers, where the default limit (a fraction of detected RAM) may not match the container's cgroup limit.

**Lakehouse role.** DuckDB reads Iceberg and Delta tables through extensions and is the reference engine for DuckLake, a table format that keeps metadata in a SQL database (covered in [DuckLake and catalog-backed table formats](#lakehouse)). On Google Cloud, a common small-data pattern is Parquet or Iceberg in Cloud Storage, transformed by DuckDB in a Cloud Run job, with BigQuery reading the results as external or Iceberg tables.

**When not to use it.** Data that does not fit one machine's disk and CPUs, many concurrent writers, high-rate point updates, and strict multi-user security all point to a warehouse (BigQuery) or a server database instead.

> **Interview:** "Why is DuckDB often faster than pandas for the same transformation?" It is columnar and vectorized like pandas, but it also plans the whole query (pushing filters and projections into file readers), runs on all cores with morsel-driven parallelism, avoids materialising intermediate DataFrames, and spills to disk instead of running out of memory.
:::

::: check
?? What does "in-process" mean for DuckDB, and what is one consequence?
It runs as a library inside the host program, with no server or network connection. One consequence: only one process can write a given database file at a time.

?? How can DuckDB query a pandas DataFrame named `orders` without loading it?
By referring to `orders` as a table name in SQL; DuckDB finds the variable and scans its memory (largely zero-copy through Arrow).

?? What does DuckDB do when a hash join is larger than the memory limit?
It spills partitions of the join to temporary files on disk and processes them in turn, instead of failing with an out-of-memory error.
:::

::: resources
- Docs: [DuckDB documentation](https://duckdb.org/docs/) · SQL reference, clients, extensions and performance guides
- Paper: [DuckDB: an Embeddable Analytical Database](https://duckdb.org/pdf/SIGMOD2019-demo-duckdb.pdf) · the SIGMOD 2019 demo paper describing the design goals and architecture
- Paper: [Morsel-Driven Parallelism](https://db.in.tum.de/~leis/papers/morsels.pdf) · Leis and colleagues on the NUMA-aware parallel scheduling DuckDB adopted
- Book: [DuckDB in Action](https://www.manning.com/books/duckdb-in-action) · Needham, Hunger and Simons; practical SQL, Python integration and pipelines with DuckDB
- Blog: [DuckDB blog](https://duckdb.org/news/) · release notes and deep dives on storage, compression and execution by the core team
:::

### MotherDuck — DuckDB in the Cloud

::: plain
DuckDB is a brilliant personal workshop in your garage. MotherDuck adds a shared workshop in town: the same tools, but with storage everyone in your team can reach, bigger machines when you need them, and a way to lend your work to others. The clever part is that a job can be split between the two: the parts that need your local files run in your garage, the parts that need the shared data run in town, and only small results travel between them.
:::

**MotherDuck** is a managed cloud service built on DuckDB, created by a company founded in 2022 (by people from the BigQuery team, among others) in partnership with DuckDB Labs. It adds what an in-process engine lacks: durable shared storage, managed compute, sharing between users, and access from many clients at once.

**Connecting.** Any DuckDB client becomes a MotherDuck client by attaching a database whose name starts with `md:`:

```python
import duckdb

# Authenticates with the token in the motherduck_token environment variable
con = duckdb.connect("md:analytics")

con.sql("""
    CREATE TABLE IF NOT EXISTS orders AS
    SELECT * FROM read_parquet('gs://my-bucket/orders/*.parquet')
""")   # runs in the cloud; needs a GCS secret stored in MotherDuck
```

From the DuckDB command line, `ATTACH 'md:';` attaches all your MotherDuck databases next to any local ones.

**Hybrid (dual) execution.** A single query can combine local and cloud data:

```sql
SELECT s.segment, SUM(o.total) AS revenue
FROM read_csv('segments_from_marketing.csv') AS s    -- a file on your laptop
JOIN analytics.main.orders AS o USING (customer_id)  -- a table in MotherDuck
GROUP BY ALL;
```

The planner marks each operator as local or remote, depending on where its data lives, and inserts transfer operators at the boundaries. The CSV is scanned locally, the large `orders` table is scanned in the cloud, and only the data needed for the join crosses the network, usually the smaller side. The CIDR 2024 paper "MotherDuck: DuckDB in the cloud and in the client" describes the design.

**One DuckDB per user.** Rather than one big shared cluster, MotherDuck gives each user (or service account) their own DuckDB instance in the cloud, called a "duckling", sized for their work. One user's heavy query does not slow another's, and compute is billed by usage. For many concurrent readers, such as an application or BI tool, **read scaling** serves queries from several read replicas.

**Sharing.** A database can be shared read-only without copying:

```sql
CREATE SHARE orders_share FROM analytics;    -- returns a share URL (md:_share/...)
-- the recipient:
ATTACH 'md:_share/analytics/<share-id>' AS shared_analytics;
SELECT COUNT(*) FROM shared_analytics.main.orders;
```

Shares can be limited to your organisation or to named users, and updated automatically or manually when the source database changes.

**DuckDB in the browser.** DuckDB compiles to WebAssembly (**DuckDB-Wasm**), so a web page can run SQL on Parquet or CSV files entirely in the browser. MotherDuck's web UI and its Wasm client use this to run part of a query in the user's browser and part in the cloud, which makes interactive filtering of already-fetched results instant. This is useful for embedded analytics where each user explores a modest amount of data.

**The extension ecosystem.** Because MotherDuck runs DuckDB, the same extensions work for reading Parquet, CSV, JSON, Iceberg and Delta from object storage, with a curated set enabled in the cloud. Locally, community extensions add many more sources and functions. MotherDuck also offers managed DuckLake tables, keeping data in open formats.

**Where it fits.**

| Situation | MotherDuck fit |
|---|---|
| A small or mid-sized team whose data is gigabytes to a few terabytes | Good: a warehouse without cluster sizing, using familiar DuckDB SQL |
| Analysts who already work in DuckDB locally | Good: same SQL, hybrid queries, easy sharing |
| Per-customer analytics in a SaaS app | Good: one isolated instance per tenant or user |
| Petabyte-scale scans, complex enterprise governance on Google Cloud | Prefer BigQuery |

> **Pitfall:** Assuming hybrid execution is free. A query that joins a large local file with a large cloud table still has to move one of them over your network connection. Upload large local data first, or filter it locally before joining.

> **Pitfall:** Writing credentials for object storage into notebooks or scripts. Store them as MotherDuck secrets (`CREATE SECRET IN MOTHERDUCK ...`) or fetch them from Secret Manager at run time.

::: deeper
**Why per-user instances work.** Most analytical queries in most companies touch far less data than a single large machine can handle; the MotherDuck founders argue in "Big Data is Dead" that the average query does not need a distributed cluster. Scaling out across users instead of across nodes per query avoids the coordination costs of MPP for typical workloads, at the price of a ceiling on the largest single query.

**Storage design.** MotherDuck stores databases in its own managed storage with snapshots, so sharing and time-based restores work at the storage level rather than by copying files. Data read from external object storage stays where it is.

**Hybrid planning details.** Each operator gets a location property (local or remote); the optimizer can move work across the boundary when that reduces transfer, for example running a filter or partial aggregation before sending data. Transfers use DuckDB's columnar format, so there is no row-by-row serialisation.

**Comparison with BigQuery.** BigQuery is a multi-tenant MPP service: any query can use thousands of slots, and governance integrates with Google Cloud IAM, Knowledge Catalog and VPC Service Controls. MotherDuck trades that ceiling for DuckDB's single-node speed, low latency on small queries and a local-first workflow. Teams on Google Cloud sometimes use both: BigQuery as the system of record, DuckDB or MotherDuck for local development, exploration and small per-tenant workloads.

> **Interview:** "How would hybrid execution handle a join between a 10 MB local file and a 500 GB cloud table?" Scan the local file locally, send its small result to the cloud, and run the join and aggregation remotely next to the large table, returning only the result. The planner places operators to minimise data crossing the network.
:::

::: check
?? What does attaching `md:` do in a DuckDB client?
It connects the client to MotherDuck, making your cloud databases available alongside local ones, so queries can read and write cloud data and mix it with local data.

?? In a hybrid query joining a local CSV with a large cloud table, which data should cross the network?
Ideally only the smaller side (often the filtered local data) and the final result; the planner places scans next to their data and inserts transfer operators at the boundary.

?? Why does MotherDuck give each user their own DuckDB instance instead of one shared cluster?
To isolate workloads (one user's heavy query does not slow others), to bill compute per user and to keep DuckDB's simple single-node execution, scaling across users rather than across nodes for each query.
:::

::: resources
- Docs: [MotherDuck documentation](https://motherduck.com/docs/) · connecting, hybrid execution, sharing, secrets and read scaling
- Paper: [MotherDuck: DuckDB in the cloud and in the client](https://www.cidrdb.org/cidr2024/papers/p46-atwal.pdf) · the CIDR 2024 paper on hybrid query processing
- Blog: [MotherDuck blog](https://motherduck.com/blog/) · engineering posts on DuckDB, hybrid execution and small-data analytics
- Blog: [Big Data is Dead](https://motherduck.com/blog/big-data-is-dead/) · Jordan Tigani's argument that most workloads fit on one machine
- Repo: [DuckDB-Wasm](https://github.com/duckdb/duckdb-wasm) · DuckDB compiled to WebAssembly for in-browser analytics
- Docs: [DuckDB community extensions](https://duckdb.org/community_extensions/) · the catalogue of community-built DuckDB extensions
:::

## Performance

The engines in the previous group differ in architecture, but they speed queries up with the same small set of tools. This group goes through them in the order you usually reach for them. First, avoid reading data: **indexes** suited to analytics (zone maps, bitmaps, bloom filters, sparse indexes) and a physical layout (**partitioning and clustering**) that lets them skip most blocks. Second, avoid recomputing: **materialized views and pre-aggregations** store answers to frequent questions. Third, compute well: the **query optimizer** chooses join orders and strategies from statistics, and you can help or mislead it.

The last two topics move from speed to meaning. A **semantic layer** makes every tool compute "revenue" the same way, and often adds caching and routing to pre-aggregations on top. **OLAP operations** (roll-up, drill-down, slice, dice, pivot) are the vocabulary of multi-dimensional analysis that BI tools and semantic layers translate into the SQL you have seen throughout.

### Indexing in Analytical Systems

::: plain
In a transactional database an index is like the index at the back of a book: it tells you the exact page for a word. Analytical systems rarely need exact pages; they need to know which chapters they can skip. So their "indexes" are mostly summaries written on each chapter's cover: "this chapter covers dates from 3 to 5 March, customers 100 to 400". A reader looking for 10 March skips the chapter without opening it. Some summaries are cleverer, like a quick yes-or-no list of names that might appear inside.
:::

**Why B-trees are not enough.** A B-tree index (the default in PostgreSQL) maps each key to the location of its row. It is perfect for "find order 42" but wasteful for "sum revenue for November", which touches millions of rows: following millions of pointers is slower than scanning compressed columns. Analytical engines therefore favour **coarse, compact structures** that let them skip blocks, plus a few specialised indexes for selective lookups.

| Structure | What it stores | Best for | Found in |
|---|---|---|---|
| Zone map (min-max) | Min and max of each column per block | Range filters on sorted or clustered columns | Parquet, ORC, BigQuery, Snowflake, DuckDB, ClickHouse |
| Sparse primary index | Key of the first row of each granule | Range scans on the sort key | ClickHouse |
| Bitmap index | One bit vector per distinct value | Low-cardinality columns, combined filters | Oracle, Druid, Pinot, many OLAP engines |
| Bloom filter | Probabilistic "might contain / definitely not" | Equality on high-cardinality columns (IDs, emails) | Parquet, ORC, ClickHouse skip indexes |
| Inverted / search index | Token to row (or block) list | Text and log search, needle lookups | BigQuery search indexes, Elasticsearch; ClickHouse token bloom filters approximate it |

**Zone maps.** Every columnar format records, for each block (row group, micro-partition, granule), the minimum and maximum value of each column. A filter `WHERE day BETWEEN 40 AND 42` skips every block whose range does not overlap. This costs almost nothing to maintain, but it only works when similar values sit in the same blocks. A simulation makes the point:

```python
BLOCK = 1_000
# 100,000 orders loaded in date order: day 0..99, 1,000 orders per day
rows = [{"day": i // 1_000, "customer": (i * 7_919) % 5_000} for i in range(100_000)]

blocks = [rows[i:i + BLOCK] for i in range(0, len(rows), BLOCK)]
zone_map = [{c: (min(r[c] for r in b), max(r[c] for r in b)) for c in ("day", "customer")}
            for b in blocks]

def blocks_to_read(col, lo, hi):
    """Blocks whose [min, max] range overlaps [lo, hi]; the rest are skipped."""
    return sum(1 for z in zone_map if z[col][1] >= lo and z[col][0] <= hi)

print(blocks_to_read("day", 40, 42), "of", len(blocks))        # 3 of 100
print(blocks_to_read("customer", 42, 42), "of", len(blocks))   # 100 of 100

# Re-sort by customer and rebuild the zone map: now customer filters prune
blocks = [sorted(rows, key=lambda r: r["customer"])[i:i + BLOCK] for i in range(0, len(rows), BLOCK)]
zone_map = [{c: (min(r[c] for r in b), max(r[c] for r in b)) for c in ("day", "customer")}
            for b in blocks]
print(blocks_to_read("customer", 42, 42), "of", len(blocks))   # 1 of 100
print(blocks_to_read("day", 40, 42), "of", len(blocks))        # 100 of 100
```

Loaded in date order, a three-day filter reads 3 of 100 blocks, but a customer filter reads all of them, because every block contains customers from 0 to 4,999. Sorting by customer flips the result. One sort order cannot serve every filter, which is why clustering on several columns (next topic) and secondary structures exist.

**Bitmap indexes.** For each distinct value of a column, keep a bit vector with one bit per row. A filter becomes bitwise operations, which CPUs do 64 bits at a time:

```python
status  = ["paid", "paid", "refunded", "paid", "pending", "paid", "refunded", "paid"]
country = ["IN",   "DE",   "IN",       "IN",   "IN",      "FR",   "DE",       "IN"]

def bitmap_index(values):
    """One integer per distinct value; bit i is set when row i has that value."""
    idx = {}
    for i, v in enumerate(values):
        idx[v] = idx.get(v, 0) | (1 << i)
    return idx

s, c = bitmap_index(status), bitmap_index(country)
hits = s["paid"] & c["IN"]                       # WHERE status = 'paid' AND country = 'IN'
print(f"{s['paid']:08b}")                        # 10101011  (row 0 is the rightmost bit)
print(f"{c['IN']:08b}")                          # 10011101
print(f"{hits:08b}", [i for i in range(len(status)) if hits >> i & 1])   # 10001001 [0, 3, 7]
print(bin(s["paid"] | s["pending"]).count("1"))  # 6: COUNT(*) WHERE status IN ('paid', 'pending')
```

`AND`, `OR` and `NOT` of filters become `&`, `|` and `~`, and `COUNT(*)` becomes a population count, with no rows touched. Real systems compress bitmaps (run-length schemes such as WAH, or **Roaring bitmaps**, which switch between arrays and bitsets per chunk of 65,536 rows) so that high-cardinality columns do not explode. Bitmaps are expensive to update row by row, which is why they suit read-mostly analytical data. Real-time OLAP engines rely on them heavily; see [RTOLAP Indexing Deep Dive](#rtolap).

**Bloom filters.** A **bloom filter** is a small bit array that answers "is value X in this block?" with either "definitely not" or "maybe". For a high-cardinality column such as `user_id`, zone maps are useless (each block spans a wide range), but a bloom filter per block lets a lookup skip almost every block. Parquet supports optional per-column-chunk bloom filters; ClickHouse adds them as data skipping indexes (`INDEX idx_user user_id TYPE bloom_filter GRANULARITY 4`). The mathematics and sizing are in [Bloom Filters & Probabilistic Structures](#hardware).

**Search indexes for needles.** Finding one request ID in terabytes of logs is a "needle in a haystack" query. BigQuery **search indexes** (`CREATE SEARCH INDEX ... ON logs(ALL COLUMNS)`) build an inverted index over tokens, used by the `SEARCH()` function and, for some equality and `IN` predicates, by ordinary filters. Snowflake's **search optimization service** plays the same role. Both cost storage and background maintenance, so use them for genuinely selective lookups that run often.

> **Pitfall:** Creating a skipping index (bloom filter, minmax) on a column whose values appear in every block. The index is checked on every query, costs storage and maintenance, and never skips anything. Measure with `EXPLAIN` (ClickHouse `EXPLAIN indexes = 1`, or bytes and blocks read in the query plan) before and after.

> **Tip:** The cheapest index in a column store is a good sort order. Before adding any secondary structure, check whether clustering on the filtered column would let zone maps do the work.

::: deeper
**Small materialized aggregates.** Zone maps descend from Moerkotte's "Small Materialized Aggregates" (VLDB 1998): per-block min, max, count and sum. Some engines answer `MIN`, `MAX` and `COUNT(*)` entirely from this metadata (Snowflake's metadata cache, DuckDB and Parquet statistics) without reading data.

**Page-level and file-level stats.** Pruning happens at several levels: whole partitions (from partition values), files (table-format manifests in Iceberg and Delta store per-file column stats), row groups (footer stats) and pages (the Parquet page index). Each level skips more precisely but costs more metadata to read; engines check coarse levels first.

**Bitmap index variants.** *Equality-encoded* bitmaps store one vector per value. *Range-encoded* bitmaps store "value ≤ v" vectors, which answer range filters with one or two vectors. *Bit-sliced* indexes store one vector per bit of a numeric value and can compute sums directly. Druid and Pinot build inverted indexes as Roaring bitmaps per dimension value.

**Bloom filter limits.** A bloom filter supports only equality (and `IN`), not ranges; its false-positive rate depends on bits per value, so very high-cardinality blocks need larger filters. A 1% false-positive rate needs about 9.6 bits per distinct value.

> **Interview:** "Why don't warehouses use B-tree indexes?" Analytical queries touch large fractions of a table, and scanning compressed columns sequentially beats millions of random index lookups. Warehouses instead keep tiny per-block summaries (zone maps) that let them skip blocks, and add specialised structures (bloom filters, bitmaps, search indexes) only for selective lookups.
:::

::: check
?? Why does a zone map on `customer_id` skip nothing in a table loaded in date order?
Each block holds orders from many customers, so its min and max for `customer_id` span almost the whole range, and every block overlaps any value searched for.

?? Which structure would you use to skip blocks for `WHERE email = 'a@b.com'` on a large table, and why?
A bloom filter (or a search index): email is high-cardinality, so min-max ranges are useless, but a bloom filter per block says "definitely not here" for almost every block.

?? How does a bitmap index evaluate `status = 'paid' AND country = 'IN'`?
It takes the bit vector for `paid` and the bit vector for `IN` and computes their bitwise AND; the set bits are the matching rows, and a population count gives `COUNT(*)`.
:::

::: resources
- Docs: [Parquet bloom filter specification](https://parquet.apache.org/docs/file-format/bloomfilter/) · how Parquet stores split-block bloom filters per column chunk
- Docs: [Understanding ClickHouse data skipping indexes](https://clickhouse.com/docs/optimize/skipping-indexes) · minmax, set and bloom filter indexes and when they help
- Docs: [Introduction to search in BigQuery](https://cloud.google.com/bigquery/docs/search-intro) · search indexes and the SEARCH function for needle-in-a-haystack queries
- Docs: [Snowflake search optimization service](https://docs.snowflake.com/en/user-guide/search-optimization-service) · selective point lookups on large tables
- Paper: [Better bitmap performance with Roaring bitmaps](https://arxiv.org/abs/1402.6407) · Chambi, Lemire and colleagues on the compressed bitmap format used by many engines
- Paper: [Small Materialized Aggregates](https://www.vldb.org/conf/1998/p476.pdf) · Moerkotte's VLDB 1998 paper behind zone maps
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; chapter 3 on indexes and column-oriented storage
:::

### Partitioning & Clustering Strategies

::: plain
Partitioning is putting each year's paperwork in its own box: to find something from 2025 you open one box and ignore the rest, and throwing out 2019 means discarding a box, not sorting through papers. Clustering is keeping the papers inside each box in alphabetical order, so you can flip straight to the right place. Partitioning gives hard walls; clustering gives good order within them. You pick the walls by how you usually look things up and how you throw things away.
:::

**Partitioning** splits a table into separately stored pieces by the value of a **partition key**. The engine can then (1) **prune**: skip partitions a filter excludes, and (2) **manage** data per partition: load, overwrite, archive or drop one partition without touching the others. There are three classic schemes:

| Scheme | Rule | Typical key | Good for |
|---|---|---|---|
| Range | Each partition holds a range of values | Date, timestamp, numeric ID | Time-series facts; retention by age |
| List | Each partition holds a listed set of values | Region, country, tenant tier | Separating known groups |
| Hash | `hash(key) mod N` picks the partition | Customer ID, user ID | Even spread; co-locating joins |

PostgreSQL's declarative partitioning shows all three:

```sql
CREATE TABLE orders (
  order_id    bigint,
  order_date  date NOT NULL,
  region      text NOT NULL,
  customer_id bigint,
  total       numeric
) PARTITION BY RANGE (order_date);

CREATE TABLE orders_2026_11 PARTITION OF orders
  FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');      -- upper bound is exclusive

-- List and hash schemes, for other tables
CREATE TABLE stores (store_id int, region text) PARTITION BY LIST (region);
CREATE TABLE stores_eu PARTITION OF stores FOR VALUES IN ('DE', 'FR', 'NL');

CREATE TABLE sessions (user_id bigint, started_at timestamptz) PARTITION BY HASH (user_id);
CREATE TABLE sessions_p0 PARTITION OF sessions FOR VALUES WITH (MODULUS 4, REMAINDER 0);
```

Dropping a month is then `DROP TABLE orders_2025_11;` (or `DETACH PARTITION`), instant compared with `DELETE ... WHERE order_date < ...`.

**Partitioning in analytical engines.**

- **BigQuery**: partitions by a date or timestamp column (day, hour, month or year), by ingestion time, or by an integer range, with a limit of 10,000 partitions per table. Details, limits and `require_partition_filter` are in [Partitioning and clustering in BigQuery](#bigquery).
- **ClickHouse**: `PARTITION BY toYYYYMM(event_date)` mainly for data management; the sort key does the pruning.
- **Snowflake**: no user-defined partitions; micro-partitions are automatic.
- **Lakehouse tables**: Hive-style directories (`dt=2026-11-20/`) in plain data lakes; Iceberg's **hidden partitioning** stores a transform of a column (`days(event_ts)`, `bucket(16, user_id)`) so queries filter on the column itself, and partitioning can evolve without rewriting old data.

**Clustering (sort keys).** Clustering orders rows *within* a table or partition by one or more columns, so that each block covers a narrow range of those columns and zone maps prune well. Unlike partitions, clustering has no hard boundaries and no limit on distinct values, so it suits high-cardinality columns such as `customer_id`.

```sql
-- BigQuery: partition by day, cluster by the next most common filters
CREATE TABLE mart.fct_orders
PARTITION BY order_date
CLUSTER BY customer_id, product_id
AS SELECT * FROM staging.orders;
```

Engines implement clustering differently: BigQuery's `CLUSTER BY` (up to four columns, re-clustered automatically at no charge), Snowflake **clustering keys** (automatic clustering billed in credits), ClickHouse's `ORDER BY` (sorted on insert and merge), Iceberg sort orders (`WRITE ORDERED BY`) applied by compaction, and Delta **liquid clustering**. With a multi-column sort, the first column prunes best and later columns prune well only within ranges of the earlier ones. **Z-order** and **Hilbert curves** interleave several columns so each prunes moderately well; [Data Skipping, Z-Ordering & Liquid Clustering](#lakehouse) explains them.

**Choosing keys: a worked example.** A `clickstream` fact holds 3 years of data, about 400 GB per day. Dashboards filter on the last 7 days and often on `country` and `customer_id`; a retention rule deletes data older than 3 years.

1. Partition by `event_date` (day): 3 × 365 = 1,095 partitions, well under limits, each about 400 GB. A 7-day query reads 7 / 1,095 ≈ 0.6% of the table, and retention is "drop partitions older than 1,095 days".
2. Cluster by `country, customer_id`: within each day, a filter on country (low cardinality, used often) prunes first, and customer filters prune within it.
3. Do not partition by `customer_id`: millions of values would create millions of tiny partitions (or hit limits), destroying both pruning and file sizes.

| Question | Use |
|---|---|
| Do queries almost always filter on a date range? | Partition by date |
| Do you delete or reload data by day or month? | Partition by that period |
| Is the filtered column high-cardinality (IDs)? | Cluster, do not partition |
| Several filter columns, none dominant? | Multi-column clustering, or Z-order or Hilbert |
| Is the table small (a few GB)? | Probably neither; maybe cluster |
| Do two large tables always join on one key? | Hash-distribute or bucket both on that key |

> **Pitfall:** Over-partitioning. Hourly partitions on a modest table, or partitions on a high-cardinality column, create thousands of tiny partitions and files. Metadata grows, each file compresses poorly, and planning gets slower. Partitions should each hold a substantial amount of data; below that, cluster instead.

> **Pitfall:** Filtering on an expression of the partition column, such as `WHERE FORMAT_DATE('%Y-%m', order_date) = '2026-11'`. Many engines cannot prune through arbitrary functions. Filter on the column with a range: `order_date >= '2026-11-01' AND order_date < '2026-12-01'`.

::: deeper
**Static and dynamic pruning.** Static pruning happens at planning time when the filter has constant values. Dynamic (run-time) pruning happens when the values come from another table, for example `WHERE order_date IN (SELECT day FROM calendar WHERE is_holiday)` or a join against a filtered dimension. Engines such as Trino, Spark, Snowflake and BigQuery push the join's build-side values to the probe-side scan at run time (dynamic filtering), but you can make it easier with explicit date ranges.

**Partition overwrite for idempotent loads.** Reloading a day by replacing its partition (`INSERT OVERWRITE` in Spark and Hive, dbt's `insert_overwrite` strategy on BigQuery, or a `MERGE` restricted to the partition) makes backfills idempotent: running the load twice gives the same result. It requires that each load writes complete partitions.

**Bucketing.** Hive and Spark **bucketing** hash-partitions files by a key into a fixed number of buckets. If two tables are bucketed identically on the join key, the engine can join bucket to bucket without a shuffle. Iceberg's `bucket(N, col)` transform generalises this, and storage-partitioned joins in Spark use it.

**Reclustering cost.** Clustering degrades as new data arrives in arbitrary order. Systems that re-cluster in the background (BigQuery for free; Snowflake and Delta for compute credits) trade write-side work for read-side pruning. Measure the benefit: compare bytes or partitions scanned for typical queries before paying for heavy clustering.

> **Interview:** "Partition or cluster on `customer_id`?" Cluster. It has too many distinct values for partitions, which have per-partition overhead and limits; clustering sorts rows so zone maps prune for customer filters, with no limit on cardinality. Partition by the date the queries filter on and the retention policy uses.
:::

::: check
?? What two benefits does partitioning give, and which one does clustering not provide?
Pruning (skipping partitions that cannot match) and per-partition management (overwrite, drop, expire). Clustering helps pruning but gives no hard boundaries for managing data.

?? A table is partitioned by `order_date`. Why might `WHERE EXTRACT(MONTH FROM order_date) = 11` scan every partition?
The engine may not be able to map a function of the column back to partitions; a range filter on the column itself (`order_date >= ... AND order_date < ...`) prunes reliably.

?? Why does the first column of a multi-column clustering key matter most?
Rows are sorted by the first column, then by the second within equal first values, so blocks have narrow ranges for the first column but wide ranges for later ones unless the earlier ones are also filtered.
:::

::: resources
- Docs: [PostgreSQL table partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html) · range, list and hash partitioning, pruning and partition maintenance
- Docs: [BigQuery: introduction to partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables) · partition types, limits and partitioning versus sharding
- Docs: [BigQuery: introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables) · how clustering prunes blocks and when to combine it with partitioning
- Docs: [Snowflake clustering keys and clustered tables](https://docs.snowflake.com/en/user-guide/tables-clustering-keys) · when a clustering key helps and what automatic clustering costs
- Docs: [Apache Iceberg partitioning](https://iceberg.apache.org/docs/latest/partitioning/) · hidden partitioning, transforms and partition evolution
- Docs: [ClickHouse custom partitioning key](https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key) · why partitions in ClickHouse are for data management
:::

### Materialized Views & Pre-Aggregations

::: plain
If a hundred people ask the librarian every morning how many books were borrowed yesterday, she should not count the loan slips a hundred times. She counts once, writes the answer on the board, and updates it whenever new slips arrive. A materialized view is that number on the board: a stored answer to a frequent question, kept up to date as the data changes, so the expensive counting happens once instead of on every request.
:::

A **view** stores only a query; each read runs it again. A **materialized view** (MV) stores the query's **result** as data and keeps it fresh when the base tables change. More generally, a **pre-aggregation** is any stored summary (a rollup table, a cube, an MV, a cache) that answers queries more cheaply than the raw data.

**Refresh strategies.**

| Strategy | How it works | Cost per refresh | Freshness |
|---|---|---|---|
| Full refresh | Recompute the whole result and replace it | Proportional to all data | As of the last run |
| Incremental refresh | Apply only changes (deltas) since the last refresh | Proportional to the changes | As of the last run, cheaply frequent |
| Query-time merge | Store the result, merge unapplied deltas at read time | Small, at query time | Always current |

**Incremental maintenance.** For aggregates that can be combined, a delta can be applied without rereading old data. `SUM` and `COUNT` are easy: new totals are old totals plus the totals of the new rows. This sketch maintains a daily revenue MV incrementally in SQLite and checks it against a full recompute:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE sales (sale_id INTEGER PRIMARY KEY, day TEXT, amount REAL);
CREATE TABLE mv_daily (day TEXT PRIMARY KEY, revenue REAL, n INTEGER);
INSERT INTO sales (day, amount) VALUES ('2026-11-20', 40), ('2026-11-20', 10), ('2026-11-21', 25);
-- initial full build of the materialized view
INSERT INTO mv_daily SELECT day, SUM(amount), COUNT(*) FROM sales GROUP BY day;
""")

def refresh_incremental(new_rows):
    """Apply only the new rows: aggregate the delta, then merge it into the view."""
    db.executemany("INSERT INTO sales (day, amount) VALUES (?, ?)", new_rows)
    db.execute("CREATE TEMP TABLE delta (day TEXT, amount REAL)")
    db.executemany("INSERT INTO delta VALUES (?, ?)", new_rows)
    db.execute("""
        INSERT INTO mv_daily (day, revenue, n)
        SELECT day, SUM(amount), COUNT(*) FROM delta GROUP BY day
        ON CONFLICT (day) DO UPDATE SET revenue = revenue + excluded.revenue,
                                        n = n + excluded.n""")
    db.execute("DROP TABLE delta")

refresh_incremental([("2026-11-21", 5), ("2026-11-22", 60)])
print(db.execute("SELECT * FROM mv_daily ORDER BY day").fetchall())
# [('2026-11-20', 50.0, 2), ('2026-11-21', 30.0, 2), ('2026-11-22', 60.0, 1)]
full = db.execute("SELECT day, SUM(amount), COUNT(*) FROM sales GROUP BY day ORDER BY day").fetchall()
print(full == db.execute("SELECT * FROM mv_daily ORDER BY day").fetchall())   # True
```

Other aggregates are harder. `MIN` and `MAX` survive inserts but not deletes (if the maximum row is deleted, you need the next largest, which the summary does not have). `COUNT(DISTINCT user_id)` cannot be added across days (a user active on two days would be counted twice); store a **sketch** such as HyperLogLog (`HLL_COUNT.INIT` in BigQuery, `uniqState` in ClickHouse) and merge sketches instead. Averages are stored as sum and count, never as an average. The classification into additive, semi-additive and non-additive measures is in [OLAP Cube Design & Modern Alternatives](#analytics).

**Materialized views by engine.**

- **BigQuery** MVs refresh incrementally in the background, combine stored results with base-table changes at query time so reads are always fresh, and are used automatically by **smart tuning** (queries on the base table are rewritten to read the MV when it can answer them). Creation rules and limits are in [Datasets, tables, views and materialized views in BigQuery](#bigquery).
- **Snowflake** MVs (Enterprise edition) are maintained by a background service billed in credits and are limited to a single table (no joins); **dynamic tables** cover multi-table, incrementally refreshed pipelines with a target lag.
- **ClickHouse** MVs are insert triggers writing to a target table (see the ClickHouse topic), and **projections** store alternative orderings or aggregates inside the table.
- **PostgreSQL** MVs refresh only fully, with `REFRESH MATERIALIZED VIEW CONCURRENTLY` to avoid blocking readers (it needs a unique index on the MV).

**Pre-aggregations outside the database.** In **dbt**, a model's `materialized` setting decides how it is stored: `view`, `table`, `incremental` (insert or merge only new rows on each run), `ephemeral` (inlined as a CTE) or `materialized_view` (a database MV, where the adapter supports it):

```sql
-- models/marts/agg_daily_revenue.sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'order_date', 'data_type': 'date'}
) }}
SELECT order_date, region, SUM(net_amount) AS revenue, COUNT(*) AS orders
FROM {{ ref('fct_orders') }}
{% if is_incremental() %}
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)   -- reprocess late data
{% endif %}
GROUP BY order_date, region
```

dbt **exposures** declare which dashboards and applications depend on which models, so you can see which aggregates are actually used before you build or drop them. **Semantic layers and BI tools** route queries to pre-aggregations automatically: Cube matches a request to a defined pre-aggregation and serves it from its own store; Looker's **aggregate awareness** rewrites explores to the smallest suitable aggregate table. This "smart rollup" routing is what lets one dashboard use a tiny daily table while a drill-down to single orders still reaches the fact table.

> **Pitfall:** Building a pre-aggregation for every dashboard. Each one is a table to refresh, test and pay for, and many are read a few times. Start from query history (the most frequent, most expensive query shapes) and build the few aggregates that serve many of them.

> **Pitfall:** Summing an average or adding distinct counts across a rollup. Store the components (sum and count, or a mergeable sketch) and compute the ratio at the end.

::: deeper
**View maintenance theory.** Incremental view maintenance derives, for each operator, how a change to its inputs changes its output: for a join, Δ(R ⋈ S) = (ΔR ⋈ S) ∪ (R ⋈ ΔS) ∪ (ΔR ⋈ ΔS); for a grouped `SUM`, add the delta's group sums. Deletes are handled by counting (keeping a count per group, removing the group when it reaches zero). Systems differ in which operators they support incrementally; outer joins, window functions and non-deterministic functions usually force a full refresh. Streaming engines such as Materialize and RisingWave apply the same ideas continuously (differential dataflow in Materialize's case).

**Freshness versus cost.** A refresh every minute on a large fact can cost more than the queries it saves. Reason about it with the ratio of reads to changes: an MV read thousands of times between refreshes is a clear win; one refreshed more often than it is read is not.

**Staleness tolerance.** BigQuery's `max_staleness` option lets an MV return results up to a chosen age without merging recent changes, trading freshness for lower query cost; it is useful for dashboards where data a few hours old is acceptable.

> **Interview:** "How would you make a slow, heavily used dashboard fast?" Look at its queries in the job history, identify the common grain and filters, build one incrementally maintained aggregate (MV or incremental model) at that grain with mergeable measures, route the dashboard to it (directly, via aggregate awareness or a semantic layer), and measure bytes and latency before and after.
:::

::: check
?? Why can a daily `SUM` be maintained incrementally but a daily `COUNT(DISTINCT user_id)` cannot simply be added up into a monthly figure?
Sums of disjoint sets add up exactly. Distinct counts do not: a user active on several days would be counted once per day. Store mergeable sketches (HyperLogLog) per day and merge them instead.

?? What is the difference between a dbt `incremental` model and a database materialized view?
An incremental model is a table that dbt updates on each scheduled run with SQL you write (insert, merge or overwrite partitions). A materialized view is maintained by the database itself, often automatically and with query rewriting.

?? What does aggregate awareness (or pre-aggregation matching) do?
It rewrites a query to read the smallest stored aggregate that can answer it correctly, falling back to the detailed table when no aggregate matches.
:::

::: resources
- Docs: [dbt materializations](https://docs.getdbt.com/docs/build/materializations) · view, table, incremental, ephemeral and materialized view strategies
- Docs: [Introduction to materialized views in BigQuery](https://cloud.google.com/bigquery/docs/materialized-views-intro) · incremental refresh, smart tuning and limits
- Docs: [Snowflake materialized views](https://docs.snowflake.com/en/user-guide/views-materialized) · maintenance, cost and when to prefer dynamic tables
- Docs: [Cube pre-aggregations](https://cube.dev/docs/product/caching/using-pre-aggregations) · defining, refreshing and matching rollups to queries
- Docs: [Looker aggregate awareness](https://cloud.google.com/looker/docs/aggregate_awareness) · routing explores to aggregate tables automatically
- Docs: [dbt exposures](https://docs.getdbt.com/docs/build/exposures) · declaring downstream dashboards and their model dependencies
:::

### Query Optimization in DWH

::: plain
Asking a warehouse a question is like asking a travel agent for a route from Lisbon to Tokyo. You only say where to start and where to end; the agent chooses the airlines, the connections and the order of the legs, using timetables and prices. A good agent finds a route that takes a day; a bad one sends you through five airports. The query optimizer is that agent: SQL says what you want, and the optimizer chooses how to get it, using statistics about your data as its timetables. When its information is wrong, it picks a bad route.
:::

SQL is **declarative**: it says what result you want, not how to compute it. The **optimizer** turns the SQL into a **plan** (a tree of operators such as scan, filter, join, aggregate) and chooses among the many equivalent plans the one it expects to be cheapest.

**From SQL to plan.**

1. **Parse and bind**: check syntax, resolve tables and columns, check permissions.
2. **Logical rewrites** (rule-based): transformations that are almost always good, such as pushing filters below joins (**predicate pushdown**), dropping unused columns (**projection pruning**), folding constants, turning subqueries into joins (**decorrelation**), removing joins whose result is not used (**join elimination**, possible when keys are declared) and eliminating partitions.
3. **Cost-based choices**: join order, join algorithm, where to aggregate, how to distribute data across workers. Each candidate gets an estimated cost from **statistics**.
4. **Physical plan** and execution, possibly adapted at run time.

**Statistics drive the estimates.** The optimizer needs to know how many rows each step produces (**cardinality**). It uses table row counts, the number of distinct values (NDV) per column, null fractions, min and max, and sometimes histograms of value frequencies. With them it estimates **selectivity**: the fraction of rows a filter keeps. `WHERE country = 'FR'` with 50 distinct countries is estimated at 1/50 of the rows, unless a histogram says France is more common.

The classic weakness is the **independence assumption**: for `WHERE country = 'FR' AND city = 'Paris'`, the optimizer multiplies the two selectivities, as if they were unrelated. They are not:

```python
# 1,000,000 customers; city and country are strongly correlated
rows = 1_000_000
n_countries, n_cities = 50, 2_000          # distinct values the optimizer knows
sel_country = 1 / n_countries              # uniform guess for country = 'FR'
sel_city = 1 / n_cities                    # uniform guess for city = 'Paris'

estimate = rows * sel_country * sel_city   # independence: multiply selectivities
actual = rows * sel_city                   # every Paris row is also an FR row
print(f"estimated {estimate:.0f} rows, actual {actual:.0f} rows")   # estimated 10 rows, actual 500 rows
print(f"error factor {actual / estimate:.0f}x")                    # error factor 50x
```

A 50-fold underestimate may make the optimizer broadcast what it thinks is a 10-row result to every worker, or choose a nested-loop join that is fine for 10 rows and terrible for 500,000 after a few more joins multiply the error. Errors compound through a plan, which is why bad plans usually come from bad cardinality estimates rather than bad cost formulas.

**Join ordering.** With *n* tables there are many orders: 5 tables give 120 left-deep orders (5!) and 1,680 bushy trees. Optimizers search them with dynamic programming (the System R approach from Selinger's 1979 paper) for small joins and with heuristics or greedy search for large ones. The goal is to keep intermediate results small: join and filter the most selective combinations first.

**Join algorithms in a warehouse.** **Hash joins** dominate: build a hash table on the smaller input, probe it with the larger. In a distributed engine the choice is about data movement: **broadcast** the small side to every worker, or **shuffle** both sides by the join key. Sort-merge joins appear when inputs are already sorted; nested-loop joins only for small inputs or non-equality conditions. The physical strategies are detailed in [Physical join strategies](pyspark-streaming-kafka-topic-map.html#joinphys) in the PySpark guide.

**Run-time techniques.** Modern warehouses do not rely on estimates alone. **Dynamic (runtime) filtering** sends the join keys found on the build side (often as a bloom filter or min-max range) to the probe-side scan, which then skips blocks and rows that cannot join; a filter on a small dimension thus prunes the large fact. **Adaptive execution** changes the plan mid-query when real sizes differ from estimates: switching join strategy, splitting skewed partitions, changing parallelism (BigQuery's dynamic query execution, Spark's AQE).

**Collecting statistics.** Warehouses with managed storage (BigQuery, Snowflake) maintain metadata automatically. Engines over open files often need you to collect it:

```sql
-- PostgreSQL: refresh statistics, and teach the planner about correlated columns
ANALYZE customers;
CREATE STATISTICS customers_city_country (dependencies) ON city, country FROM customers;

-- Trino over Hive or Iceberg tables
ANALYZE hive.sales.orders;
SHOW STATS FOR hive.sales.orders;

-- Spark SQL
ANALYZE TABLE sales.orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_date;
```

**Reading plans.** Every engine shows its plan: `EXPLAIN` (and `EXPLAIN ANALYZE`, which runs the query and reports actual rows and times) in PostgreSQL, Trino, DuckDB and Spark; the query profile in Snowflake; the execution graph in BigQuery (see [Reading BigQuery query plans and tuning queries](#bigquery)). The habit that matters: compare **estimated** rows with **actual** rows at each step. Where they diverge by orders of magnitude, the plan is built on a wrong guess.

> **Pitfall:** Hiding filter columns inside functions or casts (`WHERE CAST(order_id AS STRING) = '42'`, `WHERE DATE(ts) = ...` on an engine that cannot push it down). The optimizer cannot use statistics or pruning on the raw column. Filter on the column itself and convert the constant instead.

> **Tip:** Declare primary and foreign keys, even unenforced ones (`PRIMARY KEY (id) NOT ENFORCED` in BigQuery, informational constraints in Snowflake and Trino connectors). Optimizers use them for join elimination and better estimates; then test them, because a wrong declared key gives wrong results.

::: deeper
**How good are optimizers?** Leis and colleagues' VLDB 2015 paper "How Good Are Query Optimizers, Really?" built the Join Order Benchmark on real, correlated data (IMDB) and showed that cardinality estimation errors grow exponentially with the number of joins in every system tested, and that good cardinality estimates matter far more than an accurate cost model.

**Cascades and extensibility.** Many modern optimizers (SQL Server, Calcite-based systems, Greenplum's Orca, CockroachDB) use the Volcano/Cascades framework: a memo structure of equivalent expressions explored with transformation rules and pruned by cost. [Cost-Based Optimization (CBO)](#queryengines) and Apache Calcite in the Query engine internals section go deeper.

**Aggregation pushdown and eager aggregation.** An optimizer may aggregate before a join (pre-aggregate the fact by the join key, then join the smaller result to the dimension) when that shrinks the data. In distributed engines, a **partial aggregate** on each worker before the shuffle and a **final aggregate** after is standard, and is why `GROUP BY` on a low-cardinality key moves very little data.

**Learned and feedback-driven optimizers.** Some systems feed actual cardinalities from past runs back into estimates, or use machine learning to steer plan choices; these remain an active research area, and most production gains still come from adaptive execution at run time.

> **Interview:** "A query got slow after the data grew, with no SQL change. What do you look at?" The plan, comparing estimated and actual rows per step: a changed join order or strategy (a broadcast that no longer fits, a nested loop on a big input), skew, stale or missing statistics, and lost pruning. Then fix the cause: refresh statistics, add filters the optimizer can push down, declare keys, or restructure the query.
:::

::: check
?? Why does `WHERE country = 'FR' AND city = 'Paris'` often get a badly underestimated row count?
The optimizer multiplies the selectivities of the two filters as if they were independent, but city determines country, so the true selectivity is close to that of the city filter alone.

?? What does dynamic (runtime) filtering do in a star-schema query?
It collects the join keys that survive the dimension's filter at run time and pushes them to the fact-table scan, which skips blocks and rows that cannot match the join.

?? What is the single most useful thing to compare when reading `EXPLAIN ANALYZE` output?
Estimated versus actual row counts at each operator; large differences show where the optimizer's assumptions went wrong.
:::

::: resources
- Book: [Database Internals](https://www.oreilly.com/library/view/database-internals/9781492040330/) · Alex Petrov; storage and distributed internals that underlie plans and costs
- Paper: [How Good Are Query Optimizers, Really?](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) · Leis and colleagues on cardinality estimation errors and the Join Order Benchmark
- Docs: [PostgreSQL: using EXPLAIN](https://www.postgresql.org/docs/current/using-explain.html) · reading plans, estimated versus actual rows, and costs
- Docs: [Trino cost-based optimizations](https://trino.io/docs/current/optimizer/cost-based-optimizations.html) · join reordering, join distribution and the statistics they need
- Article: [Use The Index, Luke](https://use-the-index-luke.com/) · Markus Winand's guide to how SQL is executed and how to write optimizer-friendly SQL
- Course: [CMU 15-721 Advanced Database Systems](https://15721.courses.cs.cmu.edu/) · lectures on optimizer implementation, cost models and cardinality estimation
:::

### Semantic Layer & Metrics Layer

::: plain
Imagine every department in a company keeping its own calculator with its own buttons for "revenue": one includes tax, one excludes refunds, one uses the shipping date. Meetings become arguments about whose number is right. A semantic layer replaces them with one shared calculator that everyone uses through their own screen, whether a dashboard, a spreadsheet or a chatbot. It knows the official definitions, how the tables join and who may see what, and it can keep popular answers ready so they come back instantly.
:::

[[fig:warehousing_semantic]]

A **semantic layer** sits between the warehouse and the tools that consume data. Consumers ask in business terms ("revenue by month for the EU"); the layer translates that into correct SQL against the curated tables, applies access rules, and often serves results from a cache or pre-aggregation. How to *model* the metrics themselves (entities, measures, dimensions, metric types, MetricFlow YAML) is covered in [Semantic Layer & Metrics Modeling](#modeling); this topic is about the layer as a piece of warehouse architecture: where it runs, how queries flow through it, and which tool to choose.

**What a request looks like.** A front end, a notebook or an AI agent sends a structured request rather than SQL, for example to Cube's REST API:

```json
{
  "measures": ["orders.revenue"],
  "dimensions": ["customers.region"],
  "timeDimensions": [{"dimension": "orders.order_date", "granularity": "month",
                      "dateRange": ["2026-01-01", "2026-10-31"]}],
  "filters": [{"member": "customers.region", "operator": "equals", "values": ["EU"]}]
}
```

The layer then:

1. **Resolves names** to definitions: `orders.revenue` is `SUM(net_amount)` on the orders fact with its built-in filters.
2. **Plans joins** from declared relationships (orders many-to-one customers), choosing safe join paths and avoiding fan-out when measures come from different facts.
3. **Applies security**: row-level rules (only this user's region or tenant), hidden fields.
4. **Routes** to a pre-aggregation or cache if one can answer, otherwise generates warehouse SQL.
5. **Returns** the result in the shape the client wants (JSON, a SQL result set, an Arrow stream).

A matching Cube data model, with a pre-aggregation the router can use:

```yaml
cubes:
  - name: orders
    sql_table: mart.fct_orders
    joins:
      - name: customers
        relationship: many_to_one
        sql: "{CUBE}.customer_id = {customers}.customer_id"
    measures:
      - name: revenue
        sql: net_amount
        type: sum
      - name: count
        type: count
    dimensions:
      - name: order_date
        sql: order_date
        type: time
    pre_aggregations:
      - name: revenue_by_region_day
        measures: [CUBE.revenue, CUBE.count]
        dimensions: [customers.region]
        time_dimension: CUBE.order_date
        granularity: day
        refresh_key:
          every: 1 hour
```

The monthly EU request above is answered from `revenue_by_region_day` (rolled up from days to months), never touching the fact table.

**The main tools.**

| Tool | Definition language | How consumers connect | Notes |
|---|---|---|---|
| Looker (LookML) | LookML views, explores, measures | Looker UI, embedded, Open SQL Interface (JDBC), connectors to other BI tools | Mature modeling, aggregate awareness, Git-based development |
| dbt Semantic Layer | MetricFlow YAML in the dbt project | JDBC and GraphQL APIs, integrations with BI tools and spreadsheets | Definitions live next to the dbt models that feed them |
| Cube | YAML or JavaScript data model | SQL API (PostgreSQL protocol), REST, GraphQL | Open-source core; strong caching with Cube Store pre-aggregations |
| AtScale | Virtual cube model | MDX, DAX, SQL interfaces (Excel, Power BI, Tableau) | Aimed at spreadsheet and cube-style tools |

**Headless BI** is the name for this design: the semantic layer is "headless" (no built-in charts) and exposes APIs that any "head" (dashboards, notebooks, apps, AI agents) can use. The alternative, definitions inside one BI tool, works well when that tool is the only consumer; once three tools and an application need the same numbers, a shared layer saves repeated, drifting definitions.

A small LookML example for comparison:

```text
view: orders {
  sql_table_name: mart.fct_orders ;;
  dimension: order_id { primary_key: yes  type: number  sql: ${TABLE}.order_id ;; }
  dimension_group: ordered { type: time  timeframes: [date, week, month]  sql: ${TABLE}.order_ts ;; }
  measure: revenue { type: sum  sql: ${TABLE}.net_amount ;;  value_format_name: usd }
}
```

**Semantic layers and AI.** Natural-language and agent interfaces generate far more reliable answers when they query governed metrics through a semantic layer than when they write raw SQL against hundreds of tables: the model chooses a metric and dimensions, and the layer guarantees the joins and definitions. Looker's conversational analytics features and the dbt Semantic Layer's integrations both build on this idea.

> **Pitfall:** Defining metrics in the semantic layer while dashboards keep their own custom SQL. Two sources of truth are worse than one. Migrate the important dashboards, and make the layer the only route to certified metrics.

> **Pitfall:** Putting heavy transformation logic in the semantic layer. It should define measures, joins and filters over clean marts, not clean data; complex logic belongs in the transformation layer (dbt, Dataform), where it is tested and materialised.

::: deeper
**Fan-out and chasm traps.** A request for `orders.revenue` and `shipments.count` by customer joins two facts through a shared dimension. A naive join multiplies rows (each order is repeated once per shipment), inflating revenue. Semantic layers avoid this by aggregating each fact separately to the requested grain and then joining the aggregates (sometimes called **symmetric aggregates** in Looker, which uses distinct-key tricks inside SQL, or multi-fact queries in MetricFlow and Cube).

**Caching layers.** Responses can be cached at several levels: an in-memory result cache keyed by the compiled query, persisted pre-aggregations refreshed on a schedule or by a refresh key (a query whose result changes when data changes, such as `MAX(updated_at)`), and the warehouse's own result cache. Invalidation is the hard part; refresh keys tie it to data changes rather than clocks.

**Where it runs.** Looker is a managed Google Cloud service (Looker, Google Cloud core). dbt's Semantic Layer runs in dbt's platform; MetricFlow itself is open source. Cube can be self-hosted (for example on Cloud Run or GKE, with Cube Store on persistent disks) or used as Cube Cloud. Latency matters: put the layer in the same region as BigQuery.

**Standards.** Every tool has its own definition language, so moving between them means rewriting definitions. Industry efforts to define a shared, vendor-neutral format for semantic models were announced in 2025; check their status before relying on them.

> **Interview:** "Why use a semantic layer if dbt already builds clean marts?" Marts fix the data, not the arithmetic on top: each tool still writes its own `SUM`, filters and joins, and they drift. A semantic layer defines each metric once, generates correct joins at any grain, enforces access rules and caches results, for every consumer including applications and AI agents.
:::

::: check
?? Name the steps a semantic layer performs between receiving a request and returning a result.
Resolve metric and dimension names to definitions, plan the joins, apply access rules, route to a pre-aggregation or cache or generate warehouse SQL, and return the result in the requested format.

?? What does "headless BI" mean?
A semantic layer without its own visualisation that exposes metric definitions through APIs (SQL, REST, GraphQL), so any front end can query the same definitions.

?? Why can joining two fact tables through a shared dimension inflate a measure, and how do semantic layers avoid it?
Each row of one fact is repeated for every matching row of the other (fan-out). Semantic layers aggregate each fact to the requested grain first and then join the aggregates, or use symmetric aggregate techniques.
:::

::: resources
- Docs: [dbt Semantic Layer](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl) · MetricFlow-based metrics served through APIs and integrations
- Docs: [Cube documentation](https://cube.dev/docs/product/introduction) · data modeling, APIs, caching and pre-aggregations in Cube
- Docs: [What is LookML?](https://cloud.google.com/looker/docs/what-is-lookml) · Looker's modeling language for views, explores and measures
- Docs: [Looker Open SQL Interface](https://cloud.google.com/looker/docs/sql-interface) · querying LookML models over JDBC from other tools
- Blog: [The missing piece of the modern data stack](https://benn.substack.com/p/metrics-layer) · Benn Stancil's influential essay on the metrics layer
:::

### OLAP Operations & Multi-Dimensional Analysis

::: plain
Think of sales data as a big cube made of small boxes: one edge is time, one is region, one is product, and each small box holds the sales for one month, one region and one product. OLAP operations are the ways you handle the cube. You can zoom out (years instead of months), zoom in, cut one thin slice (only bikes), carve out a smaller block (2026, Europe, bikes and parts), or turn the cube to look at a different face (regions down the side, products across the top). Every dashboard click is one of these moves.
:::

**The multi-dimensional model.** Analysts think of a fact table as a **cube**: **dimensions** are the axes (time, region, product), **measures** are the values in the cells (amount, quantity), and each dimension has **hierarchies** of levels (Year > Quarter > Month > Day; Category > Subcategory > Product; Region > Country > City). The cube is a mental model; in a modern warehouse it is usually a star schema queried with SQL, not a physical cube.

**The five operations**, on a small sales table:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE sales (year INT, quarter TEXT, region TEXT, category TEXT, amount INT);
INSERT INTO sales VALUES
 (2025,'Q4','EU','Bikes',100), (2025,'Q4','APAC','Bikes',80), (2025,'Q4','EU','Parts',30),
 (2026,'Q1','EU','Bikes',120), (2026,'Q1','APAC','Parts',50), (2026,'Q2','EU','Parts',40),
 (2026,'Q2','APAC','Bikes',90), (2026,'Q2','EU','Bikes',70);
""")
q = lambda sql: db.execute(sql).fetchall()

# Roll-up: from quarters to years
print(q("SELECT year, SUM(amount) FROM sales GROUP BY year ORDER BY year"))
# [(2025, 210), (2026, 370)]

# Drill-down: open 2026 into its quarters
print(q("SELECT quarter, SUM(amount) FROM sales WHERE year = 2026 GROUP BY quarter ORDER BY quarter"))
# [('Q1', 170), ('Q2', 200)]

# Slice: fix one dimension (category = Bikes); dice: restrict several (2026, EU)
print(q("SELECT region, SUM(amount) FROM sales WHERE category = 'Bikes' GROUP BY region ORDER BY region"))
# [('APAC', 170), ('EU', 290)]
print(q("SELECT category, SUM(amount) FROM sales WHERE year = 2026 AND region = 'EU' GROUP BY category ORDER BY category"))
# [('Bikes', 190), ('Parts', 40)]

# Pivot: regions as rows, categories as columns
print(q("""SELECT region,
                  SUM(amount) FILTER (WHERE category = 'Bikes') AS bikes,
                  SUM(amount) FILTER (WHERE category = 'Parts') AS parts
           FROM sales GROUP BY region ORDER BY region"""))
# [('APAC', 170, 50), ('EU', 290, 70)]
```

| Operation | Meaning | SQL shape |
|---|---|---|
| Roll-up | Go up a hierarchy (month to year) or drop a dimension | Coarser `GROUP BY` |
| Drill-down | Go down a hierarchy (year to quarter) | Finer `GROUP BY`, usually with a filter on the parent |
| Slice | Fix one dimension to one value | One `WHERE` equality |
| Dice | Restrict several dimensions to subsets | Several `WHERE` conditions |
| Pivot | Rotate: turn row values into columns | `PIVOT`, or conditional aggregation |
| Drill-across | Combine measures from two facts on shared dimensions | Aggregate each fact, join on conformed dimensions |
| Drill-through | Show the detail rows behind a cell | `SELECT` from the fact with the cell's filters |

**Several levels in one query.** Reports often need detail rows, subtotals and a grand total together. `GROUPING SETS`, `ROLLUP` and `CUBE` produce them in one pass; their mechanics are covered in [Advanced SQL for Analytics](#sql). In multi-dimensional terms, `ROLLUP (year, quarter)` walks one hierarchy upward, and `CUBE (region, category)` computes every combination of two independent dimensions. `GROUPING()` tells a real `NULL` from a subtotal row:

```sql
SELECT
  IF(GROUPING(region) = 1, 'All regions', region)       AS region,
  IF(GROUPING(category) = 1, 'All categories', category) AS category,
  SUM(amount) AS amount
FROM sales
WHERE year = 2026
GROUP BY CUBE (region, category)
ORDER BY GROUPING(region), region, GROUPING(category), category;
```

BigQuery also has a `PIVOT` operator, which spells the pivot directly:

```sql
SELECT *
FROM (SELECT region, category, amount FROM sales)
PIVOT (SUM(amount) FOR category IN ('Bikes', 'Parts'));
```

**Hierarchies in practice.** A **balanced** hierarchy has every level filled (calendar). A **ragged** hierarchy skips levels for some members (a city-state with no region). A **parent-child** hierarchy has variable depth (an org chart, a chart of accounts) and is stored as `employee_id, manager_id`, then flattened with a recursive CTE or a bridge table so roll-ups sum correctly. The modeling patterns are in [Advanced SQL Modeling Patterns](#sql) and in the Data modeling section.

**MDX in one example.** Classic cube servers are queried with **MDX** (Multidimensional Expressions). It names axes and a slicer instead of `GROUP BY` and `WHERE`:

```text
SELECT
  { [Measures].[Amount] } ON COLUMNS,
  NON EMPTY { [Region].[Region].MEMBERS } * { [Product].[Category].MEMBERS } ON ROWS
FROM [Sales]
WHERE ( [Date].[Year].[2026] )
```

This is a dice (year 2026) shown as regions crossed with categories. You meet MDX today mostly through Excel pivot tables connected to cube-style services, and through semantic layers (such as AtScale) that accept MDX and translate it to warehouse SQL. Cube design itself (MOLAP, ROLAP, cuboid explosion) is in [OLAP Cube Design & Modern Alternatives](#analytics).

> **Pitfall:** Drilling down on a semi-additive measure such as account balance or inventory level, then summing across time. Balances add across accounts but not across days; roll them up over time with the last value (or an average), not `SUM`.

> **Pitfall:** Pivoting on a column with unbounded values. A pivot needs a fixed, known list of output columns; with hundreds of products it produces an unreadable, slow query. Pivot only small, stable value sets, and do the rest in the BI tool.

::: deeper
**How BI tools turn clicks into SQL.** A drill-down on a chart is a new query with a finer `GROUP BY` plus a filter for the clicked member; a pivot table is usually one aggregated query reshaped client-side. Semantic layers and aggregate awareness (previous topics) make these repeated queries cheap by routing them to pre-aggregations at the right grain.

**Totals that do not add up.** Distinct counts and ratios behave differently at each level: distinct customers per region do not sum to distinct customers overall, and the total margin percentage is not the sum (or average) of regional percentages. A correct multi-level report computes each level from base data or from mergeable components, which `GROUPING SETS` does naturally.

**Data cube theory.** Gray and colleagues' 1997 paper "Data Cube: A Relational Aggregation Operator" introduced the `CUBE` and `ROLLUP` operators and showed how each grouping can be computed from a finer one, which is how engines evaluate them efficiently.

> **Interview:** "Explain slice versus dice." Slicing fixes a single dimension to one value, giving a lower-dimensional view (only 2026). Dicing selects a sub-cube by restricting several dimensions to subsets (2026, EU, bikes and parts). Both are `WHERE` clauses in SQL; the difference is how many dimensions they restrict.
:::

::: check
?? A dashboard shows revenue by year; the user clicks 2026 and sees quarters. Which operation is this, and what SQL changes?
Drill-down: the `GROUP BY` moves from year to quarter, with a filter `year = 2026` added.

?? What does `GROUPING(region) = 1` mean in a row produced by `GROUP BY CUBE (region, category)`?
That the row is a subtotal over all regions (region was not part of that grouping set), so its `NULL` region means "all", not a missing value.

?? Why can't you sum daily inventory levels to get a monthly inventory figure?
Inventory is semi-additive: it adds across products and warehouses but not across time. Use the period-end value (or an average) when rolling up over time.
:::

::: resources
- Docs: [BigQuery query syntax: GROUP BY ROLLUP, CUBE, GROUPING SETS and PIVOT](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax) · the GoogleSQL reference for multi-level grouping and pivoting
- Docs: [PostgreSQL: GROUPING SETS, CUBE and ROLLUP](https://www.postgresql.org/docs/current/queries-table-expressions.html) · the standard SQL semantics with examples
- Paper: [Data Cube: A Relational Aggregation Operator](https://arxiv.org/abs/cs/0701155) · Gray and colleagues introduce CUBE and ROLLUP
- Book: [The Data Warehouse Toolkit, 3rd edition](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) · Kimball and Ross on hierarchies, drill-across and semi-additive facts
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; chapter 3 on data cubes and materialized aggregates
:::

## Running a warehouse

A warehouse that is fast and well designed can still fail its users: a refactor silently changes revenue, a new source takes three weeks of hand-written SQL to onboard, or one customer's heavy queries slow down everyone else's dashboards. This group covers the engineering practices that keep a warehouse trustworthy and economical as it grows.

**Testing** catches wrong logic before it ships and wrong data before it reaches consumers. **Automation** generates the repetitive parts of a warehouse (tables, loads, merges, tests) from metadata, so people spend their time on business logic. **Multi-tenancy** covers warehouses that serve many customers or teams from one platform: how to isolate their data, keep one from starving another, and attribute cost to each. All three build on the earlier groups: layers give tests their boundaries, partitioning gives tenants their isolation, and the optimizer's statistics show who used what.

### Data Warehouse Testing & TDD

::: plain
A bakery checks its work twice. Before baking a new recipe for real, the baker tries it on a small batch with measured ingredients and checks that the result tastes as intended: that is a unit test. Then every day, each tray coming out of the oven is inspected for burnt or missing pieces before it goes on the shelf: that is a data test. Test-driven development means writing down "what a good loaf looks like" before changing the recipe, so you know at once if the change broke something.
:::

Warehouse code is mostly SQL transformations, and SQL that runs without errors can still be wrong: a join that duplicates rows, a filter that drops `NULL`s, a tie that picks a random row. Testing a warehouse means checking two different things, and confusing them is a common mistake:

| Kind | Checks | Runs on | When | Example |
|---|---|---|---|---|
| Unit test | The **logic** of one transformation | Small, fixed input rows you write | In CI, before deploy | Latest version per customer wins, ties broken by load time |
| Data test | The **data** produced meets rules | The real built table | After each build | `order_id` unique and not null |
| Contract test | The **shape** at a boundary | Schema and types of a model or source | At build time | `fct_orders.total` is `NUMERIC`, column exists |
| Reconciliation | Totals agree with a trusted source | Real data, aggregated | After each build | Revenue equals the ledger within 0.1% |

**Unit tests and TDD.** A unit test feeds a transformation known input rows and compares its output with the expected rows. Test-driven development (TDD) writes that test first, watches it fail (red), changes the model until it passes (green), and then tidies the code (refactor). Here is the loop for a "latest version per customer" model, using SQLite as the engine:

```python
import sqlite3

def model_sql(order_by):
    """The transformation under test: keep the latest version of each customer."""
    return f"""
    SELECT customer_id, email FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY {order_by}) AS rn
      FROM raw_customers)
    WHERE rn = 1"""

TESTS = [
    ("keeps the newest version",
     [(1, "a@old.com", "2026-01-01", 1), (1, "a@new.com", "2026-03-01", 2), (2, "b@x.com", "2026-02-01", 3)],
     [(1, "a@new.com"), (2, "b@x.com")]),
    ("same updated_at: the later load wins",
     [(3, "c@first.com", "2026-05-01", 4), (3, "c@second.com", "2026-05-01", 5)],
     [(3, "c@second.com")]),
]

def run(order_by):
    for name, given, expect in TESTS:
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE raw_customers (customer_id, email, updated_at, loaded_at)")
        db.executemany("INSERT INTO raw_customers VALUES (?, ?, ?, ?)", given)
        got = sorted(db.execute(model_sql(order_by)).fetchall())
        print("PASS" if got == sorted(expect) else f"FAIL got {got}:", name)

run("updated_at DESC")                  # first version of the model
# PASS keeps the newest version
# FAIL got [(3, 'c@first.com')]: same updated_at: the later load wins
run("updated_at DESC, loaded_at DESC")  # fixed: a deterministic tie-breaker
# PASS keeps the newest version
# PASS same updated_at: the later load wins
```

The second test describes an edge case (two versions with the same timestamp) before the model handles it. With only `updated_at` in the ordering, which row wins a tie is up to the engine, so the result is not guaranteed; the test makes the requirement explicit and the fix (a tie-breaker) obvious.

**The same in dbt.** dbt (1.8 and later) has native unit tests written in YAML, with `given` inputs mocked for each `ref` or `source`:

```yaml
unit_tests:
  - name: latest_version_wins_ties_by_load_time
    model: dim_customer
    given:
      - input: ref('stg_customers')
        rows:
          - {customer_id: 3, email: c@first.com,  updated_at: 2026-05-01, loaded_at: 4}
          - {customer_id: 3, email: c@second.com, updated_at: 2026-05-01, loaded_at: 5}
    expect:
      rows:
        - {customer_id: 3, email: c@second.com}
```

`dbt test --select test_type:unit` runs them without touching production data. Use them for logic with edge cases: date boundaries, `NULL` handling, deduplication, SCD logic, complex `CASE` expressions.

**Data tests in dbt.** Four generic tests come built in: `unique`, `not_null`, `accepted_values` and `relationships`. A **singular test** is a SQL file in `tests/` that returns failing rows. A **custom generic test** is a reusable macro:

```sql
-- tests/generic/non_negative.sql
{% test non_negative(model, column_name) %}
SELECT * FROM {{ model }} WHERE {{ column_name }} < 0
{% endtest %}
```

It can then be attached to any column as `data_tests: [non_negative]`. Configure `severity: warn` for rules that need investigation rather than a stopped pipeline, and `store_failures: true` to keep the failing rows in a table for debugging. The table of what to test on each kind of model is in [Model Quality, Testing & Observability](#modeling).

**Contracts.** A dbt **model contract** (`contract: {enforced: true}` with every column's `data_type`) makes the build fail if the model's output columns or types differ from the declaration, before the table is replaced. It protects downstream consumers from accidental schema changes; the design of contracts is in [Data Contract Modeling](#modeling).

**Great Expectations and other frameworks.** **Great Expectations** (GX) defines "expectations" such as `expect_column_values_to_be_between` or `expect_column_proportion_of_unique_values_to_be_between`, groups them into suites and runs them as validation steps, often in an orchestrator (Managed Service for Apache Airflow (formerly Cloud Composer), for example) right after a load and before downstream models run. The `dbt-expectations` package brings many of the same checks into dbt as generic tests. On Google Cloud, **Dataform** has built-in assertions in each action's config (`uniqueKey`, `nonNull`, `rowConditions`), and **Knowledge Catalog** data quality scans check tables on a schedule.

**Testing in CI.** A typical pipeline on every pull request:

1. Build only changed models and their children into a temporary dataset: `dbt build --select state:modified+ --defer --state ./prod-artifacts` (unmodified parents are read from production).
2. Run unit tests, data tests and contract checks on that build.
3. Compare key outputs with production (row counts, totals by day) to catch unintended changes.
4. Drop the temporary dataset when the pull request closes.

**Write-audit-publish (WAP)** applies the same idea in production: write the new version of a table to a staging location, audit it with tests, and only then publish it (swap or copy into place). Consumers never see a version that failed its tests.

> **Pitfall:** Only testing with `not_null` and `unique`. They catch broken keys but not wrong logic: a revenue model that drops refunds passes every structural test. Add unit tests for business rules and at least one reconciliation against a trusted total.

> **Pitfall:** Unit tests that copy the model's logic into the expected output (computing "expected" with the same SQL). Write expected rows by hand from the requirement, so the test checks the requirement, not the implementation.

::: deeper
**Test the edges, not the middle.** Most transformation bugs live at boundaries: the first and last day of a month, time zones around midnight, leap days, late-arriving rows, empty inputs, `NULL` in join keys, duplicate keys in a dimension, and ties in ordering. A good unit test suite has one small case per edge rather than one big realistic dataset.

**Cost of tests in the warehouse.** Data tests are queries, and on large tables they cost money and time. Restrict them to recent partitions where the rule allows (`where: "order_date >= current_date - 3"` in a test's config), sample very large tables for statistical checks, and keep full scans for nightly runs.

**Blue-green and zero-copy environments.** Snowflake zero-copy clones and BigQuery table clones make it cheap to build a full copy of production for testing a change, then swap. On Iceberg tables, **branches** support WAP directly: write to a branch, validate, then fast-forward `main`.

**Testing the tests.** Introduce a deliberate bug (mutation testing) and check that some test fails. A suite that stays green when you delete a `WHERE` clause is not protecting that clause.

> **Interview:** "How do you test a dbt project?" Unit tests for transformation logic with hand-written inputs and outputs in CI; generic and singular data tests on built tables (keys, relationships, accepted values, business rules) with severities; contracts on public models; reconciliation against source or finance totals; slim CI that builds changed models and their children into a temporary dataset; and write-audit-publish so failed builds never reach consumers.
:::

::: check
?? What is the difference between a unit test and a data test in a warehouse?
A unit test checks a transformation's logic on small, fixed input rows you write, in CI before deploy. A data test checks that the real data produced by a build meets rules, after the build.

?? Why does TDD start by watching the new test fail?
A test that passes before the change might not be testing anything; seeing it fail proves it detects the missing behaviour, and seeing it pass afterwards proves the change added it.

?? What does write-audit-publish prevent?
Consumers reading a table version that failed its tests: new data is written to a staging location, audited, and only published once the audit passes.
:::

::: resources
- Docs: [dbt data tests](https://docs.getdbt.com/docs/build/data-tests) · generic, singular and custom generic tests, severity and stored failures
- Docs: [dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests) · YAML unit tests with mocked inputs and expected outputs
- Docs: [dbt model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) · enforcing column names and types at build time
- Docs: [Great Expectations expectations gallery](https://greatexpectations.io/expectations/) · the catalogue of built-in expectations
- Docs: [Dataform assertions](https://cloud.google.com/dataform/docs/assertions) · built-in and manual data quality assertions in Dataform on Google Cloud
- Blog: [Announcing unit testing in dbt](https://docs.getdbt.com/blog/announcing-unit-testing) · why dbt added native unit tests and how they work
:::

### Data Warehouse Automation

::: plain
If you write the same kind of letter a hundred times, you stop writing each one by hand and make a template with blanks for the name and address. Warehouse automation does that for warehouse code. Most tables are loaded, merged, tested and documented in the same few ways, so instead of hand-writing each one, you describe each table in a short list of facts (its name, keys and columns) and let a generator write the repetitive code. People then spend their time on the parts that really differ.
:::

**What gets automated.** A large warehouse contains hundreds of objects built from a handful of patterns: a staging model per source table, an incremental load with `MERGE`, a type 2 slowly changing dimension, a Data Vault hub or satellite, the standard tests on keys, and documentation. **Data warehouse automation** means generating these from **metadata**: a description of each object (source, keys, columns, types, load pattern), kept in YAML files, a database table or a catalog.

**A metadata-driven generator in miniature.** Given the metadata of one table, this script writes its DDL and a `MERGE` in BigQuery GoogleSQL:

```python
# Metadata for one table: in real life this comes from YAML, a catalog or a spreadsheet
table = {
    "name": "dim_customer",
    "source": "staging.stg_customers",
    "keys": ["customer_id"],
    "columns": {"customer_id": "INT64", "email": "STRING", "city": "STRING",
                "updated_at": "TIMESTAMP"},
}

def ddl(t):
    cols = ",\n".join(f"  {c} {typ}" for c, typ in t["columns"].items())
    return f"CREATE TABLE IF NOT EXISTS mart.{t['name']} (\n{cols}\n);"

def merge(t):
    on = " AND ".join(f"T.{k} = S.{k}" for k in t["keys"])
    non_keys = [c for c in t["columns"] if c not in t["keys"]]
    sets = ", ".join(f"{c} = S.{c}" for c in non_keys)
    cols = ", ".join(t["columns"])
    vals = ", ".join(f"S.{c}" for c in t["columns"])
    return (f"MERGE mart.{t['name']} T\nUSING {t['source']} S\nON {on}\n"
            f"WHEN MATCHED THEN UPDATE SET {sets}\n"
            f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals});")

print(ddl(table))
print(merge(table))
```

It prints:

```text
CREATE TABLE IF NOT EXISTS mart.dim_customer (
  customer_id INT64,
  email STRING,
  city STRING,
  updated_at TIMESTAMP
);
MERGE mart.dim_customer T
USING staging.stg_customers S
ON T.customer_id = S.customer_id
WHEN MATCHED THEN UPDATE SET email = S.email, city = S.city, updated_at = S.updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, email, city, updated_at) VALUES (S.customer_id, S.email, S.city, S.updated_at);
```

Adding a table now means adding five lines of metadata, not fifty lines of SQL, and fixing a bug in the pattern (say, adding `WHEN NOT MATCHED BY SOURCE` handling) fixes every table at once. Real generators add more patterns (SCD type 2, partition overwrite, Data Vault hashes), quoting, type mapping per platform, and tests.

**Templating with dbt and Jinja.** In dbt projects, automation usually means **macros** (Jinja functions that return SQL) and **packages**:

- The **dbt-codegen** package generates starting code from the warehouse's metadata: `dbt run-operation generate_source --args '{"schema_name": "raw_shop", "generate_columns": true}'` writes a sources YAML listing every table and column; `generate_base_model` writes a staging model; `generate_model_yaml` writes documentation and test stubs.
- Macros encapsulate patterns: a `scd2_snapshot()` or `hash_key()` macro used by many models.
- Packages such as **AutomateDV** generate Data Vault 2.0 hubs, links and satellites from a few lines of metadata per entity; `dbt_utils` provides generic helpers (`generate_surrogate_key`, `union_relations`, `pivot`).

**Dataform on Google Cloud.** Dataform (BigQuery's native SQL workflow tool) writes models in SQLX and can generate many actions from JavaScript, for example one staging view per source table:

```text
// definitions/staging.js
["orders", "customers", "payments"].forEach(name => {
  publish(`stg_${name}`, { type: "view", schema: "staging" })
    .query(ctx => `SELECT * EXCEPT (_raw_payload) FROM ${ctx.ref("raw_shop", name)}`);
});
```

**Commercial automation tools.** **WhereScape** (RED and 3D) keeps a metadata repository of sources, targets and patterns, generates DDL and ELT code for platforms such as Snowflake, Databricks and SQL Server, and produces lineage and documentation from the same metadata. **VaultSpeed** does the same for Data Vault. They trade flexibility for speed and consistency, and suit large, pattern-heavy warehouses with many sources.

| Approach | Effort to start | Flexibility | Best for |
|---|---|---|---|
| Hand-written SQL | Low | Total | Few, unique models |
| Jinja macros and dbt packages | Medium | High | Most dbt projects |
| Own metadata-driven generator | Medium to high | High, and fully yours | Many similar sources, custom patterns |
| Commercial automation (WhereScape, VaultSpeed) | Licence and training | Bounded by the tool's patterns | Large enterprise warehouses, Data Vault |

> **Pitfall:** Hand-editing generated code. The next generation run overwrites the edit, or worse, the edit survives and the code silently drifts from its metadata. Change the metadata or the template; if a table genuinely needs custom logic, take it out of the generator explicitly.

> **Pitfall:** Automating before the pattern is stable. Generating fifty tables from a pattern you will change next week multiplies rework. Hand-write the first two or three instances, agree on the pattern, then generate.

::: deeper
**Metadata sources.** Generators read metadata from source system catalogs (`INFORMATION_SCHEMA.COLUMNS`), schema registries (Avro or Protobuf schemas for events), data contracts, or a curated configuration repository. The richer the metadata (descriptions, owners, sensitivity tags), the more you can generate: documentation, column-level access policies, data quality rules and catalog entries in Knowledge Catalog, not just SQL.

**Generated code still needs tests.** Test the generator itself (unit tests on its output for small metadata examples, like the script above) and run generated data tests on every generated table. A bug in a template is a bug in every table, so the generator deserves more testing than any single model.

**Schema drift automation.** When a source adds a column, a metadata-driven pipeline can detect it (comparing the source schema with stored metadata), update the metadata in a pull request, and regenerate staging models and docs, keeping humans in the review loop rather than in the typing.

**AI-assisted generation.** Large language models are useful for drafting staging models, documentation and tests from schemas and sample data. Treat their output like any generated code: review it, keep the source of truth in metadata or version control, and let tests decide whether it is correct.

> **Interview:** "You need to onboard 300 source tables into a warehouse in a month. How?" Classify tables by pattern (append-only, upsert by key, full snapshot, SCD2), collect metadata automatically from source catalogs, generate staging models, loads and standard tests from templates, hand-write only the exceptions, and run the whole set through CI. Then spend the saved time on the business-logic marts.
:::

::: check
?? What is metadata-driven generation in a warehouse?
Describing each object (source, keys, columns, types, load pattern) as metadata and generating the DDL, load or merge code, tests and docs from templates, instead of hand-writing each object.

?? Why should you not edit generated code by hand?
The next generation overwrites the change, or the code drifts away from its metadata so nobody knows which is right. Change the metadata or the template instead.

?? What does the dbt-codegen `generate_source` macro produce?
A sources YAML block listing the tables (and optionally columns) of a schema, read from the warehouse's metadata, as a starting point for documentation and staging models.
:::

::: resources
- Docs: [Jinja and macros in dbt](https://docs.getdbt.com/docs/build/jinja-macros) · writing reusable SQL patterns as macros
- Repo: [dbt-codegen](https://github.com/dbt-labs/dbt-codegen) · macros that generate sources, base models and YAML from warehouse metadata
- Repo: [AutomateDV](https://github.com/Datavault-UK/automate-dv) · a dbt package that generates Data Vault 2.0 structures from metadata
- Docs: [Dataform overview](https://cloud.google.com/dataform/docs/overview) · SQLX, JavaScript generation and workflows for BigQuery
- Blog: [WhereScape resources](https://www.wherescape.com/resources/) · whitepapers and case studies on metadata-driven warehouse automation
- Docs: [dbt materializations](https://docs.getdbt.com/docs/build/materializations) · the built-in patterns generators usually target
:::

### Multi-Tenant Data Warehousing

::: plain
A multi-tenant warehouse is an apartment building. Many households (tenants: customers of a SaaS product, or business units) live under one roof and share the foundations, plumbing and staff, which is much cheaper than a house each. But every household needs its own locked door, nobody should hear the neighbours' parties, and the electricity bill must be split fairly. Designing for tenants means choosing how thick the walls are, how the doors are locked, and how each household's meter is read.
:::

A **tenant** is a customer (in a SaaS product) or an internal unit (a brand, a country, a department) whose data must be kept separate from others in a shared platform. Multi-tenant warehousing has four concerns: **isolation** (no tenant sees another's data), **performance isolation** (no tenant slows others down), **cost attribution** (who used what), and **lifecycle** (onboarding, schema changes and deletion per tenant). The customer-facing BI side (signed embedding, access filters in Looker) is covered in [Embedded Analytics & Multi-Tenancy](#analytics); this topic is about the warehouse underneath.

**Isolation models.**

| Model | BigQuery | Snowflake | Isolation | Overhead |
|---|---|---|---|---|
| Silo: platform unit per tenant | Project per tenant | Account per tenant (in an organization) | Strongest: separate IAM, billing, quotas, location | Highest: many objects, cross-tenant analysis hard |
| Bridge: schema per tenant | Dataset per tenant | Database or schema per tenant | Strong: grants per dataset, per-tenant retention | Schema changes repeated per tenant |
| Pool: shared tables | `tenant_id` column, row-level filtering | `tenant_id` column, row access policy | Depends on filters always being applied | Lowest: one table, easy cross-tenant analytics |

Most SaaS platforms mix them: pool for the long tail of small tenants, bridge or silo for large or regulated tenants who need their own location, keys or contracts.

**Pooled tables, done safely.** The shared table needs a `tenant_id` on every row and a physical layout that keeps each tenant's rows together:

```sql
CREATE TABLE saas.events (
  tenant_id  INT64 NOT NULL,
  event_date DATE  NOT NULL,
  event_ts   TIMESTAMP,
  user_id    STRING,
  event_type STRING
)
PARTITION BY event_date
CLUSTER BY tenant_id, event_type;   -- one tenant's rows sit in few blocks
```

Then enforce the filter in the warehouse, not only in applications. Two BigQuery patterns:

```sql
-- 1. Row access policy: members of a group see only their tenant's rows
CREATE ROW ACCESS POLICY tenant_101 ON saas.events
GRANT TO ('group:acme-analysts@example.com')
FILTER USING (tenant_id = 101);

-- 2. Authorized view with a mapping table: one view serves every tenant user
CREATE VIEW shared.events AS
SELECT e.*
FROM saas.events AS e
JOIN saas.tenant_users AS u ON u.tenant_id = e.tenant_id
WHERE u.user_email = SESSION_USER();
```

Consumers get access to the view's dataset only; the view is authorized to read `saas`. Policies scale poorly to thousands of tenants (one per tenant or group), while the mapping-table view scales with a single object. Snowflake expresses the second pattern as a **row access policy** attached to the column:

```sql
CREATE ROW ACCESS POLICY security.tenant_rows AS (t_id NUMBER) RETURNS BOOLEAN ->
  EXISTS (SELECT 1 FROM security.tenant_roles m
          WHERE m.role_name = CURRENT_ROLE() AND m.tenant_id = t_id);

ALTER TABLE saas.events ADD ROW ACCESS POLICY security.tenant_rows ON (tenant_id);
```

Row-level security in general (policies, policy tags, masking) is in [BigQuery security and governance](#bigquery).

**Performance isolation.** In a shared warehouse, one tenant's huge export can delay everyone else's dashboards ("noisy neighbour"). Tools:

- **BigQuery**: run each tier's queries from separate projects assigned to separate **reservations** (a premium tier with its own baseline slots, a shared pool for the rest), and set custom **quotas** on bytes scanned per day per project or user.
- **Snowflake**: separate **virtual warehouses** per tier or per large tenant, with resource monitors capping credits.
- **ClickHouse and others**: workload or resource groups, per-user query limits, and settings such as `max_execution_time`.

**Cost attribution.** Every query should carry the tenant. In BigQuery, set job labels (`bq query --label tenant:acme ...`, or `SET @@query_label = "tenant:acme";` in a session) and read them back from `INFORMATION_SCHEMA.JOBS`:

```sql
SELECT l.value AS tenant,
       SUM(j.total_bytes_billed) / POW(1024, 4) AS tib_billed,
       SUM(j.total_slot_ms) / 1000 / 3600       AS slot_hours
FROM `region-us`.INFORMATION_SCHEMA.JOBS AS j, UNNEST(j.labels) AS l
WHERE l.key = 'tenant'
  AND j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY tenant
ORDER BY slot_hours DESC;
```

With on-demand pricing the bill per query is bytes billed times the price per TiB; with reserved slots, a fair split is each tenant's share of slot time. A small allocation script shows both:

```python
from collections import defaultdict

PRICE_PER_TIB = 6.25        # on-demand list price used for the example, USD per TiB
TIB = 2 ** 40
jobs = [  # what INFORMATION_SCHEMA.JOBS returns: labels and bytes billed per query
    {"labels": {"tenant": "acme"},   "bytes_billed": 3 * TIB, "slot_ms": 9_000_000},
    {"labels": {"tenant": "acme"},   "bytes_billed": 1 * TIB, "slot_ms": 3_000_000},
    {"labels": {"tenant": "globex"}, "bytes_billed": 2 * TIB, "slot_ms": 12_000_000},
    {"labels": {},                   "bytes_billed": TIB // 2, "slot_ms": 1_000_000},
]

cost, slots = defaultdict(float), defaultdict(int)
for j in jobs:
    tenant = j["labels"].get("tenant", "UNLABELLED")
    cost[tenant] += j["bytes_billed"] / TIB * PRICE_PER_TIB
    slots[tenant] += j["slot_ms"]

total_slot_ms = sum(slots.values())
for t in sorted(cost):
    print(f"{t:<11} on-demand ${cost[t]:6.2f}   share of reserved slots {slots[t] / total_slot_ms:.0%}")
# UNLABELLED  on-demand $  3.12   share of reserved slots 4%
# acme        on-demand $ 25.00   share of reserved slots 48%
# globex      on-demand $ 12.50   share of reserved slots 48%
```

Note that acme scanned twice globex's bytes but used the same slot time, so the two pricing models attribute cost differently. Check the current price for your region on the pricing page, and track the unlabelled share: it should shrink towards zero. Storage can be attributed by tenant too, from table sizes (silo and bridge) or from bytes per `tenant_id` (pool). In Snowflake, query tags (`ALTER SESSION SET QUERY_TAG = 'tenant:acme'`) and the `QUERY_ATTRIBUTION_HISTORY` view in `ACCOUNT_USAGE` play the same role.

**Lifecycle.** Keep a **tenant registry** (ID, tier, isolation model, region, owner, status) and drive everything from it: Terraform creates datasets, reservations assignments and IAM for bridge and silo tenants; onboarding scripts insert mapping rows for pooled tenants. **Deletion** on contract end or a privacy request is easiest in silo and bridge models (drop the project or dataset); in a pool it means `DELETE ... WHERE tenant_id = ...` across every table, plus waiting out time travel and fail-safe windows before data is truly gone.

> **Pitfall:** Relying on every query remembering `WHERE tenant_id = ...`. One forgotten filter in one dashboard leaks data. Enforce isolation in the warehouse (row access policies, authorized views, separate datasets), and test it by querying as a tenant user and asserting that no other tenant's rows appear.

> **Pitfall:** Choosing dataset-per-tenant for tens of thousands of small tenants. Every schema change, view and permission is then multiplied by that number. Pool the small ones and reserve dedicated datasets for those who need them.

::: deeper
**Data residency.** A BigQuery dataset has one location, so tenants who require data to stay in a region need datasets (bridge) or projects (silo) in that region, and pipelines that never copy their data elsewhere. Pools are per region.

**Encryption per tenant.** Customer-managed encryption keys (Cloud KMS) can be set per dataset or table, so a bridge model can give each large tenant its own key, and revoking the key makes their data unreadable (crypto-shredding). In a pooled table all tenants share the table's key.

**Schema evolution across tenants.** Bridge and silo models drift: one tenant's dataset gets a column the others do not. Generate tenant datasets from one template (the Automation topic) and roll schema changes out to all of them through CI, with the tenant registry recording each tenant's schema version.

**Cross-tenant analytics.** The platform team often needs to analyse all tenants together (usage, benchmarks). In a pool this is a simple query; in bridge and silo models it needs `UNION ALL` views over many datasets or a separate, anonymised aggregate table, which is also the safe way to offer benchmark features ("you versus the median of similar tenants").

> **Interview:** "Design the warehouse for a SaaS product with 5,000 small customers and 20 enterprise customers." Pool the small tenants in shared tables clustered by `tenant_id`, with an authorized view or row access policy driven by a mapping table; give enterprise tenants their own datasets (with their own location and keys if required); separate reservations for enterprise and shared tiers; label every job with the tenant for cost attribution; drive onboarding, schema changes and deletion from a tenant registry.
:::

::: check
?? What are the three main isolation models, from strongest to cheapest?
Silo (a project or account per tenant), bridge (a dataset or schema per tenant) and pool (shared tables with a `tenant_id` column and row-level filtering).

?? Why cluster a pooled table by `tenant_id`?
So that each tenant's rows are stored in few blocks; queries filtered to one tenant then read only those blocks, which is cheaper and keeps tenants from paying to scan each other's data.

?? How can you attribute BigQuery compute cost to tenants in a shared project?
Label every job with the tenant (job labels or `@@query_label`) and aggregate `total_bytes_billed` (on-demand) or `total_slot_ms` (reservations) per label from `INFORMATION_SCHEMA.JOBS`.
:::

::: resources
- Docs: [Best practices for multi-tenant workloads on BigQuery](https://cloud.google.com/bigquery/docs/best-practices-for-multi-tenant-workloads-on-bigquery) · isolation models, reservations and quotas for SaaS on BigQuery
- Docs: [Introduction to BigQuery row-level security](https://cloud.google.com/bigquery/docs/row-level-security-intro) · row access policies, how they combine and their limits
- Docs: [Authorized views](https://cloud.google.com/bigquery/docs/authorized-views) · sharing filtered query results without granting access to base tables
- Docs: [Introduction to BigQuery labels](https://cloud.google.com/bigquery/docs/labels-intro) · labelling datasets, tables and jobs for cost attribution
- Docs: [Snowflake row access policies](https://docs.snowflake.com/en/user-guide/security-row-intro) · mapping-table driven row-level security in Snowflake
- Docs: [Managing accounts in your Snowflake organization](https://docs.snowflake.com/en/user-guide/organizations-manage-accounts) · creating and managing an account per tenant within one organization
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Kimball versus Inmon | Kimball: business-process stars with conformed dimensions, bottom-up; Inmon: integrated 3NF warehouse first, marts derived |
| Layers | Landing, raw (append-only), staging (typed, deduplicated), intermediate, marts, semantic; consumers read marts only |
| MPP skew | Busiest worker ÷ average; fix by filtering, isolating or salting the hot key |
| Columnar wins | Read only referenced columns, encode then compress, skip blocks with min/max, vectorized execution |
| Encodings | Dictionary for low cardinality, RLE for sorted runs, delta for timestamps, bit-packing for small integers |
| HTAP or CDC? | One app's live data: HTAP (AlloyDB columnar engine, TiFlash); many sources and history: Datastream into BigQuery |
| Snowflake warehouse | `CREATE WAREHOUSE w WAREHOUSE_SIZE='SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;` |
| Snowflake history and copies | `SELECT ... AT(OFFSET => -3600)`, `UNDROP TABLE t`, `CREATE TABLE t2 CLONE t` |
| ClickHouse table | `ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY (tenant_id, type, ts)`; batch inserts |
| DuckDB on files | `SELECT ... FROM read_parquet('path/*.parquet', hive_partitioning = true) GROUP BY ALL` |
| MotherDuck | `duckdb.connect("md:db")`; hybrid joins of local files with cloud tables; `CREATE SHARE` |
| Skipping structures | Zone maps for sorted ranges, bloom filters for IDs, bitmaps for low-cardinality combinations, search indexes for needles |
| Partition or cluster? | Partition by the date you filter and expire on; cluster by high-cardinality filter columns |
| Incremental aggregates | Store sums and counts (or sketches), never averages or distinct counts; merge deltas |
| Bad plans | Compare estimated and actual rows; refresh statistics; avoid functions on filter columns; declare keys |
| Semantic layer | Define metrics once (LookML, MetricFlow, Cube); route to pre-aggregations; enforce access in the layer |
| OLAP operations | Roll-up and drill-down change `GROUP BY` level; slice and dice add `WHERE`; pivot with `PIVOT` |
| Warehouse tests | Unit tests for logic in CI, data tests after build, contracts at boundaries, reconciliation to trusted totals |
| Automation | Metadata plus templates generate DDL, merges, tests and docs; never hand-edit generated code |
| Multi-tenant cost | Label every job with the tenant; sum bytes billed or slot time per label from `INFORMATION_SCHEMA.JOBS` |

### Practice

::: try
**1. Choose a layout.** A `page_views` table in BigQuery receives 200 GB per day and keeps 2 years. Almost every query filters on the last 1 to 30 days; many also filter on `site_id` (about 3,000 sites) and some on `user_id` (hundreds of millions). Data older than 2 years must be deleted. Propose partitioning and clustering, and estimate the fraction of the table a 7-day query reads.
:::

?? Answer to 1
Partition by `view_date` (daily) with a partition expiration of about 730 days, which handles deletion automatically, and `require_partition_filter = TRUE`. Cluster by `site_id, user_id`: site is filtered often and prunes first; user is high-cardinality and must not be a partition key. There are about 730 partitions, so a 7-day query reads about 7 / 730 ≈ 1% of the table before clustering prunes further.

::: try
**2. Find the skew.** A distributed join of `events` with `users` on `user_id` has one worker running for 40 minutes while the other 63 finish in under 2 minutes. What do you suspect, how do you confirm it with one query, and name two fixes.
:::

?? Answer to 2
Skew on the join key: one `user_id` (often `NULL`, `0`, `'unknown'` or a bot) holds a large share of the rows. Confirm with `SELECT user_id, COUNT(*) AS n FROM events GROUP BY user_id ORDER BY n DESC LIMIT 10`. Fixes: filter or separately handle the hot key (for example, rows with `NULL` user cannot match anyway, so exclude them before the join), or salt the key into sub-keys on the large side and replicate the matching `users` row once per salt value. Adaptive execution features that split skewed partitions also help when the engine has them.

::: try
**3. Distinct users per month from a daily aggregate.** You keep a daily table `agg_daily(day, site_id, users)` where `users` is `COUNT(DISTINCT user_id)` per day. A product manager wants distinct users per month. Why is `SUM(users)` wrong, and how would you change the daily table in BigQuery so the monthly figure can be computed from it?
:::

?? A solution to 3
`SUM(users)` counts a user once per day they were active, so it overstates monthly distinct users. Store a mergeable HyperLogLog sketch per day instead and merge the sketches per month (the result is an approximation with small, known error):

```sql
CREATE OR REPLACE TABLE mart.agg_daily
PARTITION BY day AS
SELECT DATE(event_ts) AS day, site_id,
       HLL_COUNT.INIT(user_id) AS users_sketch
FROM mart.fct_page_views
GROUP BY day, site_id;

SELECT DATE_TRUNC(day, MONTH) AS month, site_id,
       HLL_COUNT.MERGE(users_sketch) AS distinct_users
FROM mart.agg_daily
GROUP BY month, site_id;
```

::: try
**4. Write the failing test first.** A revenue model is `SELECT order_date, SUM(amount) AS revenue FROM orders WHERE status <> 'cancelled' GROUP BY order_date`. Orders with a `NULL` status are valid (not yet classified) and must count. Write a unit test in Python with `sqlite3` that exposes the bug, then fix the model.
:::

?? A solution to 4

```python
import sqlite3

MODEL = """
SELECT order_date, SUM(amount) AS revenue
FROM orders
WHERE status IS DISTINCT FROM 'cancelled'
GROUP BY order_date
"""   # with <> 'cancelled' the test prints: FAIL got [('2026-11-20', 40), ('2026-11-21', 25)]

def test_revenue_excludes_cancelled():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE orders (order_id, order_date, status, amount)")
    db.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", [
        (1, "2026-11-20", "paid", 40), (2, "2026-11-20", "cancelled", 99),
        (3, "2026-11-21", "paid", 25), (4, "2026-11-21", None, 10)])
    got = sorted(db.execute(MODEL).fetchall())
    expect = [("2026-11-20", 40), ("2026-11-21", 35)]
    print("PASS" if got == expect else f"FAIL got {got}")

test_revenue_excludes_cancelled()   # PASS
```

`NULL <> 'cancelled'` is unknown, not true, so the original filter drops order 4. `IS DISTINCT FROM` (supported by BigQuery, PostgreSQL and SQLite) treats `NULL` as a comparable value; `COALESCE(status, '') <> 'cancelled'` also works.

::: try
**5. Size a Snowflake warehouse.** A nightly job takes 24 minutes on a Small warehouse (2 credits per hour) and scales well. The team needs it to finish in under 10 minutes. Which size do you pick, and what happens to the credits used per run?
:::

?? Answer to 5
Medium would take about 12 minutes (too slow); Large (8 credits per hour) about 6 minutes. Credits stay roughly the same if scaling is close to linear: 2 × 24 / 60 = 0.8 credits on Small and 8 × 6 / 60 = 0.8 on Large. Set a short `AUTO_SUSPEND` so the larger warehouse stops billing as soon as the job ends, and check the query profile that it really scales (no spilling, no single-threaded steps).

### What comes next

You now know how analytical warehouses store, distribute, index and optimise data, how the main engines differ, and how to test, automate and share a warehouse safely. The next section, **Data lakes and lakehouses**, takes the same ideas to open files in object storage: Parquet and other formats, table formats such as Iceberg, Delta and Hudi that add transactions and time travel to files, and the catalogs and maintenance that let BigQuery, Spark, Trino and DuckDB share one copy of the data.
