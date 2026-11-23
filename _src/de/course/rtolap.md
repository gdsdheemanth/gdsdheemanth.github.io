This section covers **real-time OLAP** (RTOLAP) databases: Apache Druid, Apache Pinot, StarRocks and Apache Doris. They answer analytical questions (sums, counts, top-N, percentiles over billions of rows) in well under a second, on data that arrived seconds ago, for thousands of users at once. It is written for data engineers who are asked to build a live dashboard, an in-product analytics page or an operational monitoring tool, and must decide whether a warehouse is enough or a specialised engine is needed.

It assumes you know columnar storage, compression and partitioning (Data warehousing, Storage internals and hardware), how query engines scan, filter and aggregate (Query engine internals), and the basic idea of a log of events such as Kafka (introduced in Data engineering foundations and covered fully in the next section, Streaming systems).

**What you will be able to do afterwards**

- Explain what makes an RTOLAP engine different from a data warehouse, a stream processor and a search engine, and recognise the workloads that need one.
- Describe the indexes these engines rely on (dictionary encoding, forward and inverted indexes, Roaring bitmaps, sorted, range and star-tree indexes) and say which one serves which filter.
- Decide when to pre-aggregate with rollup or materialized views, and what you give up when you do.
- Design for updates and duplicates: Pinot upserts and dedup, StarRocks primary key tables, and patterns for engines without updates.
- Describe the architecture of Druid, Pinot and StarRocks, and choose between them for a given use case.
- Plan, run and monitor an RTOLAP cluster on Google Cloud: replication, tiered storage, query routing and capacity.

**The big picture.** A warehouse such as BigQuery is built for a few analysts running heavy, varied queries over all of history. A stream processor such as Flink is built to compute a fixed answer continuously. RTOLAP engines sit between the two. They ingest events straight from a stream, make each event queryable within seconds, and store it in columnar **segments** covered by many **indexes**, so a typical query touches only a tiny fraction of the data. The price is that you must design the table for its queries in advance: which columns are filtered, which are aggregated, how time is bucketed, which values are pre-aggregated. Every topic in this section is a variation on that bargain: do work at ingestion time so that queries are cheap and fast at serving time.

[[fig:rtolap_place]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Concepts

The four topics in this group are engine-neutral. They explain the problem RTOLAP solves and the three techniques every engine uses to solve it: indexes that turn filters into cheap bit operations, pre-aggregation that shrinks data before queries see it, and update handling for streams that deliver the same record more than once or change it later.

Once these ideas are clear, the engines in the next group become easy to compare, because each one is a particular choice of index types, pre-aggregation options and update model.

### Real-Time OLAP Concepts & Landscape

::: plain
A data warehouse is a library: it holds everything, and a researcher can ask any question, but each question takes a while and only a few people use it at once. A real-time OLAP engine is a departures board at an airport: it shows a fixed set of views over data that changed seconds ago, and thousands of people glance at it at the same time and expect an instant answer. Both deal with the same flights; they are built for different readers.
:::

**OLAP** (online analytical processing) means queries that scan many rows and aggregate them: "orders per minute by city for the last hour". **Real-time** OLAP adds three requirements at once:

1. **Freshness**: an event is queryable within seconds of happening (ingestion-to-query latency), not after the next hourly batch load.
2. **Low query latency**: tens to a few hundred milliseconds at the 99th percentile, because a person or a program is waiting.
3. **High concurrency**: hundreds to many thousands of queries per second, because the queries come from an application's users, not from a few analysts.

A warehouse can meet one or two of these, rarely all three at a reasonable cost. A BigQuery query has planning and scheduling overhead measured in hundreds of milliseconds, and on-demand pricing charges every query for the bytes it scans, so 5,000 dashboard queries per second would be both slow and very expensive. RTOLAP engines avoid this by keeping data on local disks and in memory on long-running servers, building indexes at ingestion, and answering each query from a small number of segments.

**Where RTOLAP fits:**

| System type | Typical latency | Freshness | Concurrency | Query flexibility | Examples |
|---|---|---|---|---|---|
| Data warehouse | Seconds to minutes | Minutes to hours (seconds with streaming ingestion) | Tens | Any SQL, big joins | BigQuery, Snowflake |
| Real-time OLAP | 10 to 500 ms | Seconds | Thousands | Mostly filters, group-bys, top-N on one wide table | Druid, Pinot, StarRocks, Doris, ClickHouse |
| Streaming SQL / stream processor | Continuous | Sub-second | Results served elsewhere | Queries fixed in advance | Flink SQL, Materialize, RisingWave |
| Key-value or serving store | Single-digit ms | Seconds | Very high | Lookups by key only | Bigtable, Redis |
| Search engine | 10 to 500 ms | Seconds | High | Text relevance, some aggregations | Elasticsearch, OpenSearch |

**Typical use cases:**

- **User-facing analytics**: analytics shown inside a product to its customers. LinkedIn built Pinot for features such as "Who viewed your profile"; Uber uses Pinot for restaurant dashboards in Uber Eats. Every user is a query, so concurrency is enormous and each query filters to one member or one restaurant.
- **Operational dashboards on live data**: orders per minute, delivery delays, payment failures by provider, refreshed every few seconds for operations teams.
- **Observability and telemetry**: playback quality, ad impressions, network events. Netflix has described using Druid for real-time playback metrics.
- **Anomaly detection and alerting** that query the last few minutes repeatedly.

**The common design.** Despite their differences, the engines share a shape:

1. **Ingest** from a stream (Kafka, Pub/Sub through a connector, Kinesis-compatible systems) and from batch files (Parquet in Cloud Storage).
2. Build **segments**: immutable, columnar chunks of rows, usually partitioned by time, with dictionaries and indexes built for each column.
3. Keep a copy of every segment in **deep storage** (Cloud Storage, HDFS) for durability, and load segments onto **serving nodes** with local SSDs and plenty of memory.
4. A **broker** receives each query, works out which segments can contain matching rows, sends the query to the servers holding them (scatter), and merges partial results (gather).

> **Pitfall:** RTOLAP engines are not general warehouses. Large joins between big tables, complex multi-step SQL and ad hoc exploration over all of history are slower, harder or unsupported (Pinot and Druid have added multi-stage engines that make joins possible, but they are not their strength). Keep the warehouse for that work and serve the narrow, hot, high-concurrency queries from RTOLAP.

> **Tip:** Before choosing an engine, write down the queries, the freshness target, the expected queries per second and the p99 latency target. If the honest answers are "a few analysts, a 15-minute delay is fine", BigQuery with BI Engine or materialized views is simpler and cheaper.

On Google Cloud there is no first-party RTOLAP database, so teams either run Druid, Pinot or StarRocks on GKE (with Cloud Storage as deep storage and Google Managed Service for Apache Kafka as the stream), or use a vendor's managed service. BigQuery can get close for some needs: streaming through the Storage Write API makes rows queryable in seconds, BI Engine caches hot data in memory for dashboards, and continuous queries process new rows as they arrive. It still cannot serve thousands of cheap, sub-100 ms queries per second the way an RTOLAP cluster does.

::: deeper
**ClickHouse** is often grouped with these engines. It is a columnar database with very fast scans and a rich SQL dialect, strongest when queries are varied and analysts write them; its sparse primary index and data-skipping indexes are coarser than the per-value bitmap indexes of Druid and Pinot, which favours scan-heavy over needle-in-a-haystack workloads. The MergeTree family and its materialized views are covered in the Query engine internals section.

**Why concurrency changes the design.** If each query scans 1 GB and you have 1,000 queries per second, you need 1 TB/s of scan bandwidth. No affordable cluster has that. The only way out is to make each query read far less: indexes (read only matching rows), pre-aggregation (read fewer rows), partition and segment pruning (open fewer segments), and caching. That arithmetic, not raw scan speed, is what separates RTOLAP from warehouses.

> **Interview:** "A product team wants a dashboard inside the app showing each merchant its sales in the last 24 hours, updated within 10 seconds, for 200,000 merchants. Warehouse or RTOLAP?" RTOLAP. Freshness of seconds, thousands of concurrent queries and a filter on one merchant per query are the classic signature. Partition and sort by merchant, roll up to the minute, and keep the raw events in the lake for everything else.
:::

::: check
?? Name the three requirements that together define real-time OLAP.
Freshness of seconds (ingestion-to-query latency), query latency of tens to hundreds of milliseconds, and high concurrency (hundreds to thousands of queries per second).

?? Why is a warehouse with on-demand pricing a poor fit for user-facing analytics?
Each query has planning overhead of hundreds of milliseconds, and every query is billed for the bytes it scans, so thousands of queries per second would be slow and very expensive.

?? What does the broker do in the common RTOLAP design?
It receives a query, prunes to the segments that can contain matching rows, sends sub-queries to the servers holding them, and merges their partial results.
:::

::: resources
- Book: [Building Real-Time Analytics Systems](https://www.oreilly.com/library/view/building-real-time-analytics/9781098138783/) · Mark Needham; end-to-end design with Kafka and Pinot, written for engineers new to the field
- Docs: [Apache Druid architecture](https://druid.apache.org/docs/latest/design/architecture/) · a clear example of the segment, broker and deep storage design
- Docs: [Apache Pinot concepts](https://docs.pinot.apache.org/basics/concepts) · tables, segments, tenants and the cluster model
- Article: [Real-time analytics: a definitive guide](https://www.tinybird.co/blog-posts/real-time-analytics-a-definitive-guide) · definitions, use cases and the trade-offs between freshness, latency and concurrency
- Blog: [Choosing a database for real-time analytics](https://imply.io/blog/choosing-a-database-for-real-time-analytics-druid-and-pinot/) · a comparison of Druid and Pinot from a Druid vendor (read with that bias in mind)
- Docs: [BigQuery BI Engine](https://docs.cloud.google.com/bigquery/docs/bi-engine-intro) · Google Cloud's in-memory acceleration layer, the warehouse-side alternative
:::

### RTOLAP Indexing Deep Dive

::: plain
Imagine a filing cabinet of a billion receipts and the question "how many were paid by card in Paris yesterday?". Reading every receipt is hopeless. Instead, you keep a list for every city ("Paris: receipts 3, 17, 42...") and for every payment method, and you answer by overlapping two lists. RTOLAP engines keep such lists for every value of every important column, packed so tightly that overlapping them takes microseconds.
:::

Every RTOLAP engine stores data by column, and for each column it can build several structures. Knowing them is how you design a fast table.

**Dictionary encoding.** A string column with values `IN, US, IN, DE` is stored as a sorted **dictionary** (`DE=0, IN=1, US=2`) plus a column of small integer **ids** (`1, 2, 1, 0`). Ids need only as many bits as the dictionary size requires (2 bits for 3 values), compress well, and can be compared as integers. Low and medium cardinality columns (countries, devices, statuses) benefit most.

**Forward index.** The forward index answers "what is the value in row 7?". It is the column itself: the dictionary ids (or raw values for high-cardinality or numeric columns, which skip the dictionary) in row order. Projections and aggregations read it.

**Inverted index.** The inverted index answers the opposite question: "which rows have the value `IN`?". For each dictionary id it stores the set of row ids, usually as a **bitmap** with one bit per row. Filters then become bit operations: `AND` for `AND`, `OR` for `OR` and `IN (...)`, `NOT` for negation. The engine only touches the forward index for the rows that survive.

[[fig:rtolap_bitmap]]

This tiny Python model shows the idea, using Python integers as bit sets:

```python
# A tiny bitmap index: one bitmap (a Python int used as a bit set) per value
rows = ["IN", "US", "IN", "DE", "US", "IN", "IN", "DE"]   # country of rows 0..7
devices = ["ios", "web", "web", "ios", "ios", "ios", "web", "web"]

def bitmap_index(column):
    index = {}
    for row_id, value in enumerate(column):
        index[value] = index.get(value, 0) | (1 << row_id)
    return index

country_idx = bitmap_index(rows)
device_idx = bitmap_index(devices)

def row_ids(bits):
    return [i for i in range(bits.bit_length()) if bits >> i & 1]

print(row_ids(country_idx["IN"]))                          # [0, 2, 5, 6]
match = country_idx["IN"] & device_idx["ios"]              # WHERE country='IN' AND device='ios'
print(row_ids(match))                                      # [0, 5]
either = country_idx["US"] | country_idx["DE"]             # WHERE country IN ('US', 'DE')
print(row_ids(either), bin(either).count("1"))             # [1, 3, 4, 7] 4
```

`COUNT(*)` with a filter is just the number of set bits in the result bitmap, which CPUs count with a single instruction per 64 bits.

**Roaring bitmaps.** A plain bitmap for a 5-million-row segment is 625 KB per value. For a column with 10,000 values most bitmaps are almost empty, which wastes space; for a value present in most rows, a list of row ids would be huge. **Roaring bitmaps**, used by Druid, Pinot, StarRocks, Doris, ClickHouse, Lucene and Spark, solve both. A Roaring bitmap splits the 32-bit row id space into chunks of 65,536 ids (by the high 16 bits) and stores each non-empty chunk in the cheapest of three **containers**:

| Container | Used when | Size |
|---|---|---|
| Array | At most 4,096 values in the chunk | 2 bytes per value (sorted 16-bit integers) |
| Bitmap | More than 4,096 values | Always 8 KB (65,536 bits) |
| Run | Values form long consecutive runs | 4 bytes per run (start and length), plus a small header |

The 4,096 threshold is not arbitrary: 4,096 values × 2 bytes = 8 KB, exactly the size of a bitmap container. A run container is chosen after an optimisation pass when it is smaller than both. This sketch picks the container the same way:

```python
# Which Roaring container would hold one 65,536-value chunk?
def container(values):
    n = len(values)
    runs = 0
    for i, v in enumerate(values):
        if i == 0 or v != values[i - 1] + 1:
            runs += 1
    sizes = {"array": 2 * n, "bitmap": 8192, "run": 2 + 4 * runs}
    if n > 4096:
        del sizes["array"]                 # arrays are only used up to 4,096 values
    kind = min(sizes, key=sizes.get)
    return kind, sizes[kind]

print(container([3, 900, 41000]))                          # ('array', 6)
print(container(list(range(0, 65536, 2))))                 # ('bitmap', 8192)
print(container(list(range(1000, 61000))))                 # ('run', 6)
```

Operations between containers have specialised algorithms (array-array intersection by galloping search, bitmap-bitmap by 64-bit word `AND`, and so on), so Roaring is usually both smaller and faster than older compressed bitmaps such as Concise or WAH.

**Sorted index.** If a segment is physically sorted by a column, each value occupies one contiguous range of rows, and the "index" is just a start and end row per value. Pinot lets you choose one sorted column per table; Druid sorts rows within a segment by time and then by dimension order. Sorting by the most selective, most filtered column (for example `merchant_id` in a per-merchant dashboard) gives the best filter speed and compression.

**Other indexes:**

| Index | Serves | Engines |
|---|---|---|
| Range index | `amount > 100`, timestamp ranges on high-cardinality numbers | Pinot; zone maps (min/max per block) in StarRocks, Doris and Druid's numeric columns |
| Bloom filter | Equality on high-cardinality columns (`user_id = ...`): "definitely not in this segment" | Pinot, StarRocks, Doris |
| Text index | Full-text search, `TEXT_MATCH` (Lucene) | Pinot, Doris (inverted index), Druid (limited) |
| JSON index | Filters on fields inside JSON columns | Pinot; Druid nested columns |
| Geospatial (H3) | Points within a distance or region | Pinot |
| Star-tree | Pre-aggregated answers for group-bys on chosen dimensions | Pinot |

**Star-tree index.** Pinot's star-tree is an index that stores pre-aggregated values. You choose an order of dimensions (`dimensionsSplitOrder`) and the aggregations to keep (`functionColumnPairs` such as `SUM__impressions`). Pinot builds a tree in which each level splits on one dimension; at each level there is also a special **star node** that aggregates over all values of that dimension. Splitting stops when a node has at most `maxLeafRecords` rows (10,000 by default), which bounds the work for any query. A query whose filters and group-by columns are all in the tree, and whose aggregations are in the list, walks the tree and reads at most a few thousand pre-aggregated records instead of millions of raw rows.

```json
"starTreeIndexConfigs": [{
  "dimensionsSplitOrder": ["country", "device", "browser"],
  "skipStarNodeCreationForDimensions": [],
  "functionColumnPairs": ["COUNT__*", "SUM__impressions", "SUM__clicks"],
  "maxLeafRecords": 10000
}]
```

> **Pitfall:** Every index costs ingestion time, memory and disk. An inverted index on a column with millions of distinct values (user ids, request ids) is large and rarely helps; use a bloom filter, a sorted column or partitioning instead. Index the columns your queries actually filter on, and check with the engine's query statistics that they are used.

> **Tip:** Cardinality is the first number to know for each column. Low cardinality: dictionary plus inverted index. High cardinality and filtered by equality: bloom filter, or make it the sort or partition key. High cardinality and only displayed: raw forward index, no dictionary.

::: deeper
**Why bitmaps beat B-trees here.** A B-tree index returns row pointers one by one and is ideal for fetching a few rows. Analytical filters often match millions of rows and combine several predicates. Bitmaps combine predicates with word-level CPU operations (and SIMD instructions in modern implementations) and feed directly into vectorized aggregation. The result of a filter is itself a bitmap that can be cached and reused.

**64-bit Roaring.** Segments rarely exceed a few million rows, so 32-bit Roaring is enough inside a segment. For larger id spaces (for example a bitmap of user ids across a whole table, used for exact distinct counts) there are 64-bit variants, and StarRocks and Doris expose a `BITMAP` column type with functions such as `BITMAP_UNION` and `BITMAP_COUNT` for exact, mergeable distinct counts over integer ids.

**Pruning before indexing.** Indexes work inside a segment. Before that, brokers skip whole segments: by time range (every segment covers an interval), by partition (if the table is partitioned by `merchant_id`, a query for one merchant touches one partition), by min/max metadata and by bloom filters stored at segment level. Good pruning plus good indexes is the difference between touching 3 segments and touching 3,000.

> **Interview:** "Why is the Roaring array container limited to 4,096 values?" Because at 2 bytes per value, 4,096 values take 8 KB, exactly the size of a bitmap container for a 65,536-value chunk. Beyond that a bitmap is smaller and faster.
:::

::: check
?? What is the difference between a forward index and an inverted index?
A forward index maps row to value (the column in row order), used to read values. An inverted index maps value to the set of rows containing it, used to evaluate filters.

?? A Roaring chunk contains 10,000 values scattered at random. Which container is used, and how big is it?
A bitmap container (more than 4,096 values, and random values give many runs), always 8 KB.

?? Which index would you choose for frequent equality filters on `user_id`, a column with 50 million distinct values?
Not an inverted index. Make `user_id` the sorted column or partition key if it is the main filter; otherwise use a bloom filter to skip segments that cannot contain the value.
:::

::: resources
- Paper: [Consistently faster and smaller compressed bitmaps with Roaring](https://arxiv.org/abs/1603.06549) · Lemire et al.; the three container types, run containers and benchmarks
- Paper: [Better bitmap performance with Roaring bitmaps](https://arxiv.org/abs/1402.6407) · Chambi et al.; the original Roaring paper with the array/bitmap design
- Docs: [Roaring bitmaps](https://roaringbitmap.org/about/) · overview and the list of systems and languages that use Roaring
- Docs: [Pinot star-tree index](https://docs.pinot.apache.org/basics/indexing/star-tree-index) · how the tree is built and when queries can use it
- Docs: [Pinot inverted index](https://docs.pinot.apache.org/basics/indexing/inverted-index) · bitmap and sorted inverted indexes with configuration examples
- Docs: [Druid segments](https://druid.apache.org/docs/latest/design/segments/) · dictionary, column data and bitmap structures for each string column
:::

### Rollup, Pre-Aggregation & Cube Strategies

::: plain
If a café records every coffee sold with the exact second, but the manager only ever asks "how many coffees per hour, per type?", the café can keep a tally sheet instead of the receipts: one line per hour and coffee type with a count. The sheet is a hundred times shorter and answers the manager's questions instantly. The catch: once the receipts are thrown away, nobody can ask "how many were sold between 10:03 and 10:07?".
:::

**Pre-aggregation** means computing aggregates before queries ask for them. RTOLAP engines offer it at three points:

1. **At ingestion (rollup)**: rows are aggregated as they are written, and only the aggregates are stored.
2. **As an index or materialized view**: raw rows are kept, and a pre-aggregated structure is maintained next to them (Pinot's star-tree, StarRocks and Doris materialized views).
3. **At query time with caching**: results are cached at the broker or per segment.

**Druid rollup.** In a Druid ingestion spec you declare **dimensions** (columns to group by), **metrics** (aggregations to keep) and a **query granularity** that truncates timestamps. With `rollup: true`, Druid merges all rows with the same truncated timestamp and the same dimension values into one row. Here is the effect in plain Python:

```python
# Rollup at ingestion: truncate time to the minute, group by dimensions, keep metrics
from collections import defaultdict

events = [  # (timestamp, page, country, bytes)
    ("2026-11-22T10:00:05", "/home", "IN", 1200),
    ("2026-11-22T10:00:17", "/home", "IN", 800),
    ("2026-11-22T10:00:42", "/cart", "US", 300),
    ("2026-11-22T10:00:59", "/home", "IN", 1000),
    ("2026-11-22T10:01:03", "/home", "IN", 500),
    ("2026-11-22T10:01:30", "/cart", "US", 700),
]

rolled = defaultdict(lambda: {"count": 0, "sum_bytes": 0})
for ts, page, country, nbytes in events:
    minute = ts[:16]                      # queryGranularity = MINUTE
    row = rolled[(minute, page, country)]
    row["count"] += 1
    row["sum_bytes"] += nbytes

for key, metrics in sorted(rolled.items()):
    print(key, metrics)
print(len(events), "->", len(rolled), "rows")
# ('2026-11-22T10:00', '/cart', 'US') {'count': 1, 'sum_bytes': 300}
# ('2026-11-22T10:00', '/home', 'IN') {'count': 3, 'sum_bytes': 3000}
# ('2026-11-22T10:01', '/cart', 'US') {'count': 1, 'sum_bytes': 700}
# ('2026-11-22T10:01', '/home', 'IN') {'count': 1, 'sum_bytes': 500}
# 6 -> 4 rows
```

The equivalent part of a Druid spec:

```json
"dataSchema": {
  "dataSource": "page_views",
  "timestampSpec": {"column": "ts", "format": "iso"},
  "dimensionsSpec": {"dimensions": ["page", "country"]},
  "metricsSpec": [
    {"type": "count", "name": "count"},
    {"type": "longSum", "name": "sum_bytes", "fieldName": "bytes"},
    {"type": "HLLSketchBuild", "name": "users_hll", "fieldName": "user_id"}
  ],
  "granularitySpec": {"segmentGranularity": "DAY", "queryGranularity": "MINUTE", "rollup": true}
}
```

The **rollup ratio** (input rows divided by stored rows) depends on the cardinality of the dimension combination. Ratios of 10 to 100 are common for metrics data; adding a high-cardinality dimension such as `user_id` can drop it to about 1, which means rollup does nothing but cost ingestion time. Measure it: with a `count` metric, `SELECT SUM("count") * 1.0 / COUNT(*) FROM page_views` gives the ratio.

Note the `count` metric: after rollup, `COUNT(*)` counts stored rows, not events. Queries must use `SUM("count")`. And distinct counts cannot be computed from rolled-up rows unless you store a mergeable **sketch** (HyperLogLog or Theta sketches from Apache DataSketches), which gives approximate distinct counts that combine across rows, segments and time.

Druid's rollup is **perfect** when every duplicate combination ends up in one row (batch ingestion with hash or range partitioning) and **best-effort** when ingestion runs in parallel tasks that each roll up their own rows (streaming), so the same combination may appear in several rows. Query results are the same either way; only storage differs, and **compaction** can merge rows later.

**Pinot** can aggregate metrics during real-time ingestion (`aggregationConfigs` in the table's ingestion config, for functions such as `SUM`, `MIN`, `MAX` and `COUNT`), and its **star-tree index** (previous topic) is the more flexible option: raw rows stay available, and only queries that match the tree use the pre-aggregates.

**StarRocks and Doris** offer three tools:

- The **Aggregate table** type, where value columns have an aggregation function (`SUM`, `MAX`, `REPLACE`, `BITMAP_UNION`, `HLL_UNION`) and rows with the same key are merged, like Druid rollup.
- **Synchronous materialized views** (rollups) on a single table, updated in the same transaction as each load and chosen automatically by the optimizer.
- **Asynchronous materialized views** across several tables (joins included), refreshed on a schedule or manually, which the optimizer can use to **rewrite** queries transparently.

```sql
-- StarRocks: an asynchronous materialized view refreshed every minute
CREATE MATERIALIZED VIEW orders_per_minute
REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)
AS
SELECT date_trunc('minute', order_ts) AS minute, city_id,
       COUNT(*) AS orders, SUM(amount) AS revenue
FROM orders
GROUP BY date_trunc('minute', order_ts), city_id;
```

**Cubes.** All of this descends from the OLAP **cube**: precompute aggregates for every combination of dimensions. A full cube over *n* dimensions has 2^n group-by combinations (10 dimensions give 1,024), which explodes quickly. Star-tree indexes and selective materialized views are practical compromises: pre-aggregate the combinations that queries actually use.

| Approach | Raw rows kept | New questions possible later | Storage | Query speed |
|---|---|---|---|---|
| No pre-aggregation | Yes | Yes | Largest | Slowest |
| Star-tree or materialized view | Yes | Yes | Raw plus a fraction | Fast for matching queries |
| Ingestion rollup | No | Only along stored dimensions and granularity | Smallest | Fastest |

> **Pitfall:** Rollup is irreversible inside the engine. If product later asks for per-second data or a dimension you dropped, you need the raw events. Keep them in the lake (Parquet or Iceberg in Cloud Storage, or BigQuery) so you can re-ingest with a new spec.

> **Tip:** A good pattern is two datasources: a rolled-up one for dashboards with long retention, and a raw one with short retention (a few days) for drill-down and debugging.

::: deeper
**Granularity versus segment granularity.** In Druid, `queryGranularity` truncates timestamps inside rows (the finest time you can query), while `segmentGranularity` decides the time span of each segment (for example `DAY` or `HOUR`), which controls how many segments a time-filtered query opens and how big each one is.

**Sketch accuracy.** An HLL sketch with the default size gives a relative error of about 1 to 2%; Theta sketches support set operations (intersection, difference), useful for "users who did A and B"; quantile sketches (KLL) give approximate percentiles. They trade exactness for mergeability, which is exactly what rolled-up and distributed data needs.

**Materialized view maintenance cost.** A synchronous view adds work to every load. An asynchronous view adds refresh jobs and can serve slightly stale data; StarRocks lets you control how stale a rewrite may be. Count these costs before adding views for every dashboard tile.

> **Interview:** "After enabling rollup, `COUNT(*)` on the datasource dropped by 40 times. Is data lost?" No. `COUNT(*)` now counts rolled-up rows. The event count is `SUM` of the ingestion-time `count` metric.
:::

::: check
?? After Druid rollup with a `count` metric, how do you count the original events?
`SUM("count")`. `COUNT(*)` counts stored, rolled-up rows.

?? Why does adding `user_id` as a dimension usually ruin the rollup ratio?
Almost every row then has a unique combination of timestamp bucket and dimensions, so few rows merge and the ratio falls towards 1.

?? How can you keep approximate distinct user counts in a rolled-up table?
Store a mergeable sketch metric (HyperLogLog or Theta) of `user_id` at ingestion and merge the sketches at query time.
:::

::: resources
- Docs: [Druid data rollup](https://druid.apache.org/docs/latest/ingestion/rollup/) · rollup, perfect and best-effort modes, and how to measure the ratio
- Docs: [Druid schema design tips](https://druid.apache.org/docs/latest/ingestion/schema-design/) · choosing dimensions, metrics and sketches
- Docs: [StarRocks asynchronous materialized views](https://docs.starrocks.io/docs/using_starrocks/async_mv/Materialized_view/) · creation, refresh strategies and query rewrite
- Docs: [StarRocks synchronous materialized views](https://docs.starrocks.io/docs/using_starrocks/Materialized_view-single_table/) · single-table rollups maintained with each load
- Docs: [Apache DataSketches](https://datasketches.apache.org/) · the HLL, Theta and quantile sketches used by Druid and others
- Article: [What makes Apache Pinot fast, chapter II](https://startree.ai/resources/what-makes-apache-pinot-fast-chapter-ii/) · ingestion-time aggregation and the star-tree in Pinot
:::

### Upsert & Deduplication in RTOLAP

::: plain
Imagine a parcel tracking board. The courier sends "parcel 42: picked up", then "parcel 42: in transit", then "parcel 42: delivered", and sometimes sends the same message twice because the radio was patchy. Most RTOLAP storage is like writing in pen in a notebook: you can only add lines, never cross out. **Upsert** means "show only the latest line for each parcel"; **deduplication** means "ignore a line I have already written".
:::

RTOLAP engines store immutable segments because that makes indexes, compression and replication simple. But real data changes: orders move from `created` to `paid` to `refunded`; change data capture (CDC) streams from databases carry updates and deletes; and **at-least-once** delivery (the default in most pipelines, see Streaming systems) means the same event may arrive twice. Each engine handles this differently.

**Apache Pinot: upsert and dedup on real-time tables.** Pinot keeps every incoming row in its segments but maintains, on each server, a **primary key map** from key to the location of the latest row, and a per-segment bitmap of **valid document ids**. Queries automatically filter by that bitmap, so they see only the latest version of each key. This model shows the mechanics:

```python
# Full upsert as Pinot does it: append every row, keep a primary-key map,
# and mark only the latest version of each key as valid.
stream = [  # (order_id, status, updated_at) in arrival order
    ("o1", "created", 100),
    ("o2", "created", 105),
    ("o1", "paid", 130),
    ("o1", "created", 100),   # duplicate redelivered by an at-least-once producer
    ("o2", "cancelled", 160),
]

docs = []          # the append-only segment
latest = {}        # primary key -> (doc_id, comparison value)
valid = set()      # validDocIds

for order_id, status, updated_at in stream:
    doc_id = len(docs)
    docs.append((order_id, status, updated_at))
    old = latest.get(order_id)
    if old is None or updated_at >= old[1]:      # newer or equal comparison value wins
        if old is not None:
            valid.discard(old[0])
        latest[order_id] = (doc_id, updated_at)
        valid.add(doc_id)

print(len(docs), "docs stored")                       # 5 docs stored
print([docs[d] for d in sorted(valid)])               # [('o1', 'paid', 130), ('o2', 'cancelled', 160)]
```

The **comparison column** (the time column by default, or a version column you name) decides which row is "latest", so out-of-order arrivals do not overwrite newer data. Configuration lives in the schema and table config:

```json
"primaryKeyColumns": ["order_id"],
"upsertConfig": {
  "mode": "PARTIAL",
  "comparisonColumns": ["updated_at"],
  "partialUpsertStrategies": {"status": "OVERWRITE", "item_count": "INCREMENT"},
  "deleteRecordColumn": "is_deleted"
},
"routing": {"instanceSelectorType": "strictReplicaGroup"}
```

- **FULL** mode replaces the whole row with the latest one. **PARTIAL** mode merges column by column with a strategy each (`OVERWRITE`, `IGNORE`, `INCREMENT`, `APPEND`, `UNION`, `MAX`, `MIN`), which suits streams that send only changed fields.
- The input topic **must be partitioned by the primary key**, so every version of a key lands on the same server, and queries must use `strictReplicaGroup` routing so they read a consistent replica.
- **Dedup** (`dedupConfig`) is a lighter feature for append-only data: Pinot keeps the first row for each primary key and drops later ones, ideal for removing redeliveries.

The cost is memory: the primary key map holds an entry for every live key on every server. Hundreds of millions of keys need careful sizing, a metadata time-to-live (TTL) for keys that stop changing, or off-heap storage of the map.

**StarRocks and Doris: primary key (merge-on-write) tables.** A StarRocks **Primary Key table** keeps an index from primary key to row location. When a load writes a row whose key exists, StarRocks marks the old row as deleted in a **delete vector** and writes the new one: "delete plus insert" at write time, called **merge-on-write**. Reads simply skip deleted rows, so queries stay as fast as on append-only tables. Doris's **Unique Key** model offers the same merge-on-write behaviour (the default for new tables in recent versions).

```sql
-- StarRocks
CREATE TABLE orders (
  order_id   BIGINT NOT NULL,
  status     VARCHAR(20),
  amount     DECIMAL(12, 2),
  updated_at DATETIME
)
PRIMARY KEY (order_id)
DISTRIBUTED BY HASH (order_id);
```

Loads can carry an `__op` column (0 for upsert, 1 for delete) to apply CDC streams, support **partial updates** of chosen columns, and support **conditional updates** (`merge_condition` on a column such as `updated_at`) so an older event cannot overwrite a newer one.

The older alternative, **merge-on-read**, keeps all versions and merges them during queries (and background compaction). Writes are cheap, reads are slower. ClickHouse's `ReplacingMergeTree` works this way: duplicates disappear only when parts merge, or when you add `FINAL` to the query at extra cost.

**Apache Druid: no row-level upsert.** Druid segments are immutable and versioned per time chunk. You have three tools:

- **Reingest** a time range with SQL-based ingestion (`REPLACE INTO ... OVERWRITE WHERE __time >= ... `), which builds new segments that **overshadow** the old ones. Good for corrections and daily restatements.
- **Query-time latest value** with `LATEST_BY(status, updated_at)` grouped by key, or ingestion-time `stringLast` and `longLast` aggregators with rollup.
- **Avoid duplicates at the source**: Druid's Kafka ingestion is exactly-once with respect to Kafka offsets, so the duplicates you see come from upstream producers; deduplicate in the stream processor (Flink, Dataflow) before Druid.

| Engine | Model | Where duplicates disappear | Main cost |
|---|---|---|---|
| Pinot | Primary key map + valid doc bitmap | At ingestion, per server | Memory for keys; key-partitioned topic required |
| StarRocks / Doris | Merge-on-write primary key | At write, via delete vectors | Write amplification, index memory |
| ClickHouse ReplacingMergeTree | Merge-on-read | At background merge or `FINAL` | Possibly stale duplicates; slower `FINAL` queries |
| Druid | Immutable, versioned time chunks | On reingestion or at query time | Reprocessing, query-time aggregation |

> **Pitfall:** Pinot upsert silently gives wrong answers if the Kafka topic is not partitioned by the primary key: two versions of a key land on different servers, each thinks its own row is the latest, and both are counted. Partition by key in the producer, and keep the partition count fixed.

> **Pitfall:** Exactly-once ingestion from Kafka does not remove duplicates already present in the topic. If a producer retried without idempotence, the topic holds two copies, and the engine faithfully ingests both.

::: deeper
**Upsert and segment lifecycle.** In Pinot, when a consuming segment commits, the primary key map must still point at the right rows across committed segments; on server restart the map is rebuilt from segments or from persisted snapshots of `validDocIds` (`enableSnapshot`), which makes restarts much faster. Background **compaction** tasks run by Pinot Minions can rewrite segments to drop invalid rows and reclaim space.

**Upsert on offline data.** Pinot's upsert applies to real-time tables; offline (batch) tables are replaced segment by segment instead. For a table that is both corrected in batch and updated in real time, design carefully which side owns which time range.

**Choosing a comparison column.** Use a version or update timestamp produced by the source system (for CDC, the database commit sequence or timestamp), not the ingestion time, otherwise a late-arriving old version wins.

> **Interview:** "How would you build a live order-status dashboard from a CDC stream?" Partition the CDC topic by `order_id`; ingest into a Pinot real-time table with full or partial upsert on `order_id`, comparison column set to the source commit timestamp and a delete column for deletes, or into a StarRocks primary key table with `__op` and a merge condition. Keep the raw CDC log in the lake for replay.
:::

::: check
?? Why must a Pinot upsert table's input topic be partitioned by the primary key?
So that all versions of a key are consumed by the same server, whose primary key map can then decide which row is the latest. Otherwise several servers each keep their own "latest" version and queries count duplicates.

?? What is the difference between merge-on-write and merge-on-read?
Merge-on-write resolves updates at write time (marking old rows deleted, for example with delete vectors), so reads are fast. Merge-on-read stores all versions and resolves them during reads or later compaction, so writes are cheap and reads cost more.

?? Druid has no row-level upsert. How do you correct a day of wrong data?
Reingest that time range (for example with `REPLACE INTO ... OVERWRITE WHERE`), producing new segments with a newer version that overshadow the old ones.
:::

::: resources
- Docs: [Pinot stream ingestion with upsert](https://docs.pinot.apache.org/basics/data-import/upsert) · full and partial modes, comparison columns, deletes and requirements
- Docs: [Pinot stream ingestion with dedup](https://docs.pinot.apache.org/basics/data-import/dedup) · dropping repeated primary keys on append-only tables
- Docs: [StarRocks primary key table](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/) · merge-on-write, the primary key index and partial updates
- Docs: [Doris unique key model](https://doris.apache.org/docs/table-design/data-model/unique) · merge-on-write and merge-on-read in Apache Doris
- Article: [Introduction to upserts in Apache Pinot](https://startree.ai/resources/introduction-to-upserts-in-apache-pinot/) · a walkthrough of how upserts work and when to use them
- Docs: [ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree) · the merge-on-read alternative and its `FINAL` trade-off
:::

## Engines

With the concepts in place, this group looks at the engines one by one. Druid and Pinot are the two classic RTOLAP systems: both were built for event data with a time column, both store immutable segments with bitmap indexes, both ingest directly from Kafka. They differ in architecture details, index options and update support. StarRocks and Doris come from a different family: MPP (massively parallel processing) SQL databases with a MySQL-compatible interface, strong joins and a cost-based optimizer, which added real-time ingestion and updates.

Read each engine's topics with the same questions in mind: what are the node roles, what is a segment (or tablet), how does fresh data become queryable, and where do the indexes come from?

### Apache Druid — Architecture & Internals

::: plain
Druid is like a newspaper archive with a live newsroom. Reporters (ingestion tasks) write today's stories and can answer questions about them right away. Each evening the stories are bound into a volume (a segment), a copy goes to the deep vault (deep storage), and librarians (historicals) put copies on their shelves. A front desk (the broker) takes your question, sends it to whichever reporters and librarians have the relevant volumes, and combines their answers.
:::

Apache Druid started at Metamarkets in 2011 for ad-tech analytics and became an Apache top-level project in 2019. A Druid cluster is a set of services, usually grouped into three server types:

| Server type | Services | Job |
|---|---|---|
| Master | **Coordinator**, **Overlord** | Coordinator assigns segments to historicals, applies retention rules, balances and compacts. Overlord accepts ingestion tasks and assigns them to workers |
| Query | **Broker**, **Router** | Broker plans queries, routes them to data nodes and merges results. Router is an optional gateway and hosts the web console |
| Data | **Historical**, **MiddleManager** (or **Indexer**) | Historicals serve published segments. MiddleManagers run ingestion tasks in child processes called **peons** (the Indexer runs them as threads in one process) |

Three external dependencies hold state:

- **Deep storage**: the durable copy of every segment. On Google Cloud, a Cloud Storage bucket through the `druid-google-extensions` extension (`druid.storage.type=google`). Historicals can always reload from it, so losing a historical loses no data.
- **Metadata store**: a relational database (PostgreSQL or MySQL, for example Cloud SQL) holding the list of published segments, ingestion specs, rules and Kafka offsets.
- **ZooKeeper**: service discovery, leader election and segment announcements (a Kubernetes-based discovery extension can replace it on Kubernetes).

[[fig:rtolap_druid]]

**Segments.** A **datasource** (Druid's table) is split by time into **time chunks** according to the segment granularity (say, one day), and each chunk into one or more **segments**, each a self-contained columnar file with its own dictionaries and bitmaps. Segments are identified by datasource, interval, version and partition number. A newer version for the same interval **overshadows** older ones, which is how reingestion replaces data atomically. The documentation recommends segments of roughly 5 million rows (a few hundred MB).

**Real-time ingestion from Kafka.** You submit a **supervisor** spec. The supervisor runs on the Overlord, starts indexing tasks that read assigned Kafka partitions, and manages their lifecycle:

1. Each task reads events, keeps them in an in-memory, queryable index, and periodically persists them to disk. Events are queryable **as soon as they are read**, because tasks answer broker queries for their data.
2. After `taskDuration` (one hour by default) the task stops reading, builds final segments, pushes them to deep storage and **publishes** them in the metadata store, together with the Kafka offsets they cover, in one transaction. That transaction is what makes ingestion exactly-once with respect to Kafka.
3. The Coordinator sees the new segments and tells historicals to load them. Once a historical announces it is serving a segment, the task drops it: this is **handoff**.

```json
{
  "type": "kafka",
  "spec": {
    "ioConfig": {
      "type": "kafka",
      "topic": "page_views",
      "consumerProperties": {"bootstrap.servers": "bootstrap.events.europe-west1.managedkafka.my-proj.cloud.goog:9092"},
      "taskCount": 4,
      "replicas": 2,
      "taskDuration": "PT1H",
      "useEarliestOffset": false
    },
    "dataSchema": {"dataSource": "page_views", "...": "dimensions, metrics, granularity"},
    "tuningConfig": {"type": "kafka", "maxRowsPerSegment": 5000000}
  }
}
```

`taskCount` sets read parallelism (it cannot usefully exceed the number of Kafka partitions), and `replicas` runs duplicate tasks so that a failed worker does not make fresh data unavailable.

**Batch ingestion.** Native batch (`index_parallel`) and **SQL-based ingestion** on the multi-stage query (MSQ) engine read files from Cloud Storage and write segments:

```sql
REPLACE INTO page_views OVERWRITE WHERE __time >= TIMESTAMP '2026-11-01' AND __time < TIMESTAMP '2026-11-02'
SELECT TIME_PARSE(ts) AS __time, page, country, bytes
FROM TABLE(EXTERN('{"type":"google","uris":["gs://raw/page_views/2026-11-01/part-0.json"]}',
                  '{"type":"json"}',
                  '[{"name":"ts","type":"string"},{"name":"page","type":"string"},{"name":"country","type":"string"},{"name":"bytes","type":"long"}]'))
WHERE TIME_PARSE(ts) >= TIMESTAMP '2026-11-01' AND TIME_PARSE(ts) < TIMESTAMP '2026-11-02'
PARTITIONED BY DAY
CLUSTERED BY page
```

**Queries.** **Druid SQL** is parsed and planned with Apache Calcite and translated into native JSON queries (timeseries, topN, groupBy, scan). The broker uses the segment timeline to find which segments overlap the query's time range, sends sub-queries to the historicals and tasks that serve them, and merges results. Each data node scans its segments in parallel using bitmap indexes, then returns partial aggregates.

```sql
SELECT TIME_FLOOR(__time, 'PT5M') AS bucket, country, SUM("count") AS views
FROM page_views
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR AND page = '/checkout'
GROUP BY 1, 2
ORDER BY views DESC
LIMIT 20
```

> **Pitfall:** Always filter on `__time`. The broker prunes segments by time first; a query without a time filter fans out to every segment in the datasource.

> **Tip:** Run the Coordinator and Overlord together (`druid.coordinator.asOverlord.enabled`) in small and medium clusters; it is simpler and is a common deployment.

::: deeper
**Query from deep storage.** With the MSQ engine, Druid can also query segments that are not loaded on any historical, reading them directly from deep storage. This makes long, cheap retention possible (years of data in Cloud Storage) while only recent data sits on expensive local disks; such queries are slower and meant for occasional use.

**Joins and lookups.** Druid supports joins (best when the right side is small or a **lookup**, an in-memory key-value map such as country code to name) and the MSQ engine can run larger shuffled joins, but Druid is happiest with one wide, denormalised datasource per use case. Denormalise in the stream processor or at ingestion.

**Paper and history.** The SIGMOD 2014 paper describes the original design: real-time nodes, historical nodes, brokers and coordinators. MiddleManagers and the Kafka indexing service replaced real-time nodes, and the core ideas (immutable versioned segments, bitmap indexes, deep storage) remain.

> **Interview:** "What happens to Druid queries if every historical crashes?" Fresh data still served by ingestion tasks stays queryable. Published segments become unavailable until historicals restart and reload them from deep storage; no data is lost, because deep storage holds every published segment.
:::

::: check
?? What is handoff in Druid real-time ingestion?
The moment a segment built by an ingestion task is published, loaded by a historical and announced, after which the task stops serving it.

?? Why is Druid's Kafka ingestion exactly-once with respect to Kafka offsets?
Tasks publish the segments and the Kafka offsets they cover in one metadata-store transaction, so a restarted task resumes from exactly the committed offsets without gaps or duplicates.

?? What happens when you reingest a day with a new version?
New segments for that interval are published with a newer version and overshadow the old ones; queries switch atomically, and the old segments can be dropped.
:::

::: resources
- Docs: [Druid architecture](https://druid.apache.org/docs/latest/design/architecture/) · services, server types and external dependencies
- Docs: [Druid Kafka ingestion](https://druid.apache.org/docs/latest/ingestion/kafka-ingestion/) · supervisor specs, task settings and offset handling
- Docs: [Druid SQL-based ingestion](https://druid.apache.org/docs/latest/multi-stage-query/) · `INSERT`, `REPLACE` and the multi-stage query engine
- Paper: [Druid: a real-time analytical data store](http://static.druid.io/docs/druid.pdf) · Yang et al., SIGMOD 2014; the original architecture
- Docs: [Google Cloud Storage extension](https://druid.apache.org/docs/latest/development/extensions-core/google/) · using Cloud Storage as deep storage and for ingestion
- Article: [Druid architecture and concepts](https://imply.io/druid-architecture-concepts/) · an illustrated tour from the company founded by Druid's creators
:::

### Apache Druid — Indexing & Storage Internals

::: plain
A Druid segment is like a well-indexed reference book for one slice of time. Each column is a separate chapter. For every word that appears in a chapter (each value of a string column) there is an index at the back listing the pages where it occurs. Numbers are stored compactly, and the dates on every page are written so that a whole chapter of times takes very little room.
:::

Each segment is a directory of columns (packed into a small number of files) plus metadata. Inside, Druid treats three kinds of column differently.

**The timestamp column `__time`.** Every row has one. It is stored as a compressed long column, and rows in a segment are sorted by time first by default, then by the dimensions in declared order. Sorting makes runs of similar values, so compression works better, and the time range of a segment is known from its metadata, so time filters prune segments before any column is read.

**String dimensions.** A string dimension has three structures, exactly as in the indexing topic:

1. A **dictionary** mapping each distinct value to an integer id. Values are sorted, so the id order matches the value order.
2. The **column data**: the id of each row's value, packed with as few bits as needed and compressed in blocks (LZ4 by default).
3. A **bitmap** per value, listing the rows that contain it. Druid uses Roaring bitmaps by default (Concise is the older alternative).

For a `country` column with 200 values in a 5-million-row segment, that is a dictionary of 200 strings, 5 million 8-bit ids, and 200 Roaring bitmaps. A filter `country = 'IN'` looks up `IN` in the dictionary (binary search), takes its bitmap and never reads rows that do not match.

**Multi-value dimensions.** A row can hold several values in one string dimension (for example `tags: ["sale", "mobile"]`). The row then appears in the bitmap of each value. A filter `tags = 'sale'` matches the row, and `GROUP BY tags` produces one group per value, which can surprise people: one row contributes to several groups.

**Numeric columns and metrics.** Long, float and double columns are stored as compressed blocks without a dictionary or bitmaps; filters on them scan the column (with block-level skipping where possible). Metric columns produced by rollup (sums, counts) are numeric columns; sketch metrics are stored as complex columns holding serialised sketches.

**Nested JSON columns.** Druid can store nested data as a `COMPLEX<json>` column. Internally each nested path is stored as its own column with its own dictionary and indexes, so `JSON_VALUE(attrs, '$.device.os') = 'ios'` can use indexes like a normal dimension.

**Segment granularity and partitioning.** Two settings decide how data is cut into segments:

- **Segment granularity** (`HOUR`, `DAY`, `MONTH`): the time span of a chunk. Too fine and you get many tiny segments with high per-segment overhead; too coarse and recent data is mixed with old data in huge segments.
- **Secondary partitioning** inside a time chunk: **dynamic** (by row count, used by streaming ingestion), **hash** (by hash of dimensions), or **range** (sorted ranges of one or more dimensions). Range partitioning by `customer_id` lets the broker skip every segment whose range cannot contain the queried customer.

| Setting | Effect | Rule of thumb |
|---|---|---|
| `segmentGranularity` | Time span per segment | Choose so a typical chunk holds one or a few segments of a few million rows |
| `queryGranularity` | Truncates `__time` in stored rows | As coarse as queries allow, to improve rollup |
| `partitionsSpec` | Secondary partitioning | Range on the most common filter for batch data |
| `maxRowsPerSegment` | Upper bound for dynamic partitioning | Around 5 million |

**Compaction.** Streaming ingestion produces segments per task and per period, often small and with best-effort rollup. **Compaction** (manual tasks or automatic compaction configured on the Coordinator) rewrites a time chunk into fewer, larger segments, can change partitioning to range, and can apply a coarser query granularity to older data. It runs in the background and publishes a new version that overshadows the old segments.

```json
{
  "dataSource": "page_views",
  "skipOffsetFromLatest": "PT2H",
  "tuningConfig": {
    "partitionsSpec": {"type": "range", "partitionDimensions": ["country"], "targetRowsPerSegment": 5000000}
  },
  "granularitySpec": {"segmentGranularity": "DAY"}
}
```

`skipOffsetFromLatest` keeps compaction away from the most recent two hours, which are still being written.

> **Pitfall:** Small segments are the most common Druid performance problem. Thousands of segments of a few thousand rows each mean thousands of tiny scans per query and a huge amount of metadata for the Coordinator and brokers. Configure automatic compaction from day one.

> **Tip:** Declare dimensions in order of how often they are filtered and their cardinality (low first). Since rows are sorted by time and then by dimension order, a good order improves compression and helps the most common filters.

::: deeper
**Memory mapping.** Historicals memory-map segment files from their local segment cache. The operating system's page cache then decides what stays in RAM, which is why historical sizing is mostly about free memory for the page cache, not JVM heap. A query on data that is not in the page cache pays disk reads; on local SSD that is fine, on network disks it is slow.

**Bitmap operations at query time.** Druid evaluates filters into a bitmap, then the query engine (vectorized for most query types) iterates over set bits to read column values in batches. Pre-filtered bitmaps can be cached, and `NOT` filters are computed as complements, which can be expensive on very large segments.

**Dictionary encoding and `LIKE`.** Because the dictionary is sorted, a prefix filter (`page LIKE '/cart%'`) is a range in the dictionary; Druid unions the bitmaps of that range. An infix filter (`'%cart%'`) must check every dictionary value, but still never touches the rows directly.

> **Interview:** "Why do high-cardinality string dimensions hurt Druid?" Each distinct value adds a dictionary entry and a bitmap, rollup stops working, and segments grow. Consider not indexing them, storing them only as needed, or moving them to a separate raw datasource.
:::

::: check
?? Which three structures does Druid store for a string dimension?
A sorted dictionary of values, the column of encoded ids per row, and a bitmap index per value.

?? What does automatic compaction fix, and why does it skip the latest data?
It merges small, best-effort segments into fewer, larger, better-partitioned ones. It skips the latest period (`skipOffsetFromLatest`) because ingestion tasks are still writing segments for it.

?? Why does a `GROUP BY` on a multi-value dimension return more total counts than there are rows?
A row with several values contributes to the group of each value, so it is counted once per value.
:::

::: resources
- Docs: [Druid segments](https://druid.apache.org/docs/latest/design/segments/) · the segment file format and per-column structures
- Docs: [Druid partitioning](https://druid.apache.org/docs/latest/ingestion/partitioning/) · time chunks, secondary partitioning and sort order
- Docs: [Druid compaction](https://druid.apache.org/docs/latest/data-management/compaction/) · manual and automatic compaction and their settings
- Docs: [Druid multi-value dimensions](https://druid.apache.org/docs/latest/querying/multi-value-dimensions/) · how multi-value strings are stored, filtered and grouped
- Docs: [Druid query processing](https://druid.apache.org/docs/latest/querying/query-processing/) · how brokers and data nodes prune and scan segments
- Repo: [Apache Druid source](https://github.com/apache/druid) · the `processing` module holds the column and bitmap code
:::

### Apache Pinot — Architecture & Internals

::: plain
Pinot is a fast-food kitchen built for queues. Orders (queries) come in at a counter (the broker), which knows exactly which cooks (servers) have which ingredients (segments) and sends each cook just their part. A manager (the controller) decides which cook keeps which ingredients and replaces cooks who go home. Fresh ingredients arrive on a conveyor belt (Kafka) and can be used the moment they land.
:::

Apache Pinot was built at LinkedIn around 2014 for user-facing analytics, open-sourced in 2015, and became an Apache top-level project in 2021. StarTree, founded by its creators, sells a managed version. A Pinot cluster has four component types:

- **Controller**: manages the cluster through **Apache Helix**, which stores cluster state in ZooKeeper. It holds table configs and schemas, assigns segments to servers, runs the segment completion protocol for real-time tables and handles uploads of batch segments.
- **Broker**: receives queries, uses a **routing table** (which server holds which segment) to choose servers, prunes segments, scatters the query and merges results.
- **Server**: hosts segments and runs queries on them. Real-time servers also consume from the stream.
- **Minion**: runs background tasks: building segments from batch files, merging and rolling up small segments, purging records, moving data from real-time to offline tables.

[[fig:rtolap_pinot]]

**Tables.** A Pinot table has a **schema** (dimensions, metrics, date-time columns, primary keys) and a **table config** (indexes, replication, retention, ingestion). There are two physical table types:

- **OFFLINE** tables receive segments built in batch (by Minion tasks or a Spark job) from files in Cloud Storage. Each push can replace segments.
- **REALTIME** tables consume from Kafka (or Kinesis, Pulsar and others) partition by partition.

A **hybrid** table is an offline and a real-time table with the same name. The broker splits each query at a **time boundary**: older data from the offline table, recent data from the real-time table, so batch corrections and streaming freshness combine without double counting. The boundary is derived from the latest time covered by offline segments.

**Real-time segment lifecycle.** For each Kafka partition and replica, a server keeps a **consuming segment**: a mutable, in-memory segment that is queryable immediately. When it reaches a flush threshold (row count, time or size), the **segment completion protocol** runs: the controller picks one replica as the committer; that replica builds the immutable segment (with all indexes), uploads it to **deep store** (Cloud Storage on Google Cloud), and the controller records the end offset. The other replicas either build an identical segment from the same offsets or download the committed one. A new consuming segment starts from the next offset.

```json
{
  "tableName": "orders",
  "tableType": "REALTIME",
  "segmentsConfig": {"timeColumnName": "order_ts", "replicasPerPartition": "2", "retentionTimeUnit": "DAYS", "retentionTimeValue": "30"},
  "tableIndexConfig": {
    "sortedColumn": ["restaurant_id"],
    "invertedIndexColumns": ["city", "status"],
    "bloomFilterColumns": ["order_id"],
    "streamConfigs": {
      "streamType": "kafka",
      "stream.kafka.topic.name": "orders",
      "stream.kafka.broker.list": "bootstrap.events.europe-west1.managedkafka.my-proj.cloud.goog:9092",
      "stream.kafka.consumer.type": "lowlevel",
      "realtime.segment.flush.threshold.rows": "0",
      "realtime.segment.flush.threshold.segment.size": "300M",
      "realtime.segment.flush.threshold.time": "6h"
    }
  }
}
```

Setting the row threshold to `0` lets Pinot size segments by the target segment size instead, which avoids tiny or huge segments when the data rate changes.

**Indexes.** Pinot has the widest index menu of the engines in this section: forward (dictionary or raw), sorted (one column per table), inverted (bitmap), range, bloom filter, text (Lucene and a native text index), FST and regex, JSON, geospatial (H3), timestamp, vector (HNSW, for similarity search) and the star-tree. Each is configured per column. Choosing them is most of Pinot tuning.

**Queries.** Pinot SQL is parsed with Apache Calcite. The original **single-stage engine** is a scatter-gather design: the broker sends the whole query to each server, each server filters and aggregates its segments, and the broker merges. It is extremely fast for filter-aggregate-top-N queries and supports only limited joins (lookup joins against small **dimension tables** replicated to every server). The **multi-stage engine** (covered in the next topic) adds distributed joins and window functions.

```sql
SELECT city, COUNT(*) AS orders, SUM(amount) AS revenue
FROM orders
WHERE restaurant_id = 8812 AND order_ts > ago('PT1H')
GROUP BY city
ORDER BY revenue DESC
LIMIT 10
```

Because `restaurant_id` is the sorted column, the server finds that restaurant's rows as one contiguous range in each segment, and with partition-aware routing the broker sends the query only to the servers owning that restaurant's partition.

> **Pitfall:** `replicasPerPartition` of 1 means a single server failure makes fresh data unavailable until another server re-consumes it. Use at least 2 for anything users see.

> **Tip:** Use **tenants** (server and broker tags) to isolate workloads: a noisy internal dashboard should not share servers with the customer-facing table.

::: deeper
**Helix state model.** Each segment has an ideal state (which servers should host it, and whether online or consuming) and an external view (what servers report). The controller writes ideal states; servers transition their segments to match; brokers build routing tables from the external view. Most operational questions ("why is this segment not queryable?") are answered by comparing the two.

**Segment pruning at the broker.** Brokers can prune by time, by partition (when the table declares a partition function matching the Kafka partitioner) and, on servers, by bloom filters and column min/max values. With **replica-group** instance assignment, each query goes to one group of servers that together hold a full copy, so it touches fewer servers and tail latency improves.

**Upsert placement.** Upsert tables need every version of a key on the same server, so they rely on Kafka partitioning and strict replica-group routing, as described in the upsert topic.

> **Interview:** "How does a Pinot hybrid table avoid counting the same events twice?" The broker splits the query at the time boundary: rows up to the boundary come from the offline table and rows after it from the real-time table, even though the real-time table still holds older data until retention removes it.
:::

::: check
?? What does a Pinot Minion do?
It runs background tasks such as building segments from batch data, merging and rolling up small segments, purging records and moving data from real-time to offline tables.

?? How do replicas of a real-time segment end up identical?
Through the segment completion protocol: the controller chooses one replica to commit and records the end offset; other replicas build from the same offsets or download the committed segment from deep store.

?? Why does choosing `restaurant_id` as the sorted column speed up per-restaurant queries?
Rows for each restaurant are contiguous in every segment, so the filter becomes a start and end row range and the scan reads only that range.
:::

::: resources
- Docs: [Pinot architecture](https://docs.pinot.apache.org/basics/architecture) · controller, broker, server, minion and how queries flow
- Docs: [Pinot tables](https://docs.pinot.apache.org/basics/components/table) · offline, real-time and hybrid tables and the table config
- Docs: [Pinot stream ingestion from Kafka](https://docs.pinot.apache.org/basics/data-import/pinot-stream-ingestion/import-from-apache-kafka) · stream configs and consumption settings
- Paper: [Pinot: Realtime OLAP for 530 Million Users](https://dl.acm.org/doi/10.1145/3183713.3190661) · Im et al., SIGMOD 2018; LinkedIn's design and production experience
- Article: [What makes Apache Pinot fast, chapter 1](https://startree.ai/resources/what-makes-apache-pinot-fast-chapter-1/) · segments, indexes and pruning explained by the creators' company
- Repo: [Apache Pinot source](https://github.com/apache/pinot) · code, quickstarts and Helm charts for Kubernetes
:::

### Apache Pinot — Advanced Features

::: plain
The basic Pinot kitchen makes one kind of dish very fast: filter, add up, rank. The advanced features add special stations: one that can combine two dishes (joins), one that finds things near a place on a map, one that searches text, and one that reaches inside JSON packages. Each station needs its own equipment (an index), set up before the orders arrive.
:::

**The multi-stage query engine.** Pinot 1.0 added a **multi-stage engine** (often called v2) beside the original single-stage engine. The broker compiles the query (with Calcite) into a plan of **stages**; stages run on servers and exchange data through **mailboxes** over the network, much like Spark or Trino stages exchange shuffled data. This enables:

- Distributed **joins** between large tables (hash-distributed or broadcast), including semi- and anti-joins.
- **Window functions** (`ROW_NUMBER`, `RANK`, `SUM(...) OVER (...)`).
- Subqueries, set operations and more complete SQL.

You enable it per query or in clients and the query console:

```sql
SET useMultistageEngine = true;

SELECT o.city, r.cuisine, COUNT(*) AS orders
FROM orders o
JOIN restaurants r ON o.restaurant_id = r.restaurant_id
WHERE o.order_ts > ago('PT1H')
GROUP BY o.city, r.cuisine
ORDER BY orders DESC
LIMIT 20;
```

Leaf stages still use Pinot's indexes, so filtering stays fast; the extra cost is the data exchanged between stages. For user-facing, high-QPS queries keep to the single-stage engine and a denormalised table; use the multi-stage engine for richer internal analytics.

**Geospatial queries with H3.** Pinot stores points as a geometry or geography type and supports `ST_` functions (`ST_Point`, `ST_Distance`, `ST_Contains`, `ST_Within`). An **H3 index** (H3 is Uber's hierarchical hexagonal grid) maps each point to hexagons at chosen resolutions, so a query like "drivers within 2 km" first selects candidate hexagons from the index and computes exact distances only for their points.

```json
"fieldConfigList": [{
  "name": "location_st_point",
  "encodingType": "RAW",
  "indexTypes": ["H3"],
  "properties": {"resolutions": "5"}
}]
```

```sql
SELECT COUNT(*) FROM drivers
WHERE ST_Distance(location_st_point, ST_Point(-122.4194, 37.7749, 1)) < 2000
```

**Text search.** A **text index** (built on Lucene) supports `TEXT_MATCH(column, 'query')` with terms, phrases, wildcards and boolean operators, useful for log lines or product descriptions. A simpler **native text index** and an **FST index** (for `REGEXP_LIKE` on low-cardinality columns) cover lighter needs.

**JSON indexes.** Events often carry a JSON column with fields that vary. A **JSON index** flattens each document into path-value pairs and builds inverted indexes over them, so `JSON_MATCH` filters are fast without declaring a column for every field:

```sql
SELECT COUNT(*) FROM events
WHERE JSON_MATCH(attributes, '"$.device.os" = ''ios'' AND "$.app.version" = ''5.2''')
```

For a few hot fields, it is better to extract them into real columns at ingestion with **ingestion transforms** (`jsonPathString(attributes, '$.device.os')`), which gives dictionary encoding, the full index menu and simpler SQL.

**Time boundary handling.** For hybrid tables, the broker computes the time boundary from offline segments and rewrites each query into two: `WHERE time <= boundary` on the offline table and `WHERE time > boundary` on the real-time table. The practical consequences:

- Offline pushes must cover complete periods (for example whole days); a partial push moves the boundary and real-time data for that period is hidden.
- The real-time table must retain at least a few periods beyond the boundary, or there will be a gap.
- The **RealtimeToOfflineSegmentsTask** (run by Minions) can build offline segments from real-time ones automatically, which keeps the boundary moving without a separate batch pipeline.

**Kafka ingestion features.** Beyond basic consumption: **ingestion transforms** and filters (drop or derive columns at ingestion), **complex type handling** (flatten nested records, unnest arrays), schema-registry-aware Avro and Protobuf decoders, **pauseless consumption** settings to avoid freshness dips during segment commits, and consumption from several topics or clusters. Partition counts should match the parallelism you need, since each partition is consumed by one server per replica.

> **Pitfall:** Indexes cannot be used by a query that wraps the column in a function the index does not understand. `WHERE LOWER(city) = 'paris'` cannot use the inverted index on `city`. Normalise values at ingestion instead.

> **Tip:** Pinot's query response includes statistics such as `numDocsScanned`, `numEntriesScannedInFilter`, `numSegmentsQueried` and `numSegmentsPruned`. If entries scanned in filter is high, an index is missing or unused.

::: deeper
**Engine choice per query.** The single-stage engine has very low overhead: one network round trip from broker to servers and back. The multi-stage engine adds stage scheduling and data exchange, so its latency floor is higher, and memory per query is larger. Teams often route by workload: application queries to the single-stage path with strict timeouts, analyst queries to the multi-stage path with a separate broker tenant.

**Vector search.** Recent Pinot versions add a vector index (HNSW) for approximate nearest-neighbour queries on embedding columns, which lets one engine combine filters and similarity search; check the version notes for supported distance functions.

> **Interview:** "Your Pinot query filters on a JSON field and is slow. What do you do?" Check whether a JSON index exists on that column and whether the query uses `JSON_MATCH` rather than `JSON_EXTRACT_SCALAR` in the filter. For a field used often, extract it into its own column with an ingestion transform and give it an inverted or sorted index.
:::

::: check
?? What does the multi-stage engine add, and what does it cost?
Distributed joins, window functions and fuller SQL, by running stages that exchange data between servers. It costs higher latency and memory per query than the single-stage scatter-gather engine.

?? How does an H3 index speed up a distance query?
It maps points to hexagonal cells, so the query first selects candidate cells near the target and computes exact distances only for points in those cells.

?? What goes wrong if an offline push for a hybrid table covers only half a day?
The time boundary moves forward based on offline segments, so queries take that whole period from the offline table, and the missing half day disappears from results.
:::

::: resources
- Docs: [Pinot multi-stage query engine](https://docs.pinot.apache.org/reference/multi-stage-engine) · architecture, supported SQL and limits
- Docs: [Pinot geospatial support](https://docs.pinot.apache.org/basics/indexing/geospatial-support) · types, `ST_` functions and the H3 index
- Docs: [Pinot text search support](https://docs.pinot.apache.org/basics/indexing/text-search-support) · the Lucene text index and `TEXT_MATCH`
- Docs: [Pinot JSON index](https://docs.pinot.apache.org/basics/indexing/json-index) · `JSON_MATCH` and how documents are flattened
- Docs: [Pinot time boundary](https://docs.pinot.apache.org/basics/components/table/time-boundary) · how hybrid queries are split
- Blog: [Pinot multi-stage query engine](https://startree.ai/blog/multi-stage-query-engine) · the design of the v2 engine from the team that built it
:::

### StarRocks / Doris — MPP Real-Time OLAP

::: plain
Druid and Pinot are specialists: brilliant at one kind of question on one big table. StarRocks and Doris are more like a full restaurant kitchen that also serves fast: they speak ordinary SQL (they even pretend to be MySQL, so existing tools connect), handle joins between many tables well, and still load fresh data within seconds and update rows in place.
:::

**Apache Doris** began at Baidu as Palo, was donated to Apache and became a top-level project in 2022. **StarRocks** forked from it in 2020 (first as DorisDB), rewrote large parts (a fully vectorized engine, a new optimizer) and is now a Linux Foundation project, with CelerData selling a managed version. Doris has also continued to evolve quickly (VeloDB sells its managed version). Both share the same basic architecture.

**Frontend and backend.**

- **FE (frontend)**, written in Java: speaks the MySQL protocol, holds metadata, parses and plans queries, schedules execution and coordinates loads. FEs run as a **leader** plus **followers** (which replicate metadata and can be elected leader) and optional **observers** (read-only replicas that scale query planning).
- **BE (backend)**, written in C++: stores data and executes query fragments. In the **shared-nothing** mode, each table is split into **partitions** (usually by date) and **buckets** (tablets, by hash of a key), and each tablet is replicated on three BEs by default.
- **Shared-data** mode (StarRocks 3.0+, Doris 3.0+ "compute-storage decoupled"): data lives in object storage (Cloud Storage on Google Cloud, through its S3-compatible interface or a native connector where supported) with a local cache, and stateless **CN** (compute node) or compute groups scale up and down independently.

[[fig:rtolap_starrocks]]

**Execution.** Queries run as MPP fragments across BEs. StarRocks's engine is fully **vectorized** (operators work on column batches with SIMD instructions) and uses a **pipeline execution** model that splits fragments into pipelines scheduled on a fixed pool of threads, which keeps cores busy under high concurrency. Runtime filters (bloom or min/max filters built from the small side of a join and pushed to the scan of the large side) and **colocate joins** (tables bucketed identically on the join key, so joins need no network exchange) make joins on star schemas fast.

**Optimizer.** StarRocks has a **cost-based optimizer** in the Cascades style, using table and column statistics (collected automatically) to choose join order, join distribution (broadcast, shuffle, colocate, bucket shuffle) and aggregation strategies. This is the main reason StarRocks and Doris cope with normalised schemas where Druid and Pinot expect a single denormalised table.

**Table types (data models).**

| StarRocks table type | Doris model | Behaviour |
|---|---|---|
| Duplicate Key | Duplicate | Keeps every row; sort key only (logs, events) |
| Aggregate | Aggregate | Merges rows with the same key using per-column aggregate functions (rollup) |
| Unique Key | Unique (merge-on-read) | Keeps the latest row per key, resolved at read and compaction |
| Primary Key | Unique (merge-on-write) | Upserts and deletes resolved at write; fast reads |

**Loading.** Data arrives through several paths: **Stream Load** (HTTP push of a file or batch, transactional), **Routine Load** (continuous consumption from Kafka, managed by the FE), **Broker Load** and `INSERT INTO ... SELECT FROM FILES(...)` for files in Cloud Storage, and connectors for Flink and Spark. Each load is a transaction that becomes visible atomically, typically within seconds for streaming loads.

```sql
-- StarRocks: continuous ingestion from Kafka into a primary key table
CREATE ROUTINE LOAD shop.orders_load ON orders
COLUMNS (order_id, status, amount, updated_at)
PROPERTIES ("format" = "json", "desired_concurrent_number" = "3")
FROM KAFKA (
  "kafka_broker_list" = "bootstrap.events.europe-west1.managedkafka.my-proj.cloud.goog:9092",
  "kafka_topic" = "orders"
);
```

**Materialized views.** Synchronous views (single-table rollups maintained with each load) and asynchronous views (multi-table, refreshed on a schedule, used for transparent query rewrite) were covered in the rollup topic. Asynchronous views can also be defined over **external catalogs**, which is how these engines act as accelerators for lakehouse tables.

**External catalogs.** Both engines can query Apache Iceberg, Hive, Hudi, Delta Lake and Paimon tables in place through catalogs, so one SQL engine can serve both hot internal tables and the lake. With Iceberg tables in Cloud Storage registered in a REST catalog such as the Lakehouse runtime catalog (formerly BigLake metastore), StarRocks can read them directly.

**Doris versus StarRocks.** They are close cousins, and both move fast, so compare current versions on your own workload. Broad differences commonly cited: StarRocks emphasises its optimizer, pipeline engine, shared-data architecture and lakehouse query performance; Doris emphasises built-in inverted indexes for text search, a `VARIANT` type for semi-structured data and log analytics. Both speak the MySQL protocol, so BI tools connect with a MySQL driver.

| Need | Lean towards |
|---|---|
| One wide event table, extreme QPS, simple queries | Pinot or Druid |
| Star schema with joins, ad hoc SQL, sub-second | StarRocks or Doris |
| Frequent row updates from CDC | Pinot upsert, StarRocks primary key, Doris merge-on-write |
| Querying the lakehouse in place, plus a hot tier | StarRocks or Doris with external catalogs |
| Log analytics with text search | Doris (inverted index), Pinot text index, or a search engine |

> **Pitfall:** Bucketing decides parallelism and data distribution. Too few buckets limits parallel scans; too many creates tiny tablets and metadata overhead; bucketing on a skewed key puts most data on a few BEs. Choose a high-cardinality, evenly distributed bucket key, or use random bucketing for append-only tables where supported.

> **Tip:** Because both speak MySQL, you can test them with any MySQL client: `mysql -h fe-host -P 9030 -u root`. Port 9030 is the FE's default MySQL protocol port.

::: deeper
**Primary key index memory.** The StarRocks primary key table keeps a key-to-row index; it can be persisted to local disk to reduce memory, but key width and count still drive memory and write cost. Prefer compact keys (integers rather than long strings).

**Query caching.** Both engines offer result and partial (per-tablet) query caches that help dashboards repeating similar aggregations; check invalidation behaviour for tables that update every few seconds.

**Shared-data trade-offs.** Decoupled storage reduces cost and lets compute scale to zero, but the first read of cold data goes to object storage. Size the local cache for the hot working set and pre-warm it for latency-sensitive tables.

> **Interview:** "Why can StarRocks handle a normalised star schema that Pinot would struggle with?" A cost-based optimizer picks join order and distribution, runtime filters prune the fact table scan, and colocate or bucket-shuffle joins avoid moving the large table. Pinot's fast path is a single-table scatter-gather, and its multi-stage engine adds joins at a higher latency cost.
:::

::: check
?? What are the roles of FE and BE in StarRocks and Doris?
The FE (Java) handles the MySQL protocol, metadata, planning and coordination; the BE (C++) stores data and executes query fragments.

?? What is a colocate join, and why is it fast?
A join between tables bucketed identically on the join key and placed together, so each bucket joins locally with no network exchange.

?? A team needs sub-second dashboards on a star schema with five dimension tables and occasional ad hoc joins. Which family would you start with, and why?
StarRocks or Doris: their cost-based optimizer, runtime filters and MPP joins handle star schemas well, while Druid and Pinot prefer one denormalised table.
:::

::: resources
- Docs: [StarRocks architecture](https://docs.starrocks.io/docs/introduction/Architecture/) · FE, BE and CN roles, shared-nothing and shared-data modes
- Docs: [StarRocks table types](https://docs.starrocks.io/docs/table_design/table_types/) · duplicate, aggregate, unique and primary key tables compared
- Docs: [What is Apache Doris](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris/) · Doris's architecture, use cases and features
- Docs: [Doris compute-storage decoupled mode](https://doris.apache.org/docs/compute-storage-decoupled/overview) · running Doris on object storage with compute groups
- Docs: [StarRocks query rewrite with materialized views](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/) · how the optimizer uses views transparently
- Repo: [StarRocks source](https://github.com/StarRocks/starrocks) · code, design documents and release notes
:::

## Production

Choosing an engine and designing tables is half the job. The other half is running the cluster for years: keeping replicas healthy, moving old data to cheaper storage, routing queries so that one dashboard cannot starve the others, warming caches after restarts, sizing hardware, and knowing within minutes when freshness or latency slips. This group gathers those practices across the three engine families.

### RTOLAP in Production — Operations & Tuning

::: plain
Running an RTOLAP cluster is like running a busy shop with a warehouse at the back. The most popular goods sit on shelves near the till (hot tier, fast SSDs), older goods in the back room (warm tier, cheaper disks), and everything ever sold has a copy in a distant depot (deep storage). You need enough staff at peak hour, a spare person for every role in case someone is ill, and a dashboard that shows queues forming before customers complain.
:::

**Replication.** Every engine replicates for availability, and it also replicates for throughput, because replicas share queries.

- **Druid**: the Coordinator's **load rules** set how many historicals in each tier load each segment (for example 2 replicas of the last 30 days in the hot tier). Real-time freshness depends on `replicas` in the Kafka supervisor.
- **Pinot**: `replication` for offline tables and `replicasPerPartition` for real-time tables. **Replica-group** assignment groups servers so that each group holds a full copy, and each query uses one group.
- **StarRocks / Doris**: the replication number per table (3 by default) in shared-nothing mode; in shared-data mode durability comes from object storage and compute nodes cache data.

Deep storage (Cloud Storage) is the durable copy for Druid and Pinot, so serving replicas protect availability and throughput, not durability.

**Tiered storage.** Recent data gets most queries; old data gets few. Put them on different hardware:

```json
[
  {"type": "loadByPeriod", "period": "P7D", "includeFuture": true, "tieredReplicants": {"hot": 2}},
  {"type": "loadByPeriod", "period": "P90D", "includeFuture": true, "tieredReplicants": {"_default_tier": 1}},
  {"type": "dropForever"}
]
```

These Druid rules, applied in order, keep the last 7 days on two `hot` historicals (local SSDs, lots of memory), days 8 to 90 on one replica in the default tier (cheaper machines), and drop older segments from historicals. They stay in deep storage, where they can still be queried from deep storage through the MSQ engine or reloaded. Each historical declares its tier with `druid.server.tier`.

Pinot's **tiered storage** uses `tierConfigs` to move segments older than a given age to servers with a different tag (cheaper instances), and StarRocks and Doris support storage medium and cooldown settings per partition, or object storage in shared-data mode.

**Query routing and isolation.**

- **Druid**: **query laning** limits how many queries of a lane (for example low-priority reports) can run at once on a broker, and **query priority** lets historicals favour interactive queries. Separate broker pools can serve different applications.
- **Pinot**: **tenants** tag brokers and servers so tables and workloads are physically isolated; **query quotas** cap queries per second per table; partition-aware routing and replica groups reduce fan-out.
- **StarRocks / Doris**: **resource groups** (workload groups in Doris) limit CPU, memory and concurrency per user or query class, and multiple warehouses or compute groups isolate workloads in shared-data mode.

Always set **timeouts** at every layer (client, broker, server): a runaway query is worse than a failed one.

**Caching and cache warming.** Three caches matter:

1. The **operating system page cache** holding memory-mapped segment files (Druid historicals, Pinot servers). After a restart or a rebalance, the first queries read from disk. Warm it by replaying a sample of recent production queries, or by preloading segments, before a node receives traffic.
2. **Per-segment result caches** (Druid historicals) and **result caches** at the broker. They help repeated dashboard queries on immutable segments; results touching real-time data are usually not cached.
3. **Local caches over object storage** in shared-data StarRocks and Doris. Size them for the hot working set.

**Capacity planning: a worked example.** Suppose 50,000 events per second at an average of 500 bytes:

- Raw ingest: 50,000 × 500 B = 25 MB/s, about 2.16 TB per day.
- Assume columnar encoding and compression shrink it 5 times (measure with your data): about 430 GB per day.
- Assume rollup to the minute gives a ratio of 8: about 54 GB per day.
- Hot tier: 30 days × 54 GB × 2 replicas ≈ 3.2 TB of local SSD across historicals or servers, plus headroom (say 30%) for compaction and rebalancing: about 4.2 TB.
- Memory: the query working set (typically the last few days of hot data, about 150 to 300 GB here) should fit in the page cache across nodes for consistent p99 latency.
- Ingestion: Kafka partitions and ingestion tasks sized so that each task handles a comfortable share of 50,000 events per second (benchmark with your schema and indexes), with replicas doubling the ingestion CPU.

Then load-test with realistic queries at the target QPS: latency at high concurrency, not single-query speed, decides cluster size.

**Monitoring.** The metrics that catch most incidents:

| Signal | Druid | Pinot | StarRocks / Doris |
|---|---|---|---|
| Freshness (consumer lag) | `ingest/kafka/lag` | Real-time consumption lag and ingestion delay | Routine Load lag and load job errors |
| Query latency p50 / p99 | `query/time` per broker and node | Broker query latency metrics | FE query latency |
| Unavailable data | `segment/unavailable/count` | Segments in error or offline state (ideal state vs external view) | Unhealthy tablets |
| Queueing | `query/wait/time`, `segment/scan/pending` | Server scheduler wait | Query queue length per resource group |
| Resources | JVM GC, heap, disk usage per tier | JVM GC, heap, off-heap, disk | BE memory, compaction score |

Druid emits metrics through emitters (a Prometheus emitter extension exists), Pinot exposes JMX metrics (scraped by the Prometheus JMX exporter), and StarRocks and Doris expose Prometheus endpoints. Grafana dashboards on top are the norm. On Google Cloud, Google Cloud Managed Service for Prometheus can scrape these from GKE and feed Cloud Monitoring alerts.

**Running on Google Cloud.** A common layout: GKE node pools per role (brokers, hot data nodes with Local SSD, warm data nodes with Persistent Disk, ingestion workers), Cloud Storage for deep storage and backups, Cloud SQL for PostgreSQL as Druid's metadata store, Google Managed Service for Apache Kafka as the stream, and Helm charts or operators (a Druid operator, Pinot's Helm charts, the StarRocks operator) to deploy. Managed offerings from the engines' vendors remove most of this work at a price.

> **Pitfall:** Rebalancing after adding nodes moves many segments at once, filling network and disks and evicting page caches, so latency spikes. Throttle segment moves (Druid's `maxSegmentsToMove` and replication throttling, Pinot rebalance options) and rebalance outside peak hours.

> **Pitfall:** Ingestion and queries compete for CPU. A backfill that reingests a month can starve live queries. Run backfills on separate workers or tiers, and watch lag and p99 together.

::: deeper
**Upgrades.** Roll services in the documented order (for Druid, typically historicals and MiddleManagers first, then brokers, routers and masters) one node at a time, checking that segments stay available between steps. Pinot recommends controllers, then brokers, then servers and minions. Keep the previous version's images ready to roll back.

**Backups.** For Druid and Pinot, deep storage plus the metadata store (or ZooKeeper and Helix state for Pinot, which the controller can rebuild from deep store and table configs) are what you back up. Test restoring into a fresh cluster at least once.

**Cost levers.** Retention (fewer days hot), rollup (fewer rows), tiering (cheaper warm hardware), right-sized indexes (index only filtered columns), Spot VMs for stateless brokers and some ingestion workers (never for hot data nodes without spare replicas) and query result caching.

> **Interview:** "Your p99 latency doubled after a deployment, but p50 is unchanged. Where do you look?" Tail latency usually comes from a few slow servers or segments: a node with a cold page cache after restart, GC pauses, a rebalance in progress, a skewed partition, or a query that lost its index (check segments scanned and entries scanned in filter). Compare per-server latency and look for the outlier.
:::

::: check
?? What do Druid tiered load rules let you do?
Keep recent segments on more replicas on fast "hot" historicals and older segments on fewer replicas on cheaper tiers, and drop the oldest from historicals while they stay in deep storage.

?? Why must you warm caches after restarting a historical or Pinot server?
Segments are memory-mapped and served from the page cache; after a restart the cache is empty, so the first queries read from disk and latency spikes until hot data is back in memory.

?? Name three signals you would alert on for a user-facing RTOLAP table.
Consumer lag (freshness), p99 query latency, and unavailable segments or offline replicas (also queueing time and GC are good candidates).
:::

::: resources
- Docs: [Druid retention rules](https://druid.apache.org/docs/latest/operations/rule-configuration/) · load and drop rules, tiers and replicas
- Docs: [Druid basic cluster tuning](https://druid.apache.org/docs/latest/operations/basic-cluster-tuning/) · sizing memory, threads and caches per service
- Docs: [Druid mixed workloads](https://druid.apache.org/docs/latest/operations/mixed-workloads/) · query laning, priorities and service tiering
- Docs: [Pinot tiered storage](https://docs.pinot.apache.org/operators/operating-pinot/tiered-storage) · moving older segments to cheaper servers
- Docs: [Pinot monitoring](https://docs.pinot.apache.org/operators/operating-pinot/monitoring) · the metrics to watch and how to export them
- Blog: [Operating Apache Pinot at Uber](https://www.uber.com/blog/operating-apache-pinot/) · lessons from one of the largest Pinot deployments
:::

## Wrap-up

### Cheat sheet

| Concept or task | Rule of thumb, config or SQL |
|---|---|
| RTOLAP signature | Freshness in seconds + p99 under a few hundred ms + thousands of QPS |
| Warehouse instead | Few users, varied SQL, big joins, minutes of delay acceptable |
| Low-cardinality filter column | Dictionary + inverted (bitmap) index |
| High-cardinality equality filter | Sort or partition key, or bloom filter; not an inverted index |
| Roaring containers | Array up to 4,096 values, bitmap 8 KB above, run for long runs |
| Pre-aggregated group-bys in Pinot | Star-tree index with `dimensionsSplitOrder` and `functionColumnPairs` |
| Count events after Druid rollup | `SUM("count")`, never `COUNT(*)` |
| Distinct counts after rollup | Store HLL or Theta sketches at ingestion |
| Upsert in Pinot | `primaryKeyColumns`, `upsertConfig`, key-partitioned topic, `strictReplicaGroup` |
| Upsert in StarRocks | `PRIMARY KEY (...)` table; `__op` for deletes; merge condition for order |
| Correct data in Druid | `REPLACE INTO ... OVERWRITE WHERE __time ...` |
| Druid freshness path | Supervisor tasks serve new rows; handoff to historicals after publishing |
| Always filter on time | Lets the broker prune segments before scanning |
| Segment size | Around 5 million rows or a few hundred MB; compact small segments |
| Pinot hybrid table | Offline + real-time split at the time boundary |
| StarRocks / Doris connect | MySQL protocol, `mysql -h fe-host -P 9030 -u root` |
| Joins on star schemas | StarRocks or Doris (CBO, runtime filters, colocate joins) |
| Tiering | Hot replicas on SSD for recent days, warm tier for older, deep storage for all |
| Alert on | Consumer lag, p99 latency, unavailable segments, queue time, GC |
| Google Cloud layout | GKE + Local SSD for hot nodes, Cloud Storage deep storage, Managed Kafka source |

### Practice

::: try
**1. Warehouse or RTOLAP?** For each need, choose BigQuery (possibly with BI Engine or materialized views) or an RTOLAP engine, and justify: (a) a finance team's daily revenue report; (b) a page inside a delivery app showing each courier their earnings today, updated within 10 seconds, for 300,000 couriers; (c) an operations wall screen showing orders per minute by city, refreshed every 30 seconds, watched by 20 people; (d) a data scientist exploring two years of clickstream.
:::

?? Answer to 1
(a) BigQuery: daily freshness, few users. (b) RTOLAP (Pinot with upsert or dedup, or StarRocks): seconds of freshness, huge concurrency, each query filtered to one courier. (c) Either can work; BigQuery with streaming ingestion and BI Engine is often enough for 20 viewers at 30-second refresh, and an RTOLAP engine is justified only if sub-second freshness or many more viewers are needed. (d) BigQuery: varied ad hoc SQL over all history.

::: try
**2. Design the indexes.** A Pinot table of ad impressions has columns `ts`, `advertiser_id` (50,000 values), `campaign_id` (2 million values), `country` (200), `device` (4), `impression_id` (unique), `cost`. Almost every query filters on one `advertiser_id` and a time range, sometimes on `country` or `device`, and groups by `campaign_id`. Choose the sorted column, inverted indexes and any other index.
:::

?? Answer to 2
Sorted column: `advertiser_id`, the filter in almost every query (and partition the Kafka topic and table by it for partition-aware routing). Inverted indexes: `country` and `device` (low cardinality, occasional filters). No inverted index on `campaign_id` (it is grouped, not filtered) or `impression_id` (unique); use a bloom filter on `impression_id` only if lookups by it are needed. `ts` is the time column, used for segment pruning; a range or timestamp index helps fine-grained time filters. A star-tree on (`advertiser_id`, `country`, `device`) with `SUM__cost` and `COUNT__*` would help if those group-bys dominate.

::: try
**3. Rollup arithmetic.** A Druid datasource ingests 1.2 billion events per day. After rollup with `queryGranularity: MINUTE` and dimensions `page`, `country` and `device`, the datasource holds 30 million rows per day. What is the rollup ratio? A product manager asks to add `user_id` as a dimension. What happens, and what would you suggest instead?
:::

?? Answer to 3
The ratio is 1,200,000,000 / 30,000,000 = 40. With `user_id` added, almost every row becomes unique within a minute, so the ratio falls towards 1 and the datasource grows up to 40 times. If they need distinct users, add an HLL or Theta sketch metric on `user_id`. If they need per-user drill-down, keep a separate raw datasource with short retention, or query the raw events in BigQuery.

::: try
**4. Fix the double counting.** A Pinot real-time table with full upsert on `order_id` shows some orders twice in counts. The Kafka producer uses the default partitioner with a null key. What is wrong, and how do you fix it?
:::

?? Answer to 4
With a null key, the producer spreads records for the same `order_id` across partitions, so different versions land on different servers, each of which considers its own copy the latest. Produce with `order_id` as the record key so every version goes to the same partition, keep the partition count fixed, make sure the table uses `strictReplicaGroup` routing, and re-ingest the table from the corrected topic (or from the lake) to clear existing duplicates.

::: try
**5. Size the hot tier.** An event stream carries 20,000 events per second at 400 bytes each. Your tests show 5 times compression and a rollup ratio of 10. You must keep 14 days hot with 2 replicas and 30% headroom. Estimate the hot-tier SSD capacity.
:::

?? Answer to 5
Raw: 20,000 × 400 B = 8 MB/s, about 691 GB per day (8 MB × 86,400 s). After 5× compression: about 138 GB per day; after rollup by 10: about 13.8 GB per day. 14 days × 13.8 GB × 2 replicas ≈ 387 GB; with 30% headroom, about 500 GB of SSD across the hot nodes.

### What comes next

You now know how real-time OLAP engines make fresh data queryable in milliseconds: segments, bitmap indexes, pre-aggregation, upserts and the operations that keep them healthy. Each of them depends on a stream of events arriving reliably and in order. The next section, **Streaming systems**, studies that stream itself: event time and watermarks, windows, delivery guarantees, Kafka's internals, schema registries, and the alternatives to Kafka such as Pulsar, Redpanda and streaming SQL engines.
