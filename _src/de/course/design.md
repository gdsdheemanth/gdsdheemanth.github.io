This section pulls the whole course together. Until now each section studied one layer: storage formats, warehouses, engines, streaming, ingestion, orchestration, the Google Cloud services, and the distributed systems ideas underneath them. Here you design complete platforms, the way you would in a design review at work or in a system design interview. It is written for data engineers who can already build a pipeline and now need to decide what the whole platform should look like, defend the choices with numbers, and explain what happens when things fail. It assumes the earlier sections, especially **Data lakes and lakehouses** (table formats), **Streaming systems** and **Real-time OLAP engines**, **Data ingestion**, **BigQuery in depth**, **Pipelines on Google Cloud**, **Operational databases, AI and governance on Google Cloud** (whose reference architectures topic is the short version of this section) and **Distributed systems** (failure, consistency and SLOs). The Python examples run with only the standard library.

**What you will be able to do afterwards**

- Design a data platform end to end on Google Cloud: zones, processing engines, serving stores, self-serve access and governance, with a reason for every component.
- Lay out medallion layers with real quality gates, incremental processing, late data handling and schema evolution rules.
- Choose between a central lakehouse and a data mesh, and design data products with specs, SLAs, output ports and versioning.
- Design real-time analytics, high-volume ingestion and feature stores, with latency classes and failure handling.
- Put numbers on a design: volumes, throughput, storage, query cost, capacity, recovery objectives, egress.
- Plan privacy, multi-region and migration work so that it is safe, testable and reversible.
- Answer data engineering system design interviews with a repeatable framework and worked examples.

**The big picture.** Every data platform, however large, answers five questions. **Where does data come from and how does it arrive?** (ingestion: push or pull, batch or stream, with contracts). **Where does it live?** (storage: object storage and tables in open or managed formats, organised in layers). **Who transforms it, and when?** (processing: SQL in BigQuery, Dataflow, Spark, small serverless jobs, orchestrated). **How is it used?** (serving: dashboards, APIs, features, shares, each with its own latency). **What keeps it trustworthy, safe and affordable?** (governance, security, reliability and cost, which cut across all the others). A good design gives a short, justified answer to each, sized by numbers: how many events per second, how many terabytes, how fresh, how many users, how much money. A weak design lists services. Keep the five questions and the numbers in mind; every topic below is one of them looked at closely.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Architectures

This group draws the main architectures you will be asked to design. It starts with the general shape every platform shares (zones from ingestion to serving, with governance across them), then zooms into the layered table design inside it (medallion), and the complete lakehouse reference with tool choices per layer. Then it changes the organisational lens with data mesh, and finishes with three specialised designs that appear in almost every large company: real-time analytics, high-volume ingestion and feature stores for machine learning.

The thread through the group is that architecture is driven by requirements. Freshness, query latency, data volume, the number of teams and the regulatory context decide the design. The same company can need a daily batch lakehouse for finance, a seconds-fresh path for operations and a millisecond feature store for fraud scoring, all reading the same raw data.

### Data Platform End-to-End Design

::: plain
A data platform is like a city's water system. Water is collected from many sources (rivers, wells, rain), stored in reservoirs, treated in a plant, and piped to homes, factories and fire hydrants, each needing different pressure and quality. Inspectors test the water at every stage and a control room watches the whole network. Designing a data platform means deciding where each of those stages sits, what equipment it uses, and how much it must carry.
:::

**The zone architecture.** Almost every platform can be drawn as a left-to-right flow of zones, with a vertical band of cross-cutting concerns:

[[fig:design_platform]]

| Zone | Job | Typical Google Cloud choice |
|---|---|---|
| Sources | Operational databases, SaaS apps, event streams, files from partners | Cloud SQL, AlloyDB, Spanner, apps publishing events, SFTP drops |
| Ingest | Move data in reliably, without transformation logic | Datastream (CDC), Pub/Sub or Managed Kafka, BigQuery Data Transfer Service, Storage Transfer Service, Cloud Run connectors |
| Land (raw, bronze) | Keep an immutable copy exactly as received | Cloud Storage buckets, raw BigQuery datasets or Iceberg tables |
| Process (silver, gold) | Clean, conform, join, aggregate | BigQuery SQL with Dataform or dbt, Dataflow, Managed Service for Apache Spark (formerly Dataproc) |
| Serve | Deliver data at the latency each consumer needs | BigQuery with BI Engine, Bigtable, AlloyDB, Pub/Sub topics, BigQuery sharing (formerly Analytics Hub) |
| Consume | Use it | Looker, Data Studio (formerly Looker Studio), notebooks, applications, ML on Gemini Enterprise Agent Platform (formerly Vertex AI) |
| Cross-cutting | Govern, secure, orchestrate, observe, pay | Knowledge Catalog (formerly Dataplex Universal Catalog), IAM, VPC Service Controls, Managed Service for Apache Airflow (formerly Cloud Composer), Cloud Monitoring, budgets |

The zones exist to separate concerns. Ingestion only moves bytes, so a bug in business logic never loses source data. The landing zone is immutable, so any later layer can be rebuilt. Processing is where logic lives, versioned in Git. Serving is shaped by consumers, not by sources.

**Compute-storage separation** is the property that makes this layout cheap and flexible. Data lives once in Cloud Storage or BigQuery storage; engines (BigQuery slots, Dataflow workers, Spark executors) attach to it, scale for the job and go away. You can run a 500-slot backfill on Sunday without buying a bigger cluster, and two engines can read the same Iceberg table. The mechanics are in [Google Cloud foundations for data](#gcpfound); for design, the consequence is that you size storage for retention and compute for the busiest workload separately.

**Choosing the processing layer.** Pick the simplest engine that meets the requirement, and default to SQL in the warehouse.

| Requirement | First choice | Why |
|---|---|---|
| Batch transformations on tables, analysts can read the code | BigQuery SQL with Dataform or dbt | No cluster, scales automatically, lineage in Knowledge Catalog |
| Streaming with event time, windows, dedupe, enrichment | Dataflow (Apache Beam) | Exactly-once processing, autoscaling, watermarks |
| Existing Spark code, heavy Python or ML preprocessing on files | Managed Service for Apache Spark, serverless batches | Runs Spark without managing clusters |
| Small API pulls, file parsing, glue | Cloud Run jobs or Cloud Run functions | Cheap, per-second billing, any language |
| Simple stream into a table, no transformation | Pub/Sub BigQuery subscription | No pipeline code at all |

**The serving layer** is chosen per consumer, by latency and access pattern. Dashboards with seconds of tolerance read BigQuery (with BI Engine or materialized views for speed). An application that looks up one customer's profile in under 10 ms reads Bigtable or AlloyDB, loaded from BigQuery by a scheduled job or reverse ETL. Other teams and partners get BigQuery sharing listings, not copies. Event consumers subscribe to curated Pub/Sub topics. One table rarely serves all of them well.

**The self-serve access layer** lets people find and use data without filing a ticket: Knowledge Catalog search with owners and descriptions, a Looker semantic model so "revenue" means one thing, sandbox datasets per team with quotas, and a request flow for access through groups. The Data Platform Self-Service Design topic later in this section goes deeper.

**Governance integration** means governance is built into the flow, not added at the end: classification and policy tags applied when a table is created, quality scans as gates between layers, lineage collected automatically from BigQuery and Dataflow jobs, and audit logs exported. The details are in [Data governance, security and compliance](#governance).

**Putting numbers on it.** A worked example: an online retailer with a PostgreSQL order database (AlloyDB), a clickstream, and 30 SaaS sources. Before choosing anything, size it.

```python
# Sizing a mid-size retail platform (assumptions in comments)
click_eps, click_bytes = 3_000, 1_000          # peak events/s, bytes per event
cdc_rows_per_day, cdc_bytes = 20_000_000, 400  # changed rows/day from AlloyDB
saas_gb_per_day = 5

click_gb_day = click_eps * 0.5 * click_bytes * 86_400 / 1e9   # average is half of peak
cdc_gb_day = cdc_rows_per_day * cdc_bytes / 1e9
raw_gb_day = click_gb_day + cdc_gb_day + saas_gb_per_day
print(f"peak ingest: {click_eps * click_bytes / 1e6:.1f} MB/s")          # peak ingest: 3.0 MB/s
print(f"raw per day: {raw_gb_day:.0f} GB")                              # raw per day: 143 GB
print(f"raw per year: {raw_gb_day * 365 / 1000:.0f} TB uncompressed")   # raw per year: 52 TB uncompressed
```

3 MB/s at peak is small for Pub/Sub, 143 GB a day is comfortable for BigQuery, and about 52 TB a year uncompressed (roughly 5 to 10 TB stored after columnar compression) means partitioning by date and long-term storage pricing matter, but nothing exotic is needed. The design that follows: Datastream from AlloyDB into BigQuery; a Pub/Sub topic with a BigQuery subscription for clicks (plus a Dataflow job later only if sessionisation must be real time); Data Transfer Service or a managed connector for SaaS; Dataform for silver and gold; Looker on gold; Managed Airflow only if cross-system dependencies appear.

> **Pitfall:** Designing from the service catalogue instead of from requirements. A diagram with twelve Google Cloud logos and no numbers is not a design. Every box should answer "which requirement forces this?".

> **Tip:** Keep one **architecture decision record** (ADR) per important choice: context, options, decision, consequences, and the signal that would make you revisit it ("if clicks exceed 50,000 per second, add Dataflow"). It turns arguments into documents.

::: deeper
**Project layout follows zones.** A common layout uses separate projects for landing, warehouse (silver and gold datasets), pipelines (Dataflow, Airflow, service accounts), and consumption (BI, sandboxes), repeated per environment. It gives clean IAM boundaries (the BI service account cannot read landing), separate quotas and clear bills. VPC Service Controls draw one perimeter around the data projects.

**Location is a design decision made once.** BigQuery cannot join across locations in a single query, and moving data across regions costs money and time. Pick the home location (a region such as `europe-west1` or a multi-region such as `EU`) for all data services before the first dataset is created, and enforce it with the `gcp.resourceLocations` organization policy.

**Coupling between zones.** Zones talk through durable interfaces: files in buckets, tables, topics. Avoid direct calls (a processing job calling a source database) except at the ingest zone. That is what lets each zone fail and recover independently: if Dataform fails, ingestion keeps landing data and the rebuild catches up.

**Evolution path.** Platforms grow in stages: a warehouse with batch loads; then CDC and a landing zone; then streaming for a few use cases; then domain ownership and data products. Design so the next stage is an addition, not a rewrite: open formats or a managed warehouse, layers with clear promises, IaC from day one.

> **Interview:** When asked to "design a data platform", spend the first minutes on requirements and numbers (sources, volume, freshness per use case, consumers, compliance), then draw the zones, then justify one engine per zone. Interviewers mark down designs that jump to tools.
:::

::: check
?? Why is the landing zone kept immutable?
So that every later layer can be rebuilt from it. If a transformation bug corrupts silver or gold, you fix the code and reprocess from the raw copy instead of asking the source for the data again (which may be impossible).

?? An application needs a customer's profile in under 10 ms. Why not query BigQuery directly?
BigQuery is built for scans, with query latency typically in hundreds of milliseconds to seconds and limited concurrency per project. A key-value lookup at application latency belongs in a serving store such as Bigtable or AlloyDB, loaded from BigQuery.

?? What does compute-storage separation change about capacity planning?
You size storage for data volume and retention, and compute for the busiest workload, independently. Compute can scale up for a backfill and back to zero without moving data.
:::

::: resources
- Docs: [Architecture Center: big data and analytics](https://docs.cloud.google.com/architecture/big-data-analytics) · Google's reference designs for data platforms
- Docs: [Google Cloud Well-Architected Framework](https://docs.cloud.google.com/architecture/framework) · review checklist for reliability, security, cost and operations
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley's lifecycle (generation, ingestion, storage, transformation, serving) and undercurrents
- Book: [Data Management at Scale, 2nd edition](https://www.oreilly.com/library/view/data-management-at/9781098138851/) · Strengholt on architecture for large, distributed organisations
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · Kleppmann's foundations for every storage and processing choice
- Article: [Documenting architecture decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions) · Michael Nygard's original ADR format
:::

### Medallion Architecture Deep Dive

::: plain
Think of a restaurant kitchen. Deliveries arrive at the back door and go into the store room exactly as they came (bronze). Cooks wash, peel and portion them into labelled containers (silver). Plates are assembled for each dish on the menu (gold). Nothing reaches a plate without passing the washing station, and if a dish goes wrong you can make it again from the store room. The medallion architecture organises tables the same way, with a check at every doorway.
:::

[Pipelines and transformation](#pipelines) introduced the three layers. This topic is about designing them so that each layer keeps a promise.

[[fig:design_medallion]]

**What each layer promises.**

| Layer | Contents | Promise to readers | Who reads it |
|---|---|---|---|
| Bronze (raw) | Source data as received, plus ingestion metadata (`_ingested_at`, `_source_file`, `_batch_id`) | Complete and unchanged; nothing dropped | Data engineers, replays |
| Silver (cleaned, conformed) | Typed, deduplicated, validated records; conformed keys and units; one table per business entity or event | Correct types, unique keys, valid values, UTC timestamps | Engineers, analytics engineers, data scientists |
| Gold (business) | Facts, dimensions, aggregates and features shaped for a use case | Business definitions, documented, tested, with an SLA | Analysts, BI, applications |

The promise matters more than the name. A silver table without uniqueness tests is just a second bronze table.

**Quality gates between layers.** Each transition runs checks, and the result decides what happens to the data.

- **Bronze to silver**: schema conformance (expected columns and types), parse errors, required fields not null, primary key unique within the batch, values in range. Rows that fail go to a **quarantine** table with the failure reason; the batch proceeds if the failure rate is under a threshold (for example 0.1%), otherwise the whole batch stops.
- **Silver to gold**: referential integrity (every order has a known customer), reconciliation with the source (row counts and sums within tolerance), freshness, and business rules (no negative revenue). A failure here blocks publication: gold keeps yesterday's good version.

In Dataform, assertions express these gates in SQL; in dbt, tests; in Knowledge Catalog, data quality scans. A quarantine pattern in BigQuery:

```sql
-- Split a bronze batch into valid rows and quarantined rows
CREATE TEMP TABLE checked AS
SELECT
  *,
  CASE
    WHEN SAFE_CAST(amount AS NUMERIC) IS NULL THEN 'amount not numeric'
    WHEN order_id IS NULL THEN 'missing order_id'
    WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', created_at) IS NULL THEN 'bad timestamp'
  END AS reject_reason
FROM bronze.orders_raw
WHERE _batch_id = @batch_id;

INSERT INTO quarantine.orders (batch_id, reject_reason, payload)
SELECT @batch_id, reject_reason, TO_JSON_STRING(t)
FROM checked AS t WHERE reject_reason IS NOT NULL;

MERGE silver.orders AS s
USING (
  SELECT order_id, customer_id,
         CAST(amount AS NUMERIC) AS amount,
         PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', created_at) AS created_at,
         _ingested_at
  FROM checked WHERE reject_reason IS NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) = 1
) AS b
ON s.order_id = b.order_id
WHEN MATCHED AND b._ingested_at > s._ingested_at THEN
  UPDATE SET customer_id = b.customer_id, amount = b.amount, created_at = b.created_at, _ingested_at = b._ingested_at
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, amount, created_at, _ingested_at)
  VALUES (b.order_id, b.customer_id, b.amount, b.created_at, b._ingested_at);
```

The `MERGE` is idempotent: re-running the same batch changes nothing, because the incoming `_ingested_at` is not newer than the stored one.

**Incremental processing.** Rebuilding every layer from scratch each day stops working once tables reach terabytes. Three patterns, from simplest:

1. **Partition overwrite**: recompute only the partitions touched by the new data (for example the last 3 days) and replace them atomically. Simple and idempotent; works when late data is bounded.
2. **Watermark on ingestion time**: process rows with `_ingested_at` greater than the last processed value, stored in a state table. Use ingestion time, not event time, so late events are still picked up.
3. **Change feeds**: read only changed rows from the table format (Iceberg incremental reads, Delta change data feed, BigQuery change history with `APPENDS` or `CHANGES` table-valued functions) and merge them downstream.

**Late-arriving data.** An event from 3 days ago arrives today. Silver absorbs it naturally if it merges on keys. Gold aggregates by event date need the affected partitions recomputed. The design rule is a **lookback window**: each gold run recomputes the last N days of partitions, where N comes from measuring lateness (for example, 99.9% of events arrive within 2 days, so N = 3). Events later than the window are either applied by a weekly correction job or accepted as a known, documented gap. Late **dimension** data (an order whose customer row has not arrived yet) is handled with an "unknown" member or an inferred dimension row that is updated when the real row lands, so the fact is not dropped.

**Schema evolution per layer.**

| Change at the source | Bronze | Silver | Gold |
|---|---|---|---|
| New column | Store it automatically (schema-on-read JSON column or auto-evolving table) | Add when needed, with a default | Add only by a reviewed change |
| Column removed | Keep historical column, null going forward | Keep column, alert the owner | Contract decides: deprecate with notice |
| Type widened (int to bigint) | Accept | Widen | Widen |
| Type changed incompatibly (string to struct) | Land in a new column or new table version | Map explicitly in code | Unchanged contract for consumers |
| Rename | Treated as drop plus add | Map old and new into one column | Unchanged |

Bronze is permissive so nothing is lost; gold is strict because consumers depend on it. Silver is where translation happens.

> **Pitfall:** Business logic in bronze. Filtering "test orders" during ingestion feels tidy, but when the definition of a test order changes you cannot recover the rows. Keep bronze complete and filter in silver.

> **Tip:** Store bronze in a format that can hold anything: a typed table for well-behaved sources, a table with a JSON column plus extracted keys for messy ones. The BigQuery `JSON` type lets silver parse fields with `JSON_VALUE` without losing unknown fields.

::: deeper
**How many layers?** Three is a convention. Large platforms often add a staging layer before bronze (files not yet registered), an "integration" layer between silver and gold for conformed shared entities, and a serving layer of denormalised extracts. The rule is not the count but that each layer has a written promise and tests that enforce it.

**Bronze retention.** Bronze is the replay buffer, but it holds personal data and costs money. Typical policy: keep bronze 90 days to 2 years depending on regulation and replay needs, partition by ingestion date, expire partitions automatically (`partition_expiration_days`), and move raw files to Coldline or Archive with lifecycle rules. Erasure requests must reach bronze too.

**Write-audit-publish (WAP).** Gold tables are built into a staging table or an Iceberg branch, audited, and only then published by an atomic swap (BigQuery `CREATE OR REPLACE TABLE ... CLONE`, Iceberg `fast_forward` of the main branch). Readers never see a half-built or failing gold table.

**Streaming medallion.** The same layers work continuously: Pub/Sub to bronze through a BigQuery subscription, a Dataflow job or BigQuery continuous query to silver, materialized views or scheduled micro-batches to gold. Quality gates become per-record checks with dead-letter topics, plus windowed checks on rates.

> **Interview:** "Where do you deduplicate?" In silver, on a business key with a deterministic tie-breaker (latest `_ingested_at`, highest source sequence number). Bronze keeps duplicates as evidence; gold must be able to assume uniqueness.
:::

::: check
?? Why should incremental processing usually track ingestion time rather than event time?
Late events have an old event time but a new ingestion time. A watermark on event time would skip them forever; a watermark on ingestion time picks them up in the next run, and the affected event-time partitions are then recomputed.

?? A bronze-to-silver batch has 4% of rows failing validation while the threshold is 0.1%. What should the pipeline do?
Stop the batch, keep silver unchanged, quarantine the failing rows for inspection and alert the source owner. A failure rate that high usually means a schema or source change, not random bad rows.

?? How does the medallion design handle a source that renames a column?
Bronze sees it as one column stopping and a new one appearing, and stores both. Silver code maps the old and new names into one column, so gold and its consumers see no change.
:::

::: resources
- Article: [Medallion architecture](https://www.databricks.com/glossary/medallion-architecture) · Databricks' definition of the three layers
- Docs: [Dataform assertions](https://docs.cloud.google.com/dataform/docs/assertions) · quality gates in SQL between layers on BigQuery
- Docs: [Knowledge Catalog data quality scans](https://docs.cloud.google.com/dataplex/docs/auto-data-quality-overview) · managed rule-based checks on BigQuery tables
- Docs: [dbt: how we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview) · staging, intermediate and marts, the dbt version of the layers
- Blog: [Medallion architecture best practices](https://piethein.medium.com/medallion-architecture-best-practices-for-managing-bronze-silver-and-gold-486de7c90055) · Piethein Strengholt on what each layer should and should not contain
- Docs: [Apache Iceberg branching and tagging](https://iceberg.apache.org/docs/latest/branching/) · the mechanism behind write-audit-publish on a lakehouse
:::

### Data Lakehouse Reference Architecture

::: plain
A reference architecture is a tested floor plan. Instead of arguing from scratch about where the kitchen goes, you start from a plan that thousands of houses have used, and change only what your family needs. For a lakehouse, the plan says which tool sits in each layer, how the layers hand data to each other, and which single "register of everything" every tool consults.
:::

The lakehouse idea (open table formats on object storage, readable by many engines) is covered in [Data lakes and lakehouses](#lakehouse), and the short Google Cloud version is plan 1 in [Operational databases, AI and governance on Google Cloud](#gcpdata). This topic gives the full reference design with the choices at each layer and the reasons behind them.

**The reference flow.**

```text
Sources
  OLTP (AlloyDB, Cloud SQL, Spanner)  --Datastream-->      bronze (BigQuery tables or Iceberg)
  Events (apps, devices)              --Pub/Sub/Kafka-->   bronze (Iceberg via Dataflow, or BigQuery subscription)
  Files (partners, exports)           --Storage Transfer-> landing bucket --> bronze (external or loaded)
  SaaS                                --connectors-->      bronze
Bronze -> Silver: Spark (serverless batches) or BigQuery SQL; dedupe, type, validate, conform
Silver -> Gold:   Dataform or dbt on BigQuery; facts, dimensions, aggregates, features
Serving: BigQuery (+ BI Engine) for BI; Bigtable/AlloyDB for apps; BigQuery sharing for others;
         Trino, Spark, DuckDB read the same Iceberg tables
Metadata: Lakehouse runtime catalog (Iceberg tables) + Knowledge Catalog (search, lineage, quality, policy)
Control:  Managed Airflow or Dataform schedules; Terraform; Cloud Monitoring
```

**Tooling choices per layer.** Each layer has a Google-managed default and an open or vendor alternative. Pick the column that matches your constraints, not the newest one.

| Layer | Google Cloud default | Open or vendor alternative | Choose the alternative when |
|---|---|---|---|
| Table format | Apache Iceberg managed tables (formerly BigQuery tables for Apache Iceberg) or native BigQuery tables | Iceberg written by Spark or Flink; Delta Lake on Databricks; Hudi or Paimon for heavy upserts | Several engines must write, or an existing Databricks estate |
| Catalog | Lakehouse runtime catalog (formerly BigLake metastore), Iceberg REST endpoint | Apache Polaris, Unity Catalog, Nessie | Multi-vendor federation or an existing catalog |
| Batch compute | BigQuery SQL; Managed Service for Apache Spark serverless batches | Databricks on Google Cloud, Trino clusters on GKE | Team skills, existing notebooks and jobs |
| Streaming compute | Dataflow; BigQuery continuous queries for SQL-only cases | Flink on GKE, Confluent Cloud with Flink | Existing Flink jobs, Kafka-native stack |
| Transformation framework | Dataform | dbt Core or dbt Cloud | Multi-warehouse teams, dbt ecosystem packages |
| Orchestration | Managed Airflow, Dataform workflows | Dagster, Prefect, Astronomer | Asset-based orchestration preferences |
| BI and semantics | Looker (LookML), Data Studio | dbt Semantic Layer, Cube | Semantic layer shared by several BI tools |
| Governance | Knowledge Catalog, BigQuery policy tags and data policies | DataHub, OpenMetadata, Collibra | Catalog spanning non-Google systems |

**The unified metadata layer** is what turns files into a lakehouse. It has two jobs that are often confused:

1. **Technical catalog**: maps a table name to its current metadata file (the Iceberg pointer), and makes commits atomic. Every engine must use the *same* technical catalog for a table, or writers will overwrite each other's commits. On Google Cloud this is the Lakehouse runtime catalog, which BigQuery, Spark and other Iceberg REST clients can share.
2. **Business catalog**: search, descriptions, owners, glossary, classification, lineage, quality results, access policies. That is Knowledge Catalog, which also harvests the technical catalog's tables.

Design rule: exactly one technical catalog per table, and one business catalog for the organisation.

**Sizing and cost, worked.** A lakehouse holding 200 TB of compressed data, growing 10 TB a month, with 40 TB scanned per day by BI and pipelines. Compare storing the data as Iceberg in Cloud Storage with native BigQuery storage, using prices from earlier sections (check the pricing pages for your region).

```python
TB = 1024  # GiB per TiB, prices are per GiB-month
stored_tib = 200
gcs_standard = 0.020                     # USD per GiB-month, Cloud Storage Standard (approx.)
bq_physical_active, bq_physical_lt = 0.04, 0.02
share_long_term = 0.7                    # 70% of partitions untouched for 90+ days

gcs = stored_tib * TB * gcs_standard
bq = stored_tib * TB * (share_long_term * bq_physical_lt + (1 - share_long_term) * bq_physical_active)
print(f"Cloud Storage Standard: ${gcs:,.0f}/month")      # Cloud Storage Standard: $4,096/month
print(f"BigQuery physical:      ${bq:,.0f}/month")       # BigQuery physical:      $5,325/month

scan_tib_day = 40
print(f"on-demand scans: ${scan_tib_day * 30 * 6.25:,.0f}/month")   # on-demand scans: $7,500/month
```

Storage is the smaller line; compute dominates. At 1,200 TiB scanned a month, on-demand costs about $7,500; a reservation sized from measured slot usage is usually cheaper at that volume (the method is in [BigQuery in depth](#bigquery)). The decision between Iceberg and native tables is therefore rarely about storage price. It is about **who must read and write**: native BigQuery tables give the best BigQuery performance and features; Iceberg gives other engines first-class access.

**A dbt or Dataform project structure** mirrors the layers: `sources/` (declarations of bronze), `staging/` (one model per source table: rename, cast), `intermediate/` (joins and business logic), `marts/` (gold, grouped by domain). Tags per layer let the scheduler run `staging` hourly and `marts` daily.

> **Pitfall:** Two technical catalogs for the same Iceberg table, for example Spark committing through a Hive metastore while BigQuery reads through another catalog. Each sees a different "current" snapshot and commits can be lost. Register every Iceberg table in one catalog and point every engine at it.

> **Tip:** Start native, open where needed. Many Google Cloud teams keep silver and gold as native BigQuery tables and use Iceberg only where another engine must write (Spark ML preprocessing, Flink streaming) or where data is shared outside BigQuery.

::: deeper
**Table maintenance is part of the architecture.** Iceberg tables written by streaming jobs accumulate small files and snapshots. The reference design includes scheduled jobs for compaction (`rewrite_data_files`), snapshot expiry and orphan file removal, with targets such as 256 MB to 512 MB files. Apache Iceberg managed tables do this automatically; self-managed tables need it in the scheduler. See [Data lakes and lakehouses](#lakehouse).

**Performance across engines.** The same Iceberg table can be fast in one engine and slow in another, because statistics, sort order and file sizes help engines differently. Define partitioning by the dominant filter (usually date), a sort order or clustering on the next most common filter, and measure the top 20 queries in each engine before committing.

**Vendor platforms on Google Cloud.** Databricks and Snowflake both run on Google Cloud and can read and write Iceberg. In a mixed estate, the open format plus a shared Iceberg REST catalog lets BigQuery, Databricks and Snowflake read the same gold tables without copies. The cost is operational: three permission systems to keep consistent, which is why a single business catalog and policy-as-code matter.

> **Interview:** "Why not just put everything in BigQuery?" For many companies that is the right answer: simpler, faster, fewer moving parts. A lakehouse layer is justified when several engines must work on the same data, when large volumes of raw or semi-structured data would be expensive to load, or when the organisation wants to avoid one engine owning the storage.
:::

::: check
?? What are the two jobs of the lakehouse metadata layer, and which Google Cloud service does each?
The technical catalog maps table names to current Iceberg metadata and makes commits atomic (Lakehouse runtime catalog). The business catalog provides search, ownership, lineage, quality and policy (Knowledge Catalog).

?? In the worked example, which cost line dominates, and what does that imply?
Compute: about $7,500 a month of scans versus roughly $4,000 to $5,300 of storage. Optimisation effort should go to query patterns, partitioning and capacity pricing, not to shaving storage.

?? When would you keep gold tables as native BigQuery tables instead of Iceberg?
When BigQuery is the only engine that reads and writes them. Native tables give the best BigQuery performance and features with no maintenance; Iceberg is worth it when other engines need direct access.
:::

::: resources
- Docs: [Analytics lakehouse on Google Cloud](https://docs.cloud.google.com/architecture/big-data-analytics/analytics-lakehouse) · Google's lakehouse reference with Cloud Storage, BigQuery and Spark
- Docs: [Google Cloud Lakehouse overview](https://docs.cloud.google.com/biglake/docs/introduction) · Iceberg tables, the runtime catalog and engine access on Google Cloud
- Docs: [Apache Iceberg REST catalog spec](https://iceberg.apache.org/rest-catalog-spec/) · the open protocol engines use to share one technical catalog
- Docs: [dbt: how we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview) · project layout that mirrors the layers
- Paper: [Lakehouse: a new generation of open platforms](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf) · the CIDR 2021 paper that defined the lakehouse
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · storage abstractions and the lifecycle behind the reference flow
:::

### Data Mesh Architecture

::: plain
Imagine a large newspaper where one central desk writes every article for every section. It becomes a bottleneck: the sports desk waits for the central writers, who do not really understand football. A data mesh is the alternative: each section (sports, finance, culture) writes its own articles, to a shared style guide, printed on the same presses. The central team stops writing articles and runs the presses and the style guide instead.
:::

**Why data mesh exists.** In a central model, one data team ingests, models and serves data for the whole company. It works until the number of sources and consumers outgrows the team: requests queue, the central engineers lack domain knowledge, and source teams change schemas without knowing who depends on them. Zhamak Dehghani's data mesh (2019) moves ownership to the domains that produce the data.

**The four principles.**

| Principle | What it means | What changes in practice |
|---|---|---|
| Domain ownership | The team that runs a business domain (orders, payments, logistics) owns its analytical data too | Domains have data engineers; the central team stops modeling their data |
| Data as a product | Data shared outside the domain is a product with consumers, quality, documentation and an SLA | Products have owners, contracts, versions; raw tables are private |
| Self-serve data platform | A platform team provides the tools so domains can build products without being infrastructure experts | Templates, provisioning, pipelines, catalog registration as a service |
| Federated computational governance | Global rules decided jointly and enforced by the platform as code | Policies (classification, retention, identifiers) checked automatically |

Governance mechanics (who decides which rules, fitness functions, contracts across domains) are covered in [Data governance, security and compliance](#governance). This topic is about the **architecture**: what the mesh looks like as systems.

**The architecture on Google Cloud.** Each domain gets a folder with projects per environment; its data products are BigQuery datasets (or Iceberg tables) exposed through output ports; the platform team owns the shared layer.

```text
Organization
  folder: platform      -> Terraform modules, CI templates, Knowledge Catalog, shared monitoring
  folder: domain-orders -> projects orders-dev / orders-prod
        private datasets (bronze, silver)           -> only the domain team
        product dataset orders_v2 (gold, contract)  -> BigQuery sharing listing, Knowledge Catalog data product
  folder: domain-payments  -> same shape
  folder: domain-marketing -> consumes orders_v2 and payments_v1 through sharing; publishes campaign_attribution_v1
Cross-domain rules: org policies (locations, no public access), VPC Service Controls, required labels and tags
```

Domains publish; consumers subscribe. A marketing analyst joins `orders_v2` with `payments_v1` because both use the same global `customer_id` and UTC timestamps, which are global policies.

**Mesh topologies.** "Data mesh" covers a spectrum, and choosing the point on it is the real design decision.

| Topology | Description | Strength | Weakness |
|---|---|---|---|
| Central platform, central data (not a mesh) | One team owns everything | Consistency, low overhead | Bottleneck at scale |
| Hub and spoke (governed mesh) | Domains build products on one shared platform, a central hub hosts shared core products (customer, product) and governance | Balance of autonomy and consistency; most common in practice | Hub can become a new bottleneck |
| Fully federated | Domains choose their own stacks, interoperate only through contracts and standards | Maximum autonomy | Duplicated tooling, hard interoperability, higher cost |
| Mesh of meshes | Business units each run a mesh, connected by a thin global layer | Fits conglomerates and acquisitions | Two governance levels |

Most companies that succeed land on hub and spoke: one platform, one catalog, one identity system, domain-owned products, and a small central team owning master data and shared dimensions.

**When not to build a mesh.** It requires domains with enough data skill to own products, and an organisation large enough that the central team is really a bottleneck. A company with one data team of eight people and fifteen sources pays the coordination cost (product contracts, versioning, governance meetings) without the benefit. A useful signal: if the central backlog is dominated by requests that need domain knowledge, and several domains already have engineers who could own data, the mesh pays off.

> **Pitfall:** Renaming the data lake to "mesh" without changing ownership. If domains have no data engineers, no product owners and no budget, the "data products" are central tables with new labels, and the bottleneck stays.

> **Tip:** Start with two or three domains that are willing and capable, with a platform that already offers templates for one product type (a BigQuery dataset with contract, tests and a sharing listing). Expand when those products have real consumers.

::: deeper
**Cost and duplication.** Domains often recompute the same things (each builds its own customer dimension). The hub owns shared entities to prevent that; the platform catalog makes duplicates visible. Per-domain projects make each domain's BigQuery bill visible, which is also an incentive to retire unused products.

**Cross-domain joins and performance.** Sharing through BigQuery sharing or authorized views does not copy data, and queries across domain projects in the same location work normally. Cross-location products cannot be joined in one query, so a mesh needs a global location policy.

**Mesh and streaming.** Products can be streams: a domain publishes a curated `orders.events.v1` topic with a schema, retention and SLA, alongside its tables. Consumers subscribe instead of reading the domain's internal topics.

**Measuring success.** Lead time to a new data product, share of products with active consumers, number of requests waiting on the central team, and incidents caused by unannounced schema changes. If these do not improve after a year, the mesh is decoration.

> **Interview:** A common question is "what is the difference between a data mesh and a data lake with access control?" The difference is ownership and accountability, not technology: in a mesh, the producing domain is responsible for the product's quality, contract and SLA, and the platform makes that cheap to do.
:::

::: check
?? Which data mesh principle prevents domains from each building their own infrastructure from scratch?
The self-serve data platform: a platform team provides the shared tools (templates, pipelines, catalog, access) so domains build products without infrastructure expertise.

?? Why does a mesh need global rules about identifiers and timestamps?
Products from different domains are only joinable if they agree on keys (one `customer_id`), units, currencies and time zones. Without these, a mesh becomes a set of silos.

?? A company has one data team of six and twenty sources. Should it adopt a data mesh?
Probably not. The central team is not yet a bottleneck that needs domain ownership to fix, and the overhead of product contracts and federated governance would outweigh the benefits. Good practices from mesh (contracts, owners, a catalog) can still be adopted.
:::

::: resources
- Book: [Data Mesh](https://www.oreilly.com/library/view/data-mesh/9781492092384/) · Zhamak Dehghani's full treatment of the principles and architecture
- Article: [Data mesh principles and logical architecture](https://martinfowler.com/articles/data-mesh-principles.html) · the four principles with diagrams
- Article: [How to move beyond a monolithic data lake to a distributed data mesh](https://martinfowler.com/articles/data-monolith-to-mesh.html) · the original 2019 article
- Docs: [Architecture and functions in a data mesh](https://docs.cloud.google.com/architecture/data-mesh) · Google's reference for building a mesh on Google Cloud
- Article: [Data Mesh Architecture](https://www.datamesh-architecture.com/) · practical patterns, topologies and tool examples
- Book: [Data Management at Scale, 2nd edition](https://www.oreilly.com/library/view/data-management-at/9781098138851/) · Strengholt's governed-mesh and hub-and-spoke approach
:::

### Real-Time Analytics System Design

::: plain
A hospital has two kinds of records. The monitor beside the bed shows heart rate right now, updated every second, but only for the last few hours. The medical file holds years of history, complete and checked, but nobody expects it to show the last minute. A real-time analytics system is built the same way: a fast, recent "hot path" next to a complete, slower "cold path", and a front desk that knows which one to ask.
:::

**Start from latency classes, not tools.** Requirements for "real-time" range over six orders of magnitude, and each class has a different design:

| Class | Freshness / query latency | Example | Typical Google Cloud design |
|---|---|---|---|
| Interactive serving | Data seconds old, queries under 100 ms, thousands of QPS | "Views on your video in the last hour" in a creator app | Dataflow pre-aggregates into Bigtable; or an RTOLAP engine (Apache Pinot, Druid, ClickHouse) on GKE |
| Operational dashboards | Seconds to a minute, queries under 2 s | Order operations wall screen | Pub/Sub BigQuery subscription or Storage Write API, materialized views, BI Engine |
| Near real-time analytics | 1 to 15 minutes | Marketing campaign monitoring | Micro-batch Dataform runs or BigQuery continuous queries |
| Batch analytics | Hours to a day | Finance, planning | Batch lakehouse |

Write the class next to every requirement before drawing anything. A design that puts every metric on the fastest path is expensive; one that treats an operational screen as batch is useless.

[[fig:design_realtime]]

**Hot path plus cold path.** The **hot path** consumes events as they arrive, keeps a recent window (hours to days), and serves low-latency queries. The **cold path** stores the full history in the lakehouse, is reprocessed when logic changes, and is the source of truth. This is the Lambda shape, but without its classic flaw if both paths share one codebase or one definition: Dataflow can run the same Beam transforms in streaming and batch mode, and SQL definitions can be shared between a continuous query and a batch model. The Kappa alternative uses only the stream and replays it from retained history (Kafka with long retention, or Iceberg as the replay source); it fits when the stream retains enough history and replays are rare. Background is in [Streaming systems](#streaming) and [Real-time OLAP engines](#rtolap).

**Pre-aggregation tiers.** Most real-time queries ask for counts, sums and distinct counts by a few dimensions over time. Storing raw events and aggregating at query time works up to a point; beyond it, pre-aggregate in tiers.

```python
eps = 50_000                              # events per second
raw_rows_day = eps * 86_400
active_groups_per_min = 30_000            # distinct (country, device, campaign) seen per minute
minute_rows_day = active_groups_per_min * 1_440
hour_rows_day = 120_000 * 24              # more groups per hour, fewer rows overall
print(f"raw rows/day:     {raw_rows_day:,}")                 # raw rows/day:     4,320,000,000
print(f"1-min rows/day:   {minute_rows_day:,}")              # 1-min rows/day:   43,200,000
print(f"1-hour rows/day:  {hour_rows_day:,}")                # 1-hour rows/day:  2,880,000
print(f"raw to 1-min reduction: {raw_rows_day // minute_rows_day}x")  # raw to 1-min reduction: 100x
```

A dashboard over the last 7 days reads about 20 million hourly rows instead of 30 billion raw events. Keep raw events for drill-down and reprocessing, 1-minute rollups for recent detail, hourly and daily rollups for long ranges. Distinct counts need mergeable sketches (HyperLogLog: BigQuery `HLL_COUNT.INIT` and `HLL_COUNT.MERGE`, or theta sketches in Druid), because distinct counts cannot be summed across rollup rows.

**Query routing by latency class.** A serving API (or the BI tool's semantic layer) decides where each query goes:

- time range within the last 2 hours and a known metric: the hot store (Bigtable rows keyed by `metric#entity#minute`, or the RTOLAP engine);
- longer ranges: the hourly or daily rollup table in BigQuery;
- unusual dimensions or raw drill-down: the warehouse, with an explicit "slower query" expectation.

A query spanning the boundary is split: older buckets from the rollup, the recent part from the hot store, merged in the API. The boundary must overlap slightly, and buckets must be computed identically in both paths, or numbers jump at the seam.

**SLA-driven architecture.** Work backwards from the number the product promises. "p99 under 200 ms at 2,000 QPS, data at most 30 seconds old" means: Dataflow window triggers every 10 seconds with watermark allowance; Bigtable with row keys designed so each query reads one row range; enough Bigtable nodes for the peak QPS with headroom; and an alert on end-to-end lag (event time to serving time), not only on job health.

**Caching and warm-up.** Caches hide latency only when they are warm. BI Engine reservations accelerate repeated BigQuery dashboard queries once data is loaded into memory; materialized views are refreshed incrementally; RTOLAP engines load segments to local disk or memory after a restart. After a deploy or failover, replay the top queries (from query logs) before switching traffic, so the first users do not hit cold caches. Set cache TTLs to the freshness class: a cache that lives 10 minutes cannot serve a 30-second freshness requirement.

> **Pitfall:** Measuring the pipeline but not the user's view. A Dataflow job can be healthy while the dashboard shows 20-minute-old data because of a cache or a slow materialized view refresh. Measure freshness at the point of consumption: the newest event time visible in the serving query.

> **Tip:** Publish a single "data as of" timestamp with every real-time response. Users trust numbers more when they know how fresh they are, and support tickets drop.

::: deeper
**Late data in the hot path.** Windows close at the watermark plus allowed lateness. Events arriving later are dropped from the hot aggregate but still land in the cold path, so hot and cold numbers differ slightly for recent windows. Document it, and replace hot numbers with cold ones once the cold path has processed a window ("restatement").

**Exactly-once versus at-least-once.** Counting impressions for billing needs exactly-once results: Dataflow exactly-once processing plus idempotent sinks, or at-least-once with event IDs and dedupe. For a monitoring dashboard, at-least-once with rare duplicates is often acceptable and cheaper. Decide per metric.

**Choosing the hot store.** Bigtable for known access patterns at very high QPS (pre-computed keys); an RTOLAP engine when users slice by arbitrary dimensions with sub-second latency; BigQuery with BI Engine when concurrency is modest (tens of dashboards) and freshness of seconds to a minute is enough. Many teams start with BigQuery and add Pinot or Druid only when concurrency or latency forces it.

**Case studies.** Uber's real-time platform combined Kafka, Flink, Pinot and a SQL layer to serve thousands of internal and external dashboards; Facebook's Scuba kept recent data in memory across many servers for interactive drill-down; Netflix uses Druid for real-time quality-of-experience metrics. All three separate a fast recent store from long-term storage.

> **Interview:** "Design a real-time dashboard for ad impressions and clicks." State the latency class, size events per second, choose event-time windows with lateness, pre-aggregate by minute and campaign, store hot aggregates with a TTL, keep raw events in the lakehouse for billing-grade recomputation, and explain how hot numbers are replaced by reconciled cold numbers.
:::

::: check
?? Why can't distinct users be stored as plain counts in minute rollups?
The same user can appear in many minutes, so summing minute counts overcounts. Store a mergeable sketch (HyperLogLog) per rollup row and merge sketches for longer ranges.

?? A query asks for the last 24 hours. The hot store keeps 2 hours. How is it answered?
The router splits it: the first 22 hours from the hourly rollup in BigQuery, the last 2 hours from the hot store, using identical bucket boundaries, merged in the serving API.

?? Where should you measure freshness for a real-time dashboard?
At the point of consumption: the newest event time visible to the serving query, compared with the current time. Job health alone misses delays in caches, views and sinks.
:::

::: resources
- Docs: [BigQuery continuous queries](https://docs.cloud.google.com/bigquery/docs/continuous-queries-introduction) · SQL over streaming data written to BigQuery
- Docs: [Bigtable schema design](https://docs.cloud.google.com/bigtable/docs/schema-design) · row keys for time-series and pre-aggregated serving
- Blog: [Uber's big data platform: 100+ petabytes with minute latency](https://www.uber.com/blog/uber-big-data-platform/) · how Uber evolved from batch to minute latency
- Paper: [Scuba: diving into data at Facebook](https://research.facebook.com/publications/scuba-diving-into-data-at-facebook/) · in-memory real-time analytics at large scale
- Blog: [How Netflix uses Druid for real-time insights](https://netflixtechblog.com/how-netflix-uses-druid-for-real-time-insights-to-ensure-a-high-quality-experience-19e1e8568d06) · RTOLAP for quality-of-experience metrics
- Article: [Questioning the Lambda architecture](https://www.oreilly.com/radar/questioning-the-lambda-architecture/) · Jay Kreps' case for Kappa
:::

### Designing Scalable Ingestion Systems

::: plain
An ingestion system is a post office for data. Letters arrive from thousands of senders, some dropped off (push) and some collected on rounds (pull). The office must sort them, forward copies to several departments, cope with the Christmas rush without losing anything, and put damaged letters aside for a human to look at instead of throwing them away. Designing ingestion means planning those rounds, the sorting room and the rush.
:::

[Data ingestion](#ingestion) covers each technique (CDC, APIs, files, logs, throughput engineering). This topic is about designing an ingestion **system** that handles many sources with predictable guarantees.

**Push versus pull.**

| | Push (source sends) | Pull (platform fetches) |
|---|---|---|
| Examples | Apps publishing to Pub/Sub, webhooks, devices | API polling, database CDC, SFTP pickup, SaaS connectors |
| Who controls rate | The source; the platform must absorb bursts | The platform; it fetches at its own pace |
| Failure handling | Source must retry; platform needs a durable front door | Platform retries; source must keep data available |
| Freshness | Immediate | Polling interval or log position |
| Design need | Autoscaling endpoint, buffer, authentication per sender | Checkpoints (cursors, offsets), rate limiting, scheduling |

Push systems need a durable buffer at the edge; on Google Cloud that is a Pub/Sub topic (or Managed Kafka), optionally behind a Cloud Run endpoint that validates and publishes webhook calls. Pull systems need a state store for cursors (a small Firestore or Cloud SQL table, or the orchestrator's state).

**Fan-out.** Land each source once, then let many consumers read it. A Pub/Sub topic with several subscriptions (one BigQuery subscription for bronze, one Dataflow subscription for real-time features, one Cloud Storage subscription for raw archive) gives each consumer its own position and its own failures. Never let consumers read from the source system directly; every extra reader of a production database is a risk.

**Back-pressure.** When downstream is slower than upstream, something must give. The options, best first:

1. **Buffer durably** and let consumers catch up: Pub/Sub retains unacknowledged messages for 7 days by default (configurable up to 31 days), Kafka for its retention period.
2. **Slow the consumer's intake** with flow control (Pub/Sub client `max_outstanding_messages` and `max_outstanding_bytes`) so workers are not overwhelmed and autoscaling can react.
3. **Slow the producer**: rate limits and 429 responses at the push endpoint, so senders retry with backoff.
4. **Shed load**: sample or drop low-value data, deliberately and visibly.

Size catch-up capacity explicitly. After an outage, the consumer must process the backlog *and* the live traffic:

```python
live_rate = 20_000          # messages/s arriving
consumer_capacity = 30_000  # messages/s the consumers can process at max scale
outage_hours = 2

backlog = live_rate * outage_hours * 3_600
drain_rate = consumer_capacity - live_rate
print(f"backlog: {backlog:,} messages")                          # backlog: 144,000,000 messages
print(f"time to catch up: {backlog / drain_rate / 3_600:.1f} h")  # time to catch up: 4.0 h
```

With only 50% headroom, a 2-hour outage costs 4 more hours of lag. If the freshness SLA is 1 hour, you need far more burst capacity (or a cheaper backfill path, such as loading the backlog from the raw archive in batch).

**Multi-source ingestion as a framework.** At 10 sources, each pipeline can be hand-written. At 300, ingestion must be **config-driven**: a metadata table or YAML file per source (type, connection secret name, schedule, mode full or incremental, cursor column, target table, owner, SLA), and generic, tested code that reads the config. New sources become pull requests that add a config file. Managed connectors (BigQuery Data Transfer Service, Datastream, Fivetran, Airbyte) cover common SaaS and databases; custom Cloud Run jobs cover the rest.

```yaml
source: crm_accounts
type: rest_api
secret: projects/shop-prod/secrets/crm-token
schedule: "*/30 * * * *"
mode: incremental
cursor: updated_at
target: bronze.crm_accounts
owner: sales-data@shop.example
sla: {freshness_minutes: 60, completeness_check: row_count_vs_api_total}
```

**Schema registry integration.** Events carry a schema contract. Pub/Sub supports **schemas** (Avro or Protocol Buffers) attached to a topic, with revisions; publishing a message that does not match is rejected at the door. For Kafka, a schema registry (Confluent Schema Registry or Apicurio) stores versions and enforces compatibility modes (backward, forward, full). Producers register schemas in CI, so incompatible changes fail before deployment, not at 3 a.m.

**Ingestion SLAs.** Each source gets measurable targets: **freshness** (data no older than X when queried), **completeness** (row counts reconcile with the source within tolerance), and **latency** (time from source commit to bronze). Monitor them per source in one dashboard, with alerts routed to the owner in the config.

**Dead-letter handling.** A message that cannot be processed after a few attempts goes to a **dead-letter topic** (Pub/Sub supports this per subscription, with maximum delivery attempts between 5 and 100) or a quarantine table, with the error and the original payload. A DLQ needs an owner, an alert on its size, and a replay tool that republishes fixed messages to the main topic. A DLQ nobody reads is a slow data loss.

> **Pitfall:** Transforming during ingestion. Parsing, filtering and joining in the ingest layer couples ingestion to business logic, so a logic bug loses data. Land raw first; transform from bronze.

> **Tip:** Stamp every record at ingestion with source, ingestion time, batch or offset and schema version. Debugging, dedupe, incremental processing and lineage all depend on these four columns.

::: deeper
**Ordering.** Pub/Sub ordering keys give per-key order within a region; Kafka gives per-partition order. Most analytics does not need global order, only per-entity order (all events of one order in sequence), and even that can usually be restored downstream with a sequence number. Requiring global order caps throughput at one partition.

**Exactly-once at ingestion.** Pub/Sub exactly-once delivery (per subscription, in a region) and Kafka idempotent producers reduce duplicates, but end-to-end exactly-once still needs idempotent writes downstream: event IDs and `MERGE`, or the BigQuery Storage Write API with offsets in committed streams.

**Multi-tenant ingestion.** When many customers send data (a SaaS analytics product), isolate tenants: per-tenant quotas, separate subscriptions or partitions for large tenants, and tenant ID in every key so a noisy tenant cannot starve the others.

**Case studies.** LinkedIn's Brooklin is a single service that streams data from many sources (databases, Kafka) to many destinations, an example of ingestion as a platform. Uber's Hudi came from incremental ingestion needs: upserting changes into a petabyte lake with minute latency.

> **Interview:** For "ingest from 500 sources", answer with the framework: classification of sources (CDC, API, file, event), config-driven connectors, one durable landing layer, per-source SLAs and owners, a DLQ with replay, and schema contracts. Then give numbers for the largest source.
:::

::: check
?? Consumers process 30,000 messages per second at most, live traffic is 25,000 per second, and an outage lasted 1 hour. How long does catching up take?
Backlog is 25,000 x 3,600 = 90 million messages. The net drain rate is 5,000 per second, so catching up takes 18,000 seconds, 5 hours.

?? Why use fan-out from one topic instead of letting each consumer read the source?
The source is read once, so production systems are protected; each consumer has its own subscription, position and failure handling; and new consumers can be added without touching the source.

?? What makes a dead-letter queue useful rather than a slow data loss?
An owner, an alert on its size and age, the original payload with the error reason, and a tested replay path that republishes fixed messages.
:::

::: resources
- Docs: [Pub/Sub overview](https://docs.cloud.google.com/pubsub/docs/overview) · topics, subscriptions, retention and delivery types
- Docs: [Pub/Sub dead-letter topics](https://docs.cloud.google.com/pubsub/docs/handling-failures) · retry policy, maximum delivery attempts and forwarding
- Docs: [Pub/Sub schemas](https://docs.cloud.google.com/pubsub/docs/schemas) · Avro and Protocol Buffer schemas and revisions on topics
- Blog: [Open sourcing Brooklin](https://engineering.linkedin.com/blog/2019/brooklin-open-source) · LinkedIn's many-to-many streaming ingestion service
- Blog: [Hudi: Uber engineering's incremental processing framework](https://www.uber.com/blog/hoodie/) · incremental ingestion into a very large lake
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · logs, stream processing and back-pressure fundamentals
:::

### Feature Store Architecture

::: plain
A feature store is a shared pantry for machine learning ingredients. Data scientists prepare ingredients (features such as "orders in the last 30 days") once, label them, and store them in two places: a big storeroom with the full history, used to train models, and a small fridge next to the stove with only today's values, used to serve predictions in milliseconds. Both are filled from the same recipe, so the model tastes the same in training as in production.
:::

A **feature** is an input to a model, computed from raw data: `orders_30d`, `avg_basket_value`, `minutes_since_last_login`. A **feature store** manages features across their life: definition, computation, storage for training, low-latency serving and monitoring. [Operational databases, AI and governance on Google Cloud](#gcpdata) shows Agent Platform Feature Store's setup; this topic is the architecture behind any feature store, and the problems it solves.

[[fig:design_featurestore]]

**Two stores, one definition.**

| | Offline store | Online store |
|---|---|---|
| Purpose | Training sets, batch scoring, analysis | Real-time inference |
| Content | Full history of feature values with timestamps | Latest value per entity |
| Access | Large scans and point-in-time joins | Key lookup, single-digit milliseconds, high QPS |
| Google Cloud | BigQuery tables | Bigtable (Agent Platform Feature Store's Bigtable online serving), Memorystore for Redis |
| Open source | Parquet or Iceberg, BigQuery via Feast | Redis, Bigtable, PostgreSQL via Feast |

The online store is **materialised** from the offline store (or written directly by a streaming job) on a schedule matching each feature's freshness need.

**Point-in-time correct joins.** The central problem. To build training data, each label (did this customer churn on 2026-03-10?) must be joined to the feature values *as they were known at that time*, never later. Joining to today's values leaks the future into training (**label leakage**), producing a model that looks excellent offline and fails in production. The join is an "as of" join: for each label row, take the latest feature row with `feature_ts <= label_ts`.

```python
from bisect import bisect_right

# feature history for customer 7: (timestamp, orders_30d)
history = [("2026-03-01", 2), ("2026-03-08", 5), ("2026-03-15", 9)]
times = [t for t, _ in history]

def as_of(label_ts):
    i = bisect_right(times, label_ts)     # rows with timestamp <= label_ts
    return history[i - 1][1] if i else None

labels = ["2026-02-20", "2026-03-10", "2026-03-15", "2026-04-01"]
print([as_of(ts) for ts in labels])       # [None, 5, 9, 9]
print(history[-1][1])                     # 9  <- a naive join would use this for every label
```

The label on 2026-03-10 must see 5, not 9. In BigQuery the same join is a window or `QUALIFY` over a range join, or the `ML.FEATURES_AT_TIME` and `ML.ENTITY_FEATURES_AT_TIME` functions that do it for you:

```sql
SELECT *
FROM ML.ENTITY_FEATURES_AT_TIME(
  TABLE features.customer_daily,     -- entity_id, feature_timestamp, feature columns
  TABLE ml.churn_labels,             -- entity_id, time (the label timestamp)
  num_rows => 1,
  ignore_feature_nulls => TRUE);
```

Feast's `get_historical_features` and Agent Platform Feature Store's registry perform the same point-in-time logic.

**Feature computation pipelines.** Features fall into three kinds by freshness:

- **Batch features** (daily or hourly): SQL in BigQuery over silver tables, scheduled by Dataform or Airflow. Most features belong here.
- **Streaming features** (seconds to minutes): `clicks_last_5_min`, computed by Dataflow over Pub/Sub with sliding windows, written to the online store and appended to the offline store for training.
- **On-demand features** (computed at request time from request data): `distance_between_user_and_store`. They are code, shared between the serving service and the training pipeline so both compute them identically.

**Training-serving skew** is the second big problem: features computed one way in training (SQL) and another in serving (Java in the app). The cure is one definition used by both paths, which is what the store's registry enforces.

**The main products.**

| Product | Architecture notes |
|---|---|
| Agent Platform Feature Store | BigQuery is the offline store (no copy); feature views sync to Bigtable online serving; optional registry for feature groups and monitoring |
| Feast (open source) | A registry plus SDK over your own stores: BigQuery or files offline, Redis, Bigtable or others online; `materialize` jobs copy to online |
| Tecton | Managed feature platform with declarative batch, streaming and real-time features (acquired by Databricks in 2025) |
| Hopsworks | Feature store with an online store built on RonDB and an offline store of lakehouse tables, plus a model registry |

A Feast setup on Google Cloud:

```yaml
# feature_store.yaml
project: shop
provider: gcp
registry: gs://shop-feast/registry.db
offline_store:
  type: bigquery
  dataset: feast_offline
online_store:
  type: redis
  connection_string: "10.0.0.5:6379"     # Memorystore for Redis, private IP
entity_key_serialization_version: 3
```

```python
from feast import FeatureStore
store = FeatureStore(repo_path=".")
training = store.get_historical_features(
    entity_df=labels_df,                       # customer_id, event_timestamp, label
    features=["customer_stats:orders_30d", "customer_stats:avg_basket"],
).to_df()
store.materialize_incremental(end_date=now)    # copy new values to Redis
online = store.get_online_features(
    features=["customer_stats:orders_30d"], entity_rows=[{"customer_id": 7}]
).to_dict()
```

> **Pitfall:** Feature timestamps set to the processing time instead of the time the value became valid. A daily job that runs at 06:00 for yesterday's data should stamp features with the end of yesterday (or the time they were actually available, if you want to model serving delay honestly). Wrong timestamps silently break point-in-time correctness.

> **Tip:** Size the online store from the serving side: entities x features x bytes, and peak QPS x features per request. 20 million customers x 50 features x 8 bytes is about 8 GB of values before overhead, small for Bigtable; the QPS and tail latency decide the node count.

::: deeper
**Freshness versus cost.** Syncing every feature every minute is expensive. Classify features by required freshness and sync each feature view on its own schedule. Fraud features may need seconds (streaming); recommendation profiles may be fine daily.

**Backfills.** A new feature needs history for training. Compute it over past data in batch with the same definition and write it to the offline store with historical timestamps. Streaming-only features have no history until they have run for a while, unless the batch definition can reproduce them from raw events.

**Monitoring.** Track feature freshness (time since last sync), null rates, distribution drift between training and serving (for example population stability index), and online lookup latency. Agent Platform Feature Store's registry offers drift monitoring; otherwise log served features to BigQuery and compare.

**Embeddings.** Feature stores increasingly hold embeddings. Exact key lookup of an embedding is a feature store job; nearest-neighbour search is a vector index job (Vector Search, AlloyDB with pgvector, BigQuery vector search). Do not force one into the other.

> **Interview:** "How do you prevent training-serving skew?" One feature definition in a registry, used by both the offline pipeline and online serving; point-in-time joins for training sets; logging served features and comparing their distributions with training data.
:::

::: check
?? What goes wrong if a training set joins labels to the latest feature values?
Label leakage: training rows see information from after the label time, so offline metrics look much better than real performance, and the model disappoints in production.

?? In the Python example, why does the label on 2026-02-20 get `None`?
No feature row has a timestamp at or before 2026-02-20, so the feature was unknown at that time. The correct value is missing, not the earliest later value.

?? Which features should be streaming rather than batch?
Only those whose value must reflect events from the last seconds or minutes for the model to work, such as recent clicks or transactions for fraud. Most features are fine as daily or hourly batch.
:::

::: resources
- Docs: [Feature Store on Gemini Enterprise Agent Platform](https://docs.cloud.google.com/vertex-ai/docs/featurestore/latest/overview) · BigQuery offline store, feature views and Bigtable online serving
- Docs: [Feast documentation](https://docs.feast.dev/) · the open-source feature store's concepts, stores and SDK
- Docs: [ML.ENTITY_FEATURES_AT_TIME](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-entity-feature-time) · point-in-time feature lookup in BigQuery
- Blog: [What is a feature store?](https://www.tecton.ai/blog/what-is-a-feature-store/) · the components of a feature platform explained by Tecton
- Blog: [Meet Michelangelo: Uber's machine learning platform](https://www.uber.com/blog/michelangelo-machine-learning-platform/) · the platform that popularised feature stores
- Docs: [Hopsworks documentation](https://docs.hopsworks.ai/) · online and offline feature groups in practice
:::

## Products and platforms

The architectures above decide where data flows. This group is about the units people actually use and the platform that makes building them cheap. A **data product** is a dataset (or stream, or API) built for consumers, with an owner, a contract and a service level. The first topic designs the product's interface: its specification, output ports, versions and deprecation. The second is about building and running products over their life, as a team with users. The third designs the self-service platform underneath, so that a domain team can ship a product in days without filing tickets.

These ideas come from data mesh, but they are useful in a central platform too: a central team that treats its gold tables as products with contracts and SLAs is more trusted than one that treats them as by-products of pipelines.

### Data Product Design Patterns

::: plain
A data product is like a packaged food in a shop. The label tells you what is inside, how much, the best-before date and who made it. The recipe can change, but the label and the nutrition table follow rules, and when a product is discontinued the shop gives notice. Designing a data product means designing that label, the ways customers can buy it, and the rules for changing it.
:::

**The product specification.** Every product has a machine-readable spec stored with its code and published to the catalog. A practical shape, following the ideas of the Open Data Contract Standard (ODCS) and the data product canvas:

```yaml
product: orders
version: 2.3.0
domain: commerce
owner: {team: commerce-data, contact: commerce-data@shop.example, product_owner: orders-po@shop.example}
description: One row per order, current state, all channels.
output_ports:
  - name: orders_table
    type: bigquery_dataset
    location: EU
    ref: shop-commerce-prod.orders_v2.orders
    access: request via group commerce-orders-readers
  - name: order_events
    type: pubsub_topic
    ref: projects/shop-commerce-prod/topics/orders.events.v2
    schema: schemas/order_event_v2.avsc
schema: schemas/orders_v2.yaml
semantics: {order_value: "gross, incl. tax, EUR", timestamps: UTC}
sla:
  freshness: {max_delay_minutes: 60, measured_on: max(updated_at)}
  availability: 99.5%           # share of hours the table is queryable and passes checks
  completeness: row count within 0.5% of source
quality_checks: [unique(order_id), not_null(customer_id), accepted_values(status)]
classification: {contains_pii: false}
retention: 7 years
lifecycle: {status: active, deprecation_notice_days: 90}
```

**SLA definitions that can be measured.** Each SLA term needs an SLI (the measurement), an SLO (the target) and a published consequence. Freshness is measured on the data, not on job success (newest `updated_at` versus now). Availability is the share of time the port can be queried and passes its checks. Completeness compares with the source. "High quality" is not an SLA; "unique order IDs, checked hourly, 99.9% of checks pass" is. SLO mechanics are in [Distributed systems](#distsys).

**Output port types.** The same product can be offered through several ports, each for a kind of consumer:

| Port | Best for | Google Cloud implementation | Design notes |
|---|---|---|---|
| Dataset (tables, views) | Analytics, BI, data science | BigQuery dataset shared through authorized views or BigQuery sharing; Iceberg tables for other engines | Cheapest to offer; consumers pay their own query cost in their project |
| Stream | Event-driven consumers, real-time features | Pub/Sub topic with schema; Managed Kafka topic | Needs ordering, retention and replay rules |
| API | Applications needing one entity fast, external partners | Cloud Run service over Bigtable or AlloyDB, behind API gateway | Owner must run a service with latency SLOs |
| File export | Partners and legacy tools | Cloud Storage bucket with Parquet or CSV, signed URLs or bucket IAM | Define file naming, completeness marker (`_SUCCESS`), retention |

Offer the fewest ports that serve real consumers. Each port is an interface you must keep stable.

**Versioning strategy.** Use semantic versioning for the contract: **patch** for fixes without schema change, **minor** for backwards-compatible additions (a new nullable column), **major** for breaking changes (a removed or renamed column, a changed meaning, a different grain). Major versions get a new physical interface (`orders_v3` dataset, `orders.events.v3` topic) running in parallel with the old one. Within a major version, consumers read through views, so internal tables can be rebuilt without breaking them:

```sql
-- Stable interface for v2; the underlying table can change
CREATE OR REPLACE VIEW `shop-commerce-prod.orders_v2.orders` AS
SELECT order_id, customer_id, status, order_value_eur AS order_value, created_at, updated_at
FROM `shop-commerce-prod.orders_internal.orders_current`;
```

CI compares the new schema with the published contract and fails a build that makes a breaking change without a major version bump.

**Deprecation policy.** A written, predictable policy: announce in the catalog and to registered consumers; run old and new versions in parallel for the notice period (for example 90 days); track who still reads the old version from audit logs (`INFORMATION_SCHEMA.JOBS` referenced tables, or Data Access audit logs); contact remaining consumers; then remove access before deleting data. Never delete a product someone queried last week without talking to them.

**Discoverability design.** A product nobody can find has no consumers. Register it automatically from the spec in Knowledge Catalog as a data product with its assets, owner, description, glossary terms and access groups. Good descriptions answer three questions: what is one row, where does it come from, and what should I not use it for. Sample queries and a link to the contract help more than a long description.

> **Pitfall:** Publishing internal tables as products. If consumers read `orders_internal.orders_current` directly, every refactor is a breaking change. Expose only views or dedicated tables named as the product's interface.

> **Tip:** Make the spec the source of truth for automation: the platform reads it to create datasets, grant access, register catalog entries, configure quality scans and freshness alerts. Then documentation cannot drift from reality.

::: deeper
**Contract enforcement points.** Producer-side checks in CI (schema diff against the published contract), producer-side runtime checks before publication (quality gates, write-audit-publish), and consumer-side checks for critical uses. See [Pipelines and transformation](#pipelines) for data contracts in detail.

**Grain changes are the most dangerous breaking change.** Changing from one row per order to one row per order line keeps column names valid and silently doubles every sum. Treat grain as part of the contract and a change of grain as a major version.

**Cost of ports.** Dataset ports put query cost on consumers (in their own project). API ports put serving cost on the producer. Streams cost the producer storage and throughput. Charge or show back accordingly, or producers will avoid offering the more expensive ports.

> **Interview:** "How would you change a column's meaning without breaking consumers?" Do not change it in place. Add a new column with the new meaning (minor version) or publish a new major version, run both, announce deprecation of the old one with a date, monitor usage, then remove.
:::

::: check
?? A product team wants to rename `amount` to `order_value`. What kind of change is it, and how is it released?
A breaking change (major version). Publish `orders_v3` with the new name alongside `orders_v2`, announce deprecation of v2 with a notice period, track remaining users and remove v2 after the period. Alternatively add `order_value` as a new column in v2 (minor) and deprecate `amount` later.

?? Why is "the pipeline succeeded" a poor freshness SLI?
A job can succeed while loading nothing new, or loading stale source data. Freshness should be measured on the data itself, such as the newest `updated_at` in the table compared with the current time.

?? When would you offer an API port instead of a dataset port?
When consumers are applications that need a single entity at low latency, or external partners who should not query a warehouse. It costs the producer more, because they must run a service with latency SLOs.
:::

::: resources
- Article: [Data Product Canvas](https://www.datamesh-architecture.com/data-product-canvas) · a one-page template for designing a data product
- Spec: [Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/latest/) · the open YAML standard for data contracts, schemas and SLAs
- Docs: [Knowledge Catalog data products](https://docs.cloud.google.com/dataplex/docs/data-products-overview) · registering and sharing data products on Google Cloud
- Docs: [Introduction to BigQuery sharing](https://docs.cloud.google.com/bigquery/docs/analytics-hub-introduction) · listings and exchanges as dataset output ports
- Book: [Data Mesh](https://www.oreilly.com/library/view/data-mesh/9781492092384/) · chapters on designing data products and their affordances
- Spec: [Semantic Versioning 2.0.0](https://semver.org/) · the versioning rules applied to contracts
:::

### Building and operating data products

::: plain
Launching a data product is like opening a café, not like building a bridge. The opening day matters, but what decides success is everything after: regulars who come back, a menu that changes with their tastes, someone in charge when the coffee machine breaks, and closing the café properly if nobody comes. Building and operating a data product means running it as a small business with customers, for as long as it lives.
:::

**Product thinking.** A product starts from consumers and a problem, not from a source table. Before building, answer: who will use it (named teams and use cases), what decision or system it feeds, what grain and freshness they need, and how you will know it succeeded (active consumers, queries per week, decisions supported, tickets avoided). Products without a named first consumer usually become unused tables that still cost money.

**Ports in both directions.** A product has **input ports** (what it consumes: source tables, other products, streams) and **output ports** (what it offers). Declaring inputs makes dependencies explicit: lineage tools show the graph, and an upstream product's deprecation notice reaches the right owners. The rule from data mesh is that a product consumes other products through their output ports, never through their internal tables.

**The ownership model.**

| Role | Responsibility |
|---|---|
| Product owner | Consumers, roadmap, priorities, deprecation decisions; the person a consumer calls |
| Data engineers in the domain | Build and run pipelines, tests and ports; on-call for the product |
| Platform team | Tools, templates, infrastructure, catalog; not the product's content |
| Governance group | Global policies the product must satisfy |
| Consumers | Register usage, report issues, give notice of new requirements |

One accountable owner per product, named in the spec and the catalog. "The data team" is not an owner.

**The life cycle.** 1. **Discover**: consumers, use cases, existing products to reuse. 2. **Design**: spec, ports, contract, SLA (previous topic). 3. **Build** from the platform template: pipeline, tests, quality gates, monitoring. 4. **Launch**: catalog entry, documentation, sample queries, first consumers onboarded, SLO dashboard live. 5. **Operate**: incidents, changes, cost, feedback. 6. **Evolve**: new versions with deprecation. 7. **Retire**: when usage falls to zero or a replacement exists.

**Operating against the SLA.** Operation is measurable. A small example computes SLO compliance for freshness over a month of hourly checks:

```python
# One entry per hourly check: minutes behind the source (None = check failed)
checks = [12] * 700 + [75] * 15 + [None] * 5      # 720 hours in a 30-day month
slo_minutes, target = 60, 0.995

good = sum(1 for c in checks if c is not None and c <= slo_minutes)
compliance = good / len(checks)
budget_hours = (1 - target) * len(checks)
print(f"compliance: {compliance:.2%}")                 # compliance: 97.22%
print(f"error budget: {budget_hours:.1f} h, used: {len(checks) - good} h")  # error budget: 3.6 h, used: 20 h
```

The product breached its SLO: 20 bad hours against a budget of 3.6. The error-budget policy applies (reliability work before new features), and consumers get an incident summary. Publishing this monthly is what turns an SLA from a promise into a practice.

**Interoperability.** Products are valuable in combination. Global standards make them combinable: shared identifiers from master data, UTC timestamps, ISO currency codes and explicit currency columns, consistent naming (`snake_case`, `_id` suffix for keys, `_at` for timestamps), and a common format (BigQuery tables or Iceberg). Each product's documentation names its join keys and which product defines them.

**Discoverability in practice.** Beyond catalog registration: announce launches where consumers are (a data newsletter or channel), keep sample queries and a short "how to use" note, show quality and freshness status in the catalog entry, and make access requests one click (a group to join, approved by the owner).

**Cost ownership.** Each product has a cost: storage, pipeline compute, serving. With one project or labels per product, the owner sees it monthly. Cost per active consumer is a useful signal: a product costing $3,000 a month with no queries in 60 days is a retirement candidate.

> **Pitfall:** Launch and forget. A product is built, announced, and then nobody watches its SLO, answers consumer questions or reviews its cost. Within a year it is stale and nobody trusts it. Budget engineering time for operation, typically a significant share of the team's capacity.

> **Tip:** Track three numbers per product on one dashboard: SLO compliance, active consumers (distinct principals querying it in 30 days, from `INFORMATION_SCHEMA.JOBS`) and monthly cost. Together they tell you whether to invest, maintain or retire.

::: deeper
**Usage telemetry on BigQuery.** Active consumers per product come from job metadata:

```sql
SELECT t.dataset_id, COUNT(DISTINCT j.user_email) AS consumers_30d, COUNT(*) AS queries_30d
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION AS j, UNNEST(j.referenced_tables) AS t
WHERE j.creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND t.project_id = 'shop-commerce-prod'
GROUP BY t.dataset_id
ORDER BY consumers_30d DESC;
```

Service accounts appear as users, so map them to the teams that own them.

**Incident communication.** When a product breaks its SLA, consumers need to know before they notice: an automatic status banner in the catalog entry, a message to registered consumers, and a post-incident summary. Incident practice is in [DataOps and platform engineering](#dataops).

**Product maturity levels.** A simple scale helps a portfolio: experimental (no SLA, may change), beta (contract, best-effort SLA), stable (contract, SLA, on-call), deprecated. Show the level in the catalog so consumers choose knowingly.

> **Interview:** "How do you know a data product is successful?" Named consumers using it regularly, SLOs met, decisions or systems depending on it, and cost justified by that usage. Vanity metrics (number of tables, number of products) do not count.
:::

::: check
?? A product has a 99.5% freshness SLO measured hourly over 720 hours. How many bad hours does the error budget allow?
(1 - 0.995) x 720 = 3.6 hours a month.

?? Why should a product consume another product through its output port rather than its internal tables?
Internal tables can change at any time without notice; output ports have a contract, versions and deprecation rules. Consuming internals creates hidden dependencies that break on the producer's next refactor.

?? What signals suggest a product should be retired?
No or very few active consumers for a long period, a replacement product that covers its use cases, and cost or maintenance effort out of proportion to usage.
:::

::: resources
- Book: [Data Mesh](https://www.oreilly.com/library/view/data-mesh/9781492092384/) · Dehghani on data product ownership, life cycle and the product owner role
- Article: [How to move beyond a monolithic data lake to a distributed data mesh](https://martinfowler.com/articles/data-monolith-to-mesh.html) · data products and their qualities (discoverable, addressable, trustworthy)
- Article: [Data Mesh Architecture](https://www.datamesh-architecture.com/) · input and output ports, product teams and examples
- Docs: [INFORMATION_SCHEMA.JOBS view](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs) · job metadata for usage and consumer telemetry
- Book: [Site Reliability Engineering](https://sre.google/sre-book/table-of-contents/) · SLOs, error budgets and incident practice applied to products
:::

### Data Platform Self-Service Design

::: plain
A self-service platform is like a well-equipped shared workshop. Instead of asking the workshop manager to build each shelf for you, you get good tools, safety guards on the saws, labelled materials and a few proven plans. You build your own shelf in an afternoon, and you cannot cut your finger off. The workshop team's job is the tools and guards, not the shelves.
:::

**Why self-service.** In a ticket-driven platform, every new dataset, pipeline, access grant or environment waits for the central team. Lead time is days or weeks, and the central team spends its time on repetitive requests. A self-service platform turns those requests into products that teams use themselves, with guardrails so that the easy way is also the safe way.

**Internal developer platform for data.** Borrowing from platform engineering, a data platform offers **golden paths**: opinionated, supported ways to do the common things.

| Golden path | What the user provides | What the platform does |
|---|---|---|
| New data product | Name, domain, owner, spec YAML | Creates projects or datasets, groups, IAM, catalog entry, CI pipeline, monitoring, budget |
| New ingestion source | Source config (type, secret, schedule, target) | Deploys a connector, schedules it, adds freshness alerts |
| New transformation | SQL models in the product repo | CI runs compile, tests and a dry-run cost estimate; CD deploys to Dataform |
| Sandbox | Team name | A dataset with expiry, a quota and access to approved products |
| Access to a product | Request in the catalog | Owner approval, group membership, audit trail |

Users stay on the golden path because it is faster, not because it is mandatory. Going off-path is allowed with support from the platform team, and repeated off-path needs become new paths.

**Guardrails and abstraction layers.** The platform hides complexity behind a small interface but enforces rules underneath. A Terraform module is a typical abstraction:

```hcl
module "orders_product" {
  source      = "git::https://git.shop.example/platform/tf-data-product.git?ref=v4.2.0"
  name        = "orders"
  domain      = "commerce"
  owner_group = "commerce-data@shop.example"
  location    = "EU"
  readers     = ["group:commerce-orders-readers@shop.example"]
  monthly_budget_usd = 2000
}
```

Inside the module, guardrails the user does not have to think about: the dataset location comes from an allowed list, default table expiration on sandbox datasets, CMEK where required, required labels (`team`, `product`, `env`), no `allUsers` grants, a budget with alerts, Knowledge Catalog registration and quality scan templates. Organization policies and VPC Service Controls enforce the hard limits even for resources created outside the module. Policy checks in CI (for example with OPA/Conftest on the Terraform plan) catch violations before apply.

Levels of abstraction matter. Too low (raw Terraform for every resource) and only experts can use the platform; too high (a form with five fields) and the first unusual need forces users off the platform entirely. Good platforms offer layers: a portal form for the common case, the Terraform module for more control, raw resources with review for the rare case.

**Catalog-driven discovery.** The catalog is the front door: users search for data, read the product page (description, owner, schema, quality, freshness, sample queries), and request access from the same page. On Google Cloud, Knowledge Catalog provides search, data products, aspects and lineage; Looker's semantic model provides governed metrics for business users. The catalog must be populated automatically from the specs and job metadata, or it will be empty and stale.

**Data portal UX.** Many companies put a portal in front of everything (often built on Backstage, the open-source developer portal): a software catalog of pipelines, products and owners; templates for golden paths ("create a data product" runs the scaffolder, opens a repository and a Terraform pull request); documentation; and dashboards of SLOs and costs per team. Measure the portal like a product: time to first query for a new analyst, time to launch a new product, number of tickets to the platform team.

**The platform team.** A platform team builds and runs the platform as a product for internal users: it has a roadmap, users, documentation and SLOs of its own (for example "new product provisioning completes in under 30 minutes"). Team structures (stream-aligned, platform, enabling teams) are covered in [DataOps and platform engineering](#dataops), and self-serve analytics for business users in [Analytics engineering and BI](#analytics).

> **Pitfall:** Building a platform nobody asked for. Teams spend a year on a portal, then discover users wanted faster access requests and a sandbox. Start from the top three repetitive tickets and turn each into a golden path.

> **Tip:** Track "tickets per new product" and "days from idea to first consumer". When both fall, the platform is working.

::: deeper
**Multi-tenancy and quotas.** Self-service means many teams sharing capacity. On BigQuery: per-project custom quotas on bytes scanned per day for on-demand, reservations with assignments per domain folder (and idle slot sharing) for capacity, and `maximum_bytes_billed` defaults in tooling. A runaway query in one sandbox must not slow down finance's month-end close.

**Paved road versus freedom.** Mandating one stack simplifies support but frustrates teams with real special needs. A workable rule: anything on the golden path is supported by the platform team with SLOs; anything off it is the owning team's responsibility, must still pass the policy checks, and is reviewed every quarter for adoption into the paved road.

**Self-service for consumers too.** Self-service is not only for producers. Analysts need a sandbox with access to gold products, a semantic layer for governed metrics and a way to publish a finished analysis as a product (promotion path from sandbox to a managed dataset with tests).

> **Interview:** "Design a self-service data platform for 40 teams." Describe golden paths for the top requests, the abstraction layers (portal, Terraform modules, raw), guardrails (org policies, VPC SC, CI policy checks), catalog-driven discovery and access, quotas and reservations for isolation, and how you would measure success (lead time, tickets, adoption).
:::

::: check
?? What is a golden path, and why should it be optional rather than mandatory?
A supported, opinionated way to do a common task, such as creating a data product. Users choose it because it is faster and safer; keeping it optional (with policy checks still applied) lets special cases proceed and reveals where new paths are needed.

?? Name three guardrails a data product Terraform module can enforce.
Any three of: allowed locations, required labels, CMEK, no public grants, default expiration for sandboxes, budgets and alerts, catalog registration, quality scan templates.

?? How can one team's heavy queries be prevented from slowing down another team on BigQuery?
Separate reservations (with assignments per project or folder) for capacity workloads, custom per-project quotas on bytes scanned for on-demand, and `maximum_bytes_billed` defaults in tools.
:::

::: resources
- Article: [What is platform engineering?](https://platformengineering.org/blog/what-is-platform-engineering) · internal developer platforms and golden paths explained
- Docs: [Backstage: what is Backstage?](https://backstage.io/docs/overview/what-is-backstage) · the open-source developer portal often used for data portals
- Docs: [Terraform blueprints and modules for Google Cloud](https://docs.cloud.google.com/docs/terraform/blueprints/terraform-blueprints) · reusable modules as platform abstractions
- Book: [Team Topologies](https://teamtopologies.com/book) · platform teams and the thinnest viable platform
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · data platform undercurrents and the role of the platform
- Article: [Data Mesh Architecture](https://www.datamesh-architecture.com/) · self-serve data platform capabilities in practice
:::

## Non-functional design

A design that produces the right numbers is only half finished. The other half is the set of qualities nobody asks for until they are missing: what it costs, whether it keeps up next year, what happens when a region fails, whether it respects people's privacy and the law, where the data physically lives, and how you move to it from what exists today. Interviewers and design reviews spend much of their time here, because these are the decisions that are expensive to change later.

Each topic in this group gives a model you can compute with (a cost model, a capacity model, recovery objectives, an egress estimate, a migration plan) rather than a list of features. The earlier sections hold the service details; here they are combined into design decisions.

### Cost Optimization in Data Platforms

::: plain
Running a data platform is like running a delivery fleet. You pay for the warehouses where parcels sit (storage) and for the vans and drivers (compute). Warehouses are cheap per parcel; vans are expensive, and an idle van with its engine running is pure waste. Saving money means keeping fewer vans idling, using cheaper rented vans for flexible jobs, moving old parcels to a cheaper back store, and stopping anyone from sending a lorry to deliver one envelope.
:::

**The cost model.** Every platform bill has the same few lines. Knowing which dominates tells you where to work.

| Line | How it is billed on Google Cloud | Typical share | Main levers |
|---|---|---|---|
| Query compute | BigQuery on-demand per TiB scanned, or slots per slot-hour with editions | Usually the largest | Partitioning, clustering, fewer full scans, capacity pricing |
| Pipeline compute | Dataflow per vCPU, memory and data processed; Spark per VM (plus service fee); Cloud Run per vCPU-second | Large | Right-sizing, ephemeral clusters, Spot VMs, autoscaling |
| Storage | Per GiB-month; BigQuery active and long-term, logical or physical; Cloud Storage classes | Smaller but grows forever | Retention, lifecycle rules, long-term pricing, physical billing |
| Streaming ingestion | Pub/Sub per TiB of throughput; BigQuery Storage Write API per GiB | Medium for event-heavy platforms | Batching, compression, fewer subscriptions |
| Network | Per GiB across zones, regions, continents, to the internet | Small unless the design is wrong | Co-location, avoiding cross-region reads |
| Always-on services | Managed Airflow environments, Bigtable nodes, AlloyDB instances, BI Engine | Fixed baseline | Right-size, autoscale, consolidate |

Attribution, budgets, chargeback and the FinOps practice are in [DataOps and platform engineering](#dataops); service pricing details are in [BigQuery in depth](#bigquery) and [Google Cloud foundations for data](#gcpfound). This topic is about designing for cost.

**Compute: pay only while working.** The biggest waste in most estates is compute that runs when nothing needs it. A worked comparison for a Spark job that needs 10 workers for 50 minutes a day (VM prices approximate, service fees excluded):

```python
workers, usd_per_worker_hour = 10, 0.39        # n2-standard-8, on-demand, approx.
always_on = workers * usd_per_worker_hour * 730               # cluster up all month
ephemeral = workers * usd_per_worker_hour * (50 / 60) * 30    # created per run, deleted after
spot = ephemeral * 0.30                                       # assume 70% Spot discount
print(f"always-on cluster: ${always_on:,.0f}/month")   # always-on cluster: $2,847/month
print(f"ephemeral cluster: ${ephemeral:,.0f}/month")   # ephemeral cluster: $98/month
print(f"ephemeral on Spot: ${spot:,.0f}/month")        # ephemeral on Spot: $29/month
```

Ephemeral compute (serverless batches for Spark, per-run clusters, Cloud Run jobs, Dataflow batch jobs) removes idle time. **Spot VMs** (up to 91% cheaper, can be reclaimed at any time) suit work that can retry: Spark secondary workers, Dataflow batch with FlexRS (flexible resource scheduling, which delays a job by up to 6 hours in exchange for a lower price), Batch jobs. Keep the Spark driver and anything holding state on standard VMs. Details are in [Google Cloud foundations for data](#gcpfound).

**Query cost governance.** Most warehouse waste is a few bad patterns repeated: `SELECT *` on wide tables, dashboards scanning raw tables on every refresh, missing partition filters, and full rebuilds where incremental would do. Build guardrails into the platform:

```sql
-- Force partition filters on large tables
ALTER TABLE silver.events SET OPTIONS (require_partition_filter = TRUE);

-- Cap a scheduled query or tool connection (also settable per job)
-- bq query --maximum_bytes_billed=50000000000 ...   (about 50 GB)

-- Find the most expensive recurring queries of the last 30 days
SELECT query_info.query_hashes.normalized_literals AS query_hash,
       ANY_VALUE(query) AS sample, COUNT(*) AS runs,
       ROUND(SUM(total_bytes_billed) / POW(1024, 4), 1) AS tib_billed
FROM `region-eu`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
GROUP BY query_hash
ORDER BY tib_billed DESC
LIMIT 20;
```

Then fix the top of the list: materialized views or rollups for dashboards, clustering for common filters, incremental models. Add custom quotas on bytes per day for on-demand projects, and move steady workloads to reservations with autoscaling where measured slot usage makes it cheaper.

**Tiered storage policies.** Storage is cheap per GiB but never shrinks unless you design it to. Decide retention per layer (bronze 1 year, silver 3 years, gold as the business requires), set partition expiration on BigQuery tables, and let unmodified partitions drop to long-term pricing automatically after 90 days. In Cloud Storage, lifecycle rules move raw files to Nearline (30-day minimum), Coldline (90 days) and Archive (365 days), or use Autoclass. Mind the minimum durations and retrieval fees: moving data that is read weekly to Coldline costs more, not less. Evaluate physical (compressed) storage billing for BigQuery datasets with good compression.

**Workload scheduling for cost.** With capacity pricing, a reservation sized for the 08:00 peak is idle at night. Shift heavy batch work into off-peak hours, use a small baseline with autoscaling for peaks, and share idle slots between reservations. Stagger pipelines instead of starting everything at 00:00. For Spark, schedule large backfills on Spot capacity at night.

**Unit economics.** Track cost per unit of business value: per TiB ingested, per pipeline run, per dashboard view, per active customer. Absolute cost grows with the business; unit cost should fall.

> **Pitfall:** Committing before optimising. Buying a three-year slot commitment sized on today's wasteful usage locks in the waste. Fix usage first (scans, idle compute), then commit to the reduced baseline.

> **Tip:** Put a cost estimate in every design document: monthly cost at expected volume and at 10x. A dry run (`bq query --dry_run`) gives the bytes a query will scan before it runs.

::: deeper
**Storage versus compute trade-offs.** Spending storage to save compute is usually right: materialized views, rollups, denormalised gold tables and clustering all store more or reorganise data so queries scan less. A rollup that costs $50 a month to store and refresh can save thousands in dashboard scans.

**Streaming costs.** Streaming pipelines run all the time, so a "real-time" requirement that could be 15-minute micro-batches is often the single biggest avoidable cost. Pub/Sub throughput is billed per TiB (about $40 per TiB at the time of writing), so compressing or batching small messages and avoiding unnecessary extra subscriptions matter at high volume.

**Hidden costs.** Cross-region reads, Cloud Logging ingestion of verbose pipeline logs, BigQuery time travel and fail-safe bytes under physical billing, Iceberg snapshots retained for months, idle Managed Airflow environments in every developer project, and Bigtable clusters sized for a peak that no longer happens.

**Case study.** Airbnb's engineering team described reducing Spark infrastructure costs substantially through a combination of autoscaling, instance choices and job tuning, which is the general recipe: remove idle, buy cheaper, do less work.

> **Interview:** "Your BigQuery bill doubled this month. What do you do?" Query `INFORMATION_SCHEMA.JOBS` grouped by user, project and normalized query hash to find what changed; check for new dashboards or scheduled queries scanning raw tables; fix the top offenders (partition filters, materialized views, incremental models); add guardrails (quotas, `maximum_bytes_billed`, required partition filters); then reconsider the pricing model with the new usage.
:::

::: check
?? Why is an ephemeral Spark cluster so much cheaper than an always-on one for a 50-minute daily job?
You pay for VMs only while they exist. An always-on cluster bills 730 hours a month; an ephemeral one about 25 hours (50 minutes x 30 days), so idle time disappears.

?? Which work is suitable for Spot VMs and which is not?
Suitable: retryable, stateless or checkpointed work such as Spark secondary workers, batch Dataflow with FlexRS and Batch jobs. Not suitable: Spark drivers, components holding state that cannot be recomputed cheaply, or work with strict deadlines and no slack.

?? Why can moving data to Coldline increase costs?
Coldline has a 90-day minimum storage duration (early deletion is charged) and higher retrieval fees. If the data is read often or deleted early, retrieval and early-deletion charges exceed the storage saving.
:::

::: resources
- Docs: [BigQuery: estimate and control costs](https://docs.cloud.google.com/bigquery/docs/best-practices-costs) · query, storage and quota practices for BigQuery
- Docs: [Well-Architected Framework: cost optimization](https://docs.cloud.google.com/architecture/framework/cost-optimization) · Google's principles and recommendations for cost
- Docs: [Dataflow FlexRS](https://docs.cloud.google.com/dataflow/docs/guides/flexrs) · cheaper batch execution with delayed scheduling
- Docs: [Cloud Storage classes](https://docs.cloud.google.com/storage/docs/storage-classes) · minimum durations and retrieval costs per class
- Blog: [Reducing Apache Spark infrastructure costs by 73%](https://medium.com/airbnb-engineering/reducing-apache-spark-infrastructure-costs-by-73-b0c90e284f94) · Airbnb's case study on Spark cost reduction
- Article: [FinOps Framework](https://www.finops.org/framework/) · the FinOps Foundation's practice model for cloud cost
:::

### Capacity Planning & Benchmarking

::: plain
Before a wedding, the caterer estimates guests, courses and how fast the kitchen can plate food, then runs a tasting to check. Capacity planning is the estimate; benchmarking is the tasting. Without the estimate you buy the wrong ovens; without the tasting you discover on the day that the fryer is the bottleneck.
:::

**What capacity planning answers.** Will the platform meet its latency and freshness targets at next year's volume, and what will it cost? The method has four steps: **model the workload**, **estimate resources**, **measure with benchmarks and load tests**, **find the bottleneck and iterate**.

**Workload profiles.** Different workloads stress different resources, so size them separately:

| Profile | Shape | Stresses | Sizing unit on Google Cloud |
|---|---|---|---|
| Batch ETL | Large scans and joins, deadline-driven | Throughput, shuffle | Slot-hours per run; Spark executor-hours; Dataflow vCPU-hours |
| Interactive BI | Many short queries, concurrency peaks | Concurrency, latency | Peak concurrent slots; BI Engine memory |
| Streaming | Continuous, bursty | Sustained throughput, catch-up headroom | Messages/s and MB/s per worker; Pub/Sub throughput |
| Serving | Key lookups at high QPS | Tail latency | Bigtable nodes (QPS and storage per node), AlloyDB vCPUs |
| ML training | Periodic, heavy | Accelerators, I/O | GPU or TPU hours, read throughput |

**Estimating with simple laws.** **Little's law** says the average number of items in a system equals arrival rate times time in the system (L = λW). It sizes concurrency for BI:

```python
queries_per_hour = 3_000        # peak hour, from INFORMATION_SCHEMA.JOBS
avg_seconds = 4                 # average query duration
avg_slot_seconds = 400          # average slot-seconds per query (total_slot_ms / 1000)

arrival_per_s = queries_per_hour / 3_600
concurrent = arrival_per_s * avg_seconds                    # Little's law
avg_slots = queries_per_hour * avg_slot_seconds / 3_600
print(f"average concurrent queries: {concurrent:.1f}")      # average concurrent queries: 3.3
print(f"average slots in peak hour: {avg_slots:.0f}")       # average slots in peak hour: 333
print(f"with 3x burst headroom:     {avg_slots * 3:.0f}")   # with 3x burst headroom:     1000
```

A baseline near 300 slots with autoscaling to about 1,000 is a starting point, to be checked against measured slot usage. Averages hide bursts, so always look at the distribution (p95 concurrency per minute), not only the hourly mean.

**Benchmarks.** Standard benchmarks give comparable, repeatable workloads:

- **TPC-H**: 8 tables and 22 ad hoc decision-support queries; the scale factor (SF) is roughly the raw data size in GB (SF 1000 is about 1 TB).
- **TPC-DS**: a retail snowflake schema with 24 tables (7 fact tables, 17 dimensions) and 99 queries, more varied and closer to real BI and ETL than TPC-H.
- **ClickBench**: a single wide web-analytics table with 43 queries, widely used to compare analytical engines on one machine or cluster.
- **TPC-DS kits** (the official `dsdgen` and `dsqgen` tools, and forks such as Databricks' `tpcds-kit`) generate the data and queries to run TPC-DS on Spark, Trino or BigQuery.

Use them to compare engines and configurations, never as a forecast of your own workload. Official TPC results are audited; vendor "TPC-like" results are not, and are tuned. The most useful benchmark is your own: your top 50 queries by cost and frequency, replayed on representative data.

**Load testing pipelines.** For streaming, generate synthetic events at 1x, 2x and 5x expected peak (a Dataflow or Cloud Run generator publishing to a test topic with realistic key distribution, including hot keys), and measure end-to-end latency, backlog (Pub/Sub `oldest_unacked_message_age`) and worker scaling. Also test catch-up: stop the consumer for an hour, restart, measure how long it takes to recover. For batch, run the job on a full-size copy (or a scaled sample with the same skew) and record duration and cost per run.

**Bottleneck analysis.** Find the one resource that limits throughput. The **USE method** (for each resource check Utilization, Saturation and Errors) works for VMs and services. In Spark, the UI shows whether stages are limited by shuffle, skew or memory; in BigQuery, the query plan shows slot time per stage, shuffle bytes and skew between workers; in Dataflow, the job graph shows which step lags and its data freshness. Fix the bottleneck, then measure again: the next one will appear.

**Cost-capacity trade-off modeling.** For each option, compute cost and the latency or deadline met, then choose the cheapest that meets the requirement. A batch job that must finish by 06:00 does not need to finish by 02:00; halving its workers may double duration and keep cost similar, while sharing capacity better. For BigQuery, compare on-demand cost per month with a reservation sized from measured slot usage. Repeat the model for expected growth (for example 3x in 18 months) to see when the design changes, not only the bill.

> **Pitfall:** Load testing with uniform data. Real data has hot keys (one huge customer, one viral video). A test with evenly distributed keys passes, production fails on skew. Copy the real key distribution into synthetic data.

> **Tip:** Keep a capacity sheet per platform component with current peak, headroom, growth rate and the date it will run out. Review it quarterly.

::: deeper
**Queueing near saturation.** Latency does not rise linearly with load. For a simple queue, waiting time grows roughly as utilisation / (1 - utilisation): at 50% utilisation the wait factor is 1, at 90% it is 9, at 95% it is 19. Plan for 60 to 70% utilisation at peak on resources with latency targets.

**Benchmark hygiene.** Warm versus cold caches (BigQuery caches results, so disable cached results with `use_query_cache=false` when measuring), data layout (clustered or not), concurrency (single query versus many), and the price of the configuration. A fair report lists all of them.

**Bigtable sizing.** Size nodes for throughput (each node handles thousands to tens of thousands of simple reads per second depending on row size, check the current performance guidance) and for storage per node, then add headroom and enable autoscaling with a CPU target. Load test with the real row key distribution.

> **Interview:** When asked about scale, give numbers in order: events per second (average and peak), bytes per event, daily and yearly volume, query concurrency, latency target, then the resource estimate and the bottleneck you expect first. Interviewers mostly check that you reason with numbers, not that the numbers are exact.
:::

::: check
?? A dashboard workload has 1,800 queries in its peak hour, each taking 2 seconds. How many queries run concurrently on average?
Arrival rate is 0.5 per second; by Little's law, 0.5 x 2 = 1 concurrent query on average. Peaks within the hour will be higher, so check per-minute concurrency.

?? Why is TPC-DS usually more representative than TPC-H?
It has a richer snowflake schema (24 tables) and 99 more varied queries, including complex joins, subqueries and reporting patterns, closer to real BI and ETL workloads.

?? Why plan for 60 to 70% utilisation rather than 95%?
Queueing delay grows sharply near saturation: at 95% utilisation, waiting time is many times longer than at 70%, and any burst causes large latency spikes.
:::

::: resources
- Spec: [TPC-H](https://www.tpc.org/tpch/) · the decision-support benchmark specification and results
- Spec: [TPC-DS](https://www.tpc.org/tpcds/) · the retail decision-support benchmark with 99 queries
- Repo: [tpcds-kit](https://github.com/databricks/tpcds-kit) · Databricks' fork of the TPC-DS data and query generator, used for Spark benchmarks
- Article: [ClickBench](https://benchmark.clickhouse.com/) · an open benchmark comparing analytical databases
- Article: [The USE method](https://www.brendangregg.com/usemethod.html) · Brendan Gregg's checklist for finding bottlenecks
- Docs: [BigQuery: understand slots](https://docs.cloud.google.com/bigquery/docs/slots) · how slots relate to capacity and concurrency
:::

### Disaster Recovery & Business Continuity

::: plain
Disaster recovery is the fire plan for your data. You decide in advance how much work you can afford to lose (the last hour? the last day?) and how quickly you must be open again, then you keep copies and practise the evacuation. A plan that has never been rehearsed is only a hope.
:::

**Two numbers define every DR design.**

- **RPO (recovery point objective)**: the maximum acceptable data loss, measured in time. RPO 15 minutes means after a disaster you may lose at most the last 15 minutes of writes.
- **RTO (recovery time objective)**: the maximum acceptable time until the service works again.

They are business decisions, set per data product. Finance month-end reports may tolerate RPO 24 hours and RTO 1 day; a fraud-scoring feature store may need RPO near zero and RTO minutes. Lower numbers cost more, so do not give everything the strictest tier.

**What disasters you are designing for.** Zone failure (handled by regional services automatically), region outage (rare, needs cross-region design), and, far more common, **logical disasters**: a bad deployment, a `DELETE` without `WHERE`, a corrupted backfill, ransomware or a compromised account. Replication copies logical disasters instantly, so DR needs **point-in-time recovery** as well as replicas.

**Strategies.**

| Strategy | How | RPO / RTO | Cost |
|---|---|---|---|
| Backup and restore | Snapshots or exports to another region; rebuild on demand | Hours / hours to days | Lowest |
| Pilot light | Data replicated continuously; compute defined in Terraform but not running | Minutes / about an hour | Low |
| Warm standby (active-passive) | Replicated data plus scaled-down running pipelines in the second region | Minutes / minutes | Medium |
| Active-active | Both regions serve traffic; data replicated synchronously or with conflict handling | Near zero / near zero | Highest, and complex |

Most analytics platforms choose pilot light or warm standby; active-active is reserved for serving systems (Spanner, Bigtable with multi-cluster routing) that need it.

**Geo-replication on Google Cloud, by service.**

| Component | Zone failure | Region failure | Logical errors |
|---|---|---|---|
| Cloud Storage | Regional buckets span zones | Dual-region or multi-region buckets; turbo replication for a 15-minute RPO target | Object versioning, soft delete, retention locks |
| BigQuery | Data is replicated across zones in the location | Cross-region dataset replication; managed disaster recovery (Enterprise Plus) with failover | Time travel (up to 7 days), fail-safe (7 more days, via support), table snapshots |
| Spanner | Regional configuration | Multi-region configurations (99.999% availability SLA) | Point-in-time recovery, backups |
| Bigtable | A cluster lives in one zone: replicate to a second cluster in another zone | Replication to clusters in other regions, multi-cluster routing | Backups |
| AlloyDB / Cloud SQL | High-availability configuration | Cross-region secondary cluster or replica, promoted on failover | Point-in-time recovery, backups |
| Pub/Sub | Messages stored in multiple zones | Global service; publish and subscribe endpoints in surviving regions | Seek to a timestamp or snapshot for replay |
| Pipelines, Airflow | Redeploy | Terraform plus CI in the second region; Airflow environment snapshots | Git history |

**Backup and restore for lakehouses.** BigQuery: schedule table snapshots of critical gold tables (cheap, they store only changed data) and replicate critical datasets. Iceberg on Cloud Storage has a trap: **metadata files contain absolute paths** (`gs://lake-prod/silver/orders/data/...`). Copying the bucket to a differently named bucket in another region gives metadata pointing at the old bucket. Options: use a dual-region bucket (same name, same paths, replicated by Google), or rewrite paths on restore (Iceberg's `rewrite_table_path` procedure in recent versions), or rebuild tables from replicated bronze. Snapshots inside Iceberg protect against logical errors only while they are retained.

**Sketch of a BigQuery cross-region replica and promotion:**

```sql
-- Add a secondary replica of a dataset in another region
ALTER SCHEMA gold ADD REPLICA `europe-west4` OPTIONS (location = 'europe-west4');

-- During a regional outage (or a drill), make the secondary the primary
ALTER SCHEMA gold SET OPTIONS (primary_replica = 'europe-west4');

-- Protect against logical errors: daily snapshot of a critical table
CREATE SNAPSHOT TABLE gold_snapshots.revenue_20261123
CLONE gold.revenue
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 35 DAY));
```

Replication is asynchronous, so the RPO is the replication lag, and both regions must be compatible with your residency rules.

**DR runbooks and testing.** A runbook lists, in order: who declares a disaster, how to check the scope, the failover steps per component (data first, then pipelines, then consumers' connection strings), how to verify (row counts, freshness, smoke queries), how to communicate, and how to fail back. Test it: a tabletop exercise every quarter, a real failover drill of at least one product every year, and a restore test of backups every month. A backup that has never been restored is not a backup.

> **Pitfall:** Treating replication as backup. A replicated `TRUNCATE` arrives in the secondary within minutes. Keep point-in-time recovery (time travel, snapshots, versioning, PITR on databases) that reaches back further than the time it takes to notice a mistake.

> **Tip:** Write the DR tier (RPO, RTO, strategy) into each data product's spec, so the platform can configure replication and snapshots automatically and auditors can see it.

::: deeper
**Dependencies decide RTO.** A gold table replicated to another region is useless if the pipelines that refresh it, the secrets they need, the service accounts, the Airflow environment and the BI connections exist only in the failed region. List every dependency, including identity (IAM is global), DNS and Terraform state (keep the state bucket dual-region).

**Recovering streaming pipelines.** After a failover, a Dataflow streaming job restarts in the new region from its source position. With Pub/Sub, subscriptions keep unacknowledged messages, and `seek` to a snapshot or timestamp allows replay; downstream writes must be idempotent so replayed messages do not duplicate data.

**Ransomware and account compromise.** Bucket retention policies with locks, object versioning and separate backup projects with different administrators protect against an attacker deleting both data and backups. Keep at least one copy that production credentials cannot delete.

> **Interview:** "Design DR for the analytics platform with RPO 1 hour and RTO 4 hours." Classify stateful components; dual-region buckets for raw data; BigQuery cross-region replicas for curated datasets (or rebuild from bronze if within RTO); daily snapshots for logical errors; Terraform for all compute; pipelines resumed from Pub/Sub or CDC positions; a runbook and a twice-yearly drill with measured actual RPO and RTO.
:::

::: check
?? What is the difference between RPO and RTO?
RPO is how much data you can afford to lose, measured as time before the failure. RTO is how long you can afford to be down before service is restored.

?? Why do cross-region replicas not protect against an accidental `DELETE`?
Replication copies the delete to the replica. Protection against logical errors needs point-in-time recovery: time travel, snapshots, object versioning or database PITR.

?? Why is copying an Iceberg table's bucket to a new bucket not a complete restore?
Iceberg metadata files store absolute file paths pointing to the original bucket. The copy needs a path rewrite, the same bucket name (a dual-region bucket), or a rebuild of the table metadata.
:::

::: resources
- Docs: [Disaster recovery planning guide](https://docs.cloud.google.com/architecture/dr-scenarios-planning-guide) · RPO, RTO and DR patterns on Google Cloud
- Docs: [BigQuery managed disaster recovery](https://docs.cloud.google.com/bigquery/docs/managed-disaster-recovery) · failover reservations and cross-region replication
- Docs: [Cloud Storage bucket locations](https://docs.cloud.google.com/storage/docs/locations) · regional, dual-region and multi-region buckets and turbo replication
- Docs: [BigQuery table snapshots](https://docs.cloud.google.com/bigquery/docs/table-snapshots-intro) · cheap point-in-time copies of tables
- Book: [Site Reliability Engineering](https://sre.google/sre-book/table-of-contents/) · data integrity and disaster testing chapters
- Book: [Designing Distributed Systems](https://www.oreilly.com/library/view/designing-distributed-systems/9781491983638/) · Brendan Burns on replication and serving patterns
:::

### Designing for GDPR & Privacy by Design

::: plain
Privacy by design is like building a house with the plumbing planned from the start instead of drilling through finished walls later. If you know from day one which rooms need water, the pipes go in neatly. If personal data is scattered through every table before anyone thinks about privacy, deleting one person's data later means drilling through every wall in the house.
:::

The laws themselves (GDPR, CCPA, HIPAA), the rights they give, masking techniques and consent are covered in [Data governance, security and compliance](#governance). This topic is the architecture: where in the platform each privacy requirement is met, so that compliance is a property of the design rather than a yearly project.

**Privacy by design** (a principle written into GDPR Article 25 as "data protection by design and by default") means: collect only what is needed, protect it from the moment it arrives, make the private option the default, and make rights such as erasure cheap to fulfil. For a data platform this becomes five design decisions.

**1. Detect and classify PII at ingestion.** Every new source and column is classified before it reaches silver. Sensitive Data Protection **discovery** profiles BigQuery tables and Cloud Storage buckets continuously and reports infoTypes (email, phone, national IDs) and risk per column; its findings can feed Knowledge Catalog and policy tags. For streams, a Dataflow step can call the de-identification API inline so raw identifiers never land. The contract of each source declares which fields are personal, and CI fails a new field without a classification.

**2. Pseudonymise early, keep identities in one place.** Replace direct identifiers with a keyed pseudonym in the landing-to-silver step, and keep the mapping and the personal attributes in a small set of **PII vault tables** (identity, contact, address) with tight access. Facts and events carry only the pseudonym. A keyed hash (HMAC) with the key in Secret Manager or Cloud KMS is deterministic, so joins still work, but it cannot be reversed or brute-forced without the key:

```python
import hmac, hashlib

KEY = b"demo-key-from-secret-manager"     # in production: fetched from Secret Manager, rotated

def pseudonym(email: str) -> str:
    normalised = email.strip().lower()
    return hmac.new(KEY, normalised.encode(), hashlib.sha256).hexdigest()[:16]

print(pseudonym("Ana@Example.com"))     # 3447967bb95c2ba2
print(pseudonym("ana@example.com "))    # 3447967bb95c2ba2  (same person, same pseudonym)
print(pseudonym("bob@example.com"))     # bb0cda3b79d739d9
```

Normalise before hashing, or the same person gets two pseudonyms. Pseudonymised data is still personal data under GDPR (it can be re-linked with the key), but the blast radius of a leak and the scope of erasure both shrink.

**3. Design erasure for immutable storage.** Lakehouse files are immutable, and BigQuery keeps deleted data in time travel and fail-safe. Erasure therefore has several stages, and the design must cover each:

| Store | Delete step | When data is really gone |
|---|---|---|
| BigQuery table | `DELETE ... WHERE user_key IN (...)` | After the time travel window (2 to 7 days, configurable per dataset) plus 7 days of fail-safe |
| Iceberg table, copy-on-write | `DELETE` rewrites affected data files | After `expire_snapshots` removes old snapshots and their files |
| Iceberg table, merge-on-read | `DELETE` writes delete files (position or equality deletes; deletion vectors in format v3) | After compaction rewrites data files without the rows, and old snapshots expire |
| Pub/Sub, Kafka | Cannot delete one message | When retention expires; or use compacted topics with tombstones, or crypto-shredding |
| Backups, snapshots, exports | Usually not editable | When they expire; or re-apply the erasure log after any restore |

A batch erasure job on Iceberg with Spark SQL:

```sql
-- Weekly erasure batch, within the one-month deadline
DELETE FROM lake.silver.events
WHERE user_key IN (SELECT user_key FROM lake.gov.erasure_requests WHERE batch_id = '2026-W47');

CALL lake.system.rewrite_data_files(table => 'silver.events',
  where => 'event_date >= DATE ''2024-01-01''');             -- physically drop rows held in delete files
CALL lake.system.expire_snapshots(table => 'silver.events',
  older_than => TIMESTAMP '2026-11-16 00:00:00', retain_last => 5);
```

The design levers that make this cheap: personal data concentrated in vault tables (erasure touches a handful of tables, not hundreds), large event tables clustered or partitioned by the pseudonymous key, requests batched weekly, snapshot retention shorter than the legal deadline, and **crypto-shredding** for data that cannot be rewritten: encrypt each person's fields with a per-person key (BigQuery AEAD functions with keysets), and delete the key to make every copy unreadable at once.

**4. Data residency by construction.** Choose dataset and bucket locations explicitly, enforce them with the `gcp.resourceLocations` organization policy, and remember derived data: query results, temporary tables, logs, ML training copies and backups all have locations. A global company often runs one platform per legal region (EU data in EU locations, US data in US locations) and shares only anonymised aggregates across them. The next topic covers multi-region layouts.

**5. Privacy-preserving analytics.** Many uses need patterns, not people. BigQuery offers **aggregation thresholds** (results only for groups with at least N distinct entities) and **differential privacy** (calibrated noise with a privacy budget), and data clean rooms let partners join data without seeing each other's rows:

```sql
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (epsilon = 1.0, delta = 1e-6, max_groups_contributed = 1, privacy_unit_column = user_key)
  country, COUNT(*) AS users
FROM silver.sessions
GROUP BY country;
```

Use these for wide internal sharing, partner reports and published statistics, so that consumers never need row-level personal data.

> **Pitfall:** Forgetting the copies. Erasure that deletes from the gold customer table but not from bronze files, Pub/Sub replays, a data scientist's sandbox extract or a three-month backup is not erasure. Inventory every store that can hold personal data (the catalog's classification and lineage help) and give each an erasure mechanism.

> **Tip:** Treat the erasure request table as a product: every request, its batch, the tables processed, row counts deleted and the completion time. It is your evidence for auditors and the log to re-apply after a restore.

::: deeper
**Purpose limitation in architecture.** GDPR ties processing to purposes. Practical designs tag datasets with allowed purposes (an aspect in Knowledge Catalog or a label), and access requests state a purpose; marketing cannot use support-ticket text collected for service. Consent status becomes a dimension joined into pipelines so that opted-out users are excluded from uses that need consent.

**Minimisation and retention.** The cheapest personal data to protect is data you never stored. Drop fields with no use at ingestion (declared in the contract), truncate IP addresses and precise locations early, and expire raw layers sooner than curated ones.

**Re-identification risk.** Pseudonyms plus quasi-identifiers (postcode, birth date, gender) can identify people. Aggregation thresholds and generalisation reduce the risk for shared outputs; k-anonymity checks on extracts catch obvious problems.

**Logs and observability data.** Pipeline logs frequently contain payloads with personal data. Log identifiers, not payloads, and set log retention and exclusion filters in Cloud Logging.

> **Interview:** "A user asks to be forgotten. Your lakehouse has 300 tables in Iceberg and BigQuery." Answer with: lineage-driven inventory of tables holding the user's pseudonym, vault deletion, batched deletes on clustered event tables, compaction and snapshot expiry, Pub/Sub retention, backups handled by expiry or crypto-shredding, and an audit record of the request.
:::

::: check
?? After a BigQuery `DELETE`, how long can the deleted rows still exist?
Through the dataset's time travel window (2 to 7 days) and then 7 days of fail-safe, so up to about two weeks. Plan the erasure schedule so this fits within the legal deadline.

?? Why must an Iceberg merge-on-read table be compacted after an erasure `DELETE`?
The `DELETE` only writes delete files; the rows remain inside the original data files. Compaction rewrites data files without the deleted rows, and snapshot expiry removes the old files.

?? What does keyed hashing (HMAC) give you over a plain SHA-256 hash for pseudonyms?
Without the secret key nobody can recompute pseudonyms, so emails or phone numbers cannot be recovered by hashing candidate values (a dictionary attack). Joins still work because the same input and key always give the same pseudonym.
:::

::: resources
- Docs: [Sensitive Data Protection overview](https://docs.cloud.google.com/sensitive-data-protection/docs/sensitive-data-protection-overview) · discovery, inspection and de-identification on Google Cloud
- Docs: [BigQuery differential privacy](https://docs.cloud.google.com/bigquery/docs/differential-privacy) · privacy-preserving aggregates in GoogleSQL
- Spec: [Apache Iceberg table spec: row-level deletes](https://iceberg.apache.org/spec/) · position deletes, equality deletes and deletion vectors
- Book: [Practical Data Privacy](https://www.oreilly.com/library/view/practical-data-privacy/9781098129453/) · Katharine Jarmul on privacy engineering techniques
- Blog: [Implementing the GDPR right to be forgotten in Delta Lake](https://www.databricks.com/blog/2022/03/23/implementing-the-gdpr-right-to-be-forgotten-in-delta-lake.html) · erasure on an immutable lakehouse format
- Docs: [GDPR Article 25: data protection by design and by default](https://gdpr-info.eu/art-25-gdpr/) · the legal text behind privacy by design
:::

### Hybrid and multi-region data architecture on Google Cloud

::: plain
Data is heavy, like water. Moving a few buckets is easy; moving a lake is slow and expensive. So you build the town where the water is, and pipe only what you need elsewhere. Hybrid and multi-region design is deciding where each "lake" of data lives (in your own data centre, in one Google Cloud region, in several), and what small, cheap streams flow between them.
:::

**Why designs span locations.** Four forces: **residency** (laws or contracts require some data to stay in a country or region), **latency** (users or applications far from the data), **resilience** (survive a regional outage) and **legacy** (data still in an on-premises data centre or with a partner). Each force pulls in a different direction, so name which ones apply before designing.

**Data gravity.** Large datasets attract the compute and applications that use them, because moving data costs money (egress), time and governance effort. The design principle follows: **move compute to data, and move small results rather than large inputs.** A worked example of the cost of getting it wrong:

```python
rate = 0.05                    # USD per GiB, roughly North America to Europe
raw_gib_per_day = 500          # replicate all new raw events to the other continent
agg_gib_per_day = 5            # or replicate only daily aggregates
print(f"replicate raw:        ${raw_gib_per_day * rate * 30:,.0f}/month")    # replicate raw:        $750/month
print(f"replicate aggregates: ${agg_gib_per_day * rate * 30:,.2f}/month")   # replicate aggregates: $7.50/month
```

The bigger cost is usually not the bill but the second copy to secure, govern and keep consistent. Egress pricing details are in [Google Cloud foundations for data](#gcpfound).

**Single-region, multi-region and multi-location layouts.**

| Layout | Description | Fits |
|---|---|---|
| One region | All data services in one region (for example `europe-west1`) | Most companies; simplest, cheapest, low latency between services |
| Multi-region location | BigQuery `US` or `EU` multi-region, Cloud Storage multi-region or dual-region | Higher availability within a continent, still one logical location |
| Regional hubs | One platform per legal area (EU, US, APAC), each with its own datasets; only anonymised aggregates are shared to a global hub | Global companies with residency requirements |
| Primary plus DR region | Data replicated to a second region for recovery; compute starts there only on failover | Strict RTO and RPO for critical products |

BigQuery cannot join tables in different locations in one query. A regional hub design therefore computes per-region results locally and publishes small aggregates (or replicates selected datasets) to wherever they are combined.

**Federation patterns.** Sometimes the right move is to query data where it lives instead of copying it:

- **Federated queries** from BigQuery to Cloud SQL, AlloyDB and Spanner with `EXTERNAL_QUERY` (useful for small reference data or low-volume lookups, not for scanning large operational tables):

```sql
SELECT o.order_id, o.status, c.segment
FROM EXTERNAL_QUERY(
  'projects/shop-prod/locations/europe-west1/connections/alloydb-orders',
  'SELECT order_id, status, customer_id FROM orders WHERE updated_at > now() - interval ''1 hour''') AS o
JOIN gold.customers AS c USING (customer_id);
```

- **External and lakehouse tables** over Cloud Storage files (Parquet, Iceberg) read by BigQuery without loading.
- **Sharing instead of copying**: BigQuery sharing listings let another project or organisation query your data in place, in the same location.

Federation trades freshness and simplicity for performance and load on the source. Use it for small or rarely queried data; replicate when queries are frequent or large.

**Hybrid: on-premises plus Google Cloud.** During migrations and in regulated industries, part of the data stays on-premises. The building blocks: **Cloud Interconnect** (Dedicated or Partner) or HA VPN for private connectivity with predictable bandwidth; **Datastream** over private connectivity for CDC from on-premises Oracle, MySQL, PostgreSQL or SQL Server; **Storage Transfer Service** agents to copy file systems and HDFS to Cloud Storage; Pub/Sub or Managed Kafka (with mirroring from on-premises Kafka using MirrorMaker 2 or Kafka Connect) for events. Plan bandwidth: 50 TB over a 10 Gbps link at 70% efficiency takes about 16 hours; over 1 Gbps about 6.6 days.

**Cloud-agnostic design with open formats.** Portability comes from open data formats and open engines, not from abstracting every service. Store long-lived data in Parquet and Iceberg (or Avro for events), keep transformation logic in SQL and Beam or Spark, define infrastructure in Terraform, and keep a catalog that supports the Iceberg REST protocol. Then any future engine can read the data without an export. Do not wrap BigQuery in a generic layer that hides its strengths; the portable part is the data and the logic, not the API calls.

**Meshes across locations.** In a mesh spanning regions or on-premises, each domain product declares its location, and the global policy names which products may be replicated where. Cross-location consumers use aggregated products or replicas, never direct cross-region queries.

> **Pitfall:** Datasets created in the default location by accident. A team creates `staging` in `US` while everything else is in `EU`; joins fail, someone copies data across, and personal data has now left the EU. Enforce locations with organization policy and Terraform defaults.

> **Tip:** Draw a location map: every bucket, dataset, database, topic and pipeline with its location, plus every flow between locations with its volume per day. It reveals egress, residency and DR gaps in one picture.

::: deeper
**Pub/Sub and global services.** Pub/Sub is global, but messages are stored in regions; a **message storage policy** on a topic restricts which regions may store messages, which matters for residency. Similarly, check where each managed service stores data and metadata (logs, query results, model artifacts).

**Latency between regions.** Round trips between regions on one continent are typically tens of milliseconds, across oceans often 100 ms or more. Synchronous dependencies across regions (a pipeline calling a database in another region per record) multiply that latency; batch them or co-locate.

**Spanner as the multi-region exception.** When an operational system truly needs strong consistency across regions, Spanner multi-region configurations provide it with synchronous replication, at a latency and cost premium. Analytics rarely needs this; it is a serving-layer choice.

> **Interview:** "Your company operates in the EU and the US, and EU customer data must stay in the EU. Design the analytics platform." Regional hubs with identical Terraform-defined stacks; location policies per folder; EU and US marts computed locally; a global hub that receives only aggregated, anonymised metrics; Pub/Sub message storage policies; Knowledge Catalog across both with location aspects; DR within each legal area.
:::

::: check
?? Why does "move compute to data" usually beat replicating raw data to another region?
Raw data is large, so copying it costs egress and creates a second copy to secure, govern and keep consistent. Computing locally and moving small results is cheaper and simpler.

?? When is a BigQuery federated query to AlloyDB a good idea?
For small reference data or low-volume, fresh lookups, where copying would add delay and the query does not scan large tables. For large or frequent analytical queries, replicate with Datastream instead.

?? Where does cloud portability really come from in a data platform?
From open storage formats (Parquet, Iceberg, Avro), logic in standard SQL and open engines (Beam, Spark), infrastructure as code and an open catalog protocol, not from wrapping every managed service in an abstraction.
:::

::: resources
- Docs: [BigQuery dataset locations](https://docs.cloud.google.com/bigquery/docs/locations) · regions, multi-regions and location rules for queries
- Docs: [Cloud SQL and AlloyDB federated queries](https://docs.cloud.google.com/bigquery/docs/federated-queries-intro) · `EXTERNAL_QUERY` and its limits
- Docs: [Hybrid and multicloud architecture patterns](https://docs.cloud.google.com/architecture/hybrid-multicloud-patterns) · Google's patterns for distributing workloads
- Docs: [Restricting resource locations](https://docs.cloud.google.com/resource-manager/docs/organization-policy/defining-locations) · the `gcp.resourceLocations` organization policy
- Article: [How we got to the open lakehouse](https://www.dremio.com/blog/how-we-got-to-open-lakehouse/) · the case for open formats as the portable layer
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · replication, partitioning and the cost of moving data
:::

### Data Platform Migration Strategies

::: plain
Migrating a data platform is like moving a busy restaurant to a new building without closing for a day. You set up the new kitchen, cook the same dishes in both kitchens for a while and compare them, move the regulars over table by table, and keep the old kitchen ready in case the new oven fails. Only when everyone is happily eating in the new place do you hand back the keys to the old one.
:::

**Three depths of migration.**

| Approach | What changes | Example | Speed | Benefit |
|---|---|---|---|---|
| Lift and shift (rehost) | Infrastructure only | Hadoop cluster rebuilt on Compute Engine VMs; same HDFS, same jobs | Fast | Leaves the data centre; little else |
| Re-platform | Managed services, same logic | HDFS to Cloud Storage, Hive and Spark jobs on Managed Service for Apache Spark, Teradata SQL translated to BigQuery | Medium | Elastic compute, less operations, most code kept |
| Re-architect | Design and logic | Batch ETL rewritten as ELT in BigQuery with Dataform, CDC with Datastream, streaming with Dataflow | Slow | Full benefit of the target platform |

Most successful migrations re-platform first (move data and workloads with minimal changes, so the old system can be switched off), then re-architect domain by domain once on the new platform. Re-architecting everything during the migration doubles the risk: when numbers differ, you cannot tell whether the migration or the redesign caused it.

**The migration phases.** 1. **Assess**: inventory tables, jobs, queries, users, dependencies and volumes; BigQuery Migration Service's assessment analyses logs and metadata from warehouses such as Teradata, Oracle and Snowflake. 2. **Plan**: group workloads into waves by dependency and business priority, starting with a low-risk, high-visibility domain. 3. **Move data**: initial bulk copy, then incremental sync. 4. **Move logic**: SQL translation (BigQuery's batch and interactive SQL translators convert many dialects to GoogleSQL), job conversion, orchestration. 5. **Validate**: parallel runs with reconciliation. 6. **Cut over** consumers. 7. **Decommission** the old system.

**Dual-write versus dual-run.** Two ways to keep old and new in step during transition:

- **Dual-write**: sources write to both systems. Simple to describe, hard to keep correct: partial failures leave the systems different, and every producer must change. Use it only with an outbox or a log in between, which is really a CDC design.
- **Dual-run (parallel run)**: sources write to the old system as before; data is replicated to the new one (CDC or bulk sync), and both run the same pipelines. Outputs are compared daily until they match. This is the safer default.

**Reconciliation** is what makes cutover a decision based on evidence. Compare per partition: row counts, sums of key measures, distinct keys, and a hash of sorted keys, with tolerances agreed with the business. The open-source **Data Validation Tool** (DVT) from Google automates this between many databases and BigQuery. The idea in miniature:

```python
# Per-day (rows, revenue in cents) from the old warehouse and from BigQuery
old = {"2026-11-01": (1000, 5234050), "2026-11-02": (1020, 5311000), "2026-11-03": (998, 5100225)}
new = {"2026-11-01": (1000, 5234050), "2026-11-02": (1019, 5308500), "2026-11-03": (998, 5100225)}

mismatches = [(d, old[d], new.get(d)) for d in sorted(old) if old[d] != new.get(d)]
for day, o, n in mismatches:
    print(day, "old", o, "new", n)      # 2026-11-02 old (1020, 5311000) new (1019, 5308500)
print(f"{len(old) - len(mismatches)}/{len(old)} days match")   # 2/3 days match
```

Each mismatch is investigated to a root cause (a late-arriving row, a time zone difference, a rounding rule, a translation bug) before cutover. Time zones, `NULL` handling in aggregates, string collation and integer division are the classic culprits when migrating SQL between engines.

**Cutover strategies.**

| Strategy | How | Risk |
|---|---|---|
| Big bang | All consumers switch on one date | High: everything must be ready; hard to roll back |
| Phased by domain or wave | One domain's pipelines and consumers move at a time | Medium; most common |
| Strangler fig | New platform takes over functions one by one behind a stable interface (views, a semantic layer, an API) until the old system serves nothing | Low; needs the interface layer |
| Consumer-by-consumer | Repoint each dashboard and extract after validation | Low, slow |

Keep a rollback path for each wave: the old system stays live and fed until the new one has run cleanly through at least one full business cycle (often a month-end close).

**Hadoop to Google Cloud.** HDFS data moves to Cloud Storage (Storage Transfer Service with agents, or DistCp with the Cloud Storage connector); Hive tables become Iceberg tables in the Lakehouse runtime catalog or native BigQuery tables; Spark and Hive jobs run on Managed Service for Apache Spark (serverless batches for most, ephemeral clusters where custom configuration is needed); Oozie workflows become Managed Airflow DAGs; HBase moves to Bigtable (HBase-compatible API). The big change is storage-compute separation: instead of one long-lived cluster, each job gets its own compute, and data locality stops mattering. See [The Hadoop ecosystem](#hadoop) for what is being migrated.

**On-premises warehouse to BigQuery.** Schema and data with BigQuery Data Transfer Service (which supports Teradata among others) or exports to Cloud Storage and load jobs; SQL with the translation service; stored procedures rewritten as Dataform or BigQuery procedural SQL; ETL tool jobs re-platformed or rebuilt as ELT; security mapped to IAM, row access policies and policy tags; BI tools repointed. Performance tuning changes: indexes and statistics give way to partitioning and clustering.

> **Pitfall:** Migrating everything, including the dead. Large estates often have a third or more of tables and jobs that nobody uses. Use query logs to find what is actually read in the last 90 days, and leave the rest behind (archived, not migrated).

> **Tip:** Agree on the definition of "done" for each wave before starting: reconciliation passing for N consecutive days, performance targets met, consumers signed off, old jobs disabled. Without it, migrations stall at 90% for a year, paying for two platforms.

::: deeper
**Freeze windows and change control.** During parallel run, changes to logic must be applied to both systems or frozen; otherwise reconciliation diverges for reasons unrelated to the migration. Many teams freeze non-urgent changes in a domain while its wave is in parallel run.

**Performance surprises.** Queries tuned for one engine can be slow on another (correlated subqueries, many small `INSERT`s, row-by-row procedures). The translation service converts syntax, not design. Budget time to rewrite the top 20 heaviest jobs idiomatically.

**Cost during migration.** For months you pay for both platforms plus transfer. Plan the overlap and the decommissioning dates as part of the business case; every month of delay costs the old platform's run rate.

**Data migration at scale.** For hundreds of terabytes, network bandwidth sets the timeline (see the bandwidth estimate in the previous topic); Transfer Appliance (a physical device shipped to your data centre) is an option when links are too slow. Copy cold history in bulk early and sync only recent partitions incrementally.

> **Interview:** "Migrate a 400 TB Teradata warehouse with 3,000 reports to BigQuery." Assessment from query logs (what is used); waves by subject area; bulk transfer plus incremental sync; SQL translation and rewrite of heavy jobs; parallel run with automated reconciliation (DVT); strangler-style cutover through views or the semantic layer; rollback per wave; decommission dates; numbers: transfer time over the available link, overlap cost per month.
:::

::: check
?? Why is re-platforming first, then re-architecting, usually safer than re-architecting during migration?
When outputs differ, re-platforming keeps the logic the same, so differences point to the migration itself. Changing design and platform at once makes every mismatch ambiguous and the validation much harder.

?? Why is dual-run with reconciliation usually preferred over dual-write?
Dual-write requires every producer to write to both systems and leaves them inconsistent on partial failures. Dual-run keeps sources unchanged, replicates data and compares outputs, so correctness is measured rather than assumed.

?? What makes the strangler fig pattern low risk?
Consumers talk to a stable interface (views, semantic layer, API), and functions move behind it one at a time. Each move is small and reversible, and the old system shrinks until nothing uses it.
:::

::: resources
- Docs: [Introduction to BigQuery Migration Service](https://docs.cloud.google.com/bigquery/docs/migration-intro) · assessment, SQL translation, transfer and validation
- Docs: [Migrating on-premises Hadoop infrastructure to Google Cloud](https://docs.cloud.google.com/architecture/hadoop) · the Hadoop migration guide series
- Repo: [Data Validation Tool](https://github.com/GoogleCloudPlatform/professional-services-data-validator) · open-source reconciliation between source and target systems
- Article: [Strangler fig application](https://martinfowler.com/bliki/StranglerFigApplication.html) · Martin Fowler on incremental replacement of legacy systems
- Blog: [Migrating from Hadoop to Google Cloud](https://cloud.google.com/blog/products/data-analytics/migrating-from-hadoop-to-google-cloud) · Google's guidance on moving Hadoop workloads
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · evaluating and changing data architectures over time
:::

## Interviews

Data engineering interviews at most mid-size and large companies include a system design round: 45 to 60 minutes to design a data system from a one-line prompt. It tests exactly what this section has covered, under time pressure: turning vague requirements into numbers, choosing an architecture and defending it, and knowing how it fails and what it costs. This group gives one framework that works for any prompt, then applies it to the six prompts that come up most often.

### System Design Interview Patterns for DE

::: plain
A system design interview is like being asked to plan a wedding in an hour for a couple you have just met. A good planner does not start by booking a band. They ask how many guests, what budget, which date, what matters most to the couple, then sketch the day, then work out the tricky parts (the rain plan, the guest who needs a wheelchair ramp). Interviewers are watching for that order of thinking, more than for any particular answer.
:::

**The framework.** Seven steps, with a rough time budget for a 45-minute round. Say the steps out loud so the interviewer can follow and steer.

[[fig:design_interview]]

1. **Clarify requirements (5 minutes).** Functional: what questions must the data answer, for whom, through what interface (dashboard, API, file, model)? Non-functional: freshness, query latency, volume, retention, accuracy (approximate or exact, billing-grade?), availability, compliance. Write them on the board as a short list. Ask what is out of scope.
2. **Estimate (5 minutes).** Events per second (average and peak), bytes per event, daily and yearly storage, query concurrency. Round numbers are fine; show the arithmetic.
3. **Define the data model and interfaces (5 minutes).** The event schema, the main tables (grain of each), the serving query or API signature. This is where many candidates skip ahead and lose points.
4. **Sketch the high-level architecture (10 minutes).** Zones from ingestion to serving, one component per box, with the reason for each. Name the Google Cloud service or open-source engine.
5. **Deep dive (10 minutes).** The two or three hardest parts for this prompt: dedupe and exactly-once, late data, hot keys, point-in-time features, the serving store's key design. Interviewers often pick one; be ready for any.
6. **Failure modes and operations (5 minutes).** What happens when a component fails, how you replay, how you detect bad data, SLOs and alerts, backfills.
7. **Cost, trade-offs and evolution (5 minutes).** Monthly cost drivers, what you would simplify for a smaller company, what changes at 10x.

**Estimation in practice.** The ad-click aggregation prompt, sized:

```python
clicks_per_day = 1_000_000_000
bytes_per_click = 500
peak_factor = 5

avg_qps = clicks_per_day / 86_400
print(f"average: {avg_qps:,.0f}/s, peak: {avg_qps * peak_factor:,.0f}/s")  # average: 11,574/s, peak: 57,870/s
gb_day = clicks_per_day * bytes_per_click / 1e9
print(f"raw: {gb_day:.0f} GB/day, {gb_day * 365 / 1000:.0f} TB/year")       # raw: 500 GB/day, 182 TB/year
ads, minutes = 2_000_000, 1_440
print(f"max per-minute aggregate rows/day: {ads * minutes:,}")             # max per-minute aggregate rows/day: 2,880,000,000
```

58,000 events per second at peak is a moderate Pub/Sub and Dataflow load; 182 TB a year of raw data needs partitioning and retention rules; per-minute aggregates are bounded by active ads (most of the 2 million ads have no clicks in a given minute, so the real count is far lower).

**Worked example 1: ad-click aggregation.** *Requirements*: advertisers see clicks per ad per minute with under 1 minute delay; billing needs exact counts daily; detect click fraud bursts. *Design*: click servers publish to Pub/Sub (event ID, ad ID, user, timestamp, IP); Dataflow dedupes on event ID, filters known bots, windows by event time (1-minute tumbling, allowed lateness 10 minutes) and writes aggregates to Bigtable (row key `ad_id#minute`, for the advertiser dashboard API) and BigQuery (for analysis); raw events land in BigQuery through a separate subscription. *Deep dives*: exactly-once (Dataflow exactly-once plus idempotent writes keyed by window), hot ads (a viral ad on one key: combine per key with fan-out in Beam), late events (hot numbers updated within lateness; the daily billing job recomputes from raw events in BigQuery and is the number invoiced). *Failure*: Pub/Sub retains 7 days, so Dataflow can restart or replay; reconciliation of streaming totals against the batch recompute daily.

**Worked example 2: YouTube-style video analytics.** *Requirements*: creators see views, watch time and audience by video, near real-time for the last 48 hours and historical by day; billions of view events per day. *Design*: player heartbeat events to Pub/Sub; Dataflow sessionises views (session windows per viewer and video) and computes watch time; real-time per-video counters to Bigtable for the creator app; full events to BigQuery partitioned by date and clustered by `video_id`; daily rollups by video, country and device in gold; creator queries for history hit rollups, never raw events. *Deep dives*: view counting rules (a view counts after N seconds; dedupe replays and bots), distinct viewers with HyperLogLog sketches, a viral video as a hot key, multi-tenant query isolation for millions of creators.

**Worked example 3: ride-share data platform.** *Requirements*: live supply and demand by area for pricing; trip and payment analytics for finance; driver ETA model. *Design*: driver location pings (every few seconds per active driver) to Pub/Sub; Dataflow aggregates supply and demand per geospatial cell (S2 or H3 cells) per minute into Bigtable for the pricing service; trip and payment databases replicated with Datastream into BigQuery; finance marts in Dataform with month-end quality gates; ETA features in BigQuery (offline) and Bigtable online serving through Agent Platform Feature Store. *Deep dives*: location volume (500,000 active drivers at one ping every 4 seconds is 125,000 events per second; keep raw pings short-lived, keep aggregates), city-level partitioning, GDPR on trip histories, consistency between trip state in operational systems and analytics.

**Worked example 4: e-commerce recommendation pipeline.** *Requirements*: personalised product recommendations on the home page within 100 ms; models retrained daily; recent behaviour (last session) should matter. *Design*: clickstream to Pub/Sub and BigQuery; daily batch training on Agent Platform Pipelines over BigQuery features (point-in-time correct training sets); candidate generation precomputed nightly per user (top 500 candidates) into Bigtable; a streaming job maintains recent-session features (last 20 viewed items) in the online store; the ranking service fetches candidates and features, scores and returns. *Deep dives*: training-serving skew, cold start for new users and products, feedback loops (log what was shown, not only what was clicked), A/B testing with assignment logged into the event stream.

**Worked example 5: real-time leaderboard.** *Requirements*: a game with 10 million players shows global and friends' top 100 with updates within seconds. *Design*: score events to Pub/Sub; a consumer updates a sorted set in Memorystore (Redis or Valkey), `ZINCRBY leaderboard:2026-11 <points> <player>` and `ZREVRANGE leaderboard:2026-11 0 99 WITHSCORES` for the top 100, `ZREVRANK` for a player's rank; scores also land in BigQuery for history and anti-cheat analysis; monthly leaderboards as separate keys with expiry. *Deep dives*: one sorted set of 10 million members fits in memory (roughly a gigabyte), but very high write rates may need sharding by region with a merge of top-k lists; durability (rebuild Redis from BigQuery or the event log on loss); cheating detection offline.

**Worked example 6: event-driven audit system.** *Requirements*: every change to customer accounts must be recorded immutably for 7 years, searchable by auditors, tamper-evident. *Design*: services emit audit events (actor, action, resource, before and after, timestamp) to Pub/Sub, ideally through a transactional outbox so no change is committed without its event; a subscription writes to BigQuery (append-only table, partitioned by day, no update or delete permissions for anyone but a break-glass role) and a Cloud Storage subscription writes Avro files to a bucket with a locked retention policy of 7 years; each batch file's hash is chained to the previous one (or signed with Cloud KMS) for tamper evidence; Cloud Audit Logs cover administrative actions on the platform itself. *Deep dives*: completeness (reconcile event counts with service-side counters), ordering per resource, PII inside audit events versus erasure rights (audit records are often a legal-obligation exception, but minimise their content).

**What interviewers score.** Structure (requirements before design), quantitative reasoning, justified choices with trade-offs, depth in at least one hard part, awareness of failure and operations, and communication. They rarely care whether you choose Pinot or ClickHouse, as long as you say why.

> **Pitfall:** Designing for Google scale when the prompt says "a startup". Matching the design to the stated scale is part of the test. If the numbers say 100 events per second, a Pub/Sub BigQuery subscription and scheduled SQL beat a Dataflow and Bigtable design.

> **Tip:** Keep a one-page "numbers I know" list: a day is about 86,400 seconds (round to 100,000 for quick maths), a year about 31.5 million seconds, typical event sizes (0.2 to 2 KB), columnar compression (5 to 10x), and the rough limits of the services you name.

::: deeper
**Handling an interviewer's curveball.** Prompts change mid-way ("now the freshness requirement is 5 seconds", "now it must work in the EU only"). Restate the change, identify which components it affects, and adjust those, rather than redrawing everything. It shows the design is modular.

**Common deep-dive questions.** How do you guarantee no double counting? How do you handle an event that arrives two days late? What happens if the stream processor is down for an hour? How do you backfill a new metric for the last year? How do you know the numbers are right? How would you cut the cost in half? Prepare a crisp answer for each using the topics in this section and [Streaming systems](#streaming).

**Data modeling rounds.** Some companies separate data modeling (schemas, grain, SCDs, star versus wide tables) into its own round. Practise with [Data modeling](#modeling). In a design round, still state the grain of every table you draw.

**Practice method.** Time-box yourself to 45 minutes per prompt, draw on paper, then compare with this section's topics and note what you missed. The PySpark and Kafka guide's [Data system design](pyspark-streaming-kafka-topic-map.html#design) section has further Spark- and Kafka-centric prompts.

> **Interview:** A strong close in the final minutes: summarise the design in three sentences, list the two biggest risks and how you would monitor them, and say what you would build first (the smallest version that delivers value) versus later.
:::

::: check
?? In the ad-click design, why does the daily billing number come from a batch recomputation rather than the streaming aggregates?
The stream drops events later than its allowed lateness and may have rare duplicates or restarts. Billing needs exact, auditable numbers, so it is recomputed from the complete raw events in BigQuery after all late data has arrived, and the streaming totals are reconciled against it.

?? 500,000 active drivers each send a location every 4 seconds. What is the event rate, and what does it imply?
125,000 events per second. Raw pings are high volume and low long-term value, so aggregate them per cell and minute in the stream, serve aggregates from Bigtable, and keep raw pings only briefly.

?? What should you do in the first five minutes of a design interview?
Clarify functional and non-functional requirements (questions to answer, consumers, freshness, latency, volume, accuracy, retention, compliance) and write them down, then estimate volumes before choosing any technology.
:::

::: resources
- Book: [System Design Interview, Volume 2](https://bytebytego.com/) · Alex Xu's worked designs, including ad-click aggregation and real-time leaderboards
- Repo: [The System Design Primer](https://github.com/donnemartin/system-design-primer) · general system design concepts, estimates and practice prompts
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · the reference for the trade-offs interviewers probe
- Paper: [The Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) · event time, windows and triggers behind most streaming answers
- Docs: [Architecture Center: big data and analytics](https://docs.cloud.google.com/architecture/big-data-analytics) · reference designs to compare your answers with
- Docs: [Redis sorted sets](https://redis.io/docs/latest/develop/data-types/sorted-sets/) · the data structure behind leaderboards
:::

## Wrap-up

### Cheat sheet

| Concept or task | Rule of thumb, command or SQL |
|---|---|
| Start any design | Requirements, then numbers (events/s, bytes, TB/year, concurrency), then zones, then services |
| Zones | Sources, ingest, land (immutable), process, serve, consume; governance across all |
| Default processing choice | BigQuery SQL with Dataform or dbt; Dataflow for event-time streaming; Spark for existing Spark code |
| Medallion promises | Bronze complete and unchanged; silver typed, unique, valid; gold business-ready with an SLA |
| Late data in gold | Recompute a lookback window of partitions sized from measured lateness |
| Incremental watermark | Track ingestion time (`_ingested_at`), not event time |
| One technical catalog per table | Lakehouse runtime catalog for Iceberg; Knowledge Catalog as the business catalog |
| Data mesh fit | Many capable domains and a central bottleneck; otherwise a central platform with product practices |
| Real-time | Write the latency class per requirement; hot path for recent data, cold path as source of truth |
| Distinct counts in rollups | `HLL_COUNT.INIT` per rollup row, `HLL_COUNT.MERGE` across rows |
| Catch-up time | backlog / (capacity - live rate); plan headroom from the freshness SLA |
| Point-in-time join | Latest feature row with `feature_ts <= label_ts`; `ML.ENTITY_FEATURES_AT_TIME` in BigQuery |
| Product versioning | Semver: minor adds, major breaks; new major gets a new dataset or topic, old one deprecated with notice |
| Query guardrails | `require_partition_filter`, `--maximum_bytes_billed`, custom quotas, top queries from `INFORMATION_SCHEMA.JOBS` |
| Little's law | Concurrent = arrival rate x duration; plan 60 to 70% peak utilisation |
| DR | RPO and RTO per product; replicas for regions, snapshots and time travel for logical errors |
| Erasure | Delete, then compact (merge-on-read) and expire snapshots; batch requests; crypto-shred what cannot be rewritten |
| Multi-location | Move compute to data; share aggregates across regions; enforce `gcp.resourceLocations` |
| Migration | Re-platform first, dual-run with reconciliation, phased or strangler cutover, rollback per wave |

### Practice

::: try
**1. Size and sketch.** A fitness app has 2 million daily active users. Each sends on average 300 events a day (1 KB each), with a peak hour carrying 15% of the day's traffic. Product wants a dashboard of active users and workouts by hour (fresh within 15 minutes), and data science wants daily training data. Estimate peak events per second and yearly raw volume, then sketch the simplest design that meets the requirements.
:::

?? A solution to 1
Events per day: 2,000,000 x 300 = 600 million. Peak hour: 15% = 90 million in 3,600 s, about 25,000 events per second. Raw volume: 600 million x 1 KB = 600 GB a day, about 219 TB a year uncompressed (roughly 20 to 45 TB stored after compression). Design: apps publish to Pub/Sub; a BigQuery subscription writes bronze events (partitioned by date, clustered by user); Dataform runs every 15 minutes to build silver (deduped on event ID) and hourly gold aggregates, with distinct users as HLL sketches; Looker reads gold. Data science reads silver and daily feature tables. No Dataflow is needed: 15-minute freshness is a micro-batch requirement. Retention: bronze 180 days with partition expiration, silver 2 years.

::: try
**2. Fix the medallion.** A team's gold `daily_revenue` table is rebuilt each night from silver events filtered by `event_date = CURRENT_DATE() - 1`. Finance reports that revenue for past days changes in the source system for up to 5 days (refunds, late payments), but gold never reflects it. Silver is incrementally loaded with a watermark on `event_date`. What are the two bugs, and how do you fix them?
:::

?? A solution to 2
First, silver's watermark is on event time, so events that arrive late (with an old `event_date`) are never loaded. Track `_ingested_at` instead and merge on the business key. Second, gold only recomputes yesterday, so changes to older days never reach it. Recompute a lookback window of the last 6 days of partitions (5 days of changes plus margin) each night, replacing those partitions atomically, and document that revenue can be restated within that window.

::: try
**3. DR tiers.** Assign an RPO, RTO and strategy to each of: (a) bronze clickstream files in Cloud Storage, (b) the gold finance marts in BigQuery used for a monthly close, (c) a Bigtable online feature store used for real-time fraud scoring. Name the Google Cloud mechanisms.
:::

?? A solution to 3
(a) RPO about 15 minutes to 1 hour, RTO hours: a dual-region bucket (turbo replication if 15 minutes is needed) plus object versioning or soft delete against logical errors. (b) RPO 24 hours, RTO 1 day outside close week (stricter during close): daily table snapshots, time travel, and either a cross-region dataset replica or rebuild from replicated bronze and silver via Terraform-deployed pipelines; drill before each year-end. (c) RPO near zero, RTO minutes: Bigtable replication to a cluster in another region with multi-cluster routing, the scoring service deployed in both regions, and the feature pipelines able to rematerialise from BigQuery if the online store is lost.

::: try
**4. Interview drill.** In 45 minutes, design "a real-time dashboard of food delivery orders for an operations team in three cities, plus weekly courier pay calculations". Use the seven-step framework and write down at least three numbers, the grain of each table, and two failure modes with their handling.
:::

?? A solution to 4
Requirements: ops dashboard by city and zone, fresh within 30 seconds; courier pay weekly, exact and auditable. Numbers (assumed): 60,000 orders a day across three cities, peak 3x average, about 8 events per order (placed, accepted, picked up, delivered and so on) gives 480,000 events a day, about 17 per second at peak; trivial volume. Tables: `bronze.order_events` (one row per event), `silver.orders` (one row per order, current state), `gold.zone_minute` (one row per zone per minute), `gold.courier_pay_week` (one row per courier per week). Architecture: the order service writes events through an outbox to Pub/Sub; a BigQuery subscription loads bronze; a continuous query or 1-minute micro-batch maintains silver and the zone aggregates; Looker with BI Engine for the dashboard; weekly pay computed in Dataform from silver with reconciliation against the payments database (Datastream). Failure modes: duplicate events (dedupe on event ID in silver), late events (the pay calculation runs after a 2-day settling period, the dashboard tolerates small restatements), Pub/Sub subscription stalls (alert on oldest unacked message age, replay with seek). Cost is dominated by small always-on pieces; no Dataflow or Bigtable needed at this scale.

### What comes next

You can now design data platforms end to end and defend them with numbers: the architectures and layers, data products and the platform under them, the non-functional qualities of cost, capacity, recovery, privacy, location and migration, and a framework for design interviews. The next section, **DataOps and platform engineering**, is about delivering and running such platforms day to day: CI/CD for pipelines, infrastructure as code, environments and safe deployments, testing strategies, reliability engineering, monitoring and incidents, FinOps practice, and how data teams are organised.
