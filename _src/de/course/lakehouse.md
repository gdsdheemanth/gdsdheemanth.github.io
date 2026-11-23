This section is about the other way to build an analytics platform: keep data as open files in cheap object storage, and add just enough structure on top (a table format and a catalog) that many engines can treat those files as reliable database tables. It is written for data engineers who will design data lakes and lakehouses, choose formats, and keep them fast and affordable in production. It assumes the vocabulary from Data engineering foundations (OLTP versus OLAP, row versus column storage, batch versus streaming), comfortable SQL from SQL for data engineering, and the warehouse ideas from Data modeling and Data warehousing: fact and dimension tables, partitioning, slowly changing dimensions and the medallion layers. A little Python helps; every Python example here runs with the standard library only.

**What you will be able to do afterwards**

- Explain what a lakehouse is, why it appeared, and when it beats or loses to a warehouse such as BigQuery.
- Use Cloud Storage well as the storage layer of a lake: consistency, uploads, request costs, storage classes and lifecycle rules.
- Describe, at the byte level, how Parquet, ORC, Avro and Arrow lay out data, and pick the right one for each job, including the newer AI-oriented formats such as Lance.
- Explain how Apache Iceberg, Delta Lake, Apache Hudi, Apache Paimon and DuckLake turn a folder of files into an ACID table with snapshots, schema evolution and time travel, and compare them honestly.
- Run a lakehouse in production: catalogs and governance, compaction, clustering and Z-ordering, snapshot expiry, query tuning, streaming ingestion and unstructured data.

**The big picture.** A lakehouse has four layers. At the bottom is **object storage** (on Google Cloud, Cloud Storage): unlimited, durable and cheap, but it only knows about whole files, called objects. Above it sit **file formats** such as Parquet, which store a chunk of a table efficiently inside one file. Above them sits a **table format** such as Iceberg or Delta Lake: a set of metadata files that records exactly which data files make up the table at each moment, so that writers can commit changes atomically and readers always see a consistent snapshot. At the top is a **catalog**, a small service that knows where each table's current metadata lives, controls who may change it, and lets every engine (BigQuery, Spark, Flink, Trino, DuckDB) find tables by name. Most of this section is about one of those four layers, and most production problems come from forgetting which layer is responsible for what.

[[fig:lakehouse_layers]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Architecture and storage

This group lays the foundations. It starts with the architecture itself: what problem the lakehouse solves, where it came from and what it costs you. It then looks at the two layers that everything else stands on: object storage, which on Google Cloud means Cloud Storage, and the catalog, which turns a bucket of files into named, governed tables. The last topic looks at the open catalog standards (the Iceberg REST catalog, Apache Polaris, Project Nessie, Apache Gravitino) that let many engines and many catalogs share one set of tables.

If you have only used a warehouse, the important shift is this: in a warehouse, storage, metadata, security and compute are one product. In a lakehouse you assemble them from separate parts, which gives you freedom (any engine, open files, no lock-in) and responsibility (you must keep the parts consistent).

### Lakehouse Architecture

::: plain
A warehouse is like a restaurant: you hand over your ingredients, the restaurant stores and cooks them, and you can only eat there. A plain data lake is like a huge shared fridge: anyone can put anything in, anyone can take things out, and nobody knows what is fresh. A **lakehouse** is a shared fridge with a strict inventory book on the door: every item is recorded when it goes in or out, so any cook from any kitchen (any engine) can trust what is there and use it.
:::

**Three generations.** The first analytics platforms were **data warehouses**: databases built for analytical SQL, with their own storage, strong schemas and transactions. They were reliable but expensive, held only structured data, and kept data in a proprietary format that only the warehouse could read. Around 2010 companies started dumping raw files into **data lakes**, first on Hadoop's HDFS and later on cloud object storage. Lakes were cheap and accepted anything (logs, JSON, images, Parquet), and many engines could read the files. But a folder of files is not a table. There were no transactions, so a reader could see half of a job's output; no reliable schema, so columns drifted; no cheap updates or deletes; and listing millions of files to plan a query was slow. Many companies ended up running both: a lake for raw data and machine learning, and a warehouse fed from the lake for BI. That meant two copies, two security models and pipelines that kept them in sync.

The **lakehouse** (named and described in the CIDR 2021 paper by Armbrust, Ghodsi, Xin and Zaharia) keeps the lake's open files on object storage and adds a **transactional metadata layer** on top. That layer, an open table format such as Apache Iceberg, Delta Lake or Apache Hudi, records which files belong to each version of a table. With it you get warehouse features directly on the lake:

- **ACID transactions**: a write either fully appears or does not appear at all; concurrent writers do not corrupt each other; readers see a consistent snapshot.
- **Schema enforcement and evolution**: the table has a schema, writes that do not match are rejected, and columns can be added or renamed safely.
- **Updates, deletes and merges** at row level, needed for GDPR deletion requests and change data capture (CDC).
- **Time travel**: query the table as it was at an earlier version.
- **Performance features**: statistics for skipping files, clustering, compaction and caching, so SQL engines can be fast on open files.
- **One copy for every workload**: BI, SQL, streaming and machine learning read the same tables in open formats.

**The layers in practice.** On Google Cloud a typical lakehouse looks like this:

| Layer | Job | Google Cloud and open-source choices |
|---|---|---|
| Object storage | Durable, cheap bytes | Cloud Storage buckets |
| File format | Efficient columnar data inside a file | Parquet (default), ORC, Avro for raw landing |
| Table format | Snapshots, ACID commits, schema, deletes | Apache Iceberg (best supported on Google Cloud), Delta Lake, Hudi |
| Catalog | Table names, current metadata pointer, access control | Lakehouse runtime catalog (formerly BigLake metastore), Apache Polaris, Unity Catalog |
| Governance | Discovery, lineage, quality, policies | Knowledge Catalog (formerly Dataplex Universal Catalog), policy tags |
| Engines | Read and write tables | BigQuery, Managed Service for Apache Spark (formerly Dataproc), Dataflow, Trino, Flink, DuckDB |

Google Cloud's product name for this stack is **Google Cloud Lakehouse** (formerly BigLake). The BigQuery in depth section shows how BigQuery reads and writes Iceberg tables in it.

**The medallion pattern.** Most lakehouses organise tables in three layers of quality. **Bronze** holds raw data as it arrived (append-only, replayable). **Silver** holds cleaned, deduplicated, conformed data (types fixed, keys joined, CDC applied with `MERGE`). **Gold** holds business-level tables and aggregates for BI and ML. The names are a convention, not a technology: what matters is that each layer is reproducible from the one below.

```sql
-- Spark SQL on an Iceberg catalog called lake: bronze -> silver with MERGE
MERGE INTO lake.silver.customers AS t
USING (
  SELECT customer_id, name, email, op FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY change_ts DESC) AS rn
    FROM lake.bronze.customer_changes
    WHERE ingest_date = DATE '2026-11-21'
  ) WHERE rn = 1                       -- keep only the latest change per customer
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email
WHEN NOT MATCHED AND s.op <> 'D' THEN
  INSERT (customer_id, name, email) VALUES (s.customer_id, s.name, s.email);
```

> **Pitfall:** "Lakehouse" is not a product you install. Putting Parquet files in a bucket gives you a lake, not a lakehouse. Without a table format and a catalog, you still have no atomic commits, no safe concurrent writes and no consistent reads.

> **Tip:** On Google Cloud, if most of your work is SQL and BI and only BigQuery touches the data, native BigQuery tables are simpler and often faster. Choose a lakehouse when several engines must share one copy, when you need open files to avoid lock-in, or when storage volume makes object storage pricing decisive.

::: deeper
**What the CIDR paper actually argued.** The paper made three claims: that a metadata layer on object storage can provide transactions and versioning (Delta Lake was its example); that SQL performance on open formats can approach warehouse performance through caching, auxiliary data such as statistics and indexes, and data layout optimisation; and that direct file access through declarative DataFrame APIs suits machine learning better than ODBC exports from a warehouse. Its benchmark claims came from Databricks, so read them as a vendor's data point.

**Where the comparison really stands.** Warehouses have also moved: BigQuery separates storage and compute, reads and writes Iceberg, and exposes its storage through the Storage Read API with Arrow. Lakehouses have moved towards warehouses: managed catalogs, automatic compaction and fine-grained security. The real decision is less "lake or warehouse" and more "who owns the table metadata and which engines may write".

**Costs you take on.** Small files, snapshot growth, orphan files, compaction jobs, catalog availability and cross-engine feature gaps (one engine supports deletion vectors, another does not) become your problems. A lakehouse without table maintenance gets slower every week.

**Consistency across tables.** Most table formats commit one table at a time. A job that updates `orders` and `order_items` together can leave readers seeing one change without the other. Catalog-level transactions (Nessie, DuckLake, some managed catalogs) address this; otherwise design consumers to tolerate it or publish both tables behind a view switch.

> **Interview:** "What is a lakehouse, and why not just use a warehouse?" Define it as open file formats on object storage plus a transactional table format and a catalog, giving ACID, schema and time travel to many engines over one copy of data. Then give the trade-off: openness and engine choice versus the operational work of maintenance and governance, and say when you would still choose native warehouse tables.
:::

::: check
?? What does a table format add to a folder of Parquet files?
A transaction log or metadata tree that records exactly which files form each version of the table, giving atomic commits, consistent snapshots, schema enforcement and evolution, row-level updates and deletes, and time travel.

?? Name the four layers of a lakehouse and one Google Cloud or open-source choice for each.
Object storage (Cloud Storage), file format (Parquet), table format (Apache Iceberg), catalog (Lakehouse runtime catalog or Apache Polaris).

?? When would you keep data in native BigQuery tables instead of a lakehouse?
When BigQuery is the only engine that needs the data, especially for hot BI workloads, because native tables have the widest feature support and need no table maintenance from you.
:::

::: resources
- Paper: [Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf) · the CIDR 2021 paper that defined the term and its design
- Docs: [What is borderless Lakehouse?](https://docs.cloud.google.com/biglake/docs/introduction) · Google Cloud Lakehouse: Iceberg, the runtime catalog and engines on Google Cloud
- Book: *Delta Lake: The Definitive Guide* by Denny Lee, Tristen Wentling, Scott Haines and Prashanth Babu · lakehouse design and the medallion pattern explained through Delta Lake
- Book: [Apache Iceberg: The Definitive Guide](https://www.oreilly.com/library/view/apache-iceberg-the/9781098148614/) · the lakehouse from the Iceberg point of view, with catalogs and engines
- Blog: [What is a Data Lakehouse?](https://www.databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html) · the original 2020 blog post that introduced the idea, short and readable
- Paper: [BigLake: BigQuery's Evolution toward a Multi-Cloud Lakehouse](https://research.google/pubs/biglake-bigquerys-evolution-toward-a-multi-cloud-lakehouse/) · how Google built lakehouse features into BigQuery
:::

### Object storage for data lakes (Google Cloud Storage)

::: plain
Object storage is a giant cloakroom. You hand over a coat (a file), you get a ticket (its name), and you can collect it later from anywhere. You cannot alter the coat while it hangs there: to change it, you hand in a whole new coat under the same ticket. The cloakroom is enormous, cheap and almost never loses anything, but every visit to the counter costs a little, so a million tiny coats cost more to handle than a hundred big ones.
:::

**Cloud Storage** stores **objects** (immutable blobs of bytes plus metadata) in **buckets**. An object's name, such as `lake/orders/dt=2026-11-21/part-0001.parquet`, looks like a path, but in a standard bucket there are no real folders: the slashes are just characters in a flat namespace, and "listing a folder" means listing all names with a given prefix. Objects can be up to 5 TiB. You cannot modify part of an object; you replace the whole object, and each version gets a new **generation number**.

**Consistency.** Cloud Storage is **strongly consistent** for object operations: once a write or delete succeeds, every subsequent read, metadata read and listing in any region sees it. That matters for table formats: a reader that has learned the name of a new metadata file can always read it. (Two things are eventually consistent: IAM permission changes take time to apply, and publicly cached objects may be served stale from caches.)

**Conditional writes.** Every write can carry a **precondition**. `ifGenerationMatch=0` means "create this object only if it does not exist yet", and `ifGenerationMatch=N` means "replace it only if it is still at generation N". These are the atomic "put if absent" and "compare and swap" operations that table formats need to commit safely without a database. Delta Lake on Cloud Storage relies on exactly this to make sure only one writer creates commit number 42.

**Uploads.** Small files can be sent in one request. Large files should use a **resumable upload**, which survives network failures and continues from the last confirmed byte. For parallelism there is the XML API **multipart upload** (upload parts concurrently, then complete) and **parallel composite uploads**, where the client uploads pieces as separate objects and then **composes** them (up to 32 sources per compose request) into one. The `gcloud storage` command uses these automatically for large files. Engines such as Spark use the Cloud Storage connector, which buffers output and uploads it in the background.

**Reads.** Readers can request a **byte range** of an object. Columnar formats depend on this: a Parquet reader first fetches the footer at the end of the file, then only the column chunks it needs.

**Request costs.** You pay for storage (per GiB per month, by storage class), for network egress, and for **operations**. Writes, lists and most metadata changes are **Class A** operations; reads and metadata reads are **Class B**. At list prices for a regional Standard bucket at the time of writing, Class A costs about $0.005 per 1,000 and Class B about $0.0004 per 1,000 (check the pricing page for your region). A worked example of why small files hurt:

```text
Streaming job: 200 partitions x 1 file per minute = 288,000 new files per day
Writes: 288,000 Class A ops/day      ~ $1.44/day, ~ $43/month (cheap)
Reads:  a daily query over 30 days opens 8.6 million files, 2+ ranged reads each
        ~ 17 million Class B ops     ~ $7 per run, and minutes of planning and latency
Compacted to 256 MiB files: a few thousand files, cents per run, seconds to plan
```

The money is rarely the main pain; the latency of millions of requests is.

**Request rate.** A bucket starts with capacity for roughly 1,000 object writes and 5,000 object reads per second and scales up automatically as load grows. Google advises ramping up gradually (doubling no faster than about every 20 minutes) and avoiding sequential names such as timestamps at the start of object names during massive bursts, because they concentrate load on one key range.

**Storage classes and lifecycle.** The classes are **Standard**, **Nearline** (30-day minimum storage duration), **Coldline** (90 days) and **Archive** (365 days): cheaper to store, more expensive to read, with early-deletion charges if you delete before the minimum. **Autoclass** moves each object between classes based on access. **Lifecycle rules** act on objects automatically: delete them, change their class, or abort stale multipart uploads, based on conditions such as age, prefix, suffix or number of newer versions.

```json
{
  "lifecycle": {
    "rule": [
      {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
       "condition": {"age": 30, "matchesPrefix": ["landing/"]}},
      {"action": {"type": "Delete"},
       "condition": {"age": 400, "matchesPrefix": ["landing/"]}},
      {"action": {"type": "AbortIncompleteMultipartUpload"},
       "condition": {"age": 7}}
    ]
  }
}
```

```bash
gcloud storage buckets create gs://acme-lake-eu --location=europe-west1 \
  --uniform-bucket-level-access
gcloud storage buckets update gs://acme-lake-eu --lifecycle-file=lifecycle.json
```

> **Pitfall:** Never let lifecycle rules delete files under a table format's prefix. Iceberg or Delta metadata may still reference an "old" file; deleting it breaks the table. Expire data through the table format (snapshot expiry, `VACUUM`), and use lifecycle rules only for raw landing zones and temporary paths.

> **Pitfall:** Keep the bucket in the same region as your engines. Cross-region reads add latency and transfer charges, and some BigQuery features require co-location.

::: deeper
**Hierarchical namespace buckets.** A bucket created with hierarchical namespace enabled has real folders: renaming a folder is a single atomic metadata operation instead of copying and deleting every object, and the bucket gets higher initial request rates. That helps Hadoop-style jobs that write to a temporary folder and rename it on success. Table formats do not need renames (they commit through metadata), so HNS mainly helps older Spark and Hive output committers.

**Soft delete and versioning.** By default, new buckets keep deleted objects recoverable for 7 days (soft delete), and you pay storage for them. Compaction and snapshot expiry delete a lot of data in a lakehouse, so soft-deleted bytes can be a visible part of the bill. Decide deliberately: shorten the window on high-churn table buckets if the table format's own time travel already covers recovery. Object versioning keeps noncurrent versions until a lifecycle rule removes them; it is rarely useful for table data.

**Caching close to compute.** Anywhere Cache is an SSD read cache in a zone, in front of a bucket, which helps repeated reads by Spark or ML training jobs. Cloud Storage FUSE mounts a bucket as a file system for tools that expect local paths, with file-system semantics weaker than a real disk (no atomic rename of non-empty directories, no concurrent writes to one file).

**Colossus underneath.** Cloud Storage and BigQuery both store bytes in Colossus, Google's cluster file system, which is why BigQuery can read external Parquet in Cloud Storage efficiently.

> **Interview:** "Why do table formats avoid listing directories?" Listing is slow on huge prefixes, costs Class A operations, and on some older object stores was only eventually consistent. Iceberg and Delta keep an explicit list of files in metadata, so planning reads a few metadata files instead of listing millions of objects.
:::

::: check
?? Why does Cloud Storage's `ifGenerationMatch=0` precondition matter to table formats?
It makes object creation conditional ("only if absent"), so two writers cannot both create the same commit file. That is the atomic primitive Delta Lake needs for optimistic concurrency on Cloud Storage.

?? A team writes one Parquet file per minute per partition. What problems appear, and which matter most?
Millions of small files: more operations to pay for, but mainly slow query planning, many ranged reads per query and large table metadata. Compaction and larger write batches fix it.

?? Why should lifecycle delete rules not cover table data folders?
Table metadata may still reference old files for current or time-travel snapshots. Deleting them outside the table format corrupts the table; use snapshot expiry or `VACUUM` instead.
:::

::: resources
- Docs: [Cloud Storage consistency](https://cloud.google.com/storage/docs/consistency) · exactly which operations are strongly consistent
- Docs: [Request preconditions](https://cloud.google.com/storage/docs/request-preconditions) · generation-match conditions for safe concurrent writes
- Docs: [Request rate and access distribution guidelines](https://cloud.google.com/storage/docs/request-rate) · ramp-up advice and naming for very high request rates
- Docs: [Object Lifecycle Management](https://cloud.google.com/storage/docs/lifecycle) · actions, conditions and examples for lifecycle rules
- Docs: [Cloud Storage pricing](https://cloud.google.com/storage/pricing) · storage classes, operation classes and minimum durations
- Docs: [Hierarchical namespace](https://cloud.google.com/storage/docs/hns-overview) · real folders, atomic renames and when to use them
- Blog: [A peek behind Colossus, Google's file system](https://cloud.google.com/blog/products/storage-data-transfer/a-peek-behind-colossus-googles-file-system) · the storage system underneath Cloud Storage and BigQuery
:::

### Data Lake Governance & Cataloging

::: plain
A library with a million books and no catalogue is useless: you cannot find anything, you do not know which copy is current, and anyone could walk out with a rare book. A **data catalog** is the library's catalogue and front desk in one. It knows the name and location of every table, its columns, who owns it and who is allowed to read it, and in a lakehouse it also keeps the one official pointer to each table's current version.
:::

"Catalog" means two related things, and mixing them up causes confusion:

- A **technical catalog** (also called a metastore) is part of the query path. Engines ask it "where is table `sales.orders`, and what is its schema?" before they can read. For table formats it also stores the pointer to the current metadata and performs the atomic commit. If it is down, queries fail.
- A **business catalog** (a discovery or governance catalog) is for people. It indexes many systems, adds descriptions, owners, glossary terms, classifications, data quality scores and lineage, and lets people search. It is usually not in the query path.

**The Hive Metastore.** The first widely used technical catalog came from Apache Hive. The **Hive Metastore (HMS)** is a Thrift service backed by a relational database (MySQL or PostgreSQL). It stores databases, tables, columns, the storage location of each table, the file format and SerDe, and, crucially, a list of **partitions** with the folder of each. Spark, Trino, Flink and Hive all speak to it, which is why it became the lake's de facto standard. Its weaknesses shaped what came next: it tracks folders, not files, so engines must list folders; tables with hundreds of thousands of partitions make it slow; it has no real transaction model across engines; and its security is coarse. Iceberg and Delta can store their pointer in HMS, but the modern replacement is a catalog designed for table formats.

**Modern technical catalogs.**

| Catalog | What it is | Notes |
|---|---|---|
| Lakehouse runtime catalog (formerly BigLake metastore) | Google Cloud's serverless catalog for Iceberg | Iceberg REST API, credential vending, used by BigQuery, Spark, Flink and Trino on Google Cloud |
| Unity Catalog | Databricks' catalog, open-sourced in 2024 under the LF AI & Data Foundation | Three-level names `catalog.schema.table`, governs tables, volumes, functions and models; Delta and Iceberg access |
| Apache Polaris | An open-source Iceberg catalog, originally from Snowflake | Implements the Iceberg REST spec with role-based access control and credential vending |
| Hive Metastore | The classic Thrift metastore | Still common; Google Cloud offers a managed Hive-compatible endpoint |
| Project Nessie | A catalog with Git-like branches and commits | Covered in the next topic |

**Governance on top.** On Google Cloud the business catalog is **Knowledge Catalog** (formerly Dataplex Universal Catalog, and Data Catalog before that). It automatically harvests metadata from BigQuery, Lakehouse tables, Cloud Storage, Cloud SQL, Spanner and more; lets you attach **aspects** (typed metadata such as owner or sensitivity) and glossary terms; runs data profiling and data quality scans; and shows lineage. Access control for lake tables is enforced by the engine that reads them: BigQuery enforces row-level security, column-level security through **policy tags** and dynamic masking on Lakehouse tables because it reads the files on the user's behalf through a connection's service account.

**Access delegation and credential vending.** The old way to secure a lake was bucket permissions: give each user or job read access to the folders they need. It breaks down quickly (folders do not match tables, and anyone with bucket access can bypass row and column rules). Modern catalogs **vend credentials**: an engine asks the catalog for a table, the catalog checks the caller's permissions on that table, and returns short-lived storage credentials scoped to that table's files. Users never get bucket access at all.

```sql
-- BigQuery: govern lake tables like native ones
CREATE ROW ACCESS POLICY eu_only
ON lake.orders
GRANT TO ("group:analysts-eu@acme.com")
FILTER USING (region = 'EU');
```

> **Pitfall:** Two technical catalogs pointing at the same table is a classic corruption bug. If Spark commits through one catalog and Trino through another, each believes it has the current metadata pointer, and one will silently overwrite the other's commit. Each table must have exactly one catalog of record; other catalogs may only federate (read through) it.

> **Tip:** Write down for every table: its catalog of record, its owner, which engines may write it, and which may only read it. Most lakehouse incidents trace back to a missing line in that list.

::: deeper
**What a catalog commit really is.** For Iceberg, the catalog stores one pointer per table: the location of the current `metadata.json`. A commit is a compare-and-swap: "set the pointer to `v43.metadata.json` only if it is still `v42.metadata.json`". The catalog can implement that with a database transaction (JDBC catalog, Polaris, the Lakehouse runtime catalog), with HMS table locks, or with an object store precondition. Everything else about Iceberg's safety depends on this one operation being atomic.

**Lineage standards.** OpenLineage is an open standard for lineage events emitted by Spark, Airflow, dbt and Flink; Knowledge Catalog accepts OpenLineage events through the Data Lineage API, so lineage from open-source tools appears beside lineage from BigQuery.

**Partition limits in HMS.** Each `ALTER TABLE ADD PARTITION` is a row in the metastore database; queries that filter partitions turn into SQL against that database. Large tables with many partitions slow every engine that plans against them, which is one reason Iceberg moved partition tracking into its own manifest files.

> **Interview:** "How would you secure a lakehouse shared by Spark, Trino and BigQuery?" One catalog of record per table, no direct bucket access for people, credential vending or a service-account connection for engines, fine-grained rules (row and column) enforced in the engines that support them, and a governance catalog (Knowledge Catalog) for discovery, classification and lineage.
:::

::: check
?? What is the difference between a technical catalog and a business catalog?
A technical catalog is in the query path: engines use it to find tables, schemas and current metadata, and to commit. A business catalog is for people: search, descriptions, owners, classifications, quality and lineage across many systems.

?? Why did the Hive Metastore struggle with very large tables?
It tracks partitions as folders in a relational database, so huge partition counts make planning slow and engines still have to list files in each folder.

?? What is credential vending?
The catalog checks the caller's permission on a table and returns short-lived storage credentials scoped to that table's files, so users and jobs never need broad bucket permissions.
:::

::: resources
- Docs: [About the Lakehouse runtime catalog](https://docs.cloud.google.com/bigquery/docs/about-blms) · Google Cloud's serverless Iceberg catalog and its REST API
- Docs: [Knowledge Catalog overview](https://cloud.google.com/dataplex/docs/catalog-overview) · discovery, aspects, glossaries and lineage on Google Cloud
- Docs: [Unity Catalog (open source)](https://docs.unitycatalog.io/) · the open-source Unity Catalog server, its namespace and APIs
- Docs: [Apache Polaris](https://polaris.apache.org/) · an open Iceberg REST catalog with access control and credential vending
- Docs: [Hive Metastore administration](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration) · how the classic metastore is structured and run
- Spec: [OpenLineage](https://openlineage.io/docs/) · the open lineage standard that connects Spark, Airflow and dbt to catalogs
:::

### Open Lakehouse & Catalog Federation

::: plain
Imagine every country issuing passports in its own shape that only its own border guards could read. Travel would be miserable. An open **catalog standard** is the agreed passport format: any engine can ask any compliant catalog "where is this table and may I read it?" in the same way. **Federation** is the agreement between countries to honour each other's passports, so one catalog can show tables that actually live in another.
:::

**The problem.** A table format like Iceberg is open, but every engine still needs a catalog client. Before 2023, each catalog (Hive, Glue-style services, JDBC, Nessie, vendor catalogs) needed its own client library in every engine, and features differed. The **Iceberg REST catalog specification** fixed this: it is an OpenAPI description of HTTP endpoints that any catalog can implement and any engine can call. Engines now need one client, and catalogs compete on what happens behind the API.

**What the REST catalog API covers.**

- `GET /v1/config`: the client learns server defaults and the URL prefix to use.
- Namespaces: create, list, load and drop (`/v1/{prefix}/namespaces`).
- Tables: create, list, load, rename, drop, and register an existing metadata file.
- **Commit**: the client sends a list of **requirements** (for example `assert-current-schema-id` or `assert-ref-snapshot-id` for `main`) and **updates** (add snapshot, set ref, add schema). The server applies them atomically only if every requirement still holds; otherwise it returns `409 Conflict` and the client retries on top of the new state.
- **Multi-table transactions** (`/v1/{prefix}/transactions/commit`), where the server supports them.
- **Access delegation**: the client sends the header `X-Iceberg-Access-Delegation: vended-credentials` and receives temporary storage credentials in the load-table response.
- Scan planning endpoints, so a server can plan which files to read on the client's behalf.

```python
# PyIceberg against any Iceberg REST catalog (pip install "pyiceberg[gcsfs,pyarrow]")
from pyiceberg.catalog import load_catalog

catalog = load_catalog("lake", **{
    "type": "rest",
    "uri": "https://catalog.example.com/api/catalog",
    "warehouse": "analytics",
    "header.X-Iceberg-Access-Delegation": "vended-credentials",
})
table = catalog.load_table("sales.orders")
print(table.schema())
rows = table.scan(row_filter="order_date >= '2026-11-01'", limit=10).to_arrow()
```

**The main open catalogs.**

- **Apache Polaris** implements the REST spec with a security model of principals, principal roles, catalog roles and privileges on catalogs, namespaces and tables. It supports internal catalogs (Polaris is the catalog of record) and external catalogs (Polaris mirrors tables managed elsewhere, read-only).
- **Project Nessie** adds **Git-like semantics** across many tables: commits, branches, tags and merges. You can create a branch `etl-2026-11-21`, run a whole pipeline that changes ten tables, validate the results and then merge the branch into `main` atomically. Readers of `main` never see a half-finished pipeline. Nessie serves Iceberg through the REST API as well.
- **Apache Gravitino** is a **meta-catalog**: a single API and permission model over many underlying catalogs (Hive, Iceberg REST, JDBC databases, Kafka topics, file sets and models). It is aimed at organisations with many regions, clouds and systems who want one place to manage metadata.
- **Unity Catalog** (open-source edition) and vendor catalogs such as Snowflake Horizon also expose Iceberg REST endpoints for their tables.

**Federation on Google Cloud.** The Lakehouse runtime catalog implements the Iceberg REST API for tables it manages, and can federate other catalogs so BigQuery and Spark on Google Cloud can query tables registered in, for example, Unity Catalog, without copying them. In federation the remote catalog stays the catalog of record; Google Cloud reads through it.

```bash
# Spark on Managed Service for Apache Spark, pointing at an Iceberg REST catalog
spark-sql \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=rest \
  --conf spark.sql.catalog.lake.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog \
  --conf spark.sql.catalog.lake.warehouse=gs://acme-lake-eu \
  -e "SHOW NAMESPACES IN lake"
```

(Authentication settings for the Google endpoint, shown in BigQuery in depth, are also needed.)

> **Pitfall:** "Supports Iceberg REST" does not mean "supports everything". Check per engine and catalog: write support, multi-table transactions, credential vending, view support, Iceberg V3 features and branch handling vary. Test the exact combination you will run.

> **Tip:** Pick the catalog first, then the engines. The catalog decides security, the commit path and federation; engines are easier to add later.

::: deeper
**Why a server-side commit protocol matters.** With file-based catalogs, the client builds the new metadata file and performs the swap itself, so every engine must implement the commit logic correctly. With REST, the server validates requirements and applies updates, so it can add checks (for example rejecting schema changes that break policy), centralise audit logging and coordinate multi-table commits.

**Catalog-level versioning versus table-level.** Iceberg branches and tags exist inside one table's metadata. Nessie versions the whole catalog: a Nessie commit can contain new metadata pointers for many tables. That is what makes cross-table atomic publication and "data as code" workflows (branch, test, merge) possible.

**Views.** The Iceberg view spec stores SQL views in the catalog with one or more dialect-specific SQL texts. Sharing views across engines is still harder than sharing tables, because SQL dialects differ.

> **Interview:** "Several teams use different engines and catalogs; how do you avoid copies?" Standardise on one open table format, put each table under one catalog of record that speaks the Iceberg REST API, federate other catalogs read-only, and use credential vending so access is decided by the catalog rather than bucket permissions.
:::

::: check
?? How does a REST catalog commit stay safe when two writers race?
The client sends requirements (such as "the current snapshot of main is X") with its updates. The server applies them atomically only if the requirements still hold; the loser gets a 409 Conflict and retries on the new state.

?? What does Project Nessie add that table-level branches do not?
Catalog-wide commits and branches, so a pipeline can change many tables on a branch and publish them all at once with a merge.

?? What is the role of Apache Gravitino?
A meta-catalog: one API and access model over many underlying catalogs and data systems.
:::

::: resources
- Spec: [Iceberg REST catalog OpenAPI specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) · the authoritative list of endpoints, requirements and updates
- Docs: [Apache Polaris](https://polaris.apache.org/) · the open REST catalog, its entities and access control model
- Docs: [Project Nessie](https://projectnessie.org/) · Git-like branches and commits across many tables
- Repo: [Apache Gravitino](https://github.com/apache/gravitino) · the meta-catalog that unifies many catalogs and systems
- Docs: [PyIceberg](https://py.iceberg.apache.org/) · a Python client for Iceberg catalogs and tables, handy for learning the API
- Docs: [About the Lakehouse runtime catalog](https://docs.cloud.google.com/bigquery/docs/about-blms) · Google Cloud's REST catalog and federation
:::

## File formats

A table format records which files make up a table; the **file format** decides how the rows inside each file are laid out. That choice drives most of a lakehouse's performance: how many bytes a query reads, how well data compresses, how quickly a reader can skip what it does not need, and whether one record can be fetched without reading thousands of others.

This group covers the four formats every data engineer meets: **Parquet** and **ORC** (columnar storage formats for analytics), **Avro** (a row format for messages, logs and landing data) and **Arrow** (the in-memory columnar format that engines use to process and exchange data). It ends with the newer formats designed for machine learning and random access, such as Lance. The Data engineering foundations section introduced these formats in one table; here you open the files and look inside.

### Apache Parquet — Format Internals

::: plain
Imagine a spreadsheet printed as a book. A row format prints one customer per page. Parquet instead cuts the sheet into thick slices of rows, and within each slice prints each column on its own pages: all the countries together, then all the amounts together. Similar values side by side shrink enormously when compressed, and a reader who only wants amounts can skip straight to those pages. An index at the back of the book says what is on every page.
:::

**Parquet** is the default storage format of the lakehouse: Iceberg, Delta, Hudi and DuckLake all store data as Parquet by default, and BigQuery, Spark, Trino, Flink, DuckDB and Polars all read it. It descends from Google's Dremel paper (2010), which is also the ancestor of BigQuery's storage.

[[fig:lakehouse_parquet]]

**File layout**, from the outside in:

1. The file starts and ends with the 4-byte magic `PAR1`.
2. The data is split into **row groups**: horizontal slices of rows, typically 128 MiB to 1 GiB each. A row group is the unit of parallelism: two tasks can read two row groups independently.
3. Inside a row group, each column is stored as one **column chunk**, contiguous on disk. Reading one column means reading one chunk per row group.
4. Each column chunk is a series of **pages** (typically about 1 MiB): an optional **dictionary page** first, then **data pages**. A page is the unit of encoding and compression.
5. At the end sits the **footer**: the `FileMetaData` structure, serialised with Apache Thrift, then a 4-byte length and the final `PAR1`. The footer holds the schema, every row group's location and row count, and for every column chunk its offsets, sizes, encodings, codec and **statistics** (minimum, maximum, null count).

A reader therefore starts at the end: one ranged read for the last 8 bytes gives the footer length, a second read fetches the footer, and from then on it reads only the column chunks (and pages) it needs.

**Encodings.** Before compression, each page is **encoded** to exploit patterns in the data:

| Encoding | Idea | Good for |
|---|---|---|
| `PLAIN` | Values written one after another | Fallback for anything |
| Dictionary (`RLE_DICTIONARY`) | Store each distinct value once; data becomes small integer ids | Low and medium cardinality strings and numbers |
| RLE / bit-packing hybrid | Runs of equal values as (value, count); short runs bit-packed | Dictionary ids, repetition and definition levels, booleans |
| `DELTA_BINARY_PACKED` | Store differences between neighbours, bit-packed | Sorted or slowly increasing integers, timestamps |
| `DELTA_BYTE_ARRAY` | Store shared prefixes once | Sorted strings, URLs |
| `BYTE_STREAM_SPLIT` | Split floats into byte streams before compressing | Floating-point measurements |

This standard-library sketch shows why dictionary plus run-length encoding is so effective on a column of country codes:

```python
values = ["DE", "DE", "DE", "FR", "FR", "DE", "US", "US", "US", "US"]
dictionary = list(dict.fromkeys(values))      # distinct values, first-seen order
ids = [dictionary.index(v) for v in values]
print(dictionary)  # ['DE', 'FR', 'US']
print(ids)         # [0, 0, 0, 1, 1, 0, 2, 2, 2, 2]
runs = []
for i in ids:
    if runs and runs[-1][0] == i:
        runs[-1][1] += 1
    else:
        runs.append([i, 1])
print(runs)        # [[0, 3], [1, 2], [0, 1], [2, 4]]
print((len(dictionary) - 1).bit_length())   # 2  (bits needed per id)
```

Ten strings became three dictionary entries and four runs of 2-bit ids. Sorting the data by country before writing would make it a single run per country, which is why sort order matters so much for file size.

After encoding, each page is **compressed** with a codec chosen per column chunk: `SNAPPY` (fast, Spark's default), `ZSTD` (better ratio at similar speed, increasingly the default choice), `GZIP`, `LZ4_RAW` or `BROTLI`.

**Writing and inspecting** with PyArrow:

```python
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({"country": ["DE", "FR", "DE"], "amount": [10, 20, 30]})
pq.write_table(table, "orders.parquet", compression="zstd")

meta = pq.ParquetFile("orders.parquet").metadata
col = meta.row_group(0).column(1)          # the amount column chunk
print(meta.num_row_groups, meta.num_rows)  # 1 3
print(col.statistics.min, col.statistics.max, col.compression)   # 10 30 ZSTD
```

In DuckDB, `SELECT * FROM parquet_metadata('orders.parquet')` shows the same footer as a table.

> **Pitfall:** Statistics only help if the data is clustered. If every file contains every country, each column chunk's min is `AT` and max is `US`, and a filter on `country = 'DE'` can skip nothing. Sort or cluster on the columns you filter by before writing.

> **Tip:** Aim for files of roughly 128 MiB to 1 GiB with row groups of 128 MiB or more for analytical tables. Many tiny row groups make footers large and reads inefficient.

::: deeper
**Nested data: repetition and definition levels.** Parquet stores nested and repeated fields as flat columns using the Dremel encoding. Each value carries a **definition level** (how many optional or repeated fields on its path are actually present) and a **repetition level** (at which repeated field this value starts a new list). For a column `tags` declared `repeated binary tag (STRING)`:

| Record | Value | Repetition level | Definition level |
|---|---|---|---|
| `{tags: ["a", "b"]}` | a | 0 (new record) | 1 |
| | b | 1 (same list) | 1 |
| `{tags: []}` | (null) | 0 | 0 (list absent) |
| `{tags: ["c"]}` | c | 0 | 1 |

From the levels alone a reader can rebuild the original records, and a flat column of a deeply nested field can still be scanned and compressed on its own.

**Page index.** Footer statistics are per column chunk. The optional **page index** (a `ColumnIndex` with min, max and null information per page, and an `OffsetIndex` with page locations) lets readers skip individual pages, and line up the same rows across columns.

**Bloom filters.** For high-cardinality columns where min and max are useless (ids, emails), a writer can add a **split block Bloom filter** per column chunk. A reader looking for `user_id = 'u-8812'` checks the filter and skips chunks that definitely do not contain it. Bloom filters cost space and help only equality and `IN` filters.

**Types.** Physical types are few (`BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, plus the deprecated `INT96` used by old Spark and Hive for timestamps). **Logical types** annotate them: `STRING`, `DATE`, `TIMESTAMP` with unit and UTC flag, `DECIMAL`, `UUID`, `JSON`. Newer additions include `FLOAT16`, a `VARIANT` type for semi-structured data, and geospatial types. Engines adopt new types at different speeds; check every engine that reads the table before using one.

**Why the footer is a weakness.** Thrift footers must be decoded entirely before reading anything. For tables with thousands of columns (common in ML feature stores), that decode becomes slow and the footer grows to megabytes. This is one motivation for the formats in the topic Lance and AI-native columnar formats.

> **Interview:** "Why is Parquet fast for analytics?" Column pruning (read only needed columns), predicate pushdown with statistics, dictionary page and Bloom filters (skip row groups and pages), encodings that exploit sorted and repetitive data, and per-column compression. Mention that all of it depends on good file sizes and data clustering.
:::

::: check
?? Why does a Parquet reader start at the end of the file?
The footer, with the schema, row group locations and column statistics, is at the end, followed by its length and the magic bytes. The reader needs it to know which byte ranges to read.

?? A filter on `customer_id = 42` skips no row groups, although statistics exist. Why, and what can help?
The data is not sorted or clustered by `customer_id`, so every row group's min-max range contains 42. Sort or cluster by the column, or add Bloom filters for equality lookups.

?? What is the difference between encoding and compression in Parquet?
Encoding (dictionary, RLE, delta) rewrites values using knowledge of their type and patterns; compression (Snappy, ZSTD) is a general byte compressor applied to each encoded page afterwards.
:::

::: resources
- Docs: [Parquet file format](https://parquet.apache.org/docs/file-format/) · the official description of row groups, pages, metadata and encodings
- Spec: [parquet-format repository](https://github.com/apache/parquet-format) · the Thrift definition of every metadata structure, encoding and logical type
- Paper: [Dremel: Interactive Analysis of Web-Scale Datasets](https://research.google/pubs/dremel-interactive-analysis-of-web-scale-datasets-2/) · the Google paper behind Parquet's nested encoding and BigQuery
- Blog: [Dremel made simple with Parquet](https://blog.x.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet) · the classic, illustrated explanation of repetition and definition levels
- Docs: [PyArrow file formats API](https://arrow.apache.org/docs/python/api/formats.html) · the Parquet reader and writer options for row groups, compression and statistics
- Docs: [Loading Parquet data into BigQuery](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet) · how BigQuery maps Parquet types and schemas
:::

### Apache ORC — Format Internals

::: plain
ORC is Parquet's sibling, born in the Hive world. It also cuts a table into slices of rows and stores each column separately, but it builds a small index into every slice: for each block of ten thousand rows it notes the smallest and largest value, like a filing cabinet with a label on every drawer. A reader can skip drawers, not just whole cabinets.
:::

**ORC** (Optimized Row Columnar) was created in 2013 by Hortonworks and Facebook to speed up Apache Hive, replacing the older RCFile. It remains the native format of Hive's ACID tables and is well supported by Trino, Spark, Flink and Iceberg. Outside Hive-centred platforms Parquet is far more common, so think of ORC as a format you will meet, maintain and sometimes migrate from.

**File layout.**

- The file starts with the magic `ORC`.
- The body is a series of **stripes** (64 MiB by default in the Java writer, `orc.stripe.size`). A stripe is the unit of parallel reading, like Parquet's row group.
- Each stripe has three parts: **index data** (for each column, a **row index** entry per **row group** of 10,000 rows by default, `orc.row.index.stride`, holding min, max, counts and the stream positions needed to jump there), **row data** (the column **streams**), and a **stripe footer** listing the streams and the encoding used for each column.
- At the end: the **file footer** (schema, the list of stripes, and file-level statistics per column), then the **postscript** (compression codec, footer length, format version), and finally one byte holding the postscript's length.

**Streams.** Each column is stored as several streams. An integer column has a `PRESENT` stream (a bit per row: is it null?) and a `DATA` stream. A string column with dictionary encoding has `DATA` (dictionary ids), `LENGTH` and `DICTIONARY_DATA`. Splitting values from nulls and lengths lets each stream use the best encoding.

**Encodings.** Integers use **RLE v2**, which picks per run between short repeats, direct bit-packing, patched base (for runs with a few outliers) and delta encoding. Strings use a dictionary when the ratio of distinct values to rows is below a threshold (`orc.dictionary.key.threshold`, 0.8 by default), and direct encoding otherwise. Streams are then compressed in chunks with ZLIB (the Java default), Snappy, LZ4 or ZSTD.

**Three levels of skipping.** File statistics in the footer, stripe statistics, and the row index inside each stripe. With the row index a reader can skip 10,000-row groups inside a stripe, which is finer than Parquet's footer statistics (Parquet's optional page index plays the same role). Optional **Bloom filters** per column (`orc.bloom.filter.columns`) help equality lookups.

```sql
-- Hive / Spark SQL: an ORC table with Bloom filters and ZSTD
CREATE TABLE events (
  event_id STRING, user_id BIGINT, event_type STRING, ts TIMESTAMP
)
STORED AS ORC
TBLPROPERTIES (
  'orc.compress' = 'ZSTD',
  'orc.bloom.filter.columns' = 'user_id',
  'orc.stripe.size' = '134217728'
);
```

```bash
# inspect a file with the ORC tools jar: stripes, streams, encodings and statistics
java -jar orc-tools-<version>-uber.jar meta part-00000.orc
```

**Hive ACID.** ORC is the storage of Hive's transactional tables. A Hive ACID table is a folder of `base_N` directories (a full snapshot as of write id N) and `delta_M_N` and `delete_delta_M_N` directories (inserts and deletes from later transactions). Every row is wrapped in a struct with `operation`, `originalTransaction`, `bucket`, `rowId` and `currentTransaction`, so readers can merge base and deltas and hide deleted rows. An update is written as a delete plus an insert. **Minor compaction** merges deltas together; **major compaction** rewrites base and deltas into a new base. The transaction state (open, committed, aborted) lives in the Hive Metastore database, which makes the format usable mainly from Hive and engines that implement Hive's ACID protocol.

> **Pitfall:** Hive ACID tables are hard to share. Spark's built-in readers do not handle them fully, and other engines vary. If many engines need the data, migrate to Iceberg (which can keep ORC or switch to Parquet files) rather than building on Hive ACID.

> **Tip:** On Google Cloud, BigQuery loads ORC files and reads them through external and BigLake tables, and Managed Service for Apache Spark runs Hive and Spark on ORC. For new tables, Parquet is usually the better default because more engines optimise for it.

::: deeper
**ORC versus Parquet in practice.** Both are columnar with statistics, dictionary encoding and compression. ORC's lightweight indexes and RLE v2 often give smaller files and good skipping; Parquet has the broader ecosystem, better nested-type support across engines, and is the format table formats and vendors optimise first. Benchmarks swing both ways depending on data and engine; pick by ecosystem unless you have measured.

**Types and timestamps.** ORC has a rich type system including `union`, `decimal`, `char` and `varchar`, and distinguishes `timestamp` (local) from `timestamp with local time zone` (an instant). Time-zone handling of timestamps has differed between Hive versions, a frequent source of off-by-hours bugs in migrations.

**Vectorised reading.** ORC readers decode streams into column vectors (Hive's `VectorizedRowBatch`, 1,024 rows by default), which is where the format's speed comes from in Hive and Trino.

**DWRF.** Meta forked ORC into DWRF for its warehouse, with changes for very wide tables and encryption; it informed the design of Meta's newer Nimble format.

> **Interview:** "How do Hive ACID tables support updates on immutable files?" They write new delta and delete-delta files per transaction, tag each row with transaction and row ids, merge base and deltas at read time, and compact periodically. The same merge-on-read idea appears in Hudi and in Iceberg and Delta deletion files.
:::

::: check
?? What are ORC's three levels of statistics?
File-level statistics in the footer, stripe-level statistics, and the row index inside each stripe, with an entry per row group of 10,000 rows by default.

?? How does a Hive ACID table represent an `UPDATE`?
As a delete (a row in a `delete_delta` directory identifying the old row by transaction, bucket and row id) plus an insert in a new delta directory. Readers merge them; compaction later rewrites them.

?? Why is Parquet usually preferred over ORC for new lakehouse tables?
Broader engine and vendor support, especially in table formats and in BigQuery, Spark and DuckDB; ORC's advantages are real but smaller than the ecosystem difference.
:::

::: resources
- Spec: [ORC specification v1](https://orc.apache.org/specification/ORCv1/) · the byte-level layout of stripes, streams, indexes and encodings
- Docs: [Apache ORC documentation](https://orc.apache.org/docs/) · types, configuration, tools and language bindings
- Docs: [Hive transactions](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions) · how Hive ACID uses ORC base and delta directories and compaction
- Docs: [Loading ORC data into BigQuery](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-orc) · type mapping and options on Google Cloud
- Book: *Programming Hive* by Edward Capriolo, Dean Wampler and Jason Rutherglen · the Hive world ORC was designed for
:::

### Apache Avro — Format Internals

::: plain
Avro is a compact way to write records one after another, like a form where you print only the answers, not the questions. The questions (the **schema**) are written once, at the top of the file or in a shared registry. Because the reader always has the schema, the data can be tiny, and because the schema is explicit, you can change it carefully over time without breaking old data.
:::

**Avro** is a **row-oriented**, schema-based binary format from the Hadoop project (2009). Each record is stored whole, which makes Avro good for writing one record at a time: messages on Kafka, CDC events, logs and raw landing files. It is poor for analytical scans, where columnar formats win. Avro also appears where you might not expect it: Iceberg's manifest files are Avro, and BigQuery's Storage Read API can return Avro.

**Schemas** are JSON documents:

```json
{
  "type": "record", "name": "Order", "namespace": "com.acme",
  "fields": [
    {"name": "id",    "type": "long"},
    {"name": "name",  "type": "string"},
    {"name": "delta", "type": ["null", "long"], "default": null}
  ]
}
```

Primitive types are `null`, `boolean`, `int`, `long`, `float`, `double`, `bytes` and `string`; complex types are `record`, `enum`, `array`, `map`, `union` and `fixed`. **Logical types** add meaning: `date`, `timestamp-millis`, `timestamp-micros`, `decimal`, `uuid`. An optional field is a union with `null`.

**Binary encoding.** Avro writes **no field names and no tags**: fields are written in schema order, which is why a reader must have the writer's schema. Integers and longs use **zigzag** encoding (so small negative numbers are small too) followed by a **variable-length** encoding of 7 bits per byte. Strings and bytes are a length followed by the bytes. A union is the index of the chosen branch followed by the value. This standard-library code encodes one record of the schema above:

```python
def zigzag(n):            # small negative and positive numbers both get small codes
    return (n << 1) ^ (n >> 63)

def varint(n):            # 7 bits per byte, high bit = "more bytes follow"
    out = bytearray()
    while True:
        b, n = n & 0x7F, n >> 7
        out.append(b | 0x80 if n else b)
        if not n:
            return bytes(out)

def long_(n):   return varint(zigzag(n))
def string(s):  b = s.encode(); return long_(len(b)) + b
def union(branch, payload): return long_(branch) + payload

# schema: {"id": long, "name": string, "delta": ["null", "long"]}
record = long_(42) + string("Ada") + union(1, long_(-3))
print(record.hex(" "))    # 54 06 41 64 61 02 05
print(len(record), "bytes")   # 7 bytes
print(long_(300).hex(" "))    # d8 04
```

Seven bytes for a record whose JSON form is about 35. Read the bytes: `54` is 42 zigzagged (84); `06` is the length 3; `41 64 61` is "Ada"; `02` selects union branch 1 (`long`); `05` is -3 zigzagged.

**Object container files** (`.avro`) store many records: a header with the magic `Obj` plus version byte 1, a metadata map containing `avro.schema` (the full writer schema) and `avro.codec` (`null`, `deflate`, `snappy`, `zstandard` and others), and a random 16-byte **sync marker**. Then come **blocks**, each holding a record count, a byte size, the compressed records and the sync marker again. Sync markers let a reader start in the middle of a file and find the next block, so big Avro files can be split across workers.

**Schema evolution.** Avro reads data with two schemas: the **writer's schema** (how it was written) and the **reader's schema** (what the application expects). Fields are matched by name. A field present in the reader but not the writer takes its **default** (so new fields must have one); a field present in the writer but not the reader is skipped; some types may be promoted (`int` to `long`, `float` or `double`); **aliases** allow renames.

**Schema registry.** On Kafka, putting the whole schema in each message would waste space, so producers register the schema in a **schema registry** and send a small header instead. In the widely used Confluent wire format, each message starts with a magic byte `0`, then a 4-byte schema id, then the Avro binary. The registry enforces a **compatibility mode** per subject when a new schema version is registered: `BACKWARD` (the default: consumers using the new schema can read data written with the previous one), `FORWARD`, `FULL`, their `_TRANSITIVE` variants (checked against all earlier versions, not only the last) or `NONE`.

| Change | Backward compatible? | Forward compatible? |
|---|---|---|
| Add a field with a default | Yes | Yes |
| Add a field without a default | No | Yes |
| Remove a field that had a default | Yes | Yes |
| Rename a field (with an alias in the reader) | Yes | No |
| Change `int` to `long` | Yes | No |

On Google Cloud, **Pub/Sub schemas** validate messages against an Avro or Protocol Buffers schema with numbered revisions, and BigQuery loads Avro files directly (use `--use_avro_logical_types` so `timestamp-micros` becomes `TIMESTAMP` rather than `INT64`). Datastream can write CDC output to Cloud Storage as Avro.

> **Pitfall:** Adding a required field without a default breaks every consumer that reads old data with the new schema. Give new fields defaults (usually by making them a union with `null` and default `null`), and let the registry's compatibility check reject unsafe changes in CI.

::: deeper
**Avro versus Protocol Buffers and JSON.** Protobuf writes a numeric tag with every field, so it can decode without the writer's schema and tolerates unknown fields; Avro omits tags, so it is a little smaller but always needs the writer's schema. JSON needs no schema at all but is several times larger and has weak types (no int versus long, no decimal, no bytes).

**Canonical form and fingerprints.** The spec defines a **Parsing Canonical Form** of a schema (whitespace, docs and defaults removed) and fingerprints over it, such as the 64-bit Rabin fingerprint. **Single-object encoding** prefixes a record with the two bytes `C3 01` and the 8-byte fingerprint, a registry-free way to identify a schema.

**Sort order.** Avro defines a binary sort order, so encoded records can be compared without decoding, which Hadoop MapReduce used for shuffles.

**Why Iceberg uses Avro for manifests.** Manifests are written once, read whole during planning and evolve over format versions; Avro's compact rows and schema resolution suit that well.

> **Interview:** "A producer added a field and consumers crashed. What happened, and how do you prevent it?" The field had no default and consumers read with a schema that required it, or the change was not backward compatible for the consumer's direction. Prevent it with a registry compatibility mode enforced on registration and schema checks in CI, plus defaults on every new field.
:::

::: check
?? Why must an Avro reader have the writer's schema?
The binary encoding contains no field names or tags, only values in schema order, so without the writer's schema the bytes cannot be interpreted.

?? In the example, why is -3 written as the single byte `05`?
Zigzag encoding maps -3 to 5 (`(-3 << 1) ^ (-3 >> 63)`), turning small negative numbers into small positive codes, and 5 fits in one varint byte.

?? What does `BACKWARD` compatibility in a schema registry guarantee?
That consumers using the new schema can read data written with the previous schema, so you upgrade consumers first and producers after.
:::

::: resources
- Spec: [Apache Avro specification](https://avro.apache.org/docs/current/specification/) · types, binary encoding, container files and schema resolution rules
- Docs: [Pub/Sub schemas](https://cloud.google.com/pubsub/docs/schemas) · Avro and Protocol Buffers validation and schema revisions on Google Cloud
- Docs: [Loading Avro data into BigQuery](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro) · logical type mapping and load options
- Docs: [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) · the registry compatibility modes with examples
- Blog: [Schema evolution in Avro, Protocol Buffers and Thrift](https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html) · Martin Kleppmann's byte-by-byte comparison of the three
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · chapter 4 on encoding and evolution puts Avro in context
:::

### Apache Arrow — In-Memory Format

::: plain
Parquet is how data is packed for the warehouse shelf: squeezed, labelled and compact. **Arrow** is how data is laid out on the workbench while you work on it: unpacked, in neat columns, ready to use. Because every Arrow-speaking tool lays out the workbench the same way, one tool can hand its work to another without unpacking and repacking, which is often the slowest part of a data pipeline.
:::

**Apache Arrow** is a specification for **columnar data in memory**, plus libraries in many languages (C++, Rust, Java, Go, Python, JavaScript and more) that implement it. It is not a file format first: it is a shared layout that engines use while computing, and a wire format for moving that data between processes. pandas 2, Polars, DuckDB, Spark (for Python UDFs and `toPandas`), BigQuery's Storage Read API, Snowflake, Dremio and many others produce or consume Arrow.

**The layout.** An Arrow **array** (one column) is a few contiguous memory buffers:

- A **validity bitmap**: one bit per value, 1 if present, 0 if null (it can be omitted when there are no nulls).
- For fixed-width types (integers, floats, dates), a **values buffer** of the raw values side by side.
- For variable-width types (strings, binary), an **offsets buffer** (where each value starts and ends) and a **data buffer** with all bytes concatenated.

```python
names = ["ana", None, "bo", "cyd"]

validity = 0
for i, v in enumerate(names):
    if v is not None:
        validity |= 1 << i                # bit i set means row i is not null
print(f"{validity:08b}")                  # 00001101

offsets, data = [0], b""
for v in names:
    data += (v or "").encode()
    offsets.append(len(data))
print(offsets)                            # [0, 3, 3, 5, 8]
print(data)                               # b'anabocyd'

row = 3
print(data[offsets[row]:offsets[row + 1]].decode())   # cyd
```

That is exactly how Arrow's `utf8` type stores the column (with 32-bit offsets; `large_utf8` uses 64-bit). Finding row *i* is two lookups, there is no per-value object or pointer, and a loop over a numeric buffer runs at the speed of the CPU's vector instructions. Buffers are recommended to be aligned and padded to 64 bytes for exactly that reason.

A **record batch** is a set of equal-length arrays plus a **schema**; a **table** is a sequence of record batches. Nested types (`list`, `struct`, `map`) are built from child arrays, and `dictionary` arrays store integer ids plus a dictionary, like Parquet's dictionary pages but decoded and ready to compute on.

**Zero-copy.** Because the layout is fully specified, two libraries in the same process can share Arrow buffers by passing pointers through the **C Data Interface**, with no copying or conversion. That is how DuckDB can query a Polars or pandas DataFrame directly, and how PyArrow hands data to Spark's Python workers efficiently.

```python
import pyarrow as pa
import pyarrow.compute as pc

t = pa.table({"country": ["DE", "FR", "DE"], "amount": [10, 20, 30]})
print(pc.sum(pc.filter(t["amount"], pc.equal(t["country"], "DE"))))   # 40
```

**Moving Arrow between processes.**

- **Arrow IPC**: a serialisation of record batches with flatbuffer metadata, in a **streaming** form (for sockets and pipes) and a **file** form with a footer for random access (also known as Feather version 2). An IPC file can be memory-mapped and used without parsing.
- **Arrow Flight**: an RPC framework on gRPC for sending streams of record batches between services (`GetFlightInfo`, `DoGet`, `DoPut`, `DoExchange`), with parallel streams from many endpoints. **Flight SQL** builds a database protocol on top: send SQL, get Arrow back.
- **ADBC** (Arrow Database Connectivity): a client API standard, like JDBC or ODBC, but returning Arrow. Drivers exist for PostgreSQL, SQLite, DuckDB, Flight SQL, Snowflake and BigQuery, among others. With a row-based driver, the client must convert every row; with ADBC, columnar engines hand over columns directly.

On Google Cloud, the **BigQuery Storage Read API** streams table data to clients as Arrow (or Avro) record batches over parallel streams, which is what Spark, pandas (`to_dataframe`) and other tools use for fast reads from BigQuery.

> **Pitfall:** Arrow is not a storage format replacement for Parquet. IPC files are lightly compressed at best and have no statistics for skipping. Store in Parquet; compute and exchange in Arrow.

> **Tip:** When moving more than a few thousand rows between a database and Python, prefer an Arrow path (ADBC, the Storage Read API, DuckDB's `.arrow()` or `.pl()`) over row-by-row drivers. The speed difference is often ten times or more.

::: deeper
**String views.** Arrow added `utf8_view` and `binary_view` (2023): each value is a 16-byte view holding its length and either the whole string (if 12 bytes or fewer) or a 4-byte prefix plus a pointer into a data buffer. Short strings need no indirection, comparisons can often be decided on the prefix, and slicing or filtering does not copy bytes. The design comes from the Umbra database and is used by Polars and DataFusion.

**Run-end encoding** (added 2023) stores runs of equal values compactly in memory, so engines can process "the same value for 10,000 rows" without expanding it.

**Arrow-native engines.** Apache DataFusion (a Rust query engine), Polars, Velox (Meta's C++ execution library) and DuckDB (which uses its own vector format but converts cheaply) all process data in Arrow-like columnar batches. Spark's Python integration (pandas UDFs, `applyInPandas`, Arrow-optimised UDFs) uses Arrow to move batches between the JVM and Python workers.

**Versioning.** Arrow separates the **format version** (stable since 1.0 in 2020, with backward-compatible additions) from **library versions**, which release frequently.

> **Interview:** "Why is Arrow fast?" Contiguous, aligned columnar buffers allow vectorised CPU instructions and cache-friendly scans; a standard layout means zero-copy sharing between libraries and processes; and Flight and ADBC remove the row-by-row serialisation that dominates traditional database clients.
:::

::: check
?? What three buffers make up an Arrow string array?
A validity bitmap (which rows are not null), an offsets buffer (where each value starts and ends) and a data buffer with all the string bytes concatenated.

?? What does "zero-copy" mean between DuckDB and Polars?
Both understand the same memory layout, so one can pass pointers to its Arrow buffers to the other through the C Data Interface instead of serialising and copying the data.

?? Why does BigQuery's Storage Read API return Arrow?
So clients receive columnar batches they can use directly, in parallel streams, without converting each row, which makes bulk reads into Spark or pandas much faster.
:::

::: resources
- Spec: [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html) · the exact buffer layouts of every type
- Docs: [Apache Arrow documentation](https://arrow.apache.org/docs/) · libraries, IPC, Flight and the C Data Interface
- Docs: [ADBC: Arrow Database Connectivity](https://arrow.apache.org/adbc/) · the columnar client API and its drivers
- Blog: [Introducing Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) · why Flight exists and how it moves data in parallel
- Docs: [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage) · parallel streams of Arrow or Avro from BigQuery tables
- Book: *In-Memory Analytics with Apache Arrow* by Matt Topol · a practical guide to Arrow, Flight and ADBC
:::

### Lance and AI-native columnar formats

::: plain
Parquet was built for questions like "add up every sale in March": read a few columns from millions of rows. Machine learning often asks the opposite: "give me these 512 random rows, with their images and embeddings, right now", again and again while a model trains. That is like asking a librarian for single pages scattered across hundreds of books. **AI-native formats** such as Lance are organised so that fetching scattered rows, large blobs and vectors is fast, while keeping the columnar benefits for scans.
:::

**Why Parquet struggles with ML workloads.**

- **Random access.** To read one row, a Parquet reader must fetch and decode at least a whole page of each needed column (often a whole column chunk if there is no page index). Random sampling during training, or fetching the rows returned by a vector search, therefore reads far more than it uses.
- **Large values.** Images, audio and long documents make pages huge and row groups awkward to size.
- **Very wide tables.** Feature tables with thousands of columns make Parquet's Thrift footer slow to decode on every file open.
- **Vectors.** Embeddings are fixed-size lists of floats; Parquet stores them but has no notion of vector indexes.

**Lance.** **Lance** is an open-source columnar format from the LanceDB project, and also a lightweight table format on top of it. Its main ideas:

- **No row groups.** A Lance file stores each column as its own sequence of pages, and its newer file versions (2.x) choose a **structural encoding** per column so that a single value can be located and decoded with about one small read, while scans remain fast.
- **Datasets with versions.** A Lance **dataset** is a folder of data files plus **manifests**; every write creates a new version, readers can time-travel by version number, and appends or added columns do not rewrite existing data.
- **Indexes inside the dataset.** Vector indexes (IVF with product quantisation, HNSW variants) and scalar indexes (B-tree, bitmap, full-text) are stored with the data, so similarity search, filtering and retrieval happen in one system.
- **Arrow in, Arrow out.** Lance reads and writes Arrow, so it plugs into PyArrow, pandas, Polars, DuckDB, Ray, PyTorch and Spark.

```python
import lance                      # pip install pylance
import pyarrow as pa

tbl = pa.table({
    "id": [1, 2, 3],
    "caption": ["a cat", "a dog", "a red car"],
    "uri": ["gs://acme-ml/img/1.jpg", "gs://acme-ml/img/2.jpg", "gs://acme-ml/img/3.jpg"],
})
ds = lance.write_dataset(tbl, "gs://acme-ml/captions.lance")     # version 1
print(ds.take([0, 2]).to_pydict()["caption"])     # ['a cat', 'a red car']

more = pa.table({"id": [4], "caption": ["a bus"], "uri": ["gs://acme-ml/img/4.jpg"]})
ds = lance.write_dataset(more, "gs://acme-ml/captions.lance", mode="append")   # version 2
old = lance.dataset("gs://acme-ml/captions.lance", version=1)                  # time travel
print(old.count_rows(), ds.count_rows())          # 3 4
```

On a realistic dataset you would add an embedding column and build an index with `ds.create_index("vector", index_type="IVF_PQ", ...)`, then search with `ds.to_table(nearest={"column": "vector", "q": query_vector, "k": 10})`. **LanceDB** is a vector database built on the format, embedded (in-process) or as a service.

**Vortex.** **Vortex** is an extensible columnar file format created by SpiralDB and hosted by the LF AI & Data Foundation since 2025. It treats encodings as plug-ins and cascades them (for example dictionary, then bit-packing), using modern lightweight encodings such as FastLanes bit-packing, FSST for strings and ALP for floating-point numbers. Many operations can run on compressed data, and random access is designed in. The project promises file format backward compatibility from release 0.36, and DuckDB gained a Vortex extension in 2026. Its speed claims come mostly from its authors; independent benchmarks show it competitive with Parquet, faster on some workloads and not others.

**Nimble.** **Nimble** is a columnar format from Meta (in the `facebookincubator` organisation on GitHub), designed for very wide ML feature tables. It uses flatbuffers for metadata so opening files with thousands of streams is cheap, makes encodings pluggable and nestable, and is intended as a successor to Meta's DWRF. Treat it as an emerging format: watch it, but do not build on it unless your engines support it.

| Need | Reasonable choice |
|---|---|
| General analytics, any engine, BigQuery | Parquet (inside Iceberg or another table format) |
| ML training data with random sampling, multimodal rows, vector search | Lance |
| Evaluating next-generation columnar storage | Vortex, Nimble (check engine support first) |
| Moving data between processes | Arrow IPC or Flight |

> **Pitfall:** BigQuery, most Spark deployments and most BI tools do not read Lance or Vortex. If a dataset must also feed SQL analytics, keep the governed copy in Parquet and Iceberg, and produce the Lance dataset from it for training and retrieval.

::: deeper
**Where Parquet is heading.** The Parquet community is responding with page indexes, better encodings, and proposals for a faster footer. The formats above are partly experiments that may feed back into Parquet; expect convergence rather than a sudden switch.

**Random access versus compression.** Heavy compression (large blocks, general-purpose codecs) makes random access expensive, because the reader must decompress a whole block for one value. Lance and Vortex prefer lightweight, type-aware encodings that can be decoded selectively, accepting slightly larger files for far cheaper point lookups.

**Lance and table formats.** Lance's versioned manifests make it a small table format of its own. It does not yet have the multi-engine catalog ecosystem of Iceberg, so organisations usually treat Lance datasets as derived, rebuildable artefacts for ML rather than as the system of record.

**On Google Cloud.** Lance datasets live happily in Cloud Storage (`gs://` paths), and training jobs on Gemini Enterprise Agent Platform (formerly Vertex AI), GKE or Compute Engine can read them. Anywhere Cache in the training zone reduces repeated reads during many epochs.

> **Interview:** "Why not just use Parquet for an embeddings table?" You can for storage and batch scans, but random retrieval of rows returned by a vector search reads whole pages, and there is no built-in vector index. Lance gives cheap point reads plus indexes; the trade-off is a smaller ecosystem.
:::

::: check
?? Why is random row access slow in Parquet?
Values are encoded and compressed per page (and laid out per column chunk), so fetching one row means reading and decoding at least one whole page for each needed column.

?? Name two features of a Lance dataset beyond the file format.
Versioned manifests with time travel and cheap appends, and built-in vector and scalar indexes stored with the data.

?? When would you keep a Parquet copy of data that also lives in Lance?
When SQL engines such as BigQuery, Spark or BI tools must query it, since they do not read Lance; the Parquet and Iceberg copy is usually the governed system of record.
:::

::: resources
- Docs: [Lance documentation](https://lance.org/) · the file and table format, its Python API and indexes
- Repo: [lancedb/lance](https://github.com/lancedb/lance) · source, format specification and benchmarks
- Docs: [LanceDB documentation](https://lancedb.github.io/lancedb/) · the vector database built on the Lance format
- Repo: [vortex-data/vortex](https://github.com/vortex-data/vortex) · the Vortex format, its encodings and compatibility policy
- Blog: [Announcing Vortex support in DuckDB](https://duckdb.org/2026/01/23/duckdb-vortex-extension.html) · using Vortex from DuckDB, with an independent benchmark
- Repo: [facebookincubator/nimble](https://github.com/facebookincubator/nimble) · Meta's columnar format for wide ML tables
:::

## Open table formats

Files alone do not make a table. This group covers the **open table formats**: the metadata layers that turn a folder of Parquet files into a table with atomic commits, snapshots, schema evolution, row-level changes and time travel. They all solve the same problem under the same constraint (object storage files are immutable, and listing is slow), but they make different design choices, and those choices decide what each is good at.

You will meet the three big formats (Iceberg, Delta Lake, Hudi) in depth, then Hudi's 1.0 generation, Paimon (built for streaming), and DuckLake (which keeps metadata in a SQL database instead of files). The group ends with an honest comparison and with XTable, which translates one format's metadata into another's so that one copy of data can be read as all three.

### Apache Iceberg — Internals & Operations

::: plain
Think of an Iceberg table as a photo album of a warehouse shelf. Every time anything changes, a new photo (a **snapshot**) is taken showing exactly which boxes (data files) are on the shelf. Nobody moves boxes around in place; you add new boxes and take a new photo. The album's index page (the catalog pointer) always points at the latest photo, so readers see either the old shelf or the new one, never a half-stocked shelf, and you can always flip back to an older photo.
:::

**Apache Iceberg** was created at Netflix (2017) to fix the Hive table problems described in the catalog topic, and became an Apache top-level project in 2020. It is now the most widely adopted open table format across vendors, and the one Google Cloud supports most deeply (Apache Iceberg managed tables, the Lakehouse runtime catalog, BigQuery and Spark interoperability).

[[fig:lakehouse_iceberg]]

**The metadata tree.** Reading from the top:

1. The **catalog** holds one pointer per table: the location of the current **table metadata file** (`v43.metadata.json` or a UUID-named file).
2. The **table metadata file** (JSON) holds the table's schemas (each column has a permanent numeric **field id**), partition specs, sort orders, properties, the list of **snapshots**, the current snapshot id and named **refs** (branches and tags).
3. Each **snapshot** points to one **manifest list** (an Avro file) with one entry per manifest, including partition value ranges for that manifest so planners can skip whole manifests.
4. Each **manifest** (Avro) lists data files or delete files with their partition values, record counts, file sizes and **column statistics**: value counts, null counts, and lower and upper bounds per column.
5. **Data files** are Parquet (or ORC or Avro) files that are never modified after being written.

**Writing a commit.** A writer (for example a Spark job inserting rows) reads the current metadata, writes new data files, writes a new manifest listing them, writes a new manifest list that references the new manifest plus the unchanged existing ones, writes a new metadata file, and finally asks the catalog to swap the pointer from the old metadata file to the new one, **only if it has not changed** in the meantime. If another writer won the race, the loser re-reads the new state, checks whether the two changes conflict (two appends never conflict; a delete and an overwrite of the same files do), and retries. This is **optimistic concurrency control**: no locks while working, one atomic check at the end.

**Scan planning** works top-down and prunes at every level. This toy version shows the file-level step using manifest statistics:

```python
# one manifest: a data file per row, with per-column lower and upper bounds
manifest = [
    {"file": "a.parquet", "order_date": ("2026-01-01", "2026-01-09"), "amount": (5, 900)},
    {"file": "b.parquet", "order_date": ("2026-01-10", "2026-01-19"), "amount": (1, 40)},
    {"file": "c.parquet", "order_date": ("2026-01-20", "2026-01-31"), "amount": (3, 7000)},
]

def may_match(entry, col, lo, hi):          # can [min, max] overlap [lo, hi]?
    mn, mx = entry[col]
    return mx >= lo and mn <= hi

# WHERE order_date BETWEEN '2026-01-15' AND '2026-01-25' AND amount > 1000
plan = [e["file"] for e in manifest
        if may_match(e, "order_date", "2026-01-15", "2026-01-25")
        and may_match(e, "amount", 1001, float("inf"))]
print(plan)   # ['c.parquet']
```

No file was listed or opened to decide this; planning used only metadata.

**Hidden partitioning.** In Hive you partition by a column you must create and filter on explicitly (`dt` as a string derived from a timestamp). Iceberg partitions by a **transform** of a real column: `identity`, `year`, `month`, `day`, `hour`, `bucket[N]` (hash into N buckets) and `truncate[W]`. Users filter on the real column, and Iceberg applies the transform for pruning, so nobody has to know the partition layout.

```sql
-- Spark SQL with an Iceberg catalog named lake
CREATE TABLE lake.sales.orders (
  order_id BIGINT, customer_id BIGINT, order_ts TIMESTAMP,
  amount DECIMAL(12, 2), country STRING
) USING iceberg
PARTITIONED BY (days(order_ts), bucket(16, customer_id));

-- pruned to one day's partitions, although the query never mentions a partition column
SELECT country, SUM(amount) FROM lake.sales.orders
WHERE order_ts >= TIMESTAMP '2026-11-21 00:00:00'
  AND order_ts <  TIMESTAMP '2026-11-22 00:00:00'
GROUP BY country;
```

**Evolution without rewrites.** Because columns are tracked by field id, not by name or position, you can add, drop, rename and reorder columns and widen types (`int` to `long`, `float` to `double`, wider decimals) as metadata-only changes. **Partition evolution** changes the spec for new data only; old files keep their old spec and planning handles both.

```sql
ALTER TABLE lake.sales.orders ADD COLUMN channel STRING;
ALTER TABLE lake.sales.orders RENAME COLUMN channel TO sales_channel;
ALTER TABLE lake.sales.orders REPLACE PARTITION FIELD order_ts_day WITH hours(order_ts);
```

**Time travel and refs.**

```sql
SELECT snapshot_id, committed_at, operation FROM lake.sales.orders.snapshots;
SELECT * FROM lake.sales.orders TIMESTAMP AS OF '2026-11-20 00:00:00';
SELECT * FROM lake.sales.orders VERSION AS OF 'audit-2026-11';   -- a tag or branch name
CALL lake.system.rollback_to_snapshot('sales.orders', 8744736658442914487);
ALTER TABLE lake.sales.orders CREATE TAG `audit-2026-11` RETAIN 365 DAYS;
```

Metadata tables (`snapshots`, `history`, `files`, `manifests`, `partitions`, `refs`) let you inspect any table with SQL.

**Row-level changes.** Iceberg offers two strategies per operation (`write.delete.mode`, `write.update.mode`, `write.merge.mode`): **copy-on-write** rewrites every data file that contains a changed row (slow writes, fastest reads), and **merge-on-read** writes small **delete files** that readers apply (fast writes, slightly slower reads until compaction). Format version 2 has **position deletes** (file path plus row position) and **equality deletes** (delete every row where `id = 7`, used by Flink streaming upserts). Format version 3 adds **deletion vectors**: one compact bitmap per data file, stored in a Puffin file, replacing position delete files for new writes.

> **Pitfall:** An Iceberg table without maintenance degrades: snapshots, manifests and small files accumulate, and merge-on-read delete files pile up. Schedule `rewrite_data_files`, `rewrite_position_delete_files` where needed, `rewrite_manifests`, `expire_snapshots` and `remove_orphan_files` (the Operating a lakehouse group shows how), or use a managed service that does it for you.

> **Tip:** Iceberg managed tables in BigQuery run compaction, reclustering and garbage collection automatically. If BigQuery is the main writer, that removes most maintenance work.

::: deeper
**Format versions.** Version 1 covered analytic tables with immutable files. Version 2 added row-level deletes and stricter metadata. Version 3 (finalised in 2025) adds deletion vectors, **row lineage** (each row gets a stable id and a last-updated sequence number, enabling efficient change tracking), column **default values**, new types (`variant` for semi-structured data, `geometry` and `geography`, nanosecond timestamps, and `unknown`), and multi-argument transforms. Engines adopt v3 at different speeds; check before upgrading a shared table, because readers that do not understand v3 cannot read it.

**Sequence numbers.** Every snapshot has a **sequence number**; delete files apply only to data files with a lower sequence number. That is how a delete written at sequence 10 does not wrongly hide a row re-inserted at sequence 11.

**Puffin files** hold blobs that do not fit in manifests: `apache-datasketches-theta-v1` sketches for the number of distinct values (used by cost-based optimisers in Trino and Spark), and in v3 the deletion vectors.

**Isolation.** Spark operations on Iceberg default to **serializable** isolation for deletes, updates and merges, and you can relax them to **snapshot** isolation (`write.merge.isolation-level`), which allows more concurrency with weaker guarantees. Concurrent appends from many writers are always safe; concurrent `MERGE`s on overlapping files will retry and can fail after `commit.retry.num-retries`.

**Write-audit-publish.** Write new data to a branch (`spark.wap.branch`), run quality checks on that branch, then fast-forward `main` to it. Readers of `main` never see unvalidated data.

> **Interview:** "How does Iceberg guarantee readers never see a partial write?" Writers produce new immutable files and a new metadata tree, and the only visible change is an atomic compare-and-swap of the table's metadata pointer in the catalog. Readers resolve the pointer once and read a fixed snapshot.
:::

::: check
?? List the levels of Iceberg's metadata tree from the catalog down.
Catalog pointer, table metadata file, snapshot, manifest list, manifests, then data and delete files.

?? What makes hidden partitioning "hidden"?
Partitions are defined as transforms of real columns (such as `days(order_ts)`), so users filter on the real column and Iceberg derives partition pruning; there is no separate partition column to know about or filter on.

?? Why can Iceberg rename a column without rewriting data?
Columns are identified by permanent field ids stored in the schema and in the data files' metadata, not by name, so a rename only changes the name attached to the id in the table metadata.
:::

::: resources
- Spec: [Apache Iceberg table spec](https://iceberg.apache.org/spec/) · the metadata tree, versions 1 to 3, delete files and commit rules
- Docs: [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/) · Spark DDL, writes, procedures, metadata tables and configuration
- Book: [Apache Iceberg: The Definitive Guide](https://www.oreilly.com/library/view/apache-iceberg-the/9781098148614/) · internals, catalogs and operations engine by engine
- Docs: [Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/iceberg-tables) · BigQuery-managed Iceberg with automatic optimisation
- Spec: [Puffin file format](https://iceberg.apache.org/puffin-spec/) · statistics and deletion vector blobs
- Docs: [PyIceberg](https://py.iceberg.apache.org/) · a Python way to explore snapshots and manifests yourself
:::

### Delta Lake — Internals & Operations

::: plain
A Delta table keeps a numbered diary next to its data. Every change is one new diary page: "added these files, removed those files". To know what the table contains, you read the diary from page one, or jump to the latest summary page and read only the pages after it. Two writers who try to write the same page number collide, and only one wins; the other must re-read and try again.
:::

**Delta Lake** was created at Databricks (open-sourced in 2019, now a Linux Foundation project) and is the default table format on Databricks. It runs on Spark anywhere, and also has Rust (`delta-rs`, with the Python `deltalake` package), Flink, Trino and DuckDB implementations, plus **Delta Kernel** libraries for engine builders.

[[fig:lakehouse_delta]]

**The transaction log.** A Delta table is a folder of Parquet data files plus a `_delta_log/` folder. Each commit is a JSON file named by its zero-padded version: `00000000000000000000.json`, `00000000000000000001.json` and so on. Each line is an **action**:

| Action | Meaning |
|---|---|
| `protocol` | Minimum reader and writer versions and the **table features** a client must support |
| `metaData` | Schema, partition columns, table properties |
| `add` | A data file joins the table, with its size, partition values and statistics (row count, min, max, null counts) |
| `remove` | A data file leaves the table (it stays on storage until `VACUUM`) |
| `txn` | An application id and version, so streaming writers can commit exactly once |
| `commitInfo` | Who did what: operation, parameters, metrics |
| `cdc` | Change data files, when change data feed is enabled |

The table at version N is the result of **replaying** commits 0 to N: the set of files added and not later removed. This standard-library sketch replays a small log:

```python
import json

# _delta_log/ as a dict: file name -> newline-delimited JSON actions
delta_log = {
    "00000000000000000000.json":
        '{"add": {"path": "part-000.parquet", "size": 1000}}\n'
        '{"add": {"path": "part-001.parquet", "size": 1200}}',
    "00000000000000000001.json":
        '{"add": {"path": "part-002.parquet", "size": 900}}',
    "00000000000000000002.json":                  # OPTIMIZE: 2 small files -> 1
        '{"remove": {"path": "part-000.parquet"}}\n'
        '{"remove": {"path": "part-001.parquet"}}\n'
        '{"add": {"path": "part-003.parquet", "size": 2100}}',
}

def files_at(version):
    live = set()
    for name in sorted(delta_log):
        if int(name[:20]) > version:
            break
        for line in delta_log[name].splitlines():
            action = json.loads(line)
            if "add" in action:
                live.add(action["add"]["path"])
            elif "remove" in action:
                live.discard(action["remove"]["path"])
    return sorted(live)

print(files_at(1))   # ['part-000.parquet', 'part-001.parquet', 'part-002.parquet']
print(files_at(2))   # ['part-002.parquet', 'part-003.parquet']
```

**Checkpoints.** Replaying thousands of JSON files would be slow, so every 10 commits by default (`delta.checkpointInterval`) a writer also writes a **checkpoint**: a Parquet file with the full state at that version (`00000000000000000010.checkpoint.parquet`), and updates `_delta_log/_last_checkpoint`. A reader loads the latest checkpoint and replays only the JSON commits after it.

**Concurrency.** To commit version 43, a writer must create `00000000000000000043.json`, and the create must fail if the file already exists. On Cloud Storage this is a conditional write with `ifGenerationMatch=0`, so concurrent writers from several clusters are safe. The loser checks whether the winning commit conflicts with what it read (for example, did the winner remove a file the loser also rewrote?) and either retries at version 44 or fails with a concurrent-modification error.

**Core operations.**

```sql
-- Spark SQL with Delta Lake
CREATE TABLE sales.orders (order_id BIGINT, customer_id BIGINT, order_ts TIMESTAMP, amount DECIMAL(12,2))
USING delta
CLUSTER BY (customer_id);                          -- liquid clustering

MERGE INTO sales.orders t USING updates s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;

OPTIMIZE sales.orders;                             -- compact small files (and cluster)
DESCRIBE HISTORY sales.orders;                     -- commits with operation metrics
SELECT * FROM sales.orders VERSION AS OF 42;
SELECT * FROM sales.orders TIMESTAMP AS OF '2026-11-20';
RESTORE TABLE sales.orders TO VERSION AS OF 42;
VACUUM sales.orders RETAIN 168 HOURS;              -- delete unreferenced files older than 7 days
```

- **OPTIMIZE** bin-packs small files into larger ones (about 1 GiB by default). With **Z-ORDER BY** it also co-locates rows with similar values in several columns, improving data skipping (see the topic Data Skipping, Z-Ordering & Liquid Clustering).
- **Liquid clustering** (`CLUSTER BY`) replaces partitioning and Z-order for most tables: you declare clustering columns, `OPTIMIZE` clusters incrementally, and you can change the columns later without rewriting the table.
- **Deletion vectors** (`delta.enableDeletionVectors`) make `DELETE`, `UPDATE` and `MERGE` mark rows as deleted in a small bitmap file instead of rewriting whole Parquet files, the merge-on-read idea. `REORG TABLE ... APPLY (PURGE)` rewrites files to drop the deleted rows physically.
- **Change data feed** (`delta.enableChangeDataFeed`) records row-level inserts, updates and deletes so downstream jobs can read only changes with `table_changes('sales.orders', 42)`.

**On Google Cloud.** Run Delta on Managed Service for Apache Spark (clusters or serverless batches) by adding the `delta-spark` package and the Delta SQL extension and catalog settings; BigQuery can read Delta tables as BigLake tables over Cloud Storage. If BigQuery must read the same data as Iceberg, enable **UniForm** (below) or use XTable.

> **Pitfall:** `VACUUM` with a short retention breaks time travel and can break running readers and streaming jobs that still need old files. Keep the default 7 days unless you understand every consumer; Delta refuses shorter retention unless you disable a safety check, and that check exists for a reason.

> **Tip:** Statistics are collected only for the first 32 columns by default (`delta.dataSkippingNumIndexedCols`). Put the columns you filter on early in the schema, or set `delta.dataSkippingStatsColumns` explicitly.

::: deeper
**Protocol versions and table features.** Older Delta used numeric reader and writer versions. Since writer version 7 and reader version 3, a table lists named **table features** (`deletionVectors`, `columnMapping`, `timestampNtz`, `v2Checkpoint`, `liquid clustering` and so on) and a client must support every listed feature to read or write. Enabling a feature can lock out older clients, so check every engine first.

**Column mapping.** Delta originally identified Parquet columns by name, so renames required rewrites. **Column mapping** mode `name` (or `id`) stores a stable physical name per column, enabling rename and drop as metadata operations, similar to Iceberg's field ids.

**UniForm.** With `delta.universalFormat.enabledFormats = 'iceberg'`, Delta writes Iceberg metadata asynchronously after each commit, pointing at the same Parquet files, so Iceberg readers (including BigQuery) can read the table. Writes still go through Delta, and some features (such as deletion vectors in older UniForm versions) restrict what can be enabled.

**Isolation levels.** `WriteSerializable` (the default) allows appends to proceed concurrently with other operations; `Serializable` is stricter. Most conflicts come from concurrent `MERGE`, `UPDATE` or `DELETE` touching the same files; partition or cluster the table so concurrent jobs touch disjoint files.

**Retention properties.** `delta.logRetentionDuration` (30 days by default) controls how long commit files are kept (and therefore how far back time travel can reach in the log); `delta.deletedFileRetentionDuration` (7 days) controls how long removed data files are protected from `VACUUM`. Time travel needs both the log and the files.

> **Interview:** "How does Delta implement ACID on object storage?" An ordered log of JSON commits with put-if-absent creation of each version file gives atomicity and a total order; readers replay from the latest Parquet checkpoint to get a consistent snapshot; optimistic conflict detection handles concurrent writers.
:::

::: check
?? How does a reader find the current state of a Delta table efficiently?
It reads `_last_checkpoint`, loads that Parquet checkpoint, and replays only the JSON commits written after it.

?? What prevents two writers from both committing version 43?
Creating the version file is a put-if-absent operation (a conditional create on Cloud Storage), so only one writer succeeds; the other detects the conflict and retries or fails.

?? What do deletion vectors change about a `DELETE` of 10 rows from a 1 GiB file?
Instead of rewriting the whole file without those rows, Delta writes a small bitmap marking the 10 rows as deleted; readers skip them, and a later `OPTIMIZE` or `REORG` rewrites the file.
:::

::: resources
- Docs: [Delta Lake documentation](https://docs.delta.io/latest/index.html) · operations, configuration, table features and integrations
- Spec: [Delta transaction log protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) · the exact actions, checkpoints and table features
- Paper: [Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores](https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf) · the VLDB 2020 design paper
- Book: *Delta Lake: The Definitive Guide* by Denny Lee, Tristen Wentling, Scott Haines and Prashanth Babu · internals and operations in depth
- Docs: [delta-rs and the deltalake Python package](https://delta-io.github.io/delta-rs/) · Delta without Spark, useful for small jobs and learning
- Docs: [Managed Service for Apache Spark overview](https://cloud.google.com/dataproc/docs/concepts/overview) · running Spark (and Delta) on Google Cloud
:::

### Apache Hudi — Internals & Operations

::: plain
Hudi was built for a very common job: keeping a copy of a busy database table in the lake, with millions of updates a day. It works like a bank ledger with a monthly statement. Updates are first written quickly as small ledger entries next to the last statement; readers combine the statement and the entries; and every so often the bank prints a new statement that folds the entries in. You choose whether to print a new statement on every update (simple reads, costly writes) or only now and then (cheap writes, a little more work for readers).
:::

**Apache Hudi** (Hadoop Upserts Deletes and Incrementals) was created at Uber in 2016 to ingest database changes into the lake with minutes of latency, and became an Apache top-level project in 2020. Its strengths are **upserts at scale**, **incremental processing** (read only what changed since your last run) and built-in **table services** that keep tables healthy.

**Keys and file groups.** Every Hudi record has a **record key** (like a primary key) and a **partition path**. Within a partition, records are assigned to **file groups**; a given key always lives in the same file group. A file group contains a series of **file slices**, one per version: each slice has a **base file** (Parquet) and, for merge-on-read tables, a set of **log files** with later changes. An **index** maps keys to file groups so an upsert knows which file group to update without scanning the table.

**Two table types.**

| | Copy-on-Write (COW) | Merge-on-Read (MOR) |
|---|---|---|
| On update | Rewrite the base file containing the record | Append the change to a log file of that file group |
| Write cost | High (write amplification) | Low |
| Read cost | Lowest: plain Parquet | Merge base and logs at read time, until compaction |
| Data freshness | Per commit | Per delta commit, often minutes |
| Best for | Read-heavy tables, modest update rates | Heavy upserts, CDC, streaming ingestion |

A worked example of write amplification: 1,000 updated rows spread across 200 base files of 128 MiB. COW rewrites all 200 files, about 25 GiB, to change 1,000 rows. MOR appends roughly a megabyte of log data and defers the rewrite to **compaction**, which can run later, in batch, for many commits at once.

**The timeline.** The `.hoodie/` folder holds the **timeline**: an ordered record of every action on the table. Each action is an **instant** with a time, an action type and a state (`requested`, `inflight`, `completed`). Action types include `commit` (COW write), `deltacommit` (MOR write), `compaction`, `clean`, `rollback`, `replacecommit` (clustering and overwrite) and `savepoint`. Readers only see completed instants, which gives atomic commits; failed writes are rolled back using the timeline.

**Writing with Spark.**

```python
(updates_df.write.format("hudi")
    .option("hoodie.table.name", "orders")
    .option("hoodie.datasource.write.recordkey.field", "order_id")
    .option("hoodie.datasource.write.partitionpath.field", "order_date")
    .option("hoodie.datasource.write.precombine.field", "updated_at")   # latest wins
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    .option("hoodie.datasource.write.operation", "upsert")
    .mode("append")
    .save("gs://acme-lake-eu/hudi/orders"))
```

The **precombine** (ordering) field decides which version wins when the same key appears more than once: the record with the highest `updated_at`, so late or replayed events cannot overwrite newer data.

**Three query types.**

- **Snapshot**: the latest state (for MOR, base files merged with logs).
- **Read-optimised** (MOR only): base files only, fast but as stale as the last compaction.
- **Incremental**: only records changed after a given instant, the feature that makes Hudi pipelines cheap.

```python
changes = (spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "incremental")
    .option("hoodie.datasource.read.begin.instanttime", "20261121000000")
    .load("gs://acme-lake-eu/hudi/orders"))
```

A downstream job stores the last instant it processed and asks only for later changes, so a silver-to-gold job touches megabytes instead of the whole table.

**Indexes.** Options include **Bloom** (Bloom filters in base file footers, plus key ranges), **simple** (join incoming keys with existing keys), **bucket** (hash keys into a fixed number of file groups, no lookup needed) and the **record-level index**, stored in Hudi's internal **metadata table**, which maps every key to its file group directly. The metadata table also stores file listings, column statistics and Bloom filters, so planning never lists storage.

**Table services.** **Compaction** merges MOR logs into new base files; **clustering** rewrites files to change layout (sort or co-locate data); **cleaning** removes old file slices beyond the retention you need for time travel and incremental readers; **archival** moves old timeline instants out of the active timeline. Each can run **inline** (inside the write job), **async** (in the same process, in the background) or as a **separate job**.

On Google Cloud, Managed Service for Apache Spark offers Hudi as an optional component, and Hudi's BigQuery sync tool can register Hudi tables so BigQuery can query them.

> **Pitfall:** Choosing the wrong precombine field (or none) lets out-of-order events overwrite newer data. Use a monotonically meaningful field such as the source's commit timestamp or log sequence number, not ingestion time.

> **Tip:** Start CDC tables as MOR with async compaction, and give downstream consumers incremental queries. That is the workload Hudi was designed for.

::: deeper
**Write path.** An upsert batch is deduplicated by key with the precombine field, tagged through the index (existing key: which file group; new key: insert), partitioned by file group, then written: COW rewrites affected base files, MOR appends log blocks. **Small file handling** routes inserts into existing file groups whose base file is below `hoodie.parquet.small.file.limit` (100 MiB by default), so inserts grow files towards `hoodie.parquet.max.file.size` (120 MiB) instead of creating new tiny ones.

**Global versus non-global indexes.** A non-global index guarantees key uniqueness within a partition only. If a record's partition value can change (for example, a customer moves country), you need a global index or the old copy remains in the old partition.

**Concurrency.** Hudi supports one writer with async table services by default. Multiple writers need **optimistic concurrency control** with a lock provider (ZooKeeper, Hive Metastore or a file-system based lock); Hudi 1.0 adds non-blocking concurrency control for MOR tables (next topic).

**Cleaning and incremental readers.** A downstream job reading incrementally from instant X needs the file slices written since X. If the cleaner removes them first (`hoodie.cleaner.commits.retained`, 10 by default), the job must fall back to a full snapshot read. Size retention to the slowest consumer's schedule.

> **Interview:** "Why would you choose Hudi over Iceberg or Delta?" For update-heavy CDC ingestion with key-based upserts, record-level indexing, mature incremental queries and built-in table services. Iceberg has the broader multi-engine and vendor support, so say how readers will access the data (for example BigQuery via sync, or XTable).
:::

::: check
?? What is the difference between COW and MOR tables?
COW rewrites base files on every update, so reads are plain Parquet but writes are expensive. MOR appends changes to log files and merges at read time until compaction, so writes are cheap and reads do a little extra work.

?? What is an incremental query used for?
Reading only records changed after a given instant, so downstream jobs process changes instead of rescanning the whole table.

?? Why does Hudi need an index?
To find which file group already contains each incoming record key during an upsert, without scanning the table.
:::

::: resources
- Docs: [Apache Hudi overview](https://hudi.apache.org/docs/overview) · concepts, table types, timeline, indexes and services
- Docs: [Table and query types](https://hudi.apache.org/docs/table_types) · COW versus MOR and snapshot, read-optimised and incremental queries
- Blog: [Apache Hudi: the streaming data lake platform](https://hudi.apache.org/blog/2021/07/21/streaming-data-lake-platform) · the design vision from Hudi's creator
- Docs: [Hudi on Managed Service for Apache Spark](https://cloud.google.com/dataproc/docs/concepts/components/hudi) · the Hudi optional component on Google Cloud
- Docs: [Google Cloud BigQuery sync](https://hudi.apache.org/docs/gcp_bigquery) · exposing Hudi tables to BigQuery
- Blog: [Apache Hudi blog](https://hudi.apache.org/blog/) · engineering posts on indexing, compaction and real deployments
:::

### Apache Hudi 1.0 — Next Gen Features

::: plain
Hudi 1.0 is a renovation rather than a new building. The ledger (the timeline) is reorganised so that years of history stay quick to search, new kinds of indexes act like the index at the back of a book for any column you choose, and several writers can now work on the same records at the same time without waiting for each other.
:::

Hudi 1.0 was released in December 2024 after a long design effort. **RFC-69** describes the overall 1.x design, mapping Hudi's components layer by layer onto a database architecture: storage format, timeline as the transaction log, indexes, concurrency control and table services. The release changes the table format version, so plan upgrades (below).

**A new timeline.** In earlier versions each action had one instant time (when it started). In 1.0 each action has a **requested time** and a **completion time**, generated with TrueTime-style semantics (monotonic, unique times across writers). Ordering by completion time is what makes non-blocking concurrency possible. The **LSM timeline** stores the full history as a log-structured merge tree of files instead of moving old instants to a slow archive, so reading long histories stays fast; the stated aim is effectively unbounded history for time travel and incremental reads.

**Indexes as a first-class feature.** Hudi's metadata table now hosts several index types, created and dropped with SQL in Spark:

- **Secondary indexes** on any column, used by both writers and readers, and built asynchronously without blocking writers. The release notes report large latency reductions for selective queries on a 10 TB TPC-DS table.
- **Expression indexes**: an index on an expression of a column, such as `from_unixtime(ts, 'yyyy-MM-dd')`, so queries that filter by date benefit even though the table stores epoch seconds.
- **Partition stats indexes**: aggregated column statistics per partition, letting the planner prune partitions on columns other than the partition key.
- **Bloom filter indexes** created through SQL and stored in the metadata table.

```sql
-- Spark SQL with Hudi 1.0
CREATE INDEX idx_city ON hudi_db.rides (city);
CREATE INDEX idx_ride_day ON hudi_db.rides
  USING column_stats (ts) OPTIONS (expr = 'from_unixtime', format = 'yyyy-MM-dd');
SHOW INDEXES FROM hudi_db.rides;
```

Treating partitions as a coarse index (via partition stats and expression indexes) means you can partition less aggressively, which reduces small files.

**Partial updates.** `MERGE INTO ... WHEN MATCHED THEN UPDATE SET col = ...` on a MOR table now writes only the changed columns into the log, not the whole record. For wide tables with frequent narrow updates, the release notes report much lower write amplification and faster queries.

**Merge modes.** How two versions of the same key combine is now an explicit table setting:

| Merge mode | Rule |
|---|---|
| `EVENT_TIME_ORDERING` (default) | Keep the record with the highest ordering field value (the old precombine idea) |
| `COMMIT_TIME_ORDERING` | Keep the record written by the latest commit, by completion time |
| `CUSTOM` | Use your own `RecordMerger` implementation |

**Non-blocking concurrency control (NBCC)**, designed in RFC-66, lets several writers, and compaction, write to the **same** file groups of a MOR table at once. Writers append log files and do not conflict; conflicts are resolved later by the merge mode when reading or compacting. A typical use: two Flink jobs each updating different columns of the same records. Classic optimistic concurrency would make one of them fail and retry.

**Partition TTL.** **RFC-65** designs partition time-to-live management: a table service that deletes partitions older than a policy (for example, drop `event_date` partitions older than 400 days). Check your exact Hudi version's configuration reference for the supported strategies and options before relying on it.

**Upgrading.** Hudi 1.0 writers can write tables that older readers still understand, allowing a rolling upgrade: upgrade writers first in a compatible mode, then readers, then switch the table to the new format version. The release notes list the steps, including temporarily disabling the metadata table during the upgrade.

> **Pitfall:** Many 1.0 index features were first available from Spark only, with Flink, Trino and Presto support planned for later releases. Check each engine's support before designing queries around a secondary or expression index.

::: deeper
**Why completion time matters.** With only start times, a long-running writer that started first but finished last would appear "older" than a short write that started later, so incremental readers could miss its data or concurrency checks would be pessimistic. Ordering by completion time, with unique monotonic timestamps, gives readers a correct order of visibility and lets compaction and writers run concurrently on the same file groups.

**File group reader.** 1.0 unifies how engines read a file slice (base file plus logs, with merge modes and partial updates) into one reader implementation, so behaviour is consistent across Spark, Flink and others as support lands.

**Catalog interoperability.** Hudi tables register in the Hive Metastore and sync to BigQuery and other catalogs; for Iceberg and Delta readers, Apache XTable can publish Iceberg or Delta metadata over Hudi's files.

> **Interview:** "Two streaming jobs must update different columns of the same table. How?" With Hudi 1.0, use a MOR table with non-blocking concurrency control and partial updates, so both append changes without conflicting and the merge combines columns. With formats using optimistic concurrency, you would usually merge the streams first and have one writer.
:::

::: check
?? Why does Hudi 1.0 record a completion time for each action?
So actions can be ordered by when they became visible, which gives incremental readers a correct order and enables non-blocking concurrency between writers and compaction.

?? What problem does an expression index solve?
Queries filtering on a function of a column (such as a date derived from an epoch timestamp) can use an index on that expression, so they prune data without the table being partitioned on it.

?? What do merge modes decide?
How two versions of the same record key are combined: by event-time ordering field, by latest commit, or by custom logic.
:::

::: resources
- Blog: [Announcing Apache Hudi 1.0](https://hudi.apache.org/blog/2024/12/16/announcing-hudi-1-0-0/) · the release post with every major feature and the upgrade path
- Docs: [Apache Hudi overview](https://hudi.apache.org/docs/overview) · current concepts and configuration for the 1.x line
- Docs: [Hudi indexes](https://hudi.apache.org/docs/indexes) · record, secondary, expression and Bloom indexes in the metadata table
- Docs: [Concurrency control](https://hudi.apache.org/docs/concurrency_control) · optimistic and non-blocking concurrency control
- Repo: [Hudi RFCs](https://github.com/apache/hudi/tree/master/rfc) · the design documents, including RFC-65, RFC-66 and RFC-69
:::

### Apache Paimon — Streaming Lakehouse Format

::: plain
Most table formats were designed for batch jobs that write big chunks now and then, and streaming was added later. Paimon was designed the other way round: for a never-ending stream of small updates. It works like a sorting office that keeps several small, sorted piles and merges them in the background, so new letters can be filed instantly and anyone can still find the latest letter for any address.
:::

**Apache Paimon** began as Flink Table Store inside the Apache Flink project, and became an Apache top-level project in 2024. It is a lake format built around a **log-structured merge tree (LSM tree)**, the same structure used by RocksDB and many databases for fast writes. It integrates most deeply with Flink, and is also readable and writable from Spark and readable from Trino, StarRocks, Doris and other engines.

**Primary-key tables.** A table with a primary key is split into **buckets** (by hashing the key). Each bucket is an LSM tree: new writes are sorted and flushed as small files at level 0, and background **compaction** merges files into larger sorted runs at higher levels. Readers merge the levels by key to get the latest values. Because every file within a bucket is sorted by key, merging is cheap, and an update never needs to find and rewrite an old file.

```sql
-- Flink SQL
CREATE CATALOG lake WITH ('type' = 'paimon', 'warehouse' = 'gs://acme-lake-eu/paimon');
USE CATALOG lake;

CREATE TABLE orders (
  order_id    BIGINT,
  customer_id BIGINT,
  status      STRING,
  amount      DECIMAL(12, 2),
  updated_at  TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '8',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup'
);

INSERT INTO orders SELECT order_id, customer_id, status, amount, updated_at FROM kafka_orders;
```

**Merge engines** decide what "latest" means when the same key arrives again:

| Merge engine | Result per key | Typical use |
|---|---|---|
| `deduplicate` (default) | The last record wins | CDC mirror of a database table |
| `partial-update` | Non-null fields from each record are merged | Building a wide table from several streams |
| `aggregation` | Fields are aggregated (`sum`, `max`, `last_non_null_value`...) | Real-time metrics tables |
| `first-row` | The first record is kept | Deduplicating event streams |

```sql
CREATE TABLE daily_revenue (
  day DATE, country STRING, revenue DECIMAL(18, 2), orders BIGINT,
  PRIMARY KEY (day, country) NOT ENFORCED
) WITH (
  'merge-engine' = 'aggregation',
  'fields.revenue.aggregate-function' = 'sum',
  'fields.orders.aggregate-function' = 'sum'
);
```

**Changelogs.** Streaming consumers usually need a proper **changelog** (insert, update-before, update-after, delete) rather than just the latest values. The `changelog-producer` option chooses how Paimon produces it: `none`, `input` (trust the input, for example CDC that is already a changelog), `lookup` (look up old values during compaction to emit exact changes, at extra cost) or `full-compaction`. A downstream Flink job can then read the table as a stream of changes, making Paimon tables both lake tables and streaming sources, which is the basis of a **streaming lakehouse** (see the topic Streaming Lakehouse Architecture).

**Append tables.** Tables without a primary key are **append tables**: insert-only, for logs and events, with optional sorting and clustering for data skipping. They can be bucketed for ordered streaming reads or bucket-unaware for maximum write parallelism.

**Snapshots and files.** Paimon keeps snapshots, manifest lists and manifests (much like Iceberg), so batch engines can time-travel and read consistent versions. Each Flink checkpoint produces a commit, so the commit interval equals the checkpoint interval.

**Compaction as a service.** Writers compact their own buckets by default. For large deployments you can set `'write-only' = 'true'` on writers and run a **dedicated compaction job**, separating ingestion latency from compaction cost.

On Google Cloud, run Flink on Managed Service for Apache Spark (which offers Flink as an optional component) or on GKE, with the warehouse in Cloud Storage.

> **Pitfall:** Choosing the bucket count matters. Too few buckets limit write parallelism (one writer task per bucket); too many create small files. Paimon also offers dynamic bucket modes that size buckets automatically; prefer them unless you have measured.

::: deeper
**Lookup and full-compaction changelogs.** To emit `update-before` records, Paimon must know the previous value of each key. The `lookup` producer reads it from higher LSM levels during compaction (using local lookup caches), producing changelogs within one checkpoint at the cost of extra I/O. `full-compaction` produces them only at full compactions, cheaper but with more latency.

**Deletion vectors.** With `'deletion-vectors.enabled' = 'true'`, compaction writes bitmaps marking superseded rows, so batch readers can read files without merging levels, trading write cost for much faster reads in engines such as Spark and StarRocks.

**Iceberg compatibility.** Paimon can also generate Iceberg metadata for its tables, so Iceberg readers can read Paimon-written data; check which table types and features this covers in your version.

**Paimon versus Hudi MOR.** Both buffer updates and merge later. Hudi keeps a base file plus logs per file group and compacts them; Paimon keeps sorted runs per bucket and merges them like a database. Paimon's sorted structure makes key lookups and changelog production cheaper; Hudi has the longer history and broader Spark tooling.

> **Interview:** "Design a table that is both a lake table and a streaming source of changes." A Paimon primary-key table fed by Flink from CDC, with a changelog producer, read in streaming mode by downstream Flink jobs and in batch by Spark or Trino, with dedicated compaction.
:::

::: check
?? Why does an LSM tree make updates cheap?
Updates are written as new sorted files and merged in the background; nothing has to locate and rewrite the old file synchronously.

?? Which merge engine would you use to maintain running totals per key?
`aggregation`, with an aggregate function such as `sum` per field.

?? What does the `lookup` changelog producer add?
It looks up previous values during compaction so the table can emit exact changelogs (including update-before records) to streaming consumers.
:::

::: resources
- Docs: [Apache Paimon documentation](https://paimon.apache.org/docs/master/) · concepts, table types, merge engines and engine integrations
- Docs: [Primary key tables](https://paimon.apache.org/docs/master/primary-key-table/) · the LSM design, buckets and compaction
- Repo: [apache/paimon](https://github.com/apache/paimon) · source code, releases and design discussions
- Article: [Apache Paimon becomes a top-level project](https://news.apache.org/foundation/entry/apache-software-foundation-announces-new-top-level-project-apache-paimon) · the project's origins and goals
- Docs: [Flink on Managed Service for Apache Spark](https://cloud.google.com/dataproc/docs/concepts/components/flink) · running Flink, and so Paimon, on Google Cloud
:::

### DuckLake and catalog-backed table formats

::: plain
Iceberg and Delta keep their inventory book as a pile of small files next to the data, and every change writes more of them. DuckLake asks a simple question: we already have excellent tools for keeping records consistent, called databases, so why not keep the inventory in one? The data stays in Parquet files in object storage; the list of which files belong to which table and version lives in ordinary SQL tables.
:::

**The design choice.** File-based table formats (Iceberg, Delta, Hudi) store all metadata as files in object storage and still need a catalog for the atomic pointer swap. That has costs: every commit writes several metadata files; planning reads a chain of files (Iceberg: metadata file, manifest list, manifests); small frequent commits create metadata sprawl that must be compacted; and transactions across several tables are hard because each table commits on its own.

A **catalog-backed** (or database-backed) format moves the metadata into a transactional database. **DuckLake**, created by the DuckDB team (first released in 2025, version 1.0 in April 2026), is the clearest example:

- **Metadata** lives in SQL tables in a catalog database: DuckDB, SQLite, PostgreSQL or MySQL. Tables record snapshots, schemas, tables, columns, data files, delete files and per-file column statistics.
- **Data** lives as plain Parquet files on local disk or object storage, such as Cloud Storage.
- A **commit** is one SQL transaction in the catalog database: insert a snapshot row, insert data file rows, mark removed files as ending at this snapshot. The database's own ACID guarantees make it atomic, and it can span many tables.

This sketch, using SQLite from the standard library, shows the core idea:

```python
import sqlite3

db = sqlite3.connect(":memory:")       # the catalog database
db.executescript("""
CREATE TABLE snapshot  (snapshot_id INTEGER PRIMARY KEY, committed_at TEXT);
CREATE TABLE data_file (path TEXT, table_name TEXT,
                        begin_snapshot INTEGER, end_snapshot INTEGER);
""")

def commit(snapshot_id, ts, add=(), remove=(), table="orders"):
    with db:                            # one SQL transaction = one atomic commit
        db.execute("INSERT INTO snapshot VALUES (?, ?)", (snapshot_id, ts))
        for p in remove:
            db.execute("UPDATE data_file SET end_snapshot = ? "
                       "WHERE path = ? AND end_snapshot IS NULL", (snapshot_id, p))
        for p in add:
            db.execute("INSERT INTO data_file VALUES (?, ?, ?, NULL)",
                       (p, table, snapshot_id))

commit(1, "2026-11-01", add=["f1.parquet", "f2.parquet"])
commit(2, "2026-11-02", add=["f3.parquet"])
commit(3, "2026-11-03", add=["f4.parquet"], remove=["f1.parquet", "f2.parquet"])

def files_as_of(s):
    return [r[0] for r in db.execute(
        "SELECT path FROM data_file WHERE table_name = 'orders' "
        "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) "
        "ORDER BY path", (s, s))]

print(files_as_of(2))   # ['f1.parquet', 'f2.parquet', 'f3.parquet']
print(files_as_of(3))   # ['f3.parquet', 'f4.parquet']
```

Time travel is a `WHERE` clause; planning is one indexed query, not a walk through files.

**Using DuckLake from DuckDB.**

```sql
INSTALL ducklake;
-- PostgreSQL (for example Cloud SQL) as the catalog, Cloud Storage for data
ATTACH 'ducklake:postgres:dbname=lake host=10.20.0.5' AS lake
  (DATA_PATH 'gs://acme-lake-eu/ducklake/');
USE lake;

CREATE TABLE orders AS SELECT * FROM 'gs://acme-landing/orders/*.parquet';
UPDATE orders SET status = 'refunded' WHERE order_id = 42;

SELECT * FROM lake.snapshots();                 -- the list of snapshots
SELECT count(*) FROM orders AT (VERSION => 1);  -- time travel
```

(DuckDB needs credentials for Cloud Storage, for example a `gcs` secret with HMAC keys.) For a single user, `ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'data/')` keeps the catalog in a local DuckDB file; for several users, use PostgreSQL so many DuckDB clients can read and write concurrently.

**Benefits.** Multi-table transactions, fast commits (a few rows in a database, not several files), cheap planning, many snapshots without metadata compaction, and an easy mental model: you can inspect the catalog with SQL. DuckLake also **inlines** very small inserts into the catalog database instead of writing a tiny Parquet file each time, avoiding the small-files problem for trickle writes.

**Costs.** The catalog database becomes critical infrastructure: it must be available, backed up and sized for the commit rate. Engine support is still centred on DuckDB, so DuckLake suits DuckDB-centric teams and small to medium platforms far more than a multi-engine enterprise lakehouse today.

**Other catalog-backed designs.** The idea is not new: Hive ACID kept transaction state in the metastore database, and warehouses always kept metadata in their own systems. BigQuery's **Apache Iceberg managed tables** keep the authoritative metadata inside BigQuery and export Iceberg metadata files for other readers. Iceberg REST catalogs that plan scans on the server also move work from files to a service. The trend is convergence: data in open Parquet, metadata in whatever system can commit and serve it fastest.

> **Pitfall:** Do not point two different catalog databases at the same `DATA_PATH`, and do not modify DuckLake's Parquet files with other tools. Like any table format, it trusts that its metadata is the only record of which files are live.

::: deeper
**Concurrency.** Conflicts are detected by the catalog database's transactions plus DuckLake's own checks of what each transaction read and wrote. With PostgreSQL as the catalog, many concurrent DuckDB writers are possible; with a local DuckDB file, only one process can attach for writing.

**Iceberg compatibility.** DuckLake stores data as Parquet with field ids and statistics compatible with Iceberg, and can import Iceberg metadata or export data so Iceberg readers can use it; check the exact interoperability features of your version.

**Comparing commit cost.** An Iceberg append writes at least a manifest, a manifest list and a metadata file, then performs a catalog swap: several object-store round trips, tens to hundreds of milliseconds each. A DuckLake commit is a single database transaction, typically milliseconds. For one large batch per hour this does not matter; for thousands of small commits per hour it changes what is practical.

> **Interview:** "What are the trade-offs of storing table metadata in a database instead of files?" Faster commits and planning, multi-table transactions and simpler inspection, versus dependence on a database's availability and scale, and a smaller multi-engine ecosystem than file-based formats with a standard catalog API.
:::

::: check
?? How does DuckLake make a commit atomic?
The whole commit (new snapshot, added files, removed files, possibly for several tables) is one transaction in the catalog SQL database.

?? Name two costs of file-based table metadata that DuckLake avoids.
Any two of: several metadata files written per commit, multi-file reads to plan a query, metadata compaction for many snapshots, and no simple multi-table transactions.

?? When is DuckLake a poor fit?
When many different engines (BigQuery, Spark, Trino, Flink) must read and write the same tables, because its ecosystem is centred on DuckDB.
:::

::: resources
- Docs: [DuckLake](https://ducklake.select/) · the format's home page, documentation and specification
- Blog: [DuckLake: SQL as a lakehouse format](https://duckdb.org/2025/05/27/ducklake.html) · the announcement explaining why metadata belongs in a database
- Repo: [duckdb/ducklake](https://github.com/duckdb/ducklake) · the DuckDB extension and its releases
- Docs: [DuckDB documentation](https://duckdb.org/docs/) · the engine DuckLake is built for, including Cloud Storage access
- Docs: [Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/iceberg-tables) · BigQuery's own take on service-held metadata with open files
:::

### Table Format Comparison

::: plain
Iceberg, Delta and Hudi are like three good car brands. All of them will get you there safely, and the differences are in where the dealerships are (which engines and vendors support them), how they handle in particular conditions (heavy updates, streaming, many engines) and who builds them. The best choice usually depends less on engine benchmarks and more on which roads you will drive.
:::

**Side by side.**

| | Apache Iceberg | Delta Lake | Apache Hudi | Apache Paimon | DuckLake |
|---|---|---|---|---|---|
| Origin | Netflix, 2017 | Databricks, 2019 | Uber, 2016 | Alibaba / Flink, 2022 | DuckDB Labs, 2025 |
| Governance | Apache | Linux Foundation | Apache | Apache | Open source (MIT) |
| Metadata | Tree of JSON and Avro files plus a catalog | Ordered JSON log plus Parquet checkpoints | Timeline plus internal metadata table | Snapshots and manifests over LSM buckets | Tables in a SQL database |
| Partitioning | Hidden transforms, evolvable | Columns, or liquid clustering | Partition path, plus indexes | Partitions plus buckets | Partitions |
| Row-level changes | COW or merge-on-read with delete files, deletion vectors (v3) | COW or deletion vectors | COW or MOR with log files | LSM merge, optional deletion vectors | Delete files |
| Strengths | Multi-engine, vendor support, catalog standard | Spark and Databricks integration, simple log | Upserts, indexes, incremental queries, table services | Streaming upserts and changelogs | Simplicity, fast commits, multi-table transactions |
| Google Cloud support | Deepest: managed tables, runtime catalog, BigQuery read and write | Spark on Managed Service for Apache Spark; BigQuery reads | Spark component; BigQuery sync | Flink on Managed Service for Apache Spark or GKE | DuckDB anywhere |

**Ecosystem is the main differentiator.** Iceberg has become the common denominator: BigQuery, Snowflake, Databricks (through Unity Catalog and UniForm), Trino, Spark, Flink, DuckDB, ClickHouse, StarRocks and most vendors read it, and the Iceberg REST catalog API gives it a standard catalog protocol. Delta is the native format of Databricks and has excellent Spark support. Hudi shines for ingestion-heavy pipelines. Paimon is the natural fit where Flink is central.

**How to decide.**

1. **Which engines must write?** If only one engine writes and others read, choose the writer's best-supported format and publish others through translation (UniForm, XTable). If many engines write, choose Iceberg with a REST catalog.
2. **Which platform are you on?** On Google Cloud with BigQuery at the centre, Iceberg is the default: BigQuery can write it, manage it and expose it to Spark through the Lakehouse runtime catalog. On Databricks, Delta (with UniForm for Iceberg readers).
3. **What is the write pattern?** Append-mostly analytics: any format. Heavy CDC upserts: Hudi MOR, Paimon, or Iceberg/Delta with merge-on-read and frequent compaction. Many tiny commits from a small team: consider DuckLake.
4. **What maintenance can you run?** Managed optimisation (BigQuery managed Iceberg, vendor services) may matter more than format features.

**Migration paths.** Iceberg has procedures to **migrate** a Hive table in place (`CALL catalog.system.migrate('db.tbl')`), **snapshot** it (create an Iceberg table over the same files for testing) or **add files** from existing Parquet. Delta can `CONVERT TO DELTA` a Parquet folder. Between table formats, the cheapest path is metadata translation (next topic); a full rewrite (`CREATE TABLE ... AS SELECT`) is simplest and lets you fix layout at the same time.

```sql
-- Spark SQL: try Iceberg over an existing Hive table without touching it
CALL lake.system.snapshot('legacy_db.orders', 'lake.sales.orders_iceberg_test');
-- when satisfied, convert in place
CALL lake.system.migrate('legacy_db.orders');
```

> **Pitfall:** Do not choose a format from a vendor benchmark. Benchmarks are usually run by one format's vendor on workloads that favour it. Test your own write pattern and query mix on your engines.

> **Tip:** Write down the features you actually need (row-level deletes, streaming upserts, time travel window, engines, catalog) before comparing. Most teams need fewer than they think, and the "best" format is the one every consumer can read.

::: deeper
**Convergence.** The formats have borrowed from each other: deletion vectors (Delta, then Iceberg v3), liquid or incremental clustering, row lineage and change feeds, variant types, REST-style catalogs. Differences in the next few years will be mostly ecosystem, catalogs and managed services rather than core capabilities.

**Feature-level compatibility inside one format.** "Supports Iceberg" can mean v1 only, v2 reads but not merge-on-read deletes, or full v3. "Supports Delta" may exclude deletion vectors or column mapping. Build a matrix per engine and per feature, and gate table property changes (format version upgrades, enabling deletion vectors) on it.

**Organisational factors.** Who supports you at 3 a.m. matters: a managed service with automatic maintenance (BigQuery managed Iceberg, Databricks with Delta) may be worth more than a technically superior self-run choice.

> **Interview:** "Iceberg, Delta or Hudi?" Avoid absolutes. Ask which engines write and read, which platform and catalog, the update pattern and the team's operating capacity. Then, for a Google Cloud BigQuery-centred platform, explain why Iceberg with the Lakehouse runtime catalog is the natural default, and name the case (heavy CDC upserts, Databricks-first teams) where you would choose differently.
:::

::: check
?? Why is ecosystem support often the deciding factor between table formats?
Core features have converged, so what matters is whether every engine and service you use can read and write the format with the features you need, and which managed services can maintain it.

?? You must convert a large Hive Parquet table to Iceberg without rewriting data. How?
Use Iceberg's `snapshot` procedure to test an Iceberg table over the same files, then `migrate` to convert in place (or `add_files` into a new table); these create Iceberg metadata pointing at the existing files.

?? On a BigQuery-centred Google Cloud platform, which format is the natural default, and why?
Iceberg: BigQuery can write and manage it (Iceberg managed tables), the Lakehouse runtime catalog shares it with Spark and other engines through the Iceberg REST API, and most other vendors read it.
:::

::: resources
- Blog: [Table format comparison: Iceberg, Hudi and Delta Lake](https://www.dremio.com/blog/comparison-of-data-lake-table-formats-apache-iceberg-apache-hudi-and-delta-lake/) · a feature-by-feature comparison (from an Iceberg-leaning vendor)
- Blog: [Hudi vs Delta vs Iceberg feature comparison](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison) · the same comparison from a Hudi-leaning vendor; read both
- Docs: [Iceberg Spark procedures](https://iceberg.apache.org/docs/latest/spark-procedures/) · migrate, snapshot, add_files and maintenance procedures
- Docs: [Delta Lake documentation](https://docs.delta.io/latest/index.html) · conversion, UniForm and feature compatibility
- Docs: [What is borderless Lakehouse?](https://docs.cloud.google.com/biglake/docs/introduction) · which formats and engines Google Cloud supports, and how
:::

### OneTable / XTable — Cross-Format Interop

::: plain
Suppose you wrote a book in English and readers want it in French and German. Retyping it twice is expensive. XTable is a translator that only rewrites the table of contents: the pages (the Parquet data files) stay exactly as they are, and each language gets its own table of contents pointing at the same pages. Iceberg, Delta and Hudi readers each find their own metadata and read one shared copy of the data.
:::

**The observation.** Iceberg, Delta and Hudi tables all store data as Parquet files. What differs is only the metadata that lists those files and their statistics. So a table written in one format can be made readable as another by **generating the other format's metadata** over the same files. No data is copied.

**Apache XTable** (incubating), which began at Onehouse as **OneTable** in 2023, does exactly this. It reads the source table's metadata (for example the Delta log), and writes equivalent metadata for one or more target formats (for example an Iceberg metadata tree and a Hudi timeline) in the same table folder. Readers of each format see a normal table.

**Running it.** XTable is a Java utility (and a library you can embed). You describe datasets in a YAML file:

```yaml
# xtable.yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
  - HUDI
datasets:
  - tableBasePath: gs://acme-lake-eu/delta/orders
    tableName: orders
```

```bash
java -jar xtable-utilities-<version>-bundled.jar --datasetConfig xtable.yaml
```

Run it after each source commit (for example as a step in the orchestration DAG right after the write job), or on a schedule. XTable keeps track of what it has already translated and performs **incremental sync** where possible, falling back to a **full sync** when needed. It can also register the target table in a catalog, so Iceberg readers find it by name.

**Where it fits.**

- A Databricks team writes Delta; a Google Cloud team wants to query it from BigQuery as Iceberg.
- A Hudi CDC pipeline feeds tables that Trino and Snowflake users read as Iceberg.
- A migration: translate first, move readers to the new format, then switch writers.

**Alternatives built into formats.** **Delta UniForm** writes Iceberg metadata after each Delta commit. **Paimon** can generate Iceberg metadata for its tables. BigQuery's `EXPORT TABLE METADATA` publishes Iceberg metadata for Iceberg managed tables. These are usually simpler than running XTable when they cover your case; XTable is the general, format-neutral option.

| Approach | Direction | Runs where |
|---|---|---|
| Apache XTable | Any of Iceberg, Delta, Hudi to the others | Separate job or library |
| Delta UniForm | Delta to Iceberg (and Hudi) | Inside Delta writes |
| Paimon Iceberg compatibility | Paimon to Iceberg | Inside Paimon commits |
| Rewrite with CTAS | Any to any | A full copy |

> **Pitfall:** Translation is one-way at a time. Treat the source format as the only writable one; if a Spark job writes to the Iceberg view of a Delta-sourced table, the two metadata histories diverge and the next sync can corrupt or drop changes.

> **Pitfall:** Not every feature translates. Features without an equivalent in the target (some delete-file representations, merge-on-read log files, certain partition transforms, newer type features) may be unsupported or force a full sync. Check XTable's support matrix for your exact formats and table types before relying on it.

::: deeper
**What is translated.** Schema (with field ids or column mapping), partition specs (where transforms map cleanly), the list of data files with their statistics, and the commit history as target snapshots. Because statistics are carried across, target readers can still prune files.

**Latency and freshness.** Target metadata lags the source by however often you run the sync. For BI that reads hourly, that is fine; for near-real-time consumers, either sync after every commit or read the source format directly.

**Statistics fidelity.** Formats store statistics differently (Delta stores them as JSON strings in the log, often only for the first 32 columns; Iceberg stores bounds in manifests). A translated table is only as prunable as the statistics the source collected.

**Catalog sync.** XTable can register targets in catalogs (Hive Metastore, Iceberg catalogs). Keep the source's catalog as the catalog of record and give the targets read-only registrations.

> **Interview:** "Two teams need the same table, one in Delta and one in Iceberg, without copying data. How?" Keep one writer format, and publish the other format's metadata over the same Parquet files with Delta UniForm or Apache XTable, run after each commit; restrict the translated view to read-only and check feature compatibility.
:::

::: check
?? Why can XTable make a Delta table readable as Iceberg without copying data?
Both formats store data as Parquet files; XTable only generates Iceberg metadata (schema, partitions, file lists, statistics, snapshots) that points at the same files.

?? Why must the translated format be treated as read-only?
If both formats are written, their metadata histories diverge, and the next sync can overwrite or lose changes. Only the source format should accept writes.

?? When would you prefer Delta UniForm over XTable?
When the source is Delta and the target is Iceberg (or Hudi), because UniForm produces the metadata automatically inside each commit with no separate job.
:::

::: resources
- Docs: [Apache XTable documentation](https://xtable.apache.org/) · concepts, setup, catalog integration and the feature support matrix
- Repo: [apache/incubator-xtable](https://github.com/apache/incubator-xtable) · source, releases and configuration examples
- Blog: [OneTable is now open source](https://www.onehouse.ai/blog/onetable-is-now-open-source) · the original announcement and motivation
- Docs: [Delta Lake UniForm](https://docs.delta.io/latest/delta-uniform.html) · Delta's built-in Iceberg and Hudi metadata generation
- Docs: [Export Iceberg metadata from BigQuery](https://docs.cloud.google.com/bigquery/docs/iceberg-tables) · how BigQuery publishes Iceberg metadata for other readers
:::

## Operating a lakehouse

Creating a lakehouse table takes one statement; keeping it fast, cheap and correct for years takes routine work. This group is about that work. Every topic here exists because object storage files are immutable and table formats only ever add files and snapshots: left alone, a table accumulates small files, stale snapshots, scattered data and orphaned files, and gets slower and more expensive week by week.

The topics follow the life of a table: keeping files the right size (compaction), keeping related rows together so queries can skip data (clustering and Z-ordering), keeping just enough history (snapshot management), tuning reads, running all of it under continuous streaming writes, and finally extending the lakehouse to images, audio and documents.

### Small Files Problem & Compaction

::: plain
Imagine a library that receives every new page as a separate leaflet instead of as bound books. After a year it has millions of leaflets, and finding anything means opening thousands of them, even though the total amount of text is the same. **Compaction** is the librarian binding the leaflets into proper books in the background, without changing a word of the content.
:::

**Where small files come from.**

- **Streaming and micro-batch writes**: every commit (every Flink checkpoint, every Spark micro-batch) writes at least one file per writer task per partition it touches.
- **Over-partitioning**: partitioning a 50 GiB table by day and country and device produces thousands of partitions with a few megabytes each.
- **Too many writer tasks**: a Spark job with 2,000 shuffle partitions writing into 30 date partitions can create up to 60,000 files, each tiny.
- **Row-level changes**: merge-on-read writes small delete files and log files on every update.

**Why they hurt.** Each file has fixed costs regardless of size: a metadata entry to track, planning work, at least two ranged reads (footer, then data), a request to the object store, and a task or split to schedule. A worked example for a 10 TiB table:

| File size | Number of files | Effect on a full scan |
|---|---|---|
| 8 MiB | about 1.3 million | 1.3 million footers to read, huge manifests, planning in minutes |
| 128 MiB | about 82,000 | Reasonable |
| 512 MiB | about 20,000 | Iceberg's default target; efficient scans and small metadata |

The data is identical; only the overhead changes. Small files also compress worse (less data per dictionary and per page) and make statistics less useful (more files to check).

**Preventing them at write time** is cheaper than fixing them later:

- Partition coarsely (by day, not by hour and country) and let clustering handle the rest.
- Make the writer **shuffle by partition** before writing so each partition is written by few tasks. In Iceberg, `write.distribution-mode` set to `hash` asks Spark to do this; Spark's adaptive query execution then coalesces shuffle partitions to a sensible size.
- Increase the commit interval of streaming jobs where latency allows (a 5-minute commit writes a fifth as many files as a 1-minute one).
- Use format features that bin-pack at write time: Delta **optimized writes** (`delta.autoOptimize.optimizeWrite`), Hudi's small-file handling (inserts are routed into existing undersized files), DuckLake's inlining of tiny inserts.

```sql
ALTER TABLE lake.sales.orders SET TBLPROPERTIES (
  'write.distribution-mode' = 'hash',
  'write.target-file-size-bytes' = '536870912'    -- 512 MiB, the default
);
```

**Compaction** rewrites many small files into fewer large ones in a new snapshot. Readers continue to use the old snapshot until the commit, so compaction is invisible to them.

```sql
-- Iceberg (Spark): bin-pack recent partitions only
CALL lake.system.rewrite_data_files(
  table   => 'sales.orders',
  where   => 'order_ts >= TIMESTAMP ''2026-11-14 00:00:00''',
  options => map('target-file-size-bytes', '536870912',
                 'min-input-files', '5',
                 'partial-progress.enabled', 'true')
);
-- merge-on-read tables also need their delete files rewritten
CALL lake.system.rewrite_position_delete_files(table => 'sales.orders');

-- Delta: bin-packing (and clustering, for clustered tables)
OPTIMIZE sales.orders WHERE order_date >= '2026-11-14';
```

Hudi compacts merge-on-read logs into base files with its **compaction** service and fixes layout with **clustering**; Paimon compacts its LSM levels continuously. Delta's **auto compaction** (`delta.autoOptimize.autoCompact`) runs a small compaction after writes.

**Scheduling compaction.** Compact only what changed (filter by recent partitions), run it often enough that file counts stay bounded, and run it when it does not collide with heavy writers. A typical batch platform compacts yesterday's partitions every night; a streaming table compacts closed partitions every hour.

> **Pitfall:** Compaction conflicts with concurrent writers that delete or update the same files. Compacting the partition a streaming `MERGE` is writing to causes repeated commit failures. Compact partitions that are no longer receiving updates, or use `partial-progress.enabled` so the rewrite commits in smaller groups.

> **Tip:** On Google Cloud, Apache Iceberg managed tables compact and re-cluster automatically. For tables you maintain yourself, run compaction as a scheduled serverless Spark batch on Managed Service for Apache Spark, orchestrated alongside the pipelines that write the table.

::: deeper
**Bin-packing versus sorting.** Bin-packing only combines files to reach the target size, and is cheap. A **sort** rewrite (`strategy => 'sort'`) also orders rows, improving data skipping, and costs a full shuffle. A common pattern is bin-pack hourly and sort or Z-order weekly.

**Write amplification.** Every compaction rewrites data that was already written once; frequent compaction of hot partitions can write more bytes than the original ingestion. Measure bytes written by maintenance as a cost line in its own right.

**Metadata compaction.** Small commits also create many small manifests (Iceberg) or many log entries (Delta, handled by checkpoints). Iceberg's `rewrite_manifests` regroups manifests so that planning reads few files, ideally clustered by partition.

**Orphans after failures.** A failed compaction leaves written but uncommitted files. They are never visible, but they cost storage until an orphan-file cleanup removes them (see Time Travel & Snapshot Management).

For Spark-side causes and fixes in more depth (coalesce versus repartition, AQE, output committers), see [Small files and output layout](pyspark-streaming-kafka-topic-map.html#smallfiles) in the PySpark + Kafka guide.

> **Interview:** "A lakehouse table's queries got ten times slower over three months with no change in data volume per day. What do you check?" File counts and average file size per partition, number of snapshots and manifests, delete-file counts for merge-on-read tables, and whether compaction and snapshot expiry jobs are running. Then fix at the writer (distribution, commit interval, partitioning) as well as compacting.
:::

::: check
?? Name three common causes of small files.
Any three of: frequent streaming commits, over-partitioning, many writer tasks writing into each partition, and merge-on-read delete or log files from frequent updates.

?? Why is preventing small files at write time better than compacting later?
Compaction rewrites data that was already written (write amplification) and costs compute; shuffling by partition, coarser partitions and longer commit intervals avoid creating the problem.

?? Why can compaction fail repeatedly on a table being updated by a streaming job?
It rewrites files the streaming job is concurrently deleting or updating, so one of the commits conflicts; compact only partitions that are no longer changing, or commit in smaller groups.
:::

::: resources
- Docs: [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/) · compaction, manifest rewrites, snapshot expiry and orphan cleanup
- Docs: [Iceberg Spark procedures](https://iceberg.apache.org/docs/latest/spark-procedures/) · every option of `rewrite_data_files` and related procedures
- Docs: [Delta Lake optimizations](https://docs.delta.io/latest/optimizations-oss.html) · OPTIMIZE, optimized writes, auto compaction and Z-order
- Docs: [Hudi file sizing](https://hudi.apache.org/docs/file_sizing) · how Hudi avoids small files on write and fixes them with clustering
- Docs: [Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/iceberg-tables) · BigQuery's automatic storage optimisation for Iceberg
- Book: [Apache Iceberg: The Definitive Guide](https://www.oreilly.com/library/view/apache-iceberg-the/9781098148614/) · chapters on table maintenance and performance
:::

### Data Skipping, Z-Ordering & Liquid Clustering

::: plain
If a shop's receipts are filed in a random order, finding all of March's receipts means checking every box. If they are filed by date, you only open the March boxes. But what if people also search by customer? Z-ordering files the receipts so that both dates and customers stay roughly together, like arranging a city map in small squares instead of long strips, so a search by either one only opens a few boxes.
:::

**Data skipping** means not reading files (or row groups, or pages) whose statistics prove they cannot contain matching rows. It is the single most important performance feature of a lakehouse, and it only works when the data is **clustered**: rows with similar values in the filtered columns must sit in the same files, so each file's min-max range is narrow.

**Linear sorting** (`ORDER BY a, b`) clusters perfectly on `a`, but rows with the same `b` are scattered across all files unless `a` is constant. A filter on the second sort column skips almost nothing.

**Z-ordering** maps several columns onto one number by **interleaving their bits**, then sorts by that number. Points close together in all dimensions end up close together in the sort order, so each file covers a small square of values instead of a long strip. This standard-library code counts how many files a query must open on a 4 by 4 grid of rows written as four files of four rows:

```python
def z_value(x, y, bits=2):
    """Interleave the bits of x and y: y1 x1 y0 x0."""
    z = 0
    for i in range(bits):
        z |= ((x >> i) & 1) << (2 * i)
        z |= ((y >> i) & 1) << (2 * i + 1)
    return z

points = [(x, y) for x in range(4) for y in range(4)]   # 16 rows, a 4x4 grid

def files_touched(order, predicate, rows_per_file=4):
    rows = sorted(points, key=order)
    files = [rows[i:i + rows_per_file] for i in range(0, len(rows), rows_per_file)]
    return sum(any(predicate(p) for p in f) for f in files)

linear = lambda p: (p[0], p[1])        # ORDER BY x, y
zorder = lambda p: z_value(*p)         # ZORDER BY (x, y)

for name, order in [("linear", linear), ("z-order", zorder)]:
    print(name, files_touched(order, lambda p: p[0] == 1),
                files_touched(order, lambda p: p[1] == 1))
# linear 1 4
# z-order 2 2
```

Linear sorting is perfect for `x` (1 file) and useless for `y` (all 4 files). Z-order is good for both (2 files each). With real tables of thousands of files the gap is much larger.

[[fig:lakehouse_zorder]]

**How each format does it.**

```sql
-- Delta: Z-order during OPTIMIZE
OPTIMIZE sales.orders ZORDER BY (customer_id, product_id);

-- Delta: liquid clustering (declare once, OPTIMIZE clusters incrementally)
ALTER TABLE sales.orders CLUSTER BY (customer_id, order_date);
OPTIMIZE sales.orders;

-- Iceberg: a table sort order used by writers, and a Z-order rewrite
ALTER TABLE lake.sales.orders WRITE ORDERED BY customer_id, order_ts;
CALL lake.system.rewrite_data_files(
  table => 'sales.orders', strategy => 'sort',
  sort_order => 'zorder(customer_id, product_id)');
```

Hudi's **clustering** service rewrites file groups with a layout strategy (linear, Z-order or Hilbert curve) on chosen columns. BigQuery's own **clustering** (on native tables and Iceberg managed tables) does the same job automatically.

**Liquid clustering** (Delta, and the same idea in Databricks and in BigQuery) replaces both Hive-style partitioning and one-off Z-ordering. You declare clustering keys; the format clusters new data incrementally during `OPTIMIZE`, re-clusters only what needs it, and lets you change keys without rewriting the whole table. Delta's open-source implementation uses a Hilbert curve, which keeps neighbours together slightly better than a Z-curve.

**Choosing columns.**

- Cluster on columns that appear in selective filters and join keys: `customer_id`, `event_date`, `tenant_id`.
- Use one to four columns. Each extra column dilutes the clustering of the others.
- Prefer columns with medium to high cardinality for clustering; very low cardinality columns (a boolean, five countries) gain little and may be better as a partition.
- Partition only by a coarse time unit (or not at all with liquid clustering), and cluster within partitions.

> **Pitfall:** Z-ordering is not maintained automatically on new writes. After a week of appends, the new files are unclustered again. Schedule Z-order or sort rewrites on recent data, or use liquid clustering or managed tables that re-cluster for you.

> **Tip:** Measure skipping, do not assume it. Compare "files in table" with "files scanned" for typical queries (the Spark UI, Iceberg scan metrics, Delta's `numFilesTotal` versus files read, BigQuery's bytes processed).

::: deeper
**Why the curve matters.** The Z-curve jumps between quadrants (the "Z" shape), so some neighbouring points end up far apart in the order. The **Hilbert curve** never jumps, so it preserves locality better, at slightly higher computation cost. Both are space-filling curves; neither is perfect for all query shapes.

**Value ranges.** Bit interleaving needs comparable integer values. Implementations first map each column's values to ranks or ranges (Delta uses range partition ids; Iceberg normalises values into bytes), so skewed distributions do not waste the curve on empty ranges.

**Clustering and file boundaries.** Skipping happens per file (and per row group and page). Clustering a table into 100 files but writing each file with 50 row groups still lets readers skip row groups within files, provided the data inside each file is also ordered.

**Partition versus clustering.** Partitions give hard boundaries and cheap deletes of whole partitions (drop last year), but too many partitions mean small files. Clustering gives soft boundaries with no small-file penalty. Modern advice: partition by a coarse time unit only if you need lifecycle management by time, and cluster everything else.

> **Interview:** "Queries filter by customer_id or by product_id, never both. How do you lay out the table?" Explain that linear sort helps only the first column, Z-order or Hilbert clustering on both gives good skipping for each, and liquid clustering or managed clustering keeps it maintained; mention measuring files scanned before and after.
:::

::: check
?? Why does a linear sort on `(a, b)` help filters on `a` but not on `b`?
Rows are grouped by `a` first, so each file has a narrow range of `a`; values of `b` repeat across every group, so every file's `b` range is wide and cannot be skipped.

?? In the Python example, why does Z-order touch two files for each filter?
Each file holds a 2 by 2 square of the grid, so any single row or column of the grid crosses exactly two squares.

?? What advantage does liquid clustering have over a one-off `ZORDER BY`?
It is declared on the table and maintained incrementally by `OPTIMIZE` as new data arrives, and the clustering keys can be changed without a full rewrite.
:::

::: resources
- Docs: [Delta Lake optimizations](https://docs.delta.io/latest/optimizations-oss.html) · data skipping, Z-order and how statistics are used
- Docs: [Delta Lake liquid clustering](https://docs.delta.io/latest/delta-clustering.html) · declaring, changing and maintaining clustering keys
- Docs: [Iceberg Spark writes: sort order and distribution](https://iceberg.apache.org/docs/latest/spark-writes/) · write ordering, distribution modes and Z-order rewrites
- Docs: [Hudi clustering](https://hudi.apache.org/docs/clustering) · layout strategies including Z-order and Hilbert curves
- Docs: [Introduction to clustered tables in BigQuery](https://cloud.google.com/bigquery/docs/clustered-tables) · the same idea in BigQuery, maintained automatically
- Article: [Z-order curve](https://en.wikipedia.org/wiki/Z-order_curve) · the mathematics of bit interleaving with pictures
:::

### Time Travel & Snapshot Management

::: plain
Time travel is the undo history of a document. It is wonderful when you delete a paragraph by mistake, but if you keep every version forever the file grows without limit, and a sentence you "deleted" for legal reasons is still in the history. Snapshot management is deciding how much undo history to keep, and cleaning out the rest on a schedule.
:::

**Why history exists.** Table formats never modify files; each commit creates a snapshot that references some old files and some new ones. Old snapshots keep their files alive. That gives you:

- **Time travel**: query the table as of a version or a time (`TIMESTAMP AS OF`, `VERSION AS OF`, `AT (VERSION => n)`).
- **Rollback**: point the table back at a good snapshot after a bad write.
- **Reproducibility**: train a model or rerun a report on exactly the data used before.
- **Incremental reads**: downstream jobs read changes between two snapshots.
- **Safe concurrency**: long-running readers keep reading the snapshot they started with.

**What it costs.** Every rewritten or deleted file is kept as long as any retained snapshot references it. A worked example: a 10 TiB table where a daily `MERGE` rewrites about 2 percent of the data (200 GiB). Keeping 30 days of snapshots retains up to 30 x 200 GiB = 6 TiB of extra files, 60 percent more storage, at roughly $20 per TiB per month for Standard storage in many regions, about $120 a month. Keeping 7 days retains about 1.4 TiB. Metadata also grows with every snapshot.

**The three cleanup operations.**

1. **Expire snapshots**: remove old snapshots from the table metadata and delete the files that no remaining snapshot references.
2. **Remove orphan files**: delete files in the table location that no snapshot ever referenced (left by failed writes or failed compactions).
3. **Trim metadata**: old metadata files and log entries (Iceberg `write.metadata.delete-after-commit.enabled`, Delta log retention, Hudi timeline archival).

```sql
-- Iceberg (Spark)
CALL lake.system.expire_snapshots(
  table => 'sales.orders',
  older_than => TIMESTAMP '2026-11-14 00:00:00',
  retain_last => 10);
CALL lake.system.remove_orphan_files(
  table => 'sales.orders',
  older_than => TIMESTAMP '2026-11-19 00:00:00');

-- keep a named version for an audit, independently of expiry
ALTER TABLE lake.sales.orders CREATE TAG `fy2026-close` RETAIN 2555 DAYS;

-- Delta
VACUUM sales.orders RETAIN 168 HOURS;
ALTER TABLE sales.orders SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days');
```

**Defaults to know.**

| Format | Setting | Default |
|---|---|---|
| Iceberg | `history.expire.max-snapshot-age-ms` (used when `expire_snapshots` runs without explicit arguments) | 5 days |
| Iceberg | `history.expire.min-snapshots-to-keep` | 1 |
| Iceberg | `remove_orphan_files` `older_than` | 3 days before now |
| Delta | `delta.deletedFileRetentionDuration` (VACUUM threshold) | 7 days |
| Delta | `delta.logRetentionDuration` | 30 days |
| Hudi | `hoodie.cleaner.commits.retained` | 10 commits |
| BigQuery native and Iceberg managed tables | Time travel window, then fail-safe | 2 to 7 days (default 7), then 7 days |

Iceberg does not expire snapshots by itself: the defaults apply when you run the procedure. Delta does not delete files until you run `VACUUM`. Something must run these regularly.

**Hudi** keeps history with its **cleaner** (policies by number of commits, number of file versions, or hours) and offers **savepoints**, which protect a commit and its files from cleaning so you can restore to it.

**Choosing retention.** Work backwards from requirements:

- The slowest consumer: an incremental job that runs weekly needs at least a week of snapshots.
- Recovery: how long might it take to notice a bad write? Many teams keep 7 days.
- Legal: deletion requests (GDPR, CCPA) are only complete once the snapshots containing the deleted rows have expired and the files are physically removed. Retention must be shorter than your legal deadline.
- Long-term versions: use tags (Iceberg) or savepoints (Hudi), or copy to an archive table, rather than keeping all snapshots for years.

> **Pitfall:** Running `remove_orphan_files` with a short `older_than` can delete files of a write that is still in progress, because they look like orphans until the commit lands. Keep the threshold longer than your longest-running write job (the default 3 days is deliberately conservative).

> **Pitfall:** A `DELETE` for a privacy request does not remove the data from storage. Old snapshots still reference the files containing it. Plan expiry and physical cleanup as part of the deletion process, and for merge-on-read tables also compact so rows are physically removed from data files.

::: deeper
**Branches and tags change expiry.** In Iceberg, snapshots referenced by a branch or tag are not expired while the ref exists; refs have their own `max-ref-age-ms` and snapshot retention settings. An abandoned audit branch can silently keep terabytes alive. List refs (`SELECT * FROM lake.sales.orders.refs`) in your maintenance checks.

**Time travel depth versus file retention.** In Delta, you need both the log entries (log retention) and the data files (deleted file retention) to time-travel; whichever is shorter limits you. In Iceberg, the snapshot entry and its files go together on expiry.

**Expiry at scale.** Expiring snapshots on huge tables means reading many manifests to find unreferenced files; Iceberg's Spark procedure can do this in parallel (`stream_results`, `max_concurrent_deletes`). Orphan-file removal lists the whole table location, which is slow on very large prefixes; run it less often than expiry (weekly or monthly).

**Soft delete underneath.** On Cloud Storage, files deleted by expiry go into the bucket's soft delete retention (7 days by default) and still cost storage there. Account for it, or shorten it on table buckets.

> **Interview:** "How do you design snapshot retention for a lakehouse?" Derive retention from consumers, recovery time and legal deletion deadlines; automate expiry, orphan cleanup and metadata trimming; use tags or savepoints for long-term versions; and monitor storage held by old snapshots and refs.
:::

::: check
?? Why does keeping many snapshots increase storage cost?
Each snapshot keeps alive the files it references, including files later rewritten or deleted, so the more history you keep, the more superseded data stays in storage.

?? Why might a GDPR deletion not be complete after `DELETE FROM`?
Older snapshots still reference files containing the rows; they remain readable through time travel and physically present until those snapshots expire and the files are removed (and merge-on-read files are compacted).

?? What is the risk of running orphan-file removal with a very short threshold?
It can delete files of writes still in progress, which would then commit metadata pointing at missing files and corrupt the table.
:::

::: resources
- Docs: [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/) · expiring snapshots, removing orphans and metadata cleanup
- Docs: [Iceberg branching and tagging](https://iceberg.apache.org/docs/latest/branching/) · refs, retention and how they interact with expiry
- Docs: [Delta Lake table utility commands](https://docs.delta.io/latest/delta-utility.html) · VACUUM, history, restore and retention properties
- Docs: [Hudi cleaning](https://hudi.apache.org/docs/cleaning) · cleaner policies and how they affect time travel and incremental reads
- Docs: [BigQuery time travel and fail-safe](https://cloud.google.com/bigquery/docs/time-travel) · how BigQuery handles history for comparison
:::

### Lakehouse Query Performance Tuning

::: plain
A fast query in a lakehouse is mostly a lazy query: it reads as little as possible. Think of finding a recipe in a library. First pick the right shelf (partition), then the right books by their spine labels (file statistics), then the right chapter by the table of contents (row groups and pages), and only then read the few pages you need (columns). Tuning is making sure every one of those shortcuts actually works for your queries.
:::

**The pruning funnel.** For each query, a lakehouse engine narrows the data in stages:

1. **Partition pruning** from partition values recorded in metadata (Iceberg's manifest list ranges and manifest entries, Delta's `add` actions).
2. **File pruning** using per-file column statistics (min, max, null counts).
3. **Row-group and page pruning** inside each Parquet file, using footer statistics, the page index and Bloom filters.
4. **Projection pushdown**: read only the needed columns.
5. **Late materialisation**: read the filter columns first, then fetch other columns only for matching rows (engines vary).

Each stage depends on something you control: partition design, clustering, statistics collection and file sizes.

**Read the plan.** Check that filters are pushed into the scan:

```sql
-- Spark SQL on Iceberg
EXPLAIN FORMATTED
SELECT customer_id, SUM(amount) FROM lake.sales.orders
WHERE order_ts >= TIMESTAMP '2026-11-01 00:00:00' AND country = 'DE'
GROUP BY customer_id;
-- look for the filters listed on the BatchScan node of lake.sales.orders
```

Then compare files and bytes scanned with the table's total, in the Spark UI (scan node metrics), in Iceberg's scan report metrics, in Trino's `EXPLAIN ANALYZE`, or in BigQuery's job details.

**Common reasons pruning fails.**

| Symptom | Cause | Fix |
|---|---|---|
| All files scanned despite a date filter | Filter on a derived expression the engine cannot push down, such as a string-formatted date | Filter on the raw column with a range; use Iceberg hidden partitioning |
| Statistics present but nothing skipped | Data not clustered on the filter column | Sort, Z-order or cluster |
| No statistics for a column | Column beyond the metrics limit (Iceberg's first 100 columns, Delta's first 32) | Configure metrics for that column, or reorder |
| Type mismatch | `WHERE order_id = 123` on a string column wraps the column in a cast | Compare with the right type (`'123'`) |
| Slow planning, fast execution | Too many small files, manifests or snapshots | Compact, rewrite manifests, expire snapshots |

**Statistics configuration.**

```sql
ALTER TABLE lake.sales.orders SET TBLPROPERTIES (
  'write.metadata.metrics.default' = 'truncate(16)',          -- the default
  'write.metadata.metrics.column.customer_email' = 'none',    -- long, never filtered
  'write.metadata.metrics.column.order_id' = 'full',
  'write.parquet.bloom-filter-enabled.column.order_id' = 'true'
);
```

`truncate(16)` stores the first 16 characters of string bounds, enough for most filters and much smaller metadata. Turning statistics off for wide text columns keeps manifests small. Bloom filters help equality lookups on high-cardinality ids.

**Planning performance.** Planning reads metadata, so it slows down when metadata grows: thousands of small manifests, millions of file entries, long snapshot histories. `rewrite_manifests` groups manifest entries by partition so a filtered query reads few manifests; snapshot expiry and compaction keep the rest small. Iceberg REST catalogs can also plan scans on the server, and engines cache metadata (Trino's metadata cache, BigQuery's metadata caching for Lakehouse tables with `max_staleness`).

**Execution performance.**

- **File and split sizes**: Spark reads Iceberg files in splits of `read.split.target-size` (128 MiB by default), so very large files are still read in parallel; very small ones cannot be combined beyond limits and waste tasks.
- **Vectorised reads**: Parquet readers in Spark, Trino and DuckDB decode batches of values at a time; keep them enabled (they are by default) and avoid types they do not vectorise in your engine version.
- **Merge-on-read overhead**: many delete files or log files make every read merge them. Compact delete files regularly.
- **Join statistics**: cost-based optimisers use row counts and distinct counts. Engines can store distinct-value sketches in Iceberg Puffin files (for example Trino's `ANALYZE`), which improves join ordering.

> **Pitfall:** Partition evolution helps new data only. After changing an Iceberg table from daily to hourly partitions, queries on old data still prune by day; rewrite old partitions only if their queries justify the cost.

> **Tip:** Tune for the top ten queries by cost or frequency, not for every query. Find them from engine query logs (for BigQuery, `INFORMATION_SCHEMA.JOBS`), then check their scan ratios one by one.

::: deeper
**Dynamic partition pruning and runtime filters.** In a star-schema join (`fact JOIN dim WHERE dim.region = 'EU'`), the filter is on the dimension, not the fact. Spark's dynamic partition pruning and Trino's dynamic filters compute the matching keys at runtime and push them into the fact table scan, pruning partitions and files. They work best when the fact table is partitioned or clustered on the join key.

**Statistics truncation and correctness.** Truncated upper bounds are rounded up (and lower bounds down) so pruning remains correct: a file is never skipped wrongly, only sometimes not skipped when it could have been.

**Engine differences.** Not every engine uses every statistic: some ignore page indexes or Bloom filters, some do not apply Iceberg's partition transforms for every predicate shape. A layout tuned for Spark may need checking in BigQuery and Trino.

**Caching.** Local SSD caches (Trino's file system cache, Alluxio, Anywhere Cache on Cloud Storage) reduce repeated object reads; footer and metadata caches cut planning time. They hide problems rather than fix them, so tune layout first.

For Spark-specific scan and join tuning, see [Storage and table-format optimization](pyspark-streaming-kafka-topic-map.html#storageopt) in the PySpark + Kafka guide.

> **Interview:** "A lakehouse query scans the whole table although it filters on a date. Walk me through debugging." Check the plan for pushed filters; check whether the filter is on the partition source column and of the right type; check file-level statistics and clustering for that column; check planning time versus execution time to separate metadata problems from data problems; then fix the layout or the query.
:::

::: check
?? List the stages of the pruning funnel.
Partition pruning, file pruning by column statistics, row-group and page pruning inside files, projection pushdown, and late materialisation.

?? A query filters `WHERE order_id = 12345` on a string column and reads every file. Why?
The engine must cast the column to compare it with an integer, so it cannot use the string statistics for pruning. Compare with a string literal.

?? Planning takes 40 seconds and execution 5. What do you look at?
Metadata size: number of snapshots, manifests and data files. Expire snapshots, rewrite manifests and compact small files.
:::

::: resources
- Docs: [Iceberg performance](https://iceberg.apache.org/docs/latest/performance/) · scan planning, statistics and pruning explained by the project
- Docs: [Iceberg configuration](https://iceberg.apache.org/docs/latest/configuration/) · metrics modes, split sizes, Bloom filters and write options
- Docs: [Delta Lake optimizations](https://docs.delta.io/latest/optimizations-oss.html) · data skipping statistics and their configuration
- Docs: [BigLake metadata caching](https://cloud.google.com/bigquery/docs/biglake-intro) · how BigQuery speeds up planning over lake files
- Docs: [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html) · Trino's pruning, statistics and table maintenance support
- Book: [Apache Iceberg: The Definitive Guide](https://www.oreilly.com/library/view/apache-iceberg-the/9781098148614/) · performance optimisation chapters with engine-specific advice
:::

### Streaming Lakehouse Architecture

::: plain
Classic lakehouses are restocked like a supermarket at night: one big delivery, then shelves stay untouched all day. A **streaming lakehouse** is restocked continuously, a few boxes every minute, so the shelves are always nearly current. The hard part is not carrying the boxes but keeping the shop tidy while people are constantly adding to it.
:::

**The goal.** One copy of data in open tables that is fresh within minutes (sometimes seconds), queryable by batch engines, and usable as a source of changes for further streaming, without a separate real-time system that must be kept in sync. It replaces the **Lambda architecture** (separate batch and speed layers) with one storage layer that both batch and streaming jobs read and write.

**The pieces.**

- **Sources**: events on Kafka (Google Managed Service for Apache Kafka) or Pub/Sub, and database changes from Datastream or Debezium.
- **Stream processor**: Apache Flink, Spark Structured Streaming or Dataflow (Apache Beam).
- **Streaming-capable table format**: Iceberg (append and equality-delete upserts), Hudi (MOR upserts), Paimon (LSM upserts and changelogs), Delta (Spark streaming source and sink).
- **Table services** running continuously: compaction, clustering, snapshot expiry.
- **Readers**: batch SQL (BigQuery, Trino, Spark) and incremental or streaming reads by downstream jobs.

**Commits follow checkpoints.** A streaming sink cannot commit every record; it buffers rows in data files and commits at checkpoints. In Flink, each successful checkpoint commits one table snapshot, which also gives **exactly-once** results: if the job fails, it restarts from the last checkpoint and the uncommitted files are discarded. The checkpoint interval therefore sets the freshness and the file count at the same time:

| Checkpoint interval | Freshness | Commits per day | Files per day (50 writers, 1 partition) |
|---|---|---|---|
| 10 seconds | about 10 s | 8,640 | up to 432,000 |
| 1 minute | about 1 min | 1,440 | up to 72,000 |
| 5 minutes | about 5 min | 288 | up to 14,400 |

Most teams settle on one to five minutes and compact continuously.

```sql
-- Flink SQL: Kafka to an Iceberg table, upserting by key
CREATE TABLE lake.silver.orders (
  order_id BIGINT, status STRING, amount DECIMAL(12, 2), updated_at TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

INSERT INTO lake.silver.orders
SELECT order_id, status, amount, updated_at FROM kafka_orders;
```

With upserts, the Flink Iceberg sink writes **equality delete files** for each changed key, which makes reads progressively slower until compaction. That is why upsert-heavy streaming often uses Paimon or Hudi MOR, which are designed to merge such changes cheaply, or lands append-only in bronze and applies `MERGE` in small batches.

**Real-time medallion.** A practical design:

1. **Bronze**: an append-only stream of raw events into an Iceberg or Paimon table (cheap commits, no deletes).
2. **Silver**: a streaming job upserts deduplicated, typed records by key (Paimon primary-key table, Hudi MOR or Iceberg with frequent compaction), or a micro-batch `MERGE` every few minutes.
3. **Gold**: incremental aggregates, maintained by streaming aggregation (Paimon's aggregation merge engine) or by incremental batch jobs reading only new snapshots.

**On Google Cloud.**

- **Dataflow** can write Iceberg tables through Beam's managed Iceberg I/O, using the Lakehouse runtime catalog, so a Pub/Sub to Iceberg pipeline needs no custom sink code.
- **BigQuery** Apache Iceberg managed tables accept high-throughput streaming through the Storage Write API and optimise their files automatically, the simplest way to get a streaming Iceberg table when BigQuery is the writer.
- **Flink** runs on Managed Service for Apache Spark (Flink optional component) or GKE, with Iceberg or Paimon tables in Cloud Storage.
- **Spark Structured Streaming** runs on Managed Service for Apache Spark and writes Iceberg, Delta or Hudi.

```python
# Spark Structured Streaming: Kafka to an Iceberg bronze table every minute
(spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "bootstrap.acme-kafka.europe-west1.managedkafka.acme-prod.cloud.goog:9092")
    .option("subscribe", "orders")
    .load()
    .selectExpr("CAST(key AS STRING) AS order_key", "CAST(value AS STRING) AS payload",
                "timestamp AS kafka_ts")
    .writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", "gs://acme-lake-eu/checkpoints/bronze_orders")
    .toTable("lake.bronze.orders_raw"))
```

(Managed Kafka also needs authentication options for the client; see the Streaming systems section.)

**Reading incrementally.** Downstream jobs read only new data: Flink's Iceberg source in streaming mode polls for new snapshots, Paimon tables can be read as changelog streams, Hudi has incremental queries, Delta tables are Spark streaming sources, and BigQuery offers change history functions for supported tables.

> **Pitfall:** Compaction and streaming writers conflict when they touch the same files. Compact partitions that are closed (for example, previous hours), use formats whose compaction is built into the write path (Paimon, Hudi async compaction), or rely on managed tables.

> **Tip:** Write down the freshness the consumers actually need. "Real-time" often turns out to mean "within 15 minutes", which allows longer commit intervals, fewer files and simpler batch `MERGE`s.

::: deeper
**Streaming storage in front of the lake.** Some designs add a low-latency streaming storage layer that tiers older data into lake tables automatically, so readers get sub-second freshness from the stream and history from the lake through one table abstraction (for example Apache Fluss, incubating, with Paimon and Iceberg, and vendor features such as Confluent's Tableflow that materialise Kafka topics as Iceberg tables). These reduce the custom pipeline code between Kafka and the lake.

**Exactly-once end to end.** The sink commits only on checkpoint completion, and the commit records the checkpoint id in the snapshot summary, so a restarted job can tell whether its last checkpoint's commit already happened and avoid committing twice. Upstream, the source offsets are part of the same checkpoint.

**Late data.** Late events update old partitions. With upserts they create deletes in already-compacted partitions; with appends they create small files there. Bound lateness with watermarks, route very late data to a separate correction path, and include late partitions in compaction.

**Kappa with a lake.** Reprocessing can come from the lake instead of the log: bronze tables keep full history more cheaply than Kafka retention, and a backfill job can reread bronze snapshots.

For streaming semantics in depth (event time, watermarks, state, exactly-once), see [Checkpointing, fault tolerance and exactly-once](pyspark-streaming-kafka-topic-map.html#ft) in the PySpark + Kafka guide.

> **Interview:** "Design a pipeline that keeps a lake table of orders within five minutes of the source database." Datastream or Debezium CDC to Kafka or Pub/Sub, a streaming job (Flink or Dataflow) upserting into a Paimon, Hudi MOR or Iceberg table keyed by order id with a five-minute checkpoint, continuous compaction of closed partitions, snapshot expiry, and BigQuery reading the table through the Lakehouse runtime catalog; discuss exactly-once, late updates and how you would backfill.
:::

::: check
?? Why does the checkpoint interval affect both freshness and file count?
The streaming sink commits one snapshot per checkpoint, writing new files for each writer and partition, so shorter intervals mean fresher data and more, smaller files.

?? Why do Flink upserts into Iceberg slow down reads over time?
Each changed key produces equality deletes that every reader must apply until compaction rewrites the data files.

?? Name two Google Cloud ways to stream into Iceberg tables.
Any two of: Dataflow with Beam's managed Iceberg I/O, BigQuery Iceberg managed tables via the Storage Write API, Flink on Managed Service for Apache Spark or GKE, Spark Structured Streaming on Managed Service for Apache Spark.
:::

::: resources
- Docs: [Iceberg Flink writes](https://iceberg.apache.org/docs/latest/flink-writes/) · checkpoint commits, upsert mode and distribution options
- Docs: [Apache Paimon documentation](https://paimon.apache.org/docs/master/) · streaming upserts, changelog producers and compaction
- Docs: [Dataflow managed I/O for Apache Iceberg](https://cloud.google.com/dataflow/docs/guides/managed-io-iceberg) · writing Iceberg from Dataflow on Google Cloud
- Docs: [Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/iceberg-tables) · streaming with the Storage Write API and automatic optimisation
- Blog: [From Kappa architecture to Streamhouse](https://www.ververica.com/blog/from-kappa-architecture-to-streamhouse-making-lakehouses-real-time) · the streaming lakehouse idea from the Flink community
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · Akidau, Chernyak and Lax on the theory behind streaming tables
:::

### Lakehouse for Unstructured Data

::: plain
Most company data is not rows and columns: it is photos, scanned contracts, call recordings, PDFs and videos. A lakehouse for unstructured data is like a museum: the objects themselves sit in storage rooms (object storage), while a well-kept catalogue (a table) records what each object is, where it is, who may see it, and what has been learned about it, such as a caption or an embedding. You search the catalogue, then fetch only the objects you need.
:::

**The core pattern.** Store each file once in Cloud Storage, and keep a **metadata table** in the lakehouse (Iceberg, Delta or a BigQuery table) with one row per object:

| Column | Example | Purpose |
|---|---|---|
| `uri` | `gs://acme-media-eu/claims/2026/11/c-8812.jpg` | Where the object lives |
| `content_hash` | `sha256:4be1...` | Deduplication and integrity |
| `content_type`, `size_bytes` | `image/jpeg`, `2481934` | Filtering and cost planning |
| `source`, `ingested_at`, `owner` | `claims-app`, a timestamp, a team | Lineage and governance |
| `labels`, `caption`, `extracted_text` | derived by models | Search and analytics |
| `embedding` | `ARRAY<FLOAT64>` of length 768 or more | Similarity search |
| `pii_detected`, `retention_until` | `true`, a date | Policy enforcement |

The table is small, queryable with SQL, versioned like any lakehouse table, and joinable with business data (claims, customers, products). The objects stay immutable; derived fields are recomputed by pipelines when models improve.

**On Google Cloud with BigQuery.** An **object table** is a read-only BigQuery table over Cloud Storage objects, with one row per object and columns such as `uri`, `content_type`, `size`, `updated` and custom metadata, governed through a connection like other Lakehouse tables:

```sql
CREATE EXTERNAL TABLE media.claim_photos
WITH CONNECTION `acme-prod.eu.lake_conn`
OPTIONS (
  object_metadata = 'SIMPLE',
  uris = ['gs://acme-media-eu/claims/*'],
  max_staleness = INTERVAL 1 DAY,
  metadata_cache_mode = 'AUTOMATIC'
);

-- multimodal embeddings with a remote model on Gemini Enterprise Agent Platform
CREATE OR REPLACE MODEL media.mm_embed
  REMOTE WITH CONNECTION `acme-prod.eu.lake_conn`
  OPTIONS (ENDPOINT = 'multimodalembedding@001');

CREATE OR REPLACE TABLE media.claim_photo_vectors AS
SELECT uri, ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(MODEL media.mm_embed, TABLE media.claim_photos);
```

From there, `CREATE VECTOR INDEX` and `VECTOR_SEARCH` find similar images, and the generative AI functions can caption, classify or extract fields from documents and images using Gemini models (the BigQuery in depth section covers these functions). Row-level security on the object table controls which objects each user can even see, and signed URLs give applications temporary access to the files themselves.

**Open-source options.**

- **Iceberg or Delta metadata tables** built by Spark jobs, with embeddings computed on Managed Service for Apache Spark or on GPUs in Gemini Enterprise Agent Platform.
- **Lance and LanceDB** (see Lance and AI-native columnar formats) store embeddings, metadata and even the blobs themselves in one versioned dataset with vector indexes, suited to ML training and retrieval.
- **Document processing**: OCR and layout parsing (for example Document AI on Google Cloud) turn PDFs into text and fields stored in the metadata table.

**A typical pipeline.**

1. Objects land in a bucket (uploads, transfers, application writes), named by content hash or a stable id.
2. An event (Cloud Storage notification to Pub/Sub, or Eventarc) or a scheduled job registers new objects in the metadata table.
3. Enrichment jobs compute derived columns: dimensions, extracted text, PII scans (Sensitive Data Protection), captions, embeddings.
4. Consumers query the table: analysts by SQL, applications by vector search, ML training by selecting a versioned snapshot of URIs and labels.

> **Pitfall:** Deleting a row from the metadata table does not delete the object, and deleting the object leaves a dangling row. Treat the table as the source of truth and run a reconciliation job that deletes unreferenced objects (after a grace period) and flags rows whose objects are missing. Privacy deletions must remove both.

> **Pitfall:** Do not store large blobs inside Parquet columns of a general-purpose table. Every scan of other columns gets slower and files balloon. Keep blobs in object storage (or in a format designed for them, such as Lance) and reference them by URI.

::: deeper
**Reproducible training sets.** A model trained on "all images labelled defective" is not reproducible if labels and objects change. Pin training data to a table snapshot (Iceberg tag or version) and keep objects immutable (new versions get new names), so a snapshot fully determines the training set.

**Embedding versioning.** Embeddings from different model versions are not comparable. Store the model name and version with each vector, and re-embed in bulk when you change models; vector indexes must be rebuilt.

**Costs.** Model inference over millions of objects dominates cost. Filter first using cheap metadata (type, size, date, source), process only new or changed objects (incremental by `updated` or hash), and cache derived results in the table.

**Governance.** Unstructured data hides personal data in unexpected places: faces, number plates, signatures, spoken names. Classify at ingestion, store classification results as columns, enforce access through the table (row-level policies) and keep raw buckets closed to people.

> **Interview:** "Design a searchable archive of ten million support call recordings." Recordings in Cloud Storage named by id; a metadata table with call attributes; a pipeline that transcribes (speech-to-text), redacts PII, summarises and embeds each call; BigQuery or LanceDB for SQL and vector search; access control on the table; and reconciliation and retention jobs for deletions.
:::

::: check
?? Why keep unstructured objects in object storage and their metadata in a table?
Objects are large and immutable, so object storage holds them cheaply; the table is small, queryable, versioned and joinable, holding everything you search and govern by.

?? What is a BigQuery object table?
A read-only table over Cloud Storage objects with one row per object and its metadata, which can be governed with BigQuery security and processed with remote AI models.

?? Why store the embedding model's version with each vector?
Vectors from different models (or versions) are not comparable, so you must know which model produced each one and re-embed consistently when models change.
:::

::: resources
- Docs: [Introduction to object tables](https://cloud.google.com/bigquery/docs/object-table-introduction) · BigQuery tables over unstructured objects in Cloud Storage
- Docs: [Generate multimodal embeddings](https://cloud.google.com/bigquery/docs/generate-multimodal-embeddings) · embeddings for images and video from BigQuery
- Docs: [Introduction to vector search](https://cloud.google.com/bigquery/docs/vector-search-intro) · vector indexes and `VECTOR_SEARCH` in BigQuery
- Docs: [LanceDB documentation](https://lancedb.github.io/lancedb/) · a multimodal, versioned vector store built on Lance
- Docs: [Pub/Sub notifications for Cloud Storage](https://cloud.google.com/storage/docs/pubsub-notifications) · triggering registration and enrichment when objects arrive
- Docs: [Sensitive Data Protection overview](https://cloud.google.com/sensitive-data-protection/docs/sensitive-data-protection-overview) · finding and redacting personal data in text and images
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Lakehouse layers | Object storage, file format, table format, catalog (plus governance) |
| Default stack on Google Cloud | Cloud Storage + Parquet + Iceberg + Lakehouse runtime catalog, queried by BigQuery and Spark |
| Safe concurrent create on Cloud Storage | Write with precondition `ifGenerationMatch=0` |
| Target file size | 128 MiB to 1 GiB; Iceberg default 512 MiB |
| Inspect a Parquet footer | `pq.ParquetFile(path).metadata` or DuckDB `parquet_metadata(path)` |
| Analytics storage format | Parquet; Avro for messages and landing; Arrow in memory; Lance for ML retrieval |
| Avro schema change | New fields need defaults; enforce compatibility in the registry |
| Iceberg partitioning | `PARTITIONED BY (days(ts), bucket(16, id))`, filter on the real column |
| Iceberg time travel | `SELECT ... FROM t TIMESTAMP AS OF '...'` or `VERSION AS OF <id or tag>` |
| Delta state | Latest checkpoint plus later JSON commits in `_delta_log/` |
| Heavy CDC upserts | Hudi MOR, Paimon primary-key table, or merge-on-read with frequent compaction |
| Compact Iceberg | `CALL lake.system.rewrite_data_files(table => 'db.t', where => ...)` |
| Compact Delta | `OPTIMIZE t` (add `ZORDER BY (a, b)` or use `CLUSTER BY`) |
| Multi-column skipping | Z-order, Hilbert or liquid clustering on 1 to 4 filter columns |
| Expire history | Iceberg `expire_snapshots`, Delta `VACUUM`, Hudi cleaner; derive retention from consumers and law |
| Orphan cleanup | `remove_orphan_files` with a threshold longer than the longest write job |
| Pruning not working | Check pushed filters, types, clustering, statistics config, metadata size |
| Streaming into a lake | Commit per checkpoint (1 to 5 minutes), compact closed partitions |
| Cross-format readers | Delta UniForm, Paimon Iceberg metadata, Apache XTable; one writable format |
| Unstructured data | Objects in Cloud Storage, metadata and embeddings in a table (BigQuery object tables) |

### Practice

::: try
**1. Count the files.** A Spark job writes a day of clickstream data (300 GiB) into an Iceberg table partitioned by `day(event_ts)` and `country` (60 countries). It runs with 1,000 shuffle partitions and no write distribution. Roughly how many files can it create per day, how big would they be, and what would you change?
:::

?? Answer to 1
Without distribution, each of the 1,000 tasks can write a file for each of the 60 country partitions it sees: up to 60,000 files of about 5 MiB each. Set `write.distribution-mode` to `hash` so each partition is written by few tasks (about 60 to a few hundred files of 512 MiB or less), or drop `country` from the partition spec and cluster by it instead (300 GiB / 512 MiB = about 600 files per day).

::: try
**2. Will it skip?** A table has three files with `amount` statistics (min, max): A (1, 50), B (20, 400), C (300, 9000). Which files must a reader open for `WHERE amount > 350`, for `WHERE amount = 25`, and for `WHERE amount < 0`? Extend the `may_match` function from the Iceberg topic to check.
:::

?? A solution to 2

```python
files = {"A": (1, 50), "B": (20, 400), "C": (300, 9000)}

def may_match(bounds, lo, hi):
    mn, mx = bounds
    return mx >= lo and mn <= hi

inf = float("inf")
print([f for f, b in files.items() if may_match(b, 351, inf)])   # ['B', 'C']
print([f for f, b in files.items() if may_match(b, 25, 25)])     # ['A', 'B']
print([f for f, b in files.items() if may_match(b, -inf, -1)])   # []
```

::: try
**3. Choose a format.** For each case, choose a table format and say why: (a) a BigQuery-centred analytics platform where Spark jobs also write some tables; (b) a database CDC mirror receiving 50,000 updates per second, read in batch by Spark; (c) a Flink team building real-time aggregates that downstream Flink jobs consume as change streams; (d) a three-person team running everything in DuckDB with frequent small writes.
:::

?? Answer to 3
(a) Iceberg, with Iceberg managed tables for BigQuery-written data and the Lakehouse runtime catalog for Spark-written tables. (b) Hudi MOR (record-level index, async compaction, incremental queries) or a Paimon primary-key table; Iceberg with merge-on-read is possible but needs aggressive compaction. (c) Paimon with an aggregation merge engine and a changelog producer. (d) DuckLake, with the catalog in PostgreSQL if several people write at once.

::: try
**4. Retention plan.** A 20 TiB Iceberg table is rewritten about 1 percent per day by `MERGE`. An incremental consumer runs weekly, the compliance team must complete deletions within 30 days, and finance needs the exact table state at each month end for seven years. Propose retention settings and the maintenance jobs.
:::

?? A solution to 4
Keep 14 days of snapshots (covers the weekly consumer with margin and is well inside the 30-day deletion deadline); this retains up to about 14 x 200 GiB = 2.8 TiB extra. Run `expire_snapshots(older_than => now - 14 days, retain_last => 10)` daily, `remove_orphan_files` weekly with the default 3-day threshold, and `rewrite_manifests` weekly. For month ends, create a tag such as `fy2026-11-close` with `RETAIN 2555 DAYS`, but note that tagged snapshots keep deleted personal data: either apply deletions by rewriting the tagged state into an archive table, or agree with compliance that month-end snapshots are archived in a separate, access-controlled copy that also receives deletions.

### What comes next

You can now design, run and tune a lakehouse on Google Cloud: object storage underneath, Parquet and its relatives inside the files, Iceberg and the other table formats on top, and catalogs and maintenance around them. The next section, **Databases: OLTP, NoSQL and specialised**, turns to the operational systems that produce most of this data: relational databases, key-value and document stores, wide-column, time-series and search systems, and how their designs differ from the analytical storage you have just studied.
