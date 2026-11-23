This section is about the rules and the machinery that make data trustworthy and safe to use: knowing what data you have, where it came from, whether it is correct, who may see it, and what the law says you must do with it. It is written for data engineers who already build pipelines and now have to answer questions such as "which dashboards break if we drop this column?", "who read the customer table last month?" and "a user asked us to delete their data: where is it?". It assumes you know SQL and data modeling (SQL for data engineering, Data modeling), understand lakehouse table formats (Data lakes and lakehouses), and have met BigQuery security, IAM and Knowledge Catalog in BigQuery in depth and Operational databases, AI and governance on Google Cloud. DataOps and platform engineering, the previous section, gave you CI/CD, testing and infrastructure as code; this section uses them to turn policies into code.

**What you will be able to do afterwards**

- Design a metadata layer: a catalog that people can search, a business glossary, ownership, and lineage that is collected automatically from Airflow, Spark, dbt and BigQuery.
- Explain how DataHub and Unity Catalog work inside, and how Knowledge Catalog (formerly Dataplex Universal Catalog) covers the same ground on Google Cloud.
- Build master data: match records that describe the same customer and merge them into a golden record.
- Define data quality as measurable rules, run them with dbt, Great Expectations, Soda or Knowledge Catalog data quality scans, and add observability that catches the problems no rule anticipated.
- Implement access control with roles and attributes: row filters, column masks, tag-based policies, Apache Ranger and policy as code with OPA, plus audit logs and zero-trust service access.
- Handle personal data: detect it, mask or tokenise it, erase it on request even in an immutable lakehouse, respect consent and retention rules, share data safely and run privacy-preserving analysis in clean rooms.
- Explain how governance works in a data mesh, where many domain teams own their data.

**The big picture.** Think of a large public library. The **catalog** tells you what books exist, where they sit and what they are about. The **provenance** notes in rare books (lineage) record where each came from. **Conservation staff** (quality) check that pages are not missing. **Access rules** decide who may enter the rare books room, and a **visitor log** (audit) records who did. Some archives hold private letters that may only be read with names blacked out (privacy), must be destroyed after a set time (retention) or returned to the family on request (erasure). Data governance is all of this for data. The old view treated it as a committee that writes policies in documents. The modern view, the one this section teaches, treats governance as **engineering**: metadata collected automatically, rules written as code, checks running in pipelines, and policies enforced by the query engine itself, so that doing the right thing is the default rather than an extra chore.

[[fig:governance_layers]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Metadata

Everything else in governance depends on **metadata**, data about data: table and column names, types, descriptions, owners, tags, freshness, quality scores, who uses what, and how each table was produced. You cannot protect personal data you cannot find, you cannot assess the impact of a change without lineage, and you cannot hold anyone accountable for a table that has no owner.

This group builds the metadata layer from the bottom up. It starts with catalogs and discovery (what exists and what it means), then lineage (how data flows), then looks inside two metadata platforms you will meet often: DataHub, the most widely used open-source one, and Unity Catalog, the governance layer of the Databricks lakehouse. It ends with master data management, which deals with a different kind of metadata problem: several systems describing the same real-world customer or product in conflicting ways.

### Data Catalog & Discovery

::: plain
A **data catalog** is a search engine and a labelled map for a company's data. Instead of asking around on chat "does anyone know where the real revenue table is?", you search the catalog, see which tables match, read their descriptions, check who owns them and how fresh they are, and see which ones colleagues actually use. Like a library catalog, it does not hold the books themselves; it holds the cards that describe them.
:::

A catalog stores three kinds of metadata:

| Kind | Examples | Where it comes from |
|---|---|---|
| Technical | Schemas, types, partitioning, row counts, file locations | Harvested automatically from databases, warehouses, lakes, BI tools |
| Operational | Freshness, job runs, query counts, quality results, lineage | Harvested from logs, orchestrators and engines |
| Business | Descriptions, glossary terms, owners, domains, classifications such as "PII" | Written by people, suggested by tools (increasingly by AI), then reviewed |

The first rule of a useful catalog is **automation**. A catalog filled by hand is out of date within a month. Every serious tool ships **connectors** (also called crawlers or ingestion sources) that connect to BigQuery, PostgreSQL, Kafka, dbt, Airflow, Looker and so on, and pull schemas, statistics and lineage on a schedule. People then add what machines cannot know: what a table means, which one is the trusted version, and who to ask.

**The main tools.**

- **DataHub** (open source, created at LinkedIn, now developed mainly by the company behind its hosted version): event-driven metadata platform with search, lineage, glossary, domains, data products and an actions framework. Covered in its own topic below.
- **OpenMetadata** (open source): a similar platform built around a single, strongly typed metadata schema and an API-first design, with built-in data quality tests and profiling.
- **Apache Atlas**: the governance catalog of the Hadoop world. It models metadata as typed entities and classifications (tags) that propagate along lineage, and feeds tag-based policies to Apache Ranger. You meet it in on-premises Hadoop estates.
- **Collibra** and **Alation** (commercial): enterprise catalogs strong on business glossaries, stewardship workflows (who approves a definition), policy management and data governance programmes. Alation started from query-log analysis to show which tables people really use.
- **Knowledge Catalog (formerly Dataplex Universal Catalog; Data Catalog before that)**: Google Cloud's catalog. It harvests metadata automatically from BigQuery, Cloud SQL, Spanner, Pub/Sub, Bigtable, Cloud Storage, Gemini Enterprise Agent Platform (formerly Vertex AI) and more, and adds business glossaries, data profiling, data quality scans, lineage and data products.

**How Knowledge Catalog models metadata.** Every asset is an **entry** (a BigQuery table, a Pub/Sub topic) that lives in an **entry group** and has an **entry type**. Metadata is attached as **aspects**, each an instance of an **aspect type** that defines a schema. Google provides system aspect types (schema, storage, usage); you define your own for business metadata. For example, a `data-contract` aspect type might require `owner_email`, `tier` (gold, silver, bronze) and `contains_pii` fields. Because aspects are typed, you can search on them ("all gold-tier tables owned by finance that contain PII") and validate them in CI.

**Discovery in practice.** Even without a catalog, the warehouse already holds a lot of technical metadata. In BigQuery the `INFORMATION_SCHEMA` views answer many discovery questions, and a catalog builds on the same information:

```sql
-- Which tables in the region have a column whose name suggests an email address?
SELECT table_schema, table_name, column_name, data_type
FROM `region-eu`.INFORMATION_SCHEMA.COLUMNS
WHERE LOWER(column_name) LIKE '%email%'
ORDER BY table_schema, table_name;

-- Which tables are actually used? Count queries per referenced table over 30 days.
SELECT ref.dataset_id, ref.table_id, COUNT(*) AS queries,
       COUNT(DISTINCT user_email) AS users
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
     UNNEST(referenced_tables) AS ref
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
GROUP BY 1, 2
ORDER BY queries DESC;
```

The second query is the core of "popularity" ranking in catalog search: a table queried by 200 people a week is a better default answer than an abandoned copy with the same name.

**A business glossary** defines terms ("active customer", "net revenue") once, in business language, and links each term to the tables and columns that implement it. It is where governance meets modeling: if three teams compute "active customer" three ways, the glossary is where you notice and settle it.

> **Pitfall:** Launching a catalog as a big-bang project with hundreds of empty description fields. People search once, find nothing useful and never return. Start with the 50 most-queried tables, give each an owner and a real description, mark the trusted ones, and grow from there.

> **Tip:** Put descriptions where engineers already work. dbt `description:` fields, BigQuery column descriptions and Terraform labels are all harvested by catalogs. A description written next to the code is updated in the same pull request as the code.

::: deeper
**Push versus pull ingestion.** Most catalogs **pull**: a scheduled crawler connects to a source and reads its metadata. That is simple but always somewhat stale and puts load on the source. **Push** means the source (or the pipeline) emits a metadata event when something changes, for example an Airflow task emitting lineage at run time or a schema change published to Kafka. Push is fresher and cheaper at scale, which is why DataHub's design is event-driven. Large platforms use both: push for operational events, pull for periodic full syncs that repair drift.

**Metadata as a graph.** Underneath the search box every modern catalog is a graph: datasets, columns, jobs, dashboards, people, glossary terms and domains are nodes; "produces", "owns", "is tagged with" and "consumes" are edges. Search is served from an inverted index (Elasticsearch or OpenSearch in the open-source tools), but questions like "everything downstream of this column" are graph traversals.

**Active metadata.** The term describes metadata that triggers actions instead of waiting to be read: a new column tagged PII automatically gets a masking policy, a failed quality check posts to the owner's channel and marks the table "untrusted" in the catalog, a table unused for 180 days is proposed for deletion. This is where catalogs and DataOps meet.

**AI and catalogs.** Catalogs now use language models to draft descriptions and glossary entries from schemas, sample values and query logs, and expose metadata to AI agents (Knowledge Catalog offers this as context for Gemini and through an MCP server). Generated descriptions save time but must be reviewed: a confident wrong description is worse than none.

> **Interview:** "How would you make a data catalog that people actually use?" Automate technical and operational metadata; seed business metadata for the most-used assets first; rank search by usage and certification; require an owner for every production table (enforced in CI); show freshness and quality next to each table; and integrate the catalog with the tools people already use (BI, SQL editor, chat).
:::

::: check
?? Name the three kinds of metadata a catalog holds and give one example of each.
Technical (schemas and types), operational (freshness, job runs, query counts, lineage) and business (descriptions, owners, glossary terms, PII classification).

?? Why is a manually maintained catalog almost always out of date?
Schemas, tables and pipelines change every week. Without connectors that harvest technical metadata automatically, and descriptions kept next to the code, nobody has time to keep thousands of entries current.

?? In Knowledge Catalog, what is an aspect?
A typed piece of metadata attached to an entry, defined by an aspect type (a schema). System aspects hold schema and storage details; custom aspects hold business metadata such as owner, tier or PII flags.
:::

::: resources
- Docs: [Knowledge Catalog overview](https://docs.cloud.google.com/knowledge-catalog/docs/introduction) · Google Cloud's catalog: entries, aspects, glossaries, search and lineage
- Docs: [DataHub documentation](https://docs.datahub.com/docs/) · the open-source metadata platform: features, connectors and concepts
- Docs: [OpenMetadata documentation](https://docs.open-metadata.org/) · an alternative open-source catalog with a unified metadata schema
- Docs: [Apache Atlas](https://atlas.apache.org/) · the Hadoop-era catalog with typed entities and propagating classifications
- Book: [Data Management at Scale](https://www.oreilly.com/library/view/data-management-at/9781492054771/) · Strengholt; metadata, catalogs and governance in large organizations
- Blog: [DataHub: popular metadata architectures explained](https://www.linkedin.com/blog/engineering/data-management/datahub-popular-metadata-architectures-explained) · LinkedIn's explanation of the three generations of metadata architecture
:::

### Data Lineage — Column & Table Level

::: plain
**Lineage** is the family tree of a piece of data: which sources it came from, which jobs transformed it, and which tables, dashboards and models use it afterwards. Imagine a river map showing every stream that feeds a lake and every town that drinks from it. If a stream is polluted, the map tells you which towns to warn; if a town's water tastes odd, it tells you which streams to check.
:::

Lineage answers two everyday questions:

- **Impact analysis (downstream):** "If I rename `orders.amount`, what breaks?" Walk the graph forward from the column to every table, view, dashboard and model that uses it, and notify their owners before you merge.
- **Root cause analysis (upstream):** "Why is yesterday's revenue wrong?" Walk the graph backward to find the job or source that changed.

It also serves compliance. Regulations and auditors ask you to show where a reported number came from (BCBS 239 in banking, covered in Industry patterns), and privacy laws need you to know everywhere personal data flows so you can protect, report on and erase it.

**Table-level and column-level.** Table-level lineage says "`marts.revenue` is built from `staging.orders` and `staging.fx_rates`". Column-level lineage says "`marts.revenue.amount_eur` is computed from `staging.orders.amount` and `staging.fx_rates.rate`". Column-level lineage is much more useful for impact analysis (most changes touch one column, and most tables use only some columns of their inputs) and for PII tracking, but much harder to collect, because it requires understanding the SQL or code, not just which tables a job read and wrote.

**How lineage is collected.** There are three approaches, usually combined:

1. **Parsing**: read the SQL (dbt models, view definitions, query logs) and derive inputs and outputs from it. SQL parsers such as sqlglot can resolve column-level lineage through CTEs and joins. Works only for SQL, and only if you can resolve `SELECT *` against the schema.
2. **Runtime instrumentation**: the engine or orchestrator reports what it actually read and wrote while the job runs. This works for Spark, Python and anything with an integration, and it reflects reality rather than code.
3. **Query logs**: the warehouse records inputs and outputs of every job. BigQuery does this natively, which is how Knowledge Catalog builds lineage automatically.

**OpenLineage** is the open standard for runtime lineage, a Linux Foundation (LF AI & Data) project. It defines a JSON event with three core objects: a **Run** (one execution, identified by a UUID), a **Job** (the recurring thing being run, with a namespace and name) and **Datasets** (inputs and outputs, also with namespace and name). Extra information is attached as **facets**: schema, SQL text, data quality metrics, and a `columnLineage` facet on outputs. Producers emit events at `START`, `RUNNING`, `COMPLETE`, `FAIL` or `ABORT`. A minimal event looks like this:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-11-20T02:14:09Z",
  "producer": "https://example.com/my-pipeline",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
  "run": { "runId": "0193f4a2-6c1e-7b8d-9a00-5f0e1d2c3b4a" },
  "job": { "namespace": "composer-prod", "name": "daily_revenue.build_mart" },
  "inputs":  [ { "namespace": "bigquery", "name": "shop-prod.staging.orders" } ],
  "outputs": [ { "namespace": "bigquery", "name": "shop-prod.marts.revenue",
    "facets": { "columnLineage": { "fields": {
      "amount_eur": { "inputFields": [
        { "namespace": "bigquery", "name": "shop-prod.staging.orders", "field": "amount" } ] }
    } } } } ]
}
```

(A real `columnLineage` facet also carries `_producer` and `_schemaURL` fields and, in recent versions, a `transformations` list that says whether the input was used directly or only for filtering or grouping.)

Integrations exist for Airflow (the `apache-airflow-providers-openlineage` provider), Spark (the `openlineage-spark` listener), dbt (the `dbt-ol` wrapper), Flink and others. Events go to a backend over HTTP or Kafka:

- **Marquez** is the reference OpenLineage server: it stores events in PostgreSQL and serves a lineage API and UI.
- **Knowledge Catalog lineage** (the Data Lineage API) records lineage automatically for BigQuery jobs and for Managed Service for Apache Airflow (formerly Cloud Composer), Managed Service for Apache Spark (formerly Dataproc) and Dataflow integrations, and also accepts OpenLineage events through its `ProcessOpenLineageRunEvent` method, so custom pipelines can add themselves to the same graph.
- **DataHub**, **OpenMetadata** and commercial catalogs also consume OpenLineage.

[[fig:governance_lineage]]

Turning on Spark lineage is configuration, not code. For a Spark batch you add the listener and point it at your backend:

```bash
gcloud dataproc batches submit pyspark job.py --region=europe-west1 \
  --properties=spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener,\
spark.openlineage.transport.type=http,\
spark.openlineage.transport.url=http://marquez.internal:5000,\
spark.openlineage.namespace=spark-prod
```

(The listener jar must also be on the classpath, for example through `--jars` or a custom image.)

> **Pitfall:** Lineage with gaps is trusted as if it were complete. If one Python job that copies data through a bucket is not instrumented, the graph shows two unrelated islands and impact analysis silently misses everything downstream of the gap. Track lineage coverage (the share of production jobs that emit lineage) as a metric.

> **Tip:** Use lineage in CI. Before merging a change that drops or renames a column, a check can query the lineage graph for downstream consumers and post them in the pull request. dbt's `state:modified+` selector does a simple version of this inside one dbt project.

::: deeper
**Coverage and limits on Google Cloud.** Native BigQuery column-level lineage covers SQL transformations (queries, `CREATE TABLE AS SELECT`, `MERGE`), but not load jobs, copy jobs or routines, and it stops at external tables. If a single job would create more than 1,500 column-level links, only table-level lineage is recorded. OpenLineage events sent to the Data Lineage API contribute column-level links only through the standard `columnLineage` facet with its `transformations` field. The Data Lineage API is billed under a Knowledge Catalog premium processing SKU, so check whether it is enabled in projects that do not need it.

**Static versus runtime lineage disagree.** Parsed SQL tells you what the code could read; runtime lineage tells you what one run did read. A conditional branch, a dynamic table name or a `SELECT *` over an evolving schema makes them differ. The best systems merge both and keep the run identifier, so you can ask "which run of which job wrote the bad partition?".

**Lineage granularity beyond columns.** Some regulated workloads need **row-level** or **value-level** provenance ("which source records contributed to this number?"). That is not practical as a metadata graph; it is done by carrying source identifiers through the pipeline (for example an array of source record keys, or a batch identifier on every row) so that provenance is part of the data.

**Facets as an extension point.** OpenLineage keeps the core small and lets producers add custom facets (prefixed with their own name). This is how quality results, ownership or cost can ride along with lineage events without changing the spec. Consumers ignore facets they do not understand.

> **Interview:** "A finance dashboard shows the wrong number this morning. How do you use lineage?" Walk upstream from the dashboard's table to its inputs, check freshness and quality results on each, find the first node whose data changed unexpectedly (a late source, a schema change, a failed run that was partially written), fix it, then walk downstream from that node to list every other asset affected and tell their owners.
:::

::: check
?? Why is column-level lineage more useful than table-level lineage for impact analysis?
Most changes touch a single column and most consumers use only some columns of a table. Table-level lineage flags every downstream table; column-level lineage flags only those that actually use the changed column.

?? What are the three core objects in an OpenLineage run event?
The Run (one execution, with a run ID), the Job (what is being run, by namespace and name) and Datasets (inputs and outputs). Details such as schema and column lineage are attached as facets.

?? A Python job copies files from one bucket to another and is not instrumented. What happens to the lineage graph?
The graph has a gap: upstream and downstream appear unrelated, so impact and root-cause analysis silently miss everything that flows through that job.
:::

::: resources
- Spec: [OpenLineage documentation](https://openlineage.io/docs/) · the object model, facets, integrations and client libraries
- Repo: [OpenLineage on GitHub](https://github.com/OpenLineage/OpenLineage) · the JSON schema, the Spark listener and other integrations
- Docs: [Marquez](https://marquezproject.ai/) · the reference OpenLineage server and UI
- Docs: [About data lineage in Knowledge Catalog](https://docs.cloud.google.com/knowledge-catalog/docs/about-data-lineage) · automatic lineage for BigQuery and other services, column-level limits
- Docs: [OpenLineage mapping in Knowledge Catalog](https://docs.cloud.google.com/knowledge-catalog/docs/openlineage-mapping) · how OpenLineage events map onto the Data Lineage API
- Repo: [sqlglot](https://github.com/tobymao/sqlglot) · a SQL parser with a column-lineage module, useful for static lineage
:::

### DataHub — Metadata Platform Internals

::: plain
DataHub is an open-source metadata platform: the engine behind a catalog. You can picture it as a newsroom. Reporters (connectors and pipelines) file short reports about every table, job and dashboard. An editor (the metadata service) checks each report and files it in the archive (the database). Every accepted report is also announced on the wire (Kafka), so the search desk (the search index) and the map room (the lineage graph) update themselves straight away.
:::

DataHub was built at LinkedIn to replace an earlier catalog (WhereHows) and open-sourced in 2020. Its design choices answer one question: how do you keep metadata for hundreds of thousands of assets fresh, searchable and extensible, when it is written by hundreds of different systems?

**The metadata model.** Everything is an **entity** identified by a **URN** (uniform resource name), for example:

```text
urn:li:dataset:(urn:li:dataPlatform:bigquery,shop-prod.marts.orders,PROD)
urn:li:corpuser:asha
urn:li:glossaryTerm:NetRevenue
```

An entity's metadata is split into **aspects**: small, independently versioned records such as `datasetProperties` (description, custom properties), `schemaMetadata` (fields and types), `ownership`, `globalTags`, `glossaryTerms`, `upstreamLineage` and `status`. Aspects are defined in PDL (Pegasus schema language) and versioned, so a change to ownership does not rewrite the schema aspect, and every aspect has a history. **Relationships** (edges such as `DownstreamOf` or `OwnedBy`) are declared inside aspects, which is how the graph is built.

**The components.**

- **GMS (Generalized Metadata Service)**: the Java backend. It validates writes, stores aspects, serves the REST and GraphQL APIs, and emits change events.
- **Primary store**: a relational database (MySQL or PostgreSQL) holding every version of every aspect. It is the source of truth.
- **Kafka**: the event backbone. Writes can arrive as **MCPs** (Metadata Change Proposals) on Kafka or via REST. After GMS commits a change, it emits an **MCL** (Metadata Change Log) event describing it. In older documentation these were called **MCE** (Metadata Change Event) and **MAE** (Metadata Audit Event); the MCP and MCL forms replaced them.
- **Consumers**: the MCE consumer turns proposals from Kafka into writes; the MAE consumer reads change logs and updates the **search index** (Elasticsearch or OpenSearch) and the **graph index** (Elasticsearch by default, or Neo4j). They can run inside GMS or as separate jobs for scale.
- **Frontend**: a React web app behind a small server, using GraphQL.
- **Actions framework**: subscribes to change events and runs code, for example posting to chat when a table is tagged PII or opening a ticket when a deprecated table gains a new consumer.

[[fig:governance_datahub]]

Because the primary store is the source of truth and the indexes are derived from the change log, DataHub can **rebuild** search and graph indexes from the database at any time, the same pattern as event sourcing.

**Ingestion.** The Python package `acryl-datahub` provides the `datahub` CLI and dozens of sources. You describe a run in a **recipe**:

```yaml
# bigquery_recipe.yml : run with  datahub ingest -c bigquery_recipe.yml
source:
  type: bigquery
  config:
    project_ids: ["shop-prod"]
    include_table_lineage: true
    profiling:
      enabled: false
sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
```

Each source extracts metadata and turns it into MCPs; the sink sends them to GMS. Recipes run on a schedule (from Airflow or the UI), and **stateful ingestion** remembers what was seen last time, so tables deleted at the source are soft-deleted in DataHub instead of lingering.

**Programmatic ingestion.** Pipelines can push metadata directly with the SDK, for example to record ownership and a description when a Python job creates a table:

```python
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetPropertiesClass

emitter = DatahubRestEmitter(gms_server="http://datahub-gms:8080")
urn = make_dataset_urn(platform="bigquery", name="shop-prod.marts.orders", env="PROD")

props = DatasetPropertiesClass(
    description="One row per order, net of refunds. Owner: commerce data team.",
    customProperties={"tier": "gold", "contains_pii": "false"},
)
emitter.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=props))
```

Note that this replaces the whole `datasetProperties` aspect. To change one field without overwriting others, use a patch (the SDK has patch builders) or read, modify and write.

**Lineage in DataHub** comes from three places: sources that parse SQL or read query logs (BigQuery, Snowflake, dbt), OpenLineage events, and `upstreamLineage` aspects emitted by your own code. Column-level lineage is stored as fine-grained lineage inside that aspect.

> **Pitfall:** Running DataHub's quickstart Docker Compose in production. It bundles single-node Kafka, MySQL and Elasticsearch with no backups. Use the Helm charts on GKE with Cloud SQL as the primary store, Google Managed Service for Apache Kafka and a properly sized search cluster, or use the hosted product.

::: deeper
**Why aspects instead of one document per entity?** Different systems own different facts: the BigQuery connector owns the schema, the HR sync owns ownership, people own descriptions. With per-aspect writes, each writer touches only its aspect, so they cannot overwrite each other, and each aspect has its own version history and audit trail ("who changed this description, and when?").

**Consistency model.** GMS commits to the primary store first and then emits the MCL, so search and graph are eventually consistent: a new description may take a moment to appear in search. Writes can be **synchronous** (REST to GMS) or **asynchronous** (MCP on Kafka), which absorbs bursts from large ingestion runs.

**Timeseries aspects.** Operational metadata such as dataset profiles (row counts, null fractions over time), usage statistics and assertion results are stored as **timeseries aspects** in the search store rather than in the primary database, because they grow without bound and are queried by time range.

**Extending the model.** You can add new aspects or entities without forking: define them in a model extension, register them, and the APIs, search and UI pick them up. **Structured properties** are the lighter-weight option: typed key-value fields attached to entities and searchable, similar to Knowledge Catalog's custom aspect types.

**Operating at scale.** The heaviest load is usually reindexing and large ingestion runs. Keep MAE consumers separate from GMS so search updates do not slow API calls, partition the Kafka topics, and monitor consumer lag the same way you would for any Kafka application (Streaming systems).

> **Interview:** "Why is DataHub built around Kafka?" To decouple writers from the many derived views (search, graph, notifications). Producers write once; every consumer updates itself from the change log; indexes can be rebuilt from the source of truth; and bursts of metadata are buffered instead of overloading the service.
:::

::: check
?? What is the difference between an MCP and an MCL in DataHub?
An MCP (Metadata Change Proposal) is a request to change an aspect, sent to GMS by REST or Kafka. An MCL (Metadata Change Log) is the event GMS emits after it has committed the change, which consumers use to update search and graph indexes.

?? Why can DataHub rebuild its search index from scratch?
The relational primary store holds every aspect and is the source of truth. The search and graph indexes are derived from it (via change events), so they can be regenerated at any time.

?? Two systems write metadata about the same table: one sets the schema, the other sets the owner. Why do they not overwrite each other?
They write different aspects (`schemaMetadata` and `ownership`), and aspects are stored and versioned independently.
:::

::: resources
- Docs: [DataHub architecture overview](https://docs.datahub.com/docs/architecture/architecture) · components, the metadata model and the serving tier
- Docs: [DataHub metadata model](https://docs.datahub.com/docs/metadata-modeling/metadata-model) · entities, aspects, relationships and URNs
- Docs: [DataHub Python SDK](https://docs.datahub.com/docs/metadata-ingestion/as-a-library) · emitting metadata from your own code
- Repo: [DataHub on GitHub](https://github.com/datahub-project/datahub) · source, Helm charts and ingestion sources
- Blog: [DataHub: a generalized metadata search and discovery tool](https://engineering.linkedin.com/blog/2019/data-hub) · LinkedIn's original design and motivation
- Book: [Data Management at Scale](https://www.oreilly.com/library/view/data-management-at/9781492054771/) · how metadata platforms fit into an enterprise architecture
:::

### Unity Catalog — Databricks Governance

::: plain
Unity Catalog is the single rulebook and index for everything stored in a Databricks lakehouse. Think of an apartment building with one front desk: every resident (user or job) shows their badge at the same desk, whichever floor they are heading to, and the desk keeps one register of who went where. Before Unity Catalog, each Databricks workspace had its own desk with its own rules; now one desk serves them all.
:::

Databricks runs on Google Cloud, and many Google Cloud customers use it alongside BigQuery. **Unity Catalog** is its governance layer: a central metastore for tables, files, models and functions, with access control, auditing, lineage and sharing built in. Since June 2024 an **open-source Unity Catalog** project also exists under the LF AI & Data foundation; it implements the catalog API and the Apache Iceberg REST catalog interface, so other engines can use it. The commercial Databricks version has more features; this topic describes that one unless stated.

**The object model.** A **metastore** is the top-level container, one per cloud region, attached to many workspaces. Inside it is a three-level namespace:

```text
catalog.schema.object        e.g.  sales.marts.orders
```

- **Catalogs** usually map to environments or business units (`dev`, `prod`, `sales`).
- **Schemas** (databases) group related objects.
- **Objects**: tables (managed or external), views, materialized views, **volumes** (governed folders of files), functions and registered ML models.

Two more object families connect Unity Catalog to storage: a **storage credential** wraps a cloud identity (on Google Cloud, a service account) and an **external location** binds that credential to a storage path such as `gs://shop-lake/raw`. Users never get bucket permissions; Unity Catalog checks their privileges and the engine uses the credential.

**Privileges** are granted with SQL and inherited downward:

```sql
GRANT USE CATALOG ON CATALOG sales TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA  sales.marts TO `analysts`;
GRANT SELECT      ON SCHEMA  sales.marts TO `analysts`;   -- every current and future table in it
GRANT MODIFY      ON TABLE   sales.marts.orders TO `etl-service`;
```

A user needs `USE CATALOG` and `USE SCHEMA` on the containers plus `SELECT` on the object (or on a parent) to read a table. Every object has an **owner** who can grant on it.

**Row filters and column masks** are SQL functions attached to a table:

```sql
CREATE FUNCTION sales.gov.region_filter(region STRING)
RETURN is_account_group_member('global-sales') OR region = 'EU';

ALTER TABLE sales.marts.orders SET ROW FILTER sales.gov.region_filter ON (region);

CREATE FUNCTION sales.gov.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('support') THEN email
            ELSE regexp_replace(email, '^[^@]+', '***') END;

ALTER TABLE sales.marts.customers ALTER COLUMN email SET MASK sales.gov.mask_email;
```

Every query, from any workspace or compute type that supports Unity Catalog, now sees filtered rows and masked emails. Newer **attribute-based** policies (governed tags plus ABAC policies, in preview at the time of writing) let you write one policy such as "mask every column tagged `pii=email` for everyone except support" instead of attaching functions table by table.

**Audit and lineage** are recorded automatically and exposed as **system tables** you query with SQL: `system.access.audit` (who did what), `system.access.table_lineage` and `system.access.column_lineage` (captured at run time from queries, notebooks and jobs).

**Delta Sharing** is the open protocol for sharing live tables with other organizations. The provider creates a **share** (a named set of tables), adds a **recipient**, and grants access; the recipient reads the data directly from the provider's storage through short-lived pre-signed URLs, using Databricks, Spark, pandas or any client that speaks the REST protocol. No copy is made.

```sql
CREATE SHARE partner_orders;
ALTER SHARE partner_orders ADD TABLE sales.marts.orders;
CREATE RECIPIENT acme_corp;          -- gives an activation link for open sharing
GRANT SELECT ON SHARE partner_orders TO RECIPIENT acme_corp;
```

**How it compares with Google Cloud's own stack.** BigQuery plus Knowledge Catalog covers the same ground: datasets and tables with IAM, row access policies, policy tags and masking, automatic lineage, audit logs, and BigQuery sharing (formerly Analytics Hub) for cross-organization sharing. The Lakehouse runtime catalog (formerly BigLake metastore) plays the role of an open Iceberg catalog. A team using both Databricks and BigQuery has two governance planes and must keep policies consistent between them, often by generating both from the same policy definitions in code.

| Concept | Unity Catalog | BigQuery and Knowledge Catalog |
|---|---|---|
| Namespace | `catalog.schema.table` | `project.dataset.table` |
| Grants | SQL `GRANT` on securables | IAM roles on project, dataset, table |
| Row security | Row filter functions | Row access policies |
| Column security | Column masks, ABAC (preview) | Policy tags, data policies (masking) |
| Lineage | `system.access.*_lineage` tables | Knowledge Catalog lineage |
| External sharing | Delta Sharing | BigQuery sharing |

> **Pitfall:** Granting access on the storage bucket "just to make it work". Anyone with direct bucket access bypasses Unity Catalog's row filters, masks and audit. Storage should be readable only by the credentials Unity Catalog uses.

::: deeper
**Managed versus external tables.** For a managed table, Unity Catalog controls the storage location and the lifecycle: dropping the table deletes the data (after a retention period), and Databricks can run automatic optimisation. For an external table, you own the path; dropping it only removes metadata. Prefer managed tables unless another system must write the files.

**Credential vending.** When an engine outside Databricks (Spark, Trino, DuckDB) reads a Unity Catalog table through the open APIs, the catalog checks privileges and hands out a short-lived, down-scoped storage credential for just that table's path. This is the same pattern as Lakehouse runtime catalog credential vending for Iceberg and is what lets a catalog govern engines it does not run. Row filters and column masks, however, need an engine that enforces them; a raw file reader cannot.

**Upgrading from the legacy Hive metastore.** Older workspaces stored tables in a per-workspace Hive metastore (`hive_metastore` catalog). Upgrades use `SYNC` or `CREATE TABLE ... CLONE` to move tables into Unity Catalog; until then, legacy table ACLs and Unity Catalog grants coexist, which is a common source of confusion.

**Delta Sharing internals.** The protocol is a small REST API: list shares, schemas and tables, then query a table, which returns the table's current file list with pre-signed URLs. Clients read Parquet directly. Change data feed and history sharing let recipients read incremental changes. Because data never passes through a server, sharing large tables is cheap, but the provider pays egress if the recipient reads from another region or cloud.

> **Interview:** "How does a lakehouse enforce column masking when the data is just Parquet files in a bucket?" The files are not readable by users. Only the catalog's storage credential can read them, and only trusted engines that apply the catalog's policies receive that access. The engine applies the mask function during query execution, so users never see raw values.
:::

::: check
?? What privileges does a user need to `SELECT` from `sales.marts.orders` in Unity Catalog?
`USE CATALOG` on `sales`, `USE SCHEMA` on `sales.marts`, and `SELECT` on the table or on a parent (the schema or catalog).

?? How does a Delta Sharing recipient read data without the provider copying it?
The sharing server returns pre-signed, short-lived URLs to the table's current Parquet files; the recipient's client reads the files directly from the provider's storage.

?? Why must users not have direct access to the bucket behind a Unity Catalog table?
Direct access bypasses the catalog's privileges, row filters, column masks and audit logging.
:::

::: resources
- Docs: [What is Unity Catalog?](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/) · the object model, privileges and features, Google Cloud edition of the docs
- Docs: [Row filters and column masks](https://docs.databricks.com/gcp/en/tables/row-and-column-filters) · syntax, limitations and performance notes
- Spec: [Delta Sharing protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md) · the REST protocol for open sharing
- Repo: [Unity Catalog (open source)](https://github.com/unitycatalog/unitycatalog) · the LF AI & Data project with Iceberg REST support
- Blog: [Introducing Unity Catalog](https://www.databricks.com/blog/2021/05/26/introducing-databricks-unity-catalog-fine-grained-governance-for-data-and-ai-on-the-lakehouse.html) · the original announcement and motivation
- Book: *Delta Lake: The Definitive Guide* by Denny Lee, Tristen Wentling, Scott Haines and Prashanth Babu · Delta tables, sharing and governance in depth
:::

### Master Data Management (MDM)

::: plain
**Master data** is the shared list of the important "nouns" of a business: customers, products, suppliers, employees, locations. The trouble is that each system keeps its own copy: the shop has "ASHA RAO", the CRM has "Asha Rao" and billing has "Asha R.". **Master data management** finds the records that describe the same real thing, merges them into one trusted **golden record**, and keeps that record in sync. It is like merging three contact lists on your phone after switching phones twice.
:::

Without MDM, simple questions have no answer: "How many customers do we have?" depends on which system you count, and "what is this customer's lifetime value?" misses the purchases recorded under their other identities. MDM has four parts.

**1. Standardisation.** Make values comparable before comparing them: lowercase and trim names, remove punctuation, normalise phone numbers to E.164 (`+919812345678`), parse addresses into fields, map country names to ISO codes.

**2. Entity resolution (matching).** Decide which records refer to the same entity.

- **Deterministic** rules match on exact keys: same email, same tax ID, same loyalty number. Precise, but they miss records with missing or mistyped keys.
- **Probabilistic** matching combines evidence from many fields into a score. The classic model, from Fellegi and Sunter (1969), estimates for each field the probability that it agrees when two records are a true match (`m`) and when they are not (`u`). Agreement on a field adds `log2(m/u)` to the match weight and disagreement adds `log2((1-m)/(1-u))`, which is negative. A surname that agrees with `m = 0.9` and `u = 0.01` adds about 6.5; agreement on sex (`u` around 0.5) adds less than 1. Pairs above an upper threshold are matches, below a lower one are non-matches, and the band between goes to human review.

**Blocking** makes this feasible. Comparing every pair of 10 million records is about 5 × 10^13 comparisons. Instead you only compare records that share a **blocking key** (same postcode, same first letter of surname plus birth year), which cuts the work by orders of magnitude at the risk of missing matches that disagree on the key. Real systems use several blocking rules and take the union.

**3. Clustering.** Matches are pairwise, but entities are groups: if A matches B and B matches C, all three are one customer (connected components). Long chains can wrongly merge distinct people, so systems cap cluster size or require a minimum score inside the cluster.

**4. Survivorship.** Build the golden record by choosing, field by field, which value survives: the most trusted source, the most recent, the most complete, or the most frequent.

Here is the whole flow in miniature, runnable with only the standard library:

```python
from difflib import SequenceMatcher
from itertools import combinations

records = [
    {"id": "crm-1",  "src": "crm",     "name": "Asha Rao",   "email": "asha.rao@example.com", "city": "Pune",  "updated": "2026-03-01"},
    {"id": "shop-7", "src": "shop",    "name": "ASHA  RAO",  "email": "Asha.Rao@Example.com", "city": "",      "updated": "2026-05-10"},
    {"id": "bill-3", "src": "billing", "name": "Asha R.",    "email": "",                     "city": "Pune",  "updated": "2026-01-15"},
    {"id": "crm-2",  "src": "crm",     "name": "Ravi Kumar", "email": "ravi@example.com",     "city": "Delhi", "updated": "2026-02-20"},
]

def norm(r):
    return {**r, "name": " ".join(r["name"].lower().replace(".", "").split()),
            "email": r["email"].strip().lower()}

def score(a, b):
    if a["email"] and a["email"] == b["email"]:
        return 1.0                                   # deterministic rule
    s = SequenceMatcher(None, a["name"], b["name"]).ratio()
    if a["city"] and a["city"] == b["city"]:
        s += 0.1                                     # supporting evidence
    return round(min(s, 1.0), 2)

clean = [norm(r) for r in records]
# blocking: only compare records whose names start with the same letter
pairs = [(a, b) for a, b in combinations(clean, 2) if a["name"][0] == b["name"][0]]
for a, b in pairs:
    print(a["id"], b["id"], score(a, b))
# crm-1 shop-7 1.0
# crm-1 bill-3 0.96
# shop-7 bill-3 0.86

# cluster matches above a threshold (union-find)
parent = {r["id"]: r["id"] for r in clean}
def find(x):
    while parent[x] != x:
        x = parent[x]
    return x
for a, b in pairs:
    if score(a, b) >= 0.8:
        parent[find(b["id"])] = find(a["id"])

clusters = {}
for r in clean:
    clusters.setdefault(find(r["id"]), []).append(r)

trust = {"crm": 3, "billing": 2, "shop": 1}
for members in clusters.values():
    golden = {}
    for field in ("name", "email", "city"):
        candidates = [m for m in members if m[field]]
        # survivorship: most trusted source wins, then most recent
        best = max(candidates, key=lambda m: (trust[m["src"]], m["updated"]))
        golden[field] = best[field]
    print(sorted(m["id"] for m in members), golden)
# ['bill-3', 'crm-1', 'shop-7'] {'name': 'asha rao', 'email': 'asha.rao@example.com', 'city': 'Pune'}
# ['crm-2'] {'name': 'ravi kumar', 'email': 'ravi@example.com', 'city': 'Delhi'}
```

The output should also keep a **cross-reference** (which source IDs belong to which golden ID), which is as important as the golden record itself: it is how you join any source system's data to the master entity.

**MDM hub styles.**

| Style | How it works | Trade-off |
|---|---|---|
| Registry | Hub stores only the cross-reference and match results; data stays in sources | Light and fast to build; golden view assembled at query time |
| Consolidation | Hub copies source data and builds golden records for analytics | Ideal for the warehouse; sources stay inconsistent |
| Coexistence | Golden records are published back to sources | Sources converge; needs conflict handling |
| Centralised (transactional) | New master data is created in the hub first | Strongest control; biggest organizational change |

Data engineers most often build the **consolidation** style in the warehouse: match and merge in SQL or Spark, publish `dim_customer` with a stable golden key, and keep the cross-reference table.

> **Pitfall:** Overwriting golden IDs on every run. If tonight's clustering assigns new IDs, every downstream fact table and dashboard filter breaks. Keep golden IDs stable: when clusters merge, keep the oldest ID and record the merge; when they split, keep the ID on the larger part.

> **Tip:** Measure matching quality with a labelled sample: precision (of the pairs you merged, how many are truly the same) and recall (of the true matches, how many you found). Wrongly merging two customers is usually worse than missing a match, especially when the golden record drives communication or billing.

::: deeper
**Learning the weights.** Fellegi–Sunter `m` and `u` values can be estimated without labelled data using the expectation-maximisation (EM) algorithm. **Splink**, an open-source library from the UK Ministry of Justice, implements this at scale with SQL backends such as DuckDB and Spark, and produces explainable match weights per field. On Google Cloud you can run Splink on Managed Service for Apache Spark, or write the comparisons in BigQuery SQL using functions such as `EDIT_DISTANCE` and `SOUNDEX`.

**Machine learning matchers.** Supervised models (gradient boosted trees over field similarities, or embedding models over whole records) can beat rules on messy data, but need labels and are harder to explain to auditors and stewards.

**Incremental matching.** Re-clustering everything nightly does not scale and destabilises IDs. Incremental systems match only new or changed records against existing clusters (using the blocking index), and re-evaluate a cluster only when one of its members changes.

**Reference data versus master data.** Reference data is small, slowly changing code lists (currencies, country codes, product categories), often defined outside the company (ISO standards). It needs versioning and ownership but not matching. Master data needs matching because many systems create it independently.

**Governance angle.** A golden customer record concentrates personal data from many sources, so it is a prime target for access control and must be included in erasure workflows: deleting a person means deleting their golden record, their cross-references and their data in every source.

> **Interview:** "How would you deduplicate 100 million customer records?" Standardise fields, choose several blocking keys to limit comparisons, compute field similarities and a match score (deterministic rules for strong keys, probabilistic weights for the rest), cluster matches with connected components and a size cap, apply survivorship rules, keep stable golden IDs and a cross-reference table, and measure precision and recall on a labelled sample.
:::

::: check
?? What is blocking in entity resolution, and what is its risk?
Comparing only records that share a blocking key, to avoid comparing every pair. The risk is missing true matches whose blocking key differs (for example a typo in the postcode), which is why several blocking rules are combined.

?? A matches B with score 0.9 and B matches C with score 0.9, but A and C score 0.3. What happens with simple connected-components clustering?
All three are merged into one entity, because clustering follows the chain of pairwise matches. Systems guard against this with minimum within-cluster scores or cluster size limits.

?? Why must golden record IDs be stable between runs?
Fact tables, dashboards and other systems reference them. If IDs change on every run, joins and filters break and history is lost.
:::

::: resources
- Paper: [A Theory for Record Linkage](https://www.tandfonline.com/doi/abs/10.1080/01621459.1969.10501049) · Fellegi and Sunter, 1969: the probabilistic matching model still used today
- Repo: [Splink](https://github.com/moj-analytical-services/splink) · scalable, explainable probabilistic record linkage with DuckDB and Spark backends
- Article: [The mathematics of the Fellegi-Sunter model](https://moj-analytical-services.github.io/splink/topic_guides/theory/fellegi_sunter.html) · a clear, worked explanation of match weights from the Splink team
- Book: [Data Management at Scale](https://www.oreilly.com/library/view/data-management-at/9781492054771/) · master data management styles and how they fit a modern architecture
- Book: *Entity Resolution and Information Quality* by John R. Talburt · the standard textbook on matching, clustering and survivorship
- Docs: [BigQuery string functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/string_functions) · `EDIT_DISTANCE`, `SOUNDEX` and normalisation functions for matching in SQL
:::

## Quality

Metadata tells you what data exists and where it comes from. The next question is whether you can trust it. Bad data is worse than missing data, because people act on it: a duplicated order inflates revenue, a null country silently drops customers from a regional report, a late file makes yesterday look like a bad day.

This group treats quality in two complementary ways. **Data quality frameworks** check what you know to check: explicit rules written by people who understand the data ("order IDs are unique", "amounts are not negative"). **Data observability** watches for what nobody thought to check: sudden changes in volume, freshness, schema or distribution, learned from history. Mature teams use both, and wire the results back into the catalog and lineage from the previous group so that consumers see a table's health next to its description.

### Data Quality Frameworks

::: plain
A data quality framework is a set of automatic inspections for your tables, like the checks a factory makes before a product leaves the line: is anything missing, is anything the wrong size, are there duplicates, did it arrive on time? You write down what "good" means for each table as rules, a tool runs the rules every time the data changes, and a failed rule stops the line or raises an alarm before a bad number reaches a dashboard.
:::

**Quality dimensions** give a shared vocabulary for what can go wrong:

| Dimension | Question | Example rule |
|---|---|---|
| Completeness | Is required data present? | `customer_id` is never null; every store sent a file today |
| Uniqueness | Is each entity recorded once? | `order_id` is unique |
| Validity | Do values follow the format and domain? | `status` in (paid, refunded, cancelled); email matches a pattern |
| Accuracy | Do values match reality? | Totals reconcile with the finance ledger |
| Consistency | Do related data agree? | Every `orders.customer_id` exists in `customers`; sum of line items equals order total |
| Timeliness (freshness) | Is the data recent enough? | Newest `created_at` is less than 2 hours old |

Accuracy is the hardest: it needs a trusted reference (a ledger, a source system count, a manual sample). Most automated checks cover the other five.

**Every check is a query that counts bad rows.** Whatever tool you use, rules compile down to SQL like this, runnable with Python's built-in SQLite:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE orders (order_id INT, customer_id INT, amount REAL, status TEXT, created_at TEXT);
INSERT INTO orders VALUES
 (1, 10, 40.0,  'paid',      '2026-11-20 09:00'),
 (2, 11, 15.5,  'paid',      '2026-11-20 09:05'),
 (2, 11, 15.5,  'paid',      '2026-11-20 09:05'),
 (3, NULL, 22.0, 'refunded', '2026-11-20 10:00'),
 (4, 12, -5.0,  'paid',      '2026-11-20 11:00'),
 (5, 13, 60.0,  'shipped',   '2026-11-20 12:00');
""")

checks = {
    "completeness: customer_id not null":
        "SELECT COUNT(*) FROM orders WHERE customer_id IS NULL",
    "uniqueness: order_id":
        "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM orders",
    "validity: amount >= 0":
        "SELECT COUNT(*) FROM orders WHERE amount < 0",
    "validity: status in set":
        "SELECT COUNT(*) FROM orders WHERE status NOT IN ('paid','refunded','cancelled')",
}
for name, sql in checks.items():
    bad = db.execute(sql).fetchone()[0]
    print(f"{'PASS' if bad == 0 else 'FAIL'}  {name}  (bad rows: {bad})")
# FAIL  completeness: customer_id not null  (bad rows: 1)
# FAIL  uniqueness: order_id  (bad rows: 1)
# FAIL  validity: amount >= 0  (bad rows: 1)
# FAIL  validity: status in set  (bad rows: 1)
```

Frameworks add what this script lacks: a declarative way to write rules, thresholds, history of results, alerting, documentation and integration with orchestrators.

**dbt tests** are the most common starting point, because they live next to the models they test. Generic tests are declared in YAML; each compiles to a query that returns failing rows:

```yaml
models:
  - name: orders
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: ['paid', 'refunded', 'cancelled']
      - name: customer_id
        data_tests:
          - relationships:
              arguments:
                to: ref('customers')
                field: customer_id
              config:
                severity: warn
```

(Since dbt 1.8 the key is `data_tests:`, and since 1.10 test arguments go under `arguments:`; older projects use `tests:` and put arguments directly under the test name.) `dbt build` runs each model and then its tests, and stops downstream models when an `error`-severity test fails. Singular tests are any SQL file in `tests/` that returns failing rows, and packages such as `dbt-expectations` add dozens of ready-made checks.

**Great Expectations (GX Core)** is a Python framework: you define **Expectations** (such as `ExpectColumnValuesToNotBeNull`), group them in suites, run them against batches of data from pandas, Spark or SQL databases, and get validation results and human-readable "Data Docs".

```python
import great_expectations as gx
import pandas as pd

df = pd.DataFrame({"order_id": [1, 2, 2], "customer_id": [10, None, 11]})
context = gx.get_context()
batch = (context.data_sources.add_pandas("shop")
         .add_dataframe_asset(name="orders")
         .add_batch_definition_whole_dataframe("all")
         .get_batch(batch_parameters={"dataframe": df}))

result = batch.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
print(result.success)            # False: one null customer_id
```

**Soda** uses a compact YAML language (SodaCL, in Soda Core 3) and a CLI that runs checks against a warehouse:

```yaml
# checks.yml : run with  soda scan -d bigquery_prod -c configuration.yml checks.yml
checks for orders:
  - row_count > 0
  - missing_count(customer_id) = 0
  - duplicate_count(order_id) = 0
  - invalid_count(status) = 0:
      valid values: [paid, refunded, cancelled]
  - freshness(created_at) < 2h
```

**Knowledge Catalog data quality scans** (auto data quality) run rules on BigQuery tables without any infrastructure. You define rules with a dimension and a pass threshold, and the scan writes results to the catalog and optionally to a BigQuery table:

```yaml
# dq_spec.yaml
rules:
- column: customer_id
  dimension: COMPLETENESS
  nonNullExpectation: {}
  threshold: 1.0
- column: order_id
  dimension: UNIQUENESS
  uniquenessExpectation: {}
- column: amount
  dimension: VALIDITY
  rangeExpectation:
    minValue: '0'
- column: status
  dimension: VALIDITY
  setExpectation:
    values: ['paid', 'refunded', 'cancelled']
rowFilter: "DATE(created_at) = CURRENT_DATE()"
```

```bash
gcloud dataplex datascans create data-quality orders-dq \
  --location=europe-west1 \
  --data-source-resource="//bigquery.googleapis.com/projects/shop-prod/datasets/marts/tables/orders" \
  --data-quality-spec-file=dq_spec.yaml
```

A companion feature, **data profiling scans**, computes statistics per column (null share, distinct values, min, max, top values) and can suggest rules from them.

**Where checks run.** Put the strictest checks at the **boundaries**: when data enters (to reject or quarantine bad source data and alert the source owner) and before data is published to consumers (so a bad build never replaces a good table). Checks in the middle help debugging.

> **Pitfall:** Hundreds of warning-level tests that fail every day. People learn to ignore the alerts, and the one that matters is missed. Every rule should have an owner and an action: fix the data, fix the rule, or delete it.

> **Tip:** Write a **data contract** for important tables: schema, semantics, owner, freshness and quality guarantees, agreed between producer and consumer and checked in CI and at run time. It turns "the data looks wrong" into "the producer broke rule 3 of the contract".

::: deeper
**Quality SLAs and SLOs.** Borrowing from site reliability engineering, define a **service level indicator** (the share of daily partitions that pass all critical checks and land before 07:00), an **objective** (99% per quarter) and, if needed, an **agreement** with consequences. An error budget then tells you when to stop shipping features and fix reliability.

**Thresholds instead of absolutes.** Real data is rarely perfect. "No more than 0.1% of rows have a null email" is a more honest rule than "zero nulls". Knowledge Catalog's `threshold`, Soda's `warn`/`fail` levels and dbt's `warn_if`/`error_if` configs all express this.

**Write-audit-publish.** The strongest pattern for publish-time checks: write the new data somewhere invisible (a staging table, an Iceberg branch, a dbt `--defer` build in a CI schema), audit it with the checks, and only then publish atomically (swap the table, fast-forward the branch, `MERGE`). Consumers never see unaudited data. The Pipelines and transformation section covers the mechanics.

**Cost of checks.** Each rule is a query. On BigQuery, a uniqueness check over a 5 TB table scans the column every run. Restrict checks to the new partition (`rowFilter` or `where:` configs), use sampling for statistical checks, and run full-table checks less often.

**Quarantine instead of drop.** When ingestion rejects rows, write them with the failure reason to a quarantine table rather than discarding them. Counting quarantined rows per source is itself a quality metric, and the rows can be replayed after a fix.

> **Interview:** "Where would you put data quality checks in a pipeline?" At ingestion (schema and basic validity, quarantine bad rows, alert the source owner), after transformations that can introduce errors (uniqueness after joins, reconciliation of totals), and as a gate before publishing (write-audit-publish), with results stored over time, shown in the catalog and tied to SLOs.
:::

::: check
?? Name the six common data quality dimensions.
Completeness, uniqueness, validity, accuracy, consistency and timeliness (freshness).

?? Why is accuracy harder to check automatically than validity?
Validity only needs the value to follow a format or domain, which a rule can test. Accuracy needs a trusted reference that says what the true value is, such as a ledger or the source system.

?? What does the write-audit-publish pattern guarantee?
Consumers never see data that has not passed the checks: the new data is written somewhere invisible, audited, and only then published in one atomic step.
:::

::: resources
- Docs: [Auto data quality overview](https://docs.cloud.google.com/knowledge-catalog/docs/auto-data-quality-overview) · Knowledge Catalog data quality scans: rule types, thresholds and results
- Docs: [dbt data tests](https://docs.getdbt.com/docs/build/data-tests) · generic and singular tests, severity and storing failures
- Docs: [Great Expectations (GX Core) documentation](https://docs.greatexpectations.io/docs/core/introduction/) · expectations, suites, checkpoints and Data Docs
- Docs: [Soda documentation](https://docs.soda.io/) · SodaCL checks and scans against warehouses
- Book: [Data Quality Fundamentals](https://www.oreilly.com/library/view/data-quality-fundamentals/9781098112035/) · Moses, Gavish and Vorwerck; quality dimensions, testing and SLAs
- Book: *Driving Data Quality with Data Contracts* by Andrew Jones · agreeing and enforcing quality between producers and consumers
:::

### Data Observability Platforms

::: plain
Quality rules are like a checklist a mechanic goes through. Observability is like the warning lights on a car's dashboard: they watch the engine all the time and light up when something behaves unusually, even if nobody wrote a rule for that exact fault. A data observability platform watches every table's volume, freshness, schema and value patterns, learns what normal looks like, and alerts you when today does not look like the past.
:::

Rules only catch what someone anticipated. Many real incidents are surprises: a source starts sending 40% fewer rows because a mobile release broke tracking, a column's null share creeps from 1% to 30%, an upstream team adds a column that shifts a CSV by one position. **Data observability** borrows the idea from software monitoring: collect signals about every dataset continuously, detect anomalies against history, and help you find the cause quickly.

The commonly cited **five pillars**:

| Pillar | Signal | Typical incident caught |
|---|---|---|
| Freshness | When the table last changed, delay versus usual | Upstream job stuck; file not delivered |
| Volume | Row count per load or partition | Half the events missing; duplicate load |
| Schema | Columns added, removed, retyped | Upstream rename breaks downstream joins |
| Distribution | Null share, distinct counts, mean, value mix | Default value suddenly in 30% of rows |
| Lineage | Which upstream and downstream assets are linked | Finding the root cause and blast radius |

**How anomaly detection works.** Most platforms fit a model per metric to its history, accounting for trend and weekly seasonality (Mondays differ from Sundays), and alert when a value falls outside the expected band. The simplest version is a z-score against recent history:

```python
from statistics import mean, stdev

# daily row counts for the orders table, oldest first; the last value is today
counts = [10210, 10480, 9950, 10330, 10120, 10610, 10290, 10400, 10050, 6120]
history, today = counts[:-1], counts[-1]

mu, sigma = mean(history), stdev(history)
z = (today - mu) / sigma
print(f"mean={mu:.0f} stdev={sigma:.0f} today={today} z={z:.1f}")
if abs(z) > 3:
    print("ANOMALY: volume outside 3 standard deviations, pausing publish")
# mean=10271 stdev=211 today=6120 z=-19.6
# ANOMALY: volume outside 3 standard deviations, pausing publish
```

Real detectors use longer windows, seasonal models and feedback ("this alert was expected") to cut false positives.

**Where the signals come from.** Observability tools mostly read **metadata**, not data, which keeps them cheap and safe: BigQuery `INFORMATION_SCHEMA` (`TABLE_STORAGE`, `PARTITIONS`, `JOBS`), table modification times, query logs, dbt run results and orchestrator events. Distribution checks need to query the data, usually on a sample or on the newest partition. On BigQuery a freshness and volume probe is a cheap metadata query:

```sql
SELECT table_name, partition_id, total_rows, last_modified_time
FROM marts.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'orders'
ORDER BY partition_id DESC
LIMIT 14;
```

**The tools.**

- **Monte Carlo**: the company that popularised the term; automatic monitors for freshness, volume and schema on every table, plus custom monitors, lineage-based incident grouping and root-cause hints.
- **Acceldata**: observability across data, pipelines and compute cost, including Spark and Hadoop estates.
- **Datafold**: focused on change management: **data diff** compares a table before and after a code change (row by row and column by column) in CI, so you see the effect of a dbt pull request before merging.
- **Others**: Bigeye, Anomalo (ML-based content anomaly detection), Metaplane, and open-source options such as **Elementary** (dbt-native anomaly tests and a report) and Soda's anomaly checks.
- **On Google Cloud**: Knowledge Catalog data profiling and data quality scans, BigQuery `INFORMATION_SCHEMA`, Cloud Monitoring alerts on custom metrics, and Managed Airflow task metrics cover a good part of the same ground with less automation.

**Circuit breakers** turn observations into protection. A circuit breaker is a check inside the pipeline that **stops** the pipeline when a signal is badly off, instead of only alerting. In Airflow it is a task before the publish step that fails (or short-circuits downstream tasks) when the anomaly score is too high, so the previous good version of the table stays in place.

**Table health scoring** summarises many signals into one number or a traffic light per table (freshness met, tests passing, no open incidents, documented, owned). Shown in the catalog, it lets consumers decide whether to trust a table at a glance and gives producers a target.

> **Pitfall:** Turning on anomaly detection for every table on day one. Thousands of tables produce hundreds of alerts, most of them for tables nobody uses. Start with the tables that feed important dashboards and models (lineage tells you which), and route each alert to the table's owner.

::: deeper
**Seasonality and change points.** Volumes have weekly and yearly cycles, holidays and planned launches. Good detectors model seasonality (for example with a seasonal decomposition or Prophet-like models) and accept annotations ("Black Friday", "migration on the 12th") so expected changes do not page anyone. After a genuine level shift, the model must re-learn quickly or it will alert every day.

**Incident management for data.** Treat data incidents like production incidents: an owner, severity levels based on downstream impact (lineage again), a status page in the catalog ("orders table delayed, ETA 10:00"), and a short post-incident review. Measure **time to detection** and **time to resolution**.

**Metadata versus data cost.** Freshness, volume and schema monitors read only metadata and cost almost nothing on BigQuery. Distribution monitors run queries; on a large table, monitoring 30 columns every hour can cost more than the pipeline itself. Use partition filters, sampling (`TABLESAMPLE SYSTEM (1 PERCENT)`) and lower frequency.

**Data diff internals.** Comparing two large tables row by row is expensive. Diff tools split both tables into key ranges, compare checksums of each range (inside the database), and recurse only into ranges whose checksums differ, so identical regions cost one aggregate query each.

> **Interview:** "What is the difference between data quality testing and data observability?" Testing checks explicit, known expectations written by people, and is precise but only catches anticipated problems. Observability monitors many signals on every table automatically, learns normal behaviour, and catches unknown problems, but is probabilistic and needs tuning. You want both, connected through lineage and incident management.
:::

::: check
?? Name the five pillars of data observability.
Freshness, volume, schema, distribution and lineage.

?? What is a circuit breaker in a data pipeline?
A check inside the pipeline that stops it from publishing (or running downstream steps) when data looks badly wrong, so consumers keep the last good version instead of receiving bad data.

?? Why are freshness and volume monitors cheap on BigQuery?
They read metadata (modification times, row counts per partition from `INFORMATION_SCHEMA`) rather than scanning table data.
:::

::: resources
- Blog: [What is data observability?](https://www.montecarlodata.com/blog-what-is-data-observability/) · Monte Carlo's definition of the five pillars
- Book: [Data Quality Fundamentals](https://www.oreilly.com/library/view/data-quality-fundamentals/9781098112035/) · the observability chapters: monitoring, anomaly detection and incident management
- Docs: [Data profiling overview](https://docs.cloud.google.com/knowledge-catalog/docs/data-profiling-overview) · Knowledge Catalog column statistics as the basis for monitoring
- Docs: [Elementary](https://docs.elementary-data.com/) · open-source, dbt-native anomaly detection and observability reports
- Docs: [Datafold data diff](https://docs.datafold.com/) · comparing tables before and after code changes in CI
- Docs: [BigQuery INFORMATION_SCHEMA](https://docs.cloud.google.com/bigquery/docs/information-schema-intro) · the metadata views observability tools read
:::

## Access and security

Knowing what data you have and trusting it is not enough; you also have to control who can use it. This group moves from the general model to specific tools. It starts with the two families of access control, role-based and attribute-based, and how they become row filters, column masks, tag-based policies and policy as code. Then it looks at Apache Ranger, the policy engine of the Hadoop world that still guards many Spark, Hive, Trino and Kafka estates. Then audit logging and zero trust: proving who accessed what, and never trusting a request just because it comes from inside the network. It ends with post-quantum cryptography, the slow but real change to the encryption that protects data at rest and in transit.

BigQuery in depth covered BigQuery's own mechanisms (IAM roles, row access policies, policy tags, dynamic masking) in detail; here they appear as examples of general patterns that every platform implements in its own way.

[[fig:governance_access]]

### Data Access Control — RBAC & ABAC

::: plain
**Role-based access control (RBAC)** gives permissions to roles, and people get roles: like a hotel where housekeeping key cards open every guest room and the manager's card also opens the office. **Attribute-based access control (ABAC)** decides each request from facts about the person, the data and the situation: "staff may open rooms on their own floor, during their shift". RBAC is simple to understand; ABAC scales better when there are thousands of tables and many kinds of users.
:::

**RBAC.** You define roles (analyst, data engineer, finance reader), attach permissions to roles, and assign users or groups to roles. On Google Cloud, IAM is RBAC: roles such as `roles/bigquery.dataViewer` bundle permissions, and you bind them to **groups** on a resource (project, dataset, table). The pattern scales well for coarse access ("finance analysts can read the finance dataset") but leads to **role explosion** for fine access: one role per region times per department times per sensitivity level quickly reaches hundreds of roles that nobody understands.

**ABAC.** The decision is a function of attributes:

- **Subject** attributes: department, country, clearance, employment type.
- **Resource** attributes: sensitivity tag, data domain, owning country, contains PII.
- **Context**: time, network location, device security state, purpose of the request.

One ABAC rule such as "allow read if `user.country == row.country` or the user is in global-sales" replaces dozens of roles. The cost is that rules are less obvious to audit ("who can see this table?" has no single list), so ABAC needs good tooling and tests.

**Fine-grained controls** sit inside a table:

- **Row-level security**: users see only the rows that match a filter.
- **Column-level security**: some columns are denied to some users.
- **Masking**: users see a transformed value (hashed, partially hidden, nulled) instead of being denied.

In BigQuery, a row access policy can implement ABAC by looking up the user's attributes in a mapping table:

```sql
CREATE TABLE gov.user_regions (email STRING, region STRING);

CREATE ROW ACCESS POLICY by_user_region ON marts.sales
GRANT TO ('group:sales-analysts@example.com')
FILTER USING (region IN (SELECT region FROM gov.user_regions WHERE email = SESSION_USER()));

CREATE ROW ACCESS POLICY pipelines_all ON marts.sales
GRANT TO ('serviceAccount:etl@shop-prod.iam.gserviceaccount.com', 'group:finance@example.com')
FILTER USING (TRUE);
```

Now the attribute ("which regions can this person see") lives in data that HR or an access request process maintains, and a single policy covers every analyst.

**Tag-based policies** attach rules to tags instead of tables. Columns tagged `pii.email` are masked for everyone without a specific permission, wherever the tag appears. BigQuery policy tags work this way (the taxonomy tag carries the access rule and masking data policies); Unity Catalog ABAC policies and Apache Ranger tag policies are the same idea. Combined with automatic classification (PII detection, next group), tags make protection follow the data: a new table with an email column is protected the moment it is tagged.

**Conditional access on Google Cloud.** IAM Conditions add ABAC-style conditions, written in CEL (Common Expression Language), to role bindings. Two common uses are temporary access and tags on resources:

```bash
# Temporary read access that expires automatically
gcloud projects add-iam-policy-binding shop-prod \
  --member="user:contractor@example.com" --role="roles/bigquery.dataViewer" \
  --condition='expression=request.time < timestamp("2026-12-31T00:00:00Z"),title=until-year-end'

# Access only to datasets carrying a given Resource Manager tag value
gcloud projects add-iam-policy-binding shop-prod \
  --member="group:analysts@example.com" --role="roles/bigquery.dataViewer" \
  --condition='expression=resource.matchTag("shop-org/sensitivity", "low"),title=low-sensitivity-only'
```

**Policy as code with OPA.** **Open Policy Agent** (OPA, a CNCF graduated project) is a general policy engine. You write policies in **Rego**, send it a JSON input describing the request, and it returns a decision. Data platforms use it in two places: to decide access at run time (an API or a query gateway asks OPA) and to check infrastructure changes in CI (for example "no BigQuery dataset may grant `allUsers`").

```rego
package data.access

default allow := false

# analysts may read non-restricted tables of their own department
allow if {
    input.user.role == "analyst"
    input.resource.department == input.user.department
    input.resource.classification != "restricted"
}

# the data owner may always read
allow if input.user.email == input.resource.owner
```

```bash
echo '{"user": {"role": "analyst", "department": "finance", "email": "a@example.com"},
       "resource": {"department": "finance", "classification": "internal", "owner": "b@example.com"}}' > input.json
opa eval --data access.rego --input input.json 'data.data.access.allow'   # result: true
```

(This is Rego v1 syntax, the default since OPA 1.0, which requires the `if` keyword.)

> **Pitfall:** Granting to individual users. When people change teams, their access stays. Grant to groups that are managed by your identity provider, so joining and leaving teams updates access automatically, and review group membership regularly.

> **Tip:** Follow **least privilege** by default and make access requests easy: a self-service request with an approver and an expiry date beats a culture of broad standing access granted "to save time".

::: deeper
**Policy decision and enforcement points.** Access control architectures separate the **PDP** (policy decision point, which evaluates rules, such as OPA or Ranger Admin's policies) from the **PEP** (policy enforcement point, which intercepts requests and applies the decision, such as a query engine or API gateway) and the **PIP** (policy information point, which supplies attributes, such as the HR directory). For data, the best PEP is the query engine itself, because only it can filter rows and mask columns on every access path.

**Where masking happens matters.** Masking in a view or BI layer protects only that path; someone with table access bypasses it. Engine-level masking (BigQuery data policies, Unity Catalog masks, Ranger masking) applies to every query. BigQuery applies masking before any other query operation, so filters and joins on the masked column see the masked value, preventing inference by `WHERE email = 'x'`.

**Inference attacks.** Even with masking, aggregates can leak: if only one employee in a department is over 60, the average salary by department and age band reveals their salary. That is a privacy problem addressed by aggregation thresholds and differential privacy (Privacy and compliance group).

**Testing policies.** Rego has a built-in test runner (`opa test`), and row access policies can be tested by running queries as test service accounts in CI. Treat policies as code: reviewed, versioned, tested and deployed through the same pipeline as Terraform.

> **Interview:** "When would you choose ABAC over RBAC?" When access depends on combinations of attributes (region, department, sensitivity, purpose) that would need a role for every combination. Use RBAC for coarse platform roles and ABAC (row policies with attribute tables, tag-based masking, IAM Conditions) for fine-grained data access.
:::

::: check
?? What is role explosion, and how does ABAC avoid it?
Creating a separate role for every combination of attributes (region, department, sensitivity) until there are hundreds of roles. ABAC evaluates the attributes directly in a rule, so one rule replaces many roles.

?? Why is masking in a BI tool weaker than masking in the query engine?
Anyone with direct table access, or another tool, bypasses the BI layer. Engine-level masking applies to every query from every path.

?? In the BigQuery example, why is there a second row access policy with `FILTER USING (TRUE)`?
Once a table has any row access policy, principals not covered by a policy see no rows. Pipelines and finance need all rows, so they get their own policy that matches everything.
:::

::: resources
- Docs: [Introduction to row-level security](https://docs.cloud.google.com/bigquery/docs/row-level-security-intro) · row access policies, subqueries and interactions with other features
- Docs: [IAM Conditions overview](https://docs.cloud.google.com/iam/docs/conditions-overview) · attribute-based conditions on Google Cloud role bindings
- Docs: [Open Policy Agent documentation](https://www.openpolicyagent.org/docs/) · Rego, deployment patterns and testing
- Spec: [NIST SP 800-162: Guide to ABAC](https://csrc.nist.gov/pubs/sp/800/162/upd2/final) · the reference definition of attribute-based access control
- Docs: [Introduction to column-level access control](https://docs.cloud.google.com/bigquery/docs/column-level-security-intro) · policy tags as tag-based column policies
- Book: *Data Governance: The Definitive Guide* by Evren Eryurek and others (Google authors) · access control and governance in practice
:::

### Apache Ranger & Fine-Grained Access

::: plain
Apache Ranger is a central security office for a Hadoop-style data platform. Instead of every system (the file store, Hive, Kafka, HBase) keeping its own rules, administrators write all the rules in one place, and a small guard (a plugin) inside each system checks every request against a local copy of those rules and writes a line in the visitor book. Ranger can also hide columns and filter rows, so two people running the same query can see different results.
:::

Ranger grew up in the Hadoop ecosystem (Hadoop ecosystem section), where each engine had a different, weak permission model. It is still the standard for on-premises and self-managed Spark, Hive, Trino, HBase and Kafka estates, including clusters on Google Cloud: Managed Service for Apache Spark offers Ranger as an **optional component**.

**Architecture.**

- **Ranger Admin**: the web UI and REST API where policies are defined, stored in a relational database (MySQL, PostgreSQL, Oracle).
- **Plugins**: lightweight libraries running inside each service (HDFS NameNode, HiveServer2, HBase, Kafka brokers, YARN, Trino, Solr and others). A plugin **polls** Ranger Admin for policies and caches them locally, so every access decision is made in-process, without a network call, and services keep enforcing the last known policies if Admin is down.
- **Usersync**: imports users and groups from LDAP, Active Directory or Unix.
- **Tagsync**: imports tags (classifications) from Apache Atlas, enabling tag-based policies.
- **Audit**: plugins send an audit record for each access decision to Solr (for the UI's audit search), HDFS, Elasticsearch or log files.
- **Ranger KMS**: an optional key management server for HDFS transparent encryption.

**Policy types.**

1. **Access policies**: for a resource (database, table, column; topic; path), which users, groups or roles get which permissions (select, update, create, drop, read, write...), with optional **deny** conditions and **exclusions**.
2. **Masking policies** (Hive, Trino, Spark through integrations): for a column, which users see which mask: redact (letters to `x`, digits to `n`), show last 4, show first 4, hash, nullify, date show only year, or a custom expression.
3. **Row-level filter policies**: for a table, which users see only rows matching a SQL predicate, such as `country = 'IN'`.
4. **Tag-based policies**: rules attached to tags (from Atlas) rather than resources, so "deny `PII` to contractors" applies to every tagged column on every service.

Policies can be created through the REST API, which is how teams manage them as code:

```json
{
  "service": "hive_prod",
  "name": "mask_customer_email",
  "policyType": 1,
  "resources": {
    "database": {"values": ["crm"]},
    "table":    {"values": ["customers"]},
    "column":   {"values": ["email"]}
  },
  "dataMaskPolicyItems": [
    {"groups": ["analysts"], "accesses": [{"type": "select", "isAllowed": true}],
     "dataMaskInfo": {"dataMaskType": "MASK_SHOW_LAST_4"}}
  ]
}
```

```bash
curl -u admin -H "Content-Type: application/json" \
  -X POST https://ranger.internal:6182/service/public/v2/api/policy -d @mask_email.json
```

(`policyType` is 0 for access, 1 for masking and 2 for row-level filter policies.)

**Evaluation order.** Tag-based policies are evaluated before resource-based ones. Within policies, **deny beats allow**: an explicit deny (unless an exclusion applies) wins over any allow. If no policy decides, Ranger can fall back to the service's native permissions (for HDFS, the POSIX permissions) or deny, depending on configuration. This makes "deny contractors access to anything tagged PII" a reliable guard rail even if someone grants a broad allow later.

> **Pitfall:** Forgetting that Ranger only protects access paths that go through a plugin. A user with direct access to the underlying files (an HDFS superuser, or a Cloud Storage bucket readable by their account) bypasses Hive masking entirely. Lock down the storage layer so only the engines can read it.

> **Tip:** Prefer tag-based policies for sensitive data. Resource policies need updating for every new table; a tag policy covers every table that is classified correctly, which moves the problem to classification, where automation helps.

::: deeper
**Policy caching and propagation delay.** Plugins poll for changes on an interval (tens of seconds by default) and keep a local policy cache on disk, so a revoked permission can remain effective until the next poll. For urgent revocations, lower the interval for sensitive services or remove the user from the identity source too.

**Kerberos and identity.** Ranger trusts the identity the service authenticated. In Hadoop that means Kerberos; without it, users can claim any name. On Managed Service for Apache Spark, enabling Ranger requires Kerberos on the cluster. Ranger is only as strong as the authentication in front of it.

**Spark is the hard case.** Hive and Trino run queries in a server that holds the plugin. Classic Spark runs user code in executors that read files directly, so a plugin cannot reliably filter rows or mask columns (the user's code could read the files anyway). Solutions route Spark SQL through a trusted server (Apache Kyuubi with a Ranger authorizer, or a Ranger-aware Spark distribution), or rely on a catalog with credential vending such as Unity Catalog or Google Cloud Lakehouse.

**Ranger versus cloud-native controls.** On Google Cloud, data in BigQuery is governed by IAM, row access policies and policy tags; Ranger matters for engines running on clusters (Hive, Trino, Spark with a gateway). Mixed estates either maintain two policy sets or generate both from one source of truth.

> **Interview:** "How does Ranger enforce a policy without adding latency to every query?" Each service embeds a plugin that caches all relevant policies locally and evaluates decisions in-process. It polls Ranger Admin for changes periodically and sends audit events asynchronously, so the request path never waits on a remote call.
:::

::: check
?? Where are Ranger access decisions actually made?
In the plugin inside each service (HiveServer2, the NameNode, Kafka brokers), using a locally cached copy of the policies.

?? An analyst is allowed by one policy and denied by a tag-based policy. What happens?
Access is denied: deny takes precedence over allow (unless an explicit exclusion applies).

?? Why is row filtering for classic Spark jobs harder than for Hive or Trino?
Spark runs user code in executors that read the files directly, so there is no trusted server to apply filters; the user's code could bypass them. A trusted gateway or credential-vending catalog is needed.
:::

::: resources
- Docs: [Apache Ranger](https://ranger.apache.org/) · project home, downloads and documentation links
- Docs: [Ranger on the Apache wiki](https://cwiki.apache.org/confluence/display/RANGER/) · architecture, REST API, row filtering and masking guides
- Docs: [Managed Service for Apache Spark Ranger component](https://docs.cloud.google.com/managed-spark/docs/concepts/components/ranger) · enabling Ranger on Google Cloud clusters
- Repo: [Apache Ranger on GitHub](https://github.com/apache/ranger) · source for Admin, plugins, Usersync and Tagsync
- Book: *Hadoop Security* by Ben Spivey and Joey Echeverria · authentication, authorization and auditing in Hadoop clusters
- Docs: [Apache Kyuubi authorization](https://kyuubi.readthedocs.io/en/master/security/authorization/spark/overview.html) · enforcing Ranger policies for Spark SQL through a gateway
:::

### Audit Logging & Zero Trust Data Architecture

::: plain
An **audit log** is the visitor book of your data platform: who opened which table, when, from where, and what they did. It must be written automatically and must be impossible to quietly edit. **Zero trust** is a security attitude: never assume a request is safe just because it comes from inside the office network. Every request, from a person or a program, must prove who it is and be checked against the rules, every time, like a building where every door needs a badge, not just the front entrance.
:::

**Why audit logs matter.** They answer questions from security ("did anyone export the customer table?"), compliance (GDPR and HIPAA require you to know and show who accessed personal data), operations ("which job deleted this partition?") and cost. They are also evidence: an auditor wants to see that they are complete and cannot be altered.

**Cloud Audit Logs on Google Cloud** come in four types:

| Type | What it records | Default |
|---|---|---|
| Admin Activity | Configuration changes: create dataset, change IAM policy | Always on, cannot be disabled, no charge |
| Data Access | Reading data or metadata, and user-driven writes of data | Off by default for most services; **on for BigQuery** |
| System Event | Changes made by Google systems | Always on |
| Policy Denied | Requests refused by a security policy (VPC Service Controls) | On, can be excluded |

Logs land in log buckets: `_Required` (Admin Activity and System Event, kept 400 days) and `_Default` (everything else, 30 days by default, configurable). For long retention and analysis, route them with an **aggregated sink** at the organization level to a BigQuery dataset or a locked log bucket:

```bash
gcloud logging sinks create org-audit-to-bq \
  bigquery.googleapis.com/projects/sec-logs/datasets/audit \
  --organization=123456789012 --include-children \
  --log-filter='logName:"cloudaudit.googleapis.com"' \
  --use-partitioned-tables
```

**BigQuery's own history.** Besides audit logs, `INFORMATION_SCHEMA.JOBS` keeps 180 days of job history with the user, the query text and the referenced tables:

```sql
-- Who read crm.customers in the last 30 days, and how often?
SELECT user_email, COUNT(*) AS jobs, MAX(creation_time) AS last_read
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, UNNEST(referenced_tables) AS t
WHERE t.dataset_id = 'crm' AND t.table_id = 'customers'
  AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY user_email
ORDER BY jobs DESC;
```

Other platforms have equivalents: Snowflake's `ACCOUNT_USAGE.ACCESS_HISTORY` view records the objects and columns each query read and wrote (including base tables behind views), and Databricks has `system.access.audit`.

**Making logs trustworthy.** An audit log an administrator can delete is not evidence. Put the copy of record in a separate security project that data teams cannot administer, use **locked retention** on the log bucket (`gcloud logging buckets update ... --locked` after setting retention, which cannot then be shortened) or a Cloud Storage bucket with a locked retention policy, and alert on changes to sinks.

**Zero trust.** NIST SP 800-207 defines zero trust architecture around a few tenets: all data sources and services are resources; all communication is secured regardless of network location; access is granted per session, based on identity and dynamic policy; and the enterprise monitors the security posture of all assets. Google's own **BeyondCorp** programme, which moved employee access off the VPN, is the best-known implementation. For data platforms, zero trust means:

1. **Identity first.** Every human and every workload has its own identity. Workloads use service accounts attached to the compute (Cloud Run, GKE Workload Identity, Composer) or **Workload Identity Federation** for external systems (GitHub Actions, another cloud), never downloaded service account keys. The organization policy `iam.disableServiceAccountKeyCreation` enforces this.
2. **Short-lived credentials.** Tokens that expire in an hour, created on demand, instead of long-lived keys.
3. **Context-aware access.** Identity-Aware Proxy (IAP) and Access Context Manager access levels check device and location before allowing access to consoles and internal tools.
4. **Micro-segmentation.** **VPC Service Controls** draw a perimeter around projects holding sensitive data, so that even a valid credential cannot copy data from BigQuery or Cloud Storage to a project outside the perimeter. This is the main defence against exfiltration with stolen credentials.
5. **Encrypted, authenticated service-to-service traffic.** Private Service Connect and Private Google Access keep traffic off the internet; mTLS (for example in a service mesh) authenticates services to each other.
6. **Continuous monitoring.** Audit logs feed detection rules: unusual export volumes, access from new locations, a service account suddenly reading tables it never read.

> **Pitfall:** Leaving Data Access audit logs off for Cloud Storage, Spanner or Cloud SQL "to save money", then being unable to answer who read a file during an incident. Turn them on at least for projects holding sensitive data, and use exclusions for noisy, low-value entries instead of disabling everything.

::: deeper
**Volume and cost.** Data Access logs for a busy platform can reach terabytes per month. Route them with filters (exclude health checks and known automated reads), store raw logs in a cheaper bucket, and keep a summarised table (user, table, day, count) in BigQuery for routine questions.

**What audit logs do not show.** They show that a query read a table, not which rows the user saw after row-level filtering, and not what they did with the result afterwards. Combine them with data loss prevention controls (VPC Service Controls, disabling downloads, BigQuery data egress restrictions on shared data) where that matters.

**Tamper evidence.** For the strongest guarantees, logs are chained by hashes or signed so that deletion or editing is detectable, and stored on WORM (write once, read many) storage. Cloud Storage retention policies with Bucket Lock are designed to meet regulatory WORM requirements such as SEC Rule 17a-4.

**Zero trust is not a product.** Vendors sell "zero trust" tools, but the architecture is a set of practices. A useful test: if an attacker gets a laptop on the corporate network, or a copied service account key, how much data can they read? In a zero trust design the answer is "nothing without passing identity, device and context checks, and nothing outside the perimeter even then".

> **Interview:** "A service account key leaked on a public repository. What limits the damage, and what do you do?" Damage is limited by least privilege on the account, VPC Service Controls perimeters (the key cannot exfiltrate data to outside projects), and short key lifetimes. Respond by disabling and deleting the key, reviewing audit logs for its use since the leak, rotating anything it could reach, and moving the workload to attached service accounts or Workload Identity Federation so keys are no longer needed.
:::

::: check
?? Which Cloud Audit Log type records a user reading a BigQuery table, and is it on by default?
Data Access audit logs. They are off by default for most services but on by default for BigQuery.

?? Why should the copy of record of audit logs live in a separate project with locked retention?
So that the administrators of the data platform, or an attacker with their credentials, cannot delete or shorten the evidence of what happened.

?? What does VPC Service Controls protect against that IAM alone does not?
Exfiltration with valid credentials: even an identity with read access cannot copy data from services inside the perimeter to resources outside it.
:::

::: resources
- Docs: [Cloud Audit Logs overview](https://docs.cloud.google.com/logging/docs/audit) · log types, defaults, retention and permissions
- Docs: [BigQuery audit logs](https://docs.cloud.google.com/bigquery/docs/reference/auditlogs) · what BigQuery records and how to query it
- Docs: [VPC Service Controls overview](https://docs.cloud.google.com/vpc-service-controls/docs/overview) · service perimeters against data exfiltration
- Spec: [NIST SP 800-207: Zero Trust Architecture](https://csrc.nist.gov/pubs/sp/800/207/final) · the reference definition and tenets
- Paper: [BeyondCorp: A New Approach to Enterprise Security](https://research.google/pubs/beyondcorp-a-new-approach-to-enterprise-security/) · Google's original zero trust design
- Docs: [Snowflake ACCESS_HISTORY view](https://docs.snowflake.com/en/sql-reference/account-usage/access_history) · column-level access auditing in another warehouse
:::

### Quantum-Resistant Cryptography for Data

::: plain
Today's internet security relies on maths problems that ordinary computers cannot solve in any reasonable time. A large enough quantum computer could solve some of them quickly, and then the locks protecting much of today's traffic would open. Nobody has such a computer yet, but data stolen today could be stored and unlocked years later. **Post-quantum cryptography** replaces the vulnerable locks with new ones based on problems quantum computers are not known to solve, much like changing a building's locks before a known master key goes into production.
:::

**What is at risk, and what is not.** Cryptography in a data platform has two families:

- **Symmetric** encryption (AES) protects data at rest: BigQuery tables, Cloud Storage objects and disks are encrypted with AES-256. Grover's algorithm, the relevant quantum attack, roughly halves the effective key length, so AES-256 keeps about 128 bits of security. **Symmetric encryption at rest with 256-bit keys is considered safe.**
- **Public-key** cryptography (RSA, elliptic curves such as X25519 and ECDSA) is used for key exchange in TLS, digital signatures, certificates, SSH, PGP-encrypted file transfers and wrapping keys with RSA. **Shor's algorithm** would break all of these on a large quantum computer.

The urgent threat is **harvest now, decrypt later**: an attacker records encrypted traffic today (a nightly file transfer to a partner, replication between data centres) and decrypts it once a quantum computer exists. Data that must stay confidential for 10 or more years (health records, financial histories, government data) is exposed already. Signatures are a later concern, since a forgery requires the quantum computer at the time of the attack, but long-lived signatures (firmware, archived documents, root certificates) need to move too.

**The new standards.** In August 2024 NIST published the first three post-quantum standards:

| Standard | Algorithm | Based on | Use |
|---|---|---|---|
| FIPS 203 | ML-KEM (from CRYSTALS-Kyber) | Module lattices | Key encapsulation (key exchange) |
| FIPS 204 | ML-DSA (from CRYSTALS-Dilithium) | Module lattices | Digital signatures |
| FIPS 205 | SLH-DSA (from SPHINCS+) | Hash functions | Digital signatures (conservative backup) |

A fourth, FN-DSA (from Falcon), is being standardised, and in March 2025 NIST selected HQC as a backup key encapsulation algorithm based on different maths. NIST's transition guidance proposes deprecating quantum-vulnerable public-key algorithms by 2030 and disallowing them by 2035.

**Hybrid schemes.** Because the new algorithms are young, the first deployments are **hybrid**: combine a classical exchange (X25519) with ML-KEM, so the session is safe unless both are broken. The hybrid group `X25519MLKEM768` is already used by default in Chrome and in Google's own TLS, and OpenSSL 3.5 supports the new algorithms:

```bash
openssl version                                         # needs 3.5 or later
openssl genpkey -algorithm ML-DSA-65 -out sign_key.pem   # post-quantum signing key
openssl genpkey -algorithm ML-KEM-768 -out kem_key.pem   # post-quantum key encapsulation key
openssl s_client -connect www.google.com:443 -groups X25519MLKEM768 </dev/null 2>/dev/null | grep -i "group"
```

Post-quantum keys and signatures are larger: an ML-DSA-65 signature is 3,309 bytes and its public key 1,952 bytes, against 64 and 32 bytes for Ed25519. That matters for certificates, tokens signed per request and protocols with tight size limits.

**What a data engineer actually does.**

1. **Inventory**: find where public-key cryptography protects data you own: TLS endpoints, SFTP and PGP transfers with partners, client-side encryption that wraps keys with RSA, signed exports, JWTs between services. This is sometimes called a cryptographic bill of materials.
2. **Prioritise by data lifetime**: long-lived confidential data crossing networks first.
3. **Use managed services that migrate for you**: Google Cloud already protects much of its internal and front-end traffic with hybrid post-quantum key exchange. Cloud KMS offers the NIST algorithms: quantum-safe digital signatures (ML-DSA and SLH-DSA) and post-quantum key encapsulation (ML-KEM, plus the hybrid X-Wing KEM). Check the Cloud KMS documentation for which algorithms are generally available in your region before depending on them.
4. **Build crypto agility**: avoid hard-coding algorithms; keep algorithm identifiers in configuration and metadata so keys can be rotated to a new algorithm without rewriting pipelines.
5. **Prefer symmetric envelope encryption at rest**: data encrypted with AES-256 data keys, wrapped by a symmetric Cloud KMS key (CMEK), has no public-key step to break.

> **Pitfall:** Re-encrypting every table "for quantum safety". Data at rest under AES-256 with symmetric key wrapping is already considered quantum-resistant. Effort belongs in transit (TLS, file transfers) and in asymmetric key wrapping and signatures.

::: deeper
**Why lattices?** ML-KEM and ML-DSA rely on the hardness of problems such as Module Learning With Errors: recovering a secret vector from noisy linear equations over polynomial rings. No efficient quantum algorithm is known for them, and they are fast in software, faster than RSA for key generation. SLH-DSA relies only on the security of hash functions, which makes it the most conservative choice, at the cost of large signatures (about 7.8 KB for the smallest parameter set) and slow signing.

**KEM, not key exchange.** ML-KEM is a key encapsulation mechanism: one side encapsulates a random secret to the other's public key, producing a ciphertext and a shared secret. TLS 1.3 integrates it as a key share. It is not a drop-in replacement for RSA encryption of arbitrary data; protocols that "RSA-encrypt the file" need redesign (for example KEM plus AES-GCM, as in HPKE).

**Implementation risk.** New algorithms bring new implementation bugs and side channels; early candidates (SIKE, Rainbow) were broken by classical attacks during the competition. Hybrids hedge against that, and using well-reviewed libraries (BoringSSL, OpenSSL, Tink, cloud KMS) rather than your own code is essential.

**Timeline uncertainty.** Estimates of when a cryptographically relevant quantum computer might exist vary widely. The data engineering question is simpler: how long must this data stay secret, and how long will migration take? If the sum exceeds the earliest plausible date, start now.

> **Interview:** "Is BigQuery data at rest threatened by quantum computers?" Not in a practical sense: it is encrypted with AES-256, and quantum attacks on symmetric ciphers only halve the effective key length. The risk is in public-key cryptography: TLS key exchange on transfers, RSA key wrapping and signatures, which is where hybrid ML-KEM and ML-DSA are being deployed.
:::

::: check
?? Why is AES-256 encryption at rest considered safe against quantum computers while RSA is not?
Grover's algorithm only halves the effective key length of symmetric ciphers, leaving AES-256 at about 128-bit security. Shor's algorithm breaks RSA and elliptic-curve cryptography completely.

?? What is "harvest now, decrypt later", and which data does it threaten most?
Recording encrypted traffic today to decrypt it once a quantum computer exists. It threatens data that must stay confidential for many years and crosses networks protected by classical key exchange.

?? Why are hybrid schemes such as X25519 plus ML-KEM used during the transition?
The session stays secure unless both the classical and the post-quantum algorithm are broken, which hedges against undiscovered weaknesses in the new algorithms.
:::

::: resources
- Spec: [NIST post-quantum cryptography project](https://csrc.nist.gov/projects/post-quantum-cryptography) · the standards process, FIPS 203, 204 and 205, and transition guidance
- Spec: [FIPS 203: ML-KEM](https://csrc.nist.gov/pubs/fips/203/final) · the key encapsulation standard
- Blog: [Announcing quantum-safe digital signatures in Cloud KMS](https://cloud.google.com/blog/products/identity-security/announcing-quantum-safe-digital-signatures-in-cloud-kms) · ML-DSA and SLH-DSA in Google Cloud's key service
- Blog: [Announcing quantum-safe key encapsulation mechanisms in Cloud KMS](https://cloud.google.com/blog/products/identity-security/announcing-quantum-safe-key-encapsulation-mechanisms-in-cloud-kms) · ML-KEM and the hybrid X-Wing KEM
- Blog: [Google Cloud's post-quantum cryptography roadmap](https://cloud.google.com/blog/products/identity-security/pqc-in-plaintext-google-clouds-post-quantum-cryptography-roadmap) · what Google has migrated and what customers need to do
- Book: [Serious Cryptography, 2nd edition](https://nostarch.com/serious-cryptography-2nd-edition) · Aumasson; a practical introduction that includes post-quantum algorithms
:::

## Privacy and compliance

Security keeps unauthorised people out. **Privacy** goes further: even authorised people and systems may use personal data only for agreed purposes, keep it only as long as needed, and must be able to show, correct and delete it on request. Laws such as the EU's GDPR, California's CCPA and the US HIPAA for health data turn these principles into obligations with deadlines and fines, and most of the hard work lands on data engineers, because personal data spreads through pipelines, copies, logs and backups.

This group follows personal data through its life. It starts with **finding and masking** it, then the main **regulations** and what they require technically (erasure in an immutable lakehouse, residency, audit trails), then **privacy engineering** techniques that let you analyse data without exposing individuals, including data clean rooms. Then **consent** (tracking what each person agreed to and enforcing it), **retention** (deleting data on schedule), **sharing** with other organizations, and finally how all of this is governed when many domain teams own their data in a **data mesh**.

[[fig:governance_erasure]]

> **Note:** This group explains what regulations require of data systems. It is not legal advice; for any real decision, work with your privacy and legal teams.

### PII Detection & Data Masking

::: plain
**PII** (personally identifiable information) is any data that identifies a person, alone or combined with other data: a name, an email, a phone number, a national ID, an exact location. Before you can protect it you must find it, which is hard because it hides in free-text notes, logs and JSON blobs as well as in obvious columns. Masking is then like blacking out names in a document before photocopying it: the document stays useful, but the private parts are hidden or replaced.
:::

**Finding PII.** Detection combines several techniques:

- **Column metadata**: names such as `email`, `phone`, `dob`. Cheap and fast but easily fooled (`col_17`, `contact`).
- **Patterns**: regular expressions plus checksums (credit card numbers pass the Luhn check; many national IDs have check digits).
- **Context**: words near a match ("SSN", "passport") raise confidence.
- **Named entity recognition**: NLP models find names, addresses and organizations in free text.

On Google Cloud, **Sensitive Data Protection** (formerly Cloud DLP) does this as a service. It has more than 150 built-in **infoTypes** (`EMAIL_ADDRESS`, `PHONE_NUMBER`, `CREDIT_CARD_NUMBER`, `INDIA_PAN_INDIVIDUAL`...), custom infoTypes from dictionaries or regexes, and a **discovery** service that continuously profiles BigQuery, Cloud SQL and Cloud Storage across an organization, scoring each table's sensitivity and publishing results to Knowledge Catalog and Security Command Center. **Microsoft Presidio** is the main open-source option: an analyzer (regex recognisers, spaCy NER, context words) and an anonymizer with operators (replace, redact, mask, hash, encrypt), runnable anywhere, for example in a Dataflow or Spark job.

A minimal pattern-based detector, tokeniser and masker, using only the standard library:

```python
import hashlib, hmac, re

EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
PHONE = re.compile(r"\+?\d[\d -]{8,}\d")
SECRET = b"load-me-from-secret-manager"      # never hard-code a real key

def token(value: str) -> str:
    # deterministic: the same input always gives the same token, so joins still work
    return "tok_" + hmac.new(SECRET, value.lower().encode(), hashlib.sha256).hexdigest()[:16]

def mask_email(m):
    user, domain = m.group().split("@")
    return user[0] + "***@" + domain

note = "Customer asha.rao@example.com called from +91 98123 45678 about order 991."
print("found:", EMAIL.findall(note) + PHONE.findall(note))
print("masked:", PHONE.sub("[PHONE]", EMAIL.sub(mask_email, note)))
print("tokens equal:", token("Asha.Rao@example.com") == token("asha.rao@example.com"))
# found: ['asha.rao@example.com', '+91 98123 45678']
# masked: Customer a***@example.com called from [PHONE] about order 991.
# tokens equal: True
```

**Protecting PII: the techniques.**

| Technique | What it does | Reversible? | Keeps joins? |
|---|---|---|---|
| Redaction / suppression | Removes the value or replaces it with a label | No | No |
| Partial masking | Hides part (`a***@example.com`, last 4 digits) | No | No |
| Hashing (unkeyed) | SHA-256 of the value | No, but brute-forceable for small domains | Yes |
| Keyed hashing (HMAC) | Hash with a secret key | No | Yes |
| Tokenisation | Replaces with a random or keyed token; a vault or key maps back | Yes, with the vault or key | Yes |
| Format-preserving encryption (FPE) | Encrypts into a value of the same format (16 digits stay 16 digits) | Yes, with the key | Yes |
| Generalisation / bucketing | Age 37 becomes 30-39; exact location becomes city | No | Partly |
| Date shifting | Moves dates by a random, per-person offset | With the offset | Keeps intervals |

**Static versus dynamic masking.** **Static** masking writes a transformed copy (a de-identified dataset for analysts, a masked copy for a test environment); the raw data is not present in the copy at all. **Dynamic** masking keeps one copy and masks at query time depending on who asks (BigQuery data policies, Unity Catalog masks, Ranger). Use static masking when data leaves the controlled environment (test systems, partners, ML training sets); use dynamic masking inside the warehouse so authorised users see real values without duplicate tables.

**Synthetic data** is generated data with the statistical shape of the real data but no real people in it. It is useful for development and testing. Simple generators (Faker) produce realistic-looking fake values; model-based generators learn joint distributions. Model-based synthetic data can still leak real records if the model memorises them, so treat it with care unless it is generated with differential privacy.

> **Pitfall:** Believing that hashing an email anonymises it. Anyone with a list of emails can hash them and match. Under GDPR, hashed or tokenised data is **pseudonymised**, which is still personal data; only properly anonymised data falls outside the regulation.

> **Tip:** Scan where PII leaks rather than where it is expected: log tables, JSON payload columns, free-text comments, error messages and exported CSVs in buckets. Discovery that runs continuously across the organization finds the copy someone made last week.

::: deeper
**De-identification on Google Cloud.** Sensitive Data Protection's de-identify API supports `CryptoDeterministicConfig` (AES-SIV deterministic encryption, reversible with the key, keeps joins), `CryptoReplaceFfxFpeConfig` (format-preserving encryption with FF1), `CryptoHashConfig` (HMAC-SHA-256), bucketing, character masking and date shifting. Keys are wrapped by Cloud KMS. It can run inside a Dataflow pipeline, so data is de-identified before it ever lands in BigQuery. In BigQuery itself, the `DETERMINISTIC_ENCRYPT` and AEAD functions provide column encryption with keysets that can be wrapped by Cloud KMS.

**Vault versus vaultless tokenisation.** A token vault stores a random token for each value: perfectly unlinkable without the vault, but the vault becomes a high-availability, high-security database on every critical path. Vaultless tokenisation derives the token cryptographically (FPE or deterministic encryption), so there is no lookup table, only a key to protect.

**Quasi-identifiers.** Removing direct identifiers is not enough. Latanya Sweeney showed that ZIP code, birth date and sex together identify most of the US population uniquely. Fields that identify in combination are **quasi-identifiers**, and they need generalisation or the privacy techniques in the topic after next.

**Detection quality.** Detectors produce false positives (order numbers that look like phone numbers) and false negatives (unusual formats). Measure on a labelled sample, tune likelihood thresholds per infoType, and combine metadata and content signals.

> **Interview:** "How would you make production data safe for a test environment?" Do not copy raw production data. Generate synthetic data, or create a statically de-identified copy: drop columns that are not needed, tokenise keys deterministically so joins still work, mask or generalise quasi-identifiers, shift dates, and scan the result with a PII detector before it leaves production.
:::

::: check
?? What is the difference between static and dynamic masking?
Static masking writes a transformed copy, so the raw values are not present in it. Dynamic masking keeps one copy and masks at query time based on who is asking.

?? Why is an unkeyed SHA-256 hash of a phone number weak protection?
The set of possible phone numbers is small enough to hash every one and look up the result, so the hash can be reversed by brute force. A keyed hash or tokenisation is needed.

?? Is tokenised data still personal data under GDPR?
Yes. It is pseudonymised: it can be linked back to the person with additional information (the key or vault), so it remains personal data.
:::

::: resources
- Docs: [Sensitive Data Protection overview](https://docs.cloud.google.com/sensitive-data-protection/docs/sensitive-data-protection-overview) · inspection, discovery and de-identification on Google Cloud
- Docs: [De-identifying sensitive data](https://docs.cloud.google.com/sensitive-data-protection/docs/deidentify-sensitive-data) · masking, tokenisation, FPE and date shifting transforms
- Repo: [Microsoft Presidio](https://github.com/microsoft/presidio) · open-source PII detection and anonymisation for text and images
- Docs: [Presidio documentation](https://microsoft.github.io/presidio/) · analyzers, recognisers and anonymizer operators
- Book: [Practical Data Privacy](https://www.oreilly.com/library/view/practical-data-privacy/9781098129453/) · Jarmul; pseudonymisation, anonymisation and privacy in pipelines
- Docs: [BigQuery AEAD encryption concepts](https://docs.cloud.google.com/bigquery/docs/aead-encryption-concepts) · column-level encryption and keysets in SQL
:::

### GDPR, CCPA, HIPAA Compliance

::: plain
Privacy laws give people rights over data about them: to see it, correct it, take it elsewhere and have it deleted, and they require companies to protect it and use it only for clear purposes. For a data engineer, each right becomes a technical requirement. "Delete my data within a month" means you must be able to find every copy of a person's data across tables, files, streams and backups, and remove it, even from storage systems that were designed never to change a file once written.
:::

**The three laws you meet most often.**

| | GDPR (EU and EEA, also UK GDPR) | CCPA as amended by CPRA (California) | HIPAA (US health data) |
|---|---|---|---|
| Protects | Personal data of people in the EU | Personal information of California residents | Protected health information (PHI) held by covered entities and their business associates |
| Key rights | Access, rectification, erasure, portability, restriction, objection | Know, delete, correct, opt out of sale or sharing, limit use of sensitive data | Access and amendment of health records |
| Response time | One month, extendable by two more for complex requests | 45 days, extendable by 45 more | Access requests within 30 days |
| Other duties | Lawful basis, purpose limitation, minimisation, records of processing, breach notice within 72 hours, transfer rules | Notice at collection, honour opt-out signals such as Global Privacy Control | Privacy Rule, Security Rule, breach notification, business associate agreements |
| Fines | Up to €20 million or 4% of global annual turnover | Per-violation civil penalties | Tiered civil and criminal penalties |

Many other laws follow similar patterns (Brazil's LGPD, India's Digital Personal Data Protection Act, US state laws in Virginia, Colorado, Texas and others), so building for GDPR-style rights usually covers most of them.

**Right to erasure on an immutable lake.** Lakehouse files (Parquet) are immutable. Table formats make deletion possible by writing new files, but old files remain until cleaned up. With Apache Iceberg, erasing a user takes three steps:

```sql
-- Spark SQL on an Iceberg table
-- 1. Logical delete: new snapshot without the user's rows
DELETE FROM lake.crm.events WHERE user_id = 'u-123';

-- 2. If the table uses merge-on-read, compact so delete files are applied to data files
CALL lake.system.rewrite_data_files(table => 'crm.events', where => "event_date >= '2026-01-01'");

-- 3. Physically remove old snapshots, and with them the files still holding the rows
CALL lake.system.expire_snapshots(table => 'crm.events',
                                  older_than => TIMESTAMP '2026-11-21 00:00:00',
                                  retain_last => 1);
```

Until step 3 runs, time travel to an older snapshot still shows the deleted rows. Delta Lake is similar: `DELETE`, then `VACUUM` after the retention period (and `REORG TABLE ... APPLY (PURGE)` when deletion vectors are used). In BigQuery, a `DELETE` removes the rows from the current table, but they stay recoverable through **time travel** (2 to 7 days, configurable per dataset) and **fail-safe** (a further 7 days, recoverable only by Google support); your erasure policy should state that deletion completes after those windows.

**Crypto-shredding** is the other approach: encrypt each person's sensitive fields with a per-person key, and to erase them, delete the key. All copies, including backups and old snapshots, become unreadable at once. BigQuery's AEAD functions support it:

```sql
-- one keyset per user
CREATE TABLE gov.user_keys AS
SELECT user_id, KEYS.NEW_KEYSET('AEAD_AES_GCM_256') AS keyset
FROM crm.users;

-- encrypt on write (the user_id as additional data binds ciphertext to its owner)
SELECT u.user_id, AEAD.ENCRYPT(k.keyset, u.email, u.user_id) AS email_enc
FROM staging.users AS u JOIN gov.user_keys AS k USING (user_id);

-- erase: deleting the key makes every copy of email_enc unreadable
DELETE FROM gov.user_keys WHERE user_id = 'u-123';
```

The keys table itself must not be kept in long time travel or backups, or the "deleted" key can be recovered.

**Data residency.** GDPR does not forbid transfers outside the EU, but requires a legal mechanism for them (adequacy decisions such as the EU-US Data Privacy Framework, or standard contractual clauses), and many companies and public bodies require data to stay in a region. On Google Cloud, choose dataset and bucket locations explicitly (`EU`, `europe-west3`), enforce them with the organization policy constraint `gcp.resourceLocations`, and use Assured Workloads for regulated environments. Remember derived data: logs, query results, caches, ML training sets and backups have locations too.

**HIPAA.** Covered entities may only use cloud services under a **business associate agreement** (BAA); Google Cloud signs one covering a listed set of products, and only those may hold PHI. HIPAA defines two ways to de-identify data: **Safe Harbor** (remove 18 types of identifiers, including names, small geographic units, all date elements except year, phone numbers and record numbers) and **Expert Determination** (a qualified expert certifies that re-identification risk is very small). Industry patterns returns to health data pipelines.

**Audit trail design.** Regulators ask you to demonstrate compliance: records of processing (what personal data, which purposes, which systems, retention), a log of every rights request and how it was fulfilled, access logs for personal data, and evidence of deletion (counts per table, job IDs, timestamps), kept without the personal data itself.

> **Pitfall:** Erasing a user from the warehouse but forgetting the raw landing bucket, Kafka topics with long retention, ML feature stores, exported CSVs, BI extracts and backups. Use the catalog and lineage to list every asset containing the user key, and make each pipeline owner responsible for an erasure step.

::: deeper
**Designing for erasure from the start.** Keep personal data in as few places as possible: store identifiers and attributes in a small set of **PII vault tables** keyed by a surrogate ID, and let event and fact tables carry only the surrogate. Erasure then touches a handful of tables, and analytics on facts keeps working (with the person anonymised). Partitioning or clustering large tables on the user key makes `DELETE` cheap; deleting one user from an unclustered 50 TB table scans all of it.

**Batching erasure requests.** Running one `DELETE` per request on a big lake table creates thousands of small snapshots and delete files. Collect requests into a daily or weekly batch (within the legal deadline), apply one `DELETE ... WHERE user_id IN (SELECT user_id FROM gov.erasure_requests WHERE ...)`, then compact and expire snapshots.

**Streams and logs.** Kafka topics are append-only. Options are short retention for topics with personal data, **compacted topics** where a tombstone (a null value for the key) deletes the record after compaction, or crypto-shredding of payloads. Pub/Sub messages expire after the retention period (at most 31 days), which usually fits within deadlines.

**Backups.** Restoring an old backup can resurrect erased data. Either keep backups short enough to expire within the deadline, crypto-shred, or keep an erasure log that is re-applied after every restore.

> **Interview:** "How do you implement GDPR erasure on an Iceberg lakehouse?" Find all tables with the user's key (catalog, lineage); batch requests; run `DELETE` (cheap if the table is clustered or partitioned on the key); compact merge-on-read delete files; expire snapshots older than the delete and remove orphan files so the old data files are physically gone; handle raw zones, streams and backups; record evidence; or use per-user keys and crypto-shredding where physical deletion is impractical.
:::

::: check
?? An Iceberg `DELETE` has run for a user. Why might their data still be readable?
Older snapshots still reference the original data files, so time travel can read them. The data is physically gone only after compaction (for merge-on-read) and snapshot expiry remove those files.

?? What is crypto-shredding?
Encrypting each person's data with their own key and erasing them by destroying the key, which makes every copy, including backups, unreadable.

?? Under HIPAA Safe Harbor, may you keep a patient's full date of admission?
No. Safe Harbor requires removing all elements of dates (except the year) directly related to the individual.
:::

::: resources
- Spec: [GDPR full text](https://gdpr-info.eu/) · the regulation, article by article, with recitals
- Docs: [California Consumer Privacy Act](https://oag.ca.gov/privacy/ccpa) · the California Attorney General's guide to CCPA rights and duties
- Docs: [HIPAA de-identification guidance](https://www.hhs.gov/hipaa/for-professionals/special-topics/de-identification/index.html) · Safe Harbor and Expert Determination explained by HHS
- Docs: [Google Cloud HIPAA compliance](https://cloud.google.com/security/compliance/hipaa) · the BAA and covered products
- Docs: [Apache Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/) · expiring snapshots, removing orphan files and compaction
- Book: [Practical Data Privacy](https://www.oreilly.com/library/view/practical-data-privacy/9781098129453/) · Jarmul; privacy regulations translated into engineering practice
- Blog: [Implementing the GDPR right to be forgotten in Delta Lake](https://www.databricks.com/blog/2022/03/23/implementing-the-gdpr-right-to-be-forgotten-in-delta-lake.html) · the same problem in another table format
:::

### Privacy Engineering & Data Clean Rooms

::: plain
Sometimes you want the answer to a question about many people ("how many customers in Pune bought shoes?") without anyone learning anything about one particular person. Privacy engineering offers tools for that. **Differential privacy** adds a carefully measured amount of random noise to results, like a pollster who blurs the numbers just enough that no single answer can be traced back. A **data clean room** is a neutral meeting room where two companies can analyse their combined data under strict rules, without either one seeing the other's raw records.
:::

**Why anonymisation by removing names fails.** Datasets with names removed have been re-identified again and again by linking them with other data: Netflix ratings with public movie reviews, health records with voter lists through ZIP code, birth date and sex. Formal techniques address this.

**k-anonymity, l-diversity and t-closeness.** A table is **k-anonymous** if every combination of quasi-identifier values is shared by at least k rows, so any person hides in a crowd of k. You get there by generalising (age 34 becomes 30-39) and suppressing rare rows. **l-diversity** adds that each group must contain at least l distinct values of the sensitive attribute; otherwise, if everyone in a group has the same diagnosis, k-anonymity protects nothing. **t-closeness** requires each group's distribution of the sensitive value to be close to the overall distribution.

```python
from collections import Counter, defaultdict

rows = [  # (zip, age, sex, diagnosis): zip, age and sex are quasi-identifiers
    ("411001", 34, "F", "asthma"),
    ("411001", 36, "F", "diabetes"),
    ("411002", 52, "M", "asthma"),
    ("411002", 55, "M", "flu"),
    ("411002", 58, "M", "flu"),
    ("560001", 29, "F", "flu"),
]

def generalise(z, a, s):
    decade = a // 10 * 10
    return (z[:3] + "***", f"{decade}-{decade + 9}", s)

def k_and_l(table):
    groups = defaultdict(list)
    for qi, diagnosis in table:
        groups[qi].append(diagnosis)
    k = min(len(v) for v in groups.values())
    l = min(len(set(v)) for v in groups.values())
    return k, l

raw = [((z, a, s), d) for z, a, s, d in rows]
print("raw:", k_and_l(raw))                     # raw: (1, 1)

gen = [(generalise(z, a, s), d) for z, a, s, d in rows]
print("generalised:", k_and_l(gen))             # generalised: (1, 1)

sizes = Counter(qi for qi, _ in gen)
kept = [(qi, d) for qi, d in gen if sizes[qi] >= 2]   # suppress small groups
print("suppressed:", k_and_l(kept), "rows kept:", len(kept))   # suppressed: (2, 2) rows kept: 5
```

These techniques are easy to explain but fragile: they assume you know which fields are quasi-identifiers and what the attacker already knows.

**Differential privacy (DP)** gives a mathematical guarantee instead. A computation is ε-differentially private if adding or removing any one person changes the probability of any output by at most a factor of e^ε. In practice you add noise calibrated to the **sensitivity** of the query (how much one person can change it) divided by **ε** (epsilon, the privacy budget): smaller ε means more noise and more privacy. For a count, one person changes the result by at most 1, so Laplace noise with scale 1/ε suffices:

```python
import math, random

def laplace(scale, rng):
    u = rng.random() - 0.5                      # uniform in [-0.5, 0.5)
    return -scale * math.copysign(1, u) * math.log(1 - 2 * abs(u))

true_count = 1234          # users in a city
sensitivity = 1            # one person changes a count by at most 1
rng = random.Random(7)
for epsilon in (0.1, 1.0, 10.0):
    scale = sensitivity / epsilon
    errors = [abs(laplace(scale, rng)) for _ in range(10000)]
    print(f"epsilon={epsilon:>4}: noise scale={scale:>4}, mean abs error={sum(errors)/len(errors):.1f}")
# epsilon= 0.1: noise scale=10.0, mean abs error=10.1
# epsilon= 1.0: noise scale= 1.0, mean abs error=1.0
# epsilon=10.0: noise scale= 0.1, mean abs error=0.1
```

An error of about 10 on a count of 1,234 is harmless for analysis, yet it hides whether any particular person is in the data. Every query spends budget, and budgets add up across queries, which is why DP systems track total ε per dataset.

BigQuery supports DP in SQL. You name the **privacy unit** (the column identifying a person) and the budget, and BigQuery bounds each person's contribution and adds noise:

```sql
SELECT WITH DIFFERENTIAL_PRIVACY
  OPTIONS (epsilon = 1.0, delta = 1e-5, max_groups_contributed = 1, privacy_unit_column = user_id)
  city,
  COUNT(*) AS buyers
FROM sales.shoe_purchases
GROUP BY city;
```

A simpler guard is an **aggregation threshold**: results are only returned for groups with at least N distinct privacy units.

```sql
SELECT WITH AGGREGATION_THRESHOLD OPTIONS (threshold = 50, privacy_unit_column = user_id)
  city, COUNT(*) AS buyers
FROM sales.shoe_purchases
GROUP BY city;
```

**Data clean rooms.** Two organizations (a retailer and a brand, an advertiser and a publisher) want to measure overlap or campaign results on combined data, but cannot legally or commercially share raw customer records. A clean room lets each side contribute data, enforces **analysis rules** on what may be queried (only aggregates above a threshold, only differentially private results, only joins on approved keys, no row-level output), and returns only permitted results. Options include **BigQuery data clean rooms** (built on BigQuery sharing: each contributor shares views with analysis rules, and subscribers query them without seeing raw rows), Snowflake data clean rooms, Ads Data Hub for Google advertising data, and vendor platforms such as LiveRamp.

**Other techniques**: **secure multi-party computation** (parties jointly compute a function on private inputs with cryptographic protocols, so no party sees another's input), **federated learning** (models are trained on devices or in each organization and only model updates are shared, often with DP added), **homomorphic encryption** (computing on encrypted data; still slow for general use) and **trusted execution environments** (Confidential VMs and Confidential Space on Google Cloud, which keep data encrypted in memory and attest the code that runs).

> **Pitfall:** Releasing many slightly different aggregates of the same data. Each one may pass a threshold, but subtracting two of them (all customers, and all customers except those in one small group) can reveal an individual. Differential privacy with a tracked budget is the principled defence; thresholds alone are not.

::: deeper
**Choosing epsilon.** There is no universal value. Deployed systems have used ε from below 1 to around 10 per release or per day. Smaller is more private. Decide with privacy and legal teams, document the choice, and account for repeated releases (sequential composition: budgets add).

**Contribution bounding.** Sensitivity is only bounded if each person contributes a bounded amount. DP SQL engines therefore clip each person's contribution (`max_groups_contributed`, `contribution_bounds_per_group` in BigQuery). Choosing bounds too small biases results; too large adds noise.

**Delta.** (ε, δ)-DP allows a small probability δ of a larger privacy loss, which enables Gaussian noise and some algorithms (such as dropping rare groups). δ should be much smaller than one divided by the number of people.

**DP in machine learning.** DP-SGD (Abadi and others, 2016) clips each example's gradient and adds noise during training, giving a DP guarantee for the model. Federated learning without DP does not prevent models from memorising individual data.

**Clean rooms are governance, not magic.** Analysis rules must be designed for the actual threat: who is the attacker (the other party), what auxiliary data they hold, and how many queries they may run. Combine thresholds or DP with query limits, approved query templates and audit logs.

> **Interview:** "Two companies want to know how many customers they share without exchanging customer lists. How?" Use a data clean room: each side contributes hashed or tokenised identifiers in shared views with analysis rules (list overlap or aggregation threshold, no row-level output), the join and count run inside the clean room, and only the aggregate count, optionally with DP noise, is returned. Alternatively, private set intersection protocols compute the overlap cryptographically.
:::

::: check
?? A dataset is 5-anonymous, but in one group every person has the same diagnosis. What is the problem, and which property addresses it?
An attacker who knows someone is in that group learns their diagnosis. l-diversity requires each group to contain at least l distinct sensitive values.

?? In differential privacy, what happens to the noise when you halve epsilon?
The noise scale doubles (scale is sensitivity divided by epsilon), so results are less accurate and more private.

?? What does a data clean room return to its participants?
Only results permitted by the analysis rules, typically aggregates above a threshold or with differential privacy noise, never the other party's raw rows.
:::

::: resources
- Docs: [Use differential privacy in BigQuery](https://docs.cloud.google.com/bigquery/docs/differential-privacy) · DP SQL syntax, privacy units and contribution bounds
- Docs: [BigQuery data clean rooms](https://docs.cloud.google.com/bigquery/docs/data-clean-rooms) · contributors, analysis rules and subscribers
- Book: [Programming Differential Privacy](https://programming-dp.com/) · Near and Abuah; a free online book with runnable examples
- Book: *The Algorithmic Foundations of Differential Privacy* by Cynthia Dwork and Aaron Roth · the foundational text, free from the authors
- Paper: [Deep Learning with Differential Privacy](https://arxiv.org/abs/1607.00133) · Abadi and others, 2016: DP-SGD for training models
- Repo: [Google differential privacy libraries](https://github.com/google/differential-privacy) · the open-source DP libraries behind BigQuery's implementation
:::

### Consent Management & Privacy Orchestration

::: plain
When a website asks "may we use cookies for advertising?" and you click "no", that choice must travel to every system that holds your data: analytics, the warehouse, the email tool, the ad platforms. **Consent management** records what each person agreed to, for which purposes, and when. **Privacy orchestration** is the plumbing that carries those choices, and requests such as "show me my data" or "delete it", to every system automatically, like a building's fire alarm that, once pressed, closes every fire door on every floor.
:::

**Purpose limitation.** Under GDPR, data collected for one purpose (fulfilling an order) may not be reused for an incompatible one (targeted advertising) without a lawful basis. **Consent** is one of six lawful bases (others include contract, legal obligation and legitimate interests); where it is the basis, it must be freely given, specific, informed and as easy to withdraw as to give. So consent is recorded **per purpose** (analytics, personalisation, marketing email, advertising, sharing with partners), not as one yes or no.

**The consent record.** A good consent store keeps history, not just current state, because you must prove what applied when the data was used:

| Field | Example |
|---|---|
| Subject | `user_id = u1` (or device ID before login) |
| Purpose | `marketing` |
| Decision | granted or denied |
| Valid from / to | `2026-01-01` to `2026-06-01` |
| Source and evidence | web banner v12, notice text version, IP country |
| Mechanism | banner click, account settings, Global Privacy Control signal |

That is a slowly changing dimension of type 2 (Data modeling), and enforcement becomes a point-in-time join: use an event for a purpose only if consent for that purpose was granted at the event's time. This runs with Python's SQLite as is:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE consent (user_id TEXT, purpose TEXT, granted INT, valid_from TEXT, valid_to TEXT);
INSERT INTO consent VALUES
 ('u1', 'marketing', 1, '2026-01-01', '2026-06-01'),
 ('u1', 'marketing', 0, '2026-06-01', '9999-12-31'),
 ('u2', 'marketing', 1, '2026-03-01', '9999-12-31');
CREATE TABLE events (user_id TEXT, event_date TEXT, page TEXT);
INSERT INTO events VALUES
 ('u1', '2026-05-20', '/shoes'), ('u1', '2026-07-02', '/bags'),
 ('u2', '2026-07-03', '/shoes'), ('u3', '2026-07-03', '/hats');
""")
rows = db.execute("""
SELECT e.user_id, e.event_date, e.page
FROM events e
JOIN consent c
  ON c.user_id = e.user_id AND c.purpose = 'marketing' AND c.granted = 1
 AND e.event_date >= c.valid_from AND e.event_date < c.valid_to
ORDER BY e.event_date
""").fetchall()
for r in rows:
    print(r)
# ('u1', '2026-05-20', '/shoes')
# ('u2', '2026-07-03', '/shoes')
```

User u1's July event is excluded because they withdrew consent in June; u3 never consented, so the inner join excludes them by default.

**Propagating consent.** Consent changes are events. Publish them to a Pub/Sub topic (or Kafka) from the consent platform; each downstream system subscribes: the warehouse updates the consent dimension, the email tool unsubscribes, the ad audience sync removes the user, the feature store stops serving personalisation. Downstream tables built for a purpose (a marketing audience mart) should be rebuilt or filtered with the consent join on every run, so a withdrawal takes effect at the next build.

**Standards and signals.**

- **IAB Transparency and Consent Framework (TCF) v2.2**: the advertising industry's standard in Europe; the consent banner produces a **TC string** encoding purposes and vendors, passed along with ad requests.
- **Google Consent Mode v2**: tags on websites and apps adjust behaviour based on four consent states (`ad_storage`, `analytics_storage`, `ad_user_data`, `ad_personalization`).
- **Global Privacy Control (GPC)**: a browser signal meaning "do not sell or share my data", which California law requires businesses to honour.

**DSAR automation.** A **data subject access request** (access, deletion, correction) follows the same steps every time, so it should be a workflow, not a ticket:

1. **Intake and identity verification**: confirm the requester is the person (or their authorised agent).
2. **Discovery**: find every system and table holding their data. This is where the catalog's PII classification and lineage from the Metadata group pay off.
3. **Fulfilment**: export their data in a portable format, or run the erasure steps from the previous topics, system by system (each with an owner and an API or job).
4. **Evidence**: record what was done, when, and by which job, without keeping the personal data.
5. **Deadline tracking**: one month under GDPR, 45 days under CCPA.

Privacy platforms (OneTrust, Osano, Transcend, Ketch, and the open-source **Fides**) provide consent banners, consent storage, DSAR workflows and connectors to common SaaS tools; data engineers build the connectors for in-house systems and the warehouse.

> **Pitfall:** Storing only the latest consent state. When an auditor asks whether a marketing email sent in March was allowed, you cannot answer if the record now says "denied" and the history is gone. Keep the full history.

::: deeper
**Identity resolution and consent.** Consent given on an anonymous device must be linked to the logged-in user, and consent withdrawn by the user must apply to all their devices. That requires the same identity graph as MDM, with a rule for conflicts (typically the most recent and most restrictive decision wins).

**Enforcement points.** Enforce consent as early as possible: in the tag or SDK (do not collect), at ingestion (route non-consented events to a restricted, short-retention table), and at use (consent joins in purpose-specific marts). Early enforcement reduces how much data you must later delete.

**Purpose-based access control.** Some organizations tag datasets with permitted purposes and require users or jobs to declare a purpose when they query (for example through a service account per purpose). Access is granted only if the purpose matches, which turns purpose limitation into an access control rule (ABAC with "purpose" as a context attribute).

**Event-driven DSAR.** At scale, DSARs become events on a topic; each system's handler performs its part and reports completion to a tracker table, and an orchestrator (Workflows or Managed Airflow) escalates systems that have not completed before the deadline.

> **Interview:** "How would you make sure a user's consent withdrawal is respected in the warehouse?" Store consent as a history table per purpose, propagate changes as events, filter purpose-specific marts with a point-in-time consent join on every build, remove the user from downstream audience syncs, keep evidence, and test the flow end to end with a test user.
:::

::: check
?? Why must consent be recorded per purpose rather than as a single yes or no?
Purpose limitation means data may be used only for purposes the person agreed to (or another lawful basis covers). A person may agree to analytics but not to advertising.

?? Why keep consent history instead of only the current state?
To prove that each past use of the data was allowed at the time, and to process historical data correctly with point-in-time joins.

?? Which part of a DSAR workflow depends most on the data catalog and lineage?
Discovery: finding every system and table that holds the requester's data.
:::

::: resources
- Spec: [IAB Europe Transparency and Consent Framework](https://iabeurope.eu/transparency-consent-framework/) · TCF v2.2 policies and the TC string
- Docs: [Google consent mode overview](https://developers.google.com/tag-platform/security/concepts/consent-mode) · consent states and tag behaviour
- Spec: [Global Privacy Control](https://globalprivacycontrol.org/) · the browser opt-out signal and its legal status
- Docs: [Fides documentation](https://ethyca.com/docs) · open-source privacy engineering: data maps, consent and DSAR automation
- Article: [IAPP resource center](https://iapp.org/resources/) · articles, guidance and tools from the main privacy professionals' association
- Book: [Data Privacy: A runbook for engineers](https://www.manning.com/books/data-privacy) · Bhajaria; consent, data subject requests and privacy tooling from an engineer's view
:::

### Data Retention & Lifecycle Management

::: plain
Not all data needs to live forever, and some must not. A retention policy is a schedule for each kind of data: keep it handy for a while, move it to cheaper storage as it ages, and delete it when there is no longer a reason or a right to keep it. Think of a household's paperwork: this year's bills on the desk, older ones in a box in the attic, tax records kept for the years the law requires, then shredded. A **legal hold** is the exception: when there is a lawsuit, you must keep the relevant papers even if the schedule says shred.
:::

**Why retention matters.** Two forces pull in opposite directions. Laws and regulations require keeping some records for minimum periods (financial records often 5 to 10 years, depending on the country and the record type). Privacy laws require **storage limitation**: personal data kept no longer than necessary for its purpose. Keeping everything forever also costs money, increases breach impact and makes erasure harder. A **retention schedule** settles this per data category:

| Data category | Retention | Reason | Action at end |
|---|---|---|---|
| Raw clickstream with device IDs | 13 months | Analytics need, privacy minimisation | Delete |
| Aggregated daily traffic | 10 years | No personal data | Keep |
| Orders and invoices | 10 years | Tax and accounting law | Archive, then delete |
| Support call recordings | 90 days | Quality checks | Delete |
| Security audit logs | 400 days hot, 7 years archive | Security policy | Archive |

**Storage tiers on Google Cloud.** Cloud Storage has four storage classes with the same API and latency but different prices: **Standard** (frequent access), **Nearline** (about once a month; 30-day minimum storage duration), **Coldline** (about once a quarter; 90 days) and **Archive** (about once a year; 365 days). Colder classes cost less to store and more to read, and deleting an object before its minimum duration is charged as if it had stayed. **Autoclass** moves objects between classes automatically based on access.

**Lifecycle rules** automate transitions and deletion:

```json
{
  "lifecycle": {
    "rule": [
      {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
       "condition": {"age": 30, "matchesStorageClass": ["STANDARD"]}},
      {"action": {"type": "SetStorageClass", "storageClass": "ARCHIVE"},
       "condition": {"age": 365, "matchesStorageClass": ["NEARLINE"]}},
      {"action": {"type": "Delete"},
       "condition": {"age": 3650}}
    ]
  }
}
```

```bash
gcloud storage buckets update gs://shop-raw-events --lifecycle-file=lifecycle.json
```

**Retention in BigQuery** is set with expirations. Partition expiration is the workhorse for event tables: each daily partition is deleted automatically when it ages out.

```sql
ALTER TABLE raw.clickstream SET OPTIONS (partition_expiration_days = 395);   -- about 13 months
ALTER SCHEMA scratch SET OPTIONS (default_table_expiration_days = 7);       -- temporary tables
```

BigQuery also has long-term storage pricing (tables or partitions not modified for 90 days cost about half), so cold data in BigQuery is often cheap enough not to move.

**WORM storage and legal holds.** Some regulations require records to be immutable for a period: write once, read many (WORM). Cloud Storage provides this with **retention policies** on a bucket (no object can be deleted or overwritten until it reaches the retention age) and **Bucket Lock**, which makes the policy permanent: it cannot be removed or shortened, even by the project owner. **Object holds** (temporary or event-based) block deletion of individual objects, which is how you implement a legal hold. **Object Retention Lock** sets retention per object.

```bash
gcloud storage buckets update gs://shop-ledger-archive --retention-period=7y
gcloud storage buckets update gs://shop-ledger-archive --lock-retention-period   # irreversible
gcloud storage objects update gs://shop-ledger-archive/2026/11/ledger.parquet --temporary-hold
```

**Automated deletion workflows.** Expirations cover simple cases. Others need jobs: delete customer records five years after account closure, delete support tickets 90 days after resolution unless under hold. Implement them as scheduled, idempotent pipelines driven by a retention table (category, rule, hold flag), with dry-run counts, approval for large deletions, and an evidence log.

> **Pitfall:** Locking a bucket's retention policy to test it. Bucket Lock cannot be undone: you cannot delete objects, or the bucket, until every object has aged out. Test retention on unlocked buckets and lock only after review.

> **Tip:** Apply retention by **category tag**, not by table. Tag each dataset or bucket with its data category in the catalog, and let a single job read the schedule and apply expirations. New tables then inherit the right retention when they are tagged.

::: deeper
**Soft delete and recovery windows.** Cloud Storage keeps deleted objects for a soft delete period (7 days by default, configurable) so accidental deletions can be undone, and BigQuery has time travel and fail-safe. These protect against mistakes but extend how long "deleted" data exists; include them in retention and erasure statements.

**Retention versus erasure conflicts.** A person asks for erasure, but tax law requires keeping their invoices for 10 years. The legal obligation usually wins for the records it covers, but you must restrict their use (no marketing, limited access) and delete the rest. Model this explicitly: a restriction flag and a "retain until" date per record.

**Cost of transitions.** Lifecycle transitions and early deletions have charges, and many small objects in Archive cost more in per-operation fees than they save. Compact small files before archiving (Data lakes and lakehouses covers compaction).

**Kafka and Pub/Sub retention.** Topic retention (time or size based) is a retention policy too. Long retention on topics with personal data needs the same scrutiny as any table.

> **Interview:** "Design retention for a clickstream platform." Raw events in a partitioned table with 13-month partition expiration; daily aggregates without personal identifiers kept long term; raw files in Cloud Storage moving to Nearline after 30 days and deleted after 13 months; legal holds via object holds when required; all rules generated from a retention schedule keyed by catalog tags, with evidence of deletions.
:::

::: check
?? What happens if you delete a Coldline object after 20 days?
You are charged as if it had been stored for the full 90-day minimum storage duration.

?? What is the difference between a bucket retention policy and Bucket Lock?
A retention policy prevents deleting or overwriting objects younger than the retention period. Bucket Lock makes that policy permanent, so it can no longer be removed or shortened.

?? How do you keep event data for 13 months in BigQuery without a custom deletion job?
Partition the table by event date and set `partition_expiration_days` to about 395, so each partition is deleted automatically when it ages out.
:::

::: resources
- Docs: [Object Lifecycle Management](https://docs.cloud.google.com/storage/docs/lifecycle) · lifecycle actions and conditions for Cloud Storage
- Docs: [Storage classes](https://docs.cloud.google.com/storage/docs/storage-classes) · Standard, Nearline, Coldline and Archive, with minimum durations
- Docs: [Bucket Lock](https://docs.cloud.google.com/storage/docs/bucket-lock) · retention policies, locking and regulatory WORM use
- Docs: [Object holds](https://docs.cloud.google.com/storage/docs/object-holds) · temporary and event-based holds for legal holds
- Docs: [Managing partitioned tables](https://docs.cloud.google.com/bigquery/docs/managing-partitioned-tables) · partition expiration and other retention settings in BigQuery
- Book: *Data Governance: The Definitive Guide* by Evren Eryurek and others · retention, lifecycle and the governance programme around them
:::

### Data Sharing & Marketplace

::: plain
Companies increasingly exchange data: a retailer gives suppliers their sales numbers, a weather company sells forecasts, a bank receives credit data from a bureau. The old way was to email files or set up nightly file transfers, which creates stale copies that nobody controls. Modern **data sharing** is like giving someone a library card for one shelf of your library instead of photocopying the books: they read the live data in place, you decide what they can see, and you can take the card back at any time.
:::

**The problems with copying.** File-based exchange (SFTP, CSV in a bucket) creates copies that drift out of date, multiply storage and pipelines on both sides, cannot be revoked once sent, and leave you no record of how the data is used.

**Live, zero-copy sharing.** Modern warehouses share data **in place**: the consumer queries the provider's storage through their own account, with the provider's access rules applied.

- **BigQuery sharing (formerly Analytics Hub)**: a provider creates a **data exchange** (private, or public in the marketplace) and publishes **listings**; each listing points at a shared dataset (tables, views, routines) or a Pub/Sub topic. A subscriber subscribes, which creates a **linked dataset** in their project: a read-only pointer, not a copy. Subscribers pay for their own queries; the provider pays storage. Providers can enable **data egress controls** (prevent copying, exporting and downloading results), see subscriber usage, and revoke subscriptions. Row-level security, column-level security and masking on the shared objects still apply. Commercial listings can be sold through Google Cloud Marketplace.
- **Snowflake Secure Data Sharing**: the provider creates a **share** with grants on databases, schemas, tables and secure views, and adds consumer accounts. Consumers create a read-only database from the share; no data moves within the same region. Across regions or clouds, the provider replicates the data or uses Listings with cross-cloud auto-fulfilment. The Snowflake Marketplace hosts public and paid listings.
- **Delta Sharing**: the open protocol covered in the Unity Catalog topic, readable by Spark, pandas, BI tools and other platforms, so the recipient does not need the provider's platform.
- **Open table formats**: Iceberg tables in a shared Cloud Storage bucket, exposed through a catalog with credential vending (Lakehouse runtime catalog, Unity Catalog, Apache Polaris), let different engines in different organizations read the same tables under catalog-managed access.

**Designing a share.**

1. **Share views, not tables**, or a curated dataset built for the consumer. Views decouple the consumer's interface from your internal schema and let you filter rows (one supplier sees only their products) and exclude columns.
2. **Apply row-level policies** for multi-tenant shares, so one shared dataset serves many consumers, each seeing only their rows.
3. **Agree a contract**: schema, refresh frequency, semantics, deprecation notice period. A share is a public API.
4. **Control egress**: disable export and copy where the data is sensitive or licensed.
5. **Monitor usage**: who queries what and how often, both for value and for misuse.

```sql
-- provider side: a curated, filtered view for a supplier portal
CREATE OR REPLACE VIEW share_suppliers.weekly_sales AS
SELECT s.supplier_id, p.sku, DATE_TRUNC(o.order_date, WEEK) AS week,
       SUM(o.quantity) AS units, SUM(o.net_amount) AS revenue
FROM marts.order_lines AS o
JOIN marts.products AS p USING (sku)
JOIN marts.suppliers AS s USING (supplier_id)
GROUP BY 1, 2, 3;
-- then: authorise the view on marts, add a row access policy per supplier group,
-- and publish the share_suppliers dataset as a listing in a private exchange
```

**Monetisation models.** Free (to partners, or open data to build an ecosystem), subscription (flat fee per period), usage-based (per query or per row), and tiered access (sample free, full data paid). Marketplaces handle billing and contracts.

> **Pitfall:** Sharing a table directly and later renaming a column. Every consumer's queries break, and unlike internal users you cannot fix them yourself. Share stable views, version breaking changes (`weekly_sales_v2`) and announce deprecations.

> **Tip:** Remember who pays. In BigQuery and Snowflake sharing the consumer pays for compute on their side, which is fair for heavy users; for small partners who cannot pay, consider pre-aggregated, small listings.

::: deeper
**Regions and egress.** Zero-copy sharing works best within a region. A linked dataset in BigQuery sharing must be in the same location as the source; for other regions the provider sets up replicas (cross-region dataset replication) or separate listings. Snowflake replicates shares for cross-region consumers. Delta Sharing reads from the provider's bucket wherever the recipient is, so the provider pays network egress.

**Streaming sharing.** BigQuery sharing can share Pub/Sub topics, so subscribers receive a live stream of events rather than polling tables. This suits real-time feeds (prices, inventory) between partners.

**Clean rooms are sharing with rules.** A data clean room is a sharing exchange where the shared views carry analysis rules (thresholds, DP, join restrictions), so subscribers get only aggregates. The same infrastructure serves both.

**Licensing and lineage.** Shared and purchased data comes with licence terms (permitted uses, attribution, no redistribution). Record them as metadata on the linked datasets in the catalog and propagate them through lineage, so a derived table built on licensed data is flagged with the same restrictions.

> **Interview:** "How would you share sales data with 300 suppliers so each sees only their own products?" One curated view or dataset, a row access policy keyed on a supplier mapping table (or one policy per supplier group), published as a private listing in BigQuery sharing; each supplier subscribes, gets a linked dataset and pays their own queries; egress controls on, usage monitored, and a versioned contract for schema changes.
:::

::: check
?? What does a BigQuery sharing subscriber receive when they subscribe to a listing?
A linked dataset in their own project: a read-only pointer to the provider's shared dataset, not a copy.

?? Why share views rather than base tables?
Views give a stable interface independent of the internal schema, and let you filter rows and hide columns for each consumer.

?? In zero-copy sharing, who usually pays for storage and who for queries?
The provider pays storage; the consumer pays for the queries they run.
:::

::: resources
- Docs: [Introduction to BigQuery sharing](https://docs.cloud.google.com/bigquery/docs/analytics-hub-introduction) · exchanges, listings, linked datasets and egress controls
- Docs: [Snowflake Secure Data Sharing](https://docs.snowflake.com/en/user-guide/data-sharing-intro) · shares, consumers and how zero-copy sharing works
- Spec: [Delta Sharing](https://delta.io/sharing/) · the open sharing protocol and its connectors
- Docs: [Authorized views](https://docs.cloud.google.com/bigquery/docs/authorized-views) · exposing curated views without granting access to base tables
- Book: [Data Mesh](https://www.oreilly.com/library/view/data-mesh/9781492092384/) · Dehghani; data products as shareable interfaces with contracts
:::

### Data Mesh Governance

::: plain
In a **data mesh**, each business area (sales, marketing, logistics) owns and publishes its own data as products, instead of one central data team doing everything. That raises an obvious worry: with twenty teams publishing data, how do you stop twenty different standards and twenty security holes? The answer is like a federation of states: a few rules are national and apply everywhere (privacy, security, shared identifiers), each state runs its own affairs within them, and the rules are built into the shared roads and infrastructure so they are followed automatically.
:::

Zhamak Dehghani described data mesh with four principles:

1. **Domain ownership**: the teams closest to the data own it end to end.
2. **Data as a product**: each published dataset has an owner, documentation, quality guarantees, an interface and consumers, like a product.
3. **Self-serve data platform**: a central platform team provides the tools so domains do not each build their own infrastructure.
4. **Federated computational governance**: governance decisions are made jointly by domain and central representatives, and enforced **automatically by the platform** rather than by manual review.

The fourth principle is this topic. It has two halves.

**Federated: who decides.** A governance group with representatives from each domain, the platform team, security, legal and privacy agrees on **global policies**: the few rules every data product must follow because they affect everyone. Everything else is a **local** decision for the domain.

| Global (everyone) | Local (each domain) |
|---|---|
| Classification of PII and sensitive data; masking rules | Internal table design and modeling |
| Identity, access request and audit standards | Tools for transformation within the platform's options |
| Shared identifiers (customer ID, product ID) and their semantics | Release schedule of their products |
| Required product metadata: owner, description, SLOs, contract | Which quality checks beyond the mandatory ones |
| Retention schedule, erasure handling | Internal naming conventions |
| Interoperability: formats, time zones, currency handling | Team processes |

**Computational: how rules are enforced.** Policies are code that runs in the platform, so following them is automatic and violating them is blocked:

- **At creation**: the platform's templates (Terraform modules, a data product scaffold) create datasets with the right location, labels, encryption and baseline IAM.
- **In CI**: a data product's descriptor is validated on every pull request: owner present, classification of every column, masking for PII, freshness SLO declared.
- **At run time**: policy tags and masking apply from classifications; quality checks and contracts are executed by the pipeline; organization policies (locations, no public access, no service account keys) are enforced by Google Cloud itself.
- **Continuously**: discovery scans flag unclassified PII; dashboards show each domain's compliance.

A CI check of data product descriptors, runnable with only the standard library:

```python
import json

# data product descriptors as a domain team would commit them (normally YAML files)
products = json.loads("""[
 {"name": "orders", "domain": "commerce", "owner": "commerce-data@example.com",
  "tier": "gold", "freshness_hours": 2, "columns": {"order_id": "none", "email": "pii"},
  "masked": ["email"]},
 {"name": "clicks", "domain": "marketing", "owner": "",
  "tier": "gold", "columns": {"user_id": "pii", "page": "none"}, "masked": []}
]""")

def violations(p):
    found = []
    if not p.get("owner"):
        found.append("every product needs an owner")
    if p.get("tier") == "gold" and "freshness_hours" not in p:
        found.append("gold products must declare a freshness SLO")
    for col, cls in p["columns"].items():
        if cls == "pii" and col not in p.get("masked", []):
            found.append(f"PII column '{col}' must be masked")
    return found

failed = False
for p in products:
    for v in violations(p):
        failed = True
        print(f"{p['domain']}.{p['name']}: {v}")
print("CI result:", "FAIL" if failed else "PASS")
# marketing.clicks: every product needs an owner
# marketing.clicks: gold products must declare a freshness SLO
# marketing.clicks: PII column 'user_id' must be masked
# CI result: FAIL
```

In a real platform the same rules might be written in Rego and run with `conftest` against YAML and Terraform plans.

**Data mesh on Google Cloud.** A common layout gives each domain its own projects (separate billing, IAM and quotas), with BigQuery datasets as the storage for products. **Knowledge Catalog** provides the shared layer: a central catalog and glossary across all domain projects, **data products** that bundle assets with owners, documentation, access groups and contracts, data quality scans and lineage across domains. **BigQuery sharing** publishes products across domains (and to partners) without copies. Organization policies, VPC Service Controls and Terraform modules provide the computational guard rails. Knowledge Catalog also supports organising assets into lakes (one per domain) and zones.

**Interoperability** is what makes a mesh more than a collection of silos. Products from different domains can only be joined if they agree on identifiers (one customer ID from MDM), units, time zones (UTC timestamps), currency handling and formats. These are global policies with the highest value, because they make cross-domain analysis possible.

> **Pitfall:** Calling a reorganization a data mesh while governance stays manual. If every new product waits weeks for a central review board, domains bypass the process; if there is no review at all, standards diverge. Put the mandatory rules into templates and CI, and keep the governance group for deciding rules, not approving each change.

::: deeper
**Fitness functions.** Borrowing from evolutionary architecture, a mesh tracks **fitness functions** per domain: share of products with complete metadata, share passing quality SLOs, share of PII classified and masked, number of products with active consumers. They make governance measurable and comparable without central micromanagement.

**Data contracts across domains.** Each product publishes a contract (schema, semantics, SLOs, change policy). Consumers in other domains depend on the contract, not on internal tables. Breaking changes require a new version and a deprecation period, checked in CI by comparing the new schema with the published one.

**Where mesh does not fit.** Small organizations with one data team gain little and pay coordination overhead. Mesh works when there are many domains with real data expertise and the central team has become a bottleneck. Many companies end up with a hybrid: a central team owns shared core products (customer, product master data) and the platform, while domains own their specific products.

**Cost governance.** With domain projects, each domain sees and owns its BigQuery bill. Global policies can still set guard rails: reservations per domain, quotas on on-demand bytes, labels required on every job for chargeback.

> **Interview:** "What does 'federated computational governance' mean in practice?" A cross-domain group decides a small set of global policies (classification, access, identifiers, required metadata, retention), and the self-serve platform enforces them automatically through templates, CI checks on data product descriptors, policy tags and masking, organization policies and continuous scanning, while domains decide everything else locally.
:::

::: check
?? Give two examples of global policies and two examples of local decisions in a data mesh.
Global: PII classification and masking rules; required product metadata such as owner and SLOs (also shared identifiers, retention, access standards). Local: internal table design; release schedule (also choice of tools within the platform, extra quality checks).

?? What makes governance "computational"?
Policies are implemented as code in the platform (templates, CI checks, engine-enforced policies, organization policies, continuous scans), so they are applied automatically instead of by manual review.

?? Why are shared identifiers a global concern?
Without the same customer and product identifiers, products from different domains cannot be joined, so cross-domain analysis breaks and the mesh becomes a set of silos.
:::

::: resources
- Book: [Data Mesh](https://www.oreilly.com/library/view/data-mesh/9781492092384/) · Dehghani; the four principles and federated computational governance
- Article: [Data Mesh Principles and Logical Architecture](https://martinfowler.com/articles/data-mesh-principles.html) · Dehghani's foundational article on martinfowler.com
- Docs: [About data products in Knowledge Catalog](https://docs.cloud.google.com/knowledge-catalog/docs/data-products-overview) · bundling assets, owners, access and contracts on Google Cloud
- Docs: [Build a data mesh with Knowledge Catalog](https://docs.cloud.google.com/knowledge-catalog/docs/build-a-data-mesh-quickstart) · lakes, zones and assets per domain
- Article: [Data Mesh Architecture](https://www.datamesh-architecture.com/) · practical patterns, including governance and data contracts
- Docs: [Conftest](https://www.conftest.dev/) · running OPA Rego policies against configuration files in CI
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Find columns that may hold emails | `SELECT table_name, column_name FROM region-eu.INFORMATION_SCHEMA.COLUMNS WHERE LOWER(column_name) LIKE '%email%'` |
| Most-used tables | Count `referenced_tables` in `INFORMATION_SCHEMA.JOBS_BY_PROJECT` over 30 days |
| Lineage standard | OpenLineage: Run, Job, Datasets, facets; Marquez or Knowledge Catalog as backend |
| Spark lineage | `spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener` |
| DataHub ingestion | `datahub ingest -c recipe.yml` |
| Unity Catalog row filter | `ALTER TABLE t SET ROW FILTER f ON (col)` |
| Entity resolution steps | Standardise, block, compare and score, cluster, survivorship, stable golden IDs |
| Quality dimensions | Completeness, uniqueness, validity, accuracy, consistency, timeliness |
| Publish safely | Write-audit-publish; circuit breaker before the publish step |
| Observability pillars | Freshness, volume, schema, distribution, lineage |
| ABAC row policy in BigQuery | `FILTER USING (region IN (SELECT region FROM gov.user_regions WHERE email = SESSION_USER()))` |
| Expiring access | IAM Condition `request.time < timestamp("...")` |
| Ranger rule of thumb | Plugins decide locally from cached policies; deny beats allow; tag policies first |
| Long-term audit logs | Aggregated sink to BigQuery or a locked log bucket in a security project |
| Exfiltration guard | VPC Service Controls perimeter around sensitive projects |
| Quantum risk | AES-256 at rest is fine; migrate key exchange and signatures to ML-KEM, ML-DSA (hybrid first) |
| Erase from Iceberg | `DELETE`, compact, `expire_snapshots`, remove orphan files |
| Crypto-shredding | Per-user keyset (`KEYS.NEW_KEYSET`), `AEAD.ENCRYPT`; delete the key to erase |
| Differential privacy in SQL | `SELECT WITH DIFFERENTIAL_PRIVACY OPTIONS (epsilon=..., privacy_unit_column=...)` |
| Event retention in BigQuery | `ALTER TABLE t SET OPTIONS (partition_expiration_days = 395)` |

### Practice

::: try
**1. Impact analysis.** The `staging.orders.amount` column is about to be renamed to `amount_gross`. Describe how you would find everything affected and avoid breaking consumers, using the tools from this section.
:::

?? A solution to 1
Query the lineage graph (Knowledge Catalog lineage, DataHub or Marquez) for column-level downstream consumers of `staging.orders.amount`: tables, views, dashboards and models. For SQL-only projects, dbt's `state:modified+` selection shows the affected models. Notify the owners listed in the catalog. Avoid a breaking change by adding `amount_gross` alongside `amount` (or keeping `amount` as an alias in a view) for a deprecation period, update downstream models, check in CI that no consumer still references `amount`, then remove it. Gaps in lineage coverage (uninstrumented scripts, BI extracts) need a search of query logs for the column name too.

::: try
**2. Quality gate.** Write Knowledge Catalog data quality rules (or dbt tests) for an `orders` table where `order_id` must be unique, `customer_id` must not be null for more than 0.1% of rows, `amount` must be non-negative, and only today's partition should be checked.
:::

?? A solution to 2

```yaml
rules:
- column: order_id
  dimension: UNIQUENESS
  uniquenessExpectation: {}
- column: customer_id
  dimension: COMPLETENESS
  nonNullExpectation: {}
  threshold: 0.999
- column: amount
  dimension: VALIDITY
  rangeExpectation:
    minValue: '0'
rowFilter: "DATE(created_at) = CURRENT_DATE()"
```

The `threshold` is the share of rows that must pass, so 0.999 allows 0.1% nulls. Run it before the publish step and stop the pipeline if it fails.

::: try
**3. Regional access.** Analysts in India must see only Indian customers in `crm.customers`; support staff must see email addresses but analysts must not; the nightly export service account needs everything. Sketch the BigQuery controls.
:::

?? A solution to 3
A row access policy `GRANT TO ('group:analysts-in@example.com') FILTER USING (country = 'IN')`, and a second policy granting the export service account and support `FILTER USING (TRUE)` (or a support-specific filter). A policy tag on `email` with a masking data policy (for example the email mask) for analysts as Masked Readers, and Fine-Grained Reader on the tag for support and the export service account. All grants to groups, the project on Enterprise edition or higher, Data Access audit logs kept in a security project.

::: try
**4. Erasure request.** A customer asks for erasure. Their data is in BigQuery (`crm.customers`, `analytics.events`), an Iceberg table `lake.events` on Cloud Storage, a Kafka topic with 7 days of retention, and nightly backups kept for 30 days. Plan the erasure and say when it is complete.
:::

?? A solution to 4
Verify identity and log the request. Find all assets holding the customer's key through the catalog (PII classification) and lineage. In BigQuery, delete their rows from both tables (cheap if clustered on the customer key); the data is fully gone after the dataset's time travel window plus 7 days of fail-safe. In Iceberg, batch the delete with other requests, compact, expire snapshots older than the delete and remove orphan files. The Kafka topic ages out within 7 days. Backups expire within 30 days; keep an erasure log to re-apply if a backup is restored (or crypto-shred so backups are unreadable at once). Erasure is complete when the last copy (backups, here 30 days) has expired, which fits within GDPR's one month; record evidence of each step without storing the personal data.

::: try
**5. Privacy budget.** A team wants to publish weekly counts of app users per city with differential privacy, using ε = 1 per weekly release. After a year, what is the total privacy loss under simple composition, and what could they do about it?
:::

?? Answer to 5
Under simple (sequential) composition budgets add: 52 releases at ε = 1 give a total ε of 52 for a person present all year, which is a weak guarantee. Options: lower ε per release (accepting more noise), release less often or fewer breakdowns, bound each person's contribution per release, use tighter composition accounting (advanced composition or other privacy accounting methods give smaller totals), or publish only aggregates above an aggregation threshold where formal DP is not required. The decision belongs with privacy and legal teams and must be documented.

### What comes next

You now have the governance toolkit: catalogs and lineage that tell you what data exists and how it flows, quality checks and observability that tell you whether to trust it, access controls and audit logs that decide and record who uses it, and privacy techniques and processes that keep personal data lawful from collection to erasure. The next section, **Data engineering for AI and ML**, applies all of this to a fast-growing kind of consumer: feature pipelines, training datasets, feature stores, vector databases and retrieval for generative AI, where lineage, quality, consent and privacy matter at least as much as in analytics.
