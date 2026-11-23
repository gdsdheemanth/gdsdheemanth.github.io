This section is about the last mile of the data platform: turning clean tables into numbers people trust, dashboards they use, experiments they can believe and data that flows back into the tools where the business actually works. It is written for data engineers who build for analysts, and for analysts who are becoming analytics engineers. It assumes you can write SQL comfortably (SQL for data engineering), know facts, dimensions and the star schema (Data modeling, Data warehousing), and have seen how transformation tools and schedulers run models (Pipelines and transformation, Orchestration). Examples use BigQuery and Looker, with dbt, Dataform and open-source BI tools where they are the clearer example.

**What you will be able to do afterwards**

- Explain what an analytics engineer does, structure a dbt or Dataform project into staging, intermediate and mart layers, and document and test it.
- Design a governed self-serve layer: certified marts, a semantic layer that defines each metric once, and row-level and column-level access.
- Decide when pre-aggregation (cubes, rollups, materialized views) is worth it, and explain how Tableau, Power BI, Looker and Superset turn clicks into queries.
- Write funnel, sessionization and cohort retention queries, and explain how product analytics tools collect events and resolve identities.
- Build the data side of an A/B testing platform: assignment, exposure logs, metric pipelines, sample ratio checks and CUPED.
- Serve analytics to many customers safely (embedded analytics and multi-tenancy), draw charts over millions of points, and sync warehouse models back into business tools with reverse ETL.

**The big picture.** Everything in this section sits on top of the warehouse and follows one direction of travel. Raw data lands, is cleaned into **models** (staging, then marts), and is described by a **semantic layer** that names the metrics. Consumers then reach it in four ways: **BI** (dashboards and exploration), **analysis** (funnels, cohorts, experiments), **embedded analytics** (charts inside your own product, for your customers), and **activation** (reverse ETL into CRMs, ad platforms and support tools). The rule that ties it together: define a number once, close to the data, and let every consumer read that one definition.

[[fig:analytics_layers]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Analytics engineering

Pipelines deliver data; analytics engineering turns it into a product that analysts and business users can rely on. This group covers the discipline itself (how a modeling project is organised, tested and documented), the architecture of self-serve analytics (who may see what, and how they find it), and pre-aggregation: the old idea of the OLAP cube and its modern descendants.

The previous sections taught you to move and transform data. Here the question changes from "does the job run?" to "does everyone get the same answer to the same question?" Most of the techniques are about removing ambiguity: one definition per metric, one owner per model, one documented grain per table.

### Analytics Engineering Practices

::: plain
A data engineer is like the team that builds a city's water pipes; an analyst is the person who fills a glass and drinks. The **analytics engineer** works in between, at the treatment plant: taking the raw water that arrives and making sure what comes out of every tap is clean, labelled and the same everywhere. In practice that means writing tested, documented SQL models that turn raw tables into trusted business tables, and treating that SQL like software, with version control, review and tests.
:::

**Where the role sits.** The term spread around 2019, mostly through dbt Labs and the Locally Optimistic community, to describe people who were doing software engineering on SQL. A rough split of responsibilities:

| Concern | Data engineer | Analytics engineer | Analyst |
|---|---|---|---|
| Ingestion, CDC, streaming | Owns | Consumes | Rarely touches |
| Infrastructure, orchestration, cost | Owns | Uses, gives feedback | Uses |
| Staging and mart models | Reviews, supports | Owns | Requests, contributes |
| Metric definitions and semantic layer | Supports | Owns with the business | Co-owns |
| Dashboards and analysis | Rarely | Sometimes | Owns |

The boundaries move with company size; in a small team one person does all three columns. What defines the analytics engineer is not the tool but the habits: models in Git, every change reviewed, tests run in CI, documentation generated from code.

**SQL-centric transformation.** In an ELT stack (extract, load, then transform inside the warehouse), the transformation layer is a directed graph of SQL `SELECT` statements. The tool (dbt, or Dataform, which is built into BigQuery) works out the dependency order from references between models, wraps each `SELECT` in the right `CREATE TABLE` or `MERGE`, and runs them.

**The standard project layout.** dbt Labs' structure guide, followed by most teams, has three layers:

```text
models/
  staging/          one folder per source system
    shop/
      _shop__sources.yml
      _shop__models.yml
      stg_shop__orders.sql
      stg_shop__customers.sql
  intermediate/     reusable building blocks, not exposed to users
    int_orders_with_payments.sql
  marts/            business-facing tables, one folder per area
    finance/
      fct_orders.sql
      dim_customers.sql
```

- **Staging** models are one-to-one with source tables. They rename columns, cast types, convert units and nothing else: no joins, no business logic. Everything downstream reads staging, never the raw source, so a renamed source column is fixed in exactly one place.
- **Intermediate** models hold logic shared by several marts: a join of orders to payments, a deduplication, a pivot.
- **Marts** are the tables people query: facts (`fct_`, one row per event or transaction) and dimensions (`dim_`, one row per entity), at a documented grain.

A staging model:

```sql
-- models/staging/shop/stg_shop__orders.sql
with source as (
    select * from {{ source('shop', 'orders') }}
),

renamed as (
    select
        id                              as order_id,
        customer_id,
        cast(created_at as timestamp)   as ordered_at,
        lower(status)                   as order_status,
        amount_cents / 100.0            as amount
    from source
)

select * from renamed
```

And its tests and documentation, in YAML next to it:

```yaml
models:
  - name: stg_shop__orders
    description: One row per order placed in the shop database.
    columns:
      - name: order_id
        description: Primary key of the order.
        data_tests:
          - unique
          - not_null
      - name: customer_id
        data_tests:
          - not_null
```

`dbt build` runs the models and their tests in dependency order, and stops downstream models when a test fails. `dbt docs generate` turns the descriptions and the dependency graph into a browsable site. Dataform offers the same ideas with SQLX files, `ref()`, and assertions, and runs inside BigQuery without extra infrastructure.

**Data modeling ownership.** Every mart should have a named owner, a stated grain ("one row per order line"), and a primary key with a uniqueness test. The grain is the single most important piece of documentation: most wrong numbers come from joining two tables at different grains and silently duplicating rows (fan-out).

**Documentation culture.** Documentation that lives in a wiki goes stale; documentation in the YAML next to the code is reviewed with every change. A practical rule: a pull request that adds a column to a mart without a description does not get merged.

> **Pitfall:** Putting business logic in staging models ("exclude test orders" in `stg_shop__orders`). The next team that needs test orders cannot get them without a second source of truth. Keep staging faithful to the source and filter in intermediate or mart models.

> **Tip:** Name things for the business, not the source. `ordered_at` and `order_status` survive a migration from one shop platform to another; `created_ts_utc_v2` does not.

::: deeper
**Materializations.** Each model chooses how it is stored: `view` (cheap, always fresh, recomputed on every read), `table` (rebuilt each run), `incremental` (only new or changed rows are merged in, using `is_incremental()` and a `unique_key`), or `ephemeral` (inlined into the models that reference it). A sensible default is views in staging, tables or incremental models in marts. On BigQuery, incremental models should also set `partition_by` and `cluster_by` so the `MERGE` only touches recent partitions.

**Contracts and versions.** For marts consumed by other teams, dbt model **contracts** make the schema a promise: the build fails if the SQL produces different column names or types than the YAML declares.

```yaml
models:
  - name: fct_orders
    config:
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: int64
        constraints:
          - type: not_null
      - name: amount
        data_type: numeric
```

When a breaking change is unavoidable, **model versions** (`fct_orders_v2` alongside `v1`, with a deprecation date) let consumers migrate on their own schedule.

**CI for SQL.** A good pipeline builds only the models changed in the pull request and everything downstream of them (`dbt build --select state:modified+` against the production manifest), in a temporary dataset, and runs the tests there. "Slim CI" of this kind keeps checks fast on projects with thousands of models.

**Unit tests.** Data tests check real data after a build; unit tests (dbt 1.8 and later) check the logic of one model against small, hand-written input rows and expected output, before any real data is touched. Use them for tricky case statements and window logic.

> **Interview:** "What is the difference between a data engineer and an analytics engineer?" The data engineer owns getting data in and the platform it runs on; the analytics engineer owns turning loaded data into tested, documented business models and metric definitions. Both use software engineering practices; the analytics engineer applies them mainly to SQL in the warehouse.
:::

::: check
?? What belongs in a staging model, and what does not?
Renaming, type casting and unit conversion of one source table. Joins, filters that encode business rules, and aggregations belong in intermediate or mart models.

?? Why is the grain of a mart the most important thing to document?
Because joining tables at different grains duplicates rows and inflates sums. Knowing that `fct_orders` is one row per order (not per order line) tells every user how to join and aggregate it safely.

?? What does an enforced model contract add on top of data tests?
It fails the build if the model's column names or data types differ from the declared schema, so downstream consumers are protected from accidental breaking changes before any data is written.
:::

::: resources
- Docs: [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview) · dbt Labs' guide to staging, intermediate and mart layers, with naming conventions
- Docs: [Dataform overview](https://cloud.google.com/dataform/docs/overview) · the SQL workflow tool built into BigQuery: SQLX, dependencies and assertions
- Book: [Analytics Engineering with SQL and dbt](https://www.oreilly.com/library/view/analytics-engineering-with/9781098142377/) · Machado and Russa; modeling, testing and documenting a dbt project end to end
- Article: [What is analytics engineering?](https://www.getdbt.com/what-is-analytics-engineering) · dbt Labs' definition of the role and its practices
- Article: [The Analytics Engineer](https://locallyoptimistic.com/post/analytics-engineer/) · the Locally Optimistic essay that helped name and describe the role
- Docs: [Model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) · enforcing column names and types for models other teams depend on
:::

### Self-Serve Analytics Architecture

::: plain
Self-serve analytics is like a well-run supermarket instead of a shop where you ask the clerk for everything. The shelves (curated tables) are stocked by staff, every product has a clear label (documentation and metric definitions), some aisles need a staff card (access control), and customers find what they want without queuing at the counter. The goal is not "everyone can query everything"; it is "people can answer their own questions with numbers that match everyone else's".
:::

**Why it is hard.** Giving analysts raw access to a warehouse produces the opposite of self-service: twenty versions of "revenue", queries that join at the wrong grain, and personal data in spreadsheets. A governed self-serve design adds layers between raw data and users, each with a clear job.

**The layers, from bottom to top:**

1. **Certified marts.** A small set of tested, documented fact and dimension tables (the previous topic), in datasets that only the transformation pipeline can write.
2. **Semantic layer.** A model that names the business concepts: entities, dimensions, measures and the joins between them. Looker's LookML, the dbt Semantic Layer (MetricFlow) and Cube are the common choices. Users ask for "revenue by region last quarter", and the layer generates correct SQL.
3. **Access control.** Who may see which datasets, rows and columns, enforced in the warehouse so it applies to every tool.
4. **Discovery.** A catalog where people can search for data, see owners, descriptions, freshness and certification status.
5. **Tools.** Dashboards, exploration, notebooks and SQL editors, all reading through the layers above.

**The semantic model as an API.** Treat metric definitions like an API contract between the data team and everyone else. A MetricFlow definition:

```yaml
semantic_models:
  - name: orders
    model: ref('fct_orders')
    defaults:
      agg_time_dimension: ordered_at
    entities:
      - name: order_id
        type: primary
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: ordered_at
        type: time
        type_params:
          time_granularity: day
      - name: order_status
        type: categorical
    measures:
      - name: revenue
        agg: sum
        expr: amount

metrics:
  - name: revenue
    label: Revenue
    type: simple
    type_params:
      measure: revenue
```

Once `revenue` is defined here, a dashboard, a notebook and a spreadsheet integration all request the metric by name and get the same SQL. Changing the definition (say, excluding refunds) is a reviewed pull request that changes it everywhere at once.

**Data contracts for consumers.** For each certified table or metric, publish what consumers can rely on: schema, grain, freshness target ("updated by 07:00 UTC"), owner, and how breaking changes are announced. Enforce the mechanical parts (contracts, tests, freshness checks) in code.

**Row-level and column-level access in BigQuery.** Access is enforced in the warehouse, so no BI tool can bypass it:

```sql
-- Analysts in the EMEA group only see EMEA rows
CREATE ROW ACCESS POLICY emea_only
ON `analytics.fct_orders`
GRANT TO ('group:emea-analysts@example.com')
FILTER USING (region = 'EMEA');
```

For columns, you attach **policy tags** from a taxonomy (for example `pii.email`) to sensitive columns; only principals with the Fine-Grained Reader role on that tag can read the values, and **dynamic data masking** can show others a hash or null instead of an error. **Authorized views** let you share a curated view of a table without granting access to the table itself.

**Discoverability.** On Google Cloud, Knowledge Catalog (formerly Dataplex Universal Catalog, and Data Catalog before that) indexes BigQuery datasets automatically, holds business glossaries and aspects (structured metadata), and shows lineage. Push descriptions from your dbt or Dataform YAML into BigQuery column descriptions (dbt's `persist_docs` setting does this) so the catalog and the SQL editor show the same text.

> **Pitfall:** Building a semantic layer on top of raw tables. The semantic layer describes and combines data; it should not be the place where messy data is cleaned. Put cleaning in models, then describe the clean marts.

> **Tip:** Mark tables by tier. "Certified" tables have owners, tests and freshness targets; "sandbox" datasets are free for experiments but never feed a shared dashboard. A visible badge in the catalog saves endless "which table should I use?" questions.

::: deeper
**Where to enforce what.** A rule of thumb: enforce **security** in the warehouse (row access policies, policy tags, IAM), and **meaning** in the semantic layer. If row filtering lives only in the BI tool, someone with a SQL client sees everything. If metric logic lives only in the warehouse as views, every tool must still know which view to aggregate and how.

**Semantic layer trade-offs.**

| Option | Where it runs | Strengths | Watch out for |
|---|---|---|---|
| Looker (LookML) | Inside Looker, SQL pushed to the warehouse | Mature, joins and symmetric aggregates, fine-grained access filters | Definitions reachable mainly through Looker and its connectors |
| dbt Semantic Layer (MetricFlow) | dbt platform service over the warehouse | Definitions live with the models, tool-agnostic APIs | Needs the dbt platform and supported integrations |
| Cube | Separate service with its own cache | Caching and pre-aggregations, SQL and REST APIs | Another service to run and secure |

**Cost of self-service.** On BigQuery on-demand pricing, every exploration query is billed by bytes scanned. Guard rails: partitioned and clustered marts, `require_partition_filter` on large tables, a per-user or per-project custom quota on bytes scanned per day, and a reservation for BI workloads so cost is predictable.

**Measuring success.** Track how many questions are answered without a ticket to the data team, how many dashboards read from certified tables versus sandboxes, and how often two teams report different values for the same metric. Those numbers tell you whether the architecture works better than any adoption chart.

> **Interview:** "How would you stop five teams reporting five different revenue numbers?" Define revenue once in a semantic layer on top of a tested, certified mart; make every dashboard read the metric by name; route changes through review; and retire dashboards that compute it themselves.
:::

::: check
?? Why should row-level security be enforced in BigQuery rather than only in the BI tool?
Because people reach the warehouse through many tools (SQL editors, notebooks, other BI products). A filter in one tool is bypassed by all the others; a row access policy applies to every query.

?? What problem does a semantic layer solve that certified marts alone do not?
Marts provide clean tables, but each tool and user still writes their own aggregation and join logic. A semantic layer defines measures, dimensions and joins once, so every consumer generates the same SQL for the same metric.

?? What is an authorized view used for?
To share a curated query result (selected columns or rows) with users who have no access to the underlying table. The view is authorized on the source dataset, so readers only need access to the view.
:::

::: resources
- Docs: [Introduction to row-level security](https://cloud.google.com/bigquery/docs/row-level-security-intro) · row access policies, how they combine and how they interact with other features
- Docs: [Introduction to column-level access control](https://cloud.google.com/bigquery/docs/column-level-security-intro) · policy tags and taxonomies for sensitive columns
- Docs: [Authorized views](https://cloud.google.com/bigquery/docs/authorized-views) · sharing query results without sharing the source tables
- Docs: [dbt Semantic Layer](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl) · defining metrics with MetricFlow and querying them from many tools
- Docs: [Knowledge Catalog overview](https://cloud.google.com/dataplex/docs/catalog-overview) · search, glossaries, aspects and lineage for discovery on Google Cloud
- Article: [The data product manager](https://locallyoptimistic.com/post/data-product-manager/) · Locally Optimistic on running analytics as a product with real users
:::

### OLAP Cube Design & Modern Alternatives

::: plain
Imagine a shop owner who, every night, fills in a giant ledger of totals: sales per day per product per store, per month per category, per year per region, every combination anyone might ask about. In the morning any question is answered instantly by looking up the right page. That ledger is an **OLAP cube**: totals computed in advance along every combination of dimensions. The cost is the night's work and the size of the ledger, which is why modern systems precompute only the pages people actually read.
:::

**Cube vocabulary.** A cube is built from a star schema:

- **Measures** are the numbers you aggregate: sales amount, quantity, count of orders.
- A **measure group** is a set of measures that come from one fact table at one grain (sales measures from the sales fact, inventory measures from the inventory snapshot).
- **Dimensions** are the ways you slice: date, product, store, customer.
- **Hierarchies** order a dimension's levels: Year, Quarter, Month, Day; or Category, Subcategory, Product. Users **drill down** a hierarchy and **roll up** back.
- **Aggregations** are the precomputed totals at chosen combinations of levels.

Classic cube servers, such as Microsoft SQL Server Analysis Services (SSAS) in its multidimensional mode, store these aggregations in their own format (**MOLAP**), or leave the data in a relational database and generate SQL (**ROLAP**), or mix both (**HOLAP**). They are queried with **MDX** (Multidimensional Expressions), a language built around axes rather than rows:

```text
SELECT
  [Measures].[Sales Amount] ON COLUMNS,
  [Date].[Calendar Year].MEMBERS ON ROWS
FROM [Adventure Works]
WHERE [Product].[Category].[Bikes]
```

This asks for sales amount (columns) by every calendar year (rows), sliced to the Bikes category.

**Why cubes explode.** If dimension *i* has *L* levels in its hierarchy, plus the "All" level, the number of possible aggregation combinations (called **cuboids**) is the product of (L + 1) over all dimensions:

```python
from math import prod

# levels per dimension (excluding the "All" level)
levels = {"date": 3, "product": 3, "store": 2, "customer": 2}   # year>quarter>month, ...
cuboids = prod(n + 1 for n in levels.values())   # +1 for the "All" level
print(cuboids)                                   # 144
print(2 ** len(levels))                          # 16  (no hierarchies: each dimension in or out)
```

Add a few more dimensions and the full cube becomes larger than the fact table it summarises, and the nightly processing window runs out. Cube designers therefore pick a subset of aggregations, guided by which queries are common.

**What changed.** Columnar warehouses (BigQuery, Snowflake, ClickHouse) scan and aggregate billions of rows in seconds, so most questions no longer need a precomputed answer. SQL grew the cube operators too: BigQuery supports `GROUP BY ROLLUP`, `CUBE` and `GROUPING SETS`, which compute several levels of subtotals in one query:

```sql
SELECT region, product_category, SUM(amount) AS revenue
FROM analytics.fct_orders
GROUP BY ROLLUP (region, product_category);
-- rows per (region, category), per region (category NULL), and a grand total
```

**Pre-aggregation is still useful** when dashboards need sub-second answers for many concurrent users, when the fact table is huge, or when each query would cost money. The modern alternatives precompute selectively and route queries to the summary automatically:

| Approach | How it works | Typical use |
|---|---|---|
| BigQuery materialized views | Incrementally maintained aggregates; the optimizer can rewrite queries on the base table to use them | Repeated dashboard aggregates |
| Looker aggregate awareness | LookML `aggregate_table` definitions; Looker picks the smallest table that can answer | Looker dashboards on large facts |
| Cube pre-aggregations | Rollups built from your warehouse into Cube Store and refreshed on a schedule | API-driven and embedded analytics |
| Druid rollup | Rows are aggregated at ingestion to a chosen time granularity | Real-time slicing of event streams |
| StarRocks async materialized views | Refreshed on a schedule, with transparent query rewrite | Low-latency, high-concurrency serving |
| ClickHouse materialized views and projections | Aggregates maintained on insert into `AggregatingMergeTree` tables | Event analytics at very high volume |

A BigQuery materialized view for a dashboard's daily revenue:

```sql
CREATE MATERIALIZED VIEW analytics.mv_daily_revenue
OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)
AS
SELECT DATE(ordered_at) AS order_date, order_status,
       SUM(amount) AS revenue, COUNT(*) AS orders
FROM analytics.fct_orders
GROUP BY order_date, order_status;
```

A dashboard query against `fct_orders` that sums revenue by status over a date range can be answered from this much smaller view, either because the dashboard reads the view directly or because BigQuery's smart tuning rewrites the query to use it.

> **Pitfall:** Precomputing non-additive measures. You can add daily sums to get monthly sums, but you cannot add daily distinct user counts to get monthly distinct users (the same user appears on many days). Store sketches such as HyperLogLog (`HLL_COUNT.INIT` in BigQuery) for distinct counts, or compute them from detail.

> **Tip:** Start with no pre-aggregation. Measure which dashboard queries are slow or expensive, then add a materialized view or aggregate table for exactly those, and check in the job history that queries actually use it.

::: deeper
**Additivity decides what you can precompute.** Measures are **additive** (sum across every dimension: revenue), **semi-additive** (sum across some dimensions but not time: account balance, inventory level, where you take the last value per period), or **non-additive** (ratios, distinct counts, medians). Cubes and rollups handle additive measures trivially; for ratios, store the numerator and denominator separately and divide at query time; for distinct counts, store mergeable sketches.

**The lattice.** The set of cuboids forms a lattice: any cuboid can be computed from any finer one that contains its dimensions. The classic 1996 paper by Harinarayan, Rajaraman and Ullman gives a greedy algorithm for picking which cuboids to materialize under a space budget, and the same reasoning applies when you decide which aggregate tables to build today.

**Query rewrite.** Automatic routing to a summary (BigQuery smart tuning, StarRocks rewrite, Looker aggregate awareness) only works when the engine can prove the summary is sufficient: the query's dimensions must be a subset of the summary's, filters must be on those dimensions, and the measures must be re-aggregatable. Check each tool's rules; a `COUNT(DISTINCT)` or a filter on a column the summary lacks silently falls back to the base table.

**The cube's afterlife.** The semantic layer inherited the cube's best idea (named measures and dimensions with hierarchies) without its storage format. Benn Stancil's "metrics layer" essay argued exactly this: the business logic of the cube was valuable, the precomputation was an implementation detail.

> **Interview:** "Do we still need OLAP cubes?" Rarely as a separate server. Columnar warehouses answer most aggregate queries directly; pre-aggregation survives as materialized views, aggregate tables and rollups used selectively for latency, concurrency or cost, ideally with automatic query rewrite.
:::

::: check
?? A cube has three dimensions with hierarchies of 2, 3 and 1 levels. How many cuboids does the full cube have?
(2 + 1) × (3 + 1) × (1 + 1) = 24, counting the "All" level of each dimension.

?? You precompute daily active users per country. Can you add them up to get monthly active users per country? Why?
No. Distinct counts are not additive: a user active on ten days is counted ten times. Use HyperLogLog sketches that can be merged, or compute from detail.

?? When is a materialized view a good idea in BigQuery?
When the same aggregation over a large base table is queried repeatedly (dashboards), especially at high concurrency or cost, and the view's dimensions and measures can answer those queries through rewrite.
:::

::: resources
- Docs: [Introduction to materialized views](https://cloud.google.com/bigquery/docs/materialized-views-intro) · incremental maintenance, smart tuning and limits in BigQuery
- Docs: [Aggregate awareness](https://cloud.google.com/looker/docs/aggregate_awareness) · how Looker routes queries to precomputed aggregate tables
- Docs: [Cube pre-aggregations](https://cube.dev/docs/product/caching/using-pre-aggregations) · defining, refreshing and matching rollups in Cube
- Docs: [Druid data rollup](https://druid.apache.org/docs/latest/ingestion/rollup) · aggregating at ingestion time and its trade-offs
- Paper: [Data Cube: A Relational Aggregation Operator](https://arxiv.org/abs/cs/0701155) · Gray and colleagues introduce CUBE and ROLLUP to SQL
- Book: [The Data Warehouse Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) · Kimball and Ross on dimensions, hierarchies and additivity
- Article: [The missing piece of the modern data stack](https://benn.substack.com/p/metrics-layer) · Benn Stancil's case for a metrics layer as the cube's successor
:::

## BI and analysis

With trusted models and a semantic layer in place, this group looks at the tools and analyses that sit on top. First the BI tools themselves: what actually happens between a click on a chart and a query in the warehouse. Then the analyses that data engineers are most often asked to support: funnels, cohorts and retention, the product analytics stacks that collect the events behind them, and the experiment platforms that decide whether a change worked.

The last two topics deal with scale in two directions: serving dashboards to thousands of external customers (embedded analytics and multi-tenancy), and drawing charts over millions of data points without freezing a browser.

### BI Tool Internals

::: plain
A BI tool is a translator. You drag "Region" onto rows and "Revenue" onto columns, and the tool translates that gesture into a query, sends it somewhere, and turns the answer into a chart. Tools differ mainly in **where the answer is computed**: some copy the data into their own fast engine (like keeping a copy of a book at home), others ask the warehouse every time (like phoning the library). Knowing which a tool does tells you how fresh, fast and expensive your dashboards will be.
:::

**Two architectures.** Every BI tool chooses, per data source, between:

- **Extract or import:** the tool copies data into its own engine on a schedule. Queries are fast and do not touch the warehouse, but data is only as fresh as the last refresh, and the copy has size limits and its own security.
- **Live or direct query:** each interaction generates SQL that runs in the warehouse. Data is fresh and security lives in one place, but speed and cost depend on the warehouse.

**Tableau.** Tableau's core is **VizQL**, a visual query language that grew out of the Polaris research project at Stanford. Placing fields on shelves (rows, columns, colour, filters) defines a specification that VizQL compiles both into a query and into a rendering. With a **live connection** the query is SQL in the source's dialect; with an **extract**, data is stored in a `.hyper` file and queried by **Hyper**, Tableau's in-memory columnar engine (introduced in 2018). Since 2020, Tableau's **relationships** (the logical layer) let you relate tables without fixing a join type; Tableau chooses join types per visualization, which avoids fan-out from pre-joined flat tables.

**Power BI.** A Power BI semantic model (formerly "dataset") has a storage mode per table:

| Mode | Where data lives | Query language path | Trade-off |
|---|---|---|---|
| Import | In-memory **VertiPaq** columnar engine, compressed | DAX runs in memory | Fastest; refresh limits and model size limits apply |
| DirectQuery | In the source | DAX is translated to SQL per visual | Fresh; every visual is a source query |
| Dual | Both | Engine chooses per query | Used for dimension tables in composite models |

A **composite model** mixes modes: a large fact table in DirectQuery, small dimensions in Dual, and **aggregation tables** in Import that answer summary queries in memory, falling back to DirectQuery for detail.

**Looker.** Looker never extracts data. A **LookML** project describes the database: **views** map to tables (or derived tables) and declare dimensions and measures; **explores** declare which views can be joined, and how; **models** group explores and point to a connection. At query time Looker generates SQL from the LookML and runs it in the warehouse:

```text
view: orders {
  sql_table_name: `analytics.fct_orders` ;;

  dimension: order_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.order_id ;;
  }
  dimension_group: ordered {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.ordered_at ;;
  }
  measure: revenue {
    type: sum
    sql: ${TABLE}.amount ;;
    value_format_name: usd
  }
}

explore: orders {
  join: customers {
    type: left_outer
    relationship: many_to_one
    sql_on: ${orders.customer_id} = ${customers.customer_id} ;;
  }
}
```

Because primary keys and join relationships are declared, Looker can compute **symmetric aggregates**: when a join fans out rows, it still sums each order's amount once. Looker adds a query cache governed by **datagroups** (cache invalidation tied to a trigger such as "the ETL finished"), **persistent derived tables** (PDTs, tables Looker builds and refreshes in a scratch schema), and aggregate awareness.

**Apache Superset.** Superset is open source and SQL-first. **SQL Lab** is a browser SQL IDE; **datasets** are physical tables or saved SQL ("virtual datasets") with metrics and calculated columns; charts and dashboards query datasets. It talks to databases through SQLAlchemy dialects, runs long queries asynchronously on **Celery** workers, and caches results (commonly in Redis).

**On Google Cloud.** Looker and Data Studio (formerly Looker Studio) query BigQuery live. **BI Engine** is an in-memory acceleration layer inside BigQuery: you reserve memory in a project, and eligible queries from any tool (Looker, Data Studio, Tableau, Power BI via the connectors) are served from memory with no change to the tool. It gives live tools some of the speed of extracts without a copy.

> **Pitfall:** DirectQuery or live dashboards on BigQuery on-demand pricing can be expensive: a dashboard with 15 tiles, opened 500 times a day, sends 7,500 queries a day before anyone filters. Use the BI tool's cache, BI Engine, aggregate tables or materialized views, and a reservation for BI workloads.

> **Tip:** When a dashboard is slow, find the generated SQL first (Looker's SQL tab, Tableau's performance recording, Power BI's Performance analyzer, BigQuery's job history). Most problems are visible there: a missing partition filter, an unexpected join, or one query per tile where one would do.

::: deeper
**Query generation is where correctness lives.** A flat, pre-joined table (orders joined to order lines joined to shipments) breaks every sum that is not at the finest grain. Tools handle this differently: Looker's symmetric aggregates (using primary keys to de-duplicate inside the sum), Tableau's relationships (querying each table at its own level of detail, then combining), Power BI's star-schema modeling with one-to-many relationships and filter direction. Superset leaves it to you. Know which your tool does before trusting a total.

**Caching layers stack up.** From the user inward: the browser, the BI tool's result cache (keyed by generated SQL and user permissions), the BI tool's own engine (Hyper, VertiPaq), BigQuery's 24-hour result cache (only for identical queries on unchanged tables, and not for queries with non-deterministic functions such as `CURRENT_TIMESTAMP()`), BI Engine, and finally storage. A dashboard that is fast "only the second time" is hitting a cache; make the first time fast with pre-aggregation or warmup queries scheduled after each data load.

**Concurrency.** Live dashboards turn users into concurrent queries. A morning spike of 300 people opening the same dashboard means hundreds of queries in a minute. Solutions: cache warming after loads, a BI reservation with autoscaling slots, BI Engine for small hot tables, and fewer tiles per dashboard.

**Security models differ.** Extracts copy data out of the warehouse, so row-level security must be re-implemented in the tool (Tableau user filters, Power BI roles). Live tools can pass the viewer's identity to the warehouse (OAuth to BigQuery), so warehouse row access policies apply; or they connect with one service account and enforce access in the tool (Looker access filters).

> **Interview:** "Extract or live connection?" Live when freshness matters, the warehouse is fast enough, and you want security in one place; extract when the source is slow, queries are repeated, or the source is expensive per query. In practice, many teams use live connections with caching and pre-aggregation in the warehouse.
:::

::: check
?? A Power BI report has a large fact table in DirectQuery and small dimensions in Dual mode. Why Dual for the dimensions?
So that slicers and visuals using only dimensions can be answered from memory, while queries that join to the DirectQuery fact can still send the dimension to the source as part of a single source query.

?? What does Looker need in LookML to compute symmetric aggregates correctly?
A declared primary key on each view and the correct `relationship` on joins, so it can de-duplicate measure values when a join fans out rows.

?? What is BI Engine, and does the BI tool need to change to use it?
An in-memory acceleration layer inside BigQuery, sized by a memory reservation per project. Eligible queries from any connected tool are accelerated automatically; the tool does not change.
:::

::: resources
- Docs: [LookML terms and concepts](https://cloud.google.com/looker/docs/lookml-terms-and-concepts) · views, explores, models, dimensions and measures in Looker
- Docs: [Introduction to BI Engine](https://cloud.google.com/bigquery/docs/bi-engine-intro) · in-memory acceleration for BigQuery queries from BI tools
- Docs: [DirectQuery in Power BI](https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-directquery-about) · how DirectQuery works and its limits
- Docs: [Composite models in Power BI](https://learn.microsoft.com/en-us/power-bi/transform-model/desktop-composite-models) · mixing Import, DirectQuery and Dual storage modes
- Docs: [Extract your data (Tableau)](https://help.tableau.com/current/pro/desktop/en-us/extracting_data.htm) · when and how Tableau copies data into Hyper extracts
- Paper: [Polaris: a system for query, analysis and visualization](https://graphics.stanford.edu/papers/polaris/) · the Stanford research behind Tableau's VizQL
- Docs: [Apache Superset documentation](https://superset.apache.org/docs/intro) · SQL Lab, datasets, caching and async queries
:::

### Funnel, Cohort & Retention Analysis

::: plain
A **funnel** asks how many people make it through each step of a journey: of 100 who looked at a product, 30 added it to the cart and 10 bought it. A **cohort** groups people by when they started, like a school class year. **Retention** asks how many of each class are still around after one week, two weeks, ten weeks. Together they answer the two questions every product team asks: where do people drop off, and do they come back?
:::

**Event tracking schema.** All three analyses start from an **event table**: one row per thing a user did. A good schema has:

| Column | Purpose |
|---|---|
| `event_id` | Unique ID, for deduplication of retried sends |
| `event_name` | A verb-object name from a tracking plan: `product_viewed`, `order_completed` |
| `event_timestamp` | When it happened on the device (plus `received_at` on the server) |
| `anonymous_id` | Device or cookie identifier, present before login |
| `user_id` | Known user identifier, present after login |
| `session_id` | Optional, often derived later |
| `properties` | Event-specific fields (`JSON` or a `STRUCT`) |
| `context` | Device, app version, page, campaign |

In BigQuery, partition the table by event date and cluster by `event_name` and `user_id`, because nearly every query filters on a date range and an event name.

**Session stitching.** A **session** is a run of activity separated by inactivity, conventionally 30 minutes. Compute it with window functions: flag an event as the start of a new session when the gap since the previous event is 30 minutes or more, then take a running sum of the flags.

```sql
WITH ordered AS (
  SELECT
    user_id, event_name, event_timestamp,
    LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS prev_ts
  FROM analytics.events
  WHERE DATE(event_timestamp) BETWEEN '2026-11-01' AND '2026-11-07'
),
flagged AS (
  SELECT *,
    IF(prev_ts IS NULL
       OR TIMESTAMP_DIFF(event_timestamp, prev_ts, MINUTE) >= 30, 1, 0) AS is_new_session
  FROM ordered
)
SELECT *,
  SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS session_number
FROM flagged;
```

**Identity stitching** is related but different: linking the `anonymous_id` used before login to the `user_id` after login, so that a visitor's pre-signup browsing counts towards their journey. The next topic covers it.

**Funnel queries.** A strict funnel counts users who did step 1, then step 2 after step 1, then step 3 after step 2, all within a conversion window from step 1. Here it is as a runnable SQLite example (the logic is identical in BigQuery with `TIMESTAMP_ADD`):

```python
import sqlite3

con = sqlite3.connect(":memory:")
con.execute("CREATE TABLE events (user_id TEXT, event_name TEXT, ts TEXT)")
con.executemany("INSERT INTO events VALUES (?, ?, ?)", [
    ("u1", "view_product", "2026-11-01 10:00"), ("u1", "add_to_cart", "2026-11-01 10:05"),
    ("u1", "purchase",     "2026-11-01 10:09"),
    ("u2", "view_product", "2026-11-01 11:00"), ("u2", "add_to_cart", "2026-11-02 09:00"),
    ("u3", "add_to_cart",  "2026-11-01 12:00"), ("u3", "view_product", "2026-11-01 12:01"),
    ("u4", "view_product", "2026-11-01 13:00"), ("u4", "add_to_cart", "2026-11-12 13:00"),
])

funnel = """
WITH v AS (SELECT user_id, MIN(ts) AS t1 FROM events
           WHERE event_name = 'view_product' GROUP BY user_id),
     c AS (SELECT v.user_id, v.t1, MIN(e.ts) AS t2 FROM v JOIN events e
           ON e.user_id = v.user_id AND e.event_name = 'add_to_cart'
          AND e.ts > v.t1 AND julianday(e.ts) <= julianday(v.t1) + 7
           GROUP BY v.user_id, v.t1),
     p AS (SELECT c.user_id FROM c JOIN events e
           ON e.user_id = c.user_id AND e.event_name = 'purchase'
          AND e.ts > c.t2 AND julianday(e.ts) <= julianday(c.t1) + 7
           GROUP BY c.user_id)
SELECT (SELECT COUNT(*) FROM v), (SELECT COUNT(*) FROM c), (SELECT COUNT(*) FROM p)
"""
print(con.execute(funnel).fetchone())   # (4, 2, 1)
```

User `u3` added to cart *before* viewing, so does not count as converting; `u4` added to cart 11 days later, outside the 7-day window. The funnel is 4, 2, 1: 50% from view to cart, 50% from cart to purchase.

**Cohort retention matrix.** Assign each user to the week of their first activity, then count how many are active in each later week:

```sql
WITH firsts AS (
  SELECT user_id, DATE_TRUNC(MIN(DATE(event_timestamp)), WEEK(MONDAY)) AS cohort_week
  FROM analytics.events
  GROUP BY user_id
),
activity AS (
  SELECT DISTINCT user_id, DATE_TRUNC(DATE(event_timestamp), WEEK(MONDAY)) AS active_week
  FROM analytics.events
),
cohort_size AS (
  SELECT cohort_week, COUNT(*) AS users FROM firsts GROUP BY cohort_week
)
SELECT
  f.cohort_week,
  DIV(DATE_DIFF(a.active_week, f.cohort_week, DAY), 7) AS week_number,
  COUNT(*) AS active_users,
  ROUND(COUNT(*) / ANY_VALUE(s.users), 3) AS retention
FROM firsts AS f
JOIN activity AS a USING (user_id)
JOIN cohort_size AS s USING (cohort_week)
GROUP BY f.cohort_week, week_number
ORDER BY f.cohort_week, week_number;
```

Pivot `week_number` into columns in the BI tool, and each row becomes one cohort's retention curve: 1.0 in week 0, then (for a typical app) a steep drop and a flattening tail. A curve that flattens above zero means the product keeps a core of users; one that trends to zero means it does not.

> **Pitfall:** Using `DATE_DIFF(..., WEEK)` for week numbers. It counts week boundaries crossed (Sunday-based by default), not seven-day periods, which shifts users between columns. Truncate both dates to the same week start and divide the day difference by 7, as above.

> **Tip:** Define "active" explicitly. Opening the app, completing a core action and paying are three different retention curves. Agree on one per product and document it next to the metric.

::: deeper
**Retention definitions.** *N-day* (classic) retention counts a user in day N only if active exactly on day N. *Unbounded* (rolling) retention counts them if active on day N or any later day, which is always higher and smoother. *Bracket* retention uses custom ranges (days 1 to 7, 8 to 30). Tools pick different defaults, which is why two dashboards disagree.

**Time zones.** "Day 1" depends on whose midnight. Choose one rule (usually the user's local time zone for consumer apps, UTC for B2B), store the zone with the event, and compute dates with `DATE(event_timestamp, 'Europe/Berlin')`-style conversions.

**Scaling funnels.** The self-join approach above is clear but expensive on billions of events. Alternatives: pre-filter events to only the funnel's event names and date range; aggregate to one row per user with `ARRAY_AGG(STRUCT(event_name, event_timestamp) ORDER BY event_timestamp)` and evaluate steps in a single pass; or use engines with a built-in function, such as ClickHouse's `windowFunnel`.

**Product analytics data models.** Common designs are the plain event table plus `dim_users`; a **sessions** table (one row per session, with start, end, landing page and conversion flags); and the **Activity Schema**, a single narrow table of `(entity, activity, timestamp, features)` rows that turns most journey questions into self-joins on one table. Whatever you choose, keep the raw events immutable and build these as models on top.

> **Interview:** "Write a query for 7-day retention by signup cohort." Find each user's first date, find all active dates, join, compute the day difference, and count distinct users with difference 7 divided by cohort size. Mention the definition of active, time zones, and that the latest cohorts are incomplete.
:::

::: check
?? Why does the funnel above exclude user `u3`?
Because `u3` added to cart before viewing the product. A strict funnel requires each step to happen after the previous one.

?? How do you split a user's events into sessions with a 30-minute inactivity rule in SQL?
Use `LAG` to get the previous event's timestamp per user, flag events where the gap is 30 minutes or more (or there is no previous event), then a running `SUM` of the flag gives the session number.

?? Why are the most recent cohorts in a retention matrix misleading?
Their later weeks have not happened yet, so cells look like zero retention instead of missing. Show them as blank and compare cohorts only on weeks all of them have completed.
:::

::: resources
- Docs: [BigQuery export schema for Google Analytics 4](https://support.google.com/analytics/answer/7029846) · a real event schema at scale: event parameters, user properties and daily tables
- Docs: [Window function calls](https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls) · `LAG`, running sums and frames used for sessions and funnels
- Article: [Best practices for identifying users](https://segment.com/academy/collecting-data/best-practices-for-identifying-users/) · Segment on anonymous IDs, user IDs and when to call identify
- Spec: [Activity Schema](https://www.activityschema.com/) · a single-table model designed for journey, funnel and cohort questions
- Book: *Lean Analytics* by Alistair Croll and Benjamin Yoskovitz · which metrics matter at each stage of a product, including cohorts and retention
- Docs: [Mixpanel retention](https://docs.mixpanel.com/docs/reports/retention) · clear definitions of retention variants as a product analytics tool computes them
:::

### Product Analytics Stacks

::: plain
Product analytics tools are like a flight recorder for an app. Small pieces of code inside the website or app (trackers) note every tap and page view, send them to a collector, and the collector cleans, labels and stores them so you can replay what users did. The tools differ in who owns the recorder's data: some keep it in their own database and give you charts, others put every event straight into your own warehouse.
:::

**The pipeline every stack shares:**

1. **Instrumentation:** an SDK in the web page, mobile app or backend sends events (`track("Order Completed", {...})`).
2. **Collection:** an HTTP endpoint receives batches of events, often from millions of devices, and acknowledges quickly.
3. **Validation and enrichment:** events are checked against a schema or tracking plan and enriched (IP to location, user agent to device, campaign parameters, consent flags).
4. **Identity resolution:** anonymous device identifiers are linked to known users.
5. **Storage and query:** either the vendor's own event database or your warehouse.

**Segment** (part of Twilio) is a customer data platform built around a simple spec of calls: `track` (an action), `identify` (who the user is, with traits), `page` and `screen`, `group` (an account), and `alias`. Events go from **sources** to many **destinations** (analytics tools, ad platforms, the warehouse) with one SDK. **Protocols** enforces a tracking plan; its identity product builds profiles across devices.

**Snowplow** is a pipeline you can run in your own cloud. Trackers send events to a **collector**; the **enrich** step validates each event against a JSON Schema stored in the **Iglu** schema registry and adds enrichments; valid events are loaded into BigQuery (or other warehouses and lakes), and invalid ones are kept as **failed events** instead of being dropped. Snowplow's strength is strict schemas and full ownership of the data. Its core moved from the Apache 2.0 licence to a source-available licence in 2024, so check the licence terms for your use.

**Amplitude and Mixpanel** are analytics applications with their own event stores: columnar engines built for fast funnels, retention and path queries over events grouped by user. You get excellent interactive analysis without writing SQL. Both can now also read from or sync with warehouses.

**Warehouse-native analytics** inverts the model: the events live only in your warehouse (BigQuery, Snowflake, Databricks), and the analytics tool queries them there. The advantages are one copy of the data, joins with business tables (revenue, subscriptions), and your own governance. The costs are query latency and warehouse spend.

**On Google Cloud**, the most common source is **Google Analytics 4 with BigQuery export**: GA4 writes daily tables `events_YYYYMMDD` (and an intraday table) to a BigQuery dataset, one row per event with a repeated `event_params` record of key and value pairs:

```sql
SELECT
  event_date,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page,
  COUNT(*) AS views
FROM `my-project.analytics_123456789.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20261101' AND '20261107'
  AND event_name = 'page_view'
GROUP BY event_date, page
ORDER BY views DESC
LIMIT 20;
```

The `_TABLE_SUFFIX` filter limits the wildcard to one week of daily tables; without it you scan the whole history.

**Identity resolution.** A person browses on a laptop (cookie `anon_a`), signs up, later uses a phone (`anon_b`) and logs in. Each login event links an anonymous ID to a user ID. Resolving identities means finding connected groups of identifiers and choosing one canonical ID per person, which is a union-find (disjoint set) problem:

```python
# Each pair says "these two identifiers belong to the same person".
links = [("anon_a", "user_42"),    # logged in on the laptop
         ("anon_b", "user_42"),    # logged in on the phone
         ("anon_c", "anon_c")]     # never logged in

parent = {}
def find(x):
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]   # path halving keeps chains short
        x = parent[x]
    return x

def union(a, b):
    ra, rb = find(a), find(b)
    if ra != rb:
        if ra.startswith("user_"):      # prefer a known user_id as the canonical identity
            parent[rb] = ra
        else:
            parent[ra] = rb

for a, b in links:
    union(a, b)

print({x: find(x) for x in ["anon_a", "anon_b", "anon_c"]})
# {'anon_a': 'user_42', 'anon_b': 'user_42', 'anon_c': 'anon_c'}
```

In a warehouse you store the result as an **identity map** table (`anonymous_id`, `canonical_user_id`) and join events to it, so history before login is attributed to the user retroactively.

> **Pitfall:** Merging identities on shared devices or shared emails. A family tablet or `info@company.com` can glue hundreds of people into one "user". Set limits (for example, at most N user IDs per device) and never merge two different known user IDs automatically.

> **Tip:** Write a tracking plan before writing tracking code: event names, required properties, types, and owners, reviewed like a schema. Most product analytics problems are instrumentation problems that no query can repair.

::: deeper
**Client-side versus server-side.** Browser trackers are blocked by ad blockers and lose events when a tab closes; mobile SDKs batch events offline and send them late, so `event_timestamp` can be days older than `received_at`. Server-side tracking (from your backend) is reliable for business events such as payments. A common rule: track intent and UI events client-side, and track facts (orders, sign-ups, subscription changes) server-side.

**Clock skew.** Device clocks are wrong. Snowplow and Segment record several timestamps (device sent, collector received) and derive a corrected time. Use the corrected time for ordering and the received time for incremental loading.

**Event stores.** Product analytics engines are column stores partitioned by time and organised by user, so "all events for these users in order" is fast. That is why funnel and path queries feel instant in Amplitude or Mixpanel but need careful SQL in a general warehouse. Open-source options in the same space include PostHog (built on ClickHouse) and RudderStack (a Segment-like collector that loads into your warehouse).

**Consent and privacy.** Collection must respect consent: do not send analytics events before consent where it is required, carry a consent state on every event, and support deletion of a user's events on request. Hash or drop IP addresses after enrichment if you do not need them.

> **Interview:** "How would you build product analytics for a new app on Google Cloud?" Instrument with an SDK (GA4 or a Snowplow or RudderStack tracker) using a reviewed tracking plan; land events in a date-partitioned BigQuery table; build sessions, an identity map and per-user models with dbt or Dataform; and serve funnels and retention through Looker or a warehouse-native analytics tool.
:::

::: check
?? Why does Snowplow keep failed events instead of dropping them?
So that events that failed schema validation can be inspected, fixed (for example by updating the schema) and reprocessed, rather than silently losing data.

?? In a GA4 BigQuery export query, what does the `_TABLE_SUFFIX` filter do?
It restricts the `events_*` wildcard to the daily tables whose names match the date range, so BigQuery only scans those tables.

?? Why should you not merge two different known user IDs automatically?
Because a shared device or account would link different people, corrupting their histories. Only link anonymous identifiers to a known user.
:::

::: resources
- Docs: [Segment Spec](https://segment.com/docs/connections/spec/) · the track, identify, page, group and alias calls that most event pipelines imitate
- Docs: [Snowplow documentation](https://docs.snowplow.io/) · collectors, enrichment, Iglu schemas, failed events and warehouse loaders
- Docs: [Identity management in Mixpanel](https://docs.mixpanel.com/docs/tracking-methods/id-management) · how a product analytics tool links device and user identifiers
- Docs: [BigQuery export schema for Google Analytics 4](https://support.google.com/analytics/answer/7029846) · event tables, nested parameters and user fields
- Docs: [Amplitude documentation](https://amplitude.com/docs) · event model, identity, and the analyses a dedicated tool provides
- Docs: [PostHog documentation](https://posthog.com/docs) · an open-source product analytics stack built on ClickHouse
:::

### A/B Testing Data Infrastructure

::: plain
An A/B test is a fair coin toss repeated millions of times. Each user is randomly put into group A (the current version) or group B (the new one), and because the toss is random, the only systematic difference between the groups is the change you made. If group B buys more, the change caused it. The data engineering job is to make the coin toss truly fair, to record who saw what, and to compute the results correctly.
:::

**The pieces of an experimentation platform:**

1. **Assignment:** deciding each unit's variant, consistently.
2. **Exposure logging:** recording when a unit actually encountered the experiment.
3. **Metric pipeline:** computing each unit's outcome metrics from the warehouse.
4. **Analysis:** comparing variants with the right statistics, plus health checks.
5. **Storage of results:** so decisions can be audited later.

[[fig:analytics_experiment]]

**Randomization unit.** The unit is what you randomize: usually the **user**, sometimes the session, device, or a cluster such as a city or a seller (when users interact, as in marketplaces). Analyse at the same unit you randomize; analysing page views when you randomized users treats correlated views as independent and makes results look more certain than they are.

**Deterministic assignment.** Hash the experiment ID and the unit ID into buckets. No assignment table is needed, any service computes the same answer, and a user stays in the same variant on every visit:

```python
import hashlib

def bucket(experiment: str, user_id: str, buckets: int = 1000) -> int:
    """Deterministic bucket in [0, buckets): the same user always lands in the same place."""
    digest = hashlib.sha256(f"{experiment}:{user_id}".encode()).hexdigest()
    return int(digest[:15], 16) % buckets

def variant(experiment: str, user_id: str) -> str:
    return "treatment" if bucket(experiment, user_id) < 500 else "control"

print(variant("checkout_v2", "user_42") == variant("checkout_v2", "user_42"))  # True
counts = {"control": 0, "treatment": 0}
for i in range(100_000):
    counts[variant("checkout_v2", f"user_{i}")] += 1
print(counts)   # {'control': 49983, 'treatment': 50017}
```

Including the experiment ID in the hash makes assignments independent across experiments, so the same users are not always in treatment together.

**Exposure logs.** Assignment says which variant a user *would* see; **exposure** says they reached the code path where the variants differ. Analyse only exposed users, from their first exposure onward, or the effect is diluted by users who never saw the change. An exposure table:

```sql
CREATE TABLE experiments.exposures (
  experiment_id STRING,
  variant STRING,
  user_id STRING,
  first_exposed_at TIMESTAMP
)
PARTITION BY DATE(first_exposed_at)
CLUSTER BY experiment_id, user_id;
```

**Metric computation.** Compute one row per unit per metric, then compare distributions:

```sql
SELECT
  x.variant,
  COUNT(*) AS users,
  AVG(IFNULL(r.revenue, 0)) AS revenue_per_user,
  STDDEV_SAMP(IFNULL(r.revenue, 0)) AS sd
FROM experiments.exposures AS x
LEFT JOIN (
  SELECT o.user_id, SUM(o.amount) AS revenue
  FROM analytics.fct_orders AS o
  JOIN experiments.exposures AS e
    ON e.user_id = o.user_id
   AND e.experiment_id = 'checkout_v2'
   AND o.ordered_at >= e.first_exposed_at
  GROUP BY o.user_id
) AS r USING (user_id)
WHERE x.experiment_id = 'checkout_v2'
GROUP BY x.variant;
```

Note the `IFNULL(..., 0)`: users who bought nothing must count as zero, not disappear.

**Sample ratio mismatch (SRM).** If you split 50/50 and see 50,800 against 49,200, something is broken (a redirect that loses users, a bot filter applied to one arm, a crash). Check it before reading any metric:

```python
from math import erfc, sqrt

def srm_p_value(n_control: int, n_treatment: int) -> float:
    """Chi-square test (1 degree of freedom) that a 50/50 split really is 50/50."""
    expected = (n_control + n_treatment) / 2
    chi2 = ((n_control - expected) ** 2 + (n_treatment - expected) ** 2) / expected
    return erfc(sqrt(chi2 / 2))      # survival function of chi-square with 1 df

print(f"{srm_p_value(50_100, 49_900):.3f}")   # 0.527  fine: normal noise
print(f"{srm_p_value(50_800, 49_200):.6f}")   # 0.000000  sample ratio mismatch: stop and debug
```

**How many users?** The sample size per group for a two-sided test is about 2(z₁₋α/₂ + z₁₋β)²σ²/δ², where σ is the metric's standard deviation and δ the smallest effect worth detecting:

```python
from math import ceil
from statistics import NormalDist

def users_per_group(sd: float, mde: float, alpha: float = 0.05, power: float = 0.8) -> int:
    z = NormalDist().inv_cdf
    return ceil(2 * (z(1 - alpha / 2) + z(power)) ** 2 * sd ** 2 / mde ** 2)

# revenue per user: standard deviation 40, we want to detect a change of 1.00
print(users_per_group(sd=40, mde=1.0))        # 25117
print(users_per_group(sd=40, mde=0.5))        # 100466: half the effect needs four times the users
```

> **Pitfall:** Peeking. Checking a fixed-horizon test every day and stopping the first time p < 0.05 raises the false positive rate far above 5%. Decide the duration in advance, or use a sequential method designed for continuous monitoring.

> **Tip:** Run an A/A test (both arms identical) on a new platform. It should show SRM-free splits and "significant" results in about 5% of metrics; anything else means a bug in assignment or metric pipelines.

::: deeper
**CUPED (variance reduction).** Much of a metric's variance comes from differences between users that existed before the test (heavy buyers stay heavy buyers). CUPED, from Microsoft's experimentation team (Deng and colleagues, 2013), subtracts the part of the outcome predicted by a pre-experiment covariate X: Y' = Y − θ(X − mean(X)), with θ = cov(X, Y) / var(X). Because X is measured before assignment, the adjustment does not bias the comparison, and variance falls by a factor of (1 − ρ²), where ρ is the correlation between X and Y.

```python
from statistics import covariance, mean, variance

pre  = [10, 12,  9, 30, 25,  5, 14, 20]     # spend in the 4 weeks before the test
post = [11, 14,  9, 33, 27,  6, 15, 23]     # spend during the test

theta = covariance(pre, post) / variance(pre)
x_bar = mean(pre)
adjusted = [y - theta * (x - x_bar) for x, y in zip(pre, post)]

print(round(theta, 3))                         # 1.096
print(round(variance(post), 1))                # 89.4
print(round(variance(adjusted), 2))            # 0.44
print(mean(post) == round(mean(adjusted), 9))  # True: the mean is unchanged
```

Real data is never this well correlated, but reductions of 30 to 50% are common for engagement and revenue metrics, which means the same precision with far fewer users. In SQL, compute θ once per metric across both arms (pooled), then add an adjusted column per user.

**Ratio metrics.** Click-through rate per user is a ratio of two sums (clicks / views) when the unit is the user but the metric is per view. Its variance needs the **delta method**; treating each view as independent understates it.

**Frequentist or Bayesian.** Frequentist analysis reports a p-value and confidence interval for a fixed sample size. Bayesian analysis reports a posterior, such as "probability that B beats A" and "expected loss if we ship B", which many stakeholders find easier to read; it still needs priors and does not make peeking free. Platforms such as GrowthBook offer both. What matters more than the framework is a pre-registered primary metric, guardrail metrics (latency, errors, unsubscribes) and a fixed decision rule.

**Warehouse-native experimentation.** Tools such as GrowthBook, Eppo and Statsig's warehouse-native mode read your exposure and metric tables directly in BigQuery, so metrics are defined once in SQL and results are reproducible. Store each analysis run (experiment, metric, variant, estimate, interval, run time) in a results table for audits.

> **Interview:** "The treatment group has 2% more users than control in a 50/50 test. What do you do?" Treat it as a sample ratio mismatch: do not trust the metrics, run a chi-square test to confirm, and look for causes such as redirects, bot filtering, assignment bugs or logging differences between arms.
:::

::: check
?? Why hash the experiment ID together with the user ID for assignment?
So assignments are deterministic (the same user always gets the same variant without storing anything) and independent between experiments (being in treatment in one test does not predict treatment in another).

?? What is exposure logging, and why analyse only exposed users?
A record of when a user actually reached the code where the variants differ. Users who were assigned but never exposed cannot be affected, so including them dilutes the measured effect.

?? How does CUPED reduce variance without biasing the result?
It subtracts the part of each user's outcome predicted by a covariate measured before the experiment. Because that covariate cannot be affected by the treatment, the difference between arms is unchanged in expectation, while the noise from pre-existing differences is removed.
:::

::: resources
- Book: [Trustworthy Online Controlled Experiments](https://www.cambridge.org/core/books/trustworthy-online-controlled-experiments/D97B26382EB0EB2DC2019A7A7B518F59) · Kohavi, Tang and Xu; the standard reference on experiment design, SRM, variance and pitfalls
- Paper: [Improving the Sensitivity of Online Controlled Experiments (CUPED)](https://exp-platform.com/Documents/2013-02-CUPED-ImprovingSensitivityOfControlledExperiments.pdf) · the original CUPED paper from Microsoft
- Docs: [GrowthBook documentation](https://docs.growthbook.io/) · an open-source, warehouse-native experimentation platform: assignment SDKs, metrics in SQL, Bayesian and frequentist engines
- Article: [How Not To Run an A/B Test](https://www.evanmiller.org/how-not-to-run-an-ab-test.html) · Evan Miller's short explanation of why peeking breaks significance tests
- Blog: [It's All A/Bout Testing: The Netflix Experimentation Platform](https://netflixtechblog.com/its-all-a-bout-testing-the-netflix-experimentation-platform-4e1ca458c15b) · how a large platform handles allocation, metrics and analysis
- Docs: [Firebase A/B Testing](https://firebase.google.com/docs/ab-testing) · Google's experimentation service for mobile apps, with results exportable to BigQuery
:::

### Embedded Analytics & Multi-Tenancy

::: plain
Embedded analytics means putting dashboards inside your own product for your customers, the way a banking app shows you charts of your own spending. The hard part is that thousands of customers share one system, and each must see only their own data, like flats in one building where every key opens exactly one door. Getting the locks right (tenant isolation) matters more than getting the charts pretty.
:::

**What "embedded" means in practice.** Your web app shows a chart or dashboard served by a BI tool (Looker, Superset, Metabase and others) inside an iframe or through a JavaScript SDK, styled to look like your product (**white-label**). The viewer is your customer, who has no account in the BI tool and no idea it exists.

**The signed-URL pattern.** Your backend, which already knows who the user is, asks the BI tool for a short-lived, signed URL or token describing exactly what this viewer may see, and the browser loads that:

1. The user logs into your app.
2. Your backend creates a signed embed request: which dashboard, which user attributes (`tenant_id = 42`), which permissions, an expiry time.
3. The BI tool verifies the signature, creates or updates an embed user, applies the attributes as filters, and serves the content.

Metabase's static embedding, for example, uses a JSON Web Token signed with a secret shared between your server and Metabase. With only the standard library, the signing looks like this:

```python
import base64, hashlib, hmac, json

def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

def sign_embed(payload: dict, secret: str) -> str:
    header = b64url(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    body = b64url(json.dumps(payload, separators=(",", ":")).encode())
    sig = hmac.new(secret.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
    return f"{header}.{body}.{b64url(sig)}"

payload = {
    "resource": {"dashboard": 7},
    "params": {"tenant_id": 42},     # locked parameter: the viewer cannot change it
    "exp": 1793000000,               # expiry as a Unix timestamp
}
token = sign_embed(payload, secret="server-side-secret")
url = f"https://bi.example.com/embed/dashboard/{token}#bordered=false"
print(url[:60])           # https://bi.example.com/embed/dashboard/eyJhbGciOiJIUzI1NiIsI
print(token.count("."))   # 2: header.payload.signature
```

Because the tenant ID is inside the signed payload, a user who edits the URL breaks the signature. Use a maintained JWT library in production; the point here is that the secret never leaves your server.

**Looker** offers signed embedding (your server builds a signed URL with user attributes and permissions) and the Embed SDK. Tenant isolation uses an **access filter** in the explore, tied to a user attribute:

```text
explore: orders {
  access_filter: {
    field: orders.tenant_id
    user_attribute: tenant_id
  }
}
```

Every query from that user then includes `WHERE tenant_id = <their attribute>`, whatever they click. **Superset** has an embedded SDK: your backend requests a **guest token** from Superset's API for a dashboard, optionally with row-level security clauses such as `tenant_id = 42`, and the SDK renders the dashboard with it.

**Multi-tenant data modeling.** Three isolation models, from strongest to cheapest:

| Model | In BigQuery | Strengths | Weaknesses |
|---|---|---|---|
| Project or database per tenant | One project per tenant | Strong isolation, per-tenant billing and quotas, easy deletion | Many objects to manage; cross-tenant analysis is harder |
| Dataset (schema) per tenant | One dataset per tenant, same tables | Clear boundaries, IAM per dataset, per-tenant location possible | Schema changes repeated per dataset |
| Shared tables with `tenant_id` | One table, clustered by `tenant_id` | Simple, cheap, easy cross-tenant analysis | Isolation depends entirely on filters being applied |

With shared tables, cluster (or partition by integer range) on `tenant_id` so each tenant's query reads only its blocks, and enforce the filter in two places: the semantic layer (access filters, guest token RLS) and, where viewers have their own warehouse identity, a row access policy.

> **Pitfall:** Trusting a tenant ID that comes from the browser. Anything in a URL parameter, cookie or JavaScript can be edited. The tenant ID must come from your server's session and travel inside a signed token.

> **Tip:** Write an automated test that logs in as tenant A, requests every embedded dashboard, and asserts that no row from tenant B appears. Run it on every change to LookML, datasets or embed code.

::: deeper
**Connection pooling and noisy neighbours.** An embedded dashboard opened by 2,000 customers at 9 a.m. turns into thousands of warehouse queries. With databases that have connection limits (PostgreSQL, Cloud SQL), put a pooler such as PgBouncer between the BI tool and the database, and cap the BI tool's connections per database. With BigQuery the limit is concurrency and slots: put embedded workloads in their own reservation so one large tenant cannot slow down internal analytics, and consider per-tier reservations (premium tenants get dedicated slots). Cache aggressively: most tenants look at the same few charts, and caches keyed by query plus tenant attribute serve repeat views.

**Serving layer choice.** For very high concurrency and sub-second latency (thousands of tenants refreshing live charts), a warehouse may be the wrong serving engine. Teams copy per-tenant aggregates into a real-time OLAP engine (ClickHouse, Druid, Pinot) or into a serving database (AlloyDB, Bigtable) and build charts on that, keeping the warehouse as the source of truth.

**Cost attribution.** Tag queries with the tenant (BigQuery job labels, or one service account per tier) so you can see the cost per customer and price your analytics feature sensibly. `INFORMATION_SCHEMA.JOBS` shows bytes and slot time per label.

**Data residency.** Some customers require their data to stay in a region. That pushes you to dataset-per-tenant or project-per-tenant models, since a BigQuery dataset has one location.

> **Interview:** "How would you add customer-facing dashboards to a SaaS product?" Choose an isolation model (shared tables with `tenant_id` for most, dedicated datasets for large or regulated tenants); serve through a BI tool's signed embedding with the tenant in a server-signed token and an access filter in the semantic layer; isolate workloads with a separate reservation and caching; and test cross-tenant leakage automatically.
:::

::: check
?? Why must the tenant ID be inside a signed token rather than a URL parameter?
A URL parameter can be edited by the viewer to see another tenant's data. A signature computed with a server-side secret breaks if anything in the payload changes, so the BI tool rejects tampered requests.

?? What does a Looker `access_filter` do?
It adds a filter on a chosen field to every query in the explore, using the value of the viewer's user attribute (for example their tenant ID), so users cannot query rows outside their tenant.

?? When would you choose a dataset per tenant over shared tables with a `tenant_id` column?
When tenants need strong isolation, separate data locations (residency), separate access grants, or easy complete deletion, and the number of tenants is manageable.
:::

::: resources
- Docs: [Looker embedding overview](https://cloud.google.com/looker/docs/embed-overview) · private and signed embedding, the Embed SDK and security considerations
- Docs: [Signed embedding](https://cloud.google.com/looker/docs/signed-embedding) · building signed URLs with user attributes and permissions
- Docs: [Metabase embedding](https://www.metabase.com/docs/latest/embedding/introduction) · static embedding with signed JWTs and the interactive alternatives
- Repo: [Superset embedded SDK](https://www.npmjs.com/package/@superset-ui/embedded-sdk) · guest tokens and row-level security for embedded Superset dashboards
- Docs: [Best practices for multi-tenant workloads on BigQuery](https://cloud.google.com/bigquery/docs/best-practices-for-multi-tenant-workloads-on-bigquery) · isolation models, reservations and quotas for SaaS on BigQuery
- Blog: [Embedded analytics with Superset](https://preset.io/blog/embedded-analytics-unlock-data-driven-decision-making/) · Preset on use cases and architecture for embedding
:::

### Data Journalism & Visualization at Scale

::: plain
A chart is a picture made of marks: dots, bars, lines. A screen has only about two million pixels, so drawing fifty million dots does not show more, it just smears ink and freezes the browser. Visualization at scale is the craft of deciding what to compute in the database and what to draw in the browser, and of choosing tools that draw quickly. Data journalists add a second craft: telling a clear, honest story with the result.
:::

**Grammars of graphics.** Modern charting libraries describe a chart as data plus **encodings**: which field maps to x, y, colour, size. **Vega-Lite** does this declaratively in JSON, and compiles to a lower-level Vega specification that renders to SVG or Canvas:

```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "data": {"url": "daily_revenue.csv"},
  "mark": "line",
  "encoding": {
    "x": {"field": "order_date", "type": "temporal"},
    "y": {"field": "revenue", "type": "quantitative"},
    "color": {"field": "region", "type": "nominal"}
  }
}
```

The same spec can be produced from Python with Altair, which makes it easy to generate charts from pipeline outputs.

**The main tools and what they are for:**

| Tool | Model | Good at |
|---|---|---|
| Vega-Lite / Altair | Declarative JSON grammar | Quick, consistent statistical charts, interactive selections |
| Apache ECharts | Configuration object, Canvas or SVG renderer | Dashboards, large series (sampling, progressive rendering), many chart types |
| D3 | Low-level library for binding data to DOM | Custom, bespoke graphics (the base of many newsroom visuals) |
| Observable Framework | Static site generator with data loaders | Data apps and reports where data is prepared at build time |
| deck.gl | WebGL layers | Millions of points, arcs and polygons, especially on maps |
| Kepler.gl | Application on top of deck.gl | Interactive geospatial exploration without code |

**Rendering limits.** Every SVG mark is a DOM element, and browsers slow down noticeably as these reach tens of thousands. **Canvas** draws pixels directly and handles hundreds of thousands of marks. **WebGL** (deck.gl, ECharts GL) uses the GPU and handles millions. The exact numbers depend on the device, so test on the slowest laptop your audience uses.

**Reduce before you render.** The faster fix is almost always to send less data:

```python
points = 50_000_000            # rows in the time series
bytes_per_point = 16           # an 8-byte timestamp and an 8-byte value, as raw numbers
print(points * bytes_per_point / 1e9, "GB")   # 0.8 GB to ship to the browser

pixels = 1_200                 # width of the chart in pixels
print(pixels * 4)              # 4800 points after min, max, first and last per pixel column
```

The second figure is the idea behind **M4 aggregation**: for a line chart, keeping the first, last, minimum and maximum value in each pixel column gives a chart that looks the same as the full data. Do it in BigQuery, so only thousands of rows leave the warehouse:

```sql
DECLARE t0 TIMESTAMP DEFAULT TIMESTAMP '2026-11-01';
DECLARE t1 TIMESTAMP DEFAULT TIMESTAMP '2026-11-08';
DECLARE width INT64 DEFAULT 1200;

SELECT
  DIV(TIMESTAMP_DIFF(ts, t0, SECOND) * width, TIMESTAMP_DIFF(t1, t0, SECOND)) AS px,
  MIN(value) AS min_v,
  MAX(value) AS max_v,
  ARRAY_AGG(value ORDER BY ts LIMIT 1)[OFFSET(0)] AS first_v,
  ARRAY_AGG(value ORDER BY ts DESC LIMIT 1)[OFFSET(0)] AS last_v
FROM telemetry.readings
WHERE ts >= t0 AND ts < t1
GROUP BY px
ORDER BY px;
```

For maps, aggregate points into cells (BigQuery's `S2_CELLIDFROMPOINT`, or H3 functions from the CARTO Analytics Toolbox) and draw one shape per cell. For scatter plots, bin into a 2D grid and draw a heatmap.

> **Pitfall:** Random sampling for line charts. A 1% random sample hides exactly the spikes and outages people look for. Use visually aware downsampling (M4, or LTTB, "largest triangle three buckets") that keeps extremes, or aggregate with min and max.

> **Tip:** Precompute the chart data as a small table or file in the pipeline (a mart per chart, or an Observable Framework data loader) rather than having every page view query raw data.

::: deeper
**Streaming visualization.** Live charts receive updates over WebSockets or server-sent events. Append points and drop old ones in a fixed-size buffer rather than redrawing everything; ECharts and similar libraries support incremental updates. Upstream, a streaming job (Dataflow, or a real-time OLAP engine) should pre-aggregate to the display resolution (for example one value per second per series) so the browser never receives raw event rates.

**ECharts at scale.** Line series support `sampling: 'lttb'` and other strategies that downsample on the client to the pixel width; scatter series have a `large` mode that batches drawing; `progressive` rendering draws big datasets in chunks so the page stays responsive. These help, but sending fewer rows still wins.

**Data journalism practices.** Newsrooms publish thousands of charts, so they standardise: house style guides for colour, type and annotation; reproducible pipelines from source data to chart (scripts in Git, not hand edits); data notes that state sources and methods; and accessible charts (text alternatives, colour palettes readable by colour-blind readers, direct labels instead of legends). Tools such as Datawrapper and Flourish exist because most charts should be simple and fast to publish; D3 and Observable are kept for the bespoke pieces.

**Honest charts.** Start bar charts at zero; label axes and units; avoid dual axes that invite false correlations; show uncertainty (intervals, ranges) when the data is a sample or forecast; and say what is missing. A chart over aggregated data should say how it was aggregated.

> **Interview:** "A dashboard must show a year of per-second sensor data for 500 devices. How?" Never send raw points: precompute rollups at several resolutions (per minute, per hour, per day) in the pipeline, query the resolution that matches the zoom level, use min/max (M4) downsampling to the pixel width, and render with Canvas or WebGL.
:::

::: check
?? Why does M4 aggregation keep the first, last, minimum and maximum per pixel column?
Those four values determine exactly what a line chart draws in that column (the vertical extent and the line's entry and exit points), so the downsampled chart looks identical to the full one.

?? Why is random sampling a bad way to reduce a time series for display?
It drops most points at random, including the rare spikes and dips that are often the most important features of the chart.

?? Roughly, when would you move from SVG to Canvas or WebGL rendering?
When the number of marks reaches the tens of thousands (Canvas) or millions (WebGL), because each SVG mark is a DOM element and the browser slows down. But first try to aggregate so fewer marks are needed.
:::

::: resources
- Docs: [Vega-Lite](https://vega.github.io/vega-lite/) · the declarative grammar of interactive graphics, with an example gallery
- Docs: [Apache ECharts](https://echarts.apache.org/en/index.html) · a charting library with Canvas and SVG renderers and large-data options
- Docs: [Observable Framework](https://observablehq.com/framework/) · data apps and reports with data loaders that run at build time
- Docs: [deck.gl](https://deck.gl/) · WebGL layers for millions of points, arcs and polygons, especially on maps
- Docs: [Kepler.gl](https://kepler.gl/) · a no-code geospatial exploration tool built on deck.gl
- Paper: [M4: A Visualization-Oriented Time Series Data Aggregation](https://www.vldb.org/pvldb/vol7/p797-jugel.pdf) · why min, max, first and last per pixel are enough for line charts
- Book: [Fundamentals of Data Visualization](https://clauswilke.com/dataviz/) · Claus Wilke's free book on choosing and designing honest charts
:::

## Activation

Dashboards inform people; activation puts data to work without a person in the loop. The sales team does not open a churn dashboard every morning, but they do live in their CRM, so a churn-risk score that appears on each account record changes what they do. This group covers the mechanics of moving modeled data out of the warehouse into operational tools (reverse ETL), then the design of activation use cases: what to sync, for whom, and how to keep it correct and lawful.

It closes the loop of the whole section. The models, semantic definitions and identity work from the previous groups are exactly what makes activation trustworthy: a lead score is only as good as the `dim_customers` it is computed from.

[[fig:analytics_reverse]]

### Reverse ETL

::: plain
Ordinary ETL carries data from business tools into the warehouse. **Reverse ETL** carries it back: from the warehouse into the tools people use every day, such as the CRM, the email platform or the support desk. Think of the warehouse as a kitchen where ingredients from many suppliers are turned into meals; reverse ETL is the delivery service that brings each meal to the table where it is eaten.
:::

**Why it exists.** The warehouse is where data from every system is joined and modeled: product usage, billing, support tickets, marketing. Business tools each see only their own slice. Reverse ETL lets you compute something in SQL once (lifetime value, plan usage, churn risk, product-qualified lead) and push it into each tool as a normal field.

**How a sync works.** Tools such as Hightouch and Census, and others like Polytomic and RudderStack, share the same building blocks:

1. **Model:** a SQL query or dbt model in the warehouse, with a stable primary key, defining the rows to sync (for example one row per customer account).
2. **Destination object and mapping:** which object in the target (Salesforce `Account`, HubSpot `Contact`) and which column maps to which field, plus the **match key** used to find existing records (an external ID or email).
3. **Sync mode:** `upsert` (update or insert), `update` only, `insert` only, or `mirror` (also delete records that left the model).
4. **Diffing:** the tool remembers what it sent last time and sends only rows that changed.
5. **Delivery:** batched API calls, respecting rate limits, with retries and per-row error reporting.
6. **Schedule or trigger:** every 15 minutes, hourly, or after the dbt run that rebuilds the model.

**Diffing** is the core trick. Destination APIs are slow and rate-limited (often a few thousand records per minute), so syncing two million unchanged rows every hour is impossible. The tool keeps a snapshot of the last successful sync and compares:

```python
import hashlib, json

def fingerprint(row: dict) -> str:
    return hashlib.md5(json.dumps(row, sort_keys=True).encode()).hexdigest()

last_sync = {   # key -> fingerprint of what the destination already has
    "c1": fingerprint({"id": "c1", "tier": "gold", "ltv": 900}),
    "c2": fingerprint({"id": "c2", "tier": "silver", "ltv": 300}),
    "c3": fingerprint({"id": "c3", "tier": "bronze", "ltv": 50}),
}
model_now = [
    {"id": "c1", "tier": "gold", "ltv": 900},     # unchanged
    {"id": "c2", "tier": "gold", "ltv": 650},     # changed
    {"id": "c4", "tier": "bronze", "ltv": 20},    # new
]                                                 # c3 disappeared

current = {r["id"]: fingerprint(r) for r in model_now}
upserts = [k for k, fp in current.items() if last_sync.get(k) != fp]
deletes = [k for k in last_sync if k not in current]
print(upserts, deletes)   # ['c2', 'c4'] ['c3']
```

In BigQuery the same comparison is a query over the current model and a snapshot table, using `FARM_FINGERPRINT(TO_JSON_STRING(t))` as the row fingerprint. Tools that run diffing inside your warehouse keep the snapshot there, so customer data does not have to be stored by the vendor.

**Google Cloud native paths.** For operational stores you own, BigQuery can push data itself:

- `EXPORT DATA` with `format = 'CLOUD_BIGTABLE'` writes query results to a Bigtable table, for low-latency serving of features or profiles.
- `EXPORT DATA` with `format = 'CLOUD_SPANNER'` writes to a Spanner table.
- **Continuous queries** (which need an Enterprise or Enterprise Plus edition reservation) run a SQL statement continuously over new rows and can export results to Pub/Sub or Bigtable (among other targets), so an event can trigger action within seconds.

```sql
EXPORT DATA OPTIONS (
  uri = 'https://bigtable.googleapis.com/projects/my-project/instances/serving/appProfiles/default/tables/customer_profile',
  format = 'CLOUD_BIGTABLE',
  overwrite = true,
  auto_create_column_families = true
) AS
SELECT
  CAST(customer_id AS STRING) AS rowkey,
  STRUCT(lifetime_value, churn_risk, plan) AS profile
FROM analytics.customer_scores;
```

Here `rowkey` becomes the Bigtable row key and the `profile` struct becomes a column family whose fields are columns. For SaaS tools (Salesforce, HubSpot, Zendesk, ad platforms) a reverse ETL product or a small Cloud Run job calling the tool's API does the delivery.

> **Pitfall:** Syncing a model without a stable primary key. If the key changes between runs (a `ROW_NUMBER()` computed on unordered data, say), every row looks new and the tool creates duplicates in the CRM. Use a real business key and test it for uniqueness.

> **Tip:** Trigger syncs from the orchestrator after the model build succeeds, rather than on a fixed clock. A sync that runs halfway through a rebuild can push a half-empty model, and in `mirror` mode that deletes records.

::: deeper
**Rate limits and bulk APIs.** Most SaaS APIs have per-day and per-second limits shared with every other integration in the account. Bulk endpoints (such as Salesforce's Bulk API) accept large batches asynchronously and are much cheaper per record, at the cost of latency. A good sync engine picks the bulk path for large changes and the regular API for small ones, and backs off on HTTP 429 responses.

**Errors per row.** One invalid email or a picklist value that the CRM does not accept should fail one row, not the whole sync. Sync tools record rejected rows with the destination's error message; surface those as a table in the warehouse and alert when the rejection rate jumps.

**Field ownership.** Decide which system owns each field. If reverse ETL writes `lifecycle_stage` and a sales rep also edits it, the next sync overwrites the rep's change. Write only to fields the warehouse owns (often named with a prefix such as `dw_`), and make them read-only in the destination.

**Build versus buy.** A homemade sync is a loop that reads a query, diffs against a snapshot and calls an API. The hard parts are everything around it: authentication refresh, schema discovery of the destination, bulk versus regular APIs, retries, partial failures, observability, and keeping up with API changes across dozens of tools. That is what the products sell.

> **Interview:** "How would you get a product usage score into Salesforce every hour?" Build a dbt or Dataform model with one row per account keyed on the Salesforce account ID; sync it with a reverse ETL tool in upsert mode into warehouse-owned fields, triggered after the model build; diff to send only changes; monitor rejected rows and sync freshness.
:::

::: check
?? Why do reverse ETL tools diff against the previous sync instead of sending every row?
Destination APIs are slow and rate-limited. Sending only new and changed rows (and deletions, in mirror mode) keeps syncs within limits and fast.

?? What can go wrong if a sync runs while its model is being rebuilt?
It may read a partial or empty model. In upsert mode records are missed; in mirror mode the tool may delete destination records that are only temporarily missing.

?? Which BigQuery feature exports query results directly to Bigtable?
`EXPORT DATA` with `format = 'CLOUD_BIGTABLE'` (and, for continuous export of new rows, continuous queries).
:::

::: resources
- Docs: [Export data to Bigtable](https://cloud.google.com/bigquery/docs/export-to-bigtable) · reverse ETL from BigQuery to Bigtable with `EXPORT DATA`
- Docs: [Export data to Spanner](https://cloud.google.com/bigquery/docs/export-to-spanner) · syncing BigQuery results into Spanner tables
- Docs: [Introduction to continuous queries](https://cloud.google.com/bigquery/docs/continuous-queries-introduction) · SQL that runs continuously over new rows and exports results to other systems
- Article: [What is reverse ETL?](https://hightouch.com/blog/reverse-etl) · Hightouch's explanation of models, syncs and use cases
- Article: [What is reverse ETL?](https://www.getcensus.com/blog/what-is-reverse-etl) · Census on the same idea, with a focus on operational analytics
- Docs: [Hightouch documentation](https://hightouch.com/docs) · sync modes, match keys, diffing and warehouse-side state in practice
:::

### Data activation: syncing models to business tools

::: plain
Reverse ETL is the delivery truck; **data activation** is deciding what to deliver, to whom, and why. A churn score on an account, a list of customers likely to buy, an alert when a big client's usage drops: each is a model in the warehouse that only creates value once it reaches the person or system that acts on it. Good activation starts from the action ("the account manager calls at-risk customers") and works backwards to the data.
:::

**Common use cases:**

| Use case | Model | Destination | Action |
|---|---|---|---|
| Lead scoring | Product usage and firmographics per account | CRM (Salesforce, HubSpot) | Sales prioritises product-qualified leads |
| Customer health score | Usage trend, support tickets, billing status | Customer success tool, CRM | Account managers act on at-risk accounts |
| Audiences for ads | Segments such as "bought twice, not in 60 days" | Google Ads, other ad platforms | Re-engagement campaigns, exclusion of existing customers |
| Personalization | Preferences and recommendations per user | Email platform, app via Bigtable or Firestore | Tailored emails and in-app content |
| Alerting | Thresholds on business metrics | Slack, PagerDuty, email | A person investigates a drop in orders |
| Support context | Plan, lifetime value, recent orders | Support desk (Zendesk and others) | Agents see context and route VIPs |

**Composable CDP.** A **customer data platform** (CDP) builds unified customer profiles and audiences. A packaged CDP (Segment, and others) collects data into its own store. A **composable** or warehouse-native CDP uses the warehouse as the profile store: identity resolution and audiences are models in BigQuery, and reverse ETL delivers them. The trade-off is the usual one: more control, one copy of data and your own governance, against more engineering work.

**An audience model:**

```sql
-- One row per customer who bought at least twice but not in the last 60 days
CREATE OR REPLACE VIEW activation.aud_lapsed_repeat_buyers AS
SELECT
  c.customer_id,
  c.email,
  COUNT(*)           AS orders,
  MAX(o.ordered_at)  AS last_order_at
FROM analytics.dim_customers AS c
JOIN analytics.fct_orders    AS o USING (customer_id)
WHERE c.marketing_consent = TRUE
GROUP BY c.customer_id, c.email
HAVING COUNT(*) >= 2
   AND MAX(o.ordered_at) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY);
```

Notice the consent filter inside the model: activation is where data meets real people, so permission belongs in the data, not in a checkbox someone has to remember.

**Identifiers and hashing.** Ad platforms match audiences to their users by identifiers such as email or phone, which are normalized and hashed with SHA-256 before upload (Google Ads Customer Match follows this rule). Normalization matters: the same person must produce the same hash.

```python
import hashlib

def normalize_and_hash(email: str) -> str:
    normalized = email.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

a = normalize_and_hash("  Ana.Silva@Example.com ")
b = normalize_and_hash("ana.silva@example.com")
print(a == b)        # True: same person, same hash
print(len(a))        # 64 hex characters
```

Each platform documents its own normalization rules (for example, extra rules for Gmail addresses or phone number formats); follow them exactly, or match rates drop. In BigQuery, `TO_HEX(SHA256(LOWER(TRIM(email))))` gives the same result.

**Designing a use case.** Write down, before any SQL: who acts, in which tool, how often, which fields they need, how fresh they must be, who owns each field, and how you will know it worked (calls made, conversion of scored leads, churn of contacted accounts). Then build the model, sync it, and measure the outcome. Many activation projects fail not on engineering but because nobody changed their workflow to use the new field.

> **Pitfall:** Activating personal data without checking purpose and consent. A customer who agreed to receive receipts did not agree to be targeted by ads. Model consent per purpose (`marketing_consent`, `ads_consent`) and filter on it in every audience.

> **Tip:** Keep activation models in their own dataset (`activation`), one model per sync, documented with the destination and owner. When someone asks "why does Salesforce say this account is at risk?", the answer is one model away.

::: deeper
**Freshness tiers.** Most activation is fine hourly or daily. Some is not: an abandoned-cart email works within the hour, a fraud flag within seconds. Match the path to the need: scheduled reverse ETL for hourly and daily; BigQuery continuous queries to Pub/Sub, or a streaming pipeline in Dataflow, for minutes and seconds. Do not make everything real time; it multiplies cost and failure modes.

**Feedback loops.** Activation changes behaviour, which changes the data. A lead score that sends sales to high-score accounts will see those accounts convert more, partly because they were called. When evaluating a score, keep a random holdout group that is not acted on, the same idea as an A/B test control.

**Observability.** Treat each sync like a production pipeline: alert on failed or skipped runs, on rejected-row rates, on sudden changes in row counts ("the audience dropped from 40,000 to 12"), and on freshness. Write sync results back to the warehouse so they can be joined with outcomes.

**Data minimisation.** Sync only the fields the destination needs. Every copy of personal data in a SaaS tool is another place to secure, another processor to list in your privacy documentation, and another system to clean when a user asks for deletion. Deletion requests must propagate to destinations, either through mirror syncs or explicit delete calls.

> **Interview:** "Marketing wants a daily audience of likely churners in Google Ads. What do you build?" A churn model or rule in the warehouse; an audience view filtered by ads consent with normalized, hashed identifiers; a reverse ETL sync (mirror mode, so people who stop qualifying leave the audience) triggered after the daily build; a random holdout to measure impact; and alerts on audience size and sync failures.
:::

::: check
?? What is the difference between a packaged CDP and a composable CDP?
A packaged CDP collects and stores customer data in its own platform and builds profiles there. A composable CDP uses your warehouse as the profile store, with identity resolution and audiences as warehouse models and reverse ETL for delivery.

?? Why must emails be normalized before hashing for an ad platform audience?
Hashing is exact: `Ana@Example.com` and `ana@example.com` produce different hashes. Normalizing (trim, lowercase, platform-specific rules) ensures the same person produces the hash the platform expects, so records match.

?? Why keep a random holdout group when activating a lead or churn score?
Acting on the score changes outcomes, so comparing acted-on accounts with the rest is biased. A random holdout that is not acted on shows the true effect of the activation.
:::

::: resources
- Docs: [About Customer Match](https://support.google.com/google-ads/answer/6379332) · Google Ads rules for uploading hashed customer lists, including normalization
- Article: [What is a composable CDP?](https://hightouch.com/blog/composable-cdp) · the warehouse-native approach to customer data platforms
- Docs: [Census documentation](https://docs.getcensus.com/) · models, segments, sync behaviours and observability for activation
- Docs: [Introduction to continuous queries](https://cloud.google.com/bigquery/docs/continuous-queries-introduction) · real-time activation paths from BigQuery, for example to Pub/Sub and Bigtable
- Docs: [Polytomic](https://www.polytomic.com/) · another sync platform covering reverse ETL and bulk syncs between systems
- Book: [Analytics Engineering with SQL and dbt](https://www.oreilly.com/library/view/analytics-engineering-with/9781098142377/) · building the trusted models that activation depends on
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| dbt layers | Staging (rename, cast) → intermediate (shared logic) → marts (`fct_`, `dim_`) |
| Build and test in order | `dbt build` (or a Dataform workflow run with assertions) |
| Protect a shared model | Enforced contract plus model versions for breaking changes |
| Document the grain | "One row per ..." on every mart, with a unique-key test |
| Row-level security | `CREATE ROW ACCESS POLICY p ON t GRANT TO (...) FILTER USING (...)` |
| Column-level security | Policy tags on sensitive columns; dynamic masking for others |
| Define a metric once | Semantic layer: LookML measure, MetricFlow metric or Cube measure |
| Subtotals in one query | `GROUP BY ROLLUP (a, b)`, `CUBE`, `GROUPING SETS` |
| Distinct counts in rollups | Store `HLL_COUNT.INIT` sketches, merge with `HLL_COUNT.MERGE` |
| Repeated dashboard aggregate | Materialized view, aggregate table or BI Engine |
| Sessionize events | `LAG` gap ≥ 30 min → flag → running `SUM` of the flag |
| Week numbers for cohorts | Truncate to the same week start, then `DIV(DATE_DIFF(..., DAY), 7)` |
| GA4 export, one week | `events_*` with `_TABLE_SUFFIX BETWEEN '20261101' AND '20261107'` |
| Experiment assignment | Hash of experiment ID and unit ID into buckets |
| Before reading results | Sample ratio mismatch check (chi-square) |
| Users per group | About 2(z₁₋α/₂ + z₁₋β)²σ²/δ²; half the effect needs four times the users |
| Variance reduction | CUPED: Y − θ(X − mean X), θ = cov(X, Y) / var(X) |
| Embedded tenant isolation | Tenant ID in a server-signed token plus an access filter |
| Chart over millions of points | Aggregate in SQL to pixel width (M4), render with Canvas or WebGL |
| Reverse ETL to Bigtable | `EXPORT DATA OPTIONS (format = 'CLOUD_BIGTABLE', ...) AS SELECT ...` |

### Practice

::: try
**1. Layer the logic.** A raw table `shop.orders` has columns `id`, `cust`, `created`, `status` (mixed case), `amount_cents`, and includes test orders where `cust = 0`. Write the staging model and say in which layer the test-order filter belongs.
:::

?? A solution to 1

```sql
-- models/staging/shop/stg_shop__orders.sql
select
    id                            as order_id,
    cust                          as customer_id,
    cast(created as timestamp)    as ordered_at,
    lower(status)                 as order_status,
    amount_cents / 100.0          as amount,
    cust = 0                      as is_test_order
from {{ source('shop', 'orders') }}
```

Staging keeps every row and only renames and casts; it can add a descriptive flag such as `is_test_order`. The filter `where not is_test_order` belongs in the intermediate or mart model, so anyone who needs test orders can still get them from staging.

::: try
**2. Seven-day retention.** Using a table `analytics.events (user_id, event_timestamp)`, write a BigQuery query that returns, for each signup date (the user's first event date), the cohort size and the share of users active exactly 7 days later.
:::

?? A solution to 2

```sql
WITH firsts AS (
  SELECT user_id, MIN(DATE(event_timestamp)) AS signup_date
  FROM analytics.events GROUP BY user_id
),
active AS (
  SELECT DISTINCT user_id, DATE(event_timestamp) AS active_date
  FROM analytics.events
)
SELECT
  f.signup_date,
  COUNT(DISTINCT f.user_id) AS cohort_size,
  ROUND(COUNT(DISTINCT IF(DATE_DIFF(a.active_date, f.signup_date, DAY) = 7, a.user_id, NULL))
        / COUNT(DISTINCT f.user_id), 3) AS d7_retention
FROM firsts AS f
LEFT JOIN active AS a USING (user_id)
WHERE f.signup_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)   -- only complete cohorts
GROUP BY f.signup_date
ORDER BY f.signup_date;
```

::: try
**3. Is this test healthy?** A 50/50 experiment ends with 101,200 users in control and 98,800 in treatment. Treatment shows +3% revenue per user with p = 0.01. What do you conclude, and what do you check?
:::

?? Answer to 3
Do not ship yet. The split is off by 1.2% of 200,000 users; a chi-square test gives a statistic of 28.8 and a p-value far below 0.001, so there is a sample ratio mismatch and the revenue result cannot be trusted. Check for redirects or crashes that lose treatment users, bot filtering that differs by arm, assignment code that depends on something other than the hash, and exposure logging that fires differently in the two variants. Fix the cause and rerun.

::: try
**4. Embedded dashboards.** Your SaaS app has 3,000 customers in one shared BigQuery table with a `tenant_id` column. Describe how you would serve each customer a dashboard of their own data in your app, and two tests you would automate.
:::

?? Answer to 4
Cluster the table by `tenant_id`. Use a BI tool's signed embedding (Looker signed embed, Superset guest tokens or Metabase static embedding): the backend takes the tenant ID from the logged-in session, puts it in a server-signed, short-lived token, and the semantic layer applies it as an access filter (`access_filter` on `tenant_id`, or an RLS clause). Run embedded queries in their own reservation with caching. Tests: (a) log in as tenant A and assert that no row with another tenant's ID appears in any embedded dashboard; (b) tamper with the token or URL and assert the request is rejected.

::: try
**5. Activation design.** Customer success wants an at-risk flag on each account in the CRM, updated daily. List the pieces you would build, from model to monitoring.
:::

?? Answer to 5
A health score model in the warehouse, one row per account keyed on the CRM account ID, built from usage trend, support tickets and billing status, with tests on the key. A reverse ETL sync in upsert mode into warehouse-owned, read-only CRM fields (`dw_health_score`, `dw_at_risk`), triggered by the orchestrator after the daily model build, sending only changed rows. Monitoring of sync failures, rejected rows, freshness and sudden changes in the number of at-risk accounts. A random holdout of at-risk accounts that customer success does not contact, to measure whether acting on the flag reduces churn.

### What comes next

You can now turn warehouse tables into trusted metrics, dashboards, experiments and synced fields. The next section, **Google Cloud foundations for data**, steps back to the platform underneath all of this: projects and resource hierarchy, IAM, networking, regions, Cloud Storage and the other building blocks that every Google Cloud data system, including BigQuery, Looker and the activation paths in this section, depends on.
