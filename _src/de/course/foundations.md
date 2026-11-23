This is where the course starts, and it assumes no background in data work at all. If you can use a computer, open a terminal when asked and read a few lines of code slowly, you have everything you need. Where a topic shows Python, the companion Python course teaches the language from zero; where it shows SQL, the next section, SQL for data engineering, teaches it properly. Here you only need to follow the idea, not write the code yourself.

The section gives you the vocabulary and the mental map that every later section builds on. Data engineering has a lot of jargon (pipelines, warehouses, lakehouses, streaming, CDC, ELT, orchestration) and a long list of tools. Most beginners feel lost because they meet the tools before the ideas. This section does the opposite: first the ideas, in plain words, then the tools as examples of those ideas.

**What you will be able to do afterwards**

- Explain what a data engineer does, how the job differs from data analysts, data scientists, analytics engineers and ML engineers, and what a data platform is for.
- Describe the journey of data from the moment it is created to the moment someone uses it, and name the stage any tool belongs to.
- Tell transactional (OLTP) systems from analytical (OLAP) systems, and explain why analytical databases store data by column.
- Choose between batch and streaming for a given need, and justify the choice with latency, cost and complexity.
- Sketch a modern data stack on Google Cloud and say what each piece does.
- Pick a sensible file format, move around a Linux server, call an API with `curl`, and work on a team through Git and pull requests.
- Plan your own path into the field: which skills matter at which level, and which projects prove them.

**The big picture.** Think of a city's water system. Water starts in many places (rivers, wells, rain). It has to be collected, carried through pipes, stored in reservoirs, cleaned at a treatment plant, and finally delivered to taps, where people use it without thinking about where it came from. Data engineering is the same job for data. Data is created in many places (apps, websites, sensors, other companies' systems), and data engineers build and run the pipes, reservoirs and treatment plants that turn it into clean, trustworthy, well-labelled tables that analysts, dashboards and machine learning models can use. When the system works, nobody notices it. When it breaks, every report in the company is wrong.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## The field

This group answers the first questions anyone asks: what is this job, what happens to data on its way from creation to use, and what are the main families of systems involved? You will meet five ideas that come back in every later section: the role itself, the data lifecycle, the split between transactional and analytical systems, the split between batch and streaming, and the "modern data stack" that ties the tools together.

Read the topics in order. Each one uses words defined in the ones before it, and together they give you a map. When a later section dives into Spark, BigQuery or Kafka, you will already know where that tool sits on the map and why it exists.

### What data engineering is

::: plain
A **data engineer** builds and looks after the systems that collect data from where it is created, clean it up, and deliver it to the people and programs that need it. Think of a restaurant: the diners (analysts, managers, data scientists) see only the finished dishes, but someone has to order the ingredients, check they are fresh, store them properly and prepare them in a clean, repeatable way. Data engineers run that kitchen. Their customers are other people inside the company, and their product is **reliable data**.
:::

Every company produces data as a side effect of doing business. An online shop records orders in its database, its website logs every page view, the payment provider sends a file of settlements each night, and the marketing team's tools record every email sent. Each of these lives in a different system, in a different shape, owned by a different team. Nobody can answer a simple question such as "how much revenue did the spring email campaign bring in?" until someone brings those pieces together, cleans them and makes them agree.

That someone is the data engineer. The job has three parts:

1. **Move data**: copy it from source systems (databases, APIs, files, event streams) into a central place, on a schedule or continuously. This is called **ingestion**.
2. **Shape data**: remove duplicates, fix types, join related records, apply business rules, and organise the result into tables that are easy to query. This is **transformation**.
3. **Keep it running**: make the whole thing reliable, secure, monitored, documented and affordable, day after day, as the sources change underneath you.

A **pipeline** is the name for a chain of these steps that runs automatically. The simplest pipeline fits in a few lines. Here is one in Python, using only the standard library (the in-memory "files" stand in for a real source and a real database):

```python
import csv, io, sqlite3

raw = io.StringIO(
    "order_id,customer,amount,status\n"
    "1,asha,40.00,paid\n"
    "2,bo,15.50,cancelled\n"
    "3,asha,22.00,paid\n"
    "3,asha,22.00,paid\n"       # the same order sent twice, as real sources do
)

# Extract: read the raw rows
rows = list(csv.DictReader(raw))

# Transform: drop duplicates and unpaid orders, turn text into numbers
seen, clean = set(), []
for r in rows:
    if r["order_id"] in seen or r["status"] != "paid":
        continue
    seen.add(r["order_id"])
    clean.append((int(r["order_id"]), r["customer"], float(r["amount"])))

# Load: write the clean rows into a table people can query
db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE orders (order_id INTEGER, customer TEXT, amount REAL)")
db.executemany("INSERT INTO orders VALUES (?, ?, ?)", clean)

print(len(rows), "raw rows,", len(clean), "clean rows")   # 4 raw rows, 2 clean rows
print(db.execute(
    "SELECT customer, SUM(amount) FROM orders GROUP BY customer"
).fetchall())                                             # [('asha', 62.0)]
```

The three steps have names you will see everywhere: **extract, transform, load**, or **ETL**. When the raw data is loaded first and transformed later inside the warehouse, the order becomes **ELT**, which is the common pattern on Google Cloud today. Real pipelines do the same thing at a much larger scale: millions of rows, dozens of sources, runs every hour, and a lot of care about what happens when a step fails halfway.

**How the role differs from its neighbours.** Several jobs work with data, and the boundaries blur in small companies. A rough guide:

| Role | Main question | Typical output | Typical tools |
|---|---|---|---|
| Data engineer | Is the right data available, correct, on time and affordable? | Pipelines, platforms, clean tables | SQL, Python, Spark, Airflow, BigQuery, Kafka, Terraform |
| Analytics engineer | Are the business tables well modelled, tested and documented? | Tested SQL models, metric definitions | SQL, dbt or Dataform, Git |
| Data analyst | What happened, and why? | Reports, dashboards, answers | SQL, spreadsheets, Looker |
| Data scientist | What will happen, and what should we do? | Experiments, statistical models | Python, notebooks, statistics |
| ML engineer | How do we run models reliably in production? | Training and serving systems | Python, Vertex AI, feature stores |

The data engineer sits upstream of all the others. Analysts, scientists and ML engineers are the data engineer's customers, and the job is judged by whether they can trust and find what they need.

**What a data platform does.** In a larger company, data engineers stop building one pipeline at a time and build a **data platform**: shared infrastructure that lets many teams ingest, store, transform, discover and use data safely by themselves. A platform typically provides storage and compute (on Google Cloud, BigQuery and Cloud Storage), standard ways to ingest data, a scheduler, a catalog where people can search for datasets, access control, data quality checks and cost monitoring. The diagram in the topic The modern data stack, at the end of this group, shows how these pieces fit together.

> **Pitfall:** Beginners often think the job is mostly about big, exciting tools. In practice most of the work is careful plumbing: handling a source that changed a column name, making a job safe to re-run, chasing a number that differs between two dashboards. Learn to enjoy making things boringly reliable.

> **Tip:** Strong SQL is the single most valuable skill in this field. Every tool in this course, from BigQuery to Spark to Flink, can be driven by SQL.

::: deeper
**Where the role came from.** In the 2000s the same work was called **ETL development** or **business intelligence (BI) engineering**, done with graphical tools feeding an on-premises data warehouse. The big-data era (Hadoop, from around 2006) needed people who could program distributed systems, and the title "data engineer" spread in the 2010s; Maxime Beauchemin's 2017 essay "The Rise of the Data Engineer" described the shift from drag-and-drop tools to code. Cloud warehouses such as BigQuery then made storage and compute cheap and elastic, which moved much transformation work into SQL and created the analytics engineer role.

**Two flavours of data engineer.** Some engineers lean towards **platform** work: infrastructure, performance, reliability, security, building tools for others. Others lean towards **pipelines and modelling**: understanding the business, integrating sources and producing trustworthy tables. Job adverts rarely say which they mean, so read the responsibilities, not the title.

**Data as a product.** A growing idea is to treat each important dataset like a product, with an owner, documentation, a promised freshness, quality checks and users who can complain. **Data contracts** (an agreed schema and set of guarantees between a source team and the data team) and **data mesh** (domain teams owning their own data products on a shared platform) both come from this idea. The Data governance, security and compliance section returns to them.

**What "good" looks like.** A useful definition of success is that consumers get data that is **correct, complete, fresh, discoverable, secure and affordable**, and that the engineers can change the system without fear. Each word becomes a concrete practice later: tests and quality checks, completeness monitoring, freshness alerts, a catalog, IAM and policy tags, cost controls and CI/CD.

> **Interview:** "What does a data engineer do?" A strong short answer: "I build and run reliable systems that move data from where it is produced to where it is used, turning raw, messy sources into trusted, well-modelled datasets, with the testing, monitoring, security and cost control that production systems need."
:::

::: check
?? What are the three parts of a data engineer's job?
Moving data from sources into a central place (ingestion), shaping it into clean, usable tables (transformation), and keeping the whole system reliable, secure, monitored and affordable over time.

?? What is the difference between ETL and ELT?
The order of the steps. ETL transforms data before loading it into the destination; ELT loads raw data first and transforms it inside the destination (usually a cloud warehouse such as BigQuery), using its compute.

?? How does an analytics engineer's work differ from a data engineer's?
The analytics engineer focuses on modelling business data in SQL inside the warehouse (tested, documented tables and metrics). The data engineer is responsible for getting data there reliably and for the platform underneath.
:::

::: resources
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Joe Reis and Matt Housley; the best single overview of the field, written for beginners and practitioners alike
- Article: [The Rise of the Data Engineer](https://www.freecodecamp.org/news/the-rise-of-the-data-engineer-91be18f1e603/) · Maxime Beauchemin's essay that helped define the role and its history
- Docs: [Professional Data Engineer certification](https://cloud.google.com/learn/certification/data-engineer) · Google's own description of what a data engineer on Google Cloud is expected to know
- Course: [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) · a free, hands-on course that builds a complete pipeline on Google Cloud
- Article: [Emerging Architectures for Modern Data Infrastructure](https://a16z.com/emerging-architectures-for-modern-data-infrastructure/) · a widely cited map of the tools and how companies combine them
:::

### The data lifecycle

::: plain
Data goes on a journey. It is **born** somewhere (a customer taps "buy"), it is **collected** and carried to a central place, it is **kept** safely, it is **cleaned and combined** with other data, and finally it is **used**: on a dashboard, in a report, by a model. Like food going from farm to table, every step can spoil it, so the steps have to be planned together. Running underneath the whole journey are concerns that never go away, such as keeping data secure and making sure each step runs at the right time.
:::

The **data lifecycle** is the sequence of stages that data passes through. Fundamentals of Data Engineering (Reis and Housley) names five stages and six "undercurrents"; this course uses the same map because it fits every tool you will meet.

[[fig:foundations_lifecycle]]

**1. Generation.** Data is created in **source systems**: an application's database, a mobile app sending events, a payment provider's API, IoT sensors, spreadsheets uploaded by a finance team. Data engineers usually do not own these systems, which is why so many problems start here: a developer renames a column, a sensor sends temperatures in Fahrenheit, a partner's API starts returning pages of 100 rows instead of 1,000. Your first job with any source is to understand it: how much data, how fast it grows, how it changes, and who to call when it breaks.

**2. Ingestion.** Moving data from sources into your platform. The two big choices are **batch versus streaming** (covered two topics from now) and **push versus pull**: does the source send data to you, or do you go and fetch it? Common patterns are full extracts (copy everything each time), incremental extracts (copy rows changed since last time, using an `updated_at` column), **change data capture** or **CDC** (read the database's change log so you see every insert, update and delete), and event streams. On Google Cloud: Datastream for CDC, Pub/Sub for events, Storage Transfer Service and BigQuery Data Transfer Service for files and SaaS sources, plus third-party tools such as Fivetran and Airbyte.

**3. Storage.** Data has to live somewhere at every stage, so storage sits underneath the others rather than in a line. The main choices are **object storage** (cheap, unlimited files: Cloud Storage), a **data warehouse** (tables optimised for analytical SQL: BigQuery) and a **lakehouse**, which puts open table formats such as Apache Iceberg on object storage so many engines can share the same tables. Storage choices are about cost, speed of access, how long to keep data (**retention**) and how often it is read (hot, warm or cold data).

**4. Transformation.** Turning raw data into something useful. Simple transformations fix types, rename columns and remove duplicates. Business transformations join orders to customers, apply the company's definition of "active user" and compute metrics. Modelling organises the result into fact and dimension tables (the Data modeling section). Many teams organise tables in layers: **raw** (exactly as received), **staging** (cleaned, one table per source table) and **marts** (business-ready). Databricks popularised the names bronze, silver and gold for the same idea, the **medallion architecture**.

**5. Serving.** Making data available to the people and programs that use it: dashboards in Looker, ad-hoc SQL for analysts, training data and features for Vertex AI models, APIs for applications, and **reverse ETL**, which pushes results back into operational tools such as a CRM.

A concrete journey, for one ride in a ride-hailing app:

| Stage | What happens to the ride | Example on Google Cloud |
|---|---|---|
| Generation | The app writes a row to its PostgreSQL database and sends a `ride_completed` event | Cloud SQL, the mobile app |
| Ingestion | CDC copies the row; the event is published to a topic | Datastream, Pub/Sub |
| Storage | Raw rows and events land, unchanged, in raw tables | BigQuery dataset `raw`, Cloud Storage |
| Transformation | Duplicates are removed, prices converted to one currency, rides joined to drivers | SQL models in Dataform or dbt |
| Serving | A daily revenue dashboard and a demand-forecasting model read the result | Looker, Vertex AI |

**The undercurrents.** Six concerns apply to every stage:

- **Security**: who may see and change what. Least-privilege access, encryption, secrets kept out of code.
- **Data management**: governance, quality, lineage (where did this number come from?), metadata, privacy and retention rules.
- **DataOps**: applying good engineering practice to data: automation, testing, monitoring, alerting and incident response.
- **Data architecture**: designing the whole system so it meets today's needs and can change tomorrow.
- **Orchestration**: running each step in the right order, at the right time, with retries when something fails (Managed Service for Apache Airflow (formerly Cloud Composer), which is managed Apache Airflow).
- **Software engineering**: version control, code review, CI/CD, writing maintainable code.

> **Pitfall:** Treating the stages separately. A transformation is only as good as the ingestion feeding it, and a dashboard only as good as the transformation. When you design a pipeline, walk the whole lifecycle first and ask, for each stage, "what happens when this step fails or the data arrives late?"

> **Tip:** Always keep a copy of the raw data exactly as received, before any transformation. When you find a bug in your logic (you will), you can fix it and rebuild everything from raw. Without raw data, a bug can mean permanent loss.

::: deeper
**Idempotency.** A step is **idempotent** if running it twice gives the same result as running it once. Because pipelines fail and get retried, every stage should be idempotent. In practice that means overwriting a whole partition for a given date instead of appending to it, or using `MERGE` on a key instead of `INSERT`. This single property prevents more duplicate-data incidents than any other.

**Backfills.** A **backfill** re-runs a pipeline for past dates, for example after fixing a bug or adding a new column. Pipelines designed around a date parameter ("process the data for 2026-11-20") and idempotent writes make backfills routine; pipelines that only know "now" make them painful.

**Lineage and metadata.** As platforms grow, the hard question is no longer "how do I compute this?" but "where did this number come from, and who uses it?" **Lineage** records which tables and jobs produced each table. On Google Cloud, Dataplex Universal Catalog records lineage for BigQuery jobs and other supported services, and catalogs datasets so people can search for them.

**Retention and deletion.** Data protection laws (such as GDPR) give people the right to have their data deleted. That requirement crosses every stage: raw copies, backups, derived tables and ML training sets. Designing for deletion from the start (for example by keeping personal data in a small number of tables keyed by a user ID) is far easier than adding it later.

**Other lifecycle models.** You will meet variants: the analytics lifecycle (question, data, analysis, communication), the ML lifecycle (data, training, evaluation, deployment, monitoring) and Google's older "ingest, store, process and analyse, explore and visualise" diagram. They describe the same flow from different seats.

> **Interview:** "Walk me through how data flows in a platform you built." Structure the answer along the lifecycle: sources, ingestion method and frequency, storage layers, transformation logic and tooling, consumers, and then the undercurrents (how it is orchestrated, tested, monitored and secured). Interviewers listen for whether you thought about failure, late data and re-runs.
:::

::: check
?? Name the five stages of the data lifecycle in order.
Generation, ingestion, storage, transformation and serving. Storage is drawn underneath the other stages because every stage reads from and writes to it.

?? Why keep raw data unchanged after ingestion?
So that when transformation logic has a bug or business rules change, you can rebuild every downstream table from the original data instead of losing information.

?? What does it mean for a pipeline step to be idempotent, and why does it matter?
Running it more than once gives the same result as running it once. Pipelines are retried after failures and re-run for backfills, so non-idempotent steps create duplicates or wrong totals.
:::

::: resources
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · chapter 2 defines the lifecycle and its undercurrents used here
- Docs: [Google Cloud Architecture Center](https://cloud.google.com/architecture) · reference architectures for analytics pipelines on Google Cloud
- Docs: [Managed Airflow overview](https://cloud.google.com/composer/docs/composer-3/composer-overview) · how orchestration is provided as a managed Airflow service
- Article: [Data Mesh Principles and Logical Architecture](https://martinfowler.com/articles/data-mesh-principles.html) · Zhamak Dehghani on ownership and data as a product across the lifecycle
- Article: [Medallion architecture](https://www.databricks.com/glossary/medallion-architecture) · the bronze, silver and gold layering of data, explained briefly
- Course: [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) · walk through every stage of the lifecycle with real code
:::

### OLTP vs OLAP

::: plain
Databases do two very different jobs. A shop's checkout system needs to record one sale, then the next, then the next, thousands of times a second, and never lose any of them. That is **OLTP**: online transaction processing. A manager asking "what were our total sales per month for the last three years?" needs to read millions of sales at once and add them up. That is **OLAP**: online analytical processing. One database is built like a fast cashier, the other like an accountant with a calculator, and using one for the other's job goes badly.
:::

**OLTP systems** run the business. Every time a customer signs up, places an order or updates an address, the application sends a small query that reads or writes a few rows. These systems need:

- **Low latency** for tiny queries: milliseconds.
- **High concurrency**: thousands of users at once.
- **Transactions** with the **ACID** guarantees: **atomicity** (all steps of a transaction happen, or none do), **consistency** (rules such as "balance cannot be negative" always hold), **isolation** (concurrent transactions do not see each other's half-finished work) and **durability** (once committed, data survives a crash).

A typical OLTP transaction, in PostgreSQL:

```sql
BEGIN;
UPDATE accounts SET balance = balance - 40.00 WHERE account_id = 17;
INSERT INTO orders (order_id, account_id, amount) VALUES (9001, 17, 40.00);
COMMIT;   -- both changes become visible together, or neither does
```

Examples: PostgreSQL, MySQL, and on Google Cloud, Cloud SQL, AlloyDB and Spanner.

**OLAP systems** help people understand the business. Queries are few but large: scan a year of orders, group by month and region, compute sums and averages. They need:

- **High throughput** for big scans: read billions of rows quickly.
- **Aggregations, joins and window functions** over large tables.
- Mostly **append** workloads: new data is added in batches or streams; old rows are rarely updated one at a time.

A typical OLAP query, in BigQuery:

```sql
SELECT
  DATE_TRUNC(DATE(order_ts), MONTH) AS month,
  region,
  SUM(amount) AS revenue,
  COUNT(DISTINCT customer_id) AS customers
FROM `shop.orders`
WHERE order_ts >= TIMESTAMP '2024-01-01'
GROUP BY month, region
ORDER BY month, region;
```

Examples: BigQuery, Snowflake, ClickHouse, DuckDB, Databricks SQL.

| | OLTP | OLAP |
|---|---|---|
| Purpose | Run the business | Analyse the business |
| Typical query | Read or write a few rows by key | Scan and aggregate millions or billions of rows |
| Users | Applications, many at once | Analysts, dashboards, pipelines |
| Data shape | Normalised: many small related tables | Denormalised or star schema: wide fact tables |
| Storage layout | Row-oriented | Column-oriented |
| Freshness | Always current | Minutes to a day behind, depending on pipelines |
| Google Cloud examples | Cloud SQL, AlloyDB, Spanner | BigQuery |

**Why the storage layout differs.** The key difference is physical. A **row-oriented** database stores all the values of one row next to each other on disk. That is perfect for "fetch order 9001": one read brings back the whole row. A **column-oriented** (columnar) database stores all the values of one column together. That is perfect for "sum the amount column": it reads only that column and skips the rest.

[[fig:foundations_rowcol]]

Worked numbers make the difference concrete. Suppose an orders table has one billion rows and 100 columns, and each value takes 8 bytes. A query that needs only two columns (the timestamp and the amount):

```python
rows = 1_000_000_000      # one billion orders
columns = 100             # columns in the table
value_bytes = 8           # assume every value takes 8 bytes

whole_table = rows * columns * value_bytes
two_columns = rows * 2 * value_bytes      # the query needs order_ts and amount

print(f"row store reads    {whole_table / 1e9:,.0f} GB")   # row store reads    800 GB
print(f"column store reads {two_columns / 1e9:,.0f} GB")   # column store reads 16 GB
print(f"that is {whole_table // two_columns}x less data")  # that is 50x less data
```

On top of that, a column holds values of one type that often repeat (the same country, the same status), so it **compresses** far better than a row of mixed values, which shrinks the read further.

**Why warehouses exist.** You could, in principle, run analytical queries on the production database. Teams try, and it fails for three reasons. First, a big scan competes with customers for the same CPU and disk, so the checkout slows down. Second, row storage makes the scan slow. Third, the data you need is spread across many systems (the app database, the payment provider, the CRM), and no single OLTP database holds it all. A **data warehouse** solves all three: a separate, column-oriented system that gathers data from many sources, keeps history, and is built for analytical queries. Copying data from OLTP sources into an OLAP warehouse is one of the most common things data engineers do.

> **Pitfall:** `SELECT *` on a columnar warehouse reads every column. In BigQuery, on-demand queries are billed by bytes read, so `SELECT *` on a wide table can cost many times more than selecting the three columns you need. A `LIMIT` does not reduce the bytes scanned.

> **Tip:** Before running a large BigQuery query, check the estimate in the console (or use `bq query --dry_run`), which reports how many bytes it will process without running it.

::: deeper
**How BigQuery stores and runs it.** BigQuery stores tables in a columnar format called Capacitor on Google's distributed file system (Colossus), and runs queries on a separate, shared pool of compute workers. A unit of compute is a **slot**. Separating storage from compute means you can store petabytes cheaply and pay for compute only when you query, either per byte scanned (on-demand pricing) or per slot capacity (BigQuery editions: Standard, Enterprise and Enterprise Plus). The design descends from Google's Dremel system, described in the 2010 paper and its 2020 retrospective.

**Inside a column store.** Columnar engines use several tricks beyond reading less: **encoding** (dictionary encoding replaces repeated strings with small integers; run-length encoding stores "Pune, 5,000 times" as one entry), **vectorised execution** (processing a batch of thousands of values per CPU instruction loop rather than one row at a time) and **zone maps** (per-block minimum and maximum values, so a filter like `order_ts >= '2026-01-01'` can skip whole blocks). **Partitioning** (splitting a table by date) and **clustering** (sorting within partitions by a column) let BigQuery skip even more.

**Why OLAP updates are awkward.** Changing one row in a column store means rewriting compressed blocks across many column files. Warehouses therefore prefer bulk operations: append new data, `MERGE` changes in batches, or rewrite whole partitions. Running thousands of single-row `UPDATE` statements against BigQuery is an anti-pattern; it also hits DML quotas.

**HTAP and blurring lines.** Some systems try to do both (hybrid transactional/analytical processing). AlloyDB for PostgreSQL has a columnar engine that keeps a column-oriented copy of selected data in memory to speed up analytical queries, and Spanner offers Data Boost to run analytical queries on separate compute. These help with light analytics on fresh data, but large-scale analysis across many sources still belongs in a warehouse.

> **Interview:** "Why is a columnar database faster for analytics?" It reads only the columns a query needs, those columns compress well because similar values sit together, and the engine processes values in batches. The price is slower single-row lookups and updates, which is why OLTP databases stay row-oriented.
:::

::: check
?? A query fetches one customer's full profile by ID. Is that an OLTP or OLAP pattern, and which storage layout suits it?
OLTP. A row-oriented layout suits it, because all the customer's columns are stored together and come back in one read.

?? Why does a column store compress better than a row store?
Each column holds values of one type that often repeat or change slowly, which encodings such as dictionary and run-length encoding shrink very well. A row mixes types and values, so it compresses poorly.

?? Give two reasons not to run heavy analytical queries on the production OLTP database.
They compete with live customer traffic for CPU and disk, slowing the application; row storage makes big scans slow; and the data needed usually lives in several systems, not one database.
:::

::: resources
- Docs: [BigQuery storage overview](https://cloud.google.com/bigquery/docs/storage_overview) · how BigQuery stores tables in columnar form and why it matters for cost and speed
- Docs: [What is OLAP?](https://cloud.google.com/learn/what-is-olap) · Google Cloud's short introduction to analytical processing
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · Martin Kleppmann; the chapter on storage and retrieval explains row and column storage beautifully
- Paper: [Dremel: Interactive Analysis of Web-Scale Datasets](https://research.google/pubs/pub36632/) · the 2010 paper behind BigQuery's columnar engine
- Paper: [Dremel: A Decade of Interactive SQL Analysis at Web Scale](https://www.vldb.org/pvldb/vol13/p3461-melnik.pdf) · the 2020 retrospective on separating storage and compute
- Course: [CMU 15-445 Database Systems](https://15445.courses.cs.cmu.edu/) · free university lectures on how databases store and process data
:::

### Batch vs streaming

::: plain
There are two ways to deal with the post. You can let letters pile up in the mailbox and open them all once a day: that is **batch** processing. Or you can open each letter the moment it arrives: that is **streaming**. Batch is simpler and cheaper but your news is up to a day old. Streaming keeps you up to date within seconds, but someone has to stand by the door all day. Data pipelines face exactly the same choice.
:::

**Batch processing** collects data over a period, then processes all of it together on a schedule: every night, every hour, every fifteen minutes. The input is **bounded**: a known, finite set of records, such as "all orders from 20 November". The job starts, reads its input, writes its output and stops.

**Stream processing** handles data continuously as it arrives. The input is **unbounded**: it never ends, because customers keep clicking and sensors keep sending. The job runs all the time, keeps some **state** in memory (running totals, open sessions), and emits results continuously.

[[fig:foundations_batchstream]]

The same calculation can be done both ways. Here, total spend per user:

```python
events = [                      # (event time, user, amount)
    ("09:00", "asha", 40),
    ("09:05", "bo", 15),
    ("09:07", "asha", 22),
]

# Batch: wait until the events are stored, then compute once over all of them
def batch_totals(all_events):
    totals = {}
    for _, user, amount in all_events:
        totals[user] = totals.get(user, 0) + amount
    return totals

print(batch_totals(events))     # {'asha': 62, 'bo': 15}

# Streaming: keep running state and update it as each event arrives
state = {}
for _, user, amount in events:  # imagine these arriving one at a time, forever
    state[user] = state.get(user, 0) + amount
    print("after", user, state)
# after asha {'asha': 40}
# after bo {'asha': 40, 'bo': 15}
# after asha {'asha': 62, 'bo': 15}
```

Both reach the same answer. The streaming version has an answer at every moment; the batch version has one answer, later. Real streaming systems add the hard parts this toy skips: what if the machine holding `state` crashes, what if events arrive out of order, and when is a total "final"?

**The words to know.**

- **Latency**: how long one record takes to go from source to result.
- **Freshness**: how old the newest data in a table is. A nightly batch gives freshness of up to 24 hours or more.
- **Throughput**: how many records per second the system handles.
- **Event time** versus **processing time**: when something happened versus when the system saw it. A phone that was offline may send yesterday's events today.
- **Micro-batch**: running small batches very often (every few seconds or minutes). Spark Structured Streaming works this way by default; it sits between the two.

**The trade-offs.**

| | Batch | Streaming |
|---|---|---|
| Freshness | Minutes to a day | Seconds |
| Cost | Pay while the job runs | Pay for an always-on job, plus a message system |
| Complexity | Lower: re-run a failed day | Higher: state, ordering, late data, exactly-once |
| Fixing bugs | Re-run the affected dates | Replay the stream or backfill from batch |
| Typical tools on Google Cloud | BigQuery scheduled queries, Dataproc (Spark), Dataflow batch | Pub/Sub, Dataflow streaming, Managed Service for Apache Kafka |

**When each fits.** Ask "what decision depends on this data, and how fast must it be made?"

- Finance reports, monthly invoices, most dashboards, ML training data: **batch**. Nobody acts on them within seconds, and batch is easier to get right.
- Fraud detection on a card payment, live delivery tracking, stock levels on a website, alerting on a failing machine: **streaming**. The value of the data decays in seconds.
- Many companies end up with both: streaming for a few urgent use cases, batch for everything else.

On Google Cloud a common streaming path is: applications publish events to **Pub/Sub** (a managed messaging service), a **Dataflow** job (managed Apache Beam) reads them, transforms them and writes to BigQuery. For simple cases a Pub/Sub **BigQuery subscription** writes messages straight into a table with no code at all.

> **Pitfall:** Choosing streaming because it sounds modern. A streaming pipeline costs more to run and much more to operate. If the people using the data look at it once a day, a batch job every hour already exceeds their needs.

> **Tip:** Ask for the freshness requirement as a number ("data no more than 15 minutes old during business hours"), not as an adjective ("real-time"). Different people mean anything from 200 milliseconds to the next morning by "real-time".

::: deeper
**Batch is a special case of streaming.** The Dataflow Model paper (Google, 2015), which became Apache Beam, argues that a bounded dataset is just a stream that happens to end. With the right model (windows, triggers and watermarks), one program can process both, which is how Beam and Flink treat batch today.

**Lambda and Kappa architectures.** The **Lambda architecture** (Nathan Marz) runs a batch layer for correct, complete results and a speed layer for fast, approximate ones, then merges them. It works but means writing the logic twice. The **Kappa architecture** (Jay Kreps, 2014) keeps a replayable log, such as Kafka, as the single source and uses one streaming system for everything, reprocessing by replaying the log. Many modern platforms land somewhere between: streaming ingestion into a lakehouse or warehouse, then batch or incremental transformations on top.

**Windows and watermarks.** You cannot "sum all events" on an unbounded stream, because it never ends. You sum within **windows**: fixed windows (each five minutes), sliding windows, or session windows (activity separated by gaps). A **watermark** is the system's estimate that "all events up to time T have probably arrived", which tells it when a window can produce a result. Events arriving after the watermark are **late data**, and you decide whether to drop them, update the result, or send them to a side output.

**Delivery guarantees.** **At-most-once** may lose records; **at-least-once** never loses but may duplicate; **exactly-once** means each record affects the result once. Exactly-once is usually achieved with checkpoints plus idempotent or transactional writes, not by magic in the network. Dataflow streaming jobs offer an exactly-once mode and a cheaper at-least-once mode.

**Incremental processing.** Between the two worlds, many teams run frequent incremental batches that only process new or changed data: a dbt incremental model every 15 minutes, BigQuery materialized views, or change streams. They give near-real-time freshness with batch-style simplicity, and are often the right first step before true streaming.

> **Interview:** "When would you choose streaming over batch?" When the business value depends on acting within seconds or minutes (fraud, alerting, personalisation, operational tracking), and the team can afford the operational complexity. Otherwise prefer batch or frequent incremental batches, because they are cheaper, simpler and easier to backfill.
:::

::: check
?? What is the difference between bounded and unbounded data?
Bounded data has a known end (yesterday's orders), so a batch job can read all of it and finish. Unbounded data never ends (a click stream), so a streaming job runs continuously.

?? A phone records an event at 09:00 but is offline until 09:40, when it sends the event. What are the event time and the processing time?
The event time is 09:00, when it happened. The processing time is 09:40 or later, when the system received and processed it.

?? A team wants "real-time" sales data for a dashboard the CFO checks each morning. What would you suggest?
Ask for the actual freshness need. A batch or incremental pipeline finishing before the morning (or running hourly) meets it at a fraction of the cost and complexity of streaming.
:::

::: resources
- Article: [Streaming 101: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/) · Tyler Akidau's classic introduction to streaming concepts
- Paper: [The Dataflow Model](https://research.google/pubs/pub43864/) · the Google paper behind Apache Beam and Dataflow, unifying batch and streaming
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · Akidau, Chernyak and Lax; windows, watermarks and exactly-once in depth
- Docs: [What is Pub/Sub?](https://cloud.google.com/pubsub/docs/overview) · Google Cloud's managed messaging service, the usual entry point for streams
- Docs: [Dataflow overview](https://cloud.google.com/dataflow/docs/overview) · running Beam pipelines for batch and streaming on Google Cloud
- Article: [Questioning the Lambda Architecture](https://www.oreilly.com/radar/questioning-the-lambda-architecture/) · Jay Kreps's argument for the Kappa architecture
:::

### The modern data stack

::: plain
The **modern data stack** is a set of specialised tools that snap together like building blocks to form a data platform: one tool copies data in, one stores it, one transforms it, one schedules the work, one draws the charts. Rather than buying a single giant system that does everything, teams pick a good tool for each job and connect them, much as you might build a kitchen from a separate fridge, oven and dishwasher instead of one machine that tries to do it all.
:::

The phrase became popular around 2020, when cloud warehouses made storage and compute cheap and elastic. That made the **ELT** pattern practical: load raw data into the warehouse first, then transform it there with SQL. A whole ecosystem of tools grew around that pattern. On Google Cloud, a typical stack looks like this:

[[fig:foundations_stack]]

**The layers, left to right.**

| Layer | What it does | Google Cloud | Common third-party and open-source |
|---|---|---|---|
| Sources | Where data is created | Cloud SQL, AlloyDB, Spanner, Firestore, app logs | SaaS apps (Salesforce, Stripe, HubSpot) |
| Ingestion | Copy data into the platform | Datastream (CDC), Pub/Sub, BigQuery Data Transfer Service, Storage Transfer Service | Fivetran, Airbyte, dlt, Kafka Connect |
| Storage and compute | Keep data and run queries | BigQuery, Cloud Storage, BigLake and BigQuery tables for Apache Iceberg | Snowflake, Databricks, Apache Iceberg |
| Transformation | Clean and model data with SQL or code | Dataform, BigQuery SQL, Dataproc (Spark), Dataflow | dbt, SQLMesh, Spark |
| Orchestration | Run steps in order, on schedule, with retries | Managed Airflow (Airflow), Workflows | Airflow, Dagster, Prefect |
| Serving and BI | Dashboards, ad-hoc analysis | Looker, Looker Studio, Connected Sheets | Tableau, Power BI, Superset, Metabase |
| ML and AI | Train and serve models | Vertex AI, BigQuery ML | MLflow |
| Reverse ETL | Push results back into operational tools | BigQuery to Pub/Sub or APIs via your own jobs | Hightouch, Census |
| Governance | Catalog, lineage, quality, access | Dataplex Universal Catalog, IAM, policy tags | OpenMetadata, DataHub, Monte Carlo |

**How the pieces connect, step by step.** Imagine an online shop on Google Cloud:

1. The shop's orders live in Cloud SQL for PostgreSQL. **Datastream** reads the database's change log and replicates every insert, update and delete into a BigQuery dataset called `raw_shop`, within seconds to minutes.
2. Payment data comes from Stripe through an ingestion tool, landing in `raw_stripe`. Marketing files arrive in a Cloud Storage bucket and are loaded nightly:

```bash
bq load --source_format=PARQUET raw_marketing.campaigns \
  "gs://acme-landing/campaigns/2026-11-20/*.parquet"
```

3. Transformations written as SQL files in **dbt** or **Dataform** build staging tables and business-ready marts. A model is just a `SELECT` statement; the tool works out the dependencies (`ref`) and creates the table:

```sql
-- models/marts/fct_daily_revenue.sql (a dbt model)
select
  date(order_ts) as order_date,
  count(*)       as orders,
  sum(amount)    as revenue
from {{ ref('stg_orders') }}
where status = 'paid'
group by order_date
```

4. **Managed Airflow** runs the nightly load, then the transformations, then the data quality tests, in order, retrying failures and alerting someone if a step still fails.
5. **Looker** dashboards read the marts. A **semantic layer** (Looker's LookML) defines metrics such as "revenue" once, so every dashboard agrees.
6. A reverse ETL job sends "customers likely to churn" from BigQuery back into the CRM so the support team sees it.

**Why this design won.** Each tool is managed (no servers to patch), scales on demand, and is good at one thing. Analysts who know SQL can build transformations without waiting for engineers. Storage and compute are separate, so you pay mostly for what you use.

**Its weaknesses.** Many tools means many bills, many logins and many places for things to break. Costs can grow quietly, since every scheduled query and dashboard refresh scans data. And because each tool sees only part of the flow, lineage and ownership get blurry without a catalog.

> **Pitfall:** Buying tools before understanding needs. A team of three with ten sources does not need a dozen products. Start with the fewest pieces that work (often: an ingestion tool or Datastream, BigQuery, Dataform or dbt, a scheduler, Looker Studio) and add more only when a real problem demands it.

> **Tip:** Keep transformation logic in version-controlled SQL files (dbt or Dataform), not in dashboard tools or one-off scheduled queries. Logic hidden in many places is the main source of "why do these two numbers differ?"

::: deeper
**Warehouse versus lakehouse.** The original modern data stack centred on a warehouse. **Lakehouses** store tables in open formats (Apache Iceberg, Delta Lake, Apache Hudi) as files on object storage, with a catalog, so several engines (BigQuery, Spark, Trino, Flink, DuckDB) can read and write the same tables. On Google Cloud, BigLake and BigQuery tables for Apache Iceberg let BigQuery manage Iceberg tables stored in Cloud Storage. The Data lakes and lakehouses section covers the trade-offs.

**Bundling and consolidation.** After years of "one tool per job", the market has been consolidating: vendors buy each other and add neighbouring features (Fivetran acquired Census in 2025, and Fivetran and dbt Labs announced a merger later that year), and cloud platforms bundle more into one product. BigQuery, for example, now includes SQL pipelines through Dataform, built-in ML, notebooks, data canvas and Gemini assistance. Expect the tool names in any architecture diagram to change over a few years; the layers in the table above change much more slowly.

**Cost model awareness.** In this stack, cost follows usage. BigQuery on-demand bills per byte scanned (with a monthly free tier of 1 TiB of query processing and 10 GiB of storage); editions bill for slot capacity. Ingestion tools often bill per row or per "monthly active row". Dashboards that refresh every five minutes over unpartitioned tables are a classic surprise bill. The DataOps and platform engineering section covers cost governance (FinOps).

**Open-source equivalents.** Everything in the stack can be self-hosted: Airbyte or dlt for ingestion, Postgres or ClickHouse or DuckDB for storage, dbt Core or SQLMesh for transformation, Airflow or Dagster for orchestration, Superset for BI. Self-hosting trades licence fees for engineering time; on Google Cloud you would typically run such tools on GKE or Cloud Run.

> **Interview:** "Design a simple analytics platform for a startup on Google Cloud." A good answer names the layers first (sources, ingestion, storage, transformation, orchestration, serving, governance), picks the smallest set of managed services for each, explains how data quality and cost are controlled, and states what you would add as the company grows.
:::

::: check
?? Why did cloud warehouses make ELT more popular than ETL?
Warehouses such as BigQuery have cheap storage and elastic compute, so it became practical to load raw data first and transform it inside the warehouse with SQL, instead of transforming it on separate servers before loading.

?? What does reverse ETL do?
It copies results computed in the warehouse (such as customer scores or segments) back into operational tools such as a CRM or marketing platform, so people use them in their daily work.

?? Which Google Cloud service replicates changes from a Cloud SQL database into BigQuery, and what is the technique called?
Datastream, using change data capture (CDC): reading the database's change log to capture every insert, update and delete.
:::

::: resources
- Docs: [What is BigQuery?](https://cloud.google.com/bigquery/docs/introduction) · the centre of most data stacks on Google Cloud
- Docs: [Datastream overview](https://cloud.google.com/datastream/docs/overview) · serverless change data capture into BigQuery and Cloud Storage
- Docs: [Dataform overview](https://cloud.google.com/dataform/docs/overview) · SQL transformation workflows built into BigQuery
- Docs: [What is dbt?](https://docs.getdbt.com/docs/introduction) · the most widely used transformation tool in the modern data stack
- Article: [Emerging Architectures for Modern Data Infrastructure](https://a16z.com/emerging-architectures-for-modern-data-infrastructure/) · a reference map of the stack's layers and vendors
- Docs: [Looker documentation](https://cloud.google.com/looker/docs/intro) · BI and the LookML semantic layer
:::

## Working basics

The first group gave you the map. This group gives you the everyday tools you will use on every job, whatever the stack: the file formats data travels in, the command line and networks that pipelines run on, the Git workflow teams use to change code safely, and a plan for your own learning and career.

None of these topics is glamorous, and job adverts rarely list them, but they are what separates someone who has read about data engineering from someone who can do it. A pipeline is code running on a machine, reading files over a network, changed by a team through reviews. Being comfortable at each of those layers makes every later section easier.

### Data formats for beginners

::: plain
Data has to be written down in some format to be stored or sent, just as a recipe can be written on a card, in a table or as a numbered list. **CSV** is a simple table in plain text, **JSON** is text with labels and nesting, **JSON Lines** is one JSON record per line, and **Parquet** and **Avro** are compact binary formats designed for machines rather than human eyes. Each is good at different things, and picking the right one saves money, time and headaches.
:::

**CSV (comma-separated values)** is a plain-text table: one line per row, values separated by commas, usually with a header line.

```text
order_id,customer,amount,status
1,asha,40.00,paid
2,bo,15.50,cancelled
```

It is universal: every spreadsheet, database and language reads it. But it has no types (everything is text), no standard way to say "missing" (empty string? `NULL`? `NA`?), and values that contain commas, quotes or line breaks must be quoted. There is a document describing the common format (RFC 4180), but many producers ignore it.

**JSON** (JavaScript Object Notation) writes data as labelled fields with nesting: objects in curly braces, lists in square brackets. It has a few types (string, number, boolean, null, object, array), so it can describe an order with a list of items inside it. APIs almost always speak JSON. Its weaknesses: field names repeat in every record (bulky), and a single large JSON array must be read whole before you can use it.

**JSON Lines** (also called newline-delimited JSON, NDJSON) fixes that: one complete JSON object per line. You can read it line by line, split a big file into pieces, and append to it. BigQuery loads it as `NEWLINE_DELIMITED_JSON`.

Here is the same small dataset written as CSV and as JSON Lines, using only Python's standard library:

```python
import csv, io, json

rows = [
    {"id": 1, "name": "Asha", "note": 'said "hi", then left'},
    {"id": 2, "name": "Bo", "note": None},
]

# CSV: every value becomes text; a value containing commas or quotes gets quoted
buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=["id", "name", "note"])
writer.writeheader()
writer.writerows(rows)
print(buf.getvalue())
# id,name,note
# 1,Asha,"said ""hi"", then left"
# 2,Bo,

back = list(csv.DictReader(io.StringIO(buf.getvalue())))
print(back[1])   # {'id': '2', 'name': 'Bo', 'note': ''}

# JSON Lines: one complete JSON object per line
text = "\n".join(json.dumps(r) for r in rows)
print(text)
# {"id": 1, "name": "Asha", "note": "said \"hi\", then left"}
# {"id": 2, "name": "Bo", "note": null}

first, second = (json.loads(line) for line in text.splitlines())
print(second)    # {'id': 2, 'name': 'Bo', 'note': None}
```

Notice what CSV lost on the round trip: the number `2` came back as the text `'2'`, and the missing note (`None`) came back as an empty string, indistinguishable from a real empty note. JSON kept both.

**Parquet** is a binary, **column-oriented** file format (the file-level version of the column store from the OLTP vs OLAP topic). It stores a schema with real types, compresses each column separately, and keeps statistics (such as the minimum and maximum of each column chunk) so readers can skip data. It is the standard format for analytics files, data lakes and lakehouses. You cannot open it in a text editor; you read it with a library or engine:

```python
import pyarrow as pa
import pyarrow.parquet as pq          # pip install pyarrow

table = pa.Table.from_pylist(rows)    # the rows from the example above
pq.write_table(table, "people.parquet", compression="zstd")
print(pq.read_schema("people.parquet"))                    # shows id as int64, name and note as string
names = pq.read_table("people.parquet", columns=["name"])  # reads only the name column
```

**Avro** is a binary, **row-oriented** format with a schema written as JSON and stored in the file header. It is fast to write record by record and handles **schema evolution** well (adding a field with a default, for example), which is why it is popular for messages in Kafka and Pub/Sub and for data exchange between systems.

| Format | Layout | Types | Human-readable | Compression | Best for |
|---|---|---|---|---|---|
| CSV | Rows, text | None | Yes | Whole file only (e.g. gzip) | Small exports, spreadsheets, simple interchange |
| JSON | Nested text | Basic | Yes | Whole file only | APIs, configuration, nested documents |
| JSON Lines | One JSON per line | Basic | Yes | Whole file only | Logs, event dumps, loading into BigQuery |
| Avro | Rows, binary | Rich, with schema | No | Per block | Messages, row-by-row writing, schema evolution |
| Parquet | Columns, binary | Rich, with schema | No | Per column | Analytics, data lakes, lakehouse tables |

> **Pitfall:** Opening a CSV of IDs or postcodes in a spreadsheet and saving it again. Spreadsheets "helpfully" drop leading zeros (`00742` becomes `742`), turn long numbers into scientific notation and reformat dates. Never round-trip pipeline data through a spreadsheet.

> **Tip:** A simple rule for beginners: receive whatever the source gives you, keep that raw copy, and convert to Parquet (or load into BigQuery) as early as possible. Analytical work should happen on typed, columnar data.

::: deeper
**Inside a Parquet file.** A file is split into **row groups** (horizontal slices of rows). Inside each row group, every column is stored as a **column chunk**, itself split into **pages**. A **footer** at the end holds the schema and per-chunk statistics. A reader first reads the footer, then fetches only the column chunks it needs (**projection pushdown**) and skips row groups whose min/max statistics cannot match the filter (**predicate pushdown**). Columns are encoded (dictionary, run-length, delta) and then compressed (Snappy, Gzip or Zstandard are common).

**Avro and schemas.** An Avro file's header holds the writer's schema; data blocks follow, separated by sync markers so the file can be split. Records are stored without field names, which keeps them small. A reader can supply its own **reader schema**, and Avro resolves differences (new fields get defaults, removed fields are skipped). In streaming systems, a **schema registry** stores schemas centrally and messages carry a small schema ID instead. Pub/Sub supports Avro and Protocol Buffers schemas on topics.

**ORC and Protocol Buffers.** **ORC** is another columnar format, from the Hive world, with a design similar to Parquet. **Protocol Buffers** (protobuf) is Google's compact binary format for messages and APIs; you meet it in gRPC services, Pub/Sub schemas and the BigQuery Storage Write API.

**Loading into BigQuery.** BigQuery loads CSV, JSON Lines, Avro, Parquet and ORC from Cloud Storage. Google's documentation recommends Avro as the preferred format for loading, because compressed Avro blocks can still be read in parallel. A gzip-compressed CSV or JSON file cannot be split, so it loads more slowly. Parquet and Avro carry their schemas, so BigQuery can create the table without you describing every column.

**JSON number traps.** The JSON standard does not limit number precision, but many parsers (including JavaScript's) store numbers as 64-bit floating point, which represents integers exactly only up to 2^53. Large IDs can silently change. Send big IDs as strings, and use exact decimal types for money.

> **Interview:** "Why is Parquet better than CSV for analytics?" It is columnar (read only needed columns), typed (no parsing or guessing), compressed per column (much smaller), and carries statistics that let engines skip data. CSV must be read and parsed in full every time.
:::

::: check
?? What two pieces of information did the CSV round trip lose in the example, and why?
The type of `id` (the number 2 came back as the text `'2'`) and the difference between a missing value and an empty string (`None` came back as `''`). CSV stores only text and has no standard null.

?? Why is JSON Lines easier to process at scale than one big JSON array?
Each line is a complete record, so a file can be read line by line, split into chunks for parallel processing, and appended to, without parsing the whole file first.

?? You must send events through a message queue and later add new fields without breaking consumers. Which format fits, and why?
Avro (or Protocol Buffers): compact binary records with a schema that supports evolution, such as adding a field with a default value, which a schema registry can manage.
:::

::: resources
- Docs: [Apache Parquet documentation](https://parquet.apache.org/docs/) · the file format's structure: row groups, column chunks, encodings
- Docs: [Apache Avro documentation](https://avro.apache.org/docs/) · schemas, binary encoding and schema resolution
- Spec: [RFC 4180: Common Format for CSV Files](https://www.rfc-editor.org/rfc/rfc4180) · the closest thing CSV has to a standard, including quoting rules
- Spec: [JSON Lines](https://jsonlines.org/) · the one-record-per-line convention in a single short page
- Docs: [Introduction to loading data into BigQuery](https://cloud.google.com/bigquery/docs/loading-data) · supported formats and how to choose between them
- Docs: [Python csv module](https://docs.python.org/3/library/csv.html) · reading and writing CSV correctly, including dialects and quoting
:::

### Linux, shell and networking basics for data engineers

::: plain
Almost every pipeline runs on a **Linux** computer somewhere: a virtual machine, a container, a managed service built on Linux. The **shell** is the text window where you type commands to that computer instead of clicking. And **networking** is how that computer talks to databases, APIs and storage. You do not need to be a system administrator, but you need to find your way around a server, read a log file, and work out why a pipeline "cannot connect", in the same way a driver does not need to build an engine but must know how to check the oil.
:::

**Files and folders.** Linux organises everything in one tree starting at `/`. Your home folder is `~`. The commands you will use daily:

```bash
pwd                         # print the folder you are in
ls -la                      # list files, including hidden ones, with details
cd /var/log                 # change folder
mkdir -p data/raw           # create folders (and parents)
cp a.csv backup/            # copy; mv renames or moves; rm deletes (no undo)
less big.log                # page through a file (q to quit, / to search)
head -n 2 orders.csv        # first lines; tail -n 20 shows the last lines
tail -f app.log             # follow a log as new lines are written
du -sh data/                # how much disk a folder uses
df -h                       # free space on each disk
```

**Pipes: small tools joined together.** Each command reads text and writes text, and the pipe `|` sends one command's output into the next command's input. Given this `orders.csv`:

```text
order_id,customer,amount,status
1,asha,40.00,paid
2,bo,15.50,cancelled
3,asha,22.00,paid
4,chen,9.99,paid
```

```bash
grep -c ',paid$' orders.csv                       # 3   (lines ending in ,paid)
tail -n +2 orders.csv | cut -d, -f2 | sort -u     # asha, bo, chen (one per line)
awk -F, 'NR > 1 && $4 == "paid" {sum[$2] += $3}
         END {for (c in sum) print c, sum[c]}' orders.csv | sort
# asha 62
# chen 9.99
```

`tail -n +2` skips the header, `cut` picks field 2, `sort -u` sorts and removes duplicates, and `awk` is a small language for column-based text. `>` writes output to a file, `>>` appends, and `2>` redirects error messages. Learn `jq` too: it is `grep` and `cut` for JSON (`jq '.items[].price' order.json`).

**Permissions.** Every file has an owner, a group and three sets of permissions (read `r`, write `w`, execute `x`) for owner, group and others. `ls -l` shows them as `-rw-r--r--`. The numeric form adds 4 for read, 2 for write and 1 for execute:

```bash
chmod 600 key.json     # rw------- : only you can read or write it (secrets)
chmod 755 run.sh       # rwxr-xr-x : everyone may run it, only you may change it
chmod +x run.sh        # add execute permission
```

**Scripts that fail safely.** A shell script keeps going after an error unless told otherwise, which in a pipeline means loading half a file and reporting success. Start every bash script with:

```bash
#!/usr/bin/env bash
set -euo pipefail   # stop on errors, on unset variables, and on failures inside pipes
```

Every command returns an **exit code**: `0` means success, anything else is failure. Schedulers such as Airflow use it to decide whether a task succeeded. `echo $?` shows the last one.

**Remote machines.** **SSH** (secure shell) opens a shell on another computer over an encrypted connection, usually on port 22. On Google Cloud, `gcloud` handles keys for you, and Identity-Aware Proxy (IAP) lets you reach VMs that have no public IP address:

```bash
gcloud compute ssh etl-vm --zone=europe-west1-b --tunnel-through-iap
```

**Calling APIs with curl.** `curl` sends HTTP requests from the shell. It is the fastest way to test an API a pipeline will use:

```bash
curl -sS https://api.example.com/v1/orders?page=1 \
  -H "Authorization: Bearer $API_TOKEN" | jq '.data | length'
curl -sS -o /dev/null -w "%{http_code}\n" https://example.com   # print only the HTTP status
```

**Networking in five ideas.**

- An **IP address** identifies a machine on a network (`10.0.0.5` is private, inside your network).
- A **port** identifies a service on that machine. Common ones: 22 SSH, 80 HTTP, 443 HTTPS, 5432 PostgreSQL, 3306 MySQL, 9092 Kafka.
- **DNS** translates names (`bigquery.googleapis.com`) into IP addresses. `dig +short <name>` or `nslookup <name>` shows the answer.
- **Firewalls** decide which traffic is allowed. In a Google Cloud VPC, incoming traffic is denied unless a firewall rule allows it.
- **TLS** encrypts traffic (the `s` in `https`). Certificates prove the server is who it claims to be.

When a pipeline "cannot connect", check in that order: does the name resolve (DNS), is the port reachable (firewall), does the service accept you (credentials), is the connection encrypted as expected (TLS).

> **Pitfall:** Putting passwords or API tokens directly in commands or scripts. They end up in your shell history, in logs and in Git. Read them from environment variables or, on Google Cloud, from Secret Manager.

> **Tip:** When a command's options confuse you, paste it into explainshell or read `man <command>`. Most of the shell is a few dozen commands used in combination.

::: deeper
**Processes and resources.** `ps aux` lists running processes, `top` or `htop` shows CPU and memory live, and `kill <pid>` stops one. A process that runs out of memory may be killed by the kernel's **OOM killer**; `dmesg` or the system journal shows it. On a busy pipeline machine, `free -h` (memory), `df -h` (disk) and `uptime` (load average) answer most "why is it slow?" questions in seconds.

**Which ports are open?** `ss -ltnp` (Linux) lists listening TCP ports and the program behind each; `lsof -i :8080` works on macOS. `nc -zv db.internal 5432` tests whether a port is reachable without speaking its protocol.

**Environment variables and PATH.** `export NAME=value` sets a variable for programs started from that shell. `PATH` is the list of folders searched for commands, which is why "command not found" often means "installed somewhere not on PATH". Containers and schedulers pass configuration through environment variables, so `env` is a good first debugging step.

**Private connectivity on Google Cloud.** Production databases should not have public IP addresses. Pipelines reach them through private networking: a VPC, private IPs, **Private Google Access** (so VMs without public IPs can reach Google APIs), Private Service Connect, and the Cloud SQL Auth Proxy, which creates an authenticated, encrypted tunnel to a Cloud SQL instance:

```bash
cloud-sql-proxy --port 5432 my-project:europe-west1:orders-db
psql "host=127.0.0.1 port=5432 dbname=shop user=etl"
```

**DNS caching.** DNS answers are cached for a time-to-live (TTL). After a database fails over to a new address, clients may keep the old answer until the TTL expires, which explains "it works from my laptop but not from the job".

> **Interview:** "A job that loads from an API started failing with timeouts. How do you debug it?" Reproduce with `curl` from the same machine or network, check DNS resolution, check whether the port is reachable and whether firewall or egress rules changed, check credentials and rate limits (HTTP 401, 403, 429), and read the job's logs for the exact error, before touching code.
:::

::: check
?? What does `cat app.log | grep ERROR | wc -l` do?
It counts the lines in `app.log` that contain `ERROR`: `cat` outputs the file, `grep` keeps matching lines, and `wc -l` counts lines.

?? Why start a bash script with `set -euo pipefail`?
So it stops at the first failing command (including failures inside pipes) and on unset variables, instead of carrying on and possibly reporting success after a partial load.

?? A job cannot reach a PostgreSQL database at `db.internal`. Name three things to check, in order.
DNS (does `db.internal` resolve?), network reachability of port 5432 (firewall rules, `nc -zv`), then credentials and TLS settings.
:::

::: resources
- Course: [The Missing Semester of Your CS Education](https://missing.csail.mit.edu/) · MIT's free lectures on the shell, scripting, editors and version control
- Book: [The Linux Command Line](https://linuxcommand.org/tlcl.php) · William Shotts; a complete, gentle introduction, free to read online
- Book: [Data Science at the Command Line](https://jeroenjanssens.com/dsatcl/) · Jeroen Janssens; processing data with shell tools, free online
- Docs: [explainshell](https://explainshell.com/) · paste any shell command to see what each part and option does, taken from the man pages
- Docs: [curl man page](https://curl.se/docs/manpage.html) · every option of the tool you will use to test APIs
- Docs: [Connect to VMs using IAP](https://cloud.google.com/compute/docs/connect/ssh-using-iap) · reaching private VMs on Google Cloud without public IPs
- Blog: [Julia Evans's blog](https://jvns.ca/) · clear, friendly explanations of DNS, networking and Linux internals
:::

### Git and code review for data work

::: plain
**Git** keeps the full history of your code, like an unlimited "undo" that also records who changed what and why. It lets many people work on the same project at once: each works on a separate copy called a **branch**, then proposes their change through a **pull request**, where teammates review it before it joins the main version. For data work this matters even more than usual, because a careless change to one SQL file can quietly make every report in the company wrong.
:::

**The basic loop.** A **repository** (repo) is a project folder whose history Git tracks. A **commit** is a saved snapshot with a message. **main** is the shared, trusted branch. A **branch** is a movable pointer to a line of commits, cheap to create and throw away.

```bash
git switch main && git pull                 # start from the latest main
git switch -c fix/revenue-currency          # create and move to a new branch
# ... edit models/marts/fct_daily_revenue.sql ...
git status                                  # what changed?
git diff                                    # exactly which lines changed
git add -p                                  # stage changes piece by piece, reviewing each
git commit -m "Convert revenue to EUR before summing"
git push -u origin fix/revenue-currency     # publish the branch
```

Then open a **pull request** (PR, called a merge request in GitLab) on GitHub or GitLab, asking to merge the branch into main.

[[fig:foundations_git]]

**What happens in a pull request.**

1. **Description**: what changed and, above all, *why*; how you tested it; what consumers will notice.
2. **Automated checks (CI)**: continuous integration runs on every push. For data projects that typically means a SQL linter (SQLFluff), unit tests for Python, a compile or dry run of every SQL model, and data tests on a small or sampled dataset in a separate CI dataset.
3. **Review**: at least one colleague reads the change, asks questions and approves. A `CODEOWNERS` file can require approval from the team that owns a folder.
4. **Merge** into main, usually with a squash merge (all the branch's commits become one), followed by deployment.

**Reviewing SQL and pipeline code.** Data code has failure modes that ordinary code review misses. Ask:

- **Grain**: what does one row of the output represent? Could a join multiply rows (a fan-out) and inflate sums?
- **Nulls**: does `WHERE status != 'cancelled'` silently drop rows where `status` is `NULL`? (It does: comparisons with `NULL` are never true.)
- **Re-runs**: is the step idempotent? Does it overwrite the partition, or append duplicates on retry?
- **Time**: which time zone? Event time or load time? What about late data?
- **Cost**: does the query filter on the partition column? Does it `SELECT *` a wide table?
- **Downstream impact**: who reads this table? Does a renamed or removed column break a dashboard?
- **Secrets and personal data**: no keys in code, no personal data copied into new places without reason.

A small example of a review comment worth making:

```sql
-- Before: a LEFT JOIN to payments, which can have several rows per order
select o.order_id, o.amount, p.method
from orders o
left join payments p on p.order_id = o.order_id
-- Review: an order paid in two instalments now appears twice,
-- so SUM(o.amount) downstream double-counts it. Aggregate payments first,
-- or join to one row per order.
```

**Monorepo or many repos?** A **monorepo** holds many projects (ingestion code, dbt models, infrastructure, DAGs) in one repository; a **polyrepo** setup gives each its own. Monorepos make cross-cutting changes atomic and keep tooling consistent; they need path-based CI so a change to one folder does not test everything. Polyrepos give clear ownership and independent releases, but changes that span them need coordination. Many data teams use one repo for the data platform (dbt or Dataform project, DAGs, Terraform) and separate repos for applications.

> **Pitfall:** Committing secrets: a service account key file, a `.env` file, a database password in a config. Once pushed, assume it is leaked (history keeps it even after deletion) and rotate it. Add such files to `.gitignore`, and let CI authenticate to Google Cloud with Workload Identity Federation instead of key files.

> **Tip:** Keep pull requests small and focused: one logical change, ideally under a few hundred lines. Small PRs get reviewed faster and more carefully, and are easy to revert.

::: deeper
**Merge, rebase and squash.** A **merge commit** joins two histories and keeps every commit. **Rebase** replays your branch's commits on top of the latest main, giving a straight line of history; never rebase commits that others have already pulled. A **squash merge** turns the whole branch into one commit on main, which keeps main readable. Teams usually pick one style and enforce it with branch protection rules.

**Trunk-based development.** Most effective teams keep branches short-lived (hours to a couple of days) and merge to main often, rather than maintaining long-lived `develop` or release branches. Unfinished features hide behind flags or simply are not scheduled yet. For data, the equivalent is to land model changes in small steps with backwards-compatible schemas.

**CI for data projects.** Useful checks, from cheap to expensive: formatting and linting (SQLFluff, Ruff, pre-commit hooks), compiling all models, BigQuery dry runs (catch syntax errors and estimate bytes scanned), unit tests on fixed inputs, building only changed models and their downstream dependents in an isolated CI dataset (dbt's `dbt build --select state:modified+ --defer --state <prod-artifacts>`, often called "slim CI"), and comparing results against production (data diffs). On Google Cloud, Cloud Build or GitHub Actions run these.

**Infrastructure as code.** Datasets, buckets, service accounts and permissions belong in Git too, described in Terraform and applied through the same review process. Then "who gave this group access to that dataset?" has an answer in the history.

**Large files and notebooks.** Git is for code, not data: never commit datasets or credentials. Jupyter notebooks produce noisy diffs because outputs are stored in the file; strip outputs before committing or move finished logic into `.py` and `.sql` files.

> **Interview:** "What do you look for when reviewing a SQL change?" Correct grain and join cardinality (no fan-out), null handling, idempotency and incremental logic, time zones and late data, partition filters and cost, tests added or updated, and backwards compatibility for downstream consumers.
:::

::: check
?? What is the difference between a commit and a pull request?
A commit is a saved snapshot of changes in your repository. A pull request is a proposal, on GitHub or GitLab, to merge a branch's commits into another branch (usually main), with discussion, automated checks and review.

?? Why might `WHERE status != 'cancelled'` drop more rows than intended?
Rows where `status` is `NULL` are also dropped, because any comparison with `NULL` is unknown, not true. Use `WHERE status IS DISTINCT FROM 'cancelled'` or handle nulls explicitly.

?? You accidentally pushed a service account key to a repository and then deleted the file in the next commit. Is it safe now?
No. The key is still in the history and may already have been copied. Revoke and rotate it immediately, then remove it from history if required, and switch CI to keyless authentication.
:::

::: resources
- Book: [Pro Git](https://git-scm.com/book/en/v2) · Scott Chacon and Ben Straub; the free, complete book on Git, from basics to internals
- Docs: [GitHub flow](https://docs.github.com/en/get-started/using-github/github-flow) · the branch and pull request workflow in one page
- Article: [Google's code review guidelines](https://google.github.io/eng-practices/review/) · how Google reviewers and authors approach reviews
- Docs: [dbt continuous integration](https://docs.getdbt.com/docs/deploy/continuous-integration) · testing only changed models in CI
- Docs: [SQLFluff](https://docs.sqlfluff.com/) · a SQL linter and formatter that supports BigQuery and dbt
- Article: [Trunk Based Development](https://trunkbaseddevelopment.com/) · the case for short-lived branches and frequent merges
- Paper: [Why Google Stores Billions of Lines of Code in a Single Repository](https://research.google/pubs/pub45424/) · the trade-offs of a monorepo at extreme scale
:::

### Data engineering roadmap and career

::: plain
Learning data engineering is like learning to cook for a restaurant: first you learn to use a knife and follow a recipe (SQL, Python, the command line), then you cook full meals (build pipelines end to end), and only later do you design the kitchen (platforms and architecture). Trying to design the kitchen before you can chop an onion leads nowhere. This topic gives you the order to learn things in, what each career level expects, and how to prove your skills with projects.
:::

**The core skills, in learning order.**

1. **SQL**: joins, aggregations, window functions, CTEs, and reading a query plan. Every later skill uses it.
2. **Python**: enough to write clean scripts, call APIs, handle files and errors, and test your code. The companion Python course covers this.
3. **Command line and Git**: the previous two topics.
4. **Data modeling**: how to shape tables (facts, dimensions, keys, slowly changing data) so they answer questions correctly.
5. **A warehouse in depth**: BigQuery, including partitioning, clustering, cost and security.
6. **Pipelines and orchestration**: ingestion patterns, idempotency, backfills, Airflow or Managed Airflow, dbt or Dataform.
7. **Distributed processing**: Spark, and when you need it.
8. **Streaming**: Pub/Sub or Kafka, Dataflow or Flink, and the concepts of windows and late data.
9. **Cloud and operations**: IAM, networking, Terraform, CI/CD, monitoring, cost control.
10. **Governance and quality**: testing data, contracts, catalogs, privacy.

**What each level looks like.** Titles vary between companies; the expectations below are typical.

| Level | You are trusted to... | Typical evidence |
|---|---|---|
| Junior (0 to 2 years) | Build and fix well-defined pipelines and SQL models with guidance | Clean, tested code; asks good questions; learns the stack |
| Mid-level (2 to 5 years) | Own pipelines end to end: design, build, monitor, fix incidents | Delivers projects alone; handles on-call; reviews others' code |
| Senior (5+ years) | Design systems across teams, choose tools, set standards | Architecture decisions, mentoring, reliability and cost improvements |
| Staff and above | Shape the data strategy and platform for the organisation | Multi-team initiatives, technical direction, influence without authority |

**Portfolio projects that prove skill.** Hiring managers trust working systems more than certificates. A good project is small, complete, documented and honest about trade-offs. Ideas, from simple to ambitious:

- **API to warehouse**: pull a public API daily (weather, public transport, exchange rates), land raw JSON in Cloud Storage, load to BigQuery, model with dbt or Dataform, schedule it, add tests, build a Looker Studio dashboard.
- **CDC pipeline**: a Cloud SQL (PostgreSQL) database with a small app writing to it, Datastream into BigQuery, and a model that rebuilds the current state and a history table.
- **Streaming**: publish simulated events to Pub/Sub, process them with Dataflow (or Spark Structured Streaming on Dataproc), compute windowed metrics, handle late data.
- **Lakehouse**: write Iceberg tables on Cloud Storage with Spark, query them from BigQuery and DuckDB, and compare costs.

For each, write a README with an architecture diagram, how to run it, what it costs, what breaks, and what you would do differently. Keep infrastructure in Terraform so anyone can recreate it, and tear it down after demos to avoid bills.

**Certifications.** Google Cloud offers the **Associate Data Practitioner** (aimed at people with about six months of hands-on Google Cloud data experience) and the **Professional Data Engineer** (aimed at about three years of industry experience, including Google Cloud). The Professional exam is two hours of multiple-choice questions and the certification is valid for two years. A certificate helps you get past CV filters, especially early on, but projects and interviews decide offers.

**How this course maps to the job.** The course follows the same order as the skills list. Foundations (this section), SQL, Programming, Data modeling and Data warehousing give the junior core. Lakehouses, databases, storage internals, Hadoop, Spark, other engines, query engine internals and real-time OLAP give the processing depth expected at mid-level. Streaming, event-driven architecture, ingestion, pipelines, orchestration and analytics engineering cover building production pipelines. The Google Cloud sections (foundations, BigQuery in depth, pipelines, operational databases and AI) make it concrete on one platform. Distributed systems, system design, DataOps, governance, AI and ML, graph data and industry patterns are the senior material and the core of most system design interviews.

> **Pitfall:** "Tutorial hell": watching course after course without building anything. After each section of this course, build something small with what you learned, break it on purpose, and fix it.

> **Tip:** Learn concepts before tools. Tools change every few years; idempotency, partitioning, data modeling, the difference between event time and processing time, and how a shuffle works will stay useful for your whole career.

::: deeper
**Interview loops.** A typical data engineering interview process includes: a SQL round (often live, with window functions, deduplication and gaps-and-islands problems), a coding round in Python (data manipulation more often than algorithms puzzles, though some companies use both), a **data modeling** round (design tables for a business, discuss grain and slowly changing dimensions), a **system design** round (design a pipeline or platform: ingestion, storage, processing, serving, failure handling, scale, cost), and a behavioural round about past projects and incidents. Senior loops weigh design and behaviour more heavily.

**Specialisations.** After a few years many engineers specialise: **streaming** systems, **platform and infrastructure** (Kubernetes, Terraform, developer tooling for data), **analytics engineering** and semantic layers, **ML and AI data** (feature pipelines, training data, embeddings and retrieval for LLM applications), or **governance and privacy**. Each has its own section later in the course.

**Growing past senior.** Beyond senior, the job shifts from writing most of the code to multiplying others: writing design documents, setting standards, reviewing architecture, mentoring, and translating between business goals and technical plans. Will Larson's writing on staff engineers describes the archetypes (tech lead, architect, solver, right hand).

**Keeping current.** The field moves quickly. Follow release notes for the tools you use (BigQuery, Dataflow and Managed Airflow publish them), a few practitioners' newsletters and engineering blogs, and the main conferences' talks (Data + AI Summit, Current, Google Cloud Next). Be sceptical of hype: ask what problem a new tool solves and what it costs to operate.

> **Interview:** "Tell me about a pipeline you built." Use a clear structure: the business problem, the sources and volumes, your design and why, what went wrong and how you fixed it, how you tested and monitored it, and a measurable result (hours saved, freshness improved, cost reduced). Interviewers look for ownership and judgement more than tool names.
:::

::: check
?? Which two skills should a beginner learn first, and why?
SQL and Python. SQL is used by almost every data tool, and Python is the main language for writing pipelines, calling APIs and testing; everything else builds on them.

?? What makes a portfolio project convincing to a hiring manager?
It runs end to end on realistic tools, is documented (README, architecture diagram, how to run it), includes tests and scheduling, and honestly discusses costs, failures and trade-offs.

?? What is the main difference between what a mid-level and a senior data engineer are trusted with?
A mid-level engineer owns pipelines end to end. A senior engineer designs systems across teams, sets standards and chooses tools, and mentors others.
:::

::: resources
- Docs: [Professional Data Engineer certification](https://cloud.google.com/learn/certification/data-engineer) · exam guide, format and recommended experience
- Docs: [Associate Data Practitioner certification](https://cloud.google.com/learn/certification/data-practitioner) · the entry-level Google Cloud data certification
- Course: [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) · free, project-based course with a community and a final project
- Repo: [Data Engineer Handbook](https://github.com/DataExpert-io/data-engineer-handbook) · a curated list of courses, books, projects and communities
- Article: [Data Engineer Roadmap](https://roadmap.sh/data-engineer) · an interactive map of skills to learn, in order
- Course: [Google Cloud Skills Boost](https://www.cloudskillsboost.google/) · hands-on labs in real Google Cloud projects
- Article: [StaffEng](https://staffeng.com/) · Will Larson's guides and interviews on growing beyond senior engineer
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| The job in one line | Move, shape and serve data reliably, securely and affordably |
| ETL vs ELT | ETL transforms before loading; ELT loads raw first and transforms in the warehouse |
| Lifecycle | Generation, ingestion, storage, transformation, serving, plus the undercurrents |
| Keep raw data | Store sources unchanged so you can rebuild after bugs |
| Idempotent step | Running twice equals running once: overwrite partitions or `MERGE` on a key |
| OLTP | Many small transactions, row storage, ACID: Cloud SQL, AlloyDB, Spanner |
| OLAP | Big scans and aggregations, column storage: BigQuery |
| Check cost before running | `bq query --dry_run --use_legacy_sql=false 'SELECT ...'` |
| Batch vs streaming | Ask for freshness as a number; default to batch unless seconds matter |
| Streaming on Google Cloud | Pub/Sub, then Dataflow, then BigQuery |
| CDC into BigQuery | Datastream |
| Load a file into BigQuery | `bq load --source_format=PARQUET dataset.table "gs://bucket/path/*.parquet"` |
| Format for analytics | Parquet (columnar, typed, compressed); Avro for messages and schema evolution |
| Safe bash script | `#!/usr/bin/env bash` then `set -euo pipefail` |
| Count matching lines | `grep -c 'pattern' file` |
| Test an HTTP endpoint | `curl -sS -o /dev/null -w "%{http_code}\n" URL` |
| Private VM shell | `gcloud compute ssh VM --tunnel-through-iap` |
| Start a change | `git switch -c fix/name`, commit, push, open a pull request |
| Review SQL for | Grain, join fan-out, nulls, idempotency, time zones, partition filters |
| Learning order | SQL, Python, shell and Git, modeling, warehouse, pipelines, Spark, streaming, cloud ops |

### Practice

::: try
**1. Place the tools.** For each tool, name the lifecycle stage or stack layer it mainly belongs to: Datastream, Looker, Managed Airflow, Dataform, Cloud Storage, Pub/Sub, Dataplex Universal Catalog.
:::

?? Answer to 1
Datastream: ingestion (CDC). Looker: serving (BI). Managed Airflow: orchestration, an undercurrent. Dataform: transformation. Cloud Storage: storage. Pub/Sub: ingestion of events (streaming). Dataplex Universal Catalog: data management and governance, an undercurrent.

::: try
**2. OLTP or OLAP?** Classify each workload and say which Google Cloud database you would use: (a) saving a customer's shopping cart on every click; (b) computing revenue per product category for the last five years; (c) checking a user's password hash at login; (d) a weekly report of churn by country.
:::

?? Answer to 2
(a) OLTP: Cloud SQL, AlloyDB or Firestore. (b) OLAP: BigQuery. (c) OLTP: the application database such as Cloud SQL. (d) OLAP: BigQuery, fed by a pipeline from the operational systems.

::: try
**3. Batch or streaming?** For each need, choose batch, frequent incremental batch, or streaming, and justify it: (a) blocking a suspicious card payment; (b) a monthly finance close; (c) a sales dashboard refreshed during business hours that managers check a few times a day; (d) alerting when a factory machine overheats.
:::

?? Answer to 3
(a) Streaming: the decision must happen within seconds, before the payment completes. (b) Batch: the data is needed once a month and correctness matters more than speed. (c) Frequent incremental batch, such as every 15 to 60 minutes: fresh enough, much cheaper and simpler than streaming. (d) Streaming: the alert is only useful within seconds or minutes.

::: try
**4. Make the pipeline idempotent.** The first topic's Python pipeline creates a fresh in-memory database each time. Suppose instead it appends to a permanent `orders` table, and it is accidentally run twice on the same input. What goes wrong, and how would you fix it in SQL?
:::

?? A solution to 4
Running it twice inserts every order twice, so all totals double. Make the load idempotent: either delete and rewrite the data for the batch being loaded (for example one date partition) inside one transaction, or upsert on the key so repeats change nothing. In SQLite:

```sql
CREATE TABLE IF NOT EXISTS orders (
  order_id INTEGER PRIMARY KEY,
  customer TEXT,
  amount   REAL
);
INSERT INTO orders (order_id, customer, amount) VALUES (1, 'asha', 40.0)
ON CONFLICT (order_id) DO UPDATE SET
  customer = excluded.customer,
  amount   = excluded.amount;
```

In BigQuery the same idea is a `MERGE` statement on `order_id`, or overwriting the day's partition.

::: try
**5. Shell detective.** Using `orders.csv` from the Linux topic, write one command that prints the number of cancelled orders, and one that prints the total amount of all paid orders.
:::

?? A solution to 5

```bash
grep -c ',cancelled$' orders.csv                                       # 1
awk -F, '$4 == "paid" {t += $3} END {print t}' orders.csv              # 71.99
```

### What comes next

You now have the map: what data engineers do, how data flows through its lifecycle, why analytical and transactional systems differ, when to stream, how the modern stack fits together on Google Cloud, and the everyday tools of the trade. The next section, **SQL for data engineering**, turns to the single most important skill on that map. It takes you from your first `SELECT` to the joins, window functions, deduplication patterns and query plans that real pipelines are built from, using BigQuery as the main engine.
