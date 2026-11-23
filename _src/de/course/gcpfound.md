This section opens the Google Cloud part of the course. It is for data engineers who know the general ideas (pipelines, warehouses, lakehouses, streaming, orchestration) from the earlier sections and now need to run them on one specific cloud. It assumes the vocabulary of Data engineering foundations, Data warehousing, Data lakes and lakehouses, Streaming systems and Orchestration, and it does not assume you have used Google Cloud before.

**What you will be able to do afterwards**

- Name the Google Cloud service for each stage of a data platform and explain why you would pick it over its neighbours.
- Lay out an organization, folders and projects for a data team, attribute costs with labels and the billing export, and know which quotas bite first.
- Explain compute-storage separation and what it changes about cost, scaling and latency, using BigQuery, Snowflake and Databricks as examples.
- Grant least-privilege access with IAM, service accounts and organization policies, and keep data private with Private Service Connect and VPC Service Controls.
- Choose between default encryption, customer-managed keys and confidential computing, and keep pipeline secrets out of code.
- Monitor pipelines with Cloud Monitoring and Cloud Logging, use Spot VMs safely, avoid surprise network charges, and manage the whole platform with Terraform.

**The big picture.** Every data service on Google Cloud sits on the same four foundations. **Structure**: resources live in projects, projects live in folders under an organization, and policies flow down that tree. **Identity**: every request is made by a principal (a person, a group or a service account) and is allowed or denied by IAM. **Network and keys**: data moves over Google's network inside regions and between them, encrypted with keys that Google or you manage, inside perimeters you draw. **Operations and money**: metrics, logs, quotas and the bill tell you what is happening and what it costs. The services in the next sections (BigQuery, Pub/Sub, Dataflow, Managed Service for Apache Spark and the rest) are easy to start with and hard to run well without these foundations, which is why they come first.

[[fig:gcpfound_map]]

> **Note:** Google Cloud renamed several data products in 2025 and 2026. This course uses the current names and gives the former name once: for example Managed Service for Apache Spark (formerly Dataproc), Knowledge Catalog (formerly Dataplex Universal Catalog) and Gemini Enterprise Agent Platform (formerly Vertex AI). API names, `gcloud` commands and IAM roles usually kept the old names, so you will still type `gcloud dataproc` and grant `roles/dataproc.worker`.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Platform basics

Before choosing tools you need a map and a frame. The map is the set of managed services Google Cloud offers for each stage of a data platform, and which one fits which job. The frame is the resource hierarchy (organization, folders, projects) that decides who pays, who can see what, and which limits apply.

Two ideas then explain most of the platform's behaviour. **Compute-storage separation** is the reason BigQuery, Spark on Cloud Storage and every modern lakehouse can scale compute on demand and bill it separately. **IAM** is how every one of those services decides whether a request is allowed. Get these four topics right and the rest of the Google Cloud part reads as variations on a theme.

### The Google Cloud data ecosystem: a map

::: plain
Think of Google Cloud as a large hardware shop where every aisle sells one kind of tool: one aisle for moving data in, one for transforming it, one for storing it, one for showing it to people, and a service desk that coordinates and guards everything. You do not need every tool. You need to know which aisle solves your problem, and which two or three tools in that aisle are worth comparing.
:::

A data platform has the same stages on any cloud: **ingest** data from sources, **process** it, **store** it, **serve** it to people and programs, and around all of that **orchestrate**, **govern** and **operate**. On Google Cloud each stage has a small set of managed services. The table is the map this part of the course follows.

| Stage | Main Google Cloud services | Typical job |
|---|---|---|
| Ingest: events | Pub/Sub, Google Managed Service for Apache Kafka | Clicks, logs, IoT messages, service events |
| Ingest: databases | Datastream | Change data capture from MySQL, PostgreSQL, Oracle, SQL Server into BigQuery |
| Ingest: files and SaaS | Storage Transfer Service, BigQuery Data Transfer Service | Bulk copies, scheduled imports from Google and SaaS sources |
| Process: streams and batch code | Dataflow (managed Apache Beam) | Streaming ETL, windowed aggregation, enrichment |
| Process: Spark and Hadoop | Managed Service for Apache Spark (formerly Dataproc), clusters or serverless | Existing Spark jobs, PySpark, notebooks, lakehouse maintenance |
| Process: SQL | BigQuery, Dataform | ELT inside the warehouse, SQL pipelines with tests |
| Process: low code | Cloud Data Fusion | Visual pipelines for integration teams |
| Store: files and lake | Cloud Storage, Google Cloud Lakehouse (formerly BigLake) with Apache Iceberg | Landing zone, raw and curated open-format tables |
| Store: warehouse | BigQuery | Analytics, BI, ML on structured and semi-structured data |
| Store: operational | Bigtable, Spanner, AlloyDB, Cloud SQL, Firestore | Low-latency reads and writes for applications |
| Serve and analyse | Looker, Data Studio (formerly Looker Studio), Connected Sheets | Dashboards, semantic layer, self-service |
| ML and AI | Gemini Enterprise Agent Platform (formerly Vertex AI), BigQuery ML | Training, serving, embeddings, agents |
| Orchestrate | Managed Service for Apache Airflow (formerly Cloud Composer), Workflows, Cloud Scheduler, Eventarc | DAG scheduling, service chaining, event routing |
| Serverless glue | Cloud Run, Cloud Run functions (formerly Cloud Functions), Batch | Small transforms, API ingestion, containerised batch jobs |
| Govern | Knowledge Catalog (formerly Dataplex Universal Catalog), Sensitive Data Protection, IAM | Catalog, lineage, quality, PII discovery, access |
| Operate | Cloud Monitoring, Cloud Logging, Cloud Billing | Metrics, alerts, logs, audit, cost |

A realistic first platform uses only a handful of these. A common shape is:

```text
Applications --> Pub/Sub --> Dataflow --> BigQuery (raw, staging, marts) --> Looker
Postgres ------> Datastream ------------> BigQuery
Partner files -> Cloud Storage --load---> BigQuery
                 Dataform builds the marts; Managed Airflow or Workflows schedules the batch parts
```

**How to choose inside an aisle.** Most choices come down to three questions:

1. **Is there existing code?** Spark jobs move to Managed Service for Apache Spark with few changes; Beam pipelines run on Dataflow; Airflow DAGs run on Managed Airflow. Rewriting working code to use a "more native" service is rarely worth it.
2. **Streaming or batch, and how fresh?** Seconds of latency point to Pub/Sub plus Dataflow or a Pub/Sub BigQuery subscription; minutes to hours point to files in Cloud Storage plus load jobs or SQL in BigQuery.
3. **Who maintains it?** SQL-fluent analytics engineers do best with BigQuery plus Dataform or dbt. Software engineers are comfortable with Beam or Spark. Integration teams who prefer screens to code may choose Data Fusion.

> **Tip:** Start from BigQuery and add services only when SQL cannot do the job. Many teams discover that load jobs, scheduled queries, Dataform and a Pub/Sub BigQuery subscription cover most of their needs, with no clusters and no custom code to run.

> **Pitfall:** Mixing regions by accident. Most data services are regional, and BigQuery can only join tables in the same location. Pick a home region (or a multi-region such as `EU` or `US`) for the platform on day one and put buckets, datasets, Dataflow jobs and Spark clusters there.

::: deeper
**Where the services came from.** Many Google Cloud data services are the public versions of systems Google described in research papers. BigQuery is built on **Dremel** (2010); Dataflow on **FlumeJava** (2010) and **MillWheel** (2013), which together became the **Dataflow model** paper (2015) and then Apache Beam; Bigtable on the **Bigtable** paper (2006), which inspired HBase and Cassandra; Spanner on the **Spanner** paper (2012); Pub/Sub on Google's internal messaging systems. Managed Service for Apache Spark goes the other way: it hosts the open-source Hadoop and Spark ecosystem that grew out of Google's MapReduce and GFS papers. Knowing the lineage helps in interviews and explains design choices, such as why Dataflow talks about watermarks and why BigQuery bills by bytes scanned.

**Serverless versus provisioned.** The services fall on a spectrum. BigQuery, Pub/Sub, Dataflow, Datastream, Workflows and Cloud Run have no machines for you to size; you pay per use or per reserved capacity. Managed Spark clusters, Managed Airflow environments, Bigtable, Cloud SQL and Managed Kafka clusters are provisioned: you choose sizes and pay while they exist, even when idle. Serverless services trade control for less operations work; provisioned ones give predictable performance and are often cheaper at steady high load.

**Open formats as the escape hatch.** Storing curated data as Apache Iceberg tables in Cloud Storage, registered in the Lakehouse runtime catalog (formerly BigLake metastore), lets BigQuery, Spark, Trino and Flink read the same tables. That keeps engine choices reversible, which matters more than any single service decision. The topic [Google Cloud Lakehouse and Apache Iceberg on BigQuery](#bigquery) covers it.

> **Interview:** "Design a data platform on Google Cloud for an e-commerce company." Walk the stages: events through Pub/Sub, database changes through Datastream, files into Cloud Storage; processing in Dataflow for streams and BigQuery SQL (Dataform or dbt) for batch; BigQuery as the warehouse with Iceberg tables for data shared with Spark; Looker for BI; Managed Airflow for orchestration; Knowledge Catalog for governance; one region, separate projects per environment, IAM through groups, and cost controls from day one.
:::

::: check
?? A team has 200 working PySpark jobs on an on-premises Hadoop cluster. Which processing service should they start with on Google Cloud, and why?
Managed Service for Apache Spark (formerly Dataproc). It runs the same Spark code with few changes, so they can migrate first and modernise later, instead of rewriting everything into Beam or SQL.

?? Which two services would you combine to get application events into BigQuery within seconds with no custom code?
Pub/Sub for the events, and a Pub/Sub BigQuery subscription, which writes messages straight into a table.

?? Why should a platform choose a home region early?
Most data services are regional and BigQuery cannot join across locations; cross-region traffic also costs money. Choosing late means copying data and rebuilding pipelines.
:::

::: resources
- Docs: [Google Cloud documentation: data analytics](https://docs.cloud.google.com/docs/data) · the entry point to every data service's documentation
- Course: [Data Engineer learning path (Google Cloud Skills Boost)](https://www.cloudskillsboost.google/paths/16) · Google's own course sequence with hands-on labs for each service
- Book: [Data Engineering with Google Cloud Platform, 2nd edition](https://www.packtpub.com/en-us/product/data-engineering-with-google-cloud-platform-9781835080115) · Adi Wijaya's practical tour of BigQuery, Dataflow, Composer and Dataproc with projects
- Docs: [Professional Data Engineer certification](https://cloud.google.com/learn/certification/data-engineer) · the exam guide doubles as a checklist of what a Google Cloud data engineer is expected to know
- Paper: [The Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) · the design behind Dataflow and Apache Beam
- Docs: [Google Cloud Architecture Center: big data and analytics](https://docs.cloud.google.com/architecture/big-data-analytics) · reference architectures that combine these services
:::

### Resource hierarchy, projects and billing

::: plain
Google Cloud organises everything like a company filing cabinet. The cabinet is your **organization**, the drawers are **folders** (one per department or environment), and the files inside are **projects**. Every database, bucket or job lives in exactly one project. Rules you put on a drawer apply to every file in it, and each project is linked to a **billing account** that pays for what it uses.
:::

The hierarchy has four levels:

- **Organization**: the root, tied to your company's Google Workspace or Cloud Identity domain (for example `acme.com`). Organization administrators see and control everything below.
- **Folders**: optional groupings, nested up to 10 levels. Data teams usually group by environment (`prod`, `nonprod`, `sandbox`) and then by domain or team.
- **Projects**: the unit of ownership, quota, API enablement and (usually) billing. A project has a **project ID** you choose (`acme-dwh-prod`, globally unique and permanent), a display **name** you can change, and a **project number** Google assigns.
- **Resources**: buckets, datasets, topics, clusters, service accounts and so on, each in one project.

[[fig:gcpfound_hierarchy]]

**Inheritance.** IAM allow policies and organization policies set on a node apply to everything below it. A role granted on the `prod` folder applies to every project in it, and an organization policy that restricts resource locations to the EU applies to every new resource in the organization. This is powerful and dangerous: an over-broad grant high in the tree is invisible when you look at a single project.

**A layout for a data platform.** Separate projects by environment and by blast radius, not by service:

```text
acme.com (organization)
  folder: data-prod
    acme-data-landing-prod     Cloud Storage landing buckets, Pub/Sub, Datastream
    acme-data-dwh-prod         BigQuery datasets (raw, staging, marts)
    acme-data-pipelines-prod   Dataflow, Managed Spark, Managed Airflow, their service accounts
    acme-bi-prod               Looker connection and BI query jobs (own quota and reservation)
  folder: data-nonprod
    acme-data-dwh-dev, acme-data-pipelines-dev ...
  folder: sandboxes
    acme-sbx-<user>            short-lived, strict budgets and quotas
  acme-billing-admin           billing export dataset, budgets, BigQuery reservations admin
```

Splitting storage and compute projects lets you grant analysts `bigquery.jobUser` in a compute project while data stays in another, and lets BI load have its own quota.

**Labels and tags.** Two different features with similar names:

| | Labels | Tags (Resource Manager tags) |
|---|---|---|
| Shape | `key: value` on a resource | Key and value defined centrally, then bound to resources |
| Inherited down the tree | No | Yes |
| Used for | Cost attribution in the billing export, filtering | IAM conditions, organization policy conditions, firewall policies |
| Who can set them | Anyone who can edit the resource | Requires tag roles, so they can be trusted in policies |

Use labels such as `team`, `env`, `pipeline` and `cost-center` on every resource and on BigQuery jobs, and use tags where a policy must depend on them (for example "deny deletes on resources tagged `env=prod`").

```bash
gcloud projects create acme-data-dwh-prod --folder=123456789012 --labels=team=data,env=prod
gcloud billing projects link acme-data-dwh-prod --billing-account=0X0X0X-0X0X0X-0X0X0X
gcloud services enable bigquery.googleapis.com pubsub.googleapis.com dataflow.googleapis.com \
  --project=acme-data-dwh-prod
gcloud storage buckets update gs://acme-landing-prod --update-labels=team=data,pipeline=orders
```

**Quotas.** Each project has **quotas** per service and often per region: allocation quotas (how many CPUs, IP addresses or Dataflow jobs you can have) and rate quotas (API requests per minute). They protect you and Google from runaway usage. View and request increases in the console's Quotas page or with the Cloud Quotas API; increases are reviewed and can take time, so request them before a migration, not during it. Data teams most often hit regional CPU quotas (Dataflow and Spark workers), in-use IP addresses, BigQuery concurrent queries and load jobs, and Pub/Sub throughput for very large topics.

**Billing.** A **Cloud Billing account** pays for linked projects. Turn on the **billing export to BigQuery** on day one: the standard export writes daily cost rows per service, SKU, project and label; the detailed export adds resource-level rows; a pricing export lists your prices. With it, cost becomes a SQL problem:

```sql
-- cost per team label for last month, net of credits
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key = 'team') AS team,
  service.description                                   AS service,
  ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)), 2) AS net_cost
FROM `acme-billing-admin.billing.gcp_billing_export_v1_0X0X0X_0X0X0X_0X0X0X`
WHERE invoice.month = '202610'
GROUP BY team, service
ORDER BY net_cost DESC;
```

**Budgets** send alerts at thresholds (and can publish to Pub/Sub for automation), but they do not stop spending. The [BigQuery cost control](#bigquery) topic shows how to combine budgets with hard limits for queries.

> **Pitfall:** A project ID is permanent and cannot be reused after the project is deleted. Choose IDs with a naming convention (`<company>-<domain>-<purpose>-<env>`) instead of `my-project-12345`.

> **Pitfall:** Labels are not inherited. A label on a project does not appear on its buckets or jobs in the export; label resources and BigQuery jobs explicitly, or attribute by project.

::: deeper
**Deleting projects.** A deleted project enters a pending-deletion state for about 30 days, during which it can be restored; afterwards its resources are gone. A **lien** (`gcloud alpha resource-manager liens create` or set automatically by some services, such as Shared VPC host projects) blocks deletion of important projects.

**Shared VPC.** A **host project** owns a VPC network that **service projects** use. Data platforms often run Dataflow and Spark workers in service projects on a centrally managed network; the workers' service accounts then need the Network User role on the host's subnets.

**Organization policies** are constraints, not permissions. Examples that matter to data teams: `gcp.resourceLocations` (allowed regions), `iam.allowedPolicyMemberDomains` (domain-restricted sharing: no grants to outside Gmail accounts), `storage.publicAccessPrevention`, `iam.disableServiceAccountKeyCreation`, and `gcp.restrictNonCmekServices` (require customer-managed keys). Set them on the organization or a folder; projects inherit them. Custom constraints let you write your own rules in CEL.

**Billing export details.** The export is not real time: rows usually arrive within hours, and costs for a day can be updated later, so do not build minute-level alerting on it. Credits (free tier, committed use discounts, promotions) are in a repeated `credits` field and are negative, which is why the query adds them. Costs in the export are in the billing account's currency.

**Projects as quota and blast-radius boundaries.** Because quotas are per project, a runaway notebook in a shared project can starve production pipelines of CPUs or BigQuery concurrency. Separate projects are cheap; use them generously, and automate their creation (the Terraform topic at the end of this section shows how).

> **Interview:** "How would you organise Google Cloud projects for a data platform with three teams?" Folders per environment, projects per environment and purpose (landing, warehouse, pipelines, BI), shared services (billing export, reservations, networking) in admin projects, IAM granted to groups at folder or project level, labels for cost attribution, organization policies for locations and domains, and project creation through Terraform so the layout stays consistent.
:::

::: check
?? A role is granted to a group on the `data-prod` folder. Which resources does it apply to?
Every project in that folder (and its subfolders) and every resource in those projects, because allow policies are inherited down the hierarchy.

?? What is the difference between a label and a tag?
Labels are simple key-value metadata, not inherited, used mainly for cost attribution. Tags are centrally defined, inherited, need special roles to bind, and can be used in IAM and organization policy conditions.

?? Does a budget alert stop resources from running when the budget is exceeded?
No. Budgets only notify (by email or Pub/Sub). Stopping spend needs quotas, hard limits or automation that reacts to the notification.
:::

::: resources
- Docs: [Resource hierarchy](https://docs.cloud.google.com/resource-manager/docs/cloud-platform-resource-hierarchy) · organization, folders, projects and how policies are inherited
- Docs: [Export Cloud Billing data to BigQuery](https://docs.cloud.google.com/billing/docs/how-to/export-data-bigquery) · standard, detailed and pricing exports and their schemas
- Docs: [Creating and managing labels](https://docs.cloud.google.com/resource-manager/docs/creating-managing-labels) · label rules and how labels reach the bill
- Docs: [Tags overview](https://docs.cloud.google.com/resource-manager/docs/tags/tags-overview) · tags versus labels and using tags in policies
- Docs: [Enterprise foundations blueprint](https://docs.cloud.google.com/architecture/blueprints/security-foundations) · Google's reference layout of folders, projects, networking and policies
- Docs: [Cloud Quotas overview](https://docs.cloud.google.com/docs/quotas/overview) · allocation and rate quotas and how to request increases
:::

### Compute-Storage Separation

::: plain
A traditional database is like a restaurant where each chef keeps the ingredients in their own fridge: to add a chef you must also buy a fridge and move food into it. With **compute-storage separation** all ingredients live in one huge shared pantry and chefs work at any free stove. When the dinner rush comes you hire more chefs for the evening and send them home afterwards, and the pantry never has to move.
:::

**The old model: shared nothing with local disks.** Hadoop HDFS, Teradata and early MPP warehouses split data across nodes and stored each slice on that node's disks. Compute went to the data, which avoided network transfers when networks were slow. The cost was rigidity: to get more CPU you added nodes, which meant rebalancing terabytes of data; to store more data you added nodes, which meant paying for CPUs you did not need; and the cluster had to run 24 hours a day because it *was* the storage.

**The new model: shared storage, elastic compute.** Data lives in a durable, replicated storage service (Colossus for BigQuery, Cloud Storage for lakehouses). Compute nodes are stateless or keep only caches; they read what they need over a very fast network, process it and go away. Three things made this practical: datacenter networks fast enough that remote reads approach local disk speed, columnar formats that read only the needed columns, and object stores that are cheap, durable and (on Cloud Storage) strongly consistent.

[[fig:gcpfound_separation]]

What you gain:

- **Independent scaling.** Store a petabyte and query it with ten machines, or store a terabyte and throw a thousand at it for a minute.
- **Elasticity and isolation.** Several compute clusters can read the same data at once: a nightly ETL cluster, a BI warehouse and a data science notebook, without competing for the same CPUs.
- **Separate bills.** Storage is billed per GiB-month; compute per second, per slot or per byte scanned.
- **Many engines, one copy.** Open table formats in Cloud Storage let BigQuery, Spark and Trino share tables.

What you pay:

- **Network and latency.** Every read crosses the network. Engines compensate with local SSD caches, columnar pruning and prefetching.
- **Cold starts.** A cluster or warehouse that was suspended starts with an empty cache, so the first queries are slower.
- **Metadata becomes the bottleneck.** Listing millions of small files in an object store is slow, so table formats and catalogs (Iceberg manifests, BigQuery's metadata service) keep file lists and statistics outside the object store.

**Three implementations.**

| | BigQuery | Snowflake (on Google Cloud) | Databricks (on Google Cloud) |
|---|---|---|---|
| Storage | Capacitor files in Colossus, managed by BigQuery; or Iceberg in Cloud Storage | Micro-partitions in the cloud object store (Cloud Storage), managed by Snowflake | Delta Lake or Iceberg tables in your Cloud Storage buckets |
| Compute unit | Slots, from a shared pool or reservations | Virtual warehouses (clusters) sized XS to 6XL | Clusters and SQL warehouses of VMs |
| Start-up | None visible: slots are always warm | Warehouses resume in seconds, cache starts cold | Classic clusters take minutes; serverless SQL warehouses seconds |
| Billing | Bytes scanned or slot-hours | Credits per warehouse-second (60-second minimum) | DBUs plus the VMs (classic) or bundled (serverless) |
| Caching | Shuffle tier and internal caches; BI Engine | Local SSD cache per warehouse, result cache | Disk cache on local SSDs, result cache |

The [BigQuery architecture](#bigquery) topic explains in detail how Dremel slots, the shuffle tier and Colossus fit together.

**A worked number.** A team needs 64 vCPUs for 3 hours each night and almost nothing during the day. A coupled cluster sized for the peak runs 24 hours; elastic compute runs 3:

```python
VCPUS, PEAK_HOURS, DAYS = 64, 3, 30
PRICE_PER_VCPU_HOUR = 0.04          # illustrative rate, not a real price list

always_on = VCPUS * 24 * DAYS * PRICE_PER_VCPU_HOUR
elastic = VCPUS * PEAK_HOURS * DAYS * PRICE_PER_VCPU_HOUR
print(f"always on: ${always_on:,.2f}")       # always on: $1,843.20
print(f"elastic:   ${elastic:,.2f}")         # elastic:   $230.40
print(f"saving:    {1 - elastic / always_on:.1%}")   # saving:    87.5%
```

The saving shrinks as utilisation rises: a cluster that is busy 20 hours a day gains little from elasticity and may be cheaper on reserved or committed capacity.

> **Pitfall:** Separation is not free across regions. Compute in one region reading storage in another pays inter-region transfer for every byte and adds latency. Keep compute and storage in the same region.

> **Tip:** Treat Cloud Storage as the universal storage tier for open data: landing files, Iceberg and Delta tables, Spark checkpoints and exports. Tools written for other object stores often work against Cloud Storage through its XML API or its connectors, so you rarely need a second copy of the data.

::: deeper
**Snowflake's design** (SIGMOD 2016) is the reference paper for the separated architecture: a cloud services layer (metadata, optimiser, transactions, security), virtual warehouses of stateless VMs with local SSD caches, and immutable micro-partitions in object storage. Warehouses that read the same micro-partitions consistently land on the same nodes (consistent hashing), so their caches stay warm.

**BigQuery goes further** by also separating **shuffle** from compute: intermediate data between stages lives in a distributed memory tier, so workers can be added, removed or replaced mid-query. Spark has the same idea in its external shuffle service and remote shuffle services; Managed Service for Apache Spark serverless batches rely on it to scale executors up and down.

**Consistency matters.** Early object stores were eventually consistent for listings, which forced workarounds such as S3Guard. Cloud Storage gives strong consistency for object reads after writes, overwrites, deletes and listings, which removes a class of lakehouse bugs, but table formats still need a catalog to commit atomically (an object store has no multi-object transactions).

**Caching layers.** Separated systems rebuild locality with caches: result caches (identical queries), local SSD data caches (hot columns and files), metadata caches (file lists and statistics) and in-memory accelerators (BI Engine). A warehouse that is suspended and resumed loses its local cache; a serverless pool keeps warm caches across customers, which is one reason BigQuery has no visible cold start.

**Disaggregation keeps going.** Research and industry systems now also separate memory (remote memory pools), the write-ahead log (Aurora-style log services, AlloyDB's storage layer) and even the query optimiser. The lakehouse paper (CIDR 2021) describes the economic argument for open formats on object storage.

> **Interview:** "Why did the industry move to compute-storage separation, and what are its downsides?" Faster networks and columnar formats made remote reads cheap, and the cloud made compute rentable by the second, so separating them gives elastic scaling, workload isolation and separate billing. Downsides: network latency and egress if regions differ, cold caches after suspension, metadata and small-file overheads, and the need for a catalog to provide transactions over object storage.
:::

::: check
?? Why does compute-storage separation let several teams query the same data without slowing each other down?
Each team can run its own compute (warehouse, cluster or reservation) against the shared storage, so they do not compete for the same CPUs; only storage is shared, and it scales independently.

?? What is a cold start in a separated system, and how do engines reduce it?
The first queries after compute starts or resumes find empty local caches and must read everything remotely. Engines reduce it with persistent shared caches, result caches, metadata caches, consistent assignment of files to nodes, and always-on serverless pools.

?? When does elastic compute save little money?
When the workload keeps compute busy most of the day. Then elasticity has little idle time to remove, and reserved or committed capacity is usually cheaper.
:::

::: resources
- Paper: [The Snowflake Elastic Data Warehouse (SIGMOD 2016)](https://dl.acm.org/doi/10.1145/2882903.2903741) · the reference description of a separated cloud warehouse
- Paper: [Dremel: A Decade of Interactive SQL Analysis at Web Scale (VLDB 2020)](https://research.google/pubs/dremel-a-decade-of-interactive-sql-analysis-at-web-scale/) · how BigQuery's engine moved to disaggregated storage, memory and shuffle
- Paper: [Lakehouse: A New Generation of Open Platforms (CIDR 2021)](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf) · the case for open formats on object storage with separate engines
- Docs: [BigQuery storage overview](https://docs.cloud.google.com/bigquery/docs/storage_overview) · how BigQuery stores data apart from compute
- Docs: [Cloud Storage consistency](https://docs.cloud.google.com/storage/docs/consistency) · which operations are strongly consistent and why it matters for lakehouses
- Blog: [Frequently asked questions about the data lakehouse (Databricks)](https://www.databricks.com/blog/2021/08/30/frequently-asked-questions-about-the-data-lakehouse.html) · the lakehouse vendor's view of separated compute over open storage
:::

### IAM for data on Google Cloud

::: plain
IAM is the building's security desk. Every visitor (a person or a program) shows a badge saying who they are, and the desk checks a list on each door: "the data team may read in this room", "the loading robot may write in that one". Instead of writing a name on every door, you write group names ("analysts", "pipeline robots") and simply add or remove people from the groups.
:::

Identity and Access Management answers one question for every API call: **may this principal perform this permission on this resource?**

- A **principal** is who is asking: a Google account (`user:ana@acme.com`), a **group** (`group:analysts@acme.com`), a **service account** (`serviceAccount:etl@acme-data-pipelines-prod.iam.gserviceaccount.com`), or an identity from an external provider through **workforce** or **workload identity federation**.
- A **permission** is one action, such as `bigquery.tables.getData` or `storage.objects.create`.
- A **role** is a named bundle of permissions. **Basic roles** (Owner, Editor, Viewer) are far too broad for data work; **predefined roles** (`roles/bigquery.dataViewer`, `roles/pubsub.publisher`, `roles/dataflow.worker`) fit most needs; **custom roles** fill gaps.
- An **allow policy** attached to a resource lists role bindings: "these principals have this role here". Policies are inherited down the hierarchy and the effective access is the union of all bindings from the resource up to the organization.

Most data resources accept policies directly: a BigQuery dataset, table or view; a Cloud Storage bucket; a Pub/Sub topic or subscription; a Secret Manager secret; a KMS key. This plays the role that resource-based policies play on other platforms. Grant at the lowest level that works:

```bash
# readers of one bucket, granted to a group
gcloud storage buckets add-iam-policy-binding gs://acme-landing-prod \
  --member=group:data-eng@acme.com --role=roles/storage.objectViewer

# a pipeline may publish to one topic only
gcloud pubsub topics add-iam-policy-binding orders \
  --member=serviceAccount:orders-api@acme-app-prod.iam.gserviceaccount.com \
  --role=roles/pubsub.publisher
```

```sql
-- BigQuery data control language: grant on a dataset (schema)
GRANT `roles/bigquery.dataViewer`
ON SCHEMA `acme-data-dwh-prod.marts`
TO "group:analysts@acme.com";
```

**Service accounts** are identities for programs. Rules for data platforms:

- **One service account per pipeline or workload**, with only the roles it needs. A Dataflow job reading Pub/Sub and writing BigQuery needs `roles/dataflow.worker` in its project, `roles/pubsub.subscriber` on its subscription and `roles/bigquery.dataEditor` on its target dataset, not Editor on the project.
- **No downloaded keys.** A service account key file is a long-lived password that leaks into laptops, repositories and CI logs. Code running on Google Cloud gets credentials automatically from the attached service account; code elsewhere uses **workload identity federation** (for example GitHub Actions exchanging its OIDC token for short-lived Google credentials). Enforce it with the organization policy `iam.disableServiceAccountKeyCreation`.
- **Impersonation for humans.** Engineers who need to act as a pipeline get `roles/iam.serviceAccountTokenCreator` on that service account and run `gcloud ... --impersonate-service-account=...`, leaving an audit trail, instead of holding its key.
- **Service agents** are Google-managed service accounts that a service uses on your behalf (for example the Dataflow service agent that creates worker VMs, or the BigQuery service agent that uses your KMS key). Do not delete or strip their roles.

**Guard rails beyond allow policies.**

| Need | Google Cloud feature |
|---|---|
| "Never, whatever else is granted" | **IAM deny policies** on the organization, a folder or a project (for example deny `bigquery.datasets.delete` to everyone except a break-glass group) |
| Access only under conditions | **IAM conditions** in CEL: expiry dates, resource name prefixes, resource tags |
| Organization-wide rules | **Organization policies**: allowed locations, domain-restricted sharing, no public buckets, no service account keys |
| Temporary elevated access | **Privileged Access Manager**: just-in-time grants with approval and expiry |
| Network-level protection | **VPC Service Controls** (next group) |
| Row, column and masking rules in BigQuery | Row access policies, policy tags and data policies (see [BigQuery security and governance](#bigquery)) |
| Lake-wide governance | **Knowledge Catalog** (formerly Dataplex Universal Catalog), covered later in the course |

> **Pitfall:** Granting `roles/editor` to a pipeline's service account because "it needed many things". Editor can delete datasets and read every bucket in the project. Use the IAM recommender, which suggests removing permissions a principal has not used in the last 90 days, to shrink such grants.

> **Pitfall:** Granting to individuals. When people change teams their old grants stay behind. Grant to groups and manage membership in one place.

::: deeper
**Evaluation order.** For each request Google Cloud checks principal access boundary policies (which resources a principal is even eligible to access), then deny policies (a matching deny wins), then allow policies (some binding on the resource or an ancestor must grant the permission). Organization policies and VPC Service Controls are separate checks that can still block an allowed request. The **Policy Troubleshooter** and **Policy Analyzer** answer "why can or can't this principal do this?" across all layers.

**Propagation.** IAM changes are not instant: they usually take effect within a couple of minutes, but can take longer. Pipelines that grant and then immediately use a role should retry on permission errors.

**Two projects, two roles.** In BigQuery a query needs `bigquery.jobs.create` in the project that runs (and pays for) the job and data permissions where the data lives. In Dataflow the **worker service account** needs access to sources and sinks, while the person launching the job needs `roles/dataflow.developer` and permission to act as that service account (`iam.serviceAccounts.actAs`). Forgetting `actAs` is the most common launch error.

**Groups and policy size.** Allow policies have size limits, and every binding with an individual makes them harder to audit. Groups keep policies small, make access reviews a matter of reading group memberships, and let HR-driven processes remove access.

**Cross-project access** works naturally: a service account from one project can be granted roles on resources in another. That is how a central pipelines project writes into a warehouse project. Combine it with domain-restricted sharing so nobody can grant to principals outside your organization.

> **Interview:** "A Dataflow job fails with a permission error writing to BigQuery. How do you debug it?" Identify the worker service account (not the launcher), check its roles on the target dataset with the Policy Troubleshooter, look for deny policies or VPC Service Controls violations in the audit logs, and confirm the job runs in the expected project. Then fix it with a dataset-level grant to that service account, not a project-wide role.
:::

::: check
?? Why should a pipeline's service account not have a downloaded key?
A key is a long-lived credential that can leak and is hard to rotate. Workloads on Google Cloud get short-lived credentials from the attached service account, and external systems can use workload identity federation instead.

?? An analyst has `roles/bigquery.dataViewer` on a dataset but cannot run a query. What is missing?
Permission to create jobs (for example `roles/bigquery.jobUser`) in some project that will run and pay for the query.

?? When would you use a deny policy rather than simply not granting a role?
When you need a rule that holds regardless of what is granted elsewhere, for example "nobody except the break-glass group may delete production datasets", even if someone later grants Owner by mistake.
:::

::: resources
- Docs: [IAM overview](https://docs.cloud.google.com/iam/docs/overview) · principals, roles, allow policies and inheritance
- Docs: [Use IAM securely](https://docs.cloud.google.com/iam/docs/using-iam-securely) · Google's checklist of IAM best practices
- Docs: [Best practices for using service accounts](https://docs.cloud.google.com/iam/docs/best-practices-service-accounts) · one account per workload, no keys, impersonation
- Docs: [Workload Identity Federation](https://docs.cloud.google.com/iam/docs/workload-identity-federation) · keyless access from CI systems and other clouds' workloads
- Docs: [Deny policies](https://docs.cloud.google.com/iam/docs/deny-overview) · how deny rules are evaluated and which permissions they support
- Docs: [Introduction to the Organization Policy Service](https://docs.cloud.google.com/resource-manager/docs/organization-policy/overview) · constraints that apply across the hierarchy
:::

## Networking and security

With identities in place, the next questions are about where data travels and who could read it on the way. This group covers three layers of protection that go beyond IAM. The **network** layer keeps traffic on private addresses and draws a perimeter that stops data from being copied out even by someone with valid credentials. The **encryption** layer decides who holds the keys to data at rest and how secrets reach pipelines. **Confidential computing** protects data while it is being processed, which matters when partners or regulators do not want to trust even the cloud operator.

Each layer answers a different threat: stolen credentials, a malicious insider, a compromised host, a curious partner. Real platforms combine them, and the cost of each in complexity is part of the design.

### VPC, Private Service Connect and VPC Service Controls

::: plain
Imagine your data services as rooms in an office building. A **VPC** is the private corridor system connecting your rooms, so traffic does not go out onto the public street. **Private Service Connect** is a private door from your corridor straight into a shop next door (a Google service or a partner), so you never step outside to reach it. **VPC Service Controls** is a security fence around a group of rooms: even someone holding a valid key cannot carry documents through the fence to a room outside it.
:::

**VPC networks.** A Virtual Private Cloud network on Google Cloud is **global**: one network spans all regions, and **subnets** are regional IP ranges inside it. VMs in any subnet reach each other over internal IPs, subject to **firewall rules** and policies. Dataflow workers, Managed Spark clusters, Managed Airflow environments and Batch jobs all run their VMs in a subnet you choose.

**Reaching Google APIs privately.** BigQuery, Cloud Storage and Pub/Sub are APIs, not VMs in your network. By default a VM reaches them through public endpoints. Two features keep that traffic private:

- **Private Google Access** (a per-subnet switch) lets VMs **without external IP addresses** call Google APIs over Google's network. Turn it on for every subnet that runs data workers, and run workers without public IPs: `--no_use_public_ips` for Dataflow (Python), `--no-address` for Managed Spark clusters.
- **Private Service Connect (PSC) endpoints for Google APIs** give Google APIs an internal IP address inside your VPC, so on-premises systems connected by Cloud VPN or Cloud Interconnect can reach them privately too. You point DNS for `*.googleapis.com` at the endpoint.

**Reaching managed services and partners privately.** Some services run in Google-managed or vendor networks: Cloud SQL, AlloyDB, Google Managed Service for Apache Kafka, Confluent Cloud, Snowflake and many others. **Private Service Connect** lets a producer publish a service and you consume it through an endpoint in your own subnet: traffic stays private, IP ranges do not have to be coordinated, and only that one service is exposed (unlike VPC peering, which joins whole networks). Older services use **private services access**, a VPC peering to a Google-managed network.

**Outbound internet.** Workers that must call external APIs (a SaaS source, PyPI) without public IPs use **Cloud NAT** on the subnet's region.

```bash
gcloud compute networks subnets update data-workers-euw3 \
  --region=europe-west3 --enable-private-ip-google-access

python orders_pipeline.py --runner=DataflowRunner --region=europe-west3 \
  --subnetwork=regions/europe-west3/subnetworks/data-workers-euw3 \
  --no_use_public_ips \
  --service_account_email=orders-df@acme-data-pipelines-prod.iam.gserviceaccount.com
```

**VPC Service Controls (VPC SC).** IAM decides *who* may act; VPC SC decides *from where and to where* data may move. A **service perimeter** surrounds a set of projects and a list of **restricted services** (BigQuery, Cloud Storage, Pub/Sub, Dataflow and so on). Inside the perimeter, services talk freely. Requests that cross the boundary are blocked unless a rule allows them:

- A user with `dataViewer` copies a production table into a personal project outside the perimeter: **blocked**.
- A compromised service account key is used from the internet: **blocked**, unless an **access level** (from Access Context Manager: IP ranges, device policy, identity) allows that origin.
- A pipeline must read a partner bucket in another organization: allowed only by an explicit **egress rule** naming the identity, service, methods and target project.

[[fig:gcpfound_perimeter]]

```bash
gcloud access-context-manager perimeters create data_prod \
  --policy=123456789 --title="Data production" \
  --resources=projects/111111111111,projects/222222222222 \
  --restricted-services=bigquery.googleapis.com,storage.googleapis.com,pubsub.googleapis.com,dataflow.googleapis.com \
  --perimeter-type=regular
```

Always start a perimeter in **dry-run mode**: violations are logged but not enforced, so you can see what would break (scheduled transfers, BI tools, CI pipelines, cross-project jobs) and write ingress and egress rules before enforcing.

> **Pitfall:** Turning on enforcement without a dry run. Looker, BigQuery Data Transfer Service runs, Cloud Build deploys and notebooks outside the perimeter can all fail at once. Run in dry-run mode for at least a couple of weeks of normal traffic first.

> **Tip:** On-premises and VM clients inside a perimeter should resolve Google APIs to the `restricted.googleapis.com` virtual IP range (199.36.153.4/30) or a PSC endpoint for the restricted bundle, so that only VPC SC-supported services are reachable.

::: deeper
**Why both IAM and VPC SC?** IAM protects against unauthorised principals; VPC SC protects against authorised principals moving data to the wrong place (exfiltration), and against stolen credentials used from outside. Regulators and security teams usually ask for both on sensitive data.

**Perimeter design.** One large perimeter for all production data projects is easier to run than many small ones; use separate perimeters for clearly separate trust zones (for example a PCI zone). **Perimeter bridges** let two perimeters share specific projects, but ingress and egress rules are the more precise, modern tool. Rules can name specific methods (for example only `google.storage.objects.get`), which limits what a rule opens.

**Troubleshooting.** Blocked requests return an error with a **VPC SC violation unique ID**; the audit log entry for that ID names the perimeter, the service and the reason. The VPC SC troubleshooter in the console decodes it. Collect violations during dry run with a log sink into BigQuery and group by service and identity to design rules.

**Service-specific behaviour.** Some services need extra configuration inside perimeters: Dataflow workers must run in the perimeter's projects; Managed Airflow needs its environment's projects inside; BigQuery cross-project queries require all referenced projects to be in the same perimeter or allowed by rules. BigQuery sharing listings across perimeters need explicit rules. Check the "supported products and limitations" page for each service you add.

**PSC versus VPC peering.** Peering is non-transitive and requires non-overlapping IP ranges between networks; PSC exposes a single service through a single IP in your subnet and works across overlapping ranges, which is why newer managed services (Managed Kafka, AlloyDB and Cloud SQL with PSC) prefer it.

> **Interview:** "How do you stop a data scientist with read access from copying customer data to a personal project?" IAM alone cannot, because reading and copying use the same permissions. Put production projects in a VPC Service Controls perimeter with BigQuery and Cloud Storage restricted, so copies to projects outside the perimeter fail; give data scientists a project inside the perimeter for their work; add access levels so access works only from managed devices or corporate networks.
:::

::: check
?? A Dataflow job runs with `--no_use_public_ips` and fails to reach BigQuery. What is the likely cause?
Private Google Access is not enabled on the workers' subnet, so VMs without external IPs cannot reach Google APIs.

?? What does VPC Service Controls protect against that IAM does not?
Data exfiltration by principals who do have access (copying data to resources outside the perimeter), and use of valid credentials from outside allowed networks or devices.

?? Why is Private Service Connect often preferred over VPC peering for managed services?
It exposes only the one service through an endpoint in your subnet, does not require non-overlapping IP ranges, and avoids joining whole networks.
:::

::: resources
- Docs: [VPC network overview](https://docs.cloud.google.com/vpc/docs/vpc) · global networks, regional subnets, routes and firewalls
- Docs: [Private Google Access](https://docs.cloud.google.com/vpc/docs/private-google-access) · reaching Google APIs from VMs without external IPs
- Docs: [Private Service Connect](https://docs.cloud.google.com/vpc/docs/private-service-connect) · endpoints for Google APIs and published services
- Docs: [Overview of VPC Service Controls](https://docs.cloud.google.com/vpc-service-controls/docs/overview) · perimeters, access levels, ingress and egress rules, dry run
- Docs: [Dataflow network and subnetwork options](https://docs.cloud.google.com/dataflow/docs/guides/specifying-networks) · running Dataflow workers privately in your VPC
- Docs: [Security foundations blueprint: networking](https://docs.cloud.google.com/architecture/blueprints/security-foundations/networking) · a reference network layout with Shared VPC and private access
:::

### Encryption and keys: CMEK, Cloud KMS and Secret Manager

::: plain
Every box in Google's warehouse is already locked, and Google keeps the keys in its own safe: that is **default encryption**, and you get it without doing anything. With **customer-managed keys** you keep the master key in your own safe (Cloud KMS) and Google must ask your safe every time it wants to open your boxes. If you take the key away, nobody can open them, not even Google. **Secret Manager** is a separate small safe for passwords and API tokens that your pipelines need, so they never sit in code.
:::

**Default encryption.** All data at rest on Google Cloud is encrypted with AES-256, and data in transit between Google's facilities and to Google APIs is encrypted with TLS. At rest Google uses **envelope encryption**: data is split into chunks, each chunk is encrypted with a **data encryption key (DEK)**, and the DEKs are themselves encrypted with a **key encryption key (KEK)** kept in Google's internal key management service. You do nothing and pay nothing for this.

**Customer-managed encryption keys (CMEK).** With CMEK the KEK for a resource is a key **you** create in **Cloud KMS**. The service (BigQuery, Cloud Storage, Pub/Sub, Dataflow, Managed Spark, Managed Airflow and most other data services) asks Cloud KMS to wrap and unwrap DEKs with your key. That gives you:

- **Control**: disabling or destroying the key version makes the data unreadable (crypto-shredding).
- **Audit**: every use of the key appears in Cloud Audit Logs.
- **Separation of duties**: key administrators (in a separate project) are different people from data administrators.
- **Location and protection level**: software keys, **Cloud HSM** keys (FIPS 140-2 Level 3 hardware), or **Cloud EKM** keys held in an external key manager outside Google.

Objects in Cloud KMS:

```text
project acme-kms-prod
  key ring  data-euw3   (location europe-west3, must match the data's location)
    key     bq-dwh      (purpose ENCRYPT_DECRYPT, rotation period 90 days)
      version 1  (enabled)
      version 2  (enabled, primary: used for new encryption)
```

```bash
gcloud kms keyrings create data-euw3 --location=europe-west3 --project=acme-kms-prod
gcloud kms keys create bq-dwh --keyring=data-euw3 --location=europe-west3 \
  --purpose=encryption --rotation-period=90d --next-rotation-time=2026-12-01T00:00:00Z \
  --project=acme-kms-prod

# let BigQuery's service agent for the data project use the key
gcloud kms keys add-iam-policy-binding bq-dwh --keyring=data-euw3 --location=europe-west3 \
  --project=acme-kms-prod \
  --member=serviceAccount:bq-222222222222@bigquery-encryption.iam.gserviceaccount.com \
  --role=roles/cloudkms.cryptoKeyEncrypterDecrypter
```

```sql
-- new tables in this dataset are protected by the key
CREATE SCHEMA `acme-data-dwh-prod.finance`
OPTIONS (
  location = 'europe-west3',
  default_kms_key_name = 'projects/acme-kms-prod/locations/europe-west3/keyRings/data-euw3/cryptoKeys/bq-dwh'
);
```

**Rotation** creates a new key version and makes it primary. New data is encrypted with the new version; existing data stays encrypted with the version used at the time and remains readable as long as that version is enabled. So rotation limits how much data one version protects, but it does not re-encrypt old data. Never destroy old versions while data still depends on them. Destroying is delayed (scheduled destruction, 30 days by default) to give you time to notice mistakes.

**Cloud KMS Autokey** automates CMEK: when a developer creates a resource in a folder with Autokey enabled, Cloud KMS creates a key ring and key in a dedicated key project with recommended settings and grants the service agent access. It removes most of the manual work above.

**Secret Manager** stores credentials that pipelines need: database passwords, SaaS API tokens, webhook signing keys. A **secret** has **versions**; code asks for `latest` or a pinned version, and access is controlled by IAM (`roles/secretmanager.secretAccessor` on that secret only).

```bash
printf '%s' "s3cr3t-from-the-vault" | gcloud secrets create salesforce-api-token \
  --replication-policy=automatic --data-file=- --project=acme-data-pipelines-prod
gcloud secrets add-iam-policy-binding salesforce-api-token \
  --member=serviceAccount:sfdc-ingest@acme-data-pipelines-prod.iam.gserviceaccount.com \
  --role=roles/secretmanager.secretAccessor --project=acme-data-pipelines-prod
```

```python
from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()
name = "projects/acme-data-pipelines-prod/secrets/salesforce-api-token/versions/latest"
token = client.access_secret_version(name=name).payload.data.decode("utf-8")
```

Where secrets reach data tools: Cloud Run and Cloud Run functions can mount a secret as an environment variable or a file; Managed Airflow can use Secret Manager as its **secrets backend** for Airflow connections and variables; Dataflow and Spark jobs read secrets at start-up with the client library.

> **Pitfall:** Putting passwords in Airflow variables, DAG files, pipeline options or environment variables printed at start-up. They end up in Git, in logs and in the Airflow UI. Use Secret Manager and log only the secret's name.

> **Pitfall:** Key and data in different locations. A BigQuery dataset in `EU` needs a key in the `europe` multi-region location (or a matching one), and a dataset in `europe-west3` needs a key in `europe-west3`. Mismatched locations fail at creation time.

::: deeper
**What CMEK does not do.** It does not hide data from principals who have IAM access to it: BigQuery decrypts transparently for any authorised query. It protects against loss of control over storage and gives you a kill switch; per-value protection inside a table needs column-level encryption (BigQuery's AEAD functions) or masking.

**Availability risk.** With CMEK, Cloud KMS becomes a dependency of every read and write. If the key is disabled, or the service agent loses its role, pipelines fail. With Cloud EKM, the external key manager's availability matters too. Monitor key state and alert on changes to key IAM.

**Organization policies for keys.** `gcp.restrictNonCmekServices` forces listed services to be created with CMEK, and `gcp.restrictCmekCryptoKeyProjects` limits which projects keys may come from, which keeps keys in the security team's projects.

**Rotation and re-encryption.** Some services re-encrypt data in the background or offer an explicit re-key operation; others rewrite data only when you rewrite it (for example by copying a table or rewriting objects). For Cloud Storage, `gcloud storage objects update --encryption-key=...` rewrites objects with a new key. Check each service's CMEK page before promising auditors that "all data uses the latest key version".

**Costs.** Cloud KMS charges per active key version per month and per cryptographic operation (at the time of writing about $0.06 per software key version per month and about $0.03 per 10,000 operations, more for HSM and EKM keys). Services cache unwrapped DEKs, so operation counts stay modest.

**Secret Manager details.** Secrets can be global with automatic or user-managed replication, or **regional** for data residency. **Rotation schedules** publish a message to a Pub/Sub topic when a secret is due, and your automation creates the new version; Secret Manager does not rotate third-party passwords by itself. Pin versions in production deployments if you need reproducible releases, and use `latest` only where rotation must take effect without a redeploy.

> **Interview:** "When would you recommend CMEK over default encryption?" When regulation or contracts require customer control of keys, a demonstrable kill switch (crypto-shredding), audit of every key use, or separation of duties between key and data administrators. Otherwise default encryption is equally strong cryptographically and avoids the operational dependency on Cloud KMS.
:::

::: check
?? A key is rotated. Is existing BigQuery data re-encrypted with the new key version?
Not by rotation alone. New data uses the new primary version; existing data stays protected by the version used at the time and remains readable while that version is enabled.

?? What happens to CMEK-protected data if the key version is disabled?
The service can no longer unwrap the data encryption keys, so the data becomes unreadable until the version is re-enabled (or permanently, if it is destroyed).

?? Where should a pipeline that calls a SaaS API keep its token, and how does it get access?
In Secret Manager. The pipeline's service account gets `roles/secretmanager.secretAccessor` on that one secret and reads it at run time through the client library or a mounted secret.
:::

::: resources
- Docs: [Default encryption at rest](https://docs.cloud.google.com/docs/security/encryption/default-encryption) · envelope encryption and key hierarchy inside Google
- Docs: [Customer-managed encryption keys (CMEK)](https://docs.cloud.google.com/kms/docs/cmek) · how CMEK works and which services support it
- Docs: [Key rotation](https://docs.cloud.google.com/kms/docs/key-rotation) · rotation periods, primary versions and re-encryption
- Docs: [Autokey overview](https://docs.cloud.google.com/kms/docs/autokey-overview) · automated key creation and assignment for CMEK
- Docs: [Secret Manager overview](https://docs.cloud.google.com/secret-manager/docs/overview) · secrets, versions, replication and access control
- Docs: [Configure Secret Manager for a Managed Airflow environment](https://docs.cloud.google.com/composer/docs/composer-3/configure-secret-manager) · Secret Manager as the Airflow secrets backend
:::

### Confidential Computing for Data

::: plain
Ordinary encryption protects data in a locked suitcase while it travels and while it sits on a shelf, but to work on the contents you must open the suitcase on a table where the hotel staff could look. **Confidential computing** gives you a sealed glass box you can work inside: the processor keeps memory encrypted so that even the machine's owner cannot read it, and it can prove to others exactly what software is running inside.
:::

Data has three states: **at rest** (on disk), **in transit** (on the network) and **in use** (in memory while a CPU processes it). Encryption covers the first two. Confidential computing covers the third, using a **trusted execution environment (TEE)**: a hardware-isolated region where memory is encrypted with keys that never leave the processor.

Two properties matter:

- **Isolation**: the hypervisor, the host operating system and the cloud operator cannot read the TEE's memory.
- **Attestation**: the hardware produces a signed report of what is running (firmware, boot measurements, the workload image). A key service releases secrets only to a TEE whose report matches a policy, so data owners can trust the code without trusting the host.

**Hardware approaches.**

| Technology | Isolation unit | Notes |
|---|---|---|
| AMD SEV, SEV-SNP | Whole VM | Memory encrypted per VM; SNP adds integrity protection against a malicious hypervisor |
| Intel TDX | Whole VM ("trust domain") | Similar VM-level model with integrity protection |
| Intel SGX | Process enclave | Small enclaves inside a process; strong isolation but code must be partitioned, so it is rarely used for data platforms |
| NVIDIA confidential computing | GPU | Extends the TEE to supported GPUs over encrypted PCIe channels |

Google Cloud uses the **VM-level** model, so existing software runs unchanged.

**What you can use on Google Cloud.**

- **Confidential VM**: a Compute Engine VM with SEV, SEV-SNP or TDX, chosen at creation. No code changes.
- **Confidential VM in data services**: Dataflow workers (the `enable_confidential_compute` service option), Managed Service for Apache Spark clusters (a confidential compute flag on cluster creation), GKE nodes (Confidential GKE Nodes) and Gemini Enterprise Agent Platform Workbench.
- **Confidential Space**: a hardened, attested runtime for a container. Each data owner grants their encryption key (through workload identity federation and an attestation policy) only to a specific container image running in Confidential Space. Several companies can then pool sensitive data for a joint computation, and none of them, nor the operator, can see the others' raw data.

```bash
python fraud_features.py --runner=DataflowRunner --region=europe-west3 \
  --dataflow_service_options=enable_confidential_compute

gcloud dataproc clusters create claims-spark --region=europe-west3 \
  --confidential-compute --master-machine-type=n2d-standard-8 --worker-machine-type=n2d-standard-8
```

The overhead of VM-level memory encryption is small for most data workloads (low single-digit percent in Google's and AMD's published measurements), though machine-type choices are restricted (for example AMD-based N2D or C3D families for SEV).

**Cryptographic alternatives** compute on data without any trusted hardware:

- **Secure multi-party computation (SMPC)**: parties split their inputs into random-looking **shares**, compute on the shares, and combine only the final result. Google's open-source **Private Join and Compute** library lets two parties compute, for example, the sum of spend for customers they have in common without revealing their lists.
- **Homomorphic encryption (HE)**: compute directly on ciphertext. Fully homomorphic encryption supports arbitrary computation but is still orders of magnitude slower than plaintext; it suits small, high-value computations. Google publishes an open-source FHE compiler toolchain.
- **Differential privacy** and **data clean rooms**: release only aggregates with noise or thresholds. BigQuery data clean rooms (with BigQuery sharing) are often the practical answer to "let a partner analyse our data without seeing rows".

A tiny example of additive secret sharing, the core idea of many SMPC protocols:

```python
import random

MOD = 2**61 - 1                      # all arithmetic is modulo a large prime
random.seed(7)

def share(secret, parties=3):
    """Split a value into random shares that add up to it modulo MOD."""
    shares = [random.randrange(MOD) for _ in range(parties - 1)]
    shares.append((secret - sum(shares)) % MOD)
    return shares

salaries = {"bank_a": 52000, "bank_b": 61000, "bank_c": 47000}
all_shares = [share(v) for v in salaries.values()]

# party i receives the i-th share of every input and adds them locally
partial_sums = [sum(s[i] for s in all_shares) % MOD for i in range(3)]
print(all(p != 52000 for p in partial_sums))   # True: no party sees a real salary
print(sum(partial_sums) % MOD)                 # 160000: only the total is revealed
```

| Technique | Protects against | Cost | Typical use |
|---|---|---|---|
| Confidential VM | Host, hypervisor, operator reading memory | Small overhead, no code change | Regulated pipelines, defence in depth |
| Confidential Space | Other parties and the operator | Container packaging, attestation setup | Multi-party analytics, model training on pooled data |
| SMPC | Other parties | Protocol-specific, network heavy | Private set intersection, joint statistics |
| Homomorphic encryption | Whoever runs the computation | Very slow | Small, high-value computations |
| Clean rooms, differential privacy | Partners learning individual rows | Low | Marketing measurement, partner analytics |

> **Pitfall:** Treating confidential computing as a replacement for IAM. A TEE protects memory from the host; anyone your IAM lets read the output still can. It is one layer, not the whole design.

::: deeper
**Attestation in practice.** In Confidential Space, the workload's container image digest, the launch policy and the hardware measurements go into an attestation token signed by Google Cloud Attestation. Each data owner's workload identity pool trusts that token only if its claims match (for example `image_digest == sha256:...` and `support_attributes` includes `STABLE`), and only then can the workload impersonate a service account that may decrypt that owner's data with their Cloud KMS key. Changing one line of code changes the digest and locks everyone out until owners approve the new image, which is the point.

**Threat model limits.** TEEs do not protect against bugs in your own code, against data that you write out unencrypted, or against all side channels (cache timing and similar attacks have been demonstrated against several TEE generations). Firmware updates and newer generations (SEV-SNP, TDX) close many of them.

**Performance.** Memory encryption mainly costs on memory-bound workloads and on I/O, because data crossing into and out of the TEE must be bounced through unencrypted buffers. Large shuffles and heavy disk or network traffic show the most overhead; CPU-bound transforms show the least. Measure your own pipeline.

**SGX versus VM-level TEEs.** SGX enclaves have a tiny trusted computing base (only the enclave code), which is attractive for key management, but require rewriting applications into enclave and non-enclave parts. VM-level TEEs trust the whole guest OS but run existing Spark, Beam or database code unchanged, which is why cloud data services adopted them.

> **Interview:** "Two hospitals want to train a model on their combined patient data without sharing it. What options exist on Google Cloud?" Confidential Space with each hospital's key released only to an attested training container; federated learning if data must never leave each site; SMPC or differential privacy for simpler statistics; or a clean room if aggregate analysis is enough. Discuss the trust each option requires, the performance cost, and how results are audited.
:::

::: check
?? Which state of data does confidential computing protect, and which hardware feature makes it possible?
Data in use, in memory during processing. A trusted execution environment, where the CPU encrypts memory with keys that never leave the processor and can attest to what is running.

?? Why did cloud data services adopt VM-level TEEs rather than SGX enclaves?
VM-level TEEs run unmodified software (Spark, Beam, databases), while SGX requires partitioning applications into enclave code.

?? In additive secret sharing, why does no single party learn an input?
Each party holds only one random-looking share of each input; any subset of shares short of all of them reveals nothing, and only the combined final result is reconstructed.
:::

::: resources
- Docs: [Confidential Computing overview](https://docs.cloud.google.com/confidential-computing/docs/confidential-computing-overview) · Confidential VM, Confidential Space and the services that support them
- Docs: [Confidential VM overview](https://docs.cloud.google.com/confidential-computing/confidential-vm/docs/confidential-vm-overview) · supported technologies, machine types and attestation
- Docs: [Confidential Space overview](https://docs.cloud.google.com/confidential-computing/confidential-space/docs/confidential-space-overview) · multi-party computation with attested containers
- Docs: [Dataflow service options](https://docs.cloud.google.com/dataflow/docs/reference/service-options) · includes the option that runs Dataflow workers on Confidential VMs
- Repo: [Private Join and Compute](https://github.com/google/private-join-and-compute) · Google's open-source secure computation protocol for intersections and sums
- Article: [Confidential Computing Consortium: technical analysis](https://confidentialcomputing.io/resources/white-papers-reports/) · vendor-neutral white papers on TEEs, attestation and threat models
:::

## Running and paying for it

A platform that is secure but invisible and expensive is not finished. This group is about running data workloads day to day: seeing what they do (metrics, logs, alerts and audit trails), running them on cheaper capacity without losing work, keeping network charges under control, cutting the bill across storage and compute, and describing the whole platform as code so it can be rebuilt and reviewed.

These topics connect back to the first group. Projects and labels decide how cost and metrics are grouped; IAM decides who may see logs and change alerts; regions decide what you pay for data transfer. They also lead forward: every service in the next sections (BigQuery, Pub/Sub, Dataflow, Managed Spark) reports into Cloud Monitoring and Cloud Logging, can be bought in cheaper ways, and is usually created with Terraform.

### Cloud Monitoring and Cloud Logging for data pipelines

::: plain
Running pipelines without monitoring is like driving at night without a dashboard: you only learn you were out of fuel when the car stops. **Metrics** are the dials (speed, fuel, temperature), **logs** are the trip diary of everything that happened, **alerts** are the warning lights that call you before the engine fails, and **audit logs** are the security camera recording who opened which door.
:::

Google Cloud's observability suite has two main products, and every data service writes to both automatically.

**Cloud Monitoring** stores **metrics**: numeric time series with labels, sampled every minute or faster. Every service publishes built-in metrics; you add **custom metrics** (through OpenTelemetry or the Monitoring API) and **log-based metrics** (counters or distributions computed from matching log entries). You query them in Metrics Explorer with filters or PromQL, put them on dashboards, and attach **alerting policies**.

The metrics that matter most for data pipelines:

| Service | Metric | What it tells you |
|---|---|---|
| Pub/Sub | `subscription/oldest_unacked_message_age` | How far behind consumers are, in seconds: the single best streaming alert |
| Pub/Sub | `subscription/num_undelivered_messages` | Backlog size in messages |
| Pub/Sub | `subscription/dead_letter_message_count` | Messages that failed repeatedly |
| Dataflow | `job/system_lag` | Longest time an element has waited to be processed |
| Dataflow | `job/data_watermark_age` | How far event-time progress is behind real time |
| Dataflow | `job/is_failed`, `job/current_num_vcpus` | Failures and scaling |
| BigQuery | Slot usage and job counts by reservation; `INFORMATION_SCHEMA.JOBS` | Capacity pressure, failing or slow queries |
| Managed Airflow | DAG run and task failure metrics, scheduler heartbeat | Orchestration health |
| Cloud Storage | Request counts, bytes sent, by bucket | Unexpected readers, landing volume |

**Alerting policies** combine a condition (metric above a threshold for a duration, metric absent, a log match, or a PromQL expression) with **notification channels** (email, SMS, Slack, PagerDuty, webhooks, Pub/Sub) and documentation that tells the on-call engineer what to do.

```yaml
# alert-pubsub-backlog.yaml: page when orders consumers fall 10 minutes behind
displayName: "orders-sub: consumers more than 10 minutes behind"
combiner: OR
conditions:
  - displayName: "oldest unacked message > 600 s for 5 min"
    conditionThreshold:
      filter: >
        resource.type = "pubsub_subscription" AND
        resource.labels.subscription_id = "orders-sub" AND
        metric.type = "pubsub.googleapis.com/subscription/oldest_unacked_message_age"
      comparison: COMPARISON_GT
      thresholdValue: 600
      duration: 300s
      aggregations:
        - alignmentPeriod: 60s
          perSeriesAligner: ALIGN_MAX
documentation:
  content: "Check the Dataflow job orders-stream for errors or a stuck stage. Runbook: go/orders-runbook"
notificationChannels:
  - projects/acme-data-pipelines-prod/notificationChannels/1234567890
```

```bash
gcloud monitoring policies create --policy-from-file=alert-pubsub-backlog.yaml \
  --project=acme-data-pipelines-prod
```

**Cloud Logging** stores **log entries**: structured records with a resource, severity, timestamp, labels and a text or JSON payload. Everything flows through the **Log Router**, which applies **sinks**: rules with a filter and a destination.

- Every project has a `_Required` bucket (admin audit logs and similar, kept 400 days, not configurable, no charge) and a `_Default` bucket (everything else, 30 days by default, configurable).
- **Sinks** can route matching entries to another log bucket (including a central one in another project), to BigQuery, to Cloud Storage for cheap long-term retention, or to Pub/Sub for streaming to other tools. **Exclusion filters** drop noisy logs before they are stored and billed.
- **Log Analytics** lets you query log buckets with SQL, and link a bucket to a BigQuery dataset so logs can be joined with other data.

```bash
# keep every Dataflow and Managed Spark error log in BigQuery for a year of analysis
gcloud logging sinks create pipeline-errors-to-bq \
  bigquery.googleapis.com/projects/acme-ops/datasets/pipeline_logs \
  --log-filter='severity>=ERROR AND (resource.type="dataflow_step" OR resource.type="cloud_dataproc_cluster")' \
  --use-partitioned-tables --project=acme-data-pipelines-prod
# then grant the sink's writer identity roles/bigquery.dataEditor on the dataset
```

**Structured logs** make filtering and log-based metrics easy. On Cloud Run, GKE and Managed Spark, a JSON object printed on one line to standard output becomes a `jsonPayload`, and fields such as `severity` and `message` are recognised:

```python
import json, sys

def log(severity, message, **fields):
    entry = {"severity": severity, "message": message, **fields}
    print(json.dumps(entry, sort_keys=True), file=sys.stdout, flush=True)

rows = 12840
log("INFO", "batch loaded", pipeline="orders", rows=rows, partition="2026-11-01")
# {"message": "batch loaded", "partition": "2026-11-01", "pipeline": "orders", "rows": 12840, "severity": "INFO"}
```

A log-based metric on `jsonPayload.rows` then gives a row-count time series per pipeline, and an absence alert on it tells you when a daily load did not run at all.

**Audit logs** answer "who did what, where and when":

| Log | Enabled | Records |
|---|---|---|
| Admin Activity | Always, free | Configuration changes: creating datasets, changing IAM, deleting buckets |
| Data Access | Off by default for most services (BigQuery's are on), billed | Reads and writes of data and metadata |
| System Event | Always, free | Google-initiated changes such as live migration |
| Policy Denied | Always on, billed | Requests refused by VPC Service Controls or organization policies |

Turn on Data Access logs for sensitive buckets and services, and route all audit logs to a central, locked-down bucket with long retention.

> **Pitfall:** Alerting on every failed task. Pipelines retry; an alert on the first failure trains people to ignore pages. Alert on symptoms users feel (freshness, backlog age, missing loads) and send single failures to a ticket queue.

> **Pitfall:** Logging every record in a high-volume pipeline. Logging is billed per GiB ingested (at the time of writing about $0.50 per GiB after a free allowance of 50 GiB per project per month), so a Dataflow job that logs each element can cost more than the job. Log samples and counts, and use exclusion filters.

::: deeper
**Freshness as the headline SLO.** For a batch table, publish a custom or log-based metric with the timestamp of the latest loaded partition and alert when `now - latest > SLA`. For streams, `oldest_unacked_message_age` plus Dataflow's `data_watermark_age` cover ingestion and processing lag. These two numbers answer "is the data late?" better than any CPU graph.

**Multi-project views.** A **metrics scope** lets one Monitoring project see metrics from many projects, so the data platform team can build dashboards across landing, pipeline and warehouse projects. For logs, use **aggregated sinks** at the folder or organization level (with `--include-children`) into a central log bucket.

**Log routing order and cost.** Sinks evaluate each entry independently, so an entry can go to several destinations. Exclusions on the `_Default` sink stop storage charges but not routing to other sinks. Retention beyond 30 days in log buckets is billed per GiB-month; Cloud Storage is cheaper for archives, BigQuery better for analysis.

**BigQuery-specific monitoring.** `INFORMATION_SCHEMA.JOBS` and the BigQuery audit logs cover query cost and errors (see [BigQuery cost control](#bigquery)); export the `BigQueryAuditMetadata` to a dataset to build "who read this table" reports.

**OpenTelemetry.** Custom application metrics and traces from Spark drivers, Beam DoFns or Cloud Run services can be sent with the OpenTelemetry SDK and collector to Cloud Monitoring and Cloud Trace, keeping instrumentation vendor-neutral.

> **Interview:** "What would you monitor for a streaming pipeline from Pub/Sub through Dataflow to BigQuery?" Oldest unacked message age and dead-letter counts on the subscription; system lag, watermark age, errors and worker count on the Dataflow job; write errors and streaming throughput on BigQuery; and an end-to-end freshness check that compares the newest event time in the table with now. Alert on age and freshness, with runbooks attached.
:::

::: check
?? Which single Pub/Sub metric best indicates that a streaming consumer is falling behind?
`subscription/oldest_unacked_message_age`: the age of the oldest message that has not been acknowledged.

?? You want to keep all ERROR logs from pipelines for a year and query them with SQL. What do you configure?
A log sink with a severity filter routing to a BigQuery dataset (or a log bucket with Log Analytics and a longer retention), and grant the sink's writer identity access to the destination.

?? Which audit logs are always on, and which must you enable?
Admin Activity and System Event logs are always on (and Policy Denied logs are generated automatically). Data Access logs are off by default for most services (BigQuery is an exception) and must be enabled.
:::

::: resources
- Docs: [Cloud Monitoring overview](https://docs.cloud.google.com/monitoring/docs/monitoring-overview) · metrics, dashboards, alerting and metrics scopes
- Docs: [Routing and storage overview](https://docs.cloud.google.com/logging/docs/routing/overview) · the Log Router, sinks, log buckets and retention
- Docs: [Cloud Audit Logs overview](https://docs.cloud.google.com/logging/docs/audit) · the four audit log types and how to enable Data Access logs
- Docs: [Dataflow monitoring interface](https://docs.cloud.google.com/dataflow/docs/guides/monitoring-overview) · job graphs, lag and watermark metrics for pipelines
- Docs: [Monitor Pub/Sub](https://docs.cloud.google.com/pubsub/docs/monitoring) · the subscription metrics to alert on and why
- Book: [Site Reliability Engineering: Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/) · Google's chapter on symptoms versus causes and alerting philosophy
:::

### Spot VMs and preemptible capacity for data workloads

::: plain
Spot VMs are like stand-by airline seats: very cheap, but the airline can take the seat back whenever a full-fare passenger shows up. That is a bad deal for a meeting you cannot miss, and a great deal for a trip where you can catch the next flight. Data jobs that save their progress regularly, or that can redo a small piece of work, are the trips that can fly stand-by.
:::

**Spot VMs** are Compute Engine VMs sold from spare capacity at large discounts: up to 91% off the on-demand price for many machine types, GPUs and local SSDs. The catch:

- Compute Engine can **preempt** (stop or delete) them at any time when it needs the capacity back.
- On preemption the VM gets a best-effort **shutdown period of up to 30 seconds**; you can also configure a preemption **notice period** (for example 120 seconds) for workloads that need more time to react. The metadata value `preempted` changes to `TRUE`, which software can watch.
- They are not covered by any SLA, cannot live-migrate, and may be unavailable in a zone when capacity is short.
- Unlike the older **preemptible VMs**, Spot VMs have **no 24-hour maximum runtime**. Preemptible VMs still exist and behave much like Spot with that 24-hour limit; new work should use Spot.

**What survives preemption** decides what can use Spot:

| Workload | Good on Spot? | Why |
|---|---|---|
| Spark executors on secondary workers | Yes | Lost tasks are recomputed from lineage; primary workers keep the cluster alive |
| Spark driver or cluster master | No | Losing it kills the application |
| Dataflow batch with FlexRS | Yes | The service mixes preemptible and regular VMs and handles retries for you |
| Dataflow streaming | No | Streaming jobs use regular workers |
| Batch jobs with checkpoints or small tasks | Yes | Failed tasks are retried; progress is saved |
| Long single-process jobs without checkpoints | No | Hours of work lost on each preemption |
| Databases, Kafka brokers, Airflow schedulers | No | Stateful and latency-sensitive |

**Managed Service for Apache Spark.** A cluster has **primary workers** (run HDFS DataNodes and YARN NodeManagers) and optional **secondary workers** (compute only, no HDFS). Make primaries standard VMs and secondaries Spot:

```bash
gcloud dataproc clusters create nightly-etl --region=europe-west3 \
  --num-workers=4 --worker-machine-type=n2-standard-8 \
  --num-secondary-workers=16 --secondary-worker-type=spot \
  --properties=dataproc:efm.spark.shuffle=primary-worker \
  --autoscaling-policy=etl-spot-policy --max-idle=30m
```

The property `dataproc:efm.spark.shuffle=primary-worker` turns on **enhanced flexibility mode**: shuffle data is written to primary workers, so when a Spot secondary disappears its shuffle output is not lost and stages do not have to be recomputed. **Flexible VMs** let secondary workers draw from a ranked list of machine types, which improves the odds of getting Spot capacity.

**Dataflow FlexRS** (Flexible Resource Scheduling) is for batch pipelines that can wait: you accept that the job may start up to six hours later, and Dataflow runs it on a mix of preemptible and regular VMs at a lower price.

```bash
python backfill_2025.py --runner=DataflowRunner --region=europe-west3 \
  --flexrs_goal=COST_OPTIMIZED
```

**GKE** has Spot node pools; schedule interruptible pods (Spark on Kubernetes executors, Ray workers, batch inference) onto them with node selectors or taints, and keep system pods and drivers on standard nodes. **Batch** jobs set `provisioningModel: SPOT` and a retry policy, so tasks preempted mid-run are retried automatically.

**Checkpointing turns a preemption into a short delay.** Without checkpoints a preemption costs all work since the job started; with them, at most one interval. A quick model:

```python
JOB_HOURS = 10.0            # useful work needed
PREEMPT_EVERY = 3.0         # assume one preemption every 3 hours of running
RESTART_COST = 0.1          # hours to get a new VM and reload state

def wall_clock(checkpoint_every):
    done, clock = 0.0, 0.0
    while done < JOB_HOURS:
        run = min(PREEMPT_EVERY, JOB_HOURS - done)
        if run < PREEMPT_EVERY:            # finishes before the next preemption
            return clock + run
        clock += run + RESTART_COST
        kept = (run // checkpoint_every) * checkpoint_every if checkpoint_every else 0.0
        done += kept                       # work after the last checkpoint is lost
    return clock

print(f"{wall_clock(0.5):.1f} h")   # 10.3 h with a checkpoint every 30 minutes
print(f"{wall_clock(2.0):.1f} h")   # 14.4 h with a checkpoint every 2 hours
# wall_clock(0), with no checkpoints, never finishes: every 3-hour run is lost
```

Frequent checkpoints keep the job close to its 10 useful hours. Without them, a 10-hour job on capacity that is preempted every 3 hours never completes. Real preemption rates vary by region, zone, machine family and time of day; Google does not publish them, so measure your own and keep each task or checkpoint interval well below the typical time between preemptions.

**Mixed fleets.** The usual pattern is a stable core of standard VMs plus an elastic layer of Spot: a Spark cluster with 4 standard primaries and up to 40 Spot secondaries, a GKE cluster with a small standard node pool and a large Spot pool, a managed instance group that spreads Spot VMs across zones and several machine types. The core guarantees progress; the Spot layer makes it fast and cheap when capacity is there.

> **Pitfall:** A cluster made mostly of Spot secondaries with few primaries. When many secondaries vanish at once, the primaries must absorb all shuffle traffic and recomputation, and jobs can thrash. Keep enough primaries to finish the work slowly on their own, and cap the ratio of secondaries to primaries.

> **Tip:** Make tasks short. Spark tasks of a few minutes, Beam bundles and Batch tasks that each handle one file turn a preemption into a small retry instead of a large loss.

::: deeper
**Graceful decommissioning.** When an autoscaling policy removes workers, Managed Spark can wait for running tasks to finish (`gracefulDecommissionTimeout` in the policy) instead of killing them. Spot preemptions give no such grace, so enhanced flexibility mode and shuffle placement matter more than autoscaling settings.

**Spark settings that help.** `spark.task.maxFailures` (default 4) controls how many times a task may fail before the job fails; preemptions count as executor loss, not task failures, in recent Spark versions, but very unstable capacity can still exhaust retries. Spark's own decommissioning (`spark.decommission.enabled`, with shuffle and RDD block migration) moves data off executors that receive a shutdown signal, which works best with a longer preemption notice.

**Capacity planning.** Spot availability differs by region, zone and machine family, and can disappear during regional demand peaks. Spread requests across zones and families, prefer slightly older or less popular families, and design jobs to finish (slowly) on standard capacity alone when Spot is unavailable. Batch and managed instance groups can fall back automatically if you configure several instance templates or policies.

**Pricing.** Spot prices are set per machine type and region and can change (Google states they change at most once a month). There is no bidding. Committed use discounts do not apply to Spot VMs, so the comparison for steady workloads is Spot versus committed-use on-demand, not Spot versus list price.

**Preemptible VMs today.** Existing preemptible VMs keep their 24-hour runtime limit and fixed-time behaviour; Managed Spark's default secondary worker type is still preemptible unless you choose Spot. Spot is the recommended model for new work.

> **Interview:** "How would you cut the cost of a nightly 6-hour Spark job by half without missing its deadline?" Move it to an ephemeral cluster created per run, keep a few standard primary workers and add Spot secondaries with enhanced flexibility mode and flexible machine types, make tasks short, write intermediate results so a failed stage can restart cheaply, and size the standard core so the job still meets its deadline if Spot capacity is unavailable. Alternatively run it as a serverless Spark batch and compare.
:::

::: check
?? Why are secondary workers a good place for Spot VMs in a Managed Spark cluster, but the master node is not?
Secondary workers only run executors; lost tasks are recomputed and the cluster survives. The master runs the cluster services (and often the driver), so losing it fails the application.

?? How much warning does a Spot VM get before it stops?
A best-effort shutdown period of up to 30 seconds, plus an optional preemption notice period (for example 120 seconds) if configured. The metadata value `preempted` becomes `TRUE`.

?? What does Dataflow FlexRS trade for its lower price?
Start-time flexibility: the batch job may be delayed by up to six hours, and it runs on a mix of preemptible and regular VMs. It is not available for streaming jobs.
:::

::: resources
- Docs: [Spot VMs](https://docs.cloud.google.com/compute/docs/instances/spot) · pricing model, preemption process, notice periods and limitations
- Docs: [Secondary workers in Managed Service for Apache Spark](https://docs.cloud.google.com/dataproc/docs/concepts/compute/secondary-vms) · Spot and preemptible secondaries and how they behave
- Docs: [Enhanced flexibility mode](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/flex) · keeping shuffle data on primary workers
- Docs: [Using Flexible Resource Scheduling in Dataflow](https://docs.cloud.google.com/dataflow/docs/guides/flexrs) · delayed, discounted batch execution
- Docs: [Use Spot VMs in GKE](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/spot-vms) · Spot node pools, taints and scheduling
- Docs: [Automate task retries in Batch](https://docs.cloud.google.com/batch/docs/automate-task-retries) · retry policies that make Batch jobs on Spot VMs survive preemption
:::

### Data Transfer & Egress Cost Engineering

::: plain
Moving data around the cloud is like shipping parcels. Moving a box between rooms in the same building is free, carrying it to another building on the same street costs a little, sending it to another city costs more, and posting it abroad or to a customer's home costs the most. A data platform ships billions of boxes, so where you put the buildings decides a large part of the bill.
:::

**Data transfer** (often called egress) is charged when bytes leave a boundary. On Google Cloud the main boundaries, from cheapest to most expensive, are:

| Path | Typical charge (at the time of writing) |
|---|---|
| Within the same zone, internal IPs | Free |
| Between zones in the same region | About $0.01 per GiB (VM-to-VM traffic) |
| Between regions on the same continent | About $0.02 per GiB within North America |
| Between continents | About $0.05 per GiB North America to Europe, more for other routes |
| To the internet | Highest, priced by destination and monthly volume tier |
| Into Google Cloud (ingress) | Free |

Prices differ by route and service and change over time; the network pricing page is the authority. What matters for design is the **ratio**: crossing a continent costs several times more than crossing a region, and both cost nothing when you stay put.

[[fig:gcpfound_egress]]

**Service rules worth knowing.**

- **Cloud Storage**: reading a bucket from compute in the same region is free. Reading from another region or continent pays the inter-region rate. Dual-region and multi-region buckets also charge for replicating writes. Retrieval fees for Nearline, Coldline and Archive are separate from transfer.
- **BigQuery**: queries only read data in their own location, so ordinary queries have no transfer charges. Copying datasets or **cross-region replication** pays transfer; exporting to a bucket in another region pays it too. Reading with the Storage Read API from another region adds transfer.
- **Pub/Sub**: publish and subscribe throughput is billed per TiB, and data that crosses zones or regions between publishers, the service and subscribers can add transfer charges.
- **Managed Kafka, Managed Spark, Dataflow**: VMs spread across zones generate inter-zone traffic (replication between Kafka brokers, Spark shuffle between workers in different zones). For batch jobs, one zone is often fine.

**A worked example.** A Dataflow job in `us-central1` reads 10 TiB per day of Parquet from a bucket in `europe-west1` and writes 1 TiB per day to BigQuery in the `US` multi-region:

```python
GIB_PER_TIB = 1024
RATES = {"same_region": 0.00, "inter_region_na": 0.02, "intercontinental_na_eu": 0.05}  # USD per GiB

def monthly_cost(tib_per_day, path, days=30):
    return tib_per_day * GIB_PER_TIB * RATES[path] * days

before = monthly_cost(10, "intercontinental_na_eu")
after = monthly_cost(10, "same_region")          # run the job in europe-west1 instead
print(f"cross-continent reads: ${before:,.0f} per month")   # cross-continent reads: $15,360 per month
print(f"co-located reads:      ${after:,.0f} per month")    # co-located reads:      $0 per month
```

Moving the job next to its input removes the largest charge; the 1 TiB per day of output then crosses the ocean instead, at a tenth of the volume. The general rule: **move the computation to the data, and move only results.**

**Patterns that cut transfer.**

1. **One home region** (or one multi-region) per data domain, with buckets, datasets, Dataflow, Spark and Managed Kafka all inside it.
2. **Reduce before you move**: filter, aggregate and compress (Parquet with Zstandard or Snappy, Avro with compression) before sending data across a boundary. Columnar formats let readers fetch only needed columns.
3. **Share instead of copy**: BigQuery sharing (formerly Analytics Hub) linked datasets and authorized views give other teams access without copies, within a region.
4. **Private, high-volume links** for hybrid platforms: **Cloud Interconnect** (Dedicated or Partner) gives lower data transfer rates than the internet for traffic to on-premises, and **Cross-Cloud Interconnect** links to other clouds.
5. **Bulk migration**: **Storage Transfer Service** for online copies from other object stores, HTTP sources and file systems; **Transfer Appliance**, a shipped storage device, when the network would take weeks.
6. **Read-heavy public data**: serve files through **Cloud CDN** in front of a bucket, or make consumers pay with **Requester Pays** buckets.

> **Pitfall:** "Global" defaults. A multi-region `US` dataset joined with exports into a `us-east1` bucket, a Dataflow job in `us-central1`, and a Spark cluster in `us-west1` all look "in the US" but generate inter-region traffic between each pair.

> **Pitfall:** Forgetting zones. A three-zone Kafka or Spark cluster that shuffles terabytes a day pays inter-zone transfer on every shuffled byte that crosses zones. That is the price of zone redundancy; for batch jobs that can simply be rerun, a single zone may be the better trade.

::: deeper
**Network Service Tiers.** Internet egress from Google Cloud comes in **Premium Tier** (traffic rides Google's backbone to the edge closest to the user) and **Standard Tier** (traffic leaves Google's network near the source region and uses the public internet). Standard is cheaper for bulk internet egress where latency matters less.

**Measuring transfer.** In the billing export, filter SKUs whose description contains "Data Transfer", "Network" or "Egress" and group by project, service and label. VPC Flow Logs (sampled) show which VMs talk to which, across zones and regions. For Cloud Storage, usage logs and Monitoring's `network/sent_bytes_count` by bucket show who reads what.

**Data residency versus cost.** Regulations may require EU data to stay in the EU, which fixes your region. Inside that constraint, pick the region with the services and machine types you need, then put everything there. Disaster recovery copies to a second region are a deliberate, budgeted transfer, not an accident.

**Egress waivers and exits.** Google waives data transfer fees for customers who leave Google Cloud entirely (on request), in line with the EU Data Act. This does not reduce everyday egress, but it changes lock-in arguments.

> **Interview:** "Your Google Cloud bill shows $40,000 a month of network egress for the data platform. How do you find and cut it?" Break down the billing export by SKU, project and label; identify inter-region versus internet versus inter-zone; use VPC Flow Logs and bucket metrics to find the readers; co-locate compute with storage, filter and compress before transfer, replace copies with shared datasets, and move hybrid flows to Interconnect.
:::

::: check
?? Why is running a Dataflow job in the same region as its input bucket usually the biggest single egress saving?
Reading from a bucket in the same region is free, while cross-region or cross-continent reads are charged per GiB on the full input, which is usually much larger than the output.

?? Do ordinary BigQuery queries incur data transfer charges?
No. Queries run in the dataset's location and read data there. Transfer charges come from copying or replicating datasets across regions, exporting to buckets elsewhere, or reading table data from another region.

?? When would you use Transfer Appliance instead of Storage Transfer Service?
When the volume is so large, or the network so limited, that an online transfer would take too long (weeks or months); the appliance is filled on site and shipped to Google.
:::

::: resources
- Docs: [All networking pricing](https://cloud.google.com/vpc/network-pricing) · current inter-zone, inter-region and internet transfer rates
- Docs: [Cloud Storage pricing](https://cloud.google.com/storage/pricing) · storage, retrieval, replication and data transfer for buckets
- Docs: [Network Service Tiers overview](https://docs.cloud.google.com/network-tiers/docs/overview) · Premium versus Standard tier for internet egress
- Docs: [Storage Transfer Service overview](https://docs.cloud.google.com/storage-transfer/docs/overview) · online bulk copies into Cloud Storage
- Docs: [Transfer Appliance overview](https://docs.cloud.google.com/transfer-appliance/docs/4.0/overview) · offline migration of very large datasets
:::

### Cloud Cost Optimization Deep Dive

::: plain
Cutting a cloud bill is like cutting a household budget: first find out where the money goes, then deal with the big items (rent and the car) before the small ones (coffee). For a data platform the big items are usually warehouse queries, always-on clusters and forgotten data, and the fixes are boring habits: right size, switch off, tier down, and commit to what you always use.
:::

Cost work is a loop, often called **FinOps**: **inform** (who spends what, from the billing export and labels), **optimise** (change the expensive things), **operate** (budgets, policies and reviews so savings stick). The levers below are ordered roughly by how much they usually save on a data platform.

**1. Warehouse compute.** BigQuery is typically the largest line. Partitioning, clustering, avoiding `SELECT *`, materialized views, quotas and choosing between on-demand and reservations are covered in depth in [BigQuery cost control](#bigquery) and the slots topic there; do those first.

**2. Always-on clusters.** A Managed Spark cluster left running overnight, or a dev Managed Airflow environment nobody uses at weekends, costs money every hour.

- Prefer **ephemeral clusters**: create a cluster per job or per DAG run, delete it at the end (Airflow operators do this), or set `--max-idle` so idle clusters delete themselves.
- Use **serverless Spark batches** for spiky jobs: you pay only while the batch runs.
- Use **autoscaling policies** with a Spot secondary pool for clusters that must stay up.
- **Right-size** with the Recommender: it flags idle VMs and over-provisioned machine types.

**3. Storage tiering.** Cloud Storage classes trade storage price for access price:

| Class | Storage price (us-central1, about, per GiB-month) | Minimum storage duration | Retrieval fee | Use for |
|---|---|---|---|---|
| Standard | $0.020 | None | None | Landing data, active lake tables |
| Nearline | $0.010 | 30 days | Yes | Data read about once a month |
| Coldline | $0.004 | 90 days | Higher | Data read a few times a year |
| Archive | $0.0012 | 365 days | Highest | Compliance copies, raw backups |

Objects deleted or moved before the minimum duration are charged as if stored for the full period. **Lifecycle rules** move or delete objects automatically; **Autoclass** moves each object between classes based on its own access pattern, for a small management fee per object, with no retrieval or early-deletion charges.

```json
{
  "rule": [
    {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
     "condition": {"age": 30, "matchesPrefix": ["raw/"]}},
    {"action": {"type": "SetStorageClass", "storageClass": "ARCHIVE"},
     "condition": {"age": 365, "matchesPrefix": ["raw/"]}},
    {"action": {"type": "Delete"},
     "condition": {"age": 7, "matchesPrefix": ["tmp/", "staging/"]}}
  ]
}
```

```bash
gcloud storage buckets update gs://acme-landing-prod --lifecycle-file=lifecycle.json
```

Is a colder class worth it? Compare the storage saved with the retrieval paid:

```python
# monthly cost of 100 TiB that is read once a month in full (illustrative us-central1 prices)
TIB = 100 * 1024                     # GiB
standard = TIB * 0.020
nearline = TIB * 0.010 + TIB * 0.01  # storage + retrieval of every byte once
coldline = TIB * 0.004 + TIB * 0.02
print(round(standard), round(nearline), round(coldline))   # 2048 2048 2458
```

Read monthly, Nearline only breaks even and Coldline costs more: cold classes pay off only for data that is rarely read. For lakehouse tables that are compacted and rewritten, minimum durations also bite, so keep active table files in Standard and tier only raw archives.

**4. Commitments for steady usage.** **Committed use discounts (CUDs)** trade a 1- or 3-year commitment for a lower price: resource-based CUDs for specific vCPU and memory amounts in a region, and spend-based (flexible) CUDs that apply across machine families and several services. **Sustained use discounts** apply automatically to some older machine families that run most of a month. BigQuery has its own slot commitments and spend-based CUDs. Commit only to the baseline you are sure to use; cover peaks with on-demand or Spot.

**5. Pipeline engines.**

- **Dataflow**: pick machine types that fit the work (memory-heavy joins on high-memory machines), set `--max_num_workers` to cap autoscaling, use Streaming Engine and Dataflow Shuffle (the defaults) so workers can be smaller, consider **at-least-once streaming mode** where duplicates are acceptable (it is cheaper than exactly-once), and use FlexRS for delayable batch.
- **Managed Spark**: Spot secondaries, ephemeral clusters, and adaptive query execution; write efficient file layouts so later readers scan less.
- **Databricks on Google Cloud**: cluster policies that force auto-termination and limit sizes, job clusters instead of all-purpose clusters for scheduled work, and Spot instances for workers.

**6. Forgotten resources.** Orphaned disks and snapshots, old dev datasets, test Pub/Sub subscriptions retaining messages, unused static IPs, idle Managed Airflow environments. Labels plus a weekly report from the billing export find them.

> **Pitfall:** Optimising the wrong line. Teams spend weeks tuning Spark clusters that cost $500 a month while one dashboard's full-scan queries cost $8,000. Always start from the billing export, sorted by cost.

> **Pitfall:** Moving lake files to Nearline or Coldline and then compacting or querying them. Early-deletion charges and retrieval fees can exceed the storage saved. Tier by age and prefix only where access is truly rare, or use Autoclass.

::: deeper
**Unit economics.** Absolute spend grows with the business; what you manage is **cost per unit**: per TiB ingested, per pipeline run, per dashboard view, per active customer. A dashboard of unit costs from the billing export (joined with pipeline metadata) shows whether optimisation is working even as volume grows.

**Showback and chargeback.** Labels on projects, resources, BigQuery jobs and reservations let you show each team its spend (showback) or bill it internally (chargeback). Shared costs (a reservation, a network) need an allocation rule, for example by slot-hours used from `INFORMATION_SCHEMA.JOBS`.

**Recommenders and FinOps tools.** The Recommender API covers idle VMs, machine type rightsizing, idle disks and IP addresses, unattended projects, IAM and CUD purchase suggestions; the console's FinOps hub collects these with estimated savings. Export recommendations to BigQuery to track them.

**Commitment risk.** A 3-year commitment saves more but assumes you will still run the same workload in the same region. Platforms in the middle of a migration (for example from Managed Spark to BigQuery) should commit only to the long-term baseline, or prefer spend-based commitments that follow usage across services.

**Storage at scale.** Small objects cost more than their size suggests: Autoclass charges per object, Class A and B operations are charged per request, and listing many small files slows engines. Compaction (for Iceberg, Delta or plain Parquet) lowers both request costs and query time.

> **Interview:** "The data platform's bill doubled in six months. What is your plan?" Use the billing export to break cost down by service, project, SKU and label; compute unit costs to separate growth from waste; fix the top items first (usually BigQuery scans, always-on clusters, cross-region transfer); add guard rails (quotas, budgets, autoscaling caps, lifecycle rules, ephemeral clusters); then buy commitments for the stable baseline, and review monthly.
:::

::: check
?? A team moves 50 TiB of lake files to Coldline and keeps compacting them weekly. Why might the bill go up?
Coldline has a 90-day minimum storage duration and retrieval fees. Weekly compaction reads (retrieval fees) and rewrites files, deleting the old ones early (early-deletion charges), which can exceed the storage savings.

?? What is the difference between a resource-based and a spend-based committed use discount?
Resource-based CUDs commit to specific vCPU and memory amounts in a region; spend-based (flexible) CUDs commit to an hourly spend that applies across machine families and services.

?? Why are ephemeral Managed Spark clusters usually cheaper than a long-running shared cluster?
You pay only while jobs run, each job gets a right-sized cluster, and nothing sits idle overnight; job isolation also removes contention.
:::

::: resources
- Docs: [Cost optimization pillar of the Google Cloud Well-Architected Framework](https://docs.cloud.google.com/architecture/framework/cost-optimization) · principles and recommendations across compute, storage, network and data
- Docs: [Estimate and control BigQuery costs](https://docs.cloud.google.com/bigquery/docs/best-practices-costs) · the warehouse levers, usually the biggest data cost
- Docs: [Cloud Storage classes](https://docs.cloud.google.com/storage/docs/storage-classes) · prices, minimum durations and retrieval fees by class
- Docs: [Autoclass](https://docs.cloud.google.com/storage/docs/autoclass) · automatic per-object class transitions
- Docs: [Committed use discounts](https://docs.cloud.google.com/docs/cuds) · resource-based and spend-based commitments
- Book: [Cloud FinOps, 2nd edition](https://www.oreilly.com/library/view/cloud-finops-2nd/9781492098348/) · J.R. Storment and Mike Fuller on the inform, optimise, operate loop
- Blog: [Best practices for cost management on Databricks](https://www.databricks.com/blog/best-practices-cost-management-databricks) · cluster policies, auto-termination and job clusters
:::

### Terraform on Google Cloud for data platforms

::: plain
Clicking through the console to build a platform is like furnishing a house from memory: it works once, but nobody can rebuild it exactly, and nobody knows who moved the sofa. **Terraform** is a written floor plan. You describe every dataset, topic, bucket and permission in text files, a tool compares the plan with what exists and makes only the needed changes, and the files live in Git where every change is reviewed.
:::

**Terraform** (and its open-source fork **OpenTofu**, which uses the same language) is an infrastructure-as-code tool. You write **resources** in HCL, the tool builds a dependency graph, `terraform plan` shows the difference between the code and reality, and `terraform apply` makes the changes through the Google Cloud APIs. The **google** provider (and **google-beta** for preview features) covers almost every Google Cloud resource.

A minimal data platform module:

```hcl
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"          # pin a major version; check the registry for the current one
    }
  }
  backend "gcs" {
    bucket = "acme-tfstate-prod"  # versioned bucket, access limited to the CI service account
    prefix = "data-platform"
  }
}

provider "google" {
  project = var.project_id
  region  = "europe-west3"
  default_labels = { team = "data", env = "prod", managed_by = "terraform" }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = "europe-west3"
  delete_contents_on_destroy = false
  default_partition_expiration_ms = 400 * 24 * 60 * 60 * 1000   # 400 days
}

resource "google_pubsub_topic" "orders" {
  name                       = "orders"
  message_retention_duration = "86400s"
}

resource "google_pubsub_topic" "orders_dead_letter" {
  name = "orders-dead-letter"
}

resource "google_pubsub_subscription" "orders_to_bq" {
  name  = "orders-to-bq"
  topic = google_pubsub_topic.orders.id

  bigquery_config {
    table          = "${var.project_id}.raw.orders"
    use_table_schema = true
  }
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.orders_dead_letter.id
    max_delivery_attempts = 10
  }
}

resource "google_service_account" "orders_df" {
  account_id   = "orders-df"
  display_name = "Dataflow worker for the orders pipeline"
}

resource "google_bigquery_dataset_iam_member" "orders_df_writes_raw" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.orders_df.email}"
}
```

References such as `google_pubsub_topic.orders.id` create dependencies, so Terraform creates the topic before the subscription. Variables (`var.project_id`) and **modules** (folders of resources with inputs and outputs) let one definition serve dev and prod.

**State.** Terraform records what it manages in a **state file**. On Google Cloud keep it in a **Cloud Storage backend**: the backend locks the state during a run so two applies cannot collide, and object versioning on the bucket lets you recover an earlier state. State can contain secrets (generated passwords, keys), so restrict the bucket to the CI service account and a few administrators.

**IAM resources: the trap.** The provider offers three styles for each resource type, and mixing them causes fights:

| Resource | Behaviour | Use when |
|---|---|---|
| `google_..._iam_member` | Adds one member to one role; leaves others alone | Default choice; safe alongside grants made elsewhere |
| `google_..._iam_binding` | Owns the full member list for one role; removes anyone else | One team owns that role on that resource completely |
| `google_..._iam_policy` | Owns the whole policy; replaces every binding | Rarely; it can remove service agent roles and lock people out |

**Modules and blueprints.** Google maintains open-source modules (**terraform-google-modules** on GitHub: project factory, network, BigQuery, Pub/Sub, Managed Airflow and more) and the **Cloud Foundation Fabric** repository of opinionated modules and blueprints. Start with them for projects and networks, and write thin modules of your own for data products (a dataset, its service accounts and grants, its topics).

**Workflow.** Store code in Git; on a pull request, CI runs `terraform fmt -check`, `terraform validate` and `terraform plan` and posts the plan for review; after merge, CI runs `terraform apply` with a deployment service account obtained through workload identity federation, never with a downloaded key. Google's **Infrastructure Manager** is a managed service that runs Terraform deployments for you, if you prefer not to operate the pipeline.

```bash
terraform init
terraform plan -var="project_id=acme-data-dwh-prod" -out=tfplan
terraform apply tfplan
```

**What not to manage with Terraform.** Tables created and evolved by pipelines (dbt, Dataform, Spark), data itself, and frequently changing pipeline code are better owned by those tools. Terraform owns the long-lived containers: projects, networks, buckets, datasets, topics, service accounts, IAM, Managed Airflow environments, reservations and alerting policies.

> **Pitfall:** Managing a BigQuery table's schema in Terraform while a pipeline also changes it. Each plan shows drift and may try to recreate the table. Let one tool own each object. If Terraform does own a table, keep `deletion_protection = true` (the provider's default for tables) so a replacement cannot silently drop data.

> **Pitfall:** Changing an attribute that forces replacement. Some changes (a dataset's location, a bucket's name, certain subscription settings) destroy and recreate the resource. Read the plan: `-/+ destroy and then create replacement` on a data resource is a red flag.

::: deeper
**Import and drift.** Resources created by hand can be adopted with `import` blocks (`import { to = google_storage_bucket.landing id = "acme-landing-prod" }`) followed by a plan that should show no changes. Scheduled `terraform plan` runs in CI detect drift (changes made outside Terraform) and alert on it.

**Protecting data resources.** Use `lifecycle { prevent_destroy = true }` on buckets and datasets holding data, `deletion_protection` where the provider offers it, and IAM deny policies so even the Terraform service account cannot delete production datasets without a break-glass process.

**Provider authentication.** Locally, the provider uses Application Default Credentials (`gcloud auth application-default login`); the `impersonate_service_account` provider argument lets engineers plan as the deployment service account without holding its key. In CI, workload identity federation from GitHub Actions, GitLab or Cloud Build gives short-lived tokens.

**Scaling Terraform.** Split state by lifecycle and blast radius: organization and folders, shared networking, each environment's platform, each data domain. Large monolithic states make every plan slow and every mistake broad. Pass values between states with outputs and `terraform_remote_state`, or better, with naming conventions and data sources.

**Alternatives.** OpenTofu is a drop-in open-source fork with the same provider. Config Connector manages Google Cloud resources as Kubernetes objects for teams that prefer GitOps on GKE. Pulumi writes the same resources in Python or TypeScript. The provider underneath is generated from the same Google API definitions (Magic Modules), so resource coverage is similar. The DataOps section later covers infrastructure as code tool-independently.

> **Interview:** "How do you manage IAM for BigQuery datasets in Terraform without breaking grants made by other tools?" Use the additive `google_bigquery_dataset_iam_member` resources rather than authoritative bindings or policies, grant to groups and service accounts, keep each grant in the module of the data product that needs it, and review plans for removals.
:::

::: check
?? Why keep Terraform state in a Cloud Storage bucket rather than on a laptop?
Shared state lets a team and CI work on the same infrastructure, the backend locks state during runs to prevent concurrent changes, versioning allows recovery, and access can be restricted because state may contain secrets.

?? What is the difference between `google_bigquery_dataset_iam_member` and `google_bigquery_dataset_iam_binding`?
`_iam_member` adds one principal to a role and leaves other members alone; `_iam_binding` makes Terraform the owner of the whole member list for that role and removes anyone not listed.

?? Which objects in a data platform should usually not be managed by Terraform?
Objects that pipelines create and evolve (tables built by dbt, Dataform or Spark), the data itself, and frequently changing pipeline code.
:::

::: resources
- Docs: [Terraform on Google Cloud](https://docs.cloud.google.com/docs/terraform) · Google's guides, best practices and samples for Terraform
- Docs: [Best practices for using Terraform](https://docs.cloud.google.com/docs/terraform/best-practices/general-style-structure) · module structure, state, naming and CI from Google
- Docs: [Google provider documentation](https://registry.terraform.io/providers/hashicorp/google/latest/docs) · every resource and argument, including the IAM resource styles
- Repo: [terraform-google-modules](https://github.com/terraform-google-modules) · Google-maintained modules for projects, networks, BigQuery, Pub/Sub and more
- Repo: [Cloud Foundation Fabric](https://github.com/GoogleCloudPlatform/cloud-foundation-fabric) · opinionated modules and blueprints for landing zones and data platforms
- Book: [Terraform: Up and Running, 3rd edition](https://www.oreilly.com/library/view/terraform-up-and/9781098116736/) · Yevgeniy Brikman on state, modules, testing and team workflows
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, setting or rule of thumb |
|---|---|
| Platform layout | Folders per environment; projects per environment and purpose (landing, warehouse, pipelines, BI) |
| Cost attribution | Labels on resources and BigQuery jobs; billing export to BigQuery from day one |
| Budgets | Notify only; combine with quotas, caps and automation |
| Least privilege | Predefined roles at dataset, bucket or topic level, granted to groups |
| Pipeline identity | One service account per pipeline, no keys; workload identity federation for CI |
| Hard "never" rules | IAM deny policies and organization policies |
| Private workers | Private Google Access on the subnet; `--no_use_public_ips` (Dataflow), `--no-address` (Spark) |
| Stop exfiltration | VPC Service Controls perimeter, started in dry-run mode |
| Customer-held keys | CMEK in Cloud KMS, key in the same location as the data; or Autokey |
| Pipeline secrets | Secret Manager plus `roles/secretmanager.secretAccessor` on one secret |
| Data in use | Confidential VMs (Dataflow, Managed Spark, GKE), Confidential Space for multi-party work |
| Streaming alert | `subscription/oldest_unacked_message_age` and Dataflow `job/data_watermark_age` |
| Keep logs for analysis | Log sink to BigQuery or a central log bucket; exclusion filters for noise |
| Cheap batch compute | Spot secondaries with enhanced flexibility mode; Dataflow FlexRS; short tasks |
| Avoid transfer charges | Compute in the same region as storage; reduce before moving; share instead of copy |
| Storage tiering | Lifecycle rules or Autoclass; cold classes only for rarely read data |
| Steady baseline | Committed use discounts; peaks on on-demand or Spot |
| Infrastructure as code | Terraform with a GCS backend; `_iam_member` resources; plan in CI, apply after review |

### Practice

::: try
**1. Lay out projects.** A company has a data platform team, a marketing analytics team and a finance team. It needs dev and prod, a landing zone for files and events, a BigQuery warehouse, Dataflow and Spark pipelines, and Looker. Sketch the folders and projects, and say where IAM grants, the billing export and BigQuery reservations live.
:::

?? A solution to 1
Folders `data-prod`, `data-nonprod` and `sandboxes`. In each environment folder: `landing` (buckets, Pub/Sub, Datastream), `dwh` (BigQuery datasets per domain: raw, staging, marketing marts, finance marts), `pipelines` (Dataflow, Managed Spark, Managed Airflow and their service accounts) and `bi` (Looker connection project with its own quota or reservation). An `admin` project holds the billing export dataset, budgets and BigQuery reservations; a `kms` project holds keys. IAM: groups per team at dataset level (marketing analysts read marketing marts, finance analysts read finance marts), `jobUser` in the BI and analyst compute projects, pipeline service accounts with dataset-level editor roles. Organization policies for locations and domain-restricted sharing on the organization; labels `team` and `env` everywhere.

::: try
**2. Fix a permission error.** A Dataflow job launched by `ana@acme.com` with service account `orders-df@...` fails writing to dataset `raw` in another project. The job runs with `--no_use_public_ips`. List three things you would check, in order.
:::

?? Answer to 2
First, that the worker service account `orders-df` (not Ana) has `roles/bigquery.dataEditor` on `raw`, using the Policy Troubleshooter, and that no deny policy blocks it. Second, that both projects are inside the same VPC Service Controls perimeter (or an ingress or egress rule allows the call), by searching audit logs for a VPC SC violation. Third, that Private Google Access is enabled on the workers' subnet, since workers without public IPs need it to reach the BigQuery API.

::: try
**3. Spot or not?** For each workload, say whether to use Spot VMs and how: (a) a nightly 4-hour Spark job that writes Parquet; (b) a Managed Kafka cluster; (c) a one-off backfill of 3 years of events in Dataflow batch; (d) a 30-hour single-process simulation without checkpoints.
:::

?? Answer to 3
(a) Yes: standard primary workers plus Spot secondaries, enhanced flexibility mode, short tasks. (b) No: brokers are stateful and need stable capacity (and Managed Kafka does not offer Spot anyway). (c) Yes, through FlexRS (`--flexrs_goal=COST_OPTIMIZED`) if a delayed start is acceptable. (d) No, unless you first add checkpoints: every preemption would lose up to 30 hours of work.

::: try
**4. Find the egress.** Write a SQL query against the billing export that lists last month's data transfer cost by project and SKU, largest first. Hint: SKU descriptions for transfer usually contain "Data Transfer" or "Egress".
:::

?? A solution to 4

```sql
SELECT
  project.id      AS project_id,
  service.description AS service,
  sku.description AS sku,
  ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)), 2) AS net_cost
FROM `acme-billing-admin.billing.gcp_billing_export_v1_0X0X0X_0X0X0X_0X0X0X`
WHERE invoice.month = '202610'
  AND (REGEXP_CONTAINS(sku.description, r'(?i)data transfer|egress|network'))
GROUP BY project_id, service, sku
ORDER BY net_cost DESC
LIMIT 50;
```

### What comes next

You now have the foundations that every Google Cloud data service relies on: how resources, identities, networks, keys, monitoring and money fit together. The next section, **BigQuery in depth**, puts them to work on the centre of the platform: how BigQuery stores and executes data, how to design and load tables, how to pay for compute, and how to tune, extend and secure it.
