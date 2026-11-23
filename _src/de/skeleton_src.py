# Curriculum skeleton. "@Title" = original topic (exact), "@Title => New title" = original renamed,
# "+New title | what it covers" = new topic. Groups: (group title, [topics]).
# link=(guide, section ids) marks overlap topics: shorter article + link to the other guide.
S = []
def sec(part, sid, title, d, groups, link=None): S.append(dict(part=part, id=sid, t=title, d=d, g=groups, link=link or {}))

P1 = ("Foundations", "spark", "What data engineering is, the tools of the trade, SQL and programming, and how to model data.")
sec(P1, "foundations", "Data engineering foundations", "The vocabulary, the data lifecycle and the moving parts of a modern data platform, explained from zero.", [
 ("The field", ["+What data engineering is | the role, how it differs from analytics and ML engineering, what a data platform does",
                "+The data lifecycle | generation, ingestion, storage, transformation, serving, and the undercurrents (security, ops, orchestration)",
                "+OLTP vs OLAP | transactional vs analytical workloads, row vs column, why warehouses exist",
                "+Batch vs streaming | latency, freshness, cost and complexity trade-offs, when each fits",
                "+The modern data stack | ingestion, warehouse or lakehouse, transformation, orchestration, BI, reverse ETL, and how they connect on GCP"]),
 ("Working basics", ["+Data formats for beginners | CSV, JSON, JSON Lines, Parquet, Avro: structure, pros and cons, when to use each",
                     "+Linux, shell and networking basics for data engineers | files, pipes, permissions, ssh, curl, ports, DNS",
                     "+Git and code review for data work | branches, pull requests, reviewing SQL and pipeline code, monorepos",
                     "+Data engineering roadmap and career | skills by level, portfolio projects, how this course maps to the job"]),
])
sec(P1, "sql", "SQL for data engineering", "From joins and window functions to reading execution plans and writing SQL that scales.", [
 ("SQL foundations", ["+SQL refresher: SELECT, filtering, joins and GROUP BY | the core of SQL from zero, with a small sample schema used through the section", "+Window functions in depth | PARTITION BY, ORDER BY, frames, ranking, running totals, gaps and islands", "+CTEs, subqueries and recursive queries | readable multi-step SQL, correlated subqueries, recursive hierarchies", "+Dates, time zones and timestamps in SQL | DATE vs TIMESTAMP vs DATETIME, UTC, truncation, calendars, common bugs", "+Semi-structured data in SQL: JSON, arrays and structs | UNNEST, nested and repeated fields, JSON functions in BigQuery"]),
 ("Writing SQL", ["@Advanced SQL for Analytics", "@Advanced SQL Modeling Patterns", "@Jinja & Templating in Data"]),
 ("Making SQL fast and correct", ["@SQL Query Optimization & Execution Plans", "@SQL Anti-Patterns & Debugging"]),
])
sec(P1, "programming", "Programming for data engineering", "Python and the other languages data engineers use, and how to structure pipeline code.", [
 ("Python", ["@Python for Data Engineering", "@Concurrency & Parallelism in Python for DE", "@Python Type System & Protocols for DE", "@Data Engineering Design Patterns in Code"]),
 ("DataFrames beyond pandas", ["@Polars — High-Performance DataFrames", "@Ibis — Portable DataFrame API"]),
 ("Other languages", ["@Scala for Spark — Deep Dive", "@Rust for Data Engineering", "@Go for Data Engineering"]),
], link={"python": ["Python for Data Engineering", "Concurrency & Parallelism in Python for DE", "Python Type System & Protocols for DE"]})
sec(P1, "modeling", "Data modeling", "Conceptual, logical and physical models, dimensional modeling, Data Vault and modeling for lakehouses and events.", [
 ("Modeling fundamentals", ["@Modeling Fundamentals & Design Process", "@Conceptual Data Modeling", "@Logical Data Modeling & Normalization", "@Physical Data Modeling for OLTP Systems"]),
 ("Analytical modeling", ["@Dimensional Modeling (Kimball)", "@Dimensional Modeling => Star, snowflake and galaxy schemas in practice", "@Slowly Changing Dimensions (SCD)", "@OBT Pattern & Denormalization", "@Data Vault 2.0 Modeling", "@Anchor Modeling & Temporal Patterns", "@Temporal Tables & Bi-Temporal Data Modeling"]),
 ("Specialised models", ["@Semantic Layer & Metrics Modeling", "@Master Data Modeling (MDM)", "@Reference Data & Taxonomy Modeling", "@Event & Message Schema Modeling", "@Lakehouse Table Modeling (Iceberg/Delta/Hudi)", "@Graph & Network Data Modeling", "@Industry Data Models & Standards", "@Data Contract Modeling"]),
 ("Modeling in practice", ["@Schema Evolution, Versioning & Migration", "@Model Quality, Testing & Observability", "@Documentation-First Modeling with dbt", "@Warehouse Modeling Playbooks (BigQuery, Snowflake, Redshift) => Warehouse Modeling Playbooks (BigQuery, Snowflake)", "@Metadata, Catalogs & Lineage Modeling", "@Modeling Tools & Diagramming Workflows", "@End-to-End Modeling Project (Portfolio)", "@Data Modeling Interview Case Studies"]),
])

P2 = ("Storage, warehouses and lakehouses", "stream", "Where analytical data lives: warehouses, file and table formats, lakehouses, databases and the hardware underneath.")
sec(P2, "warehousing", "Data warehousing", "How analytical warehouses are built and tuned, from MPP and columnar storage to Snowflake, ClickHouse and DuckDB.", [
 ("Concepts and architecture", ["@Data Warehouse Concepts & History", "@Data Warehouse Layers & Zones", "@MPP Architecture", "@Columnar Storage Internals", "@HTAP — Hybrid Transactional/Analytical"]),
 ("Engines", ["@Snowflake — Architecture & Internals", "@ClickHouse — Architecture & Use Cases", "@DuckDB — Embedded OLAP", "@MotherDuck — DuckDB in the Cloud"]),
 ("Performance", ["@Indexing in Analytical Systems", "@Partitioning & Clustering Strategies", "@Materialized Views & Pre-Aggregations", "@Query Optimization in DWH", "@Semantic Layer & Metrics Layer", "@OLAP Operations & Multi-Dimensional Analysis"]),
 ("Running a warehouse", ["@Data Warehouse Testing & TDD", "@Data Warehouse Automation", "@Multi-Tenant Data Warehousing"]),
])
sec(P2, "lakehouse", "Data lakes and lakehouses", "File formats, open table formats, catalogs and the lakehouse architecture, with every major technology.", [
 ("Architecture and storage", ["@Lakehouse Architecture", "@Object Storage — S3, GCS, ADLS => Object storage for data lakes (Google Cloud Storage)", "@Data Lake Governance & Cataloging", "@Open Lakehouse & Catalog Federation"]),
 ("File formats", ["@Apache Parquet — Format Internals", "@Apache ORC — Format Internals", "@Apache Avro — Format Internals", "@Apache Arrow — In-Memory Format", "+Lance and AI-native columnar formats | Lance, Vortex and Nimble: random access, versioning and vectors for ML data"]),
 ("Open table formats", ["@Apache Iceberg — Internals & Operations", "@Delta Lake — Internals & Operations", "@Apache Hudi — Internals & Operations", "@Apache Hudi 1.0 — Next Gen Features", "@Apache Paimon — Streaming Lakehouse Format", "+DuckLake and catalog-backed table formats | DuckLake's SQL-database metadata design versus file-based metadata", "@Table Format Comparison", "@OneTable / XTable — Cross-Format Interop"]),
 ("Operating a lakehouse", ["@Small Files Problem & Compaction", "@Data Skipping, Z-Ordering & Liquid Clustering", "@Time Travel & Snapshot Management", "@Lakehouse Query Performance Tuning", "@Streaming Lakehouse Architecture", "@Lakehouse for Unstructured Data"]),
])
sec(P2, "databases", "Databases: OLTP, NoSQL and specialised", "How operational databases store, index and replicate data, and which one fits which job.", [
 ("Relational internals", ["@Relational DB Internals (PostgreSQL)", "@Storage Engines — B-Tree vs. LSM", "@MVCC — Multi-Version Concurrency Control", "@RocksDB — LSM-Tree Deep Dive"]),
 ("Distributed and NoSQL", ["@NewSQL — TiDB, CockroachDB, Spanner", "@Apache Cassandra — Internals", "@Database Sharding Strategies", "@Elasticsearch — Internals & Search", "@Time-Series Databases — InfluxDB, TimescaleDB"]),
 ("Caching and operations", ["@Redis — In-Memory Data Structures & Internals", "@Distributed Caching Patterns", "@Connection Pooling & Query Routing", "@Zero-Downtime Database Migrations"]),
])
sec(P2, "hardware", "Storage internals and hardware", "Disks, memory, the OS, compression and encodings: the physics every data system is built on.", [
 ("Hardware", ["@SSD vs HDD — I/O Characteristics for DE", "@Memory Hierarchy & Cache Effects", "@NUMA Architecture & Memory Locality", "@GPU-Accelerated Data Processing", "@GPU-Accelerated ETL — RAPIDS & cuDF"]),
 ("Operating system", ["@OS-Level I/O — mmap, sendfile, io_uring", "@io_uring — High-Performance Async I/O", "@File System Internals for Data Engineers"]),
 ("Bytes on disk and on the wire", ["@Compression Algorithms Deep Dive", "@Data Encoding Techniques", "@Serialization Formats — Thrift, Protobuf, FlatBuffers", "@Bloom Filters & Probabilistic Structures", "@Data Encryption Internals"]),
 ("Performance work", ["@JVM Tuning for Data Systems", "@Performance Profiling for Data Systems"]),
])

P3 = ("Processing engines", "kafka", "Hadoop, Spark, Flink, Beam, Trino, query-engine internals and real-time OLAP.")
sec(P3, "hadoop", "The Hadoop ecosystem", "HDFS, YARN, MapReduce, Hive, HBase and the projects that grew around them, and what still matters today.", [
 ("Core Hadoop", ["@HDFS Architecture & Internals", "@HDFS — Architecture & Internals => HDFS operations: replication, balancing and failure handling", "@Hadoop Federation & Namespace", "@YARN Resource Management", "@MapReduce Programming Model", "@Hadoop Security — Kerberos, Ranger, Knox"]),
 ("SQL and storage on Hadoop", ["@Apache Hive & HiveQL Internals", "@Apache Tez — DAG Execution Engine", "@Apache Impala — MPP SQL on Hadoop", "@Apache HBase — Wide-Column Store", "@Apache Kylin — OLAP on Hadoop"]),
 ("Beyond classic Hadoop", ["@Alluxio — Distributed Caching Layer", "@Apache Ozone — Hadoop Object Storage", "@Apache Kyuubi — JDBC/SQL Gateway", "@Hadoop Ecosystem in 2024 — What's Still Relevant => The Hadoop ecosystem today: what is still relevant"]),
])
sec(P3, "spark", "Apache Spark", "Spark's architecture, optimizer, memory, shuffle and tuning, with links into the PySpark guide for full depth.", [
 ("Architecture", ["@Spark Architecture — Driver, Executors, Cluster Managers", "@RDD, DataFrame & Dataset API Internals", "@Spark Connect — Client-Server Architecture", "@Spark on Kubernetes"]),
 ("Optimizer and execution", ["@Catalyst Optimizer — Rule-Based & Cost-Based", "@Tungsten Execution Engine & WSCG", "@Adaptive Query Execution (AQE)", "@Shuffle Internals — Sort-Based Shuffle", "@Distributed Join Algorithms"]),
 ("Performance and operations", ["@Spark Memory Management", "@Data Skew Handling", "@JVM GC Tuning for Spark", "@Spark UI & Performance Debugging", "@PySpark Internals & Arrow Optimization", "@Spark Testing & CI Pipelines", "@Databricks — Photon Engine & Delta Engine"]),
 ("Streaming", ["@Spark Structured Streaming — Internals"]),
], link={"pyspark": "*"})
sec(P3, "engines", "Flink, Beam, Trino and other engines", "The other engines data engineers meet: Flink for streams, Beam for portability, Trino for federation.", [
 ("Apache Flink", ["@Apache Flink — Architecture & Internals", "@Flink State Management", "@Flink CDC & Flink SQL Ecosystem"]),
 ("Apache Beam", ["@Apache Beam — Unified Batch & Streaming", "@Watermarks & Late Data Handling"]),
 ("SQL engines", ["@Apache Trino / Presto — Query Federation", "@Federated Query Execution"]),
])
sec(P3, "queryengines", "Query engine internals", "How SQL becomes a fast plan: parsing, optimization, execution models, vectorization and code generation.", [
 ("Planning", ["@Apache Calcite — Query Planning Framework", "@Cost-Based Optimization (CBO)", "@Substrait — Cross-Engine Query Plans"]),
 ("Execution", ["@Query Execution Models — Volcano vs Push", "@Vectorized Execution & SIMD", "@SIMD Vectorization for Analytics", "@Runtime Code Generation — LLVM JIT"]),
 ("Modern engines", ["@Velox — Meta's Unified Execution Engine", "@Apache DataFusion — Rust Query Engine", "@Apache Comet & Gluten — Native Spark Acceleration", "@WASM for Portable Data Compute"]),
])
sec(P3, "rtolap", "Real-time OLAP engines", "Druid, Pinot, StarRocks and Doris: sub-second analytics on fresh data.", [
 ("Concepts", ["@Real-Time OLAP Concepts & Landscape", "@RTOLAP Indexing Deep Dive", "@Rollup, Pre-Aggregation & Cube Strategies", "@Upsert & Deduplication in RTOLAP"]),
 ("Engines", ["@Apache Druid — Architecture & Internals", "@Apache Druid — Indexing & Storage Internals", "@Apache Pinot — Architecture & Internals", "@Apache Pinot — Advanced Features", "@StarRocks / Doris — MPP Real-Time OLAP"]),
 ("Production", ["@RTOLAP in Production — Operations & Tuning"]),
])

P4 = ("Streaming, events and ingestion", "stream", "Kafka and other logs, stream processing concepts, event-driven patterns and getting data in.")
sec(P4, "streaming", "Streaming systems", "Logs, brokers and stream processing guarantees, with links into the PySpark and Kafka guide for full depth.", [
 ("Concepts", ["@Event Time vs. Processing Time", "@Window Functions in Streaming", "@Stream Processing Guarantees", "@Exactly-Once Semantics Internals", "@Backpressure & Flow Control", "@Lambda & Kappa Architectures"]),
 ("Apache Kafka", ["@Apache Kafka — Architecture & Internals", "@Kafka Producers & Consumers — Deep Dive", "@Kafka KRaft — ZooKeeper-Free Kafka", "@Kafka Tiered Storage", "@MirrorMaker 2 — Cross-Cluster Replication", "@Kafka Streams & ksqlDB", "@Schema Registry & Schema Evolution"]),
 ("Other brokers and streaming SQL", ["@Apache Pulsar", "@Redpanda — Kafka-Compatible Streaming", "@Streaming SQL — Flink SQL, Materialize, RisingWave"]),
], link={"pyspark": ["Apache Kafka — Architecture & Internals", "Kafka Producers & Consumers — Deep Dive", "Kafka KRaft — ZooKeeper-Free Kafka", "Kafka Tiered Storage", "Kafka Streams & ksqlDB", "Exactly-Once Semantics Internals", "Event Time vs. Processing Time", "Window Functions in Streaming"]})
sec(P4, "events", "Event-driven architecture", "Event sourcing, CQRS, outbox, sagas and schema design for systems that talk in events.", [
 ("Patterns", ["@Event Sourcing Pattern", "@CQRS Pattern", "@Transactional Outbox Pattern", "@Saga Pattern & Compensating Transactions", "@Microservice Data Patterns"]),
 ("Designing events", ["@Event Schema Design & CloudEvents", "@AsyncAPI & Event Catalog", "@Event Storming for Data Systems", "@API Design for Data Products"]),
 ("Events and data mesh", ["@Data Mesh Event Patterns", "@Kafka as Event Backbone for Data Mesh"]),
])
sec(P4, "ingestion", "Data ingestion", "Getting data in: batch, incremental and CDC, APIs, files, logs, IoT and clickstreams.", [
 ("Strategies", ["@Bulk vs Incremental vs CDC Ingestion", "@Schema-on-Read vs Schema-on-Write", "@Change Data Capture (CDC)", "@Debezium — CDC Deep Dive", "@Apache Kafka Connect — Internals", "@Database Replication to Lake — Patterns"]),
 ("Sources", ["@SaaS API Ingestion at Scale", "@REST APIs & GraphQL for Ingestion", "@Webhook Ingestion Systems", "@File-Based Ingestion — SFTP, EDI, Flat Files", "@Log Ingestion Pipelines", "@Clickstream & Event Tracking Pipelines", "@IoT Data Ingestion Architectures", "@Financial Data Ingestion (Market Data)"]),
 ("At scale", ["@Ingestion at Scale — Throughput Engineering", "@Streaming Ingestion to Lakehouse", "@Data Profiling & Anomaly Detection at Ingestion", "@Zero-ETL Patterns"]),
])

P5 = ("Pipelines and analytics engineering", "prep", "Transforming and orchestrating data reliably, and turning it into metrics and products.")
sec(P5, "pipelines", "Pipelines and transformation", "ETL and ELT, dbt, data quality, contracts and the design patterns that make pipelines reliable.", [
 ("ETL and ELT", ["@ETL vs ELT Architecture Trade-offs", "@Batch Processing Patterns", "@Parallel & Micro-Batch Processing Patterns", "@Pipeline Design Patterns", "@Pipeline Idempotency & Exactly-Once"]),
 ("dbt", ["@dbt — Core to Advanced", "@dbt — Advanced Patterns"]),
 ("Connectors", ["@Fivetran & Airbyte — Managed Connectors", "@Meltano & Singer Ecosystem", "@Apache NiFi"]),
 ("Reliability", ["@Data Quality & Validation", "@Data Contracts", "@Schema Evolution & Compatibility", "@Error Handling & Dead Letter Queues", "@Backfill Strategies & Historical Reprocessing", "@Pipeline Observability & Lineage"]),
])
sec(P5, "orchestration", "Orchestration", "Airflow, Dagster and Prefect: scheduling, dependencies and running pipelines in production.", [
 ("Concepts", ["@DAG Design & Dependency Management", "+Scheduling, sensors and event-driven triggers | cron vs data-aware scheduling, sensors, deferrable operators, datasets/assets"]),
 ("Tools", ["@Apache Airflow — Internals & Production", "@Apache Airflow — Production Operations", "@Dagster — Asset-Based Orchestration", "@Prefect — Modern Workflow Orchestration", "+Choosing an orchestrator | Airflow vs Dagster vs Prefect vs Cloud Composer vs Workflows: a decision guide"]),
])
sec(P5, "analytics", "Analytics engineering and BI", "From models to metrics: analytics engineering, BI internals, experimentation and data activation.", [
 ("Analytics engineering", ["@Analytics Engineering Practices", "@Self-Serve Analytics Architecture", "@OLAP Cube Design & Modern Alternatives"]),
 ("BI and analysis", ["@BI Tool Internals", "@Funnel, Cohort & Retention Analysis", "@Product Analytics Stacks", "@A/B Testing Data Infrastructure", "@Embedded Analytics & Multi-Tenancy", "@Data Journalism & Visualization at Scale"]),
 ("Activation", ["@Reverse ETL", "@Reverse ETL & Data Activation => Data activation: syncing models to business tools"]),
])

P6 = ("Google Cloud data platform", "kafka", "The GCP services a data engineer uses every day, from BigQuery and Dataflow to governance, security and cost.")
sec(P6, "gcpfound", "Google Cloud foundations for data", "How Google Cloud is organised and secured, and the platform choices that sit under every data service.", [
 ("Platform basics", ["@GCP Data Ecosystem — Deep Dive => The Google Cloud data ecosystem: a map", "+Resource hierarchy, projects and billing | organisation, folders, projects, labels, quotas and billing export for data teams", "@Compute-Storage Separation", "@Cloud IAM & Security for Data => IAM for data on Google Cloud"]),
 ("Networking and security", ["+VPC, Private Service Connect and VPC Service Controls | keeping data services private and preventing exfiltration", "+Encryption and keys: CMEK, Cloud KMS and Secret Manager | default encryption, customer-managed keys, key rotation, secrets in pipelines", "@Confidential Computing for Data"]),
 ("Running and paying for it", ["+Cloud Monitoring and Cloud Logging for data pipelines | metrics, log sinks, alerting policies, audit logs", "@Spot / Preemptible Instances for Data => Spot VMs and preemptible capacity for data workloads", "@Data Transfer & Egress Cost Engineering", "@Cloud Cost Optimization Deep Dive", "+Terraform on Google Cloud for data platforms | provider, state, modules for BigQuery, Pub/Sub, Composer and IAM"]),
])
sec(P6, "bigquery", "BigQuery in depth", "BigQuery from first query to internals: storage, compute, pricing, performance, security and its newest features.", [
 ("Architecture", ["@Google BigQuery — Internals & Tuning => BigQuery architecture: Dremel, Colossus, Jupiter and Borg", "+BigQuery storage: Capacitor, table types and storage billing | native tables, logical vs physical bytes, time travel, fail-safe", "+Datasets, tables, views and materialized views in BigQuery | organising data, authorised views, table snapshots and clones"]),
 ("Designing and loading", ["+Partitioning and clustering in BigQuery | time-unit, ingestion-time and integer-range partitions, clustering, pruning", "+Loading and streaming into BigQuery | load jobs, Storage Write API, BigQuery Data Transfer Service, external tables", "+Google Cloud Lakehouse and Apache Iceberg on BigQuery | Lakehouse (formerly BigLake) tables, Apache Iceberg managed tables, the Lakehouse runtime catalog, open formats"]),
 ("Compute, cost and performance", ["+BigQuery compute: slots, editions, reservations and autoscaling | on-demand vs capacity pricing, workload management", "+Reading BigQuery query plans and tuning queries | execution graph, stages, shuffle, common anti-patterns", "+BigQuery cost control | quotas, custom cost controls, INFORMATION_SCHEMA jobs analysis, dry runs"]),
 ("Features", ["+Advanced BigQuery SQL | MERGE, scripting, procedures, UDFs, remote functions, JSON, arrays and structs, search indexes", "+BigQuery ML and Gemini in BigQuery | training and serving models in SQL, generative AI functions, vector search", "+BI Engine, Looker and Connected Sheets | in-memory acceleration, LookML basics, Data Studio (formerly Looker Studio), BI on BigQuery", "+BigQuery security and governance | row-level security, column-level security with policy tags, dynamic masking, data sharing"]),
])
sec(P6, "gcppipes", "Pipelines on Google Cloud", "Pub/Sub, Dataflow, Dataproc, Composer, Datastream and the other services that move and transform data on GCP.", [
 ("Messaging and streaming", ["+Pub/Sub in depth | topics, subscriptions, ordering keys, exactly-once delivery, dead-letter topics, BigQuery and Cloud Storage subscriptions", "@Managed Kafka — Confluent, MSK, Aiven => Managed Kafka on Google Cloud: Google Managed Service for Apache Kafka, Confluent Cloud and Aiven", "+Dataflow in depth | Beam on Dataflow, Streaming Engine, shuffle service, autoscaling, templates, monitoring"]),
 ("Batch and transformation", ["+Managed Service for Apache Spark (formerly Dataproc) and serverless Spark | Spark and Hadoop on GCP, clusters vs serverless batches, autoscaling, Spark with BigQuery", "+Dataform and SQL pipelines on BigQuery | SQLX, dependencies, assertions, compared with dbt", "+Cloud Data Fusion | visual pipelines on CDAP, when a low-code tool fits"]),
 ("Ingestion and orchestration", ["+Datastream: serverless CDC into BigQuery | MySQL, PostgreSQL, Oracle and SQL Server sources, backfill, latency", "@Managed Orchestration — MWAA, Composer, Astro => Managed orchestration: Managed Service for Apache Airflow (formerly Cloud Composer) and Astronomer", "+Workflows, Cloud Scheduler and Eventarc | lightweight orchestration and event routing on GCP", "@Serverless Data Processing => Serverless data processing on GCP: Cloud Run, Cloud Run functions and Batch"]),
])
sec(P6, "gcpdata", "Operational databases, AI and governance on Google Cloud", "Bigtable, Spanner and AlloyDB, Vertex AI for data engineers, Dataplex governance and reference architectures.", [
 ("Databases", ["+Cloud Bigtable in depth | wide-column design, row keys, hotspotting, replication, Bigtable vs BigQuery", "+Cloud Spanner in depth | TrueTime, splits, interleaving, change streams, Spanner Graph", "+AlloyDB and Cloud SQL | managed PostgreSQL and MySQL, columnar engine, read pools, replication to BigQuery"]),
 ("Governance and sharing", ["+Knowledge Catalog (formerly Dataplex Universal Catalog) | discovery, metadata, lineage, data quality and profiling scans on GCP", "+BigQuery sharing (formerly Analytics Hub) and data clean rooms | publishing and subscribing to datasets, privacy-safe collaboration", "@Delta Sharing & Open Data Sharing Standards"]),
 ("AI and architecture", ["+Gemini Enterprise Agent Platform (formerly Vertex AI) for data engineers | feature store, pipelines, datasets, batch prediction from BigQuery", "+Google Cloud reference architectures for data | batch lakehouse, streaming analytics, CDC to BigQuery, ML platform"]),
])

P7 = ("Distributed systems and system design", "prep", "The theory behind every data system and how to design platforms end to end.")
sec(P7, "distsys", "Distributed systems", "Consensus, consistency, clocks and fault tolerance, explained for data engineers.", [
 ("Foundations", ["@Distributed Systems Fundamentals", "@CAP Theorem & PACELC", "@Consistent Hashing", "@Vector Clocks & Logical Clocks"]),
 ("Agreement and coordination", ["@Consensus Algorithms — Raft & Paxos", "@Leader Election & Coordination", "@Apache ZooKeeper & etcd", "@Byzantine Fault Tolerance", "@Gossip Protocols & SWIM"]),
 ("Building reliable systems", ["@Distributed Transactions — 2PC, Saga", "@Fault Tolerance Patterns", "@Rate Limiting & Backpressure at Scale", "@gRPC & Apache Arrow Flight", "@Observability in Distributed Systems", "@SLA/SLO/SLI for Data Systems"]),
])
sec(P7, "design", "Data engineering system design", "Designing data platforms end to end, and how to answer system design interviews.", [
 ("Architectures", ["@Data Platform End-to-End Design", "@Medallion Architecture Deep Dive", "@Data Lakehouse Reference Architecture", "@Data Mesh Architecture", "@Real-Time Analytics System Design", "@Designing Scalable Ingestion Systems", "@Feature Store Architecture"]),
 ("Products and platforms", ["@Data Product Design Patterns", "@Designing for Data Products => Building and operating data products", "@Data Platform Self-Service Design"]),
 ("Non-functional design", ["@Cost Optimization in Data Platforms", "@Capacity Planning & Benchmarking", "@Disaster Recovery & Business Continuity", "@Designing for GDPR & Privacy by Design", "@Multi-Cloud & Hybrid Data Architecture => Hybrid and multi-region data architecture on Google Cloud", "@Data Platform Migration Strategies"]),
 ("Interviews", ["@System Design Interview Patterns for DE"]),
])

P8 = ("Operations and governance", "spark", "Running data platforms reliably and securely: DataOps, SRE, governance and compliance.")
sec(P8, "dataops", "DataOps and platform engineering", "CI/CD, infrastructure as code, testing, containers and SRE practices for data platforms.", [
 ("Delivery", ["@CI/CD for Data Pipelines", "@Infrastructure as Code for Data", "@GitOps for Data Infrastructure", "@Environment Management for Data", "@Blue-Green & Canary Deployments for Pipelines", "@Docker & Kubernetes for Data Engineers"]),
 ("Testing", ["@Data Pipeline Testing Strategies", "@Testing Frameworks for Data Engineering"]),
 ("Reliability", ["@Data Platform SRE — Reliability Engineering", "@Pipeline Monitoring & Alerting", "@Logging Best Practices for Data Systems", "@Pipeline Incident Management", "@Chaos Engineering for Data Systems", "@Data Observability — Monte Carlo, Acceldata, Datafold", "@Secrets Management for Data"]),
 ("Teams and money", ["@FinOps for Data Engineering", "@Data Team Topologies & Engineering Culture", "@Data Engineering Maturity Model"]),
])
sec(P8, "governance", "Data governance, security and compliance", "Catalogs, lineage, access control, privacy and regulation, with the open-source and GCP tools that implement them.", [
 ("Metadata", ["@Data Catalog & Discovery", "@Data Lineage — Column & Table Level", "@DataHub — Metadata Platform Internals", "@Unity Catalog — Databricks Governance", "@Master Data Management (MDM)"]),
 ("Quality", ["@Data Quality Frameworks", "@Data Observability Platforms"]),
 ("Access and security", ["@Data Access Control — RBAC & ABAC", "@Apache Ranger & Fine-Grained Access", "@Audit Logging & Zero Trust Data Architecture", "@Quantum-Resistant Cryptography for Data"]),
 ("Privacy and compliance", ["@PII Detection & Data Masking", "@GDPR, CCPA, HIPAA Compliance", "@Privacy Engineering & Data Clean Rooms", "@Consent Management & Privacy Orchestration", "@Data Retention & Lifecycle Management", "@Data Sharing & Marketplace", "@Data Mesh Governance"]),
])

P9 = ("AI, graphs and industry", "kafka", "Data engineering for machine learning and LLMs, graph data, and patterns from specific industries.")
sec(P9, "aiml", "Data engineering for AI and ML", "Feature pipelines, training data, vectors, RAG and MLOps: the data side of machine learning.", [
 ("Features and training data", ["@Feature Engineering at Scale", "@Real-Time Feature Engineering with Flink", "@Training Data Pipelines & Labeling", "@Petabyte-Scale ML Data Processing", "@Data Versioning — DVC, LakeFS, Delta", "@Synthetic Data Generation", "@Synthetic Data Generation at Scale"]),
 ("LLMs and vectors", ["@LLM Pre-Training Data Pipelines", "@LLM Fine-Tuning Data Pipelines", "@Embedding Pipelines at Scale", "@Vector Databases — Pinecone, Weaviate, pgvector", "@Vector Database Internals & ANN Search", "@RAG Architecture & Vector Pipelines", "@Knowledge Graphs for Data Engineering"]),
 ("MLOps", ["@MLflow & Experiment Tracking", "@MLOps Pipelines — End-to-End", "@Model Serving & Inference Data Flow", "@Model Monitoring & Data Drift Detection", "@Federated Learning for Data Engineering"]),
 ("AI in the data stack", ["@AI-Augmented Data Pipelines", "@Data Engineering with LLMs (Text2SQL, Agents)"]),
])
sec(P9, "graph", "Graph data engineering", "Graph databases, distributed graph processing and the applications that need them.", [
 ("Graph systems", ["@Graph Databases — Neo4j & Property Graph Model", "@Graph Databases — Neo4j Internals", "@TigerGraph & GSQL", "@Distributed Graph Processing — Pregel, GraphX", "@Graph Algorithms at Scale"]),
 ("Applications", ["@Knowledge Graph Construction Pipelines", "@Fraud Detection Data Engineering", "@Social Network Data Engineering", "@Geospatial Data Engineering"]),
])
sec(P9, "industry", "Industry patterns", "How data engineering looks in finance, healthcare, retail, mobility, media, manufacturing, gaming and advertising.", [
 ("Industries", ["@Financial Services Data Engineering", "@Healthcare Data Engineering — FHIR & HL7", "@E-Commerce Data Engineering", "@Ride-Sharing / Mobility Data Engineering", "@Media & Streaming Data Engineering", "@Manufacturing / Industrial IoT Data Engineering", "@Gaming Data Engineering", "@AdTech Data Engineering"]),
])
