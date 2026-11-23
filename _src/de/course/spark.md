This section is a compact tour of Apache Spark, the most widely used engine for processing large datasets on a cluster. It is written for data engineers who will write Spark jobs, size clusters and debug slow or failing applications. It assumes you know SQL and Python (SQL for data engineering, Programming for data engineering), file formats and table formats (Data lakes and lakehouses), and the ideas from The Hadoop ecosystem: splitting work across machines, moving computation to the data, and the cost of a shuffle.

Spark already has a full, deep companion guide on this site, the **PySpark + Kafka guide**. This section does not repeat it. Every topic here is a complete, beginner-friendly article that gives you the idea, a working example and the main pitfalls, then points you to the matching part of the guide for internals, tuning and failure modes. Read this section first to get the map; open the guide when you need the territory.

**What you will be able to do afterwards**

- Describe what the driver, executors and cluster manager do, and follow a job from `spark-submit` to tasks running on executors.
- Explain lazy evaluation, lineage and the difference between narrow and wide dependencies, and say why that difference decides where stages begin.
- Read an `explain()` plan, and explain what Catalyst, Tungsten and Adaptive Query Execution each do to your query.
- Pick a join strategy, recognise a skewed stage, and size executor memory without guessing.
- Use the Spark UI to find the stage, task or setting that makes a job slow.
- Run Spark on Google Cloud with Managed Service for Apache Spark (formerly Dataproc), its serverless mode for Spark batches (formerly Dataproc Serverless), or Kubernetes, and test Spark code in CI.

**The big picture.** Spark turns your program into a plan, cuts the plan into **stages** wherever data must be redistributed between machines (a **shuffle**), and runs each stage as many small **tasks**, one per partition of data, on a pool of worker processes called **executors**. A single coordinating process, the **driver**, holds the plan and hands out the tasks. Almost every performance question in Spark is one of three questions: how much data crosses the network in shuffles, whether the tasks of a stage are evenly sized, and whether each task has enough memory to do its work without spilling to disk or crashing. Keep those three questions in mind and the rest of the section will feel like detail around them.

[[fig:spark_arch]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Architecture

Before tuning anything you need a picture of the moving parts: who plans the work, who does it, who hands out machines, and how your code talks to all of them. This group builds that picture in four steps: the classic architecture (driver, executors, cluster manager), the data abstractions your code uses (RDDs and DataFrames), the newer client-server way of connecting (Spark Connect), and the most common modern place to run it all besides a managed service (Kubernetes).

If you came from the Hadoop section, you will recognise the shape: a coordinator, many workers and a resource manager such as YARN. Spark's difference is that it keeps intermediate data in memory where it can, plans a whole chain of steps at once, and gives you a high-level API that an optimizer can rewrite.

### Spark Architecture — Driver, Executors, Cluster Managers

::: plain
A Spark application works like a building site. The **driver** is the site manager with the plans: it decides what must be built and in what order. The **executors** are teams of workers, each with a few pairs of hands (CPU cores) and a storage shed (memory and local disk). The **cluster manager** is the staffing agency that supplies the teams. The manager never lays bricks; the workers never read the full plans.
:::

Every Spark application has exactly one **driver**: the process that runs your `main` program or notebook and creates a `SparkSession` (which wraps the older `SparkContext`). When your code calls an **action** such as `count()` or `write`, the driver turns the plan into a **job** and passes it through two schedulers:

1. The **DAGScheduler** builds a directed acyclic graph (DAG) of the job and cuts it into **stages** at every shuffle boundary. Inside a stage, data never leaves the machine it is on.
2. The **TaskScheduler** turns each stage into **tasks** (one per partition), sends them to executors and retries failed ones. It prefers executors that already hold the data (data locality).

**Executors** are long-lived JVM processes on worker machines. Each executor has a number of **cores**, and each core runs one task at a time, so an application with 10 executors of 4 cores runs 40 tasks in parallel. Executors also keep cached data and shuffle files, and report back to the driver through heartbeats.

The **cluster manager** grants machines and containers. Spark supports:

| Cluster manager | Where you meet it |
|---|---|
| Standalone | Spark's own simple manager, for dedicated clusters and labs |
| YARN | Hadoop clusters, including Managed Service for Apache Spark clusters on Google Cloud |
| Kubernetes | Containers on GKE or any Kubernetes cluster |
| `local[*]` | Not a manager: driver and executor in one process on your laptop |

Mesos was deprecated in Spark 3.2 and removed in Spark 4.0.

The **deploy mode** says where the driver runs. In **client mode** the driver runs where you typed `spark-submit` (your laptop, a notebook, an edge node), so its logs and output appear right there, but if that machine disconnects the job dies. In **cluster mode** the driver runs inside the cluster, which is what production batch jobs use.

```bash
# A batch job on a Managed Service for Apache Spark cluster (YARN underneath)
gcloud dataproc jobs submit pyspark gs://my-bucket/jobs/daily_sales.py \
  --cluster=etl-cluster --region=europe-west1 \
  --properties=spark.executor.cores=4,spark.executor.memory=12g

# The same job as a serverless batch: no cluster to create or size
gcloud dataproc batches submit pyspark gs://my-bucket/jobs/daily_sales.py \
  --region=europe-west1 --version=2.3
```

> **Pitfall:** Calling `collect()` or `toPandas()` on a large DataFrame pulls every row into the driver's memory. The driver is one process; the cluster is many. Aggregate or `limit()` first, or write the result to storage.

> **Tip:** **Dynamic allocation** (`spark.dynamicAllocation.enabled=true`) lets Spark add executors when tasks queue up and release idle ones. Managed Service for Apache Spark clusters turn it on by default, and serverless batches autoscale for you.

::: deeper
The driver also hosts the **BlockManagerMaster** (which tracks where cached blocks and shuffle outputs live) and the Spark UI on port 4040. If an executor dies, the driver reschedules its running tasks and, when shuffle files are lost with it, re-runs the map tasks that produced them. If the driver dies, the application dies, which is why streaming jobs keep their progress in a checkpoint.

> **Interview:** "What decides the number of stages in a job?" The number of shuffle boundaries. A chain of `filter`, `select` and `withColumn` is one stage; each `groupBy`, non-broadcast `join` or `repartition` adds one.
:::

::: guide
For the scheduler, task lifecycle and deployment details, read [Spark architecture and application lifecycle](pyspark-streaming-kafka-topic-map.html#arch), [Execution model and scheduling](pyspark-streaming-kafka-topic-map.html#sched) and [Deployment and cluster managers](pyspark-streaming-kafka-topic-map.html#deploy) in the PySpark + Kafka guide.
:::

::: check
?? What are the jobs of the DAGScheduler and the TaskScheduler?
The DAGScheduler cuts a job into stages at shuffle boundaries and submits them in dependency order. The TaskScheduler launches one task per partition of each stage on executors, with locality preference and retries.

?? An application has 8 executors with 5 cores each. How many tasks can run at the same time?
40, one per core.

?? Why do production batch jobs usually use cluster deploy mode?
The driver then runs inside the cluster, so the job does not depend on the submitting machine staying connected.
:::

::: resources
- Docs: [Cluster mode overview](https://spark.apache.org/docs/latest/cluster-overview.html) · the official description of driver, executors, cluster managers and deploy modes
- Docs: [Managed Service for Apache Spark documentation](https://docs.cloud.google.com/dataproc/docs) · clusters, jobs and serverless batches on Google Cloud
- Book: [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/) · Damji et al.; chapter 1 and 2 explain the architecture clearly
- Book: [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) · Chambers and Zaharia; part IV covers applications and the life of a job
- Repo: [Spark Internals](https://github.com/JerryLead/SparkInternals) · Lijie Xu's illustrated walk through jobs, stages, shuffle and the block manager
:::

### RDD, DataFrame & Dataset API Internals

::: plain
An **RDD** is a recipe card for a dataset, not the dataset itself. It says "start from these files, keep rows where amount > 0, then add up per customer". Spark writes the recipe down step by step and only cooks when someone asks for the dish. If a cook drops a plate (a machine fails), Spark rereads the recipe and remakes only that plate.
:::

The **RDD** (Resilient Distributed Dataset) is Spark's original abstraction: an immutable collection split into **partitions**, plus the **lineage** that says how each partition is computed from its parents. Operations come in two kinds:

- **Transformations** (`map`, `filter`, `join`, `groupBy`) return a new dataset and do nothing yet. They are **lazy**.
- **Actions** (`count`, `collect`, `write`, `show`) force Spark to run the plan and return or save a result.

Laziness lets Spark see the whole chain before running it, combine steps and skip work that is not needed.

Each transformation has a **dependency** type, and this is the most useful idea in Spark:

- **Narrow**: each output partition depends on one input partition (`map`, `filter`, `select`). Spark pipelines these inside a single task with no network traffic.
- **Wide**: an output partition needs data from many input partitions (`groupBy`, `join` on unaligned data, `distinct`, `repartition`). This needs a **shuffle**, and the DAGScheduler starts a new stage there.

A **DataFrame** is a distributed table with a schema: named, typed columns. You describe *what* you want (columns, filters, aggregations) and Spark's optimizer decides *how*. Under the hood a DataFrame is a logical plan; when it runs, Spark compiles it into RDD operations over rows stored in its compact binary format (`UnsafeRow`). In Scala, a DataFrame is literally `Dataset[Row]`.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("demo").getOrCreate()
orders = spark.read.parquet("gs://my-bucket/orders/")          # transformation: nothing runs

big = (orders
       .filter(F.col("amount") > 100)                           # narrow
       .groupBy("customer_id")                                  # wide: shuffle, new stage
       .agg(F.sum("amount").alias("total")))

big.write.mode("overwrite").parquet("gs://my-bucket/out/")     # action: now the job runs
```

A **Dataset** (Scala and Java only) adds compile-time types: `Dataset[Order]` lets the compiler catch `order.amout` as a typo. An **encoder** converts between your JVM objects and Spark's binary rows. Typed lambdas such as `ds.filter(o => o.amount > 100)` are opaque to the optimizer, so they are often slower than the same filter written with columns. Python has no Dataset API because Python has no compile-time types; in PySpark you use DataFrames.

| API | Typed at compile time | Optimized by Catalyst | Use it for |
|---|---|---|---|
| RDD | Yes (Scala) | No | Low-level control, legacy code |
| DataFrame | No (checked at analysis) | Yes | Almost everything, all languages |
| Dataset | Yes | Partly (not inside lambdas) | Typed Scala or Java pipelines |

> **Pitfall:** Each action re-runs the lineage from the source unless you cache. Calling `df.count()` and then `df.write` on an expensive DataFrame computes it twice. Use `cache()` (and `unpersist()` later) only when you really reuse a result.

> **Tip:** Prefer DataFrame functions in `pyspark.sql.functions` over RDD code and Python UDFs. They run inside the JVM, are optimized, and are usually many times faster.

::: guide
For lineage, partitioning and the DataFrame API in depth, read [RDDs, lineage and the core abstractions](pyspark-streaming-kafka-topic-map.html#rdd) and [DataFrame API essentials](pyspark-streaming-kafka-topic-map.html#df) in the PySpark + Kafka guide.
:::

::: check
?? Why does `df.filter(...).select(...)` run as a single stage while `df.groupBy(...).count()` needs two?
Filter and select are narrow: each output partition comes from one input partition, so they are pipelined in one task. `groupBy` is wide: rows with the same key must be brought together, which needs a shuffle and so a new stage.

?? How does Spark recover a lost partition of an RDD?
It recomputes it from its lineage, re-running only the transformations needed for that partition from the last available input or shuffle output.

?? Why is a typed Scala lambda filter often slower than a column expression?
Catalyst cannot see inside the lambda, so it cannot push the filter down to the file reader or generate optimized code, and each row must be deserialized into an object first.
:::

::: resources
- Docs: [RDD programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html) · transformations, actions, persistence and shuffles from the source
- Docs: [Spark SQL, DataFrames and Datasets guide](https://spark.apache.org/docs/latest/sql-programming-guide.html) · the official introduction to the structured APIs
- Paper: [Resilient Distributed Datasets](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf) · Zaharia et al., NSDI 2012; the paper that introduced RDDs and lineage
- Book: [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) · chapters 2 to 5 cover the structured APIs and their execution
- Blog: [A tale of three Apache Spark APIs](https://www.databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html) · when to use RDDs, DataFrames and Datasets
:::

### Spark Connect — Client-Server Architecture

::: plain
Classic Spark puts your program inside the driver, like a passenger who must sit in the engine room to steer the ship. **Spark Connect** gives you a remote control instead: your program runs anywhere (a laptop, a web app, a notebook), sends the plan over the network to a Spark server, and gets results back. If your program crashes, the ship keeps sailing.
:::

In the classic model your application code and the driver are the same JVM process (PySpark talks to it through a local bridge called Py4J). That coupling causes real problems: a client library must match the cluster's Spark version exactly, a heavy notebook can take down a shared driver with an out-of-memory error, and upgrading the cluster means upgrading every client.

**Spark Connect**, added in Spark 3.4, splits the two. The client builds an **unresolved logical plan** (just a description of what you asked for), encodes it as **Protocol Buffers** and sends it over **gRPC** to a Spark Connect server that runs inside the driver. The server analyses, optimizes and executes it, then streams results back as **Apache Arrow** record batches.

[[fig:spark_connect]]

```bash
# On the cluster side: start a Spark Connect server (default port 15002)
./sbin/start-connect-server.sh

# On the client side: a lightweight, pure-Python client (Spark 4.0+)
pip install pyspark-client
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-host:15002").getOrCreate()
df = spark.read.parquet("gs://my-bucket/orders/")
df.groupBy("country").count().show()     # plan goes to the server; rows come back as Arrow
```

What changes for you:

- **Thin clients.** The `pyspark-client` package has no JVM. Clients exist for Python, Scala, Go, Rust and Swift, because any language that speaks gRPC and Protocol Buffers can build plans.
- **Isolation and upgrades.** Many clients share one server, each with its own session. A client crash does not kill the driver, and the server can be upgraded without rebuilding clients, within supported version ranges.
- **Same DataFrame API.** Most DataFrame and SQL code runs unchanged.

What you lose: anything that needs direct access to the driver JVM. There is no `SparkContext`, no RDD API, and no `sc._jvm` tricks. Libraries that rely on those must be rewritten to use DataFrames.

> **Pitfall:** With Spark Connect, errors in your plan appear only when an action runs on the server, and some appear later than in classic mode (for example a misspelled column name in a long chain). Call `df.columns` or `df.schema` in tests to force analysis early.

> **Tip:** Since Spark 4.0 you can switch an existing PySpark program to Connect with the setting `spark.api.mode=connect`, which is a cheap way to find code that still depends on RDDs or the JVM.

::: guide
For the protocol, sessions and what Connect changes in production, read [Spark Connect architecture](pyspark-streaming-kafka-topic-map.html#connect) and [PySpark runtime and environment](pyspark-streaming-kafka-topic-map.html#pyruntime) in the PySpark + Kafka guide.
:::

::: check
?? What travels from a Spark Connect client to the server, and what comes back?
An unresolved logical plan encoded as Protocol Buffers goes to the server over gRPC; results come back as Apache Arrow record batches.

?? Name two things that do not work with Spark Connect.
The RDD API and `SparkContext`, and any code that reaches into the driver JVM directly (such as `sc._jvm`).
:::

::: resources
- Docs: [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html) · architecture, how to start a server and connect clients
- Repo: [Spark Connect source in Apache Spark](https://github.com/apache/spark/tree/master/sql/connect) · the protocol definitions and server code
- Repo: [Spark Connect Go client](https://github.com/apache/spark-connect-go) · a non-JVM client that shows how thin a client can be
- Blog: [Introducing Spark Connect](https://www.databricks.com/blog/2022/07/07/introducing-spark-connect-the-power-of-apache-spark-everywhere.html) · the motivation and design from the team that proposed it
- Book: [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/) · the DataFrame API that Connect exposes, explained from the ground up
:::

### Spark on Kubernetes

::: plain
Kubernetes is a general-purpose crane operator for containers: you tell it "run this container with 4 CPUs and 16 GB of memory" and it finds room on some machine. Spark on Kubernetes asks that crane operator for a driver container, and the driver then asks for executor containers. No Hadoop cluster is needed; Spark shares machines with every other service in the company.
:::

Since Spark 3.1 Kubernetes has been a production-ready cluster manager. The flow is simple: `spark-submit` asks the Kubernetes API server to create a **driver pod**; the driver then creates **executor pods** directly, watches them, and deletes them when the job ends.

```bash
spark-submit \
  --master k8s://https://<api-server-host>:443 \
  --deploy-mode cluster \
  --name daily-sales \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.container.image=europe-west1-docker.pkg.dev/my-proj/spark/app:1.4 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.executor.instances=10 \
  --conf spark.kubernetes.node.selector.pool=spark-spot \
  local:///opt/app/daily_sales.py
```

The `local://` path means "the file is already inside the container image", which is the normal way to ship code: build an image with your job and dependencies and push it to Artifact Registry.

The key features:

- **Pod templates** (`spark.kubernetes.driver.podTemplateFile`, `spark.kubernetes.executor.podTemplateFile`) let you set anything Spark has no option for: tolerations, sidecars, init containers, security contexts.
- **Node selectors** (`spark.kubernetes.node.selector.<label>=<value>`) place pods on a chosen node pool, for example Spot VMs for executors.
- **Volumes** (`spark.kubernetes.executor.volumes.<type>.<name>.mount.path`) mount `emptyDir`, `hostPath`, `persistentVolumeClaim` or NFS volumes. A volume whose name starts with `spark-local-dir-` is used for shuffle and spill scratch space, which is how you put shuffle on fast local SSDs.
- **Dynamic allocation** works without an external shuffle service by turning on **shuffle tracking** (`spark.dynamicAllocation.shuffleTracking.enabled=true`): executors that still hold shuffle data are kept until it is no longer needed.

Most teams do not call `spark-submit` by hand. The **Kubeflow Spark Operator** (started at Google as `spark-on-k8s-operator`) adds a `SparkApplication` custom resource, so a job becomes a YAML file you apply, version and schedule like any other Kubernetes object. For fair sharing and **gang scheduling** (start all executors of a job together or none), clusters add a batch scheduler such as Apache YuniKorn or Volcano.

On Google Cloud you can run this on GKE yourself, or let Managed Service for Apache Spark handle it: its serverless batches run Spark in Google-managed containers with no cluster to operate at all.

> **Pitfall:** Executor pods are sized by memory *plus* overhead. With `spark.executor.memory=8g` the pod requests about 8.8 GiB (the default overhead is 10%, at least 384 MiB), and PySpark jobs often need more. If the pod exceeds its limit, Kubernetes kills it with `OOMKilled`, which Spark only sees as a lost executor.

> **Tip:** Use `spark.kubernetes.executor.deleteOnTermination=false` while debugging so failed executor pods stay around and you can read their logs with `kubectl logs`.

::: guide
For cluster managers, resource sizing and decommissioning, read [Deployment and cluster managers](pyspark-streaming-kafka-topic-map.html#deploy) and [Tuning methodology and resource sizing](pyspark-streaming-kafka-topic-map.html#method) in the PySpark + Kafka guide.
:::

::: check
?? Who creates the executor pods in Spark on Kubernetes?
The driver pod, by calling the Kubernetes API directly. `spark-submit` (or the operator) only creates the driver.

?? How does dynamic allocation work on Kubernetes without an external shuffle service?
With shuffle tracking: Spark keeps executors that still hold shuffle files that may be read, and releases only those that are idle and hold none.

?? What does the Spark Operator add?
A `SparkApplication` custom resource and a controller, so Spark jobs are declared, submitted, restarted and monitored as Kubernetes objects.
:::

::: resources
- Docs: [Running Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html) · every `spark.kubernetes.*` option, pod templates and volumes
- Repo: [Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator) · the operator and `SparkApplication` resource, formerly Google's spark-on-k8s-operator
- Docs: [Serverless for Apache Spark overview](https://docs.cloud.google.com/dataproc-serverless/docs/overview) · Google's managed alternative with no cluster to run
- Docs: [Apache YuniKorn: run Spark](https://yunikorn.apache.org/docs/user_guide/workloads/run_spark/) · gang scheduling and queues for Spark on Kubernetes
- Docs: [GKE: about Spot VMs](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/spot-vms) · cheap, preemptible nodes for executor pools
:::

## Optimizer and execution

The first group explained who runs the work. This group explains how Spark decides *what* work to run. When you write a DataFrame query or SQL, you describe a result; Spark's optimizer, **Catalyst**, chooses the plan; its execution engine, **Tungsten**, makes each step fast on the CPU; and **Adaptive Query Execution** corrects the plan while it runs, using real sizes instead of guesses.

The last two topics cover the most expensive thing a plan can contain, the shuffle, and the operation that most often causes one, the join. Once you can read a plan and spot its shuffles and join strategies, you can predict most performance problems before they happen.

### Catalyst Optimizer — Rule-Based & Cost-Based

::: plain
Catalyst is like a sat-nav for your query. You type the destination ("total sales per country for 2026"); it looks at the map (table schemas and statistics), throws away pointless detours, moves filters as early as possible and picks the fastest route. You can ask it to show the route before you drive with `explain()`.
:::

Every DataFrame or SQL query goes through four phases:

1. **Parsing** builds an **unresolved logical plan**: a tree of operators whose column and table names are still just strings.
2. **Analysis** resolves names and types against the **catalog** (which tables exist, what their columns are). A misspelled column fails here, before anything runs.
3. **Logical optimization** applies batches of **rules** that rewrite the tree into an equivalent, cheaper one: `ConstantFolding` turns `1 + 2` into `3`, `PushDownPredicates` moves filters below joins and projections, `ColumnPruning` drops unused columns, `CombineFilters` merges adjacent filters, `BooleanSimplification` simplifies conditions.
4. **Physical planning** turns the logical plan into one or more **physical plans** (concrete algorithms such as `SortMergeJoin` or `BroadcastHashJoin`, `HashAggregate`, `FileScan` with pushed filters), picks one, and hands it to Tungsten for code generation.

[[fig:spark_catalyst]]

```python
from pyspark.sql import functions as F

q = (spark.read.parquet("gs://my-bucket/orders/")
     .join(spark.read.parquet("gs://my-bucket/customers/"), "customer_id")
     .filter(F.col("order_year") == 2026)
     .select("country", "amount")
     .groupBy("country").agg(F.sum("amount")))

q.explain(mode="formatted")   # also: "simple", "extended", "codegen", "cost"
```

In the formatted plan you will see the filter moved down into the scan of `orders` (listed under `PushedFilters` or `PartitionFilters`), only the needed columns in `ReadSchema`, and an `Exchange` operator wherever a shuffle happens. Reading plans from the bottom up, and counting the `Exchange` nodes, is the single most useful Spark skill.

Rules always apply when they match. **Cost-based optimization (CBO)** goes further and uses statistics (row counts, column distinct counts, min and max, histograms) to choose between plans, mainly to reorder joins and to estimate sizes for join strategy selection. CBO is off by default (`spark.sql.cbo.enabled=false`) and needs statistics that you collect yourself:

```sql
ANALYZE TABLE sales.orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_year;
SET spark.sql.cbo.enabled = true;
SET spark.sql.cbo.joinReorder.enabled = true;
```

> **Pitfall:** Statistics go stale. A CBO plan based on last month's row counts can be worse than no CBO at all. If you enable it, refresh statistics as part of the pipeline that loads the table.

> **Tip:** Many teams skip CBO and rely on Adaptive Query Execution, which measures real sizes at runtime and needs no statistics. Use CBO when you have many-way joins over well-maintained catalog tables.

::: guide
For rule batches, physical strategies and how to read every node of a plan, read [Catalyst optimizer and query planning](pyspark-streaming-kafka-topic-map.html#catalyst) and [Spark SQL engine internals](pyspark-streaming-kafka-topic-map.html#sqlengine) in the PySpark + Kafka guide.
:::

::: check
?? In which phase does a query with a misspelled column name fail, and why is that useful?
In analysis, when names are resolved against the catalog. It fails immediately, before any data is read or any task runs.

?? What does an `Exchange` node in a physical plan mean?
A shuffle: data is redistributed across partitions (for example hash-partitioned by a join or group key), which also marks a stage boundary.

?? What does cost-based optimization need that rule-based optimization does not?
Table and column statistics, collected with `ANALYZE TABLE ... COMPUTE STATISTICS`, and the `spark.sql.cbo.enabled` setting.
:::

::: resources
- Docs: [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) · optimizer settings, statistics, hints and AQE
- Docs: [EXPLAIN statement](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html) · the explain modes and what each shows
- Paper: [Spark SQL: Relational Data Processing in Spark](https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf) · Armbrust et al., SIGMOD 2015; the design of Catalyst
- Blog: [Deep dive into Spark SQL's Catalyst optimizer](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html) · trees, rules and the four phases with examples
- Blog: [Cost-based optimizer in Apache Spark 2.2](https://www.databricks.com/blog/2017/08/31/cost-based-optimizer-in-apache-spark-2-2.html) · statistics, cardinality estimation and join reordering
:::

### Tungsten Execution Engine & WSCG

::: plain
Catalyst picks the route; **Tungsten** is the engine tuning. Instead of passing each row through a chain of small, general-purpose functions (like a parcel handed from clerk to clerk), Spark writes one specialised function for the whole chain at runtime and compiles it, so a row goes through in one tight loop. It also stores rows as packed bytes rather than Java objects, which saves memory and keeps the CPU busy.
:::

**Project Tungsten** (Spark 1.4 to 2.0) attacked the CPU and memory cost of running on the JVM. Its main pieces:

- **Binary row format.** Rows are stored as `UnsafeRow`: a compact byte layout with a null bitmap, fixed-width slots and a region for variable-length values. There are no per-row Java objects, so memory use is much lower and the garbage collector has far less to do.
- **Explicit memory management.** Spark allocates large pages of memory (on-heap or, optionally, off-heap through `sun.misc.Unsafe`) and manages them itself, tracking how much each task uses.
- **Cache-aware algorithms.** Sorting works on an array of (key prefix, pointer) pairs that fits CPU caches, comparing prefixes first and following the pointer only on ties. This is the Tungsten sort used by shuffle and sort-merge join.
- **Whole-stage code generation (WSCG).** Spark fuses a chain of operators (scan, filter, project, partial aggregate) into one generated Java function, compiles it with the Janino compiler at runtime, and lets the JIT turn it into machine code. Values stay in CPU registers instead of being passed between virtual function calls.

You can see WSCG in a plan. Operators inside a generated stage are marked with an asterisk and a codegen stage id:

```text
*(2) HashAggregate(keys=[country], functions=[sum(amount)])
+- Exchange hashpartitioning(country, 200)
   +- *(1) HashAggregate(keys=[country], functions=[partial_sum(amount)])
      +- *(1) Project [country, amount]
         +- *(1) Filter (isnotnull(amount) AND (amount > 100))
            +- *(1) ColumnarToRow
               +- FileScan parquet [country, amount] ...
```

Codegen stage 1 fuses the filter, projection and partial aggregate. The `Exchange` breaks fusion, and stage 2 starts after it. `df.explain(mode="codegen")` prints the generated Java code itself.

Alongside codegen, Spark uses **columnar** execution where it can: the vectorized Parquet and ORC readers decode a batch of values per column at a time (`ColumnarToRow` converts to rows for the fused code), and Arrow carries columnar batches to Python.

> **Pitfall:** Python UDFs, some complex expressions and very wide schemas fall out of whole-stage codegen. Spark then runs those operators one row at a time. If a plan shows operators without the `*`, check whether a UDF or an unsupported function caused it.

> **Tip:** Leave `spark.sql.codegen.wholeStage` at its default (`true`). Turning it off is a debugging tool, not a tuning knob.

::: deeper
WSCG follows the "produce/consume" model from the HyPer database: each operator generates code that pulls from its child and pushes to its parent, and the generated code for a whole pipeline becomes one loop. Its limits led to a new generation of **native vectorized engines** that plug into Spark's planner: Databricks Photon (C++), Apache Gluten with the Velox library, and Apache DataFusion Comet (Rust). They process columnar batches with SIMD instructions instead of generating JVM code. Google also offers its own accelerated engine for Managed Service for Apache Spark (Lightning Engine, in the premium tier); check the current documentation for availability.
:::

::: guide
For the row format, codegen limits and columnar execution, read [Tungsten, code generation and columnar execution](pyspark-streaming-kafka-topic-map.html#tungsten) in the PySpark + Kafka guide.
:::

::: check
?? What does the `*(1)` prefix in a physical plan mean?
The operator is part of whole-stage code generation stage 1: it is fused with the other `*(1)` operators into one generated function.

?? Why does the `UnsafeRow` format reduce garbage collection?
Rows are stored as bytes inside large memory pages that Spark manages, not as millions of small Java objects, so the garbage collector has far fewer objects to track.
:::

::: resources
- Blog: [Project Tungsten: bringing Spark closer to bare metal](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html) · the original goals: memory management, cache-aware computation, codegen
- Blog: [Apache Spark as a compiler](https://www.databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html) · how whole-stage codegen works, with benchmarks
- Paper: [Efficiently compiling efficient query plans for modern hardware](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) · Neumann, VLDB 2011; the produce/consume model behind WSCG
- Book: [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) · Karau and Warren; how the execution engine shapes your code choices
- Repo: [Apache Gluten](https://github.com/apache/incubator-gluten) · a native vectorized backend for Spark built on Velox
:::

### Adaptive Query Execution (AQE)

::: plain
Planning a query before it runs is like planning a road trip before you know the traffic. **Adaptive Query Execution** re-plans at every rest stop: after each shuffle, Spark looks at how much data actually came out and changes the rest of the route, for example choosing a faster join because a table turned out to be small, or merging many tiny piles of data into a few sensible ones.
:::

A static plan relies on estimates, and estimates are often wrong: a filter keeps 1% of rows instead of 50%, or one key holds half the data. **AQE** (on by default since Spark 3.2, `spark.sql.adaptive.enabled=true`) uses the fact that every shuffle is a natural checkpoint. When a shuffle's map side finishes, Spark knows the exact size of every shuffle partition, and it re-optimizes the remaining plan with those numbers.

It does three main things:

1. **Coalescing shuffle partitions.** `spark.sql.shuffle.partitions` defaults to 200 for every shuffle, which is too many for small data and too few for large data. AQE merges adjacent small partitions until each is close to `spark.sql.adaptive.advisoryPartitionSizeInBytes` (64 MB by default). A job that would have run 200 tasks on 2 MB each may run 7.
2. **Switching join strategies.** If one side of a sort-merge join turns out to be small after filtering, AQE converts it to a **broadcast hash join** (threshold `spark.sql.adaptive.autoBroadcastJoinThreshold`, which defaults to the static `spark.sql.autoBroadcastJoinThreshold` of 10 MB) or, when partitions are small, to a shuffled hash join.
3. **Splitting skewed partitions.** In a sort-merge join, a partition is treated as skewed when it is larger than `spark.sql.adaptive.skewJoin.skewedPartitionFactor` (default 5) times the median partition size *and* larger than `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (default 256 MB). AQE splits it into several tasks and duplicates the matching part of the other side.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")                     # default since 3.2
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
spark.conf.set("spark.sql.shuffle.partitions", "1000")   # start high; AQE coalesces down
```

In the Spark UI's SQL tab and in `explain()`, an adaptive plan starts with `AdaptiveSparkPlan isFinalPlan=false` before it runs and `isFinalPlan=true` afterwards, and you will see nodes such as `AQEShuffleRead` with `coalesced` or `skewed` markers.

> **Pitfall:** AQE can only coalesce, never split ordinary partitions. If you set `spark.sql.shuffle.partitions` too low, each task gets too much data and AQE cannot help. Start with a generous number and let AQE merge.

> **Tip:** AQE does not change the very first stage, which reads files: its parallelism comes from file splits (`spark.sql.files.maxPartitionBytes`, 128 MB by default). Fix small or huge input files with table maintenance, not AQE.

::: guide
For every AQE rule, its settings and when it fails to trigger, read [Adaptive Query Execution](pyspark-streaming-kafka-topic-map.html#aqe) in the PySpark + Kafka guide.
:::

::: check
?? Why can AQE re-optimize at a shuffle and not in the middle of a stage?
At a shuffle the map side has finished and written its output, so Spark knows the exact size of every partition and can re-plan the stages that read it. Inside a stage there is no such point.

?? A join stage has partitions of median size 40 MB and one of 1 GB. With default settings, will AQE treat it as skewed?
Yes: 1 GB is more than 5 times the 40 MB median and more than the 256 MB threshold, so AQE splits it.

?? Why is it reasonable to set `spark.sql.shuffle.partitions` high when AQE is on?
AQE coalesces small partitions after measuring them, but it cannot split ordinary partitions that are too large, so a high starting number is safe and a low one is not.
:::

::: resources
- Docs: [Adaptive Query Execution in the performance tuning guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) · every AQE setting and its default
- Blog: [Adaptive Query Execution: speeding up Spark SQL at runtime](https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html) · the design and the three optimizations with diagrams
- Docs: [Adaptive query execution on Databricks](https://docs.databricks.com/gcp/en/optimizations/aqe) · a practical list of what AQE changes and how to read adaptive plans in a query profile
- Book: [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) · partitioning and join behaviour that AQE now adjusts automatically
:::

### Shuffle Internals — Sort-Based Shuffle

::: plain
A shuffle is a giant mail sort. Every post office (map task) sorts its letters into bags by destination town (reduce partition) and leaves the bags on its doorstep. Then each town's courier (reduce task) drives round every post office to collect its bag. It is the most expensive thing Spark does, because every letter is written to disk and carried across the network.
:::

A **shuffle** redistributes rows so that all rows with the same key end up in the same partition. It happens for `groupBy`, non-broadcast joins, `distinct`, `repartition` and window functions with `partitionBy`. It has two sides:

- **Shuffle write (map side).** Each task of the earlier stage computes, for each row, the target partition (usually `hash(key) % numPartitions`), sorts or groups its output by that partition id, and writes **one data file plus one index file** to local disk. The index says where each partition's bytes start.
- **Shuffle read (reduce side).** Each task of the next stage fetches its partition's slice from every map output, over the network, then merges, aggregates or sorts the pieces.

[[fig:spark_shuffle]]

Since Spark 2.0 there is a single **SortShuffleManager**, which picks one of three writers per shuffle:

| Writer | When it is used | How it works |
|---|---|---|
| BypassMergeSortShuffleWriter | No map-side aggregation and few partitions (≤ `spark.shuffle.sort.bypassMergeThreshold`, 200) | One temporary file per partition, concatenated at the end |
| UnsafeShuffleWriter (serialized) | Serializer supports relocation, no map-side aggregation, fewer than about 16 million partitions | Sorts serialized binary records by partition id using Tungsten memory |
| SortShuffleWriter | Everything else, including map-side aggregation | `ExternalSorter` buffers, aggregates and spills to disk, then merges |

Numbers make the cost concrete. A 500 GB table grouped by key with 1,000 map tasks and 1,000 reduce partitions produces up to 1,000,000 map-output blocks. Every reduce task makes 1,000 fetches, each perhaps only 500 KB. Lots of small random reads are why large shuffles are slow even on fast networks, and why the number and size of partitions matter so much.

Map outputs live on the executors' local disks. If an executor dies, its shuffle files are lost and the map tasks must re-run. Two features reduce that risk: the **external shuffle service** (a separate process on each node that keeps serving files after an executor is removed, used with YARN), and on Kubernetes, **shuffle tracking** and executor **decommissioning**, which can migrate shuffle blocks before an executor leaves. **Push-based shuffle** (Spark 3.2+, YARN only, based on LinkedIn's Magnet) pushes map outputs to remote shuffle services that merge them per partition, turning many small reads into a few large ones.

> **Pitfall:** On a Managed Service for Apache Spark cluster, putting executors on preemptible Spot VM secondary workers means lost shuffle files every time a VM is reclaimed. Keep shuffle data on non-preemptible primary workers (enhanced flexibility mode does this) or use serverless batches.

> **Tip:** The cheapest shuffle is the one you avoid: filter and select columns before wide operations, broadcast small tables, and do not `repartition()` just before a write unless you need to.

::: guide
For the writers, fetch protocol, spill and push-based shuffle in depth, read [Shuffle internals](pyspark-streaming-kafka-topic-map.html#shuffle) and [Spark Core engine internals](pyspark-streaming-kafka-topic-map.html#core) in the PySpark + Kafka guide.
:::

::: check
?? What does one map task write during a shuffle?
One data file containing its output sorted (grouped) by target partition, plus an index file giving each partition's offset in the data file.

?? Why does losing an executor sometimes cause earlier stages to re-run?
Its local shuffle output files are lost, so the map tasks that produced them must be re-executed before the reduce stage can fetch their data (unless an external shuffle service or decommissioning preserved them).
:::

::: resources
- Docs: [Shuffle operations in the RDD guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations) · what triggers a shuffle and its performance impact
- Docs: [Shuffle behaviour configuration](https://spark.apache.org/docs/latest/configuration.html#shuffle-behavior) · every `spark.shuffle.*` setting and its default
- Paper: [Magnet: push-based shuffle service for large-scale data processing](https://www.vldb.org/pvldb/vol13/p3382-shen.pdf) · Shen et al., VLDB 2020; the design behind push-based shuffle
- Book: [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) · chapter 2 and the key/value chapter explain shuffle costs
- Repo: [Spark Internals: shuffle details](https://github.com/JerryLead/SparkInternals) · illustrated walk through shuffle write and read
:::

### Distributed Join Algorithms

::: plain
Joining two big tables across many machines is like matching guests to tables at a huge wedding. If the seating chart is short, you photocopy it and give every usher a copy (a **broadcast join**). If both lists are long, you first send every guest whose surname starts with A to room A, and every seat card starting with A to room A too, then match inside each room (a **shuffle join**). Which way is cheaper depends on the size of the smaller list.
:::

Every distributed engine has the same two basic choices: **broadcast** the small side to every worker, or **partition** (shuffle) both sides by the join key so matching keys meet. Spark has five physical join operators:

| Strategy | How it works | Good for | Needs |
|---|---|---|---|
| Broadcast hash join (BHJ) | Small side collected, built into a hash table, sent to every executor; big side not shuffled | Big table joined to a small one | Small side below the broadcast threshold (10 MB default) and fits in memory |
| Shuffle hash join (SHJ) | Both sides shuffled; a hash table built per partition from the smaller side | Medium tables with small partitions | Each partition of the build side fits in memory |
| Sort-merge join (SMJ) | Both sides shuffled and sorted by key, then merged | Two large tables; the default | Sortable keys; spills safely to disk |
| Broadcast nested loop join | One side broadcast, every pair compared | Non-equi joins (`a.ts BETWEEN b.start AND b.end`) | A small side |
| Cartesian product | Every row with every row | Cross joins | Very small inputs, or a good reason |

Spark chooses using estimated sizes, your hints and the join condition: equi-joins prefer BHJ when one side is small enough, then SMJ (`spark.sql.join.preferSortMergeJoin=true`), and non-equi joins fall back to nested loops. AQE can then convert an SMJ into a BHJ at runtime.

You can force a strategy with a hint:

```python
from pyspark.sql.functions import broadcast

enriched = orders.join(broadcast(countries), "country_code")       # force BHJ

# SQL hints: BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL
spark.sql("""
  SELECT /*+ SHUFFLE_HASH(c) */ o.order_id, c.segment
  FROM orders o JOIN customers c ON o.customer_id = c.customer_id
""")
```

A broadcast join of a 1 TB fact table with a 5 MB dimension avoids shuffling the terabyte entirely: only 5 MB times the number of executors crosses the network. The same join as SMJ shuffles and sorts all 1 TB.

The same ideas appear in other engines with other names. Trino chooses between a **broadcast** (replicated) and a **partitioned** join distribution; BigQuery decides between broadcast and hash joins per stage; Flink adds streaming-specific joins (interval joins, temporal joins, lookup joins against an external store). **Storage-partitioned joins** (Spark 3.3+, for sources such as Apache Iceberg tables bucketed on the join key) skip the shuffle entirely because the data is already laid out by key.

> **Pitfall:** Broadcasting a table that is "small" on disk can still fail. A 50 MB compressed Parquet file may expand to 500 MB in memory, and the driver must collect it first. Watch for driver out-of-memory errors and broadcast timeouts (`spark.sql.broadcastTimeout`, 300 seconds by default).

> **Pitfall:** Joining on a key with many nulls or one very common value sends all those rows to one partition. That is skew, covered two topics below.

::: guide
For how Spark selects and executes each join, read [Physical join strategies](pyspark-streaming-kafka-topic-map.html#joinphys) and [Joins: semantics and API](pyspark-streaming-kafka-topic-map.html#joins) in the PySpark + Kafka guide.
:::

::: check
?? Why does a broadcast hash join avoid shuffling the large table?
Each executor receives a full copy of the small side as a hash table, so every partition of the large table can be joined where it already is.

?? Which join strategy does Spark use for a condition like `a.ts BETWEEN b.start_ts AND b.end_ts` with no equality?
A broadcast nested loop join if one side is small enough to broadcast, otherwise a cartesian product with a filter. Sort-merge and hash joins need an equality condition.

?? When does a storage-partitioned join apply?
When both tables are stored partitioned or bucketed compatibly on the join key in a source that reports that layout (such as Iceberg), so Spark can join matching buckets without a shuffle.
:::

::: resources
- Docs: [Join strategy hints](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html#join-hints) · the four join hints and their priority
- Docs: [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) · broadcast threshold and AQE join conversion
- Docs: [Trino: join distribution type](https://trino.io/docs/current/optimizer/cost-based-optimizations.html) · how another engine chooses broadcast versus partitioned joins
- Book: [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) · the joins chapter, including custom partitioning
- Book: *Designing Data-Intensive Applications* by Martin Kleppmann · chapter 10 explains sort-merge, broadcast and partitioned hash joins in batch systems
:::

## Performance and operations

The optimizer gives you a good plan; this group is about what happens when the plan meets real hardware and real data. Most production Spark incidents fall into a few families: executors running out of memory, one task running for an hour while the others finish in a minute, the garbage collector eating the CPU, and Python code that is far slower than it needs to be. The Spark UI is the tool that tells you which family you are in.

The group ends with two practical topics: how to test Spark code so these problems are caught before production, and how the commercial Databricks runtime differs from open-source Spark.

### Spark Memory Management

::: plain
An executor's memory is like a kitchen counter shared by two cooks. One cook needs space to chop and mix (execution: joins, sorts, aggregations); the other keeps prepared dishes ready (storage: cached data). Spark lets either cook borrow the other's space when it is free, and when the counter is full, food goes into the fridge (spilled to disk). If even that is not enough, the kitchen closes (out of memory).
:::

An executor container's memory has several parts. With `spark.executor.memory=8g`:

- **Memory overhead** (`spark.executor.memoryOverhead`, default 10% of executor memory with a minimum of 384 MB, so about 819 MB here) sits outside the JVM heap. It covers thread stacks, native libraries, network buffers and, by default, Python worker processes. The container asks for heap plus overhead.
- **Reserved memory**: a fixed 300 MB of the heap for Spark's internals.
- **Unified memory**: `spark.memory.fraction` (default 0.6) of the rest. Here: (8192 − 300) × 0.6 ≈ 4,735 MB.
- **User memory**: the remaining 40%, about 3,157 MB, for your own data structures, UDF objects and Spark metadata.

[[fig:spark_memory]]

Inside unified memory, **execution** memory (buffers for shuffles, sorts, joins and aggregations) and **storage** memory (cached blocks and broadcast variables) share one pool. `spark.memory.storageFraction` (default 0.5) marks the part of storage that execution cannot evict. The rules:

- Either side can borrow free space from the other.
- Execution can evict cached blocks beyond the protected storage fraction.
- Storage can never evict execution memory, because evicting a running sort's buffers would break the task.

When a task cannot get more execution memory, it **spills**: it writes sorted runs to local disk and merges them later. Spilling is slow but safe. Out-of-memory errors happen when something cannot spill: a huge single record, a broadcast table too big for memory, user code holding large objects, or memory used outside the heap exceeding the overhead.

```bash
gcloud dataproc batches submit pyspark job.py --region=europe-west1 \
  --properties=spark.executor.memory=12g,spark.executor.memoryOverhead=3g,spark.executor.cores=4
```

Memory per task is roughly unified memory divided by executor cores. Four cores in a 12 GB executor give each concurrent task about 1.7 GB of execution and storage space; eight cores halve it.

**Off-heap** memory (`spark.memory.offHeap.enabled=true` with `spark.memory.offHeap.size`) lets Tungsten allocate pages outside the JVM heap, which reduces garbage collection pressure. It is added to the container size, so budget for it.

> **Pitfall:** "Container killed for exceeding memory limits" (YARN) or `OOMKilled` (Kubernetes) is not a heap problem. The process used more than heap plus overhead, usually because of Python workers, Arrow buffers or native libraries. Raise `spark.executor.memoryOverhead`, not `spark.executor.memory`.

> **Tip:** Prefer more, moderately sized executors (4 to 5 cores, tens of GB) over a few huge ones. Very large heaps make garbage collection pauses longer, and a single executor loss costs more.

::: guide
For the memory manager, spill paths and a method for sizing executors, read [Memory management](pyspark-streaming-kafka-topic-map.html#memory), [Out-of-memory failures](pyspark-streaming-kafka-topic-map.html#oom) and [Tuning methodology and resource sizing](pyspark-streaming-kafka-topic-map.html#method) in the PySpark + Kafka guide.
:::

::: check
?? With `spark.executor.memory=10g` and default settings, about how big is unified memory?
(10,240 − 300) × 0.6 ≈ 5,964 MB, about 5.8 GB.

?? Can cached data force a running sort to give up its memory?
No. Execution memory can evict cached blocks (beyond the protected storage fraction), but storage can never evict execution memory.

?? An executor is killed by YARN for exceeding container memory, but the JVM heap was not full. What do you change?
Increase `spark.executor.memoryOverhead` (or `spark.executor.pyspark.memory` for Python workers), because the memory was used outside the heap.
:::

::: resources
- Docs: [Memory management overview in the tuning guide](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview) · the unified model and its two settings
- Docs: [Spark configuration: memory settings](https://spark.apache.org/docs/latest/configuration.html#memory-management) · every memory property and default
- Docs: [Serverless for Apache Spark properties](https://docs.cloud.google.com/dataproc-serverless/docs/concepts/properties) · which memory and core settings serverless batches accept
- Book: [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) · memory, caching and spill behaviour in practice
- Blog: [Understanding your Spark application through visualization](https://www.databricks.com/blog/2015/06/22/understanding-your-spark-application-through-visualization.html) · reading the UI timeline to see where memory and time go
:::

### Data Skew Handling

::: plain
Imagine twenty checkout lanes at a supermarket and one customer with a trolley piled ten metres high. Nineteen lanes empty in a minute; one lane takes an hour, and everyone waits for it. **Data skew** is when one key (one customer, one country, one null) holds far more rows than the rest, so one task does most of the work while the cluster sits idle.
:::

Spark puts all rows with the same key into the same partition during a shuffle. If 30% of orders have `customer_id = NULL` or belong to one marketplace account, that partition is 30% of the data, and the stage takes as long as that one task.

**Diagnose it in the Spark UI.** Open the slow stage and look at the summary metrics table. Skew looks like this:

| Metric | Min | 25th pct | Median | 75th pct | Max |
|---|---|---|---|---|---|
| Duration | 3 s | 4 s | 5 s | 6 s | 41 min |
| Shuffle read size | 40 MB | 48 MB | 52 MB | 55 MB | 31 GB |

When max is tens or hundreds of times the median, find the key:

```python
orders.groupBy("customer_id").count().orderBy("count", ascending=False).show(10)
```

**Fixes, from simplest to most work:**

1. **Filter or handle the hot value separately.** Nulls usually cannot match in an inner join anyway: filter them out before the join, or process them in a separate branch and `union` the results.
2. **Broadcast the other side.** If the table you join to is small, a broadcast join has no shuffle, so the hot key is spread over the big table's partitions.
3. **Let AQE split it.** With `spark.sql.adaptive.skewJoin.enabled=true` (the default), AQE splits oversized sort-merge join partitions automatically. Lower the thresholds if your skew is real but below them.
4. **Salt the key.** Add a random number to the hot key on the big side, and replicate the small side once per salt value, so one key becomes N keys spread across N partitions.

```python
from pyspark.sql import functions as F

N = 16
orders_s = orders.withColumn("salt", (F.rand() * N).cast("int"))
cust_s = customers.crossJoin(spark.range(N).withColumnRenamed("id", "salt"))
joined = orders_s.join(cust_s, ["customer_id", "salt"]).drop("salt")
```

For aggregations, salting becomes **two-phase aggregation**: aggregate by (key, salt) first, then by key. This works for sums, counts, min and max, but not directly for exact distinct counts or medians.

> **Pitfall:** Salting multiplies the replicated side by N. Salt only the hot keys (join them in a separate branch) when the other side is large, or the fix costs more than the skew.

> **Pitfall:** Skew also hides in writes. `partitionBy("country")` on data where one country holds half the rows produces one huge task and one huge folder.

::: guide
For more detection methods and fixes, including skew in windows and writes, read [Data skew: detection and fixes](pyspark-streaming-kafka-topic-map.html#skew) and [Adaptive Query Execution](pyspark-streaming-kafka-topic-map.html#aqe) in the PySpark + Kafka guide.
:::

::: check
?? Which Spark UI numbers reveal skew?
In the stage's summary metrics, the max task duration and shuffle read size far above the median (for example tens of times larger).

?? How does salting fix a skewed join, and what does it cost?
It adds a random salt to the hot keys on the large side and replicates the other side once per salt value, so the hot key is processed by N tasks. The cost is N copies of the replicated side.

?? Why is filtering nulls before an inner join a skew fix?
Null keys never match in an inner join, but they are still shuffled to one partition. Removing them first removes the hot partition at no cost to the result.
:::

::: resources
- Docs: [Optimizing skew join with AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join) · the automatic skew split and its thresholds
- Docs: [Spark web UI: stage detail](https://spark.apache.org/docs/latest/web-ui.html#stage-detail) · where the summary metrics that reveal skew live
- Book: [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) · key/value chapter on skew, custom partitioners and secondary sort
- Book: [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) · performance chapter covering partitioning and skew
:::

### JVM GC Tuning for Spark

::: plain
Java programs never free memory by hand. A **garbage collector** (GC) walks through memory now and then, finds objects nobody uses any more and reclaims them. While it works, your tasks may pause, like a restaurant closing for ten minutes to clear tables. If the restaurant spends a third of its evening clearing tables, it serves far fewer meals.
:::

Spark executors are JVMs, so every object your job creates (deserialized rows in RDD code, Python-to-JVM conversions, cached Java objects, hash maps in aggregations) must eventually be garbage collected. Tungsten's binary format avoids most per-row objects, so DataFrame jobs suffer far less than old RDD code, but GC still matters with large heaps, wide caches and user code.

**How to see it.** In the Spark UI, the Stages page shows **GC Time** per task, and the Executors tab shows total GC time per executor and highlights executors whose GC time is a large share of task time. As a rule of thumb, GC above about 10% of task time deserves attention.

**The collectors.** Spark 4 runs on Java 17 or 21, and the choice is between:

| Collector | Behaviour | For Spark |
|---|---|---|
| G1 (default) | Splits the heap into regions, collects the most garbage-filled first, targets a pause time | The right default for almost all jobs |
| Parallel | Maximises throughput, longer stop-the-world pauses | Sometimes faster for pure batch jobs with small heaps |
| ZGC (generational in Java 21+) | Very short pauses even with huge heaps, some throughput cost | Large heaps, latency-sensitive drivers or streaming |
| CMS | Removed in Java 14 | Do not use; old guides still mention it |

You pass JVM options through `spark.executor.extraJavaOptions` (and `spark.driver.extraJavaOptions`):

```bash
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -Xlog:gc*:stderr"
```

`-Xlog:gc*` is the Java 9+ unified logging flag; old flags such as `-XX:+PrintGCDetails` are gone. You cannot set the heap size here (`-Xmx`); use `spark.executor.memory`.

**What actually fixes GC problems,** in order of payoff:

1. Use DataFrames instead of RDDs of objects, and built-in functions instead of UDFs.
2. Cache in serialized form (`MEMORY_AND_DISK_SER` for RDDs) or not at all; DataFrame caching already uses a compact columnar format.
3. Use more executors with smaller heaps (for example 4 to 5 cores and 16 to 32 GB) instead of one huge heap per machine.
4. Give G1 headroom: start concurrent marking earlier (`InitiatingHeapOccupancyPercent`) or increase memory so the heap is not constantly full.

> **Pitfall:** Long GC pauses can look like network failures. An executor frozen in a full GC misses heartbeats, the driver declares it lost, and its tasks re-run. If you see "executor lost" with no obvious cause, check the GC logs first.

> **Tip:** Fix the data and the code before the collector. Switching collectors rarely gives more than a modest gain; removing a cached RDD of Java objects can make GC disappear.

::: guide
For reading GC logs, heap sizing and when to switch collectors, read [JVM and garbage collection tuning](pyspark-streaming-kafka-topic-map.html#gc) and [Memory management](pyspark-streaming-kafka-topic-map.html#memory) in the PySpark + Kafka guide.
:::

::: check
?? Where in the Spark UI do you see whether GC is a problem?
In the GC Time column of task metrics on the Stages page and in the Executors tab, which shows GC time per executor relative to task time.

?? Why can you not set `-Xmx` in `spark.executor.extraJavaOptions`?
Spark sets the heap size from `spark.executor.memory` and rejects heap size flags in the extra options, so that container sizing stays consistent.
:::

::: resources
- Docs: [Garbage collection tuning in the Spark tuning guide](https://spark.apache.org/docs/latest/tuning.html#garbage-collection-tuning) · measuring GC, data structure advice and G1 tips
- Docs: [Java 21 garbage collection tuning guide](https://docs.oracle.com/en/java/javase/21/gctuning/) · G1, Parallel and ZGC explained by Oracle
- Blog: [Tuning Java garbage collection for Spark applications](https://www.databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html) · an experiment comparing collectors on Spark workloads (Java 8 era, ideas still valid)
- Book: [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) · data structure choices that reduce GC pressure
:::

### Spark UI & Performance Debugging

::: plain
The Spark UI is the dashboard of your car. You do not need it while things go well, but when the car is slow or makes a strange noise, it tells you whether you are low on fuel, overheating or stuck in first gear. Learning to read five or six numbers on it saves hours of guessing.
:::

Every running application serves a web UI from the driver (port 4040 by default). After the application ends, the **Spark History Server** rebuilds the same UI from event logs (`spark.eventLog.enabled=true`). On Google Cloud, Managed Service for Apache Spark clusters link the UI from the console, a **Persistent History Server** cluster can serve logs of deleted clusters, and serverless batches show their Spark UI in the console.

The tabs and what to look for:

| Tab | What it shows | What to look for |
|---|---|---|
| Jobs | One row per action, event timeline | Which action is slow; executors added and removed |
| Stages | Every stage with input, output, shuffle read and write | The slowest stage; huge shuffle write; spill |
| Stage detail | Task summary quantiles, per-task table | Max far above median (skew), GC time, spill |
| SQL / DataFrame | The physical plan as a graph with row counts per operator | Where rows explode or shrink; which join ran |
| Storage | Cached datasets and how much is in memory or on disk | Caches you forgot to unpersist |
| Executors | Memory, cores, tasks, GC time, failures per executor | Lost executors, high GC, uneven work |
| Environment | All settings as the application saw them | Whether your setting actually applied |

**A debugging routine that works:**

1. In **Jobs**, find the slowest job; click into it and find the slowest stage.
2. In the **stage detail**, compare max and median task duration. A large gap means skew; equal and long means the stage simply has too much work per task or too few tasks.
3. Check **Spill (memory)** and **Spill (disk)**. Any spill means tasks lacked execution memory: more partitions or more memory per task.
4. Check **GC time**. Above about 10% of task time points to memory pressure or object-heavy code.
5. Open the **SQL** tab for that query. Read the plan graph: row counts after each join tell you where a join multiplied rows; the join node names tell you whether a broadcast happened.
6. Look at **Shuffle read size** per task. Healthy tasks typically read tens to a few hundred MB.

> **Pitfall:** The Environment tab is the truth about settings. A property set in a notebook after the session started, or misspelled, simply does not apply. Check it there before concluding that a setting "did nothing".

> **Tip:** Name your work. `spark.sparkContext.setJobDescription("load orders")` (or `setJobGroup`) labels jobs in the UI, which makes a 40-job application readable.

::: guide
For a full tour of every page and how to profile Python code, read [Spark UI mastery](pyspark-streaming-kafka-topic-map.html#ui) and [Profiling CPU, memory and Python](pyspark-streaming-kafka-topic-map.html#profiling) in the PySpark + Kafka guide.
:::

::: check
?? A stage's tasks all take about 20 minutes and all show large disk spill. What is wrong, and what do you try?
Each task has too much data for its memory, evenly (not skew). Increase the number of shuffle partitions so each task gets less, or give each task more memory (fewer cores per executor or more executor memory).

?? Where do you check whether a broadcast join actually happened?
In the SQL / DataFrame tab's plan graph (or the final adaptive plan in `explain()`), which names the join operator: `BroadcastHashJoin` versus `SortMergeJoin`.
:::

::: resources
- Docs: [Spark web UI](https://spark.apache.org/docs/latest/web-ui.html) · every tab explained with screenshots
- Docs: [Monitoring and instrumentation](https://spark.apache.org/docs/latest/monitoring.html) · the History Server, event logs, REST API and metrics
- Docs: [Persistent History Server](https://docs.cloud.google.com/dataproc/docs/concepts/jobs/history-server) · keeping Spark UIs after Google Cloud clusters are deleted
- Book: [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/) · debugging and performance chapters
- Blog: [Understanding your Spark application through visualization](https://www.databricks.com/blog/2015/06/22/understanding-your-spark-application-through-visualization.html) · the event timeline and DAG views explained
:::

### PySpark Internals & Arrow Optimization

::: plain
PySpark is a Python remote control for a Java engine. As long as you press the engine's own buttons (built-in DataFrame functions), all the work happens in Java and Python is only the hand on the remote. When you write your own Python function, Spark has to ship every row out to Python and back, like translating a book one word at a time. **Apache Arrow** lets it ship whole pages at once.
:::

In classic PySpark, the Python driver talks to the JVM driver through **Py4J**, a bridge that forwards method calls. Your DataFrame code only builds a plan in the JVM, so it runs at the same speed as Scala. Python code runs on executors only when you use Python functions:

- **Row-at-a-time Python UDFs** (`@udf`): the executor starts Python worker processes, serializes rows with pickle in batches, the worker calls your function once per row, and results are pickled back. Serialization often costs more than the function.
- **pandas UDFs** (`@pandas_udf`): data moves as **Arrow** columnar batches (10,000 rows per batch by default, `spark.sql.execution.arrow.maxRecordsPerBatch`), and your function receives a `pandas.Series` per batch and works with vectorized operations.
- **Arrow-optimized Python UDFs** (Spark 3.5+, `@udf(useArrow=True)` or `spark.sql.execution.pythonUDF.arrow.enabled`): ordinary row-wise functions but with Arrow transfer instead of pickle.

```python
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

@F.udf("double")                              # slow: one Python call per row, pickled
def with_tax_row(amount):
    return amount * 1.2 if amount is not None else None

@pandas_udf("double")                         # fast: one call per Arrow batch
def with_tax_vec(amount: pd.Series) -> pd.Series:
    return amount * 1.2

best = orders.withColumn("gross", F.col("amount") * 1.2)   # fastest: no Python at all
```

Other Arrow-based APIs follow the same idea: `mapInPandas` and `mapInArrow` for whole-partition transforms, `applyInPandas` for grouped data, and `toPandas()` / `createDataFrame(pandas_df)`, which use Arrow when `spark.sql.execution.arrow.pyspark.enabled=true`.

| Approach | Data transfer | Typical speed |
|---|---|---|
| Built-in functions | None (JVM only) | Fastest |
| pandas UDF | Arrow batches, vectorized | Close to built-ins for numeric work |
| Arrow-optimized Python UDF | Arrow batches, row-wise calls | Faster than classic UDF |
| Classic Python UDF | Pickled rows, row-wise calls | Slowest |

> **Pitfall:** Python workers live outside the JVM heap. Large pandas batches, Arrow buffers and libraries such as NumPy use **overhead** memory, so PySpark jobs with UDFs often need a larger `spark.executor.memoryOverhead` (or a set `spark.executor.pyspark.memory`).

> **Pitfall:** `toPandas()` collects everything into the driver. With Arrow it is much faster, but it is still a single machine.

::: guide
For worker processes, serialization, Arrow settings and Python profiling, read [UDFs, pandas and Arrow in PySpark](pyspark-streaming-kafka-topic-map.html#udf), [PySpark-specific performance](pyspark-streaming-kafka-topic-map.html#pyperf) and [PySpark runtime and environment](pyspark-streaming-kafka-topic-map.html#pyruntime) in the PySpark + Kafka guide.
:::

::: check
?? Why does a DataFrame query written in PySpark usually run as fast as the same query in Scala?
PySpark only builds the plan; execution happens entirely in the JVM. Python is involved at run time only when the plan contains Python UDFs or Python data transfers.

?? What makes a pandas UDF faster than a classic Python UDF?
Data moves as Arrow columnar batches instead of pickled rows, and the function runs vectorized pandas operations on a whole batch instead of being called once per row.
:::

::: resources
- Docs: [Apache Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html) · pandas UDF types, Arrow settings and supported types
- Docs: [Python user-defined table functions](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_udtf.html) · Python functions that return tables, a newer UDF form
- Blog: [Introducing vectorized UDFs for PySpark](https://www.databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html) · the original pandas UDF design with benchmarks
- Blog: [Speeding up PySpark with Apache Arrow](https://arrow.apache.org/blog/2017/07/26/spark-arrow/) · how Arrow transfer works between JVM and Python
- Repo: [Py4J](https://github.com/py4j/py4j) · the bridge the classic PySpark driver uses to call the JVM
:::

### Spark Testing & CI Pipelines

::: plain
A Spark job is a function from input tables to output tables, so you can test it like any function: build a tiny input by hand, run the function and compare the output with what you expected. You do not need a cluster; Spark runs happily inside a single process on your laptop or in a CI runner.
:::

The key to testable Spark code is structure. Keep **transformations** as plain functions that take and return DataFrames, and keep reading, writing and configuration at the edges:

```python
# jobs/sales.py
from pyspark.sql import DataFrame, functions as F

def daily_revenue(orders: DataFrame) -> DataFrame:
    return (orders.filter(F.col("status") == "paid")
                  .groupBy("order_date")
                  .agg(F.sum("amount").alias("revenue")))
```

Then test with **pytest** and a local session shared by all tests (creating a session takes seconds):

```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")      # 200 tiny tasks would slow tests
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()

# tests/test_sales.py
from pyspark.testing import assertDataFrameEqual
from jobs.sales import daily_revenue

def test_daily_revenue_ignores_unpaid(spark):
    orders = spark.createDataFrame(
        [("2026-11-01", "paid", 10.0), ("2026-11-01", "refunded", 5.0),
         ("2026-11-01", "paid", 2.5)],
        "order_date string, status string, amount double")
    expected = spark.createDataFrame([("2026-11-01", 12.5)],
                                     "order_date string, revenue double")
    assertDataFrameEqual(daily_revenue(orders), expected)
```

`assertDataFrameEqual` and `assertSchemaEqual` (in `pyspark.testing` since Spark 3.5) compare rows ignoring order by default and show a readable diff. Before that, teams used libraries such as **chispa** (Python) or **spark-testing-base** (Scala, Java and Python).

**Levels of testing:**

| Level | What it checks | How |
|---|---|---|
| Unit | One transformation's logic, edge cases (nulls, duplicates, empty input) | pytest, local session, tiny DataFrames |
| Integration | Reading and writing real formats and tables (Parquet, Delta, Iceberg) | Local session writing to a temporary directory |
| End-to-end | The deployed job on real infrastructure | A small serverless batch in a test project |
| Data quality | The real output in production | Assertions or quality checks after each run |

**In CI** (GitHub Actions, Cloud Build) the runner needs a Java runtime (17 or 21 for Spark 4) and `pip install pyspark`. Run unit tests on every pull request; run integration tests that submit a serverless batch on merges.

> **Pitfall:** Tests that create a `SparkSession` per test, or keep the default 200 shuffle partitions, take minutes instead of seconds. Use one session-scoped fixture and small partition counts.

> **Pitfall:** Notebooks are hard to test. Move logic into an importable module and keep the notebook as a thin caller.

::: guide
For project layout, fixtures, property-based tests and deployment pipelines, read [Testing PySpark code](pyspark-streaming-kafka-topic-map.html#testing) and [Code organization, CI/CD and orchestration](pyspark-streaming-kafka-topic-map.html#codeorg) in the PySpark + Kafka guide.
:::

::: check
?? Why should transformations be functions that take and return DataFrames?
So they can be tested with small in-memory DataFrames, without any real input files, output tables or cluster configuration.

?? Why set `spark.sql.shuffle.partitions` to a small number in tests?
Each shuffle would otherwise create 200 tasks for a few rows, and task scheduling overhead would dominate test time.
:::

::: resources
- Docs: [Testing PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html) · the official guide to `pyspark.testing` and pytest
- Repo: [chispa](https://github.com/MrPowers/chispa) · readable DataFrame equality assertions for PySpark
- Repo: [spark-testing-base](https://github.com/holdenk/spark-testing-base) · Holden Karau's base classes for testing Spark in Scala, Java and Python
- Docs: [pytest fixtures](https://docs.pytest.org/en/stable/how-to/fixtures.html) · session-scoped fixtures and how sharing works
- Docs: [Submit a Spark batch workload](https://docs.cloud.google.com/dataproc-serverless/docs/quickstarts/spark-batch) · running an end-to-end test as a serverless batch
:::

### Databricks — Photon Engine & Delta Engine

::: plain
Open-source Spark is the engine design everyone can build. **Databricks** sells its own tuned version of that engine, plus a new high-performance part called **Photon** that replaces some of the original machinery. Your code stays the same Spark code; the managed platform makes it run faster and handles more of the maintenance for you, for a price.
:::

Databricks, founded by Spark's creators, runs Spark as a managed platform on the major clouds, including Google Cloud (Databricks on Google Cloud runs on GKE in your project). Its **Databricks Runtime** (DBR) is a distribution of Spark with extra features:

- **Photon**, a native vectorized query engine written in C++. Instead of generating Java code per query (WSCG), Photon processes columnar batches with precompiled, SIMD-friendly kernels. Catalyst still plans the query; operators that Photon supports run in Photon, and the rest fall back to normal Spark. The SIGMOD 2022 paper explains why: a JVM engine struggled with modern hardware, and interpreted vectorized execution was easier to build, observe and extend than code generation.
- **Delta Lake integration** and table maintenance: deletion vectors, liquid clustering, automatic file compaction and **predictive optimization**, which schedules `OPTIMIZE` and `VACUUM` for you.
- **Unity Catalog**, the governance layer for tables, files, models and permissions. An open-source version is a Linux Foundation project.

"**Delta Engine**" was the 2020 marketing name for the combination of Photon, an improved optimizer and caching on Delta tables. It is no longer a separate product: today you choose a Databricks Runtime version and whether to enable Photon.

| | Open-source Spark | Databricks Runtime |
|---|---|---|
| Execution | JVM, whole-stage codegen | JVM plus Photon (C++, vectorized) |
| Table format | Any (Parquet, Iceberg, Delta, Hudi) | Delta first; Iceberg reads and writes through Unity Catalog |
| Governance | Your catalog (Hive Metastore, Polaris, Lakehouse runtime catalog) | Unity Catalog |
| Operations | You or your managed service run clusters | Managed clusters, serverless compute, jobs |
| Cost | Infrastructure only | Infrastructure plus DBUs (Databricks units) |

The open-source world is building similar engines: **Apache Gluten** with Velox and **Apache DataFusion Comet** plug a native vectorized engine into ordinary Spark, and on Google Cloud, Managed Service for Apache Spark offers Google's own accelerated runtime in its premium tier.

> **Pitfall:** Photon speeds up SQL and DataFrame operators. It does nothing for Python UDFs or RDD code, and an unsupported expression in the middle of a plan forces costly transitions back to row-based Spark. Check the query profile for operators that did not run in Photon.

> **Tip:** Compare cost, not only run time. A job that runs 2 times faster with Photon but costs more per hour may or may not be cheaper; measure it on your workload.

::: guide
For how native engines plug into Spark, and table-format optimizations that apply on any platform, read [Tungsten, code generation and columnar execution](pyspark-streaming-kafka-topic-map.html#tungsten) and [Storage and table-format optimization](pyspark-streaming-kafka-topic-map.html#storageopt) in the PySpark + Kafka guide.
:::

::: check
?? How does Photon's execution model differ from whole-stage code generation?
Photon runs precompiled C++ kernels over columnar batches (interpreted vectorized execution); WSCG generates and compiles Java code per query that processes one row at a time in a fused loop.

?? Does enabling Photon speed up a job dominated by Python UDFs?
No. Photon accelerates supported SQL and DataFrame operators; Python UDFs still run in Python workers.
:::

::: resources
- Docs: [Photon on Databricks](https://docs.databricks.com/gcp/en/compute/photon) · what Photon accelerates, limitations and how to enable it
- Paper: [Photon: A Fast Query Engine for Lakehouse Systems](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf) · Behm et al., SIGMOD 2022; design and evaluation
- Docs: [Databricks on Google Cloud](https://docs.databricks.com/gcp/en/) · how the platform runs on Google Cloud
- Repo: [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) · an open-source native accelerator for Spark
- Repo: [Unity Catalog](https://github.com/unitycatalog/unitycatalog) · the open-source catalog and governance layer
:::

## Streaming

Everything so far processed data that already sat in files. Spark can also process data that keeps arriving, using the same DataFrame API and the same engine. This last group is a short bridge to the Streaming systems section later in the course, which covers event time, windows and delivery guarantees across engines.

### Spark Structured Streaming — Internals

::: plain
Structured Streaming treats a stream as a table that never stops growing. You write an ordinary DataFrame query against it, and Spark re-runs that query every few seconds on just the new rows, adding the results to what it already computed. It writes down which rows it has processed in a notebook (the checkpoint), so if it crashes, it carries on exactly where it stopped.
:::

You declare a streaming query with `readStream` and `writeStream`; the code in between is ordinary DataFrame code:

```python
from pyspark.sql import functions as F

events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "bootstrap.my-kafka.europe-west1.managedkafka.my-proj.cloud.goog:9092")
          .option("subscribe", "clicks")
          .load()
          .select(F.from_json(F.col("value").cast("string"),
                              "user_id string, page string, ts timestamp").alias("e"))
          .select("e.*"))

counts = (events.withWatermark("ts", "10 minutes")
          .groupBy(F.window("ts", "5 minutes"), "page")
          .count())

query = (counts.writeStream
         .outputMode("append")
         .format("parquet")
         .option("path", "gs://my-bucket/page_counts/")
         .option("checkpointLocation", "gs://my-bucket/checkpoints/page_counts/")
         .trigger(processingTime="1 minute")
         .start())
```

**How it runs.** By default the engine uses **micro-batches**. For each trigger, the driver:

1. Asks the source for the latest available offsets (for Kafka, an offset per partition) and writes the planned range to the **offset log** in the checkpoint, before processing (a write-ahead log).
2. Runs the query as a normal Spark job over just that range, using Catalyst and Tungsten like any batch query. This is called incremental execution.
3. Reads and updates **state** for stateful operators (aggregations, deduplication, stream-stream joins) in the **state store**, which is versioned per micro-batch.
4. Writes the output to the sink, then records the batch in the **commit log**.

On restart, Spark compares the two logs: a batch that was planned but not committed is re-run with exactly the same offsets. With a replayable source (Kafka, files) and an idempotent or transactional sink (file sink, Delta, Iceberg, or `foreachBatch` with `MERGE`), the result is **exactly-once** output.

**Triggers:** `processingTime="1 minute"` (a micro-batch every minute), `availableNow=True` (process everything available in several batches, then stop: incremental batch jobs), and the experimental **continuous** mode (millisecond latency, at-least-once, few operators). `once=True` is deprecated in favour of `availableNow`.

**State stores.** The default store keeps state in executor memory and snapshots it to the checkpoint location. The **RocksDB** state store (`spark.sql.streaming.stateStore.providerClass` set to the RocksDB provider) keeps state on local disk, handles far larger state with less GC, and supports **changelog checkpointing** that uploads only changes each batch.

**Watermarks** bound state. The watermark is the maximum event time seen so far minus the allowed delay (10 minutes above). Windows that end before the watermark are final: they are emitted in append mode and their state is dropped; events older than the watermark may be ignored.

> **Pitfall:** The checkpoint is tied to the query. Changing the stateful parts of a query (grouping keys, aggregations, the source) and restarting on the same checkpoint can fail or give wrong results. Plan changes; sometimes a new checkpoint and a backfill is the right answer.

> **Tip:** On Google Cloud, a long-running streaming job fits a Managed Service for Apache Spark cluster; incremental `availableNow` jobs fit serverless batches scheduled by Managed Service for Apache Airflow (formerly Cloud Composer). For a fully managed streaming service with autoscaling, Dataflow (Apache Beam) is the other option, covered in the next section.

::: guide
For triggers, state stores, watermarks and recovery in depth, read [Streaming fundamentals](pyspark-streaming-kafka-topic-map.html#ssbasics), [Event time, watermarks and windows](pyspark-streaming-kafka-topic-map.html#eventtime), [State store internals](pyspark-streaming-kafka-topic-map.html#statestore) and [Checkpointing, fault tolerance and exactly-once](pyspark-streaming-kafka-topic-map.html#ft) in the PySpark + Kafka guide.
:::

::: check
?? Why does Spark write the offset range to the offset log before processing a micro-batch?
So that, after a crash, it can re-run an uncommitted batch with exactly the same input range, which (with a replayable source and idempotent sink) gives exactly-once results.

?? What does `trigger(availableNow=True)` do, and when is it useful?
It processes all data available at start, possibly in several micro-batches, then stops. It turns a streaming query into an incremental batch job that can be scheduled, keeping checkpoint-based exactly-once tracking.

?? What does a watermark of 10 minutes allow Spark to do?
Treat windows that end more than 10 minutes before the latest event time seen as final: emit them (in append mode), drop their state and ignore later events for them.
:::

::: resources
- Docs: [Structured Streaming programming guide](https://spark.apache.org/docs/latest/streaming/index.html) · the official guide: sources, sinks, triggers, state and watermarks
- Paper: [Structured Streaming: a declarative API for real-time applications](https://people.eecs.berkeley.edu/~matei/papers/2018/sigmod_structured_streaming.pdf) · Armbrust et al., SIGMOD 2018; design and incremental execution
- Docs: [Structured Streaming and Kafka integration](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html) · offsets, options and exactly-once with Kafka
- Blog: [Performance improvements for stateful pipelines](https://www.databricks.com/blog/performance-improvements-stateful-pipelines-apache-spark-structured-streaming) · RocksDB state store and changelog checkpointing
- Book: [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/) · chapter 8 is a clear introduction to Structured Streaming
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, setting or rule of thumb |
|---|---|
| Stages | One per shuffle boundary; count `Exchange` nodes in `explain()` |
| Parallel tasks | Executors × cores per executor |
| Submit to a cluster | `gcloud dataproc jobs submit pyspark job.py --cluster=C --region=R` |
| Serverless batch | `gcloud dataproc batches submit pyspark job.py --region=R` |
| See the plan | `df.explain(mode="formatted")`; `*(n)` marks codegen stages |
| AQE | On by default since 3.2; set shuffle partitions high and let it coalesce |
| Target partition size | About 64 to 200 MB per task (`advisoryPartitionSizeInBytes`) |
| Broadcast join | `broadcast(df)` or `/*+ BROADCAST(t) */`; default threshold 10 MB |
| Skew signal | Max task duration or shuffle read far above the median |
| Skew fixes | Filter nulls, broadcast, AQE skew join, salt hot keys |
| Unified memory | (heap − 300 MB) × `spark.memory.fraction` (0.6) |
| Container killed for memory | Raise `spark.executor.memoryOverhead`, not the heap |
| Executor size | 4 to 5 cores, tens of GB; more executors beat one giant heap |
| GC check | GC time above about 10% of task time needs attention; G1 is the default |
| Python speed | Built-in functions, then pandas UDFs, then Arrow UDFs, then classic UDFs |
| Spark Connect | `SparkSession.builder.remote("sc://host:15002")`; no RDDs or `SparkContext` |
| Kubernetes scratch space | Volumes named `spark-local-dir-*`; shuffle tracking for dynamic allocation |
| Unit tests | Session-scoped `local[2]` fixture, 2 shuffle partitions, `assertDataFrameEqual` |
| Streaming exactly-once | Replayable source + checkpoint + idempotent or transactional sink |
| Incremental batch | `trigger(availableNow=True)` |

### Practice

::: try
**1. Count the stages.** How many stages does this job have, and where are the boundaries? `orders.filter(...).join(customers, "customer_id").groupBy("country").agg(F.sum("amount")).write.parquet(...)`, where both tables are large and no broadcast happens.
:::

?? Answer to 1
Four in the plan. One stage reads and filters `orders` and writes shuffle output for the join; one reads `customers` and writes shuffle output; one performs the sort-merge join and the partial aggregation and writes shuffle output by `country`; the last performs the final aggregation and writes Parquet. With AQE, the stages after the shuffles may run with coalesced partitions, and if the filtered `orders` side turned out small, the join could become a broadcast join.

::: try
**2. Size the memory.** An executor has `spark.executor.memory=16g` and 4 cores, with default settings. Compute the memory overhead, unified memory and the approximate unified memory per concurrent task.
:::

?? Answer to 2
Overhead: 10% of 16 GB, about 1.6 GB (above the 384 MB minimum), so the container asks for about 17.6 GB. Unified memory: (16,384 − 300) × 0.6 ≈ 9,650 MB. Per task with 4 cores: about 2.4 GB of execution and storage memory.

::: try
**3. Diagnose the stage.** A join stage has 400 tasks. The median task reads 90 MB of shuffle data and takes 12 seconds; the maximum reads 38 GB and takes 50 minutes. The join key is `merchant_id`, and the other side is a 2 GB table. What is happening, and what would you try, in order?
:::

?? Answer to 3
Skew: one `merchant_id` (or null) holds a huge share of rows. First find the key with a `groupBy(...).count()`. If it is null, filter nulls before the join. Check that AQE skew join is on and that this partition qualifies (38 GB is far above 5 × 90 MB and above 256 MB, so it should be split; if it was not, look at whether the join is a sort-merge join, since AQE only splits those). A 2 GB table is too large to broadcast safely, so the next fix is salting only the hot merchant: process its rows in a separate branch with a salted join and `union` the results.

::: try
**4. Make it testable.** Rewrite this notebook cell as a function that can be unit tested, and write one test for it: `df = spark.read.parquet("gs://raw/events/"); df = df.dropDuplicates(["event_id"]).filter("event_type = 'purchase'"); df.write.parquet("gs://clean/purchases/")`.
:::

?? A solution to 4

```python
# jobs/purchases.py
from pyspark.sql import DataFrame, functions as F

def clean_purchases(events: DataFrame) -> DataFrame:
    return (events.dropDuplicates(["event_id"])
                  .filter(F.col("event_type") == "purchase"))

# tests/test_purchases.py (uses the session-scoped spark fixture)
from pyspark.testing import assertDataFrameEqual
from jobs.purchases import clean_purchases

def test_dedupes_and_keeps_purchases(spark):
    events = spark.createDataFrame(
        [("e1", "purchase"), ("e1", "purchase"), ("e2", "view")],
        "event_id string, event_type string")
    expected = spark.createDataFrame([("e1", "purchase")],
                                     "event_id string, event_type string")
    assertDataFrameEqual(clean_purchases(events), expected)
```

The read and write stay in a thin `main` that calls `clean_purchases`.

::: try
**5. Choose the trigger.** A team needs click counts per page in Cloud Storage, refreshed every hour, and wants to pay as little as possible. Should they run a continuous streaming query with `processingTime="1 hour"`, or something else? Explain.
:::

?? Answer to 5
Use `trigger(availableNow=True)` and schedule the job hourly (for example as a serverless batch started by Managed Airflow). It processes only the new Kafka offsets since the last run, keeps exactly-once tracking through the checkpoint, and uses no compute between runs. A permanently running query with an hourly trigger would hold executors idle for most of the hour.

### What comes next

You now know how Spark plans, shuffles, joins, remembers and streams, and where to look when it misbehaves. The next section, **Flink, Beam, Trino and other engines**, puts Spark in context: Apache Flink for true record-at-a-time streaming, Apache Beam and Dataflow for unified batch and streaming pipelines on Google Cloud, Trino for interactive SQL over many sources, and the newer single-node engines such as DuckDB and Polars that make a cluster unnecessary for many jobs.
