This section is about the processing engines a data engineer meets beside Spark: **Apache Flink** for low-latency, stateful stream processing, **Apache Beam** for writing a pipeline once and running it on several engines (on Google Cloud, on Dataflow), and **Trino** for interactive SQL across many data sources at once. It is for engineers who already know one engine well and now need to choose between engines, or to run a pipeline someone else built on one of them. It assumes [Apache Spark](#spark) (driver and executors, DAGs, shuffles, Structured Streaming micro-batches), file formats and table formats from [Data lakes and lakehouses](#lakehouse), and the Hive Metastore and YARN from [The Hadoop ecosystem](#hadoop). The streaming concepts used here (event time, windows, delivery guarantees, backpressure) are introduced where they are needed and treated across all engines in [Streaming systems](#streaming).

**What you will be able to do afterwards**

- Describe Flink's runtime (JobManager, TaskManagers, slots, operator chaining) and explain how aligned and unaligned checkpoints and savepoints let a job survive failures and upgrades.
- Choose a Flink state backend, size and bound state with TTL, rescale a stateful job, and evolve state schemas safely.
- Build streaming ETL with Flink SQL and Flink CDC: changelog streams, dynamic tables, temporal joins and upsert sinks.
- Write an Apache Beam pipeline with PCollections, PTransforms, windows and triggers, and run the same code on the Direct runner, Flink or Dataflow.
- Explain how watermarks are generated and propagated, and route late data deliberately in Flink, Beam and Spark.
- Explain Trino's coordinator and worker design, its memory limits and fault-tolerant execution, and design federated queries that push work down to the sources.

**The big picture.** Every engine in this section turns a program into a **dataflow graph**: operators connected by data channels, split into parallel instances spread over machines. They differ in three choices. **When does data move?** Spark mostly runs stages one after another and moves data at stage boundaries; Flink keeps all operators running at once and streams each record through the whole graph; Trino pipelines pages of rows through in-memory operators for one query and then stops. **Where does state live?** Flink keeps large, long-lived state inside the engine and snapshots it; Trino keeps almost none beyond one query; Beam leaves it to the runner. **Who owns the data?** Flink and Beam read and write external systems; Trino owns no storage at all and reads other systems through connectors. Once you can place an engine on those three axes, its strengths, its failure modes and its configuration knobs make sense.

[[fig:engines_landscape]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Apache Flink

Spark's Structured Streaming treats a stream as a series of small batches. Apache Flink starts from the opposite end: everything is a stream, a batch is just a stream that happens to end, and operators run continuously, handing each record to the next operator as soon as it is ready. That design gives Flink millisecond latency and, more importantly, first-class **state**: counts, windows, join buffers and deduplication sets that can grow to terabytes and still be recovered exactly after a crash.

This group goes from the runtime (how a job is deployed, scheduled and checkpointed), to the state that the checkpoints protect, to the SQL layer and the change data capture (CDC) connectors that most new Flink pipelines are written with today.

### Apache Flink — Architecture & Internals

::: plain
Picture an assembly line that never stops. Events roll in at one end, pass through stations (operators) that filter, count and join them, and results roll out at the other end one at a time, without waiting for a batch to fill up. A foreman (the JobManager) plans the line and hands out work; the workers (TaskManagers) each have a few workbenches (slots). Every few seconds the foreman sends a marker down the line, and each station photographs its workbench as the marker passes, so that after a power cut the whole line can restart from the last complete set of photos.
:::

Apache Flink is a distributed engine for **stateful computations over unbounded and bounded streams**. You write a program that reads from sources, transforms the data and writes to sinks; Flink turns it into a graph of parallel tasks that run for as long as the job runs, often months. Here is a small job in the Java DataStream API (Flink 2.x) that counts clicks per page:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(30_000);                       // snapshot state every 30 s

KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("bootstrap.events.europe-west1.managedkafka.my-proj.cloud.goog:9092")
    .setTopics("clicks")
    .setGroupId("click-counter")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();                                          // plus SASL settings for Managed Kafka

env.fromSource(source, WatermarkStrategy.noWatermarks(), "clicks")
   .map(line -> Tuple2.of(line.split(",")[0], 1L))     // (page, 1)
   .returns(Types.TUPLE(Types.STRING, Types.LONG))
   .keyBy(t -> t.f0)                                   // route each page to one subtask
   .sum(1)                                             // running count per page (keyed state)
   .print();

env.execute("click-counter");
```

**From program to running tasks.** The program builds a logical graph (source, map, keyBy plus sum, print). Flink then **chains** operators that can run in the same thread without a network hop: the source and the `map` become one task, because records pass between them as plain method calls. `keyBy` cannot be chained, because each record must be sent to the subtask that owns its key, so it becomes a network exchange (a shuffle, in Spark's terms). With parallelism 4 the job has two tasks of 4 **subtasks** each, connected all-to-all.

**The processes.**

- The **client** compiles the program into a `JobGraph` and submits it. In application mode (below) the program's `main()` runs on the cluster instead.
- The **JobManager** coordinates. It has three parts: the **ResourceManager** acquires and hands out task slots (from Kubernetes, YARN or a fixed pool); the **Dispatcher** offers the REST API and web UI and starts one **JobMaster** per job; the JobMaster schedules that job's subtasks, watches them, and runs the **checkpoint coordinator**.
- **TaskManagers** are the worker JVMs. Each offers a fixed number of **task slots** (`taskmanager.numberOfTaskSlots`, default 1). A slot is a fixed share of the TaskManager's managed memory; CPU is not isolated between slots.

**Slot sharing.** By default one slot may hold one subtask of *every* operator of a job, so a whole slice of the pipeline lives in one slot. A job therefore needs as many slots as its highest operator parallelism, not the sum. Worked numbers: a job whose largest operator has parallelism 12, on TaskManagers with 4 slots each, needs 3 TaskManagers.

[[fig:engines_flink_arch]]

**Checkpoints.** Flink's fault tolerance rests on **checkpoint barriers**, small markers that the sources inject into the data stream at the checkpoint interval. A barrier separates the records that belong to checkpoint *n* from those that belong to *n+1*. When an operator receives barrier *n* on its input, it snapshots its state asynchronously to durable storage (Cloud Storage, for a job on Google Cloud) and forwards the barrier. When every sink has acknowledged, the checkpoint is complete. On failure, Flink restarts the tasks, loads the last completed checkpoint, and rewinds the sources to the offsets stored in it, so every record affects the state exactly once.

An operator with several inputs (after a `keyBy` it receives from every upstream subtask) has a choice:

- **Aligned checkpoints**: when the barrier arrives on one input, the operator stops reading that input and keeps processing the others until their barriers arrive too, then snapshots. The snapshot is a clean cut, but a slow input delays the checkpoint, and under backpressure barriers can wait minutes behind full buffers.
- **Unaligned checkpoints**: the first barrier to arrive overtakes the buffered records, the operator snapshots immediately, and the records still **in flight** (in input and output buffers) are written into the checkpoint as well. Checkpoints stay fast under backpressure at the cost of larger checkpoints.

This simulation shows that both produce the same consistent cut:

```python
# One operator with two input channels and checkpoint barrier 1 ("|").
# The operator keeps a running count of records (its state).
A = ["a1", "a2", "|", "a3", "a4"]
B = ["b1", "b2", "b3", "b4", "|", "b5"]

def aligned(a, b):
    qa, qb, count, blocked, held = list(a), list(b), 0, set(), []
    while qa or qb:
        for name, q in (("A", qa), ("B", qb)):
            if not q:
                continue
            rec = q.pop(0)
            if rec == "|":
                blocked.add(name)
                if blocked == {"A", "B"}:
                    print(f"aligned: snapshot count={count}, then replay held {held}")
                    count += len(held); held = []; blocked = set()
            elif name in blocked:
                held.append(rec)          # channel already passed its barrier: wait
            else:
                count += 1
    return count

def unaligned(a, b):
    qa, qb, count, snapped = list(a), list(b), 0, False
    while qa or qb:
        for q in (qa, qb):
            if not q:
                continue
            rec = q.pop(0)
            if rec == "|":
                if not snapped:
                    other = qb if q is qa else qa
                    in_flight = other[:other.index("|")]
                    print(f"unaligned: snapshot count={count} plus in-flight {in_flight}")
                    snapped = True
            else:
                count += 1
    return count

print("final", aligned(A, B))
print("final", unaligned(A, B))
# aligned: snapshot count=6, then replay held ['a3', 'a4']
# final 9
# unaligned: snapshot count=4 plus in-flight ['b3', 'b4']
# final 9
```

The aligned snapshot holds 6 (everything before both barriers). The unaligned snapshot holds 4 plus the two in-flight records; restoring it means loading 4 and replaying `b3` and `b4`, which reaches the same 6. In configuration (Flink 2.x reads `config.yaml`):

```yaml
execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.dir: gs://my-flink-bucket/checkpoints/click-counter
execution.checkpointing.unaligned.enabled: true
execution.checkpointing.aligned-checkpoint-timeout: 10s   # start aligned, switch if slow
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
```

**Savepoints** use the same mechanism but are triggered and owned by you, not by Flink. You take one before an upgrade, a rescale or a migration, then start the new version from it:

```bash
flink stop --savepointPath gs://my-flink-bucket/savepoints <jobId>   # savepoint, then stop
flink run -s gs://my-flink-bucket/savepoints/savepoint-3f2a1b-0123456789ab \
  -p 8 click-counter.jar                                             # resume with parallelism 8
```

| | Checkpoint | Savepoint |
|---|---|---|
| Purpose | Automatic recovery from failures | Planned upgrades, rescaling, migration |
| Triggered by | Flink, on an interval | You (CLI, REST, Kubernetes operator) |
| Lifetime | Deleted when superseded (unless retained) | Kept until you delete it |
| Format | Backend-specific, may be incremental | Canonical (portable) or native |

> **Pitfall:** Set a stable `.uid("dedup-by-event-id")` on every stateful operator. Flink maps saved state to operators by these IDs; without them it generates IDs from the graph shape, and adding one operator can make the old state impossible to restore.

**Deploying.** In **application mode** each application gets its own JobManager and the program's `main()` runs there; this is the production default. In **session mode** several jobs share one long-running cluster, which is fine for development and SQL exploration but lets one job's failure affect the others. (The old per-job mode was removed in Flink 2.0.) On Google Cloud, run Flink on GKE with the **Flink Kubernetes Operator**, which manages `FlinkDeployment` resources, upgrades jobs through savepoints and restarts failed ones; checkpoints go to Cloud Storage through Flink's `gs://` file system plugin. Flink is also available as an optional component on Managed Service for Apache Spark (formerly Dataproc) clusters, where it runs on YARN.

::: deeper
**Why barriers give exactly-once.** The algorithm is an adaptation of the Chandy-Lamport distributed snapshot, called asynchronous barrier snapshotting. Because barriers flow with the data and sources record their offsets in the same checkpoint, the saved state and the saved read positions always match. Exactly-once applies to Flink's *state*; for exactly-once *output* the sink must also take part, through idempotent writes or a two-phase commit tied to checkpoint completion ([Streaming systems](#streaming) covers both). In `AT_LEAST_ONCE` mode operators do not align, so after recovery some records are applied twice, in exchange for lower latency.

**Failover granularity.** `jobmanager.execution.failover-strategy: region` (the default) restarts only the **pipelined region** containing the failed task. In batch jobs, regions are separated by blocking exchanges and are small; in a streaming job with a `keyBy`, every task is connected to every other, so the region is usually the whole job. Restart strategies (`fixed-delay`, `exponential-delay`, `failure-rate`) decide how often and how fast Flink retries before failing the job.

**High availability.** The JobManager is a single point of failure unless HA is on. With Kubernetes HA (or ZooKeeper), the JobManager stores the job graph and a pointer to the latest checkpoint in durable storage, and a new JobManager picks them up after a crash.

**Batch on the same runtime.** With `execution.runtime-mode: BATCH`, Flink uses blocking exchanges, sorts instead of keeping state for keyed operations, and schedules stage by stage, like Spark. The adaptive batch scheduler can even choose each operator's parallelism from the data size.

**Elasticity.** The **adaptive scheduler** can run a job with fewer slots than requested and scale up when more arrive; **reactive mode** (`scheduler-mode: reactive`) in standalone application clusters makes the job's parallelism follow the number of TaskManagers, so an autoscaler that adds or removes pods effectively rescales the job. The Kubernetes operator also has its own autoscaler that adjusts per-operator parallelism from busy-time and backlog metrics.

**Flink 2.0 (2025)** removed the DataSet API, the Scala APIs, per-job mode and the legacy `SourceFunction`/`SinkFunction` interfaces, made Java 17 the default, replaced `flink-conf.yaml` with a standard YAML `config.yaml`, and introduced disaggregated state (next topic).

> **Interview:** "Flink or Spark Structured Streaming?" Spark's micro-batches give second-level latency and reuse your Spark batch code and team; Flink gives millisecond latency, per-record event-time processing with timers, very large keyed state with incremental checkpoints, and finer control (custom windows, side outputs, savepoints for upgrades). Many companies use Spark for batch and Flink for the latency-critical or state-heavy streams.
:::

::: check
?? A job's operators have parallelism 6, 6 and 10, and each TaskManager has 4 slots. How many TaskManagers does it need with default slot sharing?
Three. With slot sharing a job needs as many slots as its highest parallelism (10), and 10 slots at 4 per TaskManager rounds up to 3.

?? What is the difference between aligned and unaligned checkpoints?
Aligned checkpoints pause each input that has delivered the barrier until all inputs have, then snapshot only operator state. Unaligned checkpoints snapshot as soon as the first barrier arrives and also store the in-flight records, so they stay fast under backpressure but are larger.

?? Why should every stateful operator have an explicit `uid`?
Savepoint state is matched to operators by ID. Generated IDs change when the job graph changes, so without explicit IDs an upgraded job may not be able to restore its state.
:::

::: resources
- Docs: [Flink architecture](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/) · JobManager, TaskManagers, slots and deployment modes from the official docs
- Docs: [Checkpoints vs savepoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints_vs_savepoints/) · ownership, formats and when to use each
- Docs: [Checkpointing under backpressure](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpointing_under_backpressure/) · unaligned checkpoints and buffer debloating
- Book: [Stream Processing with Apache Flink](https://www.oreilly.com/library/view/stream-processing-with/9781491974285/) · Hueske and Kalavri; the architecture and fault-tolerance chapters
- Course: [Learn Flink: hands-on training](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/overview/) · the official tutorial with exercises
- Docs: [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) · running Flink applications on Kubernetes, including GKE
- Paper: [Lightweight asynchronous snapshots for distributed dataflows](https://arxiv.org/abs/1506.08603) · Carbone et al.; the checkpoint algorithm
:::

### Flink State Management

::: plain
State is an operator's memory. A counter that adds up clicks per page must remember the current total for every page; a deduplicator must remember which event IDs it has already seen. Flink gives each operator a notebook for this, with one page per key, keeps the notebook either in memory or on local disk, and photocopies it to safe storage at every checkpoint. Managing state well means choosing where the notebook lives, tearing out old pages before it gets too thick, and making sure next year's version of the job can still read this year's notebook.
:::

Most useful streaming logic is stateful: windows hold events until they close, joins buffer one side while waiting for the other, deduplication remembers IDs, fraud rules remember a customer's recent behaviour. In Flink this state is **local**: it lives in the same process as the operator, so reading it costs a memory or local disk access, not a network call to a database. Flink then makes it durable through checkpoints and redistributes it when you rescale.

**Two kinds of state.**

| | Keyed state | Operator state |
|---|---|---|
| Scope | One value per key, after `keyBy` | One value per parallel operator instance |
| Typical users | Windows, aggregations, dedup, joins, timers | Sources (offsets per partition), some sinks |
| Types | `ValueState`, `ListState`, `MapState`, `ReducingState`, `AggregatingState` | `ListState` (split or union on restore), `BroadcastState` |
| On rescale | Moves with its key groups | List items are redistributed |

Keyed state is the one you will write most often. This deduplicator keeps one flag per event ID and lets Flink expire it after 24 hours:

```java
public class Dedup extends KeyedProcessFunction<String, Event, Event> {
  private transient ValueState<Boolean> seen;

  @Override
  public void open(OpenContext ctx) {
    StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(24))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();
    ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
    desc.enableTimeToLive(ttl);
    seen = getRuntimeContext().getState(desc);
  }

  @Override
  public void processElement(Event e, Context ctx, Collector<Event> out) throws Exception {
    if (seen.value() == null) {        // first time this event ID appears
      seen.update(true);
      out.collect(e);
    }
  }
}
// events.keyBy(Event::eventId).process(new Dedup()).uid("dedup-by-event-id")
```

The `seen` handle looks like a single variable, but Flink scopes it to the key of the record being processed: inside `processElement` for event `e-17`, `seen.value()` reads the flag stored for `e-17`.

**Key groups and rescaling.** Flink does not assign keys directly to subtasks. It hashes each key into one of a fixed number of **key groups**, equal to the job's **max parallelism** (by default derived from the parallelism, at least 128), and gives each subtask a contiguous range of key groups. When you rescale from a savepoint, whole key groups move between subtasks, and each new subtask reads only the ranges it now owns:

```python
import zlib

MAX_PARALLELISM = 128                  # number of key groups, fixed for the job's lifetime

def key_group(key):
    # Flink applies a murmur hash to key.hashCode(); crc32 stands in for it here
    return zlib.crc32(key.encode()) % MAX_PARALLELISM

def subtask(group, parallelism):
    return group * parallelism // MAX_PARALLELISM

for p in (4, 3):
    ranges = {}
    for g in range(MAX_PARALLELISM):
        lo_hi = ranges.setdefault(subtask(g, p), [g, g])
        lo_hi[1] = g
    print(f"parallelism {p}:", {i: tuple(r) for i, r in ranges.items()})

g = key_group("user-42")
print(g, subtask(g, 4), subtask(g, 3))
# parallelism 4: {0: (0, 31), 1: (32, 63), 2: (64, 95), 3: (96, 127)}
# parallelism 3: {0: (0, 42), 1: (43, 85), 2: (86, 127)}
# 115 3 2
```

The key never changes key group; only the owner of the group changes. That is also why you can never scale beyond the max parallelism, and why changing max parallelism breaks restoring from old state: every key would land in a different group. Set it explicitly (`pipeline.max-parallelism`) for long-lived jobs, with room to grow.

**State backends** decide where working state lives:

| Backend | Working state | Strengths | Limits |
|---|---|---|---|
| `hashmap` | Java objects on the JVM heap | Fastest access, no serialization per access | State must fit in memory; large heaps mean long GC pauses; full snapshots |
| `rocksdb` | Serialized in an embedded RocksDB on local disk | State far larger than memory; **incremental checkpoints** | Every read and write serializes; needs fast local SSDs |
| `forst` | Remote storage (such as Cloud Storage) with a local cache | Disaggregated: tiny local disks, fast rescaling and recovery | Introduced in Flink 2.0 as experimental; works best with the asynchronous state API |

```yaml
state.backend.type: rocksdb
execution.checkpointing.incremental: true
execution.checkpointing.dir: gs://my-flink-bucket/checkpoints/orders-enrich
```

**Incremental checkpoints** exploit RocksDB's structure. RocksDB is a log-structured merge tree ([Storage internals and hardware](#hardware)): it writes immutable sorted files (SST files) and merges them in the background. A checkpoint flushes memory to a new SST file and uploads only the SST files the previous checkpoint did not already have; unchanged files are referenced, not copied. Worked numbers: with 500 GB of state and 5 GB of changes per interval, each checkpoint uploads roughly 5 GB plus compaction output instead of 500 GB.

**State TTL.** Unbounded keys (event IDs, session IDs, user IDs that churn) make state grow for ever. A TTL declares how long a value stays valid after it was written (or read, with `OnReadAndWrite`). Expired values are never returned when you configure `NeverReturnExpired`, and they are physically removed on access and in the background: by incremental cleanup for the heap backend and by a RocksDB compaction filter. TTL uses **processing time** only. In Flink SQL the equivalent is `table.exec.state.ttl` for the whole job, or the `STATE_TTL` hint per join or aggregation.

> **Pitfall:** A TTL changes results. If a deduplicator forgets IDs after 24 hours, a duplicate arriving 25 hours later passes through. Choose the TTL from the business rule ("duplicates arrive within minutes"), not from the size of the disk.

**State migration.** A job will change over time, and its state must still restore. Flink checks the new serializer against the one stored in the savepoint:

- **POJO types** can add and remove fields; **Avro** types can change in Avro-compatible ways (add fields with defaults, remove fields). Changing a field's type, or the class name, is not supported.
- Types serialized by **Kryo** (Flink's fallback for classes it cannot analyse) cannot evolve at all; disable the fallback (`pipeline.generic-types: false`) so you find out at development time.
- **Keys** cannot evolve: changing the key type or its serializer is not supported.
- For anything else (renaming state, splitting an operator, bootstrapping state from a table), the **State Processor API** reads a savepoint as a dataset, lets you transform it with a batch job, and writes a new savepoint.

> **Tip:** Treat state like a database schema. Keep stateful types as POJOs or Avro records, give every stateful operator a `uid`, and add a "restore from the last production savepoint" step to the CI pipeline, so an incompatible change fails a test rather than a deployment.

::: deeper
**RocksDB in practice.** By default Flink gives RocksDB the TaskManager's managed memory (`state.backend.rocksdb.memory.managed: true`) and shares one block cache and write buffer pool across all RocksDB instances in a slot, which keeps memory bounded. Performance problems usually come from slow disks (use local SSDs, not network disks, for the working directory), from very large values (a `ValueState<List<...>>` rewritten on every update: use `ListState` or `MapState`, which RocksDB can append to or update per entry), and from too many states per operator, each of which is a separate column family.

**Checkpoint size versus state size.** The web UI shows both. With incremental checkpoints, "checkpointed data size" is the delta and "full checkpoint data size" the total. A delta that keeps growing usually means state is not expiring. Compaction also rewrites files, so deltas are larger than the logical change.

**Disaggregated state (ForSt).** Flink 2.0 added a backend that keeps the primary copy of state in a distributed file system and uses local disk only as a cache, plus an **asynchronous state API** so an operator can issue many state reads in parallel instead of blocking on each. The motivation is cloud native operation: checkpoints become nearly free (the data is already remote), and rescaling or recovery does not need to download hundreds of gigabytes first.

**Timers** are state too. `ctx.timerService().registerEventTimeTimer(ts)` stores a timer per key; Flink checkpoints timers along with values. Windows, interval joins and session logic are all built on keyed state plus timers.

**Operator state redistribution.** On rescale, *even-split* list state divides the list items among the new subtasks (the Kafka source's partition offsets work like this), while *union* list state gives every subtask the full list and lets each choose. Broadcast state is replicated identically to all subtasks and is used for rules or configuration that every record must be checked against.

> **Interview:** "How does Flink rescale a stateful job from 4 to 3 subtasks?" Take a savepoint, restart with parallelism 3. Keys were hashed into a fixed number of key groups (max parallelism), each subtask owned a contiguous range, and after restart each new subtask reads only the key-group ranges assigned to it, so no key is split and nothing is re-hashed.
:::

::: check
?? Why can't you change a job's max parallelism and still restore its keyed state?
Max parallelism is the number of key groups. Changing it changes which group every key hashes to, so the saved state no longer lines up with the keys.

?? When would you choose the RocksDB backend over the heap (`hashmap`) backend?
When state is larger than memory, when GC pauses from a huge heap are a problem, or when you want incremental checkpoints for large state. The heap backend is faster when the state comfortably fits in memory.

?? A deduplication job's checkpoints grow by 2 GB every day for weeks. What is the likely cause and fix?
State is never removed because the keys are unbounded. Add a state TTL that matches the business rule for duplicates (or `table.exec.state.ttl` in SQL).
:::

::: resources
- Docs: [Stateful stream processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/) · keyed state, key groups and checkpointing concepts
- Docs: [Working with state](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/) · state types, TTL and operator state with code
- Docs: [State backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/) · hashmap, RocksDB and ForSt, with tuning options
- Docs: [State schema evolution](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/) · which changes are allowed for POJO and Avro state
- Paper: [State management in Apache Flink](https://www.vldb.org/pvldb/vol10/p1718-carbone.pdf) · Carbone et al., VLDB 2017; key groups, snapshots and rescaling
- Blog: [Managing large state in Apache Flink: incremental checkpointing](https://flink.apache.org/2018/01/30/managing-large-state-in-apache-flink-an-intro-to-incremental-checkpointing/) · how RocksDB SST files make checkpoints incremental
- Book: [Stream Processing with Apache Flink](https://www.oreilly.com/library/view/stream-processing-with/9781491974285/) · chapter 7 on stateful operators and chapter 10 on operating them
:::

### Flink CDC & Flink SQL Ecosystem

::: plain
A database already keeps a diary of every change it makes (the binlog in MySQL, the write-ahead log in PostgreSQL), so it can recover after a crash. **Change data capture** (CDC) reads that diary and turns each insert, update and delete into an event. Flink CDC reads the diary for you, and Flink SQL lets you treat the resulting stream as a table that keeps changing: you write ordinary SQL against it, and the answer keeps itself up to date as the source database changes.
:::

Most new Flink pipelines are not written in Java. They are SQL statements over **dynamic tables**: tables whose contents change over time, fed by streams and producing streams. [Streaming systems](#streaming) introduces streaming SQL, the changelog row kinds (`+I`, `-U`, `+U`, `-D`) and the four kinds of streaming join; this topic shows how CDC feeds Flink SQL and how to build a correct streaming ETL job from the two.

**Flink CDC** started at Ververica and Alibaba, was donated to the Apache Software Foundation in 2024, and is now an Apache Flink subproject (version 3.x). It offers two ways in:

1. **SQL source connectors** (`mysql-cdc`, `postgres-cdc`, `oracle-cdc`, `sqlserver-cdc`, `mongodb-cdc` and others) that you declare as tables in Flink SQL.
2. **YAML pipelines** that synchronise whole databases (many tables, with schema changes) into a sink such as Kafka, Iceberg, Paimon, StarRocks or Doris, without writing SQL per table.

A CDC table in Flink SQL:

```sql
CREATE TABLE orders_cdc (
  order_id    BIGINT,
  customer_id BIGINT,
  status      STRING,
  amount      DECIMAL(10, 2),
  updated_at  TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector'     = 'mysql-cdc',
  'hostname'      = '10.20.0.5',          -- Cloud SQL for MySQL, private IP
  'port'          = '3306',
  'username'      = 'flink_cdc',
  'password'      = '${MYSQL_CDC_PASSWORD}',  -- injected at deploy time from Secret Manager
  'database-name' = 'shop',
  'table-name'    = 'orders',
  'server-id'     = '5401-5404'           -- one ID per parallel reader
);
```

**How the source reads a live table without locking it.** A CDC job must first read the existing rows (the snapshot) and then follow the log, and the two must join seamlessly. Flink CDC's **incremental snapshot** algorithm (based on Netflix's DBLog design) splits the table into chunks by primary key and reads chunks in parallel. For each chunk it notes the log position just before and just after the chunk query, then replays the log entries between the two positions for keys in that chunk, so the chunk is consistent even though the table was changing while it was read. When all chunks are done, a single reader continues from the log. No global lock is taken, the snapshot is parallel, and a failure only re-reads the current chunk.

The output is a **changelog stream**. An `UPDATE` in MySQL becomes a `-U` row (the old values) followed by a `+U` row (the new values); a `DELETE` becomes `-D`. Downstream operators must handle all four kinds, which is easiest to see with an aggregation:

```python
# A CDC source emits row changes for table orders(order_id, status, amount).
# The query is: SELECT status, SUM(amount) FROM orders GROUP BY status
changes = [
    ("+I", 1, "NEW", 50),
    ("+I", 2, "NEW", 30),
    ("-U", 1, "NEW", 50),       # order 1 changes status: old row retracted...
    ("+U", 1, "PAID", 50),      # ...and the new row added
    ("-D", 2, "NEW", 30),       # order 2 is deleted
]
totals = {}
for op, order_id, status, amount in changes:
    sign = 1 if op in ("+I", "+U") else -1
    before = totals.get(status)
    totals[status] = totals.get(status, 0) + sign * amount
    print(f"{op} order {order_id}: {status} {before} -> {totals[status]}")
print(totals)
# +I order 1: NEW None -> 50
# +I order 2: NEW 50 -> 80
# -U order 1: NEW 80 -> 30
# +U order 1: PAID None -> 50
# -D order 2: NEW 30 -> 0
# {'NEW': 0, 'PAID': 50}
```

Without the `-U` row, the `NEW` total would stay at 80 for ever. This is why the old values matter, and why a CDC source must be configured to provide full before-images (MySQL `binlog_row_image=FULL`, PostgreSQL `REPLICA IDENTITY FULL` when the key alone is not enough).

**A streaming ETL job.** Read the CDC table, aggregate, and write an always-current result keyed by customer:

```sql
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '7 d';

CREATE TABLE paid_totals (
  customer_id BIGINT,
  paid_total  DECIMAL(12, 2),
  paid_orders BIGINT,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'paid-totals',
  'properties.bootstrap.servers' = 'bootstrap.events.europe-west1.managedkafka.my-proj.cloud.goog:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO paid_totals
SELECT customer_id, SUM(amount), COUNT(*)
FROM orders_cdc
WHERE status = 'PAID'
GROUP BY customer_id;
```

When an order changes from `PAID` to `REFUNDED`, the `-U` row still has `status = 'PAID'`, so it passes the filter and is subtracted; the `+U` row does not pass. The result stays correct with no special code. The **upsert-kafka** sink writes the latest value per primary key (and a tombstone for deletes), which a compacted Kafka topic, a database or a lakehouse table can apply.

To see what kind of stream each operator produces before deploying, ask the planner:

```sql
EXPLAIN CHANGELOG_MODE
SELECT customer_id, SUM(amount) FROM orders_cdc GROUP BY customer_id;
-- each node is annotated, for example changelogMode=[I,UB,UA,D]
```

**Whole-database pipelines.** For replication rather than transformation, a Flink CDC YAML pipeline is shorter and handles new tables and schema changes (added columns) automatically:

```yaml
source:
  type: mysql
  hostname: 10.20.0.5
  port: 3306
  username: flink_cdc
  password: ${MYSQL_CDC_PASSWORD}   # substituted by your deployment tooling
  tables: shop.\.*
  server-id: 5401-5404

sink:
  type: kafka
  properties.bootstrap.servers: bootstrap.events.europe-west1.managedkafka.my-proj.cloud.goog:9092

pipeline:
  name: shop-to-kafka
  parallelism: 4
```

Run it with `bin/flink-cdc.sh shop-to-kafka.yaml` against a Flink cluster. Pipelines also support `route` rules (merge sharded tables into one) and `transform` rules (projections, computed columns and filters).

**The rest of the ecosystem.** The **SQL Client** is an interactive shell; the **SQL Gateway** exposes Flink SQL over REST and a HiveServer2-compatible endpoint, so BI tools and orchestrators can submit statements. **Catalogs** store table definitions: the Hive catalog (backed by a Hive Metastore), JDBC catalogs, and the Iceberg and Paimon catalogs for lakehouse tables. **Apache Paimon** began as Flink Table Store and is a lakehouse format built for streaming upserts; Iceberg is the common choice when other engines (BigQuery, Spark, Trino) must read the result. **Materialized tables** (since Flink 1.20) let you declare a query and a freshness target and have Flink choose between a continuous job and scheduled refreshes.

**On Google Cloud**, the managed alternative to Flink CDC is **Datastream**, which reads MySQL, PostgreSQL, Oracle and SQL Server and writes to BigQuery or Cloud Storage with no cluster to run ([Data ingestion](#ingestion) compares the options). Choose Flink CDC when you need to transform, join or enrich the change stream before it lands, or to write to Kafka or Paimon. For Cloud SQL sources, MySQL needs binary logging (enabled with point-in-time recovery) and PostgreSQL needs the `cloudsql.logical_decoding` flag.

> **Pitfall:** Writing an updating result to an append-only sink (a plain Kafka topic, a file) fails at planning time in Flink, and if you work around it you get duplicates. Declare a primary key and use an upsert-capable sink.

> **Pitfall:** Each parallel MySQL CDC reader registers as a replica with its own `server-id`. Two jobs that share IDs, or an ID that clashes with a real replica, disconnect each other in a loop. Give every job its own range.

::: deeper
**ChangelogNormalize.** A source that only provides upserts (the latest row per key, as upsert-kafka does) cannot say what the *previous* value was. Flink inserts a `ChangelogNormalize` operator that keeps the last row for every key in state to generate `-U` rows. It is correct but expensive: state grows with the number of keys. CDC sources that provide full before-images do not need it.

**Non-determinism in updates.** If a query computes a column with a non-deterministic function (`NOW()`, a lookup join against a changing table) and the sink needs to retract that row later, the retraction may not match the row originally emitted. Flink's planner detects many such cases (`table.optimizer.non-deterministic-update.strategy`), and the sink upsert materializer (`table.exec.sink.upsert-materialize`) can repair ordering problems at the cost of extra state.

**Skewed aggregations.** A `GROUP BY` on a hot key sends most records to one subtask. **Mini-batch** aggregation (`table.exec.mini-batch.enabled`) buffers records briefly so each key's state is read and written once per batch, and **two-phase aggregation** pre-aggregates locally before the shuffle, like a combiner in MapReduce. For `COUNT(DISTINCT ...)`, `table.optimizer.distinct-agg.split.enabled` splits the work by a hash bucket first.

**One SQL, two modes.** The same statement runs in streaming mode (continuous, changelog output) or batch mode (`SET 'execution.runtime-mode' = 'batch'`, one final result). Backfills often run the batch version over the lakehouse history and then start the streaming version from the current log position.

> **Interview:** "Why does a CDC update arrive as two rows?" Downstream aggregations and joins need to undo the effect of the old value before applying the new one. The `-U` row carries the old values so a `SUM` can subtract them, a join can retract the old joined row, and a filter can remove a row that no longer matches.
:::

::: check
?? Why does Flink CDC's incremental snapshot not need a table lock?
It reads the table in primary-key chunks and, for each chunk, replays the log entries recorded between the start and end of that chunk's read, which makes each chunk consistent with the log. After the last chunk, it continues from the log.

?? An order moves from `PAID` to `REFUNDED`. How does `SUM(amount) ... WHERE status = 'PAID'` stay correct?
The update arrives as `-U` (old row, status `PAID`) and `+U` (new row, status `REFUNDED`). The `-U` row passes the filter and is subtracted; the `+U` row is filtered out.

?? When would you choose Datastream over Flink CDC on Google Cloud?
When you only need to replicate tables into BigQuery or Cloud Storage and want a serverless service with no cluster or code. Choose Flink CDC when the change stream must be joined, aggregated or enriched before it lands, or written to other sinks.
:::

::: resources
- Docs: [Flink CDC documentation](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) · source connectors, YAML pipelines, routing and schema evolution
- Repo: [apache/flink-cdc](https://github.com/apache/flink-cdc) · the project source, releases and examples
- Docs: [Dynamic tables](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/) · stream-table duality and changelog encodings
- Docs: [Flink SQL queries](https://nightlies.apache.org/flink/flink-docs-stable/docs/sql/reference/queries/overview/) · the query reference, including joins and windows
- Paper: [DBLog: a watermark based change-data-capture framework](https://arxiv.org/abs/2010.12597) · Netflix's lock-free snapshot algorithm that Flink CDC adapts
- Paper: [One SQL to rule them all](https://arxiv.org/abs/1905.12133) · Begoli et al., SIGMOD 2019; the design of streaming SQL in Flink, Beam and Calcite
:::

## Apache Beam

Flink is an engine: code written for it runs on Flink. Apache Beam is a **programming model**: you describe the pipeline once with Beam's SDK (Java, Python, Go, or YAML), and a **runner** translates it for an engine, such as Dataflow on Google Cloud, Flink or Spark. Beam came out of Google's internal systems (MapReduce, FlumeJava, MillWheel) and the Dataflow model paper, and it is the API you use to write Dataflow jobs.

The model's main idea is that batch and streaming are the same problem: a bounded dataset is just a stream that ends. The first topic covers the model and the runners; the second covers the part of the model that makes streaming correct, watermarks and late data, and compares how Flink, Beam and Spark handle it.

### Apache Beam — Unified Batch & Streaming

::: plain
Beam is like writing a recipe instead of cooking. The recipe lists the ingredients and steps ("take the orders, group them by city, add up the amounts every minute") without saying which kitchen will cook it. A runner is the kitchen: a small home kitchen for testing on your laptop, or a large industrial one (Dataflow, Flink, Spark) for production. The same recipe works for a single bag of groceries (batch) or a conveyor belt that never stops (streaming).
:::

A Beam program builds a **pipeline**, a directed graph of two kinds of things:

- A **PCollection** is a distributed, immutable collection of elements. It can be **bounded** (a set of files) or **unbounded** (a Pub/Sub subscription). Every element carries a **timestamp** (its event time) and belongs to one or more **windows**.
- A **PTransform** turns PCollections into PCollections. The core transforms are few: `ParDo` (run a `DoFn` on each element, emitting zero or more outputs, like a flat map), `GroupByKey` (gather all values per key and window), `CoGroupByKey` (a join), `Combine` (aggregate associatively, which lets runners pre-combine before the shuffle), `Flatten` (union) and `Partition` (split). Everything else, from `Count.PerKey` to `WriteToBigQuery`, is a **composite transform** built from these.

A batch pipeline in Python that reads CSV files from Cloud Storage and writes revenue per city to BigQuery:

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def parse(line):
    order_id, city, amount = line.split(",")
    return city, float(amount)

with beam.Pipeline(options=PipelineOptions()) as p:
    (p
     | "Read" >> beam.io.ReadFromText("gs://my-bucket/orders/2026-11-22/*.csv",
                                      skip_header_lines=1)
     | "Parse" >> beam.Map(parse)
     | "SumPerCity" >> beam.CombinePerKey(sum)
     | "ToRow" >> beam.Map(lambda kv: {"city": kv[0], "revenue": kv[1]})
     | "Write" >> beam.io.WriteToBigQuery(
           "my-proj:shop.revenue_by_city",
           schema="city:STRING,revenue:FLOAT",
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
```

The `|` operator applies a transform and `>>` gives it a name; names must be unique and stable, because runners use them to identify steps (Dataflow uses them to match steps when you update a running job). Building the graph does no work. The `with` block calls `run()` at the end, and the runner executes the whole graph.

The streaming version changes the source and adds **windowing**. Everything between stays the same:

```python
from apache_beam.transforms import window
from apache_beam.transforms.trigger import (AfterWatermark, AfterProcessingTime,
                                            AfterCount, AccumulationMode)
import json

(p
 | beam.io.ReadFromPubSub(subscription="projects/my-proj/subscriptions/orders-sub",
                          timestamp_attribute="event_ts")   # event time from an attribute
 | beam.Map(lambda b: json.loads(b))
 | beam.Map(lambda o: (o["city"], o["amount"]))
 | beam.WindowInto(window.FixedWindows(60),
                   trigger=AfterWatermark(early=AfterProcessingTime(10),
                                          late=AfterCount(1)),
                   accumulation_mode=AccumulationMode.ACCUMULATING,
                   allowed_lateness=600)                    # seconds
 | beam.CombinePerKey(sum)
 | ...)
```

This reads as four questions, the framing of the Dataflow model: **what** is computed (a sum per city), **where** in event time (one-minute fixed windows), **when** in processing time results are emitted (a speculative result every 10 seconds, the on-time result when the watermark passes the window end, and an update for each late element), and **how** successive results relate (accumulating: each firing contains the full total so far). [Streaming systems](#streaming) explains window types, triggers and accumulation modes in depth; the point here is that windowing is a property of the PCollection, so `CombinePerKey` groups by key *and* window without any change to its code. For a bounded input, everything sits in the single global window and the same transforms produce one result per key.

**Runners.**

| Runner | Where it runs | Typical use |
|---|---|---|
| Direct runner (and Prism, the newer local runner) | Your machine | Tests and development; it checks your code for model violations such as mutating inputs |
| Dataflow runner | Dataflow, Google Cloud's managed service | Production on Google Cloud: autoscaling, no cluster |
| Flink runner | A Flink cluster (for example on GKE) | Beam portability on infrastructure you run |
| Spark runner | A Spark cluster | Running Beam code where Spark is the platform |

Not every runner supports every feature; Beam's **capability matrix** lists which runners implement which parts of the model (stateful processing, timers, triggers, splittable DoFns). Dataflow and Flink are the most complete.

**Dataflow, briefly.** Running on Dataflow is a matter of options:

```bash
python revenue.py \
  --runner=DataflowRunner --project=my-proj --region=europe-west1 \
  --temp_location=gs://my-bucket/tmp --streaming
```

Dataflow builds the graph, applies optimisations such as **fusion** (merging consecutive steps into one stage, like Flink's chaining), starts workers, autoscales them on throughput and backlog, and in streaming mode keeps shuffle and state in its backend service (Streaming Engine) rather than on worker disks. Templates (Flex Templates) package a pipeline so others can launch it with parameters. Operations, cost and tuning are covered in [Pipelines on Google Cloud](#gcppipes).

**Beam SQL and Beam YAML.** `SqlTransform` runs SQL over a PCollection with a schema (in Python it is a cross-language transform that calls the Java implementation, so it needs Java available). **Beam YAML** describes a whole pipeline declaratively, which suits simple ingestion jobs:

```yaml
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config:
        path: gs://my-bucket/orders/2026-11-22/*.csv
    - type: Filter
      config:
        language: python
        keep: "amount > 0"
    - type: Sql
      config:
        query: "SELECT city, SUM(amount) AS revenue FROM PCOLLECTION GROUP BY city"
    - type: WriteToJson
      config:
        path: gs://my-bucket/out/revenue
```

> **Pitfall:** A `DoFn` may be retried, and may run more than once for the same element on different workers (for example when a straggling bundle is re-executed). Side effects inside a `DoFn` (an HTTP call, a database insert) must be idempotent. Beam's exactly-once guarantees cover the pipeline's own state and outputs, not your external calls.

> **Tip:** Write pipeline logic as small `PTransform` subclasses and test them with `TestPipeline` and `assert_that` on the Direct runner. The same transforms then run unchanged on Dataflow.

::: deeper
**Portability.** Modern runners do not run your Python inside the engine's JVM. The runner executes the graph, and each language's code runs in an **SDK harness** container next to it; the two talk through the **Fn API** (gRPC streams for data, state and control). That is how a Python pipeline runs on Flink or Dataflow (Dataflow Runner v2), and how **cross-language transforms** work: a Python pipeline can use Java's Kafka or BigQuery Storage Write API connectors, which an expansion service inserts into the graph.

**Bundles and fusion.** Runners process elements in **bundles**, and a `DoFn`'s `setup`, `start_bundle`, `finish_bundle` and `teardown` methods let you open connections once and batch writes per bundle. Fusion avoids materialising data between steps, but it can also hide parallelism: if a step expands one element into millions (one file name into its lines), the whole expansion runs on the worker that read the file. Insert `beam.Reshuffle()` to break fusion and redistribute.

**Splittable DoFn and state.** Sources are written as **splittable DoFns**, which can report progress and let the runner split remaining work (dynamic work rebalancing in Dataflow). **Stateful DoFns** with **timers** give per-key, per-window state, the same building blocks as Flink's `KeyedProcessFunction`.

**Combine and lifting.** Because `Combine` functions are associative and commutative, runners apply them before the shuffle ("combiner lifting"), so only one partial sum per key per worker crosses the network. `GroupByKey` followed by a Python `sum` loses this optimisation.

> **Interview:** "What does Beam give you that writing directly for Flink or Spark does not?" A runner-independent model that unifies batch and streaming with explicit event-time semantics (what, where, when, how), and on Google Cloud the path to Dataflow's serverless execution. The cost is an extra abstraction layer and features that depend on runner support.
:::

::: check
?? What are a PCollection and a PTransform?
A PCollection is an immutable, possibly unbounded, distributed collection of timestamped, windowed elements. A PTransform is an operation that takes PCollections and produces PCollections, such as `ParDo`, `GroupByKey` or `Combine`.

?? What changes when the batch revenue pipeline becomes a streaming one?
The source becomes unbounded (Pub/Sub, with event timestamps) and a `WindowInto` step is added, with triggers if needed. The parsing and `CombinePerKey` steps are unchanged, because grouping happens per key and per window.

?? Why might a step that reads file names and emits their lines run on only one worker in Dataflow?
Fusion merged it with the following steps, so all lines from one element are processed where that element was read. A `Reshuffle` after the expanding step redistributes the work.
:::

::: resources
- Docs: [Apache Beam documentation](https://beam.apache.org/documentation/) · the programming guide, transforms catalogue and SDK references
- Docs: [Beam capability matrix](https://beam.apache.org/documentation/runners/capability-matrix/) · which runners support which parts of the model
- Docs: [Dataflow documentation](https://docs.cloud.google.com/dataflow/docs) · running Beam pipelines on Google Cloud
- Paper: [The Dataflow Model](https://vldb.org/pvldb/vol8/p1792-Akidau.pdf) · Akidau et al., VLDB 2015; the what, where, when and how of Beam
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · Akidau, Chernyak and Lax; the Beam model explained by its authors
- Course: [Tour of Beam](https://tour.beam.apache.org/) · interactive lessons with runnable examples in the browser
- Docs: [Beam YAML](https://beam.apache.org/documentation/sdks/yaml/) · declarative pipelines and the available transforms
:::

### Watermarks & Late Data Handling

::: plain
Imagine collecting exam papers from several classrooms. You cannot publish the results for the morning exam until you are fairly sure every morning paper has arrived, but one classroom's runner is always slow, and occasionally a paper turns up the next day. A watermark is your note saying "I believe all papers written before 10:00 are in". It can only move forward, it moves at the pace of the slowest classroom, and you must decide in advance what to do with a paper that arrives after you have already published.
:::

A **watermark** is a timestamp *W* flowing through a pipeline that asserts: "events with event time earlier than *W* are not expected any more". Windows and event-time timers fire when the watermark passes them. [Streaming systems](#streaming) introduces event time, watermark delay and the three answers to late data (drop, update with allowed lateness, side output). This topic is about the mechanics: where watermarks are created, how they travel through a parallel job, what stalls them, and how each engine lets you route late data on purpose.

**Generating watermarks.** Watermarks start at the sources, from one of three strategies:

| Strategy | Watermark | When to use |
|---|---|---|
| Bounded out-of-orderness | max event time seen minus a fixed delay | Most Kafka and Pub/Sub sources; the default choice |
| Monotonous (ascending) | max event time seen | Sources that are strictly ordered by time, such as one log file per partition |
| Source-provided | Estimated by the source itself | Pub/Sub in Dataflow (from the oldest unacknowledged message), file sources that know their time ranges |

In Flink you attach the strategy where the source is created, so it is applied **per source split** (per Kafka partition) inside the source:

```java
WatermarkStrategy<Order> strategy = WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((order, recordTs) -> order.eventTimeMillis())
    .withIdleness(Duration.ofMinutes(1))
    .withWatermarkAlignment("orders", Duration.ofSeconds(30), Duration.ofSeconds(1));

DataStream<Order> orders = env.fromSource(kafkaSource, strategy, "orders");
```

Per-partition generation matters: if one consumer reads two partitions, one 10 minutes behind the other, a single watermark computed over the merged stream would race ahead with the fast partition and make every record of the slow one late.

**Propagation: the minimum rule.** Every operator tracks the latest watermark from each of its input channels and sets its own watermark to the **minimum** across them; when that minimum advances, it forwards the new watermark downstream. After a `keyBy`, every subtask receives from every upstream subtask, so the whole job's event-time clock moves at the pace of the slowest partition. Beam defines it the same way per PCollection: a transform's input watermark is the minimum of its upstream output watermarks.

[[fig:engines_watermarks]]

The minimum rule has a trap: a partition that receives **no events** never advances its watermark and holds back the entire job. **Idleness** detection fixes this by excluding inputs that have been silent for a timeout. This simulation shows a stalled job and the same job with an idleness timeout:

```python
# Three source partitions feed one window operator. Each partition's watermark
# is (max event time seen - 5 s). The operator's watermark is the minimum over
# its active inputs. Partition p2 receives nothing after processing time 2.
DELAY = 5
arrivals = [  # (processing time, partition, event time), in seconds
    (1, "p0", 10), (1, "p1", 12), (2, "p2", 11),
    (5, "p0", 30), (6, "p1", 33), (9, "p0", 52), (10, "p1", 55),
    (14, "p0", 71), (15, "p1", 74),
]

def run(idle_timeout=None):
    max_ts = {"p0": None, "p1": None, "p2": None}
    last_seen, marks = {}, []
    for now, part, ts in arrivals:
        max_ts[part] = ts if max_ts[part] is None else max(max_ts[part], ts)
        last_seen[part] = now
        active = [p for p in max_ts if idle_timeout is None
                  or p not in last_seen or now - last_seen[p] < idle_timeout]
        if any(max_ts[p] is None for p in active):
            marks.append(None)            # some input has no watermark yet
        else:
            marks.append(min(max_ts[p] for p in active) - DELAY)
    print(marks)

run()                  # p2 holds the watermark at 6 for ever
run(idle_timeout=10)   # p2 is idle from t=12 and is ignored
# [None, None, 5, 6, 6, 6, 6, 6, 6]
# [None, None, 5, 6, 6, 6, 6, 50, 66]
```

Without idleness, no window after the first few seconds ever closes. With it, the watermark jumps forward as soon as `p2` is declared idle. The risk is the reverse: if `p2` wakes up with old events, they are now late.

**Watermark alignment** handles the opposite problem, a source that is too *fast*. When one partition is hours ahead (for example while catching up after an outage), downstream windows must buffer its data until the slow partitions catch up, and state balloons. `withWatermarkAlignment` makes sources in the same group pause reading splits whose watermark is more than the allowed drift (30 seconds above) ahead of the group's minimum.

**Routing late data in Flink.** In the DataStream API, a window can keep its state for an extra period and send anything later still to a side output:

```java
OutputTag<Order> lateTag = new OutputTag<Order>("late-orders") {};

SingleOutputStreamOperator<CityRevenue> revenue = orders
    .keyBy(Order::city)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .allowedLateness(Duration.ofMinutes(5))     // re-fire the window for late events
    .sideOutputLateData(lateTag)                // after that, divert instead of dropping
    .aggregate(new SumAmount(), new ToCityRevenue());

revenue.getSideOutput(lateTag).sinkTo(lateOrdersSink);   // for example a lakehouse table
```

In Flink SQL, window functions drop late rows, but the `CURRENT_WATERMARK` function lets you split the stream yourself before the window:

```sql
-- on-time rows go to the windowed aggregation, late rows to a reconciliation table
INSERT INTO late_orders
SELECT * FROM orders
WHERE CURRENT_WATERMARK(ts) IS NOT NULL AND ts <= CURRENT_WATERMARK(ts);
```

**Routing late data in Beam.** Elements later than `allowed_lateness` are dropped. Within allowed lateness, a late element triggers a **late pane**, and a `DoFn` can read each pane's timing and route accordingly:

```python
from apache_beam.utils.windowed_value import PaneInfoTiming

class SplitByTiming(beam.DoFn):
    def process(self, kv, pane=beam.DoFn.PaneInfoParam):
        if pane.timing == PaneInfoTiming.LATE:
            yield beam.pvalue.TaggedOutput("late", kv)   # corrections
        else:
            yield kv                                      # early and on-time results

results = sums | beam.ParDo(SplitByTiming()).with_outputs("late", main="on_time")
# results.on_time and results.late are separate PCollections
```

**Spark Structured Streaming.** `withWatermark("ts", "10 minutes")` sets the delay; state for windows older than the watermark is dropped, and later rows *may* be dropped (Spark guarantees not to drop rows within the delay, but does not guarantee to drop all rows beyond it). There is no built-in side output; the streaming query progress reports `numRowsDroppedByWatermark` per stateful operator, which you should monitor. With several input streams, Spark computes one global watermark using `spark.sql.streaming.multipleWatermarkPolicy`: `min` (the default, safe) or `max` (faster results, with late rows from slower streams dropped).

> **Pitfall:** A lagging watermark is often the first symptom of trouble, not a problem in itself. Watch the gap between the watermark and wall-clock time (Flink's `currentInputWatermark` per operator, Dataflow's data freshness and system latency metrics, Spark's event-time progress). A sudden growth points to an idle or slow partition, a skewed key or backpressure.

::: deeper
**Watermark holds (Beam).** A transform's *output* watermark is the minimum of its input watermark and any **holds** it has placed. A window that is buffering data, or a timer that has not fired, holds the output watermark back so that results it has not emitted yet are not considered late downstream. This is why a downstream window cannot close before the upstream window that feeds it has fired, and why a stuck timer can stall a whole Dataflow pipeline.

**Output timestamps.** When a window fires, the result needs an event time. Beam defaults to the end of the window (`TimestampCombiner.END_OF_WINDOW`, strictly the window's maximum timestamp); Flink windows emit results stamped with the window's end minus one millisecond. Choosing "earliest" instead makes outputs look earlier and therefore more likely to be late in the next windowing step.

**Emission frequency.** Flink generators emit watermarks periodically (`pipeline.auto-watermark-interval`, 200 ms by default) or per record for punctuated strategies, which can be expensive. Watermarks are records in the stream, so they also queue behind backpressure.

**Batch is the limiting case.** On bounded input the watermark is minus infinity until the input is exhausted and then jumps to plus infinity, firing every window exactly once. That is how Beam and Flink run the same windowed code over a backfill.

**Perfect versus heuristic.** A perfect watermark (possible when the source knows its data, such as ordered files) never produces late data; a heuristic one trades latency against completeness. No setting removes the need to decide what late data means for the business: corrections, a daily reconciliation job, or an accepted loss.

> **Interview:** "A Flink job's windows stopped firing, but data is still flowing. What do you check?" The watermark per operator in the web UI. If it is stuck, look for an idle source partition (fix with `withIdleness`), a partition with a bad future or past timestamp, or more source subtasks than partitions, which leaves some subtasks with no input to advance their watermark.
:::

::: check
?? Why does an operator take the minimum of its inputs' watermarks rather than the maximum?
A watermark promises that no earlier events will arrive. The operator can only promise what all its inputs promise, which is the smallest of their watermarks; taking a larger one would make the slower inputs' events late.

?? What does idleness detection fix, and what risk does it add?
It stops a partition that receives no events from holding back the whole job's watermark. If the idle partition later receives events with old timestamps, they may now be late.

?? How can a Beam pipeline send corrections for late data to a different sink from on-time results?
Allow lateness in `WindowInto`, read the pane timing with `beam.DoFn.PaneInfoParam` in a `DoFn`, and emit late panes to a tagged output that is written separately.
:::

::: resources
- Docs: [Generating watermarks in Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/) · strategies, per-split generation, idleness and alignment
- Docs: [Flink side outputs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/side_output/) · tagged outputs, including late data from windows
- Docs: [Spark: handling late data and watermarking](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) · `withWatermark`, its guarantees and multiple-watermark policy
- Book: [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) · chapter 3 is a full treatment of watermarks, holds and propagation
- Paper: [Watermarks in stream processing systems](https://www.vldb.org/pvldb/vol14/p3135-begoli.pdf) · Begoli et al., VLDB 2021; semantics compared across Flink and Dataflow
- Docs: [Dataflow streaming pipelines](https://docs.cloud.google.com/dataflow/docs/concepts/streaming-pipelines) · watermarks, windows and late data on Dataflow
:::

## SQL engines

Flink and Beam move data from sources to sinks. The engines in this group answer questions: an analyst writes a query, and the engine reads whatever it needs, from wherever it lives, and returns a result in seconds. **Trino** (and its sibling **Presto**) is the open-source standard for this. It stores nothing itself: it reads files and lakehouse tables in Cloud Storage, operational databases and even BigQuery through **connectors**, and can join them in one query.

The first topic explains how Trino runs a query; the second explains **federation**, querying several systems at once, and the pushdown that makes it fast. Several other engines sit nearby, and you will meet them in later sections: **DuckDB** and **Polars** run analytical SQL and DataFrame queries on a single machine and often make a cluster unnecessary for data up to tens or hundreds of gigabytes; ClickHouse, Druid, Pinot and StarRocks serve real-time analytics ([Real-time OLAP engines](#rtolap)); and the planners and vectorised executors that all of them share are the subject of the next section, [Query engine internals](#queryengines).

### Apache Trino / Presto — Query Federation

::: plain
Trino is a translator and a team of fast readers who own no books. You ask one question in SQL; the coordinator works out which libraries (data sources) hold the answer, splits the reading into many small jobs, and hands them to the readers (workers), who read in parallel, pass partial answers to each other, and assemble the result. Because nothing is stored in Trino itself, you can point it at a new library just by adding a catalog entry.
:::

**History in two lines.** Presto was built at Facebook in 2012 to run interactive SQL over its Hive data warehouse, replacing slow MapReduce-based Hive queries. In 2019 its creators, who had left Facebook, continued it as PrestoSQL, renamed **Trino** in 2020; the original continues as **PrestoDB** under the Linux Foundation, now with a C++ worker (Presto C++, built on Velox). The two share their architecture; this topic uses Trino, which has the larger open-source community, and the same ideas apply to Presto and to Starburst, the commercial Trino distribution.

**The processes.** A Trino cluster has one **coordinator** and many **workers**:

- The **coordinator** accepts queries (over HTTP from the CLI, JDBC, Python clients or BI tools), parses and analyses them, looks up metadata through the connectors, plans and optimises, and schedules the work on workers. It also tracks memory and kills queries that exceed limits.
- **Workers** execute tasks and exchange intermediate data with each other over HTTP. They announce themselves to the coordinator's discovery service, so adding a worker is a matter of starting one.

**A query's anatomy.** The optimised plan is cut into **stages** (plan fragments) at every point where data must be redistributed, just as Spark cuts stages at shuffles ([Apache Spark](#spark)). Each stage runs as **tasks**, usually one per worker. A source stage processes **splits**, chunks of data that a connector can read independently (a range of a Parquet file, a BigQuery read stream). Inside a task, **drivers** run pipelines of **operators** (scan, filter, project, hash join build, aggregate), passing **pages**: columnar batches of rows. Crucially, everything is **pipelined and in memory**: a downstream stage starts consuming pages while upstream stages are still producing them, and nothing is written to disk between stages. That is the source of Trino's interactive speed, and of its main limitation.

[[fig:engines_trino]]

**Catalogs and connectors.** A **connector** implements Trino's service provider interface (SPI): it supplies metadata (schemas, tables, columns, statistics), splits, a page source that reads data, and optionally a page sink for writes. A **catalog** is a configured instance of a connector, defined by a properties file; tables are then addressed as `catalog.schema.table`.

```text
# etc/catalog/lake.properties: Iceberg tables on Cloud Storage
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://metastore.internal:9083
fs.native-gcs.enabled=true
gcs.project-id=my-proj

# etc/catalog/pg.properties: an operational PostgreSQL database (Cloud SQL)
connector.name=postgresql
connection-url=jdbc:postgresql://10.20.0.7:5432/shop
connection-user=trino_reader
connection-password=${ENV:PG_PASSWORD}

# etc/catalog/bq.properties: BigQuery, read through the Storage Read API
connector.name=bigquery
bigquery.project-id=my-proj
```

```sql
-- One query, three systems
SELECT c.segment, d.region, sum(o.amount) AS revenue
FROM lake.sales.orders AS o
JOIN pg.public.customers AS c ON o.customer_id = c.id
JOIN bq.reference.store_dim AS d ON o.store_id = d.store_id
WHERE o.order_date >= DATE '2026-11-01'
GROUP BY c.segment, d.region;
```

**The optimizer.** Trino's planner applies an iterative set of **rules** (push filters and projections down, prune columns, merge projections, push work into connectors) and makes **cost-based** decisions where statistics exist: the join order (`join_reordering_strategy`) and whether each join is **broadcast** (copy the small side to every worker) or **partitioned** (hash-redistribute both sides) through `join_distribution_type`, both `AUTOMATIC` by default. Statistics come from connectors: Iceberg and Hive keep table and column statistics (`ANALYZE lake.sales.orders` refreshes them); many JDBC sources expose few. **Dynamic filtering** adds a run-time trick: while the build side of a join is read, Trino collects its join keys and uses them to skip files and partitions on the probe side, often cutting a fact-table scan to a fraction.

`EXPLAIN ANALYZE` runs the query and shows, per stage and operator, rows, data size, CPU and wall time, and whether dynamic filters were applied; it is the first tool for any slow query.

**Memory.** Because intermediate data lives in memory, Trino enforces limits: `query.max-memory` (user memory one query may use across the cluster), `query.max-memory-per-node`, and `query.max-total-memory` (including system memory such as exchange buffers). A query that exceeds them fails with an "exceeded memory limit" error; when the whole cluster runs out, the **low-memory killer** cancels the query using the most memory on the blocked nodes. Worked numbers: a hash join whose build side is 40 GB of rows, partitioned across 10 workers, needs about 4 GB per worker plus hash-table overhead; broadcast instead, it needs the full 40 GB on *every* worker, which is why statistics that misjudge the "small" side cause memory failures.

> **Pitfall:** Out of the box, a Trino query has no intermediate checkpoints. If one worker dies, the whole query fails and must be rerun. For long ETL queries enable fault-tolerant execution (below) or use Spark; keep plain Trino for interactive and shorter queries.

**Running Trino on Google Cloud.** The usual choices are GKE with the community Helm chart (with workers on Spot VMs only when fault-tolerant execution is on), the Trino optional component on a Managed Service for Apache Spark cluster, or Starburst Galaxy, the vendor's managed service, which runs on Google Cloud. Data stays in Cloud Storage (Iceberg, Delta Lake or Hive tables), in BigQuery, or in the source databases.

::: deeper
**Fault-tolerant execution.** With `retry-policy=TASK`, workers write intermediate data to an **exchange manager** backed by object storage (Cloud Storage via `exchange.base-directories=gs://...`) instead of streaming it only in memory, and the coordinator retries failed tasks individually. Long queries survive worker loss and can use Spot VMs, and memory-heavy stages can be split into more tasks. The cost is latency from writing and reading exchange data, so many teams run two clusters: a low-latency interactive one and a fault-tolerant ETL one. `retry-policy=QUERY` simply reruns whole short queries.

**Spill to disk** lets joins, aggregations, sorts and window functions write revocable memory to local disk instead of failing. The Trino documentation now describes it as legacy and recommends fault-tolerant execution with the task retry policy instead.

**Resource groups** queue and limit queries per user, source or tag (maximum concurrent queries, memory share, CPU quota), the way warehouse workload management does. Without them, one analyst's cross join can starve a dashboard.

**The coordinator** is a single point of failure and a scheduling bottleneck in very large clusters; it holds no data, so a restart loses only running queries. Gateways (such as the open-source Trino Gateway) route clients across several clusters for availability and blue-green upgrades.

**Why Trino is fast without owning storage.** Columnar pages, generated bytecode for expressions, pipelined execution with no stage barriers, aggressive pruning through partition, file and row-group statistics in Parquet and ORC, and dynamic filtering. When the data layout is poor (thousands of small files, no partitioning, CSV), none of this helps: Trino's speed is bounded by the layout of the tables it reads ([Data lakes and lakehouses](#lakehouse)).

> **Interview:** "Trino or Spark SQL for the same lakehouse?" Trino for interactive and BI queries: low latency, many concurrent users, federation across sources, no job startup. Spark for large batch ETL, complex pipelines mixing SQL with code, ML, and jobs that must survive failures over hours. Fault-tolerant execution narrows the gap, but the default trade-off still holds.
:::

::: check
?? What are stages, tasks and splits in Trino?
A stage is a fragment of the plan between data redistributions. Each stage runs as tasks on workers. A split is a chunk of source data that one task's operators can read independently, such as part of a Parquet file.

?? Why does a worker failure normally fail the whole Trino query?
Stages stream pages to each other in memory with no intermediate copies, so the lost work cannot be recovered. With `retry-policy=TASK` and an exchange manager, intermediate data is spooled to object storage and failed tasks are retried.

?? What does dynamic filtering do?
While a join's build side is read, Trino collects its join keys and uses them to skip partitions, files or row groups on the probe side, reducing the data the large table scan reads.
:::

::: resources
- Docs: [Trino documentation](https://trino.io/docs/current/) · installation, connectors, SQL reference and administration
- Docs: [Trino concepts](https://trino.io/docs/current/overview/concepts.html) · coordinator, workers, catalogs, stages, tasks, splits and pages
- Docs: [Fault-tolerant execution](https://trino.io/docs/current/admin/fault-tolerant-execution.html) · retry policies and exchange managers on object storage
- Book: [Trino: The Definitive Guide](https://www.oreilly.com/library/view/trino-the-definitive/9781098107703/) · Fuller, Moser and Traverso; architecture, connectors and operations
- Paper: [Presto: SQL on Everything](https://trino.io/Presto_SQL_on_Everything.pdf) · Sethi et al., ICDE 2019; the design paper from Facebook
- Docs: [Trino component on Managed Service for Apache Spark](https://docs.cloud.google.com/dataproc/docs/concepts/components/trino) · running Trino on a Google Cloud cluster
:::

### Federated Query Execution

::: plain
Federation means asking one question that needs answers from several places, without first copying everything into one place. It is like a travel agent who books a flight, a hotel and a train in one call to you, while each company keeps its own booking system. The trick that makes it work is delegation: the agent asks each company only for the few options that match your dates, instead of downloading their whole timetable and searching it.
:::

A **federated query** reads from two or more independent systems and combines the results in one engine. The previous topic showed Trino doing it; BigQuery does it with external queries to Cloud SQL, AlloyDB and Spanner; Dremio and Starburst build products around it. The appeal is obvious: no pipeline to build, always-current data, one SQL dialect over everything. The costs are less obvious, and they are what this topic is about.

**What happens inside.** For each table in the query, the engine asks the table's connector for metadata and for splits, then reads rows through the connector, then does everything the source did not do (filters, joins, aggregations) itself. The deciding factor for performance is therefore **how much work the connector can push down to the source**, because every row a source returns must cross the network, be converted into the engine's format and be processed again.

The kinds of pushdown, from most to least widely supported:

| Pushdown | What the source does | Effect |
|---|---|---|
| Projection | Returns only the referenced columns | Less data per row |
| Predicate (filter) | Applies `WHERE` conditions, or prunes partitions and files | Fewer rows |
| Limit and top-N | Applies `LIMIT` and `ORDER BY ... LIMIT` | Stops early |
| Aggregation | Runs `GROUP BY` and aggregate functions | One row per group instead of all rows |
| Join | Joins two tables of the same source | Only joined rows leave the source |

This simulation counts the rows a connector ships for one query with three levels of pushdown:

```python
import sqlite3

# A "remote" PostgreSQL stand-in with 100,000 orders over 100 days.
remote = sqlite3.connect(":memory:")
remote.execute("CREATE TABLE orders (id INTEGER, day INTEGER, country TEXT, amount REAL)")
remote.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)",
                   [(i, i % 100, ("DE", "FR", "IN", "US")[i // 100 % 4], i // 100 % 50)
                    for i in range(100_000)])

def connector_scan(sql, params=()):
    """What the connector fetches over the network: count the rows shipped."""
    rows = remote.execute(sql, params).fetchall()
    return rows, len(rows)

# Query: SELECT country, SUM(amount) FROM orders WHERE day = 7 GROUP BY country

# 1. No pushdown: ship every row, filter and aggregate in the engine
rows, shipped = connector_scan("SELECT day, country, amount FROM orders")
totals = {}
for day, country, amount in rows:
    if day == 7:
        totals[country] = totals.get(country, 0) + amount
print("no pushdown:", shipped, "rows shipped", sorted(totals.items()))

# 2. Predicate pushdown: the source applies WHERE day = 7
rows, shipped = connector_scan("SELECT country, amount FROM orders WHERE day = ?", (7,))
print("filter pushed:", shipped, "rows shipped")

# 3. Aggregation pushdown: the source also does the GROUP BY
rows, shipped = connector_scan(
    "SELECT country, SUM(amount) FROM orders WHERE day = ? GROUP BY country", (7,))
print("aggregate pushed:", shipped, "rows shipped", sorted(rows))
# no pushdown: 100000 rows shipped [('DE', 6000.0), ('FR', 6250.0), ('IN', 6000.0), ('US', 6250.0)]
# filter pushed: 1000 rows shipped
# aggregate pushed: 4 rows shipped [('DE', 6000.0), ('FR', 6250.0), ('IN', 6000.0), ('US', 6250.0)]
```

Same answer, 25,000 times fewer rows moved. Real numbers are starker: a federated query that pulls 200 million rows from a PostgreSQL replica at, say, 200,000 rows per second spends over 16 minutes on transfer alone, whatever the engine's speed.

[[fig:engines_pushdown]]

**Connectors differ in what they push down.**

- **Lakehouse connectors** (Hive, Iceberg, Delta Lake) cannot run SQL at the source, because the source is files. They "push down" by **pruning**: partition values, file-level column statistics and Parquet row-group statistics let them skip data, and dynamic filtering adds pruning from join keys at run time. They split work into many splits, so reads are parallel.
- **JDBC connectors** (PostgreSQL, MySQL, SQL Server, Oracle) translate filters, projections, limits and, for many functions, aggregations and joins into SQL for the source. Most read each table through a single connection, so a large unfiltered read is a single slow stream.
- **The BigQuery connector** reads through the BigQuery Storage Read API in parallel streams with column selection and filters pushed down; for anything more complex it can send a whole query to BigQuery.
- **TPC-H and TPC-DS connectors** generate benchmark data on the fly. They are ideal for testing a cluster's sizing and configuration without loading data: `SELECT count(*) FROM tpcds.sf100.store_sales`.

When the engine cannot push down what you need, many connectors offer **query pass-through**: a table function that sends native SQL unchanged to the source.

```sql
-- Let PostgreSQL run its own window function and return only the result
SELECT * FROM TABLE(pg.system.query(query => '
  SELECT customer_id, amount,
         rank() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk
  FROM orders WHERE order_date >= current_date - 7'));
```

Check what happened with `EXPLAIN`: a pushed-down filter appears inside the table scan's description of the source query or constraint, while a filter that stays above the scan as a separate filter step was not pushed. Rewriting a predicate so the connector understands it (a plain comparison on a column instead of a function wrapped around it) is often the whole fix.

**Federation on Google Cloud.** BigQuery has built-in federation: **external queries** send SQL to Cloud SQL, AlloyDB or Spanner through a connection and return the result to BigQuery, where it can be joined with native tables.

```sql
SELECT c.segment, SUM(o.amount) AS revenue
FROM `my-proj.sales.orders` AS o
JOIN EXTERNAL_QUERY('my-proj.europe-west1.shop-pg',
                    'SELECT id, segment FROM customers') AS c
  ON o.customer_id = c.id
GROUP BY c.segment;
```

The inner query runs in PostgreSQL exactly as written, so put the filters inside it. BigQuery also reads lakehouse tables in Cloud Storage directly (external and Iceberg tables), which is federation over files.

**Federation products.** **Starburst** is commercial Trino (Starburst Enterprise, and Starburst Galaxy as a managed service) with extra connectors, parallel JDBC reads, caching and indexing ("Warp Speed"), and fine-grained access control. **Dremio** is a lakehouse query engine built on Apache Arrow, whose **reflections** (materialisations of tables or aggregates that the optimizer substitutes transparently) trade freshness for speed. Both, like Trino, are most efficient when most data lives in an open lakehouse and federation covers the remaining sources.

**The trade-offs.**

| Federate when | Copy (ingest) when |
|---|---|
| Queries are occasional or exploratory | Queries run constantly or power dashboards |
| You need the current state of an operational system | Minutes-old data is fine |
| Data is small after pushdown | Queries scan large parts of the source |
| A pipeline is not justified yet | The source must be protected from analytical load |

> **Pitfall:** A federated query against a production OLTP database runs on that database. One analyst's full scan can slow checkout for customers. Point connectors at read replicas, use resource groups and connector limits, and move heavy, repeated queries to ingested copies.

> **Pitfall:** There is no consistent snapshot across sources. A federated join reads each system at a slightly different moment, so totals can disagree with either source during heavy writes. For audited numbers, ingest and reconcile.

::: deeper
**Where the join runs.** If both tables come from the same JDBC source and the connector supports join pushdown, the source executes the join; otherwise the engine reads both sides and joins them itself. The cost-based optimizer decides from statistics, and JDBC sources often provide poor ones, so a "small" dimension table read through JDBC may be broadcast happily, or a huge one may be chosen as the build side and exceed memory. Collect statistics where the connector supports it, or use pass-through for the heavy part.

**Latency budget.** A federated query's latency is roughly the slowest source's time to first row plus transfer time plus engine processing. Interactive federation therefore works best against sources with fast, selective indexes (a lookup by customer ID) and lakehouse tables with good pruning, and poorly against sources that must scan (a JDBC source without an index on the filter column).

**Type and semantic mismatches.** Sources differ in collation (is `'a' = 'A'`?), in timestamp precision and time zones, in `NULL` ordering and in decimal precision. Connectors push a predicate down only when the source's semantics match the engine's; when they do not (for example a case-sensitive comparison against a case-insensitive MySQL collation), the filter stays in the engine and is correct but slower. Expect some predicates to "mysteriously" not push down for this reason.

**Caching.** Repeated federated queries can be served from caches: Trino's file system cache for object storage, Starburst's caching, Dremio's reflections, or simply a materialised copy. Every cache reintroduces staleness, which is the thing federation was chosen to avoid, so be explicit about how fresh each use case needs data to be.

> **Interview:** "Why is a federated query against PostgreSQL slow when the same query is fast in PostgreSQL itself?" Either predicates or aggregations are not being pushed down (check `EXPLAIN`), so the engine pulls far more rows over a single JDBC connection than the source would return, or the join is executed in the engine with a poor plan because the source lacks statistics. Rewrite predicates so they push down, or use query pass-through.
:::


::: check
?? Why does pushdown matter more for federated queries than for queries over the engine's own storage?
Every row a source returns crosses the network and is converted and processed again by the engine. Pushing filters, projections and aggregations to the source shrinks that transfer, often by orders of magnitude.

?? How does an Iceberg or Hive connector "push down" a filter when the source is just files?
Through pruning: it uses partition values, file-level column statistics and Parquet or ORC row-group statistics (plus dynamic filters at run time) to skip files and row groups that cannot match.

?? Give two reasons to ingest data rather than federate a query to an operational database.
The analytical query load would slow the production database, queries run often or scan a lot of data, or results must be consistent across sources (any two).
:::

::: resources
- Docs: [Trino connectors](https://trino.io/docs/current/connector.html) · every connector with its configuration and pushdown support
- Docs: [Trino pushdown](https://trino.io/docs/current/optimizer/pushdown.html) · which operations are pushed down and how to confirm it with EXPLAIN
- Docs: [Dynamic filtering](https://trino.io/docs/current/admin/dynamic-filtering.html) · run-time pruning from join keys
- Docs: [BigQuery federated queries](https://docs.cloud.google.com/bigquery/docs/federated-queries-intro) · external queries to Cloud SQL, AlloyDB and Spanner
- Book: [Trino: The Definitive Guide](https://www.oreilly.com/library/view/trino-the-definitive/9781098107703/) · the chapters on connectors and federation
- Docs: [Starburst documentation](https://docs.starburst.io/) · the commercial Trino distribution, including Galaxy and caching
- Docs: [Dremio documentation](https://docs.dremio.com/) · Arrow-based lakehouse engine and reflections
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, setting or rule of thumb |
|---|---|
| Slots a Flink job needs | Highest operator parallelism (slot sharing), divided by `taskmanager.numberOfTaskSlots` per TaskManager |
| Enable checkpoints | `execution.checkpointing.interval: 30s`, `execution.checkpointing.dir: gs://...` |
| Keep checkpoints fast under backpressure | `execution.checkpointing.unaligned.enabled: true` |
| Upgrade or rescale a Flink job | `flink stop --savepointPath gs://...` then `flink run -s <savepoint> -p <n>` |
| Make state restorable after code changes | `.uid("...")` on every stateful operator; POJO or Avro state types |
| Large state | `state.backend.type: rocksdb` plus `execution.checkpointing.incremental: true` |
| Bound state growth | `StateTtlConfig` in DataStream, `table.exec.state.ttl` in Flink SQL |
| Rescaling limit | Never above max parallelism (key groups); set `pipeline.max-parallelism` once |
| Read a MySQL table as a changelog | `'connector' = 'mysql-cdc'` with a primary key and a `server-id` range |
| See what an SQL operator emits | `EXPLAIN CHANGELOG_MODE SELECT ...` |
| Write an updating result | Upsert sink with a primary key (`upsert-kafka`, Paimon, Iceberg, a database) |
| Beam: run on Dataflow | `--runner=DataflowRunner --project --region --temp_location gs://...` |
| Beam: event time from Pub/Sub | `ReadFromPubSub(..., timestamp_attribute="event_ts")` |
| Beam: break fusion | `beam.Reshuffle()` after a step that expands one element into many |
| Watermark rule | Operator watermark = minimum over its inputs; idle inputs need `withIdleness` |
| Route late data | Flink `sideOutputLateData`, Flink SQL `CURRENT_WATERMARK`, Beam late panes, Spark: monitor `numRowsDroppedByWatermark` |
| Trino catalog | `etc/catalog/<name>.properties` with `connector.name=...`; tables are `catalog.schema.table` |
| Long Trino ETL queries | `retry-policy=TASK` with an exchange manager on Cloud Storage |
| Slow federated query | `EXPLAIN` to confirm pushdown; rewrite predicates; query pass-through |
| Federation inside BigQuery | `EXTERNAL_QUERY('project.region.connection', 'SELECT ...')` |

### Practice

::: try
**1. Size a Flink cluster.** A Flink job has a Kafka source with parallelism 12 (the topic has 12 partitions), a keyed window operator with parallelism 24, and a sink with parallelism 6. TaskManagers have 8 slots. How many TaskManagers does the job need with default slot sharing? What happens to 12 of the window operator's upstream channels, and what would happen if you set the source to parallelism 24?
:::

?? Answer to 1
24 slots are needed (the highest parallelism), so 3 TaskManagers. Each window subtask receives from all 12 source subtasks; parallelism is not limited by the number of partitions after the `keyBy`. A source parallelism of 24 with only 12 partitions would leave 12 source subtasks with no partition: they do no work and, unless they are treated as idle, can hold back the watermark.

::: try
**2. Watermark arithmetic.** Extend the watermark simulation from this section so that partition `p2` comes back at processing time 16 with an event at event time 40, while the idleness timeout is 10. What is the operator's watermark after that event, and is the event late for a 60-second tumbling window that starts at 0?
:::

?? A solution to 2

```python
DELAY = 5
arrivals = [
    (1, "p0", 10), (1, "p1", 12), (2, "p2", 11),
    (5, "p0", 30), (6, "p1", 33), (9, "p0", 52), (10, "p1", 55),
    (14, "p0", 71), (15, "p1", 74), (16, "p2", 40),
]
max_ts, last_seen, wm = {"p0": None, "p1": None, "p2": None}, {}, None
for now, part, ts in arrivals:
    late = wm is not None and ts < 60 <= wm      # window [0, 60) already closed?
    max_ts[part] = ts if max_ts[part] is None else max(max_ts[part], ts)
    last_seen[part] = now
    active = [p for p in max_ts if p not in last_seen or now - last_seen[p] < 10]
    if any(max_ts[p] is None for p in active):
        continue                                 # no watermark until all inputs report
    new = min(max_ts[p] for p in active) - DELAY
    wm = new if wm is None else max(wm, new)     # watermarks never go backwards
print(wm, late)
# 66 True   (the new minimum is 35, but a watermark never moves backwards)
```

The watermark does not move backwards (it stays at 66), and the event at 40 belongs to window [0, 60), which already fired when the watermark passed 60. It is late: dropped, or handled by allowed lateness or a side output.

::: try
**3. CDC aggregation.** A Flink SQL job computes `SELECT status, COUNT(*) FROM orders_cdc GROUP BY status`. Write down the changelog rows the source emits, and the output totals after each, for: insert order 1 as `NEW`, insert order 2 as `NEW`, update order 1 to `PAID`, delete order 2.
:::

?? Answer to 3
`+I (1, NEW)`: NEW 1. `+I (2, NEW)`: NEW 2. `-U (1, NEW)`: NEW 1. `+U (1, PAID)`: PAID 1. `-D (2, NEW)`: NEW 0. Final: NEW 0, PAID 1. The aggregation itself emits `-U`/`+U` pairs for each changed count, so the sink must be an upsert sink keyed by `status`.

::: try
**4. Federation design.** Analysts want a daily dashboard joining 3 years of orders (2 billion rows, Iceberg on Cloud Storage) with customer segments that change during the day (2 million rows in Cloud SQL for PostgreSQL). Would you federate with Trino, and how would you make the query fast and safe for the database?
:::

?? A solution to 4
Federation fits: the large side is in the lakehouse with good pruning, and the operational side is small. Point the PostgreSQL catalog at a read replica, select only `id` and `segment` (projection pushdown), and let Trino broadcast the 2 million-row side and use dynamic filtering on the orders scan. Partition the Iceberg table by order date so dashboard date filters prune files. If the dashboard is refreshed often by many users, ingest the segments table instead (for example with Datastream) and query BigQuery or Trino over copies only.

### What comes next

You can now place Flink, Beam and Trino next to Spark: how each runs a dataflow, where it keeps state, and how it fails and recovers. The next section, **Query engine internals**, opens the engines up: how SQL becomes a plan (Apache Calcite, cost-based optimization, Substrait), and how modern executors such as DuckDB, Velox and DataFusion run that plan with vectorised, SIMD and compiled code.
