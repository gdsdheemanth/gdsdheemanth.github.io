This section is about the control room of a data platform: the software that decides when each pipeline runs, in what order its steps run, what happens when one fails, and who gets told. It is written for data engineers who can already build a pipeline step (a SQL model, a load job, a Spark job) and now need dozens or hundreds of them to run reliably every day. It builds directly on [Pipelines and transformation](#pipelines), which covered what each step should do: idempotent load patterns, fan-out and fan-in shapes, dynamic generation, backfills and lineage. Where a topic touches those, it links back instead of repeating them. It also uses ideas from [Data ingestion](#ingestion) (where data comes from and when it arrives) and [Streaming systems](#streaming) (for the boundary between scheduled batches and always-on jobs). How to test, deploy and monitor orchestrated pipelines is covered later in [DataOps and platform engineering](#dataops), and the Google Cloud services around Managed Service for Apache Airflow (formerly Cloud Composer) and Workflows get their own treatment in [Pipelines on Google Cloud](#gcppipes).

Examples use Apache Airflow 3 (the current major version; 3.4 was released in November 2026), Dagster 1.13 and Prefect 3. Airflow, Dagster and Prefect code needs those packages installed; every example marked "runs as is" uses only the Python standard library and prints what its comments say.

**What you will be able to do afterwards**

- Design DAGs that are easy to rerun and reason about: the right task size, fan-out and fan-in with explicit trigger rules, task groups, and dependencies between pipelines that do not turn into hidden cycles.
- Choose between time-based schedules, sensors, deferrable operators and data-aware (asset-based) scheduling, and explain what each costs in worker slots and latency.
- Explain how Airflow 3 works inside: the DAG processor, serialized DAGs, the scheduler loop, executors, the triggerer, the Task SDK and the API server, and DAG versioning.
- Run Airflow in production: pick an executor, size workers and the metadata database, use XComs safely, and operate it on Managed Airflow Gen 3 or on Kubernetes.
- Build asset-based pipelines in Dagster (assets, partitions, backfills, IO managers, sensors, declarative automation) and Python-native flows in Prefect 3 (flows, tasks, deployments, work pools, workers, automations).
- Choose an orchestrator for a given team and workload, including when Google Cloud's Workflows or a simple Cloud Scheduler job is enough.

**The big picture.** Picture an airport. Each flight is a pipeline run, and each step of the flight (boarding, refuelling, take-off) is a task. The control tower does not fly any plane. It keeps the timetable (scheduling), makes sure refuelling finishes before take-off (dependencies), stops too many planes using one runway at once (pools and concurrency limits), reroutes and retries when something goes wrong, and keeps a log of everything that happened (run history). An orchestrator is that control tower for data. It should do very little heavy work itself: the warehouse, Spark or a container does the work, and the orchestrator starts it, waits for it, and records the outcome. Two schools of thought exist. **Task-centric** orchestrators (Airflow, Prefect, Workflows) describe *what to run and in which order*. **Asset-centric** orchestrators (Dagster, and Airflow's assets layered on top) describe *which data should exist and how fresh it should be*, and derive the runs from that. The first group of this section covers design and scheduling ideas that apply to every tool. The second group covers the tools one by one and ends with a decision guide.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Concepts

Before choosing a tool, it pays to know what good orchestration looks like in any tool. Two decisions shape every pipeline: how the work is cut into tasks and wired together (the DAG), and what makes each run start (a clock, a sensor, or the arrival of new data). Mistakes here are expensive because they spread: a badly cut task cannot be retried on its own, a hidden cross-pipeline dependency breaks silently when someone renames a table, and a sensor that holds a worker for six hours blocks everything else.

This group takes the patterns from [Pipeline Design Patterns](#pipelines) (fan-out, fan-in, branching, templates) and looks at them from the orchestrator's side: trigger rules, priorities, groups and cross-DAG dependencies first, then schedules, sensors, deferral and data-aware triggers.

### DAG Design & Dependency Management

::: plain
A DAG is a to-do list where some items can only start after others are finished, and nothing ever depends on itself, even indirectly. Designing one well is like planning a dinner: you start the slow roast first, chop vegetables while it cooks, and only plate up when everything is ready. If two dishes need the one oven, you decide which goes first. And if the dessert recipe secretly needs leftovers from tomorrow's lunch, the plan can never finish, which is exactly what a hidden circular dependency does.
:::

A **DAG** (directed acyclic graph) is a set of **tasks** joined by **dependencies**: an arrow from A to B means B may start only after A finishes. "Acyclic" means you can never follow arrows back to where you started, so there is always at least one valid order to run everything. In Airflow a DAG is a Python file; in Dagster the graph is built from assets and their inputs; in Prefect the graph is whatever your flow function calls, in the order it calls it.

**Cut tasks at retry boundaries.** The most important design choice is the size of a task. A task should be the smallest unit you would want to retry on its own, and each task should be idempotent (see [Pipeline Idempotency & Exactly-Once](#pipelines)). Too large ("run the whole nightly load in one Python function") and one failed API call reruns three hours of work. Too small ("one task per SQL statement") and you pay scheduling overhead of a few seconds per task, thousands of log files, and a graph nobody can read. A good rule: one task per external job you would look up separately (one BigQuery job, one Spark batch, one dbt build selection, one file load).

**Fan-out and fan-in with explicit trigger rules.** Fan-out (one task feeding many parallel tasks) and fan-in (many tasks feeding one) are described in [Pipelines and transformation](#pipelines). The orchestrator-specific decision is what the fan-in task does when some branches fail or are skipped. In Airflow this is the **trigger rule** of the downstream task:

| Trigger rule | The task runs when... | Typical use |
|---|---|---|
| `all_success` (default) | every upstream task succeeded | building a fact table from all sources |
| `all_done` | every upstream task finished, whatever the outcome | cleanup, notifications |
| `one_failed` | at least one upstream task failed | sending an alert early |
| `none_failed` | no upstream failed (skips are fine) | joining after a branch |
| `none_failed_min_one_success` | no upstream failed and at least one succeeded | joining after a branch where most paths are skipped |
| `always` | regardless of upstream state, even before they finish | rarely; watchdog tasks |

Here is a small Airflow 3 DAG that loads three tables in a task group, builds a fact table only if all loads succeed, and always posts a summary:

```python
from datetime import datetime
from airflow.sdk import dag, task, task_group

@dag(schedule="0 3 * * *", start_date=datetime(2026, 11, 1), catchup=False,
     default_args={"retries": 2}, tags=["sales"])
def sales_daily():

    @task_group(group_id="load")
    def load():
        @task(pool="crm_api")                    # at most N concurrent calls to the CRM
        def load_table(name: str) -> int:
            ...                                  # load one table, return row count
            return 1200
        return load_table.expand(name=["orders", "payments", "customers"])

    @task
    def build_fact(counts: list[int]) -> None:
        ...                                      # one BigQuery job

    @task(trigger_rule="all_done")
    def post_summary() -> None:
        ...                                      # runs even if something above failed

    build_fact(load()) >> post_summary()

sales_daily()
```

**Task groups** (`@task_group` or `TaskGroup`) fold related tasks into one collapsible box in the UI and prefix their IDs (`load.load_table`). They are purely visual and organisational: they do not change scheduling. Airflow 2 also had **SubDAGs**, which ran a whole DAG inside a task; they caused deadlocks and were removed in Airflow 3. Use task groups for grouping and assets or separate DAGs for real boundaries.

**Avoiding circular dependencies.** Inside one DAG the orchestrator rejects cycles when it parses the file. Between pipelines nobody checks for you. A cycle usually sneaks in through a "small" change: the dashboard-refresh pipeline starts writing a cache table that the extract pipeline reads. Python's `graphlib` shows both ideas, the weight each task gets and the cycle detection. This runs as is:

```python
from graphlib import TopologicalSorter, CycleError

deps = {                                   # task: the tasks it waits for
    "extract_orders":    set(),
    "extract_payments":  set(),
    "stg_orders":        {"extract_orders"},
    "stg_payments":      {"extract_payments"},
    "fct_revenue":       {"stg_orders", "stg_payments"},
    "export_finance":    {"fct_revenue"},
    "refresh_dashboard": {"fct_revenue"},
}

children = {t: set() for t in deps}
for task, ups in deps.items():
    for up in ups:
        children[up].add(task)

def descendants(task):
    seen, stack = set(), [task]
    while stack:
        for child in children[stack.pop()]:
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return seen

# weight_rule="downstream": own weight (1) plus one per task downstream of it
for task in TopologicalSorter(deps).static_order():
    print(f"{task:18} {1 + len(descendants(task))}")

deps["extract_orders"] = {"refresh_dashboard"}   # a careless new dependency
try:
    list(TopologicalSorter(deps).static_order())
except CycleError as e:
    print("cycle:", " -> ".join(e.args[1]))
# extract_orders     5
# extract_payments   5
# stg_orders         4
# stg_payments       4
# fct_revenue        3
# export_finance     1
# refresh_dashboard  1
# cycle: extract_orders -> stg_orders -> fct_revenue -> refresh_dashboard -> extract_orders
```

Keep a platform-wide dependency graph (from asset definitions, dbt's manifest or lineage events) and run the same cycle check in CI.

**Priority and priority inversion.** When workers or pool slots are scarce, the scheduler queues the ready tasks with the highest **priority weight** first. Airflow computes an effective weight from each task's `priority_weight` (default 1) and its `weight_rule`: `downstream` (the default: own weight plus all downstream tasks, as printed above), `upstream`, or `absolute`. The downstream rule makes the start of a long chain go first, which shortens the whole run.

**Priority inversion** is when a high-priority task waits on something held by low-priority work. The name comes from operating systems, but the orchestration version is common: a 60-day backfill of a low-value table fills every slot in the shared pool, and the critical 06:00 finance DAG queues behind it. Fixes, from cheapest to strongest:

1. Cap the low-priority work: `max_active_runs` on the backfill, or a small pool just for backfills.
2. Raise the critical DAG's `priority_weight` (or use `weight_rule="absolute"` with a large value so it does not depend on graph shape).
3. Give critical pipelines their own pool, worker queue or executor, so nothing else can occupy their capacity.

**Cross-DAG dependencies.** Real platforms have many pipelines owned by different teams, and one pipeline's output is another's input. There are three ways to express that, shown in the figure below:

| Approach | How it works | Strengths | Weaknesses |
|---|---|---|---|
| Push: `TriggerDagRunOperator` | The producer's last task starts the consumer DAG | Simple, immediate | Producer must know every consumer; tight coupling |
| Pull: `ExternalTaskSensor` | The consumer waits until a task in another DAG succeeded for the matching run | Consumer owns the dependency | Schedules and logical dates must line up; waiting costs slots unless deferrable |
| Data-aware: assets | The producer declares it updates an asset; consumers schedule on that asset | Loose coupling, visible lineage, many consumers | Signals "updated", not "correct"; needs quality checks |

[[fig:orchestration_cross_dag]]

```python
from airflow.sdk import Asset, dag, task

fct_revenue = Asset("sales.fct_revenue")

@dag(schedule="0 3 * * *", catchup=False)
def sales_daily():
    @task(outlets=[fct_revenue])          # success records an asset event
    def build_fact(): ...
    build_fact()

@dag(schedule=[fct_revenue], catchup=False)   # runs after each update
def finance_export():
    @task
    def export(): ...
    export()

sales_daily()
finance_export()
```

> **Pitfall:** Hard-coding another team's DAG and task IDs in an `ExternalTaskSensor`. When they rename a task, your sensor waits until it times out and nothing tells them they broke you. Depend on data (an asset or a table's freshness), not on someone else's implementation.

> **Tip:** Put shared settings (`retries`, `retry_delay`, owner, alert callbacks) in `default_args` or a small internal library, and keep each DAG file short. A reviewer should see the graph's shape in under a minute.

::: deeper
**Why trigger rules matter for skips.** In Airflow, a skipped task propagates "skipped" to downstream tasks that use `all_success`, because skipped is not success. After a `@task.branch`, the join task therefore needs `none_failed_min_one_success`, or it is skipped along with the branch not taken. This is the most common branching bug.

**Setup and teardown tasks.** Since Airflow 2.7 you can mark tasks as setup and teardown (for example, create a temporary cluster, then delete it). A teardown runs even if work tasks failed, and it does not make the DAG run fail on its own when the work succeeded. This is cleaner than `trigger_rule="all_done"` for resource cleanup.

**Cross-DAG cycles with assets.** Asset scheduling makes cycles easy to create: DAG A updates asset X, DAG B is scheduled on X and updates Y, and someone schedules A on Y. Each run then triggers the next forever. Airflow does not refuse this at parse time. Draw the asset graph (the UI has one) and add a CI check.

**Dagster's take.** In Dagster the graph is built from asset dependencies across the whole code location, so cross-pipeline dependencies are the normal case rather than a special operator, and a cycle anywhere fails when definitions load.

> **Interview:** "Two teams' pipelines depend on each other's tables. How do you wire them?" Avoid direct triggers. Each producer declares the asset it updates; consumers schedule on assets (or on a freshness check). Keep a global dependency graph from asset definitions or lineage, check it for cycles in CI, and give critical consumers their own pool or priority so a backfill on one side cannot starve the other.
:::

::: check
?? How big should a task be?
The smallest unit you would want to retry on its own, typically one external job (one BigQuery job, one Spark batch, one file load). Bigger tasks rerun too much work on failure; tiny tasks add scheduling overhead and noise.

?? A branch skips one path and the join task is unexpectedly skipped too. Why, and what is the fix?
The join uses the default `all_success` rule, and a skipped upstream is not a success, so the skip propagates. Use `none_failed_min_one_success` on the join.

?? Give an example of priority inversion in an orchestrator and one fix.
A large low-priority backfill fills a shared pool, so a critical daily DAG queues behind it. Fix it by capping the backfill with `max_active_runs` or its own small pool, or by giving the critical DAG a dedicated pool or a higher absolute priority weight.
:::

::: resources
- Docs: [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) · task design, top-level code, idempotency and testing
- Docs: [Airflow DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html) · dependencies, trigger rules, branching, task groups, setup and teardown
- Docs: [Airflow priority weights](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/priority-weight.html) · how `priority_weight` and `weight_rule` decide queueing order
- Article: [Cross-DAG dependencies (Astronomer)](https://www.astronomer.io/docs/airflow/cross-dag-dependencies) · assets, `TriggerDagRunOperator` and `ExternalTaskSensor` compared with examples
- Docs: [Dagster assets](https://dagster.io/docs/guides/build/assets) · asset-based graph design, where dependencies come from data
- Book: [Data Pipelines with Apache Airflow, 2nd edition](https://www.manning.com/books/data-pipelines-with-apache-airflow-second-edition) · Manning; DAG design, dependencies, scheduling and testing in depth
:::

### Scheduling, sensors and event-driven triggers

::: plain
There are three ways to know when to start cooking dinner. You can start at 18:00 every day whatever happens (a time schedule). You can keep walking to the door to check whether the shopping has arrived (a sensor). Or you can ask the courier to ring the bell when it arrives and do something else until then (an event or a deferred wait). The first is simple but may start before the shopping is there; the second works but wastes your evening walking back and forth; the third is efficient but needs someone to ring the bell.
:::

**Time-based schedules.** Most pipelines still run on a clock, written as a **cron expression**: five fields for minute, hour, day of month, month and day of week. `0 3 * * *` is 03:00 every day; `*/15 * * * *` is every 15 minutes; `0 6 * * 1` is 06:00 on Mondays. Airflow also accepts presets such as `@daily` and `@hourly`, and timedeltas.

Time zones need care. A schedule of `0 2 * * *` in `Europe/Berlin` skips a day when clocks jump from 02:00 to 03:00 in spring and may run twice in autumn. Schedule in UTC, or avoid the hour between 01:00 and 03:00 in zones with daylight saving time, and always give the DAG a time-zone-aware `start_date`.

**What a run is "for".** A run that starts at 03:00 on 23 November usually processes data from 22 November. [Batch Processing Patterns](#pipelines) explains why every query should depend on the interval it is given, not on the clock. Airflow models this with **timetables**, and Airflow 3 changed the default:

| Timetable | A run... | `data_interval_start` / `end` | Default for |
|---|---|---|---|
| `CronTriggerTimetable` | fires at each cron tick | both equal to the tick time | cron strings in Airflow 3 |
| `CronDataIntervalTimetable` | covers the interval between two ticks and starts when it ends | start and end of that interval | cron strings in Airflow 2 |
| `DeltaTriggerTimetable` / `DeltaDataIntervalTimetable` | the same two behaviours for a fixed timedelta | as above | timedelta schedules |
| `MultipleCronTriggerTimetable` | fires on several cron expressions | tick time | irregular calendars |
| `EventsTimetable` | fires at a fixed list of datetimes | event time | one-off dates |

The setting `[scheduler] create_cron_data_intervals` (default `False` in Airflow 3) decides which of the first two a plain cron string gets. Airflow 3 also changed `catchup` to default to `False`: when a DAG is created or unpaused, only the latest run is created, not one for every missed interval. The difference matters. This runs as is and shows how many runs each combination creates:

```python
from datetime import datetime, timedelta, timezone

start = datetime(2026, 11, 18, tzinfo=timezone.utc)      # the DAG's start_date
now = datetime(2026, 11, 23, 2, 30, tzinfo=timezone.utc)  # when the scheduler looks
day = timedelta(days=1)

def interval_runs(catchup):
    """@daily with CronDataIntervalTimetable: a run covers [d, d+1) and starts at d+1."""
    runs, d = [], start
    while d + day <= now:                  # the interval must be over before it runs
        runs.append((d, d + day))
        d += day
    return runs if catchup else runs[-1:]

def trigger_runs(catchup):
    """@daily with CronTriggerTimetable (Airflow 3 default): a run per tick, no interval."""
    runs, d = [], start
    while d <= now:
        runs.append((d, d))                # data_interval_start == data_interval_end
        d += day
    return runs if catchup else runs[-1:]

fmt = lambda t: t.strftime("%m-%d %H:%M")
for name, fn in [("interval", interval_runs), ("trigger", trigger_runs)]:
    for catchup in (True, False):
        runs = fn(catchup)
        print(f"{name:8} catchup={catchup!s:5} {len(runs)} run(s), last covers "
              f"{fmt(runs[-1][0])} to {fmt(runs[-1][1])}")
# interval catchup=True  5 run(s), last covers 11-22 00:00 to 11-23 00:00
# interval catchup=False 1 run(s), last covers 11-22 00:00 to 11-23 00:00
# trigger  catchup=True  6 run(s), last covers 11-23 00:00 to 11-23 00:00
# trigger  catchup=False 1 run(s), last covers 11-23 00:00 to 11-23 00:00
```

> **Pitfall:** Moving a DAG from Airflow 2 to 3 without checking its timetable. Under the Airflow 3 default, `data_interval_start` equals `data_interval_end`, so a filter such as `WHERE ts >= '{{ data_interval_start }}' AND ts < '{{ data_interval_end }}'` selects nothing and the run succeeds with zero rows. If tasks rely on intervals, use `CronDataIntervalTimetable` explicitly (or set `create_cron_data_intervals = True`) and add a row-count check.

**Sensors.** A **sensor** is a task that waits for a condition: a file in Cloud Storage, a partition in BigQuery, a row in a control table, another DAG's task. It checks (pokes) every `poke_interval` seconds until the condition is true or `timeout` passes. Sensors have three ways to wait:

| Mode | While waiting | Cost of 40 sensors waiting 3 hours each |
|---|---|---|
| `mode="poke"` (default) | holds a worker slot and sleeps between checks | 40 slots busy for 3 hours: 120 slot-hours |
| `mode="reschedule"` | releases the slot, is rescheduled for each check | about 1,440 short task runs (one per 5-minute check), each paying start-up overhead |
| `deferrable=True` | hands the wait to the triggerer, no worker slot | 40 small coroutines in one triggerer process |

**Deferrable operators and the triggerer.** A deferrable operator starts its work (submits a BigQuery job, or decides what to wait for), then calls `self.defer(trigger=..., method_name=...)`. The task leaves its worker slot and enters the `deferred` state. The **trigger** (a small piece of `asyncio` code) runs in a separate **triggerer** process, which can watch many triggers concurrently in one event loop. When the trigger fires an event, the scheduler puts the task back in the queue, and a worker resumes it at the named method. Many Google provider operators and sensors accept `deferrable=True`:

```python
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

wait_for_file = GCSObjectExistenceSensor(
    task_id="wait_for_partner_file",
    bucket="acme-landing",
    object="partner/{{ ds }}/_SUCCESS",
    deferrable=True,              # wait in the triggerer, not in a worker slot
    poke_interval=300,
    timeout=6 * 60 * 60,          # fail after 6 hours
)

build = BigQueryInsertJobOperator(
    task_id="build_fct_revenue",
    configuration={"query": {"query": "CALL sales.build_fct_revenue()", "useLegacySql": False}},
    deferrable=True,              # submit the job, then wait for it without a worker
)

wait_for_file >> build
```

[[fig:orchestration_defer]]

**Data-aware scheduling with assets.** A clock is a guess about when data will be ready. **Asset scheduling** removes the guess: a task declares which assets it updates (`outlets`), and a consumer DAG is scheduled on assets instead of a clock. Each successful producer task records an **asset event**; the consumer runs once the condition is met. Airflow 3 renamed datasets to assets and added conditions and combinations:

```python
from airflow.sdk import DAG, Asset, AssetOrTimeSchedule, CronTriggerTimetable

orders = Asset("sales.stg_orders")
payments = Asset("sales.stg_payments")

# Run when both have been updated since the last run
with DAG(dag_id="build_revenue", schedule=(orders & payments)):
    ...

# Run on updates, and also at 06:00 even if nothing changed
with DAG(dag_id="refresh_dashboard",
         schedule=AssetOrTimeSchedule(
             timetable=CronTriggerTimetable("0 6 * * *", timezone="UTC"),
             assets=(orders | payments))):
    ...
```

External systems can create asset events too: through the REST API (push), or through an **AssetWatcher** that attaches an event trigger (for example a message-queue trigger) to an asset, so a message on a queue updates the asset and starts consumers (pull, added in Airflow 3.0). In Dagster the equivalent ideas are **sensors** and **declarative automation** (covered in the Dagster topic below); in Prefect they are **events**, **automations** and deployment triggers.

**Event-driven without an orchestrator.** On Google Cloud, a file landing in Cloud Storage or a message on Pub/Sub can start a Cloud Run job or a Workflows execution through Eventarc, and Cloud Scheduler covers plain cron. These suit simple, irregular arrivals. [Pipelines on Google Cloud](#gcppipes) covers them.

> **Tip:** Combine the two styles. Schedule the consumer on data (assets or events) for freshness, and add a deadline alert or freshness check on a clock, so you also notice when the data *never* arrives.

::: deeper
**How the triggerer scales.** Each triggerer process runs an `asyncio` event loop and accepts up to `[triggerer] capacity` triggers (1,000 by default). Run two or more triggerers for high availability; triggers are assigned to triggerers through the metadata database and reassigned if one dies. Trigger code must never block (no `time.sleep`, no synchronous HTTP clients): one blocking trigger stalls every other trigger in that loop, and Airflow logs a warning when the loop is blocked. On Managed Airflow the triggerer is an environment component whose count and size you set; with zero triggerers, deferrable tasks cannot run.

**Sensor timeouts and soft failures.** `timeout` limits total waiting time; `execution_timeout` limits a single attempt. `soft_fail=True` marks the sensor skipped instead of failed when it times out, useful for optional inputs. Retries on a sensor restart the waiting, so a sensor with `timeout=6h` and `retries=3` can wait 24 hours; decide which you meant.

**Asset events batch by default (before 3.4).** Up to Airflow 3.3, several producer events that arrive before the consumer runs are collected into one consumer run, which can read them from `triggering_asset_events`. Airflow 3.4 changed asset-triggered DAGs to create one run per event by default (the `AssetTriggeredTimetable(batch_asset_events=...)` setting controls it). Check the release notes for your version, because the behaviour decides whether your consumer must handle several updates at once.

**Asset partitions.** Airflow 3.2 and 3.3 added asset partitioning: an asset event can carry a partition key, and mappers decide which downstream partitions to run (for example rolling hourly partitions up to a daily one). This brings Airflow closer to Dagster's partitioned assets; it is new, so test it before relying on it.

> **Interview:** "A partner drops a file sometime between 01:00 and 09:00. How do you schedule the load?" Prefer an event (Cloud Storage notification through Eventarc or an AssetWatcher) or a deferrable sensor with a timeout at 09:30, not a poke-mode sensor that holds a worker all night. Make the load idempotent per file, add a deadline alert for 09:30, and handle late or duplicate files explicitly.
:::

::: check
?? Under Airflow 3 defaults, a DAG with `schedule="0 3 * * *"` uses `data_interval_start` and `data_interval_end` in its SQL filter and loads zero rows. Why?
Cron strings now use `CronTriggerTimetable`, where both values equal the trigger time, so the interval is empty. Use `CronDataIntervalTimetable` (or the config switch) when tasks rely on intervals.

?? What does a deferrable sensor save compared with a poke-mode sensor, and what new component does it need?
It frees the worker slot while waiting, because the wait runs as an `asyncio` trigger in the triggerer process. You must run at least one triggerer.

?? Why combine asset-based scheduling with a clock-based check?
Asset scheduling starts runs when data arrives, but nothing happens when data never arrives. A deadline or freshness check on a clock catches the missing update.
:::

::: resources
- Docs: [Airflow timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html) · trigger versus data-interval timetables and the Airflow 3 default
- Docs: [Airflow deferrable operators and triggers](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html) · how deferral, triggers and the triggerer work, and how to write your own
- Docs: [Airflow asset-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html) · conditional asset schedules, aliases, asset-or-time schedules
- Docs: [Airflow event-driven scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html) · AssetWatcher and event triggers
- Article: [Sensors (Astronomer)](https://www.astronomer.io/docs/airflow/what-is-a-sensor) · poke, reschedule and deferrable modes with examples
- Docs: [Dagster declarative automation](https://dagster.io/docs/guides/automate/declarative-automation) · conditions such as `eager` and `on_cron` instead of schedules
- Docs: [Eventarc overview](https://docs.cloud.google.com/eventarc/docs/overview) · routing Cloud Storage and Pub/Sub events to Cloud Run and Workflows
:::

## Tools

The ideas above are shared by every orchestrator; the tools differ in what they put at the centre. **Apache Airflow** is the most widely deployed: a task-centric scheduler with a huge library of provider packages, available as a managed service on Google Cloud (Managed Airflow). It gets two topics: how it works inside, then how to run it in production. **Dagster** puts data assets at the centre and derives runs from them. **Prefect** makes ordinary Python functions into observable, retryable workflows with very little ceremony. The group ends with a decision guide that also covers Google Cloud's own options, Managed Airflow Gen 3 and Workflows.

Read the Airflow topics even if you plan to use another tool: most interview questions and most existing platforms use Airflow's vocabulary (DAG, task, operator, sensor, executor, XCom), and the other tools define themselves against it.

### Apache Airflow — Internals & Production

::: plain
Think of a restaurant kitchen. Recipes are written on cards (DAG files). One person reads every card and copies it into the order system (the DAG processor). The head chef watches the clock and the order system and calls out which dish is ready to start (the scheduler). A runner passes each call to a free cook (the executor), and the cooks do the actual cooking (workers). Everything that happens is written in one ledger (the metadata database), and the screen in the dining room shows that ledger to everyone (the UI).
:::

Airflow is a set of cooperating processes around a **metadata database** (PostgreSQL in nearly all production setups; MySQL is also supported). Airflow 3 reorganised them so that task code never touches the database directly:

| Component | What it does |
|---|---|
| **DAG bundles** | Where DAG files come from: a local folder, or a Git repository (`GitDagBundle`) whose commits give each DAG a version |
| **DAG processor** | Imports DAG files, turns each DAG into JSON (**serialized DAG**) and stores it in the database. Since Airflow 3 it always runs as its own process |
| **Scheduler** | Reads serialized DAGs, creates DAG runs when schedules or assets say so, decides which task instances are ready, and hands them to the executor |
| **Executor** | Plugin inside the scheduler that decides *where* tasks run: in local processes, on Celery workers, in Kubernetes pods, on remote edge workers |
| **Workers** | Run the task code through the **Task SDK** and report back over the **Execution API** |
| **Triggerer** | Runs `asyncio` triggers for deferred tasks (see the scheduling topic above) |
| **API server** | Serves the React UI, the public REST API and the Execution API that workers talk to (it replaced Airflow 2's webserver) |

[[fig:orchestration_airflow3_arch]]

**DAG parsing.** The DAG processor imports each Python file in the bundle on a loop. A file is re-parsed at most every `min_file_process_interval` seconds (default 30), and the bundle is checked for new files every `refresh_interval` seconds (default 300). Importing a file runs **all of its top-level code**: the code outside task functions. That is why the classic rules exist:

- No database queries, API calls or `Variable.get()` at the top level; they run every 30 seconds per file, forever.
- No heavy imports (pandas, ML libraries) at the top level; import them inside the task function.
- Generating many DAGs from a config file in the repository is fine; generating them from a live database is not (see [Pipeline Design Patterns](#pipelines)).
- `.airflowignore` excludes helper modules and tests from parsing.

You can measure parse time with `airflow dags report`, and a broken import shows up as an import error on the UI's DAG list.

**The scheduler loop.** In each loop the scheduler creates new DAG runs that are due, checks running DAG runs, moves task instances whose upstream tasks are done to `scheduled`, and then enters its **critical section**: it selects scheduled task instances, in priority order, that fit every limit (installation-wide `parallelism`, each DAG's `max_active_tasks`, free slots in each **pool**, executor capacity), marks them `queued` and sends them to the executor. The critical section locks the pool rows in the database so that two schedulers never hand out the same slot. A toy version of the selection runs as is:

```python
# A toy version of the scheduler's "critical section": which scheduled tasks may be queued?
parallelism = 6                                   # free slots across the installation
running_per_dag = {"orders": 2, "crm": 0}
max_active_tasks = {"orders": 3, "crm": 8}        # per-DAG cap
pool_open = {"default_pool": 10, "crm_api": 2}    # free slots per pool

scheduled = [  # (dag, task, pool, pool_slots, priority_weight)
    ("crm",    "load_accounts",  "crm_api",      1, 10),
    ("orders", "stg_orders",     "default_pool", 1, 9),
    ("crm",    "load_contacts",  "crm_api",      1, 8),
    ("orders", "stg_payments",   "default_pool", 1, 7),
    ("crm",    "load_deals",     "crm_api",      1, 6),
    ("crm",    "archive_files",  "default_pool", 2, 3),
]

queued = []
for dag, task, pool, slots, prio in sorted(scheduled, key=lambda t: -t[4]):
    if len(queued) == parallelism:
        print(f"skip  {task:15} parallelism reached"); continue
    if running_per_dag[dag] >= max_active_tasks[dag]:
        print(f"skip  {task:15} {dag} at max_active_tasks"); continue
    if pool_open[pool] < slots:
        print(f"skip  {task:15} pool {pool} full"); continue
    pool_open[pool] -= slots
    running_per_dag[dag] += 1
    queued.append(task)
    print(f"queue {task:15} priority {prio}")
print("sent to executor:", queued)
# queue load_accounts   priority 10
# queue stg_orders      priority 9
# queue load_contacts   priority 8
# skip  stg_payments    orders at max_active_tasks
# skip  load_deals      pool crm_api full
# queue archive_files   priority 3
# sent to executor: ['load_accounts', 'stg_orders', 'load_contacts', 'archive_files']
```

When tasks sit in `scheduled` or `queued` for a long time, one of these limits is the reason; the next topic shows how to find which.

**Executors.** The executor is configured in `[core] executor` and decides where task processes run:

| Executor | Where tasks run | Fits |
|---|---|---|
| `LocalExecutor` | Subprocesses on the scheduler's machine | Development, small single-machine installations |
| `CeleryExecutor` | Long-running worker processes that pull from a broker (Redis or RabbitMQ) | Many short tasks, low start-up latency |
| `KubernetesExecutor` | One new Kubernetes pod per task | Isolation, per-task resources and images, spiky load |
| `EdgeExecutor` | Remote workers that poll the API server over HTTP (edge3 provider) | Workers in other networks or sites |

Since Airflow 2.10 you can configure several executors at once (`executor = CeleryExecutor,KubernetesExecutor`) and choose one per task with `executor="KubernetesExecutor"`; the first in the list is the default. The old hybrid `CeleryKubernetesExecutor` was removed in Airflow 3 in favour of this.

**XComs.** XCom ("cross-communication") is how tasks pass small values: a TaskFlow task's return value is pushed as an XCom, and passing it to another task pulls it. XComs are stored in the metadata database by default, so they are for metadata (a row count, a file path, a job ID), not data. Pass a Cloud Storage path or a table name, never a DataFrame. The next topic covers XCom storage and custom backends.

**Dynamic DAGs.** Airflow supports two kinds. Parse-time generation creates DAGs or tasks in a loop over static configuration. **Dynamic task mapping** creates task instances at run time from upstream results with `.expand()` (and `.partial()` for the arguments that stay fixed), as in the first topic; one mapped task can create up to `max_map_length` instances (default 1,024). Task groups can be mapped too, which repeats a small chain (load, validate, publish) per item.

**DAG versioning.** In Airflow 2 the UI always showed the latest code, even for runs from last month, and a DAG file changed mid-run could make a run use two versions of its code. Airflow 3 records a **DAG version** each time the structure changes. With a versioned bundle such as `GitDagBundle`, a run executes from the same commit from start to finish, and the UI shows each run's graph and code as they were. Airflow 3.3 added a setting (`rerun_with_latest_version`) for whether clears, reruns and backfills use the latest code or the run's original version.

> **Pitfall:** `datetime.now()` or `random` at the top level of a DAG file (for example in a `start_date` or a task ID). The serialized DAG changes on every parse, which creates endless new versions and confuses the scheduler. Use fixed dates and deterministic IDs.

::: deeper
**The Task SDK and the Execution API.** In Airflow 2, every task process connected to the metadata database with full rights. In Airflow 3, a worker starts a small **supervisor** process, which runs the task code in a child process. The task uses the Task SDK (`airflow.sdk`: `dag`, `task`, `Asset`, `Variable`, `Connection`, `BaseOperator`...) and every interaction (fetching a connection, pushing an XCom, reporting state, heartbeating) goes through the supervisor to the API server's Execution API with a short-lived token. Benefits: workers no longer need database credentials, which improves security and cuts database connections; workers can run in remote networks (Edge executor); and tasks can be written in other languages through the same API (experimental Go and Java Task SDKs since 3.3). The cost: DAG code that imported `airflow.models` and queried the database directly breaks, which is the main Airflow 3 migration work.

**High-availability schedulers.** You can run several schedulers against one database. They coordinate through row-level locks (`SELECT ... FOR UPDATE SKIP LOCKED`), which is why HA requires PostgreSQL or MySQL 8+. Two schedulers mostly add resilience; throughput gains are smaller than you might expect because the critical section is serialised by the pool locks. `[scheduler] max_tis_per_query` (default 16) limits how many task instances one pass examines.

**Heartbeats and lost tasks.** A running task heartbeats through the supervisor. If the heartbeats stop (the worker or pod died), the scheduler marks the task failed after a threshold and retries it if retries remain. Because a task can be retried after it actually did its work, idempotency is not optional.

**Why not put data in XComs?** Each XCom is a row written through the API into the database. Large values bloat the database, slow the UI and every query on the `xcom` table, and are held in memory during serialization. The default limit depends on the database column type, but the real limit is good sense: kilobytes, not megabytes.

> **Interview:** "Walk me through what happens between a DAG file being committed and its first task running." The bundle picks up the new commit; the DAG processor imports the file and stores a serialized DAG and a new DAG version; the scheduler sees a due run (schedule or asset), creates a DAG run, sets ready task instances to `scheduled`, checks parallelism, DAG limits and pool slots in its critical section, sets them `queued` and sends them to the executor; the executor starts them on a worker (Celery process or Kubernetes pod); the worker's supervisor runs the task through the Task SDK, which talks to the API server for connections, XComs and state; on completion the scheduler evaluates downstream trigger rules.
:::

::: check
?? Why does `Variable.get("api_key")` at the top level of a DAG file hurt even though it works?
Top-level code runs every time the DAG processor parses the file, about every 30 seconds per file, so it issues constant calls and slows parsing. Read variables inside the task, or through Jinja templates rendered at run time.

?? A task is ready but stays in `scheduled`. Name three limits the scheduler might be enforcing.
Installation-wide `parallelism`, the DAG's `max_active_tasks`, and free slots in the task's pool (also the executor's own capacity).

?? What changed about how task code talks to the metadata database in Airflow 3, and why?
Tasks no longer connect to the database. They go through the Task SDK and the API server's Execution API, which removes database credentials from workers, reduces connections, and allows remote and non-Python workers.
:::

::: resources
- Docs: [Airflow architecture overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html) · the Airflow 3 components and how they talk to each other
- Docs: [Airflow scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html) · the scheduling loop, critical section, HA and tuning settings
- Docs: [Airflow Task SDK](https://airflow.apache.org/docs/task-sdk/stable/index.html) · the `airflow.sdk` interface that DAG authors use in Airflow 3
- Docs: [DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html) · where DAG code comes from and how versioning works
- Docs: [Dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) · `expand`, `partial`, mapped task groups and limits
- Course: [Astronomer Academy](https://academy.astronomer.io/) · free Airflow courses and certification preparation
- Book: [Data Pipelines with Apache Airflow (1st edition)](https://www.oreilly.com/library/view/data-pipelines-with/9781617296901/) · Harenslak and de Ruiter; the classic explanation of Airflow's internals (written for Airflow 2)
:::

### Apache Airflow — Production Operations

::: plain
Knowing how a car engine works is different from running a delivery fleet. Production Airflow is the fleet: how many vans (workers) you need and when to add more, how to keep the depot's single phone line (the metadata database) from being jammed, what each driver may carry in the cab (XComs) versus the trailer (Cloud Storage), and how to change routes (deploy new code) without stranding a van halfway. Most Airflow outages are fleet problems, not engine problems.
:::

**Choosing an executor.** The two production workhorses are Celery and Kubernetes:

| | `CeleryExecutor` | `KubernetesExecutor` |
|---|---|---|
| How it runs a task | A long-running worker process picks it from a Redis or RabbitMQ queue | The scheduler creates one pod per task; the pod exits afterwards |
| Start-up latency | Low (the worker is already running) | A pod start: seconds to more than a minute when a node must be added |
| Isolation | Tasks share a worker's environment and memory | Each task has its own image, CPU and memory requests |
| Idle cost | Workers run (and cost money) even when idle, unless autoscaled | Nothing runs when there are no tasks |
| Extra infrastructure | A broker, plus a result backend (usually the metadata database) | A Kubernetes cluster (GKE) |
| Good for | Many short tasks, steady load | Heterogeneous tasks, strict isolation, spiky load |

Many platforms combine them with multiple executors: Celery as the default for the thousands of short "submit a BigQuery job and wait" tasks, Kubernetes for the few tasks that need a special image or 16 GB of memory:

```python
@task(executor="KubernetesExecutor",
      executor_config={"pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[
          k8s.V1Container(name="base",
                          resources=k8s.V1ResourceRequirements(requests={"memory": "16Gi"}))]))})
def train_model(): ...
```

(`k8s` is `kubernetes.client.models` from the Kubernetes Python client.) An alternative that keeps the scheduler's choice simple is to run heavy work outside Airflow entirely: a `KubernetesPodOperator`, a Cloud Run job, a Spark batch on Managed Service for Apache Spark (formerly Dataproc) or a Dataflow job, with Airflow only starting and watching it. That is usually the better design.

**Sizing and scaling.** Concurrency is capped at several levels, and the smallest one wins:

| Setting | Default | Limits |
|---|---|---|
| `[core] parallelism` | 32 | Running task instances per scheduler |
| `[core] max_active_tasks_per_dag` | 16 | Running tasks in one DAG (override with `max_active_tasks`) |
| `[core] max_active_runs_per_dag` | 16 | Concurrent runs of one DAG (override with `max_active_runs`) |
| `default_pool` slots | 128 | Tasks without an explicit pool |
| `[celery] worker_concurrency` | 16 | Task processes per Celery worker |

A worked example: 300 tasks must run in a 2-hour window, each about 4 minutes, mostly waiting on BigQuery. That is 1,200 task-minutes, so you need about 10 concurrent slots on average; allowing for bursts at the start of the window, plan for 32. With `worker_concurrency=16` that is two Celery workers, and `parallelism` must be at least 32. If the same tasks used deferrable operators, the waiting would move to the triggerer and the worker need would drop to a handful of slots. Scale Celery workers on queue length (the official Helm chart can use **KEDA**, a Kubernetes autoscaler driven by a metric such as queued tasks), and make sure the warehouse, not only Airflow, can take the extra concurrency.

**The metadata database and connection pooling.** Every component holds a pool of database connections through SQLAlchemy (`[database] sql_alchemy_pool_size`, default 5, plus `sql_alchemy_max_overflow`, default 10). In Airflow 2, every running task also opened connections, so 200 running tasks could exhaust a PostgreSQL server's connection limit. The standard fix is **PgBouncer**, a connection pooler between Airflow and PostgreSQL (the Helm chart can deploy it). Airflow 3 helps a lot because tasks go through the API server instead. Keep the database healthy by:

- Cleaning old rows regularly with `airflow db clean --clean-before-timestamp ...` (task instances, logs, XComs, DAG runs). Tables with tens of millions of rows slow the scheduler and the UI.
- Giving it enough CPU and IOPS: the scheduler's critical section is a stream of small transactions.
- Backing it up, and upgrading it with `airflow db migrate` as part of every Airflow upgrade.

**XCom internals.** An XCom is a row keyed by DAG, run, task, map index and key. Values are serialized to JSON by Airflow's serializer (which also knows datetimes, dataclasses and a few other types). The `xcom_backend` setting replaces the storage; the common.io provider's object storage backend keeps small values in the database and writes values above a threshold to Cloud Storage, storing only the path:

```bash
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=gs://google_cloud_default@acme-airflow-xcom/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=65536      # bytes; smaller values stay in the database
```

Even with such a backend, the right design is to write data to its proper home (a BigQuery table, a Parquet file) and pass the reference.

**DAG serialization.** The DAG processor stores each DAG as JSON in the `serialized_dag` table (refreshed at most every `min_serialized_dag_update_interval`, 30 seconds by default), together with a hash. The scheduler and API server never import your Python files; they work from this JSON. Consequences: a DAG that parses on your laptop but not in the DAG processor's environment (a missing package) simply does not appear or keeps its last good version, so watch import errors; and custom objects in DAG arguments must be serializable.

**The TaskFlow API in production.** TaskFlow (the `@task` decorator, introduced in Airflow 2.0 and imported from `airflow.sdk` in Airflow 3) turns Python functions into tasks and wires XComs automatically from function calls. Use it for Python logic and glue. For calls to external systems, prefer the tested provider operators (`BigQueryInsertJobOperator`, `DataprocSubmitJobOperator`, `CloudRunExecuteJobOperator`), which already handle job IDs, retries, deferral and cancellation. Mixing both styles in one DAG is normal: `build_fact(load()) >> BigQueryInsertJobOperator(...)`.

**Task states and where time goes.** A task instance moves through a small state machine. Knowing it turns "Airflow is slow" into a specific diagnosis:

[[fig:orchestration_ti_states]]

| Stuck in | Usually means |
|---|---|
| `scheduled` | A limit: parallelism, DAG max active tasks, pool slots full |
| `queued` | The executor cannot start it: no free Celery worker, pods pending on GKE, broker problems |
| `running` for too long | The external job is slow or hung; set `execution_timeout` |
| `deferred` for too long | The trigger has not fired; check the triggerer logs and that a triggerer is running |
| `up_for_retry` repeatedly | A real fault being retried; alert when retries are exhausted |

**Running it on Google Cloud.** Managed Airflow Gen 3 is managed Airflow: Google runs the scheduler, DAG processor, triggerer, workers, API server and database; you upload DAGs to the environment's Cloud Storage bucket (or deploy them from CI), choose an image with your Airflow version (Composer 3 offers Airflow 2.11 and Airflow 3 images), install PyPI packages, and set worker autoscaling limits. Self-managing Airflow on GKE with the official Helm chart gives more control and more work. [Pipelines on Google Cloud](#gcppipes) covers Composer operations in depth; CI/CD for DAGs is in [DataOps and platform engineering](#dataops).

```bash
# Deploy DAGs to a Composer environment and add a package (run from CI, not by hand)
gcloud composer environments storage dags import \
  --environment=prod-composer --location=europe-west1 --source=dags/
gcloud composer environments update prod-composer --location=europe-west1 \
  --update-pypi-package="apache-airflow-providers-dbt-cloud>=4.0"
```

> **Pitfall:** Upgrading Airflow by bumping the version and hoping. Airflow 3 removed SubDAGs, SLAs (replaced by Deadline Alerts), `schedule_interval` (use `schedule`), `execution_date` in templates, and direct database access from tasks. Run Ruff's Airflow rules (`ruff check --select AIR3`) over your DAGs, test in a staging environment with a copy of production DAGs, and upgrade the database with a backup in hand.

> **Tip:** Treat the Airflow metadata as data. Query run durations, queue times and retry counts from the REST API or the database into BigQuery, and chart them. Slowly rising queue time is the earliest sign that you need more workers or fewer tasks.

::: deeper
**Celery internals.** Each Celery worker runs `worker_concurrency` task processes. The scheduler sends a message per task to a named queue (`default` unless the task sets `queue=`), and workers subscribe to one or more queues; dedicated queues are how you reserve capacity for critical DAGs or route tasks to workers with special network access. With `task_acks_late`, a task message is acknowledged only after it finishes, so a worker that dies does not lose it. Graceful shutdown matters: when autoscaling removes a worker, it must finish (or hand back) its running tasks first, which is why Celery workers on Kubernetes need a long `terminationGracePeriodSeconds`.

**Kubernetes executor internals.** The scheduler watches pod events and maps them back to task instances. Pods that cannot be scheduled stay pending until `[kubernetes_executor] worker_pods_pending_timeout` and then fail the task. Pod start time dominates short tasks, so the KubernetesExecutor is a poor fit for thousands of 10-second tasks. A `pod_template_file` sets defaults (service account, node selector, sidecars) for every pod; on GKE, Workload Identity lets the pod's Kubernetes service account act as a Google service account without keys.

**Scheduler performance.** Throughput problems usually come from: too many DAG files or slow parsing (raise `parsing_processes`, simplify top-level code), a slow or bloated database (clean it), many tiny tasks (merge them), and very large mapped tasks (each instance is a row and a scheduling decision). The scheduler emits metrics (StatsD or OpenTelemetry) such as loop duration and the number of queued and running tasks; graph them.

**Deadline Alerts replace SLAs.** Airflow 2's SLA misses were checked only after tasks finished and were removed in 3.0. Airflow 3.1 added Deadline Alerts: a reference time (for example when the run was queued), an interval, and a callback that fires as soon as the deadline passes, even while the run is still hanging. [Pipeline Monitoring & Alerting](#dataops) covers alerting strategy.

> **Interview:** "Tasks sit in `queued` for 20 minutes every morning at 03:00. What do you check?" Queued means the executor has not started them: check worker count and autoscaling (Celery queue length, KEDA limits) or pending pods and node autoscaling on GKE; check that tasks route to a queue that has workers; check broker health. Then reduce the burst: spread schedules, use deferrable operators for waiting tasks, and give critical DAGs a dedicated queue.
:::

::: check
?? When does the KubernetesExecutor beat the CeleryExecutor, and when does it lose?
It wins when tasks need different images or large, isolated resources, or when load is spiky (nothing runs when idle). It loses for many short tasks, where pod start-up time dominates and long-running Celery workers start tasks almost instantly.

?? Why do large Airflow installations put PgBouncer in front of PostgreSQL?
Every component, and in Airflow 2 every running task, opens database connections. A pooler multiplexes them onto a small number of real connections so the database's connection limit is not exhausted.

?? A task needs to give a 2 GB DataFrame to the next task. What should it pass?
A reference: it writes the data to a table or a file in Cloud Storage and returns the table name or path as the XCom. XComs are for small metadata.
:::

::: resources
- Docs: [Airflow executors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html) · executor types, multiple executors and per-task selection
- Article: [Airflow executors explained (Astronomer)](https://www.astronomer.io/docs/airflow/airflow-executors-explained) · Celery, Kubernetes and local executors compared in practice
- Docs: [Airflow production deployment](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html) · database, executor, logging and scaling choices for production
- Docs: [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html) · breaking changes, Ruff rules and the migration path
- Docs: [Airflow Helm chart](https://airflow.apache.org/docs/helm-chart/stable/index.html) · running Airflow on GKE with KEDA autoscaling and PgBouncer
- Docs: [Managed Airflow Gen 3 overview](https://docs.cloud.google.com/composer/docs/composer-3/composer-overview) · what Google manages and how environments are structured
- Article: [Scaling Airflow to optimize performance (Astronomer)](https://www.astronomer.io/docs/airflow/airflow-scaling-workers) · how the concurrency settings interact, with tuning advice
:::

### Dagster — Asset-Based Orchestration

::: plain
Airflow thinks like a factory foreman with a list of jobs: "at 03:00, run these steps in this order". Dagster thinks like a librarian with a catalogue: "these are the tables we keep, this is what each is made from, and this one is out of date". You describe the things you want to exist (assets) and how each is built from others; Dagster works out what needs to run, keeps a history of every version of every asset, and can rebuild any day of any table on request.
:::

**Software-defined assets.** An **asset** is a persistent piece of data: a BigQuery table, a file in Cloud Storage, a model. In Dagster you define each asset as a decorated Python function that builds it, and its dependencies are the assets it reads. The graph of the whole platform is the union of these definitions, so lineage is not a separate tool: it *is* the code.

[[fig:orchestration_assets]]

```python
import dagster as dg
from dagster_gcp import BigQueryResource

daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")

@dg.asset(partitions_def=daily, group_name="sales", kinds={"bigquery"})
def stg_orders(context: dg.AssetExecutionContext, bigquery: BigQueryResource) -> dg.MaterializeResult:
    day = context.partition_key                       # for example "2026-11-22"
    with bigquery.get_client() as client:
        job = client.query(f"""
            DELETE FROM sales.stg_orders WHERE order_date = DATE '{day}';
            INSERT INTO sales.stg_orders
            SELECT * FROM raw.orders WHERE DATE(created_at) = DATE '{day}';
        """)
        job.result()
    return dg.MaterializeResult(metadata={"bigquery_job": job.job_id})

@dg.asset(partitions_def=daily, deps=[stg_orders], group_name="sales",
          automation_condition=dg.AutomationCondition.eager())
def fct_revenue(context: dg.AssetExecutionContext, bigquery: BigQueryResource) -> None:
    ...                                               # rebuild this day's revenue

@dg.asset_check(asset=fct_revenue)
def revenue_not_negative(bigquery: BigQueryResource) -> dg.AssetCheckResult:
    with bigquery.get_client() as client:
        rows = list(client.query(
            "SELECT COUNT(*) AS n FROM sales.fct_revenue WHERE revenue < 0").result())
    return dg.AssetCheckResult(passed=rows[0].n == 0, metadata={"negative_rows": rows[0].n})

defs = dg.Definitions(
    assets=[stg_orders, fct_revenue],
    asset_checks=[revenue_not_negative],
    resources={"bigquery": BigQueryResource(project="acme-prod")},
)
```

The pieces:

- **Resources** (`BigQueryResource`) are configured clients injected by parameter name. Tests pass a fake resource instead, so asset code is easy to unit-test.
- **Materializing** an asset means running its function. Each materialization is recorded with metadata (rows, job ID, code version), so the UI shows when every partition of every asset was last built and by which run.
- **Asset checks** attach data quality tests to an asset; a failed blocking check can stop downstream materialization.
- **Definitions** collect everything in a **code location**; `dg dev` starts the UI locally, and `dg check defs` validates definitions in CI.

**Partitions.** A **partitions definition** splits an asset into slices: `DailyPartitionsDefinition`, `HourlyPartitionsDefinition`, `MonthlyPartitionsDefinition`, `StaticPartitionsDefinition(["eu", "us"])`, `DynamicPartitionsDefinition` (keys added at run time, such as one per customer file), and `MultiPartitionsDefinition` (two dimensions, such as date and region). Each partition is materialized and tracked separately, so the UI can show "312 of 330 days materialized, 2 failed". **Partition mappings** describe how partitions depend on each other when they are not one to one, for example `TimeWindowPartitionMapping(start_offset=-6)` for a rolling 7-day asset that reads the last seven daily partitions.

**Backfills.** A backfill is a request to materialize a set of partitions of a set of assets, launched from the UI or the API. Dagster orders it along the asset graph (day D of `stg_orders` before day D of `fct_revenue`) and tracks progress per partition. By default each partition is a separate run. A **backfill policy** (`BackfillPolicy.single_run()` or `multi_run(max_partitions_per_run=N)`) lets one run process a range of partitions, and the asset reads `context.partition_key_range` (or the time window) instead of a single key. That suits engines where one query over 30 days is much cheaper than 30 queries. Planning ranges from missing partitions is simple; this runs as is:

```python
from datetime import date, timedelta

def daily_keys(start, end):
    """Partition keys like DailyPartitionsDefinition: one per day, end excluded."""
    d, keys = start, []
    while d < end:
        keys.append(d.isoformat())
        d += timedelta(days=1)
    return keys

keys = daily_keys(date(2026, 11, 1), date(2026, 11, 11))
materialized = {"2026-11-01", "2026-11-02", "2026-11-05", "2026-11-06", "2026-11-10"}
missing = [k for k in keys if k not in materialized]
print(f"{len(keys)} partitions, {len(missing)} missing: {missing}")

# Group the missing keys into contiguous ranges: one range per run, not one key per run.
ranges, run = [], [missing[0]]
for prev, cur in zip(missing, missing[1:]):
    if date.fromisoformat(cur) - date.fromisoformat(prev) == timedelta(days=1):
        run.append(cur)
    else:
        ranges.append((run[0], run[-1])); run = [cur]
ranges.append((run[0], run[-1]))
print("runs:", ranges)
# 10 partitions, 5 missing: ['2026-11-03', '2026-11-04', '2026-11-07', '2026-11-08', '2026-11-09']
# runs: [('2026-11-03', '2026-11-04'), ('2026-11-07', '2026-11-09')]
```

**IO managers.** When an asset function *returns* data (a pandas DataFrame), an **IO manager** decides where it is stored and how the downstream asset loads it as an input: `BigQueryPandasIOManager` writes DataFrames to BigQuery tables, a Cloud Storage IO manager pickles objects to a bucket. This keeps storage code out of business logic and lets tests swap in an in-memory manager. For SQL that runs *inside* the warehouse, as in the example above, you do not need one: declare `deps` and run the SQL with a resource.

**Automation: schedules, sensors and declarative automation.** Dagster offers three ways to start runs:

- **Schedules** run a job (a selection of assets) on cron: `dg.ScheduleDefinition(job=orders_job, cron_schedule="0 3 * * *")`.
- **Sensors** are functions the daemon calls every 30 seconds by default (configurable with `minimum_interval_seconds`); they look at something external and return `RunRequest`s or a `SkipReason`. A `run_key` on each request makes the sensor idempotent: Dagster never launches two runs with the same key.
- **Declarative automation** puts the rule on the asset itself: `AutomationCondition.eager()` (materialize when upstream changes), `on_cron("0 6 * * *")` (once per tick after upstreams have updated), `on_missing()` (fill partitions that are missing once upstreams exist). Conditions are composable expressions, evaluated by the `default_automation_condition_sensor`, which you switch on in the UI.

```python
orders_job = dg.define_asset_job("orders_job", selection=[stg_orders])

@dg.sensor(job=orders_job, minimum_interval_seconds=120)
def partner_file_sensor(context: dg.SensorEvaluationContext):
    for day in new_success_markers(since=context.cursor):    # your helper: list _SUCCESS files
        yield dg.RunRequest(run_key=f"orders-{day}", partition_key=day)
    context.update_cursor(latest_marker_seen())
```

> **Pitfall:** Treating every SQL statement as an asset with an IO manager. Returning large DataFrames through Python to write them back to BigQuery moves data out of the warehouse and back. Let the warehouse do the work and use Dagster to orchestrate and record it.

> **Tip:** Use the dbt integration (`dagster-dbt`) when you have a dbt project: each dbt model becomes an asset, so dbt models, Python ingestion assets and downstream exports appear in one graph with one lineage view.

::: deeper
**Architecture.** A Dagster deployment has a **webserver** (UI and GraphQL API), a **daemon** (schedules, sensors, the run queue, backfills, declarative automation), one or more **code locations** (gRPC servers that load your definitions in their own Python environment, so teams can use different dependencies), and **storage** in PostgreSQL (runs, the event log, schedules state). A **run launcher** decides where each run starts (a process, a Docker container, a Kubernetes job), and the run's **executor** decides how its steps run (multiprocess by default, or one Kubernetes pod per step with `k8s_job_executor`). **Concurrency pools** limit how many steps that touch the same resource run at once, like Airflow pools. **Dagster+** is the managed service, in Serverless or Hybrid form (Hybrid runs an agent and your code in your own cloud, such as GKE, while Dagster hosts the control plane).

**Components and the `dg` CLI.** Recent Dagster versions add **components**: reusable, configurable building blocks declared in YAML (for example a dbt project or a set of Python scripts) that generate assets, plus the `dg` command-line tool to scaffold, check and run projects. They help platform teams give analysts a "fill in the YAML" path while keeping full Python for engineers.

**Versioning and staleness.** Assets can declare a `code_version`, and materializations record data versions. The UI can then mark an asset as stale when its code or an upstream data version changed, which is the basis for "rebuild only what changed" automation.

**Dagster versus Airflow assets.** Airflow 3's assets let DAGs schedule each other on data updates, and Airflow 3.2 and 3.3 added partitioned assets, but the unit of execution is still the DAG and its tasks. In Dagster the asset is the unit: per-partition status, checks, versioning and backfills are first-class. Teams migrating from Airflow often start by wrapping existing DAGs as external assets so the lineage is visible before code moves.

> **Interview:** "What does asset-based orchestration give you over task-based?" The graph describes data, not steps, so lineage, freshness and per-partition status come for free; backfills are "materialize these partitions" rather than "rerun these dates of this DAG"; and automation can be declared per asset (update when upstream changes, or at least daily), so cross-team dependencies are normal edges instead of sensors.
:::

::: check
?? What is a software-defined asset, and where do its dependencies come from?
A Python definition of a persistent data object (a table, file or model) and the function that builds it. Its dependencies are the assets it reads, declared with `deps` or function inputs, so the graph is derived from the definitions.

?? When do you need an IO manager, and when not?
When asset functions return in-memory objects (DataFrames) that must be stored and loaded by downstream assets. Not when the asset runs SQL inside the warehouse; then declare `deps` and use a resource.

?? Why give a sensor's `RunRequest` a `run_key`?
It makes the sensor idempotent: Dagster launches at most one run per key, so re-evaluating the same files never starts duplicate runs.
:::

::: resources
- Docs: [Dagster assets](https://dagster.io/docs/guides/build/assets) · defining assets, dependencies, metadata and materialization
- Docs: [Partitions and backfills](https://dagster.io/docs/guides/build/partitions-and-backfills) · partition types, mappings, backfill policies
- Docs: [IO managers](https://dagster.io/docs/guides/build/io-managers) · when to use them and when resources are the better choice
- Docs: [Sensors](https://dagster.io/docs/guides/automate/sensors) · cursors, run keys and asset sensors
- Course: [Dagster University](https://courses.dagster.io/) · free courses on essentials, dbt with Dagster and testing
- Blog: [What are software-defined assets?](https://dagster.io/blog/software-defined-assets) · the design argument for asset-based orchestration
- Video: [Dagster on YouTube](https://www.youtube.com/@dagsterio) · talks, demos and deep dives from the Dagster team
:::

### Prefect — Modern Workflow Orchestration

::: plain
Prefect takes the Python script you already have and gives it a safety harness. Add `@flow` to the main function and `@task` to the steps, and every run is recorded, failed steps retry, and results can be reused instead of recomputed. When you want it to run every morning on a server instead of your laptop, you give it a schedule and tell it which kind of machine to use. There is no separate DAG language to learn: the order of the steps is simply the order your code calls them.
:::

**Flows and tasks.** A **flow** is a decorated Python function and the unit you deploy and run; a **task** is a decorated function called inside a flow and the unit of retries, caching and concurrency. The dependency graph is discovered while the code runs, so ordinary Python (`if`, `for`, `try`) controls it, and branches or loops whose size is only known at run time need no special operators.

```python
from datetime import timedelta
from prefect import flow, task, unmapped
from prefect.cache_policies import INPUTS, TASK_SOURCE
from prefect.concurrency.sync import concurrency

@task(retries=3, retry_delay_seconds=[10, 60, 300],          # three retries, growing waits
      cache_policy=INPUTS + TASK_SOURCE, cache_expiration=timedelta(days=1))
def extract(table: str, day: str) -> int:
    with concurrency("crm_api", occupy=1):                   # global limit across all runs
        ...                                                  # call the API, write to GCS
    return 1200

@task
def build_fact(day: str, counts: list[int]) -> None:
    ...                                                      # run one BigQuery job

@flow(log_prints=True)
def sales_daily(day: str):
    futures = extract.map(["orders", "payments", "customers"], day=unmapped(day))
    counts = futures.result()                                # wait for all, raise on failure
    build_fact(day, counts)
    print(f"loaded {sum(counts)} rows for {day}")

if __name__ == "__main__":
    sales_daily("2026-11-22")                                # runs locally, recorded in Prefect
```

Calling a task directly runs it and returns its result; `.submit()` runs it concurrently through the flow's **task runner** (a thread pool by default; Dask and Ray task runners exist for distributed work) and returns a **future**; `.map()` submits one task run per element.

> **Pitfall:** Forgetting `unmapped` for a constant argument to `.map()`. A string is iterable, so `extract.map(tables, day="2026-11-22")` tries to map over the characters of the date. Wrap constants in `unmapped(...)`.

**States.** Every flow and task run moves through states: `Scheduled`, `Pending`, `Running`, then a final state such as `Completed`, `Failed`, `Crashed` (the infrastructure died, not your code), or `Cancelled`, plus `Retrying`, `Paused` and `Late` (a scheduled run that has not started on time). `Cached` is a kind of `Completed`. States drive retries, alerts and automations.

**Caching and results.** A task's **cache policy** computes a key from its inputs and context; if a completed run with the same key exists and has not expired, Prefect returns the stored result instead of running the task. The default policy includes the flow run ID, so caching only applies within a run (useful on retries); `INPUTS + TASK_SOURCE` reuses results across runs until the inputs or the task's code change. Results are persisted to result storage (local disk by default, or a Cloud Storage bucket via the `prefect-gcp` integration). The mechanism is simple; this runs as is:

```python
import hashlib, inspect, json

cache = {}          # Prefect persists results to storage; a dict stands in for it here
calls = 0

def cache_key(fn, kwargs):
    """Like cache_policy=INPUTS + TASK_SOURCE: hash the arguments and the function's code."""
    payload = json.dumps(kwargs, sort_keys=True) + inspect.getsource(fn)
    return hashlib.sha256(payload.encode()).hexdigest()[:12]

def cached_task(fn):
    def wrapper(**kwargs):
        key = cache_key(fn, kwargs)
        if key in cache:
            return cache[key], "Cached"
        cache[key] = fn(**kwargs)
        return cache[key], "Completed"
    return wrapper

@cached_task
def daily_total(day, region):
    global calls
    calls += 1
    return {"day": day, "region": region, "total": 1200}

print(daily_total(day="2026-11-22", region="eu")[1])
print(daily_total(day="2026-11-22", region="eu")[1])
print(daily_total(day="2026-11-23", region="eu")[1])
print("real executions:", calls)
# Completed
# Cached
# Completed
# real executions: 2
```

**Deployments, work pools and workers.** Running a flow on a schedule, remotely, needs three things:

| Concept | What it is |
|---|---|
| **Deployment** | A flow plus where its code lives, its default parameters, schedules and triggers. Created with `flow.serve()`, `flow.deploy()` or `prefect deploy` from a `prefect.yaml` file |
| **Work pool** | A queue of scheduled runs plus a template for the infrastructure they run on: `process`, `docker`, `kubernetes`, or a Google Cloud type such as Cloud Run |
| **Worker** | A long-running process that polls a work pool and starts each run on that infrastructure (`prefect worker start --pool my-pool`) |

**Push work pools** remove the worker: Prefect Cloud submits each run directly to a serverless service. On Google Cloud, `prefect work-pool create --type cloud-run:push --provision-infra my-cloud-run-pool` sets one up, and each flow run becomes a Cloud Run job execution. The simplest option of all, `flow.serve(cron="0 3 * * *")`, runs a small process that both schedules and executes the flow, good for one machine. Prefect 2's **agents** were replaced by workers and are gone in Prefect 3.

```yaml
# prefect.yaml (excerpt): deploy with `prefect deploy --all`
deployments:
- name: sales-daily
  entrypoint: flows/sales.py:sales_daily
  work_pool:
    name: my-cloud-run-pool
  parameters:
    day: "2026-11-22"
  schedules:
  - cron: "0 3 * * *"
    timezone: "Europe/Berlin"
```

**Events and automations.** Prefect records **events** for state changes and anything you emit yourself, and **automations** react to them: "if a run of `sales-daily` stays `Running` for more than two hours, cancel it and notify the channel", or "when the partner webhook event arrives, run the `partner-load` deployment". Deployment **triggers** start deployments from events, which gives event-driven pipelines without polling sensors.

**Prefect Cloud versus self-hosted.** The open-source Prefect server (`prefect server start`, backed by SQLite or PostgreSQL) provides the API, UI, scheduling, events and automations. **Prefect Cloud** is the hosted version with workspaces, users and roles, push work pools, audit logs and service-level features. In both, your code and data stay on your own infrastructure: workers pull run instructions, so nothing needs inbound access to your network.

::: deeper
**Hybrid execution model.** The Prefect API only stores metadata and orchestration state; your code runs wherever a worker or served process runs. Task runs report state to the API as they happen. If the infrastructure disappears mid-run (a Cloud Run instance killed, a pod evicted), the run is marked `Crashed` by heartbeat or infrastructure checks rather than silently hanging, and automations can retry it.

**Transactions.** Prefect 3 added transactions: a group of tasks whose results are committed together, with `on_rollback` hooks that undo side effects (delete a staged file) if a later task in the group fails. Combined with caching keyed on inputs, this gives idempotent, resumable flows without writing state tables yourself.

**Concurrency.** Global concurrency limits (`concurrency("crm_api", occupy=1)`) and rate limits are named counters on the server, shared by every run, like Airflow pools. Work pools and their queues also have concurrency limits, which cap how many flow runs start at once on given infrastructure.

**Assets.** Recent Prefect 3 releases add assets: a task decorated with `@materialize("gs://acme-curated/sales/fct_revenue")` records that it updated that asset, and Prefect builds an asset graph from the tasks' inputs. It is lighter than Dagster's model: the flow and its tasks remain the unit of execution, and assets add lineage and visibility on top.

> **Interview:** "When would you pick Prefect over Airflow?" When the work is Python-heavy and dynamic (the shape depends on data at run time), the team wants plain functions and local runs without a DAG framework, and you want serverless execution per run (Cloud Run push pools) rather than an always-on cluster. Airflow wins when you need its provider ecosystem, a managed Google Cloud service (Composer), or a large existing estate.
:::

::: check
?? How does Prefect know the order of tasks in a flow?
From the code as it runs: tasks run when they are called, and passing one task's result or future to another creates the dependency. There is no separate graph definition.

?? What is the difference between a work pool and a worker?
A work pool is a queue of scheduled runs plus an infrastructure template (process, Docker, Kubernetes, Cloud Run). A worker is the process that polls the pool and launches each run on that infrastructure. Push pools need no worker.

?? Why does the default cache policy not reuse yesterday's result for the same inputs?
The default policy includes the flow run ID in the key, so cached results are only reused within the same run, such as on retries. To reuse across runs, choose a policy like `INPUTS + TASK_SOURCE` with an expiration.
:::

::: resources
- Docs: [Prefect flows](https://www.prefect.io/docs/v3/concepts/flows) · defining flows, parameters, subflows and states
- Docs: [Prefect tasks](https://www.prefect.io/docs/v3/concepts/tasks) · retries, caching, concurrency, mapping and futures
- Docs: [Work pools](https://www.prefect.io/docs/v3/concepts/work-pools) · infrastructure types, push pools and work queues
- Docs: [Deployments](https://www.prefect.io/docs/v3/concepts/deployments) · `serve`, `deploy`, `prefect.yaml`, schedules and triggers
- Docs: [Serverless push work pools](https://www.prefect.io/docs/v3/how-to-guides/deployment_infra/serverless) · running each flow run as a Cloud Run job
- Docs: [Automations](https://www.prefect.io/docs/v3/concepts/automations) · reacting to events and states
- Blog: [Prefect blog](https://www.prefect.io/blog) · release notes, design articles and production stories
:::

### Choosing an orchestrator

::: plain
Choosing an orchestrator is like choosing how to get around a city. A bicycle (a cron job) is cheap and perfect for short, simple trips. A bus network (Airflow on Managed Airflow) is big, everyone knows how to use it, and it goes almost everywhere, but it costs money even when it is empty. A taxi app (a serverless workflow service) costs nothing while you wait and you pay per trip, but it is not built for moving house. The right choice depends on the trips you make every day, not on which vehicle is the most impressive.
:::

**First, what kind of orchestration do you need?** Three different jobs share the word "orchestration":

1. **Data pipeline orchestration**: many scheduled or data-triggered pipelines with dependencies, backfills, partitions and a run history that analysts and engineers inspect. Airflow, Dagster and Prefect live here.
2. **Service orchestration**: a sequence of API calls between services (call a Cloud Run service, wait for a long-running operation, branch on the answer, notify someone), often triggered by events. Google Cloud Workflows lives here, as do general workflow engines such as Temporal.
3. **In-warehouse SQL dependencies**: run these SQL models in dependency order. dbt (with any scheduler) or Dataform, BigQuery's built-in SQL workflow tool, already do this; you may only need something to start them.

Many platforms use two of these: Workflows to glue services together, and Airflow or Dagster for the data pipelines. That is fine, as long as each pipeline has one owner and one place where its runs are visible.

**The main options compared.**

| | Managed Airflow Gen 3 (Airflow) | Dagster (OSS or Dagster+) | Prefect 3 (OSS or Cloud) | Workflows | Cloud Scheduler plus Cloud Run jobs |
|---|---|---|---|---|---|
| Model | Task DAGs, plus assets | Assets with partitions | Python flows and tasks | Steps in YAML or JSON | One cron trigger per job |
| Strengths | Huge provider ecosystem, most hiring familiarity, managed on Google Cloud | Lineage, per-partition status, backfills, asset checks, dbt integration | Plain Python, dynamic flows, serverless runs per flow | Serverless, pay per step, long waits and callbacks, native Google API connectors | Minimal moving parts |
| Weak spots | Always-on environment cost, scheduler latency of seconds, operational knobs | Smaller ecosystem than Airflow; managed option is a third party | Smaller ecosystem; many features best in Prefect Cloud | Not a data platform: no backfill tooling, partitions or rich run history across pipelines | No dependencies, retries across jobs or history beyond logs |
| Runs on Google Cloud as | Fully managed service | Self-hosted on GKE, or Dagster+ Hybrid agent on GKE | Self-hosted server on GKE or Cloud Run, or Prefect Cloud with Cloud Run push pools | Fully managed service | Fully managed services |
| Typical fit | Many teams, many pipelines, Google Cloud native | Analytics platforms built around dbt and data products | Python-heavy teams, ML and dynamic workloads | Event-driven service glue, a few simple pipelines | Fewer than a handful of independent jobs |

**A decision guide.** Answer these in order:

1. **How many pipelines, and do they depend on each other?** Fewer than five independent jobs: Cloud Scheduler triggering Cloud Run jobs (or dbt Cloud's or Dataform's own schedules) is enough. Dozens with dependencies and backfills: you need a real orchestrator.
2. **Is the work mostly calling Google Cloud APIs in response to events?** Workflows, triggered by Eventarc or Cloud Scheduler.
3. **Does the team think in tables and data products, with dbt at the centre?** Dagster's asset model fits naturally; Airflow with assets and the Cosmos dbt integration is a solid alternative.
4. **Must it be a Google-managed service (procurement, support, data residency)?** Managed Airflow Gen 3.
5. **Is the work dynamic Python (ML, per-customer fan-out decided at run time) and do you want pay-per-run infrastructure?** Prefect with Cloud Run push pools, or Dagster.
6. **What do you already run, and what does the team know?** Migration costs are real: a working Airflow estate with 400 DAGs is rarely worth rewriting just for a nicer model. Upgrade it to Airflow 3 first.

**Workflows in one example.** A Workflows definition is a list of steps in YAML. Connectors call Google APIs and wait for long-running jobs; `try` and `retry` handle failures. This one builds a BigQuery table and then runs a Cloud Run job:

```yaml
main:
  params: [input]
  steps:
    - init:
        assign:
          - project: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
          - day: ${input.day}
    - build_fact:
        try:
          call: googleapis.bigquery.v2.jobs.insert     # waits until the job finishes
          args:
            projectId: ${project}
            body:
              configuration:
                query:
                  query: ${"CALL sales.build_fct_revenue(DATE '" + day + "')"}
                  useLegacySql: false
        retry:
          predicate: ${http.default_retry_predicate}
          max_retries: 3
          backoff:
            initial_delay: 10
            max_delay: 300
            multiplier: 2
    - export:
        call: googleapis.run.v2.projects.locations.jobs.run
        args:
          name: ${"projects/" + project + "/locations/europe-west1/jobs/export-finance"}
    - done:
        return: ${day}
```

Each execution is independent: there is no shared view of "which days of which table are built", no catchup, and no backfill command. That is the line between a workflow engine and a data orchestrator. [Pipelines on Google Cloud](#gcppipes) covers Workflows, Cloud Scheduler, Eventarc and Managed Airflow environments in depth.

**Cost thinking.** A Managed Airflow environment runs all the time, so it has a baseline cost even on quiet days, plus worker capacity that scales with load; it pays off when many pipelines share it. Workflows and Cloud Scheduler charge per execution or step and cost almost nothing at low volume. Self-hosting Airflow, Dagster or Prefect on GKE trades licence or service fees for engineering time: upgrades, the database, security patches and on-call. Price the people, not only the infrastructure.

**Other tools you will meet.** **Kestra** (declarative YAML flows with a plugin ecosystem), **Flyte** (typed, containerised ML and data workflows on Kubernetes), **Argo Workflows** (Kubernetes-native DAGs of containers, common in ML platforms), **Temporal** (durable execution for long-running application workflows, more a service-orchestration engine than a data orchestrator) and **Mage** (notebook-style pipelines). Managed Airflow is also offered by Astronomer (Astro), which can run on Google Cloud.

> **Pitfall:** Building an orchestrator of orchestrators: Workflows starting Composer DAGs that trigger Dagster jobs. Every hop splits run history, retries and alerting across tools, and nobody can answer "did yesterday's revenue finish?". Pick one owner per pipeline and keep cross-tool calls at clear boundaries.

> **Tip:** Run a two-week spike before committing: implement one real pipeline (with a backfill, a failure and an alert) in the two leading candidates, deploy it through CI, and let the people who will be on call judge the result.

::: deeper
**Lock-in and portability.** Orchestration code is glue, and glue is rarely portable: Airflow operators, Dagster resources and Prefect blocks all differ. Keep business logic in plain, tested Python modules, SQL models (dbt or Dataform) and containers, and keep orchestrator code thin. Then a migration changes the wiring, not the logic. OpenLineage events (see [Pipeline Observability & Lineage](#pipelines)) also keep lineage independent of the tool.

**Latency floors.** Batch orchestrators are not stream processors. Airflow's scheduling loop and task start-up add seconds to minutes per task; Dagster and Prefect are similar per run. If you need results within seconds of an event, use a streaming job ([Streaming systems](#streaming)) and orchestrate only its deployment and its batch side jobs.

**Composer versions.** Managed Airflow Gen 3 is the current generation: Google runs the Airflow components in its own infrastructure, you choose an environment size and worker autoscaling limits, and images exist for both Airflow 2.11 and Airflow 3. Earlier generations (Composer 1 and 2) still exist, but new environments should use Composer 3; check the version support pages before planning an upgrade.

**Team scale.** One orchestrator installation for a whole company becomes a shared bottleneck (one upgrade schedule, one set of packages). Options: one environment per domain team with shared templates; Dagster code locations or Airflow 3.2's experimental multi-team mode for isolation inside one installation; or a platform team that offers orchestration as a self-service product (see [Data Team Topologies & Engineering Culture](#dataops)).

> **Interview:** "A 15-person data team on Google Cloud runs 60 dbt models, 25 ingestion jobs and a few ML batch jobs. Which orchestrator?" Weigh the evidence aloud: dependencies and backfills rule out cron alone; Workflows lacks data-orchestration features; Managed Airflow Gen 3 is a safe managed default, especially if anyone already knows Airflow; Dagster is a strong alternative if the team centres on dbt and wants asset lineage and partitions, run on GKE or Dagster+ Hybrid. State the deciding factors (managed service, skills, dbt-centric design, budget) and propose a spike.
:::

::: check
?? Why is Workflows usually not a replacement for Airflow or Dagster in a data platform?
It orchestrates steps and API calls well, but it has no notion of data intervals, catchup, backfills, partitions or a cross-pipeline run history, which data platforms need daily.

?? A team has three independent nightly jobs and no dependencies between them. What is the simplest sound setup on Google Cloud?
Cloud Scheduler triggering three Cloud Run jobs (or the jobs' own schedules), with alerting on failures. An orchestrator would add cost and operations without benefit.

?? Name two reasons a team might choose Managed Airflow Gen 3 over self-hosted Dagster even if they like Dagster's model.
It is a Google-managed service (support, procurement, no cluster or database to run), and Airflow skills and provider operators are widely available. Existing Airflow DAGs are another common reason.
:::

::: resources
- Docs: [Managed Airflow Gen 3 overview](https://docs.cloud.google.com/composer/docs/composer-3/composer-overview) · architecture and what Google manages in the current Composer generation
- Docs: [Workflows overview](https://docs.cloud.google.com/workflows/docs/overview) · steps, connectors, callbacks and when to use Workflows
- Docs: [Managed Airflow versions](https://docs.cloud.google.com/composer/docs/composer-versions) · supported Airflow versions and images
- Docs: [Dataform overview](https://docs.cloud.google.com/dataform/docs/overview) · BigQuery's built-in SQL workflow tool, for SQL-only dependency graphs
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley; orchestration as an undercurrent of the data engineering lifecycle
- Blog: [Dagster vs Airflow: feature comparison](https://dagster.io/blog/dagster-airflow) · the asset-based argument from Dagster's side (read with Airflow's docs for balance)
- Docs: [Temporal documentation](https://docs.temporal.io/) · durable execution, to understand where service orchestration differs from data orchestration
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, code or rule of thumb |
|---|---|
| Task size | The smallest unit you would retry on its own; usually one external job |
| Join after a branch | `trigger_rule="none_failed_min_one_success"` |
| Cleanup that always runs | A teardown task, or `trigger_rule="all_done"` |
| Limit load on a shared system | A pool with N slots; Dagster concurrency pool; Prefect `concurrency("name")` |
| Protect a critical DAG from backfills | Own pool or queue, higher `priority_weight`, cap backfills with `max_active_runs` |
| Cross-DAG dependency | Schedule on an asset (`schedule=[Asset("sales.fct_revenue")]`), not on another team's task ID |
| Interval-based runs in Airflow 3 | `CronDataIntervalTimetable`; cron strings default to `CronTriggerTimetable` |
| Waiting without a worker slot | `deferrable=True`, and run at least one triggerer |
| Run on data or at a time | `AssetOrTimeSchedule(timetable=..., assets=...)` |
| Fan out at run time | `task.expand(arg=upstream())`, constants with `.partial()` |
| Top-level DAG code | No API calls, database queries, `Variable.get()` or heavy imports |
| Task stuck in `scheduled` / `queued` | A concurrency limit or full pool / no executor capacity |
| Pass data between tasks | Write it to a table or Cloud Storage; pass the name or path |
| Airflow 3 upgrade check | `ruff check --select AIR3 dags/` |
| Clean the metadata database | `airflow db clean --clean-before-timestamp 2026-08-01` |
| Dagster rolling window input | `TimeWindowPartitionMapping(start_offset=-6)` |
| Dagster idempotent sensor | `RunRequest(run_key=...)` |
| Prefect constant in `.map()` | `unmapped(value)` |
| Prefect serverless runs on Google Cloud | `prefect work-pool create --type cloud-run:push --provision-infra NAME` |
| A few independent jobs | Cloud Scheduler plus Cloud Run jobs; no orchestrator needed |

### Practice

::: try
**1. The skipped join.** An Airflow DAG has `check_day >> [full_load, incremental_load] >> publish`, where `check_day` is a `@task.branch` that picks one load. Every run, `publish` is skipped. Explain why and fix it.
:::

?? Answer to 1
The load not chosen is skipped, and `publish` uses the default `all_success` rule, under which a skipped upstream is not a success, so `publish` is skipped too. Set `trigger_rule="none_failed_min_one_success"` on `publish`: it then runs when the chosen load succeeds and the other is skipped, and still does not run if the chosen load fails.

::: try
**2. Size the workers.** Each morning, 480 Airflow tasks must finish between 05:00 and 07:00. Each takes about 6 minutes, of which 5 are spent waiting for BigQuery. You use the CeleryExecutor with `worker_concurrency=16`. How many workers do you need with ordinary operators, and what changes with deferrable operators? Which settings must also allow it?
:::

?? Answer to 2
480 tasks × 6 minutes = 2,880 task-minutes in 120 minutes, so about 24 concurrent slots on average; with headroom for bursts, plan for about 32 slots, which is 2 workers of 16. With deferrable operators only about 1 minute per task uses a worker (480 task-minutes, about 4 slots on average), so one worker is plenty, plus a triggerer (two for availability). In both cases `parallelism`, the DAGs' `max_active_tasks` and any pools must allow the chosen concurrency, and BigQuery capacity (slots or reservations) must absorb the parallel queries.

::: try
**3. Plan a backfill.** Write a function `plan_backfill(start, end, existing, reprocess)` that returns the days to run, where `existing` maps a day to the state of its last run (`"success"` or `"failed"`) and `reprocess` follows Airflow 3's backfill options: `"none"` (only days without a run), `"failed"` (also rerun failed days) or `"completed"` (rerun every day). Use only the standard library.
:::

?? A solution to 3

```python
from datetime import date, timedelta

def plan_backfill(start, end, existing, reprocess="failed"):
    """Days in [start, end] to run. existing maps day -> state of the last run."""
    rerun = {"none": set(), "failed": {"failed"}, "completed": {"failed", "success"}}[reprocess]
    days, d = [], start
    while d <= end:
        state = existing.get(d.isoformat())
        if state is None or state in rerun:
            days.append(d.isoformat())
        d += timedelta(days=1)
    return days

existing = {"2026-11-01": "success", "2026-11-02": "failed", "2026-11-04": "success"}
for mode in ("none", "failed", "completed"):
    print(mode, plan_backfill(date(2026, 11, 1), date(2026, 11, 5), existing, mode))
# none ['2026-11-03', '2026-11-05']
# failed ['2026-11-02', '2026-11-03', '2026-11-05']
# completed ['2026-11-01', '2026-11-02', '2026-11-03', '2026-11-04', '2026-11-05']
```

::: try
**4. The unreliable partner file.** A partner uploads `orders_YYYY-MM-DD.csv` to a Cloud Storage bucket at an unpredictable time between 00:00 and 10:00, sometimes twice. Finance needs the loaded and modelled data by 11:00. Sketch the design in Airflow 3 and in Dagster.
:::

?? Answer to 4
Airflow 3: a DAG with a deferrable `GCSObjectExistenceSensor` (or an event from a Cloud Storage notification through Pub/Sub feeding an asset watcher) waiting for the file, with a timeout at 10:30; an idempotent load that overwrites that day's partition, so a second upload simply reloads it; the load task updates an asset, and the dbt or modelling DAG is scheduled on that asset; a Deadline Alert at 10:30 and a freshness check on the final table. Dagster: a sensor that lists new files using a cursor and yields `RunRequest(run_key=file_name_and_generation, partition_key=day)` for a daily-partitioned raw asset; downstream assets use `AutomationCondition.eager()`; an asset check validates row counts; a freshness check or alert covers the case where no file arrives. In both, keep the file name, upload generation and load time in metadata for audits.

::: try
**5. Pick the tool.** A 6-person team on Google Cloud runs 12 Python ingestion jobs whose number of sub-steps depends on how many customer accounts exist that day, followed by one dbt build. Nobody knows Airflow, the budget is tight and load is very spiky (nothing runs for 20 hours a day). Recommend an orchestrator and justify it in four sentences.
:::

?? A solution to 5
Prefect 3 with a Cloud Run push work pool fits well: flows are plain Python, so the per-account fan-out is just `.map()` over the day's accounts with a concurrency limit for the source API. Each flow run becomes a Cloud Run job execution, so nothing runs (or costs much) during the idle 20 hours, unlike an always-on Composer environment. The dbt build can be a final task, and automations alert on failures or late runs. Dagster+ Serverless or Hybrid would be the alternative if the team wants asset lineage and partitions; Managed Airflow becomes attractive later if the platform grows to many teams and pipelines.

### What comes next

You can now design DAGs that are safe to retry and easy to read, choose between clocks, sensors, deferral and data-aware triggers, explain and operate Airflow 3 from the DAG processor to the executor, build asset-based pipelines in Dagster and Python-native flows in Prefect, and choose among them and Google Cloud's own options. The pipelines now run on time and in order; the next question is what the people downstream do with the tables they produce. The next section, **Analytics engineering and BI**, covers that: analytics engineering practices and self-serve architecture, OLAP cubes and their modern alternatives, how BI tools work inside, funnel and cohort analysis, product analytics and A/B testing infrastructure, embedded analytics, and reverse ETL to push modelled data back into business tools.
