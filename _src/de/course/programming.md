This section is about the code a data engineer writes around the SQL: the Python that calls APIs, reads files, validates records and starts jobs, the DataFrame libraries that run transformations on one machine, and the other languages you will meet inside data platforms (Scala in Spark, Rust in the new query engines, Go in stream processors and tooling). It assumes you have read Data engineering foundations and SQL for data engineering, and that you can read basic Python: variables, functions, loops, lists and dictionaries. If you cannot yet, the companion [Python course](python-end-to-end-topic-map.html) teaches the language from zero, and the first three topics here link to its deeper chapters instead of repeating them.

**What you will be able to do afterwards**

- Write Python pipeline code that streams data in constant memory, validates records at the boundary, is typed and tested, and installs reproducibly.
- Choose between threads, processes and `asyncio` for a given job, and size connection pools and worker counts sensibly.
- Structure pipeline code with a few well-chosen design patterns (registry, strategy, decorator, observer, builder) without over-engineering it.
- Use Polars for fast single-machine transformations, and Ibis to write one DataFrame program that runs on DuckDB locally and BigQuery in production.
- Explain where Scala, Rust and Go fit in data engineering, read code in each, and decide when (rarely) to write one of them yourself.

**The big picture.** A data platform has two kinds of code. **Declarative code** (SQL, YAML configuration, Terraform) says *what* result you want and lets an engine work out how. **Imperative code** (Python, Scala, Go, Rust) says *how*, step by step. Good data engineers push as much work as possible into declarative engines, because BigQuery or Spark will run a join far better than a hand-written loop. The imperative code is the glue: it talks to systems that do not speak SQL, enforces contracts, controls retries, and occasionally implements an engine itself. This section is about writing that glue well, and about knowing which language each piece of the platform is written in and why.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Python

Python is the default language of data engineering. Airflow DAGs, Dataflow (Apache Beam) jobs, PySpark applications, dbt's Python models, ingestion scripts, API clients and test suites are all Python. That does not mean data engineers write Python the way web developers do. Pipeline code has its own concerns: data that does not fit in memory, slow networks, flaky sources, schemas that drift, and jobs that must be safe to run twice.

The first three topics are short, because the companion Python course covers the language features in depth; here you see how each feature is used in pipelines, and a link takes you to the full chapter. The fourth topic, on design patterns, is the bridge from "scripts that work" to "a codebase a team can maintain".

### Python for Data Engineering

::: plain
Python is the data engineer's pocket knife: not the fastest tool for any one job, but one that can do almost every job well enough, with a library for every database, file format and cloud service. The trick is to use Python to *coordinate* heavy work (tell BigQuery or Spark what to do) rather than to *do* the heavy work in slow Python loops, and to write it so it streams data instead of loading everything into memory.
:::

A typical Python pipeline step reads from a source, reshapes records and writes them somewhere. The difference between a script that works on a laptop and one that survives production is mostly four habits.

**1. Stream, don't load.** A **generator** is a function that uses `yield` to hand back one item at a time, pausing between items. Chaining generators gives you a pipeline that holds only one record (or one batch) in memory, however big the input:

```python
import csv, io
from dataclasses import dataclass
from itertools import batched          # Python 3.12+

@dataclass(frozen=True, slots=True)
class Order:
    order_id: int
    customer: str
    amount: float

def read_rows(f):
    yield from csv.DictReader(f)                       # one dict per line, lazily

def parse(rows):
    for r in rows:
        if r["status"] == "paid":
            yield Order(int(r["order_id"]), r["customer"], float(r["amount"]))

raw = io.StringIO("order_id,customer,amount,status\n1,asha,40.0,paid\n"
                  "2,bo,15.5,cancelled\n3,asha,22.0,paid\n4,cy,9.5,paid\n")
for batch in batched(parse(read_rows(raw)), 2):        # write in batches of 2
    print([o.order_id for o in batch])                 # [1, 3] then [4]
```

Nothing runs until the `for` loop pulls items, and memory use stays flat whether the file has four lines or forty million. Batching matters because most sinks (BigQuery's Storage Write API, Pub/Sub, a database) are far more efficient with hundreds of rows per call than with one.

**2. Give records a shape.** `dataclasses` (standard library) turn loose dictionaries into named, typed records; `frozen=True` makes them immutable and `slots=True` makes them smaller. For untrusted input, Pydantic adds validation (see the typing topic below).

**3. Pick the right engine for the data size.**

| Data size | Good tool | Why |
|---|---|---|
| Up to a few GB on one machine | Polars, DuckDB, pandas | Fast, simple, no cluster |
| Already in the warehouse | SQL in BigQuery, driven from Python | The data never leaves the engine |
| Tens of GB to many TB, files in Cloud Storage | PySpark on Managed Service for Apache Spark (formerly Dataproc) | Distributed, but adds cluster overhead |
| Streaming, or unified batch and streaming | Apache Beam on Dataflow | Autoscaling workers, exactly-once sinks |

**4. Make it reproducible.** One virtual environment per project, dependencies declared in `pyproject.toml` and pinned in a lock file, tests run with `pytest`, and the whole thing packaged into a container image for production:

```bash
uv init orders-pipeline && cd orders-pipeline
uv add polars google-cloud-bigquery pydantic     # writes pyproject.toml and uv.lock
uv add --dev pytest ruff mypy
uv run pytest -q
```

> **Pitfall:** Pulling a whole table into pandas with `SELECT *` to filter it in Python. Push the filter and the aggregation into the SQL and bring back only the result. Moving data out of the warehouse is slow and, across regions, can cost money.

> **Tip:** Keep business logic in pure functions that take and return plain data (records, DataFrames). They are trivial to unit test, and the I/O around them (reading Cloud Storage, writing BigQuery) can be swapped for fakes.

::: guide
For the language itself, read [Iteration and generators](python-end-to-end-topic-map.html#iteration), [Python for data](python-end-to-end-topic-map.html#data), [Testing](python-end-to-end-topic-map.html#testing) and [Packaging](python-end-to-end-topic-map.html#packaging) in the Python course, and [PySpark runtime and environment](pyspark-streaming-kafka-topic-map.html#pyruntime) in the PySpark + Kafka guide for how Python code runs on Spark executors.
:::

::: check
?? Why does chaining generators keep memory flat?
Each generator produces one item only when the next stage asks for it, so at any moment only the item (or batch) being processed is in memory, not the whole input.

?? A script loads a 30 GB BigQuery table into pandas to compute daily totals. What should it do instead?
Run the aggregation in BigQuery (`SELECT date, SUM(amount) ... GROUP BY date`) and fetch only the small result. Python should drive the engine, not replace it.

?? Why batch records before writing them to a sink?
Every call has fixed overhead (network round trip, request parsing, commit). Sending hundreds of rows per call spreads that cost and raises throughput enormously.
:::

::: resources
- Docs: [Python documentation](https://docs.python.org/3/) · the language reference, tutorial and standard library, including `itertools`, `csv` and `dataclasses`
- Docs: [BigQuery client libraries](https://cloud.google.com/bigquery/docs/reference/libraries) · the official Python client: queries, load jobs and DataFrame integration
- Book: [Fluent Python, 2nd Edition](https://www.oreilly.com/library/view/fluent-python-2nd/9781492056348/) · Luciano Ramalho; the best book on idiomatic Python, with deep chapters on iterators, generators and data classes
- Book: [Python for Data Analysis, 3rd Edition](https://wesmckinney.com/book/) · Wes McKinney's free online book on pandas and NumPy, from the creator of pandas
- Docs: [uv documentation](https://docs.astral.sh/uv/) · projects, lock files and fast installs for reproducible pipeline environments
- Course: [Real Python](https://realpython.com/) · in-depth tutorials on generators, packaging, testing and much more
:::

### Concurrency & Parallelism in Python for DE

::: plain
Most pipeline code spends its time **waiting**: for an API to answer, for a file to upload, for a database to return rows. While one request waits, the program could start others, the way a waiter takes several tables' orders instead of standing by the kitchen. That is **concurrency**. **Parallelism** is doing several pieces of real computation at the same instant, which needs several CPU cores working at once, like several cooks. Python has a different tool for each.
:::

Python offers three models:

| Model | Module | Best for | How it works |
|---|---|---|---|
| Threads | `threading`, `concurrent.futures.ThreadPoolExecutor` | I/O-bound work with blocking libraries | OS threads; only one runs Python code at a time (the GIL), but waiting threads release it |
| Processes | `multiprocessing`, `ProcessPoolExecutor` | CPU-bound pure-Python work | Separate interpreters on separate cores; data is copied between them |
| Async | `asyncio` | Very many concurrent I/O operations (hundreds to thousands) | One thread, an event loop switching between tasks at each `await` |

The **GIL** (global interpreter lock) lets only one thread execute Python bytecode at a time in standard CPython. Threads still help for I/O, because a thread waiting on the network or disk releases the GIL. For CPU work, threads give no speed-up; use processes, or better, a library whose heavy loops run in C or Rust and release the GIL (NumPy, Polars, DuckDB, PyArrow).

[[fig:programming_concurrency]]

Downloading ten files with a thread pool, simulated with `time.sleep`:

```python
import time
from concurrent.futures import ThreadPoolExecutor

def fetch(name):
    time.sleep(0.2)                   # stands in for a 200 ms network call
    return f"{name}: ok"

names = [f"part-{i}.json" for i in range(10)]
start = time.perf_counter()
with ThreadPoolExecutor(max_workers=10) as pool:
    results = list(pool.map(fetch, names))         # keeps input order
print(results[0], len(results))                    # part-0.json: ok 10
print(f"{time.perf_counter() - start:.1f} s")      # about 0.2 s, not 2.0 s
```

The same idea with `asyncio`, using a **semaphore** to cap concurrency so you do not overwhelm the source:

```python
import asyncio

async def fetch(name, limit):
    async with limit:                      # at most 3 requests in flight
        await asyncio.sleep(0.1)           # stands in for an async HTTP call
        return name.upper()

async def main():
    limit = asyncio.Semaphore(3)
    return await asyncio.gather(*(fetch(n, limit) for n in ["a", "b", "c", "d"]))

print(asyncio.run(main()))                 # ['A', 'B', 'C', 'D']
```

In real code the `sleep` is an async client: `httpx.AsyncClient` for HTTP, `aiofiles` for files, or `asyncpg` for PostgreSQL. A **connection pool** keeps a fixed set of open database connections that tasks borrow and return, so you do not pay the connection handshake on every query and do not exceed the database's connection limit:

```python
import asyncpg

async def load(dsn, rows):
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.copy_records_to_table("orders", records=rows)   # fast bulk COPY
    await pool.close()
```

For many uploads to Cloud Storage, the client library has a transfer manager that parallelises for you:

```python
from google.cloud.storage import Client, transfer_manager

bucket = Client().bucket("my-raw-bucket")
results = transfer_manager.upload_many_from_filenames(
    bucket, ["a.parquet", "b.parquet"], source_directory="out/", max_workers=8)
```

> **Pitfall:** Unbounded concurrency. `gather` over 50,000 requests opens 50,000 connections at once and gets you rate-limited or blocked. Always bound it: a semaphore, a pool size, or `max_workers`.

> **Pitfall:** Calling a blocking library (such as `requests` or a synchronous database driver) inside `async def` freezes the whole event loop. Use an async library, or push the call to a thread with `await asyncio.to_thread(func, ...)`.

::: guide
For how the GIL works, the free-threaded build (officially supported since Python 3.14), executors, event loops and cancellation, read [The GIL](python-end-to-end-topic-map.html#gil), [Concurrency](python-end-to-end-topic-map.html#concurrency) and [asyncio](python-end-to-end-topic-map.html#asyncio) in the Python course.
:::

::: check
?? A job calls a slow REST API 2,000 times. Threads, processes or asyncio?
Threads or asyncio, because the work is I/O-bound: the program mostly waits. Asyncio scales to more concurrent requests cheaply; a thread pool is simpler with a blocking client. Either way, bound the concurrency.

?? Why does a thread pool not speed up a pure-Python loop that parses JSON and sums numbers?
The GIL lets only one thread run Python bytecode at a time, so CPU-bound Python code runs one thread after another. Use processes, or a library whose work runs outside the GIL.

?? What does a connection pool protect you from?
The cost of opening a new connection per query, and exceeding the database's maximum number of connections when many tasks run at once.
:::

::: resources
- Docs: [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) · thread and process pools behind one simple interface
- Docs: [asyncio](https://docs.python.org/3/library/asyncio.html) · the event loop, tasks, semaphores and `to_thread`
- Docs: [Python support for free threading](https://docs.python.org/3/howto/free-threading-python.html) · how the build without the GIL works and what it means for libraries
- Book: [Python Concurrency with asyncio](https://www.manning.com/books/python-concurrency-with-asyncio) · Matthew Fowler; asyncio, pools and concurrency patterns with realistic I/O examples
- Docs: [asyncpg](https://magicstack.github.io/asyncpg/current/) · a fast async PostgreSQL driver with connection pools and bulk COPY
- Article: [Speed up your Python program with concurrency](https://realpython.com/python-concurrency/) · Real Python's side-by-side comparison of threads, processes and asyncio
:::

### Python Type System & Protocols for DE

::: plain
Type hints are labels on your code that say what kind of value each function expects and returns, like the labels on a lab's sample jars. Python ignores them when it runs, but a checker such as mypy reads them and warns you before you run anything, for example when you pass text where a number belongs. **Validation** is the other half: checking at run time that data arriving from outside (a file, an API, a message) really has the shape the labels promise.
:::

Pipelines have two very different type problems:

1. **Inside your code**, you control the values. Static type hints plus a checker catch mistakes such as passing a `str` date to a function that does date arithmetic. They cost nothing at run time.
2. **At the boundary**, data comes from systems you do not control. No static checker can know that tomorrow's API response will have `amount` as a string. You need **run-time validation**.

**Static hints and Protocols.** A `Protocol` describes the methods an object must have, without requiring it to inherit from anything. This is **structural typing**: any object with the right shape qualifies. It suits pipeline plug-ins such as sinks:

```python
from typing import Protocol, runtime_checkable
from collections.abc import Iterable

@runtime_checkable
class Sink(Protocol):
    def write(self, rows: Iterable[dict]) -> int: ...

class ListSink:                           # does not inherit from Sink
    def __init__(self) -> None:
        self.rows: list[dict] = []
    def write(self, rows: Iterable[dict]) -> int:
        self.rows.extend(rows)
        return len(self.rows)

def run(sink: Sink) -> None:
    print(sink.write([{"id": 1}, {"id": 2}]))

s = ListSink()
run(s)                                    # 2
print(isinstance(s, Sink))                # True: it has a write method
```

A BigQuery sink, a Cloud Storage sink and an in-memory test sink can all satisfy `Sink` without sharing a base class, and mypy checks that each one has a matching `write`.

**Run-time validation with Pydantic.** Pydantic (version 2, whose validation core is written in Rust) builds validators from type hints:

```python
from datetime import datetime
from pydantic import BaseModel, Field, ValidationError, field_validator

class OrderEvent(BaseModel):
    order_id: int
    amount: float = Field(ge=0)
    currency: str
    created_at: datetime

    @field_validator("currency")
    @classmethod
    def upper_iso(cls, v: str) -> str:
        if len(v) != 3:
            raise ValueError("currency must be a 3-letter ISO code")
        return v.upper()

ok = OrderEvent.model_validate(
    {"order_id": "17", "amount": "40.5", "currency": "eur", "created_at": "2026-11-02T10:00:00Z"})
print(ok.order_id, ok.currency)          # 17 EUR   ("17" was coerced to int)

try:
    OrderEvent.model_validate({"order_id": 1, "amount": -3, "currency": "EUR", "created_at": "x"})
except ValidationError as e:
    print(e.error_count())               # 2   (amount below 0, bad datetime)
```

In a streaming pipeline, records that fail validation go to a **dead-letter** destination (a Pub/Sub topic or a BigQuery error table) together with the error text, rather than crashing the job or silently entering the warehouse.

**Running mypy.** Add it to CI so a type error fails the pull request:

```bash
uv run mypy --strict src/
```

Use `--strict` on new code; for an old codebase, enable checks module by module.

> **Pitfall:** Treating hints as validation. `def load(amount: float)` happily receives `"40.5"` from a JSON file at run time; nothing checks it. Hints help your editor and mypy; validation at the boundary protects the data.

> **Tip:** Use Pydantic (or `TypedDict` for plain dictionaries) at the edges, and lighter `dataclasses` inside the pipeline where data is already trusted. Validating the same record at every step wastes CPU.

::: guide
For generics, `TypedDict`, `Literal`, overloads, variance and how type checkers work, read [Typing](python-end-to-end-topic-map.html#typing) in the Python course, and [Testing](python-end-to-end-topic-map.html#testing) for wiring mypy into CI alongside pytest.
:::

::: check
?? Why can't type hints alone protect a pipeline from a bad API response?
Hints are not checked at run time; the interpreter ignores them. Data from outside must be validated at run time, for example with Pydantic.

?? What makes `ListSink` count as a `Sink` without inheriting from it?
`Sink` is a `Protocol`, which uses structural typing: any class with a compatible `write` method satisfies it.

?? What should a streaming job do with a record that fails validation?
Send it, with the error, to a dead-letter destination for inspection and replay, and keep processing the rest.
:::

::: resources
- Docs: [typing: support for type hints](https://docs.python.org/3/library/typing.html) · the standard library reference, including `Protocol` and `TypedDict`
- Docs: [Pydantic](https://docs.pydantic.dev/latest/) · models, validators, strict mode and JSON Schema generation
- Docs: [mypy documentation](https://mypy.readthedocs.io/en/stable/) · configuring strict checking and adopting types gradually
- Spec: [PEP 544: Protocols, structural subtyping](https://peps.python.org/pep-0544/) · the design and rationale of Protocol classes
- Book: [Robust Python](https://www.oreilly.com/library/view/robust-python/9781098100650/) · Patrick Viafore; types, Protocols and validation for maintainable codebases
- Article: [Python type checking guide](https://realpython.com/python-type-checking/) · Real Python's practical introduction to hints and mypy
:::

### Data Engineering Design Patterns in Code

::: plain
A design pattern is a well-known shape for code that solves a problem that keeps coming back, the way carpenters reuse the same proven ways to frame a door. In pipelines the recurring problems are: "create the right connector from a config file", "retry flaky calls", "pick the output format at run time" and "tell several systems when a run finishes". Knowing a handful of patterns lets you solve these in a standard way that teammates recognise at once.
:::

Patterns come from object-oriented design, but in Python many of them shrink to a dictionary, a function or a decorator. Five earn their place in data code.

**1. Factory with a registry: create connectors from configuration.** Pipelines are often driven by YAML that says `type: bigquery` or `type: csv`. A **factory** turns that string into an object. A **registry** (a dictionary filled by a decorator) lets new connectors add themselves without anyone editing the factory:

```python
SOURCES = {}

def register(kind):
    def wrap(cls):
        SOURCES[kind] = cls
        return cls
    return wrap

@register("csv")
class CsvSource:
    def __init__(self, path): self.path = path
    def read(self): return f"reading {self.path} as CSV"

@register("bigquery")
class BigQuerySource:
    def __init__(self, table): self.table = table
    def read(self): return f"SELECT * FROM `{self.table}`"

def make_source(config):
    kind = config.pop("type")
    return SOURCES[kind](**config)        # KeyError for unknown types: fail fast

print(make_source({"type": "csv", "path": "gs://raw/orders.csv"}).read())
# reading gs://raw/orders.csv as CSV
print(sorted(SOURCES))                    # ['bigquery', 'csv']
```

**2. Decorator: retries and logging around any call.** A **decorator** wraps a function with extra behaviour. Retrying with **exponential backoff** (wait 1 s, then 2 s, then 4 s...) plus **jitter** (a random part, so many clients do not retry in lockstep) is the classic one:

```python
import functools, random, time

def retry(times=4, base=0.01, on=(ConnectionError,)):
    def deco(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except on:
                    if attempt == times:
                        raise
                    time.sleep(base * 2 ** (attempt - 1) * random.uniform(0.5, 1.5))
        return inner
    return deco

calls = {"n": 0}

@retry()
def flaky_fetch():
    calls["n"] += 1
    if calls["n"] < 3:
        raise ConnectionError("timeout")
    return "payload"

print(flaky_fetch(), calls["n"])          # payload 3
```

Retry only errors that are **transient** (timeouts, HTTP 429 and 503), never validation errors, and only around operations that are **idempotent** (safe to repeat). In production, the `tenacity` library or the Google client libraries' built-in retry settings do this for you.

**3. Strategy: choose an algorithm at run time.** A **strategy** is an interchangeable piece of behaviour. In Python it is usually just a function in a dictionary:

```python
import json, csv, io

def to_jsonl(rows):
    return "\n".join(json.dumps(r) for r in rows)

def to_csv(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0]), lineterminator="\n")
    w.writeheader(); w.writerows(rows)
    return buf.getvalue().rstrip("\n")

WRITERS = {"jsonl": to_jsonl, "csv": to_csv}
rows = [{"id": 1, "city": "Pune"}, {"id": 2, "city": "Oslo"}]
print(WRITERS["csv"](rows))
# id,city
# 1,Pune
# 2,Oslo
```

**4. Observer: tell interested parties about pipeline events.** The pipeline publishes events ("run started", "rows written", "run failed") to a list of **subscribers** without knowing what each does. One sends a chat message, one records lineage, one emits a metric:

```python
class Events:
    def __init__(self): self.subs = []
    def subscribe(self, fn): self.subs.append(fn); return fn
    def emit(self, name, **data):
        for fn in self.subs:
            fn(name, data)

events = Events()
log = []
events.subscribe(lambda n, d: log.append(f"metric {n} {d.get('rows', 0)}"))
events.subscribe(lambda n, d: log.append(f"lineage {n}"))
events.emit("load_succeeded", rows=1200)
print(log)        # ['metric load_succeeded 1200', 'lineage load_succeeded']
```

Airflow callbacks (`on_failure_callback`), OpenLineage listeners and Spark's `SparkListener` are all this pattern.

**5. Builder: assemble a complex configuration step by step.** When a job has many optional settings, a **builder** collects them with readable chained calls and validates once at the end. PySpark's `SparkSession.builder.appName(...).config(...).getOrCreate()` is the familiar example:

```python
from dataclasses import dataclass, field

@dataclass(frozen=True)
class JobConfig:
    source: str
    target: str
    partitions: list = field(default_factory=list)
    dry_run: bool = False

class JobBuilder:
    def __init__(self): self._kw = {}
    def source(self, s): self._kw["source"] = s; return self
    def target(self, t): self._kw["target"] = t; return self
    def partition(self, p): self._kw.setdefault("partitions", []).append(p); return self
    def dry_run(self): self._kw["dry_run"] = True; return self
    def build(self):
        if "source" not in self._kw or "target" not in self._kw:
            raise ValueError("source and target are required")
        return JobConfig(**self._kw)

cfg = JobBuilder().source("gs://raw/orders").target("shop.orders").partition("2026-11-01").build()
print(cfg.target, cfg.partitions, cfg.dry_run)    # shop.orders ['2026-11-01'] False
```

[[fig:programming_patterns]]

> **Pitfall:** Pattern-first design. A 40-line script wrapped in an abstract base class, a factory and a builder is harder to read than the 40 lines. Introduce a pattern when the second or third variant appears, not before.

> **Tip:** Prefer plain functions and dictionaries to class hierarchies. In Python a strategy is a function, a registry is a `dict`, and a decorator is a function that returns a function.

::: deeper
**Plug-in discovery with entry points.** A registry inside one package works until other teams want to add connectors without changing your code. Python packages can advertise plug-ins through **entry points** in `pyproject.toml` (`[project.entry-points."mypipe.sources"]`), and your factory finds them with `importlib.metadata.entry_points(group="mypipe.sources")`. pytest plug-ins and Airflow provider packages are discovered this way.

**Patterns you already use in data frameworks.**

| Pattern | Where it appears |
|---|---|
| Factory / registry | Airflow hooks and connections, Beam I/O connectors, fsspec protocols (`gs://`, `file://`) |
| Template method | Airflow `BaseOperator.execute`, Beam `DoFn.process`: the framework calls your method at the right moment |
| Observer | Airflow callbacks, `SparkListener`, OpenLineage integrations |
| Builder | `SparkSession.builder`, BigQuery's `QueryJobConfig`, Beam pipeline construction |
| Strategy | Pluggable serialisers, partitioners, compression codecs |
| Adapter | Wrapping an awkward vendor SDK behind your own small interface |

**Dependency injection without a framework.** Pass collaborators (the sink, the clock, the HTTP client) into functions as parameters instead of creating them inside. Tests then inject fakes, and production injects real clients. This one habit does more for testability than all the other patterns together.

**Functional core, imperative shell.** Keep transformations pure (data in, data out, no I/O) and push all side effects to a thin outer layer. The book *Architecture Patterns with Python* calls the outer layer adapters and the inner layer the domain model.

> **Interview:** "How would you design a pipeline framework that supports many sources and sinks?" Define small interfaces (a `Source` with `read`, a `Sink` with `write`) as Protocols, build objects from configuration through a registry-based factory, wrap I/O in retry decorators that only retry transient errors, publish lifecycle events to observers for alerting and lineage, and keep transformation logic in pure functions tested without any I/O.
:::

::: check
?? Which pattern lets a pipeline create a `BigQuerySource` or `CsvSource` from `type: bigquery` in YAML, and what makes it easy to extend?
A factory, backed by a registry dictionary that connector classes add themselves to with a decorator. New connectors register themselves without the factory being edited.

?? Why must retried operations be idempotent?
A retry may repeat an operation that actually succeeded before the error was reported (the write landed but the response timed out). If repeating it changes the result, you get duplicates.

?? Name two places in common data frameworks where the observer pattern appears.
Airflow task callbacks such as `on_failure_callback`, Spark's `SparkListener`, and OpenLineage listeners.
:::

::: resources
- Book: [Head First Design Patterns, 2nd Edition](https://www.oreilly.com/library/view/head-first-design/9781492077992/) · a gentle, visual introduction to the classic patterns
- Article: [Refactoring.Guru: design patterns](https://refactoring.guru/design-patterns) · every classic pattern explained with diagrams and Python examples
- Book: [Architecture Patterns with Python](https://www.cosmicpython.com/) · Percival and Gregory, free online; dependency injection, adapters and a functional core in real Python
- Article: [Python Design Patterns](https://python-patterns.guide/) · Brandon Rhodes on which classic patterns matter in Python and which the language makes unnecessary
- Docs: [Entry points specification](https://packaging.python.org/en/latest/specifications/entry-points/) · how packages advertise plug-ins that factories can discover
- Docs: [Tenacity](https://tenacity.readthedocs.io/en/latest/) · a production retry library with backoff, jitter and stop conditions
:::

## DataFrames beyond pandas

pandas made Python the language of data analysis, but it was designed in 2008 for data that fits comfortably in one machine's memory, it runs on one CPU core, and it executes every line immediately, so it cannot optimise a whole query. Two newer libraries attack those limits from different sides. **Polars** is a faster, multithreaded, lazily optimised DataFrame engine that runs on your machine. **Ibis** is not an engine at all: it is a DataFrame *language* that compiles your code to SQL and runs it wherever the data already lives, BigQuery included.

Both follow the lesson of the previous group: describe the result, let an engine plan the work. Learn Polars for fast local and single-node processing, and Ibis when you want Python code that is portable across engines.

### Polars — High-Performance DataFrames

::: plain
Polars is a DataFrame library (tables you manipulate in code) written in Rust, with a Python interface. Where pandas does each step the moment you write it, Polars can wait until you have described the whole job, then plan it the way a database plans a query: skip the columns you never use, filter before reading the rest, and spread the work over every CPU core. It is like giving a courier the whole delivery list at once instead of one address at a time, so they can plan the best route.
:::

Polars works on **columns** stored in the Apache Arrow memory format (a standard columnar layout for in-memory data, the same idea as columnar files from the foundations section). You write **expressions**, small descriptions of computations on columns, and Polars runs them in parallel.

```python
import polars as pl

orders = pl.DataFrame({
    "order_id": [1, 2, 3, 4],
    "customer": ["asha", "bo", "asha", "cy"],
    "amount":   [40.0, 15.5, 22.0, 9.5],
    "status":   ["paid", "cancelled", "paid", "paid"],
})

result = (
    orders
    .filter(pl.col("status") == "paid")
    .group_by("customer")
    .agg(pl.col("amount").sum().alias("total"), pl.len().alias("orders"))
    .sort("total", descending=True)
)
print(result.rows())        # [('asha', 62.0, 2), ('cy', 9.5, 1)]
```

The core vocabulary is small:

| Method | What it does | SQL equivalent |
|---|---|---|
| `select(...)` | Choose or compute columns | `SELECT` |
| `with_columns(...)` | Add or replace columns, keep the rest | `SELECT *, expr AS x` |
| `filter(...)` | Keep rows matching a condition | `WHERE` |
| `group_by(...).agg(...)` | Aggregate per group | `GROUP BY` |
| `join(other, on=..., how=...)` | Join two frames | `JOIN` |
| `expr.over("col")` | Window function per group | `OVER (PARTITION BY col)` |
| `pl.when(...).then(...).otherwise(...)` | Conditional value | `CASE WHEN` |

Expressions compose. Each customer's share of their own spending, with a size label:

```python
shares = orders.with_columns(
    share=pl.col("amount") / pl.col("amount").sum().over("customer"),
    size=pl.when(pl.col("amount") >= 20).then(pl.lit("big")).otherwise(pl.lit("small")),
)
```

Every expression inside one `with_columns` or `select` runs in parallel on different cores.

**Lazy mode and the query optimiser.** Replace `read_parquet` with `scan_parquet` (or call `.lazy()` on a DataFrame) and nothing runs until `collect()`. Polars first builds a **logical plan**, a tree describing the query, then rewrites it:

- **Predicate pushdown**: filters move as close to the scan as possible, so Parquet row groups whose statistics rule them out are never read.
- **Projection pushdown**: only the columns the query uses are read from the file.
- **Slice pushdown**: a `head(10)` stops the scan early.
- **Common subplan elimination**: a sub-query used twice is computed once.

```python
lf = (
    pl.scan_parquet("gs://my-raw-bucket/orders/*.parquet")   # cloud paths work directly
    .filter(pl.col("order_date") >= pl.date(2026, 11, 1))
    .group_by("customer")
    .agg(pl.col("amount").sum())
)
print(lf.explain())                        # the optimised plan, filter and columns pushed into the scan
df = lf.collect()                          # run it in memory
df = lf.collect(engine="streaming")        # run in batches, for data larger than memory
lf.sink_parquet("out/totals.parquet")      # stream the result to a file without collecting it
```

[[fig:programming_polars]]

**Polars vs pandas.**

| | pandas | Polars |
|---|---|---|
| Execution | Eager, one step at a time | Eager or lazy with a query optimiser |
| Cores | Mostly one | All, by default |
| Memory model | NumPy arrays (Arrow-backed types optional) | Arrow columns |
| Row index | Yes, central to the API | No index; rows are positional |
| Missing values | `NaN` and `None` mixed, depending on type | One consistent `null` for every type; `NaN` is a float value |
| Larger than memory | No | Streaming engine and `sink_*` methods |

Typical speed-ups on group-bys and joins are large (often 5 to 20 times on a laptop), mostly from multithreading, lazy optimisation and avoiding Python objects. The pandas ecosystem (plotting, scikit-learn, many connectors) is still bigger, and conversion is cheap: `df.to_pandas()`, `pl.from_pandas(pdf)`, `df.to_arrow()` and `pl.from_arrow(table)`.

**With BigQuery.** Let BigQuery do the heavy filtering, fetch the result as Arrow and continue in Polars:

```python
from google.cloud import bigquery

table = bigquery.Client().query(
    "SELECT customer, amount FROM `shop.orders` WHERE order_date = CURRENT_DATE()"
).to_arrow()                              # uses the fast BigQuery Storage Read API when installed
df = pl.from_arrow(table)
```

To write back, save Parquet to Cloud Storage and run a BigQuery load job; it is much faster than inserting rows.

> **Pitfall:** Using `map_elements` (a Python function applied per row) for logic that expressions can express. It drops back to slow Python, single-threaded, and hides the work from the optimiser. Look for a built-in expression first: string, date and list namespaces (`pl.col("s").str...`, `.dt...`, `.list...`) cover most needs.

> **Tip:** Start every new job with `scan_*` and lazy mode, even on small data. You get the optimiser for free, and the same code scales to bigger files with `engine="streaming"`.

::: deeper
**Inside Polars.** The core is a set of Rust crates. A column is a `Series` wrapping a `ChunkedArray`, a list of Arrow arrays (chunks), so appending data can add a chunk instead of copying. Polars uses its own Arrow implementation, and stores strings in Arrow's **string view** layout, where short strings sit inline and long ones point into shared buffers, which makes comparisons and filters faster. Work is spread over a thread pool (Rayon); `POLARS_MAX_THREADS` caps it, which matters in containers where Polars sees the host's cores but is allowed only a few.

**The streaming engine** processes data in **morsels** (small batches) that flow through pipelined operators on all cores, an approach from the database research on morsel-driven parallelism. Operators that need all their input (a sort, a full group-by) keep state, so not every query can stream in constant memory, but scans, filters, projections and most aggregations do.

**Polars is not distributed.** It scales up (more cores, more memory on one machine) rather than out. A large Compute Engine machine with dozens of cores and hundreds of GB of memory handles a surprising amount of work, often cheaper and simpler than a Spark cluster for jobs up to a few hundred GB. Beyond one machine, use BigQuery or Spark.

**Strict types.** Polars refuses to silently change a column's type. An integer column with a null stays an integer column with a null (pandas historically turned it into float). Casting is explicit with `cast(pl.Int64)`, and `strict=True` casts fail on bad values instead of producing nulls.

> **Interview:** "Why is Polars faster than pandas?" Columnar Arrow memory with no Python objects in the hot path; vectorised kernels in Rust; parallel execution across all cores by default; and lazy evaluation with a query optimiser that pushes filters and column selection down into the scan, so less data is read at all.
:::

::: check
?? What does projection pushdown do when a lazy query reads a 50-column Parquet file but uses three columns?
It changes the plan so the scan reads only those three columns from the file, skipping the other 47 entirely.

?? Why is `map_elements` with a Python lambda usually a bad idea?
It runs Python code row by row on one thread, outside the optimiser, losing most of Polars' speed. A built-in expression does the same work in parallel Rust code.

?? Your data is 2 TB of Parquet in Cloud Storage. Is Polars the right tool?
Probably not on its own. Polars runs on one machine; for 2 TB, query it with BigQuery (external or Iceberg tables) or Spark, and use Polars on the smaller results.
:::

::: resources
- Docs: [Polars user guide](https://docs.pola.rs/) · expressions, lazy mode, the streaming engine and the full API reference
- Docs: [Polars lazy optimisations](https://docs.pola.rs/user-guide/lazy/optimizations/) · the list of rewrites the optimiser applies, with examples
- Book: *Python Polars: The Definitive Guide* by Jeroen Janssens and Thijs Nieuwdorp · O'Reilly's thorough book on the expression API and lazy execution
- Book: [Polars Cookbook](https://www.oreilly.com/library/view/polars-cookbook/9781805121152/) · Yuki Kakegawa; short recipes for common transformation tasks
- Spec: [Apache Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html) · the in-memory layout Polars, DuckDB and BigQuery's Arrow APIs share
- Paper: [Morsel-Driven Parallelism](https://db.in.tum.de/~leis/papers/morsels.pdf) · Leis et al.; the multi-core execution model behind modern streaming engines
:::

### Ibis — Portable DataFrame API

::: plain
Ibis lets you write a data transformation once, in Python, and run it on many different engines: DuckDB on your laptop, BigQuery in production, or Snowflake, Spark and Trino elsewhere. It works like a translator: your Python code is turned into the SQL dialect of whichever engine you connect to, and the engine does the work where the data lives. Your laptop never has to download the table.
:::

An Ibis program has three parts: a **connection** to a backend, **table expressions** that describe what you want, and an **execution** call that compiles the expression and runs it on the backend.

```python
import ibis
from ibis import _

def paid_totals(orders):
    """Business logic, written once, independent of the engine."""
    return (
        orders.filter(_.status == "paid")
        .group_by("customer")
        .aggregate(total=_.amount.sum(), n=_.count())
        .order_by(ibis.desc("total"))
    )

# Local development: DuckDB in-process, on sample Parquet files
dev = ibis.duckdb.connect()
sample = dev.read_parquet("sample/orders/*.parquet", table_name="orders")
print(paid_totals(sample).execute())        # a pandas DataFrame

# Production: the same function on the real table in BigQuery
prod = ibis.bigquery.connect(project_id="my-project", dataset_id="shop")
result = paid_totals(prod.table("orders")).to_pyarrow()
```

`_` is Ibis's **deferred** placeholder: `_.amount` means "the `amount` column of whatever table this is applied to", which keeps chains short. Methods mirror SQL: `select`, `filter`, `mutate` (add columns), `group_by`/`aggregate`, `join`, `order_by`, `limit`, and window functions through `ibis.window` or `.over(...)`.

**Expressions are not data.** Until you call `execute()` (to pandas), `to_pyarrow()`, `to_polars()` or `to_parquet()`, nothing runs. An expression is a tree of operations, and you can look at the SQL it compiles to without connecting to anything, using an **unbound table** that has only a schema:

```python
orders = ibis.table(
    {"order_id": "int64", "customer": "string", "amount": "float64", "status": "string"},
    name="orders",
)
print(ibis.to_sql(paid_totals(orders), dialect="bigquery"))
print(ibis.to_sql(paid_totals(orders), dialect="duckdb"))
```

Both print a `SELECT ... WHERE ... GROUP BY ... ORDER BY` query, each with its own quoting and function names. This makes Ibis logic easy to unit test: test the function on DuckDB with tiny in-memory tables (`ibis.memtable(...)`), and review the generated SQL in code review.

[[fig:programming_ibis]]

**When to use it.**

| Situation | Good choice |
|---|---|
| Team writes SQL fluently, transformations live in the warehouse | dbt or Dataform SQL models |
| Python-first team, logic must run on DuckDB in tests and BigQuery in production | Ibis |
| pandas-style code that should run inside BigQuery | BigQuery DataFrames (`bigframes`) |
| Data fits on one machine and is already local | Polars or DuckDB directly |
| Need to move between warehouses (a migration, or several clouds' engines) | Ibis, to keep one codebase |

**Interoperability.** Ibis returns results as pandas, PyArrow or Polars, and `ibis.memtable(df)` turns a local pandas or Polars DataFrame into a table expression, so you can join a small local lookup table with a warehouse table (the backend uploads it as a temporary table).

> **Pitfall:** Assuming "portable" means "identical". Engines differ in details: integer division, `NULL` ordering in sorts, time zone handling of timestamps, string collation, and which functions exist. Ibis smooths over many differences and raises an error for operations a backend cannot do, but run your tests on the production backend too, not only on DuckDB.

> **Tip:** Keep BigQuery costs visible. An Ibis expression on a BigQuery table is a BigQuery query, billed the same way. Select only needed columns early, filter on the partition column, and check the SQL with `ibis.to_sql` when a job is expensive.

::: deeper
**How compilation works.** Every method call creates a new immutable operation node (`expr.op()` shows it), so an expression is a tree such as Aggregate(Filter(Table)). At execution time the backend's compiler walks the tree. Since Ibis 9, SQL backends compile through **SQLGlot**, a Python SQL parser and transpiler: Ibis builds a SQLGlot syntax tree, and SQLGlot generates the dialect-specific SQL text. Non-SQL backends translate the tree differently; the Polars backend, for example, builds a Polars lazy query.

**Backends.** Ibis supports around twenty engines, including DuckDB (the default for `memtable`), BigQuery, Snowflake, Databricks, PySpark, Trino, PostgreSQL, ClickHouse, DataFusion and Polars. Ibis 10 removed the old pandas and Dask backends; local execution goes through DuckDB, DataFusion or Polars instead.

**Escape hatches.** When Ibis lacks a function, `@ibis.udf.scalar.builtin` declares a backend's native SQL function so you can call it from Ibis, and `con.sql("SELECT ...")` wraps raw SQL as a table expression you can keep chaining on. Python UDFs exist for some backends but run row by row and should be a last resort.

**Ibis versus SQLGlot alone.** If you only need to translate existing SQL between dialects (for a migration), SQLGlot by itself does that: `sqlglot.transpile(sql, read="snowflake", write="bigquery")`. Ibis is for writing new logic as composable Python.

> **Interview:** "How would you let analysts develop pipelines locally without warehouse costs, then run them in BigQuery?" Write transformations as Ibis functions over table expressions, run unit tests on DuckDB with small `memtable` fixtures, and in production bind the same functions to BigQuery tables. Add an integration test on BigQuery for dialect differences, and review the generated SQL with `ibis.to_sql`.
:::

::: check
?? When does an Ibis expression actually query BigQuery?
Only when you call an execution method such as `execute()`, `to_pyarrow()`, `to_polars()` or `to_parquet()`. Building and chaining expressions is free.

?? Why should tests run on the production backend as well as DuckDB?
Engines differ in semantics (null ordering, integer division, time zones, available functions). Tests on DuckDB alone can pass while the BigQuery result differs.

?? What library does Ibis use to produce dialect-specific SQL?
SQLGlot, a Python SQL parser and transpiler, used by Ibis's SQL backends since version 9.
:::

::: resources
- Docs: [Ibis documentation](https://ibis-project.org/) · tutorials, the expression API and per-backend notes
- Docs: [Ibis BigQuery backend](https://ibis-project.org/backends/bigquery) · connecting, authentication and BigQuery-specific behaviour
- Repo: [ibis-project/ibis](https://github.com/ibis-project/ibis) · source, release notes and discussions
- Repo: [SQLGlot](https://github.com/tobymao/sqlglot) · the SQL parser and transpiler underneath Ibis's SQL backends
- Docs: [BigQuery DataFrames](https://cloud.google.com/bigquery/docs/bigquery-dataframes-introduction) · Google's pandas-style API that runs inside BigQuery, the main alternative on Google Cloud
- Docs: [Why Ibis?](https://ibis-project.org/why) · the project's own case for a portable DataFrame layer, and its trade-offs
- Docs: [Ibis internals](https://ibis-project.org/concepts/internals) · how expressions, operation nodes and backend compilers fit together
:::

## Other languages

Python is where most data engineers spend their days, but the platforms underneath are written in other languages, and each was chosen for a reason. Spark and Kafka run on the JVM, and Spark's own API language is **Scala**. A new generation of query engines and file-format libraries (DataFusion, Polars, delta-rs, Lance) is written in **Rust** for speed without a garbage collector. Much of the infrastructure you deploy on (Kubernetes, Terraform, Prometheus, Docker) and several stream processors are written in **Go**.

You do not need to master all three. The goal of this group is to read each one comfortably, know what it is good at, and recognise the moments when writing a piece of your platform in it is worth the extra cost for a team that mostly writes Python and SQL.

### Scala for Spark — Deep Dive

::: plain
Scala is the language Spark itself is written in. It runs on the Java Virtual Machine (JVM), the same runtime as Java, and mixes object-oriented code with **functional programming**: building results by transforming collections with small functions (map, filter, fold) instead of changing variables in loops. Writing Spark jobs in Scala is like speaking to Spark in its native language: you get compile-time checks, direct access to every internal API and no translation layer to Python.
:::

**The functional core.** Scala code describes data as immutable values and transforms them with higher-order functions, exactly the style Spark's API copies:

```scala
case class Order(orderId: Long, customer: String, amount: Double, status: String)

val orders = List(
  Order(1, "asha", 40.0, "paid"),
  Order(2, "bo", 15.5, "cancelled"),
  Order(3, "asha", 22.0, "paid"))

val total  = orders.map(_.amount).foldLeft(0.0)(_ + _)        // 77.5
val paid   = orders.filter(_.status == "paid").map(_.orderId) // List(1, 3)
val words  = List("a b", "c").flatMap(_.split(" "))           // List(a, b, c)
val byCust = orders.filter(_.status == "paid")
                   .groupMapReduce(_.customer)(_.amount)(_ + _) // Map(asha -> 62.0)
```

- `map` transforms each element; `flatMap` transforms each element into zero or more elements and flattens the result; `filter` keeps matching elements.
- `foldLeft(start)(f)` walks the collection carrying an accumulator; `reduce` does the same without a start value. Spark's `reduce` and `aggregate` have the same meaning but need the function to be **associative**, because partitions are combined in any order.
- `_ + _` is shorthand for `(a, b) => a + b`.
- A **case class** is an immutable record with equality, a readable `toString` and pattern matching built in. In Spark it doubles as a **schema definition**.

**Typed Spark with Datasets.** Spark's Scala API offers DataFrames (untyped rows, the same as PySpark) and **Datasets** (`Dataset[Order]`), where the compiler knows every field:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DailyTotals {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("daily-totals").getOrCreate()
    import spark.implicits._                                 // encoders for case classes

    val orders = spark.read.parquet(args(0)).as[Order]       // Dataset[Order]
    orders
      .filter($"status" === "paid")                          // column expression: optimisable
      .groupBy($"customer")
      .agg(sum($"amount").as("total"))
      .write.mode("overwrite").parquet(args(1))
    spark.stop()
  }
}
```

A typo such as `_.amout` in a typed lambda fails at compile time, not two hours into a job.

**Implicits and type classes.** `import spark.implicits._` brings **implicit** values into scope: values the compiler passes automatically when a function asks for them. Spark uses them to find an **encoder** (the code that converts a case class to Spark's binary row format) for each type. The same mechanism implements **type classes**, a way to add behaviour to types you do not own:

```scala
trait Show[A] { def show(a: A): String }

implicit val orderShow: Show[Order] = (o: Order) => s"#${o.orderId} ${o.customer}"

def render[A](xs: List[A])(implicit s: Show[A]): String = xs.map(s.show).mkString(", ")

render(orders)      // "#1 asha, #2 bo, #3 asha"
```

Scala 3 renamed this machinery to `given` and `using`, with clearer rules. Spark 4 is built for Scala 2.13, so Spark code still uses the Scala 2 syntax.

**Futures** run work asynchronously on a thread pool, useful in driver code that waits on several external calls:

```scala
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

val loads = (1 to 4).map(i => Future { s"part-$i loaded" })
val all   = Await.result(Future.sequence(loads), 30.seconds)   // Vector(part-1 loaded, ...)
```

**Building and running.** Scala projects use **sbt**. Spark itself is marked `provided`, because the cluster already has it:

```scala
// build.sbt
ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "daily-totals",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.0.1" % "provided",
      "org.scalatest"    %% "scalatest" % "3.2.19" % Test
    )
  )
```

`%%` appends the Scala version to the artifact name (`spark-sql_2.13`). Build a JAR with `sbt package` (or a "fat" JAR including dependencies with the sbt-assembly plugin), copy it to Cloud Storage, and submit it to Managed Service for Apache Spark, here in its serverless mode for Spark batches (formerly Dataproc Serverless):

```bash
sbt package
gcloud storage cp target/scala-2.13/daily-totals_2.13-0.1.0-SNAPSHOT.jar gs://my-artifacts/
gcloud dataproc batches submit spark \
  --region=europe-west1 \
  --class=DailyTotals \
  --jars=gs://my-artifacts/daily-totals_2.13-0.1.0-SNAPSHOT.jar \
  -- gs://my-raw-bucket/orders/ gs://my-curated-bucket/daily_totals/
```

Match the Scala and Spark versions to the runtime version you run on; a JAR built for a different Scala binary version fails with confusing `NoSuchMethodError`s.

> **Pitfall:** Typed lambdas hide work from the optimiser. `orders.filter(_.status == "paid")` is a black-box Scala function: Catalyst cannot push it into the Parquet scan, and every row must be decoded into an `Order` object first. `filter($"status" === "paid")` is an expression Catalyst understands and pushes down. Use typed operations for complex logic, column expressions for filters and projections.

> **Tip:** For most teams PySpark is the right default. Choose Scala when you write Spark libraries, custom data sources or listeners, need low-overhead UDFs, or work in a JVM-heavy shop.

::: deeper
**Why Scala UDFs beat Python UDFs.** A Scala UDF runs inside the executor's JVM, on Spark's internal rows. A plain Python UDF ships every batch of rows to a separate Python worker process and back, paying serialisation both ways. Arrow-based pandas UDFs narrow the gap, but built-in functions beat both, because Catalyst can optimise them and Tungsten can generate code for them.

**The JVM underneath.** Scala compiles to JVM bytecode. The JVM's **JIT** (just-in-time) compiler turns hot code into native machine code while the program runs, which is why Spark jobs speed up after warming up. Memory is managed by a **garbage collector** (G1 by default on modern Java); large heaps with many short-lived objects cause GC pauses, which is one reason Spark keeps data in its own binary format (Tungsten) rather than as JVM objects. Spark 4 requires Java 17 or 21.

**Libraries for functional programming.** **Cats** provides type classes such as `Functor`, `Monad` and `Semigroup`; **Cats Effect** and **ZIO** provide purely functional effects and concurrency for services and streaming apps (**fs2** for streams). Scalaz was the earlier library in this space and is now rarely chosen for new code. Spark jobs rarely need any of them; streaming services and Kafka tooling sometimes do.

**Testing.** Unit-test pure functions with ScalaTest or MUnit; for Spark logic create a local `SparkSession` (`.master("local[2]")`) once per suite and compare small Datasets with expected values.

> **Interview:** "Dataset or DataFrame in Scala?" A DataFrame is `Dataset[Row]`. Typed Datasets give compile-time safety and convenient domain objects, but lambdas over them are opaque to Catalyst and force object deserialisation. Use DataFrame column expressions on hot paths and typed operations where safety matters more than speed.
:::

::: check
?? What is the difference between `map` and `flatMap`?
`map` produces exactly one output per input element. `flatMap` produces zero or more outputs per element and flattens them into one collection.

?? Why is Spark marked `provided` in `build.sbt`?
The cluster or serverless runtime already supplies Spark. Bundling it again bloats the JAR and can cause version conflicts.

?? Why can `filter(_.status == "paid")` on a `Dataset[Order]` be slower than `filter($"status" === "paid")`?
The lambda is opaque to Catalyst, so the filter cannot be pushed down into the scan, and each row must be deserialised into an `Order` object before it runs.
:::

::: resources
- Docs: [Scala 3 Book](https://docs.scala-lang.org/scala3/book/introduction.html) · the official, free introduction to the language, including given/using
- Docs: [Spark SQL, DataFrames and Datasets guide](https://spark.apache.org/docs/latest/sql-programming-guide.html) · the Scala API with typed Datasets and encoders
- Book: [Programming in Scala, 5th Edition](https://www.artima.com/shop/programming_in_scala_5ed) · Odersky, Spoon and Venners; the definitive book from the language's creator
- Course: [Functional Programming Principles in Scala](https://www.coursera.org/learn/scala-functional-programming) · EPFL's course taught by Martin Odersky
- Docs: [sbt reference manual](https://www.scala-sbt.org/1.x/docs/) · builds, dependencies, scopes and plug-ins such as assembly
- Docs: [Cats](https://typelevel.org/cats/) · type classes and functional abstractions for Scala
:::

### Rust for Data Engineering

::: plain
Rust is a programming language that produces programs as fast as C or C++, but with a compiler that refuses to build code that could corrupt memory or race between threads. It manages memory without a garbage collector, through strict rules about who **owns** each piece of data. For data engineering that makes it the language of choice for building engines and libraries: the fast heart of Polars, DataFusion, delta-rs, Pydantic and uv is Rust, while you call it from Python.
:::

**Why engines are moving to Rust.** A query engine spends its life in tight loops over columns of numbers and strings. It needs:

- **Predictable speed**: no garbage-collection pauses, control over memory layout, use of SIMD (single instruction, multiple data) CPU instructions.
- **Safety**: C++ engines have a long history of crashes and security bugs from memory errors; Rust's compiler rules out whole classes of them.
- **Easy parallelism**: the compiler checks that threads cannot share mutable data unsafely, so multithreaded code is less risky to write.
- **Good Python bindings**: PyO3 and maturin make a Rust library installable from PyPI as an ordinary wheel.

**Ownership in one example.** Every value has exactly one owner. You can lend it out (**borrow**) for reading with `&`, and when the owner goes out of scope the memory is freed. No garbage collector is needed:

```rust
fn total(amounts: &[f64]) -> f64 {     // borrows the slice; does not take ownership
    amounts.iter().sum()
}

fn main() {
    let amounts = vec![40.0, 22.0, 9.5];
    println!("{}", total(&amounts));   // 71.5
    let moved = amounts;               // ownership moves to `moved`
    // println!("{:?}", amounts);      // compile error: borrow of moved value
    println!("{}", moved.len());       // 3
}
```

**The Rust data ecosystem.**

| Project | What it is |
|---|---|
| arrow-rs | The official Rust implementation of Apache Arrow, plus the `parquet` crate |
| Apache DataFusion | An extensible SQL and DataFrame query engine built on arrow-rs |
| DataFusion Ballista | Distributed execution for DataFusion: a scheduler and executors |
| DataFusion Comet | A Spark plug-in that runs parts of Spark SQL plans natively on DataFusion |
| Polars | The DataFrame engine from the previous group |
| delta-rs, iceberg-rust | Delta Lake and Apache Iceberg table formats without a JVM |
| Lance | A columnar format and library for ML and vector data |

**DataFusion** is the one most likely to appear in your platform. It is a library, not a server: you embed it to get a full SQL engine with a parser, a cost-aware optimiser and a multithreaded, vectorised executor over Arrow data. A complete program that runs SQL on Parquet:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("orders", "data/orders.parquet", ParquetReadOptions::default()).await?;
    let df = ctx
        .sql("SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer")
        .await?;
    df.show().await?;
    Ok(())
}
```

DataFusion also has Python bindings (`pip install datafusion`), so you can try it without writing Rust.

**Writing a fast Python extension.** The realistic Rust task for a data engineer is speeding up one hot function, for example a custom parser or checksum, and calling it from Python:

```rust
use pyo3::prelude::*;

#[pyfunction]
fn checksum(data: &[u8]) -> u64 {
    data.iter().map(|&b| b as u64).sum()
}

#[pymodule]
fn fastcheck(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(checksum, m)?)?;
    Ok(())
}
```

```bash
pip install maturin
maturin develop --release          # builds and installs the module into the active venv
python -c "import fastcheck; print(fastcheck.checksum(b'abc'))"   # 294
```

> **Pitfall:** Rewriting a pipeline in Rust to make it faster. Most pipeline time is spent waiting on I/O or inside engines that are already native (BigQuery, Spark, Polars). Profile first; Rust pays off for CPU-bound inner loops and for libraries many jobs share, not for glue code.

> **Tip:** You can benefit from Rust without writing it: Polars, DataFusion's Python package, `deltalake` (delta-rs), Pydantic v2, Ruff and uv are all Rust underneath.

::: deeper
**How DataFusion is built for extension.** Almost every part is a trait you can replace: `TableProvider` (a new data source, such as a custom file layout or an API), scalar, aggregate and window UDFs, optimiser rules, and physical operators. That is why many products embed it rather than writing an engine from scratch: InfluxDB 3 and several stream and lakehouse engines use it as their SQL layer. Execution is **pull-based and streaming**: each physical operator produces a stream of Arrow `RecordBatch`es, partitions run in parallel on the Tokio runtime, and operators such as hash aggregate can spill to disk under memory pressure.

**Ballista** adds a scheduler that splits a DataFusion plan into stages at shuffle boundaries and sends tasks to executors over gRPC, the same architecture as Spark. It is much less mature than Spark and suits experiments and embedded use more than production platforms.

**Async and Tokio.** Rust I/O libraries are usually async, built on the **Tokio** runtime (`#[tokio::main]`). `async`/`await` works like Python's, but futures do nothing until polled, and the compiler checks that data shared between tasks is safe to send between threads (`Send` and `Sync` traits).

**Cost of adoption.** Rust has a steep learning curve (ownership, lifetimes, traits) and slower compile times. For a Python team, the sensible scope is a small, well-tested extension module or a contribution to an existing engine, built in CI into wheels for each platform with maturin.

> **Interview:** "Why do new data systems choose Rust over Java or C++?" Performance close to C++ with no garbage-collection pauses and control over memory layout for vectorised execution; memory and thread safety checked at compile time; a strong Arrow ecosystem (arrow-rs, DataFusion) to build on; and easy Python bindings through PyO3, which matters because most users call engines from Python.
:::

::: check
?? What does Rust use instead of a garbage collector to free memory?
Ownership: every value has one owner, and its memory is freed when the owner goes out of scope. Borrowing lets other code use the value temporarily without owning it.

?? What is DataFusion, and how is it different from a database server?
An embeddable query engine library in Rust, with a SQL parser, optimiser and vectorised executor over Arrow. You link it into your own program rather than connecting to a running server.

?? When is writing Rust worth it for a data team that mostly writes Python?
For CPU-bound hot spots (custom parsing, encoding, checksums) or shared libraries used by many jobs, after profiling shows the time is spent there. Not for I/O-bound glue code.
:::

::: resources
- Book: [The Rust Programming Language](https://doc.rust-lang.org/book/) · the free official book; ownership, traits, error handling and concurrency
- Docs: [Apache DataFusion](https://datafusion.apache.org/) · user guide, SQL reference, Python bindings and the library guide for extending it
- Paper: [Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine](https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf) · Lamb et al., SIGMOD 2024; the design of DataFusion
- Repo: [apache/arrow-rs](https://github.com/apache/arrow-rs) · the Rust implementation of Arrow and Parquet
- Docs: [DataFusion Ballista](https://datafusion.apache.org/ballista/) · the distributed scheduler and executors for DataFusion
- Docs: [PyO3 user guide](https://pyo3.rs/) · writing Python extension modules in Rust, built and published with maturin
:::

### Go for Data Engineering

::: plain
Go is a small, simple language from Google, designed for building network services and tools that are fast, easy to read and easy to deploy. A Go program compiles to a single file you can copy anywhere and run, with no interpreter or libraries to install. Its standout feature is **goroutines**: very cheap threads that let one program handle thousands of things at once, connected by **channels**, which work like conveyor belts passing items between workers.
:::

You probably already run a lot of Go: Kubernetes, Docker, Terraform, Prometheus, Grafana's backend, and the Google Cloud CLI's components are written in it. In data engineering Go shows up in three places: **stream processors and connectors**, **command-line tools** for the platform, and **network services** such as ingestion APIs and gRPC endpoints.

**Concurrency with goroutines and channels.** A pipeline of stages, each running concurrently, with three parallel workers in the middle:

```go
package main

import (
	"fmt"
	"strings"
	"sync"
)

func main() {
	lines := make(chan string)
	upper := make(chan string)

	go func() { // stage 1: produce
		for _, l := range []string{"asha,40", "bo,15", "cy,9"} {
			lines <- l
		}
		close(lines)
	}()

	var wg sync.WaitGroup
	for w := 0; w < 3; w++ { // stage 2: three parallel workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for l := range lines {
				upper <- strings.ToUpper(l)
			}
		}()
	}
	go func() { wg.Wait(); close(upper) }() // close the output when all workers finish

	count := 0
	for range upper { // stage 3: consume
		count++
	}
	fmt.Println(count) // 3
}
```

Unlike Python threads, goroutines run Go code on all CPU cores at once, so this model works for both I/O-bound and CPU-bound work.

**Benthos, Redpanda Connect and Bento.** Benthos is a stream processor written in Go and configured entirely in YAML: inputs, processors and outputs, with a mapping language called **Bloblang** for transforming messages. It is popular for the unglamorous middle of data platforms: read from Kafka, clean and reshape JSON, enrich, route, and write to Pub/Sub, Cloud Storage or a database, with at-least-once delivery and back pressure built in. In 2024 Redpanda acquired Benthos and renamed it **Redpanda Connect** (the core engine stays open source in the `redpanda-data/benthos` repository, and many connectors are under Redpanda's licences). WarpStream then forked the MIT-licensed code as **Bento**. The configuration format is the same:

```yaml
input:
  kafka:
    addresses: [ "kafka-1:9092" ]
    topics: [ "orders" ]
    consumer_group: "orders-cleaner"

pipeline:
  processors:
    - mapping: |
        root = this
        root.customer = this.customer.lowercase()
        root.amount_cents = (this.amount * 100).round()

output:
  gcp_pubsub:
    project: my-project
    topic: orders-clean
```

One static binary, one YAML file, and it runs happily on Cloud Run or GKE.

**CLI tools.** Platform teams write Go for internal tools (a `dataplat` command that creates datasets, rotates keys or validates configs) because a single binary is trivial to distribute, starts instantly and needs no virtual environment. Libraries such as Cobra handle commands and flags.

**Services and gRPC.** **gRPC** is a protocol for calling functions on another machine, with the request and response types defined in Protocol Buffers (`.proto` files) and client and server code generated for each language. Many Google Cloud APIs, including Pub/Sub and the BigQuery Storage Read and Write APIs, are gRPC underneath. Go is a natural fit for high-throughput ingestion services that accept events over gRPC or HTTP and forward them to Pub/Sub.

**Go vs Python.**

| | Go | Python |
|---|---|---|
| Speed of plain code | Compiled; typically many times faster for CPU-bound loops | Interpreted; fast only when calling native libraries |
| Concurrency | Goroutines on all cores | GIL limits threads to one core for Python code (without the free-threaded build) |
| Deployment | One static binary | Interpreter plus dependencies, usually in a container |
| Data libraries | Thin: no pandas, Spark or Polars equivalents of the same maturity | The richest ecosystem of any language |
| Best for | Services, CLIs, connectors, stream routing | Transformations, analysis, orchestration, ML |

> **Pitfall:** Leaking goroutines. A goroutine blocked forever on a channel nobody reads is never freed. Always close channels from the sending side when done, and use `context.Context` cancellation so workers stop when a request or job is cancelled.

> **Tip:** Before writing a custom Go service to move messages between systems, check whether a Redpanda Connect or Bento configuration does it. Many "ingestion microservices" turn out to be 20 lines of YAML.

::: deeper
**The scheduler.** The Go runtime multiplexes many goroutines onto a few OS threads (an M:N scheduler). `GOMAXPROCS` sets how many run Go code at once (by default the number of CPUs; since Go 1.25 it also respects container CPU limits). Goroutines start with a small stack that grows as needed, so a program can run hundreds of thousands of them.

**Garbage collection.** Go has a concurrent, low-pause garbage collector tuned for latency rather than throughput. That suits services; for engines that churn through billions of values, the allocation cost is one reason engine builders prefer Rust or C++.

**Structured concurrency.** The `golang.org/x/sync/errgroup` package runs a group of goroutines, cancels the rest if one fails, and returns the first error: the Go equivalent of Python's `asyncio.TaskGroup`.

**Google Cloud client libraries.** Google publishes idiomatic Go clients (`cloud.google.com/go/...`) for Pub/Sub, BigQuery, Cloud Storage and most other services, with built-in retries and flow control. The Pub/Sub client, for example, handles leasing, acknowledgement deadlines and concurrency limits for subscribers.

> **Interview:** "When would you choose Go over Python for a data component?" For long-running, high-throughput network services and connectors (ingestion endpoints, message routers), CLIs that must ship as one binary, and anything needing real multi-core concurrency with low memory. Keep transformations, analysis and orchestration in Python and SQL, where the ecosystem is far richer.
:::

::: check
?? Why is a single static binary an advantage for platform tools?
It runs anywhere with no interpreter, virtual environment or dependencies to install, starts instantly, and is trivial to distribute and put in a small container image.

?? What does a Benthos (Redpanda Connect or Bento) configuration consist of?
An input, a pipeline of processors (often Bloblang mappings) and an output, all in YAML, run by one binary.

?? How do goroutines differ from Python threads for CPU-bound work?
Goroutines run Go code in parallel on all CPU cores. Python threads in standard CPython share one GIL, so CPU-bound Python code runs on one core at a time.
:::

::: resources
- Docs: [Go documentation](https://go.dev/doc/) · getting started, effective Go and the standard library
- Course: [A Tour of Go](https://go.dev/tour/) · the free interactive introduction, including goroutines and channels
- Blog: [Go concurrency patterns: pipelines and cancellation](https://go.dev/blog/pipelines) · the official guide to building pipelines with channels
- Docs: [Redpanda Connect documentation](https://docs.redpanda.com/redpanda-connect/home/) · the current docs for Benthos: inputs, processors, outputs and Bloblang
- Repo: [Bento](https://github.com/warpstreamlabs/bento) · the MIT-licensed community fork of Benthos
- Docs: [gRPC quick start for Go](https://grpc.io/docs/languages/go/quickstart/) · define a service in protobuf and generate a Go server and client
- Book: *Concurrency in Go* by Katherine Cox-Buday · O'Reilly's deep treatment of goroutines, channels and concurrency patterns
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, code or rule of thumb |
|---|---|
| Python's job in a pipeline | Drive engines (BigQuery, Spark) and glue systems; do not loop over big data in Python |
| Stream records in constant memory | Chain generators; write with `itertools.batched(rows, 500)` |
| Reproducible environment | `uv add ...` writes `pyproject.toml` and `uv.lock`; `uv run pytest` |
| I/O-bound concurrency | `ThreadPoolExecutor(max_workers=N)` or `asyncio` with a `Semaphore` |
| CPU-bound pure Python | `ProcessPoolExecutor`, or a native library (Polars, DuckDB, NumPy) |
| Blocking call inside async code | `await asyncio.to_thread(func, ...)` |
| Static types | Hints plus `mypy --strict` in CI |
| Validate untrusted records | Pydantic `Model.model_validate(data)`; failures to a dead-letter table |
| Create connectors from config | Registry dictionary plus factory function |
| Retry safely | Exponential backoff with jitter, transient errors only, idempotent operations only |
| Polars lazy query | `pl.scan_parquet(...).filter(...).group_by(...).agg(...).collect()` |
| Larger than memory in Polars | `collect(engine="streaming")` or `sink_parquet(...)` |
| See Ibis SQL | `ibis.to_sql(expr, dialect="bigquery")` |
| Portable Ibis logic | Functions over table expressions; test on DuckDB, run on BigQuery |
| Scala Spark dependency | `"org.apache.spark" %% "spark-sql" % "<version>" % "provided"` |
| Submit a Scala Spark batch | `gcloud dataproc batches submit spark --class=Main --jars=gs://.../app.jar` |
| Fast filter in Scala Spark | Column expressions (`$"status" === "paid"`), not typed lambdas |
| Rust SQL engine to embed | Apache DataFusion (`SessionContext::new()`, `ctx.sql(...)`) |
| Python extension in Rust | PyO3 plus `maturin develop --release` |
| Message routing without code | Redpanda Connect or Bento YAML: input, processors, output |

### Practice

::: try
**1. A streaming parser with a dead-letter list.** Write a generator `parse(lines)` that reads CSV lines `order_id,amount`, yields `(int, float)` tuples for valid lines and appends invalid lines to a list `rejects`. Run it on `["1,40.0", "2,abc", "3,9.5", "oops"]` and print the valid tuples and the number of rejects. Use only the standard library.
:::

?? A solution to 1

```python
def parse(lines, rejects):
    for line in lines:
        try:
            oid, amount = line.split(",")
            yield int(oid), float(amount)
        except ValueError:
            rejects.append(line)

rejects = []
good = list(parse(["1,40.0", "2,abc", "3,9.5", "oops"], rejects))
print(good)           # [(1, 40.0), (3, 9.5)]
print(len(rejects))   # 2
```

::: try
**2. Choose the concurrency model.** For each job, pick threads, processes, asyncio or "push it to an engine", and say why: (a) download 5,000 small JSON files from an HTTP API; (b) compute SHA-256 hashes of 200 large local files in pure Python; (c) join two 40 GB tables already in BigQuery; (d) call a blocking SFTP library for 20 files.
:::

?? Answer to 2
(a) asyncio (or a thread pool) with bounded concurrency: it is I/O-bound with many requests. (b) Processes, or a thread pool, because `hashlib` releases the GIL for large inputs; processes are the safe general answer for CPU-bound work. (c) Push it to the engine: run the join in BigQuery SQL, never in Python. (d) A small thread pool: the library blocks, and the work is I/O-bound.

::: try
**3. A registry-based writer.** Extend the strategy example from the design patterns topic into a registry: a `@writer("jsonl")` decorator that adds functions to a `WRITERS` dictionary, and a `write(rows, fmt)` function that raises a clear `ValueError` for unknown formats. Show it rejecting `"xml"`.
:::

?? A solution to 3

```python
import json

WRITERS = {}

def writer(fmt):
    def deco(fn):
        WRITERS[fmt] = fn
        return fn
    return deco

@writer("jsonl")
def to_jsonl(rows):
    return "\n".join(json.dumps(r) for r in rows)

def write(rows, fmt):
    try:
        return WRITERS[fmt](rows)
    except KeyError:
        raise ValueError(f"unknown format {fmt!r}; choose from {sorted(WRITERS)}") from None

print(write([{"id": 1}], "jsonl"))   # {"id": 1}
try:
    write([{"id": 1}], "xml")
except ValueError as e:
    print(e)                          # unknown format 'xml'; choose from ['jsonl']
```

::: try
**4. Lazy or eager?** In Polars, a teammate writes `pl.read_parquet("gs://bucket/events/*.parquet").filter(pl.col("country") == "IN").select("user_id", "ts")` on 300 files with 80 columns. What is wasteful, and how do you fix it?
:::

?? Answer to 4
`read_parquet` is eager: it reads all 80 columns of every row group of every file into memory before filtering. Use `pl.scan_parquet(...)` with the same chain and `.collect()` at the end, so projection pushdown reads only `user_id`, `ts` and `country`, and predicate pushdown skips row groups whose statistics show no `IN` rows. For very large inputs, add `engine="streaming"`, or run the query in BigQuery instead.

### What comes next

You can now write the code around your SQL: streaming, typed, tested Python; fast local DataFrames with Polars; portable logic with Ibis; and you know where Scala, Rust and Go fit in the platform. The next section, **Data modeling**, steps back from code to design: how to decide which tables to build, at what grain, with which keys, so that the pipelines you write produce data that is correct, easy to query and able to change as the business does.
