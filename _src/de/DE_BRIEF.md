# Brief: write sections of the Data Engineering course

You are writing sections of a static, single-page data engineering course, "Data engineering, end to end (Google Cloud edition)". It replaces an old topic map that only listed titles and links, so **every article is written from scratch**. The format is the one already approved for the companion Python course. Read `model_setup.md` (a finished Python section) in full before writing, and match its structure, tone and depth.

Working directory (DE): `/Users/dheemanth/Desktop/projects/gdsdheemanth.github.io/_src/de`. Everything below is relative to it.

## What to produce

For each section id `<sid>` assigned to you:

1. `course/<sid>.md`: the section text (required).
2. `figs/<sid>.js`: diagrams for the section (2 to 5 where a picture genuinely helps; optional for very small sections).

**Do not edit any other file**: not app.js, shell.html, build.py, skeleton*, decheck.py, linkcheck.py, other agents' files. Do not run `build.py`. Do not commit. Do not touch anything outside DE except throwaway files in your own temp folder.

Get your section's plan with `python3 skel.py <sid>`. It prints the groups and the **exact** topic titles in order, a scope line for each topic (what it should cover, at minimum), the original resources from the old page, whether the topic is new, and OVERLAP flags. Group and topic titles must match it character for character, in the same order.

## File format (same dialect as model_setup.md)

1. **Section intro** (text before the first `##`): who it is for and what it assumes (name earlier sections it builds on), "What you will be able to do afterwards" (bullets), and the big-picture mental model. Then one line: "Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more." Do not repeat setup.md's long "How each topic is laid out" explainer.
2. **Groups**: `## <group title>` then a 1 to 3 paragraph bridge: why the group matters and how it connects to the previous one.
3. **Topics**: `### <topic title>`, then in this order:
   - `::: plain` ... `:::` The idea in everyday words with an analogy, for someone meeting it for the first time. 2 to 5 sentences. Required.
   - **The explanation**: what it is, why it exists, how it works internally, with concrete examples (SQL, Python, YAML, Terraform, bash, config) and small worked numbers where they help. Build from simple to realistic. Define jargon on first use. Use `> **Pitfall:**` and `> **Tip:**` callouts for real mistakes and advice. Use tables for comparisons.
   - `::: deeper` ... `:::` Internals, edge cases, performance details, version notes, trade-offs, and `> **Interview:**` callouts. Include in nearly every topic.
   - `::: check` ... `:::` 2 or 3 `?? question` lines, each followed directly by its answer line(s) (no blank line between question and answer; a blank line between questions).
   - `::: resources` ... `:::` 4 to 7 resources to learn this topic in depth, one per line, in this exact form:
     `- Type: [Title](https://url) · short note on what it gives you`
     where Type is one of `Book`, `Docs`, `Paper`, `Course`, `Video`, `Article`, `Blog`, `Repo`, `Talk`, `Spec`. A book with no good URL may be written `- Book: *Title* by Author · note` (no link). Mix kinds: official documentation first, then a book or book chapter, a paper or design doc where one exists, and one or two high-quality articles, talks or courses. Keep the original resources from `skel.py` when they are still good and relevant (re-check them), drop the weak ones.
   - Length: about 1,000 to 1,300 words per topic before resources. OVERLAP topics are shorter, see below.
4. **`## Wrap-up`** at the end with `### Cheat sheet` (two-column table, 10 to 20 rows: concept or task | command, SQL, rule of thumb), `### Practice` (3 to 5 exercises in `::: try` blocks, each followed by `?? Answer to N` or `?? A solution to N`; a solution may be a fenced code block after a blank line), and `### What comes next` (one paragraph naming the next section by its exact title; `skel.py` prints it).

Markdown supported: `##`/`###`/`####`, paragraphs, `- ` and `1. ` lists (continuation lines indented 2+ spaces), fenced code with a language (```sql, ```python, ```bash, ```yaml, ```hcl, ```json, ```text), `|` tables (escape a literal pipe in a cell as `\|`), callouts `> **Tip:**` / `> **Pitfall:**` / `> **Interview:**` / `> **Note:**`, `?? question` + answer, `::: plain|deeper|check|try|guide|resources` blocks closed by a line `:::`, `[[fig:key]]` alone on a line, inline `code`, **bold**, *italic*, [links](https://...). Nothing else: no HTML, no nested lists, no images other than figures. Never write the literal text `</script`.

## Cloud rule: Google Cloud only

The user wants AWS and Azure removed completely. Google Cloud is the only cloud in this course.

- Never describe, recommend or link AWS or Azure services or docs (no aws.amazon.com, no azure.microsoft.com, no learn.microsoft.com/azure). Amazon book-shop links are allowed but prefer the publisher's page (oreilly.com, manning.com) or the author's site.
- When a topic needs a cloud service, use the Google Cloud one: object storage = Cloud Storage; warehouse = BigQuery; streaming = Pub/Sub or Google Managed Service for Apache Kafka; Spark/Hadoop = Managed Service for Apache Spark (formerly Dataproc); Beam = Dataflow; serverless = Cloud Run, Cloud Run functions, Batch; orchestration = Managed Service for Apache Airflow (formerly Cloud Composer), Workflows; CDC = Datastream; catalog/governance/quality = Knowledge Catalog (formerly Dataplex Universal Catalog); secrets = Secret Manager; keys = Cloud KMS; NoSQL = Bigtable, Firestore; relational = Cloud SQL, AlloyDB, Spanner; ML = Gemini Enterprise Agent Platform (formerly Vertex AI); BI = Looker.
- Keep every open-source project and every vendor platform (Snowflake, Databricks, Confluent, ClickHouse, MotherDuck, Fivetran, Airbyte, Monte Carlo and so on), and all lakehouse technologies (Iceberg, Delta, Hudi, Paimon, Unity Catalog, Polaris...). Where hosting matters, describe them running on Google Cloud.
- Use current Google Cloud names (verified on Google's docs, November 2026), and give the former name once on first mention in your section, for example "Knowledge Catalog (formerly Dataplex Universal Catalog)":
  - Managed Service for Apache Spark (formerly Dataproc); its serverless mode for Spark batches (formerly Dataproc Serverless).
  - Knowledge Catalog (formerly Dataplex Universal Catalog; Data Catalog before that).
  - Google Cloud Lakehouse (formerly BigLake); Lakehouse runtime catalog (formerly BigLake metastore); Apache Iceberg managed tables (formerly BigQuery tables for Apache Iceberg).
  - Gemini Enterprise Agent Platform (formerly Vertex AI).
  - BigQuery sharing (formerly Analytics Hub).
  - Data Studio (formerly Looker Studio). Looker (the BI platform with LookML) keeps its name.
  - Also: Cloud Run functions (formerly Cloud Functions), Artifact Registry, BigQuery editions (Standard, Enterprise, Enterprise Plus), Gemini in BigQuery, Google Managed Service for Apache Kafka, Spot VMs.
  - Managed Service for Apache Airflow, or "Managed Airflow" (formerly Cloud Composer; generations Gen 3, Gen 2, Legacy Gen 1).
  - API names, CLI commands (`gcloud dataproc ...`, `bq ...`) and IAM role names usually keep the old product names; that is expected.
  If you are not sure a feature is GA, say "preview" or leave it out.

## OVERLAP topics (marked by skel.py)

These are covered in full depth by the companion guides. Write a complete, beginner-friendly article (plain words, the core explanation with an example, the key points, one or two pitfalls, Check yourself, resources), about 500 to 700 words, with a short or no Go deeper. Then add, right before `::: check`:

```
::: guide
For the internals, tuning and failure modes, read [Shuffle internals](pyspark-streaming-kafka-topic-map.html#shuffle) and [Data skew: detection and fixes](pyspark-streaming-kafka-topic-map.html#skew) in the PySpark + Kafka guide.
:::
```

Link the most relevant section(s). PySpark + Kafka guide section ids (link as `pyspark-streaming-kafka-topic-map.html#<id>`): dist (Distributed data processing fundamentals), arch (Spark architecture and application lifecycle), rdd (RDDs, lineage and the core abstractions), pyruntime (PySpark runtime and environment), df (DataFrame API essentials), joins (Joins: semantics and API), agg (Aggregations and window functions), sql (Spark SQL language and catalogs), io (Data sources, file formats and layout), udf (UDFs, pandas and Arrow in PySpark), sched (Execution model and scheduling), catalyst (Catalyst optimizer and query planning), tungsten (Tungsten, code generation and columnar execution), aqe (Adaptive Query Execution), shuffle (Shuffle internals), memory (Memory management), joinphys (Physical join strategies), partition (Partitioning, caching and checkpointing), connect (Spark Connect architecture), deploy (Deployment and cluster managers), method (Tuning methodology and resource sizing), skew (Data skew), smallfiles (Small files and output layout), gc (JVM and garbage collection tuning), pyperf (PySpark-specific performance), storageopt (Storage and table-format optimization), ui (Spark UI mastery), profiling (Profiling CPU, memory and Python), oom (Out-of-memory failures), ssbasics (Streaming fundamentals), eventtime (Event time, watermarks and windows), stateful (Stateful processing), statestore (State store internals), ft (Checkpointing, fault tolerance and exactly-once), kbasics (Kafka architecture fundamentals), kstorage (Kafka storage internals), krepl (Replication, durability and KRaft), kprod (Producer internals), kcons (Consumers, groups and rebalancing), ktxn (Transactions and exactly-once), kstreams (Kafka Streams, Connect and the ecosystem), kops (Kafka operations, security and sizing), testing (Testing PySpark code), codeorg (Code organization, CI/CD and orchestration), design (Data system design), modeling (Data modeling for interviews), core (Spark Core engine internals), sqlengine (Spark SQL engine internals).
Python guide section ids (link as `python-end-to-end-topic-map.html#<id>`): setup, types, functions, iteration, oop, datamodel, decorators, context, typing, stdlib, functional, gil, concurrency, asyncio, perf, testing, packaging, idiomatic, patterns, web, db, data, automation.

## Diagrams

Put them in `figs/<sid>.js`:

```js
Object.assign(FIGS, {
<sid>_name:{ t:"Short title", cap:"One or two sentences explaining what the reader should see.", svg:S("<sid>1", 260, "aria label",
  B(20, 40, 160, 50, "sp", ["Box title", "small second line"]) + A("<sid>1", "M180 65 L300 65") + T(380, 30, "label", "sm", "middle")) },
});
```

Helpers (already loaded, see `figs_helpers.js` and the examples in `py_figs_all.js`): `S(markerId, height, ariaLabel, body)` makes a 760-wide SVG; `B(x, y, w, h, cls, [lines], bold)` a rounded box with centred text (first line bold); `A(markerId, "M.. L..", cls)` an arrow path (cls `ln` default, or `acc`, `kac`, `stc`, plus `dash`); `T(x, y, text, cls, anchor)` text (cls `sm`, `b`, `mono`, `tR`, `tA`, `tK`, `tS`, `tP`). Box classes: `box`, `fill`, `sp`, `st`, `ka`, `pr`, `okf`. Use a unique marker id per figure (`<sid>1`, `<sid>2`...) and the same id in its `A()` calls. Keys must start with `<sid>_`. Reference each key exactly once in the course file with `[[fig:<sid>_name]]` on its own line. Keep text inside boxes short so it does not overflow (about 7 px per character at 13 px). Check syntax with: `node -e "eval(require('fs').readFileSync('figs_helpers.js','utf8')+require('fs').readFileSync('figs/<sid>.js','utf8')); console.log(Object.keys(FIGS))"`.

## Quality rules

- **Correct and current** as of November 2026. When unsure of a detail (a default value, a limit, a version), check official docs (you may use web search/fetch sparingly) or leave the detail out. Never invent numbers, flags or APIs.
- **Every URL must be real.** Only use URLs you are confident exist, prefer stable top-level documentation pages over deep anchors, and verify them with `python3 linkcheck.py course/<sid>.md`. Fix or remove every link in `bad`. Links in `blocked` (sites that refuse scripts, such as oreilly.com, amazon, medium, udemy) are acceptable only if you are confident the page exists.
- Code: SQL defaults to BigQuery GoogleSQL unless the topic is about another engine (then use that engine's dialect: PostgreSQL, Trino, Spark SQL, ClickHouse, DuckDB, Flink SQL). Python examples that can run with only the standard library must run with `python3` and print what their comments say; verify them. SQL that runs on SQLite can be checked with Python's sqlite3. Comments that show results must be exact.
- Style: plain, direct English; short sentences; define terms; no em dashes (use colons, commas or parentheses); no filler; no marketing language.

## Checks before finishing

1. `python3 decheck.py <sid>`: 0 problems (titles, counts, plain/check/resources in every topic, at least 3 resources, balanced blocks, figure keys exist, Wrap-up present, no AWS/Azure links).
2. `python3 linkcheck.py course/<sid>.md`: no `bad` links left.
3. Figures load with the node command above, and every key is referenced exactly once.
4. Runnable code examples verified.

## Report back (short)

Per section: topics, words, figures, check results, links in `blocked` you kept, and anything unresolved.

## Section order (for "What comes next")

See `section_order.txt`. The last section (industry) ends the course: its "What comes next" should suggest how to keep learning and practising (projects, certifications such as the Google Cloud Professional Data Engineer, communities).
