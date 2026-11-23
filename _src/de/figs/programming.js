/* programming: Programming for data engineering, original drawings */
Object.assign(FIGS, {

programming_concurrency:{ t:"Choosing a concurrency model", cap:"Start from what the job spends its time on. Waiting on networks or disks is I/O-bound: use threads with blocking libraries or asyncio for very many requests, always with a bound. Heavy computation is CPU-bound: use processes, or better a native library that runs outside the GIL. Data already in a warehouse should not be processed in Python at all.", svg:S("programming1", 300, "Decision tree from the kind of work to threads, asyncio, processes, native libraries or the warehouse",
  B(280, 16, 200, 50, "box", ["What does the job", "spend its time on?"]) +
  B(30, 110, 200, 54, "ka", ["Waiting on I/O", "APIs, files, databases"]) +
  B(280, 110, 200, 54, "sp", ["Computing in Python", "parsing, hashing, maths"]) +
  B(530, 110, 200, 54, "st", ["Big data in a warehouse", "joins, aggregations"]) +
  A("programming1", "M330 66 L150 108") + A("programming1", "M380 66 L380 108") + A("programming1", "M430 66 L610 108") +
  B(10, 210, 115, 60, "okf", ["Threads", "blocking clients"]) +
  B(135, 210, 115, 60, "okf", ["asyncio", "1000s of requests"]) +
  B(270, 210, 105, 60, "okf", ["Processes", "pure Python"]) +
  B(385, 210, 105, 60, "okf", ["Native libs", "Polars, DuckDB"]) +
  B(530, 210, 200, 60, "okf", ["Push down to the engine", "SQL in BigQuery or Spark"]) +
  A("programming1", "M100 164 L70 208") + A("programming1", "M160 164 L190 208") +
  A("programming1", "M350 164 L325 208") + A("programming1", "M410 164 L435 208") + A("programming1", "M630 164 L630 208") +
  T(130, 290, "bound it: Semaphore, pool size, max_workers", "sm tR", "middle")) },

programming_patterns:{ t:"Patterns in one pipeline run", cap:"A YAML config goes to a builder that validates it. A factory looks the connector type up in a registry and creates the source and sink. Every I/O call is wrapped in a retry decorator, a strategy function picks the output format, and lifecycle events go to observers that alert, record lineage and emit metrics.", svg:S("programming2", 300, "Config flows through builder, factory and registry into a source and sink wrapped by retry, with strategy and observers",
  B(20, 30, 120, 54, "box", ["config.yaml", "type: bigquery"]) +
  B(170, 30, 130, 54, "pr", ["Builder", "validate once"]) +
  B(330, 30, 130, 54, "sp", ["Factory", "kind to class"]) +
  B(490, 22, 250, 70, "fill", ["Registry", "csv: CsvSource", "bigquery: BigQuerySource"]) +
  A("programming2", "M140 57 L168 57") + A("programming2", "M300 57 L328 57") + A("programming2", "M460 57 L488 57") +
  B(120, 140, 150, 54, "ka", ["Source.read()", "inside @retry"]) +
  B(305, 140, 150, 54, "st", ["Transform", "pure functions"]) +
  B(490, 140, 150, 54, "ka", ["Sink.write()", "strategy: csv/jsonl"]) +
  A("programming2", "M395 84 L210 138", "ln dash") + A("programming2", "M395 84 L560 138", "ln dash") +
  A("programming2", "M270 167 L303 167") + A("programming2", "M455 167 L488 167") +
  B(120, 236, 520, 46, "okf", ["Observers: alerting · lineage · metrics", "events.emit(\"load_succeeded\", rows=1200)"]) +
  A("programming2", "M380 194 L380 234", "acc")) },

programming_polars:{ t:"Polars lazy plan: pushdown into the scan", cap:"The query as written reads the files, then filters, then aggregates. The optimiser rewrites the plan so the filter and the column list move into the Parquet scan: row groups ruled out by their statistics are skipped and unused columns are never read, so far less data reaches the aggregation.", svg:S("programming3", 290, "Naive plan versus optimised plan with predicate and projection pushdown",
  T(190, 26, "As written", "b", "middle") + T(570, 26, "After optimisation", "b", "middle") +
  B(90, 40, 200, 46, "sp", ["Aggregate", "sum(amount) by customer"]) +
  B(90, 116, 200, 46, "sp", ["Filter", "order_date >= 2026-11-01"]) +
  B(90, 192, 200, 60, "ka", ["Scan Parquet", "all 50 columns", "all row groups"]) +
  A("programming3", "M190 192 L190 164") + A("programming3", "M190 116 L190 88") +
  B(470, 40, 200, 46, "sp", ["Aggregate", "sum(amount) by customer"]) +
  B(470, 140, 200, 112, "okf", ["Scan Parquet", "columns: 3 of 50", "filter applied in scan", "skips row groups", "by min/max statistics"]) +
  A("programming3", "M570 140 L570 88") +
  A("programming3", "M300 140 L460 170", "acc dash") + T(380, 140, "pushdown", "sm tA", "middle") +
  T(380, 278, "lf.explain() shows the optimised plan", "sm", "middle")) },

programming_ibis:{ t:"One Ibis expression, many engines", cap:"Ibis code builds an expression tree without touching data. At execution time the chosen backend compiles the tree (through SQLGlot for SQL engines) into its own dialect and runs it where the data lives. Only the result travels back to Python, as pandas, Arrow or Polars.", svg:S("programming4", 290, "Python expression tree compiled to DuckDB, BigQuery and Snowflake SQL, with results returned to Python",
  B(20, 40, 170, 70, "pr", ["Python code", "filter, group_by,", "aggregate"]) +
  B(20, 150, 170, 90, "box", ["Expression tree", "Aggregate", "Filter", "Table orders"]) +
  A("programming4", "M105 110 L105 148") +
  B(240, 110, 140, 60, "sp", ["Compiler", "SQLGlot"]) +
  A("programming4", "M190 180 L238 145") +
  B(440, 30, 150, 50, "ka", ["DuckDB SQL", "laptop, tests"]) +
  B(440, 115, 150, 50, "st", ["BigQuery SQL", "production"]) +
  B(440, 200, 150, 50, "ka", ["Snowflake, Trino", "Spark, Postgres..."]) +
  A("programming4", "M380 130 L438 58") + A("programming4", "M380 140 L438 140") + A("programming4", "M380 150 L438 222") +
  B(620, 100, 120, 80, "okf", ["Result", "pandas, Arrow", "or Polars"]) +
  A("programming4", "M590 140 L618 140", "acc") +
  T(250, 270, "nothing runs until execute(), to_pyarrow() or to_polars()", "sm tR")) },

});
