/* Pipelines and transformation: section diagrams */
Object.assign(FIGS, {

pipelines_etl_elt:{ t:"ETL versus ELT", cap:"In ETL a separate engine transforms data before it reaches the warehouse, which receives only finished tables. In ELT raw data is loaded first and transformed by SQL inside the warehouse, so the raw layer stays available for reruns over history.", svg:S("pipelines1", 290, "Two rows comparing ETL, where a separate engine transforms before loading, with ELT, where raw data is loaded and transformed inside the warehouse",
  T(20, 30, "ETL", "b") +
  B(20, 45, 130, 70, "ka", ["Sources", "DBs, APIs, files"]) +
  B(220, 45, 220, 70, "pr", ["Transform engine", "Spark, Dataflow, Python"]) +
  B(510, 45, 230, 70, "box", ["Warehouse", "finished tables only"]) +
  A("pipelines1", "M150 80 L218 80") + A("pipelines1", "M440 80 L508 80") +
  T(185, 70, "E", "sm", "middle") + T(475, 70, "L", "sm", "middle") +
  T(20, 160, "ELT", "b") +
  B(20, 175, 130, 70, "ka", ["Sources", "DBs, APIs, files"]) +
  B(220, 175, 220, 70, "st", ["Load raw", "connectors, LOAD DATA"]) +
  B(510, 175, 230, 70, "okf", ["Warehouse", "raw, staging, marts (SQL)"]) +
  A("pipelines1", "M150 210 L218 210", "acc") + A("pipelines1", "M440 210 L508 210", "acc") +
  T(185, 200, "E", "sm", "middle") + T(475, 200, "L", "sm", "middle") +
  T(625, 270, "T happens here, raw kept for reruns", "sm", "middle")) },

pipelines_load_patterns:{ t:"What each load pattern rewrites", cap:"Seven daily partitions of a target table, and what one run touches under each pattern. A full refresh rebuilds everything; append adds only the new day; merge updates matching keys wherever they are and inserts new ones; a lookback overwrite replaces the last few days so late events are picked up.", svg:S("pipelines2", 290, "Grid of seven date partitions showing which ones a full refresh, an append, a merge and a three-day lookback overwrite rewrite",
  ["17 Nov", "18 Nov", "19 Nov", "20 Nov", "21 Nov", "22 Nov", "23 Nov"].map(function(d, i){ return T(225 + i * 80, 32, d, "sm", "middle"); }).join("") +
  B(20, 45, 150, 50, "fill", ["Full refresh", "whole table"]) +
  [0, 1, 2, 3, 4, 5, 6].map(function(i){ return B(190 + i * 80, 45, 70, 50, "sp", ["rebuild"], false); }).join("") +
  B(20, 105, 150, 50, "fill", ["Append", "new rows only"]) +
  [0, 1, 2, 3, 4, 5].map(function(i){ return B(190 + i * 80, 105, 70, 50, "box", ["keep"], false); }).join("") +
  B(670, 105, 70, 50, "okf", ["insert"], false) +
  B(20, 165, 150, 50, "fill", ["Merge", "upsert by key"]) +
  ["keep", "keep", "update", "keep", "update", "keep", "insert"].map(function(s, i){ return B(190 + i * 80, 165, 70, 50, s === "keep" ? "box" : (s === "update" ? "pr" : "okf"), [s], false); }).join("") +
  B(20, 225, 150, 50, "fill", ["Lookback", "last 3 days"]) +
  [0, 1, 2, 3].map(function(i){ return B(190 + i * 80, 225, 70, 50, "box", ["keep"], false); }).join("") +
  [4, 5, 6].map(function(i){ return B(190 + i * 80, 225, 70, 50, "sp", ["replace"], false); }).join("")) },

pipelines_dag_patterns:{ t:"Fan-out, fan-in and the medallion layers", cap:"One task lists what arrived and fans out to parallel loads into raw (bronze) tables. The cleaning step fans in, waiting for all loads, and builds staging (silver); marts (gold) are built from it and published in one atomic step.", svg:S("pipelines3", 320, "A DAG where a list task fans out to three loads, which fan in to a cleaning step, then marts and publish, labelled with bronze, silver and gold layers",
  B(20, 130, 110, 50, "box", ["List files", "what arrived"]) +
  B(170, 40, 130, 50, "ka", ["Load orders", "raw table"]) +
  B(170, 130, 130, 50, "ka", ["Load payments", "raw table"]) +
  B(170, 220, 130, 50, "ka", ["Load customers", "raw table"]) +
  B(350, 130, 130, 50, "st", ["Clean and join", "staging"]) +
  B(520, 130, 110, 50, "okf", ["Build marts", "facts, dims"]) +
  B(650, 130, 90, 50, "sp", ["Publish", "atomic"]) +
  A("pipelines3", "M130 150 L168 70") + A("pipelines3", "M130 155 L168 155") + A("pipelines3", "M130 160 L168 240") +
  A("pipelines3", "M300 65 L348 145") + A("pipelines3", "M300 155 L348 155") + A("pipelines3", "M300 245 L348 165") +
  A("pipelines3", "M480 155 L518 155") + A("pipelines3", "M630 155 L648 155") +
  T(235, 26, "fan-out: run in parallel", "sm", "middle") + T(415, 115, "fan-in: wait for all", "sm", "middle") +
  T(235, 300, "bronze: raw, replayable", "tK", "middle") + T(415, 300, "silver: cleaned, tested", "tS", "middle") +
  T(630, 300, "gold: owned, documented", "tA", "middle")) },

pipelines_wap:{ t:"Write, audit, publish", cap:"New data is written where consumers cannot see it, audited with dataset checks, and published in one atomic step only if every check passes. A failed audit blocks publication and alerts, so consumers keep reading the previous good version.", svg:S("pipelines4", 260, "Flow from a new batch to a candidate table, an audit step, and then either an atomic publish or a blocked publish with an alert",
  B(20, 95, 110, 60, "box", ["New batch", "from staging"]) +
  B(160, 95, 140, 60, "ka", ["Write", "candidate or branch"]) +
  B(330, 95, 140, 60, "pr", ["Audit", "keys, volume, sums"]) +
  B(510, 30, 230, 60, "okf", ["Publish (atomic)", "swap, COPY or fast-forward"]) +
  B(510, 160, 230, 60, "sp", ["Block and alert", "previous version stays live"]) +
  A("pipelines4", "M130 125 L158 125") + A("pipelines4", "M300 125 L328 125") +
  A("pipelines4", "M470 115 L508 65", "acc") + A("pipelines4", "M470 135 L508 185", "ln dash") +
  T(480, 78, "pass", "tA", "middle") + T(480, 182, "fail", "sm", "middle") +
  T(250, 230, "consumers only ever read the published table", "sm", "middle")) },

pipelines_dlq:{ t:"Retries, dead letters and replay", cap:"Transient errors are retried with backoff inside the processor. Records that can never succeed go to a dead-letter store with their payload and error, so good records keep flowing. When most records fail at once, a circuit breaker stops the pipeline instead. After a fix, dead letters are replayed into the input.", svg:S("pipelines5", 320, "A processor reading from an input, retrying transient errors, writing good records to a sink and permanent failures to a dead-letter store, with a circuit breaker and a replay path back to the input",
  B(20, 110, 120, 60, "ka", ["Input", "topic or files"]) +
  B(200, 110, 150, 60, "pr", ["Process", "parse, validate"]) +
  B(430, 40, 160, 60, "okf", ["Sink", "BigQuery table"]) +
  B(430, 180, 160, 60, "sp", ["Dead letters", "payload + error"]) +
  B(620, 180, 120, 60, "box", ["Fix cause", "then replay"]) +
  B(200, 215, 150, 50, "st", ["Circuit breaker", "most fail: stop, page"]) +
  A("pipelines5", "M140 140 L198 140") +
  A("pipelines5", "M245 110 C245 60, 305 60, 305 108", "ln dash") +
  T(275, 52, "transient: retry with backoff", "sm", "middle") +
  A("pipelines5", "M350 125 L428 75", "acc") + T(380, 88, "ok", "tA", "middle") +
  A("pipelines5", "M350 155 L428 205") + T(372, 196, "permanent", "sm", "end") +
  A("pipelines5", "M275 170 L275 213", "ln dash") +
  A("pipelines5", "M590 210 L618 210") +
  A("pipelines5", "M680 240 L680 300 L80 300 L80 172", "ln dash") +
  T(500, 292, "replay, idempotent writes", "sm", "middle")) },

});
