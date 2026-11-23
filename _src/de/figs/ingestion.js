/* Ingestion: section diagrams */
Object.assign(FIGS, {

ingestion_modes:{ t:"Three ways to copy a table", cap:"A full snapshot rereads the whole table every run, so its cost grows with the table but deletes take care of themselves. Incremental extraction asks only for rows above a watermark, which is cheap but never sees hard deletes. Change data capture reads the database's own log and sees every insert, update and delete in commit order.", svg:S("ingestion1", 290, "A source table copied by full snapshot, incremental watermark extraction and change data capture, with what each costs and misses",
  B(20, 100, 140, 100, "ka", ["Source table", "orders", "+ its change log"]) +
  B(200, 20, 200, 70, "sp", ["Full snapshot", "read every row, replace copy"]) +
  B(420, 20, 320, 70, "box", ["Cost grows with table size", "deletes handled: absent rows vanish"]) +
  B(200, 110, 200, 70, "st", ["Incremental", "WHERE updated_at > watermark"]) +
  B(420, 110, 320, 70, "box", ["Cheap: only changed rows", "misses hard deletes and silent updates"]) +
  B(200, 200, 200, 70, "pr", ["Change data capture", "read the database log"]) +
  B(420, 200, 320, 70, "okf", ["Sees every insert, update, delete", "in commit order; needs log access"]) +
  A("ingestion1", "M160 130 L198 58") + A("ingestion1", "M160 150 L198 145") + A("ingestion1", "M160 170 L198 232") +
  A("ingestion1", "M400 55 L418 55") + A("ingestion1", "M400 145 L418 145") + A("ingestion1", "M400 235 L418 235")) },

ingestion_connect:{ t:"Inside a Kafka Connect cluster", cap:"A source task polls PostgreSQL, single message transforms (SMTs) edit each record, the converter turns it into bytes and the worker's producer sends it to Kafka. A sink runs the same steps in reverse into BigQuery. Configuration, source offsets and task status live in compacted internal topics, so any worker can pick up a failed worker's tasks.", svg:S("ingestion2", 335, "Source pipeline from PostgreSQL into Kafka and sink pipeline from Kafka into BigQuery, with internal topics and the REST API",
  B(20, 50, 120, 60, "ka", ["PostgreSQL", "source system"]) +
  B(20, 150, 120, 60, "okf", ["BigQuery", "sink system"]) +
  '<rect class="fill" x="160" y="20" width="440" height="200" rx="12"/>' + T(380, 38, "Connect cluster: workers sharing a group.id", "b", "middle") +
  B(175, 56, 95, 56, "sp", ["Task", "poll()"]) + B(280, 56, 95, 56, "pr", ["SMTs", "transform"]) +
  B(385, 56, 95, 56, "st", ["Converter", "to bytes"]) + B(490, 56, 95, 56, "box", ["Producer", "send"]) +
  A("ingestion2", "M270 84 L278 84") + A("ingestion2", "M375 84 L383 84") + A("ingestion2", "M480 84 L488 84") +
  T(380, 133, "source path: poll, transform, serialise, send", "sm", "middle") +
  B(490, 150, 95, 56, "box", ["Consumer", "fetch"]) + B(385, 150, 95, 56, "st", ["Converter", "from bytes"]) +
  B(280, 150, 95, 56, "pr", ["SMTs", "transform"]) + B(175, 150, 95, 56, "sp", ["Task", "put()"]) +
  A("ingestion2", "M490 178 L482 178") + A("ingestion2", "M385 178 L377 178") + A("ingestion2", "M280 178 L272 178") +
  B(620, 50, 120, 160, "ka", ["Kafka", "data topics"]) +
  A("ingestion2", "M140 80 L173 84") + A("ingestion2", "M585 84 L618 84") +
  A("ingestion2", "M620 178 L587 178") + A("ingestion2", "M175 178 L142 180") +
  B(20, 245, 120, 50, "pr", ["REST API", "configure, restart"]) +
  B(160, 245, 580, 50, "box", ["Internal topics in Kafka (compacted)", "config.storage.topic · offset.storage.topic · status.storage.topic"]) +
  A("ingestion2", "M80 245 L80 230 L170 230 L170 222", "ln dash") + A("ingestion2", "M380 220 L380 243", "ln dash") +
  T(380, 322, "tasks.max splits the work; sink tasks form one consumer group", "sm", "middle")) },

ingestion_replica:{ t:"Snapshot plus CDC, stitched at one log position", cap:"The snapshot copies every row as of one log position (here LSN 100), and the change stream starts at exactly that position, so there is no gap and no overlap. Every event lands unchanged in an append-only changelog, and a MERGE keeps the latest row per key and drops deleted keys to build the current-state table.", svg:S("ingestion3", 300, "A database log with a snapshot point at LSN 100, a snapshot before it and CDC after it, both landing in a raw changelog that builds a current-state table",
  T(20, 30, "Step 1: one consistent snapshot, then every change after it, from the same log position", "b") +
  A("ingestion3", "M20 75 L740 75") + T(740, 95, "database log (WAL, binlog, redo)", "sm", "end") +
  '<path class="acc" d="M300 55 L300 95"/>' + T(300, 110, "LSN 100: snapshot point", "sm tA", "middle") +
  '<circle class="st" cx="420" cy="75" r="6"/><circle class="st" cx="500" cy="75" r="6"/><circle class="st" cx="580" cy="75" r="6"/><circle class="st" cx="660" cy="75" r="6"/>' +
  T(420, 62, "210", "sm", "middle") + T(500, 62, "220", "sm", "middle") + T(580, 62, "230", "sm", "middle") + T(660, 62, "240", "sm", "middle") +
  B(20, 120, 270, 44, "sp", ["Snapshot", "every row as of LSN 100"]) +
  B(310, 120, 430, 44, "st", ["CDC stream", "changes 210, 220, 230, 240 ... in log order"]) +
  B(160, 200, 250, 60, "ka", ["Step 2: raw changelog", "append-only: op, LSN, timestamps"]) +
  B(470, 200, 270, 60, "okf", ["Step 3: current-state table", "latest row per key, deletes dropped"]) +
  A("ingestion3", "M155 164 L230 198") + A("ingestion3", "M450 164 L370 198") +
  A("ingestion3", "M410 230 L468 230") + T(440, 222, "MERGE", "mono", "middle") +
  T(380, 288, "no gap and no overlap: every change is applied once, in log order", "sm", "middle")) },

ingestion_webhook:{ t:"Acknowledge fast, process later", cap:"The receiver only verifies the signature, publishes the raw body to Pub/Sub and returns 200 within a second or two. Each consumer reads its own subscription, and because senders retry on timeouts and errors, the pipeline deduplicates again on the event ID and reconciles against the sender's API every day.", svg:S("ingestion4", 305, "A sender posts to a Cloud Run receiver that publishes to a Pub/Sub topic with three subscriptions; retries cause duplicates that are removed downstream",
  B(20, 60, 130, 70, "ka", ["Sender", "Stripe, GitHub", "Shopify"]) +
  B(190, 30, 200, 130, "sp", ["Cloud Run receiver", "1. verify signature", "2. publish raw body", "3. return 200 fast"]) +
  A("ingestion4", "M150 80 L188 80") + T(169, 72, "POST", "sm", "middle") +
  A("ingestion4", "M188 112 L152 112", "ln dash") + T(170, 128, "2xx", "sm", "middle") +
  T(20, 182, "timeout or 5xx: the sender retries,", "sm tR") + T(20, 198, "so the same event can arrive twice", "sm tR") +
  B(430, 70, 130, 50, "st", ["Pub/Sub topic", "retain and seek"]) + A("ingestion4", "M390 95 L428 95") +
  B(600, 20, 140, 50, "okf", ["BigQuery sub", "raw events table"]) +
  B(600, 85, 140, 50, "box", ["Processor sub", "parse, enrich"]) +
  B(600, 150, 140, 50, "box", ["Alerts sub", "notify a team"]) +
  A("ingestion4", "M560 85 L598 47") + A("ingestion4", "M560 95 L598 110") + A("ingestion4", "M560 105 L598 172") +
  B(20, 240, 150, 50, "box", ["Daily reconcile", "against the API"]) +
  B(190, 240, 550, 50, "pr", ["Downstream: deduplicate again on event_id", "QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ...) = 1"]) +
  A("ingestion4", "M670 200 L670 238", "ln dash")) },

ingestion_lake:{ t:"Streaming into a lakehouse table", cap:"The streaming writer turns a continuous flow into one commit per micro-batch or checkpoint, and each commit creates a new table snapshot plus a few small files. A separate compaction job rewrites partitions the stream has finished with into large files, and scheduled snapshot expiry and orphan cleanup keep the metadata small.", svg:S("ingestion5", 305, "A Kafka topic feeding a streaming writer that commits snapshots and small files to today's partition, and a compaction job rewriting yesterday's partition into large files",
  B(20, 50, 120, 70, "ka", ["Kafka topic", "orders"]) +
  B(170, 40, 170, 90, "sp", ["Streaming writer", "Spark or Flink", "commit per batch", "or checkpoint"]) +
  A("ingestion5", "M140 85 L168 85") +
  T(380, 30, "Table snapshots: one per commit", "b") +
  B(380, 45, 70, 40, "st", ["snap 41"], false) + B(462, 45, 70, 40, "st", ["snap 42"], false) + B(544, 45, 70, 40, "st", ["snap 43"], false) +
  T(630, 70, "every 1-5 min", "sm") +
  A("ingestion5", "M340 70 L378 65") +
  '<rect class="fill" x="380" y="100" width="360" height="60" rx="10"/>' + T(560, 117, "today: many small files (the stream still writes here)", "sm", "middle") +
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map(function(i){ return '<rect class="box" x="' + (395 + i * 26) + '" y="128" width="18" height="20" rx="3"/>'; }).join("") +
  A("ingestion5", "M340 110 L378 128") +
  B(170, 180, 170, 60, "pr", ["Compaction job", "rewrite_data_files"]) +
  '<rect class="fill" x="380" y="180" width="360" height="60" rx="10"/>' + T(560, 197, "yesterday: rewritten into large files", "sm", "middle") +
  B(395, 204, 160, 28, "okf", ["large file"], false) + B(565, 204, 160, 28, "okf", ["large file"], false) +
  A("ingestion5", "M340 210 L378 210") +
  T(380, 268, "Compact only partitions the stream no longer writes, so commits do not conflict.", "sm", "middle") +
  T(380, 290, "Then expire old snapshots and delete orphan files on a schedule.", "sm", "middle")) },

});
