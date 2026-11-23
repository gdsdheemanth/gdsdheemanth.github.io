/* Pipelines on Google Cloud: section diagrams */
Object.assign(FIGS, {

gcppipes_map:{ t:"Pipeline services on Google Cloud", cap:"Data enters through a transport (Pub/Sub or Kafka for events, Datastream for database changes, Cloud Storage for files), is processed by Dataflow, Spark or small Cloud Run jobs, and lands in BigQuery, where Dataform builds the marts. The control layer underneath decides when each part runs.", svg:S("gp1", 330, "Sources flow through transport and processing services into BigQuery, with a control layer underneath",
  T(90, 18, "Sources", "b", "middle") + T(280, 18, "Transport", "b", "middle") + T(480, 18, "Process", "b", "middle") + T(670, 18, "Store and serve", "b", "middle") +
  B(20, 30, 140, 50, "box", ["Applications", "events, logs"]) + A("gp1", "M160 55 L198 55") +
  B(200, 30, 160, 50, "ka", ["Pub/Sub", "or Managed Kafka"]) + A("gp1", "M360 55 L398 55") +
  B(400, 30, 160, 50, "sp", ["Dataflow", "Beam, stream or batch"]) + A("gp1", "M560 55 L598 55") +
  B(20, 100, 140, 50, "box", ["Databases", "MySQL, PostgreSQL"]) + A("gp1", "M160 125 L198 125") +
  B(200, 100, 160, 50, "ka", ["Datastream", "CDC, backfill"]) + A("gp1", "M360 125 L598 125") + T(480, 118, "direct, merged", "sm", "middle") +
  B(20, 170, 140, 50, "box", ["Files and SaaS", "partners, APIs"]) + A("gp1", "M160 195 L198 195") +
  B(200, 170, 160, 50, "ka", ["Cloud Storage", "landing bucket"]) + A("gp1", "M360 195 L398 195") +
  B(400, 170, 160, 50, "sp", ["Managed Spark", "or Cloud Run jobs"]) + A("gp1", "M560 195 L598 195") +
  B(600, 30, 140, 190, "st", ["BigQuery", "raw, staging, marts", "Dataform builds marts", "Looker reads them"]) +
  B(20, 250, 720, 56, "pr", ["Control: Managed Airflow, Workflows, Cloud Scheduler, Eventarc", "decides when each job runs, retries failures, alerts"]) +
  A("gp1", "M480 248 L480 222", "ln dash") + A("gp1", "M280 248 L280 222", "ln dash")) },

gcppipes_pubsub:{ t:"One topic, many subscriptions", cap:"Every subscription receives its own copy of each message. The subscription type decides who moves the message out: your client (pull), Pub/Sub over HTTPS (push), or Pub/Sub writing straight into BigQuery or Cloud Storage. Any subscription can send messages that keep failing to a dead-letter topic.", svg:S("gp2", 340, "A publisher sends to a topic with four subscriptions of different types and a dead-letter topic",
  B(20, 120, 140, 56, "box", ["Publisher", "key, attributes"]) + A("gp2", "M160 148 L188 148") +
  B(190, 120, 170, 56, "ka", ["Topic: orders", "retention up to 31 days"]) +
  A("gp2", "M360 140 L398 45") + A("gp2", "M360 145 L398 110") + A("gp2", "M360 152 L398 175") + A("gp2", "M360 158 L398 240") +
  B(400, 20, 180, 50, "sp", ["Pull subscription", "ordering, exactly-once"]) + A("gp2", "M580 45 L608 45") +
  B(400, 85, 180, 50, "sp", ["Push subscription", "HTTPS POST with OIDC"]) + A("gp2", "M580 110 L608 110") +
  B(400, 150, 180, 50, "sp", ["BigQuery subscription", "Storage Write API"]) + A("gp2", "M580 175 L608 175") +
  B(400, 215, 180, 50, "sp", ["Cloud Storage sub", "files by time or size"]) + A("gp2", "M580 240 L608 240") +
  B(610, 20, 130, 50, "box", ["Dataflow, GKE", "your client acks"]) +
  B(610, 85, 130, 50, "box", ["Cloud Run", "2xx means ack"]) +
  B(610, 150, 130, 50, "st", ["BigQuery table", "rows, metadata"]) +
  B(610, 215, 130, 50, "st", ["Bucket", "text or Avro"]) +
  A("gp2", "M490 265 L490 283", "ln dash") +
  B(400, 285, 180, 44, "pr", ["Dead-letter topic", "after N failed attempts"]) +
  T(20, 300, "Unacked messages are redelivered", "sm") + T(20, 318, "after the ack deadline (at least once)", "sm")) },

gcppipes_dataflow:{ t:"Inside a Dataflow job", cap:"The service turns your Beam code into an optimised graph and starts worker VMs in your project. Shuffle and state live in a Google backend (Streaming Engine for streams, Dataflow Shuffle for batch), so workers stay small and the autoscaler can add or remove them quickly based on backlog and CPU.", svg:S("gp3", 300, "Beam pipeline submitted to the Dataflow service, worker VMs, autoscaler and the shuffle and state backend",
  B(20, 20, 170, 56, "box", ["Beam pipeline", "Python, Java, Go"]) + A("gp3", "M190 48 L238 48") + T(214, 40, "submit", "sm", "middle") +
  B(240, 20, 250, 56, "pr", ["Dataflow service", "fusion, combiner lifting, launch"]) +
  B(540, 20, 200, 56, "ka", ["Autoscaler", "backlog, CPU, throughput"]) + A("gp3", "M540 48 L492 48", "ln dash") +
  A("gp3", "M365 76 L365 108") + A("gp3", "M640 76 L640 108") + T(648, 96, "add or remove", "sm") +
  '<rect class="fill" x="20" y="110" width="720" height="86" rx="12"/>' + T(380, 128, "Worker VMs in your project (Runner v2: SDK container next to the worker)", "b", "middle") +
  B(50, 138, 190, 46, "sp", ["Worker 1", "fused stages"]) + B(285, 138, 190, 46, "sp", ["Worker 2", "fused stages"]) + B(520, 138, 190, 46, "sp", ["Worker N", "fused stages"]) +
  A("gp3", "M145 198 L145 226") + A("gp3", "M380 226 L380 198") + A("gp3", "M615 198 L615 226") +
  B(20, 228, 720, 56, "st", ["Streaming Engine (streaming) or Dataflow Shuffle (batch)", "shuffle and window state kept in Google's backend, not on worker disks"])) },

gcppipes_datastream:{ t:"Datastream from PostgreSQL to BigQuery", cap:"A stream snapshots existing rows (backfill) while it reads new changes from the log (CDC). Changes are written through the Storage Write API and BigQuery merges them into a current replica within the table's freshness bound. Writing files to Cloud Storage instead lets Dataflow or Spark apply custom logic.", svg:S("gp4", 270, "PostgreSQL feeding a Datastream stream with backfill and CDC into BigQuery or Cloud Storage",
  B(20, 60, 160, 64, "box", ["PostgreSQL", "tables and WAL", "slot + publication"]) +
  '<rect class="fill" x="220" y="10" width="240" height="160" rx="12"/>' + T(340, 30, "Datastream stream", "b", "middle") +
  A("gp4", "M180 80 L238 65") + A("gp4", "M180 105 L238 125") +
  B(240, 42, 200, 46, "sp", ["Backfill", "snapshot in chunks"]) +
  B(240, 102, 200, 46, "sp", ["CDC", "logical decoding"]) +
  A("gp4", "M460 80 L498 45") +
  B(500, 20, 240, 50, "ka", ["Storage Write API", "upserts and deletes by key"]) + A("gp4", "M620 70 L620 98") +
  B(500, 100, 240, 60, "st", ["BigQuery replica", "merged within max_staleness"]) +
  A("gp4", "M340 170 L340 193", "ln dash") + T(350, 186, "or", "sm") +
  B(240, 195, 200, 50, "box", ["Cloud Storage", "Avro or JSON files"]) + A("gp4", "M440 220 L498 220") +
  B(500, 195, 240, 50, "pr", ["Dataflow template or Spark", "custom merge, Iceberg"])) },

gcppipes_events:{ t:"Event-driven and scheduled triggers", cap:"Eventarc turns a file landing into a CloudEvent that starts a workflow execution; Cloud Scheduler starts the same kind of execution on a cron schedule. Workflows calls BigQuery and Pub/Sub through connectors, waits for long jobs and retries failed steps.", svg:S("gp5", 260, "Cloud Storage event through Eventarc and a Cloud Scheduler cron job both starting a Workflows execution",
  B(20, 30, 150, 56, "box", ["Cloud Storage", "object finalized"]) + A("gp5", "M170 58 L208 58") +
  B(210, 30, 150, 56, "ka", ["Eventarc trigger", "CloudEvents"]) + A("gp5", "M360 58 L398 58") +
  '<rect class="fill" x="400" y="10" width="340" height="160" rx="12"/>' + T(570, 30, "Workflows execution", "b", "middle") +
  B(415, 45, 145, 50, "sp", ["Load job", "BigQuery connector"]) + A("gp5", "M560 70 L578 70") +
  B(580, 45, 145, 50, "sp", ["Publish", "Pub/Sub connector"]) +
  B(415, 110, 310, 46, "okf", ["Retries with backoff, try / except", "waits on jobs without cost"]) +
  B(20, 180, 150, 56, "box", ["Cloud Scheduler", "cron, time zone"]) + A("gp5", "M170 208 L208 208") +
  B(210, 180, 150, 56, "pr", ["HTTP call", "OAuth token"]) + A("gp5", "M360 200 L460 172") +
  T(400, 222, "or a Cloud Run job, an HTTP endpoint, a Pub/Sub topic", "sm")) },

});
