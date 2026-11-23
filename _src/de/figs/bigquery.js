/* BigQuery in depth: section diagrams */
Object.assign(FIGS, {

bigquery_arch:{ t:"How BigQuery runs a query", cap:"Storage and compute are separate services joined by a very fast network. The query service plans the query using table metadata, Dremel workers (slots, scheduled by Borg) run each stage in parallel, intermediate results pass through an in-memory shuffle tier, and table data is read as Capacitor column files from Colossus.", svg:S("bq1", 340, "BigQuery architecture: client, query service, Dremel stages, shuffle tier, Jupiter network and Colossus storage",
  B(20, 20, 170, 50, "box", ["Client", "console, bq, API, BI"]) + A("bq1", "M190 45 L248 45") +
  B(250, 20, 230, 50, "pr", ["Query service", "parse, optimise, schedule"]) + A("bq1", "M480 45 L528 45") +
  B(530, 20, 210, 50, "box", ["Metadata", "schemas, block statistics"]) +
  A("bq1", "M365 70 L365 93") +
  '<rect class="fill" x="20" y="95" width="720" height="95" rx="12"/>' + T(380, 115, "Dremel execution: slots (workers) scheduled by Borg", "b", "middle") +
  B(40, 128, 200, 48, "sp", ["Stage 1: read, filter", "many leaf workers"]) +
  B(280, 128, 200, 48, "sp", ["Stage 2: join", "hash-partitioned input"]) +
  B(520, 128, 200, 48, "sp", ["Stage 3: aggregate", "then write the result"]) +
  B(140, 205, 480, 36, "ka", ["In-memory shuffle tier between stages"]) +
  A("bq1", "M160 176 L220 203") + A("bq1", "M330 203 L370 178") + A("bq1", "M410 176 L460 203") + A("bq1", "M560 203 L610 178") +
  T(380, 262, "Jupiter datacenter network: petabit-scale bandwidth between compute and storage", "sm", "middle") +
  B(20, 275, 720, 50, "st", ["Colossus distributed file system", "Capacitor columnar files, replicated across zones in the region"]) +
  A("bq1", "M80 273 L80 178")) },

bigquery_storage:{ t:"The life of stored bytes", cap:"Data that has not been modified for 90 consecutive days drops to the long-term rate automatically. When rows are changed or deleted, the old versions stay queryable for the time travel window (2 to 7 days) and then sit in fail-safe for 7 more days. Under logical billing you never pay for those old versions; under physical billing you do.", svg:S("bq2", 260, "Active storage becomes long-term after 90 days; old versions go through time travel then fail-safe",
  B(20, 30, 170, 56, "box", ["Write or modify", "load, DML, stream"]) + A("bq2", "M190 58 L238 58") +
  B(240, 30, 230, 56, "sp", ["Active storage", "modified in last 90 days"]) + A("bq2", "M470 58 L518 58") + T(494, 50, "90 days", "sm", "middle") +
  B(520, 30, 220, 56, "okf", ["Long-term storage", "about half the price"]) +
  T(20, 130, "When rows are changed or deleted, the old versions are kept:", "b") +
  B(20, 150, 170, 56, "box", ["Old version", "after UPDATE or DELETE"]) + A("bq2", "M190 178 L238 178") +
  B(240, 150, 230, 56, "ka", ["Time travel", "2 to 7 days, self-service"]) + A("bq2", "M470 178 L518 178") +
  B(520, 150, 220, 56, "pr", ["Fail-safe", "7 more days, via support"]) +
  T(380, 238, "Logical billing: time travel and fail-safe bytes are free. Physical billing: they are charged.", "sm", "middle")) },

bigquery_prune:{ t:"Partition pruning, then block pruning", cap:"The date filter removes whole partitions before the query starts, which is why a dry run can report the cost exactly. Inside the remaining partition, rows are sorted into blocks by the clustering column, and block statistics let BigQuery skip every block whose customer range cannot match.", svg:S("bq3", 285, "Three daily partitions; two are pruned and only one clustered block in the middle partition is read",
  T(380, 22, "WHERE event_date = '2026-03-02' AND customer_id BETWEEN 200 AND 299", "b mono", "middle") +
  '<rect class="fill" x="40" y="40" width="180" height="200" rx="12"/>' + T(130, 60, "2026-03-01", "b", "middle") +
  B(50, 72, 160, 34, "box", ["customers 0-99"], false) + B(50, 114, 160, 34, "box", ["100-199"], false) + B(50, 156, 160, 34, "box", ["200-299"], false) + B(50, 198, 160, 34, "box", ["300-399"], false) +
  '<rect class="fill" x="290" y="40" width="180" height="200" rx="12"/>' + T(380, 60, "2026-03-02", "b", "middle") +
  B(300, 72, 160, 34, "box", ["customers 0-99"], false) + B(300, 114, 160, 34, "box", ["100-199"], false) + B(300, 156, 160, 34, "okf", ["200-299  (read)"]) + B(300, 198, 160, 34, "box", ["300-399"], false) +
  '<rect class="fill" x="540" y="40" width="180" height="200" rx="12"/>' + T(630, 60, "2026-03-03", "b", "middle") +
  B(550, 72, 160, 34, "box", ["customers 0-99"], false) + B(550, 114, 160, 34, "box", ["100-199"], false) + B(550, 156, 160, 34, "box", ["200-299"], false) + B(550, 198, 160, 34, "box", ["300-399"], false) +
  T(130, 266, "pruned: wrong partition", "sm", "middle") + T(380, 266, "1 of 4 blocks read", "sm", "middle") + T(630, 266, "pruned: wrong partition", "sm", "middle")) },

bigquery_slots:{ t:"Commitments, reservations and assignments", cap:"Capacity is bought in an administration project. Reservations divide it into pools with baseline slots plus autoscaling headroom; idle baseline slots are lent between reservations and reclaimed within milliseconds. Assignments route a project's, folder's or organization's jobs to a reservation, and anything left unassigned silently uses on-demand pricing.", svg:S("bq4", 300, "An administration project with two reservations, assignments from a folder and a project, and an unassigned project on on-demand",
  B(20, 20, 720, 44, "pr", ["Administration project: Enterprise edition, 500-slot annual commitment"]) +
  A("bq4", "M210 64 L210 98") + A("bq4", "M550 64 L550 98") +
  B(80, 100, 260, 60, "sp", ["Reservation: etl", "baseline 300, autoscale up to +500"]) +
  B(420, 100, 260, 60, "sp", ["Reservation: bi", "baseline 200, autoscale up to +200"]) +
  A("bq4", "M340 130 L418 130", "ln dash") + T(379, 122, "idle slots", "sm", "middle") +
  B(20, 200, 220, 56, "box", ["Folder: data-eng", "QUERY and PIPELINE jobs"]) +
  B(270, 200, 220, 56, "box", ["Project: dashboards", "QUERY jobs"]) +
  B(520, 200, 220, 56, "ka", ["Unassigned project", "falls back to on-demand"]) +
  A("bq4", "M130 198 L190 162") + A("bq4", "M380 198 L500 162") +
  T(380, 284, "Assignments route each project's jobs to a reservation; anything unassigned is billed per TiB scanned.", "sm", "middle")) },

bigquery_lake:{ t:"One copy of Iceberg data, many engines", cap:"Table data stays in your Cloud Storage bucket as Parquet files plus Iceberg metadata. The Lakehouse runtime catalog (formerly BigLake metastore) tracks the current snapshot of each table and vends short-lived credentials, so BigQuery, Spark, Trino and Flink all read and write the same tables without copies.", svg:S("bq5", 300, "BigQuery, Spark and other engines share Iceberg tables in Cloud Storage through the Lakehouse runtime catalog",
  B(20, 20, 220, 50, "sp", ["BigQuery", "SQL, DML, BigQuery ML"]) +
  B(270, 20, 220, 50, "sp", ["Apache Spark", "Managed Service for Apache Spark"]) +
  B(520, 20, 220, 50, "sp", ["Trino, Flink", "other open engines"]) +
  A("bq5", "M150 70 L250 118") + A("bq5", "M380 70 L380 118") + A("bq5", "M630 70 L510 118") +
  B(120, 120, 520, 56, "pr", ["Lakehouse runtime catalog", "Iceberg REST endpoint, commits, credential vending"]) +
  A("bq5", "M380 176 L380 223") +
  A("bq5", "M60 70 L60 223", "ln dash") + T(68, 205, "data files", "sm") +
  B(20, 225, 720, 56, "st", ["Cloud Storage bucket that you own", "Parquet data files plus Iceberg metadata (metadata.json, manifest lists, manifests)"])) },

});
