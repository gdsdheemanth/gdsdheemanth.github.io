/* engines: Flink, Beam, Trino and other engines, original drawings */
Object.assign(FIGS, {

engines_landscape:{ t:"Where the engines sit", cap:"Sources on the left, engines in the middle, consumers on the right. A Beam pipeline is written once and executed by a runner (Dataflow, Flink or Spark). Flink and Spark move data from sources to sinks; Trino owns no storage and answers interactive queries by reading the sources through connectors.", svg:S("engines1", 300, "Sources feed Beam runners, Flink, Spark and Trino, which feed sinks and analysts",
  T(20, 16, "Sources", "sm") +
  B(20, 25, 150, 60, "ka", ["Event logs", "Kafka, Pub/Sub"]) +
  B(20, 115, 150, 60, "st", ["Databases", "CDC, JDBC"]) +
  B(20, 205, 150, 60, "st", ["Lakehouse", "Cloud Storage, Iceberg"]) +
  B(240, 20, 300, 48, "pr", ["Beam pipeline (SDK)", "written once, run by a runner"]) +
  B(240, 100, 92, 62, "sp", ["Dataflow", "managed"]) +
  B(344, 100, 92, 62, "sp", ["Flink", "continuous"]) +
  B(448, 100, 92, 62, "sp", ["Spark", "stages"]) +
  A("engines1", "M286 68 L286 98", "dash") + A("engines1", "M390 68 L390 98", "dash") + A("engines1", "M494 68 L494 98", "dash") +
  B(240, 200, 300, 66, "fill", ["Trino", "interactive SQL, no storage", "reads sources via connectors"]) +
  A("engines1", "M170 55 L236 120") + A("engines1", "M170 145 L236 135") + A("engines1", "M170 235 L236 150") +
  A("engines1", "M170 150 L236 225") + A("engines1", "M170 240 L236 240") +
  T(600, 16, "Consumers", "sm") +
  B(600, 90, 140, 80, "okf", ["Sinks", "Kafka, BigQuery,", "Iceberg tables"]) +
  B(600, 200, 140, 66, "okf", ["Analysts, BI", "query results"]) +
  A("engines1", "M540 131 L596 131") + A("engines1", "M540 233 L596 233")) },

engines_flink_arch:{ t:"Flink runtime: JobManager, TaskManagers and slots", cap:"The client submits a job graph. The JobManager's Dispatcher starts a JobMaster for the job, the ResourceManager obtains slots from Kubernetes or YARN, and the JobMaster deploys subtasks into the slots and coordinates checkpoints. With slot sharing, each slot holds one slice of the whole pipeline; keyed data crosses between TaskManagers, and every TaskManager writes its state snapshots to Cloud Storage.", svg:S("engines2", 345, "Client, JobManager with Dispatcher, ResourceManager and JobMaster, two TaskManagers with two slots each, and checkpoint storage",
  B(20, 45, 120, 60, "pr", ["Client", "builds JobGraph"]) +
  A("engines2", "M140 75 L178 75", "acc") +
  '<rect class="fill" x="180" y="20" width="380" height="110" rx="8"/>' + T(192, 38, "JobManager", "b") +
  B(192, 50, 110, 66, "ka", ["Dispatcher", "REST, web UI"]) +
  B(312, 50, 120, 66, "ka", ["ResourceManager", "slots"]) +
  B(442, 50, 108, 66, "ka", ["JobMaster", "schedules,", "checkpoints"]) +
  B(600, 45, 140, 60, "box", ["Kubernetes", "or YARN"]) +
  A("engines2", "M560 75 L598 75") +
  '<rect class="fill" x="180" y="170" width="270" height="110" rx="8"/>' + T(192, 188, "TaskManager 1", "b") +
  B(192, 198, 120, 70, "sp", ["Slot 1", "source, map,", "window, sink"]) +
  B(320, 198, 120, 70, "sp", ["Slot 2", "source, map,", "window, sink"]) +
  '<rect class="fill" x="470" y="170" width="270" height="110" rx="8"/>' + T(482, 188, "TaskManager 2", "b") +
  B(482, 198, 120, 70, "sp", ["Slot 3", "source, map,", "window, sink"]) +
  B(610, 198, 120, 70, "sp", ["Slot 4", "source, map,", "window, sink"]) +
  A("engines2", "M480 116 L330 168", "acc") + A("engines2", "M510 116 L600 168", "acc") +
  T(590, 150, "deploy subtasks", "sm tA") +
  A("engines2", "M440 222 L480 222", "kac") + A("engines2", "M482 244 L442 244", "kac") +
  T(460, 300, "keyBy exchange", "sm", "middle") +
  B(180, 308, 560, 30, "st", ["Cloud Storage: checkpoints and savepoints"]) +
  A("engines2", "M260 280 L260 306", "dash") + A("engines2", "M640 280 L640 306", "dash")) },

engines_watermarks:{ t:"Watermark propagation and idle inputs", cap:"Each source partition tracks its own watermark. The window operator takes the minimum over its inputs, so the silent partition p2 holds the whole job at 6 and no window fires. With idleness detection, p2 is excluded after a timeout and the watermark advances to 50.", svg:S("engines3", 270, "Three partitions with watermarks 66, 50 and 6 feed a window operator whose watermark is the minimum",
  B(20, 25, 160, 56, "ka", ["Partition p0", "watermark 66"]) +
  B(20, 105, 160, 56, "ka", ["Partition p1", "watermark 50"]) +
  B(20, 185, 160, 56, "box", ["Partition p2", "silent, watermark 6"]) +
  A("engines3", "M180 53 L318 118") + A("engines3", "M180 133 L318 133") + A("engines3", "M180 213 L318 148", "dash") +
  B(320, 98, 180, 70, "sp", ["Window operator", "min(66, 50, 6) = 6"]) +
  A("engines3", "M500 118 L568 70") + A("engines3", "M500 148 L568 196", "acc") +
  B(570, 35, 170, 62, "box", ["Without idleness", "watermark stuck at 6"]) +
  B(570, 170, 170, 62, "okf", ["With idleness", "p2 ignored: 50"]) +
  T(250, 255, "minimum rule: an operator is only as far along as its slowest input", "sm", "middle")) },

engines_trino:{ t:"Trino: coordinator, workers and connectors", cap:"The coordinator plans the query with metadata from the catalogs and schedules stages on the workers. Workers read splits from each source through its connector, exchange pages with each other in memory, and stream the result back through the coordinator to the client.", svg:S("engines4", 335, "Client, coordinator, metastore, three workers exchanging pages, and three sources read through connectors",
  B(20, 40, 120, 60, "pr", ["Client", "CLI, JDBC, BI"]) +
  A("engines4", "M140 70 L188 70", "acc") +
  B(190, 25, 310, 90, "ka", ["Coordinator", "parse, analyse, plan, optimise", "schedule stages, track memory"]) +
  B(560, 25, 180, 90, "box", ["Catalog metadata", "metastore, schemas,", "statistics"]) +
  A("engines4", "M500 70 L558 70") +
  B(190, 160, 95, 66, "sp", ["Worker 1", "tasks"]) +
  B(297, 160, 95, 66, "sp", ["Worker 2", "tasks"]) +
  B(404, 160, 95, 66, "sp", ["Worker 3", "tasks"]) +
  A("engines4", "M237 115 L237 158") + A("engines4", "M344 115 L344 158") + A("engines4", "M451 115 L451 158") +
  A("engines4", "M285 193 L295 193", "kac") + A("engines4", "M392 193 L402 193", "kac") +
  T(520, 190, "pages exchanged", "sm") + T(520, 206, "in memory", "sm") +
  B(100, 268, 180, 56, "st", ["Cloud Storage", "Iceberg, Parquet"]) +
  B(300, 268, 160, 56, "st", ["PostgreSQL", "JDBC connector"]) +
  B(480, 268, 180, 56, "st", ["BigQuery", "Storage Read API"]) +
  A("engines4", "M200 266 L230 228", "stc") + A("engines4", "M380 266 L344 228", "stc") + A("engines4", "M560 266 L460 228", "stc") +
  T(680, 300, "splits", "sm")) },

engines_pushdown:{ t:"Pushdown in a federated query", cap:"For SELECT country, SUM(amount) ... WHERE day = 7 GROUP BY country, a connector without pushdown ships all 100,000 rows to the engine, which filters and aggregates them. With filter and aggregation pushed down, the source does that work and ships 4 rows. The answer is identical; the network transfer is not.", svg:S("engines5", 230, "Two lanes: without pushdown 100,000 rows cross the network, with pushdown 4 rows do",
  T(20, 18, "No pushdown", "b") +
  B(20, 30, 170, 60, "st", ["PostgreSQL", "returns every row"]) +
  A("engines5", "M190 60 L428 60", "acc") + T(309, 50, "100,000 rows over the network", "sm tA", "middle") +
  B(430, 30, 170, 60, "sp", ["Engine", "filter, group by"]) +
  A("engines5", "M600 60 L638 60") +
  B(640, 30, 100, 60, "okf", ["Result", "4 rows"]) +
  T(20, 133, "Filter and aggregation pushed down", "b") +
  B(20, 145, 170, 60, "st", ["PostgreSQL", "WHERE, GROUP BY"]) +
  A("engines5", "M190 175 L428 175") + T(309, 165, "4 rows", "sm", "middle") +
  B(430, 145, 170, 60, "sp", ["Engine", "returns rows"]) +
  A("engines5", "M600 175 L638 175") +
  B(640, 145, 100, 60, "okf", ["Result", "4 rows"])) },

});
