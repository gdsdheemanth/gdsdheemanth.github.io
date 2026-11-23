/* design: Data engineering system design, original drawings */
Object.assign(FIGS, {

design_platform:{ t:"The zone architecture of a data platform", cap:"Data flows left to right through zones that talk only through durable interfaces (topics, files, tables), so each can fail and recover on its own. The landing zone is immutable, which lets every later zone be rebuilt. Governance and operations cut across every zone rather than sitting at the end.", svg:S("design1", 300, "Six zones from sources to consumers with a governance band underneath",
  B(8, 40, 115, 130, "sp", ["Sources", "AlloyDB, Spanner", "SaaS apps", "app events", "partner files"]) +
  B(130, 40, 115, 130, "ka", ["Ingest", "Datastream", "Pub/Sub, Kafka", "transfers", "connectors"]) +
  B(252, 40, 115, 130, "st", ["Land", "raw / bronze", "immutable", "Cloud Storage", "raw datasets"]) +
  B(374, 40, 115, 130, "st", ["Process", "silver, gold", "BigQuery SQL", "Dataflow", "Spark"]) +
  B(496, 40, 115, 130, "pr", ["Serve", "BigQuery", "Bigtable, AlloyDB", "sharing", "topics"]) +
  B(618, 40, 134, 130, "okf", ["Consume", "Looker", "Data Studio", "apps, APIs", "ML models"]) +
  A("design1", "M123 105 L128 105") + A("design1", "M245 105 L250 105") + A("design1", "M367 105 L372 105") +
  A("design1", "M489 105 L494 105") + A("design1", "M611 105 L616 105") +
  '<path class="ln dash" d="M65 172 L65 208"/><path class="ln dash" d="M187 172 L187 208"/><path class="ln dash" d="M309 172 L309 208"/>' +
  '<path class="ln dash" d="M431 172 L431 208"/><path class="ln dash" d="M553 172 L553 208"/><path class="ln dash" d="M685 172 L685 208"/>' +
  B(8, 210, 744, 50, "fill", ["Governance and operations (every zone)", "Knowledge Catalog, IAM and VPC Service Controls, Managed Airflow, Cloud Monitoring, budgets"]) +
  T(380, 25, "compute attaches to storage and scales per job", "sm", "middle") +
  T(380, 285, "zones exchange data only through topics, files and tables", "sm", "middle")) },

design_medallion:{ t:"Medallion layers with quality gates", cap:"Each arrow between layers passes a gate. Rows failing the bronze-to-silver gate go to a quarantine table with their reason; a failing silver-to-gold gate blocks publication so gold keeps its last good version. Bronze stays complete, so any layer can be rebuilt.", svg:S("design2", 290, "Bronze, silver and gold boxes with two gates between them, a quarantine table and a blocked publish path",
  B(20, 40, 150, 80, "sp", ["Bronze", "as received", "+ ingest metadata"]) +
  B(185, 40, 105, 80, "pr", ["Gate 1", "types, keys", "nulls, ranges"]) +
  B(305, 40, 150, 80, "st", ["Silver", "typed, deduped", "conformed keys"]) +
  B(470, 40, 105, 80, "pr", ["Gate 2", "reconcile, RI", "freshness"]) +
  B(590, 40, 150, 80, "okf", ["Gold", "facts, marts", "SLA, documented"]) +
  A("design2", "M170 80 L183 80") + A("design2", "M290 80 L303 80") + A("design2", "M455 80 L468 80") + A("design2", "M575 80 L588 80") +
  A("design2", "M237 120 L237 168", "ln dash") + A("design2", "M522 120 L522 168", "ln dash") +
  B(165, 170, 145, 55, "box", ["Quarantine", "rows + reason"]) +
  B(450, 170, 145, 55, "box", ["Publish blocked", "gold keeps last good"]) +
  T(237, 150, "fail", "sm tR", "start") + T(530, 150, "fail", "sm tR", "start") +
  T(380, 260, "gold recomputes a lookback window for late data and publishes atomically (write-audit-publish)", "sm", "middle")) },

design_realtime:{ t:"Hot path, cold path and query routing", cap:"Events feed two paths. The hot path keeps a few hours of pre-aggregated data for fast queries; the cold path keeps full history and rollups and is the source of truth. A router sends each query to the right store by time range and latency class, splitting queries that span both.", svg:S("design3", 320, "Events splitting into a hot path to a hot store and a cold path to rollups, both feeding a query router",
  B(15, 130, 110, 60, "ka", ["Events", "Pub/Sub"]) +
  B(175, 40, 200, 70, "sp", ["Hot path", "Dataflow: windows, dedupe"]) +
  B(175, 210, 200, 70, "st", ["Cold path", "BigQuery raw events"]) +
  B(420, 40, 160, 70, "ka", ["Hot store", "Bigtable or Pinot", "last 2 hours"]) +
  B(420, 210, 160, 70, "st", ["Rollups", "minute, hour, day"]) +
  B(625, 120, 125, 80, "pr", ["Query router", "by time range", "and latency"]) +
  A("design3", "M125 150 L173 80") + A("design3", "M125 170 L173 240") +
  A("design3", "M375 75 L418 75") + A("design3", "M375 245 L418 245") +
  A("design3", "M580 80 L640 118") + A("design3", "M580 240 L640 202") +
  T(275, 28, "seconds old, millisecond queries", "sm", "middle") +
  T(275, 302, "complete history, recomputable, source of truth", "sm", "middle") +
  T(687, 225, "dashboards, APIs", "sm", "middle")) },

design_featurestore:{ t:"Feature store: one definition, two stores", cap:"Batch and streaming pipelines compute features from one registered definition. The offline store keeps full history for point-in-time correct training sets; the online store keeps the latest value per entity for millisecond lookups and is synced from the offline store or written by the stream.", svg:S("design4", 310, "Raw data feeding batch and streaming feature pipelines, an offline and an online store, training and inference",
  B(15, 120, 120, 70, "sp", ["Raw data", "silver tables", "Pub/Sub"]) +
  B(180, 40, 170, 60, "st", ["Batch features", "BigQuery SQL"]) +
  B(180, 210, 170, 60, "st", ["Streaming features", "Dataflow"]) +
  B(400, 40, 160, 60, "fill", ["Offline store", "BigQuery history"]) +
  B(400, 210, 160, 60, "ka", ["Online store", "Bigtable, latest"]) +
  B(600, 40, 145, 60, "pr", ["Training", "point-in-time join"]) +
  B(600, 210, 145, 60, "okf", ["Inference", "lookup under 10 ms"]) +
  A("design4", "M135 140 L178 75") + A("design4", "M135 170 L178 235") +
  A("design4", "M350 70 L398 70") + A("design4", "M350 240 L398 240") + A("design4", "M350 225 L398 90", "ln dash") +
  A("design4", "M480 100 L480 208") + T(490, 160, "sync", "sm", "start") +
  A("design4", "M560 70 L598 70") + A("design4", "M560 240 L598 240") +
  T(380, 297, "one definition in the registry feeds both stores, so training and serving match", "sm", "middle")) },

design_interview:{ t:"A seven-step framework for design interviews", cap:"Spend the first third of the time on requirements, estimates and the data model before drawing services. The deep dive and the failure discussion are where most of the marks are won or lost.", svg:S("design5", 190, "Seven steps in a row from clarify requirements to cost and trade-offs, with time budgets",
  B(8, 50, 100, 80, "sp", ["1 Clarify", "requirements", "5 min"]) +
  B(116, 50, 100, 80, "sp", ["2 Estimate", "events/s, TB", "5 min"]) +
  B(224, 50, 100, 80, "sp", ["3 Model", "schemas, grain", "5 min"]) +
  B(332, 50, 100, 80, "st", ["4 Sketch", "zones, tools", "10 min"]) +
  B(440, 50, 100, 80, "ka", ["5 Deep dive", "2-3 hard parts", "10 min"]) +
  B(548, 50, 100, 80, "pr", ["6 Failures", "replay, SLOs", "5 min"]) +
  B(656, 50, 100, 80, "okf", ["7 Cost", "trade-offs", "5 min"]) +
  A("design5", "M108 90 L115 90") + A("design5", "M216 90 L223 90") + A("design5", "M324 90 L331 90") +
  A("design5", "M432 90 L439 90") + A("design5", "M540 90 L547 90") + A("design5", "M648 90 L655 90") +
  T(166, 35, "before any service is named", "sm", "middle") +
  T(380, 165, "say each step out loud; the interviewer may steer you into any deep dive", "sm", "middle")) },

});
