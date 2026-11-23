/* Operational databases, AI and governance on Google Cloud: section diagrams */
Object.assign(FIGS, {

gcpdata_bigtable:{ t:"How Bigtable serves a table", cap:"A table is cut into tablets, each a contiguous range of row keys. Nodes serve tablets but the data itself lives in Colossus, so moving a tablet to a less busy node moves only metadata. Keys that start with a timestamp send every new write to the last tablet and its node.", svg:S("gd1", 300, "Clients reach three Bigtable nodes, each serving a key range of tablets stored in Colossus",
  B(20, 105, 140, 60, "box", ["Client", "via app profile"]) +
  A("gd1", "M160 125 L218 65") + A("gd1", "M160 135 L218 135") + A("gd1", "M160 145 L218 205") +
  B(220, 40, 180, 50, "sp", ["Node 1", "dev-0000 to dev-0332"]) +
  B(220, 110, 180, 50, "sp", ["Node 2", "dev-0333 to dev-0665"]) +
  B(220, 180, 180, 50, "sp", ["Node 3", "dev-0666 to dev-0999"]) +
  A("gd1", "M400 65 L478 65", "ln dash") + A("gd1", "M400 135 L478 135", "ln dash") + A("gd1", "M400 205 L478 205", "ln dash") +
  B(480, 40, 260, 190, "st", ["Colossus", "SSTables and commit logs", "for every tablet", "shared by all nodes"]) +
  T(310, 262, "Key device#time: writes spread over all three nodes", "sm", "middle") +
  T(310, 282, "Key time#device: every new write goes to the last tablet (hotspot)", "sm", "middle")) },

gcpdata_spanner:{ t:"Spanner splits and Paxos groups", cap:"Each split is a key range replicated in three zones. One replica of each split is the Paxos leader, which orders that split's writes; the others vote and can serve reads. Leaders are spread across zones, and a transaction that stays inside one split needs only one Paxos group.", svg:S("gd2", 300, "Three splits, each replicated in three zones with one leader per split",
  T(285, 22, "Zone A", "b", "middle") + T(475, 22, "Zone B", "b", "middle") + T(665, 22, "Zone C", "b", "middle") +
  B(20, 35, 160, 50, "st", ["Split 1", "keys 0 to 3fff..."]) +
  B(20, 105, 160, 50, "st", ["Split 2", "keys 4000... to 9fff..."]) +
  B(20, 175, 160, 50, "st", ["Split 3", "keys a000... to ffff..."]) +
  B(200, 35, 170, 50, "ka", ["Leader", "orders writes"]) + B(390, 35, 170, 50, "box", ["Replica", "votes, serves reads"]) + B(580, 35, 160, 50, "box", ["Replica", "votes, serves reads"]) +
  B(200, 105, 170, 50, "box", ["Replica", "votes, serves reads"]) + B(390, 105, 170, 50, "ka", ["Leader", "orders writes"]) + B(580, 105, 160, 50, "box", ["Replica", "votes, serves reads"]) +
  B(200, 175, 170, 50, "box", ["Replica", "votes, serves reads"]) + B(390, 175, 170, 50, "box", ["Replica", "votes, serves reads"]) + B(580, 175, 160, 50, "ka", ["Leader", "orders writes"]) +
  T(380, 255, "Each row is one Paxos group. Interleaved children stay in their parent's split.", "sm", "middle") +
  T(380, 277, "Load-based splitting cuts a busy split in two and moves the halves.", "sm", "middle")) },

gcpdata_catalog:{ t:"Knowledge Catalog as a platform service", cap:"Google Cloud sources are harvested automatically and other systems are imported. Inside the catalog, entries carry aspects and glossary links, lineage connects them, and scans add discovery, profiles and quality results. People, BI tools, alerts and AI agents all read the same metadata.", svg:S("gd3", 320, "Sources feed Knowledge Catalog, which serves people, tools and agents",
  T(90, 20, "Sources", "b", "middle") + T(380, 20, "Knowledge Catalog", "b", "middle") + T(665, 20, "Consumers", "b", "middle") +
  B(20, 35, 140, 44, "box", ["BigQuery", "tables, jobs"]) +
  B(20, 90, 140, 44, "box", ["Cloud Storage", "files in buckets"]) +
  B(20, 145, 140, 44, "box", ["Databases", "Spanner, AlloyDB, SQL"]) +
  B(20, 200, 140, 44, "box", ["Pub/Sub, Bigtable", "models, more"]) +
  B(20, 255, 140, 44, "box", ["Other systems", "metadata import"]) +
  A("gd3", "M160 57 L218 100") + A("gd3", "M160 112 L218 130") + A("gd3", "M160 167 L218 160") + A("gd3", "M160 222 L218 190") + A("gd3", "M160 277 L218 220") +
  B(220, 35, 320, 265, "fill", [""]) +
  B(235, 50, 140, 70, "sp", ["Entries", "aspects, owners", "tiers, contracts"]) +
  B(385, 50, 140, 70, "sp", ["Glossary", "business terms", "linked to columns"]) +
  B(235, 135, 290, 55, "ka", ["Lineage", "BigQuery jobs, Airflow, Spark, OpenLineage"]) +
  B(235, 205, 290, 80, "pr", ["Scans", "discovery: files become tables", "profile: column statistics", "quality: rules and thresholds"]) +
  A("gd3", "M540 75 L598 57") + A("gd3", "M540 120 L598 125") + A("gd3", "M540 200 L598 195") + A("gd3", "M540 255 L598 262") +
  B(600, 35, 140, 44, "okf", ["People", "search, glossary"]) +
  B(600, 103, 140, 44, "okf", ["BI and SQL", "Looker, BigQuery"]) +
  B(600, 173, 140, 44, "okf", ["AI agents", "APIs and MCP"]) +
  B(600, 240, 140, 44, "okf", ["Alerts", "results in BigQuery"])) },

gcpdata_cleanroom:{ t:"BigQuery sharing and a data clean room", cap:"Top: a publisher lists a dataset of views in an exchange, and each subscriber gets a linked dataset in their own project, with no copy. Bottom: in a clean room, contributors share views protected by analysis rules, and a subscriber's join returns only aggregates over enough people.", svg:S("gd4", 330, "Publisher to exchange to subscriber, and contributors to a clean room that returns only aggregates",
  T(20, 20, "BigQuery sharing", "b") +
  B(20, 35, 190, 60, "st", ["Publisher project", "sharing dataset of views", "pays storage"]) + A("gd4", "M210 65 L278 65") +
  B(280, 35, 200, 60, "pr", ["Exchange and listing", "egress controls", "subscriber role"]) + A("gd4", "M480 65 L548 65") +
  B(550, 35, 190, 60, "sp", ["Subscriber project", "linked dataset, no copy", "pays its queries"]) +
  T(20, 140, "Data clean room", "b") +
  B(20, 155, 190, 56, "st", ["Retailer view", "aggregation threshold 50"]) +
  B(20, 230, 190, 56, "st", ["Advertiser data", "exposed users"]) +
  A("gd4", "M210 183 L278 215") + A("gd4", "M210 258 L278 235") +
  B(280, 185, 200, 76, "pr", ["Clean room", "analysis rules, join on", "hashed key, templates"]) + A("gd4", "M480 223 L548 223") +
  B(550, 185, 190, 76, "okf", ["Results", "groups of 50+ users only", "small groups dropped"]) +
  T(380, 310, "SELECT WITH AGGREGATION_THRESHOLD ... GROUP BY product_category", "mono", "middle")) },

gcpdata_refarch:{ t:"Four reference architectures", cap:"Most Google Cloud data platforms combine four standard flows: a batch lakehouse, streaming analytics, CDC replication of operational databases, and an ML platform. They share one cross-cutting layer of projects, identity, perimeter, governance, monitoring and infrastructure as code.", svg:S("gd5", 360, "Four lanes of services for batch, streaming, CDC and ML, above a shared platform layer",
  B(20, 25, 120, 50, "fill", ["Batch", "lakehouse"]) +
  B(160, 25, 130, 50, "box", ["Files, SaaS", "DB exports"]) + A("gd5", "M290 50 L308 50") +
  B(310, 25, 130, 50, "ka", ["Cloud Storage", "landing, raw"]) + A("gd5", "M440 50 L458 50") +
  B(460, 25, 130, 50, "sp", ["Spark, Iceberg", "bronze, silver"]) + A("gd5", "M590 50 L608 50") +
  B(610, 25, 130, 50, "st", ["BigQuery marts", "Dataform, Looker"]) +
  B(20, 95, 120, 50, "fill", ["Streaming", "analytics"]) +
  B(160, 95, 130, 50, "box", ["Apps, devices", "events"]) + A("gd5", "M290 120 L308 120") +
  B(310, 95, 130, 50, "ka", ["Pub/Sub", "or Managed Kafka"]) + A("gd5", "M440 120 L458 120") +
  B(460, 95, 130, 50, "sp", ["Dataflow", "dedupe, windows"]) + A("gd5", "M590 120 L608 120") +
  B(610, 95, 130, 50, "st", ["BigQuery", "and Bigtable"]) +
  B(20, 165, 120, 50, "fill", ["CDC", "replica"]) +
  B(160, 165, 130, 50, "box", ["Cloud SQL", "AlloyDB, Spanner"]) + A("gd5", "M290 190 L308 190") +
  B(310, 165, 130, 50, "ka", ["Datastream", "or change streams"]) + A("gd5", "M440 190 L458 190") +
  B(460, 165, 130, 50, "st", ["BigQuery", "merge or append"]) + A("gd5", "M590 190 L608 190") +
  B(610, 165, 130, 50, "sp", ["Dataform", "history, current"]) +
  B(20, 235, 120, 50, "fill", ["ML", "platform"]) +
  B(160, 235, 130, 50, "st", ["BigQuery", "point-in-time"]) + A("gd5", "M290 260 L308 260") +
  B(310, 235, 130, 50, "sp", ["Pipelines", "train, evaluate"]) + A("gd5", "M440 260 L458 260") +
  B(460, 235, 130, 50, "pr", ["Model Registry", "batch, endpoint"]) + A("gd5", "M590 260 L608 260") +
  B(610, 235, 130, 50, "okf", ["Scores", "Feature Store"]) +
  B(20, 300, 720, 46, "pr", ["Shared layer: projects and IAM, VPC Service Controls, Knowledge Catalog,", "Cloud Monitoring, budgets, Terraform and CI/CD"])) },

});
