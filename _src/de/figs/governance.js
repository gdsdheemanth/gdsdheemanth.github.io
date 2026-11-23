/* governance: Data governance, security and compliance, original drawings */
Object.assign(FIGS, {

governance_layers:{ t:"Governance as layers of engineering", cap:"Each layer builds on the one below. Metadata (catalog, lineage, master data) makes data findable; quality makes it trustworthy; access control decides and records who uses it; privacy and compliance govern personal data through its life. Policies written as code and enforced by the platform run through all four layers.", svg:S("governance1", 300, "Four stacked layers: metadata, quality, access and security, privacy and compliance, with policy as code alongside",
  B(20, 20, 560, 56, "okf", ["Privacy and compliance", "PII masking · erasure · consent · retention · sharing · clean rooms"]) +
  B(20, 88, 560, 56, "pr", ["Access and security", "RBAC and ABAC · row filters · column masks · Ranger · audit · zero trust"]) +
  B(20, 156, 560, 56, "st", ["Quality", "rules and tests · observability · circuit breakers · SLOs"]) +
  B(20, 224, 560, 56, "sp", ["Metadata", "catalog and glossary · lineage · DataHub · Unity Catalog · MDM"]) +
  B(610, 20, 130, 260, "ka", ["Policy as code", "templates", "CI checks", "engine-enforced", "policies", "org policies", "continuous scans"]) +
  A("governance1", "M608 150 L584 150")) },

governance_lineage:{ t:"Collecting lineage with OpenLineage", cap:"Orchestrators, engines and transformation tools emit OpenLineage run events (a run, a job, input and output datasets, plus facets such as column lineage). A backend stores them as a graph, which is then used for impact analysis, root-cause analysis and compliance. BigQuery jobs are recorded by Knowledge Catalog without any instrumentation.", svg:S("governance2", 280, "Producers send OpenLineage events to lineage backends, which serve impact, root cause and compliance use cases",
  T(100, 24, "Producers", "b", "middle") +
  B(20, 36, 160, 44, "sp", ["Airflow / Composer", "OpenLineage provider"]) +
  B(20, 90, 160, 44, "sp", ["Spark", "OpenLineage listener"]) +
  B(20, 144, 160, 44, "sp", ["dbt", "dbt-ol wrapper"]) +
  B(20, 198, 160, 44, "box", ["BigQuery jobs", "recorded natively"]) +
  B(250, 90, 140, 96, "pr", ["Run event", "run · job", "inputs · outputs", "facets"]) +
  A("governance2", "M180 58 L248 120") + A("governance2", "M180 112 L248 130") + A("governance2", "M180 166 L248 150") +
  T(380, 24, "Backends", "b", "middle") +
  B(440, 50, 150, 44, "st", ["Marquez", "reference server"]) +
  B(440, 116, 150, 44, "st", ["Knowledge Catalog", "Data Lineage API"]) +
  B(440, 182, 150, 44, "st", ["DataHub", "OpenMetadata"]) +
  A("governance2", "M390 120 L438 74") + A("governance2", "M390 138 L438 138") + A("governance2", "M390 156 L438 202") +
  A("governance2", "M180 220 L300 220 L300 250 L515 250 L515 162", "ln dash") +
  B(620, 50, 120, 44, "okf", ["Impact", "what breaks?"]) +
  B(620, 116, 120, 44, "okf", ["Root cause", "what changed?"]) +
  B(620, 182, 120, 44, "okf", ["Compliance", "where is PII?"]) +
  A("governance2", "M590 72 L618 72") + A("governance2", "M590 138 L618 138") + A("governance2", "M590 204 L618 204")) },

governance_datahub:{ t:"Inside DataHub", cap:"Metadata arrives as change proposals (MCPs) over REST or Kafka. GMS validates them and commits each aspect to the relational primary store, the source of truth, then emits a change log event (MCL). Consumers of the change log update the search and graph indexes and trigger actions, so derived views can always be rebuilt from the primary store.", svg:S("governance3", 300, "Ingestion sends MCPs to GMS, which writes the primary store and emits MCLs consumed by search, graph and actions",
  B(20, 40, 150, 56, "sp", ["Ingestion sources", "recipes and connectors"]) +
  B(20, 120, 150, 56, "sp", ["Python / Java SDK", "pipelines push metadata"]) +
  B(20, 200, 150, 56, "box", ["Frontend", "React + GraphQL"]) +
  B(250, 100, 160, 80, "pr", ["GMS", "metadata service", "REST and GraphQL"]) +
  A("governance3", "M170 68 L248 120") + T(205, 84, "MCP", "sm tP", "middle") +
  A("governance3", "M170 148 L248 140") +
  A("governance3", "M170 228 L248 166") +
  B(250, 222, 160, 56, "st", ["Primary store", "MySQL or PostgreSQL"]) +
  A("governance3", "M330 180 L330 220") + T(340, 204, "commit aspect", "sm") +
  B(460, 110, 110, 60, "ka", ["Kafka", "MCL topic"]) +
  A("governance3", "M410 140 L458 140") + T(434, 132, "MCL", "sm tK", "middle") +
  B(610, 30, 130, 50, "okf", ["Search index", "Elasticsearch"]) +
  B(610, 115, 130, 50, "okf", ["Graph index", "lineage queries"]) +
  B(610, 200, 130, 50, "okf", ["Actions", "alerts, tickets"]) +
  A("governance3", "M570 130 L608 58") + A("governance3", "M570 140 L608 140") + A("governance3", "M570 150 L608 222")) },

governance_access:{ t:"How a data access decision is made", cap:"A request carries an identity. The query engine (the enforcement point) asks the policies (the decision point), which use attributes such as group membership and resource tags. The engine then returns only permitted rows with masked columns, and every decision is written to the audit log.", svg:S("governance4", 280, "Request flows to the query engine, which consults policies and attributes, returns filtered and masked data, and writes an audit log",
  B(20, 90, 140, 70, "sp", ["Request", "user or service", "identity + context"]) +
  B(220, 90, 160, 70, "pr", ["Query engine", "enforcement point", "BigQuery, Trino, Spark"]) +
  A("governance4", "M160 125 L218 125") +
  B(220, 10, 160, 56, "st", ["Policies", "IAM, row policies, tags"]) +
  B(440, 10, 150, 56, "box", ["Attributes", "groups, tags, purpose"]) +
  A("governance4", "M300 88 L300 68", "ln") + A("governance4", "M300 66 L300 86", "ln dash") +
  A("governance4", "M438 38 L382 38") +
  B(440, 90, 150, 70, "okf", ["Result", "rows filtered", "columns masked"]) +
  A("governance4", "M380 125 L438 125") +
  B(620, 90, 120, 70, "ka", ["Consumer", "BI, notebook", "pipeline"]) +
  A("governance4", "M590 125 L618 125") +
  B(220, 200, 370, 56, "fill", ["Audit log", "who, what, when: kept in a locked, separate project"]) +
  A("governance4", "M300 160 L300 198", "ln dash")) },

governance_erasure:{ t:"Erasing a person from an Iceberg table", cap:"A DELETE creates a new snapshot without the person's rows, but the old snapshot still references the original data file, so time travel can still read it. Only after snapshot expiry (and compaction, for merge-on-read tables) is the file physically removed. Crypto-shredding takes a different route: destroying the person's key makes every copy unreadable at once.", svg:S("governance5", 300, "Snapshots before and after a delete, snapshot expiry removing the old file, and a crypto-shredding alternative",
  T(20, 24, "Physical deletion in a table format", "b") +
  B(20, 40, 160, 60, "box", ["Snapshot 1", "file A (has u-123)", "file B"]) +
  B(240, 40, 160, 60, "sp", ["Snapshot 2", "file A' (rewritten)", "file B"]) +
  A("governance5", "M180 70 L238 70") + T(209, 62, "DELETE", "sm", "middle") +
  B(460, 40, 130, 60, "st", ["expire_snapshots", "and remove", "orphan files"]) +
  A("governance5", "M400 70 L458 70") +
  B(620, 40, 120, 60, "okf", ["File A gone", "erasure done"]) +
  A("governance5", "M590 70 L618 70") +
  T(100, 124, "time travel still reads file A here", "sm tR", "middle") +
  A("governance5", "M20 150 L740 150", "ln dash") +
  T(20, 182, "Crypto-shredding", "b") +
  B(20, 200, 160, 60, "ka", ["Per-user key", "u-123 keyset"]) +
  B(240, 200, 220, 60, "box", ["Encrypted copies", "tables, snapshots, backups"]) +
  A("governance5", "M180 230 L238 230") + T(209, 222, "encrypts", "sm", "middle") +
  B(520, 200, 220, 60, "okf", ["Delete the key", "every copy becomes unreadable"]) +
  A("governance5", "M460 230 L518 230", "acc")) },

});
