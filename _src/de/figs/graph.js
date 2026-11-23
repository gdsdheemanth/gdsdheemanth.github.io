/* graph: Graph data engineering, original drawings */
Object.assign(FIGS, {

graph_twoworkloads:{ t:"Two graph workloads", cap:"Graph queries start from a few known nodes and explore their neighbourhood in milliseconds; graph analytics read every edge many times to compute something about the whole graph. They need different systems, often fed from the same node and edge tables.", svg:S("graph1", 270, "Graph queries served by Neo4j, TigerGraph and Spanner Graph; graph analytics run on GraphFrames, Neo4j GDS and BigQuery; both fed by node and edge tables",
  B(20, 20, 340, 70, "sp", ["Graph queries", "start from a few nodes, explore nearby", "milliseconds, tiny part of the graph"]) +
  B(400, 20, 340, 70, "ka", ["Graph analytics", "read every edge, many times", "minutes to hours, whole graph"]) +
  B(20, 125, 105, 60, "st", ["Neo4j", "AuraDB"]) +
  B(137, 125, 105, 60, "st", ["TigerGraph", "GSQL"]) +
  B(254, 125, 106, 60, "st", ["Spanner", "Graph"]) +
  B(400, 125, 105, 60, "pr", ["GraphFrames", "on Spark"]) +
  B(517, 125, 105, 60, "pr", ["Neo4j GDS", "in memory"]) +
  B(634, 125, 106, 60, "pr", ["BigQuery", "Graph, SQL"]) +
  A("graph1", "M190 90 L190 123") + A("graph1", "M570 90 L570 123") +
  B(170, 220, 420, 40, "okf", ["Node and edge tables in BigQuery or Iceberg"]) +
  A("graph1", "M300 220 L200 187") + A("graph1", "M460 220 L560 187")) },

graph_records:{ t:"Index-free adjacency in the record store", cap:"Node and relationship records have fixed sizes, so a record is found by multiplying its id by the record size. Each node points to its first relationship, and each relationship points to the next one in the chain of both of its endpoints, so expanding a node is pointer chasing with no index lookup.", svg:S("graph2", 270, "Four node records and four relationship records; Alice points to rel 2, which links to rel 1, which links to rel 0",
  T(20, 28, "Node store: offset = id x record size", "b") +
  B(20, 40, 100, 50, "sp", ["node 0", "Alice"]) +
  B(130, 40, 100, 50, "sp", ["node 1", "Bob"]) +
  B(240, 40, 100, 50, "sp", ["node 2", "Carol"]) +
  B(350, 40, 100, 50, "sp", ["node 3", "Dave"]) +
  B(480, 40, 260, 50, "fill", ["Block format (5.22+)", "node, properties, rels together"]) +
  T(20, 143, "Relationship store: each record sits in two chains", "b") +
  B(20, 155, 130, 60, "st", ["rel 0", "0 to 1"]) +
  B(170, 155, 130, 60, "st", ["rel 1", "0 to 2"]) +
  B(320, 155, 130, 60, "st", ["rel 2", "3 to 0"]) +
  B(470, 155, 130, 60, "st", ["rel 3", "1 to 2"]) +
  A("graph2", "M70 90 L380 153", "acc") +
  A("graph2", "M385 215 Q310 250 240 218", "acc") +
  A("graph2", "M235 215 Q160 250 90 218", "acc") +
  T(620, 240, "Alice's chain: rel 2, rel 1, rel 0", "sm", "middle") +
  T(600, 125, "Alice: first relationship = rel 2", "sm", "middle")) },

graph_pregel:{ t:"Supersteps in the vertex-centric model", cap:"Single-source shortest paths from A, as in the simulation. In each superstep only vertices that received messages compute; improved distances are sent to neighbours and delivered after the barrier. The job ends when no messages are left.", svg:S("graph3", 250, "Four supersteps separated by barriers, with distances improving until no messages are sent",
  B(20, 40, 160, 100, "sp", ["Superstep 0", "A = 0", "sends 2 messages"]) +
  B(205, 40, 160, 100, "sp", ["Superstep 1", "B = 4, C = 1", "sends 3 messages"]) +
  B(390, 40, 160, 100, "sp", ["Superstep 2", "B = 3, D = 5", "sends 1 message"]) +
  B(575, 40, 165, 100, "okf", ["Superstep 3", "D = 4", "no messages: halt"]) +
  '<path class="ln dash" d="M192 30 L192 150"/>' + '<path class="ln dash" d="M377 30 L377 150"/>' + '<path class="ln dash" d="M562 30 L562 150"/>' +
  T(192, 168, "barrier", "sm", "middle") + T(377, 168, "barrier", "sm", "middle") + T(562, 168, "barrier", "sm", "middle") +
  B(20, 190, 220, 44, "fill", ["compute(messages)"]) +
  B(270, 190, 220, 44, "fill", ["send to neighbours"]) +
  B(520, 190, 220, 44, "fill", ["vote to halt"]) +
  A("graph3", "M240 212 L268 212") + A("graph3", "M490 212 L518 212")) },

graph_kgpipe:{ t:"A knowledge graph construction pipeline", cap:"Each stage writes a staging table, so steps are idempotent and can be re-run. Every fact keeps its source, run and confidence, and only new or changed documents are processed on each run.", svg:S("graph4", 240, "Seven stages from ingest to embeddings, above a row of BigQuery staging tables, with an incremental loop",
  B(8, 50, 96, 80, "ka", ["Ingest", "docs, tables"]) +
  B(116, 50, 96, 80, "sp", ["Extract", "entities,", "relations"]) +
  B(224, 50, 96, 80, "sp", ["Resolve", "block, match,", "cluster"]) +
  B(332, 50, 96, 80, "sp", ["Link", "Wikidata, LEI", "master ids"]) +
  B(440, 50, 96, 80, "pr", ["Validate", "SHACL, SQL", "quarantine"]) +
  B(548, 50, 96, 80, "st", ["Load", "graph store,", "provenance"]) +
  B(656, 50, 96, 80, "okf", ["Embed", "TransE,", "node vectors"]) +
  A("graph4", "M104 90 L114 90") + A("graph4", "M212 90 L222 90") + A("graph4", "M320 90 L330 90") +
  A("graph4", "M428 90 L438 90") + A("graph4", "M536 90 L546 90") + A("graph4", "M644 90 L654 90") +
  A("graph4", "M596 50 Q380 0 164 48", "ln dash") +
  T(380, 22, "incremental: only new or changed documents", "sm", "middle") +
  B(40, 170, 680, 50, "fill", ["BigQuery staging tables", "mentions, entity_map, entity_xref, facts (source, run, confidence)"])) },

graph_fraud:{ t:"The data side of fraud scoring", cap:"Streaming velocity features and nightly identity-graph features meet in the online store, which the scoring service reads within the request; graph lookups add a few capped hops. Served features are logged, so training joins late-arriving labels to the values the model actually saw.", svg:S("graph5", 290, "Events feed stream features into an online store read by the scoring service; an offline log and identity graph job feed back; labels join the offline log",
  B(10, 40, 130, 70, "ka", ["Events", "payments, logins", "Pub/Sub / Kafka"]) +
  B(180, 40, 150, 70, "sp", ["Stream features", "velocity windows", "Dataflow / Flink"]) +
  B(370, 40, 150, 70, "st", ["Online store", "Bigtable", "features by key"]) +
  B(570, 40, 180, 70, "pr", ["Scoring service", "rules + model", "timeout, fallback"]) +
  B(10, 170, 130, 70, "ka", ["Labels", "chargebacks", "maturity window"]) +
  B(180, 170, 150, 70, "fill", ["Offline log", "BigQuery", "as-served features"]) +
  B(370, 170, 150, 70, "sp", ["Identity graph", "components, rings", "Spark, nightly"]) +
  B(570, 170, 180, 70, "st", ["Graph lookups", "Spanner Graph", "2-3 hops, capped"]) +
  A("graph5", "M140 75 L178 75") + A("graph5", "M330 75 L368 75") + A("graph5", "M520 75 L568 75") +
  A("graph5", "M255 110 L255 168") + A("graph5", "M330 205 L368 205") + A("graph5", "M445 170 L445 112") +
  A("graph5", "M660 170 L660 112") + A("graph5", "M140 205 L178 205") +
  T(380, 275, "Training joins labels to features as they were at decision time", "sm", "middle")) },

});
