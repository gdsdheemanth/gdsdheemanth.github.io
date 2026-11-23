/* Data warehousing: section diagrams */
Object.assign(FIGS, {

warehousing_layers:{ t:"Layers from sources to consumers", cap:"Data moves left to right through layers, each with its own contract. Raw keeps everything exactly as received, so after a bug fix every layer above it can be rebuilt without asking the sources again. Pipelines own the left part; analysts and tools read only marts and the semantic layer.", svg:S("wh1", 260, "Landing, raw, staging, intermediate, marts and semantic layers, with medallion names and a rebuild path from raw",
  T(10, 20, "Sources: OLTP databases, SaaS APIs, event streams, files", "sm") + A("wh1", "M65 28 L65 58") +
  B(10, 60, 110, 60, "box", ["Landing", "files as sent"]) + A("wh1", "M120 90 L134 90") +
  B(136, 60, 110, 60, "st", ["Raw", "append-only"]) + A("wh1", "M246 90 L260 90") +
  B(262, 60, 110, 60, "sp", ["Staging", "typed, deduped"]) + A("wh1", "M372 90 L386 90") +
  B(388, 60, 110, 60, "sp", ["Intermediate", "joined, rules"]) + A("wh1", "M498 90 L512 90") +
  B(514, 60, 110, 60, "okf", ["Marts", "facts, dims"]) + A("wh1", "M624 90 L638 90") +
  B(640, 60, 110, 60, "pr", ["Semantic", "metrics, caches"]) +
  T(191, 140, "bronze", "tA", "middle") + T(380, 140, "silver", "tA", "middle") + T(569, 140, "gold", "tA", "middle") +
  A("wh1", "M191 150 L191 172 L569 172 L569 152", "ln dash") +
  T(380, 190, "after a bug fix, rebuild every layer above raw", "sm", "middle") +
  B(10, 210, 490, 36, "fill", ["Built by pipelines (dbt, Dataform); restricted access"]) +
  B(514, 210, 236, 36, "okf", ["What analysts and tools read"])) },

warehousing_mpp:{ t:"MPP: leader, workers and the exchange", cap:"The leader plans the query and splits it into stages. Each stage 1 worker scans its own slice of the data and pre-aggregates; the exchange then sends every row to the stage 2 worker that owns its key. The query finishes only when the slowest worker finishes, so one hot key that sends most rows to one worker makes everyone wait.", svg:S("wh2", 305, "Client, leader, four stage 1 workers, a shuffle exchange and four stage 2 workers, one of them overloaded by a hot key",
  B(20, 20, 150, 44, "box", ["Client", "sends SQL"]) + A("wh2", "M170 42 L248 42") +
  B(250, 20, 260, 44, "pr", ["Leader (coordinator)", "parse, optimise, split into stages"]) + A("wh2", "M510 42 L588 42") +
  B(590, 20, 150, 44, "box", ["Result", "merged by the leader"]) +
  A("wh2", "M340 64 L101 93") + A("wh2", "M370 64 L287 93") + A("wh2", "M390 64 L473 93") + A("wh2", "M420 64 L659 93") +
  B(21, 95, 160, 46, "sp", ["Worker 1", "scans its slice"]) +
  B(207, 95, 160, 46, "sp", ["Worker 2", "scans its slice"]) +
  B(393, 95, 160, 46, "sp", ["Worker 3", "scans its slice"]) +
  B(579, 95, 160, 46, "sp", ["Worker 4", "scans its slice"]) +
  A("wh2", "M101 141 L101 163") + A("wh2", "M287 141 L287 163") + A("wh2", "M473 141 L473 163") + A("wh2", "M659 141 L659 163") +
  B(21, 165, 718, 34, "ka", ["Exchange (shuffle): each row goes to the worker that owns its key"]) +
  A("wh2", "M101 199 L101 221") + A("wh2", "M287 199 L287 221") + A("wh2", "M473 199 L473 221") + A("wh2", "M659 199 L659 221") +
  B(21, 223, 160, 46, "sp", ["Stage 2, worker 1", "join, final agg"]) +
  B(207, 223, 160, 46, "sp", ["Stage 2, worker 2", "join, final agg"]) +
  B(393, 223, 160, 46, "sp", ["Stage 2, worker 3", "join, final agg"]) +
  B(579, 223, 160, 46, "sp", ["Stage 2, worker 4", "join, final agg"]) +
  '<rect class="bad" x="574" y="218" width="170" height="56" rx="10"/>' +
  T(743, 293, "hot key: this worker gets 55% of the rows", "tR", "end")) },

warehousing_snowflake:{ t:"Snowflake's three layers", cap:"A shared cloud services layer holds metadata, plans queries and serves the result cache. Independent virtual warehouses run the queries, each sized and suspended on its own and each caching data on local SSD. All of them read the same immutable micro-partitions in object storage, so workloads are isolated without copying data.", svg:S("wh3", 315, "Cloud services layer on top, three independent virtual warehouses in the middle, shared micro-partition storage at the bottom",
  B(20, 20, 720, 56, "pr", ["Cloud services", "authentication, metadata, optimizer, transactions, result cache"]) +
  A("wh3", "M130 76 L130 111") + A("wh3", "M380 76 L380 111") + A("wh3", "M630 76 L630 111") +
  B(20, 113, 220, 66, "sp", ["ETL warehouse", "Large, runs nightly", "local SSD cache"]) +
  B(270, 113, 220, 66, "sp", ["BI warehouse", "Small, 1 to 4 clusters", "local SSD cache"]) +
  B(520, 113, 220, 66, "sp", ["Data science warehouse", "Medium, auto-suspend", "local SSD cache"]) +
  A("wh3", "M130 179 L130 221") + A("wh3", "M380 179 L380 221") + A("wh3", "M630 179 L630 221") +
  B(20, 223, 720, 56, "st", ["Storage: immutable micro-partitions in object storage", "Cloud Storage on Google Cloud; columnar, 50 to 500 MB uncompressed each"]) +
  T(380, 302, "Each warehouse is billed only while it runs, and none waits for another", "sm", "middle")) },

warehousing_mergetree:{ t:"ClickHouse sparse primary index", cap:"A part is sorted by its ORDER BY key and cut into granules of 8,192 rows. The primary index keeps only the key of each granule's first row, and the marks give each granule's offset in the column files. For tenant 42 clicks, granule 2 starts with that key and granule 1 may hold earlier rows with the same key, so only granules 1 and 2 are read.", svg:S("wh4", 290, "Sparse primary index entries pointing to granules; the query reads two of five granules",
  T(20, 24, "One part, sorted by ORDER BY (tenant_id, event_type, event_time)", "b") +
  '<rect class="fill" x="20" y="40" width="220" height="200" rx="10"/>' +
  T(130, 60, "primary.idx (sparse)", "b", "middle") +
  T(36, 88, "g0: tenant 1, click", "mono") + T(36, 123, "g1: tenant 7, view", "mono") +
  T(36, 158, "g2: tenant 42, click", "mono") + T(36, 193, "g3: tenant 42, view", "mono") +
  T(36, 228, "g4: tenant 97, click", "mono") +
  T(330, 60, "marks: granule to offset", "sm", "middle") +
  A("wh4", "M220 84 L418 84") + A("wh4", "M220 119 L418 119", "acc") + A("wh4", "M220 154 L418 154", "acc") +
  A("wh4", "M220 189 L418 189") + A("wh4", "M220 224 L418 224") +
  B(420, 70, 320, 28, "box", ["granule 0: rows 0 to 8,191"]) +
  B(420, 105, 320, 28, "okf", ["granule 1: rows 8,192 to 16,383 (read)"]) +
  B(420, 140, 320, 28, "okf", ["granule 2: rows 16,384 to 24,575 (read)"]) +
  B(420, 175, 320, 28, "box", ["granule 3: rows 24,576 to 32,767"]) +
  B(420, 210, 320, 28, "box", ["granule 4: rows 32,768 to 40,959"]) +
  T(20, 272, "WHERE tenant_id = 42 AND event_type = 'click': binary search the index, read 2 of 5 granules", "sm")) },

warehousing_semantic:{ t:"A semantic layer between tools and the warehouse", cap:"Every consumer sends requests in business terms to one layer. The layer resolves metric names to definitions, plans safe joins, applies access rules and then answers from a pre-aggregation or cache when it can, or generates SQL against the warehouse marts when it cannot.", svg:S("wh5", 290, "Dashboards, spreadsheets, notebooks and AI agents query a semantic layer, which routes to pre-aggregations or warehouse marts",
  B(20, 30, 170, 40, "box", ["Dashboards", "Looker, Data Studio"]) +
  B(20, 85, 170, 40, "box", ["Spreadsheets"]) +
  B(20, 140, 170, 40, "box", ["Notebooks"]) +
  B(20, 195, 170, 40, "box", ["Apps and AI agents"]) +
  A("wh5", "M190 50 L258 50") + A("wh5", "M190 105 L258 105") + A("wh5", "M190 160 L258 160") + A("wh5", "M190 215 L258 215") +
  B(260, 30, 260, 205, "sp", ["Semantic layer", "metrics, dimensions, joins", "1. resolve metric names", "2. plan safe joins", "3. apply access rules", "4. route: cache or SQL"]) +
  A("wh5", "M520 65 L588 65", "acc") + A("wh5", "M520 192 L588 192") +
  B(590, 30, 150, 70, "ka", ["Pre-aggregations", "and result cache"]) +
  B(590, 150, 150, 85, "st", ["Warehouse marts", "facts and dimensions", "in BigQuery"]) +
  A("wh5", "M665 150 L665 102", "ln dash") + T(675, 130, "refresh", "sm") +
  T(380, 268, "Requests name metrics (revenue by month); the layer writes the SQL", "sm", "middle")) },

});
