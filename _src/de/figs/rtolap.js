/* Real-time OLAP engines: section diagrams */
Object.assign(FIGS, {

rtolap_place:{ t:"Where real-time OLAP sits", cap:"Events flow from Kafka into an RTOLAP engine that serves fresh, narrow, high-concurrency queries to apps and live dashboards. The same events land in the lake and warehouse for history, heavy joins and ad hoc analysis, and can be re-ingested from there.", svg:S("rt1", 270, "Kafka feeds an RTOLAP engine serving apps and dashboards, and in parallel a lake and warehouse serving analysts",
  B(20, 100, 150, 60, "ka", ["Event stream", "Kafka, Pub/Sub"]) +
  A("rt1", "M170 115 L268 60") + A("rt1", "M170 145 L268 200") +
  B(270, 30, 220, 64, "sp", ["Real-time OLAP", "Druid, Pinot, StarRocks"]) +
  B(270, 170, 220, 64, "st", ["Lake and warehouse", "Cloud Storage, BigQuery"]) +
  A("rt1", "M380 168 L380 96", "ln dash") + T(392, 136, "re-ingest", "sm") +
  A("rt1", "M490 62 L568 62", "acc") + A("rt1", "M490 202 L568 202") +
  B(570, 30, 170, 64, "okf", ["Apps and live views", "seconds fresh, 1000s QPS"]) +
  B(570, 170, 170, 64, "pr", ["Analysts, ML, BI", "minutes fresh, any SQL"])) },

rtolap_bitmap:{ t:"Dictionary, forward index and bitmaps", cap:"A string column is stored as a sorted dictionary plus one small id per row (the forward index). The inverted index keeps a bitmap per value. A filter such as country = IN AND device = ios becomes a bitwise AND of two bitmaps, and only the matching rows are read.", svg:S("rt2", 290, "Country column with dictionary, encoded ids and per-value bitmaps; two bitmaps are ANDed",
  T(110, 24, "Dictionary", "b", "middle") +
  B(40, 36, 140, 30, "box", ["DE = 0"], false) + B(40, 70, 140, 30, "box", ["IN = 1"], false) + B(40, 104, 140, 30, "box", ["US = 2"], false) +
  T(330, 24, "Forward index (rows 0 to 7)", "b", "middle") +
  B(220, 36, 220, 30, "sp", ["1  2  1  0  2  1  1  0"], false) +
  T(330, 92, "values: IN US IN DE US IN IN DE", "sm", "middle") +
  T(620, 24, "Inverted index (bitmaps)", "b", "middle") +
  B(490, 36, 250, 30, "box", ["DE: 0 0 0 1 0 0 0 1"], false) +
  B(490, 70, 250, 30, "okf", ["IN: 1 0 1 0 0 1 1 0"], false) +
  B(490, 104, 250, 30, "box", ["US: 0 1 0 0 1 0 0 0"], false) +
  T(380, 170, "WHERE country = 'IN' AND device = 'ios'", "b mono", "middle") +
  B(100, 190, 200, 34, "okf", ["IN:  1 0 1 0 0 1 1 0"], false) + T(330, 212, "AND", "b", "middle") +
  B(360, 190, 200, 34, "ka", ["ios: 1 0 0 1 1 1 0 0"], false) + A("rt2", "M560 207 L598 207") +
  B(600, 190, 140, 34, "pr", ["1 0 0 0 0 1 0 0"], false) +
  T(670, 248, "rows 0 and 5 match", "sm", "middle") + T(380, 276, "Only matching rows are read from the forward index and metric columns", "sm", "middle")) },

rtolap_druid:{ t:"Apache Druid architecture", cap:"Ingestion tasks on MiddleManagers read Kafka and answer queries on fresh data at once. They publish finished segments to deep storage and the metadata store, the Coordinator tells historicals to load them, and the broker merges results from tasks and historicals.", svg:S("rt3", 320, "Druid: Kafka to ingestion tasks, segments to deep storage, historicals load them, broker queries both",
  B(20, 30, 140, 54, "ka", ["Kafka topic", "partitions"]) + A("rt3", "M160 57 L218 57") +
  B(220, 30, 200, 54, "sp", ["MiddleManager tasks", "ingest, serve fresh rows"]) +
  A("rt3", "M320 84 L320 128") + T(332, 110, "publish segments", "sm") +
  B(220, 130, 200, 54, "st", ["Deep storage", "Cloud Storage"]) +
  B(20, 130, 160, 54, "box", ["Metadata store", "segments, offsets"]) + A("rt3", "M220 157 L182 157", "ln dash") +
  A("rt3", "M320 184 L320 226") +
  B(220, 228, 200, 54, "sp", ["Historicals", "load and serve segments"]) +
  B(20, 228, 160, 54, "pr", ["Coordinator", "assign, balance, rules"]) + A("rt3", "M180 255 L218 255", "ln dash") +
  B(470, 130, 140, 54, "pr", ["Overlord", "runs tasks"]) + A("rt3", "M470 145 L422 70", "ln dash") +
  B(620, 30, 120, 54, "okf", ["Broker", "plan, merge"]) +
  A("rt3", "M620 62 L422 62", "acc") + A("rt3", "M650 84 L422 248", "acc") + T(560, 200, "scatter", "tA", "middle") +
  B(620, 240, 120, 54, "box", ["Router", "console, gateway"]) + A("rt3", "M680 240 L680 86")) },

rtolap_pinot:{ t:"Apache Pinot: hybrid table and time boundary", cap:"Real-time servers consume Kafka into consuming segments that are queryable immediately; committed segments go to the deep store. Minion tasks or batch jobs build offline segments. The broker splits each query at the time boundary so no row is counted twice.", svg:S("rt4", 300, "Pinot broker splits a query between offline and real-time servers at the time boundary; controller manages both",
  B(20, 20, 160, 54, "ka", ["Kafka", "key-partitioned"]) + A("rt4", "M180 47 L248 47") +
  B(250, 20, 230, 54, "sp", ["Real-time servers", "consuming + committed"]) +
  B(20, 220, 160, 54, "st", ["Batch files", "Cloud Storage"]) + A("rt4", "M180 247 L248 247") +
  B(250, 220, 230, 54, "sp", ["Offline servers", "segments from batch"]) +
  B(250, 120, 230, 54, "box", ["Minion", "real-time to offline"]) + A("rt4", "M365 74 L365 118", "ln dash") + A("rt4", "M365 174 L365 218", "ln dash") +
  B(560, 120, 180, 54, "okf", ["Broker", "split, scatter, merge"]) +
  A("rt4", "M560 135 L482 55", "acc") + A("rt4", "M560 160 L482 240", "acc") +
  T(600, 70, "time > boundary", "tA", "middle") + T(600, 222, "time <= boundary", "tA", "middle") +
  B(560, 20, 180, 40, "pr", ["Controller + Helix"]) + T(650, 290, "ZooKeeper holds cluster state", "sm", "middle")) },

rtolap_starrocks:{ t:"StarRocks and Doris: frontends and backends", cap:"Clients connect with the MySQL protocol to a frontend, which plans the query with a cost-based optimizer. Backends store replicated tablets and run vectorized fragments in parallel. In shared-data mode, compute nodes cache data whose durable copy is in object storage.", svg:S("rt5", 300, "MySQL clients to FE leader and followers; plan fragments run on BEs holding tablets; optional object storage",
  B(20, 30, 150, 54, "box", ["BI and apps", "MySQL protocol"]) + A("rt5", "M170 57 L228 57") +
  '<rect class="fill" x="230" y="15" width="300" height="85" rx="12"/>' +
  B(245, 30, 130, 54, "pr", ["FE leader", "metadata, CBO"]) + B(385, 30, 130, 54, "pr", ["FE followers", "and observers"]) +
  A("rt5", "M380 100 L190 148") + A("rt5", "M380 100 L380 148") + A("rt5", "M380 100 L570 148") + T(470, 128, "plan fragments", "sm", "middle") +
  B(110, 150, 160, 64, "sp", ["BE 1", "tablets, vectorized"]) + B(300, 150, 160, 64, "sp", ["BE 2", "tablets, vectorized"]) + B(490, 150, 160, 64, "sp", ["BE 3", "tablets, vectorized"]) +
  A("rt5", "M270 182 L298 182", "kac dash") + A("rt5", "M460 182 L488 182", "kac dash") +
  B(110, 240, 540, 44, "st", ["Shared-data mode: object storage (Cloud Storage) holds data; compute nodes cache it"]) +
  A("rt5", "M380 214 L380 238", "ln dash")) },

});
