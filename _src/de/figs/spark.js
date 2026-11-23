/* Apache Spark: section diagrams */
Object.assign(FIGS, {

spark_arch:{ t:"Driver, cluster manager and executors", cap:"The driver turns each action into a job, the DAGScheduler cuts it into stages at shuffle boundaries, and the TaskScheduler sends one task per partition to executor cores. The cluster manager only hands out containers; it never sees your data.", svg:S("sp1", 300, "The driver with its schedulers asks a cluster manager for executors and sends tasks to their cores",
  '<rect class="fill" x="20" y="20" width="250" height="260" rx="12"/>' + T(145, 44, "Driver", "b", "middle") +
  B(40, 60, 210, 44, "pr", ["SparkSession", "your program, the plan"]) +
  B(40, 118, 210, 44, "sp", ["DAGScheduler", "job to stages"]) +
  B(40, 176, 210, 44, "sp", ["TaskScheduler", "stages to tasks, retries"]) +
  B(40, 230, 210, 36, "box", ["Spark UI :4040"], false) +
  A("sp1", "M145 104 L145 116") + A("sp1", "M145 162 L145 174") +
  B(320, 20, 200, 56, "ka", ["Cluster manager", "YARN, Kubernetes, Standalone"]) +
  A("sp1", "M270 48 L318 48") + T(294, 40, "ask", "sm", "middle") +
  '<rect class="fill" x="560" y="20" width="180" height="120" rx="12"/>' + T(650, 42, "Executor 1 (JVM)", "b", "middle") +
  B(575, 54, 70, 34, "okf", ["task"], false) + B(655, 54, 70, 34, "okf", ["task"], false) +
  B(575, 96, 150, 32, "st", ["cache + shuffle files"], false) +
  '<rect class="fill" x="560" y="160" width="180" height="120" rx="12"/>' + T(650, 182, "Executor 2 (JVM)", "b", "middle") +
  B(575, 194, 70, 34, "okf", ["task"], false) + B(655, 194, 70, 34, "okf", ["task"], false) +
  B(575, 236, 150, 32, "st", ["cache + shuffle files"], false) +
  A("sp1", "M520 60 L558 75", "ln dash") + A("sp1", "M520 66 L558 200", "ln dash") + T(500, 110, "launch", "sm", "middle") +
  A("sp1", "M250 198 L558 90", "acc") + A("sp1", "M250 204 L558 225", "acc") + T(420, 180, "tasks", "tA", "middle")) },

spark_connect:{ t:"Spark Connect: client and server", cap:"The client only builds an unresolved plan and sends it over gRPC. Analysis, optimization and execution happen in the server inside the driver, and results return as Arrow batches. Many thin clients can share one server.", svg:S("sp2", 230, "Thin clients send plans over gRPC to a Spark Connect server that runs in the driver and returns Arrow batches",
  B(20, 20, 190, 50, "box", ["Python client", "pyspark-client, no JVM"]) +
  B(20, 90, 190, 50, "box", ["Go or Scala client", "any gRPC language"]) +
  B(20, 160, 190, 50, "box", ["Notebook or app", "its own session"]) +
  A("sp2", "M210 45 L318 95") + A("sp2", "M210 115 L318 115") + A("sp2", "M210 185 L318 135") +
  T(265, 80, "plan (protobuf)", "sm", "middle") +
  '<rect class="fill" x="320" y="40" width="230" height="150" rx="12"/>' + T(435, 62, "Driver", "b", "middle") +
  B(335, 74, 200, 44, "pr", ["Spark Connect server", "gRPC, port 15002"]) +
  B(335, 130, 200, 44, "sp", ["Catalyst and scheduler", "analyse, optimise, run"]) +
  A("sp2", "M435 118 L435 128") +
  A("sp2", "M550 152 L598 152") + B(600, 120, 140, 64, "st", ["Executors", "do the work"]) +
  A("sp2", "M320 168 L212 200", "acc dash") + T(268, 214, "results (Arrow)", "tA", "middle")) },

spark_catalyst:{ t:"From query to code: the Catalyst pipeline", cap:"A DataFrame or SQL query becomes an unresolved plan, is resolved against the catalog, rewritten by rules, turned into candidate physical plans, and finally compiled into Java code by whole-stage code generation. AQE can re-plan the remaining stages at each shuffle.", svg:S("sp3", 260, "Parsing, analysis, logical optimization, physical planning and code generation, with AQE looping back",
  B(20, 30, 130, 56, "box", ["SQL or", "DataFrame code"]) + A("sp3", "M150 58 L176 58") +
  B(178, 30, 130, 56, "sp", ["Unresolved", "logical plan"]) + A("sp3", "M308 58 L334 58") +
  B(336, 30, 130, 56, "sp", ["Analyzed", "logical plan"]) + A("sp3", "M466 58 L492 58") +
  B(494, 30, 130, 56, "sp", ["Optimized", "logical plan"]) + A("sp3", "M624 58 L650 58") +
  B(652, 30, 90, 56, "pr", ["Physical", "plans"]) +
  T(243, 108, "parse", "sm", "middle") + T(401, 108, "catalog", "sm", "middle") + T(559, 108, "rules", "sm", "middle") + T(697, 108, "strategies", "sm", "middle") +
  A("sp3", "M697 86 L697 150") +
  B(520, 152, 220, 50, "okf", ["Selected plan + codegen", "Tungsten, *(n) stages"]) + A("sp3", "M520 177 L382 177") +
  B(180, 152, 200, 50, "st", ["Execution", "stages of tasks"]) +
  A("sp3", "M280 202 L280 230 L630 230 L630 204", "kac dash") + T(455, 248, "AQE: re-optimise after each shuffle with real sizes", "tK", "middle")) },

spark_shuffle:{ t:"Sort-based shuffle", cap:"Each map task sorts its output by target partition and writes one data file and one index file to local disk. Each reduce task then fetches its own slice from every map output over the network. M map tasks and R reduce tasks mean up to M times R fetches.", svg:S("sp4", 290, "Three map tasks write partitioned files; two reduce tasks each fetch their slice from all three",
  T(110, 24, "Stage 1: map tasks (shuffle write)", "b", "middle") + T(640, 24, "Stage 2: reduce tasks (shuffle read)", "b", "middle") +
  B(20, 40, 180, 64, "sp", ["Map task 1", "data file + index"]) +
  B(20, 120, 180, 64, "sp", ["Map task 2", "data file + index"]) +
  B(20, 200, 180, 64, "sp", ["Map task 3", "data file + index"]) +
  B(205, 44, 34, 26, "okf", ["P0"], false) + B(205, 74, 34, 26, "ka", ["P1"], false) +
  B(205, 124, 34, 26, "okf", ["P0"], false) + B(205, 154, 34, 26, "ka", ["P1"], false) +
  B(205, 204, 34, 26, "okf", ["P0"], false) + B(205, 234, 34, 26, "ka", ["P1"], false) +
  B(560, 60, 180, 64, "okf", ["Reduce task P0", "merge, aggregate"]) +
  B(560, 170, 180, 64, "ka", ["Reduce task P1", "merge, aggregate"]) +
  A("sp4", "M239 57 L558 85", "acc") + A("sp4", "M239 137 L558 92", "acc") + A("sp4", "M239 217 L558 99", "acc") +
  A("sp4", "M239 87 L558 192", "kac") + A("sp4", "M239 167 L558 199", "kac") + A("sp4", "M239 247 L558 206", "kac") +
  T(400, 278, "local disk on the map side; network fetch on the reduce side", "sm", "middle")) },

spark_memory:{ t:"Inside an executor's memory", cap:"With spark.executor.memory=8g the container also gets about 819 MB of overhead outside the heap. Inside the heap, 300 MB is reserved, 60% of the rest is the unified pool shared by execution and storage, and 40% is user memory. Execution can evict storage beyond the protected half, never the reverse.", svg:S("sp5", 250, "Executor container split into overhead, reserved, user memory and a unified pool of execution and storage",
  T(380, 22, "Executor container = heap (spark.executor.memory) + overhead", "b", "middle") +
  B(20, 40, 120, 150, "ka", ["Overhead", "~10%, min 384 MB", "Python, native,", "off-heap buffers"]) +
  '<rect class="fill" x="155" y="40" width="585" height="150" rx="12"/>' + T(447, 60, "JVM heap", "b", "middle") +
  B(170, 72, 90, 104, "box", ["Reserved", "300 MB"]) +
  B(272, 72, 160, 104, "pr", ["User memory", "40% of rest", "your objects,", "UDF data"]) +
  B(444, 72, 140, 104, "sp", ["Execution", "shuffle, sort,", "join, aggregate"]) +
  B(590, 72, 140, 104, "st", ["Storage", "cache, broadcast", "half protected"]) +
  A("sp5", "M514 200 L656 200", "acc") + A("sp5", "M656 214 L514 214", "kac dash") +
  T(450, 238, "unified pool: 60% of (heap - 300 MB); borrow both ways, only execution evicts", "sm", "middle")) },

});
