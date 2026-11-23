/* queryengines: Query engine internals, original drawings */
Object.assign(FIGS, {

queryengines_pipeline:{ t:"From SQL text to results", cap:"The top row is planning: the parser builds a syntax tree, the validator resolves names and types against the catalog, and the optimizer rewrites the logical plan with rules and, using statistics, by cost. The bottom row is execution: the physical plan says which algorithms to use, and the executor runs it.", svg:S("queryengines1", 295, "SQL text flows through parser, validator, logical plan and optimizer, then physical plan, executor and results, with the catalog feeding validator and optimizer",
  T(20, 18, "Planning", "sm") +
  B(20, 30, 110, 56, "pr", ["SQL text", "SELECT ..."]) +
  B(160, 30, 120, 56, "sp", ["Parser", "syntax tree"]) +
  B(310, 30, 120, 56, "sp", ["Validator", "names, types"]) +
  B(460, 30, 130, 56, "sp", ["Logical plan", "what to compute"]) +
  B(620, 30, 120, 56, "sp", ["Optimizer", "rules, then cost"]) +
  A("queryengines1", "M130 58 L158 58") + A("queryengines1", "M280 58 L308 58") +
  A("queryengines1", "M430 58 L458 58") + A("queryengines1", "M590 58 L618 58") +
  B(310, 118, 280, 46, "box", ["Catalog and statistics", "schemas, row counts, NDV, histograms"]) +
  A("queryengines1", "M370 118 L370 88", "dash") + A("queryengines1", "M590 130 L650 88", "dash") +
  B(620, 200, 120, 56, "st", ["Physical plan", "how: algorithms"]) +
  A("queryengines1", "M680 86 L680 198", "acc") +
  B(330, 200, 250, 56, "fill", ["Executor", "pipelines, vectors, SIMD, JIT"]) +
  A("queryengines1", "M620 228 L582 228", "acc") +
  B(20, 200, 140, 56, "okf", ["Results", "rows or batches"]) +
  A("queryengines1", "M330 228 L162 228", "acc") +
  T(330, 282, "Execution", "sm")) },

queryengines_substrait:{ t:"Substrait: producers and consumers", cap:"Producers on the left turn SQL, DataFrame code or Spark plans into one standard plan format. Any consumer on the right that supports the operators and functions in the plan can execute it, with no SQL dialect in between.", svg:S("queryengines2", 255, "Ibis, Isthmus, DuckDB and Gluten produce a Substrait plan that Acero, DataFusion, DuckDB and Velox consume",
  T(105, 20, "Producers", "sm", "middle") +
  B(20, 30, 170, 40, "pr", ["Ibis"]) +
  B(20, 85, 170, 40, "pr", ["Isthmus (Calcite)"]) +
  B(20, 140, 170, 40, "pr", ["DuckDB, DataFusion"]) +
  B(20, 195, 170, 40, "pr", ["Gluten (from Spark)"]) +
  A("queryengines2", "M190 50 L278 115") + A("queryengines2", "M190 105 L278 125") +
  A("queryengines2", "M190 160 L278 135") + A("queryengines2", "M190 215 L278 145") +
  B(280, 85, 200, 80, "ka", ["Substrait plan", "protobuf or JSON", "relations, types, functions"]) +
  A("queryengines2", "M480 115 L568 50") + A("queryengines2", "M480 125 L568 105") +
  A("queryengines2", "M480 135 L568 160") + A("queryengines2", "M480 145 L568 215") +
  T(655, 20, "Consumers", "sm", "middle") +
  B(570, 30, 170, 40, "okf", ["Arrow Acero"]) +
  B(570, 85, 170, 40, "okf", ["DataFusion"]) +
  B(570, 140, 170, 40, "okf", ["DuckDB"]) +
  B(570, 195, 170, 40, "okf", ["Velox (via Gluten)"])) },

queryengines_pullpush:{ t:"Pull one row at a time versus push morsels through pipelines", cap:"On the left, each Volcano operator calls next() on its child and receives one row per call. On the right, worker threads each take the next morsel and push it through a whole pipeline into a sink, a pipeline breaker that merges thread-local results before the next pipeline starts.", svg:S("queryengines3", 290, "Volcano pull model with Sum, Filter and Scan, next to a push model where three threads run a pipeline over morsels into a sink",
  T(20, 20, "Pull (Volcano): one row per call", "b") +
  B(60, 40, 140, 44, "sp", ["Sum"]) +
  B(60, 125, 140, 44, "sp", ["Filter"]) +
  B(60, 210, 140, 44, "sp", ["Scan"]) +
  A("queryengines3", "M100 84 L100 123", "dash") + A("queryengines3", "M160 123 L160 86") +
  A("queryengines3", "M100 169 L100 208", "dash") + A("queryengines3", "M160 208 L160 171") +
  T(94, 108, "next()", "sm mono", "end") + T(168, 108, "row", "sm") +
  T(94, 193, "next()", "sm mono", "end") + T(168, 193, "row", "sm") +
  T(300, 20, "Push: pipelines over morsels", "b") +
  B(300, 34, 40, 30, "st", ["m1"]) + B(345, 34, 40, 30, "st", ["m2"]) + B(390, 34, 40, 30, "st", ["m3"]) +
  B(435, 34, 40, 30, "st", ["m4"]) + B(480, 34, 40, 30, "st", ["m5"]) + B(525, 34, 40, 30, "st", ["m6"]) +
  A("queryengines3", "M320 64 L320 94", "dash") + T(330, 84, "each thread takes the next morsel", "sm") +
  B(300, 96, 240, 36, "fill", ["T1: scan, filter, local agg"], false) +
  B(300, 142, 240, 36, "fill", ["T2: scan, filter, local agg"], false) +
  B(300, 188, 240, 36, "fill", ["T3: scan, filter, local agg"], false) +
  B(590, 120, 150, 60, "ka", ["Sink (breaker)", "merge local states"]) +
  A("queryengines3", "M540 114 L588 140") + A("queryengines3", "M540 160 L588 152") + A("queryengines3", "M540 206 L588 166") +
  B(590, 214, 150, 44, "okf", ["Next pipeline", "starts when done"]) +
  A("queryengines3", "M665 180 L665 212", "acc") +
  T(300, 250, "breakers: join build, aggregate, sort, exchange", "sm")) },

queryengines_simd:{ t:"A SIMD filter: compare, mask, selection vector", cap:"One AVX2 instruction compares eight 32-bit values with 10 at once. The result becomes an 8-bit mask, one bit per lane, and the positions of the set bits form the selection vector, with no branch that depends on the data.", svg:S("queryengines4", 275, "Eight values compared with 10 in one instruction produce a mask 10101010 and the selection vector 1, 3, 5, 7",
  T(20, 20, "One 256-bit register: 8 lanes of 32-bit integers", "b") +
  B(20, 32, 60, 34, "st", ["5"]) + B(86, 32, 60, 34, "st", ["12"]) + B(152, 32, 60, 34, "st", ["7"]) + B(218, 32, 60, 34, "st", ["30"]) +
  B(284, 32, 60, 34, "st", ["1"]) + B(350, 32, 60, 34, "st", ["18"]) + B(416, 32, 60, 34, "st", ["9"]) + B(482, 32, 60, 34, "st", ["25"]) +
  A("queryengines4", "M281 66 L281 92", "acc") +
  B(20, 94, 522, 34, "sp", ["compare with 10 in all 8 lanes: one instruction"]) +
  A("queryengines4", "M281 128 L281 148", "acc") +
  B(20, 150, 60, 34, "box", ["0"]) + B(86, 150, 60, 34, "okf", ["1"]) + B(152, 150, 60, 34, "box", ["0"]) + B(218, 150, 60, 34, "okf", ["1"]) +
  B(284, 150, 60, 34, "box", ["0"]) + B(350, 150, 60, 34, "okf", ["1"]) + B(416, 150, 60, 34, "box", ["0"]) + B(482, 150, 60, 34, "okf", ["1"]) +
  T(50, 202, "lane 0", "sm", "middle") + T(116, 202, "1", "sm", "middle") + T(182, 202, "2", "sm", "middle") + T(248, 202, "3", "sm", "middle") +
  T(314, 202, "4", "sm", "middle") + T(380, 202, "5", "sm", "middle") + T(446, 202, "6", "sm", "middle") + T(512, 202, "lane 7", "sm", "middle") +
  B(580, 32, 160, 62, "box", ["Scalar loop", "8 compares,", "8 branches"]) +
  B(580, 150, 160, 34, "pr", ["mask 10101010"]) +
  A("queryengines4", "M542 167 L578 167") +
  B(580, 214, 160, 46, "ka", ["Selection vector", "[1, 3, 5, 7]"]) +
  A("queryengines4", "M660 184 L660 212", "acc") +
  T(20, 236, "bit i of the mask is lane i: read right to left", "sm")) },

queryengines_native:{ t:"Native acceleration under Spark", cap:"Spark plans the query as usual; the plugin replaces supported operators with native ones and ships plan fragments to executors. Native operators work on columnar batches; an unsupported operator, such as a UDF, falls back to the JVM, with columnar-to-row (C2R) and row-to-columnar (R2C) transitions around it.", svg:S("queryengines5", 295, "Spark driver and plugin produce native plan fragments; inside an executor native scan and aggregate feed a C2R transition, a JVM operator and an R2C transition, plus a columnar shuffle",
  B(20, 30, 180, 60, "pr", ["Spark driver", "Catalyst plans as usual"]) +
  A("queryengines5", "M200 60 L238 60") +
  B(240, 30, 230, 60, "ka", ["Plugin rewrites plan", "Comet, Gluten, Lightning NQE"]) +
  A("queryengines5", "M470 60 L508 60") +
  B(510, 30, 230, 60, "box", ["Plan fragments", "Substrait or protobuf, via JNI"]) +
  A("queryengines5", "M625 90 L625 118", "acc") +
  '<rect class="fill" x="20" y="120" width="720" height="160" rx="8"/>' + T(32, 140, "Executor", "b") +
  B(40, 160, 150, 56, "okf", ["Native scan", "Parquet to Arrow"]) +
  A("queryengines5", "M190 188 L208 188") +
  B(210, 160, 150, 56, "okf", ["Native filter", "and aggregate"]) +
  A("queryengines5", "M360 188 L378 188") +
  B(380, 160, 110, 56, "box", ["C2R", "transition"]) +
  A("queryengines5", "M490 188 L508 188") +
  B(510, 160, 100, 56, "st", ["JVM op", "e.g. a UDF"]) +
  A("queryengines5", "M610 188 L628 188") +
  B(630, 160, 100, 56, "box", ["R2C", "transition"]) +
  A("queryengines5", "M285 216 L285 230", "acc") +
  B(210, 232, 320, 36, "ka", ["Columnar shuffle to other executors"], false)) },
});
