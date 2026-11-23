/* Data lakes and lakehouses: section diagrams */
function LHgrid(ox, oy, fileOf){
  var cls = ["sp", "st", "ka", "pr"], s = "";
  for (var x = 0; x < 4; x++) for (var y = 0; y < 4; y++) {
    var f = fileOf(x, y);
    s += '<rect class="' + cls[f] + '" x="' + (ox + 44 * x) + '" y="' + (oy + 44 * y) + '" width="40" height="40" rx="4"/>';
    s += T(ox + 44 * x + 20, oy + 44 * y + 25, "F" + f, "sm", "middle");
  }
  for (var i = 0; i < 4; i++) {
    s += T(ox + 44 * i + 20, oy - 8, "x=" + i, "sm", "middle");
    s += T(ox - 8, oy + 44 * i + 25, "y=" + i, "sm", "end");
  }
  return s;
}
Object.assign(FIGS, {

lakehouse_layers:{ t:"The four layers of a lakehouse", cap:"Engines never list folders to find a table. They ask the catalog for the table's current metadata, the table format's metadata lists the exact data files of that snapshot, and the files themselves are Parquet objects in Cloud Storage. Each layer can be swapped independently.", svg:S("lh1", 330, "Engines on top, then catalog, table format, file format and object storage layers",
  B(20, 15, 132, 44, "box", ["BigQuery"]) + B(162, 15, 132, 44, "box", ["Spark"]) + B(304, 15, 132, 44, "box", ["Flink"]) + B(446, 15, 132, 44, "box", ["Trino"]) + B(588, 15, 152, 44, "box", ["DuckDB"]) +
  A("lh1", "M380 61 L380 79") +
  B(20, 81, 720, 50, "pr", ["Catalog: table name to current metadata pointer, access control", "Lakehouse runtime catalog, Apache Polaris, Unity Catalog, Hive Metastore"]) +
  A("lh1", "M380 133 L380 149") +
  B(20, 151, 720, 50, "ka", ["Table format: snapshots, file lists, statistics, atomic commits", "Apache Iceberg, Delta Lake, Apache Hudi, Apache Paimon, DuckLake"]) +
  A("lh1", "M380 203 L380 219") +
  B(20, 221, 720, 44, "sp", ["File format: columnar data inside each file", "Parquet (default), ORC, Avro"]) +
  A("lh1", "M380 267 L380 279") +
  B(20, 281, 720, 40, "st", ["Object storage: Cloud Storage buckets (immutable objects)"])) },

lakehouse_parquet:{ t:"Inside a Parquet file", cap:"Rows are split into row groups, each column of a row group is stored as one contiguous column chunk made of pages, and the footer at the end describes everything. A reader fetches the footer first and then only the column chunks it needs.", svg:S("lh2", 300, "Parquet file layout with row groups, column chunks, pages and footer, and the read path",
  B(20, 12, 380, 28, "box", ["PAR1 (4-byte magic)"], false) +
  '<rect class="fill" x="20" y="48" width="380" height="104" rx="10"/>' + T(210, 68, "Row group 0 (for example 128 MiB of rows)", "b", "middle") +
  B(32, 78, 174, 64, "sp", ["Column chunk: country", "dictionary + data pages"]) +
  B(214, 78, 174, 64, "sp", ["Column chunk: amount", "data pages"]) +
  B(20, 160, 380, 36, "sp", ["Row group 1, row group 2 ..."]) +
  B(20, 204, 380, 50, "ka", ["Footer (Thrift FileMetaData)", "schema, row group offsets, min/max/nulls"]) +
  B(20, 262, 380, 28, "box", ["footer length (4 bytes) + PAR1"], false) +
  B(450, 20, 290, 46, "pr", ["1. Read the last 8 bytes", "footer length and magic"]) + A("lh2", "M595 66 L595 84") +
  B(450, 86, 290, 46, "pr", ["2. Read the footer", "schema, offsets, statistics"]) + A("lh2", "M595 132 L595 150") +
  B(450, 152, 290, 46, "pr", ["3. Skip row groups", "whose statistics cannot match"]) + A("lh2", "M595 198 L595 216") +
  B(450, 218, 290, 46, "pr", ["4. Ranged reads", "only the needed column chunks"]) +
  A("lh2", "M402 229 L448 112", "acc dash")) },

lakehouse_iceberg:{ t:"Iceberg's metadata tree", cap:"The catalog points at the current table metadata file, which lists snapshots. Each snapshot has a manifest list, manifests record data files with their statistics, and a new snapshot reuses unchanged manifests. A commit is an atomic swap of the catalog pointer.", svg:S("lh3", 330, "Catalog pointer to table metadata, snapshots, manifest lists, manifests and data files",
  B(260, 8, 240, 44, "pr", ["Catalog", "orders: v3.metadata.json"]) + A("lh3", "M380 52 L380 70") +
  B(200, 72, 360, 44, "box", ["Table metadata (v3.metadata.json)", "schemas, partition specs, snapshot list"]) +
  A("lh3", "M300 116 L200 140") + A("lh3", "M460 116 L560 140") +
  B(80, 142, 220, 44, "st", ["Snapshot 1", "manifest list ml-1.avro"]) +
  B(460, 142, 220, 44, "st", ["Snapshot 2 (current)", "manifest list ml-2.avro"]) +
  A("lh3", "M180 186 L160 210") + A("lh3", "M500 186 L270 212", "acc dash") + A("lh3", "M600 186 L600 210") +
  B(60, 212, 210, 44, "ka", ["Manifest m1.avro", "files + column bounds"]) +
  B(495, 212, 210, 44, "ka", ["Manifest m2.avro", "files + column bounds"]) +
  T(383, 240, "snapshot 2 reuses m1", "sm", "middle") +
  A("lh3", "M120 256 L85 282") + A("lh3", "M210 256 L230 282") + A("lh3", "M550 256 L530 282") + A("lh3", "M650 256 L675 282") +
  B(20, 284, 130, 36, "okf", ["a.parquet"]) + B(165, 284, 130, 36, "okf", ["b.parquet"]) +
  B(465, 284, 130, 36, "okf", ["c.parquet"]) + B(610, 284, 130, 36, "okf", ["d.parquet"])) },

lakehouse_delta:{ t:"Delta Lake's transaction log", cap:"Each commit is a numbered JSON file of add and remove actions. Every 10 commits a Parquet checkpoint stores the full state, and _last_checkpoint points at it, so a reader loads the checkpoint and replays only the later commits.", svg:S("lh4", 270, "Delta log with JSON commits, a checkpoint, the last checkpoint pointer and a reader",
  T(20, 24, "_delta_log/", "b") +
  B(20, 36, 110, 48, "box", ["000.json", "add a, b"]) + B(140, 36, 110, 48, "box", ["001.json", "add c"]) +
  B(260, 36, 60, 48, "box", ["..."], false) + B(330, 36, 140, 48, "ka", ["010.checkpoint", "full state (Parquet)"]) +
  B(480, 36, 120, 48, "box", ["011.json", "remove a, add e"]) + B(610, 36, 130, 48, "box", ["012.json", "add f"]) +
  B(330, 124, 140, 42, "pr", ["_last_checkpoint", "version 10"]) + A("lh4", "M400 124 L400 87") +
  T(20, 132, "Writers race to create the next", "sm") + T(20, 150, "file (013.json) with put-if-absent;", "sm") + T(20, 168, "only one can win.", "sm") +
  A("lh4", "M400 168 L400 204") + A("lh4", "M540 86 L540 204") + A("lh4", "M675 86 L675 204") +
  B(20, 206, 720, 50, "sp", ["Reader: load checkpoint 10, then replay 011 and 012", "live files = checkpoint files, minus a, plus e and f"])) },

lakehouse_zorder:{ t:"Linear sort versus Z-order", cap:"Sixteen rows on a 4 by 4 grid of x and y, written as four files (F0 to F3). Sorting by x makes each file a column, perfect for filters on x and useless for y. Z-order makes each file a square, so a filter on either column opens only two files.", svg:S("lh5", 300, "Two grids comparing file assignment under linear sort and Z-order",
  T(170, 22, "ORDER BY x, y", "b", "middle") + T(570, 22, "ZORDER BY (x, y)", "b", "middle") +
  LHgrid(82, 50, function(x, y){ return x; }) +
  LHgrid(482, 50, function(x, y){ return (y >> 1) * 2 + (x >> 1); }) +
  T(170, 252, "x = 1: 1 file,  y = 1: 4 files", "sm", "middle") +
  T(570, 252, "x = 1: 2 files,  y = 1: 2 files", "sm", "middle") +
  T(380, 284, "Each file's min-max range on a column decides whether it can be skipped.", "sm", "middle")) },

});
