/* databases: Databases: OLTP, NoSQL and specialised, original drawings */
Object.assign(FIGS, {

databases_pgwrite:{ t:"PostgreSQL write path", cap:"A change is applied to the page in shared buffers and described in a WAL record. At commit only the WAL must reach disk (one sequential write). The checkpointer writes dirty data pages later, and replicas and CDC tools read the same WAL.", svg:S("databases1", 300, "Client update goes to shared buffers and WAL; WAL is flushed at commit; checkpointer writes pages later; replicas read the WAL",
  B(20, 40, 130, 56, "ka", ["Client", "UPDATE ... COMMIT"]) +
  B(200, 20, 200, 56, "sp", ["Shared buffers", "8 kB pages, now dirty"]) +
  B(200, 110, 200, 56, "sp", ["WAL buffers", "change records"]) +
  B(460, 110, 140, 56, "st", ["WAL files", "16 MB segments"]) +
  B(460, 20, 140, 56, "st", ["Data files", "table heap, indexes"]) +
  B(620, 200, 120, 56, "okf", ["Replicas, CDC", "read the WAL"]) +
  B(200, 220, 200, 56, "pr", ["Checkpointer", "writes dirty pages later"]) +
  A("databases1", "M150 60 L198 48") + A("databases1", "M150 76 L198 132") +
  A("databases1", "M400 138 L458 138", "acc") + T(430, 128, "fsync at commit", "sm tA", "middle") +
  A("databases1", "M300 218 L300 78", "ln dash") + A("databases1", "M400 240 L530 240 L530 78", "ln dash") +
  A("databases1", "M560 166 L640 198")) },

databases_lsm:{ t:"LSM tree: write and read paths", cap:"Writes go to a log for durability and to an in-memory memtable, which is flushed as an immutable sorted file in level 0. Compaction merges files down into larger, non-overlapping levels. A read checks the memtable first, then files from newest to oldest, skipping files whose Bloom filter rules the key out.", svg:S("databases2", 320, "Write to WAL and memtable, flush to level 0, compaction down to L1 and L2, read path checks newest first",
  B(20, 30, 120, 50, "ka", ["Write", "put(k, v)"]) +
  B(190, 10, 170, 40, "st", ["WAL (append only)"]) +
  B(190, 70, 170, 50, "sp", ["Memtable", "sorted, in memory"]) +
  A("databases2", "M140 48 L188 32") + A("databases2", "M140 62 L188 92") +
  T(420, 40, "memory", "sm tR") + A("databases2", "M20 140 L740 140", "ln dash") + T(420, 160, "disk", "sm tR") +
  B(190, 175, 170, 36, "box", ["L0: overlapping files"], false) +
  B(190, 225, 260, 36, "box", ["L1: sorted, non-overlapping"], false) +
  B(190, 275, 400, 36, "box", ["L2: about 10 times larger than L1"], false) +
  A("databases2", "M275 122 L275 173") + T(285, 160, "flush", "sm") +
  A("databases2", "M365 193 L400 223", "acc") + A("databases2", "M455 243 L500 273", "acc") + T(480, 210, "compaction", "sm tA") +
  B(620, 70, 120, 50, "okf", ["Read", "get(k)"]) +
  T(620, 190, "1. memtable", "sm") + T(620, 210, "2. L0 newest first", "sm") + T(620, 230, "3. one file per level", "sm") + T(620, 250, "Bloom filter skips", "sm") + T(620, 266, "files without the key", "sm")) },

databases_mvcc:{ t:"MVCC: one row, two versions, two snapshots", cap:"Transaction 105 updated the row: it set xmax on the old version and created a new one. A snapshot taken before 105 committed still sees balance 100; a snapshot taken after sees 70. Neither reader waited for the writer, and the old version stays until VACUUM removes it.", svg:S("databases3", 250, "Old tuple version with xmin 90 xmax 105 and new version with xmin 105; an old snapshot sees the old version and a new snapshot sees the new one",
  B(240, 30, 280, 60, "box", ["balance = 100", "xmin 90 · xmax 105"]) +
  B(240, 140, 280, 60, "sp", ["balance = 70", "xmin 105 · xmax none"]) +
  A("databases3", "M380 92 L380 138", "ln dash") + T(390, 120, "UPDATE by transaction 105", "sm") +
  B(20, 30, 170, 60, "ka", ["Snapshot A", "taken before 105 committed"]) +
  B(570, 140, 170, 60, "okf", ["Snapshot B", "taken after 105 committed"]) +
  A("databases3", "M190 60 L238 60", "acc") + A("databases3", "M570 170 L522 170", "acc") +
  T(20, 120, "A sees 100", "sm tA") + T(740, 230, "B sees 70", "sm tA", "end") +
  T(20, 230, "VACUUM removes the old version once no snapshot needs it", "sm")) },

databases_ring:{ t:"Cassandra token ring with RF = 3", cap:"The partition key is hashed to a token on the ring. The node owning that token range stores the partition, and the next two distinct nodes clockwise store the other replicas. Any node can coordinate a request and forward it to the replicas.", svg:S("databases4", 320, "Six nodes on a ring; a partition key hashes to a token and is stored on three consecutive nodes",
  '<circle class="fill" cx="250" cy="160" r="120"/>' +
  B(220, 18, 60, 30, "sp", ["N1"]) + B(340, 78, 60, 30, "sp", ["N2"]) + B(340, 212, 60, 30, "sp", ["N3"]) +
  B(220, 272, 60, 30, "box", ["N4"]) + B(100, 212, 60, 30, "box", ["N5"]) + B(100, 78, 60, 30, "box", ["N6"]) +
  '<circle class="st" cx="318" cy="60" r="7"/>' + T(330, 50, "token(k)", "sm tA") +
  A("databases4", "M372 110 L372 210", "acc") +
  B(470, 30, 270, 70, "ka", ["Partition key k", "Murmur3 hash gives a token"]) +
  B(470, 120, 270, 70, "sp", ["Replicas: N2, N3, N4", "owner plus the next two clockwise"]) +
  B(470, 210, 270, 70, "okf", ["LOCAL_QUORUM", "2 of 3 must acknowledge"]) +
  A("databases4", "M470 65 L330 60") + A("databases4", "M605 100 L605 118") + A("databases4", "M605 190 L605 208")) },

databases_cacheaside:{ t:"Cache-aside reads and writes", cap:"On a read the application checks the cache first. On a miss it reads the database and stores the result with a TTL. On a write it updates the database and then deletes the cache key, so the next read reloads a fresh value.", svg:S("databases5", 270, "Application reads cache, on miss reads database and fills cache; writes go to database and delete the cache key",
  B(20, 100, 140, 60, "ka", ["Application"]) +
  B(320, 20, 180, 60, "sp", ["Cache", "Redis or Valkey"]) +
  B(320, 180, 180, 60, "st", ["Database", "system of record"]) +
  A("databases5", "M160 115 L318 45") + T(200, 55, "1. GET key", "sm") +
  A("databases5", "M160 145 L318 205") + T(170, 200, "2. miss: SELECT", "sm") +
  A("databases5", "M160 125 L318 65", "ln dash") + T(215, 105, "3. SET key, TTL", "sm") +
  B(560, 100, 180, 60, "pr", ["On write", "UPDATE, then DEL key"]) +
  A("databases5", "M600 160 L502 210", "acc") + A("databases5", "M600 100 L502 50", "acc")) },
});
