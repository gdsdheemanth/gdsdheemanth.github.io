This section is for anyone who has met databases only from the outside: you can write SQL and you know that an application "saves things in Postgres", but you have never looked at what happens on disk when you press Enter. It builds on **Data engineering foundations** (the split between OLTP and OLAP systems), **SQL for data engineering** (queries, joins, transactions), **Data modeling** (keys and normalisation), and the two sections just before this one, **Data warehousing** and **Data lakes and lakehouses**, which covered the analytical side. Here we turn to the operational side: the databases that applications write to every second, and that your pipelines read from.

Why should a data engineer care how an OLTP database works inside? Because almost every pipeline starts at one. You will run change data capture against PostgreSQL's write-ahead log, run extracts that must not slow down a production database, explain why a replica is twenty minutes behind, pick a store for a feature that needs millisecond lookups, and sit in design reviews where someone proposes Cassandra, Redis or Elasticsearch. Each of those conversations goes better when you know what the engine is doing.

**What you will be able to do afterwards**

- Explain how PostgreSQL stores rows in pages, makes commits durable with a write-ahead log, keeps old row versions for concurrent readers, and cleans them up with VACUUM.
- Compare B-tree and LSM-tree storage engines by read, write and space amplification, and say which engines use which.
- Describe how distributed SQL databases (Spanner, CockroachDB, TiDB) and Cassandra spread data across machines and keep replicas consistent.
- Choose a sharding key and spot a hot shard before it happens.
- Say when a search engine, a time-series database or an in-memory store is the right tool, and how each works inside.
- Put a cache and a connection pooler in front of a database safely, and change a production schema without downtime.

**The big picture.** Every database answers the same three questions in its own way. **Where do the bytes live?** (pages in a B-tree, or sorted immutable files in an LSM tree.) **What happens when two people touch the same row at once?** (locks, or several versions of the row.) **What happens when a machine dies?** (a log that can be replayed, and copies on other machines.) A relational database on one server answers all three inside one process. Distributed databases answer them across many machines, which forces trade-offs between consistency, latency and availability. Specialised stores (search, time series, caches) give up generality to be excellent at one access pattern. Once you see each system as a set of answers to these questions, the zoo of products becomes a small number of recurring designs.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Relational internals

The first group opens up a single relational database. PostgreSQL is the running example because it is open source, very widely used (Cloud SQL for PostgreSQL and AlloyDB for PostgreSQL on Google Cloud are both built on it), and unusually well documented. Most of what you learn transfers to MySQL, SQL Server and Oracle, with different names for the same ideas.

The four topics build on each other. The first gives the whole map of PostgreSQL: processes, pages, the write-ahead log, indexes and the planner. The second steps back and compares the two big families of storage engine, B-trees and LSM trees. The third goes deep on MVCC, the trick that lets readers and writers work at the same time, and the problems it causes. The fourth takes the LSM family to its most important open-source implementation, RocksDB, which hides inside many systems you will use as a data engineer, from Kafka Streams to Flink.

### Relational DB Internals (PostgreSQL)

::: plain
A relational database looks like a set of tidy tables, but inside it is closer to a library with a very careful librarian. Rows are stored on fixed-size pages, like pages in a ring binder. Before the librarian changes any page, she first writes a short note in a journal saying what she is about to change, so that if the lights go out she can redo the work from the journal. Indexes are the card catalogue that lets her find a row without reading every page, and the query planner is the part of her brain that decides whether to use the catalogue or just walk the shelves.
:::

**Processes and memory.** PostgreSQL runs as a family of operating-system processes. A supervisor process called the **postmaster** listens for connections and starts one **backend process** per client connection; that backend parses, plans and runs your queries. Background processes do housekeeping: the **checkpointer** and **background writer** write changed pages to disk, the **WAL writer** flushes the log, the **autovacuum launcher** starts cleanup workers, and **WAL senders** stream the log to replicas. All of them share a memory area called **shared buffers** (the page cache; the default `shared_buffers` is 128 MB, and production servers usually set it to about a quarter of RAM).

The process-per-connection design is why PostgreSQL struggles with thousands of direct connections, a problem the topic Connection Pooling & Query Routing solves.

**Pages and tuples.** Every table is stored as a **heap**: a file split into 8 kB **pages**. Each page has a header, an array of line pointers, and the row versions themselves, which PostgreSQL calls **tuples**. A tuple is addressed by its **ctid**, a pair of (page number, line pointer). Alongside each table PostgreSQL keeps a **free space map** (which pages have room) and a **visibility map** (which pages contain only rows visible to everyone, so they can be skipped by VACUUM and by index-only scans).

```sql
-- PostgreSQL: see where rows physically live
CREATE TABLE accounts (id int PRIMARY KEY, owner text, balance numeric);
INSERT INTO accounts VALUES (1, 'asha', 100), (2, 'bo', 50);
SELECT ctid, xmin, xmax, * FROM accounts;
-- ctid (0,1) and (0,2): page 0, line pointers 1 and 2
-- xmin is the id of the transaction that created each row; xmax is 0
UPDATE accounts SET balance = 70 WHERE id = 1;
SELECT ctid, * FROM accounts WHERE id = 1;
-- ctid (0,3): the update wrote a new version; the old one is still on the page
```

That last line is the heart of PostgreSQL's concurrency design, **MVCC** (multi-version concurrency control): an `UPDATE` never overwrites a row in place, it writes a new version and marks the old one as deleted by the updating transaction. Readers that started earlier keep seeing the old version. The topic MVCC (below) covers the rules in depth.

**TOAST.** A tuple must fit in a page, so large values (long text, JSON documents, arrays) are handled by **TOAST**, "the oversized-attribute storage technique". When a row exceeds about 2 kB, PostgreSQL first tries to compress the large columns (with `pglz`, or `lz4` if you set `default_toast_compression`), and if that is not enough it moves them to a separate TOAST table in chunks, leaving a pointer behind. Queries that do not select the large column never read it, which is good; queries that do pay for extra lookups and decompression.

**The write-ahead log.** Changing a page in memory is fast, but writing every changed 8 kB page to disk at commit would be slow and random. Instead PostgreSQL writes a compact description of each change to the **write-ahead log (WAL)**, a sequential file split into 16 MB segments, and only guarantees that the *log* is on disk when a transaction commits. The changed data pages ("dirty pages") are written later by the checkpointer. If the server crashes, recovery replays the WAL from the last **checkpoint** (a point at which all earlier changes are known to be on disk) and the database is consistent again.

[[fig:databases_pgwrite]]

The WAL is also what makes **replication** and **change data capture** possible. Physical replicas replay the same WAL byte for byte. **Logical decoding** turns WAL records back into row-level inserts, updates and deletes, which is what Datastream, Debezium and PostgreSQL's own logical replication read. As a data engineer you will meet replication slots (a slot makes the server keep WAL until the consumer has read it) and the setting `wal_level = logical`.

> **Pitfall:** A logical replication slot whose consumer has stopped (a paused CDC pipeline, a deleted Debezium connector) makes PostgreSQL keep WAL forever. The disk fills and the primary database stops. Monitor `pg_replication_slots` and drop slots you no longer use.

**Indexes.** PostgreSQL has six built-in index types. The ones you will use:

| Index | Good for | Example |
|---|---|---|
| B-tree (default) | equality, ranges, sorting, `LIKE 'abc%'` | `CREATE INDEX ON orders (customer_id, created_at)` |
| GIN (generalized inverted) | values that contain many keys: JSONB, arrays, full-text | `CREATE INDEX ON events USING gin (payload jsonb_path_ops)` |
| GiST (generalized search tree) | overlap and nearness: geometry, ranges, PostGIS | `CREATE INDEX ON bookings USING gist (during)` |
| BRIN (block range) | huge tables naturally ordered by a column, such as time | `CREATE INDEX ON logs USING brin (ts)` |
| Hash, SP-GiST | equality only; space-partitioned data | rarely needed |

A B-tree index stores keys in sorted order with pointers (ctids) back to the heap. Because PostgreSQL tables are heaps, every index is a **secondary index**: an index lookup finds the ctid, then visits the heap page to fetch the row and check that this version is visible. An **index-only scan** can skip the heap when the visibility map says the page is all-visible. A **BRIN** index is tiny: it stores only the minimum and maximum value per range of pages (128 pages by default), which works when values on neighbouring pages are similar.

**The planner.** For each query the planner builds several possible plans (sequential scan or index scan, nested loop, hash or merge join, which table first) and estimates the cost of each from **statistics** gathered by `ANALYZE`: row counts, null fractions, the most common values and histograms per column. Costs are in abstract units where reading a page sequentially costs `seq_page_cost = 1.0` and a random page costs `random_page_cost = 4.0` (many SSD-backed servers lower this to about 1.1). The cheapest estimated plan wins.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM orders WHERE customer_id = 42 ORDER BY created_at DESC LIMIT 10;
-- Read the plan from the innermost node outwards. For each node compare
-- "rows=" (estimate) with "actual rows=": a large gap means stale or missing
-- statistics, and is the most common root cause of a bad plan.
```

> **Tip:** When a query is slow, run `EXPLAIN (ANALYZE, BUFFERS)` before changing anything. `shared hit` means pages found in memory, `read` means pages fetched from disk. A plan that reads millions of buffers to return ten rows is missing an index or using the wrong one.

**VACUUM.** Because updates and deletes leave old versions behind, something must reclaim them. **VACUUM** scans tables, removes tuples that no running transaction can still see, records the free space, updates the visibility map and **freezes** old rows. **Autovacuum** runs it automatically: by default a table is vacuumed when dead tuples exceed 50 plus 20% of its rows (`autovacuum_vacuum_threshold` and `autovacuum_vacuum_scale_factor`). Plain VACUUM does not shrink the file, it makes space reusable; `VACUUM FULL` rewrites the table and takes an exclusive lock, so avoid it on busy tables (the extension `pg_repack` does the same online).

::: deeper
**HOT updates.** If an update changes no indexed column and the new version fits on the same page, PostgreSQL makes a **heap-only tuple (HOT)** update: the indexes keep pointing at the old line pointer, which redirects to the new version. No index entries are written, which saves a lot of I/O on update-heavy tables. Leaving free space on each page with `ALTER TABLE ... SET (fillfactor = 90)` makes HOT updates more likely. `pg_stat_user_tables.n_tup_hot_upd` shows how often it happens.

**Full-page writes.** The first time a page changes after a checkpoint, PostgreSQL writes the whole 8 kB page into the WAL, not just the change. This protects against **torn pages** (a crash halfway through writing a page). It is also why WAL volume spikes right after each checkpoint, and why spacing checkpoints further apart (`checkpoint_timeout`, default 5 minutes; `max_wal_size`, default 1 GB) reduces WAL volume.

**Durability knobs.** `synchronous_commit = off` lets a commit return before its WAL reaches disk: a crash can lose the last few hundred milliseconds of commits, but the database stays consistent. It is a reasonable choice for data you can regenerate. `fsync = off` is never reasonable outside a throwaway test, because a crash can corrupt the database.

**Transaction ID wraparound.** Transaction ids are 32-bit counters, so after about 2 billion transactions they wrap around. To keep old rows visible, VACUUM **freezes** them (marks them as older than every transaction). If freezing falls too far behind, PostgreSQL forces an aggressive anti-wraparound vacuum (`autovacuum_freeze_max_age`, default 200 million) and, as a last resort, stops accepting writes. Long-running transactions and abandoned replication slots are the usual culprits.

**Recent versions.** PostgreSQL 17 (2024) reworked VACUUM's memory use and added incremental backups. PostgreSQL 18 (2025) added an asynchronous I/O subsystem (`io_method`), B-tree **skip scan** (a multicolumn index can be used even without a condition on its first column), `uuidv7()` for time-ordered keys, and statistics that survive a major-version upgrade.

**Google Cloud.** Cloud SQL for PostgreSQL runs standard PostgreSQL as a managed service. AlloyDB for PostgreSQL keeps the PostgreSQL front end but replaces the storage layer with a distributed, log-based one (the database ships WAL to a storage service that builds pages itself), and adds a columnar engine for analytical queries.

> **Interview:** "Why does PostgreSQL need VACUUM when MySQL does not run it?" Because PostgreSQL keeps old row versions inside the table itself, so something has to remove them later. InnoDB keeps old versions in a separate undo log and overwrites rows in place; its background **purge** thread does the equivalent cleanup.
:::

::: check
?? Why does PostgreSQL write to the WAL before writing data pages?
Appending a small log record sequentially is much faster than writing random 8 kB pages, and once the log is on disk the change can always be replayed after a crash. Data pages can then be written lazily by the checkpointer.

?? What is TOAST, and when does it matter?
The mechanism that compresses large column values and moves them to a side table when a row exceeds about 2 kB. It matters for wide JSON or text columns: selecting them costs extra reads and decompression, and selecting only the small columns avoids that cost.

?? A query's plan shows `rows=12` estimated but `actual rows=480000`. What is the first thing to check?
Statistics. Run `ANALYZE` on the table, and if the estimate is still wrong consider raising the statistics target for that column or adding extended statistics for correlated columns.
:::

::: resources
- Docs: [PostgreSQL Internals](https://www.postgresql.org/docs/current/internals.html) · the official chapters on the planner, the WAL, storage layout, index access methods and TOAST
- Book: [The Internals of PostgreSQL](https://www.interdb.jp/pg/) · Hironobu Suzuki's free online book, with clear diagrams of every subsystem
- Book: [PostgreSQL 14 Internals](https://postgrespro.com/community/books/internals) · Egor Rogov's free book on MVCC, buffer cache, WAL, locks, planning and every index type
- Book: [Database Internals](https://www.oreilly.com/library/view/database-internals/9781492040330/) · Alex Petrov; storage engines and distributed systems from first principles
- Docs: [Using EXPLAIN](https://www.postgresql.org/docs/current/using-explain.html) · how to read plans, with worked examples
- Docs: [AlloyDB for PostgreSQL overview](https://cloud.google.com/alloydb/docs/overview) · how Google's PostgreSQL-compatible database separates compute from storage
:::

### Storage Engines — B-Tree vs. LSM

::: plain
There are two classic ways to keep a large sorted collection on disk. A **B-tree** is like a filing cabinet with labelled drawers and folders: to change a document you find its folder and edit it in place. An **LSM tree** (log-structured merge tree) is like dropping every new document into an in-tray, and every so often sorting the in-tray and merging it into the archive boxes. The filing cabinet is fast to look things up in; the in-tray is fast to write to. Every database storage engine sits somewhere between the two.
:::

A **storage engine** is the part of a database that stores and retrieves keys and values on disk. SQL, transactions and replication sit on top. Two designs dominate.

**B-trees.** A B-tree is a balanced tree of fixed-size pages (8 kB in PostgreSQL, 16 kB in MySQL's InnoDB). The root and inner pages hold keys and pointers to child pages; the leaf pages hold the keys with their row data or row pointers. Each page holds hundreds of keys, so the tree is very shallow: with 500 keys per page, three levels address 500 × 500 × 500 = 125 million keys, and the top two levels usually sit in memory. A lookup is a handful of page reads.

To insert or update, the engine finds the right leaf and modifies it **in place**. If the leaf is full it **splits** into two half-full pages and adds a pointer to the parent. Crash safety comes from the write-ahead log described in the previous topic.

In **InnoDB** (MySQL) the table itself is a B-tree ordered by the primary key, called a **clustered index**, and secondary indexes store the primary key value rather than a physical pointer. In **PostgreSQL** the table is an unordered heap and every index is secondary. This is why a random UUID primary key hurts InnoDB more than PostgreSQL: every insert lands on a random leaf of the clustered index.

**LSM trees.** An LSM tree never modifies data on disk. Writes go to an in-memory sorted structure, the **memtable**, and to a sequential log for crash safety. When the memtable is full it is written out as an immutable sorted file, an **SSTable** (sorted string table). Over time many SSTables pile up, so a background process called **compaction** merges them into fewer, larger files, keeping only the newest version of each key. A delete is just another write: a marker called a **tombstone**, which hides older values until compaction drops them.

[[fig:databases_lsm]]

A read must check the memtable and then the SSTables from newest to oldest until it finds the key. To avoid opening every file, each SSTable carries a **Bloom filter** (a compact structure that can say "this key is definitely not here") and a sparse index of its blocks.

This tiny simulation, standard library only, shows the essential behaviour: buffered writes, sorted runs, newest-wins reads, tombstones and compaction.

```python
memtable, runs, LIMIT = {}, [], 3          # runs: newest first, each a sorted list

def put(key, value):
    memtable[key] = value
    if len(memtable) >= LIMIT:              # memtable full: flush a sorted run
        runs.insert(0, sorted(memtable.items()))
        memtable.clear()

def get(key):
    if key in memtable:
        return memtable[key]
    for run in runs:                        # newest run wins
        for k, v in run:
            if k == key:
                return v
    return None

def compact():
    merged = {}
    for run in reversed(runs):              # oldest first, newer values overwrite
        merged.update(run)
    runs[:] = [sorted((k, v) for k, v in merged.items() if v is not None)]

for i, k in enumerate(["a", "b", "c", "a", "d", "b"]):
    put(k, i)
put("c", None)                              # a delete is a write: a tombstone
print(len(runs), "runs, memtable", memtable)   # 2 runs, memtable {'c': None}
print(get("a"), get("b"), get("c"))            # 3 5 None
put("e", 7); put("f", 8)                       # flush the tombstone too
compact()
print(runs)          # [[('a', 3), ('b', 5), ('d', 4), ('e', 7), ('f', 8)]]
```

**The three amplifications.** Engines are compared by how much extra work they do per unit of useful work:

- **Write amplification**: bytes written to disk divided by bytes the application wrote. A B-tree rewrites a whole 8 or 16 kB page (plus WAL) to change a 100-byte row. An LSM tree writes each byte once to the log, once at flush, and again every time compaction rewrites it into a lower level.
- **Read amplification**: how many pages or files a read touches. A B-tree touches one path from root to leaf. An LSM tree may check several files, mitigated by Bloom filters and caching.
- **Space amplification**: bytes on disk divided by the size of the live data. B-trees waste space in half-empty pages after splits. LSM trees hold obsolete versions and tombstones until compaction removes them.

| | B-tree | LSM tree |
|---|---|---|
| Write pattern | random in-place page writes | sequential appends and merges |
| Best at | point reads, range scans, read-heavy OLTP | write-heavy and ingest-heavy workloads |
| Read cost | predictable, one root-to-leaf path | variable, depends on levels and filters |
| Space | page fragmentation | stale versions until compacted; compresses well |
| Background work | little (page splits, vacuum in PostgreSQL) | constant compaction, which competes for I/O |
| Examples | PostgreSQL, InnoDB, SQL Server, Oracle, SQLite | RocksDB, LevelDB, Cassandra, ScyllaDB, HBase, Bigtable |

> **Pitfall:** "LSM is for writes, B-tree is for reads" is a starting point, not a law. A well-tuned LSM engine with Bloom filters and a block cache serves point reads very quickly, and a B-tree on fast NVMe handles heavy writes. Benchmark your real access pattern before choosing.

> **Tip:** Many systems you already use are LSM trees inside: Bigtable, HBase, Cassandra, the RocksDB state stores of Flink and Kafka Streams, and CockroachDB and TiDB through Pebble and TiKV. When they slow down mysteriously, compaction is the first suspect.

::: deeper
**Compaction styles.** **Leveled compaction** (LevelDB, RocksDB's default) keeps levels L1, L2, ... each about 10 times larger than the one above, and within a level the files do not overlap in key range. A read checks at most one file per level, and space amplification stays near 1.1, but a byte may be rewritten several times per level on its way down, so write amplification is high. **Size-tiered compaction** (Cassandra's long-time default, RocksDB's "universal" style) merges files of similar size together: far fewer rewrites, but a key can live in many files, and a big merge temporarily needs as much free space as the data it merges.

**The RUM conjecture.** Athanassoulis and colleagues (2016) argued that an access method can optimise at most two of read overhead, update overhead and memory (space) overhead, at the expense of the third. B-trees and LSM trees are two points on that triangle, and compaction settings move an LSM tree around it.

**Bε-trees and other hybrids.** Fractal-tree and Bε-tree designs keep a B-tree shape but buffer writes in inner nodes and push them down in batches, getting LSM-like write cost with B-tree-like reads. WiredTiger (MongoDB's engine) is a B-tree that writes pages copy-on-write. Key-value separation (WiscKey, RocksDB's BlobDB) keeps large values out of the LSM tree to cut compaction cost.

**SSDs change the maths.** Random reads are cheap on SSDs, which helps B-trees, but SSDs wear out with writes and do their own internal garbage collection, which punishes high write amplification. The next section, Storage internals and hardware, covers this in depth.

> **Interview:** "Why does Cassandra write so fast?" Every write is an append to the commit log plus an insert into an in-memory memtable, with no read-before-write and no random disk I/O. The cost is paid later, in compaction and in reads that may have to merge several SSTables.
:::

::: check
?? Why is an LSM tree's write path faster than a B-tree's on the same hardware?
It turns random writes into sequential ones: writes are buffered in memory and appended to a log, then flushed as whole sorted files. A B-tree must find and modify the correct page in place.

?? What is a tombstone, and why can it make reads slow?
A marker written to record a delete in an LSM tree. Until compaction removes it, a read or range scan must still find and skip it, so a table with many deletes can make scans read huge numbers of tombstones.

?? Which costs more space on disk temporarily, leveled or size-tiered compaction, and why?
Size-tiered: merging several large files into one needs room for the inputs and the output at the same time, up to the size of the data being merged.
:::

::: resources
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · Kleppmann and Riccomini; chapter "Storage and Retrieval" is the clearest introduction to B-trees and LSM trees
- Book: [Database Internals](https://www.oreilly.com/library/view/database-internals/9781492040330/) · Alex Petrov; part one covers B-tree variants and LSM trees in detail
- Paper: [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) · O'Neil and colleagues, 1996, the original LSM paper
- Paper: [Designing Access Methods: The RUM Conjecture](https://openproceedings.org/2016/conf/edbt/paper-12.pdf) · the read, update and memory trade-off behind every engine
- Docs: [RocksDB Wiki: Compaction](https://github.com/facebook/rocksdb/wiki/Compaction) · leveled versus universal compaction in a production engine
:::

### MVCC — Multi-Version Concurrency Control

::: plain
Imagine a shared document that many people edit at once. Instead of locking it while someone types, the system quietly keeps every saved version, each stamped with who saved it and when. When you open the document, you are shown the newest version that was finished before you started reading, so you never see someone's half-typed sentence, and nobody has to wait for you to finish reading. That is **MVCC**: readers get a consistent snapshot, writers create new versions, and old versions are thrown away when nobody needs them any more.
:::

Without MVCC, a database protects consistency with **locks**: a reader takes a shared lock, a writer an exclusive one, and they wait for each other. Long reports would block updates, and updates would block reports. **Multi-version concurrency control (MVCC)** removes most of that waiting with one rule: **readers never block writers and writers never block readers**. Only two writers touching the same row still have to wait for each other.

**Versions and snapshots in PostgreSQL.** Each transaction gets a **transaction id (xid)**. Each tuple carries two hidden fields:

- `xmin`: the xid of the transaction that created this version.
- `xmax`: the xid of the transaction that deleted or replaced it (0 if none).

An `UPDATE` sets `xmax` on the old version and inserts a new version with `xmin` set to its own xid. A `DELETE` only sets `xmax`.

When a statement or transaction starts, it takes a **snapshot**, which records three things: `xmin` (every xid below this has finished), `xmax` (every xid from this upwards had not started yet) and the list of xids that were still in progress. A tuple version is **visible** to the snapshot if its creator had committed before the snapshot was taken and its deleter (if any) had not. Commit status itself lives in a small structure called the **commit log** (`pg_xact`), and PostgreSQL caches the answer on the tuple as **hint bits** so later readers do not have to look it up.

[[fig:databases_mvcc]]

The following simulation applies exactly these rules to two versions of one row:

```python
# Each row version records who created it (xmin) and who deleted it (xmax).
versions = [
    {"id": 1, "balance": 100, "xmin": 90,  "xmax": 105},   # old version
    {"id": 1, "balance": 70,  "xmin": 105, "xmax": None},  # new version
]

def visible(v, snap):
    """snap = (xmin, xmax, in_progress): the PostgreSQL-style snapshot."""
    s_xmin, s_xmax, in_progress = snap
    def committed_before_snapshot(x):
        return x is not None and x < s_xmax and x not in in_progress
    # the creator must be visible, and the deleter (if any) must not be
    return committed_before_snapshot(v["xmin"]) and not committed_before_snapshot(v["xmax"])

old_reader = (100, 105, set())     # snapshot taken before transaction 105 started
new_reader = (106, 106, set())     # snapshot taken after 105 committed
during     = (100, 106, {105})     # snapshot taken while 105 was still running

for name, snap in [("old_reader", old_reader), ("new_reader", new_reader), ("during", during)]:
    print(name, [v["balance"] for v in versions if visible(v, snap)])
# old_reader [100]
# new_reader [70]
# during [100]
```

(The real implementation also checks whether transactions aborted, and treats a transaction's own changes as visible to itself; the simulation assumes everything listed committed.)

**Isolation levels.** The SQL standard defines isolation levels by the anomalies they prevent. MVCC implements them by choosing *when* to take the snapshot:

| PostgreSQL level | Snapshot taken | Prevents | Still allows |
|---|---|---|---|
| Read Committed (default) | at the start of each statement | dirty reads | non-repeatable reads, lost updates in read-modify-write code, write skew |
| Repeatable Read | once, at the first statement of the transaction | the above plus non-repeatable reads, phantoms, lost updates | write skew |
| Serializable | once, plus tracking of read/write dependencies | all of the above, including write skew | nothing; some transactions fail and must be retried |

PostgreSQL's Repeatable Read is what the literature calls **snapshot isolation**. If two transactions update the same row, the second to commit fails with `could not serialize access due to concurrent update`; the application must retry it.

**Write skew.** Snapshot isolation still allows a subtle anomaly. The classic example: a hospital rule says at least one doctor must be on call. Two doctors, both on call, each ask to go off call at the same moment.

```sql
-- PostgreSQL, both sessions at REPEATABLE READ
-- Session A                                   -- Session B
BEGIN;                                         BEGIN;
SELECT count(*) FROM on_call WHERE on_duty;    SELECT count(*) FROM on_call WHERE on_duty;
-- 2, so it is safe for me to leave            -- 2, so it is safe for me to leave
UPDATE on_call SET on_duty = false             UPDATE on_call SET on_duty = false
 WHERE doctor = 'alice';                        WHERE doctor = 'bob';
COMMIT;                                        COMMIT;
-- Both commit: they updated different rows, so no write conflict was detected.
-- Now nobody is on call.
```

Each transaction read a consistent snapshot and wrote a row the other did not touch, so snapshot isolation sees no conflict. Fixes: run at `SERIALIZABLE` (PostgreSQL's **serializable snapshot isolation**, SSI, detects the dangerous read/write pattern and aborts one transaction), lock the rows you read with `SELECT ... FOR UPDATE`, or turn the rule into a constraint the database enforces.

> **Pitfall:** The default Read Committed level takes a new snapshot for every statement, so code that reads a value, computes in the application and writes it back (`SELECT balance` then `UPDATE ... SET balance = <computed>`) can lose concurrent updates. Use `UPDATE accounts SET balance = balance - 30 WHERE id = 1` (atomic in one statement), `SELECT ... FOR UPDATE`, or a higher isolation level with retries.

**The cost: cleanup.** Old versions must be removed once no snapshot can see them. PostgreSQL's VACUUM does this, but only for versions older than the oldest snapshot still in use, called the **xmin horizon**. One forgotten session left in an open transaction (`idle in transaction`), a long analytical query on the primary, or a stale replication slot holds the horizon back, and every table starts to **bloat** with dead tuples.

```sql
-- Find what is holding back cleanup
SELECT pid, state, xact_start, now() - xact_start AS age, query
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL
ORDER BY xact_start
LIMIT 5;
```

> **Tip:** Set `idle_in_transaction_session_timeout` (for example to a few minutes) so abandoned transactions are closed automatically, and run long extracts against a replica rather than the primary.

::: deeper
**Where old versions live.** PostgreSQL stores every version in the table itself (versions of one row need not even sit on the same page), so tables bloat and need VACUUM, but rollback is instant. **InnoDB** and **Oracle** update rows in place and copy the old version into an **undo log**; a reader that needs an older version follows a chain of undo records backwards. Tables stay compact, but long-running readers make the undo log grow (InnoDB's "history list length") and old reads get slower as chains lengthen. SQL Server can keep row versions in `tempdb` when snapshot isolation is enabled.

**Visibility and indexes.** PostgreSQL indexes do not store visibility information, so an index scan must visit the heap to check each version, unless the visibility map says the page is all-visible. This is why index-only scans work best on tables that are vacuumed often.

**SSI internals.** PostgreSQL's serializable level (since 9.1) tracks "rw-antidependencies": transaction T1 read something that T2 later wrote. A cycle with two consecutive such edges is the signature of a possible anomaly, and one transaction is aborted with a serialization failure. It uses predicate locks (`SIReadLock` in `pg_locks`) that never block anyone. False positives happen, so retry logic is required.

**MVCC in distributed databases.** Spanner, CockroachDB and TiDB all use multi-version storage keyed by timestamp rather than xid, which makes consistent snapshots across machines possible and enables "read as of 10 seconds ago" queries. BigQuery's time travel and Iceberg snapshots apply the same idea at table level.

> **Interview:** "Explain write skew and how you would prevent it." Two transactions read an overlapping set of data, each makes a decision based on what it read, and each writes a different row, so neither conflicts directly, but together they break a rule. Prevent it with serializable isolation (with retries), explicit row locks on what you read, or a database constraint.
:::

::: check
?? Under MVCC, why can a long report run without blocking updates to the same table?
The report reads from its snapshot, seeing the row versions that were committed when it started. Updates create new versions instead of overwriting the old ones, so neither has to wait for the other.

?? Why can one idle session in an open transaction cause table bloat across the whole database?
VACUUM can only remove versions that no snapshot can see. An open transaction holds an old snapshot (the xmin horizon), so every dead version created since then must be kept, in every table.

?? Two transactions at Repeatable Read each update a different row based on a count they both read. Can both commit in PostgreSQL? What changes at Serializable?
Yes, both can commit at Repeatable Read: this is write skew, and no write conflict is detected. At Serializable, PostgreSQL detects the dangerous read/write dependency and aborts one of them with a serialization failure, which the application retries.
:::

::: resources
- Docs: [PostgreSQL: Concurrency Control](https://www.postgresql.org/docs/current/mvcc.html) · the official chapter on MVCC, isolation levels and explicit locking
- Book: [The Internals of PostgreSQL: Concurrency Control](https://www.interdb.jp/pg/pgsql05.html) · step-by-step diagrams of xmin, xmax, snapshots and visibility checks
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · the "Transactions" chapter on snapshot isolation, lost updates and write skew
- Paper: [A Critique of ANSI SQL Isolation Levels](https://arxiv.org/abs/cs/0701157) · Berenson and colleagues; defines snapshot isolation and write skew
- Paper: [Serializable Snapshot Isolation in PostgreSQL](https://drkp.net/papers/ssi-vldb12.pdf) · Ports and Grittner; how PostgreSQL's serializable level works
- Blog: [Hermitage: testing transaction isolation levels](https://github.com/ept/hermitage) · Martin Kleppmann's tests showing which anomalies each database really allows
:::

### RocksDB — LSM-Tree Deep Dive

::: plain
RocksDB is a small library, not a server: a program links it in and gets a fast, reliable key-value store on local disk. Think of it as the engine block that many different cars share. Kafka Streams, Apache Flink, TiKV, MyRocks and many others put their own body and controls on top, but underneath they all use RocksDB to keep sorted keys on SSDs. Understanding it once explains the behaviour of all of them.
:::

RocksDB started at Facebook in 2012 as a fork of Google's LevelDB, rebuilt for servers with fast flash storage and many cores. It is an **embedded** key-value store: there is no network protocol, just an API (`Put`, `Get`, `Delete`, `Merge`, iterators, write batches) that a program calls in-process. Keys and values are arbitrary byte strings, kept sorted by key.

**The write path.** A write goes to two places:

1. The **write-ahead log (WAL)**, appended sequentially, so the write survives a crash.
2. The **memtable**, an in-memory sorted structure (a skip list by default).

When the memtable reaches `write_buffer_size` (64 MB by default) it becomes **immutable** and a new one takes its place. A background thread **flushes** the immutable memtable to disk as an **SST file** (sorted string table) in **level 0 (L0)**. Files in L0 may overlap in key range, because each is just a dump of one memtable.

**The levels.** Below L0, each level L1, L2, ... Ln holds files whose key ranges do *not* overlap, and each level has a target size roughly 10 times the level above (`max_bytes_for_level_multiplier = 10`). **Compaction** picks a file from one level, merges it with the overlapping files in the next level, and writes new files there, dropping overwritten values and expired tombstones. With dynamic level sizing (on by default in recent releases), RocksDB sizes levels from the bottom up so that about 90% of data sits in the last level and space amplification stays near 1.1.

**The read path.** A `Get` checks the active memtable, then the immutable memtables, then every L0 file (newest first), then at most one file per lower level. For each file it first consults the file's **Bloom filter**; if the filter says "not here", the file is skipped without any disk read. If the filter says "maybe", RocksDB uses the file's **index block** to find the right **data block** (4 kB by default, compressed), loading it into the **block cache** (an in-memory cache of uncompressed blocks shared by all files).

**Bloom filters in numbers.** A Bloom filter with `b` bits per key and `k` hash functions has a false-positive rate of about (1 − e^(−k/b))^k. RocksDB's usual setting is 10 bits per key:

```python
import math

def false_positive_rate(bits_per_key, k):
    # classic Bloom filter estimate: (1 - e^(-k/b))^k
    return (1 - math.exp(-k / bits_per_key)) ** k

for b in (5, 10, 16):
    k = round(b * math.log(2))             # the best number of hash functions
    print(b, "bits/key", k, "hashes", f"{false_positive_rate(b, k):.2%}")
# 5 bits/key 3 hashes 9.18%
# 10 bits/key 7 hashes 0.82%
# 16 bits/key 11 hashes 0.05%
```

At 10 bits per key, fewer than 1 in 100 unnecessary file reads get through. For a billion keys that costs about 1.25 GB of filter memory, which is why filter memory is a real sizing concern. RocksDB also offers **Ribbon filters**, which give the same accuracy in about 30% less memory at some extra CPU cost.

**Column families.** A RocksDB database can hold several **column families**: separate key spaces with their own memtables, SST files and options, but a shared WAL so a write batch across them is atomic. Kafka Streams and Flink, for example, use one column family per state store or per piece of operator state.

**Compaction styles.**

| Style | How it works | Strength | Weakness |
|---|---|---|---|
| Leveled (default) | merge one level's file into the overlapping files of the next | low space and read amplification | high write amplification |
| Universal | merge whole sorted runs of similar size | low write amplification | temporary space up to 2×, more runs to read |
| FIFO | drop the oldest files when a size or age limit is reached | almost free; suits caches and short-lived time series | not a general store: old data simply disappears |

**Write stalls.** If flushes and compactions cannot keep up with incoming writes, L0 files pile up. RocksDB starts compacting L0 at `level0_file_num_compaction_trigger = 4` files, deliberately **slows** writes at 20 files and **stops** them at 36. It also stalls when too many immutable memtables wait to be flushed, or when too many bytes are pending compaction. A stall shows up upstream as mysterious latency spikes: a Flink job's checkpoints get slow, or a Kafka Streams application's processing rate drops.

> **Pitfall:** Giving RocksDB more memory than the container allows. Memtables, the block cache, filters and index blocks all live outside the JVM heap in Flink and Kafka Streams, so a JVM sized only by `-Xmx` can be killed by the operating system for exceeding its container limit. Use the frameworks' managed-memory settings, which cap RocksDB with a shared cache and write buffer manager.

> **Tip:** Every RocksDB instance writes a `LOG` file in its directory with all options and periodic statistics, including write-stall counts and compaction throughput. It is the first file to read when a RocksDB-backed system misbehaves.

::: deeper
**Tuning in order of impact.** (1) Give the block cache enough memory for the hot set, and keep index and filter blocks in it (`cache_index_and_filter_blocks`) with high priority so they are not evicted. (2) Choose compression per level: fast LZ4 or none on upper levels, stronger ZSTD on the bottom level where most data lives. (3) Size `write_buffer_size` and `max_write_buffer_number` for your write rate. (4) Give background jobs enough threads (`max_background_jobs`). (5) Use a rate limiter so compaction does not saturate the disk. The RocksDB wiki's Tuning Guide walks through each.

**Merge operator.** Instead of read-modify-write, an application can write a **merge operand** (for example "+1") and let RocksDB combine operands lazily during reads and compaction. Counters and list appends become blind writes.

**Prefix seeks and compaction filters.** A **prefix extractor** builds Bloom filters on key prefixes so that range scans within a prefix (all events for one user) also benefit from filtering. A **compaction filter** lets the application drop or rewrite entries during compaction, which is how time-to-live is implemented in several systems.

**BlobDB.** Large values make compaction expensive, because they are rewritten at every level. Integrated BlobDB (since RocksDB 6.18) stores values above a size threshold in separate blob files and keeps only references in the LSM tree, following the WiscKey paper's key-value separation.

**Where you meet it.** Flink's RocksDB state backend (with incremental checkpoints that upload only new SST files), Kafka Streams state stores, Spark Structured Streaming's RocksDB state store provider, TiKV (TiDB's storage layer), MyRocks (MySQL on RocksDB, used at Meta), and as the metadata store of systems such as Apache Ozone. CockroachDB replaced RocksDB with **Pebble**, a Go reimplementation with a compatible file format.

> **Interview:** "Your Flink job's checkpoints are getting slower as state grows. What do you check?" State size and RocksDB metrics: whether compaction keeps up (pending compaction bytes, L0 file count, stalls), whether incremental checkpoints are enabled, disk throughput on the task managers, and whether the block cache is too small for the hot keys.
:::

::: check
?? Why can files in level 0 overlap in key range, while files in L1 and below cannot?
L0 files are direct dumps of memtables, each covering whatever keys were written in that period. Compaction into L1 and below merges and splits data so that each level is one sorted, non-overlapping run, which lets a read check at most one file per level.

?? What does a Bloom filter save on a RocksDB read, and what does it cost?
It lets a read skip SST files that certainly do not contain the key, avoiding disk reads. It costs memory (about 10 bits per key for a 1% false-positive rate) and a little CPU.

?? Writes to a RocksDB-backed service suddenly have latency spikes and the LOG shows many stall messages. What is happening?
Flushes or compactions are not keeping up: too many L0 files, too many immutable memtables or too many pending compaction bytes, so RocksDB throttles writes. Look at disk throughput, background thread counts and compaction settings.
:::

::: resources
- Docs: [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki) · the official documentation: architecture, compaction, filters, tuning and tools
- Docs: [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) · which options matter and how to reason about amplification
- Blog: [Integrated BlobDB](https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html) · the RocksDB team on key-value separation for large values
- Paper: [RocksDB: Evolution of Development Priorities in a Key-value Store Serving Large-scale Applications](https://dl.acm.org/doi/10.1145/3483840) · the RocksDB team's own retrospective on design choices
- Book: [Database Internals](https://www.oreilly.com/library/view/database-internals/9781492040330/) · Alex Petrov; LSM trees, compaction and Bloom filters in depth
- Docs: [Flink: Using the RocksDB state backend](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/) · how a stream processor uses RocksDB, with memory configuration
:::

## Distributed and NoSQL

A single PostgreSQL server goes a long way: a large machine with fast disks handles tens of thousands of transactions per second and many terabytes. But some workloads outgrow one machine, need to survive the loss of a whole data centre, or serve users on several continents with low latency. This group is about databases that spread data across many machines.

Two families appear. **Distributed SQL** (also called NewSQL) keeps the relational model and full ACID transactions and hides the distribution from you. **NoSQL** stores such as Cassandra relax the relational model or consistency guarantees to get simpler scaling and very high write throughput. Between them sits the general technique both rely on, **sharding**, and two specialised families that every data platform eventually meets: search engines and time-series databases. All of them build on the storage engines and MVCC ideas from the previous group.

### NewSQL — TiDB, CockroachDB, Spanner

::: plain
A distributed SQL database is like a chain of shops that behaves as if it were one shop. Stock is spread across many branches, but when you buy something the whole chain agrees on the sale at once, and no branch can sell the same item twice. Under the hood the data is cut into small pieces, each piece is copied to several machines, and the copies vote on every change, so the database keeps working when a machine dies and still gives the same answers a single server would.
:::

**The problem.** Sharding a relational database by hand (the topic Database Sharding Strategies, below) works, but you lose cross-shard transactions, joins and global constraints, and the application has to know where data lives. **Distributed SQL** databases do the sharding, replication and transaction coordination for you, behind a normal SQL interface.

They all share the same layered design:

1. A **SQL layer** that parses, plans and runs queries, and is usually stateless so you can add nodes freely.
2. A **distributed, transactional key-value layer** underneath, where each table row and index entry becomes a key-value pair, and the key space is split into **ranges** (also called splits, regions or tablets).
3. **Consensus replication** per range: each range has several replicas (usually 3 or 5) that agree on every write using a consensus protocol, **Raft** or **Paxos**. A write succeeds once a majority has stored it, so a range survives the loss of a minority of its replicas.
4. **Multi-version storage** keyed by timestamp, so that reads can see a consistent snapshot across machines.

**Consensus in one paragraph.** In Raft, the replicas of a range elect a **leader**. The leader appends each write to its log and sends it to the followers; once a majority has acknowledged it, the entry is **committed** and applied. If the leader fails, the followers stop hearing its heartbeats and elect a new one with an up-to-date log. With 3 replicas the range tolerates 1 failure; with 5 it tolerates 2. The cost is that every write needs a network round trip to a majority, so write latency depends on how far apart the replicas are.

**Transactions across ranges.** A transaction that touches several ranges uses a form of **two-phase commit**: write provisional values ("intents" or locks) in every range, then atomically flip a single transaction record to committed. Timestamps order the transactions. How each system gets those timestamps is the main difference between them:

| | Spanner | CockroachDB | TiDB |
|---|---|---|---|
| Origin | Google, internal from about 2012, Cloud Spanner 2017 | Cockroach Labs, open core, inspired by Spanner | PingCAP, open source (Apache 2.0) |
| SQL dialect | GoogleSQL and a PostgreSQL interface | PostgreSQL wire protocol and dialect | MySQL wire protocol and dialect |
| Replication | Paxos per split | Raft per range | Raft per region (in TiKV) |
| Timestamps | TrueTime: GPS and atomic clocks with a known error bound | hybrid logical clocks, with a maximum clock offset | a central timestamp oracle in the Placement Driver (PD) |
| Default isolation | serializable, plus external consistency | serializable (read committed also available) | snapshot isolation (called repeatable read) |
| Analytics | Data Boost: separate compute for analytical reads | follower reads | TiFlash columnar replicas (HTAP) |

**Spanner and TrueTime.** Spanner's key idea is an API, `TT.now()`, that returns an interval [earliest, latest] guaranteed to contain the true time, kept small by GPS receivers and atomic clocks in every data centre. When a transaction commits with timestamp *s*, Spanner waits until `TT.now().earliest > s` before acknowledging (**commit wait**, a few milliseconds). After that wait, any transaction that starts later anywhere in the world gets a larger timestamp. The result is **external consistency**: if transaction T1 commits before T2 starts, T2 sees T1's effects, across continents, without a central clock.

On Google Cloud, Spanner is a managed service sized in **processing units** (1,000 processing units equal one node) and sold in Standard, Enterprise and Enterprise Plus editions. It adds automatic splitting and rebalancing, regional and multi-region configurations (a multi-region instance offers a 99.999% availability SLA), and features such as Spanner Graph and vector search.

```sql
-- Spanner (GoogleSQL): interleave child rows with their parent so they share a split
CREATE TABLE Customers (
  CustomerId STRING(36) NOT NULL,
  Name       STRING(MAX),
) PRIMARY KEY (CustomerId);

CREATE TABLE Orders (
  CustomerId STRING(36) NOT NULL,
  OrderId    STRING(36) NOT NULL,
  Total      NUMERIC,
) PRIMARY KEY (CustomerId, OrderId),
  INTERLEAVE IN PARENT Customers ON DELETE CASCADE;
```

Interleaving stores each customer's orders physically next to the customer row, so a transaction on one customer usually touches one split and avoids cross-split two-phase commit.

**CockroachDB** splits data into ranges (512 MiB by default) and replicates each with Raft. One replica of each range holds a **lease** and serves reads and coordinates writes; placing leaseholders near users is a key tuning lever. It uses hybrid logical clocks and a configured maximum clock offset (500 ms by default) instead of special hardware: if a read sees a value with a timestamp inside its uncertainty window, it restarts at a higher timestamp.

**TiDB** has three components: stateless **TiDB** SQL servers, **TiKV** storage nodes (RocksDB under Raft), and the **Placement Driver**, which hands out timestamps and decides where regions live. Its transaction protocol is based on Google's **Percolator**. **TiFlash** keeps columnar replicas of selected tables, updated through Raft, so analytical queries can run on fresh data without an ETL step.

> **Pitfall:** Monotonically increasing primary keys (auto-increment integers, timestamps) send every insert to the last range, creating a write hotspot on one set of replicas. Use UUIDv4, a hashed prefix, or the database's own tools (Spanner's bit-reversed sequences, CockroachDB's hash-sharded indexes, TiDB's `AUTO_RANDOM`).

> **Tip:** Distributed SQL is not a faster PostgreSQL. A single-row write needs a consensus round trip, so latency per statement is higher than on one server. Choose it for scale, availability and geographic distribution, and design transactions to touch few ranges.

::: deeper
**Why not just 2PC?** Classic two-phase commit blocks if the coordinator dies between the phases. Distributed SQL databases put the transaction record itself in a Raft or Paxos-replicated range, so the "coordinator's decision" is durable and highly available, and any node can finish an abandoned transaction.

**Clock skew matters for correctness.** CockroachDB nodes that drift beyond the maximum offset shut themselves down rather than risk anomalies. Spanner's TrueTime bound makes the uncertainty explicit and waits it out. TiDB avoids the problem with a central oracle, which adds a round trip for timestamps but needs no clock discipline.

**Follower and stale reads.** Reading slightly old data (`AS OF SYSTEM TIME '-10s'` in CockroachDB, stale reads with a bounded staleness in Spanner) lets any replica answer without contacting the leader, which cuts latency for reads in remote regions.

**Calvin and deterministic databases.** An alternative design (Calvin, used by FaunaDB) orders transactions first through a replicated log and then executes them deterministically on every replica, avoiding 2PC entirely, at the cost of needing to know a transaction's read and write sets in advance.

**YugabyteDB** is a fourth widely used system: a PostgreSQL-compatible query layer (reusing PostgreSQL's code) on top of DocDB, a RocksDB-based store with Raft per tablet.

> **Interview:** "How does Spanner give external consistency without a single global clock?" TrueTime gives each node a bounded uncertainty interval for the current time. A transaction picks a commit timestamp and then waits out the uncertainty (commit wait) before it becomes visible, so any later transaction anywhere must receive a higher timestamp.
:::

::: check
?? A CockroachDB cluster has ranges with 3 replicas. How many replicas of a range can fail before writes to that range stop, and why?
One. Writes need a majority (2 of 3) to acknowledge. With two replicas down, no majority exists and the range becomes unavailable for writes.

?? What problem does Spanner's commit wait solve?
Clock uncertainty. By waiting until the true time is certainly past its commit timestamp, a transaction guarantees that any transaction starting afterwards gets a larger timestamp, which gives external consistency.

?? Why would you interleave a child table in its parent in Spanner?
To store the child rows physically with their parent row in the same split, so reads and transactions that touch one parent and its children stay on one split and avoid cross-split coordination.
:::

::: resources
- Docs: [Spanner documentation](https://cloud.google.com/spanner/docs) · Google's managed distributed SQL database: schema design, transactions, TrueTime and editions
- Paper: [Spanner: Google's Globally-Distributed Database](https://research.google/pubs/spanner-googles-globally-distributed-database-2/) · the OSDI 2012 paper introducing TrueTime and external consistency
- Docs: [CockroachDB Architecture Overview](https://www.cockroachlabs.com/docs/stable/architecture/overview) · the SQL, transaction, distribution, replication and storage layers explained one by one
- Docs: [TiDB Architecture](https://docs.pingcap.com/tidb/stable/tidb-architecture) · how TiDB, TiKV, PD and TiFlash fit together
- Paper: [In Search of an Understandable Consensus Algorithm (Raft)](https://raft.github.io/raft.pdf) · Ongaro and Ousterhout; the consensus protocol under CockroachDB and TiDB
- Paper: [Large-scale Incremental Processing Using Distributed Transactions and Notifications (Percolator)](https://research.google/pubs/large-scale-incremental-processing-using-distributed-transactions-and-notifications/) · the transaction design TiDB adopted
:::

### Apache Cassandra — Internals

::: plain
Cassandra is a database with no boss. Every machine in the cluster is equal, each one is responsible for a slice of the data, and each slice is copied to a few neighbours. When you write, the machine you talk to forwards the data to the right owners and tells you "done" once enough of them have it. You choose how many is "enough" for each request, trading a little consistency for speed or the other way round. Because nobody is in charge, there is nobody whose failure stops the cluster.
:::

Apache Cassandra was built at Facebook in 2008, combining the data model of Google's Bigtable (rows with many columns, stored in an LSM tree) with the cluster design of Amazon's Dynamo paper (a leaderless ring with tunable consistency). It is an Apache project used for very high write volumes across data centres: messaging, IoT telemetry, activity feeds, fraud features. Cassandra 5.0 (2024) is the current major line.

**The ring and consistent hashing.** Each row belongs to a **partition**, identified by its **partition key**. Cassandra hashes the partition key with the Murmur3 partitioner into a 64-bit **token**, and the token space forms a **ring**. Each node owns several ranges of the ring, called **virtual nodes (vnodes)**; since Cassandra 4.0 the default is 16 per node (`num_tokens`). A partition is stored on the node that owns its token and on the next nodes around the ring, up to the **replication factor (RF)**.

[[fig:databases_ring]]

Consistent hashing is what makes adding a node cheap: the new node takes over some token ranges from its neighbours, and only those partitions move. The topic Database Sharding Strategies shows the numbers.

**Replication.** You set replication per **keyspace** (Cassandra's word for a database):

```sql
-- CQL (Cassandra Query Language)
CREATE KEYSPACE shop
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_eu': 3, 'dc_us': 3};
```

`NetworkTopologyStrategy` places replicas on different racks within each data centre, so a rack failure does not lose all copies.

**Tunable consistency.** Every read and write names a **consistency level**: how many replicas must answer before the coordinator (the node the client talks to) replies. Common levels are `ONE`, `QUORUM` (a majority of all replicas), `LOCAL_QUORUM` (a majority in the local data centre) and `ALL`. The rule to remember: if the replicas written (W) plus the replicas read (R) exceed the replication factor (RF), every read overlaps at least one replica that has the latest write.

| RF | Write level | Read level | W + R > RF? | Behaviour |
|---|---|---|---|---|
| 3 | `QUORUM` (2) | `QUORUM` (2) | 4 > 3, yes | reads see the latest acknowledged write; tolerates 1 replica down |
| 3 | `ONE` (1) | `ONE` (1) | 2 > 3, no | fastest, but reads may be stale |
| 3 | `ALL` (3) | `ONE` (1) | 4 > 3, yes | fast reads, but any replica down blocks writes |

`LOCAL_QUORUM` for both reads and writes is the usual production choice: strong within a data centre, no cross-region latency on each request.

**Write path.** The coordinator sends the write to all replicas. Each replica appends it to its **commit log** and adds it to a **memtable**, then acknowledges. Memtables are flushed to immutable **SSTables**. There is no read before write: an `UPDATE` and an `INSERT` are the same operation (an **upsert**), and a write with an older timestamp loses to one with a newer timestamp (**last write wins**, per column).

**Read path.** A replica merges the row from the memtable and every SSTable that might hold it, using Bloom filters, partition indexes and a key cache to avoid disk reads. When replicas disagree, the coordinator returns the newest value and repairs the stale replicas (**read repair**).

**Data modelling: query first.** Cassandra has no joins and filters efficiently only on the primary key, so you design one table per query.

```sql
CREATE TABLE shop.orders_by_customer (
  customer_id uuid,
  order_date  date,
  order_id    timeuuid,
  total       decimal,
  PRIMARY KEY ((customer_id), order_date, order_id)
) WITH CLUSTERING ORDER BY (order_date DESC, order_id DESC);

-- Efficient: one partition, read in clustering order
SELECT * FROM shop.orders_by_customer
WHERE customer_id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2 LIMIT 20;
```

The **partition key** (`customer_id`) decides which nodes hold the data; the **clustering columns** (`order_date`, `order_id`) sort rows inside the partition. If another screen needs orders by product, you create another table and write to both.

> **Pitfall:** Unbounded partitions. A partition keyed only by `sensor_id` grows forever, and very large partitions (hundreds of megabytes) make reads, repairs and compaction slow. Add a time bucket to the key, for example `PRIMARY KEY ((sensor_id, day), ts)`.

> **Pitfall:** Deletes create **tombstones**, which are kept for `gc_grace_seconds` (10 days by default) so that every replica learns about the delete. Using Cassandra as a queue (insert, read, delete) piles up tombstones that every read must scan.

::: deeper
**Anti-entropy.** Three mechanisms keep replicas in sync. **Hinted handoff**: if a replica is down, the coordinator stores a hint and replays it when the replica returns (for a limited window, 3 hours by default). **Read repair**: fixes inconsistencies found during reads. **Repair** (`nodetool repair`, or automated with tools such as Cassandra Reaper): replicas compare Merkle trees of their data and stream differences. Running repair on every node within `gc_grace_seconds` is mandatory; otherwise a replica that missed a delete can resurrect the deleted data ("zombie" rows).

**Gossip and failure detection.** Nodes exchange state with a few random peers every second (**gossip**), and each node judges whether a peer is down with the **phi accrual failure detector**, which outputs a suspicion level rather than a yes or no.

**Compaction strategies.** **SizeTieredCompactionStrategy** (STCS, still the default if none is set) suits write-heavy tables. **LeveledCompactionStrategy** (LCS) suits read-heavy tables with updates. **TimeWindowCompactionStrategy** (TWCS) groups SSTables by time window and is ideal for time series with TTLs: whole files expire and are dropped without rewriting. Cassandra 5.0 added the **Unified Compaction Strategy** (UCS), which can behave like tiered or leveled compaction through one parameter and shards compaction work; the project recommends it for new tables.

**Cassandra 5.0 features.** Storage-attached indexes (**SAI**) make secondary indexes practical, a vector data type with approximate nearest-neighbour search, trie-based memtables and SSTable indexes that cut memory, and support for Java 17.

**Lightweight transactions.** `INSERT ... IF NOT EXISTS` and conditional updates use Paxos among the replicas for compare-and-set semantics. They cost several round trips, so use them sparingly. Work on full ACID transactions (the Accord protocol) is under way in the project but not part of 5.0.

**Running it on Google Cloud.** Google does not offer a managed Cassandra service. Teams run it on Compute Engine or on GKE with the K8ssandra operator, use a partner service such as DataStax Astra or ScyllaDB Cloud on Google Cloud, or move wide-column workloads to **Bigtable**, Google's managed descendant of the original Bigtable design.

> **Interview:** "With RF=3, why is QUORUM/QUORUM consistent while ONE/ONE is not?" Two replicas acknowledge each write and two answer each read; 2 + 2 > 3, so the two sets must share at least one replica holding the newest value, and timestamps decide which value wins. With ONE/ONE, the read may hit a replica the write has not reached yet.
:::

::: check
?? In Cassandra, what does the partition key decide, and what do clustering columns decide?
The partition key's hash (token) decides which nodes store the row. Clustering columns decide the sort order of rows within the partition, which enables efficient range reads inside it.

?? RF is 3 and writes use LOCAL_QUORUM. Reads use ONE. Is a read guaranteed to see the latest write?
No. W + R = 2 + 1 = 3, which is not greater than RF = 3, so the read might go to the one replica that has not received the write yet. Use LOCAL_QUORUM for reads too.

?? Why must repair run more often than `gc_grace_seconds`?
Tombstones are dropped after `gc_grace_seconds`. If a replica missed the delete and has not been repaired before then, it still holds the old value with no tombstone to cancel it, and the deleted data comes back.
:::

::: resources
- Docs: [Apache Cassandra documentation](https://cassandra.apache.org/doc/latest/) · architecture, CQL, data modelling and operations, for the current release
- Paper: [Cassandra: A Decentralized Structured Storage System](https://www.cs.cornell.edu/projects/ladis2009/papers/lakshman-ladis2009.pdf) · the original Facebook paper by Lakshman and Malik
- Paper: [Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) · the SOSP 2007 paper behind Cassandra's ring, hinted handoff and tunable consistency
- Book: [Cassandra: The Definitive Guide, 3rd edition](https://www.oreilly.com/library/view/cassandra-the-definitive/9781492097136/) · Carpenter and Hewitt; data modelling and internals, updated for 4.0
- Blog: [Apache Cassandra 5.0 Features: Unified Compaction Strategy](https://cassandra.apache.org/_/blog/Apache-Cassandra-5.0-Features-Unified-Compaction-Strategy.html) · the project's explanation of UCS
:::

### Database Sharding Strategies

::: plain
When one filing cabinet is full, you buy more cabinets and decide which documents go in which one. Sharding is that decision for a database: split the rows into groups and give each group its own server. The hard part is the rule you use to split. A good rule spreads the work evenly and lets most questions be answered by one cabinet. A bad rule sends everyone to the same cabinet, or forces you to open all of them to answer anything.
:::

**Sharding** (also called **horizontal partitioning**) splits one logical table across several databases, called **shards**, each holding a subset of the rows. It is different from **replication** (copies of the same data, for availability and read scaling) and from **vertical partitioning** (different tables or columns on different servers). Most large systems combine sharding and replication: each shard is itself a primary with replicas.

You shard when one machine can no longer hold the data or absorb the writes, and only then, because sharding makes everything else harder: cross-shard joins, transactions, unique constraints, schema changes and backups.

**The shard key.** Every strategy starts with a **shard key**: the column (or columns) used to route a row. Choose it from the access patterns. A good shard key has high cardinality, spreads writes evenly, and appears in the `WHERE` clause of most queries, so that each query goes to one shard. For a multi-tenant SaaS product, `tenant_id` is the classic choice; for a social app, `user_id`.

**The main strategies.**

| Strategy | Routing rule | Strengths | Weaknesses |
|---|---|---|---|
| Hash (key-based) | `hash(key) mod N`, or a hash ring | even spread, no hotspots from sequential keys | range queries hit every shard; naive modulo makes resharding expensive |
| Range | shard 1 holds keys A to F, shard 2 holds G to M ... | efficient range scans; easy to split a range | sequential keys (timestamps, auto-increments) all hit the last shard |
| Directory (lookup) | a lookup table maps each key or tenant to a shard | full control; move one tenant at a time | the directory is a critical dependency and must be cached and highly available |
| Geographic | route by region or country | data residency, low latency for local users | uneven regions; users who travel or move |

**Resharding and consistent hashing.** With `hash(key) mod N`, changing N from 4 to 5 changes the shard of almost every key, which means moving almost all the data. **Consistent hashing** places shards at many points on a hash ring and assigns each key to the next point clockwise; adding a shard moves only the keys that now fall before its points. This standard-library script measures both:

```python
import bisect, hashlib

def h(s):
    return int(hashlib.md5(s.encode()).hexdigest(), 16)

keys = [f"user:{i}" for i in range(10_000)]

# 1. hash modulo N: adding a fifth shard moves most keys
before = {k: h(k) % 4 for k in keys}
after = {k: h(k) % 5 for k in keys}
moved = sum(before[k] != after[k] for k in keys)
print(f"modulo: {moved / len(keys):.0%} of keys moved")

# 2. consistent hashing with 100 virtual nodes per shard
def ring(shards, vnodes=100):
    points = sorted((h(f"{s}#{v}"), s) for s in shards for v in range(vnodes))
    return [p for p, _ in points], [s for _, s in points]

def owner(r, key):
    hashes, owners = r
    i = bisect.bisect(hashes, h(key)) % len(hashes)   # next point clockwise
    return owners[i]

r4, r5 = ring(["s0", "s1", "s2", "s3"]), ring(["s0", "s1", "s2", "s3", "s4"])
moved = sum(owner(r4, k) != owner(r5, k) for k in keys)
print(f"consistent hashing: {moved / len(keys):.0%} of keys moved")
# modulo: 80% of keys moved
# consistent hashing: 20% of keys moved
```

20% is the ideal: the new shard should take one fifth of the data, and nothing else should move.

A simpler practical alternative is **many logical shards on few physical servers**: create, say, 4,096 logical shards up front (`hash(key) mod 4096`) and map them to physical servers with a small table. Growing means moving whole logical shards to new servers; the hash function never changes. Instagram's well-known design used thousands of logical shards (PostgreSQL schemas) spread over a few physical databases, and generated IDs that embed the logical shard: 41 bits of milliseconds since a custom epoch, 13 bits of shard ID and 10 bits of per-shard sequence. Any ID therefore says which shard holds the row and sorts roughly by time.

**Hot shards.** Even with a good key, one key can be too popular: a celebrity's account, one huge tenant, a viral product. Remedies: give the biggest tenants dedicated shards (directory-based routing makes this easy), split a hot key into sub-keys (`product_id#0` to `product_id#9`) and merge on read, cache hot reads in front of the database, and monitor per-shard load so you see skew early.

> **Pitfall:** Sharding on a timestamp or an auto-increment ID with range routing. All new writes land on the newest shard, which does all the work while the others idle. Hash the key, or put a high-cardinality column first.

> **Tip:** Before sharding, exhaust the cheaper options: a larger machine, read replicas, caching, archiving old data, and better indexes. On Google Cloud, Spanner shards automatically, and AlloyDB and Cloud SQL scale a single primary a long way; manual sharding is now a last resort for most teams.

::: deeper
**Cross-shard queries.** A query without the shard key must **scatter** to every shard and **gather** the results; its latency is that of the slowest shard, and its cost grows with the number of shards. Common fixes are a secondary "global index" table keyed by the other column, a denormalised copy of the data sharded by the other key, or sending such queries to an analytical copy in BigQuery fed by CDC.

**Cross-shard transactions.** Application-level sharding usually gives up multi-shard ACID. Designs work around it by keeping each business transaction inside one shard (co-locating a tenant's data), or by using sagas: a sequence of local transactions with compensating actions on failure.

**Middleware.** **Vitess** (built at YouTube, now a CNCF project) shards MySQL behind a proxy (`vtgate`) that routes queries using a **VSchema**, and performs online resharding. **Citus** (a PostgreSQL extension) distributes tables by a distribution column and co-locates tables that share it, so joins on that column run locally. Both make sharding largely transparent to applications.

**Rebalancing in practice.** Moving a shard online follows the same pattern everywhere: copy a snapshot, stream changes from the source's log until the copy catches up, briefly stop writes, switch routing, then resume. It is the same as an online schema migration (the last topic in this section) at the level of whole tables.

> **Interview:** "Design the sharding for a multi-tenant SaaS database." Shard by `tenant_id` so each tenant's queries and transactions stay on one shard; use a directory to map tenants to shards so large tenants can be moved or isolated; generate globally unique IDs that do not need coordination; and send cross-tenant analytics to a warehouse rather than scatter-gathering across shards.
:::

::: check
?? Why does `hash(key) mod N` make adding a shard expensive, and what avoids it?
Changing N changes the result of the modulo for most keys, so most rows must move. Consistent hashing, or a fixed large number of logical shards mapped to servers, moves only the data that must move.

?? A table is range-sharded on `created_at`. What problem will you see?
A hot shard: all new rows have the newest timestamps and go to the same shard, so it takes all the write load.

?? What is a scatter-gather query, and why does it get worse as you add shards?
A query that has no shard key, so it must be sent to every shard and the results combined. Its cost grows with the number of shards, and its latency is set by the slowest shard.
:::

::: resources
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · the "Sharding" chapter covers key-range and hash partitioning, secondary indexes and rebalancing
- Blog: [Sharding & IDs at Instagram](https://medium.com/instagram-engineering/sharding-ids-at-instagram-1cf5a71e5a5c) · logical shards and shard-aware ID generation in PostgreSQL
- Docs: [Vitess documentation](https://vitess.io/docs/) · sharding MySQL with a routing proxy, VSchema and online resharding
- Docs: [Citus documentation](https://docs.citusdata.com/) · distributed PostgreSQL with co-located tables
- Docs: [Spanner schema design best practices](https://cloud.google.com/spanner/docs/schema-design) · how to avoid hotspots when the database shards for you
- Paper: [Consistent Hashing and Random Trees](https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf) · Karger and colleagues, 1997, the origin of consistent hashing
:::

### Elasticsearch — Internals & Search

::: plain
A search engine works like the index at the back of a book. Instead of reading every page to find the word "replication", you look it up in the index, which lists the pages where it appears. Elasticsearch builds such an index for every word in every document, then ranks the matching documents by how well they fit your search, so the best ones come first. It spreads the index over many machines so that it can search billions of documents in a fraction of a second.
:::

**Elasticsearch** is a distributed search and analytics engine built on **Apache Lucene**, a Java search library. You send it JSON documents over a REST API; it indexes every field and answers full-text searches, filters and aggregations in milliseconds. Data engineers meet it as the store behind product search, log and observability platforms (the "ELK" stack: Elasticsearch, Logstash, Kibana), and increasingly vector search. **OpenSearch** is a fork made in 2021 from Elasticsearch 7.10 under the Apache 2.0 licence; the two share the core ideas below. (Elasticsearch itself added the open-source AGPL licence as an option in 2024.)

**The inverted index.** For each field, Lucene builds an **inverted index**: a sorted dictionary of every **term** (word) pointing to a **postings list** of the documents that contain it, with term frequencies and positions. Searching for `kafka alert` means looking up two terms and combining their postings lists, which is fast no matter how many documents there are.

Before indexing, text goes through an **analyzer**: a tokenizer splits it into terms, and filters lowercase them, remove stop words and reduce words to stems ("replicas" becomes "replica"). The same analyzer runs on the query, so the terms match.

**Mappings.** A **mapping** is the schema of an index. The key distinction is between `text` fields (analysed, for full-text search) and `keyword` fields (stored as one exact term, for filters, sorting and aggregations).

```json
PUT /tickets
{
  "settings": { "number_of_shards": 3, "number_of_replicas": 1 },
  "mappings": {
    "properties": {
      "title":   { "type": "text" },
      "status":  { "type": "keyword" },
      "created": { "type": "date" },
      "service": { "type": "keyword" }
    }
  }
}
```

> **Pitfall:** Relying on dynamic mapping in production. Elasticsearch guesses types from the first document it sees, so a field that once contained `"42"` becomes a string forever, and every new JSON key becomes a new field until you hit the field limit (a "mapping explosion"). Define explicit mappings, or use index templates.

**Relevance: BM25.** Matching documents are ranked by **BM25**, the default similarity since Elasticsearch 5. A term contributes more to the score when it is rare across the index (inverse document frequency, IDF), when it appears often in the document (term frequency, with diminishing returns controlled by `k1 = 1.2`), and when the document is short (length normalisation controlled by `b = 0.75`). This standard-library script builds a tiny inverted index and scores it with Lucene's BM25 formula:

```python
import math
from collections import Counter, defaultdict

docs = {
    1: "kafka consumer lag alert",
    2: "spark job failed with consumer error",
    3: "kafka broker disk full kafka alert",
}

# Build the inverted index: term -> {doc_id: term frequency}
index = defaultdict(dict)
lengths = {}
for doc_id, text in docs.items():
    terms = text.split()                     # a real analyzer also lowercases, stems...
    lengths[doc_id] = len(terms)
    for term, tf in Counter(terms).items():
        index[term][doc_id] = tf

N, avgdl, k1, b = len(docs), sum(lengths.values()) / len(docs), 1.2, 0.75

def bm25(query):
    scores = Counter()
    for term in query.split():
        postings = index.get(term, {})
        idf = math.log(1 + (N - len(postings) + 0.5) / (len(postings) + 0.5))
        for doc_id, tf in postings.items():
            norm = k1 * (1 - b + b * lengths[doc_id] / avgdl)
            scores[doc_id] += idf * tf * (k1 + 1) / (tf + norm)
    return [(d, round(s, 3)) for d, s in scores.most_common()]

print(dict(index["kafka"]))          # {1: 1, 3: 2}
print(bm25("kafka alert"))           # [(3, 1.071), (1, 1.047)]
```

Document 3 wins narrowly: it mentions "kafka" twice, but it is longer than document 1, so length normalisation takes back most of the advantage.

**Shards, replicas and the cluster.** An index is split into **primary shards**, each a complete Lucene index, and each primary has **replica shards** on other nodes for failover and read throughput. Since version 7 an index has one primary shard by default; you choose more when you create it, because the number of primaries cannot change without reindexing (or the split and shrink APIs). Elastic's guidance is to aim for shards of roughly 10 to 50 GB. A search is sent to one copy of every shard; each returns its top hits, and the coordinating node merges them (**query then fetch**).

**Near real-time.** Lucene writes new documents into small immutable **segments**. A new segment becomes searchable at the next **refresh**, every second by default, which is why Elasticsearch is "near real-time": a document is searchable about a second after you index it. For durability, each operation is also written to a **translog**; a **flush** commits segments to disk and trims the translog. Background **merges** combine small segments into larger ones, physically removing deleted documents.

**Aggregations.** Besides search, Elasticsearch computes aggregations (counts by status, percentiles of latency, date histograms) using **doc values**, a columnar on-disk structure built for each `keyword`, numeric and date field. This is what Kibana dashboards run on.

```json
GET /tickets/_search
{
  "query": { "bool": {
    "must":   [{ "match": { "title": "consumer lag" } }],
    "filter": [{ "term": { "status": "open" } },
               { "range": { "created": { "gte": "now-7d" } } }]
  }},
  "aggs": { "by_service": { "terms": { "field": "service" } } }
}
```

Clauses under `filter` do not affect the score and are cached, so put exact conditions there and keep `must` for full-text relevance.

> **Tip:** Elasticsearch is a search index, not a system of record. Keep the source of truth in a database or in Cloud Storage, and feed Elasticsearch from it (through CDC or a pipeline) so you can rebuild an index whenever the mapping changes.

::: deeper
**Logs and ECS.** For logs and metrics, Elasticsearch uses **data streams** (an append-only series of hidden backing indices that roll over by size or age) with **index lifecycle management (ILM)** moving older indices from hot to warm, cold and frozen tiers, and finally deleting them. The **Elastic Common Schema (ECS)** defines standard field names (`host.name`, `event.dataset`, `source.ip`) so data from different sources can be searched and correlated together; it has been contributed to OpenTelemetry's semantic conventions.

**Deep pagination.** `from` + `size` paging forces each shard to collect `from + size` hits, which is why results beyond 10,000 are blocked by default (`index.max_result_window`). Use `search_after` with a point-in-time for deep paging and exports.

**Vector and hybrid search.** The `dense_vector` field type stores embeddings and builds HNSW graphs for approximate k-nearest-neighbour search, with quantisation to reduce memory. Combining BM25 and vector results with reciprocal rank fusion (**hybrid search**) is a common pattern for retrieval-augmented generation.

**Split brain and the cluster coordination layer.** Since version 7, master election uses a quorum-based protocol that cannot form two masters, replacing the old `minimum_master_nodes` setting that was easy to misconfigure.

**On Google Cloud.** Elastic Cloud is available through Google Cloud Marketplace and runs on Google Cloud regions; you can also run Elasticsearch or OpenSearch on GKE with their operators. For simpler needs, BigQuery has `SEARCH` indexes and vector search, and AlloyDB and Cloud SQL support `pgvector`.

> **Interview:** "Why is a newly indexed document not immediately searchable?" New documents go into an in-memory buffer and become searchable only when a refresh writes them into a new Lucene segment, every second by default. You can force a refresh, but frequent refreshes create many small segments and hurt indexing throughput.
:::

::: check
?? What is the difference between a `text` and a `keyword` field?
A `text` field is analysed into terms for full-text search, so "Disk Full" matches "disk". A `keyword` field is stored as one exact value, used for filters, sorting and aggregations.

?? Why can you not simply change the number of primary shards of an existing index?
Documents are routed to primary shards by a hash of their routing value modulo the number of primaries. Changing the number would change where every document belongs, so you must reindex (or use the split and shrink APIs, which work only by whole factors).

?? In BM25, why does a rare term contribute more to the score than a common one?
Through inverse document frequency: a term that appears in few documents distinguishes them well, so matching it is stronger evidence of relevance than matching a word that is in almost every document.
:::

::: resources
- Docs: [Elasticsearch Guide](https://www.elastic.co/docs/reference/elasticsearch) · the official reference for mappings, queries, aggregations and cluster settings
- Docs: [Size your shards](https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/size-shards) · Elastic's guidance on shard size and count
- Blog: [Practical BM25: the BM25 algorithm and its variables](https://www.elastic.co/blog/practical-bm25-part-2-the-bm25-algorithm-and-its-variables) · Elastic's walk-through of each term in the BM25 formula, with worked numbers
- Docs: [Lucene BM25Similarity](https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/search/similarities/BM25Similarity.html) · the exact scoring formula Lucene implements
- Docs: [OpenSearch documentation](https://docs.opensearch.org/latest/) · the Apache 2.0 fork, with the same core concepts
- Course: [Elastic free training](https://www.elastic.co/training/free) · free on-demand courses from Elastic
:::

### Time-Series Databases — InfluxDB, TimescaleDB

::: plain
A time-series database is a diary for machines. Every few seconds, thousands of sensors, servers or apps write down a measurement with a timestamp: temperature 21.4 at 10:00:00, 21.5 at 10:00:10. The diary only ever grows at the end, nobody edits old pages, and almost every question is about a time range ("average per minute over the last hour"). A time-series database is built around those habits: it stores the entries very compactly, keeps summaries ready, and throws away old pages automatically.
:::

**Time-series data** is a sequence of measurements, each with a timestamp, one or more values, and some identifying **tags** (labels such as `host`, `region`, `sensor_id`). Monitoring metrics, IoT telemetry, financial ticks and application events all have this shape. The workload is distinctive:

- Writes are almost all **appends** at the current time, at high volume.
- Queries scan **time ranges** and aggregate: averages per minute, maximum per hour, last value per device.
- Old data is rarely updated, is often **downsampled** (kept at lower resolution), and is eventually deleted.

A general OLTP database can store this, but a time-series database makes each of these cheap.

**TimescaleDB** is a PostgreSQL extension (developed by Timescale, which renamed itself TigerData in 2025), so you keep full SQL, joins and the PostgreSQL ecosystem. Its central object is the **hypertable**: a table that looks normal but is automatically split into **chunks**, each covering a time interval (7 days by default).

```sql
-- PostgreSQL with the TimescaleDB extension
CREATE TABLE metrics (
  ts        timestamptz NOT NULL,
  device_id int         NOT NULL,
  cpu       double precision
);
SELECT create_hypertable('metrics', by_range('ts', INTERVAL '1 day'));

-- Rows older than 7 days are converted to compressed columnar form
ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id');
SELECT add_compression_policy('metrics', INTERVAL '7 days');

-- Drop whole chunks older than 90 days: instant, no row-by-row DELETE
SELECT add_retention_policy('metrics', INTERVAL '90 days');
```

Chunks keep the recent, heavily written data small enough to stay in memory with its indexes, let queries skip chunks outside their time range, and make retention a matter of dropping files. Compression converts old chunks into a columnar layout grouped by the `segmentby` column, typically shrinking them by 90% or more. Recent releases call this compressed form the **columnstore**, with the `add_columnstore_policy` function as the newer name for the same idea.

**Continuous aggregates** are materialised views that TimescaleDB refreshes incrementally, only recomputing the time buckets that received new data:

```sql
CREATE MATERIALIZED VIEW metrics_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 hour', ts) AS hour, device_id,
       avg(cpu) AS avg_cpu, max(cpu) AS max_cpu
FROM metrics
GROUP BY hour, device_id;

SELECT add_continuous_aggregate_policy('metrics_hourly',
  start_offset => INTERVAL '3 hours', end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '30 minutes');
```

Dashboards then read the small hourly view instead of scanning raw rows, and you can keep hourly aggregates for years while dropping raw data after 90 days.

**InfluxDB** is a purpose-built time-series database. Data is written in **line protocol**, one point per line: measurement, tags, fields and a timestamp in nanoseconds.

```text
cpu,host=web-1,region=eu usage=41.5,idle=58.5 1735689600000000000
cpu,host=web-2,region=eu usage=12.0,idle=88.0 1735689600000000000
```

InfluxDB 3 (Core and Enterprise, generally available since 2025) is a rewrite in Rust on the **FDAP** stack: Apache **F**light for transport, **D**ataFusion as the SQL query engine, **A**rrow in memory and **P**arquet files on local disk or object storage. It is queried with SQL (and InfluxQL), and it removed the old limits on **cardinality** (the number of distinct tag combinations) that plagued InfluxDB 1.x and 2.x.

**Compression.** Time series compress extremely well because neighbouring values are similar. The **Gorilla** paper (Facebook, 2015) introduced two techniques now used almost everywhere: timestamps stored as **delta of deltas** (regular intervals become zeros), and floats stored as the **XOR** with the previous value (similar values share most bits, so only a few differing bits are stored).

```python
ts = [1700000000, 1700000010, 1700000020, 1700000030, 1700000041, 1700000050]

deltas = [b - a for a, b in zip(ts, ts[1:])]
dods = [b - a for a, b in zip(deltas, deltas[1:])]
print("deltas:", deltas)                   # deltas: [10, 10, 10, 11, 9]
print("delta of deltas:", dods)            # delta of deltas: [0, 0, 1, -2]

# Gorilla stores the first timestamp and first delta in full, then each
# delta of delta: a zero costs a single bit, small values a few bits.
bits = sum(1 if d == 0 else 9 for d in dods)   # 9 bits: '10' prefix + 7-bit value
print(len(ts) * 64, "bits raw vs about", 64 + 14 + bits, "bits encoded")
# 384 bits raw vs about 98 bits encoded
```

On Facebook's production data, Gorilla averaged about 1.37 bytes per point, against 16 bytes for a raw 64-bit timestamp and 64-bit value.

> **Pitfall:** High-cardinality tags. Putting a request ID, user ID or full URL in a tag creates one series per distinct value. Systems that index every series (Prometheus, older InfluxDB) run out of memory. Keep unbounded values as fields, not tags.

> **Tip:** Decide retention and downsampling on day one: for example raw data for 30 days, 1-minute aggregates for a year, hourly aggregates forever. Retrofitting it onto a 20 TB table is much harder.

::: deeper
**Other engines.** **Prometheus** has its own TSDB (two-hour in-memory head block, then immutable compressed blocks) and is the standard for infrastructure metrics; Thanos, Cortex and Mimir add long-term storage on object storage. **ClickHouse** is a column store often used for time series and logs at very large scale (the Real-time OLAP engines section covers it). **QuestDB** and **Apache IoTDB** are other specialised open-source options.

**Late and out-of-order data.** Devices that buffer data while offline send old timestamps later. TimescaleDB can insert into compressed chunks (with some overhead), and continuous aggregates must be refreshed far enough back (`start_offset`) to include late points. This is the same event-time problem the Streaming systems section addresses with watermarks.

**Time series on Google Cloud.** **Bigtable** is a common choice for high-volume time series: design the row key as `device_id#reverse_timestamp` (or with a time bucket) so a device's recent points are contiguous and writes spread across nodes. **BigQuery** handles analytical time series with partitioning by day, clustering by device, and functions such as `TIMESTAMP_BUCKET` and `GAP_FILL`. Google Cloud Managed Service for Prometheus stores Prometheus metrics in Google's Monarch backend. TimescaleDB runs on Compute Engine or GKE, or through TigerData's own cloud.

> **Interview:** "Why not just use PostgreSQL for metrics?" You can, up to a point. Without partitioning by time, indexes grow until they no longer fit in memory and inserts slow down, retention needs slow mass `DELETE`s that bloat the table, and range aggregations scan raw rows. Time partitioning, columnar compression, chunk dropping and incremental aggregates solve exactly those problems, which is what TimescaleDB adds to PostgreSQL.
:::

::: check
?? Why does dropping a whole chunk beat `DELETE FROM metrics WHERE ts < now() - interval '90 days'`?
Dropping a chunk removes its files at once. A `DELETE` must find and mark every row, generates WAL, leaves dead tuples that VACUUM must clean, and can take hours on a big table.

?? Why do delta-of-delta timestamps compress so well?
Most measurements arrive at a regular interval, so the difference between consecutive deltas is usually zero, which can be stored in a single bit.

?? A team adds `user_id` as a tag on every metric. What will go wrong?
Cardinality explodes: every user creates a new series. Memory use and index size grow with the number of series, and queries slow down. User ID should be a field, or belong in a log or event store instead.
:::

::: resources
- Docs: [TimescaleDB documentation](https://docs.timescale.com/) · hypertables, compression and columnstore, continuous aggregates and retention policies
- Docs: [InfluxDB 3 Core documentation](https://docs.influxdata.com/influxdb3/core/) · line protocol, the FDAP architecture and SQL queries
- Paper: [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf) · VLDB 2015; delta-of-delta and XOR compression
- Docs: [Bigtable schema design for time series data](https://cloud.google.com/bigtable/docs/schema-design-time-series) · row key patterns for time series on Google Cloud
- Docs: [Prometheus storage](https://prometheus.io/docs/prometheus/latest/storage/) · how the Prometheus TSDB lays out blocks, the WAL and retention
:::

## Caching and operations

The last group is about running databases well in production, which is where data engineers and application engineers meet. A database rarely stands alone: an in-memory store such as Redis sits in front of it to absorb repeated reads, a connection pooler sits between hundreds of application processes and the database's limited connection slots, and schema changes have to be rolled out while traffic keeps flowing and CDC pipelines keep reading.

Each of these layers exists because of something you learned earlier in the section: caches because disk reads and query planning cost milliseconds, poolers because PostgreSQL uses one process per connection, and careful migrations because DDL takes locks and rewrites pages. Getting them wrong is a common cause of outages, and of broken downstream pipelines.

### Redis — In-Memory Data Structures & Internals

::: plain
Redis is a whiteboard that the whole team can read and write at lightning speed. It keeps everything in memory, so answers come back in well under a millisecond, and instead of only plain values it understands useful shapes: lists, sets, counters, leaderboards, queues. Because it is a whiteboard, you must decide whether you mind if it is wiped: Redis can photograph the board now and then, or keep a diary of every change, so it can be restored after a restart.
:::

**Redis** (remote dictionary server) is an in-memory data-structure server. Clients send short commands over TCP; Redis executes them against data held in RAM and replies, typically in tens of microseconds. It is used as a cache, a session store, a rate limiter, a leaderboard, a lightweight queue and a message broker. **Valkey** is a Linux Foundation fork created in 2024 when Redis moved away from its BSD licence; Redis 8 (2025) added the AGPLv3 open-source licence as an option. On Google Cloud, **Memorystore** offers managed Redis, Redis Cluster and Valkey.

**Single-threaded execution.** Redis executes commands one at a time on a single main thread, using an event loop to serve thousands of connections. That sounds slow but is the reason Redis is simple and fast: no locks, and every command is **atomic** (no other command runs halfway through it). Since Redis 6, optional **I/O threads** parse requests and write responses, but command execution stays single-threaded. The flip side: one slow command (`KEYS *` on millions of keys, a huge `SMEMBERS`, a long Lua script) blocks everyone.

**Data structures.** Every key holds one typed value:

```text
127.0.0.1:6379> SET page:home "<html>...</html>" EX 60
OK
127.0.0.1:6379> INCR views:2026-11-22
(integer) 1
127.0.0.1:6379> HSET user:42 name Asha plan pro
(integer) 2
127.0.0.1:6379> HGET user:42 plan
"pro"
127.0.0.1:6379> LPUSH jobs j1 j2
(integer) 2
127.0.0.1:6379> RPOP jobs
"j1"
127.0.0.1:6379> ZADD leaderboard 120 asha 95 bo 150 chen
(integer) 3
127.0.0.1:6379> ZRANGE leaderboard 0 1 REV WITHSCORES
1) "chen"
2) "150"
3) "asha"
4) "120"
```

| Type | What it is | Typical use |
|---|---|---|
| String | bytes, up to 512 MB; also integers for `INCR` | cached pages and objects, counters, flags |
| Hash | a small map of fields to values | an object such as a user profile |
| List | a linked list of values | simple queues, recent-items lists |
| Set | unordered unique values | tags, "who has seen this" |
| Sorted set (ZSet) | unique members ordered by a score | leaderboards, priority queues, time-ordered indexes |
| Stream | an append-only log with consumer groups | event queues with acknowledgements |
| Others | bitmaps, HyperLogLog, geospatial, JSON, vector sets | flags per user, approximate distinct counts, nearby search |

Internally, small hashes, sets and sorted sets use a compact encoding (a **listpack**) and switch to hash tables or a **skip list** plus hash table when they grow. That is why a sorted set can both look up a member's score in O(1) and return ranges in O(log n).

**Expiry and eviction.** Any key can have a time-to-live (`EX 60`, `EXPIRE`). Redis deletes expired keys lazily (when accessed) and actively (sampling keys with a TTL in the background). When memory reaches `maxmemory`, the **eviction policy** decides what happens. The default, `noeviction`, rejects writes; caches should use `allkeys-lru` or `allkeys-lfu` (approximate least recently or least frequently used), while `volatile-*` policies evict only keys that have a TTL.

**Persistence.** Two mechanisms, usable together:

- **RDB snapshots**: Redis forks a child process, which writes the whole dataset to a compact file while the parent keeps serving (thanks to the operating system's copy-on-write). Fast restarts, but you lose writes since the last snapshot.
- **AOF (append-only file)**: every write command is appended to a log. With `appendfsync everysec` (the default when AOF is on) you lose at most about one second of writes. The log is periodically **rewritten** to its minimal form, and since Redis 7 it is split into a base file plus incremental files.

> **Pitfall:** Forking a large instance for an RDB snapshot or AOF rewrite can briefly stall Redis and, under heavy writes, nearly double memory use as pages are copied. Leave memory headroom (often 25 to 50%) on write-heavy instances.

**Replication and Cluster.** A primary streams its writes asynchronously to replicas; **Sentinel** or the managed service promotes a replica on failure. **Redis Cluster** shards data across primaries using **16,384 hash slots**: a key's slot is `CRC16(key) mod 16384`, and each primary owns a range of slots. Clients cache the slot map; if they ask the wrong node they get a `MOVED` redirect. Multi-key commands work only when all keys hash to the same slot, which you force with a **hash tag**: only the part inside braces is hashed, so `{user:42}:cart` and `{user:42}:profile` live together.

**Pub/Sub, Streams and Lua.** **Pub/Sub** (`PUBLISH`, `SUBSCRIBE`) is fire-and-forget: a subscriber that is offline misses messages. **Streams** keep messages: `XADD` appends, consumer groups (`XGROUP CREATE`, `XREADGROUP`) share work among consumers, and `XACK` acknowledges, with unacknowledged entries visible through `XPENDING` and reclaimable with `XAUTOCLAIM`. **Lua scripts** (`EVAL`) and, since Redis 7, **functions** run several commands atomically on the server. A fixed-window rate limiter:

```text
127.0.0.1:6379> EVAL "local c = redis.call('INCR', KEYS[1]) if c == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end return c" 1 rate:user:42 60
(integer) 1
```

> **Tip:** Name keys with a consistent `type:id:field` pattern, always set a TTL on cache keys, and use `SCAN` instead of `KEYS` to iterate. Monitor `used_memory`, `evicted_keys`, hit ratio (`keyspace_hits` against `keyspace_misses`) and the slow log (`SLOWLOG GET`).

::: deeper
**Big keys and hot keys.** A single 100 MB sorted set takes seconds to delete (use `UNLINK`, which frees memory in the background) and lands on one cluster node. A single very hot key sends all its traffic to one node regardless of cluster size. `redis-cli --bigkeys` and `--hotkeys` (with an LFU policy) find them; the fixes are splitting the value or replicating the hot key under several names.

**Replication is asynchronous.** A primary acknowledges writes before replicas have them, so a failover can lose recent writes. `WAIT numreplicas timeout` lets a client wait for replication, but this is still not a consensus system. Do not use Redis as the only copy of data you cannot lose.

**Distributed locks.** `SET lock:job42 <token> NX PX 30000` takes a lock with an expiry, and a Lua script releases it only if the token matches. The multi-node **Redlock** algorithm is controversial (Martin Kleppmann's critique points out that pauses and clock jumps can let two holders believe they hold the lock); for correctness-critical locks, use fencing tokens or a consensus-based store.

**Memorystore on Google Cloud.** Memorystore for Redis offers a primary with an optional replica. Memorystore for Redis Cluster and Memorystore for Valkey are sharded, scale without downtime and offer a 99.99% SLA in their highly available configuration. They are reached on private IP from your VPC, typically from Cloud Run, GKE or Compute Engine.

**Redis in data pipelines.** Common uses are serving precomputed features or aggregates written by a batch or streaming job, deduplicating events with `SET key NX EX`, and holding small lookup tables for stream enrichment. The data still originates in BigQuery or a database; Redis is the fast serving copy.

> **Interview:** "Redis is single-threaded. How can it handle 100,000 operations per second?" Each command is a fast in-memory operation, so the CPU cost per command is a few microseconds; an event loop multiplexes many connections without thread switches or locks; and pipelining sends many commands per network round trip. The bottleneck is usually the network, not the CPU.
:::

::: check
?? Why is every Redis command atomic without any locking?
Commands execute one at a time on a single thread, so no other command can run in the middle of one.

?? A cache instance fills up and writes start failing with an "OOM command not allowed" error. What is the likely configuration problem?
The eviction policy is still the default `noeviction`. For a cache set `maxmemory-policy` to `allkeys-lru` or `allkeys-lfu` so old keys are evicted instead.

?? In Redis Cluster, how do you make `{user:42}:cart` and `{user:42}:profile` usable in one transaction?
They already are: with a hash tag only `user:42` is hashed, so both keys map to the same slot and the same node, which allows multi-key commands and transactions on them.
:::

::: resources
- Docs: [Redis documentation](https://redis.io/docs/latest/) · data types, persistence, replication, cluster and commands
- Docs: [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) · RDB and AOF, their trade-offs and how to configure them
- Docs: [Redis cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/) · hash slots, redirection, failover and the cluster bus
- Book: [Redis in Action](https://www.manning.com/books/redis-in-action) · Josiah Carlson; practical patterns built from Redis data structures
- Docs: [Memorystore documentation](https://cloud.google.com/memorystore/docs) · managed Redis, Redis Cluster and Valkey on Google Cloud
- Article: [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) · Martin Kleppmann on why Redlock is unsafe for correctness
:::

### Distributed Caching Patterns

::: plain
A cache is a small notebook you keep on your desk with the answers you looked up recently, so you do not have to walk to the library every time. The hard questions are about keeping the notebook honest: when do you write a new answer into it, and when do you cross out an answer that has become wrong? And if a popular answer is crossed out, you do not want the whole office to run to the library at the same moment.
:::

A **cache** stores the results of expensive work (a database query, an API call, a rendered page) in a faster store so that repeated requests are cheap. A **distributed cache** is a cache shared by many application servers, usually Redis, Valkey or **Memcached**, spread across several nodes. The benefits are lower latency and much less load on the database. The cost is that the cache can disagree with the database, so every design is about how stale you can tolerate and who updates what.

**The four main patterns.**

[[fig:databases_cacheaside]]

| Pattern | On read | On write | Consistency | Notes |
|---|---|---|---|---|
| Cache-aside (lazy loading) | the app checks the cache; on a miss it reads the database and fills the cache | the app writes the database, then deletes the cache entry | stale until the next miss or TTL | the most common; only requested data is cached |
| Read-through | the cache itself loads from the database on a miss | (combined with one of the write patterns) | as for the write pattern | needs a cache layer that knows the database |
| Write-through | reads hit the cache | the app (or cache layer) writes cache and database together | cache stays fresh | writes are slower; caches data that may never be read |
| Write-behind (write-back) | reads hit the cache | the cache acknowledges and writes the database later, in batches | database lags; data lost if the cache dies | fast writes, real risk; use only for data you can afford to lose |

**Cache-aside in code** (Python-like pseudocode for a Redis client):

```python
def get_product(product_id):
    key = f"product:{product_id}"
    cached = redis.get(key)
    if cached is not None:
        return json.loads(cached)                    # hit
    row = db.fetch_one("SELECT * FROM products WHERE id = %s", product_id)
    redis.set(key, json.dumps(row), ex=300 + random.randint(0, 60))   # TTL with jitter
    return row

def update_price(product_id, price):
    db.execute("UPDATE products SET price = %s WHERE id = %s", price, product_id)
    redis.delete(f"product:{product_id}")            # invalidate, do not update
```

Two details matter. First, on write **delete** the cache entry rather than writing the new value into it: if two writers race, setting values can leave the older value in the cache, while deleting just causes one extra miss. Second, add **random jitter** to TTLs so that entries created at the same moment do not all expire at the same moment.

**Invalidation strategies.**

- **TTL only**: every entry expires after a fixed time. Simple and self-healing; staleness is bounded by the TTL.
- **Explicit invalidation**: the code that changes data deletes the affected keys. Fresh, but easy to miss a code path, and hard when one change affects many cached results.
- **Event-driven invalidation**: a CDC stream from the database (Datastream or Debezium reading the WAL) triggers deletes. It catches every change, including those made by batch jobs and manual fixes.
- **Versioned keys**: include a version in the key (`product:42:v17`), bumped on every change, so old entries are simply never read again and expire on their own.

> **Pitfall:** The cache-aside race. Reader A misses and reads the old row from the database. Writer B updates the row and deletes the cache key. Reader A, delayed, now writes the *old* row into the cache, which stays stale until its TTL expires. Short TTLs bound the damage; **leases** (below) or a second delayed delete fix it properly.

**Thundering herd (cache stampede).** When a popular key expires, every request that arrives before it is refilled misses at once and hits the database together. Under load this can take the database down, which causes more misses, which makes it worse. The defences:

1. **Request coalescing (single flight)**: only one request per key loads from the database; the others wait for its result.
2. **A lock in the cache**: the first miss takes `SET lock:key token NX PX 5000`; others wait briefly, or serve a stale copy.
3. **Early or background refresh**: refresh hot keys before they expire, either from a background job or probabilistically (each request recomputes with a small probability that grows as expiry approaches).
4. **Serve stale while revalidating**: keep the old value a little past its logical expiry and return it while one request refreshes it.

This runnable standard-library script shows the effect of coalescing when 20 requests miss at the same moment:

```python
import threading, time

db_calls = 0
counter_lock = threading.Lock()

def load_from_db(key):
    global db_calls
    with counter_lock:
        db_calls += 1
    time.sleep(0.1)                          # a slow query
    return f"value-of-{key}"

def run(get, n=20):
    global db_calls
    db_calls = 0
    start = threading.Barrier(n)             # all requests arrive together
    def worker():
        start.wait()
        get("home_page")
    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads: t.start()
    for t in threads: t.join()
    return db_calls

# 1. Plain cache-aside: every request that misses goes to the database.
cache = {}
def get_naive(key):
    if key not in cache:
        cache[key] = load_from_db(key)
    return cache[key]

# 2. Single flight: one request loads, the others wait for its result.
cache2, inflight, guard = {}, {}, threading.Lock()
def get_single_flight(key):
    with guard:
        if key in cache2:
            return cache2[key]
        event = inflight.get(key)
        leader = event is None
        if leader:
            event = inflight[key] = threading.Event()
    if leader:
        cache2[key] = load_from_db(key)
        with guard:
            del inflight[key]
        event.set()
    else:
        event.wait()
    return cache2[key]

print("naive:", run(get_naive), "database calls")                 # naive: 20 database calls
print("single flight:", run(get_single_flight), "database call")  # single flight: 1 database call
```

In a fleet of servers, coalescing inside each process reduces the herd from thousands of requests to one per server; a lock or lease in the shared cache reduces it to one overall.

**Memcached versus Redis.** **Memcached** is a pure cache: multithreaded, strings only, a slab memory allocator with LRU eviction, no persistence and no replication; clients spread keys across servers with consistent hashing. It is very fast and predictable for simple get and set workloads. Redis adds data structures, persistence, replication and cluster sharding. Most new systems choose Redis or Valkey; Memcached remains common at very large scale.

> **Tip:** Measure the hit ratio before and after you add a cache. A cache with a 30% hit ratio adds a network hop to 70% of requests and may make things slower overall. Cache data that is read much more often than it changes.

::: deeper
**Leases (Facebook's memcache).** In the NSDI 2013 paper "Scaling Memcache at Facebook", a miss returns a **lease token**; only the holder of a valid token may fill the key, and a delete invalidates outstanding tokens. This fixes the stale-set race (the delayed reader's token is no longer valid) and the herd (other readers are told to wait briefly or use a slightly stale value). The paper also covers regional pools, cold-cluster warm-up and invalidation driven by the MySQL commit log, which is the CDC idea in a 2013 form.

**Probabilistic early expiration.** The **XFetch** algorithm (Vattani, Chierichetti and Lowenstein, VLDB 2015) recomputes a value early when `now - delta * beta * ln(random()) >= expiry`, where `delta` is how long a recomputation takes. Since `ln(random())` is negative, the left side is a little in the future, by an amount that is usually small but occasionally large. One request tends to refresh the key shortly before it expires, without any coordination.

**Multi-level caches.** Large systems stack caches: an in-process cache (microseconds, per server), a shared distributed cache (sub-millisecond), and a CDN for public content (Cloud CDN on Google Cloud). Each level needs its own invalidation, and the in-process level is the hardest to keep consistent; give it short TTLs.

**Caching in analytics.** BigQuery caches query results for about 24 hours when the query is deterministic and the underlying tables have not changed, and BI Engine caches data in memory for dashboards. Materialised views and pre-aggregated tables are caches too, with the warehouse handling invalidation for you.

> **Interview:** "How would you protect the database when a hot cache key expires?" Coalesce requests so only one loads per key, add a lock or lease in the cache so only one process recomputes, refresh hot keys early or in the background, serve slightly stale data while refreshing, and add TTL jitter so many keys do not expire together.
:::

::: check
?? Why should a writer delete the cache entry rather than set it to the new value?
Deleting is safe under races: the worst case is one extra miss. Setting values can leave an older value in the cache if two writers or a writer and a slow reader interleave.

?? What is a cache stampede, and name two ways to prevent it.
Many requests missing the same expired key at once and all hitting the database. Prevent it with request coalescing or a lock so only one request recomputes, early or background refresh, serving stale data while revalidating, or TTL jitter.

?? When is write-behind caching a bad idea?
When losing recent writes is unacceptable, such as payments or orders: the cache acknowledges before the database has the data, so a cache failure loses it.
:::

::: resources
- Paper: [Scaling Memcache at Facebook](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/nishtala) · NSDI 2013; leases, regional pools and invalidation at enormous scale
- Paper: [Optimal Probabilistic Cache Stampede Prevention](https://www.vldb.org/pvldb/vol8/p886-vattani.pdf) · VLDB 2015; the XFetch early-expiration algorithm
- Docs: [Memorystore for Valkey overview](https://cloud.google.com/memorystore/docs/valkey/product-overview) · Google Cloud's managed, sharded cache service
- Docs: [Memcached wiki](https://github.com/memcached/memcached/wiki) · how Memcached allocates memory, evicts and scales
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · derived data, caches and keeping them consistent with the system of record
- Blog: [Cache made consistent](https://engineering.fb.com/2022/06/08/core-infra/cache-made-consistent/) · Meta on measuring and fixing cache inconsistency
:::

### Connection Pooling & Query Routing

::: plain
A database can only talk to a limited number of callers at once, like a bank with a few counters. If every customer insisted on keeping a counter to themselves for the whole day, the bank would run out of counters by mid-morning. A **connection pooler** is the queue manager at the door: it keeps a small number of counters busy all the time and hands each one to whichever customer is ready, for just as long as their business takes. A **query router** goes further and sends quick questions to the side counters (replicas), keeping the main counter for changes.
:::

**Why connections are expensive.** Opening a PostgreSQL connection means a TCP and TLS handshake, authentication and forking a new backend process, which takes milliseconds and several megabytes of memory. Each open connection also costs the server something even when idle, and too many active ones fight over CPU, locks and memory. PostgreSQL's default `max_connections` is 100. Meanwhile modern applications multiply connections: 30 application servers, each with a pool of 20, is already 600, and serverless platforms such as Cloud Run can scale to hundreds of instances in seconds.

**Application-side pools.** Every serious database driver has a pool (HikariCP in Java, SQLAlchemy's pool in Python, `pgxpool` in Go): it keeps a few connections open and lends them to requests. That solves the handshake cost inside one process, but not the total count across many processes.

**PgBouncer.** A lightweight proxy between applications and PostgreSQL. Clients connect to PgBouncer (thousands of cheap client connections); PgBouncer keeps a small pool of real server connections per database and user, and assigns them to clients according to its **pool mode**:

| Pool mode | A server connection is assigned for | Allows | Breaks |
|---|---|---|---|
| `session` | the whole client session | everything | little multiplexing: no fewer server connections than active clients |
| `transaction` | one transaction | high multiplexing, the usual choice | session state: `SET`, session advisory locks, `LISTEN`, temporary tables across transactions |
| `statement` | one statement | maximum multiplexing | multi-statement transactions |

```ini
; pgbouncer.ini
[databases]
shop = host=10.0.0.5 port=5432 dbname=shop

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt
pool_mode = transaction
max_client_conn = 2000          ; clients PgBouncer accepts
default_pool_size = 20          ; server connections per database and user pair
max_prepared_statements = 100   ; track protocol-level prepared statements
```

PgBouncer 1.21 added support for protocol-level prepared statements in transaction mode (set `max_prepared_statements` above zero), which removed the most common reason applications could not use transaction pooling.

> **Pitfall:** In transaction mode, `SET search_path = tenant_7` or `SET statement_timeout = ...` applies to whatever server connection you happen to have, and leaks to the next client that gets it. Use `SET LOCAL` inside a transaction, or put settings in the connection string or the role's defaults.

**Pool sizing.** More connections are not faster. A database server with 8 cores can only run about 8 queries truly in parallel; beyond a point, more active connections just add contention. Size from measurements with **Little's law**: the average number of busy connections equals the arrival rate times the time each request holds a connection.

```python
import math

# Little's law: busy connections = arrival rate x time each request holds one
queries_per_second = 1200
seconds_per_query = 0.004            # 4 ms average, measured on the database
busy = queries_per_second * seconds_per_query
print("average busy connections:", busy)              # average busy connections: 4.8

# Leave headroom for bursts, then compare with the HikariCP rule of thumb
pool = math.ceil(busy * 2)
cores = 8
print("pool with 2x headroom:", pool)                 # pool with 2x headroom: 10
print("rule of thumb (cores * 2 + 1):", cores * 2 + 1)  # rule of thumb (cores * 2 + 1): 17

# 30 app servers each opening a pool of 20 would ask for:
print("naive total:", 30 * 20, "server connections")  # naive total: 600 server connections
```

HikariCP's widely quoted starting point is `connections = cores * 2 + effective spindle count` (1 for a single SSD in the example). Both methods land in the tens, not the hundreds. Put PgBouncer in front and let thousands of client connections share those few.

**Read replica routing.** Replicas receive changes from the primary's WAL and serve reads. Routing reads there scales read capacity, but replication is usually **asynchronous**, so a replica can lag. A user who saves a profile and is immediately sent to a lagging replica sees the old profile. Common rules: route a user's reads to the primary for a short time after they write (**read-your-writes**), route by query type (reports and extracts to replicas, transactional reads to the primary), and stop routing to replicas whose lag exceeds a threshold.

**ProxySQL** does this for MySQL: it holds pools to **hostgroups** (for example 10 = writer, 20 = readers), routes each query with regular-expression **query rules**, monitors replica lag and removes lagging replicas automatically.

```sql
-- ProxySQL admin interface (port 6032)
INSERT INTO mysql_query_rules (rule_id, active, match_digest, destination_hostgroup, apply)
VALUES (1, 1, '^SELECT .* FOR UPDATE', 10, 1),
       (2, 1, '^SELECT', 20, 1);
LOAD MYSQL QUERY RULES TO RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
```

Rule 1 comes first so that locking reads still go to the writer.

> **Tip:** Point data extracts and CDC-free batch exports at a read replica, never the primary. On Google Cloud, Cloud SQL read replicas and AlloyDB read pool instances (one endpoint that load-balances across several read nodes) exist exactly for this.

::: deeper
**Connection lifecycle settings.** Close server connections that have been idle too long (`server_idle_timeout`) or alive too long (`server_lifetime`) so load rebalances after failovers and memory leaks in long-lived backends are bounded. On the client side, set a maximum lifetime slightly shorter than any timeout in between (load balancers, NAT gateways), or connections will be cut silently and the pool will hand out dead ones.

**Health checks and failover.** After a primary failover, pools hold connections to a server that is now a read-only replica or gone. Good poolers validate connections on checkout or after errors; managed services give you a stable endpoint (Cloud SQL's IP stays the same on HA failover) so only the reconnection matters.

**Other poolers.** **pgcat** and **Supavisor** add sharding and multi-tenant features to the PgBouncer model; **Odyssey** is a multithreaded pooler from Yandex. PostgreSQL's own work on reducing per-connection cost continues, but external pooling remains standard practice.

**Serverless and Google Cloud.** Cloud Run and Cloud Run functions can scale to many instances, each with its own pool, so cap `max_instances` and keep per-instance pools small (often 1 to 5). The **Cloud SQL Auth Proxy** and the Cloud SQL language connectors handle IAM-based authentication and TLS, but they are not poolers: you still need an application pool, and often PgBouncer, in front of busy instances.

**Prepared statements and plans.** Prepared statements let PostgreSQL reuse a parsed statement and, after several executions, a generic plan. In transaction pooling they used to fail because the next transaction might run on a server connection that never saw the `PREPARE`; PgBouncer now tracks and re-prepares them on the right connection.

> **Interview:** "Our database CPU is at 100% with 800 active connections. Would adding more connections help?" No: with far more active connections than cores, the server spends its time switching and contending. Put a pooler in front, cap the server pool near a small multiple of the core count, and let requests queue in the pooler, where waiting is cheap. Then look at the slowest queries.
:::

::: check
?? Why does PgBouncer's transaction mode break `SET search_path`?
`SET` changes session state on the server connection, but in transaction mode the client gets a different server connection for each transaction, and the next client may inherit the setting. Use `SET LOCAL` inside a transaction instead.

?? A service handles 600 queries per second, each holding a connection for 10 ms on average. How many connections are busy on average?
By Little's law, 600 × 0.010 = 6 connections. A pool of 10 to 15 gives headroom for bursts.

?? A user updates their address and the next page still shows the old one. Reads go to replicas. What is happening, and how do you fix it?
Replication lag: the replica has not yet replayed the write. Route that user's reads to the primary for a short window after a write (read-your-writes), or wait until the replica has caught up to the write's position before reading from it.
:::

::: resources
- Docs: [PgBouncer configuration](https://www.pgbouncer.org/config.html) · every setting, pool modes and their limitations
- Docs: [PgBouncer features](https://www.pgbouncer.org/features.html) · which SQL features work in which pool mode
- Article: [About Pool Sizing (HikariCP)](https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing) · why small pools are faster, with the cores-based rule of thumb
- Docs: [ProxySQL documentation](https://proxysql.com/documentation/) · hostgroups, query rules and read/write splitting for MySQL
- Docs: [Cloud SQL: managing database connections](https://cloud.google.com/sql/docs/postgres/manage-connections) · pooling, limits and connection patterns on Google Cloud
- Docs: [AlloyDB read pool instances](https://cloud.google.com/alloydb/docs/instance-read-pool-create) · scaling reads behind one endpoint
:::

### Zero-Downtime Database Migrations

::: plain
Changing a production database schema is like renovating a shop while it stays open. You cannot close the doors for a day, so you work in small, careful steps: build the new shelf next to the old one, start putting new stock on both, move the old stock across a little at a time, switch the signs, and only then remove the old shelf. At every step the shop works, and if something goes wrong you can stop without having broken anything.
:::

A **schema migration** changes a database's structure: adding a column, renaming a table, changing a type, adding an index or a constraint. Two things make that dangerous in production. First, many DDL statements take **locks** that block reads or writes, and some **rewrite the whole table**, which on a 500 GB table can take hours. Second, the application code and the schema must stay compatible during the rollout, because old and new versions of the application run side by side while a deployment rolls through.

**Expand and contract.** The core pattern (also called **parallel change**) splits a breaking change into backward-compatible steps. To rename `customers.name` to `full_name`:

1. **Expand**: add the new column `full_name`, nullable. Old code ignores it.
2. **Dual write**: deploy code that writes both `name` and `full_name`, still reading `name`.
3. **Backfill**: copy `name` into `full_name` for existing rows, in small batches.
4. **Switch reads**: deploy code that reads `full_name` (it still writes both, so a rollback is safe).
5. **Contract**: deploy code that stops writing `name`, then drop the column.

Every step is reversible on its own, and at no point does a running version of the application see a schema it does not understand. The cost is time: a rename becomes several deployments.

**What is safe in PostgreSQL.**

| Operation | Behaviour | Safe pattern |
|---|---|---|
| `ADD COLUMN` (nullable, or with a constant default) | metadata only since PostgreSQL 11 | safe as is |
| `ADD COLUMN ... DEFAULT now()` or another volatile default | rewrites the table | add nullable, then backfill |
| `CREATE INDEX` | blocks writes for the whole build | `CREATE INDEX CONCURRENTLY` |
| `ADD FOREIGN KEY` / `ADD CHECK` | scans the table under a lock | `ADD CONSTRAINT ... NOT VALID`, then `VALIDATE CONSTRAINT` |
| `ALTER COLUMN ... SET NOT NULL` | scans the table under an exclusive lock | add a `CHECK (col IS NOT NULL) NOT VALID`, validate it, then `SET NOT NULL` (no scan since PostgreSQL 12) |
| `ALTER COLUMN ... TYPE` | usually rewrites the table and indexes | add a new column, dual write, backfill, switch |

```sql
-- PostgreSQL: a safe migration session
SET lock_timeout = '3s';            -- give up instead of queueing behind a long query
SET statement_timeout = '0';        -- but let the long concurrent build run

CREATE INDEX CONCURRENTLY idx_orders_customer ON orders (customer_id);

ALTER TABLE orders
  ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id)
  REFERENCES customers (id) NOT VALID;       -- instant: new rows are checked from now on
ALTER TABLE orders VALIDATE CONSTRAINT orders_customer_fk;  -- scans without blocking writes

-- Backfill in batches so each transaction is short
UPDATE customers SET full_name = name
WHERE id IN (SELECT id FROM customers WHERE full_name IS NULL LIMIT 5000);
-- repeat until 0 rows are updated
```

> **Pitfall:** The lock queue. Even a "fast" `ALTER TABLE` needs a brief exclusive lock. If a long-running query holds a conflicting lock, the `ALTER` waits, and every new query on that table queues *behind the `ALTER`*. A metadata-only change can freeze the table for minutes. Always set `lock_timeout` and retry.

> **Pitfall:** `CREATE INDEX CONCURRENTLY` cannot run inside a transaction block, and if it fails it leaves an `INVALID` index behind. Many migration frameworks wrap each migration in a transaction by default; mark these migrations as non-transactional, and check for invalid indexes afterwards.

**MySQL: online DDL and copy-based tools.** MySQL 8 runs many changes in place: `ALGORITHM=INSTANT` adds or drops columns as a metadata change (adding at the end since 8.0.12, in any position since 8.0.29), and `ALGORITHM=INPLACE, LOCK=NONE` builds indexes while allowing writes. For changes that still need a table copy, or to control load on replicas, teams use external tools:

- **pt-online-schema-change** (Percona Toolkit) creates a new table with the new schema, copies rows in chunks, and uses **triggers** on the original table to apply concurrent changes, then renames the tables atomically.
- **gh-ost** (GitHub) does the same without triggers: it reads the **binary log** (MySQL's replication stream) to apply concurrent changes to a "ghost" table, which puts less load on the primary and lets it pause, throttle on replica lag, and postpone the final cut-over until a human is ready.

```bash
gh-ost --host=replica-1.db.internal --database=shop --table=orders \
  --alter="ADD COLUMN currency CHAR(3) NOT NULL DEFAULT 'EUR'" \
  --max-lag-millis=1500 --chunk-size=1000 \
  --postpone-cut-over-flag-file=/tmp/ghost.postpone \
  --execute
```

**Tooling and process.** Keep migrations as versioned files in Git, reviewed like code, and applied by a tool: **Flyway** or **Liquibase** (Java ecosystem), **Alembic** (SQLAlchemy), Django and Rails migrations, **sqitch**, or **pgroll** (an open-source PostgreSQL tool that automates expand and contract by serving old and new schema versions through views at the same time). Run them in CI against a copy of production-sized data, and apply them separately from application deploys.

> **Tip:** Plan rollback as "roll forward". Once data has been written in a new shape, reversing a migration is often impossible. Expand and contract keeps every step compatible, so the safe response to a problem is usually to stop, fix and continue, not to undo.

::: deeper
**Blue-green schema deploys.** For large changes (a major-version upgrade, a new table layout), build a complete second database, keep it in sync with **logical replication** or CDC, test against it, then switch application traffic. PostgreSQL's logical replication, Google Cloud's **Database Migration Service** and Datastream all follow this pattern. The switch still needs a brief write pause so the target catches up exactly.

**Backfill design.** Batches should be keyed (by primary key ranges, not `OFFSET`), short (a few thousand rows), idempotent (`WHERE new_col IS NULL`), throttled on replica lag and WAL volume, and resumable. A huge single `UPDATE` creates one enormous transaction, bloats the table with dead tuples and can make replicas fall far behind.

**Downstream impact.** Every schema change is also a change to the CDC stream that data pipelines consume. Dropping a column breaks downstream models that select it; changing a type can break a BigQuery load. Datastream and Debezium propagate many changes automatically, but the safest practice is a **data contract**: schema changes to tables that feed analytics are announced, reviewed with the data team and follow expand and contract on the analytics side too.

**Spanner and BigQuery.** Spanner applies schema changes online as long-running operations, validating existing data in the background without blocking traffic, though large backfills (new indexes) still take time and resources. BigQuery allows adding nullable columns and relaxing `REQUIRED` to `NULLABLE` as metadata changes; other changes (renames, type changes) are supported only for some cases, and the rest need a new table built with `CREATE TABLE ... AS SELECT`.

> **Interview:** "How would you rename a heavily used column with zero downtime?" Expand and contract: add the new column, deploy dual writes, backfill in batches, switch reads, stop writing the old column, and drop it last. Each step is backward compatible, uses only cheap DDL with a lock timeout, and can be paused or reversed.
:::

::: check
?? Why might a simple `ALTER TABLE ... ADD COLUMN` freeze a busy PostgreSQL table, even though it is metadata only?
It needs a brief exclusive lock. If a long query holds a conflicting lock, the `ALTER` waits in the lock queue, and every new query on the table waits behind it. Set `lock_timeout` so it gives up and retries instead.

?? What is the difference between pt-online-schema-change and gh-ost?
Both copy the table into a new one with the new schema and swap them. pt-online-schema-change captures concurrent changes with triggers on the original table; gh-ost reads them from the binary log, with no triggers, which reduces load and allows pausing and throttling.

?? In expand and contract, why does the application keep writing the old column after reads have switched to the new one?
So that rolling back the application to the previous version is still safe: the previous version reads the old column, which must stay current until the new version is proven.
:::

::: resources
- Repo: [gh-ost](https://github.com/github/gh-ost) · GitHub's triggerless online schema migration tool for MySQL, with design documents
- Docs: [pt-online-schema-change](https://docs.percona.com/percona-toolkit/pt-online-schema-change.html) · Percona's trigger-based tool and its safety options
- Docs: [PostgreSQL: ALTER TABLE](https://www.postgresql.org/docs/current/sql-altertable.html) · which subcommands take which locks and which rewrite the table
- Article: [Parallel Change](https://martinfowler.com/bliki/ParallelChange.html) · Martin Fowler's description of the expand and contract pattern
- Repo: [pgroll](https://github.com/xataio/pgroll) · zero-downtime, reversible PostgreSQL migrations using expand and contract
- Book: [Refactoring Databases](https://martinfowler.com/books/refactoringDatabases.html) · Ambler and Sadalage; the catalogue of safe, evolutionary schema changes
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| See row versions in PostgreSQL | `SELECT ctid, xmin, xmax, * FROM t` |
| Diagnose a slow query | `EXPLAIN (ANALYZE, BUFFERS) ...`; compare estimated and actual rows |
| Find what blocks VACUUM | `pg_stat_activity` ordered by `xact_start`; check `pg_replication_slots` |
| B-tree vs LSM | B-tree: in-place pages, predictable reads; LSM: appends and compaction, cheap writes |
| Bloom filter sizing | about 10 bits per key gives about 1% false positives |
| Snapshot isolation anomaly | write skew; fix with `SERIALIZABLE`, `SELECT ... FOR UPDATE` or a constraint |
| Avoid lost updates | `UPDATE t SET x = x - 1` in one statement, not read then write |
| Raft or Paxos majority | 3 replicas tolerate 1 failure; 5 tolerate 2 |
| Cassandra strong reads | `W + R > RF`, usually `LOCAL_QUORUM` for both |
| Cassandra modelling | one table per query; bounded partitions with a time bucket |
| Shard key | high cardinality, even writes, present in most queries; never a bare timestamp |
| Resharding | consistent hashing or many logical shards mapped to few servers |
| Elasticsearch fields | `text` for full-text search, `keyword` for filters and aggregations |
| Time-series retention | drop whole chunks or partitions, keep aggregates longer |
| Redis cache eviction | `maxmemory-policy allkeys-lru` (or `allkeys-lfu`); always set TTLs |
| Redis Cluster co-location | hash tags: `{user:42}:cart` and `{user:42}:profile` share a slot |
| Cache writes | write the database, then delete the key; add TTL jitter |
| Stampede defence | single flight, lock or lease, early refresh, serve stale |
| Pool size | Little's law: rate × hold time; keep server pools near a few × cores |
| Safe PostgreSQL DDL | `SET lock_timeout`; `CREATE INDEX CONCURRENTLY`; `NOT VALID` then `VALIDATE` |
| Breaking schema change | expand, dual write, backfill, switch reads, contract |

### Practice

::: try
**1. Read the version history.** In PostgreSQL, a row has two versions: (balance 100, xmin 200, xmax 230) and (balance 80, xmin 230, xmax 0). Transaction 230 has committed. Which balance does each of these snapshots see? (a) A snapshot with xmax 225 and no transactions in progress. (b) A snapshot with xmax 240 and transaction 230 listed as in progress. (c) A snapshot with xmax 240 and nothing in progress.
:::

?? Answer to 1
(a) 100: transaction 230 had not started when the snapshot was taken, so its delete and its new version are both invisible. (b) 100: 230 was still running when the snapshot was taken, so its changes are invisible. (c) 80: 230 committed before the snapshot, so the old version is deleted and the new one visible.

::: try
**2. Choose the store.** For each need, pick a database family and a Google Cloud service or open-source system, and give one reason: (a) a global payments ledger that must never double-spend, with users on three continents; (b) 2 million sensor readings per second, queried by device and time range; (c) product search with typo tolerance and relevance ranking; (d) session data for a web app, read on every request; (e) an order management system for one country with a few hundred writes per second.
:::

?? Answer to 2
(a) Distributed SQL: Spanner, for serializable, externally consistent transactions across regions. (b) A wide-column LSM store: Bigtable (or Cassandra), with a row key of device plus time bucket, for very high write throughput and range reads per device. (c) A search engine: Elasticsearch or OpenSearch, for analyzers, fuzzy matching and BM25 ranking. (d) An in-memory store: Memorystore for Valkey or Redis, for sub-millisecond reads with TTLs. (e) A single relational database: Cloud SQL or AlloyDB for PostgreSQL; the load is far below one server's capacity, so distribution would only add latency and complexity.

::: try
**3. Fix the Cassandra table.** A team stores events as `PRIMARY KEY ((sensor_id), ts)` with RF 3, writes at `ONE` and reads at `ONE`. Readings sometimes look stale, and reads of busy sensors keep getting slower. Explain both problems and propose fixes.
:::

?? Answer to 3
Stale reads: W + R = 2, which is not greater than RF = 3, so a read can reach a replica that has not received the latest write; use `LOCAL_QUORUM` for both reads and writes. Slowing reads: each sensor's partition grows without limit, so busy sensors have huge partitions spread over many SSTables. Add a time bucket to the partition key, for example `PRIMARY KEY ((sensor_id, day), ts)`, and use TimeWindowCompactionStrategy with a TTL so old data expires by whole files.

::: try
**4. Size the pool.** An API on Cloud Run serves 2,000 requests per second at peak. Each request runs two queries that hold a connection for 3 ms each. Cloud Run scales to at most 50 instances. The AlloyDB primary has 16 vCPUs. How many connections are busy on average, what would you set per-instance pool size to, and would you add PgBouncer?
:::

?? A solution to 4
Busy connections = 2,000 × 2 × 0.003 = 12 on average. With bursts, around 25 to 35 server connections is plenty, close to twice the core count. With 50 instances, a per-instance pool of 1 or 2 already allows 50 to 100 connections, so keep it small (for example 2). Adding PgBouncer in transaction mode with a server pool of about 30 lets the instance count grow later without exceeding the database's connection limit.

::: try
**5. Plan the migration.** The `orders` table (400 GB, PostgreSQL) has `amount numeric` and must become `amount_cents bigint`. Applications run as a rolling deployment, and a Datastream CDC stream feeds BigQuery. Write the step-by-step plan.
:::

?? A solution to 5
1. Expand: `ALTER TABLE orders ADD COLUMN amount_cents bigint;` with `lock_timeout` set (metadata only).
2. Deploy code that writes both `amount` and `amount_cents` on every insert and update.
3. Backfill in keyed batches: `UPDATE orders SET amount_cents = round(amount * 100) WHERE id BETWEEN ... AND amount_cents IS NULL`, throttled on replica lag.
4. Add `CHECK (amount_cents IS NOT NULL) NOT VALID`, `VALIDATE` it, then `SET NOT NULL` if required.
5. Tell the data team; update BigQuery models to use `amount_cents` while `amount` still arrives.
6. Deploy code that reads `amount_cents`, still writing both.
7. Contract: stop writing `amount`, wait until downstream models no longer use it, then drop it.

### What comes next

You have now looked inside the operational databases that feed every data platform: how PostgreSQL keeps commits durable and readers consistent, how B-trees and LSM trees trade reads for writes, how distributed SQL and Cassandra spread data and agree on it, and how caches, poolers and careful migrations keep production systems healthy. Every one of these designs is shaped by the hardware underneath: the cost of a random read, an fsync, a network round trip or a gigabyte of RAM. The next section, **Storage internals and hardware**, goes down to that level: disks and SSDs, file systems, memory hierarchies, networks and the numbers every data engineer should know.
