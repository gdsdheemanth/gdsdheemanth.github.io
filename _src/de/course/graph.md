This section is for data engineers who meet data whose value lies in its connections: payments between accounts, follows between users, roads between junctions, facts between entities. It assumes the core of the course: SQL and recursive CTEs ([SQL for data engineering](#sql)), how graphs are modelled as nodes and edges ([Data modeling](#modeling), topic Graph & Network Data Modeling), storage engines and indexes ([Databases: OLTP, NoSQL and specialised](#databases)), Spark ([Apache Spark](#spark)), streaming features ([Streaming systems](#streaming)) and Spanner ([Operational databases, AI and governance on Google Cloud](#gcpdata)). Knowledge graphs as a companion to retrieval and ML were introduced in [Data engineering for AI and ML](#aiml); here they get their construction pipeline.

**What you will be able to do afterwards**

- Write Cypher and GQL queries over a property graph, and decide between Neo4j, TigerGraph, Spanner Graph and BigQuery Graph for a workload.
- Explain how a native graph database stores nodes and relationships, why index-free adjacency makes traversals cheap, and how a Neo4j cluster replicates writes.
- Run graph algorithms (PageRank, connected components, Louvain, shortest paths, triangle counting) on billions of edges with the vertex-centric model, GraphFrames or a graph data science library, and know which ones scale.
- Build the pipelines behind knowledge graphs, fraud scoring, social recommendations and geospatial products: extraction, entity resolution, feature computation, partitioning and spatial joins.

**The big picture.** Graph work splits into two very different workloads, and most confusion comes from mixing them. **Graph queries** start from a few known nodes and explore their neighbourhood: "which accounts are within three transfers of this one?" They need low latency and touch a tiny part of the graph, which is what graph databases (Neo4j, TigerGraph, Spanner Graph) are built for. **Graph analytics** compute something about the whole graph: a rank for every page, a community for every user, the connected components of a billion identity links. They read every edge, many times, and belong in batch engines (GraphFrames on Spark, Pregel-style systems, Neo4j Graph Data Science, BigQuery). The first group of this section covers the systems for both; the second group applies them to the four places data engineers most often meet graphs.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Graph systems

A relational database can store any graph: a table of nodes and a table of edges. The trouble starts with the questions. "Friends of friends of friends" is three self-joins; "any path of any length" is a recursive CTE whose cost grows with every hop. Graph systems change the storage and the execution model so that following a connection is cheap and natural to write.

This group moves from the query side to the analytics side. It starts with the property graph model and its query languages (Cypher and the ISO standard GQL), then opens up Neo4j to see how a native graph store works, then looks at TigerGraph's parallel design and its GSQL language. The last two topics leave the database: the vertex-centric model (Pregel, GraphX, GraphFrames) for processing graphs too big for one machine, and the algorithms you run with it.

[[fig:graph_twoworkloads]]

### Graph Databases — Neo4j & Property Graph Model

::: plain
A property graph is a whiteboard drawing that the computer can query. You draw circles for things (a person, an account, a device), arrows for how they relate (OWNS, SENT_MONEY_TO, LOGGED_IN_FROM), and write small notes on both (a name, an amount, a date). A graph database stores that drawing as it is, and its query language lets you draw a small pattern, such as "a person who owns an account that sent money to an account owned by someone else", and ask for every place it occurs.
:::

**The property graph model** has four parts:

| Part | Meaning | Example |
|---|---|---|
| Node | an entity | an account |
| Label | a type or role of a node; a node may have several | `:Account`, `:Account:Flagged` |
| Relationship | a directed, typed connection between two nodes | `(:Person)-[:OWNS]->(:Account)` |
| Property | a key-value pair on a node or relationship | `{iban: "DE89...", opened: date("2025-03-01")}` |

Relationships always have exactly one type and a direction, but you can query them in either direction. They carry properties too, which is the main difference from RDF triples (see [Data modeling](#modeling) for choosing between the two and for modelling decisions such as edge versus node for events).

**Neo4j** is the most widely used graph database. It stores this model natively, runs on a single server or a cluster, and comes as Community Edition (open source), Enterprise Edition and the managed **AuraDB**, which runs on Google Cloud among others. Since 2025 it uses calendar version numbers (2025.x, 2026.x), with 5.26 as the long-term support line of the old numbering.

**Cypher by example.** Cypher describes patterns with ASCII art: `()` for nodes, `-[]->` for relationships.

```text
// create a small graph
CREATE (dana:Person {name: 'Dana'})-[:OWNS]->(a1:Account {id: 'A1'}),
       (lee:Person {name: 'Lee'})-[:OWNS]->(a2:Account {id: 'A2'}),
       (a1)-[:TRANSFER {amount: 900, at: datetime('2026-11-02T10:00')}]->(a2);

// who received money from Dana's accounts?
MATCH (:Person {name: 'Dana'})-[:OWNS]->(:Account)-[t:TRANSFER]->(:Account)<-[:OWNS]-(p:Person)
WHERE t.amount > 500
RETURN p.name AS receiver, sum(t.amount) AS total
ORDER BY total DESC;
```

The clauses read like SQL turned sideways: `MATCH` finds the pattern, `WHERE` filters, `RETURN` projects and aggregates (grouping is implicit by the non-aggregated columns), and `WITH` chains query parts the way a CTE does. Other everyday clauses:

- `OPTIONAL MATCH`: like a left outer join; missing parts become `null`.
- `MERGE`: match the whole pattern, or create it if it does not exist. The basis of idempotent loading.
- `UNWIND $rows AS row`: turn a list parameter into rows, the standard way to load batches from a driver.
- Variable-length paths: `-[:TRANSFER*1..3]->` (older syntax), or the quantified relationship `-[:TRANSFER]->{1,3}` and quantified path patterns such as `((:Account)-[:TRANSFER]->(:Account)){1,3}` (Cypher 5 and later).
- `SHORTEST k` and `ALL SHORTEST` path selectors.

**Loading data idempotently.** Pipelines load graphs in batches with `UNWIND` and `MERGE`, backed by a uniqueness constraint so that `MERGE` uses an index and duplicates cannot appear:

```text
CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE;

UNWIND $rows AS row                       // rows sent by the driver, e.g. 10,000 at a time
MERGE (s:Account {id: row.src})
MERGE (d:Account {id: row.dst})
MERGE (s)-[t:TRANSFER {txn_id: row.txn_id}]->(d)
SET t.amount = row.amount, t.at = datetime(row.at);
```

For a very large first load, the offline `neo4j-admin database import` tool builds the store files directly from CSV, which is far faster than transactional inserts.

> **Pitfall:** `MERGE` on a long pattern. `MERGE (a:Account {id: 1})-[:OWNS]-(p:Person {name: 'Dana'})` creates the *whole* pattern, including new duplicate nodes, if any part of it is missing. Merge each node on its key first, then merge the relationship between the bound variables, as above.

**Why graphs win on deep, variable-length questions.** Compare a ring detection question in SQL. This runs in SQLite and finds every path from account A, including the cycle back to it:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE transfer(src TEXT, dst TEXT, amount INT);
INSERT INTO transfer VALUES ('A','B',900),('B','C',850),('C','D',800),
  ('D','A',780),('B','E',40),('E','F',35);
""")
rows = db.execute("""
WITH RECURSIVE reach(acct, hops, path) AS (
  SELECT dst, 1, 'A>' || dst FROM transfer WHERE src = 'A'
  UNION ALL
  SELECT t.dst, r.hops + 1, r.path || '>' || t.dst
  FROM reach r JOIN transfer t ON t.src = r.acct
  WHERE r.hops < 4 AND r.acct <> 'A'                       -- stop when back at A
    AND (instr(r.path, t.dst) = 0 OR t.dst = 'A')          -- no revisits except A
)
SELECT acct, hops, path FROM reach ORDER BY hops, acct
""").fetchall()
for r in rows: print(r)
# ('B', 1, 'A>B')
# ('C', 2, 'A>B>C')
# ('E', 2, 'A>B>E')
# ('D', 3, 'A>B>C>D')
# ('F', 3, 'A>B>E>F')
# ('A', 4, 'A>B>C>D>A')
```

It works, but you manage paths as strings and cycle checks by hand. The Cypher version is one line, and the engine handles cycles (by default a relationship is not traversed twice in one path):

```text
MATCH p = (a:Account {id: 'A'})-[:TRANSFER]->{2,4}(a)
RETURN [n IN nodes(p) | n.id] AS ring;
```

**GQL, the standard.** **GQL** (ISO/IEC 39075:2024) is the first new ISO database language since SQL, and it grew out of Cypher, so the pattern syntax is nearly the same. In parallel, **SQL/PGQ** (part of SQL:2023) defines property graphs over relational tables, queried from SQL with `GRAPH_TABLE`. Neo4j is converging on GQL: Cypher 25 (selected per query with the `CYPHER 25` prefix or per database) adds GQL features while Cypher 5 stays frozen for compatibility.

On Google Cloud the same GQL dialect runs in two places, with the same schema language. **Spanner Graph** (GA) defines a property graph over Spanner tables for transactional workloads; [Operational databases, AI and governance on Google Cloud](#gcpdata) shows it. **BigQuery Graph** (generally available since September 2026, after a preview from April 2026) does the same over BigQuery tables for analytical graph queries, without copying data:

```sql
CREATE OR REPLACE PROPERTY GRAPH graph_db.FinGraph
  NODE TABLES (graph_db.Account, graph_db.Person)
  EDGE TABLES (
    graph_db.PersonOwnAccount
      SOURCE KEY (id) REFERENCES Person (id)
      DESTINATION KEY (account_id) REFERENCES Account (id) LABEL Owns,
    graph_db.AccountTransferAccount
      SOURCE KEY (id) REFERENCES Account (id)
      DESTINATION KEY (to_id) REFERENCES Account (id) LABEL Transfers);

GRAPH graph_db.FinGraph
MATCH ANY SHORTEST (src:Account {id: 7})-[e:Transfers]->{1, 3}(dst:Account {id: 20})
RETURN src.id AS from_id, dst.id AS to_id;
```

GQL queries in BigQuery need on-demand pricing or an Enterprise or Enterprise Plus reservation. Results can be fed back into SQL with `GRAPH_TABLE`, so a graph pattern becomes one step of a normal SQL pipeline.

| Option | Best for | Storage |
|---|---|---|
| Neo4j (AuraDB or self-managed) | interactive traversals, rich Cypher ecosystem, graph data science | native graph store |
| Spanner Graph | operational graph queries inside OLTP transactions at global scale | Spanner tables |
| BigQuery Graph | analytical pattern queries over warehouse data, joined with SQL | BigQuery tables |
| Graph tables in a lakehouse plus Spark | whole-graph batch algorithms | Parquet or Iceberg |

> **Tip:** Keep the system of record relational and treat the graph as a derived view unless the graph *is* the product. A graph built from tables with a nightly or streaming pipeline can be rebuilt; a graph that is the only copy of your data needs the same backup and migration discipline as any primary database.

::: deeper
**Path semantics.** A pattern with a cycle can match infinitely many walks, so every language restricts them. Cypher's default is relationship isomorphism (*trail* semantics): a relationship appears at most once per path. GQL lets you choose `WALK`, `TRAIL`, `ACYCLIC` (no repeated nodes) or `SIMPLE`, and path search prefixes (`ANY`, `ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k`) bound the result. Always bound variable-length patterns with an upper limit; `-[*]->` on a dense graph can enumerate millions of paths.

**How a pattern is executed.** The planner picks an anchor (a node found through an index, ideally the most selective one), then expands relationships step by step, filtering as early as it can. `EXPLAIN` shows the plan, `PROFILE` runs it and reports rows and database hits per operator. A plan starting with `AllNodesScan` or `NodeByLabelScan` on a big label usually means a missing index or constraint.

**Parameters.** Always send values as parameters (`$name`), never by string concatenation: it prevents injection and lets the server cache the plan.

> **Interview:** "When would you *not* use a graph database?" When queries are aggregations over many rows (warehouse territory), when relationships are shallow and fixed (one or two joins are cheap in SQL), or when the team cannot operate another database. Spanner Graph and BigQuery Graph remove the third objection on Google Cloud, because the graph is a view over tables you already run.
:::

::: check
?? What is the difference between a label and a relationship type in a property graph?
A label classifies a node, and a node can have several labels. A relationship type classifies a relationship, and each relationship has exactly one type and one direction.

?? Why should a loading job merge each node on its key before merging the relationship?
`MERGE` creates the whole pattern if any part is missing, so merging a long pattern can create duplicate nodes. Merging nodes on unique keys first, then the relationship between bound nodes, is idempotent.

?? What do Spanner Graph and BigQuery Graph have in common, and how do they differ?
Both define a property graph over existing tables and query it with the same GQL dialect and schema language. Spanner Graph serves transactional, low-latency queries; BigQuery Graph serves analytical queries over warehouse data and combines with SQL through `GRAPH_TABLE`.
:::

::: resources
- Docs: [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/current/) · clauses, patterns, quantified paths and shortest path selectors
- Docs: [Introduction to BigQuery Graph](https://docs.cloud.google.com/bigquery/docs/graph-overview) · graph schemas over BigQuery tables, GQL queries and visualisation
- Docs: [Spanner Graph overview](https://docs.cloud.google.com/spanner/docs/graph/overview) · property graphs and GQL inside Spanner
- Book: [Graph Databases, 2nd edition](https://www.oreilly.com/library/view/graph-databases-2nd/9781491930885/) · Robinson, Webber and Eifrem; the property graph model and Cypher with worked domains
- Spec: [ISO/IEC 39075:2024 GQL](https://www.iso.org/standard/76120.html) · the international standard graph query language
- Course: [Neo4j GraphAcademy](https://graphacademy.neo4j.com/) · free hands-on courses on Cypher, modelling and importing data
:::

### Graph Databases — Neo4j Internals

::: plain
Imagine a library where every book has, glued inside its cover, a list of shelf positions for every book it cites. To follow a citation you walk straight to the shelf; you never consult the catalogue. That is **index-free adjacency**: each node record points directly at its relationships, and each relationship points at the next one, so following a connection costs the same whether the library holds a thousand books or a billion. The previous topic was about what you can ask a graph; this one is about how Neo4j lays the graph out on disk and keeps copies of it in a cluster.
:::

**Native versus non-native graph storage.** A non-native graph engine stores nodes and edges in tables or a key-value store and finds neighbours through an index: each hop is an index lookup, roughly O(log n) in the size of the index, repeated for every node on the frontier. A native store such as Neo4j's keeps adjacency as physical pointers, so each hop is O(1) in the size of the graph and proportional only to the degree of the node you are expanding. That is the whole argument for native graphs: deep traversals do not slow down as the graph grows. The cost is that everything else (scans, aggregations, partitioning across machines) is harder.

**The record store.** Neo4j's classic store formats (`standard` and `aligned`) are separate files of **fixed-size records**, so the record for id *n* lives at byte offset *n* × record size and can be read without any lookup:

| Store file | One record holds |
|---|---|
| Node store | in-use flag, id of the first relationship, id of the first property, label information |
| Relationship store | start node, end node, type, and four pointers: previous and next relationship of the start node, previous and next of the end node, plus the first property |
| Property store | key, type and value, chained to the next property; long strings and arrays spill into dynamic stores |
| Label and token stores | the names behind label, type and property-key ids |

In the standard format a node record is 15 bytes and a relationship record 34 bytes. The relationship record is the clever part: it sits in **two doubly linked lists** at once, the chain of its start node and the chain of its end node. Reading a node's relationships means following its chain.

[[fig:graph_records]]

The simulation below builds that layout in two byte arrays (node records hold only a first-relationship pointer, relationship records hold the two endpoints and four chain pointers) and walks the chains:

```python
import struct

NODE = struct.Struct("<i")          # first_rel id (-1 = none)
REL = struct.Struct("<iiiiii")      # src, dst, src_prev, src_next, dst_prev, dst_next
node_store, rel_store = bytearray(), bytearray()

def add_node():
    node_store.extend(NODE.pack(-1))
    return len(node_store) // NODE.size - 1

def node_first(n):                  # offset = id * record size: no index lookup
    return NODE.unpack_from(node_store, n * NODE.size)[0]

def rel(r):
    return list(REL.unpack_from(rel_store, r * REL.size))

def put_rel(r, fields):
    REL.pack_into(rel_store, r * REL.size, *fields)

def add_rel(a, b):
    r = len(rel_store) // REL.size
    rel_store.extend(bytes(REL.size))
    put_rel(r, [a, b, -1, node_first(a), -1, node_first(b)])
    for n in (a, b):                # new relationship goes to the head of each chain
        old = node_first(n)
        if old != -1:
            f = rel(old)
            if f[0] == n: f[2] = r  # n is the old rel's source: fix its src_prev
            if f[1] == n: f[4] = r  # n is the old rel's target: fix its dst_prev
            put_rel(old, f)
        NODE.pack_into(node_store, n * NODE.size, r)
    return r

def neighbours(n):
    out, r = [], node_first(n)
    while r != -1:                  # pointer chasing along n's chain
        src, dst, _, src_next, _, dst_next = rel(r)
        out.append(dst if src == n else src)
        r = src_next if src == n else dst_next
    return out

alice, bob, carol, dave = (add_node() for _ in range(4))
add_rel(alice, bob); add_rel(alice, carol); add_rel(dave, alice); add_rel(bob, carol)
print(neighbours(alice))            # [3, 2, 1]
print(neighbours(carol))            # [1, 0]
print(len(node_store), len(rel_store))   # 16 96
```

The doubly linked chains make deleting a relationship O(1) (unlink it from both chains) and let a traversal start from either end.

**Dense nodes.** A celebrity account with ten million followers has a ten-million-long chain; finding its few `OWNS` relationships means walking past all the `FOLLOWS` ones. Neo4j therefore converts nodes above a degree threshold into **dense nodes**: the node points to **relationship group** records, one per relationship type and direction, each with its own chain. Queries that name a type and direction skip the rest.

**The block format.** Since 5.22, new Enterprise databases default to the **block** format (Community Edition still uses `aligned`; `standard` and `high_limit` are deprecated since 5.23). Block format keeps a node, its labels, its properties and, for most nodes, its relationships together in the same fixed-size blocks, so one page read brings in what a traversal step needs, instead of hopping across three files. It also lifts the old limits: up to 2^48 nodes, with no fixed upper bound on relationships and properties.

**Memory and durability.** All store files are read through the **page cache**, an off-heap cache of 8 KiB pages that Neo4j manages itself; sizing it to hold the whole store (`server.memory.pagecache.size`) is the single most important tuning step, because index-free adjacency only stays O(1) when the pointer you chase is in memory. Writes go first to the **transaction log** (a write-ahead log), are applied to pages in memory, and are flushed by periodic **checkpoints**; after a crash, recovery replays the log since the last checkpoint.

**Indexes still exist.** Index-free adjacency is about *hops*. Finding the *starting* nodes needs indexes: range indexes (equality, ranges, prefix), text indexes, point indexes, full-text indexes (Lucene), vector indexes (for embeddings) and token lookup indexes (which nodes have a label, which relationships have a type).

**Clustering.** A Neo4j cluster is a set of **servers**; each database is hosted on some of them in one of two modes:

- **Primaries** (typically 3 or 5) replicate every transaction with the **Raft** consensus protocol. One primary is the leader for that database; a commit succeeds once a majority has written it to their logs, so 3 primaries survive 1 failure.
- **Secondaries** replicate asynchronously from primaries. They scale reads and analytics (Graph Data Science jobs are often run on them) and do not slow down writes.

```text
CREATE DATABASE fraud TOPOLOGY 3 PRIMARIES 2 SECONDARIES;
```

Drivers connect with the `neo4j://` scheme, fetch a **routing table** and send writes to the leader and reads to any member. **Bookmarks** give causal consistency: a client passes the bookmark of its last write, and a secondary waits until it has applied that transaction before answering, so you always read your own writes. Neo4j does not shard a single graph across machines in this design; every member holds a full copy. **Composite databases** (built on the Fabric feature) let one query span several databases, which is how very large or multi-tenant graphs are split by hand.

**APOC** ("Awesome Procedures on Cypher") is the standard library of procedures and functions. APOC Core ships with Neo4j and is supported; APOC Extended is community-maintained. Typical uses in pipelines: `apoc.load.json` to read JSON from a URL or file, `apoc.periodic.iterate` for batched updates, `apoc.path.expandConfig` for traversals with label and type filters, and `apoc.refactor` procedures to merge duplicate nodes. Since Neo4j 5, `CALL { ... } IN TRANSACTIONS OF 10000 ROWS` covers most batching that used to need `apoc.periodic.iterate`.

> **Pitfall:** A page cache smaller than the store. Traversals then turn every pointer hop into a random disk read and performance falls off a cliff, which teams misdiagnose as "graphs do not scale". Check the page cache hit ratio before anything else.

::: deeper
**Why not shard?** Partitioning a graph cuts edges, and every cut edge on a traversal path becomes a network hop, which destroys the O(1) hop. Social graphs have no clean cuts. Neo4j's answer is vertical scale plus read replicas (secondaries) and manual splitting through composite databases; TigerGraph (next topic) and Spanner Graph take the other side, distributing the graph and paying network hops for scale.

**Locking and isolation.** Neo4j provides read-committed isolation by default with write locks on nodes and relationships. Adding a relationship locks both endpoints, so many concurrent writers attaching relationships to one supernode serialise on it; batch such writes, or sort them by node id to avoid deadlocks.

**IDs are internal.** Record ids are reused after deletion (in the classic formats), so never store `id(n)` or `elementId(n)` outside the database as a business key. Give every node a stable key property with a uniqueness constraint.

**Backups and upgrades.** Online backups (`neo4j-admin database backup`, Enterprise) produce full and differential artifacts; `neo4j-admin database migrate` converts store formats, which is a planned step now that `standard` and `high_limit` are deprecated.

> **Interview:** "Explain index-free adjacency and its limits." Each node record holds a direct pointer to its relationship chain, so expanding a node costs time proportional to its degree, independent of graph size. Limits: it needs the store in memory, supernodes make expansion expensive, and it makes sharding hard because a cut edge becomes a network call.
:::

::: check
?? Why can Neo4j read node 1,000,000 without an index lookup?
Records have a fixed size, so the record for id *n* is at offset *n* times the record size in the store file.

?? What problem do relationship groups solve?
On dense nodes, walking one long chain of every relationship to find a few of one type is slow. Groups give a separate chain per type and direction, so a query naming the type skips the others.

?? In a cluster with 3 primaries and 2 secondaries, how many servers must acknowledge a write, and what do bookmarks guarantee?
A majority of the primaries (2 of 3) through Raft. Bookmarks let a client read its own writes on any member, because the member waits until it has applied the bookmarked transaction.
:::

::: resources
- Docs: [Neo4j Operations Manual: database internals](https://neo4j.com/docs/operations-manual/current/database-internals/) · transaction logs, checkpoints, locks and store formats
- Docs: [Neo4j store formats](https://neo4j.com/docs/operations-manual/current/database-internals/store-formats/) · block, aligned, standard and high_limit, with limits and deprecations
- Docs: [Neo4j clustering](https://neo4j.com/docs/operations-manual/current/clustering/) · servers, primaries and secondaries, Raft, routing and bookmarks
- Docs: [APOC Core documentation](https://neo4j.com/docs/apoc/current/) · every procedure and function in the standard library
- Book: [Graph Databases, 2nd edition](https://www.oreilly.com/library/view/graph-databases-2nd/9781491930885/) · chapter 6, "Graph Database Internals", on native storage and index-free adjacency
- Paper: [The LDBC Social Network Benchmark](https://arxiv.org/abs/2001.02299) · the standard workload for comparing graph databases
:::

### TigerGraph & GSQL

::: plain
Neo4j is a single brilliant librarian who knows where every book is. TigerGraph is a team of librarians, each responsible for one section of a very large library, all working at the same time: when you ask a question that spans the whole collection, every librarian searches their section in parallel and they pass notes to each other. That is what "native parallel graph" means, and GSQL is the language you use to hand the team its instructions, including the notepads (accumulators) on which they collect results.
:::

**What TigerGraph is.** A distributed graph database written in C++ that partitions one graph across many machines and runs each query step in parallel over all of them. Its design goals are deep traversals (many hops) and whole-graph analytics on graphs too large for one server, with fresh data. It runs self-managed (including on Google Kubernetes Engine with its Kubernetes operator) or as the managed TigerGraph Savanna service.

**Native parallel graph (NPG)** combines three ideas:

- **Native storage**: vertices and edges are stored as graph structures (adjacency lists) in a compressed format, mostly in memory, with disk persistence.
- **Automatic partitioning**: vertices are hashed across the machines of the cluster with their outgoing edges; you do not choose a partitioning scheme.
- **MPP execution**: each step of a query is a bulk-synchronous step that runs on every partition at once, the Pregel idea (two topics on) inside a database.

**The schema.** TigerGraph is schema-first: every vertex and edge type is declared, which lets it store them compactly and compile queries.

```text
CREATE VERTEX Account (PRIMARY_ID id STRING, opened DATETIME, risk FLOAT)
CREATE VERTEX Device (PRIMARY_ID id STRING, os STRING)
CREATE VERTEX Card (PRIMARY_ID fingerprint STRING)
CREATE DIRECTED EDGE TRANSFER (FROM Account, TO Account, amount DOUBLE, ts DATETIME)
  WITH REVERSE_EDGE="reverse_TRANSFER"
CREATE UNDIRECTED EDGE USED_DEVICE (FROM Account, TO Device, first_seen DATETIME)
CREATE UNDIRECTED EDGE USED_CARD (FROM Account, TO Card)
CREATE GRAPH Fraud (Account, Device, Card, TRANSFER, USED_DEVICE, USED_CARD)
```

`WITH REVERSE_EDGE` stores a mirror edge so you can traverse a directed edge backwards efficiently ("who sent money to this account?").

**Loading jobs** map files or Kafka topics to vertices and edges declaratively, and loading is an upsert on the primary id, so re-running a load is safe:

```text
CREATE LOADING JOB load_transfers FOR GRAPH Fraud {
  DEFINE FILENAME f;
  LOAD f TO EDGE TRANSFER VALUES ($"src", $"dst", $"amount", $"ts")
    USING header="true", separator=",";
}
RUN LOADING JOB load_transfers USING f="/data/transfers.csv"
```

**GSQL queries.** A GSQL query is a procedure made of **SELECT blocks**. Each block starts from a set of vertices, follows one edge pattern to a new set, and runs code on every matched edge in parallel. Results are collected in **accumulators**: typed variables that many parallel workers can add to safely, because they define how contributions combine.

| Accumulator | Combines contributions by | Typical use |
|---|---|---|
| `SumAccum<INT>` | adding | counts, totals |
| `MaxAccum` / `MinAccum` | keeping the largest or smallest | shortest distance, latest time |
| `OrAccum` / `AndAccum` | logical or / and | visited flags |
| `SetAccum<T>` / `ListAccum<T>` | union / append | collecting ids |
| `MapAccum<K, V>` | per-key accumulation | breakdowns |
| `HeapAccum<T>` | keeping the top k | ranked results |

A name with one `@` is attached to each vertex (`@visited`), with two `@@` it is global (`@@total`). This query expands from a seed account through shared devices, up to a hop limit, and counts the accounts it reaches:

```text
CREATE QUERY shared_device_reach(VERTEX<Account> seed, INT max_hops) FOR GRAPH Fraud SYNTAX v2 {
  OrAccum @visited;
  SumAccum<INT> @@reached;
  Start = {seed};
  Start = SELECT s FROM Start:s POST-ACCUM s.@visited = TRUE;
  WHILE Start.size() > 0 LIMIT max_hops DO
    Start = SELECT t
            FROM Start:s -(USED_DEVICE)- Device:d -(USED_DEVICE)- Account:t
            WHERE t.@visited == FALSE
            POST-ACCUM t.@visited = TRUE;
    @@reached += Start.size();
  END;
  PRINT @@reached;
}
INSTALL QUERY shared_device_reach
RUN QUERY shared_device_reach("acc1", 3)
```

Read it as a breadth-first search written by hand: each loop iteration is one parallel step over the current frontier. The explicit control flow (`WHILE`, `IF`, `FOREACH`) and accumulators are why GSQL can express algorithms such as PageRank or label propagation inside the database, which Cypher alone cannot. `INSTALL QUERY` compiles the query to C++ for speed; queries can also run in interpreted mode while you develop them. Recent versions also accept openCypher queries.

**Real-time analytics and fraud patterns.** TigerGraph's typical use is answering a multi-hop question while a transaction waits: during a payment authorisation, compute "how many accounts within three hops of this card have a chargeback in the last 90 days?" and return it as a feature to the risk model. Common patterns:

- **Shared-identifier rings**: accounts linked through devices, cards, phone numbers or addresses (the query above).
- **Money mule chains**: funds moving through several accounts quickly, each keeping a little (a path query over `TRANSFER` with time and amount filters).
- **Graph features for models**: degree, community id, PageRank or the number of flagged neighbours, computed in the graph and written back as vertex attributes for scoring.

The topic Fraud Detection Data Engineering later in this section builds the full pipeline around such features.

**Schema design rules** that apply beyond TigerGraph:

1. **Make linking identifiers vertices, not attributes.** If `device_id` is only an attribute of `Account`, finding accounts sharing a device is a scan. As a `Device` vertex, it is a two-hop traversal.
2. **Use reverse edges** for every directed edge you will traverse backwards.
3. **Watch for supernodes.** A `Country` vertex or a shared public Wi-Fi IP connects to millions of accounts and makes every traversal through it explode. Model such values as attributes, or exclude high-degree identifiers (a "known shared" list) from linking.
4. **Put time on edges** and filter by it, so traversals reflect the graph at the relevant moment.

> **Pitfall:** Unbounded expansion in a real-time query. A three-hop query through one supernode can touch most of the graph and blow the latency budget for every request. Cap the hop count, cap the fan-out per vertex (sample or skip vertices above a degree limit), and set a query timeout.

::: deeper
**Execution model.** Each SELECT block is one bulk-synchronous superstep: all partitions process their part of the frontier, `ACCUM` clauses run per matched edge in parallel (so they may only add to accumulators, never read a value another worker is changing), then a barrier, then `POST-ACCUM` runs once per target vertex. This is why accumulator semantics matter: the result does not depend on the order in which parallel workers contribute.

**Distributed queries.** By default a query runs coordinated from one server, fetching remote data as needed, which is efficient for queries that touch a small part of the graph. `CREATE DISTRIBUTED QUERY` runs the work on every server, which suits queries that touch most of the graph (whole-graph algorithms).

**Graph Data Science library.** TigerGraph ships a library of algorithms written in GSQL (centrality, community, similarity, path, embeddings) that you install and call like any query, and its ML workbench exports graph features to Python training code.

**Comparison with Neo4j.** Neo4j: one full copy per server, Cypher and GQL, a large ecosystem, easiest for interactive queries up to the size of one machine's memory. TigerGraph: one graph partitioned across servers, procedural GSQL with accumulators, strongest for deep or whole-graph queries at scale, a smaller ecosystem and a schema you must design upfront. Spanner Graph and BigQuery Graph: graph queries over tables you already run, with the operational model of those services.

> **Interview:** "Design a real-time fraud graph lookup under 50 ms." Keep linking identifiers as vertices, precompute expensive whole-graph features (community, PageRank) offline and store them as attributes, keep real-time queries to two or three hops with fan-out caps, and fall back to a default score if the query times out so payments are never blocked by the graph.
:::

::: check
?? Why does GSQL need accumulators?
Each SELECT block runs on many vertices and edges in parallel. Accumulators define how parallel contributions combine (sum, max, set union and so on), so results are correct regardless of order.

?? A query to find accounts that share devices is slow because `device_id` is an attribute. What should change?
Model devices as vertices linked to accounts by edges. Then sharing a device is a two-hop traversal instead of a scan and comparison of attributes.

?? What is the difference between `@visited` and `@@reached`?
`@visited` is a vertex-attached accumulator (one value per vertex); `@@reached` is a global accumulator (one value per query).
:::

::: resources
- Docs: [TigerGraph documentation](https://docs.tigergraph.com/) · installation, schema, loading, GSQL and the graph data science library
- Docs: [GSQL language reference](https://docs.tigergraph.com/gsql-ref/current/intro/) · SELECT blocks, accumulators, control flow and query modes
- Paper: [TigerGraph: A Native MPP Graph Database](https://arxiv.org/abs/1901.08248) · the storage and parallel execution design behind NPG
- Blog: [Why fraud and risk teams are adopting graph analytics](https://www.tigergraph.com/blog/why-fraud-risk-teams-are-adopting-graph-analytics/) · fraud patterns that graphs expose
- Repo: [tigergraph/ecosys](https://github.com/tigergraph/ecosys) · example schemas, loading jobs and queries, including benchmarks
:::

### Distributed Graph Processing — Pregel, GraphX

::: plain
How do you compute something about a graph with ten billion edges that no single machine can hold? Pregel's answer is to "think like a vertex". Every vertex is a tiny worker that only knows its own value and its own edges. In rounds, everyone reads the letters they received, updates their value, and posts letters to their neighbours; then everyone waits for the post to be delivered before the next round. When nobody has anything new to say, the computation is finished.
:::

**The vertex-centric model.** Google's **Pregel** (2010) organises a graph computation as a sequence of **supersteps**, following the bulk synchronous parallel (BSP) model:

1. In superstep *s*, every **active** vertex runs the same user function, `compute(messages)`, on the messages sent to it in superstep *s* - 1.
2. The function may change the vertex's value, send messages along its edges (delivered in superstep *s* + 1), and **vote to halt**.
3. A **barrier** separates supersteps: all messages are delivered before the next one starts.
4. A halted vertex is reactivated by an incoming message. The job ends when all vertices have halted and no messages are in flight.

Single-source shortest paths in this model: the source starts with distance 0; a vertex whose distance improves sends `distance + weight` to each neighbour; vertices that receive nothing better stay silent. The simulation prints the state after each superstep:

```python
import math
from collections import defaultdict

edges = {"A": [("B", 4), ("C", 1)], "C": [("B", 2), ("D", 5)], "B": [("D", 1)], "D": []}
value = {v: math.inf for v in edges}
inbox = {"A": [0]}                       # superstep 0: the source gets distance 0
step = 0
while inbox:                             # stop when every vertex has halted and no messages flow
    outbox = defaultdict(list)
    for v, msgs in inbox.items():        # only vertices with messages are active
        best = min(msgs)                 # a min combiner could do this on the sender side
        if best < value[v]:
            value[v] = best
            for dst, w in edges[v]:
                outbox[dst].append(best + w)
    print(step, dict(sorted(value.items())), "messages:", sum(map(len, outbox.values())))
    inbox, step = outbox, step + 1
# 0 {'A': 0, 'B': inf, 'C': inf, 'D': inf} messages: 2
# 1 {'A': 0, 'B': 4, 'C': 1, 'D': inf} messages: 3
# 2 {'A': 0, 'B': 3, 'C': 1, 'D': 5} messages: 1
# 3 {'A': 0, 'B': 3, 'C': 1, 'D': 4} messages: 0
```

B first hears 4 (directly from A), then the better 3 (through C); D improves from 5 to 4 one superstep later. Two Pregel features cut the cost:

- **Combiners** merge messages to the same vertex on the sending machine (here, keep only the minimum), so far fewer messages cross the network.
- **Aggregators** compute global values each superstep (the total change in PageRank, the number of active vertices), used for convergence tests.

Fault tolerance is simple: workers checkpoint vertex values and messages to durable storage every few supersteps; on failure, the job restarts from the last checkpoint. Open-source descendants include **Apache Giraph** (used by Facebook for graphs with up to a trillion edges) and the Pregel operators in GraphX and GraphFrames.

[[fig:graph_pregel]]

**GraphX** is Spark's graph library (Scala and Java). A `Graph[VD, ED]` is two RDDs: a **VertexRDD** of `(id, value)` pairs and an **EdgeRDD** of `(srcId, dstId, value)`, stored with indexes inside each partition. An **EdgeTriplet** joins an edge with both endpoint values, and `aggregateMessages` is the core primitive: a function runs on each triplet and may send a message to either endpoint, and messages are reduced per vertex. The `pregel` operator iterates that until no messages remain. Shortest paths, from the GraphX guide:

```text
// Scala (GraphX)
val initial = graph.mapVertices((id, _) =>
  if (id == sourceId) 0.0 else Double.PositiveInfinity)
val sssp = initial.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => math.min(dist, newDist),          // vertex program
  t => if (t.srcAttr + t.attr < t.dstAttr)                 // send messages
         Iterator((t.dstId, t.srcAttr + t.attr)) else Iterator.empty,
  (a, b) => math.min(a, b))                                // combiner
```

GraphX has had no significant new features for years and has no Python API. Python users work with **GraphFrames**, a DataFrame-based graph library: vertices and edges are DataFrames (with `id`, and `src` and `dst` columns), it supports motif finding, `aggregateMessages`, a Pregel API and the standard algorithms, and it has been actively developed again since 2025 (Spark 4 and Spark Connect support, a `graphframes-py` package on PyPI). On Google Cloud it runs on Managed Service for Apache Spark (formerly Dataproc) or its serverless mode for Spark batches (formerly Dataproc Serverless), reading node and edge tables from BigQuery or Iceberg:

```python
from graphframes import GraphFrame

v = spark.read.table("netdata.accounts").selectExpr("account_id AS id", "risk")
e = spark.read.table("netdata.transfers").selectExpr("src_account AS src", "dst_account AS dst", "amount")
g = GraphFrame(v, e)

spark.sparkContext.setCheckpointDir("gs://my-bucket/graph-checkpoints")  # iterative jobs need it
components = g.connectedComponents()               # adds a 'component' column
ranks = g.pageRank(resetProbability=0.15, maxIter=10)
loops = g.find("(a)-[]->(b); (b)-[]->(a)")         # motif: two accounts paying each other
```

**Partitioning a graph.** How you split a graph across machines decides how much data crosses the network.

| Strategy | What is assigned to machines | What is replicated | Good for |
|---|---|---|---|
| Edge-cut (Pregel, Giraph) | vertices, with their out-edges | messages along cut edges | graphs with even degrees |
| Vertex-cut (PowerGraph, GraphX) | edges | vertices that have edges on several machines (mirrors) | power-law graphs with supernodes |

Real graphs follow a **power law**: a few vertices have millions of edges. With an edge-cut, the machine holding a celebrity vertex does a huge share of the work. **PowerGraph** (2012) introduced the vertex-cut and the **gather-apply-scatter (GAS)** model: a supernode's edges are spread over many machines, each gathers a partial result locally, and only the partial sums travel to the vertex's master copy. GraphX adopted vertex-cuts with strategies such as `RandomVertexCut`, `CanonicalRandomVertexCut`, `EdgePartition1D` and `EdgePartition2D`. The 2D strategy arranges machines in a grid and places each edge by (source, destination) hash, which bounds the number of copies of any vertex by about 2√N for N partitions. The **replication factor** (average copies per vertex) is the number to watch: lower means less communication.

> **Pitfall:** Running iterative graph jobs on Spark without checkpointing. Every iteration extends the lineage, so after a few dozen iterations planning time and stack depth explode, and a failure recomputes from the start. Set a checkpoint directory (Cloud Storage), and let the library checkpoint every few iterations.

::: deeper
**Synchronous versus asynchronous.** BSP is easy to reason about but every superstep waits for the slowest worker. Asynchronous engines (GraphLab, PowerGraph's async mode) let vertices read their neighbours' latest values and converge faster for algorithms such as PageRank, at the cost of determinism and harder fault tolerance. Most production systems stayed synchronous.

**When the model is a poor fit.** Algorithms whose number of supersteps equals the graph diameter (BFS, label-propagation components) are slow on graphs with long chains: a path of a million vertices needs a million supersteps. Better algorithms reduce rounds (the next topic shows a components algorithm with logarithmic rounds), or a single large machine finishes faster: a graph with a few billion edges fits in the memory of one large VM, and single-machine engines avoid all network cost. Measure before distributing.

**GraphX internals.** Each edge partition keeps a local index of its vertices; a **routing table** records which edge partitions need each vertex, so vertex values are shipped only where needed and reused across iterations (incremental view maintenance of the triplets). That is what makes GraphX faster than writing the same iteration with plain joins.

For Spark-side performance (shuffles, skew, checkpointing and memory), see [Shuffle internals](pyspark-streaming-kafka-topic-map.html#shuffle) and [Data skew](pyspark-streaming-kafka-topic-map.html#skew) in the PySpark + Kafka guide.

> **Interview:** "Why does PowerGraph use a vertex-cut?" Because in power-law graphs a few vertices have huge degrees. An edge-cut puts all of a supernode's edges and work on one machine. A vertex-cut spreads its edges across machines and combines partial results, balancing work at the cost of replicating vertex state.
:::

::: check
?? When does a Pregel computation stop?
When every vertex has voted to halt and no messages are in transit. A halted vertex becomes active again if it receives a message.

?? What does a combiner do, and when is it allowed?
It merges messages addressed to the same vertex before they are sent (for example keeping only the minimum distance). It is allowed when the vertex program only depends on a commutative, associative reduction of its messages.

?? Why is a vertex-cut better than an edge-cut for a social graph?
Social graphs have supernodes. A vertex-cut spreads a supernode's edges over many machines and replicates only its value, so no single machine carries all its work.
:::

::: resources
- Paper: [Pregel: A System for Large-Scale Graph Processing](https://research.google/pubs/pub36726/) · Google's vertex-centric BSP model, combiners, aggregators and fault tolerance
- Docs: [Spark GraphX programming guide](https://spark.apache.org/docs/latest/graphx-programming-guide.html) · property graphs, aggregateMessages, the Pregel API and partition strategies
- Docs: [GraphFrames](https://graphframes.io/) · DataFrame-based graphs for PySpark: motifs, Pregel and algorithms
- Paper: [PowerGraph: Distributed Graph-Parallel Computation on Natural Graphs](https://www.usenix.org/conference/osdi12/technical-sessions/presentation/gonzalez) · OSDI 2012; vertex-cuts and gather-apply-scatter
- Paper: [GraphX: Graph Processing in a Distributed Dataflow Framework](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/gonzalez) · OSDI 2014; how GraphX maps graphs onto Spark
- Docs: [Apache Giraph](https://giraph.apache.org/) · the open-source Pregel implementation on Hadoop
:::

### Graph Algorithms at Scale

::: plain
Graph algorithms answer questions about the shape of a whole network. Who is most important (centrality)? Which groups hang together (communities)? What is the quickest route (shortest paths)? How tightly knit is a neighbourhood (triangles)? On a whiteboard with ten circles you can answer by eye. On a billion edges you need algorithms whose cost grows gently with size, and a plan for where the data lives while they run.
:::

**The families.**

| Family | Question | Algorithms | Cost on a big graph |
|---|---|---|---|
| Centrality | which nodes matter most | degree, PageRank, betweenness, closeness | PageRank cheap per iteration; betweenness very expensive |
| Components | which nodes are connected at all | weakly and strongly connected components | near linear |
| Community | which nodes form dense groups | label propagation, Louvain, Leiden | near linear per pass |
| Paths | how to get from A to B | BFS, Dijkstra, A*, delta-stepping | cheap from one source; all pairs impossible |
| Structure | how clustered is the graph | triangle count, clustering coefficient, k-core | superlinear, needs care with supernodes |
| Similarity and embeddings | which nodes are alike | Jaccard, node similarity, FastRP, node2vec | grows quickly with degree |

**PageRank.** A random surfer follows a random outgoing link with probability *d* (the damping factor, usually 0.85) and jumps to a random page otherwise. A page's rank is the long-run fraction of time the surfer spends there:

PR(v) = (1 - d) / N + d × Σ PR(u) / outdegree(u), over pages u linking to v.

It is computed by **power iteration**: start everyone at 1/N and apply the formula until ranks stop changing.

```python
links = {"A": ["B", "C"], "B": ["C"], "C": ["A"], "D": ["C"]}
nodes, d = sorted(links), 0.85
rank = {n: 1 / len(nodes) for n in nodes}
for it in range(50):
    new = {n: (1 - d) / len(nodes) for n in nodes}
    for src, outs in links.items():
        for dst in outs:
            new[dst] += d * rank[src] / len(outs)
    delta = sum(abs(new[n] - rank[n]) for n in nodes)
    rank = new
    if delta < 1e-6:
        break
print(it + 1, {n: round(r, 3) for n, r in rank.items()})
# 28 {'A': 0.373, 'B': 0.196, 'C': 0.394, 'D': 0.038}
```

C ranks highest (three pages link to it), A is close behind because C's whole vote goes to A, and D, which nobody links to, keeps only the random-jump share 0.15/4 = 0.0375. Each iteration is one pass over the edges, which is a join plus a group-by. That means PageRank runs in plain SQL too; this BigQuery script iterates 20 times over node and edge tables:

```sql
DECLARE i INT64 DEFAULT 0;
DECLARE n INT64 DEFAULT (SELECT COUNT(*) FROM netdata.nodes);
CREATE TEMP TABLE outdeg AS SELECT src, COUNT(*) AS d FROM netdata.edges GROUP BY src;
CREATE TEMP TABLE pr AS SELECT node_id, 1.0 / n AS rank FROM netdata.nodes;
WHILE i < 20 DO
  CREATE OR REPLACE TEMP TABLE pr_next AS
  SELECT v.node_id, 0.15 / n + 0.85 * IFNULL(SUM(p.rank / o.d), 0) AS rank
  FROM netdata.nodes AS v
  LEFT JOIN netdata.edges AS e ON e.dst = v.node_id
  LEFT JOIN pr AS p ON p.node_id = e.src
  LEFT JOIN outdeg AS o ON o.src = e.src
  GROUP BY v.node_id;
  CREATE OR REPLACE TEMP TABLE pr AS SELECT * FROM pr_next;
  SET i = i + 1;
END WHILE;
SELECT * FROM pr ORDER BY rank DESC LIMIT 20;
```

Two refinements matter in practice. **Dangling nodes** (no outgoing links) leak rank out of the system; the usual fix redistributes their rank evenly each iteration. **Personalized PageRank** jumps back to a chosen set of nodes instead of a random one, which ranks nodes by closeness to that set: the basis of "accounts related to this fraudster" and of many recommendation systems.

**Connected components.** On one machine, **union-find** (disjoint sets) is almost linear:

```python
parent = {}
def find(x):
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]      # path halving keeps trees flat
        x = parent[x]
    return x
def union(a, b):
    ra, rb = find(a), find(b)
    if ra != rb: parent[max(ra, rb)] = min(ra, rb)
for a, b in [("u1", "u2"), ("u3", "u4"), ("u2", "u5"), ("u6", "u6")]:
    union(a, b)
groups = {}
for x in list(parent):
    groups.setdefault(find(x), []).append(x)
print(sorted(groups.values()))   # [['u1', 'u2', 'u5'], ['u3', 'u4'], ['u6']]
```

Distributed, the simplest approach is **label propagation of the minimum id**: every vertex adopts the smallest id among itself and its neighbours, until nothing changes. It needs as many rounds as the graph's diameter, which is too many on long chains. The **large-star / small-star** algorithm (Kiveris and others, 2014) rewires edges towards the smallest id in a neighbourhood and converges in a logarithmic number of rounds; GraphFrames' DataFrame implementation is based on this approach. Connected components over identity edges (shared email, phone, device) is the core of identity resolution and of fraud ring detection.

**Community detection with Louvain.** **Modularity** measures how many more edges fall inside communities than you would expect at random. Louvain maximises it greedily in two repeating phases: (1) move each node to the neighbouring community that increases modularity most, until no move helps; (2) collapse each community into a single node and repeat on the smaller graph. It is fast (near linear per pass) and gives a hierarchy of communities. **Leiden** (2019) adds a refinement step that guarantees communities are internally connected, which plain Louvain does not; prefer it when available. Both can miss small communities in huge graphs (the **resolution limit**), so tune the resolution parameter and inspect results.

**Shortest paths.** BFS for unweighted graphs, Dijkstra for non-negative weights, A* when a good distance estimate exists (road networks). At scale, single-source paths are a frontier expansion (the Pregel example in the previous topic); **delta-stepping** processes the frontier in distance buckets so that many vertices are relaxed in parallel. All-pairs shortest paths are infeasible on large graphs simply because the output has N² entries; use landmarks (precomputed distances to a few hub nodes) for estimates.

**Triangle counting** finds the number of three-node cliques, the input to the **clustering coefficient** (how often a node's neighbours know each other), a strong signal of real versus fake accounts. The trick that makes it tractable is to orient each edge from the lower-degree to the higher-degree endpoint and only look forward, so no triangle is counted twice and supernodes do not dominate the work (O(m^1.5) for m edges):

```python
edges = [(1, 2), (1, 3), (2, 3), (2, 4), (3, 4), (4, 5)]
adj = {}
for a, b in edges:
    adj.setdefault(a, set()).add(b); adj.setdefault(b, set()).add(a)
rank = {v: (len(adj[v]), v) for v in adj}          # order by degree, then id
fwd = {v: {u for u in adj[v] if rank[u] > rank[v]} for v in adj}
triangles = [(v, u, w) for v in adj for u in fwd[v] for w in fwd[v] & fwd[u]]
print(len(triangles), sorted(tuple(sorted(t)) for t in triangles))  # 2 [(1, 2, 3), (2, 3, 4)]
```

**Where to run them.** Neo4j Graph Data Science (GDS) projects a graph into an in-memory format and runs more than 65 algorithms with four modes (`stream`, `stats`, `mutate` to keep results in the projection, `write` to persist them):

```text
CALL gds.graph.project('txn', 'Account', {TRANSFER: {orientation: 'UNDIRECTED'}});
CALL gds.wcc.write('txn', {writeProperty: 'component'});
CALL gds.louvain.write('txn', {writeProperty: 'community'});
CALL gds.pageRank.stream('txn') YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS account, score ORDER BY score DESC LIMIT 10;
```

Each algorithm also has an `.estimate` mode that predicts memory before you run it. GraphFrames on Spark covers components, PageRank, label propagation, shortest paths and triangle counts for graphs that exceed one machine; SQL loops in BigQuery cover simple iterative algorithms without moving data.

> **Pitfall:** Running betweenness centrality on the full graph. Exact betweenness (Brandes' algorithm) is O(N × M): on a billion edges, years of compute. Use sampled betweenness (from a few thousand random sources), or a cheaper proxy such as PageRank or degree.

::: deeper
**Making a billion edges fit.** Raw string ids waste memory and make every join slow. Re-index vertices to dense 64-bit (or 32-bit, if fewer than about four billion) integers once, store edges as two integer columns sorted by source, and convert back at the end. A billion edges as two int64 columns is 16 GB before compression, which fits one large machine's memory; GDS and single-node engines often beat a cluster at that size.

**Supernodes again.** A vertex with ten million neighbours makes one task huge (skew) and dominates triangle counts and similarity joins. Options: exclude or cap them, process them separately, or use vertex-cut partitioning.

**Convergence and determinism.** Iterative algorithms need a stopping rule (maximum iterations plus a tolerance on total change). Louvain and label propagation depend on processing order, so results change between runs; fix a seed and, for production features, keep community ids stable across runs by matching new communities to old ones by overlap.

**Incremental updates.** Recomputing PageRank daily on the full graph is often fine. When it is not, incremental methods update ranks around changed edges, and connected components can be maintained with union-find as edges arrive (deletions are the hard part).

> **Interview:** "Compute connected components on 50 billion identity edges." Re-index ids to integers, use a log-round algorithm (large-star / small-star) in Spark with checkpointing rather than label propagation, handle supernode identifiers (shared Wi-Fi, test phone numbers) by excluding them, and validate by sampling components for size outliers (one giant component usually means a bad linking key).
:::

::: check
?? In the PageRank example, why is D's rank exactly the random-jump share?
No page links to D, so it receives nothing from the sum; only (1 - 0.85) / 4 = 0.0375 remains.

?? Why is label propagation a poor choice for connected components on a graph with long chains?
It needs as many rounds as the graph's diameter, because a label moves one hop per round. Log-round algorithms such as large-star / small-star finish far sooner.

?? What does Leiden fix compared with Louvain?
Louvain can produce communities that are internally disconnected; Leiden adds a refinement step that guarantees connected communities.
:::

::: resources
- Docs: [Neo4j Graph Data Science library](https://neo4j.com/docs/graph-data-science/current/) · projections, execution modes, memory estimation and the algorithm catalogue
- Book: [Graph Algorithms](https://www.oreilly.com/library/view/graph-algorithms/9781492047674/) · Needham and Hodler; practical examples in Spark and Neo4j
- Paper: [Fast unfolding of communities in large networks](https://arxiv.org/abs/0803.0476) · Blondel and others; the Louvain method
- Paper: [From Louvain to Leiden: guaranteeing well-connected communities](https://arxiv.org/abs/1810.08473) · Traag, Waltman and van Eck
- Docs: [NetworkX algorithms](https://networkx.org/documentation/stable/reference/algorithms/index.html) · readable reference implementations for prototyping on small graphs
- Paper: [The PageRank Citation Ranking: Bringing Order to the Web](http://ilpubs.stanford.edu:8090/422/) · the original PageRank report
:::

## Applications

The first group gave you the machinery: graph models and query languages, native and distributed stores, the vertex-centric model and the algorithms. This group is about the pipelines that feed it, which is where most of a data engineer's graph work actually happens. A graph is only as good as its nodes and edges, and in every application the hard part is producing them correctly, keeping them fresh, and turning graph structure back into features or answers.

Four applications recur across industries. Knowledge graphs turn scattered records and text into connected entities. Fraud detection combines streaming features with graph structure under a tight latency budget. Social networks are the largest graphs anyone runs, with their own storage and partitioning designs. Geospatial data is a graph of places and roads with its own indexing and file formats.

### Knowledge Graph Construction Pipelines

::: plain
Building a knowledge graph is like compiling a who's who from a pile of newspapers, company registers and customer files. You underline every name (extraction), work out which names mean the same person or company (resolution), attach each to an official record (linking), write down how they relate in a fixed vocabulary (the ontology), and check every entry against the rules before it goes into the book. The [Data engineering for AI and ML](#aiml) section explained what knowledge graphs are for; this topic is about running the factory that builds one.
:::

[[fig:graph_kgpipe]]

**The pipeline, stage by stage.** Treat construction like any other batch or streaming pipeline: staged tables, idempotent steps, versions and quality checks. A practical layout on Google Cloud keeps every stage in BigQuery tables and loads the final graph into Spanner Graph, BigQuery Graph or Neo4j.

| Stage | Input | Output table | Main tools |
|---|---|---|---|
| Ingest | documents, tables, APIs | `documents`, `records` | Cloud Storage, BigQuery, Datastream |
| Extract entities | text | `mentions(doc_id, start, end, surface, type, extractor, confidence)` | spaCy, fine-tuned NER models, Gemini with structured output |
| Extract relations | text plus mentions | `raw_facts(subj_mention, predicate, obj_mention, evidence, confidence)` | relation classifiers, LLM prompts with a fixed relation list |
| Resolve | mentions and records | `entity_map(mention_id, entity_id)` | blocking and matching (Splink, custom), clustering |
| Link | entities | `entity_xref(entity_id, scheme, external_id)` | alias tables, Wikidata, LEI, internal master data |
| Validate | facts | `facts`, `quarantine` | ontology rules, SHACL, SQL tests |
| Load and embed | facts | graph store, `entity_embeddings` | graph bulk loaders, PyKEEN, GDS embeddings |

**Ontology design.** The ontology is the schema of the graph: classes (`Company`, `Person`, `Product`), relations (`subsidiaryOf`, `employs`, `supplies`), and rules about them. Design it from **competency questions**, the questions the graph must answer ("which suppliers of our top products are owned by a sanctioned company?"), and reuse existing vocabularies (schema.org, or FIBO in finance) rather than inventing names. In OWL you can state that a relation is transitive, that two relations are inverses, or that a class is a subclass of another; a reasoner then infers new facts. In Turtle syntax:

```text
@prefix ex:   <https://kg.example.com/ontology#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Company    a owl:Class .
ex:Supplier   a owl:Class ; rdfs:subClassOf ex:Company .
ex:subsidiaryOf a owl:ObjectProperty, owl:TransitiveProperty ;
    rdfs:domain ex:Company ; rdfs:range ex:Company .
ex:parentOf   a owl:ObjectProperty ; owl:inverseOf ex:subsidiaryOf .
```

OWL reasons under the **open-world assumption**: a missing fact is unknown, not false, so OWL will not complain that a company has no legal name. To *validate* data you need closed-world checks, which is what **SHACL** shapes (or plain SQL tests on the facts table) provide. Most teams use a light ontology for meaning and SHACL or SQL for quality. Property graph stores have no reasoner, so you either materialise inferred facts in the pipeline (compute the transitive `subsidiaryOf` closure) or express them in queries.

**Extraction from text.** Named entity recognition (NER) finds spans and types; relation extraction decides how two mentions relate. LLMs now do both in one call, and the engineering rule is to constrain them: give a fixed list of entity types and relations, require JSON that matches a schema (structured output with Gemini on Gemini Enterprise Agent Platform, formerly Vertex AI), keep the evidence sentence, and run extraction as a batch job over new or changed documents only.

```json
{
  "entities": [{"id": "m1", "surface": "Acme GmbH", "type": "Company"},
               {"id": "m2", "surface": "Berlin", "type": "City"}],
  "relations": [{"subject": "m1", "predicate": "headquarteredIn", "object": "m2",
                 "evidence": "Acme GmbH, based in Berlin, said on Monday...", "confidence": 0.93}]
}
```

**Entity resolution.** Comparing every mention with every other is quadratic, so resolution runs in three steps: **blocking** groups mentions that could match by a cheap key (same city, same first three letters of a normalised name, same postcode), **matching** scores pairs inside each block, and **clustering** turns matched pairs into entities (connected components, or a stricter method when transitivity is risky).

```python
import re
from collections import defaultdict
from itertools import combinations

mentions = [("crm", 1, "ACME GmbH", "Berlin"), ("erp", 7, "Acme Gmbh.", "Berlin"),
            ("news", 3, "Acme", "Berlin"), ("erp", 9, "Apex Logistics", "Hamburg"),
            ("crm", 4, "APEX Logistics Ltd", "Hamburg")]
STOP = {"gmbh", "ltd", "inc", "ag"}
def tokens(name):
    return {t for t in re.findall(r"[a-z0-9]+", name.lower()) if t not in STOP}
def jaccard(a, b):
    return len(a & b) / len(a | b)

blocks = defaultdict(list)                       # blocking: only compare within the same city
for m in mentions:
    blocks[m[3]].append(m)
pairs = []
for city, ms in blocks.items():
    for a, b in combinations(ms, 2):
        s = jaccard(tokens(a[2]), tokens(b[2]))
        if s >= 0.8:
            pairs.append(((a[0], a[1]), (b[0], b[1]), round(s, 2)))
print(pairs)
# [(('crm', 1), ('erp', 7), 1.0), (('crm', 1), ('news', 3), 1.0),
#  (('erp', 7), ('news', 3), 1.0), (('erp', 9), ('crm', 4), 1.0)]
```

Four comparisons instead of ten, and two entities come out. But notice the risk: after removing legal suffixes, the bare news mention "Acme" scores a perfect 1.0, and in a city with two companies called Acme that would merge them. Real matchers combine several fields (name, address, registration numbers, websites) with learned weights, as Splink does with the Fellegi-Sunter model, and send uncertain pairs to human review.

**Entity linking** attaches resolved entities to identifiers outside the pipeline: your master data ids, Wikidata items, LEI codes for legal entities. Linking generates candidates from an alias table (every known name of every entity) and disambiguates by context (the other entities in the same document, the type, the country).

**Stable ids and provenance.** The output must survive re-runs. Give every entity a persistent id that does not change when clusters are recomputed; when two entities merge, keep the old id as a redirect (`same_as`) so downstream references still resolve. Store every fact with its source, extraction run, extractor version and confidence, so that a bad extractor version can be rolled back by deleting its facts and a disputed fact can be traced to the sentence that produced it.

**Knowledge graph embeddings** learn a vector per entity and relation so that true facts score well, which enables link prediction (suggesting missing facts) and similarity search. TransE models a relation as a translation, head + relation ≈ tail:

```python
# TransE: a fact (head, relation, tail) is plausible when head + relation is close to tail
emb = {"Acme": [0.9, 0.1], "Berlin": [1.0, 0.9], "Hamburg": [0.1, 1.0], "located_in": [0.1, 0.8]}
def score(h, r, t):                       # smaller distance = more plausible
    return round(sum((a + b - c) ** 2 for a, b, c in zip(emb[h], emb[r], emb[t])) ** 0.5, 2)
print(score("Acme", "located_in", "Berlin"))    # 0.0
print(score("Acme", "located_in", "Hamburg"))   # 0.91
```

Training adjusts the vectors so true triples get small distances and corrupted ones (a random tail swapped in) get large ones. TransE cannot model symmetric or one-to-many relations well; RotatE and ComplEx handle more relation patterns. Libraries such as PyKEEN train and evaluate them; the standard metrics are mean reciprocal rank (MRR) and Hits@10 in the *filtered* setting (other true facts are not counted as errors).

> **Pitfall:** Letting the LLM invent the schema. Free-form extraction produces `worksFor`, `employedBy`, `isEmployeeOf` and `works_at` for the same relation, and the graph fragments. Fix the relation list in the prompt and the JSON schema, and map anything else to quarantine.

::: deeper
**Incremental construction.** Re-extracting the whole corpus each night is expensive with LLMs. Hash each document, extract only new or changed ones, and recompute resolution only for blocks touched by new mentions. Because a new mention can connect two existing clusters, incremental resolution must handle merges (and, after corrections, splits) while keeping ids stable.

**Quality measurement.** Sample facts per source and extractor version for human labelling, and track precision by relation type. Track entity-level metrics too: duplicate rate (pairs judged the same entity but with different ids) and over-merge rate. Over-merging is usually worse than duplication, because a wrong merge silently connects unrelated facts.

**Temporal facts.** Many facts are true for a period (a CEO, a parent company). Model validity intervals on facts (or on edges in a property graph) rather than overwriting, and record both when the fact held and when you learned it.

**Graph-based retrieval.** For GraphRAG-style systems, the construction pipeline also writes text summaries per entity and per community, and embeddings of those summaries, so a retriever can enter the graph by vector search and expand along edges. [Data engineering for AI and ML](#aiml) covers that serving side.

> **Interview:** "Build a company knowledge graph from news and internal CRM data." Define competency questions and a small ontology; extract mentions with constrained LLM output and keep evidence; block and match against CRM records with several fields; cluster with persistent ids; link to LEI and Wikidata; validate with SHACL or SQL; load to the graph with provenance and confidence; measure precision by sampling; process incrementally.
:::

::: check
?? Why does entity resolution use blocking?
Comparing every pair of mentions is quadratic. Blocking groups mentions by a cheap key so only plausible pairs inside each block are compared.

?? OWL says nothing is wrong with a company that has no legal name. Why, and what do you use instead?
OWL uses the open-world assumption: a missing fact is unknown, not false. Validation needs closed-world checks such as SHACL shapes or SQL tests.

?? How do you keep entity ids stable when two clusters merge?
Keep one id as the survivor and record the other as a redirect (`same_as`), so existing references still resolve.
:::

::: resources
- Paper: [Knowledge Graphs](https://arxiv.org/abs/2003.02320) · Hogan and others; a comprehensive survey of models, construction, quality and embeddings
- Spec: [OWL 2 Web Ontology Language Primer](https://www.w3.org/TR/owl2-primer/) · classes, properties and reasoning from the W3C
- Book: [Knowledge Graphs: Data in Context for Responsive Businesses](https://www.oreilly.com/library/view/knowledge-graphs/9781098128777/) · Barrasa, Hodler and Webber; building and using knowledge graphs in organisations
- Docs: [Splink](https://moj-analytical-services.github.io/splink/) · probabilistic record linkage at scale on Spark, DuckDB and other backends
- Docs: [PyKEEN](https://pykeen.readthedocs.io/) · training and evaluating knowledge graph embedding models
- Blog: [Building the LinkedIn knowledge graph](https://engineering.linkedin.com/blog/2016/10/building-the-linkedin-knowledge-graph) · entity taxonomies, extraction, resolution and inference at LinkedIn
:::

### Fraud Detection Data Engineering

::: plain
Catching fraud is like being a shop detective with half a second to decide. You look at the customer's recent behaviour (three cards tried in two minutes?), at what they carry (a phone seen on twenty other accounts?), and at who they are connected to (friends with known fraudsters?). Then, weeks later, you find out whether you were right, when the bank reports which purchases were disputed. A fraud data platform supplies those three kinds of evidence fast enough, and records the outcomes carefully enough to learn from them.
:::

**Shape of the problem.** A card payment, sign-up or payout must be scored inside a request, typically with a total budget of tens to a few hundred milliseconds, of which feature lookup gets a small slice. Fraud is rare, adversarial (fraudsters adapt within days), and its labels arrive late. That leads to four pipelines that must agree with each other: real-time features, graph features, identity (device) resolution and labels.

[[fig:graph_fraud]]

**1. Real-time feature pipeline.** Events (payment attempts, logins, profile changes) flow through Pub/Sub or Google Managed Service for Apache Kafka into a stateful stream processor (Dataflow or Flink), keyed by the entity the feature describes (card, account, device, IP, email). The processor keeps windowed state and upserts the latest feature values into a low-latency store (Bigtable, or Memorystore), and also appends them to a log that lands in BigQuery as the offline history used for training. The streaming mechanics, state, TTLs and dual writes are covered in [Data engineering for AI and ML](#aiml) (topic Real-Time Feature Engineering with Flink); here the focus is which features and how they stay correct.

**Velocity features** count or sum events per entity over recent windows: payments per card in the last 10 minutes, distinct cards per device in the last hour, amount per account in the last day, failed logins per IP. Computed at each event, they must use only events strictly before it, in event time:

```python
from bisect import bisect_left
from collections import defaultdict

events = [("card2", 0), ("card2", 120), ("card2", 300), ("card2", 590), ("card2", 700), ("card9", 650)]
times = defaultdict(list)
for card, ts in sorted(events, key=lambda e: e[1]):
    window = times[card]
    count_before = len(window) - bisect_left(window, ts - 600)   # events in [ts - 600, ts)
    print(card, ts, count_before)
    window.append(ts)
# card2 0 0
# card2 120 1
# card2 300 2
# card2 590 3
# card9 650 0
# card2 700 3
```

At 700 seconds the event at 0 has left the 600-second window, so the count stays at 3. Useful variations: distinct counts (cards per device; use HyperLogLog sketches when cardinalities are large), ratios to the entity's own history ("amount compared with this card's 90-day median"), and time since the last event of a kind (minutes since password change).

**2. Device fingerprinting pipeline.** Fraudsters create new accounts freely; their devices are harder to change. A client SDK collects signals (app-generated device id, operating system and version, screen, time zone, language, user agent, IP address and network), the backend hashes and normalises them, and a resolution step assigns a **device id** that stays stable when weak signals change (a new IP, a browser update) but splits when strong ones differ. It is entity resolution again (previous topic), applied to devices, with an IP enrichment step (network owner, known proxy, VPN and hosting ranges). Fingerprinting is personal data processing: collect only what the stated fraud-prevention purpose needs, document it, and respect consent and retention rules ([Data governance, security and compliance](#governance)).

**3. Graph features and ring detection.** Link accounts through shared identifiers (device, card fingerprint, phone, email, address, payout bank account). Fraud rings show up as clusters that are unusually large or dense:

```python
from collections import defaultdict

# (account, device, card_fingerprint) seen at sign-up or payment
seen = [("acc1", "devA", "card1"), ("acc2", "devA", "card2"), ("acc3", "devB", "card2"),
        ("acc4", "devC", "card3"), ("acc5", "devB", "card4"), ("acc6", "devD", "card5")]
parent = {}
def find(x):
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]; x = parent[x]
    return x
for acc, dev, card in seen:              # shared identifiers join accounts into one cluster
    parent[find(dev)] = find(acc); parent[find(card)] = find(acc)
clusters = defaultdict(set)
for acc, _, _ in seen:
    clusters[find(acc)].add(acc)
print(sorted(sorted(c) for c in clusters.values() if len(c) > 2))
# [['acc1', 'acc2', 'acc3', 'acc5']]
```

acc1 and acc2 share a device, acc2 and acc3 share a card, acc3 and acc5 share a device: four accounts that look unrelated one by one form one ring. In production the batch version runs as connected components (GraphFrames, or SQL in BigQuery) over all identity edges every few hours, writes `cluster_id`, cluster size, number of confirmed fraud accounts in the cluster and community features to the online store, and a streaming job applies union-find to new edges between batch runs. Real-time multi-hop lookups ("is this card within two hops of a confirmed fraudster?") go to a graph store: Spanner Graph for an operational graph inside Google Cloud, or Neo4j or TigerGraph.

> **Pitfall:** Linking on identifiers that many innocent users share. A mobile carrier's NAT IP address, a corporate office Wi-Fi, a default test phone number or a popular shipping forwarder's address joins thousands of unrelated accounts into one giant "ring". Measure the degree of every identifier value and exclude or down-weight the high-degree ones before computing components.

**4. Labels.** The truth arrives late and biased:

- **Delay.** Chargebacks and disputes arrive days to months after the payment. A transaction from last week with no chargeback is not yet "legitimate"; it is *immature*. Train only on transactions older than a label maturity window chosen from your own chargeback-delay distribution, and report recent performance with that caveat.
- **Several sources.** Chargebacks, manual review decisions, customer reports, rules that fired, and linked-account propagation ("same ring as a confirmed fraudster"). Keep the source and time of each label; weak labels are useful but should not be mixed blindly with confirmed ones.
- **Selection bias.** Blocked transactions never get an outcome, so the model never learns whether they were fraud. A common remedy is to let a tiny random share of would-be-blocked low-value transactions through (or send them to review) to obtain unbiased labels, and to weight them in evaluation.
- **Point in time.** Training features must be the values as they were at decision time, which is why the online store's writes are logged rather than recomputed later.

**Evaluation that matches operations.** Accuracy is meaningless when 99.9% of transactions are fine. Measure precision and recall at the threshold you will run, the review queue size it produces (analysts can only review so many cases per day), and money: fraud losses prevented versus good revenue declined. Rules (fast, explainable, written by analysts, as in Uber's Mastermind rules engine) and models (Stripe Radar style) usually run together, and the data platform must log every rule and model decision with its inputs.

On Google Cloud, **Anti Money Laundering AI** offers a managed model for transaction monitoring in banking, consuming a prescribed BigQuery data model; card and account fraud is typically built with the pipeline above and models on Gemini Enterprise Agent Platform.

::: deeper
**Training-serving consistency.** The highest-risk bug in fraud systems is a feature computed differently online and offline (different window boundaries, time zones, or late-event handling). Define each feature once and run the same code in streaming and in backfill, or log served features and train on the log.

**Feature freshness versus completeness.** Streaming windows close on watermarks; events that arrive later than the allowed lateness are dropped from the live value but present in the offline recomputation. Either allow enough lateness, or train on logged (as-served) features so the model learns the values it will actually see.

**Adversarial drift.** Fraud patterns shift when a defence works. Monitor feature distributions and score distributions daily, keep a fast path for analysts to add rules, and retrain often. Graph features help because changing devices, cards and payout accounts all at once is costly for fraudsters.

**Graph freshness.** A ring discovered in the nightly batch is a day late. Combine nightly components with streaming union-find for new links, and recompute properly (to handle deletions and corrections) in the next batch.

> **Interview:** "Design the data side of card fraud scoring." Streaming velocity features keyed by card, device and account into Bigtable, logged to BigQuery; nightly identity graph components plus streaming union-find; device resolution with an excluded list of high-degree identifiers; a scoring service with a strict timeout and a fallback; every decision logged with features; labels from chargebacks with a maturity window, review outcomes and a small exploration sample; evaluation by losses and review capacity.
:::

::: check
?? Why must velocity features at training time use only events strictly before each transaction?
At serving time the model only sees the past. Including the event itself or later events leaks the future, so offline performance looks better than it will be in production.

?? Why is a two-day-old transaction without a chargeback not a good negative label?
Chargebacks arrive weeks or months later, so the label is immature. Use a maturity window based on your chargeback delay distribution.

?? A connected-components job puts 40% of all accounts in one cluster. What is the likely cause?
A shared identifier with very high degree, such as a carrier NAT IP, office Wi-Fi or a test phone number. Measure identifier degrees and exclude such values before linking.
:::

::: resources
- Blog: [How we built it: Stripe Radar](https://stripe.com/blog/how-we-built-it-stripe-radar) · the model architecture and decision explanations of a large payments fraud system
- Blog: [Mastermind: using Uber engineering to combat fraud in real time](https://www.uber.com/blog/mastermind/) · a real-time rules engine and its data flow
- Docs: [Anti Money Laundering AI](https://cloud.google.com/anti-money-laundering-ai/docs) · Google Cloud's managed AML monitoring and its BigQuery data model
- Book: *Fraud Analytics Using Descriptive, Predictive, and Social Network Techniques* by Bart Baesens, Véronique Van Vlasselaer and Wouter Verbeke · fraud features, network analytics and evaluation
- Docs: [Neo4j Graph Data Science: weakly connected components](https://neo4j.com/docs/graph-data-science/current/algorithms/wcc/) · the components algorithm used for identity clusters
- Article: [A primer on machine learning for fraud detection](https://stripe.com/guides/primer-on-machine-learning-for-fraud-protection) · labels, features, precision and recall, and operating fraud models
:::

### Social Network Data Engineering

::: plain
A social network is a giant address book in which every page points to other pages: whom you follow, whom you message, what you liked. Two jobs run on it all day. One is reading it extremely fast (show this person's feed, check whether these two are friends, count the likes) for billions of requests. The other is studying it in bulk to suggest things (people you may know, posts you may enjoy). Social network data engineering builds the storage for the first and the pipelines for the second, on a graph so big and lopsided that ordinary partitioning stops working.
:::

**The data model.** Users, content (posts, photos, groups, pages) and the edges between them: follows or friendships, authorship, likes, comments, shares, views, blocks. Directed edges (follow) differ from symmetric ones (friendship, stored as two directed edges). Most edges carry a time and are queried newest first. Interaction events (impressions, clicks, dwell time) are far larger than the social graph itself and live in the lakehouse or warehouse, not the graph store. The modelling choices are covered in [Data modeling](#modeling); this topic is about serving and processing.

**Serving the graph: Facebook's TAO.** TAO (2013) is the canonical design for a read-heavy social graph:

- Two concepts only: **objects** (typed nodes with an id and key-value data) and **associations** (typed, directed edges `(id1, atype, id2, time, data)`).
- An association list (all edges of one type from one object) is kept ordered by time, and the API is built around it: `assoc_get`, `assoc_range` (the newest *n* edges), `assoc_count`, `assoc_time_range`.
- Storage is sharded MySQL; an object and its outgoing associations live on the shard chosen by the object id, so a range query hits one shard. The inverse edge (B "followed by" A) is a separate association on B's shard, written in a second step, so the two can briefly disagree.
- In front of storage sits a two-tier, geographically replicated cache (followers and leaders) that answers the overwhelming majority of requests, which are reads.

LinkedIn followed a related path with LIquid, a graph database designed for its economic graph with an in-memory, relational-style query model. The lesson is the same: serve the graph from a store optimised for "edges of type T from node X, newest first, and their count", and push everything else (analytics, recommendations) to offline pipelines.

**Feeds: fan-out on write or on read.** Building a home feed means merging recent posts from everyone you follow.

| Approach | How it works | Cost driver | Problem case |
|---|---|---|---|
| Fan-out on write (push) | when someone posts, append the post id to every follower's inbox | followers per post | a celebrity with 50 million followers triggers 50 million writes per post |
| Fan-out on read (pull) | when you open the feed, fetch recent posts from everyone you follow | follows per reader | users who follow thousands of accounts make every read expensive |
| Hybrid | push for ordinary authors, pull for high-follower authors at read time | both, bounded | the usual production answer |

Ranking then runs as a funnel: **candidate generation** collects a few thousand items (inbox posts, posts liked by your network, embedding neighbours, trending items), a light model trims them to hundreds, a heavy model scores them with rich features, and a final stage applies filters, diversity rules and integrity checks. The data pipelines behind it are the same as any recommendation system: impression and engagement logging, features logged at serving time, training sets joined by request id, and daily or continuous retraining. X's open-sourced recommendation code shows such a funnel end to end.

**Connection recommendations (people you may know).** Most new connections close a triangle, so candidates come from friends of friends, scored by how strongly they are connected. **Adamic-Adar** counts mutual friends but weights each by 1 / log(degree), because a mutual friend with few friends is stronger evidence than one who knows everybody:

```python
import math
from collections import defaultdict
friends = {"ana": {"bo", "cy", "dev"}, "bo": {"ana", "cy", "eli"}, "cy": {"ana", "bo", "eli", "fay"},
           "dev": {"ana", "fay"}, "eli": {"bo", "cy"}, "fay": {"cy", "dev", "gus", "hal", "ivy"},
           "gus": {"fay"}, "hal": {"fay"}, "ivy": {"fay"}}
def recommend(user, k=3):
    score = defaultdict(float)
    for f in friends[user]:                       # 1 hop
        for fof in friends[f]:                    # 2 hops
            if fof != user and fof not in friends[user]:
                score[fof] += 1 / math.log(len(friends[f]))   # Adamic-Adar: rare mutual friends count more
    return sorted(((round(s, 2), c) for c, s in score.items()), reverse=True)[:k]
print(recommend("ana"))   # [(2.16, 'fay'), (1.63, 'eli')]
```

fay and eli both have two mutual friends with ana, but fay wins because one of them, dev, has only two friends (1 / ln 2 ≈ 1.44) while eli's mutual friends have three and four. In production this runs as a batch job (Spark on Managed Service for Apache Spark, or SQL self-joins in BigQuery) that generates candidates for every user daily, adds features (mutual count, Adamic-Adar, shared employer or school, embedding similarity), scores them with a model, and writes the top few hundred per user to a key-value store (Bigtable) for the app to read. The two-hop expansion is the expensive part: a user with 5,000 friends, each with 5,000 friends, has 25 million paths, so cap the number of friends expanded per user and sample supernodes.

**Graph embeddings for recommendation.** Instead of hand-made graph features, learn a vector per user and item:

- **node2vec / DeepWalk**: generate random walks over the graph and train a word2vec-style model on them, so nodes that appear in similar walks get similar vectors.
- **GraphSAGE and PinSage**: graph neural networks that build a node's vector by aggregating sampled neighbours' features; Pinterest's PinSage scaled this to billions of pins using random-walk-based neighbour sampling and MapReduce-style inference.
- **Two-tower models** trained on engagement, with graph features as inputs.

The vectors are served from an approximate nearest neighbour index for candidate generation ([Data engineering for AI and ML](#aiml) covers vector indexes), and must be versioned together: user and item vectors from different training runs live in different spaces and must not be mixed.

**Partitioning the social graph.** Hash partitioning by user id balances load but scatters friends across shards, so a "friends of friends" request touches almost every shard. **Locality-aware partitioning** places users who interact often on the same shard. Facebook's Social Hash Partitioner (balanced graph partitioning by iterative label moves) reduced cross-shard fan-out substantially for queries that touch a user's friends. The same idea applies offline: co-partition edge tables by source id so that joins for two-hop features do not shuffle the whole graph twice.

> **Pitfall:** Ignoring privacy edges. Blocks, mutes, private accounts and deleted relationships must be applied in every pipeline: a recommendation of a blocked person or a feed item from a private account is a serious incident. Keep these filters in one shared dataset applied at candidate generation and again at serving.

::: deeper
**Consistency choices.** TAO accepts eventual consistency between an edge and its inverse and between cache tiers, because a like count off by one for a second harms nobody. Operations that must be correct (blocking, privacy settings) are routed to the leader tier or the database. Decide per edge type which guarantees you need.

**Counting at scale.** "Number of followers" and "number of likes" are read far more than they change. Store counts as separate, cached objects updated with each edge write rather than counting edges on read, and accept approximate counts for very hot objects.

**Offline graph snapshots.** Recommendation pipelines read a daily snapshot of the graph (edge tables partitioned by date in BigQuery or Iceberg) rather than the live store. Point-in-time correctness matters here too: training examples for "did the user accept this suggestion?" must use the graph as it was when the suggestion was shown.

**Supernode handling.** Celebrities, bots and spam accounts distort two-hop features and embeddings. Cap degrees in random walks, down-weight high-degree neighbours (as Adamic-Adar does) and filter known bots before training.

> **Interview:** "Design Twitter's home timeline." Hybrid fan-out: push post ids into Redis-like timeline caches for authors under a follower threshold, pull celebrity posts at read time; rank with a candidate-generation and ranking funnel; log impressions and features for training; store the follow graph in a sharded, cached association store; handle deletes and blocks at read time.
:::

::: check
?? Why does a hybrid feed design pull posts from celebrities at read time?
Pushing a celebrity's post would mean tens of millions of inbox writes per post. Pulling their few posts at read time is cheap, while ordinary authors keep the fast push path.

?? In Adamic-Adar, why is a mutual friend with few friends weighted more?
Knowing the same person is less informative when that person knows everyone. Weighting by 1 / log(degree) makes rare shared connections count more.

?? What problem does locality-aware partitioning solve?
With hash partitioning, a user's friends are spread over almost every shard, so neighbourhood queries fan out everywhere. Placing frequently interacting users together reduces cross-shard requests.
:::

::: resources
- Paper: [TAO: Facebook's Distributed Data Store for the Social Graph](https://www.usenix.org/conference/atc13/technical-sessions/presentation/bronson) · USENIX ATC 2013; objects, associations, caching and consistency
- Blog: [TAO: The power of the graph](https://engineering.fb.com/2013/06/25/core-infra/tao-the-power-of-the-graph/) · Facebook's overview of TAO
- Paper: [node2vec: Scalable Feature Learning for Networks](https://arxiv.org/abs/1607.00653) · biased random walks for node embeddings
- Paper: [Graph Convolutional Neural Networks for Web-Scale Recommender Systems](https://arxiv.org/abs/1806.01973) · PinSage at Pinterest
- Paper: [Social Hash: an Assignment Framework for Optimizing Distributed Systems Operations on Social Networks](https://www.usenix.org/conference/nsdi16/technical-sessions/presentation/shalita) · NSDI 2016; locality-aware partitioning at Facebook
- Repo: [twitter/the-algorithm](https://github.com/twitter/the-algorithm) · the open-sourced recommendation system behind X's timeline
:::

### Geospatial Data Engineering

::: plain
Geospatial data is everything with a place on Earth: a phone's GPS ping, a delivery zone, a road, a country border. Working with it at scale means three skills. First, a way to file locations so nearby things sit together, like the grid squares on a paper map (H3 hexagons, S2 cells). Second, joins that ask "which zone contains this point?" for billions of points without testing every pair. Third, file formats and coordinate systems that keep the shapes correct as they move between tools. The [Industry patterns](#industry) section shows a ride-sharing use (distance, surge cells); this topic is the engineering underneath.
:::

**Coordinates first.** A location is a pair of numbers in a **coordinate reference system** (CRS). GPS and most data use **WGS 84** longitude and latitude in degrees (EPSG:4326). Web maps draw in **Web Mercator** (EPSG:3857), which distorts area badly away from the equator. Distances and areas in metres need either spherical (geodesic) calculations or a local projected CRS (a UTM zone, a national grid). Every geometry column must carry its CRS, or someone will eventually compute "distance" in degrees.

**Geometry types.** Points, line strings (roads, GPS traces), polygons (zones, buildings) and their multi- versions, written as **WKT** text (`POINT(73.8567 18.5204)`, longitude first) or compact **WKB** binary.

**File formats.**

| Format | What it is | Strengths | Limits |
|---|---|---|---|
| GeoJSON (RFC 7946) | JSON features with geometry and properties, WGS 84 only | universal, readable, web friendly | large, slow to parse, no spatial index |
| Shapefile | a set of files (`.shp`, `.shx`, `.dbf`, `.prj`) | supported by every GIS tool | 2 GB per file, 10-character field names, many files |
| GeoPackage | a SQLite database with geometry tables | one file, indexed, many layers | row-oriented, not for analytics engines |
| GeoParquet | Parquet with WKB geometry columns plus metadata (CRS, bounding boxes) | columnar, compressed, read by Spark, BigQuery, DuckDB, Sedona | needs readers that understand the metadata |

GeoParquet 1.1 adds an optional per-row bounding-box column so engines can skip row groups by area. The Parquet format itself now defines native `GEOMETRY` and `GEOGRAPHY` logical types with statistics, and Apache Iceberg format v3 adds geometry and geography column types, so spatial data in a lakehouse gets the same file skipping as any other column. For lake and warehouse pipelines, convert inputs to GeoParquet (or native types) at the bronze-to-silver step.

**Grid indexes: H3 and S2.** A grid index maps a point to a cell id, so "nearby" becomes "same or neighbouring cell", which is an equality join or a small `IN` list.

- **H3** (Uber) projects an icosahedron onto the sphere and tiles it with **hexagons** (plus 12 pentagons): 122 base cells and 16 resolutions (0 to 15), each cell having about seven children. Hexagons have six neighbours at equal distance, which makes rings (`grid_disk`) and smoothing natural. Cells are 64-bit integers; parent and child cells are cheap to compute, but children do not exactly tile the parent, so roll-ups by parent are approximate at the edges.
- **S2** (Google) projects the sphere onto a cube and divides each face into a quadtree of square cells, 31 levels (0 to 30), numbered along a **Hilbert curve**, so cells close in id are mostly close in space and a region becomes a few id ranges. Children exactly tile the parent. BigQuery uses S2 internally and exposes `S2_CELLIDFROMPOINT` and `S2_COVERINGCELLIDS`.
- **Geohash** interleaves latitude and longitude bits into a base-32 string; prefixes are rectangles. Simple and widely supported, but neighbouring places can have very different prefixes at cell boundaries.

```python
# pip install h3   (H3 v4 API)
import h3
cell = h3.latlng_to_cell(18.5204, 73.8567, 9)      # resolution 9: about 0.1 square km per cell
ring = h3.grid_disk(cell, 1)                       # the cell and its 6 neighbours
parent = h3.cell_to_parent(cell, 7)                # roll up to a coarser cell
```

**PostGIS** adds spatial types, functions and indexes to PostgreSQL, and is available on Cloud SQL for PostgreSQL and AlloyDB. It has two families of types: `geometry` (planar, any CRS, fast) and `geography` (spherical WGS 84, results in metres). A **GiST** index (an R-tree-like structure over bounding boxes) makes spatial predicates fast:

```sql
-- PostgreSQL with PostGIS
CREATE TABLE stores (id bigint PRIMARY KEY, name text, location geography(Point, 4326));
CREATE INDEX stores_location_gix ON stores USING GIST (location);

-- stores within 2 km of a point, nearest first
SELECT id, name, ST_Distance(location, ST_MakePoint(73.8567, 18.5204)::geography) AS metres
FROM stores
WHERE ST_DWithin(location, ST_MakePoint(73.8567, 18.5204)::geography, 2000)
ORDER BY metres
LIMIT 10;
```

Use `ST_DWithin` rather than `ST_Distance(...) < 2000` in the `WHERE` clause: only the former can use the index.

**Spatial joins: filter, then refine.** Joining a billion GPS pings to ten thousand delivery zones cannot test every pair. Every spatial join engine does two steps: a cheap **filter** using an index or a grid (bounding boxes, cells), then an exact **refine** test (point in polygon) only for the candidates. The idea in plain Python:

```python
import math
from collections import defaultdict

def inside(x, y, poly):                     # ray casting: count edge crossings to the right
    hit = False
    for (x1, y1), (x2, y2) in zip(poly, poly[1:] + poly[:1]):
        if (y1 > y) != (y2 > y) and x < x1 + (y - y1) * (x2 - x1) / (y2 - y1):
            hit = not hit
    return hit

zones = {"Z1": [(0, 0), (4, 0), (4, 4), (0, 4)], "Z2": [(4, 0), (9, 0), (6, 5)]}
points = {"p1": (1, 1), "p2": (5, 1), "p3": (3.9, 3.9), "p4": (8, 4), "p5": (6, 3)}
CELL = 2.0
def cells_of_bbox(poly):                    # every grid cell the polygon's bounding box touches
    xs, ys = [p[0] for p in poly], [p[1] for p in poly]
    return {(i, j) for i in range(math.floor(min(xs) / CELL), math.floor(max(xs) / CELL) + 1)
                   for j in range(math.floor(min(ys) / CELL), math.floor(max(ys) / CELL) + 1)}
index = defaultdict(list)                   # filter step: cell -> candidate zones
for z, poly in zones.items():
    for c in cells_of_bbox(poly):
        index[c].append(z)
tests, joined = 0, []
for p, (x, y) in points.items():
    for z in index[(math.floor(x / CELL), math.floor(y / CELL))]:
        tests += 1                          # refine step: exact geometry test
        if inside(x, y, zones[z]):
            joined.append((p, z))
print(joined)   # [('p1', 'Z1'), ('p2', 'Z2'), ('p3', 'Z1'), ('p5', 'Z2')]
print(tests, "exact tests instead of", len(points) * len(zones))   # 6 exact tests instead of 10
```

p4 sits inside Z2's bounding box but outside the triangle, so the filter keeps it as a candidate and the refine step rejects it. At scale the savings are many orders of magnitude.

**Apache Sedona** brings this to Spark (and Flink): spatial types, `ST_` SQL functions, GeoParquet and shapefile readers, and spatial join plans that partition both sides by space (KDB-tree or quadtree partitioning) or broadcast the small side with an index. On Google Cloud it runs on Managed Service for Apache Spark:

```python
from sedona.spark import SedonaContext

sedona = SedonaContext.create(SedonaContext.builder().getOrCreate())
sedona.read.format("geoparquet").load("gs://geo-lake/pings/").createOrReplaceTempView("pings")
sedona.read.format("geoparquet").load("gs://geo-lake/zones/").createOrReplaceTempView("zones")
per_zone = sedona.sql("""
  SELECT z.zone_id, COUNT(*) AS pings
  FROM zones z JOIN pings p ON ST_Contains(z.geometry, p.geometry)
  GROUP BY z.zone_id
""")
```

In BigQuery, `GEOGRAPHY` columns, `ST_` functions and spatial joins with `ST_INTERSECTS`, `ST_CONTAINS` or `ST_DWITHIN` are optimised automatically, and tables can be clustered on a geography column so that area filters prune blocks.

**Routing pipelines.** A road network is a weighted graph: intersections are nodes, road segments are directed edges with length, speed and turn restrictions. A typical pipeline downloads an **OpenStreetMap** extract (`.osm.pbf`) weekly, filters it to roads, applies speed profiles (from your own GPS telemetry after map matching, by time of week), and builds a routing engine such as **OSRM**, Valhalla or GraphHopper. These engines precompute shortcuts, for example **contraction hierarchies**, so that a continental shortest path query takes milliseconds instead of a Dijkstra search over millions of nodes; the price is a long preprocessing step that must rerun when weights change. Deploy the rebuilt graph blue-green and keep the previous version for rollback. A managed alternative is the Google Maps Platform Routes API, billed per request.

> **Pitfall:** Planar maths on longitude and latitude. Computing distance as the square root of degree differences, or buffering a point by "0.01" in EPSG:4326, gives wrong and latitude-dependent results (a degree of longitude is about 111 km at the equator and about 56 km at 60 degrees north). Use `geography` types, geodesic functions, or project to a local metric CRS first.

::: deeper
**Choosing a cell resolution.** Pick the resolution at which a cell is about the size of your question: surge pricing over a few city blocks, delivery zones over a neighbourhood. Store several resolutions as columns (cheap integers) so roll-ups do not need recomputation, and use rings or coverings (sets of cells covering a polygon) to turn polygon filters into `IN` lists of cell ids.

**Skew.** Spatial data is extremely uneven: one city block in a downtown area can hold more pings than a whole rural region. Spatial partitioners split dense areas finer, but fixed grids do not, so partition by a grid *and* time, or salt hot cells ([Data skew](pyspark-streaming-kafka-topic-map.html#skew) in the PySpark + Kafka guide).

**Polygon complexity.** A country border with a million vertices makes every point-in-polygon test slow. Subdivide large polygons into smaller pieces (PostGIS `ST_Subdivide`) before joins, and simplify geometries for display, never for analysis.

**Validity.** Real-world polygons self-intersect, have wrong winding order or unclosed rings. Validate at ingestion (`ST_IsValid`, `ST_MakeValid`) and quarantine the rest; invalid geometries make joins silently miss matches.

> **Interview:** "Count GPS pings per delivery zone every hour for a billion pings a day." Store pings as GeoParquet or BigQuery `GEOGRAPHY` with an H3 or S2 cell column; precompute each zone's covering cells; join on cell first (equality join) and refine with point-in-polygon only for cells on zone boundaries; partition by hour; handle skew in dense cells; validate zone polygons on load.
:::

::: check
?? Why are hexagons (H3) convenient for neighbourhood analysis?
Every hexagon has six neighbours at the same distance, so rings and smoothing over neighbours treat all directions equally, unlike squares with edge and corner neighbours.

?? What are the two steps of every spatial join, and why?
A cheap filter by index, grid or bounding box to find candidate pairs, then an exact geometry test on the candidates only. Testing every pair exactly would be far too slow.

?? Why is GeoParquet better than GeoJSON for a data lake?
It is columnar and compressed, carries CRS and bounding-box metadata for skipping, and is read directly by Spark, BigQuery, DuckDB and Sedona. GeoJSON is large, slow to parse and has no index.
:::

::: resources
- Docs: [H3 documentation](https://h3geo.org/docs/) · the hexagonal grid, resolutions, APIs and comparisons with other grids
- Docs: [S2 Geometry](http://s2geometry.io/) · cube projection, Hilbert curve cell ids and region coverings
- Docs: [PostGIS documentation](https://postgis.net/documentation/) · spatial types, functions and GiST indexing for PostgreSQL
- Docs: [Apache Sedona](https://sedona.apache.org/) · spatial SQL, spatial partitioning and joins on Spark and Flink
- Spec: [GeoParquet](https://geoparquet.org/) · the specification for geospatial data in Apache Parquet
- Docs: [Introduction to geospatial analytics in BigQuery](https://docs.cloud.google.com/bigquery/docs/geospatial-intro) · the `GEOGRAPHY` type, functions and visualisation
- Docs: [OSRM](https://project-osrm.org/) · open-source routing on OpenStreetMap with contraction hierarchies
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, query or rule of thumb |
|---|---|
| Query or analytics? | few start nodes and their neighbourhood: graph database; every edge, many times: batch engine |
| Find a pattern (Cypher or GQL) | `MATCH (a:Account)-[:TRANSFER]->{1,3}(b:Account) RETURN b` |
| Load idempotently into Neo4j | uniqueness constraint, then `UNWIND $rows AS row MERGE (n:Label {id: row.id})` |
| Graph over BigQuery tables | `CREATE PROPERTY GRAPH ds.g NODE TABLES (...) EDGE TABLES (...)`, then `GRAPH ds.g MATCH ...` |
| Graph inside SQL | `GRAPH_TABLE` (SQL/PGQ) on BigQuery Graph and Spanner Graph |
| Index-free adjacency | each hop costs O(degree), not O(log n); needs the store in the page cache |
| Neo4j cluster | primaries replicate with Raft (majority), secondaries scale reads; bookmarks for read-your-writes |
| GSQL results in parallel | accumulators: `@` per vertex, `@@` global |
| Vertex-centric model | supersteps, messages, vote to halt; combiners cut messages |
| Partition power-law graphs | vertex-cut (PowerGraph, GraphX), watch the replication factor |
| PySpark graphs | GraphFrames: `GraphFrame(v, e).connectedComponents()` with a checkpoint directory |
| PageRank | damping 0.85, power iteration, fix dangling nodes |
| Components at scale | union-find on one machine; large-star / small-star distributed; exclude supernode identifiers |
| Communities | Leiden over Louvain; fix a seed; keep ids stable across runs |
| Knowledge graph quality | competency questions, fixed relation list, blocking plus matching, persistent ids, provenance per fact |
| Fraud features | velocity windows strictly before the event, identity graph components, label maturity window |
| Friend suggestions | friends of friends scored by Adamic-Adar, capped fan-out, precomputed daily |
| Spatial join | filter by cell or bounding box, then refine with the exact test |
| Spatial formats | GeoParquet or native Parquet and Iceberg geo types in the lake; GeoJSON for the web |

### Practice

::: try
**1. Shared devices in Cypher.** In a graph with `(:Account)-[:USED]->(:Device)` and a boolean property `flagged` on accounts, write a Cypher query that returns every unflagged account that shares a device with a flagged account, with the number of shared devices, highest first.
:::

?? A solution to 1

```text
MATCH (f:Account {flagged: true})-[:USED]->(d:Device)<-[:USED]-(a:Account)
WHERE NOT a.flagged
RETURN a.id AS account, count(DISTINCT d) AS shared_devices
ORDER BY shared_devices DESC;
```

In production, exclude devices with a very high degree (emulators, shared kiosks) first, or the query fans out across a large part of the graph.

::: try
**2. Label propagation by hand.** For the undirected edges `(1,2), (2,3), (3,4), (5,6)`, implement minimum-id label propagation in Python: in each round, every vertex takes the smallest label among itself and its neighbours. Print the number of rounds that changed something and the final labels. Why does the number of rounds equal the longest shortest path in the component?
:::

?? A solution to 2

```python
edges = [(1, 2), (2, 3), (3, 4), (5, 6)]
nbrs = {}
for a, b in edges:
    nbrs.setdefault(a, set()).add(b); nbrs.setdefault(b, set()).add(a)
label = {v: v for v in nbrs}
rounds = 0
while True:
    new = {v: min([label[v]] + [label[u] for u in nbrs[v]]) for v in nbrs}
    if new == label:
        break
    label, rounds = new, rounds + 1
print(rounds, label)   # 3 {1: 1, 2: 1, 3: 1, 4: 1, 5: 5, 6: 5}
```

Label 1 moves one hop per round, and vertex 4 is three hops from vertex 1. That is why label propagation needs as many rounds as the diameter, and why log-round algorithms exist.

::: try
**3. Dangling nodes.** Change the PageRank example so that C links nowhere (`"C": []`). Without a fix, the total rank leaks away. Add the standard fix (spread the rank of dangling nodes evenly over all nodes each iteration) and check that the ranks still sum to 1.
:::

?? A solution to 3

```python
links = {"A": ["B", "C"], "B": ["C"], "C": [], "D": ["C"]}   # C is now dangling
nodes, d = sorted(links), 0.85
n = len(nodes)
rank = {v: 1 / n for v in nodes}
for _ in range(100):
    dangling = sum(rank[v] for v in nodes if not links[v])
    new = {v: (1 - d) / n + d * dangling / n for v in nodes}   # spread dangling rank evenly
    for src, outs in links.items():
        for dst in outs:
            new[dst] += d * rank[src] / len(outs)
    rank = new
print(round(sum(rank.values()), 6), {v: round(r, 3) for v, r in rank.items()})
# 1.0 {'A': 0.145, 'B': 0.206, 'C': 0.504, 'D': 0.145}
```

::: try
**4. Pick the system.** Choose a store or engine for each, and give one reason: (a) a payments company on Google Cloud wants two-hop "linked to a fraudster" checks inside its Spanner transactions; (b) analysts want ad hoc pattern queries over customer and order tables already in BigQuery; (c) a nightly job must compute connected components over 30 billion identity edges; (d) a product team wants an interactive graph application with rich Cypher queries and graph algorithms on a 200 million edge graph.
:::

?? Answer to 4
(a) Spanner Graph: the graph is a view over the same Spanner tables, inside the same transactions. (b) BigQuery Graph: GQL over existing tables without copying, combined with SQL through `GRAPH_TABLE`. (c) GraphFrames on Managed Service for Apache Spark (or its serverless mode) with re-indexed integer ids, checkpointing and supernode identifiers excluded: a whole-graph batch algorithm beyond one database. (d) Neo4j (AuraDB or self-managed) with Graph Data Science: the graph fits one machine's memory and the team wants Cypher and its ecosystem.

### What comes next

You can now store, query and process graphs, and build the pipelines that turn records, text, payments, social activity and locations into connected data. The next and final section, **Industry patterns**, applies the whole course to specific industries: financial services, healthcare, e-commerce, mobility, media, manufacturing, gaming and advertising, each with its own data, regulations and hard problems, many of which (AML rings, recommendations, geospatial cells) use what you learned here.
