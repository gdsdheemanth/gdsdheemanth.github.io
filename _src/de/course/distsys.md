This section is for data engineers who already build pipelines and now want to understand the machinery underneath them. Every tool in this course is a distributed system: BigQuery, Spanner, Kafka, Spark, Flink, Bigtable, Cloud Storage, even Airflow with two schedulers. When they behave strangely (a duplicate row, a stale read, a job that hangs after a network blip, a leader that flips back and forth), the explanation is almost always one of the ideas in this section. It assumes the earlier parts of the course, in particular **Databases: OLTP, NoSQL and specialised** (replication and partitioning), **Streaming systems** and **Event-driven architecture** (delivery guarantees, sagas), and **Operational databases, AI and governance on Google Cloud** (Spanner, Bigtable, Firestore). You need no maths beyond counting, and the Python examples run with only the standard library.

**What you will be able to do afterwards**

- Name the ways a network and a machine can fail, and explain why clocks cannot be trusted to order events.
- Use CAP and PACELC correctly: say what they mean, what they do not mean, and where Spanner, Bigtable, Cassandra and Kafka sit.
- Explain consistent hashing, vector clocks, CRDTs, Raft, leases and fencing tokens well enough to implement a toy version.
- Choose between ZooKeeper, etcd and a database for coordination, and know when Byzantine fault tolerance matters.
- Make pipelines resilient with timeouts, retries, backoff, circuit breakers, rate limits and backpressure.
- Move data efficiently with gRPC and Arrow Flight, trace a request across services, and run a pipeline against SLOs with an error budget.

**The big picture.** A distributed system is a group of computers that cooperate over a network and try to look like one computer. The trouble is that the network loses and delays messages, machines crash and come back, and no two clocks agree. So every distributed design answers the same three questions. **Who has the data?** (partitioning and replication: consistent hashing, quorums). **What order did things happen in, and who decides?** (clocks, consensus, leaders). **What happens when something fails?** (fault models, transactions, retries, circuit breakers, SLOs). Keep those three questions in mind and each topic below slots into place.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Foundations

This group gives you the vocabulary that the rest of the section uses. It starts with what can go wrong (networks, machines, clocks), then moves to the most quoted and most misused result in the field, CAP, and its more useful successor, PACELC. The last two topics are two concrete tools that appear inside almost every data system: consistent hashing, which decides which machine holds which key, and logical clocks, which order events without trusting wall-clock time.

If you remember one sentence from this group, make it this one: **in a distributed system you cannot tell a slow machine from a dead one.** Almost every design decision later in the section follows from that.

### Distributed Systems Fundamentals

::: plain
A distributed system is like a team spread across several cities that can only communicate by post. Letters can arrive late, arrive twice, or never arrive, and when a colleague goes quiet you cannot tell whether they are ill, busy, or their letters are stuck in the post. Everyone also has their own watch, and the watches drift apart. Distributed systems design is the craft of getting reliable work done anyway, without ever knowing for sure what the others are doing right now.
:::

**Why distribute at all?** Three reasons, and every data system has at least one: **scale** (the data or the traffic does not fit on one machine), **fault tolerance** (one machine will fail, so keep copies elsewhere) and **latency** (put data close to users in several regions). The price is that you now have **partial failure**: some parts work while others do not, which a single computer almost never does.

**The fallacies of distributed computing.** In the 1990s, engineers at Sun Microsystems (Peter Deutsch, later James Gosling) listed eight false assumptions that newcomers make. Each one has a data engineering version:

| Fallacy | What really happens in a pipeline |
|---|---|
| The network is reliable | A write to Cloud Storage times out; did it happen or not? |
| Latency is zero | A loop making one API call per row takes hours instead of seconds |
| Bandwidth is infinite | Shuffling 5 TB across zones costs time and egress money |
| The network is secure | Credentials in a connection string end up in logs |
| Topology does not change | A Kafka broker is replaced and clients cache its old address |
| There is one administrator | A DBA changes a source table without telling you |
| Transport cost is zero | Cross-region replication doubles the bill |
| The network is homogeneous | One client speaks HTTP/1.1, another gRPC, a third uses an old driver |

**The central problem: timeouts are ambiguous.** When you send a request and hear nothing back, there are at least four possibilities: the request was lost, the remote machine crashed before acting, it acted and crashed before replying, or the reply was lost. From the sender's side these look identical. So after a timeout you do not know whether the work was done. This single fact is why pipelines need **idempotent** operations (safe to repeat) and why "exactly once" is always built from "at least once plus deduplication".

**Fault models** describe what a broken node is allowed to do. Designs state which model they handle:

- **Crash-stop:** a node works correctly until it stops, and never comes back.
- **Crash-recovery:** a node may stop and later restart, with whatever it had written to disk. This is the realistic model for databases and brokers.
- **Omission:** a node or link drops some messages.
- **Byzantine:** a node may do anything, including lying or sending corrupt data (covered in its own topic below).

**Timing models** describe what you may assume about delays. In a **synchronous** model, messages and steps have a known upper bound on time. In an **asynchronous** model there is no bound at all. Real systems are **partially synchronous**: usually well behaved, but occasionally a network delay or a 30-second garbage collection pause breaks every bound. Good algorithms (Raft, Paxos) stay *safe* (never give a wrong answer) even when timing assumptions fail, and only need timing to make *progress*.

**Clocks.** Each machine has two clocks, and mixing them up causes real bugs:

- The **time-of-day clock** (`time.time()` in Python) returns wall-clock time. It is synchronised with NTP (Network Time Protocol) and can jump forwards or backwards when it is corrected.
- The **monotonic clock** (`time.monotonic()`) only moves forwards. It is meaningless across machines but correct for measuring durations and timeouts on one machine.

Wall clocks on different machines disagree, typically by milliseconds inside a data centre and more across the internet. That makes "last write wins by timestamp" dangerous:

```python
# Two servers write the same key. Server B's clock runs 300 ms behind.
writes = [
    {"server": "A", "value": "price=10", "true_ms": 1_000, "clock_skew_ms": 0},
    {"server": "B", "value": "price=12", "true_ms": 1_100, "clock_skew_ms": -300},
]
for w in writes:
    w["stamp"] = w["true_ms"] + w["clock_skew_ms"]   # what each server writes down

winner = max(writes, key=lambda w: w["stamp"])         # last write wins, by timestamp
print("really last:", writes[-1]["value"])           # really last: price=12
print("LWW keeps:  ", winner["value"])               # LWW keeps:   price=10
```

The newer write was silently discarded, with no error anywhere. The topic Vector Clocks & Logical Clocks shows how to order events without trusting wall clocks.

**Ordering.** Inside one machine, events have an obvious order. Across machines, the only order you can be sure of is **causal**: if event A could have influenced event B (because a message carried information from A to B), then A happened before B. Events with no such link are **concurrent**, even if one has an earlier timestamp. Kafka gives you a total order only *within one partition*; across partitions there is no order, which is why you choose the partition key so that events that must stay ordered (all events of one order, one account) share a key.

> **Pitfall:** Using wall-clock timestamps to decide which update wins, or to detect "new rows since my last run" (`WHERE updated_at > last_run`). A row committed with a slightly older timestamp, or a long transaction that commits after your query starts, is skipped forever. Use a database sequence, a log position (CDC), or overlap the window and deduplicate.

> **Tip:** On Google Cloud, Compute Engine VMs synchronise time from the metadata server, and Google's public NTP service (`time.google.com`) "smears" leap seconds over 24 hours instead of repeating a second. Do not mix smeared and non-smeared time sources in one system.

::: deeper
**The FLP result.** Fischer, Lynch and Paterson proved in 1985 that in a fully asynchronous system, no deterministic algorithm can guarantee that a group reaches agreement if even one process may crash. Real consensus algorithms escape this by assuming partial synchrony (timeouts eventually work) or by using randomness. FLP is why every consensus system can, in theory, stall during a bad network period: it chooses to stop rather than give a wrong answer.

**TrueTime.** Spanner does use physical clocks for ordering, but honestly. Google's TrueTime API returns an interval `[earliest, latest]` that is guaranteed to contain the real time, using GPS receivers and atomic clocks in each data centre to keep the interval small (a few milliseconds). Before a Spanner transaction makes its commit visible, it waits until its commit timestamp is definitely in the past on every clock ("commit wait"). The result is **external consistency**: if transaction T1 commits before T2 starts, T2's timestamp is larger. The lesson is not "clocks are fine", but "clocks are fine if you know and pay for their uncertainty".

**Gray failures.** The hardest failures are partial: a node that answers health checks but serves 1% of requests very slowly, a NIC that drops large packets only, a disk that is slow but not dead. Health checks that only test "is the process up?" miss these. Measure what clients experience (latency percentiles, error rates), not only liveness.

**Network partitions are common.** Studies of production networks (for example Bailis and Kingsbury's 2014 survey "The Network is Reliable") found partitions, switch failures and misconfigurations happening regularly, even inside a single data centre. Assume they will happen to you.

> **Interview:** "Your job writes to an external API and the call times out. What do you do?" Explain that a timeout is ambiguous, so you retry with backoff using an **idempotency key** that the server uses to recognise the repeat, or you check state before retrying. Never assume the write failed.
:::

::: check
?? Why can't a node tell a crashed peer from a slow one?
Both look the same from outside: no reply arrives. Without an upper bound on message delay, any timeout might fire just before the reply would have arrived.

?? Which clock should you use to measure how long a task took, and why?
The monotonic clock, because it never jumps backwards when NTP corrects the time-of-day clock. Durations measured with wall-clock time can even come out negative.

?? What does "crash-recovery" add to the crash-stop fault model?
Nodes can restart after a crash and continue with whatever they had saved durably. Algorithms must handle a node that rejoins with old state, which is the realistic case for databases and brokers.
:::

::: resources
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · Kleppmann and Riccomini; the chapter on the trouble with distributed systems is the best single introduction
- Course: [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/) · free lectures, papers and labs (formerly 6.824) where you build Raft and a sharded store
- Book: [Database Internals](https://www.oreilly.com/library/view/database-internals/9781492040330/) · Alex Petrov; part II covers failure detection, clocks, replication and consensus clearly
- Paper: [Spanner: Google's Globally-Distributed Database](https://research.google/pubs/spanner-googles-globally-distributed-database-2/) · TrueTime and commit wait, from the source
- Article: [Fallacies of distributed computing](https://en.wikipedia.org/wiki/Fallacies_of_distributed_computing) · the eight fallacies with their history
- Book: [Distributed Systems (Maarten van Steen and Andrew Tanenbaum)](https://www.distributed-systems.net/index.php/books/ds4/) · a free textbook covering every topic in this section
:::

### CAP Theorem & PACELC

::: plain
Imagine two bank branches that share account balances by phone. If the phone line goes down, each branch must choose: refuse to serve customers until the line is back (stay **consistent**), or keep serving and risk the two branches disagreeing about a balance (stay **available**). That forced choice during a broken line is the CAP theorem. PACELC adds the everyday case: even when the phone works, waiting for the other branch to confirm every change makes each customer wait longer, so you also choose between **latency** and **consistency** all the time.
:::

**CAP** was proposed by Eric Brewer in 2000 and proved by Seth Gilbert and Nancy Lynch in 2002. It names three properties of a replicated data store:

- **Consistency (C):** here it means **linearizability**: every read returns the most recent completed write, as if there were only one copy of the data. (This is *not* the C in ACID, which means "the database respects its constraints".)
- **Availability (A):** every request sent to a node that has not crashed gets a non-error response, eventually.
- **Partition tolerance (P):** the system keeps operating even when the network drops messages between nodes.

The theorem: **when a network partition happens, a system must give up either C or A.** You cannot "choose CA", because you do not get to choose whether partitions happen; a system that claims CA simply has not said what it does during one. So in practice the question is: *during a partition, does the system refuse some requests (CP) or answer them with possibly stale data (AP)?*

[[fig:distsys_cap]]

**A concrete partition.** Three replicas, a client on each side of a broken link. A CP system (Spanner, etcd, ZooKeeper) lets only the side with a **majority** of replicas accept writes; the minority side returns errors or waits. An AP system (Cassandra or Dynamo-style stores at low consistency levels) lets both sides accept writes and reconciles them later, so for a while readers see different values.

**Quorums make the trade-off tunable.** Many stores keep N copies, wait for W acknowledgements on write and R replies on read. If `W + R > N`, every read set overlaps every write set, so a read sees the latest acknowledged write (under normal conditions):

```python
def describe(n, w, r):
    overlap = w + r > n            # every read set meets every write set
    survives = n - max(w, r)       # nodes that can fail and still serve both
    return f"N={n} W={w} R={r}: overlap={overlap}, tolerates {survives} down"

for n, w, r in [(3, 2, 2), (3, 1, 1), (3, 3, 1), (5, 3, 3)]:
    print(describe(n, w, r))
# N=3 W=2 R=2: overlap=True, tolerates 1 down
# N=3 W=1 R=1: overlap=False, tolerates 2 down
# N=3 W=3 R=1: overlap=True, tolerates 0 down
# N=5 W=3 R=3: overlap=True, tolerates 2 down
```

In Cassandra this is `QUORUM` versus `ONE` per query. Quorum overlap alone is not full linearizability (concurrent writes and sloppy quorums can still surprise you), but it is the core idea.

**PACELC.** Daniel Abadi pointed out in 2010 (and in a 2012 paper) that CAP only talks about the rare partition case, while the trade-off you feel every day is latency. PACELC reads: **if Partition, choose Availability or Consistency; Else, choose Latency or Consistency.** Keeping replicas in sync means waiting for other nodes on every write, which costs latency even when nothing is broken.

| System | During a partition (PA/PC) | Normally (EL/EC) | Notes |
|---|---|---|---|
| Spanner | PC | EC | Synchronous Paxos replication; pays latency for external consistency |
| Bigtable (single cluster) | PC | EC | One tablet server owns each row range |
| Bigtable with multi-cluster replication | PA | EL | Replication between clusters is eventually consistent |
| Cassandra, Dynamo-style | PA | EL | Tunable per query; strong settings move it towards PC/EC |
| Firestore | PC | EC | Strongly consistent reads and transactions |
| Kafka (`acks=all`, `min.insync.replicas=2`) | PC | EC | Rejects writes if too few in-sync replicas remain |
| Kafka (`acks=1`) | PA | EL | Fast, but a leader failure can lose acknowledged writes |

**Consistency is a spectrum, not a switch.** Between linearizable and "anything goes" there are useful levels that data engineers meet daily:

- **Read-your-writes:** after you write, you see your own write (others may not yet).
- **Monotonic reads:** you never see time go backwards, for example by reading a newer replica and then an older one.
- **Causal consistency:** if B depends on A, everyone who sees B also sees A.
- **Eventual consistency:** if writes stop, all replicas converge, with no promise about when.

BigQuery, for example, is strongly consistent for its managed storage: once a load job, DML statement or committed Storage Write API stream completes, every later query sees the data. Many caches and replicas in front of databases are only eventually consistent, which is why a dashboard can briefly disagree with the source.

> **Pitfall:** "We are AP, so we are always available" is wrong in practice. An AP store whose clients use `QUORUM` reads and writes behaves like a CP one during a partition, and a CP store with five regions may be more available to users than a badly operated AP one. CAP describes a single, narrow trade-off, not overall uptime.

> **Tip:** When someone describes a system with CAP letters, ask two questions instead: "What does a client see during a partition?" and "How many network round trips does a write wait for in normal operation?" The answers tell you far more.

::: deeper
**What the proof actually assumes.** Gilbert and Lynch's proof uses an asynchronous network where messages can be lost forever, a single register, and the strict definitions of C (linearizable) and A (every non-failed node answers). Weaken any of them and the theorem says nothing. Brewer's own 2012 article "CAP Twelve Years Later" argues that the "two out of three" slogan misleads: partitions are rare, so a system should be as consistent and available as possible normally, detect partitions, operate in an explicit partition mode, and recover afterwards.

**Spanner and CAP.** Brewer, who now works at Google, wrote in 2017 that Spanner is technically CP: during a partition the minority side cannot commit. But because Google's private network makes partitions extremely rare and Spanner runs replicas across failure domains, it is "effectively CA" for users, with SLAs of 99.999% for multi-region instances. The point: availability is mostly an engineering and operations property, not a theorem.

**Harvest and yield.** Fox and Brewer (1999) suggested another way out: instead of failing a query during trouble, return a partial answer (lower *harvest*, the fraction of data reflected) while keeping *yield* (the fraction of queries answered) high. A search engine that skips one shard and says so is doing this. Dashboards can do the same: show results with a "data incomplete" banner rather than an error.

**Linearizability costs latency even without failures.** Attiya and Welch proved that a linearizable read or write must take time proportional to the network delay between replicas. That is the theoretical root of PACELC's "else" branch, and why multi-region strongly consistent writes on Spanner take tens of milliseconds.

> **Interview:** "Is Kafka CP or AP?" It depends on configuration. With `acks=all`, `min.insync.replicas=2` and `unclean.leader.election.enable=false`, a partition that leaves too few in-sync replicas makes producers fail (CP). With `acks=1` or unclean leader election, it stays writable but can lose acknowledged data (AP). Saying "it depends, here is the setting" is the strong answer.
:::

::: check
?? Why is "CA" not a real choice for a distributed database?
Partitions are not optional: the network will drop messages at some point. A system must then refuse requests or risk inconsistency, so the only real choice is what to give up during a partition.

?? With N=3 replicas, which W and R give overlapping quorums while tolerating one node down for both reads and writes?
W=2 and R=2: 2 + 2 > 3, and each operation still succeeds with one node unavailable.

?? What does the "ELC" half of PACELC add?
Even with no partition, keeping replicas consistent means waiting for other nodes, so you trade latency against consistency on every normal request.
:::

::: resources
- Paper: [Brewer's Conjecture and the Feasibility of Consistent, Available, Partition-Tolerant Web Services](https://dl.acm.org/doi/10.1145/564585.564601) · Gilbert and Lynch's proof of CAP
- Paper: [Consistency Tradeoffs in Modern Distributed Database System Design](https://ieeexplore.ieee.org/document/6127847) · Abadi's PACELC paper in IEEE Computer
- Article: [CAP Twelve Years Later: How the "Rules" Have Changed](https://www.infoq.com/articles/cap-twelve-years-later-how-the-rules-have-changed/) · Brewer's own correction of the "pick two" slogan
- Paper: [Spanner, TrueTime and the CAP Theorem](https://research.google/pubs/spanner-truetime-and-the-cap-theorem/) · Brewer on why Spanner is CP but effectively highly available
- Docs: [Jepsen consistency models](https://jepsen.io/consistency) · a clickable map of consistency levels with precise definitions
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · the consistency and consensus chapter explains linearizability and CAP with diagrams
:::

### Consistent Hashing

::: plain
Imagine sorting mail into pigeonholes by the last digit of the house number. It works until you add an eleventh pigeonhole: now almost every letter belongs in a different hole, and you must re-sort everything. Consistent hashing arranges the pigeonholes around a circle instead, and each letter goes to the next pigeonhole clockwise from its position. Add a new pigeonhole and only the letters between it and its neighbour move; everything else stays put.
:::

Any system that spreads keys over machines needs a rule that maps a key to a machine. The obvious rule is **modulo hashing**: `node = hash(key) % N`. It spreads keys evenly, but when N changes, almost every key moves, because `hash % 4` and `hash % 5` agree only by coincidence. For a cache that means a sudden flood of misses; for a database, copying most of the data across the network.

**The ring.** Consistent hashing (Karger and colleagues, 1997) hashes both keys and nodes onto the same circle of numbers (for example 0 to 2^128 - 1 with MD5). A key belongs to the first node found going clockwise from the key's position. When a node joins, it takes over only the keys between itself and its predecessor; when a node leaves, only its keys move, to its successor. On average only `1/N` of the keys move.

**Virtual nodes.** With one point per node, the arcs between points are uneven, so some nodes get far more keys than others. The fix is to give each physical node many points on the ring, called **virtual nodes** (vnodes) or tokens. More points make the load more even, and when a node leaves, its load spreads across many nodes instead of landing on one neighbour.

[[fig:distsys_ring]]

Here is a complete ring with virtual nodes, compared with modulo hashing, using 10,000 keys:

```python
import bisect, hashlib

def h(s):
    return int(hashlib.md5(s.encode()).hexdigest(), 16)

class Ring:
    def __init__(self, nodes, vnodes=100):
        self.vnodes = vnodes
        self.points = []                       # sorted list of (hash, node)
        for n in nodes:
            self.add(n)

    def add(self, node):
        for i in range(self.vnodes):
            bisect.insort(self.points, (h(f"{node}#{i}"), node))

    def owner(self, key):
        i = bisect.bisect(self.points, (h(key), ""))
        return self.points[i % len(self.points)][1]   # wrap around the ring

keys = [f"user-{i}" for i in range(10_000)]

# Modulo placement: node = hash(key) % N
before = {k: h(k) % 4 for k in keys}
after = {k: h(k) % 5 for k in keys}
print("modulo, 4 -> 5 nodes, keys moved:", sum(before[k] != after[k] for k in keys))
# modulo, 4 -> 5 nodes, keys moved: 7976

ring = Ring(["a", "b", "c", "d"])
before = {k: ring.owner(k) for k in keys}
ring.add("e")
after = {k: ring.owner(k) for k in keys}
print("ring, 4 -> 5 nodes, keys moved:", sum(before[k] != after[k] for k in keys))
# ring, 4 -> 5 nodes, keys moved: 2029
print("keys now on e:", sum(v == "e" for v in after.values()))
# keys now on e: 2029
```

Modulo hashing moved about 80% of the keys; the ring moved about 20%, which is the ideal `1/5`, and every moved key went to the new node. The number of virtual nodes controls balance. With the same keys and four nodes, the key counts per node were:

| Virtual nodes per node | Keys per node (sorted) |
|---|---|
| 1 | 381, 641, 1715, 7263 |
| 10 | 2034, 2258, 2363, 3345 |
| 100 | 2150, 2211, 2729, 2910 |
| 1000 | 2441, 2490, 2521, 2548 |

**Replication on the ring.** To keep three copies, a key is stored on the first three *distinct physical* nodes clockwise from it (the "preference list" in Amazon's Dynamo paper). Cassandra adds rack and data centre awareness so the three copies land in different failure domains, for example different Google Cloud zones.

**Hotspots.** Consistent hashing balances *keys*, not *traffic*. One celebrity user or one `NULL` key can still overload a node. Remedies: split hot keys by adding a suffix (`user-42#3`, called salting), cache hot reads, or let the system split and move busy ranges.

**Where you meet it, and where you do not.** Cassandra, ScyllaDB, Riak, Dynamo-style key-value stores, memcached client libraries (the "ketama" scheme) and many load balancers use consistent hashing. Many data tools do **not**: Kafka's default partitioner uses `murmur2(key) % partitions`, and Spark's `HashPartitioner` uses modulo too. That is why adding partitions to a Kafka topic changes which partition a key goes to and breaks per-key ordering for a while.

> **Pitfall:** Increasing the partition count of a Kafka topic (or the number of partitions a Spark job writes by hash) silently remaps keys. If consumers rely on "all events of a customer go to the same partition", plan the change: create a new topic with the new count and migrate, or accept a period of mixed ordering.

> **Tip:** Bigtable and Spanner use **range partitioning**, not hashing: rows are sorted by key and split into contiguous ranges (tablets, splits) that move between servers as load changes. Sequential keys such as timestamps then send all new writes to one range. Design row keys so writes spread out (for example, put a well-distributed field first), as the Bigtable schema design guide explains.

::: deeper
**Alternatives to the ring.**

- **Rendezvous hashing** (highest random weight, 1996): for each key, compute `hash(key, node)` for every node and pick the highest. It needs no ring, balances well and moves the minimum keys, at the cost of O(N) work per lookup, which is fine for tens or hundreds of nodes.
- **Jump consistent hash** (Lamping and Veach, Google, 2014): a few lines of arithmetic that map a key to one of N buckets with perfect balance and minimal movement, using no memory. Its limit: buckets are numbered 0 to N-1, so you can only add or remove at the end, which suits sharded storage but not arbitrary node failures.
- **Maglev hashing** (Google's software load balancer, NSDI 2016): builds a lookup table so each backend gets an almost equal share and few connections move when backends change.

**Bounded loads.** "Consistent hashing with bounded loads" (Mirrokni, Thorup and Zadimoghaddam, Google, 2016) caps each node at, for example, 1.25 times the average load; a key whose node is full goes to the next node clockwise. Load balancers use it to stop hot spots without losing most of the cache affinity.

**Token counts in Cassandra.** Older Cassandra versions defaulted to 256 tokens per node. Cassandra 4.0 changed the default to 16, together with an allocation algorithm that places tokens to balance load, because many tokens made repair and streaming slower. More virtual nodes is not automatically better.

> **Interview:** "Design a distributed cache." Mention consistent hashing with virtual nodes for placement, replication to the next distinct nodes, what happens to hit rate when a node joins (only about 1/N of keys miss), and hot key handling (local caches, key splitting). Then mention that Memorystore for Redis Cluster on Google Cloud uses Redis Cluster's 16,384 hash slots, a fixed-slot variant of the same idea.
:::

::: check
?? With modulo hashing over 9 nodes, roughly what fraction of keys move when you add a tenth node? With a consistent hash ring?
With modulo, about 90% move (a key stays only if `hash % 9 == hash % 10`). With a ring, about 1/10 move, all to the new node.

?? What problem do virtual nodes solve?
Uneven arcs: with one point per node some nodes own far more of the ring than others. Many points per node average out the arc lengths and spread a departing node's keys across many nodes.

?? Does consistent hashing stop a single very popular key from overloading a node?
No. It balances the number of keys, not their traffic. Hot keys need caching, salting into several sub-keys, or bounded-load placement.
:::

::: resources
- Paper: [Consistent Hashing and Random Trees](https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf) · Karger and colleagues, the original 1997 paper
- Paper: [Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) · consistent hashing with virtual nodes, preference lists and vector clocks in one design
- Paper: [A Fast, Minimal Memory, Consistent Hash Algorithm](https://arxiv.org/abs/1406.2294) · jump consistent hash from Google, with code
- Docs: [Bigtable schema design](https://cloud.google.com/bigtable/docs/schema-design) · how range partitioning and row key design avoid hotspots
- Blog: [Consistent Hashing with Bounded Loads](https://research.google/blog/consistent-hashing-with-bounded-loads/) · Google Research on capping per-node load
- Article: [A Guide to Consistent Hashing](https://www.toptal.com/big-data/consistent-hashing) · a gentle illustrated walk-through of the ring
:::

### Vector Clocks & Logical Clocks

::: plain
If you and a friend edit the same shared shopping list on two phones while offline, which version is "newer"? Looking at the phone clocks does not help: they may disagree, and neither of you saw the other's edit. Logical clocks replace wall-clock time with counters that record **who has seen what**. With them, a system can say for certain "this change came after that one" or "these two changes happened independently, so they conflict", which is exactly what it needs to merge them correctly.
:::

**The happens-before relation.** Leslie Lamport's 1978 paper defined the order that really matters. Event A **happened before** event B (written A → B) if:

1. A and B are on the same process and A came first, or
2. A is the sending of a message and B is its receipt, or
3. there is a chain of such steps from A to B.

If neither A → B nor B → A, the events are **concurrent**: neither could have influenced the other. Concurrency here is about information, not time.

**Lamport timestamps** give every event a number consistent with happens-before. Each process keeps a counter. It adds one before each event; it attaches the counter to every message it sends; when it receives a message, it sets its counter to `max(own, received) + 1`.

```python
class Process:
    def __init__(self, name):
        self.name, self.clock = name, 0
    def local(self):
        self.clock += 1
        return self.clock
    def send(self):
        self.clock += 1
        return self.clock              # timestamp travels with the message
    def receive(self, ts):
        self.clock = max(self.clock, ts) + 1
        return self.clock

p, q = Process("P"), Process("Q")
p.local(); p.local()                   # P: 1, 2
ts = p.send()                          # P: 3, message carries 3
q.local()                              # Q: 1 (Q has done little so far)
print(q.receive(ts))                   # 4: max(1, 3) + 1
print(p.local(), q.local())            # 4 5: equal stamps on different processes are possible
```

The guarantee runs one way only: if A → B then `L(A) < L(B)`. The reverse is false: a smaller stamp does not mean "happened before", so Lamport clocks cannot detect concurrent updates. Breaking ties by process name gives a **total order** that everyone agrees on, which is enough for things like a fair distributed queue, but not for detecting conflicts.

**Vector clocks** fix that. Each process keeps a vector of counters, one per process. On a local event it increments its own entry; on send it attaches the whole vector; on receive it takes the element-wise maximum and then increments its own entry. Comparing two vectors tells you everything:

- every entry of a ≤ every entry of b (and they differ): a happened before b;
- the other way round: b happened before a;
- otherwise: **concurrent**, a real conflict.

```python
def compare(a, b):
    """Compare two vector clocks (dicts of node -> counter)."""
    nodes = a.keys() | b.keys()
    a_le_b = all(a.get(n, 0) <= b.get(n, 0) for n in nodes)
    b_le_a = all(b.get(n, 0) <= a.get(n, 0) for n in nodes)
    if a_le_b and b_le_a:
        return "equal"
    if a_le_b:
        return "a happened before b"
    if b_le_a:
        return "b happened before a"
    return "concurrent: conflict"

def merge(a, b):
    return {n: max(a.get(n, 0), b.get(n, 0)) for n in a.keys() | b.keys()}

base = {"A": 1}                   # cart written once on replica A
v1 = {**base, "A": 2}             # phone updates via replica A
v2 = {**base, "B": 1}             # laptop updates via replica B, never saw v1
print(compare(base, v1))          # a happened before b
print(compare(v1, v2))            # concurrent: conflict
resolved = merge(v1, v2)
resolved["A"] += 1                # the resolving write is a new event on A
print(sorted(resolved.items()))   # [('A', 3), ('B', 1)]
print(compare(v2, resolved))      # a happened before b
```

[[fig:distsys_vclock]]

**Version vectors** are the same structure used by replicas of a data item rather than by processes: each replica counts the updates it has accepted. Amazon's Dynamo returned all concurrent versions ("siblings") of a shopping cart to the application, which merged them; Riak later used **dotted version vectors** to stop the vectors growing without bound when many clients write through few servers.

**CRDTs: merge without conflicts.** Detecting a conflict is half the job; something still has to resolve it. A **conflict-free replicated data type** (CRDT, Shapiro and colleagues, 2011) is a data structure designed so that concurrent updates *always* merge to the same result, with no coordination. The simplest is a grow-only counter: each replica counts its own increments, and merging takes the maximum per replica.

```python
class GCounter:
    """Grow-only counter CRDT: one slot per replica, merge takes the max."""
    def __init__(self, replica):
        self.replica, self.slots = replica, {}
    def incr(self, n=1):
        self.slots[self.replica] = self.slots.get(self.replica, 0) + n
    def merge(self, other):
        for r, v in other.slots.items():
            self.slots[r] = max(self.slots.get(r, 0), v)
    def value(self):
        return sum(self.slots.values())

eu, us = GCounter("eu"), GCounter("us")
eu.incr(3); us.incr(5)                 # concurrent increments, no coordination
eu.merge(us); us.merge(eu); eu.merge(us)   # merge in any order, any number of times
print(eu.value(), us.value())          # 8 8
```

There are two families:

| | State-based (CvRDT) | Operation-based (CmRDT) |
|---|---|---|
| What is sent | The whole state (or a delta) | Each operation ("add 3") |
| Merge rule | Must be commutative, associative and idempotent | Concurrent operations must commute |
| Network needs | Any order, duplicates fine | Every operation delivered exactly once, in causal order |
| Examples | G-Counter, PN-Counter, OR-Set, LWW-Register | Collaborative text editing, op-based counters |

Other common CRDTs: a PN-Counter (two G-Counters, increments minus decrements), an OR-Set (observed-remove set, where an element added concurrently with its removal survives), and registers and maps built from them. Redis Enterprise active-active, Riak and collaborative editors such as those built on Automerge or Yjs use CRDTs.

> **Pitfall:** A last-writer-wins register is technically a CRDT (it converges), but it converges by **throwing data away**: the concurrent write with the lower timestamp is lost. "It converges" and "it keeps every user's change" are different promises.

> **Tip:** In data pipelines you already use logical clocks: a Kafka offset is a per-partition logical clock, a database's log sequence number (LSN) or Datastream's source position orders changes, and a Spanner commit timestamp orders transactions globally. Order and deduplicate by these, not by `updated_at`.

::: deeper
**Hybrid logical clocks (HLC).** Kulkarni and colleagues (2014) combined a physical timestamp with a logical counter: the clock stays close to wall time (so it is readable and usable for "as of" queries) but never goes backwards and always respects happens-before. CockroachDB and YugabyteDB use HLCs, and MongoDB's cluster time is a similar hybrid. It is the practical middle ground between Lamport clocks and TrueTime.

**Vector size.** A vector clock needs one entry per participant. That is fine for a handful of replicas, but not for millions of clients, which is why systems track vectors per *server replica* (version vectors) and why dotted version vectors were invented.

**Causal delivery.** Some systems guarantee that a message is delivered only after every message that causally precedes it, using vector timestamps to hold back early arrivals. Op-based CRDTs need this; state-based CRDTs do not, which is why state-based (and delta-state) CRDTs are more common over unreliable networks.

**Why CRDTs are not everything.** They cannot enforce invariants that need coordination, such as "the balance never goes below zero" or "a username is unique". Bailis and colleagues call the dividing line **invariant confluence**: if merging two valid states can produce an invalid one, you need coordination (a transaction or consensus) for that operation.

> **Interview:** "How would you detect conflicting updates in a multi-region store?" Explain vector or version vectors per replica: compare them on read, and if neither dominates, the writes were concurrent. Then choose a resolution: return siblings to the application, merge with a CRDT, or accept LWW and its data loss explicitly.
:::

::: check
?? If Lamport timestamp L(A) = 3 and L(B) = 5, did A happen before B?
Not necessarily. Lamport clocks guarantee that A → B implies L(A) < L(B), not the reverse. A and B may be concurrent. Vector clocks are needed to tell.

?? Compare the vector clocks {A: 2, B: 0} and {A: 1, B: 1}.
Neither is less than or equal to the other in every entry (2 > 1 for A, 0 < 1 for B), so the events are concurrent: a conflict.

?? Why must a state-based CRDT's merge be idempotent?
Replicas may receive the same state many times through gossip or retries. Merging the same state twice must not change the result, otherwise duplicates would inflate counters or corrupt sets.
:::

::: resources
- Paper: [Time, Clocks, and the Ordering of Events in a Distributed System](https://dl.acm.org/doi/10.1145/359545.359563) · Lamport 1978, one of the most cited papers in computing
- Paper: [Conflict-free Replicated Data Types](https://inria.hal.science/hal-00932836) · Shapiro, Preguiça, Baquero and Zawirski, the paper that defined CRDTs
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · chapters on replication and on clocks and ordering
- Blog: [Why Vector Clocks Are Hard](https://riak.com/posts/technical/why-vector-clocks-are-hard/) · Basho on the practical problems of vector clocks in Riak
- Article: [CRDT resources](https://crdt.tech/) · a curated hub of CRDT papers, talks and implementations
- Paper: [Logical Physical Clocks (Hybrid Logical Clocks)](https://cse.buffalo.edu/tech-reports/2014-04.pdf) · Kulkarni and colleagues' HLC paper
:::

## Agreement and coordination

The first group showed that machines cannot trust each other's clocks or tell slow from dead. Yet many systems need a single answer to questions such as "who is the leader?", "has this write committed?" or "which worker owns this partition?". This group is about how a group of unreliable machines agrees on such answers.

It starts with the algorithms (Raft and Paxos), moves to the patterns built on them (leader election, leases, locks and fencing tokens), and then to the two systems that package those patterns as a service (ZooKeeper and etcd). The last two topics cover the edges of the field: Byzantine fault tolerance, for when nodes may lie, and gossip protocols, for when you would rather spread information cheaply than agree on it exactly.

### Consensus Algorithms — Raft & Paxos

::: plain
Consensus is how a committee of machines agrees on one decision even though some members may be absent and messages may be lost. The trick is a **majority vote**: any two majorities of the same group always share at least one member, so two conflicting decisions can never both win. Raft adds a chairperson (the **leader**) who proposes every decision, and a rule for electing a new chairperson when the old one goes quiet. As long as most members are reachable, the committee keeps working and never contradicts itself.
:::

**What consensus gives you.** A **replicated log**: an ordered list of commands that every node applies in the same order, so every node ends up in the same state. This is called **state machine replication**. A key-value store, a lock service, a Kafka controller or a database's replicas can all be built as "apply the agreed log in order". Consensus guarantees **safety** (no two nodes ever decide different values for the same log position) always, and **liveness** (decisions keep being made) whenever a majority can communicate for long enough.

**Majorities (quorums).** With N nodes, a majority is `N // 2 + 1`. Two majorities always overlap, which is what prevents **split brain** (two leaders both accepting writes). The cost is that you need `2f + 1` nodes to survive `f` failures:

```python
def commit_index(leader_last, match_index, n_nodes):
    """Highest log index stored on a majority (leader counts itself)."""
    indexes = sorted([leader_last] + match_index, reverse=True)
    majority = n_nodes // 2 + 1
    return indexes[majority - 1]

# 5-node cluster; leader has entries up to 7; followers have replicated up to:
print(commit_index(7, [7, 5, 3, 2], 5))   # 5: entries 1-5 are on 3 of 5 nodes
print(commit_index(7, [7, 7, 3, 2], 5))   # 7
for n in (1, 2, 3, 4, 5, 7):
    print(f"{n} nodes: majority {n // 2 + 1}, survives {(n - 1) // 2} down")
# 1 nodes: majority 1, survives 0 down
# 2 nodes: majority 2, survives 0 down
# 3 nodes: majority 2, survives 1 down
# 4 nodes: majority 3, survives 1 down
# 5 nodes: majority 3, survives 2 down
# 7 nodes: majority 4, survives 3 down
```

Notice that 4 nodes survive no more failures than 3, and 2 nodes survive fewer than 1 (both must be up). That is why consensus clusters have odd sizes, usually 3 or 5.

**Raft** (Ongaro and Ousterhout, 2014) was designed to be understandable. It splits the problem into three parts.

1. **Leader election.** Time is divided into numbered **terms**. Every node starts as a **follower**. If a follower hears nothing from a leader for its **election timeout** (randomised, for example 150 to 300 ms in the paper, so nodes rarely time out together), it becomes a **candidate**, increments the term, votes for itself and sends `RequestVote` to the others. Each node votes for at most one candidate per term. A candidate with votes from a majority becomes **leader** and starts sending heartbeats.
2. **Log replication.** Clients send commands to the leader. The leader appends each command to its log with the current term and sends it to followers with `AppendEntries`. Once an entry is stored on a majority, it is **committed**; the leader applies it, replies to the client, and tells followers the new commit index in later messages.
3. **Safety.** A node only votes for a candidate whose log is at least as up to date as its own (compare the last entry's term, then its index). Because a committed entry is on a majority and any winning candidate needs a majority of votes, every new leader already holds every committed entry. Followers whose logs disagree with the leader's are overwritten from the point of divergence.

[[fig:distsys_raft]]

**Paxos** (Lamport, published 1998, explained again in "Paxos Made Simple" in 2001) is older and solves the same problem. Basic Paxos agrees on a single value in two phases:

- **Phase 1 (prepare and promise):** a proposer picks a proposal number `n` and asks acceptors to promise to ignore proposals numbered below `n`. Each acceptor replies with a promise and with any value it has already accepted.
- **Phase 2 (accept):** if a majority promised, the proposer sends `accept(n, v)`, where `v` must be the value of the highest-numbered proposal already accepted by any of them (or its own value if none). The value is chosen once a majority accepts it.

**Multi-Paxos** runs this for each log position and keeps a stable leader so that phase 1 runs once and each new entry needs only phase 2, which makes it look a lot like Raft. In practice the differences are in the details: Raft fixes many choices that Paxos leaves open (how leaders are chosen, how logs are repaired), which is why most new systems pick Raft.

**Where you meet consensus:**

| System | Algorithm | Used for |
|---|---|---|
| etcd (and so Kubernetes) | Raft | All cluster state |
| Kafka KRaft controllers | Raft-based protocol | Cluster metadata (Kafka 4.0 removed ZooKeeper entirely) |
| ZooKeeper | ZAB, a Paxos-like atomic broadcast | Coordination data |
| Spanner | Paxos, one group per split | Replicating every write across zones or regions |
| CockroachDB, TiKV, YugabyteDB | Raft, one group per range | Replicating each range of keys |
| Consul, HashiCorp Vault (integrated storage) | Raft | Service catalog, secrets |

> **Pitfall:** Running a consensus cluster across exactly two locations. With 3 nodes split 2 and 1, losing the site with 2 nodes stops the cluster. Put the nodes in three failure domains (three zones in a Google Cloud region, for example), so any single zone can fail.

> **Tip:** Consensus is for small amounts of critical metadata (leaders, configuration, offsets, schemas), not for bulk data. Each write needs a network round trip to a majority and a disk sync. Systems such as Spanner and CockroachDB scale by running many independent consensus groups, one per data range.

::: deeper
**Committing entries from earlier terms.** A Raft leader only counts replicas to commit entries from *its own* term. An entry from an earlier term can be on a majority and still be overwritten by a later leader (Figure 8 in the Raft paper shows exactly how). The new leader commits older entries indirectly, by committing a new entry of its own term on top of them; Raft leaders append a no-op entry at the start of their term for this reason.

**Reads are not free.** A leader that serves reads from its own state may have been deposed without knowing it (for example during a partition), and would return stale data. Options: put the read through the log (slow), use **ReadIndex** (confirm leadership with a round of heartbeats, then read), or use **leader leases** (assume leadership for a bounded time after the last heartbeat, which relies on clocks running at roughly the same rate). etcd's default linearizable reads use ReadIndex; its "serializable" reads skip the check and may be stale.

**Membership changes.** Adding or removing nodes is dangerous, because the old and new configurations could each form a majority. Raft handles it with joint consensus or with single-server changes, one node at a time. Operators should follow the system's documented procedure (for example `etcdctl member add` as a learner first) rather than editing configuration files.

**Pre-vote and check quorum.** A node that was partitioned keeps raising its term and, when it returns, forces an unnecessary election. Pre-vote (ask "would you vote for me?" without raising the term) and check quorum (a leader steps down if it cannot reach a majority) prevent this; etcd supports both.

> **Interview:** "Why does a Raft cluster need an odd number of nodes?" It does not strictly need one, but an even number adds cost without adding fault tolerance: 4 nodes tolerate 1 failure like 3 do, and need 3 votes instead of 2. Also explain why two majorities always overlap and how that prevents split brain.
:::

::: check
?? A 5-node Raft cluster loses 2 nodes. Can it still commit writes? What about losing 3?
With 2 down, 3 of 5 remain, which is a majority, so it keeps working. With 3 down, only 2 remain, less than a majority, so it stops accepting writes (safety is kept, liveness is lost).

?? Why are Raft election timeouts randomised?
So that followers rarely time out at the same moment. If they did, they would all become candidates, split the vote and need another round; randomness lets one candidate usually win quickly.

?? What does it mean for a log entry to be "committed" in Raft?
It is stored on a majority of nodes (and is from the leader's current term), so every future leader will have it. It can then be applied to the state machine and acknowledged to the client.
:::

::: resources
- Paper: [In Search of an Understandable Consensus Algorithm (Raft)](https://raft.github.io/raft.pdf) · the Raft paper, readable in an afternoon
- Docs: [The Raft Consensus Algorithm](https://raft.github.io/) · an interactive visualisation plus links to implementations and talks
- Paper: [Paxos Made Simple](https://www.microsoft.com/en-us/research/uploads/prod/2016/12/paxos-simple-Copy.pdf) · Lamport's short explanation of basic Paxos
- Article: [The Secret Lives of Data: Raft](https://thesecretlivesofdata.com/raft/) · an animated step-by-step guide to elections and replication
- Course: [MIT 6.5840 labs](https://pdos.csail.mit.edu/6.824/) · implement Raft yourself in Go, with tests that inject failures
- Paper: [Paxos Made Live](https://static.googleusercontent.com/media/research.google.com/en//archive/paxos_made_live.pdf) · Google's lessons from building Chubby's Paxos in production
:::

### Leader Election & Coordination

::: plain
Many jobs need exactly one worker in charge: one scheduler deciding what runs, one process compacting a table, one consumer handling a partition. Leader election is how a group of workers picks that one, and picks a new one if it disappears. Think of the "talking stick" in a meeting: only the person holding it speaks. The hard part is the person who fell asleep holding the stick, wakes up, and starts talking again after someone else has been given a new one. Fencing tokens are the numbered sticks that let everyone else ignore the sleeper.
:::

**Why one leader?** Many tasks are much simpler with a single decision-maker: assigning work without duplicates, writing to a file that does not support concurrent writers, sending each scheduled email once, running one Airflow scheduler loop at a time on a partition of DAGs. The goal is **at most one active leader at a time** (safety) and **a new leader soon after the old one fails** (liveness).

**Leases, not locks.** A lock that lives forever is dangerous: if its holder crashes, nobody else can ever take it. Distributed locks are therefore **leases**: a lock with an expiry time that the holder must renew (heartbeat) regularly. If the holder dies, the lease expires and another node can acquire it. All coordination services work this way:

| Tool | Lease mechanism | Leader election recipe |
|---|---|---|
| ZooKeeper | A session with a timeout; **ephemeral** znodes vanish when the session expires | Each candidate creates an ephemeral sequential znode; the lowest number is leader; others watch the node just before theirs |
| etcd | A **lease** with a TTL that the client keeps alive; keys attached to it are deleted on expiry | The `concurrency` package's `Election` (or `etcdctl elect`): the candidate with the oldest key revision under a prefix leads |
| Kubernetes | A `Lease` object in `coordination.k8s.io` renewed by the holder | client-go's `leaderelection` package, used by controllers |
| A SQL database | A row with `owner` and `expires_at`, or row locks | `UPDATE ... WHERE expires_at < now()` or `SELECT ... FOR UPDATE SKIP LOCKED` |
| Cloud Storage | An object written with a generation precondition | Create `leader.lock` with `ifGenerationMatch=0` (only if absent); renew by rewriting with the current generation |

**The sleeping leader problem.** A lease does not stop the *old* holder from acting. Suppose worker 1 holds a lease for 10 seconds and then pauses for 15 seconds (a long garbage collection, a VM live migration, a network stall). Its lease expires, worker 2 takes over, and then worker 1 wakes up, still believing it is the leader, and writes. Now two leaders write. Checking "do I still hold the lease?" just before writing does not help, because the pause can happen between the check and the write.

**Fencing tokens.** The fix is to make the *resource being protected* reject stale leaders. Every time the lock service grants a lease, it hands out a **fencing token**: a number that only ever increases. The leader sends its token with every write, and the storage remembers the highest token it has seen and refuses lower ones:

```python
class Storage:
    """Accepts a write only if its fencing token is the highest seen so far."""
    def __init__(self):
        self.max_token, self.data = 0, []
    def write(self, token, value):
        if token < self.max_token:
            return f"rejected {value!r} (token {token} < {self.max_token})"
        self.max_token = token
        self.data.append(value)
        return f"accepted {value!r} (token {token})"

lock_service_counter = 0
def acquire_lease():
    global lock_service_counter
    lock_service_counter += 1          # tokens only ever increase
    return lock_service_counter

s = Storage()
t1 = acquire_lease()                   # worker 1 gets token 1, then pauses (GC, network)
t2 = acquire_lease()                   # lease expired; worker 2 gets token 2
print(s.write(t2, "from worker 2"))    # accepted 'from worker 2' (token 2)
print(s.write(t1, "from worker 1"))    # rejected 'from worker 1' (token 1 < 2)
```

[[fig:distsys_fencing]]

Real systems already provide tokens: ZooKeeper's `zxid` or a znode's version, an etcd key's revision, a Kafka **epoch** (a controller epoch, a leader epoch, a producer epoch), a Cloud Storage object's **generation** number, or a `version` column in a database row checked with `UPDATE ... WHERE version = :expected`. The general name for this is **compare-and-set**: write only if the state is still what I last saw.

**Coordination patterns data engineers use:**

- **Work assignment:** in a Kafka consumer group, one broker acts as the group coordinator and hands out partitions to members; every rebalance starts a new **generation** with a higher number, and commits from members of an older generation are rejected.
- **Singleton jobs:** a nightly compaction that must not run twice uses a lease row plus an idempotent write (for example an Iceberg commit, which fails if the table changed underneath it).
- **Highly available schedulers:** Airflow 2 and later can run several schedulers at once; they coordinate through row-level locks in the metadata database (`SELECT ... FOR UPDATE SKIP LOCKED`) rather than electing one leader.
- **Service discovery:** clients find the current leader or the live set of workers by reading a registry (etcd keys, ZooKeeper znodes, Consul, or Kubernetes Services and DNS) and watching it for changes.

> **Pitfall:** Redis-based locks (a single `SET key value NX PX 10000`, or the multi-node Redlock algorithm) are fine for efficiency ("avoid doing the work twice, usually") but not for correctness ("never do it twice"). They give no fencing token and depend on timing. If a duplicate would corrupt data, use fencing or compare-and-set on the resource itself.

> **Tip:** Prefer designs that need no leader. If every write is idempotent and every commit is a compare-and-set (a `MERGE` keyed on a business key, an Iceberg snapshot commit, a Cloud Storage write with a generation precondition), two workers doing the same job is merely wasteful, not dangerous.

::: deeper
**Herd effect.** In the naive ZooKeeper recipe every candidate watches the leader's znode. When it disappears, all of them wake up and query at once. Watching only your immediate predecessor wakes one node at a time; the ZooKeeper recipes page describes this carefully, including the case where the predecessor vanishes before the watch is set.

**Lease timing.** A lease holder must stop acting *before* the lease expires by its own clock, with a safety margin, because its clock may run slow relative to the lock service's. Lease durations are a trade-off: short leases mean fast failover but more false failovers during brief stalls; long leases mean slow recovery. Typical values are a few seconds to tens of seconds.

**Chubby.** Google's Chubby lock service (Burrows, 2006) introduced many of these ideas: coarse-grained locks held for hours, sessions with keepalives, and **sequencers** (fencing tokens) that servers can check. ZooKeeper was designed as an open-source service in the same spirit, though it exposes lower-level primitives.

**Leader election versus consensus.** Electing a leader *safely* (never two at once) is itself a consensus problem, which is why practical systems delegate it to a consensus-backed service or a strongly consistent database. Spanner, Cloud SQL, AlloyDB or Firestore transactions can all hold a lease row correctly; an eventually consistent store cannot.

> **Interview:** "How do you make sure a scheduled job runs exactly once across three replicas?" Answer in layers: a lease (in etcd, ZooKeeper, Kubernetes or a transactional database) picks one runner; a fencing token or compare-and-set stops a paused former leader; and the job itself is idempotent, so that a rare duplicate run does no harm.
:::

::: check
?? Why is "check that I still hold the lease, then write" not safe?
The process can pause (garbage collection, network stall) between the check and the write. The lease can expire and be granted to someone else during the pause, so the write happens without a valid lease.

?? What property must fencing tokens have, and who checks them?
They must strictly increase with each new lease grant. The protected resource (storage, database, API) checks them and rejects any token lower than the highest it has already seen.

?? Name two things already present in Google Cloud or Kafka that can act as fencing tokens.
For example: a Cloud Storage object's generation number (with `ifGenerationMatch` preconditions), a Kafka producer or leader epoch, or a version column checked in a Spanner or Cloud SQL transaction.
:::

::: resources
- Docs: [ZooKeeper Recipes and Solutions](https://zookeeper.apache.org/doc/current/recipes.html) · the official leader election, lock and barrier recipes, including the herd effect
- Docs: [etcd documentation](https://etcd.io/docs/) · leases, the concurrency API and `etcdctl elect` and `lock`
- Blog: [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) · Kleppmann on fencing tokens and why timing-based locks are unsafe
- Paper: [The Chubby lock service for loosely-coupled distributed systems](https://research.google/pubs/the-chubby-lock-service-for-loosely-coupled-distributed-systems/) · Google's lock service, sequencers and lessons learned
- Docs: [Cloud Storage request preconditions](https://cloud.google.com/storage/docs/request-preconditions) · generation-match preconditions for compare-and-set on objects
- Docs: [Kubernetes Leases](https://kubernetes.io/docs/concepts/architecture/leases/) · how Kubernetes components elect leaders with Lease objects
:::

### Apache ZooKeeper & etcd

::: plain
ZooKeeper and etcd are small, extremely reliable notice boards that a whole cluster of machines can read and write. They hold only a little information (who is the leader, which workers are alive, the current configuration), but every machine sees the same notices in the same order, and anyone can ask to be told the moment a notice changes. Instead of every system inventing its own way to agree, they put the hard part in one place and use it as a service.
:::

**Why a coordination service?** Raft and Paxos are hard to implement correctly. ZooKeeper (Yahoo, 2008, now an Apache project) and etcd (CoreOS, 2013, now a CNCF project) implement consensus once and expose simple primitives on top: a small, strongly consistent key-value store, **sessions or leases** to detect dead clients, **watches** to get notified of changes, and **conditional updates** for compare-and-set. Leader election, locks, membership, configuration and barriers are built from those primitives.

**ZooKeeper's model.** Data lives in a tree of **znodes**, like a file system: `/kafka/brokers/ids/1`. Each znode holds a small piece of data (the default limit is about 1 MB, and real usage should be far smaller) and has a version number. Special kinds:

- **Ephemeral** znodes are deleted automatically when the session of the client that created them ends. A worker registers itself with an ephemeral znode; if it dies, its znode disappears and everyone watching sees it.
- **Sequential** znodes get a monotonically increasing suffix (`lock-0000000042`), which gives a fair queue and the leader election recipe.
- **Watches** notify a client when a znode or its children change. Classic watches fire once and must be set again; ZooKeeper 3.6 added persistent and recursive watches.

```bash
# ZooKeeper CLI (zkCli.sh)
create /workers ""
create -e -s /workers/w- "host-a:9000"      # ephemeral + sequential: /workers/w-0000000000
ls -w /workers                               # list children and set a watch on them
get -s /workers/w-0000000000                 # data plus stat (version, zxid, ephemeralOwner)
```

ZooKeeper replicates with **ZAB** (ZooKeeper Atomic Broadcast): a leader orders all writes, each identified by a **zxid** (an epoch number for the current leader plus a counter), and a write commits when a majority has logged it. Writes are linearizable. Reads are served by whichever server the client is connected to, so they are fast but may be slightly stale; a client that must see the latest value calls `sync` first. ZooKeeper does guarantee that a single client sees its own updates in order.

**etcd's model.** etcd is a flat key-value store (keys are byte strings; prefixes such as `/registry/pods/` act like directories) replicated with Raft. Every change increments a cluster-wide **revision**, and etcd keeps old versions (MVCC) until compaction, which enables two useful features: you can read the store as of a revision, and you can **watch from a revision**, so a client that disconnects can resume without missing events. Leases with TTLs replace sessions, and transactions (`Txn`) provide compare-and-set: "if key X has revision R, then put Y, else get X".

```bash
# etcd v3 CLI
etcdctl put /config/batch_size 500
etcdctl get /config/ --prefix                       # read everything under a prefix
etcdctl watch /config/ --prefix                     # stream changes as they happen
etcdctl lease grant 10                              # lease 694d... granted with TTL(10s)
etcdctl put /workers/host-a alive --lease=694d...   # key disappears if the lease is not kept alive
etcdctl elect batch-leader host-a                   # campaign for leadership; blocks until elected
```

etcd reads are linearizable by default (the leader confirms it is still leader before answering) and can be made **serializable** (served locally, possibly stale) for speed.

**Comparison.**

| | ZooKeeper | etcd |
|---|---|---|
| Language, API | Java; custom binary protocol with Java and C clients | Go; gRPC API with a JSON gateway |
| Data model | Hierarchical znodes | Flat keys with revisions (MVCC) |
| Replication | ZAB | Raft |
| Liveness of clients | Sessions and ephemeral znodes | Leases with TTLs |
| Change notification | Watches (one-shot, or persistent since 3.6) | Watch streams resumable from a revision |
| Default read consistency | Possibly stale per server (use `sync`) | Linearizable (serializable optional) |
| Best known user | Hadoop HA, HBase, older Kafka, Solr | Kubernetes |

**Where data engineers meet them.** HDFS NameNode high availability uses ZooKeeper (through the ZKFailoverController) to pick the active NameNode; HBase uses it to track region servers; Kafka used it for metadata until KRaft, and **Kafka 4.0 (2025) removed ZooKeeper support entirely**, so new clusters, including Google Managed Service for Apache Kafka, do not need it. Kubernetes keeps all cluster state in etcd, which matters if you run Spark, Flink or Airflow on GKE: an overloaded etcd slows every deployment. On Google Cloud you rarely run either directly: GKE manages etcd for you, and Managed Service for Apache Spark (formerly Dataproc) runs ZooKeeper for you when you create a high-availability cluster.

> **Pitfall:** Storing large or fast-changing data in a coordination service, such as per-record offsets, big configuration blobs or metrics. Both systems keep the whole data set in memory on every node and push each write through consensus. Store pointers and small metadata there, and keep data elsewhere. etcd's storage quota defaults to 2 GB, and exceeding it puts the cluster into a read-and-delete-only alarm state.

> **Tip:** Run three or five members in different zones, on fast disks: both systems sync their log to disk before acknowledging a write, so disk latency becomes write latency. etcd's own docs recommend SSDs and monitoring `wal_fsync_duration_seconds`.

::: deeper
**Sessions and timeouts.** A ZooKeeper session times out if the server hears no heartbeat for the negotiated session timeout. During a long garbage collection pause in the client, its session can expire and its ephemeral znodes vanish, so another node becomes leader while the paused client still thinks it is. This is the sleeping leader problem from the previous topic, and the reason to combine ephemeral znodes with fencing.

**Observers and learners.** ZooKeeper **observers** receive and serve the replicated data but do not vote, so you can add read capacity (or a replica in a remote region) without slowing the write quorum. etcd's **learners** are non-voting members used to catch up a new node before promoting it to voter, so it does not reduce availability while it copies data.

**Compaction and defragmentation.** Because etcd keeps old revisions, it must **compact** history (Kubernetes' API server does it regularly) and occasionally **defragment** the backing database file to reclaim disk space. Forgetting either is a classic cause of etcd hitting its quota.

**Why Kafka dropped ZooKeeper.** With ZooKeeper, a new Kafka controller had to load all metadata from ZooKeeper on failover, which took a long time for clusters with hundreds of thousands of partitions. In KRaft, the controllers form a Raft quorum and store metadata as an internal event log (`__cluster_metadata`), so a standby controller is already up to date and brokers fetch metadata changes incrementally.

> **Interview:** "How does Kubernetes know a pod's state?" All objects live in etcd; the API server is the only component that talks to etcd, and controllers use watches (via the API server) to react to changes. That is the same watch-and-reconcile pattern ZooKeeper made popular.
:::

::: check
?? What is an ephemeral znode, and what is it used for?
A znode deleted automatically when the session of the client that created it ends. It is used for liveness: worker registration, leader election and locks that must disappear if their holder dies.

?? Why can an etcd client that disconnects for a minute resume its watch without missing changes, as long as history has not been compacted?
etcd versions every change with a cluster-wide revision and keeps old revisions. The client resumes the watch from the last revision it saw and receives every change after it.

?? Does a new Kafka 4.x cluster need ZooKeeper?
No. Kafka 4.0 removed ZooKeeper support; metadata is managed by KRaft controllers using a Raft-based log.
:::

::: resources
- Paper: [ZooKeeper: Wait-free Coordination for Internet-scale Systems](https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf) · the USENIX ATC 2010 paper explaining the design and its guarantees
- Docs: [Apache ZooKeeper documentation](https://zookeeper.apache.org/doc/current/) · programmer's guide, administrator's guide and recipes
- Docs: [Why etcd](https://etcd.io/docs/v3.5/learning/why/) · etcd's own comparison with ZooKeeper, Consul and databases
- Docs: [etcd hardware recommendations](https://etcd.io/docs/v3.5/op-guide/hardware/) · disks, memory and network for a healthy cluster
- Article: [KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum) · the design proposal behind KRaft
- Book: [ZooKeeper: Distributed Process Coordination](https://www.oreilly.com/library/view/zookeeper/9781449361297/) · Junqueira and Reed, written by ZooKeeper's authors
:::

### Byzantine Fault Tolerance

::: plain
Most distributed systems assume broken machines simply stop. Byzantine fault tolerance (BFT) handles a nastier case: machines that keep running but send **wrong or contradictory** messages, whether because of a bug, corrupted memory or an attacker. It is named after a puzzle about generals who must agree to attack or retreat while some of them are traitors who tell different generals different things. Tolerating liars costs much more than tolerating crashes, which is why almost no data system inside one company bothers, while blockchains, where nobody trusts anybody, must.
:::

**The Byzantine generals problem** (Lamport, Shostak and Pease, 1982): several divisions of an army, each led by a general, surround a city and communicate only by messenger. They must agree on a common plan, but some generals are traitors who may send "attack" to one and "retreat" to another. The paper proved that with oral (unsigned) messages, agreement is possible only if fewer than one third of the generals are traitors: you need at least `3f + 1` participants to tolerate `f` Byzantine ones.

**Why 3f + 1?** A node must make progress after hearing from `n - f` nodes, because `f` may never answer. Among those it heard from, up to `f` may be liars. For the honest majority of what it heard to outweigh the liars, `n - f - f > f`, so `n > 3f`. Crash faults need only `2f + 1`, because a crashed node says nothing rather than something wrong:

```python
for f in (1, 2, 3):
    cft_nodes, cft_quorum = 2 * f + 1, f + 1
    bft_nodes, bft_quorum = 3 * f + 1, 2 * f + 1
    print(f"tolerate {f}: crash-only {cft_nodes} nodes (quorum {cft_quorum}), "
          f"Byzantine {bft_nodes} nodes (quorum {bft_quorum})")
# tolerate 1: crash-only 3 nodes (quorum 2), Byzantine 4 nodes (quorum 3)
# tolerate 2: crash-only 5 nodes (quorum 3), Byzantine 7 nodes (quorum 5)
# tolerate 3: crash-only 7 nodes (quorum 4), Byzantine 10 nodes (quorum 7)
```

(Here "quorum" for the crash-only case means the minimum that must answer; Raft's write quorum for 3 nodes is 2.)

**PBFT.** For 17 years BFT was considered impractical. Practical Byzantine Fault Tolerance (Castro and Liskov, OSDI 1999) showed it could run fast enough for real services. A primary (leader) orders client requests in three phases:

1. **Pre-prepare:** the primary assigns a sequence number to a request and multicasts it.
2. **Prepare:** each replica multicasts that it accepts this assignment; once a replica has `2f` matching prepares from others (plus the pre-prepare), it knows the honest replicas agree on the order.
3. **Commit:** replicas multicast commit messages; after `2f + 1` commits a replica executes the request and replies.

The client waits for `f + 1` identical replies, because at least one of them must come from an honest replica. If the primary misbehaves or stalls, replicas run a **view change** to replace it. Messages are authenticated (signatures or message authentication codes), so a liar cannot forge others' messages. The cost: every replica talks to every other replica, so messages grow with the square of the number of nodes, which keeps classical PBFT to tens of nodes.

**BFT versus CFT.**

| | Crash fault tolerant (CFT) | Byzantine fault tolerant (BFT) |
|---|---|---|
| Faulty nodes may | Stop, lose messages, restart | Do anything: lie, equivocate, collude |
| Nodes for f faults | 2f + 1 | 3f + 1 |
| Message cost | Linear in n (leader to followers) | Quadratic in classic PBFT; linear in HotStuff-style protocols |
| Examples | Raft, Paxos, ZAB, Kafka replication | PBFT, Tendermint (CometBFT), HotStuff, BFT-SMaRt |
| Typical setting | One organisation's data centres | Mutually distrusting parties, public blockchains |

**Where BFT matters for data engineers.** Inside one company's platform, nodes are run by the same team with the same software, so a full BFT protocol buys little: a software bug would hit every replica the same way, and BFT only helps if fewer than a third misbehave. What *does* matter is the Byzantine-ish failure you meet in practice: **silent data corruption**. Disks, memory, network cards and even CPUs occasionally return wrong bits without an error. Data systems defend against this with cheaper, targeted tools:

- **Checksums end to end:** Kafka record batches carry a CRC, HDFS and Cloud Storage store checksums (CRC32C and MD5 for objects) and verify them, and Parquet pages can carry CRCs. Upload with `gcloud storage cp`, which validates checksums, rather than trusting a byte count.
- **Merkle trees** (hash trees) let replicas compare large data sets cheaply; Cassandra's anti-entropy repair uses them.
- **Validation at boundaries:** schema checks, row counts and reconciliation queries between source and target catch corruption that checksums cannot (wrong but well-formed data).

**Blockchains** are the main home of BFT. Permissioned ledgers use PBFT-style protocols among known members; public chains such as Bitcoin use **proof of work** (Nakamoto consensus), which tolerates Byzantine participants as long as honest ones control most of the computing power, at the cost of only probabilistic finality (a block becomes safer the deeper it is buried). Ethereum uses proof of stake with a BFT-style finality gadget. For a data engineer this mostly means ingesting chain data: the BigQuery public datasets include several blockchains, and you must handle **reorganisations** (recent blocks that get replaced) by treating the newest blocks as provisional.

> **Pitfall:** Assuming "it has a checksum" means "the data is right". A checksum computed *after* a bug produced wrong values happily confirms the wrong values. Checksums protect transport and storage; business validation protects meaning.

> **Tip:** When you copy data between systems, compare a checksum or an aggregate fingerprint at both ends, for example `COUNT(*)` plus `BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t)))` in BigQuery against the equivalent at the source. It turns silent corruption into a visible mismatch.

::: deeper
**Signed messages change the bound.** With unforgeable signatures (the "signed messages" case in the 1982 paper), agreement is possible with any number of traitors for some problem variants, because a traitor cannot claim another general said something they did not. Practical protocols still use 3f + 1 replicas because they must also cope with asynchrony: a correct but slow node is indistinguishable from a silent faulty one.

**HotStuff and linear BFT.** HotStuff (2019) restructured BFT so that the leader collects votes and broadcasts a compact certificate (a threshold signature), making each phase linear in the number of nodes and leader changes cheap. Many recent blockchain protocols descend from it.

**Mercurial cores.** Google's 2021 paper "Cores that don't count" reported CPUs that, rarely and silently, compute wrong results (a few per several thousand machines), sometimes only for specific instructions. Large fleets now screen for them. This is the most realistic Byzantine fault in a data centre, and the reason end-to-end validation of critical results is worth its cost.

**Hyperledger Fabric** shows the trade-off: its ordering service used crash-tolerant Raft for years, because members of a consortium trusted the orderers; version 3.0 added a BFT ordering option for consortia that do not.

> **Interview:** "Why does Raft not tolerate Byzantine faults?" Raft trusts what nodes say: a lying node could claim to have entries it does not, vote twice, or send different entries to different followers. Detecting that needs authenticated messages, more replicas (3f + 1) and extra rounds of cross-checking, as PBFT does.
:::

::: check
?? How many nodes do you need to tolerate 2 Byzantine faults? 2 crash faults?
Byzantine: 3f + 1 = 7. Crash: 2f + 1 = 5.

?? Why does a PBFT client wait for f + 1 matching replies rather than just one?
Up to f replicas may lie. With f + 1 identical replies, at least one comes from an honest replica, so the answer is correct.

?? Inside a single company's data platform, what is the most practical defence against Byzantine-style faults?
End-to-end integrity checks: checksums on transport and storage, Merkle-tree style comparisons between replicas, and reconciliation and validation queries between source and target, rather than a BFT consensus protocol.
:::

::: resources
- Paper: [The Byzantine Generals Problem](https://dl.acm.org/doi/10.1145/357172.357176) · Lamport, Shostak and Pease, 1982, the original problem and the 3f + 1 bound
- Paper: [Practical Byzantine Fault Tolerance](https://pdos.csail.mit.edu/6.824/papers/castro-practicalbft.pdf) · Castro and Liskov's OSDI 1999 paper
- Paper: [HotStuff: BFT Consensus in the Lens of Blockchain](https://arxiv.org/abs/1803.05069) · the linear-communication BFT protocol behind many modern chains
- Paper: [Cores that don't count](https://research.google/pubs/cores-that-dont-count/) · Google on silent CPU corruption in production fleets
- Docs: [Cloud Storage data validation and change detection](https://cloud.google.com/storage/docs/data-validation) · CRC32C and MD5 checksums and how to use them
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · its treatment of Byzantine faults and why most systems do not need BFT
:::

### Gossip Protocols & SWIM

::: plain
Gossip spreads news the way rumours spread in an office: every so often, each person picks a few colleagues at random and tells them everything they have heard. Nobody is in charge and nobody needs a list of who has been told, yet within a few rounds everyone knows. Gossip protocols use this to spread membership ("node 7 has joined", "node 3 looks dead") and state across large clusters cheaply, accepting that for a moment some nodes know more than others.
:::

**Why gossip?** Consensus gives a single, exact answer, but every decision needs a majority to reply. For information that is useful even if slightly out of date (which nodes are alive, how loaded they are, what schema version they run), that is overkill. **Gossip** (also called epidemic protocols, from Demers and colleagues' 1987 work on replicated databases at Xerox) trades exactness for scalability and robustness:

- Each node, every period (for example every second), picks one or a few random peers and exchanges state with them.
- Information spreads exponentially: after round k roughly `fanout^k` nodes know, so it reaches everyone in **O(log n)** rounds.
- Load per node is constant regardless of cluster size, and there is no single point of failure.

```python
import random

def rounds_to_spread(n, fanout, seed):
    rng = random.Random(seed)
    informed = {0}                       # node 0 learns a new fact
    rounds = 0
    while len(informed) < n:
        rounds += 1
        for node in list(informed):      # every informed node tells `fanout` random peers
            informed.update(rng.sample(range(n), fanout))
    return rounds

for n in (100, 1_000, 10_000):
    print(n, "nodes:", rounds_to_spread(n, fanout=3, seed=7), "rounds")
# 100 nodes: 7 rounds
# 1000 nodes: 8 rounds
# 10000 nodes: 10 rounds
```

A hundred times more nodes needed only three more rounds. That logarithmic growth is the whole appeal.

**Push, pull and push-pull.** In *push* gossip an informed node sends news; in *pull* a node asks a peer for news; *push-pull* does both in one exchange and converges fastest. **Anti-entropy** is gossip used to repair replicas: peers compare their data (often with Merkle trees or version vectors) and exchange whatever differs, so replicas converge even if some updates were lost. **Rumour mongering** spreads a new update eagerly for a while and then stops, which is cheaper but can leave a few nodes out, so systems combine both.

**Failure detection.** A cluster needs to know who is dead. The naive way, every node pinging every other node, costs O(n²) messages. Gossip-style heartbeats (each node increments a counter that is gossiped, and a node whose counter stops rising is suspected) scale better, but detection is slow and false alarms are common. **SWIM** (Scalable Weakly-consistent Infection-style Process Group Membership, Das, Gupta and Motivala, 2002) separates the two jobs:

1. **Failure detection by probing.** Every protocol period, a node pings one random member. If no ack arrives in time, it asks `k` other random members to ping that member on its behalf (`ping-req`). Only if none of them get an answer is the member suspected. Indirect probing avoids blaming a node for a problem on one network path.
2. **Suspicion.** A suspected member is not removed immediately; it gets a timeout during which it can refute the suspicion by gossiping that it is alive with a higher **incarnation number**. This sharply cuts false positives from brief pauses.
3. **Dissemination by piggybacking.** Membership changes (joined, suspected, dead) ride along on the ping and ack messages, so spreading news costs no extra messages.

The result: constant message load per node, detection time that does not grow with cluster size, and membership that converges in O(log n) periods.

**Where data engineers meet gossip:**

- **Cassandra and ScyllaDB:** every node gossips once per second with up to three other nodes, exchanging state (status, tokens, schema version, load). A **phi accrual failure detector** turns heartbeat arrival times into a suspicion level (phi) rather than a yes/no, and Cassandra marks a node down when phi crosses `phi_convict_threshold` (default 8).
- **HashiCorp memberlist, Serf and Consul:** SWIM with the "Lifeguard" improvements, which make a node that is itself slow less likely to accuse others.
- **Redis Cluster:** nodes exchange gossip on a separate cluster bus to agree on slot ownership and failures (with an election for failover).
- **Anti-entropy in object and NoSQL stores:** background repair between replicas is gossip in all but name.

> **Pitfall:** Treating gossip membership as authoritative. A gossip view is eventually consistent: two nodes can briefly disagree about who is alive. Do not use it to decide "exactly one owner" (that needs consensus or a lease); use it for routing hints, load information and failure suspicion.

> **Tip:** When a Cassandra or Consul cluster shows nodes "flapping" (marked down and up repeatedly), look first at long garbage collection pauses and overloaded network links on the accused nodes, and at clock or CPU starvation on the accusers, before raising the failure detector threshold.

::: deeper
**Why O(log n)?** In push gossip with fanout 1, the number of informed nodes roughly doubles each round until about half know; then the remaining uninformed nodes are found at a shrinking rate. Pittel's analysis gives about `log2(n) + ln(n)` rounds to inform everyone. Push-pull shortens the tail, because uninformed nodes actively ask.

**The phi accrual detector.** Hayashibara and colleagues (2004) model heartbeat inter-arrival times as a distribution learned from history. Phi is `-log10` of the probability that a heartbeat would be this late if the node were alive; phi = 8 means roughly a one in a hundred million chance of a false positive under that model. Applications pick a threshold that suits them rather than a fixed timeout.

**Seeds and partitions.** New nodes need at least one **seed** address to start gossiping. If seeds are chosen badly (for example all in one zone), a zone failure can split the cluster into groups that cannot rediscover each other. Use a few seeds per zone.

**Gossip and security.** Unauthenticated gossip lets one bad message poison every node's view. Production systems encrypt and authenticate gossip traffic (Consul's gossip encryption key, Cassandra's internode TLS).

> **Interview:** "How would you detect failed nodes in a 5,000-node cluster?" Explain why all-to-all heartbeats do not scale, then SWIM: random probe, indirect probes through k peers, a suspicion phase with incarnation numbers to refute, and piggybacked dissemination. Mention phi accrual as a way to tune sensitivity.
:::

::: check
?? Roughly how does the time for gossip to reach every node grow with cluster size?
Logarithmically: O(log n) rounds. Multiplying the cluster size by 100 adds only a few rounds.

?? In SWIM, why does a node ask others to ping a member (ping-req) before suspecting it?
To avoid blaming the member for a problem on one network path or a temporarily slow prober. If several other nodes also cannot reach it, the suspicion is far more likely to be real.

?? Why is gossip-based membership unsuitable for deciding which single node owns a lock?
It is only eventually consistent: different nodes can briefly hold different views, so two nodes could both believe they are the owner. Exclusive ownership needs consensus, a lease or compare-and-set.
:::

::: resources
- Paper: [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) · the original SWIM paper
- Paper: [Epidemic Algorithms for Replicated Database Maintenance](https://dl.acm.org/doi/10.1145/41840.41841) · Demers and colleagues, the 1987 paper that started gossip and anti-entropy
- Docs: [Cassandra architecture: dynamo, gossip and failure detection](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html) · how Cassandra uses gossip, phi accrual and repair
- Blog: [SWIM: The scalable membership protocol](https://www.brianstorti.com/swim/) · a clear illustrated explanation of probing and suspicion
- Paper: [Lifeguard: Local Health Awareness for More Accurate Failure Detection](https://arxiv.org/abs/1707.00788) · HashiCorp's improvements to SWIM used in Consul
- Book: [Cassandra: The Definitive Guide, 3rd edition](https://www.oreilly.com/library/view/cassandra-the-definitive/9781492097136/) · gossip, snitches and failure detection in a production database
:::

## Building reliable systems

The first two groups were about agreement inside one system. This group is about building whole services and pipelines that keep working when the parts they depend on are slow, overloaded or broken. It starts with transactions that span several systems (two-phase commit), then the defensive patterns every service needs (timeouts, retries, circuit breakers, bulkheads), and the controls that stop one fast producer from drowning a slow consumer (rate limiting and backpressure).

The last three topics are about how data moves and how you know it is healthy: gRPC and Arrow Flight for efficient data transfer, observability for seeing inside a distributed request, and SLOs, which turn "the pipeline should be reliable" into numbers that a team can promise, measure and spend.

### Distributed Transactions — 2PC, Saga

::: plain
A distributed transaction is a change that must happen in several systems at once, or not at all: take money from one bank, add it to another. Two-phase commit (2PC) works like a wedding ceremony: the officiant first asks each partner "do you?", and only if both say yes declares them married; if either hesitates, nothing happens. Its weakness is that once everyone has said "I do", they must wait for the officiant's final word, and if the officiant faints at that moment, everyone is stuck at the altar. Sagas avoid the wait by doing each step separately and undoing earlier steps if a later one fails.
:::

**The problem.** A single database gives you ACID transactions: either all changes of a transaction become visible or none do. When one business action touches several independent systems (an orders database, a payments service and a stock database), no single system can guarantee that. If the order is saved but the payment fails, someone must clean up. Two families of solution exist: **atomic commit protocols** (2PC), which make several systems commit together, and **sagas**, which accept separate commits and compensate on failure.

**Two-phase commit.** One node, the **coordinator** (often the application's transaction manager), drives a set of **participants** (databases, brokers):

1. **Prepare (voting) phase.** The coordinator asks every participant to prepare. Each participant does all the work, writes it durably together with a "prepared" record, keeps its locks, and votes yes or no. A yes vote is a promise: "I can commit this no matter what, even if I crash and restart." After voting yes, the participant may no longer decide on its own.
2. **Commit phase.** If all voted yes, the coordinator writes **commit** to its own durable log (this moment is the **commit point**) and tells everyone to commit. If any voted no or did not answer, it decides **abort** and tells everyone to roll back. Participants acknowledge, and the coordinator can forget the transaction.

```python
class Participant:
    def __init__(self, name, can_commit=True):
        self.name, self.can_commit, self.state = name, can_commit, "idle"
    def prepare(self):
        # A real participant writes its changes and a PREPARED record to disk here,
        # holding locks. After voting yes it may no longer decide on its own.
        self.state = "prepared" if self.can_commit else "aborted"
        return self.can_commit
    def finish(self, decision):
        if self.state == "prepared":
            self.state = decision

def two_phase_commit(participants, log):
    votes = [p.prepare() for p in participants]          # phase 1: prepare / vote
    decision = "committed" if all(votes) else "aborted"
    log.append(decision)                                  # the commit point: durable decision
    for p in participants:                                # phase 2: tell everyone
        p.finish(decision)
    return decision

log = []
ps = [Participant("orders_db"), Participant("payments_db"), Participant("stock_db", can_commit=False)]
print(two_phase_commit(ps, log))                 # aborted
print([(p.name, p.state) for p in ps])
# [('orders_db', 'aborted'), ('payments_db', 'aborted'), ('stock_db', 'aborted')]
```

**Why 2PC blocks.** If the coordinator crashes after participants voted yes but before they learn the decision, they are **in doubt**: they cannot commit (others may have voted no) and cannot abort (the coordinator may have decided commit). They must keep their locks and wait until the coordinator recovers and reads its log. Meanwhile other transactions that need those rows wait too. This is the central weakness of 2PC: it is **safe but blocking**, and the coordinator is a single point of failure.

**Three-phase commit** (Skeen, 1981) adds a "pre-commit" phase so that participants can finish on their own after a coordinator failure. It only works if the network has bounded delays and no partitions, which real networks do not, so 3PC is studied far more than it is used.

**Where 2PC is used well.** The blocking problem disappears if the coordinator itself is replicated with consensus. That is what **Spanner** does: each participant is a Paxos group, the coordinator is the leader of one of them, and its decision is replicated before it is announced, so a single machine failure never leaves anyone in doubt. **Kafka transactions** are also 2PC-like: a transaction coordinator (replicated through Kafka's own log) writes the decision and then writes commit or abort markers into every partition the transaction touched; consumers with `isolation.level=read_committed` skip aborted data. PostgreSQL exposes the participant side directly:

```sql
-- PostgreSQL participant side (requires max_prepared_transactions > 0)
BEGIN;
UPDATE stock SET qty = qty - 1 WHERE sku = 'A-42';
PREPARE TRANSACTION 'order-981';   -- phase 1: durable, locks held, survives restart
-- ... later, when the coordinator decides:
COMMIT PREPARED 'order-981';       -- or ROLLBACK PREPARED 'order-981'
```

**XA** is the standard interface that lets one transaction manager drive 2PC across different databases and message brokers. It works, but it couples every participant's availability and latency to the slowest one, and many modern services (most HTTP APIs, Pub/Sub, Cloud Storage) do not support it at all.

**Sagas.** When participants are independent services, the usual answer is a **saga**: a sequence of local transactions, each committed immediately, where every step has a **compensating** action that semantically undoes it (refund a payment, release a reservation). If step 3 fails, the saga runs the compensations for steps 2 and 1. Sagas never block and never hold locks across services, but they give up isolation: other readers can see the intermediate states. The section **Event-driven architecture** covers sagas in depth (orchestration versus choreography, compensation design and tools such as Temporal and Workflows); here the important point is when to choose which:

| | Two-phase commit | Saga |
|---|---|---|
| Atomicity | All or nothing at commit time | Eventually all or compensated |
| Isolation | Yes: locks hide partial state | No: partial states are visible |
| Availability | Every participant must be up to commit | Each step needs only its own service |
| Latency | At least two round trips, locks held throughout | Each step commits immediately |
| Fits | Several databases under one operator; Spanner; Kafka | Independent services and external APIs |

**Distributed transactions in data pipelines.** Data engineers mostly avoid 2PC by design: they make each write idempotent and **atomic per table**, and publish results with a single atomic switch. Examples: an Iceberg or Delta commit replaces the table's metadata pointer in one compare-and-set, so readers see all of a batch or none; BigQuery jobs are atomic per destination table, and multi-statement transactions (`BEGIN TRANSACTION; ... COMMIT TRANSACTION;`) can change several BigQuery tables atomically; the write-audit-publish pattern writes to a staging table, checks it, then swaps it in.

> **Pitfall:** Writing to a database and then publishing an event as two separate steps ("dual write"). If the process dies between them, the two disagree forever. Use the transactional outbox pattern (covered in **Event-driven architecture**) or change data capture, which turn the second write into a consequence of the first.

> **Tip:** Monitor in-doubt transactions. In PostgreSQL, `SELECT * FROM pg_prepared_xacts` lists prepared transactions; an old one holds locks and blocks vacuum until someone commits or rolls it back.

::: deeper
**Percolator.** Google's Percolator (2010), which built incremental web indexing on Bigtable, implemented cross-row 2PC on a store without transactions: a timestamp oracle hands out start and commit timestamps, and each transaction picks one cell as the **primary lock**. Committing the primary is the commit point; other cells are cleaned up lazily by whoever finds a stale lock and checks the primary. TiDB's transaction model is directly based on it.

**Deterministic databases.** Calvin (2012) and its descendants avoid 2PC's in-doubt state by agreeing on the *order* of transactions first, through consensus, and then having every participant execute them deterministically. If all replicas run the same transactions in the same order, they reach the same result without a commit vote.

**Exactly-once across systems.** Flink's two-phase commit sink (pre-commit on checkpoint, commit when the checkpoint completes) and Spark Structured Streaming's idempotent sinks are both ways to get end-to-end exactly-once into an external system. Both need the sink to support either transactions or idempotent writes keyed by batch or checkpoint id.

> **Interview:** "Why not just use 2PC between microservices?" Each service would hold locks while waiting for the slowest one, every service's outage would block the others, the coordinator becomes a single point of failure, and many APIs cannot participate at all. Sagas plus idempotent steps keep services independent, at the cost of visible intermediate states that the business must accept.
:::

::: check
?? What does a participant promise when it votes yes in 2PC?
That it has made the changes durable and can commit them whatever happens, including after a crash. After a yes vote it must wait for the coordinator's decision and cannot abort on its own.

?? Why is 2PC called a blocking protocol?
If the coordinator fails after participants voted yes, they cannot decide alone, so they keep locks and wait until it recovers.

?? How does Spanner make 2PC tolerable?
Each participant and the coordinator are Paxos-replicated groups, so the coordinator's decision survives machine failures and participants are not left in doubt by a single crash.
:::

::: resources
- Book: [Designing Data-Intensive Applications, 2nd edition](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/) · atomic commit, 2PC and its limits explained step by step
- Docs: [PostgreSQL PREPARE TRANSACTION](https://www.postgresql.org/docs/current/sql-prepare-transaction.html) · the participant side of 2PC in a real database
- Paper: [Large-scale Incremental Processing Using Distributed Transactions and Notifications (Percolator)](https://research.google/pubs/large-scale-incremental-processing-using-distributed-transactions-and-notifications/) · cross-row 2PC on Bigtable
- Docs: [Spanner transactions overview](https://cloud.google.com/spanner/docs/transactions) · read-write transactions, locking and commit behaviour in Spanner
- Article: [Pattern: Saga](https://microservices.io/patterns/data/saga.html) · Chris Richardson's description of sagas as the alternative to 2PC
- Docs: [BigQuery multi-statement transactions](https://cloud.google.com/bigquery/docs/transactions) · atomic changes across several BigQuery tables
:::

### Fault Tolerance Patterns

::: plain
When the shop you depend on is closed, you do not stand at the door knocking all day. You wait a bit, try again a few times with longer gaps, and if it is clearly shut you stop trying for a while and buy elsewhere. Fault tolerance patterns teach software the same manners: **timeouts** so it never waits forever, **retries with backoff** for brief hiccups, **circuit breakers** to stop hammering something that is down, **bulkheads** so one problem cannot sink the whole ship, and **fallbacks** for a sensible answer when the real one is unavailable.
:::

Every call across a network can fail or hang. A service that calls three dependencies is only as reliable as its handling of their failures, and the most dangerous failure is not an error but **slowness**: a dependency that takes 30 seconds to answer ties up threads, connections and memory in every caller, which then become slow themselves. This chain reaction is a **cascading failure**, the most common way a small problem becomes an outage. Michael Nygard's book *Release It!* named the stability patterns below.

**1. Timeouts and deadlines.** Never make a network call without a timeout. Choose it from the dependency's latency distribution (for example a little above its 99th percentile), not from a round number. Better still, propagate a **deadline**: the caller says "I need an answer within 2 seconds in total", and each hop passes on the remaining time, so nobody keeps working on a request whose caller has already given up. gRPC has deadlines built in.

**2. Retries with exponential backoff and jitter.** Many failures are transient (a dropped connection, a leader election, a `429 Too Many Requests` or `503 Unavailable`). Retrying fixes them, but naive retries make overload worse: if a thousand clients fail at the same moment and all retry after exactly one second, they hit the recovering service together. So:

- Retry only errors that are retryable (timeouts, 429, 503, `UNAVAILABLE`), never `400 Bad Request` or permission errors.
- Retry only **idempotent** operations, or make them idempotent with a request id.
- Wait **exponentially longer** between attempts, up to a cap, and add **jitter** (randomness) so clients spread out.
- Limit attempts and total time.

```python
import random

def backoff_delay(attempt, base=0.5, cap=30.0):
    """Exponential backoff with full jitter: a random sleep between 0 and the ceiling."""
    ceiling = min(cap, base * 2 ** attempt)
    return random.uniform(0, ceiling)

print([min(30.0, 0.5 * 2 ** a) for a in range(8)])
# [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]  the ceilings; actual sleeps are random below them
```

Google Cloud client libraries already retry idempotent calls with exponential backoff (Cloud Storage's retry strategy page lists which operations are retried and when). Pub/Sub subscriptions have a **retry policy** with a minimum and maximum backoff and an optional **dead-letter topic** after a maximum number of delivery attempts, and Cloud Tasks, Workflows and Managed Service for Apache Airflow (formerly Cloud Composer) tasks all have retry settings. Configure them rather than writing loops.

**3. Circuit breakers.** A circuit breaker wraps calls to a dependency and tracks failures. While things are fine it is **closed** (calls pass through). After too many failures it **opens**: calls fail immediately without touching the dependency, which protects both sides. After a cool-down it becomes **half-open** and lets a trial call through; success closes it, failure opens it again.

```python
import time

class CircuitBreaker:
    def __init__(self, threshold=3, reset_after=30.0, clock=time.monotonic):
        self.threshold, self.reset_after, self.clock = threshold, reset_after, clock
        self.failures, self.opened_at, self.state = 0, None, "closed"

    def call(self, fn):
        if self.state == "open":
            if self.clock() - self.opened_at < self.reset_after:
                raise RuntimeError("circuit open: failing fast")
            self.state = "half-open"                 # let one trial call through
        try:
            result = fn()
        except Exception:
            self.failures += 1
            if self.state == "half-open" or self.failures >= self.threshold:
                self.state, self.opened_at = "open", self.clock()
            raise
        self.failures, self.state = 0, "closed"      # success closes the circuit
        return result

now = [0.0]
cb = CircuitBreaker(threshold=3, reset_after=30, clock=lambda: now[0])
def broken(): raise ConnectionError("downstream is down")

for _ in range(4):
    try:
        cb.call(broken)
    except Exception as e:
        print(cb.state, "-", e)
# closed - downstream is down
# closed - downstream is down
# open - downstream is down
# open - circuit open: failing fast
now[0] = 31.0                                        # 31 seconds later
print(cb.call(lambda: "ok"), cb.state)               # ok closed
```

**4. Bulkheads.** Ships are divided into watertight compartments so one leak does not sink them. In software, give each dependency its own limited pool of threads, connections or concurrency, so a slow reporting database can exhaust only its own pool, not the threads that serve checkout. In data platforms the same idea appears as separate BigQuery reservations for ETL and for dashboards, separate Kafka consumer groups per use case, separate Cloud Run services per workload, and Airflow **pools** that cap how many tasks may hit one source at once.

**5. Fallbacks and graceful degradation.** When a call fails, return something useful: a cached value, a default, a partial result with a warning, or a queued request to process later. A recommendations panel can show best-sellers when the model service is down. Be honest in data products: a dashboard should say "data delayed since 06:00" rather than silently show yesterday's numbers.

**6. Load shedding.** When a service is overloaded, rejecting some requests quickly (with 429 or 503) keeps it healthy for the rest; queuing everything makes every request slow and eventually times them all out.

> **Pitfall:** Retrying at every layer. If the API retries 3 times, its client retries 3 times, and the job runner retries the task 3 times, one failing call becomes 27 attempts that arrive in bursts. Retry at one layer (usually the outermost one that knows the operation is idempotent) and use a **retry budget**, for example retries may add at most 10% extra load.

> **Tip:** Make every pipeline step idempotent before you add retries: `MERGE` on a business key instead of `INSERT`, overwrite a partition instead of appending, write files with deterministic names. Then a retry, a re-run or a duplicate delivery is harmless.

::: deeper
**Hedged requests and the tail at scale.** Dean and Barroso's "The Tail at Scale" (2013) showed that when a request fans out to 100 servers, the slowest one decides the latency, so even rare slowness becomes common. A **hedged request** sends a second copy to another replica if the first has not answered within, say, the 95th percentile latency, and uses whichever reply comes first. It costs a few percent extra load and cuts tail latency sharply. Spark's **speculative execution** (`spark.speculation`) is the batch version: re-launch straggling tasks elsewhere.

**Retry amplification and metastable failures.** A system can enter a state where retries alone keep it overloaded even after the original trigger is gone: a **metastable failure**. Breaking the loop needs load shedding, retry budgets or circuit breakers, not just more capacity. Google's SRE book chapter on cascading failures describes these dynamics and remedies in detail.

**Libraries.** Python's `tenacity` provides decorators for retry with backoff and jitter; Java's Resilience4j provides circuit breakers, bulkheads, rate limiters and retries; service meshes (Istio, Cloud Service Mesh) can apply timeouts, retries and outlier detection (a form of circuit breaking) without code changes. Google's `google-api-core` library is what gives Google Cloud Python clients their default retry behaviour.

> **Interview:** "An upstream API sometimes takes 60 seconds and your pipeline backs up. What do you change?" Add a timeout below the job's own deadline; retry idempotent calls with capped exponential backoff and jitter; wrap the API in a circuit breaker; cap concurrency to it with a bulkhead (a pool); and send failed records to a dead-letter table for replay instead of failing the whole run.
:::

::: check
?? Why add jitter to exponential backoff?
Without it, clients that failed together retry together, producing synchronised waves of load that can knock the recovering service over again. Randomness spreads the retries out.

?? What are the three states of a circuit breaker, and what happens in each?
Closed: calls pass through and failures are counted. Open: calls fail immediately without reaching the dependency. Half-open: after a cool-down, a trial call is allowed; success closes the circuit, failure opens it again.

?? Which kinds of errors should not be retried?
Errors that will not change on retry, such as 400 Bad Request, 401 and 403 permission errors and validation failures, and any non-idempotent operation that might already have succeeded, unless it carries an idempotency key.
:::

::: resources
- Book: [Release It!, 2nd edition](https://pragprog.com/titles/mnee2/release-it-second-edition/) · Michael Nygard; the original catalogue of stability patterns and anti-patterns
- Article: [Circuit Breaker](https://martinfowler.com/bliki/CircuitBreaker.html) · Martin Fowler's concise explanation with code
- Book: [Addressing Cascading Failures (Site Reliability Engineering)](https://sre.google/sre-book/addressing-cascading-failures/) · Google's chapter on overload, retries and load shedding
- Docs: [Cloud Storage retry strategy](https://cloud.google.com/storage/docs/retry-strategy) · which operations Google's clients retry, with exponential backoff
- Paper: [The Tail at Scale](https://research.google/pubs/the-tail-at-scale/) · Dean and Barroso on tail latency and hedged requests
- Docs: [Tenacity](https://tenacity.readthedocs.io/) · the standard Python library for retries with backoff, jitter and stop conditions
:::

### Rate Limiting & Backpressure at Scale

::: plain
A rate limiter is the bouncer at a club door: it lets people in at a steady pace, allows a small group to rush in at once, and turns the rest away politely so the club never becomes dangerously crowded. Backpressure is the club telling the queue outside to slow down because the bar is full. Rate limiting protects a service from too many requests; backpressure lets a slow consumer tell a fast producer to ease off, instead of drowning in a backlog.
:::

**Why limit?** To protect a service from overload (including by one misbehaving client), to share capacity fairly between customers, to respect someone else's limits (a SaaS API that allows 100 calls per minute), and to control cost. Data engineers meet rate limits on both sides: they call quota-limited APIs during ingestion, and they build data APIs that need protecting.

**The main algorithms.**

| Algorithm | How it works | Bursts | Memory per client |
|---|---|---|---|
| Fixed window counter | Count requests per calendar window (e.g. per minute); reset at the boundary | Up to 2x the limit across a boundary | One counter |
| Sliding window log | Store a timestamp per request; count those in the last window | Exact | One entry per request |
| Sliding window counter | Weight the previous window's count by its overlap with the sliding window | Approximate, smooth | Two counters |
| Token bucket | Tokens refill at rate r up to capacity b; each request takes one | Allows bursts up to b | Two numbers (tokens, last refill time) |
| Leaky bucket (as a queue) | Requests enter a queue that drains at a fixed rate | No bursts in output | A queue |

**Token bucket** is the most widely used, because it allows short bursts while enforcing an average rate:

```python
class TokenBucket:
    """Allows bursts up to `capacity`, refills at `rate` tokens per second."""
    def __init__(self, rate, capacity):
        self.rate, self.capacity = rate, capacity
        self.tokens, self.last = capacity, 0.0
    def allow(self, now):
        self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False

bucket = TokenBucket(rate=2, capacity=5)                 # 2 requests/s, bursts of 5
burst = [bucket.allow(0.0) for _ in range(7)]            # 7 requests at the same instant
print(burst)          # [True, True, True, True, True, False, False]
print(bucket.allow(0.5), bucket.allow(0.5))              # True False: 0.5 s refilled 1 token
print(bucket.allow(3.0))                                 # True: refilled, capped at 5
```

Note that the bucket stores no timer: it computes the refill lazily from the elapsed time whenever a request arrives. The **leaky bucket** used as a queue does the opposite: it smooths output to a constant rate, which suits a worker calling a fragile downstream system. (Used as a meter, a leaky bucket is equivalent to a token bucket.)

The **sliding window counter** is a cheap, smooth approximation used by many API gateways:

```python
def sliding_window_estimate(prev_count, curr_count, window_s, elapsed_in_curr_s):
    """Sliding window counter: weight the previous fixed window by how much of it still overlaps."""
    overlap = (window_s - elapsed_in_curr_s) / window_s
    return prev_count * overlap + curr_count

# limit 100 per minute; 84 requests last minute, 36 so far in this one, 15 s into it
est = sliding_window_estimate(84, 36, 60, 15)
print(est, "allowed" if est < 100 else "rejected")   # 99.0 allowed
```

**Distributed rate limiting.** With twenty API servers, each needs to see the same counts. Options:

- **A shared counter store**, typically Redis (on Google Cloud, Memorystore for Redis or Valkey). An atomic `INCR` on a key per client per window plus `EXPIRE` gives a fixed window; a small Lua script that reads, refills and decrements atomically gives a token bucket. It costs one round trip per request.
- **Local limits with a share of the global budget**: each of N servers allows `limit / N`, adjusted periodically. No round trip, less exact.
- **The gateway or edge**: let infrastructure do it. On Google Cloud, **Apigee** offers a SpikeArrest policy (smooths traffic to a rate) and a Quota policy (counts calls per developer or app over hours or days), and **Cloud Armor** rate-limiting rules can throttle or temporarily ban clients by IP or header in front of a load balancer.

**Gateway versus internal limits.** Edge limits protect against abusive or buggy *external* clients, keyed by API key or IP. Internal limits protect *each service* from its callers, including other internal services and batch jobs, keyed by caller identity and operation cost. You need both: an edge limit cannot stop your own nightly backfill from overwhelming the orders database.

**Respond properly.** Return `429 Too Many Requests` with a `Retry-After` header, so well-behaved clients back off for the right time. As a client, honour `Retry-After` and quota headers instead of guessing.

**Backpressure.** Rate limiting rejects excess work; **backpressure** slows the producer down instead. It is essential in streaming, where producer and consumer run continuously and any lasting speed difference grows a backlog without bound (Little's law: items in the system = arrival rate x time in system, so if arrivals exceed throughput, the queue and the latency grow forever).

- **Pull-based systems have it built in.** Kafka consumers and Pub/Sub pull subscribers fetch only what they can handle. Pub/Sub's client libraries add **flow control** (a maximum of outstanding messages and bytes per subscriber), so a slow subscriber stops pulling instead of running out of memory.
- **Stream processors propagate it.** Flink uses credit-based flow control between operators: a slow sink stops granting credits, the operators upstream slow down, and eventually the source reads more slowly from Kafka. Dataflow measures backlog and autoscales workers instead.
- **Batch readers limit intake.** Spark Structured Streaming's `maxOffsetsPerTrigger` caps how much each micro-batch reads from Kafka, so a big backlog after an outage is processed in digestible pieces.
- **Buffers are not a solution.** An unbounded in-memory queue between a fast producer and a slow consumer only delays the failure until memory runs out. Bound every queue and decide what happens when it is full: block, shed or spill to durable storage.

> **Pitfall:** Ingesting from a rate-limited SaaS API with many parallel workers that each obey the limit separately. Ten workers at "100 per minute" make 1,000 per minute, and the vendor blocks your key. Share one limiter across workers (an Airflow pool, a Redis bucket, or a single Cloud Tasks queue with a dispatch rate) and honour `Retry-After`.

> **Tip:** Cloud Tasks queues have `maxDispatchesPerSecond` and `maxConcurrentDispatches` settings, which make them a simple managed rate limiter for calling an external API from Cloud Run: enqueue one task per call and let the queue pace them.

::: deeper
**GCRA.** The generic cell rate algorithm stores a single "theoretical arrival time" per client and is equivalent to a token bucket with less state; the `redis-cell` module and several gateways implement it.

**Cost-based limits.** Not all requests cost the same: a query scanning a year of data is not one "request" like a key lookup. Stripe's engineering blog describes layering a request rate limiter, a concurrency limiter (how many requests in flight at once), and load shedders that reserve capacity for critical traffic. Concurrency limits are often better than rate limits for expensive operations, because they adapt automatically to how long each request takes.

**Adaptive concurrency.** Netflix's concurrency-limits library and similar tools adjust a client's allowed in-flight requests from observed latency, like TCP congestion control: increase slowly while latency is stable, cut sharply when it rises. It finds the right limit without hand-tuning.

**Standard headers.** The IETF HTTP API working group is standardising `RateLimit` and `RateLimit-Policy` response headers (still a draft); many APIs already send similar `X-RateLimit-*` headers.

> **Interview:** "Design a rate limiter for a public API with 50 servers." Cover the algorithm (token bucket for burst tolerance), where the state lives (Redis with an atomic script, or approximate local buckets), keys (API key plus operation), the response (429 with `Retry-After`), what happens if Redis is down (fail open with local limits rather than fail closed), and edge protection (Apigee or Cloud Armor) in front.
:::

::: check
?? With a token bucket of rate 10 per second and capacity 50, how many requests can arrive at once after a quiet minute, and what is the long-run rate?
50 at once (the bucket is full), then 10 per second on average.

?? Why can a fixed window limiter of 100 per minute let through 200 requests in a few seconds?
A client can send 100 just before the window boundary and 100 just after it; both windows are within their limit.

?? What is the difference between rate limiting and backpressure?
Rate limiting rejects or delays requests beyond a set rate to protect a service. Backpressure is a feedback signal from a slow consumer that makes the producer slow down, so work is delayed rather than lost and queues stay bounded.
:::

::: resources
- Blog: [Scaling your API with rate limiters](https://stripe.com/blog/rate-limiters) · Stripe's four kinds of limiters, with code
- Docs: [Apigee SpikeArrest policy](https://cloud.google.com/apigee/docs/api-platform/reference/policies/spike-arrest-policy) · smoothing traffic at the gateway; see also the Quota policy
- Docs: [Cloud Armor rate limiting overview](https://cloud.google.com/armor/docs/rate-limiting-overview) · throttle and rate-based ban rules in front of load balancers
- Docs: [Pub/Sub flow control](https://cloud.google.com/pubsub/docs/flow-control-messages) · how subscribers bound outstanding messages and bytes
- Docs: [Redis INCR: rate limiter pattern](https://redis.io/docs/latest/commands/incr/) · fixed-window rate limiting with atomic counters
- Book: [System Design Interview, Volume 1](https://bytebytego.com/courses/system-design-interview/design-a-rate-limiter) · Alex Xu's rate limiter chapter, comparing all five algorithms
:::

### gRPC & Apache Arrow Flight

::: plain
When two programs talk over a network, they need a shared language and an efficient way to post messages. **gRPC** is like a well-run courier service with a fixed form for every kind of request: both sides agree on the form in advance (a Protocol Buffers schema), messages are packed compactly, and many conversations share one connection. **Arrow Flight** is a courier specialised in moving huge tables: instead of turning every row into a separate parcel, it ships whole blocks of columns in the same layout the receiving program uses in memory, so nothing has to be unpacked.
:::

**Why not just JSON over HTTP?** For a few requests per second, REST with JSON is simple and fine. For service-to-service calls at volume, or for moving millions of rows, it wastes effort: JSON is verbose text, every number is parsed from characters, field names repeat in every record, and HTTP/1.1 needs a connection per concurrent request. gRPC and Flight address the two cases: many small calls, and bulk data.

**gRPC** (open-sourced by Google in 2015, now a CNCF project) combines:

- **Protocol Buffers** (Protobuf): an interface definition language and a compact binary encoding. You define messages and services in a `.proto` file and generate client and server code for many languages.
- **HTTP/2** as transport: many concurrent calls (streams) multiplexed over one TCP connection, header compression, and per-stream flow control (which gives you backpressure for free).
- **Four kinds of call:** unary (one request, one response), server streaming, client streaming, and bidirectional streaming.
- **Deadlines, cancellation, status codes and metadata** built in: `DEADLINE_EXCEEDED`, `UNAVAILABLE` (retryable), `RESOURCE_EXHAUSTED` (quota) and so on.

```text
// orders.proto
syntax = "proto3";
package shop.v1;

message GetOrderRequest { string order_id = 1; }
message Order {
  string order_id  = 1;
  string customer  = 2;
  int64  amount_cents = 3;
  repeated string skus = 4;
}
message ExportRequest { string since_date = 1; }

service Orders {
  rpc GetOrder(GetOrderRequest) returns (Order);          // unary
  rpc ExportOrders(ExportRequest) returns (stream Order); // server streaming
}
```

The numbers (`= 1`, `= 2`) are **field tags**, and they, not the names, identify fields on the wire. That is the basis of Protobuf's compatibility rules: you may add new fields with new tags, and old readers skip unknown fields; you must never reuse or renumber a tag, and you should mark removed tags as `reserved`.

**gRPC in Google Cloud data services.** Many Google Cloud APIs are gRPC first. The **BigQuery Storage Read API** streams table data over gRPC in Arrow or Avro format, splitting a table into several streams that workers read in parallel; Spark, Dataflow, the BigQuery Python client (`to_dataframe`) and many BI tools use it for fast reads. The **Storage Write API** ingests rows as Protobuf messages over a bidirectional stream. Pub/Sub's high-throughput clients use streaming pull over gRPC, and Spark Connect uses gRPC between client and Spark server.

```python
# Read a BigQuery table in parallel Arrow streams (pip install google-cloud-bigquery-storage pyarrow)
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types

client = bigquery_storage_v1.BigQueryReadClient()
session = client.create_read_session(
    parent="projects/my-project",
    read_session=types.ReadSession(
        table="projects/bigquery-public-data/datasets/usa_names/tables/usa_1910_current",
        data_format=types.DataFormat.ARROW,
        read_options=types.ReadSession.TableReadOptions(
            selected_fields=["name", "number", "state"],
            row_restriction='state = "WA"',
        ),
    ),
    max_stream_count=4,                      # up to 4 streams that can be read in parallel
)
reader = client.read_rows(session.streams[0].name)
table = reader.to_arrow(session)             # an Arrow table for this stream
```

**Apache Arrow** is a standard columnar *in-memory* format: a column of int64 values is a contiguous buffer of 8-byte integers plus a validity bitmap for nulls. Because pandas, Polars, DuckDB, Spark (for Python UDFs), BigQuery's Storage API and many others can read Arrow buffers directly, data can pass between them with little or no conversion. The **Arrow IPC format** is the same layout serialised as a stream of record batches.

**Arrow Flight** is a gRPC service framework for sending Arrow record batches. Its main calls:

- `GetFlightInfo(descriptor)`: "what would this query or dataset return?" The server answers with a schema and a list of **endpoints**, each with a **ticket** and one or more locations.
- `DoGet(ticket)`: stream the record batches for one ticket. Because a FlightInfo can list several endpoints on different servers, a client can fetch them **in parallel**, which is how Flight scales beyond one server.
- `DoPut` uploads batches, `DoExchange` streams both ways, and `DoAction` runs custom commands.

```python
# Arrow Flight client sketch (pip install pyarrow); assumes a Flight server on localhost:8815
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
descriptor = flight.FlightDescriptor.for_command(b"SELECT * FROM trips WHERE day = '2026-11-01'")
info = client.get_flight_info(descriptor)
tables = [client.do_get(ep.ticket).read_all() for ep in info.endpoints]   # could run in parallel
print(info.schema)
```

**Flight SQL** builds a database protocol on Flight: standard commands for executing queries, prepared statements and fetching catalog metadata, so that one client driver works against any Flight SQL server. Dremio, InfluxDB 3 and several other engines speak it. **ADBC** (Arrow Database Connectivity) is the client-side API: like JDBC or ODBC, but returning Arrow data, with drivers for Flight SQL, PostgreSQL, SQLite, Snowflake, DuckDB and others.

| | REST + JSON | gRPC + Protobuf | Arrow Flight |
|---|---|---|---|
| Best for | Public APIs, browsers, simple integrations | Service-to-service calls, streaming events | Moving large tables between engines |
| Encoding | Text, row by row | Binary, message by message | Binary columnar batches |
| Schema | Optional (OpenAPI) | Required (`.proto`) | Arrow schema per stream |
| Parallel bulk reads | Manual pagination | Manual | Built in (multiple endpoints) |
| Browser support | Native | Needs gRPC-Web or a proxy | No |

> **Pitfall:** Changing a Protobuf field's type or tag number, or reusing a deleted tag. Old and new services then decode each other's bytes as the wrong field, silently. Add new fields, deprecate old ones, and `reserved` their tags and names.

> **Tip:** gRPC implementations limit received message size by default (4 MB in most of them). Stream large results as many messages rather than raising the limit, and set a deadline on every call.

::: deeper
**Why Arrow is fast to move.** Serialising rows to JSON or even to Protobuf means visiting every value; Arrow batches are already laid out as buffers, so sending them is close to copying memory to a socket, and the receiver can use the buffers in place (zero-copy) without parsing. The cost is that Arrow is not compact on disk (no heavy encoding or compression by default); that is Parquet's job. Arrow is for memory and the wire, Parquet for storage.

**Load balancing gRPC.** Because gRPC keeps one long-lived HTTP/2 connection, a classic layer 4 (TCP) load balancer sends all calls from one client to one server. Use client-side load balancing, a layer 7 proxy (Envoy, Google Cloud's Application Load Balancer), or a service mesh to spread calls per request.

**Compatibility checks in CI.** Tools such as Buf lint `.proto` files and fail a build on breaking changes (removed fields, changed types), which is the gRPC equivalent of a schema registry's compatibility check.

**Spark Connect** separates a thin client from the Spark driver: the client sends logical plans as Protobuf over gRPC, and results come back as Arrow batches. It is a good example of both technologies working together.

> **Interview:** "Why would a data platform use Arrow Flight instead of JDBC?" JDBC and ODBC move rows one at a time and convert each value between the database format and the driver's types, which dominates the cost for large results. Flight sends columnar Arrow batches that analytics tools use directly, and can fetch from several endpoints in parallel.
:::

::: check
?? Why can an old Protobuf reader handle a message from a newer writer that added a field?
Fields are identified by tag numbers, and readers skip tags they do not know. As long as existing tags keep their meaning, adding a field is backward and forward compatible.

?? What makes Arrow Flight faster than row-based protocols for large result sets?
It sends columnar Arrow record batches that need no per-value parsing or conversion, the receiver can use them in memory directly, and data can be fetched from several endpoints in parallel.

?? Which BigQuery API uses gRPC to stream table data in Arrow format?
The BigQuery Storage Read API.
:::

::: resources
- Docs: [gRPC documentation](https://grpc.io/docs/) · core concepts, the four call types, deadlines and language guides
- Docs: [Protocol Buffers language guide (proto3)](https://protobuf.dev/programming-guides/proto3/) · syntax, encoding and the rules for updating messages safely
- Docs: [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html) · the Flight protocol specification
- Docs: [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) · the SQL database protocol on top of Flight
- Docs: [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage) · read sessions, parallel streams, Arrow and Avro formats
- Blog: [Introducing Apache Arrow Flight: A Framework for Fast Data Transport](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) · the Arrow project's explanation of why Flight exists
:::

### Observability in Distributed Systems

::: plain
When a parcel goes missing, you want a tracking number that shows every depot it passed through and how long it sat in each. Observability gives software the same thing. **Logs** are the depot diaries (what happened, in detail), **metrics** are the dashboard of counts and timings (how many parcels, how fast), and **traces** follow one parcel's whole journey across every depot. With all three, linked by the same tracking number, you can answer questions about failures you never predicted.
:::

**Monitoring versus observability.** Monitoring answers questions you thought of in advance ("is CPU above 90%?"). Observability is the ability to answer *new* questions about a system from its outputs ("why are only Tuesday's orders from Brazil slow?"). In a distributed system, where one request crosses many services and one pipeline crosses many jobs, you need data that can be sliced by any dimension and joined across services.

**The three signals.**

- **Metrics:** numbers aggregated over time, cheap to store and fast to query. Counters (requests served, rows loaded), gauges (queue depth, consumer lag) and histograms or distributions (latency). Use them for dashboards and alerts. The **RED** method for services (Rate, Errors, Duration) and the **USE** method for resources (Utilisation, Saturation, Errors) say which ones to collect.
- **Logs:** timestamped records of events, best as **structured** JSON with consistent fields (`job_id`, `run_id`, `table`, `trace_id`) rather than free text, so you can filter and aggregate them.
- **Traces:** a **trace** is the tree of work done for one request; each unit of work is a **span** with a name, start and end time, attributes and a parent. Traces show where time went and which service failed.

**OpenTelemetry** (OTel, a CNCF project that merged OpenTracing and OpenCensus) is now the standard way to produce all three: one set of APIs and SDKs in every major language, automatic instrumentation for common libraries (HTTP, gRPC, database drivers), the **OTLP** protocol to send data, and the **Collector**, an agent that receives, processes (batches, samples, removes sensitive fields) and exports telemetry to any back end. On Google Cloud the back ends are Cloud Logging, Cloud Monitoring (including Google Cloud Managed Service for Prometheus) and Cloud Trace, which accept OpenTelemetry data through the Collector's Google Cloud exporters or Google's OTLP endpoint.

```python
# pip install opentelemetry-sdk (plus an exporter); a pipeline step with a span
from opentelemetry import trace

tracer = trace.get_tracer("orders-pipeline")

def load_batch(rows):
    with tracer.start_as_current_span("load_batch") as span:
        span.set_attribute("rows.count", len(rows))
        span.set_attribute("destination.table", "marts.orders")
        # ... write rows; an exception here is recorded on the span automatically
```

**Context propagation** is what turns separate spans into one trace. When service A calls service B, A sends its trace id and span id in a header; B continues the same trace. The W3C **Trace Context** standard defines the `traceparent` header:

```python
header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
version, trace_id, parent_span_id, flags = header.split("-")
print(trace_id)                          # 4bf92f3577b34da6a3ce929d0e0e4736  (16 bytes, hex)
print(parent_span_id)                    # 00f067aa0ba902b7  (8 bytes, hex)
print("sampled" if int(flags, 16) & 1 else "not sampled")   # sampled
```

HTTP and gRPC instrumentation propagates it automatically. **Asynchronous hops need you to do it**: put the context into Kafka record headers or Pub/Sub message attributes when publishing (OTel's `propagate.inject` writes it into a dictionary), and extract it when consuming, linking the consumer span to the producer's. Recent Pub/Sub client libraries can emit OpenTelemetry spans for publish and receive themselves.

**Correlation.** The signals are most useful together. Put `trace_id` in every log line, so you can jump from a slow span to its logs. Use **exemplars**: a histogram bucket stores a sample trace id, so from a latency spike on a chart you click straight into a trace that was in that spike. Cloud Monitoring and Prometheus both support exemplars.

**Observability for data pipelines.** Request tracing was designed for services, but pipelines need the same ideas:

- **Run-level metrics:** rows read, written and rejected per run, duration, bytes billed, and freshness of each output table.
- **Lag metrics** for streams: Kafka consumer lag, Pub/Sub oldest unacknowledged message age, Dataflow system lag and data freshness.
- **A run id everywhere**: the Airflow or Composer run id in job labels (BigQuery job labels, Dataflow job names) and in logs, so that one search finds everything a run did, and costs can be attributed.
- **Lineage** (OpenLineage, Knowledge Catalog (formerly Dataplex Universal Catalog) lineage): which job produced which table from which inputs, the pipeline version of a trace.

> **Pitfall:** High-cardinality labels on metrics. Adding `user_id` or `order_id` as a metric label creates one time series per value, which explodes cost and can hit limits. Put high-cardinality values in logs and trace attributes; keep metric labels to bounded sets (service, table, status).

> **Tip:** Sample traces, but sample smartly. **Head sampling** (decide at the start, for example keep 5%) is cheap but misses rare errors. **Tail sampling** in the Collector decides after the trace finishes, so it can keep all errors and slow traces plus a small share of normal ones.

::: deeper
**Clock skew in traces.** Spans from different machines are timed by different clocks, so a child span can appear to start before its parent. Tracing back ends adjust for small skew, but large gaps in a trace view are sometimes clock differences, not waiting time.

**Span links versus parent-child.** A consumer that processes a batch of 500 messages from different producers cannot have 500 parents. OpenTelemetry **links** connect one span to many related spans without making them parents, which is the right model for batch consumers and fan-in jobs.

**Cost control.** Telemetry can become a large bill. Typical levers: log at `INFO` with structured fields and drop `DEBUG` in production, use log exclusion filters and routing (Cloud Logging sinks to BigQuery or Cloud Storage for cheap retention), aggregate metrics before export, and sample traces.

**Profiles as a fourth signal.** Continuous profiling (CPU and memory profiles sampled in production, as in Cloud Profiler) is increasingly treated as a fourth signal; OpenTelemetry has added a profiling signal specification, though support is still maturing.

> **Interview:** "A user says the order page is slow, but every service's dashboard looks normal. How do you find the cause?" Use a distributed trace for one slow request (find it by exemplar or by user attribute), see which span dominates, then use that span's trace id to pull its logs. Explain why averages hide the problem and why you need percentiles and traces.
:::

::: check
?? What is the difference between a trace and a span?
A span is one timed unit of work (one call, one query) with attributes and a parent. A trace is the whole tree of spans for one request, sharing one trace id.

?? How does a trace continue across a Pub/Sub or Kafka hop?
The producer injects the trace context (the `traceparent` value) into message attributes or record headers, and the consumer extracts it and starts a span linked to or parented by the producer's span.

?? Why should `order_id` not be a metric label?
Every distinct label value creates a new time series, so an unbounded value like an order id creates millions of series, which is expensive and slow. Put it in logs or span attributes instead.
:::

::: resources
- Docs: [OpenTelemetry documentation](https://opentelemetry.io/docs/) · concepts, language SDKs, the Collector and semantic conventions
- Book: [Observability Engineering](https://www.oreilly.com/library/view/observability-engineering/9781492076438/) · Majors, Fong-Jones and Miranda on high-cardinality, event-based observability
- Spec: [W3C Trace Context](https://www.w3.org/TR/trace-context/) · the `traceparent` and `tracestate` headers
- Docs: [Cloud Trace overview](https://cloud.google.com/trace/docs/overview) · Google Cloud's tracing back end and its OpenTelemetry support
- Docs: [Google Cloud Observability](https://cloud.google.com/stackdriver/docs) · Cloud Logging, Cloud Monitoring and Managed Service for Prometheus in one place
- Paper: [Dapper, a Large-Scale Distributed Systems Tracing Infrastructure](https://research.google/pubs/dapper-a-large-scale-distributed-systems-tracing-infrastructure/) · the Google paper that started distributed tracing
:::

### SLA/SLO/SLI for Data Systems

::: plain
A bus company might promise "95% of buses arrive within five minutes of the timetable". The measurement (how late each bus was) is the **indicator**, the 95% target is the **objective**, and if the promise is written into a contract with refunds, it is an **agreement**. The 5% of buses allowed to be late is an **error budget**: while you have budget left you can take risks such as changing routes; when it is spent, you focus on punctuality. Data teams can make the same kind of promise about how fresh, complete and correct their tables are.
:::

**The three terms**, as Google's Site Reliability Engineering (SRE) practice defines them:

- **SLI (service level indicator):** a measured quantity, best expressed as a ratio of good events to all events. "Share of hourly loads that finished within 30 minutes of the hour."
- **SLO (service level objective):** a target for an SLI over a window. "99% of hourly loads finish within 30 minutes, measured over 28 days."
- **SLA (service level agreement):** a contract with consequences (credits, penalties) if an SLO-like target is missed. SLAs are set looser than internal SLOs, so you notice trouble before you owe anyone money. Google Cloud's own SLAs (for example for BigQuery or Pub/Sub) are of this kind.

**Error budgets.** If the SLO is 99%, then 1% of events may be bad. That 1% is a budget the team can spend: on risky deployments, migrations or experiments. When it is exhausted, the agreed policy kicks in, typically freezing feature work in favour of reliability work. The budget turns arguments about "is it reliable enough?" into a number both the data team and its consumers agreed on.

```python
slo = 0.99                    # 99% of hourly loads land within 30 minutes of the hour
loads_per_month = 30 * 24     # 720 hourly loads
budget = round((1 - slo) * loads_per_month)
print("late loads allowed per month:", budget)          # late loads allowed per month: 7
late_so_far, days_elapsed = 5, 10
burn_rate = (late_so_far / budget) / (days_elapsed / 30)
print(f"burn rate: {burn_rate:.1f}x")                   # burn rate: 2.1x
```

A **burn rate** of 1 means the budget would last exactly the window; 2.1 means it will run out in under half the window at this pace, which is worth a ticket, not yet a page.

**SLIs for data.** Services measure availability and latency; data products need their own indicators:

| Dimension | Question | Example SLI |
|---|---|---|
| Freshness | How old is the newest data? | Share of minutes in which `now - max(event_loaded_at)` is under 60 minutes |
| Timeliness | Did the scheduled load land on time? | Share of daily loads complete by 07:00 |
| Completeness | Did all expected data arrive? | Share of days where row count is within 5% of the source count |
| Validity, correctness | Is the data right? | Share of rows passing quality rules (non-null keys, valid ranges, referential integrity) |
| Availability | Can consumers query it? | Share of dashboard queries that succeed |
| Latency (streaming) | How long from event to table? | Share of events visible in BigQuery within 2 minutes of event time |

Measuring freshness in BigQuery is a single query, run on a schedule and written to a metrics table or exported to Cloud Monitoring:

```sql
-- Freshness: minutes since the newest row was loaded
SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_loaded_at), MINUTE) AS staleness_minutes
FROM marts.orders;

-- Timeliness SLI over 28 days from a run log written by the orchestrator
SELECT
  COUNTIF(TIMESTAMP_DIFF(finished_at, scheduled_for, MINUTE) <= 30) / COUNT(*) AS sli,
  COUNTIF(TIMESTAMP_DIFF(finished_at, scheduled_for, MINUTE) > 30) AS late_runs
FROM ops.load_runs
WHERE pipeline = 'orders_hourly'
  AND scheduled_for >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY);
```

**Where to measure.** Measure as close to the consumer as possible: the freshness of the table the dashboard reads, not "the Airflow task succeeded". A task can succeed while loading zero rows. Tools that help: dbt's `source freshness` checks with `warn_after` and `error_after`; Dataform assertions; Knowledge Catalog data quality scans, which run rules on BigQuery tables and publish results you can alert on; and data observability platforms such as Monte Carlo, which learn normal volumes and freshness and alert on anomalies.

**Choosing targets.** Start from what consumers need, not from what you achieve today. Ask: "When would you notice? When would it hurt?" A finance close report needed by 09:00 on the first working day has a sharp timeliness SLO; an exploratory dataset may need none. Do not promise 100%: it leaves no budget for change and is impossible anyway, since your sources and cloud services are not 100% reliable either.

**Alerting on SLOs.** Alert on **burn rate**, not on single failures. The Google SRE workbook recommends multiwindow, multi-burn-rate alerts: for example, page when 2% of a 30-day budget is consumed within one hour (a burn rate of 14.4) and confirmed over a shorter window, and open a ticket for slower burns over days. Cloud Monitoring's SLO monitoring lets you define an SLO on a custom metric and create burn-rate alerting policies directly.

**Data contracts** connect SLOs to producers: the team that owns a source agrees on a schema, semantics and delivery SLOs for what it publishes, and the data team's own SLOs are built on top. Without upstream commitments, a downstream SLO is a promise you cannot keep.

> **Pitfall:** Measuring only job success. "100% of DAG runs succeeded" says nothing about whether the right rows arrived, on time. Measure what consumers see: freshness, volume and quality of the output tables.

> **Tip:** Publish SLOs next to the data, in the catalog entry or table description ("updated hourly, 99% within 30 minutes, owner: orders-data team"). Consumers then know what to expect and whom to call, and fewer people ask "is the data broken?".

::: deeper
**Windows.** Rolling windows (the last 28 days) give a continuously updated view and avoid a "budget reset" that encourages risk at the start of each month. Calendar windows match business reporting. Use 28 or 30 days so that each window contains the same number of each weekday.

**Event-based versus time-based SLIs.** "Share of runs that were on time" counts events; "share of minutes the table was fresh" counts time. Time-based SLIs suit continuously consumed data (a streaming table); event-based ones suit discrete deliveries (a daily file). Pick one per SLO and write the definition down precisely, including how missing data is counted.

**Composite SLOs.** A dashboard that reads five tables is only as fresh as the stalest one; the probability that all five meet their SLO is roughly the product of their individual probabilities. Five independent 99% SLOs give about 95% for the combination, so a consumer-facing SLO often needs tighter SLOs on each input.

**Error budget policy.** Write down in advance what happens when the budget is spent (who decides, which work stops, how long) and get product owners to sign it. Without a policy, error budgets become a dashboard nobody acts on.

> **Interview:** "How would you define reliability for a data pipeline?" Name consumer-facing SLIs (freshness, completeness, validity), give a concrete SLO with a window ("99% of days the revenue table is complete by 07:00 over 28 days"), explain the error budget and burn-rate alerting, and mention data contracts with upstream owners.
:::

::: check
?? What is the difference between an SLO and an SLA?
An SLO is an internal reliability target for an SLI over a window. An SLA is a contract with external consequences, usually set looser than the SLO so the team has warning before breaching it.

?? Your SLO allows 7 late loads per month and you have had 6 by day 15. What is the burn rate and what should happen?
Burn rate = (6 / 7) / (15 / 30) = about 1.7: the budget will run out well before the month ends. Follow the error budget policy, for example pause risky changes and prioritise reliability fixes.

?? Why is "all Airflow tasks succeeded" a poor SLI?
It measures the tool, not the data. Tasks can succeed while loading no rows, duplicate rows or late data. SLIs should measure what consumers experience: freshness, completeness and validity of the output tables.
:::

::: resources
- Book: [Service Level Objectives (Site Reliability Engineering)](https://sre.google/sre-book/service-level-objectives/) · Google's chapter defining SLIs, SLOs and SLAs
- Book: [Alerting on SLOs (The Site Reliability Workbook)](https://sre.google/workbook/alerting-on-slos/) · multiwindow, multi-burn-rate alerting, worked step by step
- Docs: [Cloud Monitoring SLO monitoring](https://cloud.google.com/stackdriver/docs/solutions/slo-monitoring) · defining SLOs and burn-rate alerts in Google Cloud
- Docs: [Knowledge Catalog data quality](https://cloud.google.com/dataplex/docs/data-quality-overview) · rule-based quality scans on BigQuery tables
- Docs: [dbt source freshness](https://docs.getdbt.com/docs/deploy/source-freshness) · declaring and checking freshness expectations on sources
- Book: [Implementing Service Level Objectives](https://www.oreilly.com/library/view/implementing-service-level/9781492076803/) · Alex Hidalgo's practical guide to choosing SLIs and running error budgets
:::

## Wrap-up

### Cheat sheet

| Concept or task | Rule of thumb, command or formula |
|---|---|
| Timeout with no reply | Ambiguous: the work may or may not have happened; retry only idempotent operations |
| Measure durations | Monotonic clock (`time.monotonic()`), never wall-clock time |
| Order events across machines | Causal order: Lamport clocks for a total order, vector clocks to detect concurrency |
| CAP | During a partition, choose consistency or availability; "CA" is not an option |
| PACELC | Else (no partition), choose latency or consistency |
| Quorum overlap | `W + R > N`; N=3, W=2, R=2 survives one node down |
| Majority for consensus | `N // 2 + 1`; tolerate f crash faults with `2f + 1` nodes; use 3 or 5 nodes in 3 zones |
| Byzantine faults | `3f + 1` nodes; inside one company, prefer checksums and reconciliation |
| Key placement that survives resizing | Consistent hashing with virtual nodes: about `1/N` of keys move |
| Kafka partition count change | Remaps keys (`murmur2 % partitions`); plan it like a migration |
| Conflict-free merges | CRDTs: merge must be commutative, associative and idempotent |
| One leader at a time | Lease (etcd, ZooKeeper, Kubernetes Lease, transactional DB row) plus fencing token |
| Compare-and-set on Cloud Storage | `ifGenerationMatch` precondition; `ifGenerationMatch=0` means create only if absent |
| Cross-system atomic change | 2PC only inside one operator's systems (Spanner, Kafka); otherwise saga plus idempotency |
| Retries | Retryable errors only, exponential backoff with jitter, capped attempts, one layer only |
| Stop hammering a dead dependency | Circuit breaker: closed, open, half-open |
| Protect a rate-limited API | One shared token bucket (Redis, Airflow pool, Cloud Tasks queue), honour `Retry-After` |
| Bounded streaming memory | Backpressure: Pub/Sub flow control, Flink credits, `maxOffsetsPerTrigger` |
| Fast bulk reads from BigQuery | Storage Read API: gRPC streams of Arrow in parallel |
| Trace across Pub/Sub or Kafka | Inject `traceparent` into attributes or headers; extract in the consumer |
| Data SLIs | Freshness, timeliness, completeness, validity, availability |
| Error budget | `(1 - SLO) x events in window`; alert on burn rate, not single failures |

### Practice

::: try
**1. Pick a quorum.** A store keeps N = 5 replicas across three zones. You need reads to see the latest acknowledged write, and both reads and writes must keep working with any two replicas down. Which W and R work? What happens to writes if three replicas are down?
:::

?? Answer to 1
W = 3 and R = 3: 3 + 3 > 5 gives overlap, and each operation still has 3 replicas when 2 are down. With three replicas down only 2 remain, fewer than W, so writes fail (the system chooses consistency). W = 4, R = 2 also overlaps but fails writes with two replicas down, so it does not meet the requirement.

::: try
**2. Fence the compactor.** A nightly job compacts small files in a Cloud Storage prefix and then writes a manifest object `manifest.json` that readers use. Two copies sometimes run at once after a scheduler failover, and the older one occasionally overwrites the newer manifest. Design a fix without adding a new service.
:::

?? A solution to 2
Use the manifest's object generation as a fencing token. Each run reads `manifest.json` and remembers its generation number, does its work, then writes the new manifest with the precondition `ifGenerationMatch=<generation it read>`. If another run has written in between, the generation has changed and the write fails with `412 Precondition Failed`; the losing run discards its output (or re-reads and retries from the new state). Make compaction output files content-addressed or uniquely named, so a losing run's files are harmless and can be cleaned up by a lifecycle rule.

::: try
**3. Rate-limit an ingestion job.** A vendor API allows 600 requests per minute per key, with bursts of up to 50. Your Managed Airflow DAG fetches 40,000 customer records, one request each, using a pool of parallel tasks. Write a token bucket configuration, say how long the fetch takes at best, and describe how the parallel tasks should share the limit.
:::

?? A solution to 3
Rate 10 tokens per second (600 per minute), capacity 50. At best the fetch takes about 40,000 / 10 = 4,000 seconds, a little over 66 minutes (the initial burst of 50 saves only 5 seconds). All tasks must take tokens from one shared bucket, for example a Redis key updated atomically by a Lua script, or a single Cloud Tasks queue with `maxDispatchesPerSecond` = 10 that calls a Cloud Run fetcher; per-task limits would multiply the rate by the number of tasks. On 429 responses, back off with jitter and honour `Retry-After`.

::: try
**4. Write an SLO.** Analysts use `marts.daily_revenue` for a 09:00 meeting each working day. The table is rebuilt after the source system's nightly export lands, usually around 05:00. Write an SLI and SLO, compute the error budget for a quarter of 63 working days at your target, and describe the alert.
:::

?? A solution to 4
SLI: share of working days on which `marts.daily_revenue` contains the previous day's data, passes its quality checks (row count within 5% of the source, no null keys) and is complete by 08:00. SLO: 98% over a rolling quarter. Budget: (1 - 0.98) x 63 = 1.26, so 1 missed day per quarter. Alert: a scheduled check at 07:00 opens a ticket if the load has not finished (giving an hour to fix), and pages at 08:00 if the table is still not complete; the second miss in a quarter triggers the error budget policy (for example, no non-urgent changes to the pipeline until a fix for the root cause ships). Publish the SLO in the table's catalog entry.

### What comes next

You can now reason about what happens inside the distributed systems you use: how they place data, agree on order and leaders, survive failures, and how to make your own services and pipelines degrade gracefully and measure their reliability. The next section, **Data engineering system design**, puts all of this to work: it walks through designing complete data platforms end to end (lakehouse and medallion architectures, data mesh, real-time analytics, ingestion systems and feature stores), then the non-functional side (cost, capacity planning, disaster recovery, privacy, multi-region designs and migrations), and finishes with the system design interview patterns you are expected to handle.
