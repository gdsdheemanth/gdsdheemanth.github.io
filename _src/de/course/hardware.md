This section goes one floor below the databases and warehouses you have met so far. It is for anyone who has used BigQuery, Spark or PostgreSQL and wondered why one layout is fast and another slow, why a job with plenty of CPUs still crawls, or why everyone says "Kafka is fast because of the page cache". It assumes the earlier sections: Data engineering foundations (row versus column storage), Data warehousing and Data lakes and lakehouses (Parquet, partitioning) and, most directly, Databases: OLTP, NoSQL and specialised (B-trees, LSM trees and storage engines). You do not need to know C or assembly. Where a topic shows Python, it runs with the standard library and prints exactly what its comments say.

**What you will be able to do afterwards**

- Explain why sequential I/O beats random I/O, how SSDs differ from hard disks inside, and choose between Persistent Disk, Hyperdisk and Local SSD for a data workload on Google Cloud.
- Reason about CPU caches, cache lines, NUMA and the TLB, and recognise code and data layouts that waste them.
- Decide when GPUs help a data pipeline, and run a pandas-style or Spark job on GPUs with RAPIDS.
- Describe the Linux I/O path: the page cache, direct I/O, `mmap`, `sendfile` zero-copy and io_uring, and say which data systems rely on each.
- Pick a compression codec and understand the encodings inside Parquet, the wire format of Protocol Buffers, and probabilistic structures such as Bloom filters and HyperLogLog.
- Explain envelope encryption with Cloud KMS and Parquet modular encryption.
- Tune the JVM under Spark, Kafka and Flink, and profile a slow data job with `perf`, flame graphs, eBPF and async-profiler.

**The big picture.** Every data system is a negotiation with physics. Moving a byte from a CPU register costs a fraction of a nanosecond; from main memory about a hundred nanoseconds; from an NVMe SSD tens of microseconds; from a spinning disk several milliseconds; from another region over the network tens of milliseconds. That is a range of about a hundred million. Nearly every design trick in this course (columnar files, compression, batching, append-only logs, caching, vectorized execution) exists to keep data in the fast, close layers and to move it in large, sequential chunks when it must move. Learn the ladder of latencies and the reasons behind the tricks become obvious.

[[fig:hardware_ladder]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Hardware

This group is about the machine itself: the storage devices that hold data at rest, the memory and caches that feed the CPU, the way large servers split memory between sockets, and the GPUs that are increasingly used for data work. These are the costs that every higher layer (file formats, query engines, Spark) is built around.

Even in the cloud, where you never touch a server, the same physics applies. A Compute Engine VM has a real CPU with real caches, its disks are real SSDs or hard disks behind a network, and choosing the wrong disk type or machine shape costs money and time. The topics below give you the numbers and mental models to make those choices on purpose.

### SSD vs HDD — I/O Characteristics for DE

::: plain
A hard disk drive (HDD) is a record player: a metal platter spins and an arm moves to the right track before it can read anything. Reading a long track in order is quick, but jumping around costs a few milliseconds per jump while the arm moves. A solid-state drive (SSD) has no moving parts; it is a grid of memory chips, so a jump costs almost nothing. That single difference, the cost of a **random** read compared with a **sequential** one, shaped decades of database design.
:::

**Sequential and random I/O.** Sequential I/O reads or writes consecutive blocks; random I/O touches blocks scattered across the device. Two numbers describe a device: **throughput** (megabytes per second, what matters for sequential scans) and **IOPS** (input/output operations per second, what matters for small random reads). Latency (time for one operation) links them.

| | HDD (7,200 rpm) | SATA SSD | NVMe SSD (PCIe 4.0 or 5.0) |
|---|---|---|---|
| Random read latency | 5 to 10 ms (seek plus rotation) | about 100 µs | tens of µs |
| Random 4 KiB IOPS | 100 to 200 | tens of thousands | hundreds of thousands to over a million |
| Sequential throughput | about 150 to 280 MB/s | about 550 MB/s (SATA limit) | several GB/s |
| Cost per TB | lowest | middle | higher |

Worked numbers make the gap concrete. Reading 1 GiB as 4 KiB blocks:

```python
# Time to read 1 GiB as 4 KiB blocks, with assumed but realistic device figures
GiB = 1024**3
blocks = GiB // 4096                     # 262,144 blocks of 4 KiB

devices = {
    #             sequential MB/s, random 4 KiB IOPS
    "HDD 7200 rpm": (250, 150),
    "NVMe SSD":     (7000, 1_000_000),
}
for name, (seq_mbps, iops) in devices.items():
    seq_s = GiB / (seq_mbps * 1e6)
    rand_s = blocks / iops
    print(f"{name:13} sequential {seq_s:7.2f} s   random {rand_s:8.2f} s")
# HDD 7200 rpm  sequential    4.29 s   random  1747.63 s
# NVMe SSD      sequential    0.15 s   random     0.26 s
```

On a hard disk, random access is about 400 times slower than sequential. On NVMe the gap almost closes. That is why systems designed in the HDD era (HDFS, Kafka, LSM trees, Parquet scans) are built around large sequential reads and append-only writes, and why those choices still pay off on SSDs: sequential access is also friendlier to caches, prefetching and the SSD's own internals.

**NVMe and PCIe lanes.** SATA was designed for disks and allows one command queue of 32 entries. **NVMe** (Non-Volatile Memory Express) talks to the SSD directly over **PCIe** lanes and allows up to 65,535 queues of up to 65,536 commands each, so many CPU cores can submit I/O in parallel without sharing a lock. A typical NVMe drive uses four lanes: PCIe 4.0 x4 gives roughly 7 GB/s and PCIe 5.0 x4 roughly 14 GB/s. To reach the advertised IOPS you need **queue depth**: many requests in flight at once. A single thread issuing one synchronous read at a time sees the latency, not the IOPS.

**Inside an SSD: pages, blocks and the FTL.** NAND flash is written in **pages** (typically 4 to 16 KiB) but erased only in much larger **erase blocks** (hundreds of pages, several MiB). A page cannot be overwritten in place: it must be erased first, and erase works on the whole block. The drive's firmware, the **flash translation layer (FTL)**, hides this. It writes every update to a fresh page, remaps the logical address, and marks the old page stale. Later, **garbage collection** copies the still-valid pages out of a mostly-stale block and erases it.

[[fig:hardware_ftl]]

That copying is **write amplification**: the drive writes more bytes to flash than the host sent. A write amplification factor (WAF) of 3 means each 1 GB from the host costs 3 GB of flash writes. WAF grows when the drive is nearly full (less free space to shuffle into) and when writes are small and random. **TRIM** (or `discard`) tells the drive which logical blocks are free so garbage collection does not copy dead data.

**Wear.** Each flash cell survives a limited number of program/erase cycles, from tens of thousands for SLC (one bit per cell) down to around a thousand or fewer for QLC (four bits per cell). Drives spread writes evenly (**wear levelling**) and are rated in **TBW** (terabytes written) or **DWPD** (drive writes per day over the warranty). Write-heavy data systems (Kafka brokers, RocksDB state for Flink, Spark shuffle) chew through endurance, so check the rating.

**I/O schedulers.** The kernel's block layer can reorder and merge requests before they reach the device. Older guides mention **CFQ**, **deadline** and **noop**: those were the single-queue schedulers, removed in Linux 5.0. Modern kernels use multi-queue (blk-mq) schedulers: `none`, `mq-deadline`, `bfq` and `kyber`. For NVMe the usual choice is `none`, because the device is fast and has its own queues; `mq-deadline` suits hard disks and some SATA SSDs.

```bash
cat /sys/block/nvme0n1/queue/scheduler          # [none] mq-deadline kyber bfq
lsblk -d -o NAME,ROTA,SIZE,MODEL                # ROTA=1 means a rotating disk
# Measure, do not guess: random 4 KiB reads at queue depth 64, bypassing the page cache
fio --name=randread --filename=/mnt/data/test --size=4G --rw=randread --bs=4k \
    --iodepth=64 --ioengine=io_uring --direct=1 --runtime=60 --time_based
```

**Disks on Google Cloud.** Compute Engine offers network-attached block storage and local disks:

| Option | What it is | Good for |
|---|---|---|
| Persistent Disk (`pd-standard`, `pd-balanced`, `pd-ssd`) | Network block storage, replicated; `pd-standard` is HDD-backed | Boot disks, general data; performance scales with size and vCPUs |
| Hyperdisk (Balanced, Throughput, Extreme, ML) | Newer network block storage where you provision IOPS and throughput separately from capacity | Databases, Kafka brokers, analytics nodes on current machine series |
| Local SSD | NVMe drives physically attached to the host | Scratch space: Spark shuffle, temp files, caches. Data is lost if the VM stops |

For Spark on Managed Service for Apache Spark (formerly Dataproc), attaching Local SSDs to workers speeds up shuffle and spill, which are random-ish writes and reads of intermediate files:

```bash
gcloud dataproc clusters create etl-cluster --region=europe-west1 \
  --worker-machine-type=n2-standard-16 --num-workers=4 \
  --num-worker-local-ssds=2 --worker-local-ssd-interface=NVME
```

> **Pitfall:** Treating Local SSD as durable. It disappears when the VM is stopped, deleted or preempted (Spot VMs). Use it for data you can recompute, never for the only copy.

> **Tip:** A disk benchmark that hits the page cache measures memory, not the disk. Use `--direct=1` in `fio`, a file larger than RAM, or both.

::: deeper
**Why LSM trees and logs love SSDs and HDDs alike.** An LSM tree (RocksDB, Cassandra, Bigtable's design) turns random writes into sequential ones: it buffers writes in memory, flushes sorted runs, and compacts them in the background. On HDDs that avoids seeks; on SSDs it keeps host writes large and aligned, which lowers the drive's internal write amplification. The price is the engine's own write amplification from compaction, often 10x or more for levelled compaction. B-trees do in-place page updates: more random writes, less rewriting overall. The Databases section compares the two engines.

**Over-provisioning.** Leaving part of an SSD unpartitioned (or buying a drive with more spare area) gives the FTL room to work and reduces WAF and latency spikes under sustained writes. Enterprise drives reserve more spare area than consumer ones.

**Tail latency.** Garbage collection inside the drive can stall reads for milliseconds. For latency-sensitive services, the 99.9th percentile matters more than the average. Network block storage adds its own latency (around a millisecond or less for SSD-backed types), which is why local NVMe still wins for shuffle-heavy jobs.

**Zoned and host-managed devices.** Zoned Namespace (ZNS) SSDs and SMR hard disks expose zones that must be written sequentially, moving garbage collection to the host. Log-structured systems fit them naturally; general file systems need special support.

> **Interview:** "Why do Kafka and HDFS prefer large sequential I/O even on SSDs?" Sequential access needs fewer operations per byte, lets the kernel prefetch, keeps the SSD's internal write amplification low, and still scales to hard disks for cheap capacity. Random small I/O is what costs latency and wears flash.
:::

::: check
?? Why is random I/O so much slower than sequential I/O on a hard disk, and why is the gap much smaller on an NVMe SSD?
Each random read on an HDD waits for the arm to seek and the platter to rotate (milliseconds). An SSD has no moving parts, so a random read costs tens of microseconds, and its parallel internals serve many requests at once.

?? What is write amplification in an SSD, and what makes it worse?
The drive writing more data to flash than the host sent, because garbage collection copies valid pages before erasing blocks. Small random writes, a nearly full drive and missing TRIM make it worse.

?? Your Spark jobs on Managed Service for Apache Spark spend most of their time in shuffle. Which disk change helps, and what is the caveat?
Attach Local SSDs (NVMe) to the workers for shuffle and spill files. They are fast but ephemeral: data is lost when the VM stops, which is acceptable for recomputable intermediate data only.
:::

::: resources
- Docs: [Compute Engine storage options](https://cloud.google.com/compute/docs/disks) · Persistent Disk, Hyperdisk and Local SSD compared, with performance limits
- Docs: [Local SSD disks](https://cloud.google.com/compute/docs/disks/local-ssd) · how Local SSD works, interfaces and data persistence rules
- Book: [Database Internals](https://www.oreilly.com/library/view/database-internals/9781492040330/) · Alex Petrov; the storage chapters explain B-trees, LSM trees and how they meet disks
- Paper: [The Unwritten Contract of Solid State Drives](https://research.cs.wisc.edu/adsl/Publications/eurosys17-he.pdf) · EuroSys 2017; the rules software must follow to get good SSD performance
- Docs: [fio documentation](https://fio.readthedocs.io/en/latest/fio_doc.html) · the standard tool for measuring IOPS, throughput and latency
- Blog: [Linux performance](https://www.brendangregg.com/linuxperf.html) · Brendan Gregg's map of Linux I/O and observability tools
:::

### Memory Hierarchy & Cache Effects

::: plain
Your CPU is a chef who works incredibly fast, and main memory is a supermarket across town. Fetching each ingredient from the supermarket would leave the chef idle most of the time, so the kitchen has a tiny counter (L1 cache), a fridge (L2), a pantry (L3) and only then the supermarket (RAM). Whatever is on the counter is used instantly. Fast code is code that keeps the right ingredients on the counter and fetches them in large, predictable trips.
:::

**The hierarchy.** Approximate figures for a current server CPU (exact sizes vary by model):

| Level | Typical size | Latency | Shared by |
|---|---|---|---|
| Registers | a few hundred bytes | under 1 ns | one core |
| L1 data cache | 32 to 64 KiB | about 1 ns (4 to 5 cycles) | one core |
| L2 cache | 1 to 3 MiB | about 3 to 5 ns | one core |
| L3 cache | tens to hundreds of MiB | about 10 to 40 ns | all cores on a die or socket |
| Main memory (DRAM) | hundreds of GiB | about 80 to 120 ns | the socket (see NUMA) |

**Cache lines.** Memory moves between levels in fixed chunks called **cache lines**, 64 bytes on x86 and most Arm server cores (some Apple chips use 128). Reading one 4-byte integer brings in its 15 neighbours for free. If your next reads use those neighbours, they hit the cache; if they jump elsewhere, each one is a miss. This is **spatial locality**. Reusing the same data soon after is **temporal locality**.

A tiny cache simulator shows the effect. It walks a 1024 x 1024 matrix of 4-byte integers, stored row by row, through a 32 KiB least-recently-used cache:

```python
from collections import OrderedDict

LINE = 64                      # bytes per cache line
CACHE_LINES = 512              # a 32 KiB cache (512 lines of 64 bytes)
N = 1024                       # a 1024 x 1024 matrix of 4-byte integers

def misses(order):
    cache, miss = OrderedDict(), 0
    for i in range(N):
        for j in range(N):
            r, c = (i, j) if order == "row" else (j, i)
            line = (r * N + c) * 4 // LINE         # which cache line holds this value
            if line in cache:
                cache.move_to_end(line)            # hit: mark as recently used
            else:
                miss += 1                          # miss: load it, evict the oldest
                cache[line] = True
                if len(cache) > CACHE_LINES:
                    cache.popitem(last=False)
    return miss

print("row-by-row misses:   ", misses("row"))     # row-by-row misses:    65536
print("column-by-column:    ", misses("col"))     # column-by-column:     1048576
```

Same data, same arithmetic, 16 times more misses. Row-by-row uses all 16 integers of every line it loads; column-by-column uses one and the line is evicted before the next column comes back for it. This is exactly why columnar formats and engines are fast for analytics: a scan over one column reads densely packed values of one type, line after line.

**Prefetching.** CPUs detect sequential and strided access patterns and fetch lines before you ask. Pointer chasing (linked lists, trees of small objects, hash tables with chains) defeats the prefetcher, because the next address is unknown until the current load finishes. That is one reason engines prefer arrays and open-addressing hash tables, and why a Python list of objects scans far slower than a NumPy or Arrow array.

**The TLB.** Programs use virtual addresses, which the CPU translates to physical ones through page tables. The **translation lookaside buffer (TLB)** caches recent translations. With 4 KiB pages, a few thousand TLB entries cover only a few MiB to tens of MiB, so random access across a 50 GiB hash table misses the TLB constantly and each miss walks the page tables. **Huge pages** (2 MiB or 1 GiB) cover far more memory per entry. Linux can use them transparently (Transparent Huge Pages, THP).

**False sharing.** Two threads updating different variables that happen to sit in the same cache line force that line to bounce between cores, because the coherence protocol tracks whole lines. Each core thinks it is writing private data, yet the program runs as if it were contended. The fix is padding or aligning per-thread counters to separate lines (Java has `@Contended`; C and C++ use `alignas(64)`).

**Latency versus bandwidth.** DRAM latency is about 100 ns, but bandwidth is high (tens of GB/s per channel, hundreds of GB/s per socket) if many requests are in flight. Sequential scans approach bandwidth limits; random lookups are latency-bound. A query engine that scans compressed columns can be bandwidth-bound; one that probes a huge hash table is latency-bound.

> **Pitfall:** Benchmarking on a small dataset that fits in L3 and extrapolating to production. Once the working set exceeds the caches, performance can drop several times. Test with realistic sizes.

> **Tip:** On Linux, `lscpu` shows cache sizes and `getconf LEVEL1_DCACHE_LINESIZE` prints the line size.

::: deeper
**Cache-conscious and cache-oblivious algorithms.** Cache-conscious code is tuned for known sizes: query engines size hash-join partitions or vectors to fit L2 (DuckDB's default vector of 2,048 values is one such choice), and radix-partitioned joins split inputs until each partition's hash table fits in cache. **Cache-oblivious** algorithms (recursive matrix layouts, van Emde Boas tree layouts, funnel sort) are designed to use every cache level well without knowing the sizes, by recursively dividing work until it fits.

**Structure of arrays.** Storing records as an array of structs mixes fields in each line; a struct of arrays (one array per field) is the in-memory version of a columnar file, and it is what Apache Arrow standardises.

**Measuring.** `perf stat -e cache-misses,cache-references,dTLB-load-misses ./job` counts hardware events. The Performance work group covers `perf`. Intel and AMD also provide top-down analysis tools that classify stalls as front-end, back-end, memory or bad speculation.

**THP caveats.** Some databases (MongoDB in older versions, Redis) recommended disabling THP because background compaction caused latency spikes; others benefit. Measure for your workload.

> **Interview:** "Why does iterating a 2D array column by column run slower than row by row in C or Java?" Arrays are stored row-major; row order uses every value in each 64-byte cache line and triggers the prefetcher, while column order touches a new line for every element and evicts lines before they are reused.
:::

::: check
?? What is a cache line, and how big is it on typical x86 servers?
The fixed-size unit that moves between memory and caches: 64 bytes. Loading one value loads its neighbours in the same line too.

?? Two threads increment two separate counters and the program scales badly. What might be wrong?
False sharing: the counters sit in the same cache line, so each write invalidates the other core's copy. Pad or align them to separate cache lines.

?? Why do huge pages help a query that probes a very large hash table at random?
Each TLB entry then covers 2 MiB instead of 4 KiB, so far fewer address translations miss the TLB and walk the page tables.
:::

::: resources
- Paper: [What Every Programmer Should Know About Memory](https://people.freebsd.org/~lstewart/articles/cpumemory.pdf) · Ulrich Drepper; the classic long guide to caches, TLBs and NUMA
- Book: [Systems Performance, 2nd edition](https://www.brendangregg.com/systems-performance-2nd-edition-book.html) · Brendan Gregg; the CPU and memory chapters with methods and tools
- Article: [Gallery of Processor Cache Effects](https://igoro.com/archive/gallery-of-processor-cache-effects/) · Igor Ostrovsky; seven small experiments that make cache behaviour visible
- Docs: [Transparent Hugepage Support](https://docs.kernel.org/admin-guide/mm/transhuge.html) · the Linux kernel's description of THP and its settings
- Article: [Latency numbers every programmer should know](https://gist.github.com/jboner/2841832) · the widely shared latency table, useful as a mental model
:::

### NUMA Architecture & Memory Locality

::: plain
A big server is really two or more computers glued together. Each processor socket has its own bank of memory close to it. A core can still read the other socket's memory, but it has to ask across a bridge, which is slower. That design is **NUMA**: non-uniform memory access. It is like an office with two floors and a filing cabinet on each: you can use the other floor's cabinet, but every trip takes longer, so you keep your own files on your own floor.
:::

**What a NUMA node is.** A **NUMA node** is a group of CPU cores plus the memory controller and DIMMs attached to them. In a two-socket server there are usually two nodes; some CPUs split a single socket into several nodes (sub-NUMA clustering). Nodes are linked by an interconnect (Intel UPI, AMD Infinity Fabric). Accessing **local** memory is fastest; **remote** memory costs more latency (often 1.5 to 2 times) and shares the interconnect's bandwidth.

See the topology on Linux:

```bash
lscpu | grep -i numa
# NUMA node(s):        2
# NUMA node0 CPU(s):   0-31,64-95
# NUMA node1 CPU(s):   32-63,96-127
numactl --hardware        # nodes, memory per node, and the distance matrix
numastat -p $(pgrep -f kafka.Kafka)   # how much of a process's memory sits on each node
```

The distance matrix printed by `numactl --hardware` uses relative units, 10 for local; a value of 20 or 21 for remote is typical of two-socket machines.

**How Linux places memory.** The default policy is **first touch**: a page is allocated on the node of the CPU that first writes it. That is usually good, but it causes a classic problem: one thread initialises a large buffer (all pages land on its node), then worker threads on the other node use it remotely. Policies you can set with `numactl` or `set_mempolicy`:

| Policy | Meaning | When it helps |
|---|---|---|
| Local (default, first touch) | Allocate on the node of the CPU that touches the page | Threads that use their own data |
| Bind (`--membind`) | Only allocate from given nodes | Pinning a process to one socket |
| Interleave (`--interleave=all`) | Spread pages round-robin over nodes | Large shared structures used by all cores |
| Preferred (`--preferred`) | Try a node first, fall back | Soft placement |

```bash
# Run one Kafka broker or one benchmark entirely on socket 0: its CPUs and its memory
numactl --cpunodebind=0 --membind=0 bin/kafka-server-start.sh config/server.properties
# Spread a large shared in-memory cache evenly across both sockets
numactl --interleave=all ./my-cache-server
```

Linux also has **automatic NUMA balancing** (`kernel.numa_balancing`), which samples accesses and migrates pages or tasks closer together. It helps many workloads and costs some overhead; databases sometimes disable it after measuring.

**NUMA-aware data structures.** The idea is to partition work and data by node: each node gets its own memory pool, queue or hash-table partition, and threads mostly touch their node's partition. Morsel-driven parallelism (from the HyPer database, and used in DuckDB-style engines) does this for queries: input is split into small chunks (morsels), each worker prefers morsels whose data is on its own node, and work-stealing balances the rest. The Query engine internals section returns to it.

**NUMA in JVM data systems.** The JVM can allocate the young generation per node with `-XX:+UseNUMA`. It has long worked with the Parallel collector, and G1 gained NUMA-aware allocation in JDK 14. For Kafka and Flink on large machines, common practice is either to run one process per NUMA node (pinned with `numactl`) or to run one process with `-XX:+UseNUMA` and measure. Off-heap memory (Netty buffers, RocksDB) follows the normal first-touch rule.

**On Google Cloud.** Small VMs fit inside one NUMA node, so NUMA rarely matters. Large machine types (many vCPUs, hundreds of GiB) can span sockets, and Compute Engine exposes the topology to the guest, so `lscpu` inside the VM shows more than one node. If a big VM gives disappointing scaling, check the node count before anything else, and consider several smaller VMs or one process per node.

> **Pitfall:** Allocating and filling a huge buffer from the main thread, then processing it from workers on all sockets. All pages live on one node, and half the workers pay remote latency and saturate one memory controller. Initialise data from the threads that will use it, or interleave.

> **Tip:** When a job scales well to the core count of one socket and then flattens, NUMA is a prime suspect.

::: deeper
**Bandwidth, not only latency.** A process confined to one node gets that node's memory bandwidth only. For a bandwidth-bound scan, interleaving over both nodes can be faster than binding, even though half the accesses are remote. For latency-bound probing with good locality, binding wins. Measure both.

**Sub-NUMA clustering (SNC) and chiplets.** Modern CPUs are built from several dies. Intel SNC and AMD NPS (nodes per socket) settings split one socket into 2 or 4 NUMA nodes so software can exploit the shorter distance to nearby memory controllers. In the cloud the provider chooses this; you see the result in `lscpu`.

**Thread pinning.** Linux may migrate threads between nodes. `taskset`, `numactl --cpunodebind`, cgroup `cpusets` (used by Kubernetes' static CPU manager) keep them in place. Pinning helps consistency but can hurt if the pinned set is overloaded.

**Measuring remote access.** `perf stat -e node-loads,node-load-misses` or `numastat` show local versus remote traffic. A high share of remote access in a process you thought was local usually means first-touch went wrong.

> **Interview:** "A 128-vCPU VM runs your in-memory aggregation only 1.3 times faster than a 64-vCPU VM. What do you check?" Whether the larger VM spans two NUMA nodes, whether memory was allocated by one thread (first touch on one node), and whether the workload is bandwidth-bound on one memory controller. Fixes: per-node partitioning, interleaving, or two processes pinned one per node.
:::

::: check
?? What is first-touch allocation, and how can it hurt a multi-threaded data job?
Linux places a page on the NUMA node of the CPU that first writes it. If one thread initialises all the data, it all lands on one node, and threads on other nodes then access it remotely and slowly.

?? Write a command that runs a program with both its CPUs and memory restricted to NUMA node 1.
`numactl --cpunodebind=1 --membind=1 ./program`

?? When might interleaving memory across nodes beat binding to one node?
When the workload is bandwidth-bound and shared by threads on all nodes: interleaving uses every memory controller's bandwidth, even though some accesses are remote.
:::

::: resources
- Docs: [NUMA memory policy](https://docs.kernel.org/admin-guide/mm/numa_memory_policy.html) · the Linux kernel's description of local, bind, interleave and preferred policies
- Docs: [numactl manual page](https://man7.org/linux/man-pages/man8/numactl.8.html) · every option for binding CPUs and memory
- Paper: [Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework](https://db.in.tum.de/~leis/papers/morsels.pdf) · Leis et al., SIGMOD 2014; how a query engine schedules work by NUMA node
- Paper: [What Every Programmer Should Know About Memory](https://people.freebsd.org/~lstewart/articles/cpumemory.pdf) · Drepper; section 5 covers NUMA in depth
- Book: [Systems Performance, 2nd edition](https://www.brendangregg.com/systems-performance-2nd-edition-book.html) · Brendan Gregg; NUMA observability and tuning in the memory chapter
:::

### GPU-Accelerated Data Processing

::: plain
A CPU is a handful of brilliant specialists; a GPU is a stadium full of people who can each do simple arithmetic. Give the stadium a job that splits into millions of identical little steps (compare these numbers, add those up, sort this column) and it finishes far sooner than the specialists. But everyone has to be told what to do and handed their data first, and the bus that brings the data to the stadium is slow. GPUs win when there is a lot of work per byte delivered.
:::

This topic explains **why and when** GPUs speed up data processing and how the main algorithms map onto them. The next topic, on GPU-accelerated ETL with RAPIDS and cuDF, is the hands-on companion: the libraries, the code and how to run them on Google Cloud.

**How a GPU differs from a CPU.** A server CPU has tens of powerful cores, large caches, branch predictors and SIMD units that process 8 to 16 numbers per instruction (see SIMD Vectorization for Analytics in the Query engine internals section). A data-centre GPU has thousands of simpler cores grouped into streaming multiprocessors. Threads run in groups of 32 (a **warp**) that execute the same instruction on different data, a model NVIDIA calls SIMT. GPUs hide memory latency by switching between thousands of in-flight threads rather than by large caches, and they sit on very fast memory: high-end GPUs with HBM reach several TB/s, against a few hundred GB/s for a CPU socket.

**The catch: data has to get there.** The GPU usually connects over PCIe. PCIe 4.0 x16 moves about 25 to 32 GB/s each way, much slower than the GPU's own memory. GPU memory is also small compared with a server's RAM (24 GB on an L4, 40 or 80 GB on an A100). So the question is always how much work you do per byte moved:

```python
# Does moving 10 GB to a GPU pay off? Rough, assumed bandwidth figures in GB/s.
data_gb = 10
pcie = 25          # PCIe 4.0 x16, effective, host to GPU
gpu_mem = 2000     # high-end GPU memory bandwidth (HBM)
cpu_mem = 100      # one CPU socket's memory bandwidth

transfer = data_gb / pcie
for passes in (1, 20):                     # a simple filter vs a heavy join or sort
    gpu = transfer + passes * data_gb / gpu_mem
    cpu = passes * data_gb / cpu_mem
    print(f"{passes:2} passes over the data: CPU {cpu:5.2f} s, GPU {gpu:5.2f} s (transfer {transfer:.2f} s)")
#  1 passes over the data: CPU  0.10 s, GPU  0.41 s (transfer 0.40 s)
# 20 passes over the data: CPU  2.00 s, GPU  0.50 s (transfer 0.40 s)
```

A single filter is faster on the CPU because the transfer dominates. A pipeline that joins, sorts, aggregates and transforms the same data many times amortises the transfer and the GPU wins. Keeping data **resident on the GPU** across many operations is the central design rule.

**How relational operators map to GPUs.**

| Operator | GPU approach | Why it fits |
|---|---|---|
| Filter and projection | One thread per row computes a predicate, then a parallel **prefix sum** (scan) computes output positions to compact the survivors | Perfectly data-parallel |
| Sort | **Radix sort**, which sorts by digits with counting passes instead of comparisons | Regular memory access, no branches |
| Hash join | Build a hash table in GPU memory in parallel, then probe it with one thread per row | Huge memory bandwidth for random probes |
| Hash aggregation | Threads insert into a shared hash table with **atomic** add operations | Fast while groups are not too contended |
| Decode and decompress | Parquet decoding and codecs (NVIDIA nvCOMP) run on the GPU | Moves CPU-heavy work off the CPU |

**GPU shuffle.** In distributed engines the slow part is often the shuffle. Spark's RAPIDS shuffle manager can move data between GPUs over fast interconnects (using UCX, which can use NVLink or RDMA networks) and keeps shuffle data in GPU memory where possible, avoiding round trips through host memory and disk.

**The Spark integration.** The **RAPIDS Accelerator for Apache Spark** (sometimes loosely called "cuSpark", but that is not its name) is a Spark plugin. It inspects each physical plan and replaces supported operators with GPU versions built on cuDF, falling back to the CPU for the rest. No code change is needed for DataFrame and SQL jobs:

```bash
spark-submit \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=0.25 \
  --conf spark.rapids.sql.explain=NOT_ON_GPU \
  job.py
```

`spark.rapids.sql.explain=NOT_ON_GPU` logs every operator that stayed on the CPU and why. Each CPU-GPU boundary inserts a columnar-to-row or row-to-columnar transition and a copy, so a plan that keeps hopping back and forth can be slower than pure CPU.

**NVIDIA DALI** is a different tool for a different job: it decodes and augments images, video and audio on the GPU to feed deep-learning training, so the GPU is not starved while the CPU decodes JPEGs. It belongs to ML input pipelines (see the Data engineering for AI and ML section), not SQL-style ETL.

**When GPUs make sense, and when CPU vectorization is enough.**

- Good fits: tens of GB to TB of data per job; heavy joins, sorts, high-cardinality aggregations, string and regex work, Parquet decoding; long pipelines that stay on the GPU; feature engineering feeding GPU model training on the same machine.
- Poor fits: small data (the overhead dominates); I/O-bound jobs where reading from Cloud Storage is the bottleneck; row-at-a-time Python UDFs; operators the GPU library does not support, which cause fallbacks.
- The competition is strong: vectorized CPU engines (DuckDB, Velox, ClickHouse, BigQuery's own engine) use SIMD and cache-aware algorithms and run on cheaper machines. A GPU VM costs several times a CPU VM per hour, so the speed-up must beat the price ratio. Measure cost per job, not only time.

> **Pitfall:** Expecting the GPU to fix a job that is slow because of shuffle skew, too many small files or slow Cloud Storage reads. Those bottlenecks stay; fix them first.

> **Tip:** Run the RAPIDS qualification tool on existing Spark event logs. It estimates which jobs would benefit and which operators would fall back, before you pay for GPUs.

::: deeper
**Memory is the hard limit.** When a GPU operator's input exceeds GPU memory, engines must split work into batches or spill to host memory. The RAPIDS Accelerator spills to host and disk and limits concurrent GPU tasks (`spark.rapids.sql.concurrentGpuTasks`) to avoid out-of-memory errors. Too much concurrency causes spills; too little leaves the GPU idle.

**GPUDirect Storage and RDMA.** NVIDIA GPUDirect technologies let NVMe drives and network cards move data straight into GPU memory, skipping a bounce through host RAM. They matter most for on-premises clusters with local NVMe and InfiniBand.

**Other GPU engines.** HeavyDB (formerly OmniSci and MapD) and Kinetica are GPU-native databases; the Polars DataFrame library has a GPU engine built on cuDF; research engines such as Sirius explore GPU execution for DuckDB-style SQL. BlazingSQL, an early GPU SQL engine, is no longer developed.

**Why atomics and divergence hurt.** Threads in a warp run in lockstep, so an `if` where half the threads take each branch runs both paths (divergence). Many threads updating the same hash bucket contend on atomics. Low-cardinality group-bys (a handful of groups) can therefore be slower than expected; libraries use per-block partial aggregation to reduce contention.

> **Interview:** "When would you put an ETL job on GPUs?" When it is large and compute-heavy (joins, sorts, aggregations, string processing), its operators are supported end to end so data stays on the GPU, I/O is not the bottleneck, and the speed-up beats the higher hourly price. Verify with a qualification run and a cost-per-job comparison.
:::

::: check
?? Why can a simple filter over 10 GB be slower on a GPU than on a CPU?
The data must first cross PCIe, which is much slower than both CPU and GPU memory. For one cheap pass, the transfer costs more than the CPU's whole scan.

?? What does the RAPIDS Accelerator for Apache Spark do when it meets an operator it cannot run on the GPU?
It leaves that operator on the CPU and inserts transitions (with copies) between columnar GPU data and Spark's row format. `spark.rapids.sql.explain=NOT_ON_GPU` shows which operators fell back.

?? Name two workloads that suit GPUs and two that do not.
Suit: large joins and sorts, high-cardinality aggregations, string or regex processing, Parquet decoding in long pipelines. Do not suit: small datasets, I/O-bound jobs, row-at-a-time Python UDFs, plans full of unsupported operators.
:::

::: resources
- Docs: [RAPIDS Accelerator for Apache Spark](https://docs.nvidia.com/spark-rapids/) · NVIDIA's user guide, configuration reference and qualification tools
- Docs: [RAPIDS cuDF documentation](https://docs.rapids.ai/api/cudf/stable/) · the GPU DataFrame library underneath most of the ecosystem
- Docs: [GPUs on Compute Engine](https://cloud.google.com/compute/docs/gpus) · GPU models, machine series and regional availability on Google Cloud
- Book: [Programming Massively Parallel Processors, 4th edition](https://shop.elsevier.com/books/programming-massively-parallel-processors/hwu/978-0-323-91231-0) · Hwu, Kirk and El Hajj; how GPUs execute code, with scan, sort and histogram patterns
- Repo: [spark-rapids](https://github.com/NVIDIA/spark-rapids) · source, supported-operator lists and release notes for the Spark plugin
- Docs: [NVIDIA DALI](https://docs.nvidia.com/deeplearning/dali/user-guide/docs/) · GPU data loading and augmentation for deep-learning pipelines
:::

### GPU-Accelerated ETL — RAPIDS & cuDF

::: plain
If you already write pandas or Spark code, you can often run the same code on a GPU with a library swap or one configuration change. RAPIDS is NVIDIA's open-source family of libraries that does this: cuDF is "pandas on the GPU", and its siblings cover Spark, Dask, Polars and machine learning. It is like changing the engine in a car you already know how to drive.
:::

The previous topic, GPU-Accelerated Data Processing, explained why GPUs speed up joins, sorts and aggregations and when the PCIe transfer eats the gain. This topic is practical: which RAPIDS tool to use, what the code looks like, how to scale beyond one GPU, and how to run it on Google Cloud.

**The RAPIDS family for ETL.**

| Tool | What it is | Use it when |
|---|---|---|
| **cuDF** | A GPU DataFrame library with a pandas-like API, built on Apache Arrow's columnar memory layout | Single-GPU ETL in Python |
| **cudf.pandas** | An accelerator mode that runs your existing pandas code on the GPU and falls back to CPU pandas for anything unsupported | You have pandas code and want zero changes |
| **Polars GPU engine** | Polars' lazy queries executed by cuDF (`collect(engine="gpu")`) | You use Polars |
| **Dask with cuDF** | Partitioned DataFrames spread over many GPUs and machines | Data bigger than one GPU's memory |
| **RAPIDS Accelerator for Apache Spark** | A Spark plugin (previous topic) | Existing Spark SQL and DataFrame jobs |
| **cuML, cuGraph** | GPU machine learning and graph algorithms | The ETL feeds model training or graph analytics |

**cuDF in practice.** The API deliberately mirrors pandas:

```python
import cudf

orders = cudf.read_parquet("orders.parquet")            # decoded on the GPU
customers = cudf.read_parquet("customers.parquet")

paid = orders[orders["status"] == "paid"]
joined = paid.merge(customers, on="customer_id", how="inner")   # GPU hash join
summary = (joined.groupby(["country", "segment"])
                 .agg({"amount": ["sum", "mean"], "order_id": "count"})
                 .reset_index())
summary.to_parquet("summary.parquet")
pdf = summary.to_pandas()                               # copy back to host memory when needed
```

Every step runs on the GPU; the data crosses PCIe only when it is read and when you call `to_pandas()`. String methods (`.str.contains`, `.str.extract`), datetime operations and window-style operations (`rolling`) are also on the GPU.

**Zero-change acceleration with cudf.pandas.**

```bash
python -m cudf.pandas etl_job.py        # run an unchanged pandas script on the GPU
# In Jupyter: %load_ext cudf.pandas   before   import pandas as pd
```

Each pandas call is tried on the GPU; if cuDF cannot handle it, the data is moved to the CPU and real pandas runs it. Correctness is preserved, but a script that falls back often moves data back and forth, so profile it (`python -m cudf.pandas --profile etl_job.py` reports GPU and CPU time per call).

**Beyond one GPU with Dask.** Dask splits a DataFrame into partitions and schedules work across workers; with the cuDF backend, each partition is a cuDF DataFrame on one GPU:

```python
import dask
import dask.dataframe as dd
from dask_cuda import LocalCUDACluster
from dask.distributed import Client

client = Client(LocalCUDACluster())                   # one worker per local GPU
dask.config.set({"dataframe.backend": "cudf"})

events = dd.read_parquet("gs://my-bucket/events/")      # partitions land on GPUs
daily = events.groupby(["event_date", "event_type"])["user_id"].nunique()
daily.to_frame().to_parquet("gs://my-bucket/daily_unique_users/")
```

Joins and group-bys that need data from every partition shuffle between GPUs, ideally over NVLink within a machine.

**Running it on Google Cloud.**

- **Single machine:** a Compute Engine VM with GPUs (for example the G2 series with NVIDIA L4, or A2 and A3 with A100 and H100) using a Deep Learning VM image, or a Gemini Enterprise Agent Platform (formerly Vertex AI) Workbench notebook with a GPU attached. Install RAPIDS with `pip install cudf-cu12` (match the CUDA major version) or the RAPIDS container images.
- **Spark on a cluster:** Managed Service for Apache Spark clusters with GPU workers and the RAPIDS initialization action, or serverless Spark batches, whose runtimes already include the RAPIDS Accelerator. A serverless GPU batch:

```bash
gcloud dataproc batches submit pyspark etl_job.py --region=us-central1 \
  --properties=spark.dataproc.executor.compute.tier=premium,\
spark.dataproc.executor.disk.tier=premium,\
spark.dataproc.executor.resource.accelerator.type=l4,\
spark.executor.instances=4,\
spark.shuffle.manager=com.nvidia.spark.rapids.RapidsShuffleManager
```

GPU batches use the premium pricing tier and need GPU quota (for example `NVIDIA_L4_GPUS`) in the region.

**When GPU ETL beats CPU ETL.** In published RAPIDS benchmarks the wins come from joins, group-bys and string-heavy transformations on tens of gigabytes or more. Simple column renames, filters on small tables, or jobs dominated by reading compressed files from object storage gain little. A quick rule: if the CPU version spends most of its time in compute (high CPU use, little I/O wait) on operators cuDF supports, try the GPU; if it waits on I/O, fix I/O first.

> **Pitfall:** Out-of-memory errors because a cuDF DataFrame plus the intermediate results of a join exceed GPU memory. Filter and select columns early, use Dask or Spark to partition, or enable spilling to host memory (`CUDF_SPILL=on`).

> **Pitfall:** Mismatched CUDA, driver and RAPIDS versions. RAPIDS packages are built for a CUDA major version (`-cu12`); the NVIDIA driver must support it. Use the RAPIDS containers or a Deep Learning VM image to avoid this.

::: deeper
**Memory management with RMM.** GPU memory allocation through CUDA is slow, so RAPIDS uses the RAPIDS Memory Manager (RMM). A **pool allocator** reserves a large block once and hands out pieces, which speeds up workloads with many small allocations. Managed (unified) memory lets the GPU oversubscribe its memory by paging to host RAM, at a large speed cost.

**Arrow under the hood.** cuDF columns follow the Arrow columnar format, so data can move to other Arrow-based tools (PyArrow, DuckDB, Polars) and between GPU libraries through the DLPack and Arrow C Device interfaces without reformatting.

**Pandas compatibility limits.** cuDF differs from pandas in places: no object columns holding arbitrary Python values, some ordering differences in `groupby` results unless you sort, and row-wise `apply` with arbitrary Python functions is limited (cuDF can compile simple user functions with Numba). cudf.pandas hides most of these differences by falling back.

**Release cadence.** RAPIDS ships every two months with versions named by year and month (for example 25.10). Check the support matrix for Python, CUDA and GPU architecture before upgrading.

> **Interview:** "You have a 200 GB daily pandas job that runs for hours on a big VM. How would you speed it up with GPUs?" Profile first. If time is in joins, group-bys and string work, try cudf.pandas on one GPU with a sample; partition with Dask and the cuDF backend (or move to Spark with the RAPIDS Accelerator) because 200 GB does not fit in one GPU; push filters and column selection into the Parquet read; compare cost per run with a CPU alternative such as DuckDB or BigQuery.
:::

::: check
?? What is the difference between cuDF and cudf.pandas?
cuDF is a GPU DataFrame library with a pandas-like API that you import directly. cudf.pandas runs unchanged pandas code, using cuDF where possible and falling back to CPU pandas for anything unsupported.

?? Your data is 300 GB and a GPU has 80 GB of memory. Which RAPIDS tools let you process it?
Dask with the cuDF backend across several GPUs, or Spark with the RAPIDS Accelerator. Both partition the data and shuffle between workers.

?? Name two settings a serverless Spark GPU batch on Google Cloud needs.
The premium compute tier (`spark.dataproc.executor.compute.tier=premium`) and an accelerator type such as `spark.dataproc.executor.resource.accelerator.type=l4`, plus GPU quota in the region.
:::

::: resources
- Docs: [RAPIDS documentation](https://docs.rapids.ai/) · installation, support matrix and links to every RAPIDS library
- Docs: [cudf.pandas](https://docs.rapids.ai/api/cudf/stable/cudf_pandas/) · how the zero-code-change accelerator works, its profiler and limits
- Docs: [Use GPUs with serverless Spark](https://docs.cloud.google.com/managed-spark/docs/guides/gpus-serverless) · Google Cloud's guide to GPU batches and their required properties
- Repo: [cuDF on GitHub](https://github.com/rapidsai/cudf) · source code, examples and benchmarks
- Docs: [Dask-CUDA](https://docs.rapids.ai/api/dask-cuda/stable/) · running Dask workers on GPUs, spilling and NVLink communication
- Docs: [Polars GPU support](https://docs.pola.rs/user-guide/gpu-support/) · running Polars lazy queries on the cuDF engine
:::

## Operating system

Between your program and the hardware sits the operating system, almost always Linux for data systems. It decides when a read actually touches the disk, keeps recently used file data in memory, copies bytes between the kernel and your program, and lays files out on the device. Many famous performance claims ("Kafka is fast because it uses the page cache and zero-copy", "RocksDB uses io_uring", "too many small files kill HDFS") are really statements about this layer.

The group moves from the general I/O path (page cache, direct I/O, memory mapping, zero-copy and readiness notification), to the newest Linux I/O interface, io_uring, in detail, and finally to file systems: how files, directories and durability work, and how local, shared and distributed file systems differ from Cloud Storage.

### OS-Level I/O — mmap, sendfile, io_uring

::: plain
When a program asks for part of a file, Linux does not run to the disk every time. It keeps a big shelf of recently used file pieces in spare memory, called the **page cache**, and hands out copies from the shelf when it can. Writes are also put on the shelf first and taken to the disk a little later. Different ways of reading and writing (normal reads, mapping a file into memory, sending a file straight to the network) decide how many times the bytes get copied on their way, and every copy costs time.
:::

This topic is a map of the whole Linux I/O path and of the main ways to use it. The next topic, on io_uring as a high-performance asynchronous interface, zooms into one of them in detail.

**The page cache.** Linux caches file data in otherwise unused RAM, in 4 KiB pages. A `read()` that finds its pages cached is a memory copy; a miss triggers device I/O. The kernel also does **readahead**: when it sees sequential reads, it fetches the next chunk before you ask. Writes are **buffered**: `write()` copies data into the page cache, marks the pages **dirty**, and returns. Kernel flusher threads write dirty pages to disk later, driven by `vm.dirty_background_ratio` and `vm.dirty_ratio` (or their `_bytes` versions). Only `fsync()` (or `fdatasync()`) guarantees the data reached stable storage.

```bash
free -h                    # the buff/cache column is mostly page cache: memory in use, but reclaimable
cat /proc/meminfo | grep -E 'Dirty|Writeback'
fincore --bytes /var/lib/kafka/data/orders-0/*.log   # how much of each file is cached (util-linux)
```

> **Pitfall:** Seeing little "free" memory on a Kafka or database host and adding RAM or restarting. Page cache shows up as used but is given back on demand. Look at "available" instead.

**Direct I/O.** Opening a file with `O_DIRECT` bypasses the page cache: data moves by DMA between the device and your buffer, which must be aligned (usually to 4 KiB). Databases that manage their own cache use it to avoid caching data twice (**double buffering**) and to control exactly when data hits the disk: MySQL InnoDB (`innodb_flush_method=O_DIRECT`), ScyllaDB, and RocksDB optionally (`use_direct_reads`). PostgreSQL traditionally relies on the page cache in addition to its own `shared_buffers`.

| Approach | Who caches | Good for | Watch out for |
|---|---|---|---|
| Buffered I/O | The kernel | Most programs, streaming logs (Kafka) | Double caching, unpredictable writeback stalls |
| Direct I/O | Your program | Databases with their own buffer pool | Alignment rules; you must do your own readahead |
| `mmap` | The kernel, through page faults | Read-mostly files, indexes | Error handling, write control, stalls on faults |

**mmap.** `mmap()` maps a file into your address space, so file bytes look like an ordinary array. Touching a page that is not yet loaded causes a **page fault**; the kernel reads it and resumes your code. There is no explicit `read()` and no copy into a private buffer:

```python
import mmap, os

path = "events.log"
with open(path, "wb") as f:
    f.write(b"2026-11-01 login\n2026-11-01 click\n2026-11-02 logout\n")

with open(path, "r+b") as f, mmap.mmap(f.fileno(), 0) as m:
    print(len(m))                       # 52
    print(m[:10])                       # b'2026-11-01'
    pos = m.find(b"logout")             # searched in place, no read() into a buffer
    print(pos)                          # 45
    m[pos:pos + 6] = b"LOGOUT"          # writes go to the page cache, flushed later
    m.flush()                           # msync: ask the kernel to write the dirty pages

print(open(path, "rb").read().splitlines()[-1])   # b'2026-11-02 LOGOUT'
```

Kafka memory-maps its small offset and time **index** files; LMDB is built entirely on `mmap`; Arrow and Parquet readers can memory-map local files for zero-copy access. But `mmap` is a poor fit for a database's main data files: the kernel decides when dirty pages are written (breaking write-ahead-log ordering unless you work hard), an I/O error arrives as a `SIGBUS` signal instead of an error code, a page fault blocks the thread with no way to overlap I/O, and evicting pages on many cores triggers expensive TLB shootdowns. The 2022 paper "Are You Sure You Want to Use MMAP in Your Database Management System?" documents these problems.

**sendfile and zero-copy.** Serving a file over the network the naive way (`read()` then `write()` to a socket) copies the data four times: disk to page cache (DMA), page cache to your buffer (CPU), your buffer to the socket buffer (CPU), socket buffer to the network card (DMA), with four switches between user and kernel mode. `sendfile()` tells the kernel to move data from a file to a socket directly; with scatter-gather DMA, the CPU copies disappear entirely.

[[fig:hardware_zerocopy]]

This is how Kafka serves consumers: log segments are written sequentially into the page cache, and fetch requests use Java's `FileChannel.transferTo`, which calls `sendfile` on Linux. Consumers that are caught up read data that is still in the page cache, so a broker can serve them without touching the disk or copying through the JVM heap.

> **Pitfall:** Enabling TLS on Kafka listeners disables this zero-copy path, because the broker must encrypt the bytes in user space before sending them. Expect higher broker CPU with TLS and size brokers accordingly (the PySpark + Kafka guide's [Kafka storage internals](pyspark-streaming-kafka-topic-map.html#kstorage) covers this).

**epoll for network I/O.** A server with thousands of connections cannot dedicate a thread to each. `epoll` lets one thread register many sockets and ask "which ones are ready to read or write now?". It is **readiness-based**: it tells you an operation will not block, and you then perform it. Java NIO selectors (and so Kafka's network threads and Netty) use `epoll` on Linux. It does not help regular files, which always report "ready" even if a read will wait for the disk. That gap is one of the reasons io_uring exists.

**io_uring in one paragraph.** io_uring (Linux 5.1 and later) is a **completion-based** asynchronous interface: the program places requests in a ring buffer shared with the kernel and later collects results from a second ring, for files and sockets alike, often with no system call per operation. The next topic covers it in depth.

::: deeper
**Writeback stalls.** When dirty pages exceed `vm.dirty_ratio`, writing processes are throttled until writeback catches up. Large ratios on hosts with lots of RAM can cause multi-second stalls when a burst of dirty data is flushed. Some database and Kafka tuning guides lower the background ratio so writeback starts earlier and smoother.

**fsync semantics.** `fsync` on a file does not make a newly created or renamed file's directory entry durable; you also need to `fsync` the directory. In 2018 PostgreSQL developers found that after a failed `fsync`, Linux could drop the dirty pages and report success on retry ("fsyncgate"); PostgreSQL now treats an `fsync` failure as fatal and recovers from its write-ahead log.

**Other zero-copy calls.** `splice()` moves data between a file descriptor and a pipe without copying to user space; `copy_file_range()` copies between files inside the kernel (and on some file systems as a metadata-only clone); `MSG_ZEROCOPY` lets `send()` avoid the user-to-kernel copy for large buffers.

**Kafka and the page cache.** Kafka's design document argues for keeping data in the OS cache rather than the JVM heap: no GC pressure, a warm cache survives broker restarts, and the kernel's readahead and writeback are well tuned for sequential logs. The consequence for operations is to leave most of a broker's RAM unallocated so the page cache can hold recent segments.

> **Interview:** "Why is Kafka fast?" Sequential appends to log segments, batching and compression of records, reliance on the OS page cache instead of a heap cache, and zero-copy transfer from the page cache to the socket with `sendfile`. Mention that TLS removes the zero-copy benefit.
:::

::: check
?? What does `fsync()` guarantee that `write()` does not?
`write()` only copies data into the page cache and marks it dirty. `fsync()` returns only after the file's data (and needed metadata) has reached stable storage.

?? Why do some databases open their data files with `O_DIRECT`?
They have their own buffer pool, so bypassing the page cache avoids caching data twice and gives them control over when data is written.

?? How does `sendfile` make Kafka consumers cheap to serve, and what breaks it?
It copies log data from the page cache to the socket inside the kernel, with no copy through the JVM. TLS breaks it, because data must be encrypted in user space first.
:::

::: resources
- Docs: [Kafka design: persistence and efficiency](https://kafka.apache.org/documentation/#design_filesystem) · Kafka's own explanation of the page cache and zero-copy choices
- Paper: [Are You Sure You Want to Use MMAP in Your Database Management System?](https://db.cs.cmu.edu/mmap-cidr2022/) · Crotty, Leis and Pavlo, CIDR 2022; the case against mmap for DBMS data files
- Docs: [sendfile(2) manual page](https://man7.org/linux/man-pages/man2/sendfile.2.html) · the zero-copy system call and its limits
- Docs: [epoll(7) manual page](https://man7.org/linux/man-pages/man7/epoll.7.html) · readiness notification, edge- and level-triggered modes
- Book: [The Linux Programming Interface](https://man7.org/tlpi/) · Michael Kerrisk; the file I/O, buffering and memory mapping chapters
- Article: [Why you should use io_uring for network I/O](https://developers.redhat.com/articles/2023/04/12/why-you-should-use-iouring-network-io) · Red Hat Developer; a readable comparison of epoll and io_uring
:::

### io_uring — High-Performance Async I/O

::: plain
Imagine ordering at a busy café. The old way: you order, then stand at the counter until your coffee is ready, and only then order the next thing. io_uring works like a pair of trays: you put a slip for every order on the "orders" tray, go back to your table and do other work, and finished items appear on the "done" tray, each labelled with your slip. One trip can carry many orders, and you never stand waiting.
:::

The previous topic, on OS-level I/O, placed io_uring in the overall Linux I/O path. This topic is about how the interface itself works, why it beats the older asynchronous APIs, and where data systems use it today.

**Why a new interface.** Before io_uring, Linux had three options for overlapping I/O with work, all flawed for storage-heavy systems:

| Interface | Model | Limitation |
|---|---|---|
| Blocking `read`/`write` plus threads | One thread waits per request | Thread switching and memory overhead at high queue depths |
| `epoll` | Readiness: "this socket can be read now" | Useless for regular files; still one system call per read |
| Linux AIO (`io_submit`) | Completion-based | Truly async only with `O_DIRECT`; buffered I/O may silently block; few operation types |

NVMe drives can serve hundreds of thousands of requests per second, and at that rate the cost of one or two system calls per request becomes the bottleneck. io_uring, added in Linux 5.1 (2019) by Jens Axboe, removes it.

**Two rings shared with the kernel.** At setup (`io_uring_setup`), the kernel and the program map two circular buffers into shared memory:

- The **submission queue (SQ)** holds **submission queue entries (SQEs)**. Each SQE describes one operation: an opcode (read, write, fsync, accept, send, recv, openat...), a file descriptor, a buffer, an offset, flags, and a 64-bit `user_data` tag of your choice.
- The **completion queue (CQ)** holds **completion queue entries (CQEs)**: the `user_data` of the finished request and its result (bytes transferred, or a negative error code).

[[fig:hardware_uring]]

The program writes SQEs and moves the SQ tail; one call to `io_uring_enter` submits all of them and can also wait for completions. The kernel writes CQEs and moves the CQ tail; the program reads them and moves the CQ head. Because the rings are shared, submitting 64 reads costs one system call instead of 64, and reaping completions costs none.

A minimal read with liburing, the helper library most programs use:

```c
#include <liburing.h>
#include <fcntl.h>
#include <stdio.h>

int main(void) {
    struct io_uring ring;
    io_uring_queue_init(64, &ring, 0);              /* rings with 64 entries */

    int fd = open("part-0001.parquet", O_RDONLY);
    static char buf[1 << 20];

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    io_uring_prep_read(sqe, fd, buf, sizeof buf, 0);  /* read 1 MiB at offset 0 */
    io_uring_sqe_set_data64(sqe, 42);                 /* our tag for this request */
    io_uring_submit(&ring);                           /* one syscall, could carry many SQEs */

    struct io_uring_cqe *cqe;
    io_uring_wait_cqe(&ring, &cqe);
    printf("request %llu read %d bytes\n",
           (unsigned long long)io_uring_cqe_get_data64(cqe), cqe->res);
    io_uring_cqe_seen(&ring, cqe);                    /* advance the CQ head */
    io_uring_queue_exit(&ring);
    return 0;
}
```

**Features that matter for performance.**

- **Batching:** queue many SQEs, submit once.
- **SQPOLL mode:** a kernel thread polls the submission queue, so a busy program submits with no system calls at all, at the cost of a CPU core spinning.
- **IOPOLL mode:** completions for NVMe devices opened with `O_DIRECT` are polled instead of interrupt-driven, lowering latency.
- **Registered (fixed) files and buffers:** `io_uring_register_files` and `io_uring_register_buffers` let the kernel look up file descriptors and pin buffer memory once, instead of on every request.
- **Linked requests:** the `IOSQE_IO_LINK` flag chains SQEs, for example "write, then fsync", executed in order without a round trip.
- **Multishot operations and provided buffers:** one `accept` or `recv` request can produce many completions, and the kernel picks receive buffers from a ring you supply, which suits servers with many connections.
- **Zero-copy send** (`IORING_OP_SEND_ZC`) for large network transfers.

**Where data systems use it.**

- **RocksDB** uses io_uring to issue the many block reads of `MultiGet` and asynchronous scans in parallel (the `async_io` read option), which cuts latency for batched lookups.
- **PostgreSQL 18** introduced asynchronous I/O with the `io_method` setting; `io_uring` is one option on Linux (the default is `worker`, which uses background I/O processes).
- **ScyllaDB** (through the Seastar framework), **TigerBeetle**, QEMU and the `fio` benchmark tool all support io_uring backends; ClickHouse can read local files with it.
- Rust and Java runtimes have io_uring backends (tokio-uring, glommio, Netty's io_uring transport) for servers.

> **Pitfall:** Assuming io_uring is available everywhere. Because io_uring has been a frequent source of kernel security bugs, some environments restrict it: Google disabled it on ChromeOS and its production servers and limits it on Android, Linux 6.6 added the `kernel.io_uring_disabled` sysctl, and some container runtimes' default seccomp profiles block its system calls. Software should fall back to another I/O method when setup fails.

> **Tip:** Compare engines on your own disks with `fio`: run the same job with `--ioengine=psync`, `--ioengine=libaio` and `--ioengine=io_uring` at several `--iodepth` values.

::: deeper
**Memory ordering.** The rings are lock-free single-producer, single-consumer queues. Each side must use acquire and release memory barriers when reading and updating head and tail indices; liburing does this for you, which is the main reason to use it rather than raw system calls.

**CQ overflow.** If completions arrive faster than the program reaps them, the CQ fills. Modern kernels hold overflowed completions internally and set a flag rather than dropping them, but a program should size the CQ (it defaults to twice the SQ) and reap promptly.

**Buffered I/O through io_uring.** Unlike Linux AIO, io_uring handles buffered reads asynchronously: if data is in the page cache, the read completes inline; if not, the kernel uses internal worker threads (io-wq) or asynchronous page-cache callbacks, so the submitting thread never blocks.

**Is it always faster?** For a few large sequential reads, the difference from plain `read()` is small; the gain shows at high request rates and queue depths, where syscall and context-switch overhead dominate. Gains also depend on the kernel version, because many features (multishot, zero-copy send, provided buffer rings) arrived in 5.x and 6.x releases.

> **Interview:** "How is io_uring different from epoll?" epoll is readiness-based and only useful for sockets and pipes: it tells you an operation will not block, then you make the call. io_uring is completion-based and works for files and sockets: you submit the operation itself through a shared ring and collect its result later, batching many operations per system call or none with SQPOLL.
:::

::: check
?? What are SQEs and CQEs, and how does a program match a completion to its request?
Submission queue entries describe requested operations; completion queue entries report results. Each SQE carries a 64-bit `user_data` value that the kernel copies into the matching CQE.

?? Why was Linux AIO not enough for databases using buffered I/O?
It was only reliably asynchronous with `O_DIRECT`; buffered requests could block the caller, and it supported few operation types.

?? What do registered (fixed) buffers save?
The per-request work of pinning user memory and mapping it for the kernel; the buffers are registered once and referenced by index.
:::

::: resources
- Paper: [Efficient IO with io_uring](https://kernel.dk/io_uring.pdf) · Jens Axboe's design document explaining the rings and the motivation
- Repo: [liburing](https://github.com/axboe/liburing) · the helper library, examples and man pages for every operation
- Docs: [io_uring(7) manual page](https://man7.org/linux/man-pages/man7/io_uring.7.html) · the kernel interface: rings, SQEs, CQEs and system calls
- Article: [Lord of the io_uring](https://unixism.net/loti/) · a step-by-step tutorial from basic reads to a web server
- Docs: [PostgreSQL asynchronous I/O settings](https://www.postgresql.org/docs/current/runtime-config-resource.html) · the `io_method` and related settings introduced in PostgreSQL 18
- Blog: [Learnings from kCTF VRP's 42 Linux kernel exploits submissions](https://security.googleblog.com/2023/06/learnings-from-kctf-vrps-42-linux.html) · Google Security Blog on why io_uring was restricted
:::

### File System Internals for Data Engineers

::: plain
A file system is the librarian of a disk. The disk itself only knows numbered blocks; the file system keeps the catalogue that says which blocks belong to which file, what the file is called, who may read it, and where it sits in the folder tree. A good librarian also keeps a notebook of half-finished changes (a journal) so that, after a power cut, the catalogue never ends up half updated.
:::

**Inodes and directories.** On Unix file systems, each file has an **inode**: a record holding its size, owner, permissions, timestamps and the location of its data blocks (as **extents**, ranges of contiguous blocks, in ext4 and XFS). The inode does not hold the name. A **directory** is a special file mapping names to inode numbers. That is why a file can have several names (hard links) and why renaming within a file system is a quick metadata change:

```python
import os

with open("data.csv", "w") as f:
    f.write("id,amount\n1,40\n")
os.link("data.csv", "data_alias.csv")         # a hard link: a second name, same inode

a, b = os.stat("data.csv"), os.stat("data_alias.csv")
print(a.st_ino == b.st_ino)                   # True: both names point to one inode
print(a.st_nlink)                             # 2: the inode counts its names
os.remove("data.csv")                         # removes a name, not the data
print(open("data_alias.csv").read().splitlines())   # ['id,amount', '1,40']
print(os.stat("data_alias.csv").st_nlink)     # 1
```

Each file costs an inode plus at least one block, and every open, list or stat is a metadata operation. Millions of tiny files therefore exhaust inodes (`df -i` shows usage) and turn a scan into a metadata storm. This is the local version of the small-files problem that hurts HDFS, Spark and lakehouse tables.

**Journaling.** Updating a file touches several structures (inode, block bitmap, directory). A crash in the middle could leave them inconsistent. A **journaling** file system first writes the intended changes to a log (the journal), then applies them; after a crash it replays or discards journal entries instead of scanning the whole disk.

- **ext4**: the long-time Linux default. Its default `data=ordered` mode journals metadata and writes file data before the metadata that points to it; `data=journal` also journals data (safer, slower); `data=writeback` gives the weakest ordering.
- **XFS**: journals metadata, splits the disk into **allocation groups** that can be updated in parallel, and handles very large files and directories well. It is the default on Red Hat Enterprise Linux and a common choice for Kafka, Cassandra and HDFS data disks.

**Copy-on-write file systems.** **ZFS** and **Btrfs** never overwrite live data: a change writes new blocks and then atomically switches the pointer to them. They do not need a classic journal (ZFS keeps an intent log for synchronous writes). Copy-on-write makes **snapshots** nearly free and enables end-to-end **checksums**, which detect silent corruption, plus built-in compression (LZ4, Zstd). The costs are fragmentation under random overwrites (databases on ZFS often tune the record size to the database page size) and more write amplification.

| File system | Strengths | Typical data use |
|---|---|---|
| ext4 | Mature, simple, predictable | General purpose, boot disks |
| XFS | Parallel allocation, large files, scales with cores | Kafka logs, HDFS data nodes, databases |
| ZFS | Checksums, snapshots, compression, integrated RAID | Storage servers, PostgreSQL with snapshots |
| Btrfs | Snapshots, checksums, subvolumes | Desktops, some container hosts |

**RAID for data nodes.** RAID combines disks: **RAID 0** stripes for speed with no redundancy, **RAID 1** mirrors, **RAID 5** and **RAID 6** add one or two parity blocks per stripe, **RAID 10** stripes over mirrors. Distributed systems that already replicate data across machines (HDFS with three replicas, Kafka with replication factor 3) usually prefer **JBOD** ("just a bunch of disks"): each disk is mounted separately and listed in the system's configuration (`dfs.datanode.data.dir`, Kafka's `log.dirs`). One failed disk then loses only its replicas, which the cluster re-creates, and there is no parity overhead or slow RAID rebuild.

**Shared file systems.** **NFS** exports a file system over the network to many clients; its "close-to-open" consistency means one client sees another's writes reliably only after the writer closes and the reader reopens the file. **SMB/CIFS** is the Windows equivalent. On Google Cloud, Filestore provides managed NFS, and Google Cloud NetApp Volumes provides NFS and SMB. Shared file systems suit tools that expect POSIX paths (legacy ETL, home directories, some ML training), not high-throughput analytics.

**Distributed and parallel file systems.** **Lustre** and **IBM Storage Scale (formerly GPFS)** stripe each file across many storage servers and keep metadata on dedicated servers, giving one POSIX namespace with aggregate throughput of hundreds of GB/s; they come from high-performance computing and now serve AI training. Google Cloud offers Google Cloud Managed Lustre. **HDFS** (covered in The Hadoop ecosystem) is distributed but not POSIX: files are append-only.

**Object storage is not a file system.** Cloud Storage stores immutable objects in a flat namespace: no in-place updates, no appends (you compose or rewrite), and in a standard bucket a "directory rename" is a copy and delete of every object. Buckets with **hierarchical namespace** enabled add real folders with atomic rename, which helps Spark and Hadoop jobs that commit output by renaming. Cloud Storage FUSE mounts a bucket as a file system but cannot hide these semantics: random writes and renames remain slow.

```bash
df -hT /data && df -i /data            # space and inode usage, with file system type
stat part-0001.parquet                 # inode number, size, blocks, timestamps
sudo mkfs.xfs /dev/nvme0n1             # format a Local SSD as XFS
sudo mount -o noatime /dev/nvme0n1 /mnt/shuffle   # skip access-time updates on every read
```

> **Pitfall:** Writing thousands of tiny output files per job. Each costs an inode, a metadata update and, on Cloud Storage, a separate request. Coalesce outputs to files of roughly 128 MiB to 1 GiB.

> **Tip:** Mount data disks with `noatime`. Otherwise every read updates the inode's access time, turning reads into small writes.

::: deeper
**Delayed allocation and extents.** ext4 and XFS delay choosing blocks until writeback, so they can allocate large contiguous extents for files written sequentially. Preallocating with `fallocate` (Kafka can preallocate segment files with `log.preallocate`) reduces fragmentation further.

**Crash consistency for applications.** Journaling protects the file system's own structures, not your file's contents. The safe pattern to replace a file atomically is: write a temporary file, `fsync` it, `rename` it over the target, then `fsync` the directory. Lakehouse table formats such as Iceberg and Delta achieve the same atomic commit on object storage with a single metadata pointer swap or conditional write instead of a rename.

**RAID 5 write hole.** If power fails between writing data and parity, the stripe becomes inconsistent and a later rebuild silently produces corrupt data. Hardware controllers use battery-backed caches; ZFS's RAID-Z avoids the problem with copy-on-write full-stripe writes.

**Metadata scale.** Single-metadata-server designs (the HDFS NameNode, Lustre's metadata server per file system) cap the number of files. Lakehouse formats move file listings into metadata files (Iceberg manifests, Delta logs) partly to avoid listing millions of objects.

> **Interview:** "Why do Kafka and HDFS recommend JBOD rather than RAID?" They replicate data across machines already, so RAID's redundancy is duplicated cost. JBOD gives full raw capacity and throughput, and a disk failure only loses replicas the cluster re-creates, without a long RAID rebuild that degrades the whole node.
:::

::: check
?? What does an inode store, and what does it not store?
Metadata (size, owner, permissions, timestamps) and the locations of the file's data blocks. It does not store the file name; directories map names to inode numbers.

?? Why does renaming a directory of Parquet files behave differently on a local XFS disk and in a standard Cloud Storage bucket?
On XFS it is one atomic metadata operation. In a standard Cloud Storage bucket there are no real directories, so it becomes a copy and delete of every object, which is slow and not atomic. Hierarchical namespace buckets add atomic folder rename.

?? What advantage do copy-on-write file systems such as ZFS bring for data integrity?
They never overwrite live blocks and keep checksums for all data, so they detect silent corruption and can take consistent snapshots cheaply.
:::

::: resources
- Book: [Systems Performance, 2nd edition](https://www.oreilly.com/library/view/systems-performance-2nd/9780136821694/) · Brendan Gregg; the file systems and disks chapters, with analysis tools
- Book: [Operating Systems: Three Easy Pieces](https://pages.cs.wisc.edu/~remzi/OSTEP/) · Arpaci-Dusseau; free chapters on file system implementation, journaling and crash consistency
- Docs: [ext4 data structures and algorithms](https://docs.kernel.org/filesystems/ext4/index.html) · the kernel's description of ext4 inodes, extents and journaling
- Docs: [Cloud Storage hierarchical namespace](https://cloud.google.com/storage/docs/hns-overview) · folders and atomic rename in Cloud Storage buckets
- Docs: [Google Cloud Managed Lustre](https://cloud.google.com/managed-lustre/docs/overview) · Google Cloud's parallel file system for high-throughput workloads
- Paper: [All File Systems Are Not Created Equal](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai) · OSDI 2014; how applications get crash consistency wrong
:::

## Bytes on disk and on the wire

The first two groups covered where bytes live and how they move. This group is about the bytes themselves: how to make them smaller (compression and encodings), how to lay out records so different programs can read them (serialization), how to answer questions about huge sets with tiny summaries (Bloom filters and sketches), and how to keep them secret (encryption).

These ideas are inside every file format and engine you use. A Parquet file is dictionary- and run-length-encoded pages, compressed with Snappy or Zstd, described by a Thrift footer, optionally with Bloom filters and modular encryption. BigQuery's `APPROX_COUNT_DISTINCT` is HyperLogLog. Pub/Sub schemas are Avro or Protocol Buffers. Knowing the internals lets you choose settings on purpose and explain why one table is ten times smaller than another.

### Compression Algorithms Deep Dive

::: plain
Compression finds repetition and writes it down more briefly, like writing "the same as above" instead of copying a long address again. Some methods are quick and only spot obvious repeats; others think harder and shrink data more, but take longer. Choosing a codec is choosing where you want to spend: CPU time to squeeze, or disk space and network time to move the bigger file.
:::

**How general-purpose codecs work.** Almost all of them combine two steps. First, an **LZ77-style** match finder replaces a repeated byte sequence with a short reference: "copy 12 bytes from 340 bytes back". Second, an **entropy coder** gives frequent symbols shorter bit codes (Huffman coding, or finite-state entropy in Zstd). Fast codecs skip or simplify the second step and search less hard for matches.

| Codec | Design | Speed | Ratio | Where you meet it |
|---|---|---|---|---|
| LZ4 | LZ77 only, very simple format | Fastest; decompression several GB/s per core | Lowest | Kafka, ClickHouse default, RocksDB, in-memory caches |
| Snappy | LZ77 only, by Google | Very fast | Low | Spark's default Parquet codec, Bigtable heritage, Kafka |
| Zstd (Zstandard) | LZ77 plus FSE and Huffman, levels from negative to 22 | Fast at low levels, slow at high | High; tunable | Parquet, Kafka, ORC, RocksDB bottom levels, backups |
| Gzip / Deflate | LZ77 plus Huffman (1990s) | Slow to compress, moderate to decompress | Medium | CSV and JSON files, HTTP, legacy pipelines |
| Brotli | LZ77 plus Huffman with context modelling and a built-in dictionary | Slow at high levels | High for text | Web content, some Parquet files |
| xz / LZMA, bzip2 | Heavy match search, range coding or BWT | Slowest | Highest | Archives, rarely in pipelines |

As a rough guide, the Zstandard project's published benchmark on one desktop core shows LZ4 and Snappy compressing at hundreds of MB/s with ratios near 2.1, Zstd level 1 at about 500 MB/s with a ratio near 2.9, and zlib level 1 at about 100 MB/s with a ratio near 2.7. Decompression is where LZ4 shines (several GB/s) and where Zstd stays fast at every level.

You can feel the trade-off with Python's standard library, which includes Zstd since 3.14:

```python
import bz2, lzma, random, zlib
from compression import zstd           # in the standard library since Python 3.14

random.seed(7)
countries = ["IN", "NO", "DE", "US", "BR"]
rows = [f"{i},{random.choice(countries)},{random.randint(1, 500)}" for i in range(50_000)]
data = "\n".join(rows).encode()

codecs = {
    "zlib level 1": lambda b: zlib.compress(b, 1),
    "zlib level 9": lambda b: zlib.compress(b, 9),
    "zstd level 3": lambda b: zstd.compress(b, 3),
    "zstd level 19": lambda b: zstd.compress(b, 19),
    "bz2": bz2.compress,
    "lzma (xz)": lzma.compress,
}
for name, fn in codecs.items():
    print(f"{name:14} ratio {len(data) / len(fn(data)):4.1f}x")
# Ratios depend on the library versions, so they are not shown here; zstd and lzma
# at high levels compress best, zlib level 1 compresses least.

print(zstd.decompress(zstd.compress(data)) == data)    # True: lossless
```

**What determines the ratio is mostly the data.** Sorting and grouping similar values together helps more than any codec level:

```python
import random, zlib

random.seed(1)
status = [random.choice(["paid", "refunded", "cancelled"]) for _ in range(30_000)]
unsorted_size = len(zlib.compress("\n".join(status).encode()))
sorted_size = len(zlib.compress("\n".join(sorted(status)).encode()))
print(sorted_size < unsorted_size / 10)      # True: sorted data compresses over 10x better
```

That is why BigQuery clustering, Iceberg sort orders and `ORDER BY` before writing Parquet shrink files and speed scans.

**Columnar-specific compression.** Column stores first apply **encodings** that exploit the type and order of a column (run-length encoding, bit-packing, delta, dictionary: the next topic, Data Encoding Techniques, covers them), then optionally a general codec on top. Encodings are cheap to decode and some can be queried without decoding; the general codec squeezes what is left.

**Choosing a codec for data engineering.**

- **Parquet and ORC files in a lake:** Zstd (level 1 to 3) is the modern default choice: usually noticeably smaller than Snappy at similar decode speed. Snappy remains fine and is Spark's default. Set it in Spark with `spark.sql.parquet.compression.codec=zstd`.
- **Kafka topics:** `compression.type=zstd` or `lz4` on the producer. Compression applies to whole record batches, so larger batches (`linger.ms`, `batch.size`) compress better.
- **Files exchanged as CSV or JSON:** gzip is universal but **not splittable**: one gzipped 20 GB file is read by one task. Prefer many medium files, or a splittable format.
- **BigQuery:** storage is compressed internally (and physical-bytes storage billing charges for the compressed size). For exports, choose the codec per format:

```sql
EXPORT DATA OPTIONS (
  uri = 'gs://my-bucket/exports/orders-*.parquet',
  format = 'PARQUET',
  compression = 'ZSTD',
  overwrite = true
) AS
SELECT * FROM `shop.orders` WHERE order_date = '2026-11-01';
```

> **Pitfall:** Loading large gzipped CSV or JSON files into BigQuery or Spark and wondering why it is slow. Compressed CSV and JSON cannot be split, so each file is read serially, and BigQuery limits the size of compressed CSV and JSON files more tightly than uncompressed ones. Use Parquet or Avro, or many smaller files.

> **Tip:** Measure on your own data with the actual engine. Ratios and speeds vary widely between log text, numeric metrics and already-compressed content such as images, which do not compress further.

::: deeper
**Zstd features worth knowing.** Levels 1 to 19 trade speed for ratio, levels 20 to 22 need `--ultra` and much memory, and negative levels ("fast") approach LZ4 speed. **Dictionary training** (`zstd --train samples/*`) builds a dictionary from sample data, which dramatically improves the ratio for many small, similar messages (JSON events), because small inputs give the compressor little history to match against. **Long-distance matching** (`--long`) finds repeats far apart in very large files.

**Block size matters.** Parquet compresses each page (often around 1 MiB) separately, and Kafka compresses each batch; codecs cannot find repeats across blocks. Very small pages or batches compress poorly.

**Splittable compression.** Bzip2 is splittable by design; block-based formats (Parquet, ORC, Avro container files) are splittable regardless of codec because they compress blocks independently. "Splittable gzip" requires an index or a special layout such as BGZF.

**Decompression cost lands on readers.** A file is written once and read many times, so it can pay to compress harder at write time (for example Zstd level 9 for cold archives) when the decode speed stays high, as Zstd's does.

**Hardware offload.** Some CPUs (Intel QuickAssist and the In-Memory Analytics Accelerator on recent Xeons) and SmartNICs offer compression offload; support in mainstream data tools is still limited.

> **Interview:** "Snappy or Zstd for Parquet?" Zstd at a low level usually gives noticeably smaller files at comparable decompression speed, which saves storage and I/O; Snappy is the historical default with the lowest CPU cost. The bigger win is sorting or clustering data so encodings work well.
:::

::: check
?? What are the two main steps inside codecs such as Zstd and gzip?
An LZ77-style match finder that replaces repeated sequences with back-references, and an entropy coder (Huffman or finite-state entropy) that gives frequent symbols shorter codes.

?? Why is a single 20 GB gzipped CSV file a problem for Spark or BigQuery loads?
Gzip is not splittable, so the file can only be decompressed serially by one worker. Split it into many files or use a block-compressed format such as Parquet.

?? Besides choosing a better codec, what is the most effective way to make a Parquet table smaller?
Sort or cluster the data by columns with repeated values, so similar values sit together and encodings plus compression find long runs.
:::

::: resources
- Docs: [Zstandard](https://facebook.github.io/zstd/) · the project page with benchmarks, levels and dictionary compression
- Blog: [Smaller and faster data compression with Zstandard](https://engineering.fb.com/2016/08/31/core-infra/smaller-and-faster-data-compression-with-zstandard/) · Meta Engineering's introduction to how Zstd works
- Docs: [LZ4](https://lz4.org/) · the LZ4 project, format description and speed figures
- Docs: [Parquet compression](https://parquet.apache.org/docs/file-format/data-pages/compression/) · the codecs Parquet supports and how pages are compressed
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) · Martin Kleppmann; column compression in the storage chapter
- Docs: [Python compression.zstd](https://docs.python.org/3/library/compression.zstd.html) · the standard-library Zstd module used in the example
:::

### Data Encoding Techniques

::: plain
Before a column store compresses data, it **encodes** it: it rewrites the values in a shape that is smaller and easier to compress, using what it knows about the column. A column that says "paid" ten thousand times in a row can be stored as "paid, ten thousand times". A column of order numbers that each go up by one or two can be stored as the first number plus small steps. These tricks are cheap to undo, and some let the engine answer questions without undoing them at all.
:::

**Why encodings come before compression.** General codecs see bytes; encodings see typed values in order. They are lightweight (a few CPU instructions per value), often decodable with SIMD, and some support operating directly on the encoded form (a filter on a dictionary-encoded column compares small integer codes instead of strings). The common ones, all runnable here:

```python
from itertools import groupby

def rle(values):
    return [(v, len(list(run))) for v, run in groupby(values)]

def delta(values):
    return [values[0]] + [b - a for a, b in zip(values, values[1:])]

def bits_needed(values):
    return max(values).bit_length() if max(values) > 0 else 0

status = ["paid"] * 5 + ["refunded"] * 2 + ["paid"] * 3
print(rle(status))            # [('paid', 5), ('refunded', 2), ('paid', 3)]

# Dictionary encoding: store each distinct string once, then small integer codes
dictionary = sorted(set(status))
codes = [dictionary.index(s) for s in status]
print(dictionary, codes)      # ['paid', 'refunded'] [0, 0, 0, 0, 0, 1, 1, 0, 0, 0]
print(bits_needed(codes))     # 1: each code fits in one bit

# Delta encoding of sorted order ids, then bit-packing the small deltas
ids = [100_000, 100_003, 100_004, 100_009, 100_010]
print(delta(ids))             # [100000, 3, 1, 5, 1]
print(bits_needed(ids), bits_needed(delta(ids)[1:]))   # 17 3

# Delta-of-delta for timestamps taken every 60 seconds, with one late reading
ts = [1_767_225_600, 1_767_225_660, 1_767_225_720, 1_767_225_781, 1_767_225_840]
d1 = delta(ts)[1:]
print(d1)                     # [60, 60, 61, 59]
print(delta(d1))              # [60, 0, 1, -2]

# Frame of reference: subtract the block minimum, then bit-pack the offsets
prices = [1012, 1009, 1015, 1010]
base = min(prices)
print(base, [p - base for p in prices], bits_needed([p - base for p in prices]))   # 1009 [3, 0, 6, 1] 3
```

| Encoding | Idea | Works best on |
|---|---|---|
| Run-length (RLE) | Store (value, count) for runs of equal values | Sorted or clustered low-cardinality columns |
| Bit-packing | Store each integer in only as many bits as the largest needs | Small integers: codes, deltas, flags |
| Dictionary | Store distinct values once and replace each value by a code | Strings with few distinct values (country, status) |
| Delta | Store differences between consecutive values | Sorted keys, increasing counters |
| Delta-of-delta | Store differences of differences | Regular timestamps (time series) |
| Frame of reference (FOR) | Subtract a per-block base value, bit-pack the offsets | Numbers clustered in a narrow range |
| FSST | Replace frequent 1 to 8 byte substrings with 1-byte codes from a small table | Strings such as URLs, emails, product names |

**Parquet's encodings.** Parquet stores the dictionary in a dictionary page and the codes with a **hybrid RLE and bit-packing** encoding (`RLE_DICTIONARY`). It also uses RLE and bit-packing for definition and repetition levels (how it represents nulls and nesting), `DELTA_BINARY_PACKED` for integers, `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY` (shared prefixes) for strings, and `BYTE_STREAM_SPLIT` for floating-point data, which reorders bytes so a general codec compresses them better. Writers choose dictionary encoding first and **fall back** to plain encoding when the dictionary grows too large for a column chunk (common with IDs and free text).

You can see which encodings a file uses with PyArrow:

```python
import pyarrow.parquet as pq

meta = pq.ParquetFile("orders.parquet").metadata
col = meta.row_group(0).column(2)
print(col.path_in_schema, col.encodings, col.compression)
# for example: country ('PLAIN', 'RLE', 'RLE_DICTIONARY') ZSTD
```

**FSST: compressing strings with random access.** General codecs compress a block of strings together, so reading one string means decompressing the whole block. FSST (Fast Static Symbol Table, VLDB 2020) builds a table of up to 255 frequent symbols, each 1 to 8 bytes long, and replaces occurrences with one-byte codes. Each string is compressed on its own, so an engine can decompress only the strings a query needs, and decompression is a simple table lookup per code. DuckDB uses FSST for string columns, and the technique has been adopted by other columnar formats.

**Inside warehouses.** BigQuery's Capacitor format applies these encodings and goes further: it may reorder rows within a block to make runs longer for RLE. Time-series databases use delta-of-delta for timestamps and XOR-based encodings for floats (from Facebook's Gorilla paper); DuckDB uses ALP for floating-point columns; ClickHouse lets you choose codecs per column (`CODEC(Delta, ZSTD)`, `CODEC(DoubleDelta)`, `CODEC(Gorilla)`).

> **Pitfall:** Random row order destroys runs. A table written in arrival order with shuffled countries gets little out of RLE. Sorting by low-cardinality columns first, or clustering in BigQuery, can shrink files several times.

> **Tip:** High-cardinality strings (UUIDs, raw URLs) blow up dictionaries and fall back to plain encoding. Do not expect dictionary benefits there; store UUIDs as 16-byte binary where the engine supports it.

::: deeper
**Operating on compressed data.** With RLE, `SUM(x)` over a run is `value * count`; `COUNT(*) WHERE status = 'paid'` on a dictionary column is a scan of codes for one integer; zone maps (per-block minimum and maximum) let engines skip blocks entirely. Vectorized engines often keep data in dictionary or constant form in memory too (see Vectorized Execution & SIMD in the Query engine internals section).

**Bit-packing and SIMD.** Packed values cross byte boundaries, so decoding is shifting and masking. Layouts such as those in the FastLanes research decode many values at once with SIMD instructions; Parquet packs values in groups of 8 so that each group occupies a whole number of bytes.

**Delta-of-delta and Gorilla.** In Gorilla, a timestamp whose delta-of-delta is zero is stored in a single bit, so regularly sampled metrics cost about one bit per timestamp. Values are XORed with the previous value; similar floats share their leading and trailing zero bits, which are stored compactly.

**Choosing encodings automatically.** Most writers pick per column chunk based on statistics: number of distinct values, run lengths, value range. Parquet writers expose knobs such as dictionary page size limits and per-column encoding overrides, but defaults are usually sensible; layout (sort order, row-group size) matters more.

> **Interview:** "How would you store a column of per-minute timestamps efficiently?" Delta-of-delta encoding: consecutive deltas are almost always 60 seconds, so the second difference is mostly zero and bit-packs into one or two bits per value, followed by a light general codec.
:::

::: check
?? Which encoding suits a sorted `country` column with five distinct values, and why?
Dictionary encoding plus run-length encoding: five distinct values give tiny codes, and sorting makes long runs of the same code that RLE stores as single entries.

?? Why does a Parquet writer sometimes fall back from dictionary to plain encoding?
When a column chunk has too many distinct values, the dictionary grows past its size limit and stops saving space, so the writer switches to plain encoding.

?? What advantage does FSST have over compressing a block of strings with Zstd?
Each string is compressed independently with a small symbol table, so the engine can decompress individual strings quickly without decompressing the whole block.
:::

::: resources
- Spec: [Parquet encodings](https://parquet.apache.org/docs/file-format/data-pages/encodings/) · the official definitions of every Parquet encoding
- Paper: [FSST: Fast Random Access String Compression](https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf) · Boncz, Neumann and Leis, VLDB 2020
- Paper: [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf) · VLDB 2015; delta-of-delta timestamps and XOR floats
- Blog: [Lightweight compression in DuckDB](https://duckdb.org/2022/10/28/lightweight-compression.html) · RLE, bit-packing, FOR, dictionary and FSST in a real engine
- Blog: [Inside Capacitor, BigQuery's next-generation columnar storage format](https://cloud.google.com/blog/products/bigquery/inside-capacitor-bigquerys-next-generation-columnar-storage-format) · how BigQuery encodes and reorders data
- Paper: [The Design and Implementation of Modern Column-Oriented Database Systems](https://stratos.seas.harvard.edu/files/stratos/files/columnstoresfntdbs.pdf) · Abadi et al.; the survey chapter on compression in column stores
:::

### Serialization Formats — Thrift, Protobuf, FlatBuffers

::: plain
Serialization is packing a suitcase. A record in memory is spread around in pointers and objects; to send it over the network or save it to a file, you must fold it into a flat sequence of bytes, and the receiver must unfold it. Formats differ in how compact the folding is, whether the receiver needs a packing list (a schema), whether new items can be added later without confusing old receivers, and whether the receiver can find one item without unpacking everything.
:::

**Schema-based binary formats.** JSON repeats field names in every record and stores numbers as text. Binary formats with a schema replace names with small numeric tags and numbers with compact binary forms. The three classic families are Protocol Buffers (Google), Thrift (originally Facebook, now Apache) and Avro (Apache, from the Hadoop world, covered with file formats in the Data lakes and lakehouses section).

**The Protocol Buffers wire format.** A `.proto` file declares fields with **field numbers**:

```text
syntax = "proto3";
message Order {
  int64  id      = 1;
  string country = 2;
  sint32 delta   = 3;
}
```

On the wire, each field is a **tag** followed by a value. The tag is a varint holding `(field_number << 3) | wire_type`. The wire types are 0 VARINT (integers, booleans, enums), 1 I64 (fixed 8 bytes), 2 LEN (length-delimited: strings, bytes, nested messages, packed repeated fields) and 5 I32 (fixed 4 bytes); 3 and 4 are deprecated group markers. A **varint** stores an integer 7 bits per byte, with the top bit set when more bytes follow, so small numbers take one byte. The encoding by hand:

```python
def varint(n):
    out = bytearray()
    while True:
        byte = n & 0x7F                 # take the low 7 bits
        n >>= 7
        if n:
            out.append(byte | 0x80)     # more bytes follow: set the top bit
        else:
            out.append(byte)
            return bytes(out)

def zigzag(n):                          # sint32/sint64: small negatives stay small
    return (n << 1) ^ (n >> 63)

def field(number, wire_type, payload):
    return varint((number << 3) | wire_type) + payload

print(varint(1).hex(" "), varint(150).hex(" "), varint(300).hex(" "))   # 01 96 01 ac 02
print(varint(2**64 - 1).hex(" "))      # ff ff ff ff ff ff ff ff ff 01  (-1 as int64: 10 bytes)
print(zigzag(-1), zigzag(1), zigzag(-2))   # 1 2 3

# message Order { int64 id = 1; string country = 2; sint32 delta = 3; }
msg = (field(1, 0, varint(150))
       + field(2, 2, varint(2) + b"IN")
       + field(3, 0, varint(zigzag(-2))))
print(msg.hex(" "))                    # 08 96 01 12 02 49 4e 18 03
print(len(msg), "bytes")               # 9 bytes
```

The same record as JSON, `{"id":150,"country":"IN","delta":-2}`, is 36 bytes. Note the trap in the second line: a negative `int64` is encoded as a huge unsigned number and always takes 10 bytes, which is why `sint32` and `sint64` use **ZigZag** encoding to map small negatives to small positives.

**Schema evolution rules for Protobuf.** Readers skip fields with unknown numbers, so you can add fields freely. Never change a field's number or reuse a deleted number (mark it `reserved`), and do not change types in incompatible ways. Field numbers 1 to 15 take a one-byte tag, so give them to frequent fields.

**Thrift.** Thrift has a similar IDL with numbered fields and several **protocols**: the **binary protocol** writes a type byte, a 16-bit field ID and fixed-width values; the **compact protocol** packs the type and the delta from the previous field ID into one byte and uses ZigZag varints, giving output comparable to Protobuf. Data engineers meet Thrift every day without noticing: the Parquet file footer (schema, row groups, column statistics) is serialized with Thrift's compact protocol, and Hive Metastore's API is a Thrift service.

**FlatBuffers and Cap'n Proto: zero-copy access.** Protobuf and Thrift must be **parsed**: the reader walks the bytes and builds objects. **FlatBuffers** (Google) lays data out with offsets and per-table "vtables" so a reader can access any field directly in the received buffer, with no parsing or allocation. That suits large messages where you read a few fields, memory-mapped files, and games. Apache Arrow uses FlatBuffers for its IPC metadata (schemas and message headers), while the data buffers themselves are raw Arrow columns. **Cap'n Proto**, by the original author of Protobuf version 2, takes the same zero-copy approach and adds an RPC system with promise pipelining. The cost of zero-copy is larger messages (alignment padding, offsets) and a more awkward building API.

**Schemaless binary: MessagePack and CBOR.** These are "binary JSON": compact type tags, no schema, field names still included. Good for caches and quick interchange, weak for long-lived data contracts.

| Format | Schema | Tags or names | Parse step | Typical data use |
|---|---|---|---|---|
| JSON | Optional | Names in every record | Yes, slow | APIs, logs, landing zones |
| Protobuf | Required (`.proto`) | Field numbers | Yes, fast | gRPC, Pub/Sub, BigQuery Storage Write API |
| Thrift | Required (IDL) | Field IDs | Yes | Parquet footers, Hive Metastore, legacy RPC |
| Avro | Required (JSON schema, stored with files) | None: order from schema | Yes | Kafka with Schema Registry, Hadoop files |
| FlatBuffers, Cap'n Proto | Required | Offsets | No (zero-copy) | Arrow IPC metadata, low-latency services |
| MessagePack | None | Names | Yes | Caches, simple interchange |

**On Google Cloud.** Pub/Sub topics can enforce a schema in Avro or Protocol Buffer format; the BigQuery Storage Write API takes rows as serialized Protobuf messages (described by a descriptor); the BigQuery Storage Read API streams results as Arrow or Avro; all Google APIs use gRPC with Protobuf underneath.

> **Pitfall:** Renumbering Protobuf fields when tidying a `.proto` file. Old data and old clients still use the old numbers, so values silently land in the wrong fields or disappear. Field numbers are a permanent contract.

::: deeper
**Packed repeated fields.** In proto3, repeated scalar fields are packed by default: one LEN field holding all values back to back, instead of a tag per element.

**Unknown fields and round-tripping.** Protobuf parsers keep unknown fields and write them back when re-serializing, so a service on an old schema does not destroy data added by a newer one. JSON conversions of Protobuf (`JsonFormat`) cannot preserve unknown fields.

**Field presence.** Proto3 originally could not tell "zero" from "not set" for scalars; the `optional` keyword (since protobuf 3.15) restores explicit presence. Protobuf **Editions** (from 2023) replace the `proto2`/`proto3` split with per-feature settings.

**Avro's different approach.** Avro writes no tags at all: the reader must have the writer's schema (from the file header or a schema registry) and resolves it against its own schema by field name, applying defaults for missing fields. Data is smaller, but every message needs its schema identified, typically by a schema ID prefix.

**Deterministic serialization.** Protobuf does not guarantee identical bytes for equal messages (map ordering, unknown fields), so never hash serialized Protobuf to detect changes or deduplicate; hash a canonical form instead.

> **Interview:** "How does Protobuf support backward and forward compatibility?" Each field is identified by a number on the wire. New readers see missing fields as defaults; old readers skip unknown numbers. Compatibility holds as long as you never reuse or change field numbers and keep types compatible.
:::

::: check
?? What bytes encode field 1 with the integer value 150 in Protobuf?
`08 96 01`: the tag `08` is field 1 with wire type 0 (varint), and `96 01` is 150 as a varint.

?? Why does Protobuf offer `sint32` in addition to `int32`?
Negative `int32` values are sign-extended and take 10 bytes as varints. `sint32` uses ZigZag encoding so small negative numbers become small positive varints.

?? What does "zero-copy" mean for FlatBuffers, and what is the trade-off?
Readers access fields directly in the received buffer through offsets, with no parsing or object allocation. The trade-off is larger messages and a more complex way to build them.
:::

::: resources
- Spec: [Protocol Buffers encoding](https://protobuf.dev/programming-guides/encoding/) · the official description of tags, wire types, varints and ZigZag
- Docs: [FlatBuffers](https://flatbuffers.dev/) · the zero-copy format's documentation and internals
- Docs: [Apache Thrift](https://thrift.apache.org/) · the IDL, protocols and transports
- Blog: [Schema evolution in Avro, Protocol Buffers and Thrift](https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html) · Martin Kleppmann's byte-level comparison
- Docs: [Pub/Sub schemas](https://cloud.google.com/pubsub/docs/schemas) · enforcing Avro or Protocol Buffer schemas on topics
- Docs: [Cap'n Proto](https://capnproto.org/) · encoding and RPC design, with a comparison to Protobuf
:::

### Bloom Filters & Probabilistic Structures

::: plain
Sometimes you do not need an exact answer, only a very good guess, and you need it from a summary small enough to fit in memory. A Bloom filter is a bouncer with a fuzzy guest list: if it says "you are not on the list", that is certain; if it says "you might be", you check the real list. Other sketches answer "roughly how many different visitors?" or "which items are most frequent?" using kilobytes instead of gigabytes.
:::

**Bloom filters.** A Bloom filter is an array of `m` bits and `k` hash functions. To add a key, set the `k` bits its hashes point to. To test a key, check those bits: if any is 0, the key was never added (**no false negatives**); if all are 1, it was probably added (a **false positive** is possible, because other keys may have set those bits). With `n` keys, the false positive rate is about `(1 - e^(-kn/m))^k`. The best number of hashes is `k = (m/n) ln 2`, and for a target rate `p` you need `m = -n ln p / (ln 2)^2` bits: about 9.6 bits per key for 1 percent, 14.4 for 0.1 percent, independent of key size.

```python
import hashlib, math

class BloomFilter:
    def __init__(self, n, p):
        self.m = math.ceil(-n * math.log(p) / math.log(2) ** 2)   # bits
        self.k = round(self.m / n * math.log(2))                   # hash functions
        self.bits = bytearray((self.m + 7) // 8)

    def _positions(self, key):
        h = hashlib.sha256(key.encode()).digest()
        h1, h2 = int.from_bytes(h[:8], "big"), int.from_bytes(h[8:16], "big")
        return [(h1 + i * h2) % self.m for i in range(self.k)]    # double hashing

    def add(self, key):
        for pos in self._positions(key):
            self.bits[pos // 8] |= 1 << (pos % 8)

    def might_contain(self, key):
        return all(self.bits[pos // 8] >> (pos % 8) & 1 for pos in self._positions(key))

bf = BloomFilter(n=10_000, p=0.01)
print(bf.m, bf.k, round(bf.m / 10_000, 1))   # 95851 7 9.6   (bits, hashes, bits per key)
for i in range(10_000):
    bf.add(f"user-{i}")

print(all(bf.might_contain(f"user-{i}") for i in range(10_000)))   # True: no false negatives
fp = sum(bf.might_contain(f"other-{i}") for i in range(100_000))
print(f"false positive rate {fp / 100_000:.2%}")                   # false positive rate 0.95%
print(len(bf.bits), "bytes")                                       # 11982 bytes
```

Twelve kilobytes summarise ten thousand keys of any length. **Where data systems use them:** LSM-tree engines (RocksDB, Cassandra, Bigtable) keep a Bloom filter per file so a point lookup skips files that cannot contain the key; Parquet can store **split block Bloom filters** per column chunk so readers skip row groups for `WHERE user_id = 'x'` (which min/max statistics cannot do for unsorted IDs); distributed joins send a Bloom filter of the build side's keys to the probe side to drop rows early (a **runtime filter**, used by Spark, Trino and Impala).

A **counting Bloom filter** replaces each bit with a small counter, so keys can also be removed; it costs several times the memory. **Cuckoo** and **xor** filters are newer alternatives with deletion support or smaller size.

**HyperLogLog: counting distinct values.** Hash every value; in a uniform hash, a run of `r` leading zero bits appears about once per `2^r` distinct values. HyperLogLog splits hashes into `m = 2^p` buckets (registers) by their first `p` bits, records the longest run seen in each, and combines the registers with a harmonic mean. The standard error is about `1.04 / sqrt(m)`: with `p = 14` (16,384 registers, about 12 KiB) the error is around 0.8 percent, whether you count a thousand or a billion values. HLL++ (from Google) adds a 64-bit hash, bias correction and a sparse mode for small counts.

Sketches **merge**: the union of two HLLs is the register-wise maximum. BigQuery exposes this directly, which lets you precompute daily sketches and combine them into any date range:

```sql
-- Build one sketch per day once
CREATE OR REPLACE TABLE analytics.daily_user_sketch AS
SELECT event_date, HLL_COUNT.INIT(user_id, 14) AS users_sketch
FROM analytics.events
GROUP BY event_date;

-- Distinct users for any range, without rescanning raw events
SELECT HLL_COUNT.MERGE(users_sketch) AS approx_users
FROM analytics.daily_user_sketch
WHERE event_date BETWEEN '2026-10-01' AND '2026-10-31';
```

You cannot add daily distinct counts (users active on two days would be counted twice), but you can merge daily sketches. `APPROX_COUNT_DISTINCT(user_id)` is the one-shot version.

**Count-Min Sketch: frequencies.** A grid of `d` rows of `w` counters, one hash function per row. Adding an item increments one counter per row; its estimated count is the minimum over its counters. Collisions only add, so estimates never undercount; with `w = e/ε` and `d = ln(1/δ)` the overestimate stays below `ε` times the total count with probability `1 - δ`. It finds heavy hitters (top URLs, hot keys causing skew) in a stream with fixed memory. BigQuery's `APPROX_TOP_COUNT` and `APPROX_TOP_SUM` answer the same kind of question.

**Theta sketches, MinHash and bitmaps.**

- **Theta sketches** (Apache DataSketches) estimate distinct counts like HLL but also support **intersection and difference**: "users who did A and also B". Druid and Pinot use them.
- **MinHash** estimates the **Jaccard similarity** of two sets from the minimum hash values under many hash functions; combined with locality-sensitive hashing, it finds near-duplicate documents at scale, a standard step when deduplicating text for training large language models.
- **Roaring bitmaps** are not probabilistic but belong in the same toolbox: compressed sets of integers that split the space into 65,536-value chunks stored as arrays, bitmaps or runs. They power fast filtering in Druid, Pinot, ClickHouse and Lucene, and deletion vectors in Iceberg and Delta tables.

| Structure | Question | Error | Mergeable |
|---|---|---|---|
| Bloom filter | Is x in the set? | False positives only | Yes (bitwise OR, same parameters) |
| HyperLogLog | How many distinct? | About 1.04/sqrt(m) relative | Yes (union) |
| Count-Min Sketch | How often does x occur? | Overestimates only | Yes (add counters) |
| Theta sketch | Distinct counts with set operations | Relative, tunable | Union, intersection, difference |
| MinHash | How similar are two sets? | Tunable by signature size | Yes |

> **Pitfall:** Sizing a Bloom filter for today's key count. As `n` grows past the design size, the false positive rate climbs quickly. Rebuild or use scalable variants when the set grows.

> **Tip:** Store sketches, not raw IDs, in aggregate tables: they are small, mergeable across time and dimensions, and keep distinct counts cheap for dashboards.

::: deeper
**Why HLL uses a harmonic mean.** A single register's estimate is noisy and outliers (one lucky long run) are large; the harmonic mean damps them. Small and large ranges need corrections: linear counting for few distinct values, and HLL++ empirical bias correction.

**Bloom filter math in practice.** Real implementations use two hashes combined by double hashing (as above) rather than `k` independent ones, which does not change the asymptotic false positive rate. Blocked Bloom filters (including Parquet's split block filter) confine all `k` bits of a key to one cache line or 256-bit block, so a lookup costs one cache miss and can use SIMD, at a slightly higher false positive rate.

**Privacy.** Sketches of user IDs are still derived from personal data, and small counts can leak information. Treat them under the same governance as the raw data.

**Precision in BigQuery.** `HLL_COUNT.INIT` accepts precision from 10 to 24 (default 15); higher precision means bigger sketches and smaller error. Sketches with different precision can be merged at the lower precision.

> **Interview:** "How would you report monthly active users for any custom date range cheaply?" Store one HyperLogLog sketch per day (and per dimension if needed), then merge the sketches for the requested range. Merging is exact union semantics with HLL's usual error, and avoids rescanning raw events or double-counting users active on several days.
:::

::: check
?? A Bloom filter says a key is not present. Can that be wrong? And if it says present?
"Not present" is always correct: there are no false negatives. "Present" may be a false positive.

?? Why can you merge HyperLogLog sketches but not add daily distinct counts?
Merging takes the register-wise maximum, which is the sketch of the union of the sets. Adding counts double-counts values that appear on several days.

?? Roughly how many bits per key does a Bloom filter need for a 1 percent false positive rate?
About 9.6 bits per key, with about 7 hash functions.
:::

::: resources
- Docs: [BigQuery HyperLogLog++ functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions) · `HLL_COUNT.INIT`, `MERGE`, `MERGE_PARTIAL` and `EXTRACT`
- Paper: [HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) · Flajolet et al., 2007
- Paper: [HyperLogLog in Practice](https://research.google/pubs/hyperloglog-in-practice-algorithmic-engineering-of-a-state-of-the-art-cardinality-estimation-algorithm/) · Heule, Nunkesser and Hall; Google's HLL++ improvements
- Article: [Bloom filters by example](https://llimllib.github.io/bloomfilter-tutorial/) · an interactive tutorial on how bits get set and tested
- Docs: [Apache DataSketches](https://datasketches.apache.org/) · production sketch library with theta, HLL, quantiles and frequency sketches
- Paper: [Better bitmap performance with Roaring bitmaps](https://arxiv.org/abs/1402.6407) · Chambi, Lemire et al.; the compressed bitmap design
:::

### Data Encryption Internals

::: plain
Encryption locks data so only someone with the key can read it. The hard part is not the lock but the keys: where they live, who may use them, and how to change them without re-locking everything. The standard trick is to lock the data with a fresh key, then lock that key with a master key kept in a secure key service. It is like putting valuables in a box with its own key, and keeping all the box keys inside one bank vault.
:::

**Encryption at rest with AES-256-GCM.** AES is a block cipher; a 256-bit key gives AES-256. On its own a block cipher encrypts 16-byte blocks; a **mode** turns it into something usable for whole files. **GCM** (Galois/Counter Mode) encrypts with a counter stream and also computes an **authentication tag**, so tampering is detected on decryption. That makes it an **AEAD** cipher: authenticated encryption with associated data, where associated data (a table name, a column ID) is authenticated but not encrypted, binding the ciphertext to its context. GCM takes a 96-bit **nonce** that must never repeat for the same key: reusing one leaks the XOR of plaintexts and lets an attacker forge tags. Libraries generate random nonces, and keys should be rotated long before random nonces could collide.

**Encryption in transit with TLS 1.3.** TLS 1.3 (RFC 8446) shortened the handshake to one round trip, removed static RSA key exchange so every connection uses ephemeral Diffie-Hellman keys (**forward secrecy**: stealing the server's key later does not decrypt recorded traffic), and allows only AEAD ciphers (AES-GCM, ChaCha20-Poly1305). Its optional 0-RTT resumption can be replayed, so only idempotent requests should use it. **Mutual TLS** (mTLS) also authenticates the client with a certificate, common between Kafka clients and brokers. Google encrypts traffic between its data centres and between Google services automatically; your job is TLS on the paths you control (client to Cloud SQL, Kafka listeners, internal APIs).

**Envelope encryption.** Encrypting terabytes directly with a key in a remote key service would be slow and would send all data to that service. Instead:

1. Generate a random **data encryption key (DEK)** locally and encrypt the data with it (AES-256-GCM).
2. Send only the DEK to the key service, which encrypts (**wraps**) it with a **key encryption key (KEK)** that never leaves the service.
3. Store the wrapped DEK next to the data and discard the plaintext DEK.
4. To read, ask the key service to unwrap the DEK (an IAM-checked, audit-logged call), then decrypt locally.

[[fig:hardware_envelope]]

Google Cloud does this for all stored data by default, with AES-256 and keys managed by Google. With **customer-managed encryption keys (CMEK)** the KEK lives in your Cloud KMS project: you control rotation, IAM and disabling, and every unwrap appears in Cloud Audit Logs. **Cloud HSM** keeps KEKs in hardware security modules validated to FIPS 140-2 Level 3, and **Cloud External Key Manager (EKM)** keeps them in a key manager outside Google.

```bash
gcloud kms keyrings create data-keys --location=europe-west1
gcloud kms keys create orders-kek --keyring=data-keys --location=europe-west1 \
  --purpose=encryption --rotation-period=90d \
  --next-rotation-time=2027-01-01T00:00:00Z --protection-level=hsm
# Make it the default key for new tables in a BigQuery dataset
bq update --default_kms_key=projects/my-proj/locations/europe-west1/keyRings/data-keys/cryptoKeys/orders-kek \
  my-proj:sales
```

The BigQuery service account needs the `roles/cloudkms.cryptoKeyEncrypterDecrypter` role on the key, and the key's location must match the dataset's.

**Column-level encryption.** CMEK protects whole tables at rest against storage-level threats, but anyone with table access reads plaintext. To protect individual sensitive columns (and to enable **crypto-shredding**: deleting one customer's key makes their data unreadable everywhere, including backups), encrypt values themselves. BigQuery has AEAD functions; keysets can be wrapped by Cloud KMS:

```sql
DECLARE kms_key STRING DEFAULT 'gcp-kms://projects/my-proj/locations/europe-west1/keyRings/data-keys/cryptoKeys/orders-kek';
DECLARE wrapped_keyset BYTES DEFAULT FROM_BASE64('CiQA...');  -- created once with KEYS.NEW_WRAPPED_KEYSET

SELECT
  customer_id,
  AEAD.ENCRYPT(KEYS.KEYSET_CHAIN(kms_key, wrapped_keyset), email, CAST(customer_id AS STRING)) AS email_enc
FROM staging.customers;
```

`DETERMINISTIC_ENCRYPT` produces the same ciphertext for the same input, so encrypted columns can still be joined and grouped on, at the cost of revealing equality.

**Parquet modular encryption.** The Parquet format can encrypt each column with its own key and the footer with another. Two algorithms exist: `AES_GCM_V1` (everything authenticated with GCM) and `AES_GCM_CTR_V1` (data pages with faster, unauthenticated CTR, metadata with GCM). A **plaintext footer** mode lets old readers see the schema and read unencrypted columns. Keys are managed by envelope encryption through a pluggable KMS client. In Spark:

```text
spark.hadoop.parquet.crypto.factory.class=org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory
spark.hadoop.parquet.encryption.kms.client.class=com.example.CloudKmsClient
spark.hadoop.parquet.encryption.footer.key=footerKeyId
spark.hadoop.parquet.encryption.column.keys=piiKeyId:email,phone;finKeyId:card_last4
```

Users without access to `piiKeyId` can still read the other columns.

**Key rotation strategies.** Rotating a Cloud KMS key creates a new **primary version** used for new encryption; older versions stay enabled to decrypt existing data. Rotation therefore limits how much data one key version protects, without rewriting anything. If policy requires that old versions be retired, re-encrypt (rewrite tables or files, or re-wrap DEKs, which is cheap because only DEKs change) and then disable and destroy the old versions. Destroying a version makes everything still encrypted under it unreadable, which is the point of crypto-shredding and a risk if done by mistake (Cloud KMS schedules destruction after a waiting period).

> **Pitfall:** Disabling or destroying a CMEK key version to "test" access. BigQuery tables, Cloud Storage objects and Pub/Sub messages protected by it become unreadable, and pipelines start failing. Test in a separate project.

> **Pitfall:** Storing a DEK, a keyset or a service account key in a repository or notebook. Use Secret Manager for secrets and Cloud KMS for keys, and grant IAM to service accounts, not people.

::: deeper
**Why GCM limits matter.** With random 96-bit nonces, NIST guidance limits one key to about 2^32 encryptions to keep the collision probability negligible. Envelope encryption naturally respects this, because each file or chunk gets its own DEK. AES-GCM-SIV and deterministic AEAD (AES-SIV, used by BigQuery's `DETERMINISTIC_ENCRYPT`) are misuse-resistant variants that do not fail catastrophically on nonce reuse.

**HSM integration.** A hardware security module is a tamper-resistant device that generates and uses keys without ever exporting them. Cloud HSM lets you create Cloud KMS keys with protection level `HSM`; the API is identical, so applications do not change. The trade-offs are cost per key version and per operation, and request quotas.

**Defence in depth.** Encryption at rest protects against stolen disks and storage-layer access; it does nothing against a user with legitimate query access. Combine it with IAM, column-level access policies and dynamic data masking in BigQuery (covered in the Data governance, security and compliance section).

**TLS termination and zero-copy.** Encryption has CPU cost: TLS on Kafka disables `sendfile` (see the OS-level I/O topic), and AES-NI instructions in modern CPUs make AES-GCM fast enough that this is usually acceptable.

> **Interview:** "Explain envelope encryption and why it is used." Data is encrypted locally with a per-object data key; only that small key is encrypted by a master key held in a KMS. It avoids sending bulk data to the KMS, lets each object have its own key, makes rotation cheap (re-wrap keys, not data), and centralises access control and auditing on the master key.
:::

::: check
?? What does the authentication tag in AES-GCM provide that plain AES-CTR does not?
Integrity: decryption fails if the ciphertext or associated data was modified. CTR alone only provides confidentiality.

?? In envelope encryption, what is sent to Cloud KMS: the data, or something else?
Only the data encryption key, which KMS wraps or unwraps with the key encryption key. The bulk data never leaves your service.

?? After rotating a Cloud KMS key, can old data still be read? What must you do to retire the old key version?
Yes: old versions stay enabled for decryption, and only new encryption uses the new primary version. To retire an old version, re-encrypt or re-wrap the data under the new version first, then disable and destroy the old one.
:::

::: resources
- Docs: [Cloud KMS documentation](https://cloud.google.com/kms/docs) · key rings, keys, versions, rotation, Cloud HSM and EKM
- Docs: [Envelope encryption](https://cloud.google.com/kms/docs/envelope-encryption) · Google Cloud's explanation of DEKs and KEKs
- Docs: [BigQuery AEAD encryption concepts](https://cloud.google.com/bigquery/docs/aead-encryption-concepts) · column-level encryption, keysets and crypto-deletion in SQL
- Spec: [Parquet modular encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/) · the Parquet encryption specification
- Docs: [Spark columnar encryption](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#columnar-encryption) · using Parquet modular encryption from Spark
- Spec: [RFC 8446: TLS 1.3](https://www.rfc-editor.org/rfc/rfc8446) · the TLS 1.3 standard
- Book: *Serious Cryptography, 2nd edition* by Jean-Philippe Aumasson · a practical explanation of AES, GCM, key exchange and TLS
:::

## Performance work

The last group turns knowledge into practice. Most big data engines run on the Java Virtual Machine (Spark, Kafka, Flink, Trino, Hadoop), so their speed and stability depend on how the JVM manages memory and compiles code. And when something is slow, guessing is expensive: profilers and tracers show where the time actually goes, down to the function, system call or disk request.

Everything earlier in this section comes together here. A flame graph that shows time in `memcpy` points back to copies in the I/O path; a high cache-miss count points to memory layout; long GC pauses point to heap sizing and object churn. The aim is a repeatable method: measure, form a hypothesis, change one thing, measure again.

### JVM Tuning for Data Systems

::: plain
Java programs do not free memory themselves. A **garbage collector** (GC) walks through memory now and then, finds objects nothing uses any more, and reclaims them, a bit like a cleaner who tidies a room while people work in it. Sometimes the cleaner asks everyone to freeze for a moment (a **pause**). For a data system, long or frequent pauses mean stalled tasks, missed heartbeats and slow queries, so tuning the JVM is mostly about giving the cleaner the right amount of room and the right cleaning strategy.
:::

**The heap and generations.** Java objects live in the **heap**, sized with `-Xms` (initial) and `-Xmx` (maximum). Most objects die young, so collectors split the heap into a **young generation**, collected often and cheaply, and an **old generation** for long-lived objects. Data engines create huge numbers of short-lived objects (rows, buffers, strings) and some very long-lived ones (cached data, state, broadcast tables).

**The collectors in current JDKs.**

| Collector | How it works | Pauses | Use it for |
|---|---|---|---|
| G1 (default since JDK 9) | Heap divided into regions; collects the regions with most garbage first; concurrent marking | Tens to a few hundred ms, aiming at `-XX:MaxGCPauseMillis` (default 200) | Spark executors, Kafka brokers, most services |
| ZGC | Concurrent copying with coloured pointers; generational since JDK 21 and the only mode since JDK 24 | Usually under a millisecond, independent of heap size | Large heaps, latency-sensitive services (Trino coordinators, Kafka with big heaps) |
| Shenandoah | Concurrent compaction with forwarding pointers; available in OpenJDK builds such as Red Hat's | Very short | Similar to ZGC |
| Parallel | Stop-the-world, many threads | Longer, but highest throughput | Batch jobs where pauses do not matter |
| CMS | Old concurrent mark-sweep collector | n/a | Removed in JDK 14; delete it from old configs |

Concurrent collectors trade throughput (CPU spent on GC threads, barriers in your code) for short pauses. For a batch Spark job, total throughput matters more than pause length; for a Kafka broker or a streaming job with tight latency, pauses matter more.

**Read the GC log before changing anything.** Unified logging (JDK 9 and later) writes every collection with cause, duration and heap before and after:

```bash
-Xlog:gc*:file=/var/log/app/gc.log:time,uptime,level,tags:filecount=5,filesize=20m
# a G1 line looks like:
# [2026-11-03T10:15:02.113+0000][512.334s][info][gc] GC(87) Pause Young (Normal) (G1 Evacuation Pause) 6140M->1210M(8192M) 48.512ms
```

Look for: pause durations and frequency; **Full GC** events (a stop-the-world collection of the whole heap, almost always a problem); **humongous allocations** in G1 (objects at least half a region, such as large arrays, allocated straight into the old generation); and the old generation filling up after each collection (a leak, or a heap too small for the live data). Tools such as JDK Mission Control with Java Flight Recorder (`-XX:StartFlightRecording`) and GC log analysers turn logs into charts.

**Heap sizing rules of thumb.**

- Set `-Xms` equal to `-Xmx` on servers, so the heap never resizes at runtime.
- Stay below about 32 GB per heap when you can: below that the JVM uses **compressed ordinary object pointers** (4-byte references instead of 8), so a 31 GB heap can hold about as much as a 40 GB one. Above it, prefer ZGC or split work across more processes.
- In containers, the JVM reads the container's memory limit; `-XX:MaxRAMPercentage=75` sizes the heap as a share of it and leaves room for non-heap memory.

**Off-heap memory.** Much of a data system's memory is outside the heap: thread stacks, metaspace (class metadata), JIT code cache, direct `ByteBuffer`s (Netty, Kafka clients), native libraries (RocksDB, compression codecs, Arrow) and Spark's optional off-heap storage. The container or YARN limit applies to the **whole process**, so a job can be killed for exceeding memory while the heap looks healthy.

**Per-engine settings.**

- **Spark executors.** The container size is roughly `spark.executor.memory` (heap) plus `spark.executor.memoryOverhead` (default the larger of 384 MiB and 10 percent of the heap) plus `spark.memory.offHeap.size` if enabled plus PySpark worker memory. Inside the heap, `spark.memory.fraction` (default 0.6 of heap minus 300 MiB) is shared by execution and storage. Spark 4 runs on Java 17 or 21 with G1 by default; pass flags through `spark.executor.extraJavaOptions`. The PySpark + Kafka guide's [Memory management](pyspark-streaming-kafka-topic-map.html#memory) and [JVM and garbage collection tuning](pyspark-streaming-kafka-topic-map.html#gc) sections go deep on this.

```bash
gcloud dataproc jobs submit spark --cluster=etl-cluster --region=europe-west1 \
  --class=com.example.DailyJob --jars=gs://my-bucket/jobs/daily-job.jar \
  --properties=spark.executor.memory=20g,spark.executor.memoryOverhead=4g,\
spark.executor.extraJavaOptions=-XX:+UseG1GC\ -XX:MaxGCPauseMillis=200\ -Xlog:gc*:file=/tmp/gc.log
```

- **Kafka brokers.** Keep the heap modest (the Kafka documentation's example uses 6 GB with G1 and a short pause target) and leave the rest of RAM to the page cache, which holds the log data. Set it with `KAFKA_HEAP_OPTS` and `KAFKA_JVM_PERFORMANCE_OPTS`. Kafka 4 brokers need Java 17 or later.
- **Flink TaskManagers.** Configure the total with `taskmanager.memory.process.size`; Flink splits it into framework and task heap, **managed memory** (off-heap, used by RocksDB state and batch operators, 40 percent of Flink memory by default), network buffers, metaspace and JVM overhead. With the RocksDB state backend, most state lives in native memory, so a TaskManager killed for memory usually needs more managed memory or overhead, not more heap.

**JIT warm-up.** The JVM starts by interpreting bytecode, then compiles hot methods with the fast C1 compiler and later the optimising C2 compiler. A short task (a Spark stage of a few seconds, a fresh executor) runs partly in slow, unoptimised code. Long-running executors, fewer and bigger tasks, and class data sharing archives (plus the ahead-of-time cache in recent JDKs) reduce warm-up cost.

**Native memory tracking.** When the process grows beyond heap plus expected overhead:

```bash
java -XX:NativeMemoryTracking=summary ...      # small overhead; enable when investigating
jcmd <pid> VM.native_memory summary            # heap, class, thread, code, GC, internal, other...
```

> **Pitfall:** Raising `-Xmx` to fix "Container killed for exceeding memory limits". The heap was not the problem; native memory was. Increase the overhead setting (`spark.executor.memoryOverhead`, Flink's managed or overhead memory) or find the native leak.

> **Pitfall:** Copying old JVM flags (`-XX:+UseConcMarkSweepGC`, `-XX:+UseParNewGC`, `-XX:PermSize`) into new deployments. Modern JDKs reject or ignore them; start from defaults and add flags only for measured problems.

::: deeper
**How G1 decides.** G1 predicts how long collecting each region will take and picks a set that fits the pause target. Young collections copy live objects from eden to survivor regions; when old-generation occupancy crosses the adaptive initiating threshold (`-XX:InitiatingHeapOccupancyPercent`, default 45, adjusted at runtime), it starts concurrent marking, followed by **mixed** collections that also clean old regions. If marking cannot keep up, G1 falls back to a Full GC. Fixes: start marking earlier, give it more threads (`-XX:ConcGCThreads`), reduce allocation, or grow the heap.

**Why big executors hurt.** One executor with 64 GB heap and 16 cores loses compressed oops, has longer pauses, and runs many tasks that share one GC. Several executors of about 4 to 5 cores and under 32 GB each are usually more efficient.

**Allocation is the real cost.** Every short-lived object costs allocation and eventual collection. Engines reduce allocation with off-heap and columnar buffers (Spark's Tungsten), object reuse, and code generation that keeps values in local variables. Native engines (Velox, DataFusion; see the Query engine internals section) avoid the GC altogether.

**Safepoints and time-to-safepoint.** All threads must reach a safepoint before a stop-the-world phase. A thread in a long counted loop can delay everyone, making pauses longer than the GC work itself; `-Xlog:safepoint` reveals it.

> **Interview:** "A Spark job has long GC pauses and occasional executor losses. How do you investigate?" Enable GC logging and check pause times, Full GCs and humongous allocations; check whether executors are killed by the container for native memory or by heartbeat timeouts during pauses; then reduce allocation (avoid huge collects, broadcast smaller tables, fix skewed partitions), right-size executors under 32 GB, and adjust memory overhead.
:::

::: check
?? Why is a 31 GB heap often better than a 40 GB heap?
Below about 32 GB the JVM uses compressed 4-byte object pointers, so the same objects take less memory; a 40 GB heap uses 8-byte pointers and may hold no more data while having longer GC work.

?? A Flink TaskManager using RocksDB keeps getting killed for exceeding its memory limit, yet heap usage is low. What is the likely cause?
RocksDB's native (off-heap) memory, or other native allocations, exceeding the managed or overhead budget. Increase managed or overhead memory, or limit RocksDB memory, instead of raising the heap.

?? Which GC log events deserve attention first?
Full GC events, long pauses relative to the target, humongous allocations, and old-generation occupancy that keeps rising after collections.
:::

::: resources
- Docs: [HotSpot Virtual Machine Garbage Collection Tuning Guide](https://docs.oracle.com/en/java/javase/21/gctuning/) · Oracle's guide to G1, ZGC and the Parallel collector
- Docs: [Spark tuning: memory management and GC](https://spark.apache.org/docs/latest/tuning.html) · Spark's official advice on memory, serialization and garbage collection
- Docs: [Flink memory configuration](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/mem_setup/) · how Flink splits process memory, including managed memory
- Docs: [Kafka Java version and JVM settings](https://kafka.apache.org/documentation/#java) · the recommended broker JVM options
- Book: [Optimizing Cloud Native Java, 2nd edition](https://www.oreilly.com/library/view/optimizing-cloud-native/9781098149332/) · Evans and Gough; GC, JIT and measurement on modern JVMs
- Book: [Java Performance, 2nd edition](https://www.oreilly.com/library/view/java-performance-2nd/9781492056102/) · Scott Oaks; GC and heap chapters with practical flags
:::

### Performance Profiling for Data Systems

::: plain
Profiling is putting a stopwatch on every part of a program instead of guessing which part is slow. A **sampling profiler** peeks at what the program is doing many times a second, like a manager glancing at every desk every few seconds: after a while, the tally of what people were doing shows where the time goes. A **flame graph** draws that tally as a picture where the widest bars are where the time is spent.
:::

**Start with a method, not a tool.** The **USE method** asks, for every resource (CPU, memory, disks, network): what is its **utilisation**, is it **saturated** (work queuing), and are there **errors**? A quick first pass on a Linux host:

```bash
uptime                   # load averages: is demand growing?
vmstat 1                 # run queue (r), memory, swap, I/O wait (wa)
mpstat -P ALL 1          # per-CPU usage: one hot core means a single-threaded bottleneck
pidstat 1                # which processes use CPU
iostat -xz 1             # per-disk utilisation, queue size, await (latency)
free -m                  # memory, including page cache
sar -n DEV 1             # network throughput per interface
```

That tells you whether the job is CPU-bound, I/O-bound, memory-bound or waiting on something else. Only then pick a profiler.

**perf and hardware counters.** `perf` is Linux's built-in profiler. `perf stat` counts hardware events, which reveals *how* the CPU spends its cycles:

```bash
perf stat -d -- ./run_query.sh
#  instructions per cycle (IPC) under about 1 often means memory stalls;
#  high cache-misses or branch-misses point at data layout or unpredictable branches
perf record -F 99 -a -g -- sleep 30        # sample all CPUs 99 times per second with call stacks
perf report --stdio | head -50
```

**Flame graphs.** Collapse the sampled stacks and draw them:

```bash
git clone https://github.com/brendangregg/FlameGraph
perf script | ./FlameGraph/stackcollapse-perf.pl | ./FlameGraph/flamegraph.pl > cpu.svg
```

How to read one: each box is a function; its **width** is the share of samples where it was on the stack; boxes above it are the functions it called. The x-axis is sorted alphabetically, not by time. Look for wide plateaus at the top (functions burning CPU themselves) and wide towers you did not expect (time in `memcpy`, decompression, regex, JSON parsing, lock spinning). **Off-CPU** flame graphs show where threads were blocked (I/O, locks), which on-CPU profiles cannot.

**eBPF tracing.** eBPF runs small, verified programs inside the kernel when events happen (system calls, disk completions, scheduler switches, function entries), aggregating results in the kernel with low overhead. The bcc tools and bpftrace make it practical:

```bash
biolatency 10 1                       # histogram of disk I/O latency over 10 seconds
cachestat 1                           # page cache hit ratio per second
runqlat 10 1                          # how long threads wait for a CPU
bpftrace -e 'tracepoint:syscalls:sys_enter_openat { @[comm] = count(); }'   # who opens files most
```

For a Kafka broker that suddenly slows down, `biolatency` shows whether disk latency changed and `cachestat` whether consumers fell out of the page cache.

**Profiling the JVM: async-profiler.** `perf` alone cannot name JIT-compiled Java methods without extra agents. **async-profiler** samples Java and native stacks together, without the "safepoint bias" of older Java profilers, and writes flame graphs directly:

```bash
asprof -d 30 -e cpu -f /tmp/broker-cpu.html $(pgrep -f kafka.Kafka)   # CPU for 30 s
asprof -d 30 -e alloc -f /tmp/alloc.html <pid>                         # who allocates (GC pressure)
asprof -d 30 -e wall -t -f /tmp/wall.html <pid>                        # wall clock, per thread: includes waiting
```

To profile Spark executors, load it as an agent through `spark.executor.extraJavaOptions` (for example `-agentpath:/opt/async-profiler/lib/libasyncProfiler.so=start,event=cpu,file=/tmp/exec-%p.html`) and collect the files from the workers, or use the executor thread dumps in the Spark UI for a quick look. Flink's web UI has built-in flame graphs for operators (`rest.flamegraph.enabled: true`), next to its backpressure view.

**Profiling Python in data jobs.** Driver-side Python and pandas code can be profiled with `cProfile` (deterministic, higher overhead) or `py-spy` (sampling, attaches to a running process: `py-spy record -o profile.svg --pid <pid>`). Since Python 3.12, `python -X perf` lets Linux `perf` show Python function names. For PySpark UDFs, Spark 4 adds a UDF profiler (`spark.sql.pyspark.udf.profiler` set to `perf` or `memory`, then `spark.profile.show()`). A standard-library example where call counts, unlike timings, are exact:

```python
import cProfile, pstats

def parse(line):
    return line.split(",")

def total(lines):
    return sum(int(parse(l)[1]) for l in lines)

lines = [f"order-{i},{i % 50}" for i in range(10_000)]
prof = cProfile.Profile()
prof.enable()
result = total(lines)
prof.disable()

print(result)                                        # 245000
stats = pstats.Stats(prof)
calls = {name: nc for (_, _, name), (cc, nc, *_) in stats.stats.items()}
print(calls["parse"], calls["total"])                # 10000 1
stats.sort_stats("cumulative").print_stats(3)        # top 3 by cumulative time (times vary)
```

**Continuous profiling on Google Cloud.** **Cloud Profiler** samples CPU and heap from production services (Java, Go, Python, Node.js) with low overhead and keeps the history, so you can compare flame graphs across versions. Managed Service for Apache Spark can enable it for jobs through job properties. The PySpark + Kafka guide's [Profiling CPU, memory and Python](pyspark-streaming-kafka-topic-map.html#profiling) and [Spark UI mastery](pyspark-streaming-kafka-topic-map.html#ui) cover the Spark-specific workflow.

> **Pitfall:** Profiling the wrong thing. A CPU flame graph of a job that is waiting on Cloud Storage or on a skewed task shows little; first establish where the wall-clock time goes (Spark UI stage timeline, `iostat`, off-CPU or wall-clock profiles), then zoom in.

> **Tip:** Change one thing at a time and keep the before and after profiles. A flame graph diff is far more convincing in a review than "it felt faster".

::: deeper
**Sampling frequency.** 99 Hz rather than 100 Hz avoids sampling in lockstep with periodic timers. Higher rates give finer detail at higher overhead; for production, low rates over longer windows work well.

**Frame pointers and stack walking.** Many distributions compiled software without frame pointers, which breaks cheap stack walking in `perf`. The JVM needs `-XX:+PreserveFramePointer` for `perf`-based Java stacks; async-profiler avoids the problem with its own stack walker. Recent Linux distributions have started re-enabling frame pointers by default, which improves whole-system profiling.

**Safepoint bias.** Traditional Java profilers sample only at safepoints, so tight loops without safepoints are invisible and time is attributed to the wrong code. async-profiler and Java Flight Recorder sample asynchronously and avoid this.

**Top-down microarchitecture analysis.** When a hot function is identified, tools such as `perf stat --topdown` (on supported Intel CPUs) classify cycles into front-end bound, back-end bound (memory or core), bad speculation and retiring, telling you whether to change data layout, branches or the algorithm.

> **Interview:** "A Spark stage became twice as slow after an upgrade. How do you find out why?" Compare Spark UI stage metrics (task time, shuffle, spill, GC time, skew) between versions; if CPU time grew, capture async-profiler flame graphs of executors on both versions and diff them; check the physical plan for changed operators (codegen, join strategy); confirm with `perf stat` whether IPC or cache misses changed.
:::

::: check
?? What does the width of a box in a CPU flame graph represent, and what does the x-axis order mean?
The share of samples in which that function was on the stack, so roughly its share of CPU time including its callees. The x-axis is sorted alphabetically and does not represent time.

?? Why use async-profiler instead of plain `perf` for a Kafka broker or Spark executor?
It resolves JIT-compiled Java frames together with native and kernel frames, avoids safepoint bias, and can profile CPU, allocations, locks and wall-clock time with low overhead.

?? A job is slow but CPU utilisation is low. Which kind of profile or tool helps?
Off-CPU or wall-clock profiles and I/O tools (`iostat`, `biolatency`, `cachestat`) that show where threads wait: disk, network, locks or remote services.
:::

::: resources
- Blog: [Flame Graphs](https://www.brendangregg.com/flamegraphs.html) · Brendan Gregg's introduction, with CPU, off-CPU and memory variants
- Book: [BPF Performance Tools](https://www.oreilly.com/library/view/bpf-performance-tools/9780136588870/) · Brendan Gregg; eBPF tracing for disks, file systems, networking and runtimes
- Repo: [async-profiler](https://github.com/async-profiler/async-profiler) · the low-overhead JVM sampling profiler, with usage docs
- Repo: [bpftrace](https://github.com/bpftrace/bpftrace) · the high-level tracing language, its reference guide and one-liners
- Docs: [Cloud Profiler](https://cloud.google.com/profiler/docs) · continuous CPU and heap profiling on Google Cloud
- Article: [The USE Method](https://www.brendangregg.com/usemethod.html) · a checklist for finding resource bottlenecks quickly
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Latency ladder | L1 ~1 ns, DRAM ~100 ns, NVMe ~tens of µs, HDD ~5 to 10 ms |
| Prefer | Large sequential I/O, append-only writes, columnar scans |
| Benchmark a disk | `fio --rw=randread --bs=4k --iodepth=64 --ioengine=io_uring --direct=1 ...` |
| Shuffle-heavy Spark on Google Cloud | Local SSD (NVMe) on workers; never for the only copy of data |
| Cache line | 64 bytes: keep data you use together contiguous; pad per-thread counters |
| NUMA topology and pinning | `numactl --hardware`; `numactl --cpunodebind=0 --membind=0 cmd` |
| GPU worth it? | Big, compute-heavy, operators supported end to end, data stays on GPU |
| pandas on GPU | `python -m cudf.pandas job.py` |
| Durable write | `write()` then `fsync()`; for atomic replace also `rename` and `fsync` the directory |
| Kafka speed | Sequential logs, page cache, batching, `sendfile` zero-copy (lost with TLS) |
| io_uring | Shared submission and completion rings; batch many ops per syscall |
| Many small files | Inode and metadata cost; aim for files of 128 MiB to 1 GiB |
| Parquet codec | Zstd low level for smaller files; Snappy for least CPU; sort data first |
| Encodings | Dictionary plus RLE for low cardinality, delta for sorted ints, delta-of-delta for timestamps |
| Protobuf tag | `(field_number << 3) \| wire_type`; never reuse field numbers |
| Bloom filter size | ~9.6 bits per key for 1% false positives |
| Distinct users over any range | Daily `HLL_COUNT.INIT`, then `HLL_COUNT.MERGE` |
| Envelope encryption | Data key encrypts data; Cloud KMS key wraps the data key |
| JVM heap | `-Xms` = `-Xmx`, under ~32 GB, G1 by default, ZGC for low pauses |
| Container killed, heap fine | Native memory: raise overhead or managed memory, check NMT |
| Profile a JVM | `asprof -d 30 -e cpu -f out.html <pid>` |
| Profile the whole host | `perf record -F 99 -a -g -- sleep 30` then a flame graph |

### Practice

::: try
**1. Pick the storage.** For each workload on Compute Engine, choose Persistent Disk or Hyperdisk, or Local SSD, and justify it: (a) the data directory of a self-managed Kafka broker; (b) Spark shuffle and spill space on a Managed Service for Apache Spark cluster; (c) a PostgreSQL database on a VM; (d) a cache of model features that is rebuilt every hour from BigQuery.
:::

?? Answer to 1
(a) Persistent Disk or Hyperdisk: broker data must survive VM restarts, and Kafka replication handles disk loss across brokers; size throughput for sequential writes. (b) Local SSD: shuffle data is recomputable and benefits from low-latency NVMe. (c) Persistent Disk or Hyperdisk (or better, Cloud SQL or AlloyDB): the data must be durable. (d) Local SSD is acceptable because the cache can be rebuilt; network storage also works if restart time matters.

::: try
**2. Count the cache misses.** Change the cache simulator in Memory Hierarchy & Cache Effects so the matrix is 256 x 256 instead of 1024 x 1024. Predict the misses for row-by-row and column-by-column traversal before running it, and explain the result.
:::

?? Answer to 2
Both give 4,096 misses (256 x 256 x 4 bytes / 64 bytes per line). One column now touches only 256 lines, which fits in the 512-line cache, so the lines loaded for column 0 are still cached when columns 1 to 15 reuse them. Cache effects appear only once the working set exceeds the cache.

::: try
**3. Size a Bloom filter.** A Parquet table has 50 million distinct `user_id` values spread over row groups. You want a Bloom filter per column chunk with about 0.1 percent false positives, and each row group holds about 1 million distinct users. How many bits per key and roughly how many bytes per row group's filter do you need?
:::

?? A solution to 3
For p = 0.001, bits per key = -ln(0.001) / (ln 2)^2, about 14.4, with about 10 hash functions. For 1 million keys that is about 14.4 million bits, roughly 1.8 MB per row group.

```python
import math
n, p = 1_000_000, 0.001
bits = -n * math.log(p) / math.log(2) ** 2
print(round(bits / n, 1), round(bits / 8 / 1e6, 1))   # 14.4 1.8
```

::: try
**4. Decode a Protobuf message.** Decode the bytes `08 ac 02 12 03 4e 4f 52` by hand, given `message Order { int64 id = 1; string country = 2; }`.
:::

?? Answer to 4
`08` is field 1, wire type 0 (varint); `ac 02` is 0x2c + (0x02 << 7) = 44 + 256 = 300, so `id = 300`. `12` is field 2, wire type 2 (length-delimited); `03` is the length; `4e 4f 52` is ASCII "NOR", so `country = "NOR"`.

::: try
**5. Diagnose a broker.** A Kafka broker with 64 GB RAM has `-Xmx48g`. Consumers that fall a few minutes behind make disk reads spike and latency rise. What is wrong, and what would you change?
:::

?? A solution to 5
The heap takes most of the RAM, leaving little page cache, so even recently written segments are evicted and lagging consumers force disk reads. Shrink the heap to around 6 GB (G1 with a short pause target), leaving most memory to the page cache. Verify with `cachestat` or `free -m` (buff/cache) and `biolatency` before and after.

### What comes next

You now know the physical layer under every data system: how disks, caches, NUMA and GPUs behave, how Linux moves bytes, how data is compressed, encoded, serialized, summarised and encrypted, and how to tune and profile the JVM engines that run on top. The next section, **The Hadoop ecosystem**, shows the first generation of distributed data systems that was designed around exactly these constraints: HDFS with its large sequential blocks and JBOD disks, MapReduce with its sort-based shuffle, YARN, Hive and the file formats that grew up with them.
