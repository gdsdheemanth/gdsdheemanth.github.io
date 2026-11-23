This section is about Apache Hadoop and the projects that grew up around it: the first open-source platform that let ordinary companies store and process petabytes on clusters of cheap machines. It is for data engineers who will meet Hadoop in one of three ways: running or migrating an on-premises cluster, using a managed Hadoop and Spark service such as Managed Service for Apache Spark (formerly Dataproc) on Google Cloud, or simply working with tools (the Hive Metastore, ORC files, YARN, Ranger, HBase-style row keys) whose design only makes sense once you know where they came from. It assumes SQL for data engineering, Data lakes and lakehouses (file formats, partitioning, object storage) and, most directly, [Storage internals and hardware](#hardware): sequential I/O, JBOD disks, the page cache, LSM trees and the JVM. The next section, [Apache Spark](#spark), builds directly on the ideas here.

**What you will be able to do afterwards**

- Explain how HDFS stores a file as replicated or erasure-coded blocks, how the NameNode stays highly available, and why small files hurt.
- Operate HDFS: read `fsck` and `dfsadmin` output, rebalance data, decommission nodes, and reason about what happens when a disk, a DataNode or a NameNode fails.
- Describe YARN's ResourceManager, NodeManagers and ApplicationMasters, and configure Capacity Scheduler queues so teams share a cluster fairly.
- Trace a MapReduce job through map, spill, sort, shuffle and reduce, and recognise the same pattern inside Spark, Hive and BigQuery.
- Secure a Hadoop cluster with Kerberos, Ranger policies and a Knox gateway.
- Explain how Hive compiles SQL to Tez DAGs, why the Hive Metastore outlived Hive itself, and when Impala, HBase or Kylin fit.
- Place newer projects (Alluxio, Ozone, Kyuubi) and decide what to keep, replace or migrate when a Hadoop platform moves to Google Cloud.

**The big picture.** Hadoop is three ideas stacked on top of each other. At the bottom, a **distributed file system** (HDFS) splits files into large blocks and spreads copies over many machines, so that no single disk failure loses data and many disks can be read in parallel. In the middle, a **resource manager** (YARN) treats the memory and CPU of all those machines as one pool and hands out slices of it, called containers, to applications. On top, **processing engines** (MapReduce first, then Tez, Spark and others) send the computation to the machines that hold the data, instead of pulling the data to the computation. Everything else in the ecosystem, from SQL engines (Hive, Impala) to databases (HBase) and security layers (Ranger, Knox), plugs into one of those three layers. Today the bottom layer is usually replaced by object storage such as Cloud Storage and the middle layer is often Kubernetes, but the shape, and many of the components, are still everywhere.

[[fig:hadoop_stack]]

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Core Hadoop

Core Hadoop is the part shipped by the Apache Hadoop project itself: HDFS for storage, YARN for resources and MapReduce for processing, plus the security model that wraps them. Everything later in this section, and much of Spark, runs on these pieces or copies their design.

The group moves from the bottom of the stack upwards. First HDFS, in three steps: how it works inside, how you keep it healthy in production, and how it scales past one NameNode. Then YARN, which shares the cluster between applications, then MapReduce, the programming model that started it all, and finally the security layer (Kerberos, Ranger and Knox) that every real cluster needs.

### HDFS Architecture & Internals

::: plain
Imagine a huge library with a single catalogue desk. The librarian at the desk does not hold any books; she only knows which shelf each chapter of each book is on. The books themselves are cut into big chapters, and every chapter is photocopied onto three different shelves, at least one of them in another room, so a fire in one room or a collapsed shelf never loses a chapter. HDFS works exactly like this: the **NameNode** is the catalogue desk, the **DataNodes** are the shelves, and the chapters are **blocks**.
:::

**What HDFS is for.** The Hadoop Distributed File System is a file system spread over many machines, modelled on Google's GFS paper (2003). Its design goals are unusual: very large files (gigabytes to terabytes), reads that stream through whole files rather than jumping around, files that are written once and then only appended to, and commodity hardware where a failed disk is a daily event, not an emergency. It gives up what it does not need for that job: there is no random write into the middle of a file, and only one writer per file at a time.

**Blocks.** HDFS splits every file into **blocks** of a fixed size, 128 MB by default (`dfs.blocksize`), and stores each block as an ordinary file on a DataNode's local disks. A 1 GB file becomes 8 blocks; a 1 KB file becomes one block that uses only 1 KB of disk (a block is a maximum, not an allocation). Large blocks keep metadata small and make reads sequential: at 150 MB/s, reading 128 MB takes nearly a second, so the time spent finding the block is negligible.

**The NameNode** holds the whole **namespace** (the directory tree, file names, permissions, and which blocks make up each file) in memory, so every metadata operation is fast. It makes that metadata durable with two files on disk: the **fsimage**, a full snapshot of the namespace, and the **edit log**, a journal of every change since the snapshot. It does *not* persist where blocks live. Each DataNode sends a **block report** (the list of blocks it holds) when it starts and periodically afterwards, and the NameNode rebuilds its block map from those reports. Every DataNode also sends a **heartbeat** every 3 seconds; the reply carries commands such as "copy block X to node Y" or "delete block Z".

**Reading a file.** The client asks the NameNode for the block locations of the file, gets back a list of DataNodes for each block sorted by network distance, and then reads the bytes directly from the nearest DataNode. The NameNode never touches file data, which is why one NameNode can serve thousands of clients. Every 512 bytes of data carry a CRC32C checksum (stored in a `.meta` file next to the block), and the client verifies them as it reads. A mismatch makes it report the bad replica and read from another.

**Writing a file and the replication pipeline.** To write, the client asks the NameNode to add a block; the NameNode picks target DataNodes (three by default, `dfs.replication`) and grants the client a **lease**, which is the single-writer lock on that file. The client then streams data in **packets** (about 64 KB each) to the first DataNode, which stores each packet and forwards it to the second, which forwards it to the third. Acknowledgements flow back along the same pipeline, and the client keeps every packet in an **ack queue** until all three nodes have confirmed it. If a DataNode in the pipeline fails, the client rebuilds the pipeline from the healthy nodes, carries on writing, and the NameNode later notices the block is under-replicated and copies it to a new node.

[[fig:hadoop_hdfs]]

**Rack awareness.** HDFS knows which rack each DataNode is in (you give it a topology script or class through `net.topology.script.file.name`). The default placement policy for three replicas is: the first on the writer's own node (or a random node if the client is outside the cluster), the second on a node in a *different* rack, and the third on a different node in the *same* rack as the second. The data survives the loss of a whole rack, yet only one of the three copies crosses the rack-to-rack link during the write.

```bash
hdfs dfs -mkdir -p /data/events
hdfs dfs -put events-2026-11-23.json /data/events/          # write: pipeline of 3 DataNodes
hdfs dfs -ls -h /data/events
hdfs fsck /data/events -files -blocks -locations -racks     # where every block replica lives
```

**NameNode high availability.** In the original design the NameNode was a single point of failure. Production clusters now run an **Active** NameNode and one or more **Standby** NameNodes (more than one standby since Hadoop 3.0). The active writes every edit to a group of **JournalNodes**, usually three or five, and an edit counts as written once a majority (a **quorum**) has stored it; this is the **Quorum Journal Manager**, which tolerates the loss of a minority (one of three, two of five). Standbys tail the JournalNodes and replay the edits, so their namespace stays current. DataNodes send heartbeats and block reports to every NameNode, so a standby already knows where blocks are. A **ZKFailoverController** (ZKFC) process next to each NameNode watches its health and holds a lock in ZooKeeper; if the active dies, the lock expires and the other ZKFC promotes its standby. **Fencing** stops a "zombie" old active from writing: the JournalNodes accept writes only from the NameNode with the newest epoch number.

**Erasure coding.** Three-way replication costs 200 percent extra disk. Hadoop 3 added **erasure coding** (EC): instead of copying a block, HDFS **stripes** data across several DataNodes in cells (1 MB in the built-in policies) and adds **parity** cells computed with **Reed-Solomon** codes. The policy `RS-6-3-1024k` writes 6 data cells and 3 parity cells per stripe; any 6 of the 9 are enough to rebuild the rest.

```python
# Raw storage needed for 100 TB of data, and how many lost blocks each layout survives
layouts = {
    #                 data units, extra units
    "3x replication": (1, 2),
    "RS-3-2":         (3, 2),
    "RS-6-3":         (6, 3),
    "RS-10-4":        (10, 4),
}
for name, (data, extra) in layouts.items():
    factor = (data + extra) / data
    print(f"{name:15} {100 * factor:6.1f} TB raw  overhead {factor:.2f}x  survives {extra} losses")
# 3x replication   300.0 TB raw  overhead 3.00x  survives 2 losses
# RS-3-2           166.7 TB raw  overhead 1.67x  survives 2 losses
# RS-6-3           150.0 TB raw  overhead 1.50x  survives 3 losses
# RS-10-4          140.0 TB raw  overhead 1.40x  survives 4 losses
```

EC halves the storage bill and tolerates more failures, but every read is a network read from several nodes (no single node holds a whole block, so data locality is lost), and rebuilding a lost cell costs CPU and reads from six other nodes. It suits cold, large files. EC is set per directory:

```bash
hdfs ec -listPolicies
hdfs ec -enablePolicy -policy RS-6-3-1024k
hdfs ec -setPolicy -path /data/archive -policy RS-6-3-1024k   # new files under it are striped
```

> **Pitfall:** Setting an EC policy does not convert existing files; only new files are striped. Rewrite old files (for example with DistCp into the EC directory) to convert them. Also count racks: RS-6-3 needs at least 9 DataNodes, and to survive a whole-rack loss you need at least (6 + 3) / 3 = 3 racks, so that no rack holds more cells of a stripe than there are parity cells.

**Federation** removes the last single-NameNode limit by splitting the namespace over several independent NameNodes. It is big enough to have its own topic, Hadoop Federation & Namespace, below.

> **Pitfall:** HDFS is not a POSIX file system. You cannot update bytes in the middle of a file, a file being written is invisible to readers until a block completes or the writer calls `hflush`, and two clients cannot write the same file. Tools that expect a local disk (databases, editors) do not work on it.

::: deeper
**How the NameNode starts.** On start it loads the fsimage, replays the edit log, then waits in **safe mode** (read-only) until DataNodes have reported enough blocks: by default 99.9 percent of blocks must have at least one replica (`dfs.namenode.safemode.threshold-pct`). On a cluster with hundreds of millions of blocks this can take many minutes, one more reason HA matters. In a non-HA cluster a **Secondary NameNode** periodically merges the edit log into a new fsimage; despite the name, it is not a standby and cannot take over. With HA, the standby performs these checkpoints.

**Generation stamps and lease recovery.** Every block has a generation stamp that increases when its pipeline is rebuilt, so a stale replica left on a node that dropped out of the pipeline is recognised and deleted. If a writer dies, its lease expires (after a soft limit of about a minute and a longer hard limit) and the NameNode runs **lease recovery**: it makes the replicas agree on the last block's length and closes the file.

**Short-circuit local reads.** When the client runs on the same machine as the replica (the normal case for MapReduce and Spark tasks), it can read the block file directly through a Unix domain socket instead of streaming through the DataNode process (`dfs.client.read.shortcircuit`), saving a copy and a context switch.

**Observer NameNode.** Since Hadoop 3.3, an **Observer** is a standby that also serves read requests (listings, `getBlockLocations`), using a state-id protocol so clients never read metadata older than their own writes. Read-heavy clusters use it to take load off the active.

**Storage types and policies.** DataNode directories can be tagged `DISK`, `SSD`, `ARCHIVE` or `RAM_DISK`, and policies such as `HOT`, `WARM`, `COLD` and `ALL_SSD` place replicas accordingly; the **Mover** tool migrates existing blocks after a policy change.

**On Google Cloud.** A Managed Service for Apache Spark cluster still runs HDFS on its worker disks, but Google recommends keeping data in Cloud Storage (paths `gs://bucket/...`, through the Cloud Storage connector) and using cluster HDFS only for scratch data. Storage then outlives the cluster, so clusters can be created per job and deleted afterwards.

> **Interview:** "Why does HDFS use 128 MB blocks when local file systems use 4 KB?" To keep the NameNode's in-memory metadata small (one object per block), to make each read long and sequential so seek time is negligible, and to give MapReduce one large, efficient unit of work per block. The cost is that small files waste NameNode memory, because each still needs its own file and block objects.

> **Interview:** "What happens when the active NameNode crashes?" The ZKFC's ZooKeeper session expires, the standby's ZKFC wins the lock, fences the old active, and the standby finishes replaying edits from the JournalNode quorum and becomes active. DataNodes are already reporting to it, so the cluster resumes in seconds to a minute; clients retry through the failover proxy configured for the nameservice.
:::

::: check
?? Does the NameNode store block locations on disk?
No. It persists the namespace (fsimage plus edit log) but rebuilds the block-to-DataNode map from the block reports that DataNodes send at startup and periodically.

?? Where do the three replicas of a block go under the default placement policy?
The first on the writer's node (or a random node), the second on a node in a different rack, the third on another node in that same second rack. One rack can fail without losing the block, and only one copy crosses racks during the write.

?? Why is erasure coding usually reserved for cold data?
It saves storage (1.5x instead of 3x for RS-6-3) but loses data locality, because every read gathers cells from several nodes, and reconstruction after a failure costs CPU and network. Hot data read by many jobs is cheaper to keep replicated.
:::

::: resources
- Docs: [HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) · the official design: blocks, replication, placement and the read and write paths
- Docs: [HDFS High Availability using the Quorum Journal Manager](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html) · JournalNodes, ZKFC and fencing, with full configuration
- Docs: [HDFS Erasure Coding](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSErasureCoding.html) · striping, policies, hardware needs and the `hdfs ec` command
- Paper: [The Hadoop Distributed File System (MSST 2010)](https://pages.cs.wisc.edu/~akella/CS838/F15/838-CloudPapers/hdfs.pdf) · the original paper by the Yahoo! team, short and readable
- Paper: [The Google File System](https://research.google/pubs/the-google-file-system/) · the 2003 design HDFS copied, and why it made the choices it did
- Book: [Hadoop: The Definitive Guide, 4th edition](https://www.oreilly.com/library/view/hadoop-the-definitive/9781491901687/) by Tom White · chapter 3 walks through HDFS reads, writes and coherency in detail
:::

### HDFS operations: replication, balancing and failure handling

::: plain
Running HDFS is like looking after a big warehouse where shelves break every week. Nobody panics when a shelf collapses, because every item has copies elsewhere, but someone has to notice, make new copies, move stock off overloaded shelves onto new empty ones, and retire old shelves without losing anything. This topic is that someone's job: the daily checks, the tools that move data around, and what the system does by itself when hardware fails.
:::

**The health check.** Two commands tell you most of what you need. `hdfs dfsadmin -report` shows capacity, usage and state per DataNode. `hdfs fsck` walks the namespace and classifies blocks:

```bash
hdfs dfsadmin -report | head -20        # configured capacity, DFS used, live and dead DataNodes
hdfs fsck / | tail -20                  # summary for the whole namespace
hdfs fsck / -list-corruptfileblocks     # only the files you may have lost
```

The `fsck` summary uses words worth knowing precisely:

| State | Meaning | What happens |
|---|---|---|
| Under-replicated | Fewer live replicas than the file's replication factor | NameNode schedules copies automatically |
| Over-replicated | More replicas than required (often after a node returns) | Extra replicas are deleted |
| Mis-replicated | Enough replicas but they break the rack policy | Moved to satisfy the policy |
| Missing / corrupt | No healthy replica left | Data is lost unless a replica comes back; needs a human |

`fsck` reads only metadata from the NameNode, so it is safe to run on a busy cluster, although a full scan of a large namespace takes a while.

**Changing replication.** The replication factor is per file. `hdfs dfs -setrep -w 2 /data/tmp` lowers it for a directory tree (`-w` waits until the change is done). Lowering replication of scratch data is a quick way to free space; raising it for small, hot reference files spreads the read load.

**When a DataNode dies.** Heartbeats arrive every 3 seconds. The NameNode declares a node dead after `2 x dfs.namenode.heartbeat.recheck-interval + 10 x dfs.heartbeat.interval`, which with defaults is 2 x 5 minutes + 10 x 3 seconds = 10.5 minutes. The wait is deliberate: a node that reboots quickly should not trigger terabytes of needless copying. After that, every block that node held is under-replicated, and the NameNode schedules re-replication, throttled so it does not saturate the network (`dfs.namenode.replication.max-streams` and related settings). With thousands of nodes this happens continuously and quietly.

**When a disk dies.** By default a DataNode shuts itself down when any one of its data disks fails (`dfs.datanode.failed.volumes.tolerated` is 0). On nodes with 12 or more JBOD disks, set it to 1 or 2 so one bad disk only loses that disk's replicas instead of taking the whole node offline.

**Balancing between nodes.** New DataNodes start empty and HDFS never moves existing blocks by itself, so a cluster that has grown becomes uneven. The **balancer** moves blocks until every node's usage is within a threshold of the cluster average:

```bash
hdfs dfsadmin -setBalancerBandwidth 104857600   # allow 100 MB/s per DataNode for balancing
hdfs balancer -threshold 10                    # each node within 10 percentage points of the mean
```

The **disk balancer** solves the other imbalance, between disks inside one node (after a disk is replaced, for example): `hdfs diskbalancer -plan <host>`, then `-execute` with the generated plan.

**Decommissioning and maintenance.** To retire a node safely, add its hostname to the exclude file (`dfs.hosts.exclude`) and run `hdfs dfsadmin -refreshNodes`. The node enters *Decommission In Progress*, its replicas are copied elsewhere, and it reaches *Decommissioned* only when nothing depends on it; only then do you power it off. For a short planned outage (an OS patch) use **maintenance state** instead, which keeps replicas where they are for a set time and avoids a full re-replication storm.

**Protecting against humans.** Most real data loss is a mistaken delete, not hardware. Three features help:

- **Trash**: with `fs.trash.interval` set (minutes), `hdfs dfs -rm` moves files to the user's `.Trash` folder instead of deleting them. `-skipTrash` bypasses it, so avoid it in scripts.
- **Snapshots**: `hdfs dfsadmin -allowSnapshot /data/warehouse` then `hdfs dfs -createSnapshot /data/warehouse before-migration`. A snapshot is a read-only, copy-on-write view stored as metadata, so it is instant and costs nothing until files change.
- **Quotas**: `hdfs dfsadmin -setQuota 1000000 /user/ana` caps the number of names; `-setSpaceQuota 10t` caps raw space (including replication).

**The small-files problem.** Each file, directory and block is an object of roughly 150 bytes in NameNode heap. The same petabyte stored in different file sizes:

```python
# NameNode heap for 1 PB of data, by average file size (rule of thumb: ~150 bytes per file and per block)
PB, BLOCK, PER_OBJECT = 10**15, 128 * 2**20, 150
for label, size in [("1 MiB", 2**20), ("128 MiB", 128 * 2**20), ("1 GiB", 2**30)]:
    files = PB // size
    blocks = files * -(-size // BLOCK)          # ceiling division: blocks per file
    heap_gb = (files + blocks) * PER_OBJECT / 1e9
    print(f"{label:>7} files: {files:>11,} files, {blocks:>11,} blocks, ~{heap_gb:,.0f} GB heap")
#   1 MiB files: 953,674,316 files, 953,674,316 blocks, ~286 GB heap
# 128 MiB files:   7,450,580 files,   7,450,580 blocks, ~2 GB heap
#   1 GiB files:     931,322 files,   7,450,576 blocks, ~1 GB heap
```

No JVM serves a 286 GB heap comfortably, and every one of those files also means one map task, one open and one RPC. Small files come from streaming jobs that write every minute, from too many output partitions, and from over-partitioned Hive tables. The fixes are upstream: compact output into files of a few hundred megabytes (a periodic compaction job, `coalesce` in Spark, Hive's `hive.merge.*` settings), or pack old files into a **Hadoop Archive** (`hadoop archive -archiveName logs.har -p /logs 2025 /archive`).

> **Pitfall:** Running the balancer during the day at full bandwidth competes with production jobs for disk and network. Set a modest bandwidth, run it in quiet hours, and remember it can take days on a large cluster.

> **Tip:** Alert on these NameNode metrics rather than discovering problems in `fsck`: missing blocks, under-replicated blocks, dead DataNodes, volume failures, heap usage and RPC queue time. A steadily rising under-replicated count means re-replication cannot keep up with failures.

::: deeper
**Re-replication priority.** The NameNode keeps under-replicated blocks in priority queues: blocks with a single remaining replica go first, then blocks whose replicas are all on one rack, then the rest. This is why losing a node is routine while losing two nodes in different racks at once is the scenario that actually threatens data.

**Corrupt replicas.** DataNodes run a **block scanner** that reads every replica in the background (about every three weeks by default) and checks checksums, plus a **directory scanner** that reconciles on-disk files with in-memory state. Corrupt replicas are reported and replaced from healthy ones, so silent disk corruption (bit rot) is repaired before a reader hits it.

**NameNode failover in practice.** `hdfs haadmin -getServiceState nn1` shows which NameNode is active; `hdfs haadmin -failover nn1 nn2` performs a graceful, planned failover for upgrades. Rolling upgrades (`hdfs dfsadmin -rollingUpgrade prepare`) let you upgrade NameNodes and DataNodes one at a time without downtime.

**Edit log and fsimage hygiene.** If checkpoints stop (a broken standby is the usual cause), the edit log grows without bound and the next restart replays hours of edits. Monitor the time since the last checkpoint.

**Cloud contrast.** In Cloud Storage, replication, scrubbing, balancing and hardware replacement are the provider's problem, and durability is designed for eleven nines. The operational work that remains is data layout (avoid tiny objects), lifecycle rules instead of manual cleanup, and object versioning or soft delete as the equivalent of trash and snapshots.

> **Interview:** "A DataNode goes offline for five minutes during a reboot. How much data does HDFS copy?" Usually none: it is only declared dead after about 10.5 minutes with default settings, so it returns before re-replication starts. If it stays away longer, its blocks are re-replicated, and when it comes back the now-surplus replicas are deleted as over-replicated.
:::

::: check
?? How long does the NameNode wait before declaring a silent DataNode dead with default settings, and why so long?
About 10.5 minutes (2 x 5-minute recheck interval + 10 x 3-second heartbeats). Waiting avoids copying terabytes for nodes that are only briefly unavailable, such as during a reboot.

?? What is the difference between the balancer and the disk balancer?
The balancer moves blocks between DataNodes so each node's usage is near the cluster average. The disk balancer moves blocks between the disks inside one DataNode.

?? Why do millions of small files hurt even when they use little disk?
Every file and block is an object in NameNode memory, so heap and RPC load grow with the file count, not the byte count. Each small file also becomes its own task in MapReduce and similar engines.
:::

::: resources
- Docs: [HDFS Users Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html) · safe mode, fsck, balancer, rack awareness and upgrades in one page
- Docs: [HDFS Commands Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html) · every `hdfs` admin command with its options
- Docs: [HDFS Disk Balancer](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSDiskbalancer.html) · planning and running intra-node balancing
- Docs: [HDFS Snapshots](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsSnapshots.html) · how copy-on-write snapshots work and how to manage them
- Paper: [The Hadoop Distributed File System (MSST 2010)](https://pages.cs.wisc.edu/~akella/CS838/F15/838-CloudPapers/hdfs.pdf) · the replication, balancer and block scanner design from Yahoo!'s large clusters
- Book: [Hadoop Operations](https://www.oreilly.com/library/view/hadoop-operations/9781449327279/) by Eric Sammer · a practitioner's guide to running clusters; old versions, still-valid habits
:::

### Hadoop Federation & Namespace

::: plain
A single catalogue desk works until the library grows so large that the librarian cannot remember every book or answer every question fast enough. The fix is to open several desks, each responsible for a different part of the library (one for science, one for history), while all of them share the same shelves. A sign at the entrance, or a receptionist, tells visitors which desk to ask. That is HDFS federation: several NameNodes, each owning part of the namespace, sharing one pool of DataNodes, with a mount table that sends each path to the right NameNode.
:::

**Why one NameNode stops being enough.** The NameNode keeps the entire namespace in one JVM heap and serves every metadata call (open, list, create, delete, block report) from one process. That gives three hard limits:

- **Memory**: a few hundred million files and blocks need a heap of 100 GB or more, where garbage-collection pauses become a risk (see JVM Tuning for Data Systems in [Storage internals and hardware](#hardware)).
- **Throughput**: all clients share one RPC queue, so one team's job that lists millions of files slows everyone.
- **Isolation**: one bad workload, a namespace-wide permission mistake, or a restart affects every user of the cluster.

High availability does not help with any of these, because only the active NameNode serves writes.

**HDFS Federation.** Federation runs several independent NameNodes, each managing its own **namespace volume**: a directory tree plus a **block pool** (the set of blocks belonging to that namespace). The NameNodes do not talk to each other. Every DataNode registers with all of them, stores blocks for every block pool in separate directories, and sends a separate block report to each NameNode. Storage is shared; metadata is partitioned. Each namespace can also have its own HA pair, so a large deployment has several **nameservices** (`ns1`, `ns2`...), each with an active and a standby.

| Approach | Namespace | Client sees | Main limit |
|---|---|---|---|
| Single NameNode (with HA) | One tree, one heap | `hdfs://ns1/...` | Memory and RPC of one process |
| Federation + ViewFs | Several trees | One tree, through a client-side mount table | Mount table must be copied to every client |
| Router-based Federation | Several trees | One tree, through Routers | Routers and State Store are extra services |
| Object storage (Ozone, Cloud Storage) | Flat key space, metadata scaled out | Buckets and keys, or a Hadoop-compatible file system | Different rename and listing semantics |

**ViewFs: the client-side mount table.** Applications do not want to know that `/user` lives on `ns1` and `/data` on `ns2`. **ViewFs** is a client-side file system (`viewfs://`) configured with a mount table in `core-site.xml`, much like `/etc/fstab` on Linux. Written as name and value pairs:

```text
fs.defaultFS                                   = viewfs://prod
fs.viewfs.mounttable.prod.link./user           = hdfs://ns1/user
fs.viewfs.mounttable.prod.link./data/warehouse = hdfs://ns2/warehouse
fs.viewfs.mounttable.prod.link./tmp            = hdfs://ns1/tmp
```

A client opening `/data/warehouse/orders` is sent straight to `ns2`. There is no extra hop, but the mount table lives in every client's configuration, so changing it means redeploying configuration to every gateway machine, job and notebook, and a client with stale configuration silently looks in the wrong place.

**Router-based Federation (RBF).** RBF moves the mount table to the server side. One or more stateless **Routers** expose the normal NameNode RPC interface, so clients keep using `hdfs://` and think they are talking to one NameNode. A Router looks up the path in the mount table, which is kept in a shared **State Store** (usually ZooKeeper), and forwards the call to the right nameservice. Routers also track which NameNodes are active, so clients need no HA configuration for each namespace.

```bash
# Map a path in the global namespace to a nameservice and a path inside it
hdfs dfsrouteradmin -add /data/warehouse ns2 /warehouse
hdfs dfsrouteradmin -add /user ns1 /user
hdfs dfsrouteradmin -ls /                      # show the mount table
# One mount point can spread over several namespaces, choosing a destination per file
hdfs dfsrouteradmin -add /logs ns1,ns2 /logs -order HASH
```

Routers add a network hop, but they are cheap and you run several behind a load balancer. They also enforce quotas across namespaces and can mark mount points read-only during maintenance. Large operators run RBF at the scale of many nameservices and billions of objects.

**Namespace isolation in practice.** A typical split gives each heavy tenant its own nameservice: one for the shared warehouse, one for user home directories and scratch space, one for a streaming team that creates many small files. A runaway listing in `/tmp` then queues only on `ns1`, and the warehouse NameNode is untouched. Plan the split on directory boundaries, because a single directory cannot span two namespaces (except through multi-destination mount points, which spread *files* across namespaces).

**Moving data between namespaces.** A rename is a metadata operation inside one NameNode. Across namespaces there is no shared metadata, so `rename` from `ns1` to `ns2` fails, and the data must be physically copied. The standard tool is **DistCp** (distributed copy), which runs a MapReduce job where each map task copies a set of files in parallel:

```bash
hadoop distcp -update -p hdfs://ns1/projects/clickstream hdfs://ns2/projects/clickstream
```

`-update` copies only files that differ, so you can run it repeatedly while the source is live, then stop writers, run a final pass, and switch the mount table entry. Newer Hadoop 3 releases also include a federation balance tool (FedBalance) that automates this copy, then updates the Router mount table. DistCp is also the usual tool to move data from HDFS into Cloud Storage, with a `gs://` destination.

> **Pitfall:** Federation partitions metadata, not data placement. All namespaces share the same DataNodes and disks, so a namespace that fills the disks still affects everyone. Use space quotas per namespace or per directory.

> **Tip:** Start with Router-based Federation rather than ViewFs on any new federated cluster. Clients need no special configuration, and you can rebalance namespaces by changing the mount table in one place.

::: deeper
**Block pools inside a DataNode.** Each block pool has its own directory under every DataNode data directory (`current/BP-<id>-<ip>-<timestamp>`), and the DataNode runs one service thread per NameNode for heartbeats and reports. A **ClusterID** ties all NameNodes and DataNodes of one federated cluster together; a DataNode refuses to join NameNodes with a different ClusterID, which protects against accidentally mixing clusters.

**Keeping `hdfs://` URIs with ViewFs.** Rewriting every job and Hive table location from `hdfs://ns1/...` to `viewfs://...` is painful. **ViewFileSystemOverloadScheme** lets the mount table be applied under the `hdfs` scheme itself, so existing paths keep working while being routed; mount table entries can even point a path at a different file system, such as an object store, which is useful during a migration.

**RBF and security.** In a Kerberos cluster the Router authenticates clients itself and issues its own delegation tokens, because a token issued by one NameNode is meaningless to another. The Router then talks to NameNodes as a trusted proxy user.

**Why the namespace is hard to scale in general.** A hierarchical file system needs atomic operations across directories (rename, recursive delete, permission checks up the path). Partitioning by subtree keeps those operations local to one server; that is why both federation and Ozone place each namespace or bucket on one metadata service, and why object stores such as Cloud Storage that use flat names make directory rename a copy of every object (except buckets with hierarchical namespace enabled, which support atomic folder renames).

> **Interview:** "HA versus federation: which solves which problem?" HA removes the NameNode as a single point of *failure*: a standby takes over. Federation removes it as a single point of *scale*: several active NameNodes split the namespace. Large clusters use both, with an HA pair for each nameservice.
:::

::: check
?? In a federated cluster, which components are shared and which are partitioned?
DataNodes and their disks are shared: every DataNode stores blocks for every block pool. The namespace and its block metadata are partitioned: each NameNode owns one namespace volume and its block pool.

?? What is the main operational difference between ViewFs and Router-based Federation?
ViewFs keeps the mount table in every client's configuration, so changes must be redeployed everywhere. RBF keeps it in a shared State Store behind Routers, so clients use plain `hdfs://` paths and the mapping changes in one place.

?? Why does `hdfs dfs -mv` fail between two namespaces, and what do you use instead?
Rename is a metadata operation inside one NameNode, and namespaces share no metadata. Copy the data with DistCp (or FedBalance under RBF), then delete the source.
:::

::: resources
- Docs: [HDFS Federation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/Federation.html) · the design, block pools, ClusterID and configuration
- Docs: [HDFS Router-based Federation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs-rbf/HDFSRouterFederation.html) · Routers, the State Store, mount tables and `dfsrouteradmin`
- Docs: [ViewFs Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html) · client-side mount tables and the overload scheme
- Docs: [DistCp Guide](https://hadoop.apache.org/docs/stable/hadoop-distcp/DistCp.html) · options for incremental, preserving and cross-cluster copies
- Book: [Hadoop: The Definitive Guide, 4th edition](https://www.oreilly.com/library/view/hadoop-the-definitive/9781491901687/) by Tom White · the HDFS chapter covers federation and HA together
- Docs: [Cloud Storage connector](https://docs.cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) · how `gs://` paths behave as a Hadoop file system on Google Cloud
:::

### YARN Resource Management

::: plain
Think of a cluster as a big shared kitchen. Many cooks (applications) want burners and counter space (CPU and memory) at the same time. YARN is the kitchen manager: it knows how much space every counter has, keeps a list of who is waiting, and hands each cook a reserved spot for a while. Each cook brings their own head chef (the **ApplicationMaster**) who decides what to cook where; the manager only decides who gets which spot. That split, one manager for space and one planner per meal, is what lets MapReduce, Spark, Tez and Flink share the same machines.
:::

**Why YARN exists.** In Hadoop 1, one JobTracker both scheduled resources and tracked every task of every MapReduce job. It could not scale past a few thousand nodes and it only ran MapReduce. YARN ("Yet Another Resource Negotiator", Hadoop 2) split those jobs: a cluster-wide **ResourceManager** allocates resources, and each application gets its own **ApplicationMaster** that plans and monitors its tasks. Any engine can now run on Hadoop by writing an ApplicationMaster.

**The pieces.**

- **ResourceManager (RM)**: one active per cluster (with a standby for HA through ZooKeeper). Its **Scheduler** decides who gets resources; its **ApplicationsManager** accepts submissions and starts each application's first container.
- **NodeManager (NM)**: one per worker machine. It advertises the node's resources (`yarn.nodemanager.resource.memory-mb`, `yarn.nodemanager.resource.cpu-vcores`), launches containers, enforces their limits, and reports health in heartbeats.
- **Container**: a slice of one node (for example 4 GB and 2 vcores) in which a process runs: an AM, a map task, or a Spark executor.
- **ApplicationMaster (AM)**: the application's own coordinator, running in a container. Spark's driver (in cluster mode) or a MapReduce `MRAppMaster` are AMs.

**An application's lifecycle.**

1. The client submits an application to the RM with the command to launch its AM.
2. The RM finds a container for the AM and asks that node's NM to start it.
3. The AM registers with the RM, then requests containers in its heartbeats, with sizes and preferred locations (nodes or racks holding the input blocks).
4. The scheduler grants containers as resources free up; the AM asks the NMs to launch its tasks in them.
5. The AM watches its tasks, retries failures, and finally unregisters; the NMs clean up and aggregate logs to HDFS or Cloud Storage.

[[fig:hadoop_yarn]]

```bash
yarn application -list -appStates RUNNING      # what is running, in which queue
yarn application -status application_1763900000000_0042
yarn logs -applicationId application_1763900000000_0042 | less   # aggregated container logs
yarn node -list -all                           # NodeManagers and their state
yarn queue -status etl                         # capacity and usage of a queue
yarn application -kill application_1763900000000_0042
```

**Container sizing.** Requests are rounded up to a multiple of `yarn.scheduler.minimum-allocation-mb` (1024 MB by default) and capped at `yarn.scheduler.maximum-allocation-mb`. Spark adds overhead memory on top of the executor heap, so the rounding matters:

```python
import math
node_mb = 57344          # memory each NodeManager offers (yarn.nodemanager.resource.memory-mb)
min_alloc = 1024         # yarn.scheduler.minimum-allocation-mb: requests round up to a multiple of this
for heap_mb in (2048, 4096, 8192):
    overhead = max(384, int(heap_mb * 0.10))           # Spark's default executor memory overhead
    asked = heap_mb + overhead
    granted = math.ceil(asked / min_alloc) * min_alloc
    print(f"heap {heap_mb:5} MB -> asks {asked:5} MB -> gets {granted:5} MB, "
          f"{node_mb // granted:2} per node, {granted - asked:3} MB rounded away each")
# heap  2048 MB -> asks  2432 MB -> gets  3072 MB, 18 per node, 640 MB rounded away each
# heap  4096 MB -> asks  4505 MB -> gets  5120 MB, 11 per node, 615 MB rounded away each
# heap  8192 MB -> asks  9011 MB -> gets  9216 MB,  6 per node, 205 MB rounded away each
```

By default the Capacity Scheduler counts only memory (`DefaultResourceCalculator`). Switch to `DominantResourceCalculator` when CPU matters, or a node may be "full" of containers that all want CPU.

**Capacity Scheduler.** The default scheduler in Apache Hadoop organises resources as a tree of **queues**, each with a guaranteed share. Queues may grow beyond their share when others are idle (up to `maximum-capacity`), and **user limits** stop one person from taking a whole queue. In `capacity-scheduler.xml`, written as name and value pairs:

```text
yarn.scheduler.capacity.root.queues                      = etl,adhoc,ml
yarn.scheduler.capacity.root.etl.capacity                = 60
yarn.scheduler.capacity.root.etl.maximum-capacity        = 100
yarn.scheduler.capacity.root.adhoc.capacity              = 30
yarn.scheduler.capacity.root.adhoc.maximum-capacity      = 50
yarn.scheduler.capacity.root.adhoc.user-limit-factor     = 1
yarn.scheduler.capacity.root.ml.capacity                 = 10
yarn.scheduler.capacity.root.ml.maximum-capacity         = 40
yarn.scheduler.capacity.root.ml.accessible-node-labels   = gpu
```

Apply changes without a restart with `yarn rmadmin -refreshQueues`, and submit to a queue with `--queue etl` (Spark) or `-Dmapreduce.job.queuename=etl`.

**Fair Scheduler.** The Fair Scheduler aims to give every running application (or queue) an equal share over time, weighted by configuration, with minimum shares and Dominant Resource Fairness for mixed CPU and memory. It was the usual choice in Cloudera's older distributions; Cloudera's current platform uses the Capacity Scheduler, which gained many fair-sharing features, and Hadoop ships a converter (`yarn fs2cs`) for migrations.

| | Capacity Scheduler | Fair Scheduler |
|---|---|---|
| Core idea | Guaranteed capacity per queue, elastic up to a maximum | Equal (weighted) share among active applications |
| Within a queue | FIFO or fair ordering policy | Fair, FIFO or DRF policy |
| Typical use | Multi-team clusters with agreed shares | Many small interactive jobs |
| Status | Default in Apache Hadoop, actively developed | Supported, less common in new deployments |

**Node labels.** Labels partition nodes, for example GPU nodes or nodes with large memory. A label is **exclusive** (only queues that can use it get those nodes) or **non-exclusive** (idle labelled nodes can serve the default partition). `yarn rmadmin -addToClusterNodeLabels "gpu(exclusive=true)"` then `yarn rmadmin -replaceLabelsOnNode "worker17=gpu"`.

**Preemption.** Elasticity means a queue can borrow idle capacity, but then the owner comes back. With the scheduling monitor enabled (`yarn.resourcemanager.scheduler.monitor.enable=true`), the Capacity Scheduler's preemption policy picks containers in over-capacity queues, first asks their AMs to give them back, and kills them after a grace period. Engines that tolerate lost containers (MapReduce, Spark with retries) cope well; long services do not, so put them in queues that are never over capacity.

> **Pitfall:** "Container killed by YARN for exceeding memory limits" means the whole process (heap plus native memory, Python workers, buffers) exceeded the container size, not that the heap was full. For Spark, raise `spark.executor.memoryOverhead`; for MapReduce, keep `-Xmx` around 80 percent of `mapreduce.map.memory.mb`.

> **Tip:** On Google Cloud, Managed Service for Apache Spark clusters run YARN, and their autoscaling policies add or remove workers based on YARN's pending and available memory. Graceful decommissioning lets running containers finish before a node is removed.

::: deeper
**Locality and delay scheduling.** The AM asks for containers on the nodes that hold its input blocks. The scheduler can wait a few scheduling opportunities for a node-local slot before accepting rack-local, then any node (`yarn.scheduler.capacity.node-locality-delay`). On cloud clusters that read from Cloud Storage there is no locality to win, so this delay only costs time.

**RM restart and HA.** The RM persists application state in a state store (ZooKeeper is the usual one). After failover, running AMs re-register and NMs resync their containers, so applications keep running instead of restarting (work-preserving restart).

**Opportunistic containers and resource types.** Since Hadoop 3, YARN can schedule **opportunistic** containers that queue at the NM and run when the node has idle resources, and it supports extra **resource types** such as GPUs (`yarn.io/gpu`) and FPGAs, with isolation through cgroups and Docker containers.

**YARN versus Kubernetes.** Kubernetes now runs much of the work YARN used to: Spark on Kubernetes is mature and Flink runs natively there. YARN still has advantages for batch-heavy clusters: hierarchical queues with elastic capacity and preemption, data locality, and decades of tuning for short-lived containers. Kubernetes batch schedulers such as Apache YuniKorn (which brings YARN-style queues to Kubernetes), Volcano and Kueue close the gap.

> **Interview:** "What is the difference between the ResourceManager and an ApplicationMaster?" The RM is one per cluster and only decides which application gets which containers. An AM is one per application and decides what to run in those containers, tracks progress and handles task failures. Splitting them is what let YARN scale and run engines other than MapReduce.
:::

::: check
?? A Spark executor asks for 4 GB of heap on a cluster with a 1 GB minimum allocation. How big is the container?
The request is 4 GB plus overhead (max of 384 MB and 10 percent, so about 410 MB), roughly 4.4 GB, which YARN rounds up to 5 GB.

?? Queue `ml` has capacity 10 and maximum-capacity 40. What does that mean when the cluster is idle, and when it is busy?
When idle, `ml` can use up to 40 percent of the cluster. When other queues need their guaranteed shares back, `ml` is only guaranteed 10 percent, and preemption (if enabled) can reclaim the rest.

?? Why does YARN kill containers whose JVM heap is not full?
YARN enforces the container's limit on the whole process's memory, including off-heap buffers, thread stacks, native libraries and child Python processes, not just the heap.
:::

::: resources
- Docs: [Apache Hadoop YARN architecture](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html) · the official overview of RM, NM and AM
- Docs: [Capacity Scheduler](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html) · every queue property, preemption and ordering policies
- Docs: [YARN Node Labels](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/NodeLabel.html) · exclusive and non-exclusive partitions and how queues use them
- Paper: [Apache Hadoop YARN: Yet Another Resource Negotiator (SoCC 2013)](https://dl.acm.org/doi/10.1145/2523616.2523633) · the design paper explaining why the JobTracker was split
- Book: [Hadoop: The Definitive Guide, 4th edition](https://www.oreilly.com/library/view/hadoop-the-definitive/9781491901687/) by Tom White · chapter 4 on YARN and its schedulers
- Docs: [Autoscaling clusters on Managed Service for Apache Spark](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling) · how YARN metrics drive cluster scaling on Google Cloud
:::

### MapReduce Programming Model

::: plain
Suppose you must count every word in a million books, with a hundred helpers. You give each helper a pile of books and ask them to write "word, 1" on a slip for every word they read: that is the **map** step. Then everyone sorts their slips into boxes by the first letter, and each helper takes one letter's box from everybody and adds up the counts for each word: that is the **shuffle** and the **reduce** step. Nobody needs to see all the books, and if one helper falls ill, you give their pile to someone else. MapReduce is this recipe, made automatic.
:::

**The model.** Google's 2004 MapReduce paper showed that a huge range of batch jobs fit two functions written by the user:

- `map(key, value) -> list of (k2, v2)`: called once per input record, emits intermediate pairs.
- `reduce(k2, list of v2) -> list of results`: called once per distinct intermediate key, with all values for that key.

The framework does everything else: splitting the input, running map tasks near their data, grouping all values of each key together across the cluster, re-running failed tasks, and writing the output. The user writes no networking or locking code, and the job scales by adding machines.

**The phases, step by step.**

1. **Input splits.** The `InputFormat` divides the input into splits, normally one per HDFS block, and one map task runs per split, scheduled on a node that holds the block.
2. **Map.** A `RecordReader` turns the split into records (for text, one line per record), and the user's `map` emits pairs into an in-memory buffer (`mapreduce.task.io.sort.mb`, 100 MB by default).
3. **Partition, sort, spill.** Each pair is assigned to a reducer by the **partitioner** (by default a hash of the key modulo the number of reducers). When the buffer is 80 percent full, a background thread sorts it by partition and key, runs the **combiner** if there is one, and **spills** it to a local file. At the end the map task merges its spills into one sorted file per task, divided into partitions.
4. **Shuffle.** Each reduce task fetches its partition from every map task's output over HTTP, from a shuffle service running inside each NodeManager.
5. **Merge and reduce.** The reducer merge-sorts the fetched runs (in memory and on disk), then calls `reduce` once per key with an iterator over its values, and writes results to HDFS through an `OutputCommitter`, which makes the output visible atomically only when the job succeeds.

[[fig:hadoop_mr]]

**Word count, simulated.** This standard-library program performs the same steps on two tiny splits so you can see what moves where:

```python
from collections import defaultdict
from itertools import groupby
import zlib

splits = ["the cat sat on the mat", "the dog sat on the log"]   # one input split per map task
R = 2                                                             # number of reduce tasks

def mapper(line):
    for word in line.split():
        yield word, 1

def partition(key):
    # Hadoop's HashPartitioner uses (hashCode & MAX_INT) % R; crc32 keeps this run deterministic
    return zlib.crc32(key.encode()) % R

def combine(pairs):
    # a combiner is a local reduce run on one map task's output before the shuffle
    totals = defaultdict(int)
    for k, v in pairs:
        totals[k] += v
    return list(totals.items())

map_outputs, emitted, shuffled = [], 0, 0
for line in splits:                                   # map phase
    parts = defaultdict(list)
    for k, v in mapper(line):
        parts[partition(k)].append((k, v))
        emitted += 1
    out = {p: sorted(combine(kvs)) for p, kvs in parts.items()}   # combine, then sort per partition
    shuffled += sum(len(kvs) for kvs in out.values())
    map_outputs.append(out)

for r in range(R):                                    # shuffle and reduce phase
    fetched = sorted(kv for out in map_outputs for kv in out.get(r, []))   # merge sorted runs
    for word, group in groupby(fetched, key=lambda kv: kv[0]):
        print(f"reducer {r}: {word} {sum(v for _, v in group)}")
print(f"map output records {emitted}, shuffled after combiner {shuffled}")
# reducer 0: cat 1
# reducer 0: mat 1
# reducer 0: on 2
# reducer 0: sat 2
# reducer 0: the 4
# reducer 1: dog 1
# reducer 1: log 1
# map output records 12, shuffled after combiner 10
```

Every word lands on exactly one reducer, so each reducer can produce final totals independently. The combiner merged the two "the" pairs in each map task before the shuffle. On real text, where common words repeat thousands of times per split, combiners cut shuffle volume by orders of magnitude.

**Running Python on Hadoop.** Production MapReduce is Java, but **Hadoop Streaming** runs any executable as mapper and reducer, reading lines on standard input and writing tab-separated key and value lines:

```bash
mapred streaming \
  -files mapper.py,reducer.py \
  -input /data/books -output /out/wordcount \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -numReduceTasks 8
```

**Design patterns.**

- **Combiner**: valid only when the reduce operation is associative and commutative (sum, count, max). An average needs a combiner that emits (sum, count) pairs, never partial averages.
- **Custom partitioner**: controls which reducer receives a key, for example all events of a country to one output file, or range partitioning to produce globally sorted output (as in the TeraSort benchmark).
- **Secondary sort**: to get each key's values in a chosen order (a user's events by time), put both fields in a composite key, partition and group on the natural key only, and sort on the full key. The framework's sort then does the ordering for you.
- **Reduce-side join**: both tables are mapped to (join key, tagged record) and the reducer combines records from the two sides. It works for any size but shuffles both tables.
- **Map-side join**: when one table is small, ship it to every mapper through the distributed cache and join in memory, with no shuffle at all. If both inputs are already partitioned and sorted the same way on the join key, mappers can merge them directly.

These are exactly the broadcast and shuffle joins you will meet in Spark and BigQuery under other names.

> **Pitfall:** A single reducer (`mapreduce.job.reduces` defaults to 1) turns a cluster into one machine for the final phase. Set the number of reducers to match the data, typically so each reducer handles 1 to 5 GB.

> **Pitfall:** Skew breaks MapReduce just as it breaks Spark: if one key holds a third of the records, one reducer does a third of the work while the rest sit idle. Salt hot keys or pre-aggregate with a combiner.

::: guide
For how the same map, shuffle and reduce pattern works inside Spark, read [Distributed data processing fundamentals](pyspark-streaming-kafka-topic-map.html#dist) and [Shuffle internals](pyspark-streaming-kafka-topic-map.html#shuffle) in the PySpark + Kafka guide.
:::

::: deeper
**Why MapReduce was slow, and why it was robust.** Every job reads its input from HDFS and writes its output to HDFS with full replication, and every map output is sorted and written to local disk. A pipeline of ten steps is ten jobs, each paying JVM start-up, a full materialization and a sort, even when the next step does not need sorted data. In return, any task can be re-run independently from durable inputs, which made MapReduce extremely tolerant of failure on unreliable hardware. Tez and Spark keep the fault-tolerance model (recompute a lost task from its inputs) but avoid the unnecessary writes and sorts.

**Speculative execution.** Near the end of a job, the framework launches duplicate attempts of tasks that run much slower than their peers (stragglers) and keeps whichever finishes first. It helps with a slow disk; it does nothing for skew, because the duplicate has the same amount of data.

**Shuffle tuning knobs.** `mapreduce.task.io.sort.mb` (map buffer), `mapreduce.map.sort.spill.percent` (spill threshold, 0.80), `mapreduce.task.io.sort.factor` (streams merged at once), map output compression (`mapreduce.map.output.compress` with Snappy or LZ4), and `mapreduce.job.reduce.slowstart.completedmaps` (start fetching when 5 percent of maps are done, so the shuffle overlaps the map phase).

**MapReduce today.** Few new jobs are written in MapReduce. It survives in legacy Java jobs, in tools built on it (DistCp, some Sqoop and Hive workloads), and as the mental model behind every distributed engine: BigQuery's shuffle tier, Spark's stages and Beam's `GroupByKey` are all the same partition, sort or hash, and merge pattern.

> **Interview:** "Where does sorting happen in MapReduce, and why?" Map tasks sort their output by partition and key before spilling, and reducers merge-sort the fetched runs. Sorting is how MapReduce groups all values of a key together without holding everything in memory: once sorted, equal keys are adjacent and can be streamed through `reduce`.
:::

::: check
?? Can you use the reducer of an average calculation as its combiner? Why not?
No. The average of partial averages is wrong when parts have different sizes. Have the combiner emit (sum, count) and divide only in the reducer.

?? How does the default partitioner decide which reducer gets a key?
It hashes the key and takes the result modulo the number of reducers, so every occurrence of a key goes to the same reducer.

?? Why is a map-side join faster than a reduce-side join, and when can you use it?
It needs no shuffle: each mapper holds the small table in memory and joins locally. It works when one side fits in memory on every task (or when both sides are identically partitioned and sorted on the join key).
:::

::: resources
- Paper: [MapReduce: Simplified Data Processing on Large Clusters (OSDI 2004)](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/) · the original paper by Dean and Ghemawat, still the clearest explanation
- Docs: [MapReduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html) · the Java API, word count and job configuration
- Docs: [Hadoop Streaming](https://hadoop.apache.org/docs/stable/hadoop-streaming/HadoopStreaming.html) · writing mappers and reducers in Python or any language
- Book: [Data-Intensive Text Processing with MapReduce](https://lintool.github.io/MapReduceAlgorithms/) by Jimmy Lin and Chris Dyer · free book on combiners, secondary sort and join patterns
- Book: [Designing Data-Intensive Applications](https://dataintensive.net/) by Martin Kleppmann · the batch processing chapter compares MapReduce with dataflow engines
- Book: [Hadoop: The Definitive Guide, 4th edition](https://www.oreilly.com/library/view/hadoop-the-definitive/9781491901687/) by Tom White · chapters 7 to 9 on how jobs run, shuffle and sort, and MapReduce features
:::

### Hadoop Security — Kerberos, Ranger, Knox

::: plain
Out of the box, a Hadoop cluster believes whatever you tell it: if your laptop says "I am the user hdfs", the cluster lets you delete anything. Securing it takes three layers, like a building. **Kerberos** is the ID card check: every person and every service must prove who they are with a ticket from a trusted office. **Ranger** is the list on each door saying who may enter which room and do what. **Knox** is the single reception desk at the front entrance, so visitors from outside never wander the corridors directly.
:::

**Why "simple" authentication is no authentication.** Hadoop's default, `hadoop.security.authentication=simple`, takes the user name from the client operating system (or the `HADOOP_USER_NAME` environment variable). Anyone who can reach the NameNode port can claim to be anyone. HDFS permissions and ACLs still exist, but they only stop honest mistakes. Every production cluster that holds real data turns on **secure mode**: Kerberos for authentication, plus authorization, encryption and auditing.

**Kerberos in one page.** Kerberos is a network authentication protocol built around a trusted **Key Distribution Center** (KDC), often MIT Kerberos or Active Directory. Every user and every service has a **principal**, a name in a **realm**:

- users: `ana@EXAMPLE.COM`
- services, one per host: `nn/nn1.example.com@EXAMPLE.COM`, `dn/worker17.example.com@EXAMPLE.COM`, `HTTP/nn1.example.com@EXAMPLE.COM`

The flow:

1. The user runs `kinit`, proves their password (or a key in a **keytab** file) to the KDC's Authentication Service, and receives a **ticket-granting ticket** (TGT), valid for hours.
2. To call the NameNode, the client presents the TGT to the KDC's Ticket Granting Service and receives a **service ticket** for `nn/nn1.example.com`.
3. The client sends the service ticket to the NameNode, which decrypts it with its own key from its keytab. No password ever crosses the network, and the NameNode never contacts the KDC for this check.

```bash
kinit ana@EXAMPLE.COM                                    # interactive user: asks for the password
klist                                                    # shows the TGT and its expiry
hdfs dfs -ls /data                                       # works while the ticket is valid
kdestroy && hdfs dfs -ls /data                           # fails: GSS initiate failed, no valid credentials
kinit -kt /etc/security/keytabs/etl.keytab etl@EXAMPLE.COM   # service account from a keytab, no prompt
```

Hadoop maps principals to local short names with `hadoop.security.auth_to_local` rules, so `nn/nn1.example.com@EXAMPLE.COM` becomes the user `hdfs`, and `ana@EXAMPLE.COM` becomes `ana`. Groups then come from LDAP or the operating system.

**Delegation tokens.** A job with 10,000 tasks cannot have every task ask the KDC for tickets; that would overload it, and the user's TGT is not on the worker machines anyway. Instead, at submission time the client uses its Kerberos identity to obtain **delegation tokens** from the NameNode (and from Hive, HBase or Kafka as needed). The tokens travel with the job, tasks use them to authenticate, and the ResourceManager renews them while the job runs. They expire at a maximum lifetime (seven days by default for HDFS), so long-running streaming jobs must be given a keytab (`spark-submit --principal etl@EXAMPLE.COM --keytab etl.keytab`) to obtain fresh tokens.

**SPNEGO for web UIs and REST.** The NameNode, ResourceManager and history server UIs, and REST APIs such as WebHDFS, speak HTTP. **SPNEGO** carries Kerberos over HTTP with the `Negotiate` header. Enable it with `hadoop.http.authentication.type=kerberos` and an `HTTP/host` principal; clients then need a ticket and a SPNEGO-aware tool:

```bash
curl --negotiate -u : "https://nn1.example.com:9871/webhdfs/v1/data?op=LISTSTATUS"
```

**Encryption.** Kerberos authenticates, but data still crosses the network in the clear unless you also set `hadoop.rpc.protection=privacy` (encrypted RPC), `dfs.encrypt.data.transfer=true` (encrypted block transfer) and HTTPS for web endpoints. For data at rest, HDFS **transparent encryption** uses **encryption zones**: every file in a zone is encrypted with its own data key, which is itself encrypted by a zone key held in the **Hadoop KMS**, so DataNode disks only ever hold ciphertext. This is the envelope encryption pattern from Data Encryption Internals in [Storage internals and hardware](#hardware).

```bash
hadoop key create finance-key
hdfs crypto -createZone -keyName finance-key -path /data/finance
```

**Apache Ranger: central authorization.** HDFS permissions are per path, Hive's built-in authorization is per table, and HBase has its own ACLs; managing all three by hand does not scale. **Ranger** centralises authorization:

- **Ranger Admin** stores policies in a database and provides a web UI and REST API.
- **Plugins** run inside each service (the NameNode, HiveServer2, HBase RegionServers, Kafka brokers, the YARN ResourceManager, Trino and others). Each plugin downloads policies periodically, caches them, and evaluates every access request locally, so Ranger Admin is not in the request path.
- **Audit** records every allowed and denied access to Solr (for searching) and to long-term storage.
- **Usersync** imports users and groups from LDAP or Active Directory.

A policy says who may do what on which resource. Through the REST API, a Hive policy granting the `analysts` group read access to one table looks like this:

```json
{
  "service": "prod_hive",
  "name": "analysts-read-orders",
  "resources": {
    "database": {"values": ["sales"]},
    "table": {"values": ["orders"]},
    "column": {"values": ["*"]}
  },
  "policyItems": [
    {"groups": ["analysts"], "accesses": [{"type": "select", "isAllowed": true}]}
  ]
}
```

Beyond allow and deny, Ranger supports **row-level filters** (analysts in Germany see only `country = 'DE'`), **column masking** (show only the last four digits of a card number), and **tag-based policies**, where a classification such as `PII` set in Apache Atlas applies a policy to every table and column carrying that tag.

**Apache Knox: the perimeter gateway.** A secured cluster has dozens of hosts and ports. **Knox** is a reverse proxy that exposes the REST APIs and UIs (WebHDFS, HiveServer2 over HTTP, the YARN and Spark UIs, Livy) through one HTTPS endpoint. A **topology** file per cluster lists the authentication provider (LDAP, Active Directory, or single sign-on with KnoxSSO, SAML or OIDC) and the services to proxy. Outside users authenticate to Knox with familiar credentials; Knox talks to the cluster with Kerberos on their behalf. Only the gateway is reachable from outside the network.

```bash
curl -u ana "https://knox.example.com:8443/gateway/prod/webhdfs/v1/data?op=LISTSTATUS"   # prompts for the password
```

| Layer | Question it answers | Tool |
|---|---|---|
| Authentication | Who are you? | Kerberos (inside), LDAP or SSO at the Knox gateway (outside) |
| Authorization | What may you do? | Ranger policies, HDFS permissions and ACLs as fallback |
| Perimeter | How do you get in? | Knox gateway, firewalls |
| Encryption | Can anyone read it on the way or on disk? | RPC privacy, TLS, encryption zones with the Hadoop KMS |
| Audit | Who did what, when? | Ranger audit logs |

> **Pitfall:** Kerberos is unforgiving about names and clocks. Hostnames must resolve forwards and backwards to the same fully qualified name used in the principal, and clocks must be within a few minutes of the KDC (keep NTP running), or authentication fails with cryptic errors.

> **Tip:** On Google Cloud, Managed Service for Apache Spark can create Kerberized clusters (the cluster security configuration sets up a KDC, or trusts your Active Directory), and Ranger is an optional component (`--optional-components=SOLR,RANGER`, with the admin password stored encrypted with Cloud KMS). Component Gateway gives authenticated access to cluster web UIs without opening ports. Many teams avoid much of this by using job-scoped clusters, service accounts and IAM on Cloud Storage, with table-level policies in BigQuery or Knowledge Catalog (formerly Dataplex Universal Catalog).

::: deeper
**Impersonation and proxy users.** Services such as HiveServer2, Oozie, Livy and Kyuubi authenticate as themselves but act for end users. `hadoop.proxyuser.hive.hosts` and `hadoop.proxyuser.hive.groups` say which service principal may impersonate which users from which hosts. Overly broad settings (`*` for both) let a compromised service act as anyone.

**Ranger plugin behaviour.** Because plugins cache policies, a policy change reaches a service after the next poll (tens of seconds by default), and services keep enforcing the last known policies if Ranger Admin is down. For HDFS, if no Ranger policy matches, the plugin can fall back to native HDFS permissions; turning that fallback off makes Ranger the only source of truth, which is cleaner but means you must model every path.

**Ranger versus Sentry.** Apache Sentry was an alternative authorization project used by older Cloudera distributions; it has been retired to the Apache Attic, and Ranger is the standard. Ranger also secures engines outside classic Hadoop, such as Trino, Kafka and (through Kyuubi's authorization plugin) Spark SQL.

**Cross-realm trust.** Large companies usually keep users in Active Directory and run a separate MIT KDC for cluster service principals, connected by a one-way cross-realm trust. Users keep one password, and creating thousands of service principals does not touch the corporate directory.

**Token theft and lifetimes.** A delegation token is a bearer credential: whoever has it can use it until it expires. Keep job staging directories private, set sensible token lifetimes, and never log token contents.

> **Interview:** "Why does Hadoop need delegation tokens if it already has Kerberos?" Tasks run on many machines that do not hold the user's Kerberos credentials, and thousands of tasks asking the KDC at once would overload it. Tokens are obtained once with Kerberos at submission, distributed with the job, renewed by YARN, and checked by the services themselves without the KDC.
:::

::: check
?? Why is Hadoop's default "simple" authentication unsafe?
It trusts the user name the client reports, so any client can claim to be any user, including the HDFS superuser.

?? Where does a Ranger plugin evaluate policies, and what happens if Ranger Admin is down?
Inside the service (NameNode, HiveServer2 and so on), against a locally cached copy. If Ranger Admin is down, services keep enforcing the last policies they downloaded.

?? What problem does Knox solve that Kerberos does not?
Perimeter access: it gives outside users one HTTPS entry point with familiar authentication (LDAP, SSO), hides internal hosts and ports, and talks Kerberos to the cluster on their behalf.
:::

::: resources
- Docs: [Hadoop in Secure Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html) · principals, keytabs, auth_to_local, wire encryption and proxy users
- Docs: [Apache Ranger](https://ranger.apache.org/) · architecture, supported plugins and the policy model
- Docs: [Apache Knox](https://knox.apache.org/) · gateway topologies, providers and the user guide
- Docs: [Transparent Encryption in HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/TransparentEncryption.html) · encryption zones and the KMS design
- Book: [Hadoop Security](https://www.oreilly.com/library/view/hadoop-security/9781491900970/) by Ben Spivey and Joey Echeverria · the full picture of authentication, authorization and auditing on Hadoop
- Docs: [Ranger component on Managed Service for Apache Spark](https://docs.cloud.google.com/dataproc/docs/concepts/components/ranger) · installing and configuring Ranger on Google Cloud clusters
:::

## SQL and storage on Hadoop

Writing Java MapReduce jobs for every question did not scale to the analysts who actually had the questions. This group covers what grew on top of core Hadoop to fix that: SQL engines (Hive, compiled to MapReduce and later to Tez DAGs, and Impala, a massively parallel engine with its own daemons), a database for fast random reads and writes (HBase), and a pre-aggregation layer for sub-second BI (Kylin).

Two ideas from this group outlived most of its engines. The **Hive Metastore**, a shared catalog of tables, columns, partitions and file locations, is still how Spark, Trino, Flink and many lakehouse tools find tables. And the storage tricks Hive popularised (columnar ORC and Parquet files, directory partitioning, bucketing, statistics for pruning) are the ones you use today in Data lakes and lakehouses and in BigQuery.

### Apache Hive & HiveQL Internals

::: plain
Hive is a translator. Analysts write SQL, and Hive turns each query into a batch of distributed work on the cluster, so nobody has to write MapReduce by hand. To translate, it needs a dictionary that says "the table `orders` is these files, in this folder, in this format, with these columns": that dictionary is the **metastore**. The files themselves stay ordinary files on HDFS or Cloud Storage, which is why many other tools can read the same tables.
:::

**Schema on read.** A Hive table is metadata pointing at a directory. Hive does not own the files the way a database owns its pages: when you query, it reads whatever files are in the directory and applies the table's schema and **SerDe** (serializer and deserializer, the code that parses each file format) at read time. That made it trivial to put SQL on top of data already sitting in HDFS.

```sql
-- HiveQL: an external table over existing files, partitioned by day
CREATE EXTERNAL TABLE sales.orders (
  order_id   BIGINT,
  user_id    BIGINT,
  country    STRING,
  amount     DECIMAL(12,2)
)
PARTITIONED BY (order_date STRING)
STORED AS ORC
LOCATION 'gs://acme-lake/sales/orders/'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

MSCK REPAIR TABLE sales.orders;   -- register partition folders already present in storage

SELECT country, SUM(amount) AS revenue
FROM sales.orders
WHERE order_date BETWEEN '2026-11-01' AND '2026-11-07'
GROUP BY country;
```

**Managed versus external.** Dropping an **external** table deletes only its metadata; dropping a **managed** table deletes the data too. Use external tables for data that other tools write or read.

**The Hive Metastore (HMS).** The metastore is a Thrift service (port 9083 by convention) backed by a relational database such as MySQL or PostgreSQL. It stores databases, tables, columns and types, partitions with their storage locations, SerDe and format details, and statistics. Its API became the de facto catalog standard: Spark (`enableHiveSupport()`), Trino and Presto, Impala, Flink and many ingestion tools all read and write it. On Google Cloud the managed version is **Dataproc Metastore**, a fully managed HMS that clusters, serverless Spark batches (Managed Service for Apache Spark's serverless mode, formerly Dataproc Serverless) and other engines can share.

**How a query is compiled.** HiveServer2 accepts the query over JDBC or ODBC (clients such as `beeline` connect to it) and hands it to the **driver**:

1. **Parse** the HiveQL into an abstract syntax tree.
2. **Semantic analysis**: resolve tables and columns against the metastore, check types, expand `*`.
3. **Logical plan and optimisation**: Hive uses **Apache Calcite** for cost-based optimisation (join order, predicate pushdown, partition pruning), driven by table and column statistics collected with `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS`.
4. **Physical plan**: operators are packed into a DAG of tasks for the execution engine.
5. **Execute** on YARN and stream results back.

`EXPLAIN` (and `EXPLAIN CBO`, `EXPLAIN VECTORIZATION`) shows each stage of that pipeline.

**Execution engines.** Hive first compiled to chains of MapReduce jobs, which meant minutes of latency even for small queries. **Tez** (next topic) replaced it as the recommended engine, and Hive on MapReduce is deprecated. Hive on Spark existed for some years and was removed in Hive 4. **LLAP** ("live long and process") adds long-running daemons on each node that keep a columnar **cache** of hot data and execute query fragments in already-warm JVMs, giving interactive, sub-second to few-second latency for BI tools.

**Vectorization.** With `hive.vectorized.execution.enabled=true` operators process a batch of rows at a time (1,024 by default), column by column, instead of one row object at a time. Tight loops over primitive arrays use the CPU caches well, the effect described in Memory Hierarchy & Cache Effects in [Storage internals and hardware](#hardware).

**ORC and predicate pushdown.** ORC, Hive's native columnar format, splits a file into **stripes** (64 MB by default) and keeps min, max and count statistics per column for each stripe and for each **row group** of 10,000 rows, plus optional Bloom filters (`orc.bloom.filter.columns`). Hive pushes `WHERE` predicates into the reader as a "search argument", and the reader skips stripes and row groups whose statistics cannot match:

```python
# ORC keeps min/max statistics per stripe; a filter skips stripes whose range cannot match
stripes = [("2026-11-01", "2026-11-07"), ("2026-11-08", "2026-11-14"),
           ("2026-11-15", "2026-11-21"), ("2026-11-22", "2026-11-28")]   # (min, max) of order_date
wanted = "2026-11-17"                                                     # WHERE order_date = '2026-11-17'
read = [i for i, (lo, hi) in enumerate(stripes) if lo <= wanted <= hi]
print(f"read stripes {read}, skipped {len(stripes) - len(read)} of {len(stripes)}")
# read stripes [2], skipped 3 of 4
```

Skipping works only when the data is clustered on the filtered column; with dates scattered randomly, every stripe's range covers everything and nothing is skipped. Sort or cluster data on the columns people filter by.

**Partitioning and bucketing.** A **partition** is a subdirectory per value (`order_date=2026-11-17/`), and a filter on the partition column prunes whole directories before any file is opened. **Bucketing** hashes rows on a column into a fixed number of files per partition:

```sql
CREATE TABLE sales.orders_bucketed (order_id BIGINT, user_id BIGINT, amount DECIMAL(12,2))
PARTITIONED BY (order_date STRING)
CLUSTERED BY (user_id) SORTED BY (user_id) INTO 64 BUCKETS
STORED AS ORC;
```

Two tables bucketed and sorted the same way on `user_id` can be joined bucket by bucket (a **sort-merge-bucket join**) with no shuffle.

> **Pitfall:** Over-partitioning is the most common Hive mistake. Partitioning by `user_id`, or by hour and country and device together, creates millions of tiny directories and files, overloads the NameNode and the metastore, and makes queries slower. Partition on low-cardinality columns that almost every query filters on (usually a date), and aim for partitions of at least hundreds of megabytes.

**Hive ACID.** Plain Hive tables can only be appended to or overwritten by partition. **Transactional** (ACID) tables, stored as ORC with `TBLPROPERTIES ('transactional'='true')`, support `INSERT`, `UPDATE`, `DELETE` and `MERGE`. Each transaction writes a new **delta** directory, readers merge the base files with the deltas they are allowed to see (snapshot isolation using transaction IDs from the metastore), and background **compaction** merges deltas: **minor** compaction combines deltas, **major** compaction rewrites base plus deltas into a new base.

```sql
MERGE INTO sales.customers t
USING staging.customers s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET email = s.email
WHEN NOT MATCHED THEN INSERT VALUES (s.customer_id, s.email);

SHOW COMPACTIONS;
ALTER TABLE sales.customers COMPACT 'major';
```

::: deeper
**Hive 4 and Iceberg.** Hive 4 (first released in 2024) can create and query **Apache Iceberg** tables (`CREATE TABLE ... STORED BY ICEBERG`), with snapshots, time travel and schema evolution handled by Iceberg's own metadata files rather than by the metastore's partition list. This is the direction most Hive users are moving: the table format, not the metastore, tracks files, and the metastore just points at the current metadata file. Data lakes and lakehouses covers Iceberg in depth.

**Why partition listings hurt.** A classic Hive table with 100,000 partitions means the planner fetches 100,000 partition records from the metastore database and, on object storage, may list as many prefixes. Partition filters on non-partition-key expressions (`WHERE substr(order_date, 1, 7) = '2026-11'`) can defeat pruning and fetch them all. Table formats with manifest files and column statistics avoid both costs.

**Statistics drive everything.** Without statistics, Calcite cannot choose a good join order or decide that a table is small enough to broadcast (Hive calls this a map join, controlled by `hive.auto.convert.join` and a size threshold). `hive.stats.autogather` collects basic statistics on insert; column statistics usually need an explicit `ANALYZE`.

**On Google Cloud.** Hive runs on Managed Service for Apache Spark clusters with Tez as its engine and Cloud Storage as the warehouse location. Many migrations go further: BigQuery can query Hive-partitioned Parquet or ORC in Cloud Storage as external tables or as tables in Google Cloud Lakehouse (formerly BigLake), and the BigQuery Migration Service translates HiveQL to GoogleSQL in batch.

> **Interview:** "Why is the Hive Metastore still everywhere if few people run Hive queries?" Because its Thrift API is the common catalog for Spark, Trino, Impala, Flink and ingestion tools. It maps table names to schemas, formats and storage locations, and replacing it means changing every engine's configuration. Iceberg REST catalogs are the modern successor, and many of them can still sit beside or behind an HMS.
:::

::: check
?? What happens to the data when you drop an external Hive table, and when you drop a managed one?
External: only the metadata is removed and the files stay. Managed: Hive deletes the files as well.

?? Why might a filter on an ORC column skip nothing, even with predicate pushdown on?
Skipping relies on per-stripe and per-row-group min and max values. If the data is not sorted or clustered on that column, every stripe's range covers the filter value, so every stripe must be read.

?? What does compaction do for a Hive ACID table?
It merges the delta directories written by each transaction: minor compaction combines deltas, major compaction rewrites base plus deltas into a new base, so reads stop having to merge many small files.
:::

::: resources
- Docs: [Apache Hive](https://hive.apache.org/) · releases, the language manual and Hive 4 documentation
- Docs: [Hive Design](https://cwiki.apache.org/confluence/display/Hive/Design) · the architecture of the driver, compiler, metastore and execution
- Docs: [Dataproc Metastore overview](https://docs.cloud.google.com/dataproc-metastore/docs/overview) · the managed Hive Metastore on Google Cloud
- Paper: [Major Technical Advancements in Apache Hive (SIGMOD 2014)](https://dl.acm.org/doi/10.1145/2588555.2595630) · ORC, vectorization and the move beyond MapReduce, by the Hive developers
- Book: [Programming Hive](https://www.oreilly.com/library/view/programming-hive/9781449326944/) by Edward Capriolo, Dean Wampler and Jason Rutherglen · HiveQL, partitioning and tuning; old versions, still-valid ideas
- Docs: [Apache ORC specification](https://orc.apache.org/specification/) · stripes, indexes, encodings and Bloom filters in detail
:::

### Apache Tez — DAG Execution Engine

::: plain
MapReduce is a production line where every station must put its finished parts back in the central warehouse, and the next station must fetch them again, even if it is standing right next door. Tez lets you design the whole factory at once: a map of stations (vertices) connected by conveyor belts (edges), where parts flow directly from one station to the next. The parts only go back to the warehouse at the very end. For a query with five steps, that removes four round trips to the warehouse.
:::

**The problem Tez solves.** A Hive query with a join, an aggregation and an `ORDER BY` was three MapReduce jobs. Each one started new JVMs, read its input from HDFS, sorted its map output whether it needed sorting or not, and wrote its full result back to HDFS with three replicas, only for the next job to read it again.

```python
# GB written to replicated HDFS by a three-step query: join, then aggregate, then sort
step_output_gb = {"join": 400, "aggregate": 20, "sort": 20}
replication = 3
mr = sum(step_output_gb.values()) * replication    # each MapReduce job stores its output in HDFS
tez = step_output_gb["sort"] * replication         # a Tez DAG stores only the final result
print(f"MapReduce chain writes {mr} GB to HDFS, one Tez DAG writes {tez} GB")
# MapReduce chain writes 1320 GB to HDFS, one Tez DAG writes 60 GB
```

Apache Tez (Hindi for "speed") is a framework for running a whole job as one **directed acyclic graph** (DAG) on YARN. Intermediate data moves between steps through local shuffle files or memory, never through HDFS.

**Vertices, tasks and edges.**

- A **vertex** is one step of processing logic, such as "scan and filter orders" or "aggregate by country". It runs as many parallel **tasks**, one per partition of its input.
- Each task is built from three plug-in parts, the **IPO model**: **inputs** (read from HDFS or from upstream vertices), a **processor** (the logic, for Hive a pipeline of operators) and **outputs** (to downstream vertices or to the final sink).
- An **edge** connects two vertices and has properties. Its **data movement** type says how task outputs reach downstream tasks: **one-to-one** (task i feeds task i), **broadcast** (every upstream output goes to every downstream task, used for small join sides) or **scatter-gather** (each upstream task partitions its output and every downstream task gathers its partition, which is the classic shuffle). Further properties say whether data is persisted and when the consumer may start.

A three-way Hive join and aggregation that needed four MapReduce jobs becomes one DAG: scan vertices for each table, a broadcast edge for the small dimension table, a scatter-gather edge to the join vertex, another to the aggregation vertex, and a final vertex that writes the result. Sorting happens only on edges that need it.

**The runtime.** Each DAG has a **Tez ApplicationMaster** that schedules its vertices' tasks onto YARN containers. Three runtime features matter most in practice:

- **Container reuse**: a finished task's JVM (with its loaded classes and JIT-compiled code) is handed the next task instead of being thrown away, avoiding start-up cost on every task.
- **Session mode**: the AM itself stays alive between DAGs, so a sequence of queries from the same user or HiveServer2 session skips YARN application start-up. Hive can also **pre-warm** containers when a session opens.
- **Dynamic graph reconfiguration**: **vertex managers** can change the plan while it runs. The most important one, the shuffle vertex manager, watches the actual size of upstream output and reduces the number of downstream tasks when the data is smaller than planned (auto-reducer parallelism), and it starts downstream tasks only when enough upstream output is ready (slow start).

```sql
-- Hive on Tez settings you will meet (set per session or in hive-site.xml)
SET hive.execution.engine=tez;
SET hive.tez.container.size=4096;              -- MB per Tez task container
SET hive.tez.auto.reducer.parallelism=true;    -- let Tez shrink reducer counts at run time
SET hive.prewarm.enabled=true;                 -- start containers when the session opens
EXPLAIN SELECT country, COUNT(*) FROM sales.orders GROUP BY country;   -- shows the vertices and edges
```

**The Tez UI.** Tez publishes DAG, vertex and task events to the YARN Timeline Server, and the Tez UI (a web application) shows each DAG as a graph with per-vertex counters, task timings and failed attempts. It is to Hive on Tez what the Spark UI is to Spark: the place to find the slow vertex or the skewed task.

> **Pitfall:** `hive.tez.container.size` too small causes "container killed for exceeding memory" or heavy spilling in joins; too large wastes cluster capacity because YARN reserves the full size. Size it from the YARN minimum allocation upward (a multiple of it) and keep the Java heap (`hive.tez.java.opts`) around 80 percent of the container.

> **Tip:** When a Hive on Tez query is slow, look first for a vertex with one or a few long-running tasks: that is skew (one join key or group key with far more rows than the rest), not a lack of resources.

::: deeper
**Tez and Spark.** Tez and Spark were both answers to MapReduce's rigid two-stage model, and both run DAGs of tasks with shuffles between stages. The difference is focus. Tez is a low-level library for engine builders (Hive and Pig were its main users) and keeps MapReduce's process model: short tasks in YARN containers, data through local files. Spark is a full engine with its own APIs and an in-memory caching layer, long-lived executors and its own optimizer. Outside Hive, most new workloads chose Spark.

**Edge properties in full.** Besides the data movement type, each edge has a **data source type** (persisted, persisted-reliable or ephemeral, saying whether output survives the producing task) and a **scheduling type** (sequential, where consumers wait for producers, or concurrent, where both run at once for pipelined streaming). Custom edges let an engine define its own routing; Hive uses one for bucketed joins.

**Failure handling.** If a task fails, Tez re-runs it. If a downstream task cannot fetch an upstream output (the node died and took its local shuffle files with it), Tez re-runs the upstream task that produced it, the same lineage-based recovery that Spark uses.

**Status.** Tez remains the execution engine of Hive 4 and of Hive in Cloudera's platform, and is available on Managed Service for Apache Spark clusters for Hive. It has little use outside Hive today.

> **Interview:** "Why is Hive on Tez faster than Hive on MapReduce for the same plan?" One DAG instead of a chain of jobs: no intermediate results written to replicated HDFS, no unnecessary sorts between steps, reused containers and sessions instead of new JVMs per task, broadcast edges for small tables, and run-time adjustment of parallelism.
:::

::: check
?? What are the three data movement types of a Tez edge, and which one is a shuffle?
One-to-one, broadcast and scatter-gather. Scatter-gather is the shuffle: each upstream task partitions its output and each downstream task gathers one partition from all of them.

?? What does auto-reducer parallelism do?
The shuffle vertex manager measures the actual size of upstream output at run time and lowers the number of downstream tasks when the data is smaller than estimated, avoiding many tiny tasks.

?? What do container reuse and session mode save?
Container reuse avoids starting a new JVM for every task. Session mode keeps the Tez ApplicationMaster alive across queries, so each new query skips YARN application start-up.
:::

::: resources
- Docs: [Apache Tez](https://tez.apache.org/) · project overview, releases and links to the user guides
- Docs: [Tez user guides](https://tez.apache.org/user_guides.html) · the Tez UI, local mode and configuration guides
- Paper: [Apache Tez: A Unifying Framework for Modeling and Building Data Processing Applications (SIGMOD 2015)](https://dl.acm.org/doi/10.1145/2723372.2742790) · the design paper: DAG API, IPO model and runtime reconfiguration
- Repo: [apache/tez on GitHub](https://github.com/apache/tez) · source and examples of the DAG API
- Docs: [Hive on Tez configuration](https://cwiki.apache.org/confluence/display/Hive/Hive+on+Tez) · how Hive maps plans to Tez and the settings involved
:::

### Apache Impala — MPP SQL on Hadoop

::: plain
Hive is like ordering a custom-built piece of furniture: the workshop plans the job, books workers and builds it, so even a small order takes a while. Impala is a shop with staff already behind every counter. Its workers run all the time on every machine, already know where the data is, and start answering as soon as the question arrives. You give up some of the workshop's resilience (if a worker collapses mid-order, the order starts again), but simple questions come back in seconds instead of minutes.
:::

**What Impala is.** Apache Impala is a **massively parallel processing** (MPP) SQL engine for data in HDFS, object storage, Kudu and HBase, started at Cloudera in 2012. It reads the same tables as Hive (it uses the Hive Metastore), but executes queries with its own long-running C++ daemons instead of launching jobs on YARN. The goal was interactive analytics, BI dashboards and ad hoc SQL, on the data already in the lake.

**The daemons.**

- **impalad** runs on every worker node. Any impalad can act as the **coordinator** for a query (it parses, plans, distributes the work and returns results) and every impalad is an **executor** that scans and processes its share of the data. Large deployments dedicate a few nodes to coordinating.
- **statestored** is a publish and subscribe service. It tracks which impalads are alive and broadcasts **topics**: cluster membership, catalog updates and admission-control state. If it is down, queries still run, but membership and metadata changes stop spreading.
- **catalogd** loads table metadata from the Hive Metastore and file and block information from storage, and distributes it to coordinators. Statements run in Impala (`CREATE TABLE`, `INSERT`) update the catalog directly.

**How a query runs.** The coordinator's planner builds a single-node plan, then splits it into **plan fragments** separated by **exchanges** (data redistribution points). Fragments run in parallel on executors, and rows stream between them through the network as they are produced, with no intermediate materialization to disk unless an operator spills. Scans read local HDFS blocks where possible, and Impala uses HDFS short-circuit reads and its own I/O manager to keep all disks busy.

**LLVM code generation.** For each query, Impala compiles hot paths (expression evaluation, hashing, row materialization for a specific schema) into machine code with **LLVM** at run time. A generic interpreter must check types and branch on every value; generated code knows that column 3 is a non-null `BIGINT` and does exactly the work needed. On CPU-bound queries this gives large speed-ups; for tiny queries the compile time can outweigh the gain, and Impala skips code generation when it estimates few rows.

**Join strategy from statistics.** The planner chooses between a **broadcast** join (send the smaller side to every node) and a **partitioned** join (hash-redistribute both sides) by estimating bytes moved, which is why `COMPUTE STATS` matters so much:

```python
# Broadcast or partitioned join? Estimate the bytes each strategy sends over the network
nodes, fact_gb = 20, 2000
for dim_gb in (5, 50, 200):
    broadcast = dim_gb * nodes          # copy the whole small table to every node
    partitioned = fact_gb + dim_gb      # hash-redistribute both sides (roughly every byte moves)
    choice = "broadcast" if broadcast < partitioned else "partitioned"
    print(f"dim {dim_gb:3} GB: broadcast {broadcast:5} GB, partitioned {partitioned} GB -> {choice}")
# dim   5 GB: broadcast   100 GB, partitioned 2005 GB -> broadcast
# dim  50 GB: broadcast  1000 GB, partitioned 2050 GB -> broadcast
# dim 200 GB: broadcast  4000 GB, partitioned 2200 GB -> partitioned
```

Without statistics the planner may think a 200 GB table is tiny, broadcast it, and run nodes out of memory. **Runtime filters** add a second trick: after building the hash table for the small side of a join, Impala sends a Bloom filter (or min and max values) of its join keys to the scans of the large side, which then skip rows, and whole Parquet row groups or partitions, that cannot match.

```sql
-- Impala SQL
COMPUTE STATS sales.orders;                 -- table and column statistics for the planner
SHOW TABLE STATS sales.orders;
EXPLAIN
SELECT c.segment, SUM(o.amount)
FROM sales.orders o JOIN sales.customers c ON o.user_id = c.user_id
WHERE o.order_date = '2026-11-17'
GROUP BY c.segment;                         -- the plan shows BROADCAST or PARTITIONED and runtime filters
REFRESH sales.orders;                       -- pick up files written by Spark or Hive
```

**Kudu integration.** HDFS files are immutable, so updating a row means rewriting files. **Apache Kudu** is a columnar storage engine for tables with primary keys that accepts fast inserts, updates and deletes while still scanning like a columnar format; tablets are replicated with the Raft consensus protocol. Impala is Kudu's main SQL engine:

```sql
CREATE TABLE ops.metrics (
  host  STRING,
  ts    TIMESTAMP,
  value DOUBLE,
  PRIMARY KEY (host, ts)
)
PARTITION BY HASH (host) PARTITIONS 16
STORED AS KUDU;

UPSERT INTO ops.metrics VALUES ('web1', now(), 0.73);   -- insert or update by primary key
```

Impala plus Kudu was Cloudera's answer to "fresh, mutable data with fast analytics" before lakehouse table formats; Impala also reads and writes Apache Iceberg tables in recent releases.

**Impala versus Hive.**

| | Hive on Tez (or LLAP) | Impala |
|---|---|---|
| Process model | YARN containers per query (LLAP: long-lived daemons) | Long-lived C++ daemons on every node |
| Latency | Seconds to minutes (LLAP: seconds) | Sub-second to seconds |
| Mid-query failure | Failed tasks are retried | Query fails and is re-run (newer releases can retry it automatically) |
| Best at | Large ETL, very long queries, ACID tables | Interactive BI and ad hoc SQL |
| Memory | Spills widely | Spills, but per-query memory limits and admission control matter more |

> **Pitfall:** Impala caches metadata. Files added by Spark, Hive or a copy job are invisible until you run `REFRESH table` (new files in known partitions) or `INVALIDATE METADATA table` (new tables or major changes). Forgetting this produces "missing data" reports that are really stale metadata. `INVALIDATE METADATA` without a table name reloads everything and is expensive on large catalogs.

> **Tip:** Turn on admission control with resource pools and per-query memory limits. Without it, a few heavy queries can exhaust memory on every executor and fail everyone's queries at once.

::: deeper
**Local catalog mode.** In the classic design, catalogd broadcasts every table's full metadata through the statestore to every coordinator, which does not scale to catalogs with millions of partitions and files. **On-demand (local catalog) metadata** lets coordinators fetch only the metadata a query needs and cache it, with catalogd as the source.

**Why no mid-query fault tolerance.** Impala streams data between fragments without writing it durably, so a lost executor loses in-flight state. Recomputing just the lost part, as Spark does, needs durable shuffle outputs and lineage. Impala chose low latency instead, which suits queries that take seconds; for multi-hour ETL, Spark or Hive are safer.

**Where Impala runs today.** Impala is an active Apache project and the interactive SQL engine of Cloudera's platform, including Cloudera's data services on Google Cloud. It is not offered as a component of Managed Service for Apache Spark. On Google Cloud, the usual equivalents are BigQuery (including over Cloud Storage data through Google Cloud Lakehouse tables) and Trino, which is an optional component on Managed Service for Apache Spark clusters. Trino has a similar MPP, daemon-based design and is covered in the next sections.

> **Interview:** "How do runtime filters speed up a star-schema join?" After the dimension side is filtered and hashed, the engine sends a compact filter of its keys to the fact-table scans. The scans drop non-matching rows early, and can skip whole files, row groups or partitions, so far less data is read, decoded and sent through the join.
:::

::: check
?? What do statestored and catalogd do in Impala?
statestored tracks live impalads and broadcasts membership, metadata and admission-control topics. catalogd loads metadata from the Hive Metastore and storage and distributes it to the coordinators.

?? Spark wrote new files into a table's existing partitions, but Impala queries do not see them. What do you run?
`REFRESH table_name`, which reloads that table's file metadata. Use `INVALIDATE METADATA table_name` for new tables or larger changes.

?? Why does `COMPUTE STATS` change join performance so much?
The planner uses table sizes to choose between a broadcast and a partitioned join. Without statistics it can broadcast a large table, wasting network and memory, or shuffle a tiny one needlessly.
:::

::: resources
- Docs: [Apache Impala documentation](https://impala.apache.org/docs/build/html/index.html) · architecture, SQL reference, performance and admission control
- Paper: [Impala: A Modern, Open-Source SQL Engine for Hadoop (CIDR 2015)](https://www.cidrdb.org/cidr2015/Papers/CIDR15_Paper28.pdf) · the design paper: daemons, planner, LLVM code generation
- Docs: [Apache Kudu](https://kudu.apache.org/) · mutable columnar storage, Raft replication and Impala integration
- Repo: [apache/impala on GitHub](https://github.com/apache/impala) · the source mirror, useful for reading the planner and backend
- Docs: [Trino on Managed Service for Apache Spark](https://docs.cloud.google.com/dataproc/docs/concepts/components/trino) · the daemon-based MPP SQL engine available on Google Cloud clusters
:::

### Apache HBase — Wide-Column Store

::: plain
HDFS is great for reading whole files, but useless for "give me the profile of user 42 right now" or "update this one counter". HBase adds that on top of HDFS. Picture an enormous phone book, always kept in alphabetical order by name (the **row key**), torn into volumes so that each volume (a **region**) sits on a different desk. You can look up one name instantly or read every name between "Kim" and "Lee" in order, and new entries are jotted in a notebook first and copied into the printed volumes later.
:::

**The data model.** HBase is a sparse, distributed, sorted map, modelled on Google's Bigtable paper (2006):

`(row key, column family:qualifier, timestamp) -> value`

- **Row key**: a byte array, and the only index. Rows are stored sorted by key.
- **Column family**: declared when the table is created (keep to one to three); each family is stored in its own files and tuned separately (compression, versions, time to live).
- **Column qualifier**: any name, created on write. Rows can have millions of different qualifiers, and empty cells cost nothing.
- **Timestamp**: every cell keeps versions; reads return the newest unless asked otherwise.

There are no joins, no secondary indexes and no SQL in HBase itself; Apache Phoenix adds SQL and secondary indexes on top.

**The architecture.** A table is split into **regions**, contiguous ranges of row keys. **RegionServers** serve regions (hundreds per server) and handle all reads and writes for them. The **HMaster** handles table creation, assigns regions to RegionServers, balances them and coordinates recovery, but is not in the read or write path. **ZooKeeper** stores the master election and the location of `hbase:meta`, the system table that maps each region's key range to its RegionServer. Clients cache those locations and then talk to RegionServers directly. All files (data and logs) live on HDFS, so a RegionServer holds no unique data on local disk.

[[fig:hadoop_hbase]]

**The write path, an LSM tree.** HBase is the textbook log-structured merge tree that you met in [Databases: OLTP, NoSQL and specialised](#databases):

1. The RegionServer appends the edit to its **write-ahead log** (WAL) on HDFS, for durability.
2. It inserts the edit into the **MemStore**, a sorted in-memory buffer per region and column family.
3. When a MemStore reaches its flush size (128 MB by default, `hbase.hregion.memstore.flush.size`), it is written out as a new immutable, sorted **HFile** on HDFS.

Writes are therefore one sequential log append and one memory insert: fast and independent of table size.

**The read path.** A read must merge the MemStore and every HFile of the store, newest first. Three things keep it fast: the **block cache** (recently read HFile blocks in memory, on-heap or off-heap with BucketCache), **Bloom filters** per HFile (row-level filters are on by default) that say "this row is definitely not in this file" and let the read skip it, and the HFile **block index**, which finds the right 64 KB block inside a file with one lookup.

**Compactions.** Flushes keep producing small files, so reads would slow down without merging. **Minor compactions** merge a few smaller HFiles into one. **Major compactions** rewrite all HFiles of a store into one, and are the only point where deleted cells (marked by **tombstones**), expired versions and data past its time to live are physically removed. Major compactions read and write the whole region, so many operators turn off the time-based trigger (`hbase.hregion.majorcompaction=0`) and run them off-peak. When a region grows past a size threshold (`hbase.hregion.max.filesize`, 10 GB by default), it **splits** into two daughters, which the master may move to other servers.

```text
# HBase shell
create 'metrics', {NAME => 'd', VERSIONS => 1, BLOOMFILTER => 'ROW', COMPRESSION => 'SNAPPY'}, {SPLITS => ['1|', '2|', '3|', '4|', '5|', '6|', '7|']}
put  'metrics', '3|web1|2026-11-23T10:15:00', 'd:cpu', '0.73'
get  'metrics', '3|web1|2026-11-23T10:15:00'
scan 'metrics', {STARTROW => '3|web1|2026-11-23', STOPROW => '3|web1|2026-11-24', LIMIT => 100}
major_compact 'metrics'
```

**Row key design is everything.** Because the row key is the only index and regions are key ranges, the key decides both what you can query efficiently and how load spreads. The classic mistake is a key that increases monotonically, such as a timestamp or a sequence number: all new writes go to the last region, so one RegionServer takes the whole write load (a **hotspot**) while the others idle. **Salting** prefixes each key with a hash bucket so consecutive keys scatter over pre-split regions:

```python
from bisect import bisect_right
from collections import Counter
import zlib

def key(h, m, s):
    return f"2026-11-23T{h:02d}:{m:02d}:{s:02d}|sensor-{s % 7}"

old = sorted(key(10, m, s) for m in range(60) for s in range(60))   # yesterday's data, 3,600 rows
new = [key(11, m, s) for m in range(10) for s in range(60)]         # the next 600 writes

# Regions hold sorted key ranges. Split the existing data into 8 regions of equal size.
splits = [old[i * len(old) // 8] for i in range(1, 8)]
print("unsalted:", sorted(Counter(bisect_right(splits, k) for k in new).items()))

BUCKETS = 8
def salted(k):
    # prefix with a stable hash bucket so consecutive keys scatter over all regions
    return f"{zlib.crc32(k.encode()) % BUCKETS}|{k}"
salt_splits = [f"{b}|" for b in range(1, BUCKETS)]                  # pre-split at the bucket prefixes
print("salted:  ", sorted(Counter(bisect_right(salt_splits, salted(k)) for k in new).items()))
# unsalted: [(7, 600)]
# salted:   [(0, 70), (1, 76), (2, 74), (3, 80), (4, 76), (5, 70), (6, 80), (7, 74)]
```

The cost of salting: a scan over a time range must now run once per bucket and merge the results. The better design starts from the queries. For "latest readings per sensor", use `sensor_id | reversed timestamp` (Long.MAX_VALUE minus the timestamp, so the newest row sorts first): writes spread across sensors, and one short scan returns a sensor's latest values.

**Coprocessors.** Coprocessors run your code inside RegionServers. **Observers** are triggers (code before or after a get, put, flush or compaction), used for secondary indexes and auditing. **Endpoints** are like stored procedures: a client asks every region to compute, for example, a sum locally and returns only the partial results. They are powerful and dangerous, because a bug runs inside the server process.

> **Pitfall:** Creating a table without pre-splitting gives it one region, so all writes hit one server until the region grows to 10 GB and splits. For bulk loads and new high-write tables, pre-split on the expected key distribution (as the shell example does with salt prefixes).

> **Pitfall:** Too many column families multiply MemStores, flushes, files and compactions, because each family has its own MemStore and its own files in every region. Keep to one to three families, and group columns that are read together into one family.

::: deeper
**Recovery.** If a RegionServer dies, ZooKeeper notices its session expire, and the master reassigns its regions. Before they serve again, the dead server's WAL is split per region and replayed so no acknowledged write is lost. During that time (seconds to minutes), those regions are unavailable: HBase chooses consistency over availability for a row (each row is served by exactly one server). **Region replicas** can serve possibly stale reads from secondary copies during failover.

**Bulk loading.** For large imports, write HFiles directly with a Spark or MapReduce job sorted by region, then hand them to the RegionServers (`completebulkload`). This skips the WAL and MemStore entirely and is far faster than millions of puts.

**HBase versus Bigtable.** Google Cloud's **Bigtable** is the managed service built from the system HBase copied: the same data model, row-key design rules and LSM internals, with storage on Google's distributed file system, no RegionServers or compactions to manage, and replication across zones and regions. Bigtable offers an HBase-compatible Java client, and Google provides tooling to migrate HBase tables (schema translation, snapshot import and live replication from HBase), so it is the usual landing place when an HBase cluster moves to Google Cloud.

**When HBase still fits.** Large key-value or time-series workloads on premises next to HDFS, with heavy writes and single-row or short-range reads. For new systems in the cloud, managed wide-column stores (Bigtable) or other NoSQL databases are usually chosen instead.

> **Interview:** "Why is a timestamp a bad leading row key in HBase, and what do you do instead?" Rows are sorted and regions are key ranges, so monotonically increasing keys send every new write to the last region: one server becomes a hotspot. Lead with a well-distributed field (entity ID, a hash, or a salt prefix), and put the timestamp after it, reversed if the newest rows are read most.
:::

::: check
?? Describe the HBase write path in three steps.
Append the edit to the write-ahead log on HDFS, insert it into the in-memory MemStore, and later flush the MemStore to a new immutable HFile when it is full.

?? When are deleted cells physically removed from disk?
During a major compaction, which rewrites all HFiles of a store and drops tombstoned cells, expired versions and data past its time to live.

?? Your table's row key is `event_time|user_id` and one RegionServer is overloaded. Why, and what is a fix?
New events always have the largest keys, so they all go to the last region. Lead the key with a well-distributed value (`user_id|event_time`, a hash, or a salt bucket) and pre-split the table.
:::

::: resources
- Docs: [Apache HBase Reference Guide](https://hbase.apache.org/book.html) · the official book: architecture, schema design, compactions and operations
- Paper: [Bigtable: A Distributed Storage System for Structured Data (OSDI 2006)](https://research.google/pubs/bigtable-a-distributed-storage-system-for-structured-data/) · the design HBase reimplemented
- Book: [HBase: The Definitive Guide](https://www.oreilly.com/library/view/hbase-the-definitive/9781449399795/) by Lars George · the data model, storage internals and row key design in depth
- Docs: [Bigtable for HBase users](https://docs.cloud.google.com/bigtable/docs/hbase-differences) · what differs between HBase and Bigtable when you migrate
- Docs: [Bigtable schema design](https://docs.cloud.google.com/bigtable/docs/schema-design) · row key design rules that apply equally to HBase
:::

### Apache Kylin — OLAP on Hadoop

::: plain
If your manager asks the same kinds of questions every morning ("sales by region", "sales by region and product", "sales by month"), you could add up the receipts each time, or you could prepare all the likely totals overnight and just read them off a sheet. Kylin does the second. It pre-computes aggregates for the combinations of dimensions people query, stores them, and answers SQL by looking up the right pre-computed table. Questions over billions of rows come back in under a second, because the hard work happened before anyone asked.
:::

**The idea: a cube.** An **OLAP cube** is the set of aggregates of a fact table grouped by every combination of chosen **dimensions** (date, region, product, channel) with chosen **measures** (`SUM(amount)`, `COUNT(DISTINCT user_id)`). Each combination is a **cuboid**: `GROUP BY region` is one, `GROUP BY region, product` another, the grand total (no dimensions) a third. Apache Kylin, created at eBay and an Apache top-level project since 2015, builds these cuboids from a star or snowflake schema in Hive (or other sources) with batch jobs, stores them, and exposes ANSI SQL over JDBC, ODBC and REST. BI tools such as Tableau, Superset and Excel connect as if Kylin were a normal database.

**How it works.**

1. **Model**: you define a data model, the fact table, its dimension tables and join keys, using Kylin's web UI. This is the dimensional modelling from [Data modeling](#modeling) and [Data warehousing](#warehousing).
2. **Cube or index design**: choose dimensions and measures, plus rules that limit which cuboids to build.
3. **Build**: Kylin runs distributed jobs (originally MapReduce, later Spark) that scan the source, compute cuboids layer by layer (each cuboid from its smallest parent), and write them to storage. Incremental builds process only new date partitions, so a daily job adds one day's segment.
4. **Query**: Kylin parses incoming SQL with Apache Calcite, finds the smallest cuboid that can answer it, and reads it. A query grouped by region for November might scan a few thousand pre-aggregated rows instead of billions of fact rows.

```sql
-- An ordinary query from a BI tool; Kylin answers it from the (date, region) cuboid
SELECT d.month, s.region, SUM(f.amount) AS revenue, COUNT(DISTINCT f.user_id) AS buyers
FROM sales_fact f
JOIN dim_date d   ON f.date_key  = d.date_key
JOIN dim_store s  ON f.store_key = s.store_key
WHERE d.year = 2026
GROUP BY d.month, s.region;
```

**The cuboid explosion.** With N dimensions there are 2^N cuboids. Ten dimensions give 1,024, twenty give over a million, and building all of them is impossible. Kylin's design rules prune the combinations nobody will ask for:

- **Mandatory** dimensions appear in every query (usually date), so cuboids without them are dropped.
- **Hierarchy** dimensions such as year, quarter, month are only meaningful as prefixes: (year), (year, quarter), (year, quarter, month), not (month) alone.
- **Joint** dimensions are always queried together and count as one.
- **Aggregation groups** split dimensions into separate groups whose combinations are built independently.

```python
# How many cuboids (pre-aggregated group-by combinations) does a cube need?
dims = 10
print("all combinations:", 2 ** dims)

mandatory = 1           # date appears in every query, so every cuboid includes it
hierarchy = 3           # year > quarter > month: only (), (Y), (Y,Q), (Y,Q,M) make sense
joint = 2               # two dimensions always queried together count as one
free = dims - mandatory - hierarchy - joint
pruned = (2 ** free) * (hierarchy + 1) * 2   # free dims, hierarchy prefixes, joint pair on or off
print("with mandatory, hierarchy and joint rules:", pruned)
# all combinations: 1024
# with mandatory, hierarchy and joint rules: 128
```

**Exact distinct counts.** `COUNT(DISTINCT)` cannot be added up across cuboids (the same user appears in several regions). Kylin stores mergeable structures instead: **HyperLogLog** sketches for approximate counts and **bitmaps** (Roaring bitmaps over a global dictionary that maps each value to an integer) for exact counts. Probabilistic structures are explained in Bloom Filters & Probabilistic Structures in [Storage internals and hardware](#hardware).

**Storage over the versions.**

| Version | Build engine | Cube storage | Notes |
|---|---|---|---|
| Kylin 1 to 3 | MapReduce, later Spark | HBase (each cuboid row keyed by dimension values) | Needed a full Hadoop stack including HBase |
| Kylin 4 | Spark | Parquet files on HDFS or object storage | Dropped HBase, simpler to run in the cloud |
| Kylin 5 | Spark | Parquet, organised as aggregate indexes and table indexes | Index optimization from query history; internal tables using Apache Gluten as a native engine |

Kylin 5 replaced the rigid cube with **indexes**: an aggregate index is a cuboid, a table index is a pre-sorted copy of detail rows for queries that need raw records. Kylin can recommend indexes from the queries it has actually seen.

> **Pitfall:** Pre-computation trades query speed for build cost and staleness. High-cardinality dimensions (user ID, order ID) make cuboids nearly as large as the fact table, and a cube with too many dimensions can take longer to build than the window before the next build. Keep detailed, high-cardinality questions on a normal engine.

> **Tip:** Design from the dashboard backwards. List the queries the BI tool actually sends (Kylin can record them), and build only the dimensions and indexes those need.

::: deeper
**Kylin among the alternatives.** Kylin's niche, sub-second SQL on huge, slowly changing star schemas, is now served by several kinds of systems. Real-time OLAP engines such as Apache Druid, Apache Pinot, ClickHouse, StarRocks and Apache Doris ingest data continuously and use rollups, sorted columnar storage and materialized views (covered in [Real-time OLAP engines](#rtolap)). Cloud warehouses pre-aggregate too: BigQuery offers materialized views that it maintains incrementally and uses automatically to rewrite matching queries, plus BI Engine, an in-memory acceleration layer for dashboards. For most new projects on Google Cloud, BigQuery with partitioning, clustering and materialized views makes a separate cube unnecessary.

**Where Kylin still wins.** Very large, stable models where the same aggregate shapes are asked millions of times, strict query-latency targets, and exact distinct counts over long periods. Its query cost is almost independent of fact-table size, because queries touch only small pre-built cuboids.

**Build mechanics.** The "by layer" algorithm computes the N-dimension base cuboid first, then each (N-1)-dimension cuboid from a parent, and so on. The newer in-memory algorithm builds all cuboids of a data block in one pass. Each build produces a **segment** (usually per date range), segments are merged periodically, and a query may combine several segments.

> **Interview:** "What is the trade-off of a pre-aggregated cube versus an MPP engine?" The cube gives predictable sub-second latency and cheap queries, but costs build time and storage, is only as fresh as the last build, and answers only the dimension combinations you designed for. An MPP engine answers any query on fresh data, with cost and latency that grow with the data scanned.
:::

::: check
?? A cube has 12 dimensions with no pruning rules. How many cuboids is that, and why is it a problem?
2^12 = 4,096 cuboids. Each must be computed and stored on every build, so build time and storage explode; mandatory, hierarchy and joint rules and aggregation groups prune the combinations nobody queries.

?? Why can't Kylin store distinct counts as plain numbers per cuboid?
Distinct counts do not add up: the same user can appear in several cells. Kylin stores mergeable HyperLogLog sketches or bitmaps, which can be combined across cells and segments.

?? What changed in Kylin's storage in version 4?
It replaced HBase with Parquet files on HDFS or object storage, with Spark for building and querying, removing the dependency on HBase.
:::

::: resources
- Docs: [Apache Kylin documentation](https://kylin.apache.org/docs/overview) · Kylin 5 concepts: models, indexes, building and querying
- Docs: [Apache Kylin](https://kylin.apache.org/) · project overview, releases and links to older versions' documentation
- Repo: [apache/kylin on GitHub](https://github.com/apache/kylin) · source code and issue tracker
- Docs: [Introduction to materialized views in BigQuery](https://docs.cloud.google.com/bigquery/docs/materialized-views-intro) · the warehouse-native way to pre-aggregate on Google Cloud
- Book: *The Data Warehouse Toolkit* by Ralph Kimball and Margy Ross · the dimensional models that cubes are built on
:::

## Beyond classic Hadoop

The last group looks at what happened after the classic stack. Storage and compute separated: data moved to object stores, clusters became short-lived, and Kubernetes started to replace YARN. Three Apache projects show the responses from inside the Hadoop world: **Alluxio** caches remote data close to the compute that reads it, **Ozone** is an object store built to replace HDFS on premises, and **Kyuubi** is a multi-tenant SQL gateway that turns Spark into a shared, serverless-feeling service.

The group ends with the question every Hadoop team now faces: what is still worth running, what to replace, and how to move a Hadoop platform to Google Cloud.

### Alluxio — Distributed Caching Layer

::: plain
When your data lives in a warehouse across town (object storage, or an HDFS cluster in another data centre), every trip to fetch a box takes time, even if you fetched the same box yesterday. Alluxio is a local storeroom next to your workshop. The first time a box is needed, it is brought from the far warehouse and a copy is kept in the storeroom; the next time, it is right there. Workers keep using the same addresses, and the storeroom decides what to keep based on what is used most.
:::

**Why a caching layer.** Once compute and storage are separated, every Spark or Trino query reads its data over the network from Cloud Storage or a remote HDFS cluster. That works, but repeated reads of the same hot tables pay latency, throughput limits and sometimes cross-region or egress costs every time, and object store metadata operations (listing, `stat`) are slower than local file systems. **Alluxio** (originally Tachyon, from UC Berkeley's AMPLab in 2013) sits between compute engines and storage systems and caches data and metadata on the compute side.

**Architecture.**

- **Master**: manages the namespace and metadata, including which blocks are cached on which worker. It journals changes and runs in HA mode with an embedded Raft journal.
- **Workers**: run on or near the compute nodes and cache blocks in **tiered storage**, typically memory (`MEM`), then SSD, then HDD. Hot blocks are promoted to faster tiers and evicted with an LRU-style policy when a tier fills.
- **Clients**: libraries inside Spark, Trino, Hive or the FUSE mount (`alluxio-fuse`) that read through workers, preferring a worker on the same machine (short-circuit local reads when possible).
- **Under file systems (UFS)**: the real storage, mounted into one **unified namespace**. A single Alluxio path tree can contain mounts from Cloud Storage, HDFS and other systems, so a job can join data from two storage systems through one namespace.

```bash
# Mount a Cloud Storage bucket and an on-premises HDFS path into one namespace
alluxio fs mount /lake gs://acme-lake/
alluxio fs mount /onprem hdfs://ns1/warehouse/
alluxio fs ls /lake/sales/orders/
alluxio fs load /lake/sales/orders/order_date=2026-11-23/   # warm the cache before the morning dashboards
```

```text
# alluxio-site.properties: two cache tiers on each worker
alluxio.worker.tieredstore.levels=2
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk
alluxio.worker.tieredstore.level0.dirs.quota=64GB
alluxio.worker.tieredstore.level1.alias=SSD
alluxio.worker.tieredstore.level1.dirs.path=/mnt/ssd0
alluxio.worker.tieredstore.level1.dirs.quota=1TB
```

Applications then read `alluxio://master:19998/lake/sales/orders/` instead of `gs://acme-lake/sales/orders/`. Some deployments add a transparent-URI shim so jobs keep their original `hdfs://` or `gs://` paths and the client redirects reads through Alluxio without code changes.

**Reads and writes.** On a read, the client asks for the block; if a worker has it, it is served from cache (ideally from the same node, the **data locality** that HDFS gave you for free and object storage took away); otherwise the worker reads it from the UFS, returns it and keeps a copy. Writes are configurable: through to the UFS only, cache and UFS together, cache only (fast, not durable), or cache first with asynchronous persistence. For a lake whose source of truth is Cloud Storage, write-through keeps the cache from ever holding the only copy.

**Where it pays off.**

- **Hybrid cloud bursting**: compute in Google Cloud, data still in an on-premises HDFS cluster. Alluxio caches the working set in the cloud, so repeated queries do not cross the interconnect again.
- **Interactive SQL on object storage**: Trino or Presto dashboards that read the same tables all day.
- **AI and ML training**: many epochs over the same training files from many GPUs, where storage throughput and metadata latency starve the GPUs.

> **Pitfall:** A cache only helps when data is read more than once. Full-table ETL that reads each file once gains nothing and adds a hop. Measure the hit ratio on your real workload before adding a cluster of cache workers.

> **Pitfall:** Caches can serve stale data. If another system rewrites files in the UFS, Alluxio must re-check metadata (configured with a sync interval) or it keeps serving old copies. Immutable data, as in table formats that only add new files, avoids the problem.

::: deeper
**Metadata matters as much as data.** Listing a directory with 100,000 objects in an object store takes many paginated API calls; Alluxio's master caches listings and file status. That is often a bigger win for query planning than block caching, but it is also where staleness appears.

**Architectural evolution.** The classic design keeps all metadata in one master, which bounds scale. Alluxio's newer commercial editions, focused on AI workloads, moved to a decentralized design in which workers own data and metadata by consistent hashing and clients find the right worker without asking a master. Check which edition and version you are reading about, because documentation for the open-source 2.x line and the commercial editions differ.

**Caches built into engines.** Several engines now embed a local cache instead of a separate service: Trino has a file system cache for object storage that is built on Alluxio's caching library, Presto has a similar local cache, Spark platforms offer disk caches of Parquet data on local SSDs, and BigQuery caches internally. A separate Alluxio cluster makes sense when several engines share one hot data set, or when the unified namespace across storage systems is the point.

**On Google Cloud.** Alluxio can run on Managed Service for Apache Spark clusters (through initialization actions) or on GKE, with workers' cache on Local SSD, which suits a recomputable cache well. Compare it first with simpler options: placing compute in the same region as the bucket, Cloud Storage's own features for high-throughput reads, and larger, fewer files.

> **Interview:** "Storage and compute are separated in the cloud. What do you lose, and how do you get some of it back?" Data locality: every read crosses the network, and metadata operations on object storage are slower than on a local file system. A caching layer (Alluxio, or an engine's local SSD cache) restores locality for hot data, and table formats with manifests reduce metadata calls.
:::

::: check
?? What is a UFS in Alluxio?
An under file system: the real storage (Cloud Storage, HDFS and others) mounted into Alluxio's namespace. Alluxio caches its data and metadata; the UFS stays the source of truth.

?? For which workload would Alluxio help least?
A job that reads every file once, such as a full-table ETL pass. The cache never gets a second read to serve, so it only adds a hop.

?? Why is write-through usually preferred when the UFS is the source of truth?
It persists data to the UFS as it is written, so the cache never holds the only copy and a worker failure cannot lose committed data.
:::

::: resources
- Docs: [Alluxio open-source documentation](https://docs.alluxio.io/os/user/stable/en/Overview.html) · architecture, tiered storage, mounts and engine integrations
- Repo: [Alluxio on GitHub](https://github.com/Alluxio/alluxio) · source code of the open-source edition
- Paper: [Tachyon: Reliable, Memory Speed Storage for Cluster Computing Frameworks (SoCC 2014)](https://dl.acm.org/doi/10.1145/2670979.2670985) · the original design from AMPLab
- Paper: [Alluxio: A Virtual Distributed File System (Haoyuan Li, PhD thesis)](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html) · the full design rationale by the project's creator
- Docs: [Trino file system cache](https://trino.io/docs/current/object-storage/file-system-cache.html) · an engine-embedded cache built on Alluxio's library, the lighter-weight alternative
:::

### Apache Ozone — Hadoop Object Storage

::: plain
HDFS's catalogue desk keeps every chapter of every book in one librarian's head, and that head has a limit. Ozone changes the filing system. Chapters are packed into large crates (containers), and the central office only tracks crates, not chapters, while a separate index of book titles lives in a fast on-disk database instead of in memory. The library can now hold billions of small and large items, and visitors can use either the old library-card style (file paths) or the modern locker-number style (S3 buckets and keys).
:::

**Why Ozone exists.** Organisations that keep data on premises still want what object storage gave the cloud: billions of objects, an S3 API that new tools expect, and no small-files crisis. HDFS cannot provide that because its NameNode holds every file and block in one heap. **Apache Ozone**, started inside the Hadoop project and an Apache top-level project since 2020, is a distributed object store for the Hadoop ecosystem that scales metadata out and speaks both Hadoop file system and S3 protocols.

**The namespace.** Ozone has three levels: **volumes** (like accounts or tenants, created by administrators), **buckets** inside volumes, and **keys** (objects) inside buckets.

**The services.**

- **Ozone Manager (OM)** manages the namespace: volumes, buckets and keys, with their metadata, stored in **RocksDB** on local disk rather than in the JVM heap. It is made highly available with **Apache Ratis**, a Raft implementation, across three OM nodes.
- **Storage Container Manager (SCM)** manages storage: it groups blocks into **containers** (5 GB by default), places container replicas on DataNodes, organises DataNodes into replication **pipelines**, tracks DataNode health and issues security certificates. It also runs on Ratis for HA.
- **DataNodes** store containers. A container is the unit of replication: three replicas through a Ratis pipeline (writes go through Raft consensus, so replicas stay consistent), or erasure-coded.
- **Recon** is the monitoring and management UI; **S3 Gateway** (`s3g`) serves the S3 REST API.

**Why containers scale.** SCM tracks containers, not blocks, and DataNodes report containers, not blocks. Compare what the central services track for 10 PB:

```python
# Objects the central metadata service must track for 10 PB of data
PB = 10**15
data = 10 * PB
hdfs_blocks = data // (128 * 2**20)       # HDFS: the NameNode tracks every 128 MiB block
ozone_containers = data // (5 * 2**30)    # Ozone: SCM tracks 5 GiB containers, each holding many blocks
print(f"HDFS blocks tracked by the NameNode: {hdfs_blocks:,}")
print(f"Ozone containers tracked by SCM:     {ozone_containers:,}")
print(f"ratio: {hdfs_blocks // ozone_containers}x fewer objects in SCM")
# HDFS blocks tracked by the NameNode: 74,505,805
# Ozone containers tracked by SCM:     1,862,645
# ratio: 40x fewer objects in SCM
```

The key namespace still grows with the number of objects, but it lives in RocksDB on SSD, which holds billions of entries without the heap and garbage-collection limits of the NameNode.

**Two bucket layouts.** Object stores and file systems want different things, so each bucket chooses:

| Layout | Semantics | Good for |
|---|---|---|
| `FILE_SYSTEM_OPTIMIZED` (FSO) | Real directories in OM, atomic directory rename and delete | Hive, Spark and other Hadoop workloads that commit by renaming |
| `OBJECT_STORE` (OBS) | Flat keys, S3 semantics, "directories" are just key prefixes | S3-native applications and tools |

```bash
ozone sh volume create /analytics
ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /analytics/events
ozone sh key put /analytics/events/2026/11/23/part-0000.parquet ./part-0000.parquet
ozone fs -ls ofs://ozone1/analytics/events/2026/11/    # Hadoop-compatible rooted file system
```

Applications use **`ofs://`**, a rooted Hadoop-compatible file system that sees all volumes and buckets as directories, so Spark, Hive and Impala jobs need only a path change. S3 clients, including Hadoop's own `s3a` connector, point at the S3 Gateway instead.

**Erasing data for privacy.** Ozone supports the right to erasure under the GDPR with **GDPR-enforced buckets** (`ozone sh bucket create --enforcegdpr=true /hr/records`). Each key in such a bucket is encrypted with its own secret stored in the key's metadata. Deleting the key deletes that secret immediately, so the data becomes unreadable even before its blocks are physically reclaimed: a form of **crypto-shredding**. The setting applies only to new buckets, so existing data must be copied into one.

**Moving from HDFS.** The usual path is gradual: create FSO buckets, copy data with DistCp (`hadoop distcp hdfs://ns1/warehouse ofs://ozone1/warehouse/hive`), point Hive table locations at the new paths, and keep HDFS running for what has not moved. Cloudera's platform ships Ozone as its on-premises object store for this purpose.

> **Pitfall:** Choose the bucket layout up front. An OBS bucket gives Hadoop jobs no atomic directory rename, so a job committer that renames a temporary directory into place becomes a slow copy of every file, and failures can leave partial output. Use FSO for Hadoop-style workloads.

> **Tip:** Ozone is an on-premises answer. On Google Cloud you already have this in Cloud Storage: unlimited objects, the Hadoop-compatible `gs://` connector, and buckets with hierarchical namespace for atomic folder renames. Migrating Ozone or HDFS data to Google Cloud means copying into Cloud Storage, not running Ozone on VMs.

::: deeper
**The write path.** A client asks OM to open a key; OM asks SCM for a block in an open container on a healthy pipeline; the client writes chunks directly to the pipeline's DataNodes, where the Ratis leader replicates through Raft; on close, the client commits the key's block list to OM. Readers get block locations from OM and read from any replica, verifying checksums.

**Containers have a lifecycle.** A container is **open** while it accepts writes and **closed** once full (or when its pipeline has a problem). Closed containers are immutable, which makes replication, erasure coding and repair simpler: SCM's replication manager re-creates missing closed replicas much as the NameNode re-replicates blocks, but with far fewer objects to reason about.

**Multi-tenancy and security.** Ozone supports Kerberos, delegation tokens, TLS between services (SCM acts as a certificate authority), Ranger authorization, transparent encryption with a KMS, and S3 secrets per user. Its multi-tenancy feature maps S3 tenants to volumes with Ranger-managed access.

**Snapshots.** Recent Ozone releases add bucket snapshots and snapshot diffs, which help incremental replication between clusters (compute "what changed since the last snapshot" and copy only that).

> **Interview:** "Why does Ozone scale to more objects than HDFS?" HDFS keeps every file and block in the NameNode's heap and receives block reports for every block. Ozone separates the namespace (OM, in RocksDB on disk) from block management (SCM, which tracks large containers rather than individual blocks), so neither central service is limited by one JVM heap, and DataNode reports stay small.
:::

::: check
?? What do Ozone Manager and Storage Container Manager each manage?
OM manages the namespace (volumes, buckets, keys and their metadata, in RocksDB). SCM manages storage (containers, their replicas and pipelines, and DataNode health).

?? Which bucket layout should a Hive warehouse use, and why?
`FILE_SYSTEM_OPTIMIZED`, because it provides real directories with atomic rename and delete, which Hive and Spark commit protocols depend on.

?? How does a GDPR-enforced bucket make a deleted key unreadable immediately?
Each key is encrypted with its own secret stored in the key's metadata. Deleting the key destroys the secret, so the remaining encrypted blocks cannot be decrypted, even before they are physically reclaimed.
:::

::: resources
- Docs: [Apache Ozone documentation](https://ozone.apache.org/docs/) · architecture, bucket layouts, interfaces and administration
- Repo: [apache/ozone on GitHub](https://github.com/apache/ozone) · source, design documents and release notes
- Docs: [GDPR in Ozone](https://ozone.apache.org/docs/1.4.1/security/gdpr.html) · how GDPR-enforced buckets implement the right to erasure
- Docs: [Apache Ratis](https://ratis.apache.org/) · the Raft library Ozone uses for OM, SCM and DataNode pipelines
- Docs: [Cloud Storage hierarchical namespace](https://docs.cloud.google.com/storage/docs/hns-overview) · the Google Cloud equivalent of file-system-optimized buckets
:::

### Apache Kyuubi — JDBC/SQL Gateway

::: plain
Imagine a building with one reception desk and a pool of meeting rooms. Visitors (BI tools, notebooks, scripts) arrive at reception and ask for a room; reception checks their badge, then either shows them to the room their team already has open or books a new one, and closes rooms that have been empty for half an hour. Kyuubi is that reception desk for Spark: clients connect with ordinary JDBC, and Kyuubi starts, shares and shuts down Spark applications behind the scenes.
:::

**The problem.** Analysts and BI tools want to send SQL over JDBC or ODBC and get rows back. Spark is a library that runs as an application; somebody has to start the application, keep it running and route queries to it. The traditional answer, the **Spark Thrift Server**, is one long-running Spark application that speaks the HiveServer2 protocol. It works, but every user shares one driver (one crash affects all), every query runs with the server's identity and configuration, and it cannot scale beyond one driver's capacity.

**What Kyuubi does.** Apache Kyuubi (from NetEase, an Apache top-level project since 2022) is a multi-tenant gateway in front of Spark and other engines:

- **Frontends**: clients connect using the HiveServer2 Thrift protocol (so `beeline`, Hive JDBC drivers and BI tools work unchanged, on port 10009 by default), or through REST, the MySQL protocol, or the Trino protocol.
- **Authentication**: Kerberos, LDAP, JDBC-based or custom authentication at the gateway, so each session belongs to a real user.
- **Engines**: Kyuubi launches **engines**, which are ordinary Spark applications (on YARN or Kubernetes) submitted as the connecting user, and routes that user's SQL to them. Flink, Trino, Hive and generic JDBC engines are also supported.
- **Service discovery and HA**: several Kyuubi servers register in ZooKeeper (or etcd); clients find a live server and engines register so any server can route to them.

**Engine share levels.** The key setting is who shares an engine:

| Share level | One engine per | Isolation | Start-up cost |
|---|---|---|---|
| `CONNECTION` | JDBC connection | Highest: one Spark application per session | Paid on every connection |
| `USER` (default) | User | Users isolated; a user's sessions share an engine | Paid once per user, then reused |
| `GROUP` | Primary group | Teams share an engine and its cache | Paid once per group |
| `SERVER` | Kyuubi server | Lowest: everyone shares, like the Spark Thrift Server | Paid once |

A **subdomain** setting lets one user keep several separate engines (for example one for ETL, one for exploration).

**Engine lifecycle.**

1. A client opens a session. Kyuubi authenticates it and computes the engine key from the share level (for `USER`, the user name).
2. Kyuubi looks the key up in the discovery service. If an engine is registered, the session is attached to it.
3. If not, Kyuubi runs `spark-submit` for that user with the merged configuration (server defaults, then user defaults, then session settings), waits for the engine to register, and attaches the session.
4. When the engine has had no sessions for the idle timeout (`kyuubi.session.engine.idle.timeout`, 30 minutes by default), it shuts itself down and its resources return to the cluster.

```text
# kyuubi-defaults.conf
kyuubi.authentication                   KERBEROS
kyuubi.engine.share.level               USER
kyuubi.session.engine.idle.timeout      PT30M
kyuubi.ha.addresses                     zk1:2181,zk2:2181,zk3:2181
spark.master                            yarn
spark.dynamicAllocation.enabled         true
spark.dynamicAllocation.maxExecutors    50
```

```bash
# Connect with beeline; settings after # apply to this session's engine
beeline -u 'jdbc:hive2://kyuubi.example.com:10009/default;#kyuubi.engine.share.level=CONNECTION;spark.executor.memory=8g' -n ana
```

With dynamic allocation, an idle engine holds only its driver, and executors appear when queries arrive, which gives a shared cluster a serverless feel: users get a SQL endpoint, the cluster pays only for what runs.

**Authorization.** Because each engine runs as the real user, storage permissions apply naturally. For table, column and row-level rules on Spark SQL, Kyuubi provides a Spark extension (the **AuthZ** plugin) that checks Apache Ranger policies inside the engine, the same policies used for Hive.

> **Pitfall:** `CONNECTION` share level with many short-lived BI connections means a Spark application start (often 20 seconds or more) per connection, plus a driver per connection. Use `USER` or `GROUP` for BI tools, and keep `CONNECTION` for heavy, isolated jobs.

> **Tip:** Set queue and resource limits per user or group in the Kyuubi configuration (for example `spark.yarn.queue` in user-level defaults), so one team's exploratory engine cannot take the ETL queue.

::: deeper
**Kyuubi versus the alternatives.**

| | Spark Thrift Server | Kyuubi | Spark Connect |
|---|---|---|---|
| Protocol | HiveServer2 Thrift | HiveServer2 Thrift, REST, MySQL, Trino | gRPC with DataFrame plans |
| Applications | One shared | Many, per share level | One server per Spark application |
| Identity | Server's | Each user's | Per session, within one application |
| Typical client | BI tools, JDBC | BI tools, JDBC, notebooks | PySpark and Scala clients |

Spark Connect (covered in [Apache Spark](#spark)) solves the client side: a thin client sends DataFrame plans to a remote Spark server. Kyuubi solves the multi-tenant server side, and recent Kyuubi versions can also front Spark Connect.

**Engine pools and balancing.** An engine pool (`kyuubi.engine.pool.size`) keeps several engines for one share key and spreads sessions over them, so a large group does not overload one driver. Engines can also be launched on Kubernetes with per-user namespaces and pod templates.

**On Google Cloud.** Kyuubi can run on a Managed Service for Apache Spark cluster's master node (launching engines on its YARN) or on GKE launching Spark on Kubernetes. Google Cloud's managed alternatives cover parts of the same need: serverless interactive Spark sessions in Managed Service for Apache Spark (used from notebooks), and BigQuery itself as a SQL endpoint for BI tools. Kyuubi fits when you want a Hive-compatible JDBC endpoint over Spark with per-user engines.

> **Interview:** "How would you give 200 analysts SQL access to Spark without one shared Thrift server?" A gateway such as Kyuubi: it authenticates each user, launches or reuses a Spark engine per user or group (share level), runs it under the user's identity with dynamic allocation, applies Ranger policies through the AuthZ plugin, and stops idle engines after a timeout.
:::

::: check
?? What is the default engine share level in Kyuubi, and what does it mean?
`USER`: each user gets one Spark engine shared by all of that user's sessions, isolated from other users and reused across connections.

?? Why is Kyuubi more robust than a single Spark Thrift Server?
Engines are separate Spark applications per user, group or connection, so one user's failing query or out-of-memory driver does not take everyone down, and each engine runs with the user's identity and settings.

?? What happens to an engine nobody is using?
After the engine idle timeout (30 minutes by default) with no sessions, it shuts down and releases its cluster resources; the next session starts a new one.
:::

::: resources
- Docs: [Apache Kyuubi documentation](https://kyuubi.readthedocs.io/en/master/) · architecture, share levels, configuration and security
- Docs: [Apache Kyuubi](https://kyuubi.apache.org/) · project overview, releases and news
- Repo: [apache/kyuubi on GitHub](https://github.com/apache/kyuubi) · source, the AuthZ plugin and deployment examples
- Docs: [Distributed SQL engine (Spark Thrift Server)](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html) · the single-application alternative Kyuubi improves on
- Docs: [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html) · the client-server protocol that complements gateways
:::

### The Hadoop ecosystem today: what is still relevant

::: plain
Hadoop is like an old city. Some buildings are still in daily use and well maintained, some have been converted to new purposes, and some stand empty or have been pulled down. You do not need to live there to benefit from knowing the streets, because the new districts were laid out on the same plan. This topic is the map: which parts are alive, which are retired, and how to move out when the time comes.
:::

**What happened.** Hadoop solved "store and process petabytes on cheap machines" around 2008 to 2015. Then three things changed. Cloud object storage made storage cheap, elastic and someone else's problem. Spark replaced MapReduce as the processing engine. And Kubernetes and serverless services replaced long-lived, fixed-size clusters for many workloads. The big Hadoop distributors merged (Cloudera and Hortonworks in 2019), and much of the ecosystem consolidated into a few platforms. Yet many large companies still run big Hadoop clusters, and nearly every modern data platform reuses Hadoop's components or ideas.

**Component by component.**

| Component | Status | What replaces or hosts it on Google Cloud |
|---|---|---|
| HDFS | Still run on premises; rarely chosen for new cloud systems | Cloud Storage via the `gs://` connector |
| YARN | Active; the resource manager of Managed Service for Apache Spark clusters | Clusters, serverless Spark batches, or GKE |
| MapReduce | Legacy; few new jobs | Spark, Dataflow (Apache Beam), BigQuery |
| Hive (SQL engine) | Active (Hive 4), mainly for existing workloads | BigQuery, Spark SQL; Hive on clusters |
| Hive Metastore | Very much alive as a catalog | Dataproc Metastore; Lakehouse runtime catalog (formerly BigLake metastore); Iceberg REST catalogs |
| Tez | Active, inside Hive | Comes with Hive on clusters |
| Impala, Kudu | Active, mainly in Cloudera's platform | BigQuery, Trino |
| HBase | Active, large deployments remain | Bigtable |
| Kylin | Active, niche | BigQuery materialized views and BI Engine, real-time OLAP engines |
| Ranger, Knox | Active; Ranger also used well beyond Hadoop | Ranger component; IAM, Knowledge Catalog, BigQuery policies |
| ZooKeeper | Active, but Kafka 4.0 removed its dependency (KRaft) | Fewer systems need it |
| Ozone | Active, on premises | Cloud Storage |
| Kyuubi | Active and growing | Kyuubi on clusters or GKE; BigQuery as SQL endpoint |
| Oozie, Sqoop, Sentry | Retired to the Apache Attic | Managed Service for Apache Airflow (formerly Cloud Composer); Datastream or Spark JDBC; Ranger |

Two kinds of things are worth carrying forward from Hadoop even if you never run it: **formats and catalogs** (Parquet and ORC, Hive-style partition directories, the metastore API, which Iceberg and Lakehouse catalogs build on), and **ideas** (move computation to data, partition and sort to group, recompute from lineage to survive failures, avoid small files).

**Migration patterns to Google Cloud.** Google's guidance describes a progression rather than one big move:

1. **Lift and shift.** Run the same Hive, Spark and MapReduce jobs on Managed Service for Apache Spark clusters, with initialization actions for custom software. Quick, low risk, limited savings if clusters stay up all the time.
2. **Separate storage from compute.** Move data from HDFS to Cloud Storage (DistCp with `gs://` destinations, or Storage Transfer Service, which can copy from an HDFS source), point table locations at `gs://` paths, and move the metastore to Dataproc Metastore. Clusters now hold no data.
3. **Make clusters ephemeral.** Create a cluster per job or per workflow (from Managed Airflow or Workflows), use Spot VMs for secondary workers, or switch Spark jobs to serverless batches. You pay for compute only while jobs run.
4. **Modernise where it pays.** Move SQL workloads to BigQuery (the BigQuery Migration Service translates HiveQL), HBase to Bigtable, Oozie workflows to Managed Airflow, and Hive tables to Apache Iceberg managed tables in BigQuery (formerly BigQuery tables for Apache Iceberg) or other Iceberg tables in Google Cloud Lakehouse so that BigQuery and Spark share one copy.

```bash
# Step 2 in practice: copy a warehouse directory to Cloud Storage, incrementally
hadoop distcp -update -delete -m 200 \
  hdfs://ns1/warehouse/sales gs://acme-lake/warehouse/sales

# Step 3: a job-scoped cluster, used once and deleted
gcloud dataproc clusters create etl-2026-11-23 --region=europe-west1 \
  --num-workers=10 --secondary-worker-type=spot --num-secondary-workers=20 \
  --max-idle=15m
gcloud dataproc jobs submit hive --cluster=etl-2026-11-23 --region=europe-west1 \
  --file=gs://acme-jobs/daily_sales.sql
gcloud dataproc clusters delete etl-2026-11-23 --region=europe-west1 --quiet
```

**Total cost of ownership.** Cost comparisons usually turn on two numbers: utilisation and replication. An on-premises cluster is sized for peak load and paid for around the clock; HDFS stores three copies on disks you buy, plus free space for growth. With storage in Cloud Storage and clusters per job:

```python
# Node-hours per month: an always-on cluster versus clusters created per job (illustrative)
nodes, days = 100, 30
busy_hours_per_day = 6                   # the cluster is actually busy 6 hours a day
always_on = nodes * 24 * days
per_job = nodes * busy_hours_per_day * days
print(f"always-on: {always_on:,} node-hours, per-job clusters: {per_job:,} node-hours "
      f"({per_job / always_on:.0%} of the always-on cost)")

# Raw disk needed for 2 PB of data
hdfs_raw_pb = 2 * 3                      # HDFS with 3x replication, before headroom for growth
print(f"HDFS raw capacity: {hdfs_raw_pb} PB (plus ~25% free space); object storage bills 2 PB of data")
# always-on: 72,000 node-hours, per-job clusters: 18,000 node-hours (25% of the always-on cost)
# HDFS raw capacity: 6 PB (plus ~25% free space); object storage bills 2 PB of data
```

A fair comparison also counts what the simple numbers leave out: on-premises hardware already paid for, data centre space, power and staff on one side; cloud storage and network charges, egress during migration, and the engineering time to rewrite jobs on the other. Clusters that are busy around the clock change the picture, which is why steady, predictable workloads are the ones most often kept on premises or moved to committed-use pricing.

**When to keep Hadoop.** Keeping a cluster is reasonable when data must stay on premises (regulation, sovereignty, air-gapped networks), when the hardware is paid for and fully used, when low-latency HBase applications sit next to it, or when the cost and risk of rewriting thousands of jobs is higher than the savings. Hybrid setups are common: new workloads in the cloud, old ones on premises, with data copied or cached between them.

> **Pitfall:** Lifting and shifting an always-on cluster with HDFS on its disks usually costs *more* in the cloud than on premises, because you pay rental prices for the same idle capacity and still replicate data three times. The savings come from steps 2 and 3: storage in Cloud Storage and compute that exists only while jobs run.

> **Tip:** Inventory before migrating. Pull the job list from the YARN and Oozie history, the table list and sizes from the metastore, and access patterns from Ranger audit logs. Many clusters carry a long tail of jobs and tables nobody uses; not migrating them is the cheapest optimisation.

::: deeper
**HDFS semantics in the cloud.** Code that assumes HDFS behaviour can break on object storage: in a standard Cloud Storage bucket, directory rename is a copy and delete of every object, and listing is slower than a NameNode lookup. Spark and Hive output committers that rename temporary directories become slow and are not atomic. Fixes: use table formats (Iceberg, Delta, Hudi) that commit by writing metadata instead of renaming, use buckets with hierarchical namespace, which support atomic folder renames, and avoid huge numbers of small objects.

**Security model changes.** On premises, identity is Kerberos and authorization is Ranger plus HDFS permissions. On Google Cloud, identity is IAM (users, groups and service accounts), storage access is IAM on buckets and managed folders, and fine-grained table, column and row rules live in BigQuery and Knowledge Catalog. Clusters can still be Kerberized with Ranger for workloads that need Hadoop-style per-user access, but most migrations simplify to service accounts per job.

**Metastore as the migration bridge.** Pointing on-premises and cloud engines at a shared catalog, or replicating the metastore, lets workloads move one at a time while table names stay the same. Converting Hive tables to Iceberg in place (Iceberg can import existing files with `migrate` or `add_files` procedures) avoids rewriting data.

**What the industry kept.** BigQuery's architecture (separate storage and compute, a distributed shuffle tier), Spark's scheduler, and the lakehouse table formats all solve problems Hadoop first exposed: the NameNode's metadata limits led to manifest-based table formats; MapReduce's disk-heavy shuffles led to in-memory and remote shuffle services; YARN's queues reappear as Kubernetes batch schedulers and BigQuery reservations.

> **Interview:** "You are asked to migrate a 2 PB on-premises Hadoop cluster to Google Cloud. What is your plan?" Inventory jobs, tables and users; copy data to Cloud Storage incrementally with DistCp or Storage Transfer Service while the cluster runs; move the metastore to Dataproc Metastore; run existing jobs on job-scoped Managed Service for Apache Spark clusters with Spot secondary workers; validate results in parallel runs; then modernise by workload (SQL to BigQuery, HBase to Bigtable, Oozie to Managed Airflow, tables to Iceberg), decommissioning the old cluster in phases.
:::

::: check
?? Which Hadoop component is most alive in modern lakehouse platforms, even where Hadoop itself is gone?
The Hive Metastore (and its API), used as the catalog by Spark, Trino, Flink and others, alongside ORC and Parquet formats and Hive-style partitioning.

?? Why does a pure lift and shift of an always-on Hadoop cluster rarely save money?
You rent the same peak-sized, mostly idle capacity and still store three replicas on cluster disks. Savings come from moving data to object storage and running clusters only while jobs run.

?? Name two situations where keeping an on-premises Hadoop cluster is a reasonable choice.
Any two of: data that must stay on premises for regulation or sovereignty; fully used, already paid-for hardware; low-latency HBase applications next to the cluster; a rewrite cost and risk greater than the expected savings.
:::

::: resources
- Docs: [Migrating on-premises Hadoop infrastructure to Google Cloud](https://docs.cloud.google.com/architecture/hadoop) · Google's architecture guides for data, jobs, metastore and security migration
- Docs: [Managed Service for Apache Spark documentation](https://docs.cloud.google.com/dataproc/docs) · clusters, components, autoscaling and serverless batches
- Docs: [Transfer data from HDFS to Cloud Storage](https://docs.cloud.google.com/storage-transfer/docs/hdfs) · Storage Transfer Service with on-premises agents
- Docs: [BigQuery SQL translation](https://docs.cloud.google.com/bigquery/docs/batch-sql-translator) · translating HiveQL and other dialects to GoogleSQL in batch
- Blog: [Migrating from Hadoop to the lakehouse](https://www.databricks.com/blog/2021/08/06/5-key-steps-to-successfully-migrate-from-hadoop-to-the-lakehouse-architecture.html) · a vendor's step-by-step view of the same migration
- Docs: [The Apache Attic](https://attic.apache.org/) · which Apache projects have been retired, including Oozie, Sqoop and Sentry
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, setting or rule of thumb |
|---|---|
| HDFS block size and replication | `dfs.blocksize` 128 MB, `dfs.replication` 3 |
| Where are a file's blocks? | `hdfs fsck /path -files -blocks -locations -racks` |
| Cluster capacity and dead nodes | `hdfs dfsadmin -report` |
| Rebalance DataNodes | `hdfs balancer -threshold 10` (quiet hours, limited bandwidth) |
| Retire a DataNode | Add to `dfs.hosts.exclude`, then `hdfs dfsadmin -refreshNodes` |
| Cold data storage | `hdfs ec -setPolicy -path /archive -policy RS-6-3-1024k` (1.5x instead of 3x) |
| Which NameNode is active? | `hdfs haadmin -getServiceState nn1` |
| Copy between namespaces or to Cloud Storage | `hadoop distcp -update src dst` |
| YARN applications and logs | `yarn application -list`, `yarn logs -applicationId ID` |
| Reload queue configuration | `yarn rmadmin -refreshQueues` |
| Container sizing | Requests round up to `yarn.scheduler.minimum-allocation-mb`; Spark adds max(384 MB, 10%) overhead |
| MapReduce combiner | Only for associative, commutative operations; average needs (sum, count) |
| Kerberos login | `kinit -kt service.keytab principal@REALM`, check with `klist` |
| Hive partitions | Low-cardinality columns (dates); partitions of hundreds of MB or more |
| Impala sees new files | `REFRESH table`; new tables: `INVALIDATE METADATA table` |
| HBase row key | Lead with a well-distributed value; salt or hash monotonic keys; pre-split |
| Kylin cuboids | 2^N combinations; prune with mandatory, hierarchy and joint dimensions |
| Kyuubi engine sharing | `kyuubi.engine.share.level`: CONNECTION, USER (default), GROUP, SERVER |
| Hadoop on Google Cloud | Data in Cloud Storage, metastore in Dataproc Metastore, job-scoped clusters or serverless batches |

### Practice

::: try
**1. Place the replicas.** A cluster has two racks, A (nodes a1 to a5) and B (nodes b1 to b5). A client running on a3 writes a file with replication 3. Under the default policy, where can the three replicas go? What happens if the client runs on a laptop outside the cluster? Which failure would lose data?
:::

?? Answer to 1
First replica on a3 (the writer's node); second on a node in rack B, say b2; third on another node in rack B, say b4. From an outside laptop, the first replica goes on a random node, and the other two follow the same rule relative to it. Data is lost only if all three nodes fail before re-replication (for example rack B plus a3), because losing a whole rack still leaves one copy.

::: try
**2. Size the NameNode.** A team stores 400 TB as Parquet files averaging 8 MB, written by a streaming job every minute. Estimate the NameNode heap for these files with the 150-byte rule, then estimate it again if a nightly compaction rewrites them into 512 MB files. Write the calculation in Python.
:::

?? A solution to 2

```python
TB, PER_OBJECT, BLOCK = 10**12, 150, 128 * 2**20
for size in (8 * 2**20, 512 * 2**20):
    files = 400 * TB // size
    blocks = files * -(-size // BLOCK)
    print(f"{size // 2**20:4} MB files: {files:,} files, ~{(files + blocks) * PER_OBJECT / 1e9:.1f} GB heap")
#    8 MB files: 47,683,715 files, ~14.3 GB heap
#  512 MB files: 745,058 files, ~0.6 GB heap
```

Compaction cuts metadata by about 25 times, and it also turns 47 million map tasks or file opens into fewer than a million.

::: try
**3. Design the queues.** Three teams share a YARN cluster: ETL must always get 50 percent and finish nightly jobs by 06:00; analysts run ad hoc queries during the day and should get 35 percent; data science needs GPUs on four labelled nodes and 15 percent otherwise. Write the Capacity Scheduler settings (as name and value pairs) and say whether you would enable preemption.
:::

?? A solution to 3

```text
yarn.scheduler.capacity.root.queues                    = etl,adhoc,ds
yarn.scheduler.capacity.root.etl.capacity              = 50
yarn.scheduler.capacity.root.etl.maximum-capacity      = 100
yarn.scheduler.capacity.root.adhoc.capacity            = 35
yarn.scheduler.capacity.root.adhoc.maximum-capacity    = 60
yarn.scheduler.capacity.root.adhoc.user-limit-factor   = 1
yarn.scheduler.capacity.root.ds.capacity               = 15
yarn.scheduler.capacity.root.ds.maximum-capacity       = 40
yarn.scheduler.capacity.root.ds.accessible-node-labels = gpu
yarn.scheduler.capacity.root.ds.accessible-node-labels.gpu.capacity = 100
```

Make `gpu` an exclusive label so only `ds` uses those nodes. Enable preemption (`yarn.resourcemanager.scheduler.monitor.enable=true`) so ETL can reclaim its 50 percent at night even if ad hoc queries borrowed it; MapReduce, Tez and Spark tasks tolerate being preempted.

::: try
**4. Fix the row key.** An HBase table stores clicks with row key `yyyyMMddHHmmss|user_id`. Writes are concentrated on one RegionServer, and the main query is "the last 50 clicks of a user". Propose a new row key and explain how both problems are solved.
:::

?? Answer to 4
Use `user_id|reversed_timestamp`, where the reversed timestamp is Long.MAX_VALUE minus the event time in milliseconds, padded to a fixed width. Writes spread across regions because users are spread across the key space (hash the user ID first if IDs are sequential), and the last 50 clicks of a user are the first 50 rows of a scan starting at `user_id|`, because the newest rows sort first. Pre-split the table on the user ID (or hash) distribution.

::: try
**5. Plan a migration.** A company runs a 60-node Hadoop cluster: 900 TB in HDFS, 2,000 Hive queries scheduled by Oozie, a Ranger-secured Hive warehouse used by 150 analysts through a Spark Thrift Server, and an HBase table serving a customer-facing API. Outline a migration to Google Cloud in phases, naming the service that replaces or hosts each piece.
:::

?? A solution to 5
Phase 1: inventory jobs, tables and usage; copy HDFS data to Cloud Storage with DistCp or Storage Transfer Service, incrementally; move the metastore to Dataproc Metastore. Phase 2: run the Hive jobs unchanged on job-scoped Managed Service for Apache Spark clusters (Spot secondary workers), orchestrated by Managed Airflow, converting Oozie workflows to Airflow DAGs; compare outputs with the on-premises runs. Phase 3: give analysts BigQuery over the warehouse (translate HiveQL with the BigQuery Migration Service, or query Iceberg tables in Google Cloud Lakehouse), with access rules in BigQuery and Knowledge Catalog instead of Ranger; or, if Spark SQL must stay, run Kyuubi with Ranger on a cluster. Phase 4: migrate the HBase table to Bigtable (schema translation, snapshot import, then replication until cut-over), switch the API, then decommission the cluster.

### What comes next

You now know the stack the big data era was built on: HDFS's blocks, replicas and NameNode, YARN's containers and queues, MapReduce's map, sort, shuffle and reduce, the security trio of Kerberos, Ranger and Knox, the SQL and storage engines that grew on top, and how a Hadoop platform moves to Google Cloud. The next section, **Apache Spark**, takes the engine that replaced MapReduce: how it plans whole jobs as DAGs of stages, keeps data in memory, optimises DataFrame queries, and runs on YARN, Kubernetes or serverless batches.
