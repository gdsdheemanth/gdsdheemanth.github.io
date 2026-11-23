/* hadoop: The Hadoop ecosystem, original drawings */
Object.assign(FIGS, {

hadoop_stack:{ t:"The Hadoop stack", cap:"Storage at the bottom (HDFS, or object storage such as Cloud Storage and Ozone), YARN sharing the cluster's memory and CPU in the middle, processing engines on top, and SQL and serving tools above them. Impala and HBase run their own daemons directly on the storage layer rather than in YARN containers. The Hive Metastore and the security layer cut across every level.", svg:S("hadoop1", 310, "Layered diagram: tools, engines, YARN and storage, with the Hive Metastore and security as side columns",
  T(20, 14, "SQL, serving and gateways", "sm") +
  B(20, 20, 135, 50, "pr", ["Hive", "SQL to DAGs"]) + B(165, 20, 135, 50, "pr", ["Impala", "MPP SQL"]) +
  B(310, 20, 135, 50, "pr", ["HBase", "wide-column store"]) + B(455, 20, 135, 50, "pr", ["Kylin, Kyuubi", "cubes, SQL gateway"]) +
  T(20, 89, "Processing engines", "sm") +
  B(20, 95, 185, 50, "sp", ["MapReduce", "map, sort, reduce"]) + B(215, 95, 185, 50, "sp", ["Tez", "DAGs for Hive"]) + B(410, 95, 180, 50, "sp", ["Spark", "in-memory DAGs"]) +
  T(20, 164, "Resource management", "sm") +
  B(20, 170, 570, 50, "ka", ["YARN", "ResourceManager, NodeManagers, containers and queues"]) +
  T(20, 239, "Storage", "sm") +
  B(20, 245, 340, 50, "st", ["HDFS", "NameNode plus DataNodes, replicated blocks"]) +
  B(370, 245, 220, 50, "st", ["Object storage", "Cloud Storage, Ozone"]) +
  B(610, 20, 130, 125, "box", ["Hive Metastore", "table catalog for", "Hive, Spark,", "Impala, Trino"]) +
  B(610, 170, 130, 125, "okf", ["Security", "Kerberos", "Ranger", "Knox"]) +
  A("hadoop1", "M300 168 L300 147")) },

hadoop_hdfs:{ t:"HDFS: NameNode HA and the write pipeline", cap:"The client asks the active NameNode for target DataNodes, then streams packets through a pipeline: replica 1 on its own rack, replicas 2 and 3 on two nodes of another rack. The active NameNode writes every namespace change to a quorum of JournalNodes, which the standby replays so it can take over. DataNodes send heartbeats and block reports to both NameNodes.", svg:S("hadoop2", 330, "Client, active and standby NameNodes with JournalNodes, and a three-DataNode write pipeline across two racks",
  B(190, 20, 170, 56, "ka", ["Active NameNode", "namespace in memory"]) +
  B(390, 20, 150, 56, "box", ["JournalNodes", "quorum of 3 or 5"]) +
  B(570, 20, 170, 56, "fill", ["Standby NameNode", "replays the edits"]) +
  A("hadoop2", "M360 48 L388 48") + A("hadoop2", "M540 48 L568 48") +
  B(20, 150, 120, 56, "pr", ["Client", "writes a file"]) +
  A("hadoop2", "M100 150 L205 78", "acc") + T(28, 120, "1. ask for targets", "sm tA") +
  '<rect class="fill" x="170" y="110" width="190" height="190" rx="8"/>' + T(180, 128, "Rack 1", "b") +
  B(185, 145, 160, 50, "sp", ["DataNode 1", "replica 1"]) + B(185, 225, 160, 50, "box", ["DataNode 4"]) +
  '<rect class="fill" x="400" y="110" width="340" height="190" rx="8"/>' + T(410, 128, "Rack 2", "b") +
  B(415, 145, 150, 50, "sp", ["DataNode 2", "replica 2"]) + B(590, 145, 140, 50, "sp", ["DataNode 3", "replica 3"]) +
  B(415, 225, 150, 50, "box", ["DataNode 5"]) + B(590, 225, 140, 50, "box", ["DataNode 6"]) +
  A("hadoop2", "M140 178 L183 178", "acc") + A("hadoop2", "M345 170 L413 170", "acc") + A("hadoop2", "M565 170 L588 170", "acc") +
  T(379, 162, "2. packets", "sm tA", "middle") +
  A("hadoop2", "M300 145 L300 78", "ln dash") + T(306, 100, "heartbeats, block reports", "sm") +
  T(20, 322, "Acks flow back DataNode 3 to 2 to 1 to the client. The NameNode hands out locations and never sees file data.", "sm")) },

hadoop_yarn:{ t:"YARN: who decides what", cap:"The client submits an application to the ResourceManager, which starts the application's own ApplicationMaster in a container. The ApplicationMaster then asks the ResourceManager for more containers and has the NodeManagers launch its tasks in them. The scheduler only decides who gets resources; the ApplicationMaster decides what runs.", svg:S("hadoop3", 300, "Client, ResourceManager and two NodeManagers with an ApplicationMaster and task containers",
  B(20, 30, 120, 50, "pr", ["Client", "spark-submit"]) +
  B(200, 20, 220, 70, "ka", ["ResourceManager", "Scheduler (queues)", "ApplicationsManager"]) +
  B(450, 20, 290, 70, "box", ["The scheduler decides who gets", "containers; the ApplicationMaster", "decides what runs in them"], false) +
  A("hadoop3", "M140 55 L198 55") + T(169, 47, "1 submit", "sm", "middle") +
  '<rect class="fill" x="20" y="140" width="350" height="145" rx="8"/>' + T(30, 158, "Node 1: NodeManager", "b") +
  B(35, 170, 150, 50, "sp", ["ApplicationMaster", "plans the tasks"]) + B(200, 170, 155, 50, "okf", ["Container", "task or executor"]) +
  '<rect class="fill" x="390" y="140" width="350" height="145" rx="8"/>' + T(400, 158, "Node 2: NodeManager", "b") +
  B(405, 170, 155, 50, "okf", ["Container", "task or executor"]) + B(575, 170, 150, 50, "okf", ["Container", "task or executor"]) +
  A("hadoop3", "M230 90 L120 168", "kac") + T(100, 118, "2 start AM", "sm tK") +
  A("hadoop3", "M165 168 L300 92", "acc") + T(250, 128, "3 ask for containers", "sm tA") +
  A("hadoop3", "M110 220 L110 245 L480 245 L480 222", "acc") + A("hadoop3", "M185 195 L198 195", "acc") +
  T(295, 270, "4 NodeManagers launch the tasks", "sm tA", "middle")) },

hadoop_mr:{ t:"MapReduce: map, sort, shuffle, reduce", cap:"One map task per input split. Each map task partitions its output by key, sorts it and spills it to local disk, one section per reducer. In the shuffle every reducer fetches its section from every map task, merge-sorts the runs and calls reduce once per key, writing one output file to HDFS.", svg:S("hadoop4", 300, "Three input splits feed three map tasks whose sorted, partitioned output is shuffled to two reducers",
  T(75, 26, "Input splits", "b", "middle") + T(225, 26, "Map", "b", "middle") + T(388, 26, "Partition, sort, spill", "b", "middle") +
  T(502, 26, "Shuffle", "b", "middle") + T(585, 26, "Reduce", "b", "middle") + T(707, 26, "Output", "b", "middle") +
  B(20, 40, 110, 56, "box", ["Split 0", "HDFS block"]) + B(20, 120, 110, 56, "box", ["Split 1", "HDFS block"]) + B(20, 200, 110, 56, "box", ["Split 2", "HDFS block"]) +
  B(160, 40, 130, 56, "sp", ["Map 0", "map, combine"]) + B(160, 120, 130, 56, "sp", ["Map 1", "map, combine"]) + B(160, 200, 130, 56, "sp", ["Map 2", "map, combine"]) +
  B(320, 40, 150, 56, "fill", ["Sorted file", "part 0 | part 1"]) + B(320, 120, 150, 56, "fill", ["Sorted file", "part 0 | part 1"]) + B(320, 200, 150, 56, "fill", ["Sorted file", "part 0 | part 1"]) +
  A("hadoop4", "M130 68 L158 68") + A("hadoop4", "M130 148 L158 148") + A("hadoop4", "M130 228 L158 228") +
  A("hadoop4", "M290 68 L318 68") + A("hadoop4", "M290 148 L318 148") + A("hadoop4", "M290 228 L318 228") +
  B(530, 70, 110, 56, "ka", ["Reduce 0", "merge, reduce"]) + B(530, 170, 110, 56, "ka", ["Reduce 1", "merge, reduce"]) +
  A("hadoop4", "M470 60 L528 90", "stc") + A("hadoop4", "M470 76 L528 186", "stc") +
  A("hadoop4", "M470 140 L528 100", "stc") + A("hadoop4", "M470 156 L528 194", "stc") +
  A("hadoop4", "M470 220 L528 110", "stc") + A("hadoop4", "M470 236 L528 204", "stc") +
  B(670, 70, 75, 56, "okf", ["part-0", "on HDFS"]) + B(670, 170, 75, 56, "okf", ["part-1", "on HDFS"]) +
  A("hadoop4", "M640 98 L668 98") + A("hadoop4", "M640 198 L668 198") +
  T(20, 288, "Every key goes to exactly one reducer, so each reducer can produce final results on its own.", "sm")) },

hadoop_hbase:{ t:"HBase: write and read paths in a RegionServer", cap:"A write is appended to the write-ahead log on HDFS and inserted into the sorted in-memory MemStore; a full MemStore is flushed as a new immutable HFile. Reads merge the MemStore and the HFiles, using the block cache and Bloom filters to avoid disk reads, and compactions merge HFiles and drop deleted cells. ZooKeeper tells clients where the hbase:meta table lives.", svg:S("hadoop5", 310, "Client, ZooKeeper and HMaster beside a RegionServer with WAL, MemStore, block cache and HFiles, and compaction",
  B(20, 30, 120, 50, "pr", ["Client", "put and get"]) +
  B(20, 120, 120, 50, "box", ["ZooKeeper", "meta location"]) +
  B(20, 200, 120, 50, "box", ["HMaster", "assigns regions"]) +
  A("hadoop5", "M80 80 L80 118", "ln dash") +
  '<rect class="fill" x="170" y="20" width="380" height="265" rx="8"/>' + T(180, 40, "RegionServer, one region: keys k100 to k199", "b") +
  B(185, 55, 160, 50, "ka", ["1. WAL append", "sequential, on HDFS"]) +
  B(185, 130, 160, 50, "sp", ["2. MemStore", "sorted, in memory"]) +
  B(370, 55, 165, 50, "okf", ["Block cache", "hot HFile blocks"]) +
  B(370, 130, 165, 50, "box", ["Read path", "MemStore, cache, HFiles"]) +
  A("hadoop5", "M140 60 L183 76", "acc") + A("hadoop5", "M265 105 L265 128", "acc") +
  B(185, 220, 100, 50, "st", ["HFile", "newest"]) + B(295, 220, 100, 50, "st", ["HFile"]) + B(405, 220, 100, 50, "st", ["HFile", "oldest"]) +
  A("hadoop5", "M235 180 L235 218", "acc") + T(245, 204, "3. flush when full", "sm tA") +
  B(580, 40, 160, 70, "box", ["Regions", "sorted key ranges,", "many per server"]) +
  B(580, 130, 160, 70, "box", ["Bloom filters", "skip HFiles that", "lack the row"]) +
  B(580, 220, 160, 50, "st", ["Compaction", "merges HFiles"]) +
  A("hadoop5", "M505 245 L578 245", "stc") +
  T(20, 302, "Writes cost one log append and one memory insert. Reads merge the MemStore with the HFiles, newest first.", "sm")) },

});
