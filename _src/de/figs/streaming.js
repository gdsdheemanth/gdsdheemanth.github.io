/* Streaming systems: section diagrams */
Object.assign(FIGS, {

streaming_platform:{ t:"The two halves of a streaming platform", cap:"Producers append events to a durable log split into partitions, where every record keeps its offset and nothing is rewritten. Stream processors read the log continuously, each at its own offset, and write results to sinks. Underneath sit the two hard problems: time and correctness.", svg:S("streaming1", 310, "Producers append to a partitioned log; consumers and stream jobs read it at their own offsets and write to sinks; time and correctness are the hard problems",
  B(20, 70, 130, 110, "ka", ["Producers", "apps, services", "CDC, devices"]) +
  '<rect class="fill" x="180" y="20" width="300" height="200" rx="12"/>' + T(330, 40, "Half 1: the durable log", "b", "middle") +
  T(196, 76, "P0", "sm") + T(196, 121, "P1", "sm") + T(196, 166, "P2", "sm") +
  [0, 1, 2, 3, 4, 5, 6, 7].map(function(i){ return B(222 + i * 30, 58, 26, 26, "st", [String(i)], false); }).join("") +
  [0, 1, 2, 3, 4, 5].map(function(i){ return B(222 + i * 30, 103, 26, 26, "st", [String(i)], false); }).join("") +
  [0, 1, 2, 3, 4, 5, 6].map(function(i){ return B(222 + i * 30, 148, 26, 26, "st", [String(i)], false); }).join("") +
  T(330, 202, "append-only, ordered per partition, replayable", "sm", "middle") +
  A("streaming1", "M150 125 L178 125", "kac") +
  '<rect class="fill" x="520" y="20" width="220" height="200" rx="12"/>' + T(630, 40, "Half 2: stream processing", "b", "middle") +
  B(540, 54, 180, 46, "sp", ["Consumer group A", "reads at its own offset"]) +
  B(540, 110, 180, 46, "sp", ["Flink or Dataflow job", "transform, join, aggregate"]) +
  B(540, 166, 180, 42, "okf", ["Sinks", "BigQuery, OLAP, apps"]) +
  A("streaming1", "M480 77 L538 77") + A("streaming1", "M480 133 L538 133") + A("streaming1", "M630 156 L630 164") +
  B(20, 245, 350, 50, "pr", ["Hard problem 1: time", "when it happened vs when we saw it; late events"]) +
  B(390, 245, 350, 50, "pr", ["Hard problem 2: correctness", "count every event exactly once, even after a crash"])) },

streaming_backpressure:{ t:"Backpressure and the bottleneck", cap:"The window operator is the slowest stage, so it is busy all the time. Stages before it are backpressured (waiting for free output buffers) and the sink after it is idle. Between two tasks, the receiver grants credits for its free buffers, and the sender may send only that many, so pressure travels back to the source.", svg:S("streaming2", 290, "Four-stage pipeline with the bottleneck in the middle, pressure flowing back to the source, and credit-based flow control between a sender and a receiver",
  T(380, 22, "Rule: the bottleneck is the first task, going downstream, that is busy rather than backpressured", "sm", "middle") +
  B(20, 40, 150, 60, "pr", ["Kafka source", "backpressured"]) +
  B(210, 40, 150, 60, "pr", ["Parse + enrich", "backpressured"]) +
  B(400, 40, 150, 60, "ka", ["Window aggregate", "busy: the bottleneck"]) +
  B(590, 40, 150, 60, "box", ["Sink", "idle: waiting for input"]) +
  A("streaming2", "M170 70 L208 70") + A("streaming2", "M360 70 L398 70") + A("streaming2", "M550 70 L588 70") +
  A("streaming2", "M475 100 L475 124 L95 124 L95 102", "acc dash") +
  T(285, 142, "pressure propagates upstream: the source reads Kafka more slowly", "sm tA", "middle") +
  T(20, 186, "Credit-based flow control between two tasks (Flink)", "b") +
  B(20, 200, 200, 70, "sp", ["Sender task", "output buffers fill", "then its operator blocks"]) +
  B(540, 200, 200, 70, "sp", ["Receiver task", "a few network buffers", "one credit per free buffer"]) +
  A("streaming2", "M220 222 L538 222", "stc") + T(380, 214, "data buffers: never more than the credits", "sm", "middle") +
  A("streaming2", "M540 252 L222 252", "ln dash") + T(380, 244, "credits: how many buffers are free", "sm", "middle")) },

streaming_lambda_kappa:{ t:"Lambda vs Kappa", cap:"Lambda runs a batch layer and a speed layer side by side and merges their views at query time, so the same logic is written twice. Kappa keeps one stream job over a replayable log; to change the logic, a second version replays the log into a new table and readers switch once it has caught up.", svg:S("streaming3", 335, "Lambda with batch and speed layers merged in a serving layer; Kappa with one stream job and a second version replaying the log",
  T(20, 26, "Lambda: two code paths", "b") +
  B(20, 50, 120, 90, "ka", ["Raw events", "immutable", "master data"]) +
  B(180, 40, 170, 50, "sp", ["Batch layer", "full recompute (Spark)"]) +
  B(180, 100, 170, 50, "st", ["Speed layer", "incremental, recent only"]) +
  B(390, 40, 140, 50, "box", ["Batch views", "accurate, hours old"]) +
  B(390, 100, 140, 50, "box", ["Real-time views", "fresh, approximate"]) +
  B(570, 55, 170, 80, "okf", ["Serving layer", "merges both views", "at query time"]) +
  A("streaming3", "M140 80 L178 66") + A("streaming3", "M140 110 L178 124") +
  A("streaming3", "M350 65 L388 65") + A("streaming3", "M350 125 L388 125") +
  A("streaming3", "M530 65 L568 82") + A("streaming3", "M530 125 L568 108") +
  T(265, 170, "the same business logic, written twice in two frameworks", "sm tR", "middle") +
  T(20, 205, "Kappa: one code path, replay the log to reprocess", "b") +
  B(20, 225, 120, 90, "ka", ["Replayable log", "Kafka, long", "retention"]) +
  B(180, 220, 170, 45, "st", ["Stream job v1", "serving today"]) +
  B(180, 275, 170, 45, "sp", ["Stream job v2", "replays from offset 0"]) +
  B(390, 220, 140, 45, "box", ["Table v1"]) +
  B(390, 275, 140, 45, "box", ["Table v2", "catching up"]) +
  B(570, 235, 170, 70, "okf", ["Readers", "switch to v2 once", "it has caught up"]) +
  A("streaming3", "M140 255 L178 244") + A("streaming3", "M140 285 L178 296") +
  A("streaming3", "M350 242 L388 242") + A("streaming3", "M350 297 L388 297") +
  A("streaming3", "M530 242 L568 256") + A("streaming3", "M530 297 L568 284", "ln dash")) },

streaming_kafka:{ t:"Partitions, replicas and the high watermark", cap:"Each partition has one leader and two followers on different brokers, and producers write to the leader. Below, partition P0 on its three replicas: offsets that every in-sync replica holds are below the high watermark and readable; offsets 8 and 9 are not yet on every replica, so consumers cannot see them and an acks=all write is not yet acknowledged.", svg:S("streaming4", 320, "Three brokers each leading one partition, and the log of partition P0 on three replicas with the high watermark at offset 8",
  B(20, 20, 140, 50, "ka", ["Producer", "acks=all"]) +
  T(90, 92, "writes go to the leader;", "sm", "middle") + T(90, 108, "followers fetch from it", "sm", "middle") +
  '<rect class="fill" x="200" y="20" width="170" height="152" rx="12"/>' + T(285, 40, "Broker 0", "b", "middle") +
  B(215, 52, 140, 32, "ka", ["P0 leader"]) + B(215, 92, 140, 32, "box", ["P1 follower"], false) + B(215, 132, 140, 32, "box", ["P2 follower"], false) +
  '<rect class="fill" x="380" y="20" width="170" height="152" rx="12"/>' + T(465, 40, "Broker 1", "b", "middle") +
  B(395, 52, 140, 32, "box", ["P0 follower"], false) + B(395, 92, 140, 32, "ka", ["P1 leader"]) + B(395, 132, 140, 32, "box", ["P2 follower"], false) +
  '<rect class="fill" x="560" y="20" width="170" height="152" rx="12"/>' + T(645, 40, "Broker 2", "b", "middle") +
  B(575, 52, 140, 32, "box", ["P0 follower"], false) + B(575, 92, 140, 32, "box", ["P1 follower"], false) + B(575, 132, 140, 32, "ka", ["P2 leader"]) +
  A("streaming4", "M160 50 L213 64", "kac") +
  T(20, 194, "Inside partition P0: replicas in the ISR and the high watermark", "b") +
  T(20, 223, "Broker 0 (leader)", "sm") + T(20, 258, "Broker 1 (in ISR)", "sm") + T(20, 293, "Broker 2 (in ISR)", "sm") +
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map(function(i){ return B(130 + i * 34, 205, 30, 26, i < 8 ? "st" : "box", [String(i)], false); }).join("") +
  [0, 1, 2, 3, 4, 5, 6, 7, 8].map(function(i){ return B(130 + i * 34, 240, 30, 26, i < 8 ? "st" : "box", [String(i)], false); }).join("") +
  [0, 1, 2, 3, 4, 5, 6, 7].map(function(i){ return B(130 + i * 34, 275, 30, 26, "st", [String(i)], false); }).join("") +
  '<path class="acc dash" d="M400 198 L400 310"/>' +
  T(500, 222, "high watermark: offset 8", "b tA") +
  T(500, 242, "consumers read offsets 0 to 7 only", "sm") +
  T(500, 266, "8 and 9 are not on every ISR replica", "sm") +
  T(500, 282, "yet, so they are not committed", "sm")) },

streaming_pulsar:{ t:"Pulsar: serving and storage are separate", cap:"Stateless brokers own topics and serve producers and consumers, while Apache BookKeeper stores the data. Each ledger entry is written to several bookies in parallel and acknowledged once a quorum has it, so a topic's data is spread across many disks and a broker can hand over a topic without moving any data.", svg:S("streaming5", 310, "Producers and consumers talk to stateless brokers; a metadata store tracks ownership; BookKeeper bookies hold ledger entries striped with write quorum 3",
  B(20, 35, 130, 50, "ka", ["Producers"]) + B(20, 115, 130, 50, "okf", ["Consumers"]) +
  '<rect class="fill" x="180" y="20" width="380" height="100" rx="12"/>' + T(370, 38, "Serving layer: stateless brokers", "b", "middle") +
  B(195, 52, 110, 50, "sp", ["Broker 1", "owns topic A"]) +
  B(315, 52, 110, 50, "sp", ["Broker 2", "owns topic B"]) +
  B(435, 52, 110, 50, "sp", ["Broker 3", "takes over fast"]) +
  B(600, 35, 140, 70, "pr", ["Metadata store", "ZooKeeper, etcd", "or Oxia"]) +
  A("streaming5", "M150 60 L193 70") + A("streaming5", "M193 90 L152 135") +
  A("streaming5", "M560 70 L598 70", "ln dash") +
  A("streaming5", "M370 120 L370 148") + T(380, 138, "write entries in parallel", "sm") +
  '<rect class="fill" x="180" y="150" width="560" height="150" rx="12"/>' + T(460, 168, "Storage layer: Apache BookKeeper", "b", "middle") +
  B(200, 182, 120, 40, "st", ["Bookie 1"]) + B(335, 182, 120, 40, "st", ["Bookie 2"]) + B(470, 182, 120, 40, "st", ["Bookie 3"]) + B(605, 182, 120, 40, "st", ["Bookie 4"]) +
  T(260, 242, "e0 e2 e3", "mono", "middle") + T(395, 242, "e0 e1 e3", "mono", "middle") + T(530, 242, "e0 e1 e2", "mono", "middle") + T(665, 242, "e1 e2 e3", "mono", "middle") +
  T(460, 268, "ledger entries striped across bookies: write to 3, acknowledge after 2", "sm", "middle") +
  T(460, 286, "a new bookie adds capacity at once; old data is not moved", "sm", "middle")) },

});
