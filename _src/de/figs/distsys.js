/* distsys: Distributed systems, original drawings */
Object.assign(FIGS, {

distsys_cap:{ t:"A network partition forces a choice", cap:"Three replicas are split by a broken link: R1 and R2 can still talk, R3 is cut off. A CP system lets only the majority side accept writes and returns errors on the minority side. An AP system keeps both sides writable, so the replicas disagree until the partition heals and they reconcile.", svg:S("distsys1", 310, "Two replicas on one side of a partition and one on the other, with the CP and AP outcomes below",
  B(20, 55, 110, 50, "ka", ["Client 1"]) +
  B(180, 25, 130, 50, "sp", ["Replica R1"]) + B(180, 100, 130, 50, "sp", ["Replica R2"]) +
  A("distsys1", "M130 72 L178 52") + A("distsys1", "M130 86 L178 118") +
  '<path class="ln" d="M245 75 L245 100"/>' +
  '<path class="bad dash" d="M380 15 L380 165"/>' + T(380, 182, "partition: messages lost", "sm tR", "middle") +
  '<path class="ln dash" d="M310 90 L450 90"/>' + T(380, 84, "X", "b tR", "middle") +
  B(450, 65, 130, 50, "sp", ["Replica R3"]) + B(630, 65, 110, 50, "ka", ["Client 2"]) +
  A("distsys1", "M630 90 L582 90") +
  B(20, 200, 350, 95, "okf", ["CP: Spanner, etcd, Kafka acks=all", "R1 + R2 are a majority and keep writing", "R3 rejects writes until the link heals"]) +
  B(390, 200, 350, 95, "pr", ["AP: Cassandra at consistency ONE", "both sides accept writes", "replicas diverge, reconcile afterwards"])) },

distsys_ring:{ t:"A consistent hash ring with virtual nodes", cap:"Keys and nodes are hashed onto the same circle. Each physical node (A to D) owns several points; a key belongs to the first point clockwise from it, so key k belongs to node C. Adding node E inserts new points, and only the keys just before those points move to E.", svg:S("distsys2", 300, "A hash ring with two points per node, a key mapped clockwise to node C and a new node E inserted",
  '<circle class="fill" cx="190" cy="150" r="105"/>' +
  '<circle class="sp" cx="226" cy="51" r="9"/>' + T(233, 33, "A1", "sm", "middle") +
  '<circle class="st" cx="289" cy="114" r="9"/>' + T(311, 111, "B1", "sm", "middle") +
  '<circle class="ka" cx="285" cy="194" r="9"/>' + T(309, 210, "C1", "sm", "middle") +
  '<circle class="pr" cx="226" cy="249" r="9"/>' + T(233, 276, "D1", "sm", "middle") +
  '<circle class="sp" cx="146" cy="245" r="9"/>' + T(136, 272, "A2", "sm", "middle") +
  '<circle class="st" cx="91" cy="186" r="9"/>' + T(69, 198, "B2", "sm", "middle") +
  '<circle class="ka" cx="91" cy="114" r="9"/>' + T(69, 111, "C2", "sm", "middle") +
  '<circle class="pr" cx="146" cy="55" r="9"/>' + T(136, 37, "D2", "sm", "middle") +
  '<circle class="okf" cx="257" cy="230" r="9"/>' + T(272, 254, "E1 (new)", "sm tA", "start") +
  '<rect class="box" x="290" y="140" width="12" height="12" rx="2"/>' + T(312, 150, "key k", "b", "start") +
  A("distsys2", "M296 156 A105 105 0 0 1 290 182", "acc") +
  T(190, 154, "hash space", "sm", "middle") +
  B(420, 30, 320, 60, "box", ["Rule", "a key goes to the next point clockwise"]) +
  B(420, 105, 320, 60, "okf", ["Node E joins", "only keys between C1 and E1 move (to E)"]) +
  B(420, 180, 320, 90, "pr", ["10,000 keys, 4 to 5 nodes", "modulo hash: 7,976 keys move", "ring with virtual nodes: 2,029 move"])) },

distsys_vclock:{ t:"Vector clocks detect concurrent events", cap:"Each process keeps one counter per process. Sending a message carries the sender's vector; the receiver takes the element-wise maximum and adds one to its own entry. A at [3,0,0] and C at [0,0,1] are concurrent (neither is less than or equal to the other in every entry), while C's last event [2,2,2] happened after everything A did before its message.", svg:S("distsys3", 290, "Three process timelines A, B and C with events labelled by vector clocks and messages between them",
  T(20, 64, "A", "b") + T(20, 144, "B", "b") + T(20, 224, "C", "b") +
  '<path class="ln" d="M45 60 L740 60"/><path class="ln" d="M45 140 L740 140"/><path class="ln" d="M45 220 L740 220"/>' +
  '<circle class="sp" cx="110" cy="60" r="7"/>' + T(110, 42, "[1,0,0]", "mono", "middle") +
  '<circle class="sp" cx="210" cy="60" r="7"/>' + T(210, 42, "[2,0,0] send", "mono", "middle") +
  '<circle class="sp" cx="560" cy="60" r="7"/>' + T(560, 42, "[3,0,0]", "mono tR", "middle") +
  A("distsys3", "M215 66 L312 133", "acc") +
  '<circle class="st" cx="320" cy="140" r="7"/>' + T(320, 165, "[2,1,0] receive", "mono", "middle") +
  '<circle class="st" cx="440" cy="140" r="7"/>' + T(440, 122, "[2,2,0] send", "mono", "middle") +
  A("distsys3", "M445 146 L582 214", "stc") +
  '<circle class="ka" cx="150" cy="220" r="7"/>' + T(150, 245, "[0,0,1]", "mono tR", "middle") +
  '<circle class="ka" cx="590" cy="220" r="7"/>' + T(590, 245, "[2,2,2] receive", "mono", "middle") +
  T(380, 280, "[3,0,0] and [0,0,1]: concurrent, neither could have influenced the other", "sm tR", "middle")) },

distsys_raft:{ t:"Raft log replication and the commit index", cap:"Each row is one node's log; the number in each entry is the term in which the leader created it. The leader (term 3) has entries 1 to 7. Entries 1 to 5 are stored on three of the five nodes, a majority, so they are committed. Entries 6 and 7 are only on two nodes and are not yet committed.", svg:S("distsys4", 280, "Five Raft logs with entries one to seven, and a commit line after entry five",
  T(230, 24, "1", "sm", "middle") + T(278, 24, "2", "sm", "middle") + T(326, 24, "3", "sm", "middle") + T(374, 24, "4", "sm", "middle") + T(422, 24, "5", "sm", "middle") + T(470, 24, "6", "sm", "middle") + T(518, 24, "7", "sm", "middle") + T(150, 24, "log index", "sm", "end") +
  T(20, 54, "Leader", "b") + T(20, 94, "Follower 1", "b") + T(20, 134, "Follower 2", "b") + T(20, 174, "Follower 3", "b") + T(20, 214, "Follower 4", "b") +
  B(210, 35, 40, 28, "okf", ["t1"], false) + B(258, 35, 40, 28, "okf", ["t1"], false) + B(306, 35, 40, 28, "okf", ["t2"], false) + B(354, 35, 40, 28, "okf", ["t2"], false) + B(402, 35, 40, 28, "okf", ["t3"], false) + B(450, 35, 40, 28, "sp", ["t3"], false) + B(498, 35, 40, 28, "sp", ["t3"], false) +
  B(210, 75, 40, 28, "okf", ["t1"], false) + B(258, 75, 40, 28, "okf", ["t1"], false) + B(306, 75, 40, 28, "okf", ["t2"], false) + B(354, 75, 40, 28, "okf", ["t2"], false) + B(402, 75, 40, 28, "okf", ["t3"], false) + B(450, 75, 40, 28, "sp", ["t3"], false) + B(498, 75, 40, 28, "sp", ["t3"], false) +
  B(210, 115, 40, 28, "okf", ["t1"], false) + B(258, 115, 40, 28, "okf", ["t1"], false) + B(306, 115, 40, 28, "okf", ["t2"], false) + B(354, 115, 40, 28, "okf", ["t2"], false) + B(402, 115, 40, 28, "okf", ["t3"], false) +
  B(210, 155, 40, 28, "box", ["t1"], false) + B(258, 155, 40, 28, "box", ["t1"], false) + B(306, 155, 40, 28, "box", ["t2"], false) +
  B(210, 195, 40, 28, "box", ["t1"], false) + B(258, 195, 40, 28, "box", ["t1"], false) +
  '<path class="acc dash" d="M446 30 L446 232"/>' + T(446, 252, "commit index = 5", "b tA", "middle") +
  T(570, 54, "AppendEntries to all", "sm", "start") + T(570, 140, "3 of 5 hold entries 1-5:", "sm", "start") + T(570, 156, "a majority, so committed", "sm", "start") +
  T(380, 272, "Followers 3 and 4 lag behind; the leader keeps sending them the missing entries.", "sm", "middle")) },

distsys_fencing:{ t:"Fencing tokens stop a paused leader", cap:"Worker 1 gets a lease with token 33, then pauses (for example a long garbage collection). Its lease expires and worker 2 gets token 34 and writes. When worker 1 wakes up and writes with token 33, the storage has already seen 34 and rejects the stale write.", svg:S("distsys5", 300, "Timeline of a lock service, two workers and storage, showing a stale write rejected by its fencing token",
  T(20, 50, "Lock service", "b") + T(20, 120, "Worker 1", "b") + T(20, 180, "Worker 2", "b") + T(20, 250, "Storage", "b") +
  '<path class="ln" d="M130 45 L740 45"/><path class="ln" d="M130 115 L740 115"/><path class="ln" d="M130 175 L740 175"/><path class="ln" d="M130 245 L740 245"/>' +
  B(140, 25, 120, 40, "sp", ["grant token 33"], false) + B(340, 25, 110, 40, "box", ["lease expires"], false) + B(460, 25, 130, 40, "sp", ["grant token 34"], false) +
  A("distsys5", "M200 65 L200 103") +
  '<rect class="pr dash" x="215" y="100" width="420" height="30" rx="6"/>' + T(425, 120, "process paused (GC, VM stall): still thinks it is leader", "sm", "middle") +
  A("distsys5", "M525 65 L525 163") +
  B(540, 160, 100, 32, "okf", ["write(34)"], false) + A("distsys5", "M590 192 L590 228") +
  B(650, 100, 90, 32, "ka", ["write(33)"], false) + A("distsys5", "M695 132 C700 180 700 200 700 228", "kac") +
  B(520, 230, 120, 32, "okf", ["accept: max=34"], false) + B(650, 230, 90, 50, "box", ["reject", "33 < 34"]) +
  T(140, 290, "The storage, not the worker, checks the token: tokens only ever increase.", "sm tA")) },

});
