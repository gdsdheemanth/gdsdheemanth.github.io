/* hardware: Storage internals and hardware, original drawings */
Object.assign(FIGS, {

hardware_ladder:{ t:"The latency ladder", cap:"Approximate time to fetch data from each layer, on a logarithmic scale: each dashed grid line is 1,000 times slower than the one to its left. Caches answer in nanoseconds, main memory in about a hundred, SSDs in tens of microseconds, disks and networks in milliseconds. Data systems are designed to keep hot data on the left and move data in big sequential chunks when it must come from the right.", svg:S("hardware1", 330, "Horizontal bars on a log scale showing latency of L1, L2, L3 cache, DRAM, NVMe SSD, network disk, hard disk and cross-region network",
  '<path class="ln dash" d="M200 30 L200 284"/><path class="ln dash" d="M356 30 L356 284"/><path class="ln dash" d="M512 30 L512 284"/><path class="ln dash" d="M668 30 L668 284"/>' +
  T(200, 300, "0.1 ns", "sm", "middle") + T(356, 300, "100 ns", "sm", "middle") + T(512, 300, "100 µs", "sm", "middle") + T(668, 300, "100 ms", "sm", "middle") +
  T(190, 54, "L1 cache", "", "end") + '<rect class="sp" x="200" y="40" width="52" height="20" rx="4"/>' + T(260, 54, "~1 ns", "sm") +
  T(190, 84, "L2 cache", "", "end") + '<rect class="sp" x="200" y="70" width="83" height="20" rx="4"/>' + T(291, 84, "~4 ns", "sm") +
  T(190, 114, "L3 cache", "", "end") + '<rect class="sp" x="200" y="100" width="113" height="20" rx="4"/>' + T(321, 114, "~15 ns", "sm") +
  T(190, 144, "Main memory (DRAM)", "", "end") + '<rect class="pr" x="200" y="130" width="156" height="20" rx="4"/>' + T(364, 144, "~100 ns", "sm") +
  T(190, 174, "NVMe SSD read", "", "end") + '<rect class="ka" x="200" y="160" width="276" height="20" rx="4"/>' + T(484, 174, "~20 µs", "sm") +
  T(190, 204, "Network SSD disk", "", "end") + '<rect class="ka" x="200" y="190" width="348" height="20" rx="4"/>' + T(556, 204, "~0.5 ms", "sm") +
  T(190, 234, "HDD random read", "", "end") + '<rect class="ka" x="200" y="220" width="400" height="20" rx="4"/>' + T(608, 234, "~5 ms", "sm") +
  T(190, 264, "Cross-region round trip", "", "end") + '<rect class="st" x="200" y="250" width="452" height="20" rx="4"/>' + T(660, 264, "~50 ms", "sm") +
  T(20, 322, "Log scale: each dashed line is 1,000 times slower than the one before.", "sm")) },

hardware_ftl:{ t:"Inside an SSD: remapping and garbage collection", cap:"Flash pages cannot be overwritten in place. When the host updates a logical block, the flash translation layer writes the new data to a free page, points the logical address at it and marks the old page stale. Later, garbage collection copies the still-valid pages out of a mostly stale erase block and erases the whole block. Those extra copies are write amplification.", svg:S("hardware2", 300, "Host write remapped by the FTL to a new page; garbage collection copies valid pages from block A to block C and erases block A",
  B(20, 30, 150, 54, "box", ["Host", "update LBA 7"]) +
  B(20, 120, 150, 70, "sp", ["FTL mapping", "LBA 7: A3 → B2", "LBA 9: A1 (valid)"]) +
  A("hardware2", "M95 84 L95 118") +
  T(220, 28, "Erase block A", "b") +
  '<rect class="okf" x="220" y="40" width="44" height="34" rx="4"/><rect class="okf" x="270" y="40" width="44" height="34" rx="4"/><rect class="fill" x="320" y="40" width="44" height="34" rx="4"/><rect class="fill" x="370" y="40" width="44" height="34" rx="4"/>' +
  T(242, 62, "A1", "sm", "middle") + T(292, 62, "A2", "sm", "middle") + T(342, 62, "A3", "sm", "middle") + T(392, 62, "A4", "sm", "middle") +
  T(220, 92, "green = valid, grey = stale", "sm") +
  T(220, 128, "Erase block B", "b") +
  '<rect class="okf" x="220" y="140" width="44" height="34" rx="4"/><rect class="sp" x="270" y="140" width="44" height="34" rx="4"/><rect class="box" x="320" y="140" width="44" height="34" rx="4"/><rect class="box" x="370" y="140" width="44" height="34" rx="4"/>' +
  T(242, 162, "B1", "sm", "middle") + T(292, 162, "B2 new", "sm", "middle") + T(342, 162, "free", "sm", "middle") + T(392, 162, "free", "sm", "middle") +
  A("hardware2", "M170 186 L292 186 L292 177", "acc") +
  T(470, 28, "Garbage collection", "b") +
  B(470, 40, 270, 56, "ka", ["Copy valid A1, A2 to block C", "2 extra page writes"]) +
  B(470, 120, 270, 56, "pr", ["Erase block A", "whole block, slow, wears cells"]) +
  A("hardware2", "M414 57 L468 64", "kac") + A("hardware2", "M605 96 L605 118") +
  B(20, 220, 720, 56, "fill", ["Write amplification = flash writes / host writes", "worse when the drive is nearly full or writes are small and random; TRIM and spare area help"])) },

hardware_zerocopy:{ t:"read plus write versus sendfile", cap:"Serving a file the naive way copies the bytes from the page cache into the application and back into a socket buffer: two CPU copies and four switches between user and kernel mode. sendfile keeps the data in the kernel, and with scatter-gather DMA the network card reads straight from the page cache. Kafka serves consumers this way.", svg:S("hardware3", 300, "Two paths from disk to network card: one through an application buffer, one directly from the page cache",
  T(20, 26, "read() then write()", "b") +
  '<rect class="fill" x="20" y="36" width="720" height="44" rx="8"/>' + T(30, 52, "user space", "sm") +
  B(20, 92, 110, 44, "ka", ["Disk"]) + B(170, 92, 130, 44, "sp", ["Page cache"]) + B(330, 36, 140, 44, "pr", ["App buffer"]) + B(500, 92, 130, 44, "sp", ["Socket buffer"]) + B(650, 92, 90, 44, "st", ["NIC"]) +
  A("hardware3", "M130 114 L168 114") + T(149, 150, "DMA", "sm", "middle") +
  A("hardware3", "M300 104 L340 82", "kac") + T(250, 66, "CPU copy", "sm tR", "middle") +
  A("hardware3", "M470 82 L510 102", "kac") + T(520, 66, "CPU copy", "sm tR", "middle") +
  A("hardware3", "M630 114 L648 114") + T(640, 150, "DMA", "sm", "middle") +
  T(20, 186, "sendfile() (zero-copy)", "b") +
  B(20, 200, 110, 44, "ka", ["Disk"]) + B(170, 200, 130, 44, "sp", ["Page cache"]) + B(650, 200, 90, 44, "st", ["NIC"]) +
  A("hardware3", "M130 222 L168 222") + A("hardware3", "M300 222 L648 222", "stc") +
  T(474, 214, "DMA straight from the page cache", "sm tS", "middle") +
  T(20, 280, "No CPU copies and no trip through user space. TLS breaks it: bytes must be encrypted in the application first.", "sm")) },

hardware_uring:{ t:"io_uring: two rings shared with the kernel", cap:"The application writes submission queue entries (SQEs) into the submission ring and makes one io_uring_enter call for the whole batch, or none in SQPOLL mode. The kernel performs the I/O and places completion queue entries (CQEs), carrying the same user_data tag, in the completion ring, where the application reads them without a system call.", svg:S("hardware4", 290, "Application writing SQEs into a submission ring, kernel processing them against a device and writing CQEs into a completion ring",
  B(20, 90, 150, 100, "sp", ["Application", "user space", "writes SQEs,", "reads CQEs"]) +
  '<rect class="fill" x="220" y="20" width="320" height="250" rx="12"/>' + T(380, 40, "shared memory (mmap)", "sm", "middle") +
  T(240, 70, "Submission queue (SQ)", "b") +
  '<rect class="ka" x="240" y="80" width="56" height="34" rx="4"/><rect class="ka" x="300" y="80" width="56" height="34" rx="4"/><rect class="ka" x="360" y="80" width="56" height="34" rx="4"/><rect class="box" x="420" y="80" width="56" height="34" rx="4"/><rect class="box" x="480" y="80" width="40" height="34" rx="4"/>' +
  T(268, 101, "read", "sm", "middle") + T(328, 101, "read", "sm", "middle") + T(388, 101, "fsync", "sm", "middle") +
  T(240, 180, "Completion queue (CQ)", "b") +
  '<rect class="okf" x="240" y="190" width="56" height="34" rx="4"/><rect class="okf" x="300" y="190" width="56" height="34" rx="4"/><rect class="box" x="360" y="190" width="56" height="34" rx="4"/><rect class="box" x="420" y="190" width="56" height="34" rx="4"/><rect class="box" x="480" y="190" width="40" height="34" rx="4"/>' +
  T(268, 211, "tag 7", "sm", "middle") + T(328, 211, "tag 8", "sm", "middle") +
  B(590, 90, 150, 100, "pr", ["Kernel", "runs each SQE", "against files,", "sockets, NVMe"]) +
  A("hardware4", "M170 110 L238 97", "acc") + T(200, 92, "tail", "sm tA", "middle") +
  A("hardware4", "M522 97 L588 120", "kac") + T(560, 96, "head", "sm tK", "middle") +
  A("hardware4", "M588 165 L522 207", "stc") + T(560, 205, "tail", "sm tS", "middle") +
  A("hardware4", "M238 207 L170 170", "stc") + T(200, 205, "head", "sm tS", "middle") +
  T(380, 254, "one io_uring_enter submits a whole batch", "sm", "middle")) },

hardware_envelope:{ t:"Envelope encryption with Cloud KMS", cap:"Each object is encrypted locally with its own random data encryption key (DEK). Only the small DEK is sent to Cloud KMS, which wraps it with a key encryption key (KEK) that never leaves the service. The wrapped DEK is stored with the ciphertext. To read, the service asks KMS to unwrap the DEK, an IAM-checked and audit-logged call, and decrypts locally.", svg:S("hardware5", 316, "Plaintext encrypted with a data key; data key wrapped by a key in Cloud KMS; ciphertext and wrapped key stored together",
  B(20, 40, 150, 54, "box", ["Plaintext data", "rows, files, pages"]) +
  B(20, 160, 150, 54, "sp", ["New random DEK", "AES-256 key"]) +
  B(240, 90, 170, 64, "pr", ["Encrypt locally", "AES-256-GCM"]) +
  A("hardware5", "M170 67 L238 108") + A("hardware5", "M170 187 L238 138") +
  B(240, 210, 170, 64, "ka", ["Cloud KMS", "wrap DEK with KEK", "(KEK never leaves)"]) +
  A("hardware5", "M170 200 L238 236", "kac") +
  '<rect class="fill" x="480" y="60" width="260" height="190" rx="12"/>' + T(610, 82, "Storage", "b", "middle") +
  B(500, 100, 220, 50, "okf", ["Ciphertext + auth tag"]) +
  B(500, 170, 220, 50, "okf", ["Wrapped DEK (metadata)"]) +
  A("hardware5", "M410 122 L498 124") + A("hardware5", "M410 242 L498 198", "kac") +
  T(610, 268, "read: unwrap DEK via KMS, then decrypt", "sm", "middle") +
  T(20, 304, "Rotation re-wraps small DEKs, not terabytes of data.", "sm")) },

});
