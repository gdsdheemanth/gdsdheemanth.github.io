/* aiml: Data engineering for AI and ML, original drawings */
Object.assign(FIGS, {

aiml_flink:{ t:"A streaming feature pipeline with a dual write", cap:"Flink keys events by card and keeps windowed counts in state. Every update is upserted into the online store for millisecond reads and also appended to a log that becomes the offline history for training, so both paths come from the same computation. Feature age is measured where the model reads.", svg:S("aiml1", 300, "Events flow from Kafka or Pub/Sub into Flink, which writes to an online store and an offline log; the model reads the online store",
  B(10, 110, 140, 70, "ka", ["Events", "Kafka / Pub/Sub", "keyed by card"]) +
  B(200, 95, 170, 100, "sp", ["Flink job", "event-time windows", "keyed state, TTL", "checkpoints to GCS"]) +
  B(430, 30, 150, 70, "st", ["Online store", "Bigtable / Redis", "upsert by key"]) +
  B(430, 190, 150, 70, "st", ["Offline log", "topic to BigQuery", "append history"]) +
  B(620, 30, 130, 70, "pr", ["Model service", "reads in ms", "logs feature age"]) +
  B(620, 190, 130, 70, "okf", ["Training sets", "point-in-time", "joins"]) +
  A("aiml1", "M150 145 L198 145") + A("aiml1", "M370 125 L428 70") + A("aiml1", "M370 165 L428 222") +
  A("aiml1", "M580 65 L618 65") + A("aiml1", "M580 225 L618 225") +
  T(395, 140, "same values", "sm", "middle") + T(380, 290, "backfill: replay the log or run the same SQL in batch mode", "sm", "middle")) },

aiml_pretrain:{ t:"An LLM pre-training data pipeline", cap:"Cheap filters run first so that expensive steps see fewer documents. Each stage keeps its decisions as metadata, and the surviving text is tokenised, packed and mixed by source weight into training shards.", svg:S("aiml2", 270, "Seven stages from raw crawl to packed token shards, with document counts shrinking",
  B(8, 60, 100, 80, "sp", ["Acquire", "WARC to text", "extraction"]) +
  B(116, 60, 100, 80, "sp", ["Language", "ID and URL", "blocklists"]) +
  B(224, 60, 100, 80, "fill", ["Heuristic", "filters", "length, symbols"]) +
  B(332, 60, 100, 80, "fill", ["Dedup", "exact hash", "MinHash LSH"]) +
  B(440, 60, 100, 80, "fill", ["Quality", "classifier", "score, keep top"]) +
  B(548, 60, 100, 80, "pr", ["Safety", "PII, toxicity", "decontaminate"]) +
  B(656, 60, 96, 80, "okf", ["Tokenise", "pack, mix", "write shards"]) +
  A("aiml2", "M108 100 L114 100") + A("aiml2", "M216 100 L222 100") + A("aiml2", "M324 100 L330 100") +
  A("aiml2", "M432 100 L438 100") + A("aiml2", "M540 100 L546 100") + A("aiml2", "M648 100 L654 100") +
  '<path class="ln dash" d="M20 180 L740 180"/>' +
  T(20, 205, "petabytes of raw crawl", "sm") + T(740, 205, "terabytes of clean tokens", "sm", "end") +
  T(380, 240, "cheap per-document steps first, expensive ones last", "sm", "middle") + T(380, 40, "each stage records its decision per document", "sm", "middle")) },

aiml_ann:{ t:"Three ways to avoid scanning every vector", cap:"HNSW descends a layered graph from sparse long-range links to the dense bottom layer. IVF clusters vectors and scans only the lists nearest the query (nprobe). Product quantization shrinks each vector to a few one-byte codes so millions fit in memory, with a rescoring step on full vectors.", svg:S("aiml3", 300, "Diagrams of an HNSW layered graph, IVF clusters with probed lists, and product quantization of a vector into codes",
  '<rect class=\"fill\" x=\"10\" y=\"30\" width=\"230\" height=\"240\" rx=\"8\"/>' + T(125, 52, "HNSW graph", "b", "middle") +
  '<circle cx="60" cy="90" r="6" class="ln"/><circle cx="190" cy="90" r="6" class="ln"/>' +
  '<path class="ln" d="M66 90 L184 90"/>' +
  '<circle cx="40" cy="150" r="6" class="ln"/><circle cx="110" cy="150" r="6" class="ln"/><circle cx="190" cy="150" r="6" class="ln"/>' +
  '<path class="ln" d="M46 150 L104 150 M116 150 L184 150"/>' +
  '<circle cx="30" cy="215" r="5" class="ln"/><circle cx="70" cy="215" r="5" class="ln"/><circle cx="110" cy="215" r="5" class="ln"/><circle cx="150" cy="215" r="5" class="ln"/><circle cx="200" cy="215" r="5" class="ln"/>' +
  '<path class="ln" d="M35 215 L65 215 M75 215 L105 215 M115 215 L145 215 M155 215 L195 215"/>' +
  A("aiml3", "M190 97 L190 142", "acc") + A("aiml3", "M190 157 L152 208", "acc") +
  T(125, 255, "layers: few nodes on top, all at 0", "sm", "middle") +
  '<rect class=\"fill\" x=\"265\" y=\"30\" width=\"230\" height=\"240\" rx=\"8\"/>' + T(380, 52, "IVF lists", "b", "middle") +
  B(285, 75, 85, 60, "st", ["list 1", "probed"]) + B(390, 75, 85, 60, "box", ["list 2", "skipped"]) +
  B(285, 150, 85, 60, "st", ["list 3", "probed"]) + B(390, 150, 85, 60, "box", ["list 4", "skipped"]) +
  T(380, 235, "scan nprobe nearest lists", "sm", "middle") + T(380, 255, "k-means centroids at build", "sm", "middle") +
  '<rect class=\"fill\" x=\"520\" y=\"30\" width=\"230\" height=\"240\" rx=\"8\"/>' + T(635, 52, "Product quantization", "b", "middle") +
  B(540, 75, 190, 40, "sp", ["768 floats = 3,072 bytes"], false) +
  A("aiml3", "M635 117 L635 143") +
  B(540, 145, 44, 36, "pr", ["17"], false) + B(589, 145, 44, 36, "pr", ["203"], false) + B(638, 145, 44, 36, "pr", ["4"], false) + B(687, 145, 43, 36, "pr", ["..."], false) +
  T(635, 210, "96 codes = 96 bytes", "sm", "middle") + T(635, 235, "32x smaller, approximate", "sm", "middle") + T(635, 255, "rescore top candidates", "sm", "middle")) },

aiml_rag:{ t:"RAG: an indexing pipeline and a query pipeline", cap:"The indexing pipeline runs ahead of time and is driven by changes in the sources, including deletions and permission changes. The query pipeline retrieves with both dense and keyword search under the user's permissions, reranks, and generates an answer with citations; everything is logged for evaluation.", svg:S("aiml4", 330, "Top row: sources, parse, chunk, embed into a hybrid index. Bottom row: question, retrieve, rerank, generate, answer and log",
  T(20, 24, "Indexing (ahead of time, change-driven)", "b") +
  B(10, 40, 120, 64, "sp", ["Sources", "docs, tickets", "CDC events"]) +
  B(150, 40, 120, 64, "fill", ["Parse", "PDF, HTML", "tables, OCR"]) +
  B(290, 40, 120, 64, "fill", ["Chunk", "with titles", "and ACLs"]) +
  B(430, 40, 120, 64, "fill", ["Embed", "changed chunks", "only"]) +
  B(580, 40, 170, 64, "st", ["Hybrid index", "vectors + BM25", "metadata, ACLs"]) +
  A("aiml4", "M130 72 L148 72") + A("aiml4", "M270 72 L288 72") + A("aiml4", "M410 72 L428 72") + A("aiml4", "M550 72 L578 72") +
  T(20, 170, "Query (per question)", "b") +
  B(10, 190, 120, 64, "pr", ["Question", "+ user identity"]) +
  B(150, 190, 120, 64, "fill", ["Retrieve", "dense + keyword", "ACL filter"]) +
  B(290, 190, 120, 64, "fill", ["Fuse, rerank", "RRF, then", "cross-encoder"]) +
  B(430, 190, 120, 64, "fill", ["Generate", "top passages", "cite chunk IDs"]) +
  B(580, 190, 170, 64, "okf", ["Answer + log", "chunks, scores", "feedback"]) +
  A("aiml4", "M130 222 L148 222") + A("aiml4", "M270 222 L288 222") + A("aiml4", "M410 222 L428 222") + A("aiml4", "M550 222 L578 222") +
  A("aiml4", "M640 106 L230 186", "dash") + T(470, 150, "search the index", "sm", "middle") +
  T(380, 300, "evaluate retrieval (recall@k) and generation (faithfulness) separately", "sm", "middle")) },

aiml_mlops:{ t:"The MLOps loop", cap:"Data flows clockwise: features and labels build a versioned training set, a candidate is trained and gated against the champion, rolled out through shadow and canary, and every prediction is logged. Logs joined with late-arriving outcomes feed monitoring, which can trigger the next retraining.", svg:S("aiml5", 320, "A loop of six stages from features to training, gate, rollout, prediction logging and monitoring, with a retraining trigger back to the start",
  B(20, 40, 160, 70, "sp", ["Features, labels", "point-in-time", "snapshot, version"]) +
  B(300, 40, 160, 70, "fill", ["Train and track", "MLflow runs", "dataset inputs"]) +
  B(580, 40, 160, 70, "pr", ["Gate", "vs champion", "per segment"]) +
  B(580, 210, 160, 70, "st", ["Roll out", "shadow, canary", "alias to champion"]) +
  B(300, 210, 160, 70, "ka", ["Serve and log", "features as served", "score, version, arm"]) +
  B(20, 210, 160, 70, "okf", ["Monitor", "drift, quality", "outcomes joined"]) +
  A("aiml5", "M180 75 L298 75") + A("aiml5", "M460 75 L578 75") + A("aiml5", "M660 112 L660 208") +
  A("aiml5", "M578 245 L462 245") + A("aiml5", "M298 245 L182 245") + A("aiml5", "M100 208 L100 112", "acc") +
  T(110, 165, "retrain trigger", "sm") + T(520, 300, "fail: keep champion", "sm", "middle") +
  T(240, 300, "labels arrive late", "sm", "middle")) },

});
