/* memory: Context engineering, memory and RAG, original drawings */
Object.assign(FIGS, {

memory_context:{ t:"What fills the context window", cap:"Each model call is assembled from slots, stable ones first so the prefix can be cached, the current task last. Three systems feed the slots: the memory store, the retriever and the checkpointer that holds the thread's history. Everything competes for one token budget, with room left for the answer.", svg:S("mem1", 300, "A stacked context window of six slots from system prompt to current task, fed by a memory store, a retriever and a checkpointer, with a reserved space for the answer",
  T(250, 22, "one model call: the context window", "b", "middle") +
  B(130, 34, 240, 30, "st", ["System prompt: role, rules"], false) +
  B(130, 68, 240, 30, "st", ["Tool definitions"], false) +
  B(130, 102, 240, 30, "sp", ["Memories: user facts, rules"], false) +
  B(130, 136, 240, 30, "sp", ["Retrieved passages, numbered"], false) +
  B(130, 170, 240, 30, "fill", ["History: turns, tool results"], false) +
  B(130, 204, 240, 30, "ka", ["Current task"], false) +
  B(130, 238, 240, 30, "box", ["Reserved for the answer"], false) +
  T(120, 66, "stable", "sm", "end") + T(120, 82, "cached prefix", "sm", "end") +
  T(120, 205, "volatile", "sm", "end") + T(120, 221, "changes each call", "sm", "end") +
  B(520, 40, 210, 56, "pr", ["Memory store", "recall: relevance, recency"]) +
  B(520, 120, 210, 56, "pr", ["Retriever", "hybrid search, rerank"]) +
  B(520, 200, 210, 56, "pr", ["Checkpointer", "thread state, compacted"]) +
  A("mem1", "M520 68 L376 117") + A("mem1", "M520 148 L376 151") + A("mem1", "M520 228 L376 185") +
  T(445, 288, "budget: keep the highest-value pieces that fit", "sm", "middle")) },

memory_types:{ t:"Short-term and long-term memory", cap:"Short-term memory is the state of one thread, checkpointed after every step. Long-term memory outlives threads: semantic facts, episodic events and procedural rules, written by extraction and consolidation and recalled into later threads by relevance, recency and importance.", svg:S("mem2", 290, "A thread with checkpoints on the left feeds an extraction and consolidation step into three long-term stores, which are recalled into a new thread",
  T(150, 24, "short-term: one thread", "b", "middle") +
  B(20, 40, 260, 50, "fill", ["Thread t-1", "messages + typed state"]) +
  B(20, 110, 80, 40, "box", ["ckpt 1"], false) + B(110, 110, 80, 40, "box", ["ckpt 2"], false) + B(200, 110, 80, 40, "box", ["ckpt 3"], false) +
  A("mem2", "M150 90 L150 104") +
  B(20, 200, 260, 56, "ka", ["New thread t-2", "starts with recalled memories"]) +
  B(320, 110, 130, 56, "st", ["Extract and", "consolidate"]) +
  A("mem2", "M280 130 L314 136") +
  T(600, 24, "long-term: across threads", "b", "middle") +
  B(490, 40, 240, 56, "sp", ["Semantic", "facts: billing project"]) +
  B(490, 110, 240, 56, "sp", ["Episodic", "events: 3 Nov late file"]) +
  B(490, 180, 240, 56, "sp", ["Procedural", "rules: SQL first"]) +
  A("mem2", "M450 130 L484 72") + A("mem2", "M450 138 L484 138") + A("mem2", "M450 146 L484 204") +
  A("mem2", "M490 225 L286 232", "dash") +
  T(390, 268, "recall: relevance, recency, importance", "sm", "middle")) },

memory_rag:{ t:"RAG: offline indexing, online answering", cap:"Offline, documents are parsed, split into chunks that carry their context, embedded and indexed. Online, the question is embedded, candidates are retrieved with filters and reranked, the best few are numbered in the prompt, and the answer's citations are checked in code before it is returned.", svg:S("mem3", 260, "Top row: documents to chunk to embed to index. Bottom row: question to retrieve to rerank to prompt with sources to answer to citation check",
  T(20, 24, "offline", "b") +
  B(20, 36, 150, 50, "box", ["Documents", "runbooks, tickets"]) +
  B(200, 36, 150, 50, "fill", ["Chunk", "by section + context"]) +
  B(380, 36, 150, 50, "fill", ["Embed", "same model as queries"]) +
  B(560, 36, 180, 50, "pr", ["Index", "text, vector, metadata"]) +
  A("mem3", "M170 61 L194 61") + A("mem3", "M350 61 L374 61") + A("mem3", "M530 61 L554 61") +
  T(20, 128, "online", "b") +
  B(20, 140, 110, 50, "ka", ["Question"], false) +
  B(150, 140, 120, 50, "fill", ["Retrieve", "top 30, filters"]) +
  B(290, 140, 110, 50, "fill", ["Rerank", "keep top 5"]) +
  B(420, 140, 130, 50, "ka", ["Prompt", "numbered sources"]) +
  B(570, 140, 80, 50, "ka", ["Model"], false) +
  B(665, 140, 80, 50, "okf", ["Check", "citations"]) +
  A("mem3", "M130 165 L144 165") + A("mem3", "M270 165 L284 165") + A("mem3", "M400 165 L414 165") +
  A("mem3", "M550 165 L564 165") + A("mem3", "M650 165 L659 165") +
  A("mem3", "M650 86 L220 134", "dash") +
  T(380, 240, "retrieval quality (recall@k) sets the ceiling for answer quality", "sm", "middle")) },

memory_graphrag:{ t:"GraphRAG: local and global search", cap:"Indexing turns sources into a graph of entities and relations (from lineage metadata where possible, model extraction otherwise), groups it into communities and writes a summary of each. Local search walks out from the entities in a question and returns paths as evidence; global search maps a question over community summaries and combines the answers.", svg:S("mem4", 280, "Sources become triples, then a graph, then communities with summaries; a local question traverses paths in the graph; a global question reads community summaries",
  B(20, 40, 150, 56, "box", ["Sources", "lineage, docs, catalog"]) +
  B(200, 40, 150, 56, "st", ["Triples", "built_from, owned_by"]) +
  B(380, 40, 150, 56, "pr", ["Graph", "entities + edges"]) +
  B(560, 40, 180, 56, "sp", ["Communities", "summary per group"]) +
  A("mem4", "M170 68 L194 68") + A("mem4", "M350 68 L374 68") + A("mem4", "M530 68 L554 68") +
  B(260, 150, 210, 56, "ka", ["Local question", "who owns revenue inputs?"]) +
  B(530, 150, 210, 56, "ka", ["Global question", "main risks across DAGs?"]) +
  A("mem4", "M455 96 L380 144", "dash") + A("mem4", "M650 96 L640 144", "dash") +
  B(260, 222, 210, 44, "okf", ["Paths as cited evidence"], false) +
  B(530, 222, 210, 44, "okf", ["Map over summaries, reduce"], false) +
  A("mem4", "M365 206 L365 216") + A("mem4", "M635 206 L635 216") +
  T(20, 180, "vector search finds", "sm") + T(20, 196, "entry entities;", "sm") + T(20, 212, "the graph expands", "sm")) },

memory_vecfilter:{ t:"Filtering inside the search versus after it", cap:"Post-filtering takes the top k over every row and then drops the rows the user may not see, so a small team can get nothing back and other teams' data was searched. Filtering inside the search (a WHERE with iterative scans in pgvector, a payload filter in Qdrant, a filter expression in Milvus or OpenSearch) ranks only the allowed rows and returns a full top k.", svg:S("mem5", 250, "Two rows. Post-filter: query, top 2 over all teams, drop other teams, zero rows. Filter inside the search: query plus team filter, nearest among payments rows only, full top 2",
  T(20, 24, "post-filter: search everything, then filter", "b") +
  B(20, 36, 140, 50, "ka", ["Question", "\"file is late\""]) +
  B(190, 36, 170, 50, "fill", ["Top 2, all teams", "2 marketing runbooks"]) +
  B(390, 36, 170, 50, "st", ["Drop other teams", "team = payments"]) +
  B(590, 36, 150, 50, "box", ["0 rows returned", "data of others read"]) +
  A("mem5", "M160 61 L184 61") + A("mem5", "M360 61 L384 61") + A("mem5", "M560 61 L584 61") +
  T(20, 134, "filter inside the search", "b") +
  B(20, 146, 140, 50, "ka", ["Question", "+ team filter"]) +
  B(190, 146, 170, 50, "fill", ["Index walks only", "payments rows"]) +
  B(390, 146, 170, 50, "pr", ["Top 2 of allowed", "rows, ranked"]) +
  B(590, 146, 150, 50, "okf", ["Full top 2", "nothing else read"]) +
  A("mem5", "M160 171 L184 171") + A("mem5", "M360 171 L384 171") + A("mem5", "M560 171 L584 171") +
  T(380, 232, "bind the filter from the authenticated user, never from the model", "sm", "middle")) }

});
