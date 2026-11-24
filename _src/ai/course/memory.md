This section is for the reader who can write the agent loop by hand ([Agentic AI foundations](#foundations)), give it tools ([Tool use and function calling](#tools)) and arrange model calls into reliable designs ([Agent design patterns](#patterns)). It is about the one input every one of those designs depends on: **what the model sees** at each step. You need the same plain Python as before. No vector database, no embedding model and no API key: every retrieval example uses tiny in-memory data, a toy embedding and BM25 written from scratch, so it runs with `python3`.

**What you will be able to do afterwards**

- Explain context engineering: what goes into the context window, in what order, and why more context can make an agent worse (context rot).
- Keep long conversations and long agent runs inside the window with clearing, compaction and summarisation, and cut their cost with prompt caching.
- Store conversation state in threads and checkpoints, and explain how LangGraph checkpointers and ADK sessions do it.
- Design long-term memory: semantic, episodic and procedural memories, how they are written, recalled, updated and forgotten.
- Use real memory systems you can run yourself: the Claude memory tool, LangGraph's PostgreSQL checkpointer and store, ADK sessions and memory services, and Mem0 self-hosted with a local model.
- Build a RAG pipeline with chunking, embeddings, ranking, grounding and citation checks, then turn it into agentic RAG with query rewriting and multi-hop retrieval.
- Combine BM25 and vectors with reciprocal rank fusion, add a reranker, and measure retrieval with recall and MRR.
- Explain GraphRAG, and choose and configure a self-hosted vector database (pgvector, Qdrant, Milvus or OpenSearch) with filters that run inside the search.

**The big picture**

A model has no memory of its own. Every call starts from nothing, and the only thing it knows about your task is the text in the request. So every agent is, underneath, a program that decides what to put in front of the model at each step. Anthropic calls this **context engineering**: finding the smallest set of high-value tokens that makes the right next step likely. The context window is a fixed budget, like the memory of a Spark executor. Everything competes for it: instructions, tool definitions, memories, retrieved documents, the conversation so far and the current task.

[[fig:memory_context]]

Three mechanisms fill that budget, and this section is organised around them. **Context management** decides what stays in the window as a run gets longer: what to clear, what to summarise, what to cache. **Memory** carries information across steps, sessions and users: short-term state in checkpoints, long-term facts, events and rules in a store. **Retrieval** brings in knowledge the model never saw, from your runbooks, tickets and tables, at the moment it is needed. All three answer the same question: of everything the agent could know, what should it see right now?

If you build data platforms, the vocabulary is new but the ideas are not. The context window is a small, expensive cache in front of a large store. Checkpoints are the same idea as Airflow task state or Kafka consumer offsets. A memory store is a table with a key, a value, a timestamp and a retention policy. Retrieval is an index lookup. The data engineering course covers the infrastructure behind retrieval in depth (embedding pipelines, vector databases, ANN indexes and RAG pipelines in [AI and ML for data engineers](index.html#aiml)). This section stays on the agent side: what goes into the context and how the agent decides.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

> **Tip:** From-scratch examples run with plain `python3` and a scripted **mock model**, as in earlier sections. Framework examples were run offline against the installed packages: `langgraph` 1.2.24 (with `langgraph-checkpoint-postgres` 3.2.1), `langchain` 1.5.2, `google-adk` 2.15.0, `anthropic` 2.13.0, `qdrant-client` 1.20.0 and `mem0ai` 2.2.1. The default stack is one you can run on your own servers: PostgreSQL with pgvector, Qdrant, Redis, and local embedding and chat models served with Ollama, vLLM or sentence-transformers. Where an example needs a running server (PostgreSQL, Ollama), it was import-checked and its configuration validated, but not run; Qdrant examples run in its embedded local mode.

## Context

The context window is the model's whole world for one call. This group is about filling it well. The first topic is the discipline itself: what belongs in the window, in what order, and why more is not better. The second handles growth: long conversations and long agent runs that would overflow the window or cost too much, and the tools for that (clearing, compaction, summarisation, prompt caching). The third is the simplest form of memory: keeping the state of one conversation or one run across steps and restarts, with threads and checkpoints.

Together they cover everything an agent needs within one task. Memory that outlives a task comes in the next group.

### Context engineering

::: plain
Think of a consultant flown in for a one-hour meeting. They know nothing about your company except the briefing pack you hand them at the door. A pack with the right ten pages makes them brilliant; a pack with three hundred pages, most of them irrelevant, makes them slow and confused, and they may quote the wrong page. Context engineering is preparing that briefing pack, again, before every single step.
:::

**What it is.** Context engineering is the work of deciding, for each model call, which tokens go into the request: the system prompt, tool definitions, examples, memories, retrieved documents, earlier messages and tool results, and the current task. Prompt engineering is about writing one good instruction. Context engineering is about curating the whole request, again and again, as an agent loops and its state changes. Anthropic's guidance frames the goal as finding "the smallest possible set of high-signal tokens" for the next step.

**Why it exists.** Three facts about models make it necessary.

- **The window is finite.** Even a window of hundreds of thousands of tokens fills up in a long agent run, because every tool result is appended and the whole history is resent on every step.
- **Attention is a budget.** A model attends to every token, but not equally well. As the context grows, its ability to find and use one specific fact goes down. Research on "lost in the middle" (Liu et al., 2023) showed that models use information at the start and end of a long context better than information in the middle. Chroma's 2025 "Context Rot" study tested many current models and found that performance drops as input length grows, even on simple tasks, and that distracting but similar text hurts most. This decay is now usually called **context rot**.
- **Tokens cost money and time.** Input tokens are billed on every call and they add latency. A 30-step agent resends its history 30 times.

So the job is not to give the model everything. It is to give it what it needs for the next step and nothing that could mislead it.

**What goes in.** A useful way to think about a request is as slots, each with an owner and a lifetime:

| Slot | Contents | Changes | Source |
|---|---|---|---|
| System prompt | Role, rules, output format | Rarely (per release) | Your code, versioned |
| Tools | Names, descriptions, schemas | Rarely | Tool registry or MCP server |
| Memory | User preferences, facts, lessons | Per session | Memory store |
| Retrieved knowledge | Runbook sections, table docs, past incidents | Per step | Retriever or search tool |
| History | Earlier messages, tool calls and results | Every step | Checkpointer |
| Current task | The question, today's date, the run ID | Every call | Request |

**Ordering matters, for two reasons.** First, caching: providers cache the processed prefix of a request, so stable slots go first and volatile ones last (more in the next topic). Second, attention: put long documents before the question that uses them and repeat the key instruction near the end if the context is long. Anthropic's long-context tips say the same: documents at the top, the query at the bottom.

**A from-scratch context builder.** The code below treats the window as a budget. Each candidate piece has a slot and a priority. The builder keeps the highest-priority pieces that fit, then arranges them by slot so the stable parts come first. This is the core of every context assembler, whatever framework wraps it.

```python
def tokens(text):
    """Rough token count: about 4 characters per token for English text."""
    return max(1, len(text) // 4)

# Candidate pieces of context, each with a slot (where it goes) and a priority (lower = keep first)
pieces = [
    {"slot": "system",  "prio": 0, "text": "You are the on-call assistant for the payments data platform. "
                                            "Answer with the cause, the evidence and the next action."},
    {"slot": "tools",   "prio": 0, "text": "Tools: run_sql(query), get_dag_runs(dag_id), read_runbook(name)"},
    {"slot": "memory",  "prio": 1, "text": "User prefers SQL first. Team: payments. Billing project: eu-billing."},
    {"slot": "docs",    "prio": 2, "text": "Runbook orders_daily: late files from the PSP arrive after 06:00 UTC; " * 3},
    {"slot": "docs",    "prio": 3, "text": "Runbook refunds_daily: refunds are restated on Mondays; " * 3},
    {"slot": "docs",    "prio": 4, "text": "Glossary: GMV means gross merchandise value. " * 6},
    {"slot": "history", "prio": 2, "text": "user: why is orders_daily late? assistant: checking DAG runs..."},
    {"slot": "history", "prio": 5, "text": "user: (yesterday) how do I request Trino access? assistant: ... " * 4},
    {"slot": "task",    "prio": 0, "text": "Current question: orders_daily is 2 hours late today. Why?"},
]
ORDER = ["system", "tools", "memory", "docs", "history", "task"]   # stable first, volatile last

def build_context(pieces, budget):
    kept, used = [], 0
    for p in sorted(pieces, key=lambda p: p["prio"]):               # 1. choose by priority
        cost = tokens(p["text"])
        if used + cost <= budget:
            kept.append(p); used += cost
    dropped = [p for p in pieces if p not in kept]
    kept.sort(key=lambda p: ORDER.index(p["slot"]))                  # 2. arrange by slot
    return kept, dropped, used

kept, dropped, used = build_context(pieces, budget=150)
print([p["slot"] for p in kept], used)
# ['system', 'tools', 'memory', 'docs', 'history', 'task'] 142
print([(p["slot"], p["prio"]) for p in dropped])
# [('docs', 3), ('docs', 4), ('history', 5)]
```

Read what it decided. The system prompt, tools and the current question have priority 0 and always fit. The relevant runbook and the current thread survive. The refunds runbook, the glossary and yesterday's unrelated conversation are dropped: they would cost tokens and offer the model text to be distracted by. In a real agent the priorities come from retrieval scores, recency and the task type, and the budget is a fraction of the window that leaves room for the answer and for the steps still to come.

**The four moves.** LangChain's write-up on context engineering groups the techniques into four moves, a handy checklist in interviews:

| Move | What it means | Examples in this section |
|---|---|---|
| Write | Save information outside the window for later | Scratchpads, memory files, checkpoints |
| Select | Pull the right information in | Retrieval, memory recall, tool selection |
| Compress | Keep the meaning, drop the tokens | Summaries, compaction, trimming tool results |
| Isolate | Split context across separate windows | Sub-agents, sandboxes, state fields hidden from the model |

> **Pitfall:** Treating the context as a log. Appending every tool result in full is the default in a naive loop, and it is why long agent runs get slower, more expensive and less accurate as they go. Decide per tool what the model needs to see: a 5,000-row query result should come back as a count, a schema and ten sample rows.

> **Tip:** Log the exact context of every call (or a hash of each slot plus its size). When an agent makes a strange decision, the first question is "what did it see?", and you cannot answer it from the final transcript alone if your code trimmed or reordered the history.

::: deeper
**Just-in-time context.** There are two ways to give an agent knowledge. Pre-loading retrieves everything that might be relevant before the first step and puts it in the prompt. Just-in-time loading gives the agent light references (file paths, table names, document IDs) and tools to fetch details when it needs them. Anthropic describes coding agents that keep only paths in context and read files with tools; the same works for a data agent that sees table names and descriptions and calls `get_schema` when it picks one. Pre-loading is faster and cheaper per step; just-in-time scales to large corpora and avoids stale or irrelevant text. Most production agents mix them: a small, always-relevant core is pre-loaded, the rest is fetched.

**Why long context does not remove the problem.** Large windows let you skip retrieval for small corpora, and that is often a good choice (a 50-page runbook fits). But context rot means accuracy falls as length grows, cost grows linearly with input tokens on every step, and latency grows too. Long context and retrieval are complements: retrieval narrows, the long window gives room for what retrieval found plus the run's history.

**Isolation through sub-agents.** A sub-agent with its own fresh window can read 50 documents and return a one-paragraph finding. The parent's context grows by one paragraph, not 50 documents. Anthropic reported that its multi-agent research system spent many more tokens in total than a single agent but did better on broad research tasks largely because each sub-agent worked in a clean context. The trade-off is cost and coordination ([Multi-agent systems](#multiagent)).

**Failure modes to name.** Context poisoning: a hallucinated or wrong fact enters the history and is reused as truth in later steps. Context distraction: the history is so long that the model repeats past actions instead of planning new ones. Context confusion: irrelevant tools or documents lead the model to call or cite the wrong thing. Context clash: two pieces of context contradict each other (an old memory and a new instruction). Each has a fix in this section: validation before writing to memory, compaction, tool and document selection, and update rules for memories.

**Tool definitions are context too.** Every tool's name, description and schema is sent on every call. An agent with 80 tools carries thousands of tokens of definitions and has more ways to pick the wrong one. Select tools per task (a router, or a tool-search tool that loads definitions on demand) instead of registering everything ([Tool use and function calling](#tools)).

> **Interview:** "What is context engineering and how is it different from prompt engineering?" Prompt engineering is writing a good instruction. Context engineering is deciding, for every call in an agent's loop, what the whole request contains: system prompt, tools, memories, retrieved documents, history and task, within a token budget. It exists because windows are finite, models get worse as context grows (context rot, lost in the middle), and tokens cost money. The techniques are write (save outside the window), select (retrieve), compress (summarise, trim) and isolate (sub-agents).

> **Interview:** "Our agent gets worse the longer it runs. Why, and what would you do?" The history is growing: large tool outputs, repeated attempts and stale observations crowd out the task and push key facts into the middle. I would trim tool results at the source, clear old tool outputs, compact the history into a summary when it passes a threshold, keep durable facts in a scratchpad or memory file the agent rereads, and move broad exploration into sub-agents. Then I would measure accuracy against run length on an evaluation set.
:::

::: check
?? Why can adding more relevant-looking documents to the prompt make an answer worse?
Models use long contexts unevenly (lost in the middle) and degrade as input grows (context rot); similar but irrelevant text distracts most. More tokens also cost more and add latency.

?? In what order should the slots of a request go, and why?
Stable content first (system prompt, tools, long-lived memory), then retrieved documents and history, then the current task last. Stable prefixes cache well, and the question near the end, after the documents it refers to, is used better.

?? What are the four moves of context engineering?
Write (save information outside the window), select (retrieve the right information in), compress (summarise or trim), and isolate (split work across separate contexts such as sub-agents).
:::

::: resources
- Blog: [Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents) · Anthropic's guide: smallest high-signal context, just-in-time retrieval, compaction
- Blog: [Context engineering for agents](https://blog.langchain.com/context-engineering-for-agents/) · the write, select, compress and isolate framing
- Paper: [Lost in the Middle: How Language Models Use Long Contexts](https://arxiv.org/abs/2307.03172) · position effects in long prompts
- Article: [Context Rot](https://research.trychroma.com/context-rot) · Chroma's study of how performance falls as input length grows
- Blog: [Context engineering](https://simonwillison.net/2025/Jun/27/context-engineering/) · Simon Willison on why the term caught on
- Blog: [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system) · context isolation with sub-agents in practice
:::

### Context window management: compaction, summarisation and caching

::: plain
A detective on a long case keeps a notebook. She cannot carry every witness statement in her head, so every few days she rewrites her notes into one page: what is known, what was ruled out, what to check next. The original statements go into a box she can open again if needed. Compaction is that one-page rewrite. Prompt caching is different: it is the office remembering the case file she reads every morning, so she does not pay to read it from scratch each time.
:::

**The problem.** An agent's history grows on every step. Each step adds an assistant turn (the tool call) and a user turn (the tool result), and the whole history is sent again on the next call. Without management, a long run eventually fails with a context-length error, and long before that it gets slower, more expensive and less accurate. A long chat has the same problem more slowly.

**The toolbox,** from cheapest and safest to most lossy:

| Technique | What it does | Loses | Good for |
|---|---|---|---|
| Trim at the source | Tools return compact results (counts, samples, IDs) | Nothing the model needed | Every agent, always |
| Clear old tool results | Replace old results with a placeholder; keep the call | Raw detail the agent can re-fetch | Tool-heavy runs |
| Sliding window | Keep the last N messages | Everything older | Simple chats |
| Summarise (compaction) | Replace older turns with a model-written summary | Detail not in the summary | Long runs and chats |
| Offload to memory | Agent writes notes to a file or store and rereads them | Nothing, if notes are good | Multi-session work |

The first two are nearly free and deterministic, so apply them first. Summarisation costs a model call and may lose details, so it runs only when the cheaper steps are not enough.

**A from-scratch manager.** The example below builds a realistic history (six rounds of SQL calls with large results), then applies the two steps in order: clear all but the last two tool results, then, if still over budget, summarise the middle of the conversation while keeping the goal and the most recent turns verbatim.

```python
import json

def tokens(msg):
    return max(1, len(json.dumps(msg)) // 4)

def total(messages):
    return sum(tokens(m) for m in messages)

# An agent history: 6 rounds of tool use, each tool result a large JSON blob
history = [{"role": "user", "content": "Find why orders_daily row counts dropped this week."}]
for day in range(6):
    history.append({"role": "assistant", "content": [
        {"type": "tool_use", "id": f"tu_{day}", "name": "run_sql",
         "input": {"query": f"SELECT ... WHERE load_date = '2026-11-{17 + day}'"}}]})
    history.append({"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": f"tu_{day}",
         "content": json.dumps({"rows": [[f"psp_{i}", 1000 - day * 90] for i in range(25)]})}]})

def clear_old_tool_results(messages, keep=2, placeholder="[result cleared; re-run the tool if needed]"):
    """Step 1: replace all but the last `keep` tool results with a short placeholder."""
    result_idx = [i for i, m in enumerate(messages) if isinstance(m["content"], list)
                  and m["content"][0]["type"] == "tool_result"]
    out = [dict(m) for m in messages]
    for i in result_idx[:-keep]:
        block = dict(out[i]["content"][0], content=placeholder)
        out[i] = {"role": "user", "content": [block]}
    return out

def mock_summarize(messages):
    """Stand-in for a model call that summarises older turns."""
    return ("Summary of earlier work: ran run_sql for 2026-11-17..20; PSP rows fell "
            "from 1000 to 730 per PSP; no failed DAG runs found so far.")

def compact(messages, budget, keep_last=4):
    """Step 2: if still over budget, summarise everything except the goal and the last turns."""
    if total(messages) <= budget:
        return messages
    goal, middle, recent = messages[0], messages[1:-keep_last], messages[-keep_last:]
    first = {"role": "user", "content": goal["content"] + "\n\n" + mock_summarize(middle)}
    return [first] + recent          # recent starts with an assistant turn, so roles still alternate

print("raw:", total(history))
# raw: 1106
step1 = clear_old_tool_results(history)
print("after clearing:", total(step1))
# after clearing: 664
step2 = compact(step1, budget=500)
print("after compaction:", total(step2), "messages:", len(step2))
# after compaction: 413 messages: 5
```

Three details carry over to real systems:

- **Tool calls and results stay paired.** The clearing step keeps each `tool_use` and replaces only the content of its `tool_result`, so the transcript stays valid. The compaction step cuts at an even boundary so the kept turns start with an assistant `tool_use` whose result follows. Breaking a pair is a common cause of API errors after compaction.
- **The goal is never summarised away.** The user's original request survives verbatim at the top. Summaries drift; the goal must not.
- **The placeholder tells the model what to do.** "Re-run the tool if needed" turns lost detail into a recoverable situation rather than a hallucination risk.

**The same thing, managed for you.** Providers and frameworks now ship these steps:

- **Claude context editing** (beta header `context-management-2025-06-27`) clears old tool results on the server when input passes a trigger. The edit `clear_tool_uses_20250919` takes `trigger`, `keep`, `clear_at_least`, `exclude_tools` and `clear_tool_inputs`; a second edit, `clear_thinking_20251015`, clears old thinking blocks.
- **Claude compaction** (beta) summarises older turns on the server and returns a compaction block you send back in place of the turns it covers. It comes in two forms: on demand (you request it) and at a token threshold (the `compact_20260112` edit inside `context_management`). Check the compaction docs for the current beta header, which differs between the two.
- **LangChain middleware** for `create_agent`: `SummarizationMiddleware` (trigger by tokens, messages or a fraction of the window, keep the last N) and `ContextEditingMiddleware` with `ClearToolUsesEdit`.
- **ADK** compacts session events with `EventsCompactionConfig` on the `App`, by a sliding window of invocations or a token threshold.

```python
# Claude context editing: clear old tool results on the server (anthropic 2.13.0, beta)
context_management = {"edits": [{
    "type": "clear_tool_uses_20250919",
    "trigger": {"type": "input_tokens", "value": 30_000},   # start clearing past 30k input tokens
    "keep": {"type": "tool_uses", "value": 3},                # keep the 3 most recent results
    "clear_at_least": {"type": "input_tokens", "value": 5_000},
    "exclude_tools": ["memory"],                               # never clear memory reads
}]}
# client.beta.messages.create(model=..., max_tokens=..., messages=..., tools=...,
#                             betas=["context-management-2025-06-27"],
#                             context_management=context_management)

# LangChain 1.5: the same ideas as agent middleware
from langchain.agents.middleware import (SummarizationMiddleware, ContextEditingMiddleware,
                                         ClearToolUsesEdit)
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

summarizer = GenericFakeChatModel(messages=iter([AIMessage("summary...")]))  # in production: a small chat model
middleware = [
    ContextEditingMiddleware(edits=[ClearToolUsesEdit(trigger=30_000, keep=3)]),
    SummarizationMiddleware(model=summarizer, trigger=("tokens", 60_000), keep=("messages", 20)),
]
# agent = create_agent(model, tools, middleware=middleware, checkpointer=...)

# ADK 2.15: compact session events every 5 invocations, overlapping by 1
from google.adk.apps import App
from google.adk.apps.app import EventsCompactionConfig
compaction = EventsCompactionConfig(compaction_interval=5, overlap_size=1)
# app = App(name="oncall", root_agent=root_agent, events_compaction_config=compaction)
```

**Prompt caching.** Clearing and compaction reduce the tokens you send. Prompt caching reduces what you pay for the tokens you keep sending. The provider stores the processed state of a request's prefix; a later request with a byte-identical prefix reuses it at a fraction of the price and with lower latency. On Claude you mark cache breakpoints with `cache_control` on a block (or set a top-level `cache_control` that marks the last cacheable block automatically); cache writes cost more than normal input (1.25 times base for the default five-minute lifetime, more for one hour) and cache reads much less (about a tenth). The response's `usage` reports `cache_creation_input_tokens` and `cache_read_input_tokens`, which you should log. Self-hosted models get the same benefit from the inference server: vLLM's automatic prefix caching keeps the KV cache (the model's processed state for each token) of recent prefixes in GPU memory and reuses it for any request that starts the same way, which cuts latency and raises throughput rather than a bill. Hosted Gemini models offer implicit caching and explicit context caches with a TTL.

Why it matters so much for agents: the history is resent on every step, so most input tokens are a prefix the provider has already seen. A quick model of one run makes the point:

```python
def run_cost(steps=20, prefix=8_000, per_step=1_500, cached=False,
             write_mult=1.25, read_mult=0.10):
    """Input-token cost of one agent run, in units of 'one uncached input token'."""
    cost, prev = 0.0, 0
    for step in range(steps):
        prompt = prefix + per_step * step          # whole history is resent every step
        if not cached:
            cost += prompt
        else:                                     # cache read up to last step's prompt, write the rest
            cost += prev * read_mult + (prompt - prev) * write_mult
        prev = prompt
    return cost

plain, cached = run_cost(), run_cost(cached=True)
print(f"uncached: {plain:,.0f}  cached: {cached:,.0f}  saving: {1 - cached / plain:.0%}")
# uncached: 445,000  cached: 86,475  saving: 81%
```

> **Pitfall:** Compaction and caching pull in opposite directions. Rewriting the start of the history invalidates the cached prefix, so the next call pays full price for everything. Compact rarely and in large steps (at a threshold, not every turn), and keep the system prompt and tools, which never change, before the part you rewrite.

> **Tip:** Put a token budget in your loop alongside the step limit, and log tokens per step. A sudden jump is almost always one oversized tool result, and the fix belongs in the tool.

::: deeper
**What a good summary keeps.** A compaction prompt should ask for the goal, decisions made and why, facts established (with their source), what was tried and failed, open questions and the next steps, plus identifiers verbatim (table names, run IDs, file paths). LangChain's default `SummarizationMiddleware` prompt asks for session intent, summary, artifacts and next steps for exactly this reason. Test the summary: run the agent from the compacted state on an evaluation set and compare with the uncompacted run.

**Recursive summaries drift.** Compacting a history that already starts with a summary summarises a summary. Details fade and early mistakes harden. Mitigations: keep durable facts in a separate notes file or memory the agent maintains (it is never summarised, only edited), keep the original turns in storage so a human or a tool can recover them, and overlap compaction windows (ADK's `overlap_size`).

**Caching rules worth knowing.** Caches match on exact prefixes, so anything that changes early (a timestamp in the system prompt, tools in a different order, a changed tool description) invalidates everything after it. There is a minimum cacheable length (about a thousand tokens or more, depending on the model). Claude allows a small number of explicit breakpoints per request, and caches expire after their TTL unless reused. Caching changes cost and latency, not the output.

**Context editing versus compaction.** Clearing tool results is deterministic and keeps the conversation structure: the model sees that it called a tool and that the result was cleared. Compaction rewrites history into prose, which is more compressive but lossy and harder to audit. Anthropic's guidance suggests clearing for tool-heavy agents and compaction for long conversations, and pairing either with a memory file for facts that must survive.

**Token counting.** Character heuristics (four characters per token) are fine for budgets with headroom. For decisions near the limit, use the provider's token-counting endpoint, since tokenizers differ by model and count tool definitions and images too.

> **Interview:** "How would you keep a 200-step agent inside the context window?" Layered: trim tool outputs at the source; clear old tool results beyond the last few (keeping the call so the model knows it can re-run it); compact the older history into a structured summary at a high threshold, keeping the goal and the last turns verbatim; keep durable facts in a notes file or memory the agent maintains; push broad exploration into sub-agents. Then prompt caching to cut the cost of what remains, and a token budget in the loop as a guard.

> **Interview:** "How does prompt caching work and how do you get the most from it?" The provider stores the processed prefix of a request; requests with an identical prefix reuse it at a fraction of the input price and with less latency. To benefit: stable content first (system prompt, tool definitions, long documents), volatile content last, no timestamps or request IDs in the prefix, deterministic tool ordering, and enough reuse within the TTL. Monitor cache-read tokens per call; a drop means something in the prefix started changing.
:::

::: check
?? Why clear old tool results before summarising?
Clearing is deterministic, free and keeps the transcript structure; the agent can re-run a tool to get the detail back. Summarising costs a model call and can lose or distort facts, so it should run only when cheaper steps are not enough.

?? What must a compaction step never break?
The pairing of each `tool_use` with its `tool_result`, the alternation of roles, and the original goal of the run.

?? An agent's cache hit rate dropped to zero after a release. What is the likely cause?
Something early in the request started changing between calls: a timestamp or ID in the system prompt, tool definitions in a different order, or a tool description that is generated per request.
:::

::: resources
- Docs: [Prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching) · breakpoints, TTLs, pricing and usage fields
- Docs: [Context editing](https://platform.claude.com/docs/en/build-with-claude/context-editing) · clearing tool results and thinking blocks on the server
- Docs: [Compaction](https://platform.claude.com/docs/en/build-with-claude/compaction) · server-side summarisation on demand or at a threshold
- Docs: [Prebuilt middleware](https://docs.langchain.com/oss/python/langchain/middleware/built-in) · `SummarizationMiddleware` and `ContextEditingMiddleware`
- Docs: [Context compression in ADK](https://adk.dev/context/compaction/) · `EventsCompactionConfig` for sessions
- Docs: [Automatic prefix caching in vLLM](https://docs.vllm.ai/en/latest/design/prefix_caching.html) · how a self-hosted inference server reuses the KV cache of shared prefixes
:::

### Short-term memory and conversation state

::: plain
A bank teller handling your request across three visits does not ask for your story each time: there is a ticket with everything so far. If the teller goes home, a colleague picks up the same ticket and carries on. Short-term memory is that ticket. A thread is one ticket, and a checkpoint is the ticket's state after each step, saved so anyone can continue from it.
:::

**What it is.** Short-term memory is the state of one conversation or one agent run: the messages so far, plus any structured fields the agent tracks (the table it is investigating, the plan, the approval status). It lives for the length of a **thread** (one conversation or one task) and is saved after every step as a **checkpoint**, so the run can continue after a restart, in another process, or days later.

**Why it exists.** The model is stateless; your application holds the state. In a notebook, a Python list is enough. In production, requests land on different servers, processes restart, users come back tomorrow, and human approvals take hours. The state must be persisted, keyed by thread, and loadable by any worker. It is the same problem as checkpointing a streaming job: you save offsets and state so a restart resumes where it stopped instead of starting over.

**Three ideas to keep apart:**

| Idea | What it is | Data engineering analogue |
|---|---|---|
| Thread | The identity of one conversation or run (`thread_id`) | A job run ID |
| State schema | The typed fields the agent keeps, and how updates merge | A table schema with merge rules |
| Checkpoint | A saved snapshot of the state after a step | Task state in the Airflow metadata DB, Kafka offsets |

**State is more than messages.** A good state schema has typed fields for what the agent must not forget: `focus_table`, `plan`, `approved`, `budget_used`. Fields are cheaper and more reliable than hoping the model rereads a fact from message 14. They can be shown to the model selectively, used by code for routing, and kept out of the prompt entirely when the model does not need them.

**A from-scratch checkpointer.** Here the state of each thread is saved to SQLite after every turn, keyed by `(thread_id, step)`. A new turn loads the latest checkpoint for its thread. Two threads never see each other's state.

```python
import json, sqlite3

class Checkpointer:
    """Saves the full state of a thread after every step, keyed by (thread_id, step)."""
    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS checkpoints ("
                        "thread_id TEXT, step INTEGER, state TEXT, PRIMARY KEY (thread_id, step))")
    def put(self, thread_id, step, state):
        self.db.execute("INSERT INTO checkpoints VALUES (?, ?, ?)", (thread_id, step, json.dumps(state)))
        self.db.commit()
    def latest(self, thread_id):
        row = self.db.execute("SELECT step, state FROM checkpoints WHERE thread_id = ? "
                              "ORDER BY step DESC LIMIT 1", (thread_id,)).fetchone()
        return (row[0], json.loads(row[1])) if row else (0, None)
    def history(self, thread_id):
        return [s for (s,) in self.db.execute(
            "SELECT step FROM checkpoints WHERE thread_id = ? ORDER BY step", (thread_id,))]

def mock_model(state):
    """Scripted assistant: remembers the table named earlier in the same thread."""
    last = state["messages"][-1]["content"]
    if "orders_daily" in last:
        return "orders_daily last loaded 2026-11-24 05:10 UTC."
    if "it" in last.split() and state["focus"]:
        return f"{state['focus']} has 3 upstream tasks."
    return "Which table do you mean?"

def chat_turn(cp, thread_id, user_text):
    step, state = cp.latest(thread_id)
    state = state or {"messages": [], "focus": None}           # new thread: empty state
    state["messages"].append({"role": "user", "content": user_text})
    if "orders_daily" in user_text:
        state["focus"] = "orders_daily"                         # a typed field, not just chat text
    state["messages"].append({"role": "assistant", "content": mock_model(state)})
    cp.put(thread_id, step + 1, state)
    return state["messages"][-1]["content"]

cp = Checkpointer()
print(chat_turn(cp, "t-1", "When did orders_daily last load?"))
# orders_daily last loaded 2026-11-24 05:10 UTC.
print(chat_turn(cp, "t-1", "How many upstream tasks does it have?"))
# orders_daily has 3 upstream tasks.
print(chat_turn(cp, "t-2", "How many upstream tasks does it have?"))
# Which table do you mean?
print(cp.history("t-1"), cp.latest("t-1")[1]["focus"])
# [1, 2] orders_daily
```

Every checkpoint is kept, not overwritten. That gives you replay (load step 1 and re-run step 2 with a new prompt), audit (what did the agent know when it acted?) and recovery after a crash mid-step.

**In LangGraph.** The graph's state is a `TypedDict`; each field can have a **reducer** that says how updates merge (`add_messages` appends messages and updates them by ID; `operator.add` concatenates lists; no reducer means last write wins). Compile with a **checkpointer** and pass a `thread_id` in the config; the graph saves a checkpoint at every step (checked with `langgraph` 1.2.24):

```python
import operator
from typing import Annotated, TypedDict
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

class ChatState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]   # reducer: append, update by id
    focus: str | None                                     # no reducer: last write wins
    tables_seen: Annotated[list[str], operator.add]        # reducer: concatenate

def respond(state: ChatState) -> dict:
    text = state["messages"][-1].content
    focus = "orders_daily" if "orders_daily" in text else state.get("focus")
    reply = f"Looking at {focus}." if focus else "Which table do you mean?"
    return {"messages": [AIMessage(reply)], "focus": focus,
            "tables_seen": [focus] if focus and focus != state.get("focus") else []}

builder = StateGraph(ChatState)
builder.add_node("respond", respond)
builder.add_edge(START, "respond")
builder.add_edge("respond", END)
app = builder.compile(checkpointer=InMemorySaver())

t1 = {"configurable": {"thread_id": "t-1"}}
app.invoke({"messages": [HumanMessage("Check orders_daily")]}, t1)
out = app.invoke({"messages": [HumanMessage("Is it late?")]}, t1)
print(out["messages"][-1].content, len(out["messages"]), out["tables_seen"])
# Looking at orders_daily. 4 ['orders_daily']
other = app.invoke({"messages": [HumanMessage("Is it late?")]}, {"configurable": {"thread_id": "t-2"}})
print(other["messages"][-1].content)
# Which table do you mean?
snapshot = app.get_state(t1)
print(snapshot.values["focus"], snapshot.next, len(list(app.get_state_history(t1))))
# orders_daily () 6
```

The second call on thread `t-1` sends only the new message; the checkpointer supplies the rest, so the reply knows the focus table. Thread `t-2` starts empty. `get_state` returns the latest snapshot (`next` is empty because the graph finished), and `get_state_history` lists every checkpoint: three per invocation here (the input, the start, and after `respond`). `InMemorySaver` is for tests; production uses a durable checkpointer from a separate package with the same interface. The usual self-hosted choice is `PostgresSaver` from `langgraph-checkpoint-postgres`, backed by the PostgreSQL you already run: `with PostgresSaver.from_conn_string(DB_URI) as checkpointer:`, call `checkpointer.setup()` once to create its tables, then `builder.compile(checkpointer=checkpointer)`. Nothing else in the graph changes. `SqliteSaver` (`langgraph-checkpoint-sqlite`) suits a single process, and a Redis checkpointer (`langgraph-checkpoint-redis`) suits short-lived threads that need very fast reads. The next topic shows the PostgreSQL checkpointer and store wired together.

**In ADK.** A **session** is the thread: it holds the list of **events** (every message, tool call and result) and a **state** dictionary. State keys carry scope by prefix: no prefix is this session only, `user:` is shared by all sessions of the same user, `app:` by all users of the app, and `temp:` lasts only for the current invocation and is never saved. State changes are written as `state_delta` on events, which is how the runner persists them. Session services: `InMemorySessionService` for tests and `DatabaseSessionService` for production, which stores sessions, events and state in any SQLAlchemy database, for example your own PostgreSQL with `DatabaseSessionService(db_url="postgresql+asyncpg://...")`. On Google Cloud, `VertexAiSessionService` stores them in Agent Platform Sessions on Gemini Enterprise Agent Platform (formerly Vertex AI) instead. Checked with `google-adk` 2.15.0:

```python
import asyncio
from google.adk.events import Event, EventActions
from google.adk.sessions import InMemorySessionService
from google.genai import types

async def main():
    sessions = InMemorySessionService()
    s1 = await sessions.create_session(app_name="dq_agent", user_id="priya",
                                       state={"user:team": "payments", "draft_sql": ""})
    # what the runner does after each turn: append an event carrying a state delta
    await sessions.append_event(s1, Event(
        author="dq_agent", invocation_id="inv-1",
        content=types.Content(role="model", parts=[types.Part(text="Noted.")]),
        actions=EventActions(state_delta={"user:billing_project": "eu-billing",
                                          "temp:scratch": "x", "draft_sql": "SELECT 1"})))
    s1 = await sessions.get_session(app_name="dq_agent", user_id="priya", session_id=s1.id)
    print(s1.state)
    s2 = await sessions.create_session(app_name="dq_agent", user_id="priya")
    print(s2.state)

asyncio.run(main())
# {'draft_sql': 'SELECT 1', 'user:team': 'payments', 'user:billing_project': 'eu-billing'}
# {'user:team': 'payments', 'user:billing_project': 'eu-billing'}
```

The `temp:` key was dropped, the session key `draft_sql` stayed in session 1, and the `user:` keys appear in a brand new session for the same user. In an agent, tools change state through `tool_context.state["user:billing_project"] = ...` and instructions can read it with `{user:billing_project}` templating; the runner turns both into event deltas.

> **Pitfall:** Mutating `session.state` directly on a session object you fetched outside a tool or callback. The change is not recorded as an event, so it is not persisted and other workers never see it. Change state through the context objects or an appended event.

::: deeper
**Short-term versus long-term.** The boundary is the thread. Short-term memory is scoped to one thread and usually deleted or archived when it ends. Long-term memory is scoped to a user, team or app and outlives threads (next group). ADK's `user:` and `app:` state prefixes and LangGraph's store blur the line slightly: they are cross-thread key-value state, a simple form of long-term memory.

**Checkpoint storage.** Each checkpoint stores the full state, so long threads with large messages create a lot of data. Durable checkpointers store channel values once and reference them, and you should set retention (delete threads older than N days, keep only the last K checkpoints of finished threads). Treat checkpoint tables like any other operational table: indexes on thread ID, partitioning or TTL, and backups.

**Time travel and forking.** Because every checkpoint is kept, you can resume from an earlier one (LangGraph: pass a `checkpoint_id` in the config, or `update_state` to edit state and continue). This is how you debug ("what if the agent had chosen the other table?") and how human-in-the-loop edits work ([Agent design patterns](#patterns)).

**Concurrency.** Two requests on the same thread at once can both load step 5 and both write step 6. Serialise work per thread (a queue keyed by thread, or a lock), or use the checkpointer's own versioning to detect the conflict. The same issue appears with Kafka consumers sharing a partition.

**Durable execution.** Checkpointing after each step is what makes long agent runs restartable. Some teams go further and run the agent loop on a durable execution engine (Temporal, for example), which records every activity result and replays the loop deterministically after a crash. Either way, make tool calls idempotent: after a restart, the step that crashed may run again.

> **Interview:** "How do you persist conversation state for an agent that runs on many servers?" Each conversation is a thread with an ID. After every step the full state (messages plus typed fields) is written as a checkpoint to a durable store keyed by thread ID; any worker loads the latest checkpoint to continue. In LangGraph that is a checkpointer such as the Postgres one with `thread_id` in the config; in ADK it is `DatabaseSessionService` on the same PostgreSQL. I would add retention on old threads, serialise concurrent requests per thread, and make tools idempotent so a replayed step is safe.

> **Interview:** "What is a reducer in LangGraph state?" A function that says how a node's update to a field merges with the current value. `add_messages` appends new messages and replaces ones with the same ID; `operator.add` concatenates lists; without a reducer the update overwrites. Reducers matter when parallel nodes write the same field.
:::

::: check
?? What is the difference between a thread and a checkpoint?
A thread is the identity of one conversation or run; a checkpoint is a saved snapshot of that thread's state after one step. A thread has many checkpoints.

?? In ADK, what happens to a state key named `temp:rows` at the end of the invocation, and to `user:team`?
`temp:rows` is discarded and never persisted; `user:team` is saved and visible in every session of the same user in that app.

?? Why keep typed fields in the state instead of relying on the message history?
Code can read them for routing and checks, they cannot be lost when history is trimmed or summarised, and you choose whether the model sees them.
:::

::: resources
- Docs: [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence) · threads, checkpoints, state history and time travel
- Docs: [Short-term memory in LangChain](https://docs.langchain.com/oss/python/langchain/short-term-memory) · checkpointers with `create_agent`, trimming and summarising
- Docs: [ADK sessions](https://adk.dev/sessions/) · sessions, events and session services
- Docs: [ADK state](https://adk.dev/sessions/state/) · `user:`, `app:` and `temp:` prefixes and how state is saved
- Repo: [langgraph-checkpoint-postgres](https://github.com/langchain-ai/langgraph/tree/main/libs/checkpoint-postgres) · the PostgreSQL checkpointer and store, setup and connection options
:::

## Memory

Short-term memory ends with the thread. Long-term memory is what lets an agent know, in a new conversation next month, that this user always queries the EU billing project, that `orders_daily` was late three times this quarter for the same reason, and that answers to this team should show the SQL first. It is a store outside the model, written during or after conversations and read back when relevant.

The first topic is the design: the three kinds of long-term memory, how memories are written, recalled, updated and forgotten, built from scratch. The second is the products and frameworks you will actually use: the Claude memory tool, LangGraph's store on PostgreSQL with pgvector, ADK memory services, and Mem0 self-hosted with a local model.

### Long-term memory: semantic, episodic and procedural

::: plain
A good colleague remembers three kinds of things about working with you. Facts: you are on the payments team and prefer SQL to charts. Events: last month's incident, when the provider's file arrived late. Habits: how you like a handover note written. Agents need the same three, written down somewhere they can look them up, kept up to date when facts change, and cleared out when they no longer matter.
:::

**What it is.** Long-term memory is information an agent keeps across threads, scoped to a user, a team or the whole application, and brought back into the context when it is relevant. It is not in the model's weights; it is data in a store your application owns, which means you can inspect it, correct it, secure it and delete it.

**The three kinds.** The labels come from cognitive science and were adopted for agents by the CoALA paper (Cognitive Architectures for Language Agents, Sumers et al., 2023):

| Kind | Holds | Example | Typical storage | How it is used |
|---|---|---|---|---|
| Semantic | Facts about the user, the domain, the world | "Priya's billing project is eu-billing" | Key-value profile, or documents with embeddings | Recalled by key or similarity, added to context |
| Episodic | Past events and experiences | "orders_daily was late on 3 Nov because the PSP file came after 06:00" | Timestamped records with embeddings | Recalled by similarity to the current situation, as examples |
| Procedural | How to do things: rules, preferences about behaviour, learned instructions | "Show the SQL first, then two lines of explanation" | Text in the system prompt, or a rules file | Always loaded, or loaded per task type |

Semantic memory answers "what is true?", episodic memory "what happened last time?", and procedural memory "how should I act?". Procedural memory is often just the prompt: an agent that rewrites its own instructions from feedback (as in the Reflexion work in [Agent design patterns](#patterns)) is updating procedural memory.

[[fig:memory_types]]

**The lifecycle.** Every memory system, from a JSON file to a managed service, does the same four things:

1. **Write.** Decide what is worth remembering. Either the agent calls a memory tool during the conversation ("hot path"), or a background job extracts memories from finished conversations ("background"). Extraction is a model call that turns a transcript into a few short, self-contained statements.
2. **Consolidate.** Compare each candidate with existing memories: add it if new, update the old memory if it changed, ignore it if it duplicates, delete the old one if it is now wrong. Without this, the store fills with contradictions.
3. **Recall.** Before (or during) a step, retrieve the few memories relevant to the current situation and add them to the context. Scoring usually combines relevance, recency and importance.
4. **Forget.** Expire memories that are stale or unused, honour deletion requests, and cap the store's size.

**From scratch.** The store below keeps memories per user, keyed by kind and key. A semantic fact with an existing key is updated (the old value is kept for audit). Recall scores each memory by relevance (word overlap), recency (exponential decay since last use) and importance, the three signals used in the Generative Agents paper (Park et al., 2023). Forgetting drops episodic memories that have not been used for 180 days.

```python
import json, math, re

def words(text):
    return set(re.findall(r"[a-z0-9_]+", text.lower())) - {"the", "a", "for", "of", "to", "is", "on", "in"}

class MemoryStore:
    def __init__(self):
        self.items = {}                      # (user, kind, key) -> memory
    def write(self, user, mem, now):
        k = (user, mem["kind"], mem["key"])
        old = self.items.get(k)
        if old and mem["kind"] == "semantic" and old["text"] != mem["text"]:
            mem["replaces"] = old["text"]    # a fact changed: keep the newest, record the old one
        self.items[k] = {**mem, "created": now, "last_used": now}
    def recall(self, user, query, now, k=3, half_life_days=30):
        q = words(query)
        scored = []
        for (u, kind, key), m in self.items.items():
            if u != user:
                continue                     # memories are scoped per user
            relevance = len(q & words(m["text"])) / (len(q | words(m["text"])) or 1)
            age = now - m["last_used"]
            recency = 0.5 ** (age / half_life_days)            # exponential decay
            score = 3.0 * relevance + 0.3 * recency + 0.2 * m["importance"]
            scored.append((round(score, 2), kind, m["text"]))
        top = sorted(scored, reverse=True)[:k]
        for _, kind, text in top:                               # recalling refreshes a memory
            for m in self.items.values():
                if m["text"] == text:
                    m["last_used"] = now
        return top
    def forget(self, now, max_idle_days=180):
        stale = [k for k, m in self.items.items()
                 if k[1] == "episodic" and now - m["last_used"] > max_idle_days]
        for k in stale:
            del self.items[k]
        return len(stale)

def mock_extract(conversation):
    """Stand-in for a model call that turns a finished conversation into candidate memories."""
    return json.dumps([
        {"kind": "semantic", "key": "billing_project", "importance": 0.8,
         "text": "Priya runs orders queries in the eu-billing project"},
        {"kind": "episodic", "key": "2026-11-03-orders-late", "importance": 0.6,
         "text": "orders_daily was late on 2026-11-03 because the PSP file arrived after 06:00"},
        {"kind": "procedural", "key": "answer_style", "importance": 0.9,
         "text": "Show the SQL first, then a two-line explanation"},
    ])

store = MemoryStore()
for mem in json.loads(mock_extract("...")):
    store.write("priya", mem, now=0)                            # day 0
store.write("priya", {"kind": "semantic", "key": "billing_project", "importance": 0.8,
                      "text": "Priya runs orders queries in the us-billing project"}, now=40)

for score, kind, text in store.recall("priya", "why is orders_daily late again", now=41):
    print(score, kind, text)
# 0.64 episodic orders_daily was late on 2026-11-03 because the PSP file arrived after 06:00
# 0.45 semantic Priya runs orders queries in the us-billing project
# 0.3 procedural Show the SQL first, then a two-line explanation
print(store.items[("priya", "semantic", "billing_project")]["replaces"])
# Priya runs orders queries in the eu-billing project
print("forgotten:", store.forget(now=400))
# forgotten: 1
```

What to notice:

- **Keys make updates possible.** Because the billing fact has a stable key, the day-40 correction replaces the day-0 value instead of adding a second, contradictory memory. Free-text memories without keys need a model call to decide whether a new statement updates an old one; that is what consolidation in Mem0 and similar services does.
- **The question about lateness pulls the episode first.** Relevance dominates; recency and importance break ties. Tune the weights on real queries, not by intuition.
- **Scope is enforced in code.** The `if u != user` line is the most important line in the class. Memory leaking between users is a privacy incident, not a quality bug.
- **Forgetting is a policy.** Here only episodes expire. Semantic facts are replaced rather than aged out; procedural rules are reviewed by people.

> **Pitfall:** Remembering everything. If every message becomes a memory, recall returns noise, the store grows without limit, and the agent starts quoting stale trivia. Extract a few durable, self-contained statements per conversation, and prefer updating over adding.

> **Tip:** Write memories as complete sentences that make sense alone ("Priya runs orders queries in the eu-billing project"), not fragments ("eu-billing"). They will be read months later, out of context, by a model that has never seen the original conversation.

::: deeper
**Hot path versus background writing.** Writing in the hot path (the agent calls `save_memory` mid-conversation) makes memories available immediately and lets the agent decide what matters, but it adds latency and tool-call clutter, and the agent may save too much or too little. Background extraction (after the session ends, or after N turns) keeps the conversation fast and can use a careful prompt and a cheaper model, but memories arrive late. Mem0 and managed services such as Agent Platform Memory Bank on Google Cloud run extraction asynchronously for this reason. Many systems do both: explicit "remember this" requests in the hot path, everything else in the background.

**Consolidation is the hard part.** Given an existing set and a new candidate, the system must choose add, update, delete or no-op. The Mem0 paper (Chhikara et al., 2025) describes exactly this: retrieve similar existing memories, then ask a model to choose one of those operations. Consolidation also resolves time: "Priya moved to the platform team" should end the old team fact, not sit beside it. Some systems keep validity intervals (valid from, valid to) so the agent can answer "what was true in March?".

**MemGPT and the operating-system analogy.** MemGPT (Packer et al., 2023) treats the context window as RAM and external stores as disk, and gives the model tools to page information in and out (core memory it edits in place, recall memory of past messages, archival memory searched by similarity). The idea survives in Letta (the project's successor) and in file-based memory tools: the model manages its own memory with explicit operations.

**Memory is an attack surface.** Anything written to memory is replayed into future prompts, so a prompt injection that gets saved (from a web page or a malicious ticket) persists across sessions. This is called memory poisoning. Validate and filter before writing, record the source of each memory, let users view and delete theirs, and never let content from untrusted tools write procedural memory without review.

**Privacy and deletion.** Memories about people are personal data. You need per-user scoping, encryption, retention limits, an export and a deletion path that also covers embeddings and backups. Design the store like any other table holding personal data, with lineage back to the conversation that produced each memory.

**Evaluating memory.** Benchmarks such as LoCoMo and LongMemEval test recall of facts across long, multi-session conversations, including updates and temporal questions. For your own system, build test cases: a fact stated in session 1, changed in session 3, asked in session 5. Check that the agent answers with the latest value and does not leak another user's memories.

> **Interview:** "Explain semantic, episodic and procedural memory for agents." Semantic memory is facts (user profile, domain knowledge), usually key-value or embedded documents recalled by key or similarity. Episodic memory is past events and experiences, timestamped and recalled by similarity to the current situation, often as few-shot examples. Procedural memory is how to act: rules and instructions, usually in the system prompt, updated from feedback. All three are stored outside the model and must be written, consolidated, recalled and forgotten.

> **Interview:** "How do you stop long-term memory from filling with contradictions?" Consolidate on write: retrieve similar existing memories and decide add, update, delete or no-op (with a model call when keys are not available); use stable keys for profile facts; keep validity timestamps; and run periodic cleanup that merges duplicates and expires unused episodes. Recall should prefer the newest value when two memories conflict.
:::

::: check
?? A user says "from now on, put the SQL first". Which kind of memory is that, and where would you store it?
Procedural memory (how to behave). Store it as a rule scoped to that user and load it into the system prompt or instructions of their future sessions.

?? Name three signals commonly combined to rank memories at recall time.
Relevance to the current query (similarity), recency (time since created or last used, with decay) and importance (a score assigned when the memory was written).

?? Why is writing a web page's content straight into memory risky?
A prompt injection in the page would be replayed into every future session that recalls it (memory poisoning). Filter and validate before writing and track each memory's source.
:::

::: resources
- Paper: [Cognitive Architectures for Language Agents (CoALA)](https://arxiv.org/abs/2309.02427) · the semantic, episodic and procedural framing for agents
- Paper: [Generative Agents](https://arxiv.org/abs/2304.03442) · memory streams with relevance, recency and importance, plus reflection
- Paper: [MemGPT: Towards LLMs as Operating Systems](https://arxiv.org/abs/2310.08560) · the context window as RAM, external memory as disk
- Paper: [Mem0: Building Production-Ready AI Agents with Scalable Long-Term Memory](https://arxiv.org/abs/2504.19413) · extraction and add, update, delete consolidation
- Docs: [Memory overview](https://docs.langchain.com/oss/python/concepts/memory) · LangChain's guide to short-term and long-term memory types
- Blog: [LLM Powered Autonomous Agents](https://lilianweng.github.io/posts/2023-06-23-agent/) · Lilian Weng's survey, including the memory component
:::

### Memory systems in practice

::: plain
Once you know what to remember, you still need a filing cabinet. Some cabinets are folders of notes the agent edits itself, some are databases you query by key or by meaning, and some are services that read your conversations and file the important bits for you. They differ in who decides what to keep: the agent, your code, or the service.
:::

**Four designs you will meet.** Real memory systems fall into a few shapes, distinguished by who writes and how memories are found:

| Design | Who writes | How recalled | Examples |
|---|---|---|---|
| File memory the agent edits | The agent, through a tool | The agent reads files it chooses | Claude memory tool, `CLAUDE.md`-style notes files |
| Key-value and document store | Your code or the agent's tools | By key, filter or vector similarity | LangGraph store on PostgreSQL with pgvector, Redis, a plain Postgres table |
| Library or server with extraction | A model call made by the library, from conversation messages | Similarity search scoped to a user | Mem0 open source (self-hosted), LangMem, Letta |
| Managed extraction service | The provider, from conversation events | Similarity search scoped to a user | Mem0 platform; on Google Cloud, Agent Platform Memory Bank |

The rest of this topic shows the first three, all of which you can run on your own servers.

**The Claude memory tool.** Anthropic provides a client-side tool, type `memory_20250818` with name `memory`. When it is in the request, Claude checks a `/memories` directory before starting and records progress and learnings as it works, using commands `view`, `create`, `str_replace`, `insert`, `delete` and `rename`. Your application runs those commands against storage you control and returns the results as `tool_result` blocks; the tool is generally available and needs no beta header. Because it is your code that touches storage, your code must make sure no path escapes the memory root. The handler below implements four commands with a path check and replays a scripted session of tool calls:

```python
import json, tempfile
from pathlib import Path

ROOT = Path(tempfile.mkdtemp()) / "priya"       # one directory per user in a real system
ROOT.mkdir(parents=True)

def safe_path(virtual):
    """Map /memories/... onto ROOT and refuse anything that escapes it."""
    if not virtual.startswith("/memories"):
        raise ValueError(f"path must start with /memories: {virtual}")
    real = (ROOT / virtual.removeprefix("/memories").lstrip("/")).resolve()
    if not real.is_relative_to(ROOT.resolve()):
        raise ValueError(f"path escapes the memory directory: {virtual}")
    return real

def memory_tool(command, path=None, file_text=None, old_str=None, new_str="", **_):
    """Client-side handler for the memory tool's file commands (view, create, str_replace, delete)."""
    p = safe_path(path)
    if command == "view":
        if p.is_dir():
            files = sorted(f.name for f in p.iterdir())
            return f"Directory {path}: " + (", ".join(files) or "(empty)")
        lines = p.read_text().splitlines()
        return "\n".join(f"{i:6d}\t{line}" for i, line in enumerate(lines, 1))
    if command == "create":
        p.write_text(file_text)
        return f"File created successfully at: {path}"
    if command == "str_replace":
        text = p.read_text()
        if text.count(old_str) != 1:
            raise ValueError(f"old_str must appear exactly once in {path}")
        p.write_text(text.replace(old_str, new_str))
        return "The memory file has been edited."
    if command == "delete":
        p.unlink()
        return f"Successfully deleted {path}"
    raise ValueError(f"unsupported command {command}")

# A scripted model session: check memory, save a preference, later correct it, then try to escape
calls = [
    {"command": "view", "path": "/memories"},
    {"command": "create", "path": "/memories/preferences.md",
     "file_text": "- billing project: eu-billing\n- answer style: SQL first\n"},
    {"command": "str_replace", "path": "/memories/preferences.md",
     "old_str": "eu-billing", "new_str": "us-billing"},
    {"command": "view", "path": "/memories/preferences.md"},
    {"command": "view", "path": "/memories/../../etc/passwd"},
]
for i, call in enumerate(calls):
    block = {"type": "tool_use", "id": f"tu_{i}", "name": "memory", "input": call}
    try:
        result = {"type": "tool_result", "tool_use_id": block["id"], "content": memory_tool(**block["input"])}
    except (ValueError, OSError) as exc:
        result = {"type": "tool_result", "tool_use_id": block["id"], "content": str(exc), "is_error": True}
    print(("ERROR " if result.get("is_error") else "") + result["content"])
# Directory /memories: (empty)
# File created successfully at: /memories/preferences.md
# The memory file has been edited.
#      1	- billing project: us-billing
#      2	- answer style: SQL first
# ERROR path escapes the memory directory: /memories/../../etc/passwd
```

With a real client, the tool definition is the whole configuration: `tools=[{"type": "memory_20250818", "name": "memory"}]`. The Python SDK (2.13.0) also ships `BetaAbstractMemoryTool` to subclass with your own storage and a ready-made `BetaLocalFilesystemMemoryTool(base_path=...)` that works with `client.beta.messages.tool_runner`. The memory tool pairs naturally with context editing: clear old tool results, but exclude `memory`, so the agent's notes survive while bulky results go.

**LangGraph stores.** A **store** is LangGraph's long-term memory: JSON documents organised by a **namespace** tuple (such as `("users", user_id, "memories")`) and a key, with optional semantic search when you configure an index with an embedding function. Nodes get it through the `Runtime`. The example below uses a toy hashing embedding so it runs offline (checked with `langgraph` 1.2.24):

```python
import hashlib, math, re
from dataclasses import dataclass
from typing import TypedDict
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph, START, END
from langgraph.runtime import Runtime
from langgraph.store.memory import InMemoryStore

STOP = {"the", "a", "for", "on", "then", "which", "of", "to", "is", "in"}
def toy_embed(texts):
    """Hashing-trick embedding (64 buckets, unit length). Stands in for a real embedding model."""
    out = []
    for text in texts:
        v = [0.0] * 64
        for w in re.findall(r"[a-z0-9_-]+", text.lower()):
            if w not in STOP:
                v[int(hashlib.md5(w.encode()).hexdigest(), 16) % 64] += 1.0
        n = math.sqrt(sum(x * x for x in v)) or 1.0
        out.append([x / n for x in v])
    return out

store = InMemoryStore(index={"dims": 64, "embed": toy_embed, "fields": ["text"]})
ns = ("users", "priya", "memories")                    # namespace: who the memories belong to
store.put(ns, "billing", {"text": "uses the eu-billing project for orders queries", "kind": "semantic"})
store.put(ns, "oncall", {"text": "on call for the payments pipeline on fridays", "kind": "semantic"})
store.put(ns, "style", {"text": "wants SQL first then a short explanation", "kind": "procedural"})
store.put(("users", "tom", "memories"), "billing", {"text": "uses the us-billing project", "kind": "semantic"})

for item in store.search(ns, query="which billing project for orders", limit=2):
    print(item.key, round(item.score, 2), item.value["text"])
# billing 0.65 uses the eu-billing project for orders queries
# oncall 0.0 on call for the payments pipeline on fridays
print(store.search(ns, filter={"kind": "procedural"})[0].value["text"])
# wants SQL first then a short explanation
print(len(store.search(("users",), query="billing project")))   # a prefix search spans users
# 4

# ---- inside a graph: recall before answering, remember explicit rules ----
@dataclass
class Ctx:
    user_id: str

class S(TypedDict):
    question: str
    memories: list[str]

def recall(state: S, runtime: Runtime[Ctx]) -> dict:
    hits = runtime.store.search(("users", runtime.context.user_id, "memories"),
                                query=state["question"], limit=2)
    return {"memories": [h.value["text"] for h in hits if h.score > 0.3]}

def remember(state: S, runtime: Runtime[Ctx]) -> dict:
    if "from now on" in state["question"]:
        runtime.store.put(("users", runtime.context.user_id, "memories"), "rule",
                          {"text": state["question"], "kind": "procedural"})
    return {}

g = StateGraph(S, context_schema=Ctx)
g.add_node("recall", recall); g.add_node("remember", remember)
g.add_edge(START, "recall"); g.add_edge("recall", "remember"); g.add_edge("remember", END)
app = g.compile(checkpointer=InMemorySaver(), store=store)    # short-term and long-term memory

out = app.invoke({"question": "which billing project do I use for orders?"},
                 {"configurable": {"thread_id": "a"}}, context=Ctx(user_id="priya"))
print(out["memories"])
# ['uses the eu-billing project for orders queries']
app.invoke({"question": "from now on, always add LIMIT 100", "memories": []},
           {"configurable": {"thread_id": "b"}}, context=Ctx(user_id="priya"))
print(store.get(("users", "priya", "memories"), "rule").value["text"])
# from now on, always add LIMIT 100
```

The checkpointer holds each thread; the store holds what crosses threads. The user ID comes from the runtime context set by your application, never from the model, so one user cannot ask for another's namespace. Notice the prefix search: searching `("users",)` returned all four memories, including Tom's. Always search the full user namespace. LangMem is a LangChain library that adds memory extraction and consolidation tools on top of any store.

**The same graph on your own servers.** In production both kinds of memory usually live in one PostgreSQL database. `PostgresSaver` holds the threads; `PostgresStore` holds the long-term memories as JSON rows and, when you give it an `index`, keeps their embeddings in a pgvector column with an HNSW index. The embedding function is the only piece that changes: instead of the toy hashing function, call a local embedding model. The one below calls Ollama's `/api/embed` endpoint with `nomic-embed-text` (768 dimensions), so no text leaves your network; a `sentence-transformers` model loaded in the same process works too. Import-checked with `langgraph-checkpoint-postgres` 3.2.1; running it needs PostgreSQL with the `vector` extension available and an Ollama server:

```python
import json, os, urllib.request
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.store.postgres import PostgresStore
from langgraph.store.postgres.base import PostgresIndexConfig

DB_URI = os.environ["AGENT_DB_URI"]          # postgresql://agent:...@pg:5432/agent, from your secret store

def local_embed(texts):
    """Embed with a local model served by Ollama (after: ollama pull nomic-embed-text)."""
    body = json.dumps({"model": "nomic-embed-text", "input": list(texts)}).encode()
    req = urllib.request.Request("http://ollama:11434/api/embed", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)["embeddings"]

index: PostgresIndexConfig = {"dims": 768, "embed": local_embed, "fields": ["text"],
                              "ann_index_config": {"kind": "hnsw"}, "distance_type": "cosine"}

with PostgresSaver.from_conn_string(DB_URI) as checkpointer, \
     PostgresStore.from_conn_string(DB_URI, index=index) as store:
    checkpointer.setup()                     # run once (for example in a migration job): creates tables
    store.setup()                            # creates the store tables, the vector column and the HNSW index
    app = g.compile(checkpointer=checkpointer, store=store)    # g: the graph built above, unchanged
    app.invoke({"question": "which billing project do I use for orders?"},
               {"configurable": {"thread_id": "a"}}, context=Ctx(user_id="priya"))
```

Three practical notes. `store.setup()` runs `CREATE EXTENSION vector`, so either let the migration role create it or have a DBA install pgvector first. The `dims` value must match the model; changing the embedding model means a new index and a re-embed of every memory, like any backfill. And because memories are ordinary rows, your usual PostgreSQL tooling applies: backups, row counts per namespace, a `DELETE` for a user's right to erasure, and replication to a standby.

**ADK memory services.** ADK separates the session (short-term) from a **memory service** (long-term). You call `add_session_to_memory(session)` when a session is worth keeping, and the agent reads memory through the built-in `load_memory` tool (the model decides when to search) or `preload_memory` (searched automatically before each turn). `InMemoryMemoryService` does keyword matching for tests; `SqliteMemoryService` keeps the same raw events in a local SQLite file with full-text search, which is enough for one server. For a shared, self-hosted memory you implement `BaseMemoryService` yourself (two methods, `add_session_to_memory` and `search_memory`) over Mem0 or a pgvector table. Checked offline with `google-adk` 2.15.0:

```python
import asyncio
from google.adk.agents import Agent
from google.adk.events import Event
from google.adk.memory import InMemoryMemoryService
from google.adk.sessions import InMemorySessionService
from google.adk.tools import load_memory
from google.genai import types

async def main():
    sessions, memory = InMemorySessionService(), InMemoryMemoryService()
    s = await sessions.create_session(app_name="dq_agent", user_id="priya")
    for author, role, text in [("user", "user", "Always use the EU billing project for orders queries"),
                               ("dq_agent", "model", "Noted: EU billing project for orders.")]:
        await sessions.append_event(s, Event(author=author, invocation_id="inv-1",
            content=types.Content(role=role, parts=[types.Part(text=text)])))
    s = await sessions.get_session(app_name="dq_agent", user_id="priya", session_id=s.id)
    await memory.add_session_to_memory(s)               # after the session: keep it
    hits = await memory.search_memory(app_name="dq_agent", user_id="priya", query="billing project")
    for m in hits.memories:
        print(m.author, "|", m.content.parts[0].text)

asyncio.run(main())
# user | Always use the EU billing project for orders queries
# dq_agent | Noted: EU billing project for orders.

agent = Agent(name="dq_agent", model="a-current-gemini-model-id",  # on-prem: LiteLlm(model="ollama_chat/<model>")
              instruction="Use load_memory to check the user's preferences before writing SQL.",
              tools=[load_memory])
# Runner(app_name="dq_agent", agent=agent, session_service=sessions, memory_service=memory)
# One server: memory = SqliteMemoryService(db_path="/var/lib/agent/memory.db")
```

The in-memory service stores raw events, which is why both turns come back. An extraction-based memory (Mem0, below, behind a custom `BaseMemoryService`) would instead store a statement such as "The user prefers the EU billing project for orders queries", and update it if the user later changes their mind. On Google Cloud, `VertexAiMemoryBankService` does the same with Agent Platform Memory Bank, a managed extraction and consolidation service.

**Mem0 self-hosted.** Mem0 is an open-source memory layer (Apache 2.0, with an optional hosted platform) that does extraction and consolidation for you: `add` sends conversation messages, a model extracts short facts and decides add, update or delete against the user's existing memories, and `search` returns the relevant ones. Every part is pluggable, so the whole thing can run inside your network: the extraction model on Ollama or vLLM, the embedder on Ollama or a Hugging Face model, and the vectors in pgvector, Qdrant, Milvus, OpenSearch or Redis. The configuration below was validated with `mem0ai` 2.2.1; the commented calls need Ollama and PostgreSQL running:

```python
import os
os.environ["MEM0_TELEMETRY"] = "False"             # on-prem: no anonymous usage events sent out
from mem0 import Memory
from mem0.configs.base import MemoryConfig

OLLAMA = "http://ollama:11434"
config = {
    "llm": {"provider": "ollama",                  # extraction and consolidation, on a local model
            "config": {"model": "qwen3:8b", "temperature": 0, "ollama_base_url": OLLAMA}},
    "embedder": {"provider": "ollama",
                 "config": {"model": "nomic-embed-text", "ollama_base_url": OLLAMA}},
    "vector_store": {"provider": "pgvector",       # or "qdrant" with host and port
                     "config": {"connection_string": os.environ.get("MEM0_DB_URI", "postgresql://..."),
                                "collection_name": "agent_memories", "embedding_model_dims": 768}},
    "history_db_path": "/var/lib/agent/mem0_history.db",   # audit trail of every add, update, delete
}
print(type(MemoryConfig(**config).vector_store.config).__name__)
# PGVectorConfig

# memory = Memory.from_config(config)
# memory.add([{"role": "user", "content": "I run orders queries in the eu-billing project."},
#             {"role": "assistant", "content": "Got it, eu-billing for orders."}],
#            user_id="priya", metadata={"source": "oncall-chat"})
# hits = memory.search("which billing project for orders?", filters={"user_id": "priya"}, top_k=3)
# for item in hits["results"]:
#     print(item["memory"], item["score"])
```

Two API details in recent versions: `search` takes the scope in `filters` (at least one of `user_id`, `agent_id` or `run_id`) and the count as `top_k`, and `add(..., infer=False)` stores messages as they are, without the extraction call. A small local model is usually enough for extraction, but test it on your own conversations: extraction quality decides what the agent will remember.

**Choosing.** Start with what you already run. For a LangGraph app, `PostgresStore` with pgvector next to the checkpointer. For an ADK app, `DatabaseSessionService` on PostgreSQL plus a memory service over Mem0 or pgvector. For a Claude agent working on long tasks, the memory tool with your own storage. Reach for Mem0 when you need extraction and consolidation across many users and do not want to build them. Redis fits hot, short-lived state (session caches, rate counters) better than long-term memory you must audit. Whatever you choose, keep a per-user scope enforced in code, a way to list and delete a user's memories, and an evaluation set of multi-session conversations.

> **Pitfall:** Trusting the model to choose the namespace. If a tool takes `user_id` as an argument, a prompt injection can read or write another user's memories. Bind the scope from the authenticated session in your code and expose only "my memories" to the model.

> **Tip:** Give each memory a source (thread ID, message ID, extraction run) and a timestamp. When a user asks "why do you think I use the US project?", you can show where it came from, and you can delete everything derived from a bad conversation.

::: deeper
**File memory versus vector memory.** File memory (the agent reads and edits notes) is transparent, easy for people to review and correct, and works well for project state and procedures, but it relies on the agent to organise it and it does not scale to thousands of facts per user. Vector memory scales and finds memories by meaning, but it is opaque, needs extraction and consolidation, and can return similar but wrong memories. Many systems use both: a small, curated notes file always loaded, and a searchable store for the long tail.

**Memory and retrieval are the same machinery.** Recalling memories by similarity is RAG over a corpus of memories. Everything in the next group (chunking, hybrid search, reranking, evaluation) applies. The differences are that memories are written by the system, change often, are scoped per user, and must be updated and deleted, which makes consolidation and governance the hard parts.

**Latency and cost.** Recall before every turn adds a search (tens of milliseconds) and tokens; extraction adds a model call per session. Run extraction in the background, cache the user's profile per session, and cap recalled memories to a handful.

**Version notes.** ADK 2.15 tool contexts expose `search_memory` and `add_memory`, so custom tools can read and write memory directly through whichever memory service the runner has. The Claude memory tool helpers live in the SDK's beta namespace even though the tool itself is generally available. LangGraph stores support TTLs on items (`ttl` on `put`) for automatic forgetting.

> **Interview:** "Design long-term memory for an internal data assistant used by 2,000 engineers." Short-term: threads in a durable checkpointer. Long-term: per-user namespace in a store (PostgreSQL with pgvector, embeddings from a local model), holding profile facts (team, projects, preferences) with stable keys and episodic notes from past incidents. Background extraction after each session with consolidation (add, update, delete), a handful of memories recalled per turn by relevance and recency, team-level procedural rules reviewed by people. Scope bound from authentication, sources and timestamps on every memory, user-visible list and delete, TTLs, and an evaluation set of multi-session scenarios including updates and cross-user leakage tests.

> **Interview:** "How does the Claude memory tool work?" It is a client-side tool (`memory_20250818`). Claude issues file commands (view, create, str_replace, insert, delete, rename) under `/memories`; your application executes them against your own storage and returns results. Claude checks memory at the start of a task and records progress as it goes, which lets long tasks survive context resets. You must validate every path to keep operations inside the memory directory, and you choose where the files live, how big they can get and when they expire.
:::

::: check
?? In LangGraph, what is the difference between a checkpointer and a store?
The checkpointer saves the state of one thread step by step (short-term memory); the store holds JSON documents by namespace and key across threads (long-term memory), optionally with semantic search.

?? In ADK, what is the difference between `load_memory` and `preload_memory`?
`load_memory` is a tool the model calls when it decides to search memory; `preload_memory` searches memory automatically before each turn and adds the results to the request.

?? Why should a memory tool never accept the user ID from the model?
A prompt injection or a confused model could read or write another user's memories. The scope must come from the authenticated session in your code.
:::

::: resources
- Docs: [Memory tool](https://platform.claude.com/docs/en/agents-and-tools/tool-use/memory-tool) · commands, handler helpers and path traversal protection
- Docs: [Long-term memory in LangChain](https://docs.langchain.com/oss/python/langchain/long-term-memory) · stores, namespaces and reading and writing in tools
- Docs: [ADK memory](https://adk.dev/sessions/memory/) · memory services, `load_memory` and `preload_memory`
- Docs: [Mem0 open source](https://docs.mem0.ai/open-source/overview) · the self-hosted library, its configuration and supported model and vector store providers
- Repo: [mem0ai/mem0](https://github.com/mem0ai/mem0) · source code, self-hosted server and examples
- Docs: [LangMem](https://langchain-ai.github.io/langmem/) · memory extraction and management tools for LangGraph
- Repo: [Memory cookbook](https://github.com/anthropics/claude-cookbooks/tree/main/tool_use/memory_cookbook.ipynb) · Anthropic's worked example of the memory tool with context editing
:::

## Retrieval

Memory is what the agent learned from its own conversations. Retrieval brings in knowledge it never saw: your runbooks, table documentation, incident reviews, policies and tickets. **Retrieval-augmented generation (RAG)** retrieves the passages relevant to a question and puts them in the context, so the model answers from your documents instead of from its training data, and can cite them.

The data engineering course covers the infrastructure in depth: embedding pipelines, vector databases and ANN index internals, and production RAG pipelines ([AI and ML for data engineers](index.html#aiml)). This group is about the agent side. It starts with the fundamentals from an agent builder's view, then moves control to the agent (agentic RAG), shows how to get and prove good retrieval quality (hybrid search, reranking, metrics), adds graphs for questions that vectors handle badly (GraphRAG), and ends with the self-hosted vector databases an agent's retrieval tools run on.

### RAG fundamentals for agents

::: plain
An open-book exam. The student does not need to have memorised the textbook; they need to find the right page quickly, read it, and write an answer that points to the page. RAG gives a model the same deal: a librarian (the retriever) fetches the few pages that matter, the model writes the answer from them, and every claim carries a page number you can check.
:::

**What it is.** A RAG system has an offline part and an online part. Offline, documents are split into **chunks**, each chunk is turned into an **embedding** (a vector that represents its meaning) and stored in an index with its text and metadata. Online, the question is embedded, the index returns the nearest chunks, the best are assembled into a prompt with instructions to answer only from them and to cite them, and the model's answer is checked.

[[fig:memory_rag]]

**Why agents need it.** Models do not know your private data, their knowledge has a cut-off date, and they fill gaps with plausible inventions. Retrieval fixes the first two and reduces the third, because an answer grounded in retrieved text can be checked against it. It is also cheaper than fine-tuning for knowledge, and updating knowledge means re-indexing a document, not retraining a model. Fine-tuning is for behaviour and format; retrieval is for facts.

**The steps and the choices that matter:**

| Step | Choice | Rule of thumb |
|---|---|---|
| Parse | PDF, HTML, Markdown, tables to clean text | Keep structure: headings, lists, table rows |
| Chunk | Fixed size, by structure, semantic | Split on structure (sections), a few hundred tokens, small overlap |
| Enrich | Add title, path, owner, date to each chunk | A chunk must make sense alone |
| Embed | Embedding model and dimensions | Same model for documents and queries; re-embed on change |
| Index | Exact or approximate (ANN), filters | Exact below about 100k chunks; ANN above ([AI and ML](index.html#aiml)) |
| Retrieve | Top-k, filters, hybrid | Retrieve 20 to 50 candidates, keep 3 to 8 after reranking |
| Assemble | Order, numbering, budget | Numbered sources with IDs, best first, question last |
| Generate | Instructions | Answer only from sources, cite each claim, say when unknown |
| Check | Citation validation, groundedness | Every citation exists; claims supported |

**From scratch.** The pipeline below indexes three small documents. Chunking splits on Markdown headings and prefixes each chunk with its document title, so a section called "Owner" still says whose owner it is. The embedding is a toy: a hashing trick over words and word pairs, normalised to unit length. It captures word overlap, not meaning, but the code around it is exactly what you would write with a real embedding model.

```python
import hashlib, math, re

DOCS = {   # small Markdown documents; "\n## " starts a section
    "runbook/orders_daily.md": "# orders_daily\n"
        "Loads PSP settlement files into raw.orders every day at 06:30 UTC.\n"
        "## Late files\nIf the PSP file arrives after 06:00 UTC the load waits up to two hours. "
        "After that the sensor fails.\n"
        "## Owner\nPayments data team, channel #payments-data.",
    "runbook/refunds_daily.md": "# refunds_daily\n"
        "Loads refunds into raw.refunds every day at 07:00 UTC.\n"
        "## Restatements\nRefunds are restated every Monday; expect row counts for the last 7 days to change.",
    "policy/retention.md": "# Retention\n"
        "Raw tables keep 400 days of partitions. Personal data columns are masked after 30 days.",
}

def chunk(doc_id, text):
    """Split on headings, so each chunk is one section, and keep the title for context."""
    title = text.splitlines()[0].lstrip("# ")
    sections = re.split(r"\n(?=## )", text)
    return [{"id": f"{doc_id}#{i}", "doc": doc_id, "text": f"{title}: {s.strip()}"}
            for i, s in enumerate(sections)]

STOP = {"the", "a", "an", "of", "to", "is", "in", "for", "and", "after", "every", "day", "at", "it", "if", "what", "when", "does", "why"}
def tokenize(text):
    return [w for w in re.findall(r"[a-z0-9_]+", text.lower()) if w not in STOP]

def embed(text, dims=256):
    """Toy embedding: hash each word (and word pair) into a bucket, then L2-normalise."""
    v = [0.0] * dims
    toks = tokenize(text)
    for t in toks + [a + " " + b for a, b in zip(toks, toks[1:])]:
        v[int(hashlib.md5(t.encode()).hexdigest(), 16) % dims] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]

def cosine(a, b):
    return sum(x * y for x, y in zip(a, b))            # both are unit vectors

# ---- offline: chunk and index ----
INDEX = [dict(c, vec=embed(c["text"])) for d, t in DOCS.items() for c in chunk(d, t)]

# ---- online: retrieve, build a grounded prompt, answer, check citations ----
def retrieve(query, k=2):
    q = embed(query)
    return sorted(INDEX, key=lambda c: cosine(q, c["vec"]), reverse=True)[:k]

def build_prompt(question, hits):
    sources = "\n".join(f"[{i}] ({h['doc']}) {h['text']}" for i, h in enumerate(hits, 1))
    return ("Answer only from the sources. Cite each claim as [n]. "
            "If the sources do not contain the answer, say so.\n\n"
            f"Sources:\n{sources}\n\nQuestion: {question}")

def mock_model(prompt):
    if "arrives after 06:00" in prompt:
        return "The load waits up to two hours for a late PSP file, then the sensor fails [1]."
    return "The sources do not say."

def check_citations(answer, hits):
    cited = {int(n) for n in re.findall(r"\[(\d+)\]", answer)}
    return {"cited": sorted(cited), "valid": all(1 <= n <= len(hits) for n in cited),
            "grounded": bool(cited) or "do not say" in answer}

question = "What happens when the PSP file for orders_daily is late?"
hits = retrieve(question)
print([h["id"] for h in hits])
# ['runbook/orders_daily.md#1', 'runbook/orders_daily.md#0']
answer = mock_model(build_prompt(question, hits))
print(answer)
# The load waits up to two hours for a late PSP file, then the sensor fails [1].
print(check_citations(answer, hits))
# {'cited': [1], 'valid': True, 'grounded': True}
```

Three habits to copy:

- **Chunks carry their context.** "Owner: Payments data team" is useless without "orders_daily". Prefixing the title (or a model-written one-line context, as Anthropic's contextual retrieval does) fixes the most common RAG failure: the right fact in a chunk that does not look relevant.
- **Sources are numbered and the model must cite them.** That turns "is this answer right?" into a checkable question: does source [1] say this?
- **Citations are checked in code.** A citation to a source that was not retrieved is a hallucination by definition. The groundedness check here is minimal; production systems add a model-based check that each sentence is supported by its cited passage.

**RAG as a tool.** In an agent, retrieval is usually not a fixed step before the model call but a tool the model calls: `search_docs(query)` ([Tool use and function calling](#tools) covers the tool interface and citations in provider APIs). The next topic is about that shift. The fundamentals above do not change: the tool runs the same pipeline and returns numbered, attributed passages.

> **Pitfall:** Chunks that are too small or too large. Tiny chunks lose context ("it waits two hours" for what?); huge chunks dilute the embedding and waste the context budget. Split by document structure first, then by size, and test chunk sizes on your evaluation set rather than copying a default.

> **Tip:** Store chunk text and metadata in a governed table (a PostgreSQL table, or an Iceberg table queried with Trino or Spark) as the source of truth, and treat the vector index as a rebuildable copy. Permissions, lineage and deletes then work the way your platform already handles them.

::: deeper
**Grounding and citations in provider APIs.** Claude can return citations natively when you pass documents as `document` content blocks with citations enabled; the response then contains text blocks with `citations` pointing to exact passages. Native citations are more reliable than parsing `[n]` from text, because the provider ties each citation to the source span. With an open-weight model served by vLLM or Ollama there is no such feature, so the numbered-sources prompt and the citation check above are what you use; check them in code every time.

**Real embeddings, locally.** Replace the toy `embed` with an embedding model you host. Two common routes: `sentence-transformers` loads a model such as `BAAI/bge-small-en-v1.5` (384 dimensions) inside your Python process, and Ollama serves models such as `nomic-embed-text` (768 dimensions) over HTTP at `/api/embed`, so several services share one model. Pick the model on your own evaluation set (the MTEB leaderboard is a starting list, not a verdict), record its name and version next to every vector, and use the same model for documents and queries.

**On Google Cloud.** RAG Engine and Agent Search (formerly Vertex AI Search) on Gemini Enterprise Agent Platform run this whole pipeline as a managed service, with grounding metadata for Gemini answers.

**Access control is a retrieval problem.** If the index contains documents a user may not read, the model will happily quote them. Filter at retrieval time by the user's permissions (metadata filters or per-tenant indexes), not after generation. This is the RAG version of row-level security.

**Freshness.** An index is a materialised view of your documents. It needs incremental updates (re-embed changed chunks, delete removed ones), a freshness SLO and monitoring, exactly like any derived table. Stale chunks produce confident, outdated answers.

**When not to use RAG.** If the whole corpus fits comfortably in the context window and changes rarely (one runbook of 30 pages), put it in the prompt and cache it. If the question is about numbers in a table, generate SQL and run it; retrieving text about numbers is the wrong tool. RAG fits large, changing, unstructured corpora.

**Research.** The term comes from Lewis et al. (2020), who combined a retriever with a sequence-to-sequence generator and trained them together. Today's RAG is usually simpler (a frozen retriever and a general model) but the idea is the same: non-parametric memory (the index) plus parametric memory (the model).

> **Interview:** "Walk me through a RAG pipeline and where it fails." Offline: parse, chunk by structure with context added, embed, index with metadata. Online: embed the query, retrieve candidates with filters (including permissions), rerank, assemble numbered sources within a budget, generate with instructions to answer only from sources and cite, then check citations. It fails at retrieval most often (the right chunk is not in the top k because of chunking, vocabulary mismatch or missing context), then at generation (the model ignores or misreads sources). So I evaluate retrieval separately (recall@k) from generation (faithfulness), and fix retrieval first.

> **Interview:** "RAG or fine-tuning for teaching a model our internal data?" RAG for knowledge: facts change, need citations and access control, and updating means re-indexing. Fine-tuning for behaviour: a format, a style, a narrow task done more cheaply by a smaller model. They combine: a fine-tuned model can still use retrieval.
:::

::: check
?? Why prefix each chunk with its document title or a short context?
So a chunk makes sense on its own. A section such as "Owner: payments team" carries no reference to the pipeline it describes; without the context its embedding does not match questions about that pipeline.

?? What is a cheap, deterministic check you can run on every RAG answer?
That every citation refers to a source that was actually retrieved (and, if you require citations, that there is at least one). A citation to a missing source is a hallucination.

?? Where should document permissions be enforced in RAG?
At retrieval time, with filters or separate indexes per tenant, so that unauthorised text never reaches the prompt.
:::

::: resources
- Paper: [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401) · Lewis et al., the original RAG paper
- Blog: [Introducing Contextual Retrieval](https://www.anthropic.com/news/contextual-retrieval) · adding context to chunks before embedding, with BM25 and reranking
- Docs: [Sentence Transformers](https://sbert.net/) · loading and running open embedding and reranking models locally
- Docs: [Embeddings in Ollama](https://docs.ollama.com/capabilities/embeddings) · serving embedding models over a local HTTP API
- Leaderboard: [MTEB](https://huggingface.co/spaces/mteb/leaderboard) · a starting list of open embedding models by task and language
- Course: [AI and ML for data engineers](index.html#aiml) · embedding pipelines, vector databases and RAG infrastructure in the data engineering course
:::

### Agentic RAG

::: plain
Classic RAG is a vending machine: one button press, one search, whatever comes out is your answer. Agentic RAG is a research assistant. It reads your question, searches, looks at what it found, notices it is not quite right, searches again with better words, follows a reference to a second document, and only then writes the answer, checking it against its sources before handing it over.
:::

**What it is.** In agentic RAG, the model controls retrieval. Retrieval becomes one or more tools, and the agent decides **whether** to retrieve (some questions need no documents), **what** to search for (rewriting the user's words into good queries), **where** (which index or tool: runbooks, tickets, the catalog, SQL), **how many times** (following a chain of facts across documents), and **when to stop** (it has enough, or it should say it does not know).

**Why it exists.** One-shot RAG fails in predictable ways:

| Failure | Example | Agentic fix |
|---|---|---|
| Vocabulary mismatch | User says "revenue table", docs say `finance.revenue_daily` | Query rewriting, retry with exact names |
| Multi-hop | "Who owns the job that feeds the revenue table?" needs two lookups | Iterative retrieval: search, read, search again |
| Wrong source | The answer is in the catalog, not the runbooks | Routing between retrieval tools |
| Irrelevant hits | Top results look similar but do not answer | Grade results, discard, retry or say unknown |
| Unsupported answer | Model adds facts not in the sources | Self-check before answering |

The cost is more model calls and more latency. Use it when questions are open, multi-step or messy; keep one-shot RAG for simple lookups ([Agent design patterns](#patterns) on workflows versus agents applies directly).

**From scratch.** The agent below has one tool, `search_docs`, that grades its own hits and returns `no_relevant_results` with a hint when nothing passes the threshold. The mock model, in the Messages API shape, starts with a vague query, rewrites it after an empty result, then makes a second hop from the lineage document to the owners of the source tables. A final check in code confirms that every citation in the answer was actually retrieved.

```python
import json, re

CHUNKS = {
    "lineage/revenue_daily": "finance.revenue_daily is built by the revenue_daily DAG from raw.orders and raw.refunds.",
    "runbook/orders_daily": "orders_daily loads raw.orders. Owner: payments data team, channel #payments-data.",
    "runbook/refunds_daily": "refunds_daily loads raw.refunds. Owner: finance engineering, channel #fin-eng.",
    "policy/retention": "Raw tables keep 400 days of partitions.",
}

def terms(text):
    return {w.strip(".") for w in re.findall(r"[a-z0-9_.]+", text.lower())}

def score(query, text):
    q, t = terms(query), terms(text)
    return len(q & t) / len(q) if q else 0.0

def search_docs(query, k=2, min_score=0.5):
    """The retrieval tool. It grades its own hits and says so when nothing is relevant."""
    hits = sorted(((score(query, t), cid, t) for cid, t in CHUNKS.items()), reverse=True)[:k]
    good = [{"id": cid, "text": t} for s, cid, t in hits if s >= min_score]
    return good or {"no_relevant_results": True, "hint": "use exact table or DAG names"}

def mock_model(messages):
    """Scripted model in the Messages API shape; it reads earlier tool results to decide."""
    seen = " ".join(b["content"] for m in messages if isinstance(m["content"], list)
                    for b in m["content"] if b["type"] == "tool_result")
    def use(q, n):
        return {"stop_reason": "tool_use", "content": [
            {"type": "tool_use", "id": f"tu_{n}", "name": "search_docs", "input": {"query": q}}]}
    if not seen:
        return use("who owns revenue", 1)                       # vague first query
    if "no_relevant_results" in seen and "lineage/" not in seen:
        return use("finance.revenue_daily built from", 2)       # rewrite with exact names
    if "lineage/revenue_daily" in seen and "runbook/orders_daily" not in seen:
        return use("raw.orders raw.refunds owner", 3)           # second hop
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text":
            "finance.revenue_daily is built from raw.orders and raw.refunds [lineage/revenue_daily]. "
            "Contact #payments-data for orders [runbook/orders_daily] and #fin-eng for refunds "
            "[runbook/refunds_daily]."}]}

def run(question, max_steps=6):
    messages = [{"role": "user", "content": question}]
    for _ in range(max_steps):
        reply = mock_model(messages)
        messages.append({"role": "assistant", "content": reply["content"]})
        if reply["stop_reason"] == "end_turn":
            return reply["content"][0]["text"], messages
        results = []
        for b in reply["content"]:
            out = search_docs(**b["input"])
            print("search:", b["input"]["query"], "->",
                  "none" if isinstance(out, dict) else [h["id"] for h in out])
            results.append({"type": "tool_result", "tool_use_id": b["id"], "content": json.dumps(out)})
        messages.append({"role": "user", "content": results})
    return "stopped: step limit", messages

answer, messages = run("Who should I contact about finance.revenue_daily being wrong?")
# search: who owns revenue -> none
# search: finance.revenue_daily built from -> ['lineage/revenue_daily']
# search: raw.orders raw.refunds owner -> ['runbook/refunds_daily', 'runbook/orders_daily']
print(answer)
# finance.revenue_daily is built from raw.orders and raw.refunds [lineage/revenue_daily]. Contact #payments-data for orders [runbook/orders_daily] and #fin-eng for refunds [runbook/refunds_daily].
cited = set(re.findall(r"\[([\w/]+)\]", answer))
retrieved = {h["id"] for m in messages if isinstance(m["content"], list) for b in m["content"]
             if b["type"] == "tool_result" for h in json.loads(b["content"]) if isinstance(h, dict)}
print("all citations retrieved:", cited <= retrieved)
# all citations retrieved: True
```

Four mechanisms are at work, and each maps to a research method:

- **Grading in the tool.** Low-scoring hits are not returned. An empty result with a hint is better than five irrelevant passages, because the model treats retrieved text as evidence. Corrective RAG (CRAG, Yan et al., 2024) formalises this: a retrieval evaluator labels results correct, ambiguous or incorrect, and incorrect results trigger a different search.
- **Query rewriting.** The second query uses exact names. Models write better search queries than users do, especially when told to use identifiers. HyDE (Gao et al., 2022) is a related trick: write a hypothetical answer and search with its embedding.
- **Multi-hop retrieval.** The third query exists only because of what the second returned. IRCoT (Trivedi et al., 2022) interleaves reasoning steps with retrieval this way, and it is what a ReAct agent does naturally.
- **Self-check.** The citation check runs in code. Self-RAG (Asai et al., 2023) trains a model to emit reflection tokens deciding when to retrieve and whether its output is supported; in practice most teams use a separate check step or a judge model.

**The same loop as a graph.** When you want the control flow fixed (retrieve, grade, rewrite at most twice, then answer or give up), write it as a LangGraph graph with a conditional edge rather than leaving it all to the model. The grading and rewriting functions would be model calls; here they are plain functions (checked with `langgraph` 1.2.24):

```python
from typing import TypedDict
from langgraph.graph import StateGraph, START, END

DOCS = {"lineage/revenue_daily": "finance.revenue_daily is built from raw.orders and raw.refunds",
        "policy/retention": "raw tables keep 400 days of partitions"}

class RAG(TypedDict, total=False):
    question: str
    query: str
    hits: list[str]
    rewrites: int
    answer: str

def retrieve(s: RAG) -> dict:
    q = set(s.get("query", s["question"]).lower().split())
    return {"hits": [k for k, t in DOCS.items() if len(q & set(t.split())) >= 2]}

def grade(s: RAG) -> str:                       # conditional edge: relevant, rewrite or give up
    if s["hits"]:
        return "generate"
    return "rewrite" if s.get("rewrites", 0) < 2 else "give_up"

def rewrite(s: RAG) -> dict:                    # in a real app: a model call that rewrites the query
    return {"query": "finance.revenue_daily built from", "rewrites": s.get("rewrites", 0) + 1}

def generate(s: RAG) -> dict:
    return {"answer": f"Answer grounded in {s['hits']}"}

def give_up(s: RAG) -> dict:
    return {"answer": "I could not find this in the documentation."}

g = StateGraph(RAG)
for name, fn in [("retrieve", retrieve), ("rewrite", rewrite), ("generate", generate), ("give_up", give_up)]:
    g.add_node(name, fn)
g.add_edge(START, "retrieve")
g.add_conditional_edges("retrieve", grade, ["generate", "rewrite", "give_up"])
g.add_edge("rewrite", "retrieve")
g.add_edge("generate", END)
g.add_edge("give_up", END)
app = g.compile()

out = app.invoke({"question": "where does revenue come from?"})
print(out["rewrites"], out["answer"])
# 1 Answer grounded in ['lineage/revenue_daily']
print(app.invoke({"question": "who approves budget?", "rewrites": 2})["answer"])
# I could not find this in the documentation.
```

This is a workflow with a bounded loop; the ReAct version above is an agent. Both are agentic RAG. The graph is easier to test and cost; the agent handles questions you did not anticipate.

> **Pitfall:** Unbounded search loops. An agent that never finds what it wants will search forever, rewording the same query. Cap retrieval calls per question, detect repeated queries, and make "I could not find this" an acceptable, well-formatted answer.

> **Tip:** Write the search tool's description for the model: what the corpus contains, what it does not, and how to query it ("use exact table or DAG names; one topic per query"). That description is the cheapest query-rewriting improvement you will make.

::: deeper
**Routing between sources.** Real agents have several retrieval tools: documentation search, a catalog lookup by table name, a ticket search, a SQL tool for numbers, web search. Routing can be the model's choice (tool descriptions do the work), a classifier in front, or both. Keep tools distinct: two overlapping search tools confuse the model.

**Decomposition.** For comparative or compound questions ("How do the retention rules for raw and curated tables differ?"), the agent splits the question into sub-queries, retrieves for each, and combines. This is the orchestrator-workers pattern applied to retrieval, and it parallelises well.

**Agentic search versus vector RAG.** Coding agents have shown that a capable model with plain tools (`grep`, `glob`, file reads) can outperform embedding retrieval on code, because exact matching and navigation suit structured text and there is no index to go stale. For large prose corpora, embeddings still win. Many agents combine them: vector search to find candidates, exact tools to read and verify.

**Cost and latency.** Each hop is a model call plus a retrieval call. Mitigations: parallel searches when sub-queries are independent, a cheaper model for query rewriting and grading, caching of retrieval results per query, and an early exit when the first retrieval is clearly sufficient (high scores, simple question).

**Evaluating agentic RAG.** Evaluate the trajectory, not just the answer: did it retrieve the right documents, how many calls did it make, did it stop when it should, did it say "not found" when the answer was absent? Include unanswerable questions in your test set; an agent that always finds something is a hallucination risk ([Evaluation and observability](#eval)).

> **Interview:** "What is agentic RAG and when would you use it over standard RAG?" In agentic RAG the model controls retrieval through tools: it decides whether to search, rewrites queries, chooses sources, retrieves iteratively for multi-hop questions, grades results and checks its answer before replying. I would use it for open, multi-step questions over several sources, such as incident investigation; for simple lookups I would keep one-shot RAG because it is cheaper, faster and easier to evaluate. Either way I would cap retrieval calls and make "not found" a valid answer.

> **Interview:** "How do you stop a RAG agent from answering from irrelevant documents?" Grade retrieval: a score threshold or a grader model in the tool, so irrelevant hits are dropped and the tool says nothing relevant was found; instructions to answer only from sources and say when the answer is missing; a citation check in code; unanswerable questions in the evaluation set to measure how often it invents answers.
:::

::: check
?? Name three things the agent decides in agentic RAG that a fixed pipeline decides for it.
Whether to retrieve at all, what query to send (rewriting), which source or tool to use, how many retrieval rounds to do (multi-hop), and when to stop or say it does not know.

?? Why should a search tool return "no relevant results" rather than its best five hits when scores are low?
The model treats retrieved text as evidence and will use whatever it gets. An explicit empty result prompts a rewrite or an honest "not found" instead of an answer built on irrelevant passages.

?? What is multi-hop retrieval? Give a data platform example.
Retrieval where the second query depends on what the first returned. Example: find which tables feed `finance.revenue_daily` (lineage doc), then find who owns the jobs that load those tables (runbooks).
:::

::: resources
- Paper: [Self-RAG: Learning to Retrieve, Generate, and Critique through Self-Reflection](https://arxiv.org/abs/2310.11511) · deciding when to retrieve and checking support
- Paper: [Corrective Retrieval Augmented Generation](https://arxiv.org/abs/2401.15884) · grading retrieval and correcting bad results
- Paper: [Interleaving Retrieval with Chain-of-Thought Reasoning (IRCoT)](https://arxiv.org/abs/2212.10509) · multi-hop retrieval guided by reasoning steps
- Paper: [Precise Zero-Shot Dense Retrieval without Relevance Labels (HyDE)](https://arxiv.org/abs/2212.10496) · searching with a hypothetical answer
- Paper: [Agentic Retrieval-Augmented Generation: A Survey](https://arxiv.org/abs/2501.09136) · patterns and architectures of agentic RAG
- Docs: [Workflows and agents in LangGraph](https://docs.langchain.com/oss/python/langgraph/workflows-agents) · building retrieval loops as graphs or agents
:::

### Hybrid search, reranking and evaluation of retrieval

::: plain
Two librarians search the same shelves. One matches exact words: ask for error ERR-1102 and she finds the page that says ERR-1102, but ask about "slow deliveries" and she misses the page that says "delayed files". The other understands meaning: she finds the delayed-files page but has no idea what ERR-1102 is. Hybrid search asks both and merges their lists. A reranker is a senior librarian who reads the top twenty carefully and puts them in the right order. Evaluation is checking, on questions with known answers, how often the right page made the list.
:::

**Two kinds of retrieval.** **Lexical** search (BM25, the classic search-engine ranking) scores documents by the query words they contain, weighted by how rare each word is across the corpus and normalised by document length. It is exact, fast, needs no model and is excellent for identifiers: error codes, table names, ticket numbers, product SKUs. **Dense** search (embeddings) scores by vector similarity, so it matches meaning across different wording ("slow" and "delayed") but blurs exact tokens it has never learned. Each fails where the other succeeds, which is why most production systems run both.

**Hybrid search and fusion.** Run both retrievers and combine the ranked lists. Scores from the two are on different scales (BM25 is unbounded, cosine is between minus one and one), so the common way to combine them is **reciprocal rank fusion (RRF)**: each document gets the sum of `1 / (k + rank)` over the lists it appears in, with `k` usually 60. It uses only ranks, needs no tuning and is hard to beat (Cormack et al., 2009). The alternative, a weighted sum of normalised scores, can do better when tuned on labelled data.

**Reranking.** First-stage retrievers embed the query and each document separately (bi-encoders), which is fast but coarse. A **reranker** (cross-encoder) reads the query and one candidate together and outputs a relevance score. It is much more accurate and much slower, so it runs only on the top 20 to 100 candidates. Options you can self-host include open cross-encoder models such as `BAAI/bge-reranker-v2-m3` (run with `sentence-transformers`' `CrossEncoder` class or served next to the embedding model) and a general open-weight LLM as a judge for small candidate sets; hosted rerank APIs (Cohere, Voyage, and Google's ranking API) exist too. Late-interaction models such as ColBERT sit in between.

**Measuring it.** You cannot improve retrieval you do not measure. Build a labelled set: real questions, each with the chunks that answer it. Then compute:

| Metric | Question it answers | Formula, informally |
|---|---|---|
| Recall@k | Did the right chunks make the top k? | Relevant found in top k / all relevant |
| Precision@k | How much of the top k is useful? | Relevant in top k / k |
| MRR | How high is the first relevant result? | Mean of 1 / rank of first relevant |
| nDCG@k | Is the ranking good, with graded relevance? | Discounted gain normalised by the ideal ranking |
| Hit rate@k | Did at least one relevant chunk appear? | Fraction of queries with any hit |

For RAG, **recall@k at the k you feed the model** matters most: a chunk that is not retrieved cannot be used. Precision matters for cost and distraction.

**From scratch.** The code implements BM25, a toy dense retriever, RRF and a stand-in reranker, then evaluates all four on a labelled set of four questions. The toy dense retriever maps words to shared concepts ("late", "delayed" and "slow" all map to `delay`), which mimics how real embeddings place synonyms close together; it knows nothing about error codes, which mimics their weakness on identifiers.

```python
import math, re
from collections import Counter

DOCS = {
    "d1": "orders_daily sensor times out when the PSP settlement file is delayed",
    "d2": "error ERR-1102 quota exceeded on the eu-billing resource group",
    "d3": "refunds are restated every Monday so weekly counts change",
    "d4": "how to request access to a Trino catalog",
    "d5": "query quota and resource group sizing for the payments catalog",
    "d6": "late arriving data handling with watermarks in streaming jobs",
    "d7": "error ERR-1103 permission denied on catalog finance",
    "d8": "the settlement file from the payment provider lands in MinIO at 05:50",
}
STOP = {"the", "is", "a", "on", "to", "of", "in", "when", "so", "how", "with", "and", "at", "from"}
def tok(text):
    return [w for w in re.findall(r"[a-z0-9_-]+", text.lower()) if w not in STOP]

# ---- BM25 (Okapi), from scratch ----
class BM25:
    def __init__(self, docs, k1=1.5, b=0.75):
        self.ids = list(docs); self.toks = {i: tok(t) for i, t in docs.items()}
        self.avgdl = sum(map(len, self.toks.values())) / len(docs)
        df = Counter(w for t in self.toks.values() for w in set(t))
        n = len(docs)
        self.idf = {w: math.log(1 + (n - f + 0.5) / (f + 0.5)) for w, f in df.items()}
        self.k1, self.b = k1, b
    def score(self, query, i):
        tf, dl = Counter(self.toks[i]), len(self.toks[i])
        return sum(self.idf.get(w, 0) * tf[w] * (self.k1 + 1) /
                   (tf[w] + self.k1 * (1 - self.b + self.b * dl / self.avgdl)) for w in tok(query))
    def search(self, query, k=3):
        scored = [(self.score(query, i), i) for i in self.ids]
        return [i for s, i in sorted(scored, reverse=True) if s > 0][:k]

# ---- toy dense retriever: words map to shared concepts, so synonyms land close ----
CONCEPTS = {"late": "delay", "delayed": "delay", "slow": "delay", "times": "delay",
            "psp": "provider", "provider": "provider", "payment": "provider",
            "file": "file", "settlement": "file", "quota": "limit", "limit": "limit",
            "access": "permission", "permission": "permission", "denied": "permission"}
def embed(text):
    return Counter(CONCEPTS[w] for w in tok(text) if w in CONCEPTS)
def cos(a, b):
    dot = sum(a[k] * b[k] for k in a)
    return dot / (math.sqrt(sum(v * v for v in a.values())) * math.sqrt(sum(v * v for v in b.values())) or 1)
def dense_search(query, k=3):
    q = embed(query)
    scored = [(cos(q, embed(t)), i) for i, t in DOCS.items()]
    return [i for s, i in sorted(scored, reverse=True) if s > 0][:k]

# ---- hybrid: reciprocal rank fusion ----
def rrf(*rankings, k=60):
    scores = Counter()
    for ranking in rankings:
        for rank, doc in enumerate(ranking, 1):
            scores[doc] += 1 / (k + rank)
    return [d for d, _ in scores.most_common()]

# ---- evaluation on a small labelled set ----
QRELS = {   # query -> relevant documents (labelled by hand)
    "is the settlement data late today": {"d1", "d8"},
    "ERR-1102": {"d2"},
    "permission denied on catalog": {"d7", "d4"},
    "payment provider delivery is slow": {"d1", "d8"},
}
def recall_at(ranked, rel, k): return len(set(ranked[:k]) & rel) / len(rel)
def mrr(ranked, rel): return next((1 / r for r, d in enumerate(ranked, 1) if d in rel), 0.0)

bm25 = BM25(DOCS)
def rerank(query, candidates):
    """Stand-in for a cross-encoder: scores each (query, document) pair together."""
    qc, qt = set(embed(query)), set(tok(query))
    def pair_score(i):
        coverage = len(qc & set(embed(DOCS[i]))) / (len(qc) or 1)     # meaning: concepts covered
        exact = len(qt & set(tok(DOCS[i]))) / len(qt)                 # wording: exact terms
        return coverage + exact
    return sorted(candidates, key=pair_score, reverse=True)

systems = {
    "bm25":   lambda q: bm25.search(q, k=5),
    "dense":  lambda q: dense_search(q, k=5),
    "hybrid": lambda q: rrf(bm25.search(q, k=5), dense_search(q, k=5)),
    "hybrid+rerank": lambda q: rerank(q, rrf(bm25.search(q, k=5), dense_search(q, k=5))),
}
for name, search in systems.items():
    r = [recall_at(search(q), rel, 2) for q, rel in QRELS.items()]
    m = [mrr(search(q), rel) for q, rel in QRELS.items()]
    print(f"{name:13s} recall@2={sum(r)/len(r):.2f} MRR={sum(m)/len(m):.2f}")
# bm25          recall@2=0.75 MRR=0.88
# dense         recall@2=0.62 MRR=0.75
# hybrid        recall@2=0.88 MRR=0.88
# hybrid+rerank recall@2=0.88 MRR=1.00
for q in ["is the settlement data late today", "ERR-1102", "payment provider delivery is slow"]:
    print(q[:12], {n: s(q)[:2] for n, s in systems.items()})
# is the settl {'bm25': ['d6', 'd8'], 'dense': ['d1', 'd6'], 'hybrid': ['d6', 'd1'], 'hybrid+rerank': ['d1', 'd6']}
# ERR-1102 {'bm25': ['d2'], 'dense': [], 'hybrid': ['d2'], 'hybrid+rerank': ['d2']}
# payment prov {'bm25': ['d8'], 'dense': ['d8', 'd1'], 'hybrid': ['d8', 'd1'], 'hybrid+rerank': ['d8', 'd1']}
```

Read the per-query rows. BM25 finds the error code that the dense retriever cannot see at all, and on "payment provider delivery is slow" it finds only one of the two relevant documents, because the other says "PSP" and "delayed". The dense retriever finds both by meaning. Hybrid takes the best of each, lifting recall@2 above either alone. On "is the settlement data late today" both retrievers like the streaming watermarks document (it literally says "late"), and fusion leaves it first; the reranker, which looks at the query and each candidate together, moves the relevant document to the top, which is what lifts MRR to 1.0. Four questions are not an evaluation, but the shape of these results is typical.

> **Pitfall:** Evaluating only the final answer. When a RAG answer is wrong you need to know whether retrieval missed the chunk or the model misused it. Measure retrieval (recall@k) and generation (faithfulness, answer correctness) separately, on the same labelled set.

> **Tip:** Start the labelled set from production logs: questions people actually asked, with the chunks a human marked as answering them. Fifty to a hundred questions, including some with no answer in the corpus, are enough to compare chunking, hybrid weights and rerankers.

::: deeper
**BM25 in one paragraph.** For each query term, BM25 multiplies an inverse document frequency (rare terms count more) by a saturating term-frequency factor (the tenth occurrence adds little) normalised by document length relative to the average (parameter `b`, about 0.75). `k1` (about 1.2 to 2) controls saturation. It has been the strong baseline of information retrieval for decades, and BEIR (Thakur et al., 2021) showed it still beats many dense models out of domain. Tokenisation decisions (lowercasing, keeping `ERR-1102` as one token, stemming) matter as much as the formula.

**Where hybrid lives.** PostgreSQL combines full-text search (`tsvector`, `ts_rank`) with pgvector in one SQL query, fusing the two ranked lists with RRF in a CTE; Qdrant stores dense and sparse vectors on the same point and fuses them with RRF in one query; Milvus has BM25 full-text search and sparse vectors next to dense ones, combined by `hybrid_search` with an RRF ranker; OpenSearch runs a `hybrid` query that normalises or rank-fuses BM25 and k-NN scores in a search pipeline. The last topic of this group shows Qdrant's version. You can also fuse in application code, as above.

**Contextual retrieval.** Anthropic's contextual retrieval adds a model-written sentence of context to each chunk before both embedding and BM25 indexing, then fuses and reranks. In their tests, each step reduced retrieval failures further, and the combination roughly two-thirds of them. The lesson generalises: fix chunk context first, then add hybrid, then rerank.

**LLM judges for retrieval and RAG.** Labelling relevance by hand does not scale. An LLM judge can grade (query, chunk) relevance and (answer, sources) faithfulness; frameworks such as Ragas package metrics like context precision, context recall and faithfulness. Calibrate any judge against human labels on a sample before trusting it ([Evaluation and observability](#eval)).

**Latency budget.** Typical numbers: BM25 and ANN search in milliseconds, a cross-encoder rerank of 50 candidates in tens to hundreds of milliseconds, an LLM rerank in seconds. For an interactive agent, rerank fewer candidates or serve the reranker on a GPU; for batch, rerank generously.

> **Interview:** "Why use hybrid search instead of only embeddings?" Dense retrieval handles paraphrase but misses exact tokens such as error codes, table names and IDs, which matter a lot in technical corpora; BM25 is the opposite. Running both and fusing with reciprocal rank fusion gets the strengths of each without score calibration, and a cross-encoder reranker on the top candidates fixes the ordering. I would prove it on a labelled set with recall@k and MRR.

> **Interview:** "How do you evaluate a retrieval system for RAG?" A labelled set of real questions with relevant chunks (including unanswerable questions); recall@k at the k given to the model as the main metric, plus MRR or nDCG for ranking quality and precision for cost; generation measured separately for faithfulness and correctness; every change (chunking, embeddings, hybrid, reranker) compared on the same set, and production monitored with sampled judge scores and user feedback.
:::

::: check
?? Why does reciprocal rank fusion use ranks instead of scores?
BM25 scores and cosine similarities are on different, uncalibrated scales; ranks are comparable across retrievers, so RRF needs no normalisation or tuning.

?? Why does a reranker run only on the top candidates?
A cross-encoder reads the query and each document together, which is accurate but too slow to run over the whole corpus. The first stage narrows to a few dozen candidates cheaply; the reranker orders them well.

?? Your RAG recall@5 is 0.6 and answers are often wrong. Where do you look first?
Retrieval: 40% of the time the needed chunk never reaches the model. Check chunking and chunk context, add hybrid search for identifiers, raise k with a reranker, then re-measure before touching the prompt.
:::

::: resources
- Paper: [Reciprocal Rank Fusion outperforms Condorcet and individual rank learning methods](https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf) · the original RRF paper
- Paper: [BEIR: A Heterogeneous Benchmark for Zero-shot Evaluation of Information Retrieval Models](https://arxiv.org/abs/2104.08663) · BM25 versus dense models across domains
- Paper: [ColBERT: Efficient and Effective Passage Search via Contextualized Late Interaction](https://arxiv.org/abs/2004.12832) · late interaction between bi-encoders and cross-encoders
- Blog: [Introducing Contextual Retrieval](https://www.anthropic.com/news/contextual-retrieval) · contextual chunks, hybrid BM25 and reranking measured together
- Model: [bge-reranker-v2-m3](https://huggingface.co/BAAI/bge-reranker-v2-m3) · an open multilingual cross-encoder reranker you can host
- Docs: [Cross-encoder usage](https://sbert.net/docs/cross_encoder/usage/usage.html) · reranking candidates with `CrossEncoder` in sentence-transformers
- Paper: [Ragas: Automated Evaluation of Retrieval Augmented Generation](https://arxiv.org/abs/2309.15217) · context precision, recall and faithfulness metrics
:::

### GraphRAG and knowledge graphs for agents

::: plain
Vector search finds paragraphs that sound like your question. But some questions are about connections: "who owns the job that feeds the table our CFO's dashboard reads?" The answer is not in any one paragraph; it is a path through four facts written in four places. A knowledge graph stores those facts as dots and lines, so the agent can walk the path. And for big-picture questions ("what are the main risks across our pipelines?") it can read summaries of whole neighbourhoods instead of a handful of paragraphs.
:::

**What it is.** A **knowledge graph** stores entities (tables, DAGs, teams, services) as nodes and relationships (built from, loaded by, owned by) as edges, usually as subject, relation, object **triples**. **GraphRAG** uses a graph in retrieval: entities are extracted from documents (often by a model) into a graph, and questions are answered by traversing it, alone or together with vector search. Microsoft's GraphRAG (Edge et al., 2024) added a second idea: detect **communities** (densely connected groups of entities) and write a summary of each at indexing time, so global questions can be answered from summaries.

**Why it exists.** Vector RAG retrieves chunks that are individually similar to the question. It struggles with two kinds of question:

- **Multi-hop, relational questions.** "Who to call if `finance.revenue_daily` is wrong?" needs lineage (revenue from orders and refunds), then job ownership. No single chunk says it. Agentic RAG can do it in several searches; a graph does it in one traversal, reliably.
- **Global, sensemaking questions.** "What are the main themes across 300 incident reviews?" The answer is spread over the whole corpus; top-k chunks give a biased sample. Community summaries, combined map-reduce style, cover everything.

**Data engineers already own graphs.** Lineage from OpenLineage or Knowledge Catalog (formerly Dataplex Universal Catalog), dbt's DAG, Airflow's task graph, ownership from the catalog and the on-call system: these are knowledge graphs with exact, trustworthy edges. For a data platform agent, building the graph from metadata beats extracting it from prose with a model. The data engineering course covers knowledge graphs as data infrastructure in [AI and ML for data engineers](index.html#aiml).

[[fig:memory_graphrag]]

**From scratch.** The triples below stand in for what extraction (or lineage metadata) produced. **Local search** starts at an entity and follows chosen relations outward until it reaches owners, returning each path, which becomes the evidence in the prompt. **Global search** groups entities into communities (connected components here; Leiden clustering in Microsoft's GraphRAG) and summarises each one; a global question is then answered from the summaries.

```python
from collections import defaultdict, deque

# Triples a model extracted from runbooks, DAG code and the catalog (subject, relation, object)
TRIPLES = [
    ("finance.revenue_daily", "built_from", "raw.orders"),
    ("finance.revenue_daily", "built_from", "raw.refunds"),
    ("raw.orders", "loaded_by", "orders_daily"),
    ("raw.refunds", "loaded_by", "refunds_daily"),
    ("orders_daily", "owned_by", "payments-data"),
    ("refunds_daily", "owned_by", "fin-eng"),
    ("orders_daily", "reads", "psp_settlement_files"),
    ("marketing.campaigns", "built_from", "raw.ad_spend"),
    ("raw.ad_spend", "loaded_by", "ads_daily"),
    ("ads_daily", "owned_by", "growth-data"),
]
out_edges = defaultdict(list)
for s, r, o in TRIPLES:
    out_edges[s].append((r, o))

def local_search(entity, follow=("built_from", "loaded_by", "owned_by"), max_hops=4):
    """Walk outward from one entity and return every path that ends at an owner."""
    paths, queue = [], deque([(entity, [entity])])
    while queue:
        node, path = queue.popleft()
        if len(path) > 2 * max_hops:
            continue
        for rel, nxt in out_edges[node]:
            if rel not in follow:
                continue
            new = path + [rel, nxt]
            (paths.append(new) if rel == "owned_by" else queue.append((nxt, new)))
    return paths

for p in local_search("finance.revenue_daily"):
    print(" -> ".join(p))
# finance.revenue_daily -> built_from -> raw.orders -> loaded_by -> orders_daily -> owned_by -> payments-data
# finance.revenue_daily -> built_from -> raw.refunds -> loaded_by -> refunds_daily -> owned_by -> fin-eng

# Global questions need the whole graph: group entities into communities, summarise each one
def communities(triples):
    nbrs = defaultdict(set)
    for s, _, o in triples:
        nbrs[s].add(o); nbrs[o].add(s)
    seen, groups = set(), []
    for start in sorted(nbrs):
        if start in seen:
            continue
        group, stack = set(), [start]
        while stack:
            n = stack.pop()
            if n not in group:
                group.add(n); stack.extend(nbrs[n] - group)
        seen |= group
        groups.append(sorted(group))
    return groups

def mock_summarize(group):
    """Stand-in for a model call that writes a community report at indexing time."""
    owners = sorted(o for s, r, o in TRIPLES if r == "owned_by" and s in group)
    tables = [n for n in group if "." in n]
    return f"{len(tables)} tables; owners: {', '.join(owners)}"

for g in communities(TRIPLES):
    print(mock_summarize(g))
# 2 tables; owners: growth-data
# 3 tables; owners: fin-eng, payments-data
```

The local search returns two complete paths, which is exactly the evidence an answer needs, with every hop citable. The global step turned ten triples into two community reports; in a real system, reports are written by a model at indexing time, and a global question is answered by asking each report for relevant points (map) and combining them (reduce).

**When graphs beat vectors, and when they do not:**

| Question type | Better choice |
|---|---|
| "What does the runbook say about late files?" | Vector or hybrid RAG |
| "Which jobs are upstream of this table, and who owns them?" | Graph traversal |
| "What changed in lineage since last week?" | Graph with time on edges |
| "Main themes across all incident reviews?" | Community summaries (global GraphRAG) |
| "Explain this error code" | Hybrid search (exact token) |

Most real systems combine them: vector search finds the entry entities ("revenue table" resolves to `finance.revenue_daily`), the graph expands to related entities, and the chunks attached to those entities are retrieved as text.

> **Pitfall:** Extracting a graph from prose with a model and trusting it. Extraction misses edges, invents edges and creates duplicate entities ("orders_daily", "orders daily job", "the orders DAG"). Use authoritative metadata where it exists, resolve entities to canonical IDs, and keep the source chunk on every extracted edge so answers can cite it.

> **Tip:** Expose the graph to the agent as tools, not as a dump: `upstream(table, depth)`, `owner(entity)`, `path(a, b)`. Small, typed tools over a graph give the model exact answers and keep the context small.

::: deeper
**Microsoft GraphRAG's pipeline.** Indexing: chunk documents, extract entities, relationships and claims with a model, build the graph, cluster it hierarchically with Leiden, and write community reports at each level. Querying: **local search** combines entities near the question with their relationships, reports and source chunks; **global search** maps the question over community reports and reduces the partial answers; **DRIFT search** mixes the two. Indexing is expensive (many model calls per chunk), so it suits corpora that change slowly and questions that need the global view. LazyGraphRAG and LightRAG (Guo et al., 2024) reduce indexing cost; HippoRAG (Gutiérrez et al., 2024) uses personalized PageRank over an extracted graph for multi-hop retrieval.

**Storage options.** Property graph databases you can self-host (Neo4j Community Edition, Memgraph) with Cypher or GQL; Apache AGE, a PostgreSQL extension that adds Cypher queries over graphs stored in PostgreSQL; or plain relational tables (an edges table plus recursive CTEs) for modest graphs, which is often enough for lineage. On Google Cloud, Spanner Graph and BigQuery Graph offer GQL over data already in those systems. Many graph stores also hold vectors, so one query can combine similarity and traversal.

**Temporal graphs.** Facts change: ownership moves, lineage is rewired. Store validity intervals on edges so the agent can answer "who owned it when the incident happened?". Temporal knowledge graphs are also used as agent memory (Zep's Graphiti is one example), combining the memory topics above with graph retrieval.

**Text-to-query.** An agent can also write Cypher, GQL or SQL against the graph. This is flexible but brings the risks of any generated query: validate it, run it read-only, and limit its cost, as with text-to-SQL ([Agents for data engineering](#dataagents)).

> **Interview:** "What is GraphRAG and when would you use it?" Retrieval over a knowledge graph of entities and relationships, often extracted from documents by a model, sometimes with community summaries written at indexing time. Local search traverses from the entities in the question, which suits multi-hop relational questions such as lineage and ownership; global search answers corpus-wide questions from community summaries with map-reduce. I would use it when relationships matter or questions are global; for most single-fact lookups hybrid RAG is cheaper. For a data platform, I would build the graph from lineage and catalog metadata rather than extracting it from prose.

> **Interview:** "How would an agent answer 'what breaks if we drop raw.refunds?'" Graph traversal downstream from `raw.refunds` over lineage edges (from OpenLineage or the catalog), returning affected tables, dashboards and owners as paths, then a summary that cites each path. Vector search over docs would miss most of the dependencies.
:::

::: check
?? Name the two kinds of question where GraphRAG beats plain vector RAG.
Multi-hop relational questions (follow relationships across several facts, such as lineage then ownership) and global questions about a whole corpus (answered from community summaries).

?? What are community summaries and when are they written?
Model-written reports of densely connected groups of entities in the graph, created at indexing time, so global questions can be answered by combining summaries instead of retrieving a few chunks.

?? Why prefer lineage metadata over model extraction for a data platform graph?
Metadata edges are exact and complete; model extraction misses and invents edges and creates duplicate entities.
:::

::: resources
- Paper: [From Local to Global: A Graph RAG Approach to Query-Focused Summarization](https://arxiv.org/abs/2404.16130) · Microsoft's GraphRAG with community summaries
- Docs: [GraphRAG](https://microsoft.github.io/graphrag/) · the open-source implementation, indexing and local, global and DRIFT search
- Paper: [LightRAG: Simple and Fast Retrieval-Augmented Generation](https://arxiv.org/abs/2410.05779) · cheaper graph-based retrieval
- Paper: [HippoRAG: Neurobiologically Inspired Long-Term Memory for Large Language Models](https://arxiv.org/abs/2405.14831) · graph retrieval with personalized PageRank
- Docs: [Neo4j documentation](https://neo4j.com/docs/) · the property graph database, Cypher and its vector index
- Docs: [Apache AGE](https://age.apache.org/) · Cypher graph queries inside PostgreSQL
:::

### Vector databases for agents: pgvector, Qdrant, Milvus and OpenSearch

::: plain
A library can keep its card catalogue in the same room as the books, in a dedicated catalogue office, in a network of warehouses, or at the search desk that already answers every other question. Vector databases offer the same choice for embeddings: inside the PostgreSQL you already run (pgvector), a dedicated vector engine (Qdrant), a distributed system built for billions of vectors (Milvus), or the search engine you already use for documents and logs (OpenSearch). All of them find "the nearest few". They differ in where the data lives, how they apply filters, and how much work they are to run.
:::

**What an agent needs from a vector store.** Four things, in this order. Filters that run inside the search (permissions, tenant, team, date), because a retrieval tool is a data access path. Predictable latency at the agent's query rate. Cheap updates and deletes, because memories and documents change. And hybrid search, because agents ask about identifiers. Raw nearest-neighbour speed is rarely the bottleneck: one model call takes longer than hundreds of searches. The data engineering course explains the index internals (HNSW, IVF, quantization) in depth ([AI and ML for data engineers](index.html#aiml)); here is the agent builder's comparison. All four are open source and run on your own servers or on Kubernetes.

| Option | What it is | Indexes | How it filters | Best when |
|---|---|---|---|---|
| pgvector | PostgreSQL extension: `vector`, `halfvec` and `sparsevec` column types | HNSW, IVFFlat, or an exact scan | SQL `WHERE` in the same query; iterative index scans (0.8 and later) keep searching until enough rows pass | Vectors next to relational data, up to tens of millions of vectors, you already run PostgreSQL |
| Qdrant | Dedicated vector engine written in Rust; points carry JSON payloads | HNSW per segment, with scalar, product or binary quantization | Payload indexes; filters are checked during the graph search, with a switch to an exact scan of matching points when the filter is very selective | Filter-heavy search, dense plus sparse hybrid, a vector service that scales on its own |
| Milvus | Distributed vector database: stateless query and data nodes, etcd for metadata, MinIO or S3 for segments, a write-ahead log for inserts | HNSW, IVF variants, DiskANN, GPU indexes | Boolean filter expressions on scalar fields, scalar indexes, a partition key per tenant | Hundreds of millions to billions of vectors, GPU search, a platform team to run it |
| OpenSearch | Search engine (Apache 2.0 fork of Elasticsearch) with the k-NN plugin and `knn_vector` fields | HNSW and IVF through the Faiss or Lucene engines | A `filter` inside the k-NN query; the engine chooses exact or approximate search by how many documents match | BM25, vectors and aggregations in one cluster you already run |

Redis (with its query engine) and Valkey can also store and search vectors. They suit small, hot sets such as a semantic cache or per-session memory better than a large document corpus.

**pgvector: vectors in the database you already run.** For most agent teams this is the first choice: no new system, transactions that write a row and its embedding together, joins to the tables the agent reasons about, and the backups, replication, roles and row-level security you already have. A runbook corpus and the agent's search query:

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE runbook_chunks (
  chunk_id    bigserial PRIMARY KEY,
  team        text NOT NULL,
  doc_uri     text NOT NULL,
  body        text NOT NULL,
  embed_model text NOT NULL,                 -- which model produced the vector
  embedding   vector(768) NOT NULL           -- nomic-embed-text, served by Ollama
);
CREATE INDEX ON runbook_chunks USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON runbook_chunks (team);

-- the agent's search tool: $1 is the question's embedding, $2 the user's team (both bound parameters)
SET hnsw.iterative_scan = strict_order;      -- keep walking the index until enough rows pass the filter
SELECT chunk_id, doc_uri, body, embedding <=> $1 AS distance
FROM runbook_chunks
WHERE team = $2
ORDER BY embedding <=> $1
LIMIT 5;
```

The operator `<=>` is cosine distance (`<->` is Euclidean, `<#>` negative inner product), and the index must be built with the matching operator class. The `SET` line matters. An HNSW index scan collects the nearest `hnsw.ef_search` candidates (40 by default) and only then applies `WHERE team = $2`. If the payments team owns 10 percent of the rows, about 4 of the 40 survive, and the tool returns fewer than 5 rows without any error. Iterative scans, added in pgvector 0.8, continue the walk until the `LIMIT` is met. For a very selective filter (one small tenant among thousands), the planner may instead use the B-tree index on `team` and compute exact distances for that tenant's rows, which is exact and fast. For large tenants, a partial index per tenant or a table partitioned by tenant keeps each search inside one tenant's data.

**Qdrant: a dedicated engine with filtering built in.** Qdrant stores **points** (an ID, one or more vectors and a JSON **payload**) in **collections**. You create a payload index on each field you filter by, and the filter is evaluated during the HNSW search instead of after it. The Python client has an embedded local mode, so the example below runs without a server (with `qdrant-client` 1.20.0). It builds a small runbook collection in which the marketing runbooks mention "late" far more than the payments ones, then compares filtering inside the search with filtering afterwards:

```python
import hashlib, math, re, warnings
from qdrant_client import QdrantClient, models

warnings.filterwarnings("ignore", "Payload indexes")    # local mode ignores payload indexes and says so

def toy_embed(text, dims=64):
    """Hashing-trick embedding. In production: a local model served by Ollama or sentence-transformers."""
    v = [0.0] * dims
    for w in re.findall(r"[a-z0-9_]+", text.lower()):
        v[int(hashlib.md5(w.encode()).hexdigest(), 16) % dims] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]

CHUNKS = [   # (team, doc, text): two payments runbooks, three marketing ones that say "late" a lot
    ("payments", "runbook/orders_daily.md", "orders_daily waits two hours for a late PSP file then the sensor fails"),
    ("payments", "runbook/refunds_daily.md", "refunds_daily is restated every Monday so weekly counts change"),
    ("marketing", "runbook/ad_spend.md", "ad_spend load is late when the ads API file arrives late"),
    ("marketing", "runbook/campaigns.md", "campaigns table is late if the ad_spend file is late"),
    ("marketing", "runbook/clicks.md", "clicks stream lag grows when the late file backlog grows"),
]

client = QdrantClient(":memory:")          # embedded local mode; QdrantClient(url="http://qdrant:6333") for a server
client.create_collection("runbooks",
    vectors_config=models.VectorParams(size=64, distance=models.Distance.COSINE))
client.create_payload_index("runbooks", field_name="team",            # index every field you filter on
                            field_schema=models.PayloadSchemaType.KEYWORD)
client.upsert("runbooks", points=[
    models.PointStruct(id=i, vector=toy_embed(text), payload={"team": team, "doc": doc, "text": text})
    for i, (team, doc, text) in enumerate(CHUNKS)])

def search_runbooks(question, team, k=2):
    """The agent's tool: `team` comes from the authenticated user, never from the model."""
    only_team = models.Filter(must=[models.FieldCondition(key="team", match=models.MatchValue(value=team))])
    hits = client.query_points("runbooks", query=toy_embed(question), query_filter=only_team, limit=k).points
    return [(h.payload["doc"], round(h.score, 2)) for h in hits]

question = "what happens when the file is late"
print("pre-filter: ", search_runbooks(question, team="payments"))
top = client.query_points("runbooks", query=toy_embed(question), limit=2).points    # search everything...
print("post-filter:", [h.payload["doc"] for h in top if h.payload["team"] == "payments"])  # ...then filter
# pre-filter:  [('runbook/orders_daily.md', 0.49), ('runbook/refunds_daily.md', 0.25)]
# post-filter: []
```

[[fig:memory_vecfilter]]

The filtered search returns a full top 2 from the payments runbooks. The unfiltered search returns the two marketing runbooks closest to "late", and filtering them afterwards leaves nothing: the agent would tell a payments engineer that there is no runbook, and the search has ranked another team's documents along the way. The local mode keeps everything in Python and ignores payload indexes; against a server (`docker run -p 6333:6333 qdrant/qdrant`, or the Helm chart on Kubernetes) the same code uses the index, and the `team` index is what keeps filtered searches fast.

**Milvus: when the corpus outgrows one machine.** Milvus separates compute from storage: query nodes serve searches from segments kept in object storage (MinIO on premises), data nodes write new segments, etcd holds metadata and a write-ahead log orders inserts. Each part scales on its own, which is what makes billions of vectors and GPU indexes practical, and also why it needs a team that runs distributed systems. Searches take a boolean `filter` expression such as `team == "payments" and updated_at > 1730000000`; a field marked as the **partition key** (`is_partition_key=True`) groups each tenant's vectors so a tenant filter searches only its partitions. Milvus Lite (an embedded library) and a single-node Docker deployment cover development.

**OpenSearch: when search is already there.** If your platform already runs OpenSearch for documents or logs, a `knn_vector` field adds vectors to the same index as the text. A `knn` query with a `filter` clause applies the filter during the search (the engine picks exact search when few documents match and approximate search otherwise), and a `hybrid` query combines BM25 and k-NN results through a search pipeline that normalises or rank-fuses the scores. You also get aggregations, highlighting and document-level security that a pure vector engine lacks. The cost is the heavier JVM cluster and more tuning.

**Choosing.** Ask four questions, in order. Where does the data already live (do not copy it without a reason)? What latency and query rate does the agent need (an interactive assistant at 50 queries a second is not a nightly batch)? How large is the corpus and how fast does it change? Who will run the system at 3 a.m.? A typical on-premises answer: pgvector in the existing PostgreSQL for most agents and for long-term memory; Qdrant when filter-heavy search at high query rates should scale separately from the transactional database; Milvus when the corpus reaches hundreds of millions of vectors or needs GPUs; OpenSearch when the platform already runs it and hybrid lexical search matters most. Decide on a labelled evaluation set and a latency target, not on benchmark charts.

> **Pitfall:** Post-filtering. Searching for the top 5 across all tenants and then filtering by tenant returns too few results, and the search has touched data the user may not see. Filter inside the search (`WHERE` with iterative scans in pgvector, `query_filter` in Qdrant, `filter` in Milvus and OpenSearch), or keep separate indexes or partitions per tenant, and bind the tenant from the authenticated session.

> **Tip:** Keep the chunk table as the source of truth and the vector index as a derived, rebuildable object, whatever the store. Track the embedding model and its version on every row; when you change models, re-embed into a new column or collection and switch over after evaluating, like any backfill.

**On Google Cloud.** AlloyDB and Cloud SQL for PostgreSQL run pgvector as a managed service (AlloyDB adds a ScaNN index), Vector Search on Gemini Enterprise Agent Platform is the managed ANN service, and BigQuery's `VECTOR_SEARCH` suits corpora that already live in the warehouse.

::: deeper
**Memory sizing.** HNSW is fastest when the index fits in RAM. Ten million 768-dimension `float32` vectors are about 30 GB before the graph links. `halfvec` in pgvector halves that (HNSW indexes `vector` up to 2,000 dimensions and `halfvec` up to 4,000); Qdrant's scalar quantization stores 8-bit vectors (4 times smaller) and its binary quantization 1-bit vectors (32 times smaller), rescoring the best candidates with the original vectors kept on disk. Disk-based indexes (DiskANN in Milvus, StreamingDiskANN in the pgvectorscale extension) trade some latency for much less memory.

**Updates and deletes.** Agent memory and live documents change constantly. In HNSW a delete marks a node; space and graph quality come back only through maintenance: `VACUUM` and occasional `REINDEX` in PostgreSQL, the segment optimiser in Qdrant, compaction in Milvus, segment merges in OpenSearch. Watch recall on your evaluation set after heavy churn, not only latency.

**Multi-tenancy patterns.** pgvector: a tenant column with iterative scans for many small tenants, partial indexes or partitions for large ones, and row-level security as a backstop. Qdrant: one collection with a keyword payload index marked `is_tenant` (it stores each tenant's points together on disk), or a collection per very large tenant. Milvus: a partition key. OpenSearch: an index per tenant, or a filter plus document-level security. Avoid a collection per user when you have thousands of users; the per-collection overhead adds up.

**Hybrid search in Qdrant.** A point can hold a dense vector and a sparse one (term weights), and one query fuses the two candidate lists with reciprocal rank fusion. Continuing the example above:

```python
from collections import Counter

def sparse(text):
    """Term counts as a sparse vector (hashed term id -> count); Qdrant applies IDF at query time."""
    c = Counter(int(hashlib.md5(w.encode()).hexdigest(), 16) % 100_000
                for w in re.findall(r"[a-z0-9_-]+", text.lower()))
    return models.SparseVector(indices=list(c), values=[float(n) for n in c.values()])

DOCS = ["error ERR-1102 quota exceeded on the payments resource group",
        "orders_daily sensor times out when the PSP settlement file is delayed",
        "query quota and resource group sizing for the payments catalog"]
client.create_collection("docs",
    vectors_config={"dense": models.VectorParams(size=64, distance=models.Distance.COSINE)},
    sparse_vectors_config={"lexical": models.SparseVectorParams(modifier=models.Modifier.IDF)})
client.upsert("docs", points=[
    models.PointStruct(id=i, vector={"dense": toy_embed(t), "lexical": sparse(t)}, payload={"text": t})
    for i, t in enumerate(DOCS)])

q = "ERR-1102"
hits = client.query_points("docs",
    prefetch=[models.Prefetch(query=toy_embed(q), using="dense", limit=20),     # two candidate lists...
              models.Prefetch(query=sparse(q), using="lexical", limit=20)],
    query=models.FusionQuery(fusion=models.Fusion.RRF), limit=2).points          # ...fused with RRF
print([h.payload["text"][:24] for h in hits])
# ['error ERR-1102 quota exc', 'query quota and resource']
```

The sparse list ranks the document containing the exact code first, and `Modifier.IDF` makes rare terms count more, as in BM25. In production the sparse vectors come from a real tokenizer (Qdrant's BM25 model in FastEmbed, or a learned sparse model such as SPLADE) rather than this hashing toy.

**Running it.** Treat the vector store like any stateful service: backups (PostgreSQL base backups and WAL archiving, Qdrant snapshots, Milvus Backup), replication, TLS and authentication (Qdrant API keys or JWT roles, Milvus users and roles, OpenSearch security plugin), Prometheus metrics (Qdrant and Milvus expose them, PostgreSQL through an exporter) and Grafana alerts on latency and memory. The agent's service account should be able to read only the collections its tools search ([Production engineering, security and cost](#production)).

**Benchmarks.** ANN-Benchmarks compares index algorithms on recall against queries per second, without filters. Agent workloads are filtered, small-k, updated often and dominated by model latency, so run your own test: your vectors, your filters, your target query rate.

> **Interview:** "Which vector store would you use for an on-premises agent over our runbooks and incident reviews?" pgvector in the PostgreSQL we already run, unless something rules it out: no new system, the chunk table and its embeddings in one transaction, filters and joins in SQL, our existing backups and roles. HNSW with cosine distance, iterative scans for the team filter, embeddings from a local model served by Ollama with the model version on each row. I would move to Qdrant if filtered search at high query rates started to load the transactional database, and to Milvus only at hundreds of millions of vectors. I would decide on a labelled evaluation set and a latency target.

> **Interview:** "What is the difference between pre-filtering and post-filtering in vector search?" Pre-filtering restricts the candidates before or during the nearest-neighbour search, so you get the top k among allowed rows; post-filtering searches everything and then drops rows, which can return fewer than k results and touches data the user may not see. Use filtering inside the search for permissions and tenancy, and check how your store does it: pgvector before 0.8 post-filtered the index results, which is why iterative scans exist.
:::

::: check
?? A pgvector query with `WHERE team = $2 ORDER BY embedding <=> $1 LIMIT 5` sometimes returns only 2 rows, although the team has hundreds of chunks. Why, and what are the fixes?
The HNSW scan collects only `hnsw.ef_search` candidates (40 by default) and the `WHERE` is applied afterwards, so few of them belong to the team. Enable iterative index scans (`SET hnsw.iterative_scan = strict_order`, pgvector 0.8 and later), raise `ef_search`, or use a partial index or partition per large team.

?? Why create a payload index on `team` in Qdrant if the filter works without one?
On a server, the payload index lets Qdrant evaluate the filter during the HNSW search and estimate how selective it is, switching to an exact scan over the matching points when few match. Without it, filtered searches are slower and less predictable as the collection grows.

?? Your platform already runs PostgreSQL and OpenSearch. An agent needs vector search over 2 million runbook chunks, plus exact matches on error codes. What are your two sensible options?
pgvector with PostgreSQL full-text search fused by RRF in SQL, or OpenSearch with a `knn_vector` field and a `hybrid` query. Pick by where the chunks already live and by a test on a labelled set; a new dedicated vector database is not needed at this size.
:::

::: resources
- Repo: [pgvector](https://github.com/pgvector/pgvector) · vector types, HNSW and IVFFlat indexes, filtering and iterative index scans
- Docs: [Qdrant filtering](https://qdrant.tech/documentation/concepts/filtering/) · payload conditions and how filters combine with search
- Docs: [Qdrant indexing](https://qdrant.tech/documentation/concepts/indexing/) · payload indexes, filterable HNSW and the query planner
- Docs: [Qdrant hybrid queries](https://qdrant.tech/documentation/concepts/hybrid-queries/) · prefetch, sparse vectors and RRF fusion
- Docs: [Milvus architecture](https://milvus.io/docs/architecture_overview.md) · query and data nodes, object storage and the write-ahead log
- Docs: [Milvus multi-tenancy](https://milvus.io/docs/multi_tenancy.md) · database, collection, partition and partition-key strategies
- Docs: [OpenSearch vector search](https://docs.opensearch.org/latest/vector-search/) · `knn_vector` fields, engines, filtering and hybrid search
- Benchmark: [ANN-Benchmarks](https://ann-benchmarks.com/) · recall against throughput for nearest-neighbour algorithms
:::

## Wrap-up

### Cheat sheet

| Idea | Remember |
|---|---|
| Context engineering | Smallest high-signal context per step; write, select, compress, isolate |
| Context rot | Accuracy falls as context grows; middle of long prompts is used least |
| Slot order | Stable first (system, tools, memory), volatile last (history, task); question after documents |
| Managing growth | Trim at the source, clear old tool results, then compact; keep goal and recent turns verbatim |
| Prompt caching | Identical prefix reused at a fraction of the price; no timestamps or reordering in the prefix |
| Threads and checkpoints | State saved after every step per thread; any worker resumes; tools idempotent |
| LangGraph state | `TypedDict` with reducers; checkpointer plus `thread_id`; store for cross-thread memory |
| ADK state | Session events and state; `user:`, `app:`, `temp:` prefixes; memory service separate |
| Memory kinds | Semantic (facts), episodic (events), procedural (rules and behaviour) |
| Memory lifecycle | Write, consolidate (add, update, delete, no-op), recall (relevance, recency, importance), forget |
| Memory safety | Scope bound in code, sources on every memory, poisoning checks, user delete |
| Claude memory tool | `memory_20250818`, client-side file commands under `/memories`, validate every path |
| RAG pipeline | Parse, chunk with context, embed, index, retrieve, rerank, assemble, generate, check citations |
| Agentic RAG | Model decides when, what and where to search; rewrite, multi-hop, grade, cap the loop |
| Hybrid search | BM25 for identifiers, vectors for meaning, fuse with RRF, rerank the top candidates |
| Retrieval metrics | Recall@k first, then MRR or nDCG, precision; evaluate retrieval and generation separately |
| GraphRAG | Traverse entities for multi-hop questions; community summaries for global ones |
| Vector databases | pgvector first (vectors next to rows), Qdrant for filter-heavy search at scale, Milvus for billions, OpenSearch when search already runs there |
| Filtering | Filter inside the search for permissions; post-filtering returns too few and leaks; pgvector needs iterative scans |

### Practice

::: try
**1. Keep the newest history.** The context builder in "Context engineering" drops whole pieces by priority. Write a version that always keeps the system prompt and the task, reserves a number of tokens for the model's answer, and then keeps as many of the **most recent** history turns as fit, without skipping a turn in the middle.
:::

?? A solution to 1

```python
def tokens(text):
    return max(1, len(text) // 4)

def build_context(pieces, window, reserve_output=60):
    """Keep fixed slots, then the newest history turns that still fit; reserve room for the answer."""
    budget = window - reserve_output
    fixed = [p for p in pieces if p["slot"] != "history"]
    history = [p for p in pieces if p["slot"] == "history"]       # oldest first
    used = sum(tokens(p["text"]) for p in fixed)
    kept = []
    for p in reversed(history):                                     # newest first
        if used + tokens(p["text"]) > budget:
            break                                                   # stop: keep turns contiguous
        kept.insert(0, p); used += tokens(p["text"])
    return fixed[:-1] + kept + fixed[-1:], used                     # task stays last

pieces = [{"slot": "system", "text": "x" * 200}] + \
         [{"slot": "history", "text": f"turn {i} " + "y" * 120} for i in range(6)] + \
         [{"slot": "task", "text": "z" * 80}]
ctx, used = build_context(pieces, window=250)
print([p["text"][:6] if p["slot"] == "history" else p["slot"] for p in ctx], used)
# ['system', 'turn 3', 'turn 4', 'turn 5', 'task'] 163
```

Walking the history newest first and stopping at the first turn that does not fit keeps the kept turns contiguous; skipping an older turn while keeping one older still would make the conversation incoherent. The reserved output tokens matter: a context that fills the whole window leaves the model no room to answer.

::: try
**2. More metrics.** Add precision@k and nDCG@k (with graded relevance: 2 for a chunk that answers the question, 1 for a partly useful one) to the evaluation in "Hybrid search, reranking and evaluation of retrieval".
:::

?? A solution to 2

```python
import math

def precision_at(ranked, rel, k):
    return len(set(ranked[:k]) & rel) / k

def ndcg_at(ranked, grades, k):
    """grades: doc -> graded relevance (2 = answers it, 1 = partly, missing = 0)."""
    dcg = sum(grades.get(d, 0) / math.log2(i + 2) for i, d in enumerate(ranked[:k]))
    ideal = sorted(grades.values(), reverse=True)[:k]
    idcg = sum(g / math.log2(i + 2) for i, g in enumerate(ideal))
    return dcg / idcg if idcg else 0.0

print(round(precision_at(["d6", "d1", "d8"], {"d1", "d8"}, 3), 2))
# 0.67
print(round(ndcg_at(["d6", "d1", "d8"], {"d1": 2, "d8": 1}, 3), 2))
# 0.67    (the best document is second)
print(round(ndcg_at(["d1", "d8", "d6"], {"d1": 2, "d8": 1}, 3), 2))
# 1.0     (ideal order)
```

nDCG rewards putting the most relevant document first, which recall@k cannot see. Use it when you have graded labels and ordering matters, for example when only the first two passages fit the budget.

::: try
**3. What breaks downstream?** Using triples like those in "GraphRAG and knowledge graphs for agents", write `downstream(entity)` that returns every table or dashboard that depends, directly or indirectly, on a given table. Test it on `raw.refunds`.
:::

?? A solution to 3

```python
from collections import defaultdict

TRIPLES = [
    ("finance.revenue_daily", "built_from", "raw.orders"),
    ("finance.revenue_daily", "built_from", "raw.refunds"),
    ("dash.cfo_weekly", "reads", "finance.revenue_daily"),
    ("finance.margin_daily", "built_from", "finance.revenue_daily"),
    ("marketing.campaigns", "built_from", "raw.ad_spend"),
]
consumers = defaultdict(list)                  # reverse the edges: object -> subjects that use it
for s, r, o in TRIPLES:
    if r in ("built_from", "reads"):
        consumers[o].append(s)

def downstream(entity):
    seen, stack = [], [entity]
    while stack:
        for nxt in consumers[stack.pop()]:
            if nxt not in seen:
                seen.append(nxt); stack.append(nxt)
    return sorted(seen)

print(downstream("raw.refunds"))
# ['dash.cfo_weekly', 'finance.margin_daily', 'finance.revenue_daily']
```

Reverse the dependency edges and walk them. In production the edges come from lineage (OpenLineage events or the catalog), and the agent exposes `downstream` as a tool that returns paths, so its impact summary can cite each one.

::: try
**4. Design the memory.** An on-call assistant for a data platform team should remember (a) each engineer's preferred billing project and answer style, (b) past incidents and how they were fixed, (c) team rules such as "never backfill more than 7 days without approval", and (d) the state of an investigation that spans a whole night with handovers. For each, say which kind of memory it is, where you would store it on your own servers with LangGraph and PostgreSQL, and how it gets written.
:::

?? Answer to 4
(a) Semantic and procedural memory per user: a few stable keys (billing project, answer style) in the user's namespace of `PostgresStore`, plus extracted preferences from Mem0 self-hosted (a local model on Ollama, vectors in pgvector) scoped by `user_id`; written by explicit "remember" requests and background extraction after sessions. (b) Episodic memory: incident write-ups as a searchable corpus in a pgvector table (or Qdrant if it grows large), embedded with a local model, written by a post-incident job from the review document, recalled by similarity to the current alert. (c) Procedural memory at team scope: in the agent's instructions or a team namespace in the store, changed only through review, never by extraction from chat. (d) Short-term memory: one thread in `PostgresSaver`, with typed state fields (current hypothesis, checked tables, next steps) and compaction for the long night; handover means another engineer resumes the same `thread_id`.

::: try
**5. Fix the leak.** A teammate's retrieval tool runs `SELECT * FROM (SELECT chunk_id, tenant, body FROM chunks ORDER BY embedding <=> $1 LIMIT 5) AS top5 WHERE tenant = $2` against a pgvector table. Users of small tenants often get no results, and a reviewer flagged a security concern. Explain both problems and fix the query.
:::

?? Answer to 5
The tenant filter runs after the search (post-filtering): the inner query picks the top 5 across all tenants and the outer `WHERE` drops the rest, so small tenants often get nothing, and the search ranks other tenants' rows as part of a user's request. Put the filter in the same query as the ordering, `SELECT chunk_id, body FROM chunks WHERE tenant = $2 ORDER BY embedding <=> $1 LIMIT 5`, with an index on `tenant` and `SET hnsw.iterative_scan = strict_order` so the HNSW scan keeps going until five rows of that tenant are found (pgvector 0.8 and later). For strict isolation, add row-level security on `tenant` or partition the table per large tenant, and bind `$2` from the authenticated session, not from the model.

### What comes next

You can now decide what an agent sees at every step: build and order its context, keep it within budget with clearing, compaction and caching, carry state across steps with checkpoints, remember across sessions with semantic, episodic and procedural memory, and bring in knowledge with RAG, agentic retrieval, hybrid search, rerankers and graphs, on a self-hosted vector database. One technique kept appearing: giving a piece of work its own clean context, as a sub-agent. The next section, **Multi-agent systems**, takes that idea further: when several agents beat one, how they divide work and share context, how they communicate, and how to keep them from multiplying cost and errors.
