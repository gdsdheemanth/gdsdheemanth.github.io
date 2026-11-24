# Brief: write a section of the Agentic AI course

You are writing one section of a static, single-page course, "Agentic AI, end to end (for data engineers)". The reader is an experienced data engineer preparing for a **Data Engineer + Agentic AI** interview next week, so every topic must leave them able to explain the idea clearly, build it, and answer interview questions on it. Every article is written from scratch. The format is the one used in the companion courses: read `model_setup.md` (a finished Python section) in full before writing, and match its structure, tone and depth.

Working directory (A): `/Users/dheemanth/Desktop/projects/gdsdheemanth.github.io/_src/ai`. Everything below is relative to it.

## What to produce

For your section `<sid>`: `course/<sid>.md` (required) and `figs/<sid>.js` (2 to 5 diagrams where a picture genuinely helps: architectures, loops, sequences). Do not edit any other file, do not run `build.py`, do not commit.

Get the plan with `python3 skel.py <sid>`: groups, **exact** topic titles in order (match character for character), a scope note per topic (cover at least that), and the previous and next sections. The full section list is in `section_order.txt`.

## File format (same dialect as model_setup.md)

1. **Section intro** (before the first `##`): who it is for and what it assumes, "What you will be able to do afterwards" (bullets), the big-picture mental model. Then: "Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more."
2. **Groups**: `## <group title>` then a 1 to 3 paragraph bridge.
3. **Topics**: `### <topic title>`, then in order:
   - `::: plain` ... `:::` everyday words with an analogy, 2 to 5 sentences. Required.
   - **Explanation**: what it is, why it exists, how it works, with runnable Python examples (see Code), sequence descriptions, tables comparing options, and `> **Pitfall:**` / `> **Tip:**` callouts. Use data engineering analogies and examples where natural (the reader knows pipelines, SQL, Spark, Airflow, Kafka) on an open-source stack (PostgreSQL, DuckDB, Trino).
   - `::: deeper` ... `:::` internals, trade-offs, failure modes, version notes, research papers, and `> **Interview:**` callouts with model answers (at least one per topic).
   - `::: check` ... `:::` 2 or 3 `?? question` lines, each followed directly by its answer (no blank line between; blank line between questions). Required.
   - `::: resources` ... `:::` 4 to 7 resources: `- Type: [Title](https://url) · note` with Type in `Docs`, `Paper`, `Blog`, `Course`, `Video`, `Book`, `Repo`, `Spec`, `Article`. Prefer official docs and specs (Anthropic, Google Cloud, OpenAI, modelcontextprotocol.io, a2a-protocol.org, LangChain/LangGraph, adk.dev, which replaced google.github.io/adk-docs), the original papers (arxiv.org), and well-known engineering posts (Anthropic "Building effective agents", "Effective context engineering for AI agents", Google's agent whitepapers, Chip Huyen, Lilian Weng, Simon Willison, Hamel Husain on evals).
   - Length: about 1,000 to 1,400 words per topic before resources.
4. **`## Wrap-up`** with `### Cheat sheet` (two-column table, 10 to 20 rows), `### Practice` (3 to 5 `::: try` exercises each followed by `?? Answer to N` or `?? A solution to N`; solutions may be code after a blank line), and `### What comes next` (one paragraph naming the next section by its exact title).

Markdown supported: `##`/`###`/`####`, paragraphs, `- ` and `1. ` lists, fenced code (```python, ```json, ```yaml, ```bash, ```sql, ```text), `|` tables (escape a literal pipe as `\|`), callouts, `?? question` + answer, `::: plain|deeper|check|try|guide|resources` blocks closed by `:::`, `[[fig:key]]` alone on a line, inline `code`, **bold**, *italic*, [links](https://...). No HTML. Never write the literal text `</script`.

Cross-links: link other sections of this course with relative anchors (`#memory`, `#mcp`...). The data engineering course on the same site covers RAG, embeddings, vector databases and feature pipelines in depth (`index.html#aiml`), databases (`index.html#databases`), query engines (`index.html#queryengines`) and Trino (`index.html#engines`); link there instead of repeating data infrastructure details. The glossary is `agentic-ai-glossary.html`.

## Code: runnable and checked

- **No real model calls and no API keys.** From-scratch examples (the agent loop, tool dispatch, routing, ReAct, memory, RAG with tiny in-memory data, evaluation harnesses) must run with plain `python3` using a small **mock model** (a function that returns scripted responses in the provider's message shape) so the reader sees the real control flow. Verify they run and that output comments are exact.
- **Framework examples** (anthropic, claude-agent-sdk, openai, openai-agents, langgraph, langchain, google-adk, mcp, crewai, pydantic-ai, dspy) must match the real installed APIs. An isolated environment with these packages is at `.venv` (use `.venv/bin/python`). For each framework snippet, at minimum import every name it uses and, where possible without network or keys, construct the objects (for example build and compile a LangGraph graph with a fake model, define an ADK `Agent`, create a FastMCP server and list its tools). Never print or require API keys. If a package is missing or a name does not exist, fix the snippet to match the installed version and state the version in the text (`pip show <pkg>`).
- Installed versions: anthropic 2.13.0, claude-agent-sdk 0.2.188, openai 3.58.0, openai-agents 0.26.0, langgraph 1.2.24, langchain 1.5.2, langchain-core 1.8.4, google-adk 2.15.0, mcp 2.3.0, pydantic-ai 2.71.0, dspy 3.5.0. **crewai could not be installed** (its tiktoken dependency does not build on Python 3.14): write CrewAI snippets from the current docs, syntax-check them with `python3 -m py_compile`, and say so in your report.
- Google ADK 2.x: `SequentialAgent`, `ParallelAgent` and `LoopAgent` are deprecated in favour of the graph-based `google.adk.workflow.Workflow`; use `Workflow` in runnable examples and mention the old agents as version notes.
- Google Cloud agent product names are changing: Google's docs now use "Agent Platform Sessions", "Agent Platform Memory Bank" and "Agent Search (formerly Vertex AI Search)", and some pages call the managed runtime "Agent Runtime" rather than "Agent Engine". Use the name the current official docs use for whatever you describe, give the older name once, and be consistent within your section.
- Show model API examples in the official current SDK shape (for Claude, the Messages API with `tools`, `tool_use` and `tool_result` blocks; for Gemini on Google Cloud, the `google-genai` SDK or ADK).

## Facts and names

- Correct as of November 2026. Check official docs (web search/fetch is allowed, sparingly) for anything version-sensitive: MCP spec version and features, A2A, framework APIs, model names and capabilities, Google Cloud product names and GA status. Never invent APIs, benchmark scores or model names; leave details out if you cannot confirm them. Do not name specific model versions in prose unless verified; prefer "a current Claude or Gemini model".
- **On-prem and open source first (user requirement, 2026-11-24).** The reader's target job is on-prem. The course must not be bound to Google Cloud or BigQuery. Default stack for examples: PostgreSQL (with pgvector), DuckDB, Trino, Apache Spark, Kafka, Apache Airflow, dbt, MinIO or plain files for object storage, Qdrant, Milvus or OpenSearch for vectors, open-weight models served with vLLM, Ollama or TGI (plus hosted model APIs such as Claude where relevant), Langfuse, Arize Phoenix, OpenTelemetry, Prometheus and Grafana for observability, Docker and Kubernetes for deployment, HashiCorp Vault for secrets. Do **not** use BigQuery or other Google Cloud services as the main example anywhere; at most add a short "On Google Cloud" note naming the managed equivalent (one or two sentences per topic, only where it adds value). The `platforms` section has one topic dedicated to Google Cloud.
- Google Cloud names (verified November 2026), giving the former name once on first mention: Gemini Enterprise Agent Platform (formerly Vertex AI; its managed agent runtime is now called Agent Runtime, formerly Agent Engine), Knowledge Catalog (formerly Dataplex Universal Catalog), Managed Service for Apache Spark (formerly Dataproc), Managed Service for Apache Airflow (formerly Cloud Composer), BigQuery sharing (formerly Analytics Hub), Google Cloud Lakehouse (formerly BigLake). When a cloud managed service is worth naming, use the Google Cloud one (no AWS or Azure services); open-source and self-hosted tools come first.

## Diagrams

`figs/<sid>.js` with `Object.assign(FIGS, { <sid>_name:{ t:"Title", cap:"Caption", svg:S("<sid>1", 260, "aria", body) } });` using the helpers in `figs_helpers.js`: `S(markerId, height, aria, body)` 760-wide SVG, `B(x, y, w, h, cls, [lines], bold)` box, `A(markerId, "M.. L..", cls)` arrow (`ln`, `acc`, `kac`, `stc`, `dash`), `T(x, y, text, cls, anchor)` text (`sm`, `b`, `mono`). Box classes `box`, `fill`, `sp`, `st`, `ka`, `pr`, `okf`. Unique marker id per figure; keys start with `<sid>_`; reference each once with `[[fig:<sid>_name]]`; keep box text short (about 7 px per character). Load test: `node -e "eval(require('fs').readFileSync('figs_helpers.js','utf8')+require('fs').readFileSync('figs/<sid>.js','utf8')); console.log(Object.keys(FIGS))"`.

## Quality rules

- Plain, direct English; short sentences; define jargon on first use; no em dashes; no hype or marketing language.
- Every URL real: verify with `python3 linkcheck.py course/<sid>.md`, fix all `bad`.
- Write the file in parts (append group by group) so nothing is lost if interrupted.

## Checks before finishing

1. `python3 decheck.py <sid>`: 0 problems.
2. `python3 linkcheck.py course/<sid>.md`: no `bad` links.
3. Figures load; each key referenced once.
4. Every from-scratch example run with `python3`; every framework snippet import-checked (and constructed where possible) with `.venv/bin/python`.

## Report back (short)

Topics, words, figures, checks (programs run, snippets checked with package versions), blocked links kept, anything unresolved.
