import json
S=[]
def sec(part, sid, t, d, groups): S.append(dict(part=part, id=sid, t=t, d=d, groups=[dict(t=g, topics=[dict(t=x.split(" | ")[0], sub=(x.split(" | ")+[""])[1]) for x in items]) for g, items in groups]))
P1=("Foundations","spark","What agents are, how LLMs behave, and how agents call tools.")
sec(P1,"foundations","Agentic AI foundations","What an AI agent is, how large language models work from an agent builder's point of view, and the agent loop.",[
 ("Agents and LLMs",["What an AI agent is | definitions, agent vs chatbot vs workflow, the spectrum of autonomy, where agents work today",
   "LLM fundamentals for agent builders | tokens, context windows, next-token prediction, sampling, temperature, why models hallucinate",
   "Working with model APIs | messages and roles, system prompts, streaming, stop reasons, token counting, rate limits, choosing a model",
   "Prompting for agents | clear instructions, examples, XML tags and structure, prompting for tool use, prompt versioning"]),
 ("The agent loop",["The agent loop: observe, think, act | the core loop in code, stop conditions, max steps, state",
   "Structured outputs and JSON schemas | schema-constrained output, Pydantic models, validation and repair",
   "Reasoning models and extended thinking | thinking budgets, interleaved thinking, when reasoning helps and what it costs",
   "Workflows vs agents: when not to build an agent | deterministic workflows first, cost of autonomy, decision checklist"])])
sec(P1,"tools","Tool use and function calling","How models call tools, how to design tools agents can use well, and how to keep tool use safe.",[
 ("Function calling",["How function calling works | tool schemas, tool_use and tool_result blocks, the request-response cycle across providers",
   "Designing good tools | names, descriptions, parameter schemas, returning useful errors, fewer smarter tools vs many small ones",
   "Tool results, errors and retries | error handling the model can act on, retries, timeouts, partial results, large outputs",
   "Parallel and sequential tool calls | parallel calls, dependencies, ordering, fan-out limits"]),
 ("Powerful tools",["Code execution and sandboxes | code interpreters, sandboxing, containers, resource limits",
   "Computer use and browser agents | screenshot-and-act loops, browser automation, reliability limits",
   "Search and retrieval tools | web search, fetch, internal search, citations",
   "Tool security and permissions | least privilege, read vs write tools, approval gates, allowlists, audit"])])
P2=("Protocols and patterns","stream","The standards that connect agents to tools and to each other, and the design patterns agents are built from.")
sec(P2,"mcp","Model Context Protocol and agent interoperability","MCP for connecting agents to tools and data, A2A for agents talking to agents, and the other emerging standards.",[
 ("Model Context Protocol",["MCP architecture: hosts, clients and servers | why MCP exists, JSON-RPC, lifecycle, capability negotiation",
   "MCP primitives: tools, resources, prompts, sampling and elicitation | what each primitive is for, roots, notifications",
   "MCP transports, authorization and security | stdio, Streamable HTTP, OAuth 2.1, token handling, tool poisoning and other risks",
   "Building an MCP server in Python | FastMCP, exposing a database safely, testing with the inspector",
   "MCP for data platforms | PostgreSQL and other database MCP servers, MCP Toolbox for Databases, read-only access, query guards"]),
 ("Agents talking to agents",["Agent2Agent protocol (A2A) | agent cards, tasks, messages and artifacts, A2A vs MCP",
   "Other standards: agent skills, AGENTS.md and OpenAPI tools | packaging capabilities and instructions, interoperability trends"])])
sec(P2,"patterns","Agent design patterns","The reusable building blocks of agentic systems, from prompt chaining to ReAct, with code for each.",[
 ("Workflow patterns",["Prompt chaining | sequential steps with gates and checks",
   "Routing | classifying input and dispatching to specialised prompts or models",
   "Parallelization: sectioning and voting | concurrent subtasks, majority voting, aggregation",
   "Orchestrator-workers | dynamic task decomposition and delegation",
   "Evaluator-optimizer | generate, critique, refine loops"]),
 ("Agent patterns",["ReAct: reasoning and acting | the paper, the loop, traces, limits",
   "Plan-and-execute | separate planner and executor, replanning",
   "Reflection and self-correction | critique, verification against tools, when it helps",
   "Human-in-the-loop | approvals, interrupts, escalation, review queues"])])
P3=("Context and multi-agent systems","kafka","Giving agents the right information and memory, and coordinating many agents.")
sec(P3,"memory","Context engineering, memory and RAG","How to fill the context window with the right information: retrieval, memory and context management.",[
 ("Context",["Context engineering | what goes in the context window, ordering, compression, context rot",
   "Context window management: compaction, summarisation and caching | long conversations, prompt caching, token budgets",
   "Short-term memory and conversation state | threads, checkpoints, state schemas"]),
 ("Memory",["Long-term memory: semantic, episodic and procedural | memory stores, writing and recalling memories, forgetting",
   "Memory systems in practice | memory tools, vector and key-value memory, Mem0 and framework memory"]),
 ("Retrieval",["RAG fundamentals for agents | retrieval pipeline, chunking, embeddings, ranking, grounding and citations",
   "Agentic RAG | the agent decides when and how to retrieve, query rewriting, multi-hop retrieval, self-check",
   "Hybrid search, reranking and evaluation of retrieval | BM25 plus vectors, rerankers, recall and precision for RAG",
   "GraphRAG and knowledge graphs for agents | entity graphs, community summaries, when graphs beat vectors",
   "Vector databases for agents: pgvector, Qdrant, Milvus and OpenSearch | self-hosted options, choosing one, managed cloud equivalents as notes"])])
sec(P3,"multiagent","Multi-agent systems","When one agent is not enough: architectures, communication, and how multi-agent systems fail.",[
 ("Architectures",["Why and when to use multiple agents | specialisation, context isolation, parallelism, costs",
   "Supervisor and hierarchical architectures | manager agents, delegation, aggregation",
   "Handoffs and swarm architectures | passing control between agents, routing by capability",
   "Subagents and context isolation | spawning focused subagents, returning summaries, research agents"]),
 ("Coordination",["Shared state, blackboards and message passing | state graphs, shared memory, message buses",
   "Debate, voting and consensus | multi-agent debate, critics, aggregation of answers",
   "How multi-agent systems fail | coordination errors, loops, error compounding, cost blowups, a failure taxonomy"])])
P4=("Frameworks and platforms","prep","The frameworks and managed platforms used to build agents, compared hands-on.")
sec(P4,"frameworks","Agent frameworks","LangGraph, Google ADK, the Claude and OpenAI agent SDKs, CrewAI and the rest: how each works and when to choose it.",[
 ("Choosing",["Choosing an agent framework | framework vs plain SDK, control vs convenience, decision guide"]),
 ("Graph and code-first frameworks",["LangGraph | state graphs, nodes and edges, checkpointers, interrupts, persistence",
   "LangChain core concepts for agents | chat models, tools, runnables, when you still need it",
   "Google Agent Development Kit (ADK) | agents, tools, workflow agents, sessions and memory, multi-agent, deployment",
   "Claude Agent SDK | the agent harness, tools, subagents, hooks, permissions, sessions",
   "OpenAI Agents SDK | agents, handoffs, guardrails, sessions, tracing"]),
 ("Other frameworks",["CrewAI | crews, agents, tasks, flows",
   "Microsoft Agent Framework (AutoGen and Semantic Kernel) | conversations, teams, workflows",
   "PydanticAI, DSPy and LlamaIndex | typed agents, programmatic prompt optimisation, data-centric agents"])])
sec(P4,"platforms","Deploying and serving agents: on-prem and cloud","Running models and agents on your own infrastructure with open-source tools, and the managed cloud option on Google Cloud.",[
 ("On-prem and open source",["Serving open-weight models on-prem: vLLM, Ollama and TGI | open-weight models, GPUs and quantisation, OpenAI-compatible endpoints, tool calling with local models",
   "LLM gateways and model routing: LiteLLM and alternatives | one API for many models, fallbacks, budgets, keys, logging",
   "Deploying agents with Docker and Kubernetes | containers, APIs with FastAPI, streaming, scaling, long-running work, Kubernetes jobs"]),
 ("Enterprise and cloud",["Agent identity, secrets and networking in the enterprise | service accounts, Vault and secret stores, network isolation, air-gapped setups",
   "Running agents on Google Cloud | Gemini Enterprise Agent Platform, its managed agent runtime, Cloud Run and GKE, as an optional managed path"])])
P5=("Agents for data engineering","kafka","Where agentic AI meets data engineering: data agents, and the data infrastructure agents depend on.")
sec(P5,"dataagents","Agents for data engineering","Text-to-SQL and data agents, agents that operate pipelines, and the data engineering that agents depend on.",[
 ("Data agents",["Text-to-SQL agents | schema linking, few-shot examples, validation, self-correction, accuracy limits",
   "Semantic layers and metadata for agents | metrics definitions, data contracts, catalogs as agent context",
   "Data agents on open-source and cloud warehouses | agents over PostgreSQL, Trino and DuckDB; managed options such as BigQuery and Snowflake as notes",
   "Building an end-to-end data analyst agent | a worked project: question to SQL to chart to answer, with evaluation"]),
 ("Agents that run data platforms",["Agents for pipeline operations | incident triage, log analysis, Airflow and dbt assistants, auto-remediation limits",
   "Data quality, documentation and lineage agents | profiling, test generation, documentation generation, lineage questions"]),
 ("Data engineering for agents",["Unstructured data pipelines for agents | parsing PDFs and documents, chunking at scale, metadata",
   "Embedding pipelines and keeping knowledge fresh | batch and streaming embeddings, CDC to vector stores, re-embedding",
   "Governance and access control for agents on data | row and column security, PII, audit, data agent guardrails"])])
P6=("Quality, production and security","spark","Evaluating agents, observing them, securing them and running them economically in production.")
sec(P6,"eval","Evaluation and observability","How to measure whether an agent works, and how to see what it is doing in production.",[
 ("Evaluation",["Why agent evaluation is hard | non-determinism, long trajectories, many valid answers",
   "Building evaluation datasets | golden sets, synthetic cases, production samples, versioning",
   "LLM-as-a-judge | rubrics, pairwise comparison, judge bias, calibration with humans",
   "Trajectory and tool-call evaluation | step-level metrics, tool selection accuracy, efficiency",
   "Agent benchmarks | SWE-bench, tau-bench, GAIA, WebArena, BIRD and Spider for text-to-SQL, what benchmarks miss"]),
 ("Observability",["Tracing agents with OpenTelemetry | spans for LLM and tool calls, GenAI semantic conventions, Jaeger and Grafana Tempo",
   "Observability platforms | Langfuse, Arize Phoenix and LangSmith (self-hosted vs SaaS), Prometheus and Grafana, what to monitor"])])
sec(P6,"production","Production engineering, security and cost","Reliability, security, guardrails, cost and latency for agents in production.",[
 ("Reliability",["Reliability patterns for agents | timeouts, retries, idempotent tools, fallbacks, step limits",
   "Durable execution and long-running agents | checkpoints, resumability, Temporal and workflow engines",
   "Guardrails | input and output validation, policy checks, moderation, structured constraints"]),
 ("Security",["Prompt injection and agent security | direct and indirect injection, the lethal trifecta, mitigations",
   "Identity, authorisation and secrets for agents | acting on behalf of users, scoped credentials, OAuth delegation"]),
 ("Cost and experience",["Cost and latency optimisation | prompt caching, model routing, batching, smaller models, token budgets",
   "Agent UX: streaming, progress and trust | streaming steps, showing work, confirmations, handling failure",
   "Responsible AI and compliance for agents | safety, privacy, EU AI Act basics, documentation"])])
P7=("Interview preparation","prep","Getting ready for a data engineering + agentic AI interview.")
sec(P7,"interview","Agentic AI interview preparation","How these interviews run, system design and coding rounds for agents, and a question bank with model answers.",[
 ("Rounds",["How agentic AI interviews are structured | rounds, what is assessed, how data engineers are evaluated",
   "Coding round: build an agent loop from scratch | a complete tool-calling agent in plain Python with a mock model",
   "System design: an enterprise data analyst agent | requirements, architecture on Google Cloud, evaluation, security",
   "System design: a multi-agent support or operations system | routing, handoffs, memory, guardrails, scale",
   "Presenting your data engineering + AI experience | stories, projects, how to talk about RAG and agents you built"]),
 ("Question bank",["Agentic AI question bank | 50+ common questions with model answers, grouped by topic"])])
json.dump(S, open("skeleton.json","w"), indent=1, ensure_ascii=False)
print(len(S), "sections", sum(len(g["topics"]) for s in S for g in s["groups"]), "topics")
