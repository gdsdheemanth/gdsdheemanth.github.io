This section is for the reader who can write the agent loop by hand ([Agentic AI foundations](#foundations)), knows the workflow and agent patterns ([Agent design patterns](#patterns)), and has seen supervisors, handoffs and subagents ([Multi-agent systems](#multiagent)). Those sections used frameworks to show one pattern at a time. This one turns it around: it takes one small job and builds it in every major framework, so you can compare them side by side and choose with reasons rather than habit. You need the Python from earlier sections and nothing else.

**What you will be able to do afterwards**

- Decide whether a task needs a framework at all, and if so which one, using a decision table you can defend in an interview.
- Build the same agent in LangGraph, LangChain, Google's Agent Development Kit (ADK), the Claude Agent SDK, the OpenAI Agents SDK, CrewAI, Microsoft Agent Framework, PydanticAI, DSPy and LlamaIndex.
- Point each of them at an open-weight model you serve yourself with vLLM or Ollama, and say where that does not work.
- Explain each framework's core abstraction (graph, agent tree, harness, crew, typed agent, program) and its persistence, human approval, multi-agent and tracing story.
- Name the version changes that trip people up: LangChain's `create_agent`, ADK 2.x `Workflow`, AutoGen and Semantic Kernel becoming Microsoft Agent Framework, DSPy's tool-calling `ReAct`.

**The big picture**

Every framework in this section wraps the same loop: send the conversation and the tool schemas to a model, run the tools it asks for, append the results, repeat until it answers. They differ in what they add around that loop and in what they treat as the main object. LangGraph makes the loop an explicit graph with saved state. ADK makes it an agent in a tree, plus graph workflows. The Claude Agent SDK hands you a complete harness, the one that runs Claude Code. The OpenAI Agents SDK keeps a small set of primitives: agents, tools, handoffs, guardrails, sessions. CrewAI models a team with roles and tasks. PydanticAI adds types and dependency injection. DSPy treats prompts as parameters you optimise. LlamaIndex starts from your data.

[[fig:frameworks_layers]]

**The scenario used everywhere.** One small, realistic job: a sales analyst agent answers questions over an `orders` table in DuckDB (or PostgreSQL) and has exactly one tool, `run_sql`, which runs a read-only `SELECT` and returns at most 50 rows as text. The question is always "Which region had the most revenue in October 2026?". The model is an open-weight model behind an OpenAI-compatible endpoint: vLLM on `http://localhost:8000/v1` or Ollama on `http://localhost:11434/v1`. Every framework example imports this shared module, so the only thing that changes between examples is the framework.

If you work on data platforms, the comparison will feel familiar. Choosing an agent framework is like choosing between a hand-written Python ETL script, Airflow, Dagster and dbt: they all move data, but they disagree about what the main object is (a task, an asset, a model), how state is kept, and how much they decide for you.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

> **Tip:** How the examples were checked. The baseline, OpenAI Agents SDK and PydanticAI examples ran for real, against a small scripted server that speaks the OpenAI Chat Completions API on `localhost:8000`, the same way vLLM does. LangGraph, LangChain, ADK and DSPy examples ran offline with each framework's fake or scripted model. The Claude Agent SDK objects were constructed and their tool and hook were called directly; running the agent needs the Claude Code runtime and a model. CrewAI, Microsoft Agent Framework and LlamaIndex are not installed in the check environment, so their examples were written from the current official docs and syntax-checked with `python3 -m py_compile`. Versions: `openai` 3.58.0, `openai-agents` 0.26.0, `langgraph` 1.2.24, `langchain` 1.5.2, `langchain-core` 1.8.4, `google-adk` 2.15.0, `claude-agent-sdk` 0.2.188, `pydantic-ai` 2.71.0, `dspy` 3.5.0. DuckDB was not installed either, so the checks ran `shop.py` with Python's built-in `sqlite3` standing in for it; both accept this SQL and return the same rows.

## Choosing

Before the tour, the decision. Most teams pick a framework because a tutorial used it, then spend months working around its assumptions. This group gives you the questions to ask first, the plain-SDK baseline every framework must beat, and a decision table. The topics after it show each framework on the same scenario, so you can check the table against real code.

### Choosing an agent framework

::: plain
A framework is a kit of parts that saves you writing the agent loop, the tool plumbing, the state saving and the tracing yourself. Like a car kit, it is quicker than building from raw metal, but you get the kit maker's design choices with it. For a small agent, writing the loop yourself takes 30 lines and you understand all of it. For a long-running agent with approvals, restarts and several agents, a good framework saves weeks. The skill is knowing which situation you are in.
:::

**What it is.** An agent framework is a library that implements the agent loop and the things around it: tool schemas from Python functions, conversation state, persistence, retries, human approval, multi-agent wiring, streaming and tracing. A **model SDK** (the `openai` or `anthropic` client) only sends one request and returns one response. Everything else is yours.

**Why the choice matters.** The framework decides your main abstraction, and that is hard to change later. If your agent is really a fixed pipeline with one model step, a framework built around free-running agents will fight you. If your agent must pause for days waiting for approval, a framework without durable state will force you to build it yourself.

**The baseline: no framework.** Here is the shared scenario module, then the whole agent with only the `openai` client talking to a local vLLM server. Every framework example below must be judged against this:

```python
# shop.py: the scenario every framework in this section uses
import duckdb

DB = duckdb.connect()   # on a server: duckdb.connect("shop.duckdb", read_only=True)
DB.execute("CREATE TABLE orders (order_id INTEGER, region VARCHAR, amount DOUBLE, order_date DATE)")
DB.execute("""INSERT INTO orders VALUES
  (1, 'EU', 120.0, '2026-10-03'), (2, 'US', 80.5, '2026-10-04'),
  (3, 'EU', 200.0, '2026-10-15'), (4, 'APAC', 55.25, '2026-10-20'),
  (5, 'US', 310.0, '2026-10-28')""")

SCHEMA = "orders(order_id INTEGER, region VARCHAR, amount DOUBLE, order_date DATE)"
INSTRUCTIONS = f"You answer questions about sales. Table: {SCHEMA}. Use run_sql, then answer in one sentence."
QUESTION = "Which region had the most revenue in October 2026?"

def run_sql(sql: str) -> str:
    """Run one read-only SQL SELECT on the orders table and return the rows as CSV text."""
    if not sql.lstrip().lower().startswith("select"):
        return "error: only SELECT statements are allowed"
    try:
        cur = DB.execute(sql)
        rows = cur.fetchmany(50)                      # never hand the model a huge result
    except Exception as e:                            # errors go back to the model as text
        return f"error: {e}"
    header = ",".join(d[0] for d in cur.description)
    return "\n".join([header] + [",".join(str(v) for v in r) for r in rows])
```

For PostgreSQL, swap the connection for `psycopg.connect(...)` with a role that has only `SELECT` on the schema and a `statement_timeout`; the prefix check in code is a convenience, the database role is the real control. The agent itself:

```python
import json
from openai import OpenAI
from shop import run_sql, INSTRUCTIONS, QUESTION

client = OpenAI(base_url="http://localhost:8000/v1", api_key="unused")   # vLLM; Ollama: :11434/v1
TOOLS = [{"type": "function", "function": {
    "name": "run_sql", "description": run_sql.__doc__,
    "parameters": {"type": "object", "properties": {"sql": {"type": "string"}}, "required": ["sql"]}}}]

messages = [{"role": "system", "content": INSTRUCTIONS}, {"role": "user", "content": QUESTION}]
for step in range(5):                                   # hard cap on model calls
    reply = client.chat.completions.create(model="local-model", messages=messages, tools=TOOLS)
    msg = reply.choices[0].message
    messages.append(msg.model_dump(exclude_none=True))
    if not msg.tool_calls:
        break
    for call in msg.tool_calls:                          # we run the tool, not the model
        args = json.loads(call.function.arguments)
        messages.append({"role": "tool", "tool_call_id": call.id, "content": run_sql(**args)})
print(f"{step + 1} model calls")
print(msg.content)
# 2 model calls
# US had the most revenue in October 2026 (390.5), ahead of EU (320.0) and APAC (55.25).
```

`local-model` is whatever name vLLM serves (the `--served-model-name`, or the model path). vLLM needs tool calling switched on at start-up (`--enable-auto-tool-choice` and a `--tool-call-parser` that matches the model family); Ollama supports tools for models whose template includes them. That is the whole agent. What it lacks is everything production needs: state that survives a restart, a pause for approval, retries, tracing, streaming, tests. Frameworks sell those.

**The questions to ask.**

1. **Workflow or agent?** If the steps are known in advance, you want a workflow engine with model calls in it (LangGraph graph, ADK `Workflow`, Microsoft Agent Framework workflows, or even plain Airflow). If the model chooses the steps, you want an agent loop.
2. **How long does a run live?** Seconds in a request: anything works. Minutes to days with human approval: you need checkpoints in a database (LangGraph checkpointer, ADK `DatabaseSessionService`, Agent Framework checkpoints).
3. **Which models must it support?** On-prem means open-weight models behind vLLM, Ollama or TGI. Most frameworks accept an OpenAI-compatible endpoint. The Claude Agent SDK expects the Anthropic Messages API, so it needs a Claude model, an Ollama build with Anthropic compatibility, or a gateway that translates.
4. **One agent or many?** Handoffs (OpenAI Agents SDK), agent trees (ADK), graphs (LangGraph), crews (CrewAI) and orchestrations (Agent Framework) are different shapes; see [Multi-agent systems](#multiagent).
5. **How will you observe it?** Look for OpenTelemetry export to a self-hosted backend such as Langfuse or Arize Phoenix ([Evaluation and observability](#eval)). Some SDKs default to sending traces to their vendor; on-prem you switch that off.
6. **Who maintains it?** The team's language (Python, TypeScript, .NET, Java), the licence, release pace and how often the API has broken.

**The decision table.**

| If your situation is | Choose | Why |
|---|---|---|
| One agent, a few tools, short runs | Plain model SDK, or PydanticAI | Fewest moving parts; PydanticAI adds types and tests cheaply |
| Long runs, approvals, restarts, exact control of each step | LangGraph | Explicit graph, checkpointer per step, `interrupt`, PostgreSQL persistence |
| You want a ready loop with middleware on LangGraph | LangChain `create_agent` | Prebuilt agent that compiles to a LangGraph graph |
| Mixed fixed workflow and agents, Python or Java, Google Cloud later | Google ADK 2.x | `Agent` plus graph `Workflow`, sessions, evals, open source, deploys anywhere |
| Agent that works on files, runs commands, edits code | Claude Agent SDK | The Claude Code harness: tools, permissions, hooks, subagents, compaction |
| Small, readable multi-agent app with handoffs and guardrails | OpenAI Agents SDK | Few primitives, any OpenAI-compatible model, sessions, tracing |
| Role-based team for content or research tasks, fast prototype | CrewAI | Crews of role agents with tasks; Flows for structure |
| .NET or Microsoft shop, typed workflows, enterprise middleware | Microsoft Agent Framework | Successor to AutoGen and Semantic Kernel; Python and .NET |
| Output quality matters and you have labelled examples | DSPy | Optimises prompts and few-shot examples against a metric |
| Agent mostly answers over documents and indexes | LlamaIndex | Data connectors, indexes and query engines as tools |

[[fig:frameworks_decision]]

> **Pitfall:** Picking the framework before the design. Write down the steps, the state, the approval points and the failure handling first. Then pick the framework whose main abstraction matches that design. A design that fights the framework produces code that nobody can debug.

> **Tip:** Keep business logic out of the framework. Put SQL guards, schemas, prompts and tool functions in plain modules (like `shop.py`), and keep the framework layer thin. Then a framework change is a rewrite of 100 lines, not of the product.

::: deeper
**Control versus convenience.** Frameworks sit on a line. At one end, the plain SDK: total control, everything written by you. Next, thin libraries (PydanticAI, OpenAI Agents SDK): a loop and a few primitives, little hidden. Then orchestration frameworks (LangGraph, ADK `Workflow`, Agent Framework workflows): you design the graph, the framework runs it durably. At the far end, harnesses (Claude Agent SDK): a complete agent with tools, permissions and context management, where you configure rather than build. Anthropic's "Building effective agents" advice is to start with the simplest option and to understand the code underneath any framework you use, because hidden prompts and loops make failures hard to diagnose.

**What really costs time.** Teams rarely regret the loop they wrote. They regret missing durability (a run dies at step 14 of 20 and starts over), missing tracing (nobody can say why the agent ran a bad query), and leaky abstractions (the framework's hidden system prompt fights theirs). Evaluate frameworks on those three, with your model: run your real tools through each candidate for a day.

**Open-weight models change the picture.** Hosted frontier models follow tool schemas well. Smaller open-weight models served on your own GPUs make more tool-call mistakes: wrong argument names, JSON in the text instead of a tool call, calling tools that do not exist. Frameworks differ in how they handle that: retries with the validation error sent back (PydanticAI, LangChain middleware), structured output through tool calls, or parsers in DSPy adapters. Test the framework with the model you will actually run. On the server side, vLLM's tool parser must match the model's chat template, or tool calls arrive as plain text.

**Interoperability lowers the stakes.** Tools exposed as MCP servers ([Model Context Protocol and agent interoperability](#mcp)) work in every framework here, and agents exposed over A2A can call each other across frameworks. If your tools are MCP servers and your traces are OpenTelemetry, switching frameworks later is a much smaller job.

**On Google Cloud.** ADK agents can be deployed to Agent Engine, the managed agent runtime on Gemini Enterprise Agent Platform (formerly Vertex AI), and Agent Engine also hosts LangGraph and other framework agents. On-prem, every framework here runs in a container you deploy yourself ([Deploying and serving agents: on-prem and cloud](#platforms)).

> **Interview:** "Would you use a framework for a new agent?" It depends on the design. For one agent with a few tools and short runs, the model SDK or a thin library like PydanticAI is enough, and every line is visible. When the run must survive restarts, pause for approval, or coordinate several agents, I use a framework whose main abstraction matches the design: LangGraph for explicit graphs with checkpoints in PostgreSQL, ADK when we mix workflows and agents, the Claude Agent SDK when the agent works on files and commands. Whatever I pick, I keep tools and business rules in plain modules, expose tools over MCP, and trace with OpenTelemetry so we can switch later.

> **Interview:** "What does a framework give you that 30 lines of loop do not?" Durable state and resume, human-in-the-loop pauses, retries and error feedback to the model, streaming, multi-agent wiring, tracing, and tested handling of provider quirks such as parallel tool calls. The price is hidden prompts and behaviour, version churn and a learning curve.
:::

::: check
?? What does the plain-SDK baseline lack that a framework typically adds?
State that survives restarts, pauses for human approval, retries with error feedback, tracing, streaming and multi-agent wiring. The loop itself is the easy part.

?? Your agent must wait up to three days for a manager to approve a data deletion. Which capability decides the framework choice?
Durable checkpoints: the run's state saved to a database after each step, and a way to resume it later in another process (LangGraph checkpointer with `interrupt`, ADK database sessions, Agent Framework workflow checkpoints).

?? Why can the Claude Agent SDK not simply point at a vLLM OpenAI-compatible endpoint?
It speaks the Anthropic Messages API, not the OpenAI Chat Completions API. It needs a Claude model, a local server with Anthropic compatibility (Ollama since 0.14), or a gateway that translates between the two.
:::

::: resources
- Blog: [Building effective agents](https://www.anthropic.com/research/building-effective-agents) · start simple, frameworks versus direct API calls
- Blog: [How to think about agent frameworks](https://blog.langchain.com/how-to-think-about-agent-frameworks/) · workflows versus agents and what a framework should own
- Article: [A practical guide to building agents](https://cdn.openai.com/business-guides-and-resources/a-practical-guide-to-building-agents.pdf) · OpenAI's guide to single and multi-agent designs
- Docs: [Tool calling in vLLM](https://docs.vllm.ai/en/latest/features/tool_calling.html) · `--enable-auto-tool-choice` and tool parsers for open-weight models
- Docs: [OpenAI compatibility in Ollama](https://docs.ollama.com/api/openai-compatibility) · the `/v1` endpoint most frameworks can use
:::

## Graph and code-first frameworks

These five frameworks are the ones you are most likely to meet in a data engineering team, and the ones interviewers ask about most. LangGraph and LangChain are the most widely used open-source stack. ADK is Google's open-source framework, model-agnostic and runnable anywhere. The Claude Agent SDK and the OpenAI Agents SDK are the two model vendors' own agent libraries, with very different philosophies: a complete harness versus a few small primitives.

Each topic builds the sales analyst from the scenario, then covers what makes that framework different: state and persistence, human approval, multi-agent support and deployment.

### LangGraph

::: plain
LangGraph lets you draw your agent as a flowchart in code: boxes are steps, arrows are what happens next, and a shared notebook (the state) travels along the arrows. After every box, it photocopies the notebook and files the copy. If the process crashes, or must wait for a person, it picks up from the last copy. It is Airflow's DAG idea applied to an agent, except that the graph may loop.
:::

**What it is.** LangGraph is an open-source (MIT) library from LangChain Inc. for building stateful agents and workflows as graphs. It is low level on purpose: it does not hide a prompt or an agent loop from you. You define a **state schema**, **nodes** (functions that read the state and return updates) and **edges** (fixed or conditional transitions). A compiled graph runs in **supersteps**: all nodes scheduled for a step run, their updates are merged into the state, then the next step starts. This is the Pregel model from Google's graph processing paper.

**Why it exists.** Real agents need more than a loop: pausing for a human, resuming after a crash, replaying from an earlier step, branching in parallel, and mixing fixed steps with model-driven ones. LangGraph makes each of those a graph feature.

**The scenario as a graph.** The agent loop becomes two nodes: `agent` calls the model, `tools` runs the requested tools. `tools_condition` is a prebuilt routing function: it sends the run to `tools` when the last message has tool calls, otherwise to the end. A fake chat model with scripted replies stands in for the local model (run with `langgraph` 1.2.24 and `langchain-core` 1.8.4):

```python
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph, MessagesState, START
from langgraph.prebuilt import ToolNode, tools_condition
import shop

class FakeModel(GenericFakeChatModel):      # scripted replies; on-prem: ChatOpenAI(base_url=...) or ChatOllama
    def bind_tools(self, tools, **kwargs):
        return self

SQL = "SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region ORDER BY revenue DESC"
llm = FakeModel(messages=iter([
    AIMessage("", tool_calls=[{"name": "run_sql", "args": {"sql": SQL}, "id": "c1"}]),
    AIMessage("US had the most revenue in October 2026 (390.5), ahead of EU (320.0) and APAC (55.25).")]))

@tool
def run_sql(sql: str) -> str:
    """Run one read-only SQL SELECT on the orders table and return the rows as CSV text."""
    return shop.run_sql(sql)

model = llm.bind_tools([run_sql])

def agent(state: MessagesState):
    return {"messages": [model.invoke([SystemMessage(shop.INSTRUCTIONS)] + state["messages"])]}

g = StateGraph(MessagesState)
g.add_node("agent", agent)
g.add_node("tools", ToolNode([run_sql]))
g.add_edge(START, "agent")
g.add_conditional_edges("agent", tools_condition)       # tool calls -> "tools", else -> END
g.add_edge("tools", "agent")
app = g.compile(checkpointer=InMemorySaver(), interrupt_before=["tools"])

cfg = {"configurable": {"thread_id": "analyst-42"}}
app.invoke({"messages": [HumanMessage(shop.QUESTION)]}, cfg)
paused = app.get_state(cfg)
print(paused.next, paused.values["messages"][-1].tool_calls[0]["args"]["sql"])
# ('tools',) SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region ORDER BY revenue DESC
out = app.invoke(None, cfg)                               # approve: resume from the checkpoint
print(out["messages"][-1].content)
# US had the most revenue in October 2026 (390.5), ahead of EU (320.0) and APAC (55.25).
print(len(list(app.get_state_history(cfg))), "checkpoints")
# 5 checkpoints
```

To use the local model, replace the fake with `ChatOpenAI(model="local-model", base_url="http://localhost:8000/v1", api_key="unused")` from `langchain-openai`, or `ChatOllama(model=...)` from `langchain-ollama`; nothing else changes.

The pieces:

- **State.** `MessagesState` is a typed dictionary with one key, `messages`, whose **reducer** appends instead of overwriting. Your own state class can add fields (`table`, `approved`, `row_count`); keys without a reducer keep the last value written.
- **Checkpointer.** `compile(checkpointer=...)` saves a checkpoint after every superstep, keyed by `thread_id`. That is why there are five checkpoints: the input, then one after each step. `InMemorySaver` is for tests; production uses `PostgresSaver` from `langgraph-checkpoint-postgres` (or SQLite, Redis), covered in [Context engineering, memory and RAG](#memory).
- **Interrupts.** `interrupt_before=["tools"]` pauses before every tool call. The state is saved, `get_state` shows what the agent wants to run, and `invoke(None, cfg)` resumes. For approval inside a node, with an answer passed back, use `interrupt()` and `Command(resume=...)`, as in [Human-in-the-loop](#patterns).
- **Time travel.** `get_state_history` lists all checkpoints; you can resume from an older one, or `update_state` to edit the state (fix a wrong SQL argument) and continue from there.

[[fig:frameworks_graph]]

> **Pitfall:** A missing `thread_id`. With a checkpointer, every call needs `{"configurable": {"thread_id": ...}}`. Reusing one thread id for different users mixes their conversations; generating a new one per request loses the history. Derive it from your own conversation or job id.

> **Tip:** Draw the graph before debugging it. `app.get_graph().draw_mermaid()` returns a Mermaid diagram of the compiled graph, which is the fastest way to spot a missing edge in code review.

::: deeper
**Supersteps and parallel writes.** Nodes that run in the same superstep see the same input state; their updates are applied together at the end. Two writes to a key without a reducer in one step raise `InvalidUpdateError` instead of silently losing one (shown in [Multi-agent systems](#multiagent)). The `Send` API fans out a node over a list of inputs, and `Command` lets a node update state and choose the next node in one return value.

**Durability modes.** Checkpoints can be written synchronously after each step (safest), asynchronously while the next step runs (faster), or only at the end. A checkpoint captures the state and the pending writes, so a crash in the middle of a superstep resumes without re-running nodes that already finished. Side effects in nodes still need idempotency keys: a node that sent an email and crashed before its checkpoint will run again.

**Functional API.** Besides `StateGraph`, LangGraph has `@entrypoint` and `@task` decorators: plain Python control flow, with each `@task` result checkpointed. It suits code that is awkward as a graph, much like Airflow's TaskFlow API versus classic operators.

**Deployment.** A compiled graph is a Python object; you can serve it with FastAPI in your own container. LangChain also offers an Agent Server (the runtime formerly sold as LangGraph Platform) with task queues, cron, and a REST API, with a self-hosted option, and LangGraph Studio for visual debugging. LangSmith is their hosted tracing product; on-prem, Langfuse and Arize Phoenix both have LangChain and LangGraph integrations, and OpenTelemetry export works too.

**Version notes.** LangGraph 1.0 (October 2025) froze the core graph API. `langgraph.prebuilt.create_react_agent` was deprecated in favour of LangChain's `create_agent`, which builds the same loop with middleware. `ToolNode` and `tools_condition` remain in `langgraph.prebuilt`.

> **Interview:** "How does LangGraph persist and resume an agent?" A checkpointer saves the graph state after every superstep under a `thread_id`. To resume, you invoke the graph with the same `thread_id`: either `None` to continue after an `interrupt_before`, or `Command(resume=value)` after an `interrupt()` call in a node, in which case that node re-runs from its start and `interrupt()` returns the value. With `PostgresSaver`, the resume can happen in another process days later. Code before `interrupt()` must be idempotent.

> **Interview:** "Why model an agent as a graph rather than a loop?" Because production agents are rarely pure loops: they have fixed steps (validate input, check permissions), approval points, parallel branches and retries. A graph makes those explicit, testable per node, and checkpointed per step. The loop is still there (agent to tools and back), but it is one cycle in a larger, visible structure.
:::

::: check
?? What does a LangGraph checkpointer save, and when?
The graph state (and pending writes) after every superstep, keyed by the `thread_id` in the run config, so a run can be inspected, resumed or replayed.

?? What does `tools_condition` do in the example?
It is a routing function for a conditional edge: if the last AI message has tool calls it returns `"tools"`, otherwise it ends the run.

?? How do you approve a paused tool call in the example, and how would you reject it?
Approve with `app.invoke(None, cfg)`, which resumes from the checkpoint. To reject, update the state (`app.update_state`) with a tool message saying the call was refused, or route to an end node; then resume so the model sees the refusal.
:::

::: resources
- Docs: [LangGraph overview](https://docs.langchain.com/oss/python/langgraph/overview) · what LangGraph is and its core benefits
- Docs: [Graph API](https://docs.langchain.com/oss/python/langgraph/graph-api) · state, reducers, nodes, edges, `Send` and `Command`
- Docs: [Persistence](https://docs.langchain.com/oss/python/langgraph/persistence) · checkpointers, threads, state history and time travel
- Docs: [Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts) · pausing for humans and resuming with `Command`
- Repo: [langgraph](https://github.com/langchain-ai/langgraph) · source, examples and checkpointer packages
- Paper: [Pregel: a system for large-scale graph processing](https://research.google/pubs/pregel-a-system-for-large-scale-graph-processing/) · the superstep model LangGraph follows
:::

### LangChain core concepts for agents

::: plain
LangChain is the toolbox that LangGraph's agents are usually built from. It gives every model provider the same plug shape, turns Python functions into tools, and offers a ready-made agent so you do not have to draw the graph yourself. Think of it as the JDBC of language models: one interface, many drivers. You can use the plug without the rest of the toolbox.
:::

**What it is.** LangChain (MIT licence) is a set of packages: `langchain-core` (the interfaces: messages, chat models, tools, prompts, runnables), provider packages (`langchain-openai`, `langchain-ollama`, `langchain-anthropic` and many more) and `langchain` itself, which since version 1.0 is a small package centred on `create_agent`.

**The concepts that matter for agents.**

| Concept | What it is | Example |
|---|---|---|
| Chat model | A common interface over providers: `invoke`, `stream`, `batch`, `bind_tools`, `with_structured_output` | `ChatOllama`, `ChatOpenAI(base_url=...)`, `init_chat_model(...)` |
| Messages | Typed messages shared by all providers | `HumanMessage`, `AIMessage` with `tool_calls`, `ToolMessage` |
| Tool | A function plus a schema built from its signature and docstring | `@tool def run_sql(sql: str) -> str` |
| Runnable | Anything with `invoke`; runnables compose with `\|` | `prompt \| model \| parser` |
| Agent | `create_agent(model, tools, ...)`: the tool loop, compiled to a LangGraph graph | the scenario below |
| Middleware | Hooks around model and tool calls inside `create_agent` | call limits, summarisation, human approval, PII redaction |

**Both styles on the scenario.** First a fixed chain (a workflow: no loop, the model writes one SQL statement and code runs it), then the agent. Fake models stand in for the local one (run with `langchain` 1.5.2 and `langchain-core` 1.8.4):

```python
from langchain.agents import create_agent
from langchain.agents.middleware import ModelCallLimitMiddleware
from langchain.tools import tool
from langchain.messages import AIMessage, HumanMessage
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel, FakeListChatModel
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import shop

# 1. A runnable chain: prompt | model | parser (LCEL). No tools, no loop.
prompt = ChatPromptTemplate.from_messages([
    ("system", "Write one DuckDB SELECT for this table: {schema}. Return only SQL."),
    ("human", "{question}")])
sql_model = FakeListChatModel(responses=[
    "SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region ORDER BY revenue DESC LIMIT 1"])
text_to_sql = prompt | sql_model | StrOutputParser()
sql = text_to_sql.invoke({"schema": shop.SCHEMA, "question": shop.QUESTION})
print(shop.run_sql(sql))
# region,revenue
# US,390.5

# 2. A tool-calling agent: create_agent builds the LangGraph loop for you.
class FakeModel(GenericFakeChatModel):
    def bind_tools(self, tools, **kwargs):
        return self

@tool
def run_sql(sql: str) -> str:
    """Run one read-only SQL SELECT on the orders table and return the rows as CSV text."""
    return shop.run_sql(sql)

model = FakeModel(messages=iter([
    AIMessage("", tool_calls=[{"name": "run_sql", "args": {"sql": sql}, "id": "c1"}]),
    AIMessage("US had the most revenue in October 2026 (390.5).")]))
# on-prem: model = init_chat_model("openai:local-model", base_url="http://localhost:8000/v1", api_key="unused")
agent = create_agent(model, tools=[run_sql], system_prompt=shop.INSTRUCTIONS,
                     middleware=[ModelCallLimitMiddleware(run_limit=5)])
out = agent.invoke({"messages": [HumanMessage(shop.QUESTION)]})
print([type(m).__name__ for m in out["messages"]])
# ['HumanMessage', 'AIMessage', 'ToolMessage', 'AIMessage']
print(out["messages"][-1].content)
# US had the most revenue in October 2026 (390.5).
print(type(agent).__name__)
# CompiledStateGraph
```

The last line is the key fact: `create_agent` returns a compiled LangGraph graph. Everything from the previous topic applies: pass `checkpointer=` for persistence, stream it, embed it as a node in a bigger graph. `init_chat_model("openai:local-model", base_url=...)` needs the `langchain-openai` package; `init_chat_model("ollama:<model>")` needs `langchain-ollama`.

**Middleware.** Instead of subclassing the agent, you add middleware objects that run before or after the model call, around each tool call, or at the start and end of a run. The prebuilt ones cover the common production needs: `ModelCallLimitMiddleware` and `ToolCallLimitMiddleware` (budgets), `SummarizationMiddleware` and `ContextEditingMiddleware` (long contexts), `HumanInTheLoopMiddleware` (approve, edit or reject chosen tools), `PIIMiddleware` (redact or block personal data), `ModelRetryMiddleware`, `ToolRetryMiddleware` and `ModelFallbackMiddleware` (resilience), `LLMToolSelectorMiddleware` (pick a subset of many tools). Custom middleware uses decorators such as `@before_model` and `@wrap_tool_call`.

**When you still need LangChain.** Use the chat model and tool interfaces whenever you use LangGraph: they save you writing provider adapters. Use `create_agent` when the standard tool loop plus middleware fits. Use runnables for small fixed chains. Skip the rest: many older chain classes (`LLMChain`, `AgentExecutor`, retrieval chains) moved to the `langchain-classic` package in 1.0 and should not appear in new code.

> **Pitfall:** Old tutorials. Most LangChain examples on the web predate 1.0 and use `AgentExecutor`, `initialize_agent` or `create_react_agent`. They still circulate in interview take-home tasks. Recognise them, and write new code with `create_agent` and middleware.

> **Tip:** For open-weight models, add `ToolRetryMiddleware` and set `handle_tool_error=True` on tools so that a malformed call returns an error message the model can correct, rather than an exception that ends the run.

::: deeper
**Runnables and LCEL.** The LangChain Expression Language composes runnables with `|`. Every runnable gets `invoke`, `ainvoke`, `stream`, `batch` and tracing for free, so a three-step chain can be batched over 1,000 rows with a concurrency limit by calling `.batch(inputs, config={"max_concurrency": 8})`. That makes it handy for model calls inside a data pipeline, such as classifying support tickets in an Airflow task.

**Structured output.** `model.with_structured_output(PydanticModel)` uses the provider's native JSON schema mode or a tool call to return a validated object. `create_agent(..., response_format=Model)` does the same for the agent's final answer. With vLLM, structured output can use guided decoding, which constrains generation to the schema and fixes most format errors from small models.

**Standard content blocks.** LangChain 1.0 added provider-neutral content blocks (`message.content_blocks`) for reasoning, citations, images and server-side tool calls, so the same code reads output from different providers.

**Ecosystem cost.** LangChain's breadth is both the attraction and the risk: hundreds of integrations of mixed quality, and a history of breaking changes before 1.0. Pin versions, read the source of any integration you depend on, and prefer `langchain-core` interfaces over high-level helpers.

> **Interview:** "What is the relationship between LangChain and LangGraph?" LangGraph is the runtime: graphs, state, checkpoints, interrupts. LangChain provides the building blocks (chat model interface, messages, tools) and, since 1.0, `create_agent`, a prebuilt agent that compiles to a LangGraph graph and is extended with middleware. You can use LangGraph without LangChain's agent, and LangChain's chat models without LangGraph.

> **Interview:** "How would you add human approval to a LangChain agent?" Add `HumanInTheLoopMiddleware(interrupt_on={"run_sql": True})` and a checkpointer. When the model calls the tool, the run pauses with an interrupt describing the call; the reviewer's decision (approve, edit, reject) is sent back with `Command(resume=...)` on the same thread.
:::

::: check
?? What does `create_agent` return, and why does that matter?
A compiled LangGraph graph (`CompiledStateGraph`), so checkpointers, streaming, interrupts and embedding in larger graphs all work exactly as in LangGraph.

?? In the chain `prompt | model | parser`, what is a runnable?
Any component with the standard `invoke`, `stream` and `batch` interface. The `|` operator joins runnables so the output of one is the input of the next.

?? Name three prebuilt middleware and the problem each solves.
`ModelCallLimitMiddleware` caps model calls per run (runaway loops), `SummarizationMiddleware` compresses old history (context overflow), `HumanInTheLoopMiddleware` pauses for approval on chosen tools (risky actions).
:::

::: resources
- Docs: [LangChain overview](https://docs.langchain.com/oss/python/langchain/overview) · what LangChain 1.x is and how it relates to LangGraph
- Docs: [Agents](https://docs.langchain.com/oss/python/langchain/agents) · `create_agent`, tools, structured output and state
- Docs: [Models](https://docs.langchain.com/oss/python/langchain/models) · `init_chat_model`, tool binding and structured output
- Docs: [Middleware](https://docs.langchain.com/oss/python/langchain/middleware) · prebuilt and custom middleware
- Docs: [ChatOllama](https://docs.langchain.com/oss/python/integrations/chat/ollama) · local models through `langchain-ollama`
- Repo: [langchain](https://github.com/langchain-ai/langchain) · source of `langchain` and `langchain-core`
:::

### Google Agent Development Kit (ADK)

::: plain
ADK is Google's open-source kit for building agents. You describe an agent (its model, instructions and tools), and a runner plays it against a session, which is the record of one conversation. Agents can have helper agents underneath them, like a team lead with specialists. For work whose steps are known, ADK 2.x adds graph workflows, a flowchart where some boxes are agents and some are plain functions. It was built for Gemini but runs with other models, including ones on your own servers.
:::

**What it is.** The Agent Development Kit is an Apache 2.0 framework from Google, available for Python, Java, Go and TypeScript, and the one Google uses for its own agent products. Its pieces:

| Piece | What it does |
|---|---|
| `Agent` (alias of `LlmAgent`) | A model, an `instruction`, `tools`, optional `sub_agents`, `output_key`, `output_schema` and callbacks |
| Tools | Plain Python functions (schema from signature and docstring), `AgentTool`, `McpToolset` for MCP servers, built-in tools |
| `Runner` | Executes an agent or workflow for one session and yields **events** (model text, tool calls, tool results, state changes) |
| Session service | Stores sessions: events plus a `state` dictionary. `InMemorySessionService`, `DatabaseSessionService` (PostgreSQL, MySQL, SQLite), or the managed one on Google Cloud |
| Memory service | Long-term memory searched across sessions, with the `load_memory` and `preload_memory` tools |
| `Workflow` (2.x) | A graph of nodes (functions, tools, agents) with routes, joins, loops and human input |
| Callbacks | `before_model_callback`, `before_tool_callback`, `after_tool_callback` and others, to inspect or override each step |

**The scenario in ADK.** ADK's models are classes derived from `BaseLlm`. To run offline, the example defines a tiny scripted one; on-prem you would use `LiteLlm` (from the `google-adk[extensions]` extra) pointed at vLLM or Ollama. Run with `google-adk` 2.15.0:

```python
import asyncio
from google.adk.agents import Agent
from google.adk.models import BaseLlm, LlmResponse
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
import shop

SQL = "SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region ORDER BY revenue DESC"

class ScriptedLlm(BaseLlm):
    """Offline stand-in. On-prem: LiteLlm(model="openai/local-model", api_base="http://localhost:8000/v1")."""
    async def generate_content_async(self, llm_request, stream=False):
        last = llm_request.contents[-1].parts[0]
        if last.function_response:                       # the tool has answered: write the reply
            rows = last.function_response.response["result"].splitlines()[1]
            part = types.Part(text=f"{rows.split(',')[0]} had the most revenue ({rows.split(',')[1]}).")
        else:                                            # first call: ask for the tool
            part = types.Part(function_call=types.FunctionCall(name="run_sql", args={"sql": SQL}))
        yield LlmResponse(content=types.Content(role="model", parts=[part]))

def run_sql(sql: str) -> str:
    """Run one read-only SQL SELECT on the orders table and return the rows as CSV text."""
    return shop.run_sql(sql)

analyst = Agent(name="sales_analyst", model=ScriptedLlm(model="scripted"),
                instruction=shop.INSTRUCTIONS, tools=[run_sql], output_key="last_answer")

async def main():
    sessions = InMemorySessionService()                  # DatabaseSessionService(db_url=...) for PostgreSQL
    runner = Runner(app_name="sales", agent=analyst, session_service=sessions)
    s = await sessions.create_session(app_name="sales", user_id="ana")
    msg = types.Content(role="user", parts=[types.Part(text=shop.QUESTION)])
    async for ev in runner.run_async(user_id="ana", session_id=s.id, new_message=msg):
        for p in ev.content.parts if ev.content else []:
            kind = "call" if p.function_call else "result" if p.function_response else "text"
            print(ev.author, kind)
    s = await sessions.get_session(app_name="sales", user_id="ana", session_id=s.id)
    print(s.state["last_answer"])

asyncio.run(main())
# sales_analyst call
# sales_analyst result
# sales_analyst text
# US had the most revenue (390.5).
```

Three things to notice. The runner streams **events**, and the session is just the list of those events plus state: that is what gets persisted. `output_key="last_answer"` writes the agent's final text into `session.state`, which other agents and workflow nodes can read (instructions can reference state with `{last_answer}` templating). And the tool returned a string; ADK wraps non-dictionary results as `{"result": ...}`, which is why the scripted model reads `response["result"]`.

**Workflows: agents inside a graph.** In ADK 2.x, `google.adk.workflow.Workflow` is the way to combine fixed steps with agents. Here a plain function screens the question, routes write requests to a refusal, sends the rest to the same `analyst` agent, and an audit node records the answer. The edges are the whole design (continuing the example above, same version):

```python
from google.adk.events import Event
from google.adk.workflow import Workflow, START

def screen(node_input: types.Content):
    text = node_input.parts[0].text
    risky = any(w in text.lower() for w in ("delete", "drop", "update"))
    return Event(output=node_input, route="refuse" if risky else "ask")

def refuse(node_input):
    return "This assistant only reads data."

def audit(node_input):
    return f"logged answer: {node_input}"

flow = Workflow(name="sales_flow", edges=[
    (START, screen),
    (screen, {"ask": analyst, "refuse": refuse}),
    (analyst, audit),
])

async def main():
    sessions = InMemorySessionService()
    runner = Runner(app_name="sales", node=flow, session_service=sessions)
    for q in (shop.QUESTION, "Drop the orders table"):
        s = await sessions.create_session(app_name="sales", user_id="ana")
        msg = types.Content(role="user", parts=[types.Part(text=q)])
        async for ev in runner.run_async(user_id="ana", session_id=s.id, new_message=msg):
            if ev.output is not None and ev.node_info.name in ("audit", "refuse"):
                print(ev.node_info.name, "->", ev.output)

asyncio.run(main())
# audit -> logged answer: US had the most revenue (390.5).
# refuse -> This assistant only reads data.
```

The [Agent design patterns](#patterns) section shows `Workflow` gates, fan-out with `JoinNode`, and loops in more detail.

**Multi-agent.** An agent's `sub_agents` form a tree. In the default `chat` mode, the model transfers control by calling `transfer_to_agent`; ADK 2.x adds `task` and `single_turn` modes for sub-agents that return to the parent, and `AgentTool` wraps an agent as a tool. These are covered with examples in [Multi-agent systems](#multiagent). ADK also speaks A2A: `adk generate_agent_card` and the A2A helpers expose an agent to other frameworks.

**Tooling and deployment.** The `adk` command line is a large part of ADK's value: `adk web` starts a local web UI to chat with an agent and inspect every event, `adk api_server` serves agents over a FastAPI REST API, `adk eval` runs evaluation sets, and `adk deploy docker` (or `gke`) builds a container you can run on your own Kubernetes cluster.

> **Pitfall:** Expecting ADK to work only with Gemini. The default model strings are Gemini names, but `LiteLlm` reaches vLLM, Ollama, TGI or any OpenAI-compatible server. The catch is the extra install, `google-adk[extensions]`, and the model prefix: `openai/<id>` with `api_base` for an OpenAI-compatible server, `ollama_chat/<model>` for Ollama.

> **Tip:** Put guard rules in `before_tool_callback`. It receives the tool, its arguments and a tool context; returning a dictionary skips the tool and uses that dictionary as the result. That is the right place to block a non-`SELECT` statement or a query on a forbidden schema, because it runs in code on every call.

::: deeper
**Version notes.** ADK 1.x expressed fixed structures with workflow agents: `SequentialAgent`, `ParallelAgent` and `LoopAgent`. In 2.x they still import and run but are deprecated in favour of `Workflow`, which adds routes, joins, retries, timeouts and human input in one model. Expect both in existing code and in interviews. ADK 2.x also added sub-agent modes (`chat`, `task`, `single_turn`), parallel workers and `adk migrate` to help upgrade projects.

**State scopes.** Session state keys can carry prefixes: no prefix (this session), `user:` (all sessions of this user), `app:` (all users of this app) and `temp:` (this invocation only, never persisted). It is a simple way to keep, for example, a user's preferred currency without a separate memory system.

**Events and resumability.** Because everything is an event appended to the session, the session is an audit log you can replay. With `DatabaseSessionService` on PostgreSQL, a workflow that pauses for human input can be resumed later from another process; agents and nodes have `rerun_on_resume` to control what runs again.

**Evaluation built in.** `adk eval` compares an agent's tool-call trajectory and final answers with expected ones stored in eval set files, which makes regression tests part of the project from day one ([Evaluation and observability](#eval)). ADK emits OpenTelemetry spans, so traces can go to a self-hosted Phoenix or Langfuse.

**On Google Cloud.** `adk deploy agent_engine` deploys to Agent Engine on Gemini Enterprise Agent Platform (formerly Vertex AI), with the managed services the docs now call Agent Platform Sessions and Agent Platform Memory Bank; `VertexAiSessionService` is the matching session class. On-prem, the same agent runs in a container with `DatabaseSessionService`.

> **Interview:** "Explain ADK's architecture." An `Agent` holds a model, instruction and tools; a `Runner` executes it for a session and yields events; a session service stores the events and a state dictionary; a memory service holds long-term knowledge. Agents compose as a tree through `sub_agents`, and in 2.x as nodes in a graph `Workflow` alongside functions, with routes, joins and loops. It is model-agnostic through `LiteLlm`, has a CLI for a dev UI, an API server, evals and deployment, and deploys to Docker, Kubernetes or Google's Agent Engine.

> **Interview:** "SequentialAgent or Workflow?" In ADK 2.x, `Workflow`. `SequentialAgent` and its siblings are deprecated; `Workflow` expresses the same sequence with edges and adds conditional routes, fan-out with joins, loops, retries and human-in-the-loop, with functions as first-class nodes so not every step needs a model.
:::

::: check
?? What is an ADK session, physically?
A record with an id, the list of events from the runs in that conversation, and a `state` dictionary, stored by a session service (memory, a SQL database, or a managed service).

?? How do you run an ADK agent on a model served by vLLM?
Install `google-adk[extensions]` and pass `model=LiteLlm(model="openai/<served model id>", api_base="http://<host>:8000/v1")` to the `Agent`.

?? What replaced `SequentialAgent`, `ParallelAgent` and `LoopAgent` in ADK 2.x?
The graph-based `google.adk.workflow.Workflow`, whose edges connect functions, tools and agents, with routes, `JoinNode` fan-in and loops.
:::

::: resources
- Docs: [Agent Development Kit](https://adk.dev/) · overview, quickstarts and concepts
- Docs: [LLM agents](https://adk.dev/agents/llm-agents/) · `Agent`, instructions, tools, `output_key` and schemas
- Docs: [Graph workflows](https://adk.dev/graphs/) · `Workflow`, nodes, edges, routes and joins in 2.x
- Docs: [Sessions](https://adk.dev/sessions/) · sessions, state prefixes and session services
- Docs: [LiteLLM models](https://adk.dev/agents/models/litellm/) · using Ollama, vLLM and other providers
- Repo: [adk-python](https://github.com/google/adk-python) · source, samples and release notes
:::

### Claude Agent SDK

::: plain
Most frameworks give you parts to build an agent. The Claude Agent SDK gives you a finished one: the same engine that runs Claude Code, Anthropic's coding agent, as a library. It already knows how to read files, run commands, search, plan, manage a long conversation and ask permission. You decide which of those abilities it has, add your own tools, and set the rules. It is like hiring an experienced contractor and handing over a site rulebook, instead of training an apprentice from scratch.
:::

**What it is.** The Claude Agent SDK (Python package `claude-agent-sdk`, TypeScript `@anthropic-ai/claude-agent-sdk`) was called the Claude Code SDK until September 2025. It runs the Claude Code runtime as a subprocess (the Python package bundles it) and talks to it over a structured stream. You get the **agent harness**: the loop, built-in tools (`Read`, `Write`, `Edit`, `Bash`, `Glob`, `Grep`, `WebSearch`, `WebFetch`, todo lists), subagents, automatic context compaction, sessions, skills and project memory files, permission checks and hooks.

**Your tools as MCP.** Custom tools are MCP tools. `@tool` plus `create_sdk_mcp_server` runs an MCP server inside your Python process, with no separate server to deploy; tool names become `mcp__<server>__<tool>`. Constructed with `claude-agent-sdk` 0.2.188:

```python
import asyncio
from claude_agent_sdk import (ClaudeAgentOptions, AgentDefinition, HookMatcher, ResultMessage,
                              create_sdk_mcp_server, query, tool)
import shop

@tool("run_sql", "Run one read-only SQL SELECT on the orders table; returns CSV text.", {"sql": str})
async def run_sql(args):
    return {"content": [{"type": "text", "text": shop.run_sql(args["sql"])}]}

warehouse = create_sdk_mcp_server(name="warehouse", version="1.0.0", tools=[run_sql])

async def only_selects(input_data, tool_use_id, context):
    """PreToolUse hook: runs in your process before every matching tool call."""
    sql = input_data["tool_input"].get("sql", "")
    if not sql.lstrip().lower().startswith("select"):
        return {"hookSpecificOutput": {"hookEventName": "PreToolUse",
                "permissionDecision": "deny", "permissionDecisionReason": "read-only agent"}}
    return {}

options = ClaudeAgentOptions(
    system_prompt=shop.INSTRUCTIONS,
    mcp_servers={"warehouse": warehouse},
    tools=[],                                         # no built-in tools (no Bash, no file edits)
    allowed_tools=["mcp__warehouse__run_sql"],        # pre-approved, no permission prompt
    hooks={"PreToolUse": [HookMatcher(matcher="mcp__warehouse__run_sql", hooks=[only_selects])]},
    agents={"sql_reviewer": AgentDefinition(
        description="Reviews a SQL query for correctness before it is run.",
        prompt="You review SQL against the orders schema.", tools=[])},
    max_turns=6,
    # on-prem: a local model behind an Anthropic-compatible endpoint (Ollama >= 0.14, or a gateway)
    # env={"ANTHROPIC_BASE_URL": "http://localhost:11434", "ANTHROPIC_AUTH_TOKEN": "ollama"},
    # model="<local model name>",
)

async def main():
    async for message in query(prompt=shop.QUESTION, options=options):
        if isinstance(message, ResultMessage):
            print(message.result, message.total_cost_usd)

# asyncio.run(main())   # needs the Claude Code runtime and a model endpoint

# Offline checks: call the tool and the hook directly.
print(asyncio.run(run_sql.handler({"sql": "SELECT COUNT(*) AS n FROM orders"}))["content"][0]["text"])
# n
# 5
print(asyncio.run(only_selects({"tool_input": {"sql": "DELETE FROM orders"}}, "t1", None))
      ["hookSpecificOutput"]["permissionDecision"])
# deny
```

`query()` streams messages: the system init message, assistant messages with text and tool-use blocks, user messages with tool results, and a final `ResultMessage` with the result text, cost, usage and session id. For a multi-turn conversation in one process, `ClaudeSDKClient` keeps the session open and lets you send follow-ups and interrupt.

**Permissions, in order.** When the model wants a tool, the runtime checks six steps. **Hooks** run first and can deny the call or change its input (a hook's `allow` does not skip the next steps). **Deny rules** (`disallowed_tools`, settings files) block it, even in `bypassPermissions` mode. **Ask rules** from settings send it straight to your callback. The **permission mode** (`default`, `dontAsk`, `acceptEdits`, `plan`, `bypassPermissions`, `auto`) may approve it. **Allow rules** (`allowed_tools`) approve matching calls. Finally the **`can_use_tool` callback**, your code, decides anything still open: the place to send an approval request to a person. Separately, the `tools` option controls which built-in tools exist at all; `tools=[]` removes them, so the agent above can only run SQL. For a headless agent, `permission_mode="dontAsk"` turns every call that would need a prompt into a denial.

[[fig:frameworks_harness]]

**Hooks.** Hooks are callbacks on lifecycle events: `PreToolUse`, `PostToolUse`, `PostToolUseFailure`, `UserPromptSubmit`, `Stop`, `SubagentStart`, `SubagentStop`, `PreCompact`, `Notification`, `PermissionRequest`. A `HookMatcher` limits a hook to tool names (a regular expression). Typical uses: block dangerous inputs, write an audit log of every tool call to your own store, add context after a tool runs, or stop the agent stopping too early.

**Subagents and sessions.** `agents={...}` defines subagents with their own prompt, tools and model; the main agent calls them through its `Agent` tool, and each runs in a fresh context and returns a summary ([Subagents and context isolation](#multiagent)). Every run has a session id; `resume=<id>` continues it, `fork_session=True` branches it, and a custom `session_store` keeps transcripts in your own storage.

> **Pitfall:** Giving the harness its defaults in production. Without `tools=` and `allowed_tools=`, the agent may get `Bash` and file editing in its working directory. For a data agent, remove the built-in tools you do not need, run it in a container with no credentials it should not have, and keep `bypassPermissions` for sandboxes only.

> **Tip:** Cap cost and length in options: `max_turns`, `max_budget_usd`, and a `task_budget`. The `ResultMessage` reports `total_cost_usd` and usage per run; log them with the session id.

::: deeper
**Why a harness.** Anthropic's position is that the hard parts of a capable agent are not the loop but the environment: good tools, file system access as working memory, context compaction for long tasks, and verification. Claude Code proved that harness on coding work, and the SDK lets you use it for other domains: research agents, support agents, data agents that write and run SQL or dbt models in a sandbox. The trade-off is less control over the loop and a heavier runtime (a subprocess per agent).

**Models and on-prem.** The harness speaks the Anthropic Messages API. Options: Claude through Anthropic's API (or Claude on Google Cloud or another cloud provider, through environment variables), a gateway such as LiteLLM that exposes an Anthropic-compatible endpoint in front of other models, or Ollama, which since version 0.14 (January 2026) serves an Anthropic-compatible `/v1/messages` API for local models. Set `ANTHROPIC_BASE_URL` and a token in `env`. Expect weaker results with small local models: the harness's prompts and tools were tuned for Claude, and Ollama implements only a subset of the API.

**Settings and skills.** `setting_sources` chooses which settings files are loaded (`user`, `project`, `local`); project settings can carry permission rules and `CLAUDE.md` memory files. `skills` enables Agent Skills (folders of instructions and scripts the agent loads when relevant). Set `setting_sources` explicitly in production so that a stray settings file in the working directory cannot change the agent's permissions.

**Sandboxing.** `sandbox=SandboxSettings(...)` restricts commands' file system and network access at the operating system level, on top of permissions. For an agent that runs code, combine it with a container and a read-only database role ([Production engineering, security and cost](#production)).

> **Interview:** "How do permissions work in the Claude Agent SDK?" Layered. The `tools` option decides which built-in tools exist. For each call: hooks first (deny or modify), then deny rules, ask rules, the permission mode, allow rules, and last the `can_use_tool` callback, which is where a human approval flow goes. A hook deny and a deny rule hold even in `bypassPermissions`. For a read-only data agent: `tools=[]`, only the SQL tool in `allowed_tools`, a `PreToolUse` hook that validates the SQL, and a database role that cannot write anyway.

> **Interview:** "Claude Agent SDK or Anthropic's Messages API with your own loop?" The Messages API with a hand-written loop (or the Anthropic SDK's tool runner) when the agent is small and I want full control and low overhead. The Agent SDK when the task benefits from the harness: working on files, running commands, long tasks that need compaction, subagents and skills, with permissions and hooks already built.
:::

::: check
?? How are custom tools added to a Claude Agent SDK agent?
As MCP tools: define them with `@tool`, bundle them with `create_sdk_mcp_server` (an in-process MCP server), pass it in `mcp_servers`, and allow them by their `mcp__<server>__<tool>` names. External MCP servers (stdio or HTTP) work the same way.

?? A `PreToolUse` hook and `allowed_tools` disagree: the hook denies a call to an allowed tool. What happens?
The call is denied. Hooks run first, before the rules and the permission mode, and a hook's deny holds in every mode, including `bypassPermissions`.

?? What must change to run the SDK against a local open-weight model?
Point it at an Anthropic-compatible endpoint with `ANTHROPIC_BASE_URL` (and a token) in `env`, for example Ollama 0.14 or later or a translating gateway, and set `model` to the local model's name.
:::

::: resources
- Docs: [Agent SDK overview](https://code.claude.com/docs/en/agent-sdk/overview) · what the harness includes and when to use it
- Docs: [Agent SDK reference, Python](https://code.claude.com/docs/en/agent-sdk/python) · `query`, `ClaudeSDKClient`, `ClaudeAgentOptions`, types
- Docs: [Permissions](https://code.claude.com/docs/en/agent-sdk/permissions) · the order of hooks, rules, modes and `can_use_tool`
- Docs: [Hooks](https://code.claude.com/docs/en/agent-sdk/hooks) · lifecycle events and hook outputs
- Blog: [Building agents with the Claude Agent SDK](https://www.anthropic.com/engineering/building-agents-with-the-claude-agent-sdk) · the gather context, act, verify loop
- Docs: [Anthropic compatibility in Ollama](https://docs.ollama.com/api/anthropic-compatibility) · running Claude Code style clients on local models
:::

### OpenAI Agents SDK

::: plain
The OpenAI Agents SDK is a small box of parts with very few pieces in it: agents, tools, handoffs (one agent passing the conversation to another), guardrails (checks that can stop a run), and sessions (memory of the conversation). It deliberately leaves the rest to ordinary Python. Despite the name, it is not tied to OpenAI's models: any server that speaks the same API, including vLLM and Ollama on your own machines, works.
:::

**What it is.** `openai-agents` (MIT licence, Python and TypeScript) is the production successor to OpenAI's experimental Swarm library. Its primitives:

| Primitive | What it does |
|---|---|
| `Agent` | Name, `instructions`, `model`, `tools`, `handoffs`, guardrails, `output_type` |
| `Runner` | `Runner.run` (async), `run_sync`, `run_streamed`; runs the loop up to `max_turns` |
| `@function_tool` | Tool schema from the signature and docstring, validated with Pydantic |
| Handoffs | `handoffs=[other_agent]` gives the model a `transfer_to_<name>` tool |
| Guardrails | Input and output guardrails on agents, and input and output guardrails on tools |
| Sessions | Conversation memory: `SQLiteSession`, plus SQLAlchemy, Redis, MongoDB, Dapr and encrypted variants |
| Tracing | Spans for every model call, tool call, handoff and guardrail, with pluggable processors |

**The scenario.** This one ran for real against the scripted OpenAI-compatible server on `localhost:8000`, exactly as it would against vLLM (`openai-agents` 0.26.0, `openai` 3.58.0):

```python
import asyncio
from openai import AsyncOpenAI
from agents import (Agent, Runner, SQLiteSession, function_tool, input_guardrail, GuardrailFunctionOutput,
                    InputGuardrailTripwireTriggered, OpenAIChatCompletionsModel, set_tracing_disabled)
import shop

set_tracing_disabled(True)          # default exporter sends traces to OpenAI; on-prem, add your own processor
local = OpenAIChatCompletionsModel(model="local-model",
    openai_client=AsyncOpenAI(base_url="http://localhost:8000/v1", api_key="unused"))

@function_tool
def run_sql(sql: str) -> str:
    """Run one read-only SQL SELECT on the orders table and return the rows as CSV text."""
    return shop.run_sql(sql)

@input_guardrail(run_in_parallel=False)            # finish before the model is called
async def no_writes(ctx, agent, user_input) -> GuardrailFunctionOutput:
    text = user_input if isinstance(user_input, str) else str(user_input)
    risky = any(w in text.lower() for w in ("delete", "drop", "update", "truncate"))
    return GuardrailFunctionOutput(output_info={"risky": risky}, tripwire_triggered=risky)

sales = Agent(name="sales_analyst", instructions=shop.INSTRUCTIONS, model=local,
              tools=[run_sql], input_guardrails=[no_writes])

async def main():
    session = SQLiteSession("analyst-42")               # in memory; SQLiteSession("id", "chat.db") persists
    result = await Runner.run(sales, shop.QUESTION, session=session, max_turns=5)
    print(result.final_output)
    print([item.type for item in result.new_items])
    try:
        await Runner.run(sales, "Delete the APAC orders", session=session)
    except InputGuardrailTripwireTriggered:
        print("blocked by guardrail")
    print([i.get("role", i.get("type")) for i in await session.get_items()])

asyncio.run(main())
# US had the most revenue in October 2026 (390.5), ahead of EU (320.0) and APAC (55.25).
# ['tool_call_item', 'tool_call_output_item', 'message_output_item']
# blocked by guardrail
# ['user', 'function_call', 'function_call_output', 'assistant', 'user']
```

What the run shows:

- **Any OpenAI-compatible model.** `OpenAIChatCompletionsModel` with an `AsyncOpenAI` client on your own `base_url` is the on-prem path. The SDK's default for OpenAI's own models is the Responses API, which vLLM serves only partly; for local servers, use the Chat Completions model class. A `LitellmModel` extension covers other providers.
- **Run items.** `result.new_items` is the typed record of what happened: tool calls, tool outputs, messages, handoffs. Use it for logging and tests rather than parsing text.
- **Guardrails are tripwires.** A guardrail returns `tripwire_triggered=True` and the run raises an exception. By default input guardrails run in parallel with the first model call (lower latency, but the model call is wasted, and paid for, when the guardrail trips). `run_in_parallel=False` makes the guardrail finish first; the scripted server logged no model call for the blocked request. A guardrail can itself call a small, cheap model as a classifier.
- **Sessions store the conversation, including blocked input.** The last line shows the refused request saved as a `user` item. The next turn's model will see it, which may be what you want (context) or not (an injection attempt replayed every turn). Decide deliberately, and remove items with `session.pop_item()` if needed.

**Handoffs and agents as tools.** Multi-agent designs use `handoffs=[...]` (the receiving agent takes over the conversation) or `agent.as_tool()` (the calling agent stays in charge and gets a result back); both are shown with this SDK in [Multi-agent systems](#multiagent).

**Tracing on-prem.** Tracing is on by default and exports to OpenAI's trace viewer, which needs an OpenAI key and sends data out of your network. On-prem, either disable it, or replace the exporter: `set_trace_processors([...])` installs your own processors, and Langfuse, Arize Phoenix and other OpenTelemetry-based tools provide processors or instrumentation for this SDK.

> **Pitfall:** Forgetting tracing in an air-gapped network. With no OpenAI key, the default exporter logs errors on every run; with a key, traces containing your data leave the network. Make the tracing decision explicit at start-up, as in the first lines of the example.

> **Tip:** Put SQL validation in a tool input guardrail (`@tool_input_guardrail`) or inside the tool itself, not only in an input guardrail on the user's text. The user can ask harmlessly and the model can still write a bad query; the check must sit where the query is.

::: deeper
**The loop.** `Runner.run` calls the model with the agent's instructions, tools and handoffs. If the output has a final answer (text, or an object matching `output_type`) the run ends; if it has a handoff, the current agent changes and the loop continues; if it has tool calls, the SDK runs them (in parallel when the model asks for several) and loops. `max_turns` raises `MaxTurnsExceeded` when exceeded. `RunHooks` and `AgentHooks` give callbacks on agent start, tool start and end, and handoffs.

**Structured output and local models.** `output_type=SomeModel` asks for JSON matching a schema. With vLLM, structured output uses guided decoding, which is reliable; with Ollama, it depends on the model. Tool calling with small open-weight models fails more often, so add `ModelSettings(tool_choice="required")` for the first step when a tool is always needed, keep tool counts small, and test.

**Human in the loop.** A tool declared with `@function_tool(needs_approval=True)` makes the run stop with pending calls in `result.interruptions`. `result.to_state()` gives a `RunState` you can serialise (`to_json`) and store; after a person decides, `state.approve(...)` or `state.reject(...)` and `Runner.run(agent, state)` resume the run. Combined with a persistent session, this gives a pause and resume pattern, though not the per-step checkpointing LangGraph offers.

**Realtime and voice.** The SDK also has realtime and voice agents built on the same primitives. They depend on OpenAI's realtime models, so they are less relevant on-prem.

> **Interview:** "Explain guardrails in the OpenAI Agents SDK." Functions that check input (before or alongside the first model call), output (after the final answer) or tool calls, and return a result with `tripwire_triggered`. When a tripwire fires, the run stops with an exception that the application handles. They can be plain code or a small classifier model. They are a control layer, not the only one: permissions in the database and validation inside tools still apply.

> **Interview:** "How would you run the OpenAI Agents SDK against a self-hosted model?" Create an `AsyncOpenAI` client with `base_url` pointing at vLLM or Ollama's `/v1` endpoint, wrap it in `OpenAIChatCompletionsModel`, and pass it as the agent's `model`. Disable or replace the default tracing exporter, and test tool calling with that model, since tool-call reliability is the main difference from hosted models.
:::

::: check
?? Why use `OpenAIChatCompletionsModel` rather than a model name string with a local server?
A plain model string makes the SDK use OpenAI's default client and, for OpenAI models, the Responses API. A local vLLM or Ollama server is reached most reliably through the Chat Completions API with a client whose `base_url` points at it.

?? What is the trade-off of running an input guardrail in parallel with the model?
Lower latency when the guardrail passes, but when it trips, the model call already started is wasted and paid for (or uses GPU time). `run_in_parallel=False` makes the guardrail finish first.

?? Where do traces go by default, and why does that matter on-prem?
To OpenAI's tracing backend. That needs an OpenAI key and sends prompts and outputs outside your network, so on-prem you disable it or install your own trace processors.
:::

::: resources
- Docs: [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/) · primitives, quickstart and design
- Docs: [Models](https://openai.github.io/openai-agents-python/models/) · Chat Completions and Responses models, other providers
- Docs: [Guardrails](https://openai.github.io/openai-agents-python/guardrails/) · input, output and tool guardrails and tripwires
- Docs: [Sessions](https://openai.github.io/openai-agents-python/sessions/) · conversation memory backends
- Docs: [Tracing](https://openai.github.io/openai-agents-python/tracing/) · spans, processors and external backends
- Repo: [openai-agents-python](https://github.com/openai/openai-agents-python) · source and examples
:::

## Other frameworks

The frameworks in this group are just as real in production, but each starts from a different idea of what an agent is. CrewAI thinks in teams of roles. Microsoft Agent Framework merges two older Microsoft projects into one enterprise framework for Python and .NET. PydanticAI, DSPy and LlamaIndex each specialise: types and testing, prompt optimisation, and data. Interviewers mostly want to know that you can place them on the map and say when you would reach for each.

The same scenario continues. Where a framework is not installed in the check environment, the code was written from its current documentation and syntax-checked; the text says so.

### CrewAI

::: plain
CrewAI describes work the way a manager staffs a project. You define agents as job roles (a SQL analyst, a report writer), each with a goal and a short backstory, and tasks with a description and the expected result. A crew is the team plus the task list. You start the crew, and it works through the tasks, passing each result to the next. For work that needs a fixed structure, Flows add ordinary event-driven code around the crews.
:::

**What it is.** CrewAI (MIT licence) is a Python framework built from scratch, independent of LangChain. It has two layers:

| Layer | Pieces | Use it for |
|---|---|---|
| Crews | `Agent` (role, goal, backstory, tools, llm), `Task` (description, expected output, agent), `Crew` (agents, tasks, `process`) | Autonomous collaboration on a task list |
| Flows | `Flow` with typed state, `@start`, `@listen`, `@router`, `@persist` | Deterministic, event-driven structure, calling crews or single model calls as steps |

The two processes are `Process.sequential` (tasks in order, each output passed as context to the next) and `Process.hierarchical` (a manager agent, with its own model, plans, delegates and checks the work).

**The scenario as a crew, inside a flow.** CrewAI could not be installed in the check environment (its `tiktoken` dependency does not build on Python 3.14), so this was written from the current CrewAI docs and checked with `python3 -m py_compile`. The model is a local OpenAI-compatible server; the docs' Ollama form is `LLM(model="ollama/<model>", base_url="http://localhost:11434")`, which needs the `crewai[litellm]` extra:

```python
from typing import Literal
from pydantic import BaseModel
from crewai import Agent, Crew, LLM, Process, Task
from crewai.flow.flow import Flow, listen, router, start
from crewai.tools import tool
import shop

local = LLM(model="openai/local-model", base_url="http://localhost:8000/v1", api_key="unused")

@tool("run_sql")
def run_sql(sql: str) -> str:
    """Run one read-only SQL SELECT on the orders table and return the rows as CSV text."""
    return shop.run_sql(sql)

analyst = Agent(role="Sales data analyst",
                goal="Answer sales questions with numbers taken from the orders table",
                backstory=f"You know SQL well and you only trust data you queried. Table: {shop.SCHEMA}",
                tools=[run_sql], llm=local, allow_delegation=False, max_iter=5)
writer = Agent(role="Report writer", goal="Turn analysis into one clear sentence for managers",
               backstory="You write short, precise business summaries.", llm=local)

analyse = Task(description="Answer this question with SQL: {question}",
               expected_output="The query result rows and which region is highest", agent=analyst)
summarise = Task(description="Write the answer to '{question}' in one sentence.",
                 expected_output="One sentence with the region and its revenue", agent=writer)
crew = Crew(agents=[analyst, writer], tasks=[analyse, summarise], process=Process.sequential)

class SalesState(BaseModel):
    question: str = ""
    answer: str = ""

class SalesFlow(Flow[SalesState]):
    @start()
    def receive(self):
        self.state.question = self.state.question.strip()

    @router(receive)
    def screen(self) -> Literal["ask", "refuse"]:
        risky = any(w in self.state.question.lower() for w in ("delete", "drop", "update"))
        return "refuse" if risky else "ask"

    @listen("ask")
    def ask_crew(self):
        result = crew.kickoff(inputs={"question": self.state.question})
        self.state.answer = result.raw
        return self.state.answer

    @listen("refuse")
    def refuse(self):
        self.state.answer = "This assistant only reads data."
        return self.state.answer

if __name__ == "__main__":
    print(SalesFlow().kickoff(inputs={"question": shop.QUESTION}))
```

The `{question}` placeholders in task descriptions are filled from `kickoff(inputs=...)`. `result.raw` is the final task's text; `result.pydantic` holds a typed object if the task sets `output_pydantic=`. `@persist` on the flow class saves its state after each step (SQLite by default), so a flow can resume.

**When it fits.** CrewAI is quick to prototype multi-role work that reads like a team: research then write, analyse then review. The role, goal and backstory fields are prompt engineering with a friendly shape. For a single tool-calling agent like our sales analyst, it adds little; the scenario uses two agents only to show the model.

> **Pitfall:** Hidden defaults that leave the network. Crew memory and knowledge features need an embedding model and default to a hosted provider; configure a local `embedder` (for example Ollama) or leave memory off. CrewAI also sends anonymous telemetry by default; set `CREWAI_DISABLE_TELEMETRY=true` (or `OTEL_SDK_DISABLED=true`) on-prem.

> **Tip:** Keep `allow_delegation=False` unless you need it, and set `max_iter` per agent. Delegation lets agents ask each other for help, which multiplies model calls in ways that are hard to predict.

::: deeper
**How a sequential crew runs.** Each task becomes a prompt for its agent built from the role, goal, backstory, task description, expected output and the outputs of earlier tasks. The agent runs a tool loop until it produces a final answer for the task. Then the next task starts. It is a prompt-chaining workflow ([Agent design patterns](#patterns)) whose steps are small agents.

**Hierarchical process.** A manager agent (set with `manager_llm` or `manager_agent`) receives the tasks, decides who does what, and validates results. It is the supervisor pattern from [Multi-agent systems](#multiagent), with the same costs: more tokens and a single point that can misroute.

**Crews versus Flows.** CrewAI's own guidance has moved towards Flows as the production backbone: deterministic code for the structure, crews only where autonomy pays. That mirrors the advice in this course: start with a workflow, add agency where it helps.

**Tools and MCP.** Tools are `BaseTool` subclasses or `@tool` functions; the `crewai-tools` package adds many ready-made ones, and MCP servers can be attached as tool sources. Guardrails on tasks validate an output and send it back for a retry when it fails.

> **Interview:** "Compare CrewAI with LangGraph." CrewAI models agents as roles in a crew working through tasks, sequentially or under a manager, with Flows for deterministic structure; it is quick to set up and reads naturally, but gives less control over each step. LangGraph is a lower-level graph runtime: you define state, nodes and edges explicitly and get per-step checkpoints, interrupts and time travel. I would prototype a role-based content or research pipeline in CrewAI, and build a long-running, approval-heavy production agent in LangGraph.

> **Interview:** "What are role, goal and backstory for?" They are structured prompt components: who the agent is, what success means, and the context that shapes its behaviour. They help with consistency across agents, but they are still prompts; evaluate their effect like any prompt change.
:::

::: check
?? What is the difference between `Process.sequential` and `Process.hierarchical`?
Sequential runs tasks in the listed order, each agent receiving earlier outputs as context. Hierarchical adds a manager agent that assigns tasks to agents and validates their results.

?? What do CrewAI Flows add on top of crews?
Deterministic, event-driven structure in code: typed state, steps triggered by `@start` and `@listen`, branching with `@router`, and persistence with `@persist`, with crews called as steps where autonomy is useful.

?? Name two CrewAI defaults to change for an air-gapped deployment.
Telemetry (set `CREWAI_DISABLE_TELEMETRY=true`) and the embedding provider used by memory and knowledge (configure a local embedder or disable memory). Also point every `LLM` at a local endpoint.
:::

::: resources
- Docs: [Crews](https://docs.crewai.com/en/concepts/crews) · crew attributes, processes and kickoff
- Docs: [Agents](https://docs.crewai.com/en/concepts/agents) · role, goal, backstory, tools and delegation
- Docs: [Tasks](https://docs.crewai.com/en/concepts/tasks) · descriptions, expected outputs, structured outputs and guardrails
- Docs: [Flows](https://docs.crewai.com/en/concepts/flows) · state, `@start`, `@listen`, `@router` and `@persist`
- Docs: [LLMs](https://docs.crewai.com/en/concepts/llms) · Ollama, local OpenAI-compatible servers and other providers
- Repo: [crewAI](https://github.com/crewAIInc/crewAI) · source and examples
:::

### Microsoft Agent Framework (AutoGen and Semantic Kernel)

::: plain
Microsoft had two popular agent projects: AutoGen, from Microsoft Research, which was good at agents talking to each other, and Semantic Kernel, which was good at enterprise plumbing. Microsoft Agent Framework merges them into one, for Python and .NET, built by the same teams. Think of it as two libraries' best parts in one box, with a new graph workflow engine added, so you can choose between free conversation between agents and a fixed, checkable process.
:::

**What it is.** Microsoft Agent Framework (MIT licence; Python, .NET, and Go in preview) is the direct successor to AutoGen and Semantic Kernel. Its docs describe four areas:

| Area | What it gives you |
|---|---|
| Agents | `Agent` with a chat client, instructions and tools (plain functions, MCP tools), sessions, middleware, context providers for memory |
| Harness agent | An opinionated agent for long tasks: planning and todos, context compaction, file access and memory, tool approval |
| Workflows | Graph workflows (`WorkflowBuilder`, executors, edges) and a functional `@workflow` API, with checkpoints and human-in-the-loop |
| Integrations | Model providers (OpenAI, Anthropic, Ollama, Foundry, Gemini and others), hosting, A2A, evaluation, UI |

**The scenario.** Agent Framework is not installed in the check environment, so this was written from the current Microsoft Learn docs (Ollama and session pages, October to November 2026) and syntax-checked with `py_compile`. The docs show `OpenAIChatClient` with a custom `base_url` for any OpenAI-compatible server, and a native `OllamaChatClient` in the `agent-framework-ollama` package:

```python
import asyncio
from typing import Never
from agent_framework import Agent, WorkflowBuilder, WorkflowContext, executor
from agent_framework.openai import OpenAIChatClient
import shop

def run_sql(sql: str) -> str:
    """Run one read-only SQL SELECT on the orders table and return the rows as CSV text."""
    return shop.run_sql(sql)

client = OpenAIChatClient(api_key="unused", base_url="http://localhost:8000/v1", model="local-model")
analyst = Agent(client=client, name="sales_analyst", instructions=shop.INSTRUCTIONS, tools=run_sql)

# A graph workflow: screen the request in code, then call the agent.
@executor(id="screen")
async def screen(question: str, ctx: WorkflowContext[str, str]) -> None:
    if any(w in question.lower() for w in ("delete", "drop", "update")):
        await ctx.yield_output("This assistant only reads data.")
    else:
        await ctx.send_message(question)

@executor(id="ask")
async def ask(question: str, ctx: WorkflowContext[Never, str]) -> None:
    result = await analyst.run(question)
    await ctx.yield_output(str(result))

builder = WorkflowBuilder(start_executor=screen)
builder.add_edge(screen, ask)
workflow = builder.build()

async def main():
    session = analyst.create_session()                  # multi-turn conversation state
    print(await analyst.run(shop.QUESTION, session=session))
    events = await workflow.run("Drop the orders table")
    print(events.get_outputs())

if __name__ == "__main__":
    asyncio.run(main())
```

`WorkflowContext[str, str]` declares that `screen` may send a `str` to the next executor and yield a `str` as workflow output; `WorkflowContext[Never, str]` means "sends nothing, yields text". The builder checks these types along the edges when it builds the graph.

**How the workflow engine runs.** Workflows use a Pregel-style superstep model, like LangGraph: all executors with pending messages run in a step, there is a barrier, then messages are delivered for the next step. Checkpoints are saved at superstep boundaries, which is what makes long-running and human-in-the-loop workflows resumable. Prebuilt orchestrations (sequential, concurrent, handoff, group chat and Magentic, a manager-led pattern from AutoGen's research) sit on the same engine.

**AutoGen and Semantic Kernel today.** You will still meet both in code bases. AutoGen 0.4 and later (`autogen-agentchat`) had `AssistantAgent` and teams such as `RoundRobinGroupChat` and `SelectorGroupChat`, where agents take turns in a shared conversation until a termination condition. Semantic Kernel had a `Kernel` with plugins (`@kernel_function`) and `ChatCompletionAgent`. Microsoft publishes migration guides from both to Agent Framework and directs new work there.

> **Pitfall:** Treating "Microsoft" as "Azure only". The framework runs against Ollama, any OpenAI-compatible server and Anthropic, and hosts in your own containers. The docs' quickstarts default to Microsoft Foundry; for on-prem, start from the Ollama and OpenAI-compatible pages.

> **Tip:** If your company is a .NET shop, this is the strongest choice in the list: the .NET and Python APIs share concepts, so a Python prototype and a C# production service can use the same design.

::: deeper
**Conversations versus workflows.** AutoGen's core idea was the multi-agent conversation: agents exchange messages in a group chat and a selector decides who speaks next. It is flexible and good for exploration, but hard to test, because the path is decided at run time by models. Agent Framework keeps that style as orchestrations, and adds typed graph workflows where the path is code. The docs' guidance matches this course: use an agent when the task is open-ended, a workflow when the steps are known, and a function when you can write one.

**Middleware and context providers.** Middleware intercepts agent runs, model calls and function calls (logging, policy checks, redaction). Context providers inject memory and retrieved knowledge into each call and update it afterwards, a structured version of the retrieval patterns in [Context engineering, memory and RAG](#memory). Tool approval is built in: a function tool can require approval, and the run returns approval requests the application answers.

**Observability.** Agent Framework emits OpenTelemetry traces and metrics following the GenAI semantic conventions, so they go straight to a self-hosted collector, Grafana Tempo, Phoenix or Langfuse.

**Version note.** Agent Framework renamed several early preview names: the conversation state object is now `AgentSession` (created with `agent.create_session()`, serialised with `to_dict()`), where early previews used threads. Check the date on any blog post you copy from.

> **Interview:** "What happened to AutoGen and Semantic Kernel?" Microsoft merged them into Microsoft Agent Framework, built by the same teams: AutoGen's simple agent and multi-agent abstractions plus Semantic Kernel's enterprise features (sessions, type safety, middleware, telemetry), and a new graph workflow engine with checkpoints. Both older projects have migration guides; new work should start on Agent Framework.

> **Interview:** "When would you choose a group chat over a workflow?" Rarely in production. A group chat suits open exploration where you cannot write the steps down, such as brainstorming or research with critique. When the process is known (validate, query, review, publish), a workflow is cheaper, faster and testable, and a group chat can still be one node inside it.
:::

::: check
?? Which two projects does Microsoft Agent Framework replace, and what did each contribute?
AutoGen, which contributed simple agent and multi-agent conversation abstractions, and Semantic Kernel, which contributed enterprise features such as session state, type safety, middleware and telemetry.

?? How does an Agent Framework workflow make long runs resumable?
It runs in supersteps with a barrier between them and saves checkpoints at superstep boundaries; human-in-the-loop requests pause the workflow until a response arrives.

?? How do you point an Agent Framework agent at a local model?
Use `OllamaChatClient` from `agent-framework-ollama`, or `OpenAIChatClient` with `base_url` set to the local OpenAI-compatible endpoint and a placeholder API key.
:::

::: resources
- Docs: [Microsoft Agent Framework overview](https://learn.microsoft.com/en-us/agent-framework/overview/) · agents, harness, workflows and integrations
- Docs: [Ollama provider](https://learn.microsoft.com/en-us/agent-framework/integrations/by-component/model-providers/ollama) · native and OpenAI-compatible local models
- Docs: [Workflow concepts](https://learn.microsoft.com/en-us/agent-framework/concepts/workflows/) · graph and functional workflows, executors and edges
- Docs: [Migration guide from AutoGen](https://learn.microsoft.com/en-us/agent-framework/migration-guide/from-autogen/) · mapping AutoGen concepts to Agent Framework
- Docs: [Sessions in Agent Framework](https://learn.microsoft.com/en-us/agent-framework/concepts/agents/conversations/session) · `AgentSession`, multi-turn runs and serialisation
- Repo: [AutoGen](https://github.com/microsoft/autogen) · the original multi-agent conversation framework
:::

### PydanticAI, DSPy and LlamaIndex

::: plain
These three are specialists. PydanticAI is for people who want an agent to behave like well-typed Python: you declare what goes in and what must come out, and the framework checks it, like a database schema for model output. DSPy treats prompts as settings to be tuned, the way you tune a model's parameters: you write the program, give examples and a score, and an optimiser finds better prompts. LlamaIndex starts from your data: it connects to documents and databases, builds indexes, and hands them to agents as tools.
:::

**PydanticAI: typed agents.** PydanticAI (MIT licence) is from the team behind Pydantic. An `Agent` is generic in two types: the **dependencies** it needs at run time (a database connection, the current user, limits) and the **output** it must return. Dependencies are injected into tools through `RunContext`, so tools never reach for globals, and tests can swap them. Output is validated with Pydantic; when validation fails, the error is sent back to the model for a retry. This example ran against the scripted OpenAI-compatible server (`pydantic-ai` 2.71.0):

```python
from collections.abc import Callable
from dataclasses import dataclass
from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
import shop

model = OpenAIChatModel("local-model",            # vLLM here; OllamaProvider exists too
    provider=OpenAIProvider(base_url="http://localhost:8000/v1", api_key="unused"))

@dataclass
class Deps:                                         # injected per run: connection, user, limits
    run_sql: Callable[[str], str]
    max_rows: int = 50

class TopRegion(BaseModel):
    region: str
    revenue: float

agent = Agent(model, deps_type=Deps, output_type=TopRegion, instructions=shop.INSTRUCTIONS)

@agent.tool
def run_sql(ctx: RunContext[Deps], sql: str) -> str:
    """Run one read-only SQL SELECT on the orders table and return the rows as CSV text."""
    return ctx.deps.run_sql(sql)

result = agent.run_sync(shop.QUESTION, deps=Deps(run_sql=shop.run_sql))
print(repr(result.output))
# TopRegion(region='US', revenue=390.5)
print(result.usage.requests, "model requests")
# 2 model requests

from pydantic_ai.models.test import TestModel

calls = []
def fake_sql(sql: str) -> str:
    calls.append(sql)
    return "region,revenue\nUS,390.5"

with agent.override(model=TestModel()):            # no network: calls every tool, fills the schema
    out = agent.run_sync(shop.QUESTION, deps=Deps(run_sql=fake_sql))
print(len(calls), type(out.output).__name__)
# 1 TopRegion
```

The scripted server saw two tools in each request, `run_sql` and `final_result`: by default PydanticAI returns structured output through a tool call, which open-weight models handle better than free-form JSON. The second half is the testing story, PydanticAI's strongest point: `TestModel` calls every tool with generated arguments and returns schema-valid output, and `agent.override` swaps the model without touching application code. `FunctionModel` lets you script exact responses. Version 2 also brought capabilities, durable execution integrations (Temporal, DBOS and others), `pydantic-graph` for state machines, `pydantic-evals`, and MCP servers attached as toolsets (`MCPToolset`).

**DSPy: programs, not prompts.** DSPy (MIT licence, from Stanford NLP) asks you to declare **signatures** (typed inputs and outputs with a docstring) and compose **modules** (`Predict`, `ChainOfThought`, `ReAct`). An **adapter** turns them into prompts and parses the replies, and an **optimiser** improves the prompts and few-shot examples against a metric you define. The agent module is `dspy.ReAct`. Run offline with DSPy's `DummyLM`, which returns one scripted set of fields per model call (`dspy` 3.5.0):

```python
import dspy
from dspy.utils.dummies import DummyLM
import shop

SQL = "SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region ORDER BY revenue DESC"
lm = DummyLM([                                    # scripted outputs, one dict per model call
    {"next_thought": "Sum revenue by region.",
     "tool_calls": {"tool_calls": [{"name": "run_sql", "args": {"sql": SQL}}]}},
    {"next_thought": "US is the first row.",
     "tool_calls": {"tool_calls": [{"name": "submit", "args": {"region": "US"}}]}},
])
# on-prem: lm = dspy.LM("openai/local-model", api_base="http://localhost:8000/v1", api_key="unused")
dspy.configure(lm=lm)

class SalesQuestion(dspy.Signature):
    """Answer questions about sales using SQL on the orders table."""
    question: str = dspy.InputField()
    region: str = dspy.OutputField(desc="the region name only")

agent = dspy.ReAct(SalesQuestion, tools=[shop.run_sql], max_iters=5)
pred = agent(question=shop.QUESTION)
print(pred.region)
# US
print(sorted(k for k in pred.keys()))
# ['history', 'region', 'termination_reason']
print(pred.termination_reason)
# submit
print(len(lm.history), "model calls")
# 2 model calls
```

In DSPy 3.5, `ReAct` produces `tool_calls` each turn and finishes by calling a built-in `submit` tool with the signature's outputs, so the final answer is validated like any tool call. The point of DSPy is the next step: give it examples and a metric, and it rewrites the instructions and picks demonstrations.

```python
trainset = [dspy.Example(question=shop.QUESTION, region="US").with_inputs("question")]   # use 20 to 200
def exact_region(example, pred, trace=None):
    return example.region.lower() == pred.region.strip().lower()

optimizer = dspy.MIPROv2(metric=exact_region, auto="light")
# optimized = optimizer.compile(agent, trainset=trainset)   # many model calls; needs a real model
# optimized.save("sales_agent.json")                        # prompts and demos, versioned like code
```

`MIPROv2` searches instructions and few-shot examples; `GEPA` evolves prompts using the model's own reflection on failures; `BootstrapFewShot` collects good traces as examples. The optimiser needs a few dozen to a few hundred labelled examples and many model calls, which with a local model costs GPU time rather than money. That makes DSPy attractive on-prem: you can tune a smaller open-weight model's prompts for your exact task.

**LlamaIndex: data first.** LlamaIndex (MIT licence) began as a library for connecting models to your data: readers for hundreds of sources, indexes (vector, keyword, summary), query engines, and a document parsing service (LlamaParse). Its agents are built on its own event-driven **Workflows**, with `FunctionAgent` as the standard tool-calling agent. Not installed in the check environment; written from the current docs and syntax-checked:

```python
import asyncio
from llama_index.core.agent.workflow import FunctionAgent
from llama_index.llms.openai_like import OpenAILike
import shop

llm = OpenAILike(model="local-model", api_base="http://localhost:8000/v1", api_key="unused",
                 is_chat_model=True, is_function_calling_model=True)
agent = FunctionAgent(tools=[shop.run_sql], llm=llm, system_prompt=shop.INSTRUCTIONS)

async def main():
    response = await agent.run(user_msg=shop.QUESTION)
    print(str(response))

if __name__ == "__main__":
    asyncio.run(main())
```

Where LlamaIndex differs is what it can wrap as a tool: an `NLSQLTableQueryEngine` over a PostgreSQL database (through SQLAlchemy and `SQLDatabase`), a vector index over your runbooks in Qdrant or pgvector, or a router over both, each exposed with `QueryEngineTool`. For local models, `llama-index-llms-ollama` and `llama-index-llms-openai-like` are the usual packages. Retrieval itself is covered in [Context engineering, memory and RAG](#memory) and, for the data infrastructure, in the data engineering course ([AI and ML data](index.html#aiml)).

> **Pitfall:** Optimising on too little data. A DSPy optimiser given ten examples will happily overfit them. Hold out a validation set, compare against the unoptimised program, and re-run the evaluation when the model or the data changes.

> **Tip:** Even if you never adopt PydanticAI, copy its dependency-injection idea: pass the connection, the user and the limits into tools as an explicit context object. It makes every framework's agents easier to test and safer to run for many users.

::: deeper
**Where each fits.** PydanticAI is a strong default for a single agent or a small set of agents in a typed Python service (FastAPI style), with excellent testing. DSPy fits when the task has a measurable answer and you have labelled data: classification, extraction, text-to-SQL, routing. LlamaIndex fits when most of the agent's value is retrieval over many documents and sources. They combine: a LlamaIndex query engine can be a tool in a PydanticAI or LangGraph agent.

**DSPy internals.** A signature plus an adapter (`ChatAdapter`, `JSONAdapter`, or native function calling) produces the actual prompt. Optimisers treat instructions and demonstrations as parameters and search them using the metric; they can also fine-tune weights (`BootstrapFinetune`), which matters when you control an open-weight model. The papers are "DSPy: Compiling Declarative Language Model Calls into Self-Improving Pipelines" (2023) and "GEPA: Reflective Prompt Evolution Can Outperform Reinforcement Learning" (2025).

**PydanticAI and validation retries.** When a tool's arguments or the output fail validation, PydanticAI sends the validation error back to the model as a retry prompt, up to `retries`. Tools can raise `ModelRetry("...")` to ask the model to try again with a hint, for example "column `revenue_eur` does not exist; columns are ...". This is the error-feedback pattern from [Tool use and function calling](#tools), built in.

**LlamaIndex Workflows.** LlamaIndex agents run on an event-driven workflow engine: steps are async functions that receive and emit typed events, with context state that can be serialised for pause and resume. `AgentWorkflow` coordinates several agents with handoffs.

> **Interview:** "What problem does DSPy solve?" Prompt engineering by hand does not scale or transfer: a prompt tuned for one model breaks on another. DSPy separates the program (signatures and modules) from the prompts, and uses an optimiser with a metric and examples to generate and select instructions and few-shot demonstrations. When the model changes, you re-run the optimiser instead of rewriting prompts.

> **Interview:** "Why would you use PydanticAI over LangChain?" For a typed Python service with one or a few agents: dependency injection through `RunContext`, validated structured output with automatic retries, any model through a small provider layer, and first-class testing with `TestModel` and `FunctionModel`. LangChain and LangGraph win when I need graphs, checkpoints and the wider integration ecosystem.
:::

::: check
?? In PydanticAI, what are `deps_type` and `RunContext` for?
`deps_type` declares the type of the dependencies each run needs (a connection, the user, settings); the value is passed with `deps=` at run time and reaches tools through `RunContext.deps`, so tools are testable and never use globals.

?? What does a DSPy optimiser need, and what does it change?
A program, a metric function and a training set of examples (plus a model to run). It changes the prompts: instructions and few-shot demonstrations (some optimisers can also fine-tune weights), not your code.

?? When is LlamaIndex the natural choice for an agent?
When most of the agent's work is retrieving from your data: many document sources, indexes and query engines (including text-to-SQL over a database) that the agent uses as tools.
:::

::: resources
- Docs: [Pydantic AI](https://ai.pydantic.dev/) · agents, dependencies, structured output, testing and V2 changes
- Docs: [DSPy](https://dspy.ai/) · signatures, modules, adapters and optimisers
- Paper: [DSPy: Compiling Declarative Language Model Calls into Self-Improving Pipelines](https://arxiv.org/abs/2310.03714) · the original DSPy paper
- Paper: [GEPA: Reflective Prompt Evolution Can Outperform Reinforcement Learning](https://arxiv.org/abs/2507.19457) · the reflective prompt optimiser in DSPy
- Docs: [LlamaIndex agents](https://developers.llamaindex.ai/python/framework/use_cases/agents/) · `FunctionAgent`, workflows and tools
- Repo: [llama_index](https://github.com/run-llama/llama_index) · source, integrations and examples
:::

## Wrap-up

### Cheat sheet

| Idea | Remember |
|---|---|
| Framework or not | Plain SDK for small, short agents; a framework when you need durability, approvals, multi-agent, tracing |
| Main question | Does the framework's main abstraction match your design (graph, tree, harness, crew, typed agent, program)? |
| On-prem model | Most frameworks: OpenAI-compatible `base_url` to vLLM or Ollama; Claude Agent SDK: Anthropic-compatible endpoint |
| LangGraph | `StateGraph`, nodes, edges, reducers, checkpointer per superstep, `thread_id`, `interrupt`, time travel |
| LangChain | Chat model, messages, `@tool`, runnables with `\|`; `create_agent` compiles to a LangGraph graph; middleware |
| Google ADK | `Agent`, `Runner`, events, session state, `LiteLlm` for local models, 2.x `Workflow` replaces Sequential/Parallel/Loop agents |
| Claude Agent SDK | The Claude Code harness; tools as in-process MCP; permissions: hooks, deny, ask, mode, allow, `can_use_tool` |
| OpenAI Agents SDK | Agent, Runner, `@function_tool`, handoffs, guardrails (tripwires), sessions; `OpenAIChatCompletionsModel` for local |
| Tracing on-prem | Disable vendor exporters; send OpenTelemetry to Langfuse, Phoenix or your collector |
| CrewAI | Role agents, tasks, crews (sequential or hierarchical); Flows for structure; disable telemetry |
| Microsoft Agent Framework | Successor to AutoGen and Semantic Kernel; `Agent`, sessions, typed graph workflows with checkpoints |
| PydanticAI | Typed deps via `RunContext`, validated output with retries, `TestModel` and `agent.override` for tests |
| DSPy | Signatures and modules; `ReAct` with `submit`; optimisers (MIPROv2, GEPA) tune prompts against a metric |
| LlamaIndex | Data first: readers, indexes, query engines as tools; `FunctionAgent` on event-driven workflows |
| Portability | Tools and rules in plain modules, tools over MCP, traces in OpenTelemetry |

### Practice

::: try
**1. Pick and defend.** For each case, choose a framework from this section and give one reason: (a) a nightly job that classifies 200,000 support tickets into ten categories with a local model, where accuracy matters; (b) an agent that investigates failed Airflow DAG runs by reading logs and code in a repository checkout and proposes a fix; (c) a data-access agent that must wait for a data owner's approval before running any query on a restricted schema, possibly for days.
:::

?? Answer to 1
(a) DSPy, or no agent framework at all: it is a fixed classification step, not an agent. DSPy lets you optimise the prompt and examples for the local model against labelled tickets; run it as a batch task in Airflow or Spark. (b) The Claude Agent SDK: the job is reading files, searching code and running commands, which is exactly the harness's built-in tool set; restrict tools and permissions, and run it in a container with a read-only checkout. (c) LangGraph with a PostgreSQL checkpointer: `interrupt()` before the gated query, durable state per `thread_id`, resume days later in another process with `Command(resume=...)`. ADK 2.x `Workflow` with database sessions or an Agent Framework workflow with checkpoints would also work.

::: try
**2. Port the baseline.** Take the plain-SDK baseline from "Choosing an agent framework" and add one production feature without a framework: save `messages` to a SQLite table after every step, keyed by a conversation id, and load them at start-up so that a crashed run can continue. Then list what you would still be missing compared with LangGraph's checkpointer.
:::

?? A solution to 2

```python
import json, sqlite3

db = sqlite3.connect(":memory:")                # a file or PostgreSQL in real use
db.execute("CREATE TABLE IF NOT EXISTS runs (thread_id TEXT PRIMARY KEY, messages TEXT)")

def load(thread_id, fresh):
    row = db.execute("SELECT messages FROM runs WHERE thread_id = ?", (thread_id,)).fetchone()
    return json.loads(row[0]) if row else fresh

def save(thread_id, messages):
    db.execute("INSERT INTO runs VALUES (?, ?) ON CONFLICT(thread_id) DO UPDATE SET messages = excluded.messages",
               (thread_id, json.dumps(messages)))
    db.commit()

msgs = load("t1", [{"role": "user", "content": "Which region had the most revenue?"}])
msgs.append({"role": "assistant", "content": "Let me check."})
save("t1", msgs)
print(len(load("t1", [])))
# 2
```

In the loop, call `save` after appending the model reply and after each tool result, and `load` instead of building `messages` fresh. Still missing compared with a checkpointer: a history of every step (this keeps only the latest), pending-write handling so a crash in the middle of parallel tool calls does not repeat finished ones, interrupts with a resume value, time travel and forking, and tested support for concurrent writers.

::: try
**3. Make the tool safe everywhere.** The `run_sql` check in `shop.py` only looks at the first word. Write a stricter `is_safe_select(sql)` that rejects multiple statements and the keywords `insert`, `update`, `delete`, `drop`, `alter`, `create`, `grant`, `copy` anywhere outside string literals, then say why the database role still matters.
:::

?? A solution to 3

```python
import re

BLOCKED = {"insert", "update", "delete", "drop", "alter", "create", "grant", "copy", "truncate"}

def is_safe_select(sql: str) -> bool:
    no_strings = re.sub(r"'(?:[^']|'')*'", "''", sql)            # remove string literals
    statements = [s for s in no_strings.split(";") if s.strip()]
    if len(statements) != 1:
        return False
    words = set(re.findall(r"[a-z_]+", statements[0].lower()))
    return statements[0].lstrip().lower().startswith(("select", "with")) and not (words & BLOCKED)

print(is_safe_select("SELECT region, SUM(amount) FROM orders GROUP BY region"))
# True
print(is_safe_select("SELECT 1; DROP TABLE orders"))
# False
print(is_safe_select("SELECT * FROM orders WHERE region = 'drop zone'"))
# True
```

A text check is a convenience that gives the model a fast, clear error. It cannot be complete: functions with side effects, dialect features and clever encodings get past keyword lists. The real control is a database role with `SELECT` only on the allowed schema, a `statement_timeout`, and row limits, so that even a query that slips through cannot change anything. Put the check inside the tool so it applies in every framework.

::: try
**4. Compare two frameworks on one axis.** Without looking back, explain how human approval before a tool call works in LangGraph, the OpenAI Agents SDK and the Claude Agent SDK. Where is the paused state stored in each?
:::

?? Answer to 4
LangGraph: `interrupt_before=["tools"]` at compile time or `interrupt()` inside a node; the state is saved by the checkpointer (PostgreSQL in production) under the `thread_id`, and the run resumes with `invoke(None, cfg)` or `Command(resume=...)`. OpenAI Agents SDK: a tool with `needs_approval=True` stops the run with pending calls in `result.interruptions`; `result.to_state()` gives a `RunState` you serialise and store yourself, then approve or reject and run again with the state. Claude Agent SDK: the `can_use_tool` callback (or a `PreToolUse` hook) decides each call while the run is live; the session transcript is stored by the runtime (or your `session_store`), so for long waits you deny with a message, end the run, and later `resume` the session with the decision.

::: try
**5. Interview drill.** In two minutes, out loud: "We are on-prem with vLLM serving an open-weight model, PostgreSQL, Airflow and Kubernetes. The team wants an agent that answers analysts' questions over the warehouse. Which framework would you choose and why?"
:::

?? Answer to 5
A good answer covers the design before the tool. The agent is a text-to-SQL assistant: a few tools (schema lookup, run a read-only query, maybe a metrics catalogue), short runs, many users. I would start with a thin framework: PydanticAI or the OpenAI Agents SDK pointed at vLLM's OpenAI-compatible endpoint, or LangGraph if we expect approval steps for restricted data or long investigations, since its PostgreSQL checkpointer fits our stack. Tools live in a plain module and are exposed over MCP so other agents can reuse them; queries run under a read-only role with timeouts. Traces go to a self-hosted Langfuse or Phoenix through OpenTelemetry, with the vendor exporter disabled. It is deployed as a container on Kubernetes, with an evaluation set of real analyst questions and expected SQL results run in CI. I would test tool-call reliability of our vLLM model in each candidate before committing, and keep the framework layer thin so we can switch.

### What comes next

You can now place every major agent framework on the map, build the same agent in each against a local model, and choose between them with a decision table and reasons. What none of these frameworks decides for you is where the agent runs: how the model is served on your GPUs, how the agent is packaged and scaled, how it authenticates as a user, and when a managed platform is worth it. The next section, **Deploying and serving agents: on-prem and cloud**, covers serving open-weight models with vLLM and Ollama, gateways such as LiteLLM, containers and Kubernetes, enterprise identity, and the managed option on Google Cloud.
