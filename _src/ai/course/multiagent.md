This section is for the reader who can write the agent loop by hand ([Agentic AI foundations](#foundations)), knows the workflow and agent patterns, including orchestrator-workers ([Agent design patterns](#patterns)), and can decide what an agent sees at each step ([Context engineering, memory and RAG](#memory)). It does not re-teach orchestrator-workers. It starts where that pattern ends: when the workers become agents with their own tools and loops, and the thing that coordinates them is an agent too. You need the same plain Python as before, a little `concurrent.futures`, and no framework experience.

**What you will be able to do afterwards**

- Decide when several agents beat one, and say what it costs: Anthropic's research system used about 15 times the tokens of a chat.
- Build a supervisor, a hierarchy of teams, a handoff network (swarm) and isolated subagents from scratch in plain Python, with caps and validation in code.
- Build the same designs in real frameworks: LangGraph with `Command`, the OpenAI Agents SDK with handoffs, Google's Agent Development Kit (ADK) with `sub_agents`, and the Claude Agent SDK with subagents.
- Coordinate agents through shared state, a blackboard or a message bus, and say which fits which problem.
- Use voting and debate to combine answers, and explain what the research says about when debate helps.
- Name the failure modes of multi-agent systems from the MAST taxonomy and put a control in code against each.

**The big picture**

A multi-agent system is a set of agents, each a model with its own instructions, tools and context, plus the rules for how work and information move between them. There are only a few shapes. A **supervisor** gives tasks to workers and gets results back. A **hierarchy** stacks supervisors into teams. A **handoff network**, often called a swarm, lets agents pass control of the conversation to each other. **Subagents** are workers whose main job is to keep the lead agent's context clean. Underneath every shape sits a choice about communication: agents can share one state, write to a blackboard, or send each other messages.

[[fig:multiagent_topologies]]

The case for multiple agents is narrow but real. Each agent gets a short, focused prompt and a small tool set, so it chooses better. Work can run in parallel. Most of all, each agent has its own context window, so the lead can coordinate a task far larger than one window holds. The case against is just as real: every extra agent multiplies tokens, adds a place where information gets lost, and makes the system harder to test. Anthropic's post on its multi-agent research system and Cognition's post "Don't build multi-agents" disagree on the conclusion but agree on the cause: success depends on what context each agent has.

If you run data platforms, you know these trade-offs already. A supervisor is an Airflow DAG whose task list is chosen at run time by a model. A handoff is a ticket moving between on-call queues. A blackboard is a shared table that several jobs read and write, and a message bus is Pub/Sub or Kafka with agents as consumers. The same rules apply: contracts between components, idempotent steps, one owner per output, and monitoring on every hop.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

> **Tip:** From-scratch examples run with plain `python3` and a scripted **mock model** in the Messages API shape, as in earlier sections, so you see the real control flow without an API key. Framework examples were checked offline against the installed packages: `langgraph` 1.2.24 and `langchain` 1.5.2 graphs were built and run with a fake chat model, while `openai-agents` 0.26.0, `google-adk` 2.15.0 and `claude-agent-sdk` 0.2.188 objects were constructed but not run, since running them calls a model. The model behind any example can be a hosted API such as Claude or an open-weight model you serve yourself with vLLM or Ollama; no example depends on a cloud data service.

## Architectures

The orchestrator-workers pattern from [Agent design patterns](#patterns) is a workflow: one model call plans, workers are single calls, code combines the results. This group covers what happens when you give the pieces more autonomy. Workers become agents with their own loops and tools. The coordinator becomes an agent that reacts to results and sends more work. Or there is no coordinator at all, and agents pass the conversation between themselves.

The first topic is the decision: when is any of this worth it? The next three are the main architectures. Supervisors and hierarchies keep control in one place. Handoffs move control around. Subagents are the most useful idea in the group for everyday work: spawn a focused agent with a clean context, let it do the reading, and keep only its summary.

### Why and when to use multiple agents

::: plain
One analyst can investigate a small incident alone: read the logs, check the tables, write the report. For a big outage, a lead splits the work: one person checks ingestion, another the warehouse, a third the dashboards. Each one reads far more than the lead could, and the lead reads only their short reports. The cost is more people, more coordination, and the risk that someone misunderstands the brief. You split the work only when the job is too big or too varied for one person.
:::

**What it is.** A multi-agent system splits one task across several agents. Each agent is a loop around a model with its own system prompt, its own tools and, most importantly, its own context window. The agents are connected by code that decides who works on what and how results flow back.

**Why it exists.** A single agent hits four limits as tasks grow.

- **Context.** Every tool result stays in the history. An agent that profiles ten datasets carries all ten profiles into every later call, and quality falls as the window fills ([context rot](#memory)). A subagent can read a whole dataset and return one paragraph.
- **Tool choice.** A model choosing among 40 tools picks the wrong one more often than a model choosing among 5. Specialists with small tool sets and short prompts choose better.
- **Parallelism.** One loop does one thing at a time. Independent subtasks (profile four datasets, review six DAGs) can run at once in separate agents.
- **Separation of duties.** A reviewer agent that cannot see the author's reasoning, or an agent that can read but not write, is a security and quality control, not just an optimisation.

**What it costs.** Anthropic published the clearest numbers in its post on the multi-agent research system behind Claude's Research feature. A lead agent with parallel subagents beat a single agent by 90.2% on its internal research evaluation. But agents used about 4 times the tokens of a chat, and the multi-agent system about 15 times. On the BrowseComp benchmark, token usage alone explained 80% of the variance in performance. In other words, much of the gain came from spending more tokens, spread across more context windows. That is worth paying only when the task is valuable.

The program below makes the trade visible. One agent profiles four datasets in one context; the alternative gives each dataset its own subagent, which also checks samples, and returns a 300-token summary to a lead. Every model call re-reads the whole context, which is how the APIs bill.

```python
def run_cost(steps, fixed):
    """Input tokens for one agent loop: every call re-sends fixed prompt + all earlier results."""
    context, total, peak = fixed, 0, 0
    for added in steps:
        total += context                 # one model call reads the whole context
        peak = max(peak, context)
        context += added                 # the tool result joins the history
    total += context                     # final call that writes the answer
    return total, max(peak, context)

DATASETS = ["sales", "finance", "marketing", "ops"]
PROFILE = 6_000          # tokens returned by profiling one dataset
SYSTEM = 2_500           # system prompt + tool definitions
SUMMARY = 300            # what a subagent sends back

# One agent profiles all four datasets in one context.
single_total, single_peak = run_cost([PROFILE] * len(DATASETS), SYSTEM)

# Lead + one subagent per dataset; each subagent also checks samples (2 extra calls).
sub_total, sub_peak = 0, 0
for _ in DATASETS:
    t, p = run_cost([PROFILE, 1_500, 1_500], SYSTEM)
    sub_total += t; sub_peak = max(sub_peak, p)
lead_total, lead_peak = run_cost([SUMMARY] * len(DATASETS), SYSTEM)

print(f"single agent: peak context {single_peak:,}  total input {single_total:,}")
print(f"multi-agent:  peak context {max(sub_peak, lead_peak):,}  total input {sub_total + lead_total:,}")
# single agent: peak context 26,500  total input 72,500
# multi-agent:  peak context 11,500  total input 145,500
```

The largest context any model sees drops from 26,500 to 11,500 tokens, and with ten datasets instead of four the single agent would overflow a typical window while each subagent stays the same size. The total bill doubles, because every subagent pays for its own system prompt and does more exploration. With the subagents running in parallel, wall-clock time is roughly one subagent plus the lead, not four in a row.

**When to use multiple agents.** Use this table as a first filter.

| Signal | Points to | Why |
|---|---|---|
| Subtasks are independent and each reads a lot | Subagents in parallel | Isolated contexts, summaries back, faster |
| Clearly separate domains with different tools and permissions | Supervisor or handoffs | Small tool sets, least privilege per agent |
| A conversation that moves between specialists | Handoffs | The specialist talks to the user directly |
| Subtasks depend tightly on each other's decisions | One agent | Splitting loses the shared context |
| Most steps are known in advance | A workflow | Cheaper, testable, no coordinator needed |
| Low-value, high-volume requests | One agent or a workflow | 15x tokens does not pay back |

> **Pitfall:** Splitting by job title instead of by context. Teams build a "planner", a "coder" and a "tester" agent because that mirrors a human team, then lose the plan's reasoning at each boundary. Split where the work is naturally independent and the interface between parts is small, the same rule you use when you split a monolithic pipeline into DAGs.

> **Tip:** Before building a multi-agent system, run the single-agent version and log where it fails. If it fails because the context fills up or the tool list is too long, more agents may help. If it fails because the model reasons badly about the task, more agents usually multiply the error.

::: deeper
**The two positions.** Cognition's June 2025 post "Don't build multi-agents" (Walden Yan) argues from building coding agents: share full context and traces, not just messages, because actions carry implicit decisions and parallel agents make conflicting ones. Its example: two subagents asked to build parts of a Flappy Bird clone produced a background and a bird in inconsistent styles, and the final agent had to merge incompatible pieces. Anthropic's post argues from research tasks, which are broad, mostly read-only and parallel: subagents explore separate directions and only their findings need to be merged. Both are right for their workloads. Writing tasks with tightly coupled decisions (code, a migration) suit one agent with good context management; reading tasks that fan out (research, audits, profiling) suit subagents. Anthropic itself says most coding tasks have fewer truly parallel subtasks than research.

**Where the gain comes from.** If 80% of the variance is explained by tokens spent, a fair comparison gives the single agent the same token budget. Several papers that report multi-agent gains compare against a single call, not against a single agent with retries, self-consistency or a longer budget. Ask for the token-matched baseline before believing a multi-agent result, including your own.

**Latency.** Parallel subagents cut wall-clock time (Anthropic reported up to 90% less for complex queries, from running 3 to 5 subagents at once, each making several tool calls in parallel). Sequential delegation does the opposite: every hop adds a full model call. A supervisor that consults three workers one after another is three times slower than one agent with the same tools.

**History.** Multi-agent systems are older than LLMs: blackboard systems in the 1970s and 1980s, contract-net protocols, agent communication languages such as FIPA ACL. LLM-era frameworks rediscovered the same shapes. CAMEL (2023) used two role-playing agents; MetaGPT and ChatDev (2023) modelled software companies with roles; AutoGen (2023) made multi-agent conversation a programming model. The survey by Guo et al. (2024) maps the field.

> **Interview:** "When would you use a multi-agent system instead of a single agent?" When the task decomposes into parts that are independent, read a lot, or need different tools or permissions, and when the task is valuable enough to pay several times the tokens. Typical fits: broad research, audits across many datasets, a help desk across separate domains. I would not split tightly coupled work like a schema migration, where every step depends on earlier decisions, and I would start with one agent and measure where it fails before adding more.

> **Interview:** "What does a multi-agent system cost compared with one agent?" More tokens (every agent pays its own system prompt and exploration; Anthropic measured about 15 times a chat versus 4 times for a single agent), more latency for sequential hops, more failure points at every boundary, and harder debugging, because a bad answer may come from a lost detail three agents back. The gain is smaller per-agent contexts, parallelism and specialisation.
:::

::: check
?? Name three limits of a single agent that multiple agents can address.
Context growth (every tool result stays in the window), tool choice getting worse with many tools, and no parallelism; also separation of duties and permissions.

?? Why did the multi-agent version in the example cost more in total even though its peak context was smaller?
Every subagent pays for its own system prompt and does its own extra exploration, and every model call re-reads that agent's whole context. Smaller windows do not mean fewer tokens overall.

?? What kind of task does Cognition argue should stay with one agent, and why?
Tasks with tightly coupled decisions such as writing code, because each action carries implicit decisions, and parallel agents that cannot see each other's full context make conflicting ones.
:::

::: resources
- Blog: [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system) · Anthropic: lead plus subagents, 90.2% gain, about 15x tokens
- Blog: [Don't build multi-agents](https://cognition.com/blog/dont-build-multi-agents) · Cognition: context sharing and conflicting decisions
- Blog: [Building effective agents](https://www.anthropic.com/research/building-effective-agents) · start simple, add agents only when it helps
- Docs: [Multi-agent](https://docs.langchain.com/oss/python/langchain/multi-agent) · LangChain: subagents, handoffs, router, custom workflows
- Paper: [Large Language Model based Multi-Agents: A Survey of Progress and Challenges](https://arxiv.org/abs/2402.01680) · Guo et al., 2024
- Paper: [AutoGen](https://arxiv.org/abs/2308.08155) · multi-agent conversation as a programming model
:::

### Supervisor and hierarchical architectures

::: plain
An incident commander does not fix anything personally. They ask the ingestion engineer "when did orders last load?", ask the warehouse engineer "how do the row counts compare?", read the answers, decide what to ask next, and write the summary. Every answer comes back to the commander. In a big company, the commander talks to team leads, and each lead runs their own people: a hierarchy of the same arrangement.
:::

**What it is.** A **supervisor** is an agent whose tools are other agents. It reads the request, decides which worker to call with which task, reads the result, and repeats until it can answer. Workers never talk to each other or to the user; every result returns to the supervisor. A **hierarchical** system nests this: a top supervisor delegates to team supervisors, each with its own workers.

**How it differs from orchestrator-workers.** In the workflow version, the orchestrator plans once, the workers are single calls, and code synthesizes. A supervisor is a loop: it can call a worker, look at the answer, and change its plan. Workers are agents with their own tools and steps. The price is that the supervisor's decisions now happen at run time, so the caps and validation must be in code.

**From scratch.** The supervisor below investigates a low revenue number on the finance dashboard. Its only tool is `delegate(agent, task)`. Each worker is a full agent loop with its own tools and a fresh context that starts from the task text alone. Code validates every delegation and caps how many there can be.

```python
import json

# --- tools the workers can use (stand-ins for PostgreSQL / Airflow API calls) ---
def last_load(table):
    return {"raw.orders": "26h ago", "raw.refunds": "2h ago"}.get(table, "unknown")

def row_change(table):
    return {"stg.orders": "-38% vs 7-day average", "stg.refunds": "+2%"}.get(table, "unknown")

class Agent:
    """One agent = a name, a description, its own tools, its own model, its own loop."""
    def __init__(self, name, description, tools, model):
        self.name, self.description, self.tools, self.model = name, description, tools, model

    def run(self, task, max_steps=5):
        messages = [{"role": "user", "content": task}]          # fresh context per task
        for _ in range(max_steps):
            reply = self.model(messages)
            messages.append({"role": "assistant", "content": reply["content"]})
            if reply["stop_reason"] == "end_turn":
                return reply["content"][0]["text"]
            results = []
            for b in reply["content"]:
                if b["type"] == "tool_use":
                    out = self.tools[b["name"]](**b["input"])
                    results.append({"type": "tool_result", "tool_use_id": b["id"],
                                    "content": json.dumps(out)})
            messages.append({"role": "user", "content": results})
        return f"{self.name}: stopped at step limit"

def tool_call(name, args, n):
    return {"stop_reason": "tool_use",
            "content": [{"type": "tool_use", "id": f"tu_{n}", "name": name, "input": args}]}

def done(text):
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}

def last_result(messages):
    last = messages[-1]["content"]
    return json.loads(last[0]["content"]) if isinstance(last, list) else None

# --- scripted worker models ---
def freshness_model(messages):
    if len(messages) == 1:
        return tool_call("last_load", {"table": "raw.orders"}, 1)
    return done(f"raw.orders last loaded {last_result(messages)}: the orders load is late.")

def volume_model(messages):
    if len(messages) == 1:
        return tool_call("row_change", {"table": "stg.orders"}, 1)
    return done(f"stg.orders row count {last_result(messages)}.")

WORKERS = {
    "freshness_agent": Agent("freshness_agent", "Checks when tables were last loaded.",
                             {"last_load": last_load}, freshness_model),
    "volume_agent": Agent("volume_agent", "Compares row counts with recent history.",
                          {"row_change": row_change}, volume_model),
}

# --- the supervisor: its only tool is 'delegate' ---
DELEGATE_TOOL = {"name": "delegate", "description": "Send a task to one worker agent.",
                 "input_schema": {"type": "object", "properties": {
                     "agent": {"type": "string", "enum": sorted(WORKERS)},
                     "task": {"type": "string"}}, "required": ["agent", "task"]}}

def supervisor_model(messages):
    seen = " ".join(b["content"] for m in messages if isinstance(m["content"], list)
                    for b in m["content"] if b["type"] == "tool_result")
    if "late" not in seen:
        return tool_call("delegate", {"agent": "freshness_agent",
                         "task": "When was raw.orders last loaded? Answer in one sentence."}, 1)
    if "row count" not in seen:
        return tool_call("delegate", {"agent": "volume_agent",
                         "task": "How does the stg.orders row count compare with last week?"}, 2)
    return done("Root cause: the orders load is 26h late, so stg.orders is 38% short "
                "and daily revenue is low. Next: rerun orders_daily, then backfill.")

def supervise(question, model, workers, max_delegations=4):
    messages = [{"role": "user", "content": question}]
    delegations = 0
    while True:
        reply = model(messages)
        messages.append({"role": "assistant", "content": reply["content"]})
        if reply["stop_reason"] == "end_turn":
            return reply["content"][0]["text"], delegations
        results = []
        for b in reply["content"]:
            agent, task = b["input"]["agent"], b["input"]["task"]
            if agent not in workers:                                  # validate the target
                out = f"error: unknown agent {agent}; choose from {sorted(workers)}"
            elif delegations >= max_delegations:                      # cap the fan-out
                out = "error: delegation budget used up; write the final answer now"
            else:
                delegations += 1
                out = workers[agent].run(task)                        # worker gets only the task
                print(f"-> {agent}: {out}")
            results.append({"type": "tool_result", "tool_use_id": b["id"], "content": out})
        messages.append({"role": "user", "content": results})

answer, n = supervise("Daily revenue on the finance dashboard is 40% low. Why?",
                      supervisor_model, WORKERS)
# -> freshness_agent: raw.orders last loaded 26h ago: the orders load is late.
# -> volume_agent: stg.orders row count -38% vs 7-day average.
print(n, answer)
# 2 Root cause: the orders load is 26h late, so stg.orders is 38% short and daily revenue is low. Next: rerun orders_daily, then backfill.
```

What to notice:

- **The supervisor sees results, not work.** Its history holds two short answers, not the tool calls the workers made. That is the context benefit of delegation.
- **The `enum` in `DELEGATE_TOOL`** tells a real model which agents exist; the check in `supervise` enforces it, since a model can still invent a name.
- **The worker's context is only the task string.** If the supervisor writes a vague task, the worker cannot recover the missing detail. Writing good task briefs is the supervisor's main skill.
- **The budget ends the loop.** After `max_delegations`, the supervisor is told to answer with what it has. Without this, a supervisor that keeps "double-checking" can loop until the token limit.

**Hierarchies.** Because a supervisor and its workers can expose the same `run(task)` interface as a single agent, a team can be a worker of a higher supervisor. Continuing the program:

```python
class Team:
    """A supervisor plus its workers, exposed with the same run(task) as a single agent."""
    def __init__(self, name, description, model, workers):
        self.name, self.description, self.model, self.workers = name, description, model, workers

    def run(self, task):
        answer, _ = supervise(task, self.model, self.workers)
        return answer

warehouse = Team("warehouse_team", "Investigates warehouse tables.", supervisor_model, WORKERS)

def head_model(messages):
    if len(messages) == 1:
        return tool_call("delegate", {"agent": "warehouse_team",
                                      "task": "Find why daily revenue is low."}, 1)
    return done("Escalated to ingestion on-call. " + messages[-1]["content"][0]["content"][:40] + "...")

answer, n = supervise("Daily revenue is 40% low.", head_model, {"warehouse_team": warehouse})
# -> freshness_agent: raw.orders last loaded 26h ago: the orders load is late.
# -> volume_agent: stg.orders row count -38% vs 7-day average.
# -> warehouse_team: Root cause: the orders load is 26h late, so stg.orders is 38% short and daily revenue is low. Next: rerun orders_daily, then backfill.
print(n, answer)
# 1 Escalated to ingestion on-call. Root cause: the orders load is 26h late,...
```

The head supervisor sees one delegation and one answer; the team's internal work stays inside the team. Hierarchies help when there are too many workers for one supervisor to choose among well (a rough rule is more than 5 to 10). Each level adds latency and another place where detail is lost, so keep them shallow.

**The same design in LangGraph.** In LangGraph, a node can return a `Command` that both updates state and says which node runs next (`goto`). That makes a supervisor a node that routes with `Command`, and each worker a node that returns to the supervisor. The routing decision is a plain function here; in production it is a model call with structured output. The `Literal` return types tell LangGraph which edges exist, so the graph can be drawn and checked (run with `langgraph` 1.2.24):

```python
from typing import Literal, Annotated, TypedDict
import operator
from langgraph.graph import StateGraph, START, END
from langgraph.types import Command

class Incident(TypedDict, total=False):
    question: str
    findings: Annotated[list[str], operator.add]
    steps: int
    report: str

def decide(state):            # in production: a model call returning structured output
    seen = " ".join(state.get("findings", []))
    if "freshness" not in seen: return "freshness_agent"
    if "volume" not in seen:    return "volume_agent"
    return "FINISH"

def supervisor(state: Incident) -> Command[Literal["freshness_agent", "volume_agent", "write_report"]]:
    steps = state.get("steps", 0) + 1
    nxt = decide(state)
    if nxt == "FINISH" or steps > 5:                    # cap in code, not in the prompt
        return Command(goto="write_report", update={"steps": steps})
    return Command(goto=nxt, update={"steps": steps})

def freshness_agent(state: Incident) -> Command[Literal["supervisor"]]:
    return Command(goto="supervisor", update={"findings": ["freshness: raw.orders last loaded 26h ago"]})

def volume_agent(state: Incident) -> Command[Literal["supervisor"]]:
    return Command(goto="supervisor", update={"findings": ["volume: stg.orders rows -38% vs 7-day avg"]})

def write_report(state: Incident) -> dict:
    return {"report": " | ".join(state["findings"])}

g = StateGraph(Incident)
for name, fn in [("supervisor", supervisor), ("freshness_agent", freshness_agent),
                 ("volume_agent", volume_agent), ("write_report", write_report)]:
    g.add_node(name, fn)
g.add_edge(START, "supervisor")
g.add_edge("write_report", END)
app = g.compile()
out = app.invoke({"question": "Why is daily revenue 40% low?"})
print(out["steps"], out["report"])
# 3 freshness: raw.orders last loaded 26h ago | volume: stg.orders rows -38% vs 7-day avg
```

In a real system each worker node would be a compiled agent (for example from `langchain.agents.create_agent`) added as a node, and a worker graph can itself contain a supervisor, which gives you a hierarchy. Here the workers share the graph state; in the from-scratch version they only saw the task. Both are valid: shared state is simpler, task-only input isolates context better.

**CrewAI.** CrewAI has a built-in hierarchical process in which a manager model assigns tasks to agents and checks their output. CrewAI could not be installed in this environment (its `tiktoken` dependency does not build on Python 3.14), so this snippet follows the current CrewAI docs and was only syntax-checked:

```python
from crewai import Agent, Crew, Process, Task

freshness = Agent(role="Freshness analyst", goal="Find late or missing loads",
                  backstory="You know the Airflow DAGs that load raw tables.")
volume = Agent(role="Volume analyst", goal="Find unexpected row count changes",
               backstory="You compare table sizes with recent history in Trino.")
investigate = Task(description="Daily revenue on the finance dashboard is 40% low. Find out why.",
                   expected_output="Root cause, evidence and the next action")

crew = Crew(agents=[freshness, volume], tasks=[investigate],
            process=Process.hierarchical, manager_llm="a-current-model-id")
# result = crew.kickoff()   # needs a model API key
```

> **Pitfall:** A supervisor that relays instead of deciding. If the supervisor passes the user's message unchanged to a worker and then copies the worker's answer back, it adds a model call, latency and a chance of paraphrasing errors, and decides nothing. Either give it real decisions (which workers, in what order, when to stop) or replace it with a router ([Routing](#patterns)).

> **Tip:** Make workers return structured results (status, finding, evidence, confidence) rather than prose. The supervisor's decisions get easier, and your code can check results before the supervisor sees them.

::: deeper
**Supervisor as tool-calling.** The common modern form is exactly the from-scratch version: the supervisor is an ordinary tool-calling agent, and each worker is wrapped as a tool. LangChain's current docs call this the **subagents** pattern and build it with `create_agent` and tools that invoke other agents; the separate `langgraph-supervisor` package offered a prebuilt `create_supervisor` and is still on GitHub, but it is not needed. The OpenAI Agents SDK has `Agent.as_tool()`, ADK has `AgentTool` and sub-agents in `single_turn` mode, and the Claude Agent SDK has subagents invoked through its `Agent` tool. These appear in [Subagents and context isolation](#multiagent).

**What the supervisor should know about workers.** A one-line description per worker, its input format and what it returns. Write these like tool descriptions ([Tool use and function calling](#tools)): the supervisor's routing is only as good as the descriptions. Overlapping descriptions ("analyses data" and "investigates tables") make it guess.

**Aggregation.** How results combine depends on the task. Concatenation works for independent findings. A final model call is needed when findings must be reconciled (two workers suggest different root causes). For numeric or categorical answers, code can aggregate (counts, votes, minimum freshness). Keep the evidence attached to each finding, so the final answer can cite which worker found what.

**Bottleneck and single point of failure.** Every result passes through the supervisor, so its context grows with each round, and a supervisor error affects everything. Anthropic noted a related limit in its research system: the lead waits for each batch of subagents to finish before continuing, which is simple to build but means it cannot steer a subagent mid-run. Mitigations: compact the supervisor's history between rounds ([Context window management](#memory)), have workers return short structured results, and checkpoint the supervisor's state so a crash resumes rather than restarts.

**Hierarchy depth.** Each level is a lossy summary of the level below. Two levels is common; three is rare and usually a sign that the problem should be split into separate systems with a contract (a table, an API) between them, the way you would split one giant DAG.

> **Interview:** "Design a multi-agent system for incident triage on a data platform." A supervisor agent receives the alert and has worker agents for freshness (Airflow and load metadata), volume and quality (SQL checks through Trino or PostgreSQL), and lineage (OpenLineage events or the catalog). Workers have read-only tools and return structured findings with evidence. The supervisor decides which workers to call based on the alert and earlier findings, with a cap on delegations and a total token budget enforced in code. Its final report cites evidence from each worker. Actions such as reruns or backfills go through human approval. I would evaluate on past incidents with known root causes.

> **Interview:** "What is the difference between a supervisor and a router?" A router makes one classification and hands off; it never sees the result. A supervisor loops: it delegates, reads the result, and decides what to do next, possibly calling several workers in sequence and combining their outputs.
:::

::: check
?? Why does the supervisor's context grow more slowly than a single agent's doing the same work?
It receives only each worker's final answer, not the worker's tool calls and raw results, which stay in the worker's own context.

?? What two checks does `supervise` apply to each delegation?
That the named agent exists, and that the delegation budget has not been used up; otherwise it returns an error as the tool result.

?? In LangGraph, what lets a node both update state and choose the next node?
Returning a `Command(goto=..., update=...)`; the `Command[Literal[...]]` return annotation declares the possible destinations.
:::

::: resources
- Docs: [Subagents](https://docs.langchain.com/oss/python/langchain/multi-agent/subagents) · LangChain: a supervisor calling agents as tools
- Docs: [Graph API](https://docs.langchain.com/oss/python/langgraph/graph-api) · LangGraph state, reducers and `Command`
- Repo: [langgraph-supervisor](https://github.com/langchain-ai/langgraph-supervisor-py) · the prebuilt supervisor package
- Docs: [Processes](https://docs.crewai.com/en/concepts/processes) · CrewAI sequential and hierarchical processes
- Blog: [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system) · a lead agent that plans, delegates and synthesizes
- Paper: [MetaGPT](https://arxiv.org/abs/2308.00352) · role-based agents with structured outputs between them
:::

### Handoffs and swarm architectures

::: plain
You call your bank. The first person you reach works out what you need and transfers you to the cards team. The cards team sees it is a fraud case and transfers you to fraud. You now talk to the fraud specialist directly, and if you call back later, you may land with them again. Nobody sits in the middle relaying messages. That is a handoff: control of the conversation moves to whoever can best handle it.
:::

**What it is.** In a handoff architecture, one agent is **active** at a time and talks to the user. Any agent can **hand off** to another by calling a transfer tool, usually named `transfer_to_<agent>`. Code then switches the active agent: the next model call uses the new agent's system prompt and tools, and sees the conversation so far. When every agent can hand off to several others, with no central coordinator, the arrangement is often called a **swarm**, after OpenAI's experimental Swarm library, which popularised it.

**Supervisor versus handoffs.** A supervisor keeps control and gets results back. A handoff gives control away. That changes three things:

| | Supervisor | Handoffs (swarm) |
|---|---|---|
| Who talks to the user | Only the supervisor | Whichever agent is active |
| Where results go | Back to the supervisor | Nowhere; the active agent continues |
| Model calls per user turn | Supervisor plus workers | Usually one per active agent |
| What each agent sees | Only its task | Usually the shared conversation |
| Best for | Tasks that combine several specialists' results | Conversations that move between domains |
| Main risk | Bottleneck, relaying | Ping-pong, lost context, no one owns the outcome |

**How a handoff works, step by step.**

1. The active agent's model sees the conversation and its tools, including the transfer tools it is allowed to use.
2. It calls `transfer_to_trino_agent` with a short reason.
3. Code checks that this agent may transfer there and that the handoff cap is not reached.
4. Code records a tool result ("Transferred to trino_agent"), so the conversation stays valid: every `tool_use` gets its `tool_result`.
5. Code sets `active = "trino_agent"` and loops. The next call uses the Trino agent's prompt and tools.
6. The active agent is saved with the conversation, so the user's next message goes straight to it.

[[fig:multiagent_handoff]]

**From scratch.** A data platform help desk with three agents: triage, a Trino specialist and an access specialist that files access requests (in a shop running Apache Ranger, say). Each has its own tools and an allow-list of agents it may hand off to.

```python
import json, itertools

ids = itertools.count(1)
def call(name, args):
    return {"stop_reason": "tool_use", "content": [
        {"type": "tool_use", "id": f"tu_{next(ids)}", "name": name, "input": args}]}
def say(text):
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}
def results_text(messages):
    return " ".join(str(b["content"]) for m in messages if isinstance(m["content"], list)
                    for b in m["content"] if b["type"] == "tool_result")

# --- domain tools ---
def query_error(query_id):
    return "Access Denied: Cannot select from table hive.finance.revenue_daily"
def request_access(table, privilege):
    return {"ticket": "ACC-4821", "status": "pending approval by finance data owner"}

# --- scripted models: each one only knows its own job ---
def triage_model(messages):
    return call("transfer_to_trino_agent", {"reason": "failing Trino query"})

def trino_model(messages):
    seen = results_text(messages)
    if "Access Denied" not in seen:
        return call("query_error", {"query_id": "20261124_0317_00042"})
    return call("transfer_to_access_agent", {"reason": "missing SELECT privilege"})

def access_model(messages):
    last_user = [m for m in messages if m["role"] == "user" and isinstance(m["content"], str)][-1]
    if "approved" in last_user["content"]:
        return say("Ticket ACC-4821 is still waiting for the finance data owner.")
    if "ACC-4821" not in results_text(messages):
        return call("request_access", {"table": "hive.finance.revenue_daily",
                                       "privilege": "SELECT"})
    return say("Your query failed on a missing permission. I opened ACC-4821 for read access; "
               "the finance data owner must approve it.")

AGENTS = {
    "triage":         {"model": triage_model,   "tools": {}, "handoffs": ["trino_agent", "access_agent"]},
    "trino_agent":    {"model": trino_model,    "tools": {"query_error": query_error}, "handoffs": ["access_agent", "triage"]},
    "access_agent":   {"model": access_model,   "tools": {"request_access": request_access}, "handoffs": ["triage"]},
}

def run_turn(state, user_text, max_handoffs=3, max_steps=8):
    """One user turn. The active agent answers or hands off; state remembers who is active."""
    state["messages"].append({"role": "user", "content": user_text})
    handoffs = 0
    for _ in range(max_steps):
        agent = AGENTS[state["active"]]
        reply = agent["model"](state["messages"])        # shared conversation history
        state["messages"].append({"role": "assistant", "content": reply["content"]})
        if reply["stop_reason"] == "end_turn":
            return f'[{state["active"]}] ' + reply["content"][0]["text"]
        results = []
        for b in reply["content"]:
            if b["name"].startswith("transfer_to_"):
                target = b["name"].removeprefix("transfer_to_")
                if target not in agent["handoffs"] or handoffs >= max_handoffs:
                    out = f"error: cannot transfer to {target}"
                else:
                    print(f'handoff {state["active"]} -> {target}: {b["input"]["reason"]}')
                    state["active"], handoffs = target, handoffs + 1
                    out = f"Transferred to {target}"
            else:
                out = json.dumps(agent["tools"][b["name"]](**b["input"]))
            results.append({"type": "tool_result", "tool_use_id": b["id"], "content": out})
        state["messages"].append({"role": "user", "content": results})
    return "stopped: step limit"

state = {"active": "triage", "messages": []}
print(run_turn(state, "My Trino query 20261124_0317_00042 fails. Can you fix it?"))
# handoff triage -> trino_agent: failing Trino query
# handoff trino_agent -> access_agent: missing SELECT privilege
# [access_agent] Your query failed on a missing permission. I opened ACC-4821 for read access; the finance data owner must approve it.
print(run_turn(state, "Has it been approved yet?"))
# [access_agent] Ticket ACC-4821 is still waiting for the finance data owner.
```

Three details carry the design:

- **Allow-lists per agent.** The access agent can only hand back to triage. Handoff graphs should be explicit, like the edges of a DAG, not "anyone to anyone".
- **A handoff cap per turn.** Two agents that each think the other should handle a request will bounce it forever. `max_handoffs` stops that; the error tells the model to answer instead.
- **The active agent persists.** The second question skips triage, because `state["active"]` is still `access_agent`. That is the main user-facing benefit of handoffs, and also a risk: if the user changes topic, the specialist must be able to hand back.

**LangGraph: handoffs with `Command`.** Each agent is a node (here, agents built with `langchain.agents.create_agent`). A handoff tool returns `Command(goto=..., graph=Command.PARENT)`: the tool runs inside the agent's own subgraph, and `graph=Command.PARENT` tells LangGraph to route in the parent graph. The update carries the triggering AI message and a matching `ToolMessage`, so the history stays well formed, and sets `active_agent` so the next turn starts with the right agent. A fake chat model stands in for the real one, which on-prem would be an open-weight model behind vLLM's or Ollama's OpenAI-compatible endpoint (run with `langgraph` 1.2.24 and `langchain` 1.5.2):

```python
from typing import NotRequired
from langchain.agents import create_agent, AgentState
from langchain.tools import tool, ToolRuntime
from langchain.messages import AIMessage, ToolMessage, HumanMessage
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langgraph.graph import StateGraph, START, END
from langgraph.types import Command

class FakeModel(GenericFakeChatModel):          # scripted replies; a real chat model in production
    def bind_tools(self, tools, **kwargs):
        return self

class TeamState(AgentState):
    active_agent: NotRequired[str]

@tool
def transfer_to_trino_agent(runtime: ToolRuntime) -> Command:
    """Hand the conversation to the Trino specialist."""
    return Command(goto="trino_agent", graph=Command.PARENT,
                   update={"active_agent": "trino_agent",
                           "messages": [runtime.state["messages"][-1],
                                        ToolMessage("Transferred to trino_agent",
                                                    tool_call_id=runtime.tool_call_id)]})

@tool
def cluster_load(cluster: str) -> str:
    """Return queued queries and busy workers for a Trino cluster."""
    return f"{cluster}: 38 queries queued, 12 of 12 workers busy"

triage_model = FakeModel(messages=iter([
    AIMessage("", tool_calls=[{"name": "transfer_to_trino_agent", "args": {}, "id": "c1"}])]))
trino_model = FakeModel(messages=iter([
    AIMessage("", tool_calls=[{"name": "cluster_load", "args": {"cluster": "trino-prod"}, "id": "c2"}]),
    AIMessage("The cluster is saturated (38 queued, all 12 workers busy); queries are waiting.")]))

triage = create_agent(triage_model, [transfer_to_trino_agent], name="triage_agent",
                      system_prompt="Route data platform questions.", state_schema=TeamState)
trino = create_agent(trino_model, [cluster_load], name="trino_agent",
                     system_prompt="You answer Trino questions.", state_schema=TeamState)

def route(state):
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and not last.tool_calls:
        return END                                   # an agent answered: end the turn
    return state.get("active_agent", "triage_agent")

g = StateGraph(TeamState)
g.add_node("triage_agent", triage)
g.add_node("trino_agent", trino)
g.add_edge(START, "triage_agent")
g.add_conditional_edges("triage_agent", route, ["trino_agent", END])
g.add_conditional_edges("trino_agent", route, ["triage_agent", END])
app = g.compile()
out = app.invoke({"messages": [HumanMessage("Why are my Trino queries slow?")]})
print(out["active_agent"])
# trino_agent
print(out["messages"][-1].content)
# The cluster is saturated (38 queued, all 12 workers busy); queries are waiting.
```

With a checkpointer and a `thread_id` ([Short-term memory and conversation state](#memory)), `active_agent` is saved per conversation, and a `START` router that reads it sends the next turn straight to the specialist. The separate `langgraph-swarm` package wraps this pattern in `create_swarm` and `create_handoff_tool`; it was not installed here and is not needed.

**OpenAI Agents SDK.** Handoffs are a first-class field. Listing an agent in `handoffs` creates a tool named `transfer_to_<agent_name>`; the `handoff()` function customises it: a callback when it fires, a typed input the model must fill (useful for logging why), and an `input_filter` that edits the history the next agent sees. `handoff_description` is what the calling agent reads when choosing. Constructed with `openai-agents` 0.26.0:

```python
from agents import Agent, handoff, function_tool, RunContextWrapper, OpenAIChatCompletionsModel
from openai import AsyncOpenAI
from agents.extensions import handoff_filters
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from pydantic import BaseModel

@function_tool
def cluster_load(cluster: str) -> str:
    """Return queued queries and busy workers for a Trino cluster."""
    return f"{cluster}: 38 queries queued, 12 of 12 workers busy"

class Escalation(BaseModel):
    reason: str
    table: str

async def log_escalation(ctx: RunContextWrapper[None], data: Escalation) -> None:
    print("escalated:", data.table, data.reason)

# Any OpenAI-compatible server works: here a local vLLM (or Ollama) endpoint, no cloud key.
local = OpenAIChatCompletionsModel(model="a-local-model-id",
    openai_client=AsyncOpenAI(base_url="http://localhost:8000/v1", api_key="unused"))

trino_agent = Agent(name="Trino agent", handoff_description="Slow queries, cluster load, query errors",
    instructions=f"{RECOMMENDED_PROMPT_PREFIX}\nYou answer Trino questions.", tools=[cluster_load], model=local)
quality_agent = Agent(name="Data quality agent", handoff_description="Nulls, duplicates, freshness, row counts",
    instructions=f"{RECOMMENDED_PROMPT_PREFIX}\nYou investigate data quality issues.", model=local)
triage = Agent(name="Triage agent",
    instructions=f"{RECOMMENDED_PROMPT_PREFIX}\nRoute each question to the right specialist.", model=local,
    handoffs=[trino_agent,
              handoff(quality_agent, on_handoff=log_escalation, input_type=Escalation,
                      input_filter=handoff_filters.remove_all_tools)])
print([h.name if isinstance(h, Agent) else h.tool_name for h in triage.handoffs])
# ['Trino agent', 'transfer_to_data_quality_agent']
# result = await Runner.run(triage, "Why is stg.orders missing rows?")   # needs the model server
# result.last_agent is the agent that answered
```

`RECOMMENDED_PROMPT_PREFIX` is a short block of text the SDK provides that explains handoffs to the model. `remove_all_tools` strips earlier tool calls from the history the quality agent receives, a simple form of context control at the boundary.

**Google ADK.** In ADK, agents form a tree: a parent lists its `sub_agents`. With the default `chat` mode, a sub-agent is reached by LLM-driven transfer: the model calls the built-in `transfer_to_agent` function, and the chosen agent takes over the conversation, which is a handoff. `disallow_transfer_to_parent` and `disallow_transfer_to_peers` restrict the allowed moves, like the allow-lists above. ADK 2.x adds a `mode` for sub-agents: `task` (may ask the user questions, returns to the parent by calling `finish_task`) and `single_turn` (no user interaction, runs once and returns, like a tool; covered in the next topic). ADK is open source and not tied to Gemini: with the `google-adk[extensions]` extra, `LiteLlm` points an agent at a local model served by Ollama or vLLM (the extra is not installed here, so it appears as a comment). Constructed with `google-adk` 2.15.0:

```python
from google.adk.agents import Agent

MODEL = "gemini-3.5-flash"   # ADK 2.15 default; on-prem: LiteLlm(model="ollama_chat/<model>")

def cluster_load(cluster: str) -> dict:
    """Return queued queries and busy workers for a Trino cluster."""
    return {"cluster": cluster, "queued": 38, "busy_workers": 12, "workers": 12}

trino_agent = Agent(name="trino_agent", model=MODEL,
    description="Answers questions about slow Trino queries, cluster load and query errors.",
    instruction="You answer Trino questions. Use cluster_load for capacity.", tools=[cluster_load])
quality_agent = Agent(name="quality_agent", model=MODEL,
    description="Investigates nulls, duplicates, freshness and row counts.",
    instruction="You investigate data quality issues.", disallow_transfer_to_peers=True)
root = Agent(name="platform_helpdesk", model=MODEL,
    instruction="Route each question to the right specialist. Answer greetings yourself.",
    sub_agents=[trino_agent, quality_agent])
print([a.name for a in root.sub_agents], trino_agent.parent_agent.name)
# ['trino_agent', 'quality_agent'] platform_helpdesk
```

ADK uses each sub-agent's `description` when deciding where to transfer, so write it like a tool description. An agent instance can have only one parent; to reuse a specialist in two trees, create two instances.

> **Pitfall:** Ping-pong. Triage sends a question to the Trino agent, which decides it is a permissions problem and sends it back, and triage sends it to Trino again. Cap handoffs per turn in code, make every handoff carry a reason, and give each agent a clear "if this is not mine and not anyone else's, answer with what you know" instruction.

> **Tip:** Log every handoff as an event (from, to, reason, turn id). Handoff counts per conversation and the most common pairs are the best early signals that agent descriptions overlap.

::: deeper
**What the next agent sees.** Handoff systems usually share the full conversation, which gives the next agent context but also all of the previous agent's tool calls, some of which it cannot interpret (it does not have those tools). Options: pass everything (simple, can confuse), filter tool calls out (`remove_all_tools` in the OpenAI SDK; `include_contents="none"` in ADK gives an agent no prior history), or pass a summary written by the outgoing agent. LangChain's handoff docs recommend passing only the handoff pair (the AI message and its tool message) and putting any needed summary in the tool message. The more you filter, the more the handoff reason must carry.

**Handoffs as state machines.** A handoff network is a finite state machine whose states are agents and whose transitions are chosen by models. Drawing it as one (who may transfer to whom, under what condition) makes it reviewable. Where transitions can be decided by code (a ticket field, a form step), do that instead; LangChain's docs show a single agent whose prompt and tools change with a `current_step` state field, which gives handoff behaviour without separate agents.

**Swarm history.** OpenAI's Swarm (2024) was an educational library built on two ideas: routines (an agent's instructions and tools) and handoffs (a tool that returns another agent). It was replaced by the OpenAI Agents SDK, which keeps handoffs and adds guardrails, sessions and tracing. "Swarm" in other frameworks (`langgraph-swarm`, swarms in research) means the same thing: peer agents with handoffs and no central coordinator.

**Handoffs and A2A.** Within one process, a handoff is a function call. Across organisations or services, the equivalent is sending a task to a remote agent over the Agent2Agent (A2A) protocol ([Model Context Protocol and agent interoperability](#mcp)). The remote agent does not join your conversation; it receives a task and returns results, which makes A2A closer to the supervisor pattern than to an in-process handoff.

> **Interview:** "How do you prevent infinite handoff loops?" Explicit allow-lists of which agent may transfer to which, a cap on handoffs per turn enforced in code, a required reason on every transfer that is logged, and an instruction that the current agent must answer when no one else fits. In monitoring, alert on conversations with many handoffs or on A-to-B-to-A patterns.

> **Interview:** "When would you choose handoffs over a supervisor?" When the user should talk directly to a specialist for several turns, as in a help desk with separate domains, and the task does not need several specialists' results combined. A supervisor fits when one answer needs input from several workers, or when you want one agent responsible for the final response.
:::

::: check
?? What is the main structural difference between a handoff and a supervisor delegating a task?
A handoff transfers control: the new agent continues the conversation and its answer goes to the user. A delegation returns the worker's result to the supervisor, which keeps control.

?? Why does the LangGraph handoff tool set `graph=Command.PARENT`?
Because the tool runs inside the agent's own subgraph; `Command.PARENT` makes the `goto` route to a node in the parent graph, where the other agent lives.

?? In the from-scratch help desk, why does the second user message go straight to the access agent?
The active agent is stored in the conversation state, and each turn starts with whoever is active.
:::

::: resources
- Docs: [Handoffs](https://docs.langchain.com/oss/python/langchain/multi-agent/handoffs) · LangChain: handoff tools with `Command`, `active_agent` routing
- Docs: [Handoffs](https://openai.github.io/openai-agents-python/handoffs/) · OpenAI Agents SDK: `handoff()`, input types and filters
- Docs: [Collaborative workflows](https://adk.dev/workflows/collaboration/) · ADK 2.x coordinators, sub-agent modes
- Repo: [Swarm](https://github.com/openai/swarm) · OpenAI's educational library for routines and handoffs
- Repo: [langgraph-swarm](https://github.com/langchain-ai/langgraph-swarm-py) · prebuilt swarm with handoff tools for LangGraph
- Docs: [Agent orchestration](https://openai.github.io/openai-agents-python/multi_agent/) · OpenAI: handoffs versus agents as tools
:::

### Subagents and context isolation

::: plain
A manager asks three analysts to each read one department's 300-page data inventory and report back. Each analyst spends a day reading and hands over a half-page memo, with the full notes saved on the shared drive. The manager reads three half-pages, not 900 pages, and can still open the notes if a detail matters. The analysts did not need to know what the others were doing, so the work went in parallel.
:::

**What it is.** A **subagent** is an agent that a lead agent starts for one focused task. It begins with a fresh context (its own system prompt and the task brief, not the lead's history), uses its own tools, runs its own loop, and returns only a final result. Everything it read along the way stays in its own context and is thrown away or saved elsewhere. Subagents are the supervisor pattern used for one specific purpose: **context isolation**.

**Why it matters.** In [Context engineering](#memory) you saw the four context strategies: write, select, compress, isolate. Subagents are the isolate strategy. A subagent can read 50 files and return one paragraph, so the lead's context grows by a paragraph. This is how Anthropic's research system covers broad questions: the lead agent plans, starts 3 to 5 subagents in parallel, each searches with its own context, and the lead combines their condensed findings. Claude Code uses the same idea: its `Explore` subagent searches a codebase and returns conclusions, not file dumps.

**What makes subagents work in practice** (Anthropic's lessons from its research system):

- **Detailed task briefs.** Each subagent needs an objective, an output format, guidance on tools and sources, and clear boundaries. Early versions with short briefs ("research the semiconductor shortage") duplicated each other's work or left gaps.
- **Effort scaling in the lead's prompt.** A simple fact-finding question needs one agent with a few tool calls; a comparison needs 2 to 4 subagents; a broad research task may need more than 10. Without such rules, models over-spawn for easy questions.
- **Artifacts instead of telephone.** Subagents can write large outputs (tables, code, full reports) to a store and return a short reference, so the content is not copied and paraphrased through the lead.

**From scratch.** A data governance check before sharing three datasets externally: does any of them contain PII? Each subagent profiles one dataset (300 lines of column profile, a stand-in for thousands), finds email and phone columns, saves its full transcript to an artifact store, and returns one line. The subagents run in parallel.

```python
import json, re
from concurrent.futures import ThreadPoolExecutor

# Column profiles per dataset: large, noisy tool output (imagine thousands of lines).
PROFILES = {
    "sales":     ["order_id BIGINT", "customer_email VARCHAR sample=a.b@x.com", "amount NUMERIC"] + ["filler_col VARCHAR"] * 300,
    "marketing": ["campaign_id BIGINT", "phone VARCHAR sample=+44 7700 900123", "spend NUMERIC"] + ["filler_col VARCHAR"] * 300,
    "ops":       ["run_id VARCHAR", "dag_id VARCHAR", "duration_s BIGINT"] + ["filler_col VARCHAR"] * 300,
}
ARTIFACTS = {}                                    # shared store: full outputs live here

def profile_columns(dataset):
    return "\n".join(PROFILES[dataset])

def subagent_model(messages):
    """Scripted subagent: profile the dataset, then report PII columns."""
    if len(messages) == 1:
        ds = re.search(r"dataset (\w+)", messages[0]["content"]).group(1)
        return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "tu_1",
                "name": "profile_columns", "input": {"dataset": ds}}]}
    profile = messages[-1]["content"][0]["content"]
    pii = [line.split()[0] for line in profile.splitlines() if "@" in line or "+44" in line]
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": json.dumps({"pii": pii})}]}

def run_subagent(task, key):
    """Fresh context, own tools, own budget. Only a short summary goes back to the lead."""
    messages = [{"role": "user", "content": task}]
    for _ in range(4):
        reply = subagent_model(messages)
        messages.append({"role": "assistant", "content": reply["content"]})
        if reply["stop_reason"] == "end_turn":
            break
        b = reply["content"][0]
        out = profile_columns(**b["input"])
        messages.append({"role": "user", "content": [{"type": "tool_result",
                         "tool_use_id": b["id"], "content": out}]})
    found = json.loads(messages[-1]["content"][0]["text"])["pii"]
    ref = f"artifact://pii/{key}"
    ARTIFACTS[ref] = {"task": task, "transcript": messages}          # full detail kept outside
    used = sum(len(json.dumps(m)) for m in messages)
    summary = f"{len(found)} PII column(s): {', '.join(found) or 'none'}. Details: {ref}"
    return summary, used

def lead(datasets):
    tasks = [f"Scan dataset {d} for PII columns (emails, phone numbers). "
             f"Return column names only. Do not query row data." for d in datasets]
    with ThreadPoolExecutor(max_workers=3) as pool:                  # subagents run in parallel
        results = list(pool.map(run_subagent, tasks, datasets))
    lead_context = [{"role": "user", "content": "Can we share sales, marketing, ops externally?"}]
    for (summary, _), d in zip(results, datasets):
        lead_context.append({"role": "user", "content": f"{d}: {summary}"})
    return lead_context, results

ctx, results = lead(["sales", "marketing", "ops"])
for m in ctx[1:]:
    print(m["content"])
# sales: 1 PII column(s): customer_email. Details: artifact://pii/sales
# marketing: 1 PII column(s): phone. Details: artifact://pii/marketing
# ops: 0 PII column(s): none. Details: artifact://pii/ops
print("lead context chars:", sum(len(json.dumps(m)) for m in ctx))
# lead context chars: 362
print("subagent context chars:", sum(used for _, used in results))
# subagent context chars: 19531
```

The lead's context is about 50 times smaller than the work done for it. The task brief says what to look for, what to return and what not to do ("Do not query row data"), which is the boundary a governance subagent needs. The artifact references mean an auditor, or the lead, can open the full evidence later without it ever passing through the lead's window.

[[fig:multiagent_subagents]]

**Claude Agent SDK.** Subagents are defined with `AgentDefinition` and passed in `agents`. The main agent starts them through its `Agent` tool (shown as `Task` in some older versions and in the init tool list), so `Agent` must be allowed. Each subagent gets a fresh context containing its own `prompt`, the task string the main agent writes, and project settings; it does not see the parent's conversation, and only its final message returns. `tools` restricts what it can do, `model` picks a model alias, and `maxTurns` caps its loop. Subagents can spawn their own subagents by default, so cap depth, concurrency and spend (constructed with `claude-agent-sdk` 0.2.188):

```python
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

options = ClaudeAgentOptions(
    allowed_tools=["Read", "Grep", "Glob", "Agent"],      # "Agent" lets the main agent spawn subagents
    agents={
        "dag-auditor": AgentDefinition(
            description="Audits one Airflow DAG file for retries, SLAs and hard-coded buckets. Use once per DAG.",
            prompt="You audit one DAG file. Report problems as a bullet list with line numbers. Do not edit files.",
            tools=["Read", "Grep"], model="haiku", maxTurns=10),
        "sql-reviewer": AgentDefinition(
            description="Reviews Trino and Spark SQL for full scans, missing partition filters and SELECT *.",
            prompt="You review SQL files for cost. Return at most five findings, each with file and line.",
            tools=["Read", "Grep", "Glob"]),
    },
    env={"CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH": "1",       # subagents may not spawn subagents
         "CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS": "5"},
    max_budget_usd=2.0,                                      # whole run, subagents included
)
print(sorted(options.agents), options.agents["dag-auditor"].tools)
# ['dag-auditor', 'sql-reviewer'] ['Read', 'Grep']
# async for message in query(prompt="Audit every DAG in dags/ and the SQL they run", options=options):
#     ...   # messages from inside a subagent carry parent_tool_use_id
```

The `description` is what the main agent reads when deciding whether to delegate, so it says when to use the subagent ("Use once per DAG"). Read-only tools make the auditor safe to run in parallel.

**LangChain: a subagent as a tool.** Wrap an agent in a tool that invokes it with a fresh message list and returns only its last message. The tool's docstring is the description the lead reads. Run with a fake chat model (`langchain` 1.5.2):

```python
from langchain.agents import create_agent
from langchain.tools import tool
from langchain.messages import AIMessage, HumanMessage
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

class FakeModel(GenericFakeChatModel):
    def bind_tools(self, tools, **kwargs):
        return self

@tool
def query_information_schema(dataset: str) -> str:
    """List tables and row counts in a dataset."""
    return "orders 1.2M rows, refunds 40k rows, customers 300k rows, " * 20   # big raw output

schema_model = FakeModel(messages=iter([
    AIMessage("", tool_calls=[{"name": "query_information_schema", "args": {"dataset": "sales"}, "id": "s1"}]),
    AIMessage("sales has 3 tables; orders is the largest (1.2M rows).")]))
schema_agent = create_agent(schema_model, [query_information_schema], name="schema_agent",
                            system_prompt="Explore one dataset and return a 1-sentence summary.")

@tool
def explore_dataset(dataset: str) -> str:
    """Delegate exploration of one dataset to a subagent; returns a short summary."""
    result = schema_agent.invoke({"messages": [HumanMessage(f"Summarise dataset {dataset}")]})
    return result["messages"][-1].content              # only the final answer goes back

lead_model = FakeModel(messages=iter([
    AIMessage("", tool_calls=[{"name": "explore_dataset", "args": {"dataset": "sales"}, "id": "l1"}]),
    AIMessage("The sales dataset has 3 tables, the largest is orders.")]))
lead = create_agent(lead_model, [explore_dataset], system_prompt="You plan data audits.")
out = lead.invoke({"messages": [HumanMessage("Audit the sales dataset")]})
print(out["messages"][-1].content)
# The sales dataset has 3 tables, the largest is orders.
print(len(out["messages"]), max(len(m.content) for m in out["messages"]))
# 4 54
```

The lead's history has four messages and none is longer than 54 characters; the 1,200-character schema dump stayed inside the subagent.

**ADK and the OpenAI Agents SDK.** In ADK 2.x, a sub-agent with `mode="single_turn"` runs once without talking to the user and returns its result to the parent, in its own session branch; ADK adds a delegation tool named after it, and single-turn sub-agents can run in parallel. `AgentTool(agent=...)` wraps any agent as an explicit tool. In the OpenAI Agents SDK, `agent.as_tool(tool_name=..., tool_description=...)` does the same. Both constructed and checked (`google-adk` 2.15.0, `openai-agents` 0.26.0):

```python
from google.adk.agents import Agent as AdkAgent
from google.adk.tools.agent_tool import AgentTool
from agents import Agent as OpenAIAgent

MODEL = "gemini-3.5-flash"   # ADK 2.15 default; on-prem: LiteLlm(model="ollama_chat/<model>")
summarizer = AdkAgent(name="schema_summarizer", model=MODEL, mode="single_turn",
                      description="Summarises one dataset's tables in two sentences.",
                      instruction="Summarise the dataset you are given.")
adk_lead = AdkAgent(name="audit_lead", model=MODEL, instruction="Plan dataset audits.",
                    sub_agents=[summarizer])
print([getattr(t, "name", t) for t in adk_lead.tools])
# ['schema_summarizer']
explicit = AdkAgent(name="audit_lead_2", model=MODEL, instruction="Plan dataset audits.",
                    tools=[AgentTool(agent=AdkAgent(name="sql_reviewer", model=MODEL,
                                                    instruction="Review one SQL query for cost."))])

reviewer = OpenAIAgent(name="SQL reviewer", instructions="Review one SQL query for cost and correctness.")
oa_lead = OpenAIAgent(name="Lead", instructions="Plan the migration review.",
                      tools=[reviewer.as_tool(tool_name="review_sql", tool_description="Review one SQL query")])
print([t.name for t in oa_lead.tools])
# ['review_sql']
```

> **Pitfall:** Starving the subagent. The subagent knows only what the brief says. "Check the orders table" leaves it guessing which project, which checks and what to return. Put file paths, table names, error messages, decisions already made, the output format and the boundaries in the brief. The Claude Agent SDK docs say this directly: the task string is the only channel from parent to subagent.

> **Tip:** Ask subagents for structured, compact results (JSON with fixed fields, or at most N bullet points with evidence). Then the lead can merge them in code, and its context grows predictably.

::: deeper
**Isolation versus sharing.** Isolation is the point and the risk. A subagent that cannot see the lead's reasoning may redo work or make a decision that conflicts with it, which is Cognition's objection. Two mitigations: give subagents read-only, independent tasks (research, profiling, review), and keep decisions that must agree with each other in the lead. Some systems offer a **fork**: a subagent that starts with a copy of the parent's conversation rather than a fresh context. Claude Code supports forked subagents; they keep shared context at the cost of a large starting window.

**Synchronous versus background subagents.** In the simplest design, the lead waits for all subagents. Anthropic notes this limits the lead's ability to steer and that one slow subagent holds up the batch. Asynchronous subagents (the Claude Agent SDK runs subagents in the background by default and delivers the report in a later turn) improve throughput but add coordination problems: results arrive out of order, and the lead must track what is outstanding.

**Depth and fan-out limits.** A subagent that can spawn subagents can grow into a tree whose cost nobody predicted. Set a depth limit (often 1: subagents do their own work), a concurrency limit and a total budget for the run, all in code or configuration rather than in the prompt. The Claude Agent SDK exposes these as `CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH`, `CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS` and `max_budget_usd`.

**Evaluating subagent systems.** Anthropic evaluated its research system with an LLM judge scoring the end result (factual accuracy, citation accuracy, completeness, source quality, tool efficiency) on a small set of about 20 queries at first, because early changes had large effects. For systems where the path varies run to run, grade the end state, not the exact sequence of delegations. More in [Evaluation and observability](#eval).

**Subagents as security boundaries.** A subagent that reads untrusted content (web pages, user-uploaded files, tickets) can be manipulated by prompt injection. If it has only read-only tools and returns a short structured result, the damage it can do is limited, and the lead can validate the result before acting. This is the dual-model idea from [Tool use and function calling](#tools) applied to agents.

> **Interview:** "How would you use subagents to audit 200 Airflow DAGs?" A lead lists the DAG files in code (no model needed for that), then spawns read-only auditor subagents in batches with a concurrency cap, each given one file path, the checklist and a JSON output format. Each returns findings with line numbers; full notes go to an artifact store. The lead merges the JSON in code, ranks issues, and writes the summary. Depth limit 1, a per-subagent turn limit and a total budget. I would check a sample of findings by hand to measure precision.

> **Interview:** "Why do subagents improve quality and not just speed?" Each one reasons over a small, relevant context instead of a long history full of other tasks' results, which reduces context rot and distraction. The lead also reasons over short summaries. The cost is more total tokens and the risk of lost context between agents.
:::

::: check
?? What does a (non-fork) subagent's context contain when it starts?
Its own system prompt, the task brief written by the lead, its tool definitions and any project settings; not the lead's conversation history.

?? Why return an artifact reference instead of the full result?
The full content stays out of the lead's context and is not copied and paraphrased through it (the "game of telephone"); the lead or a person can still open it when needed.

?? Name three limits you would set on a system that spawns subagents.
Maximum spawn depth, maximum concurrent subagents, and a total budget for the run; also a turn limit per subagent.
:::

::: resources
- Docs: [Subagents in the SDK](https://code.claude.com/docs/en/agent-sdk/subagents) · Claude Agent SDK: `AgentDefinition`, context, limits
- Docs: [Subagents](https://code.claude.com/docs/en/sub-agents) · Claude Code subagents, forks and built-in agents
- Blog: [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system) · task briefs, effort scaling, artifacts
- Blog: [Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents) · sub-agent architectures as a context strategy
- Docs: [Tools](https://openai.github.io/openai-agents-python/tools/) · OpenAI Agents SDK: agents as tools
- Docs: [Collaborative workflows](https://adk.dev/workflows/collaboration/) · ADK `single_turn` and `task` sub-agents
:::

## Coordination

The architectures in the first group say who works on what. This group is about how agents exchange information and how their outputs become one answer. There are three ways to communicate: through a **shared state** that every agent reads and writes (LangGraph's graph state, a blackboard), through **messages** sent from one agent to another (a bus or queue, or A2A across services), or through **handoffs** of the conversation itself, covered above. Each has the same trade-offs as in data systems: shared tables are simple but need ownership and concurrency rules; messages decouple producers from consumers but need schemas, ids and dead-letter handling.

Combining outputs has its own techniques. Several agents can answer independently and **vote**, or **debate**, reading each other's answers and revising. The research on when debate helps is more sceptical than the early papers suggested, and the topic covers both sides. The group ends with failure: a taxonomy of how multi-agent systems break, built from real traces, and the controls that catch each kind.

### Shared state, blackboards and message passing

::: plain
A newsroom has a whiteboard. A reporter writes "council vote at 3pm"; a photographer sees it and goes; an editor sees the story draft appear and starts editing. Nobody assigns work directly: people watch the board and act when something they can handle shows up. Other teams work by email instead: each message goes to a named person or list, and if nobody reads that list, the message sits unanswered. Both work, and both need rules: who may erase what on the board, and what every email must contain.
:::

**What it is.** Agents need a way to share what they know. There are three common designs.

- **Shared state.** All agents read and write one structured state object. LangGraph is built on this: the graph state is a typed dict, each node returns updates, and **reducers** decide how updates combine. ADK has session state, with `output_key` saving an agent's final answer under a key that later agents read.
- **Blackboard.** A shared store that agents watch. An agent runs when the entries it needs are present, and posts its result back. There is no fixed sequence; the order emerges from data dependencies. The idea comes from 1970s and 1980s AI systems such as Hearsay-II.
- **Message passing.** Agents send each other messages, directly or through a broker (a queue or a publish-subscribe topic). Each agent has its own private state and sees only the messages addressed or subscribed to it. Across processes and organisations, the A2A protocol plays this role ([Model Context Protocol and agent interoperability](#mcp)).

| | Shared state | Blackboard | Message passing |
|---|---|---|---|
| Coupling | Agents share a schema | Agents share keys on the board | Agents share message contracts |
| Who decides order | The graph edges | Data dependencies | Subscriptions and senders |
| Visibility | Everyone sees everything | Everyone sees the board | Each agent sees its messages |
| Main risk | Conflicting writes, bloated state | Unclear ownership, races | Lost or duplicated messages, no global view |
| Data engineering parallel | A shared staging table | Airflow data-aware scheduling | Kafka consumer groups |

**From scratch: a blackboard.** A schema change in a source table triggers three agents that never call each other. Each declares the keys it needs and the key it makes. Every entry records its author and a version, and writes use an **optimistic concurrency** check: you must say which version you read, and the write fails if someone changed it since. This is the same rule as a conditional update or a `MERGE` with a version column.

```python
class Blackboard:
    """Shared state with an author and a version on every entry, and an audit log."""
    def __init__(self):
        self.entries, self.log = {}, []

    def read(self, key):
        e = self.entries.get(key)
        return (e["value"], e["version"]) if e else (None, 0)

    def write(self, key, value, author, expected_version):
        _, current = self.read(key)
        if current != expected_version:                    # optimistic concurrency check
            raise ValueError(f"{author}: {key} changed (v{current}), re-read before writing")
        self.entries[key] = {"value": value, "author": author, "version": current + 1}
        self.log.append((author, key, current + 1))

# Each agent declares what it needs and what it produces. Bodies would be model calls.
AGENTS = [
    {"name": "impact_agent", "needs": ["schema_diff"], "makes": "impacted_models",
     "run": lambda b: ["stg_orders", "fct_revenue"] if "amount" in b["schema_diff"] else []},
    {"name": "fix_agent", "needs": ["impacted_models"], "makes": "patch",
     "run": lambda b: {m: "CAST(amount_minor AS NUMERIC) / 100 AS amount" for m in b["impacted_models"]}},
    {"name": "review_agent", "needs": ["patch"], "makes": "review",
     "run": lambda b: "approved" if all("/ 100" in v for v in b["patch"].values()) else "rejected"},
]

def run_board(board, max_rounds=5):
    for rnd in range(1, max_rounds + 1):
        ready = [a for a in AGENTS
                 if all(board.read(k)[0] is not None for k in a["needs"])
                 and board.read(a["makes"])[0] is None]
        if not ready:
            return rnd - 1
        for a in ready:                                    # could run in parallel
            inputs = {k: board.read(k)[0] for k in a["needs"]}
            board.write(a["makes"], a["run"](inputs), a["name"], expected_version=0)
    return max_rounds

board = Blackboard()
board.write("schema_diff", "orders.amount renamed to amount_minor (BIGINT, cents)", "schema_watcher", 0)
rounds = run_board(board)
print(rounds, board.read("review")[0])
# 3 approved
for entry in board.log:
    print(entry)
# ('schema_watcher', 'schema_diff', 1)
# ('impact_agent', 'impacted_models', 1)
# ('fix_agent', 'patch', 1)
# ('review_agent', 'review', 1)
try:
    board.write("patch", {}, "late_agent", expected_version=0)
except ValueError as e:
    print(e)
# late_agent: patch changed (v1), re-read before writing
```

Note what the blackboard gives you: agents are added by declaring needs and outputs, not by rewiring a sequence; the log is an audit trail of who wrote what; and the version check stops a slow agent from silently overwriting a newer result. Note also what it does not give you: the agents each see only their inputs, so the fix agent does not know why the impact agent chose those models. If that reasoning matters, put it on the board too.

**From scratch: message passing.** The same kind of coordination through a broker. Each message has an envelope: an id, a topic, a sender, a **correlation id** that ties every message of one run together, and a payload. Messages to a topic nobody subscribes to go to a dead-letter list instead of disappearing.

```python
import itertools
from collections import defaultdict, deque

class Bus:
    """In-process stand-in for Kafka: topics, envelopes, a dead-letter list."""
    def __init__(self):
        self.subs, self.queue, self.dead = defaultdict(list), deque(), []
        self.ids = itertools.count(1)

    def subscribe(self, topic, handler):
        self.subs[topic].append(handler)

    def publish(self, topic, sender, payload, correlation_id=None):
        msg_id = next(self.ids)
        self.queue.append({"id": msg_id, "topic": topic, "sender": sender,
                           "correlation_id": correlation_id or msg_id, "payload": payload})

    def drain(self, max_messages=20):
        delivered = 0
        while self.queue and delivered < max_messages:
            msg = self.queue.popleft(); delivered += 1
            handlers = self.subs.get(msg["topic"])
            if not handlers:
                self.dead.append(msg); continue
            for h in handlers:
                h(msg)
        return delivered

bus = Bus()

def quality_agent(msg):          # listens for finished loads, publishes check results
    rows = msg["payload"]["rows"]
    status = "fail" if rows < 1000 else "pass"
    bus.publish("dq.result", "quality_agent", {"table": msg["payload"]["table"], "status": status},
                msg["correlation_id"])

def incident_agent(msg):         # listens for failed checks only
    if msg["payload"]["status"] == "fail":
        print(f'incident for {msg["payload"]["table"]} (run {msg["correlation_id"]})')

bus.subscribe("load.finished", quality_agent)
bus.subscribe("dq.result", incident_agent)
bus.publish("load.finished", "loader", {"table": "raw.orders", "rows": 412})
bus.publish("load.finished", "loader", {"table": "raw.refunds", "rows": 52_000})
bus.publish("schema.changed", "watcher", {"table": "raw.fx"})
print(bus.drain(), "delivered;", len(bus.dead), "dead-lettered")
# incident for raw.orders (run 1)
# 5 delivered; 1 dead-lettered
```

The handlers here are plain functions; in a real system each would be an agent in its own Kafka consumer group, and `max_messages` would be a quota. On Google Cloud, Pub/Sub plays the same role, with dead-letter topics built in. The correlation id is what lets you rebuild one run's full story from logs, the multi-agent equivalent of a trace id.

**Shared state in LangGraph: reducers decide.** In a graph state, a key without a reducer keeps one value. If two nodes running in the same step both write it, LangGraph refuses the update rather than picking one silently. A key with a reducer such as `operator.add` merges the writes (run with `langgraph` 1.2.24):

```python
from typing import Annotated, TypedDict
import operator
from langgraph.graph import StateGraph, START, END
from langgraph.errors import InvalidUpdateError

class Board(TypedDict, total=False):
    status: str                                   # last-value channel: one writer per step
    findings: Annotated[list[str], operator.add]  # reducer: parallel writers are merged

def freshness(state): return {"findings": ["orders late"], "status": "checked"}
def volume(state):    return {"findings": ["orders short"], "status": "checked"}

g = StateGraph(Board)
g.add_node("freshness", freshness); g.add_node("volume", volume)
g.add_edge(START, "freshness"); g.add_edge(START, "volume")
g.add_edge("freshness", END); g.add_edge("volume", END)
try:
    g.compile().invoke({})
except InvalidUpdateError as e:
    print(type(e).__name__, str(e).split(".")[0])
# InvalidUpdateError At key 'status': Can receive only one value per step
```

Remove `status` from one node, or give it a reducer, and the graph runs; `findings` already merges both lists. This is the graph equivalent of "one owner per column".

> **Pitfall:** A shared state that grows without limit. When every agent appends its full reasoning to a shared message list, every agent's context fills with every other agent's work, which undoes the isolation that justified multiple agents. Give each agent its own private messages and share only typed results (findings, decisions, artifact references).

> **Tip:** Treat inter-agent messages like data contracts: a schema per message type, validated on send and receive, versioned when it changes. Free-text messages between agents are the multi-agent version of a CSV with no header.

::: deeper
**Choosing.** Shared state fits a single process with a known graph: simple, inspectable, checkpointable. A blackboard fits loosely ordered, data-driven work where new agents are added often (audits, enrichment). Message passing fits distributed systems, different teams' agents, and long-running or event-driven flows; it is the only choice across services. Most production systems mix them: a LangGraph state inside each service, events between services, A2A to partners.

**Concurrency and consistency.** Parallel agents writing shared state have every problem concurrent database writers have: lost updates, read-modify-write races, partial failures. The solutions are the familiar ones: one owner per key, reducers for keys with many writers, optimistic concurrency with versions, and idempotent writes keyed by run id so a retried agent does not post twice. LangGraph runs nodes in **supersteps** (all nodes scheduled together run, then their updates are applied together), which is why it can detect conflicting writes deterministically.

**Delivery semantics.** Message brokers deliver at least once, so agent handlers must be idempotent: processing the same message twice must not file two tickets. Ordering is per key or per partition, not global. Dead-letter topics need an owner and an alert. Kafka (or RabbitMQ, or NATS) gives you this machinery; an in-process list does not, so do not run long multi-agent flows on in-memory queues in production.

**Memory versus coordination state.** The shared state of one run is short-term memory for the team; facts that should outlive the run belong in a long-term store with scopes and provenance ([Long-term memory](#memory)). Do not let agents write directly to long-term memory from shared state without the consolidation and validation rules covered there; a hallucinated finding on a blackboard can otherwise become a permanent "fact".

**Classic roots.** Blackboard systems (Hearsay-II for speech recognition, later many expert systems) had a shared data structure, knowledge sources that watched it, and a control component that chose which source runs next. LLM blackboard designs reuse all three parts; the control component is often a small scheduler like `run_board`, sometimes a model.

> **Interview:** "Agents in your system share one message list and quality got worse as you added agents. Why, and what would you change?" Every agent's context now includes every other agent's tool calls and reasoning, which is noise for it and grows with each agent (context rot). I would give each agent private messages, share only typed results through state keys with reducers, and put large outputs in an artifact store referenced by id.

> **Interview:** "How would you connect agents owned by different teams?" Through messages with explicit contracts, not shared state: an event topic or A2A tasks, with a schema per message type, correlation ids for tracing, idempotent handlers, dead-letter handling and authentication between services. Each team keeps its agent's internal state private.
:::

::: check
?? What does the version check in `Blackboard.write` prevent?
A lost update: an agent overwriting an entry that changed after it read it. The write fails and the agent must re-read first.

?? Why does LangGraph raise `InvalidUpdateError` in the example, and what are two fixes?
Two nodes in the same superstep wrote the key `status`, which has no reducer. Fix by having only one node write it, or by giving it a reducer with `Annotated[..., reducer]`.

?? What is a correlation id for in message passing between agents?
It ties every message of one run together, so you can trace and debug the whole flow across agents, and route replies to the right run.
:::

::: resources
- Docs: [Graph API](https://docs.langchain.com/oss/python/langgraph/graph-api) · LangGraph state, reducers, supersteps and `Command`
- Article: [Blackboard system](https://en.wikipedia.org/wiki/Blackboard_system) · the classic architecture and Hearsay-II
- Spec: [Agent2Agent (A2A) protocol](https://a2a-protocol.org/latest/) · message passing between agents across services
- Docs: [Apache Kafka documentation](https://kafka.apache.org/documentation/) · topics, consumer groups, delivery semantics, ordering
- Paper: [AutoGen](https://arxiv.org/abs/2308.08155) · agents coordinating by conversation
- Blog: [Don't build multi-agents](https://cognition.com/blog/dont-build-multi-agents) · the case for sharing full context and traces
:::

### Debate, voting and consensus

::: plain
When a review board gets a hard question, it can ask three experts to answer separately and take the majority view. Or it can put the experts in a room, have each explain their answer, and let them change their minds after hearing the others. The room can catch a mistake that two experts shared, but it can also talk a correct expert out of the right answer. Whether the meeting helps depends on whether people change their minds because of evidence or because of pressure.
:::

**What it is.** Several agents (or several samples from one model) answer the same question, and their answers are combined into one.

- **Voting.** Agents answer independently; the most common answer wins. With one model sampled several times this is **self-consistency** (Wang et al., 2022). Code does the counting.
- **Debate.** Agents answer, then see the others' answers and reasoning and revise, for a few rounds, until they agree or a round limit is hit. Du et al. (2023) reported better factuality and reasoning this way; Liang et al. (2023) used debate with a judge to counter a single model's tendency to stick with its first idea.
- **Critic or judge.** One agent proposes and another critiques ([Evaluator-optimizer](#patterns)), or a judge reads all answers and picks or merges. ChatEval (2023) used a panel of debating agents as an evaluator.

**From scratch.** Three agents diagnose a failed load from its log. Their first answers disagree three ways, so a vote gives no majority. In the debate round each agent sees the others' answers and evidence. The key design choice is in `revise`: an agent switches only when another answer has **stronger evidence that code can check** (the quoted evidence really appears in the log, on an error line). Persuasion by evidence, not by majority.

```python
from collections import Counter

LOG = """03:02 load_orders started
03:04 WARN executor memory 92%
03:05 ERROR column "amount" of relation "orders" is of type numeric but expression is of type bigint
03:05 load_orders failed after 1 attempt"""

# First answers from three agents (in production: three model calls, maybe different models).
FIRST = {
    "agent_a": {"label": "schema_change", "evidence": "is of type numeric but expression is of type bigint"},
    "agent_b": {"label": "memory",        "evidence": "executor memory 92%"},
    "agent_c": {"label": "late_data",     "evidence": "partition arrived late"},
}

def grounded(answer):
    return answer["evidence"] in LOG                     # checked in code, not by a model

def vote(answers):
    counts = Counter(a["label"] for a in answers.values())
    (top, n), *rest = counts.most_common()
    if rest and rest[0][1] == n:
        return None, counts                              # tie: no majority
    return top, counts

def revise(name, mine, others):
    """Scripted debate turn: keep my answer unless another one has stronger checked evidence.
    'Stronger' here: it is grounded and explains a failure line (contains ERROR context)."""
    def strength(a):
        line = next((l for l in LOG.splitlines() if a["evidence"] in l), "")
        return (grounded(a), "ERROR" in line)
    best = max([mine] + others, key=strength)
    return best if strength(best) > strength(mine) else mine

def debate(answers, max_rounds=3):
    for rnd in range(1, max_rounds + 1):
        new = {}
        for name, mine in answers.items():
            others = [a for n, a in answers.items() if n != name]   # everyone sees the others
            new[name] = revise(name, mine, others)
        changed = [n for n in answers if new[n] != answers[n]]
        answers = new
        print(f"round {rnd}: changed {changed}")
        label, counts = vote(answers)
        if label and counts[label] == len(answers):                 # stop at consensus
            return label, rnd
    return vote(answers)[0], max_rounds

print(vote(FIRST))
# (None, Counter({'schema_change': 1, 'memory': 1, 'late_data': 1}))
print({n: grounded(a) for n, a in FIRST.items()})
# {'agent_a': True, 'agent_b': True, 'agent_c': False}
print(debate(FIRST))
# round 1: changed ['agent_b', 'agent_c']
# ('schema_change', 1)
```

Agent C cited evidence that is not in the log (a hallucination), and agent B cited a real line that is only a warning. Both moved to the answer backed by the error line, and the debate stopped at consensus after one round. With real models, `revise` is a model call that receives the other answers, and the risk is that it switches for weaker reasons: because two agents agree, or because one sounds confident. That is why the evidence check sits in code.

**Choosing a combination method.**

| Method | Cost | Helps when | Fails when |
|---|---|---|---|
| Single answer | 1 call | The task is easy or verifiable afterwards | Answers vary run to run |
| Majority vote / self-consistency | n calls, parallel | Answers are short and comparable (labels, numbers) | Agents share the same mistake |
| Debate | n calls per round, sequential rounds | Errors can be exposed by evidence another agent has | Agents conform to the majority or to confidence |
| Judge over candidates | n + 1 calls | Answers are long and need comparison | The judge has the same blind spots |
| Verifier in code | n calls + cheap checks | Correctness can be tested (SQL runs, counts match) | Nothing to test against |

> **Pitfall:** Voting with identical agents. Three copies of the same model with the same prompt and low temperature give nearly the same answer, so the vote adds cost and no information. Diversity has to come from somewhere: sampling temperature, different prompts or personas, different models, or different evidence (each agent queries a different source).

> **Tip:** When the answer can be checked, check it instead of voting. For "which query returns the right row count", run the candidates against a sample and compare with the known count; for "is this column PII", test the values with a pattern. Votes and debate are for questions without a cheap verifier.

::: deeper
**What the research says.** Du et al. (2023) showed gains from debate on arithmetic, reasoning and factuality tasks with several agents over a few rounds. Later work was more cautious. Smit et al. (2023), "Should we be going MAD?", found that multi-agent debate systems did not reliably beat other prompting strategies such as self-consistency, although tuned versions could. Choi, Zhu and Li (2025), "Debate or Vote", found across seven benchmarks that majority voting explains most of the gains usually credited to debate, and argued that debate on its own does not improve expected correctness; they report that biasing updates toward correction helps. The practical reading: start with voting, compare against a token-matched baseline, and add debate only if it beats that.

**Why debate can make things worse.** Models tend to agree with what they are shown (sycophancy) and with the majority (conformity). A correct minority agent can be argued out of its answer. Countermeasures: show other answers anonymously, ask each agent to state what evidence would change its mind before seeing the others, require changes to cite new evidence, and use a judge that sees the evidence rather than the vote count.

**Correlated errors.** Voting assumes errors are independent. Agents built on the same model, prompt and retrieved documents make the same mistakes, so five votes can be no better than one. Measure agreement on a labelled set: if the agents almost always agree, the extra calls are wasted; if they disagree often and the majority is usually right, voting is earning its cost.

**Aggregation beyond the majority.** For numbers, take the median, which resists outliers. For sets (tables affected, PII columns), take the union for recall-critical tasks and the intersection for precision-critical ones. For free text, a judge that merges answers and keeps citations. Use disagreement as a signal: when agents split, route the case to a human instead of forcing a consensus.

**Debate for oversight.** Irving, Christiano and Amodei (2018) proposed debate as an AI safety technique: two agents argue and a weaker judge decides, on the theory that it is easier to judge an argument than to produce the answer. This is a different goal from accuracy on benchmarks, but the same mechanism.

> **Interview:** "Does multi-agent debate improve accuracy?" Sometimes, but less than early papers suggested. Recent work found that majority voting accounts for most of the gain and that debate alone does not reliably beat self-consistency at equal cost. I would use voting for short, comparable answers, a verifier in code wherever correctness can be tested, and debate only when agents have different evidence and a token-matched evaluation shows it helps.

> **Interview:** "Three agents classify data quality incidents and usually agree. Is that good?" Not necessarily: high agreement may mean correlated errors, making two of the three calls wasted. Check accuracy against labelled incidents and agreement rates. If agreement is near 100%, use one agent; if they disagree on hard cases, use disagreement to route those to a human.
:::

::: check
?? What is self-consistency?
Sampling several answers from one model for the same question and taking the most common one; voting over samples instead of over different agents.

?? Why does the example check evidence in code rather than letting agents judge it?
Models can switch answers for weak reasons, such as majority pressure or confident wording. A code check (the quoted evidence really appears in the log, on an error line) makes changes depend on verifiable facts.

?? What did "Debate or Vote" (2025) conclude?
That majority voting explains most of the gains usually attributed to multi-agent debate, and that debate alone does not improve expected correctness.
:::

::: resources
- Paper: [Improving Factuality and Reasoning in Language Models through Multiagent Debate](https://arxiv.org/abs/2305.14325) · Du et al., 2023
- Paper: [Encouraging Divergent Thinking in Large Language Models through Multi-Agent Debate](https://arxiv.org/abs/2305.19118) · Liang et al., 2023
- Paper: [Self-Consistency Improves Chain of Thought Reasoning](https://arxiv.org/abs/2203.11171) · Wang et al., 2022: voting over samples
- Paper: [Should we be going MAD?](https://arxiv.org/abs/2311.17371) · Smit et al., 2023: debate versus other strategies
- Paper: [Debate or Vote](https://arxiv.org/abs/2508.17536) · Choi, Zhu and Li, 2025: voting explains most of the gain
- Paper: [ChatEval](https://arxiv.org/abs/2308.07201) · multi-agent debate as an evaluator
- Paper: [AI safety via debate](https://arxiv.org/abs/1805.00899) · Irving et al., 2018
:::

### How multi-agent systems fail

::: plain
A relay team can lose a race with four fast runners: someone drops the baton, two runners both think the other is taking the third leg, or the last runner stops at the wrong line. Each runner did their part; the race was lost at the handovers and the rules. Multi-agent systems fail the same way. The individual agents are often fine; the failures happen in the specs, the handovers and the checking at the end.
:::

**What it is.** Multi-agent systems fail in all the ways a single agent does, plus new ways that come from coordination. The most useful map is the **Multi-Agent System Failure Taxonomy (MAST)** from "Why Do Multi-Agent LLM Systems Fail?" (Cemri, Pan, Yang and others, 2025). The authors studied execution traces from popular open-source multi-agent frameworks (the released dataset has more than 1,600 annotated traces across 7 frameworks), built the taxonomy from 150 traces with expert annotators (inter-annotator agreement, Cohen's kappa, of 0.88), and found 14 failure modes in 3 categories.

[[fig:multiagent_mast]]

| Category | Failure modes | What it looks like on a data platform |
|---|---|---|
| System design issues | Disobey task specification; disobey role specification; step repetition; loss of conversation history; unaware of termination conditions | The "reviewer" starts rewriting SQL; an agent reruns the same profiling query five times; nobody knows when the investigation is done |
| Inter-agent misalignment | Conversation reset; fail to ask for clarification; task derailment; information withholding; ignored other agent's input; reasoning-action mismatch | The freshness agent knows the load failed but reports only "checked"; the fixer ignores the reviewer's rejection reason; an agent says it will check partitions and then queries the whole table |
| Task verification | Premature termination; no or incomplete verification; incorrect verification | The supervisor declares success after one worker answers; the verifier checks the SQL compiles but not that row counts match |

In the version of the paper checked for this course, step repetition, reasoning-action mismatch and being unaware of termination conditions were among the most frequent modes, and verification failures as a group were common. The authors' main argument matters more than the percentages: many failures come from **system design** (unclear roles and specs, no stop conditions, weak verification), not only from model limits, so better models alone will not remove them. They also showed that simple fixes (clearer role specifications, a final verification step) helped in case studies on the ChatDev framework, but did not fix everything.

**The other failure: cost.** Anthropic's numbers give the baseline: multi-agent runs used about 15 times the tokens of a chat. Loops and over-delegation multiply that. Two agents stuck in a fix-and-reject loop cost tokens on every round until something stops them, and that something must be code.

**Error compounding.** A long multi-agent run is a chain of steps, each of which can go wrong. If each step is right 95% of the time and errors are independent, a 20-step run is fully right only about a third of the time. Anthropic made the same point about its research system: errors compound in long-running, stateful agents, so it resumes from checkpoints rather than restarting and lets agents adapt when a tool fails.

**From scratch: a run guard.** Every message between agents passes through a guard that detects no progress (the same agent sending the same message again, MAST's step repetition) and enforces a message and token budget. The buggy pair below is a classic: a reviewer rejects a patch with a clear reason, and the fixer ignores the reason (MAST's "ignored other agent's input") and resubmits the same patch.

```python
import hashlib

class GuardTripped(Exception):
    pass

class RunGuard:
    """Watches every inter-agent message. Stops repeats (no progress) and cost blowups."""
    def __init__(self, max_messages=12, max_tokens=20_000, max_repeats=2):
        self.max_messages, self.max_tokens, self.max_repeats = max_messages, max_tokens, max_repeats
        self.messages, self.tokens, self.seen = 0, 0, {}

    def check(self, sender, receiver, text):
        self.messages += 1
        self.tokens += len(text) // 4 + 500                      # rough: payload + per-call overhead
        digest = hashlib.sha1(f"{sender}:{text}".encode()).hexdigest()
        self.seen[digest] = self.seen.get(digest, 0) + 1
        if self.seen[digest] > self.max_repeats:
            raise GuardTripped(f"step repetition: {sender} sent the same message {self.seen[digest]} times")
        if self.messages > self.max_messages or self.tokens > self.max_tokens:
            raise GuardTripped(f"budget: {self.messages} messages, ~{self.tokens} tokens")

# A buggy pair: the reviewer rejects, the fixer resubmits the same patch every time.
def fix_agent(feedback):
    return "ALTER TABLE raw.orders ALTER COLUMN amount TYPE numeric"

def review_agent(patch):
    return "rejected: rewrites 2B rows under an exclusive lock; add a new column and backfill in batches"

def run(guard):
    feedback = "start"
    for _ in range(20):
        patch = fix_agent(feedback);      guard.check("fix_agent", "review_agent", patch)
        feedback = review_agent(patch);   guard.check("review_agent", "fix_agent", feedback)
        if feedback.startswith("approved"):
            return "done"

try:
    run(RunGuard())
except GuardTripped as e:
    print("stopped:", e)
# stopped: step repetition: fix_agent sent the same message 3 times

try:
    run(RunGuard(max_repeats=100))                # repetition check off: the budget still stops it
except GuardTripped as e:
    print("stopped:", e)
# stopped: budget: 13 messages, ~6729 tokens

for p in (0.99, 0.95, 0.90):                      # chance every one of 20 steps is right
    print(p, round(p ** 20, 2))
# 0.99 0.82
# 0.95 0.36
# 0.9 0.12
```

Exact-match repetition is the cheapest progress check. Real loops are often near-repeats (the same patch with a comment changed), so production guards also compare normalised text or embeddings, and track whether the shared state actually changed between rounds.

**Controls, by failure category.**

| Category | Controls in code and design |
|---|---|
| Specification | One clear owner and output per agent; task briefs with objective, format, tools and boundaries; explicit "done" criteria and a stop condition in code |
| Loops and cost | Caps on steps, delegations, handoffs, depth and concurrency; a total token or money budget per run; repetition and no-progress detection |
| Misalignment | Typed messages with required fields (a rejection must carry a reason the fixer must address); shared artifacts instead of paraphrase; asking for clarification as an allowed action |
| Verification | An independent verifier with real checks (tests, row counts, dry runs, citation checks), run before the system may declare success; a human gate for irreversible actions |
| Observability | One trace per run with a correlation id, every agent call and message as a span, per-agent token counts; review traces regularly and label failures with the taxonomy |

> **Pitfall:** Verification by another model only. A reviewer agent that reads the fixer's SQL and says "looks correct" is MAST's incorrect verification waiting to happen. Where you can, verify with execution: run the query on a sample, compare row counts and checksums, run the DAG import test. Use model reviewers for what code cannot check, and treat their approval as one signal.

> **Tip:** Build a small failure library from your own traces. Each time a run goes wrong, label it with a MAST category, save the trace, and add a test or guard that would have caught it. After a few weeks you know which controls matter for your system.

::: deeper
**Why design, not just models.** MAST's authors argue, as a hypothesis, that many failures come from how the system is organised, so a better base model will not remove them all. The analogy is an organisation of capable people with unclear roles, no stop rule and no review. This matches the advice from Anthropic's post: most of its improvements came from prompts that describe tasks precisely, effort rules, tool descriptions and evaluation, not from adding agents.

**Diagnosing with traces.** You cannot debug a multi-agent run from its final answer. You need the full trace: every agent's inputs, tool calls, outputs and the messages between agents, linked by a run id. MAST's authors released an LLM-as-a-judge annotator that labels traces with the taxonomy, which you can adapt to classify your own failures at scale. Tracing tools are covered in [Evaluation and observability](#eval).

**Stateful failures in production.** Anthropic lists problems specific to long-running agent systems: an error mid-run cannot simply be retried from the start without wasting all earlier work, so checkpoint and resume; and deployments can break runs in progress, so they used rainbow deployments, shifting traffic gradually while old and new versions run side by side. Data engineers will recognise both: checkpointed streaming jobs and blue-green releases.

**Security failures.** More agents means more surfaces for prompt injection, and agents trusting each other's outputs lets an injection in one agent's input travel to another agent with more permissions. Treat messages from other agents as untrusted input when they carry content derived from external data, keep privileged actions in agents that never read raw external content, and validate structured results before acting. See [Production engineering, security and cost](#production).

**Evaluation baseline.** For every multi-agent system, keep a single-agent baseline with the same tools and a comparable token budget in your evaluation set. If the multi-agent version does not beat it on quality per dollar, the coordination is not paying for itself.

> **Interview:** "Your multi-agent pipeline costs three times more than last month and quality has not changed. How do you investigate?" Pull traces and break tokens down per agent and per run. Look for loops (repeated messages, ping-pong handoffs), over-delegation (more subagents per request than the task needs), growing shared state (agents re-reading everyone's history) and retries after failures. Then add the matching control: repetition detection, effort-scaling rules and spawn caps, private agent contexts with typed shared results, and a per-run budget with an alert.

> **Interview:** "What is MAST?" The Multi-Agent System Failure Taxonomy from "Why Do Multi-Agent LLM Systems Fail?" (2025): 14 failure modes in three categories (system design issues, inter-agent misalignment, task verification), built from annotated traces of open-source multi-agent frameworks. Its main lesson is that many failures are design failures (specs, roles, stop conditions, verification) that you can test and guard against.
:::

::: check
?? Name the three MAST categories and one failure mode in each.
System design issues (for example step repetition or unaware of termination conditions), inter-agent misalignment (for example ignored other agent's input or information withholding), and task verification (for example premature termination or incorrect verification).

?? If each step of a 20-step run is right 95% of the time, independently, how often is the whole run right, and what does that suggest?
About 36% (0.95 to the power 20). Long chains need checkpoints, verification between steps and fewer hops, not just better individual steps.

?? Why is a model reviewer not enough as the final check on a SQL fix?
It can approve wrong SQL (incorrect verification). Executing the query on a sample and comparing row counts or checksums checks the result directly.
:::

::: resources
- Paper: [Why Do Multi-Agent LLM Systems Fail?](https://arxiv.org/abs/2503.13657) · Cemri et al., 2025: the MAST taxonomy
- Repo: [MAST](https://github.com/multi-agent-systems-failure-taxonomy/MAST) · dataset, taxonomy and LLM annotator
- Blog: [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system) · production challenges, evaluation, stateful errors
- Blog: [Don't build multi-agents](https://cognition.com/blog/dont-build-multi-agents) · conflicting decisions between agents
- Paper: [ChatDev](https://arxiv.org/abs/2307.07924) · the role-playing software company studied in MAST's case studies
- Docs: [Subagents in the SDK](https://code.claude.com/docs/en/agent-sdk/subagents) · depth, concurrency and budget caps
:::

## Wrap-up

### Cheat sheet

| Idea | Remember |
|---|---|
| Why multiple agents | Context isolation, focused tools and prompts, parallelism, separation of duties |
| Cost | Anthropic: agents about 4x chat tokens, multi-agent about 15x; token use explained 80% of variance |
| When not to | Tightly coupled decisions (code, migrations), low-value requests, steps known in advance |
| Supervisor | Agent whose tools are agents; results return to it; validate targets, cap delegations |
| Hierarchy | Teams expose `run(task)` like an agent; keep it to two levels |
| Handoffs (swarm) | Active agent transfers control with `transfer_to_X`; allow-lists, cap per turn, persist active agent |
| LangGraph | `Command(goto=..., update=...)`; `graph=Command.PARENT` from a tool inside an agent subgraph |
| OpenAI Agents SDK | `handoffs=[...]`, `handoff(agent, input_type, input_filter)`, `agent.as_tool()` |
| Google ADK | `sub_agents` tree; `transfer_to_agent` in `chat` mode; `task` and `single_turn` modes; `AgentTool` |
| Subagents | Fresh context, brief in, summary out; detailed briefs, effort scaling, artifacts instead of telephone |
| Claude Agent SDK | `agents={name: AgentDefinition(...)}`, `Agent` tool allowed; cap depth, concurrency, `max_budget_usd` |
| Shared state | One schema, reducers for many writers, one owner per key |
| Blackboard | Agents run when their inputs appear; versioned writes, author on every entry |
| Message passing | Envelopes with ids and correlation ids, schemas, idempotent handlers, dead letters |
| Voting | Self-consistency; needs diverse, independent agents; check correlated errors |
| Debate | Can help with different evidence; often no better than voting at equal cost; guard against conformity |
| MAST | 14 failure modes: system design, inter-agent misalignment, task verification |
| Guards | Repetition detection, budgets, stop conditions and verification in code; trace every run |

### Practice

::: try
**1. Catch ping-pong.** The help desk in "Handoffs and swarm architectures" caps handoffs per turn. Write `ping_pong(route, limit)` that takes the list of active agents in order and returns `True` when control has bounced back to the agent it just came from (A to B to A) at least `limit` times. Test it on a bouncing route and a healthy one.
:::

?? A solution to 1

```python
def ping_pong(route, limit=2):
    """route: active agents in handoff order. Counts returns to the agent two steps back."""
    bounces = sum(1 for i in range(2, len(route))
                  if route[i] == route[i - 2] and route[i] != route[i - 1])
    return bounces >= limit

print(ping_pong(["triage", "trino_agent", "triage", "trino_agent"]))
# True
print(ping_pong(["triage", "trino_agent", "access_agent"]))
# False
```

Call it after each handoff in `run_turn` and, when it trips, refuse the transfer with an error telling the current agent to answer. Also log the pair: frequent bounces between the same two agents mean their descriptions overlap.

::: try
**2. Do not wait forever.** Change the subagent fan-out so the lead waits at most a fixed time. Subagents that finish late are reported as timed out, and the lead's summary says the coverage is partial instead of pretending it checked everything.
:::

?? A solution to 2

```python
import time
from concurrent.futures import ThreadPoolExecutor, wait

def subagent(dataset, seconds):
    time.sleep(seconds)                       # stand-in for a subagent's run
    return f"{dataset}: no PII"

def fan_out(jobs, deadline_s):
    pool = ThreadPoolExecutor(max_workers=len(jobs))
    futures = {pool.submit(subagent, d, s): d for d, s in jobs.items()}
    done, not_done = wait(futures, timeout=deadline_s)
    pool.shutdown(wait=False, cancel_futures=True)
    results = sorted(f.result() for f in done)
    missing = sorted(futures[f] for f in not_done)
    return results, missing

results, missing = fan_out({"sales": 0.01, "ops": 0.02, "marketing": 0.5}, deadline_s=0.2)
print(results)
# ['ops: no PII', 'sales: no PII']
print("partial coverage, timed out:", missing)
# partial coverage, timed out: ['marketing']
```

Python threads cannot be killed, so a real system cancels the late subagent's work through its own API (or lets it finish and discards the result) and, ideally, retries it later. The important part is that the lead's answer names what was not checked; "no PII found" after skipping a dataset is a dangerous answer.

::: try
**3. Vote on a number.** Three agents estimate how many duplicate `order_id` values a table has. Write `combine(estimates, tolerance)` that returns the median, and returns `None` (route to a human) when the estimates spread by more than `tolerance` relative to the median.
:::

?? A solution to 3

```python
import statistics

def combine(estimates, tolerance=0.1):
    med = statistics.median(estimates)
    spread = (max(estimates) - min(estimates)) / med if med else float("inf")
    return med if spread <= tolerance else None

print(combine([1204, 1198, 1210]))
# 1204
print(combine([1204, 120, 1210]))
# None
```

The median ignores one wild estimate, but a large spread is information: the agents disagree, so the system should not pretend to know. Better still, run `SELECT COUNT(*) - COUNT(DISTINCT order_id)` and skip the vote; it is a question code can answer.

::: try
**4. Label the trace.** A multi-agent run that migrates a table produced this trace: the planner asked the coder to "update the orders model"; the coder changed `stg_orders` but not `fct_revenue`, which also reads the column; the tester ran `dbt compile` (which succeeded) and reported success; the planner closed the task. Two hours later the revenue dashboard broke. Label the failures with MAST categories and name one control for each.
:::

?? Answer to 4
The brief was vague and did not list the impacted models: a specification problem (disobey or underspecified task specification, system design issues); fix with task briefs that include the full list of impacted models from lineage, computed in code. The coder did not mention that other models read the column, if it saw them: information withholding (inter-agent misalignment); fix with a structured result that lists files changed and files that reference the column. The tester checked compilation, not behaviour: incorrect or incomplete verification (task verification); fix with a verifier that runs the affected models on a sample and compares row counts and totals with the previous version. The planner closed the task on a weak signal: premature termination; fix with explicit done criteria in code (all impacted models built and verified).

::: try
**5. Design it.** Your team wants an agent system that onboards a new source: read the vendor's API docs, propose a schema, write the ingestion DAG, write dbt models and tests, and open a pull request. Would you use one agent or several? Sketch the design and say where the controls go.
:::

?? Answer to 5
Mostly one agent, with subagents only where work is independent and read-heavy. The steps are tightly coupled (the schema drives the DAG, the DAG drives the models), so splitting them across peer agents risks conflicting decisions. Design: a single lead agent with a plan-and-execute loop owns the decisions and the pull request. It uses read-only subagents to read long vendor documentation and summarise endpoints, pagination and rate limits into a structured spec (context isolation), and possibly a reviewer subagent with fresh context to critique the final diff. Controls: a schema proposal validated in code (types, keys) and approved by a human before code is written; generated DAG and dbt code verified by execution (import test, `dbt build` on a sample, row count checks); caps on steps, subagents and budget; one trace per run. Measure against a single-agent baseline without subagents.

### What comes next

You can now decide whether a task needs more than one agent, and build the main architectures from scratch: supervisors and hierarchies, handoff networks, and subagents that keep the lead's context clean. You can make agents share information through state, a blackboard or messages, combine their answers by voting or debate, and recognise and guard against the failure modes that MAST catalogues. Along the way you met four frameworks' versions of the same ideas. The next section, **Agent frameworks**, steps back and compares those frameworks directly: LangGraph and LangChain, Google ADK, the Claude Agent SDK, the OpenAI Agents SDK, CrewAI, Microsoft Agent Framework, PydanticAI, DSPy and LlamaIndex, what each is built around, and how to choose between them.
