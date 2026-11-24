This is where the course starts. It is written for a working data engineer: you know Python, SQL, pipelines, orchestration with Airflow and messaging with Kafka, but you have not built an agent yet, or you have built one by copying a tutorial and want to understand what is really going on. Nothing here assumes machine learning knowledge. Where a model's internals matter for building agents, the section explains them from the ground up and stops there.

**What you will be able to do afterwards**

- Explain what an AI agent is, how it differs from a chatbot and from a workflow, and where agents are used in production today.
- Describe how a large language model produces text (tokens, context windows, next-token prediction, sampling) and why it sometimes makes things up.
- Call a model API correctly: messages and roles, system prompts, streaming, stop reasons, token counting and rate limits.
- Write prompts that agents can follow reliably, and version them like code.
- Write the agent loop yourself in about forty lines of Python, with stop conditions, a step limit and state.
- Get validated, typed JSON out of a model, and repair it when it is wrong.
- Decide when to turn on a reasoning model's thinking, and what it costs.
- Argue, in an interview or a design review, when **not** to build an agent.

**The big picture**

An agent is a program with a loop in it. Each time round the loop, the program sends everything it knows so far to a language model; the model either answers or asks to use a tool (run a query, read a file, call an API); the program runs the tool, adds the result to what it knows, and goes round again. Everything in this course hangs off that loop. Tools and the Model Context Protocol are about what the model can *do*. Context engineering and memory are about what the model can *see*. Evaluation, observability and production engineering are about trusting what it did. If you remember one picture from this section, make it the loop.

The second idea is just as important: **most problems do not need an agent.** A fixed sequence of model calls, written as ordinary code, is cheaper, faster and easier to test. The last topic of this section is about telling the two apart, and interviewers ask about it constantly.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

> **Tip:** Every from-scratch example in this section runs with plain `python3` and a small **mock model**: a function that returns scripted responses in the same shape a real model API returns. You see the real control flow without an API key or a bill. Swapping the mock for a real client is a one-line change, shown where it matters.

## Agents and LLMs

Before writing an agent you need two things: a clear idea of what the word means (it is used loosely, and interviewers test whether you can be precise), and a working model of the engine underneath, the large language model. This group covers both, then the practical side: how to call a model API, and how to write prompts that an agent can act on.

If you have used ChatGPT, Claude or Gemini in a browser, you have seen the model from the outside. Here you look at it the way a builder does: as a function that takes a list of messages and returns a message, with a cost, a latency, a context limit and a failure rate, just like any other dependency in a pipeline.

### What an AI agent is

::: plain
A chatbot answers your question. An agent gets a job done. If you ask a chatbot "why did last night's load fail?", it tells you what *might* have happened. If you give the same question to an agent, it opens the Airflow logs, runs a query against the table, notices a schema change, and comes back with the cause and the evidence. The difference is that the agent can take actions (through tools) and decides for itself which action to take next, in a loop, until the job is done.
:::

There is no single official definition, but the useful ones agree. Anthropic's widely cited post "Building effective agents" separates **workflows**, "systems where LLMs and tools are orchestrated through predefined code paths", from **agents**, "systems where LLMs dynamically direct their own processes and tool usage". Google's agent whitepaper describes an agent as an application that tries to achieve a goal by observing the world and acting on it with the tools it has. Put simply:

> An **agent** is a language model running in a loop, using tools, to reach a goal, where the model decides the next step.

Each part of that sentence matters:

- **Language model**: the reasoning engine. It reads text and writes text. On its own it cannot do anything except produce words.
- **Tools**: functions the program exposes to the model, such as `run_sql`, `read_file` or `create_ticket`. The model asks for a tool by name with arguments; your code runs it. Tools are how words become actions.
- **Loop**: the program keeps calling the model, feeding back tool results, until a stop condition is met.
- **Goal**: a task with a definition of done, not just a question.
- **The model decides the next step**: this is the line between an agent and a workflow. In a workflow, *your code* decides what happens next. In an agent, *the model* does.

[[fig:foundations_spectrum]]

**A spectrum, not a switch.** It helps to place systems on a scale of autonomy, from "the code decides everything" to "the model decides everything":

| Level | What it looks like | Who decides the next step | Data engineering example |
|---|---|---|---|
| Single call | One prompt in, one answer out | Your code (there is no next step) | Summarise a failed job's log |
| Workflow | Fixed chain of calls, maybe with branches | Your code | Classify a ticket, then route it to one of three fixed prompts |
| Tool-using assistant | One model call that may request a tool once | Mostly your code | "What is the row count of `orders`?" answered with one query |
| Agent | Loop of model calls and tool calls | The model, within limits | Investigate a data-quality alert end to end |
| Multi-agent system | Several agents handing work to each other | Several models | A planner agent assigns tables to worker agents for a migration |

The same product often mixes levels: a mostly fixed pipeline with one agentic step in the middle is normal and usually wise.

To make "who decides" concrete, compare these two functions. The first is a workflow: the order of steps is written in code. The second is the skeleton of an agent: the model's reply chooses the step.

```python
def workflow(alert, llm):
    summary = llm(f"Summarise this alert: {alert}")          # step 1, always
    severity = llm(f"Rate severity 1-5: {summary}")          # step 2, always
    return {"summary": summary, "severity": severity}        # the code chose the path

def agent(goal, llm, tools, max_steps=10):
    history = [goal]
    for _ in range(max_steps):
        decision = llm(history)                  # the model chooses what happens next
        if decision["type"] == "answer":
            return decision["text"]
        result = tools[decision["tool"]](**decision["args"])
        history.append(result)
    return "stopped: step limit reached"
```

You will build the full version of the second function, runnable, in the topic "The agent loop: observe, think, act".

**Where agents work today.** Agents earn their cost where the path to the answer is not known in advance, the steps can be checked, and mistakes can be caught. As of late 2026 the strongest production uses are:

- **Coding agents** (Claude Code, Gemini CLI, Codex and similar) that read a repository, edit files, run tests and iterate. Tests give them a cheap, objective check, which is why they work so well.
- **Research and analysis agents** that search, read and summarise many sources.
- **Customer support** with access to order systems and refunds, under human approval for risky actions.
- **Data agents**: answering business questions over a warehouse (text-to-SQL with self-correction), triaging failed pipeline runs, drafting data-quality rules, documenting tables. The section [Agents for data engineering](#dataagents) covers these in depth.
- **Operations**: incident triage that gathers logs, metrics and recent deploys before a human decides.

They still struggle with long tasks where an early mistake silently compounds, with work that has no cheap way to verify the result, and anywhere a wrong action is expensive and irreversible.

> **Pitfall:** Calling every LLM feature an "agent". If your code fixes the sequence of calls, it is a workflow, and that is a compliment: it is easier to test and cheaper to run. Interviewers notice when candidates use the word loosely.

> **Tip:** When you describe a system, say three things: what the goal is, which tools the model has, and who decides the next step. That one sentence settles most "is it an agent?" debates.

::: deeper
**The classic definition.** Long before LLMs, Russell and Norvig's textbook defined an agent as anything that perceives its environment through sensors and acts on it through actuators. An LLM agent fits that frame: tool results and user messages are the percepts, tool calls are the actions, and the model plus your loop is the agent program. What is new is that the "agent program" is a general-purpose model steered by natural language instead of a hand-written policy.

**Why agents became practical around 2024 to 2025.** Three things improved together: models became reliable at emitting well-formed tool calls (function calling trained into the model rather than parsed from free text), context windows grew large enough to hold long tool histories, and models got better at recovering from their own mistakes when they saw an error message. The ReAct paper (2022) showed the reason-then-act pattern; the models simply had to catch up.

**The components most agent architectures share**, which later sections cover one by one:

| Component | Question it answers | Section |
|---|---|---|
| Model | How does it reason and decide? | this one |
| Tools | What can it do? | [Tool use and function calling](#tools), [MCP](#mcp) |
| Context and memory | What can it see and remember? | [Context engineering, memory and RAG](#memory) |
| Orchestration | How is the loop run and who coordinates? | [Agent design patterns](#patterns), [Multi-agent systems](#multiagent) |
| Guardrails and evaluation | How do we trust it? | [Evaluation and observability](#eval), [Production](#production) |

**Failure modes that come from autonomy itself**: loops that never converge, tool calls with wrong arguments, actions taken on hallucinated facts, cost and latency that vary wildly between runs, and prompt injection (text in a tool result that hijacks the model). Each is manageable, but each is a cost a workflow does not pay.

> **Interview:** "What is the difference between an agent and a workflow?" Both use LLMs and tools. In a workflow, the control flow is written in code, so the sequence of steps is fixed or chosen by explicit branches. In an agent, the model chooses the next step at run time, in a loop, based on what it has observed so far. Agents handle open-ended tasks where the path is not known in advance, at the price of higher cost, latency variance and a harder testing story. I start with a workflow and only move a step to an agent when the fixed path stops covering the real cases.

> **Interview:** "Is a chatbot with RAG an agent?" Usually not. If the code always retrieves and then answers, the path is fixed: that is a workflow. It becomes agentic when the model decides whether to search, what to search for, and whether to search again.
:::

::: check
?? In one sentence, what makes a system an agent rather than a workflow?
The model, not your code, decides the next step at run time, in a loop that runs tools and feeds the results back.

?? Your pipeline always calls an LLM to classify a ticket and then calls one of three fixed prompts based on the class. Is it an agent?
No. It is a workflow with a routing step: the code defines every possible path, and the model only picks a label.

?? Why do coding agents work better than many other kinds of agent?
Their work can be checked cheaply and objectively (the code compiles, the tests pass), so the agent can detect and fix its own mistakes inside the loop.
:::

::: resources
- Blog: [Building effective agents](https://www.anthropic.com/research/building-effective-agents) · Anthropic's definitions of workflows and agents, and the patterns between them
- Article: [Agents](https://huyenchip.com/2025/01/07/agents.html) · Chip Huyen's long overview of agent components, planning and failure modes
- Blog: [LLM Powered Autonomous Agents](https://lilianweng.github.io/posts/2023-06-23-agent/) · Lilian Weng's classic survey of planning, memory and tool use
- Course: [What is an agent?](https://huggingface.co/learn/agents-course/unit1/what-are-agents) · the opening unit of Hugging Face's free, open-source agents course
- Paper: [ReAct: Synergizing Reasoning and Acting in Language Models](https://arxiv.org/abs/2210.03629) · the paper behind the reason-then-act loop
:::

### LLM fundamentals for agent builders

::: plain
A large language model is an extremely well-read autocomplete. It has read a huge amount of text and learned, for any piece of text, which word is likely to come next. When it "answers" you, it is really continuing the conversation one small piece at a time, each time picking a likely next piece. It has no database of facts to look things up in; it has patterns. That is why it can be brilliant and confidently wrong in the same paragraph.
:::

You do not need to train models to build agents, but five ideas from the inside explain almost every behaviour you will debug: tokens, the context window, next-token prediction, sampling, and hallucination.

**Tokens.** Models do not read characters or words; they read **tokens**, chunks of text from a fixed vocabulary of typically tens of thousands to a few hundred thousand entries. Common words are one token; rare words, identifiers and numbers split into several. As a rule of thumb, one token is about four characters or three quarters of an English word, but code, JSON, non-English text and long numbers use more tokens per character. Tokens matter because **everything is counted and billed in tokens**: the input you send, the output you get, the context limit, the rate limits. Each provider uses its own tokenizer, so the same text costs a different number of tokens on different models, and sometimes on different model generations from the same provider.

> **Pitfall:** Estimating tokens with "characters divided by four" for anything that matters, such as a budget check before a call. Use the provider's token-counting endpoint (shown in the next topic). Wide tables, JSON and SQL can come out much larger than the rule of thumb suggests.

**The context window.** The model can only consider a limited number of tokens at once: the system prompt, the conversation so far, tool definitions, tool results, and the answer it is writing. That limit is the **context window**. Current frontier models offer windows from a few hundred thousand up to about a million tokens, and the output has its own, smaller cap per response. The model has no memory between calls: every call starts from zero, and "memory" means you sent the history again. For an agent this is the central constraint, because every tool result is appended to the history and the history grows on every step.

**Next-token prediction.** At its core the model is a function: given the tokens so far, it outputs a score (a **logit**) for every token in the vocabulary. A softmax turns the scores into probabilities. One token is picked, appended, and the whole thing runs again. A 500-token answer is 500 forward passes. Three consequences follow:

- Output is produced left to right, and **earlier tokens cannot be revised**. If the model commits to a wrong first step, the rest is built on it. This is one reason "think before you answer" helps (see the topic "Reasoning models and extended thinking" below).
- **Output tokens are slower and more expensive than input tokens**, because input is processed in parallel but output is produced one token at a time. Providers price output several times higher than input.
- The model is trained on text, so it continues text in the style of what it has seen. Clear, structured input produces clear, structured output.

**Sampling and temperature.** Picking the next token from the probabilities is called **sampling**. Picking the single most likely token every time is **greedy decoding**. Picking at random in proportion to the probabilities gives variety. **Temperature** reshapes the distribution before sampling: below 1 sharpens it towards the top choice, above 1 flattens it. **Top-p** (nucleus sampling) keeps only the smallest set of top tokens whose probabilities add up to p. This toy example runs the real arithmetic on four candidate tokens:

```python
import math, random

def softmax(logits, temperature=1.0):
    scaled = [x / temperature for x in logits]
    m = max(scaled)                        # subtract the max for numerical stability
    exps = [math.exp(s - m) for s in scaled]
    total = sum(exps)
    return [e / total for e in exps]

vocab = ["table", "partition", "dataset", "banana"]
logits = [2.0, 1.5, 1.0, -1.0]   # the model's raw score for each candidate next token

for t in (0.2, 1.0, 2.0):
    probs = softmax(logits, t)
    print(f"T={t}:", " ".join(f"{w}={p:.2f}" for w, p in zip(vocab, probs)))
# T=0.2: table=0.92 partition=0.08 dataset=0.01 banana=0.00
# T=1.0: table=0.49 partition=0.30 dataset=0.18 banana=0.02
# T=2.0: table=0.38 partition=0.30 dataset=0.23 banana=0.09

def top_p(vocab, probs, p=0.9):
    """Keep the smallest set of most likely tokens whose probabilities add up to p."""
    ranked = sorted(zip(vocab, probs), key=lambda pair: -pair[1])
    kept, total = [], 0.0
    for word, prob in ranked:
        kept.append(word)
        total += prob
        if total >= p:
            break
    return kept

print(top_p(vocab, softmax(logits, 1.0), p=0.9))   # ['table', 'partition', 'dataset']

rng = random.Random(42)
def sample(temperature):
    return rng.choices(vocab, weights=softmax(logits, temperature))[0]

print([sample(0.2) for _ in range(8)])   # eight times 'table'
print([sample(2.0) for _ in range(8)])   # a mix of 'partition' and 'table'
```

At low temperature the output is nearly deterministic; at high temperature "banana" starts to get a real chance. For agents, which must emit exact tool names and valid JSON, low variety is usually what you want.

> **Pitfall:** Assuming temperature 0 makes a model deterministic. Hosted models run on batched GPU hardware where tiny floating-point differences change which token wins a near tie, so the same request can still produce different outputs. Design agents to tolerate variation (validate outputs, make tools idempotent) rather than relying on exact repeatability.

> **Tip:** Some newer models, including current Claude models, no longer accept `temperature` or `top_p` at all; you steer them with the prompt and an effort setting instead. Check the model's documentation before porting old code that sets sampling parameters.

**Why models hallucinate.** A **hallucination** is fluent, confident output that is false: a column that does not exist, an invented API parameter, a made-up citation. It follows from how the model works. Training rewards producing plausible continuations, and evaluation benchmarks have historically rewarded guessing over saying "I don't know", so a model fills gaps with the most plausible-sounding text. Facts that were rare in the training data, very recent, or specific to your company (your table names, your runbooks) are exactly where gaps are. The fixes are architectural, not magical:

| Cause | What you do about it |
|---|---|
| The fact is not in the model at all (your schema, today's data) | Give it the fact: retrieval, tools that query the source ([Context engineering, memory and RAG](#memory)) |
| The model guesses instead of abstaining | Tell it explicitly that "I don't know" is acceptable, and reward that in evaluation |
| A long context buries the relevant fact | Put key facts near the start or end, keep context lean |
| The output is not checked | Validate: run the SQL, check the column exists, ask for citations and verify them |

Agents make hallucination both better and worse. Better, because a model with a `describe_table` tool can look up the schema instead of guessing it. Worse, because a hallucinated fact can now trigger an action.

::: deeper
**The transformer in one paragraph.** Current LLMs are transformers (Vaswani et al., 2017). Each token becomes a vector; a stack of layers repeatedly mixes information between positions using **attention**, where each token computes how much to draw from every earlier token. The last layer produces the logits. Attention compares every token with every other, so cost grows with the square of the sequence length; providers use caching of intermediate attention state (the **KV cache**) so that a long, unchanged prefix does not have to be recomputed. That is the mechanism behind **prompt caching**, which makes a stable system prompt and tool list much cheaper on repeated calls ([Production engineering, security and cost](#production)).

**Pre-training, then post-training.** A base model is pre-trained to predict the next token on a very large text corpus. It is then post-trained (supervised fine-tuning on examples, then reinforcement learning from human or AI feedback, and for reasoning models reinforcement learning on verifiable tasks such as maths and code) to follow instructions, use tools and refuse harmful requests. Tool calling and the chat format exist because of post-training; the base model knows nothing about "roles".

**Long context is not perfect recall.** Models retrieve facts from the start and end of a long context more reliably than from the middle ("Lost in the Middle", Liu et al., 2023). Newer models have improved a lot, but quality still drops as context fills with irrelevant material, a problem now called **context rot**. For agents, the practical rule is: the context window is a budget, not a bucket. Anthropic's "Effective context engineering for AI agents" is the reference for managing it.

**Knowledge cutoff.** A model knows nothing after its training data was collected. Library APIs, product names and prices change; the model will confidently use the old ones. That is one reason this course insists on checking framework code against installed packages.

**Why hallucination is structural.** Kalai and colleagues (2025) argue that hallucinations arise naturally from next-token training and persist because most benchmarks score a confident wrong answer the same as "I don't know", which teaches guessing. The implication for builders: your own evaluations should reward abstention when evidence is missing.

> **Interview:** "Why do LLMs hallucinate, and how do you reduce it in an agent?" The model generates the most plausible continuation, not a looked-up fact, and training plus benchmarks reward guessing over abstaining, so gaps get filled with plausible text. In an agent I reduce it by grounding (tools and retrieval that fetch the real schema or document), by explicitly allowing "I don't know", by keeping context focused, and by verifying outputs before acting: running the SQL in a dry run, checking cited sources, and validating against a schema.

> **Interview:** "What happens when the conversation exceeds the context window?" The request fails or older content must be removed. The model has no memory of its own, so the application decides what to keep: trimming old tool results, summarising (compaction), or moving facts to external memory and retrieving them when needed.
:::

::: check
?? Why are output tokens more expensive and slower than input tokens?
Input tokens are processed in parallel in one pass, but output tokens are generated one at a time, each needing its own pass through the model.

?? What does lowering the temperature do to the next-token distribution?
It sharpens it: the most likely token gets an even larger share of the probability, so output becomes more predictable and less varied.

?? A model invents a column name in a SQL query. Name two architectural fixes.
Give it a tool that returns the real schema (grounding), and validate the query before running it (a dry run or a check that every column exists), feeding any error back so it can correct itself.
:::

::: resources
- Video: [Deep Dive into LLMs like ChatGPT](https://www.youtube.com/watch?v=7xTGNNLPyMI) · Andrej Karpathy's three-hour walk through tokens, pre-training, post-training and hallucination
- Paper: [Attention Is All You Need](https://arxiv.org/abs/1706.03762) · the transformer paper
- Paper: [The Curious Case of Neural Text Degeneration](https://arxiv.org/abs/1904.09751) · introduces nucleus (top-p) sampling
- Paper: [Lost in the Middle: How Language Models Use Long Contexts](https://arxiv.org/abs/2307.03172) · why position in a long context matters
- Paper: [Why Language Models Hallucinate](https://arxiv.org/abs/2509.04664) · the statistical argument and the role of benchmark scoring
- Blog: [Effective context engineering for AI agents](https://www.anthropic.com/research/effective-context-engineering-for-ai-agents) · context as a finite budget
:::

### Working with model APIs

::: plain
Calling a model API is like sending a letter to a very fast, very forgetful consultant. Every letter must contain the whole story so far, because the consultant remembers nothing from your last letter. You also attach a standing brief (the system prompt) that says who they are and how to behave. The reply comes back with a note saying why they stopped writing: they finished, they ran out of paper, or they need you to fetch something for them.
:::

All the major providers expose the same basic shape: you send a list of **messages**, and you get back one new message plus metadata. Learn it once and the differences between providers are mostly naming.

[[fig:foundations_api]]

**Messages and roles.** A conversation is a list of messages, each with a **role**:

- `user`: input from the person, or from your program acting for them. In tool use, tool results also travel in a `user` message.
- `assistant`: what the model said earlier, including any tool calls it made. You send these back so the model sees its own previous turns.
- `system`: standing instructions that apply to the whole conversation. In the Claude Messages API this is a separate top-level `system` parameter; in the OpenAI-compatible Chat Completions API (which vLLM and Ollama also serve) it is a message with role `system`; in OpenAI's Responses API it is `instructions`.

A message's `content` is either a plain string or a list of **content blocks**: `text`, `image`, `document`, and for agents `tool_use` (the model asking for a tool) and `tool_result` (your answer). Because the API is **stateless**, you append the assistant's reply and the next user message to your list and send the whole list again on the next call. The conversation lives in your program, exactly like a pipeline that carries its state in a table rather than in the worker.

Here is the official Python SDK shape for Claude (`pip install anthropic`):

```python
import anthropic

client = anthropic.Anthropic()          # reads ANTHROPIC_API_KEY from the environment
MODEL = "claude-opus-5-5"               # use a current model ID from the models overview page

response = client.messages.create(
    model=MODEL,
    max_tokens=1024,                    # hard cap on the length of this reply
    system="You are a concise assistant for a data platform team.",
    messages=[{"role": "user", "content": "Explain partition pruning in Trino in two sentences."}],
)
print(response.stop_reason)             # end_turn
text = next(b.text for b in response.content if b.type == "text")
print(text)
print(response.usage.input_tokens, response.usage.output_tokens)
```

Note `next(b.text for b in response.content if b.type == "text")` rather than `response.content[0].text`: a response can contain several blocks (thinking, text, tool calls), and the text is not guaranteed to be first.

On-prem, the model usually runs on your own servers. Open-weight models (Llama, Qwen, Mistral, Gemma and others) are served with **vLLM** or **Ollama**, and both expose an OpenAI-compatible API, so the `openai` SDK (`pip install openai`) talks to them with nothing more than a different base URL:

```python
from openai import OpenAI

# vLLM (`vllm serve Qwen/Qwen3-8B`) listens on port 8000; Ollama on http://localhost:11434/v1
local = OpenAI(base_url="http://localhost:8000/v1",
               api_key="not-needed")               # a local server without --api-key accepts any key
LOCAL_MODEL = "Qwen/Qwen3-8B"                     # the model the server was started with

resp = local.chat.completions.create(
    model=LOCAL_MODEL,
    max_tokens=1024,
    messages=[{"role": "system", "content": "You are a concise assistant for a data platform team."},
              {"role": "user", "content": "Explain partition pruning in Trino in two sentences."}],
)
print(resp.choices[0].finish_reason, resp.usage.prompt_tokens)
print(resp.choices[0].message.content)
```

The shape is the same as before: a list of messages in, one message plus a finish reason and token usage out. Code written against this API moves between a laptop running Ollama, a GPU server running vLLM and a hosted provider by changing configuration only. On Google Cloud, Gemini and partner models such as Claude are served by Gemini Enterprise Agent Platform (formerly Vertex AI) through the `google-genai` SDK, with Google Cloud credentials instead of an API key.

**System prompts.** The system prompt sets the role, the rules, the output format and the background the model needs for every turn. It is the most stable part of a request, so it is also where prompt caching pays off. Keep task-specific data (today's log, the user's question) in user messages, and standing policy in the system prompt.

**Stop reasons.** Every response says why generation stopped. Your code must branch on it; ignoring it is the most common bug in hand-written agents.

| Claude `stop_reason` | Meaning | What your code does |
|---|---|---|
| `end_turn` | The model finished its answer | Use the answer |
| `tool_use` | The model wants one or more tools run | Run them, send `tool_result` blocks, call again |
| `max_tokens` | Hit your `max_tokens` cap mid-answer | Treat as incomplete: raise the cap, continue, or fail |
| `stop_sequence` | Hit one of your custom stop strings | Use the text up to that point |
| `pause_turn` | A long server-side tool turn was paused | Send the conversation back to let it continue |
| `refusal` | The model declined for safety reasons | Do not retry blindly; log and surface it |
| `model_context_window_exceeded` | The conversation filled the context window | Trim or summarise history, then continue |

OpenAI-compatible servers such as vLLM and Ollama report the equivalent in `finish_reason` (`stop`, `length`, `tool_calls`), and tool calls appear in the message's `tool_calls` list. This mock-based example shows the handling pattern; the response is a plain dictionary in the same shape the SDK returns:

```python
def mock_create(messages, max_tokens):
    """Stands in for client.messages.create: returns the provider's response shape."""
    return {
        "id": "msg_mock_01",
        "role": "assistant",
        "content": [{"type": "text", "text": "Row counts dropped 40% because the upstream file was empty."}],
        "stop_reason": "max_tokens" if max_tokens < 20 else "end_turn",
        "usage": {"input_tokens": 1830, "output_tokens": 17},
    }

def handle(response):
    match response["stop_reason"]:
        case "end_turn":
            return "".join(b["text"] for b in response["content"] if b["type"] == "text")
        case "max_tokens":
            raise RuntimeError("answer was cut off: raise max_tokens or ask for less")
        case "tool_use":
            raise NotImplementedError("run the requested tools (next section)")
        case "refusal":
            raise RuntimeError("the model declined; do not retry blindly")
        case other:
            raise RuntimeError(f"unhandled stop_reason {other!r}")

messages = [{"role": "user", "content": "Why did the orders load drop yesterday?"}]
resp = mock_create(messages, max_tokens=1024)
print(handle(resp))        # Row counts dropped 40% because the upstream file was empty.
print(resp["usage"])       # {'input_tokens': 1830, 'output_tokens': 17}

messages.append({"role": "assistant", "content": resp["content"]})   # keep the history
messages.append({"role": "user", "content": "Which file?"})
print([m["role"] for m in messages])   # ['user', 'assistant', 'user']
try:
    handle(mock_create(messages, max_tokens=10))
except RuntimeError as e:
    print(e)               # answer was cut off: raise max_tokens or ask for less
```

**Streaming.** Without streaming you wait for the whole answer. With streaming the server sends **server-sent events** as tokens are produced, so a user sees text after a fraction of a second, and long generations do not hit HTTP timeouts. The SDK hides the event plumbing:

```python
import anthropic

client = anthropic.Anthropic()
with client.messages.stream(
    model="claude-opus-5-5",
    max_tokens=4096,
    messages=[{"role": "user", "content": "Write a runbook for a failed Dataflow job."}],
) as stream:
    for text in stream.text_stream:          # text deltas as they arrive
        print(text, end="", flush=True)
    final = stream.get_final_message()       # the complete message, same shape as create()
print("\n", final.stop_reason, final.usage.output_tokens)
```

For agents, streaming matters less for the user and more for robustness: long tool-heavy turns run for minutes, and streaming keeps the connection alive. Use `get_final_message()` when you only need the result.

**Token counting.** Before sending a large request, ask the provider how many input tokens it will use. It costs nothing and uses the exact tokenizer of the model you name:

```python
count = client.messages.count_tokens(
    model="claude-opus-5-5",
    system="You are a concise assistant.",
    messages=[{"role": "user", "content": open("big_schema.sql").read()}],
)
print(count.input_tokens)
```

After each call, `response.usage` reports the real input and output tokens (and cache reads and writes). Log it on every call: it is your cost data, the same way you would log rows and bytes read for every Spark or Trino query.

**Rate limits.** Providers limit how fast you can call them, typically as requests per minute and tokens per minute (Anthropic limits input and output tokens per minute separately, per model and per usage tier). A self-hosted vLLM or Ollama server has no quota but a fixed capacity: extra requests queue, latency climbs and timeouts follow, so you cap concurrency on the client. Exceeding a limit returns HTTP **429**, often with a `retry-after` header; an overloaded service may return 529 or 503. The official SDKs already retry 429 and 5xx errors a couple of times with exponential backoff. When you need more control, the logic is the same as for any flaky upstream:

```python
import random

class RateLimitError(Exception):
    def __init__(self, retry_after=None):
        super().__init__("429 rate limited")
        self.retry_after = retry_after          # seconds, from the retry-after header

def call_with_backoff(fn, max_attempts=5, base=1.0, cap=30.0, rng=random.Random(0), sleep=None):
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except RateLimitError as err:
            if attempt == max_attempts:
                raise
            if err.retry_after is not None:
                delay = err.retry_after                       # the server knows best
            else:
                delay = min(cap, base * 2 ** (attempt - 1))   # 1, 2, 4, 8 ... seconds
                delay = rng.uniform(0, delay)                 # "full jitter"
            print(f"attempt {attempt} rate limited, waiting {delay:.2f}s")
            if sleep:
                sleep(delay)                                  # pass time.sleep in real code

# A fake endpoint that is rate limited twice, then succeeds.
responses = iter([RateLimitError(), RateLimitError(retry_after=3), {"stop_reason": "end_turn"}])
def fake_call():
    r = next(responses)
    if isinstance(r, Exception):
        raise r
    return r

print(call_with_backoff(fake_call))
# attempt 1 rate limited, waiting 0.84s
# attempt 2 rate limited, waiting 3.00s
# {'stop_reason': 'end_turn'}
```

> **Pitfall:** Retrying 400-class errors other than 429. A 400 (bad request) or 401 (bad key) will fail the same way every time; retrying only burns quota. Retry 429, 5xx and network errors; fail fast on the rest.

**Choosing a model.** Providers offer families in tiers: a largest, most capable model; a balanced mid tier; and a small, fast, cheap tier. Choose per step, not per product:

| Need | Usually choose | Why |
|---|---|---|
| Hard multi-step reasoning, the agent's main loop, coding | Largest or mid tier, thinking on | Fewer wrong turns saves more than the price difference |
| High-volume classification, extraction, routing | Small tier | Cheap, fast, good enough when the task is narrow |
| Long documents | Any tier with a large enough context window | Check the window and the per-token price |
| Data must stay on your network, or an air-gapped site | Open-weight models (Llama, Qwen, Mistral, Gemma) served with vLLM or Ollama on your own GPUs | No data leaves the network; you own capacity, upgrades and evaluation |

Decide with an evaluation on your own examples ([Evaluation and observability](#eval)), not with public leaderboards. Read model IDs from the provider's models page or Models API rather than from memory: they change several times a year.

> **Tip:** Put the model ID in configuration, not in code. You will change it more often than you think: new releases, cost tuning, fallbacks during an outage.

::: deeper
**Statelessness has consequences.** Because you resend the whole history, each turn costs the full conversation's input tokens again. A 30-step agent whose history grows to 50,000 tokens pays for roughly 30 × 25,000 = 750,000 input tokens on average, not 50,000. That is why **prompt caching** (the provider stores the processed prefix and charges a fraction of the price to reuse it) is the single biggest cost lever for agents: keep the system prompt and tool definitions byte-for-byte stable at the front of the request, and append new content at the end. Some providers now also offer server-side conversation state, but you should still keep your own copy of the history for replay, audit and debugging.

**Streaming events.** Under the SDK, a Claude stream is a sequence of events: `message_start`, then for each content block `content_block_start`, many `content_block_delta` events (text deltas, or partial JSON for tool inputs), `content_block_stop`, then `message_delta` (carrying the stop reason and final usage) and `message_stop`. Tool-call arguments arrive as partial JSON fragments; never act on a tool call until its block is complete.

**Latency has two parts**: time to first token (dominated by input length and queueing) and output speed (tokens per second). Long inputs hurt the first; long outputs hurt the second. Caching shortens the first; asking for less output, or a smaller model, shortens the second.

**Batch APIs.** For offline jobs such as classifying a million support tickets or documenting every table in a dataset, providers offer batch endpoints at about half price with results within hours. To a data engineer this is the natural fit: it looks like any other batch job ([Agents for data engineering](#dataagents)).

**Errors to expect in production**: 429 (rate limited), 529 or 503 (overloaded), 500 (server error), timeouts on very long non-streaming requests, `max_tokens` truncation of JSON or tool input, and `refusal`. Log the provider's request ID with every failure; support teams ask for it.

> **Interview:** "Walk me through what happens when your application calls an LLM API." I build the request: model ID, a stable system prompt, tool definitions, the full message history and a `max_tokens` cap. The SDK sends it with retries for 429 and 5xx. The response contains content blocks, a stop reason and token usage. I branch on the stop reason: use the answer on `end_turn`, run tools and loop on `tool_use`, treat `max_tokens` as incomplete, and surface `refusal`. I log usage and request IDs for cost and debugging, and I append the assistant message to the history because the API keeps no state for me.

> **Interview:** "How do you handle rate limits in a high-volume pipeline?" Client-side concurrency limits sized below the quota, retries with exponential backoff and jitter that respect `retry-after`, prompt caching to cut input tokens, the batch API for anything not latency-sensitive, and a fallback model or region for overload. I monitor tokens per minute against the limit the way I would monitor cluster utilisation.
:::

::: check
?? Why must you send the whole conversation on every call?
The API is stateless: the model remembers nothing between calls, so the history it should consider must be in the request each time.

?? A response comes back with `stop_reason` equal to `max_tokens` while you expected JSON. What happened, and what do you do?
The output hit the token cap and was cut off, so the JSON is probably incomplete. Do not parse it as final: raise `max_tokens`, ask for shorter output, or treat the call as failed and retry with adjusted settings.

?? Which errors should a retry wrapper retry, and which should it not?
Retry 429 (rate limits), 5xx and overload errors, and network failures, with backoff. Do not retry other 4xx errors such as 400 or 401: they will fail the same way again.
:::

::: resources
- Docs: [Messages API reference](https://platform.claude.com/docs/en/api/messages) · request and response fields, content blocks, stop reasons
- Docs: [Streaming messages](https://platform.claude.com/docs/en/build-with-claude/streaming) · event types and SDK helpers
- Docs: [Token counting](https://platform.claude.com/docs/en/build-with-claude/token-counting) · counting input tokens before a call
- Docs: [Rate limits](https://platform.claude.com/docs/en/api/rate-limits) · request and token limits, tiers, the `retry-after` header
- Docs: [OpenAI-compatible server in vLLM](https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html) · serving open-weight models behind the same API shape
- Docs: [Ollama OpenAI compatibility](https://docs.ollama.com/api/openai-compatibility) · calling local Ollama models with the `openai` SDK
- Docs: [Models overview](https://platform.claude.com/docs/en/models/overview) · current model IDs, context windows and prices
:::

### Prompting for agents

::: plain
A prompt is a work order. A vague order ("look into the pipeline") gets you a vague result; a good one says what the goal is, what the worker has to go on, what the rules are, and what the finished product should look like. An agent reads its prompt many times, once per step, so every ambiguity in it is multiplied. Treat prompts like code: structured, reviewed, tested and versioned.
:::

**Prompt engineering** is writing the instructions and context a model receives so that it behaves the way you need. For agents it has four jobs: describe the goal and the definition of done, describe the tools and when to use each, set the rules (what never to do, when to stop, when to ask a human), and fix the output format. The techniques below are the ones that move results in practice.

**1. Be clear and direct.** Write as you would brief a capable new colleague who knows nothing about your company. State the goal, the audience and the constraints. Explain *why* a rule exists: "Never run `DELETE`, because this account has write access to production" works better than "NEVER DELETE", because the model can generalise from the reason to cases you did not list. Current models follow instructions literally and precisely; if you want thoroughness, ask for it.

**2. Give examples (few-shot prompting).** One to three worked examples of input and ideal output teach format and judgement faster than paragraphs of rules. Make them varied, so the model does not copy surface details, and make sure they agree with your rules.

**3. Structure with XML tags.** Separate instructions from data with tags such as `<task>`, `<rules>`, `<log>`, `<schema>`. Tags make it clear where pasted data starts and ends (which also reduces the chance that text *inside* the data is taken as an instruction), and you can refer to them by name: "using only the evidence in `<log>`". Claude is trained to pay attention to such tags, and the technique works with other models too. Markdown headings serve a similar purpose; pick one style and stay consistent.

**4. Say what to do, not only what to avoid.** "Answer in plain sentences" works better than "do not use markdown". Give a positive target.

**5. Ask for the format you will parse.** If code reads the output, use structured outputs (see "Structured outputs and JSON schemas") rather than hoping the prose has the right shape.

Here is a prompt for a pipeline-triage step, built as a versioned template. It uses tags, one example, explicit rules with an escape hatch ("insufficient evidence"), and a fixed output format:

```python
import hashlib
from dataclasses import dataclass
from string import Template

@dataclass(frozen=True)
class Prompt:
    name: str
    version: str
    template: str

    @property
    def sha(self):
        return hashlib.sha256(self.template.encode()).hexdigest()[:8]

    def render(self, **values):
        return Template(self.template).substitute(**values)   # KeyError if a value is missing

TRIAGE_V2 = Prompt("pipeline_triage", "2.1.0", """\
You are an on-call assistant for the data platform team.

<task>
Find the most likely cause of the failed run and say how confident you are.
</task>

<rules>
- Use only the evidence inside <log> and <recent_changes>. If it is not enough, say "insufficient evidence".
- Never suggest deleting data.
- Answer in at most five sentences, then a line "confidence: low|medium|high".
</rules>

<example>
<log>KeyError: 'customer_id' in transform_orders</log>
<recent_changes>PR 812 renamed customer_id to cust_id in the source view</recent_changes>
<answer>The transform reads customer_id, which PR 812 renamed to cust_id in the source view.
confidence: high</answer>
</example>

<log>
$log
</log>
<recent_changes>
$changes
</recent_changes>""")

text = TRIAGE_V2.render(log="TimeoutError: Spark job exceeded 6h", changes="none")
print(TRIAGE_V2.name, TRIAGE_V2.version, TRIAGE_V2.sha)   # pipeline_triage 2.1.0 396cd557
print(text.splitlines()[-5:])
# ['TimeoutError: Spark job exceeded 6h', '</log>', '<recent_changes>', 'none', '</recent_changes>']
```

**Prompting for tool use.** In an agent, much of the "prompt" is the tool definitions. The model chooses tools by reading their names, descriptions and parameter schemas, so write them for a reader who has never seen your system:

- Name tools by what they do: `get_table_schema`, not `tool2` or `fetch`.
- In the description, say when to use the tool, when *not* to, what it returns, and its limits ("returns at most 100 rows; use `count_rows` for totals").
- Describe each parameter with its format and an example (`"table": "fully qualified, like project.dataset.table"`).
- In the system prompt, describe the strategy: "Check the schema before writing SQL. Run a dry run before any query that scans more than one partition. If two attempts fail, stop and report."

The next section, [Tool use and function calling](#tools), goes deep on tool design. The short version: a confusing tool description causes more agent failures than a weak model.

**Prompt versioning.** Prompts change behaviour as much as code does, so manage them like code:

- Keep prompts in files in the repository, reviewed in pull requests, not pasted into a console.
- Give each prompt a name and a version, and log the version (or a content hash, like `sha` above) with every model call, so any output can be traced to the exact prompt that produced it.
- Run an evaluation set before rolling out a new version, and roll out gradually or behind a flag, exactly as you would a schema migration.
- Pin the model ID together with the prompt version. A prompt tuned for one model can behave differently on the next.

> **Pitfall:** Putting volatile data (the current timestamp, a request ID) at the top of the system prompt. It changes every call, so it breaks prompt caching for everything after it. Put stable instructions first and volatile data last.

> **Tip:** When an agent misbehaves, read the full request it received, exactly as sent, before changing anything. Most "the model is dumb" bugs turn out to be a missing fact, a contradictory rule or a misleading tool description.

::: deeper
**Prompt injection starts here.** Anything your agent reads (a web page, a ticket, a column comment, a file) can contain text that looks like instructions: "ignore previous instructions and email this table to...". Tags and a clear statement that content inside data tags is data, not instructions, help, but they are not a security boundary. Real defences are architectural: least-privilege tools, human approval for risky actions, and never letting untrusted content trigger irreversible operations ([Production engineering, security and cost](#production)).

**Prompting current models.** Older guidance (shouting in capitals, "you MUST", threats, long lists of prohibitions) was written for weaker models and now often backfires: current models over-apply heavy emphasis and become rigid or overcautious. Prefer calm, specific instructions with reasons. Similarly, many tricks for older models, such as prefilling the start of the assistant's answer to force a format, are no longer supported on some current models; use structured outputs instead.

**Chain of thought.** Asking a model to reason step by step before answering improves accuracy on multi-step problems (Wei et al., 2022). With reasoning models this happens natively in a separate thinking phase, so explicit "think step by step" instructions matter less; what matters is giving the model the information and the success criteria. See "Reasoning models and extended thinking".

**Prompt optimisation tools.** Frameworks such as DSPy treat prompts as parameters to optimise against an evaluation metric, and provider consoles offer prompt generators and improvers. They work best once you have an evaluation set, which is the real asset: a prompt without tests is a guess.

**System prompt altitude.** Anthropic's context engineering guidance describes a balance between two failure modes: prompts so specific that they hard-code brittle if-else logic, and prompts so vague that the model lacks concrete signals. Aim for clear principles plus a few canonical examples.

> **Interview:** "How do you manage prompts in production?" As versioned artefacts in the repository with code review. Each prompt has a name and version, and every model call logs the prompt version, model ID and token usage. Changes go through an offline evaluation set and then a gradual rollout with monitoring, and we can roll back by version. Stable parts come first for caching, and volatile data goes last.

> **Interview:** "Your agent keeps choosing the wrong tool. What do you check first?" The tool names and descriptions, because that is what the model reads to choose. I look for overlapping tools, vague descriptions, missing "use this when" guidance and unclear parameter formats, then I add a short strategy section to the system prompt and test on the failing cases.
:::

::: check
?? Why are XML tags useful in prompts?
They separate instructions from pasted data and from each other, make the boundaries unambiguous, and let you refer to sections by name ("using only the evidence in `<log>`").

?? Why should every model call log the prompt version?
So any output can be traced to the exact prompt (and model) that produced it, which you need for debugging, evaluation comparisons and rollbacks.

?? Why does explaining the reason behind a rule help?
The model can generalise from the reason to situations the rule does not list, instead of applying the literal words rigidly.
:::

::: resources
- Docs: [Prompt engineering overview](https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/overview) · Anthropic's guide, including clarity, examples and XML tags
- Docs: [Prompt Engineering Guide](https://www.promptingguide.ai/) · DAIR.AI's open, vendor-neutral guide to prompting techniques, with notes for open-weight models
- Blog: [Writing effective tools for agents](https://www.anthropic.com/research/writing-tools-for-agents) · how tool names and descriptions steer agents
- Paper: [The Prompt Report: A Systematic Survey of Prompting Techniques](https://arxiv.org/abs/2406.06608) · a catalogue of prompting techniques and terms
- Paper: [Chain-of-Thought Prompting Elicits Reasoning in Large Language Models](https://arxiv.org/abs/2201.11903) · the original step-by-step reasoning result
:::

## The agent loop

Now the core of the course. An agent is a loop: send the history to the model, let it either answer or ask for tools, run the tools, append the results, repeat. This group builds that loop from scratch, then adds the pieces that make it dependable: structured outputs so code can trust what the model returns, reasoning (thinking) for the hard steps, and the judgement to know when a plain workflow is the better design.

If you have written an Airflow sensor or a Kafka consumer, the shape will feel familiar: a loop with a termination condition, a budget, state carried between iterations, and careful handling of the unhappy paths. The difference is that the "business logic" deciding what to do next is a model call.

### The agent loop: observe, think, act

::: plain
Picture an on-call engineer working through an incident. They look at what they know (observe), decide what to check next (think), run a command (act), look at the output, and repeat until they find the cause or decide to escalate. An agent loop is exactly that, with the model doing the thinking and your code doing the looking and the acting. Your code also plays the role of the team lead who says "you have an hour and ten commands, then you report back".
:::

[[fig:foundations_loop]]

Every agent, whatever framework wraps it, runs this loop:

1. **Observe**: build the request from the goal, the history so far and the latest tool results.
2. **Think**: call the model. It returns either a final answer or one or more tool calls.
3. **Act**: if there are tool calls, run them, and append the results to the history.
4. **Check the stop conditions**, then go back to step 1.

Here is the whole thing in plain Python, with two tools and a mock model. The mock is a small rule-based function that reads the conversation and replies in the Claude Messages API shape, so the loop is exactly what you would run against the real API. The scenario: the `analytics.orders` table loaded zero rows today.

```python
import json

# ---- 1. Tools: plain functions, plus the descriptions the model reads ----
TABLE_STATS = {"analytics.orders": {"rows_loaded": 0, "source": "s3://raw/orders/2026-11-23.csv"}}
FILE_SIZES = {"s3://raw/orders/2026-11-23.csv": 0}

def get_table_stats(table):
    return TABLE_STATS[table]                      # KeyError for unknown tables

def get_file_size(uri):
    return {"uri": uri, "bytes": FILE_SIZES[uri]}

TOOLS = {"get_table_stats": get_table_stats, "get_file_size": get_file_size}
TOOL_SPECS = [
    {"name": "get_table_stats",
     "description": "Row count and source file of the last load of a table.",
     "input_schema": {"type": "object", "properties": {"table": {"type": "string"}},
                      "required": ["table"]}},
    {"name": "get_file_size",
     "description": "Size in bytes of a file in object storage (MinIO or S3).",
     "input_schema": {"type": "object", "properties": {"uri": {"type": "string"}},
                      "required": ["uri"]}},
]

# ---- 2. A mock model: reads the conversation, replies in the Messages API shape ----
def mock_model(system, messages, tools):
    results = [json.loads(b["content"]) for m in messages if m["role"] == "user"
               and isinstance(m["content"], list) for b in m["content"]
               if b["type"] == "tool_result" and not b.get("is_error")]
    def reply(content, stop):
        return {"role": "assistant", "content": content, "stop_reason": stop,
                "usage": {"input_tokens": 400 + 150 * len(messages), "output_tokens": 60}}
    if not results:                                   # nothing observed yet: look at the table
        return reply([{"type": "text", "text": "Checking the last load."},
                      {"type": "tool_use", "id": "tu_1", "name": "get_table_stats",
                       "input": {"table": "analytics.orders"}}], "tool_use")
    last = results[-1]
    if "rows_loaded" in last and last["rows_loaded"] == 0:   # zero rows: inspect the source file
        return reply([{"type": "tool_use", "id": "tu_2", "name": "get_file_size",
                       "input": {"uri": last["source"]}}], "tool_use")
    return reply([{"type": "text", "text":
                   f"The load read {last['uri']}, which is {last['bytes']} bytes. "
                   "The upstream file was empty; ask the source team to re-deliver, then re-run."}],
                 "end_turn")

# ---- 3. The agent loop ----
def run_agent(goal, model, tools, tool_specs, system="", max_steps=8, token_budget=20_000):
    messages = [{"role": "user", "content": goal}]
    state = {"steps": 0, "tokens": 0, "tool_calls": []}
    while True:
        if state["steps"] >= max_steps:
            return "stopped: step limit reached", state
        response = model(system, messages, tool_specs)              # think
        state["steps"] += 1
        state["tokens"] += sum(response["usage"].values())
        messages.append({"role": "assistant", "content": response["content"]})

        if response["stop_reason"] == "end_turn":                   # done
            text = "".join(b["text"] for b in response["content"] if b["type"] == "text")
            return text, state
        if response["stop_reason"] != "tool_use":                    # max_tokens, refusal...
            return f"stopped: {response['stop_reason']}", state

        results = []
        for block in response["content"]:                           # act
            if block["type"] != "tool_use":
                continue
            state["tool_calls"].append(block["name"])
            try:
                output = tools[block["name"]](**block["input"])
                results.append({"type": "tool_result", "tool_use_id": block["id"],
                                "content": json.dumps(output)})
            except Exception as exc:                                 # errors go back to the model
                results.append({"type": "tool_result", "tool_use_id": block["id"],
                                "content": f"{type(exc).__name__}: {exc}", "is_error": True})
        messages.append({"role": "user", "content": results})       # observe
        if state["tokens"] > token_budget:
            return "stopped: token budget exhausted", state

answer, state = run_agent("Why did analytics.orders load 0 rows today?",
                          mock_model, TOOLS, TOOL_SPECS)
print(answer)
# The load read s3://raw/orders/2026-11-23.csv, which is 0 bytes. The upstream file was empty; ask the source team to re-deliver, then re-run.
print(state)
# {'steps': 3, 'tokens': 2730, 'tool_calls': ['get_table_stats', 'get_file_size']}
```

Read the loop slowly; every line is there for a reason:

- **The assistant message is appended whole**, tool calls included. The next `tool_result` must refer to a `tool_use` id the model can see in the history.
- **All tool results from one turn go back in one `user` message.** A model can ask for several tools at once (parallel tool calls); answer them all together.
- **Tool errors are returned, not raised.** A `KeyError` becomes a `tool_result` with `is_error: True`, so the model can read it and correct itself (for example, by fixing a misspelt table name). Crashing the loop on the first bad argument throws that ability away.
- **The `state` dictionary** is the loop's own bookkeeping: steps, tokens, the tools used. In production it also holds a run ID, timings and cost, and it is what you log.

**Stop conditions.** A loop without a reliable exit is an incident waiting to happen. Use several, and check all of them:

| Stop condition | Why you need it |
|---|---|
| The model answers (`end_turn`) | The normal, successful exit |
| Maximum steps | Stops a model that keeps calling tools without converging |
| Token or cost budget | Bounds spend per run, independent of step count |
| Wall-clock timeout | Bounds latency for a user or a scheduler slot |
| Unexpected stop reason (`max_tokens`, `refusal`) | The turn is incomplete or declined: stop and report |
| Repeated identical calls or repeated errors | Detects a stuck loop early |
| A "done" or "escalate" tool called by the model | An explicit, structured way for the model to finish |

Here is the step limit catching a model that keeps retrying a misspelt table name (continuing the example above):

```python
def stubborn_model(system, messages, tools):
    return {"role": "assistant", "stop_reason": "tool_use",
            "usage": {"input_tokens": 500, "output_tokens": 30},
            "content": [{"type": "tool_use", "id": f"tu_{len(messages)}",
                         "name": "get_table_stats", "input": {"table": "analytics.order"}}]}

answer, state = run_agent("Why did the load fail?", stubborn_model, TOOLS, TOOL_SPECS, max_steps=3)
print(answer)                  # stopped: step limit reached
print(state["tool_calls"])     # ['get_table_stats', 'get_table_stats', 'get_table_stats']
```

**State.** There are two kinds of state in an agent. The **conversation** (the `messages` list) is what the model sees; it grows every step and is bounded by the context window. The **run state** (`state` above, plus anything your tools changed in the world) is what your program tracks. Keep them separate: the model does not need to see your token counter, and your program should not have to parse the conversation to find out how many steps ran. For long or resumable runs, persist both after every step, like a checkpoint in a streaming job, so a crash or a human approval pause can resume where it stopped.

**The same loop against the real API.** Swap the mock for the SDK and nothing structural changes:

```python
import json
import anthropic

client = anthropic.Anthropic()

def run_claude_agent(goal, tools, tool_specs, system, max_steps=8):
    messages = [{"role": "user", "content": goal}]
    for _ in range(max_steps):
        response = client.messages.create(
            model="claude-opus-5-5", max_tokens=16000,
            system=system, tools=tool_specs, messages=messages,
        )
        messages.append({"role": "assistant", "content": response.content})
        if response.stop_reason != "tool_use":
            return response                              # end_turn, max_tokens, refusal...
        results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            try:
                content, is_error = json.dumps(tools[block.name](**block.input)), False
            except Exception as exc:
                content, is_error = f"{type(exc).__name__}: {exc}", True
            results.append({"type": "tool_result", "tool_use_id": block.id,
                            "content": content, "is_error": is_error})
        messages.append({"role": "user", "content": results})
    raise RuntimeError("step limit reached")
```

The SDK also ships a **tool runner** that runs this loop for you: decorate functions with `@beta_tool` (the docstring and type hints become the tool description and schema) and call `client.beta.messages.tool_runner(...)` with `max_iterations` as the step limit. Frameworks such as LangGraph, the OpenAI Agents SDK and Google's Agent Development Kit (ADK) wrap the same loop with state, persistence and tracing ([Agent frameworks](#frameworks)). Write it by hand once so you know what they are doing.

> **Pitfall:** Dropping the assistant's `tool_use` message and sending only the tool result. The API rejects a `tool_result` whose `tool_use_id` does not match a `tool_use` in the previous assistant message. Always append the full assistant content first.

> **Tip:** Make tools **idempotent** where you can (safe to run twice with the same arguments). Agents retry, loops get resumed after crashes, and models sometimes repeat a call. The same advice you follow for pipeline tasks applies here.

::: deeper
**ReAct.** The loop above is the **ReAct** pattern (Yao et al., 2022): interleave reasoning ("thought") with actions and observations. The original paper parsed "Thought / Action / Observation" lines out of free text; modern APIs do the same thing with structured `tool_use` blocks, and reasoning models keep the thoughts in separate thinking blocks. The [Agent design patterns](#patterns) section compares ReAct with plan-and-execute and other variants.

**Where the loop runs.** In the pattern above your code runs the loop and executes the tools: a **client-side** loop. Some tools run on the provider's side instead (web search, code execution), in which case the provider runs part of the loop within a single API call and may return `pause_turn` for a long turn. Managed runtimes go further and run the whole loop and a sandbox for you, such as Anthropic's managed agents or Agent Engine on Google Cloud; on-prem you run the loop yourself as a service, usually on Kubernetes. [Deploying and serving agents](#platforms) covers the trade-offs.

**Context growth is the loop's real limit.** Each step appends the assistant turn and the tool results. Big tool outputs (a 5,000-row query result, a full log file) fill the window in a few steps and degrade quality long before the hard limit. Design tools to return compact, relevant output (top rows plus a count, the matching log lines), and when runs are long, trim or summarise old tool results. This is the subject of [Context engineering, memory and RAG](#memory).

**Error handling has three layers.** Transient API errors (429, 5xx) are retried by the client and never reach the model. Tool errors (bad argument, missing table) go back to the model as `is_error` results so it can adapt. Fatal conditions (budget exceeded, a forbidden action requested, repeated failures) stop the loop and report. Mixing these layers up, for example feeding a 429 back to the model, produces confused agents.

**Human in the loop.** For risky tools (writes, deletes, sending messages), the loop pauses before executing, persists its state, and resumes once a human approves. This is why persisting state after every step matters: approval can take hours.

**Observability.** Log one span per model call and per tool call with inputs, outputs, tokens and latency, linked by a run ID. When an agent misbehaves, the trace of the loop is the only way to see why ([Evaluation and observability](#eval)).

> **Interview:** "Write a minimal agent loop and explain the stop conditions." Keep a messages list; call the model with the system prompt, tools and messages; append the assistant message; if the stop reason is `tool_use`, run each requested tool, append all the results in one user message (errors as `is_error` results), and loop; otherwise return. Stop on a final answer, a maximum number of steps, a token or cost budget, a timeout, an unexpected stop reason, or a stuck pattern such as the same failing call repeated. Persist state each step so runs can resume after a crash or a human approval.

> **Interview:** "Why send tool errors back to the model instead of raising?" Because the model can often fix the problem itself: correct a table name, adjust a date format, try a different tool. Raising ends the run on the first recoverable mistake. Only fatal or policy errors should stop the loop.
:::

::: check
?? What are the two possible outcomes of each model call in the loop, and what does the code do for each?
Either a final answer (stop reason `end_turn`), which ends the loop, or tool calls (stop reason `tool_use`), which the code executes, appends as results, and then calls the model again.

?? Why is a maximum step count not enough on its own?
A few steps can still be very expensive or slow (huge tool outputs, long thinking), so you also need token or cost budgets and a wall-clock timeout, plus detection of stuck patterns.

?? Where do tool errors go, and why?
Back to the model as `tool_result` blocks marked `is_error`, so it can read the error and correct its next call instead of the whole run failing.
:::

::: resources
- Docs: [Tool use with Claude](https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview) · the `tool_use` and `tool_result` contract and the tool runner
- Paper: [ReAct: Synergizing Reasoning and Acting in Language Models](https://arxiv.org/abs/2210.03629) · the reason, act, observe loop
- Docs: [Tool calling in vLLM](https://docs.vllm.ai/en/latest/features/tool_calling.html) · the same loop against a self-hosted open-weight model
- Docs: [Agent Development Kit](https://google.github.io/adk-docs/) · Google's framework that wraps this loop
- Repo: [12-Factor Agents](https://github.com/humanlayer/12-factor-agents) · principles for owning your loop, state and control flow in production
:::

### Structured outputs and JSON schemas

::: plain
When a person reads the model's answer, a nicely written paragraph is fine. When a program reads it, the answer has to fit a form exactly: these fields, these types, nothing extra. Structured outputs are how you hand the model a form instead of a blank page. Think of the difference between a free-text "comments" box and a typed column in a table: the column can be loaded into PostgreSQL without anyone reading it first.
:::

Agents and pipelines constantly need machine-readable results: a severity score to route on, extracted fields to insert into a table, a decision the next step branches on. There are three levels of reliability:

| Approach | How it works | Reliability |
|---|---|---|
| Ask nicely | "Reply in JSON with fields..." in the prompt | Usually works; sometimes adds chatter, wrong types or missing fields |
| Validate and repair | Parse, validate against a schema, send errors back and retry | Reliable in the end, costs extra calls when it fails |
| Schema-constrained output | The provider restricts generation so output must match your JSON Schema | Valid JSON that matches the schema by construction |

You want the third, plus validation anyway, because constrained decoding guarantees *shape*, not *truth*: the severity will be an integer, but not necessarily the right one.

**JSON Schema** is the standard language for describing JSON shapes: types, required fields, enums, nested objects. Every provider accepts it, for tool parameters and for response formats, and **Pydantic** models generate it for you. In Python, a Pydantic model is the natural single source of truth: it is the schema you send, the validator you run on the result, and the typed object your code uses.

**From scratch first.** To see what validation and repair involve, here is the whole cycle without any library: parse, check types and ranges, and on failure send the errors back to the model and ask again. The mock model makes two typical mistakes before getting it right.

```python
import json
from dataclasses import dataclass

@dataclass(frozen=True)
class Incident:
    table: str
    severity: int        # 1 (cosmetic) to 5 (data loss)
    cause: str
    rerun: bool

FIELDS = {"table": str, "severity": int, "cause": str, "rerun": bool}

def parse_incident(raw):
    start, end = raw.find("{"), raw.rfind("}")           # tolerate chatter around the JSON
    data = json.loads(raw[start:end + 1])                # json.JSONDecodeError if malformed
    errors = []
    for name, typ in FIELDS.items():
        if name not in data:
            errors.append(f"missing field '{name}'")
        elif type(data[name]) is not typ:                # exact type: True is not an int here
            errors.append(f"'{name}' must be {typ.__name__}, got {type(data[name]).__name__}")
    if type(data.get("severity")) is int and not 1 <= data["severity"] <= 5:
        errors.append("'severity' must be between 1 and 5")
    if extra := set(data) - set(FIELDS):
        errors.append(f"unexpected fields {sorted(extra)}")
    if errors:
        raise ValueError("; ".join(errors))
    return Incident(**data)

replies = iter([                                   # what a model without constraints might say
    'Sure! Here is the JSON: {"table": "analytics.orders", "severity": "high", "cause": "empty file"}',
    '{"table": "analytics.orders", "severity": 7, "cause": "empty upstream file", "rerun": true}',
    '{"table": "analytics.orders", "severity": 4, "cause": "empty upstream file", "rerun": true}',
])
def mock_model(messages):
    return next(replies)

def extract(prompt, model, max_attempts=3):
    messages = [{"role": "user", "content": prompt}]
    for attempt in range(1, max_attempts + 1):
        raw = model(messages)
        try:
            return parse_incident(raw), attempt
        except ValueError as err:                       # JSONDecodeError is a ValueError too
            print(f"attempt {attempt} rejected: {err}")
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content":
                             f"Your JSON was invalid: {err}. Reply with only the corrected JSON object."})
    raise RuntimeError(f"no valid output after {max_attempts} attempts")

incident, attempts = extract("Summarise this alert as JSON: ...", mock_model)
print(incident, attempts)
# attempt 1 rejected: 'severity' must be int, got str; missing field 'rerun'
# attempt 2 rejected: 'severity' must be between 1 and 5
# Incident(table='analytics.orders', severity=4, cause='empty upstream file', rerun=True) 3
```

The repair message is specific ("must be between 1 and 5"), which is what lets the model fix it in one go. A vague "invalid, try again" wastes attempts.

**With Pydantic** (`pip install pydantic`), the same model is a few lines, and the schema comes for free:

```python
from pydantic import BaseModel, ConfigDict, Field, ValidationError

class Incident(BaseModel):
    model_config = ConfigDict(extra="forbid")              # reject unexpected fields
    table: str = Field(description="Fully qualified table, like dataset.table")
    severity: int = Field(ge=1, le=5, description="1 cosmetic, 5 data loss")
    cause: str
    rerun: bool

try:
    Incident.model_validate_json('{"table": "analytics.orders", "severity": 7, "cause": "x", "rerun": true}')
except ValidationError as err:
    print(err.errors()[0]["loc"], err.errors()[0]["msg"])
# ('severity',) Input should be less than or equal to 5

schema = Incident.model_json_schema()
print(schema["required"])     # ['table', 'severity', 'cause', 'rerun']
```

**Provider-native structured outputs.** Each SDK accepts the Pydantic model directly and returns a parsed object:

```python
import anthropic
from openai import OpenAI

alert = "orders loaded 0 rows; source file s3://raw/orders/2026-11-23.csv is 0 bytes"

# Claude: constrained JSON output, parsed into the Pydantic model
claude = anthropic.Anthropic()
resp = claude.messages.parse(
    model="claude-opus-5-5", max_tokens=1024,
    messages=[{"role": "user", "content": f"Summarise this alert: {alert}"}],
    output_format=Incident,
)
incident = resp.parsed_output            # an Incident instance

# Open-weight model on vLLM or Ollama: the server enforces the JSON schema while decoding
local = OpenAI(base_url="http://localhost:8000/v1", api_key="not-needed")
resp = local.chat.completions.parse(
    model="Qwen/Qwen3-8B",
    messages=[{"role": "user", "content": f"Summarise this alert: {alert}"}],
    response_format=Incident,
)
incident = resp.choices[0].message.parsed

# OpenAI Responses API
oai = OpenAI()
resp = oai.responses.parse(model="a-current-openai-model-id",
                           input=f"Summarise this alert: {alert}", text_format=Incident)
incident = resp.output_parsed
```

On the Claude Messages API without the helper, the same thing is `output_config={"format": {"type": "json_schema", "schema": {...}}}` on `messages.create`, and tool parameters can be made exact with `"strict": True` on a tool definition, so the arguments of every tool call validate against its `input_schema`. Tool calling is itself a form of structured output: a tool's input schema is a JSON Schema the model fills in.

**Schema design tips:**

- Keep schemas flat and small. Each field is something the model must get right.
- Use enums (`Literal["low", "medium", "high"]`) instead of free strings wherever the set of values is known; your code can then branch safely.
- Give fields descriptions: they are part of the prompt.
- Allow "unknown": an optional field or an explicit `"unknown"` enum value stops the model from inventing a value to satisfy a required field.
- Put a short `reasoning` or `evidence` field *before* the decision field when you want the model to justify the decision; output is generated in order.

> **Pitfall:** Trusting schema-valid output as correct. Constrained decoding guarantees that `severity` is an integer from 1 to 5, not that 4 is the right severity. Keep business-rule validation (does the table exist? is the date in the past?) in your code, and evaluate accuracy separately.

> **Tip:** Version your output schemas like table schemas. Downstream consumers (a PostgreSQL table, another agent) depend on them, so adding an optional field is safe and renaming one is a breaking change.

::: deeper
**How constrained decoding works.** The provider compiles your JSON Schema into a grammar (a state machine). At each generation step, tokens that would break the grammar get their probability set to zero before sampling, so only tokens that keep the output valid can be chosen. That is why the output is valid by construction, and also why there are limits: very large schemas, deep recursion and some keywords (complex regular expressions, certain numeric or string constraints) may be unsupported or simplified. SDK helpers often move unsupported constraints into descriptions and validate them after parsing, which is another reason to keep your own validation.

**First-call latency.** Compiling a schema into a grammar takes time, so the first request with a new schema can be slower; providers cache the compiled grammar for later calls. Keep schemas stable for the same reason you keep prompts stable.

**Does constraining hurt quality?** Some studies found that forcing strict formats can reduce reasoning quality on some tasks ("Let Me Speak Freely?", Tam et al., 2024), because the model cannot think in free text first. Common mitigations: let the model reason first (thinking, or a reasoning field placed before the answer), or use two steps, a free-form answer followed by a cheap extraction call into the schema.

**Truncation.** If output hits `max_tokens`, JSON is cut off mid-object no matter how it was constrained. Check the stop reason before parsing.

**Libraries.** Instructor and PydanticAI wrap the validate-and-retry loop around many providers; LangChain offers `with_structured_output`. They are convenient, but the mechanics are the ones you just wrote.

> **Interview:** "How do you get reliable JSON out of an LLM?" Define the output as a Pydantic model, use the provider's schema-constrained output (or strict tool schemas) so the shape is guaranteed, check the stop reason for truncation, validate business rules in code, and on failure send the specific validation errors back for a bounded number of repair attempts. Then measure accuracy with an evaluation set, because valid is not the same as correct.

> **Interview:** "JSON mode versus structured outputs?" JSON mode only guarantees syntactically valid JSON of some shape. Structured outputs constrain generation to your specific schema, so required fields, types and enums are guaranteed.
:::

::: check
?? What does schema-constrained decoding guarantee, and what does it not?
It guarantees the output parses and matches the schema (fields, types, enums). It does not guarantee the values are correct or consistent with reality.

?? Why should a repair prompt include the specific validation error?
So the model knows exactly what to fix ("severity must be between 1 and 5") and can correct it in one attempt instead of guessing.

?? Why might you add a `reasoning` field before the `decision` field in a schema?
Output is generated in order, so the model writes its justification first and then decides with that reasoning already in its context, which tends to improve the decision.
:::

::: resources
- Docs: [Structured outputs](https://platform.claude.com/docs/en/build-with-claude/structured-outputs) · JSON outputs and strict tool use on the Claude API
- Docs: [Structured outputs in vLLM](https://docs.vllm.ai/en/latest/features/structured_outputs.html) · JSON schema, regex and grammar constraints on a self-hosted model
- Docs: [Pydantic models](https://docs.pydantic.dev/latest/concepts/models/) · validation, `model_validate_json` and JSON Schema generation
- Docs: [Understanding JSON Schema](https://json-schema.org/understanding-json-schema) · the schema language from the ground up
- Paper: [Let Me Speak Freely? A Study on the Impact of Format Restrictions on Performance of Large Language Models](https://arxiv.org/abs/2408.02442) · when strict formats hurt reasoning
- Docs: [Instructor](https://python.useinstructor.com/) · a popular validate-and-retry library for structured extraction
:::

### Reasoning models and extended thinking

::: plain
Some questions you can answer on the spot; others need scrap paper. Reasoning models get a scrap-paper phase: before writing the answer, the model writes out its working (its "thinking"), which you usually do not show the user. For hard, multi-step problems this produces much better answers. But scrap paper is not free: every line of working costs time and money, so you give the model more of it for hard problems and less for easy ones.
:::

A **reasoning model** is a model trained (with reinforcement learning on problems that have checkable answers, such as maths, code and tool tasks) to produce a long internal chain of reasoning before its final answer. The reasoning is generated as tokens like any other output, but returned separately: as `thinking` blocks on Claude, in a separate reasoning field on open-weight models such as Qwen3 or DeepSeek-R1 served by vLLM or Ollama, and as reasoning items (usually summarised or hidden) on OpenAI. Providers call this **extended thinking** or simply **thinking**.

Why it works follows from the previous topics. The model generates left to right and cannot revise earlier tokens. Thinking gives it room to try an approach, notice a problem and switch, *before* committing to the answer. Spending more computation at answer time, called **test-time compute**, turns out to be a strong lever: on hard problems, a model that thinks longer often beats a larger model that does not.

**How you control it.** The controls have changed quickly, so check the documentation for the model you use:

| Provider | Control | Notes |
|---|---|---|
| Claude (current models) | `thinking={"type": "adaptive"}` plus `output_config={"effort": "low" ... "max"}` | The model decides how much to think per request; effort sets how much it is willing to spend. Older models used a fixed `budget_tokens` instead |
| Open-weight (vLLM, Ollama) | vLLM: start the server with `--reasoning-parser`; Ollama: `think=True` per request | The reasoning comes back in its own field instead of mixed into the answer; some models let you turn thinking off per request |
| OpenAI | `reasoning={"effort": ...}` | Reasoning tokens are billed but hidden; summaries are optional |

On Claude:

```python
import anthropic

client = anthropic.Anthropic()
response = client.messages.create(
    model="claude-opus-5-5",
    max_tokens=16000,
    thinking={"type": "adaptive", "display": "summarized"},   # return a readable summary
    output_config={"effort": "high"},                          # low | medium | high | xhigh | max
    messages=[{"role": "user", "content":
               "Our nightly MERGE into analytics.orders doubled its runtime after we added "
               "a clustering column. List the three most likely causes, most likely first."}],
)
for block in response.content:
    if block.type == "thinking":
        print("THINKING:", block.thinking[:200])
    elif block.type == "text":
        print(block.text)
print(response.usage.output_tokens)     # thinking tokens are included in output tokens
```

On an open-weight reasoning model served locally by Ollama (`pip install ollama`):

```python
from ollama import chat

resp = chat(
    model="qwen3",                       # a reasoning model pulled with `ollama pull qwen3`
    messages=[{"role": "user", "content":
               "Why might a MERGE slow down after adding a clustering column?"}],
    think=True,                          # return the reasoning separately; False turns it off
)
print("THINKING:", resp.message.thinking[:200])
print(resp.message.content)
print(resp.eval_count)                   # output tokens, reasoning included
```

**Interleaved thinking.** In an agent, the most valuable thinking often happens *between* tool calls: "the row count is zero, so the problem is upstream; check the file next". **Interleaved thinking** lets the model think after each tool result, not only at the start of a turn. On current Claude models adaptive thinking does this automatically. One rule follows: **pass thinking blocks back unchanged.** The loop from "The agent loop: observe, think, act" already does this by appending `response.content` whole. If you rebuild assistant messages from just the text and tool calls, the model loses its reasoning, and some APIs reject the request or drop the reasoning.

**When reasoning helps.** Thinking pays off when the task has several dependent steps and getting one wrong ruins the rest:

| Helps a lot | Helps little or not at all |
|---|---|
| Planning a multi-step investigation | Classification and routing |
| Debugging, root-cause analysis | Simple extraction into a schema |
| Writing or fixing non-trivial SQL and code | Summarising a short document |
| Choosing among many tools with subtle trade-offs | Rewriting or formatting text |
| Maths, logic, constraint problems | Lookups where the answer is in the context |

**What it costs.** Thinking tokens are billed as output tokens, the expensive kind, and they add latency because they are generated before the answer starts. In an agent, the cost multiplies by the number of steps. A quick model of the trade-off (prices and token counts here are illustrative; plug in your own):

```python
# Illustrative prices and token counts: plug in your provider's real numbers.
PRICE_IN, PRICE_OUT = 4.00, 20.00            # dollars per million tokens

def run_cost(input_tokens, thinking_tokens, answer_tokens, steps):
    per_step = input_tokens * PRICE_IN + (thinking_tokens + answer_tokens) * PRICE_OUT
    return steps * per_step / 1_000_000

for label, thinking in [("no thinking", 0), ("light", 1_000), ("heavy", 8_000)]:
    cost = run_cost(input_tokens=20_000, thinking_tokens=thinking, answer_tokens=300, steps=12)
    print(f"{label:12} ${cost:.2f} per run, ${cost * 10_000:,.0f} per 10k runs")
# no thinking  $1.03 per run, $10,320 per 10k runs
# light        $1.27 per run, $12,720 per 10k runs
# heavy        $2.95 per run, $29,520 per 10k runs
```

Heavy thinking nearly triples the cost of this run. Whether that is worth it depends on what a wrong answer costs: for an incident root cause, probably yes; for tagging a million support tickets, no. Note also that input tokens dominate the "no thinking" cost here, which is why prompt caching matters so much for agents.

> **Pitfall:** Turning effort up to the maximum everywhere "to be safe". Measure on your evaluation set: on many routine tasks lower effort gives the same quality at a fraction of the cost and latency. Tune effort per step, not per product.

> **Tip:** With a reasoning model, give the goal, the context and the success criteria, and let it plan. Detailed step-by-step instructions written for older models can make reasoning models worse, because they constrain an approach the model could have chosen better.

::: deeper
**Where reasoning models came from.** Chain-of-thought prompting (Wei et al., 2022) showed that asking for intermediate steps improves multi-step accuracy. OpenAI's o1 (2024) trained this behaviour in with reinforcement learning and showed accuracy rising with both training compute and thinking time. DeepSeek-R1 (2025) published the recipe openly: reinforcement learning on verifiable rewards makes long, self-correcting reasoning emerge. Since then most frontier models offer a thinking mode, and several decide on their own how long to think.

**Hidden or summarised thinking.** Providers increasingly do not return the raw reasoning: some return a summary, some return nothing but bill the tokens, and some return encrypted reasoning that you must pass back for the model to reuse. Do not build features that depend on reading raw chain of thought.

**Is the thinking faithful?** Not necessarily. Research from Anthropic (2025) found that reasoning models often do not mention in their thinking a hint that actually influenced their answer. Treat the thinking as a useful debugging signal, not as a reliable audit log of why the model decided something. For audit, log tool calls and their results, which are facts.

**Effort versus model size.** Before building a cascade of models (cheap model first, expensive one on failure), measure the simpler option: the strongest model at lower effort. It often matches a weaker model at high effort, and a single model keeps one prompt cache.

**The "think" tool.** Before interleaved thinking was common, Anthropic described a simple pattern: give the agent a no-op `think` tool whose only parameter is a thought, so it can pause and reason between tool calls. With interleaved thinking built in, it matters less, but the idea shows why reasoning between actions helps.

**Thinking and caching.** Changing thinking settings mid-conversation can invalidate cached prefixes on some providers, and thinking blocks are part of the history you resend. Decide thinking settings per run, not per turn, unless the provider documents a per-turn control.

> **Interview:** "When would you use a reasoning model or turn on extended thinking in an agent?" For steps with several dependent decisions where an early mistake is costly: planning, root-cause analysis, SQL generation over a complex schema, choosing among many tools. Not for classification, simple extraction or formatting, where it adds cost and latency for no gain. I tune the effort level per step against an evaluation set, keep in mind that thinking tokens are billed as output tokens and multiply by the number of agent steps, and I pass thinking blocks back unchanged in tool loops.

> **Interview:** "What is test-time compute?" Spending more computation when answering, rather than when training: longer reasoning, several sampled answers with a vote, or search over candidate solutions. Accuracy on hard problems rises with it, which is what reasoning models exploit.
:::

::: check
?? How are thinking tokens billed, and why does that matter more for agents than for single calls?
As output tokens, the expensive kind. An agent pays for thinking on every step, so the cost multiplies by the number of steps in a run.

?? What is interleaved thinking, and what must your loop do to support it?
Thinking between tool calls, after each tool result, not only at the start of a turn. The loop must pass the assistant's content back unchanged, thinking blocks included.

?? Name two tasks where you would not turn on heavy thinking.
Any two of: classification or routing, simple extraction into a schema, summarising a short text, reformatting, lookups where the answer is already in the context.
:::

::: resources
- Docs: [Building with extended thinking](https://platform.claude.com/docs/en/build-with-claude/extended-thinking) · thinking blocks, interleaved thinking and tool use
- Docs: [Effort](https://platform.claude.com/docs/en/build-with-claude/effort) · trading thoroughness for tokens on current Claude models
- Docs: [Reasoning outputs in vLLM](https://docs.vllm.ai/en/latest/features/reasoning_outputs.html) · reasoning parsers for open-weight models
- Docs: [Thinking in Ollama](https://docs.ollama.com/capabilities/thinking) · the `think` option and the separate thinking field
- Paper: [DeepSeek-R1: Incentivizing Reasoning Capability in LLMs via Reinforcement Learning](https://arxiv.org/abs/2501.12948) · an open account of how reasoning models are trained
- Paper: [Reasoning Models Don't Always Say What They Think](https://arxiv.org/abs/2505.05410) · faithfulness of visible reasoning
- Blog: [Why We Think](https://lilianweng.github.io/posts/2025-05-01-thinking/) · Lilian Weng on test-time compute and chain of thought
:::

### Workflows vs agents: when not to build an agent

::: plain
You would not hire a detective to sort the post. If every letter goes through the same three steps, you build a sorting machine: cheap, fast, and it does the same thing every time. You call the detective when nobody knows in advance which clues to follow. Workflows are the sorting machine; agents are the detective. Most business problems are mail sorting with the occasional mystery, so most good designs are mostly workflow.
:::

This topic is the one interviewers probe most, because it separates people who have shipped LLM systems from people who have watched demos. The advice from the teams that build agents is consistent: **find the simplest solution that works, and only add autonomy when it measurably improves results.**

[[fig:foundations_decide]]

**What a workflow looks like.** A workflow is ordinary code that calls models at fixed points, with your logic deciding what happens between calls. The common building blocks are:

- **Prompt chaining**: step 2 uses the output of step 1, with **gates** (checks in code) between them.
- **Routing**: classify the input, then send it to a specialised prompt, model or handler.
- **Parallelisation**: run independent calls at once (one per table, one per document) and combine the results, or run the same call several times and vote.
- **Orchestrator and workers**: one call splits a task into parts, workers handle the parts, code combines them.
- **Evaluator and optimiser**: one call produces, another critiques, repeat a bounded number of times.

The [Agent design patterns](#patterns) section builds each of these. Here is a chain with a router and two gates, for triaging pipeline alerts. Note that the model never decides what happens next; the code does, and the code checks the model's output at each gate:

```python
def mock_llm(prompt):
    """Scripted stand-in for a model call, keyed on the step's instruction."""
    if prompt.startswith("Classify"):
        return "schema_change"
    if prompt.startswith("Draft"):
        return "ALTER TABLE staging.orders RENAME COLUMN cust_id TO customer_id;"
    return "unknown"

ALLOWED = {"schema_change", "late_data", "bad_values"}

def triage_workflow(alert, llm):
    label = llm(f"Classify this alert as one of {sorted(ALLOWED)}: {alert}")
    if label not in ALLOWED:                       # gate: check the model's output in code
        return {"route": "human", "reason": f"unexpected label {label!r}"}
    if label != "schema_change":
        return {"route": "runbook", "label": label}
    fix = llm(f"Draft a SQL fix for this schema change: {alert}")
    if not fix.upper().startswith("ALTER TABLE"):  # gate: only one kind of statement allowed
        return {"route": "human", "reason": "unexpected SQL"}
    return {"route": "review", "label": label, "sql": fix}

print(triage_workflow("KeyError: 'customer_id' in transform_orders", mock_llm))
# {'route': 'review', 'label': 'schema_change', 'sql': 'ALTER TABLE staging.orders RENAME COLUMN cust_id TO customer_id;'}
```

Every path through this function can be listed, tested and costed in advance. It makes at most two model calls. It can be scheduled in Apache Airflow like any other task, and when it goes wrong, the log tells you which step failed.

**The cost of autonomy.** An agent replaces that fixed path with a model choosing each step. You gain flexibility and pay for it in several currencies:

| Cost | Workflow | Agent |
|---|---|---|
| Model calls per task | Fixed and known | Varies, often 5 to 50 |
| Latency | Predictable | Varies widely between runs |
| Token cost | Predictable | Grows with steps and history; long tail of expensive runs |
| Testing | Unit-test each step and path | Needs scenario-based evaluation over many runs |
| Failure modes | A step fails; you see which | Wrong turns compound; loops; actions on hallucinated facts |
| Security surface | Model output flows to fixed places | Model chooses actions, so prompt injection can steer them |
| Debugging | Read the log of a known path | Read a trace of the model's choices |

**A decision checklist.** Build an agent only when the answers point that way:

1. **Can you write down the steps?** If you can draw the flowchart, write the workflow. If the steps depend on what you find along the way and the branches explode, an agent may fit.
2. **Is the task valuable enough?** Agents cost more per task; the task's value must cover the cost and latency.
3. **Can the model actually do it?** Try the hardest step by hand with the model first. If it cannot do the hard step reliably, autonomy will not help.
4. **Can errors be caught and undone?** Tests, dry runs, reviews, read-only tools, rollbacks. If a mistake is expensive and irreversible, keep a human in the loop or do not automate it.
5. **Do you have an evaluation set?** Without one you cannot tell whether the agent beats the workflow, or whether the next change made it worse.

If any answer is "no", stay with a single call or a workflow, or make only one step agentic. Anthropic's guidance on building agents applies essentially the same four tests: complexity, value, viability and the cost of errors.

**Hybrid is the norm.** In practice, the best systems are workflows with a bounded agentic step: a fixed pipeline that, for alerts it cannot classify, hands off to an investigation agent with read-only tools, a step limit and a human review at the end. You get predictability for the common cases and flexibility for the long tail.

> **Pitfall:** Building a "general" agent with twenty tools for a task that is really three fixed steps. It will be slower, more expensive and less reliable than the three-step workflow, and harder to debug. Start from the workflow and promote steps to agents only when evaluation shows a gain.

> **Tip:** Write the workflow first even if you suspect you need an agent. It becomes your baseline in evaluation, and very often it is good enough to ship.

::: deeper
**Deterministic code is a feature.** Everything a data engineer values in pipelines (idempotency, reruns, backfills, lineage, SLAs, cost predictability) comes from control flow being in code. Each step you hand to a model loses some of that. Keep validation, permissions, retries, scheduling and data movement in code, and use the model for judgement inside well-defined boundaries. The "12-factor agents" principles put it as "own your control flow".

**The long tail of cost.** Agent cost per task is not normally distributed: most runs are cheap, and a few runs loop, re-read large tool outputs and cost ten or a hundred times the median. Budget on the 95th or 99th percentile, enforce per-run caps, and alert on outliers, the same way you watch for runaway queries in Trino or Spark.

**Reliability compounds.** If each step of a 10-step task succeeds 95% of the time and steps fail independently, the whole task succeeds about 60% of the time (0.95 to the power 10). Agents can recover from some errors, which helps, but the arithmetic explains why short, checked paths beat long autonomous ones, and why verification steps (tests, dry runs, validators) are so valuable.

**Agents for exploration, workflows for production.** A good pattern is to use an agent to discover how a task is done (which queries, which checks, in what order), read its traces, and then codify the common path as a workflow. The agent remains for the cases the workflow cannot handle.

**When an agent clearly wins**: open-ended investigation (incident root cause across logs, metrics and code), coding tasks with tests, research across many sources, and data questions whose answer needs several exploratory queries. What they share is an unknown path, cheap verification and a tolerable cost of error.

> **Interview:** "When would you not build an agent?" When the steps are known in advance, when the task is high volume and low value, when errors are expensive and hard to detect or undo, or when there is no evaluation set to show the agent beats a simpler design. In those cases a workflow (prompt chaining, routing, parallel calls with gates in code) is cheaper, faster, easier to test and easier to operate. I often ship a workflow with one bounded agentic step for the long tail.

> **Interview:** "Your team wants an agent to generate and run daily data-quality checks. How would you design it?" I would not make the daily run agentic. I would use an agent offline to propose checks from the schema, profiles and past incidents, have a human review them, and store the approved checks as code or configuration. The daily run is then a deterministic workflow, and maybe a model call to summarise failures. That keeps the scheduled path predictable while still using the model where judgement adds value.
:::

::: check
?? What is the key structural difference between the triage workflow above and an agent?
In the workflow, code decides every next step and checks the model's output at gates; the set of paths is fixed and testable. In an agent, the model chooses the next step in a loop.

?? A step succeeds 95% of the time. Roughly how often does a 10-step task succeed if failures are independent and unrecovered, and what does that suggest?
About 60% (0.95 to the power 10). Long autonomous chains are fragile, so keep paths short and add verification and recovery.

?? Give two signs that a task is a good fit for an agent.
Any two of: the steps cannot be known in advance; results can be verified cheaply (tests, queries, dry runs); errors are recoverable; the task's value justifies higher cost and latency; an evaluation set exists to prove the agent beats a workflow.
:::

::: resources
- Blog: [Building effective agents](https://www.anthropic.com/research/building-effective-agents) · workflow patterns, and when to use agents
- Docs: [Workflows and agents](https://docs.langchain.com/oss/python/langgraph/workflows-agents) · the same patterns implemented in LangGraph
- Repo: [12-Factor Agents](https://github.com/humanlayer/12-factor-agents) · "own your control flow" and other production principles
- Article: [Agents](https://huyenchip.com/2025/01/07/agents.html) · Chip Huyen on planning, failure modes and compounding errors
- Blog: [Your AI Product Needs Evals](https://hamel.dev/blog/posts/evals/) · Hamel Husain on why evaluation comes before complexity
- Article: [A practical guide to building agents](https://cdn.openai.com/business-guides-and-resources/a-practical-guide-to-building-agents.pdf) · OpenAI's guide to when an agent is worth it and how to keep it bounded
:::

## Wrap-up

### Cheat sheet

| Concept | Remember |
|---|---|
| Agent | A model in a loop, using tools, toward a goal, where the model chooses the next step |
| Workflow | Model calls at fixed points; code decides the path; checked at gates |
| Token | The unit of input, output, context and billing; about four English characters |
| Context window | Everything the model sees in one call; the API keeps no memory between calls |
| Temperature | Sharpens (low) or flattens (high) the next-token distribution; not supported on some current models |
| Hallucination | Plausible but false output; fix with grounding, abstention and verification |
| Messages | `user`, `assistant`, plus a system prompt; resend the full history every call |
| Stop reason | `end_turn` answer, `tool_use` run tools, `max_tokens` truncated, `refusal` declined |
| Rate limits | 429 with `retry-after`; retry 429 and 5xx with backoff and jitter, not other 4xx |
| Token counting | `client.messages.count_tokens(...)`; log `usage` on every call |
| Prompt structure | Goal, context in XML tags, rules with reasons, examples, output format |
| Prompt versioning | Prompts in the repo, versioned, evaluated before rollout, version logged per call |
| Agent loop | Append assistant content whole; all tool results in one user message; errors as `is_error` |
| Stop conditions | Final answer, max steps, token or cost budget, timeout, unexpected stop reason, stuck detection |
| Structured outputs | Pydantic model as schema and validator; constrained decoding guarantees shape, not truth |
| Repair loop | Send the specific validation error back; bounded attempts |
| Thinking | Billed as output tokens; helps multi-step tasks; tune effort per step |
| Interleaved thinking | Thinking between tool calls; pass thinking blocks back unchanged |
| When not to build an agent | Known steps, low value per task, costly irreversible errors, no evaluation set |

### Practice

::: try
**1. Add a repeated-call detector.** Extend `run_agent` from "The agent loop: observe, think, act" so that it stops with `"stopped: repeated call"` when the model requests the same tool with the same arguments three times in a row. Test it with `stubborn_model` and `max_steps=10`.
:::

?? A solution to 1

```python
import json

def run_agent(goal, model, tools, tool_specs, system="", max_steps=8, max_repeats=3):
    messages = [{"role": "user", "content": goal}]
    state = {"steps": 0, "tool_calls": []}
    recent = []                                        # signatures of the last calls
    while state["steps"] < max_steps:
        response = model(system, messages, tool_specs)
        state["steps"] += 1
        messages.append({"role": "assistant", "content": response["content"]})
        if response["stop_reason"] != "tool_use":
            return "".join(b.get("text", "") for b in response["content"]), state
        results = []
        for block in response["content"]:
            if block["type"] != "tool_use":
                continue
            sig = (block["name"], json.dumps(block["input"], sort_keys=True))
            recent = (recent + [sig])[-max_repeats:]
            if len(recent) == max_repeats and len(set(recent)) == 1:
                return "stopped: repeated call", state
            state["tool_calls"].append(block["name"])
            try:
                out, err = json.dumps(tools[block["name"]](**block["input"])), False
            except Exception as exc:
                out, err = f"{type(exc).__name__}: {exc}", True
            results.append({"type": "tool_result", "tool_use_id": block["id"],
                            "content": out, "is_error": err})
        messages.append({"role": "user", "content": results})
    return "stopped: step limit reached", state

def stubborn_model(system, messages, tools):
    return {"role": "assistant", "stop_reason": "tool_use",
            "content": [{"type": "tool_use", "id": f"tu_{len(messages)}",
                         "name": "get_table_stats", "input": {"table": "analytics.order"}}]}

def get_table_stats(table):
    return {"analytics.orders": {"rows_loaded": 0}}[table]

answer, state = run_agent("Why?", stubborn_model, {"get_table_stats": get_table_stats}, [], max_steps=10)
print(answer, state["steps"])     # stopped: repeated call 3
```

::: try
**2. Estimate before you build.** An agent averages 15 steps per run. Each step sends about 30,000 input tokens and produces 500 output tokens, plus 2,000 thinking tokens. With illustrative prices of $4 per million input tokens and $20 per million output tokens, what does one run cost? What does 2,000 runs a day cost per month (30 days)? Which single change would cut the bill most?
:::

?? Answer to 2
Per step: 30,000 × $4 / 1M = $0.12 input, plus 2,500 × $20 / 1M = $0.05 output, so $0.17. Per run: 15 × $0.17 = $2.55. Per month: $2.55 × 2,000 × 30 = $153,000. Input is about 70% of the cost and is mostly the same prefix resent every step, so prompt caching (stable system prompt and tools first, history appended) is the biggest single lever; trimming large tool outputs and lowering effort on easy steps come next. Then ask whether the task needs an agent at all.

::: try
**3. Write a schema and a repair prompt.** A pipeline asks a model to extract `{"table": ..., "owner_email": ..., "pii": true|false, "retention_days": ...}` from free-text table documentation. Write the Pydantic model (retention between 1 and 3650 days, an optional owner email that must contain an `@` when present), and the exact repair message you would send back after a validation failure.
:::

?? A solution to 3

```python
from pydantic import BaseModel, ConfigDict, Field, ValidationError

class TableDoc(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str = Field(description="Fully qualified table, like dataset.table")
    owner_email: str | None = Field(default=None, pattern=r"^[^@\s]+@[^@\s]+$")
    pii: bool
    retention_days: int = Field(ge=1, le=3650)

raw = '{"table": "crm.customers", "owner_email": "data-team", "pii": true, "retention_days": 0}'
try:
    TableDoc.model_validate_json(raw)
except ValidationError as err:
    problems = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in err.errors())
    repair = (f"Your JSON did not validate: {problems}. "
              "Reply with only the corrected JSON object. If a value is not in the text, "
              "use null for optional fields rather than inventing one.")
    print(repair)
```

The repair message names each failing field and its rule, asks for JSON only, and explicitly allows `null` for the optional owner so the model does not invent one.

::: try
**4. Workflow or agent?** For each task, say whether you would build a single call, a workflow or an agent, and why: (a) tag 2 million support tickets with one of 12 categories every night; (b) answer ad-hoc business questions over a 400-table warehouse; (c) write a summary of each failed Airflow DAG run for the on-call channel; (d) find out why yesterday's revenue dashboard disagrees with finance's number.
:::

?? Answer to 4
(a) A single call per ticket run as a batch job (or a small fine-tuned or small-tier model): fixed task, huge volume, low value per item. (b) An agent, or a workflow with an agentic SQL step: the queries needed are not known in advance, and results can be checked by running them; give it read-only tools, schema search and a step limit. (c) A workflow: on failure, fetch the log, call the model once to summarise, post; the steps never change. (d) An agent with read-only tools and a human reviewing the conclusion: an open-ended investigation across queries, lineage and recent changes, where the path depends on what each query shows.

### What comes next

You now have the engine (the model), the interface (the messages API) and the loop. The loop is only as good as what the model can do in it, so the next section, **Tool use and function calling**, goes deep on tools: how to define and describe them, how the model chooses and calls them, parallel calls, error handling and the security questions that come with letting a model take actions.
