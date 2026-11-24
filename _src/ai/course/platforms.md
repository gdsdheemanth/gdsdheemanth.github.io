This section is for the reader who can build an agent, by hand or with a framework ([Agent frameworks](#frameworks)), and now has to run it somewhere real. It assumes the Python from earlier sections and a working knowledge of Docker and basic Kubernetes words (pod, deployment, service). It does not assume you have ever served a model on a GPU. The target is the job most data engineers will actually have: a company that runs its own infrastructure, keeps data inside its own network, and uses hosted model APIs only where policy allows.

**What you will be able to do afterwards**

- Choose an open-weight model by size, licence and tool-calling ability, and work out how many GPUs it needs with a few lines of arithmetic.
- Explain how vLLM serves many users on one GPU (PagedAttention, continuous batching, prefix caching), and when Ollama, llama.cpp or TGI is the better fit.
- Put a gateway such as LiteLLM in front of every model, with one API, fallbacks, per-team keys and budgets, and logs.
- Package an agent as a FastAPI service that streams its progress, build it into a container, and run it on Kubernetes with probes, autoscaling and Jobs for long runs.
- Give an agent its own identity, let it act on behalf of a user without gaining extra rights, keep its secrets in Vault, and run the whole stack in an air-gapped network.
- Describe the managed path on Google Cloud (Agent Runtime on Gemini Enterprise Agent Platform, Cloud Run, GKE) and say when it is worth it.

**The big picture**

A deployed agent is three services, not one. At the bottom is a **model server**: vLLM, Ollama or another engine holding an open-weight model in GPU memory and answering on an OpenAI-compatible HTTP API. In the middle is a **gateway**: one endpoint that every agent calls, which picks the model, retries, falls back, counts tokens and enforces budgets. At the top is the **agent service**: your loop, tools and prompts, packaged as an HTTP API in a container. Around all three sit identity, secrets and network rules, which decide who may call what.

[[fig:platforms_stack]]

Data engineers already know this shape. The model server is like a database server: an expensive, stateful process you size carefully and share. The gateway is like PgBouncer or a Trino coordinator: one front door that pools, routes and protects the expensive back end. The agent service is like an application or an Airflow worker: stateless code you can scale out freely, as long as its state lives elsewhere.

**The scenario.** The same sales analyst as in [Agent frameworks](#frameworks): one tool, `run_sql`, a read-only `SELECT` on an `orders` table, and the question "Which region had the most revenue in October 2026?". This section takes that agent from a script to a service that many analysts use at once, on the company's own GPUs.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

> **Tip:** How the examples were checked. The sizing calculator, the mini gateway and the identity example ran with plain `python3`. The FastAPI service ran with `fastapi` 0.165.0 through its `TestClient`, both with a scripted model and with the `openai` 3.58.0 client talking to a small scripted server that speaks the OpenAI Chat Completions API on `localhost:8000`, the same way vLLM does; the tool-calling smoke test ran against that server too. The LiteLLM `Router` example ran with `litellm` 1.110.3 using its built-in mock responses, so no model was called. The Google Cloud snippet was written from the current Agent Platform docs and syntax-checked; the `adk deploy` commands match `google-adk` 2.15.0. Shell commands, Dockerfiles and Kubernetes manifests were not run against a cluster. As in the frameworks section, `shop.py` ran with Python's built-in `sqlite3` standing in for DuckDB.

## On-prem and open source

On-prem means you own the whole stack: the GPUs, the model weights, the serving engine, the gateway and the deployment. That is more work than calling a hosted API, but it is often the only option when data may not leave the building, and it turns a per-token bill into a fixed hardware cost. This group goes bottom up: serving a model, putting a gateway in front of it, and deploying the agent itself.

The three topics map onto skills you already have. Sizing GPUs is capacity planning, like sizing a Spark cluster from data volume. A gateway is a connection pooler with quotas. Deploying the agent is the same Docker and Kubernetes work as any other service, with two twists: responses stream for a long time, and some runs last minutes or hours.

### Serving open-weight models on-prem: vLLM, Ollama and TGI

::: plain
An open-weight model is a file of numbers you can download and run on your own machines, like buying a database licence instead of renting a cloud service. To answer questions it must sit in GPU memory, and a **serving engine** turns it into a web service that many people can call at once. vLLM is the engine for production GPU servers; Ollama is the easy one for laptops and small teams; llama.cpp runs models on ordinary CPUs and Macs. Choosing a model is mostly about three things: does it fit your GPUs, does its licence allow your use, and does it call tools reliably.
:::

**Open-weight model families and licences.** "Open weights" means the trained parameters are published; it does not mean the training data or code are, so most of these are not open source in the strict OSI sense. What matters to your legal team is the licence on each model card, and it can differ between models of the same family.

| Family | Typical licence (check each model card) | Notes for agents |
|---|---|---|
| Qwen (Alibaba) | Apache 2.0 for most open models | Strong tool calling; dense and mixture-of-experts sizes; coder variants |
| Llama (Meta) | Llama Community License | Commercial use allowed, but extra terms: attribution, an acceptable use policy, and a separate licence above 700 million monthly users |
| Mistral | Apache 2.0 for many open models; some under research or non-production licences | Good function calling; check which licence each release uses |
| Gemma (Google) | Apache 2.0 from Gemma 4; earlier versions under the Gemma Terms of Use | Small to mid sizes, runs on one GPU |
| gpt-oss (OpenAI) | Apache 2.0 | Reasoning models with tool use; vLLM has a dedicated `openai` tool parser |
| DeepSeek | MIT for the main open releases | Very large mixture-of-experts models; need a multi-GPU node |
| Granite (IBM), Phi (Microsoft) | Apache 2.0, MIT | Small models for classification, extraction and routing |

> **Pitfall:** A licence is per model, not per family, and fine-tuned derivatives inherit the base model's terms. Record the model, its exact revision (a Hugging Face commit hash) and its licence in your model registry, exactly as you would record a dataset's lineage.

**How much GPU memory a model needs.** Two things live in GPU memory: the **weights** and the **KV cache**. Weights cost parameters times bytes per parameter: 2 bytes in BF16 (16-bit), 1 byte in FP8 or INT8, about half a byte in INT4. The KV cache is the model's working memory: for every token in every active conversation, each layer stores a key vector and a value vector so it does not recompute them. Its size per token is `2 x layers x kv_heads x head_dim x bytes`. Whatever memory is left after the weights becomes cache, and cache decides how many users you can serve at once. Here is the arithmetic as a program:

```python
GiB = 1024 ** 3

def weights_gib(params_billion, bits):
    return params_billion * 1e9 * bits / 8 / GiB

def kv_bytes_per_token(layers, kv_heads, head_dim, kv_bits=16):
    return 2 * layers * kv_heads * head_dim * kv_bits / 8     # 2 = one K and one V vector

def plan(name, params_b, bits, layers, kv_heads, head_dim, gpu_gib, n_gpus=1,
         util=0.90, overhead_gib=2.0, ctx=8192, kv_bits=16):
    w = weights_gib(params_b, bits)
    budget = gpu_gib * n_gpus * util                          # vLLM --gpu-memory-utilization
    kv_free = budget - w - overhead_gib * n_gpus              # activations, CUDA graphs, runtime
    per_tok = kv_bytes_per_token(layers, kv_heads, head_dim, kv_bits)
    tokens = int(kv_free * GiB / per_tok) if kv_free > 0 else 0
    print(f"{name}: weights {w:.1f} GiB, KV {per_tok / 1024:.0f} KiB/token, "
          f"cache {max(kv_free, 0):.1f} GiB = {tokens:,} tokens = {tokens // ctx} full {ctx}-token sequences")

# Llama-3.1-8B shape: 32 layers, 8 KV heads (grouped-query attention), head size 128
plan("8B  BF16 on 1x24 GiB", 8.0, 16, 32, 8, 128, gpu_gib=24)
plan("8B  BF16 on 1x80 GiB", 8.0, 16, 32, 8, 128, gpu_gib=80)
# Llama-3.1-70B shape: 80 layers, 8 KV heads, head size 128
plan("70B BF16 on 2x80 GiB", 70.6, 16, 80, 8, 128, gpu_gib=80, n_gpus=2)
plan("70B INT4 on 1x80 GiB", 70.6, 4.5, 80, 8, 128, gpu_gib=80)          # ~4.5 bits with scales
plan("70B INT4 + FP8 KV   ", 70.6, 4.5, 80, 8, 128, gpu_gib=80, kv_bits=8)
# 8B  BF16 on 1x24 GiB: weights 14.9 GiB, KV 128 KiB/token, cache 4.7 GiB = 38,492 tokens = 4 full 8192-token sequences
# 8B  BF16 on 1x80 GiB: weights 14.9 GiB, KV 128 KiB/token, cache 55.1 GiB = 451,369 tokens = 55 full 8192-token sequences
# 70B BF16 on 2x80 GiB: weights 131.5 GiB, KV 320 KiB/token, cache 8.5 GiB = 27,843 tokens = 3 full 8192-token sequences
# 70B INT4 on 1x80 GiB: weights 37.0 GiB, KV 320 KiB/token, cache 33.0 GiB = 108,183 tokens = 13 full 8192-token sequences
# 70B INT4 + FP8 KV   : weights 37.0 GiB, KV 160 KiB/token, cache 33.0 GiB = 216,366 tokens = 26 full 8192-token sequences
```

Read the results like a capacity plan. An 8B model fits a 24 GB card but leaves room for only a few long conversations. A 70B model in BF16 needs two 80 GB cards and still has almost no cache, so it serves three users. The same model quantised to INT4 fits on one card with four times the cache; storing the cache in FP8 doubles it again. Agents make this worse than chat, because every tool result stays in the context: a run with ten SQL results can easily reach 20,000 tokens.

**Quantisation** stores weights in fewer bits. The common choices:

| Format | Bits per weight | Where it runs | Quality |
|---|---|---|---|
| BF16 / FP16 | 16 | Everywhere | Reference |
| FP8 (W8A8) | 8 | vLLM, SGLang on recent NVIDIA (Ada, Hopper, Blackwell) and AMD | Usually very close to BF16 |
| INT4 weight-only: AWQ, GPTQ | about 4 to 4.5 | vLLM, TGI | Small loss; test on your tasks |
| GGUF k-quants (`Q4_K_M`, `Q5_K_M`, `Q8_0`) | about 4.5 to 8.5 | llama.cpp, Ollama, CPU and Apple Silicon | `Q4_K_M` is the usual default |

> **Pitfall:** Quantisation hurts tool calling and long-context reasoning before it hurts chat. A 4-bit model may write fluent sentences and still produce broken JSON arguments. Re-run your tool-call tests after quantising, not just a chat benchmark.

**vLLM.** vLLM is the open-source engine most teams run in production. One command downloads a model from Hugging Face (or reads it from a local path) and serves it on an OpenAI-compatible API:

```bash
vllm serve Qwen/Qwen3-32B-AWQ \
  --served-model-name analyst \
  --max-model-len 32768 \
  --gpu-memory-utilization 0.90 \
  --max-num-seqs 64 \
  --kv-cache-dtype fp8 \
  --tensor-parallel-size 1 \
  --enable-auto-tool-choice --tool-call-parser hermes \
  --api-key "$VLLM_API_KEY" --port 8000
```

`--served-model-name` is the name clients send as `model`. `--max-model-len` caps the context and so the worst-case cache per request. `--gpu-memory-utilization` is the share of GPU memory vLLM takes; it pre-allocates the cache from it at start-up. `--max-num-seqs` caps how many sequences run in one batch. `--tensor-parallel-size` splits each layer across GPUs in one node. The two tool flags matter most for agents: `--enable-auto-tool-choice` lets the model decide when to call a tool, and `--tool-call-parser` turns the model's raw output into structured `tool_calls`. Each family writes tool calls in its own format, so the parser must match: `hermes` for Qwen2.5-style models, `llama3_json` for Llama 3.1 and later, `mistral` for Mistral, `openai` for gpt-oss, `qwen3_xml` for Qwen3-Coder, and others listed in the docs. Reasoning models also take a `--reasoning-parser` that separates the thinking text from the answer.

The endpoint speaks the Chat Completions API, so every client and framework from [Agent frameworks](#frameworks) works by changing `base_url`. Before you point an agent at a new model, run a smoke test that fails loudly when tool calls come back as text:

```python
# smoke_tools.py: does the server return real tool calls, not JSON inside the text?
import json, sys
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="unused")
model = client.models.list().data[0].id                        # the --served-model-name
tools = [{"type": "function", "function": {
    "name": "run_sql", "description": "Run one read-only SELECT on the orders table.",
    "parameters": {"type": "object", "properties": {"sql": {"type": "string"}}, "required": ["sql"]}}}]

reply = client.chat.completions.create(
    model=model, tools=tools, temperature=0,
    messages=[{"role": "user", "content": "Which region had the most revenue in October 2026?"}])
choice = reply.choices[0]
calls = choice.message.tool_calls or []
if not calls:                                                  # the classic parser mismatch
    sys.exit(f"FAIL: no tool_calls; text was {choice.message.content!r}")
args = json.loads(calls[0].function.arguments)                 # must parse as JSON
assert calls[0].function.name == "run_sql" and "sql" in args
print(f"OK {model}: finish_reason={choice.finish_reason}, tool={calls[0].function.name}")
print(args["sql"][:40])
# OK local-model: finish_reason=tool_calls, tool=run_sql
# SELECT region, SUM(amount) AS revenue FR
```

Run it with 20 or 50 different questions, not one, and count failures. That number is the tool-call reliability of this model, this quantisation and this parser together.

**Ollama** wraps llama.cpp in a friendly daemon with a model library. `ollama pull qwen3:8b` downloads a GGUF build, `ollama serve` listens on port 11434, and the OpenAI-compatible API is at `http://localhost:11434/v1`. It loads and unloads models on demand, which is ideal for laptops and small shared servers. The settings you will touch are environment variables: `OLLAMA_HOST` (bind address), `OLLAMA_NUM_PARALLEL` (concurrent requests per model), `OLLAMA_MAX_LOADED_MODELS`, `OLLAMA_KEEP_ALIVE` (how long a model stays in memory) and `OLLAMA_CONTEXT_LENGTH`. A `Modelfile` pins a base model with parameters such as `num_ctx` and a system prompt.

> **Pitfall:** Ollama's default context window is small for agents. If you do not raise it (`OLLAMA_CONTEXT_LENGTH` or `num_ctx`), long tool results are silently cut from the start of the conversation and the agent "forgets" its instructions.

**llama.cpp** is the C++ engine underneath. Its `llama-server` serves GGUF files on CPU, Apple Silicon or GPU with an OpenAI-compatible API: `llama-server -m qwen3-8b-q4_k_m.gguf --jinja -c 16384 -ngl 99 --port 8080`. `--jinja` applies the model's own chat template, which tool calling needs; `-ngl` offloads layers to the GPU; `-c` sets the context.

**TGI** (Text Generation Inference) is Hugging Face's server, with a Messages API compatible with Chat Completions and a Docker image (`ghcr.io/huggingface/text-generation-inference`). It is now in maintenance mode: it receives fixes but no new features, and its own README points new deployments to vLLM or SGLang. Keep existing TGI services running if they work; do not start new ones on it.

| Engine | Best for | Hardware | Concurrency |
|---|---|---|---|
| vLLM | Production serving to many users | NVIDIA, AMD, others via plugins | High: continuous batching, paged cache |
| SGLang | Production, heavy prefix reuse, structured output | NVIDIA, AMD | High: radix-tree prefix cache |
| Ollama | Developers, small teams, many small models | CPU, Apple Silicon, consumer GPUs | Low to moderate |
| llama.cpp | CPU or edge, single binary, GGUF | Anything | Low to moderate |
| TGI | Existing deployments only (maintenance mode) | NVIDIA, AMD, others | High |

[[fig:platforms_paged]]

::: deeper
**PagedAttention.** Before vLLM, servers reserved one contiguous block of cache per request, sized for the maximum length. Most of it sat empty, and the gaps between blocks could not be reused. The vLLM paper (Kwon et al., SOSP 2023) borrowed virtual memory from operating systems: the cache is split into small fixed-size blocks (16 tokens by default), each sequence has a block table mapping its logical blocks to physical ones, and blocks are allocated only as tokens are generated. Waste drops to the last partly filled block. Sequences that share a prefix, such as the same system prompt and tool schemas, can point to the same physical blocks, with copy-on-write when they diverge. The paper reports 2 to 4 times the throughput of the systems it compared at the same latency.

**Continuous batching.** A GPU is most efficient when it processes many sequences in one step. Static batching waits until a batch is full and until its longest sequence finishes, so short answers wait for long ones. Continuous (iteration-level) batching, introduced by the Orca paper (OSDI 2022), rebuilds the batch at every decoding step: finished sequences leave, waiting ones join. vLLM's scheduler also splits long prompts into chunks (chunked prefill) so a 30,000-token prompt does not stall everyone else's tokens. When the cache is full, it pauses (preempts) sequences and resumes them later, which shows up as latency spikes.

**Prefill and decode.** Reading the prompt (prefill) is compute-bound and fast per token; generating (decode) is limited by memory bandwidth, because every new token reads all the weights. A rough ceiling for one sequence is bandwidth divided by weight size: an H100's roughly 3.35 TB/s over 37 GiB of INT4 weights gives about 85 tokens per second. Batching shares that read across many sequences, which is why throughput grows with users until the cache runs out. Agents are prefill-heavy (long contexts, short tool calls), so **prefix caching** pays off: the system prompt and tool schemas are identical across requests, and vLLM reuses their cached blocks automatically in its current engine. Keep the stable part of the prompt first and byte-identical to get the benefit.

**Mixture of experts.** A mixture-of-experts (MoE) model activates only some of its weights per token. Memory follows the total parameter count; speed follows the active count. A model with 30 billion total and 3 billion active parameters needs memory like a 30B model but decodes like a 3B one.

**Scaling past one GPU.** Tensor parallelism splits each layer across GPUs in one node and needs fast links (NVLink). Pipeline parallelism places different layers on different GPUs or nodes. Data parallelism runs independent replicas behind a load balancer, which is how you scale users once one replica fits. On Kubernetes, the vLLM production stack and llm-d add cache-aware routing and separate prefill and decode workers.

**Watching it.** vLLM exports Prometheus metrics at `/metrics`: running and waiting requests, KV cache usage, time to first token and time per output token. The waiting-requests count and cache usage are the two numbers to alert on and to autoscale from.

> **Interview:** "How would you size GPUs for a 70B model serving 30 concurrent agent sessions with 16k context?" Weights first: 70B in BF16 is about 140 GB, in INT4 about 37 GiB. Then cache: Llama-70B-shaped models need about 320 KiB per token in BF16, so 30 sessions at 16k tokens is 30 x 16,384 x 320 KiB, about 150 GiB. BF16 weights plus that cache is about 280 GiB, more than four 80 GB GPUs leave after overhead, so it needs eight GPUs or a smaller footprint. With INT4 weights and an FP8 cache (37 plus 75 GiB, about 112 GiB) it fits on two 80 GB GPUs with tensor parallelism. Then I would load-test with real agent traces, watching time to first token and waiting requests, and re-run our tool-call tests on the quantised model.

> **Interview:** "Why does vLLM serve more users than a naive server on the same GPU?" PagedAttention allocates the KV cache in small blocks on demand, so almost no memory is wasted and shared prefixes are stored once; continuous batching adds and removes sequences at every step, so the GPU is always full; chunked prefill keeps long prompts from blocking short ones. More of the GPU's memory and time go to useful tokens.
:::

::: check
?? A team serves an 8B model on a 24 GB GPU and users complain that long agent runs are slow and sometimes queue. What is the likely cause?
The KV cache. After about 15 GiB of BF16 weights, only a few GiB are left, enough for a handful of long sequences. Requests wait or are preempted. Quantise the weights, store the cache in FP8, lower `--max-model-len`, or use a bigger GPU.

?? Tool calls from a new model arrive as JSON inside the message text, with `tool_calls` empty. What do you check first?
That vLLM was started with `--enable-auto-tool-choice` and a `--tool-call-parser` matching the model family (and the right chat template). A parser mismatch is the usual cause.

?? When would you choose Ollama over vLLM?
For developers' laptops, small shared servers, CPU or Apple Silicon machines, or many small models used lightly. For many concurrent users on GPUs, vLLM's batching and paged cache give far higher throughput.
:::

::: resources
- Docs: [vLLM documentation](https://docs.vllm.ai/en/latest/) · serving, flags, quantisation, metrics
- Paper: [Efficient Memory Management for Large Language Model Serving with PagedAttention](https://arxiv.org/abs/2309.06180) · the vLLM paper
- Paper: [Orca: A Distributed Serving System for Transformer-Based Generative Models](https://www.usenix.org/conference/osdi22/presentation/yu) · iteration-level (continuous) batching
- Docs: [Tool calling in vLLM](https://docs.vllm.ai/en/latest/features/tool_calling.html) · tool parsers per model family
- Docs: [Ollama documentation](https://docs.ollama.com/) · local serving, Modelfile, OpenAI compatibility
- Repo: [llama.cpp](https://github.com/ggml-org/llama.cpp) · GGUF, `llama-server`, quantisation types
- Paper: [AWQ: Activation-aware Weight Quantization](https://arxiv.org/abs/2306.00978) · how 4-bit weight quantisation keeps quality
:::

### LLM gateways and model routing: LiteLLM and alternatives

::: plain
A gateway is a single front door for every model in the company. Agents call one address with one kind of request, and the gateway decides which model server actually answers, tries another one if the first is down, checks that the caller has a key and budget, and writes down who used what. It is the same idea as a connection pooler in front of PostgreSQL, or a Trino coordinator in front of many catalogs: callers see one simple endpoint, and the messy, expensive back end is managed in one place.
:::

**Why a gateway exists.** Without one, every agent holds its own model URLs and API keys, its own retry code and its own idea of cost. When the vLLM node is drained for maintenance, every agent breaks. When finance asks which team spent what on the hosted Claude API, nobody knows. When security asks where the Anthropic key lives, the answer is "in eleven repositories". A gateway centralises five jobs:

1. **One API.** Agents speak one format (usually OpenAI Chat Completions) whatever the back end: vLLM, Ollama, TGI, a hosted Claude or Gemini model.
2. **Routing and fallbacks.** A logical name such as `analyst` maps to one or more deployments; the gateway load-balances, retries, and falls back to another model when one fails or the context is too long.
3. **Keys and budgets.** Each team or agent gets its own virtual key with allowed models, rate limits and a spending cap. The real provider keys stay in the gateway.
4. **Logging and cost.** Every call is recorded with caller, model, tokens, latency and cost, and traces go to Langfuse, Phoenix or an OpenTelemetry collector.
5. **Policy.** One place to add guardrails, redact personal data or block a model.

[[fig:platforms_gateway]]

**LiteLLM** is the most widely used open-source gateway. It comes in two forms: a Python SDK (`litellm.completion`, `Router`) that you embed, and the **LiteLLM Proxy**, a server you deploy once for the whole company. The proxy is configured with one YAML file:

```yaml
# litellm-config.yaml
model_list:
  - model_name: analyst                          # the name agents ask for
    litellm_params:
      model: hosted_vllm/analyst                 # provider prefix / --served-model-name
      api_base: http://vllm-a.models.svc:8000/v1
      api_key: os.environ/VLLM_API_KEY           # read from the environment, never inline
      input_cost_per_token: 0.0000002            # internal GPU price, for chargeback
      output_cost_per_token: 0.0000008
  - model_name: analyst                          # same name again: a second replica, load-balanced
    litellm_params:
      model: hosted_vllm/analyst
      api_base: http://vllm-b.models.svc:8000/v1
      api_key: os.environ/VLLM_API_KEY
  - model_name: analyst-small
    litellm_params:
      model: ollama_chat/qwen3:8b
      api_base: http://ollama.models.svc:11434
  - model_name: claude                           # hosted model, only where policy allows
    litellm_params:
      model: anthropic/your-claude-model-id
      api_key: os.environ/ANTHROPIC_API_KEY

router_settings:
  routing_strategy: least-busy                   # or simple-shuffle, latency-based-routing
  num_retries: 2
  timeout: 120
  allowed_fails: 3                               # failures before a deployment is cooled down
  cooldown_time: 30
  fallbacks: [{"analyst": ["analyst-small"]}]
  context_window_fallbacks: [{"analyst-small": ["analyst"]}]
  redis_host: os.environ/REDIS_HOST              # shared state when you run several proxies
  redis_port: 6379

litellm_settings:
  callbacks: ["otel"]                            # OpenTelemetry traces to your collector
  drop_params: true                              # drop parameters a back end does not support

general_settings:
  master_key: os.environ/LITELLM_MASTER_KEY      # admin key; virtual keys are derived from it
  database_url: os.environ/DATABASE_URL          # PostgreSQL: keys, teams, spend logs
```

Start it with `litellm --config litellm-config.yaml --port 4000` or the official container image pinned to a release tag. Agents now use `base_url="http://litellm.platform.svc:4000/v1"` and their own virtual key; nothing else in the agent changes. The proxy also exposes an Anthropic-style `/v1/messages` endpoint, which is one way to give the Claude Agent SDK access to models that only speak the OpenAI format.

**Keys, teams and budgets.** With a database configured, an admin creates teams and keys through the proxy's API or admin UI:

```bash
curl -s http://litellm:4000/key/generate \
  -H "Authorization: Bearer $LITELLM_MASTER_KEY" -H "Content-Type: application/json" \
  -d '{"models": ["analyst", "analyst-small"], "team_id": "sales-analytics",
       "max_budget": 50, "budget_duration": "30d",
       "tpm_limit": 200000, "rpm_limit": 120, "max_parallel_requests": 8,
       "metadata": {"agent": "sales-analyst", "owner": "data-platform"}}'
```

The response contains a new key starting with `sk-`. A call with a model not in `models` is rejected; once spend in the 30-day window passes 50 USD, calls fail with a budget error until the window resets. For self-hosted models the "cost" is whatever internal price you set, which is how platform teams charge GPU time back to the teams that use it.

**Fallbacks with the SDK.** The same routing logic runs in-process with `litellm.Router`. This example uses LiteLLM's mock responses, so the first deployment fails like a dead GPU node and no model is called (run with `litellm` 1.110.3):

```python
from litellm import Router

router = Router(
    model_list=[
        {"model_name": "analyst",                       # the name clients ask for
         "litellm_params": {"model": "hosted_vllm/qwen-analyst",
                            "api_base": "http://vllm-a:8000/v1",
                            "mock_response": Exception("GPU node down")}},   # simulate an outage
        {"model_name": "analyst-small",
         "litellm_params": {"model": "ollama_chat/llama3.2",
                            "api_base": "http://ollama:11434",
                            "mock_response": "US had the most revenue in October 2026."}},
    ],
    fallbacks=[{"analyst": ["analyst-small"]}],          # when analyst fails, try analyst-small
    num_retries=0,
)

resp = router.completion(model="analyst",
                         messages=[{"role": "user", "content": "Which region had the most revenue?"}])
print(resp.model)
print(resp.choices[0].message.content)
# llama3.2
# US had the most revenue in October 2026.
```

**What a gateway does, from scratch.** To make the moving parts concrete, here is the core of any gateway in plain Python: authenticate the key, check its allowed models and budget, walk the route list until a deployment answers, record cost and a log row.

```python
# mini_gateway.py: what a gateway does on every request, in 40 lines
import time

PRICE = {"vllm/qwen": 0.0, "ollama/llama": 0.0, "claude": 3.00 / 1e6}   # USD per token (illustrative)
ROUTES = {"analyst": ["vllm/qwen", "ollama/llama", "claude"]}          # primary, then fallbacks
KEYS = {"sk-team-sales": {"team": "sales", "budget": 0.01, "spent": 0.0, "models": {"analyst"}}}
LOG = []

DOWN = {"vllm/qwen"}                               # pretend the GPU node is down

class Down(Exception): pass

def backend(name, messages):                       # stands in for the HTTP call to each server
    if name in DOWN:
        raise Down(f"503 from {name}")
    return {"content": f"answer from {name}", "tokens": 1200}

def complete(api_key, model, messages):
    key = KEYS.get(api_key)
    if key is None or model not in key["models"]:
        return {"status": 401, "error": "unknown key or model not allowed"}
    if key["spent"] >= key["budget"]:
        return {"status": 429, "error": "budget exceeded"}
    for target in ROUTES[model]:
        start = time.perf_counter()
        try:
            out = backend(target, messages)
        except Down as e:
            LOG.append((key["team"], target, "fail", str(e)))
            continue                               # try the next deployment
        cost = out["tokens"] * PRICE[target]
        key["spent"] += cost
        LOG.append((key["team"], target, "ok", f"{cost:.4f} USD, {time.perf_counter() - start:.3f}s"))
        return {"status": 200, "model": target, "content": out["content"]}
    return {"status": 502, "error": "all deployments failed"}

ask = [{"role": "user", "content": "Which region had the most revenue?"}]
print(complete("sk-team-sales", "analyst", ask))
DOWN.add("ollama/llama")                           # now the local fallback is down too
for _ in range(4):
    r = complete("sk-team-sales", "analyst", ask)
    print(r["status"], r.get("model", r.get("error")))
print(f"spent {KEYS['sk-team-sales']['spent']:.4f} USD; log rows: {len(LOG)}")
# {'status': 200, 'model': 'ollama/llama', 'content': 'answer from ollama/llama'}
# 200 claude
# 200 claude
# 200 claude
# 429 budget exceeded
# spent 0.0108 USD; log rows: 11
```

Notice two real-world behaviours. The budget is checked before the call, so the last allowed request pushes spend past the cap (0.0108 against 0.01): budgets in real gateways are soft by one request too. And once both local models were down, traffic silently moved to a hosted model. That may be exactly what you want, or a data-residency breach, which is why fallback chains are a policy decision.

> **Pitfall:** A fallback to a different model family is not free. Prompts, tool-call formats and output quality differ. If `analyst` is a 32B Qwen model and the fallback is an 8B Llama, the agent may start making tool-call mistakes during the outage. Test the agent against every model in its fallback chain, and never fall back from an on-prem model to a hosted one for data that may not leave the network.

> **Tip:** Give every agent its own virtual key, even when one team owns several agents. Spend, rate limits and logs are then per agent, and you can revoke one runaway agent without touching the others.

**Alternatives.**

| Option | What it is | When to pick it |
|---|---|---|
| LiteLLM Proxy | Open-source Python gateway, 100+ providers, keys, budgets, admin UI | Default choice for a mixed fleet of local and hosted models |
| Portkey Gateway | Open-source gateway (TypeScript) with routing, retries, guardrails | Similar needs, lighter runtime; hosted control plane optional |
| Envoy AI Gateway | Envoy-based gateway for LLM traffic on Kubernetes | You already run Envoy or Istio and want gateway policy in the mesh |
| Kong AI Gateway | AI plugins for the Kong API gateway | Kong is already your company's API gateway |
| Gateway API Inference Extension, vLLM production stack, llm-d | Kubernetes routing that understands model servers (queue depth, KV cache, LoRA adapters) | Routing between many vLLM replicas of the same model; pairs with a LiteLLM-style gateway in front |
| Your own thin proxy | FastAPI plus the logic above | Very few models and strict audit needs; you will rebuild features slowly |

::: deeper
**The gateway is a single point of failure, so run it like one.** Run at least two proxy replicas behind a Kubernetes Service, with PostgreSQL for keys and spend and Redis for state that must be shared across replicas (rate-limit counters, cooldowns). Without Redis, each replica counts requests per minute separately and limits are effectively multiplied by the replica count. The proxy adds a few milliseconds per call, which is noise next to model latency, but it must stream: check that it passes server-sent events through without buffering, or every agent loses token streaming.

**Routing strategies.** `simple-shuffle` spreads requests at random (weighted if you set weights). `least-busy` sends to the deployment with the fewest in-flight requests, a good default for self-hosted replicas with similar GPUs. `latency-based-routing` favours the fastest recent responder. Usage-based strategies spread load by tokens per minute against each deployment's limits, which matters for hosted APIs with quotas. For many replicas of one vLLM model, model-aware routers that read each server's queue and cache (the Kubernetes Gateway API Inference Extension, llm-d, the vLLM production stack router) beat any generic strategy, because they can send a conversation back to the replica that already holds its prefix in cache.

**Kinds of fallback.** A general fallback covers errors and timeouts. A context-window fallback sends a request that is too long for one model to a model with a larger window. A content-policy fallback covers a provider refusing a request. Cooldowns take a failing deployment out of rotation for a period after a number of failures, which is a circuit breaker.

**Budgets and cost.** Hosted model costs come from LiteLLM's price table; self-hosted costs come from prices you set per token. Spend is written to PostgreSQL, so you can query it with SQL and join it to your own data: cost per agent per day is one `GROUP BY` away. Budgets can be set per key, per user, per team and per customer.

**Supply chain.** The gateway holds every provider key, so it is the most valuable target in the stack. In March 2026 two LiteLLM releases on PyPI (1.82.7 and 1.82.8) were published by attackers with a credential-stealing payload, after a CI dependency leaked the publishing token; LiteLLM's security update describes it. The lessons are general: pin exact versions with hashes, install from your own package mirror after scanning, prefer signed release images, run the proxy with only the secrets it needs, and rotate keys if you ever installed a bad release.

**Licence notes.** The LiteLLM core and proxy are open source under the MIT licence; a set of enterprise features (for example some SSO, audit and advanced guardrail features) needs a commercial licence. Check which features you rely on before you design around them.

> **Interview:** "Why put a gateway between agents and models?" Agents should not know where models run. A gateway gives one OpenAI-compatible API over vLLM, Ollama and hosted models, so we can move or replace a model without redeploying agents. It holds the real provider keys and hands out per-agent virtual keys with model allow-lists, rate limits and budgets. It retries, load-balances and falls back with cooldowns. And it logs every call with tokens, cost and latency, feeding OpenTelemetry traces and chargeback. We run two or more replicas with PostgreSQL and Redis, and fallback chains are reviewed like policy, because falling back to a hosted model can move data out of the network.

> **Interview:** "Your vLLM node goes down. What happens to the agents?" The gateway's health checks and failure counts put that deployment in cooldown and route `analyst` to the other replicas. If none are left, it uses the configured fallback, for example a smaller local model with a larger timeout. Agents see slower or lower-quality answers, not errors. Alerts fire on the cooldown and on the waiting-requests metric so someone fixes the node.
:::

::: check
?? Two LiteLLM proxy replicas each enforce `rpm_limit: 120` for a key, but the key is getting about 240 requests per minute through. Why?
Without a shared Redis, each replica counts requests in its own memory, so the limit applies per replica. Configure Redis in `router_settings` so the counters are shared.

?? What is the difference between a general fallback and a context-window fallback?
A general fallback handles errors and timeouts from a deployment. A context-window fallback handles requests that are too long for a model's context by sending them to a model with a larger window.

?? Why should each agent have its own virtual key?
So spend, rate limits, logs and model allow-lists are tracked per agent, and one misbehaving agent can be throttled or revoked without affecting others.
:::

::: resources
- Docs: [LiteLLM Proxy (AI gateway)](https://docs.litellm.ai/docs/simple_proxy) · overview and quick start
- Docs: [LiteLLM routing, load balancing and fallbacks](https://docs.litellm.ai/docs/routing) · `Router`, strategies, cooldowns
- Docs: [LiteLLM virtual keys](https://docs.litellm.ai/docs/proxy/virtual_keys) · keys, model access, PostgreSQL
- Docs: [LiteLLM budgets and rate limits](https://docs.litellm.ai/docs/proxy/users) · per key, user and team budgets
- Blog: [LiteLLM security update, March 2026](https://docs.litellm.ai/blog/security-update-march-2026) · the PyPI supply-chain incident
- Docs: [Gateway API Inference Extension](https://gateway-api-inference-extension.sigs.k8s.io/) · model-aware routing on Kubernetes
- Docs: [Envoy AI Gateway](https://aigateway.envoyproxy.io/) · LLM gateway on Envoy
:::

### Deploying agents with Docker and Kubernetes

::: plain
An agent in a notebook helps one person. To help a hundred analysts it has to become a service: a small web server that accepts a question, runs the agent loop and sends back the answer, packed into a container so it runs the same everywhere. Kubernetes then runs many copies of that container, restarts them when they crash and adds copies when traffic grows. Two things make agents different from an ordinary web API: answers arrive slowly, so you stream progress as it happens, and some jobs take an hour, so they run as background jobs instead of inside a web request.
:::

**The agent as an HTTP service.** FastAPI is the usual choice in Python: async, typed request bodies with Pydantic, and streaming responses. The service below wraps the sales analyst's loop. `POST /ask` returns a stream of **server-sent events** (SSE): a simple HTTP format where the server keeps the response open and writes `event:` and `data:` lines as things happen. The client sees "calling run_sql", then the rows count, then the answer word by word, instead of a spinner for twenty seconds. A scripted model stands in for vLLM so the code runs anywhere (run with `fastapi` 0.165.0):

```python
# service.py: the sales analyst as an HTTP service with streaming
import asyncio, json, os, uuid
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from shop import run_sql, INSTRUCTIONS

MAX_STEPS = int(os.getenv("AGENT_MAX_STEPS", "5"))
app = FastAPI(title="sales-analyst")

class Ask(BaseModel):
    question: str

async def scripted_model(messages):
    """Stands in for the model server; returns OpenAI-shaped assistant messages."""
    await asyncio.sleep(0)                            # a real call awaits the network here
    if messages[-1]["role"] == "user":
        return {"role": "assistant", "content": None, "tool_calls": [{"id": "c1", "type": "function",
                "function": {"name": "run_sql", "arguments": json.dumps({"sql":
                "SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region ORDER BY revenue DESC"})}}]}
    return {"role": "assistant", "content": "US had the most revenue in October 2026 (390.5)."}

call_model = scripted_model                           # production: an AsyncOpenAI client to vLLM

def sse(event, data):                                 # one Server-Sent Events frame
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"

async def run_agent(question, request):
    run_id = uuid.uuid4().hex[:8]
    messages = [{"role": "system", "content": INSTRUCTIONS}, {"role": "user", "content": question}]
    yield sse("start", {"run_id": run_id})
    for step in range(MAX_STEPS):
        if await request.is_disconnected():           # user closed the tab: stop spending tokens
            return
        msg = await call_model(messages)
        messages.append(msg)
        if not msg.get("tool_calls"):
            for word in msg["content"].split(" "):     # a real server streams tokens; we fake it
                yield sse("token", {"text": word + " "})
            yield sse("done", {"run_id": run_id, "steps": step + 1})
            return
        for call in msg["tool_calls"]:
            args = json.loads(call["function"]["arguments"])
            yield sse("tool_call", {"name": call["function"]["name"], "args": args})
            result = await asyncio.to_thread(run_sql, **args)   # keep the event loop free
            yield sse("tool_result", {"rows": result.count("\n")})
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": result})
    yield sse("error", {"run_id": run_id, "reason": "step limit reached"})

@app.post("/ask")
async def ask(body: Ask, request: Request):
    return StreamingResponse(run_agent(body.question, request), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/healthz")                                  # liveness: the process answers
def healthz():
    return {"ok": True}

@app.get("/readyz")                                   # readiness: dependencies answer
def readyz():
    return {"ready": run_sql("SELECT 1 AS one").startswith("one")}
```

Test it without a server, using FastAPI's `TestClient`:

```python
from fastapi.testclient import TestClient
from service import app

client = TestClient(app)
print(client.get("/readyz").json())
with client.stream("POST", "/ask", json={"question": "Which region had the most revenue?"}) as r:
    print(r.headers["content-type"])
    for line in r.iter_lines():
        if line.startswith("event:"):
            print(line[7:], end=" ")
print()
# {'ready': True}
# text/event-stream; charset=utf-8
# start tool_call tool_result token token token token token token token token token done
```

In production, swap `call_model` for a real client pointed at vLLM or, better, at the gateway. Because the gateway speaks the OpenAI API, the swap is a few lines; this version also ran through the same test against the scripted OpenAI-compatible server:

```python
# model_client.py: the production call_model, pointed at vLLM (or the LiteLLM gateway)
import os
from openai import AsyncOpenAI

llm = AsyncOpenAI(base_url=os.getenv("MODEL_URL", "http://localhost:8000/v1"),
                  api_key=os.getenv("MODEL_API_KEY", "unused"), timeout=60, max_retries=2)
TOOLS = [{"type": "function", "function": {"name": "run_sql",
          "description": "Run one read-only SELECT on the orders table.",
          "parameters": {"type": "object", "properties": {"sql": {"type": "string"}}, "required": ["sql"]}}}]

async def vllm_model(messages):
    r = await llm.chat.completions.create(model=os.getenv("MODEL_NAME", "local-model"),
                                          messages=messages, tools=TOOLS, temperature=0)
    return r.choices[0].message.model_dump(exclude_none=True)
```

Set `service.call_model = model_client.vllm_model` at start-up. To stream the model's own tokens rather than faking it, pass `stream=True` and forward each chunk's `delta.content` as a `token` event, collecting `delta.tool_calls` pieces until the stream ends.

> **Pitfall:** Blocking calls inside `async` code freeze every stream in that worker. A synchronous database driver, `requests`, or a slow `run_sql` called directly in the event loop stops all other users' tokens. Use async clients, or push blocking work to a thread with `asyncio.to_thread`, as above.

**The container.** Keep the image small, reproducible and unprivileged:

```text
# Dockerfile
FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt   # pinned, hashed versions
COPY service.py model_client.py shop.py ./
RUN useradd --uid 10001 agent
USER 10001
EXPOSE 8080
CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8080", "--timeout-graceful-shutdown", "120"]
```

The model is not in this image: the agent is a few hundred megabytes and talks to the model server over the network. Model servers are separate images (the official `vllm/vllm-openai` image, `ollama/ollama`) with weights mounted from a volume, never baked into the agent.

**On Kubernetes.** The agent is a normal stateless Deployment. The parts that matter for agents are the probes, the long shutdown and the autoscaler:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: {name: sales-analyst, namespace: agents}
spec:
  replicas: 2
  selector: {matchLabels: {app: sales-analyst}}
  template:
    metadata: {labels: {app: sales-analyst}}
    spec:
      serviceAccountName: sales-analyst          # its own identity (next topic)
      terminationGracePeriodSeconds: 150         # let open streams finish on rollout
      containers:
        - name: agent
          image: registry.internal/agents/sales-analyst:1.4.2
          ports: [{containerPort: 8080}]
          env:
            - {name: MODEL_URL, value: "http://litellm.platform.svc:4000/v1"}
            - {name: MODEL_NAME, value: "analyst"}
            - name: MODEL_API_KEY
              valueFrom: {secretKeyRef: {name: sales-analyst-llm, key: api-key}}
          resources:
            requests: {cpu: "250m", memory: "512Mi"}
            limits: {memory: "1Gi"}
          readinessProbe: {httpGet: {path: /readyz, port: 8080}, periodSeconds: 10}
          livenessProbe: {httpGet: {path: /healthz, port: 8080}, periodSeconds: 20}
          securityContext: {runAsNonRoot: true, readOnlyRootFilesystem: true, allowPrivilegeEscalation: false}
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: sales-analyst, namespace: agents}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: sales-analyst}
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource: {name: cpu, target: {type: Utilization, averageUtilization: 60}}
  behavior:
    scaleDown: {stabilizationWindowSeconds: 300}   # do not kill pods mid-stream on a dip
```

The model server is the opposite: stateful in practice, slow to start, and expensive. A vLLM Deployment requests GPUs (`nvidia.com/gpu: 1`, with the NVIDIA device plugin or GPU Operator installed), mounts the weights from a persistent volume, gives `/dev/shm` enough room for tensor parallelism, and uses a **startup probe** with a long allowance, because loading 40 GiB of weights and capturing CUDA graphs takes minutes. Liveness checks that start too early kill the pod in a loop.

> **Pitfall:** Autoscaling agents on CPU is weak, and autoscaling vLLM on CPU is wrong. An agent pod spends most of its time waiting on the model, so its CPU stays low while users queue. Scale agents on in-flight requests and scale model servers on vLLM's waiting-requests or KV cache metrics, through the Prometheus adapter or KEDA. And remember that the agents' ceiling is the GPUs: adding agent pods beyond what the model servers can serve only moves the queue.

**Long-running work.** A web request should end in seconds or a few minutes; proxies, load balancers and browsers drop idle connections. An agent that profiles 400 tables or backfills documentation runs for an hour. Do not hold an HTTP stream open for that. Use the same pattern as any batch job:

1. `POST /runs` validates the request, writes a row to a `runs` table in PostgreSQL with status `queued`, and returns `202 Accepted` with a `run_id` at once.
2. A worker picks it up: a pod reading a queue (Kafka, Redis, or a `SELECT ... FOR UPDATE SKIP LOCKED` on the table) or a **Kubernetes Job** created per run.
3. The agent checkpoints after each step (a LangGraph PostgreSQL checkpointer, or your own table), so a crashed pod resumes instead of starting over.
4. Clients poll `GET /runs/{id}` or subscribe to its events; the final result goes to the table or object storage (MinIO).

```yaml
apiVersion: batch/v1
kind: Job
metadata: {name: profile-run-7f3a, namespace: agents, labels: {run-id: "7f3a"}}
spec:
  backoffLimit: 2                      # retry a crashed pod twice, resuming from its checkpoint
  activeDeadlineSeconds: 7200          # hard stop after two hours, whatever happens
  ttlSecondsAfterFinished: 86400       # clean up a day later
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: profiling-agent
      containers:
        - name: agent
          image: registry.internal/agents/profiler:2.0.1
          args: ["--run-id", "7f3a", "--max-steps", "400"]
          resources: {requests: {cpu: "500m", memory: "1Gi"}}
```

Recurring agent work (a nightly data-quality review) is a `CronJob`, or better an Airflow task using `KubernetesPodOperator` when it belongs to a pipeline with upstream and downstream dependencies.

[[fig:platforms_k8s]]

::: deeper
**SSE or WebSockets.** SSE is one-way (server to client), plain HTTP, works through most proxies, and browsers reconnect automatically with `Last-Event-ID`. WebSockets are two-way, which you need only when the user must interrupt or answer mid-run (approvals, voice). Most agent UIs use SSE for output and a separate `POST` for input such as an approval. MCP's Streamable HTTP transport and the A2A protocol both stream over SSE as well ([Model Context Protocol and agent interoperability](#mcp)).

**Streaming through infrastructure.** Every hop can buffer: NGINX (`proxy_buffering off`, or the `X-Accel-Buffering: no` header the service sends), ingress controllers, service meshes and the gateway. Idle timeouts (often 60 seconds) cut long tool calls; send a comment line (`: ping`) every 15 seconds as a heartbeat. Disable response compression for `text/event-stream` in middleware, because compressors buffer.

**Graceful shutdown.** On a rollout, Kubernetes sends SIGTERM and removes the pod from the Service. Uvicorn stops accepting new requests and waits up to `--timeout-graceful-shutdown` for open ones; `terminationGracePeriodSeconds` must be longer, or open streams are killed mid-answer. A short `preStop` sleep gives load balancers time to notice.

**Workers and concurrency.** One async Uvicorn worker can hold hundreds of open streams, because they are waiting on the network. Run one worker per container and scale with replicas, rather than many workers per pod, so Kubernetes sees the real load. Put a concurrency limit in the service (an `asyncio.Semaphore`) that matches what the model servers can take, and return `429` with `Retry-After` beyond it rather than queueing invisibly.

**State.** Keep agent pods stateless: conversations, checkpoints and run status go to PostgreSQL (or Redis for short-lived sessions), files to MinIO or another object store. Then any replica can serve any request, rollouts are safe and autoscaling works. Sticky sessions are a smell, except toward model servers, where routing a conversation back to the replica holding its prefix cache is an optimisation the model-aware routers do for you.

**GPU scheduling.** GPUs are whole-device resources by default. NVIDIA MIG splits an A100 or H100 into isolated slices for small models; time-slicing shares a GPU without isolation. Node pools with taints keep CPU work off GPU nodes. KServe and the vLLM production stack package these patterns; llm-d adds disaggregated prefill and decode on Kubernetes.

**Agents as data pipelines.** Many "agents" in a data team are batch jobs: classify last night's tickets, document new tables. Those belong in Airflow or Spark, with the model call as one step, retries and backfills handled by the scheduler, and the model reached through the gateway. Reserve the always-on HTTP service for interactive agents ([Agents for data engineering](#dataagents)).

> **Interview:** "How do you deploy an agent that sometimes runs for an hour?" Split interactive and long work. Interactive questions use a FastAPI service with SSE streaming on a Deployment with an HPA, probes and a long termination grace period. Long runs are submitted with `POST /runs`, which returns a run id at once; a Kubernetes Job or queue worker runs the agent with a step limit, a deadline (`activeDeadlineSeconds`), and a checkpoint in PostgreSQL after every step, so a retry resumes. Status and results go to PostgreSQL and MinIO, and the client polls or subscribes. Model capacity, not agent pods, is the real limit, so concurrency is capped to what vLLM can serve.

> **Interview:** "Why not just call the model from Airflow?" For scheduled, known steps, you should: an Airflow task calling the gateway is simpler and gets retries, backfills and lineage for free. A long-lived agent service is for interactive, open-ended work where users ask questions and expect streamed answers.
:::

::: check
?? Users say answers appear all at once after 30 seconds, although the service streams SSE. Name two likely causes.
A proxy or ingress buffering the response (NGINX `proxy_buffering`, a mesh, the gateway), or compression middleware buffering `text/event-stream`. Also check that the model call itself uses `stream=True`.

?? Why is CPU a poor autoscaling signal for agent pods?
Agent pods mostly wait on network calls to the model and tools, so CPU stays low while requests queue. In-flight requests, queue length or latency reflect load; for model servers, waiting requests and KV cache usage do.

?? What three Job settings protect you from a runaway long agent run?
`activeDeadlineSeconds` (hard time limit), `backoffLimit` (bounded retries) and a step limit passed to the agent itself; `ttlSecondsAfterFinished` cleans up afterwards.
:::

::: resources
- Docs: [FastAPI custom responses and StreamingResponse](https://fastapi.tiangolo.com/advanced/custom-response/) · streaming responses
- Docs: [Using server-sent events (MDN)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events) · the SSE format and reconnection
- Docs: [Kubernetes Horizontal Pod Autoscaling](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) · metrics and scaling behaviour
- Docs: [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/) · backoff, deadlines, TTL
- Docs: [Liveness, readiness and startup probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/) · probes for slow-starting model servers
- Repo: [vLLM production stack](https://github.com/vllm-project/production-stack) · Helm charts, router and monitoring for vLLM on Kubernetes
- Docs: [KEDA](https://keda.sh/) · event-driven autoscaling from Prometheus or queue metrics
:::

## Enterprise and cloud

The first group made the agent run. This one makes it acceptable to a security team and, if the company wants it, runs it on a managed cloud platform. In an enterprise the hard questions are not about models: who is this agent, whose rights does it use when it queries the warehouse, where are its passwords, what can it reach on the network, and can it all run with no internet at all.

The last topic is the only one in the course dedicated to Google Cloud. It is there because many companies mix on-prem and cloud, and because "would you use a managed agent runtime?" is a common interview question. Everything before it runs without any cloud.

### Agent identity, secrets and networking in the enterprise

::: plain
An agent is a new kind of employee that never sleeps and does exactly what text tells it, including text written by an attacker. So it gets its own badge (an identity), it may only open the doors the person it is helping could open (acting on behalf of the user), it never carries keys in its pocket (secrets come from a vault, for a short time), and it works in a room with locked exits (network rules). In an air-gapped site the building has no outside doors at all, so everything it needs, from model files to software packages, is carried in and checked first.
:::

**Three identities, not one.** Every agent request involves three principals, and mixing them up is the most common enterprise design flaw:

| Identity | What it is | On-prem mechanism |
|---|---|---|
| Workload identity | The agent service itself: "this is the sales-analyst agent, version 1.4" | Kubernetes ServiceAccount tokens, SPIFFE IDs issued by SPIRE, mTLS certificates |
| User identity | The human who asked | OpenID Connect (OIDC) sign-in through Keycloak or your corporate identity provider, backed by LDAP or Active Directory |
| Delegated identity | "the agent, acting for Ana" | OAuth 2.0 Token Exchange (RFC 8693): a short-lived token for one audience, carrying both the user and the actor |

**Acting on behalf of the user.** The rule is that an agent's effective rights are the **intersection** of what the user may do and what the agent is built to do. If the agent uses one powerful service account for every user, any analyst can ask it for data they could not query themselves: the classic **confused deputy**. Instead, the agent exchanges the user's sign-in token for a new token scoped to one tool, and every tool server checks that token itself. Real systems use JSON Web Tokens (JWTs) signed with asymmetric keys and verified against the identity provider's published keys; the mechanics are the same as in this standard-library sketch:

```python
# on_behalf_of.py: the agent acts with the user's rights, never more
import base64, hashlib, hmac, json, time

IDP_KEY = b"demo-signing-key"        # stands in for the identity provider's signing key (Keycloak)

def b64(b): return base64.urlsafe_b64encode(b).rstrip(b"=").decode()
def unb64(s): return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))

def issue(claims, ttl=300):          # what the IdP does at token exchange (RFC 8693)
    body = b64(json.dumps({**claims, "exp": int(time.time()) + ttl}).encode())
    return body + "." + b64(hmac.new(IDP_KEY, body.encode(), hashlib.sha256).digest())

def verify(token, audience):         # what every tool server does before acting
    body, sig = token.split(".")
    good = b64(hmac.new(IDP_KEY, body.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, good):
        raise PermissionError("bad signature")
    claims = json.loads(unb64(body))
    if claims["exp"] < time.time() or claims["aud"] != audience:
        raise PermissionError("expired or wrong audience")
    return claims

def run_sql_tool(token, sql):
    c = verify(token, audience="sql-tool")
    if "sales:read" not in c["scope"].split():
        return "error: missing scope sales:read"
    regions = ", ".join(f"'{r}'" for r in c["regions"])      # row filter from the user's claims
    return f"as {c['sub']} via {c['act']['sub']}: SELECT ... FROM ({sql}) WHERE region IN ({regions})"

# The user Ana signs in; the agent exchanges her token for one scoped to the SQL tool.
ana = issue({"sub": "ana", "aud": "sql-tool", "scope": "sales:read", "regions": ["EU"],
             "act": {"sub": "svc-sales-agent"}})               # "act": the agent acting for her
print(run_sql_tool(ana, "SELECT region, amount FROM orders"))
bob = issue({"sub": "bob", "aud": "sql-tool", "scope": "hr:read", "regions": [],
             "act": {"sub": "svc-sales-agent"}})
print(run_sql_tool(bob, "SELECT region, amount FROM orders"))
try:
    run_sql_tool(ana[:-2] + "xx", "SELECT 1")
except PermissionError as e:
    print("rejected:", e)
# as ana via svc-sales-agent: SELECT ... FROM (SELECT region, amount FROM orders) WHERE region IN ('EU')
# error: missing scope sales:read
# rejected: bad signature
```

Three checks happen at the tool, not in the agent: the signature (the token came from the identity provider), the audience (this token was minted for this tool, so a token stolen from one tool cannot be replayed at another), and the scope. The row filter here is string building for illustration only. In a real warehouse, push the user's identity into the database and let it enforce access: PostgreSQL row-level security policies keyed on a session setting, or Trino with user impersonation and its access-control rules ([Trino](index.html#engines)). The `act` claim records that the agent did it, which is what your audit log needs.

> **Pitfall:** Never let the model see or choose credentials. Tokens and passwords belong in the tool's code path, outside the conversation. If a secret appears in a prompt or tool result, it can be repeated in an answer, logged in a trace, or sent away by a prompt-injection attack.

MCP servers follow the same model. The MCP authorization spec uses OAuth 2.1 for remote servers: the client gets a token bound to that server's address (a resource indicator), and the server must not pass the token through to other services ([Model Context Protocol and agent interoperability](#mcp)).

**Secrets in Vault.** HashiCorp Vault (or its open-source fork OpenBao) is the usual on-prem secret store. The pattern for an agent on Kubernetes:

1. The pod authenticates to Vault with its Kubernetes ServiceAccount token (the **Kubernetes auth method**). No secret is needed to get secrets.
2. Vault maps that ServiceAccount to a policy that allows only what this agent needs.
3. For databases, Vault's **database secrets engine** creates a new PostgreSQL user for each lease, with only `SELECT` on the right tables and an expiry, and drops it when the lease ends. A leaked password is useless an hour later.
4. The Vault Agent injector, the Vault Secrets Operator or the External Secrets Operator delivers the secret to the pod as a file or Kubernetes Secret and renews it.

```bash
# One-time setup by the platform team
vault write database/roles/sales-readonly db_name=shop-pg \
  creation_statements="CREATE ROLE \"{{name}}\" LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}';
                       GRANT SELECT ON orders TO \"{{name}}\";" \
  default_ttl=1h max_ttl=4h

vault policy write sales-analyst - <<'HCL'
path "database/creds/sales-readonly" { capabilities = ["read"] }
path "secret/data/agents/sales-analyst/*" { capabilities = ["read"] }
HCL

vault write auth/kubernetes/role/sales-analyst \
  bound_service_account_names=sales-analyst bound_service_account_namespaces=agents \
  policies=sales-analyst ttl=1h

# What the agent's pod effectively does at start-up and before each lease expires
vault read database/creds/sales-readonly      # returns a fresh username, password and lease
```

The gateway key the agent uses (its LiteLLM virtual key) lives under `secret/agents/sales-analyst/`; the real provider keys live only in the gateway's own Vault path.

**Network isolation.** Assume the agent will one day be talked into trying something it should not. Network rules make sure it cannot reach anything useful when that happens. On Kubernetes, start with a default-deny policy and open only the paths the agent needs:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: sales-analyst-egress, namespace: agents}
spec:
  podSelector: {matchLabels: {app: sales-analyst}}
  policyTypes: [Ingress, Egress]
  ingress:
    - from: [{namespaceSelector: {matchLabels: {name: ingress}}}]
  egress:
    - to: [{namespaceSelector: {matchLabels: {name: platform}}, podSelector: {matchLabels: {app: litellm}}}]
      ports: [{port: 4000}]
    - to: [{namespaceSelector: {matchLabels: {name: data}}, podSelector: {matchLabels: {app: postgres}}}]
      ports: [{port: 5432}]
    - to: [{namespaceSelector: {matchLabels: {name: vault}}}]
      ports: [{port: 8200}]
    - to: [{namespaceSelector: {}, podSelector: {matchLabels: {k8s-app: kube-dns}}}]
      ports: [{port: 53, protocol: UDP}]
```

The agent can reach the gateway, the database, Vault and DNS, and nothing else: not the internet, not the HR database, not the model servers directly. If some agent must fetch web pages, send that traffic through an egress proxy with an allow-list of domains and full logging. A service mesh (Istio, Linkerd) adds mutual TLS between services, so each side proves its workload identity.

**Air-gapped setups.** In an air-gapped (fully disconnected) network, nothing can be downloaded at run time, so every artifact is brought in through a controlled transfer:

| Artifact | On the connected side | On the air-gapped side |
|---|---|---|
| Model weights | `hf download <repo> --revision <commit>`, record SHA-256 sums, scan, check the licence | Copy to a model store (NFS, MinIO); vLLM reads a local path with `HF_HUB_OFFLINE=1` |
| Container images | Pull pinned digests, scan, sign (cosign) | Push to an internal registry such as Harbor; admit only signed images |
| Python packages | Download wheels with hashes for an exact lock file | Serve from devpi, Nexus or Artifactory; `pip install --require-hashes` |
| Helm charts, tokenizers, chat templates | Package with the release | Same registry; no chart pulls from the internet |
| Ollama models | `ollama pull`, or a GGUF file | Copy the model directory, or `ollama create` from the GGUF with a Modelfile |

Then switch off everything that phones home: vLLM usage statistics (`VLLM_NO_USAGE_STATS=1` or `DO_NOT_TRACK=1`), framework telemetry and vendor trace exporters. Hosted model APIs are not available, so every model in the gateway is local, and observability (Langfuse, Phoenix, Prometheus, Grafana) is self-hosted too.

[[fig:platforms_identity]]

::: deeper
**Workload identity in depth.** Kubernetes projects a short-lived, audience-bound ServiceAccount token into each pod; Vault and other services verify it with the cluster's token review or its OIDC discovery keys. SPIFFE goes further: SPIRE issues each workload a SPIFFE ID such as `spiffe://corp.example/ns/agents/sa/sales-analyst` and a short-lived X.509 certificate, after attesting what is actually running (node, namespace, image). Services then authenticate each other with mutual TLS without any shared secret. Give each agent, and ideally each agent version, its own identity, so audit logs and policies can tell them apart.

**Token exchange details.** RFC 8693 defines a `subject_token` (the user's) and an optional `actor_token` (the agent's), and the issued token can carry an `act` claim naming the actor. Keep exchanged tokens short-lived (minutes), bound to one audience, and down-scoped to the tool's needs. For long-running jobs that outlive a user's session, decide explicitly: either the job runs under a service identity approved for that job, or it stores a refresh grant under strict controls. Do not quietly fall back to a shared admin account.

**Audit.** Log, for every tool call: the user, the agent identity and version, the tool, the arguments (redacted), the data touched, the decision of any policy check, and the trace id that links it to the model calls. Write these to an append-only store the agent cannot modify. This is what lets you answer "who saw this table through the agent last month?", the question an auditor will ask.

**Prompt injection and exfiltration.** The dangerous combination is private data, untrusted content and a way to send data out, all in one agent. Identity and network controls break the third leg: no open egress, no rendering of model-written links or images that load external URLs, no tool that can post to arbitrary addresses. See [Production engineering, security and cost](#production) for the full threat model.

**Dynamic secrets trade-offs.** Per-lease database users mean many short-lived roles; PostgreSQL handles this well, but connection pooling needs care, because a pooler holding connections for an expired user fails. Common choices: short pools refreshed on renewal, or a pooler (PgBouncer) authenticating with Vault-issued credentials while row-level security uses a per-request session setting for the end user.

**OpenBao and licences.** Vault moved to the Business Source License in 2023; OpenBao is the Linux Foundation fork under the Mozilla Public License, with a compatible API. Some companies standardise on it for licence reasons.

**On Google Cloud.** The managed equivalents are IAM service accounts and the SPIFFE-based Agent Identity for agents on Agent Runtime, Secret Manager, and VPC Service Controls for network perimeters; the next topic covers them briefly.

> **Interview:** "How do you stop an internal agent from becoming a way around data access controls?" The agent never has more rights than the user it serves. Users sign in through OIDC; the agent exchanges the user's token for a short-lived, audience-bound token per tool (RFC 8693), and each tool or MCP server verifies it. The warehouse enforces access with the user's identity, through PostgreSQL row-level security or Trino impersonation, so the agent cannot widen it. The agent's own workload identity (ServiceAccount or SPIFFE) gets secrets from Vault, with dynamic, read-only database credentials that expire within the hour. NetworkPolicies limit it to the gateway, the database and Vault. Every tool call is audited with user, agent and trace id.

> **Interview:** "What changes in an air-gapped deployment?" Nothing downloads at run time, so models, images, packages and charts come through a controlled transfer with checksums, scanning and signatures, into an internal registry, package mirror and model store. Servers run with offline flags and telemetry off. Only open-weight models are available, so model choice and GPU sizing happen up front, and every update is a release you carry in.
:::

::: check
?? An agent uses one service account with read access to the whole warehouse for every user. What is the risk called, and what is the fix?
The confused deputy problem: any user can get data through the agent that they could not query directly. Fix it by acting on behalf of the user: token exchange to a short-lived, scoped token per tool, and access enforced by the database with the user's identity.

?? Why are Vault dynamic database credentials better for agents than a static password in a Kubernetes Secret?
Each lease creates a separate, least-privilege database user that expires automatically, so a leaked credential is short-lived and every query is attributable to one lease. A static password lives until someone rotates it and is shared by every pod.

?? Name three artifacts you must bring into an air-gapped site before an agent stack can run.
Model weights (with checksums and licence), container images (into an internal registry, signed), and Python packages (into a package mirror with hashes); also Helm charts and tokenizer or chat-template files.
:::

::: resources
- Spec: [RFC 8693: OAuth 2.0 Token Exchange](https://datatracker.ietf.org/doc/html/rfc8693) · subject and actor tokens, the `act` claim
- Spec: [MCP authorization](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization) · OAuth 2.1 for remote MCP servers
- Docs: [Vault database secrets engine](https://developer.hashicorp.com/vault/docs/secrets/databases) · dynamic, expiring database credentials
- Docs: [Vault Kubernetes auth method](https://developer.hashicorp.com/vault/docs/auth/kubernetes) · pods authenticating with ServiceAccount tokens
- Docs: [SPIFFE overview](https://spiffe.io/docs/latest/spiffe-about/overview/) · workload identity and SPIRE
- Docs: [Kubernetes Network Policies](https://kubernetes.io/docs/concepts/services-networking/network-policies/) · default deny and egress rules
- Docs: [Hugging Face Hub environment variables](https://huggingface.co/docs/huggingface_hub/package_reference/environment_variables) · `HF_HUB_OFFLINE` and caching for offline use
:::

### Running agents on Google Cloud

::: plain
Everything in this section so far you run yourself. A managed platform runs some of it for you: you hand over your agent code, and the cloud provider keeps it running, scales it, stores its conversations and gives it an identity. It is like moving from your own PostgreSQL servers to a managed database: less work, less control, and a monthly bill instead of hardware. On Google Cloud there are three levels of "managed": a dedicated agent runtime, a serverless container platform (Cloud Run), and managed Kubernetes (GKE).
:::

**The names.** Google Cloud's AI platform is now **Gemini Enterprise Agent Platform** (formerly Vertex AI). Its managed runtime for agents is **Agent Runtime** (formerly Agent Engine). The rename is recent and not complete everywhere: the API resource is still called `ReasoningEngine`, the ADK command is still `adk deploy agent_engine`, and older code imports `vertexai.agent_engines`. Related managed services carry the new naming too: Agent Platform Sessions for conversation history and Agent Platform Memory Bank for long-term memory ([Context engineering, memory and RAG](#memory)).

**Three ways to run the sales analyst on Google Cloud.**

| Option | What you hand over | What you still own | Fits when |
|---|---|---|---|
| Agent Runtime | Agent code (ADK, LangGraph, LangChain, LlamaIndex, AG2, CrewAI or a custom container) | Agent logic, tools, IAM | You want sessions, memory, scaling and agent identity without running infrastructure |
| Cloud Run | A container (your FastAPI service unchanged) | The service, its scaling settings, state stores | Your agent is already an HTTP container; you want scale to zero and simple operations |
| GKE | Kubernetes manifests (the ones from this section) | Cluster design, GPUs, model servers, autoscaling | You serve your own open-weight models on GPUs or need the same platform as on-prem |

**Agent Runtime.** A fully managed runtime that deploys, runs and scales agents. ADK agents get the fullest integration (Python and Go); LangChain, LangGraph, AG2 and LlamaIndex agents use SDK templates; CrewAI and anything else run through a custom template or as a container that follows the runtime's contract. Deployed agents get managed Sessions and Memory Bank, IAM-controlled access, optional VPC Service Controls, and a choice of identity: a service account you name, or an **Agent Identity**, a per-agent identity based on SPIFFE with an automatically managed X.509 certificate, tied to the agent's resource. For an ADK agent, deploying is one command (`google-adk` 2.15.0):

```bash
adk deploy agent_engine --project=my-project --region=europe-west4 \
  --display_name=sales-analyst \
  --service_account=sales-analyst@my-project.iam.gserviceaccount.com \
  sales_agent/
```

From Python, the Agent Platform SDK (package `google-cloud-agentplatform`, imported as `agentplatform`) creates the same resource with `client.runtimes.create(agent=..., config={...})`; this replaced the older `vertexai.agent_engines` module in SDK version 2.0.1. The deploy docs list the configuration keys, which map directly onto the on-prem concerns of this section:

```python
# Written from the Agent Runtime deploy docs (November 2026); syntax-checked, not run.
import agentplatform

client = agentplatform.Client(project="my-project", location="europe-west4")
remote_agent = client.runtimes.create(
    agent=local_agent,                                   # an ADK agent wrapped for the runtime
    config={
        "display_name": "sales-analyst",
        "requirements": "requirements.txt",              # pinned dependencies
        "env_vars": {"MODEL_NAME": "analyst",
                     "DB_PASSWORD": {"secret": "sales-db-password", "version": "latest"}},
        "service_account": "sales-analyst@my-project.iam.gserviceaccount.com",
        "min_instances": 1, "max_instances": 10,         # warm capacity and a cost ceiling
        "container_concurrency": 8,
    },
)
```

`local_agent` is the ADK agent wrapped as the runtime's quickstart shows (`AdkApp(agent=root_agent)`). Secret values come from Secret Manager by reference, never inline. Because the docs and SDK are mid-rename, check the current quickstart for the exact import of the wrapper before you copy this.

**Cloud Run.** The FastAPI service from this section deploys unchanged: build the image, then `gcloud run deploy sales-analyst --image=... --no-allow-unauthenticated --service-account=...`, or `adk deploy cloud_run` for ADK agents. Cloud Run scales containers on concurrent requests (including to zero), supports streaming responses and request timeouts long enough for agent runs, and offers Cloud Run jobs for batch work, the equivalent of Kubernetes Jobs. It can attach NVIDIA GPUs (such as the L4) to services, which is enough to serve a small open-weight model with vLLM or Ollama for light traffic.

**GKE.** Google's managed Kubernetes runs the manifests from this section with few changes: GPU or TPU node pools for vLLM, GKE Autopilot if you do not want to manage nodes, and the GKE Inference Gateway (built on the Kubernetes Gateway API Inference Extension) for model-aware routing. `adk deploy gke` generates manifests for ADK agents. This is the natural cloud target for a team that already runs on-prem Kubernetes, because the platform stays the same.

**Models.** On Google Cloud an agent can call Gemini models through the `google-genai` SDK, use open-weight models from Model Garden on managed endpoints, or keep serving its own models with vLLM on GKE or Cloud Run. Through LiteLLM, all of these sit behind the same gateway as the on-prem models, so the agent does not change.

> **Tip:** Hybrid is common: data and model servers stay on-prem, and only stateless or public-facing parts run in the cloud, or the reverse for burst capacity. Design for it by keeping agents behind a gateway, tools behind MCP, traces in OpenTelemetry and state in PostgreSQL, so moving a component is a deployment change, not a rewrite.

::: deeper
**When the managed runtime is worth it.** Agent Runtime removes the platform work of this section (images, scaling, session store, memory store, identity plumbing) and adds managed evaluation and observability hooks. The costs are lock-in to its session and memory services and deployment contract, less control over networking and runtime details, and a per-use bill. It fits teams already on Google Cloud, building with ADK, without a platform team. It does not fit air-gapped sites or rules that keep data on company hardware. Note that "on-prem" options from Google exist (Google Distributed Cloud, including an air-gapped variant), but they are a separate product with their own scope; do not assume every managed agent service is available there.

**Cloud Run versus GKE.** Cloud Run is simpler and scales to zero; GKE gives full Kubernetes (custom schedulers, GPU sharing with MIG and time-slicing, model-aware gateways, operators such as KServe). A common split: agents on Cloud Run, model servers on GKE GPU node pools, a LiteLLM gateway on either.

**Identity on Google Cloud.** The concepts from the previous topic map one to one: workload identity is a service account or Agent Identity; secrets are in Secret Manager; network isolation is VPC, firewall rules and VPC Service Controls perimeters; user delegation still needs OAuth with the user's own token for tools that should enforce the user's rights. Agent Identity's per-agent SPIFFE identity is the managed version of what SPIRE gives you on-prem.

**Version notes.** Product names changed during 2025 and 2026 (Vertex AI to Gemini Enterprise Agent Platform, Agent Engine to Agent Runtime), and the SDK moved from `vertexai.agent_engines` to `agentplatform` modules (`runtimes`, `sessions`, `memory_banks`, `sandboxes`). Expect older tutorials, Terraform resources and API names to use the old terms.

> **Interview:** "Would you use a managed agent runtime like Agent Runtime?" It depends on constraints. If we are on Google Cloud, build with ADK and have no platform team, yes: it handles scaling, sessions, memory and per-agent identity, and we focus on the agent. If data must stay on our hardware, or we serve our own models on GPUs, I would run the same design on Kubernetes, on-prem or on GKE, with vLLM, a LiteLLM gateway and PostgreSQL for state. Either way I keep the agent portable: tools over MCP, models behind an OpenAI-compatible gateway, OpenTelemetry traces, so the hosting choice can change.

> **Interview:** "How would you move the on-prem sales analyst to Google Cloud?" The container moves unchanged to Cloud Run or GKE. The gateway gets a new model route (Gemini, or vLLM on GKE). Vault secrets map to Secret Manager, ServiceAccounts to IAM service accounts or Agent Identity, NetworkPolicies to VPC rules and VPC Service Controls. PostgreSQL state moves to a managed PostgreSQL. If we also want managed sessions and memory, we port the agent to ADK and deploy to Agent Runtime.
:::

::: check
?? What is the current name of Google Cloud's managed agent runtime, and what older names will you still see?
Agent Runtime, part of Gemini Enterprise Agent Platform. Older names: Agent Engine (and Vertex AI for the platform); the API resource is still `ReasoningEngine`, and the ADK CLI command is still `adk deploy agent_engine`.

?? Your FastAPI agent container must run on Google Cloud with minimal changes. Which option, and why?
Cloud Run: it runs any HTTP container, scales on concurrent requests, supports streaming, and needs no cluster. GKE if you also serve your own models on GPUs or want the same Kubernetes manifests as on-prem.

?? Name one reason not to use the managed agent runtime.
Data or models must stay on company hardware or in an air-gapped network; or you want to avoid lock-in to its session, memory and deployment contract.
:::

::: resources
- Docs: [Agent Runtime overview](https://docs.cloud.google.com/gemini-enterprise-agent-platform/build/runtime) · the managed runtime, supported frameworks, SDK
- Docs: [Deploy an agent to Agent Runtime](https://docs.cloud.google.com/gemini-enterprise-agent-platform/scale/runtime/deploy-an-agent) · deployment options and configuration keys
- Docs: [Use Agent Identity with Agent Runtime](https://docs.cloud.google.com/gemini-enterprise-agent-platform/scale/runtime/agent-identity) · SPIFFE-based per-agent identity
- Docs: [ADK deployment](https://adk.dev/deploy/) · Agent Runtime, Cloud Run, GKE and Docker targets
- Docs: [GPU support for Cloud Run services](https://cloud.google.com/run/docs/configuring/services/gpu) · GPUs on serverless containers
- Docs: [Serve open models with vLLM on GKE](https://cloud.google.com/kubernetes-engine/docs/tutorials/serve-gemma-gpu-vllm) · vLLM on GKE GPU node pools
:::

## Wrap-up

### Cheat sheet

| Idea | Remember |
|---|---|
| Three services | Model server (vLLM), gateway (LiteLLM), agent service (FastAPI in a container) |
| Model choice | Fits the GPUs, licence allows the use, calls tools reliably (test it) |
| Licences | Per model: Apache 2.0 (Qwen, Gemma 4, gpt-oss), Llama Community License, MIT (DeepSeek); record revision and licence |
| Weights memory | Parameters x bytes: BF16 2, FP8 1, INT4 about 0.55 |
| KV cache per token | 2 x layers x kv_heads x head_dim x bytes; 128 KiB for Llama-8B shape, 320 KiB for 70B in BF16 |
| vLLM | PagedAttention, continuous batching, prefix caching; `--enable-auto-tool-choice` plus matching `--tool-call-parser` |
| Ollama, llama.cpp | GGUF, CPU and Apple Silicon, small teams; raise the context length |
| TGI | Maintenance mode; keep existing, start new services on vLLM or SGLang |
| Gateway jobs | One API, routing and fallbacks, virtual keys and budgets, logs and cost, policy |
| LiteLLM | `model_list`, `router_settings` (fallbacks, cooldowns, Redis), PostgreSQL for keys and spend |
| Agent service | FastAPI, SSE streaming, async I/O, `/healthz` and `/readyz`, one worker per pod |
| Kubernetes | Long `terminationGracePeriodSeconds`, scale on in-flight requests, vLLM on GPU metrics, startup probes |
| Long runs | `POST /runs` returns an id; Job or worker with deadline, step limit, checkpoints in PostgreSQL |
| Identity | Workload (ServiceAccount, SPIFFE), user (OIDC), delegated (RFC 8693 token exchange, `act` claim) |
| Secrets | Vault Kubernetes auth, dynamic database credentials with short TTLs, nothing secret in prompts |
| Network | Default-deny NetworkPolicy; egress only to gateway, database, Vault; egress proxy allow-list |
| Air-gapped | Bring in weights, images, packages, charts with checksums and signatures; offline flags; telemetry off |
| Google Cloud | Agent Runtime (formerly Agent Engine) on Gemini Enterprise Agent Platform; Cloud Run for containers; GKE for GPUs |

### Practice

::: try
**1. Size it.** A Qwen-style 32B dense model has 64 layers, 8 KV heads and head size 128. You must serve 20 concurrent agent sessions at up to 16,384 tokens each. Using the sizing function from the first topic, decide whether one 80 GB GPU is enough with (a) BF16 weights, (b) INT4 weights (4.5 bits) and a BF16 cache, (c) INT4 weights and an FP8 cache.
:::

?? Answer to 1

```python
plan("32B BF16       ", 32.8, 16, 64, 8, 128, gpu_gib=80, ctx=16384)
plan("32B INT4       ", 32.8, 4.5, 64, 8, 128, gpu_gib=80, ctx=16384)
plan("32B INT4 FP8 KV", 32.8, 4.5, 64, 8, 128, gpu_gib=80, ctx=16384, kv_bits=8)
# 32B BF16       : weights 61.1 GiB, KV 256 KiB/token, cache 8.9 GiB = 36,475 tokens = 2 full 16384-token sequences
# 32B INT4       : weights 17.2 GiB, KV 256 KiB/token, cache 52.8 GiB = 216,338 tokens = 13 full 16384-token sequences
# 32B INT4 FP8 KV: weights 17.2 GiB, KV 128 KiB/token, cache 52.8 GiB = 432,677 tokens = 26 full 16384-token sequences
```

Only (c) fits 20 full-length sessions on one GPU. In practice not every session is at its maximum length at once, so (b) may be enough for real traffic; measure with your own traces. Option (a) needs a second GPU with tensor parallelism. In every quantised case, re-run the tool-call smoke test.

::: try
**2. Add a hard budget.** The mini gateway lets the last request overshoot the budget. Change `complete` so it estimates the cost of a request before calling the back end (say, `max_tokens` times the route's highest price) and rejects it if the estimate would pass the budget. What do you lose?
:::

?? A solution to 2

```python
def complete_strict(api_key, model, messages, max_tokens=1200):
    key = KEYS.get(api_key)
    if key is None or model not in key["models"]:
        return {"status": 401, "error": "unknown key or model not allowed"}
    worst = max_tokens * max(PRICE[t] for t in ROUTES[model])       # worst-case cost of this call
    if key["spent"] + worst > key["budget"]:
        return {"status": 429, "error": "budget would be exceeded"}
    return complete(api_key, model, messages)
```

You lose some usable budget: requests are rejected while real spend is still below the cap, because the estimate assumes the most expensive route and the longest answer. Real gateways usually accept soft limits plus alerts at 80 and 100 percent, and use hard pre-checks only for strict cases.

::: try
**3. Make the service safe under load.** Add a concurrency limit of 8 in-flight agent runs to `service.py`. When it is full, `POST /ask` should return HTTP 429 with a `Retry-After: 5` header instead of queueing.
:::

?? A solution to 3

```python
import asyncio
from fastapi import HTTPException

SLOTS = asyncio.Semaphore(8)

@app.post("/ask")
async def ask(body: Ask, request: Request):
    if SLOTS.locked():                                  # all 8 slots taken
        raise HTTPException(429, "busy", headers={"Retry-After": "5"})
    await SLOTS.acquire()

    async def guarded():
        try:
            async for chunk in run_agent(body.question, request):
                yield chunk
        finally:
            SLOTS.release()                             # also runs if the client disconnects
    return StreamingResponse(guarded(), media_type="text/event-stream")
```

The slot is released in `finally`, so a disconnected client does not leak it. Set the limit from what the model servers can serve, divided by the number of agent replicas, and expose the in-flight count as a metric so the autoscaler can use it.

::: try
**4. Design review.** A team proposes: one agent pod with the warehouse admin password in an environment variable, open internet egress so it can "look things up", and a fallback from the on-prem model to a hosted API when the GPU is busy. List the problems and your fixes.
:::

?? Answer to 4
The admin password gives every user admin rights through the agent (confused deputy) and never expires: replace it with on-behalf-of tokens and access enforced by the database, plus Vault dynamic read-only credentials with short TTLs for the agent's own identity. Open egress gives prompt-injection attacks a way to send data out: default-deny NetworkPolicy, and an egress proxy with an allow-list if web lookups are truly needed. The hosted fallback may move restricted data out of the network and changes tool-call behaviour: remove it for restricted data, fall back to another local replica or a smaller local model, and test the agent against every model in the chain. Also: one pod is a single point of failure; run at least two replicas behind a gateway.

::: try
**5. Interview drill.** In two minutes, out loud: "Walk me through how you would take our sales analyst agent from a notebook to production for 200 analysts, on-prem."
:::

?? Answer to 5
A strong answer goes bottom up. Model: pick an open-weight model with a licence legal approves and good tool calling on our tests; size GPUs from weights plus KV cache for the expected concurrency and context; serve it with vLLM, with the right tool parser, at least two replicas, metrics in Prometheus. Gateway: LiteLLM with per-agent virtual keys, budgets, fallbacks to another local replica, PostgreSQL for spend, Redis for shared limits, traces to Langfuse through OpenTelemetry. Agent: a FastAPI service with SSE streaming, step limits and a concurrency cap, in a pinned, non-root container, deployed on Kubernetes with probes, an HPA on in-flight requests and a long grace period; long investigations as Jobs with checkpoints. Security: users sign in with OIDC, the agent acts on their behalf with exchanged tokens, the warehouse enforces row-level security, Vault issues short-lived read-only credentials, NetworkPolicies allow only gateway, database and Vault. Then evaluation in CI on real analyst questions before every model or prompt change.

### What comes next

You can now take an agent from a script to a service: serve an open-weight model on your own GPUs and size them, put a gateway in front with keys, budgets and fallbacks, package the agent as a streaming service on Kubernetes, give it an identity that never exceeds its user's rights, and run it all air-gapped or, where it fits, on Google Cloud. The next section, **Agents for data engineering**, puts this platform to work on the reader's own job: text-to-SQL over PostgreSQL, DuckDB and Trino, agents that build and repair pipelines in Airflow, Spark and dbt, data-quality and catalogue agents, and the guardrails that keep them safe on real warehouses.
