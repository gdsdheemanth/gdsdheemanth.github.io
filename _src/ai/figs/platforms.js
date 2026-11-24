/* platforms: Deploying and serving agents, original drawings */
Object.assign(FIGS, {

platforms_stack:{ t:"An on-prem agent stack", cap:"A deployed agent is three services. Analysts call the agent service, which calls one gateway, which routes to model servers on your GPUs (and to a hosted API only where policy allows). Identity, secrets, state and observability sit beside them and are shared by every agent.", svg:S("pl1", 300, "Analysts call the agent service, which calls the LiteLLM gateway, which routes to two vLLM replicas, an Ollama server and, dashed, a hosted model API; underneath, shared services: Keycloak for sign-in, Vault for secrets, PostgreSQL and MinIO for state and data, Langfuse and Prometheus for observability",
  B(20, 60, 110, 60, "box", ["Analysts", "browser, BI, chat"]) +
  B(170, 50, 160, 80, "ka", ["Agent service", "FastAPI + SSE", "Deployment, HPA, Jobs"]) +
  B(370, 50, 160, 80, "sp", ["Gateway", "LiteLLM: one API", "keys, budgets, fallbacks"]) +
  B(580, 14, 160, 40, "st", ["vLLM replica A (GPU)"], false) +
  B(580, 62, 160, 40, "st", ["vLLM replica B (GPU)"], false) +
  B(580, 110, 160, 40, "st", ["Ollama (small models)"], false) +
  B(580, 158, 160, 40, "pr", ["Hosted API (if allowed)"], false) +
  A("pl1", "M130 90 L164 90") + A("pl1", "M330 90 L364 90") +
  A("pl1", "M530 80 L574 36") + A("pl1", "M530 88 L574 82") + A("pl1", "M530 98 L574 128") +
  A("pl1", "M530 110 L574 174", "ln dash") +
  T(380, 220, "shared platform services", "sm", "middle") +
  B(20, 232, 170, 50, "fill", ["Keycloak (OIDC)", "who is the user"]) +
  B(205, 232, 170, 50, "fill", ["Vault", "short-lived secrets"]) +
  B(390, 232, 170, 50, "fill", ["PostgreSQL, MinIO", "state, data, spend"]) +
  B(575, 232, 165, 50, "fill", ["Langfuse, Prometheus", "traces, metrics"]) +
  A("pl1", "M250 130 L250 226", "ln dash") + A("pl1", "M450 130 L450 226", "ln dash")) },

platforms_paged:{ t:"PagedAttention: the KV cache in blocks", cap:"Each sequence's KV cache is a list of small fixed-size blocks, mapped to wherever free blocks happen to be in GPU memory, like pages in virtual memory. The shared system prompt and tool schemas are stored once and referenced by both sequences. Only the last block of each sequence is partly empty, so almost no memory is wasted.", svg:S("pl2", 280, "Two sequences on the left, A with blocks prefix, A1, A2 and B with blocks prefix, B1; arrows map them to scattered physical blocks in a GPU memory grid on the right, where the prefix block is shared by both; below, a bar showing GPU memory split into weights, KV cache blocks and runtime overhead",
  T(110, 24, "logical blocks (block table)", "b", "middle") +
  T(20, 62, "seq A", "b") + B(70, 44, 80, 30, "ka", ["prefix"], false) + B(155, 44, 50, 30, "fill", ["A1"], false) + B(210, 44, 50, 30, "fill", ["A2 ..."], false) +
  T(20, 132, "seq B", "b") + B(70, 114, 80, 30, "ka", ["prefix"], false) + B(155, 114, 50, 30, "okf", ["B1 ..."], false) +
  T(557, 24, "physical blocks in GPU memory", "b", "middle") +
  B(420, 40, 64, 34, "fill", ["A1"], false) + B(490, 40, 64, 34, "box", ["free"], false) + B(560, 40, 64, 34, "okf", ["B1"], false) + B(630, 40, 64, 34, "box", ["free"], false) +
  B(420, 82, 64, 34, "ka", ["prefix"], false) + B(490, 82, 64, 34, "box", ["free"], false) + B(560, 82, 64, 34, "box", ["free"], false) + B(630, 82, 64, 34, "fill", ["A2"], false) +
  B(420, 124, 64, 34, "box", ["free"], false) + B(490, 124, 64, 34, "box", ["free"], false) + B(560, 124, 64, 34, "box", ["free"], false) + B(630, 124, 64, 34, "box", ["free"], false) +
  A("pl2", "M110 74 C 110 96, 300 96, 414 97") + A("pl2", "M110 114 C 110 102, 300 102, 414 103") +
  T(220, 180, "same colour = same sequence; prefix stored once", "sm", "middle") +
  T(380, 196, "one 80 GB GPU, 70B model in INT4", "sm", "middle") +
  B(40, 208, 250, 40, "st", ["weights 37 GiB"], false) + B(290, 208, 300, 40, "ka", ["KV cache blocks 33 GiB"], false) + B(590, 208, 130, 40, "box", ["runtime, 10%"], false) +
  T(380, 268, "the cache, not the weights, decides how many users fit", "sm", "middle")) },

platforms_gateway:{ t:"What the gateway does on each request", cap:"Every agent sends the same OpenAI-style request with its own virtual key. The gateway authenticates the key, checks allowed models, rate limits and budget, then walks the route for the model name: replicas first, fallbacks next, putting failing deployments in cooldown. Every call is logged with tokens and cost.", svg:S("pl3", 270, "An agent with a virtual key sends a request to the gateway; inside, four steps: check key and model, check rate limit and budget, route to a deployment, fall back on failure; the gateway calls vLLM replicas or a fallback model, and writes spend to PostgreSQL and traces to an OpenTelemetry collector",
  B(20, 90, 120, 60, "ka", ["Agent", "virtual key sk-..."]) +
  B(180, 20, 360, 190, "sp", []) + T(360, 42, "LiteLLM gateway", "b", "middle") +
  B(200, 58, 150, 40, "fill", ["1 key, model allowed?"], false) +
  B(370, 58, 150, 40, "fill", ["2 rate limit, budget"], false) +
  B(200, 120, 150, 40, "fill", ["3 route: analyst"], false) +
  B(370, 120, 150, 40, "fill", ["4 fallback, cooldown"], false) +
  T(360, 190, "401 or 429 if a check fails", "sm", "middle") +
  A("pl3", "M140 120 L174 120") + A("pl3", "M350 78 L364 78") + A("pl3", "M445 96 L275 114") + A("pl3", "M350 140 L364 140") +
  B(580, 30, 160, 40, "st", ["vLLM A (analyst)"], false) +
  B(580, 80, 160, 40, "st", ["vLLM B (analyst)"], false) +
  B(580, 130, 160, 40, "okf", ["fallback: small model"], false) +
  A("pl3", "M540 100 L574 52") + A("pl3", "M540 104 L574 100") + A("pl3", "M540 140 L574 150", "ln dash") +
  B(200, 222, 150, 38, "box", ["PostgreSQL: spend"], false) + B(370, 222, 150, 38, "box", ["OTel: traces"], false) +
  A("pl3", "M275 210 L275 218") + A("pl3", "M445 210 L445 218")) },

platforms_k8s:{ t:"The sales analyst on Kubernetes", cap:"Namespaces separate the layers. Interactive questions hit the agent Deployment, scaled by an HPA; long runs become Jobs. Agents reach only the gateway, PostgreSQL and Vault (NetworkPolicy). The gateway runs two replicas with Redis and PostgreSQL. vLLM runs on GPU nodes, scaled on waiting requests and cache use.", svg:S("pl4", 310, "Four namespace boxes: agents containing the sales-analyst Deployment with HPA and a Job for long runs; platform containing two LiteLLM replicas and Redis; models on GPU nodes containing vLLM pods with a persistent volume of weights; data containing PostgreSQL and MinIO; Vault at the side; arrows from ingress to agents, agents to platform, platform to models, agents to data and Vault",
  B(20, 120, 90, 50, "box", ["Ingress", "TLS, no buffer"]) +
  B(140, 20, 200, 160, "ka", []) + T(240, 42, "ns: agents", "b", "middle") +
  B(155, 56, 170, 44, "fill", ["Deployment + HPA", "FastAPI, SSE, 2 to 20 pods"]) +
  B(155, 112, 170, 44, "fill", ["Job per long run", "deadline, checkpoints"]) +
  B(380, 20, 170, 160, "sp", []) + T(465, 42, "ns: platform", "b", "middle") +
  B(395, 56, 140, 34, "fill", ["LiteLLM x2"], false) +
  B(395, 100, 140, 34, "fill", ["Redis"], false) +
  T(465, 160, "one API, keys, budgets", "sm", "middle") +
  B(580, 20, 160, 160, "st", []) + T(660, 42, "ns: models (GPU)", "b", "middle") +
  B(595, 56, 130, 34, "fill", ["vLLM pod A"], false) +
  B(595, 100, 130, 34, "fill", ["vLLM pod B"], false) +
  T(660, 152, "weights on a PVC", "sm", "middle") + T(660, 168, "startup probe, GPU HPA", "sm", "middle") +
  B(140, 220, 300, 70, "okf", ["ns: data", "PostgreSQL (state, runs, spend, RLS)", "MinIO (results, files)"]) +
  B(470, 220, 160, 70, "pr", ["Vault", "k8s auth, dynamic", "DB credentials"]) +
  A("pl4", "M110 145 L134 120") + A("pl4", "M340 90 L374 80") + A("pl4", "M550 80 L574 80") +
  A("pl4", "M240 180 L240 214") + A("pl4", "M320 175 L500 214", "ln dash") + A("pl4", "M465 180 L400 214", "ln dash")) },

platforms_identity:{ t:"Acting on behalf of the user", cap:"The user signs in through OIDC. The agent exchanges that token for a short-lived token scoped to one tool, carrying both the user and the agent (RFC 8693). The tool verifies it and the database enforces the user's rights with row-level security. Separately, the agent's own workload identity gets short-lived database credentials from Vault.", svg:S("pl5", 290, "Sequence: Ana signs in with Keycloak and gets a token; the agent sends Ana's token to Keycloak's token exchange and receives a token with audience sql-tool, subject ana and actor the agent; the SQL tool verifies signature, audience and scope; PostgreSQL applies row-level security for Ana; below, the agent pod authenticates to Vault with its ServiceAccount and receives a one-hour read-only database user",
  B(20, 20, 110, 50, "box", ["Ana", "analyst"]) +
  B(200, 20, 160, 50, "pr", ["Keycloak (IdP)", "OIDC, token exchange"]) +
  B(20, 120, 150, 60, "ka", ["Agent", "svc-sales-agent"]) +
  B(240, 120, 160, 60, "fill", ["SQL tool / MCP", "check sig, aud, scope"]) +
  B(470, 120, 150, 60, "okf", ["PostgreSQL", "RLS: region = EU"]) +
  A("pl5", "M130 45 L194 45") + T(162, 38, "1 sign in", "sm", "middle") +
  A("pl5", "M75 70 L75 114") + T(82, 96, "2 Ana's token", "sm") +
  A("pl5", "M150 120 L230 72") + T(205, 108, "3 exchange", "sm") +
  A("pl5", "M300 72 L320 114", "ln dash") + T(330, 98, "4 aud=sql-tool, sub=ana, act=agent", "sm") +
  A("pl5", "M170 150 L234 150") + A("pl5", "M400 150 L464 150") + T(432, 142, "5 as Ana", "sm", "middle") +
  B(20, 220, 150, 50, "fill", ["Agent pod", "ServiceAccount token"]) +
  B(240, 220, 160, 50, "pr", ["Vault", "Kubernetes auth"]) +
  B(470, 220, 250, 50, "okf", ["one-hour read-only DB user", "created and dropped by Vault"]) +
  A("pl5", "M170 245 L234 245") + A("pl5", "M400 245 L464 245") +
  T(650, 40, "the agent never has", "sm", "middle") + T(650, 56, "more rights than the user", "sm", "middle")) }

});
