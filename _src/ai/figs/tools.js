/* tools: Tool use and function calling, original drawings */
Object.assign(FIGS, {

tools_where:{ t:"Where tools run", cap:"Every tool is described to the model the same way, but the code runs in different places. Client tools (your functions, and Anthropic-schema tools such as computer use) come back to your application as a call to execute. Server tools such as web search and code execution run on the provider's side inside the same API call. MCP tools run in a separate server that your application connects to.", svg:S("tls1", 300, "Your application sends tool definitions to the model API; client tool calls come back to the application, server tools run at the provider, MCP tools run in a separate server",
  B(20, 30, 200, 70, "sp", ["Your application", "the loop, validation,", "permissions, logs"]) +
  B(290, 30, 180, 70, "ka", ["Model API", "reads definitions,", "emits calls"]) +
  B(540, 30, 200, 70, "st", ["Server tools", "web search, fetch,", "code execution"]) +
  A("tls1", "M220 55 L284 55") + T(252, 48, "definitions", "sm", "middle") +
  A("tls1", "M290 80 L226 80", "kac") + T(258, 94, "tool_use", "sm", "middle") +
  A("tls1", "M470 65 L534 65", "dash") + T(502, 58, "runs there", "sm", "middle") +
  B(20, 170, 200, 70, "fill", ["Client tools", "your functions, bash,", "computer, browser"]) +
  B(290, 170, 180, 70, "pr", ["MCP server", "tools from other", "systems"]) +
  A("tls1", "M120 100 L120 164") + T(128, 136, "you execute", "sm") +
  A("tls1", "M200 120 L330 164", "dash") + T(290, 130, "via MCP client", "sm") +
  T(380, 280, "same contract everywhere: name, description, JSON Schema in; call id and arguments out", "sm", "middle")) },

tools_parallel:{ t:"Parallel within a turn, sequential across turns", cap:"In one turn the model asks for every lookup it can make independently, and the executor runs them concurrently up to a limit. Anything that depends on those results comes in a later turn. Writes in the same turn run one at a time, in the order the model listed them.", svg:S("tls2", 300, "Turn one fans out three row count calls in parallel; their results feed turn two, which runs pause and notify one after the other",
  B(10, 115, 134, 60, "ka", ["Model turn 1", "3 tool_use blocks"]) +
  B(200, 30, 170, 46, "fill", ["row_count raw.orders"], false) +
  B(200, 122, 170, 46, "fill", ["row_count stg.orders"], false) +
  B(200, 214, 170, 46, "fill", ["row_count mart.orders"], false) +
  A("tls2", "M144 135 L194 56") + A("tls2", "M144 145 L194 145") + A("tls2", "M144 155 L194 234") +
  T(285, 20, "concurrent, max 2 at a time", "sm", "middle") +
  B(410, 115, 160, 60, "ka", ["Model turn 2", "sees 1200, 1200, 1188"]) +
  A("tls2", "M370 56 L404 132") + A("tls2", "M370 145 L404 145") + A("tls2", "M370 234 L404 160") +
  B(600, 70, 140, 46, "st", ["1. pause_dag"], false) +
  B(600, 176, 140, 46, "st", ["2. notify on-call"], false) +
  A("tls2", "M570 135 L594 100") + A("tls2", "M670 116 L670 170", "acc") +
  T(670, 250, "writes: one at a time,", "sm", "middle") + T(670, 266, "in the listed order", "sm", "middle")) },

tools_sandbox:{ t:"Layers around model-written code", cap:"Each layer limits something different. Resource limits stop runaway code; the container or microVM stops it reading the host; the network policy stops it sending data out; the absence of credentials means a compromised run has nothing valuable to steal. Use several layers, because each one can fail.", svg:S("tls3", 300, "Nested boxes: resource limits inside a container inside gVisor or a microVM inside a network policy, with no credentials inside",
  B(20, 20, 720, 260, "box", [""]) + T(40, 44, "Network policy: no egress, or an allowlist through a proxy; metadata server blocked", "sm") +
  B(50, 60, 660, 200, "st", [""]) + T(70, 84, "gVisor or microVM: a separate kernel boundary per run", "sm") +
  B(80, 100, 600, 140, "sp", [""]) + T(100, 124, "Container: non-root, read-only root, ephemeral, inputs read-only", "sm") +
  B(110, 140, 260, 84, "ka", ["Model-written code", "python3 -I, empty env,", "no credentials"]) +
  B(400, 140, 250, 84, "fill", ["Resource limits", "CPU, memory, wall clock,", "disk, processes, output"])) },

tools_computer:{ t:"The computer use loop", cap:"The model only ever sees screenshots (or an accessibility snapshot) and only ever proposes actions. Your harness owns the environment: it performs each action, enforces allowlists and confirmations, checks that the action had the expected effect, and takes the next screenshot.", svg:S("tls4", 290, "A loop: harness takes a screenshot, model proposes an action, harness checks policy and acts in an isolated VM, then observes again",
  B(30, 40, 200, 70, "sp", ["Harness", "screenshot or", "accessibility snapshot"]) +
  B(530, 40, 200, 70, "ka", ["Model", "proposes one action:", "click, type, scroll"]) +
  B(530, 190, 200, 70, "pr", ["Policy check", "host allowlist, confirm", "irreversible steps"]) +
  B(30, 190, 200, 70, "st", ["Isolated VM or browser", "performs the action,", "fresh profile"]) +
  A("tls4", "M230 65 L524 65") + T(377, 57, "image + goal + history", "sm", "middle") +
  A("tls4", "M630 110 L630 184", "kac") + T(640, 150, "action", "sm") +
  A("tls4", "M530 225 L236 225") + T(383, 217, "allowed", "sm", "middle") +
  A("tls4", "M130 190 L130 116", "acc") + T(140, 150, "observe again, verify effect", "sm") +
  T(380, 282, "stop on done, step limit, time limit, or a human takeover request", "sm", "middle")) },

tools_gate:{ t:"A permission layer in front of every tool", cap:"The model's tool call is a request, not a command. A policy layer decides allow, ask or deny from the tool and its arguments; asks go to a human; allowed calls run with a narrowly scoped identity; every decision is written to an audit log the agent cannot modify, and denials go back to the model as errors with a reason.", svg:S("tls5", 290, "Tool call flows to a policy check with allow, ask and deny branches, approval, execution with scoped credentials, and an audit log",
  B(10, 110, 130, 60, "ka", ["Tool call", "name + arguments"]) +
  B(190, 110, 150, 60, "sp", ["Policy", "allowlist + argument", "rules"]) +
  B(400, 20, 160, 56, "okf", ["allow", "reads"]) +
  B(400, 112, 160, 56, "fill", ["ask a human", "irreversible writes"]) +
  B(400, 204, 160, 56, "st", ["deny", "error with a reason"]) +
  B(610, 66, 140, 60, "pr", ["Execute", "scoped identity,", "byte and row caps"]) +
  B(610, 190, 140, 60, "box", ["Audit log", "append-only"]) +
  A("tls5", "M140 140 L184 140") +
  A("tls5", "M340 130 L394 50") + A("tls5", "M340 140 L394 140") + A("tls5", "M340 150 L394 228") +
  A("tls5", "M560 48 L604 86") + A("tls5", "M560 136 L604 108") + T(585, 150, "yes", "sm", "middle") +
  A("tls5", "M680 126 L680 184", "dash") + A("tls5", "M560 232 L604 224", "dash") +
  T(75, 200, "the model only", "sm", "middle") + T(75, 216, "requests", "sm", "middle")) }

});
