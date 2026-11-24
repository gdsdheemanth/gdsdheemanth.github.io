/* frameworks: Agent frameworks, original drawings */
Object.assign(FIGS, {

frameworks_layers:{ t:"Where an agent framework sits", cap:"Every framework wraps the same loop around a model API. Your tools and rules sit above it, the model server below it. On-prem, the model server is vLLM or Ollama on your own GPUs, reached through an OpenAI-compatible (or Anthropic-compatible) endpoint. Frameworks differ in their main abstraction and in what they add around the loop.", svg:S("fw1", 330, "A layered stack: your application and tools at the top, the agent framework in the middle containing the loop plus state, approvals, multi-agent and tracing, a model API layer, and model servers vLLM and Ollama at the bottom; on the right, each framework's main abstraction",
  B(30, 20, 440, 50, "ka", ["Your application", "tools (run_sql), rules, prompts, schemas: plain modules"]) +
  B(30, 95, 440, 120, "sp", ["Agent framework"]) +
  B(50, 130, 130, 34, "fill", ["agent loop"], false) +
  B(190, 130, 130, 34, "fill", ["state, checkpoints"], false) +
  B(330, 130, 120, 34, "fill", ["approvals"], false) +
  B(50, 172, 130, 34, "fill", ["multi-agent"], false) +
  B(190, 172, 130, 34, "fill", ["guardrails, hooks"], false) +
  B(330, 172, 120, 34, "fill", ["tracing (OTel)"], false) +
  B(30, 240, 440, 34, "box", ["OpenAI-compatible or Anthropic-compatible HTTP API"], false) +
  B(30, 286, 210, 34, "st", ["vLLM (GPU server)"], false) +
  B(260, 286, 210, 34, "st", ["Ollama (dev, small)"], false) +
  A("fw1", "M250 70 L250 89") + A("fw1", "M250 215 L250 234") + A("fw1", "M250 274 L250 280") +
  T(620, 30, "Main abstraction", "b", "middle") +
  T(500, 60, "LangGraph", "b") + T(620, 60, "state graph", "sm") +
  T(500, 86, "LangChain", "b") + T(620, 86, "create_agent", "sm") +
  T(500, 112, "Google ADK", "b") + T(620, 112, "agent tree, Workflow", "sm") +
  T(500, 138, "Claude SDK", "b") + T(620, 138, "full harness", "sm") +
  T(500, 164, "OpenAI SDK", "b") + T(620, 164, "agents, handoffs", "sm") +
  T(500, 190, "CrewAI", "b") + T(620, 190, "crew of roles", "sm") +
  T(500, 216, "MS Agent Fwk", "b") + T(620, 216, "agent, workflows", "sm") +
  T(500, 242, "PydanticAI", "b") + T(620, 242, "typed agent", "sm") +
  T(500, 268, "DSPy", "b") + T(620, 268, "optimised program", "sm") +
  T(500, 294, "LlamaIndex", "b") + T(620, 294, "data, query engines", "sm")) },

frameworks_decision:{ t:"Choosing a framework in four questions", cap:"Start from the design, not the tool. Known steps point to a workflow engine; open-ended work points to an agent loop. Long runs with approvals need durable checkpoints. The kind of work (files and commands, typed service, team of roles, data retrieval, measurable task) narrows the rest.", svg:S("fw2", 330, "A decision flow: are the steps known in advance; if yes use a workflow engine; if no, must the run survive restarts or wait for approval; if yes LangGraph or ADK Workflow or Agent Framework workflows; if no, what is the work like, leading to Claude Agent SDK, PydanticAI or OpenAI Agents SDK, CrewAI, LlamaIndex, DSPy, or the plain SDK",
  B(250, 14, 260, 44, "ka", ["Steps known in advance?"]) +
  A("fw2", "M250 36 L170 36 L170 76") + T(205, 30, "yes", "sm") +
  B(40, 80, 260, 44, "okf", ["Workflow engine with model steps", "LangGraph, ADK Workflow, MAF, Airflow"]) +
  A("fw2", "M510 36 L590 36 L590 76") + T(545, 30, "no", "sm") +
  B(440, 80, 300, 44, "ka", ["Long runs, restarts, approvals?"]) +
  A("fw2", "M590 124 L590 150") + T(600, 142, "yes", "sm") +
  B(440, 154, 300, 44, "okf", ["Durable checkpoints", "LangGraph + PostgresSaver, ADK DB sessions"]) +
  A("fw2", "M440 102 L380 102 L380 150") + T(392, 96, "no", "sm") +
  B(230, 154, 190, 44, "ka", ["What is the work like?"]) +
  A("fw2", "M325 198 L325 214") +
  B(20, 220, 140, 50, "fill", ["files, commands", "Claude Agent SDK"]) +
  B(170, 220, 140, 50, "fill", ["typed service", "PydanticAI, OpenAI"]) +
  B(320, 220, 140, 50, "fill", ["team of roles", "CrewAI"]) +
  B(470, 220, 140, 50, "fill", ["documents, indexes", "LlamaIndex"]) +
  B(620, 220, 120, 50, "fill", ["scored task", "DSPy"]) +
  A("fw2", "M325 214 L90 214 L90 218", "dash") + A("fw2", "M325 214 L240 214 L240 218", "dash") +
  A("fw2", "M325 214 L390 214 L390 218", "dash") + A("fw2", "M325 214 L540 214 L540 218", "dash") +
  A("fw2", "M325 214 L680 214 L680 218", "dash") +
  B(110, 284, 540, 36, "st", ["One agent, few tools, short runs: the plain model SDK is enough"], false)) },

frameworks_graph:{ t:"The sales analyst as a LangGraph graph", cap:"Two nodes and a conditional edge make the agent loop. The checkpointer saves the state after every superstep under the thread id, so the run can pause before the tools node, wait for approval, and resume from the saved checkpoint, even in another process.", svg:S("fw3", 260, "START leads to an agent node; a conditional edge goes to a tools node when there are tool calls, or to END; tools loops back to agent; below, a row of checkpoints saved after each step in PostgreSQL, with a pause before tools for approval",
  B(20, 60, 80, 40, "box", ["START"]) +
  B(150, 50, 150, 60, "ka", ["agent", "calls the model"]) +
  B(400, 50, 150, 60, "ka", ["tools", "run_sql"]) +
  B(640, 60, 90, 40, "box", ["END"]) +
  A("fw3", "M100 80 L144 80") +
  A("fw3", "M300 70 L394 70", "acc") + T(347, 62, "tool calls", "sm", "middle") +
  A("fw3", "M400 92 L306 92") + T(352, 108, "results", "sm", "middle") +
  A("fw3", "M225 50 L225 30 L685 30 L685 54", "dash") + T(455, 24, "no tool calls: answer", "sm", "middle") +
  B(380, 130, 190, 30, "pr", ["interrupt_before tools"], false) +
  A("fw3", "M475 130 L475 116", "kac") +
  T(20, 190, "checkpointer", "b") +
  B(130, 175, 90, 30, "fill", ["cp 0: input"], false) +
  B(235, 175, 90, 30, "fill", ["cp 1"], false) +
  B(340, 175, 90, 30, "fill", ["cp 2: paused"], false) +
  B(445, 175, 90, 30, "fill", ["cp 3"], false) +
  B(550, 175, 90, 30, "fill", ["cp 4: done"], false) +
  A("fw3", "M220 190 L229 190") + A("fw3", "M325 190 L334 190") + A("fw3", "M430 190 L439 190") + A("fw3", "M535 190 L544 190") +
  B(130, 220, 510, 30, "st", ["PostgresSaver: one row set per thread_id, resume from any checkpoint"], false)) },

frameworks_harness:{ t:"How the Claude Agent SDK checks a tool call", cap:"Your Python process starts the Claude Code runtime and streams messages with it. Every tool call the model asks for passes six checks in order; the first that decides wins, and a hook deny or a deny rule holds even in bypassPermissions mode. In-process MCP tools such as run_sql run back in your process.", svg:S("fw4", 300, "Your Python code with query and options, connected to the Claude Code runtime and the model API; a tool request passes hooks, deny rules, ask rules, permission mode, allow rules and the can_use_tool callback, then runs or is blocked",
  B(20, 20, 200, 70, "ka", ["Your Python process", "query(), options", "tools, hooks, callback"]) +
  B(280, 20, 200, 70, "sp", ["Claude Code runtime", "loop, built-in tools,", "compaction, sessions"]) +
  B(540, 20, 200, 70, "st", ["Model endpoint", "Claude API, or", "Anthropic-compatible local"]) +
  A("fw4", "M220 45 L274 45") + A("fw4", "M280 65 L226 65") +
  A("fw4", "M480 45 L534 45") + A("fw4", "M540 65 L486 65") +
  T(380, 118, "the model asks for a tool: checks in order", "sm", "middle") +
  B(20, 135, 110, 44, "pr", ["1 hooks"]) +
  B(142, 135, 110, 44, "pr", ["2 deny rules"]) +
  B(264, 135, 110, 44, "pr", ["3 ask rules"]) +
  B(386, 135, 110, 44, "pr", ["4 mode"]) +
  B(508, 135, 110, 44, "pr", ["5 allow rules"]) +
  B(630, 135, 110, 44, "pr", ["6 can_use_tool"]) +
  A("fw4", "M130 157 L136 157") + A("fw4", "M252 157 L258 157") + A("fw4", "M374 157 L380 157") +
  A("fw4", "M496 157 L502 157") + A("fw4", "M618 157 L624 157") +
  B(120, 225, 200, 44, "box", ["Blocked", "error result to the model"]) +
  B(440, 225, 200, 44, "okf", ["Runs", "MCP tool in your process"]) +
  A("fw4", "M75 179 L75 247 L114 247", "dash") + A("fw4", "M197 179 L197 219", "dash") +
  A("fw4", "M441 179 L500 219", "acc") + A("fw4", "M563 179 L550 219", "acc") + A("fw4", "M685 179 L620 219", "acc") +
  A("fw4", "M685 179 L685 280 L220 280 L220 273", "dash") +
  T(380, 296, "dashed: deny paths; solid: approve paths", "sm", "middle")) }

});
