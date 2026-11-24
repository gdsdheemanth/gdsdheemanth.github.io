/* foundations: Agentic AI foundations, original drawings */
Object.assign(FIGS, {

foundations_spectrum:{ t:"The autonomy spectrum", cap:"Systems differ in who decides the next step. On the left your code fixes the path and the model fills in text; on the right the model chooses tools and steps in a loop. Cost, latency variance and testing effort grow from left to right, so move right only when the task needs it.", svg:S("fnd1", 250, "Five boxes from single call to multi-agent system, with an arrow showing who decides the next step moving from code to model",
  B(10, 40, 136, 80, "fill", ["Single call", "one prompt", "one answer"]) +
  B(160, 40, 136, 80, "sp", ["Workflow", "fixed chain", "gates in code"]) +
  B(310, 40, 136, 80, "st", ["Tool-using call", "one tool request", "code runs it"]) +
  B(460, 40, 136, 80, "ka", ["Agent", "model picks tools", "in a loop"]) +
  B(610, 40, 140, 80, "pr", ["Multi-agent", "agents hand off", "work to agents"]) +
  A("fnd1", "M146 80 L158 80") + A("fnd1", "M296 80 L308 80") + A("fnd1", "M446 80 L458 80") + A("fnd1", "M596 80 L608 80") +
  A("fnd1", "M20 160 L740 160", "acc") +
  T(20, 150, "code decides the next step", "sm") + T(740, 150, "model decides the next step", "sm", "end") +
  T(380, 195, "more flexible, but more cost, latency variance and testing effort", "sm", "middle") +
  T(380, 225, "most production systems: a workflow with one bounded agentic step", "b", "middle")) },

foundations_api:{ t:"One model API call, and the next", cap:"The API is stateless. Your application sends the system prompt, the tool definitions and the whole message history; the response carries content blocks, a stop reason and token usage. On tool_use the application runs the tool, appends the assistant message and a tool_result, and sends everything again.", svg:S("fnd2", 330, "Sequence between application, model API and a tool: request, tool_use response, tool execution, second request with tool_result, final answer",
  B(20, 20, 170, 50, "sp", ["Your application", "holds the history"]) +
  B(300, 20, 160, 50, "ka", ["Model API", "stateless"]) +
  B(570, 20, 170, 50, "st", ["Tool", "SQL, files, APIs"]) +
  A("fnd2", "M105 70 L105 320", "dash") + A("fnd2", "M380 70 L380 320", "dash") + A("fnd2", "M655 70 L655 320", "dash") +
  A("fnd2", "M110 100 L374 100") + T(242, 94, "system + tools + messages", "sm", "middle") +
  A("fnd2", "M374 140 L112 140", "kac") + T(242, 134, "tool_use block, stop_reason=tool_use", "sm", "middle") +
  A("fnd2", "M110 175 L649 175") + T(520, 169, "run the tool with the arguments", "sm", "middle") +
  A("fnd2", "M649 205 L112 205", "stc") + T(520, 199, "result (or an error)", "sm", "middle") +
  A("fnd2", "M110 245 L374 245") + T(242, 239, "same request + assistant msg + tool_result", "sm", "middle") +
  A("fnd2", "M374 285 L112 285", "kac") + T(242, 279, "text, stop_reason=end_turn, usage", "sm", "middle")) },

foundations_loop:{ t:"The agent loop", cap:"Each pass builds the request from the goal and the history, calls the model, and either returns the answer or runs the requested tools and appends their results. Stop conditions are checked every pass, so a stuck or expensive run ends cleanly instead of running forever.", svg:S("fnd3", 300, "A cycle of observe, think and act boxes with a branch to answer and a set of stop conditions",
  B(20, 115, 120, 60, "fill", ["Goal", "user request"]) +
  B(190, 30, 170, 70, "sp", ["Observe", "goal + history +", "tool results"]) +
  B(420, 30, 170, 70, "ka", ["Think", "call the model"]) +
  B(420, 190, 170, 70, "st", ["Act", "run requested tools", "errors as results"]) +
  B(190, 190, 170, 70, "pr", ["Append", "assistant message +", "tool_result blocks"]) +
  B(630, 30, 120, 70, "okf", ["Answer", "end_turn"]) +
  A("fnd3", "M140 135 L188 75") + A("fnd3", "M360 65 L418 65") +
  A("fnd3", "M590 65 L628 65") + T(609, 58, "done", "sm", "middle") +
  A("fnd3", "M505 100 L505 188") + T(512, 150, "tool_use", "sm") +
  A("fnd3", "M420 225 L362 225") + A("fnd3", "M275 190 L275 102") +
  T(630, 150, "stop conditions, checked every pass:", "b") +
  T(630, 172, "max steps", "sm") + T(630, 190, "token or cost budget", "sm") + T(630, 208, "timeout", "sm") +
  T(630, 226, "max_tokens or refusal", "sm") + T(630, 244, "repeated calls", "sm") +
  T(380, 290, "state carried between passes: messages for the model, run state for your code", "sm", "middle")) },

foundations_decide:{ t:"Workflow or agent?", cap:"Work through the questions in order. A single no points to a single call or a workflow; an agent is the choice only when the path is unknown, the task is worth the cost, the model can do the hard step, errors can be caught, and an evaluation set can prove the gain.", svg:S("fnd4", 300, "Decision flow with five questions; a no on any leads to a workflow, yes on all leads to an agent",
  B(10, 20, 140, 70, "fill", ["Steps known", "in advance?"]) +
  B(160, 20, 140, 70, "fill", ["Worth the", "extra cost?"]) +
  B(310, 20, 140, 70, "fill", ["Model can do", "the hard step?"]) +
  B(460, 20, 140, 70, "fill", ["Errors caught", "and undone?"]) +
  B(610, 20, 140, 70, "fill", ["Eval set", "exists?"]) +
  A("fnd4", "M150 55 L158 55") + A("fnd4", "M300 55 L308 55") + A("fnd4", "M450 55 L458 55") + A("fnd4", "M600 55 L608 55") +
  T(154, 46, "no", "sm", "middle") + T(304, 46, "yes", "sm", "middle") + T(454, 46, "yes", "sm", "middle") + T(604, 46, "yes", "sm", "middle") +
  B(40, 190, 300, 80, "sp", ["Single call or workflow", "chain, route, parallel, gates", "in code; cheapest and testable"]) +
  B(450, 190, 280, 80, "okf", ["Agent (bounded)", "read-only tools first, step and", "cost limits, human review"]) +
  A("fnd4", "M80 90 L150 188") + T(95, 140, "yes", "sm") +
  A("fnd4", "M230 90 L220 188", "dash") + A("fnd4", "M380 90 L300 188", "dash") + A("fnd4", "M530 90 L330 188", "dash") +
  T(330, 140, "any no", "sm", "middle") +
  A("fnd4", "M680 90 L620 188", "acc") + T(660, 140, "yes", "sm")) }

});
