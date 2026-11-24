/* multiagent: Multi-agent systems, original drawings */
Object.assign(FIGS, {

multiagent_topologies:{ t:"Four ways to arrange agents", cap:"A single agent holds every tool and the whole history. A supervisor delegates tasks and gets results back, so control always returns to it. A hierarchy stacks supervisors into teams. In a handoff network (a swarm), agents pass control to each other directly and whoever holds it answers the user.", svg:S("ma1", 300, "Four panels: single agent with tools; supervisor with three workers and return arrows; a hierarchy of a head supervisor over two team supervisors; three peer agents passing control to each other",
  T(95, 24, "Single agent", "b", "middle") +
  B(30, 50, 130, 50, "ka", ["Agent", "all tools, all history"]) +
  B(30, 150, 130, 70, "fill", ["30 tools", "Trino, Airflow,", "Spark, dbt, Kafka..."]) +
  A("ma1", "M95 100 L95 144") +
  T(285, 24, "Supervisor", "b", "middle") +
  B(225, 50, 120, 50, "ka", ["Supervisor", "plans, delegates"]) +
  B(200, 170, 50, 40, "fill", ["W1"]) + B(260, 170, 50, 40, "fill", ["W2"]) + B(320, 170, 50, 40, "fill", ["W3"]) +
  A("ma1", "M250 100 L228 164") + A("ma1", "M285 100 L285 164") + A("ma1", "M320 100 L342 164") +
  T(285, 240, "results return up", "sm", "middle") +
  T(480, 24, "Hierarchical", "b", "middle") +
  B(430, 45, 100, 40, "ka", ["Head"]) +
  B(395, 125, 80, 40, "sp", ["Team A"]) + B(485, 125, 80, 40, "sp", ["Team B"]) +
  B(390, 205, 40, 36, "fill", ["w"]) + B(440, 205, 40, 36, "fill", ["w"]) +
  B(480, 205, 40, 36, "fill", ["w"]) + B(530, 205, 40, 36, "fill", ["w"]) +
  A("ma1", "M465 85 L440 119") + A("ma1", "M495 85 L520 119") +
  A("ma1", "M425 165 L412 199") + A("ma1", "M445 165 L458 199") +
  A("ma1", "M515 165 L502 199") + A("ma1", "M535 165 L548 199") +
  T(670, 24, "Handoffs (swarm)", "b", "middle") +
  B(610, 50, 120, 46, "ka", ["Triage"]) +
  B(600, 170, 70, 46, "ka", ["Trino"]) + B(680, 170, 70, 46, "ka", ["Access"]) +
  A("ma1", "M650 96 L638 164", "acc") + A("ma1", "M670 193 L674 193", "acc") +
  A("ma1", "M715 164 L700 102", "dash") +
  T(670, 250, "control moves; the", "sm", "middle") + T(670, 266, "holder talks to the user", "sm", "middle") +
  T(380, 292, "left to right: less central control, more flexibility, harder to test", "sm", "middle")) },

multiagent_handoff:{ t:"A handoff, step by step", cap:"The conversation is shared. The active agent either answers or calls a transfer tool. Code checks that the transfer is allowed, records the tool result, and changes the active agent; the next model call uses the new agent's prompt and tools. The active agent is saved, so the next user turn goes straight to it.", svg:S("ma2", 260, "A shared conversation log at the bottom; above it triage, Trino agent and access agent; transfer calls move an active-agent marker from left to right; the final answer goes to the user",
  B(20, 30, 150, 60, "ka", ["triage", "transfer_to_trino"]) +
  B(235, 30, 160, 60, "ka", ["trino_agent", "query_error, transfer"]) +
  B(460, 30, 150, 60, "ka", ["access_agent", "request_access"]) +
  B(650, 30, 95, 60, "okf", ["User", "gets answer"]) +
  A("ma2", "M170 60 L229 60", "acc") + A("ma2", "M395 60 L454 60", "acc") + A("ma2", "M610 60 L644 60") +
  T(200, 50, "check", "sm", "middle") + T(425, 50, "check", "sm", "middle") +
  B(20, 140, 725, 50, "st", ["Shared state: messages + active_agent (saved per thread)"], false) +
  A("ma2", "M95 90 L95 134", "dash") + A("ma2", "M315 90 L315 134", "dash") + A("ma2", "M535 90 L535 134", "dash") +
  T(380, 225, "each transfer: allowed target?  handoff count under the cap?  then switch prompt and tools", "sm", "middle")) },

multiagent_subagents:{ t:"Subagents with isolated context", cap:"The lead writes a detailed task for each subagent. Each subagent starts with a fresh context, uses its own tools, and may read thousands of lines. It writes its full output to a shared store and returns a short summary with a reference. The lead's context grows by a few lines per subagent.", svg:S("ma3", 290, "A lead agent on the left sends three task briefs to three subagents, each with its own context window; subagents write full results to an artifact store and return short summaries to the lead",
  B(20, 100, 150, 80, "ka", ["Lead agent", "small context:", "tasks + summaries"]) +
  B(260, 20, 200, 60, "sp", ["Subagent: sales", "fresh context, 6k tokens"]) +
  B(260, 110, 200, 60, "sp", ["Subagent: marketing", "fresh context, 6k tokens"]) +
  B(260, 200, 200, 60, "sp", ["Subagent: ops", "fresh context, 6k tokens"]) +
  B(560, 100, 180, 80, "st", ["Artifact store", "full findings,", "transcripts"]) +
  A("ma3", "M170 125 L254 50") + A("ma3", "M170 140 L254 140") + A("ma3", "M170 155 L254 230") +
  A("ma3", "M460 50 L554 120") + A("ma3", "M460 140 L554 140") + A("ma3", "M460 230 L554 160") +
  A("ma3", "M300 80 L176 172", "dash") +
  T(215, 285, "task brief out (solid), short summary + reference back (dashed)", "sm", "middle") +
  T(650, 210, "read later on demand", "sm", "middle")) },

multiagent_mast:{ t:"Where multi-agent systems fail", cap:"The MAST taxonomy groups 14 failure modes found in traces of real multi-agent frameworks into three categories. Most are design problems you can test for: unclear specs and stop conditions, agents ignoring or withholding information, and missing or weak verification. The bottom row shows the control in code that targets each category.", svg:S("ma4", 280, "Three columns for system design issues, inter-agent misalignment and task verification, each listing failure modes, with a row of controls beneath",
  B(20, 20, 230, 46, "pr", ["System design issues"]) +
  B(265, 20, 230, 46, "pr", ["Inter-agent misalignment"]) +
  B(510, 20, 230, 46, "pr", ["Task verification"]) +
  B(20, 76, 230, 110, "box", ["disobey task or role spec", "step repetition", "lost history", "no stop condition"]) +
  B(265, 76, 230, 110, "box", ["reset, derailment", "no clarifying question", "withheld or ignored input", "reasoning-action mismatch"]) +
  B(510, 76, 230, 110, "box", ["premature termination", "no or partial verification", "incorrect verification"]) +
  B(20, 210, 230, 56, "okf", ["Specs, roles, caps", "explicit done criteria"]) +
  B(265, 210, 230, 56, "okf", ["Typed messages, shared", "artifacts, trace checks"]) +
  B(510, 210, 230, 56, "okf", ["Verifier in code: tests,", "row counts, citations"]) +
  A("ma4", "M135 186 L135 204") + A("ma4", "M380 186 L380 204") + A("ma4", "M625 186 L625 204")) }

});
