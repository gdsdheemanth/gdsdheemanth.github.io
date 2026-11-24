/* patterns: Agent design patterns, original drawings */
Object.assign(FIGS, {

patterns_spectrum:{ t:"Who decides the next step", cap:"The nine patterns sit on one line. On the left, code fixes the path and the model fills in each step; moving right, the model takes over more decisions: which branch, which subtasks, which plan, and finally every next action. Human-in-the-loop can be added anywhere a mistake would be costly.", svg:S("pat1", 250, "A horizontal spectrum from code decides to model decides, with the workflow patterns on the left and the agent patterns on the right",
  A("pat1", "M30 60 L730 60", "acc") +
  T(30, 40, "code decides the path", "b") + T(730, 40, "model decides the path", "b", "end") +
  B(20, 85, 128, 56, "fill", ["Chaining", "fixed steps, gates"]) +
  B(158, 85, 128, 56, "fill", ["Routing", "model picks branch"]) +
  B(296, 85, 128, 56, "fill", ["Parallel", "fan-out, vote"]) +
  B(434, 85, 140, 56, "sp", ["Orchestrator", "model picks tasks"]) +
  B(584, 85, 156, 56, "sp", ["Evaluator loop", "model judges quality"]) +
  B(20, 160, 230, 56, "ka", ["Plan-and-execute", "model writes plan, code runs it"]) +
  B(265, 160, 230, 56, "ka", ["Reflection", "agent checks own work"]) +
  B(510, 160, 230, 56, "ka", ["ReAct", "model picks every action"]) +
  T(380, 240, "workflows (top row) are testable and cheap; agents (bottom row) adapt to what they find", "sm", "middle")) },

patterns_orchestrator:{ t:"Orchestrator-workers", cap:"The orchestrator reads the request and decides the subtasks at run time. Code validates and caps the plan before any worker runs. Each worker sees only its own slice, and a synthesis step checks and combines the results.", svg:S("pat2", 280, "A request goes to an orchestrator that emits a plan; code validates it; three workers each handle one file; a synthesizer combines and checks the results",
  B(10, 110, 120, 60, "box", ["Request", "move DAGs off", "old bucket"]) +
  B(160, 110, 130, 60, "ka", ["Orchestrator", "writes the plan"]) +
  B(320, 110, 110, 60, "st", ["Validate", "cap, targets exist"]) +
  B(470, 20, 130, 50, "fill", ["Worker 1", "orders_daily.py"]) +
  B(470, 115, 130, 50, "fill", ["Worker 2", "refunds_daily.py"]) +
  B(470, 210, 130, 50, "fill", ["Worker n", "decided at run time"]) +
  B(630, 110, 120, 60, "okf", ["Synthesize", "check, combine"]) +
  A("pat2", "M130 140 L154 140") + A("pat2", "M290 140 L314 140") +
  A("pat2", "M430 130 L464 48") + A("pat2", "M430 140 L464 140") + A("pat2", "M430 150 L464 232") +
  A("pat2", "M600 45 L624 125") + A("pat2", "M600 140 L624 140") + A("pat2", "M600 235 L624 155")) },

patterns_react_plan:{ t:"ReAct versus plan-and-execute", cap:"ReAct (left) calls the model before every action and decides one step at a time. Plan-and-execute (right) calls the model to write a plan, runs the steps in code or with a cheap model, and calls the model again only to replan when a step fails or surprises it.", svg:S("pat3", 300, "Left: a loop of model, tool and observation. Right: planner to a list of steps run by an executor, with failures going to a replanner that feeds a new plan back",
  T(170, 24, "ReAct", "b", "middle") +
  B(90, 40, 160, 50, "ka", ["Model", "thought + action"]) +
  B(90, 140, 160, 50, "fill", ["Tool", "runs the action"]) +
  B(90, 230, 160, 50, "box", ["Observation", "appended to history"]) +
  A("pat3", "M170 90 L170 134") + A("pat3", "M170 190 L170 224") +
  A("pat3", "M90 255 L50 255 L50 65 L84 65") + T(44, 160, "repeat", "sm", "end") +
  T(540, 24, "Plan-and-execute", "b", "middle") +
  B(330, 40, 130, 50, "ka", ["Planner", "writes all steps"]) +
  B(490, 40, 250, 50, "st", ["Plan: check, load, validate, swap"], false) +
  B(490, 140, 250, 50, "fill", ["Executor", "runs steps in code"]) +
  B(330, 140, 130, 50, "ka", ["Replanner", "on failure only"]) +
  B(490, 230, 250, 50, "okf", ["Done", "results and report"]) +
  A("pat3", "M460 65 L484 65") + A("pat3", "M615 90 L615 134") +
  A("pat3", "M490 165 L466 165", "dash") + A("pat3", "M395 140 L395 100 L484 100", "dash") +
  A("pat3", "M615 190 L615 224")) },

patterns_hitl:{ t:"Pause, review, resume", cap:"When the agent requests a gated action, the run saves its state and queues a review item, and the process ends. A reviewer approves, edits or rejects, possibly hours later. The run resumes from the checkpoint: approved or edited actions execute exactly as reviewed, and a rejection returns to the model as an error with the reason.", svg:S("pat4", 270, "The agent loop hits a gated tool, saves a checkpoint, puts an item in the review queue and stops; a reviewer decides; the run resumes from the checkpoint",
  B(20, 40, 150, 60, "ka", ["Agent loop", "requests delete"]) +
  B(210, 40, 150, 60, "st", ["Policy in code", "auto, approve, deny"]) +
  B(400, 40, 150, 60, "box", ["Checkpoint", "messages, call"]) +
  B(590, 40, 150, 60, "pr", ["Review queue", "exact arguments"]) +
  A("pat4", "M170 70 L204 70") + A("pat4", "M360 70 L394 70") + A("pat4", "M550 70 L584 70") +
  T(475, 125, "process ends; nothing waits in memory", "sm", "middle") +
  B(590, 170, 150, 60, "sp", ["Reviewer", "approve, edit, reject"]) +
  B(310, 170, 200, 60, "okf", ["Resume", "same run id, idempotent"]) +
  B(20, 170, 200, 60, "ka", ["Agent continues", "result or error + reason"]) +
  A("pat4", "M665 100 L665 164") + A("pat4", "M590 200 L516 200") + A("pat4", "M310 200 L226 200") +
  A("pat4", "M475 100 L475 110 L410 110 L410 164", "dash")) }

});
