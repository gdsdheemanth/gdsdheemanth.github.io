/* Orchestration: section diagrams */
Object.assign(FIGS, {

orchestration_cross_dag:{ t:"Three ways to wire pipelines together", cap:"Push: the producer starts the consumer, so it must know every consumer. Pull: the consumer waits on a task inside another DAG, so it depends on that DAG's internals and schedule. Data-aware: the producer records an update to an asset and any number of consumers schedule on the asset, so neither side knows the other's code.", svg:S("orchestration1", 300, "Three rows comparing push triggering, pull sensing and asset-based scheduling between a producer DAG and a consumer DAG",
  T(20, 26, "Push: TriggerDagRunOperator", "b") +
  B(20, 36, 190, 56, "ka", ["sales_daily", "last task triggers"]) +
  B(550, 36, 190, 56, "box", ["finance_export", "started by producer"]) +
  A("orchestration1", "M210 64 L548 64", "acc") +
  T(380, 56, "producer calls consumer", "sm", "middle") +
  T(20, 120, "Pull: ExternalTaskSensor", "b") +
  B(20, 130, 190, 56, "ka", ["sales_daily", "task build_fact"]) +
  B(550, 130, 190, 56, "box", ["finance_export", "sensor waits"]) +
  A("orchestration1", "M548 158 L212 158", "ln dash") +
  T(380, 150, "consumer polls producer's task state", "sm", "middle") +
  T(20, 214, "Data-aware: assets", "b") +
  B(20, 224, 190, 56, "ka", ["sales_daily", "outlets=[asset]"]) +
  B(290, 224, 180, 56, "st", ["sales.fct_revenue", "asset event"]) +
  B(550, 224, 190, 56, "okf", ["finance_export", "schedule=[asset]"]) +
  A("orchestration1", "M210 252 L288 252", "acc") + A("orchestration1", "M470 252 L548 252", "acc")) },

orchestration_defer:{ t:"Poke-mode sensor versus deferrable operator", cap:"A poke-mode sensor holds a worker slot for the whole wait. A deferrable task uses a worker only to start and to finish; in between, a small asyncio trigger waits in the triggerer, and when it fires the scheduler queues the task again so a worker can resume it.", svg:S("orchestration2", 300, "Comparison of a sensor occupying a worker slot for hours with a deferrable task that hands the wait to the triggerer",
  T(20, 28, "Poke mode", "b") +
  B(20, 38, 720, 54, "sp", ["Worker slot held for the whole wait", "poke, sleep, poke, sleep ... until the file exists (hours)"]) +
  T(20, 130, "Deferrable", "b") +
  B(20, 140, 150, 54, "okf", ["Worker", "start, then defer"]) +
  B(240, 140, 280, 54, "pr", ["Triggerer", "asyncio trigger waits, no slot"]) +
  B(590, 140, 150, 54, "okf", ["Worker", "resume, finish"]) +
  B(240, 232, 280, 54, "box", ["Scheduler", "queues the task on the event"]) +
  A("orchestration2", "M170 167 L238 167", "acc") +
  A("orchestration2", "M380 194 L380 230", "acc") +
  A("orchestration2", "M520 259 L665 259 L665 196", "acc") +
  T(205, 158, "defer", "sm", "middle") + T(392, 216, "event", "sm") + T(600, 250, "resume", "sm", "middle")) },

orchestration_airflow3_arch:{ t:"Airflow 3 components", cap:"The DAG processor parses DAG files from a bundle and stores serialized DAGs in the metadata database. The scheduler decides what is ready and its executor starts tasks on workers. Workers and their tasks never touch the database: they talk to the API server's Execution API, which also serves the UI and REST API. The triggerer runs deferred waits.", svg:S("orchestration3", 330, "Airflow 3 architecture with DAG bundle, DAG processor, scheduler, workers, API server, triggerer and metadata database",
  B(20, 30, 170, 56, "ka", ["DAG bundle", "Git repo or folder"]) +
  B(240, 30, 170, 56, "sp", ["DAG processor", "parse, serialize"]) +
  B(20, 140, 170, 56, "sp", ["Scheduler", "executor plugin inside"]) +
  B(300, 140, 190, 56, "pr", ["API server", "UI, REST, Execution API"]) +
  B(590, 140, 150, 56, "okf", ["Workers", "Task SDK"]) +
  B(20, 250, 170, 56, "pr", ["Triggerer", "deferred waits"]) +
  B(300, 250, 190, 56, "st", ["Metadata database", "PostgreSQL"]) +
  B(590, 250, 150, 56, "box", ["Users and CI", "browser, API calls"]) +
  A("orchestration3", "M190 58 L238 58") +
  A("orchestration3", "M270 86 L270 225 L318 248") +
  A("orchestration3", "M190 182 L298 266") +
  A("orchestration3", "M105 140 L105 116 L665 116 L665 138", "acc dash") +
  T(385, 110, "executor starts tasks", "sm", "middle") +
  A("orchestration3", "M588 168 L492 168", "acc") + T(540, 160, "Execution API", "sm", "middle") +
  A("orchestration3", "M395 196 L395 248") +
  A("orchestration3", "M190 278 L298 278") +
  A("orchestration3", "M588 270 L470 198")) },

orchestration_ti_states:{ t:"Task instance states", cap:"The main path runs from none to success. A ready task waits in scheduled until limits allow it, then in queued until the executor starts it. A running task can defer (and is scheduled again when its trigger fires), fail, or wait in up_for_retry before being scheduled again. Tasks whose trigger rule is not met become skipped or upstream_failed.", svg:S("orchestration4", 300, "State machine of an Airflow task instance from none through scheduled, queued and running to success, failed, deferred and up_for_retry",
  B(20, 40, 100, 44, "box", ["none"]) +
  B(150, 40, 120, 44, "fill", ["scheduled"]) +
  B(300, 40, 110, 44, "fill", ["queued"]) +
  B(440, 40, 110, 44, "sp", ["running"]) +
  B(590, 40, 150, 44, "okf", ["success"]) +
  B(20, 150, 170, 44, "box", ["skipped or", "upstream_failed"]) +
  B(300, 150, 110, 44, "pr", ["deferred"]) +
  B(590, 150, 150, 44, "sp", ["failed"]) +
  B(440, 240, 150, 44, "fill", ["up_for_retry"]) +
  A("orchestration4", "M120 62 L148 62") + A("orchestration4", "M270 62 L298 62") +
  A("orchestration4", "M410 62 L438 62") + A("orchestration4", "M550 62 L588 62", "acc") +
  A("orchestration4", "M60 84 L80 148") +
  A("orchestration4", "M470 84 L390 148") +
  A("orchestration4", "M320 150 L240 86", "ln dash") +
  A("orchestration4", "M530 84 L620 148") +
  A("orchestration4", "M495 84 L495 238") +
  A("orchestration4", "M440 262 L210 262 L210 86", "ln dash") +
  T(80, 120, "rule not met", "sm") + T(300, 112, "trigger fires", "sm") +
  T(325, 256, "retry delay passes", "sm", "middle") + T(503, 200, "error", "sm")) },

orchestration_assets:{ t:"Task view and asset view", cap:"A task-centric orchestrator records runs of steps: did the 22 November run succeed? An asset-centric one records the state of each data asset and each of its partitions, so it can show which days of which table are built, missing or failed, and backfill exactly those.", svg:S("orchestration5", 270, "Left, three tasks of one DAG run in sequence; right, three assets stacked with a row of daily partition squares each, some missing",
  T(20, 28, "Task view: steps of one run", "b") +
  B(20, 50, 100, 44, "box", ["extract"]) + B(140, 50, 100, 44, "box", ["load"]) + B(260, 50, 100, 44, "box", ["transform"]) +
  A("orchestration5", "M120 72 L138 72") + A("orchestration5", "M240 72 L258 72") +
  T(190, 130, "history: run of 2026-11-22", "sm", "middle") + T(190, 150, "succeeded or failed", "sm", "middle") +
  T(410, 28, "Asset view: data and partitions", "b") +
  B(410, 45, 150, 44, "ka", ["raw.orders"]) +
  B(410, 120, 150, 44, "st", ["stg_orders"]) +
  B(410, 195, 150, 44, "st", ["fct_revenue"]) +
  A("orchestration5", "M485 89 L485 118") + A("orchestration5", "M485 164 L485 193") +
  [0, 1, 2, 3, 4, 5, 6].map(function(i){ return B(580 + i * 23, 57, 19, 20, "okf", [""]); }).join("") +
  [0, 1, 2, 3, 4, 5, 6].map(function(i){ return B(580 + i * 23, 132, 19, 20, i === 4 ? "sp" : "okf", [""]); }).join("") +
  [0, 1, 2, 3, 4, 5, 6].map(function(i){ return B(580 + i * 23, 207, 19, 20, i >= 4 ? "sp" : "okf", [""]); }).join("") +
  T(410, 262, "7 daily partitions: green built, orange missing or failed", "sm")) },

});
