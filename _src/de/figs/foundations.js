/* foundations: Data engineering foundations, original drawings */
Object.assign(FIGS, {

foundations_lifecycle:{ t:"The data engineering lifecycle", cap:"Data is generated in source systems you usually do not own. Data engineering covers the middle: ingesting it, storing it, transforming it and serving it to consumers. Storage sits underneath every stage, and the undercurrents (security, data management, DataOps, architecture, orchestration, software engineering) apply to all of them.", svg:S("foundations1", 300, "Lifecycle stages from generation through ingestion, transformation and serving, on top of storage, with undercurrents below",
  B(20, 40, 130, 60, "ka", ["Generation", "source systems"]) +
  '<rect class="fill" x="180" y="14" width="420" height="176" rx="12"/>' + T(390, 32, "What data engineers build", "sm", "middle") +
  B(200, 42, 120, 60, "sp", ["Ingestion", "move it in"]) +
  B(340, 42, 120, 60, "sp", ["Transformation", "clean, join, model"]) +
  B(480, 42, 105, 60, "sp", ["Serving", "make it usable"]) +
  B(200, 126, 385, 50, "st", ["Storage", "every stage reads from it and writes to it"]) +
  B(620, 40, 120, 60, "okf", ["Consumers", "BI, ML, apps"]) +
  A("foundations1", "M150 70 L198 70") + A("foundations1", "M320 72 L338 72") + A("foundations1", "M460 72 L478 72") + A("foundations1", "M585 72 L618 72") +
  A("foundations1", "M260 102 L260 124", "ln dash") + A("foundations1", "M400 102 L400 124", "ln dash") + A("foundations1", "M532 102 L532 124", "ln dash") +
  B(20, 210, 720, 70, "pr", ["Undercurrents: they apply to every stage", "security · data management · DataOps · data architecture · orchestration · software engineering"])) },

foundations_rowcol:{ t:"Row storage vs column storage", cap:"The same three orders stored two ways. A row store keeps each row together, so fetching one order is a single read, but a sum over one column must read every row. A column store keeps each column together, so SUM(amount) reads only the amount column (highlighted) and skips the rest.", svg:S("foundations2", 280, "Three rows stored row by row and column by column, showing which data a SUM query reads",
  T(20, 30, "Row-oriented (OLTP databases such as PostgreSQL)", "b") +
  B(20, 45, 220, 40, "ka", ["1 · Pune · 40"], false) + B(260, 45, 220, 40, "ka", ["2 · Oslo · 15"], false) + B(500, 45, 220, 40, "ka", ["3 · Pune · 22"], false) +
  T(20, 108, "SUM(amount) reads all three blocks; fetching order 2 reads one block", "sm tR") +
  T(20, 150, "Column-oriented (analytical engines such as BigQuery)", "b") +
  B(20, 165, 220, 40, "box", ["id: 1, 2, 3"], false) + B(260, 165, 220, 40, "box", ["city: Pune, Oslo, Pune"], false) + B(500, 165, 220, 40, "ka", ["amount: 40, 15, 22"], false) +
  T(20, 228, "SUM(amount) reads one block; fetching order 2 must visit every block", "sm tR") +
  T(20, 262, "Columns also compress well, because similar values sit next to each other.", "sm")) },

foundations_batchstream:{ t:"Batch vs streaming", cap:"In batch processing, events pile up in storage and a scheduled job processes all of them at once, so results are as old as the schedule. In streaming, a job that never stops processes each event within seconds of its arrival, so results stay fresh at the cost of an always-running system.", svg:S("foundations3", 280, "Batch lane with events collected then processed nightly, streaming lane with each event processed on arrival",
  T(20, 28, "Batch: collect all day, then process on a schedule", "b") +
  '<rect class="fill" x="20" y="44" width="440" height="56" rx="10"/>' +
  '<circle class="st" cx="50" cy="72" r="6"/><circle class="st" cx="100" cy="72" r="6"/><circle class="st" cx="140" cy="72" r="6"/><circle class="st" cx="210" cy="72" r="6"/><circle class="st" cx="270" cy="72" r="6"/><circle class="st" cx="330" cy="72" r="6"/><circle class="st" cx="370" cy="72" r="6"/><circle class="st" cx="430" cy="72" r="6"/>' +
  T(240, 116, "events wait in storage", "sm", "middle") +
  B(500, 47, 110, 50, "sp", ["Batch job", "runs at 02:00"]) + B(640, 47, 100, 50, "okf", ["Report", "a day old"]) +
  A("foundations3", "M460 72 L498 72") + A("foundations3", "M610 72 L638 72") +
  T(20, 152, "Streaming: process each event as it arrives", "b") +
  A("foundations3", "M20 196 L470 196") +
  '<circle class="st" cx="50" cy="196" r="6"/><circle class="st" cx="100" cy="196" r="6"/><circle class="st" cx="140" cy="196" r="6"/><circle class="st" cx="210" cy="196" r="6"/><circle class="st" cx="270" cy="196" r="6"/><circle class="st" cx="330" cy="196" r="6"/><circle class="st" cx="370" cy="196" r="6"/><circle class="st" cx="430" cy="196" r="6"/>' +
  T(240, 226, "each event flows on within seconds", "sm", "middle") +
  B(500, 171, 110, 50, "st", ["Stream job", "always running"]) + B(640, 171, 100, 50, "okf", ["Dashboard", "seconds old"]) +
  A("foundations3", "M470 196 L498 196", "stc") + A("foundations3", "M610 196 L638 196", "stc") +
  A("foundations3", "M20 262 L740 262", "ln dash") + T(740, 254, "time", "sm", "end")) },

foundations_stack:{ t:"A modern data stack on Google Cloud", cap:"Data flows left to right: from source systems, through ingestion tools, into BigQuery and Cloud Storage, where SQL transformations build clean tables, and out to dashboards, ML and applications. An orchestrator schedules every step, and reverse ETL closes the loop by pushing results back into operational tools.", svg:S("foundations4", 330, "Sources, ingestion, BigQuery storage and transformation, serving, with orchestration above and reverse ETL looping back",
  B(180, 14, 560, 36, "pr", ["Orchestration: Cloud Composer (Airflow) runs and retries each step"]) +
  B(20, 80, 130, 150, "ka", ["Sources", "Cloud SQL, AlloyDB", "SaaS apps (CRM)", "app events", "files"]) +
  B(180, 80, 130, 150, "sp", ["Ingestion", "Datastream (CDC)", "Fivetran, Airbyte", "Pub/Sub", "Storage Transfer"]) +
  B(340, 80, 180, 150, "st", ["Storage + compute", "BigQuery", "Cloud Storage", "(Iceberg tables)", "raw → staging → marts"]) +
  B(550, 80, 190, 150, "okf", ["Serving", "Looker, Looker Studio", "Vertex AI (ML)", "APIs and apps", "reverse ETL"]) +
  A("foundations4", "M150 155 L178 155") + A("foundations4", "M310 155 L338 155") + A("foundations4", "M520 155 L548 155") +
  A("foundations4", "M245 50 L245 78", "ln dash") + A("foundations4", "M430 50 L430 78", "ln dash") + A("foundations4", "M645 50 L645 78", "ln dash") +
  T(430, 252, "Transformation: dbt or Dataform SQL runs inside BigQuery", "sm", "middle") +
  A("foundations4", "M645 230 L645 296 L85 296 L85 232", "acc dash") +
  T(365, 316, "reverse ETL sends results back to operational tools", "sm", "middle")) },

foundations_git:{ t:"The pull request workflow", cap:"Every change starts on a short-lived branch cut from main, is committed in small steps, and is proposed as a pull request. Automated checks run first, then a colleague reviews, and only then is the change merged back into main, which stays deployable at all times.", svg:S("foundations5", 200, "Branch, commit, pull request, CI checks, review and merge, then back to main",
  B(20, 30, 120, 54, "box", ["Branch", "git switch -c"]) +
  B(160, 30, 120, 54, "sp", ["Commit", "small steps"]) +
  B(300, 30, 130, 54, "pr", ["Pull request", "explain why"]) +
  B(450, 30, 130, 54, "st", ["CI checks", "lint, tests, dry run"]) +
  B(600, 30, 140, 54, "okf", ["Review + merge", "then deploy"]) +
  A("foundations5", "M140 57 L158 57") + A("foundations5", "M280 57 L298 57") + A("foundations5", "M430 57 L448 57") + A("foundations5", "M580 57 L598 57") +
  A("foundations5", "M20 150 L740 150") +
  A("foundations5", "M80 148 L80 86", "ln dash") + A("foundations5", "M670 86 L670 148", "acc") +
  T(380, 176, "main: always working, always deployable", "sm", "middle") + T(90, 124, "start from main", "sm") + T(660, 124, "merge", "sm", "end")) },

});
