/* analytics: Analytics engineering and BI, original drawings */
Object.assign(FIGS, {

analytics_layers:{ t:"From raw data to every consumer", cap:"Raw tables are cleaned into staging models, combined into intermediate models and published as tested marts. A semantic layer defines each metric once on top of the marts, and every consumer (BI, analysis, embedded dashboards and reverse ETL) reads that one definition. Security is enforced in the warehouse, so it applies to every path.", svg:S("analytics1", 330, "Layers: raw, staging, intermediate, marts, semantic layer, then BI, analysis, embedded analytics and activation",
  B(20, 30, 130, 56, "box", ["Raw", "as loaded"]) + A("analytics1", "M150 58 L178 58") +
  B(180, 30, 130, 56, "sp", ["Staging", "rename, cast"]) + A("analytics1", "M310 58 L338 58") +
  B(340, 30, 130, 56, "sp", ["Intermediate", "shared logic"]) + A("analytics1", "M470 58 L498 58") +
  B(500, 30, 240, 56, "okf", ["Marts: fct_ and dim_", "tested, documented grain"]) +
  A("analytics1", "M620 86 L620 118") +
  B(180, 120, 560, 50, "pr", ["Semantic layer", "metrics, dimensions and joins defined once (LookML, MetricFlow, Cube)"]) +
  A("analytics1", "M250 170 L110 218") + A("analytics1", "M380 170 L300 218") + A("analytics1", "M540 170 L490 218") + A("analytics1", "M680 170 L670 218") +
  B(20, 220, 170, 56, "ka", ["BI", "dashboards, explores"]) +
  B(210, 220, 170, 56, "ka", ["Analysis", "funnels, experiments"]) +
  B(400, 220, 170, 56, "ka", ["Embedded", "customer dashboards"]) +
  B(590, 220, 150, 56, "ka", ["Activation", "reverse ETL"]) +
  T(380, 310, "Warehouse security (IAM, row access policies, policy tags) applies underneath every path", "sm", "middle")) },

analytics_experiment:{ t:"The data side of an A/B test", cap:"Assignment hashes the experiment and user IDs into a variant. The application logs an exposure when the user reaches the changed code. Exposures are joined to outcome events in the warehouse to compute one row per user per metric, and the analysis step checks the sample ratio before comparing variants and storing the result.", svg:S("analytics2", 290, "Assignment, exposure log, events, per-user metrics, SRM check and analysis, results table",
  B(20, 30, 160, 60, "sp", ["Assignment", "hash(exp, user) mod N"]) + A("analytics2", "M180 60 L218 60") +
  B(220, 30, 160, 60, "box", ["Application", "shows variant A or B"]) + A("analytics2", "M380 50 L438 40") + A("analytics2", "M380 72 L438 92") +
  B(440, 14, 140, 50, "st", ["Exposure log", "first_exposed_at"]) +
  B(440, 76, 140, 50, "st", ["Outcome events", "orders, clicks"]) +
  A("analytics2", "M580 40 L618 70") + A("analytics2", "M580 100 L618 80") +
  B(620, 46, 120, 56, "pr", ["Per-user metrics", "zeros included"]) +
  A("analytics2", "M680 102 L680 150") +
  B(560, 152, 180, 56, "ka", ["SRM check", "chi-square on counts"]) + A("analytics2", "M560 180 L452 180") +
  B(260, 152, 190, 56, "ka", ["Analysis", "CUPED, intervals"]) + A("analytics2", "M260 180 L182 180") +
  B(20, 152, 160, 56, "okf", ["Results table", "audit trail"]) +
  T(380, 260, "Analyse at the unit you randomized, and only users who were exposed", "sm", "middle")) },

analytics_reverse:{ t:"Reverse ETL: from models back to tools", cap:"Operational systems feed the warehouse through normal ingestion. Activation models compute scores and audiences there. A sync engine diffs each model against its last sync and sends only changed rows to each destination, respecting API limits; BigQuery can also export directly to Bigtable, or continuously through continuous queries.", svg:S("analytics3", 300, "Sources flow into BigQuery models; a sync engine diffs and pushes changes to CRM, ads, support and Bigtable",
  B(20, 30, 150, 50, "box", ["CRM, billing", "support, product"]) + A("analytics3", "M170 55 L228 55") + T(199, 46, "ETL", "sm", "middle") +
  B(230, 20, 260, 70, "sp", ["BigQuery", "marts and activation models", "scores, audiences, profiles"]) +
  A("analytics3", "M360 90 L360 128") +
  B(230, 130, 260, 56, "pr", ["Sync engine", "diff vs last sync, batch, retry"]) +
  A("analytics3", "M490 145 L568 60") + A("analytics3", "M490 152 L568 120") + A("analytics3", "M490 166 L568 180") + A("analytics3", "M490 176 L568 240") +
  B(570, 36, 170, 44, "ka", ["CRM fields"]) +
  B(570, 98, 170, 44, "ka", ["Ad audiences"]) +
  B(570, 160, 170, 44, "ka", ["Support desk"]) +
  B(570, 222, 170, 44, "ka", ["Slack alerts"]) +
  A("analytics3", "M230 60 L110 200", "ln dash") +
  B(20, 202, 180, 56, "st", ["Bigtable, Spanner", "EXPORT DATA"]) +
  T(20, 288, "Dashed: BigQuery writes directly to Google Cloud serving stores", "sm")) },

});
