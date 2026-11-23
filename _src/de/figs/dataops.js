/* dataops: DataOps and platform engineering, original drawings */
Object.assign(FIGS, {

dataops_cicd:{ t:"CI on every pull request, CD after merge", cap:"CI runs the cheapest checks first and builds only the changed models, in an expiring CI dataset, before anyone merges. After merge, CD builds one artifact tagged with the commit SHA and deploys that same artifact to each environment in turn.", svg:S("dataops1", 240, "Pull request checks from lint to data diff, then merge, build once and deploy to staging and production",
  T(20, 22, "Pull request: CI", "b") +
  B(20, 35, 130, 56, "sp", ["Lint", "ruff · sqlfluff"]) +
  B(170, 35, 130, 56, "sp", ["Unit tests", "pytest · dbt unit"]) +
  B(320, 35, 130, 56, "st", ["Compile, dry run", "SQL, schema, bytes"]) +
  B(470, 35, 130, 56, "st", ["Build changed", "state:modified+"]) +
  B(620, 35, 130, 56, "okf", ["Checks and diff", "tests · data diff"]) +
  A("dataops1", "M150 63 L168 63") + A("dataops1", "M300 63 L318 63") + A("dataops1", "M450 63 L468 63") + A("dataops1", "M600 63 L618 63") +
  A("dataops1", "M685 91 L685 120 L85 120 L85 158") +
  T(740, 146, "After merge: CD", "b", "end") +
  B(20, 160, 130, 56, "box", ["Merge to main", "reviewed PR"]) +
  B(190, 160, 150, 56, "ka", ["Build once", "image tagged with SHA"]) +
  B(380, 160, 170, 56, "pr", ["Deploy dev, staging", "the same artifact"]) +
  B(590, 160, 160, 56, "okf", ["Deploy production", "Composer · Cloud Run"]) +
  A("dataops1", "M150 188 L188 188") + A("dataops1", "M340 188 L378 188") + A("dataops1", "M550 188 L588 188")) },

dataops_gitops:{ t:"Push-free deployment with GitOps", cap:"Engineers change files through pull requests; CI tests and builds images but never touches the cluster. An agent inside the cluster (Argo CD or Flux) pulls the desired state from Git, applies it, and keeps comparing it with what is running, so manual changes (drift) are detected and reverted.", svg:S("dataops2", 250, "Engineer to Git, CI to Artifact Registry, and an in-cluster agent pulling from Git and reconciling workloads",
  B(20, 40, 140, 50, "sp", ["Engineer", "pull request"]) +
  B(210, 40, 160, 50, "st", ["Git repository", "desired state"]) +
  A("dataops2", "M160 65 L208 65") +
  B(210, 160, 160, 50, "box", ["CI", "tests · build image"]) +
  A("dataops2", "M270 90 L270 158") +
  A("dataops2", "M320 158 L320 92", "ln dash") + T(328, 130, "bump tag", "sm") +
  B(20, 160, 140, 50, "pr", ["Artifact Registry", "image:sha"]) +
  A("dataops2", "M210 185 L162 185") +
  '<rect class="fill" x="440" y="14" width="300" height="222" rx="10"/>' + T(590, 34, "GKE cluster", "b", "middle") +
  B(470, 48, 240, 50, "ka", ["Argo CD or Flux", "agent in the cluster"]) +
  B(470, 168, 240, 50, "okf", ["Running workloads", "Airflow · Spark · Flink · Trino"]) +
  A("dataops2", "M372 65 L468 65") + T(420, 58, "pull", "sm", "middle") +
  A("dataops2", "M540 98 L540 166") + T(548, 136, "apply", "sm") +
  A("dataops2", "M650 166 L650 100", "ln dash") + T(658, 136, "drift?", "sm")) },

dataops_bluegreen:{ t:"Write-audit-publish and a blue-green switch", cap:"Each run writes to a place consumers cannot see, audits it, and publishes only if the checks pass; failures are quarantined. Consumers read one stable name (a view), so publishing or rolling back is a single atomic switch between table versions.", svg:S("dataops3", 250, "Write, audit and publish steps, with consumers reading a view that points at the green table and can roll back to blue",
  B(20, 30, 130, 50, "sp", ["Write", "hidden table"]) +
  B(190, 30, 130, 50, "st", ["Audit", "quality checks"]) +
  B(360, 30, 130, 50, "okf", ["Publish", "switch pointer"]) +
  B(190, 140, 130, 50, "pr", ["Quarantine", "alert the owner"]) +
  A("dataops3", "M150 55 L188 55") + A("dataops3", "M320 55 L358 55") + T(339, 47, "pass", "sm", "middle") +
  A("dataops3", "M255 80 L255 138") + T(263, 114, "fail", "sm") +
  B(560, 16, 180, 46, "box", ["Dashboards", "read one stable name"]) +
  B(560, 100, 180, 46, "ka", ["shop.fct_orders", "view"]) +
  B(530, 190, 100, 46, "sp", ["v1 (blue)", "previous"]) +
  B(650, 190, 100, 46, "okf", ["v2 (green)", "current"]) +
  A("dataops3", "M650 62 L650 98") +
  A("dataops3", "M490 55 L525 55 L525 123 L558 123") +
  A("dataops3", "M690 146 L700 188") +
  A("dataops3", "M610 146 L585 188", "ln dash") + T(560, 172, "rollback", "sm", "end")) },

dataops_testpyramid:{ t:"The data testing pyramid", cap:"Most tests should be fast unit tests on small, hand-made inputs; fewer integration and contract tests check real services and agreements between teams; a few golden-dataset end-to-end tests check the whole pipeline. Underneath, data checks run on every production load, because data can break even when code does not.", svg:S("dataops4", 330, "A pyramid with unit tests at the base, then integration, contract and end-to-end tests, above a band of production data checks",
  B(80, 208, 440, 48, "sp", ["Unit tests", "pure functions · dbt unit tests · milliseconds each"]) +
  B(130, 148, 340, 48, "st", ["Integration and DAG tests", "real services · emulators · DAG import"]) +
  B(180, 88, 240, 48, "ka", ["Contract tests", "schemas agreed with producers"]) +
  B(230, 28, 140, 48, "okf", ["End-to-end", "golden datasets"]) +
  A("dataops4", "M590 250 L590 36") +
  T(602, 44, "slower, costlier, fewer", "sm") +
  T(602, 250, "faster, cheaper, more", "sm") +
  B(20, 270, 720, 46, "fill", ["Checks on every production load", "freshness · volume · data tests · write-audit-publish"])) },

dataops_slo:{ t:"From SLI to error budget policy", cap:"An SLI measures good events as a share of valid ones, the SLO sets a target over a window, and the error budget is what the target allows you to miss. Burn-rate alerts warn while budget is being spent fast, postmortems explain big spends, and the agreed policy decides what happens when the budget runs out.", svg:S("dataops5", 220, "SLI leads to SLO, then error budget and budget policy, with burn-rate alerts and postmortems below",
  B(20, 30, 160, 64, "sp", ["SLI", "good events / valid", "days on time / days"]) +
  B(210, 30, 160, 64, "st", ["SLO", "target over a window", "95% of days, 90 days"]) +
  B(400, 30, 160, 64, "ka", ["Error budget", "1 - SLO", "4.5 late days"]) +
  B(590, 30, 150, 64, "okf", ["Budget policy", "spent: pause features", "fix reliability first"]) +
  A("dataops5", "M180 62 L208 62") + A("dataops5", "M370 62 L398 62") + A("dataops5", "M560 62 L588 62") +
  B(400, 150, 170, 50, "pr", ["Burn-rate alerts", "fast: page · slow: ticket"]) +
  B(600, 150, 140, 50, "box", ["Postmortems", "actions feed back"]) +
  A("dataops5", "M480 94 L480 148") + A("dataops5", "M570 175 L598 175") +
  A("dataops5", "M670 148 L670 96", "ln dash")) },

dataops_incident:{ t:"The life of a data incident", cap:"Monitoring detects the problem, triage sets severity from impact and blast radius, and the team mitigates (roll back, pause, mark stale) before fixing and backfilling. Communication runs alongside until consumers have correct data, and the blameless review feeds new tests, alerts and runbooks back into detection.", svg:S("dataops6", 230, "Detect, triage, mitigate, fix and backfill, review, with communication alongside and a loop back to detection",
  B(20, 36, 136, 56, "sp", ["Detect", "monitors, not users"]) +
  B(170, 36, 136, 56, "st", ["Triage", "severity · impact"]) +
  B(320, 36, 136, 56, "pr", ["Mitigate", "roll back · pause"]) +
  B(470, 36, 136, 56, "ka", ["Fix and backfill", "verify with checks"]) +
  B(620, 36, 120, 56, "okf", ["Review", "blameless"]) +
  A("dataops6", "M156 64 L168 64") + A("dataops6", "M306 64 L318 64") + A("dataops6", "M456 64 L468 64") + A("dataops6", "M606 64 L618 64") +
  B(170, 120, 436, 44, "box", ["Communicate", "status updates at fixed intervals until resolved"]) +
  A("dataops6", "M680 92 L680 200 L88 200 L88 94", "ln dash") +
  T(384, 220, "action items: new tests, alerts and runbooks", "sm", "middle")) },

dataops_topologies:{ t:"Team topologies for a data organisation", cap:"Domain data teams (stream-aligned) own their pipelines and data products and consume the platform team's golden paths as a service. An enabling team coaches teams on governance and quality for a limited time, and a complicated-subsystem team owns a specialist component that others use as a service.", svg:S("dataops7", 260, "Enabling team, three stream-aligned domain teams, a complicated subsystem team, and a platform team underneath",
  B(20, 20, 130, 150, "st", ["Enabling team", "governance,", "quality and", "dbt coaching"]) +
  B(180, 20, 130, 60, "sp", ["Orders", "stream-aligned"]) +
  B(330, 20, 130, 60, "sp", ["Marketing", "stream-aligned"]) +
  B(480, 20, 130, 60, "sp", ["Finance", "stream-aligned"]) +
  B(630, 20, 110, 150, "pr", ["Complicated", "subsystem", "feature store,", "identity", "resolution"]) +
  B(180, 190, 430, 56, "ka", ["Data platform team", "golden paths · templates · CI/CD · observability"]) +
  A("dataops7", "M245 188 L245 82") + A("dataops7", "M395 188 L395 82") + A("dataops7", "M545 188 L545 82") +
  T(470, 140, "X-as-a-Service", "sm", "middle") +
  A("dataops7", "M150 50 L178 50", "ln dash") + T(85, 190, "facilitating", "sm", "middle") +
  A("dataops7", "M630 50 L612 50") + T(685, 190, "as a service", "sm", "middle")) },

});
