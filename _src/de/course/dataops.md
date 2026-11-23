This section is about running a data platform as a piece of software that many people depend on every day. It is written for data engineers who already build pipelines and now have to change them safely, prove they produce correct data, notice quickly when they do not, recover calmly, and keep the bill under control. It assumes the earlier sections: pipelines and orchestration (Pipelines and transformation, Orchestration), the Google Cloud services they run on (Google Cloud foundations for data, BigQuery in depth, Pipelines on Google Cloud), and the architecture choices from Data engineering system design. Python and SQL fluency from Programming for data engineering and SQL for data engineering is assumed throughout.

**What you will be able to do afterwards**

- Build a CI/CD pipeline that lints, tests and dry-runs data code on every pull request and deploys it automatically, with schema changes checked for breakage.
- Describe your whole platform (datasets, buckets, topics, permissions, clusters) as code with Terraform, and keep it in sync with GitOps.
- Give every engineer a safe environment with realistic data, and release new pipeline versions with blue-green, shadow and canary techniques.
- Package pipelines as containers and run them on Cloud Run and GKE, including Spark and Flink through Kubernetes operators.
- Test pipelines at every level, from unit tests of one function to golden-dataset regression tests, and choose between dbt tests, Great Expectations, Soda and Knowledge Catalog scans.
- Define SLOs for data, build monitoring, alerting and structured logging around them, run an incident from detection to postmortem, and test resilience on purpose with chaos experiments.
- Manage secrets without leaking them, attribute and reduce cloud costs, organise data teams, and assess how mature your platform is.

**The big picture.** DataOps applies the habits of DevOps and site reliability engineering to data, with one twist: in ordinary software the code is the product, but in data engineering the product is the **data**, and data can be wrong even when every line of code is correct (a source changed, a file arrived twice, a partner sent nulls). So a data platform needs two feedback loops. The **delivery loop** controls change to code and infrastructure: version control, review, automated tests, automated deployment and safe rollout. The **reliability loop** watches the data and systems in production: SLOs, monitoring, observability, alerting, incident response and learning. Around both sit the people and the money: how teams are organised and what everything costs. The four groups of this section follow exactly that shape.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Delivery

Delivery is everything that happens between "I changed a line" and "that change runs in production". On a young data team it is a person copying SQL into the BigQuery console, editing a DAG in the Composer bucket by hand, and clicking through the Cloud console to create datasets. That works for a month. Then two people overwrite each other's changes, nobody can say what changed before last night's numbers broke, and the staging environment no longer looks like production.

This group replaces clicking with code and automation. It starts with CI/CD (testing and deploying every change), moves to infrastructure as code and GitOps (the platform itself as reviewed, versioned files), then environments (where changes are tried out), safe release techniques (blue-green, shadow and canary) and finally the packaging and runtime layer: containers and Kubernetes.

### CI/CD for Data Pipelines

::: plain
CI/CD is an assembly line for changes. **Continuous integration (CI)** means every proposed change is automatically checked (formatted, tested, compiled against the warehouse) before anyone merges it, like a quality inspector at the factory gate. **Continuous delivery or deployment (CD)** means that once a change is approved, machines, not people, put it into production the same way every time. Nobody pastes SQL into a console, and every change can be traced back to a reviewed pull request.
:::

A data repository usually holds several kinds of code: SQL models (dbt or Dataform), Python (ingestion, Beam or Spark jobs, Airflow DAGs), configuration and infrastructure. A good pipeline checks each kind at the cheapest level that can catch a problem.

[[fig:dataops_cicd]]

**What CI runs on a pull request**, cheapest first:

1. **Lint and format**: `ruff` for Python, `sqlfluff` for SQL, `yamllint` for configuration. Seconds, and it ends style debates in review.
2. **Unit tests**: pure transformation functions with `pytest`; dbt unit tests on fixed input rows.
3. **Compile and dry-run**: `dbt compile` catches bad references; a BigQuery **dry run** validates SQL, permissions and schema and reports bytes to be scanned without running anything.
4. **Build changed models on real data**, in a temporary CI dataset: only the models that changed and their downstream models.
5. **Data checks and diffs**: tests on the CI tables, and optionally a diff against production ("this change alters 312 rows of `fct_orders`").

The fourth step is the expensive one, so dbt has **slim CI**: compare the project with the production **manifest** (a JSON description of every model, saved by the last production run), select only modified models and their children with `state:modified+`, and **defer** every unchanged parent to its production table instead of rebuilding it. dbt Cloud's CI jobs do this automatically; on your own runner it looks like this GitHub Actions workflow, authenticating to Google Cloud without any stored key through Workload Identity Federation:

```yaml
name: dbt-ci
on:
  pull_request:
    paths: ["models/**", "macros/**", "dbt_project.yml"]
permissions:
  contents: read
  id-token: write        # lets the job exchange its GitHub token for a short-lived Google token
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/123456789/locations/global/workloadIdentityPools/github/providers/acme
          service_account: dbt-ci@acme-ci.iam.gserviceaccount.com
      - uses: google-github-actions/setup-gcloud@v2
      - uses: astral-sh/setup-uv@v6
      - run: uv sync --frozen
      - run: uv run sqlfluff lint models/
      - run: gcloud storage cp gs://acme-dbt-artifacts/prod/manifest.json prod-state/manifest.json
      - run: uv run dbt build --target ci --select state:modified+ --defer --state prod-state/
```

**What CD runs after merge.** Build artifacts once (a container image tagged with the commit SHA, pushed to Artifact Registry), then deploy the same artifact to each environment in turn. Cloud Build can deploy Airflow DAGs to Managed Service for Apache Airflow (formerly Cloud Composer) after their tests pass:

```yaml
# cloudbuild.yaml, triggered on pushes to main
steps:
  - name: python:3.12-slim
    entrypoint: bash
    args: ["-c", "pip install -r requirements-test.txt && python -m pytest tests/dags -q"]
  - name: gcr.io/google.com/cloudsdktool/cloud-sdk:slim
    entrypoint: gcloud
    args: ["composer", "environments", "storage", "dags", "import",
           "--environment=prod-composer", "--location=europe-west1", "--source=dags/"]
```

**Schema migrations.** Changing a table's shape is the riskiest data deployment, because downstream queries and dashboards depend on it. Additive changes (a new nullable column) are safe; drops, renames and type changes break consumers. Automate the check. A small, runnable version:

```python
def breaking_changes(old, new):
    problems = []
    for col, typ in old.items():
        if col not in new:
            problems.append(f"dropped column {col}")
        elif new[col] != typ:
            problems.append(f"type of {col} changed {typ} -> {new[col]}")
    return problems

old = {"order_id": "INT64", "amount": "NUMERIC", "status": "STRING"}
new = {"order_id": "INT64", "amount": "FLOAT64", "coupon": "STRING"}
print(breaking_changes(old, new))
# ['type of amount changed NUMERIC -> FLOAT64', 'dropped column status']
```

In CI, the "old" schema comes from production (`INFORMATION_SCHEMA.COLUMNS` or the dbt manifest) and the new one from the branch; a non-empty list fails the build unless the pull request is labelled as an approved breaking change. Apply additive changes as code (`ALTER TABLE shop.orders ADD COLUMN IF NOT EXISTS coupon_code STRING;` or dbt's `on_schema_change: append_new_columns` on incremental models). For operational databases (Cloud SQL, AlloyDB), migration tools such as Flyway, Liquibase or Alembic apply numbered, versioned scripts in order.

**Feature flags for data.** A flag lets you merge new logic switched off and turn it on later without a deployment, and turn it off again instantly if numbers look wrong. In dbt, a variable does the job: `{% if var('use_new_attribution', false) %} ... {% endif %}`, switched on with `dbt build --vars '{use_new_attribution: true}'`. For applications and services, flag tools such as OpenFeature-compatible providers do the same.

> **Pitfall:** CI that builds the full project on every pull request. It is slow, so people stop waiting for it, and in BigQuery it can cost more than production. Build only what changed, defer the rest, and give the CI dataset a default table expiration so leftovers delete themselves.

> **Tip:** Make the main branch always deployable and deploy small changes often. A one-model change that breaks is found in minutes; a 40-file monthly release that breaks takes a day to untangle.

::: deeper
**Why data CI is harder than application CI.** Application tests run on fixtures; data correctness depends on real, changing data, so CI needs warehouse access, which means credentials, cost and time. The usual compromise: fast, data-free checks on every commit (lint, unit tests, compile, dry run, dbt's `--empty` flag, which builds models with zero rows to validate SQL and schemas cheaply), and real-data builds only for changed models, sometimes on a sampled copy of production.

**Diffing data.** A "value diff" joins the production and CI versions of a table on the primary key and reports rows added, removed and changed per column. Tools such as Datafold automate this in pull requests; you can do a basic version with a `FULL OUTER JOIN` and `EXCEPT DISTINCT` in BigQuery. Reviewers then see the effect of a change on the data, not only on the code.

**Measuring delivery.** The DORA research programme's four key metrics (deployment frequency, lead time for changes, change failure rate, time to restore service) apply to data teams unchanged. Teams that deploy small changes frequently also have fewer and shorter failures, which is the strongest argument for investing in CI/CD.

**Trunk-based development and merge queues.** Short-lived branches merged daily avoid long-running divergence. A merge queue tests each pull request against the latest main plus the pull requests ahead of it, so two changes that pass separately cannot break main together.

> **Interview:** "How would you set up CI/CD for a dbt project on BigQuery?" Lint SQL; run dbt unit tests; on pull requests, run slim CI (`state:modified+` with `--defer` against the production manifest) into an expiring CI dataset; run tests and a data diff; check for breaking schema changes; authenticate with Workload Identity Federation, not keys. After merge, run the production job, which saves a new manifest for the next comparison.
:::

::: check
?? What does `--select state:modified+ --defer --state prod-state/` achieve in dbt CI?
It builds only models changed in the branch and everything downstream of them, and reads every unchanged upstream model from its production table instead of rebuilding it, which keeps CI fast and cheap.

?? Which schema changes are safe to deploy without coordinating with consumers, and which are not?
Adding a nullable column is safe. Dropping or renaming a column, or changing its type, breaks downstream queries and needs a coordinated, versioned migration.

?? Why use Workload Identity Federation in CI instead of a service account key stored as a secret?
The CI job receives a short-lived token for each run, so there is no long-lived key to leak, rotate or forget.
:::

::: resources
- Docs: [Cloud Build overview](https://cloud.google.com/build/docs/overview) · Google Cloud's CI/CD service: build steps, triggers and private pools
- Docs: [dbt continuous integration](https://docs.getdbt.com/docs/deploy/continuous-integration) · slim CI, deferral and CI jobs in dbt
- Book: [Practical DataOps](https://www.oreilly.com/library/view/practical-dataops-delivering/9781484251041/) · Harvinder Atwal; applying agile, lean and DevOps practice to data teams
- Book: [Accelerate](https://itrevolution.com/product/accelerate/) · Forsgren, Humble and Kim; the research behind the DORA delivery metrics
- Docs: [google-github-actions/auth](https://github.com/google-github-actions/auth) · keyless authentication from GitHub Actions to Google Cloud
- Blog: [Adopting CI/CD with dbt Cloud](https://www.getdbt.com/blog/adopting-ci-cd-with-dbt-cloud) · dbt Labs on rolling out CI and CD for analytics code
:::

### Infrastructure as Code for Data

::: plain
Infrastructure as code (IaC) means describing your cloud resources (datasets, buckets, topics, clusters, permissions) in text files instead of creating them by clicking. A tool reads the files and makes the cloud match them. It is like an architect's blueprint versus building from memory: anyone can read the blueprint, review a change to it, build an identical copy for testing, and rebuild after a disaster.
:::

**Terraform** is the most widely used IaC tool. You write **resources** in HCL (HashiCorp Configuration Language), each one managed by a **provider**, a plug-in that knows one platform's API. The Google provider covers every Google Cloud service; others cover Snowflake, Databricks, Confluent, GitHub and hundreds more, so one tool can describe a whole data platform.

A small data-platform slice: a raw bucket, a dataset, a partitioned table, a Pub/Sub topic and a permission.

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 7.0" }
  }
  backend "gcs" {
    bucket = "acme-tf-state"
    prefix = "data-platform/prod"
  }
}

provider "google" {
  project = var.project_id
  region  = "europe-west1"
}

resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-raw"
  location                    = "EU"
  uniform_bucket_level_access = true
  lifecycle_rule {
    condition { age = 90 }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

resource "google_bigquery_dataset" "shop" {
  dataset_id = "shop"
  location   = "EU"
  labels     = { team = "commerce", env = "prod" }
}

resource "google_bigquery_table" "orders" {
  dataset_id          = google_bigquery_dataset.shop.dataset_id
  table_id            = "orders"
  deletion_protection = true
  time_partitioning {
    type  = "DAY"
    field = "order_date"
  }
  clustering = ["customer_id"]
  schema = jsonencode([
    { name = "order_id",    type = "INT64",   mode = "REQUIRED" },
    { name = "customer_id", type = "INT64",   mode = "REQUIRED" },
    { name = "order_date",  type = "DATE",    mode = "REQUIRED" },
    { name = "amount",      type = "NUMERIC", mode = "NULLABLE" },
  ])
}

resource "google_pubsub_topic" "orders" {
  name = "orders"
}

resource "google_bigquery_dataset_iam_member" "analysts_read" {
  dataset_id = google_bigquery_dataset.shop.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "group:analysts@acme.example"
}
```

The workflow is always the same:

```bash
terraform init                 # download providers, connect to the state backend
terraform plan -out=tfplan     # show exactly what will be created, changed or destroyed
terraform apply tfplan         # make it so
```

**State.** Terraform records what it manages, and the real IDs of each resource, in a **state file**. Teams must share one copy, stored remotely with locking so two applies cannot run at once; the `gcs` backend above stores it in a Cloud Storage bucket (turn on object versioning so you can recover an old state). Split state by environment and by area (network, platform, each team's datasets) so a plan stays small and one mistake cannot touch everything.

**Modules.** A **module** is a reusable folder of Terraform with inputs and outputs. A platform team might publish a `bq_domain` module that creates a dataset, its service account, standard labels and the usual IAM bindings; a domain team then writes ten lines instead of a hundred, and every dataset follows the same rules:

```hcl
module "marketing" {
  source      = "../modules/bq_domain"
  domain      = "marketing"
  owners      = ["group:marketing-data@acme.example"]
  readers     = ["group:analysts@acme.example"]
  environment = "prod"
}
```

**Other tools.**

| Tool | Language | How it works | When it fits |
|---|---|---|---|
| Terraform | HCL | Plan, then apply; state file | The default; huge provider ecosystem |
| OpenTofu | HCL | Community fork of Terraform under an open-source licence (MPL), drop-in compatible | When licence terms matter |
| Pulumi | Python, TypeScript, Go... | Real programming languages generate the resource graph | Teams that prefer loops and functions to HCL |
| Crossplane | Kubernetes YAML | Resources are Kubernetes objects, continuously reconciled by controllers | Kubernetes-centric platforms, self-service through the Kubernetes API |
| Config Connector | Kubernetes YAML | Google's Kubernetes operator for Google Cloud resources | GKE shops managing Google Cloud resources the Kubernetes way |
| Infrastructure Manager | Terraform | Google Cloud's managed service that runs Terraform for you | When you want Google to host state and runs |

> **Pitfall:** Changing resources by hand in the console after they are managed by Terraform. The next `plan` either reverts your change or fails confusingly. If you must fix something by hand during an incident, bring the code back in line straight afterwards.

> **Pitfall:** Secrets in state. Terraform state can contain sensitive values (generated passwords, keys) in plain text. Restrict access to the state bucket, and prefer resources that never produce secrets (Workload Identity instead of keys).

> **Tip:** Protect data from deletion. A renamed resource in Terraform looks like "destroy the old one, create a new one", which for a table means losing its data. Keep `deletion_protection = true` on tables, add `lifecycle { prevent_destroy = true }` to critical datasets and buckets, and use `moved` blocks when you rename resources in code.

::: deeper
**How plan works.** Terraform builds a dependency graph from references between resources (the table refers to the dataset, so the dataset comes first), refreshes the state by reading each resource's real configuration through the provider, and computes the difference between desired and actual. Resources with no dependency between them are created in parallel. Some attribute changes can be applied in place; others force replacement, which `plan` marks as `-/+`. Reading every plan for unexpected replacements is the single most important review habit.

**Importing existing resources.** Platforms rarely start from zero. Terraform's `import` blocks (since 1.5) bring existing resources under management, and `terraform plan -generate-config-out=generated.tf` writes a first draft of their configuration.

**Data versus infrastructure.** Terraform is good at containers (datasets, buckets, topics, IAM) and bad at contents. Manage tables whose schemas evolve with every pull request in dbt or Dataform, and keep Terraform for the long-lived shell around them. Many teams let Terraform create only raw landing tables and leave modelled tables to the transformation tool.

**Policy as code.** Tools such as Open Policy Agent (with Conftest) or HashiCorp Sentinel check a plan against rules before apply: "every bucket has uniform access", "no dataset is public", "every resource has a `team` label". This turns governance rules into automated checks in CI.

> **Interview:** "Two engineers run `terraform apply` on the same state at the same time. What protects you, and what else should you do?" State locking in the remote backend makes the second apply wait or fail. Beyond that, run applies only from CI after review (never from laptops), split state into small units, and keep the state bucket versioned and tightly permissioned.
:::

::: check
?? What is Terraform state, and why must it be stored remotely with locking for a team?
A record of the resources Terraform manages and their real IDs and attributes. A shared remote copy with locking stops engineers working from different copies and prevents two applies from changing the same resources at once.

?? A plan shows `-/+` on a BigQuery table after you renamed it in code. What will happen, and how do you avoid it?
Terraform will destroy the table (and its data) and create a new one. Use a `moved` block to tell Terraform it is the same resource, and keep `deletion_protection` on so the destroy fails.

?? Why might a platform team publish a Terraform module for "a domain dataset"?
So every team creates datasets the same way, with standard labels, IAM and service accounts, by writing a few lines of inputs instead of copying long configurations.
:::

::: resources
- Docs: [Terraform on Google Cloud](https://cloud.google.com/docs/terraform) · Google's guides, best practices and samples for the Google provider
- Docs: [Google provider reference](https://registry.terraform.io/providers/hashicorp/google/latest/docs) · every resource and argument, including BigQuery, Cloud Storage and Pub/Sub
- Course: [HashiCorp Terraform tutorials](https://developer.hashicorp.com/terraform/tutorials) · official step-by-step tutorials, including Google Cloud
- Book: [Terraform: Up and Running, 3rd Edition](https://www.oreilly.com/library/view/terraform-up-and/9781098116736/) · Yevgeniy Brikman; state, modules, testing and team workflows
- Docs: [Best practices for using Terraform](https://cloud.google.com/docs/terraform/best-practices/general-style-structure) · Google's guidance on structure, state and modules
- Docs: [OpenTofu](https://opentofu.org/docs/) · the open-source fork of Terraform, with the same language and workflow
- Docs: [Snowflake Terraform provider](https://registry.terraform.io/providers/Snowflake-Labs/snowflake/latest) · managing warehouses, databases and grants as code
:::

### GitOps for Data Infrastructure

::: plain
GitOps means the Git repository is the single source of truth for what should be running, and a robot inside the platform keeps making reality match it. You never run deployment commands yourself; you change a file and merge a pull request, and the robot notices and applies it. If someone changes something by hand, the robot notices that too and puts it back, like a thermostat that keeps bringing the room back to the temperature written on the dial.
:::

GitOps rests on four principles, as defined by the OpenGitOps project: the desired state is **declarative** (files that describe what, not scripts that say how), **versioned and immutable** (in Git, with full history), **pulled automatically** (agents fetch it rather than CI pushing it), and **continuously reconciled** (agents keep correcting differences).

The difference from ordinary CD is the direction of the arrow. In **push-based** deployment, a CI job holds credentials to the cluster and runs `kubectl apply` or `helm upgrade`. In **pull-based** GitOps, an agent running inside the cluster watches Git and applies changes itself, so CI never needs production credentials, and any drift is corrected.

[[fig:dataops_gitops]]

**Argo CD and Flux** are the two main GitOps agents for Kubernetes, both graduated CNCF projects. For a data platform on GKE they deploy the long-running pieces: Airflow (from its Helm chart), the Spark and Flink operators and their jobs, Trino, Kafka on Strimzi, monitoring stacks. An Argo CD `Application` points at a folder in Git and keeps a namespace in sync with it:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: spark-jobs
  namespace: argocd
spec:
  project: data-platform
  source:
    repoURL: https://github.com/acme/data-platform-deploy.git
    targetRevision: main
    path: envs/prod/spark-jobs
  destination:
    server: https://kubernetes.default.svc
    namespace: spark-jobs
  syncPolicy:
    automated:
      prune: true        # delete resources that were removed from Git
      selfHeal: true     # revert changes made by hand in the cluster
```

Flux does the same with two objects: a `GitRepository` that says where to fetch from, and a `Kustomization` that says which path to apply:

```yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: airflow
  namespace: flux-system
spec:
  interval: 10m
  sourceRef:
    kind: GitRepository
    name: platform
  path: ./envs/prod/airflow
  prune: true
```

**Repository layout.** A common pattern keeps application code and deployment configuration in separate repositories. CI builds an image from the code repository and then opens a pull request (or commits) to the deployment repository that bumps the image tag; the agent deploys it. Environments are folders (or branches, less commonly) with small per-environment overlays:

```text
data-platform-deploy/
  base/airflow/            values shared by every environment
  base/spark-jobs/
  envs/dev/airflow/        overlay: small workers, dev connections
  envs/prod/airflow/       overlay: autoscaling, prod connections
  envs/prod/spark-jobs/    SparkApplication manifests pinned to image tags
```

**GitOps outside Kubernetes.** Much of a Google Cloud data platform is not Kubernetes: BigQuery datasets, Pub/Sub topics, Composer environments, IAM. The same principles still apply:

- **Terraform with pull-request automation**: plan runs on every pull request and is posted as a comment, apply runs on merge (tools such as Atlantis, or Cloud Build triggers). This is push-based, but Git stays the source of truth.
- **Drift detection**: a scheduled job runs `terraform plan -detailed-exitcode`, which exits with code 2 when real resources differ from the code, and alerts on it.
- **Kubernetes-native control of cloud resources**: Config Connector or Crossplane represent BigQuery datasets or buckets as Kubernetes objects, so Argo CD or Flux can reconcile them too.
- **DAGs and dbt projects**: deployed only from Git by CI, never edited in place; Composer environments get their DAG folder from the pipeline, not from people.

> **Pitfall:** Secrets in the GitOps repository. Everything in Git is readable by everyone with repository access and stays in history forever. Store references to secrets (an External Secrets Operator object naming a Secret Manager secret), never the secret values.

> **Tip:** Turn on `selfHeal` only once the team has stopped making manual fixes. Otherwise the agent silently reverts emergency changes during an incident; agree that hotfixes go through Git, using a fast-track review if needed.

::: deeper
**Reconciliation internals.** Argo CD renders the desired manifests (plain YAML, Kustomize or Helm), compares them field by field with the live objects, and reports each application as `Synced` or `OutOfSync` and `Healthy` or `Degraded`. Health checks understand built-in kinds (a Deployment is healthy when its replicas are ready) and can be extended with custom checks for operators' resources such as `SparkApplication`. **Sync waves** and hooks order deployments, for example running a database migration Job before upgrading Airflow.

**Progressive delivery.** Argo Rollouts and Flagger (from the Argo and Flux ecosystems) add canary and blue-green releases to GitOps, shifting traffic gradually and rolling back automatically when metrics degrade. For streaming services this pairs well with the techniques in the blue-green topic below.

**Config Sync** is Google's GitOps agent for GKE fleets: it syncs configuration and policies from Git to many clusters, and works with Policy Controller to enforce rules.

**Limits of GitOps for data.** GitOps reconciles configuration, not data. Rolling back a manifest does not roll back the rows a bad job wrote. Pair GitOps with data-level safety: write-audit-publish, table snapshots, and time travel in BigQuery or Iceberg.

> **Interview:** "Push-based CD or pull-based GitOps for a GKE data platform?" Pull-based keeps cluster credentials out of CI, gives continuous drift correction and a full audit trail in Git, and scales to many clusters. Push-based is simpler for small setups and for non-Kubernetes resources. Many platforms combine them: GitOps agents for Kubernetes workloads, Terraform through CI for cloud resources, both driven by pull requests.
:::

::: check
?? What is the key difference between push-based CD and pull-based GitOps?
In push-based CD the CI system holds production credentials and applies changes. In GitOps an agent inside the platform pulls the desired state from Git and applies it, continuously correcting drift.

?? What do `prune: true` and `selfHeal: true` do in an Argo CD application?
`prune` deletes live resources that were removed from Git; `selfHeal` reverts changes made directly in the cluster so it matches Git again.

?? How can you detect drift for Terraform-managed BigQuery datasets?
Run `terraform plan -detailed-exitcode` on a schedule; exit code 2 means the real resources differ from the code, which should raise an alert.
:::

::: resources
- Docs: [Argo CD documentation](https://argo-cd.readthedocs.io/en/stable/) · applications, sync policies, health checks and multi-cluster setups
- Docs: [Flux documentation](https://fluxcd.io/flux/) · the GitOps toolkit: sources, Kustomizations, Helm releases and image automation
- Spec: [OpenGitOps principles](https://opengitops.dev/) · the four principles that define GitOps, from the CNCF working group
- Docs: [Config Sync overview](https://cloud.google.com/kubernetes-engine/enterprise/config-sync/docs/overview) · Google's GitOps agent for GKE clusters and fleets
- Book: [GitOps and Kubernetes](https://www.manning.com/books/gitops-and-kubernetes) · Yuen, Matyushentsev, Ekenstam and Suen; patterns and practice with Argo CD and Flux
- Docs: [Config Connector overview](https://cloud.google.com/config-connector/docs/overview) · managing Google Cloud resources as Kubernetes objects
:::

### Environment Management for Data

::: plain
An **environment** is a complete, separate copy of the places your pipelines run and write to: one for experimenting (development), one for final checks (staging) and the real one (production). Data makes this harder than in ordinary software, because the code only behaves realistically on realistic data, and production data is large, expensive to copy and often private. Environment management is the craft of giving people realistic, safe and cheap places to work, like a flight simulator that feels real without risking a real plane.
:::

**Separate projects per environment.** On Google Cloud the cleanest boundary is a project per environment: `acme-data-dev`, `acme-data-staging`, `acme-data-prod`. Projects separate IAM, quotas, billing and accidental damage: a developer with full rights in dev cannot drop a production table. Keep the same dataset and table names inside each project, so code differs only in the project ID it is given.

**Per-developer namespaces.** Inside dev, each engineer (and each CI run) needs a private place to build. dbt does this with **targets**: the `dev` target writes to a schema named after the developer (`dbt_asha`), `ci` writes to `ci_pr_482`, `prod` writes to the real datasets. Dataform has the same idea with schema suffixes. On Kubernetes the equivalent is a **namespace** per team or developer, with a `ResourceQuota` capping CPU and memory.

**Parity: how alike should environments be?** Infrastructure should match exactly (the same Terraform modules with different sizes), software versions must match exactly, and data should be as realistic as privacy and cost allow.

**Getting realistic data into lower environments.**

| Technique | How | Strength | Weakness |
|---|---|---|---|
| Zero-copy clone | BigQuery `CREATE TABLE ... CLONE`, Snowflake `CLONE` | Instant, pay only for changed data | Still production data: privacy rules apply |
| Sample | `TABLESAMPLE SYSTEM (1 PERCENT)` | Small and cheap | Random samples break joins between tables |
| Consistent subset | Keep rows whose key hashes into a bucket | Joins still work across tables | Needs the same key on every table |
| Masked copy | Replace or hash personal fields | Safe for wide access | Masking must be complete and maintained |
| Synthetic data | Generate fake rows (Faker, SDV, Tonic) | No privacy risk at all | Misses the oddities of real data |

A **consistent subset** chooses the same customers in every table, so orders still join to customers and payments. Hashing the key makes the choice deterministic:

```python
import hashlib

def in_subset(key, percent):
    bucket = int(hashlib.sha256(str(key).encode()).hexdigest(), 16) % 100
    return bucket < percent

customers = range(1, 1001)
orders = [(oid, 1 + oid % 1000) for oid in range(1, 5001)]     # (order_id, customer_id)

kept_customers = {c for c in customers if in_subset(c, 10)}
kept_orders = [o for o in orders if in_subset(o[1], 10)]
print(len(kept_customers), len(kept_orders))                     # 93 465
print(all(c in kept_customers for _, c in kept_orders))          # True: no orphan orders
```

The same filter in BigQuery uses `MOD(ABS(FARM_FINGERPRINT(CAST(customer_id AS STRING))), 100) < 10` on every table.

**Clones and branches.** A BigQuery **table clone** is a writable copy that shares storage with its base table; you pay only for data that later changes. It is ideal for testing a risky backfill:

```sql
CREATE TABLE shop_dev.orders_backfill_test
CLONE shop.orders;
-- run the backfill against the clone, compare, then drop it
```

Table formats take this further with **branches**: Apache Iceberg supports named branches and tags on a table, and tools such as lakeFS and Project Nessie give Git-like branches across many tables at once, so a whole pipeline can run on a branch and be merged when correct.

> **Pitfall:** Copying production personal data into dev "just for a day". Development projects have broader access, weaker monitoring and longer-lived leftovers. Mask or subset under the same governance rules as production, or use synthetic data.

> **Tip:** Make the environment a parameter, never a code change. Pipelines receive `PROJECT_ID` or a dbt target; nothing in the code says `if env == "prod"` except configuration lookups.

::: deeper
**dbt's `generate_schema_name`.** By default dbt combines the target schema with a model's custom schema (`dbt_asha_marketing`). Many teams override the `generate_schema_name` macro so production uses the clean custom name (`marketing`) and other targets keep the prefix, giving clean production names and isolated developer schemas from the same code.

**Ephemeral environments.** For each pull request, CI can create a temporary dataset (with a default table expiration of a day or two) or a Kubernetes namespace, deploy the branch there, run tests and tear it down. Labels on every resource (`pr=482`) make cleanup and cost tracking straightforward.

**Masking strategies.** Deterministic hashing (`TO_HEX(SHA256(CONCAT(email, salt)))`) keeps join keys working while hiding values; format-preserving tokenisation keeps shapes (a fake but valid-looking phone number) for applications that validate formats. Sensitive Data Protection (formerly Cloud DLP) can discover and de-identify personal data in BigQuery tables. Masking must cover free-text fields, where personal data hides in comments and notes.

**Cost of environments.** Three copies of a large platform can triple storage and compute. Clones, subsets, autoscaling to zero in dev, expiration policies and scheduled shutdown of dev clusters keep lower environments to a fraction of production cost.

> **Interview:** "How do you test a pipeline change that needs production-scale data without touching production?" Clone the affected tables (BigQuery table clones or Iceberg branches) into an isolated dataset, run the new version against the clones, diff the output with the current production tables, and only then deploy; for privacy-sensitive data use masked or subsetted copies under production-grade access controls.
:::

::: check
?? Why use a separate Google Cloud project per environment rather than separate datasets in one project?
Projects separate IAM, quotas, billing and blast radius: broad rights in dev cannot affect production resources, and costs are attributed cleanly.

?? Why does a random 1% sample of each table break joins, and what fixes it?
Each table keeps a different random set of rows, so most orders lose their customers. A consistent subset keeps rows whose shared key (such as `customer_id`) hashes into the same bucket in every table.

?? What does a BigQuery table clone cost when you create it?
Almost nothing for storage at creation: it shares the base table's data, and you pay only for data that changes in the clone afterwards.
:::

::: resources
- Docs: [Introduction to table clones](https://cloud.google.com/bigquery/docs/table-clones-intro) · writable, storage-sharing copies of BigQuery tables
- Docs: [dbt environments](https://docs.getdbt.com/docs/environments-in-dbt) · targets, development and deployment environments in dbt
- Docs: [Apache Iceberg branching and tagging](https://iceberg.apache.org/docs/latest/branching/) · named branches and tags on table snapshots
- Docs: [lakeFS documentation](https://docs.lakefs.io/) · Git-like branches, commits and merges over object storage
- Docs: [Sensitive Data Protection](https://cloud.google.com/sensitive-data-protection/docs/sensitive-data-protection-overview) · discovering and de-identifying personal data before it reaches lower environments
- Docs: [Tonic.ai documentation](https://docs.tonic.ai/) · commercial test data generation and subsetting with referential integrity
:::

### Blue-Green & Canary Deployments for Pipelines

::: plain
Releasing a new version of a pipeline is like changing a bridge that thousands of cars cross every day. You do not knock down the old bridge and hope the new one holds. You build the new one next to it (**blue-green**), drive empty test lorries across it while the old one carries the traffic (**shadow**), then send a few real cars over (**canary**), and switch everyone only when it has proved itself. If anything looks wrong, traffic goes back to the old bridge in seconds.
:::

Application teams apply these ideas to web traffic. Data teams apply them to two things: **tables** that consumers read, and **pipelines** (especially streaming ones) that process incoming data.

**Blue-green for tables.** Consumers never read a physical table directly; they read a stable name that points at the current version. Build the new version (green) next to the current one (blue), validate green, then switch the pointer in one atomic step. In BigQuery the pointer is a view:

```sql
-- 1. Build green next to blue
CREATE OR REPLACE TABLE shop.fct_orders_v2 AS
SELECT ... ;                                            -- the new logic

-- 2. Validate green (row counts, key uniqueness, totals vs blue)
SELECT COUNT(*), COUNT(DISTINCT order_id), SUM(amount) FROM shop.fct_orders_v2;

-- 3. Switch consumers in one statement
CREATE OR REPLACE VIEW shop.fct_orders AS
SELECT * FROM shop.fct_orders_v2;

-- Rollback is the same statement pointing back at shop.fct_orders_v1
```

**Write-audit-publish (WAP)** is the same idea for every run of a pipeline, not just for releases: **write** the new data somewhere consumers cannot see it, **audit** it with data quality checks, and **publish** it only if the checks pass. With Iceberg you write to a branch and fast-forward `main` after the audit; in BigQuery you write to a staging table and publish with a partition swap or a `MERGE` inside a transaction. Bad data then never reaches dashboards.

[[fig:dataops_bluegreen]]

**Shadow mode.** Run the new pipeline version on the same input as the old one, writing to shadow tables that nobody reads, and compare the outputs for days. On Pub/Sub this is easy because each **subscription** receives every message: create a second subscription for the shadow pipeline. Compare continuously:

```python
def compare(blue, green, key="order_id", tol=0.01):
    b = {r[key]: r for r in blue}
    g = {r[key]: r for r in green}
    missing = sorted(b.keys() - g.keys())
    extra = sorted(g.keys() - b.keys())
    changed = sorted(k for k in b.keys() & g.keys()
                     if abs(b[k]["amount"] - g[k]["amount"]) > tol)
    return {"missing": missing, "extra": extra, "changed": changed}

blue  = [{"order_id": 1, "amount": 40.0}, {"order_id": 2, "amount": 15.5}, {"order_id": 3, "amount": 22.0}]
green = [{"order_id": 1, "amount": 40.0}, {"order_id": 2, "amount": 15.75}, {"order_id": 4, "amount": 9.5}]
report = compare(blue, green)
print(report)       # {'missing': [3], 'extra': [4], 'changed': [2]}
print("promote" if not any(report.values()) else "hold")    # hold
```

**Canary.** Send a small share of real traffic through the new version, compare its health and outputs with the old version, then raise the share in steps (1%, 10%, 50%, 100%). For data, split deterministically by key, so each customer is consistently handled by one version: `hash(customer_id) % 100 < 5` goes to the canary. Promotion and rollback are driven by metrics: error rate, latency, data quality checks and the diff above.

**Streaming pipelines on Dataflow.** You can **update** a running job in place (launch the new version with `--update` and the same job name; Dataflow checks compatibility and keeps the state), or run old and new jobs side by side and **drain** the old one, which stops reading new input and finishes processing what it has:

```bash
gcloud dataflow jobs drain JOB_ID --region=europe-west1
```

Blue-green with two jobs needs care with sinks: both versions must not write the same output twice, so the new job writes to new tables (switched by view) or the sink must be idempotent.

> **Pitfall:** Blue-green without a rollback rehearsal. Keep blue intact (do not drop `v1` on switch day), and practise switching back at least once. A rollback that has never been tried is a hope, not a plan.

> **Tip:** Automate the gate. Encode promotion rules ("row count within 0.5% of blue, zero duplicate keys, revenue total within 0.1%") as a CI or orchestration step that promotes or rolls back without a human reading charts at 3 a.m.

::: deeper
**State is the hard part for streaming.** A stateless pipeline can be replaced at any moment. A stateful one (windows, joins, deduplication) carries state that the new version must either inherit (Dataflow update, Flink savepoints with compatible operator IDs) or rebuild (replay input from a retained position: Pub/Sub seek to a snapshot or timestamp, Kafka consumer offsets). Plan the state story before the release.

**Comparing at scale.** Row-by-row comparison of large tables happens in the warehouse: a `FULL OUTER JOIN` on the key with per-column inequality counts, or hash-based comparisons of partitions (`BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t)))` per day) to find which partitions differ before drilling into rows.

**Tolerances.** Exact equality is the wrong test when the new version legitimately changes results (a bug fix, a new attribution rule). Define expected differences in advance, and treat anything outside them as a failure.

**Feature flags versus deployments.** Blue-green moves whole versions; flags switch individual behaviours inside one version. Many teams use both: deploy the new code dark (flag off), then use the flag for a canary by key range.

> **Interview:** "How would you release a rewritten revenue pipeline without risking wrong numbers on executive dashboards?" Run it in shadow mode on the same inputs, writing to separate tables; diff its outputs against the current pipeline daily with agreed tolerances; fix or explain every difference; then switch the consumer view in one step, keeping the old tables and pipeline ready for an instant switch back, and use write-audit-publish on every run afterwards.
:::

::: check
?? Why do consumers read a view instead of the physical table in a blue-green setup?
The view is a stable name that can be switched from the old table to the new one in one atomic statement, and switched back just as quickly for rollback.

?? How do you run a shadow streaming pipeline on Pub/Sub without disturbing the current one?
Create a second subscription on the same topic. Each subscription receives every message, so the shadow pipeline sees the same input while writing to its own tables.

?? Why split canary traffic by a hash of a key rather than at random per record?
So each entity (a customer, an account) is always processed by the same version, keeping per-entity results and state consistent and comparisons meaningful.
:::

::: resources
- Article: [BlueGreenDeployment](https://martinfowler.com/bliki/BlueGreenDeployment.html) · Martin Fowler's short definition of the technique and its trade-offs
- Docs: [Update an existing Dataflow pipeline](https://cloud.google.com/dataflow/docs/guides/updating-a-pipeline) · in-place updates, compatibility checks and transform mapping
- Docs: [Stopping a running Dataflow pipeline](https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline) · drain versus cancel, and what happens to in-flight data
- Book: [Site Reliability Workbook: Canarying releases](https://sre.google/workbook/canarying-releases/) · Google's chapter on canary design, metrics and evaluation
- Docs: [Pub/Sub seek](https://cloud.google.com/pubsub/docs/replay-overview) · replaying messages from a snapshot or timestamp to rebuild state
- Blog: [Write-Audit-Publish pattern](https://lakefs.io/blog/data-engineering-patterns-write-audit-publish/) · lakeFS on writing, auditing and publishing data through branches
:::

### Docker & Kubernetes for Data Engineers

::: plain
A **container** packages a program together with everything it needs (the right Python, libraries, system tools) into one sealed box that runs the same way on a laptop, in CI and in the cloud. **Docker** is the common tool for building those boxes. **Kubernetes** is a system that runs many containers across a fleet of machines: it decides where each one runs, restarts it if it crashes, and scales it up or down, like a port's crane system that places, moves and replaces shipping containers without anyone touching them by hand.
:::

**Building good images.** A **multi-stage build** uses one stage with build tools to install dependencies, then copies only the result into a small runtime image. Smaller images pull faster, start faster and carry fewer vulnerabilities:

```dockerfile
# Stage 1: install dependencies with uv
FROM python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv    # pin a version tag in real builds
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project      # dependencies only: cached layer
COPY src/ src/
RUN uv sync --frozen --no-dev                           # now the project itself

# Stage 2: small runtime image, no build tools, non-root user
FROM python:3.13-slim
RUN useradd --create-home app
WORKDIR /app
COPY --from=build /app /app
ENV PATH="/app/.venv/bin:$PATH"
USER app
ENTRYPOINT ["python", "-m", "orders_pipeline"]
```

Copying the dependency files before the source code means the slow dependency layer is rebuilt only when dependencies change, not on every code edit. Build, tag with the commit SHA and push to Artifact Registry:

```bash
docker build -t europe-west1-docker.pkg.dev/acme-data/pipelines/orders:3f9c2ab .
docker push europe-west1-docker.pkg.dev/acme-data/pipelines/orders:3f9c2ab
```

**Do you need Kubernetes at all?** Often not. For batch containers, **Cloud Run jobs** run a container to completion, in parallel tasks if you want, with no cluster to manage; each task gets its index in the `CLOUD_RUN_TASK_INDEX` environment variable so it can pick its share of the work:

```bash
gcloud run jobs create orders-load \
  --image=europe-west1-docker.pkg.dev/acme-data/pipelines/orders:3f9c2ab \
  --region=europe-west1 --tasks=10 --max-retries=3
gcloud run jobs execute orders-load --region=europe-west1
```

Use **GKE** (Google Kubernetes Engine) when you run long-lived, stateful or many interacting services: Airflow, Spark and Flink through operators, Trino, Kafka, or a self-service platform for many teams. **GKE Autopilot** manages the nodes for you and bills per pod resource request.

**The Kubernetes objects a data engineer meets.**

| Object | What it is for |
|---|---|
| Pod | One or more containers scheduled together; the unit Kubernetes runs |
| Deployment | Keeps N identical, stateless pods running (an API, the Airflow web server) |
| Job / CronJob | Runs pods to completion, once or on a schedule (a batch load) |
| StatefulSet | Pods with stable names and their own persistent disks (Kafka brokers, ZooKeeper, databases) |
| PersistentVolumeClaim | A request for durable storage, provisioned as a Persistent Disk through a StorageClass |
| Requests and limits | CPU and memory a container is guaranteed, and the most it may use |
| ResourceQuota | Total CPU, memory and object counts allowed in a namespace |
| Operator | A controller plus custom resource types that encode how to run a complex system |

**Operators** let you declare "a Spark application" or "a Flink job" as a Kubernetes object, and the operator creates and supervises the pods. With the Kubeflow Spark Operator:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: daily-totals
  namespace: spark-jobs
spec:
  type: Python
  mode: cluster
  image: europe-west1-docker.pkg.dev/acme-data/pipelines/spark-jobs:3f9c2ab
  mainApplicationFile: local:///app/jobs/daily_totals.py
  sparkVersion: "3.5.5"
  driver:
    cores: 1
    memory: 2g
    serviceAccount: spark
  executor:
    instances: 4
    cores: 2
    memory: 4g
```

The Apache Flink Kubernetes Operator does the same for Flink with a `FlinkDeployment` resource, including savepoint-based upgrades, and Strimzi runs Kafka clusters. **Helm** packages a set of Kubernetes manifests as a configurable **chart**; the official Apache Airflow chart is the usual way to run Airflow yourself on GKE.

> **Pitfall:** No resource requests and limits. Pods without requests get packed onto nodes until they starve each other; a Spark executor without a memory limit can take down its node. Always set requests (what you need) and memory limits (the most you may use).

> **Pitfall:** Running stateful systems on Kubernetes without a plan for their disks. A StatefulSet keeps each pod's volume across restarts, but backups, disk sizing and zone failures are still your job. Prefer managed services (Managed Service for Apache Kafka, Cloud SQL, BigQuery) unless you need the control.

::: deeper
**How scheduling works.** The scheduler places a pod on a node that has enough unrequested CPU and memory for its **requests**. **Limits** are enforced at run time: exceeding the CPU limit throttles the container, exceeding the memory limit kills it (`OOMKilled`). A pod's **quality of service** class (Guaranteed, Burstable, BestEffort) follows from its requests and limits and decides which pods are evicted first under memory pressure.

**Spark on Kubernetes.** Spark has native Kubernetes support: the driver runs in a pod and asks the Kubernetes API for executor pods. Shuffle files live on executor local disks, so losing an executor loses its shuffle output; decommissioning and shuffle-tracking settings soften this when using Spot VMs. Managed Service for Apache Spark can also run Spark on GKE clusters you own, combining Kubernetes packing with a managed Spark service.

**Image hygiene.** Pin base images by digest for reproducibility, scan images (Artifact Registry can scan for known vulnerabilities), run as a non-root user, avoid shells and package managers in runtime images where possible (distroless images), and never bake credentials into a layer: every layer is readable by anyone who can pull the image.

**Workload Identity Federation for GKE** maps a Kubernetes service account to IAM permissions, so pods call BigQuery or Cloud Storage without key files. It is the Kubernetes equivalent of attaching a service account to a VM.

> **Interview:** "Cloud Run jobs, GKE or Managed Service for Apache Spark for a nightly Python batch that processes 50 GB?" If it is plain Python with no cluster framework, Cloud Run jobs (parallel tasks) or a single large VM are simplest and cheapest. If it needs Spark, use serverless Spark batches. GKE is worth it only if you already run a platform on it or need fine control over scheduling and co-located services.
:::

::: check
?? Why copy `pyproject.toml` and `uv.lock` into the image before the source code?
Docker caches each layer. Installing dependencies from the lock file before copying the code means that layer is reused on code-only changes, so builds are much faster.

?? Which Kubernetes object suits Kafka brokers, and why not a Deployment?
A StatefulSet, because each broker needs a stable identity and its own persistent volume that survives restarts. A Deployment's pods are interchangeable and do not keep per-pod storage.

?? What happens when a container exceeds its memory limit?
The kernel kills it and Kubernetes reports `OOMKilled`; the pod is restarted according to its restart policy.
:::

::: resources
- Docs: [Docker multi-stage builds](https://docs.docker.com/build/building/multi-stage/) · building small, secure images with separate build and runtime stages
- Docs: [Kubernetes concepts](https://kubernetes.io/docs/concepts/) · pods, workloads, storage, scheduling and resource management
- Docs: [GKE overview](https://cloud.google.com/kubernetes-engine/docs/concepts/kubernetes-engine-overview) · Standard and Autopilot modes, Workload Identity and node pools
- Docs: [Cloud Run jobs](https://cloud.google.com/run/docs/create-jobs) · running containers to completion with parallel tasks and retries
- Book: [Kubernetes in Action](https://www.oreilly.com/library/view/kubernetes-in-action/9781617293726/) · Marko Lukša; how Kubernetes works, from pods to StatefulSets and operators
- Docs: [Apache Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) · FlinkDeployment resources, upgrades and savepoints
- Repo: [Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator) · SparkApplication resources for running Spark on Kubernetes
:::

## Testing

The delivery group gave you a pipeline that runs checks on every change. This group is about what those checks should be. Data pipelines fail in two different ways, and each needs its own kind of test. **Code defects** (a wrong join, an off-by-one date filter) are caught by tests that run the code on known input before deployment. **Data defects** (a source starts sending nulls, a partner file arrives twice) appear in production even when the code is perfect, and are caught by checks that run on every load.

The first topic is the strategy: which tests, at which level, with which data. The second covers the frameworks that implement them, from dbt tests to Great Expectations, Soda and Knowledge Catalog data quality scans, and how to choose among them.

### Data Pipeline Testing Strategies

::: plain
Testing a pipeline is like testing a kitchen. You check each recipe step on a small, known set of ingredients (unit tests), you check that the oven, fridge and suppliers actually work together (integration tests), you agree with suppliers on exactly what they deliver (contract tests), and now and then you cook a full, known menu and compare every dish with a photo of the perfect result (golden dataset regression tests). Each kind catches problems the others miss.
:::

[[fig:dataops_testpyramid]]

**Unit tests** check one transformation on tiny, hand-made input. They need no cloud, run in milliseconds and should be the bulk of your tests. The prerequisite is code structured as pure functions (see the design patterns topic in Programming for data engineering). This example is runnable with the standard library; under pytest you would drop the last loop:

```python
import csv, io

def daily_revenue(rows):
    """Sum paid amounts per day; ignore cancelled orders and duplicate order ids."""
    seen, totals = set(), {}
    for r in rows:
        if r["status"] != "paid" or r["order_id"] in seen:
            continue
        seen.add(r["order_id"])
        totals[r["day"]] = round(totals.get(r["day"], 0) + float(r["amount"]), 2)
    return totals

def test_ignores_cancelled():
    assert daily_revenue([{"order_id": "1", "day": "d1", "status": "cancelled", "amount": "5"}]) == {}

def test_dedupes():
    row = {"order_id": "1", "day": "d1", "status": "paid", "amount": "5"}
    assert daily_revenue([row, row]) == {"d1": 5.0}

GOLDEN_INPUT = """order_id,day,status,amount
1,2026-11-01,paid,40.00
2,2026-11-01,cancelled,15.50
3,2026-11-02,paid,22.00
3,2026-11-02,paid,22.00
4,2026-11-02,paid,9.50
"""
GOLDEN_OUTPUT = {"2026-11-01": 40.0, "2026-11-02": 31.5}

def test_golden():
    assert daily_revenue(csv.DictReader(io.StringIO(GOLDEN_INPUT))) == GOLDEN_OUTPUT

for t in (test_ignores_cancelled, test_dedupes, test_golden):
    t()
print("3 tests passed")      # 3 tests passed
```

For **PySpark**, start one local `SparkSession` per test session and compare small DataFrames. Spark ships `pyspark.testing.assertDataFrameEqual` (since 3.5), and the chispa library offers similar helpers with readable diffs:

```python
import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from jobs.daily_totals import paid_totals

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("tests").getOrCreate()

def test_paid_totals(spark):
    src = spark.createDataFrame(
        [("asha", 40.0, "paid"), ("bo", 15.5, "cancelled")],
        "customer string, amount double, status string")
    expected = spark.createDataFrame([("asha", 40.0)], "customer string, total double")
    assert_df_equality(paid_totals(src), expected, ignore_row_order=True)
```

For **SQL models**, dbt unit tests (next topic) feed fixed rows into one model and compare the output.

**Integration tests** check that your code works with the real systems: credentials, SQL dialect, schemas, serialisation. Run them against:

- **A real BigQuery test dataset**, created per CI run with a short default table expiration. There is no official BigQuery emulator, and dialect details matter, so test against the real service.
- **Emulators** where Google provides them: Pub/Sub, Bigtable, Firestore, Spanner and Datastore have local emulators (`gcloud beta emulators pubsub start`).
- **Containers** for open-source systems: Testcontainers starts PostgreSQL, Kafka or MinIO in Docker for the length of a test.

**DAG tests** catch the most common Airflow failure, a DAG file that does not import:

```python
from airflow.models import DagBag

def test_dags_import_cleanly():
    bag = DagBag(dag_folder="dags/", include_examples=False)
    assert bag.import_errors == {}

def test_every_dag_has_owner_and_retries():
    for dag in DagBag(dag_folder="dags/", include_examples=False).dags.values():
        assert dag.default_args.get("owner"), dag.dag_id
        assert dag.default_args.get("retries", 0) >= 1, dag.dag_id
```

**Contract tests** protect the boundary between a producer team and the consumers of its data. The **contract** states the schema, types, nullability, allowed values and freshness the producer promises. The producer's CI checks every change against the contract (so it cannot ship a breaking change by accident), and consumers' tests assume only what the contract states. On Google Cloud, Pub/Sub topics can have an attached **schema** (Avro or Protocol Buffers) that rejects non-conforming messages at publish time, which is a contract enforced by the platform; dbt **model contracts** (`contract: {enforced: true}`) make a model's build fail if its output columns or types drift from the declared ones.

**End-to-end and golden dataset tests.** A **golden dataset** is a frozen, representative input (including the nasty cases: duplicates, late events, nulls, time zone edges) together with its approved output. Run the whole pipeline on it in a test environment and diff the result with the approved output. Any difference is either a bug or an intended change that must be reviewed and re-approved. This catches regressions that no unit test anticipated.

> **Pitfall:** Tests that only use clean data. Real defects come from the messy cases: duplicate keys, null foreign keys, empty files, daylight-saving days, Unicode names, amounts as strings. Put them in your fixtures on purpose.

> **Tip:** Every production incident should leave a test behind. When bad data gets through, add the offending pattern to the golden dataset or a unit test, so the same failure can never ship again.

::: deeper
**Testing in production is part of the strategy.** Pre-deployment tests cannot see tomorrow's data. Checks that run on every load (row counts, uniqueness, null rates, freshness) are tests too, and they belong in the pipeline itself, ideally in a write-audit-publish step that blocks bad data before consumers see it. The balance is: deterministic code tests before deployment, data checks at run time, observability across everything.

**Flaky tests.** Integration tests that depend on shared datasets, the current date or real time are flaky. Inject the clock (pass `run_date` as a parameter), create unique resources per run (suffix with the CI run ID), and clean up with expirations rather than teardown code that might not run.

**Test data management.** Keep fixtures small and readable, next to the tests. Generate larger synthetic sets with a seed so they are reproducible. For golden datasets, store input and expected output in version control (or a versioned bucket) and require review for changes to expected output, exactly like code.

**Mutation thinking.** A useful review question for any test suite: "if I introduced a plausible bug (`>=` instead of `>`, a `LEFT JOIN` turned `INNER`), would a test fail?" If not, the suite tests the happy path only.

> **Interview:** "How do you test a pipeline that loads a partner's daily file into BigQuery?" Unit-test parsing and transformation on hand-made rows including bad ones; integration-test the load into a temporary BigQuery dataset; keep a golden file with its approved output for end-to-end regression; agree a contract with the partner and validate each file against it on arrival, quarantining failures; and run row count, uniqueness and freshness checks on every load before publishing.
:::

::: check
?? Why should unit tests be the majority of a pipeline's tests?
They are fast, deterministic and cheap, need no cloud resources, and pinpoint which transformation broke. Slower integration and end-to-end tests then cover what unit tests cannot.

?? What is a golden dataset test, and what does a difference in its output mean?
A frozen representative input run through the whole pipeline, compared with its approved output. A difference is either a regression or an intended change that must be reviewed and re-approved.

?? How does a Pub/Sub topic schema act as a contract?
Messages that do not match the attached Avro or Protocol Buffers schema are rejected at publish time, so producers cannot send malformed data to consumers.
:::

::: resources
- Docs: [pytest documentation](https://docs.pytest.org/en/stable/) · fixtures, parametrisation and plug-ins for Python test suites
- Docs: [PySpark testing utilities](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html) · `assertDataFrameEqual` and patterns for testing Spark code
- Repo: [chispa](https://github.com/MrPowers/chispa) · PySpark test helpers with readable DataFrame diffs
- Article: [Testing Airflow DAGs](https://www.astronomer.io/docs/learn/testing-airflow) · Astronomer's guide to DAG integrity, unit and integration tests
- Docs: [Testing apps locally with the Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator) · running Pub/Sub locally for integration tests
- Docs: [Pub/Sub schemas](https://cloud.google.com/pubsub/docs/schemas) · enforcing Avro or Protocol Buffers contracts on topics
- Book: *Data Quality Fundamentals* by Barr Moses, Lior Gavish and Molly Vorwerck · O'Reilly's book on testing, monitoring and owning data quality
:::

### Testing Frameworks for Data Engineering

::: plain
A data testing framework is a ready-made checklist machine. Instead of writing code for every check ("is this column ever empty?", "are these IDs unique?", "did today's data arrive?"), you list the checks in a short, readable form, and the framework turns them into queries, runs them, and reports what passed and what failed. Different frameworks suit different kitchens: some live inside your SQL project, some in Python, some are built into the cloud platform.
:::

All the frameworks below express the same families of checks: **completeness** (not null), **uniqueness**, **validity** (ranges, allowed values, formats), **referential integrity** (every foreign key exists), **freshness** (latest data is recent), **volume** (row counts in expected ranges) and **distribution** (means, null rates, category shares stay stable). They differ in where they run and who writes them.

**dbt tests** live next to the models they test. **Data tests** are queries that return failing rows; generic ones are declared in YAML:

```yaml
models:
  - name: fct_orders
    config:
      contract: {enforced: true}
    columns:
      - name: order_id
        data_type: int64
        data_tests: [unique, not_null]
      - name: status
        data_type: string
        data_tests:
          - accepted_values:
              arguments:
                values: ["paid", "cancelled", "refunded"]
      - name: customer_id
        data_type: int64
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_customers')
                field: customer_id
      - name: amount
        data_type: numeric
```

A **singular test** is any SQL file in `tests/` that returns the rows that break a rule. **Unit tests** (since dbt 1.8) check a model's logic on fixed input rows, before it touches real data:

```yaml
unit_tests:
  - name: fct_orders_keeps_only_paid
    model: fct_orders
    given:
      - input: ref('stg_orders')
        rows:
          - {order_id: 1, customer_id: 7, status: paid, amount: 40}
          - {order_id: 2, customer_id: 8, status: cancelled, amount: 15}
    expect:
      rows:
        - {order_id: 1, customer_id: 7, status: paid, amount: 40}
```

`dbt build` runs models and their tests in dependency order and skips downstream models when a test fails, so bad data stops spreading. Packages such as `dbt-expectations` add dozens of extra generic tests, and `store_failures` writes failing rows to a table for inspection.

**Dataform** (Google Cloud's SQL transformation service in BigQuery) has **assertions** declared in a table's config, which run as queries after the table is built:

```sql
config {
  type: "table",
  assertions: {
    uniqueKey: ["order_id"],
    nonNull: ["order_id", "customer_id"],
    rowConditions: ["amount >= 0"]
  }
}
SELECT * FROM ${ref("stg_orders")} WHERE status = "paid"
```

**Great Expectations (GX)** is a Python framework. You group **expectations** into suites, run them against batches of data (pandas, Spark or SQL tables) and get results plus HTML "data docs":

```python
import great_expectations as gx
import pandas as pd

df = pd.DataFrame({"order_id": [1, 2, 2], "amount": [40.0, -3.0, 9.5]})

context = gx.get_context()
batch = (context.data_sources.add_pandas("local")
         .add_dataframe_asset("orders")
         .add_batch_definition_whole_dataframe("all")
         .get_batch(batch_parameters={"dataframe": df}))

suite = context.suites.add(gx.ExpectationSuite(name="orders"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0))
print(batch.validate(suite).success)        # False: a duplicate id and a negative amount
```

**Soda** uses a YAML check language, SodaCL, read by the open-source Soda Core library and Soda's commercial platform. Checks are compact and readable by analysts:

```yaml
checks for orders:
  - row_count > 0
  - missing_count(order_id) = 0
  - duplicate_count(order_id) = 0
  - invalid_count(status) = 0:
      valid values: [paid, cancelled, refunded]
  - freshness(created_at) < 1d
```

**Knowledge Catalog data quality scans** (Knowledge Catalog was formerly Dataplex Universal Catalog) are Google Cloud's managed option: you declare rules for a BigQuery table, and Google runs them on a schedule, without infrastructure, publishing results to the catalog, to BigQuery and to Cloud Logging for alerting:

```yaml
# dq.yaml
rules:
  - column: order_id
    dimension: UNIQUENESS
    uniquenessExpectation: {}
  - column: customer_id
    dimension: COMPLETENESS
    nonNullExpectation: {}
  - column: amount
    dimension: VALIDITY
    ignoreNull: true
    rangeExpectation:
      minValue: "0"
```

```bash
gcloud dataplex datascans create data-quality orders-dq \
  --location=europe-west1 \
  --data-source-resource="//bigquery.googleapis.com/projects/acme-data/datasets/shop/tables/orders" \
  --data-quality-spec-file=dq.yaml
```

**Choosing.**

| Framework | Runs where | Written by | Best when |
|---|---|---|---|
| dbt tests and unit tests | In the warehouse, during `dbt build` | Analytics engineers | Your transformations are dbt models |
| Dataform assertions | In BigQuery, during Dataform runs | Analytics engineers | Your transformations are Dataform on BigQuery |
| Great Expectations | Python, against pandas, Spark or SQL | Data engineers | Python pipelines, validating files before load, many engines |
| Soda (SodaCL) | Generates SQL for many warehouses | Engineers and analysts | Readable checks across several data sources |
| Knowledge Catalog data quality | Managed by Google on BigQuery | Platform and governance teams | No infrastructure; results tied to the catalog |
| pandera | Python DataFrames (pandas, Polars, PySpark) | Data engineers | Schema checks inside Python code |

**Property-based testing** generates many random inputs and checks that a property always holds, instead of checking a few examples. The Hypothesis library does this for Python; the idea is visible even with the standard library:

```python
import random

def dedupe(rows):
    seen, out = set(), []
    for r in rows:
        if r["id"] not in seen:
            seen.add(r["id"]); out.append(r)
    return out

rng = random.Random(42)
for _ in range(500):
    rows = [{"id": rng.randint(1, 20)} for _ in range(rng.randint(0, 50))]
    once = dedupe(rows)
    assert dedupe(once) == once                                  # idempotent
    assert len({r["id"] for r in once}) == len(once)             # ids unique
    assert {r["id"] for r in once} == {r["id"] for r in rows}    # nothing lost
print("500 random cases passed")       # 500 random cases passed
```

> **Pitfall:** Hundreds of tests that nobody acts on. If a failing test only produces a warning in a log, the team learns to ignore it. Decide per test whether it **blocks** publishing (severity error) or **warns** a named owner, and delete tests that nobody would fix.

> **Tip:** Test the keys first. `unique` and `not_null` on every primary key, and `relationships` on important foreign keys, catch the majority of real data defects (duplicates and fan-out joins) for very little effort.

::: deeper
**Where to place checks.** Check at the boundaries: on arrival (validate raw files or messages against the contract and quarantine failures), after each layer that changes grain (staging, then marts), and before publishing to consumers. Checking every column of every table on every run costs money in BigQuery; focus on keys, critical metrics and fields that consumers filter on.

**Thresholds instead of absolutes.** "Zero nulls" is right for keys; for other columns, "null rate below 2%" or "row count within 30% of the 7-day median" avoids alert fatigue. dbt supports `warn_if` and `error_if` thresholds; Soda and Knowledge Catalog accept percentage thresholds per rule.

**Cost of checks.** Each check is a query. A uniqueness test over a 5 TB table scans the key column every run. Restrict checks to the newly loaded partitions (`where: "order_date = current_date()"` in dbt test config, incremental scans in Knowledge Catalog) and run full-table checks less often.

**Data contracts as specifications.** The Open Data Contract Standard (ODCS), maintained by the Bitol project under the Linux Foundation, is a YAML format describing a dataset's schema, quality rules, owners and service levels. Tools can generate dbt tests, Soda checks or Knowledge Catalog rules from one contract, keeping producer and consumer expectations in one place.

> **Interview:** "dbt tests or Great Expectations?" If transformations are dbt models in the warehouse, dbt tests are closest to the code and run in the same DAG, which is usually enough. Great Expectations fits Python-centric pipelines, validation before data reaches the warehouse, and checks across several engines. Many teams use dbt tests for models and a lighter tool (or managed scans) for raw arrivals.
:::

::: check
?? What is the difference between a dbt data test and a dbt unit test?
A data test runs against real built data and returns failing rows (for example duplicate keys). A unit test feeds fixed input rows into one model and compares its output with expected rows, checking the logic before real data is involved.

?? Why restrict checks on huge tables to new partitions?
Each check is a query billed by the bytes it scans. Checking only newly loaded partitions keeps cost proportional to the new data, while catching problems in what just changed.

?? What does property-based testing check that example-based tests miss?
It generates many random inputs and checks that general properties always hold (idempotence, uniqueness, nothing lost), finding edge cases nobody thought to write as examples.
:::

::: resources
- Docs: [dbt data tests](https://docs.getdbt.com/docs/build/data-tests) · generic and singular tests, severity and storing failures
- Docs: [dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests) · testing model logic on fixed input rows
- Docs: [Great Expectations documentation](https://docs.greatexpectations.io/docs/) · GX Core: data sources, expectations, suites and checkpoints
- Docs: [Soda documentation](https://docs.soda.io/) · SodaCL checks, Soda Core and data contracts
- Docs: [Knowledge Catalog data quality overview](https://cloud.google.com/dataplex/docs/data-quality-overview) · managed data quality scans on BigQuery tables
- Docs: [Dataform assertions](https://cloud.google.com/dataform/docs/assertions) · built-in and manual assertions for Dataform tables
- Docs: [Hypothesis](https://hypothesis.readthedocs.io/en/latest/) · property-based testing for Python
:::

## Reliability

Testing catches what you can predict before a change ships. Production brings everything else: a source system that is down at 02:00, a partner file that arrives twice, a quota that runs out, a Spark executor that dies mid-shuffle, a credential that expires on a Sunday. Reliability engineering accepts that failures will happen and asks better questions: how reliable does each dataset actually need to be, how quickly will we notice when it is not, how do we respond, and how do we make the same failure less likely next time.

This group borrows the vocabulary of site reliability engineering (SRE) and applies it to data. It starts with the targets (SLIs, SLOs and error budgets), then the signals that tell you whether you are meeting them (monitoring and alerting, logs), then what you do when you are not (incident management), how you build confidence before failures happen for real (chaos engineering), how observability tools watch the data itself, and finally how the credentials that every pipeline needs are kept safe, because a leaked or expired secret is one of the most common causes of both outages and breaches. Cloud Monitoring and Cloud Logging basics were covered in Google Cloud foundations for data; here the focus is on the practices built on top of them.

### Data Platform SRE — Reliability Engineering

::: plain
Site reliability engineering is the habit of treating reliability as a feature with a number attached. Instead of promising "the sales table is always right and always on time", you agree on a measurable target, such as "ready by 07:00 on 99% of days", and you measure it. The 1% you are allowed to miss is a budget: while you have budget left you can ship changes quickly, and when it is spent you slow down and fix things. It is like a household budget for failure: spending some is normal, overspending means you change your habits.
:::

Three terms come first, and people mix them up constantly:

- A **service level indicator (SLI)** is a measurement of how the service behaves, expressed as a ratio of good events to valid events. For a data product: "the share of days on which `fct_orders` was complete and published by 07:00 UTC", or "the share of events that reached BigQuery within 5 minutes of being published".
- A **service level objective (SLO)** is the target for an SLI over a window: "99% of days over a rolling 90 days".
- A **service level agreement (SLA)** is a promise to someone else with consequences if it is broken (a refund, an escalation to leadership). SLAs should be looser than SLOs, so you notice and react before you break a promise.

**Data SLIs.** Data has its own dimensions of "good". The useful ones map to what consumers feel:

| Dimension | SLI example | Measured from |
|---|---|---|
| Freshness | Share of days the daily partition landed by 07:00 | Partition metadata, orchestrator run end times |
| Latency (streams) | Share of events visible in BigQuery within 5 minutes | Event time versus load time, Pub/Sub oldest unacked age |
| Completeness | Share of loads with row count within 2% of the source count | Reconciliation query against the source |
| Correctness | Share of runs where all blocking quality checks passed | dbt or Knowledge Catalog check results |
| Availability | Share of successful queries against the serving layer | BigQuery job errors, API error rates |

Pick two or three per important data product, not ten. A good SLI is one whose failure a consumer would notice and complain about.

**Error budgets.** The **error budget** is `1 - SLO`. With a 99% daily-freshness SLO over 90 days you may miss 0.9 days, so in practice zero misses per quarter is the target and one miss already overspends. That tells you the SLO is probably too tight for a daily table, or the window too short. Worked numbers help you choose:

```python
def budget(slo, events):
    allowed = (1 - slo) * events
    return round(allowed, 2)

# Daily batch table over a 90-day window: 90 events
print(budget(0.99, 90), budget(0.95, 90))       # 0.9 4.5
# Hourly partitions over 30 days: 720 events
print(budget(0.99, 720), budget(0.995, 720))    # 7.2 3.6
# Streaming: 5-minute windows over 30 days: 8640 events
print(budget(0.999, 8640))                      # 8.64

misses = 3
print(f"hourly, 99%: {misses / budget(0.99, 720):.0%} of budget spent")
# hourly, 99%: 42% of budget spent
```

Low-frequency pipelines have few events, so their budgets are coarse. Daily tables usually get SLOs such as "95% of days by 07:00, 100% by 10:00" over a quarter.

**Error budget policy.** The budget is only useful if the team agrees in advance what happens when it is spent. A written **error budget policy** says, for example: when a data product has spent its quarterly budget, feature work on its pipelines stops until reliability work (the top items from the postmortems) is done; a single incident that consumes more than 20% of the budget gets a postmortem; repeated budget exhaustion is escalated to the engineering lead. This turns arguments ("should we fix flaky loads or build the new model?") into a rule both sides accepted earlier.

[[fig:dataops_slo]]

**Toil** is manual, repetitive, automatable work that grows with the size of the platform: re-running failed tasks by hand, backfilling partitions after every source outage, granting dataset access through tickets. The Google SRE book suggests keeping toil below half of an engineer's time. Measure it (a label on tickets, a weekly estimate), then automate the biggest item: automatic retries with backoff, self-service backfill commands, access through groups and Terraform.

**Runbooks** turn tribal knowledge into steps that a sleepy on-call engineer can follow. A good data runbook entry is short and specific:

```text
ALERT: fct_orders freshness SLO at risk (partition not landed by 06:30)
1. Check the Composer DAG orders_daily: which task is failing or still running?
2. If extract_orders failed with a 401: the source API token expired.
   Rotate it (runbook: secrets/salesforce-token) and clear the failed task.
3. If the source export is late: check the vendor status page; post in #data-status
   with the expected time; do not backfill until the export exists.
4. If dbt tests failed: open the store_failures table named in the log, decide
   whether the rows are a real defect. Never publish over a blocking test.
5. Escalate to the orders data product owner if not resolved by 07:00.
```

**Blameless postmortems.** After a significant incident, the team writes what happened, its impact, the timeline, the contributing causes and the actions, without blaming individuals. The assumption is that people acted reasonably with the information and tools they had, so the fix is to the system (a missing check, a confusing alert, a manual step) rather than to a person. The incident management topic covers the process.

> **Pitfall:** SLOs nobody looks at. An SLO is useful only if it drives decisions: it should be on a dashboard that the team reviews weekly, it should generate alerts when the budget burns fast, and the error budget policy should actually be applied. Otherwise it is a number in a wiki page.

> **Tip:** Start from consumers. Ask the owners of the three most important dashboards or models "when do you need this data, and what happens if it is late or wrong?". Their answers are your first SLOs.

::: deeper
**Burn-rate alerting.** Alerting when the SLI dips once is noisy; alerting when the budget is gone is too late. The SRE Workbook recommends alerting on **burn rate**, the speed at which the budget is being spent relative to plan. A burn rate of 1 spends exactly the budget over the window; a burn rate of 14.4 spends 2% of a 30-day budget in one hour. Multi-window alerts (fast burn over 1 hour and 5 minutes pages someone; slow burn over 6 hours and 30 minutes opens a ticket) catch both sudden outages and slow degradations. Cloud Monitoring's service monitoring supports SLOs and burn-rate alerting policies natively for request-based and windows-based SLIs; data freshness SLIs are typically exported as custom metrics first.

**Windows-based versus event-based SLIs.** "99% of 5-minute windows had lag under 2 minutes" (windows-based) treats a quiet night and a busy afternoon the same. "99% of events arrived within 2 minutes" (event-based) weights by volume. Event-based matches user impact better for streams; windows-based is easier when you cannot count events.

**Dependencies cap your SLO.** If your pipeline depends on a source that is available 99.5% of the time and you cannot buffer its outages, your freshness SLO cannot honestly exceed 99.5%. Either negotiate with the source owner, add buffering and retries that hide its outages from consumers, or set your SLO below theirs.

**Data products as services.** In a data mesh (Data governance, security and compliance), each data product publishes its SLOs in its contract or catalog entry, so consumers can choose and trust products, and platform teams can compare reliability across domains.

> **Interview:** "How would you define reliability for a daily revenue table?" Agree on SLIs with consumers: freshness (published by 07:00), correctness (all blocking checks passed, totals reconcile with the payment system within 0.1%) and completeness. Set SLOs over a quarter (for example 95% of days on time, 100% by 10:00), publish an error budget policy, alert on budget burn, and review misses in postmortems.
:::

::: check
?? A table has a 99.5% SLO on hourly freshness over 30 days. How many late hours does the budget allow?
720 hourly events times 0.005, which is 3.6, so three late hours fit in the budget and a fourth exceeds it.

?? Why should an SLA be looser than the internal SLO?
So that you notice the SLO being missed and react before you break the external promise, which has consequences.

?? What is toil, and what should you do about it?
Manual, repetitive, automatable work that grows with the platform (re-running failed tasks, manual backfills, access tickets). Measure it, keep it below about half of the team's time, and automate the largest items first.
:::

::: resources
- Book: [Site Reliability Engineering](https://sre.google/sre-book/table-of-contents/) · Google's free SRE book; read the chapters on SLOs, toil and postmortems
- Book: [Implementing SLOs](https://sre.google/workbook/implementing-slos/) · SRE Workbook chapter with worked SLO and error budget examples
- Book: [Alerting on SLOs](https://sre.google/workbook/alerting-on-slos/) · burn-rate and multi-window alerting explained step by step
- Docs: [Error budget policy example](https://sre.google/workbook/error-budget-policy/) · a complete policy you can adapt for data products
- Docs: [Cloud Monitoring SLO monitoring](https://cloud.google.com/stackdriver/docs/solutions/slo-monitoring) · defining SLOs and burn-rate alerts on Google Cloud
- Book: [Implementing Service Level Objectives](https://www.oreilly.com/library/view/implementing-service-level/9781492076803/) · Alex Hidalgo; choosing SLIs and running an SLO culture
- Blog: [Operational excellence in data quality at Uber](https://www.uber.com/blog/operational-excellence-data-quality/) · data quality SLAs and incident practice at scale
:::

### Pipeline Monitoring & Alerting

::: plain
Monitoring is the dashboard of a car: gauges that show speed, fuel and temperature all the time. Alerting is the warning light and the beep that get your attention only when something needs action now. A good setup shows you everything on the dashboard but beeps rarely, and only for things you must deal with immediately: an empty fuel tank, not a slightly warm engine.
:::

A data platform needs three layers of signals:

1. **Infrastructure and service metrics**: CPU, memory, worker counts, Pub/Sub backlog, BigQuery slot usage, Airflow scheduler heartbeat. Mostly built in (Cloud Monitoring collects them for Google Cloud services; GKE and self-run tools expose Prometheus metrics).
2. **Pipeline metrics**: task duration, success and failure counts, retries, rows read and written per run, late-arriving records.
3. **Data metrics**: freshness, volume, null rates, check results per table. These are the closest to what consumers feel, so they belong in your SLOs.

**Prometheus and Grafana.** Prometheus is an open-source monitoring system that **scrapes** (pulls) metrics over HTTP from targets every 15 to 60 seconds and stores them as time series identified by a metric name and **labels** (`dag="orders_daily", task="load"`). Its query language, **PromQL**, computes rates, aggregations and ratios. Grafana is the open-source dashboard and alerting layer that queries Prometheus, Cloud Monitoring, BigQuery, Loki and many other sources. On Google Cloud, **Google Cloud Managed Service for Prometheus** runs the storage and querying for you: you keep PromQL, Prometheus-style collection on GKE and Grafana dashboards, and metrics are stored in Monarch, the same backend as Cloud Monitoring.

Metric types matter. A **counter** only goes up (rows processed, failures); you graph its rate. A **gauge** goes up and down (backlog, last success timestamp). A **histogram** counts observations in buckets (task duration), so you can compute percentiles.

```text
# rows written per second by table, over 5 minutes
sum by (table) (rate(pipeline_rows_written_total[5m]))

# failure ratio of a DAG over the last hour
sum(rate(task_failures_total{dag="orders_daily"}[1h]))
  / sum(rate(task_runs_total{dag="orders_daily"}[1h]))

# 95th percentile task duration
histogram_quantile(0.95, sum by (le, task) (rate(task_duration_seconds_bucket[1h])))

# freshness: seconds since the table last loaded successfully
time() - max by (table) (table_last_success_timestamp_seconds)
```

**Batch jobs and the Pushgateway.** A batch job may end before Prometheus scrapes it. The **Pushgateway** is a small service that a job pushes its final metrics to; Prometheus scrapes the gateway. Push only a few metrics per job (last success timestamp, duration, rows), because the gateway keeps them until deleted:

```python
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

registry = CollectorRegistry()
last_ok = Gauge("table_last_success_timestamp_seconds", "Last successful load",
                ["table"], registry=registry)
rows = Gauge("table_rows_loaded", "Rows loaded by the last run", ["table"], registry=registry)

# ... run the load ...
last_ok.labels(table="fct_orders").set_to_current_time()
rows.labels(table="fct_orders").set(10431)
push_to_gateway("pushgateway.monitoring:9091", job="orders_daily", registry=registry)
```

**Alerting rules.** In Prometheus, alerting rules are PromQL expressions with a `for` duration (the condition must hold that long before firing, which filters blips). **Alertmanager** groups, deduplicates, silences and routes the resulting alerts to receivers such as PagerDuty, Slack or email:

```yaml
# rules.yml
groups:
  - name: data-freshness
    rules:
      - alert: FctOrdersStale
        expr: time() - table_last_success_timestamp_seconds{table="fct_orders"} > 26 * 3600
        for: 15m
        labels: {severity: page, team: orders}
        annotations:
          summary: "fct_orders has not loaded for over 26 hours"
          runbook: "https://wiki.example.com/runbooks/fct-orders"
```

```yaml
# alertmanager.yml (excerpt)
route:
  receiver: slack-data
  group_by: [alertname, table]
  routes:
    - matchers: [severity="page"]
      receiver: pagerduty-data
receivers:
  - name: pagerduty-data
    pagerduty_configs:
      - routing_key: <from-secret-manager>
  - name: slack-data
    slack_configs:
      - channel: "#data-alerts"
        api_url_file: /etc/alertmanager/slack-url
```

**PagerDuty** (and similar tools such as Opsgenie or incident.io) manages on-call schedules, escalation and acknowledgement. Its **Events API v2** accepts a JSON event with a routing key, an action (`trigger`, `acknowledge`, `resolve`) and a **dedup key**, so repeated alerts for the same problem update one incident instead of opening many. Cloud Monitoring, Alertmanager and Grafana all have PagerDuty integrations.

**Orchestrator alerts.** Airflow's `on_failure_callback` on a task or DAG sends a notification when it fails. For lateness, Airflow 2 had **SLA misses** (`sla=timedelta(hours=2)` on tasks plus an `sla_miss_callback`), which were checked only after tasks finished and were removed in Airflow 3.0. Airflow 3.1 added **Deadline Alerts** as the replacement (marked experimental at first): a reference time (for example when the DAG run was queued), an interval, and a callback that fires as soon as the deadline passes, even if the run is still hanging. Whatever your Airflow version, also measure freshness on the table itself, because a DAG can succeed while writing nothing.

**What deserves a page.** Page on **symptoms** that hurt consumers soon and need a human: an SLO burning fast, a critical table not landed, a stream backlog growing for 15 minutes. Send **causes** (one failed task that will retry, a slow query) to a ticket or a chat channel. Every page links a runbook.

> **Pitfall:** Alerting on every task failure. Airflow retries, so most failures heal themselves, and a page for each one teaches the on-call engineer to ignore pages. Alert when retries are exhausted or when the table's freshness is at risk.

> **Tip:** Track alert quality. For each alert over a month, count how many led to action. Delete or downgrade any alert with no action, and add a missing alert after any incident a human noticed before the monitoring did.

::: deeper
**Label cardinality.** Every distinct combination of label values is a separate time series. A label such as `customer_id` or `run_id` creates millions of series, which slows Prometheus and raises Managed Service for Prometheus costs (billed per sample ingested). Keep labels to bounded sets: DAG, task, table, environment, team.

**Absence alerts.** A broken exporter produces no data, and "value above threshold" never fires on no data. Add `absent(table_last_success_timestamp_seconds{table="fct_orders"})` rules or Cloud Monitoring metric-absence conditions for critical metrics.

**Anomaly-based thresholds.** Fixed thresholds fit freshness; volumes vary by weekday and season. Compare with the same hour last week (`offset 1w` in PromQL) or use observability tools that learn seasonality (the data observability topic).

**Alert routing as code.** Keep Prometheus rules, Alertmanager routes and Cloud Monitoring alert policies (Terraform `google_monitoring_alert_policy`) in Git, reviewed and deployed through CI like everything else in the delivery group.

> **Interview:** "How would you monitor 300 Airflow DAGs without drowning in alerts?" Export pipeline metrics and per-table freshness and volume; define SLOs for the important tables; page only on SLO burn and exhausted retries on critical paths; route everything else to team channels with owners from labels; group and deduplicate in Alertmanager or PagerDuty; review alert usefulness monthly.
:::

::: check
?? Why is a counter graphed with `rate()` while a gauge is graphed directly?
A counter only increases, so its raw value is not meaningful; its rate of increase is. A gauge already represents a current level, such as backlog or a timestamp.

?? What is the Pushgateway for, and what should you push to it?
It holds metrics from short-lived batch jobs that end before Prometheus can scrape them. Push a few summary metrics per job, such as last success timestamp, duration and rows loaded.

?? Why measure table freshness even when Airflow reports every DAG run as successful?
A run can succeed while writing no rows or writing to the wrong partition. Freshness on the table measures what consumers actually see.
:::

::: resources
- Docs: [Prometheus overview](https://prometheus.io/docs/introduction/overview/) · architecture, data model, metric types and PromQL
- Docs: [When to use the Pushgateway](https://prometheus.io/docs/practices/pushing/) · the right and wrong uses of pushing metrics from batch jobs
- Docs: [Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/) · grouping, inhibition, silences and routing
- Docs: [Google Cloud Managed Service for Prometheus](https://cloud.google.com/stackdriver/docs/managed-prometheus) · PromQL and Prometheus collection on GKE without running Prometheus storage
- Docs: [Grafana documentation](https://grafana.com/docs/grafana/latest/) · dashboards, data sources and Grafana alerting
- Docs: [Airflow Deadline Alerts](https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html) · the Airflow 3 replacement for SLA misses
- Book: [Prometheus: Up & Running](https://www.oreilly.com/library/view/prometheus-up/9781492034131/) · Brian Brazil; instrumentation, PromQL and alerting in depth
:::

### Logging Best Practices for Data Systems

::: plain
Logs are a ship's logbook: a time-ordered record of what happened, written as it happens. A good logbook entry says who, what, where and when in a fixed format, so that weeks later someone can find every entry about one storm in seconds. A bad one is a scribbled sentence nobody can search. Structured logging is simply writing every entry in the same fixed format, so machines can search and count them.
:::

**Plain text versus structured.** `Loaded 10431 rows into fct_orders in 42s` is easy to read but hard to query: to find all loads slower than 30 seconds, you need a regular expression. The same event as JSON, one object per line, can be filtered and aggregated by any field:

```json
{"severity": "INFO", "message": "load finished", "table": "fct_orders", "rows": 10431, "duration_s": 42.1, "run_id": "orders_daily__2026-11-22", "correlation_id": "c-7f3a"}
```

On Cloud Run, GKE, Cloud Run functions and Managed Service for Apache Spark (formerly Dataproc), a JSON object printed on one line to standard output is parsed by the logging agent into a structured entry, with `severity` and `message` mapped to Cloud Logging's own fields. Elsewhere, agents such as Fluent Bit or the OpenTelemetry Collector do the parsing.

**Log levels** say how much attention an entry deserves:

| Level | Use for | Example |
|---|---|---|
| DEBUG | Detail useful only while investigating | Generated SQL, per-file decisions |
| INFO | Normal milestones | Run started, rows loaded, run finished |
| WARNING | Something odd that the system handled | Retry 1 of 3, 12 rows quarantined |
| ERROR | An operation failed | Load failed after retries |
| CRITICAL | The service cannot continue | Credentials invalid, cannot start |

Run production at INFO and switch a single component to DEBUG through configuration when needed.

**Correlation IDs.** One business run touches many systems: an Airflow task starts a Dataflow job that writes to BigQuery and triggers a dbt build. A **correlation ID** generated at the start and passed along (as a job label, a Pub/Sub message attribute, an HTTP header, a dbt variable) lets you search every log line of that run across all systems. In Python, `contextvars` carries the ID through the code without passing it to every function, and a formatter adds it to every line. This runs with the standard library:

```python
import json, logging, sys, contextvars

correlation_id = contextvars.ContextVar("correlation_id", default="-")

class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {"severity": record.levelname, "message": record.getMessage(),
                 "logger": record.name, "correlation_id": correlation_id.get()}
        entry.update(getattr(record, "fields", {}))
        return json.dumps(entry)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
log = logging.getLogger("orders")
log.addHandler(handler)
log.setLevel(logging.INFO)

def load(table, rows):
    log.info("load finished", extra={"fields": {"table": table, "rows": rows}})

correlation_id.set("c-7f3a")
load("fct_orders", 10431)
log.debug("not printed at INFO level")
# {"severity": "INFO", "message": "load finished", "logger": "orders", "correlation_id": "c-7f3a", "table": "fct_orders", "rows": 10431}
```

The entry has no timestamp field because Cloud Logging records one when it receives the line; for other destinations add a `"time"` field in RFC 3339 format.

**What to log in a pipeline.** At the start: run ID, correlation ID, code version (commit SHA), parameters such as the processing date. At the end: rows read, written and rejected, bytes, duration and outcome. On errors: the exception with stack trace and the object being processed (file name, partition). Never log record contents that may hold personal data, and never log secrets.

**Aggregation and storage.** Logs from many machines must end up in one searchable place. The common pattern is agent, pipeline, store:

- **Agents** on each node or sidecar collect stdout and files (Fluent Bit, the OpenTelemetry Collector, the Ops Agent on Compute Engine).
- An optional **pipeline** buffers, parses, enriches, filters and routes (Logstash, Vector, the collector again, or Cloud Logging's Log Router and sinks).
- **Stores**: Cloud Logging for Google Cloud workloads; the **ELK** stack (Elasticsearch, Logstash, Kibana) or its fork **OpenSearch** with OpenSearch Dashboards for full-text search; **Grafana Loki**, which indexes only labels and stores log lines compressed in object storage such as Cloud Storage, making it cheap for large volumes; BigQuery for long-term analysis with SQL.

**Retention tiers.** Recent logs are searched often, old ones rarely. Keep 7 to 30 days in the fast, expensive tier (Cloud Logging buckets, Elasticsearch hot nodes), route everything to Cloud Storage or BigQuery for months or years at a fraction of the cost, and keep audit logs as long as compliance requires. Elasticsearch and OpenSearch automate this with index lifecycle policies (hot, warm, cold, delete); Cloud Logging sets retention per log bucket.

**Log-based metrics** count or extract values from log entries matching a filter (for example, entries with `jsonPayload.message="row rejected"` grouped by `jsonPayload.table`) and make them alertable metrics. They are a quick way to add monitoring to a system you cannot instrument, but metrics emitted directly are cheaper at high volume.

> **Pitfall:** Logging every record. A pipeline processing 50 million rows that logs one line per row produces gigabytes of logs per run and a large bill (Cloud Logging and most log stores charge per GiB ingested). Log counts and samples, and send rejected rows to a quarantine table, not to the log.

> **Tip:** Agree on a shared field schema across the platform (`table`, `run_id`, `correlation_id`, `dag`, `task`, `env`, `team`). Consistent names are what make cross-system search work; a field called `table` in one job and `tbl` in another breaks every saved query.

::: deeper
**Logs, metrics and traces.** Metrics are cheap, aggregated and good for alerting; logs are detailed, per-event and good for diagnosis; **traces** connect the steps of one request or run across services with timing. **OpenTelemetry** is the vendor-neutral standard for all three, and its logs data model includes trace and span IDs, so a log line can be linked to the trace of the run it belongs to. Honeycomb's argument that wide structured events with many fields can replace many separate metrics is worth reading: the more context each event carries, the more questions you can answer later without new code.

**Sampling and rate limiting.** At high volume, keep all ERROR entries, a sample of INFO entries and none of DEBUG, or rate-limit repeated messages ("this error happened 4,123 times in the last minute"). Exclusion filters in Cloud Logging drop low-value entries before storage charges apply.

**Spark and Beam specifics.** Executors and workers log on many machines; driver logs and worker logs end up in different places (YARN or Kubernetes for Spark, Dataflow worker logs in Cloud Logging). Python logging inside a UDF or DoFn runs on workers, so do not expect it in the driver output, and never log inside a per-row function at INFO level.

**Personal data and secrets in logs.** Logs are often retained longer and shared more widely than the data itself. Scrub known sensitive fields in the formatter, use the Sensitive Data Protection service or collector processors to redact patterns, and treat log stores as sensitive systems with their own access control.

> **Interview:** "A nightly job sometimes produces too few rows. How would logging help you find out why?" Structured logs at the start and end of each step with row counts in and out, keyed by a correlation ID, show exactly which step dropped rows on the bad nights; comparing those fields across good and bad runs in BigQuery points to the cause.
:::

::: check
?? Why is a JSON log line better than a sentence for operations?
Each field can be filtered, grouped and aggregated without parsing text, so questions such as "all loads of this table slower than 30 seconds" become simple queries.

?? What problem does a correlation ID solve?
It links all log entries of one business run across the many systems it passes through, so you can search one ID and see the whole story.

?? Why keep only a short period of logs in the fast tier?
Search-optimised storage is expensive and recent logs are the ones searched most. Routing older logs to Cloud Storage or BigQuery keeps them available for audits and analysis at a much lower cost.
:::

::: resources
- Docs: [Cloud Logging structured logging](https://cloud.google.com/logging/docs/structured-logging) · special JSON fields and how agents parse them
- Docs: [Python logging cookbook](https://docs.python.org/3/howto/logging-cookbook.html) · formatters, filters, context and handlers in the standard library
- Docs: [OpenTelemetry logs](https://opentelemetry.io/docs/concepts/signals/logs/) · the logs data model and its link to traces
- Docs: [Grafana Loki](https://grafana.com/docs/loki/latest/) · label-indexed log storage on object storage
- Docs: [OpenSearch documentation](https://docs.opensearch.org/latest/) · open-source search and log analytics, index state management
- Blog: [The ELK stack](https://www.elastic.co/what-is/elk-stack) · Elastic's overview of Elasticsearch, Logstash and Kibana
- Blog: [Structured events are the basis of observability](https://www.honeycomb.io/blog/structured-events-basis-observability) · why wide, structured events beat scattered metrics and text logs
:::

### Pipeline Incident Management

::: plain
An incident is a fire, and incident management is the fire drill everyone already practised. When the alarm goes off, one person takes charge, others are told exactly what to do, the people affected hear what is happening and when it will be fixed, and afterwards everyone sits down to work out why the fire started and how to stop the next one. Without the drill, five people run around, nobody tells the building, and the same fire starts again next month.
:::

A **data incident** is any event where data that people rely on is late, missing, wrong or exposed, badly enough that someone must act now. Most are not crashes: the pipeline "succeeded" and published wrong numbers. That makes data incidents harder to detect and harder to scope than application outages.

[[fig:dataops_incident]]

**1. Detection.** The best incidents are found by monitoring, not by a director asking why the dashboard shows zero sales. Automated detectors cover the main failure types: **freshness** (the table did not land by its deadline), **volume** (row count outside the expected range), **schema** (columns dropped, added or retyped upstream), **quality** (blocking tests failed) and **pipeline health** (retries exhausted, backlog growing). Measure **time to detection**: from when the data went bad to when someone knew.

**2. Triage and severity.** Severity is about **impact**, not about how broken the code is. Agree a scale in advance so nobody argues during the incident:

| Severity | Data impact | Example | Response |
|---|---|---|---|
| SEV1 | Wrong or missing data in external, regulatory or revenue-critical use; personal data exposed | Invoices generated from wrong prices; customer data in a public bucket | Page immediately, incident commander, leadership informed, status updates every 30 minutes |
| SEV2 | Key internal decisions or many users affected | Executive revenue dashboard stale for a day; ML model trained on duplicated data | Page during working hours or on-call, updates every 2 hours |
| SEV3 | Limited internal impact, workaround exists | One team's report late by a few hours | Ticket, fix within days |
| SEV4 | No user impact yet | A test dataset broken; a warning trend | Backlog |

A small classifier makes the rule explicit (and is easy to put in the alert routing code). It runs as is:

```python
def severity(external, pii_exposed, consumers, critical_asset, hours_late):
    if pii_exposed or (external and critical_asset):
        return "SEV1"
    if critical_asset and (hours_late >= 4 or consumers >= 50):
        return "SEV2"
    if consumers > 0:
        return "SEV3"
    return "SEV4"

print(severity(external=False, pii_exposed=False, consumers=120, critical_asset=True, hours_late=2))   # SEV2
print(severity(external=True,  pii_exposed=False, consumers=3,   critical_asset=True, hours_late=1))   # SEV1
print(severity(external=False, pii_exposed=False, consumers=0,   critical_asset=False, hours_late=9))  # SEV4
```

**Blast radius** comes from lineage: which dashboards, models and exports read the bad table, directly or downstream? Knowledge Catalog lineage, dbt's DAG or an observability tool answers this in minutes; without lineage, scoping is guesswork.

**3. Roles.** For SEV1 and SEV2, separate roles even if the team is small:

- **Incident commander (IC)**: coordinates, decides, keeps the timeline; does not debug.
- **Operations lead** (one or more): investigates and fixes.
- **Communications lead**: writes updates to consumers and stakeholders.

The IC can hand the role over explicitly ("I am handing IC to Priya") when shifts change. A dedicated channel per incident (`#inc-2026-11-22-orders`) keeps the discussion in one place.

**4. Mitigate first, then fix.** Stop the damage before finding the root cause. Data-specific mitigations: **pause** downstream publishing (a circuit breaker), **roll back** the consumer view to the last good version (blue-green from the delivery group), **mark** the affected dashboards as stale in the BI tool or catalog, **quarantine** the bad partition. Then fix and **backfill**: re-run the affected dates once the cause is removed, and verify with the same checks that should have caught it.

**5. Communication.** Consumers forgive delays more easily than silence or surprises. A useful update says what is affected, since when, what they should do, and when the next update comes:

```text
[SEV2] Orders data delayed, 22 Nov 08:10 UTC
Impact: fct_orders and the Revenue dashboard show data only up to 20 Nov.
Cause: a source export from the order system is late; no data is wrong.
Action for you: do not use the Revenue dashboard for 21 Nov figures yet.
Next update: 10:00 UTC or when data is available.
```

Escalation paths say who is informed or called next if the incident is not mitigated within a set time (on-call engineer, then the team lead, then the head of data, then the business owner of the data product).

**6. Resolution and review.** Close the incident when consumers have correct data again, then run a **blameless postmortem** within a few days for every SEV1 and SEV2. It records the impact, a timeline, the contributing factors, what went well, and **action items** with owners and dates.

**Root cause analysis.** Real incidents rarely have one cause. The "**5 whys**" technique asks why repeatedly ("revenue was wrong" because orders were duplicated, because the load ran twice, because a retry re-ran a non-idempotent insert, because the task used `INSERT` instead of `MERGE`, because no test ran the task twice), and usually reveals several contributing factors, each with its own fix: make the load idempotent, add a uniqueness test, add a duplicate-run test to CI.

> **Pitfall:** Fixing the data by hand and moving on. A manual `DELETE` of duplicate rows ends the symptom but leaves the cause, no record, and no test. Every SEV1 and SEV2 needs a written review with action items, and action items need owners and deadlines or they never happen.

> **Tip:** Publish a data status page or a pinned message per data product (in the catalog, the BI tool or a chat channel). Consumers then check status first instead of opening duplicate tickets.

::: deeper
**Metrics for incident practice.** Track **mean time to detect (MTTD)**, **mean time to mitigate** and **mean time to resolve (MTTR)**, plus the number of incidents found by consumers rather than by monitoring. The last number is the clearest sign of monitoring gaps. Group postmortem action items by theme (missing tests, missing alerts, manual steps) to decide where to invest.

**Silent data corruption.** The hardest incidents are discovered weeks later: a currency conversion applied twice since a deployment three weeks ago. Scoping needs lineage plus time: which partitions were written by the bad version (job labels with the commit SHA help), and which downstream tables and exports consumed them. Time travel (BigQuery's up to 7-day window, Iceberg snapshots) only helps if the incident is found within the retention period; long-lived raw data that you can reprocess is the real safety net.

**Incident tooling.** PagerDuty, Opsgenie, incident.io and FireHydrant automate the mechanics: create the channel, page roles, keep a timeline, prompt for status updates and generate a postmortem draft. Google's own process is described in the SRE book's managing incidents chapter and is based on the Incident Command System used by emergency services.

> **Interview:** "A stakeholder tells you the revenue dashboard has been wrong since Tuesday. Walk me through your response." Acknowledge and open an incident with a severity based on impact; assign an IC; mark the dashboard as unreliable; use lineage to find the source table and every downstream consumer; find the first bad partition and the change that caused it; mitigate by rolling back or pausing publication; fix, backfill and verify with checks; communicate at fixed intervals; then run a blameless postmortem and add the missing test or alert.
:::

::: check
?? Should severity depend on how hard the bug is to fix?
No. Severity depends on impact on consumers (who uses the data, for what, how badly they are affected), so a trivial bug that corrupts invoices is SEV1 and a complex failure in a test dataset is SEV4.

?? Why does the incident commander not debug?
Someone must keep the overview: coordinate people, decide on mitigations, make sure updates go out and keep the timeline. Debugging absorbs all attention, so those jobs would be dropped.

?? Give two ways to mitigate a data incident before the root cause is known.
Roll the consumer view back to the last good table version, pause downstream publishing with a circuit breaker, quarantine the bad partition, or mark affected dashboards as stale.
:::

::: resources
- Book: [Managing incidents](https://sre.google/sre-book/managing-incidents/) · SRE book chapter on roles, command and handover
- Book: [Incident response](https://sre.google/workbook/incident-response/) · SRE Workbook chapter with case studies and practice
- Book: [Postmortem culture](https://sre.google/sre-book/postmortem-culture/) · blameless postmortems and how to make them stick
- Docs: [PagerDuty incident response guide](https://response.pagerduty.com/) · an open, detailed incident process with role guides
- Article: [Atlassian incident management handbook](https://www.atlassian.com/incident-management/handbook) · severity levels, communication templates and postmortems
- Blog: [Data incident management](https://www.montecarlodata.com/blog-data-incident-management-process/) · Monte Carlo on applying incident practice to data
:::

### Chaos Engineering for Data Systems

::: plain
Chaos engineering is a fire drill with a real, small fire. Instead of hoping the sprinklers work, you light a candle under one sprinkler on a quiet afternoon with the fire brigade standing by, and watch what happens. If the sprinkler fails, you found out safely. For data systems the "candle" is a killed worker, a slow network link or a duplicated message, and the question is: does the pipeline still deliver correct data on time?
:::

Distributed data systems are full of recovery mechanisms: Spark retries tasks, Kafka replicates partitions, Dataflow replaces workers, Airflow retries tasks, Pub/Sub redelivers unacknowledged messages. Each one is code that runs only when something fails, which is exactly the code that is rarely tested. **Chaos engineering** is the discipline of running controlled experiments that inject failures to build confidence that the system withstands them.

**The experiment loop** (from the Principles of Chaos Engineering):

1. Define the **steady state** as a measurable output, not an internal metric: "events reach BigQuery within 5 minutes, and the count per hour matches the Pub/Sub publish count within 0.1%".
2. Form a **hypothesis**: "if one of three Kafka brokers is killed, the steady state holds".
3. **Inject** a realistic fault, with the smallest **blast radius** that can teach you something: one broker, one worker, in staging first.
4. **Observe** the steady-state metrics against a control.
5. **Stop** automatically if the steady state breaks badly (an abort condition), then fix what you learned.

**Faults that matter for data pipelines:**

| Fault | How to inject | What it tests |
|---|---|---|
| Worker or pod killed | Chaos Mesh `PodChaos`, deleting a pod, stopping a VM | Task retry, checkpoint recovery, operator restarts |
| Network delay or loss | Chaos Mesh `NetworkChaos`, Toxiproxy | Timeouts, retries with backoff, consumer rebalances |
| Network partition | Chaos Mesh partition between pods | Leader election, split-brain handling, Kafka ISR shrink |
| Disk full or slow | `IOChaos`, filling a volume | Spill behaviour, state store writes, log cleanup |
| Clock skew | `TimeChaos` | Watermarks, TTLs, certificate checks |
| Duplicate or reordered messages | Republishing, a replay from a Pub/Sub snapshot | Idempotent sinks, deduplication, event-time logic |
| Dependency outage | Revoking a service account's access, blocking an API | Error handling, alerting, circuit breakers |

**Chaos Mesh** and **LitmusChaos** are CNCF projects that run on Kubernetes (including GKE) and inject faults declared as custom resources. A Chaos Mesh experiment that kills one Flink task manager pod:

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: kill-one-taskmanager
  namespace: flink
spec:
  action: pod-kill
  mode: one                    # pick one matching pod at random
  selector:
    namespaces: [flink]
    labelSelectors:
      component: taskmanager
```

**Toxiproxy** (from Shopify) is a TCP proxy you put between a pipeline and a dependency in tests, adding latency, bandwidth limits or dropped connections on command. It fits integration tests in CI: "the loader must succeed when the database adds 2 seconds of latency".

**Testing exactly-once under failure.** Pipelines claim "exactly-once", but end to end that usually means **at-least-once delivery plus idempotent writes**: messages can be delivered more than once after a failure, and the sink must make repeats harmless. You can test the property with a simulation before trusting it in production. This runnable example delivers messages with random crashes before acknowledgement (so they are redelivered) and compares a naive append sink with an idempotent upsert keyed by event ID:

```python
import random

def run(sink_kind, seed=7, n=1000, crash_rate=0.05):
    rng = random.Random(seed)
    queue = list(range(n))                   # event ids waiting for delivery
    appended, upserted = [], {}
    while queue:
        event = queue[0]
        if sink_kind == "append":
            appended.append(event)           # write happens...
        else:
            upserted[event] = event          # MERGE on the event id
        if rng.random() < crash_rate:
            continue                         # ...crash before ack: redelivered
        queue.pop(0)                         # ack
    return len(appended) if sink_kind == "append" else len(upserted)

print("append rows:", run("append"))     # append rows: 1060
print("upsert rows:", run("upsert"))     # upsert rows: 1000
```

The append sink double-counts every redelivered message; the upsert sink ends with exactly 1,000 rows. In BigQuery the idempotent version is a `MERGE` on the event ID or the Storage Write API with offsets; in Spark Structured Streaming, idempotent or transactional sinks with checkpoints; in Kafka, idempotent producers and transactions.

**Game days.** A scheduled session where the team runs a planned failure in staging (or carefully in production), with everyone watching dashboards and following runbooks. Game days test the people and the runbooks as much as the software: can the on-call engineer find the right dashboard, does the alert fire, is the runbook still correct?

> **Pitfall:** Starting chaos experiments in production without monitoring or abort conditions. Chaos engineering assumes you can see the steady state clearly and stop the experiment quickly. If you cannot measure it, you are not experimenting, you are causing an outage.

> **Tip:** Begin with the failures you already had. Every postmortem is a ready-made hypothesis: "if the source API returns 503 for 20 minutes again, does the new retry policy hold?". Turn it into a repeatable experiment so the fix is verified and stays verified.

::: deeper
**Netflix origins.** Netflix built Chaos Monkey (randomly terminating production instances during working hours) when it moved to the cloud, then the wider Simian Army (latency, zone and region failures). The lesson that carried over: if failures are frequent and expected, engineers design for them; if they are rare, recovery code rots.

**Data-specific steady states.** Throughput and error rates are not enough for data; include correctness: counts reconcile between source and sink, no duplicate keys, watermarks advance, late data stays within bounds. A pipeline that survives a broker failure by silently dropping 0.3% of events has not passed.

**Testing distributed guarantees.** Jepsen's analyses of databases and queues inject partitions and clock faults and check histories for consistency violations; they have found real bugs in many systems that claimed strong guarantees. For your own pipeline, the cheaper version is property checks (unique keys, conservation of counts) run after every chaos experiment.

**Managed services.** You cannot inject faults into BigQuery or Pub/Sub internals, but you can simulate their failure modes at your boundary: revoke a role, exhaust a quota in a test project, replay messages with Pub/Sub seek, cancel a Dataflow job and restart it from a snapshot. Google publishes its services' SLAs; design your error handling for the gap between them and 100%.

> **Interview:** "How would you verify that a streaming pipeline really is exactly-once?" State the steady state (source and sink counts match, no duplicate event IDs); inject failures that cause redelivery (kill workers before checkpoints, restart consumers, replay from a snapshot); then check counts and key uniqueness in the sink. The pipeline passes only if the sink is idempotent or transactional; at-least-once delivery alone will duplicate.
:::

::: check
?? Why is the steady state defined on outputs rather than internal metrics?
Outputs (data delivered on time and correct) are what consumers care about. Internal metrics can look healthy while results are wrong, or look alarming while the output is fine.

?? In the simulation, why does the upsert sink end with exactly 1,000 rows despite redeliveries?
Each write is keyed by the event ID, so a redelivered event overwrites its own row instead of adding a new one. The write is idempotent.

?? What is a game day?
A scheduled exercise where the team deliberately triggers a planned failure and practises detection, runbooks and recovery while watching the system.
:::

::: resources
- Docs: [Principles of Chaos Engineering](https://principlesofchaos.org/) · the short definition of steady state, hypotheses and blast radius
- Book: [Chaos Engineering](https://www.oreilly.com/library/view/chaos-engineering/9781491988459/) · Basiri, Rosenthal and others from Netflix on running chaos programmes
- Docs: [Chaos Mesh documentation](https://chaos-mesh.org/docs/) · fault types and experiments on Kubernetes
- Docs: [LitmusChaos documentation](https://docs.litmuschaos.io/) · another CNCF chaos platform with a library of experiments
- Repo: [Toxiproxy](https://github.com/Shopify/toxiproxy) · simulate network conditions between services in tests
- Blog: [The Netflix Simian Army](https://netflixtechblog.com/the-netflix-simian-army-16e57fbab116) · where Chaos Monkey and its successors came from
- Article: [Jepsen analyses](https://jepsen.io/analyses) · fault-injection studies of real databases and queues
:::

### Data Observability — Monte Carlo, Acceldata, Datafold

::: plain
Tests are a checklist you wrote yourself; observability is a security camera on every table. The camera does not know what a burglar looks like in advance, but it notices when something unusual happens and records enough to work out what and where. For a data platform, that means watching every table's freshness, size, shape and values, and knowing how tables are connected, so an alert can say not just "this looks wrong" but "this is why, and these are the dashboards affected".
:::

The next section, Data governance, security and compliance, covers the five pillars (freshness, volume, schema, distribution, lineage) and how anomaly detection learns normal behaviour. This topic is about **operating** observability: which health metrics to collect, how lineage turns many alerts into one incident, how observability tools compare with the tests from the Testing group, and how to roll them out.

**Table health metrics.** Every observability tool, bought or built, keeps a time series per table. On BigQuery you can collect the core ones from metadata alone with a scheduled query that appends a snapshot each hour:

```sql
INSERT INTO ops.table_health (snapshot_time, dataset, table_name, total_rows, logical_bytes, last_modified)
SELECT CURRENT_TIMESTAMP(), table_schema, table_name, total_rows,
       total_logical_bytes, storage_last_modified_time
FROM `region-eu`.INFORMATION_SCHEMA.TABLE_STORAGE
WHERE table_schema IN ('staging', 'marts');
```

Freshness is `CURRENT_TIMESTAMP()` minus `last_modified`, volume is the change in `total_rows` between snapshots, and schema changes come from comparing `INFORMATION_SCHEMA.COLUMNS` snapshots. Distribution metrics (null share, distinct counts, means) need queries over the data, ideally only the newest partition.

**Lineage-aware alerting.** When a staging table breaks, every table downstream of it also looks wrong: ten freshness alerts and five volume alerts for one root cause. Observability platforms use lineage to **group** them into one incident, point at the **most upstream** anomalous table as the likely cause, and list the affected downstream assets (the **blast radius**) so you know whom to tell. The logic is a graph walk. This runs as is:

```python
from collections import deque

# edges: upstream -> downstream (from dbt manifest, OpenLineage or Knowledge Catalog lineage)
lineage = {
    "raw.orders": ["stg.orders"],
    "stg.orders": ["fct_orders", "fct_refunds"],
    "fct_orders": ["dash.revenue", "ml.churn_features"],
    "fct_refunds": ["dash.finance"],
    "raw.customers": ["dim_customers"],
    "dim_customers": ["dash.revenue"],
}
alerts = {"stg.orders", "fct_orders", "fct_refunds", "dash.revenue"}

def downstream(node):
    seen, todo = set(), deque([node])
    while todo:
        for nxt in lineage.get(todo.popleft(), []):
            if nxt not in seen:
                seen.add(nxt); todo.append(nxt)
    return seen

roots = sorted(a for a in alerts if not (set(p for p, c in lineage.items() if a in c) & alerts))
for r in roots:
    print("root cause candidate:", r)
    print("grouped alerts:", sorted(downstream(r) & alerts))
    print("blast radius:", sorted(downstream(r)))
# root cause candidate: stg.orders
# grouped alerts: ['dash.revenue', 'fct_orders', 'fct_refunds']
# blast radius: ['dash.finance', 'dash.revenue', 'fct_orders', 'fct_refunds', 'ml.churn_features']
```

Four alerts become one incident, with one place to look and a list of five affected assets to notify.

**The tools and what they emphasise.**

| Tool | Focus | Typical use |
|---|---|---|
| Monte Carlo | Automatic monitors on every table (freshness, volume, schema), custom monitors, lineage-based incident grouping and triage | Broad coverage of a large warehouse with little setup |
| Acceldata | Data plus pipeline and compute observability (Spark, Hadoop, warehouse cost and performance) | Platforms where cost and job performance matter as much as data quality |
| Datafold | Change management: data diffs between production and a pull request's version, column-level lineage in CI | Preventing bad changes before merge rather than detecting them after |
| Elementary | Open source, dbt-native anomaly tests and reports | Teams on dbt that want observability without a new platform |
| Knowledge Catalog | Data profiling, data quality scans and lineage on Google Cloud | Managed, metadata-integrated monitoring for BigQuery |

**dbt tests versus Monte Carlo.** They answer different questions, and mature teams run both:

| | dbt tests | Observability platform |
|---|---|---|
| Who defines the check | An engineer, per model | Mostly automatic, learned from history |
| What it catches | Known rules: unique keys, accepted values, relationships | Unknown problems: drops in volume, null spikes, late tables, upstream schema changes |
| When it runs | During `dbt build`, can block publishing | Continuously, after data lands, mostly alerts |
| Coverage | Models in the dbt project | Every table, including sources and tables outside dbt |
| Precision | Exact, rarely noisy | Probabilistic, needs tuning and feedback |

**Rolling it out.** Start with metadata monitors (freshness, volume, schema) on all tables, because they are cheap. Add distribution monitors only on the tables that feed critical dashboards and models, chosen with lineage. Route each alert to the owning team (owners from the catalog), connect it to incident management, and review noisy monitors monthly.

> **Pitfall:** Treating observability as a replacement for tests. Anomaly detection notices that today's revenue is unusual; it cannot know that a refund must never exceed its order. Keep blocking tests for business rules and use observability for the problems nobody anticipated.

> **Tip:** Build versus buy is mostly a question of coverage and people. A scheduled metadata query plus a few Cloud Monitoring alerts covers freshness and volume for a few hundred tables at almost no cost. Buying makes sense when you have thousands of tables, many sources outside your control, and no one to maintain the detectors.

::: deeper
**Metadata cost and access.** Observability vendors connect to your warehouse with a service account that reads `INFORMATION_SCHEMA`, job history and query logs, and optionally runs sampled queries. Review exactly which roles you grant: metadata-only roles (`roles/bigquery.metadataViewer` plus job listing) for most monitors, data read access only on the datasets where distribution monitors run. Data residency matters too: check where the vendor processes metadata and sample values.

**Column-level lineage for root cause.** Table-level lineage says `fct_orders` depends on `stg.orders`; column-level lineage says `fct_orders.amount` depends on `stg.orders.price` and `stg.orders.qty`. When only one column's distribution shifts, column lineage narrows the search to the transformations that touch it. dbt (from compiled SQL), Datafold, Monte Carlo and Knowledge Catalog lineage provide column-level lineage to different degrees.

**Pipeline observability.** Data metrics say *what* is wrong; pipeline metadata says *which run* caused it. Correlating anomalies with orchestrator runs (which DAG run, which commit) is where OpenLineage helps: run events connect jobs, datasets and timings in one graph.

> **Interview:** "Why do we need Monte Carlo if we already have 2,000 dbt tests?" Tests only catch what someone wrote a rule for, and only on dbt models. Observability adds automatic freshness, volume and schema monitoring on every table including raw sources, learns distributions to catch unknown problems, and uses lineage to group alerts and show impact. Keep tests for business rules that must block publishing.
:::

::: check
?? Why do freshness and volume monitors cost almost nothing on BigQuery?
They read metadata views such as `INFORMATION_SCHEMA.TABLE_STORAGE` and partition metadata, which report row counts and modification times without scanning table data.

?? How does lineage reduce alert noise?
It groups alerts on tables downstream of one anomalous upstream table into a single incident and points to that most upstream table as the likely root cause.

?? Name one thing dbt tests catch that anomaly detection would miss, and one the other way round.
A business rule such as "a refund never exceeds its order amount" (tests). A sudden 40% drop in row volume or a raw source table arriving late that no test covers (observability).
:::

::: resources
- Docs: [Monte Carlo documentation](https://docs.getmontecarlo.com/) · monitors, incidents, lineage and integrations
- Docs: [Acceldata documentation](https://docs.acceldata.io/) · data, pipeline and compute observability
- Blog: [What is data observability?](https://www.datafold.com/blog/what-is-data-observability) · Datafold's view of observability and change management
- Docs: [OpenLineage](https://openlineage.io/docs/) · the open standard for lineage run events used by many observability tools
- Docs: [BigQuery TABLE_STORAGE view](https://cloud.google.com/bigquery/docs/information-schema-table-storage) · row counts, bytes and modification times per table
- Book: [Data Quality Fundamentals](https://www.oreilly.com/library/view/data-quality-fundamentals/9781098112035/) · Moses, Gavish and Vorwerck on monitoring, lineage and incident response for data
:::

### Secrets Management for Data

::: plain
A secret is a key to someone else's house: a database password, an API token, a private key. Secrets management is the key cabinet with a guard. Keys are never copied onto sticky notes (code, config files, chat), each person or program can take only the keys it needs, the guard records every time a key is taken, and the locks are changed regularly so an old copied key stops working. The best keys expire by themselves after a few minutes.
:::

Data pipelines hold more secrets than most applications: credentials for every source database, SaaS API tokens, warehouse service accounts, SFTP keys from partners, webhook signing keys. Google Cloud foundations for data introduced Secret Manager (secrets, versions, IAM per secret, Composer's secrets backend). This topic covers the rest: avoiding secrets entirely where possible, dynamic secrets, secrets on Kubernetes, rotation and leak detection.

**The hierarchy of options**, best first:

1. **No secret at all.** Google Cloud services authenticate with service account identity: Workload Identity Federation for CI (the CI/CD topic), Workload Identity Federation for GKE for pods, attached service accounts for Cloud Run, Dataflow and Managed Service for Apache Spark. Cloud SQL and AlloyDB support **IAM database authentication**, so a pipeline logs in to PostgreSQL with its service account and a short-lived token instead of a password.
2. **Dynamic secrets.** Credentials created on demand for one consumer with a short lifetime, then revoked automatically.
3. **Static secrets in a secret manager**, rotated on a schedule, accessed by identity, audited.
4. Never: secrets in Git, Docker images, Airflow variables in plain text, notebook cells or logs.

**HashiCorp Vault.** Vault is a secrets platform that adds features beyond storage. Its **database secrets engine** connects to PostgreSQL, MySQL, Snowflake, MongoDB and others with an admin credential and creates a new database user for each request, with a **lease** (time to live); when the lease expires, Vault drops the user:

```bash
vault secrets enable database
vault write database/config/orders-pg \
  plugin_name=postgresql-database-plugin \
  connection_url="postgresql://{{username}}:{{password}}@10.20.0.5:5432/orders" \
  allowed_roles=etl-readonly username=vault-admin password="$ADMIN_PW"
vault write database/roles/etl-readonly db_name=orders-pg \
  creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"{{name}}\";" \
  default_ttl=1h max_ttl=4h

vault read database/creds/etl-readonly      # returns a fresh username, password and lease_id
```

A nightly extract asks for credentials, uses them for an hour and never stores them; a leaked credential is useless soon after. Vault's **PKI secrets engine** similarly issues short-lived TLS certificates, for example for Kafka clients using mutual TLS. Pipelines authenticate to Vault with their platform identity (the Kubernetes or GCP auth methods), so no bootstrap secret is needed. Since 2023 Vault is under the Business Source License; **OpenBao** is an open-source fork under the Linux Foundation with a compatible API.

**Secrets on Kubernetes.** A Kubernetes `Secret` is only base64-encoded, not encrypted, in its manifest, and by default stored unencrypted in etcd unless encryption at rest is configured (GKE encrypts etcd at rest, and **application-layer secrets encryption** adds envelope encryption with a Cloud KMS key you control). Three common patterns keep secret values out of Git:

| Pattern | How it works | Trade-off |
|---|---|---|
| External Secrets Operator (ESO) | An `ExternalSecret` names a secret in Secret Manager or Vault; the operator creates and refreshes a Kubernetes Secret | Git holds only references; values live in one central store |
| Sealed Secrets | `kubeseal` encrypts a Secret with the cluster's public key; the encrypted `SealedSecret` is safe to commit; a controller decrypts it in the cluster | Fully GitOps; rotation and re-sealing are manual; values tied to one cluster key |
| CSI driver (Secret Manager add-on for GKE) | Secrets are mounted as files into the pod at start, read directly from Secret Manager | No Kubernetes Secret object at all; needs workload identity |

An ESO definition that pulls a Secret Manager secret into the `airflow` namespace, refreshed every hour:

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: salesforce-token
  namespace: airflow
spec:
  refreshInterval: 1h
  secretStoreRef: {kind: ClusterSecretStore, name: gcp-secret-manager}
  target: {name: salesforce-token}
  data:
    - secretKey: token
      remoteRef: {key: salesforce-api-token}
```

**Rotation automation.** Rotating a database password safely needs a sequence that never leaves running jobs without valid credentials. The **dual-credential** pattern keeps two users and alternates between them: create a new password for the inactive user, store it as a new secret version, switch consumers to it (they read `latest` at start), wait until no session uses the old one, then rotate the old one next time. Secret Manager **rotation schedules** publish a message to a Pub/Sub topic when a secret is due; a Cloud Run function subscribed to it performs the steps. Disable old versions before destroying them, so a forgotten consumer fails loudly and can be fixed, rather than silently.

**Leak detection.** Assume secrets will be committed by accident. Run a scanner in pre-commit hooks and CI (gitleaks, trufflehog, GitHub secret scanning with push protection). A toy version of what scanners do, runnable with the standard library:

```python
import re

PATTERNS = {
    "google_api_key": re.compile(r"AIza[0-9A-Za-z_\-]{35}"),
    "private_key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "password_assignment": re.compile(r"(?i)(password|passwd|pwd)\s*[:=]\s*['\"][^'\"]{6,}['\"]"),
}

def scan(text):
    return sorted({name for name, rx in PATTERNS.items() if rx.search(text)})

config = 'db_host = "10.0.0.5"\ndb_password = "Winter2026!"\n'
print(scan(config))                     # ['password_assignment']
print(scan('key = "AIza' + "x" * 35 + '"'))  # ['google_api_key']
```

If a secret leaks, **rotate it first**, then clean history; removing the commit does not help, because clones and caches already have it.

> **Pitfall:** Rotating a secret by overwriting it in place while long-running jobs hold the old value. A Dataflow or Spark streaming job reads its credentials at start-up; changing the password underneath it breaks the job hours later on its next reconnect. Use dual credentials or restart consumers as part of the rotation.

> **Tip:** Give every pipeline its own service account and its own secrets. Shared credentials make audit logs useless ("etl-user did it") and turn one leak into a platform-wide rotation.

::: deeper
**Secret zero.** Every secret store needs a first credential to access it, and that bootstrap secret is the weakest link. Platform identity removes it: the pod's Kubernetes service account is federated to a Google service account, which IAM authorises on Secret Manager, so there is nothing to hand out. Avoid service account key files entirely; organization policy `iam.disableServiceAccountKeyCreation` enforces this.

**Caching and availability.** A secret store becomes a dependency of every pipeline start. Cache secrets in memory for the life of a process, retry on transient errors, and do not fetch a secret per record. Secret Manager has per-project access quotas that a tight loop can exhaust.

**Auditing access.** Secret Manager access is recorded in Cloud Audit Logs as Data Access logs (enable them for Secret Manager); Vault has its own audit devices. Alert on access from unexpected principals and on reads of high-value secrets outside job windows.

**Partner credentials.** SFTP keys and API tokens issued by external partners often cannot be rotated automatically. Track their expiry dates as metadata (labels or annotations on the secret) and alert weeks in advance; an expired partner certificate is a classic Monday-morning incident.

> **Interview:** "How should an Airflow DAG on Managed Airflow get the password for a partner's PostgreSQL database?" Store it in Secret Manager, enable Composer's Secret Manager backend so the Airflow connection is read from there, grant the environment's service account accessor rights on only that secret, rotate on a schedule with dual credentials, and never put it in DAG code, variables or logs. If the database were Cloud SQL, use IAM database authentication and no password at all.
:::

::: check
?? What makes a dynamic secret safer than a static password?
It is created for one consumer on demand and expires automatically after a short lease, so a leaked copy soon stops working and each use is attributable.

?? Is a Kubernetes Secret manifest safe to commit to Git?
No. Its values are only base64-encoded. Commit a reference (an External Secrets Operator object) or an encrypted SealedSecret instead.

?? A database password was committed to a public repository. What is the first thing to do?
Rotate the password immediately (and check access logs for misuse). Removing it from Git history comes after, because copies already exist.
:::

::: resources
- Docs: [HashiCorp Vault documentation](https://developer.hashicorp.com/vault/docs) · secrets engines, auth methods, leases and audit
- Docs: [Vault database secrets engine](https://developer.hashicorp.com/vault/docs/secrets/databases) · dynamic database credentials with leases
- Docs: [Secret Manager rotation](https://cloud.google.com/secret-manager/docs/secret-rotation) · rotation schedules and Pub/Sub notifications
- Docs: [External Secrets Operator](https://external-secrets.io/latest/) · syncing Secret Manager and Vault secrets into Kubernetes
- Repo: [Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets) · encrypted secrets that are safe to store in Git
- Docs: [Kubernetes Secrets good practices](https://kubernetes.io/docs/concepts/security/secrets-good-practices/) · encryption at rest, least privilege and handling
- Repo: [gitleaks](https://github.com/gitleaks/gitleaks) · secret scanning for Git repositories and CI
:::

## Teams and money

Everything so far has been about software and systems. The last group is about the two constraints that decide what a data platform can actually become: money and people. A platform that is reliable but whose cost nobody can explain will be cut in the next budget round. A platform that is technically excellent but organised so that every request queues behind one central team will be bypassed by shadow pipelines in spreadsheets and notebooks.

The group starts with FinOps (making cloud cost visible, attributable and optimised as an ongoing practice), then team topologies (how to organise data teams so that ownership and flow are clear), and ends with maturity models, which pull the whole section together into a way of assessing where your platform stands and what to invest in next. Google Cloud foundations for data covered the technical levers for reducing cost; BigQuery in depth covered BigQuery pricing and slots. This group is about the practice and the organisation around them.

### FinOps for Data Engineering

::: plain
FinOps is running the cloud bill the way a careful household runs its budget. Everyone can see where the money goes (each expense has a label), each family member pays for what they use, someone notices quickly when this month's electricity bill is twice last month's, and decisions such as "buy an annual pass or pay per visit?" are made with numbers. The aim is not to spend as little as possible, but to get the most value for what you spend.
:::

The **FinOps Foundation** (part of the Linux Foundation) defines FinOps as an operating model in which engineering, finance and business share responsibility for cloud spending. Its framework describes three repeating phases:

- **Inform**: make costs visible and attribute them to teams, products and workloads.
- **Optimize**: reduce waste and improve rates (rightsizing, scheduling, commitments, better queries).
- **Operate**: build the habits and governance that keep it going (budgets, reviews, policies, cost in design decisions).

Data platforms are a special case because most of their cost is shared and usage-driven: one BigQuery reservation serves twenty teams, one Spark cluster runs jobs for five products, and the cost of a dashboard depends on how often people open it.

**1. Tagging strategy.** Attribution starts with labels. On Google Cloud, **labels** are key-value pairs on projects and resources (lowercase keys and values, up to 64 per resource) that appear in the Cloud Billing export. A minimal, enforced label set:

| Label | Example | Purpose |
|---|---|---|
| `team` | `orders` | Who pays and who to ask |
| `product` | `revenue-mart` | Which data product the cost serves |
| `env` | `prod`, `dev` | Separate production from experiments |
| `cost-center` | `cc-4410` | Mapping to finance's structure |
| `pipeline` | `orders-daily` | Drill-down for engineers |

Enforce labels in Terraform modules (required variables) and in CI policy checks, so unlabelled resources cannot be created. For BigQuery queries, attach **job labels**: dbt's BigQuery adapter can turn query comments into job labels, and in SQL scripts `SET @@query_label = "team:orders,pipeline:orders-daily";` labels the following jobs in the session. Projects per team or product are the strongest form of attribution, since every cost in a project belongs to it.

**2. The billing export is your source of truth.** Export Cloud Billing data to BigQuery (standard and detailed usage cost exports) and query it:

```sql
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key = 'team') AS team,
  service.description AS service,
  ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)), 2) AS net_cost
FROM `acme-billing.billing.gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX`
WHERE invoice.month = '202611'
GROUP BY team, service
ORDER BY net_cost DESC;
```

Credits (committed use discounts, promotions) are negative amounts, so net cost is cost plus credits.

**3. BigQuery slot monitoring.** With capacity pricing (BigQuery editions), you pay for slots whether queries use them or not; with on-demand pricing you pay per TiB scanned. Either way, `INFORMATION_SCHEMA.JOBS` tells you who used what. Average slots used over a period is `total_slot_ms / milliseconds in the period`:

```sql
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key = 'team') AS team,
  ROUND(SUM(total_slot_ms) / (1000 * 60 * 60 * 24 * 30), 1) AS avg_slots_30d,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4), 2) AS tib_billed
FROM `region-eu`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
GROUP BY team
ORDER BY avg_slots_30d DESC;
```

Compare used slots with the reservation's baseline and autoscaling to see idle capacity (paid but unused) and peaks (where queries wait).

**4. Other platforms.** The same thinking applies to vendor platforms running on Google Cloud:

- **Snowflake** bills **credits**: a virtual warehouse consumes credits per second while running (with a 60-second minimum each time it resumes), doubling with each size step (X-Small 1 credit per hour, Small 2, Medium 4). `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` gives credits per warehouse per hour; `QUERY_TAG` attributes queries. The main levers are aggressive auto-suspend, right-sized warehouses per workload, and resource monitors that cap credit use.
- **Databricks** bills **DBUs** (Databricks units) per hour of compute, at rates that depend on the compute type: jobs compute is cheaper than all-purpose (interactive) compute for the same work, and serverless and Photon change the rate and the speed. The `system.billing.usage` table, joined with `system.billing.list_prices`, gives DBUs and list cost per workspace, job and custom tag. Levers: run production on jobs compute, auto-terminate idle clusters, use autoscaling and preemptible workers, and enforce cluster policies.

**5. Chargeback models.** Once costs are attributed, decide what to do with them:

| Model | What happens | Fits |
|---|---|---|
| Showback | Teams see their costs; the central budget pays | Early stages; building awareness |
| Chargeback | Costs are billed to team budgets | Mature attribution, teams with their own budgets |
| Hybrid | Direct costs charged back; shared platform costs absorbed or split | Most data platforms |

Shared costs (a reservation, the Composer environment, the platform team's tooling) are split by a **driver**: proportional to slot-ms, bytes stored, job count, or evenly. Proportional allocation is fair and easy to compute. This runs as is:

```python
reservation_cost = 12000.00                        # monthly cost of a shared reservation (illustrative)
slot_ms = {"orders": 5.1e9, "marketing": 2.4e9, "finance": 1.5e9, "ml": 3.0e9}
idle_share = 0.20                                  # 20% of the reservation sat idle

used_cost = reservation_cost * (1 - idle_share)
total = sum(slot_ms.values())
for team, ms in sorted(slot_ms.items(), key=lambda kv: -kv[1]):
    print(f"{team:10s} {ms / total:6.1%} {used_cost * ms / total:9.2f}")
print(f"{'idle':10s} {'':6s} {reservation_cost * idle_share:9.2f}  (platform budget)")
# orders      42.5%   4080.00
# ml          25.0%   2400.00
# marketing   20.0%   1920.00
# finance     12.5%   1200.00
# idle                2400.00  (platform budget)
```

Keeping idle capacity in the platform budget, rather than spreading it over teams, gives the platform team the incentive to size the reservation well.

**6. Cost anomaly detection.** Set **budgets** with alert thresholds on the billing account and on each project (Cloud Billing budgets can notify email recipients or publish to Pub/Sub, so a function can react). Budgets catch slow overruns; for sudden spikes, compare each day's cost per service and project with its recent history:

```sql
WITH daily AS (
  SELECT project.id AS project, service.description AS service,
         DATE(usage_start_time) AS day, SUM(cost) AS cost
  FROM `acme-billing.billing.gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX`
  WHERE usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 DAY)
  GROUP BY project, service, day
)
SELECT project, service, day, cost,
       AVG(cost) OVER w AS avg_prev_7d
FROM daily
WINDOW w AS (PARTITION BY project, service ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
QUALIFY day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND cost > 2 * avg_prev_7d AND cost > 100;
```

The second condition (`cost > 100`) stops tiny projects from alerting on noise. Note that billing data arrives with a delay of hours, so this is a daily check, not a real-time one; for real-time protection use quotas (BigQuery custom query quotas per user or project, maximum bytes billed per query).

> **Pitfall:** Attribution added after the fact. Labelling hundreds of existing resources and queries retroactively is painful and never complete. Make labels required in Terraform modules and job submission from day one.

> **Tip:** Report **unit costs** alongside totals: cost per TiB processed, per pipeline run, per dashboard view, per active customer. A rising total with a falling unit cost is growth; a rising unit cost is a problem to investigate.

::: deeper
**Rate versus usage optimisation.** FinOps separates two levers. **Usage** optimisation does less work (partition pruning, fewer full refreshes, deleting unused tables, turning off idle clusters). **Rate** optimisation pays less for the same work (committed use discounts, BigQuery edition commitments, Spot VMs, Snowflake capacity contracts). Usage first: committing to capacity you would not need after optimisation locks in waste.

**FOCUS.** The FinOps Open Cost and Usage Specification is a common schema for billing data across clouds and SaaS vendors (columns such as `BilledCost`, `EffectiveCost`, `ServiceName`, `ChargeCategory`). If you combine Google Cloud costs with Snowflake or Databricks bills, normalising to FOCUS lets one set of queries and dashboards cover all of them.

**Cost in design reviews.** The cheapest optimisation happens before anything is built: a design review that asks "how much will this cost per month at expected and at 10x volume?" catches choices such as a streaming pipeline where hourly batch would do, or a full daily rebuild of a 50 TB table where an incremental merge would do.

**Airbnb's Spark cost work.** Airbnb's engineering blog describes cutting Spark infrastructure cost substantially through autoscaling, instance choice and job tuning, a useful case study of combining usage and rate optimisation on a large Spark estate.

> **Interview:** "How would you implement chargeback for a shared BigQuery reservation?" Require job labels (team, product) through dbt and orchestration defaults; sum `total_slot_ms` per label from `INFORMATION_SCHEMA.JOBS`; allocate the reservation's used cost proportionally; keep idle capacity in the platform budget; publish a monthly report with unit costs; and start as showback before moving to chargeback.
:::

::: check
?? Why are credits added to cost in the billing export query?
Credits such as committed use discounts and promotions are stored as negative amounts, so net cost is the sum of cost and credits.

?? A team's monthly BigQuery jobs used 1.296e12 slot-ms in a 30-day month. What average slot usage is that?
1.296e12 / (1000 × 3600 × 24 × 30) = 1.296e12 / 2.592e9 = 500 slots on average.

?? Why optimise usage before buying commitments?
Commitments lock in a level of spend for one to three years. If you commit first and then remove waste, you keep paying for capacity you no longer need.
:::

::: resources
- Docs: [FinOps Framework](https://www.finops.org/framework/) · phases, principles, capabilities and maturity from the FinOps Foundation
- Docs: [Export Cloud Billing data to BigQuery](https://cloud.google.com/billing/docs/how-to/export-data-bigquery) · setting up and querying standard and detailed exports
- Docs: [BigQuery JOBS view](https://cloud.google.com/bigquery/docs/information-schema-jobs) · slot-ms, bytes billed and labels per job
- Docs: [Exploring compute cost in Snowflake](https://docs.snowflake.com/en/user-guide/cost-exploring-compute) · credits, metering views and attribution
- Docs: [Databricks billable usage system table](https://docs.databricks.com/gcp/en/admin/system-tables/billing) · DBU usage and cost on Databricks on Google Cloud
- Book: [Cloud FinOps, 2nd Edition](https://www.oreilly.com/library/view/cloud-finops-2nd/9781492098348/) · Storment and Fuller; the practice, culture and metrics of FinOps
- Spec: [FOCUS](https://focus.finops.org/) · the open specification for normalised cloud cost data
:::

### Data Team Topologies & Engineering Culture

::: plain
How you arrange a team decides what it can build, the same way the layout of a kitchen decides how fast a restaurant serves. If every dish must pass through one head chef, the queue at the pass grows no matter how good the cooks are. If each station cooks its own dishes using shared, well-equipped kitchen infrastructure, food flows. Team topologies is a vocabulary for designing that layout for software and data teams.
:::

**Conway's law** says that organisations design systems that mirror their communication structure. If one central data team owns every pipeline, the platform becomes one big shared codebase with one big queue. If teams are split by technology (an ingestion team, a modelling team, a BI team), every new data product needs a handoff across three teams. Team design is architecture design.

**Team Topologies** (Matthew Skelton and Manuel Pais) defines four team types and three ways they interact:

| Team type | Purpose | In a data organisation |
|---|---|---|
| Stream-aligned | Delivers value for one domain or flow of work end to end | A domain data team (orders, marketing) that owns its pipelines, models and data products |
| Platform | Provides internal services that stream-aligned teams use on their own | The data platform team: ingestion framework, orchestration, warehouse, CI/CD, observability, templates |
| Enabling | Helps other teams acquire skills and adopt practices, for a limited time | Data governance and quality coaches, a dbt or streaming guild that pairs with teams |
| Complicated-subsystem | Owns a part that needs deep specialist knowledge | A team owning the real-time feature store, a recommendation ranking engine, the identity resolution system |

| Interaction mode | Meaning | Example |
|---|---|---|
| X-as-a-Service | One team consumes another's service with minimal contact | Domain teams create pipelines from the platform's templates without tickets |
| Collaboration | Two teams work closely for a period to discover something new | Platform and orders teams jointly build the first streaming pipeline |
| Facilitating | One team helps another learn | An enabling team coaches a domain team on data contracts for six weeks |

[[fig:dataops_topologies]]

The key idea is **cognitive load**: each team can only hold so much in its head. A stream-aligned team should not need to understand Kubernetes networking to ship a pipeline; the platform hides that behind a simple, well-documented interface.

**Common data team structures.**

| Structure | How it works | Strengths | Weaknesses |
|---|---|---|---|
| Centralised | One data team serves the whole company | Consistent standards, easy hiring and mentoring | Queue of requests, far from domain knowledge |
| Embedded (decentralised) | Data engineers sit in product or business teams | Close to the domain, fast for that team | Duplicated tooling, inconsistent standards, isolated engineers |
| Hub and spoke (federated) | A central platform team plus engineers embedded in domains, with a shared community of practice | Platform consistency with domain speed | Needs clear ownership boundaries and a strong platform |

Most organisations move from centralised to hub and spoke as they grow, and data mesh (covered in the next section's Data Mesh Governance topic) is a strong form of the same idea with domain ownership of data products.

**Platform as a product.** A platform team succeeds when domain teams choose to use its platform. That means treating it as a product: know your users (the domain teams), publish a roadmap, measure adoption and satisfaction, write documentation, and offer **golden paths**: opinionated, supported ways to do common tasks, such as "new batch ingestion from a PostgreSQL source" as a template that creates the Datastream stream, BigQuery datasets, dbt staging models, monitoring and alerting in one pull request. Internal developer portals such as **Backstage** (a CNCF project) catalogue services, owners and templates in one place.

**Self-serve versus centralised.** Self-serve does not mean "every team does whatever it wants". It means the platform offers capabilities with guardrails built in: templates that already include labels, tests and monitors; policies in CI that reject unowned or unlabelled resources; access granted through groups. Centralised work remains for what genuinely must be shared: the platform itself, core shared data (customers, products, calendars) and governance standards.

**Engineering culture.** Practices from the rest of this section are cultural as much as technical: code review for all data changes, on-call shared by the teams that own the pipelines ("you build it, you run it"), blameless postmortems, documentation as part of done, and a **community of practice** (a guild or chapter) where data engineers across teams share patterns and agree standards.

> **Pitfall:** A "platform team" that is really a ticket queue. If domain teams must ask the platform team to create every dataset, topic or DAG, the platform team has become a bottleneck with a new name. Measure how much domain teams can do without a ticket, and automate the requests that arrive most often.

> **Tip:** Write down ownership. Every pipeline, table and dashboard has an owning team in the catalog and in resource labels. Unowned assets are where incidents go unanswered and costs go unexplained.

::: deeper
**Team API.** Team Topologies suggests each team publish a "team API": what it owns, how to request work, its communication channels, its service levels and its roadmap. For a platform team this is the front door that makes X-as-a-Service possible.

**Interaction modes change over time.** Collaboration is expensive and should be temporary: once the platform and the orders team have built the first streaming pipeline together, the result should become a golden path consumed as a service, and the teams move to X-as-a-Service. A collaboration that never ends is a sign that the boundary between the teams is wrong.

**Sizing.** Team Topologies uses teams of about 5 to 9 people, based on research on trust in small groups. A single data platform team supporting more than about ten stream-aligned teams usually needs to split, for example into ingestion and streaming, warehouse and transformation, and developer experience.

**Measuring the organisation.** DORA metrics (deployment frequency, lead time, change failure rate, time to restore) and platform adoption metrics (share of pipelines on golden paths, time to first production pipeline for a new team) show whether the team design helps flow. Surveys of developer satisfaction add what metrics miss.

> **Interview:** "How would you organise data engineering in a company with eight product areas?" A platform team providing ingestion, orchestration, warehouse, CI/CD and observability as self-service golden paths; data engineers aligned to product areas (stream-aligned), owning their data products and on-call; an enabling team or guild for governance, quality and standards; a complicated-subsystem team only where deep specialism is needed, such as a real-time feature platform.
:::

::: check
?? What does Conway's law predict for a company split into an ingestion team, a modelling team and a BI team?
That every data product will be built in three layers with handoffs between the teams, so each new product needs coordination across all three, which slows delivery.

?? Which team type is a data platform team, and which interaction mode should it mostly use with domain teams?
A platform team, interacting mostly through X-as-a-Service: domain teams use its self-service capabilities without needing tickets or close collaboration.

?? What is a golden path?
An opinionated, supported, ready-made way to do a common task (for example, a template for a new ingestion pipeline with tests, monitoring and labels included) that teams can use on their own.
:::

::: resources
- Book: [Team Topologies](https://teamtopologies.com/book) · Skelton and Pais; team types, interaction modes and cognitive load
- Article: [Team Topologies key concepts](https://teamtopologies.com/key-concepts) · a short summary of the four types and three modes
- Article: [Conway's law](https://martinfowler.com/bliki/ConwaysLaw.html) · Martin Fowler on why organisation shapes architecture
- Article: [CNCF platforms white paper](https://tag-app-delivery.cncf.io/whitepapers/platforms/) · what an internal platform is, its capabilities and how to measure it
- Article: [Data mesh principles](https://martinfowler.com/articles/data-mesh-principles.html) · Zhamak Dehghani on domain ownership and self-serve data platforms
- Docs: [Backstage](https://backstage.io/docs/overview/what-is-backstage) · the open-source internal developer portal
:::

### Data Engineering Maturity Model

::: plain
A maturity model is a map that shows where you are on a journey and what the next steps look like. A child learns to crawl, then walk, then run; skipping straight to running fails. In the same way, a data team that cannot yet load its tables reliably every morning should not start with real-time machine learning. A maturity model helps you see honestly which stage you are in, area by area, and choose the next investment.
:::

Maturity models describe **capabilities**, not tools. A team that has bought an expensive observability product but never answers its alerts is less mature than a team with three SQL checks it always acts on. A simple four-stage scale, often called crawl, walk, run, fly, works well for data platforms:

| Area | Crawl | Walk | Run | Fly |
|---|---|---|---|---|
| Delivery | Manual changes in consoles | Git and code review; some CI | CI/CD with tests and dry runs; IaC for most resources | Slim CI with data diffs; GitOps; safe rollouts by default |
| Testing and quality | Ad hoc checks after complaints | Key tests (unique, not null) on main tables | Tests on all models; contracts at boundaries; blocking checks | Observability on all tables; contracts generated into checks; quality SLOs |
| Reliability | Failures noticed by users | Alerts on failed jobs | SLOs for key products; on-call; runbooks; postmortems | Error budget policy applied; burn-rate alerts; chaos experiments |
| Metadata and governance | Knowledge in people's heads | Some documentation; manual access grants | Catalog with owners; lineage; access through groups and policies | Automated lineage and classification; policy as code everywhere |
| Cost | One bill nobody reads | Monthly review of the total | Labels, showback and unit costs | Chargeback, cost in design reviews, automated anomaly detection |
| Self-service | Every request via the data team | Shared templates and docs | Golden paths for common tasks | Most new data products built by domain teams without tickets |

**Self-assessment.** Score each area from 1 (crawl) to 4 (fly) with evidence, not opinion ("we have SLOs" counts only if the SLO dashboard exists and is reviewed). Then look at the shape, not the average: the weakest area usually limits the others. A runnable scoring sketch:

```python
scores = {"delivery": 3, "testing": 2, "reliability": 1, "governance": 2, "cost": 2, "self_service": 3}
importance = {"delivery": 2, "testing": 3, "reliability": 3, "governance": 2, "cost": 1, "self_service": 1}

avg = sum(scores.values()) / len(scores)
print(f"average maturity: {avg:.2f}")
# rank gaps by (distance to target level 3) x importance
gaps = sorted(((max(0, 3 - s) * importance[a], a) for a, s in scores.items()), reverse=True)
for weight, area in gaps:
    if weight:
        print(f"invest in {area}: priority {weight}")
# average maturity: 2.17
# invest in reliability: priority 6
# invest in testing: priority 3
# invest in governance: priority 2
# invest in cost: priority 1
```

The average (2.17) hides that reliability is at crawl: consumers still find failures before the team does, which undermines trust in everything else. That is the first investment.

**Prioritising platform investment.** Combine the assessment with pain and impact. Questions that help: which failures caused the most incident hours last quarter (postmortem themes)? Where does the team spend most toil? What are consumers asking for most often? What does the business plan need next year (more domains, real-time use cases, regulation)? Choose one or two areas per quarter and define the target state concretely ("every tier-1 table has a freshness SLO and an owner on call").

**Metrics for maturity.** Measure outcomes rather than activity:

- **Delivery**: DORA metrics (deployment frequency, lead time for changes, change failure rate, time to restore).
- **Reliability**: share of incidents detected by monitoring rather than by users; SLO attainment; MTTD and MTTR.
- **Quality**: share of tier-1 tables with tests, contracts and owners; number of data incidents per month.
- **Self-service**: time for a new team to ship its first production pipeline; share of pipelines on golden paths.
- **Cost**: unit costs over time; share of spend attributed by labels.

**DAMA DMBoK.** The Data Management Body of Knowledge, published by DAMA International, is the most widely cited framework for data management as a whole. Its "wheel" places **data governance** at the centre, surrounded by ten knowledge areas: data architecture; data modeling and design; data storage and operations; data security; data integration and interoperability; document and content management; reference and master data; data warehousing and business intelligence; metadata; and data quality. It is broader than data engineering (it covers documents, organisation and roles), so engineers use it mostly as a checklist that nothing important is missing, while the crawl-to-fly table above is closer to day-to-day platform work. The next section covers several of its areas (metadata, quality, security, master data) in depth.

> **Pitfall:** Treating the model as a target to reach "fly" everywhere. Many companies need only "run" in most areas. A 20-person start-up with five dashboards does not need chaos experiments or chargeback. Aim for the level the business needs, and spend the rest of the effort on delivering data products.

> **Tip:** Repeat the assessment every six months with the same questions and evidence rules, and share the results with stakeholders. Progress becomes visible, and the conversation about platform investment moves from opinions to evidence.

::: deeper
**Where the stages come from.** Maturity models descend from the Capability Maturity Model (CMM) for software processes, developed at Carnegie Mellon's Software Engineering Institute, with its levels from initial (ad hoc) to optimising. Data-specific models (DAMA's, vendor models, the FinOps Foundation's crawl, walk, run for each FinOps capability) adapt the same idea: processes become repeatable, then defined, then measured, then continuously improved.

**Fundamentals of Data Engineering's undercurrents.** Reis and Housley describe the data engineering lifecycle (generation, storage, ingestion, transformation, serving) and six "undercurrents" that run through all of it: security, data management, DataOps, data architecture, orchestration and software engineering. Assessing maturity per undercurrent is a useful alternative to the table above, and it maps closely to the groups of this course.

**Pitfalls of maturity scoring.** Self-assessments drift towards optimism; require evidence and, where possible, have someone outside the team score. Do not tie individual performance reviews to scores, or the scores will stop being honest. And do not let the model become a reason to delay value: maturity improves fastest while delivering real data products, not in a separate "platform year".

> **Interview:** "You join a company as its first senior data engineer. How do you decide what to fix first?" Interview consumers and the team, score the main areas with evidence, and list recent incidents and toil. Fix what most undermines trust first, usually reliability basics (alerts on key tables, ownership, tests on keys) and version control with CI, then build towards golden paths, SLOs and cost attribution, revisiting the assessment every few months.
:::

::: check
?? Why look at the weakest area rather than the average maturity score?
The weakest capability usually limits the value of the others (for example, excellent delivery is undermined if consumers keep finding failures first), so an average hides the real constraint.

?? Give two outcome metrics for data platform maturity.
Any two of: DORA metrics, share of incidents detected by monitoring rather than users, SLO attainment, time for a new team to ship its first pipeline, share of spend attributed by labels, unit cost trends.

?? What sits at the centre of the DAMA DMBoK wheel?
Data governance, surrounded by the ten other knowledge areas such as data architecture, data quality, metadata and data security.
:::

::: resources
- Book: [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) · Reis and Housley; the lifecycle and its undercurrents as a maturity lens
- Docs: [DAMA DMBoK](https://www.dama.org/cpages/body-of-knowledge) · DAMA International's Data Management Body of Knowledge
- Docs: [DORA research](https://dora.dev/) · the delivery performance metrics and capabilities behind them
- Docs: [FinOps maturity model](https://www.finops.org/framework/maturity-model/) · crawl, walk, run applied capability by capability
- Article: [The DataOps Manifesto](https://dataopsmanifesto.org/) · the principles behind DataOps as a practice
- Docs: [Google Cloud Adoption Framework](https://cloud.google.com/adoption-framework) · Google's themes and phases for assessing cloud maturity
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| dbt slim CI | `dbt build --select state:modified+ --defer --state prod-state/` |
| Validate SQL and cost without running | BigQuery dry run (`bq query --dry_run`) |
| Keyless CI auth to Google Cloud | Workload Identity Federation (`google-github-actions/auth`) |
| Safe schema change | Add nullable columns; drops, renames and type changes need a versioned migration |
| Infrastructure as code | `terraform plan` in CI on every pull request, `apply` after merge, remote state in Cloud Storage |
| GitOps | Git is the desired state; Argo CD or Flux pulls and reconciles, CI never holds cluster credentials |
| Blue-green for tables | Build `v2`, validate, `CREATE OR REPLACE VIEW` to switch; keep `v1` for rollback |
| Every run | Write-audit-publish: write hidden, check, then publish |
| Test levels | Many unit tests, fewer integration and contract tests, a few golden end-to-end tests |
| Test the keys first | `unique` and `not_null` on primary keys, `relationships` on important foreign keys |
| Error budget | `1 - SLO`, times events in the window (99% of 720 hourly loads = 7.2 misses) |
| Burn-rate alert | Page on fast burn (about 14.4x over 1 hour), ticket on slow burn |
| What to page on | Symptoms consumers feel (freshness, backlog, SLO burn), with a runbook link |
| Structured logs | One JSON object per line with `severity`, `message`, `run_id`, `correlation_id` |
| Incident roles | Incident commander, operations lead, communications lead; mitigate before root cause |
| Exactly-once in practice | At-least-once delivery plus idempotent (`MERGE` by key) or transactional sinks |
| Alert grouping | Use lineage: most upstream anomalous table is the root-cause candidate |
| Secrets | Prefer identity (no secret), then dynamic secrets, then Secret Manager with rotation |
| Average slots used | `SUM(total_slot_ms) / milliseconds in the period` from `INFORMATION_SCHEMA.JOBS` |
| Team design | Stream-aligned domain teams on a self-service platform; enabling teams for skills |

### Practice

::: try
**1. Set an SLO.** A dbt model `fct_orders` is published hourly. Consumers say "we can live with a few late hours a month, but never more than one day late". Propose an SLI and SLO over 30 days, compute the error budget in late hours, and say what should page someone.
:::

?? A solution to 1
SLI: the share of hourly partitions published within 30 minutes of the hour. SLO: 99% over a rolling 30 days. Budget: 720 × 0.01 = 7.2, so 7 late hours a month. Page on fast budget burn (for example, three consecutive late hours, or a burn rate that would spend the month's budget within a day) and on any partition more than 6 hours late; send single late hours to the team channel. Add a separate hard limit (a page and a SEV2 incident) if the table is more than 24 hours stale.

::: try
**2. Make a sink idempotent.** Rewrite the naive append in the chaos simulation so that redelivered events are harmless when the sink is a list of rows (not a dictionary), and show that it ends with 1,000 rows.
:::

?? A solution to 2

```python
import random

def run(seed=7, n=1000, crash_rate=0.05):
    rng = random.Random(seed)
    queue, rows, seen = list(range(n)), [], set()
    while queue:
        event = queue[0]
        if event not in seen:            # dedupe on the event id before writing
            rows.append(event)
            seen.add(event)
        if rng.random() < crash_rate:
            continue                     # crash before ack: redelivered
        queue.pop(0)
    return len(rows)

print(run())        # 1000
```

In a warehouse, the `seen` set is the target table itself: `MERGE ... ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT`.

::: try
**3. Chargeback.** A shared BigQuery reservation costs 20,000 per month. Last month 10% of it sat idle. Teams used these slot-ms: `{"orders": 6e9, "ads": 3e9, "finance": 1e9}`. How much does each team pay, and who pays for idle capacity?
:::

?? Answer to 3
Used cost is 20,000 × 0.9 = 18,000, split by share of 10e9 slot-ms: orders 60% (10,800), ads 30% (5,400), finance 10% (1,800). The idle 2,000 stays in the platform budget, which gives the platform team the incentive to size the reservation correctly.

::: try
**4. Incident response.** At 09:15 a finance analyst reports that yesterday's revenue on the executive dashboard is about twice the usual value. Lineage shows the dashboard reads `fct_revenue`, which reads `stg.payments`, and `stg.payments` loaded twice yesterday after an Airflow retry. Write the first four actions you take, the severity, and one action item for the postmortem.
:::

?? Answer to 4
Severity: SEV2 (wrong data on a critical internal dashboard used for decisions; SEV1 if it feeds external reporting). Actions: open an incident with an incident commander and a channel; mark the dashboard as unreliable and post a status update; mitigate by pointing the consumer view at the last good version of `fct_revenue` or by deleting the duplicated load from `stg.payments` and rebuilding; verify with a uniqueness check on payment IDs and a reconciliation of totals against the payment system. Postmortem action item: make the `stg.payments` load idempotent (`MERGE` on payment ID instead of `INSERT`) and add a `unique` test on the key that blocks publishing.

::: try
**5. Keep secrets out of Git.** A GKE-hosted Airflow deployment managed with Argo CD needs a Salesforce API token. Describe where the token lives, how it reaches the pod, how it is rotated, and what is committed to Git.
:::

?? Answer to 5
The token lives in Secret Manager. Git holds only an External Secrets Operator `ExternalSecret` (or a CSI volume definition) that names the secret; the operator, authenticated through Workload Identity Federation for GKE, creates and refreshes the Kubernetes Secret that the pod mounts. Rotation adds a new secret version on a schedule (triggered by Secret Manager's Pub/Sub rotation notice), the operator refreshes within its `refreshInterval`, workers pick it up on restart, and the old version is disabled after a grace period, then destroyed. No token value ever appears in Git, images or logs.

### What comes next

You can now change a data platform safely, test it at every level, keep it reliable, respond to incidents, protect its secrets, explain its costs and organise the people who run it. The next section, **Data governance, security and compliance**, turns to the rules around the data itself: catalogs and lineage, quality and observability platforms, access control, audit, privacy and regulation, and uses the CI/CD, testing and infrastructure-as-code habits from this section to enforce them as code.
