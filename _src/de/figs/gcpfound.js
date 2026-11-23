/* Google Cloud foundations for data: section diagrams */
Object.assign(FIGS, {

gcpfound_map:{ t:"The Google Cloud data platform map", cap:"Data flows left to right through ingest, process, store and serve services. Orchestration, governance and operations services wrap every stage rather than sitting at one point in the flow.", svg:S("gf1", 330, "Ingest, process, store and serve columns of Google Cloud services, with orchestration, governance and operations bands underneath",
  B(20, 20, 160, 36, "pr", ["Ingest"]) + B(205, 20, 160, 36, "pr", ["Process"]) + B(390, 20, 160, 36, "pr", ["Store"]) + B(575, 20, 165, 36, "pr", ["Serve"]) +
  B(20, 68, 160, 40, "sp", ["Pub/Sub"], false) + B(20, 114, 160, 40, "sp", ["Managed Kafka"], false) + B(20, 160, 160, 40, "sp", ["Datastream (CDC)"], false) + B(20, 206, 160, 40, "sp", ["Storage Transfer"], false) +
  B(205, 68, 160, 40, "box", ["Dataflow (Beam)"], false) + B(205, 114, 160, 40, "box", ["Managed Spark"], false) + B(205, 160, 160, 40, "box", ["Dataform, SQL"], false) + B(205, 206, 160, 40, "box", ["Cloud Run, Batch"], false) +
  B(390, 68, 160, 40, "st", ["BigQuery"], false) + B(390, 114, 160, 40, "st", ["Cloud Storage"], false) + B(390, 160, 160, 40, "st", ["Lakehouse, Iceberg"], false) + B(390, 206, 160, 40, "st", ["Bigtable, Spanner"], false) +
  B(575, 68, 165, 40, "okf", ["Looker"], false) + B(575, 114, 165, 40, "okf", ["Data Studio"], false) + B(575, 160, 165, 40, "okf", ["Agent Platform, ML"], false) + B(575, 206, 165, 40, "okf", ["Apps and APIs"], false) +
  A("gf1", "M180 88 L203 88") + A("gf1", "M365 88 L388 88") + A("gf1", "M550 88 L573 88") +
  B(20, 258, 720, 28, "ka", ["Orchestrate: Managed Airflow, Workflows, Scheduler, Eventarc"], false) +
  B(20, 292, 352, 28, "fill", ["Govern: IAM, Knowledge Catalog, VPC SC"], false) + B(388, 292, 352, 28, "fill", ["Operate: Monitoring, Logging, Billing"], false)) },

gcpfound_hierarchy:{ t:"Organization, folders, projects and resources", cap:"Policies set high in the tree flow down to everything below it. The billing account sits beside the tree: each project is linked to one billing account, which pays for that project's resources.", svg:S("gf2", 300, "An organization with prod and nonprod folders, projects inside them and resources inside projects, with a billing account linked to projects",
  B(260, 15, 240, 46, "pr", ["Organization: acme.com", "org policies, admin IAM"]) +
  A("gf2", "M330 61 L200 92") + A("gf2", "M430 61 L560 92") +
  B(100, 94, 200, 44, "fill", ["Folder: data-prod", "team IAM grants"]) + B(460, 94, 200, 44, "fill", ["Folder: data-nonprod"]) +
  A("gf2", "M160 138 L95 168") + A("gf2", "M240 138 L295 168") + A("gf2", "M560 138 L560 168") +
  B(20, 170, 170, 44, "box", ["dwh-prod", "project"]) + B(215, 170, 170, 44, "box", ["pipelines-prod", "project"]) + B(475, 170, 170, 44, "box", ["dwh-dev", "project"]) +
  A("gf2", "M105 214 L105 236") + A("gf2", "M300 214 L300 236") + A("gf2", "M560 214 L560 236") +
  B(20, 238, 170, 44, "st", ["Datasets, tables", "resource IAM"]) + B(215, 238, 170, 44, "st", ["Dataflow, Spark", "service accounts"]) + B(475, 238, 170, 44, "st", ["Dev datasets"]) +
  B(660, 170, 90, 44, "okf", ["Billing", "account"]) + A("gf2", "M660 190 L647 190", "ln dash") +
  T(40, 85, "policies inherit downwards", "sm")) },

gcpfound_separation:{ t:"Coupled nodes versus separated compute and storage", cap:"On the left each node owns a slice of the data, so compute and storage grow together. On the right stateless compute clusters of any size read one shared copy of the data over a fast network, and several clusters can work on it at once.", svg:S("gf3", 280, "Left: three nodes each with CPU and local disk. Right: three independent compute clusters reading one shared storage layer",
  T(180, 22, "Shared nothing: compute tied to disks", "b", "middle") +
  B(20, 40, 100, 50, "box", ["CPU + RAM"], false) + B(130, 40, 100, 50, "box", ["CPU + RAM"], false) + B(240, 40, 100, 50, "box", ["CPU + RAM"], false) +
  B(20, 95, 100, 50, "st", ["Disk: slice 1"], false) + B(130, 95, 100, 50, "st", ["Disk: slice 2"], false) + B(240, 95, 100, 50, "st", ["Disk: slice 3"], false) +
  T(180, 175, "add CPU means add disks and rebalance", "sm", "middle") +
  T(570, 22, "Separated: elastic compute, shared storage", "b", "middle") +
  B(400, 40, 105, 50, "sp", ["ETL cluster", "64 vCPU, nightly"]) + B(518, 40, 105, 50, "sp", ["BI warehouse", "small, all day"]) + B(636, 40, 105, 50, "sp", ["Notebook", "burst"]) +
  A("gf3", "M452 90 L452 180") + A("gf3", "M570 90 L570 180") + A("gf3", "M688 90 L688 180") +
  T(570, 140, "fast network", "sm", "middle") +
  B(400, 182, 341, 56, "st", ["Shared storage: Colossus or Cloud Storage", "one durable copy, billed per GiB-month"]) +
  T(570, 262, "scale or pause each cluster on its own", "sm", "middle")) },

gcpfound_perimeter:{ t:"A VPC Service Controls perimeter", cap:"Inside the perimeter, pipelines and BigQuery talk freely. A copy to a project outside the perimeter is blocked even when the user has valid IAM permissions, and outside access works only through an access level or an explicit ingress or egress rule.", svg:S("gf4", 290, "A perimeter containing landing, pipeline and warehouse projects; a copy to an outside project is blocked; an on-premises client reaches APIs through a Private Service Connect endpoint",
  '<rect class="fill" x="20" y="30" width="470" height="230" rx="14"/>' + T(255, 52, "Service perimeter: data_prod", "b", "middle") +
  B(40, 70, 130, 50, "box", ["landing-prod", "Cloud Storage"]) + B(190, 70, 130, 50, "box", ["pipelines-prod", "Dataflow"]) + B(340, 70, 130, 50, "box", ["dwh-prod", "BigQuery"]) +
  A("gf4", "M170 95 L188 95") + A("gf4", "M320 95 L338 95") +
  B(40, 170, 200, 56, "ka", ["PSC endpoint for APIs", "private IP in your VPC"]) +
  B(560, 70, 180, 50, "pr", ["personal-project", "outside the perimeter"]) +
  A("gf4", "M470 105 L558 105", "ln dash") + T(515, 128, "copy blocked", "tR", "middle") +
  B(560, 175, 180, 50, "okf", ["On-premises client", "via Interconnect or VPN"]) +
  A("gf4", "M560 200 L242 200") + T(400, 192, "allowed by access level", "sm", "middle")) },

gcpfound_egress:{ t:"Where data transfer is charged", cap:"Traffic inside a zone is free, crossing zones costs a little, crossing regions more, crossing continents more again, and internet egress the most. Ingress is free. Put compute next to the data it reads.", svg:S("gf5", 270, "Nested boxes for zone, region and continent with rising transfer prices for each boundary crossed",
  '<rect class="fill" x="20" y="20" width="520" height="220" rx="14"/>' + T(40, 42, "Continent: North America", "b") +
  '<rect class="box" x="40" y="55" width="330" height="170" rx="12"/>' + T(60, 77, "Region: us-central1", "b") +
  B(60, 95, 130, 60, "sp", ["Zone a", "worker, bucket"]) + B(220, 95, 130, 60, "sp", ["Zone b", "worker"]) +
  A("gf5", "M190 125 L218 125") + T(205, 175, "about $0.01/GiB", "sm", "middle") +
  T(125, 205, "same zone: free", "sm", "middle") +
  B(395, 95, 130, 60, "st", ["us-east1", "another region"]) + A("gf5", "M350 140 L393 140") + T(460, 180, "about $0.02/GiB", "sm", "middle") +
  B(580, 40, 160, 60, "pr", ["Europe region", "other continent"]) + A("gf5", "M525 110 L578 75") + T(660, 120, "about $0.05/GiB", "sm", "middle") +
  B(580, 160, 160, 60, "ka", ["Internet", "highest, tiered"]) + A("gf5", "M540 190 L578 190") +
  T(380, 262, "Ingress into Google Cloud is free. Prices at the time of writing; check the pricing page.", "sm", "middle")) },

});
