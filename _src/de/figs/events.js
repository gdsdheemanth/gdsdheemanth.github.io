/* events: Event-driven architecture, original drawings */
Object.assign(FIGS, {

events_es:{ t:"An event-sourced aggregate", cap:"The stream for account acc-1 is an append-only list of facts, each with a version. To handle a command, the service rehydrates the account by folding its events (optionally starting from a snapshot), then appends new events with the version it expects. Projections read the same events to build read models.", svg:S("events1", 300, "An event stream of three versions, rehydration into current state, a snapshot, a projection and a failed concurrent append",
  T(20, 26, "Stream acc-1 (append-only)", "b") +
  B(20, 40, 150, 56, "st", ["v1 AccountOpened", "owner asha"]) +
  B(185, 40, 150, 56, "st", ["v2 MoneyDeposited", "amount 100"]) +
  B(350, 40, 150, 56, "st", ["v3 MoneyWithdrawn", "amount 30"]) +
  B(515, 40, 150, 56, "pr", ["v4 next event", "append expects v3"]) +
  A("events1", "M170 68 L183 68") + A("events1", "M335 68 L348 68") + A("events1", "M500 68 L513 68", "acc dash") +
  T(712, 72, "time", "sm", "middle") +
  A("events1", "M130 96 L130 148") +
  B(20, 150, 220, 70, "sp", ["Rehydrate", "fold apply() over v1 to v3", "balance 70, version 3"]) +
  B(275, 150, 210, 70, "okf", ["Snapshot (optional)", "state at v3", "a cache, safe to rebuild"]) +
  A("events1", "M275 185 L242 185", "ln dash") +
  A("events1", "M590 96 L630 148") +
  B(520, 150, 220, 70, "ka", ["Projection", "reads all streams in order", "keeps a checkpoint"]) +
  B(20, 245, 465, 40, "pr", ["Two writers both expect v3: the second append fails"]) +
  A("events1", "M630 220 L630 243") +
  B(520, 245, 220, 40, "box", ["Read model: balances"])) },

events_outbox:{ t:"The transactional outbox", cap:"The service writes the business row and the outbox row in one local transaction, so both exist or neither does. A relay (a polling publisher or CDC such as Debezium) reads committed outbox rows and publishes them. A relay crash can publish an event twice, so consumers deduplicate on the event id.", svg:S("events2", 310, "A service writing orders and outbox rows in one transaction, a relay publishing to a broker and a deduplicating consumer",
  B(20, 95, 130, 60, "sp", ["Order service", "place_order()"]) +
  '<rect class="fill" x="180" y="30" width="220" height="180" rx="12"/>' +
  T(290, 50, "One local transaction", "b", "middle") + T(290, 66, "Cloud SQL, AlloyDB", "sm", "middle") +
  B(200, 78, 180, 50, "box", ["orders", "INSERT o1"]) +
  B(200, 145, 180, 50, "ka", ["outbox", "INSERT OrderPlaced"]) +
  A("events2", "M150 115 L198 103") + A("events2", "M150 135 L198 168") +
  B(430, 145, 150, 50, "st", ["Relay", "poll or CDC (Debezium)"]) +
  A("events2", "M380 170 L428 170") + T(560, 112, "reads committed rows only:", "sm", "middle") +
  T(560, 128, "the broker is never part of the transaction", "sm", "middle") +
  B(610, 145, 130, 50, "ka", ["Kafka, Pub/Sub", "topic orders"]) +
  A("events2", "M580 170 L608 170") +
  B(610, 240, 130, 50, "okf", ["Consumer", "inbox: skip seen ids"]) +
  A("events2", "M675 195 L675 238") +
  B(20, 240, 560, 50, "pr", ["At-least-once: a relay crash after publishing re-sends the event", "so every consumer deduplicates on the event id"])) },

events_saga:{ t:"An orchestrated saga with compensations", cap:"Checkout runs as four local transactions in three services. If charging the card (the pivot) fails, the orchestrator runs the compensations for the completed steps in reverse order. Steps after the pivot are never compensated: they are retried until they succeed.", svg:S("events3", 310, "Four saga steps in a row, two compensations below triggered by a failure at the pivot, and a comparison of choreography and orchestration",
  T(20, 24, "Orchestrated checkout saga", "b") +
  B(20, 40, 160, 60, "sp", ["T1 Create order", "status PENDING"]) +
  B(210, 40, 160, 60, "sp", ["T2 Reserve stock", "returns reservation id"]) +
  B(400, 40, 160, 60, "ka", ["T3 Charge card", "the pivot transaction"]) +
  B(590, 40, 150, 60, "okf", ["T4 Confirm, ship", "retriable"]) +
  A("events3", "M180 70 L208 70") + A("events3", "M370 70 L398 70") + A("events3", "M560 70 L588 70") +
  A("events3", "M480 100 L480 175 L372 175", "bad dash") + T(490, 140, "if T3 fails", "sm tR", "start") +
  B(210, 150, 160, 50, "pr", ["C2 Release stock", "by reservation id"]) +
  A("events3", "M210 175 L182 175", "bad dash") +
  B(20, 150, 160, 50, "pr", ["C1 Cancel order", "status FAILED"]) +
  T(665, 145, "after the pivot:", "sm", "middle") + T(665, 162, "retry, never undo", "sm", "middle") +
  B(20, 225, 350, 70, "st", ["Choreography", "services react to each other's events", "no coordinator, the flow is implicit"]) +
  B(390, 225, 350, 70, "sp", ["Orchestration", "Temporal or Workflows sends commands", "flow and saga state in one place"])) },

events_storming:{ t:"The Event Storming grammar", cap:"Process-level Event Storming reads like a sentence: an actor issues a command, an aggregate (or system) handles it and records an event, a policy reacts to the event with a new command, and read models give actors what they need to decide. Each sticky note becomes something concrete in the data platform.", svg:S("events4", 315, "A row of actor, command, aggregate, event and read model, then a policy issuing a command to another aggregate, and the mapping to data platform objects",
  A("events4", "M676 50 L676 30 L84 30 L84 48", "ln dash") + T(380, 24, "the actor reads it to decide what to do next", "sm", "middle") +
  B(20, 50, 128, 60, "box", ["Customer", "actor"]) +
  B(168, 50, 128, 60, "sp", ["Place order", "command"]) +
  B(316, 50, 128, 60, "fill", ["Order", "aggregate"]) +
  B(464, 50, 128, 60, "ka", ["Order placed", "event"]) +
  B(612, 50, 128, 60, "okf", ["Order status", "read model"]) +
  A("events4", "M148 80 L166 80") + A("events4", "M296 80 L314 80") + A("events4", "M444 80 L462 80") + A("events4", "M592 80 L610 80") +
  A("events4", "M528 110 L528 148") +
  B(464, 150, 128, 60, "pr", ["Whenever placed", "then reserve stock"]) +
  B(316, 150, 128, 60, "sp", ["Reserve stock", "command"]) +
  B(168, 150, 128, 60, "fill", ["Stock", "aggregate"]) +
  B(20, 150, 128, 60, "ka", ["Stock reserved", "event"]) +
  A("events4", "M464 180 L446 180") + A("events4", "M316 180 L298 180") + A("events4", "M168 180 L150 180") +
  T(676, 176, "policy: a reaction", "sm", "middle") + T(676, 192, "to an event", "sm", "middle") +
  B(20, 245, 720, 55, "fill", ["For the data platform", "events become event and fact tables, aggregates become entities and dimensions,", "read models become marts, policies become stream processors or triggers"])) },

events_mesh:{ t:"Events as data products in a mesh", cap:"Each domain owns its public topics and publishes them through an outbox. The backbone carries the events; consuming domains read them with their own consumer groups, and a second output port lands the same events as tables in BigQuery. The platform team provides the registry, catalog, lineage and access rules that every domain uses.", svg:S("events5", 335, "Three producing domains, an event backbone with three versioned topics, consuming domains and BigQuery, and a shared platform layer",
  B(20, 30, 200, 60, "sp", ["Orders domain", "outbox, owns orders.*"]) +
  B(20, 105, 200, 60, "sp", ["Payments domain", "owns payments.*"]) +
  B(20, 180, 200, 60, "sp", ["Logistics domain", "owns logistics.*"]) +
  '<rect class="fill" x="255" y="20" width="250" height="235" rx="12"/>' +
  T(380, 42, "Event backbone", "b", "middle") + T(380, 58, "Managed Kafka or Pub/Sub", "sm", "middle") +
  B(270, 72, 220, 36, "ka", ["orders.order-placed.v1"], false) +
  B(270, 122, 220, 36, "ka", ["payments.payment-captured.v1"], false) +
  B(270, 172, 220, 36, "ka", ["logistics.parcel-shipped.v1"], false) +
  T(380, 232, "contract: schema, owner, SLO", "sm", "middle") +
  A("events5", "M220 60 L268 88") + A("events5", "M220 135 L268 140") + A("events5", "M220 210 L268 192") +
  B(550, 30, 190, 60, "okf", ["Analytics domain", "groups analytics.*"]) +
  B(550, 105, 190, 60, "okf", ["Marketing domain", "groups marketing.*"]) +
  B(550, 180, 190, 60, "st", ["BigQuery tables", "second output port"]) +
  A("events5", "M490 90 L548 62") + A("events5", "M490 140 L548 135") + A("events5", "M490 190 L548 208") +
  B(20, 272, 720, 50, "pr", ["Self-serve platform and federated governance", "schema registry, Knowledge Catalog, lineage, IAM and ACLs by topic prefix"])) },

});
