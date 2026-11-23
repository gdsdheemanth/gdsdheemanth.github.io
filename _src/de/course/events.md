This section is for data engineers who work next to application teams that talk in events, and for anyone who has wondered why a stream of "OrderPlaced" messages is shaped the way it is. It builds directly on **Streaming systems** (Kafka, Pub/Sub, delivery guarantees, schema registries and schema evolution) and on **Databases: OLTP, NoSQL and specialised** (transactions and replication). The code is Python using only the standard library, plus SQL, YAML and a little configuration.

Event-driven architecture is usually taught as an application topic, but data engineers live with its consequences: the events services publish become the raw material of pipelines, the outbox tables they write become CDC sources, and the schemas they choose decide whether a downstream table survives the next deployment. Understanding the patterns lets you ask for events that are useful, and design the ones your own platform publishes.

**What you will be able to do afterwards**

- Explain event sourcing and CQRS, build a small event store and a read model, and say when each pattern is worth its complexity.
- Publish events reliably with the transactional outbox and keep consumers correct with idempotency.
- Coordinate a multi-step business process with a saga, choosing between choreography and orchestration, and design compensations.
- Design event schemas with a CloudEvents envelope, version them safely and document them with AsyncAPI and EventCatalog.
- Run an Event Storming session and turn its output into event and table designs, and design data APIs with sound pagination, versioning and caching.
- Use events as data products in a data mesh, with Kafka or Pub/Sub as the backbone, topic naming rules, ownership and consumer isolation.

**The big picture.** In a request-driven system, services call each other and ask for things ("charge this card"). In an event-driven system, services announce facts ("an order was placed") and anyone interested reacts. Announcing facts decouples the teams: the producer does not need to know who listens. The price is that consistency becomes eventual, failures happen between steps, and the *shape* of every event becomes a public contract. The three groups follow that arc: patterns for state and consistency, the design of the events themselves, and how events become shared data products across a company.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Patterns

These five patterns answer the questions that appear as soon as services stop sharing one database. Where does the truth live (event sourcing)? How do you serve fast queries over it (CQRS)? How do you publish an event and update a database without one of them going missing (outbox)? How do you run a business process that spans several services when you cannot use one transaction (sagas)? And how should the data of many services be organised in the first place (microservice data patterns)?

They build on each other: an outbox is how an event-sourced or ordinary service publishes reliably, CQRS read models are fed by those events, and sagas are coordinated through them. For a data engineer, the same patterns explain the change streams, projections and "status" tables you will ingest.

### Event Sourcing Pattern

::: plain
A bank does not store your balance and overwrite it with every payment. It keeps the full list of transactions, and the balance is simply the result of adding them up. Event sourcing stores application data the same way: instead of saving the current state of an order or an account, it saves every change as an **event** in an append-only list, and works out the current state by replaying them. Nothing is ever overwritten, so you always know how you got here.
:::

In a normal ("state-oriented") application, a table row holds the current state and each update overwrites it. The history is lost unless someone adds an audit table. In an **event-sourced** application, the system of record is an **event store**: an append-only log of immutable facts, each named in the past tense, such as `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`. The current state of an entity is derived by folding its events in order.

**Core vocabulary.**

- **Aggregate:** a cluster of data treated as one unit for changes, with an id (one bank account, one order). It is the consistency boundary: one aggregate's invariants ("balance never below zero") are checked inside one write.
- **Stream:** the ordered events of one aggregate, for example `account-42`. Each event has a **version** (its position in the stream).
- **Command:** a request to change something (`WithdrawMoney`). The aggregate checks it against its current state and either rejects it or emits new events.
- **Rehydration:** loading the stream and replaying it to rebuild the aggregate's state before handling a command.
- **Projection:** a process that reads events and builds a **read model** (a table optimised for queries). This is where CQRS, the next topic, comes in.

**Optimistic concurrency.** Two users may load the same account at version 2 and both try to withdraw. The event store prevents a lost update by requiring every append to state the version it expects; if someone else appended first, the write fails and the command is retried against the new state. A unique key on `(stream_id, version)` is enough to implement it:

```python
import json, sqlite3
from functools import reduce

db = sqlite3.connect(":memory:")
db.execute("""CREATE TABLE events (
    stream_id TEXT, version INTEGER, type TEXT, data TEXT,
    PRIMARY KEY (stream_id, version))""")       # one version number per stream, once

def append(stream_id, expected_version, events):
    """Append events if nobody else has written since `expected_version`."""
    with db:                                     # one transaction: all or nothing
        for v, (etype, data) in enumerate(events, start=expected_version + 1):
            db.execute("INSERT INTO events VALUES (?, ?, ?, ?)",
                       (stream_id, v, etype, json.dumps(data)))

def load(stream_id):
    rows = db.execute("SELECT type, data FROM events WHERE stream_id = ? ORDER BY version",
                      (stream_id,))
    return [(t, json.loads(d)) for t, d in rows]

def apply(state, event):
    etype, data = event
    if etype == "AccountOpened":
        return {"owner": data["owner"], "balance": 0, "version": 1}
    if etype == "MoneyDeposited":
        return {**state, "balance": state["balance"] + data["amount"], "version": state["version"] + 1}
    if etype == "MoneyWithdrawn":
        return {**state, "balance": state["balance"] - data["amount"], "version": state["version"] + 1}
    return state

append("acc-1", 0, [("AccountOpened", {"owner": "asha"}), ("MoneyDeposited", {"amount": 100})])
append("acc-1", 2, [("MoneyWithdrawn", {"amount": 30})])

account = reduce(apply, load("acc-1"), None)    # rehydrate: fold over all events
print(account)        # {'owner': 'asha', 'balance': 70, 'version': 3}

try:                  # a second writer that loaded the account at version 2
    append("acc-1", 2, [("MoneyWithdrawn", {"amount": 50})])
except sqlite3.IntegrityError as err:
    print("conflict:", err)   # conflict: UNIQUE constraint failed: events.stream_id, events.version
```

[[fig:events_es]]

**Event store design.** Whatever technology you use, an event store needs: append with an expected version (optimistic concurrency); fast reads of one stream in order; a global ordering or position so projections can read "everything after position N"; and a way to publish new events to subscribers. Options:

| Option | Strengths | Watch out for |
|---|---|---|
| A purpose-built store such as KurrentDB (formerly EventStoreDB) or Axon Server | Streams, expected versions and subscriptions built in | One more system to run |
| A relational table (Cloud SQL for PostgreSQL, AlloyDB, Spanner) | Transactions, familiar tooling; libraries such as Marten (.NET on PostgreSQL) | Publishing to subscribers needs an outbox or CDC |
| Kafka alone | Durable, ordered, fan-out to many consumers | No per-stream expected-version check; reading one aggregate's history means scanning a partition |

Kafka is an excellent **transport** for events and a good long-term log, but most teams keep the authoritative per-aggregate store in a database and publish to Kafka or Pub/Sub from there.

**Snapshots.** An account with 50,000 events is slow to rehydrate. A **snapshot** saves the folded state at some version; rehydration loads the latest snapshot and replays only later events. Snapshots are a cache: you can delete and rebuild them at any time.

**Versioning and upcasting.** Events are stored forever, but their shape changes. Because you cannot rewrite history (and should not), you handle old shapes when reading. An **upcaster** converts an old version of an event into the current one on the fly, so the application code only knows the latest shape:

```python
def upcast(event):
    """Turn any stored version of MoneyDeposited into the current shape (v2)."""
    if event["type"] == "MoneyDeposited" and event.get("schema", 1) == 1:
        # v1 stored euros as a float; v2 stores integer cents plus a currency
        data = {"amount_cents": round(event["data"]["amount"] * 100), "currency": "EUR"}
        return {**event, "schema": 2, "data": data}
    return event

old = {"type": "MoneyDeposited", "schema": 1, "data": {"amount": 12.5}}
print(upcast(old)["data"])    # {'amount_cents': 1250, 'currency': 'EUR'}
```

Other strategies: give events a new type name for a truly different meaning (`MoneyDepositedV2`), weakly typed deserialisation that tolerates missing and extra fields, and, rarely, a **copy-and-transform** migration into a new store.

**Why use it, and why not.** Event sourcing gives a complete audit trail, the ability to answer "what was the state on 3 March?", to build new read models from history long after the events happened, and to debug by replaying. It fits domains that already think in transactions: payments, ledgers, inventory movements, logistics, insurance claims. It is a poor fit for simple CRUD data, adds real complexity (eventual consistency in reads, versioning forever, harder ad hoc queries), and is hard to remove once adopted.

> **Pitfall:** Treating every database change as an event ("OrderUpdated" with the whole row). That is change data capture with extra steps and loses the business meaning. Events should record *what happened in the domain* (`OrderShipped`, `AddressCorrected`), which is what makes the history useful.

> **Tip:** For data engineers, an event-sourced service is a gift: its event log is a complete, ordered history. Ingest the events themselves (not only the projected state tables) into BigQuery, partitioned by event date, and you can rebuild any historical view with SQL.

::: deeper
**Personal data and immutability.** "Never delete events" collides with the right to erasure under GDPR. The common technique is **crypto-shredding**: encrypt personal fields in each event with a per-person key held in a key store (Cloud KMS or a key table protected by it), and delete the key when the person asks to be forgotten, which makes their data unreadable everywhere, including in backups. The alternative is to keep personal data out of events and reference it by id.

**Global ordering.** Projections that combine streams need a global position. Relational stores often use a sequence, but sequence numbers can commit out of order (transaction A takes 101, transaction B takes 102 and commits first), so a projection reading "position > 100" may skip 101 forever. Event stores solve this with a commit-ordered position, a short delay window, or by tracking gaps.

**Event sourcing versus CDC.** CDC (covered in **Data ingestion**) captures row changes from any database after the fact; event sourcing makes domain events the primary data. CDC is cheaper to adopt and works with existing services; event sourcing carries intent. Many architectures combine them: a normal service writes rows plus an outbox, and CDC publishes the outbox (see Transactional Outbox Pattern).

> **Interview:** "How do you prevent two concurrent commands corrupting an event-sourced aggregate?" Optimistic concurrency: each append names the expected stream version, enforced by a unique `(stream_id, version)` key or the store's API. The losing command reloads the stream and retries or reports a conflict.
:::

::: check
?? How is the current state of an aggregate obtained in an event-sourced system?
By loading its stream of events in version order and folding them through an apply function, optionally starting from the latest snapshot.

?? What does an upcaster do, and why is it needed?
It converts an older stored version of an event into the current shape when the event is read. Events are immutable and kept forever, so old shapes must be handled at read time instead of rewriting history.

?? Why is Kafka on its own usually not used as the authoritative event store for aggregates?
It has no per-stream expected-version check for optimistic concurrency, and reading the events of one aggregate means scanning a whole partition. It is better as the transport and long-term log fed from a store.
:::

::: resources
- Article: [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) · Martin Fowler's original description of the pattern
- Docs: [Event Sourcing pattern (microservices.io)](https://microservices.io/patterns/data/event-sourcing.html) · Chris Richardson's summary with benefits, drawbacks and related patterns
- Book: [Versioning in an Event Sourced System](https://leanpub.com/esversioning) · Greg Young's short book on upcasting, weak schemas and migrations (free to read online)
- Book: [Building Microservices, 2nd edition](https://www.oreilly.com/library/view/building-microservices-2nd/9781492034018/) · Sam Newman on events, state and when event sourcing pays off
- Docs: [KurrentDB documentation](https://docs.kurrent.io/) · a purpose-built event store: streams, expected versions and subscriptions
- Talk: [Greg Young: CQRS and Event Sourcing](https://www.youtube.com/watch?v=JHGkaShoyNs) · the pattern explained by the person who popularised it
:::

### CQRS Pattern

::: plain
A library keeps one ledger where every new book, loan and return is recorded carefully, and a separate catalogue that is easy to search by author or subject. Librarians update the ledger; readers search the catalogue; and the catalogue is updated from the ledger a little later. CQRS (command query responsibility segregation) does the same with software: one model handles **changes**, another (or several) are built purely for **reading**, each shaped for its job.
:::

**From CQS to CQRS.** Bertrand Meyer's **command-query separation** (CQS) says a method should either change state (a command) or return data (a query), not both. Greg Young extended the idea to architecture around 2010: use **separate models** for writes and reads. The write side (the **command model**) enforces business rules and is normalised for correctness. The read side consists of one or more **read models** (also called query models, projections or materialised views), each denormalised for a particular screen, API or report.

**Why separate them?** Reads and writes have different needs. An order service's writes touch one order at a time and must check rules; its reads might be "all open orders for this customer with product names and delivery status", "revenue per region per hour" or full-text search. A single schema serving all of these is a compromise for each. With CQRS:

- each read model can use the best store for its queries (a relational table, a search index, a key-value store, BigQuery),
- reads and writes scale independently,
- the write model stays small and focused on rules.

**How read models are kept up to date.** The read side is fed from the write side by one of these strategies:

| Strategy | How | Consistency | Notes |
|---|---|---|---|
| Synchronous projection | Update the read table in the same transaction as the write | Immediate | Only works when both live in the same database |
| Asynchronous projection from events | A projector consumes domain events and updates read models | Eventual (milliseconds to seconds) | The usual choice with event sourcing |
| CDC | Stream row changes from the write database into read stores | Eventual | Works with an ordinary write model; Datastream or Debezium |
| Scheduled rebuild | Recompute the read model periodically | Stale until the next run | Fine for analytics; BigQuery scheduled queries or materialized views |

A projector must be **idempotent** and **resumable**. It stores a **checkpoint** (the last event position it applied), ideally in the same transaction as the read model update, so a crash and restart neither skips nor double-applies events:

```python
events = [   # (global position, type, data) as read from the event log
    (1, "OrderPlaced",    {"order": "o1", "customer": "asha", "total": 40}),
    (2, "OrderPlaced",    {"order": "o2", "customer": "bo",   "total": 15}),
    (3, "OrderPlaced",    {"order": "o3", "customer": "asha", "total": 22}),
    (4, "OrderCancelled", {"order": "o2"}),
]

class CustomerTotals:
    """A read model: spend per customer, kept up to date from events."""
    def __init__(self):
        self.checkpoint, self.totals, self.orders = 0, {}, {}
    def handle(self, pos, etype, data):
        if pos <= self.checkpoint:          # already applied: replays are harmless
            return
        if etype == "OrderPlaced":
            self.orders[data["order"]] = (data["customer"], data["total"])
            self.totals[data["customer"]] = self.totals.get(data["customer"], 0) + data["total"]
        elif etype == "OrderCancelled":
            customer, total = self.orders.pop(data["order"])
            self.totals[customer] -= total
        self.checkpoint = pos                # in production, saved with the read model

view = CustomerTotals()
for e in events[:3]:
    view.handle(*e)
print(view.totals, view.checkpoint)      # {'asha': 62, 'bo': 15} 3
for e in events:                         # redelivery from the start plus the new event
    view.handle(*e)
print(view.totals, view.checkpoint)      # {'asha': 62, 'bo': 0} 4
```

**Living with eventual consistency.** A user who places an order and is immediately redirected to "my orders" may not see it yet, because the projector has not caught up. Common remedies:

- Return the new version or position from the command, and have the query wait until the read model has reached it (or read that one item from the write model).
- Update the user interface optimistically with what the user just did.
- Show "processing" states honestly for operations that take time anyway.
- Monitor **projection lag** (newest event position minus checkpoint, or the age of the oldest unprocessed event) as a key metric.

**Rebuilding.** Because read models are derived, you can delete one and rebuild it by replaying the event log from the start, for example to fix a bug or add a new column. Plan for it: rebuild into a new table while the old one serves traffic, then switch. With millions of events, replay speed (batching, parallelism per partition) decides whether this takes minutes or days.

**CQRS without event sourcing, and the reverse.** They are often paired, but independent. A plain relational service can have CQRS by streaming its changes into a search index or BigQuery. An event-sourced service almost always needs CQRS, because querying event streams directly is impractical.

**On Google Cloud.** A typical layout: the write model in Cloud SQL, AlloyDB or Spanner; events or CDC through Pub/Sub, Managed Service for Apache Kafka or Datastream; read models in Firestore or Bigtable for low-latency lookups, in BigQuery for analytics (with materialized views for common aggregates), and in a search engine for text queries. Dataflow or Cloud Run services act as projectors.

> **Pitfall:** Applying CQRS to a whole system by default. For simple CRUD screens it doubles the code and adds eventual consistency for no benefit. Use it for the parts with clearly different read and write needs, such as a busy query API over complex domain logic.

> **Tip:** The analytics warehouse is the read model most companies already have. When a service team asks for "a reporting endpoint", consider whether a CDC or event feed into BigQuery, with a documented table, serves them better than another API.

::: deeper
**Projection ordering across partitions.** If events are partitioned (by aggregate id in Kafka), a read model that combines aggregates sees events from different partitions in an arbitrary interleaving. Projections must therefore depend only on per-aggregate order, or handle out-of-order arrival (for example, an `OrderCancelled` arriving before its `OrderPlaced` in a cross-partition view is stored and applied later).

**Upserts in BigQuery read models.** A streaming projector into BigQuery often writes changes with the Storage Write API and relies on primary keys with CDC upserts (the `_CHANGE_TYPE` pseudocolumn with `UPSERT` and `DELETE`), or appends events and builds the current-state view with `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY position DESC) = 1`. The append-then-deduplicate approach is simpler to rebuild and audit.

**Read model per consumer.** In larger systems each consuming team builds the read model it needs from the shared event stream, instead of asking the producer for new query endpoints. This keeps producers simple and is the same idea that data mesh applies to analytics (see Data Mesh Event Patterns).

> **Interview:** "A user saves a profile and the next page shows old data. The system uses CQRS. What is happening and how do you fix it?" The read model has not yet consumed the event. Fix by returning the write's version and having the read wait for or bypass the projection for that user, or by updating the UI optimistically, and monitor projection lag.
:::

::: check
?? What is the difference between the command model and a read model in CQRS?
The command model handles changes and enforces business rules, normalised for correctness. Read models are denormalised copies built for specific queries and updated from the command side, often asynchronously.

?? Why must a projector store a checkpoint, and where?
So that after a crash or redeployment it resumes from the last applied event without skipping or double-applying. Ideally the checkpoint is saved in the same transaction as the read model update.

?? Is CQRS only possible with event sourcing?
No. Any write model can feed read models, for example through CDC or synchronous updates. Event sourcing, however, almost always needs CQRS because querying raw event streams is impractical.
:::

::: resources
- Article: [CQRS](https://martinfowler.com/bliki/CQRS.html) · Martin Fowler's description, including the warning about where not to use it
- Book: [Microservices Patterns](https://www.manning.com/books/microservices-patterns) · Chris Richardson; the chapters on CQRS views and event sourcing are practical and detailed
- Docs: [Pattern: CQRS (microservices.io)](https://microservices.io/patterns/data/cqrs.html) · the pattern summary with related patterns
- Article: [CQRS Documents by Greg Young](https://cqrs.files.wordpress.com/2010/11/cqrs_documents.pdf) · the original long-form explanation of CQRS and event sourcing
- Docs: [BigQuery materialized views](https://cloud.google.com/bigquery/docs/materialized-views-intro) · read models maintained by the warehouse itself
- Book: [Implementing Domain-Driven Design](https://www.oreilly.com/library/view/implementing-domain-driven-design/9780133039900/) · Vaughn Vernon; CQRS and event sourcing in the context of aggregates
:::

### Transactional Outbox Pattern

::: plain
Imagine a shop clerk who must both record a sale in the ledger and post a letter to the warehouse. If they post the letter and then drop the ledger, or write the ledger and forget the letter, the shop and the warehouse disagree. The outbox fix: the clerk writes the letter *into the ledger itself*, on the same page as the sale, in one go. A separate messenger regularly reads the ledger and posts any letters not yet sent. The sale and the letter can no longer be separated.
:::

**The dual-write problem.** A service handling "place order" must save the order in its database *and* publish an `OrderPlaced` event to Kafka or Pub/Sub. These are two different systems, and there is no transaction spanning both:

- Save, then publish: if the process crashes, or the broker is down, after the save, the event is never published. Downstream systems never learn about the order.
- Publish, then save: if the save fails, the world hears about an order that does not exist.
- Publish inside the database transaction: if the publish succeeds but the commit then fails, same problem; and a slow broker now holds database locks.

Distributed transactions (two-phase commit) across a database and a broker are rarely available and costly where they are (the **Distributed systems** section explains why).

**The pattern.** Add an **outbox** table to the service's own database. In the *same local transaction* that changes business data, insert a row describing the event into the outbox. Either both rows commit or neither does. A separate **message relay** then reads committed outbox rows and publishes them to the broker, and marks or deletes them once the broker acknowledges.

[[fig:events_outbox]]

```python
import json, sqlite3, uuid

db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE orders (order_id TEXT PRIMARY KEY, customer TEXT, total INTEGER);
CREATE TABLE outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,   -- relay order
    event_id TEXT UNIQUE, aggregate_id TEXT, type TEXT, payload TEXT,
    published INTEGER DEFAULT 0);
""")

def place_order(order_id, customer, total):
    with db:   # ONE local transaction: both rows or neither
        db.execute("INSERT INTO orders VALUES (?, ?, ?)", (order_id, customer, total))
        db.execute("INSERT INTO outbox (event_id, aggregate_id, type, payload) VALUES (?, ?, ?, ?)",
                   (str(uuid.uuid4()), order_id, "OrderPlaced",
                    json.dumps({"order_id": order_id, "total": total})))

broker = []                      # stands in for Kafka or Pub/Sub

def relay(crash_before_mark=False):
    rows = db.execute("SELECT seq, event_id, aggregate_id, type, payload FROM outbox "
                      "WHERE published = 0 ORDER BY seq").fetchall()
    for seq, event_id, key, etype, payload in rows:
        broker.append({"id": event_id, "key": key, "type": etype, "data": json.loads(payload)})
        if crash_before_mark:
            return               # published, but not marked: will be sent again
        with db:
            db.execute("UPDATE outbox SET published = 1 WHERE seq = ?", (seq,))

place_order("o1", "asha", 40)
relay(crash_before_mark=True)    # relay dies after publishing
relay()                          # restarted relay publishes o1 again
print(len(broker), "messages published")   # 2 messages published

processed, totals = set(), {}    # consumer side: an inbox of processed event ids
for msg in broker:
    if msg["id"] in processed:
        continue                 # duplicate: skip
    processed.add(msg["id"])
    totals[msg["key"]] = msg["data"]["total"]
print(totals)                    # {'o1': 40}
```

The example shows the property you must design for: the outbox gives **at-least-once** publishing. A relay can crash after publishing and before recording that it did, so the same event can be published twice. Consumers must therefore be **idempotent**: they record processed event ids (an **inbox** or processed-messages table, updated in the same transaction as their own changes) or make their writes naturally idempotent (upsert by key). Outbox plus idempotent consumer is how "exactly once" is achieved in practice: effectively-once processing built from at-least-once delivery.

**Two ways to build the relay.**

| Relay | How it works | Pros | Cons |
|---|---|---|---|
| Polling publisher | A process queries `WHERE published = 0 ORDER BY seq` every few hundred milliseconds | Simple, any database | Polling load and latency; must handle concurrent relays (row locks or `SKIP LOCKED`) |
| Transaction log tailing (CDC) | A CDC tool reads the database's write-ahead log and publishes inserts into the outbox table | Low latency, no polling, preserves commit order | Needs CDC infrastructure and log access |

**Debezium's outbox event router.** Debezium (covered in depth in **Data ingestion**) has a ready-made outbox implementation. The service writes rows with columns such as `id`, `aggregatetype`, `aggregateid`, `type` and `payload`; the connector captures the inserts, and the `EventRouter` single message transform routes each one to a topic named after its aggregate type, uses the aggregate id as the message key (preserving per-aggregate order), and puts the payload in the message value:

```text
# Kafka Connect configuration fragment for a PostgreSQL source with the outbox router
connector.class=io.debezium.connector.postgresql.PostgresConnector
table.include.list=public.outbox
transforms=outbox
transforms.outbox.type=io.debezium.transforms.outbox.EventRouter
transforms.outbox.route.by.field=aggregatetype
transforms.outbox.route.topic.replacement=events.${routedByValue}
```

Because the relay only needs the insert, services often delete outbox rows right after inserting them in the same transaction; the insert is still in the log for CDC to read, and the table never grows.

**On Google Cloud.** For a service on Cloud SQL for PostgreSQL or AlloyDB, run Debezium in a Kafka Connect cluster attached to Google Managed Service for Apache Kafka, or use **Debezium Server**, which can publish directly to Pub/Sub. For Spanner, **change streams** play the role of log tailing: a Dataflow pipeline (there is a Google-provided template) reads the outbox table's change stream and publishes to Pub/Sub. If the goal is only to land events in BigQuery, Datastream can replicate the outbox table straight into BigQuery.

**Ordering.** Use the aggregate id as the Kafka key or Pub/Sub ordering key so events about one order arrive in order. A polling relay must publish in commit order per aggregate; parallel relays need care so two of them do not publish events of the same aggregate out of order.

> **Pitfall:** Publishing outbox events with no stable event id, so consumers cannot deduplicate. Generate the id once, when the outbox row is written, and keep it through every retry.

> **Tip:** Treat the outbox schema as a public contract. The `type` and `payload` columns become events other teams depend on, so give them the same review and versioning discipline as any API (see Event Schema Design & CloudEvents).

::: deeper
**The inbox pattern.** The consumer-side mirror of the outbox: an `inbox` table with a unique key on message id. The consumer inserts the id and applies its changes in one transaction; a duplicate insert fails and the message is skipped. This is how a service that both consumes and produces events (consume, change state, emit new events) stays effectively-once end to end.

**Outbox without a table.** PostgreSQL's `pg_logical_emit_message()` writes a message directly into the write-ahead log inside the current transaction, without storing a row. Debezium can capture these logical decoding messages, giving outbox semantics with no table to clean up.

**Exactly-once into Kafka.** Kafka Connect source connectors can run with exactly-once support (KIP-618), which makes the relay's publish and its offset commit atomic in Kafka. That removes duplicates caused by the relay itself, but not duplicates from the consumers' side, so idempotent consumers are still needed.

> **Interview:** "Your service writes to PostgreSQL and then publishes to Kafka. Occasionally events go missing. Why, and what is the fix?" A crash or broker error between the commit and the publish loses the event: a dual write. Fix it with a transactional outbox written in the same transaction, relayed by polling or CDC (Debezium's outbox router), and make consumers idempotent because the relay is at-least-once.
:::

::: check
?? Why is "save to the database, then publish to Kafka" unsafe?
They are separate systems with no shared transaction. A crash or broker failure after the commit loses the event, so the database and downstream consumers disagree permanently.

?? What delivery guarantee does the outbox relay give, and what must consumers do about it?
At-least-once: the relay may publish an event again after a crash. Consumers must be idempotent, for example by recording processed event ids in an inbox table or using upserts keyed on a business id.

?? Name two ways to implement the relay.
A polling publisher that queries unpublished outbox rows, or transaction log tailing with a CDC tool such as Debezium (or Spanner change streams) that publishes inserted outbox rows.
:::

::: resources
- Docs: [Pattern: Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) · the pattern, polling publisher and log tailing, by Chris Richardson
- Blog: [Reliable Microservices Data Exchange With the Outbox Pattern](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/) · Gunnar Morling's walk-through with Debezium
- Docs: [Debezium outbox event router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) · configuration of the EventRouter transform
- Docs: [Spanner change streams overview](https://cloud.google.com/spanner/docs/change-streams) · log tailing for Spanner tables
- Docs: [Pattern: Idempotent Consumer](https://microservices.io/patterns/communication-style/idempotent-consumer.html) · the consumer-side half of reliable messaging
- Book: [Microservices Patterns](https://www.manning.com/books/microservices-patterns) · the chapter on transactional messaging covers outbox and inbox in depth
:::

### Saga Pattern & Compensating Transactions

::: plain
Booking a holiday means reserving a flight, a hotel and a car from three different companies. There is no way to book all three in one indivisible step, so you book them one at a time, and if the car is unavailable you cancel the hotel and the flight you already booked. A **saga** is that procedure, written down for software: a sequence of separate steps, each with a matching "undo" step (a **compensating transaction**) to run if a later step fails.
:::

A **saga** (Garcia-Molina and Salem, 1987) is a sequence of local transactions T1, T2, ..., Tn, each committed in its own service, where every step that might need undoing has a **compensating transaction** C1, C2, ... If Tk fails, the saga runs C(k-1), ..., C1 in reverse order. The end state is either "all steps done" or "all completed steps compensated". No locks are held across services, so nobody waits on anybody, but the intermediate states are visible to others: a saga gives you atomicity eventually, not isolation.

```python
def run_saga(steps):
    """steps: list of (name, action, compensation). Undo completed steps in reverse on failure."""
    done = []
    for name, action, compensate in steps:
        try:
            action()
            print("done:", name)
            done.append((name, compensate))
        except Exception as err:
            print(f"failed: {name} ({err})")
            for prev, undo in reversed(done):
                undo()
                print("compensated:", prev)
            return "rolled back"
    return "completed"

def fail(msg):
    raise RuntimeError(msg)

result = run_saga([
    ("reserve stock",  lambda: None, lambda: None),     # compensation: release stock
    ("charge card",    lambda: None, lambda: None),     # compensation: refund
    ("book courier",   lambda: fail("no courier available"), lambda: None),
])
print(result)
# done: reserve stock
# done: charge card
# failed: book courier (no courier available)
# compensated: charge card
# compensated: reserve stock
# rolled back
```

A real saga cannot keep its progress in memory like this: the coordinating process may crash halfway. Its state (which steps are done, which compensations are pending) must be stored durably, which is exactly what the tools below provide.

**Two ways to coordinate.**

- **Choreography:** no central coordinator. Each service listens for events and reacts: Orders publishes `OrderCreated`; Stock reserves items and publishes `StockReserved`; Payments charges and publishes `PaymentCaptured` or `PaymentFailed`; on `PaymentFailed`, Stock releases the reservation and Orders marks the order failed.
- **Orchestration:** a central **orchestrator** (a workflow) tells each service what to do, waits for the answer, and decides the next step or the compensations. The flow lives in one place as an explicit state machine.

[[fig:events_saga]]

| | Choreography | Orchestration |
|---|---|---|
| Coupling | Services know each other's events | Services know only the orchestrator's commands |
| Where the flow lives | Spread across services | In one workflow definition |
| Good for | Short flows with 2 to 4 steps | Long or branching flows, timeouts, human steps |
| Risks | Hard to see and change the whole flow; cyclic dependencies | The orchestrator becomes a bottleneck of logic if it grows business rules |
| Observability | Needs tracing across events | Workflow history shows every step |

**Designing compensations.** A compensation is a *semantic* undo, not a rollback: you cannot un-send an email, but you can send a correction; you do not delete a charge, you issue a refund. Rules:

- Compensations must be **idempotent** and **retryable**, because they too can fail and be retried. They must eventually succeed; if a compensation can fail permanently, the saga needs a human escalation path.
- Order the steps so that the ones hardest to undo come last. Richardson calls the step after which the saga cannot fail any more the **pivot transaction**: steps before it are **compensatable**, steps after it are **retriable** (they will be retried until they succeed rather than compensated).
- Record each step's outcome with ids (reservation id, payment id) so the compensation knows exactly what to undo.

**Dealing with the lack of isolation.** While a saga runs, others can see its intermediate states: an order "pending", stock "reserved". Countermeasures include **semantic locks** (a status field such as `APPROVAL_PENDING` that other operations respect), **commutative updates** (designing operations so their order does not matter, like debit and credit), **re-reading values** before a step to detect changes, and **versioning** records.

**Tools.**

- **Temporal** (and Cadence, its predecessor): durable execution. You write the saga as ordinary code (Go, Java, Python, TypeScript and others); Temporal persists every step's result in an event history, so if the worker crashes the workflow resumes exactly where it was. Compensation is a list of undo functions appended as steps succeed, executed in a `finally`-style block on failure.
- **Workflows** on Google Cloud: a serverless orchestrator with YAML or JSON definitions, built-in retries, `try`/`except`, callbacks and long waits (executions can run for up to a year). It calls Cloud Run services and Google APIs directly.
- **Axon Framework** (Java): a CQRS and event sourcing framework with saga support, where a saga class reacts to events (annotated handlers that start and end the saga) and dispatches commands.
- Managed Service for Apache Airflow (formerly Cloud Composer) is built for data pipelines, not for per-order business sagas: its scheduler is designed for thousands of DAG runs, not millions of short-lived transactions.

A saga step with a compensation in Workflows:

```yaml
main:
  params: [order]
  steps:
    - reserve_stock:
        call: http.post
        args:
          url: https://stock-service-abc123-ew.a.run.app/reserve
          body: {order_id: ${order.id}, items: ${order.items}}
          auth: {type: OIDC}
        result: reservation
    - charge_card:
        try:
          call: http.post
          args:
            url: https://payments-service-abc123-ew.a.run.app/charge
            body: {order_id: ${order.id}, amount_cents: ${order.total_cents}}
            auth: {type: OIDC}
          result: payment
        except:
          as: e
          steps:
            - release_stock:            # compensation for reserve_stock
                call: http.post
                args:
                  url: https://stock-service-abc123-ew.a.run.app/release
                  body: {reservation_id: ${reservation.body.reservation_id}}
                  auth: {type: OIDC}
            - give_up:
                raise: ${e}
    - done:
        return: ${payment.body}
```

> **Pitfall:** Forgetting that the *request* to a step may have succeeded even though the saga saw a timeout. If you compensate a step you are not sure happened, or retry one that did, you can refund money that was never charged or charge twice. Every step and compensation needs an idempotency key (the saga id plus the step name works well).

> **Tip:** Data engineers often inherit the side effects of sagas: tables full of `PENDING`, `COMPENSATED` and `FAILED` states. Model them explicitly in the warehouse (a status history or an accumulating snapshot fact table) rather than filtering to "completed" only, or revenue and stock reports will not reconcile.

::: deeper
**Saga state machine.** An orchestrated saga is a finite state machine: states such as `STARTED`, `STOCK_RESERVED`, `PAID`, `COMPENSATING`, `COMPLETED`, `FAILED`, with transitions triggered by replies or timeouts. Persist it (Temporal does this for you; with a homegrown orchestrator, store it in a table and drive it from events through an outbox). Each transition should be idempotent so a redelivered reply does not advance the saga twice.

**Timeouts are steps too.** A payment provider that never answers should not leave a saga pending forever. Orchestrators make timeouts explicit (Temporal timers, Workflows `sys.sleep` or callback timeouts), and the timeout path usually triggers compensation or a manual review queue.

**Sagas versus 2PC.** Two-phase commit gives isolation and immediate atomicity but blocks when the coordinator fails and requires every participant to support it. Sagas never block and work across any services and external APIs, at the cost of visible intermediate states and the work of writing compensations. The **Distributed systems** section covers 2PC in detail.

> **Interview:** "Design checkout across Orders, Stock and Payments without distributed transactions." Describe an orchestrated saga: create order (pending), reserve stock, charge payment (the pivot), confirm order and request shipping (retriable). On payment failure, release stock and mark the order failed. Mention durable saga state (Temporal or Workflows), idempotency keys per step, the outbox for events, and semantic locks via the pending status.
:::

::: check
?? What is a compensating transaction, and how does it differ from a database rollback?
A separate, forward transaction that semantically undoes an earlier committed step (a refund, a release of a reservation). A rollback discards uncommitted changes; a compensation runs after the original change was committed and visible.

?? When would you choose orchestration over choreography?
For flows with many steps, branches, timeouts or human approvals, where having the whole process in one explicit, observable definition matters more than avoiding a central coordinator.

?? What is the pivot transaction in a saga?
The step after which the saga can no longer fail. Steps before it must be compensatable; steps after it are designed to be retried until they succeed.
:::

::: resources
- Docs: [Pattern: Saga](https://microservices.io/patterns/data/saga.html) · choreography and orchestration explained by Chris Richardson
- Paper: [Sagas](https://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) · Garcia-Molina and Salem's 1987 paper that introduced the idea
- Blog: [Saga pattern made easy](https://temporal.io/blog/saga-pattern-made-easy) · implementing sagas with compensation in Temporal
- Docs: [Workflows overview](https://cloud.google.com/workflows/docs/overview) · Google Cloud's serverless orchestrator, with retries and error handling
- Book: [Microservices Patterns](https://www.manning.com/books/microservices-patterns) · chapter 4 covers sagas, pivot transactions and isolation countermeasures
- Talk: [Distributed Sagas: A Protocol for Coordinating Microservices](https://www.youtube.com/watch?v=0UTOLRTwOX0) · Caitie McCaffrey's influential talk on sagas in practice
:::

### Microservice Data Patterns

::: plain
Picture a company where every department keeps its own filing cabinet and nobody may open anyone else's: if Sales needs a figure from Finance, it asks Finance, who answers in an agreed format. This is slower than everyone rummaging through one shared cabinet, but each department can reorganise its own files without breaking anyone else's work. Microservice data patterns are the rules for that arrangement: who owns which data, how others get it, and how to keep things consistent when no single cabinet holds everything.
:::

**Database per service.** The founding rule of microservices is that each service owns its data and exposes it only through its interface (an API or published events). Other services must not read or write its tables directly. The reasons are about change: if three services query the orders table, the orders team cannot rename a column without coordinating with all three, and the services are no longer independently deployable. "Database per service" does not require a separate server per service; separate schemas with separate credentials on a shared Cloud SQL instance enforce the same boundary.

**Shared nothing versus shared database.**

| | Shared database | Database per service (shared nothing) |
|---|---|---|
| Joins across domains | Easy, in SQL | Not possible directly; needs API composition or replicated data |
| Transactions across domains | Local ACID | Sagas and eventual consistency |
| Independent schema changes | No | Yes, behind a stable contract |
| Scaling and technology choice | One engine for all | Each service picks its store |
| Typical failure | Accidental coupling, "distributed monolith" | Inconsistency between services, data duplication |

**Getting data that another service owns.** Patterns, from simplest to most decoupled:

1. **API composition:** call the owning services and combine the results in the caller or a gateway. Simple, but latency and availability depend on every service.
2. **Event-carried state transfer:** services publish events with enough data (`CustomerAddressChanged` with the new address) for others to keep a local copy of what they need. Reads are local and fast; the copy is eventually consistent.
3. **CQRS read models:** a dedicated query service builds views across domains from everyone's events.
4. **CDC to an analytical store:** for analytics, replicate each service's database or outbox into BigQuery (with Datastream, or Debezium through Kafka) and join there.

**Anti-corruption layer (ACL).** A term from domain-driven design: when your service must talk to a system whose model you do not control (a legacy CRM, a vendor API, another team's events), put a translation layer at the boundary that converts their concepts into yours. Your domain model then does not fill up with their field names, codes and quirks, and when they change, only the ACL changes.

```python
def customer_from_crm(rec):
    """Anti-corruption layer: translate a legacy CRM record into our Customer model."""
    status = {"A": "active", "S": "suspended", "X": "closed"}[rec["CUST_STAT_CD"]]
    return {
        "customer_id": f"crm-{rec['CUSTNO']}",
        "name": rec["NM_LAST"].title() + ", " + rec["NM_FIRST"].title(),
        "status": status,
    }

legacy = {"CUSTNO": 4711, "NM_FIRST": "ASHA", "NM_LAST": "RAO", "CUST_STAT_CD": "A"}
print(customer_from_crm(legacy))
# {'customer_id': 'crm-4711', 'name': 'Rao, Asha', 'status': 'active'}
```

Data engineers build ACLs constantly, under another name: the staging layer of a warehouse, which renames, retypes and decodes source columns once, so every downstream model speaks the business's language rather than each source system's.

**Consistency across services.** Without shared transactions, consistency comes from patterns covered earlier in this group: the **outbox** for reliable publishing, **idempotent consumers** for duplicate delivery, **sagas** for multi-step processes, and **reconciliation** jobs that periodically compare systems (for example, every captured payment has a confirmed order) and raise discrepancies. Accept eventual consistency where the business can (most can, for seconds or minutes), and keep strongly consistent operations inside one service's boundary.

**Choreography versus orchestration** applies beyond sagas: for any cross-service flow, decide whether services react to each other's events (loose, but the overall flow is implicit) or a coordinator drives them (explicit, but central). Many systems use events between domains and orchestration within a domain.

**Service mesh and data flow.** A service mesh (Istio, Cloud Service Mesh) manages *service-to-service calls*: mutual TLS, retries, timeouts, traffic splitting and telemetry, handled by proxies beside each service instead of in application code. It does not move or store data and does not replace an event backbone, but it secures and observes the synchronous calls (API composition, gRPC) in a microservice data architecture.

**Breaking up a monolith.** Most microservice systems start as a monolith with one big database. Martin Fowler's and Zhamak Dehghani's guidance on decomposition is to extract capabilities one at a time, starting with loosely coupled ones, and to **move the data with the capability**, so the new service owns its tables instead of reaching back into the monolith's database. The **strangler fig** approach routes traffic for one capability at a time to the new service until the old code can be removed.

> **Pitfall:** The "distributed monolith": services deployed separately but sharing a database or calling each other synchronously in long chains. You pay the costs of distribution (network failures, eventual consistency) without the benefit of independent change. A good test: can each service be deployed and its schema changed without coordinating with other teams?

> **Tip:** When a product team splits a service, ask how the data contract to analytics will be kept. A CDC feed of a table that is about to be split or renamed breaks pipelines silently; a published, versioned event or data product does not.

::: deeper
**Data duplication is deliberate.** In a microservice system, the same customer address may live in five services. That is not a modelling error: each copy serves a local need, and one service is the **system of record**. Document which one, so analytics and data quality checks know which source wins.

**Bounded contexts and language.** In domain-driven design, a **bounded context** is the boundary within which a model and its words have one meaning: "customer" in Billing (a payer with an invoice address) is not "customer" in Marketing (a contact with consent flags). Service and data boundaries work best when they follow bounded contexts, which is why Event Storming (later in this section) is a popular way to find them.

**Shared reference data.** Currency codes, country lists and product categories are needed everywhere. Options: a reference data service with heavy caching, or reference events that every service keeps a copy of. Avoid a shared table that every service writes.

> **Interview:** "Why should a service not read another service's database directly, even read-only?" Because it couples the reader to the owner's internal schema: the owner can no longer change tables without breaking others, and the reader bypasses the owner's rules and access controls. Use the owner's API, its events, or a published data product instead.
:::

::: check
?? Does "database per service" require a separate database server for each service?
No. It requires that each service's data is private to it. Separate schemas or databases with separate credentials on a shared instance enforce the same boundary.

?? What does an anti-corruption layer do?
It translates between an external or legacy model and your own domain model at the boundary, so foreign concepts and quirks do not leak into your code and changes on their side are absorbed in one place.

?? Name two ways for a service to obtain data owned by another service without querying its database.
API composition (call the owner's API) and event-carried state transfer (keep a local copy updated from the owner's events). CQRS read models and CDC into an analytical store are also valid answers.
:::

::: resources
- Docs: [Pattern: Database per service](https://microservices.io/patterns/data/database-per-service.html) · the rule, its benefits and drawbacks, and related patterns
- Article: [How to break a Monolith into Microservices](https://martinfowler.com/articles/break-monolith-into-microservices.html) · Zhamak Dehghani on decoupling capabilities and their data
- Book: [Monolith to Microservices](https://www.oreilly.com/library/view/monolith-to-microservices/9781492047834/) · Sam Newman's patterns for splitting databases and migrating incrementally
- Book: [Microservices Patterns](https://www.manning.com/books/microservices-patterns) · Chris Richardson's catalogue of data management patterns
- Article: [What do you mean by "Event-Driven"?](https://martinfowler.com/articles/201701-event-driven.html) · Fowler distinguishes event notification, event-carried state transfer, event sourcing and CQRS
- Docs: [Cloud Service Mesh overview](https://cloud.google.com/service-mesh/docs/overview) · mTLS, traffic management and telemetry for service-to-service calls
:::

## Designing events

The patterns in the first group decide *when* events flow and *why*. This group is about the events themselves: what goes inside one, how it is wrapped so any consumer can route and deduplicate it, how its shape changes over the years without breaking anyone, and how people find out which events exist. An event that leaves a service is a public interface, exactly like an API, and deserves the same design care.

The group moves from the single message (schema design and the CloudEvents envelope) to documentation and discovery (AsyncAPI and EventCatalog), then to the workshop that finds the right events in the first place (Event Storming), and ends with the synchronous counterpart that every data product also needs: a well-designed API.

### Event Schema Design & CloudEvents

::: plain
A parcel has two parts: the label on the outside (who sent it, where it goes, a tracking number, when it was posted) and the contents inside. Couriers only read the label; only the receiver opens the box. A well-designed event works the same way: a standard **envelope** with an id, a source, a type and a time that any tool can read, and a **payload** with the business data that only interested consumers care about. CloudEvents is an agreed label format, so every courier in the world can read it.
:::

**What goes into an event.** Before choosing formats, decide what kind of event you are publishing. Martin Fowler names the common styles:

| Style | Payload | Consumer must | Example |
|---|---|---|---|
| Event notification | Just ids ("something changed") | Call back to the producer for details | `OrderShipped {order_id}` |
| Event-carried state transfer | The relevant state after the change | Nothing more; it can keep a local copy | `CustomerAddressChanged {customer_id, address}` |
| Domain event (fact) | What happened, with the business details of that moment | Interpret the fact | `PaymentCaptured {payment_id, order_id, amount_cents, currency, captured_at}` |

For data pipelines, thin notifications are the worst choice: a pipeline replaying a month of events cannot call back to ask what an order looked like three weeks ago. Prefer facts that carry the values that were true **at the time of the event**.

**Rules of thumb for payloads:**

- Name events in the past tense, after a business occurrence: `OrderPlaced`, not `OrderUpdate` or `CreateOrder` (that is a command).
- Include stable business keys (`order_id`, `customer_id`) and the **event time** (when it happened), separate from the time it was published.
- Money as integer minor units plus a currency (`4250`, `"EUR"`), never floats. Timestamps in UTC as RFC 3339 strings or a timestamp logical type.
- Enumerations will grow: document that consumers must handle unknown values.
- Avoid nullable fields with two meanings ("not known" and "not applicable"); use explicit fields instead.
- Keep personal data to the minimum the consumers need, and tag it in the schema so governance tools can find it.

**The envelope: CloudEvents.** **CloudEvents** is a CNCF specification (a graduated project since January 2024) that standardises the metadata around an event, not the payload. Its context attributes:

| Attribute | Required | Meaning |
|---|---|---|
| `specversion` | Yes | The CloudEvents version, `"1.0"` |
| `id` | Yes | Identifies the event; `source` plus `id` must be unique per distinct event |
| `source` | Yes | A URI reference for the context that produced it, such as `/sales/orders-service` |
| `type` | Yes | The kind of event, prefixed with a reverse-DNS name: `com.example.orders.order.placed.v1` |
| `time` | No | When the occurrence happened (RFC 3339) |
| `subject` | No | The thing it is about within the source, such as the order id |
| `datacontenttype` | No | Media type of `data`, such as `application/json` or `application/avro` |
| `dataschema` | No | URI of the schema that `data` follows |

You may add **extension attributes** (lowercase letters and digits only), and the spec defines some, such as `traceparent` for distributed tracing and `partitionkey` for brokers that partition. Because `source` plus `id` identifies an event, consumers may treat two events with the same pair as duplicates, which is exactly what the idempotent consumers of the outbox topic need.

**Content modes.** A CloudEvent can travel in **structured mode** (the whole event, attributes and data, as one JSON document with content type `application/cloudevents+json`) or **binary mode** (the data is the message body unchanged, and the attributes go into protocol headers: `ce-id`, `ce-type` in HTTP, `ce_id`, `ce_type` in Kafka). Binary mode keeps the payload a pure Avro or Protobuf record, so existing serializers and schema registries keep working, and routers can read headers without decoding the body. Google Cloud's **Eventarc** delivers events to Cloud Run in CloudEvents format, so the envelope is also what you receive from Google services.

```python
import json, re

REQUIRED = ("specversion", "id", "source", "type")

def make_event(etype, source, event_id, time, data, subject=None):
    event = {"specversion": "1.0", "id": event_id, "source": source, "type": etype,
             "time": time, "datacontenttype": "application/json", "data": data}
    if subject:
        event["subject"] = subject
    return event

def validate(event):
    problems = [f"missing {a}" for a in REQUIRED if a not in event]
    problems += [f"bad attribute name {k!r}" for k in event
                 if k != "data" and not re.fullmatch(r"[a-z0-9]+", k)]
    return problems

def to_kafka_binary(event, key):
    """Binary content mode: attributes become ce_ headers, data becomes the value."""
    headers = [("ce_" + k, str(v)) for k, v in event.items()
               if k not in ("data", "datacontenttype")]
    headers.append(("content-type", event["datacontenttype"]))
    return key, headers, json.dumps(event["data"])

evt = make_event("com.example.orders.order.placed.v1", "/sales/orders-service",
                 "b7e1c2d4-0001", "2026-11-23T09:15:00Z",
                 {"order_id": "o-1001", "total_cents": 4250, "currency": "EUR"},
                 subject="o-1001")
print(validate(evt))                       # []
print(validate({"id": "1", "orderId": 5}))
# ['missing specversion', 'missing source', 'missing type', "bad attribute name 'orderId'"]

key, headers, value = to_kafka_binary(evt, key=evt["subject"])
print([name for name, _ in headers])
# ['ce_specversion', 'ce_id', 'ce_source', 'ce_type', 'ce_time', 'ce_subject', 'content-type']
print(value)          # {"order_id": "o-1001", "total_cents": 4250, "currency": "EUR"}
```

In production you would use the CloudEvents SDK for your language (Python, Java, Go and others), which implements the bindings exactly; the sketch shows what they do.

**Versioning strategy.** The **Streaming systems** section covers schema registries and compatibility modes (`BACKWARD`, `FORWARD`, `FULL`, transitive). On top of those mechanics you need a *policy*:

1. **Compatible changes stay in place.** Adding an optional field with a default, or adding an enum value consumers were told to tolerate, is a new schema version under the same event type and topic. The registry's compatibility check enforces it.
2. **Breaking changes get a new major version.** Changing a field's meaning or type, removing a required field, or splitting an event: publish `order.placed.v2` (new `type`, usually a new topic), run v1 and v2 side by side, migrate consumers, then retire v1 on an announced date.
3. **Semantic versioning for event schemas.** A major version lives in the type name and topic (`v1`, `v2`); minor versions (compatible additions) and patches (documentation, descriptions) are recorded in the registry and the event catalog. Consumers depend on the major version only.

> **Pitfall:** Changing the *meaning* of a field without changing its type: `amount` silently switching from gross to net. No registry can catch it, and downstream revenue numbers drift for weeks before anyone notices. A change of meaning is a breaking change and needs a new field or a new major version.

> **Tip:** Put the major version in the CloudEvents `type` and the topic name, and keep `dataschema` pointing at the exact registry version. A consumer (or a data engineer debugging a broken load) can then tell from any single message which contract it follows.

::: deeper
**Several event types in one topic.** Ordering is guaranteed only within a partition, so events about one aggregate (`OrderPlaced`, `OrderShipped`, `OrderCancelled`) often must share a topic to stay in order. With a schema registry, use `TopicRecordNameStrategy` (one subject per record type in the topic) or a union schema with schema references, as Martin Kleppmann's article on the Confluent blog explains. The rule: put events in the same topic when consumers need their relative order; otherwise separate topics are simpler to secure and evolve.

**Envelope inside the payload, or headers?** Some teams wrap every payload in their own envelope record (`{metadata: {...}, payload: {...}}`). It survives systems that drop headers (some connectors, some file exports), but forces every consumer to unwrap and duplicates what CloudEvents binary mode gives for free. If you land events in BigQuery, keep the envelope attributes as top-level columns (`event_id`, `event_type`, `event_time`, `source`) next to the payload, so deduplication and partitioning work without parsing.

**Pub/Sub specifics.** Pub/Sub topics can enforce an Avro or Protocol Buffers schema with revisions, and messages carry key-value **attributes** that play the role of headers, so a binary-mode style mapping of the CloudEvents attributes into attributes works naturally. A Pub/Sub BigQuery subscription can write the payload and, optionally, the attributes and publish metadata to columns.

> **Interview:** "A producer wants to rename `customer` to `customer_id` in a busy event. How do you roll it out?" Add `customer_id` as a new optional field and populate both (a compatible change), migrate consumers to the new field, then remove the old one in the next major version, announced through the catalog with a deprecation date. A straight rename is a breaking change for every consumer.
:::

::: check
?? Which four CloudEvents attributes are required, and which pair identifies a distinct event?
`specversion`, `id`, `source` and `type`. The pair `source` plus `id` must be unique for each distinct event, so consumers can treat repeats as duplicates.

?? What is the difference between structured and binary content mode?
Structured mode sends the whole event (attributes and data) as one document, such as `application/cloudevents+json`. Binary mode sends the data as the message body unchanged and puts the attributes in protocol headers (`ce_` headers in Kafka, `ce-` headers in HTTP).

?? Why are thin "event notification" events a poor fit for data pipelines?
They carry only ids, so the consumer must call back for details. A pipeline replaying old events cannot recover what the entity looked like at the time, and the callbacks load the producer.
:::

::: resources
- Spec: [CloudEvents specification](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md) · the context attributes, extensions and type naming rules
- Spec: [CloudEvents Kafka protocol binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md) · binary and structured mode on Kafka, header names and key mapping
- Docs: [CloudEvents](https://cloudevents.io/) · the project home, with SDKs for Python, Java, Go and more
- Article: [What do you mean by "Event-Driven"?](https://martinfowler.com/articles/201701-event-driven.html) · notification, event-carried state transfer and event sourcing compared
- Blog: [Should You Put Several Event Types in the Same Kafka Topic?](https://www.confluent.io/blog/put-several-event-types-kafka-topic/) · Martin Kleppmann on ordering, subjects and schema strategies
- Docs: [Eventarc overview](https://cloud.google.com/eventarc/docs/overview) · how Google Cloud routes events in CloudEvents format
- Book: [Designing Event-Driven Systems](https://www.confluent.io/resources/ebook/designing-event-driven-systems/) · Ben Stopford's free book on events as shared contracts
:::

### AsyncAPI & Event Catalog

::: plain
A city's bus network is useless without two things: a timetable for each line (where it goes, when, what stops it makes) and a map showing all the lines together so you can find the one you need. **AsyncAPI** is the timetable for one service's events: a precise, machine-readable description of what it publishes and consumes. An **event catalog** is the network map: a website where anyone in the company can browse every event, who produces it, who consumes it and what it looks like.
:::

**The documentation problem.** REST APIs have had OpenAPI for years: one file describes every endpoint, and tools generate docs, clients and tests from it. Event-driven systems usually have nothing equivalent, so knowledge of "which topic has the shipped events, in which format, owned by whom" lives in people's heads and stale wiki pages. Data engineers feel this most: they are the consumers of events from many teams.

**AsyncAPI** is an open specification (a Linux Foundation project) for describing message-driven APIs, deliberately similar to OpenAPI. Version 3.0 (released December 2023) separates four ideas:

- **servers**: the brokers (Kafka, AMQP, MQTT, WebSockets, Google Pub/Sub and others through protocol **bindings**),
- **channels**: where messages flow (a topic), with an `address` and the messages it carries,
- **operations**: what *this* application does on a channel, `action: send` or `action: receive`,
- **components**: reusable messages and schemas. Payloads default to AsyncAPI Schema (a superset of JSON Schema), and can also be Avro or Protobuf through `schemaFormat`.

```yaml
asyncapi: 3.0.0
info:
  title: Orders service events
  version: 1.3.0
  description: Domain events published by the Orders service, owned by orders-team.
servers:
  production:
    host: bootstrap.orders-kafka.example.internal:9092
    protocol: kafka
channels:
  orderPlaced:
    address: orders.order-placed.v1
    messages:
      OrderPlaced:
        $ref: '#/components/messages/OrderPlaced'
operations:
  publishOrderPlaced:
    action: send
    channel:
      $ref: '#/channels/orderPlaced'
    messages:
      - $ref: '#/channels/orderPlaced/messages/OrderPlaced'
components:
  messages:
    OrderPlaced:
      name: OrderPlaced
      contentType: application/json
      payload:
        type: object
        required: [order_id, customer_id, total_cents, currency]
        properties:
          order_id: {type: string}
          customer_id: {type: string}
          total_cents: {type: integer, minimum: 0}
          currency: {type: string, description: ISO 4217 code}
          channel: {type: string, description: Added in 1.3.0}
```

The AsyncAPI CLI turns the file into checks and artefacts:

```bash
npm install -g @asyncapi/cli
asyncapi validate asyncapi.yaml                       # is the document valid?
asyncapi diff old/asyncapi.yaml asyncapi.yaml         # breaking vs non-breaking changes
asyncapi generate fromTemplate asyncapi.yaml @asyncapi/html-template -o docs/
```

Other tools generate code models (Modelina) and let you edit documents in the browser (AsyncAPI Studio).

**Event catalogs.** One AsyncAPI file per service describes that service. To see the whole landscape you need a catalog that aggregates them. **EventCatalog** is an open-source, documentation-as-code tool (with commercial plans for some features): a Git repository of Markdown files for **domains**, **services**, **events**, **commands**, **queries** and **channels**, built into a static website with visualisations of who sends and receives what.

```bash
npx @eventcatalog/create-eventcatalog@latest company-catalog
```

Each event is a folder with an `index.mdx` file whose front matter carries the metadata, and services declare what they send and receive:

```yaml
# events/OrderPlaced/index.mdx (front matter)
---
id: OrderPlaced
name: Order placed
version: 1.3.0
summary: Emitted when an order is accepted. Money in minor units.
owners:
  - orders-team
---
```

```yaml
# services/OrdersService/index.mdx (front matter)
---
id: OrdersService
name: Orders service
version: 2.4.0
sends:
  - id: OrderPlaced
    version: 1.3.0
receives:
  - id: PaymentCaptured
---
```

Writing these by hand does not scale, so EventCatalog has **generators** that read AsyncAPI and OpenAPI files (and, with some plans, schema registries) and create or update the pages, keeping older versions browsable. A catalog page then shows the schema, the producers, the consumers and the version history in one place.

**A versioning workflow that holds together.** The registry, AsyncAPI and the catalog each know part of the truth. Wire them into one pull-request flow:

1. A producer changes the schema and the AsyncAPI file in the same pull request.
2. CI runs `asyncapi validate`, `asyncapi diff` and the schema registry's compatibility check against the latest registered version.
3. A compatible change bumps the minor version; a breaking change is rejected unless it introduces a new major version (new type and topic).
4. On merge, the schema is registered, and the catalog is regenerated and published.
5. The catalog's consumer list tells the producer exactly whom to notify about deprecations.

The version classification in step 3 can be automated. A simplified version for JSON Schema payloads:

```python
def classify(old, new):
    """Compare two JSON Schema object definitions and say which version part to bump."""
    o, n = old["properties"], new["properties"]
    breaking = [f"removed {f}" for f in sorted(o) if f not in n]
    breaking += [f"{f}: {o[f]['type']} -> {n[f]['type']}"
                 for f in sorted(o) if f in n and n[f]["type"] != o[f]["type"]]
    breaking += [f"{f} is now required"
                 for f in sorted(set(new.get("required", [])) - set(old.get("required", [])))]
    added = sorted(set(n) - set(o))
    if breaking:
        return "major", breaking
    return ("minor", added) if added else ("patch", [])

def bump(version, part):
    major, minor, patch = map(int, version.split("."))
    return {"major": f"{major + 1}.0.0", "minor": f"{major}.{minor + 1}.0",
            "patch": f"{major}.{minor}.{patch + 1}"}[part]

v1 = {"required": ["order_id", "total_cents"],
      "properties": {"order_id": {"type": "string"}, "total_cents": {"type": "integer"},
                     "currency": {"type": "string"}}}
add_channel = {"required": ["order_id", "total_cents"],
               "properties": {**v1["properties"], "channel": {"type": "string"}}}
retype = {"required": ["order_id", "total_cents", "currency"],
          "properties": {**v1["properties"], "total_cents": {"type": "string"}}}

for change in (add_channel, retype):
    part, reasons = classify(v1, change)
    print(bump("1.2.0", part), part, reasons)
# 1.3.0 minor ['channel']
# 2.0.0 major ['total_cents: integer -> string', 'currency is now required']
```

> **Pitfall:** Documentation written once and never updated. A catalog that disagrees with production is worse than none, because people trust it. Generate the catalog from the same files that CI validates, so it cannot drift.

> **Tip:** Add the data platform as a consumer in AsyncAPI and the catalog (for example a `receives` entry on a "BigQuery landing" service). Producers then see the warehouse in their impact list before they make a breaking change.

::: deeper
**AsyncAPI 2 versus 3.** In 2.x, a channel held `publish` and `subscribe` operations whose meaning was famously confusing (described from the point of view of the *client*, not the application). Version 3.0 moved operations out of channels and made `send` and `receive` describe what the documented application does, decoupled channel names from addresses, and made request and reply patterns expressible. Converters exist (`asyncapi convert`), but check generated templates support 3.0 before migrating.

**Bindings carry protocol details.** Kafka bindings describe the message key schema, consumer group ids and topic configuration (partitions, retention); Google Pub/Sub bindings describe topic schema settings and message retention. Keep protocol details in bindings so the core document stays portable.

**Catalog versus registry.** A schema registry enforces structure at runtime; a catalog explains meaning, ownership and relationships for humans. You need both. In Google Cloud, **Knowledge Catalog** (formerly Dataplex Universal Catalog) is where the *tables* the events land in are cataloged, with lineage and quality; link each event's catalog page to its BigQuery tables and vice versa.

> **Interview:** "How would you let 30 teams discover and safely evolve the events they publish?" AsyncAPI per service in its repository, schemas in a registry with compatibility checks in CI, a generated event catalog showing producers, consumers and versions, and a rule that breaking changes require a new major version with a deprecation window.
:::

::: check
?? In AsyncAPI 3.0, what does `action: send` on an operation mean?
That the application described by the document sends (publishes) messages to that channel. `receive` means it consumes them.

?? What does an event catalog add on top of a schema registry?
A human-oriented view of the whole landscape: which domains and services produce and consume each event, its owners, descriptions and version history. The registry only stores and checks schemas.

?? Why generate the catalog from AsyncAPI files in CI rather than writing pages by hand?
So the documentation is validated and updated from the same source as the running system and cannot drift out of date.
:::

::: resources
- Docs: [AsyncAPI documentation](https://www.asyncapi.com/docs) · concepts, tutorials and tooling for event-driven APIs
- Spec: [AsyncAPI 3.0.0 specification](https://www.asyncapi.com/docs/reference/specification/v3.0.0) · servers, channels, operations, messages and bindings
- Docs: [AsyncAPI CLI](https://www.asyncapi.com/docs/tools/cli) · validate, diff, convert and generate from the command line
- Docs: [EventCatalog documentation](https://www.eventcatalog.dev/docs/development/getting-started/introduction) · domains, services, events, versioning and generators
- Repo: [EventCatalog on GitHub](https://github.com/event-catalog/eventcatalog) · the open-source project and examples
- Blog: [AsyncAPI 3.0.0 release notes](https://www.asyncapi.com/blog/release-notes-3.0.0) · what changed from 2.x and why
:::

### Event Storming for Data Systems

::: plain
Event Storming is a workshop where the people who know a business process (sales staff, warehouse managers, developers, data engineers) stand in front of a very long wall and tell the story of the business with orange sticky notes, one note per thing that happens: "order placed", "payment failed", "parcel shipped". Arranging the notes in time order quickly shows where people disagree, where data is missing and where one department's world ends and another's begins. It is like reconstructing a film by laying out its scenes.
:::

**What it is.** **Event Storming** is a workshop format invented by Alberto Brandolini for exploring complex domains quickly. Its central idea is that **domain events** (things that happened, written in the past tense) are a vocabulary that business people and engineers share. Instead of drawing boxes for systems or tables, the group maps what happens over time, then adds the causes and consequences. It works best with 6 to 15 people, a wall of paper several metres long (or a large online whiteboard), and an experienced facilitator.

**Three formats, three depths.**

| Format | Question | Output |
|---|---|---|
| Big Picture | How does the whole business flow work, end to end? | Timeline of events, hot spots, pivotal events, candidate bounded contexts |
| Process Modelling | How does one process work, step by step? | Commands, policies, actors, read models for that process |
| Software Design | What do we build? | Aggregates, their commands and events, service boundaries |

**The sticky-note grammar.** Each colour has a meaning. The conventional colours are:

- **Orange: domain event.** "Order placed", "Refund issued". Past tense, a fact.
- **Blue: command.** An intention or request: "Place order", "Issue refund". It can be rejected.
- **Small yellow: actor.** The person or role issuing a command.
- **Large pale yellow: aggregate** (or system). The thing that receives the command, checks rules and records the event.
- **Lilac: policy.** A reaction: "Whenever an order is placed, reserve stock." Policies are the glue between events and the next command, and they are often where automation lives.
- **Green: read model.** The information an actor needs to decide: "Stock levels", "Order status".
- **Pink: external system.** A payment provider, a carrier.
- **Red or hot pink: hot spot.** A question, conflict or pain point: "What if the payment succeeds after we cancelled?"

[[fig:events_storming]]

**Command, event or query?** The distinction trips up many teams and matters for data design:

| | Command | Event | Query |
|---|---|---|---|
| Tense | Imperative: `PlaceOrder` | Past: `OrderPlaced` | Question: `GetOrderStatus` |
| Can be refused? | Yes | No, it already happened | Not applicable |
| Changes state? | Requests a change | Records a change | No |
| Recipients | One handler | Any number of subscribers | One responder |
| Lives in data as | Rarely stored (perhaps as an audit log) | Event and fact tables | Access patterns, marts and APIs |

**Running a Big Picture session, briefly.**

1. **Chaotic exploration:** everyone writes events on orange notes, silently, and sticks them roughly in time order. No discussion yet.
2. **Enforce the timeline:** the group sorts notes, removes duplicates and argues about order. Disagreements become hot spots.
3. **Pivotal events:** mark the few events that change the phase of the story ("Order paid", "Parcel handed to carrier"). They suggest boundaries.
4. **People and systems:** add actors and external systems.
5. **Problems and opportunities:** everyone votes on the most important hot spots.
6. **Boundaries:** where language changes ("customer" means a payer in billing, a recipient in delivery) and around pivotal events, draw candidate **bounded contexts**.

**Aggregate design.** In a Software Design session, the group clusters commands and events around the thing that must enforce a rule. If "cannot ship an unpaid order" is a rule, the order is an aggregate whose state includes payment status. Good aggregates are small (one order, not a customer with every order) because each is a unit of consistency and concurrency, as the Event Sourcing topic showed.

**Applying it to data systems.** Data teams can use the same workshop to design pipelines and models before any code exists:

- Each **event** is a candidate event table or fact (`fct_order_placed`) and a candidate topic.
- Each **aggregate** suggests an entity and its dimension (`dim_order`, `dim_customer`).
- **Pivotal events** become the milestones of an accumulating snapshot table and the stage boundaries of a pipeline.
- **Read models** become marts, dashboards and serving tables: they tell you what queries must be fast.
- **Policies** become stream processors, triggers or scheduled jobs.
- **Hot spots** become data quality rules and open questions in the data contract ("What is the order date: placement or payment?").

For example, the pivotal events of an order story turn directly into an accumulating snapshot, one row per order with one timestamp per milestone:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE order_events (order_id TEXT, event_type TEXT, event_time TEXT)")
db.executemany("INSERT INTO order_events VALUES (?, ?, ?)", [
    ("o1", "OrderPlaced",     "2026-11-02 09:00"),
    ("o1", "PaymentCaptured", "2026-11-02 09:01"),
    ("o1", "OrderShipped",    "2026-11-03 14:30"),
    ("o2", "OrderPlaced",     "2026-11-02 10:00"),
    ("o2", "PaymentFailed",   "2026-11-02 10:02"),
    ("o3", "OrderPlaced",     "2026-11-03 08:00"),
    ("o3", "PaymentCaptured", "2026-11-03 08:05"),
])

# One row per order, one column per pivotal event: an accumulating snapshot
query = """
SELECT order_id,
       MIN(CASE WHEN event_type = 'OrderPlaced'     THEN event_time END) AS placed_at,
       MIN(CASE WHEN event_type = 'PaymentCaptured' THEN event_time END) AS paid_at,
       MIN(CASE WHEN event_type = 'OrderShipped'    THEN event_time END) AS shipped_at,
       MAX(CASE WHEN event_type = 'PaymentFailed'   THEN 1 ELSE 0 END)   AS payment_failed
FROM order_events
GROUP BY order_id
ORDER BY order_id
"""
for row in db.execute(query):
    print(row)
# ('o1', '2026-11-02 09:00', '2026-11-02 09:01', '2026-11-03 14:30', 0)
# ('o2', '2026-11-02 10:00', None, None, 1)
# ('o3', '2026-11-03 08:00', '2026-11-03 08:05', None, 0)
```

The same SQL runs in BigQuery (where `COUNTIF(event_type = 'PaymentFailed') > 0` is a shorter way to write the last column).

> **Pitfall:** Running the workshop with engineers only. The value comes from the people who do the work: without the warehouse supervisor nobody mentions that parcels are sometimes re-labelled, and that event never reaches your model.

> **Tip:** Photograph the wall and type the events into a shared list the same day, with each event's owner and its hot spots. That list is the first draft of your event catalog and your data contracts.

::: deeper
**Finding bounded contexts.** Boundaries show up as changes in language, changes of owner, and pivotal events where a hand-off happens. They are candidates, not answers: confirm them against team structure (Conway's law says systems mirror the communication structure of the organisation) and against how data must stay consistent.

**Remote sessions.** Online whiteboards (Miro, Mural and similar) work, with stricter facilitation: timeboxes, a legend of colours always visible, and people assigned to areas of the board. Expect lower energy than in a room; shorter sessions over several days help.

**Event Storming and event modelling.** **Event Modeling** (Adam Dymitruk) is a related technique that lays out screens, commands, events and read models along a timeline as a blueprint for building a system. It is more prescriptive and maps closely onto CQRS. Both share the idea that events are the backbone of the design.

> **Interview:** "How would you start designing the data model for a domain you do not know?" Run an Event Storming session with domain experts: collect the events on a timeline, find pivotal events and hot spots, then derive facts from events, dimensions from aggregates, marts from read models, and quality rules from hot spots.
:::

::: check
?? What is the difference between a command and an event?
A command is a request to do something (`PlaceOrder`) and can be rejected. An event records something that has already happened (`OrderPlaced`) and cannot be refused, only reacted to.

?? What does a lilac policy note represent?
A reaction to an event that triggers a new command: "whenever X happens, do Y". Policies often become automation such as stream processors or scheduled jobs.

?? How can pivotal events help a data engineer?
They mark the phase changes of a process, so they make good milestones for an accumulating snapshot table and good stage boundaries in a pipeline, and they suggest where bounded contexts meet.
:::

::: resources
- Book: [Introducing EventStorming](https://leanpub.com/introducing_eventstorming) · Alberto Brandolini's book on the method, formats and facilitation
- Docs: [EventStorming](https://www.eventstorming.com/) · the official site with the notation and resources
- Book: [Domain-Driven Design Distilled](https://www.oreilly.com/library/view/domain-driven-design-distilled/9780134434964/) · Vaughn Vernon; a short introduction to bounded contexts and aggregates, with a chapter on Event Storming
- Article: [Domain Driven Design](https://martinfowler.com/bliki/DomainDrivenDesign.html) · Martin Fowler's overview and links to the core ideas
- Article: [Bounded Context](https://martinfowler.com/bliki/BoundedContext.html) · why one model per context, and how contexts relate
- Docs: [Event Modeling](https://eventmodeling.org/) · the related blueprint technique by Adam Dymitruk
:::

### API Design for Data Products

::: plain
Events push facts out as they happen; an API answers questions when asked. A data product usually needs both, the way a newspaper both delivers every morning and keeps an archive you can search. Designing the API well means deciding how people ask (the style), how they get large answers in pieces (pagination), how much they may ask (rate limits), how the rules change over time without breaking them (versioning), and how answers can be reused (caching).
:::

**Which style?**

| Style | Shape | Strengths | Weaknesses | Good for |
|---|---|---|---|---|
| REST over HTTP and JSON | Resources and URLs: `GET /v1/customers/42/orders` | Universal, cacheable with HTTP, easy to try with `curl` | Over- and under-fetching; many round trips for nested data | Public and partner APIs, simple lookups |
| GraphQL | One endpoint; the client asks for exactly the fields it needs | Flexible for many front ends; strongly typed schema | HTTP caching is harder; expensive queries need cost limits; the N+1 problem in resolvers | Product front ends combining many entities |
| gRPC | Protobuf messages over HTTP/2, generated clients | Fast, compact, streaming, strict contracts | Not browser-native; harder to inspect by hand | Internal service-to-service and high-throughput feeds |
| Bulk access | Files, shared tables, Arrow streams | Millions of rows efficiently | Not for single-record lookups | Analytics consumers and ML training |

For data products the last row matters most: if consumers want whole tables, do not make them page through an API. Share the BigQuery dataset (BigQuery sharing, formerly Analytics Hub), let them read with the Storage Read API, or serve Arrow Flight (see the **Distributed systems** section). APIs are for **lookups and small slices**: "the risk score for customer 42", "today's orders for this store".

**Do not serve APIs straight from the warehouse.** BigQuery is built for scans, not for thousands of millisecond lookups per second, and every request costs a query. The usual pattern is to compute the product in BigQuery and **publish it into a serving store**: Bigtable or Firestore for key lookups, AlloyDB, Cloud SQL or Spanner for relational queries, Memorystore for hot caches. A Cloud Run service exposes the API, and Apigee or API Gateway in front handles keys, quotas and rate limits.

**Pagination.** Never return an unbounded list. Three approaches:

| Method | Request | Problem |
|---|---|---|
| Offset | `?limit=50&offset=1000` | The database still reads and discards 1,000 rows; inserts or deletes during paging shift rows, causing duplicates or gaps |
| Keyset (seek) | `WHERE (created_at, id) < (last_created_at, last_id) ORDER BY created_at DESC, id DESC LIMIT 50` | Needs a unique, indexed sort key; no jumping to page 37 |
| Cursor (opaque token) | `?page_token=...` returning `next_page_token` | The token usually encodes a keyset; clients cannot build or modify it |

Google's API design guide (AIP-158) standardises the cursor style: requests carry `page_size` and `page_token`, responses carry `next_page_token`, empty when there are no more results. The example shows why offset pagination misbehaves on a live table:

```python
import base64, json, sqlite3

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, created_at TEXT)")
db.executemany("INSERT INTO orders VALUES (?, ?)",
               [(i, f"2026-11-23T10:0{i}:00") for i in range(1, 8)])   # ids 1..7

ORDER = "ORDER BY created_at DESC, id DESC"        # newest first, id breaks ties

def page_by_offset(page, size=3):
    sql = f"SELECT id FROM orders {ORDER} LIMIT ? OFFSET ?"
    return [r[0] for r in db.execute(sql, (size, page * size))]

def encode(row):     # opaque cursor: the sort key of the last row, base64 encoded
    return base64.urlsafe_b64encode(json.dumps(row).encode()).decode()

def page_by_cursor(cursor=None, size=3):
    if cursor is None:
        rows = db.execute(f"SELECT created_at, id FROM orders {ORDER} LIMIT ?", (size,))
    else:
        created_at, last_id = json.loads(base64.urlsafe_b64decode(cursor))
        rows = db.execute(f"SELECT created_at, id FROM orders "
                          f"WHERE (created_at, id) < (?, ?) {ORDER} LIMIT ?",
                          (created_at, last_id, size))
    rows = rows.fetchall()
    return [r[1] for r in rows], (encode(list(rows[-1])) if rows else None)

first, cursor = page_by_cursor()
print(first, page_by_offset(0))          # [7, 6, 5] [7, 6, 5]

db.execute("INSERT INTO orders VALUES (8, '2026-11-23T10:08:00')")   # a new order arrives

print(page_by_offset(1))                 # [5, 4, 3]   order 5 is shown twice
print(page_by_cursor(cursor)[0])         # [4, 3, 2]   continues exactly after 5
print(cursor)                            # WyIyMDI2LTExLTIzVDEwOjA1OjAwIiwgNV0=
```

> **Tip:** Row-value comparisons such as `(created_at, id) < (?, ?)` work in PostgreSQL, AlloyDB and SQLite. Where they are not supported, write the equivalent `created_at < @c OR (created_at = @c AND id < @id)`. In production, also sign or encrypt the token so clients cannot tamper with it.

**Versioning.** Put the **major** version in the path (`/v1/`, as Google's AIP-185 recommends), and evolve within it only by compatible changes: new optional fields, new endpoints, new enum values clients were told to expect. Removing or renaming fields, changing types or changing defaults requires `/v2/`, run in parallel with a published deprecation date. The same rules as event schemas apply, for the same reason: you do not control your consumers' release schedules.

**Rate limiting.** Protect the serving store and share capacity fairly: a token bucket per API key (the **Distributed systems** section explains the algorithm), enforced at the gateway (Apigee's Quota and SpikeArrest policies). When a client exceeds it, return `429 Too Many Requests` with a `Retry-After` header. The IETF is standardising `RateLimit` headers that tell clients their remaining quota (still a draft).

**Caching headers.** HTTP caching (RFC 9111) saves both latency and cost for data that changes on a schedule:

```text
GET /v1/stores/17/daily-sales?date=2026-11-22
200 OK
Cache-Control: public, max-age=3600
ETag: "sales-17-2026-11-22-v3"

GET /v1/stores/17/daily-sales?date=2026-11-22
If-None-Match: "sales-17-2026-11-22-v3"
304 Not Modified                      (no body: the client's copy is still good)
```

Set `max-age` to match the product's refresh schedule, and use an `ETag` derived from the data version so revalidation is cheap.

**Idempotency for writes.** Data products sometimes accept writes (feedback, labels, corrections). Clients retry on timeouts, so each request carries an **idempotency key** that the server stores with the result, returning the stored result for a repeat, as Stripe popularised. An IETF draft standardises the `Idempotency-Key` header.

**Describe it with a spec.** **OpenAPI** describes REST APIs (paths, parameters, response schemas) and **AsyncAPI** the event side, so one data product can publish both specs, generated docs and client libraries. Lint them in CI the same way as event schemas.

> **Pitfall:** Exposing the warehouse table schema directly as the API response. Every internal column rename becomes a breaking API change. Map internal tables to a stable response model, the anti-corruption layer from earlier in this section, in the opposite direction.

::: deeper
**GraphQL cost control.** A single GraphQL query can ask for customers, their orders and each order's items: thousands of resolver calls. Use batching (the DataLoader pattern) to avoid N+1 database queries, and limit query depth and complexity per client. Persisted queries (clients send a query id, not text) make GraphQL responses cacheable and stop arbitrary expensive queries.

**Consistency of paged results.** Keyset pagination avoids duplicates but still shows a moving target: rows inserted behind the cursor are not seen. For exports that must be a consistent snapshot, page over a snapshot (a BigQuery table snapshot or time travel with `FOR SYSTEM_TIME AS OF`, or a database read timestamp in Spanner) and encode that timestamp in the token.

**gRPC streaming for feeds.** Server-streaming RPCs suit "send me every change for my stores": the server pushes messages over one HTTP/2 stream. For many consumers, an event topic is usually better, because the API server would otherwise reimplement a broker.

> **Interview:** "Design an API that serves a customer risk score computed nightly in BigQuery to a checkout service with a 50 ms budget." Compute in BigQuery, publish the scores to Bigtable (row key = customer id) after each run with a version tag, serve with Cloud Run behind Apigee with per-client quotas, cache with `max-age` until the next run and an `ETag` of the run version, version the path `/v1/`, and expose the same scores as a shared BigQuery table for analysts.
:::

::: check
?? Why can offset pagination return the same row twice?
Rows inserted (or deleted) before the current position between two requests shift every later row, so the next offset page starts earlier (or later) than where the previous page ended.

?? What is in a typical opaque page token?
The sort key of the last row returned (for example its timestamp and id), encoded and ideally signed, so the server can continue with a keyset query.

?? Why should a data API not query BigQuery for every request?
BigQuery is designed for large scans, so per-request latency and cost are high for single-record lookups. Precompute in BigQuery and serve from a low-latency store such as Bigtable, Firestore or AlloyDB.
:::

::: resources
- Docs: [Google API design guide: AIP-158 Pagination](https://google.aip.dev/158) · page size, page tokens and their rules
- Docs: [AIP-185 API versioning](https://google.aip.dev/185) · major versions in the path and compatible evolution
- Spec: [OpenAPI Specification](https://spec.openapis.org/oas/latest.html) · the standard description format for HTTP APIs
- Blog: [Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency) · Stripe on idempotency keys and safe retries
- Spec: [RFC 9111: HTTP Caching](https://www.rfc-editor.org/rfc/rfc9111) · `Cache-Control`, validation and `ETag` semantics
- Docs: [Apigee overview](https://cloud.google.com/apigee/docs/api-platform/get-started/what-apigee) · keys, quotas and rate limiting for APIs on Google Cloud
- Article: [Use the Index, Luke: Paging Through Results](https://use-the-index-luke.com/sql/partial-results/fetch-next-page) · Markus Winand on why keyset pagination is fast
:::

## Events and data mesh

So far, events have been a way for services to cooperate. This group widens the view to the whole company. In a **data mesh** (introduced in **Data engineering foundations** and governed as described in **Data governance, security and compliance**), domain teams own their data and publish it as products for others. Events are a natural form for those products: they are already owned by a domain, already have schemas, and already flow in real time.

The first topic covers the patterns: which events become products, how they are contracted, governed and traced across domains. The second makes it concrete with the backbone that carries them: Kafka (or Pub/Sub) topics, naming rules, self-describing messages and the isolation that stops one domain from hurting another.

### Data Mesh Event Patterns

::: plain
In a large city, each district runs its own market, but they all agree on common weights and measures, label their goods the same way and post their opening hours. A shopper from any district can buy from any other with confidence. In a data mesh, each business domain is a district, the events it publishes are its goods, and the shared rules (contracts, schemas, ownership, lineage) are the weights and measures that let other domains rely on them.
:::

**Events as data products.** Zhamak Dehghani's data mesh rests on four principles: domain ownership, data as a product, a self-serve data platform and federated computational governance. A **data product** is a unit of data with an owner, documentation, guarantees and access through defined **output ports**. A domain's event stream is a natural output port: `orders.order-placed.v1`, published by the Orders domain, documented, versioned and supported, is a product as much as any table.

Dehghani distinguishes three kinds of products, and events map onto them:

| Product kind | What it is | Event example |
|---|---|---|
| Source-aligned | Facts as the domain records them | `OrderPlaced`, `PaymentCaptured` |
| Aggregate | Combines several domains' data | A "customer 360" stream joining profile, order and support events |
| Consumer-aligned | Shaped for one use | Per-store hourly sales for replenishment |

**Public events are not internal events.** A service has many internal events (`OrderLineRepriced`, `CartRecalculated`) that reflect its implementation. Exposing them couples every consumer to internals. Domains should publish a deliberate, smaller set of **integration events** (also called public events) designed for outsiders, usually produced through the transactional outbox, with stable names and the CloudEvents envelope. Internal events stay on private topics.

**Two output ports, one contract.** Most consumers want either a stream (react in seconds) or a table (query history). Publish both from the same contract: the topic for streaming consumers, and the same events landed in BigQuery (with a Pub/Sub BigQuery subscription, or a Kafka connector or Dataflow job) as an append-only table, plus a current-state view. Consumers then never build their own competing copies of the raw events.

**Governed event contracts.** A **data contract** is the agreement between a producing domain and its consumers. For an event product it states:

```yaml
# orders.order-placed.v1: the contract (stored in the producer's repository)
product: orders.order-placed
version: 1.3.0
owner: orders-team
contact: orders-team@example.com
schema: registry://orders.order-placed.v1-value/versions/7
semantics:
  total_cents: gross amount including tax, in minor units of currency
  event_time: when the customer confirmed the order (UTC)
guarantees:
  delivery: at-least-once, deduplicate on CloudEvents source + id
  ordering: per order_id
  freshness: 99% of events published within 60 seconds of event_time
  retention: 30 days on the topic; full history in BigQuery
classification:
  customer_id: pseudonymous identifier
  personal_data: none in payload
output_ports:
  - kafka: orders.order-placed.v1
  - bigquery: orders_products.order_placed
```

The **Open Data Contract Standard** (ODCS, from the Bitol project under the Linux Foundation) defines a fuller, standard version of such a file. The point is that schema, meaning, guarantees and ownership are written down, versioned with the code and checked in CI, so "federated computational governance" means automated checks, not meetings.

**Cross-domain lineage.** When a marketing email is sent because a parcel shipped because a payment was captured because an order was placed, you want to trace that chain across four domains. Two ids in every event make it possible:

- **Correlation id:** the same for every event in one business flow (one checkout).
- **Causation id:** the id of the event (or command) that directly caused this one.

```python
# Each event records the event that caused it and the business flow it belongs to
events = {
    "e1": {"domain": "orders",    "type": "OrderPlaced",     "causation": None, "correlation": "chk-77"},
    "e2": {"domain": "payments",  "type": "PaymentCaptured", "causation": "e1", "correlation": "chk-77"},
    "e3": {"domain": "logistics", "type": "ParcelShipped",   "causation": "e2", "correlation": "chk-77"},
    "e4": {"domain": "marketing", "type": "ReviewRequested", "causation": "e3", "correlation": "chk-77"},
    "e5": {"domain": "orders",    "type": "OrderPlaced",     "causation": None, "correlation": "chk-78"},
}

def lineage(event_id):
    """Walk causation links back to the first event."""
    path = []
    while event_id:
        e = events[event_id]
        path.append(f"{e['domain']}.{e['type']}")
        event_id = e["causation"]
    return " <- ".join(path)

print(lineage("e4"))
# marketing.ReviewRequested <- logistics.ParcelShipped <- payments.PaymentCaptured <- orders.OrderPlaced

flow = sorted({e["domain"] for e in events.values() if e["correlation"] == "chk-77"})
print(flow)        # ['logistics', 'marketing', 'orders', 'payments']
```

Carry them as CloudEvents extension attributes (names must be lowercase alphanumeric, for example `correlationid` and `causationid`) alongside `traceparent`. That is record-level lineage. **Dataset-level lineage** (which jobs read which topics and wrote which tables) comes from the pipelines themselves: **OpenLineage** events from Spark, Flink, Airflow and dbt, and the lineage in **Knowledge Catalog**, which records lineage for BigQuery, Dataflow and Managed Airflow jobs and accepts OpenLineage events from other tools.

**Event-driven data products.** Products can also be *maintained* by events: a consumer-aligned product such as "open orders per store" is a CQRS read model built by a stream processor from source-aligned events, published with its own contract. The mesh becomes a graph of products feeding products, each owned by a team.

> **Pitfall:** Treating CDC of a service's tables as the domain's data product. Row changes expose internal schema, lack business meaning and break when the service refactors. CDC is a fine *transport* (for example, of an outbox table), but the product is the designed integration event.

> **Tip:** Make "who consumes this?" answerable. Require consumer registration (a consumer group or subscription created through the platform, linked to a team) so a producer planning a breaking change knows exactly whom to tell.

::: deeper
**Federated governance in practice.** The global rules are few and automated: every public topic has an owner, a registered schema with a compatibility mode, a contract file, a classification of personal data, and a retention period. A platform pipeline rejects topic creation that lacks them. Everything else (field names, how often to publish, which events to offer) is the domain's decision.

**Privacy across domains.** Events copied into many domains multiply personal data. Keep personal fields out of public payloads where possible (reference ids instead), or apply crypto-shredding as described in the Event Sourcing topic. The **Data governance, security and compliance** section covers erasure in immutable stores.

**Mesh is organisational first.** Topics and contracts do not create ownership by themselves. Without domain teams who are staffed and measured on the quality of their products, a mesh becomes a set of abandoned topics. Start with two or three domains that have motivated owners and real consumers.

> **Interview:** "How do you prevent consumers in other domains from breaking when the Orders team changes its events?" Publish deliberate integration events (not internal events or raw CDC), with a contract that includes schema, meaning and guarantees; enforce compatibility in a registry in CI; version breaking changes as new major topics with a deprecation window; and require registered consumers so the impact is known.
:::

::: check
?? What is the difference between internal events and integration events?
Internal events reflect a service's implementation and stay private. Integration (public) events are a deliberate, stable set designed for other domains, with a contract and versioning.

?? What do correlation and causation ids give you?
The correlation id groups every event of one business flow; the causation id links each event to the one that directly caused it. Together they let you trace a chain of events across domains.

?? Why publish both a topic and a BigQuery table as output ports of the same event product?
Streaming consumers need the topic; analytical consumers need history in a table. Publishing both from one contract stops every consumer from building its own inconsistent copy.
:::

::: resources
- Book: [Data Mesh](https://www.oreilly.com/library/view/data-mesh/9781492092384/) · Zhamak Dehghani; data products, output ports and federated governance
- Article: [Data Mesh Principles and Logical Architecture](https://martinfowler.com/articles/data-mesh-principles.html) · the four principles in Dehghani's foundational article
- Blog: [How to Build a Data Mesh with Event Streams](https://www.confluent.io/blog/how-to-build-a-data-mesh-using-event-streams/) · event streams as data products, by Confluent
- Article: [Data Mesh Architecture](https://www.datamesh-architecture.com/) · practical patterns, including data contracts and products
- Spec: [Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/latest/) · the standard file format for data contracts
- Docs: [OpenLineage](https://openlineage.io/docs/) · the open standard for lineage events from pipelines
- Docs: [About data lineage](https://cloud.google.com/dataplex/docs/about-data-lineage) · lineage in Knowledge Catalog across BigQuery and pipelines
:::

### Kafka as Event Backbone for Data Mesh

::: plain
A company's internal post service works because every department has a pigeonhole with a clear name, letters carry a standard cover sheet, and each department can only open its own post. Kafka (or Pub/Sub) as an event backbone is that post room for a data mesh: topics are the named pigeonholes, the CloudEvents headers and the schema are the cover sheet, and access rules make sure each domain only writes to its own topics and reads what it is allowed to.
:::

**Why one backbone.** In a mesh, domains publish independently, but consumers should not need a different technology per producer. A shared streaming platform (Google Managed Service for Apache Kafka, Confluent Cloud on Google Cloud, or Pub/Sub) operated by the platform team gives every domain the same way to publish, subscribe, secure and monitor events. Domains own their topics; the platform owns the cluster.

[[fig:events_mesh]]

**Topic naming conventions.** With hundreds of topics, names are the first layer of governance: they say who owns a topic, what it holds and which contract version it follows, and access rules can be written by name prefix. A simple convention:

- Public events: `<domain>.<event-name>.v<major>`, for example `orders.order-placed.v1`.
- Private topics: `<domain>.private.<name>`, readable only inside the domain.
- Lowercase, hyphens inside words, dots between parts. Never put environment, team members' names or technology (`-avro`, `-json`) in the name, since they change; environments get separate clusters or projects.

Kafka itself only allows ASCII letters, digits, `.`, `_` and `-`, at most 249 characters, and warns about mixing `.` and `_` because they collide in metric names. A check such as this belongs in the platform's topic-creation pipeline:

```python
import re

LEGAL = re.compile(r"[a-zA-Z0-9._-]{1,249}")                     # Kafka's own rule
PUBLIC = re.compile(r"(?P<domain>[a-z][a-z0-9-]*)\.(?P<event>[a-z][a-z0-9-]*)\.v(?P<major>[1-9][0-9]*)")
PRIVATE = re.compile(r"(?P<domain>[a-z][a-z0-9-]*)\.private\.[a-z0-9.-]+")

def check(topic, team_domain):
    if not LEGAL.fullmatch(topic) or topic in (".", ".."):
        return "illegal Kafka topic name"
    m = PRIVATE.fullmatch(topic) or PUBLIC.fullmatch(topic)
    if not m:
        return "expected <domain>.<event-name>.v<major> or <domain>.private.<name>"
    if m["domain"] != team_domain:
        return f"team {team_domain} cannot create topics in domain {m['domain']}"
    return "ok"

for topic in ["orders.order-placed.v1", "orders.private.pricing-cache",
              "orders_order_placed", "payments.payment-captured.v2", "Orders.Order Placed"]:
    print(f"{topic:32} {check(topic, 'orders')}")
# orders.order-placed.v1           ok
# orders.private.pricing-cache     ok
# orders_order_placed              expected <domain>.<event-name>.v<major> or <domain>.private.<name>
# payments.payment-captured.v2     team orders cannot create topics in domain payments
# Orders.Order Placed              illegal Kafka topic name
```

**Self-describing events.** A consumer in another domain should understand any message on its own. Combine the two layers from earlier in this section: **CloudEvents attributes in Kafka headers** (binary mode: `ce_id`, `ce_source`, `ce_type`, `ce_time`, plus `content-type: application/avro`) for routing, deduplication and tracing, and an **Avro (or Protobuf) payload** in Schema Registry wire format, whose 5-byte prefix names the exact schema. `ce_dataschema` can point at the registry subject and version for humans and tools. The message key is the aggregate id (`order_id`), which keeps per-entity order within a partition.

**Consumer group isolation across domains.** Every consuming application gets its own **consumer group**, named with its own domain prefix (`analytics.order-loader`, `marketing.review-trigger`), so each tracks its own offsets and one domain's slow consumer never holds back another's. Access is granted by prefix:

```bash
# Analytics may read every public Orders topic, using only its own consumer groups
kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config admin.properties \
  --add --allow-principal User:analytics-loader \
  --operation Read --operation Describe \
  --topic orders. --resource-pattern-type prefixed
kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --command-config admin.properties \
  --add --allow-principal User:analytics-loader \
  --operation Read --group analytics. --resource-pattern-type prefixed
```

A prefix of `orders.` would also match `orders.private.*`, so either add a `--deny-principal` rule for the private prefix or name private topics with a separate prefix (`private.orders.*`); decide this before the first topic is created. **Google Managed Service for Apache Kafka** supports Kafka ACLs, including prefixed topic ACLs, on top of IAM permissions for connecting to the cluster. Other isolation tools are **quotas** (byte-rate limits per client or user, so a replaying consumer cannot saturate brokers) and monitoring **consumer lag per group**, so each domain sees its own health.

**Topic configuration as a product decision.** Retention is part of the contract. Fact streams use `cleanup.policy=delete` with a retention long enough for consumers to recover from outages (days to weeks, with tiered storage for longer). State streams (latest customer profile per id) use `cleanup.policy=compact`, so a new consumer can bootstrap the current state by reading the topic from the start. Use replication factor 3 and `min.insync.replicas=2` for public topics so acknowledged events survive a broker loss.

**Pub/Sub as the backbone.** The same ideas map onto Pub/Sub: each domain owns topics in its own Google Cloud project; consuming domains create **subscriptions in their own projects** (a subscription can live in a different project from its topic), which gives each consumer independent acknowledgement and backlog, like a consumer group; IAM on each topic grants `roles/pubsub.subscriber` to consuming teams; topic schemas enforce Avro or Protobuf payloads, and attributes carry the CloudEvents metadata. Pub/Sub has no partitions to manage, but ordering needs ordering keys, and there is no compaction, so replaying history relies on message retention or a BigQuery copy.

| Concern | Kafka backbone | Pub/Sub backbone |
|---|---|---|
| Ownership boundary | Topic name prefix plus ACLs | Google Cloud project plus IAM |
| Consumer isolation | One consumer group per application | One subscription per application |
| Replay and bootstrap | Retention, tiered storage, compacted topics | Message retention and seek; no compaction |
| Ordering | Per partition, by key | Per ordering key, opt-in |
| Ecosystem | Kafka Connect, Kafka Streams, Flink | Dataflow, BigQuery and Cloud Storage subscriptions |

> **Pitfall:** One shared consumer group used by several teams "to save effort". Each message is then delivered to only one member of the group, so the teams silently split the stream between them and each sees only part of the data.

> **Tip:** Create topics, schemas, ACLs and consumer groups only through code (Terraform or a platform API) with the naming check in CI. Hand-created topics are the ones nobody owns a year later.

::: deeper
**One cluster or many?** A single multi-tenant cluster is simplest and lets any domain consume any public topic directly. Separate clusters per domain or region give stronger isolation and blast-radius control, at the cost of replicating public topics between them with MirrorMaker 2 or a vendor's cluster linking (see **Streaming systems**). A common compromise: one shared cluster for public events, with domains free to run private clusters for their internal traffic.

**Header survival.** Some sinks and tools drop Kafka headers. Check that your BigQuery or Iceberg connectors map CloudEvents headers into columns; otherwise you lose the event id needed for deduplication. When in doubt, also store `event_id` in the payload.

**Schema subjects per topic version.** With the `<domain>.<event-name>.v<major>` naming, the default topic name strategy gives one subject per major version (`orders.order-placed.v1-value`). Minor versions evolve under that subject with a `BACKWARD_TRANSITIVE` or `FULL_TRANSITIVE` mode; a new major version starts a new topic and a new subject, so its schema history is clean.

> **Interview:** "Fifty teams share one Kafka cluster for a data mesh. What rules do you set?" A naming convention with domain prefixes and major versions, topic creation only through a platform pipeline that checks names, schemas and contracts; prefixed ACLs so teams write only their own topics and read what they were granted; one consumer group per application with the consumer's domain prefix; quotas per principal; CloudEvents headers plus registry-framed Avro for self-describing messages; and lag and ownership dashboards per domain.
:::

::: check
?? Why should every consuming application have its own consumer group?
Each group tracks its own offsets and receives every message. A shared group splits the partitions among its members, so applications would each see only part of the stream, and one slow consumer would affect the others.

?? What makes a Kafka event self-describing across domains?
CloudEvents attributes in headers (id, source, type, time, content type, data schema) plus a registry-framed Avro or Protobuf payload whose prefix identifies the exact schema version.

?? What is the Pub/Sub equivalent of a separate consumer group per application?
A separate subscription per application, ideally in the consuming team's own project, each with its own acknowledgements and backlog.
:::

::: resources
- Docs: [Google Managed Service for Apache Kafka overview](https://cloud.google.com/managed-service-for-apache-kafka/docs/overview) · clusters, topics and consumer groups on Google Cloud
- Docs: [Access control with Kafka ACLs](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/access-control-kafka-acls) · prefixed topic ACLs on top of IAM in Managed Kafka
- Docs: [Apache Kafka documentation: authorization and ACLs](https://kafka.apache.org/documentation/#security_authz) · resource patterns, operations and `kafka-acls.sh`
- Blog: [How to Build a Data Mesh with Event Streams](https://www.confluent.io/blog/how-to-build-a-data-mesh-using-event-streams/) · topics as products, governance and self-service on Kafka
- Book: [Designing Event-Driven Systems](https://www.confluent.io/resources/ebook/designing-event-driven-systems/) · Ben Stopford on Kafka as a shared source of truth between teams
- Spec: [CloudEvents Kafka protocol binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md) · how CloudEvents attributes map onto Kafka headers
:::

## Wrap-up

### Cheat sheet

| Concept or task | Rule of thumb, command or SQL |
|---|---|
| Event sourcing | Store facts, derive state by folding; unique `(stream_id, version)` for optimistic concurrency |
| Old event shapes | Upcast at read time; never rewrite history |
| CQRS read model | Idempotent projector with a checkpoint saved in the same transaction as the view |
| Read-your-writes with CQRS | Return the write's version; wait for or bypass the projection for that user |
| Publish an event with a DB change | Transactional outbox in the same local transaction; relay by polling or CDC (Debezium) |
| Duplicates from the relay | Idempotent consumers: inbox table keyed on event id, or upserts by business key |
| Multi-service business process | Saga: compensate completed steps in reverse; retry steps after the pivot |
| Saga coordination | Short flows: choreography; long, branching or timed flows: orchestration (Temporal, Workflows) |
| Another service's data | Its API, its events or a data product; never its tables |
| Event envelope | CloudEvents: `specversion`, `id`, `source`, `type` (+ `time`, `subject`, `dataschema`) |
| Event naming | Past tense, business meaning; major version in `type` and topic |
| Compatible change | Optional field with default; minor version bump |
| Breaking change | New major version topic, run both, announce deprecation |
| Document events | AsyncAPI 3.0 per service; `asyncapi validate`, `asyncapi diff` in CI; generated EventCatalog |
| Find the events of a domain | Event Storming: events, commands, actors, policies, read models, hot spots |
| Pagination | Keyset or opaque cursor (`page_token`, `next_page_token`); avoid offset on live data |
| API caching | `Cache-Control: max-age` matching refresh; `ETag` from the data version; `304` on match |
| Cross-domain lineage | `correlationid` and `causationid` attributes plus OpenLineage for jobs |
| Topic names | `<domain>.<event-name>.v<major>`; private topics clearly separated |
| Consumer isolation | One consumer group (or subscription) per application, prefixed ACLs, quotas, lag per group |

### Practice

::: try
**1. Find the dual write.** A Cloud Run service handles refunds: it updates the `payments` row in AlloyDB to `REFUNDED`, commits, then publishes `RefundIssued` to Pub/Sub. Finance reports that about one refund a week never reaches the ledger service. Explain the cause and design a fix, including what the ledger service must do.
:::

?? Answer to 1
It is a dual write: if the service crashes, is scaled down or Pub/Sub returns an error after the commit, the event is lost while the database already says `REFUNDED`. Fix: in the same AlloyDB transaction as the update, insert an outbox row with a generated event id, type `RefundIssued` and the payload. A relay publishes it: a polling publisher, or Debezium (for example Debezium Server publishing to Pub/Sub) reading the write-ahead log. The relay is at-least-once, so the ledger service must be idempotent: record processed event ids in an inbox table inside the same transaction as its ledger entry, and skip repeats. Add a daily reconciliation query comparing refunded payments with ledger entries.

::: try
**2. Design the compensations.** A travel booking saga reserves a seat with an airline API, charges the customer's card, books a hotel through a partner, and sends a confirmation email. List the steps in a good order, mark the pivot, and give the compensation (or retry rule) for each step. What idempotency key would you use?
:::

?? A solution to 2
Order: (1) reserve seat (compensation: cancel reservation by reservation id); (2) book hotel (compensation: cancel booking by booking id, within the free-cancellation window); (3) charge card, the pivot (once it succeeds the saga only moves forward; if it fails, compensate 2 then 1); (4) send confirmation email, retriable (retry until sent, never compensated). Hard-to-undo steps go late: charging is placed after the bookings so a failed booking does not need a refund. Each call uses the key `<saga id>:<step name>` so a retried step or compensation is not applied twice. Store the saga state durably (Temporal or Workflows) and route compensations that keep failing to a manual review queue.

::: try
**3. Version an event.** The `orders.order-placed.v1` event has `total` as a float in euros. The team wants integer minor units with a currency so they can sell in other countries, and also wants to add an optional `channel` field. Which change is compatible and which is breaking? Describe the rollout.
:::

?? A solution to 3
Adding optional `channel` is compatible: a minor version (for example 1.4.0) under the same subject and topic. Changing `total` from float euros to integer cents with a currency changes type and meaning, so it is breaking. Rollout: create `orders.order-placed.v2` with `total_cents` and `currency` (CloudEvents `type` `...order.placed.v2`, its own registry subject); have the outbox relay publish both v1 and v2 for a migration window; update the AsyncAPI files and the catalog, which lists v1's consumers; consumers move to v2; after the announced date stop publishing v1. A softer alternative is to add `total_cents` and `currency` as optional fields to v1 first, but v1's `total` must keep its old meaning.

::: try
**4. Fix the paging.** A partner API lists shipments with `GET /v1/shipments?page=N&size=500`, implemented with `ORDER BY created_at DESC LIMIT 500 OFFSET N*500` on Cloud SQL for PostgreSQL. Partners complain about duplicates and slow late pages. Redesign the endpoint in the style of AIP-158 and give the SQL.
:::

?? A solution to 4
Replace `page` with `page_size` (capped, say at 1,000) and `page_token`; respond with `next_page_token`, empty on the last page. The token encodes the last row's `(created_at, shipment_id)` and is signed. The query uses keyset pagination backed by an index on `(created_at DESC, shipment_id DESC)`:

```sql
SELECT shipment_id, created_at, status
FROM shipments
WHERE (created_at, shipment_id) < ($1, $2)   -- values decoded from page_token
ORDER BY created_at DESC, shipment_id DESC
LIMIT $3;                                      -- page_size
```

Each page is an index range read, so late pages are as fast as early ones, and new shipments inserted at the top no longer shift later pages. Keep the old parameters working in `/v1/` for a deprecation period or introduce the new form in `/v2/`.

::: try
**5. Open the backbone to a new domain.** The Marketing domain wants to trigger review emails from `logistics.parcel-shipped.v1` on the shared Managed Kafka cluster, and to keep 90 days of shipment history for analysis. What do you set up, and what do you refuse?
:::

?? A solution to 5
Set up, through the platform pipeline: a Marketing service principal; a Read ACL on the `logistics.parcel-shipped.v1` topic (or the public `logistics.` prefix, if private topics are named separately); a Read ACL on the `marketing.` group prefix and a dedicated consumer group such as `marketing.review-trigger`; registration of Marketing as a consumer in the event catalog and in the contract; lag alerts for the new group. For 90 days of history, do not ask Logistics to raise topic retention: use the BigQuery output port of the same product (or, if it does not exist yet, ask Logistics to add it), and query it there. Refuse sharing a consumer group with another team, reading Logistics' private topics, and creating topics under the `logistics.` prefix.

### What comes next

You can now design how services announce facts and stay consistent without shared transactions (event sourcing, CQRS, outbox and sagas), shape and document the events themselves (CloudEvents envelopes, compatible versioning, AsyncAPI and catalogs), discover the right events with Event Storming, build sound data APIs, and run events as products on a shared backbone. The next section, **Data ingestion**, picks up where these events leave the services: it compares bulk, incremental and CDC ingestion, goes deep on Debezium and Kafka Connect, covers every kind of source (SaaS and REST APIs, webhooks, files, logs, clickstream, IoT and market data), and finishes with throughput engineering, streaming ingestion into the lakehouse and zero-ETL patterns.
