This section is about deciding what tables to build before you build them: which things the business cares about, how they relate, at what level of detail each table holds its rows, which keys identify them, how history is kept, and how all of that is written down so other people can trust and change it. It is written for data engineers and analytics engineers who already write SQL and pipeline code and now have to design the data those pipelines produce. It assumes Data engineering foundations (OLTP versus OLAP, the raw, staging and marts layers), SQL for data engineering (joins, window functions, `MERGE`, and the SQL recipes for snapshots, bridge tables and point-in-time joins in Advanced SQL Modeling Patterns) and Programming for data engineering (enough Python to run the small checks here). Examples use BigQuery GoogleSQL for analytics and PostgreSQL for operational databases; every Python example runs with the standard library only, and most SQL also runs in SQLite through Python's `sqlite3`.

**What you will be able to do afterwards**

- Turn a vague business request into a conceptual, a logical and a physical model, and explain the decisions at each level.
- Normalise an operational schema to third normal form, choose keys and indexes, and change it safely while the application keeps running.
- Design star schemas with a declared grain, conformed dimensions and the right slowly changing dimension type for each attribute.
- Explain when to use one big table, Data Vault, anchor modeling or bi-temporal tables instead, and what each costs.
- Model metrics, master data, reference data, events, lakehouse tables, graphs and industry-standard formats, and write data contracts for them.
- Test, document, diagram and review models as code, and handle a data modeling interview round with a clear structure.

**The big picture.** A data model is an agreement about meaning. Before it is a set of `CREATE TABLE` statements, it is a set of answers to questions such as "what is a customer?", "is a cancelled order still an order?" and "when an address changes, do old orders move to the new city?". Engines and tools change every few years; those answers outlive them. Modeling works at three levels of detail. The **conceptual** model names the business things and how they relate, in words a manager would recognise. The **logical** model adds attributes, keys and rules, independent of any database product. The **physical** model is the implementation in a particular engine, with its types, indexes, partitions and costs. Operational systems are modeled for correct, fast writes (normalised); analytical systems are modeled for easy, correct reads (dimensional, denormalised); and much of a data engineer's work is the translation between the two, including keeping history that the operational system throws away.

The section has four groups. Modeling fundamentals covers the design process and the three levels. Analytical modeling covers Kimball dimensional modeling and its alternatives (one big table, Data Vault, anchor modeling, temporal tables). Specialised models covers metrics, master and reference data, events, lakehouse tables, graphs, industry standards and data contracts. Modeling in practice covers evolution, testing, documentation, platform playbooks, metadata, tools, a portfolio project and interviews.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Modeling fundamentals

Every model, whether a three-table operational schema or a warehouse with five hundred tables, comes out of the same process: understand what the business does and asks, name the things involved, pin down their attributes and rules, and only then choose an engine-specific layout. Skipping the early steps is the most common reason warehouses end up with three definitions of "customer" and dashboards that disagree.

This group walks through that process in order. The first topic covers the process itself and the habits (naming, reviews) that keep a model healthy. The next three take one level each: conceptual (the business map), logical (normalisation and keys) and physical for operational databases (indexes, partitions, safe changes). The analytical group after this builds on all of it, because a good star schema is designed from a clear understanding of the operational model it reads from.

### Modeling Fundamentals & Design Process

::: plain
A data model is the plan for your data, the way an architect's drawing is the plan for a house. You agree on the rooms and how they connect before anyone pours concrete, because moving a wall later is expensive. The plan starts as a rough sketch the owner can understand ("three bedrooms, kitchen next to the dining room"), becomes a detailed drawing with measurements, and finally a builder's specification for one particular site. Data models go through the same three stages, and most of the value comes from the conversations in the first one.
:::

**What a model contains.** Whatever the level, a model describes three kinds of thing:

- **Entities**: the things the business keeps information about, such as Customer, Order, Product and Store. An entity is a type; a single customer (Asha, from Pune) is an **instance**.
- **Attributes**: the facts recorded about each entity, such as a customer's email, a product's price or an order's date.
- **Relationships**: how entities connect, with a **cardinality** (how many on each side) and an **optionality** (whether the link is required). "A customer places zero or more orders; each order is placed by exactly one customer."

The model also records **rules**: identifiers (what makes two rows the same customer), allowed values, and constraints such as "an order line's quantity is greater than zero".

**Three levels of detail.**

[[fig:modeling_levels]]

| Level | Audience | Contains | Example |
|---|---|---|---|
| Conceptual | business owners, analysts | entities, relationships, definitions in business words | "A Customer places Orders; an Order contains Products" |
| Logical | data modelers, engineers | attributes, keys, cardinality, normalised structure, no engine details | `orders(order_id PK, customer_id FK, order_date, status)` |
| Physical | engineers, DBAs | types, indexes, partitions, clustering, storage settings for one engine | PostgreSQL `BIGINT`, B-tree on `customer_id`; BigQuery `PARTITION BY order_date` |

One logical model can have several physical models: the same orders entity becomes a normalised PostgreSQL table in the shop application and a partitioned, clustered BigQuery fact table in the warehouse.

**The design process.** A process that works for projects of any size:

1. **Collect the questions and processes.** Interview the people who will use the data. Ask what decisions they make, which numbers they look at, and which business processes create those numbers (placing an order, shipping it, returning it). Write the questions down verbatim: "Revenue per city per week, excluding cancelled orders".
2. **Find the nouns and verbs.** Nouns in the questions become candidate entities (customer, city, order), verbs become relationships (places, ships, returns), and adjectives and numbers become attributes (cancelled, revenue).
3. **Draw the conceptual model** and review it with the business. Settle definitions now: is a guest checkout a customer?
4. **Write the logical model**: attributes, keys, normal forms, cardinalities.
5. **Choose the physical design** for each target engine, driven by access patterns, volumes and cost.
6. **Review, implement, test and document**, then keep the model under version control so changes are reviewed like code.

**Bounded contexts.** Large organisations do not have one meaning for each word. "Customer" in billing is whoever pays the invoice; in support it is whoever opens a ticket; in marketing it may include people who never bought anything. Domain-Driven Design (DDD), a software design approach from Eric Evans, calls each area with its own consistent vocabulary a **bounded context**. Rather than forcing one enterprise-wide definition, you model each context separately and define explicit mappings between them ("a billing account maps to one or more support customers"). In the warehouse this shows up as conformed dimensions with clearly stated definitions, covered in the Analytical modeling group.

**Naming standards.** Names are the part of the model everyone sees. A short, enforced standard prevents years of confusion:

| Rule | Example |
|---|---|
| lower `snake_case` for tables and columns | `order_line`, `unit_price` |
| one name per concept across the whole platform | always `customer_id`, never `cust_id` in one table and `client_no` in another |
| suffix tells the type and meaning | `_id` key, `_at` timestamp, `_date` date, `_ts` only if your team prefers it |
| booleans read as questions | `is_active`, `has_discount` |
| amounts carry unit or currency | `amount_eur`, `weight_kg`, or a separate `currency_code` column |
| layer prefixes in the warehouse | `stg_` staging, `int_` intermediate, `dim_` dimension, `fct_` fact |
| no reserved words or abbreviations only you know | `order_status`, not `ord_st` |

> **Pitfall:** Naming a column `date`, `timestamp` or `value`. It works until someone joins two tables that both have a `date` column, or moves to an engine where the word is reserved. Name what the value means: `order_date`, `shipped_at`, `reading_value`.

**The model review.** Treat a model change like a code change: open a pull request with the diagram or YAML, the DDL and a short description of why. Reviewers check a list such as: Is the grain of each table stated? Is every key defined and tested for uniqueness? Do new names follow the standard? Does a new entity duplicate an existing one? Who owns it? What breaks downstream? The Modeling in practice group shows how to automate most of this.

> **Tip:** Write the grain and owner of every table in one sentence before writing any SQL: "One row per order line; owned by the commerce data team". If you cannot write that sentence, the design is not finished.

::: deeper
**Top-down and bottom-up modeling.** Top-down modeling starts from the business (interviews, processes, questions) and produces a model the sources must fit. Bottom-up modeling starts from the source systems (reverse-engineering their schemas) and generalises. Real projects combine both: the top-down model tells you what the business means, the bottom-up survey tells you what data actually exists and where the gaps are. A gap ("marketing wants revenue per campaign, but orders do not record the campaign") is a valuable early finding.

**Why the levels matter in data engineering.** Application developers often skip straight to physical design, because one application owns its database. A data engineer integrates many sources and serves many consumers, so the conceptual and logical levels are where conflicts between sources surface and are resolved. They are also what survives a platform migration: moving from an on-premises warehouse to BigQuery rewrites the physical model and keeps the logical one.

**Notation.** The most common diagram notation is **crow's foot** (information engineering): a line between entities, with marks at each end for "one" (a bar), "many" (the three-pronged crow's foot) and "optional" (a circle). Chen notation (diamonds for relationships) appears in textbooks; UML class diagrams appear in software teams. The notation matters less than using one consistently.

**Agile modeling.** You do not need the whole enterprise modeled before building anything. A common approach is to model one business process end to end (orders, say), ship it, and extend the model process by process, using conformed dimensions as the shared spine. The Kimball "bus matrix" in the next group is the planning tool for exactly this.

> **Interview:** "Walk me through how you would model data for a new product area." A strong answer names the steps (questions, processes, conceptual model reviewed with the business, logical model with keys and grain, physical design per engine, tests and documentation) and gives one concrete example of a definition you would settle early, such as whether refunds reduce revenue on the order date or the refund date.
:::

::: check
?? What are the three levels of a data model, and who is each one for?
Conceptual (entities and relationships in business words, for business owners), logical (attributes, keys and rules independent of any engine, for modelers and engineers) and physical (types, indexes, partitions and settings for one engine, for engineers and DBAs).

?? What is a bounded context, and why does it matter for a warehouse?
An area of the business with its own consistent vocabulary, such as billing or support. The same word (customer) can mean different things in different contexts, so the warehouse should model each meaning explicitly and map between them rather than silently mixing them.

?? Which sentence should you be able to write for every table before writing its SQL?
Its grain and owner, for example "one row per order line, owned by the commerce data team".
:::

::: resources
- Docs: [DAMA-DMBoK](https://www.dama.org/cpages/body-of-knowledge) · the Data Management Body of Knowledge; its data modeling and design chapter is the standard checklist
- Book: [Data Modeling Essentials](https://www.oreilly.com/library/view/data-modeling-essentials/9780128093931/) · Simsion and Witt; the design process, levels and review practice in depth
- Book: [The Data Warehouse Lifecycle Toolkit](https://www.oreilly.com/library/view/the-data-warehouse/9780471200246/) · Kimball and others; requirements gathering and design for analytics projects
- Article: [Bounded Context](https://martinfowler.com/bliki/BoundedContext.html) · Martin Fowler's short explanation of the DDD idea, with a diagram
- Docs: [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview) · dbt Labs' naming and layering conventions for staging, intermediate and marts
:::

### Conceptual Data Modeling

::: plain
A conceptual model is the map you draw on a whiteboard with the people who run the business: boxes for the things they talk about (customers, orders, products, warehouses) and lines for how those things relate. There are no columns, no data types and no technology on it. Its purpose is to make sure everyone means the same thing by the same word before any engineering starts, much as a city map shows districts and main roads but not the plumbing.
:::

**What goes on a conceptual model.** Only three things: entities, relationships with cardinality, and a one-line definition of each. For the small shop used throughout this course:

```text
Customer  ──places──<  Order  ──contains──<  Order line  >──refers to──  Product
                         │
                         └──ships via──<  Shipment
Definitions:
  Customer   a person or company with a registered account (guest checkouts are not customers)
  Order      a confirmed request to buy, from the moment payment is authorised
  Order line one product on one order, with a quantity and price
  Product    a sellable item identified by a SKU (stock keeping unit)
  Shipment   one parcel handed to a carrier; an order may ship in several parcels
```

The `──<` marks the "many" end. Notice how much business meaning is already decided: guest checkouts are excluded from Customer, an order exists only after payment authorisation, and orders can ship in several parcels. Each of those decisions changes later table designs and numbers.

**Subject areas.** Large models are split into **subject areas** (sometimes called domains): Sales, Inventory, Finance, Marketing, HR. A subject area groups related entities so a model with two hundred entities can be read one page at a time. Subject areas usually line up with the teams that own the data, which makes them the natural unit for ownership.

**Glossary alignment and ubiquitous language.** The definitions on the model belong in a **business glossary**: a list of terms with agreed definitions, owners and synonyms. DDD calls the shared vocabulary of a team the **ubiquitous language**: the words used in meetings, in code, in table names and in dashboards should be the same words. If the business says "subscriber" and the table says `user_account`, translation errors creep in. In Google Cloud, Knowledge Catalog (formerly Dataplex Universal Catalog; Data Catalog before that) provides a business glossary whose terms can be attached to tables and columns, so the definition travels with the data.

**Context boundaries and ownership.** For each entity, record which system is the **system of record** (the authoritative source) and which team owns the definition:

| Entity | System of record | Owning team | Notes |
|---|---|---|---|
| Customer | CRM | Customer data | billing accounts map to customers 1 to many |
| Order | shop database | Commerce | status changes come from the payment service |
| Product | product catalogue | Merchandising | prices change daily; history needed |
| Shipment | carrier integration | Logistics | arrives up to 2 days late |

This table is short to write and prevents long arguments later. When two systems both claim to be the source of customer addresses, you will find out now, not when a dashboard disagrees with the CRM.

**Running a conceptual modeling session.** Practical steps that work with non-technical people:

1. Ask the group to describe a typical day or a typical order from start to finish. Write the nouns on sticky notes.
2. Group the notes into candidate entities; merge synonyms ("client", "account holder" and "customer" may be one entity, or may not).
3. Draw lines for relationships and ask cardinality questions in plain words: "Can an order have no lines? Can one order belong to two customers? Can a product be in no orders?".
4. Write a one-line definition for each entity, and agree an owner.
5. Photograph the result, redraw it in a tool, and circulate it for corrections.

Event Storming, covered in the Event-driven architecture section, is a richer workshop technique that starts from business events (OrderPlaced, PaymentFailed) rather than nouns. It works well when the process matters more than the static structure.

> **Pitfall:** Letting the source system's table names become the conceptual model. Source schemas reflect one application's history (`tbl_cust_master`, `cust_type = 7`), not the business. Name entities in business terms and map the sources onto them.

> **Tip:** Ask for counterexamples. "Is there ever an order with two customers?" often gets "no", and then "well, gift orders have a buyer and a recipient". Those exceptions are where models break, so hunt for them early.

::: deeper
**Supertypes and subtypes.** Sometimes several entities share most of their meaning: Individual Customer and Business Customer are both Customers, with some different attributes. The conceptual model can show a **supertype** (Customer) with **subtypes**. At the physical level you later choose between one table with a type column and nullable subtype columns, one table per subtype, or a shared table plus subtype tables. Decide the concept first and the implementation later.

**Party modeling.** Many enterprise models use a generic **Party** entity (a person or an organisation) with **roles** (customer, supplier, employee). It handles a supplier who is also a customer, at the cost of abstraction that confuses business readers. Use it when parties genuinely play several roles; otherwise model the business terms directly.

**Conceptual models and data mesh.** In a data mesh, each domain team owns its data products. The conceptual model and its context map (which domain owns which entity, and how entities map across domains) become the backbone of the mesh: they show which data products exist and how to join them. Without it, a mesh becomes many disconnected silos.

**How detailed should it be?** A conceptual model for one project usually has 10 to 30 entities. An enterprise conceptual model may have 50 to 150, split into subject areas. If yours has attributes listed on it, it has become a logical model; that is fine, but keep a version without them for business conversations.

> **Interview:** "Two teams define 'active customer' differently. What do you do?" Make the difference visible rather than picking a winner silently: document both definitions in the glossary under distinct names (for example `active_customer_30d` for marketing and `billable_customer` for finance), agree an owner for each, and model both as separate, clearly named metrics.
:::

::: check
?? What three things appear on a conceptual model, and what does not?
Entities, relationships with cardinality, and short definitions. Attributes, data types, keys and anything engine-specific do not.

?? Why record the system of record and owner for each entity?
So conflicts between sources (two systems claiming to own customer addresses) and responsibility for definitions are settled before tables are built, not discovered when numbers disagree.

?? What is the ubiquitous language?
The shared vocabulary of a domain, used consistently in conversation, code, table names and dashboards, so meaning is not lost in translation.
:::

::: resources
- Article: [Ubiquitous Language](https://martinfowler.com/bliki/UbiquitousLanguage.html) · Martin Fowler's summary of the DDD practice of one shared vocabulary
- Docs: [Domain-Driven Design Reference](https://www.domainlanguage.com/ddd/reference/) · Eric Evans' free summary of bounded contexts, context maps and the rest of DDD
- Book: *Data Modeling Made Simple* by Steve Hoberman · a practical introduction to conceptual, logical and physical models for business and technical readers
- Article: [EventStorming](https://www.eventstorming.com/) · Alberto Brandolini's workshop technique for discovering a domain from its events
- Book: [Data Modeling Essentials](https://www.oreilly.com/library/view/data-modeling-essentials/9780128093931/) · Simsion and Witt; the chapters on conceptual modeling and business requirements
:::

### Logical Data Modeling & Normalization

::: plain
The logical model fills in the details: every attribute, every key, and exactly which table each fact lives in. The main tool is **normalisation**: arranging data so each fact is stored in exactly one place. Think of a contact list where a friend's phone number is written on every birthday card you have ever logged; when the number changes, you must find and fix every card, and you will miss some. Normalisation keeps the number on one card and points to it from everywhere else.
:::

**Functional dependencies.** Normalisation rests on one idea. Attribute B **functionally depends** on attribute A (written A → B) if each value of A always comes with exactly one value of B. In the shop, `customer_id → city` (each customer has one city) and `product → price` (at one moment, each product has one list price), but not `city → customer_id`. A **key** is a set of attributes on which every other attribute depends.

You can check a dependency in data with a few lines of Python. This runs as is:

```python
rows = [  # an unnormalised order-lines extract
    {"order_id": 101, "product": "tea",  "qty": 2, "customer_id": 1, "city": "Pune"},
    {"order_id": 101, "product": "cake", "qty": 1, "customer_id": 1, "city": "Pune"},
    {"order_id": 103, "product": "tea",  "qty": 1, "customer_id": 1, "city": "Pune"},
    {"order_id": 104, "product": "cake", "qty": 3, "customer_id": 3, "city": "Pune"},
]

def holds(rows, lhs, rhs):
    """True if lhs -> rhs: each lhs value maps to exactly one rhs value."""
    seen = {}
    for r in rows:
        k, v = tuple(r[c] for c in lhs), tuple(r[c] for c in rhs)
        if seen.setdefault(k, v) != v:
            return False
    return True

print(holds(rows, ["order_id"], ["customer_id"]))     # True
print(holds(rows, ["customer_id"], ["city"]))         # True
print(holds(rows, ["city"], ["customer_id"]))         # False
print(holds(rows, ["order_id"], ["qty"]))             # False: qty needs (order_id, product)
```

Data can only disprove a dependency; it cannot prove one (the next row might break it). The real dependencies come from business rules, which is why the conceptual work matters.

**The normal forms, on one example.** Start from the extract above, where `(order_id, product)` identifies a row.

- **First normal form (1NF)**: every column holds one atomic value, and there are no repeating groups. A column `products = 'tea, cake'` violates 1NF; one row per order line fixes it.
- **Second normal form (2NF)**: 1NF, and no non-key attribute depends on only part of a composite key. `customer_id` depends on `order_id` alone, not on `(order_id, product)`, so it moves to an `orders` table keyed by `order_id`.
- **Third normal form (3NF)**: 2NF, and no non-key attribute depends on another non-key attribute (no **transitive** dependency). In `orders(order_id, customer_id, city)`, `city` depends on `customer_id`, which depends on `order_id`. Move `city` to `customers`.
- **Boyce-Codd normal form (BCNF)**: for every non-trivial dependency A → B, A is a key. It differs from 3NF only in rare cases with overlapping candidate keys (for example, a rule "each instructor teaches one course" in a table keyed by student and course).

The result is the familiar shape:

```text
customers(customer_id PK, name, city)
orders(order_id PK, customer_id FK -> customers, order_date, status)
products(product_id PK, name, list_price)
order_lines(order_id FK, product_id FK, qty, unit_price, PK(order_id, product_id))
```

Note `unit_price` on the order line: it is the price **charged**, a fact about the sale, not a copy of `list_price`. Keeping it is not a normalisation error; dropping it would lose history when the list price changes. The classic summary of 3NF is "every non-key attribute depends on the key, the whole key, and nothing but the key".

**Why normalise.** Redundant copies cause **anomalies**: an **update anomaly** (Asha moves, and three of her five order rows still say Pune), an **insert anomaly** (you cannot record a new customer until they order), and a **delete anomaly** (deleting a customer's only order deletes the customer). Operational databases normalise to about 3NF to prevent these. Analytical models deliberately denormalise later, because they are loaded by controlled pipelines rather than edited by users.

**Surrogate versus natural keys.** A **natural key** is an identifier with business meaning (an email, a SKU, a national ID, an ISO country code). A **surrogate key** is a meaningless value generated by the system (an auto-increment integer, a UUID, a hash). Natural keys change (people change email), get reused, and leak personal data; surrogate keys are stable and compact. The usual rule: give each table a surrogate primary key, and keep the natural key as a column with a `UNIQUE` constraint so duplicates are still caught. Small, truly stable code sets (ISO currency codes) can use the natural key directly.

**Cardinality and optionality.** For each relationship, record both ends: one-to-one, one-to-many or many-to-many, and whether each side is optional. "Each order has exactly one customer" becomes a `NOT NULL` foreign key; "a customer may have no orders" needs nothing.

**Resolving many-to-many.** A relational table cannot hold a many-to-many relationship directly. Products belong to many categories and categories contain many products, so you add an **associative** (junction) table, `product_category(product_id, category_id)`, with a composite primary key. Associative tables often gain attributes of their own (`is_primary_category`, `added_at`), which is a sign they are entities in their own right.

**Temporal attributes.** Some attributes change and their history matters: prices, addresses, plans. The logical model should mark them, because they cannot simply be columns on the entity. The usual design is a child table with a validity interval, such as `product_price(product_id, valid_from, valid_to, list_price)`. The analytical group covers history in detail (slowly changing dimensions, anchor modeling, bi-temporal tables).

> **Pitfall:** Using a natural key that is not really unique or stable. Emails are shared by families, SKUs are reused after a product is retired, and phone numbers are recycled. Check uniqueness in the real data and ask the business how the value is assigned before trusting it as an identifier.

::: deeper
**Higher normal forms.** **Fourth normal form (4NF)** removes independent multi-valued facts stored together (an employee's skills and languages in one table create a cross product). **Fifth normal form (5NF)** handles join dependencies, where a table can be rebuilt losslessly only from three or more projections. **Sixth normal form (6NF)** puts each non-key attribute in its own table, usually with time; anchor modeling, later in this section, is built on it. In practice 3NF or BCNF is the target for operational schemas; 4NF and 5NF problems are rare but worth recognising.

**Decomposition must be lossless.** Splitting a table is correct only if joining the parts gives back exactly the original rows (a **lossless join**) and the dependencies can still be enforced. Splitting on a shared key that is a key of one side guarantees losslessness.

**Keys in distributed systems.** Auto-increment integers need a central counter, which is a bottleneck across many writers and a hotspot in range-partitioned stores such as Spanner and Bigtable. UUIDs avoid coordination; version 4 UUIDs are random (good for spreading writes, poor for B-tree locality), and version 7 UUIDs start with a timestamp (good locality, sortable by creation time). In warehouses, deterministic hash keys (for example `FARM_FINGERPRINT` of the natural key in BigQuery, or an MD5 or SHA-256 hash) let independent pipelines compute the same key without a lookup; Data Vault relies on this.

**Normalisation and NULLs.** A design with many optional attributes on one entity (most products have no `voltage`) can be split into subtype tables to avoid NULLs, at the cost of joins. Most teams accept nullable columns in OLTP schemas when the subtype logic is simple.

> **Interview:** "Explain 3NF in one sentence and give an example of a violation." Every non-key attribute depends on the key, the whole key and nothing but the key; storing a customer's city on each order row violates it, because city depends on the customer, not on the order.
:::

::: check
?? A table `order_lines(order_id, product_id, qty, order_date)` has key `(order_id, product_id)`. Which normal form does it violate, and how do you fix it?
2NF: `order_date` depends only on `order_id`, part of the key. Move it to `orders(order_id, order_date, ...)`.

?? Why keep `unit_price` on order lines even though products have a `list_price`?
It records the price actually charged at the time of sale. The list price changes; the charged price is a separate fact and its history must not be lost.

?? Why do most tables get a surrogate primary key even when a natural key exists?
Natural keys can change, be reused or contain personal data. A surrogate key is stable and compact; the natural key is kept as a unique column so duplicates are still caught.
:::

::: resources
- Article: [A Simple Guide to Five Normal Forms in Relational Database Theory](http://www.bkent.net/Doc/simple5.htm) · William Kent's classic, short explanation of 1NF to 5NF with examples
- Book: [Database System Concepts](https://www.oreilly.com/library/view/database-system-concepts/9780078022159/) · Silberschatz, Korth and Sudarshan; the chapters on functional dependencies and normal forms
- Book: *Database Design for Mere Mortals* by Michael Hernandez · gentle, practical logical design for beginners
- Video: [Normalization explained (1NF to BCNF)](https://www.youtube.com/watch?v=GFQaEYEc8_8) · Neso Academy's lecture on the normal forms
- Article: [Database normalization](https://en.wikipedia.org/wiki/Database_normalization) · a reference summary of every normal form with worked tables
:::

### Physical Data Modeling for OLTP Systems

::: plain
The physical model is the builder's specification: exact data types, which indexes exist, how big tables are split, and which rules the database enforces. For an operational (OLTP) database, the one an application reads and writes thousands of times a second, every choice is a trade between fast reads and fast writes. An index is like the index at the back of a book: it makes finding a topic instant, but every time you add a page, the index must be updated too.
:::

This topic uses **PostgreSQL**, the most common open-source OLTP database (on Google Cloud: Cloud SQL for PostgreSQL and AlloyDB). The Databases section covers the engines themselves; here the question is how to turn the logical model into tables that stay fast and correct.

**Types and constraints.** Choose the narrowest correct type and let the database enforce the rules from the logical model:

```sql
-- PostgreSQL
CREATE TABLE customers (
  customer_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email        TEXT NOT NULL UNIQUE,
  name         TEXT NOT NULL,
  city         TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  order_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id  BIGINT NOT NULL REFERENCES customers (customer_id),
  status       TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'shipped', 'cancelled')),
  amount       NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
  ordered_at   TIMESTAMPTZ NOT NULL
);
```

Key choices: `NUMERIC` for money (binary floating point cannot represent 0.10 exactly), `TIMESTAMPTZ` for moments in time (stored as UTC, converted on display), `TEXT` with a `CHECK` rather than an arbitrary `VARCHAR(n)` limit, and `NOT NULL` wherever the business rule says the value is required. Constraints are the cheapest data quality checks you will ever get: they run on every write, inside the transaction.

**Index strategy.** A **B-tree** index (the default) keeps keys sorted, so it serves equality, ranges, sorting and prefix matches. Rules that cover most cases:

- The primary key and every `UNIQUE` constraint get an index automatically.
- **Foreign key columns are not indexed automatically in PostgreSQL.** Index them if you join or filter on them, or delete from the parent table (otherwise each parent delete scans the child).
- A **composite** index on `(customer_id, ordered_at)` serves `WHERE customer_id = ?` and `WHERE customer_id = ? AND ordered_at > ?`, but not `WHERE ordered_at > ?` alone: the leading column must be used (the **leftmost prefix** rule). Put equality columns first, then the range column.
- A **covering** index contains every column a query needs, so the database never visits the table. In PostgreSQL, `INCLUDE` adds non-key columns: `CREATE INDEX ON orders (customer_id, ordered_at) INCLUDE (amount)`.
- A **partial** index covers only some rows: `CREATE INDEX ON orders (ordered_at) WHERE status = 'pending'` is tiny and serves the "open orders" screen.
- **Hash** indexes serve only equality and are rarely better than B-trees; GIN indexes serve JSONB, arrays and full-text search; BRIN indexes are tiny summaries for huge, naturally ordered tables (append-only logs).

You can watch the leftmost-prefix and covering rules in SQLite, which uses B-tree indexes too. This runs as is:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INT,
                     ordered_at TEXT, status TEXT, amount REAL);
CREATE INDEX ix_cust_time ON orders (customer_id, ordered_at, amount);
""")
def plan(sql):
    return db.execute("EXPLAIN QUERY PLAN " + sql).fetchall()[0][3]

print(plan("SELECT amount FROM orders WHERE customer_id = 1 AND ordered_at > '2026-03-01'"))
# SEARCH orders USING COVERING INDEX ix_cust_time (customer_id=? AND ordered_at>?)
print(plan("SELECT status FROM orders WHERE customer_id = 1"))
# SEARCH orders USING INDEX ix_cust_time (customer_id=?)
print(plan("SELECT amount FROM orders WHERE ordered_at > '2026-03-01'"))
# SCAN orders USING COVERING INDEX ix_cust_time
```

The first query is answered from the index alone. The second needs `status`, which is not in the index, so it looks rows up in the table. The third cannot seek on `ordered_at` because it is not the leading column, so it scans the whole index.

**The write cost.** Every index is updated on every insert, and on every update that touches its columns. A table with eight indexes does roughly nine writes per insert. On write-heavy tables (events, logs, queues), keep indexes to the ones queries prove they need; check unused ones with `pg_stat_user_indexes`.

**Partitioning.** PostgreSQL **declarative partitioning** splits one logical table into child tables by **range** (usually time), **list** (region, tenant) or **hash**. Queries that filter on the partition key skip other partitions (**partition pruning**), and old data is removed by dropping a partition instead of a slow `DELETE`:

```sql
CREATE TABLE events (
  event_id   BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  payload    JSONB,
  PRIMARY KEY (event_id, created_at)          -- must include the partition key
) PARTITION BY RANGE (created_at);

CREATE TABLE events_2026_11 PARTITION OF events
  FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
```

Partition only large tables (tens of gigabytes and up) with a clear partition key in most queries; thousands of tiny partitions slow planning.

**Storage layout.** PostgreSQL stores rows in a heap; an update writes a new row version and the old one is cleaned by `VACUUM`. Large values are moved out of line (TOAST). For tables with frequent updates, a lower `fillfactor` leaves free space in each page so updates that do not touch indexed columns can stay on the same page (HOT updates, which skip index maintenance).

**Migration-safe schema changes.** Production tables are in use while you change them. Safe habits in PostgreSQL:

- `CREATE INDEX CONCURRENTLY` builds an index without blocking writes (it cannot run inside a transaction block).
- Adding a nullable column, or one with a constant default, is a quick metadata change in PostgreSQL 11 and later; adding a column with a volatile default (`DEFAULT random()`) rewrites the table.
- Add a foreign key or check constraint `NOT VALID` first (new rows are checked), then `VALIDATE CONSTRAINT` later, which takes a weaker lock while scanning old rows.
- Set `lock_timeout` so a migration waiting behind a long query gives up instead of blocking everyone queued behind it.

The Modeling in practice group covers the full expand-and-contract process for renames and type changes.

> **Pitfall:** Forgetting to index a foreign key. Deleting one customer then scans the entire `orders` table to check for referencing rows, and joins from customers to orders scan too. It works in development and times out in production.

::: deeper
**Physical design on Spanner.** Spanner, Google Cloud's globally distributed relational database, splits tables by primary key range. A monotonically increasing key (timestamp or sequence) sends every insert to the same split, creating a **hotspot**. Use a UUID v4, a bit-reversed sequence or a hashed prefix as the leading key part. **Interleaving** stores child rows (order lines) physically with their parent (order) so parent-child reads are local. Both are physical decisions that do not change the logical model.

**Clustered versus heap storage.** MySQL's InnoDB stores rows inside the primary key B-tree (a **clustered index**), so the primary key choice decides physical order and every secondary index stores the primary key. Random UUID primary keys therefore fragment InnoDB tables, while PostgreSQL's heap is less affected. Know your engine before copying advice.

**Denormalising OLTP.** Occasionally an operational schema stores a derived value (an order's total, a counter of comments) to avoid expensive reads. That is a deliberate denormalisation: keep it correct with a transaction or trigger, and document it as derived.

**Constraint enforcement versus speed.** Some teams drop foreign keys "for speed". Measured overhead is usually small next to the cost of orphaned rows found months later. Keep them unless you have measured a real problem, and if you drop them, add a scheduled integrity check.

> **Interview:** "Queries filter by `customer_id` and a date range, and sort by date. Which index?" A composite B-tree on `(customer_id, ordered_at)`: equality column first so the seek narrows to one customer, then the range and sort column, so rows come back already ordered. Add `INCLUDE` columns if you want it covering.
:::

::: check
?? Does a composite index on `(customer_id, ordered_at)` help `WHERE ordered_at > '2026-03-01'` on its own? Why?
Not for seeking. The index is sorted by `customer_id` first, so dates are scattered across it; the database can at best scan the whole index.

?? Why should money be stored as `NUMERIC` rather than `DOUBLE PRECISION`?
Binary floating point cannot represent most decimal fractions exactly, so sums drift by tiny amounts. `NUMERIC` stores exact decimals.

?? How do you add a foreign key to a large, busy PostgreSQL table without blocking it for long?
Add the constraint with `NOT VALID` (enforced for new rows, quick), then run `ALTER TABLE ... VALIDATE CONSTRAINT` later, which checks existing rows under a weaker lock.
:::

::: resources
- Docs: [PostgreSQL: Indexes](https://www.postgresql.org/docs/current/indexes.html) · index types, multicolumn, partial and covering indexes, straight from the manual
- Docs: [PostgreSQL: Table partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html) · declarative range, list and hash partitioning and pruning
- Docs: [Use The Index, Luke!](https://use-the-index-luke.com/) · Markus Winand's free book on indexing for developers, across PostgreSQL, MySQL and others
- Docs: [Spanner schema design best practices](https://cloud.google.com/spanner/docs/schema-design) · hotspots, key choice and interleaving on Spanner
- Book: [High Performance MySQL, 4th edition](https://www.oreilly.com/library/view/high-performance-mysql/9781492080503/) · schema and index design, with the clustered-index view of storage
- Book: [PostgreSQL: Up and Running](https://www.oreilly.com/library/view/postgresql-up-and/9781491963401/) · a compact guide to PostgreSQL types, indexes and administration
:::

## Analytical modeling

The fundamentals group produced normalised models built for applications that write one row at a time. Analytics asks different questions: millions of rows at once, grouped and filtered by many descriptive attributes, often "as things were" at some past date. A model in third normal form answers these slowly and with many joins, and it usually throws history away (an address update overwrites the old address). Analytical modeling reshapes the data for reading and keeps the history.

This group starts with Ralph Kimball's dimensional modeling, the most widely used approach, and its concrete schema shapes (star, snowflake and galaxy). Then it covers how dimensions keep history (slowly changing dimensions), the fully flattened alternative (one big table), Data Vault for integration-heavy warehouses, anchor modeling for schemas that must evolve without breaking, and bi-temporal tables for data where corrections to the past must be auditable. The SQL recipes behind several of these (point-in-time joins, snapshots, bridge tables) are in Advanced SQL Modeling Patterns in the SQL section; here the focus is on design choices.

### Dimensional Modeling (Kimball)

::: plain
Dimensional modeling arranges analytical data the way people ask questions. Business questions sound like "how much did we sell, by product, by store, by week?". The "how much" is a measurement; the "by product, by store, by week" are the ways you slice it. Dimensional modeling stores measurements in **fact tables** and the descriptive context in **dimension tables**, so every question becomes "pick some measures, group by some dimension attributes". It is like a till receipt (the facts) plus the shop's product catalogue, store list and calendar (the dimensions).
:::

**Facts and dimensions.** A **fact table** records measurements of a business process: one row per event or per period, with foreign keys to dimensions and numeric **measures** (quantity, amount, duration). A **dimension table** holds the descriptive attributes used to filter, group and label: product name, brand and category; customer segment and city; date, month and fiscal quarter. Facts are narrow and very long (billions of rows); dimensions are wide (dozens to hundreds of columns) and comparatively short.

**The four-step design process.** Kimball's method designs one business process at a time:

1. **Select the business process**: a measurable activity, such as taking orders, shipping, or handling support tickets. Not a department.
2. **Declare the grain**: state exactly what one fact row represents. "One row per order line" or "one row per product per store per day". This is the most important decision in the design.
3. **Identify the dimensions**: the who, what, where, when and how that are true for one row at that grain: date, customer, product, store, promotion.
4. **Identify the facts**: the numeric measurements true at that grain: quantity, net amount, discount amount.

The grain comes before dimensions and facts because it decides which ones are valid. At order-line grain, product is a valid dimension and quantity a valid fact. At order grain, product is not (an order has many products), and adding it duplicates the order's totals.

> **Tip:** Choose the most **atomic** grain the source supports (order line, not daily summary). Atomic facts answer questions nobody has asked yet; summaries can always be built from them, but not the other way round.

**Types of fact table.**

| Type | One row per | Example | Updated? |
|---|---|---|---|
| Transaction | event | order line, payment, click | insert only |
| Periodic snapshot | entity per period | account balance per day, inventory per week | new rows each period |
| Accumulating snapshot | process instance, with milestone dates | order from placed to paid to shipped to delivered | row updated as milestones happen |
| Factless | event or coverage with no measures | student attended class; product was on promotion | insert only |

**Additivity.** A measure is **additive** if it can be summed across every dimension (sales amount). It is **semi-additive** if it sums across some dimensions but not time (an account balance: summing balances across accounts is fine, summing Monday's and Tuesday's balance is not; use the period-end value or an average). It is **non-additive** if it never sums (ratios, percentages, unit prices). Store the additive parts (numerator and denominator) and compute ratios at query time: a margin percentage is `SUM(margin) / SUM(revenue)`, never the sum or average of row-level percentages.

**Conformed dimensions and the bus matrix.** A **conformed dimension** means the same thing, with the same keys and attribute values, in every fact table that uses it. If the orders fact and the returns fact share one `dim_product`, you can compare sales and returns by category in one query (a **drill-across**). The **enterprise bus matrix** is the planning grid: business processes as rows, dimensions as columns, a mark where they meet:

| Business process | Date | Customer | Product | Store | Promotion | Carrier |
|---|---|---|---|---|---|---|
| Orders | x | x | x | x | x | |
| Shipments | x | x | x | x | | x |
| Returns | x | x | x | x | | |
| Inventory snapshot | x | | x | x | | |

Each row is one fact table to build; each column is one conformed dimension to build once and reuse. The matrix lets a team deliver one process at a time while guaranteeing the pieces fit together.

**Special dimensions.**

- **Degenerate dimension**: a dimension key with no attributes left, stored directly on the fact. The order number on an order-line fact is the classic case: useful for grouping lines into orders, but there is no `dim_order` with anything else in it.
- **Junk dimension**: a small table combining low-cardinality flags and indicators (payment type, is_gift, channel) instead of scattering many tiny dimensions or loose text flags across the fact. Three flags with 3, 2 and 4 values give at most 24 rows.
- **Role-playing dimension**: one physical dimension used several times in different roles. An accumulating order fact has `order_date_key`, `ship_date_key` and `delivery_date_key`, all pointing at `dim_date`, usually exposed through views named for each role.
- **Date dimension**: one row per day with every calendar attribute the business uses (day of week, month, fiscal period, holiday flag). Analysts should never need date arithmetic to find "fiscal Q3".

**Surrogate keys and the unknown member.** Dimensions get their own surrogate keys, separate from source natural keys, so that history (several versions of one customer) and multiple sources can coexist. Facts store the surrogate key. When a fact arrives whose dimension value is missing or not yet known, it points to a special **unknown** row (often key `-1`) rather than `NULL`, so inner joins never silently drop facts. Slowly changing dimensions, two topics on, decide how dimension rows change over time.

> **Pitfall:** Mixing grains in one fact table, for example order-line rows and order-level shipping charges. Summing shipping across lines multiplies it by the number of lines. Put order-level facts in an order-grain fact table, or allocate them down to lines with a documented rule.

::: deeper
**Kimball and Inmon.** Bill Inmon's approach builds a normalised, enterprise-wide warehouse first (the corporate information factory) and derives dimensional marts from it. Kimball's builds conformed dimensional marts directly, process by process, joined by the bus architecture. Most modern cloud warehouses look like a blend: a staging and integration layer (often in 3NF-like or Data Vault form) feeding Kimball-style marts. The Data warehousing section covers this history and the layering.

**Why dimensional models still matter in columnar warehouses.** BigQuery and Snowflake handle large joins well, and some teams flatten everything into one table (covered later in this group). Dimensional modeling still earns its place because it gives one conformed definition of each business entity, a declared grain per table, and a clear place to keep history. Wide tables are often built *from* a dimensional core.

**Factless facts for "what did not happen".** A coverage table recording which products were on promotion in which stores each day lets you answer "which promoted products sold nothing?" by comparing coverage to sales. You cannot answer that from the sales fact alone, because it only contains things that happened.

**Late-arriving facts.** A transaction that arrives days late must join to the dimension version that was valid at its event time, not the current one. The point-in-time join in the SQL section handles this at load time.

> **Interview:** "Declare the grain for a ride-hailing trips fact." One row per completed trip (or per trip request, if cancellations matter). Dimensions: date and time, rider, driver, vehicle, pickup zone, drop-off zone, product type. Facts: distance, duration, fare, tip, surge multiplier (non-additive, store it but do not sum it). Then mention an accumulating snapshot for the request-to-drop-off timeline if waiting times matter.
:::

::: check
?? What are the four steps of Kimball's design process, in order?
Select the business process, declare the grain, identify the dimensions, identify the facts.

?? Is an account balance additive? How should it be aggregated over time?
It is semi-additive: it sums across accounts but not across time. Over time use the closing value of the period (or an average), not a sum.

?? What is a degenerate dimension? Give an example.
A dimension key stored on the fact with no dimension table, because no attributes remain; the order number on an order-line fact.
:::

::: resources
- Docs: [Kimball dimensional modeling techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) · the official one-page definitions of every technique, from grain to junk dimensions
- Book: [The Data Warehouse Toolkit, 3rd edition](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) · Kimball and Ross; the definitive book, organised as industry case studies
- Article: [Kimball's four-step dimensional design process](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/four-4-step-design-process/) · the process in Kimball's own words
- Blog: [Building a Kimball dimensional model with dbt](https://docs.getdbt.com/blog/kimball-dimensional-model) · a hands-on walk-through from source tables to facts and dimensions in dbt
- Book: [Agile Data Warehouse Design](https://www.oreilly.com/library/view/agile-data-warehouse/9780471777090/) · Lawrence Corr; collaborative requirements and the BEAM technique for dimensional models
:::

### Star, snowflake and galaxy schemas in practice

::: plain
A **star schema** is one fact table in the middle with dimension tables around it, each joined by a single key: drawn on paper it looks like a star. A **snowflake schema** splits dimensions into further normalised tables (product points to category, which points to department), so the points of the star branch out like a snowflake. A **galaxy** (or fact constellation) is several fact tables sharing the same dimensions. They are three ways to lay out the same dimensional ideas, trading simplicity of queries against duplication.
:::

[[fig:modeling_star]]

**The star.** The figure shows an order-lines star: `fct_order_lines` at the grain of one order line, joined to `dim_date`, `dim_customer`, `dim_product` and `dim_store`. Every analytical question is the same shape: join the fact to the dimensions you need, filter, group, sum. A small version runs in SQLite as is:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE dim_date (date_key INT PRIMARY KEY, full_date TEXT, month TEXT, is_weekend INT);
INSERT INTO dim_date VALUES (20260301,'2026-03-01','2026-03',1),(20260302,'2026-03-02','2026-03',0),
  (20260303,'2026-03-03','2026-03',0),(20260401,'2026-04-01','2026-04',0);
CREATE TABLE dim_product (product_key INT PRIMARY KEY, sku TEXT, name TEXT, category TEXT);
INSERT INTO dim_product VALUES (1,'T-01','tea','drinks'),(2,'C-01','cake','bakery'),(3,'J-01','jam','pantry');
CREATE TABLE dim_customer (customer_key INT PRIMARY KEY, customer_id INT, name TEXT, city TEXT);
INSERT INTO dim_customer VALUES (-1,NULL,'unknown','unknown'),(11,1,'Asha','Pune'),(31,3,'Chen','Pune'),(21,2,'Bo','Oslo');
CREATE TABLE fct_order_lines (date_key INT, product_key INT, customer_key INT,
  order_id INT, quantity INT, net_amount REAL);
INSERT INTO fct_order_lines VALUES
  (20260301,1,11,101,2,20.0),(20260301,2,11,101,1,20.0),
  (20260302,1,11,103,1,10.0),(20260302,3,11,103,2,12.0),
  (20260303,2,31,104,3,60.0),(20260401,1,21,108,4,40.0),(20260401,3,-1,109,1,6.0);
""")
for row in db.execute("""
SELECT d.month, p.category, SUM(f.quantity) AS units, SUM(f.net_amount) AS revenue
FROM fct_order_lines AS f
JOIN dim_date AS d ON d.date_key = f.date_key
JOIN dim_product AS p ON p.product_key = f.product_key
GROUP BY d.month, p.category
ORDER BY d.month, revenue DESC"""):
    print(row)
# ('2026-03', 'bakery', 4, 80.0)
# ('2026-03', 'drinks', 3, 30.0)
# ('2026-03', 'pantry', 2, 12.0)
# ('2026-04', 'drinks', 4, 40.0)
# ('2026-04', 'pantry', 1, 6.0)
```

Order 109 has no known customer, so it points at the unknown member (`-1`) instead of `NULL`, and still counts in every total. `order_id` on the fact is a degenerate dimension.

**Designing the fact table.** A good fact table contains only:

- foreign keys to dimensions (surrogate keys, never `NULL`; use the unknown member);
- degenerate dimensions (order number, invoice number);
- measures at the declared grain, in base units (amount in the transaction currency plus a converted reporting-currency amount, if both are needed);
- a few audit columns (`loaded_at`, source batch).

In BigQuery, partition the fact by its main date (`PARTITION BY order_date`) and cluster by the most filtered keys:

```sql
CREATE TABLE mart.fct_order_lines (
  order_date     DATE NOT NULL,
  date_key       INT64 NOT NULL,
  customer_key   INT64 NOT NULL,
  product_key    INT64 NOT NULL,
  store_key      INT64 NOT NULL,
  order_id       STRING NOT NULL,          -- degenerate dimension
  line_number    INT64 NOT NULL,
  quantity       INT64 NOT NULL,
  net_amount     NUMERIC NOT NULL,
  discount_amount NUMERIC NOT NULL,
  PRIMARY KEY (order_id, line_number) NOT ENFORCED
)
PARTITION BY order_date
CLUSTER BY product_key, customer_key;
```

BigQuery accepts primary and foreign keys only as `NOT ENFORCED`; it does not check them, but the optimizer can use them (for example to skip a join whose result is not needed). You must test uniqueness yourself.

**Designing dimension tables.** Dimensions should be wide, descriptive and friendly:

- Decode codes into words (`status_code = 'P'` becomes `status = 'pending'`), so every analyst does not repeat the mapping.
- Flatten hierarchies into columns (`product_name`, `brand`, `category`, `department`) so a roll-up is a `GROUP BY` of one column.
- Include useful derived attributes (`age_band`, `is_first_order_customer`, `days_since_signup` if refreshed daily).
- Keep one row per member (or per version, for type 2 history) with a surrogate key and the natural key.

**When to snowflake.** Snowflaking (normalising `dim_product` into `dim_product`, `dim_category` and `dim_department`) saves a little storage and makes the category list a table of its own. It costs an extra join per level in every query and makes the model harder to read. In columnar warehouses the storage saving is negligible (repeated category names compress very well), so the default is a star. Snowflake when:

- a hierarchy level is shared by several dimensions (a geography table used by customer, store and supplier);
- a level has many attributes of its own and changes independently (a category with a manager, budget and history);
- a BI tool or semantic layer requires it.

**Galaxy schemas.** A real warehouse has many fact tables sharing conformed dimensions: orders, shipments, returns and inventory snapshots all use `dim_date`, `dim_product` and `dim_store`. To compare them, **drill across**: aggregate each fact to the common dimensions separately, then join the results:

```sql
WITH sales AS (
  SELECT product_key, SUM(quantity) AS units_sold
  FROM mart.fct_order_lines
  WHERE order_date BETWEEN '2026-03-01' AND '2026-03-31'
  GROUP BY product_key
),
returns AS (
  SELECT product_key, SUM(quantity) AS units_returned
  FROM mart.fct_returns
  WHERE return_date BETWEEN '2026-03-01' AND '2026-03-31'
  GROUP BY product_key
)
SELECT p.category,
       SUM(s.units_sold) AS units_sold,
       SUM(COALESCE(r.units_returned, 0)) AS units_returned,
       SAFE_DIVIDE(SUM(COALESCE(r.units_returned, 0)), SUM(s.units_sold)) AS return_rate
FROM sales AS s
LEFT JOIN returns AS r USING (product_key)
JOIN mart.dim_product AS p USING (product_key)
GROUP BY p.category;
```

> **Pitfall:** Joining two fact tables directly on their shared dimension keys (`fct_order_lines JOIN fct_returns ON product_key`). Every sale row matches every return row of the same product, multiplying both totals. Always aggregate each fact to the common grain first, then join.

::: deeper
**Outriggers.** A dimension that references another dimension (customer pointing to a `dim_date` row for the first purchase date) is an **outrigger**. Kimball allows a few; many of them signal a snowflake in disguise.

**Bridge tables for multi-valued dimensions.** When one fact row relates to several members of a dimension (a bank account with several holders, a patient visit with several diagnoses), a **bridge table** sits between them, often with a weighting factor so totals can be allocated without double counting. The SQL section's Advanced SQL Modeling Patterns shows the query.

**Aggregate fact tables.** For dashboards that only ever read monthly category totals, an aggregate fact (or a materialized view) at that grain makes queries cheap. Keep it derived from the atomic fact, never loaded separately, so the numbers cannot drift.

**Star schemas and BI tools.** Most BI tools and semantic layers (Looker, Power BI, the dbt Semantic Layer) work best with a star: one-to-many relationships from dimensions to facts and no ambiguous join paths. A snowflake with optional paths, or two facts joined directly, is where tools start generating wrong totals.

> **Interview:** "Star or snowflake for BigQuery?" Star by default: columnar compression makes repeated dimension attributes cheap, each removed join simplifies queries and plans, and BI tools handle stars best. Snowflake only for hierarchy levels shared across dimensions or with their own life cycle.
:::

::: check
?? Why does a star schema use an unknown member row instead of `NULL` foreign keys on the fact?
So inner joins to the dimension never drop facts, and the facts appear under a visible "unknown" label instead of disappearing from totals.

?? Two fact tables share `product_key`. How do you compare their measures by category correctly?
Drill across: aggregate each fact separately to `product_key` (or category), then join the aggregated results and the dimension. Never join the raw fact tables to each other.

?? When is snowflaking a dimension worth it in a columnar warehouse?
When a hierarchy level is shared by several dimensions, has many attributes and its own change history, or a tool requires it. Storage savings alone are rarely worth the extra joins.
:::

::: resources
- Docs: [Kimball techniques: fact and dimension table design](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) · fact table types, outriggers, bridges and drill-across, defined briefly
- Book: [Star Schema: The Complete Reference](https://www.oreilly.com/library/view/star-schema-the/9780071744324/) · Christopher Adamson; the most thorough book on star schema design choices
- Docs: [BigQuery primary and foreign keys](https://cloud.google.com/bigquery/docs/primary-foreign-keys) · unenforced key constraints and how the optimizer uses them
- Blog: [Building a Kimball dimensional model with dbt](https://docs.getdbt.com/blog/kimball-dimensional-model) · building the star from staging models step by step
- Article: [Star schema](https://en.wikipedia.org/wiki/Star_schema) · a short reference with the classic diagram and a comparison with snowflake schemas
:::

### Slowly Changing Dimensions (SCD)

::: plain
Dimension attributes change: customers move, products change category, employees change department. A **slowly changing dimension** (SCD) technique decides what the warehouse does when that happens. You can overwrite the old value (and history is lost), keep every version as a separate row (and old facts still show the old value), or keep just the previous value in an extra column. It is the difference between a phone's contacts app, which shows only the current address, and a passport, which keeps a record of every visa ever issued.
:::

**The question to ask.** For each attribute: when it changes, should facts recorded before the change be reported with the **old** value or the **new** value? "Revenue by customer city" in a year when Asha moved: should her March orders count for Pune (where she lived) or Mumbai (where she lives now)? Both answers are legitimate; the business must choose per attribute, and the choice decides the SCD type.

**The types.** Kimball numbers the techniques 0 to 7:

| Type | What happens on change | History | Typical use |
|---|---|---|---|
| 0 | keep the original value forever | none (original only) | date of birth, original signup channel |
| 1 | overwrite in place | lost | corrections, attributes nobody reports historically |
| 2 | add a new row with a new surrogate key; close the old row | full | city, segment, product category |
| 3 | add a column with the previous value | one prior value | "old territory" during a reorganisation |
| 4 | move fast-changing attributes to a **mini-dimension** | banded history | age band, income band, credit score band |
| 5 | type 4 mini-dimension plus a type 1 pointer to its current row on the base dimension | current and historical bands | report by current band and band at the time |
| 6 | type 2 rows plus type 1 "current" columns on every row | full, plus current on each row | report by historical or current value without a self-join |
| 7 | facts carry both the type 2 surrogate key and the durable natural key | full, with a current-view join | let users choose "as was" or "as is" per query |

Types 1 and 2 cover most real needs; type 6 and 7 are conveniences for when users need both views. Outside Kimball's list, "type 4" sometimes means a separate history table next to a current table; check which meaning a colleague has in mind.

**Type 2 mechanics.** Each version row has:

- a new **surrogate key** (`customer_sk`), so facts can point to one version;
- the **durable** (natural) key (`customer_id`) shared by all versions;
- `valid_from` and `valid_to` (half-open: from included, to excluded), with a far-future `valid_to` such as `9999-12-31` on the current row;
- `is_current` for convenience;
- often a **row hash** of the tracked attributes, to detect change cheaply.

[[fig:modeling_scd2]]

Customer 42 moved from Pune to Mumbai on 1 June. The March order was loaded with `customer_sk = 1`, the September order with `customer_sk = 3`, so "revenue by city" puts each where it belongs without any date logic at query time. The load itself is two steps: close the current row of every changed customer, then insert a new row for every new or changed customer. This runs in SQLite as is and produces exactly the rows in the figure:

```python
import sqlite3, hashlib

def row_hash(city, segment):
    return hashlib.sha256(f"{city}|{segment}".encode()).hexdigest()[:8]

db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE dim_customer (customer_sk INTEGER PRIMARY KEY, customer_id INT, city TEXT,
  segment TEXT, row_hash TEXT, valid_from TEXT, valid_to TEXT, is_current INT);
CREATE TABLE stg_customer (customer_id INT, city TEXT, segment TEXT, row_hash TEXT);
""")
db.create_function("row_hash", 2, row_hash)

def load(batch, load_date):
    db.execute("DELETE FROM stg_customer")
    db.executemany("INSERT INTO stg_customer VALUES (?, ?, ?, row_hash(?, ?))",
                   [(c, city, seg, city, seg) for c, city, seg in batch])
    # 1. close the current version of every customer whose attributes changed
    db.execute("""
      UPDATE dim_customer SET valid_to = ?, is_current = 0
      WHERE is_current = 1 AND EXISTS (
        SELECT 1 FROM stg_customer s
        WHERE s.customer_id = dim_customer.customer_id
          AND s.row_hash <> dim_customer.row_hash)""", (load_date,))
    # 2. insert a new version for new and changed customers
    db.execute("""
      INSERT INTO dim_customer (customer_id, city, segment, row_hash, valid_from, valid_to, is_current)
      SELECT s.customer_id, s.city, s.segment, s.row_hash, ?, '9999-12-31', 1
      FROM stg_customer s
      WHERE NOT EXISTS (SELECT 1 FROM dim_customer d
                        WHERE d.customer_id = s.customer_id AND d.is_current = 1)""",
      (load_date,))

load([(42, "Pune", "retail"), (7, "Oslo", "retail")], "2026-01-01")
load([(42, "Mumbai", "retail"), (7, "Oslo", "retail")], "2026-06-01")   # 42 moved
load([(42, "Mumbai", "retail"), (7, "Oslo", "retail")], "2026-06-02")   # re-run: no change

for r in db.execute("SELECT customer_sk, customer_id, city, valid_from, valid_to, is_current "
                    "FROM dim_customer ORDER BY customer_id, valid_from"):
    print(r)
# (2, 7, 'Oslo', '2026-01-01', '9999-12-31', 1)
# (1, 42, 'Pune', '2026-01-01', '2026-06-01', 0)
# (3, 42, 'Mumbai', '2026-06-01', '9999-12-31', 1)
```

The third load changes nothing: the hashes match, so no row is closed and no row is inserted. That makes the load **idempotent** (safe to re-run), which every warehouse load must be. In BigQuery the same two steps fit into one `MERGE`, using a common trick: feed changed rows in twice, once with their key (to close the old version) and once with a `NULL` key (which never matches, so it is inserted):

```sql
MERGE dw.dim_customer AS d
USING (
  SELECT s.customer_id AS join_key, s.* FROM staging.customers AS s
  UNION ALL
  SELECT NULL AS join_key, s.*                -- second copy of changed rows only
  FROM staging.customers AS s
  JOIN dw.dim_customer AS cur
    ON cur.customer_id = s.customer_id AND cur.is_current
  WHERE cur.row_hash != s.row_hash
) AS src
ON d.customer_id = src.join_key AND d.is_current
WHEN MATCHED AND d.row_hash != src.row_hash THEN
  UPDATE SET valid_to = src.loaded_at, is_current = FALSE
WHEN NOT MATCHED THEN
  INSERT (customer_sk, customer_id, city, segment, row_hash, valid_from, valid_to, is_current)
  VALUES (GENERATE_UUID(), src.customer_id, src.city, src.segment, src.row_hash,
          src.loaded_at, TIMESTAMP '9999-12-31 00:00:00 UTC', TRUE);
```

**dbt snapshots.** In dbt, a **snapshot** implements type 2 for you. You point it at a source, give the unique key and a change-detection strategy (`timestamp`, using an `updated_at` column, or `check`, comparing listed columns), and each run adds `dbt_valid_from` and `dbt_valid_to` columns. By default the current row has `dbt_valid_to` set to `NULL`; the `dbt_valid_to_current` setting replaces that with a far-future date, and `hard_deletes` decides what happens when a source row disappears (`ignore`, `invalidate` or `new_record`).

```yaml
snapshots:
  - name: customers_snapshot
    relation: source('shop', 'customers')
    config:
      unique_key: customer_id
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "cast('9999-12-31' as timestamp)"
      hard_deletes: invalidate
```

A snapshot captures what the source looked like each time it ran, so changes between two runs are invisible. If the source is overwritten twice in a day and you snapshot daily, one version is lost. For complete history, capture changes at the source with change data capture (the Data ingestion section) and build the dimension from the change log.

> **Pitfall:** Overlapping or gapped validity intervals. If one row ends on 1 June and the next starts on 2 June, an order on 1 June matches no version; if both include 1 June, it matches two and is double counted. Use half-open intervals and test that each customer's versions tile time exactly.

::: deeper
**Which attributes are type 2?** Make history a per-attribute decision. Tracking every column as type 2 creates a new version whenever a phone number is corrected, so a dimension can grow by millions of rows a day. A common split: type 2 for attributes used in historical reporting (city, segment, plan), type 1 for everything else (phone, email spelling corrections). When a type 1 attribute changes, update it on all versions so they agree.

**Rapidly changing attributes.** An attribute that changes monthly for millions of customers (credit score) makes a type 2 dimension explode. Kimball's answer is the **mini-dimension** (type 4): band the attribute (score 600 to 649) and store the band combinations in a small separate dimension whose key goes on the fact. History lives on the fact rows, and the customer dimension stays stable.

**Effective dating facts.** The surrogate key on the fact is resolved at load time by the point-in-time join (`event_time >= valid_from AND event_time < valid_to`). If a fact arrives before its dimension version (a late-arriving dimension), insert an inferred member, as described in the SQL section, and correct it when the real data lands.

**Backdated changes.** If the source tells you on 10 June that Asha actually moved on 1 May, a pure type 2 design must split the existing version and re-point facts loaded between 1 May and 10 June. That is painful, and it is exactly the case bi-temporal models (later in this group) handle cleanly.

> **Interview:** "A customer changes segment. Revenue by segment for last year must not change when we re-run it, but the account team wants to see each customer's current segment too. How?" Type 2 on segment for the historical report, plus a current-segment column on every row (type 6), or the durable key on the fact (type 7) to join to the current row.
:::

::: check
?? Which SCD type loses history, and which keeps every version?
Type 1 overwrites and loses history; type 2 adds a row per version and keeps all of it.

?? Why do type 2 dimensions use half-open validity intervals with a far-future end date?
Half-open intervals (from included, to excluded) let consecutive versions share a boundary without gaps or overlaps, so every point in time matches exactly one version; the far-future end date avoids `NULL` handling in range joins.

?? What can a daily dbt snapshot miss?
Any change that is overwritten again before the next run: it only sees the state at each run, not every change in between.
:::

::: resources
- Docs: [Kimball: slowly changing dimension techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/) · the type 2 definition, with links to types 0 to 7 alongside it
- Docs: [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots) · strategies, configuration, `dbt_valid_to_current` and hard-delete handling
- Book: [The Data Warehouse Toolkit, 3rd edition](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) · Kimball and Ross; chapter 5 covers every SCD type with examples
- Article: [Implementing slowly changing dimensions in data warehouses](https://www.sqlshack.com/implementing-slowly-changing-dimensions-scds-in-data-warehouses/) · SQLShack's walk-through of types 1, 2 and 3 in SQL
- Article: [Slowly changing dimension](https://en.wikipedia.org/wiki/Slowly_changing_dimension) · a compact reference table of every type with sample rows
:::

### OBT Pattern & Denormalization

::: plain
**One big table** (OBT) means joining the facts and all their dimension attributes ahead of time into a single, very wide table, so analysts never write a join. It is like a printed timetable that already lists each train's platform, operator and stops, instead of three separate lists you must cross-reference. It is fast and easy to use, but every time a platform changes you must reprint the whole timetable, and it is easy to print it wrong.
:::

**What an OBT looks like.** Start from the star in the previous topic and pre-join it:

```sql
CREATE OR REPLACE TABLE mart.obt_order_lines
PARTITION BY order_date
CLUSTER BY category, customer_city AS
SELECT
  f.order_date, f.order_id, f.line_number, f.quantity, f.net_amount,
  p.sku, p.product_name, p.brand, p.category,
  c.customer_id, c.segment AS customer_segment, c.city AS customer_city,
  s.store_name, s.region AS store_region, s.channel
FROM mart.fct_order_lines AS f
JOIN mart.dim_product  AS p USING (product_key)
JOIN mart.dim_customer AS c USING (customer_key)
JOIN mart.dim_store    AS s USING (store_key);
```

Every question is now a single-table query: `SELECT category, SUM(net_amount) FROM mart.obt_order_lines GROUP BY category`.

**Why it works well in columnar warehouses.** BigQuery, Snowflake and ClickHouse store each column separately and read only the columns a query names. A table with 200 columns costs nothing extra for a query that reads 4 of them. Repeated values (the category "bakery" on a million rows) compress to almost nothing. No joins means simpler plans, no join-related skew, and fewer ways for an analyst to get a join wrong. BI tools that are weak at modeling, notebooks and machine learning feature extraction all prefer one wide table.

**What it costs.**

| Concern | Star schema | One big table |
|---|---|---|
| Query simplicity | joins needed | no joins |
| Dimension change (product renamed) | update one dimension row | rebuild or update every affected fact row |
| History semantics | explicit: type 1 or type 2 per attribute | frozen at build time unless rebuilt |
| Several facts (orders, returns) | share conformed dimensions | one OBT per fact, attributes duplicated |
| Storage | small | larger, but compresses well |
| Grain clarity | each table states its grain | easy to mix grains by adding columns |

The biggest risk is **grain confusion and fan-out**. If someone adds shipment details to an order-level OBT, orders with two parcels appear twice and every sum of order amount doubles. You can see the effect in a few lines (runs as is):

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE orders (order_id INT, amount REAL);
INSERT INTO orders VALUES (101, 40.0), (103, 22.0);
CREATE TABLE shipments (shipment_id INT, order_id INT, carrier TEXT);
INSERT INTO shipments VALUES (1, 101, 'DHL'), (2, 101, 'DHL'), (3, 103, 'UPS');
""")
print(db.execute("SELECT SUM(amount) FROM orders").fetchone()[0])                  # 62.0
print(db.execute("""SELECT SUM(o.amount) FROM orders o
                    JOIN shipments s ON s.order_id = o.order_id""").fetchone()[0])   # 102.0
```

Order 101 shipped in two parcels, so its 40.0 is counted twice. A wide table built with that join silently reports 102.0 of revenue.

**Nested and repeated fields: the middle ground.** BigQuery can store one row per order with its lines and shipments as arrays of structs inside it. The grain stays one order, the children travel with it, and nothing fans out until you deliberately `UNNEST`:

```sql
CREATE TABLE mart.orders_nested (
  order_id   STRING,
  order_date DATE,
  customer   STRUCT<customer_id STRING, segment STRING, city STRING>,
  lines      ARRAY<STRUCT<sku STRING, category STRING, quantity INT64, net_amount NUMERIC>>,
  shipments  ARRAY<STRUCT<shipment_id STRING, carrier STRING, shipped_at TIMESTAMP>>
)
PARTITION BY order_date;

-- order-level totals: no fan-out, the lines are summed inside each order
SELECT customer.city, SUM((SELECT SUM(l.net_amount) FROM UNNEST(lines) AS l)) AS revenue
FROM mart.orders_nested
GROUP BY customer.city;
```

Google's BigQuery documentation recommends nested and repeated fields for data that is naturally hierarchical and read together. They are less convenient when children are queried on their own or change independently.

**Maintenance patterns.** Treat the OBT as a **derived** layer:

- keep the star (or another conformed core) as the source of truth, and build OBTs from it in the last transformation step;
- rebuild fully when dimensions are small and facts fit a daily rebuild; otherwise rebuild only recent partitions and accept that older rows keep the attribute values they were built with, documented as such;
- state the grain in the table description and test uniqueness of the grain key;
- build one OBT per use case (a sales dashboard, a churn model), not one enterprise-wide table with every column.

> **Pitfall:** Treating the OBT as the only model. When "customer segment" is redefined, every OBT that copied the old segment must be found and rebuilt; with a star, you change one dimension. Without the conformed core, OBTs drift apart and the platform ends up with five different "revenue" numbers.

::: deeper
**Which history does an OBT show?** An OBT built from a type 2 dimension via the fact's surrogate key shows the attribute as it was at the time of each fact. An OBT built by joining to the current dimension row shows today's value, and becomes stale the moment it is built. Decide deliberately, and name columns accordingly (`customer_city_at_order` versus `customer_city_current`).

**Cost.** In BigQuery's on-demand pricing you pay for bytes read, so a well-clustered OBT can be cheaper per query than a star (no dimension scans), but each rebuild rewrites the whole table. For large facts, an incremental build of recent partitions plus a nightly job that refreshes changed dimension attributes is a common compromise. Materialized views can maintain some joins automatically; check the current BigQuery limits on joins in materialized views before relying on them.

**Wide tables in other engines.** ClickHouse and Druid perform best with denormalised tables, because their joins are weaker than BigQuery's; the Real-time OLAP engines section shows this. For feature stores, wide tables with point-in-time correctness are the norm; see Data engineering for AI and ML.

> **Interview:** "Would you build a star or OBT for a new BigQuery mart?" Both: a star as the governed core (conformed dimensions, explicit history), and OBTs or nested tables derived from it for the dashboards and tools that benefit. Mention grain, fan-out and how attribute changes propagate.
:::

::: check
?? Why is the extra width of an OBT cheap in a columnar warehouse?
Queries read only the columns they reference, and repeated values compress very well, so unused columns and duplicated attributes cost little to read or store.

?? An order-level OBT is extended with one column per shipment. What goes wrong?
Orders with several shipments get several rows, so order-level measures are counted once per shipment (fan-out), inflating totals.

?? How do BigQuery nested and repeated fields avoid fan-out?
The child rows (lines, shipments) are stored as arrays inside each parent row, so the table keeps one row per order; you only multiply rows when you explicitly `UNNEST`.
:::

::: resources
- Blog: [Star schema vs OBT for data warehouse performance](https://www.fivetran.com/blog/star-schema-vs-obt) · Fivetran's benchmark comparing the two on several cloud warehouses
- Docs: [BigQuery: nested and repeated fields](https://cloud.google.com/bigquery/docs/nested-repeated) · when and how to denormalise with `STRUCT` and `ARRAY`
- Docs: [BigQuery: optimize query computation](https://cloud.google.com/bigquery/docs/best-practices-performance-compute) · Google's guidance on denormalisation, joins and nested data for performance
- Article: [One big table: the good, the bad and the ugly](https://medium.com/@rbhanot4739/one-big-table-the-good-the-bad-and-the-ugly-58b2c47745e2) · a practitioner's view of OBT trade-offs and maintenance
- Book: [Star Schema: The Complete Reference](https://www.oreilly.com/library/view/star-schema-the/9780071744324/) · Adamson; the chapters on derived and aggregate tables apply directly to OBTs
:::

### Data Vault 2.0 Modeling

::: plain
Data Vault is a way to build the integration layer of a warehouse so that new sources can be added without redesigning what exists. It splits every business concept into three kinds of table: **hubs** (just the list of business keys, like a register of customer numbers), **links** (which keys are connected, like "customer C-42 placed order 9001") and **satellites** (descriptive details with full history, like each version of a customer's address). Think of a library: the catalogue of book IDs, the loan records connecting readers and books, and the history of each book's condition, all kept separately so each can grow on its own.
:::

[[fig:modeling_vault]]

**The three building blocks.**

- **Hub**: one row per unique **business key** (the identifier the business uses: customer number, order number, SKU). Columns: a **hash key** computed from the business key, the business key itself, `load_ts` (when the warehouse first saw it) and `record_source` (which system it came from). Hubs never change once a row is inserted.
- **Link**: one row per unique relationship between hubs (customer placed order, order contains product). Columns: its own hash key (from the combined business keys), the hash keys of the hubs it connects, `load_ts` and `record_source`. Links are always many-to-many, which makes them robust when a one-to-many rule changes.
- **Satellite**: the descriptive attributes of a hub or a link, with history. Columns: the parent hash key, `load_ts`, `record_source`, a **hash diff** (a hash of all the attributes, used to detect change) and the attributes. A new row is inserted only when the hash diff changes. Satellites are usually split by source system and by rate of change (`sat_customer_crm`, `sat_customer_web`).

**Hash keys.** Data Vault 2.0 replaced sequence surrogate keys with hashes of the business key (often MD5 or SHA-1, increasingly SHA-256). Because the key is computed from the data, every table can be loaded in parallel with no lookups: the link loader computes the customer hash key itself instead of waiting for the hub. The business key is normalised first (trimmed, upper-cased, with a delimiter between parts of composite keys) so the same key always hashes the same way. This runs as is:

```python
import hashlib

def hash_key(*parts):
    """Normalise business key parts, join with a delimiter, hash with MD5."""
    return hashlib.md5("||".join(p.strip().upper() for p in parts).encode()).hexdigest()

print(hash_key("C-0042"))               # e274f4a93131ad0db177bd678d37f2e0
print(hash_key(" c-0042 "))             # e274f4a93131ad0db177bd678d37f2e0  (same customer)
print(hash_key("C-0042", "ORD-9001"))   # f3ecb1e4abccf1a36288c70f22fa68b9  (link key)
```

**Loading is insert-only.** Each load does three simple things, each of which can run in parallel with the others:

```sql
-- hub: insert business keys not seen before (BigQuery)
INSERT INTO raw_vault.hub_customer (hk_customer, customer_bk, load_ts, record_source)
SELECT DISTINCT
  TO_HEX(MD5(UPPER(TRIM(s.customer_number)))), UPPER(TRIM(s.customer_number)),
  CURRENT_TIMESTAMP(), 'crm'
FROM staging.crm_customers AS s
WHERE NOT EXISTS (
  SELECT 1 FROM raw_vault.hub_customer AS h
  WHERE h.hk_customer = TO_HEX(MD5(UPPER(TRIM(s.customer_number))))
);

-- satellite: insert a row only when the attributes differ from the latest one
INSERT INTO raw_vault.sat_customer_crm (hk_customer, load_ts, record_source, hash_diff, name, city)
SELECT s.hk_customer, CURRENT_TIMESTAMP(), 'crm', s.hash_diff, s.name, s.city
FROM (
  SELECT TO_HEX(MD5(UPPER(TRIM(customer_number)))) AS hk_customer,
         TO_HEX(MD5(CONCAT(IFNULL(name, ''), '||', IFNULL(city, '')))) AS hash_diff,
         name, city
  FROM staging.crm_customers
) AS s
LEFT JOIN (
  SELECT hk_customer, hash_diff
  FROM raw_vault.sat_customer_crm
  QUALIFY ROW_NUMBER() OVER (PARTITION BY hk_customer ORDER BY load_ts DESC) = 1
) AS latest USING (hk_customer)
WHERE latest.hash_diff IS NULL OR latest.hash_diff != s.hash_diff;
```

Nothing is ever updated or deleted in the raw vault, so the vault is a complete, auditable record of everything every source ever said.

**Raw vault, business vault and marts.** The **raw vault** applies no business rules: it stores source data as received, keyed and historised. The **business vault** adds derived structures: computed satellites (business rules applied), **point-in-time (PIT) tables** and **bridge tables**. Information marts, usually Kimball star schemas or OBTs, are built on top for users. Nobody queries the raw vault directly for reporting.

- A **PIT table** stores, for each hub key and each snapshot date, the `load_ts` of the satellite row valid on that date in each satellite. Building a type 2 dimension then becomes a set of equi-joins instead of many range joins.
- A **bridge table** pre-joins hubs and links along a commonly used path (customer to order to product) to speed up mart builds.

**When Data Vault fits.**

| Good fit | Poor fit |
|---|---|
| many source systems for the same concepts (several CRMs after acquisitions) | one or two clean sources |
| strict audit and lineage requirements (banking, insurance, public sector) | small teams that need a mart next week |
| sources and business rules change often | stable domain, few changes |
| large teams loading in parallel | ad hoc analytics |

The cost is table count and query complexity: a model with 20 entities may have 20 hubs, 25 links and 60 satellites, and every mart needs many joins. Automation is close to mandatory. In the dbt ecosystem, packages such as AutomateDV and datavault4dbt generate hubs, links, satellites and PIT tables from metadata.

> **Pitfall:** Choosing the wrong business key, for example a source system's internal row ID instead of the identifier the business uses. Different sources then create different hub rows for the same customer, and the vault cannot integrate them. Spend the time to find the true business key, and use same-as links when sources really do disagree.

::: deeper
**More structures.** **Same-as links** record that two hub keys refer to the same real thing (the output of master data matching). **Hierarchical links** connect a hub to itself (product to parent product). **Multi-active satellites** hold several valid rows at once for one key (several phone numbers). **Effectivity satellites** on links record when a relationship started and ended (a customer's assignment to an account manager). **Non-historised links** (transactional links) store immutable events such as payments.

**Hash collisions.** MD5 produces 128-bit values; the chance of an accidental collision among billions of keys is negligible for integration purposes, though MD5 is not secure against deliberate collisions. Some teams use SHA-256 for policy reasons, at a storage cost. Whatever you choose, use it everywhere: changing the hash function later means re-keying the whole vault.

**Data Vault on columnar warehouses.** Data Vault was designed in the era of row-store databases. On BigQuery, the many joins cost more than in a star, so PIT and bridge tables, partitioning satellites by `load_ts` and clustering on the hash key matter for performance. Storing hash keys as `BYTES` (16 bytes for MD5) rather than hexadecimal strings (32 characters) halves their size.

**Data Vault and lakehouses.** The insert-only pattern maps well to append-only lakehouse tables (Iceberg, Delta). The raw vault can live in Iceberg tables in Cloud Storage while marts are served from BigQuery.

> **Interview:** "Why does Data Vault model all relationships as many-to-many links?" Because cardinality rules change: an order that "always" has one customer may later have a buyer and a payer. A link absorbs that change without restructuring hubs or reloading history, at the cost of more joins.
:::

::: check
?? What do hubs, links and satellites each store?
Hubs store unique business keys; links store unique relationships between hub keys; satellites store descriptive attributes of a hub or link with their full history.

?? Why does Data Vault 2.0 use hash keys instead of sequence numbers?
A hash computed from the business key can be calculated independently by every loader, so hubs, links and satellites load in parallel without looking up keys in other tables.

?? What is a PIT table for?
For each key and snapshot date it records which satellite row was valid in each satellite, so building dimensions as of a date needs equi-joins instead of many range joins.
:::

::: resources
- Book: [Building a Scalable Data Warehouse with Data Vault 2.0](https://www.oreilly.com/library/view/building-a-scalable/9780128025109/) · Linstedt and Olschimke; the reference book, from modeling to loading patterns
- Article: [What is a Data Vault?](https://www.databricks.com/glossary/data-vault) · a short overview of hubs, links, satellites and the raw and business vault
- Docs: [AutomateDV](https://automate-dv.readthedocs.io/) · dbt package documentation with worked hub, link, satellite and PIT examples
- Repo: [datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt) · Scalefree's dbt package for Data Vault 2.0, with macros for every entity type
- Docs: [Data Vault Alliance](https://www.datavaultalliance.com/) · the standards body around Dan Linstedt, with articles and training
:::

### Anchor Modeling & Temporal Patterns

::: plain
Anchor modeling takes normalisation to its limit: every attribute of every entity gets its own small table, with its own history. A customer is just an identity number (the **anchor**); their name, city and birth date are separate tables that point to that number. It is like a filing cabinet with one drawer per kind of fact rather than one folder per customer. Adding a new kind of fact means adding a new drawer, and nothing that already exists has to move.
:::

**The building blocks.** Anchor modeling, developed by Lars Rönnbäck and colleagues, has four constructs:

- **Anchor**: an entity's identity and nothing else, such as `CU_Customer(CU_ID)`.
- **Attribute**: one property of an anchor, in its own table keyed by the anchor ID. A **static** attribute never changes; a **historized** attribute adds a `ChangedAt` column, so each change is a new row.
- **Tie**: a relationship between anchors (customer places order), which may also be historized.
- **Knot**: a small shared set of values (gender, status, country), referenced by attributes and ties, like a reference table.

This is **sixth normal form** (6NF): every table holds a key plus at most one non-key attribute, plus time. Here it is in SQLite, with a query that rebuilds the customer as it looked at any moment (runs as is):

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
-- anchor: only the identity
CREATE TABLE CU_Customer (CU_ID INTEGER PRIMARY KEY);
-- static attribute: never changes, no history column
CREATE TABLE CU_BIR_Customer_BirthDate (CU_ID INT PRIMARY KEY REFERENCES CU_Customer, BirthDate TEXT);
-- historized attribute: one row per value, keyed by (identity, changed_at)
CREATE TABLE CU_CIT_Customer_City (CU_ID INT REFERENCES CU_Customer, City TEXT,
  ChangedAt TEXT, PRIMARY KEY (CU_ID, ChangedAt));
INSERT INTO CU_Customer VALUES (1), (2);
INSERT INTO CU_BIR_Customer_BirthDate VALUES (1, '1990-04-12');
INSERT INTO CU_CIT_Customer_City VALUES
  (1, 'Pune', '2026-01-05'), (1, 'Mumbai', '2026-03-05'), (2, 'Oslo', '2026-01-20');
""")
point_in_time = """
SELECT c.CU_ID, b.BirthDate,
  (SELECT City FROM CU_CIT_Customer_City h
   WHERE h.CU_ID = c.CU_ID AND h.ChangedAt <= :t
   ORDER BY h.ChangedAt DESC LIMIT 1) AS City
FROM CU_Customer c
LEFT JOIN CU_BIR_Customer_BirthDate b ON b.CU_ID = c.CU_ID
ORDER BY c.CU_ID"""
print(db.execute(point_in_time, {"t": "2026-02-01"}).fetchall())
# [(1, '1990-04-12', 'Pune'), (2, None, 'Oslo')]
print(db.execute(point_in_time, {"t": "2026-12-31"}).fetchall())
# [(1, '1990-04-12', 'Mumbai'), (2, None, 'Oslo')]
```

Customer 2 has no birth date, and there is no `NULL` stored anywhere: the attribute row simply does not exist. Anchor models expose such point-in-time queries as views and table-valued functions (a "latest" view and a "point in time" function per anchor), so users see an ordinary wide table and never write the subqueries.

**Why build it this way.**

- **Non-destructive evolution.** Adding an attribute is a new table. No existing table is altered, no existing query breaks, and no data is migrated. Old versions of the schema remain valid subsets of the new one.
- **History per attribute.** Each attribute is historized or not on its own terms, with no wide rows duplicated because one column changed (the type 2 problem of a new row for every phone number correction).
- **No NULLs** and very little redundancy.
- **Immutability.** Rows are only inserted; a change is a new row with a later `ChangedAt`.

The costs are many tables and joins, which the views hide but the engine still executes, and unfamiliarity: most engineers and BI tools expect wide tables. Anchor modeling is used mostly in integration layers where the schema changes often and history is essential, and its ideas (attribute-level history, insert-only storage, views that present current and as-of states) are worth knowing even if you never adopt it wholesale.

**General temporal patterns.** Every history design answers the same questions: what is versioned, at what granularity, and with which time axis. The main options, from simplest to richest:

| Pattern | How | Good for | Weakness |
|---|---|---|---|
| Full snapshots | copy the whole table daily, partitioned by snapshot date | simple "as of a day" queries | storage grows with table size times days; misses intraday changes |
| Row versioning (SCD type 2) | one row per version with validity interval | dimensions in star schemas | new row whenever any tracked column changes |
| Attribute-level history (anchor, satellites) | one table per attribute or attribute group | volatile, wide entities | many joins |
| Event log (change log, event sourcing) | store every change event; derive state | complete audit, replay | state must be rebuilt for queries |
| Bi-temporal rows | two intervals: valid time and recorded time | corrections and audit, next topic | more complex queries |

**Immutable attributes and corrections.** Separate attributes that legitimately change (city) from those that should never change once known (birth date, original signup channel). An "immutable" attribute that changes is a **correction**: the first value was wrong. Treating a correction as a normal change records a false history ("she was born in 1991 until March"). Good temporal designs distinguish the two, either by overwriting static attributes (type 1 for corrections) or by recording the correction time separately from the change time, which is what bi-temporal modeling does.

**Backdated changes.** A change that arrives late ("the customer moved on 1 May; we heard on 10 June") is easy in attribute-level designs: insert a row with `ChangedAt = 2026-05-01`, and every point-in-time query from then on sees the right value for any date. In a type 2 dimension the same change requires splitting a version and re-pointing facts. Anchor modeling's extension, sometimes described as concurrent-reliance-temporal modeling, adds a positing (recording) time and a reliability so that both the old and the corrected belief are kept.

> **Pitfall:** Using one timestamp for two meanings: "when this changed in the world" and "when we loaded it". A late-arriving file then shows customers moving on the load date, and backfills rewrite history to the backfill date. Keep both columns, and name them clearly.

::: deeper
**Naming conventions.** Anchor modeling uses mnemonic prefixes, such as `CU` for customer and `CU_CIT_Customer_City` for the city attribute, so that table names sort together and can be generated. The free online Anchor Modeler draws the model and generates DDL, views and functions for several databases.

**Performance.** The many-table design relies on **join elimination**: when a query on the latest view selects only two attributes, a good optimizer skips joins to the other attribute tables. That works because each attribute is joined by primary key through a left join; engines that eliminate such joins (SQL Server, PostgreSQL and others for simple cases) keep queries fast. Column stores already store columns separately, which is one reason anchor modeling is rarer on BigQuery: the physical benefit is already there and the joins remain.

**Relation to Data Vault.** Both are insert-only, historised and evolution-friendly. Data Vault groups attributes into satellites by source and rate of change; anchor modeling goes all the way to one attribute per table. Data Vault keeps source lineage at its core; anchor modeling focuses on the shape of the information itself.

> **Interview:** "How would you design history for an entity whose schema changes every month?" Attribute-level history: either anchor-style one-table-per-attribute or Data Vault satellites split by change rate, insert-only, with views presenting the current and as-of states. Explain that adding an attribute then never alters existing tables.
:::

::: check
?? What are the four constructs of anchor modeling?
Anchors (identities), attributes (one property each, static or historized), ties (relationships between anchors) and knots (shared small value sets).

?? Why does adding a new attribute never break existing queries in an anchor model?
The new attribute is a new table; no existing table changes shape, so every existing query and view still works.

?? What is the difference between a change and a correction, and why does it matter?
A change is the world moving on (a new city); a correction fixes a value that was wrong all along. Recording a correction as a change creates a false history, so temporal designs should keep the two distinct.
:::

::: resources
- Docs: [Anchor Modeling](https://www.anchormodeling.com/) · the official site, with tutorials, the free online modeler and examples
- Paper: [Anchor Modeling publications](https://www.anchormodeling.com/publications/) · the peer-reviewed papers on the technique, including its temporal extensions
- Article: [Anchor modeling](https://en.wikipedia.org/wiki/Anchor_modeling) · a concise summary of anchors, attributes, ties and knots
- Article: [Bitemporal History](https://martinfowler.com/articles/bitemporal-history.html) · Martin Fowler on the difference between when something changed and when you learned of it
- Book: [Developing Time-Oriented Database Applications in SQL](https://www2.cs.arizona.edu/~rts/tdbbook.pdf) · Richard Snodgrass's book, free from the author, covering every temporal pattern in SQL
:::

### Temporal Tables & Bi-Temporal Data Modeling

::: plain
Data has two clocks. One says **when something was true in the world** (the price was 12 from 1 March); the other says **when the database knew it** (we found out on 10 March). Most tables track only one of them, or neither. A **bi-temporal** table tracks both, so you can answer "what was the price on 3 March?" and also "what did we *think* the price was on 3 March, when we sent that invoice?". It is like a newspaper archive that keeps every edition including the corrections, rather than silently editing old articles.
:::

**Two time axes.**

- **Valid time** (also called application time or business time): the period during which a fact is true in the real world. An address is valid from the day the person moved in to the day they moved out. You can set it in the past or the future.
- **Transaction time** (also called system time or recorded time): the period during which the database stored the row as current belief. It is set by the system when the row is written and closed when it is superseded. You cannot set it yourself, and it never lies about what you knew.

A table with valid time only is **valid-time temporal** (an SCD type 2 dimension is one). A table with transaction time only is **system-versioned** (an audit history). A table with both is **bi-temporal**.

[[fig:modeling_bitemporal]]

**A worked example.** On 1 January we record that tea costs 10 from 1 January onwards. On 10 March we learn the price had actually been 12 since 1 March. A bi-temporal table never edits the old belief: it closes its recorded interval and inserts what we now believe. This runs in SQLite as is and matches the figure:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE price (sku TEXT, price REAL,
  valid_from TEXT, valid_to TEXT,          -- when the price applies in the world
  recorded_from TEXT, recorded_to TEXT);   -- when the database believed this row
-- 1 Jan: we record that the price is 10 from 1 Jan onwards
INSERT INTO price VALUES ('T-01', 10, '2026-01-01', '9999-12-31', '2026-01-01', '9999-12-31');
""")
# 10 Mar: we learn the price was really 12 from 1 Mar. Close the old belief,
# then record what we now believe, split into the two valid periods.
db.executescript("""
UPDATE price SET recorded_to = '2026-03-10' WHERE sku = 'T-01' AND recorded_to = '9999-12-31';
INSERT INTO price VALUES
  ('T-01', 10, '2026-01-01', '2026-03-01', '2026-03-10', '9999-12-31'),
  ('T-01', 12, '2026-03-01', '9999-12-31', '2026-03-10', '9999-12-31');
""")
q = """SELECT price FROM price WHERE sku = 'T-01'
       AND valid_from <= :valid AND :valid < valid_to
       AND recorded_from <= :known AND :known < recorded_to"""
print(db.execute(q, {"valid": "2026-03-03", "known": "2026-03-05"}).fetchone()[0])  # 10.0
print(db.execute(q, {"valid": "2026-03-03", "known": "2026-11-23"}).fetchone()[0])  # 12.0
print(db.execute(q, {"valid": "2026-02-15", "known": "2026-11-23"}).fetchone()[0])  # 10.0
```

The first query reproduces the invoice sent on 5 March exactly (it used price 10, which was the best knowledge then). The second gives the corrected truth. Every bi-temporal query fixes both a valid point and a known point; "current" queries simply use today for both.

**SQL:2011 temporal tables.** The SQL standard added both axes in SQL:2011:

```sql
-- application-time (valid time) period, standard SQL:2011
CREATE TABLE product_price (
  sku         VARCHAR(20),
  price       DECIMAL(10, 2),
  valid_start DATE,
  valid_end   DATE,
  PERIOD FOR valid_period (valid_start, valid_end),
  PRIMARY KEY (sku, valid_period WITHOUT OVERLAPS)
);

-- system-versioned table: the database maintains transaction time itself
CREATE TABLE account (
  account_id INT PRIMARY KEY,
  balance    DECIMAL(12, 2),
  sys_start  TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
  sys_end    TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
  PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
) WITH SYSTEM VERSIONING;

SELECT * FROM account FOR SYSTEM_TIME AS OF TIMESTAMP '2026-03-05 00:00:00';
```

`WITHOUT OVERLAPS` makes the database reject two prices for the same SKU on the same day. Support varies: MariaDB, Db2 and SQL Server implement system versioning, MariaDB and Db2 also application-time periods, and Oracle offers similar features under its own syntax. PostgreSQL has no `PERIOD` clause, but PostgreSQL 18 added temporal constraints on range columns: `PRIMARY KEY (sku, valid_at WITHOUT OVERLAPS)` where `valid_at` is a `daterange`, and `PERIOD` in foreign keys (the `btree_gist` extension provides the index support for the plain `sku` column). Where the engine has no support, you implement the intervals yourself, as in the SQLite example, and test for overlaps.

**Time travel in lakehouses and BigQuery.** Table formats and warehouses keep old versions of tables, which gives you a form of transaction time for free:

```sql
-- BigQuery: the table as it was an hour ago (within the time travel window)
SELECT * FROM shop.orders
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);

-- Spark SQL on Delta Lake or Apache Iceberg
SELECT * FROM lake.orders VERSION AS OF 42;
SELECT * FROM lake.orders TIMESTAMP AS OF '2026-03-05 00:00:00';
```

This is excellent for debugging, reproducing a model's training data, and undoing a bad load, but it is **not** a substitute for modeled history. BigQuery's time travel window is seven days by default and can be set between two and seven days. Delta keeps removed data files for seven days by default before `VACUUM` may delete them, and log history for 30 days. Iceberg's snapshot expiry removes snapshots older than five days by default when you run it. Retention is tied to storage cost and maintenance jobs, not to business requirements. If the business needs "as it was known last year", model transaction time explicitly in the data.

**Audit history design.** A pragmatic, common design for operational data reaching the warehouse:

1. Capture every change from the source with CDC, including the source commit timestamp (Datastream on Google Cloud, Debezium elsewhere).
2. Store the change log append-only, with `source_committed_at` (close to transaction time in the source) and `ingested_at`.
3. Derive valid-time dimensions (type 2) from it for reporting, and keep the log for audit questions.
4. When the business supplies an effective date that differs from the commit time (a backdated contract amendment), carry it as valid time and keep the commit time as transaction time: that is a bi-temporal model.

**SCD type 2 versus bi-temporal.**

| Question | SCD type 2 | Bi-temporal |
|---|---|---|
| What was true on date X? | yes | yes |
| What did we report on date Y? | no (corrections rewrite history) | yes |
| Backdated change | split versions, re-point facts | insert rows, close recorded intervals |
| Query complexity | one interval | two intervals |
| Typical domains | most analytics | finance, insurance, healthcare, regulatory reporting |

> **Pitfall:** Implementing "transaction time" with a column the pipeline sets to `CURRENT_TIMESTAMP()`, then re-running a backfill. Every backfilled row now claims to have been recorded today, and the audit trail is meaningless. Transaction time must come from the system that actually recorded the fact, or be preserved across re-runs.

::: deeper
**Temporal joins.** Joining two valid-time tables means intersecting their intervals: the result is valid only where both inputs are. `GREATEST(a.valid_from, b.valid_from)` and `LEAST(a.valid_to, b.valid_to)` give the overlap, and rows with an empty overlap are dropped. Stream processors have the same concept: Flink's temporal table joins look up the version of a table valid at each event's time.

**Coalescing.** After joins and projections, consecutive rows with identical values (Pune from January to March, Pune from March to June) should be merged into one interval. This is a gaps-and-islands problem, solved with window functions (see the SQL section).

**Regulatory examples.** Financial regulations often require firms to reproduce exactly what was reported at a past date, and insurance claims depend on which policy terms were in force at the event date as known at the claim date. These are the cases where bi-temporal models are not optional.

**Event sourcing.** An append-only event log is naturally transaction-time (events are recorded in order) and can carry valid time in each event's payload. The Event-driven architecture section covers event sourcing; projections built from it are bi-temporal if they keep both timestamps.

> **Interview:** "A customer disputes an invoice from March, and the price was corrected in April. How do you show both what we charged and what we should have charged?" A bi-temporal price table: query valid time = invoice date with known time = invoice date (what we charged) and known time = now (the corrected price); never overwrite the old belief.
:::

::: check
?? What is the difference between valid time and transaction time?
Valid time is when a fact is true in the world and can be set by users, including in the past or future. Transaction time is when the database recorded the row as current belief; it is set by the system and never edited.

?? Why is lakehouse or BigQuery time travel not a replacement for modeled history?
Its retention is short and tied to storage maintenance (seven days by default in BigQuery, snapshot expiry and vacuum in lakehouses), and it records only when data was written, not when facts were true.

?? In a bi-temporal table, how is a correction to a past value applied?
The current belief's recorded interval is closed, and new rows with the corrected valid periods are inserted with a recorded interval starting now. No row is edited in place except for closing its recorded interval.
:::

::: resources
- Article: [Temporal database](https://en.wikipedia.org/wiki/Temporal_database) · definitions of valid time, transaction time and bi-temporal tables, with SQL:2011 examples
- Book: [Developing Time-Oriented Database Applications in SQL](https://www2.cs.arizona.edu/~rts/tdbbook.pdf) · Snodgrass; the classic, free, with every temporal query pattern
- Docs: [BigQuery time travel and data retention](https://cloud.google.com/bigquery/docs/time-travel) · the time travel window, `FOR SYSTEM_TIME AS OF` and fail-safe
- Blog: [Introducing Delta time travel](https://www.databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html) · how version and timestamp queries work in Delta Lake
- Docs: [Iceberg Spark queries: time travel](https://iceberg.apache.org/docs/latest/spark-queries/) · snapshot, timestamp, branch and tag queries on Iceberg tables
- Article: [Bitemporal History](https://martinfowler.com/articles/bitemporal-history.html) · Martin Fowler's clear explanation with a payroll example
:::

## Specialised models

Facts, dimensions and history cover the core of a warehouse, but a data platform models much more than sales tables. Business metrics need one governed definition that every tool uses. Customers and products arrive from many systems and must be matched into one trusted record. Country codes, product taxonomies and units need controlled lists. Events flowing through Pub/Sub or Kafka need schemas that can change without breaking consumers. Lakehouse tables need partition and sort designs that suit their engines. Some data is a network rather than a table. Some industries have their own standard models. And every dataset shared between teams needs a contract.

Each topic in this group applies the fundamentals (entities, keys, grain, history) to one of these special shapes. Several are covered from other angles elsewhere in the course: schema registries in Streaming systems, CloudEvents in Event-driven architecture, table format internals in Data lakes and lakehouses, catalogs and master data governance in Data governance, security and compliance, and graph processing in Graph data engineering. Here the question is always the same: how do you design the model?

### Semantic Layer & Metrics Modeling

::: plain
A semantic layer is a shared dictionary of business numbers that sits between the warehouse and every tool that shows numbers. Instead of each dashboard writing its own SQL for "revenue", the definition is written once ("sum of net amount on paid order lines, excluding tax, by order date") and every tool asks the semantic layer for "revenue by month". It is like a recipe book in a restaurant chain: every kitchen makes the dish the same way, so a customer gets the same meal in every branch.
:::

**The problem it solves.** Without a semantic layer, the definition of a metric lives in SQL copied across dashboards, notebooks and spreadsheets. Each copy drifts: one excludes cancelled orders, another includes tax, a third uses ship date instead of order date. The finance report and the sales dashboard then disagree, and nobody trusts either. A **semantic layer** (also called a metrics layer) stores the definitions centrally and generates the SQL.

**What a metrics model contains.** The vocabulary is similar across tools:

- **Entities**: the join keys that connect tables (order, customer, product), each marked as primary or foreign. They let the layer work out **join paths** itself.
- **Dimensions**: attributes to group and filter by: categorical (channel, city) and time (order date at day, week, month grain).
- **Measures**: aggregations of a column (sum of `net_amount`, count of distinct `order_id`).
- **Metrics**: the named business numbers built from measures. Simple (revenue), ratio (average order value = revenue / orders), derived (gross margin = revenue minus cost), cumulative (revenue to date), and conversion (share of visitors who order within 7 days).

**An example in dbt's MetricFlow.** The dbt Semantic Layer is powered by **MetricFlow**, which dbt Labs publishes as open source under the Apache 2.0 licence. A semantic model describes one dbt model; metrics are defined on top:

```yaml
semantic_models:
  - name: order_lines
    model: ref('fct_order_lines')
    defaults:
      agg_time_dimension: order_date
    entities:
      - name: order_line
        type: primary
        expr: order_line_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: channel
        type: categorical
    measures:
      - name: revenue
        agg: sum
        expr: net_amount
      - name: orders
        agg: count_distinct
        expr: order_id

metrics:
  - name: revenue
    label: Revenue
    type: simple
    type_params:
      measure: revenue
  - name: orders
    label: Orders
    type: simple
    type_params:
      measure: orders
  - name: average_order_value
    label: Average order value
    type: ratio
    type_params:
      numerator: revenue
      denominator: orders
```

A query such as "average order value by customer city and month" now needs no SQL from the user: MetricFlow sees that `city` lives in a customers semantic model reached through the `customer` entity, generates the join, aggregates numerator and denominator separately and divides at the end. The YAML spec has evolved across dbt versions, so check the documentation for the version you run.

> **Pitfall:** Defining a ratio metric as the average of a row-level ratio (`AVG(net_amount / quantity)`). The correct average price is `SUM(net_amount) / SUM(quantity)`. A semantic layer helps only if ratios are modeled as ratios of sums.

**The same idea in Looker.** Looker's **LookML** has had a semantic layer for over a decade. Views define dimensions and measures over a table, and explores define the allowed joins with their relationship (many_to_one and so on), which Looker uses to compute correct totals even when joins fan out (symmetric aggregates):

```text
view: order_lines {
  sql_table_name: mart.fct_order_lines ;;
  dimension: order_line_id { primary_key: yes  sql: ${TABLE}.order_line_id ;; }
  dimension_group: order { type: time  timeframes: [date, week, month]  datatype: date  sql: ${TABLE}.order_date ;; }
  measure: revenue { type: sum  sql: ${TABLE}.net_amount ;; }
}
explore: order_lines {
  join: customers {
    sql_on: ${order_lines.customer_id} = ${customers.customer_id} ;;
    relationship: many_to_one
  }
}
```

Cube (open source) uses a similar model of cubes with measures, dimensions and joins, and serves it through SQL and REST APIs.

**Headless BI.** A semantic layer that is independent of any one dashboard tool is called **headless BI**: definitions live in the layer, and many "heads" (BI tools, spreadsheets, notebooks, applications, AI assistants) query it through an API or a SQL interface. The benefit grows with the number of consumers. With one BI tool, its own modeling layer may be enough.

**Designing metrics well.** A metric definition is a contract. For each metric record:

| Field | Example |
|---|---|
| name and label | `net_revenue`, "Net revenue" |
| description in business words | paid order lines, after discounts, before tax and shipping |
| measure and filter | sum of `net_amount` where `status = 'paid'` |
| time dimension | order date (not ship or payment date) |
| allowed dimensions | channel, product category, customer segment |
| owner | finance analytics |
| certification status | certified, or experimental |

Model metrics on conformed dimensions and facts with a clear grain: the semantic layer can only be as correct as the star underneath it. The Data warehousing section compares semantic layer products in more depth; Analytics engineering and BI covers how teams roll them out.

::: deeper
**Join path ambiguity.** If customers can be reached from order lines both directly and through orders, the layer must know which path a metric uses. Good models make paths unique (one entity relationship per pair of semantic models) or name roles explicitly (buyer versus recipient).

**Non-additive metrics across grains.** Distinct counts (unique customers) cannot be summed from daily to monthly: 100 unique customers on each of 30 days is not 3,000 unique customers in the month. Semantic layers recompute distinct counts at the requested grain from the atomic data, or use approximate sketches (HyperLogLog) when stored pre-aggregated.

**Caching and pre-aggregation.** Generated SQL runs on the warehouse each time unless the layer caches results or builds pre-aggregated tables. Cube's pre-aggregations, Looker's aggregate awareness and BigQuery materialized views or BI Engine are the usual tools. The definitions stay in one place; only the execution is optimised.

**Metrics for AI assistants.** Natural-language query tools give far more reliable answers when they query governed metrics rather than writing SQL over raw tables, because the hard decisions (which date, which filter, which join) are already encoded. That is one reason semantic layers have become more prominent.

> **Interview:** "Two dashboards show different revenue. How do you prevent it?" Find the differences (filters, date, tax), agree one definition with an owner, implement it once in a semantic layer (or one certified model), point both dashboards at it, and add a test that reconciles it against finance's ledger.
:::

::: check
?? What is the difference between a measure and a metric in MetricFlow?
A measure is an aggregation of a column in one semantic model (sum of `net_amount`); a metric is the named business number built from one or more measures or metrics (revenue, or revenue divided by orders).

?? Why must a ratio metric be computed as a ratio of sums?
Averaging row-level ratios weights every row equally regardless of size and gives the wrong answer; the correct value divides the total numerator by the total denominator.

?? What is headless BI?
A semantic layer independent of any dashboard tool, so many consumers (BI tools, notebooks, applications) query the same metric definitions through an API or SQL interface.
:::

::: resources
- Docs: [dbt: about metrics and MetricFlow](https://docs.getdbt.com/docs/build/metrics-overview) · metric types and their YAML definitions
- Docs: [dbt: semantic models](https://docs.getdbt.com/docs/build/semantic-models) · entities, dimensions and measures in detail
- Docs: [What is LookML?](https://cloud.google.com/looker/docs/what-is-lookml) · Looker's modeling language for views, explores and measures
- Docs: [Cube data modeling concepts](https://cube.dev/docs/product/data-modeling/concepts) · cubes, views, measures, dimensions and joins in Cube
- Repo: [MetricFlow](https://github.com/dbt-labs/metricflow) · the open-source engine that compiles metric requests into SQL
:::

### Master Data Modeling (MDM)

::: plain
Master data is the small set of core things every part of the business refers to: customers, products, suppliers, employees, locations. The trouble is that each system keeps its own copy, spelt slightly differently. **Master data management** finds which records describe the same real thing and builds one trusted **golden record** for each. It is like merging the contact lists from your phone, email and work laptop: you must decide that "Asha Rao", "A. Rao" and "asha.r@example.com" are one person, and which phone number to keep.
:::

**The model behind MDM.** An MDM design has four layers of tables:

1. **Source records**: every customer record from every system, kept as received with its source and source ID.
2. **Match results**: which source records belong together, as a **match group** (cluster) with a confidence score.
3. **Cross-reference (xref) table**: source system and source ID mapped to the master ID. This is what other systems use to translate keys.
4. **Golden record**: one row per master ID with the surviving value of each attribute, and lineage saying which source each value came from.

```text
golden_customer(master_id PK, name, email, phone, name_source, phone_source, updated_at)
customer_xref(source_system, source_id, master_id FK, match_rule, match_score, linked_at)
```

**Identity resolution.** Matching (also called record linkage or entity resolution) decides which records are the same entity:

- **Deterministic** matching uses exact rules on normalised values: same lower-cased email, or same national ID, or same name plus date of birth plus postcode. It is predictable and explainable, and misses typos.
- **Probabilistic** matching scores each pair on several fields and adds up evidence. The classic **Fellegi-Sunter** model estimates, for each field, how likely agreement is among true matches versus non-matches, and turns that into a match weight. Pairs above a threshold match, pairs below do not, and pairs in between go to human review.
- **Blocking** makes matching feasible: comparing every pair of 10 million records means about 5 × 10^13 comparisons, so you only compare records that share a cheap key (same postcode, same first three letters of surname).

**Survivorship rules.** Once records are grouped, each attribute of the golden record is chosen by a rule:

| Rule | Example |
|---|---|
| most trusted source | legal name from the CRM over the web shop |
| most recent | phone number from the latest updated record |
| most complete | the longest non-null address |
| most frequent | the email that appears in most sources |
| never overwrite verified | a value confirmed by the customer wins over all others |

A tiny deterministic match with two survivorship rules runs as is:

```python
from collections import defaultdict

records = [  # customer records from three systems
    {"src": "crm",  "id": "C-17", "email": "Asha.R@Example.com ", "name": "Asha Rao",
     "phone": None,          "updated": "2026-02-01"},
    {"src": "shop", "id": "991",  "email": "asha.r@example.com",  "name": "A. Rao",
     "phone": "+91 98200 11111", "updated": "2026-03-15"},
    {"src": "support", "id": "T-5", "email": "asha.r@example.com", "name": "Asha R",
     "phone": "+91 98200 22222", "updated": "2026-01-10"},
    {"src": "shop", "id": "992",  "email": "bo@example.com",      "name": "Bo Berg",
     "phone": None,          "updated": "2026-03-01"},
]
TRUST = {"crm": 3, "shop": 2, "support": 1}          # higher = more trusted source

# 1. match: deterministic rule on the normalised email
groups = defaultdict(list)
for r in records:
    groups[r["email"].strip().lower()].append(r)

# 2. survive: name from the most trusted source, phone from the most recent non-null
for key, recs in groups.items():
    name = max(recs, key=lambda r: TRUST[r["src"]])["name"]
    phones = [r for r in recs if r["phone"]]
    phone = max(phones, key=lambda r: r["updated"])["phone"] if phones else None
    xref = sorted(f'{r["src"]}:{r["id"]}' for r in recs)
    print(key, "|", name, "|", phone, "|", xref)
# asha.r@example.com | Asha Rao | +91 98200 11111 | ['crm:C-17', 'shop:991', 'support:T-5']
# bo@example.com | Bo Berg | None | ['shop:992']
```

**MDM implementation styles.**

| Style | How | Systems keep their own data? |
|---|---|---|
| Registry | only the xref and match results are central; golden view computed on read | yes |
| Consolidation | golden records built centrally for analytics; sources unchanged | yes |
| Coexistence | golden records built centrally and pushed back to sources | yes, synchronised |
| Centralised (transactional) | the MDM hub is the system of record; sources read from it | no |

Most data platforms start with **consolidation** in the warehouse: match and survive in SQL or a matching library, publish `dim_customer` from the golden records, and expose the xref so other teams can translate IDs.

**Hierarchy management.** Master data often has hierarchies: a customer belongs to a company, which belongs to a group; products roll up to brands and categories. Model the hierarchy as its own table with validity dates (`parent_id`, `child_id`, `valid_from`, `valid_to`), because reorganisations happen and historical reports must use the hierarchy that was valid at the time.

> **Pitfall:** Overwriting the xref when matches change. Matching rules improve, and a record linked to master 17 last month may be linked to master 23 today. Keep the history of the xref (when each link was made and by which rule), or you cannot explain why last month's report counted 10,000 customers and this month's counts 9,600 for the same period.

::: deeper
**Stable master IDs through merges and splits.** When two master records turn out to be one, keep one ID as the survivor and record the merge (`merged_into`), so facts already pointing at the retired ID still resolve. When one master record turns out to be two people, splitting requires re-pointing facts: this is why conservative thresholds and review queues matter.

**Tools.** Splink, an open-source probabilistic linkage library from the UK Ministry of Justice, implements Fellegi-Sunter at scale on DuckDB, Spark and other backends. Commercial MDM suites (Informatica, Reltio, Semarchy, Profisee, Tamr) add workflow, stewardship user interfaces and real-time APIs. On Google Cloud, matching can run in BigQuery SQL or in Spark on Managed Service for Apache Spark (formerly Dataproc).

**Privacy.** Matching joins personal data across systems, so it is a sensitive process: limit who can see source records, hash or tokenise identifiers where possible, and respect consent (a customer who opted out of marketing in one system must not reappear in a marketing audience through the golden record). The governance section covers MDM's organisational side.

> **Interview:** "Design customer deduplication across three systems." Describe normalisation, blocking, deterministic rules first, probabilistic scoring for the rest, a review queue for the uncertain band, survivorship rules per attribute, an xref table with history, stable master IDs with merge records, and metrics for match quality (precision and recall on a labelled sample).
:::

::: check
?? What is the cross-reference (xref) table in MDM for?
It maps each source system's ID to the master ID, so any system or pipeline can translate its keys to the golden record and back.

?? Why is blocking needed in record matching?
Comparing every pair of records grows with the square of the record count; blocking compares only records that share a cheap key, cutting comparisons by orders of magnitude.

?? Give two survivorship rules and when you would use each.
Most trusted source (legal name from the CRM, which validates it) and most recent value (phone number, which changes and is usually freshest in the latest record).
:::

::: resources
- Book: *Master Data Management* by David Loshin · the standard introduction to MDM architecture, styles and governance
- Docs: [Splink](https://moj-analytical-services.github.io/splink/) · open-source probabilistic record linkage, with excellent tutorials on Fellegi-Sunter
- Article: [Record linkage](https://en.wikipedia.org/wiki/Record_linkage) · deterministic and probabilistic matching, blocking and the Fellegi-Sunter model
- Article: [What is master data management?](https://www.informatica.com/services-and-training/glossary-of-terms/master-data-management-definition.html) · Informatica's overview of MDM concepts and styles
- Article: [What is master data management?](https://www.talend.com/resources/what-is-master-data-management/) · Talend's guide to golden records and survivorship
:::

### Reference Data & Taxonomy Modeling

::: plain
Reference data is the set of allowed values that other data uses: country codes, currency codes, order statuses, product categories, units of measure. It changes rarely, but everything depends on it. A taxonomy arranges such values in a hierarchy (tea belongs to hot drinks, which belongs to drinks). It is like the list of postcodes the post office uses: small, slow to change, and if two systems use different lists, parcels go astray.
:::

**Reference data versus master data.** Master data describes business entities (each customer, each product) and has thousands to millions of rows. Reference data classifies them: a few to a few thousand codes, often defined outside the company (ISO standards) or by a central team. The same modeling principles apply, but reference data has its own patterns.

**Controlled vocabularies and code sets.** A **controlled vocabulary** is a fixed list of terms with definitions. Model each code set as a table with a stable code, labels and metadata:

```sql
CREATE TABLE ref.country (
  country_code STRING NOT NULL,     -- ISO 3166-1 alpha-2: 'IN', 'NO'
  name_en      STRING NOT NULL,
  region       STRING,              -- company-defined: 'APAC', 'EMEA'
  valid_from   DATE NOT NULL,
  valid_to     DATE NOT NULL,       -- '9999-12-31' while current
  PRIMARY KEY (country_code, valid_from) NOT ENFORCED
);
```

Prefer external standards wherever they exist: ISO 3166 for countries, ISO 4217 for currencies (with their minor units: 2 decimals for EUR, 0 for JPY), ISO 639 for languages, IANA time zone names, UCUM for units of measure. They are stable, documented and understood by partners.

**Code-set versioning.** Reference data does change: countries are renamed, currencies retired, categories reorganised. Treat each code set as versioned data:

- add validity dates (as above) so historical facts can be interpreted with the codes valid at the time;
- never reuse a retired code for a different meaning;
- record a version or release identifier for externally published sets;
- review and approve changes like code (a pull request to a seed file is a good workflow).

In dbt, small reference tables are often **seeds**: CSV files in the repository, loaded with `dbt seed`, versioned in Git and reviewed in pull requests.

**Conformance in pipelines.** Sources spell the same value differently ("India", "IND", "in"). Map each source value to the canonical code with a **mapping table**, and fail or quarantine rows whose value has no mapping. This runs as is:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE ref_country (country_code TEXT PRIMARY KEY, name_en TEXT, region TEXT);
INSERT INTO ref_country VALUES ('IN','India','APAC'), ('NO','Norway','EMEA'), ('DE','Germany','EMEA');
-- how each source spells countries, mapped to the canonical ISO 3166-1 alpha-2 code
CREATE TABLE map_country (source TEXT, source_value TEXT, country_code TEXT,
                          PRIMARY KEY (source, source_value));
INSERT INTO map_country VALUES ('shop','India','IN'), ('shop','Norge','NO'),
  ('crm','IND','IN'), ('crm','NOR','NO'), ('crm','DEU','DE');
CREATE TABLE stg_orders (order_id INT, source TEXT, country_raw TEXT);
INSERT INTO stg_orders VALUES (1,'shop','India'), (2,'crm','NOR'), (3,'shop','Deutschland'), (4,'crm','DEU');
""")
rows = db.execute("""
SELECT o.order_id, o.country_raw, m.country_code, r.region
FROM stg_orders o
LEFT JOIN map_country m ON m.source = o.source AND m.source_value = o.country_raw
LEFT JOIN ref_country r ON r.country_code = m.country_code
ORDER BY o.order_id""").fetchall()
for r in rows: print(r)
unmapped = [r for r in rows if r[2] is None]
print("unmapped:", unmapped)
# (1, 'India', 'IN', 'APAC')
# (2, 'NOR', 'NO', 'EMEA')
# (3, 'Deutschland', None, None)
# (4, 'DEU', 'DE', 'EMEA')
# unmapped: [(3, 'Deutschland', None, None)]
```

The shop sent "Deutschland", which nobody mapped. A good pipeline does not silently drop the order or label it "other": it raises an alert (or a test failure) so someone adds the mapping, and meanwhile the row carries an explicit unknown code.

**Hierarchies and roll-ups.** Taxonomies are trees (sometimes graphs). Three common physical shapes:

| Shape | Table | Good for | Weak at |
|---|---|---|---|
| Adjacency list | `category(id, parent_id, name)` | editing, any depth | roll-ups need recursive queries |
| Flattened levels | `level1, level2, level3` columns | BI roll-ups with one `GROUP BY` | ragged or deep trees |
| Closure table | `(ancestor_id, descendant_id, depth)` | "everything under X" in one join | more rows to maintain |

Keep the adjacency list as the source of truth and derive the flattened and closure forms in the pipeline (the SQL section's recursive CTEs do exactly this). Decide how to handle **ragged** hierarchies (some branches deeper than others): repeat the last level downward, or use "unspecified" placeholders, and document the choice.

**Ontologies and SKOS.** When a vocabulary is shared across organisations or needs richer relationships than parent and child, it becomes an **ontology** or thesaurus. W3C's **SKOS** (Simple Knowledge Organization System) models concepts with preferred and alternative labels (in many languages), `broader` and `narrower` relations, and mappings to other schemes (`exactMatch`, `closeMatch`). It is common in publishing, government and life sciences, and a useful pattern even when stored in ordinary tables.

**Multilingual labels.** Do not add a column per language (`name_en`, `name_de`, `name_fr`...) beyond one or two. Store labels in a child table: `category_label(category_id, language_code, label, is_preferred)`. Codes stay language-neutral; dashboards choose the label by the user's language, with a fallback.

> **Pitfall:** Storing labels instead of codes on facts (`status = 'Shipped'` in one source, `'shipped'` in another, `'Dispatched'` after a rename). Store the stable code, and look up the current label in the reference table.

::: deeper
**Who owns reference data?** External sets are owned by their standards bodies, but someone in the company must own the local copy, its updates and its company-specific extensions (regions, groupings). Without an owner, each team adds its own mappings and conformance breaks.

**Reference data as a service.** Large organisations publish reference data through an API or a shared dataset (in BigQuery, a dataset other projects can read, or BigQuery sharing, formerly Analytics Hub, for cross-organisation exchange), so every pipeline uses the same version.

**Slowly changing taxonomies.** When the merchandising team moves "herbal tea" from "hot drinks" to "wellness", historical reports must decide: restate history under the new taxonomy (type 1) or keep the old one (type 2). Often both are needed: finance reports as-was, merchandising analyses as-is. Model taxonomy membership with validity dates so both are possible.

> **Interview:** "A source starts sending a new status value. What should happen?" The pipeline should detect the unmapped value (an accepted-values test or a mapping join), alert the owner, and route the rows to an explicit unknown code rather than drop them; once the reference data is updated and reviewed, a re-run maps them correctly.
:::

::: check
?? What is the difference between master data and reference data?
Master data describes core business entities (customers, products) and has many rows; reference data is the small sets of allowed codes that classify them (countries, currencies, statuses).

?? Why should facts store codes rather than labels?
Labels change (renames, translations) and differ between sources; codes are stable, and the current label can always be looked up in the reference table.

?? Name three ways to store a hierarchy and one strength of each.
Adjacency list (easy to edit, any depth), flattened levels (one `GROUP BY` for BI roll-ups), closure table (all descendants of a node with one join).
:::

::: resources
- Spec: [SKOS Simple Knowledge Organization System Reference](https://www.w3.org/TR/skos-reference/) · the W3C model for concepts, labels and broader or narrower relations
- Docs: [ISO 3166 country codes](https://www.iso.org/iso-3166-country-codes.html) · the official source for country codes and their changes
- Docs: [ISO 4217 currency codes](https://www.iso.org/iso-4217-currency-codes.html) · currency codes and minor units
- Docs: [dbt seeds](https://docs.getdbt.com/docs/build/seeds) · managing small reference tables as versioned CSV files
- Book: [Agile Data Warehouse Design](https://www.oreilly.com/library/view/agile-data-warehouse/9780471777090/) · Lawrence Corr; the chapters on hierarchies and ragged dimensions
:::

### Event & Message Schema Modeling

::: plain
An event is a message saying that something happened: an order was placed, a payment failed, a parcel was delivered. Many programs read the same events, often long after they were written, so the shape of each event (its **schema**) is a promise to all of them. Changing it carelessly is like changing the layout of a printed form that thousands of clerks fill in and read: if a box moves or disappears, everyone downstream misreads it.
:::

**What to model.** An event schema is a data model like any other, with a few extra decisions:

- **Name and meaning.** Name events as business facts in the past tense: `OrderPlaced`, `PaymentFailed`, `ParcelDelivered`, not `OrderUpdate` or `ProcessOrder`. One event type per business fact keeps meaning clear.
- **Identity.** Every event carries a unique `event_id`, so consumers can deduplicate (delivery is usually at least once).
- **Time.** Carry `event_time` (when it happened, set by the producer) separately from the broker's publish time and the pipeline's ingest time. Streaming systems rely on event time; see Streaming systems.
- **Subject keys.** Include the business keys of the entities involved (`order_id`, `customer_id`), and use the main one as the message key so events for one order stay in order on one partition.
- **Thin or fat payload.** A **thin** (notification) event carries only IDs ("order 9001 changed") and consumers call back for details; a **fat** (event-carried state transfer) event carries the full relevant state. Fat events decouple consumers from the producer's API and are usually better for data pipelines, at the cost of larger messages and more fields to keep compatible.
- **Envelope.** Common metadata (`id`, `source`, `type`, `time`, `specversion`) belongs in a standard envelope; CloudEvents, covered in Event-driven architecture, is the common standard.

**Schema formats.**

| Format | Encoding | Schema needed to read? | Evolution model |
|---|---|---|---|
| Avro | compact binary | yes (writer's schema, usually from a registry) | resolution by field name, defaults fill gaps |
| Protocol Buffers | compact binary | no for basic decoding (field numbers) | by field number; unknown fields ignored |
| JSON Schema | JSON text | no | validation rules; compatibility depends on open or closed content |

An Avro schema for an order event, and the Protocol Buffers equivalent:

```json
{
  "type": "record", "name": "OrderPlaced", "namespace": "com.shop.orders",
  "fields": [
    {"name": "event_id",   "type": "string"},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "order_id",   "type": "string"},
    {"name": "customer_id","type": "string"},
    {"name": "amount",     "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
    {"name": "currency",   "type": "string", "default": "EUR"},
    {"name": "coupon",     "type": ["null", "string"], "default": null}
  ]
}
```

```text
syntax = "proto3";
package shop.orders.v1;
import "google/protobuf/timestamp.proto";

message OrderPlaced {
  string event_id = 1;
  google.protobuf.Timestamp event_time = 2;
  string order_id = 3;
  string customer_id = 4;
  string amount = 5;          // decimal as string, e.g. "40.00"
  string currency = 6;
  optional string coupon = 7;
  reserved 8;                 // was 'channel'; never reuse the number
  reserved "channel";
}
```

Money is a decimal (Avro's `decimal` logical type, or a string or units-plus-nanos message in Protobuf), never a float.

**Compatibility rules.** Schema registries (Confluent Schema Registry for Kafka; Pub/Sub has its own schemas with revisions, for Avro and Protocol Buffers) check each new version against the old ones under a **compatibility mode**:

| Mode | Guarantee | Who upgrades first | Allowed changes (Avro) |
|---|---|---|---|
| BACKWARD (Confluent default) | new schema can read data written with the previous one | consumers | delete fields; add fields with defaults |
| FORWARD | previous schema can read data written with the new one | producers | add fields; delete fields that had defaults |
| FULL | both | either | add or delete fields with defaults only |
| NONE | no check | coordinated | anything |

The `_TRANSITIVE` variants check against all earlier versions, not just the previous one; use them for topics that are replayed from the beginning. A minimal checker of the Avro rule shows why defaults are the key (runs as is):

```python
def fields(schema):
    return {f["name"]: f for f in schema["fields"]}

def can_read(writer, reader):
    """Problems a consumer with schema `reader` hits reading data written with `writer`."""
    w, r = fields(writer), fields(reader)
    problems = []
    for name, f in r.items():
        if name not in w and "default" not in f:
            problems.append(f"'{name}' missing from data and has no default")
        elif name in w and w[name]["type"] != f["type"]:
            problems.append(f"'{name}' type {w[name]['type']} vs {f['type']}")
    return problems  # fields the writer has but the reader lacks are ignored

v1 = {"type": "record", "name": "OrderPlaced", "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"}]}
v2 = {"type": "record", "name": "OrderPlaced", "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "EUR"}]}
v3 = {"type": "record", "name": "OrderPlaced", "fields": [   # drops amount
    {"name": "order_id", "type": "string"},
    {"name": "currency", "type": "string", "default": "EUR"}]}

print("v1->v2 backward:", can_read(v1, v2))   # new reader, old data: []
print("v1->v2 forward: ", can_read(v2, v1))   # old reader, new data: []
print("v2->v3 backward:", can_read(v2, v3))   # []
print("v2->v3 forward: ", can_read(v3, v2))   # ["'amount' missing from data and has no default"]
```

Adding `currency` with a default is fully compatible. Dropping `amount` is backward compatible (new readers ignore it in old data) but not forward compatible: consumers still on v2 expect `amount`, it has no default, and they fail. The real Avro rules are richer (type promotions such as int to long are allowed, aliases support renames), but the principle is the same.

**Versioning strategy.** Evolve compatibly within one schema for as long as possible: add optional fields, never change meaning, never reuse names or field numbers. When a change cannot be compatible (a different key, a new meaning), create a **new event type or topic version** (`orders.v2`), publish both for a deprecation window, migrate consumers, then retire v1.

> **Pitfall:** Changing the meaning of a field without changing its schema, for example sending `amount` in cents instead of euros. Every compatibility check passes and every consumer is wrong. Semantic changes need a new field name (`amount_minor_units`) or a new version.

::: deeper
**Protobuf specifics.** Field numbers are the identity, so renaming a field is wire-compatible but deleting one and reusing its number is a silent corruption; `reserved` prevents it. In proto3, scalar fields without `optional` cannot distinguish "not set" from the default value (0, empty string), which matters for analytics: use `optional` or wrapper types when absence is meaningful.

**JSON Schema specifics.** With an **open** content model (additional properties allowed, the default), adding a property is safe for old consumers; with a **closed** model (`additionalProperties: false`), it breaks them. Confluent's registry applies JSON-specific rules; read them before choosing JSON Schema for a high-change topic.

**From events to tables.** When events land in BigQuery (through a Pub/Sub BigQuery subscription or Dataflow), the event schema becomes a table schema. BigQuery allows adding nullable columns and relaxing `REQUIRED` to `NULLABLE` in place, so compatible event evolution maps well; renames and type changes need the expand-and-contract migration in the Modeling in practice group.

**Schema ownership.** The producing team owns the schema and its compatibility; consumers should be able to see it in a registry or catalog, and the registry check should run in the producer's CI before deployment, not only at publish time.

> **Interview:** "A producer wants to add a field and remove another. What compatibility mode, and in what order do teams deploy?" With FULL_TRANSITIVE (or BACKWARD plus FORWARD), add the new field with a default; mark the old field deprecated; once no consumer reads it and it has a default, remove it. Under BACKWARD, consumers deploy first; under FORWARD, producers deploy first.
:::

::: check
?? Under BACKWARD compatibility, which side should upgrade first, and why?
Consumers. BACKWARD guarantees the new schema can read data written with the old one, so upgraded consumers can read both old and new data; producers then switch.

?? Why must a removed Protobuf field number be marked `reserved`?
Field numbers identify fields on the wire. If a later field reuses the number, old data or old producers will be decoded into the new field with the wrong meaning.

?? Why are fat events usually preferred for data pipelines?
They carry the relevant state, so consumers do not need to call the producer's API for details, which decouples them and makes replays reproducible.
:::

::: resources
- Docs: [Confluent: schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) · every compatibility mode with the allowed changes per format
- Spec: [Apache Avro specification](https://avro.apache.org/docs/current/specification/) · types, logical types and the schema resolution rules
- Docs: [Protocol Buffers language guide (proto3)](https://protobuf.dev/programming-guides/proto3/) · field numbers, `reserved`, `optional` and updating messages safely
- Docs: [Pub/Sub schemas](https://cloud.google.com/pubsub/docs/schemas) · Avro and Protocol Buffer schemas, revisions and validation on Google Cloud
- Book: [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) · Kleppmann; chapter 4 on encoding and evolution is the best short treatment
- Article: [Consumer-Driven Contracts](https://martinfowler.com/articles/consumerDrivenContracts.html) · Ian Robinson on letting consumer expectations drive schema evolution
:::

### Lakehouse Table Modeling (Iceberg/Delta/Hudi)

::: plain
In a lakehouse, a table is a set of files in object storage (Cloud Storage on Google Cloud) plus metadata saying which files belong to it. How you lay those files out decides whether a query reads ten files or ten thousand. Modeling a lakehouse table means choosing how files are grouped (partitioning), how rows are ordered inside them (sorting and clustering), and how updates are applied. It is like organising an archive: boxes labelled by month, documents in each box sorted by customer, and a clear rule for how corrections are filed.
:::

The Data lakes and lakehouses section explains how Apache Iceberg, Delta Lake and Apache Hudi work inside. This topic is about the design decisions you make per table.

**Start from the logical model.** A lakehouse table still needs a grain, a key and a history strategy. The physical choices then follow from the main queries and the write pattern:

| Question | Drives |
|---|---|
| Which columns do most queries filter on? | partitioning and clustering |
| How is data written: append only, upserts, deletes? | merge strategy (copy-on-write or merge-on-read) |
| How fresh must it be, how often is it written? | file sizing, compaction schedule |
| How much history must be queryable? | snapshot retention, modeled history |

**Partition specs and hidden partitioning.** Partitioning groups files by a value so queries can skip whole groups. Iceberg's **hidden partitioning** derives the partition value from a column with a **transform**, so users filter on the real column and never on a separate `event_date` column:

```sql
-- Spark SQL with Apache Iceberg
CREATE TABLE lake.events (
  event_id   STRING,
  user_id    BIGINT,
  event_type STRING,
  event_ts   TIMESTAMP,
  payload    STRING
)
USING iceberg
PARTITIONED BY (days(event_ts), bucket(16, user_id))
TBLPROPERTIES ('write.distribution-mode' = 'hash');

-- the filter is on event_ts itself; Iceberg maps it to day partitions
SELECT event_type, COUNT(*) FROM lake.events
WHERE event_ts >= TIMESTAMP '2026-11-01 00:00:00' AND event_ts < TIMESTAMP '2026-11-02 00:00:00'
GROUP BY event_type;
```

Transforms include `identity`, `years`, `months`, `days`, `hours`, `bucket(N, col)` (hash into N buckets) and `truncate(W, col)`. Iceberg also supports **partition evolution**: changing the spec only affects new data, and old files keep their old layout, with no rewrite:

```sql
ALTER TABLE lake.events REPLACE PARTITION FIELD days(event_ts) WITH hours(event_ts);
```

Delta Lake and Hudi use explicit partition columns, often a generated column (`event_date` from `event_ts`), and changing the partitioning means rewriting the table.

**How many partitions?** Over-partitioning is the most common lakehouse modeling error: partitioning by user ID or by hour on a small table creates thousands of tiny files and slows every query. Rules of thumb: partition on a low-cardinality column used in most filters (usually a date), aim for partitions that each hold at least about a gigabyte, and use clustering or sorting for high-cardinality columns.

**Sort orders and clustering.** Inside partitions, ordering rows by commonly filtered columns makes each file's min and max statistics tight, so engines skip files that cannot match:

```sql
-- Iceberg: sort data within files on write and during compaction
ALTER TABLE lake.events WRITE ORDERED BY event_type, user_id;

-- Delta Lake: liquid clustering replaces partitioning and Z-ordering
CREATE TABLE lake.orders (order_id STRING, customer_id STRING, order_ts TIMESTAMP, amount DECIMAL(12,2))
USING delta
CLUSTER BY (customer_id, order_ts);
```

Delta's **liquid clustering** lets you change clustering columns later without rewriting existing data, and is now the recommended layout for new Delta tables in place of partitioning plus Z-ordering. Z-ordering (and Iceberg's z-order sort in compaction) interleaves several columns so filters on any of them benefit somewhat.

**Merge semantics.** Upserts (`MERGE INTO`) and deletes can be applied in two ways:

| Strategy | On write | On read | Good for |
|---|---|---|---|
| Copy-on-write (COW) | rewrite every file that contains a changed row | read data files only | read-heavy tables, infrequent large updates |
| Merge-on-read (MOR) | write small delete files or deletion vectors, plus new rows | merge deletes while reading | frequent small updates, CDC streams |

Iceberg chooses per operation with `write.merge.mode`, `write.update.mode` and `write.delete.mode` (copy-on-write by default). Format version 2 records deletes in position or equality delete files; format version 3 replaces position delete files with **deletion vectors**, compact bitmaps of deleted row positions. Delta Lake also supports deletion vectors. Hudi has two table types with the same names, Copy On Write and Merge On Read, and requires a **record key** (and usually an ordering field that decides which version wins when two updates for one key arrive). Merge-on-read tables need regular **compaction**, or reads slow down as delete files pile up.

```sql
-- an idempotent CDC upsert into an Iceberg table (Spark SQL)
MERGE INTO lake.customers AS t
USING (
  SELECT customer_id, name, city, op
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY change_ts DESC) AS rn
    FROM staging.customer_changes
  ) AS ranked
  WHERE rn = 1                                  -- latest change per customer only
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.city = s.city
WHEN NOT MATCHED AND s.op != 'D' THEN
  INSERT (customer_id, name, city) VALUES (s.customer_id, s.name, s.city);
```

Deduplicate the source to one row per key first: `MERGE` fails (or behaves unpredictably, depending on the engine) when several source rows match one target row.

**Time-travel-aware design.** Every commit creates a snapshot you can query (`VERSION AS OF`, `TIMESTAMP AS OF`), but snapshots are expired and files vacuumed by maintenance jobs, for storage cost. Design accordingly:

- keep modeled history (SCD type 2, bi-temporal columns, append-only change logs) for anything the business needs to query later;
- set snapshot retention to cover debugging and recovery (days, not years);
- use **tags** and **branches** (Iceberg) to pin important versions, such as the snapshot a model was trained on or a month-end close, so expiry does not remove them;
- remember that a `MERGE` creating a new snapshot every few minutes produces thousands of snapshots a day; expire them and compact regularly.

**On Google Cloud.** Iceberg tables can live in Cloud Storage and be shared by engines through the Lakehouse runtime catalog (formerly BigLake metastore), part of Google Cloud Lakehouse (formerly BigLake). Spark on Managed Service for Apache Spark (formerly Dataproc) writes them, and BigQuery reads them. **Apache Iceberg managed tables** (formerly BigQuery tables for Apache Iceberg) let BigQuery itself manage Iceberg data in your bucket, with DML, streaming writes and automatic storage optimisation. The BigQuery in depth section covers the details and current limits.

> **Pitfall:** Partitioning a lakehouse table the way a Hive table was partitioned, with a string `dt` column that users must remember to filter. With Iceberg, partition on a transform of the real timestamp so every filter on `event_ts` prunes automatically, and nobody can forget the partition column.

::: deeper
**File sizing.** Target data files of roughly 128 MB to 1 GB for analytical tables (Iceberg's default target is 512 MB). Streaming writers produce small files; schedule compaction (`rewrite_data_files` in Iceberg, `OPTIMIZE` in Delta, clustering and compaction services in Hudi). The PySpark guide's small files section covers the mechanics.

**Bucketing for joins.** `bucket(N, key)` partitioning on two tables with the same key and bucket count lets some engines join bucket by bucket without a full shuffle (storage-partitioned joins in Spark). Choose N to give reasonably sized files at today's volume with room to grow.

**Equality deletes in streaming.** Flink writing CDC into Iceberg often uses equality deletes ("delete rows where id = 42") because it does not know file positions. They are cheap to write and expensive to read; compaction converts them away. Format version 3's deletion vectors and row lineage improve this area; check your engines' support before upgrading a table's format version.

**Modeling layers in a lakehouse.** The medallion pattern (bronze raw, silver cleaned and conformed, gold marts) maps directly onto the modeling layers in this section: bronze is append-only source data, silver holds conformed entities and history, gold holds stars, OBTs and metrics-ready tables.

> **Interview:** "Design an Iceberg table for 5 billion events a day, queried by time range and often by user." Partition by `days(event_ts)` (or `hours` if most queries cover hours), add `bucket(N, user_id)` or sort by `user_id` within partitions, keep files around 512 MB with compaction, use merge-on-read only if updates are frequent, and expire snapshots older than a few days while tagging important ones.
:::

::: check
?? What is hidden partitioning in Iceberg, and why does it help users?
The partition value is derived from a real column by a transform (such as `days(event_ts)`), so users filter on the column itself and pruning happens automatically; there is no separate partition column to forget.

?? When would you choose merge-on-read over copy-on-write?
For tables with frequent, small updates or deletes (such as CDC streams), where rewriting whole files on every change would be too expensive; accept merging at read time and schedule compaction.

?? Why should you not rely on snapshot time travel for business history?
Snapshots are expired and files vacuumed for cost, usually after days; business history must be modeled explicitly (SCD, bi-temporal columns, change logs), with important snapshots tagged if they must be kept.
:::

::: resources
- Docs: [Iceberg partitioning](https://iceberg.apache.org/docs/latest/partitioning/) · hidden partitioning, transforms and partition evolution
- Docs: [Iceberg Spark DDL](https://iceberg.apache.org/docs/latest/spark-ddl/) · `PARTITIONED BY`, `WRITE ORDERED BY`, partition field changes and table properties
- Docs: [Delta Lake liquid clustering](https://docs.delta.io/latest/delta-clustering.html) · `CLUSTER BY`, changing keys and how clustering replaces partitioning
- Docs: [Apache Hudi table types](https://hudi.apache.org/docs/table_types) · Copy On Write and Merge On Read, with their write and query trade-offs
- Docs: [BigQuery: Apache Iceberg managed tables](https://cloud.google.com/bigquery/docs/iceberg-tables) · BigQuery-managed Iceberg tables in Cloud Storage
- Book: [Apache Iceberg: The Definitive Guide](https://www.oreilly.com/library/view/apache-iceberg-the/9781098148614/) · Shiran, Hughes and Merced; table design, maintenance and engine integration
:::

### Graph & Network Data Modeling

::: plain
Some data is mostly about connections: who knows whom, which account sent money to which, which parts go into which machine. A graph model stores **nodes** (the things) and **edges** (the connections) as first-class data, so "friends of friends" or "all accounts within three transfers of this one" are natural questions rather than chains of self-joins. It is like a map of a metro system: the stations matter, but the lines between them are the point.
:::

**Two graph models.**

| | Property graph | RDF (Resource Description Framework) |
|---|---|---|
| Building blocks | nodes and edges with labels and key-value properties | triples: subject, predicate, object |
| Identity | internal or user-defined IDs | global IRIs (web-style identifiers) |
| Edge properties | yes, directly | needs reification or RDF-star style statements |
| Schema | optional constraints, graph types | ontologies (RDFS, OWL), validation with SHACL |
| Query languages | Cypher, GQL (ISO/IEC 39075:2024), SQL/PGQ | SPARQL |
| Typical use | application graphs, fraud, recommendations | knowledge graphs, data integration across organisations, linked open data |

Property graphs dominate data engineering use. RDF shines when many organisations must share and merge vocabularies (life sciences, publishing, government).

**Modeling a property graph.** Start from the questions, then decide what is a node, what is an edge and what is a property:

- **Nodes** are entities with identity that you traverse through: Person, Account, Device, Merchant.
- **Edges** are relationships, with a direction and a type: `(:Person)-[:OWNS]->(:Account)`, `(:Account)-[:TRANSFERRED {amount, at}]->(:Account)`.
- **Properties** are attributes you filter on or return, not things you traverse through.

A useful test: if you will ever ask "which other X share this value?", the value should probably be a node. Two accounts that log in from the same device is a fraud signal, so Device is a node with `USED` edges, not a `device_id` property on accounts.

**Edges or nodes for events?** A transfer between two accounts can be an edge with properties. But if a transfer has its own relationships (it was approved by an employee, flagged by a rule, reversed by another transfer), promote it to a node (`(:Account)-[:SENT]->(:Transfer)-[:TO]->(:Account)`). This is the graph version of resolving a many-to-many relationship into an associative entity.

**Identity.** Give every node a stable business identifier (the account number, a hashed email), with a uniqueness constraint, and keep internal database IDs out of pipelines: they can be reused after deletion. Edges between the same pair of nodes may need an identifier too (two transfers on the same day), usually from the source transaction ID.

**Supernodes.** A **supernode** (dense node) has an enormous number of edges: a popular celebrity, a default merchant ID, a placeholder email such as `unknown@example.com`, or a country node that every person points to. Traversals that pass through it explode. Mitigations:

- do not model very low-cardinality attributes as nodes (country is usually a property);
- clean placeholder values in the pipeline before they become nodes;
- use more specific edge types (`FOLLOWS_2026_11` or `PURCHASED_IN_STORE`) or intermediate bucket nodes (by time period) so traversals touch fewer edges;
- filter on edge properties early in queries, and cap traversal depth.

**Path query patterns.** Graph queries describe patterns. In Cypher:

```text
// accounts within 1 to 3 transfers of a flagged account, in the last 30 days
MATCH path = (a:Account {flagged: true})-[:TRANSFERRED*1..3]->(b:Account)
WHERE all(t IN relationships(path) WHERE t.at >= datetime() - duration('P30D'))
RETURN b.account_id, length(path) AS hops
ORDER BY hops
LIMIT 100;
```

GQL, the ISO graph query language published in 2024, standardises this style, and SQL/PGQ (part of SQL:2023) adds graph pattern matching over ordinary relational tables.

**Hybrid relational and graph designs.** You do not always need a separate graph database. **Spanner Graph** on Google Cloud defines a property graph over existing Spanner tables, so the same data is queried with SQL and with GQL-style graph queries:

```sql
CREATE PROPERTY GRAPH FinGraph
  NODE TABLES (Person, Account)
  EDGE TABLES (
    PersonOwnAccount
      SOURCE KEY (id) REFERENCES Person (id)
      DESTINATION KEY (account_id) REFERENCES Account (id)
      LABEL Owns,
    AccountTransferAccount
      SOURCE KEY (id) REFERENCES Account (id)
      DESTINATION KEY (to_id) REFERENCES Account (id)
      LABEL Transfers
  );

GRAPH FinGraph
MATCH (p:Person {name: "Dana"})-[:Owns]->(:Account)-[t:Transfers]->(dest:Account)
RETURN dest.id AS destination, t.amount;
```

The tables are the system of record; the graph is a view over them. A similar pattern in analytics is to keep node and edge tables in BigQuery (`nodes(node_id, label, properties)` and `edges(src_id, dst_id, type, properties, valid_from, valid_to)`) and load them into a graph engine or run graph algorithms in Spark when needed.

**Graph schema constraints.** Neo4j supports property uniqueness, existence, type and key constraints (several are Enterprise Edition features), and newer versions can define a whole graph type. RDF uses SHACL shapes to validate graphs. Whatever the store, enforce identity uniqueness and required properties, because graph queries silently return wrong paths when duplicate nodes split one entity in two.

> **Pitfall:** Loading a relational schema into a graph table by table, with every foreign key as an edge and every lookup table as a node. The result has supernodes (every order points to the "status = paid" node) and gains nothing. Model the graph from the traversal questions, not from the source schema.

::: deeper
**Temporal graphs.** Relationships change: an account owner changes, a device is reassigned. Put validity intervals on edges (`valid_from`, `valid_to`) and filter traversals by time, or snapshot the graph per period. Fraud investigations usually need "the graph as of the transaction time".

**Graph algorithms versus queries.** Pattern queries answer local questions ("who is connected to X?"). Global algorithms (PageRank, connected components, community detection, shortest paths over the whole graph) run better in batch engines: Spark GraphFrames, Neo4j Graph Data Science, or SQL with iterative jobs. Connected components over identity edges is also a common way to build MDM match groups.

**RDF and property graphs together.** Knowledge graphs are increasingly fed to AI systems (graph-based retrieval). The modeling advice is the same: stable identifiers, explicit relationship types, provenance on each fact. The Graph data engineering section covers processing and storage engines in depth.

> **Interview:** "Model a social network's follow graph for recommendations." Users as nodes with a stable ID, FOLLOWS as directed edges with a created time, posts as nodes with AUTHORED and LIKED edges; discuss celebrity supernodes (sample or cap edges, precompute recommendations offline), and keep the source of truth in relational tables with the graph derived from them.
:::

::: check
?? How do you decide whether a value should be a node or a property?
If you will traverse through it or ask which other entities share it (a device shared by accounts), make it a node; if you only filter on it or return it, keep it a property.

?? What is a supernode, and name two ways to mitigate one.
A node with an enormous number of edges that makes traversals explode. Keep low-cardinality attributes as properties, clean placeholder values, use more specific edge types or time-bucketed intermediate nodes, and cap traversal depth.

?? What does Spanner Graph's `CREATE PROPERTY GRAPH` do?
It defines nodes and edges over existing Spanner tables, so the same data can be queried with SQL and with graph pattern queries without copying it into a separate graph database.
:::

::: resources
- Book: [Graph Databases, 2nd edition](https://www.oreilly.com/library/view/graph-databases-2nd/9781491930885/) · Robinson, Webber and Eifrem; graph modeling patterns and pitfalls
- Docs: [Neo4j: graph data modeling](https://neo4j.com/docs/getting-started/data-modeling/) · nodes, relationships, properties and refactoring a model
- Docs: [Spanner Graph overview](https://cloud.google.com/spanner/docs/graph/overview) · property graphs over Spanner tables, with GQL queries
- Spec: [RDF 1.1 Primer](https://www.w3.org/TR/rdf11-primer/) · triples, IRIs and graphs from the W3C
- Docs: [Neo4j Cypher constraints](https://neo4j.com/docs/cypher-manual/current/constraints/) · uniqueness, existence, type and key constraints for graph schemas
:::

### Industry Data Models & Standards

::: plain
Many industries have agreed, through standards bodies, on how common data should be shaped: hospitals exchange patient records in HL7 FHIR, banks send payments as ISO 20022 messages, and retailers identify products with GS1 barcodes. Using these standards is like using the standard shipping container: everyone's cranes, ships and trucks already fit it. Your internal model does not have to be the standard, but it must map to it cleanly.
:::

**Why standards matter for data engineers.** Partners send data in standard formats, regulators require them, and vendors' software speaks them. Knowing the standard model of your industry tells you which entities and codes to expect, saves months of design, and makes integration with partners a mapping rather than a negotiation. The Industry patterns section at the end of the course shows these industries end to end; here the focus is on the models.

**Healthcare: HL7 FHIR.** FHIR (Fast Healthcare Interoperability Resources, pronounced "fire") models healthcare data as **resources**: Patient, Practitioner, Encounter (a visit), Observation (a measurement or lab result), Condition, MedicationRequest and about 150 more. Each resource is a JSON (or XML) document with an `id`, typed fields, and **references** to other resources. Codes come from terminologies such as LOINC (lab tests), SNOMED CT (clinical terms) and ICD-10 (diagnoses):

```json
{
  "resourceType": "Observation",
  "id": "obs-1",
  "status": "final",
  "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4", "display": "Heart rate"}]},
  "subject": {"reference": "Patient/pat-42"},
  "effectiveDateTime": "2026-11-20T09:30:00Z",
  "valueQuantity": {"value": 72, "unit": "beats/minute", "system": "http://unitsofmeasure.org", "code": "/min"}
}
```

Extensions add fields the base spec lacks; **profiles** (often national, such as US Core) constrain resources for a use case. R4 is the most widely deployed release and R5 was published in 2023. For analytics, FHIR's nested documents must be flattened: the **SQL on FHIR** specification defines portable **ViewDefinitions** that turn resources into tables, and the Google Cloud Healthcare API's FHIR stores can stream resources into BigQuery. A typical warehouse then has fact tables of observations and encounters and dimensions for patients, practitioners and codes.

**Finance: ISO 20022, card standards and PCI.** **ISO 20022** is the XML-based (with a JSON representation) message standard for payments and securities: `pain.001` (a customer instructs a bank to pay), `pacs.008` (bank to bank credit transfer), `camt.053` (a bank statement). SWIFT's cross-border payments have moved to ISO 20022, and many domestic instant-payment systems use it. Its messages carry far richer structured data (full party names, addresses, remittance details) than older formats. **ISO 8583** is the long-standing format of card authorisation messages. The **PCI DSS** security standard governs card data: never store sensitive authentication data such as the card verification code after authorisation, and make the card number (PAN) unreadable wherever it is stored. The modeling consequence is to **tokenise** card numbers before they enter the data platform, so warehouse tables hold tokens and the platform stays out of most of PCI's scope. The EDM Council's **FIBO** (Financial Industry Business Ontology) is a reference ontology for financial concepts.

A typical finance modeling rule beyond the standards: money is a decimal amount plus an ISO 4217 currency code, and ledgers are double entry and append only (each transaction posts balanced debit and credit lines; corrections are new reversing entries, never updates).

**Retail and supply chain: GS1.** **GS1** standards identify things in supply chains:

| Key | Identifies | Example use |
|---|---|---|
| GTIN | a trade item (product variant) | the barcode on a tin of tea |
| GLN | a location or party | a store, a warehouse dock |
| SSCC | a logistics unit | one pallet on a truck |

**EPCIS 2.0** models supply chain events with what, when, where and why (an object was observed, packed, shipped or received at a location for a business step), with a JSON-LD representation. **GS1 Digital Link** encodes GS1 keys in web URIs, which is what 2D barcodes on products increasingly carry. Use the GTIN as the natural key of the product dimension's sellable unit, and GLNs for locations, when your partners use them.

**Canonical data models.** A **canonical model** is one shared format into which every system's data is translated. With n systems exchanging data directly you need up to n × (n - 1) translations; with a canonical model, 2n (each system to and from the canonical form). Industry standards are often used as the canonical model for exchange.

| Benefit | Risk |
|---|---|
| fewer mappings | an enterprise-wide model that tries to fit every team becomes huge and slow to change |
| shared vocabulary | lowest-common-denominator fields that fit nobody well |
| partners integrate faster | versioning one model across all systems is hard |

Domain-Driven Design's advice applies: use a canonical model for **exchange** at the boundaries (often the industry standard), and let each bounded context keep its own internal model, with explicit translations.

> **Pitfall:** Using the exchange standard as the warehouse model directly, for example querying raw FHIR JSON for every report. Standards are designed for complete, flexible exchange, not for analytics. Land them as received, then model facts and dimensions from them.

::: deeper
**Versioning standards.** Standards evolve (FHIR R4 to R5, EPCIS 1.2 to 2.0, PCI DSS 3.2.1 to 4.0 and then 4.0.1). Record the standard version on every landed record or file, keep mappings per version, and plan migrations as you would for a schema change.

**Terminology services.** Clinical and product codes form large, versioned hierarchies (SNOMED CT has hundreds of thousands of concepts). Load them as reference data with validity dates and hierarchy tables, as in the reference data topic, rather than hard-coding the codes your reports use.

**Privacy and regulation.** Healthcare and finance data carry legal obligations (patient privacy rules, banking secrecy, card security). Standards help, but you still need de-identification, access control and audit, covered in Data governance, security and compliance.

> **Interview:** "You receive FHIR Observation resources and need a dashboard of average heart rate per ward per day. How do you model it?" Land the raw resources, flatten with a ViewDefinition or SQL into an observation fact (patient key, encounter key, code, value, unit, effective time), conform units, join encounters to wards (with validity dates, since patients move), filter on the LOINC heart-rate code, and aggregate.
:::

::: check
?? What is a FHIR resource, and how do resources refer to each other?
A typed JSON or XML document for one healthcare concept (Patient, Observation, Encounter); resources refer to each other by references such as `"Patient/pat-42"`.

?? How does tokenising card numbers help a data platform?
Warehouse tables hold tokens instead of card numbers, so most of the platform never stores card data and stays out of most of PCI DSS's scope.

?? What is the trade-off of a canonical data model?
It cuts the number of mappings between systems (2n instead of up to n × (n - 1)), but an enterprise-wide canonical model risks becoming huge, slow to change and a poor fit for every team; use it for exchange at boundaries.
:::

::: resources
- Spec: [HL7 FHIR](https://hl7.org/fhir/) · the current FHIR specification, with every resource, data type and terminology binding
- Spec: [SQL on FHIR](https://sql-on-fhir.org/) · ViewDefinitions for flattening FHIR resources into analytical tables
- Docs: [Cloud Healthcare API: FHIR](https://cloud.google.com/healthcare-api/docs/concepts/fhir) · FHIR stores on Google Cloud and export to BigQuery
- Docs: [ISO 20022](https://www.iso20022.org/) · the financial messaging standard, its message catalogue and data dictionary
- Docs: [GS1 standards](https://www.gs1.org/standards) · identification keys (GTIN, GLN, SSCC), EPCIS and Digital Link
- Docs: [PCI Security Standards Council](https://www.pcisecuritystandards.org/) · PCI DSS and guidance on storing and protecting card data
:::

### Data Contract Modeling

::: plain
A data contract is a written, machine-checkable promise from the team that produces a dataset to the teams that use it: what columns it has, what they mean, how fresh and complete it will be, and how changes will be announced. It is like a service agreement with a supplier: the supplier promises what will arrive, when and in what condition, and both sides know what happens if the promise is broken.
:::

**Why contracts.** Most pipeline breakages start upstream: an application team renames a column, changes a status code or stops sending a field, and a dashboard three hops away breaks days later. The application team did not know anyone depended on it. A data contract makes the dependency explicit and turns it into checks that run automatically, ideally before the producer's change is deployed.

**What a contract contains.**

| Part | Examples |
|---|---|
| Identity and ownership | dataset name, version, owning team, contact, status (draft, active, deprecated) |
| Schema | columns, logical types, required, primary key, allowed values |
| Semantics | descriptions, units, business definitions, grain |
| Quality guarantees | uniqueness, completeness thresholds, referential rules, row-count bounds |
| Service levels | freshness (data no older than 6 hours), availability, support hours, retention |
| Change policy | what counts as breaking, notice period, versioning, deprecation window |
| Access and classification | PII columns, allowed uses, who may read |

**Open formats.** Two open YAML formats exist. The **Open Data Contract Standard** (ODCS), maintained by the Bitol project under the Linux Foundation, and the **Data Contract Specification**; the open-source **Data Contract CLI** can lint contracts and test real data against them. An abridged ODCS-style contract:

```yaml
# abridged; see the ODCS documentation for every field
apiVersion: v3.0.0
kind: DataContract
id: orders-contract
name: Orders
version: 1.2.0
status: active
domain: commerce
description:
  purpose: One row per confirmed order, for revenue reporting and fulfilment analytics.
schema:
  - name: orders
    physicalName: mart.fct_orders
    properties:
      - name: order_id
        logicalType: string
        required: true
        unique: true
        primaryKey: true
      - name: status
        logicalType: string
        required: true
        description: pending, paid, shipped or cancelled
      - name: amount
        logicalType: number
        required: true
        description: order total in EUR after discounts, before tax
      - name: ordered_at
        logicalType: timestamp
        required: true
slaProperties:
  - property: latency
    value: 6
    unit: h
```

**Checking a contract.** The checks themselves are simple; their value is running them automatically at the boundary. A minimal checker runs as is:

```python
from datetime import datetime, timedelta, timezone

contract = {
    "dataset": "orders", "version": "1.2.0", "owner": "commerce-data",
    "freshness_hours": 6,
    "columns": {
        "order_id":   {"type": str,   "required": True, "unique": True},
        "status":     {"type": str,   "required": True,
                       "accepted": {"pending", "paid", "shipped", "cancelled"}},
        "amount":     {"type": float, "required": True, "min": 0},
        "ordered_at": {"type": str,   "required": True},
    },
}

def check(rows, contract, now):
    errors, seen = [], {}
    for i, row in enumerate(rows):
        for col, rule in contract["columns"].items():
            v = row.get(col)
            if v is None:
                if rule.get("required"): errors.append(f"row {i}: {col} is missing")
                continue
            if not isinstance(v, rule["type"]):
                errors.append(f"row {i}: {col} has type {type(v).__name__}")
            elif "accepted" in rule and v not in rule["accepted"]:
                errors.append(f"row {i}: {col}={v!r} not accepted")
            elif "min" in rule and v < rule["min"]:
                errors.append(f"row {i}: {col}={v} below {rule['min']}")
            if rule.get("unique"):
                if v in seen: errors.append(f"row {i}: {col}={v!r} duplicates row {seen[v]}")
                seen.setdefault(v, i)
    newest = max(datetime.fromisoformat(r["ordered_at"]) for r in rows if r.get("ordered_at"))
    if now - newest > timedelta(hours=contract["freshness_hours"]):
        errors.append(f"stale: newest row is {now - newest} old")
    return errors

rows = [
    {"order_id": "101", "status": "paid",     "amount": 40.0, "ordered_at": "2026-11-23T08:00:00+00:00"},
    {"order_id": "102", "status": "refunded", "amount": 15.5, "ordered_at": "2026-11-23T08:05:00+00:00"},
    {"order_id": "101", "status": "paid",     "amount": -3.0, "ordered_at": "2026-11-23T08:10:00+00:00"},
    {"order_id": "104", "status": "paid",     "amount": "60", "ordered_at": None},
]
for e in check(rows, contract, now=datetime(2026, 11, 23, 16, 0, tzinfo=timezone.utc)):
    print(e)
# row 1: status='refunded' not accepted
# row 2: order_id='101' duplicates row 0
# row 2: amount=-3.0 below 0
# row 3: amount has type str
# row 3: ordered_at is missing
# stale: newest row is 7:50:00 old
```

In production the same checks are dbt tests, Knowledge Catalog data quality rules, Data Contract CLI runs or Great Expectations suites, generated from the contract file.

**Ownership boundaries.** A contract sits at a boundary between teams. The producer owns the contract and its guarantees; consumers can propose changes and register as dependents. Inside a team's own pipeline, ordinary tests are enough. Put contracts where data crosses an ownership line: application database to data platform (often enforced on CDC or event schemas), and data product to its consumers.

**Breaking-change policy.** Write down what counts as breaking:

| Breaking | Not breaking |
|---|---|
| removing or renaming a column | adding a nullable column |
| changing a type or unit | adding an accepted value, if consumers were told to expect new values |
| changing meaning or grain | improving a description |
| tightening freshness or quality guarantees downward | loosening a constraint the producer controls internally |

Breaking changes require a new **major version**, notice to registered consumers, and a period in which both versions are published (for example 30 or 90 days). Non-breaking changes bump the minor version. This is semantic versioning applied to data.

**Enforcement points.** Contracts are most useful when checked early:

1. In the **producer's CI**: a schema change that breaks the contract fails the build before deployment.
2. At **ingestion**: rows that violate the contract go to a quarantine table, and the producer is alerted.
3. In the **transformation layer**: dbt model contracts enforce column names and types when a model builds.

> **Pitfall:** Writing contracts nobody enforces. A YAML file that is not checked in CI or at ingestion decays within weeks and becomes another out-of-date document. Start with one critical dataset, enforce it automatically, and expand from there.

::: deeper
**Contracts and data mesh.** In a data mesh, each domain publishes data products; the contract is the product's interface, like an API specification for a service. Catalogs (Knowledge Catalog, DataHub) can display contracts, owners and service levels next to the data, and BigQuery sharing exposes products to other organisations.

**Contracts at the source.** The strongest contracts are enforced by the application team itself: the outbox table or event schema is the contract, checked by the schema registry and the producer's tests, so the data platform never receives breaking data. This needs buy-in from application teams, which is the hard part: frame contracts as protecting them from being paged about dashboards.

**Measuring service levels.** A freshness guarantee is a service level objective (SLO): measure it (the age of the newest row, the time a daily partition landed), report the share of time it was met, and agree what happens when it is missed. The DataOps and platform engineering section covers SLOs for data.

> **Interview:** "An upstream team keeps breaking your pipeline with schema changes. What do you do?" Propose a contract for the dataset: agree the schema and breaking-change rules with them, add an automated compatibility check to their CI, quarantine violating data at ingestion with alerts to the producer, and version breaking changes with a deprecation window.
:::

::: check
?? Where should data contracts be placed in a platform?
At ownership boundaries: where data passes from one team to another, such as an application database or event stream into the data platform, and data products to their consumers.

?? Is adding a nullable column a breaking change? Is renaming a column?
Adding a nullable column is normally not breaking; renaming (or removing) a column is breaking and needs a new major version and a deprecation window.

?? Why is a contract checked in the producer's CI more useful than one checked only in the consumer's pipeline?
It stops the breaking change before it is deployed, instead of detecting broken data after it has already arrived.
:::

::: resources
- Spec: [Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/) · the Bitol project's ODCS reference, with every section and example contracts
- Docs: [Data Contract Specification](https://datacontract.com/) · an alternative open format, with the Data Contract CLI for linting and testing
- Repo: [Data Contract CLI](https://github.com/datacontract/datacontract-cli) · test data against contracts, check for breaking changes and export to other formats
- Article: [Data contracts: a pragmatic introduction](https://www.thoughtworks.com/en-us/insights/blog/data-engineering/data-contracts-pragmatic-introduction) · Thoughtworks on where contracts help and how to start
- Article: [How to move beyond a monolithic data lake to a distributed data mesh](https://martinfowler.com/articles/data-monolith-to-mesh.html) · Zhamak Dehghani's article that framed data products and their interfaces
- Book: *Driving Data Quality with Data Contracts* by Andrew Jones · a practical book on designing and rolling out contracts
:::

## Modeling in practice

A model is never finished. Businesses change, sources add and drop fields, and new questions arrive every week. The first three groups covered how to design models; this group covers how to live with them: changing schemas without breaking consumers, testing that models stay correct, documenting them so others can use them, tuning them for the warehouse you actually run, recording metadata and lineage, diagramming and reviewing changes, and finally putting it all together in a portfolio project and a data modeling interview.

The common thread is **models as code**. Table definitions, tests, documentation, diagrams and decisions live in a Git repository, change through pull requests, and are checked automatically in CI. This is the same discipline the DataOps and platform engineering section applies to pipelines and infrastructure.

### Schema Evolution, Versioning & Migration

::: plain
Changing a table that people and programs already use is like repairing a road while traffic is still driving on it. You cannot close it and rebuild it overnight. Instead you open a new lane, move traffic onto it gradually, and only then remove the old lane. Schema migrations follow the same pattern: add the new shape alongside the old one, move readers and writers across, then remove the old shape.
:::

**Kinds of change.** Sort every proposed change by its effect on existing readers and writers:

| Change | Effect on existing consumers |
|---|---|
| add a nullable column or table | safe |
| add a value to an enumeration | usually safe, if consumers handle unknown values |
| widen a type (INT64 to NUMERIC, VARCHAR(50) to VARCHAR(100)) | usually safe |
| rename a column or table | breaking |
| remove a column | breaking for anyone reading it |
| narrow a type, change a unit or meaning, change the grain | breaking |
| make a nullable column required | breaking for writers |

Safe changes go straight through review. Breaking changes need the expand and contract process.

**Expand and contract (parallel change).** Rename `customers.fullname` to `display_name` without downtime:

1. **Expand**: add `display_name` (nullable). Nothing breaks, because nothing uses it yet.
2. **Dual write**: deploy writers that write both columns.
3. **Backfill**: copy existing values into the new column, in batches.
4. **Migrate readers**: switch every query, model and dashboard to `display_name`. Lineage (later in this group) tells you who they are.
5. **Contract**: stop writing `fullname`, wait through the announced deprecation window, then drop it.

Each step is a separate, reversible deployment, so a failure at any step can be rolled back without data loss. The backfill must not lock the table for long. A batched backfill runs in SQLite as is:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, fullname TEXT)")
db.executemany("INSERT INTO customers (fullname) VALUES (?)", [(f"name {i}",) for i in range(1, 2501)])

# expand: add the new column next to the old one (old code keeps working)
db.execute("ALTER TABLE customers ADD COLUMN display_name TEXT")

# backfill in small batches so no single transaction locks the table for long
batches, last_id = 0, 0
while True:
    ids = [r[0] for r in db.execute(
        "SELECT id FROM customers WHERE id > ? AND display_name IS NULL ORDER BY id LIMIT 1000",
        (last_id,))]
    if not ids:
        break
    db.execute("UPDATE customers SET display_name = fullname WHERE id BETWEEN ? AND ?",
               (ids[0], ids[-1]))
    db.commit()
    batches, last_id = batches + 1, ids[-1]

missing = db.execute("SELECT COUNT(*) FROM customers WHERE display_name IS NULL").fetchone()[0]
print(batches, "batches,", missing, "rows left")   # 3 batches, 0 rows left
```

The loop walks the primary key in ranges (keyset pagination), commits after each batch, and can be stopped and restarted safely because it only touches rows still missing a value.

**Migration tools.** Operational databases change through versioned migration scripts, applied in order and recorded in a history table so each runs exactly once:

```text
migrations/
  V1__create_customers.sql
  V2__create_orders.sql
  V3__add_display_name.sql          -- expand
  V4__backfill_display_name.sql     -- or a batched job outside the migration
  V5__drop_fullname.sql             -- contract, weeks later
```

Flyway and Liquibase (JVM ecosystem), Alembic (Python, SQLAlchemy) and Atlas (declarative: you describe the desired schema and it plans the changes) are common. In the warehouse, dbt models are rebuilt from code, so "migration" mostly means changing the model and handling incremental tables (`on_schema_change: append_new_columns` or `sync_all_columns` for incremental models).

**Warehouse and lakehouse specifics.**

- **BigQuery** allows adding nullable columns, relaxing `REQUIRED` to `NULLABLE`, renaming and dropping columns (`ALTER TABLE ... RENAME COLUMN`, `DROP COLUMN`), and some type widenings (`ALTER COLUMN ... SET DATA TYPE`, for example `INT64` to `NUMERIC`). Other type changes require rewriting the table with a query. Dropped columns stop being queryable immediately, so contract only after readers have moved.
- **Iceberg** tracks columns by ID rather than name, so adds, drops, renames, reorders and type widenings are metadata-only and never confuse old data files. Delta Lake supports renames and drops with **column mapping** enabled.
- **Views as a compatibility layer.** Expose consumers to a view (`mart.customers_v1`) rather than the table. During a breaking change, publish `customers_v2` alongside, and drop `v1` after the deprecation window.

**Testing compatibility in CI.** Before a model change merges, CI should:

1. build the changed models and everything downstream in a temporary schema (in dbt, `dbt build --select state:modified+` against a copy of production state);
2. compare the new schema with the old one and flag removed or retyped columns (dbt model contracts do this for contracted models: a breaking change to an enforced contract fails unless the model's version is bumped);
3. compare data: row counts and key aggregates between old and new versions of each changed model (a "data diff");
4. check event and contract schemas against the registry's compatibility rules.

> **Pitfall:** Renaming a column in one step because "only our dashboard uses it". Lineage usually reveals three more consumers: a notebook, an export job and another team's model. Use expand and contract even when you are sure, and announce deprecations where consumers will see them (the catalog, the contract, a changelog).

::: deeper
**Versioned models in dbt.** dbt supports model **versions**: `fct_orders` v1 and v2 can exist side by side, each with its own contract, with a `latest_version` and deprecation dates, and downstream `ref('fct_orders', v=1)` pins an old version during migration. This is expand and contract made explicit in the project.

**Online schema change in MySQL and PostgreSQL.** Large table rewrites in MySQL are commonly done with tools such as gh-ost or pt-online-schema-change, which copy the table in the background and swap it in. In PostgreSQL, most additive changes are fast metadata changes; the dangerous operations are those that rewrite the table or take strong locks (some type changes, adding a volatile default), covered in the physical modeling topic.

**Backfills at scale.** In warehouses, backfill by partition (one day at a time) so each step is idempotent and restartable, and throttle to protect slot or cluster capacity. Record which partitions are done in a control table rather than relying on the job's memory.

**CDC and schema changes.** When a source table changes, change data capture streams carry the change. Datastream and Debezium propagate many source DDL changes to the destination automatically (new columns appear), but renames and type changes can still break downstream models. Treat source schema changes as contract events and alert on them.

> **Interview:** "How would you change the type of a heavily used column from STRING to INT64 in BigQuery?" Expand: add a new `INT64` column, backfill it with `SAFE_CAST` per partition, and log values that fail to cast. Migrate readers through a view or contract version, dual write during the transition, then drop the old column after the deprecation window.
:::

::: check
?? What are the steps of expand and contract for a column rename?
Add the new column; write both columns; backfill the new one; move every reader to it; stop writing the old one and drop it after a deprecation window.

?? Why do batched backfills commit after each batch and select only rows still missing a value?
To keep each transaction and its locks short, and so the job can be stopped and restarted at any point without redoing or skipping work.

?? Why are column renames metadata-only in Iceberg?
Iceberg identifies columns by a stable field ID stored in the data files' metadata, not by name, so a rename changes only the table metadata and old files are still read correctly.
:::

::: resources
- Article: [Parallel Change](https://martinfowler.com/bliki/ParallelChange.html) · Martin Fowler's description of expand, migrate and contract
- Article: [Evolutionary Database Design](https://martinfowler.com/articles/evodb.html) · Sadalage and Fowler on migrations, refactoring and database changes in agile teams
- Docs: [Flyway documentation](https://documentation.red-gate.com/flyway) · versioned migrations, schema history and repeatable scripts
- Docs: [Liquibase: database version control](https://www.liquibase.com/resources/guides/database-version-control) · changelogs, rollbacks and change tracking
- Docs: [BigQuery: modifying table schemas](https://cloud.google.com/bigquery/docs/managing-table-schemas) · which schema changes BigQuery allows in place
- Docs: [Iceberg schema evolution](https://iceberg.apache.org/docs/latest/evolution/) · schema and partition evolution without rewriting data
:::

### Model Quality, Testing & Observability

::: plain
A model can be perfectly designed and still produce wrong numbers if the data inside it breaks its rules: duplicate keys, orders pointing at customers that do not exist, revenue that no longer matches the finance system. Tests check those rules every time the model is built, like a factory inspecting each batch before it ships. Observability watches the data over time and raises an alarm when something looks unusual, like a smoke detector for problems nobody wrote a test for.
:::

**What to test in a model.** The design decisions from earlier topics become tests:

| Design decision | Test |
|---|---|
| primary key / grain | the key (or key combination) is unique and not null |
| relationships | every foreign key exists in the parent (referential integrity) |
| cardinality | joining to a dimension does not change the fact's row count (no fan-out) |
| allowed values | status in the accepted list; amounts within a range |
| SCD type 2 | each key has exactly one current row; validity intervals do not overlap or leave gaps |
| business rules | net amount = gross minus discount; shipped date not before order date |
| reconciliation | totals match the source or finance system within a tolerance |
| freshness and volume | newest row is recent; today's row count is within the expected range |

**Tests as queries that return failures.** The convention dbt popularised: a test is a query that returns the rows that break the rule, and it passes when it returns nothing. A small suite runs in SQLite as is:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE dim_customer (customer_key INT, customer_id INT, city TEXT);
INSERT INTO dim_customer VALUES (-1, NULL, 'unknown'), (11, 1, 'Pune'), (21, 2, 'Oslo'), (21, 2, 'Oslo');
CREATE TABLE fct_orders (order_id INT, customer_key INT, status TEXT, amount REAL);
INSERT INTO fct_orders VALUES (101, 11, 'paid', 40.0), (102, 21, 'cancelled', 15.5),
  (103, 11, 'paid', 22.0), (104, 31, 'paid', 60.0), (105, 21, 'refunded', 35.0);
CREATE TABLE src_orders (order_id INT, amount REAL);
INSERT INTO src_orders VALUES (101, 40.0), (102, 15.5), (103, 22.0), (104, 60.0), (105, 35.0);
""")
# Each test is a query that returns the failing rows: zero rows means pass.
tests = {
  "unique dim_customer.customer_key":
    "SELECT customer_key, COUNT(*) FROM dim_customer GROUP BY customer_key HAVING COUNT(*) > 1",
  "not_null fct_orders.customer_key":
    "SELECT order_id FROM fct_orders WHERE customer_key IS NULL",
  "relationships fct_orders -> dim_customer":
    """SELECT f.order_id, f.customer_key FROM fct_orders f
       LEFT JOIN dim_customer d ON d.customer_key = f.customer_key WHERE d.customer_key IS NULL""",
  "accepted_values fct_orders.status":
    "SELECT order_id, status FROM fct_orders WHERE status NOT IN ('paid','cancelled','pending')",
  "no fan-out joining orders to customers":
    """SELECT (SELECT COUNT(*) FROM fct_orders f JOIN dim_customer d USING (customer_key)) AS joined,
              (SELECT COUNT(*) FROM fct_orders WHERE customer_key IN (SELECT customer_key FROM dim_customer)) AS expected
       WHERE joined <> expected""",
  "reconciles with source within 0.5%":
    """SELECT t, s FROM (SELECT (SELECT SUM(amount) FROM fct_orders) AS t,
                               (SELECT SUM(amount) FROM src_orders) AS s)
       WHERE ABS(t - s) > 0.005 * s""",
}
for name, sql in tests.items():
    bad = db.execute(sql).fetchall()
    print("PASS" if not bad else "FAIL", name, bad if bad else "")
# FAIL unique dim_customer.customer_key [(21, 2)]
# PASS not_null fct_orders.customer_key
# FAIL relationships fct_orders -> dim_customer [(104, 31)]
# FAIL accepted_values fct_orders.status [(105, 'refunded')]
# FAIL no fan-out joining orders to customers [(6, 4)]
# PASS reconciles with source within 0.5%
```

The duplicate customer 21 causes the fan-out failure: orders 102 and 105 each join to two dimension rows, so 4 matching orders become 6 joined rows. One root cause shows up in two tests, which is typical.

**The same tests in dbt.**

```yaml
models:
  - name: fct_orders
    description: One row per order.
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: customer_key
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: status
        data_tests:
          - accepted_values:
              values: ['pending', 'paid', 'shipped', 'cancelled']
```

Packages add more generic tests (`dbt_utils.unique_combination_of_columns` for composite grains, `dbt_expectations` for distribution checks). **Unit tests** (dbt 1.8 and later) check transformation logic on small, fixed input rows with expected output, before any real data is involved, which is the right tool for tricky SQL such as SCD logic or currency conversion.

**Severity and placement.** Not every failure should stop the pipeline. Set errors for rules whose violation makes the data wrong (unique keys, referential integrity on facts) and warnings for rules that signal investigation (a new status value, a small reconciliation gap). Test early: a uniqueness test on the staging model catches duplicates before they fan out through ten downstream models.

**Observability.** Tests catch the failures you predicted. **Data observability** watches for the ones you did not, by tracking metrics over time and alerting on anomalies:

- **freshness**: when did the table last change, compared with its usual schedule?
- **volume**: is today's row count far from the usual range for this weekday?
- **schema**: did columns appear, disappear or change type?
- **distribution**: did the null rate of `email` jump from 1% to 40%? Did the average order value halve?
- **lineage**: which upstream change explains it, and which downstream assets are affected?

On Google Cloud, **Knowledge Catalog** data quality scans run rule-based checks (and data profiling) on BigQuery tables on a schedule and publish results to the catalog; open-source tools such as Elementary (on top of dbt) and commercial platforms such as Monte Carlo add anomaly detection and incident workflow. The Data governance section covers observability platforms in depth; the Data warehousing section covers testing strategy for a whole warehouse.

> **Pitfall:** Hundreds of tests that always warn and nobody reads. Alert fatigue makes a test suite worthless. Every test should have an owner and a clear action on failure; delete or fix tests that are permanently red.

::: deeper
**Semantic tests.** The most valuable tests encode business meaning, not just structure: revenue in the mart equals revenue in the general ledger for closed months; every shipped order has a payment; active subscriptions times price equals reported MRR. They catch logic errors that structural tests cannot.

**Model contracts and SLAs.** A contract (previous group) is a promise to consumers; tests are how you check you keep it. Attach freshness and quality service levels to important models, measure how often they are met, and report it, so consumers know which tables they can rely on.

**Testing SCD and history.** For type 2 dimensions, test that each durable key has exactly one current row, that intervals tile time (each `valid_to` equals the next version's `valid_from`), and that facts point to the version valid at their event time. These are the bugs that silently misattribute history for months.

**Statistical anomaly detection.** Simple methods work surprisingly well: compare today's value to the same weekday over the last several weeks, and alert beyond a few standard deviations or outside the historical minimum and maximum. Seasonality (holidays, month-end) causes false alarms; give detectors an exclusion calendar.

> **Interview:** "Which tests do you put on a new fact table?" Grain uniqueness and not-null on keys, relationships to every dimension (with an unknown member so they never fail for missing data), accepted values on status columns, a no-fan-out check, a reconciliation against the source total, and freshness and volume monitoring; plus unit tests for any complex logic.
:::

::: check
?? How does a dbt-style test signal failure?
The test is a query that returns the rows violating the rule; it passes when it returns zero rows.

?? A fact table's row count grows after joining it to a dimension. What does that indicate, and what test catches it?
The dimension key is not unique, so facts fan out; a uniqueness test on the dimension key and a row-count (no fan-out) check catch it.

?? What does data observability catch that tests do not?
Unexpected problems nobody wrote a rule for: late data, unusual volumes, schema changes and distribution shifts, detected by tracking metrics over time.
:::

::: resources
- Docs: [dbt data tests](https://docs.getdbt.com/docs/build/data-tests) · generic and singular tests, severity and configuration
- Docs: [dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests) · testing model logic with fixed inputs and expected outputs
- Docs: [Knowledge Catalog auto data quality](https://cloud.google.com/dataplex/docs/auto-data-quality-overview) · rule-based quality scans on BigQuery tables
- Docs: [Great Expectations](https://greatexpectations.io/) · an open-source framework for expectations, validation and data docs
- Docs: [Elementary](https://docs.elementary-data.com/) · open-source data observability for dbt projects: anomaly tests, alerts and reports
- Article: [What is data observability?](https://www.montecarlodata.com/blog-data-observability/) · Monte Carlo's description of the five pillars of observability
:::

### Documentation-First Modeling with dbt

::: plain
Documentation-first modeling means writing down what a table is for, what each column means and which rules it obeys before writing the SQL that builds it. The description, tests and contract then become the specification the SQL must satisfy. It is like writing the label and the ingredient list for a jar before filling it: if you cannot write the label, you do not yet know what you are making.
:::

**dbt in one paragraph.** dbt (data build tool) turns a folder of `SELECT` statements into tables and views in your warehouse. Each model refers to its inputs with `ref('model')` or `source('system', 'table')`, from which dbt builds a dependency graph (the **DAG**, directed acyclic graph), runs models in order, tests them and generates documentation. Dataform, which runs natively in BigQuery on Google Cloud, follows the same ideas with SQLX files. The Pipelines and transformation section covers dbt in depth; this topic is about using it to document and govern models.

**Write the YAML first.** For a new model, start with its properties file:

```yaml
models:
  - name: fct_orders
    description: >
      One row per confirmed order (grain: order_id). Includes cancelled orders;
      filter on status for revenue. Owner: commerce data team.
    config:
      materialized: table
      contract:
        enforced: true
    columns:
      - name: order_id
        data_type: string
        description: Order number from the shop database.
        constraints:
          - type: not_null
        data_tests: [unique]
      - name: customer_key
        data_type: int64
        description: Surrogate key of the customer version valid at order time; -1 if unknown.
        data_tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: status
        data_type: string
        description: '{{ doc("order_status") }}'
      - name: amount
        data_type: numeric
        description: Order total in EUR after discounts, before tax and shipping.
      - name: ordered_at
        data_type: timestamp
        description: When payment was authorised, in UTC.
```

Writing this first forces the decisions that matter: the grain, the meaning of every column, what is excluded, and who owns it. Reviewers can review the specification before anyone spends time on SQL.

**Model contracts.** With `contract: {enforced: true}`, dbt checks at build time that the model's SQL returns exactly the declared columns with the declared types, and fails otherwise. Constraints such as `not_null` and `primary_key` are passed to the warehouse when it supports them (BigQuery enforces `NOT NULL`; its primary keys are not enforced). A contract turns documentation into a guarantee that the shape cannot drift silently.

**Docs blocks.** Long or shared descriptions live in Markdown files and are reused:

```text
{% docs order_status %}
The order's lifecycle state:
* pending: created, payment not yet authorised
* paid: payment authorised; counts towards revenue
* shipped: at least one parcel handed to a carrier
* cancelled: cancelled before shipping; excluded from revenue
{% enddocs %}
```

Every model with a `status` column can now reference `doc("order_status")`, so the definition is written once.

**Persisting docs to the warehouse.** `persist_docs` writes model and column descriptions into the warehouse itself (BigQuery table and column descriptions), so they appear in the BigQuery console, in Knowledge Catalog and in BI tools, not only in dbt's site:

```yaml
# dbt_project.yml
models:
  shop:
    +persist_docs:
      relation: true
      columns: true
```

**Exposures.** An **exposure** declares something outside dbt that depends on models: a dashboard, a notebook, an ML model, an application. It completes the lineage graph, so you know which dashboards break when a model changes:

```yaml
exposures:
  - name: revenue_dashboard
    type: dashboard
    maturity: high
    url: https://looker.example.com/dashboards/42
    description: Daily revenue for the leadership meeting.
    depends_on:
      - ref('mart_revenue_daily')
    owner:
      name: Finance analytics
      email: finance-analytics@example.com
```

**The lineage graph and selection.** Because every model declares its inputs with `ref` and `source`, dbt knows the whole graph and can select parts of it:

| Selector | Selects |
|---|---|
| `--select fct_orders` | just that model |
| `--select fct_orders+` | it and everything downstream |
| `--select +fct_orders` | it and everything upstream |
| `--select 1+fct_orders` | it and its direct parents only |
| `--select @fct_orders` | it, its descendants, and all ancestors of those descendants |
| `--select tag:finance` | every model tagged finance |
| `--select state:modified+` | models changed versus a saved production state, and their descendants |
| `--select source:shop+` | everything built from the shop source |

`dbt build --select state:modified+ --defer --state prod-artifacts/` is the standard CI command: it builds and tests only what a pull request changed, reading unchanged parents from production.

**The documentation site.** `dbt docs generate` produces a static site (and the `manifest.json` and `catalog.json` artifacts behind it) with every model, column, test, description and the lineage graph. Catalogs such as Knowledge Catalog and DataHub can ingest the same artifacts.

**A documented workflow.** Put it together as a checklist every pull request follows:

1. YAML first: description with grain and owner, columns with descriptions and types, tests.
2. SQL second, until `dbt build` passes the tests and contract.
3. Exposures updated if a dashboard or other consumer is added.
4. CI builds `state:modified+`, runs tests, and fails on undocumented models (a check such as dbt-project-evaluator or a small script over `manifest.json`).
5. Reviewers read the YAML diff before the SQL diff.

> **Pitfall:** Descriptions that restate the column name ("customer_id: the customer ID"). They add nothing. A useful description says what is unusual: where the value comes from, what it excludes, its unit and time zone, and what `NULL` or `-1` means.

::: deeper
**Dataform equivalents.** In Dataform, each SQLX file has a `config` block holding the same information: type, description, column descriptions, BigQuery partitioning and clustering, and assertions (Dataform's tests):

```text
config {
  type: "table",
  schema: "mart",
  description: "One row per confirmed order (grain: order_id).",
  columns: { order_id: "Order number from the shop database", amount: "Order total in EUR" },
  bigquery: { partitionBy: "DATE(ordered_at)", clusterBy: ["customer_id"] },
  assertions: { uniqueKey: ["order_id"], nonNull: ["order_id", "customer_id"] }
}
```

**Groups and access.** dbt can mark models as `public`, `protected` or `private` within named **groups** with owners, so a team's internal intermediate models cannot be referenced by other teams. Combined with contracts and versions, this gives a large dbt project explicit interfaces between teams.

**Documentation coverage as a metric.** Track the share of models with descriptions, owners and primary-key tests, per team. Making the number visible tends to improve it faster than any policy.

> **Interview:** "How do you keep a 500-model dbt project understandable?" Layered structure (staging, intermediate, marts), naming conventions, a description with grain and owner on every model, contracts on public models, groups with access levels, exposures for every dashboard, CI that blocks undocumented models, and persisted docs so the catalog shows the same descriptions.
:::

::: check
?? What does `contract: {enforced: true}` check, and when?
At build time, that the model's SQL returns exactly the declared columns with the declared data types (and applies supported constraints); the build fails if the shape differs.

?? What is an exposure for?
It declares a consumer outside dbt (a dashboard, notebook or application) and the models it depends on, completing the lineage graph so you can see what a change will affect.

?? What does `dbt build --select state:modified+` build?
The models changed relative to a saved production state, and everything downstream of them, with their tests.
:::

::: resources
- Docs: [dbt documentation](https://docs.getdbt.com/docs/build/documentation) · descriptions, docs blocks and the generated documentation site
- Docs: [dbt model contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts) · enforcing column names, types and constraints at build time
- Docs: [dbt node selection syntax](https://docs.getdbt.com/reference/node-selection/syntax) · graph operators, methods and state-based selection
- Docs: [dbt exposures](https://docs.getdbt.com/docs/build/exposures) · declaring dashboards and other downstream consumers
- Docs: [dbt persist_docs](https://docs.getdbt.com/reference/resource-configs/persist_docs) · writing descriptions into BigQuery and other warehouses
- Docs: [Dataform overview](https://cloud.google.com/dataform/docs/overview) · Google Cloud's SQL workflow tool for BigQuery, with documentation and assertions
:::

### Warehouse Modeling Playbooks (BigQuery, Snowflake)

::: plain
The same logical model performs and costs very differently depending on the warehouse it runs on, the way the same house plan needs different foundations on rock and on sand. Each warehouse has its own rules for how tables are physically organised and how you pay for queries. A playbook is the short list of design rules that work well on one platform.
:::

**The shared starting point.** In both BigQuery and Snowflake, storage is columnar and cheap relative to compute, joins are good but not free, and the main physical levers are how data is grouped and ordered so queries read less. Design rules that hold for both: model a dimensional core with a declared grain, denormalise for the heaviest read paths, and organise large tables by the columns most queries filter on.

**BigQuery playbook.** BigQuery's architecture and pricing are covered in BigQuery in depth; the modeling rules:

1. **Partition large tables by date or timestamp** (or an integer range): `PARTITION BY order_date` or `PARTITION BY DATE(event_ts)`. Queries filtering on the partition column read only matching partitions. A table can have at most 10,000 partitions, so daily partitions cover about 27 years; hourly partitions need expiry.
2. **Cluster by up to four columns** that queries filter or join on, most selective and most used first: `CLUSTER BY customer_id, product_id`. Clustering sorts data within partitions, and BigQuery re-clusters automatically in the background at no charge.
3. **Require partition filters** on big facts (`require_partition_filter = TRUE`), so an accidental full scan fails instead of costing money, and set **partition expiration** for data with a retention limit.
4. **Prefer partitioned tables over date-sharded tables** (`events_20261123`, `events_20261124`...). Sharding multiplies metadata and permissions and slows queries; Google recommends partitioning instead.
5. **Use nested and repeated fields** for children always read with their parent (order lines inside orders), and keep separate tables for entities with their own life cycle.
6. **Declare primary and foreign keys `NOT ENFORCED`** on stars, so the optimizer can use them for join elimination, and test them yourself.
7. **Keep dimensions in a star unless a wide table is clearly better** for a specific consumer; dimension tables are usually small enough that joins are cheap.
8. **Design for the pricing model.** On-demand pricing charges per byte read, so column pruning, partitioning and clustering cut cost directly. Capacity pricing (BigQuery editions: Standard, Enterprise, Enterprise Plus) charges for slots, so the same rules cut slot time. Storage can be billed by logical or physical (compressed) bytes, per dataset.

```sql
CREATE TABLE mart.fct_events (
  event_date  DATE NOT NULL,
  event_ts    TIMESTAMP NOT NULL,
  user_id     STRING NOT NULL,
  event_type  STRING NOT NULL,
  country     STRING,
  properties  JSON
)
PARTITION BY event_date
CLUSTER BY event_type, user_id
OPTIONS (
  require_partition_filter = TRUE,
  partition_expiration_days = 400,
  description = 'One row per tracked event (grain: event_id). Owner: product analytics.'
);
```

You can check how a model is used and what it costs with `INFORMATION_SCHEMA.JOBS` (bytes billed per query, per table referenced), which is the evidence for choosing partition and cluster columns.

**Snowflake playbook.** Snowflake stores every table in **micro-partitions** of about 50 to 500 MB of uncompressed data each, created automatically in load order, with min and max statistics per column used for pruning. The rules:

1. **Load in a sensible order.** Because micro-partitions follow load order, tables loaded by date are naturally well clustered by date. Most tables need nothing else.
2. **Add a clustering key only for very large tables** whose queries filter on columns that do not follow load order, and whose pruning has degraded (check with `SYSTEM$CLUSTERING_INFORMATION`). Automatic clustering then reorganises the table in the background, and it consumes credits.
3. **Use the search optimization service** for selective point lookups (a single customer ID or a substring search) on large tables, rather than a clustering key.
4. **Use `VARIANT`** for semi-structured data, and extract frequently queried paths into typed columns.
5. **Constraints are informational**: Snowflake enforces `NOT NULL` but not primary, unique or foreign keys on standard tables; test them.
6. **Use transient tables** for staging data you can rebuild (no Fail-safe period, lower storage cost), and tune Time Travel retention per table.
7. **Design for credits**: compute is billed per second while a virtual warehouse runs, so models that let small warehouses finish quickly (pruning, pre-aggregation) save money directly.

**Side by side.**

| Concern | BigQuery | Snowflake |
|---|---|---|
| Data grouping | explicit partitions (date, timestamp, integer range) | automatic micro-partitions in load order |
| Ordering | `CLUSTER BY` up to 4 columns, automatic re-clustering, free | optional clustering key, automatic clustering, billed |
| Point lookups | clustering, search indexes | search optimization service |
| Nested data | `STRUCT` and `ARRAY` columns, typed | `VARIANT`, `OBJECT`, `ARRAY` |
| Key constraints | PK and FK `NOT ENFORCED`, used by optimizer | informational except `NOT NULL` |
| Main cost driver | bytes scanned (on-demand) or slots (editions) | warehouse running time (credits) |

**Denormalisation boundaries.** On both platforms the sensible boundary is the same: keep conformed dimensions and atomic facts as the governed core, denormalise into wide tables or nested structures for specific heavy consumers, and pre-aggregate for dashboards whose queries repeat. Do not denormalise attributes that change often and are needed "as is" (current customer segment), because every change rewrites many rows.

> **Pitfall:** Partitioning a BigQuery table by a high-cardinality or rarely filtered column, or creating hourly partitions on a small table. Each partition should hold a meaningful amount of data; for small tables, clustering alone (or nothing) is better than partitioning.

::: deeper
**Clustering versus partitioning in BigQuery.** Partitioning gives a guaranteed, upfront cost estimate (the dry run shows bytes for the matching partitions) and cheap partition-level operations (expire, overwrite, delete a day). Clustering gives finer pruning on several columns but only an upper-bound estimate before running. Combine them: partition by date, cluster by the next most common filters.

**Incremental models and partitions.** In dbt on BigQuery, the `insert_overwrite` incremental strategy replaces whole partitions, which is cheaper and idempotent for date-partitioned facts; `merge` is right for tables updated by key. Choose per model.

**Cost-aware modeling habits.** Keep raw JSON in one column and materialise only the fields people query; avoid `SELECT *` views on wide tables; give each mart a single purpose so it is small; and drop unused tables (query the jobs history for tables nobody has read in 90 days).

**Other warehouses.** ClickHouse orders data by the table's `ORDER BY` key and relies on it for pruning; Databricks uses Delta liquid clustering; DuckDB and MotherDuck rely on zone maps over row groups. The idea is the same everywhere: organise data so the most common filters skip the most data. The Data warehousing section covers these engines.

> **Interview:** "A BigQuery dashboard query scans 5 TB a day. How do you fix it with modeling?" Check the filters in `INFORMATION_SCHEMA.JOBS`; partition the fact by the date the dashboard filters on and require a partition filter; cluster on the next filters; select only needed columns; and build a daily aggregate table or materialized view at the dashboard's grain.
:::

::: check
?? Why does Google recommend partitioned tables over date-sharded tables in BigQuery?
Sharded tables (one per day) multiply metadata, schemas and permissions, and queries across them are slower; a single partitioned table prunes just as well with less overhead.

?? When does a Snowflake table need a clustering key?
Only when it is very large, queries filter on columns that do not follow load order, and pruning has degraded; most tables are clustered well enough by load order.

?? What is the main cost lever of good modeling in BigQuery on-demand pricing?
Reducing bytes read: column pruning, partition pruning and clustering directly reduce the bytes billed per query.
:::

::: resources
- Docs: [BigQuery: introduction to partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables) · partition types, limits and partitioning versus sharding
- Docs: [BigQuery: introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables) · how clustering works, column order and automatic re-clustering
- Docs: [BigQuery: optimize storage](https://cloud.google.com/bigquery/docs/best-practices-storage) · storage best practices, expiration and billing models
- Docs: [Snowflake: micro-partitions and data clustering](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions) · how Snowflake organises and prunes data
- Docs: [Snowflake: clustering keys and clustered tables](https://docs.snowflake.com/en/user-guide/tables-clustering-keys) · when to define a clustering key and what it costs
:::

### Metadata, Catalogs & Lineage Modeling

::: plain
Metadata is data about data: what a table contains, who owns it, where its rows came from, when it was last updated, and which dashboards use it. A catalog collects that metadata so people can find and trust data, and lineage records how data flows from source to report. It is like a library catalogue combined with a family tree: you can find a book, see who looks after it, and trace which earlier works it was based on.
:::

**Kinds of metadata.**

| Kind | Examples | Usual source |
|---|---|---|
| Technical | schemas, types, partitions, sizes, row counts | the warehouse itself (`INFORMATION_SCHEMA`) |
| Business | descriptions, glossary terms, owners, data products | people, dbt YAML, contracts |
| Operational | job runs, freshness, quality results, query usage | orchestrators, quality tools, job logs |
| Lineage | which jobs read which datasets and wrote which | pipelines emitting lineage events, SQL parsing |
| Governance | classifications (PII), policies, access, retention | governance tools, policy tags |

**Metadata is a model too.** A catalog has its own data model, and knowing it helps you integrate with one. The common core:

- **Entities** (also called assets or entries): datasets, tables, columns, dashboards, jobs, ML models, glossary terms, people and teams.
- **Aspects** (facets): typed groups of metadata attached to an entity, such as a schema, an ownership record, a quality score or a classification. New aspects can be added without changing the entity.
- **Relationships**: owned by, derived from, contains (table contains column), defined by (column linked to glossary term).
- **Identifiers**: a stable, unique name for every entity (DataHub uses URNs; OpenLineage uses namespace plus name, such as `bigquery` and `project.dataset.table`).

Knowledge Catalog on Google Cloud follows this shape: **entries** for assets (BigQuery tables, Cloud Storage files, Spanner tables and custom systems), grouped in entry groups, with typed **aspects** defined by aspect types (your own, such as "data product" or "contract", alongside built-in ones), plus a business glossary whose terms link to columns.

**Lineage as a graph.** Lineage is a directed graph: nodes are datasets (and jobs), edges say "this job read A and wrote B". **OpenLineage** is the open standard for collecting it: each job run emits events with the job, the run, its input datasets and its output datasets, and optional **facets** (schema, column lineage, data quality, SQL text). An abridged event emitted when a dbt model finishes:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-11-23T06:10:00Z",
  "run": {"runId": "0193a1c2-7b4e-7c1d-9f00-3c2b1a0d9e8f"},
  "job": {"namespace": "dbt-prod", "name": "shop.fct_orders"},
  "inputs": [
    {"namespace": "bigquery", "name": "acme-prod.staging.stg_orders"},
    {"namespace": "bigquery", "name": "acme-prod.mart.dim_customer"}
  ],
  "outputs": [
    {"namespace": "bigquery", "name": "acme-prod.mart.fct_orders"}
  ],
  "producer": "https://github.com/OpenLineage/OpenLineage"
}
```

Integrations emit these events from Airflow (including Managed Service for Apache Airflow (formerly Cloud Composer)), Spark, dbt and Flink. Knowledge Catalog's data lineage records lineage for BigQuery jobs and other Google Cloud services automatically and accepts OpenLineage events from other tools. Marquez is the open-source reference backend.

**Using lineage for impact analysis.** The most common question is "what breaks if I change this?". It is a graph traversal. This runs as is:

```python
from collections import deque

# edges: upstream dataset -> downstream dataset (as an OpenLineage backend would store them)
edges = [
    ("crm.customers", "stg_customers"), ("shop.orders", "stg_orders"),
    ("stg_customers", "dim_customer"), ("stg_orders", "fct_orders"),
    ("dim_customer", "fct_orders"), ("fct_orders", "mart_revenue_daily"),
    ("mart_revenue_daily", "dashboard:Revenue"), ("dim_customer", "ml:churn_features"),
]
down = {}
for u, d in edges:
    down.setdefault(u, []).append(d)

def impact(node):
    """Everything downstream of node, with the number of hops."""
    seen, queue = {}, deque([(node, 0)])
    while queue:
        n, hops = queue.popleft()
        for d in down.get(n, []):
            if d not in seen:
                seen[d] = hops + 1
                queue.append((d, hops + 1))
    return seen

for name, hops in sorted(impact("crm.customers").items(), key=lambda x: (x[1], x[0])):
    print(hops, name)
# 1 stg_customers
# 2 dim_customer
# 3 fct_orders
# 3 ml:churn_features
# 4 mart_revenue_daily
# 5 dashboard:Revenue
```

A change to the CRM's customer table reaches a revenue dashboard five hops away and a churn model, which is exactly the list of owners to notify before an expand-and-contract migration.

**Table-level versus column-level lineage.** Table-level lineage says `fct_orders` depends on `stg_orders`. **Column-level** lineage says `fct_orders.amount` is computed from `stg_orders.gross_amount` and `stg_orders.discount`. Column-level lineage answers sharper questions (does dropping `stg_orders.coupon_code` affect anything?) and supports PII tracking (which columns derive from `email`?). It comes from parsing SQL or from engines that report it.

**Ownership and stewardship.** Record two roles per important asset: the **owner** (accountable; usually the producing team, who decides changes and fixes incidents) and the **steward** (responsible for definitions and quality, often in the business). An asset without an owner is a liability: nobody will fix it, and nobody can approve its deletion.

**Catalog-driven design reviews.** Use the catalog as an input to every modeling change:

1. Before creating a model, search the catalog: does a certified `dim_customer` already exist?
2. Link new columns to glossary terms, and classify sensitive ones (PII tags, BigQuery policy tags).
3. Check downstream lineage before changing or deleting anything.
4. After deployment, verify the catalog shows the descriptions, owner and quality results.

> **Pitfall:** Treating the catalog as a one-off documentation project. Metadata that is typed in by hand goes stale within months. Generate it from the systems (warehouse schemas, dbt artifacts, lineage events, quality results) and keep the human-written parts (descriptions, owners) in code, next to the models.

::: deeper
**Push versus pull metadata.** Some metadata is **pulled** by crawling systems on a schedule (schemas, sizes); some is **pushed** by the systems as events happen (lineage at job completion, schema changes). Push gives fresher, more accurate lineage; pull is easier to start. Mature catalogs use both.

**Lineage gaps.** Lineage breaks at the edges: a Python script that reads a file and writes a table without emitting events, a spreadsheet export, a manual load. Make lineage a requirement of the platform (every job runs through instrumented orchestration) rather than an optional add-on, and mark known gaps in the catalog.

**Metadata for AI.** Agents and AI assistants that write queries need descriptions, glossary terms, certified tables and lineage to pick the right data. A well-modeled catalog is becoming a prerequisite for useful natural-language analytics.

> **Interview:** "How would you find every report affected by changing a column's definition?" Use column-level lineage from the catalog (or parse the dbt manifest and SQL) to traverse downstream from the column, including exposures and BI usage metadata, then notify the owners listed for each affected asset.
:::

::: check
?? Name three kinds of metadata and a source for each.
Technical (schemas and sizes, from the warehouse's `INFORMATION_SCHEMA`), business (descriptions and owners, from people via dbt YAML or the catalog), operational (job runs and freshness, from orchestrators), and lineage (from OpenLineage events).

?? What does an OpenLineage run event contain?
The event type (such as START or COMPLETE), the event time, the run ID, the job's namespace and name, the input and output datasets, and optional facets such as schemas and column lineage.

?? Why is column-level lineage more useful than table-level lineage for PII tracking?
It shows exactly which downstream columns are derived from a sensitive column, while table-level lineage only shows that whole tables are related.
:::

::: resources
- Docs: [OpenLineage](https://openlineage.io/docs/) · the open lineage standard: object model, facets and integrations
- Docs: [OpenLineage object model](https://openlineage.io/docs/spec/object-model) · jobs, runs, datasets and facets in detail
- Docs: [Knowledge Catalog overview](https://cloud.google.com/dataplex/docs/catalog-overview) · entries, aspects, entry groups and search on Google Cloud
- Docs: [Knowledge Catalog data lineage](https://cloud.google.com/dataplex/docs/about-data-lineage) · automatic lineage for Google Cloud services and OpenLineage ingestion
- Docs: [DataHub metadata model](https://docs.datahub.com/docs/metadata-modeling/metadata-model) · entities, aspects, relationships and URNs in an open-source catalog
:::

### Modeling Tools & Diagramming Workflows

::: plain
A diagram is how most people first understand a model, and a decision record is how they later understand why it looks the way it does. Keeping both as text files in the same repository as the code means they change in the same pull request as the tables they describe, so they never fall out of date. It is like keeping the instructions inside the box rather than in a drawer somewhere else.
:::

**Entity-relationship diagrams.** An ERD (entity-relationship diagram) draws entities as boxes and relationships as lines, usually in crow's foot notation (described in the first topic). It is the standard picture for conceptual and logical models, and for physical OLTP schemas.

**Diagrams as code.** Instead of drawing boxes by hand, describe the model in text and let a tool draw it. Two widely used text formats:

DBML (Database Markup Language), used by dbdiagram.io and other tools:

```text
Table customers {
  customer_id bigint [pk]
  email text [unique, not null]
  city text
}

Table orders {
  order_id bigint [pk]
  customer_id bigint [not null, ref: > customers.customer_id]
  status text [not null]
  ordered_at timestamptz [not null]
}

Table order_lines {
  order_id bigint [ref: > orders.order_id]
  product_id bigint [ref: > products.product_id]
  qty int
  indexes {
    (order_id, product_id) [pk]
  }
}

Table products {
  product_id bigint [pk]
  sku text [unique]
}
```

Mermaid, which renders directly in GitHub, GitLab and many documentation tools:

```text
erDiagram
    CUSTOMER ||--o{ ORDER : places
    ORDER ||--|{ ORDER_LINE : contains
    PRODUCT ||--o{ ORDER_LINE : "appears in"
    CUSTOMER {
        bigint customer_id PK
        text email
        text city
    }
```

The `||--o{` notation reads "exactly one to zero or more". Text diagrams can be reviewed line by line in a pull request, merged without conflicts in binary files, and generated from code.

**Generating diagrams from the source of truth.** Hand-maintained diagrams drift. Generate them instead:

- from the database schema (most ER tools can reverse-engineer a PostgreSQL or BigQuery schema, and DBML can be generated from SQL DDL);
- from dbt artifacts: tools such as dbterd read `manifest.json` and the relationships tests to produce DBML or Mermaid ERDs of a dbt project;
- from the semantic layer or contracts, which already declare entities and joins.

Keep a hand-drawn conceptual diagram (it carries business meaning a schema cannot), and generate the logical and physical ones.

**Migration-linked diagrams.** Make the diagram part of the change: a pull request that adds a migration or a model also updates (or regenerates) the diagram, and CI checks that the generated diagram matches the committed one. Reviewers then see the structural change visually next to the SQL.

**Modeling tools.** For large enterprise models, dedicated tools add features text formats lack: subject-area views, model comparison, naming-standard checks, forward and reverse engineering, and governance workflows.

| Tool type | Examples | Good for |
|---|---|---|
| Text-based, open | DBML, Mermaid, PlantUML | small and medium models, docs as code |
| Online ER tools | dbdiagram.io, DrawSQL, SqlDBM | collaborative design, quick sharing |
| Enterprise modelers | erwin Data Modeler, ER/Studio | large enterprise models, governance processes |
| NoSQL and JSON modelers | Hackolade | document, key-value and event schemas |
| Generic diagramming | Lucidchart, draw.io | conceptual and architecture diagrams |

**Architecture decision records.** Diagrams show what the model is; an **architecture decision record** (ADR) records why. Michael Nygard's widely used format is a short Markdown file per decision:

```text
# ADR 012: Customer city is a type 2 attribute in dim_customer

Status: Accepted (2026-11-23)

Context:
Finance reports revenue by the city where the customer lived when ordering.
Marketing wants current city for campaign targeting. Customers move about
once every four years on average; dim_customer has 2 million rows.

Decision:
Track city as SCD type 2 in dim_customer, and add current_city as a type 1
column on every version (type 6), so both views need no self-join.

Consequences:
dim_customer grows by roughly 500,000 rows a year. Facts resolve customer_key
at load time with a point-in-time join. Backdated moves require re-pointing
facts; we accept this because the CRM records moves on the day they happen.
```

ADRs are numbered, never edited after acceptance (a later ADR supersedes an earlier one), and kept in the repository (`docs/adr/`). Six months later, when someone asks why the dimension is so large, the answer is one file away.

**Collaborative model reviews.** A lightweight review process that works:

1. The author opens a pull request with the YAML or DDL change, the regenerated diagram and, for significant changes, an ADR.
2. Reviewers include a data modeler (structure, grain, keys), a consumer (does it answer the question?) and the owner of any affected upstream source.
3. A checklist covers naming, grain, keys, tests, history, documentation, lineage impact and cost.
4. Large redesigns get a short synchronous design review before any code is written, with the conceptual diagram on screen.

> **Pitfall:** A beautiful diagram in a slide deck that nobody updates after the first sprint. Diagrams that are not generated from or checked against the code are wrong within months; it is better to have a plain generated diagram that is always right.

::: deeper
**Diagram scope.** A diagram with 200 tables helps nobody. Draw per subject area or per business process (the orders star; the customer domain), and link them through conformed dimensions. Most tools support views or filtered subsets of one underlying model.

**Docs-as-code pipeline.** A typical setup: model YAML and ADRs in the repository; CI regenerates ERDs and the dbt documentation site; a static site (or the catalog) publishes them on each merge. Everything a newcomer needs to understand the model is then built from the same commit as the code.

**Physical versus logical in diagrams.** Physical details (partitions, clustering, indexes) clutter a diagram meant for understanding. Show keys and relationships in the logical ERD, and document physical choices in the model YAML and ADRs.

> **Interview:** "How do you document data model decisions?" Text-based diagrams generated from the schema or dbt project and kept in the repository, ADRs for significant decisions (context, decision, consequences), descriptions with grain and owner in the model YAML, and reviews through pull requests with a checklist.
:::

::: check
?? Why are text-based diagram formats such as DBML and Mermaid easier to keep current than drawn diagrams?
They live in the repository, change in the same pull request as the code, can be reviewed line by line, and can be generated or checked automatically by CI.

?? What are the main sections of a Nygard-style ADR?
Title, status, context, decision and consequences.

?? Why should ADRs not be edited after they are accepted?
They record what was decided and why at that time; if the decision changes, a new ADR supersedes the old one, preserving the history of reasoning.
:::

::: resources
- Docs: [DBML documentation](https://dbml.dbdiagram.io/docs/) · the Database Markup Language syntax for tables, references and indexes
- Docs: [Mermaid entity relationship diagrams](https://mermaid.js.org/syntax/entityRelationshipDiagram.html) · ERD syntax with crow's foot cardinality, renderable in GitHub
- Article: [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions) · Michael Nygard's original ADR post
- Docs: [Architectural Decision Records](https://adr.github.io/) · templates, tooling and examples for ADRs
- Repo: [dbterd](https://github.com/datnguye/dbterd) · generate DBML, Mermaid and other ERDs from dbt artifacts
- Article: [What is an entity relationship diagram?](https://www.lucidchart.com/pages/er-diagrams) · Lucidchart's guide to ERD symbols and notation
:::

### End-to-End Modeling Project (Portfolio)

::: plain
The best way to learn modeling, and to show an employer you can do it, is to model one business completely: from interviews (or a written brief) to a conceptual sketch, to tables with keys and tests, to metrics on a dashboard, with every decision written down. It is like building a small piece of furniture from a drawing you made yourself: every skill is used once, and the result is something you can show.
:::

**Pick a domain with real data.** Use a public dataset rich enough to have several business processes and some history. Good choices on Google Cloud, all free to query within the BigQuery sandbox or free tier:

| Dataset | Processes to model | Interesting modeling problems |
|---|---|---|
| `bigquery-public-data.thelook_ecommerce` | orders, order items, inventory, web events | order-line grain, returns, sessions from events, customer history |
| `bigquery-public-data.new_york_citibike` | trips, stations | station dimension with changes, trip facts, daily snapshots |
| Jaffle Shop (dbt Labs' sample) | orders, payments, customers | a small, well-known starting point for dbt structure |

Alternatively, generate synthetic data with a Python script for a domain you know (a gym's memberships, a library's loans), which lets you inject changes and late data deliberately.

**Work through the levels, and write each one down.**

1. **Brief.** Write one page: the business, its processes, and ten business questions in plain words ("weekly revenue by product category", "share of customers who order again within 30 days", "average days from order to delivery by distribution centre").
2. **Conceptual model.** Entities, relationships, definitions, systems of record. One diagram.
3. **Bus matrix.** Processes against dimensions, to plan facts and conformed dimensions.
4. **Logical and dimensional design.** For each fact: grain, dimensions, measures, additivity. For each dimension: attributes, natural and surrogate keys, SCD type per attribute.
5. **Physical design.** BigQuery partitioning and clustering per table, with the reasoning in an ADR.
6. **Implementation.** dbt or Dataform project with staging, intermediate and marts layers; snapshots for type 2 history; YAML documentation written first; contracts on marts.
7. **Tests.** Grain, relationships, accepted values, a no-fan-out check, an SCD interval test, and at least one reconciliation.
8. **Semantic layer.** Define five to ten metrics with a semantic layer or LookML, and build a small dashboard in Looker or Data Studio (formerly Looker Studio).
9. **Documentation and lineage.** Generated docs site, ERD generated from the project, exposures for the dashboard.
10. **Retrospective.** A short write-up of trade-offs: what you would change at ten times the data, what you chose not to model and why.

**A repository layout.**

```text
shop-analytics/
  README.md                  # the brief, how to run, a diagram, links
  docs/
    conceptual-model.md      # Mermaid diagram plus definitions
    bus-matrix.md
    adr/
      001-grain-of-order-facts.md
      002-scd2-for-customer-city.md
      003-partitioning-and-clustering.md
  models/
    staging/                 # stg_*: one per source table, renamed and typed
    intermediate/            # int_*: business logic steps
    marts/
      core/                  # dim_*, fct_*
      finance/               # metrics-ready marts
    semantic/                # semantic models and metrics
  snapshots/
  seeds/                     # reference data: country regions, status labels
  tests/                     # singular tests: reconciliations, SCD checks
  .github/workflows/ci.yml   # dbt build on pull requests
```

**A first staging model** against the public e-commerce data shows the habits to demonstrate: explicit columns, renamed to the standard, typed, with no business logic yet:

```sql
-- models/staging/stg_thelook__order_items.sql
SELECT
  CAST(id AS STRING)         AS order_item_id,
  CAST(order_id AS STRING)   AS order_id,
  CAST(user_id AS STRING)    AS customer_id,
  CAST(product_id AS STRING) AS product_id,
  status                     AS item_status,
  sale_price                 AS sale_price_usd,
  created_at,
  shipped_at,
  delivered_at,
  returned_at
FROM {{ source('thelook', 'order_items') }}
```

**What reviewers look for.** People hiring data engineers read portfolio projects for evidence of judgement, not volume:

- a stated grain for every fact, and tests proving it;
- deliberate SCD choices with reasons;
- metrics defined once and reconciled;
- clear documentation and ADRs that show trade-offs;
- CI that runs the project from scratch;
- a README that lets a stranger run it in ten minutes.

> **Tip:** Make one thing interesting rather than everything large. A project with a carefully modeled returns process (partial returns, refunds on a different date than the order, restocking) shows more modeling skill than fifty tables of straightforward copies.

::: deeper
**Add a streaming or lakehouse slice.** To extend the project, publish synthetic events to Pub/Sub, land them in BigQuery with a BigQuery subscription or Dataflow, and model sessions from them; or write the raw layer as Iceberg tables in Cloud Storage and read them from BigQuery. Explain how the event schema evolves (a contract and compatibility mode).

**Introduce change on purpose.** Re-run the pipeline after changing source data: move a customer to a new city, change a product's category, deliver a late order, rename a source column. Show that history is right, the backfill is idempotent and the schema change is handled with expand and contract. These are the scenarios interviews ask about.

**Keep costs at zero.** Use the BigQuery sandbox or free tier, query only the columns and partitions you need, set table expiration on scratch datasets, and run dbt CI against a small sample (a `limit` in development or a date filter on staging models).

> **Interview:** "Walk me through a modeling project you are proud of." Use the levels as the structure: the business questions, the conceptual model, one fact table's grain and dimensions, one hard decision (an SCD type or a denormalisation) with its trade-off, how you tested it, and what you would change.
:::

::: check
?? What should a portfolio project's README contain?
The business brief, how to run the project, a diagram of the model, and links to the documentation, ADRs and dashboard, so a stranger can understand and run it quickly.

?? Why deliberately introduce changes (a customer moving, a renamed column) into a portfolio project?
To demonstrate that history, idempotent reloads and schema evolution actually work, which are the scenarios interviewers probe.

?? What belongs in a staging model, and what does not?
Selecting, renaming, typing and light cleaning of one source table belong there; joins and business logic belong in intermediate and mart models.
:::

::: resources
- Repo: [Jaffle Shop](https://github.com/dbt-labs/jaffle-shop) · dbt Labs' reference project, a good template for structure and tests
- Docs: [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview) · staging, intermediate and marts conventions in detail
- Docs: [BigQuery public datasets](https://cloud.google.com/bigquery/public-data) · free datasets to model, including thelook_ecommerce and Citi Bike
- Docs: [BigQuery sandbox](https://cloud.google.com/bigquery/docs/sandbox) · using BigQuery without a billing account, with its limits
- Course: [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) · a free course whose final project follows the same end-to-end path
- Video: [DataTalks.Club](https://www.youtube.com/@DataTalksClub) · project walkthroughs and talks from practitioners
:::

### Data Modeling Interview Case Studies

::: plain
A data modeling interview gives you a business ("an online shop", "a payments app", "a subscription product") and asks you to design its tables on a whiteboard in about 45 minutes. The interviewer cares less about the final diagram than about how you get there: the questions you ask, how precisely you state the grain, and how you explain trade-offs. It is like an architect's first meeting with a client: listen, sketch, ask, and explain each choice.
:::

**A structure that works.** Interviewers reward a visible, repeatable process. Say the steps out loud:

1. **Clarify the business and the questions** (5 minutes). Who uses the data? Which ten questions matter most? What volumes? How fresh? Write the questions on the board.
2. **Name the business processes** that create the data (orders, payments, shipments).
3. **Declare the grain** of each fact table in one sentence.
4. **List dimensions and facts** for each, with additivity.
5. **Handle history**: which attributes are type 1, which type 2, and why.
6. **Walk through edge cases**: cancellations, partial shipments, returns, late data, currency, time zones, deletes.
7. **Physical design**: partitioning, clustering, nested fields or wide tables for heavy consumers.
8. **Prove it answers the questions**: sketch the SQL for one or two of them.
9. **Trade-offs and extensions**: what changes at 100 times the scale, or with real-time needs.

**Case 1: e-commerce.** "Design the analytics model for an online shop."

- Processes: orders, payments, shipments, returns, web sessions.
- Grains: `fct_order_lines` (one row per order line), `fct_shipments` (one row per parcel), `fct_returns` (one row per returned line), an accumulating snapshot `fct_order_fulfilment` (one row per order, with placed, paid, shipped and delivered timestamps and durations).
- Dimensions: date, customer (type 2 on city and segment), product (type 2 on category, price as a fact on the line, not an attribute), promotion, channel.
- Edge cases: an order shipped in two parcels (shipments are their own fact; never join lines to shipments directly); partial returns (returns at line grain with returned quantity); refunds on a later date (refund amount in the returns fact on the refund date, so revenue by order date and net revenue by refund date are both possible); multi-currency (amounts in transaction currency plus a reporting currency converted at a documented rate).

**Case 2: fintech ledger.** "Design the data model for a payments app's balances and transactions."

- The core is a **double-entry ledger**: every money movement is a journal entry with two or more postings that sum to zero (debit one account, credit another). Postings are immutable; mistakes are corrected with reversing entries, never updates.
- Grains: `fct_postings` (one row per posting: entry ID, account, amount as a signed decimal, currency, effective time and recorded time), and `fct_account_balance_daily` (a periodic snapshot: one row per account per day with the closing balance, semi-additive).
- Dimensions: account (type 2 on status and product), customer, currency, transaction type.
- Edge cases: exact decimals (never floats); time zones and cut-off times for "daily" balances; backdated corrections (bi-temporal: valid time and recorded time on postings, so statements already sent can be reproduced); idempotency keys so a retried payment is not posted twice; reconciliation against the bank's statements.

**Case 3: SaaS subscriptions.** "Model monthly recurring revenue (MRR) and churn."

- Processes: subscription changes (new, upgrade, downgrade, cancel), invoices, product usage.
- Grains: `fct_subscription_changes` (one row per change event), `dim_subscription` versions or a `subscription_versions` table with validity intervals, and `fct_mrr_monthly` (a periodic snapshot: one row per account per month-end with MRR and a movement category: new, expansion, contraction, churn, reactivation).
- Edge cases: annual plans (normalise to monthly), mid-month changes (MRR at month-end versus prorated revenue), trials (not MRR), currency, accounts with several subscriptions.

The month-end snapshot is a point-in-time join of a date spine with subscription versions. This runs in SQLite as is:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
-- one row per subscription version: plan changes close one row and open another
CREATE TABLE subscription_versions (account_id INT, plan TEXT, mrr REAL,
                                    valid_from TEXT, valid_to TEXT);
INSERT INTO subscription_versions VALUES
  (1, 'basic', 20, '2026-01-10', '2026-03-05'),
  (1, 'pro',   50, '2026-03-05', '9999-12-31'),   -- upgrade
  (2, 'basic', 20, '2026-02-01', '2026-03-20'),   -- churned
  (3, 'pro',   50, '2026-03-15', '9999-12-31');   -- new
CREATE TABLE month_ends (d TEXT);
INSERT INTO month_ends VALUES ('2026-01-31'), ('2026-02-28'), ('2026-03-31');
""")
for row in db.execute("""
SELECT m.d AS month_end, COUNT(s.account_id) AS accounts, COALESCE(SUM(s.mrr), 0) AS mrr
FROM month_ends m
LEFT JOIN subscription_versions s ON s.valid_from <= m.d AND m.d < s.valid_to
GROUP BY m.d ORDER BY m.d"""):
    print(row)
# ('2026-01-31', 1, 20.0)
# ('2026-02-28', 2, 40.0)
# ('2026-03-31', 2, 100.0)
```

March's 100.0 is account 1's upgrade to 50 plus new account 3's 50; account 2 churned on 20 March and is gone. Classifying the movements (expansion +30, new +50, churn -20) is a comparison of each account's MRR with the previous month-end.

**Articulating trade-offs.** Have a sentence ready for the standard dilemmas:

| Dilemma | A good answer names |
|---|---|
| star or one big table | the star as governed core, wide tables derived for specific consumers |
| type 1 or type 2 | which reports must reproduce history, and the growth cost |
| atomic or aggregated facts | atomic as the source, aggregates for performance, both reconcilable |
| normalise or denormalise | write patterns, change frequency of attributes, query patterns |
| batch or streaming | the freshness the questions actually need, and the cost |
| surrogate or natural keys | stability, multiple sources, history |

> **Pitfall:** Starting to draw tables before asking a single question. Interviewers often withhold a key requirement ("finance needs revenue by the date the refund was processed") to see whether you ask. Spend the first minutes on questions and write the grain before any table.

::: deeper
**How interviewers score.** Typical criteria: requirement gathering (did you ask about users, questions, volumes and freshness?), correctness of grain and keys, handling of history and edge cases, physical awareness (partitioning, cost), communication (structure, clarity, checking in), and adaptability when a requirement changes midway. A correct but silent design scores worse than a slightly imperfect one explained clearly.

**Common follow-ups.** "Now the business wants it real time": streaming ingestion, an append-only fact with late-data handling, and a serving layer for low latency. "Now there are ten times more events": partitioning, clustering, pre-aggregation and incremental models. "A customer asks to be deleted": where personal data lives in the model, deletion or pseudonymisation in each layer, and how history and aggregates are handled. "Two teams define active user differently": named metrics with owners in the semantic layer.

**Practising.** Take the cases above and others (ride-hailing trips, a music streaming service's plays, a hotel's bookings, a hospital's admissions), set a 45-minute timer, talk out loud, and compare your design with the Kimball Toolkit's industry chapters, which cover retail, inventory, procurement, order management, CRM, HR, finance, education, healthcare, telecommunications, transportation and more.

> **Interview:** "What is the single most important decision in a dimensional model?" The grain of each fact table: it determines which dimensions and facts are valid, prevents mixed-grain double counting, and is the first thing to state and test.
:::

::: check
?? What should you do in the first five minutes of a data modeling interview?
Clarify the business, the users, the key questions, volumes and freshness, and write the questions down, before drawing any tables.

?? In a double-entry ledger model, how are mistakes corrected?
With new reversing (and correcting) entries; postings are immutable and never updated in place.

?? Why is MRR modeled as a periodic snapshot rather than a transaction fact?
MRR is a state measured at points in time (each month-end), and it is semi-additive over time; a snapshot per account per period makes it a simple filter and sum, and movements are derived by comparing consecutive snapshots.
:::

::: resources
- Blog: [A guide to dimensional modeling](https://www.getdbt.com/blog/guide-to-dimensional-modeling) · dbt Labs' step-by-step guide, useful as a whiteboard checklist
- Book: [The Data Warehouse Toolkit, 3rd edition](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/) · Kimball and Ross; its industry case studies are the best practice material for interviews
- Docs: [Kimball dimensional modeling techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) · quick definitions to revise before an interview
- Article: [Accounting for developers, part I](https://www.moderntreasury.com/journal/accounting-for-developers-part-i) · Modern Treasury's clear introduction to double-entry ledgers for engineers
- Blog: [Start Data Engineering](https://www.startdataengineering.com/) · practical articles on data modeling and pipeline design, many interview-oriented
- Video: [Seattle Data Guy](https://www.youtube.com/@SeattleDataGuy) · videos on data modeling, career and interview preparation
:::

## Wrap-up

### Cheat sheet

| Concept or task | Rule of thumb, SQL or command |
|---|---|
| Model levels | conceptual (business words), logical (keys, 3NF), physical (engine types, indexes, partitions) |
| Before any SQL | write the grain and owner: "one row per order line, owned by commerce data" |
| 3NF in one line | every non-key attribute depends on the key, the whole key and nothing but the key |
| Keys | surrogate primary key plus the natural key with a `UNIQUE` constraint |
| Composite B-tree index | equality columns first, then the range or sort column (leftmost prefix rule) |
| Kimball four steps | business process, grain, dimensions, facts |
| Additivity | sum additive measures; semi-additive (balances) use period-end; store ratio parts, divide last |
| Missing dimension value | point at the unknown member (`-1`), never a `NULL` foreign key |
| Comparing two facts | drill across: aggregate each to common dimensions, then join |
| SCD choice | type 1 overwrite, type 2 new row per version, type 6 both views |
| Type 2 intervals | half-open `valid_from <= t AND t < valid_to`, far-future `valid_to` on the current row |
| Fan-out check | row count before and after joining to a dimension must match |
| Data Vault | hubs (business keys), links (relationships), satellites (history); insert only |
| Bi-temporal | valid time (true in the world) and recorded time (known to the database) |
| Event schema change | add fields with defaults; never reuse names or Protobuf field numbers |
| Iceberg layout | `PARTITIONED BY (days(ts))`, then `WRITE ORDERED BY` for other filters |
| BigQuery fact table | `PARTITION BY` date, `CLUSTER BY` up to 4 columns, `require_partition_filter` |
| Breaking change | expand, dual write, backfill, migrate readers, contract after the deprecation window |
| dbt CI | `dbt build --select state:modified+ --defer --state prod-artifacts/` |
| Decision record | ADR: status, context, decision, consequences |

### Practice

::: try
**1. Declare the grain.** A coffee chain's till system records each receipt with several items, a payment method and the barista. Management wants revenue by product, store, hour and barista, and the average number of items per receipt. Declare the grain of the main fact table, list its dimensions and facts, and say which requested number cannot be summed directly.
:::

?? A solution to 1
Grain: one row per receipt line (one product on one receipt). Dimensions: date, time of day (hour), store, product, barista, payment method (or a junk dimension of payment method and flags). The receipt number is a degenerate dimension. Facts: quantity, net amount, discount amount. "Average items per receipt" is a ratio: compute it as `SUM(quantity) / COUNT(DISTINCT receipt_number)`, never as an average of per-row values or a sum of averages.

::: try
**2. Normalise.** Put this table into third normal form and name the keys: `sales(invoice_no, invoice_date, customer_id, customer_name, customer_city, product_code, product_name, qty, unit_price_charged)`, where one invoice has many products.
:::

?? A solution to 2
`customers(customer_id PK, customer_name, customer_city)`; `products(product_code PK, product_name)`; `invoices(invoice_no PK, invoice_date, customer_id FK)`; `invoice_lines(invoice_no FK, product_code FK, qty, unit_price_charged, PK(invoice_no, product_code))`. `invoice_date` and `customer_id` depended on part of the key (2NF); `customer_name` and `customer_city` depended on `customer_id` (3NF). `unit_price_charged` stays on the line because it is a fact of the sale.

::: try
**3. SCD type 2 lookup.** Using the SCD type 2 example's final `dim_customer` rows (customer 42: key 1 Pune until 2026-06-01, key 3 Mumbai from 2026-06-01), write a SQLite query that returns the surrogate key for each of these orders: customer 42 on 2026-05-31, customer 42 on 2026-06-01, customer 7 on 2026-03-10. Run it with Python's `sqlite3`.
:::

?? A solution to 3

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE dim_customer (customer_sk INT, customer_id INT, city TEXT, valid_from TEXT, valid_to TEXT);
INSERT INTO dim_customer VALUES (1, 42, 'Pune', '2026-01-01', '2026-06-01'),
  (2, 7, 'Oslo', '2026-01-01', '9999-12-31'), (3, 42, 'Mumbai', '2026-06-01', '9999-12-31');
CREATE TABLE orders (order_id INT, customer_id INT, order_date TEXT);
INSERT INTO orders VALUES (1, 42, '2026-05-31'), (2, 42, '2026-06-01'), (3, 7, '2026-03-10');
""")
print(db.execute("""
SELECT o.order_id, d.customer_sk, d.city
FROM orders o JOIN dim_customer d
  ON d.customer_id = o.customer_id
 AND d.valid_from <= o.order_date AND o.order_date < d.valid_to
ORDER BY o.order_id""").fetchall())
# [(1, 1, 'Pune'), (2, 3, 'Mumbai'), (3, 2, 'Oslo')]
```

The order on 1 June matches only the Mumbai version because the intervals are half-open.

::: try
**4. Evolve an event safely.** The `OrderPlaced` Avro schema has `order_id`, `amount` and `channel` (no defaults). The producer wants to drop `channel` and add `currency`. The topic uses FULL_TRANSITIVE compatibility. Describe the sequence of schema versions and deployments.
:::

?? A solution to 4
Version 2 adds `currency` with a default (for example `"EUR"`) and gives `channel` a default (for example `"unknown"`); both are fully compatible changes. Deploy it, then update consumers to stop reading `channel`. Version 3 removes `channel`, which is fully compatible now that it had a default in every version a consumer might use. Keep `currency`'s meaning documented, and never reuse the name `channel` for something else.

::: try
**5. Design for BigQuery.** A clickstream table receives 2 billion events a day; analysts filter by event date (usually the last 7 days), event type and sometimes user ID, and a dashboard shows daily active users by country. Propose the table design and one derived table.
:::

?? A solution to 5
`fct_events` partitioned by `event_date` with `require_partition_filter = TRUE` and a partition expiration matching the retention policy, clustered by `event_type, user_id` (add `country` if it is a frequent filter); properties as a `JSON` column with frequently queried fields extracted to typed columns. Derived: `agg_daily_active_users(event_date, country, active_users)` built incrementally per partition with `COUNT(DISTINCT user_id)` (or HyperLogLog sketches if the dashboard needs to combine days into weeks), feeding the dashboard so it never scans raw events.

### What comes next

You can now design data before building it: agree definitions in a conceptual model, normalise and key a logical model, choose physical layouts, build stars with the right grain and history, pick alternatives such as Data Vault or bi-temporal tables when they fit, model metrics, master data, events and lakehouse tables, and keep all of it tested, documented and evolving safely. The next section, **Data warehousing**, takes these models into the systems that store and serve them: how warehouses are layered, how MPP engines and columnar storage execute your star schemas, how Snowflake, ClickHouse, DuckDB and others differ, and how partitioning, materialized views and semantic layers keep a warehouse fast and trustworthy.
