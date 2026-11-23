This is the last section of the course, and it changes the point of view. Every earlier section was organised by technique: SQL, modeling, Spark, streaming, BigQuery, governance. Real jobs are organised by **industry**, and each industry bends those techniques in its own direction: banks care about exact decimals, audit trails and microsecond timestamps; hospitals about patient privacy and medical data standards; shops about funnels and inventory; ride-hailing about maps; streaming services about billions of playback events; factories about sensors; games about live leaderboards and cheaters; advertising about auctions that finish in a tenth of a second.

It assumes you have worked through the course, or at least the core sections: modeling (Data modeling), warehousing and lakehouses, streaming (Streaming systems, Event-driven architecture), pipelines and orchestration, BigQuery in depth, Pipelines on Google Cloud, Data engineering system design, and Data governance, security and compliance. Graph data engineering, the previous section, appears again in fraud rings, identity graphs and cheat detection.

**What you will be able to do afterwards**

- Describe the typical data sources, pipelines, storage choices and regulations of eight industries: financial services, healthcare, e-commerce, mobility, media streaming, manufacturing, gaming and advertising.
- Recognise each industry's hard problems (bitemporal corrections, HL7 and FHIR, sessionization, geospatial indexing, playback telemetry, OPC UA and MQTT, leaderboards, real-time bidding) and the patterns that solve them.
- Sketch a reference architecture for each on Google Cloud, using the services from earlier sections.
- Speak the domain vocabulary well enough to work with domain experts, pass an industry-flavoured interview and choose which industry to specialise in.

**The big picture.** The same five-stage lifecycle from Data engineering foundations (generate, ingest, store, transform, serve) runs through every industry. What changes are four dials: **latency** (an ad auction has 100 ms; a regulatory report has a day), **correctness** (a bank cannot lose a cent; a recommendation can be slightly stale), **regulation** (HIPAA, MiFID II, GDPR) and **data shape** (ticks, clinical documents, clickstreams, GPS traces, sensor series, game events, bid logs). When you meet a new industry, ask where those four dials sit, and you will know which parts of this course matter most there.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Industries

The eight topics can be read in any order. Each one follows the same arc: the business and its data, the sources and their quirks, the core pipelines, the storage and serving choices on Google Cloud, the regulations and risks, and the interview angle. Code examples are small and runnable with Python's standard library, or written in BigQuery SQL, so you can focus on the idea rather than the setup.

Look for the patterns that repeat across industries: event streams with late data, slowly changing reference data, entity resolution, point-in-time correctness for features, experiments, and privacy. Recognising a familiar pattern in an unfamiliar industry is the most useful skill this section can give you.

### Financial Services Data Engineering

::: plain
Banks, brokers and exchanges run on data that must be exactly right, on time, and explainable years later. Every trade, price and payment is recorded, reconciled against other records, reported to regulators, and kept for years. Think of a bank's data platform as a very strict accountant who also has to work at the speed of a stock exchange: no rounding errors, no lost records, every number traceable to its source, and the clocks synchronised to fractions of a millisecond.
:::

**The data.** Financial firms deal with a few big families of data:

- **Market data**: quotes (bid and ask prices) and trades from exchanges and venues, delivered as **ticks**, often millions per second across instruments, plus reference data (instruments, corporate actions such as splits and dividends, calendars).
- **Trade and order data**: orders sent, fills received, allocations to client accounts, confirmations and settlements.
- **Positions and risk**: holdings per account and instrument, valuations, exposures and risk measures such as Value at Risk.
- **Payments and ledgers**: account balances and transactions, which must balance (double-entry: every debit has a matching credit).
- **Customer and compliance data**: know-your-customer (KYC) records, anti-money-laundering (AML) alerts, fraud signals.

**Market data normalisation.** Every venue sends data in its own format, symbology and time zone. Normalisation maps them into one model: a single instrument identifier (internal ID mapped to ISIN, FIGI, exchange ticker and vendor codes), prices in a consistent scale and currency, timestamps in UTC, and adjustments for corporate actions so that a 2-for-1 split does not look like a 50% crash in historical charts.

**Money and time are the two classic bugs.** Floating point cannot represent most decimal fractions exactly, so money uses decimal types: `NUMERIC` or `BIGNUMERIC` in BigQuery, `Decimal` in Python. And venues report local times, which shift with daylight saving time on different dates in different countries. This example normalises three trades to UTC, sorts them, and keeps a running position:

```python
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

print(0.1 + 0.2)                               # 0.30000000000000004
print(Decimal("0.1") + Decimal("0.2"))         # 0.3

# trades as reported by each venue, in the venue's local time
trades = [
    ("LSE",  "Europe/London",    "2026-03-27 15:59:59.250", "BUY",  100, "101.25"),
    ("NYSE", "America/New_York", "2026-03-27 11:00:00.100", "SELL",  40, "101.40"),
    ("LSE",  "Europe/London",    "2026-03-30 08:00:00.000", "BUY",   10, "100.90"),
]

def to_utc(local, tz):
    ts = datetime.strptime(local, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=ZoneInfo(tz))
    return ts.astimezone(ZoneInfo("UTC"))

normalised = sorted((to_utc(local, tz), venue, side, qty, Decimal(price))
                    for venue, tz, local, side, qty, price in trades)
position = 0
for utc, venue, side, qty, price in normalised:
    position += qty if side == "BUY" else -qty
    print(utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], venue, side, qty, price * qty, position)
# 2026-03-27 15:00:00.100 NYSE SELL 40 4056.00 -40
# 2026-03-27 15:59:59.250 LSE BUY 100 10125.00 60
# 2026-03-30 07:00:00.000 LSE BUY 10 1009.00 70
```

The New York trade listed second actually happened first: on 27 March 2026 New York was already on summer time (UTC-4) while London was not yet (UTC+0). Processing in the order received would have shown a different intraday position.

**Real-time position tracking.** Trading desks need positions updated within milliseconds of each fill. The pattern is a stream of fills (Kafka or Google Managed Service for Apache Kafka, or Pub/Sub) consumed by a stateful job (Flink, Dataflow or Kafka Streams) keyed by account and instrument, with exactly-once processing so a fill is never counted twice (Streaming systems). Intraday positions are then **reconciled** at end of day against the books of record and against custodians' and brokers' statements; breaks (differences) become exceptions that people resolve.

**Bitemporal data.** Financial data is constantly corrected: a trade booked with the wrong price is amended two days later. Regulators may ask both "what was the position on 1 March?" and "what did you **report** on 1 March, before the correction?". **Bitemporal** tables answer both by keeping two time ranges per row: **valid time** (when the fact was true in the world) and **transaction time** (when the system recorded it). Rows are never updated in place; corrections close the old row's transaction time and insert a new row.

```sql
-- Position as we knew it on 2026-03-02, for the business date 2026-03-01
SELECT account_id, instrument_id, quantity
FROM risk.positions_bitemporal
WHERE DATE '2026-03-01' >= valid_from AND DATE '2026-03-01' < valid_to
  AND TIMESTAMP '2026-03-02 00:00:00+00' >= recorded_from
  AND TIMESTAMP '2026-03-02 00:00:00+00' < recorded_to;
```

**Regulatory reporting.**

- **MiFID II** (EU) requires investment firms to report transactions to regulators by the next working day (transaction reporting under RTS 22, with dozens of fields per trade), and to synchronise business clocks to UTC (RTS 25): high-frequency trading must stay within 100 microseconds of UTC with timestamps at 1 microsecond granularity or finer.
- **BCBS 239**, the Basel Committee's principles for risk data aggregation and risk reporting, asks large banks to produce accurate, complete and timely risk data, including in a crisis, with clear ownership, data quality controls and lineage from report back to source. In practice it is a governance programme: the catalog, lineage and quality tools of the previous section.
- Other regimes follow the same pattern: trade repositories (EMIR, Dodd-Frank), anti-money-laundering reporting, stress testing.

[[fig:industry_finance]]

**On Google Cloud.** A typical design uses Pub/Sub or Managed Kafka for fills and market events, Dataflow for streaming enrichment and positions, **Spanner** for ledgers and positions that need strongly consistent transactions across regions, Bigtable for high-volume tick history keyed by instrument and time, BigQuery for risk analytics, regulatory reporting and historical research, and Knowledge Catalog for lineage and quality evidence. Many trading firms also run specialised time-series databases such as kdb+ for tick analytics.

> **Pitfall:** Aggregating money in `FLOAT64`. Sums over millions of rows drift by cents, reconciliations fail and nobody can explain the difference. Use `NUMERIC` (or `BIGNUMERIC` for very large or very precise values) from ingestion onwards.

> **Tip:** Keep every source message, unchanged, in an immutable raw store with its receive time. When a regulator questions a report two years later, you must be able to reproduce it from the inputs as they were then.

::: deeper
**Event time in markets.** A trade has several timestamps: the exchange's matching time, the time the feed handler received it, and the time your system processed it. Store all of them. Latency analysis, best-execution reporting and clock-sync compliance each need a different one.

**Golden source and EOD batch.** Despite streaming, finance still runs a large **end-of-day batch**: official prices (closing marks), positions, P&L and risk computed once on a consistent snapshot. Intraday streaming gives speed; the end-of-day batch gives the official, reconciled numbers. Designs that try to replace one with the other usually fail.

**Fraud and AML.** Card fraud detection must decide within the payment authorisation (tens of milliseconds), using features computed in streaming (spend in the last hour, new merchant, distance from last transaction) served from a low-latency store such as Bigtable. AML looks for patterns across accounts over weeks (structuring, rings of mule accounts), which is where graph analytics (Graph data engineering) helps.

**Data retention and immutability.** Securities regulations require keeping records for years on non-rewritable storage (for example SEC Rule 17a-4 in the US). Cloud Storage retention policies with Bucket Lock are designed for this (Data governance, security and compliance).

> **Interview:** "Design a real-time position service for a trading desk." Fills from a durable log (Kafka) partitioned by account; a stateful streaming job with exactly-once semantics updates positions keyed by account and instrument; positions are stored in a strongly consistent database (Spanner) and pushed to traders' screens; every fill is archived raw; end-of-day reconciliation against the book of record raises breaks; time is UTC everywhere with clock synchronisation monitored; money is decimal.
:::

::: check
?? Why must financial pipelines use decimal types for money?
Binary floating point cannot represent most decimal fractions exactly, so sums drift by small amounts and reconciliations fail. Decimal types store exact decimal values.

?? What two time dimensions does a bitemporal table record, and why does finance need both?
Valid time (when the fact was true in the world) and transaction time (when the system recorded it). Finance must answer both "what was true on that date" and "what did we know or report at that time", because records are corrected after the fact.

?? Why are trades normalised to UTC before ordering them?
Venues report local times, and daylight saving changes on different dates in different countries, so local times do not sort into the true order of events.
:::

::: resources
- Spec: [BCBS 239: Principles for effective risk data aggregation and risk reporting](https://www.bis.org/publ/bcbs239.htm) · the Basel Committee's principles, the basis of risk data programmes in banks
- Spec: [MiFID II RTS 25 on clock synchronisation](https://eur-lex.europa.eu/eli/reg_del/2017/574/oj) · the regulation that sets UTC accuracy and timestamp granularity
- Article: [Bitemporal History](https://martinfowler.com/articles/bitemporal-history.html) · Martin Fowler's clear explanation of valid time and record time
- Docs: [Spanner](https://cloud.google.com/spanner) · globally consistent transactions for ledgers and positions
- Docs: [BigQuery data types](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types) · `NUMERIC`, `BIGNUMERIC` and their precision
- Blog: [Kafka in financial services](https://www.confluent.io/solutions/financial-services/) · Confluent's overview of streaming use cases in banking and trading
- Book: *Trading and Exchanges: Market Microstructure for Practitioners* by Larry Harris · how markets, orders and market data actually work
:::

### Healthcare Data Engineering — FHIR & HL7

::: plain
Hospitals produce huge amounts of data: admissions, lab results, prescriptions, scans, clinical notes. It lives in many systems that were never designed to talk to each other. Health data standards are the shared languages that let them: **HL7 version 2** is the old, telegram-like format still used inside most hospitals, and **FHIR** is the modern, web-friendly one. A healthcare data engineer translates between these languages, gathers the data into a platform where it can be analysed, and protects it fiercely, because few kinds of data are more private.
:::

**Sources.** Electronic health record systems (EHRs), laboratory and radiology systems, pharmacy, billing and claims, medical devices, and increasingly patient apps and wearables. Data arrives as HL7 v2 messages, FHIR resources, **DICOM** images (radiology, with metadata in the file headers), claims files, and free-text notes.

**HL7 v2.** Defined in the late 1980s and still the workhorse of in-hospital integration. A message is a set of **segments** separated by carriage returns; each segment has fields separated by `|`, components by `^`. Common message types are `ADT^A01` (admission), `ADT^A08` (update patient information) and `ORU^R01` (observation results, such as lab values). Messages usually travel over TCP using **MLLP** (minimal lower layer protocol), which wraps each message in start and end bytes. Parsing is mechanical but full of local variation, because each hospital customises fields:

```python
import json

msg = ("MSH|^~\\&|LAB|CITYHOSP|EHR|CITYHOSP|202611201030||ORU^R01|MSG00042|P|2.5.1\r"
       "PID|1||MRN12345^^^CITYHOSP^MR||Rao^Asha^K||19880412|F\r"
       "OBX|1|NM|2345-7^Glucose^LN||105|mg/dL|70-99|H|||F\r")

segments = {}
for line in filter(None, msg.split("\r")):
    fields = line.split("|")
    segments.setdefault(fields[0], []).append(fields)

pid = segments["PID"][0]
family, given = pid[5].split("^")[:2]
patient = {
    "resourceType": "Patient",
    "identifier": [{"system": "urn:cityhosp:mrn", "value": pid[3].split("^")[0]}],
    "name": [{"family": family, "given": [given]}],
    "gender": {"F": "female", "M": "male"}.get(pid[8], "unknown"),
    "birthDate": f"{pid[7][:4]}-{pid[7][4:6]}-{pid[7][6:8]}",
}
obx = segments["OBX"][0]
code, display, system = obx[3].split("^")
observation = {
    "resourceType": "Observation",
    "status": "final",
    "code": {"coding": [{"system": "http://loinc.org", "code": code, "display": display}]},
    "valueQuantity": {"value": float(obx[5]), "unit": obx[6]},
    "interpretation": [{"coding": [{"code": obx[8]}]}],
}
print(segments["MSH"][0][8])            # ORU^R01
print(json.dumps(patient))
# {"resourceType": "Patient", "identifier": [{"system": "urn:cityhosp:mrn", "value": "MRN12345"}], "name": [{"family": "Rao", "given": ["Asha"]}], "gender": "female", "birthDate": "1988-04-12"}
print(observation["code"]["coding"][0]["code"], observation["valueQuantity"])
# 2345-7 {'value': 105.0, 'unit': 'mg/dL'}
```

(In the `MSH` segment the field separator itself counts as field 1, so `MSH-9`, the message type, is at list index 8; in other segments index n is field n.) Production systems use tested libraries and mapping engines rather than hand-written parsers, but the shape of the work is exactly this: parse, map to a standard model, attach code systems.

**FHIR.** Fast Healthcare Interoperability Resources (HL7 FHIR) models health data as **resources**: `Patient`, `Encounter`, `Observation`, `Condition`, `MedicationRequest`, `Procedure` and about 150 more, each a JSON (or XML) document with a defined structure, linked by references (`"subject": {"reference": "Patient/123"}`). Resources are read and written through a REST API (`GET /Patient/123`, `GET /Observation?patient=123&code=2345-7`). **FHIR R4** (version 4.0.1) is the most widely deployed release and the basis of national rules, such as the US requirement that certified EHRs expose patient data through FHIR APIs; R5 was published in 2023. **Profiles** such as US Core constrain resources for a country or use case, and **terminologies** give meaning to codes: LOINC for lab tests, SNOMED CT for clinical findings, ICD-10 for diagnoses, RxNorm for medications.

For analytics, the **Bulk Data Access** specification (`$export`) exports large numbers of resources as newline-delimited JSON, and **SQL on FHIR** defines portable `ViewDefinition`s that flatten resources into tables.

**SMART on FHIR** is the security and launch layer: OAuth 2.0 and OpenID Connect profiles that let an app (or a backend service) obtain a token with scopes such as `patient/Observation.read`, so a third-party app can read a patient's data only with the right authorization.

[[fig:industry_health]]

**On Google Cloud**, the **Cloud Healthcare API** provides managed **FHIR stores**, **HL7v2 stores** (with an open-source MLLP adapter to receive messages from hospital systems) and **DICOM stores**, inside a healthcare dataset in a chosen region. FHIR stores can **stream changes to BigQuery** in an analytics-friendly schema, and the API has a built-in **de-identify** operation for FHIR, DICOM and text. A typical clinical data platform: HL7 v2 messages in through the MLLP adapter, Pub/Sub notifications on each new message, a Dataflow or Cloud Run mapping service that converts them into FHIR resources, a FHIR store as the operational source, streaming export to BigQuery for analytics, and de-identified copies for research.

**Privacy.** Health data in the US is protected health information (PHI) under HIPAA (Data governance, security and compliance covers the rules). Only services covered by Google Cloud's business associate agreement may hold PHI, access is tightly controlled and audited, and research datasets are **de-identified**: identifiers removed or replaced with keyed pseudonyms, dates shifted by a consistent random offset per patient (so intervals between visits are preserved), ZIP codes generalised, and free-text notes scanned for names and other identifiers.

> **Pitfall:** Treating an MRN (medical record number) as a global patient ID. Each hospital issues its own, patients get duplicate records, and merges happen. Use a master patient index (the MDM techniques from the previous section) and keep the source system with every identifier.

> **Tip:** Store the original message or resource alongside the mapped form. Mapping rules change and errors are found; with the original, you can re-map history instead of living with old mistakes.

::: deeper
**Updates and deletes in clinical feeds.** Clinical data is corrected often: lab results are amended, patients are merged (`ADT^A40`), encounters are cancelled. Pipelines must apply these as upserts and merges keyed by stable identifiers, and analytics must use the latest version of each resource. FHIR resources carry `meta.versionId` and `meta.lastUpdated`, and FHIR stores keep history.

**Terminology mapping.** Hospitals often use local codes for lab tests. Mapping them to LOINC is a large, ongoing task that mixes automation with review by clinical staff, and mapping errors silently corrupt research results. Treat the mapping table as governed reference data with owners and versions.

**OMOP versus FHIR.** For research, many institutions convert data to the **OMOP Common Data Model** (from the OHDSI community), a relational model designed for observational studies across institutions. FHIR is for exchange; OMOP is for analytics. Pipelines often go HL7 v2 to FHIR to OMOP.

**Imaging.** DICOM files are large (a CT study can be hundreds of megabytes); pipelines usually extract metadata to BigQuery and leave pixels in the DICOM store or Cloud Storage, with de-identification that also removes text burned into images.

> **Interview:** "Design a pipeline from hospital HL7 feeds to a research warehouse." MLLP adapter into an HL7v2 store; Pub/Sub notifications; a mapping service converts messages to FHIR resources (with original messages retained); FHIR store streams to BigQuery; a master patient index resolves identities; terminology mapping to standard codes; a de-identified research dataset (keyed pseudonyms, date shifting, generalised geography, scanned notes) with separate access control and audit; all under a BAA with HIPAA-eligible services only.
:::

::: check
?? What separators does HL7 v2 use for segments, fields and components?
Carriage returns separate segments, `|` separates fields and `^` separates components (with `~` for repetitions and `&` for subcomponents, declared in `MSH-2`).

?? What is SMART on FHIR?
A set of OAuth 2.0 and OpenID Connect profiles for FHIR that let apps and backend services obtain scoped access tokens to read or write patient data.

?? Why shift dates by a per-patient offset during de-identification instead of deleting them?
Shifting hides the real dates while preserving intervals between a patient's events (time between admission and readmission, for example), which research needs.
:::

::: resources
- Spec: [FHIR R4 overview](https://hl7.org/fhir/R4/overview.html) · resources, references, the REST API and the R4 release
- Spec: [HL7 Version 2 product brief](https://www.hl7.org/implement/standards/product_brief.cfm?product_id=185) · the v2 messaging standard and its versions
- Spec: [SMART App Launch](https://hl7.org/fhir/smart-app-launch/) · authorization scopes and launch flows for FHIR apps
- Docs: [Cloud Healthcare API](https://docs.cloud.google.com/healthcare-api/docs/introduction) · FHIR, HL7v2 and DICOM stores, de-identification and exports
- Docs: [Streaming FHIR resources to BigQuery](https://docs.cloud.google.com/healthcare-api/docs/how-tos/fhir-bigquery-streaming) · keeping an analytics copy of a FHIR store in BigQuery
- Spec: [SQL on FHIR](https://sql-on-fhir.org/) · portable view definitions for flattening FHIR into tables
- Book: *Principles of Health Interoperability: FHIR, HL7 and SNOMED CT* by Tim Benson and Grahame Grieve · standards and terminologies explained by one of FHIR's creators
:::

### E-Commerce Data Engineering

::: plain
An online shop is a data machine. Every visit leaves a trail of clicks: a product viewed, an item added to the basket, a checkout started, a purchase completed or abandoned. Data engineers turn those trails into answers ("where do shoppers give up?"), keep the stock numbers on the website honest so nobody buys something that is not on the shelf, feed the "you may also like" suggestions, and measure which website changes really sell more.
:::

**The data.** Clickstream events from the website and apps (page and product views, searches, basket and checkout events), orders and payments from the transactional database, the product catalog and prices, inventory from warehouses and stores, customer accounts, marketing campaigns, returns and customer service contacts.

**Clickstream ingestion.** Events come from a tag or SDK in the browser and app. On Google Cloud the simplest route is **Google Analytics 4 with its BigQuery export**: daily tables `events_YYYYMMDD` (plus intraday tables), one row per event, with `event_name`, `user_pseudo_id`, timestamps, device and traffic source, and a repeated `event_params` record of key-value pairs. Recommended ecommerce events (`view_item`, `add_to_cart`, `begin_checkout`, `purchase`) carry an `items` array. Querying parameters requires `UNNEST`:

```sql
SELECT
  event_date,
  COUNTIF(event_name = 'view_item')      AS item_views,
  COUNTIF(event_name = 'add_to_cart')    AS add_to_carts,
  COUNTIF(event_name = 'purchase')       AS purchases,
  SUM(IF(event_name = 'purchase', ecommerce.purchase_revenue, 0)) AS revenue
FROM `shop-prod.analytics_123456789.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20261101' AND '20261130'
GROUP BY event_date
ORDER BY event_date;

-- one parameter from the key-value array
SELECT (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page
FROM `shop-prod.analytics_123456789.events_20261120`
WHERE event_name = 'page_view';
```

Larger shops run their own collectors (a Cloud Run endpoint or server-side tagging writing to Pub/Sub, then Dataflow into BigQuery) for full control of schema, latency and consent.

**Sessions and funnels.** A **session** is a burst of activity; the common definition starts a new session after 30 minutes of inactivity. A **funnel** counts how many users reach each step. Both are window-function exercises, runnable here with SQLite:

```python
import sqlite3
db = sqlite3.connect(":memory:")
db.executescript("""
CREATE TABLE events (user_id TEXT, ts TEXT, event TEXT);
INSERT INTO events VALUES
 ('a', '2026-11-20 10:00', 'view_item'), ('a', '2026-11-20 10:02', 'add_to_cart'),
 ('a', '2026-11-20 10:05', 'begin_checkout'), ('a', '2026-11-20 10:07', 'purchase'),
 ('b', '2026-11-20 10:01', 'view_item'), ('b', '2026-11-20 10:03', 'add_to_cart'),
 ('b', '2026-11-20 11:30', 'view_item'),
 ('c', '2026-11-20 10:04', 'view_item');
""")
# sessions: a new session starts after 30 minutes of inactivity
for row in db.execute("""
WITH gaps AS (
  SELECT *, CASE WHEN (julianday(ts) - julianday(LAG(ts) OVER w)) * 1440 > 30
                   OR LAG(ts) OVER w IS NULL THEN 1 ELSE 0 END AS new_session
  FROM events WINDOW w AS (PARTITION BY user_id ORDER BY ts)
)
SELECT user_id, SUM(new_session) AS sessions FROM gaps GROUP BY user_id ORDER BY user_id
"""):
    print(row)
# ('a', 1)
# ('b', 2)
# ('c', 1)

# funnel: users reaching each step
for row in db.execute("""
SELECT COUNT(DISTINCT CASE WHEN event = 'view_item' THEN user_id END)      AS viewed,
       COUNT(DISTINCT CASE WHEN event = 'add_to_cart' THEN user_id END)    AS carted,
       COUNT(DISTINCT CASE WHEN event = 'begin_checkout' THEN user_id END) AS checkout,
       COUNT(DISTINCT CASE WHEN event = 'purchase' THEN user_id END)       AS purchased
FROM events
"""):
    print(row)
# (3, 2, 1, 1)
```

This simple funnel counts users who did each step at any time. A strict funnel also requires the steps in order within a session, which needs the session ID and step timestamps.

**Inventory.** Overselling (taking an order for stock that does not exist) costs money and customers. Inventory is best modelled as an **event log of movements** (received, reserved, shipped, returned, adjusted) from which on-hand and available quantities are derived, rather than a single mutable number that several systems overwrite. Real-time availability flows from the order database and warehouse system through change data capture (Datastream) and Pub/Sub into a fast store that the storefront reads. A nightly **reconciliation** compares the storefront's view, the warehouse management system and physical counts, and reports differences per SKU and location for investigation.

**Recommendation and pricing pipelines.** Recommendation features come from behaviour: co-views and co-purchases, recency and frequency per category, item embeddings. BigQuery ML can train a simple collaborative filter (`model_type = 'matrix_factorization'`) directly on purchase history; larger systems serve features from a feature store (Data engineering for AI and ML). Pricing pipelines collect competitor prices, demand signals and stock, and keep a full **price history** (a type 2 slowly changing dimension) so every order line can be joined to the price that applied at that moment.

**A/B testing.** Every change to the site (a new checkout button, a ranking algorithm) is tested by randomly assigning users to variants. Data engineering provides reliable **assignment logs** (who saw which variant, when), **exposure events**, metric pipelines, and health checks. The most important check is **sample ratio mismatch (SRM)**: if a 50/50 test does not split users close to 50/50, assignment or logging is broken and the results cannot be trusted.

```python
# Sample ratio mismatch: a 50/50 test assigned 50,600 and 49,400 users
control, treatment = 50_600, 49_400
expected = (control + treatment) / 2
chi2 = sum((obs - expected) ** 2 / expected for obs in (control, treatment))
print(f"chi-square = {chi2:.1f}")        # chi-square = 14.4
print("SRM!" if chi2 > 10.83 else "ok")  # SRM!   (10.83 is the critical value for p = 0.001, 1 degree of freedom)
```

> **Pitfall:** Counting revenue from clickstream `purchase` events. Tags miss purchases (ad blockers, network errors, users who decline consent) and some fire twice. Revenue comes from the order database; clickstream is for behaviour and ratios.

::: deeper
**Identity stitching.** Shoppers browse anonymously on one device, log in on another and buy on a third. Joining `user_pseudo_id` (a device and browser identifier) to logged-in customer IDs builds an identity graph; attribution and personalisation depend on it, and so does erasure.

**Peak events.** Sales days produce several times normal traffic. Pipelines must be load-tested, streaming jobs given headroom (Dataflow autoscaling limits, Pub/Sub quotas), and BigQuery capacity planned (reservations or autoscaling) for the reporting surge that follows.

**Experiment statistics.** Beyond SRM, experimentation platforms use variance reduction such as CUPED (adjusting metrics with pre-experiment behaviour), guard against peeking (stopping a test as soon as it looks significant inflates false positives; sequential tests fix this), and analyse at the randomisation unit (user, not session) to keep variance estimates correct.

**Returns and net revenue.** A purchase can be partially returned weeks later. Metrics such as net revenue must be restated as returns arrive, which means late-arriving facts and periodic recomputation of recent periods.

> **Interview:** "The checkout funnel conversion dropped 20% overnight. How do you investigate?" Check whether the drop is real or a data problem first: event volumes per step, a new app or site release changing event names, tagging or consent changes, bot traffic, the pipeline's freshness and quality checks. Compare against the order database. If orders also dropped, segment by device, browser, country and payment method to find where, and check recent deployments and experiments.
:::

::: check
?? How is a session usually defined in clickstream analysis?
A sequence of events from the same user with no gap longer than a timeout, commonly 30 minutes; a longer gap starts a new session.

?? Why should inventory be modelled as movements rather than a single quantity column?
A movement log is auditable, can be replayed to rebuild any point in time, avoids lost updates when several systems change stock, and makes reconciliation possible.

?? What does a sample ratio mismatch tell you about an A/B test?
That users were not split in the intended proportions, so assignment or logging is broken and the test results cannot be trusted until the cause is found.
:::

::: resources
- Docs: [GA4 BigQuery export schema](https://support.google.com/analytics/answer/7029846) · tables, fields and the event parameter structure
- Book: [Trustworthy Online Controlled Experiments](https://experimentguide.com/) · Kohavi, Tang and Xu; the standard guide to A/B testing, including sample ratio mismatch
- Docs: [BigQuery ML matrix factorization](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-matrix-factorization) · training a recommender in SQL
- Blog: [Shopify's data science and engineering foundations](https://shopify.engineering/shopifys-data-science-engineering-foundations) · how a large commerce platform organises its data work
- Docs: [Datastream overview](https://docs.cloud.google.com/datastream/docs/overview) · change data capture from order and inventory databases
- Book: *The Data Warehouse Toolkit* by Ralph Kimball and Margy Ross · the retail sales and inventory chapters are the classic models for commerce data
:::

### Ride-Sharing / Mobility Data Engineering

::: plain
A ride-hailing app is a live map of a city. Every few seconds, every driver's phone reports where it is; every rider's request is a point on the map with a destination. The platform must match riders to nearby drivers within seconds, guess how long the trip will take, raise prices where riders outnumber drivers, and afterwards store every trip for analysis. Data engineering here is about locations and time at once: where things are, how fast that changes, and how to summarise a city into areas you can compute on.
:::

**The data.** Driver location pings (latitude, longitude, speed, heading, accuracy) at high frequency; rider requests, offers, acceptances and cancellations; trips with routes, fares and ratings; payments; maps (road networks, points of interest); and surrounding context such as weather and events. Volumes are dominated by location pings: tens of thousands of active drivers reporting every few seconds is hundreds of millions of points per day in one large city.

**Geospatial basics.** Distances on Earth use the haversine formula (great-circle distance), and cities are divided into **cells** so that supply, demand and prices can be computed per area. A naive latitude-longitude grid works for a demonstration:

```python
import math
from collections import Counter

def haversine_km(lat1, lng1, lat2, lng2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))

def cell(lat, lng, size=0.01):            # about 1.1 km of latitude per 0.01 degree
    return (math.floor(lat / size), math.floor(lng / size))

print(round(haversine_km(18.5204, 73.8567, 19.0760, 72.8777), 1))   # Pune to Mumbai: 120.2

requests = [(18.5204, 73.8567), (18.5210, 73.8571), (18.5199, 73.8560), (18.5302, 73.8450)]
drivers  = [(18.5207, 73.8569), (18.5301, 73.8455), (18.5305, 73.8448)]
demand, supply = Counter(cell(*p) for p in requests), Counter(cell(*p) for p in drivers)
for c in sorted(demand | supply):
    if supply[c] == 0:
        status = "no drivers"
    else:
        status = "surge" if demand[c] / supply[c] > 1.5 else "normal"
    print(c, "demand", demand[c], "supply", supply[c], status)
# (1851, 7385) demand 1 supply 0 no drivers
# (1852, 7385) demand 2 supply 1 surge
# (1853, 7384) demand 1 supply 2 normal
```

Notice the first two cells: three requests within 100 metres of each other were split across two cells because they straddle a boundary. Square grids also distort with latitude and have neighbours at two different distances (edge and corner). That is why Uber created **H3**, an open-source hierarchical **hexagonal** grid: every hexagon has six equidistant neighbours, there are 16 resolutions (from continent-sized to about a square metre), and "the cell plus its ring of neighbours" smooths boundary effects. Google's **S2** library uses a hierarchy of squares on a cube projected onto the sphere and is what BigQuery exposes natively.

**Geospatial SQL in BigQuery.** The `GEOGRAPHY` type and `ST_` functions handle points, lines and polygons on the sphere:

```sql
-- requests in the last 5 minutes within 2 km of a stadium, bucketed into S2 cells
SELECT S2_CELLIDFROMPOINT(ST_GEOGPOINT(lng, lat), level => 13) AS cell,
       COUNT(*) AS requests
FROM mobility.ride_requests
WHERE request_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
  AND ST_DWITHIN(ST_GEOGPOINT(lng, lat), ST_GEOGPOINT(73.8567, 18.5204), 2000)
GROUP BY cell;
```

`ST_GEOGPOINT` takes **longitude first**. H3 functions are available in BigQuery through community function libraries such as CARTO's Analytics Toolbox.

**Surge pricing pipeline.** A streaming job (Dataflow or Flink) consumes requests and driver states, assigns each to a cell, and computes supply and demand per cell in short sliding windows (for example, the last 2 minutes, updated every 30 seconds). A pricing service combines that with smoothing (so prices do not flicker between neighbouring cells and minutes), caps and rules, and writes multipliers to a low-latency store (Memorystore or Bigtable) that the app reads. Every multiplier shown is logged with its inputs, because regulators and riders may ask why a price was what it was.

**ETA models** need historical travel times per road segment and time of week. GPS pings are noisy, so they are first **map matched**: snapped to the most likely sequence of road segments. Segment speeds are then aggregated (median speed per segment per 15-minute slot of the week) to form features; real-time speeds from current trips adjust them.

**The trip lakehouse.** Completed trips go to a lakehouse table partitioned by **city and local date**. Partitioning by city keeps queries (almost always scoped to a city or region) cheap and lets each city's data follow its local time zone and, where needed, data residency rules. Trips are updated after they end (fare adjustments, ratings, disputes), so the table needs upserts: one reason Uber created **Apache Hudi**, built for incremental upserts on a data lake.

[[fig:industry_mobility]]

> **Pitfall:** Swapping latitude and longitude. Many APIs take `(lat, lng)`, GeoJSON and BigQuery take `(lng, lat)`. Points in the wrong order often still parse, landing in the ocean or another continent. Validate coordinates against each city's bounding box at ingestion.

::: deeper
**Real-time matching data.** Matching needs every available driver near a request, now. Positions are held in memory, indexed by cell, sharded by city or region; a request queries the cells within a radius (an H3 ring or S2 covering) and ranks candidates by ETA, not straight-line distance. The data platform's job is to feed this index with fresh positions and to log every decision for later analysis.

**Late and out-of-order pings.** Phones lose signal in tunnels and send batches later. Streaming jobs use event-time windows with watermarks (Streaming systems) and must decide how long to wait; surge needs freshness and accepts approximation, billing needs completeness and waits.

**City-level operations.** Each city has its own rules (airport queues, regulatory caps on surge, data sharing with transport authorities). Model city as a first-class dimension with its configuration versioned, so historical analysis uses the rules that applied at the time.

**Marketplace experiments.** Riders and drivers interact, so randomising individual users leaks effects between groups (a pricing test on some riders changes driver availability for all). Mobility companies use **switchback** experiments (alternate the treatment for a whole city or region over time slots) and geographic randomisation.

> **Interview:** "Design the data pipeline behind surge pricing." Driver states and rider requests stream through Pub/Sub or Kafka; a streaming job maps events to hexagonal cells and computes supply and demand in sliding event-time windows; a pricing service smooths across neighbouring cells and time, applies caps, and writes multipliers to a low-latency store read by the app; every multiplier and its inputs is logged to the lakehouse for audit, analysis and model training; switchback experiments evaluate changes.
:::

::: check
?? Why do mobility platforms prefer hexagonal cells (H3) over a square latitude-longitude grid?
Hexagons have six equidistant neighbours and more uniform shapes, so neighbourhood smoothing and distance-based aggregation behave consistently; square grids have two neighbour distances and distort with latitude.

?? What is map matching, and why is it needed for ETA models?
Snapping noisy GPS pings to the most likely sequence of road segments. Travel times must be computed per road segment, which raw pings do not tell you reliably.

?? Why partition the trips table by city and local date?
Queries are almost always scoped to a city and local day, so they prune well; local dates follow each city's time zone; and city partitions make residency and city-specific rules easier to apply.
:::

::: resources
- Docs: [H3 documentation](https://h3geo.org/) · the hexagonal hierarchical index: resolutions, neighbours and APIs
- Blog: [H3: Uber's hexagonal hierarchical spatial index](https://www.uber.com/blog/h3/) · why Uber built H3 and how it is used for pricing and dispatch
- Blog: [Uber's Big Data Platform: 100+ petabytes with minute latency](https://www.uber.com/blog/uber-big-data-platform/) · the evolution that led to Apache Hudi
- Docs: [Introduction to geospatial analytics in BigQuery](https://docs.cloud.google.com/bigquery/docs/geospatial-intro) · the `GEOGRAPHY` type, functions and visualisation
- Docs: [BigQuery geography functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions) · `ST_DWITHIN`, `S2_CELLIDFROMPOINT` and the rest
- Docs: [Apache Hudi](https://hudi.apache.org/) · the upsert-oriented table format created at Uber
:::

### Media & Streaming Data Engineering

::: plain
When you watch a show on a streaming service, your player quietly reports what is happening every few seconds: playing, paused, buffering, which quality level, which device. Multiply that by hundreds of millions of viewers and you get one of the biggest event streams in the world. Media data engineers turn it into answers: what people watch and finish, whether the video played smoothly, which artwork or recommendation makes someone press play, and which subscribers are about to cancel.
:::

**The data.** Playback events and heartbeats from players on TVs, phones and browsers; quality-of-experience (QoE) metrics; the content catalog and its metadata (genres, cast, artwork, rights windows per country); recommendations shown and clicked ("impressions"); search; subscriptions and payments; content delivery network (CDN) logs; and experiment assignments.

**Viewing event ingestion at scale.** Players send events in batches over HTTPS to collection endpoints, which validate them and write to a durable log. Netflix's **Keystone** pipeline (Kafka plus Flink) and Spotify's event delivery system, which moved to **Google Cloud Pub/Sub** with Dataflow and BigQuery, are well-documented examples. Design points that recur:

- **Schemas and versioning**: thousands of client versions are in the field at once; events must carry a schema version and the pipeline must accept old versions for years.
- **At-least-once delivery**: clients retry, so events carry unique IDs and the pipeline deduplicates.
- **Client clocks are wrong**: record both device time and server receive time; use server time for ordering across devices and device time for sequence within a session.
- **Sampling**: high-volume debug telemetry can be sampled, but business events (play starts, purchases) must not be.

**From heartbeats to watch time.** A heartbeat every 30 seconds while a title plays makes watch time and QoE simple sums over states:

```python
# heartbeat events every 30 s while a title plays: (session, seconds since start, state)
beats = [
    ("s1", 0, "playing"), ("s1", 30, "playing"), ("s1", 60, "buffering"),
    ("s1", 90, "playing"), ("s1", 120, "playing"), ("s1", 150, "stopped"),
    ("s2", 0, "playing"), ("s2", 30, "playing"), ("s2", 60, "stopped"),
]
INTERVAL = 30
stats = {}
for session, _, state in beats:
    s = stats.setdefault(session, {"play": 0, "buffer": 0})
    if state == "playing":
        s["play"] += INTERVAL
    elif state == "buffering":
        s["buffer"] += INTERVAL
for session, s in stats.items():
    rebuffer = s["buffer"] / (s["play"] + s["buffer"])
    print(session, "watched", s["play"], "s, rebuffer ratio", f"{rebuffer:.0%}")
# s1 watched 120 s, rebuffer ratio 20%
# s2 watched 60 s, rebuffer ratio 0%
```

Real pipelines also handle missing heartbeats (a phone that loses connection), pauses, seeks, and sessions spanning midnight.

**QoE and CDN logs.** Key quality metrics are **startup time** (press play to first frame), **rebuffer ratio**, **average bitrate** and **playback failures**, sliced by device, network, CDN and region. CDN logs (every segment request with status, bytes, latency and cache hit or miss) explain them. On Google Cloud, Media CDN and Cloud CDN write request logs to Cloud Logging, which can be routed to BigQuery; at large scale, logs are aggregated in streaming before storage to keep costs down.

**Recommendation pipelines.** Recommendations use implicit feedback: plays, completion rate, re-watches, searches, impressions that were **not** clicked. Pipelines produce training examples ("this user was shown these titles at this time and played this one"), features computed as of the impression time (point-in-time correctness, so the model never sees the future), and candidate sets refreshed daily or faster. Netflix famously personalises artwork as well as titles, so the logging must record exactly which image was shown.

**Experimentation for content.** Streaming services test everything: ranking algorithms, artwork, UI layouts, encoding settings. Long-term metrics (retention months later) matter more than clicks, so experiment pipelines join assignments to subscription outcomes over long windows. **Interleaving** (mixing results from two ranking algorithms in one list and seeing which items get played) detects ranking improvements with far fewer users than A/B tests.

**Churn feature pipelines.** Churn models predict cancellations from features such as days since last play, viewing hours this month versus the previous three, number of profiles active, unfinished series, failed payments and recent support contacts. The features are computed daily per subscriber as a snapshot table, and labels (cancelled within the next 30 days) are joined later.

```sql
-- daily churn features as of a snapshot date (no information after it may leak in)
DECLARE snapshot DATE DEFAULT '2026-11-01';
SELECT s.subscriber_id,
       DATE_DIFF(snapshot, MAX(DATE(v.started_at)), DAY) AS days_since_last_play,
       SUM(IF(DATE(v.started_at) > DATE_SUB(snapshot, INTERVAL 30 DAY), v.seconds, 0)) / 3600 AS hours_30d,
       SUM(IF(DATE(v.started_at) BETWEEN DATE_SUB(snapshot, INTERVAL 120 DAY)
                                     AND DATE_SUB(snapshot, INTERVAL 31 DAY), v.seconds, 0)) / 3600 / 3 AS hours_prev_avg
FROM media.subscriptions AS s
LEFT JOIN media.viewing AS v
  ON v.subscriber_id = s.subscriber_id AND DATE(v.started_at) <= snapshot
WHERE s.status_as_of(snapshot) = 'active'
GROUP BY s.subscriber_id;
```

(Here `status_as_of` stands for however your model reconstructs subscription status at a date, typically a join to a type 2 subscription history table.)

> **Pitfall:** Building churn features from the current state of tables. If a feature uses today's profile count for a subscriber who cancelled last month, the model learns from information it would never have at prediction time and looks far better in testing than in production.

::: deeper
**Rights and availability.** Content is licensed per country and time window, and catalogs differ by region. Analytics must use the catalog as it was in that country at that time ("was this title even available?") before concluding that viewers did not want it.

**Real-time QoE monitoring.** Playback failures after a new client release or a CDN problem must be detected in minutes. A streaming aggregation of failure rates by device model, app version, CDN and region, with anomaly detection, feeds dashboards and automatic traffic steering between CDNs.

**Counting unique viewers.** Daily active viewers per title per country across billions of events uses approximate distinct counts (HyperLogLog sketches, `APPROX_COUNT_DISTINCT` and the `HLL_COUNT` functions in BigQuery), which can be merged across days and dimensions without rescanning raw data.

**Music streaming specifics.** Royalty reporting requires exact play counts per track, territory and rights holder, with rules on what counts as a play (for example, a minimum listening duration). Those pipelines are financial pipelines, with the reconciliation and audit requirements of the finance topic.

> **Interview:** "Design the pipeline for viewing events of a global streaming service." Clients batch events with IDs, schema versions and device times to regional collectors; collectors validate and publish to Pub/Sub or Kafka; a streaming job deduplicates, enriches with catalog and device data, and computes real-time QoE aggregates for monitoring; raw and cleaned events land in partitioned BigQuery or lakehouse tables; batch jobs build sessions, watch time, recommendation training sets and churn features with point-in-time correctness; approximate distinct counts serve reach metrics.
:::

::: check
?? Why record both device time and server receive time for playback events?
Device clocks can be wrong, so server time is needed to order events across devices; device time preserves the true sequence and timing within a session, especially for events sent late in batches.

?? What is rebuffer ratio?
The share of a playback session spent buffering (stalled) rather than playing, a key quality-of-experience metric.

?? What makes a churn feature leak information from the future, and why is that a problem?
Computing it from data after the snapshot date (such as current profile counts or later activity). The model then looks accurate in testing but cannot have that information when predicting, so it performs worse in production.
:::

::: resources
- Blog: [Evolution of the Netflix data pipeline](https://netflixtechblog.com/evolution-of-the-netflix-data-pipeline-da246ca36905) · how Netflix's event pipeline grew into Keystone
- Blog: [Spotify's event delivery: the road to the cloud](https://engineering.atspotify.com/2016/02/spotifys-event-delivery-the-road-to-the-cloud-part-i) · Spotify's move of event delivery to Pub/Sub on Google Cloud
- Repo: [Scio](https://github.com/spotify/scio) · Spotify's Scala API for Apache Beam, used with Dataflow and BigQuery
- Docs: [Media CDN logging](https://docs.cloud.google.com/media-cdn/docs/logging) · request logs for video delivery on Google Cloud
- Paper: [The Netflix Recommender System: Algorithms, Business Value, and Innovation](https://dl.acm.org/doi/10.1145/2843948) · Gomez-Uribe and Hunt; how recommendations and experiments drive retention
- Book: [Trustworthy Online Controlled Experiments](https://experimentguide.com/) · experiment design and long-term metrics, with many media examples
:::

### Manufacturing / Industrial IoT Data Engineering

::: plain
A modern factory is full of sensors: temperatures, pressures, vibrations, motor speeds, counts of parts made and rejected. The machines are run by controllers that speak industrial protocols, not web APIs. Industrial IoT data engineering brings those readings out of the factory floor safely, stores them as time series, and uses them to spot problems early, for example noticing that a motor's vibration is slowly rising weeks before it breaks, so it can be repaired during a planned stop instead of halting the line.
:::

**The layers of a factory.** The **ISA-95** standard describes the levels that data passes through:

| Level | What lives there | Data |
|---|---|---|
| 0-1 | Sensors and actuators | Raw signals, milliseconds |
| 2 | Control: PLCs (programmable logic controllers), SCADA, HMIs | Tags (named signals), alarms, seconds |
| 3 | MES (manufacturing execution system), historians | Batches, work orders, quality results |
| 4 | ERP and business systems | Orders, materials, costs |

Most valuable analysis joins level 2 sensor data with level 3 production context (which product, batch and work order was running) and level 4 business data (cost, customer).

**Industrial protocols.**

- **OPC UA** (IEC 62541) is the modern standard for industrial interoperability. It exposes an **information model**: a tree of typed nodes (a machine, its spindle, the spindle's temperature) with metadata such as units and ranges, accessible by client-server subscriptions and, in newer versions, publish-subscribe. Older plants also use Modbus, PROFINET, EtherNet/IP and vendor protocols.
- **MQTT** is a lightweight publish-subscribe protocol widely used to move data from the edge to the cloud. **Sparkplug B** (an Eclipse specification) adds what plain MQTT lacks for industry: a topic namespace (`spBv1.0/<group>/<message type>/<edge node>/<device>`), a Protobuf payload with typed metrics and timestamps, and **birth and death certificates** so consumers know when a device comes online or goes offline and what metrics it publishes.
- **Historians** (such as AVEVA PI) have stored plant time series for decades and are often the first source a cloud project connects to.

**Getting data out.** Control networks are isolated for safety, so data leaves through an **edge gateway** in a demilitarised network zone, which reads OPC UA or PLC tags, buffers locally when the connection drops, and publishes upwards over MQTT or HTTPS. On Google Cloud, the managed IoT Core service was retired in August 2023; current designs use an MQTT broker (self-managed on GKE or a partner product) or partner edge platforms, bridged into **Pub/Sub**. Google's **Manufacturing Data Engine** is a solution that packages this flow (ingestion, normalisation with tag metadata, storage in Bigtable, BigQuery and Cloud Storage) with partner edge connectivity.

[[fig:industry_iot]]

**Time-series storage.** Sensor data is narrow, high-volume and append-only, and is read by device and time range.

- **Bigtable** serves recent, high-resolution data for dashboards and applications. Row key design is the whole game: a key such as `plant#line#machine#sensor#yyyymmddhh` groups one hour of a sensor's readings into one row (readings as columns or cells), so reading a sensor's last day is a short range scan, and writes spread across machines rather than hammering one node (Databases: OLTP, NoSQL and specialised).
- **BigQuery** holds the full history for analysis, partitioned by date and clustered by machine and sensor. Time-series functions help with irregular data: `TIMESTAMP_BUCKET` aligns readings to fixed intervals and `GAP_FILL` fills missing intervals by interpolation or carrying the last value forward.

```sql
-- one-minute averages per sensor, with missing minutes filled by linear interpolation
SELECT *
FROM GAP_FILL(
  (
    SELECT machine_id, sensor,
           TIMESTAMP_BUCKET(ts, INTERVAL 1 MINUTE) AS minute,
           AVG(value) AS avg_value
    FROM iot.readings
    WHERE DATE(ts) = '2026-11-20' AND machine_id = 'press-07'
    GROUP BY machine_id, sensor, minute
  ),
  ts_column => 'minute',
  bucket_width => INTERVAL 1 MINUTE,
  partitioning_columns => ['machine_id', 'sensor'],
  value_columns => [('avg_value', 'linear')]
);
```

**Downsampling and alarms.** Raw readings at 1 to 1,000 Hz are reduced to summaries (mean, min, max, standard deviation per window) for most uses, with raw data kept for a shorter period. Windowed summaries also drive simple alarms:

```python
from statistics import mean

# vibration readings (mm/s) from one motor, one per second
readings = [2.1, 2.0, 2.2, 2.1, 2.3, 2.2, 2.4, 3.9, 4.2, 4.6, 4.8, 5.1]
WINDOW = 4
for start in range(0, len(readings), WINDOW):
    w = readings[start:start + WINDOW]
    avg, peak = mean(w), max(w)
    status = "ALARM" if avg > 4.5 else "WARN" if peak > 3.5 else "ok"
    print(f"t={start:>2}-{start + WINDOW - 1:>2}s avg={avg:.2f} max={peak:.1f} {status}")
# t= 0- 3s avg=2.10 max=2.2 ok
# t= 4- 7s avg=2.70 max=3.9 WARN
# t= 8-11s avg=4.67 max=5.1 ALARM
```

**Predictive maintenance.** Models predict failures or remaining useful life from features such as rolling statistics, trends, frequency-domain features of vibration (FFT bands) and operating context (load, speed, product). Labels come from maintenance records in a CMMS (computerised maintenance management system), which are often incomplete and free-text, so building a reliable label table is usually the hardest data engineering task. Failures are rare, so datasets are highly imbalanced.

**Digital twins.** A digital twin is a data model of a physical asset or line: its structure (asset hierarchy from the OPC UA information model or the MES), its live state (latest readings), its history and, optionally, a simulation. For a data engineer it is mostly a well-governed asset hierarchy with stable identifiers, joined to time series and events.

> **Pitfall:** Ingesting thousands of tags without their metadata. A column called `TT_401_PV` means nothing without its unit, range, machine and meaning. Capture tag metadata (from OPC UA, Sparkplug birth certificates or the historian) into a governed reference table and join it at ingestion.

::: deeper
**Edge versus cloud processing.** Control decisions that must happen in milliseconds stay at the edge (PLCs). Edge gateways often also filter, compress and aggregate (sending changes only, known as report by exception, or one summary per second) to reduce bandwidth, while keeping raw data locally for a while for investigations. Cloud processing handles cross-site analytics, model training and long history.

**Timestamps and clocks.** Sensor timestamps can come from the PLC, the gateway or the cloud receive time, and they disagree. Prefer source timestamps synchronised by NTP or PTP at the plant, store receive time too, and use event-time windows with generous lateness for buffered data after network outages.

**Unified namespace.** A popular architecture pattern publishes all plant data (machine states, MES events, quality results) into one hierarchical MQTT topic structure modelled on ISA-95 (`enterprise/site/area/line/cell/...`), so every consumer subscribes to a single, consistent source instead of point-to-point integrations.

**Security.** Operational technology (OT) networks were designed for isolation, and an exposed PLC can cause physical harm. Data flows are one-way from OT to IT wherever possible (sometimes with hardware data diodes), gateways are hardened, and standards such as IEC 62443 govern industrial cybersecurity.

> **Interview:** "Design a predictive maintenance platform for 50 plants." Edge gateways read OPC UA tags and publish Sparkplug B over MQTT with local buffering; a bridge publishes to Pub/Sub; Dataflow normalises readings with tag metadata, computes windowed features and writes to Bigtable (recent, high resolution) and BigQuery (history); maintenance records are cleaned into labels; models train in BigQuery ML or the Gemini Enterprise Agent Platform and score in streaming; alerts go to maintenance systems; the asset hierarchy is governed reference data shared by every plant.
:::

::: check
?? What does Sparkplug B add on top of plain MQTT?
A standard topic namespace, a typed Protobuf payload with metrics and timestamps, and birth and death certificates that announce when devices come online or go offline and which metrics they publish.

?? Why does a Bigtable row key for sensor data start with the device or sensor rather than the timestamp?
Reads are usually by device and time range, which then become short range scans, and writes from many devices spread across the key space instead of all landing on the newest-timestamp node.

?? Why is building labels often the hardest part of predictive maintenance?
Failure labels come from maintenance records that are incomplete, inconsistent and free-text, and failures are rare, so assembling a reliable, well-timed label table takes a lot of cleaning and domain knowledge.
:::

::: resources
- Spec: [OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/) · the OPC Foundation's overview of the industrial interoperability standard
- Spec: [Eclipse Sparkplug](https://sparkplug.eclipse.org/) · the specification for industrial MQTT payloads and state
- Docs: [MQTT](https://mqtt.org/) · the protocol, its versions and client libraries
- Docs: [Manufacturing Data Engine](https://docs.cloud.google.com/manufacturing-data-engine/docs) · Google Cloud's solution for ingesting and modelling factory data
- Docs: [Bigtable schema design for time series data](https://docs.cloud.google.com/bigtable/docs/schema-design-time-series) · row key patterns for sensor data
- Docs: [BigQuery time series functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/time-series-functions) · `GAP_FILL`, `TIMESTAMP_BUCKET` and related functions
:::

### Gaming Data Engineering

::: plain
Every action in a video game can be recorded: a match started, a level finished, an item found, a purchase made, a player who quit after losing three times in a row. Game studios use this telemetry to balance the game, catch cheaters, run live events and keep the in-game economy healthy, and they need some answers instantly, like where a player ranks on a global leaderboard right now. Think of it as running a theme park where every ride, ticket and queue is measured live, and the park's money (in-game currency) must not lose its value.
:::

**The data.** Gameplay telemetry (sessions, matches, levels, deaths, item pickups, chat reports), economy events (currency earned and spent, items crafted, traded and sold), purchases and ads revenue, matchmaking and server performance, player accounts and social graphs, and anti-cheat signals. Free-to-play games make most money from a small share of players, so purchase data and player segmentation receive a lot of attention.

**Telemetry pipelines.** Game clients batch events and send them to collectors, often through an SDK. Gaming adds its own wrinkles:

- **Many builds in the field**: every game version may log slightly different events, so events carry the build number and schema version, and pipelines keep compatibility for old builds.
- **Offline play**: mobile games buffer events offline and send them hours or days later. Use event time and allow very late data in batch, while real-time dashboards accept some incompleteness.
- **Launch spikes**: a new season or a viral moment multiplies traffic in minutes, so ingestion must autoscale (Pub/Sub and Dataflow do this well).

On Google Cloud, mobile games often start with **Google Analytics for Firebase**, whose BigQuery export provides daily event tables much like GA4's; larger studios run their own collectors into Pub/Sub, Dataflow and BigQuery. Player profiles and inventories that must be strongly consistent at global scale often live in **Spanner**, game servers run on GKE with the open-source **Agones** project, and the open-source **Open Match** framework handles matchmaking.

**Real-time leaderboards.** A leaderboard needs fast updates and fast "top N" and "my rank" queries for millions of players. The standard tool is a **sorted set** in Redis or Valkey (Memorystore on Google Cloud): `ZADD board 990 bo`, `ZINCRBY board 50 ana`, `ZREVRANGE board 0 9 WITHSCORES` for the top 10 and `ZREVRANK board bo` for a player's rank, all in logarithmic time. The same idea in Python, with a sorted list standing in for the sorted set:

```python
import bisect

# a tiny leaderboard: scores kept sorted, like a Redis sorted set
board = []                                   # list of (-score, player)
best = {}
def submit(player, score):
    if score <= best.get(player, -1):
        return                               # keep only each player's best score
    if player in best:
        board.remove((-best[player], player))
    best[player] = score
    bisect.insort(board, (-score, player))

for p, s in [("ana", 820), ("bo", 990), ("cy", 640), ("ana", 1010), ("dee", 990)]:
    submit(p, s)
print([(p, -s) for s, p in board[:3]])                  # [('ana', 1010), ('bo', 990), ('dee', 990)]
print("bo rank:", board.index((-best["bo"], "bo")) + 1)   # bo rank: 2
```

Ties need a rule (here alphabetical; games often prefer "first to reach the score"). Seasonal and regional boards are separate keys, often with expiry. The leaderboard is a serving view; the authoritative scores are also written to durable storage and BigQuery, so the board can be rebuilt and audited.

**Cheating detection.** Signals include statistical outliers (accuracy, reaction times or win rates far above a player's skill bracket), physically impossible events (moving faster than the game allows, which server-side validation should reject outright), abnormal economy activity (gaining currency faster than any legitimate path permits), and graph patterns (accounts that always play together and trade items to one account, typical of bot farms and boosting rings: Graph data engineering). Studios combine rules, models and human review, and often ban in delayed waves so cheat makers cannot easily tell which signal caught them.

**Loot drops and fairness.** Games publish drop rates for random rewards (and several platforms and countries require it). Pipelines verify that observed rates match the published odds within statistical error, per item and platform, and alert on deviations, which usually indicate bugs.

**Game economy monitoring.** Virtual economies have **sources** (where currency and items enter: quest rewards, daily bonuses, purchases) and **sinks** (where they leave: shop purchases, crafting costs, repair fees). If sources outgrow sinks, currency piles up and prices in player markets inflate, which ruins progression. Economy dashboards track the source to sink ratio, currency held per player segment, and market prices; a sudden jump usually means an exploit (an item duplication bug, for example).

```python
# game economy: coins created (sources) and destroyed (sinks) per day
days = {"2026-11-18": (120_000, 115_000), "2026-11-19": (125_000, 118_000),
        "2026-11-20": (190_000, 117_000)}
for day, (minted, spent) in days.items():
    ratio = minted / spent
    print(day, f"source/sink ratio {ratio:.2f}", "ALERT" if ratio > 1.3 else "")
# 2026-11-18 source/sink ratio 1.04
# 2026-11-19 source/sink ratio 1.06
# 2026-11-20 source/sink ratio 1.62 ALERT
```

> **Pitfall:** Trusting the client. Anything computed on the player's device (scores, currency gains, positions) can be forged. The authoritative state must be computed or validated on the server, and analytics should treat client-reported values as claims to verify.

::: deeper
**Core game metrics.** Daily and monthly active users (DAU, MAU) and their ratio (stickiness), retention cohorts (day 1, day 7, day 30 retention by install date), session length and frequency, conversion to paying, ARPDAU (average revenue per daily active user) and lifetime value (LTV) predictions. Retention cohorts are the backbone; they need correct install dates per player across devices.

**Live operations.** Modern games run events, offers and balance changes weekly. Data supports this with near-real-time dashboards during events, A/B tests of offers and difficulty, and remote configuration whose every change is logged so analysis can attribute effects.

**Matchmaking data.** Skill ratings (Elo, Glicko, TrueSkill-style systems) are updated after each match; matchmaking quality is measured by wait times, rating spread within matches and match outcomes. Rating histories are needed to detect smurfing (experienced players on new accounts).

**Privacy and minors.** Many players are children, so laws such as COPPA in the US and the age provisions of GDPR apply; collect less, restrict profiling and advertising, and honour parental controls.

> **Interview:** "Design a global leaderboard for a game with 50 million players." Scores are validated on game servers and written to a durable store and an event stream; sorted sets in Memorystore (sharded by region or season, with a global board built from regional tops) serve top-N and rank queries; a player's approximate percentile can be served from score histograms when exact rank is too expensive; boards are rebuilt from the durable store after failure; anomalies in score submissions feed anti-cheat.
:::

::: check
?? Which data structure is typically used for real-time leaderboards, and which operations does it make fast?
A sorted set (Redis or Valkey, for example in Memorystore). It makes adding or updating a score, reading the top N and finding a player's rank fast (logarithmic time).

?? What are sources and sinks in a game economy, and what happens when sources greatly outgrow sinks?
Sources create currency or items (rewards, purchases); sinks remove them (spending, crafting, fees). If sources outgrow sinks, currency accumulates and in-game prices inflate.

?? Why must game state be validated on the server?
Anything computed on the client can be forged by cheaters; only server-side computation or validation can be trusted.
:::

::: resources
- Docs: [Redis sorted sets](https://redis.io/docs/latest/develop/data-types/sorted-sets/) · the data structure behind most leaderboards, with commands and complexity
- Docs: [Memorystore for Valkey](https://docs.cloud.google.com/memorystore/docs/valkey) · managed Redis-compatible in-memory store on Google Cloud
- Docs: [Export Firebase data to BigQuery](https://firebase.google.com/docs/projects/bigquery-export) · mobile game analytics events in BigQuery
- Docs: [Agones](https://agones.dev/) · open-source dedicated game servers on Kubernetes
- Docs: [Open Match](https://open-match.dev/) · open-source matchmaking framework
- Book: *Game Analytics: Maximizing the Value of Player Data* edited by Magy Seif El-Nasr, Anders Drachen and Alessandro Canossa · telemetry, metrics and analysis for games
:::

### AdTech Data Engineering

::: plain
When a web page loads, the empty ad slot is often sold in a lightning-fast auction: the page's ad system asks many buyers "what will you pay to show an ad to this visitor?", each buyer decides in a fraction of a second, and the winner's ad appears. This happens billions of times a day. AdTech data engineering handles the firehose of auctions, impressions and clicks, works out which ads led to purchases, builds the audiences advertisers target, and is now rebuilding much of this as browsers and laws restrict tracking.
:::

**The players.** **Publishers** sell ad space; **supply-side platforms (SSPs)** and ad exchanges run auctions for them; **demand-side platforms (DSPs)** bid on behalf of advertisers; **ad servers** decide and record what was shown; measurement and verification vendors check viewability and fraud. Large companies (Google, Meta, Amazon) run several of these roles in one stack.

**Real-time bidding (RTB).** The **OpenRTB** protocol (IAB Tech Lab; version 2.6 is the widely used one) defines the messages:

1. The page loads and the SSP builds a **bid request**: an ID, the impression (slot size, position), the site or app, the device (type, OS, approximate geography), the user (IDs, if available and consented), consent strings, and `tmax`, the maximum time to respond in milliseconds (commonly around 100 to 200).
2. DSPs evaluate it against campaigns, budgets, frequency caps and predicted click or conversion rates, and respond with a **bid** (price, creative, notice URLs) or no bid.
3. The exchange runs the auction (first-price auctions are now the norm), notifies the winner, and the ad renders.
4. **Impression**, **viewability** and **click** events are recorded by pixels and redirects.

```json
{ "id": "a1b2c3", "tmax": 120,
  "imp": [{ "id": "1", "banner": { "w": 300, "h": 250 }, "bidfloor": 0.40, "bidfloorcur": "USD" }],
  "site": { "domain": "news.example", "page": "https://news.example/sport/123" },
  "device": { "devicetype": 2, "os": "Android", "geo": { "country": "IND" } },
  "user": { "id": "exch-uid-9f2" },
  "regs": { "gdpr": 0 } }
```

[[fig:industry_rtb]]

**The data engineering problems.**

- **Volume**: bid requests arrive at millions per second for a large DSP. Storing all of them is often too expensive, so DSPs keep full logs for won bids and samples of the rest, plus aggregates.
- **Latency**: the bidder reads user profiles, budgets and frequency counts within a few milliseconds, so they live in a low-latency key-value store (Bigtable, Memorystore, Aerospike) fed by streaming pipelines.
- **Joining the funnel**: bid, win, impression, viewability, click and conversion events arrive from different systems at different times. They are joined by auction and impression IDs with time windows (a click must follow its impression within, say, an hour; a conversion within a 7 or 30 day window), and deduplicated.
- **Invalid traffic (IVT)**: bots and fraud generate impressions and clicks. Filtering uses known bot lists, behavioural rules and models, and industry guidelines (the Media Rating Council's IVT standards) before anything is billed.
- **Budget pacing**: spend must be tracked in near real time so campaigns do not overspend; a streaming aggregation of spend per campaign feeds the bidder.

**Attribution.** Which ads deserve credit for a purchase? **Rule-based** models assign credit by position or time; **data-driven** models estimate each touchpoint's contribution statistically (Shapley values, Markov chains).

```python
from datetime import datetime

# touchpoints for one user before a 100.00 purchase at 2026-11-20 12:00
path = [("search_ad", "2026-11-12 09:00"), ("display", "2026-11-17 18:00"),
        ("email", "2026-11-19 08:00"), ("search_ad", "2026-11-20 11:30")]
value, conversion = 100.0, datetime(2026, 11, 20, 12, 0)

def credit(weights):
    total = sum(weights)
    out = {}
    for (channel, _), w in zip(path, weights):
        out[channel] = round(out.get(channel, 0) + value * w / total, 2)
    return out

n = len(path)
age_days = [(conversion - datetime.fromisoformat(t)).total_seconds() / 86400 for _, t in path]
print("last touch :", credit([0] * (n - 1) + [1]))
print("first touch:", credit([1] + [0] * (n - 1)))
print("linear     :", credit([1] * n))
print("time decay :", credit([0.5 ** (d / 7) for d in age_days]))   # half-life of 7 days
# last touch : {'search_ad': 100.0, 'display': 0.0, 'email': 0.0}
# first touch: {'search_ad': 100.0, 'display': 0.0, 'email': 0.0}
# linear     : {'search_ad': 50.0, 'display': 25.0, 'email': 25.0}
# time decay : {'search_ad': 46.65, 'display': 24.59, 'email': 28.76}
```

Attribution measures correlation, not causation: the search ad may get credit for a purchase that would have happened anyway. **Incrementality** experiments (holdout groups that see no ads, or geographic experiments) and **marketing mix modeling** (regression of sales on spend per channel over time; Google's open-source **Meridian** is one implementation) estimate causal effects and are growing as user-level tracking shrinks.

**Audience segmentation.** Audiences are sets of users defined by rules ("visited a product page in the last 14 days, did not buy") or models (lookalikes, predicted purchase propensity). They are computed in the warehouse and activated by syncing hashed identifiers to ad platforms (reverse ETL; for example, customer match lists use SHA-256 hashed emails), with consent checked first (Data governance, security and compliance).

**The cookieless transition.** Third-party cookies let ad tech recognise a browser across sites. Safari and Firefox block them by default. Google decided in 2024 not to phase them out of Chrome and, in October 2025, announced the retirement of most Privacy Sandbox APIs (such as Topics, Protected Audience and Attribution Reporting) after low adoption. Mobile platforms restrict device advertising IDs (Apple's App Tracking Transparency). Regulations require consent for tracking in many countries. In response, the industry has moved towards:

- **First-party data**: logged-in users and CRM data, collected with consent.
- **Server-side tagging**: tags run on a server the company controls (Google Tag Manager server containers can run on Cloud Run), improving data control and consent enforcement.
- **Data clean rooms**: advertisers and publishers match first-party data under privacy rules (Ads Data Hub for Google ads data, BigQuery data clean rooms, others).
- **Contextual targeting**: ads chosen by page content rather than user history.
- **Modelled measurement**: conversion modelling, MMM and incrementality instead of complete user-level paths.

> **Pitfall:** Joining clicks to impressions without deduplication and time windows. Double-fired pixels and clicks matched to impressions from days earlier inflate click-through rates and wrongly bill advertisers. Deduplicate on event IDs and apply explicit attribution windows.

::: deeper
**Lambda-style pipelines are common.** AdTech often runs a streaming path (budgets, pacing, near-real-time reporting) and a batch path (billing-grade reconciliation after IVT filtering and late events). Billing numbers come from batch, and the two are reconciled; the differences are expected and reported.

**Approximate counting.** Reach and frequency ("how many unique users saw this campaign, and how often?") over billions of impressions use HyperLogLog sketches that can be merged across campaigns, days and publishers, a standard technique in ad reporting.

**Identity and privacy.** Identity graphs link cookies, device IDs, hashed emails and IP-based signals. They are personal data under GDPR and CCPA, must respect consent and opt-outs, and carry significant regulatory risk. Industry identifiers such as UID2 are built on hashed, consented emails.

**Latency budgets.** With roughly 100 milliseconds end to end, including network round trips, a DSP's bidder typically has only a few tens of milliseconds of compute. Feature lookups must be single-digit milliseconds, models must be small or precomputed, and anything slow (complex joins, warehouse queries) happens offline and is pushed to the serving store.

> **Interview:** "Design the impression and click tracking pipeline for an ad network." Pixels and redirects hit stateless collectors behind a global load balancer; events with unique IDs go to Pub/Sub; a streaming job deduplicates, filters known invalid traffic, enriches with campaign data and updates spend counters in a low-latency store for pacing; events land in partitioned BigQuery tables; a batch job joins impressions, clicks and conversions with attribution windows, applies full IVT filtering, and produces billing-grade reports reconciled against the streaming numbers; reach uses HLL sketches; consent and retention rules apply throughout.
:::

::: check
?? What is `tmax` in an OpenRTB bid request, and why does it shape the data architecture?
The maximum time in milliseconds the bidder has to respond. Because it is so short, every lookup the bidder makes (profiles, budgets, frequency counts) must come from low-latency stores fed by pipelines, not from a warehouse.

?? Why is last-touch attribution misleading?
It gives all credit to the final touchpoint, ignoring earlier ads that may have created the demand, and like all attribution it measures correlation rather than the ad's causal effect.

?? Name three approaches the industry uses as third-party cookies and device IDs become less available.
Any three of: first-party data with consent, server-side tagging, data clean rooms, contextual targeting, modelled measurement such as marketing mix modeling and incrementality experiments.
:::

::: resources
- Spec: [OpenRTB](https://iabtechlab.com/standards/openrtb/) · IAB Tech Lab's real-time bidding protocol specifications
- Docs: [Ads Data Hub](https://developers.google.com/ads-data-hub) · Google's privacy-centric clean room for advertising measurement on BigQuery
- Docs: [Server-side tagging](https://developers.google.com/tag-platform/tag-manager/server-side) · running tags on a server you control
- Repo: [Meridian](https://github.com/google/meridian) · Google's open-source marketing mix modeling framework
- Article: [Google officially shuts down Privacy Sandbox](https://searchengineland.com/google-officially-shuts-down-privacy-sandbox-463561) · what was retired in 2025 and what remains
- Book: [Trustworthy Online Controlled Experiments](https://experimentguide.com/) · experiments and holdouts, the basis of incrementality measurement
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Money | `NUMERIC` or `BIGNUMERIC` in BigQuery, `Decimal` in Python, never floats |
| Time | Store UTC plus the source's local zone; keep event, receive and processing times |
| Corrections over time | Bitemporal tables: valid time and transaction time, never update in place |
| Risk data rules (banks) | BCBS 239: accurate, complete, timely, with lineage and ownership |
| HL7 v2 | Segments by carriage return, fields `\|`, components `^`; MLLP over TCP |
| FHIR | JSON resources over REST; R4 most deployed; SMART on FHIR for OAuth scopes |
| Health data on Google Cloud | Cloud Healthcare API FHIR, HL7v2, DICOM stores; stream FHIR to BigQuery |
| Sessions | New session after 30 minutes of inactivity: `LAG` plus a running sum |
| A/B test health | Check sample ratio mismatch first (chi-square on assignment counts) |
| Inventory | Model as a movement log; reconcile storefront, warehouse and counts |
| Geography in BigQuery | `ST_GEOGPOINT(lng, lat)`, `ST_DWITHIN`, `S2_CELLIDFROMPOINT` |
| City grids | H3 hexagons (six equal neighbours) or S2 cells; smooth over neighbours |
| Playback quality | Startup time, rebuffer ratio, bitrate, failures by device, CDN, region |
| Point-in-time features | Compute features only from data before the snapshot or impression time |
| Industrial data path | OPC UA at the edge, Sparkplug B over MQTT, Pub/Sub, Bigtable and BigQuery |
| Irregular time series | `TIMESTAMP_BUCKET` then `GAP_FILL` in BigQuery |
| Leaderboards | Sorted sets in Memorystore: `ZADD`, `ZREVRANGE ... WITHSCORES`, `ZREVRANK` |
| Game economy | Track source to sink ratio; spikes usually mean exploits |
| Ad auctions | OpenRTB bid requests with `tmax` around 100 ms; lookups from low-latency stores |
| Attribution | Rules or data-driven credit; measure causality with holdouts and MMM |

### Practice

::: try
**1. Bitemporal correction.** A trade booked on 3 March with quantity 100 is corrected on 5 March to quantity 120. Show the rows of a bitemporal `trades` table (valid and recorded ranges) after the correction, and say what quantity each query returns: "as of 4 March, what did we know?" and "as of today, what is true?".
:::

?? A solution to 1
Two rows. Row 1: quantity 100, valid from 3 March onwards, recorded from 3 March to 5 March. Row 2: quantity 120, valid from 3 March onwards, recorded from 5 March onwards (open-ended). "As of 4 March, what did we know?" selects the row whose recorded range contains 4 March: 100. "As of today, what is true?" selects the row whose recorded range contains today: 120. The original row is never updated in place except to close its recorded range.

::: try
**2. HL7 to FHIR.** An `ADT^A08` message updates a patient's surname. Describe what your pipeline should do in the FHIR store and in BigQuery, and what could go wrong if messages arrive out of order.
:::

?? A solution to 2
Parse the message, find the patient by the source system and MRN identifier (through the master patient index), and update the `Patient` resource's `name` (a new version in the FHIR store, which keeps history). The FHIR store's streaming export writes the new version to BigQuery; analytics use the latest version per resource. If messages arrive out of order, an older update could overwrite a newer one, so compare the message's event timestamp (`EVN` or `MSH-7`) with the last applied one and skip or park stale updates, or process messages per patient in order (for example, a Pub/Sub ordering key on patient ID).

::: try
**3. Strict funnel.** Using the `events` table from the e-commerce topic, write SQL (SQLite or BigQuery) that counts users who did `view_item`, then `add_to_cart`, then `purchase` in that order.
:::

?? A solution to 3

```sql
WITH firsts AS (
  SELECT user_id,
         MIN(CASE WHEN event = 'view_item'   THEN ts END) AS t_view,
         MIN(CASE WHEN event = 'add_to_cart' THEN ts END) AS t_cart,
         MIN(CASE WHEN event = 'purchase'    THEN ts END) AS t_buy
  FROM events
  GROUP BY user_id
)
SELECT COUNT(*)                                                    AS viewed,
       COUNT(CASE WHEN t_cart > t_view THEN 1 END)                 AS carted_after_view,
       COUNT(CASE WHEN t_cart > t_view AND t_buy > t_cart THEN 1 END) AS bought_after_cart
FROM firsts
WHERE t_view IS NOT NULL;
-- with the sample data: 3, 2, 1
```

A production version would do this within sessions and handle users who repeat steps.

::: try
**4. Sensor row keys.** A factory has 2,000 machines with 50 sensors each, reporting once per second. Propose a Bigtable row key and say how many rows per day your design creates and how a dashboard reads "the last 6 hours of sensor X on machine Y".
:::

?? A solution to 4
Key `plant#machine#sensor#yyyymmddhh`, with one row per sensor per hour and each second's reading as a cell (or a column per second). That is 2,000 × 50 × 24 = 2.4 million rows per day, each holding up to 3,600 readings. The dashboard reads a range scan from `plant#Y#X#<hour 6 hours ago>` to `plant#Y#X#<current hour>`: at most 7 rows. Starting the key with the machine and sensor spreads writes across the key space; putting the timestamp first would send all current writes to one node.

::: try
**5. Industry choice.** Pick one industry from this section. List its four dials (latency, correctness, regulation, data shape), the two Google Cloud services you would rely on most, and the single data engineering skill you would learn next to work there.
:::

?? Answer to 5
There is no single right answer. Example for AdTech: latency extreme (bids in about 100 ms), correctness moderate for bidding but billing-grade for invoices, regulation heavy (GDPR, CCPA, consent strings), data shape is huge append-only event logs with joins across funnel stages. Services: Bigtable for low-latency profile and budget lookups, and BigQuery for logs, attribution and billing (with Pub/Sub and Dataflow between them). Next skill: stateful stream processing with event-time windows and deduplication.

### What comes next

This is the end of the course. You have travelled from the first ideas of data engineering through SQL, modeling, storage, engines, streaming, pipelines, Google Cloud, system design, operations, governance, AI, graphs and the industries that use them all. The best way to make it stick is to **build**: pick one industry from this section and build a small end-to-end project on Google Cloud (for example, a clickstream pipeline from a synthetic event generator through Pub/Sub and Dataflow into BigQuery, with dbt or Dataform models, data quality checks, lineage, a dashboard in Looker or Data Studio, and Terraform for everything), publish the code and a short write-up, and explain your design choices as you would in an interview. Use the free tier, the BigQuery sandbox and public datasets to keep costs near zero. Then prepare for the **Google Cloud Professional Data Engineer** certification, which tests exactly the design trade-offs this course covers, using Google's exam guide and the hands-on labs on [Google Skills](https://www.skills.google/). Keep learning from practitioners: read engineering blogs from the companies in this section, follow the [Google Cloud Architecture Center](https://docs.cloud.google.com/architecture) for reference designs, join communities such as [DataTalks.Club](https://datatalks.club/) (and its free Data Engineering Zoomcamp) and open-source project communities (Apache Airflow, Beam, Iceberg, dbt), and contribute a fix or a document to one of them. Revisit the companion guides when you need depth: the Python course for the language, and the PySpark, Streaming and Kafka guide for distributed processing internals.
