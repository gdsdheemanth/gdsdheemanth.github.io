/* Industry data engineering: section diagrams */
Object.assign(FIGS, {

industry_finance:{ t:"A trading data platform", cap:"Fills and market data are normalised (UTC timestamps, decimal money, one instrument ID) and kept unchanged in an immutable raw store. A stateful stream keeps intraday positions, end-of-day reconciliation compares them with the books of record and custodian statements, and bitemporal tables let regulatory reports be reproduced exactly as they were sent.", svg:S("industry1", 325, "Venue data normalised into a raw store and streaming positions, reconciled at end of day, and stored bitemporally for regulatory reporting",
  B(20, 30, 130, 64, "ka", ["Venues", "ticks and fills"]) +
  B(180, 30, 160, 64, "sp", ["Normalise", "UTC, NUMERIC,", "one instrument ID"]) +
  B(370, 30, 180, 64, "st", ["Streaming positions", "keyed by account +", "instrument, exactly once"]) +
  B(580, 30, 160, 64, "okf", ["Trading desk", "positions in ms"]) +
  A("industry1", "M150 62 L178 62") + A("industry1", "M340 62 L368 62") + A("industry1", "M550 62 L578 62") +
  B(20, 130, 130, 64, "box", ["Raw store", "immutable: every", "message as received"]) +
  B(180, 130, 160, 64, "ka", ["Custodians", "and brokers", "statements"]) +
  B(370, 130, 180, 64, "pr", ["End-of-day reconcile", "vs books of record", "and custodians"]) +
  B(580, 130, 160, 64, "box", ["Breaks", "exceptions people", "resolve"]) +
  A("industry1", "M200 94 L110 128", "ln dash") + A("industry1", "M460 94 L460 128") + A("industry1", "M550 162 L578 162") +
  A("industry1", "M340 162 L368 162") +
  B(180, 230, 160, 64, "st", ["Bitemporal tables", "valid time +", "recorded time"]) +
  B(370, 230, 180, 64, "okf", ["Regulatory reports", "MiFID II (next day)", "BCBS 239 lineage"]) +
  B(580, 230, 160, 64, "box", ["Audit questions", "reproduce a report", "years later"]) +
  A("industry1", "M85 194 L85 262 L178 262") + A("industry1", "M340 262 L368 262") + A("industry1", "M550 262 L578 262") +
  T(380, 316, "Money stays NUMERIC from ingestion onwards; every timestamp is stored in UTC.", "sm", "middle")) },

industry_health:{ t:"A clinical data platform on Google Cloud", cap:"HL7 v2 messages from hospital systems arrive through the MLLP adapter into an HL7v2 store, and a Pub/Sub notification triggers a mapper that writes FHIR resources. The FHIR store serves apps through SMART on FHIR, streams changes to BigQuery for analytics, and feeds de-identified copies for research.", svg:S("industry2", 320, "Hospital systems to MLLP adapter, HL7v2 store, Pub/Sub and a mapper into a FHIR store, which serves apps, streams to BigQuery and produces de-identified research data",
  B(20, 30, 130, 64, "ka", ["Hospital systems", "EHR, lab, ADT"]) +
  B(180, 30, 120, 64, "sp", ["MLLP adapter", "HL7 v2 over TCP"]) +
  B(330, 30, 120, 64, "box", ["HL7v2 store", "raw messages"]) +
  B(480, 30, 110, 64, "st", ["Pub/Sub", "new message"]) +
  B(620, 30, 120, 64, "sp", ["Mapper", "HL7 v2 to FHIR"]) +
  A("industry2", "M150 62 L178 62") + A("industry2", "M300 62 L328 62") + A("industry2", "M450 62 L478 62") + A("industry2", "M590 62 L618 62") +
  B(20, 140, 150, 64, "pr", ["PHI controls", "BAA-covered services", "audited access"]) +
  B(330, 140, 190, 64, "box", ["Apps via REST", "SMART on FHIR tokens", "patient/Observation.read"]) +
  B(560, 140, 180, 64, "okf", ["FHIR store", "operational source", "R4 resources"]) +
  A("industry2", "M680 94 L680 138") + A("industry2", "M560 172 L522 172") +
  B(100, 245, 190, 64, "box", ["Research datasets", "no direct identifiers"]) +
  B(330, 245, 190, 64, "pr", ["De-identify", "pseudonyms, shifted dates", "generalised ZIP codes"]) +
  B(560, 245, 180, 64, "st", ["BigQuery", "streamed FHIR changes", "for analytics"]) +
  A("industry2", "M650 204 L650 243") + A("industry2", "M575 204 L500 243") + A("industry2", "M330 277 L292 277")) },

industry_mobility:{ t:"Surge pricing and the trip lakehouse", cap:"Driver pings and rider requests stream into a job that maps each point to a cell and counts supply and demand in short sliding windows. A pricing service smooths and caps the result, writes multipliers to a low-latency store the app reads, and logs every price with its inputs. Finished trips land in a lakehouse table partitioned by city and local date that accepts upserts.", svg:S("industry3", 315, "Pings and requests through Pub/Sub into a streaming cell aggregation, a pricing service and a low-latency store; completed trips into a lakehouse table for analytics",
  B(20, 30, 140, 70, "ka", ["Driver pings", "position every", "few seconds"]) +
  B(20, 120, 140, 70, "ka", ["Rider requests", "pickup + drop-off"]) +
  B(190, 75, 120, 70, "st", ["Pub/Sub", "or Kafka"]) +
  A("industry3", "M160 65 L188 95") + A("industry3", "M160 155 L188 125") +
  B(340, 40, 170, 140, "sp", ["Streaming job", "Dataflow or Flink", "point to cell (S2, H3)", "supply, demand per cell", "2 min window, every 30 s"]) +
  A("industry3", "M310 110 L338 110") +
  B(540, 40, 200, 70, "pr", ["Pricing service", "smoothing, caps, rules", "logs each price + inputs"]) +
  B(540, 130, 200, 50, "okf", ["Memorystore / Bigtable", "the app reads multipliers"]) +
  A("industry3", "M510 75 L538 75") + A("industry3", "M640 110 L640 128") +
  T(20, 225, "After the trip", "b") +
  B(20, 240, 140, 60, "ka", ["Completed trips", "fares, ratings"]) +
  B(190, 240, 250, 60, "sp", ["Trip lakehouse table", "partition: city + local date", "upserts for fares, disputes"]) +
  B(470, 240, 270, 60, "okf", ["Analytics and ETA features", "trip history, map-matched", "speeds per road segment"]) +
  A("industry3", "M160 270 L188 270") + A("industry3", "M440 270 L468 270")) },

industry_iot:{ t:"From the factory floor to the cloud", cap:"Sensor and controller data (ISA-95 levels 0 to 2) and production context from MES and historians (level 3) leave the isolated plant network through an edge gateway, which buffers when the link drops. It publishes over MQTT with Sparkplug B, the broker is bridged into Pub/Sub, and the data lands in Bigtable for recent readings, BigQuery for history and Cloud Storage for raw files.", svg:S("industry4", 330, "ISA-95 levels in the plant, an edge gateway, an MQTT broker, Pub/Sub and three storage targets on Google Cloud",
  '<rect class="fill" x="20" y="20" width="280" height="290" rx="12"/>' + T(160, 42, "Plant and business systems (ISA-95)", "b", "middle") +
  B(35, 60, 250, 48, "box", ["Level 4: ERP", "orders, materials, costs"]) +
  B(35, 120, 250, 48, "box", ["Level 3: MES, historian", "batches, work orders"]) +
  B(35, 180, 250, 48, "box", ["Level 2: PLCs, SCADA", "tags and alarms"]) +
  B(35, 240, 250, 48, "box", ["Level 0-1: sensors", "raw signals, milliseconds"]) +
  B(330, 150, 150, 80, "ka", ["Edge gateway", "reads OPC UA tags", "buffers offline"]) +
  T(405, 248, "in the DMZ", "sm", "middle") +
  A("industry4", "M285 204 L328 204") + A("industry4", "M285 144 L328 170") +
  B(330, 30, 150, 70, "st", ["MQTT broker", "Sparkplug B topics", "birth, death certs"]) +
  A("industry4", "M405 150 L405 102") + T(415, 130, "MQTT", "sm") +
  B(510, 30, 230, 50, "st", ["Pub/Sub", "bridged from the MQTT broker"]) +
  A("industry4", "M480 55 L508 55") +
  B(540, 110, 200, 48, "okf", ["Bigtable", "recent, high resolution"]) +
  B(540, 170, 200, 48, "okf", ["BigQuery", "full history for analysis"]) +
  B(540, 230, 200, 48, "okf", ["Cloud Storage", "raw files, archive"]) +
  A("industry4", "M525 80 L525 134 L538 134") + A("industry4", "M525 80 L525 194 L538 194") + A("industry4", "M525 80 L525 254 L538 254") +
  T(380, 324, "Most value comes from joining level 2 sensor data with level 3 production context.", "sm", "middle")) },

industry_rtb:{ t:"Real-time bidding and its data", cap:"Within the auction's tmax, the exchange sends a bid request to DSP bidders, which read profiles, budgets and frequency counts from a key-value store and answer with a bid or no bid. After the ad renders, impression, viewability, click and conversion events are joined by auction and impression ID, filtered for invalid traffic, and fed back as spend and counters to the bidder.", svg:S("industry5", 330, "Web page, exchange, DSP bidders and key-value store inside the auction time limit; event logs, streaming joins, BigQuery and spend pacing feeding back",
  '<rect class="fill" x="20" y="20" width="720" height="130" rx="12"/>' + T(380, 38, "Inside tmax: about 100 to 200 ms per auction", "b", "middle") +
  B(35, 55, 110, 70, "box", ["Web page", "ad slot loads"]) +
  B(175, 55, 140, 70, "ka", ["SSP / exchange", "builds bid request", "runs the auction"]) +
  B(375, 50, 150, 85, "sp", ["DSP bidders", "campaigns, budget,", "frequency caps,", "predicted CTR"]) +
  B(575, 55, 150, 70, "st", ["Key-value store", "Bigtable, Memorystore", "profiles, counters"]) +
  A("industry5", "M145 90 L173 90") +
  A("industry5", "M315 76 L373 76") + T(345, 70, "request", "sm", "middle") +
  A("industry5", "M373 108 L317 108", "ln dash") + T(345, 122, "bid", "sm", "middle") +
  A("industry5", "M573 90 L527 90") +
  B(35, 185, 150, 60, "box", ["Ad renders", "impression, view,", "click, conversion"]) +
  A("industry5", "M245 125 L115 183") +
  B(215, 185, 150, 60, "st", ["Event logs", "Pub/Sub"]) +
  B(395, 175, 170, 80, "sp", ["Streaming jobs", "join by auction and", "impression ID, drop", "invalid traffic"]) +
  B(595, 175, 130, 36, "okf", ["BigQuery"]) + B(595, 219, 130, 36, "pr", ["Spend pacing"]) +
  A("industry5", "M185 215 L213 215") + A("industry5", "M365 215 L393 215") +
  A("industry5", "M565 200 L593 193") + A("industry5", "M565 230 L593 237") +
  A("industry5", "M725 237 L745 237 L745 105 L727 105", "acc dash") +
  T(380, 290, "Streaming pipelines feed budgets and counters back to the bidder's key-value store.", "sm", "middle") +
  T(380, 310, "Full logs are kept for won bids; the rest is sampled and aggregated.", "sm", "middle")) },

});
