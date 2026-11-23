/* modeling: Data modeling, original drawings */
Object.assign(FIGS, {

modeling_levels:{ t:"Conceptual, logical and physical models", cap:"The same idea at three levels of detail. The conceptual model uses business words only, the logical model adds keys and attributes in normalised form, and the physical model implements it for each engine with its own types, indexes and layout.", svg:S("modeling1", 250, "Three columns: conceptual entities, logical tables with keys, physical tables for PostgreSQL and BigQuery",
  T(125, 24, "Conceptual", "b", "middle") + T(380, 24, "Logical", "b", "middle") + T(635, 24, "Physical", "b", "middle") +
  B(20, 40, 210, 60, "ka", ["Customer", "a person with an account"]) +
  B(20, 130, 210, 60, "ka", ["Order", "a request to buy products"]) +
  A("modeling1", "M125 100 L125 128") + T(135, 118, "places (1 to many)", "sm") +
  B(275, 40, 210, 60, "sp", ["customers", "customer_id PK, email UNIQUE"]) +
  B(275, 130, 210, 60, "sp", ["orders", "order_id PK, customer_id FK"]) +
  A("modeling1", "M380 100 L380 128") +
  B(530, 40, 210, 60, "st", ["PostgreSQL (OLTP)", "BIGINT keys, B-tree indexes"]) +
  B(530, 130, 210, 60, "st", ["BigQuery (analytics)", "PARTITION BY, CLUSTER BY"]) +
  A("modeling1", "M230 115 L273 115") + A("modeling1", "M485 115 L528 115") +
  T(125, 222, "business words, no columns", "sm", "middle") + T(380, 222, "keys, attributes, 3NF", "sm", "middle") + T(635, 222, "types, indexes, layout per engine", "sm", "middle")) },

modeling_star:{ t:"A star schema, with one snowflaked branch", cap:"The fact table in the middle holds keys and measures at a declared grain; each dimension joins to it with one key. Normalising product into a separate category table (dashed) turns that branch into a snowflake and adds a join to every query that groups by category.", svg:S("modeling2", 330, "Fact table fct_order_lines in the centre joined to date, customer, product and store dimensions; product further joined to a category table",
  B(270, 115, 220, 90, "ka", ["fct_order_lines", "grain: one order line", "keys + quantity, net_amount"]) +
  B(30, 20, 190, 60, "sp", ["dim_date", "day, month, fiscal quarter"]) +
  B(540, 20, 190, 60, "sp", ["dim_customer", "segment, city, country"]) +
  B(30, 230, 190, 60, "sp", ["dim_product", "sku, name, brand, category"]) +
  B(540, 230, 190, 60, "sp", ["dim_store", "store, region, channel"]) +
  A("modeling2", "M220 60 L268 130") + A("modeling2", "M540 60 L492 130") +
  A("modeling2", "M220 255 L268 190") + A("modeling2", "M540 255 L492 190") +
  B(285, 255, 180, 50, "box", ["dim_category", "snowflaked branch"]) +
  A("modeling2", "M220 280 L283 280", "ln dash") +
  T(380, 320, "Star: one join per dimension. Snowflake: extra joins for each normalised level.", "sm", "middle")) },

modeling_scd2:{ t:"SCD type 2: one row per version", cap:"Customer 42 moved from Pune to Mumbai on 1 June. Type 2 closes the old row and inserts a new one with a new surrogate key. An order in March was stored with surrogate key 1, an order in September with key 3, so each joins to the city that was true when it happened.", svg:S("modeling3", 270, "Timeline with two versions of a customer and two orders pointing to the version valid at their date",
  T(20, 26, "dim_customer rows for customer_id 42", "b") +
  B(60, 45, 300, 50, "box", ["sk 1 · Pune", "valid 2026-01-01 to 2026-06-01, not current"]) +
  B(360, 45, 360, 50, "okf", ["sk 3 · Mumbai", "valid 2026-06-01 to 9999-12-31, is_current true"]) +
  A("modeling3", "M40 200 L740 200") +
  T(60, 222, "Jan", "sm", "middle") + T(360, 222, "Jun", "sm", "middle") + T(700, 222, "now", "sm", "middle") +
  B(140, 130, 150, 40, "ka", ["order 15 Mar"]) + A("modeling3", "M215 130 L215 97") + T(225, 118, "customer_sk = 1", "sm tA") +
  B(480, 130, 150, 40, "ka", ["order 20 Sep"]) + A("modeling3", "M555 130 L555 97") + T(565, 118, "customer_sk = 3", "sm tA") +
  T(380, 255, "Lookup by date: valid_from <= t AND t < valid_to (half-open, so versions never overlap)", "sm", "middle")) },

modeling_vault:{ t:"Data Vault: hubs, links and satellites", cap:"Hubs hold business keys, the link records which customer placed which order, and satellites hang off hubs (and links) holding descriptive attributes with full, insert-only history. A point-in-time table in the business vault makes marts easy to build.", svg:S("modeling4", 300, "Two hubs joined by a link, each hub with a satellite, and a PIT table feeding a mart",
  B(30, 120, 180, 60, "ka", ["hub_customer", "hk_customer, customer_bk"]) +
  B(290, 120, 180, 60, "pr", ["link_customer_order", "hk_customer, hk_order"]) +
  B(550, 120, 180, 60, "ka", ["hub_order", "hk_order, order_bk"]) +
  A("modeling4", "M290 150 L212 150") + A("modeling4", "M470 150 L548 150") +
  B(30, 20, 180, 60, "sp", ["sat_customer", "name, city, hash_diff"]) +
  B(550, 20, 180, 60, "sp", ["sat_order", "status, amount, hash_diff"]) +
  A("modeling4", "M120 80 L120 118", "ln dash") + A("modeling4", "M640 80 L640 118", "ln dash") +
  B(30, 220, 180, 50, "st", ["pit_customer", "valid sat row per date"]) +
  A("modeling4", "M120 180 L120 218", "ln dash") +
  B(290, 220, 180, 50, "okf", ["dim_customer (mart)", "built from hub + PIT"]) +
  A("modeling4", "M210 245 L288 245") +
  T(500, 245, "raw vault: no business rules, insert only", "sm") + T(500, 263, "every row: load_ts, record_source", "sm")) },

modeling_bitemporal:{ t:"Two time axes: valid time and recorded time", cap:"On 1 January the database recorded a price of 10 from January onwards. On 10 March it learned that the price had been 12 since 1 March. The old belief is kept with a closed recorded interval, so a question about 3 March gives 10 as known on 5 March and 12 as known now.", svg:S("modeling5", 290, "Two rows of bars over a valid-time axis, one per recorded belief, with a vertical marker at 3 March",
  T(20, 24, "Valid time (when the price applies) runs left to right", "b") +
  T(20, 70, "recorded 1 Jan to 10 Mar", "sm") +
  B(200, 50, 520, 36, "box", ["price 10, valid 1 Jan onwards"], false) +
  T(20, 140, "recorded 10 Mar onwards", "sm") +
  B(200, 120, 160, 36, "okf", ["10, Jan to Mar"], false) +
  B(360, 120, 360, 36, "okf", ["12, valid 1 Mar onwards"], false) +
  A("modeling5", "M40 200 L740 200") +
  T(200, 222, "1 Jan", "sm", "middle") + T(360, 222, "1 Mar", "sm", "middle") +
  A("modeling5", "M385 40 L385 196", "acc dash") + T(392, 40, "3 Mar", "sm tA") +
  T(20, 255, "Known on 5 Mar: read the top row, price 10. Known now: read the bottom row, price 12.", "sm") +
  T(20, 275, "A correction never edits a row; it closes the recorded interval and adds new rows.", "sm")) },

});
