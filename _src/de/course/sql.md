This section teaches SQL from the very first query to the patterns real pipelines are built from. It assumes only what Data engineering foundations gave you: you know what a table, a pipeline and a warehouse are, and you can run a command in a terminal. If you have never written SQL, start with the first topic and type every example yourself; nothing else is needed. If you already write SQL every day, skim the first group and slow down from Window functions in depth onwards, where the material most analysts never learn begins.

SQL (Structured Query Language, pronounced "ess-cue-ell" or "sequel") is the language you use to ask a database questions and to change what it stores. It is more than fifty years old and still the most important skill in data engineering, because every serious engine speaks it: PostgreSQL, BigQuery, Spark, Trino, Flink, DuckDB, Snowflake and ClickHouse all accept SQL with small differences in spelling. Learn it well once and you can use all of them.

**What you will be able to do afterwards**

- Write queries that filter, sort, join, group and aggregate data, and predict exactly which rows they return, including what happens with `NULL`.
- Use window functions for running totals, rankings, deduplication, period-over-period changes and gaps-and-islands problems.
- Break a hard question into readable steps with CTEs, use subqueries correctly, and walk hierarchies with recursive queries.
- Handle dates, timestamps and time zones without the classic off-by-one-day bugs.
- Query nested and JSON data in BigQuery with `UNNEST` and the JSON functions.
- Apply the SQL patterns behind warehouse models: subtotals, pivots, snapshots, bridge tables, point-in-time joins and idempotent `MERGE` loads.
- Generate SQL with Jinja in dbt and Airflow without making it unreadable.
- Read an execution plan, explain why a query is slow, and spot the anti-patterns that make SQL wrong or dangerous.

**The big picture.** SQL is **declarative**: you describe the result you want ("total paid revenue per customer"), not the steps to compute it. A part of the database called the **optimizer** turns your description into a plan (which table to read first, which index to use, how to join), and an **executor** runs that plan. This split is why the same SQL can run on a laptop or on thousands of machines in BigQuery, and why the last group of this section is about reading plans: when a query is slow, the cause is almost always in the plan the optimizer chose, not in the words you typed. Everything in between is about expressing questions clearly and correctly, which matters more than speed: a fast query that double-counts revenue is worse than a slow correct one.

The examples use BigQuery's dialect, called **GoogleSQL**, because BigQuery is the warehouse of this course. Most of the first group is plain standard SQL that also runs unchanged in SQLite, which ships with Python, so you can practise on your own laptop for free. Where another engine is the point (PostgreSQL plans, for example), the example says so.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## SQL foundations

This group takes you from zero to confident everyday SQL. The first topic introduces a tiny online shop with four tables that the whole section uses, so you always know what the data looks like and can check every result by eye. It covers the core of the language: choosing columns, filtering rows, sorting, combining tables with joins, and summarising with `GROUP BY`.

The next four topics add the tools that separate everyday SQL from data engineering SQL. Window functions compute across related rows without collapsing them. CTEs and subqueries let you build a query in readable steps. Dates and time zones cause more silent bugs than anything else in pipelines. And nested data (arrays, structs and JSON) is how modern event data arrives. Read the topics in order: each one uses the sample tables and ideas from the ones before.

### SQL refresher: SELECT, filtering, joins and GROUP BY

::: plain
A database table is like a spreadsheet with strict rules: every row is one thing (one customer, one order), every column has a name and a type, and every value in a column has that type. A SQL query is a precise question you hand to the database, such as "show me the paid orders over 30, biggest first". You do not tell it how to look; you describe what you want, and it works out how to find it. Learning SQL is learning a small set of clauses (`SELECT`, `FROM`, `WHERE`, `GROUP BY`, `ORDER BY`) and how they fit together.
:::

**The sample data.** A small shop has customers and orders. `shop` is the dataset (BigQuery's name for a folder of tables):

```text
shop.customers                          shop.orders
customer_id | name | city | signup_date order_id | customer_id | order_date | status    | amount
1           | Asha | Pune | 2026-01-05  101      | 1           | 2026-03-01 | paid      | 40.0
2           | Bo   | Oslo | 2026-01-20  102      | 2           | 2026-03-01 | cancelled | 15.5
3           | Chen | Pune | 2026-02-11  103      | 1           | 2026-03-02 | paid      | 22.0
4           | Dana | NULL | 2026-03-02  104      | 3           | 2026-03-03 | paid      | 60.0
                                        105      | 2           | 2026-03-05 | paid      | 35.0
                                        106      | 1           | 2026-03-08 | paid      | 18.0
                                        107      | 9           | 2026-03-08 | paid      | 12.0
```

Two details are deliberate. Dana's city is `NULL`, SQL's marker for "unknown", and she has no orders. Order 107 belongs to customer 9, who is missing from `customers` (real data is messy like this). The section also uses `shop.order_items` (the lines of orders 101, 103 and 104: what was bought, how many and at what price; the script below creates it), `shop.customer_changes` (email changes) and `shop.employees` (an org chart); the last two are shown where they first appear.

To practise without a cloud account, save this script as `shop.py` and run `python3 shop.py`. It builds the tables in SQLite, in memory, under the same `shop.` names, and runs a query you can edit:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.execute("ATTACH ':memory:' AS shop")   # lets us write shop.orders, as in BigQuery
db.executescript("""
CREATE TABLE shop.customers (customer_id INTEGER, name TEXT, city TEXT, signup_date TEXT);
INSERT INTO shop.customers VALUES
  (1, 'Asha', 'Pune', '2026-01-05'), (2, 'Bo', 'Oslo', '2026-01-20'),
  (3, 'Chen', 'Pune', '2026-02-11'), (4, 'Dana', NULL, '2026-03-02');
CREATE TABLE shop.orders (order_id INTEGER, customer_id INTEGER, order_date TEXT,
                          status TEXT, amount REAL);
INSERT INTO shop.orders VALUES
  (101, 1, '2026-03-01', 'paid', 40.0), (102, 2, '2026-03-01', 'cancelled', 15.5),
  (103, 1, '2026-03-02', 'paid', 22.0), (104, 3, '2026-03-03', 'paid', 60.0),
  (105, 2, '2026-03-05', 'paid', 35.0), (106, 1, '2026-03-08', 'paid', 18.0),
  (107, 9, '2026-03-08', 'paid', 12.0);
CREATE TABLE shop.order_items (order_id INTEGER, product TEXT, qty INTEGER, price REAL);
INSERT INTO shop.order_items VALUES
  (101, 'tea', 2, 10.0), (101, 'cake', 1, 20.0), (103, 'tea', 1, 10.0),
  (103, 'jam', 2, 6.0), (104, 'cake', 3, 20.0);
""")

query = "SELECT name, city FROM shop.customers"
for row in db.execute(query):
    print(row)        # ('Asha', 'Pune') ... ('Dana', None)
```

**Choosing columns and filtering rows.** `SELECT` lists the columns you want, `FROM` names the table, `WHERE` keeps only rows for which a condition is true, and `ORDER BY` sorts:

```sql
SELECT order_id, amount
FROM shop.orders
WHERE status = 'paid' AND amount >= 30
ORDER BY amount DESC;
-- 104 | 60.0
-- 101 | 40.0
-- 105 | 35.0
```

Text values go in single quotes. Conditions combine with `AND`, `OR` and `NOT`; use parentheses whenever you mix `AND` and `OR`. Other useful tests are `IN ('paid', 'refunded')`, `BETWEEN 10 AND 20` (both ends included), and `LIKE 'A%'` (`%` matches any run of characters).

You can also compute new columns, name them with `AS` (an **alias**), and pick values with `CASE`, SQL's if-then-else. `LIMIT` caps the number of rows returned:

```sql
SELECT
  order_id,
  ROUND(amount * 1.18, 2) AS with_tax,
  CASE WHEN amount >= 30 THEN 'large' ELSE 'small' END AS size
FROM shop.orders
ORDER BY order_id
LIMIT 3;
-- 101 | 47.2  | large
-- 102 | 18.29 | small
-- 103 | 25.96 | small
```

`SELECT DISTINCT status FROM shop.orders` returns each different value once (`cancelled`, `paid`).

**NULL and three-valued logic.** `NULL` means "unknown", and any comparison with an unknown is itself unknown, neither true nor false. `WHERE` keeps only rows where the condition is **true**, so unknown rows disappear:

```sql
SELECT name FROM shop.customers WHERE city <> 'Pune';   -- Bo   (Dana is missing!)
SELECT name FROM shop.customers WHERE city = NULL;      -- no rows, ever
SELECT name FROM shop.customers WHERE city IS NULL;     -- Dana
SELECT name, COALESCE(city, 'unknown') AS city          -- Bo | Oslo
FROM shop.customers                                     -- Dana | unknown
WHERE city IS NULL OR city <> 'Pune';
```

Test for missing values with `IS NULL` and `IS NOT NULL`, and replace them with `COALESCE(value, fallback)`, which returns its first non-null argument.

> **Pitfall:** `WHERE city <> 'Pune'` silently drops rows where `city` is `NULL`. Whenever a column can be null, ask yourself what should happen to those rows and write it explicitly.

**Aggregates and GROUP BY.** Aggregate functions summarise many rows into one value: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.

```sql
SELECT COUNT(*) AS paid_orders, SUM(amount) AS revenue, MAX(amount) AS biggest
FROM shop.orders
WHERE status = 'paid';
-- 6 | 187.0 | 60.0

SELECT COUNT(*) AS customers, COUNT(city) AS with_city, COUNT(DISTINCT city) AS cities
FROM shop.customers;
-- 4 | 3 | 2
```

`COUNT(*)` counts rows; `COUNT(city)` counts non-null values; `COUNT(DISTINCT city)` counts different non-null values. All aggregates except `COUNT(*)` ignore nulls.

`GROUP BY` splits rows into groups that share a value and computes the aggregates once per group. `HAVING` filters groups after aggregation, the way `WHERE` filters rows before it:

```sql
SELECT customer_id, COUNT(*) AS orders, SUM(amount) AS revenue
FROM shop.orders
WHERE status = 'paid'
GROUP BY customer_id
HAVING SUM(amount) > 30
ORDER BY revenue DESC;
-- 1 | 3 | 80.0
-- 3 | 1 | 60.0
-- 2 | 1 | 35.0         (customer 9, with 12.0, is removed by HAVING)
```

The rule that trips up every beginner: every column in `SELECT` must either be in `GROUP BY` or be inside an aggregate. `SELECT customer_id, order_date, SUM(amount) ... GROUP BY customer_id` is an error, because a customer has several order dates and the database cannot know which one you mean.

**Joins** combine rows from two tables that match on a condition, usually a key. An `INNER JOIN` (or just `JOIN`) keeps only rows that match on both sides:

```sql
SELECT c.name, SUM(o.amount) AS revenue
FROM shop.orders AS o
JOIN shop.customers AS c
  ON o.customer_id = c.customer_id
WHERE o.status = 'paid'
GROUP BY c.name
ORDER BY revenue DESC;
-- Asha | 80.0
-- Chen | 60.0
-- Bo   | 35.0        (order 107 has no matching customer, so it vanished)
```

`o` and `c` are table aliases; `o.amount` means "the amount column of orders". A `LEFT JOIN` keeps every row from the left table and fills the right side with nulls where nothing matches. It is how you keep customers who have no orders:

```sql
SELECT c.name, COUNT(o.order_id) AS paid_orders
FROM shop.customers AS c
LEFT JOIN shop.orders AS o
  ON o.customer_id = c.customer_id
 AND o.status = 'paid'
GROUP BY c.name
ORDER BY c.name;
-- Asha | 3
-- Bo   | 1
-- Chen | 1
-- Dana | 0
```

[[fig:sql_joins]]

`RIGHT JOIN` is the mirror image, and `FULL OUTER JOIN` keeps unmatched rows from both sides: on our data it returns Dana with a null order and order 107 with a null name, which makes it the standard tool for comparing two tables. `CROSS JOIN` pairs every row with every row (4 customers times 7 orders is 28 rows).

> **Pitfall:** In the `LEFT JOIN` above the status test sits in the `ON` clause. Move it to `WHERE o.status = 'paid'` and Dana disappears, because her joined row has `o.status` = `NULL`, which is not `'paid'`. A condition on the right-hand table in `WHERE` quietly turns a left join into an inner join.

**The order SQL runs in.** You write `SELECT` first, but the database evaluates the clauses in a different, logical order. Knowing it explains most error messages:

[[fig:sql_order]]

That order is why you cannot use a `SELECT` alias in `WHERE` (it does not exist yet), why `HAVING` can filter on `SUM(amount)` but `WHERE` cannot, and why `ORDER BY revenue` works (it runs last).

::: deeper
**Join cardinality is the first thing to check.** A join multiplies rows whenever a key matches more than once. Joining orders (one row per order) to order lines (several per order) gives one row per line, so `SUM(o.amount)` after that join double-counts. Before writing a join, say out loud what one row of each input means (its **grain**) and what one row of the output will mean. The SQL Anti-Patterns topic returns to this "fan-out" bug.

**Joins are not set lookups.** `ON` can hold any condition, not just key equality: ranges (`o.order_date BETWEEN d.valid_from AND d.valid_to`), inequalities, or several columns. Equality on keys is what engines optimise best (hash joins, covered in the plans topic); range conditions can be much slower.

**`USING` and `NATURAL JOIN`.** `JOIN shop.customers USING (customer_id)` joins on a column with the same name on both sides and outputs it once. `NATURAL JOIN` joins on every column with the same name, which breaks silently when someone adds a column such as `updated_at` to both tables. Avoid it.

**Dialect notes.** GoogleSQL adds conveniences you will see in this course: `SELECT * EXCEPT (col)` and `SELECT * REPLACE (expr AS col)`, `GROUP BY ALL` (group by every non-aggregated column), and `QUALIFY` (covered in the next topic). BigQuery also has `SAFE_DIVIDE(a, b)`, which returns `NULL` instead of failing on division by zero, and `IF(cond, a, b)` as a short `CASE`. Integer division differs between engines: in PostgreSQL and SQLite `7 / 2` is `3`, while in BigQuery `/` always returns a `FLOAT64`, so it is `3.5`.

> **Interview:** "What is the difference between `WHERE` and `HAVING`?" `WHERE` filters input rows before grouping and cannot use aggregates; `HAVING` filters groups after aggregation and can. Filtering in `WHERE` when possible is also faster, because fewer rows reach the aggregation.
:::

::: check
?? Why does `SELECT name FROM shop.customers WHERE city <> 'Pune'` not return Dana?
Her city is `NULL`, so `city <> 'Pune'` is unknown rather than true, and `WHERE` keeps only true rows. Add `OR city IS NULL` if she should be included.

?? You need every customer with their count of paid orders, including customers with none. Which join, and where does the status condition go?
A `LEFT JOIN` from customers to orders, with `o.status = 'paid'` in the `ON` clause, and `COUNT(o.order_id)` so unmatched customers count 0. Putting the condition in `WHERE` would drop customers without paid orders.

?? Why is `SELECT customer_id, order_date, SUM(amount) FROM shop.orders GROUP BY customer_id` an error?
`order_date` is neither grouped nor aggregated, and each customer group contains several dates, so there is no single value to show.
:::

::: resources
- Docs: [GoogleSQL query syntax](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax) · the complete reference for `SELECT`, joins, `GROUP BY`, `QUALIFY` and set operations in BigQuery
- Docs: [PostgreSQL tutorial: the SQL language](https://www.postgresql.org/docs/current/tutorial-sql.html) · a short, careful introduction to tables, queries, joins and aggregates
- Book: [Learning SQL, 3rd edition](https://www.oreilly.com/library/view/learning-sql-3rd/9781492057604/) · Alan Beaulieu's beginner book, from the first query to analytic functions
- Course: [SQLBolt](https://sqlbolt.com/) · free interactive lessons that run in the browser, one clause at a time
- Course: [Select Star SQL](https://selectstarsql.com/) · a free interactive book that teaches SQL on a real dataset
- Article: [SQL queries don't start with SELECT](https://jvns.ca/blog/2019/10/03/sql-queries-don-t-start-with-select/) · Julia Evans on the logical order of evaluation, with drawings
:::

### Window functions in depth

::: plain
A `GROUP BY` squashes many rows into one per group, like adding up each customer's receipts and throwing the receipts away. A **window function** does the calculation but keeps every row: each receipt gets a note written on it, such as "running total so far: 62" or "this is the customer's 2nd biggest purchase". The "window" is the set of related rows each calculation looks at, for example all orders of the same customer, up to and including this one.
:::

A window function is any function followed by `OVER (...)`. The clause inside `OVER` defines, for each row, which rows the function sees:

- `PARTITION BY` splits rows into independent groups (like `GROUP BY`, but nothing collapses).
- `ORDER BY` sorts the rows within each partition.
- A **frame** (`ROWS BETWEEN ... AND ...`) narrows the window to a range of rows around the current one.

Start with an aggregate used as a window function:

```sql
SELECT
  order_id,
  customer_id,
  amount,
  SUM(amount) OVER () AS total_all,
  SUM(amount) OVER (PARTITION BY customer_id) AS total_customer,
  ROUND(100 * amount / SUM(amount) OVER (PARTITION BY customer_id), 1) AS pct_of_customer
FROM shop.orders
WHERE status = 'paid' AND customer_id IN (1, 2)
ORDER BY customer_id, order_id;
-- 101 | 1 | 40.0 | 115.0 | 80.0 | 50.0
-- 103 | 1 | 22.0 | 115.0 | 80.0 | 27.5
-- 106 | 1 | 18.0 | 115.0 | 80.0 | 22.5
-- 105 | 2 | 35.0 | 115.0 | 35.0 | 100.0
```

`OVER ()` is one window holding every row; `PARTITION BY customer_id` gives each customer their own window. Every input row is still there, now with its context attached. Window functions run after `WHERE` and `GROUP BY`, so they see only the rows that survived the filter.

**Running totals.** Add `ORDER BY` inside the window and an aggregate becomes cumulative:

```sql
SELECT
  customer_id,
  order_date,
  amount,
  SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM shop.orders
WHERE status = 'paid' AND customer_id = 1;
-- 1 | 2026-03-01 | 40.0 | 40.0
-- 1 | 2026-03-02 | 22.0 | 62.0
-- 1 | 2026-03-08 | 18.0 | 80.0
```

**Ranking.** Three functions number rows, and they differ only on ties. On our data, orders 101 and 102 share a date, and so do 106 and 107:

```sql
SELECT
  order_id,
  order_date,
  ROW_NUMBER() OVER (ORDER BY order_date) AS row_num,
  RANK()       OVER (ORDER BY order_date) AS rnk,
  DENSE_RANK() OVER (ORDER BY order_date) AS dense
FROM shop.orders
ORDER BY order_date, order_id;
-- 101 | 2026-03-01 | 1 | 1 | 1
-- 102 | 2026-03-01 | 2 | 1 | 1
-- 103 | 2026-03-02 | 3 | 3 | 2
-- 104 | 2026-03-03 | 4 | 4 | 3
-- 105 | 2026-03-05 | 5 | 5 | 4
-- 106 | 2026-03-08 | 6 | 6 | 5
-- 107 | 2026-03-08 | 7 | 6 | 5
```

`ROW_NUMBER` always gives distinct numbers (which of two tied rows gets 1 is arbitrary); `RANK` gives ties the same number and then skips (1, 1, 3); `DENSE_RANK` gives ties the same number without gaps (1, 1, 2). `NTILE(n)` splits rows into n roughly equal buckets, useful for quartiles.

**Looking at neighbours.** `LAG` reads a value from an earlier row in the window and `LEAD` from a later one:

```sql
SELECT
  order_id,
  amount,
  LAG(amount)  OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
  LEAD(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount
FROM shop.orders
WHERE status = 'paid' AND customer_id = 1
ORDER BY order_date;
-- 101 | 40.0 | NULL | 22.0
-- 103 | 22.0 | 40.0 | 18.0
-- 106 | 18.0 | 22.0 | NULL
```

`LAG(amount, 2)` looks two rows back, and `LAG(amount, 1, 0)` returns 0 instead of `NULL` at the edge. `FIRST_VALUE`, `LAST_VALUE` and `NTH_VALUE` pick a value from a position in the frame.

**Frames.** With `ORDER BY` and no explicit frame, the default frame is "from the start of the partition up to the current row **and all rows tied with it**" (`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`). Ties make that matter:

```sql
SELECT
  order_id,
  order_date,
  amount,
  SUM(amount) OVER (ORDER BY order_date) AS range_total,
  SUM(amount) OVER (ORDER BY order_date, order_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_total,
  ROUND(AVG(amount) OVER (ORDER BY order_date, order_id
                          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_last_3
FROM shop.orders
WHERE status = 'paid'
ORDER BY order_date, order_id;
-- 101 | 2026-03-01 | 40.0 | 40.0  | 40.0  | 40.0
-- 103 | 2026-03-02 | 22.0 | 62.0  | 62.0  | 31.0
-- 104 | 2026-03-03 | 60.0 | 122.0 | 122.0 | 40.67
-- 105 | 2026-03-05 | 35.0 | 157.0 | 157.0 | 39.0
-- 106 | 2026-03-08 | 18.0 | 187.0 | 175.0 | 37.67
-- 107 | 2026-03-08 | 12.0 | 187.0 | 187.0 | 21.67
```

Orders 106 and 107 share a date, so `RANGE` treats them as one step and both show 187. `ROWS` counts physical rows, so it needs a unique sort order (here `order_date, order_id`) to be deterministic. `avg_last_3` is a moving average over the current row and the two before it.

[[fig:sql_frame]]

> **Pitfall:** `LAST_VALUE(x) OVER (ORDER BY d)` almost never does what people expect. The default frame ends at the current row, so it returns the current row's value. Write the frame out: `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

**Deduplication: the most common window query in pipelines.** Sources often send the same record several times, or several versions of it. Keep the latest version per key with `ROW_NUMBER`:

```text
shop.customer_changes
customer_id | email            | updated_at
1           | asha@old.example | 2026-01-05 09:00:00
1           | asha@new.example | 2026-02-20 14:30:00
2           | bo@example.com   | 2026-01-20 11:00:00
2           | bo@example.com   | 2026-01-20 11:00:00
3           | chen@example.com | 2026-02-11 08:15:00
```

```sql
SELECT customer_id, email, updated_at
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM shop.customer_changes
)
WHERE rn = 1
ORDER BY customer_id;
-- 1 | asha@new.example | 2026-02-20 14:30:00
-- 2 | bo@example.com   | 2026-01-20 11:00:00
-- 3 | chen@example.com | 2026-02-11 08:15:00
```

BigQuery has a shortcut: the `QUALIFY` clause filters on a window function the way `HAVING` filters on an aggregate, so no subquery is needed:

```sql
SELECT customer_id, email, updated_at
FROM shop.customer_changes
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;
```

**Gaps and islands.** "Find runs of consecutive days with sales" is a classic. The trick: number the dates in order and subtract the number from the date. Within a run of consecutive days, date and number both go up by one, so the difference stays constant and labels the run:

```sql
WITH days AS (
  SELECT DISTINCT order_date FROM shop.orders WHERE status = 'paid'
),
numbered AS (
  SELECT order_date, ROW_NUMBER() OVER (ORDER BY order_date) AS rn
  FROM days
),
grouped AS (
  SELECT order_date, DATE_SUB(order_date, INTERVAL rn DAY) AS grp
  FROM numbered
)
SELECT MIN(order_date) AS island_start, MAX(order_date) AS island_end, COUNT(*) AS days
FROM grouped
GROUP BY grp
ORDER BY island_start;
-- 2026-03-01 | 2026-03-03 | 3
-- 2026-03-05 | 2026-03-05 | 1
-- 2026-03-08 | 2026-03-08 | 1
```

The dates 03-01, 03-02, 03-03 get numbers 1, 2, 3, so each minus its number is 02-28; 03-05 minus 4 is 03-01; 03-08 minus 5 is 03-03. Three labels, three islands. The same idea finds sessions, streaks and outages.

::: deeper
**Where windows run.** Logically, window functions are evaluated after `HAVING` and before `ORDER BY` and `LIMIT`. That is why you cannot put a window function in `WHERE` (hence `QUALIFY`), and why you can window over aggregates: `SUM(SUM(amount)) OVER ()` gives the grand total next to each group's total, for "share of total" columns.

**Cost.** To evaluate `PARTITION BY k ORDER BY t`, an engine must bring each partition together and sort it. In a distributed engine such as BigQuery or Spark that means a **shuffle** on `k`. A window with no `PARTITION BY` (`OVER (ORDER BY t)`) must put every row in one partition, so one worker sorts the whole table; on billions of rows that ends in "Resources exceeded". A huge single partition (a skewed key) has the same effect.

**Frame types.** `ROWS` counts physical rows. `RANGE` uses values: `RANGE BETWEEN 6 PRECEDING AND CURRENT ROW` over a numeric or date-like ordering includes every row whose value is within 6 of the current one, which handles missing days correctly where `ROWS` would not. PostgreSQL, SQLite and some others add `GROUPS` (count peer groups) and `EXCLUDE` options. BigQuery supports `ROWS` and `RANGE`; a `RANGE` frame with a numeric offset needs a single numeric `ORDER BY` key, so for dates you order by something like `UNIX_DATE(order_date)` (days since 1970-01-01).

**Named windows.** `WINDOW w AS (PARTITION BY customer_id ORDER BY order_date)` at the end of the query lets several functions share `OVER w`, which is shorter and lets the engine compute one sort.

**Determinism.** `ROW_NUMBER` over a non-unique ordering can pick different rows on different runs, which makes deduplication non-reproducible. Always add a tie-breaker column (an ID or an ingestion timestamp) to the `ORDER BY`.

> **Interview:** "Return the second-highest salary in each department." `DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC)` and keep rank 2. Discuss ties: `DENSE_RANK` returns all people tied for second, `ROW_NUMBER` returns an arbitrary one, and `RANK` may return nothing if two people tie for first.
:::

::: check
?? On the dates 1, 1, 2 (sorted), what do `ROW_NUMBER`, `RANK` and `DENSE_RANK` return?
`ROW_NUMBER`: 1, 2, 3. `RANK`: 1, 1, 3. `DENSE_RANK`: 1, 1, 2.

?? Why does `SUM(amount) OVER (ORDER BY order_date)` give the same value on two rows with the same date?
The default frame is `RANGE ... CURRENT ROW`, which includes all rows tied with the current one. Use `ROWS` with a unique ordering for a row-by-row running total.

?? How do you keep only the newest row per `customer_id` in BigQuery without a subquery?
`QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1`.
:::

::: resources
- Docs: [Window function calls](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls) · BigQuery's `OVER` clause, frames and named windows
- Docs: [PostgreSQL tutorial: window functions](https://www.postgresql.org/docs/current/tutorial-window.html) · the clearest short introduction to partitions and frames
- Docs: [SQLite window functions](https://www.sqlite.org/windowfunctions.html) · a precise description of frames, `GROUPS` and `EXCLUDE`, runnable locally
- Article: [Use The Index, Luke: window functions](https://use-the-index-luke.com/sql/partial-results/window-functions) · Markus Winand on how window functions execute and how indexes help them
- Course: [Window Functions (windowfunctions.com)](https://www.windowfunctions.com/) · interactive exercises from simple to tricky
- Book: [SQL for Data Analysis](https://www.oreilly.com/library/view/sql-for-data/9781492088776/) · Cathy Tanimura's book, strong on window-based analysis patterns
:::

### CTEs, subqueries and recursive queries

::: plain
Hard questions are easier in steps: "first find the paid orders, then add them up per customer, then attach names and label the big spenders". A **subquery** is a query inside another query. A **CTE** (common table expression) is a named step written at the top with `WITH`, like a labelled scratch table that exists only while the query runs. A **recursive** CTE is a step that refers to itself, which lets SQL walk a tree such as an org chart, one level at a time.
:::

**Subqueries.** A subquery in parentheses can stand where a value, a list or a table is expected.

A **scalar subquery** returns one value:

```sql
SELECT order_id, amount
FROM shop.orders
WHERE status = 'paid'
  AND amount > (SELECT AVG(amount) FROM shop.orders WHERE status = 'paid')
ORDER BY amount DESC;
-- 104 | 60.0
-- 101 | 40.0
-- 105 | 35.0        (the average paid amount is 31.17)
```

A subquery with `IN` or `EXISTS` tests membership. `EXISTS` is true when the subquery returns at least one row:

```sql
SELECT name
FROM shop.customers AS c
WHERE EXISTS (
  SELECT 1
  FROM shop.orders AS o
  WHERE o.customer_id = c.customer_id
    AND o.status = 'paid'
)
ORDER BY name;
-- Asha, Bo, Chen
```

This subquery mentions `c.customer_id` from the outer query, so it is **correlated**: logically it runs once per outer row. Correlated subqueries can also appear in `SELECT`:

```sql
SELECT
  c.name,
  (SELECT MAX(o.order_date)
   FROM shop.orders AS o
   WHERE o.customer_id = c.customer_id) AS last_order
FROM shop.customers AS c
ORDER BY c.name;
-- Asha | 2026-03-08
-- Bo   | 2026-03-05
-- Chen | 2026-03-03
-- Dana | NULL
```

A subquery in `FROM` is a **derived table**: a temporary result you then query. Here it computes revenue per customer and then the average per city:

```sql
SELECT city, AVG(revenue) AS avg_revenue
FROM (
  SELECT c.city, c.customer_id, SUM(o.amount) AS revenue
  FROM shop.customers AS c
  JOIN shop.orders AS o ON o.customer_id = c.customer_id
  WHERE o.status = 'paid'
  GROUP BY c.city, c.customer_id
) AS per_customer
GROUP BY city
ORDER BY city;
-- Oslo | 35.0
-- Pune | 70.0
```

> **Pitfall:** `NOT IN` with a list that contains a `NULL` returns no rows at all. `WHERE customer_id NOT IN (1, 2, NULL)` asks "is it different from 1, from 2 and from unknown?", and the last part is unknown for every row. If the subquery's column can contain nulls, use `NOT EXISTS` instead, which behaves as you expect.

**CTEs.** Nested subqueries read inside out. A `WITH` clause lets you name each step and read top to bottom:

```sql
WITH paid AS (
  SELECT customer_id, amount
  FROM shop.orders
  WHERE status = 'paid'
),
per_customer AS (
  SELECT customer_id, COUNT(*) AS orders, SUM(amount) AS revenue
  FROM paid
  GROUP BY customer_id
)
SELECT
  c.name,
  COALESCE(p.orders, 0) AS orders,
  COALESCE(p.revenue, 0) AS revenue,
  CASE
    WHEN p.revenue >= 50 THEN 'gold'
    WHEN p.revenue > 0 THEN 'silver'
    ELSE 'none'
  END AS tier
FROM shop.customers AS c
LEFT JOIN per_customer AS p USING (customer_id)
ORDER BY revenue DESC;
-- Asha | 3 | 80.0 | gold
-- Chen | 1 | 60.0 | gold
-- Bo   | 1 | 35.0 | silver
-- Dana | 0 | 0    | none
```

Each CTE can use the ones defined before it. This is how dbt and Dataform models are usually written: an "import" CTE per source, then one CTE per logical step, then a final `SELECT`. When a result looks wrong, you can debug by selecting from each CTE in turn and checking its row count and grain.

**Recursive CTEs.** Some questions have no fixed number of steps: "everyone who reports to Ravi, at any depth". The org chart is stored as a table where each person points to their manager:

```text
shop.employees
emp_id | name | manager_id
1      | Ravi | NULL
2      | Mei  | 1
3      | Tom  | 1
4      | Ana  | 2
5      | Lee  | 4
```

A recursive CTE has an **anchor** query (the starting rows), `UNION ALL`, and a **recursive** query that joins the table to the rows found so far. The engine repeats the recursive part, each time using only the rows produced by the previous round, until a round adds nothing:

```sql
WITH RECURSIVE chain AS (
  -- anchor: the person at the top
  SELECT emp_id, name, manager_id, 0 AS depth, name AS path
  FROM shop.employees
  WHERE manager_id IS NULL
  UNION ALL
  -- recursive step: everyone whose manager is already in the chain
  SELECT e.emp_id, e.name, e.manager_id, ch.depth + 1, ch.path || ' > ' || e.name
  FROM shop.employees AS e
  JOIN chain AS ch ON e.manager_id = ch.emp_id
)
SELECT name, depth, path
FROM chain
ORDER BY path;
-- Ravi | 0 | Ravi
-- Mei  | 1 | Ravi > Mei
-- Ana  | 2 | Ravi > Mei > Ana
-- Lee  | 3 | Ravi > Mei > Ana > Lee
-- Tom  | 1 | Ravi > Tom
```

Round 0 finds Ravi; round 1 finds Mei and Tom; round 2 finds Ana; round 3 finds Lee; round 4 finds nobody, so it stops. The same shape walks bills of materials, category trees, folder structures and graph neighbours.

> **Tip:** Recursive CTEs also generate series, but BigQuery has a simpler tool for dates: `UNNEST(GENERATE_DATE_ARRAY(DATE '2026-03-01', DATE '2026-03-08'))` returns one row per day. Joining a date series to your data with a `LEFT JOIN` is how you show days with zero sales instead of skipping them.

::: deeper
**Are CTEs materialised?** It depends on the engine. In BigQuery a CTE is like a named subquery: it is not cached, and if you reference it three times it may be computed three times. Write the result to a temporary table (in a script) when a costly CTE is reused. PostgreSQL before version 12 always materialised CTEs (an "optimisation fence"); since 12 it inlines a non-recursive CTE that is referenced once, and you can force either behaviour with `AS MATERIALIZED` or `AS NOT MATERIALIZED`. In Spark, a CTE is inlined into the plan.

**How engines run correlated subqueries.** "Once per outer row" is the logical meaning, not the execution. Good optimizers **decorrelate**: they rewrite `EXISTS` into a semi-join, `NOT EXISTS` into an anti-join, and a scalar subquery into a join with a pre-aggregated table. BigQuery decorrelates many patterns but not all; when it cannot, the error or the plan tells you, and an explicit join with a `GROUP BY` subquery usually helps.

**Recursion limits and safety.** BigQuery stops a recursive CTE after 500 iterations by default and returns an error. In its recursive term you cannot use aggregates, window functions, `ORDER BY` or `LIMIT`, and the CTE may reference itself only once. A cycle in the data (A manages B, B manages A) loops until the limit; guard against it by carrying the path and stopping when the next node is already in it. PostgreSQL 14 added a `CYCLE` clause for exactly this, plus `SEARCH DEPTH FIRST` and `SEARCH BREADTH FIRST` to control output order.

**`UNION` vs `UNION ALL`.** `UNION` removes duplicates (a costly sort or hash); `UNION ALL` keeps everything. Use `UNION ALL` unless you need deduplication. BigQuery requires you to write `UNION ALL` or `UNION DISTINCT` explicitly.

> **Interview:** "When would you use `EXISTS` instead of a join?" When you only need to know whether a match exists, not its columns. A join to a table with several matches multiplies rows and then needs `DISTINCT`; `EXISTS` (a semi-join) returns each outer row at most once.
:::

::: check
?? Why does `WHERE id NOT IN (SELECT customer_id FROM t)` return nothing when `t.customer_id` contains a `NULL`?
`x NOT IN (..., NULL)` evaluates to unknown for every `x`, and `WHERE` drops unknown rows. Use `NOT EXISTS` with a correlated condition.

?? What are the three parts of a recursive CTE, and when does it stop?
An anchor query, `UNION ALL`, and a recursive query that references the CTE. It stops when a round produces no new rows (or hits the engine's iteration limit).

?? In BigQuery, a CTE is referenced three times in one query. Is it computed once?
Not necessarily: BigQuery treats it like an inline subquery and may evaluate it each time. Materialise it into a temporary table if it is expensive.
:::

::: resources
- Docs: [WITH clause in GoogleSQL](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause) · CTE and recursive CTE rules in BigQuery
- Docs: [Work with recursive CTEs](https://docs.cloud.google.com/bigquery/docs/recursive-ctes) · BigQuery examples and the iteration limit
- Docs: [PostgreSQL: WITH queries](https://www.postgresql.org/docs/current/queries-with.html) · recursive queries, `SEARCH`, `CYCLE` and materialisation
- Docs: [PostgreSQL: subquery expressions](https://www.postgresql.org/docs/current/functions-subquery.html) · `EXISTS`, `IN`, `ANY` and their null behaviour
- Article: [Modern SQL: the WITH clause](https://modern-sql.com/feature/with) · Markus Winand on CTEs, their history and support across databases
- Docs: [SQLite: the WITH clause](https://www.sqlite.org/lang_with.html) · recursive CTE semantics explained step by step, with runnable examples
- Article: [Use The Index, Luke](https://use-the-index-luke.com/) · free online book; its join chapters explain how subqueries become joins
:::

### Dates, time zones and timestamps in SQL

::: plain
"What were yesterday's sales?" sounds simple until you ask: yesterday where? When it is 2 a.m. on Monday in Pune, it is still Sunday evening in London and Sunday afternoon in New York. A **timestamp** is an exact moment, the same everywhere in the world, like the instant a rocket launches. A **date** or a **wall-clock time** only means something once you know which place's calendar you are reading. Most date bugs come from mixing those two ideas.
:::

**The four types in BigQuery.**

| Type | What it stores | Example | Use for |
|---|---|---|---|
| `DATE` | A calendar day, no time, no zone | `DATE '2026-03-08'` | Business dates: order date, birthday, partition date |
| `TIME` | A time of day, no date, no zone | `TIME '20:30:00'` | Opening hours |
| `DATETIME` | Date and wall-clock time, **no zone** ("civil time") | `DATETIME '2026-03-08 20:30:00'` | A time as written on a local clock, such as a flight departure |
| `TIMESTAMP` | An absolute instant, microsecond precision | `TIMESTAMP '2026-03-08 20:30:00+00'` | When an event happened |

A `TIMESTAMP` has no time zone of its own: it is a point on the universal timeline, stored internally as microseconds since 1970-01-01 00:00:00 UTC (the **Unix epoch**). BigQuery displays it in UTC (`2026-03-08 20:30:00 UTC`), but the value is the same moment whichever zone you view it in. A `DATETIME` is the opposite: `2026-03-08 20:30:00` with no idea where in the world that clock hangs.

**Rule one: store events as `TIMESTAMP` in UTC, convert to local time only when you present or group.** UTC (Coordinated Universal Time) never jumps for daylight saving, so durations and ordering are always correct.

**Converting between them.** A time zone is named with an IANA identifier such as `Asia/Kolkata`, `Europe/Oslo` or `America/New_York` (avoid fixed offsets like `+05:30` and abbreviations like `EST`: they do not follow daylight-saving rules).

```sql
SELECT
  TIMESTAMP '2026-03-08 20:30:00+00'                              AS event_ts,     -- 2026-03-08 20:30:00 UTC
  DATE(TIMESTAMP '2026-03-08 20:30:00+00')                        AS utc_date,     -- 2026-03-08
  DATE(TIMESTAMP '2026-03-08 20:30:00+00', 'Asia/Kolkata')        AS pune_date,    -- 2026-03-09
  DATETIME(TIMESTAMP '2026-03-08 20:30:00+00', 'America/New_York') AS ny_clock,    -- 2026-03-08T16:30:00
  TIMESTAMP(DATETIME '2026-03-08 16:30:00', 'America/New_York')    AS back_to_ts;  -- 2026-03-08 20:30:00 UTC
```

The same event belongs to 8 March in UTC and to 9 March in India. If your business reports by Indian calendar days, compute the date with `DATE(ts, 'Asia/Kolkata')`, not `DATE(ts)`.

You can check the same facts in Python with the standard library:

```python
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# One instant, three wall clocks
ts = datetime(2026, 3, 8, 20, 30, tzinfo=timezone.utc)
print(ts.astimezone(ZoneInfo("Asia/Kolkata")))      # 2026-03-09 02:00:00+05:30
print(ts.astimezone(ZoneInfo("America/New_York")))  # 2026-03-08 16:30:00-04:00
print(ts.astimezone(ZoneInfo("Asia/Kolkata")).date())  # 2026-03-09: a different calendar day

# A local "day" is not always 24 hours
ny = ZoneInfo("America/New_York")
start = datetime(2026, 3, 8, tzinfo=ny)    # midnight local, the day clocks spring forward
end = datetime(2026, 3, 9, tzinfo=ny)
print(end.astimezone(timezone.utc) - start.astimezone(timezone.utc))  # 23:00:00

# Epoch seconds are the same everywhere
print(int(ts.timestamp()))                 # 1773001800
```

On 8 March 2026 New York moves its clocks forward an hour, so that local day lasts 23 hours. Any "hours per day" or "events per hour" metric computed in local time has a short day in spring and a long one in autumn.

**Truncating and bucketing.** Reports group by day, week or month:

```sql
SELECT
  DATE_TRUNC(DATE '2026-03-05', MONTH)         AS month_start,   -- 2026-03-01
  DATE_TRUNC(DATE '2026-03-05', WEEK)          AS week_sun,      -- 2026-03-01 (weeks start Sunday)
  DATE_TRUNC(DATE '2026-03-05', WEEK(MONDAY))  AS week_mon,      -- 2026-03-02
  DATE_TRUNC(DATE '2026-03-05', ISOWEEK)       AS iso_week,      -- 2026-03-02
  TIMESTAMP_TRUNC(TIMESTAMP '2026-03-08 20:30:00+00', DAY, 'Asia/Kolkata') AS pune_day;
  -- 2026-03-08 18:30:00 UTC, which is midnight of 9 March in Pune
```

`TIMESTAMP_TRUNC` takes a time zone because "the start of the day" depends on where you are.

**Arithmetic and differences.**

```sql
SELECT
  DATE_ADD(DATE '2026-03-01', INTERVAL 7 DAY)              AS plus_week,     -- 2026-03-08
  DATE_ADD(DATE '2026-01-31', INTERVAL 1 MONTH)            AS plus_month,    -- 2026-02-28
  LAST_DAY(DATE '2026-02-10')                              AS month_end,     -- 2026-02-28
  DATE_DIFF(DATE '2026-03-08', DATE '2026-03-01', DAY)     AS days_between,  -- 7
  DATE_DIFF(DATE '2026-03-01', DATE '2026-02-28', MONTH)   AS month_diff,    -- 1
  EXTRACT(DAYOFWEEK FROM DATE '2026-03-08')                AS dow;           -- 1 (Sunday = 1)
```

Two of these surprise people. Adding a month to 31 January gives 28 February, because BigQuery clamps to the last day of a shorter month. And `DATE_DIFF(..., MONTH)` counts month **boundaries crossed**, so 28 February to 1 March is one month even though it is one day. `DATE_DIFF` takes the later date first.

**Parsing and formatting** use format strings: `PARSE_DATE('%d/%m/%Y', '05/03/2026')` is 5 March 2026 (in the United States the same text would usually mean 3 May, which is why you always state the format), and `FORMAT_DATE('%Y-%m', order_date)` gives `2026-03`. Epoch numbers convert with `TIMESTAMP_SECONDS(1773001800)` (2026-03-08 20:30:00 UTC) and `TIMESTAMP_MILLIS`, and back with `UNIX_SECONDS` and `UNIX_MILLIS`.

**Filling gaps with a date spine.** Grouping orders by day skips days with no orders. To show zeros, generate every day and `LEFT JOIN` the data to it:

```sql
SELECT d AS day, COALESCE(SUM(o.amount), 0) AS revenue
FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2026-03-01', DATE '2026-03-08')) AS d
LEFT JOIN shop.orders AS o
  ON o.order_date = d AND o.status = 'paid'
GROUP BY d
ORDER BY d;
-- 03-01 40.0 | 03-02 22.0 | 03-03 60.0 | 03-04 0 | 03-05 35.0 | 03-06 0 | 03-07 0 | 03-08 30.0
```

Warehouses keep a permanent **date dimension** built the same way: one row per day for decades, with columns such as year, quarter, ISO week, weekday name, weekend flag, fiscal period and holiday flags, so every report uses the same calendar.

> **Pitfall:** `WHERE event_ts BETWEEN '2026-03-01' AND '2026-03-31'` misses almost all of 31 March, because the upper bound means 31 March at 00:00:00. Use a half-open range: `event_ts >= '2026-03-01' AND event_ts < '2026-04-01'`. It is correct for every type and every precision.

> **Pitfall:** BigQuery's `CURRENT_DATE()` is the date in UTC. A pipeline scheduled at 01:00 in Pune (19:30 UTC the previous day) that processes "yesterday" with `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` processes the wrong day. Pass the zone (`CURRENT_DATE('Asia/Kolkata')`) or, better, pass the run date in from the orchestrator.

::: deeper
**Other engines.** PostgreSQL has `timestamp` (no zone, like `DATETIME`) and `timestamptz` (an instant, like `TIMESTAMP`, displayed in the session's `TimeZone` setting); use `timestamptz` for events. In Spark SQL, `TIMESTAMP` means "local time zone" semantics (an instant shown in `spark.sql.session.timeZone`), and Spark 3.4 added `TIMESTAMP_NTZ` for civil time. Snowflake has three: `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ` and `TIMESTAMP_TZ`. When moving data between engines, check which semantics each column has, or an instant can silently become a wall-clock time shifted by the session zone.

**Partitioning uses UTC.** A BigQuery table partitioned by `DATE(event_ts)` or by a `TIMESTAMP` column puts rows into UTC days. A query for "9 March in Pune" therefore touches two UTC partitions (8 and 9 March), which is fine as long as the filter is on the timestamp range, not on a converted date expression the optimizer cannot use for pruning.

**Weeks are three different things.** `EXTRACT(WEEK ...)` counts Sunday-based weeks, where days before the first Sunday of the year are week 0. `ISOWEEK` follows ISO 8601: weeks start Monday and week 1 contains the year's first Thursday, so 29 December 2025 is in ISO week 1 of **ISO year 2026**, and 1 January 2027 is in ISO week 53 of ISO year 2026. Use `ISOYEAR` with `ISOWEEK`, never `YEAR` with `ISOWEEK`.

**Late and out-of-order data.** An event's **event time** (when it happened) differs from its **ingestion time** (when it arrived). Mobile apps upload hours late; time zones on devices are wrong. Partition and report on event time, but load incrementally on ingestion time, and reprocess a few days back to catch late arrivals. The Streaming systems section covers watermarks, the streaming answer to the same problem.

**Leap seconds and precision.** BigQuery timestamps have microsecond precision and cannot represent leap seconds. Nanosecond timestamps from other systems (Parquet `INT64` nanos, some Kafka producers) must be truncated or stored as integers.

> **Interview:** "Daily active users differ between the dashboard and your query by a few percent. Why?" The usual suspects: different time zone for the day boundary, `BETWEEN` on timestamps missing the last day, late-arriving events loaded after the dashboard refreshed, and counting distinct users per UTC day versus per local day.
:::

::: check
?? An event happened at `2026-03-08 20:30:00 UTC`. What calendar date is it in Pune (UTC+05:30), and how do you compute that in BigQuery?
9 March 2026 (02:00 local). `DATE(event_ts, 'Asia/Kolkata')`.

?? Why is `WHERE ts BETWEEN '2026-03-01' AND '2026-03-31'` wrong for March, and what should you write?
The upper bound is midnight at the start of 31 March, so almost all of that day is excluded. Use `ts >= '2026-03-01' AND ts < '2026-04-01'`.

?? When should you use `DATETIME` instead of `TIMESTAMP`?
When the value is a wall-clock time whose zone is unknown or irrelevant (a local appointment time). Events that happened at a real instant belong in `TIMESTAMP`.
:::

::: resources
- Docs: [Date functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/date_functions) · `DATE_TRUNC`, `DATE_DIFF`, `LAST_DAY`, `PARSE_DATE` and format elements
- Docs: [Timestamp functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions) · conversions, truncation with time zones and epoch helpers
- Docs: [GoogleSQL data types](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types) · exact definitions of `DATE`, `DATETIME`, `TIME` and `TIMESTAMP` and time zone names
- Docs: [PostgreSQL: date/time types](https://www.postgresql.org/docs/current/datatype-datetime.html) · `timestamp` vs `timestamptz` and how input is interpreted
- Docs: [Python zoneinfo](https://docs.python.org/3/library/zoneinfo.html) · IANA time zones in the standard library, for testing and pipelines
- Article: [Falsehoods programmers believe about time](https://infiniteundo.com/post/25326999628/falsehoods-programmers-believe-about-time) · a short list of assumptions that cause real bugs
:::

### Semi-structured data in SQL: JSON, arrays and structs

::: plain
Not all data fits a flat grid. An order has several items; an app event carries a bag of properties that differ from event to event. Old-style databases forced you to split these into separate tables. Modern warehouses let a single cell hold a **list** (an array), a **little record with named fields** (a struct), or a whole **JSON document**. Think of a row as an envelope: a flat row holds one sheet of paper, while a nested row can hold a folder of sheets. `UNNEST` is how you open the folder and lay its sheets out as rows.
:::

**Structs and arrays.** In BigQuery a `STRUCT` groups named fields, and an `ARRAY` is an ordered list of values of one type. A column of type `ARRAY<STRUCT<...>>` is called a **nested, repeated** field. Here is the shop's orders table stored nested, with the items inside each order:

```text
shop.orders_nested
order_id | customer_id | items  (ARRAY<STRUCT<product STRING, qty INT64, price NUMERIC>>)
101      | 1           | [{tea, 2, 10}, {cake, 1, 20}]
103      | 1           | [{tea, 1, 10}, {jam, 2, 6}]
104      | 3           | [{cake, 3, 20}]
105      | 2           | []
```

```sql
CREATE TABLE shop.orders_nested (
  order_id    INT64,
  customer_id INT64,
  items       ARRAY<STRUCT<product STRING, qty INT64, price NUMERIC>>
);
```

Why store it this way? The items travel with their order, so "orders with their items" needs no join, and a columnar engine stores `items.product`, `items.qty` and `items.price` as separate columns, so reading one of them stays cheap.

**Reading arrays without flattening.**

```sql
SELECT
  order_id,
  ARRAY_LENGTH(items)                AS n_items,       -- 2, 2, 1, 0
  items[SAFE_OFFSET(0)].product      AS first_product, -- tea, tea, cake, NULL
  (SELECT SUM(i.qty * i.price) FROM UNNEST(o.items) AS i) AS order_total  -- 40, 22, 60, NULL
FROM shop.orders_nested AS o
ORDER BY order_id;
```

`OFFSET(0)` is zero-based and fails if the index is out of range; `SAFE_OFFSET` returns `NULL` instead (`ORDINAL(1)` and `SAFE_ORDINAL` count from one). The subquery over `UNNEST(o.items)` computes per-order values while keeping one row per order.

**Flattening with UNNEST.** `UNNEST` turns an array into rows. Joined to its parent row, it gives one output row per element:

```sql
SELECT o.order_id, item.product, item.qty
FROM shop.orders_nested AS o
CROSS JOIN UNNEST(o.items) AS item
ORDER BY o.order_id;
-- 101 | tea  | 2
-- 101 | cake | 1
-- 103 | tea  | 1
-- 103 | jam  | 2
-- 104 | cake | 3
```

The comma form `FROM shop.orders_nested AS o, UNNEST(o.items) AS item` means the same. This is a **correlated** join: each order is joined only to its own items. Now aggregate across all items:

```sql
SELECT item.product, SUM(item.qty) AS units, SUM(item.qty * item.price) AS revenue
FROM shop.orders_nested AS o, UNNEST(o.items) AS item
GROUP BY item.product
ORDER BY revenue DESC;
-- cake | 4 | 80
-- tea  | 3 | 30
-- jam  | 2 | 12
```

[[fig:sql_unnest]]

> **Pitfall:** Order 105 has an empty array, so the `CROSS JOIN UNNEST` above drops it completely: there is nothing to pair it with. If every parent must survive (for "orders with zero items" or a correct order count), use `LEFT JOIN UNNEST(o.items) AS item`, which keeps order 105 with a null item.

**Testing array contents.** `WHERE 'promo' IN UNNEST(tags)` checks an array of plain values. For arrays of structs, use `EXISTS`:

```sql
SELECT order_id
FROM shop.orders_nested AS o
WHERE EXISTS (SELECT 1 FROM UNNEST(o.items) AS i WHERE i.product = 'jam');
-- 103
```

**Building nested data.** `ARRAY_AGG` collects rows back into an array, and `STRUCT` builds the elements. This turns the flat `shop.order_items` table into the nested shape:

```sql
SELECT
  order_id,
  ARRAY_AGG(STRUCT(product, qty, price) ORDER BY product) AS items
FROM shop.order_items
GROUP BY order_id;
```

**JSON.** When the shape is not known in advance (third-party webhooks, app events with free-form properties), store the document in a `JSON` column and pull fields out at query time. Suppose `shop.events.payload` holds:

```json
{"event": "purchase", "device": {"os": "ios", "version": "17.2"}, "tags": ["promo", "new"], "amount": "12.50"}
```

```sql
SELECT
  JSON_VALUE(payload, '$.device.os')               AS os,       -- 'ios' (a STRING)
  JSON_QUERY(payload, '$.device')                  AS device,   -- {"os":"ios","version":"17.2"} (JSON)
  JSON_VALUE_ARRAY(payload, '$.tags')              AS tags,     -- ['promo', 'new']
  CAST(JSON_VALUE(payload, '$.amount') AS NUMERIC) AS amount,   -- 12.5
  JSON_VALUE(payload, '$.coupon')                  AS coupon,   -- NULL: path not present
  STRING(payload.device.os)                        AS os_again  -- 'ios', using field access
FROM shop.events;
```

`JSON_VALUE` extracts a scalar as a `STRING`; `JSON_QUERY` extracts an object or array as JSON; the path syntax starts at `$`. Dot access (`payload.device.os`) returns JSON, and converter functions (`STRING`, `INT64`, `FLOAT64`, `BOOL`) turn it into a typed value; their `LAX_` versions return `NULL` or convert loosely instead of failing on a type mismatch. To load text that may be malformed, `SAFE.PARSE_JSON(text)` returns `NULL` for invalid JSON rather than failing the whole query.

> **Tip:** Keep the raw JSON in a landing table, but extract the fields people actually query into typed columns in the next layer. Typed columns are cheaper to scan, easier to test and self-documenting; JSON is for the long tail.

::: deeper
**How nested data is stored.** BigQuery's storage format, Capacitor, descends from the Dremel paper's **record shredding**: each leaf field (`items.product`) is stored as its own column, along with two small integers per value, the **repetition level** (which repeated field this value starts a new element of) and the **definition level** (how many optional or repeated ancestors are actually present, which encodes nulls and empty arrays). From those levels the engine can rebuild the nested records exactly without storing any join keys. Parquet uses the same scheme, which is why nested Parquet loads into BigQuery with its structure intact.

**Nesting versus joining.** Nesting removes a join and keeps child rows physically next to their parent, which is fast for "parent with children" queries. It costs flexibility: updating one item rewrites the parent row, and analysis that starts from the children ("all jam sales") must `UNNEST` every parent. Nest children that are owned by and read with the parent (order lines, event parameters); keep independent entities in their own tables.

**Limits worth knowing.** Arrays cannot directly contain arrays; wrap the inner one in a struct (`ARRAY<STRUCT<vals ARRAY<INT64>>>`). An array value cannot be `NULL` inside another array, and when written to a table a `NULL` array is stored as an empty array. Nesting depth for `STRUCT` and `RECORD` columns is limited to 15 levels.

**Older function names.** `JSON_EXTRACT` and `JSON_EXTRACT_SCALAR` are the older equivalents of `JSON_QUERY` and `JSON_VALUE` and still work on JSON-formatted strings; they use slightly different escaping rules for keys with special characters. Prefer the standard names in new code.

**Other engines.** Spark uses `explode` (drops empty arrays, like `CROSS JOIN UNNEST`) and `explode_outer` (keeps them, like `LEFT JOIN UNNEST`), plus `from_json` with a schema. PostgreSQL has `jsonb` with operators such as `->` and `->>`, `jsonb_array_elements` to unnest, and GIN indexes for containment queries. Snowflake has `VARIANT` and `LATERAL FLATTEN`. DuckDB and SQLite both have `->>` and table-valued JSON functions (`json_each` in SQLite).

> **Interview:** "A query joins orders to an unnested items array and then sums `order.amount`. What is wrong?" Fan-out: each order row is repeated once per item, so the order amount is counted several times. Sum item-level values after unnesting, or aggregate the items in a subquery per order before joining.
:::

::: check
?? An order has an empty `items` array. Does it appear in `FROM orders AS o, UNNEST(o.items) AS item`? How do you keep it?
No: a cross join with an empty array produces no rows for that order. Use `LEFT JOIN UNNEST(o.items) AS item`.

?? What is the difference between `JSON_VALUE` and `JSON_QUERY`?
`JSON_VALUE` returns a scalar (string, number, boolean) as a `STRING`; `JSON_QUERY` returns an object or array as JSON.

?? Why would you extract frequently queried JSON fields into typed columns?
Typed columns scan less data, can be partitioned, clustered and tested, give type errors early, and make the schema visible to every user.
:::

::: resources
- Docs: [Specify nested and repeated columns](https://docs.cloud.google.com/bigquery/docs/nested-repeated) · when and how to design `STRUCT` and `ARRAY` columns
- Docs: [Work with arrays](https://docs.cloud.google.com/bigquery/docs/arrays) · `UNNEST`, `ARRAY_AGG`, offsets and filtering, with examples
- Docs: [JSON functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/json_functions) · every extraction, conversion and construction function
- Docs: [Working with JSON data](https://docs.cloud.google.com/bigquery/docs/json-data) · loading, querying and the JSON type's limits
- Paper: [Dremel: Interactive Analysis of Web-Scale Datasets](https://research.google/pubs/dremel-interactive-analysis-of-web-scale-datasets-2/) · the original record shredding with repetition and definition levels
- Docs: [PostgreSQL: JSON types](https://www.postgresql.org/docs/current/datatype-json.html) · `json` vs `jsonb`, operators and indexing
:::

## Writing SQL

The first group gave you the vocabulary. This group is about writing SQL the way analytics and data engineering teams actually use it: answering business questions in one readable query, encoding warehouse design patterns, and generating SQL from templates when the same logic must be repeated across many tables.

The three topics build on each other. Advanced SQL for Analytics turns window functions, grouping extensions and pivots into reusable recipes (top-N, period-over-period, sessions, subtotals). Advanced SQL Modeling Patterns applies them to the tables a warehouse is made of: snapshots, bridge tables, hierarchies, late data and safe re-runnable loads. Jinja and templating then shows how tools such as dbt and Airflow produce that SQL automatically, and where generated SQL goes wrong.

### Advanced SQL for Analytics

::: plain
Most business questions are variations on a few shapes: "the top three per group", "this week compared with last week", "what share of the total", "subtotals for every region and a grand total", "turn these rows into columns". Analysts who know the recipe for each shape answer in minutes with one clear query; those who do not write five queries and glue the results together in a spreadsheet. This topic is a cookbook of those recipes.
:::

**Top-N per group.** Rank within each group and keep the first N. In BigQuery, `QUALIFY` keeps it to one statement:

```sql
SELECT customer_id, order_id, amount
FROM shop.orders
WHERE status = 'paid'
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) <= 2
ORDER BY customer_id, amount DESC;
-- 1 | 101 | 40.0
-- 1 | 103 | 22.0
-- 2 | 105 | 35.0
-- 3 | 104 | 60.0
-- 9 | 107 | 12.0
```

Choose `ROW_NUMBER` for exactly N rows, `RANK` or `DENSE_RANK` if ties should all be included.

**Period over period.** Aggregate first, then compare each period with the previous one using `LAG`. `SAFE_DIVIDE` returns `NULL` instead of an error when the previous value is zero:

```sql
WITH daily AS (
  SELECT order_date, SUM(amount) AS revenue
  FROM shop.orders
  WHERE status = 'paid'
  GROUP BY order_date
)
SELECT
  order_date,
  revenue,
  LAG(revenue) OVER w AS prev_day,
  ROUND(100 * SAFE_DIVIDE(revenue - LAG(revenue) OVER w, LAG(revenue) OVER w), 1) AS pct_change
FROM daily
WINDOW w AS (ORDER BY order_date)
ORDER BY order_date;
-- 2026-03-01 | 40.0 | NULL | NULL
-- 2026-03-02 | 22.0 | 40.0 | -45.0
-- 2026-03-03 | 60.0 | 22.0 | 172.7
-- 2026-03-05 | 35.0 | 60.0 | -41.7
-- 2026-03-08 | 30.0 | 35.0 | -14.3
```

Notice that 03-05 is compared with 03-03, not with an empty 03-04: `LAG` means "previous row", not "previous day". For true day-over-day comparisons, join to a date spine first (Dates, time zones and timestamps in SQL) so every day has a row.

**Share of total.** A window over an aggregate puts the grand total beside each group:

```sql
SELECT
  product,
  SUM(qty * price) AS revenue,
  ROUND(100 * SUM(qty * price) / SUM(SUM(qty * price)) OVER (), 1) AS pct_of_total
FROM shop.order_items
GROUP BY product
ORDER BY revenue DESC;
-- cake | 80.0 | 65.6
-- tea  | 30.0 | 24.6
-- jam  | 12.0 | 9.8
```

`SUM(SUM(...)) OVER ()` reads as: the inner `SUM` is the per-product aggregate; the outer `SUM ... OVER ()` adds those up across all groups.

**Sessions.** A user's clicks belong to the same session until there is a gap of more than 30 minutes. Flag rows that start a new session, then turn the flags into session numbers with a running sum:

```sql
WITH clicks AS (
  SELECT * FROM UNNEST([
    STRUCT('u1' AS user_id, TIMESTAMP '2026-03-08 10:00:00+00' AS ts),
    ('u1', TIMESTAMP '2026-03-08 10:10:00+00'),
    ('u1', TIMESTAMP '2026-03-08 11:00:00+00'),
    ('u1', TIMESTAMP '2026-03-08 11:20:00+00'),
    ('u2', TIMESTAMP '2026-03-08 10:05:00+00')
  ])
),
flagged AS (
  SELECT
    user_id,
    ts,
    IF(TIMESTAMP_DIFF(ts, LAG(ts) OVER (PARTITION BY user_id ORDER BY ts), MINUTE) <= 30,
       0, 1) AS new_session
  FROM clicks
)
SELECT user_id, ts, SUM(new_session) OVER (PARTITION BY user_id ORDER BY ts) AS session_no
FROM flagged
ORDER BY user_id, ts;
-- u1 | 10:00 | 1
-- u1 | 10:10 | 1
-- u1 | 11:00 | 2      (50 minutes after the previous click)
-- u1 | 11:20 | 2
-- u2 | 10:05 | 1
```

The first click of each user has no previous row, so `TIMESTAMP_DIFF` is `NULL`, the condition is not true, and `IF` returns 1: a new session. The `UNNEST([STRUCT(...), ...])` construction is BigQuery's way of writing a small inline table, handy for tests.

**Subtotals with ROLLUP, CUBE and GROUPING SETS.** A report often needs totals at several levels: per city and status, per city, and overall. `GROUP BY ROLLUP` produces all of them in one pass:

```sql
SELECT c.city, o.status, SUM(o.amount) AS revenue, GROUPING(o.status) AS is_status_total
FROM shop.orders AS o
JOIN shop.customers AS c USING (customer_id)
GROUP BY ROLLUP (c.city, o.status)
ORDER BY c.city, o.status;
-- NULL | NULL      | 190.5 | 1     grand total
-- Oslo | NULL      | 50.5  | 1     Oslo subtotal
-- Oslo | cancelled | 15.5  | 0
-- Oslo | paid      | 35.0  | 0
-- Pune | NULL      | 140.0 | 1     Pune subtotal
-- Pune | paid      | 140.0 | 0
```

`ROLLUP (a, b)` means the grouping sets `(a, b)`, `(a)` and `()`. `CUBE (a, b)` adds `(b)` as well, giving every combination (here two more rows: all cities' `cancelled` 15.5 and `paid` 175.0). `GROUPING SETS ((a, b), (b))` lists exactly the levels you want. In subtotal rows the rolled-up columns are `NULL`; `GROUPING(col)` returns 1 in those rows, which distinguishes "subtotal" from a real `NULL` value in the data.

**PIVOT and UNPIVOT.** `PIVOT` turns values of one column into columns; `UNPIVOT` does the reverse:

```sql
SELECT *
FROM (
  SELECT c.city, o.status, o.amount
  FROM shop.orders AS o
  JOIN shop.customers AS c USING (customer_id)
)
PIVOT (SUM(amount) FOR status IN ('paid' AS paid, 'cancelled' AS cancelled))
ORDER BY city;
-- Oslo | 35.0  | 15.5
-- Pune | 140.0 | NULL
```

Every input column that is not aggregated and not the `FOR` column becomes a grouping column, which is why the input is narrowed to three columns in a subquery first. The portable equivalent, which works in every engine, is conditional aggregation: `SUM(IF(status = 'paid', amount, NULL)) AS paid`. `UNPIVOT (amount FOR status IN (paid, cancelled))` turns the wide result back into rows, dropping nulls unless you write `UNPIVOT INCLUDE NULLS`.

**Lateral joins: "for each row, run this subquery".** PostgreSQL's `LATERAL` lets a subquery in `FROM` refer to columns of tables before it, for example "each customer's two latest orders":

```sql
-- PostgreSQL
SELECT c.name, latest.order_id
FROM customers AS c
LEFT JOIN LATERAL (
  SELECT o.order_id
  FROM orders AS o
  WHERE o.customer_id = c.customer_id
  ORDER BY o.order_date DESC
  LIMIT 2
) AS latest ON true;
-- Asha 106, Asha 103, Bo 105, Bo 102, Chen 104, Dana NULL
```

BigQuery has no `LATERAL` keyword; its correlated joins must go through an array. Build the array with a correlated `ARRAY(SELECT AS STRUCT ...)` subquery and `UNNEST` it, or simply use `QUALIFY ROW_NUMBER() ... <= 2` as in the top-N recipe:

```sql
SELECT c.name, latest.order_id
FROM shop.customers AS c
LEFT JOIN UNNEST(ARRAY(
  SELECT AS STRUCT o.order_id
  FROM shop.orders AS o
  WHERE o.customer_id = c.customer_id
  ORDER BY o.order_date DESC
  LIMIT 2
)) AS latest;
```

> **Tip:** Optimise analytics SQL in this order: read fewer columns (never `SELECT *` on wide tables), filter early (especially on partition columns), aggregate before joining large tables, and only then worry about clever rewrites. `APPROX_COUNT_DISTINCT(user_id)` is a fast, memory-bounded estimate (based on HyperLogLog++) when an exact distinct count is not required.

::: deeper
**FILTER and conditional aggregates.** Standard SQL's `COUNT(*) FILTER (WHERE status = 'paid')` (PostgreSQL, DuckDB, SQLite) is the cleanest conditional aggregate. BigQuery does not support `FILTER`; use `COUNTIF(status = 'paid')` and `SUM(IF(cond, x, NULL))`.

**How ROLLUP executes.** An engine can compute `ROLLUP (a, b)` by aggregating at the finest level `(a, b)` once and then re-aggregating that small result for the coarser levels, so it is much cheaper than `UNION ALL` of three separate scans. That only works for aggregates that combine correctly (`SUM`, `COUNT`, `MIN`, `MAX`); `COUNT(DISTINCT x)` subtotals must be computed from the raw rows, which is why distinct counts in cubes are expensive. Pre-aggregated "sketches" (HyperLogLog, BigQuery's `HLL_COUNT` functions) make distinct counts combinable.

**Percentiles.** `PERCENTILE_CONT(x, 0.5) OVER ()` in BigQuery is an analytic (window) function, not an aggregate; for a per-group median as an aggregate, use `APPROX_QUANTILES(x, 100)[OFFSET(50)]`. Exact percentiles need a full sort of each group, which is slow on billions of rows.

**Funnels and retention.** A funnel ("viewed, then added to cart, then paid") is a conditional aggregate per user over ordered events, often with `MIN(IF(event = 'paid', ts, NULL)) > MIN(IF(event = 'cart', ts, NULL))`. A cohort retention table groups users by first-activity month (`MIN(...) OVER (PARTITION BY user_id)`) and counts how many are active N months later, then pivots months into columns. Both are compositions of the recipes above. The Analytics engineering and BI section builds them in full.

**Readability.** BigQuery's pipe syntax (`FROM t |> WHERE ... |> AGGREGATE ... GROUP BY ...`) writes queries in execution order, which many people find easier for long analytical chains. It compiles to the same plan as the standard form.

> **Interview:** "Compute a 7-day rolling average of daily revenue." Aggregate to one row per day, join to a date spine so missing days are zero, then `AVG(revenue) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`. Mention that without the spine, `ROWS` counts rows, not days, and the average silently covers more than seven days.
:::

::: check
?? Which rows does `GROUP BY ROLLUP (region, product)` produce?
One row per (region, product), one subtotal per region (product `NULL`), and one grand total (both `NULL`): the grouping sets `(region, product)`, `(region)` and `()`.

?? Why is `LAG(revenue) OVER (ORDER BY day)` not always "yesterday's revenue"?
`LAG` returns the previous row. If days with no sales are missing from the input, the previous row may be several days earlier. Join to a date spine first.

?? How do you write "each customer's two most recent orders" in BigQuery?
`QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) <= 2`, or a correlated `ARRAY(SELECT AS STRUCT ... LIMIT 2)` unnested with `LEFT JOIN UNNEST`.
:::

::: resources
- Docs: [GROUP BY ROLLUP, CUBE and GROUPING SETS](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_rollup) · BigQuery's grouping extensions with worked examples
- Docs: [Aggregate functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions) · `COUNTIF`, `ARRAY_AGG`, `STRING_AGG`, `GROUPING` and more
- Docs: [PostgreSQL: table expressions and LATERAL](https://www.postgresql.org/docs/current/queries-table-expressions.html) · lateral subqueries, grouping sets and the `FILTER` clause
- Book: [SQL Performance Explained](https://sql-performance-explained.com/) · Markus Winand's short book on how indexes and query shape affect speed
- Article: [Mode SQL Tutorial: Advanced SQL](https://mode.com/sql-tutorial/intro-to-advanced-sql/) · free lessons on window functions, pivots and performance
- Course: [Window Functions (windowfunctions.com)](https://www.windowfunctions.com/) · practice problems for ranking, `LAG`/`LEAD` and frames
:::

### Advanced SQL Modeling Patterns

::: plain
A warehouse is built from a handful of recurring table shapes, each with its own SQL recipe. Some tables photograph the business every day (snapshots); some follow one order through its life and fill in each milestone as it happens; some connect many-to-many relationships; some flatten family trees. And every load must be safe to run twice, because pipelines fail and get re-run. This topic is the SQL behind those shapes. The Data modeling section explains when to choose each one; here you learn to build them.
:::

**Joining facts to a type 2 dimension (point-in-time joins).** A **slowly changing dimension type 2** (SCD2) keeps every version of a record, each with a validity interval. Asha moved from Pune to Mumbai on 5 March:

```text
shop.dim_customer
customer_key | customer_id | city   | valid_from | valid_to   | is_current
11           | 1           | Pune   | 2026-01-05 | 2026-03-05 | FALSE
12           | 1           | Mumbai | 2026-03-05 | 9999-12-31 | TRUE
21           | 2           | Oslo   | 2026-01-20 | 9999-12-31 | TRUE
31           | 3           | Pune   | 2026-02-11 | 9999-12-31 | TRUE
```

To attribute each order to the city the customer lived in **when they ordered**, join on the key and on the date falling inside the interval:

```sql
SELECT d.city, SUM(o.amount) AS revenue
FROM shop.orders AS o
JOIN shop.dim_customer AS d
  ON  o.customer_id = d.customer_id
  AND o.order_date >= d.valid_from
  AND o.order_date <  d.valid_to
WHERE o.status = 'paid'
GROUP BY d.city
ORDER BY revenue DESC;
-- Pune   | 122.0     (Asha's two early orders plus Chen)
-- Oslo   | 35.0
-- Mumbai | 18.0      (Asha's order after the move)
```

Joining on `is_current` instead would give Mumbai 80.0 and Pune 60.0: history rewritten as if Asha had always lived in Mumbai. The intervals are **half-open** (`valid_from` included, `valid_to` excluded) so a date on the boundary matches exactly one version, and the current row uses a far-future `valid_to` so no `NULL` handling is needed. In a real star schema you would resolve `customer_key` once, when loading the fact table, and store it on the fact row.

**Late-arriving dimensions.** Order 107 arrived for customer 9 before customer 9 reached the warehouse. Two standard answers:

1. Point the fact at a single **unknown member** row (for example `customer_key = -1`, city 'unknown') and accept that the order is never re-attributed.
2. Insert an **inferred member**: a placeholder dimension row for the new key, flagged so it can be completed later.

```sql
INSERT INTO shop.dim_customer
  (customer_key, customer_id, city, valid_from, valid_to, is_current, is_inferred)
SELECT DISTINCT
  FARM_FINGERPRINT(CONCAT(CAST(o.customer_id AS STRING), '|inferred')),
  o.customer_id, 'unknown', DATE '1900-01-01', DATE '9999-12-31', TRUE, TRUE
FROM shop.orders AS o
WHERE NOT EXISTS (
  SELECT 1 FROM shop.dim_customer AS d WHERE d.customer_id = o.customer_id
);
```

When the real customer 9 arrives, the loader overwrites the inferred row in place (and clears `is_inferred`) rather than creating a second version, so facts already pointing at it pick up the correct attributes. `FARM_FINGERPRINT` turns a string into a stable 64-bit integer, a common way to make deterministic surrogate keys in BigQuery.

**Periodic snapshots.** A periodic snapshot table has one row per entity per period ("each customer's lifetime revenue at the end of each day"), so questions about state over time become simple filters. Build it by crossing a date spine with the entities and running a cumulative sum:

```sql
WITH days AS (
  SELECT d FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2026-03-01', DATE '2026-03-04')) AS d
),
daily AS (
  SELECT customer_id, order_date, SUM(amount) AS revenue
  FROM shop.orders
  WHERE status = 'paid' AND customer_id IN (1, 3)
  GROUP BY customer_id, order_date
),
grid AS (
  SELECT c.customer_id, days.d AS snapshot_date
  FROM (SELECT DISTINCT customer_id FROM daily) AS c
  CROSS JOIN days
)
SELECT
  g.snapshot_date,
  g.customer_id,
  SUM(COALESCE(x.revenue, 0)) OVER (PARTITION BY g.customer_id ORDER BY g.snapshot_date)
    AS lifetime_revenue
FROM grid AS g
LEFT JOIN daily AS x
  ON x.customer_id = g.customer_id AND x.order_date = g.snapshot_date
ORDER BY g.snapshot_date, g.customer_id;
-- 03-01 | 1 | 40.0     03-01 | 3 | 0
-- 03-02 | 1 | 62.0     03-02 | 3 | 0
-- 03-03 | 1 | 62.0     03-03 | 3 | 60.0
-- 03-04 | 1 | 62.0     03-04 | 3 | 60.0
```

In production the daily job appends only the newest day (`INSERT ... SELECT @run_date, ...`), computed from the previous snapshot plus that day's activity, rather than rebuilding history each time. Snapshots are **semi-additive**: you can sum lifetime revenue across customers on one day, but never across days.

**Accumulating snapshots.** One row per process instance (an order), with a column per milestone, updated as each milestone happens. A `MERGE` from the recent events keeps it current:

```sql
MERGE shop.fct_order_pipeline AS t
USING (
  SELECT
    order_id,
    MIN(IF(event = 'placed',    event_ts, NULL)) AS placed_at,
    MIN(IF(event = 'paid',      event_ts, NULL)) AS paid_at,
    MIN(IF(event = 'shipped',   event_ts, NULL)) AS shipped_at,
    MIN(IF(event = 'delivered', event_ts, NULL)) AS delivered_at
  FROM shop.order_events
  WHERE event_date >= DATE_SUB(@run_date, INTERVAL 3 DAY)
  GROUP BY order_id
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  placed_at    = COALESCE(t.placed_at, s.placed_at),
  paid_at      = COALESCE(t.paid_at, s.paid_at),
  shipped_at   = COALESCE(t.shipped_at, s.shipped_at),
  delivered_at = COALESCE(t.delivered_at, s.delivered_at)
WHEN NOT MATCHED THEN INSERT (order_id, placed_at, paid_at, shipped_at, delivered_at)
  VALUES (s.order_id, s.placed_at, s.paid_at, s.shipped_at, s.delivered_at);
```

Lag measures such as `TIMESTAMP_DIFF(shipped_at, paid_at, HOUR)` then come from one row, with no self-joins. `COALESCE(t.x, s.x)` keeps the first value seen, so re-running the job changes nothing.

**Bridge tables for many-to-many.** A bank account can have several owners, and an owner several accounts. A **bridge table** sits between the fact and the dimension with one row per pair, plus a **weighting factor** that sums to 1 per account:

```text
account_balance        bridge_account_owner                    
account_id | balance   account_id | customer_id | weight       
A1         | 1000      A1         | 1 (Asha)    | 0.5
A2         | 600       A1         | 2 (Bo)      | 0.5
                       A2         | 3 (Chen)    | 1.0
```

`SUM(balance * weight)` grouped by owner gives Asha 500, Bo 500 and Chen 600, which add up to the true total of 1,600. `SUM(balance)` grouped by owner gives 1,000, 1,000 and 600: correct per owner ("balances I can access") but 2,600 in total. Both are valid answers to different questions; label them clearly.

**Flattening hierarchies.** Recursive CTEs are flexible but awkward for BI tools. Precompute a **closure table** with one row per (ancestor, descendant) pair, including each person with themself at depth 0:

```sql
WITH RECURSIVE closure AS (
  SELECT emp_id AS ancestor_id, emp_id AS descendant_id, 0 AS depth
  FROM shop.employees
  UNION ALL
  SELECT c.ancestor_id, e.emp_id, c.depth + 1
  FROM closure AS c
  JOIN shop.employees AS e ON e.manager_id = c.descendant_id
)
SELECT a.name AS manager, COUNT(*) - 1 AS people_below
FROM closure AS c
JOIN shop.employees AS a ON a.emp_id = c.ancestor_id
GROUP BY a.name
ORDER BY people_below DESC, manager;
-- Ravi | 4
-- Mei  | 2
-- Ana  | 1
-- Lee  | 0
-- Tom  | 0
```

The five-person chart has 12 closure rows. "Total sales of everyone under Mei" becomes an ordinary join on `ancestor_id`, at any depth.

**Idempotent loads.** A pipeline step is **idempotent** when running it twice leaves the same result as running it once. Three patterns, from most to least common:

1. **MERGE on a business key**, with the source deduplicated first. BigQuery's `MERGE` fails if several source rows match one target row, so deduplicate with `QUALIFY ROW_NUMBER() ... = 1` in the `USING` subquery.
2. **Replace a whole partition**: delete the run's date and insert it again inside one transaction, or write the query result to the partition `shop.daily_sales$20260308` with `WRITE_TRUNCATE`.
3. **Insert only new keys** with `INSERT ... WHERE NOT EXISTS`, for append-only facts.

```sql
BEGIN TRANSACTION;
DELETE FROM shop.daily_sales WHERE sale_date = @run_date;
INSERT INTO shop.daily_sales (sale_date, customer_id, revenue)
SELECT order_date, customer_id, SUM(amount)
FROM shop.orders
WHERE status = 'paid' AND order_date = @run_date
GROUP BY order_date, customer_id;
COMMIT TRANSACTION;
```

> **Pitfall:** Values that change on every run break idempotency: `CURRENT_TIMESTAMP()` in a key, `GENERATE_UUID()` as a surrogate key, or "load everything since the last run" based on the wall clock. Derive keys from the data (`FARM_FINGERPRINT` of the business key) and drive incremental windows from parameters the orchestrator passes in.

::: deeper
**Building SCD2 with MERGE.** One `MERGE` can close the current version and insert the new one if you feed it each changed row twice: once with the real key (matches and closes the old row) and once with a `NULL` join key (never matches, so it falls into `WHEN NOT MATCHED` and inserts the new version). dbt snapshots generate this kind of SQL for you.

**Range joins are expensive.** The point-in-time join has an inequality condition. Engines hash-join on the equality part (`customer_id`) and then filter the range, which is fine when each key has few versions. With thousands of versions per key (prices that change every minute), resolve keys at load time, or use an "as of" join where the engine supports one (DuckDB's `ASOF JOIN`, Spark's and Snowflake's `ASOF` variants).

**Snapshots grow fast.** A daily snapshot of 10 million accounts adds 3.65 billion rows a year. Partition by snapshot date, cluster by the entity key, keep daily granularity for a limited window, and keep month-end snapshots beyond that.

**Late facts, not just late dimensions.** Facts that arrive days late must land in the partition of their event date, so incremental jobs re-process a lookback window (the three days in the accumulating snapshot example) and the target must tolerate re-processing, which is exactly what idempotent `MERGE` gives you.

> **Interview:** "How do you make a daily load safe to re-run?" Scope each run to a partition or key range given by a parameter, replace that scope atomically (`MERGE` on a key or delete-insert in a transaction), deduplicate the source deterministically, and avoid run-time values in keys. Then retries and backfills are just re-runs.
:::

::: check
?? Why use half-open intervals (`valid_from <= d < valid_to`) in SCD2 dimensions?
A date on the boundary between two versions then matches exactly one of them; with closed intervals it would match both and duplicate the fact.

?? A bridge table links accounts to owners. Why does summing balance by owner overstate the total, and how do you fix it?
A jointly owned account is counted once per owner. Multiply by a weighting factor that sums to 1 per account when you need totals that add up.

?? Why must the source of a BigQuery `MERGE` be deduplicated?
If two source rows match the same target row, BigQuery cannot decide which update wins and the statement fails. Deduplicate deterministically (for example the latest version per key) before merging.
:::

::: resources
- Docs: [Kimball dimensional modeling techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) · short official definitions of every pattern here, from bridge tables to late-arriving dimensions
- Docs: [Kimball Group: fact table design tips](https://www.kimballgroup.com/2008/11/fact-tables/) · transaction, periodic and accumulating snapshot fact tables compared
- Docs: [DML statements in GoogleSQL](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax) · `MERGE`, `INSERT`, `DELETE` and their rules in BigQuery
- Book: [The Data Warehouse Toolkit, 3rd edition](https://www.wiley.com/en-us/The+Data+Warehouse+Toolkit%3A+The+Definitive+Guide+to+Dimensional+Modeling%2C+3rd+Edition-p-9781118530801) · Kimball and Ross, the reference for these patterns with case studies
- Blog: [How to join a fact and an SCD2 dimension](https://www.startdataengineering.com/post/how-to-join-fact-scd2-tables/) · Start Data Engineering's walkthrough of point-in-time joins
- Docs: [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots) · how dbt builds SCD2 tables automatically
:::

### Jinja & Templating in Data

::: plain
Imagine writing the same letter to two hundred customers, changing only the name and the amount. You would write one template with blanks and let a program fill them in. **Jinja** is that template language for SQL: you write SQL with placeholders, loops and if-statements, and a tool such as dbt or Airflow fills them in to produce plain SQL, which then runs on the warehouse. The warehouse never sees the template; it only sees the finished SQL.
:::

Jinja is a Python template engine. It has three kinds of markup:

- `{{ ... }}` prints the value of an expression: `{{ ds }}` becomes `2026-03-08`.
- `{% ... %}` runs a statement: `{% if %}`, `{% for %}`, `{% set %}`, `{% macro %}`.
- `{# ... #}` is a comment that disappears from the output.

The idea in three lines of Python (Jinja is a third-party package: `python -m pip install jinja2`):

```python
from jinja2 import Template
t = Template("select * from {{ table }} where order_date = '{{ ds }}'")
print(t.render(table="shop.orders", ds="2026-03-08"))
# select * from shop.orders where order_date = '2026-03-08'
```

Jinja knows nothing about SQL. It only produces text, so a template that renders without errors can still produce broken or wrong SQL. Always look at the **compiled** output.

**Jinja in dbt.** dbt (data build tool) compiles every model (a `SELECT` in a `.sql` file) through Jinja before running it. The most important functions are `ref` and `source`, which resolve table names and record dependencies so dbt knows the build order:

```sql
-- models/marts/customer_revenue.sql
{{ config(materialized='table') }}

select
  c.customer_id,
  c.name,
  sum(o.amount) as revenue
from {{ ref('stg_customers') }} as c
join {{ ref('stg_orders') }} as o using (customer_id)
where o.status = 'paid'
group by 1, 2
```

`{{ ref('stg_orders') }}` compiles to the full table name in the current environment, such as `` `my-project`.`dbt_dev_asha`.`stg_orders` `` in development and the production dataset in production. That is why you never hard-code table names in dbt.

**Loops and conditionals** remove repetition. Pivoting statuses into columns:

```sql
{% set statuses = ['paid', 'cancelled', 'refunded'] %}

select
  customer_id,
  {%- for s in statuses %}
  sum(case when status = '{{ s }}' then amount else 0 end) as {{ s }}_amount
  {%- if not loop.last %},{% endif %}
  {%- endfor %}
from {{ ref('stg_orders') }}
group by customer_id
```

compiles to:

```sql
select
  customer_id,
  sum(case when status = 'paid' then amount else 0 end) as paid_amount,
  sum(case when status = 'cancelled' then amount else 0 end) as cancelled_amount,
  sum(case when status = 'refunded' then amount else 0 end) as refunded_amount
from `my-project`.`analytics`.`stg_orders`
group by customer_id
```

The `-` in `{%-` strips the whitespace before the tag, which keeps the output tidy; `loop.last` is true on the final iteration, so no trailing comma is written.

**Macros** are reusable functions that return SQL text:

```sql
-- macros/cents_to_currency.sql
{% macro cents_to_currency(column, scale=2) %}
  round({{ column }} / 100, {{ scale }})
{% endmacro %}

-- in a model
select order_id, {{ cents_to_currency('amount_cents') }} as amount
from {{ ref('stg_payments') }}
-- compiles to: select order_id, round(amount_cents / 100, 2) as amount from ...
```

**Incremental models** use a conditional to load only new rows after the first run:

```sql
{{ config(materialized='incremental', unique_key='order_id') }}

select order_id, customer_id, order_date, amount, updated_at
from {{ ref('stg_orders') }}
{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

On the first run (or with `--full-refresh`) the `where` clause is omitted and the whole table is built. On later runs `is_incremental()` is true, `{{ this }}` refers to the existing table, and dbt merges the new rows on `order_id`.

**Cross-database macros with `adapter.dispatch`.** Packages that must run on BigQuery, Snowflake and PostgreSQL pick an implementation per adapter:

```sql
{% macro safe_div(a, b) %}
  {{ return(adapter.dispatch('safe_div')(a, b)) }}
{% endmacro %}

{% macro default__safe_div(a, b) %}
  case when {{ b }} = 0 then null else {{ a }} / {{ b }} end
{% endmacro %}

{% macro bigquery__safe_div(a, b) %}
  safe_divide({{ a }}, {{ b }})
{% endmacro %}
```

dbt looks for `bigquery__safe_div` when running on BigQuery and falls back to `default__safe_div` elsewhere. Packages such as dbt-utils use this throughout.

**Jinja in Airflow.** Airflow renders templated operator fields with Jinja just before a task runs, using the run's context. The most used variables are `{{ ds }}` (the logical date as `YYYY-MM-DD`), `{{ ds_nodash }}` (`YYYYMMDD`), `{{ data_interval_start }}` and `{{ data_interval_end }}`, plus helpers such as `{{ macros.ds_add(ds, -1) }}`:

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

load_day = BigQueryInsertJobOperator(
    task_id="load_daily_sales",
    configuration={
        "query": {
            "query": """
                DELETE FROM shop.daily_sales WHERE sale_date = '{{ ds }}';
                INSERT INTO shop.daily_sales
                SELECT order_date, customer_id, SUM(amount)
                FROM shop.orders
                WHERE status = 'paid' AND order_date = '{{ ds }}'
                GROUP BY order_date, customer_id;
            """,
            "useLegacySql": False,
        }
    },
)
```

Because the date comes from the run, not from the clock, re-running or backfilling the 8 March run always processes 8 March. On Google Cloud this runs in Managed Service for Apache Airflow (formerly Cloud Composer), the managed Airflow service.

> **Note:** Dataform, Google Cloud's built-in SQL workflow tool for BigQuery, solves the same problem with SQLX files and JavaScript instead of Jinja: `${ref("stg_orders")}` plays the role of `{{ ref('stg_orders') }}`. The ideas in this topic carry over directly.

> **Pitfall:** Never render values that come from users or external systems straight into SQL with Jinja. Templating is string concatenation, so it is open to SQL injection exactly like an f-string. Use query parameters (`@run_date` in BigQuery) for data values, and templating only for trusted structure such as table names.

::: deeper
**Parse time versus run time in dbt.** dbt renders every model twice: once while **parsing** the project (to find `ref` calls and build the dependency graph) and once while **executing**. Macros that query the warehouse (`run_query`, `dbt_utils.get_column_values`) return nothing useful at parse time, so wrap them in `{% if execute %}`. Values a model depends on must be knowable at parse time, which is why `ref` arguments must be literal strings.

**Custom materializations.** A materialization is the macro that turns a `SELECT` into a database object: `view`, `table`, `incremental`, `ephemeral` (inlined as a CTE) and `materialized_view` ship with dbt. You can write your own with `{% materialization name, adapter='bigquery' %} ... {% endmaterialization %}`: it runs pre-hooks, builds the relation with `statement` blocks, runs post-hooks and returns the relations it created. Teams use them for patterns such as "insert-overwrite the partitions in this run" with extra auditing. They are powerful and hard to test, so reach for configuration of existing materializations first (the BigQuery adapter's `incremental_strategy='insert_overwrite'` with `partition_by` already covers partition replacement).

**Anti-patterns.**

- **Macro soup**: so many layers of macros that nobody can read the model without compiling it. Prefer plain SQL; extract a macro only when the same logic appears in three or more places.
- **Logic in Jinja that belongs in SQL**: looping over hundreds of values in Jinja to build a giant `CASE`, when a join to a mapping table would be shorter, faster and testable.
- **Hidden dependencies**: building table names from strings (`'shop.' ~ name`) instead of `ref`, so dbt's graph does not know about the dependency and runs things in the wrong order.
- **Whitespace and comments**: a `--` comment inside a loop can comment out the rest of a generated line. Use `{# #}` for template comments.
- **Not reading compiled SQL**: `dbt compile` writes the rendered SQL to `target/compiled/`; read it when anything looks wrong.

**Airflow rendering details.** Only fields listed in an operator's `template_fields` are rendered; a templated string in any other argument stays literal. You can render a `.sql` file by passing its path if the extension is in `template_ext`. Use `params` for static values and Airflow Variables sparingly, because each lookup can hit the metadata database at parse time if used outside templates.

> **Interview:** "How would you avoid copy-pasting the same 40-line deduplication logic into 30 dbt models?" A macro that takes the relation, key and ordering columns and returns the `QUALIFY ROW_NUMBER()` query, tested once with dbt unit tests, plus documentation. Mention when not to: if the 30 sources really differ, a shared macro with many flags is worse than 30 clear models.
:::

::: check
?? What does `{{ ref('stg_orders') }}` do besides inserting a table name?
It records a dependency in dbt's graph, so `stg_orders` is built first, and it resolves to the right dataset for the current environment (development or production).

?? In an Airflow task that runs at 01:00 on 9 March for the daily interval of 8 March, what does `{{ ds }}` render to, and why use it instead of `CURRENT_DATE()`?
`2026-03-08`, the logical date of the run. It stays the same on retries and backfills, while `CURRENT_DATE()` depends on when the task happens to run.

?? Why is it dangerous to put a user-supplied value into SQL through Jinja?
Jinja is plain text substitution, so a crafted value can change the SQL (SQL injection). Pass data values as query parameters.
:::

::: resources
- Docs: [Jinja and macros in dbt](https://docs.getdbt.com/docs/build/jinja-macros) · `ref`, `source`, control structures and writing macros
- Docs: [dispatch in dbt](https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch) · how `adapter.dispatch` chooses an implementation per warehouse
- Docs: [Create new materializations](https://docs.getdbt.com/guides/create-new-materializations) · the anatomy of a custom materialization
- Docs: [Jinja template designer documentation](https://jinja.palletsprojects.com/en/stable/templates/) · the full template language: filters, tests, loops and whitespace control
- Docs: [Airflow templates reference](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html) · every built-in variable and macro available in templated fields
- Repo: [dbt-utils](https://github.com/dbt-labs/dbt-utils) · widely used macros; reading them is the best way to learn idiomatic dbt Jinja
- Docs: [Dataform overview](https://docs.cloud.google.com/dataform/docs/overview) · Google Cloud's SQLX-based alternative to dbt for BigQuery
:::

## Making SQL fast and correct

Everything so far was about saying the right thing. This group is about what happens after you press Run: how the database decides to execute your query, how to read that decision, and the handful of mistakes that make SQL slow, wrong or unsafe. The skills transfer across engines. PostgreSQL is used for the plans because its `EXPLAIN` output is the clearest teaching tool and it is the database behind Cloud SQL and AlloyDB; SQLite gives you runnable experiments on your laptop; BigQuery's own plans and tuning are covered in BigQuery in depth.

The two topics pair up. The first teaches you to read a plan and understand the optimizer's reasoning; the second is a catalogue of anti-patterns, each of which you can now recognise in a plan or a result, plus a method for debugging SQL that returns the wrong answer.

### SQL Query Optimization & Execution Plans

::: plain
When you give a database a query, a planner works out how to answer it, like a sat-nav choosing a route. Should it read the whole table or jump to the right rows using an index (like a book's index)? Which table should it join first, and how? It estimates the cost of many possible routes and picks the cheapest. An **execution plan** is that chosen route, printed out. Reading it tells you why a query is slow, and the most common reason is that the planner's estimates were wrong.
:::

**What an index does.** Without an index, finding one customer's orders means reading every row: a **full scan**. A **B-tree index** keeps the values of a column sorted in a tree with pointers to the rows, so the database can jump straight to `customer_id = 42`. You can watch the plan change in SQLite, using only Python's standard library:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TEXT, amount REAL)")
db.executemany(
    "INSERT INTO orders VALUES (?, ?, ?, ?)",
    [(i, i % 1000, f"2026-03-{i % 28 + 1:02d}", i % 50) for i in range(100_000)],
)

def plan(sql):
    for row in db.execute("EXPLAIN QUERY PLAN " + sql):
        print("  ", row[-1])

q = "SELECT SUM(amount) FROM orders WHERE customer_id = 42"
plan(q)                       # SCAN orders
db.execute("CREATE INDEX idx_orders_customer ON orders (customer_id)")
plan(q)                       # SEARCH orders USING INDEX idx_orders_customer (customer_id=?)
print(db.execute(q).fetchone())   # (4200.0,)
```

Before the index the plan is `SCAN` (read everything); after it, `SEARCH ... USING INDEX` (jump to the 100 matching rows). The answer is the same; only the route changed.

**Reading a PostgreSQL plan.** `EXPLAIN` shows the plan the planner chose with its estimates; `EXPLAIN ANALYZE` also **runs** the query and adds what really happened. A plan is a tree: each node reads rows from its children, does one job (scan, filter, join, sort, aggregate) and passes rows up. Read it from the most indented lines (which run first) outwards. An abridged example for "paid revenue per city" (your numbers will differ):

```text
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.city, SUM(o.amount) FROM orders o JOIN customers c USING (customer_id)
WHERE o.status = 'paid' GROUP BY c.city;

HashAggregate  (cost=2702.21..2702.26 rows=5 width=40) (actual time=31.2..31.2 rows=5 loops=1)
  Group Key: c.city
  ->  Hash Join  (cost=30.50..2214.71 rows=97500 width=14) (actual time=0.4..22.9 rows=97512 loops=1)
        Hash Cond: (o.customer_id = c.customer_id)
        ->  Seq Scan on orders o  (cost=0.00..2084.00 rows=97500 width=14) (actual time=0.0..11.6 rows=97512 loops=1)
              Filter: (status = 'paid'::text)
              Rows Removed by Filter: 2488
        ->  Hash  (cost=18.00..18.00 rows=1000 width=12) (actual time=0.3..0.3 rows=1000 loops=1)
              ->  Seq Scan on customers c  (cost=0.00..18.00 rows=1000 width=12) (actual ...)
```

How to read each node:

- `cost=start..total` is in arbitrary planner units. The first number is the cost before the first row can be returned; the second, the cost to return all rows. Costs include the children.
- `rows` is the **estimated** row count; `actual ... rows` is the real one. `loops` says how many times the node ran (multiply by it).
- `Filter` and `Rows Removed by Filter` show work spent reading rows that were thrown away.
- With `BUFFERS`, `shared hit` counts pages found in memory and `read` pages fetched from disk.

[[fig:sql_plan]]

**Where the cost numbers come from.** PostgreSQL's cost model charges for pages read and rows processed, with settings you can inspect: `seq_page_cost` (1.0) per page read sequentially, `random_page_cost` (4.0) per page read in random order, `cpu_tuple_cost` (0.01) per row, `cpu_index_tuple_cost` (0.005) per index entry, and `cpu_operator_cost` (0.0025) per operator or function evaluated. Worked example: `orders` has 100,000 rows in 834 pages. A sequential scan costs 834 × 1.0 + 100,000 × 0.01 = 1,834. Adding the filter `status = 'paid'` charges one operator per row, 100,000 × 0.0025 = 250, for 2,084: exactly the total cost on the `Seq Scan` line above. An index scan that fetches a few rows costs a few random pages and is far cheaper; one that fetches half the table costs tens of thousands of random page reads, so the planner rightly prefers the sequential scan. Whether an index helps depends on **selectivity**: the fraction of rows that match.

**The three join algorithms.** Every engine, PostgreSQL to Spark to BigQuery, chooses among the same three:

| Algorithm | How it works | Good when | Watch for |
|---|---|---|---|
| Nested loop | For each outer row, look up matching inner rows (ideally through an index) | The outer side is small and the inner side has an index on the join key | Huge cost when the outer side is bigger than estimated |
| Hash join | Build a hash table from the smaller input, then stream the larger input and probe it | Equality joins on large, unsorted inputs | The build side must fit in memory (`work_mem`) or it spills to disk in batches |
| Merge join | Sort both inputs on the key (or read them already sorted) and walk them together | Both sides are already sorted, for example by an index | The sorts, if the inputs are not already ordered |

**Statistics drive everything.** The planner's row estimates come from statistics gathered by `ANALYZE`: for each column, the fraction of nulls, the number of distinct values, the most common values with their frequencies, and a histogram. When statistics are stale (a table grew from 1,000 to 10 million rows since the last `ANALYZE`) or a predicate is hard to estimate, estimates go wrong and the planner picks a bad route, typically a nested loop over far more rows than it expected. The single most useful diagnostic is to compare `rows` with `actual rows` on each node and find the first place, from the bottom, where they differ by ten times or more.

**Parallel plans.** PostgreSQL can split a large scan across worker processes. The plan then shows a `Gather` (or `Gather Merge`, which preserves order) node with `Workers Planned` and `Workers Launched`, above `Parallel Seq Scan`, `Parallel Hash Join` or partial aggregates. `max_parallel_workers_per_gather` (default 2) caps the workers per `Gather`.

> **Tip:** Change one thing at a time and measure with `EXPLAIN (ANALYZE, BUFFERS)`. Run it twice: the first run often reads from disk and the second from cache, and you should know which one you are comparing.

> **Pitfall:** `EXPLAIN ANALYZE` really executes the statement. On an `UPDATE` or `DELETE`, wrap it in `BEGIN; ... ROLLBACK;` or you will change the data.

::: deeper
**Index choices.** A B-tree on `(customer_id, order_date)` serves `WHERE customer_id = 42 AND order_date >= '2026-03-01'` well: equality columns first, then the range column. The reverse order would scan every customer's rows in the date range. An **index-only scan** answers from the index without visiting the table when the index contains every needed column (add them with `INCLUDE`) and the table's visibility map says the pages are all-visible, which depends on regular vacuuming. A **bitmap heap scan** collects matching row locations from one or more indexes, sorts them by page and reads each page once: the middle ground between index scans and full scans. Every index slows writes and uses space, so index for the queries you actually run.

**Statistics in detail.** Autovacuum runs `ANALYZE` on a table after about 10% of its rows plus 50 have changed (`autovacuum_analyze_scale_factor` and `autovacuum_analyze_threshold`), which can be far too rarely for a huge table that receives a steady trickle of new rows; lower the scale factor per table or run `ANALYZE` after bulk loads. `default_statistics_target` (100) controls histogram and most-common-value list sizes; raise it per column (`ALTER TABLE ... ALTER COLUMN ... SET STATISTICS 1000`) for skewed columns. The planner assumes columns are independent, so `city = 'Pune' AND pincode = '411001'` is badly underestimated; `CREATE STATISTICS` (PostgreSQL 10+) teaches it about correlated columns.

**Join order.** For n tables there are a factorial number of join orders. PostgreSQL searches them exhaustively up to `join_collapse_limit` and `from_collapse_limit` (8 by default), and beyond `geqo_threshold` (12 tables) switches to a genetic search that may miss the best plan. Distributed engines add a second decision: **broadcast** the small side to every worker, or **shuffle** both sides by the key. The Query engine internals section covers cost-based optimizers in depth.

**Other engines.** BigQuery has no B-tree indexes on ordinary columns; its levers are partition pruning, clustering, column selection and join order, and its plan (in the job's Execution details) shows stages with slot time, rows and bytes shuffled. Spark prints plans with `EXPLAIN FORMATTED` and changes them at run time with Adaptive Query Execution. MySQL has `EXPLAIN ANALYZE` (8.0.18+) with tree output. The reading skill is the same everywhere: find the step where time or rows explode, and ask why the engine chose it.

**Tools.** Paste PostgreSQL plans into a visualiser such as explain.dalibo.com (PEV2) or explain.depesz.com to see the slowest nodes and estimate errors highlighted. `auto_explain` logs plans of slow queries automatically, and `pg_stat_statements` finds which queries consume the most time in total, which is where tuning effort pays off.

> **Interview:** "A query that ran in 2 seconds now takes 10 minutes, and the code did not change. What do you check?" The plan: compare estimated and actual rows to find a misestimate, usually from stale statistics after data growth or a new skewed value, which flipped a hash join into a nested loop. Check when `ANALYZE` last ran, data volume changes, and parameter values (a generic cached plan chosen for a rare value).
:::

::: check
?? In an `EXPLAIN ANALYZE` node, estimated `rows=10` but actual `rows=2000000`. What is the likely consequence, and the first fix to try?
The planner chose an operator suited to 10 rows (often a nested loop), which is disastrous for two million. Refresh statistics with `ANALYZE`, then look at why the estimate is wrong (correlated columns, skew, a function in the predicate).

?? Why might the planner ignore an index on `status` for `WHERE status = 'paid'`?
If most rows are paid, the predicate is not selective; reading most of the table through random index lookups costs more than one sequential scan.

?? When does a hash join spill to disk, and how does it show in the plan?
When the hash table built from the smaller input exceeds `work_mem`. The `Hash` node reports more than one batch (`Batches: 8`), and disk reads increase.
:::

::: resources
- Docs: [PostgreSQL: Using EXPLAIN](https://www.postgresql.org/docs/current/using-explain.html) · the official guide to reading plans, costs and `EXPLAIN ANALYZE` output
- Docs: [PostgreSQL: statistics used by the planner](https://www.postgresql.org/docs/current/planner-stats.html) · what `ANALYZE` collects and extended statistics
- Docs: [PostgreSQL: query planning settings](https://www.postgresql.org/docs/current/runtime-config-query.html) · cost constants, join methods and parallel settings
- Book: [Use The Index, Luke](https://use-the-index-luke.com/) · free online book on indexes and execution plans across databases
- Book: [The Internals of PostgreSQL](https://www.interdb.jp/pg/) · free book by Hironobu Suzuki, with a chapter on query processing and the cost model
- Repo: [PEV2: PostgreSQL EXPLAIN visualizer](https://explain.dalibo.com/) · paste a plan and see the expensive nodes highlighted
- Docs: [SQLite EXPLAIN QUERY PLAN](https://www.sqlite.org/eqp.html) · how to read the plans in this topic's runnable example
:::

### SQL Anti-Patterns & Debugging

::: plain
Some SQL mistakes are like a leaking tap: the query still works, so nobody notices, but it quietly wastes money or slowly gives wrong answers. Others are like leaving the front door open: one bad input and anyone can read or delete your data. This topic is a field guide to the usual leaks and open doors, how to recognise each one, and a calm, step-by-step way to debug a query whose numbers look wrong.
:::

**1. Fan-out: joins that multiply rows.** The most common source of wrong numbers. Joining orders (one row per order) to order items (several rows per order) and then summing the order amount counts each order once per item:

```sql
SELECT SUM(o.amount) AS wrong_revenue, SUM(i.qty * i.price) AS item_revenue
FROM shop.orders AS o
JOIN shop.order_items AS i ON i.order_id = o.order_id;
-- 184.0 | 122.0
```

The true revenue of orders 101, 103 and 104 is 122.0; orders 101 and 103 each have two items, so their amounts were added twice. Fix it by aggregating the many side to the grain of the one side before joining, or by summing only measures that live at the joined grain. A related explosion happens when **both** sides have duplicates on the join key: 1,000 rows per key on each side produce a million rows per key, and the query that used to take seconds runs for an hour.

**2. N+1 queries.** Application and pipeline code that fetches a list and then runs one more query per item sends N+1 round trips where one would do:

```python
import sqlite3

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name TEXT)")
db.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
db.executemany("INSERT INTO customers VALUES (?, ?)", [(i, f"c{i}") for i in range(1000)])
db.executemany("INSERT INTO orders VALUES (?, ?, ?)", [(i, i % 1000, 10.0) for i in range(20_000)])

queries = 0
def run(sql, params=()):
    global queries
    queries += 1
    return db.execute(sql, params).fetchall()

# N+1: one query for the list, then one more query per customer
totals = {}
for (cid,) in run("SELECT customer_id FROM customers"):
    totals[cid] = run("SELECT SUM(amount) FROM orders WHERE customer_id = ?", (cid,))[0][0]
print(queries, totals[7])        # 1001 200.0

# One set-based query does the same work
queries = 0
totals = dict(run("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"))
print(queries, totals[7])        # 1 200.0
```

In memory the difference is small. Against a networked database each round trip costs milliseconds, and against BigQuery each query is a job with seconds of overhead and its own minimum bill. ORMs produce N+1 by lazily loading related objects in a loop. Think in sets: one query that joins or groups.

**3. Functions on filtered columns.** Wrapping a column in a function hides it from indexes, partition pruning and zone maps:

```python
# continues the SQLite session from the plans topic, with an index on order_date
db.execute("CREATE INDEX idx_orders_date ON orders (order_date)")
plan("SELECT COUNT(*) FROM orders WHERE substr(order_date, 1, 7) = '2026-03'")
#    SCAN orders USING COVERING INDEX idx_orders_date        every entry is read
plan("SELECT COUNT(*) FROM orders WHERE order_date >= '2026-03-01' AND order_date < '2026-04-01'")
#    SEARCH orders USING COVERING INDEX idx_orders_date (order_date>? AND order_date<?)
```

Rewrite the condition as a range on the raw column (a **sargable** predicate, from "search argument able"). In BigQuery the same rule protects partition pruning: filter `event_ts >= ... AND event_ts < ...`, not `FORMAT_TIMESTAMP('%Y-%m', event_ts) = '2026-03'`.

**4. Implicit type conversion.** Comparing values of different types makes the engine convert one side, with results that differ by engine. A `code` column stored as text (`'00042'`) compared with the number `42` returns no rows at all in SQLite (the number is converted to the text `'42'`, which is not `'00042'`), is an error in PostgreSQL and BigQuery, and in MySQL converts every row's text to a number, which defeats the index. Compare like with like, and when you must convert, convert the constant, not the column.

**5. Missing or wrong WHERE on writes.** `UPDATE customers SET city = 'Pune'` without a `WHERE` updates everyone. BigQuery requires a `WHERE` clause on `UPDATE` and `DELETE` for this reason (write `WHERE TRUE` if you really mean every row). Elsewhere, run the `WHERE` as a `SELECT COUNT(*)` first, and do manual fixes inside a transaction you can roll back. In BigQuery, time travel (up to seven days by default) can restore a table if it happens anyway.

**6. Unbounded windows.** `ROW_NUMBER() OVER (ORDER BY created_at)` with no `PARTITION BY` sends every row to a single worker to be sorted. On a large table in BigQuery or Spark that is slow at best and "Resources exceeded" at worst. Partition the window, or if you only need unique IDs, use `GENERATE_UUID()` or a hash of the key instead of a global row number.

**7. `SELECT DISTINCT` as a band-aid.** Duplicates in a result usually mean a join is fanning out. `DISTINCT` hides the symptom, costs a sort or hash over the whole result, and leaves aggregates wrong elsewhere. Find the join that multiplies rows and fix the grain.

**8. SQL injection.** Building SQL by pasting values into a string lets the value change the query:

```python
# same SQLite session as the functions example above (100,000 orders)
user_input = "0 OR 1=1"
bad = f"SELECT COUNT(*) FROM orders WHERE customer_id = {user_input}"
print(db.execute(bad).fetchone())                      # (100000,)  every row leaked
good = "SELECT COUNT(*) FROM orders WHERE customer_id = ?"
print(db.execute(good, (user_input,)).fetchone())      # (0,)  treated as one plain value
```

With a **parameter**, the value is sent separately from the SQL text and can never become code. Every driver supports parameters: `?` or `:name` in sqlite3, `%s` in psycopg, and named parameters in BigQuery (`WHERE status = @status`, passed with `bq query --parameter=status:STRING:paid` or `ScalarQueryParameter` in the Python client). Identifiers such as table names cannot be parameters; check them against an allow-list before inserting them into SQL.

> **Pitfall:** `COUNT(column)` and `COUNT(*)` differ when the column has nulls, and `AVG` ignores nulls rather than treating them as zero. A "conversion rate" computed as `AVG(converted)` over a column where non-converters are `NULL` instead of 0 is 100%. Decide what null means before aggregating.

**Debugging a wrong number, step by step.**

1. **State the grain** of every input and of the output: "one row per order", "one row per customer per day".
2. **Assert uniqueness** where you expect it: `SELECT order_id, COUNT(*) FROM t GROUP BY order_id HAVING COUNT(*) > 1` should return nothing.
3. **Count rows after each step.** Turn each CTE into the final `SELECT` in turn (`SELECT COUNT(*) FROM step_n`) and find where the count jumps or drops unexpectedly. Jumps mean fan-out; drops mean an inner join or a `WHERE` removed rows (often nulls).
4. **Reconcile a total** against a trusted source at the coarsest level, then drill down one dimension at a time until the difference sits in one group.
5. **Diff two versions** of a table with `SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b` (and the reverse) to see exactly which rows changed.
6. **Shrink to a minimal example**: one customer, one day, a handful of rows you can check by hand.

::: deeper
**Anti-patterns in schema, not just queries.** Bill Karwin's catalogue includes several that data engineers inherit from source systems: **jaywalking** (comma-separated lists in a column, so `'1,4,7'` needs string splitting to join), **entity-attribute-value** tables (one row per field, impossible to type or constrain), **polymorphic associations** (a foreign key that points to different tables depending on another column) and **multi-column attributes** (`tag1`, `tag2`, `tag3`). In a warehouse you usually fix them once in the staging layer: `SPLIT` and `UNNEST` the lists, pivot the EAV rows into typed columns, and make the polymorphic reference explicit.

**Correctness traps with NULL and types.** `NULL = NULL` is unknown, so joins on nullable keys drop rows (use `IS NOT DISTINCT FROM` where supported, or `COALESCE` both sides to a sentinel that cannot occur). Floating-point sums are not exactly reproducible across runs in parallel engines, because addition order changes; use `NUMERIC` for money. Integer division (`7 / 2 = 3` in PostgreSQL) silently truncates ratios.

**Non-determinism.** `LIMIT` without `ORDER BY`, `ROW_NUMBER` over non-unique ordering, `ANY_VALUE`, and `ARRAY_AGG` without `ORDER BY` can return different results on each run. In a pipeline that means flaky tests and rows that change on every rebuild.

**Cost anti-patterns specific to BigQuery.** `SELECT *` bills every column; `LIMIT` does not reduce bytes scanned on a normal table; filters on a non-partition column do not prune partitions; repeated querying of the same expensive CTE recomputes it. Use dry runs to see bytes before running, and the `maximum_bytes_billed` setting as a guard rail.

**Detecting problems automatically.** SQL linters such as SQLFluff enforce style and catch some anti-patterns (ambiguous joins, `SELECT *` in models). dbt tests (`unique`, `not_null`, `relationships`) and row-count comparisons between layers catch fan-out and dropped rows in every run rather than when a stakeholder notices.

> **Interview:** "Revenue in your new model is 3% higher than finance's number. How do you investigate?" Agree the definition first (refunds, currency, time zone, cut-off), then reconcile at the coarsest level and drill down by date and dimension to localise the gap, check uniqueness of the keys at each join for fan-out, check for duplicate ingestion of the same source rows, and diff the rows in the differing slice.
:::

::: check
?? A query joins `orders` to `order_items` and sums `orders.amount`. Why is the total too high, and how do you fix it?
Each order row repeats once per item, so its amount is counted several times (fan-out). Sum item-level measures, or aggregate items per order in a subquery before joining.

?? Why is `WHERE DATE(created_at) = '2026-03-08'` often slower than `WHERE created_at >= '2026-03-08' AND created_at < '2026-03-09'`?
The function on the column prevents index use (or, in BigQuery on a column-partitioned table, can prevent pruning); the range on the raw column is sargable.

?? How does a query parameter prevent SQL injection?
The SQL text and the value are sent separately; the database compiles the text first and binds the value as data, so the value can never be parsed as SQL.
:::

::: resources
- Book: [SQL Antipatterns, Volume 1](https://pragprog.com/titles/bksap1/sql-antipatterns-volume-1/) · Bill Karwin's catalogue of schema and query mistakes, with fixes
- Docs: [OWASP SQL Injection Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html) · parameters, allow-listing and other defences
- Docs: [Running parameterized queries in BigQuery](https://docs.cloud.google.com/bigquery/docs/parameterized-queries) · named and positional parameters from the CLI and client libraries
- Article: [Use The Index, Luke: functions in WHERE clauses](https://use-the-index-luke.com/sql/where-clause/functions) · why functions on columns defeat indexes and how to fix it
- Blog: [depesz.com: performance articles](https://www.depesz.com/tag/performance/) · long-running blog of real PostgreSQL performance investigations
- Repo: [SQLFluff](https://github.com/sqlfluff/sqlfluff) · SQL linter and formatter that supports BigQuery and dbt templating
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, SQL or rule of thumb |
|---|---|
| Logical order of a query | `FROM`/`JOIN`, `WHERE`, `GROUP BY`, `HAVING`, window, `SELECT`, `DISTINCT`, `ORDER BY`, `LIMIT` |
| Test for missing values | `x IS NULL`, never `x = NULL`; replace with `COALESCE(x, fallback)` |
| Keep unmatched left rows | `LEFT JOIN ... ON key AND right_side_filter` (filter in `ON`, not `WHERE`) |
| Filter after aggregation | `HAVING SUM(amount) > 30` |
| Latest row per key | `QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) = 1` |
| Running total | `SUM(x) OVER (PARTITION BY k ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| Previous value | `LAG(x) OVER (PARTITION BY k ORDER BY t)` |
| Gaps and islands | Group by `date - ROW_NUMBER()` |
| Anti-join that handles nulls | `NOT EXISTS (SELECT 1 ... WHERE ...)`, not `NOT IN` |
| Walk a hierarchy | `WITH RECURSIVE` anchor `UNION ALL` recursive step |
| Event date in a local zone | `DATE(event_ts, 'Asia/Kolkata')` |
| Date range filter | `ts >= '2026-03-01' AND ts < '2026-04-01'` (half-open) |
| Days with zero sales | `LEFT JOIN` data to `UNNEST(GENERATE_DATE_ARRAY(start, end))` |
| Flatten an array | `FROM t LEFT JOIN UNNEST(t.items) AS item` |
| Scalar from JSON | `JSON_VALUE(payload, '$.device.os')` |
| Subtotals and grand total | `GROUP BY ROLLUP (a, b)` with `GROUPING(b)` |
| Rows to columns | `PIVOT (SUM(x) FOR col IN ('a' AS a, 'b' AS b))` or `SUM(IF(col = 'a', x, NULL))` |
| Point-in-time dimension join | `ON f.k = d.k AND f.d >= d.valid_from AND f.d < d.valid_to` |
| Idempotent load | `MERGE` on a key with a deduplicated source, or delete and insert the partition in one transaction |
| Read a PostgreSQL plan | `EXPLAIN (ANALYZE, BUFFERS) ...`; compare estimated and actual rows |
| Safe values in SQL | Query parameters (`?`, `%s`, `@name`), never string formatting |

### Practice

All exercises use the sample tables from the first topic. Run them in SQLite with the `shop.py` script, or in BigQuery.

::: try
**1. Customers without paid orders.** List the names of customers who have no paid order. Write it twice: once with `LEFT JOIN` and once with `NOT EXISTS`. Which customers appear, and why is customer 9 not in the list?
:::

?? A solution to 1

```sql
SELECT c.name
FROM shop.customers AS c
LEFT JOIN shop.orders AS o
  ON o.customer_id = c.customer_id AND o.status = 'paid'
WHERE o.order_id IS NULL;

SELECT c.name
FROM shop.customers AS c
WHERE NOT EXISTS (
  SELECT 1 FROM shop.orders AS o
  WHERE o.customer_id = c.customer_id AND o.status = 'paid'
);
-- both return: Dana
```

Customer 9 has an order but no row in `customers`, and the query starts from `customers`, so customer 9 cannot appear.

::: try
**2. Each customer's first order.** For every customer with at least one paid order, return the order ID, date and amount of their first paid order, plus the number of days until their next paid order (`NULL` if none). Use window functions.
:::

?? A solution to 2

```sql
SELECT customer_id, order_id, order_date, amount,
       DATE_DIFF(next_date, order_date, DAY) AS days_to_next
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, order_id) AS rn,
    LEAD(order_date) OVER (PARTITION BY customer_id ORDER BY order_date, order_id) AS next_date
  FROM shop.orders
  WHERE status = 'paid'
)
WHERE rn = 1
ORDER BY customer_id;
-- 1 | 101 | 2026-03-01 | 40.0 | 1
-- 2 | 105 | 2026-03-05 | 35.0 | NULL
-- 3 | 104 | 2026-03-03 | 60.0 | NULL
-- 9 | 107 | 2026-03-08 | 12.0 | NULL
```

`LEAD` must be computed before the `rn = 1` filter, which is why both window functions sit in the subquery. In SQLite replace `DATE_DIFF(next_date, order_date, DAY)` with `julianday(next_date) - julianday(order_date)`.

::: try
**3. Find the bug.** This query is meant to return revenue per city, but the Pune figure is wrong. Explain why, and fix it.

```sql
SELECT c.city, SUM(o.amount) AS revenue
FROM shop.orders AS o
JOIN shop.order_items AS i ON i.order_id = o.order_id
JOIN shop.customers AS c ON c.customer_id = o.customer_id
WHERE o.status = 'paid'
GROUP BY c.city;
```
:::

?? Answer to 3
The join to `order_items` does two kinds of damage. It repeats each order once per item, so orders 101 and 103 are counted twice; and it is an inner join, so orders with no item rows (106 and 105) vanish. Pune shows 184.0 (40 × 2 + 22 × 2 + 60) and Oslo disappears entirely. The items table is not needed at all: remove that join and Pune is 140.0 (Asha 80 plus Chen 60) and Oslo 35.0. If item columns are needed, aggregate items per order in a subquery first and `LEFT JOIN` it.

::: try
**4. Daily revenue with zeros and a 3-day moving average.** In BigQuery, produce one row per day from 2026-03-01 to 2026-03-08 with paid revenue (0 on days without orders) and the average of the current and two previous days.
:::

?? A solution to 4

```sql
WITH days AS (
  SELECT d FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2026-03-01', DATE '2026-03-08')) AS d
),
daily AS (
  SELECT days.d AS day, COALESCE(SUM(o.amount), 0) AS revenue
  FROM days
  LEFT JOIN shop.orders AS o ON o.order_date = days.d AND o.status = 'paid'
  GROUP BY days.d
)
SELECT day, revenue,
  ROUND(AVG(revenue) OVER (ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_3d
FROM daily
ORDER BY day;
-- revenue: 40, 22, 60, 0, 35, 0, 0, 30
-- avg_3d:  40.0, 31.0, 40.67, 27.33, 31.67, 11.67, 11.67, 10.0
```

Because the date spine gives every day a row, `ROWS BETWEEN 2 PRECEDING` really means three calendar days.

::: try
**5. Make it safe.** A colleague's Python job runs `client.query(f"DELETE FROM shop.orders WHERE status = '{status}'")` with `status` read from a configuration file that other teams can edit. Name two problems and rewrite the call with the BigQuery Python client.
:::

?? A solution to 5
The value is pasted into SQL, so a crafted value such as `x' OR TRUE --` would delete every row (SQL injection), and a typo deletes data with no review. Use a query parameter and, for destructive jobs, validate the value against an allow-list:

```python
from google.cloud import bigquery

ALLOWED = {"cancelled", "test"}
if status not in ALLOWED:
    raise ValueError(f"refusing to delete status {status!r}")

client = bigquery.Client()
job_config = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ScalarQueryParameter("status", "STRING", status)]
)
client.query("DELETE FROM shop.orders WHERE status = @status", job_config=job_config).result()
```

### What comes next

You can now ask precise questions of any SQL engine, express warehouse patterns in a few readable statements, and work out why a query is slow or wrong. SQL is half of the everyday toolkit; the other half is a general-purpose language for everything SQL is awkward at: calling APIs, parsing files, orchestrating steps, testing and packaging pipeline code. The next section, **Programming for data engineering**, covers Python as data engineers use it, fast DataFrame libraries such as Polars, and when Scala, Rust or Go earn their place.
