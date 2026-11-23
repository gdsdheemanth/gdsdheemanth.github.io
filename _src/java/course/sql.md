You already use SQL at work. Make it excellent: SQL rounds are common and you can stand out here.

This track lists every topic to learn, in order, with mini projects to build and the best resources. Work through the topics with the resources below, then build at least two of the projects.

## Relational fundamentals

### Tables, primary/foreign/composite/surrogate keys, constraints (UNIQUE, NOT NULL, CHECK)

Learn this from the resources at the end of the section.

### Normalization (1NF, 2NF, 3NF, BCNF) and when to denormalize

Learn this from the resources at the end of the section.

### ER modeling: one-to-one, one-to-many, many-to-many (join tables)

Learn this from the resources at the end of the section.


## Querying

### SELECT, WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, LIKE, IN, BETWEEN

Learn this from the resources at the end of the section.

### Aggregates & GROUP BY, HAVING vs WHERE

Learn this from the resources at the end of the section.

### JOINs: inner, left, right, full outer, self, cross

Classic: find rows in A that are missing in B with LEFT JOIN … IS NULL.

### Subqueries, correlated subqueries, EXISTS vs IN

Learn this from the resources at the end of the section.

### CTEs (WITH), recursive CTEs (hierarchies)

Learn this from the resources at the end of the section.

### Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG/LEAD, running totals, PARTITION BY

Very common in interviews.

### CASE expressions, NULL handling (COALESCE, NULLIF), set operators (UNION, INTERSECT, EXCEPT)

Learn this from the resources at the end of the section.

### String & date functions

Learn this from the resources at the end of the section.


## DDL, DML & programmability

### CREATE/ALTER/DROP; INSERT/UPDATE/DELETE; DELETE vs TRUNCATE vs DROP

Learn this from the resources at the end of the section.

### UPSERT (INSERT … ON CONFLICT / MERGE)

Learn this from the resources at the end of the section.

### Views, materialized views, stored procedures, functions, triggers

Learn this from the resources at the end of the section.


## Performance

### Indexes: B-tree, composite, covering, unique; when an index is not used

Learn this from the resources at the end of the section.

### Clustered vs non-clustered indexes

Learn this from the resources at the end of the section.

### Reading EXPLAIN / EXPLAIN ANALYZE query plans

Learn this from the resources at the end of the section.

### Query optimization habits; keyset vs offset pagination; partitioning large tables

Learn this from the resources at the end of the section.


## Transactions & concurrency

### ACID properties

Learn this from the resources at the end of the section.

### Isolation levels: read uncommitted, read committed, repeatable read, serializable

Learn this from the resources at the end of the section.

### Anomalies: dirty read, non-repeatable read, phantom read, lost update

Learn this from the resources at the end of the section.

### Locking: row/table locks, optimistic vs pessimistic, deadlocks; MVCC

Learn this from the resources at the end of the section.


## Beyond relational

### PostgreSQL (learn this) and MySQL; awareness of Oracle (common in large enterprises)

Learn this from the resources at the end of the section.

### NoSQL types: document (MongoDB), key-value (Redis), wide-column (Cassandra)

Learn this from the resources at the end of the section.

### Redis for caching, sessions, rate limiting

Learn this from the resources at the end of the section.

### CAP theorem, SQL vs NoSQL tradeoffs

Learn this from the resources at the end of the section.

## Hands-on mini projects

Build at least two of these before moving on. Put each on GitHub with a short README.

### Movie Booking Database Design (Beginner)

Design tables for movies, theatres, screens, shows, seats, users, bookings. Draw the ER diagram, normalise to 3NF, add constraints.

**Practises:** Data modelling, keys, normalization, constraints

### 50-Query Challenge on a sample DB (Intermediate)

Load a sample database (Chinook, Northwind or Pagila) into PostgreSQL and write 50 business queries: top customers, monthly revenue, best-selling items per category.

**Practises:** Joins, GROUP BY, CTEs, window functions

### Index Tuning Lab (Advanced)

Generate 1–5 million rows, run slow queries, read EXPLAIN ANALYZE, add the right indexes and record the before/after timings in a README.

**Practises:** Indexes, query plans, performance

### Transactions & Locking Experiments (Advanced)

Open two psql sessions and reproduce dirty reads, lost updates and deadlocks under different isolation levels. Document what you saw.

**Practises:** ACID, isolation levels, locking, MVCC

## Resources

::: resources
- [SQLBolt](https://sqlbolt.com/) · Interactive SQL basics
- [Mode SQL Tutorial](https://mode.com/sql-tutorial/) · Basic → advanced incl. window functions
- [PostgreSQL Exercises](https://pgexercises.com/) · Practice joins, aggregates, recursive queries
- [LeetCode SQL 50](https://leetcode.com/studyplan/top-sql-50/) · Interview-style SQL problems
- [DataLemur SQL](https://datalemur.com/) · Real company SQL interview questions
- [Use The Index, Luke](https://use-the-index-luke.com/) · Indexing & query performance explained
- [PostgreSQL docs: Tutorial](https://www.postgresql.org/docs/current/tutorial.html) · Official reference
- Video: [CMU Intro to Database Systems](https://www.youtube.com/results?search_query=CMU+Intro+to+Database+Systems+Andy+Pavlo) · How databases work internally (advanced)
:::
