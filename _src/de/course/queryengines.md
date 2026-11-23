This section opens up the engines you have been using and looks at the machinery inside: how a SQL string becomes a plan, how an optimizer chooses between millions of possible plans, and how a modern executor runs the chosen plan fast on real CPUs. It is for data engineers who already write SQL and run engines such as Spark, Trino or BigQuery, and who now want to understand *why* one query takes two seconds and another two hours, how to read a plan with confidence, and what the new native engines (DuckDB, Velox, DataFusion, Comet, Gluten) actually change. It assumes [Apache Spark](#spark) (Catalyst, Tungsten and whole-stage code generation are introduced there and only extended here), Trino's coordinator and workers from [Flink, Beam, Trino and other engines](#engines), and the hardware facts from [Storage internals and hardware](#hardware): cache lines, the memory hierarchy, NUMA and lightweight encodings.

**What you will be able to do afterwards**

- Trace a query through parsing, validation, logical optimization, physical planning and execution, and say which component of Apache Calcite does each step.
- Explain how a cost-based optimizer estimates row counts from statistics, why those estimates go wrong, and how join ordering is searched; read `EXPLAIN ANALYZE` output to spot a bad estimate.
- Describe what Substrait is, who produces and consumes it, and where it fits next to SQL and Arrow.
- Compare the Volcano iterator model, vectorized execution, push-based pipelines and morsel-driven parallelism, and say which engines use which.
- Explain how SIMD instructions speed up filters and aggregations, how selection vectors work, and when engines rely on the compiler instead of hand-written intrinsics.
- Explain when runtime code generation (LLVM, Janino) beats a vectorized interpreter and when its compile time does not pay off.
- Describe Velox and DataFusion as reusable engine libraries, decide when Comet, Gluten or Google's Lightning Engine are worth trying on Spark, and judge where WebAssembly makes sense for data work.

**The big picture.** Every SQL engine, from SQLite to BigQuery, runs the same pipeline. A **parser** turns text into a syntax tree. A **validator** (also called analyzer or binder) resolves names and types against a catalog. The tree becomes a **logical plan**: a tree of relational operators such as scan, filter, join and aggregate that says *what* to compute. An **optimizer** rewrites that plan into cheaper equivalent forms, using rules and, where it has statistics, cost estimates, and produces a **physical plan** that says *how*: which join algorithm, which order, how many parallel tasks. Finally an **executor** runs the physical plan on CPUs, memory and storage. The first group of this section is about the front half (planning); the second is about the back half (execution); the third looks at the modern open-source engines that package either half as a reusable library.

[[fig:queryengines_pipeline]]

A useful way to remember the split: the planner decides how much work there is (which rows are read, which joins blow up, which shuffles happen); the executor decides how fast each unit of work runs (instructions per row, cache misses, branch mispredictions). A great executor cannot rescue a plan that joins in the wrong order, and a perfect plan still runs slowly on an executor that spends 90% of its time interpreting. Both halves matter, and they fail in different ways.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

## Planning

Planning is where the biggest wins and the biggest disasters happen. The difference between a good and a bad join order can be a factor of a thousand in rows processed, far more than any execution trick can recover. This group starts with **Apache Calcite**, the open-source framework that many engines borrow their parser, validator and optimizer from, because it lays out the planning stages as separate, inspectable parts. It then goes inside **cost-based optimization**: statistics, cardinality estimation and join-order search. It ends with **Substrait**, an attempt to give query plans a standard, engine-neutral format, so the planner of one system can hand work to the executor of another.

If you read [Apache Spark](#spark), you have already met one optimizer, Catalyst, and its rule-based and cost-based phases. The ideas here are the general ones behind Catalyst, and they apply equally to Trino, Flink SQL, Hive, PostgreSQL and the engines later in this section.

### Apache Calcite — Query Planning Framework

::: plain
Building a database from scratch means writing a SQL parser, a type checker, an optimizer and an executor. Apache Calcite is a ready-made kit for the first three. Think of it as the engine block that several car makers buy and put into their own cars: Hive, Flink, Druid and others each add their own wheels (storage and execution), but the part that understands SQL and decides on a plan is Calcite. Because it is a separate library, you can also use it on its own to parse, rewrite or translate SQL.
:::

**What Calcite is.** Apache Calcite is a Java library for query processing that deliberately leaves out two things a database normally has: data storage and a full execution engine. It provides a SQL parser, a validator, a relational algebra representation, a large library of rewrite rules, two planners, a cost and metadata framework, and **adapters** that let it see external systems (JDBC databases, Elasticsearch, MongoDB, Cassandra, Druid, Kafka, CSV files) as tables. It began as Optiq, entered the Apache incubator in 2014 and became a top-level project in 2015. The SIGMOD 2018 paper describes its design.

**The stages, as Calcite names them.**

1. **Parsing.** `SqlParser` (generated with JavaCC) turns text into a tree of `SqlNode` objects. The grammar is configurable: conformance levels and a lenient "Babel" parser accept dialect quirks.
2. **Validation.** `SqlValidator` resolves every table and column against a **catalog** (Calcite's `Schema` and `Table` interfaces), infers types and checks that functions are called correctly. A misspelled column fails here.
3. **Conversion to relational algebra.** `SqlToRelConverter` turns the validated tree into a tree of `RelNode` operators: `LogicalTableScan`, `LogicalFilter`, `LogicalProject`, `LogicalJoin`, `LogicalAggregate`, `LogicalSort` and so on. Expressions inside them are `RexNode` trees, where `$0`, `$1` refer to input fields by position.
4. **Optimization.** A planner applies **rules** to the `RelNode` tree. A rule is a pattern ("a Filter on top of a Join") plus a rewrite ("push the filter into the join's input").
5. **Implementation.** Each operator is given a **calling convention**, a trait that says which engine will run it. Calcite's own `Enumerable` convention generates Java code (compiled with Janino) and runs it; an adapter's convention (for example JDBC) turns a subtree back into SQL for the remote system; an engine such as Flink converts the plan into its own physical operators.

A small Java program shows the stages and one rule. It parses a query over two tables in a schema `SALES`, converts it to a logical plan, and runs a heuristic planner with a single rule that pushes the filter into the join:

```java
FrameworkConfig config = Frameworks.newConfigBuilder()
    .defaultSchema(rootSchema)                 // schema with ORDERS and CUSTOMERS registered
    .build();
Planner planner = Frameworks.getPlanner(config);

SqlNode parsed    = planner.parse(
    "SELECT c.country, o.amount FROM orders o " +
    "JOIN customers c ON o.customer_id = c.customer_id WHERE c.country = 'DE'");
SqlNode validated = planner.validate(parsed);
RelNode logical   = planner.rel(validated).project();

HepProgram program = new HepProgramBuilder()
    .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
    .build();
HepPlanner hep = new HepPlanner(program);
hep.setRoot(logical);
RelNode optimized = hep.findBestExp();
System.out.println(RelOptUtil.toString(optimized));
```

The printed plans look like this (simplified: literal types omitted). Before the rule:

```text
LogicalProject(COUNTRY=[$4], AMOUNT=[$2])
  LogicalFilter(condition=[=($4, 'DE')])
    LogicalJoin(condition=[=($1, $3)], joinType=[inner])
      LogicalTableScan(table=[[SALES, ORDERS]])
      LogicalTableScan(table=[[SALES, CUSTOMERS]])
```

After it, the filter sits on the `CUSTOMERS` input, so only German customers reach the join (field `$4` of the join output is field `$1` of `CUSTOMERS`):

```text
LogicalProject(COUNTRY=[$4], AMOUNT=[$2])
  LogicalJoin(condition=[=($1, $3)], joinType=[inner])
    LogicalTableScan(table=[[SALES, ORDERS]])
    LogicalFilter(condition=[=($1, 'DE')])
      LogicalTableScan(table=[[SALES, CUSTOMERS]])
```

The same idea fits in a few lines of Python, which makes the "pattern plus rewrite" shape of a rule concrete:

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Scan:
    table: str
    cols: tuple

@dataclass(frozen=True)
class Filter:
    cond: tuple          # (column, op, value)
    child: object

@dataclass(frozen=True)
class Join:
    key: str
    left: object
    right: object

def cols(node):
    if isinstance(node, Scan):
        return set(node.cols)
    if isinstance(node, Filter):
        return cols(node.child)
    return cols(node.left) | cols(node.right)

def push_filter_into_join(node):
    """Rule: Filter(Join(L, R)) -> Join(Filter(L), R) when the filter only uses L's columns."""
    if isinstance(node, Filter) and isinstance(node.child, Join):
        j, col = node.child, node.cond[0]
        if col in cols(j.left):
            return Join(j.key, Filter(node.cond, j.left), j.right)
        if col in cols(j.right):
            return Join(j.key, j.left, Filter(node.cond, j.right))
    return None          # rule does not match

def show(node, depth=0):
    pad = "  " * depth
    if isinstance(node, Scan):
        return f"{pad}Scan({node.table})\n"
    if isinstance(node, Filter):
        return f"{pad}Filter({' '.join(map(str, node.cond))})\n" + show(node.child, depth + 1)
    return f"{pad}Join({node.key})\n" + show(node.left, depth + 1) + show(node.right, depth + 1)

orders = Scan("orders", ("order_id", "customer_id", "amount"))
customers = Scan("customers", ("customer_id", "country"))
plan = Filter(("country", "=", "DE"), Join("customer_id", orders, customers))

print(show(plan), end="")
print("---")
print(show(push_filter_into_join(plan)), end="")
# Filter(country = DE)
#   Join(customer_id)
#     Scan(orders)
#     Scan(customers)
# ---
# Join(customer_id)
#   Scan(orders)
#   Filter(country = DE)
#     Scan(customers)
```

**Two planners.** Calcite ships two strategies for applying rules:

| | HepPlanner | VolcanoPlanner |
|---|---|---|
| Strategy | Heuristic: apply rules in a programmed order until nothing changes | Cost-based: explore many equivalent plans, keep the cheapest |
| Keeps alternatives? | No, each rewrite replaces the old tree | Yes, in a memo of equivalence sets |
| Needs a cost model | No | Yes (row counts, CPU, I/O from metadata) |
| Typical use | Normalisation that is always good: merge projects, push filters, simplify expressions | Join ordering, choosing join algorithms and conventions |

Engines usually run both: a Hep phase to normalise the tree, then a Volcano phase to choose physical operators.

**Adapters and push-down.** An adapter exposes an external system as a schema and contributes rules that move work into it. With the JDBC adapter, a filter, projection, join or aggregate whose inputs all live in the same database can be converted to the JDBC convention and sent to that database as one SQL statement (Calcite's `RelToSqlConverter` writes it in the target dialect; a `BigQuerySqlDialect` exists alongside PostgreSQL, MySQL and many others). This is the same federation idea as Trino's connectors in [Flink, Beam, Trino and other engines](#engines), built as a library.

**Who uses it.** Apache Hive uses Calcite for cost-based optimization (join reordering) of HiveQL. Flink SQL and the Table API use Calcite for parsing, validation and logical optimization, then convert to Flink's own physical operators. Apache Druid's SQL layer plans queries with Calcite and translates them to native Druid queries. Apache Drill, Phoenix, Kylin, Beam SQL, Pinot's multi-stage engine and Dremio use it too. Spark (Catalyst), Trino, DuckDB and BigQuery have their own optimizers.

> **Pitfall:** Calcite's rule library is large, and adding a rule to a cost-based planner can make planning time explode, because every new equivalent expression can trigger more rules. Engines that embed Calcite set rule sets carefully per phase. If planning takes seconds, look at the planner phases before blaming execution.

> **Tip:** To learn how a Calcite-based engine plans, use its `EXPLAIN`: Flink's `EXPLAIN PLAN FOR` shows the abstract syntax tree, the optimized logical plan and the physical plan, which are Calcite stages with Flink operators at the end.

::: deeper
**How the VolcanoPlanner works.** Its **memo** groups expressions that produce the same result into equivalence sets (`RelSet`), subdivided by physical traits into `RelSubset`s (for example "the join of A and B, sorted by key, in the Enumerable convention"). Rules fire on matches and register new expressions in the right set. Costs come from the **metadata** framework: `RelMetadataQuery` answers row count, selectivity, distinct row count, column uniqueness and more, using handlers each engine can override. When exploration ends, the planner extracts the cheapest tree that satisfies the required traits at the root. The design comes from Graefe's Volcano optimizer generator and its successor **Cascades**, which also underlie SQL Server, Greenplum's Orca and CockroachDB's optimizer. Later Calcite versions added a top-down, Cascades-style driver for the VolcanoPlanner.

**Traits** are properties of a physical plan that operators can require or provide: convention, collation (sort order), and distribution. A merge join requires both inputs sorted on the join key; if one is not, the planner inserts a sort, or prefers an index scan that delivers the order for free. Trait propagation is how "interesting orders" from the System R optimizer appear in Calcite.

**RelBuilder.** Engines and tools that do not start from SQL build plans with `RelBuilder`: `builder.scan("ORDERS").filter(...).aggregate(...)`. This is how non-SQL front ends (Pig Latin historically, some DataFrame layers) reuse the optimizer.

**Streaming SQL.** Calcite defines streaming extensions (`STREAM`, windowing table functions such as `TUMBLE` and `HOP`) that Flink and Beam adopted; the paper "One SQL to rule them all" (linked in [Flink, Beam, Trino and other engines](#engines)) explains the shared model.

> **Interview:** "What is the difference between a rule-based and a cost-based optimizer, and why do real engines use both?" Rules apply rewrites that are almost always beneficial (pushing filters, pruning columns, folding constants) without needing statistics. A cost-based optimizer compares alternatives whose benefit depends on data (join order, join algorithm), using estimated row counts. Engines normalise with rules first, then make data-dependent choices by cost, which keeps the cost-based search space small.
:::

::: check
?? Which two parts of a database does Calcite deliberately leave out, and why does that make it popular?
Data storage and a full execution engine. Engines with their own storage and executors can adopt Calcite for parsing, validation and optimization without giving up their own runtime.

?? What is the difference between Calcite's HepPlanner and VolcanoPlanner?
HepPlanner applies rules heuristically in a fixed order and keeps only the current tree. VolcanoPlanner keeps all equivalent alternatives in a memo and chooses the cheapest by cost, which suits join ordering and physical choices.

?? In Calcite, what decides whether an operator runs inside Calcite, in a JDBC database or in Flink?
Its calling convention, a physical trait. Rules convert subtrees to an adapter's or engine's convention; for JDBC that subtree is turned back into SQL and sent to the database.
:::

::: resources
- Docs: [Apache Calcite documentation](https://calcite.apache.org/docs/) · the official guide: tutorial, SQL reference, adapters and the planner
- Docs: [Calcite algebra](https://calcite.apache.org/docs/algebra.html) · relational operators, RelBuilder, traits and how rules transform plans
- Paper: [Apache Calcite: A Foundational Framework for Optimized Query Processing Over Heterogeneous Data Sources](https://arxiv.org/abs/1802.10233) · Begoli et al., SIGMOD 2018; the architecture and who uses it
- Docs: [Calcite adapters](https://calcite.apache.org/docs/adapter.html) · the list of adapters and how to write one
- Repo: [Apache Calcite on GitHub](https://github.com/apache/calcite) · source, including `CoreRules` and the planner tests, which are the best examples
- Course: [CMU 15-721 Advanced Database Systems](https://15721.courses.cs.cmu.edu/spring2024/) · Andy Pavlo's course; the optimizer lectures cover Volcano and Cascades
:::

### Cost-Based Optimization (CBO)

::: plain
A cost-based optimizer is a travel planner that knows the traffic. For one query there may be millions of ways to run it: which table to read first, which join to do next, which algorithm to use for each. The optimizer estimates how many rows each step produces, turns that into a cost, and picks the cheapest plan. Its weak point is the estimate: if it believes a step yields 3,000 rows when it really yields 3 million, it will happily choose a terrible route.
:::

**The three parts.** Every cost-based optimizer has the same three pieces, and the quality of each one matters in this order:

1. **Cardinality estimation**: how many rows does each operator produce?
2. **Plan enumeration**: which alternative plans are considered (join orders, join algorithms, access paths)?
3. **Cost model**: given the row counts, how expensive is each operator (CPU, I/O, network)?

The research verdict, from Leis et al.'s "How good are query optimizers, really?" (VLDB 2015), is that cardinality estimation dominates. With true row counts plugged in, even a simple cost model picks good plans; with the estimates real systems produce, multi-join queries regularly get plans many times slower than necessary.

**Statistics.** Estimates start from statistics collected by commands such as PostgreSQL's `ANALYZE`, Spark's `ANALYZE TABLE ... COMPUTE STATISTICS` (see [Apache Spark](#spark)), Trino's `ANALYZE`, or from file and table-format metadata (Parquet footers, Iceberg manifests). Per table: the row count. Per column:

- **Number of distinct values (NDV)**, often from a sketch such as HyperLogLog.
- **Null fraction**, **minimum and maximum**.
- **Most common values (MCV)** with their frequencies, for skewed columns.
- A **histogram** of the remaining values. Most systems use **equi-depth** histograms: each bucket holds about the same number of rows, so popular ranges get narrow buckets and estimates stay accurate where the data is dense.

In PostgreSQL you can see exactly what the planner knows:

```sql
-- PostgreSQL
ANALYZE customers;
SELECT attname, n_distinct, most_common_vals, most_common_freqs, histogram_bounds
FROM pg_stats
WHERE tablename = 'customers' AND attname IN ('city', 'country');
```

**From statistics to row counts.** The classic formulas date from IBM's System R optimizer (Selinger et al., 1979):

- Equality `col = v`: selectivity is the MCV frequency of `v`, or (1 - MCV total) / (remaining distinct values) if `v` is not an MCV.
- Range `col < v`: the fraction of histogram buckets below `v`, interpolating inside the bucket.
- `p1 AND p2`: `sel(p1) * sel(p2)`, assuming the predicates are **independent**.
- Equi-join `R.a = S.b`: `|R| * |S| / max(NDV(R.a), NDV(S.b))`, assuming every key on the smaller side finds a match (**containment**).

Independence is the assumption that fails most often. A city determines its country, so `city = 'Berlin' AND country = 'DE'` is no more selective than `city = 'Berlin'` alone. Python's `sqlite3` lets you reproduce the error with exact numbers:

```python
import sqlite3

con = sqlite3.connect(":memory:")
con.execute("CREATE TABLE customers (id INTEGER, city TEXT, country TEXT)")
cities = [("Berlin", "DE"), ("Munich", "DE"), ("Hamburg", "DE"), ("Paris", "FR"),
          ("Lyon", "FR"), ("Madrid", "ES"), ("Seville", "ES"), ("Rome", "IT"),
          ("Milan", "IT"), ("Lisbon", "PT")]
rows = [(i, *cities[i % 10]) for i in range(100_000)]
con.executemany("INSERT INTO customers VALUES (?, ?, ?)", rows)

n = con.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
sel_city = con.execute("SELECT AVG(city = 'Berlin') FROM customers").fetchone()[0]
sel_country = con.execute("SELECT AVG(country = 'DE') FROM customers").fetchone()[0]

estimate = n * sel_city * sel_country            # independence assumption
actual = con.execute(
    "SELECT COUNT(*) FROM customers WHERE city = 'Berlin' AND country = 'DE'").fetchone()[0]
print(f"sel(city)={sel_city:.2f} sel(country)={sel_country:.2f}")
print(f"estimate={estimate:.0f} actual={actual}")
# sel(city)=0.10 sel(country)=0.30
# estimate=3000 actual=10000
```

A factor of 3.3 on one table looks harmless. But errors **multiply** up the plan: the estimate for a join is computed from the estimates of its inputs, so three such errors in a five-way join can be off by a factor of thirty or more, and the optimizer then picks a nested-loop join or a broadcast for an input that is really huge. PostgreSQL lets you fix this case with **extended statistics**:

```sql
-- PostgreSQL: teach the planner that city and country are correlated
CREATE STATISTICS customers_city_country (dependencies, mcv) ON city, country FROM customers;
ANALYZE customers;
EXPLAIN ANALYZE SELECT * FROM customers WHERE city = 'Berlin' AND country = 'DE';
```

`EXPLAIN ANALYZE` prints, for every node, the estimated `rows=` next to the actual `rows=`. Reading those pairs from the bottom of the plan upwards, and finding the first node where they diverge by an order of magnitude, is the fastest way to diagnose a bad plan in any engine.

**Join ordering.** For a join of `n` tables, the number of possible orders grows explosively: `n!` left-deep orders (each join has a base table on one side), and far more if **bushy** trees (both inputs of a join can be join results) are allowed. For 10 tables that is 3,628,800 left-deep orders and over 17 billion bushy ones. Optimizers search with **dynamic programming** over subsets, the System R idea: the best plan for a set of tables is built from the best plans of its subsets, so each subset is solved once. The sketch below finds the order that minimises the total number of intermediate rows (a common simple cost, often called C_out) for a four-table query, skipping cross products:

```python
from itertools import combinations

# rows per table after local filters; regions has 1 row left after WHERE name = 'EMEA'
card = {"orders": 1_000_000, "customers": 50_000, "products": 2_000, "regions": 1}
# selectivity of each join predicate (1 / distinct values of the join key)
sel = {frozenset({"orders", "customers"}): 1 / 50_000,
       frozenset({"orders", "products"}): 1 / 2_000,
       frozenset({"customers", "regions"}): 1 / 10}       # 10 regions in total

def size(tables):
    rows = 1
    for t in tables:
        rows *= card[t]
    for pair, s in sel.items():
        if pair <= tables:
            rows *= s
    return rows

def connected(a, b):
    return any(len(p & a) == 1 and len(p & b) == 1 for p in sel)

# best[S] = (cost, plan) for every subset S, built from smaller subsets
best = {frozenset({t}): (0, t) for t in card}
for k in range(2, len(card) + 1):
    for subset in map(frozenset, combinations(card, k)):
        for r in range(1, k):
            for left in map(frozenset, combinations(sorted(subset), r)):
                right = subset - left
                if left in best and right in best and connected(left, right):
                    cost = best[left][0] + best[right][0] + size(subset)
                    if subset not in best or cost < best[subset][0]:
                        best[subset] = (cost, f"({best[left][1]} JOIN {best[right][1]})")

cost, plan = best[frozenset(card)]
print(plan)
print(f"cost (sum of intermediate rows) = {cost:,.0f}")

bad = size({"orders", "products"}) + size({"orders", "products", "customers"}) + size(set(card))
print(f"naive ((orders JOIN products) JOIN customers) JOIN regions: {bad:,.0f}")
# (products JOIN (orders JOIN (customers JOIN regions)))
# cost (sum of intermediate rows) = 205,000
# naive ((orders JOIN products) JOIN customers) JOIN regions: 2,100,000
```

The good order starts from the selective filter (one region), shrinks customers to 5,000 rows, and only then touches the million-row fact table. The order in which the query was written processes ten times more rows. Note that the whole decision rests on the `card` and `sel` numbers: feed in a wrong selectivity and the "best" plan changes.

> **Pitfall:** Stale or missing statistics. After a large load, an optimizer may still believe a table has 1,000 rows and broadcast it, or choose a nested-loop join. Refresh statistics as part of the load (or rely on engines that maintain statistics for you, such as BigQuery, or that read them from table-format metadata).

> **Tip:** When a query is unexpectedly slow, compare estimated and actual rows before changing any setting. Most "optimizer bugs" are estimation errors, and the fix is better statistics (extended statistics, a histogram on the right column) or rewriting the predicate so it can be estimated (for example, avoid wrapping a filtered column in a function).

::: deeper
**Search algorithms in practice.** Exhaustive dynamic programming is affordable up to roughly 10 to 15 tables; beyond that, engines switch strategy. PostgreSQL uses DP up to `geqo_threshold` (12 `FROM` items by default) and a genetic algorithm (GEQO) above it, and stops flattening subqueries and explicit joins into one search beyond `from_collapse_limit` and `join_collapse_limit` (8 by default). Research variants such as DPccp enumerate only connected subgraphs, which avoids considering cross products at all. **Top-down** Cascades optimizers (SQL Server, Calcite's VolcanoPlanner, CockroachDB, Greenplum Orca) explore from the root with memoization and can prune with cost bounds.

**Interesting orders.** A plan that is more expensive locally can win overall if it produces a useful property, such as rows sorted on a key that a later merge join or `ORDER BY` needs. System R kept the best plan per subset *and* per interesting order; Cascades generalises this to required physical properties, including distribution in parallel engines (a plan already hash-partitioned by the join key avoids a shuffle).

**Plan caching.** Optimizing a 20-way join can take longer than running it, so engines cache plans for prepared statements. PostgreSQL first plans each execution with the actual parameter values ("custom" plans) and, after five executions, switches to a cached "generic" plan if it is not estimated to be much worse (`plan_cache_mode` controls this). The danger is a cached plan tuned for one parameter value (a tiny customer) reused for another (the largest customer), a problem SQL Server users call parameter sniffing.

**Adaptive execution** sidesteps estimation by re-planning with real sizes: Spark's AQE re-optimizes at shuffle boundaries ([Apache Spark](#spark)), Trino's dynamic filtering prunes the probe side using the build side's actual keys, and BigQuery adjusts its execution graph while a query runs (its query plan explanation shows stages, and repartition stages can appear during execution). Learned cardinality estimators and query-driven feedback are an active research area; most production engines still rely on histograms, sketches and sampling.

> **Interview:** "Why do optimizers underestimate the size of multi-way joins?" Because they multiply selectivities under independence and containment assumptions, and real data has correlated columns and skewed keys. Each join estimate is built on the inputs' estimates, so errors compound, typically as underestimates, which leads to plans (nested loops, broadcasts) that are catastrophic when the real result is large.
:::

::: check
?? Name the three components of a cost-based optimizer. Which one matters most for plan quality, according to the "How good are query optimizers, really?" study?
Cardinality estimation, plan enumeration and the cost model. Cardinality estimation matters most: with true row counts, even simple cost models pick good plans.

?? A table has `sel(brand = 'X') = 0.01` and `sel(category = 'phones') = 0.2`. What does the independence assumption estimate for both predicates together on 1,000,000 rows, and when is that wrong?
1,000,000 x 0.01 x 0.2 = 2,000 rows. It is wrong when the columns are correlated, for example if brand X only makes phones, in which case the true answer is about 10,000.

?? Why do optimizers use dynamic programming for join ordering?
The best plan for a set of tables can be built from the best plans for its subsets, so each subset is solved once instead of enumerating every one of the factorially many orders.
:::

::: resources
- Paper: [How Good Are Query Optimizers, Really?](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) · Leis et al., VLDB 2015; the Join Order Benchmark and where estimates go wrong
- Docs: [PostgreSQL: Statistics used by the planner](https://www.postgresql.org/docs/current/planner-stats.html) · pg_stats, MCVs, histograms and extended statistics
- Docs: [PostgreSQL: Row estimation examples](https://www.postgresql.org/docs/current/row-estimation-examples.html) · the selectivity formulas worked through with real numbers
- Paper: [Access Path Selection in a Relational Database Management System](https://dl.acm.org/doi/10.1145/582095.582099) · Selinger et al., SIGMOD 1979; the System R optimizer that started it all
- Book: [Database System Concepts](https://db-book.com/) · Silberschatz, Korth and Sudarshan; the query optimization chapter covers estimation and join ordering
- Course: [CMU 15-445 Intro to Database Systems](https://15445.courses.cs.cmu.edu/fall2024/) · the query planning and optimization lectures, free on YouTube
:::

### Substrait — Cross-Engine Query Plans

::: plain
SQL is a language for people, and every engine speaks a slightly different dialect. Substrait is a language for machines: a precise, standard way to write down a query *plan* (scan this, filter that, join on this key) so that one program can build the plan and a different engine can run it. Think of it as a shipping container for queries: whoever packed it, any port with the right crane can unload it.
:::

**The problem.** Today a query travels between systems as SQL text. That works, but it is lossy and fragile: dialects differ (date functions, null handling, integer division), every receiver must re-parse and re-validate, and there is no way to hand over a plan that has already been optimized. Libraries that want to target several engines (a DataFrame front end, a federated planner, a Spark accelerator) end up writing one SQL generator per dialect.

**What Substrait is.** Substrait is an open specification, started in 2021 by people from the Apache Arrow community, for **relational algebra as data**. A plan is a Protocol Buffers message (with an equivalent JSON form) made of:

- **Types**: a precise type system (`i32`, `i64`, `fp64`, `decimal<P,S>`, `string`, `varchar<L>`, `timestamp`, lists, maps, structs) with explicit nullability.
- **Relations** (operators): `ReadRel`, `FilterRel`, `ProjectRel`, `JoinRel`, `AggregateRel`, `SortRel`, `FetchRel` (limit and offset), `SetRel`, `CrossRel`, `WriteRel`, plus physical variants such as `HashJoinRel`, `MergeJoinRel` and `ExchangeRel`, and extension relations for anything engine-specific.
- **Expressions**: field references by position, literals, and calls to **functions**.
- **Function extensions**: functions are not built into the spec. They are declared in YAML extension files (arithmetic, comparison, string, datetime, aggregates) identified by a URN, with exact signatures and semantics (overflow behaviour, null handling). A plan lists the extensions it uses and refers to each function by a numeric **anchor**.

An abridged JSON plan for `SELECT amount FROM orders WHERE amount > 100` shows the shape (type details and some required fields are left out):

```json
{
  "extensionUrns": [{"extensionUrnAnchor": 1, "urn": "extension:io.substrait:functions_comparison"}],
  "extensions": [{"extensionFunction": {"extensionUrnReference": 1, "functionAnchor": 1, "name": "gt:i32_i32"}}],
  "relations": [{
    "root": {
      "names": ["amount"],
      "input": {"project": {
        "input": {"filter": {
          "input": {"read": {
            "namedTable": {"names": ["orders"]},
            "baseSchema": {"names": ["order_id", "amount"]}
          }},
          "condition": {"scalarFunction": {
            "functionReference": 1,
            "arguments": [
              {"value": {"selection": {"directReference": {"structField": {"field": 1}}}}},
              {"value": {"literal": {"i32": 100}}}
            ]
          }}
        }},
        "expressions": [{"selection": {"directReference": {"structField": {"field": 1}}}}]
      }}
    }
  }]
}
```

**Producers and consumers.** A **producer** builds Substrait plans; a **consumer** executes them.

| Role | Examples |
|---|---|
| Producers | Ibis (via `ibis-substrait`), Isthmus (SQL to Substrait using Calcite, in `substrait-java`), the DuckDB `substrait` extension, DataFusion (`datafusion-substrait`), Gluten (from Spark's physical plan) |
| Consumers | Apache Arrow's Acero engine (`pyarrow.substrait.run_query`), DuckDB, DataFusion, Velox (through Gluten) |

[[fig:queryengines_substrait]]

DuckDB makes this easy to try. The extension now lives in the community repository:

```sql
-- DuckDB
INSTALL substrait FROM community;
LOAD substrait;
CREATE TABLE orders AS SELECT range AS order_id, range % 500 AS amount FROM range(1000);
CALL get_substrait_json('SELECT amount FROM orders WHERE amount > 100');   -- the plan as JSON
CALL get_substrait('SELECT amount FROM orders WHERE amount > 100');        -- the binary plan
-- from_substrait(blob) and from_substrait_json(text) execute a plan
```

**Where it is used today.** The clearest production use is **Gluten**, which converts Spark's physical plan into a Substrait-based plan and passes it over JNI to a native engine (Velox or ClickHouse); see Apache Comet & Gluten later in this section. Ibis uses it to target engines without SQL generation. DataFusion and DuckDB use it for plan exchange and testing. Google Cloud's Lightning Engine for Spark is based on Gluten and Velox, so Substrait-style plan hand-off sits under that product too.

**How it relates to Arrow.** Arrow standardises **data** in memory (columnar batches that any engine can read without conversion). Substrait standardises **operations** on that data. Together they let a planner in one language hand both the plan and the data to an executor in another, which is the idea behind "composable" data systems.

> **Pitfall:** Two engines that both "support Substrait" may not support the same operators, types or functions. A plan using a function from an extension the consumer does not implement fails at load time, and subtle semantic differences (decimal overflow, string collation) can still exist. Test plans round trip on your actual pair of engines.

> **Tip:** Substrait is still versioned 0.x and changes often (for example, extension references moved from URIs to URNs). Pin compatible producer and consumer versions, and record the plan's `version` field when you store plans.

::: deeper
**Logical versus physical.** Substrait can express both a logical plan (what to compute, left for the consumer to optimize) and a partly physical one (which join algorithm, where to exchange data). In practice most exchanges are logical or close to it, because the consumer knows its own operators best. Gluten is the exception: it hands over physical fragments that Spark has already planned and distributed.

**Why not just SQL?** SQL is ambiguous in exactly the places plans must be precise: implicit casts, name resolution, the meaning of functions across dialects. A plan carries resolved types and fully qualified function signatures, needs no parsing or validation, and can describe things SQL cannot easily say (an already chosen join order, a specific exchange). On the other hand, SQL is readable, universally supported and stable, so it remains the right interchange for humans and for most integrations. Tools such as SQLGlot (dialect-to-dialect SQL translation) address the dialect problem from the other side.

**Comparison with other plan formats.** Spark Connect sends unresolved logical plans as its own Protocol Buffers messages (not Substrait), and Comet serialises Spark plans into its own protobuf for DataFusion. Engine-specific formats are simpler to evolve; a shared format only pays off when many producers and consumers meet.

> **Interview:** "What would you gain by sending Substrait instead of SQL from a DataFrame library to an engine?" No dialect-specific SQL generation, exact types and function semantics, no re-parsing, and the ability to target any consumer with one producer. The cost is a younger, faster-moving spec with uneven operator and function coverage across engines.
:::

::: check
?? What does Substrait standardise, and what does Apache Arrow standardise?
Substrait standardises query plans (relations, expressions, types and function signatures). Arrow standardises the in-memory columnar data format those plans operate on.

?? How does a Substrait plan refer to a function such as "greater than"?
Through an extension: the plan declares an extension URN and a function name with its signature (for example `gt:i32_i32`) under a numeric anchor, and expressions call the function by that anchor.

?? Name one production system that uses Substrait internally.
Apache Gluten, which converts Spark physical plans to a Substrait-based plan for Velox or ClickHouse.
:::

::: resources
- Spec: [Substrait specification](https://substrait.io/) · the official site: types, relations, expressions and extensions
- Docs: [SQL to Substrait tutorial](https://substrait.io/tutorial/sql_to_substrait/) · builds a plan step by step from a SQL query
- Repo: [Substrait on GitHub](https://github.com/substrait-io/substrait) · the protobuf definitions and the standard function extension YAML files
- Docs: [DuckDB substrait community extension](https://duckdb.org/community_extensions/extensions/substrait) · install and the four functions for producing and consuming plans
- Docs: [PyArrow Substrait API](https://arrow.apache.org/docs/python/api/substrait.html) · running Substrait plans on Arrow's Acero engine from Python
- Repo: [substrait-java (Isthmus)](https://github.com/substrait-io/substrait-java) · Java bindings and the Calcite-based SQL to Substrait converter
:::

## Execution

The planner hands over a physical plan; now something has to run it. For decades the bottleneck was disk, and it hardly mattered how an engine moved rows between operators. With data in memory or in fast columnar files, the CPU became the bottleneck, and engines discovered that a textbook executor spends most of its time on overhead: function calls, type checks and branches, not on the actual additions and comparisons. This group follows the answers in the order they appeared.

**Query execution models** covers the overall shape: pulling rows one at a time (Volcano), pulling or pushing batches, and splitting work into morsels for many cores. **Vectorized execution** looks at the batch idea from the engine's side: column vectors, operators and how batching removes interpretation overhead. **SIMD vectorization** goes one level lower, to the CPU instructions that process 8 or 16 values at once. **Runtime code generation** is the competing answer: instead of interpreting a plan efficiently, compile it into machine code for each query. The memory hierarchy and NUMA facts these topics rely on are in [Storage internals and hardware](#hardware).

### Query Execution Models — Volcano vs Push

::: plain
An execution model is the way operators in a plan pass data to each other. In the classic **Volcano** model, each operator asks the one below it "give me the next row", like a chain of people passing buckets one at a time, each asking the next person down the line. In a **push** model, the data source drives: it hands rows (or whole batches) up the chain as fast as they arrive, like a conveyor belt. Modern engines also cut the input into small chunks (**morsels**) so every CPU core can grab the next chunk when it is free.
:::

**The Volcano (iterator) model.** Described by Goetz Graefe in the early 1990s, it is the textbook design and still the model of PostgreSQL, MySQL and many others. Every operator implements three methods:

- `open()`: prepare (open files, allocate hash tables), and open the children.
- `next()`: return the next row, or "end of stream". An operator calls its children's `next()` as often as it needs.
- `close()`: release resources.

The plan is a tree; the root's `next()` pulls rows up through the tree one at a time. It is simple, composable (any operator can sit on any other), and naturally pipelined: a filter returns a row as soon as it finds one, without materialising its whole input.

Its cost shows up when you count calls. Here is `SELECT SUM(x) FROM t WHERE x % 2 = 0` on 10,000 rows, in Volcano style and then with batches:

```python
calls = {"next": 0}

class Scan:
    def __init__(self, rows): self.rows = rows
    def open(self): self.i = 0
    def next(self):
        calls["next"] += 1
        if self.i == len(self.rows): return None        # end of stream
        self.i += 1
        return self.rows[self.i - 1]

class Filter:
    def __init__(self, child, pred): self.child, self.pred = child, pred
    def open(self): self.child.open()
    def next(self):
        calls["next"] += 1
        while (row := self.child.next()) is not None:
            if self.pred(row): return row
        return None

class Sum:
    def __init__(self, child): self.child = child
    def open(self): self.child.open()
    def next(self):
        calls["next"] += 1
        total = 0
        while (row := self.child.next()) is not None:
            total += row
        return total

rows = list(range(10_000))
plan = Sum(Filter(Scan(rows), lambda x: x % 2 == 0))   # SELECT SUM(x) WHERE x % 2 = 0
plan.open()
print(plan.next(), calls["next"])                         # 24995000 15003

# The same query, vectorized: each call moves a batch of up to 1,024 values.
calls["next"] = 0
def scan_batches(data, size=1024):
    for start in range(0, len(data), size):
        calls["next"] += 1
        yield data[start:start + size]

def filter_batches(batches):
    for batch in batches:
        calls["next"] += 1
        yield [x for x in batch if x % 2 == 0]

print(sum(sum(b) for b in filter_batches(scan_batches(rows))), calls["next"])   # 24995000 20
```

Over 15,000 calls against 20 for the same answer. In C++ or Java each call is a virtual function call the CPU cannot predict or inline, plus per-row type checks and branches; at hundreds of millions of rows, that overhead dwarfs the useful work. The MonetDB/X100 paper (2005) measured a Volcano-style engine spending only a small fraction of its instructions on actual computation.

**Three answers to the overhead.**

| Model | Unit passed between operators | Who drives | Examples |
|---|---|---|---|
| Volcano iterator | one row | parent pulls | PostgreSQL, MySQL, older Hive |
| Vectorized pull | a batch (vector) of about 1,000 to 10,000 values per column | parent pulls batches | MonetDB/X100 and Vectorwise, DataFusion (async streams of Arrow batches) |
| Push-based pipelines | a batch, or one row inside generated code | source pushes into a pipeline | HyPer and Umbra (compiled), DuckDB (since 2021), Velox, Trino, Spark whole-stage codegen |

Vectorization (the next topic) attacks the *per-row* cost by passing batches. Push-based execution changes *control flow*: the plan is cut into **pipelines** at **pipeline breakers**, the operators that must see all their input before producing output (the build side of a hash join, a hash aggregation, a sort). Inside a pipeline, a source produces data and pushes it through filters, projections and the probe side of joins into a **sink** (a breaker or the final result). Pipelines run in dependency order: the hash table is built before the pipeline that probes it starts.

Why push? It makes the scheduler, not the operator tree, responsible for control. The scheduler can run a pipeline on many threads, pause it when a buffer is full, interleave queries, and start the next pipeline as soon as the previous one finishes. DuckDB moved from pull to push in 2021 for exactly these reasons: simpler parallelism and better handling of operators that produce output in bursts.

**Morsel-driven parallelism** (Leis et al., SIGMOD 2014, from the HyPer database) is the standard way to parallelise push pipelines on a multi-core machine:

1. Split each pipeline's input into **morsels**, small chunks (about 100,000 rows in the original paper; in DuckDB's own storage a morsel is a row group of 122,880 rows).
2. A pool of worker threads, one per core, repeatedly takes the next morsel and runs the whole pipeline on it, keeping thread-local state (a local hash table for aggregation, a local partition for the build).
3. When all morsels are done, the sink merges the thread-local states, and the next pipeline begins.

Because work is handed out in small pieces at run time, a slow core or a skewed morsel does not stall the query (the others take more morsels), the degree of parallelism can change while a query runs, and workers can prefer morsels whose data sits on their own NUMA node ([Storage internals and hardware](#hardware) covers NUMA). A miniature version:

```python
from concurrent.futures import ThreadPoolExecutor

rows = list(range(10_000))
MORSEL = 1_000

class SumSink:                          # pipeline breaker: collects partial results
    def __init__(self): self.partials = []
    def push(self, partial): self.partials.append(partial)
    def finalize(self): return sum(self.partials)

def run_pipeline(morsel, sink):        # source -> filter -> partial sum, pushed into the sink
    local = 0
    for x in morsel:
        if x % 2 == 0:                 # filter
            local += x                 # aggregate (thread-local)
    sink.push(local)

sink = SumSink()
morsels = [rows[i:i + MORSEL] for i in range(0, len(rows), MORSEL)]
with ThreadPoolExecutor(max_workers=4) as pool:   # workers take morsels as they finish
    list(pool.map(lambda m: run_pipeline(m, sink), morsels))
print(len(morsels), sink.finalize())   # 10 24995000
```

[[fig:queryengines_pullpush]]

Compare this with exchange-based parallelism, the older approach (Volcano's **exchange operator**, still used between machines in Spark, Trino and BigQuery): the plan is split into a fixed number of parallel instances connected by repartitioning operators, decided at planning time. It works well across a cluster, but on one machine it cannot rebalance when one partition is slow. Distributed engines therefore combine both: exchanges between nodes, morsel-style scheduling of pipelines within a node.

> **Pitfall:** "Pipelined" does not mean "no memory". Pipeline breakers hold their entire input (or a hash table of it) before producing output. In a plan with a large hash join build or a big `GROUP BY`, memory use is decided by the breakers; that is where spilling to disk happens.

> **Tip:** When reading a plan from any engine, find the pipeline breakers first: hash join builds, aggregations, sorts, window functions and exchanges. They tell you where memory is used, where parallelism changes and where the engine must wait.

::: deeper
**Pull versus push is not about speed alone.** Shaikhha et al. ("Push vs. pull-based loop fusion in query engines") showed that with careful compilation both can produce equally fast code; the real differences are in control flow. Pull makes `LIMIT` and early termination trivial (stop calling `next()`), while push needs a way to signal "stop" back to the source. Push handles operators with several consumers (a common subplan used twice) and multiple outputs more naturally, and lets the scheduler own threads and back-pressure.

**Operator interfaces in real engines.** Trino and Velox use an `addInput` / `getOutput` interface: a driver thread loops over a pipeline's operators, moving a page or vector from one operator's `getOutput()` to the next one's `addInput()`, and asks `isBlocked()` before scheduling, so a thread never sleeps waiting for an exchange. DataFusion keeps a pull model but makes it asynchronous: each operator returns a stream of Arrow `RecordBatch`es that the Tokio runtime polls, so I/O waits do not block threads. Spark's whole-stage code generation uses push-style generated code inside a stage and an iterator at stage boundaries (see [Apache Spark](#spark)).

**Materialisation.** MonetDB's original model went further than batches: each operator processed an entire column and wrote a full intermediate column to memory. That is very fast per operator but uses memory bandwidth and space for every intermediate. Vectors sized to stay in the CPU cache were X100's compromise, and remain the standard.

> **Interview:** "What is a pipeline breaker, and why do push-based engines care?" An operator that must consume all its input before emitting output, such as a hash join build, aggregation or sort. Push engines split the plan into pipelines at breakers, run each pipeline in parallel over morsels, and only start a pipeline when the breakers it depends on are complete.
:::

::: check
?? In the Volcano model, what are the three methods of an operator, and what is the main performance cost of the model?
`open`, `next` and `close`. The cost is per-row overhead: one or more virtual function calls, type dispatch and branches for every row at every operator.

?? What is a morsel, and why does morsel-driven scheduling handle skew better than a fixed number of parallel partitions?
A small chunk of a pipeline's input. Threads take morsels dynamically, so if one morsel is slow, the other threads keep taking new ones, whereas a fixed partitioning leaves the other threads idle while the slow partition finishes.

?? Which operators break pipelines in a push-based engine?
Operators that must see all their input first: the build side of a hash join, hash aggregation, sort, window functions, and exchanges (shuffles).
:::

::: resources
- Paper: [MonetDB/X100: Hyper-Pipelining Query Execution](https://www.cidrdb.org/cidr2005/papers/P19.pdf) · Boncz, Zukowski and Nes, CIDR 2005; why Volcano wastes the CPU and the vectorized answer
- Paper: [Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework](https://db.in.tum.de/~leis/papers/morsels.pdf) · Leis et al., SIGMOD 2014; pipelines, morsels and the dispatcher
- Paper: [Efficiently Compiling Efficient Query Plans for Modern Hardware](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) · Neumann, VLDB 2011; push-based, data-centric execution
- Paper: [Push vs. Pull-Based Loop Fusion in Query Engines](https://arxiv.org/abs/1610.09166) · Shaikhha, Dashti and Koch; a careful comparison of the two control flows
- Course: [CMU 15-721 schedule](https://15721.courses.cs.cmu.edu/spring2024/schedule.html) · lectures on processing models, scheduling and vectorization with reading lists
:::

### Vectorized Execution & SIMD

::: plain
A vectorized engine works the way a factory handles parts: not one at a time, but by the tray. Each operator receives a tray of about two thousand values from one column, runs one tight loop over the whole tray, and passes the tray on. The bookkeeping (which function to call, what type the values are) happens once per tray instead of once per value, so almost all the time goes into the real work. This topic is about that execution model; the next one, SIMD Vectorization for Analytics, is about the CPU instructions that make each loop even faster.
:::

**The idea.** Vectorized execution keeps the Volcano structure (operators, pull or push) but changes the unit of work from a row to a **vector**: a fixed-size slice of one column, typically 1,024 to 8,192 values, plus a little metadata (a null mask, an optional selection vector). A batch of rows is a set of such vectors, one per column, sharing the same length. Operators are written as loops over vectors, and expressions are evaluated by calling **primitives**: small, pre-compiled functions such as "add two int64 vectors" or "compare a string vector with a constant".

Batch sizes in real engines:

| Engine | Unit | Default size |
|---|---|---|
| DuckDB | `DataChunk` of `Vector`s | 2,048 values (`STANDARD_VECTOR_SIZE`) |
| DataFusion | Arrow `RecordBatch` | 8,192 rows (`datafusion.execution.batch_size`) |
| ClickHouse | `Block` of columns | up to 65,409 rows (`max_block_size`) |
| Velox | `RowVector` of child vectors | 1,024 rows preferred for operator output (`preferred_output_batch_rows`) |
| Spark (vectorized Parquet reader, Arrow to Python) | `ColumnarBatch` / Arrow batch | 4,096 rows / 10,000 rows |

Why that size? Large enough that per-batch overhead (a virtual call, a type switch) is spread over thousands of values; small enough that all the vectors an operator touches stay in the L1 or L2 cache while it works (2,048 values of 8 bytes is 16 KiB per column). Full-column processing, as in early MonetDB, writes every intermediate column to main memory; cache-sized vectors avoid that.

**Where the speed comes from.** A short Python model counts how often the interpreter has to decide what to do (which operator, which type) for the expression `(price + tax) * 2`:

```python
dispatch = {"count": 0}

def add(a, b): return a + b
def mul(a, b): return a * b
OPS = {"+": add, "*": mul}

def eval_row(expr, row):                 # tuple-at-a-time interpreter
    dispatch["count"] += 1
    if isinstance(expr, str): return row[expr]
    if isinstance(expr, (int, float)): return expr
    op, left, right = expr
    return OPS[op](eval_row(left, row), eval_row(right, row))

def eval_vec(expr, batch):               # vector-at-a-time interpreter
    dispatch["count"] += 1
    if isinstance(expr, str): return batch[expr]
    if isinstance(expr, (int, float)): return [expr] * len(next(iter(batch.values())))
    op, left, right = expr
    f = OPS[op]
    return [f(x, y) for x, y in zip(eval_vec(left, batch), eval_vec(right, batch))]  # one tight loop

expr = ("*", ("+", "price", "tax"), 2)   # (price + tax) * 2
n = 4_096
rows = [{"price": i, "tax": 1} for i in range(n)]
out1 = [eval_row(expr, r) for r in rows]
print("tuple-at-a-time dispatches:", dispatch["count"])     # 20480

dispatch["count"] = 0
cols = {"price": list(range(n)), "tax": [1] * n}
out2 = []
for start in range(0, n, 2_048):                            # vectors of 2,048 values
    batch = {k: v[start:start + 2_048] for k, v in cols.items()}
    out2 += eval_vec(expr, batch)
print("vector-at-a-time dispatches:", dispatch["count"])    # 10
print(out1 == out2)                                         # True
```

Same result, 2,048 times fewer interpretation steps. In a compiled engine the gain is larger than the count suggests, because a primitive's inner loop is a few instructions with no function calls and no type checks: the compiler can unroll it, keep values in registers, let the CPU overlap independent iterations, and turn it into SIMD instructions (next topic). That is why a vectorized *interpreter* can run within a small factor of hand-written C code.

**Vector representations.** Engines avoid decompressing data they do not need to. DuckDB has **flat** vectors (a plain array), **constant** vectors (one value standing for the whole vector, for literals and run-length-encoded data), **dictionary** vectors (a small array of distinct values plus one index per row) and **sequence** vectors; Velox has flat, constant, dictionary and **lazy** vectors (loaded only if a later filter leaves rows to read). Functions can then work on the compact form. Applying `upper()` to a dictionary-encoded country column needs one call per distinct value, not per row:

```python
calls = 0
def upper(s):
    global calls
    calls += 1
    return s.upper()

# A dictionary vector: a small array of distinct values plus one index per row.
dictionary = ["de", "fr", "es"]
indices = [0, 1, 0, 2, 0, 0, 1, 2] * 256          # 2,048 rows

flat = [upper(dictionary[i]) for i in indices]      # decode first, then compute
print(calls)                                        # 2048

calls = 0
new_dictionary = [upper(v) for v in dictionary]     # compute on the dictionary only
result = (new_dictionary, indices)                  # the indices are reused untouched
print(calls)                                        # 3
print([new_dictionary[i] for i in indices] == flat) # True
```

This connects directly to the encodings in [Storage internals and hardware](#hardware): Parquet dictionary pages can stay dictionary vectors in memory.

**How filters work in a vectorized engine.** A filter does not copy surviving rows into a new batch. It produces a **selection vector**, a short array of the positions that passed, and downstream primitives process only those positions; when few rows survive, an operator may compact them into a new dense batch. How a selection vector is computed without branches, and with SIMD, is covered in SIMD Vectorization for Analytics, the next topic.

**Engines built this way.** MonetDB/X100 (which became Vectorwise, now Actian Vector), DuckDB, ClickHouse, Velox, DataFusion, Databricks Photon and Snowflake are columnar and vectorized. Spark is the notable hybrid: its vectorized readers produce columnar batches, but whole-stage code generation processes rows (see [Apache Spark](#spark)), which is why native vectorized engines for Spark exist (Apache Comet & Gluten, later in this section).

> **Pitfall:** Vectorized engines are fast at bulk work and comparatively slow at returning one row. Point lookups, row-by-row inserts and very selective single-row queries pay batch overhead for nothing. That is the OLTP world's job (see [Databases: OLTP, NoSQL and specialised](#databases)).

> **Tip:** Python UDFs break vectorization in every engine: the engine must turn vectors back into Python objects, one call per row. Prefer built-in functions, or vectorized UDFs that receive whole Arrow batches (pandas UDFs in Spark, Arrow-based UDFs in DuckDB).

::: deeper
**Vectorization versus compilation.** Kersten et al. ("Everything you always wanted to know about compiled and vectorized queries but were afraid to ask", VLDB 2018) built two engines that differed only in execution model. Neither won everywhere: compiled code was better for computation-heavy queries (it keeps values in registers across operators), vectorized code was better for hash joins and memory-bound work (independent loads in a tight loop let the CPU overlap cache misses). Both were far faster than Volcano. Photon's authors chose vectorized interpretation partly for engineering reasons: it is easier to profile, debug and specialise at run time (see Runtime Code Generation, below).

**Adaptivity inside vectors.** Because primitives run per vector, an engine can make decisions per vector: check a vector's null mask once and use a no-null fast path, detect that a string vector is pure ASCII and skip UTF-8 handling, reorder `AND` terms so the most selective runs first (Velox does this adaptively), or switch from a selection vector to a dense loop when most rows pass.

**Late materialisation.** Column-at-a-time engines delay assembling rows until they must. A query that filters on `status` and returns `payload` reads `status` first and fetches `payload` values only for selected positions. Velox's lazy vectors and Parquet readers that evaluate filters before decoding other columns follow this idea.

**String layouts.** Variable-length strings are the hardest part of vectorization. Umbra introduced 16-byte "German-style" string headers holding the length, a 4-byte prefix and either the full short string or a pointer; Velox adopted it as `StringView`, and Apache Arrow added the equivalent `Utf8View` type. Comparisons and filters can often decide using the inline prefix without touching the heap.

> **Interview:** "Why is a vector size of about 1,000 to 2,000 values common, rather than 1 or 1,000,000?" One value per call leaves the per-call interpretation overhead dominant. A million values per call writes intermediates to main memory and loses cache locality. A few thousand values make overhead negligible while each operator's working set stays in L1 or L2 cache.
:::

::: check
?? What is the main overhead that vectorized execution removes, compared with the Volcano model?
Per-row interpretation overhead: virtual function calls, type dispatch and branching for every row at every operator. Vectorized engines pay it once per vector of thousands of values.

?? Why might an engine evaluate a function on a dictionary vector's dictionary rather than on every row?
The dictionary holds only the distinct values, so the function runs once per distinct value instead of once per row, and the row indices are reused unchanged.

?? How does a vectorized filter usually pass its result downstream?
As a selection vector: an array of the positions in the batch that passed, so later operators process only those rows without copying the batch.
:::

::: resources
- Paper: [MonetDB/X100: Hyper-Pipelining Query Execution](https://www.cidrdb.org/cidr2005/papers/P19.pdf) · the paper that defined vectorized execution and cache-sized vectors
- Paper: [Everything You Always Wanted to Know About Compiled and Vectorized Queries But Were Afraid to Ask](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) · Kersten et al., VLDB 2018; vectorized versus compiled, measured fairly
- Docs: [DuckDB execution format](https://duckdb.org/docs/stable/internals/vector) · flat, constant, dictionary and sequence vectors, and DataChunks
- Paper: [Velox: Meta's Unified Execution Engine](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf) · Pedreira et al., VLDB 2022; vectors, encodings and adaptive expression evaluation
- Paper: [Photon: A Fast Query Engine for Lakehouse Systems](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf) · Behm et al., SIGMOD 2022; why Databricks chose a vectorized interpreter
:::

### SIMD Vectorization for Analytics

::: plain
SIMD stands for "single instruction, multiple data". A normal instruction adds two numbers; a SIMD instruction adds two *rows* of numbers, 4, 8 or 16 pairs at once, in the same time. It is like a cashier who can scan a whole tray of items with one beep instead of one beep per item. The previous topic, Vectorized Execution & SIMD, explained how engines organise work into column vectors; this topic is about the CPU instructions that then chew through those vectors several values at a time.
:::

**Registers and lanes.** A SIMD register is a wide register divided into **lanes**. A 256-bit register holds eight 32-bit integers, four 64-bit doubles or thirty-two bytes, and one instruction applies the same operation to every lane. The instruction sets you will meet:

| Instruction set | Register width | 32-bit lanes | Notes |
|---|---|---|---|
| SSE2 to SSE4.2 (x86) | 128 bits | 4 | Baseline on every x86-64 CPU; SSE4.2 adds CRC32 and string-compare instructions |
| AVX2 (x86) | 256 bits | 8 | Widely available since about 2013; the common target for data engines |
| AVX-512 (x86) | 512 bits | 16 | Mask registers, compress and expand, conflict detection; on recent Intel Xeon and AMD Zen 4 and later |
| NEON (Arm) | 128 bits | 4 | Every 64-bit Arm server core |
| SVE / SVE2 (Arm) | 128 to 2,048 bits, chosen by the chip | varies | Length-agnostic code with predicate registers; Google Axion (Arm Neoverse V2) implements SVE2 |

On Google Cloud, the machine series decides what you get: Axion-based C4A VMs are Arm (NEON and SVE2); recent Intel and AMD series support AVX2 and, on newer generations, AVX-512. Check from inside the VM:

```bash
lscpu | grep -o -E 'avx2|avx512[a-z_]*|sve2?|asimd' | sort -u
```

**What SIMD speeds up in a query engine.**

- **Filters**: compare 8 or 16 values with a constant in one instruction, producing a **mask** (one bit per lane).
- **Aggregations**: sum, min, max and count over a column, with several partial accumulators in one register combined at the end.
- **Decoding**: unpacking bit-packed integers, applying frame-of-reference offsets, decoding dictionaries (the encodings in [Storage internals and hardware](#hardware)).
- **Hashing and Bloom filters**: hash several keys at once; blocked Bloom filters fit one probe into one SIMD word.
- **Strings and parsing**: finding delimiters, validating UTF-8, parsing JSON (simdjson processes 64 bytes per step).
- **Sorting**: sorting networks inside registers (Google Highway's vqsort, Intel's x86-simd-sort).

SIMD helps least where work is dominated by random memory access, such as probing a hash table far larger than the cache: the CPU waits for memory, not for arithmetic.

**Selection vectors without branches.** The core of a vectorized filter is "write down the positions where `x > c`". The obvious loop contains an `if`; the CPU guesses which way each `if` goes, and every wrong guess (a **branch misprediction**) costs around 15 to 20 cycles. At 50% selectivity on random data it guesses wrong about half the time. The branch-free version always writes the position and advances the output cursor by 0 or 1. The SIMD version compares a whole register, turns the comparison into a bitmask, and writes out the positions of the set bits. This Python model mimics the AVX2 steps lane by lane (it shows the logic, not the speed):

```python
LANES = 8                                  # one 256-bit AVX2 register holds 8 x 32-bit ints

def cmpgt_mask(chunk, c):                  # like _mm256_cmpgt_epi32 + movemask: one bit per lane
    return sum(1 << lane for lane, x in enumerate(chunk) if x > c)

def selection_vector(values, c):
    sel = []
    for base in range(0, len(values), LANES):
        mask = cmpgt_mask(values[base:base + LANES], c)
        while mask:                        # walk the set bits: no branch on the data itself
            low = mask & -mask             # lowest set bit
            sel.append(base + low.bit_length() - 1)
            mask ^= low
    return sel

values = [5, 12, 7, 30, 1, 18, 9, 25, 40, 2, 11, 3, 16, 8, 21, 6]
print(f"{cmpgt_mask(values[:8], 10):08b}")   # 10101010   (lane 0 is the rightmost bit)
sel = selection_vector(values, 10)
print(sel)                                  # [1, 3, 5, 7, 8, 10, 12, 14]
print([values[i] for i in sel])             # [12, 30, 18, 25, 40, 11, 16, 21]

# Branch-free scalar version: always write, advance the cursor by 0 or 1.
out, n = [0] * len(values), 0
for i, x in enumerate(values):
    out[n] = i
    n += x > 10                             # True counts as 1, False as 0
print(out[:n] == sel)                       # True
```

[[fig:queryengines_simd]]

**Three ways to get SIMD code.**

1. **Auto-vectorization.** Write a plain loop and let the compiler (Clang, GCC) emit SIMD instructions. It works when the loop has a known trip count, contiguous arrays, no dependence between iterations other than a reduction, no possible aliasing between input and output, and conditions that can become selects instead of branches. Clang vectorizes at `-O2`; GCC does so at `-O2` since GCC 12 with a conservative cost model, and more aggressively at `-O3`. Ask the compiler what it did:

```c
#include <stdint.h>
#include <stddef.h>

/* SELECT SUM(amount) WHERE amount > 100: written so the compiler can vectorize it */
int64_t sum_gt(const int32_t *restrict amount, size_t n, int32_t c) {
    int64_t total = 0;
    for (size_t i = 0; i < n; i++)
        total += (amount[i] > c) ? amount[i] : 0;   /* a select, not a branch */
    return total;
}
```

```bash
clang -O2 -c sum.c -Rpass=loop-vectorize          # GCC: gcc -O3 -c sum.c -fopt-info-vec
# sum.c:7:5: remark: vectorized loop (vectorization width: 4, interleaved count: 4)
```

That remark is from Apple Clang on an Arm machine (four 32-bit lanes of NEON, four independent accumulators); on x86 with `-mavx2` the width becomes 8.

2. **Intrinsics.** C functions that map one-to-one to instructions. The AVX2 version of the selection loop compares eight values, extracts an 8-bit mask and walks its set bits:

```c
#include <immintrin.h>
#include <stdint.h>
#include <stddef.h>

/* Write the positions of values > c into sel; return how many there are. */
size_t select_gt_avx2(const int32_t *v, size_t n, int32_t c, uint32_t *sel) {
    __m256i limit = _mm256_set1_epi32(c);             /* c in all 8 lanes */
    size_t k = 0, i = 0;
    for (; i + 8 <= n; i += 8) {
        __m256i x = _mm256_loadu_si256((const __m256i *)(v + i));
        __m256i gt = _mm256_cmpgt_epi32(x, limit);    /* lane = all ones where x > c */
        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(gt));   /* 8 bits, one per lane */
        while (mask) {                                /* walk the set bits */
            sel[k++] = (uint32_t)(i + __builtin_ctz(mask));
            mask &= mask - 1;
        }
    }
    for (; i < n; i++)                                /* scalar tail */
        if (v[i] > c) sel[k++] = (uint32_t)i;
    return k;
}
```

With AVX-512, `_mm512_cmpgt_epi32_mask` returns the mask directly in a mask register, and `_mm512_mask_compressstoreu_epi32` writes only the selected lanes to memory in one instruction, which removes the bit-walking loop entirely. Intrinsics give full control but tie code to one instruction set, so engines need a version per target.

3. **Portable SIMD libraries.** Google Highway and xsimd wrap the instruction sets behind one C++ API and can select the best implementation at run time. Velox uses xsimd. The Java Vector API (still an incubator module in recent JDKs) and Rust's portable SIMD module serve the same purpose in those languages.

**How engines choose.**

| Engine | Approach |
|---|---|
| DuckDB | No explicit SIMD: plain C++ loops written so the compiler auto-vectorizes them, for portability (its FAQ notes that porting to Apple Silicon took minutes) |
| ClickHouse | Hand-written SIMD in hot paths (filters, string search, hashing, decoding) plus **runtime CPU dispatch**: several builds of a function for different instruction sets, chosen at startup by checking the CPU |
| Velox | Mostly auto-vectorized loops, explicit SIMD through xsimd where it pays (filters in scans, hashing) |
| DataFusion / arrow-rs | Mostly relies on the Rust compiler's auto-vectorization over Arrow arrays |

> **Pitfall:** Building with `-march=native` on your workstation and deploying to a different VM family. The binary may use AVX-512 or SVE instructions the production CPU lacks and crash with "illegal instruction". Build for an explicit target (`-march=x86-64-v3` for AVX2-class x86, `-march=armv8.2-a` or a specific Neoverse target for Arm) or use runtime dispatch.

> **Pitfall:** Floating-point sums do not auto-vectorize by default, because SIMD reordering changes rounding. Compilers only vectorize them with flags such as `-ffast-math` or `-fassociative-math`, which also changes results slightly. Integer and decimal sums do not have this problem.

> **Tip:** On Google Cloud, compare price per query rather than speed per core when choosing between x86 (AVX-512) and Axion (SVE2) for a vectorized engine. Measure your workload; wider registers do not automatically win when the query is memory-bound.

::: deeper
**AVX-512 in practice.** Early AVX-512 chips (Intel Skylake-SP) lowered clock frequency when 512-bit instructions ran, so some code was faster with AVX2. Later Intel generations and AMD Zen 4 and newer reduced or removed this penalty, which is one reason AVX-512 is used more widely in data systems now. AVX-512's mask registers also remove the need for scalar tail loops: the last partial vector is processed with a mask.

**Gathers and scatters.** `vpgatherdd` loads eight values from eight arbitrary addresses in one instruction, useful for dictionary decoding and hash-table probes. It is not eight times faster than scalar loads, because each lane still needs its own memory access; it mainly saves instructions.

**Length-agnostic SIMD.** SVE code does not hard-code the lane count: a loop asks the hardware how many lanes it has and uses predicate registers for the tail. The same binary runs on 128-bit and 512-bit implementations. RISC-V's vector extension follows the same idea.

**Research.** Polychroniou, Raghavan and Ross ("Rethinking SIMD vectorization for in-memory databases", SIGMOD 2015) showed SIMD designs for selection, hash tables, partitioning and sorting, built on gather, scatter and compress primitives. Ross's earlier work on selection conditions established the branch-free selection technique used above.

> **Interview:** "How would you make `WHERE x > 10` fast on a column of 32-bit integers?" Store it contiguously in a column vector, evaluate the comparison with SIMD (8 or 16 lanes per instruction), turn the result into a bitmask, and produce a selection vector without data-dependent branches, so performance does not depend on selectivity. Skip whole blocks first using zone maps (min and max per block).
:::

::: check
?? How many 32-bit integers does one AVX2 instruction process, and how many with AVX-512?
Eight (256 bits) and sixteen (512 bits).

?? Why is a branch-free selection loop often faster than a loop with `if (x > c)` at around 50% selectivity?
At 50% selectivity on unsorted data the branch predictor guesses wrong about half the time, and each misprediction costs many cycles. The branch-free loop does the same work for every element, with no mispredictions.

?? Why does DuckDB avoid hand-written SIMD intrinsics, and what does it do instead?
Intrinsics tie code to specific instruction sets, which hurts portability and complicates builds. DuckDB writes simple loops that compilers auto-vectorize for whatever hardware it is built for.
:::

::: resources
- Docs: [Intel Intrinsics Guide](https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html) · every SSE, AVX2 and AVX-512 intrinsic with latency notes
- Docs: [LLVM auto-vectorization](https://llvm.org/docs/Vectorizers.html) · what Clang's loop and SLP vectorizers can and cannot do, with examples
- Paper: [Rethinking SIMD Vectorization for In-Memory Databases](https://www.cs.columbia.edu/~orestis/sigmod15.pdf) · Polychroniou, Raghavan and Ross, SIGMOD 2015; SIMD selections, hash tables and partitioning
- Blog: [CPU dispatch in ClickHouse](https://clickhouse.com/blog/cpu-dispatch-in-clickhouse) · how one binary uses the best instruction set available at run time
- Repo: [Google Highway](https://github.com/google/highway) · a portable C++ SIMD library with runtime dispatch, including vqsort
- Docs: [DuckDB FAQ](https://duckdb.org/faq) · the "Does DuckDB use SIMD?" answer explaining implicit SIMD
- Book: [Agner Fog's optimization manuals](https://www.agner.org/optimize/) · instruction tables and microarchitecture guides for x86
:::

### Runtime Code Generation — LLVM JIT

::: plain
An interpreter reads a recipe and follows it step by step, checking at every step what to do next. A compiler first rewrites the recipe as a set of reflexes, then just does it. Runtime code generation means the database writes a small, custom program for *your* query while it runs, compiles it to machine code, and executes that, so none of the time goes into "what do I do next?". The catch is that writing and compiling the program takes time too, which only pays off when the query runs long enough.
:::

**Interpretation, three ways.** Every engine has to evaluate expressions such as `(price + tax) * qty > 100` and run operators. There are three broad strategies:

1. **Tree-walking interpretation**: walk the expression tree for every row, calling a function per node. This is the Volcano-era default.
2. **Bytecode interpretation**: compile the plan to instructions for a small virtual machine and interpret those. SQLite does exactly this; `EXPLAIN` shows the bytecode:

```python
import sqlite3

con = sqlite3.connect(":memory:")
con.execute("CREATE TABLE t (price INTEGER, qty INTEGER)")
for addr, opcode, *rest in con.execute("EXPLAIN SELECT price * qty FROM t WHERE price > 100"):
    print(addr, opcode)
```

With SQLite 3.53 the body of the loop (addresses 3 to 9) reads: `Column`, `Le` (jump to the next row if `price <= 100`), `Column`, `Column`, `Multiply`, `ResultRow`, `Next`. The exact listing varies between SQLite versions.

3. **Compilation**: generate code specialised to this query (types known, constants inlined, operators fused) and compile it to machine code. Vectorized interpretation, covered in Vectorized Execution & SIMD, is the alternative that keeps an interpreter but amortises it over vectors.

The idea in miniature, using Python's own compiler as the "back end":

```python
# WHERE (price + tax) * qty > 100, as an expression tree
expr = (">", ("*", ("+", "price", "tax"), "qty"), 100)

visits = 0
def interpret(e, row):                       # walks the tree for every row
    global visits
    visits += 1
    if isinstance(e, str): return row[e]
    if isinstance(e, int): return e
    op, a, b = e
    x, y = interpret(a, row), interpret(b, row)
    return x + y if op == "+" else x * y if op == "*" else x > y

def to_source(e):                            # code generation: tree -> source text
    if isinstance(e, str): return e
    if isinstance(e, int): return str(e)
    op, a, b = e
    return f"({to_source(a)} {op} {to_source(b)})"

src = f"def pred(price, tax, qty):\n    return {to_source(expr)}\n"
print(src, end="")
# def pred(price, tax, qty):
#     return (((price + tax) * qty) > 100)
namespace = {}
exec(compile(src, "<generated>", "exec"), namespace)   # "compile" once per query
pred = namespace["pred"]

rows = [{"price": p, "tax": 2, "qty": q} for p in range(10) for q in range(1, 21)]
a = [interpret(expr, r) for r in rows]
b = [pred(r["price"], r["tax"], r["qty"]) for r in rows]
print(a == b, sum(b), visits)                # True 48 1400
```

The interpreter made 1,400 node visits for 200 rows; the generated function has no tree left to walk. A real engine does the same with a real compiler back end and goes much further: it fuses whole pipelines (scan, filter, join probe, aggregate) into one loop, so a row's values stay in CPU registers from the moment they are loaded until they land in a hash table.

**Where code generation is used.**

| Engine | What is generated | Compiler |
|---|---|---|
| HyPer, Umbra | Whole pipelines, push-based (produce/consume) | LLVM; Umbra adds a fast custom back end |
| Spark | Whole-stage Java code per stage | Janino, then the JVM's JIT (see [Apache Spark](#spark)) |
| PostgreSQL | Expression evaluation and tuple deforming (unpacking rows), when the plan is expensive | LLVM |
| ClickHouse | Expressions, aggregate functions and sort comparators that recur | LLVM |
| Impala | Expressions and per-query operator code | LLVM |
| Arrow Gandiva | Expressions over Arrow batches | LLVM |

Vectorized engines such as DuckDB, Velox, DataFusion and Photon do not compile queries; they rely on pre-compiled vectorized kernels instead.

**PostgreSQL's JIT** is a good place to watch the trade-off. It is used only when the planner's cost estimate is above a threshold (`jit_above_cost`, default 100,000), with inlining and full optimization at higher thresholds (`jit_inline_above_cost` and `jit_optimize_above_cost`, default 500,000 each). PostgreSQL must be built with LLVM support.

```sql
-- PostgreSQL: force JIT for a demo, then look at the JIT section of the plan
SET jit = on;
SET jit_above_cost = 0;
EXPLAIN (ANALYZE) SELECT sum(price * qty) FROM order_lines WHERE price > 100;
```

At the bottom of the output, a `JIT:` section reports how many functions were generated, which options were used (inlining, optimization, expressions, deforming) and the time spent generating, optimizing and emitting code. On a small table that time can exceed the query's execution time, which is exactly why the threshold exists.

**ClickHouse** compiles an expression only after it has been seen several times (`compile_expressions` is on by default and `min_count_to_compile_expression` is 3), caches the compiled code, and applies the same idea to aggregate functions (`compile_aggregate_expressions`) and sort comparisons (`compile_sort_description`). Compiling only repeated work keeps one-off queries from paying the compile cost.

> **Pitfall:** Code generation can fail or be skipped silently. Spark falls back to interpreted execution when a generated method is too large (`spark.sql.codegen.hugeMethodLimit`) or for unsupported expressions, and the query just runs slower. Check plans for operators outside codegen stages before assuming they were compiled.

> **Tip:** For short, interactive queries, compilation latency matters more than peak speed. If a JIT-enabled engine shows high planning or "compile" time in its profile on small queries, raise its threshold rather than turning JIT off for everything.

::: deeper
**Compile time versus run time.** An optimizing LLVM compile of a large query can take from tens of milliseconds to seconds, while a vectorized interpreter starts instantly. HyPer answered with **adaptive execution**: start every pipeline in a fast bytecode interpreter, measure progress, and compile only pipelines that will run long enough to benefit, switching mid-query. Umbra's **Flying Start** back end generates machine code directly from its own IR in a single pass, nearly as fast as interpretation to produce, and uses LLVM only for long-running pipelines.

**What compilation buys.** Values stay in registers across operators; type and constant checks vanish; branches on query parameters are resolved at compile time. Kersten et al. (VLDB 2018) found compiled engines faster for computation-heavy pipelines, and vectorized engines faster for memory-bound hash joins, where a tight loop of independent probes lets the CPU overlap cache misses. Several systems now mix the two: compiled engines call vectorized, pre-compiled routines for scans and decompression, and vectorized engines JIT-compile individual expressions.

**Engineering costs.** Generated code is hard to profile (no symbols), hard to debug and hard to test across all combinations of types and operators. Photon's paper cites observability and development speed as reasons to choose a vectorized interpreter over code generation. Code caches also need eviction, and a compiler crash inside a database process is a stability risk.

> **Interview:** "When does query compilation beat a vectorized interpreter?" For long-running, CPU-bound pipelines with complex expressions, where keeping values in registers across fused operators saves more than the compile time costs. For short queries, memory-bound joins and highly varied ad-hoc workloads, vectorized interpretation usually wins or ties, and is simpler to build and observe.
:::

::: check
?? What does whole-pipeline code generation achieve that expression-only code generation does not?
It fuses several operators (scan, filter, join probe, aggregate) into one loop, so values stay in registers between operators instead of being materialised and passed through operator interfaces.

?? Why does PostgreSQL use JIT only above a cost threshold?
Compilation itself takes time (milliseconds to more). For cheap queries the compile time exceeds the time saved, so JIT is only worth it for expensive plans.

?? Name two ways engines reduce the cost of compilation.
Compile only repeated or long-running work (ClickHouse's minimum count, HyPer's adaptive switch from interpreter to compiled code), cache compiled code, or use a faster, less optimizing back end (Umbra's Flying Start, Spark's Janino).
:::

::: resources
- Paper: [Efficiently Compiling Efficient Query Plans for Modern Hardware](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) · Neumann, VLDB 2011; the produce/consume model for LLVM-compiled pipelines
- Docs: [PostgreSQL JIT compilation](https://www.postgresql.org/docs/current/jit.html) · what is compiled, the cost thresholds and how to read the JIT section of EXPLAIN
- Blog: [JIT in ClickHouse](https://clickhouse.com/blog/clickhouse-just-in-time-compiler-jit) · how ClickHouse compiles expressions, aggregates and comparators with LLVM
- Paper: [Everything You Always Wanted to Know About Compiled and Vectorized Queries But Were Afraid to Ask](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) · Kersten et al., VLDB 2018; when compilation wins and when it does not
- Docs: [LLVM tutorial: My First Language Frontend](https://llvm.org/docs/tutorial/) · build a small JIT compiler with LLVM step by step
- Docs: [Gandiva expression compiler](https://arrow.apache.org/docs/cpp/gandiva.html) · Arrow's LLVM-based compiler for expressions over columnar batches
:::

## Modern engines

The ideas of the first two groups (rule and cost-based planning, vectorized execution, SIMD, compilation) used to live inside monolithic databases. The newest engines package them as **libraries** that other systems assemble: one project supplies the executor, another the planner, a third the file format, with Apache Arrow as the shared in-memory format and Substrait as an optional shared plan format. This is often called a **composable** data stack.

This group looks at the two most important engine libraries, **Velox** (C++, from Meta) and **Apache DataFusion** (Rust), then at the projects that put them under Spark (**Apache Comet** and **Apache Gluten**, and Google's Lightning Engine built on Gluten and Velox), and finally at **WebAssembly**, which lets an engine such as DuckDB run inside a browser or a sandbox.

### Velox — Meta's Unified Execution Engine

::: plain
Velox is a high-performance "engine room" without the rest of the ship. It has no SQL parser and no optimizer: you hand it a plan that something else has already decided on, and it runs that plan very fast on columnar data. Meta built it so that Presto, Spark, streaming and machine learning pipelines could share one fast executor instead of each maintaining its own, and released it as open source.
:::

**Why it exists.** Meta ran many data systems (Presto for interactive SQL, Spark for batch, stream processors, ML data preprocessing), each with its own execution engine, its own function library and its own subtle differences. Fixing a bug or speeding up a function meant doing it several times, and the same SQL function could behave differently in two engines. Velox, open-sourced in 2022 and described in a VLDB 2022 paper, is a single C++ library for the execution layer that those systems share.

**What it contains**, from the bottom up:

- **Type system**: scalar types plus `ARRAY`, `MAP` and `ROW`, and support for custom logical types.
- **Vectors**: Velox's columnar in-memory format, compatible with Apache Arrow's layout but with extra encodings (flat, constant, dictionary, lazy) and its `StringView` string layout, which Arrow later adopted as `Utf8View`.
- **Expression evaluation**: a vectorized evaluator for expression trees, with constant folding, common subexpression elimination, fast paths when vectors have no nulls or only ASCII strings, **peeling** of dictionary encodings (evaluate on the distinct values, keep the indices; the dictionary example in Vectorized Execution & SIMD is this idea), and adaptive reordering of `AND` and `OR` terms by measured selectivity.
- **Function packages**: the scalar, aggregate and window functions of Presto and of Spark SQL, each with its own semantics (Spark and Presto differ on things such as integer overflow and null handling), so a host engine gets results that match its own.
- **Operators**: table scan, filter and project, hash aggregation, hash join (build and probe), merge join, order by, top-N, window, unnest, exchange, local partitioning, table writer.
- **Connectors and file formats**: a Hive-style connector with readers for Parquet, ORC and DWRF (Meta's ORC variant), with filter push-down into the reader and dynamic filters from joins.
- **Resource management**: memory pools per query and operator, a memory arbitrator that moves memory between queries, and **spilling** to disk for joins, aggregations, sorts and windows.

**How a query runs.** The host engine (Presto, Spark through Gluten) plans the query and sends Velox a tree of **plan nodes** for one fragment. Velox creates a **Task**, splits the plan into pipelines, and runs each pipeline on one or more **Drivers**, each driver a thread moving vectors through a chain of **Operators** with `addInput()`, `getOutput()`, `needsInput()` and `isBlocked()`: the push-style interface described in the execution models topic earlier in this section. Splits (pieces of files) are added to the scan as the host's scheduler assigns them.

Velox's tests build plans with a fluent builder, which shows what a plan fragment looks like. A sketch, not a full program:

```cpp
// C++: scan two columns, filter, project and aggregate, as a Velox plan fragment
auto plan = PlanBuilder()
    .tableScan(ROW({"country", "amount"}, {VARCHAR(), DOUBLE()}))
    .filter("amount > 100.0")
    .project({"country", "amount * 1.19 AS gross"})
    .singleAggregation({"country"}, {"sum(gross)"})
    .planNode();

auto result = AssertQueryBuilder(plan)
    .splits(makeHiveConnectorSplits(parquetFiles))
    .copyResults(pool());
```

New scalar functions are written once with the "simple function" API, and Velox generates the vectorized wrapper, null handling and encoding handling around them:

```cpp
template <typename T>
struct TimesTwoFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  void call(int64_t& result, const int64_t& a) {
    result = a * 2;
  }
};

registerFunction<TimesTwoFunction, int64_t, int64_t>({"times_two"});
```

**Who uses it.**

| Host | How Velox is used |
|---|---|
| Presto (Presto C++, also called Prestissimo) | C++ workers built on Velox replace Java workers; the Java coordinator still parses and plans |
| Apache Spark via Apache Gluten | Gluten converts Spark physical plans to a Substrait-based form and runs them on Velox inside executors |
| Google Cloud Lightning Engine | Its Native Query Execution option for Managed Service for Apache Spark (formerly Dataproc) is based on Gluten and Velox |
| Meta internal systems | Stream processing and ML data preprocessing share the same executor |

> **Pitfall:** "Uses Velox" does not mean "behaves like Presto". Velox has separate Presto and Spark function packages; a host must register the right one, and a function missing from a package makes the host fall back or fail. When migrating, compare query results on real data, not only run times.

> **Tip:** You will rarely program Velox directly. Its value to a data engineer is in the systems built on it: when evaluating Presto C++, Gluten or Lightning Engine's native execution, check operator and function coverage for your queries and the fallback behaviour for everything else.

::: deeper
**Adaptivity.** Velox makes several decisions at run time rather than in the plan. Filters inside table scans are reordered by measured cost and selectivity, so the cheapest, most selective condition runs first. Hash joins push a dynamic filter (the set or range of build-side keys) into the probe-side scan, so non-matching rows are skipped at the reader. Expression evaluation can switch between dense loops and selection vectors depending on how many rows are active.

**Memory arbitration.** Many queries share a worker. When one needs more memory than its pool has, the arbitrator can grow it from free capacity, shrink other queries' pools, or ask operators to spill, and only as a last resort aborts a query. This is what lets a C++ engine without a garbage collector run many concurrent queries safely.

**Why C++ and not the JVM?** Explicit memory control (no garbage collection pauses, see [Storage internals and hardware](#hardware) on JVM tuning), direct use of SIMD, and data layouts chosen for the cache. The cost is a harder build and a JNI boundary when a JVM host such as Spark calls in.

**Composability in practice.** Velox illustrates the "composable data management" argument made by its authors: separate the language front end, the optimizer and the executor, standardise the interfaces between them (Arrow for data, Substrait for plans), and each part can be reused across systems.

> **Interview:** "What does Velox provide and what does it not?" It provides the execution layer: types, vectors, expression evaluation, functions, operators, connectors, memory management and spilling. It does not provide a SQL parser, an optimizer, a cluster scheduler or a metastore; the host engine supplies those and sends Velox plan fragments.
:::

::: check
?? Which parts of a database does Velox leave to its host engine?
SQL parsing, query optimization, distributed scheduling and the catalog. Velox receives an already planned fragment and executes it.

?? Why does Velox ship separate Presto and Spark function packages?
The two engines define some functions differently (overflow, null handling, casting rules). Each host needs results identical to its own semantics.

?? How do Presto and Spark each use Velox?
Presto C++ replaces Java workers with C++ workers built on Velox, keeping the Java coordinator. Spark uses it through Apache Gluten, which converts Spark physical plans for Velox to execute inside executors.
:::

::: resources
- Docs: [Velox documentation](https://facebookincubator.github.io/velox/) · developer guides for vectors, expression evaluation, operators and functions
- Paper: [Velox: Meta's Unified Execution Engine](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf) · Pedreira et al., VLDB 2022; design, adaptivity and production results
- Repo: [Velox on GitHub](https://github.com/facebookincubator/velox) · source, including `PlanBuilder` and many operator tests
- Blog: [Velox open source execution engine](https://engineering.fb.com/2023/03/09/open-source/velox-open-source-execution-engine/) · Meta's overview of why they built it and where it runs
- Docs: [Presto C++](https://prestodb.io/docs/current/presto-cpp.html) · how Presto's native workers built on Velox are configured and what they support
- Docs: [Velox project site](https://velox-lib.io/) · news, blog posts and the community around the library
:::

### Apache DataFusion — Rust Query Engine

::: plain
Apache DataFusion is a complete query engine you can embed as a library, written in Rust. Where Velox is only the engine room, DataFusion is a whole ship kit: SQL parser, planner, optimizer and a vectorized executor, all built on Apache Arrow. Teams use it to build their own databases and tools without starting from zero, much as many web frameworks are built on the same HTTP library.
:::

**What it is.** DataFusion started in 2017 as a personal project of Andy Grove, was donated to the Apache Arrow project in 2019, and became its own top-level Apache project in 2024. It is a library, not a server: you add it as a Rust dependency (or use the Python bindings or the `datafusion-cli` shell), point it at data, and run SQL or DataFrame queries. Its data format, from file reading to results, is Apache Arrow.

**The pipeline, with DataFusion's names.**

1. **SQL** is parsed by the `sqlparser-rs` crate; the DataFrame API builds the same structures directly.
2. A **`LogicalPlan`** tree (Projection, Filter, Aggregate, Join, TableScan...) is produced and checked by **analyzer** rules (type coercion, name resolution).
3. **Logical optimizer rules** rewrite it: filter and projection push-down, constant folding and expression simplification, common subexpression elimination, subquery decorrelation, limit push-down, eliminating cross joins.
4. The physical planner turns it into an **`ExecutionPlan`** tree (`ProjectionExec`, `FilterExec`, `AggregateExec`, `HashJoinExec`, `SortExec`, `RepartitionExec`, file scans), and **physical optimizer rules** enforce the required partitioning and sort order and choose join sides.
5. **Execution**: calling `execute(partition)` on the root returns an asynchronous **stream of Arrow `RecordBatch`es** (8,192 rows by default). Each plan node pulls batches from its children's streams. Parallelism comes from **partitions**: by default as many as there are CPU cores (`target_partitions`), with `RepartitionExec` spreading batches between them, all driven by the Tokio async runtime.

You can see all of this from Python (`pip install datafusion`):

```python
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_parquet("trips", "trips.parquet")
df = ctx.sql("""
    SELECT payment_type, count(*) AS trips, avg(fare) AS avg_fare
    FROM trips
    WHERE fare > 0
    GROUP BY payment_type
    ORDER BY trips DESC
""")
df.show()                                    # prints the result table
ctx.sql("EXPLAIN SELECT payment_type, avg(fare) FROM trips GROUP BY payment_type").show()
batches = df.collect()                       # a list of pyarrow.RecordBatch
```

The physical plan of an aggregation has the shape below (abridged; operator names and details change between versions). Read it bottom up: scan, a partial aggregate in each partition, a hash repartition on the group key, then a final aggregate per partition:

```text
AggregateExec: mode=FinalPartitioned, gby=[payment_type], aggr=[avg(fare)]
  RepartitionExec: partitioning=Hash([payment_type], 8)
    AggregateExec: mode=Partial, gby=[payment_type], aggr=[avg(fare)]
      DataSourceExec: file_groups={8 groups}, projection=[payment_type, fare], file_type=parquet
```

That is the same partial and final aggregation pattern as Spark and Trino (see [Apache Spark](#spark)), inside one process.

The command-line shell reads Cloud Storage directly through the `object_store` crate:

```sql
-- datafusion-cli; credentials are configured as described in the CLI docs
CREATE EXTERNAL TABLE trips
STORED AS PARQUET
LOCATION 'gs://my-bucket/trips/';

SELECT payment_type, count(*) FROM trips GROUP BY payment_type;
```

**Extending it.** DataFusion is designed to be taken apart. The most common extension point is **`TableProvider`**, which makes any data source a table. Its core is two methods: `schema()`, and `scan()`, which receives the projected columns, the filters and the limit the planner wants to push down, and returns an `ExecutionPlan`:

```rust
#[async_trait]
impl TableProvider for EventsTable {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // read only the projected columns, apply the filters you can, return a plan node
        todo!()
    }
}
```

(Method signatures shift slightly between releases; check the version you depend on.) Other extension points: scalar, aggregate, window and table functions; custom logical and physical operators; optimizer rules; catalogs; file formats.

**Who builds on it.** InfluxDB 3 (time series), Apache DataFusion Comet (Spark acceleration, next topic), delta-rs (the Rust implementation of Delta Lake, using DataFusion for queries and `MERGE`), LanceDB (vector search over the Lance format), GreptimeDB, and Ballista (a distributed scheduler that runs DataFusion plans across machines). On Google Cloud, a typical use is a small service on Cloud Run or a Batch job that queries Parquet or Iceberg files in Cloud Storage without running a cluster.

> **Pitfall:** DataFusion runs inside your process. A query with a large hash join or sort can use all available memory unless you configure a memory pool limit and a spill directory in the runtime environment. Set limits before exposing it to arbitrary user queries.

> **Tip:** Use `EXPLAIN ANALYZE` to get per-operator metrics (rows produced, elapsed compute time, spill counts). It is the DataFusion equivalent of a Spark stage view, and it shows quickly whether time goes to scanning, repartitioning or aggregating.

::: deeper
**Async pull, not morsels.** DataFusion's operators are Rust `Stream`s polled by Tokio, a pull model over batches with asynchronous I/O. Parallelism is planned through partitions and repartitioning, closer to exchange-based parallelism than to DuckDB's morsel scheduler. The project's documentation discusses running CPU-bound plans on a dedicated runtime so they do not starve network I/O in the same process.

**Statistics and pruning.** File scans use Parquet metadata aggressively: row-group statistics, page indexes and Bloom filters prune data before decoding, and filters can be evaluated during decoding ("late materialisation" of other columns). The optimizer also uses statistics for join selection, though DataFusion's cost-based optimization is lighter than mature databases'.

**DataFusion versus DuckDB.** Both are embeddable, vectorized and fast. DuckDB is a complete database product (own storage format, transactions, a huge extension ecosystem) aimed at end users. DataFusion is a toolkit aimed at engine builders: every layer is a public, replaceable Rust API, and it is Arrow-native end to end. If you want to query files, either works; if you want to build a database, DataFusion is designed for that.

**The SIGMOD 2024 paper** (Lamb et al.) describes the architecture and argues that a shared, modular engine lets new systems reach good performance far faster than writing an engine from scratch.

> **Interview:** "What is a `TableProvider` in DataFusion, and why do `scan()`'s arguments matter?" It is the interface that makes a data source queryable. `scan()` receives the projection, filters and limit the optimizer wants to push down, so a provider can read fewer columns, skip data and stop early, which is where most performance of a custom source comes from.
:::

::: check
?? In DataFusion, what is the difference between a `LogicalPlan` and an `ExecutionPlan`?
A `LogicalPlan` says what to compute (relational operators and expressions) and is rewritten by optimizer rules. An `ExecutionPlan` is the physical tree of operators with partitioning and algorithms, whose `execute()` returns streams of Arrow record batches.

?? How does DataFusion use multiple CPU cores?
Through partitions: plans run with `target_partitions` parallel streams (the number of cores by default), and `RepartitionExec` redistributes batches between them, all scheduled on the Tokio runtime.

?? Name two systems built on DataFusion.
Any two of: InfluxDB 3, Apache DataFusion Comet, delta-rs, LanceDB, GreptimeDB, Ballista.
:::

::: resources
- Docs: [Apache DataFusion user guide](https://datafusion.apache.org/user-guide/introduction.html) · features, SQL reference, configuration and the CLI
- Docs: [DataFusion library user guide](https://datafusion.apache.org/library-user-guide/index.html) · building on DataFusion: table providers, UDFs, optimizer rules and custom operators
- Paper: [Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine](https://dl.acm.org/doi/10.1145/3626246.3653368) · Lamb et al., SIGMOD 2024; architecture and benchmarks
- Repo: [Apache DataFusion on GitHub](https://github.com/apache/datafusion) · source, examples and the list of projects built on it
- Docs: [DataFusion in Python](https://datafusion.apache.org/python/) · the Python bindings, DataFrame API and Arrow interoperability
- Docs: [datafusion crate documentation](https://docs.rs/datafusion/latest/datafusion/) · the Rust API reference with architecture notes
:::

### Apache Comet & Gluten — Native Spark Acceleration

::: plain
Spark's planner and scheduler are excellent, but its execution engine runs on the Java virtual machine and processes rows. Comet and Gluten keep everything you like about Spark (the APIs, the planner, the cluster manager) and swap the engine underneath for a native, vectorized one, like fitting a new engine into a car without changing the dashboard. Your code stays the same; only a few configuration settings change.
:::

**The idea.** Since Spark 3.0, the physical planner can contain **columnar** operators that exchange columnar batches instead of rows, and plugins can rewrite the physical plan before it runs. Native accelerators use both hooks:

1. Spark's Catalyst parses, optimizes and plans the query as usual (see [Apache Spark](#spark)).
2. A plugin walks the physical plan and replaces each operator it supports (scan, filter, project, aggregate, join, sort, shuffle) with a native equivalent.
3. On executors, those operators run in a native library through JNI, on Arrow-style columnar batches in off-heap memory.
4. Operators the plugin does not support **fall back** to normal Spark. Where native and JVM operators meet, a **transition** converts columnar batches to rows (C2R) or rows to columnar (R2C).

[[fig:queryengines_native]]

**The two open-source projects.**

| | Apache DataFusion Comet | Apache Gluten |
|---|---|---|
| Origin | Built at Apple, donated in 2024; now part of the Apache DataFusion project | Started by Intel and Kyligence in 2022; entered the Apache incubator in 2024 |
| Native engine | DataFusion (Rust), plus JVM code that works directly on Arrow batches | Velox (C++) or ClickHouse (C++), chosen at build time |
| Plan hand-off | Comet's own protobuf format | A Substrait-based plan |
| Shuffle | Comet shuffle manager on Arrow data | Columnar shuffle manager |
| Enable with | `spark.plugins=org.apache.spark.CometPlugin` | `spark.plugins=org.apache.gluten.GlutenPlugin` |

Both need off-heap memory, because the native engine allocates outside the Java heap. A Comet session (the jar is built for a specific Spark and Scala version):

```bash
spark-submit \
  --jars "$COMET_JAR" \
  --conf spark.driver.extraClassPath="$COMET_JAR" \
  --conf spark.executor.extraClassPath="$COMET_JAR" \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.comet.explain.fallback.enabled=true \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4g \
  job.py
```

A Gluten session with the Velox backend looks similar:

```bash
spark-submit \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --conf spark.driver.extraClassPath="$GLUTEN_JAR" \
  --conf spark.executor.extraClassPath="$GLUTEN_JAR" \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  job.py
```

**On Google Cloud: Lightning Engine.** Managed Service for Apache Spark (formerly Dataproc) offers **Lightning Engine**, an alternative to the standard Spark engine with optimized Cloud Storage access and optimizer improvements. Its optional **Native Query Execution (NQE)** is a native engine based on Apache Gluten and Velox, tuned for Google hardware, including Axion-based C4A Arm VMs. You enable it on a cluster and, if you like, per job:

```bash
gcloud dataproc clusters create analytics \
  --region=europe-west4 \
  --engine=lightning \
  --image-version=2.3

gcloud dataproc jobs submit spark \
  --cluster=analytics \
  --region=europe-west4 \
  --properties=spark.dataproc.lightningEngine.runtime=native \
  ...
```

Google's documentation recommends NQE for compute-intensive DataFrame, Dataset and Spark SQL work over Parquet, ORC, Iceberg and Delta, and advises against it for RDD-heavy jobs, UDF-heavy jobs, most Spark ML libraries and I/O-bound jobs; it also lists unsupported input types and image requirements, and provides a qualification tool that estimates which of your jobs would benefit. Read the current page before planning, because requirements and pricing change. For serverless batches the same engine is selected in the batch configuration.

**When they help, and when they do not.**

| Helps | Does not help, or hurts |
|---|---|
| CPU-bound SQL and DataFrame queries: scans with filters, aggregations, joins, sorts over Parquet or ORC | I/O-bound jobs waiting on storage or network |
| Wide scans of columnar files with selective filters | Python UDFs, Scala UDFs and RDD code (they fall back to the JVM) |
| Stable, repeated pipelines where you can test coverage once | Plans that alternate native and JVM operators, paying a transition each time |
| Jobs where executor CPU is the main cost | Tiny jobs dominated by startup and scheduling |

The projects publish TPC-H and TPC-DS results; Comet, for example, reports about a two-fold speed-up over Spark on TPC-DS at 1 TB. Treat such numbers as a ceiling: what you get depends on how much of *your* plans runs natively.

> **Pitfall:** Silent fallback. A plan that looks accelerated can contain one unsupported expression in a filter, which forces that operator back to the JVM and adds two transitions around it. Enable the fallback explanation (`spark.comet.explain.fallback.enabled` in Comet; Gluten reports fallbacks in the Spark UI and logs) and read the physical plan: native operators carry the plugin's prefix (`Comet...` or `...Transformer`), and `ColumnarToRow` / `RowToColumnar` nodes mark transitions.

> **Pitfall:** Memory configuration. Native operators use off-heap memory, so the executor's container size must include it. Moving memory from the heap to off-heap, rather than adding it on top, avoids containers being killed for exceeding their limit.

> **Tip:** Validate results, not only speed. Native engines aim for Spark-compatible semantics, but there are documented differences (some casts, floating-point edge cases, regular expressions). Comet and Gluten both document their compatibility; run old and new engines side by side on a sample and compare outputs before switching production jobs.

::: deeper
**Why a native engine is faster than whole-stage codegen.** WSCG generates JVM code that still processes one row at a time and depends on the JVM JIT. Native engines run columnar batches through precompiled vectorized kernels (SIMD where it pays), avoid garbage collection for data, keep encodings such as dictionaries in memory, and read Parquet with native decoders. Databricks Photon applies the same idea inside the Databricks runtime (see [Apache Spark](#spark)).

**Where the time goes after acceleration.** Once operators are fast, the bottlenecks move: shuffle serialization and network, Parquet decoding, Cloud Storage latency and the JVM-native boundary. That is why both projects implement columnar shuffle, and why Lightning Engine also optimizes storage access, not only operators.

**Coverage grows with every release.** Operator, expression and data type support differ by Spark version and project release (Gluten lists supported Spark versions in its README; Comet ships one jar per Spark and Scala version). Pin versions in your cluster image or job configuration and re-test on upgrades.

> **Interview:** "Your team enabled Gluten and saw only a 5% gain. How do you investigate?" Check the physical plan for fallbacks and C2R/R2C transitions; look for UDFs or unsupported expressions that pull operators back to the JVM; check whether the job is I/O-bound (task time dominated by scan wait or shuffle fetch) rather than CPU-bound; confirm off-heap memory settings and spilling. Fix the largest fallback first, or accept that the job is not a good fit.
:::

::: check
?? What happens to an operator that Comet or Gluten does not support?
It falls back to Spark's normal JVM execution, with columnar-to-row and row-to-columnar transitions inserted where native and JVM operators meet.

?? Which native engines do Comet and Gluten use?
Comet uses Apache DataFusion (Rust). Gluten uses Velox or ClickHouse (C++). Google's Lightning Engine Native Query Execution is based on Gluten and Velox.

?? Name two kinds of Spark job that native acceleration is unlikely to speed up.
I/O-bound jobs, and jobs dominated by UDFs or RDD code (which stay on the JVM). Very small jobs dominated by startup also gain little.
:::

::: resources
- Docs: [Apache DataFusion Comet](https://datafusion.apache.org/comet/) · user guide: installation, supported operators and expressions, tuning
- Docs: [Comet installation](https://datafusion.apache.org/comet/user-guide/latest/installation.html) · the exact jars and Spark settings per Spark version
- Repo: [Apache Gluten](https://github.com/apache/incubator-gluten) · architecture, supported backends and Spark versions, configuration
- Docs: [Apache Gluten website](https://gluten.apache.org/) · getting started, Velox backend guide and release notes
- Docs: [Use Lightning Engine](https://cloud.google.com/dataproc/docs/guides/lightning-engine) · Google Cloud's guide: cluster creation, Native Query Execution, requirements
- Paper: [Photon: A Fast Query Engine for Lakehouse Systems](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf) · the design argument for a native vectorized engine under Spark's APIs
:::

### WASM for Portable Data Compute

::: plain
WebAssembly (Wasm) is a compact, safe instruction format that runs at near-native speed inside a sandbox, in any modern browser and in standalone runtimes. Compile a program such as DuckDB to Wasm and it runs inside a web page: the user's laptop does the analytics, and no server is involved. Think of it as a universal power adapter for code: build once, plug in anywhere, and the sandbox makes sure the code cannot touch anything it was not given.
:::

**What WebAssembly is.** A binary instruction format for a simple stack-based virtual machine, standardised by the W3C. Code is compiled to Wasm from C, C++, Rust, Go and other languages; a runtime (a browser engine, or Wasmtime, WasmEdge and others outside the browser) compiles it to native machine code and runs it. Its properties explain where it fits:

- **Sandboxed**: a module sees only its own **linear memory** and the functions the host explicitly gives it. No files, network or system calls unless granted.
- **Portable**: one `.wasm` file runs on x86 and Arm, in browsers and servers.
- **Fast enough**: typically within a modest factor of native code, with 128-bit SIMD available (narrower than AVX2 or AVX-512, see SIMD Vectorization for Analytics).
- **Limited**: 32-bit linear memory caps a module at 4 GiB (64-bit memory is part of WebAssembly 3.0, finished in 2025, but support varies), threads in browsers need special page headers, and every interaction with the outside world crosses a host boundary.

**DuckDB-Wasm: analytics in the browser.** DuckDB compiles to Wasm and is published as the `@duckdb/duckdb-wasm` npm package. It runs in a Web Worker, speaks Apache Arrow for input and results, and reads Parquet, CSV and JSON from local files or over HTTP, using range requests to fetch only the bytes a query needs. Setting it up in JavaScript:

```javascript
import * as duckdb from '@duckdb/duckdb-wasm';

const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
const workerUrl = URL.createObjectURL(
  new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' }));
const worker = new Worker(workerUrl);
const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
URL.revokeObjectURL(workerUrl);

const conn = await db.connect();
const result = await conn.query(`
  SELECT country, count(*) AS orders, sum(amount) AS revenue
  FROM 'https://storage.googleapis.com/my-public-bucket/orders.parquet'
  GROUP BY country ORDER BY revenue DESC`);
console.table(result.toArray());
```

To serve Parquet from Cloud Storage to such a page, the bucket needs a CORS policy that allows your site's origin, `GET` and `HEAD`, and range requests. For private data, give the browser short-lived signed URLs from your backend rather than making objects public:

```json
[
  {
    "origin": ["https://dashboards.example.com"],
    "method": ["GET", "HEAD"],
    "responseHeader": ["Content-Type", "Content-Range", "Accept-Ranges", "Range"],
    "maxAgeSeconds": 3600
  }
]
```

```bash
gcloud storage buckets update gs://my-analytics-bucket --cors-file=cors.json
```

**Where this is used.** Interactive dashboards and data apps that filter and aggregate in the browser instead of calling a server on every click; documentation and teaching sites with live SQL; data exploration tools that never upload the user's file. MotherDuck, the DuckDB cloud service, runs DuckDB-Wasm in its web UI and combines it with server-side DuckDB ("dual execution"), so part of a query can run in the browser and part in the cloud. The DuckDB documentation lists the current limits: by default one thread (multithreading is experimental), a 4 GiB memory ceiling or lower, and spilling to a temporary directory for larger intermediates.

**WASI: Wasm outside the browser.** The **WebAssembly System Interface** defines standard APIs (files, clocks, random numbers, sockets, HTTP) so modules can run in server-side runtimes. WASI 0.2 introduced the **Component Model**, in which modules declare typed interfaces in a language called WIT and can be composed across languages; WASI 0.3 adds native async support. A Rust function compiled once with `cargo build --target wasm32-wasip2` runs in Wasmtime on any operating system and CPU.

**Portable, sandboxed UDFs.** Running user code inside a data system is risky (a crash or a malicious function takes the engine down) and awkward (each engine wants its own language). Wasm offers a middle path: users write a function in any language that compiles to Wasm, and the engine runs it in a sandbox with strict memory and time limits. Examples include Wasm UDFs in SingleStore, Rust UDFs compiled to Wasm in RisingWave, and Redpanda's data transforms, which run Wasm modules inside the broker to filter and reshape records as they arrive.

> **Pitfall:** Browser memory is the hard limit. A dashboard that loads a 3 GB Parquet file into DuckDB-Wasm will fail on many laptops and most phones. Pre-aggregate on the server (BigQuery, a scheduled job), ship small Parquet extracts, and let the browser do the last mile of filtering and grouping.

> **Tip:** Treat the browser as a cache, not a warehouse. Good fits are datasets of megabytes to a few hundred megabytes, many small interactive queries, and a need for offline use or privacy (data never leaves the device).

::: deeper
**Performance details.** Wasm SIMD is 128 bits wide, so code tuned for AVX2 or AVX-512 loses some throughput; "relaxed SIMD" (part of WebAssembly 3.0) lets runtimes use faster platform-specific behaviour. Every bounds-checked memory access and every call across the host boundary costs a little. Threads require `SharedArrayBuffer`, which browsers enable only on cross-origin-isolated pages (served with `Cross-Origin-Opener-Policy` and `Cross-Origin-Embedder-Policy` headers). Network access goes through the browser's `fetch`, subject to CORS.

**Extensions in DuckDB-Wasm.** Extensions are compiled separately to Wasm and loaded on demand from an extension repository, so `INSTALL` and `LOAD` work for many (not all) extensions. Native extensions cannot be loaded, because a browser cannot run native code.

**Wasm versus containers for UDFs.** Containers isolate whole processes, start in hundreds of milliseconds and need an operating system image; Wasm modules start in microseconds to milliseconds, share the host process and have a much smaller attack surface, but can only use what the host exposes. For per-row functions inside an engine, Wasm's low call overhead and fine-grained sandbox are the attraction; for a whole service, a container on Cloud Run remains simpler.

> **Interview:** "Would you build a BI dashboard on DuckDB-Wasm?" For moderate data sizes and highly interactive exploration, yes: queries run locally with no server cost per click, and Parquet range requests keep downloads small. For large or sensitive data, keep heavy aggregation and access control in the warehouse, and send the browser only pre-aggregated, authorised extracts.
:::

::: check
?? What does the WebAssembly sandbox prevent a module from doing by default?
Accessing anything outside its own linear memory: files, network, system calls and other memory, unless the host explicitly provides functions for them.

?? Why does DuckDB-Wasm not need to download a whole Parquet file to answer a query?
It uses HTTP range requests: it reads the Parquet footer first, then fetches only the column chunks and row groups the query needs.

?? What is WASI, and what did WASI 0.2 add?
The WebAssembly System Interface: standard APIs that let Wasm modules run outside the browser in server runtimes. WASI 0.2 added the Component Model, with typed interfaces (WIT) that let modules written in different languages be composed.
:::

::: resources
- Docs: [DuckDB-Wasm overview](https://duckdb.org/docs/stable/clients/wasm/overview) · how it works, instantiation, data import and current limitations
- Blog: [DuckDB-Wasm: efficient analytical SQL in the browser](https://duckdb.org/2021/10/29/duckdb-wasm) · the design: Web Workers, Arrow, range requests and benchmarks
- Docs: [WebAssembly](https://webassembly.org/) · the specification, feature status per browser and runtime, and news on Wasm 3.0
- Spec: [WASI](https://wasi.dev/) · the WebAssembly System Interface: releases 0.1 to 0.3, interfaces and runtimes
- Docs: [The WebAssembly Component Model](https://component-model.bytecodealliance.org/) · WIT interfaces and composing components across languages
- Docs: [Cloud Storage CORS](https://cloud.google.com/storage/docs/using-cors) · configuring a bucket so browser apps can read objects
- Blog: [DuckDB Wasm in your browser](https://motherduck.com/blog/duckdb-wasm-in-browser/) · MotherDuck's walkthrough of browser analytics and its uses
:::

## Wrap-up

### Cheat sheet

| Concept or task | Command, rule of thumb |
|---|---|
| Query pipeline | Parse, validate (catalog), logical plan, optimize (rules, then cost), physical plan, execute |
| Rule-based versus cost-based | Rules for always-good rewrites (push filters, prune columns); cost for join order and algorithms |
| Calcite planners | HepPlanner: fixed rule program; VolcanoPlanner: memo plus cost |
| Find a bad estimate | PostgreSQL `EXPLAIN ANALYZE`: first node bottom up where estimated and actual `rows=` differ 10x |
| Correlated columns in PostgreSQL | `CREATE STATISTICS s (dependencies, mcv) ON a, b FROM t; ANALYZE t;` |
| Join estimate | `\|R\| * \|S\| / max(NDV(R.a), NDV(S.b))` |
| Join ordering | Dynamic programming over subsets up to about 10 to 15 tables, heuristics beyond |
| Inspect a Substrait plan (DuckDB) | `INSTALL substrait FROM community; LOAD substrait; CALL get_substrait_json('SELECT ...');` |
| Volcano model | `open` / `next` / `close`, one row per call: simple, slow per row |
| Vectorized execution | Operators on vectors of about 1,000 to 8,000 values; DuckDB 2,048, DataFusion 8,192 |
| Push pipelines | Cut the plan at breakers (join build, aggregate, sort); run pipelines over morsels |
| Check CPU SIMD support | `lscpu \| grep -o -E 'avx2\|avx512[a-z_]*\|sve2?\|asimd' \| sort -u` |
| See what the compiler vectorized | `clang -O2 -Rpass=loop-vectorize`; GCC `-fopt-info-vec` |
| Selection vector | Positions that passed a filter; compute branch-free or with SIMD masks |
| PostgreSQL JIT | `jit_above_cost` (100,000), `jit_inline_above_cost`, `jit_optimize_above_cost` (500,000) |
| ClickHouse JIT | `compile_expressions` (on), compiles after `min_count_to_compile_expression` (3) repeats |
| Comet | `spark.plugins=org.apache.spark.CometPlugin`, off-heap memory, Comet shuffle manager |
| Gluten | `spark.plugins=org.apache.gluten.GlutenPlugin`, off-heap memory, columnar shuffle manager |
| Lightning Engine NQE | `gcloud dataproc clusters create ... --engine=lightning`; job property `spark.dataproc.lightningEngine.runtime=native` |
| Browser analytics | DuckDB-Wasm, Parquet over HTTP range requests, bucket CORS for `GET` and `HEAD` |

### Practice

::: try
**1. Estimate, then check.** A `sales` table has 10,000,000 rows. `NDV(store_id) = 500`, `NDV(product_id) = 20,000`, and a `stores` table has 500 rows with 50 in region `'west'`. Estimate the rows returned by `SELECT * FROM sales JOIN stores USING (store_id) WHERE stores.region = 'west'`, assuming uniform distribution. Then name one real-world property of the data that would make the estimate wrong.
:::

?? Answer to 1
The filter leaves 50 stores. The join estimate is `10,000,000 * 50 / max(500, 50) = 1,000,000` rows (each store has about 20,000 sales). It is wrong if sales are skewed across stores: if the western stores are the busiest (or the quietest), the true count can be several times higher (or lower), because uniformity does not hold.

::: try
**2. Count the calls.** Extend the Volcano example from the execution models topic with a `Limit(n)` operator that stops after `n` rows, and put it above the `Filter` instead of `Sum`. How many `next()` calls in total does `Limit(10)` cause over 10,000 rows, how many rows does the scan read, and why is early termination natural in a pull model?
:::

?? A solution to 2

```python
calls = {"next": 0}

class Scan:
    def __init__(self, rows): self.rows = rows
    def open(self): self.i = 0
    def next(self):
        calls["next"] += 1
        if self.i == len(self.rows): return None
        self.i += 1
        return self.rows[self.i - 1]

class Filter:
    def __init__(self, child, pred): self.child, self.pred = child, pred
    def open(self): self.child.open()
    def next(self):
        calls["next"] += 1
        while (row := self.child.next()) is not None:
            if self.pred(row): return row
        return None

class Limit:
    def __init__(self, child, n): self.child, self.n = child, n
    def open(self): self.child.open(); self.seen = 0
    def next(self):
        calls["next"] += 1
        if self.seen == self.n: return None       # stop pulling: nothing below runs again
        self.seen += 1
        return self.child.next()

plan = Limit(Filter(Scan(list(range(10_000))), lambda x: x % 2 == 0), 10)
plan.open()
out = []
while (row := plan.next()) is not None:
    out.append(row)
print(out)              # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
print(calls["next"])    # 40
```

40 calls in all: 11 on the limit, 10 on the filter and 19 on the scan. The limit simply stops calling `next()` on its child, so the scan reads 19 rows (0 to 18) instead of 10,000. A push engine needs an explicit signal back to the source to stop early.

::: try
**3. Branch-free filter.** Write a Python function `select_lt(values, c)` that returns the positions of all values less than `c` without an `if` statement inside the loop, using the "always write, advance by 0 or 1" technique. Test it on `[9, 3, 7, 1, 8, 2]` with `c = 5`.
:::

?? A solution to 3

```python
def select_lt(values, c):
    out, n = [0] * len(values), 0
    for i, x in enumerate(values):
        out[n] = i              # always write
        n += x < c              # advance only if the row qualifies
    return out[:n]

print(select_lt([9, 3, 7, 1, 8, 2], 5))   # [1, 3, 5]
```

::: try
**4. Should we turn it on?** A nightly Spark job on Managed Service for Apache Spark spends 70% of task time in scans, filters and aggregations over Parquet, uses two Python UDFs in its final step, and writes to BigQuery. Outline how you would evaluate Lightning Engine's Native Query Execution (or Gluten) for it, and what result would make you keep the standard engine.
:::

?? A solution to 4
Run the qualification tool or a test run with NQE enabled on a copy of the job. Compare total runtime and cost, check the physical plan for native operators and for fallbacks (the Python UDF step will stay on the JVM and Python, with transitions around it), and compare outputs with the standard engine on the same input. Consider moving the UDF logic to built-in functions so more of the plan can run natively. Keep the standard engine if the gain is small after fallbacks, if results differ, or if the job turns out to be I/O-bound (time dominated by reading from Cloud Storage or writing to BigQuery).

::: try
**5. Read the plan.** Take any query you run often in PostgreSQL, DuckDB, DataFusion or Spark, and print its plan with `EXPLAIN` (or `EXPLAIN ANALYZE` where available). Mark the pipeline breakers, the exchanges or repartitions, and, where shown, the first operator whose estimated and actual row counts diverge by more than ten times.
:::

?? Answer to 5
There is no single answer. Breakers are hash join builds, aggregations (in DataFusion both `AggregateExec` stages, in Spark both `HashAggregate` nodes), sorts and window operators; exchanges are `Exchange` (Spark), `RepartitionExec` (DataFusion) or `Gather`/`Gather Merge` in PostgreSQL parallel plans. If an estimate diverges, the operators above it were planned on wrong numbers: fix the statistics or the predicate at that point first.

### What comes next

You now know how engines turn SQL into plans and how modern executors run those plans: vectorized, SIMD-friendly, sometimes compiled, and increasingly assembled from shared libraries such as Velox and DataFusion. The next section, **Real-time OLAP engines**, applies these ideas to systems built for sub-second analytics on fresh data (Apache Druid, Apache Pinot, StarRocks and Apache Doris): how they ingest streams, index and pre-aggregate data, and keep query latency low for thousands of concurrent users.
