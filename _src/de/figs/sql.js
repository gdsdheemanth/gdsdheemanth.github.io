/* sql: SQL for data engineering, original drawings */
Object.assign(FIGS, {

sql_order:{ t:"The logical order of a SELECT query", cap:"You write SELECT first, but the database evaluates the clauses in this order. Each step only sees what the steps before it produced, which is why a SELECT alias cannot be used in WHERE, why HAVING can filter on aggregates and why window functions need QUALIFY or a subquery to be filtered.", svg:S("sql1", 270, "Nine query clauses in evaluation order, from FROM and JOIN to LIMIT",
  B(20, 30, 128, 56, "ka", ["1 FROM / JOIN", "read and combine"]) +
  B(168, 30, 128, 56, "ka", ["2 WHERE", "filter rows"]) +
  B(316, 30, 128, 56, "sp", ["3 GROUP BY", "form groups"]) +
  B(464, 30, 128, 56, "sp", ["4 HAVING", "filter groups"]) +
  B(612, 30, 128, 56, "st", ["5 Window", "OVER (...), QUALIFY"]) +
  A("sql1", "M148 58 L166 58") + A("sql1", "M296 58 L314 58") + A("sql1", "M444 58 L462 58") + A("sql1", "M592 58 L610 58") +
  A("sql1", "M676 86 L676 106 L94 106 L94 128") +
  B(20, 130, 150, 56, "pr", ["6 SELECT", "compute, name aliases"]) +
  B(200, 130, 150, 56, "pr", ["7 DISTINCT", "remove duplicates"]) +
  B(380, 130, 150, 56, "box", ["8 ORDER BY", "aliases usable here"]) +
  B(560, 130, 150, 56, "box", ["9 LIMIT", "cut the output"]) +
  A("sql1", "M170 158 L198 158") + A("sql1", "M350 158 L378 158") + A("sql1", "M530 158 L558 158") +
  T(20, 222, "Written order: SELECT, FROM, JOIN, WHERE, GROUP BY, HAVING, QUALIFY, ORDER BY, LIMIT", "mono") +
  T(20, 248, "An alias made in step 6 does not exist yet in steps 1 to 5.", "sm")) },

sql_joins:{ t:"INNER, LEFT and FULL OUTER joins on the sample data", cap:"Customers 1 to 3 have orders, Dana (4) has none, and order 107 points at customer 9, who is missing. An inner join keeps only matches; a left join from customers also keeps Dana with empty order columns; a full outer join keeps both unmatched sides.", svg:S("sql2", 300, "Which rows survive inner, left and full outer joins between customers and orders",
  B(20, 40, 150, 210, "box", ["customers", "1 Asha", "2 Bo", "3 Chen", "4 Dana", " ", "Dana has no orders"]) +
  B(590, 40, 150, 210, "box", ["orders.customer_id", "1 (101, 103, 106)", "2 (102, 105)", "3 (104)", "9 (107)", " ", "9 is not a customer"]) +
  B(205, 30, 350, 60, "sp", ["INNER JOIN", "Asha, Bo, Chen with their orders"]) +
  B(205, 115, 350, 60, "st", ["customers LEFT JOIN orders", "the above + Dana with NULL order columns"]) +
  B(205, 200, 350, 60, "ka", ["FULL OUTER JOIN", "the above + order 107 with NULL customer columns"]) +
  A("sql2", "M170 90 L203 70") + A("sql2", "M588 90 L557 70") +
  A("sql2", "M170 160 L203 145") + A("sql2", "M170 220 L203 230") + A("sql2", "M588 220 L557 230") +
  T(380, 288, "Unmatched rows are kept only on the side(s) the join type preserves.", "sm", "middle")) },

sql_frame:{ t:"A window frame: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", cap:"Paid orders sorted by date and order_id. For the current row (order 105) the frame holds it and the two rows before it, so AVG(amount) over the frame is (22 + 60 + 35) / 3 = 39.0. As the current row moves down, the frame slides with it.", svg:S("sql3", 300, "Six ordered rows with a three-row frame highlighted around the current row",
  T(40, 24, "ORDER BY order_date, order_id", "mono") +
  B(40, 36, 300, 34, "box", ["101 · 2026-03-01 · 40.0"], false) +
  B(40, 76, 300, 34, "ka", ["103 · 2026-03-02 · 22.0"], false) +
  B(40, 116, 300, 34, "ka", ["104 · 2026-03-03 · 60.0"], false) +
  B(40, 156, 300, 34, "sp", ["105 · 2026-03-05 · 35.0  (current)"], false) +
  B(40, 196, 300, 34, "box", ["106 · 2026-03-08 · 18.0"], false) +
  B(40, 236, 300, 34, "box", ["107 · 2026-03-08 · 12.0"], false) +
  '<rect class="bad" x="30" y="70" width="320" height="126" rx="10"/>' +
  T(370, 92, "2 PRECEDING", "tK") + T(370, 178, "CURRENT ROW", "tA") +
  A("sql3", "M470 130 L560 130") +
  B(570, 100, 170, 60, "okf", ["AVG = 39.0", "(22 + 60 + 35) / 3"]) +
  T(370, 222, "Rows outside the frame are ignored", "sm") +
  T(370, 240, "for this row, but still in the output.", "sm")) },

sql_unnest:{ t:"UNNEST turns an array into rows", cap:"Each order's items array becomes one row per item, joined back to its own order. Order 105 has an empty array: CROSS JOIN UNNEST produces nothing for it, while LEFT JOIN UNNEST keeps it with NULL item columns.", svg:S("sql4", 280, "Nested order rows with item arrays flattened into one row per item",
  T(20, 24, "shop.orders_nested", "b") +
  B(20, 36, 250, 40, "box", ["101 · [tea x2, cake x1]"], false) +
  B(20, 84, 250, 40, "box", ["103 · [tea x1, jam x2]"], false) +
  B(20, 132, 250, 40, "box", ["104 · [cake x3]"], false) +
  B(20, 180, 250, 40, "pr", ["105 · [ ]  empty"], false) +
  A("sql4", "M275 128 L340 128", "acc") + T(307, 116, "UNNEST", "tA", "middle") +
  T(350, 24, "one row per item", "b") +
  B(350, 36, 180, 30, "sp", ["101 · tea · 2"], false) +
  B(350, 70, 180, 30, "sp", ["101 · cake · 1"], false) +
  B(350, 104, 180, 30, "sp", ["103 · tea · 1"], false) +
  B(350, 138, 180, 30, "sp", ["103 · jam · 2"], false) +
  B(350, 172, 180, 30, "sp", ["104 · cake · 3"], false) +
  B(350, 210, 180, 30, "pr", ["105 · NULL · NULL"], false) +
  T(545, 120, "CROSS JOIN UNNEST:", "b") + T(545, 140, "the 5 blue rows", "sm") +
  T(545, 222, "LEFT JOIN UNNEST adds", "b") + T(545, 240, "the order with no items", "sm")) },

sql_plan:{ t:"An execution plan is a tree", cap:"The plan for paid revenue per city. Data flows upward: the two scans run first, the smaller table (customers) is built into a hash table, the orders scan probes it, and the aggregate groups the joined rows. Each node reports estimated and actual rows; the first node where they disagree badly is usually where a slow plan went wrong.", svg:S("sql5", 300, "Plan tree with aggregate above a hash join above two table scans",
  B(270, 20, 220, 50, "okf", ["HashAggregate", "group by city · 5 rows"]) +
  B(270, 110, 220, 50, "sp", ["Hash Join", "o.customer_id = c.customer_id"]) +
  B(60, 210, 260, 60, "ka", ["Seq Scan on orders", "filter status = 'paid' (probe side)"]) +
  B(520, 120, 180, 44, "st", ["Hash", "build table in memory"]) +
  B(520, 210, 200, 60, "ka", ["Seq Scan on customers", "1,000 rows (build side)"]) +
  A("sql5", "M380 108 L380 72") +
  A("sql5", "M190 208 L330 162") +
  A("sql5", "M610 208 L610 166") + A("sql5", "M518 140 L492 136") +
  T(20, 40, "rows flow upward", "sm") + A("sql5", "M60 160 L60 60", "ln dash") +
  T(700, 40, "read from the", "sm", "end") + T(700, 56, "deepest node up", "sm", "end")) },
});
