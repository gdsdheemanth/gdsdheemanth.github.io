Model answers to the questions asked most often in Java full stack interviews. Read the question, answer it out loud first, then open the answer. For behavioural questions, use the STAR format (situation, task, action, result).

## Core Java

?? JDK vs JRE vs JVM?
JDK = JRE + dev tools (javac, jdb). JRE = JVM + core libraries for running code. JVM = executes bytecode; platform-specific, which is what makes Java 'write once, run anywhere'.

?? Is Java pass-by-value or pass-by-reference?
Always pass-by-value. For objects, the value passed is a copy of the reference, so you can mutate the object but not reassign the caller's variable.

?? Why are Strings immutable? What is the String pool?
Security (class names, URLs, DB connection strings), thread safety, hashCode caching, and the String pool. Literals are interned in the pool; new String() creates a separate heap object.

?? == vs equals()?
== compares references (or primitive values); equals() compares logical content as defined by the class. String, Integer etc. override equals.

?? equals() and hashCode() contract? What breaks if you violate it?
Equal objects must have equal hashCodes. If you override equals without hashCode, HashMap/HashSet put 'equal' objects in different buckets, so lookups fail and you get duplicates.

?? How does HashMap work internally?
Array of buckets. hash(key) → index = hash & (n-1). Collisions go into a linked list, which is converted to a red-black tree when a bucket has more than 8 entries (and capacity is at least 64). Default capacity 16, load factor 0.75; it resizes (doubles) and rehashes when exceeded. One null key is allowed. get/put are O(1) on average.

?? HashMap vs Hashtable vs ConcurrentHashMap?
HashMap: not thread-safe, allows a null key/values. Hashtable: legacy, synchronizes every method (slow). ConcurrentHashMap: thread-safe with fine-grained locking/CAS per bucket, no nulls, much better concurrency.

?? ArrayList vs LinkedList?
ArrayList: dynamic array, O(1) random access, amortized O(1) append, O(n) middle insert, cache-friendly. LinkedList: doubly linked, O(n) access, O(1) insert/remove at a known node. ArrayList is almost always the better choice.

?? Comparable vs Comparator?
Comparable: natural order inside the class (compareTo), one per class. Comparator: external, many orderings possible, e.g. Comparator.comparing(Emp::getSalary).reversed().

?? Fail-fast vs fail-safe iterators?
Fail-fast (ArrayList, HashMap) throw ConcurrentModificationException if the collection is modified during iteration (modCount check). Fail-safe/weakly consistent (CopyOnWriteArrayList, ConcurrentHashMap) iterate over a snapshot or tolerate changes.

?? Checked vs unchecked exceptions?
Checked (IOException, SQLException) must be handled or declared; they represent recoverable conditions. Unchecked (RuntimeException subclasses) usually signal programming errors. Spring's @Transactional rolls back on unchecked exceptions by default.

?? final vs finally vs finalize?
final: constant / no override / no subclass. finally: block that always runs after try/catch (except on System.exit or a JVM crash). finalize: deprecated GC hook, never use it.

?? Abstract class vs interface (Java 8+)?
Interface: contract, multiple inheritance of type, default/static/private methods, no instance state. Abstract class: can hold state & constructors, single inheritance. Use an interface for capability, an abstract class for shared base implementation.

?? Overloading vs overriding? Can static methods be overridden?
Overloading: same name, different parameters, resolved at compile time. Overriding: subclass redefines an instance method, resolved at runtime. Static methods are hidden, not overridden.

?? How do you create an immutable class?
final class, private final fields, no setters, initialize in the constructor, defensive copies of mutable fields (in and out). Or simply use a record (with care for mutable components).

?? String vs StringBuilder vs StringBuffer?
String is immutable. StringBuilder is mutable and not synchronized (fast). StringBuffer is mutable and synchronized (legacy). Use StringBuilder for building strings in loops.

?? Intermediate vs terminal stream operations? What is lazy evaluation?
Intermediate (map, filter, sorted) return a stream and are lazy. Terminal (collect, forEach, reduce, count) trigger processing. Nothing runs until a terminal operation is called, and elements flow one by one through the pipeline.

?? map vs flatMap?
map: one-to-one transform. flatMap: one-to-many, flattens Stream<List<T>> into Stream<T>.

?? Stream coding: highest-paid employee in each department
emps.stream().collect(Collectors.groupingBy(Emp::getDept, Collectors.maxBy(Comparator.comparing(Emp::getSalary)))). Also practise: frequency of characters, second highest number, duplicates in a list, joining names.

?? What are records and sealed classes?
Records: concise immutable data carriers (auto constructor, accessors, equals/hashCode/toString). Sealed classes: restrict which classes can extend them (permits); great with pattern-matching switch.

?? How do you implement a thread-safe Singleton?
Enum singleton (best), the static holder idiom (lazy & thread-safe), or double-checked locking with a volatile instance. In Spring, beans are singletons per container anyway.

?? Why use BigDecimal for money?
double/float are binary floating point, so 0.1 + 0.2 ≠ 0.3. BigDecimal gives exact decimal arithmetic with explicit scale and rounding. Create it with new BigDecimal("0.1") or BigDecimal.valueOf, not new BigDecimal(0.1).

?? What does the static keyword mean?
It belongs to the class, not to instances: static fields are shared, static methods can't use 'this', static blocks run once when the class loads, static nested classes need no outer instance.

## Concurrency & JVM

?? Thread vs Runnable vs Callable?
Extending Thread couples the task to the thread. Runnable is a task with no result. Callable returns a result and can throw checked exceptions; it is used with ExecutorService and returns a Future.

?? synchronized vs ReentrantLock?
synchronized: simple, auto-release, block-structured. ReentrantLock: tryLock with timeout, interruptible, fairness option, multiple Conditions; must unlock in finally.

?? What does volatile do?
Guarantees visibility (reads/writes go to main memory) and ordering (happens-before) but not atomicity, so count++ is still unsafe. Use AtomicInteger for that.

?? What is a deadlock and how do you prevent it?
Two or more threads wait forever for each other's locks. Prevent it with a consistent lock ordering, tryLock with timeouts, fewer nested locks and smaller lock scope. Detect it with a thread dump.

?? Explain ExecutorService & thread pool parameters
Reuses threads instead of creating one per task. ThreadPoolExecutor: corePoolSize, maxPoolSize, keepAlive, work queue, rejection policy. Always shut down executors.

?? CompletableFuture: how do you combine async calls?
supplyAsync → thenApply (transform), thenCompose (chain dependent async calls), thenCombine (two independent results), allOf (wait for all), exceptionally/handle (errors). Pass a custom executor for I/O work.

?? How does ConcurrentHashMap achieve thread safety?
Java 8+: CAS for empty bins, synchronized on the first node of a bin for updates, volatile reads, so no global lock. Atomic helpers: compute, merge, putIfAbsent.

?? What are virtual threads (Java 21)?
Lightweight threads managed by the JVM and mounted on a few carrier threads. Millions are possible; great for blocking I/O-heavy work (thread-per-request). Not faster for CPU-bound work. Spring Boot enables them with spring.threads.virtual.enabled=true.

?? Implement producer-consumer
Use a BlockingQueue: the producer calls put(), the consumer calls take(), and blocking handles the coordination. You can also show the wait/notify version with a while-loop condition check.

?? What are the JVM memory areas?
Heap (objects; young: eden + survivors, old gen), thread stacks (frames, local variables), metaspace (class metadata), code cache (JIT-compiled code), PC registers.

?? How does garbage collection work? What is G1?
Objects unreachable from GC roots are collected. Generational: most objects die young, so minor GCs clean the young gen. G1 splits the heap into regions, collects the regions with the most garbage first and targets pause times. ZGC gives very low pause times.

?? How would you diagnose an OutOfMemoryError or high CPU?
OOM: enable -XX:+HeapDumpOnOutOfMemoryError, analyse the dump in Eclipse MAT/VisualVM, look for large retained collections and leaks (static maps, unclosed resources). High CPU: take thread dumps (jstack) and find hot threads; use JFR for profiling.

## Spring & Spring Boot

?? What are IoC and Dependency Injection?
IoC: the container creates and wires objects instead of your code. DI: dependencies are supplied (constructor/setter/field) instead of created with new. Gives loose coupling, testability and configurability.

?? Why is constructor injection preferred?
Dependencies can be final (immutable), they're obvious and required, it's easy to unit test without Spring, it fails fast on missing beans and it exposes circular dependencies.

?? Bean scopes?
singleton (default, one per container), prototype (new each time), request, session, application (web). Watch out for injecting a prototype into a singleton.

?? @Component vs @Bean?
@Component: class-level, picked up by component scanning. @Bean: method-level inside @Configuration, for third-party classes or custom construction logic.

?? Two beans of the same type: how does Spring choose?
Use @Primary for a default, @Qualifier("name") at the injection point, or inject a List/Map of all of them. Otherwise you get NoUniqueBeanDefinitionException.

?? How does Spring Boot auto-configuration work?
@SpringBootApplication = @Configuration + @EnableAutoConfiguration + @ComponentScan. Auto-config classes listed in META-INF/spring/…AutoConfiguration.imports are applied conditionally (@ConditionalOnClass, @ConditionalOnMissingBean, @ConditionalOnProperty). Your own beans override them.

?? What are starters?
Curated dependency bundles (spring-boot-starter-web, -data-jpa, -security) with compatible versions plus the matching auto-configuration.

?? @Controller vs @RestController?
@RestController = @Controller + @ResponseBody, so return values are serialized to JSON instead of resolved as view names.

?? How do you handle exceptions globally?
@RestControllerAdvice with @ExceptionHandler methods that return ProblemDetail/ResponseEntity with proper status codes. Keep error responses consistent; never leak stack traces.

?? How does @Transactional work? What is the self-invocation problem?
Spring wraps the bean in a proxy that begins/commits/rolls back the transaction. Calling a @Transactional method from another method in the same class bypasses the proxy, so there is no transaction. By default it rolls back only on unchecked exceptions (use rollbackFor for checked ones).

?? Transaction propagation types?
REQUIRED (default: join or create), REQUIRES_NEW (suspend the current one, start a new one, e.g. audit logs), NESTED (savepoint), SUPPORTS, MANDATORY, NOT_SUPPORTED, NEVER.

?? What is Spring AOP? Give real uses
Cross-cutting concerns applied through proxies: logging, @Transactional, security, caching, metrics. Concepts: aspect, advice (before/after/around), pointcut, join point.

?? Explain the Spring Security JWT flow
Login → authenticate credentials → issue a signed JWT (claims, expiry). Client sends Authorization: Bearer <token>. A filter (or OAuth2 resource server) validates the signature/expiry, builds the Authentication and puts it in the SecurityContext. Stateless, no session.

?? What does Spring Boot Actuator give you?
Production endpoints: /health, /metrics, /info, /env, /loggers, /prometheus. Secure them and expose only what's needed.

?? How do you call another microservice from Spring Boot?
RestClient (sync, modern), WebClient (reactive/async), OpenFeign (declarative interface), with timeouts, retries and a circuit breaker.

?? How do profiles and externalized config work?
application-{profile}.yml plus spring.profiles.active. Property precedence: command line > environment variables > profile files > application.yml. Bind config with @ConfigurationProperties.

?? What changed from Spring Boot 2 to 3/4?
javax.* → jakarta.* namespace, Java 17 minimum, Micrometer observability, native image support, ProblemDetail, RestClient. Boot 4 (Spring Framework 7) continues modernisation; most companies are on 3.x.

?? Circular dependency: what is it and how do you fix it?
Bean A needs B and B needs A. Fix it by redesigning (extract a third component), using events or setter injection with @Lazy as a last resort. Boot disallows circular references by default.

## JPA / Hibernate

?? JPA vs Hibernate vs Spring Data JPA?
JPA: specification. Hibernate: an implementation of JPA (ORM). Spring Data JPA: repository abstraction on top of JPA that generates queries.

?? What is the N+1 problem and how do you fix it?
Loading N parents and then lazily loading children runs 1 + N queries. Fix it with JOIN FETCH, @EntityGraph, batch fetching (@BatchSize / default_batch_fetch_size) or DTO projections.

?? Lazy vs eager loading? LazyInitializationException?
LAZY loads on access (default for collections), EAGER loads immediately. Touching a lazy field after the session/transaction has closed throws the exception. Fix it with fetch joins or DTOs inside the transaction, not with open-in-view hacks.

?? Entity lifecycle states?
Transient (new, not managed), Managed/persistent (in the persistence context, changes are auto-flushed), Detached (the context closed), Removed.

?? First-level vs second-level cache?
First-level: per persistence context/transaction, always on. Second-level: shared across sessions, optional (Ehcache/Caffeine/Redis), good for read-mostly data.

?? How does optimistic locking work?
A @Version column is checked on update. If someone else updated the row first, an OptimisticLockException is thrown and you retry or inform the user. No DB locks are held.

?? Cascade types & orphanRemoval?
Cascade propagates operations (PERSIST, MERGE, REMOVE, ALL) from parent to child. orphanRemoval deletes a child removed from the parent's collection.

?? save() vs saveAndFlush()? What is dirty checking?
save puts the entity in the persistence context and the flush happens at commit. saveAndFlush flushes immediately. Dirty checking: managed entities' changes are detected and written automatically at flush, with no explicit update call.

## REST APIs

?? What makes an API RESTful?
Resources identified by URIs, standard HTTP methods, stateless requests, representations (JSON), proper status codes, cacheability, a uniform interface (plus HATEOAS in theory).

?? PUT vs PATCH vs POST?
POST: create / non-idempotent action. PUT: full replace, idempotent. PATCH: partial update. DELETE: idempotent.

?? What does idempotent mean? How do you make POST idempotent?
Repeating the request has the same effect as doing it once. Use an Idempotency-Key header stored with the result; duplicates return the stored response. Critical for checkouts and payments.

?? Key status codes?
200 OK, 201 Created, 204 No Content, 400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found, 409 Conflict, 422 Unprocessable, 429 Too Many Requests, 500 Server Error, 503 Unavailable.

?? How do you version APIs?
URI (/api/v1/...), header, or media type. URI versioning is the simplest and most common; keep backward compatibility.

?? How do you design pagination?
page/size with sorting (Spring Pageable) or cursor/keyset pagination for large or changing datasets. Return metadata (total, next cursor).

?? How do you secure a REST API?
HTTPS, authentication (JWT/OAuth2), authorization checks, input validation, rate limiting, CORS config, no sensitive data in logs/URLs, OWASP Top 10.

## Microservices

?? Monolith vs microservices: when would you NOT use microservices?
Microservices give independent deploys/scaling and team autonomy, at the cost of distributed complexity, network failures, data consistency and ops overhead. Small teams or unclear domains: start with a modular monolith.

?? How do microservices communicate?
Synchronously (REST/gRPC) when an immediate answer is needed; asynchronously (Kafka/RabbitMQ events) for decoupling, resilience and spikes. Prefer async between domains.

?? What does an API gateway do?
A single entry point: routing, authentication, rate limiting, SSL termination, aggregation, CORS. E.g. Spring Cloud Gateway.

?? Explain service discovery
Services register their location, and clients look them up instead of using hard-coded hosts. Eureka (Spring Cloud) or Kubernetes Services/DNS.

?? What is a circuit breaker?
Wraps remote calls. After the failure threshold is reached it opens (fails fast, with a fallback); after a wait it goes half-open and tests with a few calls; it closes on success. Prevents cascading failures (Resilience4j).

?? Explain the Saga pattern
Manages a business transaction across services without 2PC: a sequence of local transactions, each publishing an event; on failure, compensating transactions undo earlier steps. Choreography (events) or orchestration (a central coordinator).

?? What is the transactional outbox pattern?
Write the business data and an 'outbox' event row in the same DB transaction; a relay/CDC (Debezium) publishes outbox rows to Kafka. Avoids the dual-write problem (DB committed but message lost, or the reverse).

?? How do you maintain data consistency across services?
Database per service + eventual consistency via events, sagas, the outbox, idempotent consumers and reconciliation jobs.

?? How do you trace a request across services?
Correlation/trace IDs propagated in headers (W3C traceparent), OpenTelemetry/Micrometer Tracing, viewed in Zipkin/Jaeger/Tempo; logs include the trace ID.

?? What happens when a downstream service is slow or down?
Timeouts, retries with backoff (only for idempotent operations), a circuit breaker, a fallback/cached response, bulkheads, async queues to absorb the load, and alerts.

## Kafka

?? Explain topics, partitions and offsets
A topic is a named log split into partitions (ordered, append-only). Each record has an offset within its partition. Partitions enable parallelism and are replicated across brokers.

?? How do consumer groups work?
Each partition is consumed by exactly one consumer in a group, so consumers beyond the partition count sit idle. Different groups each get all the messages (pub-sub).

?? How does Kafka guarantee ordering?
Only within a partition. Use the same key (e.g. accountId) to send related events to the same partition.

?? acks=0/1/all?
0: fire-and-forget. 1: the leader wrote it. all: all in-sync replicas wrote it (the safest, combined with min.insync.replicas=2).

?? Delivery semantics: at-most-once, at-least-once, exactly-once?
At-most: commit before processing (may lose data). At-least: commit after processing (may duplicate, the most common choice). Exactly-once: idempotent producer + transactions (read-process-write within Kafka) or idempotent consumers on the sink side.

?? How do you handle duplicate messages?
Make consumers idempotent: store processed message IDs / use unique constraints / upserts, and check before applying side effects.

?? What triggers a rebalance and why is it a problem?
A consumer joins/leaves/crashes, polls exceed max.poll.interval.ms, or partitions change. Processing pauses during a rebalance. Mitigate with cooperative sticky assignment, static membership and fast processing.

?? What is consumer lag?
Latest offset minus committed offset: how far behind consumers are. Monitor it; growing lag means consumers can't keep up (scale consumers up to the partition count, optimise processing).

?? How do you handle poison messages / failures in Spring Kafka?
DefaultErrorHandler with backoff retries, then publish to a dead-letter topic (DeadLetterPublishingRecoverer), or non-blocking retry topics (@RetryableTopic). Monitor and reprocess the DLT.

?? Kafka vs RabbitMQ?
Kafka: a distributed log, high throughput, retention & replay, ordering per partition, stream processing. RabbitMQ: a traditional broker, flexible routing, per-message acks, messages deleted after consumption. Kafka suits event streaming; RabbitMQ suits task queues.

?? How many partitions should a topic have?
Based on target throughput / per-consumer throughput, expected parallelism and future growth. You can add partitions later but not remove them, and adding them changes key → partition mapping.

?? What is KRaft?
Kafka's built-in Raft-based metadata quorum that replaced ZooKeeper (ZooKeeper was removed in Kafka 4.0). Simpler operations and faster controller failover.

?? What is log compaction?
Keeps at least the latest value per key instead of deleting by time. Useful for state/changelog topics (e.g. the latest account balance).

## SQL

?? Second highest salary
SELECT MAX(salary) FROM emp WHERE salary < (SELECT MAX(salary) FROM emp); or use DENSE_RANK() = 2 in a subquery/CTE.

?? Nth highest salary per department / top 3 per department
WITH r AS (SELECT *, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) rk FROM emp) SELECT * FROM r WHERE rk <= 3;

?? Find duplicate records
SELECT col, COUNT(*) FROM t GROUP BY col HAVING COUNT(*) > 1;

?? Delete duplicates but keep one
WITH d AS (SELECT id, ROW_NUMBER() OVER (PARTITION BY email ORDER BY id) rn FROM t) DELETE FROM t WHERE id IN (SELECT id FROM d WHERE rn > 1);

?? Employees who earn more than their manager
SELECT e.name FROM emp e JOIN emp m ON e.manager_id = m.id WHERE e.salary > m.salary; (self join)

?? Find rows in one table that don't exist in another
SELECT a.* FROM orders a LEFT JOIN shipments s ON a.id = s.order_id WHERE s.order_id IS NULL; Also know NOT EXISTS, FULL OUTER JOIN for mismatches on both sides, and EXCEPT.

?? Difference between JOIN types
INNER: only matches. LEFT: all left rows + matches (NULLs otherwise). RIGHT: the mirror. FULL: everything from both. CROSS: Cartesian product. SELF: a table joined to itself.

?? WHERE vs HAVING
WHERE filters rows before grouping; HAVING filters groups after aggregation.

?? DELETE vs TRUNCATE vs DROP
DELETE: row-level, can use WHERE, logged, fires triggers, can roll back. TRUNCATE: removes all rows fast, minimal logging, resets identity. DROP: removes the table itself.

?? UNION vs UNION ALL
UNION removes duplicates (extra sort/hash cost); UNION ALL keeps all rows and is faster.

?? How do indexes work? When is an index not used?
Usually a B-tree for fast lookups/range scans. Not used for: functions on the column (WHERE UPPER(name)=…), leading wildcards (LIKE '%x'), low selectivity, a type mismatch, or not using the leftmost column of a composite index.

?? Clustered vs non-clustered index
Clustered: table rows physically ordered by the key (one per table, e.g. a MySQL InnoDB primary key). Non-clustered: a separate structure pointing to the rows.

?? ACID and isolation levels
Atomicity, Consistency, Isolation, Durability. Levels: Read Uncommitted (dirty reads) → Read Committed (Postgres default) → Repeatable Read (MySQL default) → Serializable (no anomalies, the slowest).

?? Running total with window functions
SELECT txn_date, amount, SUM(amount) OVER (ORDER BY txn_date ROWS UNBOUNDED PRECEDING) AS running_total FROM txns;

?? Explain normalization
Removing redundancy: 1NF atomic values; 2NF no partial dependency on a composite key; 3NF no transitive dependency. Denormalize deliberately for read performance.

## JavaScript & React

?? var vs let vs const?
var: function-scoped, hoisted & initialized as undefined. let/const: block-scoped, hoisted but in the temporal dead zone. const can't be reassigned (objects can still be mutated).

?? What is a closure?
A function remembers variables from its outer scope even after the outer function returns. Used for data privacy, function factories, debounce, and hooks in React.

?? Explain the event loop
JS is single-threaded. The call stack runs sync code; async callbacks queue up. Microtasks (promises) run before the next macrotask (setTimeout, I/O). Classic question: the output order of console.log + setTimeout + Promise.

?? Promises vs async/await?
async/await is syntax over promises that makes async code read like sync code. Handle errors with try/catch; use Promise.all for parallel calls.

?? How does 'this' work?
Determined by how a function is called: method call → the object; plain call → undefined (strict mode); arrow functions inherit this lexically; call/apply/bind set it explicitly.

?? What is the virtual DOM / reconciliation in React?
React builds a tree of elements, diffs the new tree against the previous one and applies minimal DOM updates. Keys help it match list items.

?? Props vs state?
Props: inputs from the parent, read-only. State: data owned by the component that triggers a re-render when updated with its setter.

?? How does useEffect work? Dependencies & cleanup?
Runs after render. [] = run on mount; [a, b] = run when they change; no array = run every render. Return a cleanup function (unsubscribe, abort fetch, clear timers). Avoid missing dependencies.

?? Why are keys important in lists?
They give items stable identity so React can reorder/update correctly. Don't use the array index when items can be reordered, inserted or deleted.

?? Controlled vs uncontrolled components?
Controlled: the input value lives in React state (value + onChange). Uncontrolled: the DOM holds the value, read via a ref. React Hook Form uses uncontrolled inputs for performance.

?? useMemo vs useCallback vs React.memo?
useMemo caches a computed value; useCallback caches a function reference; React.memo skips re-rendering a component when its props are shallow-equal. Use them when profiling shows a problem.

?? Context vs Redux/Zustand?
Context: passing rarely-changing values (theme, auth user) without prop drilling; every consumer re-renders on change. Redux Toolkit/Zustand: complex or frequently-updated global state, devtools, middleware. Server data belongs in TanStack Query.

?? How do you implement protected routes with JWT?
Store the auth state in context; a wrapper route checks for a valid token/user, otherwise it redirects to /login. Attach the token via an axios interceptor; handle 401s with refresh/logout. Prefer httpOnly cookies over localStorage for security.

?? What is CORS and how do you fix CORS errors with Spring Boot?
The browser blocks cross-origin requests unless the server allows them. Configure allowed origins/methods/headers in Spring (CorsConfigurationSource in Security, or @CrossOrigin), or use a dev proxy in Vite.

?? Debounce vs throttle?
Debounce: run only after the calls stop for X ms (search box). Throttle: run at most once per X ms (scroll handler). Be ready to write debounce.

## DevOps, Git & Testing

?? git merge vs rebase?
Merge preserves history with a merge commit. Rebase replays commits on top of another branch for linear history. Never rebase shared/public branches.

?? git reset vs revert?
reset moves the branch pointer (rewrites history, local use). revert creates a new commit that undoes a previous one (safe for shared branches).

?? Write a Dockerfile for a Spring Boot app
Multi-stage: build with a Maven + JDK image, copy the jar into a slim JRE image (e.g. eclipse-temurin:21-jre), run as a non-root user, EXPOSE 8080, ENTRYPOINT ["java","-jar","app.jar"]. Mention layered jars for caching.

?? Image vs container?
Image: a read-only template (layers). Container: a running instance of an image with a writable layer.

?? Describe a CI/CD pipeline you'd set up
On PR: build, unit + integration tests (Testcontainers), static analysis (Sonar), security scan. On merge: build the Docker image, push it to a registry, deploy to dev → QA → prod with approvals, smoke tests, rollback plan.

?? Pod vs Deployment vs Service in Kubernetes?
Pod: the smallest unit, one or more containers. Deployment: manages replicas & rolling updates of pods. Service: a stable network endpoint/load balancer in front of pods.

?? Unit vs integration tests?
Unit: one class in isolation with mocks; fast. Integration: multiple components together with a real DB/Kafka (Testcontainers), slower but catches wiring issues.

?? Mockito mock vs spy?
Mock: a fake object, all methods stubbed (return defaults). Spy: wraps a real object, real methods run unless stubbed.

?? @WebMvcTest vs @SpringBootTest?
@WebMvcTest loads only the web layer (controllers, advice) with MockMvc, so it is fast. @SpringBootTest loads the full context, for end-to-end integration tests.

## SE Practices & Agile

?? Explain the Scrum ceremonies
Sprint planning (what & how), daily stand-up (15 min: progress, plan, blockers), sprint review (demo to stakeholders), retrospective (improve the process). Sprints are usually 2 weeks.

?? What are story points? How do you estimate?
A relative measure of effort, complexity and uncertainty, not hours. Fibonacci scale, planning poker for team consensus; velocity is used for forecasting.

?? What's a Definition of Done?
A shared checklist an item must meet to count as done: code reviewed, tests written & passing, documentation updated, deployed to QA, acceptance criteria met.

?? Requirements change mid-sprint: what do you do?
Raise it with the PO; the sprint goal is protected. Changes usually go to the backlog for the next sprint. If urgent, the PO negotiates a swap of equal size or the sprint is cancelled in extreme cases.

?? Explain SOLID with examples
S: one reason to change (separate the ReportGenerator from the ReportPrinter). O: extend without modifying (new DiscountStrategy implementations). L: subtypes must be substitutable (the Square/Rectangle problem). I: small focused interfaces. D: depend on abstractions (inject a PaymentGateway interface).

?? Which design patterns have you used?
Strategy (pluggable discount/shipping rules in ShopSphere), Builder (DTOs/Lombok), Factory (parser per file type), Observer (Kafka events / Spring events), Singleton (Spring beans), Template Method (Spring's JdbcTemplate/KafkaTemplate), Adapter (wrapping an external API).

?? What do you look for in a code review?
Correctness & edge cases, readability/naming, tests, security issues, performance concerns, consistency with conventions, small focused PRs. Be kind and specific; review the code, not the person.

?? How do you handle a production incident?
Acknowledge it, assess the impact, communicate, mitigate first (rollback, feature flag, restart), then find the root cause using logs/metrics/traces, fix it, and run a blameless postmortem with action items. (Use a real example from your current production-support work.)

## Coding / DSA (practise in Java)

?? Most-asked problems for 0–3 yrs
Two Sum, Valid Parentheses, Reverse Linked List, Merge Two Sorted Lists, Best Time to Buy/Sell Stock, Longest Substring Without Repeating Characters, Group Anagrams, Top K Frequent Elements, Merge Intervals, Product of Array Except Self, Binary Search variants, Kth Largest Element, Level Order Traversal, Number of Islands, LRU Cache, Missing Number, Rotate Array, Climbing Stairs, Coin Change.

?? Java-specific coding questions
Thread-safe singleton; producer-consumer with BlockingQueue; print odd/even with two threads; a custom immutable class; an LRU cache with LinkedHashMap; character frequency with streams; a custom exception plus handling; sorting with a Comparator chain.

?? How to approach a coding round
Clarify inputs/edge cases → state a brute-force approach + its complexity → optimise → code cleanly → dry-run with an example → discuss time/space complexity. Think out loud the whole time.

?? Low-level design questions
Parking lot, library management, Splitwise, ATM, elevator, rate limiter, vending machine. Approach: requirements → entities/classes → relationships → key methods → patterns → code the core flow.

## System Design (basic level)

?? Design a URL shortener
APIs (shorten, redirect), ID generation (base62 of a counter/Snowflake ID), DB (key → URL), cache hot URLs in Redis, 301 vs 302 redirects, analytics via async events, scaling reads.

?? Design a notification service
API → Kafka topic → channel workers (email/SMS/push) → provider adapters; user preferences, templates, retries + DLQ, rate limiting, idempotency, status tracking.

?? Design an e-commerce order system
Services: catalog, cart, order, inventory, payment, notification. Order flow as a Saga over Kafka; inventory reservation with optimistic locking; idempotency keys on checkout; outbox for events; Redis cache for the catalog; read replicas; order status tracking. Prepare it well, since it matches ShopSphere.

?? Design a ticket booking system (BookMyShow)
Seat holds with TTL (Redis lock), preventing double booking (locking/unique constraints), payment timeout releases seats, high traffic on popular shows (queues, caching), notifications.

?? Design a rate limiter
Algorithms: token bucket, leaky bucket, fixed/sliding window. Redis counters with TTL/Lua for distributed limits; apply at the API gateway; return 429 with Retry-After.

?? How would you scale a Spring Boot app?
Stateless services behind a load balancer, horizontal scaling (K8s HPA), caching (Redis), DB read replicas & indexing, connection pool tuning, async processing via Kafka, CDN for static content.

## HR & Behavioural (use STAR)

?? Tell me about yourself
Use the 60–90 second script in 'The Switch Story' tab: past (current role) → present (Java skills + projects) → future (this role).

?? Why are you switching to development?
Moving toward building systems, not away from your current role. See the script in 'The Switch Story' tab.

?? Why switch so early?
Foundation years; the switch costs least now; domain knowledge as a head start. See 'The Switch Story' tab.

?? Why do you want to join our company?
Research it: their tech stack, products, engineering blog and what interests you about their product. Connect 2 specifics to your goals and skills.

?? What are your strengths and weaknesses?
Strength with evidence (e.g. attention to data accuracy from your current role, fast self-learning shown by the projects). A real weakness you're working on (e.g. 'still building depth in system design, so I'm doing X').

?? Describe a challenging problem you solved
STAR: a hard data issue you traced at work, or a tough bug in your project (e.g. Kafka duplicate processing → made the consumer idempotent). Quantify the result.

?? Tell me about a conflict with a teammate or stakeholder
STAR; focus on understanding their view, data-driven discussion, the outcome and what you learned. No blaming.

?? Tell me about a mistake you made
A real, moderate mistake, owned honestly → how you fixed it → the process change you introduced so it doesn't recur.

?? Working under a tight deadline
E.g. a production deadline at your current job: prioritised, communicated early, delivered. Use a real example.

?? How do you learn a new technology?
Official docs → a small hands-on project → go deeper on internals → teach/write about it. Point to how you learned Spring Boot/Kafka while working full-time.

?? Where do you see yourself in 5 years?
A strong full stack/backend engineer who owns services end to end, mentors juniors, and designs systems with confidence. Linked to growth at this company.

?? Do you have any questions for us?
Always ask 2–3. See the list in 'The Switch Story' tab.
