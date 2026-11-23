What separates a Java developer from someone who knows Java syntax. Concurrency and JVM questions come up in most Java interviews.

This track lists every topic to learn, in order, with mini projects to build and the best resources. Work through the topics with the resources below, then build at least two of the projects.

## Multithreading & concurrency

### Process vs thread; Thread, Runnable, Callable; thread lifecycle & states

Learn this from the resources at the end of the section.

### synchronized (methods/blocks), intrinsic locks, volatile, happens-before

Learn this from the resources at the end of the section.

### Java Memory Model, visibility & atomicity problems, race conditions

Learn this from the resources at the end of the section.

### wait/notify, producer-consumer problem

Learn this from the resources at the end of the section.

### Deadlock, livelock, starvation: causes and prevention

Learn this from the resources at the end of the section.

### Locks: ReentrantLock, ReadWriteLock, StampedLock, Condition

Learn this from the resources at the end of the section.

### Atomic classes (AtomicInteger, LongAdder), CAS

Learn this from the resources at the end of the section.

### ExecutorService, thread pools (fixed, cached, scheduled), ThreadPoolExecutor parameters

Learn this from the resources at the end of the section.

### Future, CompletableFuture (thenApply, thenCompose, allOf, exception handling)

Learn this from the resources at the end of the section.

### Concurrent collections: ConcurrentHashMap, CopyOnWriteArrayList, BlockingQueue

Learn this from the resources at the end of the section.

### Synchronizers: CountDownLatch, CyclicBarrier, Semaphore, Phaser

Learn this from the resources at the end of the section.

### ForkJoinPool & work stealing; ThreadLocal

Learn this from the resources at the end of the section.

### Virtual threads (Java 21): what they are, when to use, pinning

Hot topic in 2025–26 interviews.

### Structured concurrency & scoped values (newer Java)

Learn this from the resources at the end of the section.


## JVM internals & performance

### Class loading: bootstrap, platform, application class loaders; delegation model

Learn this from the resources at the end of the section.

### JVM memory: heap (young/old), stack, metaspace, code cache

Learn this from the resources at the end of the section.

### Garbage collection: mark-sweep, generational GC, G1, ZGC; GC roots

Learn this from the resources at the end of the section.

### JIT compilation, bytecode basics (javap)

Learn this from the resources at the end of the section.

### Memory leaks in Java, OutOfMemoryError & StackOverflowError causes

Learn this from the resources at the end of the section.

### Profiling & diagnosis: heap dumps, thread dumps, VisualVM, JFR, JDK Mission Control

Learn this from the resources at the end of the section.

### Common JVM flags: -Xms, -Xmx, -XX:+UseG1GC

Learn this from the resources at the end of the section.


## Modern Java (17 → 21 → 25)

### Records, sealed classes, pattern matching for instanceof

Learn this from the resources at the end of the section.

### Switch expressions & pattern matching for switch, record patterns

Learn this from the resources at the end of the section.

### Text blocks, enhanced NullPointerException messages

Learn this from the resources at the end of the section.

### Java 21: virtual threads, sequenced collections, record patterns

Learn this from the resources at the end of the section.

### Java 25: compact source files & instance main methods, flexible constructor bodies, scoped values, module imports

Learn this from the resources at the end of the section.

### Java modules (JPMS): awareness level

Learn this from the resources at the end of the section.


## Reflection, annotations & JDBC

### Reflection API basics, how frameworks like Spring use it

Learn this from the resources at the end of the section.

### Built-in & custom annotations, retention policies

Learn this from the resources at the end of the section.

### Dynamic proxies (JDK proxy vs CGLIB): foundation of Spring AOP

Learn this from the resources at the end of the section.

### JDBC: DriverManager, Connection, PreparedStatement, ResultSet, batch updates

Learn this from the resources at the end of the section.

### Transactions in JDBC, connection pooling (HikariCP), SQL injection prevention

Learn this from the resources at the end of the section.

### Logging: SLF4J + Logback, log levels, structured logging, MDC

Learn this from the resources at the end of the section.

## Hands-on mini projects

Build at least two of these before moving on. Put each on GitHub with a short README.

### Multithreaded Web Crawler (Intermediate)

Crawl from a start URL to depth N in parallel, avoid revisiting URLs, collect page titles. Compare a fixed thread pool vs virtual threads.

**Practises:** ExecutorService, ConcurrentHashMap, CompletableFuture, virtual threads

### Build your own Thread Pool (Intermediate)

Write a simple thread pool with a BlockingQueue, worker threads, shutdown and a rejection policy, then compare it with ThreadPoolExecutor.

**Practises:** wait/notify, BlockingQueue, thread lifecycle

### Parallel Log Analyser (Intermediate)

Process a large log file (generate 1 GB) in chunks across threads: count errors per hour, top endpoints, slowest requests. Measure the speed-up.

**Practises:** Fork/Join or executors, NIO Files, streams, benchmarking

### Rate-limited API Aggregator (Advanced)

Call 5 public APIs in parallel, combine the results, apply timeouts and a fallback when one is slow, and limit to N calls/second.

**Practises:** CompletableFuture (allOf, orTimeout), Semaphore, structured concurrency

### Memory Leak & GC Lab (Advanced)

Write a program that leaks memory (static map, unclosed resources), capture a heap dump, find the leak in VisualVM/Eclipse MAT, fix it and compare GC logs.

**Practises:** JVM memory, GC, JFR, heap/thread dumps

## Resources

::: resources
- Book: [Java Concurrency in Practice, Goetz](https://jcip.net/) · The concurrency classic
- [Jenkov: Java Concurrency](https://jenkov.com/tutorials/java-concurrency/index.html) · Clear free concurrency tutorials
- [Baeldung: Concurrency series](https://www.baeldung.com/java-concurrency) · Executors, locks, CompletableFuture…
- [Inside Java](https://inside.java/) · Official news on JVM and language features
- [OpenJDK JEPs](https://openjdk.org/jeps/0) · Read the JEP for each new feature (Java 21, 25)
- Video: [Defog Tech: Concurrency](https://www.youtube.com/results?search_query=Defog+Tech+Java+concurrency) · Short, sharp concurrency videos
- Video: [Java Brains: Java features](https://www.youtube.com/results?search_query=Java+Brains+Java+21+features) · Modern Java explained
- Book: [Modern Java in Action](https://www.manning.com/books/modern-java-in-action) · Streams, lambdas, CompletableFuture in depth
:::
