This section is the second step of Core Java. It assumes you have worked through **Core Java 1: language basics and OOP**: you can write classes, interfaces and records, you know that variables hold references to objects on the heap, and you know why `equals` and `hashCode` must be overridden together. Here you put those ideas to work in the three areas that fill most of every Java codebase and most of every Core Java interview: what happens when things go wrong (exceptions), how to write code that works for any type while staying type-safe (generics), and how to store and look up data in memory (the collections framework).

**What you will be able to do afterwards**

- Explain the exception hierarchy, choose between checked and unchecked exceptions, and write `try`-with-resources code that never leaks a connection or a file.
- Design custom exceptions for a service, chain causes correctly, and log failures in a way that helps the person on call at 3 a.m.
- Write generic classes and methods with bounds, apply the PECS rule to wildcards, and explain what type erasure removes and why.
- Pick the right collection for a job (`ArrayList`, `ArrayDeque`, `HashMap`, `TreeMap`, `PriorityQueue`...) and justify it with Big-O and internals.
- Answer the classic interview set with confidence: how `HashMap` works inside, fail-fast iterators, `Comparable` vs `Comparator`, `List.of` vs `Collections.unmodifiableList`, and the Java 21 sequenced collections.

**The big picture.** Think of a running Java program as ShopSphere's order service handling one request. The **collections** hold the data while it is being worked on: the lines of a basket in a `List`, the stock levels in a `Map`, the pending jobs in a `Queue`. **Generics** are the labels on those containers (`List<OrderLine>`, `Map<String, Integer>`) that let the compiler catch "you put a customer into the product list" before the code ever runs. **Exceptions** are the emergency exit: when a payment is declined or the database is down, an exception object travels up the call stack until some code that knows what to do catches it. Most bugs you will fix in support and in development sit exactly at the seams of these three: a swallowed exception, an unchecked cast, a `HashMap` key that changed after it was stored.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

> **Tip:** Keep `jshell` open. Collections are perfect for it: `var m = new HashMap<String, Integer>(); m.put("pen", 3); m` shows the map at once. Every complete program in this section also runs with `java Name.java`.

## Exception handling

Every program meets failures it cannot prevent: a file is missing, a network call times out, a customer sends a negative quantity. Java models each failure as an **object**, an instance of a class that extends `Throwable`, and gives you language features to **throw** it, **catch** it and **clean up** after it. Because exceptions are ordinary classes, everything you learned about inheritance in Core Java 1 applies: catching a parent type catches all its subclasses.

The four topics go from the hierarchy (what kinds of failure exist), through the syntax (`try`, `catch`, `finally`, `try`-with-resources), to designing your own exceptions, and finally to the habits that separate production-quality code from code that hides bugs.

[[fig:corecoll_exc]]

### Exception hierarchy: Throwable, Error, checked vs unchecked

::: plain
Imagine a delivery company. Some problems are the kind a driver is expected to handle: the customer is not home, so leave a note and try tomorrow. Those are **checked exceptions**: the rules force you to plan for them. Some problems are mistakes in the plan itself, like an address that was typed wrong: those are **unchecked exceptions**, and the fix is to correct the plan, not to handle them on every trip. And some problems are disasters nobody on the route can fix, like the depot burning down: those are **errors**, and the driver should not even try.
:::

**Everything throwable extends `Throwable`.** Only objects whose class extends `java.lang.Throwable` can be thrown with `throw` or caught with `catch`. `Throwable` has two direct subclasses, and one of them is split again:

| Branch | Meaning | Examples | Must you declare or catch it? |
|---|---|---|---|
| `Error` | Serious problem in the JVM or environment; the application usually cannot recover | `OutOfMemoryError`, `StackOverflowError`, `NoClassDefFoundError` | No |
| `Exception` (but not `RuntimeException`) | **Checked**: an expected failure outside your control | `IOException`, `SQLException`, `InterruptedException`, `TimeoutException` | Yes |
| `RuntimeException` and subclasses | **Unchecked**: usually a programming bug or invalid input | `NullPointerException`, `IllegalArgumentException`, `IllegalStateException`, `IndexOutOfBoundsException`, `ArithmeticException`, `ClassCastException` | No |

**Checked vs unchecked** is a compile-time rule, not a runtime one. For a **checked** exception the compiler applies the "catch or specify" requirement: a method that can throw it must either catch it or declare it in its `throws` clause. **Unchecked** exceptions (`RuntimeException`, `Error` and their subclasses) are exempt. At runtime the JVM treats all of them the same way.

```java
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class Hierarchy {
    public static void main(String[] args) {
        // Unchecked: the compiler does not force us to handle these
        List<String> items = List.of("pen");
        try {
            items.get(3);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("unchecked: " + e.getClass().getSimpleName());
        }

        // Checked: remove the try/catch and javac says
        // "unreported exception FileNotFoundException; must be caught or declared to be thrown"
        try (FileReader r = new FileReader("missing-stock.csv")) {
            System.out.println(r.read());
        } catch (IOException e) {
            System.out.println("checked: " + e.getClass().getSimpleName());
        }

        // Every exception is a Throwable; RuntimeException is an Exception
        Throwable t = new IllegalStateException("cart is closed");
        System.out.println(t instanceof Exception);
        System.out.println(t instanceof RuntimeException);
        Throwable oom = new OutOfMemoryError();
        System.out.println(oom instanceof Exception);
    }
}
// Output:
// unchecked: IndexOutOfBoundsException
// checked: FileNotFoundException
// true
// true
// false
```

**What an exception object carries.** Every `Throwable` has a **message** (`getMessage()`), an optional **cause** (`getCause()`, another `Throwable` that triggered this one), a **stack trace** (the chain of method calls active when it was created, `getStackTrace()`), and a list of **suppressed** exceptions (explained with `try`-with-resources). Printing it with `e.printStackTrace()` or a logger shows all of these.

**How to read a stack trace.** The first line is the exception class and message. The lines after it start with `at` and list the call stack, **most recent call first**. The first `at` line inside your own package is usually where to start looking. `Caused by:` sections below show the cause chain, and the **last** `Caused by:` is usually the root problem.

```text
Exception in thread "main" java.lang.IllegalStateException: Payment failed for order O-42
    at com.shopsphere.orders.OrderService.checkout(OrderService.java:31)
    at com.shopsphere.app.Main.main(Main.java:9)
Caused by: java.net.SocketTimeoutException: Read timed out
    at ...
```

**Which kind should you use?** The usual guidance (Effective Java, Item 70): use **checked** exceptions for conditions the caller can reasonably recover from and is expected to handle, and **runtime** exceptions for programming errors (precondition violations such as a null argument or an index out of range). In practice, modern frameworks lean heavily towards unchecked: Spring translates `SQLException` into its unchecked `DataAccessException` family, and most application code defines unchecked business exceptions. Checked exceptions also fit badly with lambdas and streams, because `Function.apply` declares no checked exceptions.

> **Pitfall:** Never catch `Throwable` or `Error` in ordinary code. Catching `OutOfMemoryError` and carrying on leaves the JVM in an unknown state. The only usual exception is a top-level handler in a framework or thread pool that logs the failure and stops cleanly.

> **Tip:** `NoClassDefFoundError` (an `Error`) and `ClassNotFoundException` (a checked exception) look alike but differ: the first means a class that was present at compile time is missing at runtime (a classpath or dependency problem), the second means code asked to load a class by name (`Class.forName`) and it was not found.

::: deeper
**Checked exceptions exist only in the compiler.** The JVM has no idea which exceptions are checked: the `throws` clause is stored in the class file as an `Exceptions` attribute, used by `javac` and reflection, and ignored at runtime. You can see this with a "sneaky throw": a generic method `<E extends Throwable> void sneaky(Throwable t) throws E { throw (E) t; }` called as `Main.<RuntimeException>sneaky(new IOException())` throws a checked exception without declaring it, because the cast to `E` is erased. Lombok's `@SneakyThrows` uses this trick. Other JVM languages such as Kotlin and Scala have no checked exceptions at all.

**Throwing is cheap, creating is not.** The expensive part of an exception is `fillInStackTrace()`, called by the constructor, which walks the stack. For exceptions used as control flow in hot code, frameworks sometimes override it or use the protected constructor `Throwable(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)` with `writableStackTrace = false`. HotSpot also has an optimisation (`-XX:+OmitStackTraceInFastThrow`, on by default) that, after an implicit exception such as an NPE is thrown many times at the same place, starts reusing a preallocated exception **without a stack trace**. If production logs show `java.lang.NullPointerException` with no trace, this is why: look further back in the log for the first occurrence.

**Helpful NullPointerExceptions** (JEP 358, on by default since Java 15) make the message name the null expression, for example `Cannot invoke "String.length()" because "<local1>" is null` (local variable names appear when the code was compiled with `-g`).

**`StackOverflowError` vs `OutOfMemoryError`.** The first means one thread's stack is full, almost always runaway recursion; the stack size is set by `-Xss`. The second means the heap (or metaspace, or native memory for threads) is exhausted; its message says which (`Java heap space`, `Metaspace`, `unable to create native thread`).

> **Interview:** "Difference between checked and unchecked exceptions?" Checked exceptions extend `Exception` but not `RuntimeException`; the compiler forces callers to catch or declare them, and they model recoverable conditions outside the program's control such as I/O. Unchecked exceptions extend `RuntimeException` (or are `Error`s); they are not checked at compile time and usually signal bugs or invalid arguments. The distinction is enforced only by `javac`, not by the JVM.

> **Interview:** "Is it a good idea to catch `Exception`?" Only at a boundary (a request handler, a scheduled job, a message listener) where you log and convert the failure into a response. Deep in business code, catch the specific types you can handle, otherwise you also catch bugs such as `NullPointerException` and hide them.
:::

::: check
?? Is `IllegalArgumentException` checked or unchecked, and how can you tell?
Unchecked: it extends `RuntimeException`. Any class under `RuntimeException` or `Error` is unchecked; everything else under `Exception` is checked.

?? Why should application code not catch `OutOfMemoryError`?
It is an `Error`: the JVM is in a state the application cannot reliably repair, and continuing may corrupt data or fail again unpredictably. Let it propagate, and configure the JVM to dump the heap so you can analyse it.

?? A stack trace has three `Caused by:` sections. Which one usually shows the real problem?
The last one, the root cause. Each `Caused by` wraps the one below it, so the deepest is where the failure started.
:::

::: resources
- Docs: [Exceptions (Java tutorial)](https://docs.oracle.com/javase/tutorial/essential/exceptions/index.html) · Oracle's lesson on catch-or-specify and the hierarchy
- Docs: [Throwable (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Throwable.html) · message, cause, suppressed exceptions and stack traces
- Spec: [JLS chapter 11: Exceptions](https://docs.oracle.com/javase/specs/jls/se21/html/jls-11.html) · the formal checked vs unchecked rules
- Docs: [Exceptions on dev.java](https://dev.java/learn/exceptions/) · the modern official tutorial
- Article: [Checked and unchecked exceptions in Java](https://www.baeldung.com/java-checked-unchecked-exceptions) · Baeldung with examples
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Items 69 to 77 on exceptions
:::

### try-catch-finally, multi-catch, try-with-resources, AutoCloseable

::: plain
A `try` block is like starting a risky job with a safety plan written beside it. The `catch` parts say "if this goes wrong, do that instead". The `finally` part is the cleanup that always happens, success or failure, like switching off the oven whether the cake rose or not. `try`-with-resources is a shortcut for the most common cleanup: anything you open at the start (a file, a database connection) is closed automatically at the end, in the right order, even if something blows up.
:::

**The basic shape.** When code in `try` throws, the JVM stops the `try` block immediately and checks each `catch` clause **top to bottom**, running the first one whose type matches. If none matches, the exception continues up the call stack. A `finally` block runs in every case: after a normal finish, after a caught exception, after an uncaught one, and even after `return`, `break` or `continue`.

```java
public class TryDemo {
    static int parseQty(String raw) {
        try {
            int q = Integer.parseInt(raw);
            System.out.println("parsed " + q);
            return q;
        } catch (NumberFormatException e) {
            System.out.println("bad quantity: " + e.getMessage());
            return 0;
        } finally {
            System.out.println("finally for " + raw);
        }
    }

    public static void main(String[] args) {
        System.out.println(parseQty("3"));
        System.out.println(parseQty("three"));
    }
}
// Output:
// parsed 3
// finally for 3
// 3
// bad quantity: For input string: "three"
// finally for three
// 0
```

Note the order: the `return` value is computed first, then `finally` runs, then the method actually returns.

**Order of catch clauses.** Put **specific types before general ones**. A `catch (Exception e)` before `catch (IOException e)` does not compile: javac reports "exception IOException has already been caught", because the second clause is unreachable.

**Multi-catch (Java 7).** When several exceptions need the same handling, list them with `|`:

```java
try {
    service.importStock(path);
} catch (IOException | IllegalArgumentException e) {
    log.warn("stock import skipped: {}", e.getMessage());
}
```

The variable `e` is implicitly `final`, and the alternatives must not be subclasses of each other (`FileNotFoundException | IOException` does not compile, since the first is already covered by the second).

**try-with-resources (Java 7).** Before Java 7, closing a resource correctly needed a `finally` block with its own nested `try` and null checks, and it was easy to get wrong. Now you declare resources in parentheses after `try`. Any object whose class implements **`AutoCloseable`** (one method: `void close() throws Exception`) can be used. `Closeable` (from `java.io`) is an older sub-interface whose `close` throws only `IOException`.

```java
public class TwrDemo {
    public static void main(String[] args) {
        try (var conn = new Resource("db-connection");
             var stmt = new Resource("statement")) {
            System.out.println("running query");
            throw new IllegalStateException("query failed");
        } catch (IllegalStateException e) {
            System.out.println("caught: " + e.getMessage());
            for (Throwable s : e.getSuppressed()) {
                System.out.println("suppressed: " + s.getMessage());
            }
        } finally {
            System.out.println("finally");
        }
    }
}

class Resource implements AutoCloseable {
    private final String name;
    Resource(String name) { this.name = name; System.out.println("open " + name); }
    @Override public void close() {
        System.out.println("close " + name);
        if (name.equals("statement")) throw new IllegalStateException("close failed: " + name);
    }
}
// Output:
// open db-connection
// open statement
// running query
// close statement
// close db-connection
// caught: query failed
// suppressed: close failed: statement
// finally
```

Three rules show up in that output:

1. Resources are closed in **reverse order** of opening (the statement before the connection that created it).
2. Resources are closed **before** any `catch` or `finally` block runs.
3. If the body throws and `close` also throws, the body's exception wins and the `close` exception is attached to it as a **suppressed** exception (`getSuppressed()`), so nothing is lost. The old `finally` approach silently replaced the original exception with the close failure.

Since **Java 9** you can also list an existing effectively final variable: `try (conn) { ... }`.

**Realistic use.** JDBC is the classic case (the SQL section uses it heavily):

```java
String sql = "SELECT qty FROM stock WHERE sku = ?";
try (Connection c = dataSource.getConnection();
     PreparedStatement ps = c.prepareStatement(sql)) {
    ps.setString(1, sku);
    try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getInt(1) : 0;
    }
}
```

Without the automatic close, every exception would leak a connection, and the pool would run dry after a few hundred failed requests: a classic production incident.

> **Pitfall:** Never `return` from a `finally` block. It silently discards any exception thrown in `try` or `catch`, and overrides any earlier `return` value. javac warns about it with `-Xlint:finally` ("finally clause cannot complete normally").

> **Tip:** Locks are not `AutoCloseable`, so the standard pattern stays `lock.lock(); try { ... } finally { lock.unlock(); }`, with `lock()` called **before** the `try`, so a failed `lock()` never triggers an `unlock()`.

::: deeper
**How `finally` is compiled.** There is no "finally" instruction in bytecode. javac **copies** the `finally` code onto every exit path of the `try` and `catch` blocks (normal fall-through, each `return`, each `catch`), and adds an entry to the method's **exception table** with type `any` that catches everything else, runs the copied code and rethrows. Run `javap -c` on `TryDemo` and you will see the `"finally for "` string built three times. Old JVMs used the `jsr`/`ret` instructions instead; they were removed from class files version 51 (Java 7) onwards.

**What try-with-resources expands to.** For `try (R r = init()) { body }` the compiler generates roughly:

```text
R r = init();
Throwable primary = null;
try { body }
catch (Throwable t) { primary = t; throw t; }
finally {
    if (r != null) {
        if (primary != null) {
            try { r.close(); } catch (Throwable s) { primary.addSuppressed(s); }
        } else {
            r.close();
        }
    }
}
```

A `null` resource is allowed and simply not closed. Since Java 11, javac generates a smaller version of this code, but the behaviour is identical.

**When `finally` does not run.** `System.exit()` inside `try`, the JVM crashing or being killed (`kill -9`), or an infinite loop in `try`. A daemon thread's `finally` may also never run when the JVM shuts down.

**`finally` and the return value.** For `int x = 1; try { return x; } finally { x = 2; }` the method returns 1: the return value was already saved in a local slot before `finally` ran. If `x` were a mutable object (a `StringBuilder`), changes made in `finally` **would** be visible to the caller, because the saved value is the reference.

> **Interview:** "What does try-with-resources give you over `finally`?" Automatic closing in reverse order, correct handling of `null` resources, and no lost exceptions: if both the body and `close` throw, the body's exception propagates and the close failure is attached as a suppressed exception. It works with any `AutoCloseable`.

> **Interview:** "Can a `try` block exist without `catch`?" Yes: `try`/`finally`, and `try`-with-resources with neither `catch` nor `finally`. A plain `try` with neither does not compile.
:::

::: check
?? A `try` block returns 1 and its `finally` block returns 2. What does the method return, and why is this bad style?
It returns 2. A `return` in `finally` overrides the earlier return and also discards any exception in flight, which can hide real failures.

?? In `try (var a = open("a"); var b = open("b"))`, which resource is closed first?
`b`: resources close in reverse order of declaration, so a resource is closed before the ones it may depend on.

?? What is a suppressed exception?
An exception thrown by `close()` while another exception from the `try` body is already propagating. It is attached to the primary exception with `addSuppressed` and can be read with `getSuppressed()`, so neither failure is lost.
:::

::: resources
- Docs: [The try-with-resources statement](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html) · Oracle tutorial with suppressed exceptions
- Docs: [AutoCloseable (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/AutoCloseable.html) · the contract and idempotency advice
- Spec: [JLS 14.20: The try statement](https://docs.oracle.com/javase/specs/jls/se21/html/jls-14.html#jls-14.20) · exact semantics of catch, finally and try-with-resources
- Article: [Java try-with-resources](https://www.baeldung.com/java-try-with-resources) · Baeldung with Java 9 changes
- Docs: [Catching and handling exceptions (dev.java)](https://dev.java/learn/exceptions/catching-handling/) · try, catch, finally and try-with-resources in the official tutorial
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 9: prefer try-with-resources to try-finally
:::

### throw vs throws, custom exceptions, exception chaining

::: plain
`throw` is the act of raising the alarm: "stop, something is wrong, here is what". `throws` is a warning label on a method: "calling me may raise this alarm, be ready". A custom exception is your own alarm with a precise name, such as `OutOfStockException`, so whoever catches it knows exactly what happened. Chaining is attaching the original note to your new one: "payment failed, and here is the bank's message that caused it".
:::

**`throw` vs `throws`.**

| | `throw` | `throws` |
|---|---|---|
| What it is | A statement | Part of a method or constructor declaration |
| Where | Inside a method body | After the parameter list |
| Followed by | One exception **object** | One or more exception **types** |
| Effect | Raises the exception now | Tells callers (and the compiler) what may escape |

```java
public int reserve(String sku, int qty) throws OutOfStockException {  // declaration
    if (qty <= 0) {
        throw new IllegalArgumentException("qty must be positive: " + qty);  // statement
    }
    ...
}
```

An unchecked exception may be listed in `throws` for documentation, but it is not required. For checked exceptions it is the "specify" half of catch-or-specify. An overriding method may throw **fewer or narrower** checked exceptions than the method it overrides, never broader ones, because callers using the parent type would not be ready for them.

**Custom exceptions.** Create one when callers need to tell your failure apart from others, or when it carries useful data. Extend `RuntimeException` for unchecked or `Exception` for checked, and provide constructors that accept a message and a cause. A good business exception carries the facts that explain it as fields, not only as text in the message:

```java
import java.util.HashMap;
import java.util.Map;

public class CustomDemo {
    public static void main(String[] args) {
        Inventory inv = new Inventory();
        inv.add("PEN-01", 5);
        try {
            inv.reserve("PEN-01", 8);
        } catch (OutOfStockException e) {
            System.out.println(e.getMessage());
            System.out.println("can offer " + e.available() + " of " + e.sku());
        }
    }
}

class ShopException extends RuntimeException {           // base type for the whole app
    ShopException(String message) { super(message); }
    ShopException(String message, Throwable cause) { super(message, cause); }
}

class OutOfStockException extends ShopException {
    private final String sku;
    private final int requested;
    private final int available;

    OutOfStockException(String sku, int requested, int available) {
        super("Not enough stock for " + sku + ": requested " + requested + ", available " + available);
        this.sku = sku;
        this.requested = requested;
        this.available = available;
    }
    String sku() { return sku; }
    int requested() { return requested; }
    int available() { return available; }
}

class Inventory {
    private final Map<String, Integer> stock = new HashMap<>();
    void add(String sku, int qty) { stock.merge(sku, qty, Integer::sum); }
    void reserve(String sku, int qty) {
        int have = stock.getOrDefault(sku, 0);
        if (have < qty) throw new OutOfStockException(sku, qty, have);
        stock.put(sku, have - qty);
    }
}
// Output:
// Not enough stock for PEN-01: requested 8, available 5
// can offer 5 of PEN-01
```

A common design is one application base exception (`ShopException`) with a few meaningful subclasses. A Spring `@ControllerAdvice` (covered in the Spring section) can then map each subclass to an HTTP status: `OutOfStockException` to 409 Conflict, `NotFoundException` to 404.

**Exception chaining.** When a low-level failure causes a higher-level one, wrap it: pass the original as the **cause**. This keeps the full story in the stack trace while giving callers an exception at the right level of abstraction (the order service should not leak `SQLException` to the web layer). This is called **exception translation**.

```java
import java.io.IOException;

public class ChainDemo {
    public static void main(String[] args) {
        try {
            new PaymentService().charge("O-42");
        } catch (PaymentException e) {
            System.out.println(e.getMessage());
            System.out.println("cause: " + e.getCause());
            Throwable root = e;
            while (root.getCause() != null) root = root.getCause();
            System.out.println("root: " + root.getClass().getSimpleName());
        }
    }
}

class PaymentException extends RuntimeException {
    PaymentException(String message, Throwable cause) { super(message, cause); }
}

class PaymentService {
    void charge(String orderId) {
        try {
            callGateway();
        } catch (IOException e) {
            throw new PaymentException("Payment failed for order " + orderId, e);  // keep the cause!
        }
    }
    private void callGateway() throws IOException {
        throw new IOException("gateway timeout after 5000 ms");
    }
}
// Output:
// Payment failed for order O-42
// cause: java.io.IOException: gateway timeout after 5000 ms
// root: IOException
```

**Rethrowing.** Inside a `catch` you can rethrow the same object (`throw e;`), which keeps its original stack trace. Since Java 7 the compiler does **precise rethrow** analysis: if `e` is declared as `Exception` but the `try` body can only throw `IOException`, then `throw e;` only requires `throws IOException`.

> **Pitfall:** `throw new PaymentException("Payment failed: " + e.getMessage());` without passing `e` loses the original stack trace and exception type. Always pass the cause.

> **Tip:** If a library exception has no cause constructor, use `initCause(cause)` once after creating it.

::: deeper
**What `throw` compiles to.** `throw expr` becomes the `athrow` bytecode instruction. The JVM then looks up the current method's **exception table** (a list of `from`, `to`, `target`, `type` rows) for a handler covering the current instruction whose type matches. If none matches, the frame is popped and the search repeats in the caller. This is called **stack unwinding**. If it reaches the top of the thread, the thread's `UncaughtExceptionHandler` runs; the default one prints `Exception in thread "main"` and the trace. Entering a `try` block costs nothing at runtime: the table is only consulted when something is thrown.

**`throws` on overriding methods.** Given `interface Gateway { void pay() throws IOException; }`, an implementation may declare `throws FileNotFoundException` (a subclass), or nothing, but not `throws Exception`. Unchecked exceptions are not restricted.

**Checked exceptions in lambdas.** `list.forEach(x -> Files.readString(x))` does not compile, because `Consumer.accept` declares no checked exceptions. Options: catch inside the lambda and wrap in `UncheckedIOException` (made for exactly this), or write your own functional interface with `throws`. The stream version of this is covered in Core Java 3.

**Serialization warning.** `Throwable` implements `Serializable`, so javac with `-Xlint` warns that a custom exception has no `serialVersionUID`. Add `private static final long serialVersionUID = 1L;` to silence it, and keep fields serializable.

**Version notes.** Java 7 added multi-catch, precise rethrow, try-with-resources and suppressed exceptions. Java 14 to 15 added helpful NPE messages. Nothing significant has changed in exception syntax since; pattern matching (Java 21) makes `if (e.getCause() instanceof SQLException sql)` neat when inspecting causes.

> **Interview:** "Difference between `throw` and `throws`?" `throw` is a statement that raises one exception object inside a method body. `throws` is part of a method signature that declares which exception types may propagate out of it; it is required for checked exceptions.

> **Interview:** "Should a custom exception be checked or unchecked?" Unchecked by default, as most modern frameworks do; checked only when the caller can do something meaningful about it and you want the compiler to force them to consider it. Either way give it a clear name, constructors with message and cause, and fields for the data callers need.
:::

::: check
?? What is lost by `throw new ServiceException(e.getMessage())` inside a `catch`?
The original exception's type and stack trace. Pass it as the cause, `new ServiceException("...", e)`, so the trace shows `Caused by:` with the original.

?? An interface method declares `throws IOException`. Can an implementation declare `throws Exception`?
No. An override may throw the same, narrower or no checked exceptions, never broader ones, because code calling through the interface only prepared for `IOException`.

?? What is exception translation?
Catching a low-level exception and throwing a higher-level one that fits the current layer, with the original as its cause. For example, a repository turning `SQLException` into `DataAccessException`.
:::

::: resources
- Docs: [How to throw exceptions (Java tutorial)](https://docs.oracle.com/javase/tutorial/essential/exceptions/throwing.html) · `throw`, chained exceptions and creating your own
- Docs: [Specifying the exceptions thrown by a method](https://docs.oracle.com/javase/tutorial/essential/exceptions/declaring.html) · the `throws` clause
- Article: [Create a custom exception in Java](https://www.baeldung.com/java-new-custom-exception) · Baeldung on checked and unchecked custom exceptions
- Article: [Chained exceptions in Java](https://www.baeldung.com/java-chained-exceptions) · wrapping causes properly
- Spec: [JVM spec 2.10: Exceptions](https://docs.oracle.com/javase/specs/jvms/se21/html/jvms-2.html#jvms-2.10) · how the JVM finds handlers through the exception table
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 73: throw exceptions appropriate to the abstraction
:::

### Best practices: don't swallow exceptions, use specific exceptions, log properly

::: plain
An exception is a message from the program saying "something broke, here is where and why". Swallowing it (catching it and doing nothing) is like a shop assistant who hears the fire alarm and quietly switches it off: the problem is still there, but now nobody knows. Good exception handling means either fixing the problem where you can, or passing the message on clearly, and writing it down once in the logbook with enough detail for the next person to act on it.
:::

These rules come from real incidents. Each one is a pattern you will spot in code reviews and in support tickets.

**1. Never swallow an exception.** The worst code in any codebase looks like this:

```java
try {
    paymentClient.refund(orderId);
} catch (Exception e) {
    // TODO handle
}
```

The refund failed, the customer is not refunded, and there is no log line to find. If you truly expect and accept a failure, say so in a comment and at least log it at a suitable level. If you catch `InterruptedException`, restore the flag with `Thread.currentThread().interrupt()` (the concurrency section explains why).

**2. Catch specific exceptions, as late as sensible.** Catch where you can **do** something: retry, use a default, translate to a better exception, or return an error response. Catching `Exception` in the middle of business logic also catches `NullPointerException` and `ClassCastException`, turning a bug into a silent wrong result.

**3. Throw specific exceptions, and fail fast.** Check arguments at the start of public methods and throw the standard type that fits. Reusing standard exceptions makes code easier to read:

| Situation | Throw |
|---|---|
| Argument has an invalid value | `IllegalArgumentException` |
| Argument is null where not allowed | `NullPointerException` (via `Objects.requireNonNull`) |
| Object is in the wrong state for this call | `IllegalStateException` |
| Index out of range | `IndexOutOfBoundsException` (via `Objects.checkIndex`) |
| Operation not supported by this implementation | `UnsupportedOperationException` |
| Business rule violated | your own `ShopException` subclass |

**4. Log once, at the boundary, with the exception object.** "Log and rethrow" at every layer produces the same stack trace five times in the log. Either handle it (and log), or rethrow (and do not log). The top-level handler logs it once. When you log, pass the exception as the **last argument** so the logger prints the stack trace:

```java
// SLF4J (the logging API used with Spring Boot)
log.error("Refund failed for order {}", orderId, e);        // good: message + full trace
log.error("Refund failed: " + e);                            // bad: no stack trace, string built even if disabled
log.error("Refund failed for order {}", orderId, e.getMessage()); // bad: trace lost
```

**5. Put context in messages.** "Payment failed" helps nobody. "Payment failed for order O-42, gateway=razorpay, status=504" lets support find the case in seconds. Never put passwords, card numbers or tokens in messages or logs.

**6. Do not use exceptions for normal control flow.** Exceptions are slow to create and make code hard to follow. Check with `map.containsKey` or use `Optional` instead of catching `NoSuchElementException` in a loop.

Here is a small service that applies the rules together:

```java
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

public class Practices {
    private static final Logger LOG = Logger.getLogger("orders");

    public static void main(String[] args) {
        OrderService svc = new OrderService(Map.of("O-1", 499L, "O-2", -10L));
        System.out.println(handle(svc, "O-1"));
        System.out.println(handle(svc, "O-9"));
        System.out.println(handle(svc, "O-2"));
        System.out.println(handle(svc, null));
    }

    // The boundary: the only place that converts exceptions into responses
    static String handle(OrderService svc, String id) {
        try {
            return "200 total=" + svc.total(id);
        } catch (OrderNotFoundException e) {
            return "404 " + e.getMessage();                 // expected: no stack trace needed
        } catch (RuntimeException e) {
            LOG.fine("unexpected failure for " + id);       // real code: log.error(msg, id, e)
            return "500 " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }
}

class OrderNotFoundException extends RuntimeException {
    OrderNotFoundException(String id) { super("order " + id + " not found"); }
}

class OrderService {
    private final Map<String, Long> totals;
    OrderService(Map<String, Long> totals) { this.totals = totals; }

    long total(String id) {
        Objects.requireNonNull(id, "id");                    // fail fast on bad input
        Long t = totals.get(id);
        if (t == null) throw new OrderNotFoundException(id);
        if (t < 0) throw new IllegalStateException("negative total for " + id + ": " + t);
        return t;
    }
}
// Output:
// 200 total=499
// 404 order O-9 not found
// 500 IllegalStateException: negative total for O-2: -10
// 500 NullPointerException: id
```

> **Pitfall:** `catch (Exception e) { throw new RuntimeException(e); }` everywhere is only slightly better than swallowing. It hides the type, so callers cannot react differently to different failures. Translate to a meaningful type instead.

> **Tip:** In a support role you often see only the log. A habit worth adopting from day one: search for the **first** error in a time window, read its root cause, and check whether the same request ID appears earlier. The last error is usually a consequence, not the cause.

::: deeper
**Logging frameworks in one paragraph.** Application code logs through **SLF4J** (an API); the actual work is done by an implementation such as **Logback** (Spring Boot's default) or Log4j 2. `{}` placeholders defer string building until the level is enabled. If the last argument is a `Throwable` with no matching placeholder, SLF4J treats it as the exception and prints the trace. Structured (JSON) logging and a **correlation ID** per request (put into the MDC, the "mapped diagnostic context") make traces searchable in tools like Kibana or Cloud Logging. The DevOps section returns to this.

**Exceptions and transactions.** In Spring, `@Transactional` rolls back by default only for **unchecked** exceptions and `Error`s, not for checked ones. A checked `PaymentException` thrown from a transactional method commits the transaction unless you add `rollbackFor`. This surprises many developers and is one more reason modern Spring code prefers unchecked exceptions.

**The cost of stack traces.** Filling a stack trace walks the stack, and a 200-frame Spring trace is expensive. For expected business exceptions thrown thousands of times per second (validation failures, say), consider returning a result type instead, such as the sealed `PaymentResult` from Core Java 1, which makes failure a normal value the compiler forces callers to handle.

**`Optional` vs exceptions vs null.** Return `Optional` when "not found" is a normal outcome of a query (`findById`), throw when the caller asked for something that must exist (`getById`), and avoid returning `null` from public methods. Core Java 3 covers `Optional` in detail.

> **Interview:** "What are your exception handling best practices?" Catch specific exceptions where you can act on them; never swallow; translate low-level exceptions at layer boundaries and keep the cause; fail fast with standard exceptions such as `IllegalArgumentException`; use try-with-resources; log once, at the boundary, with context and the exception object; and do not use exceptions for normal control flow.

> **Interview:** "Why is `catch (Exception e) {}` dangerous?" It hides every failure, including bugs such as NPEs, so the system continues in a wrong state with no trace of what happened. Problems surface later, far from their cause, and are very hard to debug.
:::

::: check
?? Why should you avoid logging an exception and then rethrowing it?
The same failure appears several times in the log, once per layer, which makes logs noisy and misleading. Either handle and log it, or rethrow it and let the boundary log it once.

?? What is wrong with `log.error("failed: " + e.getMessage())`?
The stack trace and the cause chain are lost, and the string is built even when error logging is off. Pass the exception itself: `log.error("failed for order {}", id, e)`.

?? Which standard exception fits "`cancel()` called on an order that has already shipped"?
`IllegalStateException`: the argument is fine, but the object is in the wrong state for the operation (or a domain-specific subclass of your base exception).
:::

::: resources
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Items 72 (standard exceptions), 75 (detail messages) and 77 (don't ignore exceptions)
- Article: [Common Java exceptions](https://www.baeldung.com/java-common-exceptions) · Baeldung overview of standard exception types
- Docs: [SLF4J manual](https://www.slf4j.org/manual.html) · placeholders and logging exceptions correctly
- Docs: [Objects (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Objects.html) · `requireNonNull` and `checkIndex` for fail-fast checks
- Docs: [Advantages of exceptions (Java tutorial)](https://docs.oracle.com/javase/tutorial/essential/exceptions/advantages.html) · why errors should travel up to the code that can handle them
- Docs: [Unchecked exceptions: the controversy](https://docs.oracle.com/javase/tutorial/essential/exceptions/runtime.html) · Oracle's own guidance on checked vs unchecked
:::

## Generics

Before Java 5, a `List` held plain `Object`s. You could put a `Customer` into a list of products, and you only found out when a cast failed at runtime, often far from the line that caused it. **Generics** let you write `List<Product>`: the compiler then knows the element type, rejects anything else, and inserts the casts for you. Bugs move from production to compile time.

The three topics build on each other: first writing your own generic classes and methods (with bounds that restrict the type), then **wildcards**, which describe "a list of some kind of number" and lead to the famous PECS rule, and finally **type erasure**, the way the compiler implements generics, which explains every strange limitation you will meet.

### Generic classes & methods, bounded types (<T extends Number>)

::: plain
A generic class is a container with a label slot. A box labelled "Shoes" only accepts shoes, and when you take something out you know it is a shoe without checking. You write the box design once, with a blank label (`T`), and fill in the label each time you use it: `Box<Shoe>`, `Box<Book>`. A **bound** restricts what may go on the label: "any kind of number" means the box can rely on number abilities, such as adding up its contents.
:::

**The problem generics solve.** Without them you need casts, and mistakes surface only at runtime:

```java
List raw = new ArrayList();          // raw type: holds Object
raw.add("PEN-01");
raw.add(42);                          // compiles: nothing stops it
String sku = (String) raw.get(1);     // ClassCastException at runtime
```

With `List<String>`, `add(42)` is a compile error and `get` returns a `String` without a cast.

**A generic class** declares **type parameters** in angle brackets after its name. By convention they are single capital letters: `T` (type), `E` (element), `K` and `V` (key and value), `R` (result). Inside the class, `T` is used like any type. Users supply a **type argument**: `Page<Product>`. The **diamond** `<>` (Java 7) lets the compiler infer the argument on the right-hand side.

**A generic method** declares its own type parameters **before the return type**, and the compiler usually infers them from the arguments. Static methods cannot use a class's type parameter (it belongs to instances), so static helpers declare their own.

**Bounded types.** `<T extends Number>` means "`T` must be `Number` or a subclass". Inside, you may call `Number` methods on a `T`. `extends` is used for both classes and interfaces, and a type can have several bounds joined with `&`: `<T extends Number & Comparable<T>>` (the class, if any, must come first).

The example builds from a simple generic class to bounded methods on ShopSphere data:

```java
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class Generics {
    public static void main(String[] args) {
        // 1. Generic class: one design, many element types
        Page<String> skus = new Page<>(List.of("PEN-01", "INK-02"), 1, 5);
        String first = skus.items().get(0);            // no cast needed
        System.out.println(first + " page " + skus.number() + " of " + skus.totalPages());

        // 2. Generic method with inference
        Pair<String, Integer> p = Pair.of("PEN-01", 3);
        System.out.println(p.key() + " x" + p.value());

        // 3. Bounded type: T must be a Number, so doubleValue() is allowed
        System.out.println(sum(List.of(1, 2, 3)));
        System.out.println(sum(List.of(new BigDecimal("19.99"), new BigDecimal("0.01"))));

        // 4. Multiple bounds: T must be comparable to itself
        System.out.println(max(List.of(499, 1299, 89)));
        System.out.println(max(List.of("ink", "pen", "box")));

        // 5. Compile-time safety
        List<Integer> qtys = new ArrayList<>();
        qtys.add(5);
        // qtys.add("five");   // error: incompatible types: String cannot be converted to Integer
        System.out.println(qtys);
    }

    static <T extends Number> double sum(List<T> values) {
        double total = 0;
        for (T v : values) total += v.doubleValue();
        return total;
    }

    static <T extends Comparable<T>> T max(List<T> values) {
        T best = values.get(0);
        for (T v : values) if (v.compareTo(best) > 0) best = v;
        return best;
    }
}

record Page<T>(List<T> items, int number, int totalPages) { }

record Pair<K, V>(K key, V value) {
    static <K, V> Pair<K, V> of(K key, V value) { return new Pair<>(key, value); }
}
// Output:
// PEN-01 page 1 of 5
// PEN-01 x3
// 6.0
// 20.0
// 1299
// pen
// [5]
```

Records, interfaces and enums' methods can all be generic. `Page<T>` is exactly the shape of a paged REST response you will return from Spring controllers later.

**Generic interfaces.** The JDK is full of them: `Comparable<T>`, `Iterable<T>`, `Function<T, R>`, `Repository<T, ID>` in Spring Data. Implementing one fixes or forwards the type parameter: `class Money implements Comparable<Money>` or `class InMemoryRepo<T> implements Repository<T, Long>`.

**Explicit type arguments.** When inference cannot work it out, write the type before the method name: `Collections.<String>emptyList()`. You rarely need this in modern Java.

**Primitive types are not allowed** as type arguments: `List<int>` does not compile. Use the wrapper (`List<Integer>`) and accept autoboxing, or use a primitive-specialised API such as `IntStream` or an `int[]` when performance matters.

> **Pitfall:** Raw types (`List` without `<...>`) still compile, for backward compatibility, but they switch off generic checks for that expression. javac warns "unchecked call to add(E) as a member of the raw type List". Treat those warnings as errors in new code.

> **Tip:** `<T extends Comparable<? super T>>` is the bound you see in `Collections.sort` and `Collections.max`. The `? super T` part, explained in the next topic, lets it work for a class whose parent implements `Comparable`, such as `java.sql.Timestamp` (which extends `java.util.Date`, which implements `Comparable<Date>`).

::: deeper
**Generic arrays.** `new T[10]` does not compile ("generic array creation") because at runtime `T` is unknown (see type erasure), and an array must know its element type. `ArrayList` works around this by storing `Object[] elementData` and casting on the way out. If you need a typed array, pass a `Class<T>` or an `IntFunction<T[]>`, which is why `list.toArray(String[]::new)` exists.

**Invariance.** `List<Integer>` is **not** a subtype of `List<Number>`, even though `Integer` is a subtype of `Number`. If it were, you could add a `Double` to a list of integers through the `List<Number>` view. Arrays made the opposite choice (they are **covariant**: `Integer[]` is an `Object[]`), and pay for it with a runtime check, `ArrayStoreException`. Wildcards (next topic) give generics controlled flexibility instead.

**Recursive bounds.** `Enum<E extends Enum<E>>` and the builder pattern `abstract class Builder<T extends Builder<T>>` use a type parameter bounded by itself, so methods in the parent can return the exact subclass type (`self()`), enabling fluent chains.

**Type inference improvements.** Java 7 added the diamond; Java 8 added target typing, so `List<String> l = Collections.emptyList();` infers from the assignment target; Java 9 allowed the diamond with anonymous classes; Java 10 added `var`. `var list = new ArrayList<>();` infers `ArrayList<Object>`, which is rarely what you want, so give the type on one side.

**Primitive generics.** Project Valhalla aims to let generics work over value types and eventually primitives (`List<int>`) without boxing. As of Java 25 this is still in development (value classes are a preview-candidate JEP), so for now prefer primitive streams and arrays in hot numeric code.

> **Interview:** "Why use generics?" Type safety at compile time (wrong types are rejected before the program runs), no explicit casts, and reusable algorithms and containers written once for many types. They do this with no runtime cost, because they are implemented by erasure.

> **Interview:** "What does `<T extends Comparable<T>>` mean?" `T` can be any type whose instances can be compared with other instances of `T`, so the method may call `compareTo` on its values. It is a bounded type parameter.
:::

::: check
?? Why can a static method not use the class's type parameter `T`?
`T` is fixed per instance (`Box<String>` vs `Box<Integer>`), but a static method belongs to the class and has no instance. It must declare its own type parameter: `static <T> Box<T> of(T value)`.

?? Is `List<Integer>` a subtype of `List<Number>`?
No. Generics are invariant. Otherwise you could add a `Double` to a list of integers through the `List<Number>` reference. Use `List<? extends Number>` to accept both.

?? What does `<T extends Number & Comparable<T>>` require?
`T` must extend `Number` **and** implement `Comparable<T>`. With several bounds, a class bound must come first and the rest must be interfaces.
:::

::: resources
- Docs: [Generics (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/generics/index.html) · Oracle's complete trail on generics
- Docs: [Generics on dev.java](https://dev.java/learn/generics/intro/) · modern introduction with type parameters and bounds
- Article: [The basics of Java generics](https://www.baeldung.com/java-generics) · Baeldung overview with bounded types
- Docs: [Java Generics FAQ (Angelika Langer)](http://www.angelikalanger.com/GenericsFAQ/JavaGenericsFAQ.html) · the most complete reference on corner cases
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Items 26 to 33 on generics
- Book: *Java Generics and Collections* by Maurice Naftalin and Philip Wadler · the classic book on both topics
:::

### Wildcards: ?, ? extends, ? super (PECS rule)

::: plain
Suppose you are packing for a trip. A friend says "give me a bag with **some kind of fruit** in it": you can take fruit **out** of any such bag, but you cannot put an apple in, because the bag might be the banana bag. Another friend says "give me a bag that can hold **apples or anything more general**" (an apple bag, a fruit bag, a food bag): you can safely put apples **in**, but when you take something out you only know it is "some food". That is the whole idea of `? extends` (read from it) and `? super` (write into it).
:::

A **wildcard** `?` stands for "some unknown type". It appears in variable and parameter types, never when creating an object (`new ArrayList<?>()` does not compile). There are three forms:

| Form | Reads as | You can get out | You can put in | Typical use |
|---|---|---|---|---|
| `List<?>` | list of something | `Object` | only `null` | size, clear, print any list |
| `List<? extends Number>` | list of Number or a subtype | `Number` | only `null` | **producer**: you read numbers from it |
| `List<? super Integer>` | list of Integer or a supertype | `Object` | `Integer` (and subtypes) | **consumer**: you write integers into it |

**Why `? extends` is read-only.** A `List<? extends Number>` might really be a `List<Double>`. The compiler cannot let you add an `Integer` to it, so it forbids adding anything except `null`. Reading is safe: whatever it is, each element is some `Number`.

**Why `? super` is write-only (for useful types).** A `List<? super Integer>` might be a `List<Integer>`, `List<Number>` or `List<Object>`. Adding an `Integer` is safe in all three. Reading gives only `Object`, since that is all the compiler can promise.

**PECS: Producer Extends, Consumer Super.** Joshua Bloch's rule for choosing: if a parameter **produces** values your method reads, use `? extends T`; if it **consumes** values your method writes, use `? super T`; if it does both, use plain `T`. The JDK's `Collections.copy` is the textbook case:

```java
public static <T> void copy(List<? super T> dest, List<? extends T> src)
```

`src` produces `T`s, `dest` consumes them.

```java
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public class Wildcards {
    public static void main(String[] args) {
        List<Integer> qtys = List.of(3, 5, 2);
        List<Double> weights = List.of(1.5, 0.25);

        // Producer: accepts List<Integer>, List<Double>, List<Number>...
        System.out.println(total(qtys));
        System.out.println(total(weights));

        // Consumer: accepts List<Integer>, List<Number>, List<Object>
        List<Number> numbers = new ArrayList<>();
        List<Object> anything = new ArrayList<>();
        addDefaults(numbers);
        addDefaults(anything);
        System.out.println(numbers + " " + anything);

        // Both at once: copy from a producer into a consumer
        List<Object> audit = new ArrayList<>();
        copyIf(qtys, audit, q -> q > 2);
        System.out.println(audit);

        // Unbounded: works for any list, but only through Object
        printAll(List.of("PEN-01", "INK-02"));
        printAll(qtys);
    }

    static double total(Collection<? extends Number> values) {   // P: extends
        double sum = 0;
        for (Number n : values) sum += n.doubleValue();
        // values.add(1);   // error: int cannot be converted to CAP#1
        return sum;
    }

    static void addDefaults(List<? super Integer> sink) {          // C: super
        sink.add(1);
        sink.add(10);
        Object o = sink.get(0);                                     // reads give Object only
    }

    static <T> void copyIf(Collection<? extends T> src, Collection<? super T> dst,
                           Predicate<? super T> keep) {
        for (T item : src) if (keep.test(item)) dst.add(item);
    }

    static void printAll(List<?> list) {
        for (Object o : list) System.out.print(o + " ");
        System.out.println("(" + list.size() + ")");
    }
}
// Output:
// 10.0
// 1.75
// [1, 10] [1, 10]
// [3, 5]
// PEN-01 INK-02 (2)
// 3 5 2 (3)
```

Notice that `Predicate<? super T>` is also a consumer: the predicate **consumes** `T` values in `test`. That is why a `Predicate<Object>` (say, "not null") can filter a list of integers. The same rule explains signatures all over the JDK: `Comparator<? super T>` in `sort`, `Function<? super T, ? extends R>` in `Stream.map` (it consumes a `T` and produces an `R`).

**`List<?>` vs `List<Object>`.** `List<Object>` is a specific type: a list that accepts any object. A `List<String>` is **not** a `List<Object>` (invariance), but it **is** a `List<?>`. Use `List<?>` for "a list of anything, I only need `Object` methods or `size()`".

**Wildcard vs type parameter.** `static void printAll(List<?> list)` and `static <T> void printAll(List<T> list)` accept the same arguments. The Effective Java guideline: if a type parameter appears only once in a method signature, replace it with a wildcard. Use a type parameter when you need to tie two things together (the return type to the argument, or two arguments to each other).

> **Pitfall:** Do not use wildcards in **return types**. Returning `List<? extends Product>` forces every caller to deal with the wildcard. Wildcards belong on parameters, where they make your API accept more.

> **Tip:** If you are unsure, write the method with plain `List<T>`, then ask of each parameter: do I only read from it (`extends`), only write to it (`super`), or both (leave it)?

::: deeper
**Capture conversion.** When the compiler sees a `List<?>`, it invents a fresh unnamed type for the `?`, shown in error messages as `CAP#1`. That is why `list.add(list.get(0))` fails on a `List<?>`: each use of the wildcard gets its own captured type, so the compiler cannot prove the element you read fits the list you write to. The standard fix is a private **helper method** with a type parameter, which "captures" the wildcard: `private static <T> void swapHelper(List<T> l, int i, int j)`. `Collections.swap(List<?> list, ...)` is implemented with a raw-type cast for the same reason.

**Why `Comparator<? super T>`.** `Collections.sort(List<T> list, Comparator<? super T> c)` lets you sort a `List<ExpressDelivery>` with a `Comparator<Delivery>` written for the parent class. Without `super`, you would need a separate comparator for each subclass. Likewise `<T extends Comparable<? super T>>` in `Collections.max` allows a type whose `compareTo` is inherited from a parent.

**Wildcards and arrays.** Arrays are covariant and checked at runtime:

```java
Object[] objs = new Integer[2];
objs[0] = "x";                 // compiles, throws ArrayStoreException at runtime
```

Generics with wildcards make the same mistake a compile error instead, which is why generic collections are preferred over arrays in APIs.

**Kotlin and C# comparison.** Java uses **use-site variance** (you write `? extends` where the type is used). Kotlin and C# also have **declaration-site variance** (`out T`, `in T` on the class itself). You may see Kotlin code in Spring projects, so it helps to know that `out` means "producer" and `in` means "consumer".

> **Interview:** "Explain PECS." Producer extends, consumer super. If a generic parameter supplies values your method reads, declare it `? extends T`, so callers can pass collections of subtypes. If it receives values your method writes, declare it `? super T`, so callers can pass collections of supertypes. If it does both, use `T` exactly. Example: `Collections.copy(List<? super T> dest, List<? extends T> src)`.

> **Interview:** "Why can't you add to a `List<? extends Number>`?" Because the actual list might be a `List<Double>` or a `List<Integer>`; the compiler cannot know which, so any addition (except `null`) could break the list's real type.
:::

::: check
?? A method only reads `Product`s from a list. Should its parameter be `List<Product>`, `List<? extends Product>` or `List<? super Product>`?
`List<? extends Product>`: it is a producer. Then callers can pass `List<Product>` and also `List<DigitalProduct>`.

?? What can you add to a `List<? super Integer>`, and what type do you get from `get`?
You can add `Integer` values (and `null`). `get` returns `Object`, because the list might be a `List<Object>`.

?? Why is `List<String>` assignable to `List<?>` but not to `List<Object>`?
Generics are invariant, so `List<String>` is not a `List<Object>` (you could otherwise add an `Integer` to it). `List<?>` means "list of some unknown type" and forbids adding, so it is safe as a supertype of every list.
:::

::: resources
- Docs: [Wildcards (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/generics/wildcards.html) · upper bounded, lower bounded and unbounded wildcards
- Docs: [Guidelines for wildcard use](https://docs.oracle.com/javase/tutorial/java/generics/wildcardGuidelines.html) · Oracle's "in" and "out" variable rules (PECS)
- Docs: [Wildcard capture and helper methods](https://docs.oracle.com/javase/tutorial/java/generics/capture.html) · the `CAP#1` error explained
- Article: [Type parameter vs wildcard in Java generics](https://www.baeldung.com/java-generics-type-parameter-vs-wildcard) · Baeldung on when to use each
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 31: use bounded wildcards to increase API flexibility
- Docs: [Collections.copy (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Collections.html#copy(java.util.List,java.util.List)) · PECS in a real JDK signature
:::

### Type erasure and its limitations

::: plain
Generics are a promise checked at the factory gate, not inside the factory. The compiler reads all your `List<Product>` labels, checks every use, adds the casts, and then **removes the labels** before writing the class file. At runtime there is only a plain `List`. That makes generics free at runtime and compatible with old code, but it also means the running program cannot ask "is this a `List<Product>` or a `List<Customer>`?": the label is gone.
:::

**What erasure does.** When compiling generic code, javac:

1. Replaces each type parameter with its **bound**, or with `Object` if it has none. `T` becomes `Object`; `T extends Number` becomes `Number`; with multiple bounds, the first one.
2. Inserts **casts** where a generic value is used as its specific type.
3. Generates **bridge methods** where needed to keep overriding working (see Go deeper).

So this source:

```java
List<String> skus = new ArrayList<>();
skus.add("PEN-01");
String s = skus.get(0);
```

compiles to bytecode equivalent to:

```java
List skus = new ArrayList();
skus.add("PEN-01");
String s = (String) skus.get(0);   // cast inserted by the compiler
```

Generics were added this way in Java 5 so that new generic code and old pre-generic libraries could run on the same JVM and call each other (**migration compatibility**).

**The limitations that follow.** Because type arguments do not exist at runtime:

| You cannot | Why | Workaround |
|---|---|---|
| `new T()` | the JVM does not know what `T` is | pass a `Supplier<T>` or `Class<T>` |
| `new T[n]` or `new List<String>[n]` | arrays need their element type at runtime | `Object[]` plus casts, `IntFunction<T[]>`, or use a `List` |
| `x instanceof List<String>` | the type argument is gone | `instanceof List<?>`, then check elements |
| overload `f(List<String>)` and `f(List<Integer>)` | both erase to `f(List)` ("name clash: ... have the same erasure") | use different method names |
| `List<int>` | type parameters erase to `Object`, which a primitive is not | wrappers, or primitive streams and arrays |
| a static field of type `T` | one static field is shared by all `Box<X>` | none; make it an instance field |
| catch a `T extends Exception`, or make a generic exception class | the `catch` must know the type at runtime | catch the bound type |

```java
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class Erasure {
    public static void main(String[] args) {
        List<String> skus = new ArrayList<>();
        List<Integer> qtys = new ArrayList<>();
        System.out.println(skus.getClass() == qtys.getClass());   // one runtime class
        System.out.println(skus.getClass().getName());

        Object o = skus;
        System.out.println(o instanceof List<?>);                  // allowed: unbounded

        // Workaround for new T(): pass a factory
        Basket basket = create(Basket::new);
        System.out.println(basket.getClass().getSimpleName());

        // Workaround for runtime type checks: carry a Class<T> token
        TypedBox<Integer> box = new TypedBox<>(Integer.class);
        System.out.println(box.accepts(5) + " " + box.accepts("five"));

        // Heap pollution: a raw type sneaks the wrong element in
        List<Integer> ids = new ArrayList<>();
        sneakIn(ids);                                               // raw type adds a String
        System.out.println(ids.size());
        try {
            Integer first = ids.get(0);                             // inserted cast fails here
        } catch (ClassCastException e) {
            System.out.println("ClassCastException far from the real bug");
        }
    }

    static <T> T create(Supplier<T> factory) { return factory.get(); }

    @SuppressWarnings({"rawtypes", "unchecked"})        // hides javac's "unchecked call" warning
    static void sneakIn(List raw) { raw.add("oops"); }
}

class Basket { }

class TypedBox<T> {
    private final Class<T> type;
    TypedBox(Class<T> type) { this.type = type; }
    boolean accepts(Object value) { return type.isInstance(value); }
}
// Output:
// true
// java.util.ArrayList
// true
// Basket
// true false
// 1
// ClassCastException far from the real bug
```

The last part shows **heap pollution**: a variable of a generic type refers to an object holding the wrong type. The error appears at the `get`, where the compiler inserted the cast, not at the raw `add` that caused it. That is exactly why raw types and ignored "unchecked" warnings are dangerous.

**Some generic information does survive.** Erasure removes type arguments from **objects** and **local variables**, but the **declarations** of classes, fields and methods keep their generic signatures in the class file (the `Signature` attribute). Reflection can read them: `Field.getGenericType()` on a `List<Product> items` field returns `java.util.List<Product>`. Frameworks rely on this: Jackson reads a field's declared type to deserialise JSON into `List<Product>`, and Spring uses it to inject a `Repository<Order>` bean into the right place. The **super type token** trick, used by Jackson's `TypeReference` and Spring's `ParameterizedTypeReference`, captures a type through an anonymous subclass: `new TypeReference<List<Product>>() {}`. The anonymous class's generic superclass is recorded, so the library can read `List<Product>` at runtime.

> **Pitfall:** `@SuppressWarnings("unchecked")` should be placed on the smallest possible scope (a single variable declaration), with a comment explaining why the cast is safe. Suppressing it on a whole class hides real problems.

> **Tip:** A `ClassCastException` on a line with no visible cast almost always means heap pollution: some code earlier used a raw type or an unchecked cast. Search for raw types and `@SuppressWarnings("unchecked")` on the path that built the collection.

::: deeper
**Bridge methods.** Consider `class Money implements Comparable<Money> { public int compareTo(Money o) {...} }`. After erasure, the interface method is `compareTo(Object)`, but the class only has `compareTo(Money)`. javac adds a synthetic **bridge method** `compareTo(Object o) { return compareTo((Money) o); }` so that calls through `Comparable` still dispatch to your method. `javap -c Money.class` shows it, marked `ACC_BRIDGE, ACC_SYNTHETIC`. Bridges also implement covariant return types.

**Erasure in bytecode.** For `static <T extends Comparable<T>> T max(List<T> list)`, `javap -s` shows the erased descriptor `(Ljava/util/List;)Ljava/lang/Comparable;`: the bound replaced `T`. A call site assigning the result to `Integer` gets a `checkcast java/lang/Integer` instruction right after the `invokestatic`.

**Varargs and generics.** A generic varargs parameter `T... args` creates a `T[]`, which is really an `Object[]` (or the bound's array) at runtime, so it can cause heap pollution. javac warns "possible heap pollution from parameterized vararg type". When the method only reads the array and never exposes it, annotate it with `@SafeVarargs` (on static, final or private methods), as `List.of(E...)` and `Arrays.asList(T...)` do.

**Reifiable types.** A type whose full information is available at runtime is called **reifiable**: primitives, non-generic types, raw types and unbounded wildcard types such as `List<?>`. Only reifiable types may be used in `instanceof`, array creation and `catch`. Java 16 relaxed `instanceof` slightly: `if (obj instanceof List<String> l)` is allowed when the compiler can prove it is safe, for example when `obj` is already declared as `Collection<String>`.

**Reified generics elsewhere.** C# keeps type arguments at runtime (`List<int>` is a distinct runtime type), and Kotlin offers `reified` type parameters in inline functions. Java chose erasure for compatibility; Project Valhalla is working towards specialised generics for value types without breaking that compatibility.

> **Interview:** "What is type erasure?" The compiler checks generic types, then removes type arguments, replacing type parameters with their bounds (or `Object`) and inserting casts where needed. At runtime `List<String>` and `List<Integer>` are both just `ArrayList`. It keeps compatibility with pre-Java 5 code and costs nothing at runtime, but prevents `new T()`, generic array creation, `instanceof` with type arguments and overloading on type arguments.

> **Interview:** "If generics are erased, how does Jackson deserialise into `List<Product>`?" Generic signatures of fields, methods and superclasses are kept in the class file and are readable by reflection. Jackson either reads the declared type of the target field, or you pass a `TypeReference<List<Product>>` anonymous subclass, whose generic superclass records the full type.
:::

::: check
?? What does `T` become after erasure in `class Box<T extends Comparable<T>>`?
`Comparable`, the erasure of its first bound. Unbounded type parameters become `Object`.

?? Why do `void f(List<String> l)` and `void f(List<Integer> l)` not compile together?
Both erase to `f(List)`, so they would have the same signature in the class file. javac reports a name clash because they have the same erasure.

?? What is heap pollution, and where does the error usually surface?
A variable of a parameterised type refers to an object that holds elements of the wrong type, usually through a raw type or unchecked cast. The error surfaces later as a `ClassCastException` at a compiler-inserted cast, far from the line that caused it.
:::

::: resources
- Docs: [Type erasure (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/generics/erasure.html) · how erasure works, with bridge methods
- Docs: [Restrictions on generics](https://docs.oracle.com/javase/tutorial/java/generics/restrictions.html) · the official list of what you cannot do
- Article: [Type erasure in Java explained](https://www.baeldung.com/java-type-erasure) · Baeldung with bytecode examples
- Spec: [JLS 4.6: Type erasure](https://docs.oracle.com/javase/specs/jls/se21/html/jls-4.html#jls-4.6) · the formal definition and reifiable types
- Docs: [Non-reifiable types and heap pollution](https://docs.oracle.com/javase/tutorial/java/generics/nonReifiableVarargsType.html) · varargs warnings and `@SafeVarargs`
- Article: [Jackson: unmarshalling a JSON array](https://www.baeldung.com/jackson-collection-array) · Baeldung on deserialising generic collections
:::

## Collections Framework

A **collection** is an object that holds other objects. The **Java Collections Framework** (in `java.util`, since Java 1.2) is a set of **interfaces** that describe what a collection can do (`List`, `Set`, `Queue`, `Map`), **implementations** that decide how it is stored (`ArrayList`, `HashMap`, `TreeSet`...), and **algorithms** in utility classes (`Collections.sort`, `Arrays.asList`). Code against the interface, choose the implementation for its performance: `List<OrderLine> lines = new ArrayList<>();`.

This is the largest group in the section and the most interview-heavy. It starts with the map of the whole framework, then goes through lists, sets and maps with their internals (`HashMap` gets a full topic of its own), queues, ordering with `Comparable` and `Comparator`, iteration and the fail-fast rule, the utility classes with immutable collections, and finally the sequenced collections added in Java 21.

[[fig:corecoll_coll]]

### Collection hierarchy: List, Set, Queue, Deque, Map

::: plain
Think of the different ways a shop stores things. A **list** is a numbered receipt: order matters and the same item can appear twice. A **set** is the guest list for a launch event: each name appears once, and you mainly ask "is this person on it?". A **queue** is the line at the till: first come, first served. A **deque** (say "deck") is a line you can join or leave at both ends. A **map** is the price board: each product code points to exactly one price.
:::

**The interfaces.** Everything except maps descends from `Iterable` (anything you can loop over with for-each) and `Collection` (a group of elements with `add`, `remove`, `contains`, `size`, `isEmpty`, `iterator`, `stream`).

| Interface | Ordered? | Duplicates? | Key idea | Main implementations |
|---|---|---|---|---|
| `List` | yes, by index | yes | positional access `get(i)` | `ArrayList`, `LinkedList` |
| `Set` | depends | no | uniqueness by `equals`/`hashCode` (or `compareTo`) | `HashSet`, `LinkedHashSet`, `TreeSet` |
| `SortedSet` / `NavigableSet` | sorted | no | ranges, `first`, `floor`, `ceiling` | `TreeSet` |
| `Queue` | FIFO or priority | yes | `offer`, `poll`, `peek` | `ArrayDeque`, `PriorityQueue`, `LinkedList` |
| `Deque` | both ends | yes | stack and queue in one | `ArrayDeque`, `LinkedList` |
| `Map` | depends | keys unique | key to value lookup | `HashMap`, `LinkedHashMap`, `TreeMap` |
| `SortedMap` / `NavigableMap` | sorted by key | keys unique | key ranges | `TreeMap` |

**`Map` is not a `Collection`.** A map stores pairs, so `add(E)` makes no sense for it. Instead it offers three **views** that are collections: `keySet()` (a `Set<K>`), `values()` (a `Collection<V>`) and `entrySet()` (a `Set<Map.Entry<K, V>>`). Views are live: removing a key from `keySet()` removes the entry from the map.

**Java 21 added sequenced interfaces** (`SequencedCollection`, `SequencedSet`, `SequencedMap`) for collections with a defined encounter order; they get their own topic at the end of this group.

**Choosing a collection** is mostly answering three questions: do I need lookup by key (map), uniqueness (set) or position (list)? Do I need order (insertion order, sorted order, or none)? Do I need to add and remove at the ends (deque) or by priority (priority queue)?

```java
import java.util.*;

public class Hierarchy {
    public static void main(String[] args) {
        List<String> basket = new ArrayList<>(List.of("pen", "ink", "pen"));
        Set<String> unique = new HashSet<>(basket);
        Set<String> sorted = new TreeSet<>(basket);
        Deque<String> tills = new ArrayDeque<>();
        Map<String, Integer> stock = new LinkedHashMap<>();

        tills.offer("alice");                          // join the back of the queue
        tills.offer("bob");
        stock.put("pen", 10);
        stock.put("ink", 4);

        System.out.println(basket + " size=" + basket.size());
        System.out.println(unique.size() + " unique, contains ink? " + unique.contains("ink"));
        System.out.println(sorted);
        System.out.println("served " + tills.poll() + ", waiting " + tills);
        System.out.println(stock.keySet() + " " + stock.values());

        stock.keySet().remove("ink");                   // views write through to the map
        System.out.println(stock);

        // Code against interfaces: the same method works for any Collection
        System.out.println(countLong(basket) + " " + countLong(sorted) + " " + countLong(tills));
    }

    static long countLong(Collection<String> c) {
        return c.stream().filter(s -> s.length() > 2).count();
    }
}
// Output:
// [pen, ink, pen] size=3
// 2 unique, contains ink? true
// [ink, pen]
// served alice, waiting [bob]
// [pen, ink] [10, 4]
// {pen=10}
// 3 2 1
```

**Optional operations.** Some `Collection` methods are documented as "optional": an implementation may throw `UnsupportedOperationException` instead. Immutable collections (`List.of`), fixed-size views (`Arrays.asList`) and unmodifiable views (`Collections.unmodifiableList`) all do this for methods that would change them. The type system cannot tell you; the documentation does.

**The legacy classes.** `Vector`, `Stack` and `Hashtable` date from Java 1.0. Every method is `synchronized`, which costs performance and still does not make compound actions ("check then put") safe. Use `ArrayList`, `ArrayDeque` and `HashMap` instead, and the concurrent collections (`ConcurrentHashMap`, `CopyOnWriteArrayList`) when threads share data; the concurrency section covers those.

> **Pitfall:** Declaring fields and parameters as implementation types (`ArrayList<Order> orders`) ties your code to one implementation. Declare `List<Order>`, and choose `ArrayList` only at the `new`.

> **Tip:** Interviewers often ask for the time complexity of common operations. A one-line summary: array-based lists are O(1) by index, hash-based sets and maps are O(1) average lookup, tree-based ones are O(log n) and sorted, linked structures are O(1) at the ends and O(n) in the middle.

::: deeper
**Skeleton classes.** The JDK provides `AbstractCollection`, `AbstractList`, `AbstractSet`, `AbstractMap` and `AbstractQueue`. To write your own read-only list you only implement `get(int)` and `size()`; `AbstractList` builds `iterator`, `contains`, `indexOf`, `equals`, `hashCode` and `subList` from them. `Arrays.asList` and `Collections.nCopies` are built this way.

**`RandomAccess` marker.** `ArrayList` implements the empty marker interface `RandomAccess`, `LinkedList` does not. Generic algorithms such as `Collections.binarySearch` and `Collections.shuffle` check `list instanceof RandomAccess` and choose an index-based or an iterator-based loop accordingly.

**Equality across implementations.** `List.equals` is defined by the interface: two lists are equal if they contain equal elements in the same order, whatever their class. So `new ArrayList<>(List.of(1, 2)).equals(new LinkedList<>(List.of(1, 2)))` is `true`. Likewise for sets (same elements) and maps (same entries). That is why `hashCode` for lists, sets and maps is also specified exactly in their interfaces.

**`Collection` defaults added over time.** Java 8: `removeIf`, `stream`, `forEach` (from `Iterable`), `Map.getOrDefault`, `putIfAbsent`, `computeIfAbsent`, `merge`. Java 9: `List.of`, `Set.of`, `Map.of`. Java 10: `copyOf`. Java 11: `toArray(IntFunction)`. Java 21: sequenced methods. Each topic below uses the relevant ones.

> **Interview:** "Why does `Map` not extend `Collection`?" A `Collection` is a group of single elements with `add(E)`. A map is a set of key-value pairs with lookup by key; forcing it into `Collection` would need awkward methods such as `add(Entry)`. Instead `Map` offers collection views: `keySet()`, `values()` and `entrySet()`.

> **Interview:** "Difference between `Collection` and `Collections`?" `Collection` is the root interface for lists, sets and queues. `Collections` is a utility class of static methods (`sort`, `unmodifiableList`, `synchronizedMap`, `emptyList`...).
:::

::: check
?? You need to store customer emails for a newsletter with no duplicates and fast "is this email subscribed?" checks. Which interface and implementation?
`Set<String>` with `HashSet` (or `LinkedHashSet` if sign-up order matters). Average O(1) `contains`, and duplicates are rejected automatically.

?? Which interfaces does `ArrayDeque` implement, and what can it act as?
`Deque`, and so `Queue`, `Collection` and `Iterable` (plus `SequencedCollection` since Java 21). It works as a FIFO queue and as a LIFO stack.

?? Why are `Vector` and `Hashtable` avoided in new code?
They synchronize every method, which is slow when not needed and insufficient for compound operations when it is. `ArrayList` and `HashMap` are the single-threaded choices; `ConcurrentHashMap` and friends are the thread-safe ones.
:::

::: resources
- Docs: [Collections framework overview](https://docs.oracle.com/en/java/javase/21/core/java-collections-framework.html) · Oracle core libraries guide for Java 21
- Docs: [The Collections tutorial trail](https://docs.oracle.com/javase/tutorial/collections/index.html) · interfaces, implementations and algorithms
- Docs: [Collections on dev.java](https://dev.java/learn/api/collections-framework/) · the modern official tutorial
- Docs: [Collection (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Collection.html) · the root interface and optional operations
- Docs: [Collection interfaces (Java tutorial)](https://docs.oracle.com/javase/tutorial/collections/interfaces/index.html) · a tour of every core interface
- Book: *Java Generics and Collections* by Maurice Naftalin and Philip Wadler · design of the framework explained
:::

### ArrayList vs LinkedList (internals & Big-O)

::: plain
An `ArrayList` is a row of numbered lockers: to reach locker 500 you walk straight to it, but to insert a new locker at the front, everyone has to move one place along. A `LinkedList` is a treasure hunt: each clue tells you where the next one is, so adding a clue in the middle is easy once you are standing there, but reaching clue 500 means following 499 clues first. In real programs the lockers win almost every time.
:::

[[fig:corecoll_lists]]

**`ArrayList` inside.** It holds an `Object[] elementData` and an `int size`. The array is usually bigger than `size`; the spare slots are the **capacity** headroom.

- `new ArrayList<>()` starts with a shared empty array; the first `add` allocates **10** slots.
- When the array is full, `add` **grows** it to about **1.5 times** the old capacity (`old + (old >> 1)`) and copies the elements with `Arrays.copyOf` (a fast native memory copy).
- Because growth is geometric, the average cost of `add` at the end is O(1): this is called **amortised** O(1).
- `add(i, x)` and `remove(i)` shift the following elements with `System.arraycopy`: O(n - i).
- Removing elements never shrinks the array; call `trimToSize()` if that matters.

**`LinkedList` inside.** A **doubly linked list**: each element is wrapped in a `Node` object with `item`, `next` and `prev` fields, and the list keeps `first`, `last` and `size`.

- Adding or removing at either end is O(1): just relink nodes.
- `get(i)` walks from whichever end is closer: O(n).
- `add(i, x)` must first **find** position `i` (O(n)), then relinks in O(1). Only when you already hold a `ListIterator` at the position is insertion truly O(1).
- `LinkedList` also implements `Deque`, so it works as a queue or stack, but `ArrayDeque` does that faster.

| Operation | `ArrayList` | `LinkedList` |
|---|---|---|
| `get(i)`, `set(i, x)` | **O(1)** | O(n) |
| `add(x)` at end | O(1) amortised | O(1) |
| `add(0, x)` / `remove(0)` | O(n) (shift all) | **O(1)** |
| `add(i, x)` / `remove(i)` in middle | O(n) shift | O(n) to find + O(1) relink |
| Insert or remove at an iterator's position | O(n) | **O(1)** |
| `contains`, `indexOf` | O(n) | O(n) |
| Memory per element | 4 or 8 bytes (one reference) | about 24 bytes for the `Node` plus the reference |
| Cache friendliness | excellent (contiguous) | poor (nodes scattered across the heap) |

**Why `ArrayList` wins in practice.** Big-O hides constants. CPUs read memory in 64-byte cache lines and prefetch sequential addresses, so scanning an array is extremely fast, while walking a linked list causes a cache miss at almost every node. `System.arraycopy` shifts thousands of references in microseconds. Benchmarks show `ArrayList` beating `LinkedList` even for many middle insertions, and `ArrayDeque` beating it as a queue. Joshua Bloch, who wrote `LinkedList`, has joked that he never uses it.

```java
import java.util.*;

public class Lists {
    public static void main(String[] args) {
        List<String> lines = new ArrayList<>(List.of("pen", "ink", "box"));
        lines.add(1, "tape");                 // shifts ink and box right
        lines.remove("box");                  // remove(Object): first equal element
        System.out.println(lines + " get(2)=" + lines.get(2));

        // The classic List<Integer> trap: remove(int index) vs remove(Object)
        List<Integer> qtys = new ArrayList<>(List.of(10, 20, 30));
        qtys.remove(1);                       // removes INDEX 1 (the 20)
        qtys.remove(Integer.valueOf(10));     // removes the VALUE 10
        System.out.println(qtys);

        // LinkedList: cheap at both ends, and it is also a Deque
        LinkedList<String> recent = new LinkedList<>();
        recent.addFirst("order O-1");
        recent.addFirst("order O-2");
        recent.addLast("order O-0");
        System.out.println(recent.getFirst() + " ... " + recent.getLast());

        // O(1) insertion only when you already stand at the position
        ListIterator<String> it = recent.listIterator();
        while (it.hasNext()) {
            if (it.next().equals("order O-1")) it.add("order O-1b");
        }
        System.out.println(recent);

        // Pre-size when you know the count: avoids repeated growth and copying
        List<Integer> ids = new ArrayList<>(10_000);
        for (int i = 0; i < 10_000; i++) ids.add(i);
        System.out.println(ids.size() + " last=" + ids.get(ids.size() - 1));

        // Lists of different classes are equal if elements and order match
        System.out.println(new ArrayList<>(List.of(1, 2)).equals(new LinkedList<>(List.of(1, 2))));
    }
}
// Output:
// [pen, tape, ink] get(2)=ink
// [30]
// order O-2 ... order O-0
// [order O-2, order O-1, order O-1b, order O-0]
// 10000 last=9999
// true
```

> **Pitfall:** Looping `for (int i = 0; i < list.size(); i++) list.get(i)` over a `LinkedList` is O(n squared): every `get` walks the list again. Use for-each (which uses the iterator) for any list.

> **Tip:** When you know roughly how many elements are coming (rows from a query with a known count, say), use `new ArrayList<>(expectedSize)`. For removing many elements by condition, `list.removeIf(predicate)` is O(n) for an `ArrayList`, while calling `remove(i)` in a loop is O(n squared).

::: deeper
**Growth in the JDK source.** Since Java 17 the grow logic lives in `ArraysSupport.newLength(oldLength, minGrowth, prefGrowth)`, called with a preferred growth of `oldCapacity >> 1`. So capacities go 10, 15, 22, 33, 49, 73... The maximum is a little under `Integer.MAX_VALUE` (`SOFT_MAX_ARRAY_LENGTH` is `Integer.MAX_VALUE - 8`, because some JVMs reserve header words in arrays). `ensureCapacity(n)` grows in one step when you know a large batch is coming.

**Memory numbers.** On a 64-bit HotSpot JVM with compressed references (the default for heaps under about 32 GB) a reference is 4 bytes and an object header 12 bytes. A `LinkedList.Node` is 12 + 3 × 4 = 24 bytes, so a million-element `LinkedList` spends about 24 MB on nodes alone, against about 4 MB of references in an `ArrayList`'s array (plus up to 50% unused capacity). Both still hold the element objects themselves, and for `List<Integer>` each element is a 16-byte `Integer` too: for large numeric data, a plain `int[]` is far smaller.

**`modCount`.** Both classes inherit an `int modCount` field from `AbstractList` that counts structural changes (adds and removes, not `set`). Iterators remember it and throw `ConcurrentModificationException` when it changes under them: the fail-fast topic later explains this.

**`subList` is a view.** `list.subList(2, 5).clear()` removes those three elements from the original list in one O(n) shift, a neat idiom. But structurally modifying the original list afterwards makes the sublist throw `ConcurrentModificationException` on next use.

**`Arrays.asList` is not an `ArrayList`** (well, it is `java.util.Arrays$ArrayList`, a different private class). It is a fixed-size view over the array: `set` works and writes through to the array, `add` and `remove` throw `UnsupportedOperationException`.

> **Interview:** "ArrayList vs LinkedList: when would you use each?" `ArrayList` is a resizable array: O(1) random access, amortised O(1) appends, O(n) inserts and removes in the middle, compact and cache-friendly. `LinkedList` is a doubly linked list: O(1) at the ends and at an iterator's position, O(n) random access, about six times more memory per element. Use `ArrayList` by default; for a queue or stack use `ArrayDeque`; `LinkedList` is rarely the best choice.

> **Interview:** "How does `ArrayList` grow?" When adding to a full backing array it allocates a new one about 1.5 times larger and copies the elements over with `Arrays.copyOf`. The default first allocation is 10 elements, made lazily on the first `add`. Geometric growth makes appends amortised O(1).
:::

::: check
?? Why is `get(i)` O(1) for `ArrayList` but O(n) for `LinkedList`?
`ArrayList` computes the slot's address directly from the index in its backing array. `LinkedList` has no index: it must follow `next` (or `prev`) links from the nearest end, one node at a time.

?? On a `List<Integer>` holding `[5, 6, 7]`, what does `list.remove(1)` remove?
The element at index 1, which is `6`. To remove the value 1 you must pass an object: `list.remove(Integer.valueOf(1))`.

?? Is inserting into the middle of a `LinkedList` really O(1)?
Only if you are already at the position, through a `ListIterator`. `add(index, x)` must first walk to the index, which is O(n).
:::

::: resources
- Docs: [ArrayList (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/ArrayList.html) · capacity, amortised cost and fail-fast behaviour
- Docs: [LinkedList (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/LinkedList.html) · doubly linked list implementing `List` and `Deque`
- Repo: [ArrayList.java source (OpenJDK)](https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/java/util/ArrayList.java) · read `grow()` and `elementData` yourself
- Article: [ArrayList vs LinkedList](https://www.baeldung.com/java-arraylist-linkedlist) · Baeldung comparison with benchmarks
- Docs: [List implementations (Java tutorial)](https://docs.oracle.com/javase/tutorial/collections/implementations/list.html) · Oracle's advice on choosing
- Book: *Java Generics and Collections* by Maurice Naftalin and Philip Wadler · chapters on List implementations and their costs
:::

### HashSet, LinkedHashSet, TreeSet

::: plain
All three are guest lists that never let the same guest in twice. A `HashSet` is a bouncer with a huge pigeonhole cabinet: checking a name is instant, but the names come out in no meaningful order. A `LinkedHashSet` is the same cabinet plus a sign-in sheet, so it also remembers the order people arrived. A `TreeSet` keeps the names alphabetised at all times, which costs a little more on every check, but lets you ask "who comes just after Meera?".
:::

**How a set decides "duplicate".** `HashSet` and `LinkedHashSet` use `hashCode` to find the bucket and `equals` to confirm, exactly the contract from Core Java 1. `TreeSet` ignores both and uses `compareTo` (or a `Comparator`): two elements are duplicates when the comparison returns 0. `add` returns `false` when the element was already present.

**`HashSet` inside** is just a `HashMap` in disguise: each element is stored as a **key**, with a shared dummy object as the value (the field is literally called `PRESENT`). So everything in the `HashMap` internals topic (buckets, load factor, treeification) applies to `HashSet`. One `null` element is allowed. Iteration order is the internal bucket order: stable for a given set, but unrelated to insertion order and liable to change when the set resizes.

**`LinkedHashSet`** extends `HashSet` and is backed by a `LinkedHashMap`, which threads a doubly linked list through all entries. Iteration follows **insertion order**; re-adding an existing element does not move it. Cost: two extra references per entry, and still O(1) operations.

**`TreeSet`** is backed by a `TreeMap`, a **red-black tree** (a self-balancing binary search tree). All basic operations are O(log n), iteration is in **sorted order**, and it implements `NavigableSet`, which gives range queries:

| Method | Returns |
|---|---|
| `first()`, `last()` | smallest, largest |
| `floor(x)`, `ceiling(x)` | greatest element `<= x`, smallest `>= x` (or `null`) |
| `lower(x)`, `higher(x)` | strictly `< x`, strictly `> x` |
| `headSet(x)`, `tailSet(x)`, `subSet(a, b)` | live views of a range |
| `pollFirst()`, `pollLast()` | remove and return the ends |
| `descendingSet()` | reversed view |

`TreeSet` does not allow `null` with natural ordering (`compareTo` on `null` throws `NullPointerException`).

| | `HashSet` | `LinkedHashSet` | `TreeSet` |
|---|---|---|---|
| Backed by | `HashMap` | `LinkedHashMap` | `TreeMap` (red-black tree) |
| `add`, `remove`, `contains` | O(1) average | O(1) average | O(log n) |
| Iteration order | none (bucket order) | insertion | sorted |
| Uniqueness by | `hashCode` + `equals` | `hashCode` + `equals` | `compareTo` / `Comparator` |
| `null` allowed | one | one | no (natural ordering) |
| Extra abilities | | stable order | ranges, floor, ceiling, first, last |

```java
import java.util.*;

public class Sets {
    public static void main(String[] args) {
        List<String> signups = List.of("meera@x.in", "arjun@x.in", "zoya@x.in", "arjun@x.in", "bala@x.in");

        Set<String> hash = new HashSet<>(signups);
        Set<String> linked = new LinkedHashSet<>(signups);
        TreeSet<String> tree = new TreeSet<>(signups);
        System.out.println(hash.size() + " unique");
        System.out.println(linked);                      // sign-up order
        System.out.println(tree);                        // sorted order
        System.out.println(linked.add("zoya@x.in"));     // already there

        // NavigableSet range queries on prices
        TreeSet<Integer> prices = new TreeSet<>(List.of(99, 249, 499, 999, 1999));
        System.out.println(prices.floor(500) + " " + prices.ceiling(500) + " " + prices.higher(999));
        System.out.println(prices.headSet(499) + " " + prices.subSet(200, true, 1000, true));
        System.out.println(prices.descendingSet());

        // A TreeSet uses the comparator, not equals: case-insensitive uniqueness
        Set<String> tags = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tags.addAll(List.of("Sale", "sale", "NEW", "new", "Eco"));
        System.out.println(tags);

        // Set algebra with bulk operations
        Set<String> boughtPen = new HashSet<>(Set.of("arjun", "meera", "zoya"));
        Set<String> boughtInk = Set.of("meera", "bala");
        Set<String> both = new TreeSet<>(boughtPen);
        both.retainAll(boughtInk);                       // intersection
        Set<String> penOnly = new TreeSet<>(boughtPen);
        penOnly.removeAll(boughtInk);                    // difference
        System.out.println(both + " " + penOnly);
    }
}
// Output:
// 4 unique
// [meera@x.in, arjun@x.in, zoya@x.in, bala@x.in]
// [arjun@x.in, bala@x.in, meera@x.in, zoya@x.in]
// false
// 499 999 1999
// [99, 249] [249, 499, 999]
// [1999, 999, 499, 249, 99]
// [Eco, NEW, Sale]
// [meera] [arjun, zoya]
```

The case-insensitive `TreeSet` kept the **first** spelling it saw of each tag ("Sale", "NEW"), because later ones compared equal and were rejected.

> **Pitfall:** Never print or rely on a `HashSet`'s iteration order in tests or output. It depends on hash codes and capacity and can change between Java versions. If order matters, use `LinkedHashSet` or `TreeSet`.

> **Pitfall:** Changing a field used by `hashCode` (or by `compareTo`, for a `TreeSet`) while the element is in the set corrupts it: `contains` and `remove` look in the wrong place. Use immutable elements, such as records or `String`s.

::: deeper
**`HashSet` source in three lines.** `private transient HashMap<E,Object> map;`, `private static final Object PRESENT = new Object();`, and `add(e)` is `return map.put(e, PRESENT) == null;`. The memory cost per element is therefore a full `HashMap.Node` (about 32 bytes) plus the table slot, which surprises people who expect a set to be cheaper than a map.

**Red-black tree in brief.** Each node is coloured red or black, with rules (no two reds in a row, the same number of black nodes on every root-to-leaf path) that keep the longest path at most twice the shortest. So the height stays O(log n) and inserts rebalance with at most a few rotations. You do not need to code one in interviews, but you should know `TreeMap`, `TreeSet` and `HashMap`'s treeified buckets all use it.

**Consistent with equals.** `TreeSet` documentation warns that the ordering "must be consistent with equals" for the set to obey the `Set` contract. `new BigDecimal("1.0")` and `new BigDecimal("1.00")` are not `equals` (different scale) but `compareTo` returns 0: a `HashSet` keeps both, a `TreeSet` keeps one. Our case-insensitive `tags` set breaks the contract on purpose, which is fine as long as you know it.

**`EnumSet`.** For sets of enum constants use `EnumSet` (`EnumSet.of(OrderStatus.PAID, OrderStatus.SHIPPED)`). It is a **bit vector**: one `long` holds up to 64 constants, so `contains` is a bit test. It is far faster and smaller than a `HashSet` of enums, and iterates in declaration order.

**Choosing.** Default to `HashSet`. Use `LinkedHashSet` for deduplication that keeps order (a common interview task: "remove duplicates from a list while keeping order" is `new ArrayList<>(new LinkedHashSet<>(list))`). Use `TreeSet` when you need sorted output or range queries, for example "next available delivery slot after 14:00".

> **Interview:** "How does `HashSet` ensure uniqueness?" It stores each element as a key in an internal `HashMap` with a dummy value. `add` calls `map.put(e, PRESENT)`; the map uses `hashCode` to pick a bucket and `equals` to detect an existing key, and `put` returns the old value, so `add` returns `false` for a duplicate.

> **Interview:** "HashSet vs TreeSet?" `HashSet` is O(1) average, unordered, allows one `null`, and uses `hashCode`/`equals`. `TreeSet` is a red-black tree, O(log n), sorted, supports range queries such as `floor` and `ceiling`, rejects `null` under natural ordering, and uses `compareTo` or a `Comparator` to decide duplicates.
:::

::: check
?? How would you remove duplicates from a `List<String>` while keeping the original order?
`new ArrayList<>(new LinkedHashSet<>(list))`, or `list.stream().distinct().toList()`. Both keep the first occurrence of each element in encounter order.

?? A `TreeSet<Product>` is created without a comparator, and `Product` does not implement `Comparable`. What happens?
The first `add` throws `ClassCastException`, because the `TreeMap` tries to cast the key to `Comparable`. Provide a `Comparator` or implement `Comparable`.

?? Which set gives "the cheapest price strictly greater than 500"?
A `TreeSet<Integer>` with `higher(500)`; `ceiling(500)` would also accept 500 itself.
:::

::: resources
- Docs: [HashSet (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/HashSet.html) · backed by a `HashMap`, performance notes
- Docs: [TreeSet (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/TreeSet.html) · `NavigableSet` methods and consistency with equals
- Docs: [LinkedHashSet (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/LinkedHashSet.html) · insertion order and sequenced methods
- Article: [A guide to HashSet in Java](https://www.baeldung.com/java-hashset) · Baeldung on internals and usage
- Article: [A guide to TreeSet in Java](https://www.baeldung.com/java-tree-set) · Baeldung on navigation methods
- Docs: [Set implementations (Java tutorial)](https://docs.oracle.com/javase/tutorial/collections/implementations/set.html) · Oracle's comparison of the three
:::

### HashMap internals: hashing, buckets, collisions, treeification (Java 8+), load factor, resizing

::: plain
Picture a cloakroom with numbered hooks. When you hand in your coat, the attendant turns your ticket name into a hook number with a quick formula and hangs the coat there. To get it back she applies the same formula, goes straight to that hook, and checks the tickets hanging there until she finds yours. Sometimes two people's formulas land on the same hook (a **collision**), so several coats share it. When the room gets about three-quarters full, she moves to a room with twice as many hooks and rehangs everything, so hooks never get crowded. That is a `HashMap`.
:::

`HashMap` is the most used data structure in Java and the most asked-about in interviews. This topic follows one `put` and one `get` all the way through, then covers what goes wrong.

[[fig:corecoll_hashmap]]

**The structure.** A `HashMap` holds an array called `table`, whose slots are **buckets**. Each non-empty bucket holds a chain of `Node` objects; each node stores four fields: `hash` (the spread hash of the key, cached), `key`, `value` and `next` (the following node in the same bucket). The length of `table` is always a **power of two**: 16 by default.

**Step 1: hash the key.** `put(key, value)` calls `key.hashCode()` and **spreads** it:

```java
static final int hash(Object key) {          // from java.util.HashMap
    int h;
    return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
}
```

XOR-ing the high 16 bits into the low 16 bits matters because the next step only uses the low bits. Without spreading, keys whose hash codes differ only in their high bits (common with some `Float` or poorly written hash codes) would all collide. A `null` key gets hash 0, so it lives in bucket 0; `HashMap` allows exactly one `null` key and any number of `null` values.

**Step 2: pick the bucket.** `index = (table.length - 1) & hash`. With a power-of-two length, `n - 1` is a mask of ones (15 is `1111` in binary), so the `&` keeps the low bits: the same result as `hash % n` for non-negative hashes, but a single fast instruction. This is why capacity is always a power of two.

**Step 3: walk the bucket.** If the bucket is empty, the new node goes straight in. Otherwise the map walks the chain and, for each node, checks `node.hash == hash && (node.key == key || key.equals(node.key))`. Comparing the cached `int` hash first is cheap and avoids most `equals` calls. If a matching key is found, its **value is replaced** and `put` returns the old value. If not, a new node is appended to the **tail** of the chain and `put` returns `null`.

**Step 4: resize if needed.** After adding, if `size > threshold`, the table is resized. `threshold = capacity × loadFactor`, and the default **load factor is 0.75**, so a default map resizes when the 13th entry arrives (16 × 0.75 = 12). Resizing **doubles** the capacity and moves every node to the new table.

**`get(key)`** repeats steps 1 to 3 without changing anything: hash, mask, walk the bucket, compare hash then `equals`, return the value or `null`.

**Collisions and treeification (Java 8).** Two keys collide when they land in the same bucket, either because their hash codes are equal (`"Aa"` and `"BB"` both hash to 2112) or because their low bits match. A long chain makes lookups O(n). Since Java 8, when one bucket reaches **8** nodes (`TREEIFY_THRESHOLD`), the chain is converted into a **red-black tree** of `TreeNode`s, making the worst case O(log n). Two conditions:

- The table must have at least **64** buckets (`MIN_TREEIFY_CAPACITY`). If it is smaller, the map **resizes** instead, since a crowded bucket in a small table usually just means the table is too small.
- When a tree shrinks to **6** nodes or fewer (`UNTREEIFY_THRESHOLD`, typically during a resize split), it turns back into a list. The gap between 8 and 6 prevents flip-flopping.

With a decent `hashCode`, chains of 8 are extremely rare: the JDK source notes that with random hashes and load factor 0.75 the probability of a bucket holding 8 entries is about 0.00000006. Treeification is a defence against bad hash codes and deliberate attacks (crafted keys that all collide, a known denial-of-service technique against web servers).

| Constant | Value | Meaning |
|---|---|---|
| `DEFAULT_INITIAL_CAPACITY` | 16 | table length on first `put` |
| `DEFAULT_LOAD_FACTOR` | 0.75 | resize when size exceeds capacity × 0.75 |
| `TREEIFY_THRESHOLD` | 8 | chain length that triggers a tree |
| `UNTREEIFY_THRESHOLD` | 6 | tree size that turns back into a list |
| `MIN_TREEIFY_CAPACITY` | 64 | below this, resize instead of treeifying |
| `MAXIMUM_CAPACITY` | 2^30 | largest table length |

Here is the arithmetic done by hand, plus the behaviour you can observe from outside:

```java
import java.util.HashMap;
import java.util.Map;

public class HashMapInside {
    static int spread(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    static int bucket(Object key, int capacity) {
        return (capacity - 1) & spread(key);
    }

    public static void main(String[] args) {
        for (String k : new String[] {"PEN-01", "INK-02", "Aa", "BB"}) {
            System.out.println(k + " hashCode=" + k.hashCode() + " bucket(16)=" + bucket(k, 16)
                    + " bucket(32)=" + bucket(k, 32));
        }

        Map<String, Integer> stock = new HashMap<>();
        System.out.println(stock.put("PEN-01", 10));     // new key: returns null
        System.out.println(stock.put("PEN-01", 7));      // existing key: returns old value
        stock.put(null, 0);                               // one null key is allowed
        stock.put("Aa", 1);
        stock.put("BB", 2);                               // collides with "Aa", both kept
        System.out.println(stock.get("Aa") + " " + stock.get("BB") + " " + stock.get(null));
        System.out.println(stock.size());

        // A terrible hashCode still works (everything collides), just slower
        Map<BadKey, String> bad = new HashMap<>();
        for (int i = 0; i < 100; i++) bad.put(new BadKey(i), "v" + i);
        System.out.println(bad.get(new BadKey(42)) + " size=" + bad.size());
    }
}

record BadKey(int id) {
    @Override public int hashCode() { return 7; }         // every key in one bucket
}
// Output:
// PEN-01 hashCode=-1938543787 bucket(16)=1 bucket(32)=1
// INK-02 hashCode=-2130725527 bucket(16)=6 bucket(32)=22
// Aa hashCode=2112 bucket(16)=0 bucket(32)=0
// BB hashCode=2112 bucket(16)=0 bucket(32)=0
// null
// 10
// 1 2 0
// 4
// v42 size=100
```

Look at the resize columns. When the table doubles from 16 to 32, the mask gains one bit, so each entry either stays at the **same index** (`PEN-01`: 1 stays 1) or moves to **index + old capacity** (`INK-02`: 6 becomes 22). Java 8's resize uses exactly this: it splits each bucket into a "low" list and a "high" list by testing `hash & oldCapacity`, preserving node order, without recomputing any hash.

**The mutable key bug.** The map stores a key in the bucket chosen by its hash **at insertion time**. If you change a field the key's `hashCode` depends on, the entry is stranded:

```java
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MutableKey {
    public static void main(String[] args) {
        Map<Sku, Integer> stock = new HashMap<>();
        Sku sku = new Sku("PEN-01");
        stock.put(sku, 10);
        sku.code = "PEN-02";                              // mutate the key after insertion
        System.out.println(stock.get(sku));               // looks in the wrong bucket
        System.out.println(stock.get(new Sku("PEN-01"))); // right bucket, but equals fails now
        System.out.println(stock.size() + " " + stock.containsValue(10));
    }
}

class Sku {
    String code;
    Sku(String code) { this.code = code; }
    @Override public boolean equals(Object o) { return o instanceof Sku s && s.code.equals(code); }
    @Override public int hashCode() { return Objects.hash(code); }
}
// Output:
// null
// null
// 1 true
```

The entry is still there (size 1), but no key can reach it: a memory leak and a correctness bug at once. This is why keys should be immutable (`String`, `Integer`, records with immutable fields, enums).

**Useful Java 8+ methods** that do the lookup once instead of `get` then `put`:

```java
counts.merge(sku, 1, Integer::sum);                              // word-count style counting
ordersByCustomer.computeIfAbsent(customerId, k -> new ArrayList<>()).add(order);  // multimap
stock.getOrDefault("NIB-09", 0);
stock.putIfAbsent("NIB-09", 0);
```

> **Pitfall:** `HashMap` is not thread-safe. Concurrent `put`s can lose entries or corrupt the structure (before Java 8, a concurrent resize could even create a cycle in a chain and make `get` loop forever, because resizing reversed chains by inserting at the head). Use `ConcurrentHashMap` when threads share a map.

> **Tip:** If you know you will store about 1,000 entries, create the map with room for them: `HashMap.newHashMap(1000)` (Java 19+) computes the right capacity for you. The older `new HashMap<>(1000)` sets the **capacity**, not the number of entries, so it still resizes when the 769th entry arrives (1,024 × 0.75 = 768).

::: deeper
**Lazy allocation and `tableSizeFor`.** `new HashMap<>()` allocates no table: the first `put` calls `resize()`, which creates the 16-slot array. A requested initial capacity is rounded up to the next power of two by `tableSizeFor` (bit tricks with `-1 >>> Integer.numberOfLeadingZeros(cap - 1)`), so `new HashMap<>(100)` has 128 buckets and resizes after 96 entries. `newHashMap(n)` computes `ceil(n / 0.75)` first.

**Ordering inside a treeified bucket.** A red-black tree needs an ordering. `TreeNode` orders by hash first; for equal hashes, if the keys' class implements `Comparable` (checked with `comparableClassFor`), it uses `compareTo`; otherwise it falls back to `tieBreakOrder`, comparing class names and then `System.identityHashCode`. For lookups with equal hashes and non-comparable keys, the tree may have to search both subtrees, so implementing `Comparable` on key classes helps in the worst case. `TreeNode`s are about twice the size of normal nodes, another reason treeification is a rare fallback.

**Why 0.75?** It is a trade-off between memory and time. A higher load factor packs the table tighter (less memory) but lengthens chains; a lower one wastes slots. With 0.75 and a good hash function, the expected chain length is well under one node, and bucket sizes follow a Poisson distribution with a mean of about 0.5. You can pass another load factor to the constructor, but there is rarely a reason to.

**Java 7 vs Java 8.** Java 7 inserted new nodes at the **head** of a chain and used a more complex supplemental hash function, with no trees. Java 8 rewrote the class: tail insertion, the simple `h ^ (h >>> 16)` spread, treeification, and the order-preserving resize split. Java 8 also dropped the alternative `String` hashing that Java 7u6 had added against collision attacks, because trees solve the same problem for every key type.

**`HashMap` vs `Hashtable` vs `ConcurrentHashMap`.** `Hashtable` synchronizes every method and rejects `null` keys and values. `ConcurrentHashMap` locks per bucket (it uses CAS for empty buckets and `synchronized` on the first node otherwise), allows concurrent reads without locking, also rejects `null`s (so `get` returning `null` always means "absent"), and has atomic `compute` and `merge`. Its iterators are weakly consistent instead of fail-fast. The concurrency section covers it fully.

**Iteration cost.** Iterating a `HashMap` visits every bucket, so it is O(capacity + size). A map that once held a million entries and was then cleared still iterates over a large empty table: `HashMap` never shrinks. Create a new map instead.

> **Interview:** "How does `HashMap` work internally?" It keeps an array of buckets whose length is a power of two. `put` computes the key's `hashCode`, spreads it with `h ^ (h >>> 16)`, and picks the bucket with `hash & (n - 1)`. In the bucket it compares the cached hash and then `equals` for each node: a match replaces the value, otherwise a new node is appended. When size exceeds capacity × load factor (0.75 by default) it doubles the table and redistributes nodes, each staying at its index or moving to index + old capacity. Since Java 8, a bucket with 8 or more nodes in a table of at least 64 becomes a red-black tree, so the worst case is O(log n) instead of O(n). `get` follows the same path.

> **Interview:** "What happens if two keys have the same hash code?" They go to the same bucket and are stored in the same chain (or tree). Lookups still work, because the map confirms each candidate with `equals`. Only performance suffers.

> **Interview:** "Why should `HashMap` keys be immutable?" The bucket is chosen from the key's hash when the entry is inserted. If a field used in `hashCode` changes later, lookups compute a different bucket and the entry becomes unreachable, though it is still in the map.
:::

::: check
?? A default `HashMap` has 16 buckets. After how many entries does it first resize, and to what size?
When the 13th entry is added (size exceeds 16 × 0.75 = 12), it doubles to 32 buckets.

?? Why must the table length be a power of two?
So the bucket index can be computed as `hash & (n - 1)`, a cheap mask, and so resizing can split each bucket into "stay" and "move by old capacity" groups by testing a single bit.

?? When does a bucket become a tree, and why does `HashMap` sometimes resize instead?
When a chain reaches 8 nodes and the table has at least 64 buckets. In a smaller table, crowding usually means the table is too small, so doubling it is the better fix.
:::

::: resources
- Docs: [HashMap (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/HashMap.html) · load factor, capacity and `newHashMap`
- Repo: [HashMap.java source (OpenJDK)](https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/java/util/HashMap.java) · the implementation notes at the top are excellent reading
- Spec: [JEP 180: Handle frequent HashMap collisions with balanced trees](https://openjdk.org/jeps/180) · why treeification was added in Java 8
- Article: [A guide to Java HashMap](https://www.baeldung.com/java-hashmap) · Baeldung on usage and internals
- Article: [Java HashMap load factor](https://www.baeldung.com/java-hashmap-load-factor) · Baeldung on capacity, threshold and rehashing
- Docs: [Map (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Map.html) · `merge`, `compute` and `getOrDefault` contracts
:::

### LinkedHashMap (LRU cache), TreeMap, EnumMap, WeakHashMap, IdentityHashMap

::: plain
These are specialist versions of the price board. A `LinkedHashMap` remembers the order you wrote entries, or the order you last looked at them, which is exactly what you need to throw out the least recently used item from a full cache. A `TreeMap` keeps keys sorted, like an index at the back of a book. An `EnumMap` is a board with one fixed slot per enum value. A `WeakHashMap` lets the garbage collector wipe an entry once nobody else uses its key. An `IdentityHashMap` treats two keys as the same only if they are literally the same object.
:::

**`LinkedHashMap`** extends `HashMap` and adds a doubly linked list through all entries (`before` and `after` pointers). It has two modes:

- **Insertion order** (default): iteration follows the order keys were first inserted. Re-putting an existing key does not move it.
- **Access order** (`new LinkedHashMap<>(16, 0.75f, true)`): every `get` or `put` moves the entry to the end, so the first entry is always the **least recently used**.

It also has a hook method, `protected boolean removeEldestEntry(Map.Entry<K,V> eldest)`, called after each `put`. Return `true` and the eldest entry is removed. Access order plus this hook is a complete **LRU cache** (least recently used) in a few lines, a very common interview task.

**`TreeMap`** is a red-black tree sorted by key (natural ordering or a `Comparator`), with O(log n) operations and the `NavigableMap` methods: `firstKey`, `lastKey`, `floorKey`, `ceilingEntry`, `headMap`, `tailMap`, `subMap`, `descendingMap`, `pollFirstEntry`. It is ideal for ranges: price bands, time slots, versioned configuration.

**`EnumMap`** is for keys of one enum type. Internally it is just an array indexed by `ordinal()`, so it is very fast and compact, iterates in declaration order, and rejects `null` keys.

**`WeakHashMap`** holds its keys through **weak references**: if the only remaining reference to a key is the map itself, the garbage collector may reclaim the key, and the entry disappears. It is used for caches of metadata attached to objects owned elsewhere. It compares keys with `equals`, so it is useless with keys such as interned strings or small `Integer`s that are always strongly reachable.

**`IdentityHashMap`** compares keys with `==` and `System.identityHashCode`, deliberately breaking the `Map` contract. Uses: tracking visited objects during graph traversal, serialization, deep copying, proxies. Inside it uses **open addressing** (linear probing) in a single array holding keys and values alternately, with no node objects.

| Map | Order | Key comparison | `get` cost | Null keys | Typical use |
|---|---|---|---|---|---|
| `HashMap` | none | `hashCode` + `equals` | O(1) | one | default choice |
| `LinkedHashMap` | insertion or access | `hashCode` + `equals` | O(1) | one | predictable order, LRU caches |
| `TreeMap` | sorted by key | `compareTo` / `Comparator` | O(log n) | no (natural order) | ranges, sorted reports |
| `EnumMap` | enum declaration | ordinal | O(1) | no | per-status counters, config |
| `WeakHashMap` | none | `hashCode` + `equals` | O(1) | one | caches tied to key lifetime |
| `IdentityHashMap` | none | `==` | O(1) | one | object graphs, visited sets |

```java
import java.util.*;

public class SpecialMaps {
    enum Status { NEW, PAID, SHIPPED, DELIVERED }

    // LRU cache: access order + removeEldestEntry
    static class LruCache<K, V> extends LinkedHashMap<K, V> {
        private final int maxEntries;
        LruCache(int maxEntries) {
            super(16, 0.75f, true);                       // true = access order
            this.maxEntries = maxEntries;
        }
        @Override protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > maxEntries;
        }
    }

    public static void main(String[] args) {
        LruCache<String, String> cache = new LruCache<>(3);
        cache.put("P1", "Pen");
        cache.put("P2", "Ink");
        cache.put("P3", "Box");
        cache.get("P1");                                   // P1 becomes most recent
        cache.put("P4", "Tape");                           // evicts P2, the least recent
        System.out.println(cache.keySet());

        // TreeMap: delivery fee by order value band
        TreeMap<Integer, Integer> feeFrom = new TreeMap<>(Map.of(0, 99, 500, 49, 1000, 0));
        for (int total : new int[] {120, 500, 2400}) {
            System.out.println(total + " -> fee " + feeFrom.floorEntry(total).getValue());
        }
        System.out.println(feeFrom.headMap(1000) + " " + feeFrom.descendingKeySet());

        // EnumMap: counters per status, iterated in declaration order
        Map<Status, Integer> counts = new EnumMap<>(Status.class);
        for (Status s : List.of(Status.PAID, Status.NEW, Status.PAID, Status.DELIVERED)) {
            counts.merge(s, 1, Integer::sum);
        }
        System.out.println(counts);

        // IdentityHashMap: equal but distinct keys stay separate
        String a = new String("SKU");
        String b = new String("SKU");
        Map<String, Integer> byEquals = new HashMap<>();
        Map<String, Integer> byIdentity = new IdentityHashMap<>();
        byEquals.put(a, 1); byEquals.put(b, 2);
        byIdentity.put(a, 1); byIdentity.put(b, 2);
        System.out.println(byEquals.size() + " " + byIdentity.size());
    }
}
// Output:
// [P3, P1, P4]
// 120 -> fee 99
// 500 -> fee 49
// 2400 -> fee 0
// {0=99, 500=49} [1000, 500, 0]
// {NEW=1, PAID=2, DELIVERED=1}
// 1 2
```

> **Pitfall:** In an access-ordered `LinkedHashMap`, even `get` is a **structural modification** (it moves the entry). Calling `get` while iterating over the map throws `ConcurrentModificationException`, and the map is not safe for concurrent readers either. For a thread-safe cache use `Collections.synchronizedMap` around it, or a library such as Caffeine.

> **Tip:** Spring and Hibernate use `LinkedHashMap` widely to keep configuration in declaration order. When you write JSON with Jackson from a `LinkedHashMap`, field order is preserved; from a `HashMap` it is not.

::: deeper
**How `LinkedHashMap` hooks into `HashMap`.** `HashMap` defines three empty callbacks, `afterNodeAccess`, `afterNodeInsertion` and `afterNodeRemoval`, and creates nodes through overridable `newNode` methods. `LinkedHashMap` overrides them: `newNode` creates an `Entry` with `before`/`after` links and appends it to the list; `afterNodeAccess` moves a node to the tail in access-order mode; `afterNodeInsertion` calls `removeEldestEntry` and removes the head if it returns `true`. Iteration walks the linked list, so it is O(size), unlike `HashMap`'s O(capacity + size).

**Java 21 additions.** `LinkedHashMap` now implements `SequencedMap`: `firstEntry()`, `lastEntry()`, `pollFirstEntry()`, `putFirst`, `putLast` and `reversed()`. `putLast` on an existing key moves it to the end even in insertion-order mode. `TreeMap` gained these through `SortedMap`, except `putFirst`/`putLast`, which throw `UnsupportedOperationException` because the position is decided by the key's order.

**`WeakHashMap` internals.** Each entry extends `WeakReference<Object>` and is registered with a `ReferenceQueue`. When the GC clears a key, the entry is enqueued, and the map purges such "stale" entries (`expungeStaleEntries`) at the start of most operations. So `size()` may drop between two calls without any code touching the map. A classic leak: a `WeakHashMap` whose **values** strongly reference their own keys, which keeps the keys alive forever. For caches with time or size limits, a library (Caffeine) is far better.

**Reference types in brief.** Strong (normal), soft (cleared only under memory pressure, used for memory-sensitive caches), weak (cleared at the next GC once only weakly reachable), phantom (for cleanup actions, used by `Cleaner`). `ThreadLocal` also uses weak keys internally.

**`TreeMap` vs `ConcurrentSkipListMap`.** For a sorted map shared between threads, `ConcurrentSkipListMap` gives the same `NavigableMap` API with lock-free concurrency, based on a skip list rather than a tree.

> **Interview:** "Implement an LRU cache." Extend `LinkedHashMap` with `accessOrder = true` in the constructor and override `removeEldestEntry` to return `size() > capacity`. `get` and `put` are O(1): the hash table finds the entry, the linked list keeps recency order, and the eldest (head) entry is evicted after a `put` exceeds the limit. If asked to build it from scratch, use a `HashMap` from key to node plus your own doubly linked list with head and tail sentinels.

> **Interview:** "When would you use `TreeMap` over `HashMap`?" When you need keys in sorted order or range queries such as "the highest key not above x" (`floorKey`), at the cost of O(log n) operations and needing comparable keys.
:::

::: check
?? Which constructor argument turns a `LinkedHashMap` into access order, and what does that change?
The third argument, `accessOrder = true`: `new LinkedHashMap<>(16, 0.75f, true)`. Every `get` or `put` of a key moves its entry to the end, so iteration runs from least to most recently used.

?? Why is `EnumMap` faster than a `HashMap` with enum keys?
It stores values in an array indexed by each constant's `ordinal()`, so there is no hashing, no buckets and no node objects. It also iterates in declaration order.

?? You put two `new String("SKU")` keys into an `IdentityHashMap`. What is its size?
2. `IdentityHashMap` compares keys with `==`, and the two strings are different objects even though they are `equals`.
:::

::: resources
- Docs: [LinkedHashMap (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/LinkedHashMap.html) · access order and `removeEldestEntry`
- Docs: [TreeMap (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/TreeMap.html) · `NavigableMap` methods
- Docs: [EnumMap (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/EnumMap.html) · compact array-based map for enum keys
- Docs: [WeakHashMap (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/WeakHashMap.html) · weak keys and their caveats
- Article: [How to implement an LRU cache in Java](https://www.baeldung.com/java-lru-cache) · Baeldung, from scratch and with `LinkedHashMap`
- Article: [A guide to TreeMap in Java](https://www.baeldung.com/java-treemap) · Baeldung on sorted maps and navigation
- Docs: [IdentityHashMap (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/IdentityHashMap.html) · reference-equality map and its uses
:::

### PriorityQueue (heap), ArrayDeque (stack/queue)

::: plain
A `PriorityQueue` is a hospital emergency room: patients are not seen in arrival order, the most urgent one always goes next. An `ArrayDeque` is a train platform where people can board or leave at either end: use one end only and it behaves like a stack of plates (last in, first out); join at the back and leave at the front and it is a fair queue (first in, first out).
:::

**The `Queue` and `Deque` methods** come in pairs: one throws an exception on failure, one returns a special value. Prefer the second form when failure is normal.

| Action | Throws on failure | Returns special value |
|---|---|---|
| Insert at tail | `add(e)` | `offer(e)` (returns `false`) |
| Remove from head | `remove()` | `poll()` (returns `null`) |
| Look at head | `element()` | `peek()` (returns `null`) |

`Deque` adds `First` and `Last` versions of each (`offerFirst`, `pollLast`, `peekLast`...) and the stack methods `push` (= `addFirst`), `pop` (= `removeFirst`) and `peek`.

**`ArrayDeque`** is a **circular array**: a resizable array with `head` and `tail` indexes that wrap around. Adding or removing at either end only moves an index, so all end operations are amortised O(1), with no node objects. It is the recommended class for both stacks and queues:

- As a **stack**, it replaces the legacy `Stack` class (which extends the synchronized `Vector` and exposes index-based methods that break the stack idea).
- As a **queue**, it beats `LinkedList` on speed and memory.
- It does not allow `null` elements (because `null` is the "empty" signal from `poll` and `peek`), and it has no index access.

**`PriorityQueue`** is a **binary min-heap** stored in an array. A heap is a complete binary tree where each parent is smaller than or equal to its children; stored in an array, the children of index `i` sit at `2i + 1` and `2i + 2`. So the smallest element is always at index 0.

- `offer`: put the element at the end, then **sift up** (swap with the parent while smaller): O(log n).
- `poll`: take index 0, move the last element to the top, then **sift down**: O(log n).
- `peek`: O(1). `remove(Object)` and `contains`: O(n).
- Ordering is natural (`Comparable`) or a `Comparator` given to the constructor; for a max-heap pass `Comparator.reverseOrder()`.

```java
import java.util.*;

public class Queues {
    record Ticket(String id, int priority) { }

    public static void main(String[] args) {
        // ArrayDeque as a FIFO queue
        Deque<String> packing = new ArrayDeque<>();
        packing.offer("O-1");
        packing.offer("O-2");
        packing.offer("O-3");
        System.out.println(packing.poll() + " then " + packing.peek());

        // ArrayDeque as a LIFO stack: undo history
        Deque<String> undo = new ArrayDeque<>();
        undo.push("add pen");
        undo.push("add ink");
        undo.push("remove pen");
        System.out.println(undo.pop() + " | next: " + undo.peek());

        // Classic stack task: balanced brackets
        System.out.println(balanced("{[()]}") + " " + balanced("{[(])}"));

        // PriorityQueue: support tickets, lowest number = most urgent
        PriorityQueue<Ticket> tickets =
                new PriorityQueue<>(Comparator.comparingInt(Ticket::priority));
        tickets.add(new Ticket("T-7", 3));
        tickets.add(new Ticket("T-8", 1));
        tickets.add(new Ticket("T-9", 2));
        List<String> served = new ArrayList<>();
        while (!tickets.isEmpty()) served.add(tickets.poll().id());
        System.out.println(served);

        // Top 3 best sellers with a size-limited MIN-heap: O(n log k)
        int[] sales = {40, 5, 77, 12, 90, 33, 61};
        PriorityQueue<Integer> top = new PriorityQueue<>();
        for (int s : sales) {
            top.offer(s);
            if (top.size() > 3) top.poll();                   // drop the smallest
        }
        List<Integer> best = new ArrayList<>(top);
        best.sort(Comparator.reverseOrder());
        System.out.println(best);

        // The internal array is a heap, NOT sorted
        PriorityQueue<Integer> pq = new PriorityQueue<>(List.of(5, 1, 4, 2, 3));
        System.out.println(pq + " peek=" + pq.peek());
    }

    static boolean balanced(String s) {
        Deque<Character> stack = new ArrayDeque<>();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '(', '[', '{' -> stack.push(c);
                case ')' -> { if (stack.isEmpty() || stack.pop() != '(') return false; }
                case ']' -> { if (stack.isEmpty() || stack.pop() != '[') return false; }
                case '}' -> { if (stack.isEmpty() || stack.pop() != '{') return false; }
                default -> { }
            }
        }
        return stack.isEmpty();
    }
}
// Output:
// O-1 then O-2
// remove pen | next: add ink
// true false
// [T-8, T-9, T-7]
// [90, 77, 61]
// [1, 2, 4, 5, 3] peek=1
```

Two outputs deserve a second look. The top-3 trick keeps a **min**-heap of size k: the smallest of the current best is at the top, ready to be evicted when something better arrives. That is O(n log k) time and O(k) memory, much better than sorting everything when k is small. And printing a `PriorityQueue` shows its internal array, `[1, 2, 4, 5, 3]`, which is a valid heap but **not sorted**: only repeated `poll` gives sorted order.

| | `ArrayDeque` | `PriorityQueue` | `LinkedList` (as queue) |
|---|---|---|---|
| Structure | circular array | binary heap in an array | doubly linked nodes |
| Order out | FIFO or LIFO | smallest first (by comparator) | FIFO or LIFO |
| Add / remove at ends | O(1) amortised | O(log n) | O(1) |
| Peek | O(1) | O(1) | O(1) |
| `null` allowed | no | no | yes |
| Thread-safe | no | no | no |

> **Pitfall:** Iterating a `PriorityQueue` with for-each, or calling `toString` or `stream()` on it, does **not** give priority order. Use `poll` in a loop, or copy and sort.

> **Pitfall:** `Stack` and `Deque` iterate in opposite directions. A `Stack` (a `Vector`) iterates from bottom to top; an `ArrayDeque` used with `push` iterates from top (most recent) to bottom. Code ported from `Stack` to `ArrayDeque` that iterates may print in reverse.

> **Tip:** For a max-heap of objects, `new PriorityQueue<>(Comparator.comparingInt(Ticket::priority).reversed())`. For primitive `int`s, `new PriorityQueue<>(Comparator.reverseOrder())`.

::: deeper
**`ArrayDeque` internals.** It stores `Object[] elements` plus `head` and `tail` indexes; `addLast` writes at `tail` and advances it with wrap-around (`inc(i, length)`), and `addFirst` decrements `head` with wrap-around. When the array is full it grows (doubling while small, then by 50%) and unwraps the elements into the new array. Before Java 9 the capacity was always a power of two so wrap-around could use a mask; the current code uses comparisons and allows any length. Removing from the middle (`remove(Object)`) is O(n).

**Heap building is O(n).** `new PriorityQueue<>(collection)` does not insert elements one by one (O(n log n)): it copies them into the array and runs **heapify**, sifting down from the last parent to the root, which is O(n) overall. The same algorithm is the first phase of heapsort.

**Stability.** `PriorityQueue` is not stable: two elements with equal priority may come out in any order. If insertion order must break ties, add a sequence number to the comparator: `comparingInt(Ticket::priority).thenComparingLong(Ticket::seq)`.

**Changing priorities.** There is no "decrease key" operation. If an element's priority field changes while it sits in the queue, the heap is silently corrupted. Remove it (O(n)), change it, and re-add it, or (common in Dijkstra's algorithm) add a new entry and skip stale ones when polled.

**Concurrent versions.** `ArrayBlockingQueue`, `LinkedBlockingQueue` and `PriorityBlockingQueue` add blocking `put` and `take` for producer-consumer designs; `ConcurrentLinkedDeque` is a lock-free deque. They appear in the concurrency section and behind every thread pool.

> **Interview:** "How would you implement a stack and a queue in Java?" Use `ArrayDeque` for both: `push`/`pop`/`peek` for a stack and `offer`/`poll`/`peek` for a queue. Avoid the legacy `Stack` class, which extends the synchronized `Vector`, and prefer `ArrayDeque` over `LinkedList` for speed and memory.

> **Interview:** "Find the k largest elements in a large array." Keep a min-heap (`PriorityQueue`) of size k: offer each element and poll when the size exceeds k. What remains are the k largest, in O(n log k) time and O(k) space. For the k smallest, use a max-heap the same way.
:::

::: check
?? What is the time complexity of `offer`, `poll` and `peek` on a `PriorityQueue`?
`offer` and `poll` are O(log n) (sift up and sift down); `peek` is O(1) because the minimum is always at index 0.

?? Why does `ArrayDeque` reject `null` elements?
`poll` and `peek` return `null` to mean "empty". If `null` could be stored, a `null` result would be ambiguous.

?? A `PriorityQueue<Integer>` prints as `[1, 3, 2, 7, 4]`. Is it broken?
No. `toString` shows the internal heap array, which only guarantees each parent is no larger than its children. Polling repeatedly still returns 1, 2, 3, 4, 7.
:::

::: resources
- Docs: [PriorityQueue (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/PriorityQueue.html) · heap ordering and complexity notes
- Docs: [ArrayDeque (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/ArrayDeque.html) · "likely to be faster than Stack when used as a stack"
- Docs: [Deque (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Deque.html) · method table for queue and stack use
- Article: [Guide to Java PriorityQueue](https://www.baeldung.com/java-priorityqueue) · Baeldung with comparators
- Article: [Introduction to the Java ArrayDeque](https://www.baeldung.com/java-array-deque) · Baeldung on stack and queue use
- Docs: [Queue implementations (Java tutorial)](https://docs.oracle.com/javase/tutorial/collections/implementations/queue.html) · Oracle's overview of the queue family
:::

### Comparable vs Comparator; Comparator.comparing().thenComparing()

::: plain
Sorting needs a rule for "which comes first?". **Comparable** is the rule a class carries with it, its natural order, like dictionary order for words or number order for prices. **Comparator** is a rule you hand over from outside, for one particular job: sort the same products by price for the shopper, by stock level for the warehouse, by name for the catalogue. One class can have only one natural order, but as many comparators as you like.
:::

**`Comparable<T>`** lives in `java.lang` and has one method, `int compareTo(T other)`. It returns a **negative** number if `this` comes first, **zero** if they are equal in ordering, and a **positive** number if `this` comes after. `String`, the wrapper classes, `BigDecimal`, `LocalDate` and enums all implement it, so `Collections.sort`, `TreeSet` and `TreeMap` can use them without extra help.

**`Comparator<T>`** lives in `java.util` and has `int compare(T a, T b)` with the same sign rules. It is a **functional interface**, so a lambda or method reference can implement it, and since Java 8 it has many factory and combinator methods:

| Method | Builds |
|---|---|
| `Comparator.comparing(Product::name)` | compare by a key that is `Comparable` |
| `comparingInt`, `comparingLong`, `comparingDouble` | same, for primitive keys without boxing |
| `comparing(Product::name, String.CASE_INSENSITIVE_ORDER)` | compare a key with its own comparator |
| `.thenComparing(...)` | tie-breaker used when the previous result is 0 |
| `.reversed()` | the opposite order |
| `Comparator.naturalOrder()`, `reverseOrder()` | the natural order and its reverse |
| `Comparator.nullsFirst(c)`, `nullsLast(c)` | handle `null` elements or keys safely |

| | `Comparable` | `Comparator` |
|---|---|---|
| Package | `java.lang` | `java.util` |
| Method | `compareTo(T other)` | `compare(T a, T b)` |
| Where it lives | inside the class being sorted | separate object, lambda or method reference |
| How many | one natural order per class | any number |
| Used by | `Collections.sort(list)`, `TreeMap` with no comparator | `list.sort(c)`, `new TreeMap<>(c)`, `stream.sorted(c)` |
| Can sort classes you do not own | no | yes |

```java
import java.math.BigDecimal;
import java.util.*;

public class Sorting {
    record Product(String sku, String name, BigDecimal price, int stock, String brand)
            implements Comparable<Product> {
        @Override public int compareTo(Product other) {        // natural order: by SKU
            return sku.compareTo(other.sku);
        }
    }

    public static void main(String[] args) {
        List<Product> catalogue = new ArrayList<>(List.of(
                new Product("P3", "pen", new BigDecimal("49.00"), 120, "Cello"),
                new Product("P1", "ink", new BigDecimal("89.50"), 0, null),
                new Product("P2", "Notebook", new BigDecimal("49.00"), 35, "Classmate"),
                new Product("P4", "box file", new BigDecimal("210.00"), 35, "Cello")));

        Collections.sort(catalogue);                              // natural order
        System.out.println(skus(catalogue));

        catalogue.sort(Comparator.comparing(Product::price)       // cheapest first
                .thenComparing(Product::name, String.CASE_INSENSITIVE_ORDER));
        System.out.println(skus(catalogue));

        catalogue.sort(Comparator.comparingInt(Product::stock).reversed()   // most stock first
                .thenComparing(Product::sku));
        System.out.println(skus(catalogue));

        catalogue.sort(Comparator.comparing(Product::brand,
                Comparator.nullsLast(Comparator.naturalOrder())));          // null brand last
        System.out.println(skus(catalogue));

        // The same comparator drives a TreeMap and a stream
        Map<Product, Integer> byName = new TreeMap<>(Comparator.comparing(Product::name));
        catalogue.forEach(p -> byName.put(p, p.stock()));
        System.out.println(byName.keySet().stream().map(Product::name).toList());
        System.out.println(catalogue.stream().max(Comparator.comparing(Product::price)).get().name());
    }

    static List<String> skus(List<Product> ps) {
        return ps.stream().map(Product::sku).toList();
    }
}
// Output:
// [P1, P2, P3, P4]
// [P2, P3, P1, P4]
// [P3, P2, P4, P1]
// [P3, P4, P2, P1]
// [Notebook, box file, ink, pen]
// box file
```

Read the third sort carefully: P2 and P4 both have stock 35, and the `thenComparing(Product::sku)` tie-breaker puts P2 first. In the fourth sort, the two "Cello" products keep the order they already had (P3 before P4) because `List.sort` is **stable**: equal elements keep their relative order. Stability lets you sort by several keys in separate passes, though one chained comparator is clearer. The `TreeMap` result shows uppercase "Notebook" first, because natural `String` order compares character codes and `'N'` (78) is less than `'b'` (98).

**Writing `compareTo` correctly.** Use `Integer.compare(a, b)`, `Long.compare`, or a key's own `compareTo`, or build a comparator with `Comparator.comparing`. The contract mirrors `equals`: the sign must flip when the arguments are swapped (`sgn(a.compareTo(b)) == -sgn(b.compareTo(a))`), it must be transitive, and ideally `compareTo` returns 0 exactly when `equals` is true (consistent with equals; `TreeSet` relies on it, as the sets topic showed).

> **Pitfall:** Never write `return this.stock - other.stock;`. For large values of opposite sign the subtraction overflows and returns the wrong sign: with `Integer.MIN_VALUE` and 1, `MIN_VALUE - 1` wraps to `Integer.MAX_VALUE`, a positive number, so the smallest value sorts last. Use `Integer.compare`.

> **Pitfall:** `Comparator.comparing(p -> p.price()).reversed()` does not compile: "cannot find symbol ... variable p of type Object". Inference cannot see through `.reversed()`, so the lambda's parameter type is unknown. Use a method reference (`Product::price`) or an explicit type, `(Product p) -> p.price()`.

::: deeper
**What `comparing` builds.** `Comparator.comparing(keyExtractor)` returns roughly `(a, b) -> keyExtractor.apply(a).compareTo(keyExtractor.apply(b))`, and `thenComparing(other)` returns `(a, b) -> { int r = compare(a, b); return r != 0 ? r : other.compare(a, b); }`. The comparators are serializable when the lambdas are. `comparingInt` avoids boxing two `Integer`s per comparison, which matters when sorting millions of elements.

**Sorting algorithms in the JDK.** `List.sort`, `Collections.sort` and `Arrays.sort(Object[])` use **TimSort**: a stable, adaptive merge sort, O(n log n) worst case and close to O(n) on already partly sorted data (very common in real data). `Arrays.sort` on primitive arrays uses **dual-pivot quicksort** (not stable, which does not matter for primitives). Since `ArrayList` overrides `sort` to sort its backing array directly, no copy is made. TimSort can detect a broken comparator and throw `IllegalArgumentException: Comparison method violates its general contract!`: a sign that your comparator is not transitive, often because of the subtraction bug or inconsistent `null` handling.

**`Comparable` and generics.** Implement `Comparable<Money>`, not raw `Comparable`, so `compareTo` takes a `Money` and the compiler checks the type. The compiler-generated bridge method `compareTo(Object)` (see type erasure) casts and delegates.

**Locale-aware text.** `String.compareTo` compares UTF-16 code units, which is wrong for human-facing sorting in many languages. For customer-visible lists use `java.text.Collator.getInstance(locale)`, which is itself a `Comparator<Object>`.

**Records and enums.** Records do not implement `Comparable` automatically; add it or use comparators. Enums do, in **declaration order** (`ordinal`), so `enum Priority { LOW, MEDIUM, HIGH }` sorts LOW first; reorder the constants and you silently change sort order.

> **Interview:** "Comparable vs Comparator?" `Comparable` defines a class's single natural ordering through `compareTo`, implemented inside the class. `Comparator` is an external strategy object with `compare(a, b)`, so you can define many orderings, including for classes you cannot modify. Since Java 8, `Comparator.comparing`, `thenComparing`, `reversed` and `nullsFirst` build them declaratively.

> **Interview:** "Is `Collections.sort` stable? What algorithm does it use?" Yes, it is stable; it uses TimSort, a hybrid of merge sort and insertion sort, O(n log n) in the worst case and very fast on partially ordered input.
:::

::: check
?? Why is `(a, b) -> a.qty - b.qty` a risky comparator?
The subtraction can overflow for large values of opposite sign, returning the wrong sign and breaking the order (and possibly TimSort's contract check). `Integer.compare(a.qty, b.qty)` or `Comparator.comparingInt(Line::qty)` is safe.

?? Write a comparator for orders: newest date first, then highest total first.
`Comparator.comparing(Order::date).reversed().thenComparing(Comparator.comparing(Order::total).reversed())`, or `Comparator.comparing(Order::date, Comparator.reverseOrder()).thenComparing(Order::total, Comparator.reverseOrder())`.

?? What does stability mean for a sort, and is `List.sort` stable?
Equal elements keep their original relative order. Yes, `List.sort` (TimSort) is stable.
:::

::: resources
- Docs: [Comparator (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Comparator.html) · all factory and combinator methods
- Docs: [Comparable (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Comparable.html) · natural ordering and consistency with equals
- Docs: [Object ordering (Java tutorial)](https://docs.oracle.com/javase/tutorial/collections/interfaces/order.html) · writing `compareTo` and comparators
- Article: [Comparator and Comparable in Java](https://www.baeldung.com/java-comparator-comparable) · Baeldung comparison with examples
- Article: [Guide to Java 8 Comparator.comparing()](https://www.baeldung.com/java-8-comparator-comparing) · Baeldung on the fluent API
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 14: consider implementing Comparable
:::

### Iterator, ListIterator; fail-fast vs fail-safe iterators

::: plain
An iterator is a bookmark that walks through a collection one element at a time: "is there another?" (`hasNext`), "give it to me" (`next`), and "throw away the one you just gave me" (`remove`). A **fail-fast** iterator is a strict librarian: if someone rearranges the shelf behind her back while she is walking along it, she stops immediately and complains, instead of handing you the wrong book. A **fail-safe** iterator walks along a photocopy of the shelf (or a shelf designed for many people at once), so changes never upset it, but it may not show the very latest books.
:::

**`Iterator<E>`** has three methods: `hasNext()`, `next()` (throws `NoSuchElementException` past the end) and the optional `remove()`, which removes the element last returned by `next`. Every `Collection` provides one through `iterator()`, and the enhanced `for` loop is compiled into exactly that:

```java
for (String sku : skus) { ... }
// compiles to:
for (Iterator<String> it = skus.iterator(); it.hasNext(); ) { String sku = it.next(); ... }
```

**`ListIterator<E>`**, from `list.listIterator()` or `listIterator(index)`, walks a list in **both directions** and can change it in place: `hasPrevious()`, `previous()`, `nextIndex()`, `previousIndex()`, `set(e)` (replace the last returned element) and `add(e)` (insert at the cursor).

**Fail-fast iterators.** The iterators of `ArrayList`, `HashMap`, `HashSet`, `LinkedList`, `TreeMap` and the other non-concurrent collections are **fail-fast**. Each collection keeps a counter, `modCount`, incremented on every **structural** change (an add or remove, not a `set`). The iterator copies it on creation (`expectedModCount`) and compares on each `next()`. If the collection was changed by anything other than the iterator's own `remove` or `add`, it throws **`ConcurrentModificationException`** (CME). The name misleads: it is mostly thrown in **single-threaded** code that modifies a collection inside a for-each loop.

**Fail-safe (weakly consistent and snapshot) iterators.** The concurrent collections never throw CME:

- `CopyOnWriteArrayList` and `CopyOnWriteArraySet` copy the whole array on every write; an iterator walks the **snapshot** taken at its creation, so it never sees later changes, and its `remove` is unsupported.
- `ConcurrentHashMap`, `ConcurrentLinkedQueue` and `ConcurrentSkipListMap` iterators are **weakly consistent**: they reflect the state at some point at or after creation, may or may not show later changes, never throw CME, and never return an element twice.

| | Fail-fast | Fail-safe (snapshot or weakly consistent) |
|---|---|---|
| Examples | `ArrayList`, `HashMap`, `HashSet`, `TreeMap` | `CopyOnWriteArrayList`, `ConcurrentHashMap` |
| Modification during iteration | throws `ConcurrentModificationException` | allowed |
| Sees concurrent changes | n/a (fails) | snapshot: no; weakly consistent: maybe |
| Cost | none extra | copying on write, or more complex structure |
| Guarantee | best effort, not guaranteed | by design |

```java
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Iterators {
    public static void main(String[] args) {
        List<String> cart = new ArrayList<>(List.of("pen", "promo-sticker", "ink", "box"));

        // 1. Wrong: removing through the list inside for-each
        try {
            for (String item : cart) {
                if (item.startsWith("promo")) cart.remove(item);
            }
        } catch (ConcurrentModificationException e) {
            System.out.println("CME from for-each removal");
        }
        System.out.println(cart);

        // 2. Right: Iterator.remove, or removeIf
        cart.add("promo-pen");
        Iterator<String> it = cart.iterator();
        while (it.hasNext()) {
            if (it.next().startsWith("promo")) it.remove();
        }
        cart.add("promo-ink");
        cart.removeIf(s -> s.startsWith("promo"));
        System.out.println(cart);

        // 3. ListIterator: replace and insert while walking, then walk back
        ListIterator<String> li = cart.listIterator();
        while (li.hasNext()) {
            String s = li.next();
            if (s.equals("ink")) li.set("INK");
            if (s.equals("pen")) li.add("pen-refill");
        }
        StringBuilder back = new StringBuilder();
        while (li.hasPrevious()) back.append(li.previous()).append(' ');
        System.out.println(cart + " backwards: " + back.toString().trim());

        // 4. Maps: remove while iterating through the entry set iterator
        Map<String, Integer> stock = new HashMap<>(Map.of("pen", 0, "ink", 4, "box", 0));
        stock.entrySet().removeIf(e -> e.getValue() == 0);
        System.out.println(stock);

        // 5. Fail-safe: snapshot iterator ignores changes made during the loop
        List<String> listeners = new CopyOnWriteArrayList<>(List.of("email", "sms"));
        for (String l : listeners) listeners.add(l + "-retry");
        System.out.println(listeners);

        // 6. Weakly consistent: ConcurrentHashMap allows removal while iterating
        Map<String, Integer> live = new ConcurrentHashMap<>(Map.of("a", 1, "b", 2, "c", 3));
        for (String k : live.keySet()) if (live.get(k) % 2 == 1) live.remove(k);
        System.out.println(live);
    }
}
// Output:
// CME from for-each removal
// [pen, ink, box]
// [pen, ink, box]
// [pen, pen-refill, INK, box] backwards: box INK pen-refill pen
// {ink=4}
// [email, sms, email-retry, sms-retry]
// {b=2}
```

Look at case 1 closely: the loop removed `promo-sticker` and **then** threw on the next `next()` call, so the list was already changed. A CME can leave data half-processed.

**The famous exception to the exception.** Removing the **second-to-last** element inside a for-each loop over an `ArrayList` does **not** throw. After the removal, `size` shrinks to equal the iterator's cursor, so `hasNext()` returns `false`, the loop ends, and `next()` is never called to do the check. The last element is silently skipped. This is why the documentation says fail-fast behaviour "cannot be guaranteed" and must only be used to detect bugs.

> **Pitfall:** Removing by index in a forward `for (int i...)` loop does not throw, but skips the element after each removed one, because the following elements shift left. Loop backwards, use `removeIf`, or use an iterator.

> **Tip:** `Collection.removeIf` (Java 8) is the cleanest way to remove by condition. On `ArrayList` it is a single O(n) pass, much faster than repeated `remove` calls.

::: deeper
**The check in the source.** `ArrayList.Itr.next()` starts with `checkForComodification()`, which is `if (modCount != expectedModCount) throw new ConcurrentModificationException();`. `Itr.remove()` calls `ArrayList.this.remove(lastRet)` and then sets `expectedModCount = modCount`, which is why removing through the iterator is allowed. `hasNext()` is just `cursor != size`, with no check, which causes the second-to-last quirk.

**Not a thread-safety tool.** `modCount` is a plain `int`, not `volatile`, so another thread's changes may not even be visible to the iterator. In multithreaded code, a missing CME proves nothing. Shared collections need concurrent collections or locking (iterating a `Collections.synchronizedList` requires a `synchronized (list)` block around the whole loop, as its documentation says).

**`CopyOnWriteArrayList` trade-off.** Every `add`, `set` or `remove` copies the entire array, O(n) per write, while reads and iteration are lock-free and fast. It fits lists that are read constantly and changed rarely: event listeners, configuration, routing tables. For a busy write path it is a poor choice.

**`Spliterator`.** Since Java 8, collections also provide a `Spliterator` ("splittable iterator") used by streams. It can split itself for parallel processing and reports characteristics (`ORDERED`, `SIZED`, `DISTINCT`, `SORTED`). Most JDK spliterators are **late-binding** and fail-fast like iterators. Core Java 3 covers streams.

**`Iterable` for your own classes.** Implement `Iterable<T>` (one method, `iterator()`) and your class works in for-each loops. Often you can just return the iterator of an internal, unmodifiable list.

> **Interview:** "What is a fail-fast iterator, and how does it work?" It throws `ConcurrentModificationException` when the collection is structurally modified after the iterator was created, other than through the iterator itself. The collection counts structural modifications in `modCount`; the iterator remembers the value and checks it in `next()`. It is a best-effort bug detector, not a guarantee.

> **Interview:** "How do you remove elements while iterating?" Use `Iterator.remove()` or, more simply, `collection.removeIf(predicate)`; for maps, `map.entrySet().removeIf(...)` or `map.values().removeIf(...)`. Or iterate over a concurrent collection such as `CopyOnWriteArrayList` or `ConcurrentHashMap`, whose iterators tolerate modification.
:::

::: check
?? Is `ConcurrentModificationException` only thrown when several threads are involved?
No. It most often happens in a single thread that modifies a collection directly while iterating over it with for-each. With several threads it is not even guaranteed to be thrown.

?? What is the difference between a snapshot iterator and a weakly consistent one?
A snapshot iterator (`CopyOnWriteArrayList`) traverses the array as it was when the iterator was created and never sees later changes. A weakly consistent iterator (`ConcurrentHashMap`) traverses the live structure and may or may not reflect changes made during iteration; neither throws CME.

?? What can a `ListIterator` do that an `Iterator` cannot?
Move backwards (`hasPrevious`, `previous`), report indexes (`nextIndex`, `previousIndex`), replace the last returned element (`set`) and insert at the cursor (`add`).
:::

::: resources
- Docs: [Iterator (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Iterator.html) · `hasNext`, `next`, `remove`, `forEachRemaining`
- Docs: [ConcurrentModificationException (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/ConcurrentModificationException.html) · why fail-fast is best effort
- Docs: [ListIterator (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/ListIterator.html) · bidirectional iteration and cursor positions
- Article: [Fail-safe iterator vs fail-fast iterator](https://www.baeldung.com/java-fail-safe-vs-fail-fast-iterator) · Baeldung comparison
- Article: [Avoiding the ConcurrentModificationException](https://www.baeldung.com/java-concurrentmodificationexception) · Baeldung on safe removal patterns
- Docs: [CopyOnWriteArrayList (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CopyOnWriteArrayList.html) · snapshot iterators and their costs
:::

### Collections & Arrays utility methods; immutable collections (List.of, Map.of)

::: plain
`Collections` and `Arrays` are toolboxes of ready-made helpers: sort this, find the biggest, fill that, wrap this list so nobody can change it. **Immutable collections** are sealed boxes: once packed, nothing can be added, removed or replaced. You can hand a sealed box to any part of the program, or to many threads at once, without worrying that someone will quietly change its contents.
:::

**`java.util.Collections`**: static helpers that work on any collection.

| Group | Methods |
|---|---|
| Ordering | `sort(list)`, `sort(list, c)`, `reverse`, `shuffle`, `swap`, `rotate` |
| Searching | `binarySearch(sortedList, key)`, `max`, `min` (with optional comparator), `frequency`, `disjoint`, `indexOfSubList` |
| Building | `nCopies(n, x)`, `addAll(c, a, b, c)`, `fill(list, x)` |
| Empty and single | `emptyList()`, `emptyMap()`, `singletonList(x)`, `singleton(x)` (all immutable) |
| Read-only views | `unmodifiableList`, `unmodifiableSet`, `unmodifiableMap`, `unmodifiableSequencedCollection` (21)... |
| Synchronized wrappers | `synchronizedList`, `synchronizedMap`... (legacy; prefer concurrent collections) |
| Type-checked views | `checkedList(list, String.class)` (throws at insertion if heap pollution occurs) |

**`java.util.Arrays`** (covered with arrays in Core Java 1): `sort`, `parallelSort`, `binarySearch`, `fill`, `copyOf`, `copyOfRange`, `equals`, `deepEquals`, `hashCode`, `toString`, `deepToString`, `mismatch`, `compare`, `stream`, `setAll` and the important bridge to collections, `asList`.

**Three kinds of "you cannot change this list".** This is a favourite interview topic because the three look alike and behave differently:

| | `Arrays.asList(a)` | `Collections.unmodifiableList(l)` | `List.of(...)` / `List.copyOf(l)` |
|---|---|---|---|
| What it is | fixed-size view over the array | read-only **view** of another list | truly **immutable** list |
| `set` | allowed, writes through to the array | throws `UnsupportedOperationException` | throws |
| `add` / `remove` | throw | throw | throw |
| Sees later changes to the source | yes (array changes) | **yes** (backing list changes) | no (no source; `copyOf` copies) |
| `null` elements | allowed | allowed | **rejected** with `NullPointerException` |
| Since | Java 1.2 | Java 1.2 | Java 9 (`of`), 10 (`copyOf`) |

**Immutable collection factories (Java 9+).** `List.of(...)`, `Set.of(...)` and `Map.of(k1, v1, ...)` (up to 10 pairs; beyond that `Map.ofEntries(Map.entry(k, v), ...)`) create compact, immutable collections. Their rules:

- Any `null` element, key or value throws `NullPointerException`, and so does `contains(null)` on them.
- `Set.of` and `Map.of` throw `IllegalArgumentException` on **duplicate** elements or keys, so mistakes in constants are caught at startup.
- `Set.of` and `Map.of` iteration order is **deliberately randomised** per JVM run, so code cannot come to depend on it.
- `List.copyOf`, `Set.copyOf` and `Map.copyOf` (Java 10) make an immutable copy, or return the argument itself if it already is one.
- `stream.toList()` (Java 16) returns an unmodifiable list that, unlike `List.of`, allows `null`s.

```java
import java.util.*;

public class Utilities {
    public static void main(String[] args) {
        List<Integer> prices = new ArrayList<>(List.of(499, 89, 1299, 89, 249));
        Collections.sort(prices);
        System.out.println(prices + " max=" + Collections.max(prices)
                + " freq(89)=" + Collections.frequency(prices, 89)
                + " idx(499)=" + Collections.binarySearch(prices, 499));
        Collections.reverse(prices);
        System.out.println(prices + " " + Collections.nCopies(3, "-"));

        // Arrays.asList: fixed size, writes through to the array
        String[] arr = {"pen", "ink"};
        List<String> view = Arrays.asList(arr);
        view.set(0, "PEN");
        System.out.println(arr[0] + " " + tryAdd(view));

        // Unmodifiable VIEW: read-only for you, but it still sees the owner's changes
        List<String> owner = new ArrayList<>(List.of("pen"));
        List<String> readOnly = Collections.unmodifiableList(owner);
        owner.add("ink");
        System.out.println(readOnly + " " + tryAdd(readOnly));

        // Truly immutable copy: independent of the source
        List<String> snapshot = List.copyOf(owner);
        owner.add("box");
        System.out.println(snapshot + " vs " + owner);

        // Factory rules
        try { List.of("pen", null); } catch (NullPointerException e) { System.out.println("List.of rejects null"); }
        try { Set.of("pen", "pen"); } catch (IllegalArgumentException e) { System.out.println(e.getMessage()); }
        Map<String, Integer> limits = Map.of("pen", 10, "ink", 5);
        System.out.println(limits.get("ink") + " " + tryPut(limits));
        System.out.println(List.copyOf(snapshot) == snapshot);    // already immutable: no copy
    }

    static String tryAdd(List<String> l) {
        try { l.add("x"); return "add ok"; }
        catch (UnsupportedOperationException e) { return "add refused"; }
    }

    static String tryPut(Map<String, Integer> m) {
        try { m.put("box", 1); return "put ok"; }
        catch (UnsupportedOperationException e) { return "put refused"; }
    }
}
// Output:
// [89, 89, 249, 499, 1299] max=1299 freq(89)=2 idx(499)=3
// [1299, 499, 249, 89, 89] [-, -, -]
// PEN add refused
// [pen, ink] add refused
// [pen, ink] vs [pen, ink, box]
// List.of rejects null
// duplicate element: pen
// 5 put refused
// true
```

**Defensive copies.** The pattern from the immutable classes topic in Core Java 1 now has a one-liner. A record or class that takes a list should store `List.copyOf(items)`: the caller cannot change your data afterwards, and you can return the field directly from a getter because nobody can change it either.

```java
record Order(String id, List<OrderLine> lines) {
    Order {
        lines = List.copyOf(lines);    // immutable copy; also rejects null lines
    }
}
```

> **Pitfall:** Immutability is **shallow**. `List.of(cart1, cart2)` cannot gain or lose carts, but each `Cart` object inside can still change. For deep immutability the elements must be immutable too (records of `String`s and numbers, for instance).

> **Pitfall:** `Arrays.asList(new int[] {1, 2, 3})` gives a `List<int[]>` with **one** element (the array), because generics cannot hold primitives. Use `List.of(1, 2, 3)` or `IntStream.of(arr).boxed().toList()`.

::: deeper
**How the factories are implemented.** `List.of` returns classes from `java.util.ImmutableCollections`: `List12` for one or two elements (fields, no array), `ListN` for more, and similar `Set12`, `SetN`, `Map1`, `MapN`. `SetN` and `MapN` use **open addressing** with linear probing in a single array, so they use less memory than `HashSet`/`HashMap`. The iteration order of `SetN` and `MapN` depends on a `SALT` value chosen randomly at JVM start-up, which is why it changes between runs.

**Why the factories reject null.** Null elements make `contains(null)` and `get` results ambiguous and are almost always bugs; the designers took the chance of a fresh API to forbid them. The flip side: `contains(null)` on these collections throws instead of returning `false`, a surprise when you pass one to code that probes for `null`.

**Unmodifiable vs immutable.** The JDK documentation uses **unmodifiable** for "this reference cannot change it" (both views and `List.of` are unmodifiable) and saves **immutable** for collections nobody can change. The interview answer is the table above: a view still reflects changes made through the original reference.

**`Collectors` and immutability.** `Collectors.toList()` makes no promise about the list type (today an `ArrayList`). `Collectors.toUnmodifiableList()` (Java 10) and `Stream.toList()` (Java 16) give unmodifiable lists; `toUnmodifiableList` rejects nulls, `Stream.toList` allows them. Core Java 3 covers collectors.

**Guava and others.** Before Java 9, Google Guava's `ImmutableList` filled this gap, and you will still see it in older code. Its semantics are almost the same as `List.copyOf`.

**`Collections.synchronizedList`** wraps every method in `synchronized (mutex)`. Iteration and compound actions ("if absent then add") still need an external `synchronized (list) { ... }` block. In new code, prefer `CopyOnWriteArrayList` or `ConcurrentHashMap`.

> **Interview:** "Difference between `Collections.unmodifiableList(list)` and `List.copyOf(list)`?" The first is a read-only view: callers cannot change it, but changes made to the original list show through it. The second is an independent immutable copy: nothing can change it, and it rejects `null` elements. Use `copyOf` for defensive copies.

> **Interview:** "What does `Arrays.asList` return?" A fixed-size list backed by the array: `set` writes through to the array, `add` and `remove` throw `UnsupportedOperationException`. Wrap it in `new ArrayList<>(Arrays.asList(...))` if you need a growable list.
:::

::: check
?? `List<String> ro = Collections.unmodifiableList(src); src.add("x");` Does `ro` contain `"x"`?
Yes. An unmodifiable view blocks changes through `ro` only; it reads through to `src`, so later changes to `src` are visible.

?? What happens with `Map.of("pen", 1, "pen", 2)`?
It throws `IllegalArgumentException` (duplicate key) when the map is created. The immutable factories reject duplicate keys and set elements, and all `null`s.

?? Why might a test that prints a `Set.of(...)` pass on one run and fail on the next?
The iteration order of `Set.of` and `Map.of` is randomised per JVM run, on purpose. Tests must not depend on it; compare sets with `equals` or sort first.
:::

::: resources
- Docs: [Collections (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Collections.html) · every static helper
- Docs: [Unmodifiable lists (List API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/List.html#unmodifiable) · the exact rules for `List.of` and `copyOf`
- Spec: [JEP 269: Convenience factory methods for collections](https://openjdk.org/jeps/269) · the design of `List.of`, `Set.of` and `Map.of`
- Docs: [Creating unmodifiable collections (core libraries guide)](https://docs.oracle.com/en/java/javase/21/core/creating-immutable-lists-sets-and-maps.html) · views vs immutable copies explained by Oracle
- Article: [Immutable ArrayList in Java](https://www.baeldung.com/java-immutable-list) · Baeldung on the different approaches
- Docs: [Arrays (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Arrays.html) · `asList`, `sort`, `mismatch` and friends
:::

### Sequenced collections (Java 21): getFirst(), getLast(), reversed()

::: plain
Many collections have a clear first and last element: a list, a queue, a set that remembers insertion order. Before Java 21, each one asked for those ends in a different way, like a family where everyone says "the oldest child" in a different language. Java 21 gave them all one shared vocabulary: `getFirst()`, `getLast()`, `addFirst()`, `addLast()`, `removeFirst()`, `removeLast()` and `reversed()`.
:::

**The problem.** Getting the last element used to look different for every type:

| Collection | First element | Last element (before Java 21) |
|---|---|---|
| `List` | `list.get(0)` | `list.get(list.size() - 1)` |
| `Deque` | `deque.getFirst()` | `deque.getLast()` |
| `SortedSet` | `set.first()` | `set.last()` |
| `LinkedHashSet` | `set.iterator().next()` | loop through the whole set |
| `LinkedHashMap` | `map.entrySet().iterator().next()` | loop through the whole map |

And there was no common type meaning "a collection with a defined order", so a method could not ask for one.

**The solution: JEP 431 (Java 21)** added three interfaces to the hierarchy:

- **`SequencedCollection<E>`** extends `Collection<E>`: `addFirst`, `addLast`, `getFirst`, `getLast`, `removeFirst`, `removeLast`, and `reversed()`, which returns a **reverse-ordered view**. `List` and `Deque` now extend it.
- **`SequencedSet<E>`** extends `SequencedCollection<E>` and `Set<E>`, with `reversed()` returning a `SequencedSet`. `LinkedHashSet` implements it, and `SortedSet` extends it.
- **`SequencedMap<K, V>`** extends `Map<K, V>`: `firstEntry`, `lastEntry`, `pollFirstEntry`, `pollLastEntry`, `putFirst`, `putLast`, `reversed()`, and the views `sequencedKeySet()`, `sequencedValues()`, `sequencedEntrySet()`. `LinkedHashMap` implements it, and `SortedMap` extends it.

`HashSet` and `HashMap` are **not** sequenced: they have no defined order. `Collections` gained `unmodifiableSequencedCollection`, `unmodifiableSequencedSet` and `unmodifiableSequencedMap`.

**Behaviour details.** `getFirst` and `getLast` on an empty collection throw `NoSuchElementException`. Where the position is decided by the collection itself, `addFirst`/`addLast` (and `putFirst`/`putLast`) throw `UnsupportedOperationException`: in a `TreeSet` sorted order decides, and immutable lists cannot change at all. In a `LinkedHashSet` or `LinkedHashMap`, adding an element that is already present with `addFirst` or `putFirst` **moves** it to that end.

```java
import java.util.*;

public class Sequenced {
    public static void main(String[] args) {
        List<String> timeline = new ArrayList<>(List.of("placed", "paid", "packed"));
        timeline.addLast("shipped");
        System.out.println(timeline.getFirst() + " -> " + timeline.getLast());
        System.out.println(timeline.reversed());

        // reversed() is a live view, not a copy
        List<String> newestFirst = timeline.reversed();
        timeline.addLast("delivered");
        System.out.println(newestFirst.getFirst());

        // LinkedHashSet: recently viewed products, most recent at the front
        LinkedHashSet<String> recentlyViewed = new LinkedHashSet<>();
        for (String p : List.of("pen", "ink", "box", "pen")) recentlyViewed.addFirst(p);
        System.out.println(recentlyViewed + " oldest=" + recentlyViewed.getLast());

        // LinkedHashMap as a SequencedMap
        LinkedHashMap<String, Integer> steps = new LinkedHashMap<>();
        steps.put("cart", 1);
        steps.put("address", 2);
        steps.put("payment", 3);
        steps.putFirst("login", 0);
        System.out.println(steps.firstEntry() + " " + steps.lastEntry() + " " + steps.reversed());
        System.out.println(steps.pollLastEntry() + " left " + steps.sequencedKeySet());

        // One method for every ordered collection
        System.out.println(ends(timeline) + " " + ends(new TreeSet<>(Set.of(3, 1, 2)))
                + " " + ends(new ArrayDeque<>(List.of('a', 'b'))));

        // Rules at the edges
        try { new TreeSet<>(Set.of(1)).addFirst(0); }
        catch (UnsupportedOperationException e) { System.out.println("TreeSet decides its own order"); }
        try { new ArrayList<String>().getFirst(); }
        catch (NoSuchElementException e) { System.out.println("empty: NoSuchElementException"); }
    }

    static String ends(SequencedCollection<?> c) {
        return c.getFirst() + ".." + c.getLast();
    }
}
// Output:
// placed -> shipped
// [shipped, packed, paid, placed]
// delivered
// [pen, box, ink] oldest=ink
// login=0 payment=3 {payment=3, address=2, cart=1, login=0}
// payment=3 left [login, cart, address]
// placed..delivered 1..3 a..b
// TreeSet decides its own order
// empty: NoSuchElementException
```

The `recentlyViewed` set shows the "move to front" rule: viewing "pen" again moved it to the front instead of adding a duplicate, which is exactly the behaviour a "recently viewed" widget needs.

> **Pitfall:** `list.getFirst()` on an empty list throws `NoSuchElementException`, while `list.get(0)` throws `IndexOutOfBoundsException`. Code that catches one will not catch the other, so check `isEmpty()` first when emptiness is normal.

> **Tip:** Write method parameters as `SequencedCollection<T>` when you need "something ordered with ends", for example a function that formats the first and last events of a timeline. It then accepts lists, deques, linked sets and sorted sets alike.

::: deeper
**Default methods make it compatible.** The new methods are `default` methods on the interfaces, so existing implementations, including third-party collections, got them without recompiling. `List.reversed()` by default returns a view (`ReverseOrderListView`) that maps each index `i` to `size() - 1 - i`, so nothing is copied. `Deque.reversed()` gives a view whose iteration uses `descendingIterator()`, and `NavigableSet`/`NavigableMap` simply return their existing `descendingSet()`/`descendingMap()`.

**Migration surprises.** Adding methods to widely implemented interfaces can clash with existing code. Two real examples from the Java 21 rollout: a class implementing both `List` and `Deque` (like `LinkedList`) needed a single covariant `reversed()` that satisfies both; and libraries or Kotlin code that already defined their own `getFirst()`/`removeFirst()` (members or extension functions) with different return types or semantics ran into clashes or silently started calling the new JDK method. If a build breaks after moving to JDK 21 with an error about `reversed()` or `getFirst()`, this is the reason.

**What was not added.** There is no `SequencedCollection.get(int)`: positional access stays a `List` feature. `HashMap` and `HashSet` gained nothing, and `Map.of`/`Set.of` results are not sequenced. Immutable lists from `List.of` are `SequencedCollection`s, so `getFirst` works, but mutators throw.

**Version notes.** JEP 431 shipped final in Java 21 with no preview phase. Java 25 (the next LTS) adds nothing new to these interfaces, so code written against Java 21 sequenced collections carries forward unchanged.

> **Interview:** "What are sequenced collections?" A Java 21 feature (JEP 431) that adds the interfaces `SequencedCollection`, `SequencedSet` and `SequencedMap` to the hierarchy for collections with a defined encounter order. They provide uniform `getFirst`, `getLast`, `addFirst`, `addLast`, `removeFirst`, `removeLast` and a `reversed()` view (plus `firstEntry`, `pollLastEntry`, `putFirst` for maps). `List`, `Deque`, `LinkedHashSet`, `SortedSet`, `LinkedHashMap` and `SortedMap` are sequenced; `HashSet` and `HashMap` are not.

> **Interview:** "Does `list.reversed()` copy the list?" No. It returns a reverse-ordered view backed by the original: changes to either show in the other. Copy it with `new ArrayList<>(list.reversed())` if you need an independent list.
:::

::: check
?? Which of these are sequenced: `ArrayList`, `HashSet`, `LinkedHashMap`, `TreeSet`, `HashMap`?
`ArrayList` (a `List`), `LinkedHashMap` (a `SequencedMap`) and `TreeSet` (a `SortedSet`, which extends `SequencedSet`). `HashSet` and `HashMap` have no defined order, so they are not.

?? What does `addFirst("pen")` do on a `LinkedHashSet` that already contains `"pen"` at the end?
It moves `"pen"` to the front. A `LinkedHashSet` cannot hold duplicates, so `addFirst` repositions an existing element.

?? Why does `treeSet.addFirst(x)` throw `UnsupportedOperationException`?
A `TreeSet` places every element by its sort order, so the caller cannot choose a position. Only `getFirst`, `getLast`, `removeFirst`, `removeLast` and `reversed` make sense for it.
:::

::: resources
- Spec: [JEP 431: Sequenced collections](https://openjdk.org/jeps/431) · motivation, the new interfaces and compatibility notes
- Docs: [SequencedCollection (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/SequencedCollection.html) · method contracts and the reversed view
- Docs: [SequencedMap (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/SequencedMap.html) · `firstEntry`, `putFirst`, `pollLastEntry`
- Article: [Sequenced collections in Java 21](https://www.baeldung.com/java-21-sequenced-collections) · Baeldung walk-through with examples
- Docs: [Collections framework overview (Java SE 21)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/doc-files/coll-overview.html) · where the sequenced interfaces sit in the framework
- Docs: [SequencedSet (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/SequencedSet.html) · the set flavour and how `LinkedHashSet` and `SortedSet` fit
:::

## Wrap-up

### Cheat sheet

| Task | Code |
|---|---|
| Always close resources | `try (var c = ds.getConnection(); var ps = c.prepareStatement(sql)) { ... }` |
| Same handling for two exceptions | `catch (IOException \| IllegalArgumentException e)` |
| Translate and keep the cause | `throw new PaymentException("Payment failed for " + id, e);` |
| Fail fast on bad input | `Objects.requireNonNull(id, "id")`, `throw new IllegalArgumentException(...)` |
| Log an exception properly | `log.error("Refund failed for order {}", id, e);` |
| Generic method with a bound | `static <T extends Comparable<? super T>> T max(List<T> xs)` |
| PECS | read from `? extends T`, write into `? super T` |
| Factory instead of `new T()` | pass `Supplier<T>` or `Class<T>` |
| Default list, set, map | `ArrayList`, `HashSet`, `HashMap` |
| Keep insertion order | `LinkedHashSet`, `LinkedHashMap` |
| Sorted with range queries | `TreeSet.floor/ceiling`, `TreeMap.floorEntry/headMap` |
| Stack or queue | `ArrayDeque`: `push/pop` or `offer/poll` |
| Top k items | min-heap `PriorityQueue` of size k |
| LRU cache | `LinkedHashMap(16, 0.75f, true)` + `removeEldestEntry` |
| Counting and grouping | `map.merge(k, 1, Integer::sum)`, `map.computeIfAbsent(k, x -> new ArrayList<>()).add(v)` |
| Pre-size a map | `HashMap.newHashMap(expectedEntries)` (Java 19+) |
| Multi-key sort | `Comparator.comparing(P::price).thenComparing(P::name).reversed()` |
| Remove while iterating | `list.removeIf(...)`, `map.entrySet().removeIf(...)`, `Iterator.remove()` |
| Immutable copy | `List.copyOf(items)`, `Map.copyOf(m)`; constants with `List.of`, `Map.of` |
| Ends of an ordered collection | `getFirst()`, `getLast()`, `reversed()` (Java 21) |

### Practice

::: try
**1. Parse order lines with clear failures.** ShopSphere receives order lines as text like `"PEN-01,3"`. Write `OrderLine parse(String raw)` that returns a record `OrderLine(String sku, int qty)`. It must throw a custom unchecked `InvalidOrderLineException` (with the raw text in its message) when the line has no comma, when the quantity is not a number (keep the `NumberFormatException` as the cause), or when the quantity is not positive. Then parse `"PEN-01,3"`, `"INK-02,x"` and `"BOX"`, printing each result or each error message and cause.
:::

?? A solution to 1

```java
public class ParseLines {
    record OrderLine(String sku, int qty) { }

    static class InvalidOrderLineException extends RuntimeException {
        InvalidOrderLineException(String message, Throwable cause) { super(message, cause); }
        InvalidOrderLineException(String message) { super(message); }
    }

    static OrderLine parse(String raw) {
        String[] parts = raw.split(",");
        if (parts.length != 2) throw new InvalidOrderLineException("expected sku,qty but got '" + raw + "'");
        int qty;
        try {
            qty = Integer.parseInt(parts[1].trim());
        } catch (NumberFormatException e) {
            throw new InvalidOrderLineException("bad quantity in '" + raw + "'", e);
        }
        if (qty <= 0) throw new InvalidOrderLineException("quantity must be positive in '" + raw + "'");
        return new OrderLine(parts[0].trim(), qty);
    }

    public static void main(String[] args) {
        for (String raw : new String[] {"PEN-01,3", "INK-02,x", "BOX"}) {
            try {
                System.out.println(parse(raw));
            } catch (InvalidOrderLineException e) {
                System.out.println(e.getMessage() + (e.getCause() != null ? " (cause: " + e.getCause().getClass().getSimpleName() + ")" : ""));
            }
        }
    }
}
// Output:
// OrderLine[sku=PEN-01, qty=3]
// bad quantity in 'INK-02,x' (cause: NumberFormatException)
// expected sku,qty but got 'BOX'
```

::: try
**2. A PECS-friendly helper.** Write a generic method `transferIf(source, target, condition)` that moves every element matching the condition from `source` to `target` (removing it from `source`). Choose wildcard types so that you can move `Integer`s out of a `List<Integer>` into a `List<Number>` using a `Predicate<Object>`.
:::

?? A solution to 2

```java
import java.util.*;
import java.util.function.Predicate;

public class Transfer {
    static <T> int transferIf(Collection<? extends T> source, Collection<? super T> target,
                              Predicate<? super T> condition) {
        int moved = 0;
        Iterator<? extends T> it = source.iterator();
        while (it.hasNext()) {
            T item = it.next();
            if (condition.test(item)) {
                target.add(item);
                it.remove();               // safe removal during iteration
                moved++;
            }
        }
        return moved;
    }

    public static void main(String[] args) {
        List<Integer> qtys = new ArrayList<>(List.of(3, 0, 7, 0, 2));
        List<Number> zeroes = new ArrayList<>();
        Predicate<Object> isZero = o -> o.equals(0);
        int n = transferIf(qtys, zeroes, isZero);
        System.out.println(n + " moved, left " + qtys + ", target " + zeroes);
    }
}
// Output:
// 2 moved, left [3, 7, 2], target [0, 0]
```

`source` produces (`extends`), `target` consumes (`super`), and the predicate consumes too (`super`). Using the iterator's `remove` avoids a `ConcurrentModificationException`.

::: try
**3. Live-coding classic: top k best-selling products.** Given a log of sold SKUs (one entry per item sold), return the k most sold SKUs, most sold first, breaking ties by SKU alphabetically. Aim for O(n log k) after counting. Test with `PEN, INK, PEN, BOX, INK, PEN, TAPE, BOX, INK` and k = 2.
:::

?? A solution to 3

```java
import java.util.*;

public class TopK {
    static List<String> topK(List<String> sold, int k) {
        Map<String, Integer> counts = new HashMap<>();
        for (String sku : sold) counts.merge(sku, 1, Integer::sum);       // O(n)

        // "worst" entry on top: lowest count, then alphabetically LAST sku
        Comparator<Map.Entry<String, Integer>> worstFirst =
                Map.Entry.<String, Integer>comparingByValue()
                        .thenComparing(Map.Entry.<String, Integer>comparingByKey().reversed());
        PriorityQueue<Map.Entry<String, Integer>> heap = new PriorityQueue<>(worstFirst);
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            heap.offer(e);
            if (heap.size() > k) heap.poll();                               // O(log k)
        }

        List<String> result = new ArrayList<>();
        while (!heap.isEmpty()) result.add(heap.poll().getKey());
        return result.reversed();                                           // best first (Java 21)
    }

    public static void main(String[] args) {
        List<String> sold = List.of("PEN", "INK", "PEN", "BOX", "INK", "PEN", "TAPE", "BOX", "INK");
        System.out.println(topK(sold, 2));
        System.out.println(topK(sold, 3));
    }
}
// Output:
// [INK, PEN]
// [INK, PEN, BOX]
```

PEN and INK both sold 3, so the tie-break puts INK first. The heap holds at most k entries, so after the O(n) count the selection is O(m log k) for m distinct SKUs. In an interview, also mention the simpler O(m log m) alternative: sort all entries with the same comparator and take the first k.

::: try
**4. First non-repeating character.** Another interview favourite: given a customer's coupon code such as `"SAVESAVE10"`, return the first character that appears exactly once, or report that there is none. Do it in one pass for counting plus one pass over the counts.
:::

?? A solution to 4

```java
import java.util.*;

public class FirstUnique {
    static Optional<Character> firstUnique(String s) {
        Map<Character, Integer> counts = new LinkedHashMap<>();         // keeps first-seen order
        for (char c : s.toCharArray()) counts.merge(c, 1, Integer::sum);
        for (Map.Entry<Character, Integer> e : counts.entrySet()) {
            if (e.getValue() == 1) return Optional.of(e.getKey());
        }
        return Optional.empty();
    }

    public static void main(String[] args) {
        System.out.println(firstUnique("SAVESAVE10"));
        System.out.println(firstUnique("ABAB"));
    }
}
// Output:
// Optional[1]
// Optional.empty
```

`LinkedHashMap` is the key choice: a `HashMap` would lose the order, and a `TreeMap` would return the alphabetically first unique character instead of the first in the string. Time O(n), extra space O(distinct characters).

::: try
**5. Find the bugs.** A teammate's code is below. It is meant to drop out-of-stock SKUs and then look one up. Without running it, list each bug and how to fix it.

```java
Map<Sku, Integer> stock = new HashMap<>();   // Sku has a mutable 'code' field used in hashCode
// ... filled elsewhere ...
for (Sku s : stock.keySet()) {
    if (stock.get(s) == 0) stock.remove(s);
}
someSku.code = someSku.code.toUpperCase();
Integer qty = stock.get(someSku);
try {
    reserve(someSku, qty);
} catch (Exception e) { }
```
:::

?? Answer to 5
Three bugs. First, removing from the map while iterating over `keySet()` with for-each throws `ConcurrentModificationException` (or silently skips entries): use `stock.entrySet().removeIf(e -> e.getValue() == 0)` or `stock.values().removeIf(q -> q == 0)`. Second, `Sku` is a mutable key: changing `code` after insertion changes its hash, so `get` looks in the wrong bucket and returns `null`, and then `reserve(someSku, qty)` may hit a `NullPointerException` on unboxing. Make `Sku` immutable (a record) and create a new key instead. Third, `catch (Exception e) { }` swallows every failure, including that NPE: catch the specific business exception you can handle, and log or rethrow the rest with context.

### What comes next

You can now handle failures the way production code does, write type-safe generic APIs with the right wildcards, explain what erasure removes, and choose and justify every major collection, down to how `HashMap` places a key and why an iterator throws `ConcurrentModificationException`. The next section, **Core Java 3: functional Java, I/O and build tools**, builds on all of it: lambdas and the Stream API process these collections declaratively (`Comparator.comparing` was your first taste), `Optional` replaces many `null` returns, `java.nio.file` and try-with-resources handle files, and Maven and Gradle turn your code into a project that compiles, tests and packages with one command.
