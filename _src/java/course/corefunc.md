This section is the third and last step of Core Java. It assumes you have worked through **Core Java 1: language basics and OOP** and **Core Java 2: exceptions, generics and collections**: you can write classes, interfaces and records, you know what a functional interface is, you can read `List<? extends T>`, you know how `HashMap` and `ArrayList` work inside, and you have already sorted products with `Comparator.comparing`. Here you learn the modern, functional style that fills every Java codebase written since 2014, the date and money types that business code cannot get wrong, and the file, JSON and build tooling that turns a folder of classes into a real project.

**What you will be able to do afterwards**

- Write lambdas and method references fluently, and pick the right interface from `java.util.function` (`Function`, `Predicate`, `Consumer`, `Supplier`, `BiFunction` and their relatives).
- Solve the classic live-coding stream tasks on a whiteboard: word frequency, group by department, second highest salary, first non-repeated character, top N per group, flattening nested lists.
- Explain laziness, intermediate vs terminal operations and short-circuiting, and say when a parallel stream helps and when it silently produces wrong answers.
- Use `Optional` the way its designers intended and spot the anti-patterns in code review.
- Handle dates, times and time zones correctly with `java.time`, including daylight saving gaps and the `yyyy` vs `YYYY` trap.
- Read and write files with `java.nio.file`, parse a CSV file into records, convert objects to and from JSON with Jackson, and do exact money arithmetic with `BigDecimal`.
- Read and change a Maven `pom.xml` and a Gradle `build.gradle`: dependencies, scopes, plugins and multi-module builds.

**The big picture.** Think again of ShopSphere's order service. In Core Java 2 you stored orders in collections. Now you **query** them: "revenue per city", "the three best-selling products", "customers with no order in 30 days". Before Java 8 each of those was a `for` loop with a temporary map and a few `if`s. Since Java 8 you describe **what** you want as a pipeline (`orders.stream().filter(...).collect(groupingBy(...))`) and the library decides **how** to loop. Lambdas are the small pieces of behaviour you pass into that pipeline. Around it sit the boundaries of the program: orders arrive as CSV files or JSON from the website, prices must be exact to the paisa, timestamps must mean the same instant in Bengaluru and in London, and the whole project must build the same way on your laptop and on the CI server. The second group of this section covers those boundaries.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

> **Tip:** Keep `jshell` open (`jshell` in the JDK's `bin` folder). Streams are perfect for it: `Stream.of("pen", "ink", "pen").distinct().toList()` answers at once. Every complete program in this section also runs with `java Name.java`.

## Functional Java (Java 8+)

Java 8 (2014) added three things that work as a team. **Lambda expressions** let you write a piece of behaviour inline, as a value you can pass to a method. **Functional interfaces** give those values a type, so the compiler knows what the lambda takes and returns. The **Stream API** is the biggest user of both: a library of operations (`filter`, `map`, `collect`...) that take lambdas and process collections declaratively.

The seven topics go from the building blocks (lambdas and method references) through streams and collectors (the part interviewers test most, with many worked problems), to how streams run under the hood and when parallel streams hurt. Two topics complete the "modern Java" toolkit: `Optional` for values that may be absent, and the `java.time` API for dates and time zones.

### Lambda expressions & functional interfaces (Function, Predicate, Consumer, Supplier, BiFunction)

::: plain
A lambda is a small, nameless piece of behaviour that you hand to someone else to run. Think of a note you give a warehouse picker: "take only items heavier than 5 kg". You do not write a whole procedure manual (a class); you write one rule on a slip of paper, and the picker applies it to every item. The **functional interface** is the printed form the slip must fit: "a rule that takes an item and answers yes or no" is a `Predicate`, "a rule that turns an item into something else" is a `Function`.
:::

A **lambda expression** is an anonymous function: parameters, an arrow, and a body. A **functional interface** is an interface with exactly one abstract method (it may have any number of `default` and `static` methods). A lambda has no type of its own; the compiler gives it the type of the functional interface expected in that place, which is called the **target type**.

```java
// Forms of lambda syntax (fragments)
Runnable r = () -> System.out.println("hi");           // no parameters
Predicate<String> empty = s -> s.isEmpty();            // one parameter, no parentheses needed
Comparator<String> byLen = (a, b) -> Integer.compare(a.length(), b.length());
BiFunction<Integer, Integer, Integer> add = (Integer a, Integer b) -> a + b;  // explicit types
Function<String, String> tidy = s -> {                 // block body needs return
    String t = s.strip();
    return t.toLowerCase();
};
```

The same lambda text can have different types in different places: `s -> s.isEmpty()` is a `Predicate<String>` in one assignment and a `Function<String, Boolean>` in another. That is why `var f = s -> s.isEmpty();` does not compile: there is no target type ("cannot infer type for local variable f ... lambda expression needs an explicit target-type").

**The `java.util.function` package.** Instead of every library inventing its own interfaces, Java 8 shipped 43 standard ones. You need to know the core six by heart; the rest are variations.

| Interface | Method | Takes → returns | ShopSphere example |
|---|---|---|---|
| `Function<T, R>` | `R apply(T t)` | one value → a result | product → its price |
| `BiFunction<T, U, R>` | `R apply(T t, U u)` | two values → a result | (price, qty) → line total |
| `Predicate<T>` | `boolean test(T t)` | one value → yes/no | "is this order paid?" |
| `Consumer<T>` | `void accept(T t)` | one value → nothing (side effect) | send a confirmation email |
| `Supplier<T>` | `T get()` | nothing → a value | create a new empty basket |
| `UnaryOperator<T>` | `T apply(T t)` | `Function<T, T>` | apply a 10% discount to a price |
| `BinaryOperator<T>` | `T apply(T a, T b)` | `BiFunction<T, T, T>` | add two amounts |
| `BiConsumer<T, U>`, `BiPredicate<T, U>` | `accept`, `test` | two values | `map.forEach((k, v) -> ...)` |

Then there are **primitive specializations** that avoid boxing: `IntPredicate`, `IntFunction<R>`, `ToIntFunction<T>`, `IntUnaryOperator`, `IntBinaryOperator`, `IntSupplier`, `ObjIntConsumer<T>`, and the same for `long` and `double`. `ToIntFunction<Product>` is what `mapToInt` and `Comparator.comparingInt` take.

The program below builds from simple to realistic: the core interfaces on their own, then composed with their `default` methods, then passed into a method that accepts behaviour as a parameter.

```java
import java.math.BigDecimal;
import java.util.*;
import java.util.function.*;

public class Lambdas {
    record Product(String sku, String name, BigDecimal price, int stock) { }

    // A method that takes behaviour as a parameter: the "strategy" is a Predicate
    static List<Product> select(List<Product> all, Predicate<Product> rule) {
        List<Product> out = new ArrayList<>();
        for (Product p : all) if (rule.test(p)) out.add(p);
        return out;
    }

    public static void main(String[] args) {
        List<Product> catalogue = List.of(
                new Product("P1", "pen", new BigDecimal("49.00"), 120),
                new Product("P2", "notebook", new BigDecimal("85.00"), 0),
                new Product("P3", "ink", new BigDecimal("210.00"), 8));

        Function<Product, String> label = p -> p.name() + " @ " + p.price();
        Predicate<Product> inStock = p -> p.stock() > 0;
        Consumer<String> print = s -> System.out.println("  " + s);
        Supplier<List<String>> newList = ArrayList::new;          // a method reference (next topic)
        BiFunction<BigDecimal, Integer, BigDecimal> lineTotal =
                (price, qty) -> price.multiply(BigDecimal.valueOf(qty));

        System.out.println(label.apply(catalogue.get(0)));
        System.out.println(inStock.test(catalogue.get(1)));
        System.out.println(lineTotal.apply(new BigDecimal("49.00"), 3));
        List<String> names = newList.get();
        names.add("first");
        System.out.println(names);

        // Composition with default methods
        Predicate<Product> cheap = p -> p.price().compareTo(new BigDecimal("100")) < 0;
        System.out.println("in stock and cheap:");
        select(catalogue, inStock.and(cheap)).forEach(p -> print.accept(p.name()));
        System.out.println("out of stock:");
        select(catalogue, inStock.negate()).forEach(p -> print.accept(p.name()));

        Function<String, String> strip = String::strip;
        Function<String, String> upper = String::toUpperCase;
        System.out.println(strip.andThen(upper).apply("  gel pen "));   // strip first, then upper
        System.out.println(strip.compose(upper).apply("  gel pen "));   // upper first, then strip

        UnaryOperator<BigDecimal> discount = price -> price.multiply(new BigDecimal("0.90"));
        BinaryOperator<BigDecimal> plus = BigDecimal::add;
        System.out.println(plus.apply(discount.apply(new BigDecimal("200")), BigDecimal.TEN));
    }
}
// Output:
// pen @ 49.00
// false
// 147.00
// [first]
// in stock and cheap:
//   pen
// out of stock:
//   notebook
// GEL PEN
// GEL PEN
// 190.00
```

The last line shows that `BigDecimal` keeps track of decimal places: `200 × 0.90` gives `180.00` (scale 2), and adding `10` keeps the scale. The `BigDecimal` topic later explains scale in detail.

**Composition methods worth knowing.** `Function` has `andThen` (this, then that) and `compose` (that, then this), plus `Function.identity()`. `Predicate` has `and`, `or`, `negate`, `Predicate.not(p)` (Java 11, handy in streams: `filter(Predicate.not(String::isBlank))`) and `Predicate.isEqual(x)`. `Consumer` has `andThen`. `BinaryOperator` has `minBy(comparator)` and `maxBy(comparator)`.

**Writing your own functional interface.** Do it when none of the standard ones says what you mean, or when you need a checked exception:

```java
@FunctionalInterface
interface PriceRule {
    BigDecimal apply(BigDecimal price, String customerTier);
    default PriceRule then(PriceRule next) {
        return (price, tier) -> next.apply(apply(price, tier), tier);
    }
}
```

`@FunctionalInterface` is optional, but it makes the compiler reject a second abstract method ("Unexpected @FunctionalInterface annotation ... multiple non-overriding abstract methods found"), which protects every lambda that already implements it.

**Variable capture.** A lambda may read local variables from the enclosing method only if they are **effectively final** (never reassigned), as Core Java 1 explained. It can freely read and write fields. To count inside a lambda, do not reach for a one-element array trick; use a stream operation (`count()`, `sum()`) or, if you truly need shared mutable state, an `AtomicInteger`.

> **Pitfall:** Checked exceptions do not pass through standard functional interfaces. `paths.forEach(p -> Files.readString(p))` fails with "unreported exception IOException; must be caught or declared to be thrown", because `Consumer.accept` declares no `throws`. Catch inside the lambda and rethrow as `UncheckedIOException`, or move the body into a helper method that does that, so the lambda stays one line.

> **Tip:** Keep lambdas short. If a lambda grows past three lines, give it a name: extract a private method and use a method reference to it. Stack traces and code review both become easier.

::: deeper
**How a lambda is compiled.** Unlike an anonymous class, a lambda does not produce a `.class` file. `javac` moves the body into a private synthetic method and emits an `invokedynamic` instruction where the lambda appears. For `int min = 10; Predicate<Integer> big = q -> q > min;` the bytecode (`javap -c -p`) is:

```text
       3: iload_1
       4: invokedynamic #7,  0   // InvokeDynamic #0:test:(I)Ljava/util/function/Predicate;
       9: astore_2
  ...
  private static boolean lambda$main$0(int, java.lang.Integer);
```

The captured variable `min` becomes an extra first parameter of `lambda$main$0`. The first time the `invokedynamic` runs, the JVM calls a **bootstrap method**, `LambdaMetafactory.metafactory`, which spins a small hidden class implementing `Predicate` and returns a factory for it. Later executions reuse that factory. A **non-capturing** lambda (one that uses no local variables or `this`) is usually created once and reused as a singleton; a capturing lambda allocates a small object each time, although the JIT often removes even that through escape analysis.

**`this` in a lambda** means the enclosing instance, not the lambda. A lambda that uses an instance field captures `this`, so its synthetic method is an instance method. That matters for memory leaks: a long-lived lambda (for example a listener) keeps its whole enclosing object alive.

**Why not function types?** Java chose to reuse interfaces instead of adding types like `(int) -> boolean`, so that every existing single-method interface (`Runnable`, `Comparator`, `Callable`, `ActionListener`) became lambda-ready on day one. The cost is the explosion of primitive specializations, which Project Valhalla aims to reduce in a future release.

**Overload ambiguity.** `ExecutorService.submit` has overloads for `Runnable` and `Callable<T>`. `submit(() -> doWork())` picks `Callable` if `doWork()` returns a value and `Runnable` if it returns `void`. When two overloads take different functional interfaces of the same shape, javac reports "reference to X is ambiguous"; cast the lambda or give it explicit parameter types.

> **Interview:** "What is a functional interface? Name some from `java.util.function`." An interface with exactly one abstract method, so it can be the target type of a lambda or method reference. `@FunctionalInterface` makes the compiler check that. The core ones are `Function<T, R>` (apply), `Predicate<T>` (test), `Consumer<T>` (accept), `Supplier<T>` (get), `BiFunction<T, U, R>`, `UnaryOperator<T>` and `BinaryOperator<T>`; older examples are `Runnable`, `Callable` and `Comparator`.

> **Interview:** "Is a lambda just an anonymous class?" No. It compiles to `invokedynamic` and a private method instead of a separate class file, `this` refers to the enclosing object, it cannot have state of its own, and it can only implement an interface with one abstract method. An anonymous class can extend a class, have fields and implement several methods.
:::

::: check
?? Which functional interface fits "given an order, decide whether it qualifies for free delivery"? And "given a customer id, load the customer"?
`Predicate<Order>` for the yes/no rule, and `Function<String, Customer>` for the lookup.

?? Why does `var rule = p -> p.stock() > 0;` not compile?
A lambda needs a target type to know which functional interface it implements, and `var` gives none. Declare `Predicate<Product> rule = ...` instead.

?? What is the difference between `f.andThen(g)` and `f.compose(g)`?
`f.andThen(g)` runs `f` first and passes its result to `g`. `f.compose(g)` runs `g` first and passes its result to `f`.
:::

::: resources
- Docs: [Lambda Expressions (dev.java)](https://dev.java/learn/lambdas/) · the official tutorial series, from syntax to composition
- Docs: [java.util.function package (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/function/package-summary.html) · every standard functional interface
- Spec: [JLS 15.27: Lambda Expressions](https://docs.oracle.com/javase/specs/jls/se21/html/jls-15.html#jls-15.27) · the exact rules for target typing and capture
- Article: [Lambda Expressions and Functional Interfaces: Tips and Best Practices](https://www.baeldung.com/java-8-lambda-expressions-tips) · Baeldung on style and pitfalls
- Article: [Functional Interfaces in Java](https://www.baeldung.com/java-8-functional-interfaces) · Baeldung tour of `java.util.function`
- Book: *Modern Java in Action* by Raoul-Gabriel Urma, Mario Fusco and Alan Mycroft · chapter 3 covers lambdas in depth
:::

### Method references

::: plain
A method reference is a shortcut that says "use that existing method as the rule". Instead of writing a note that says "take the item and read its price off the label", you just point at the label reader that already exists. `Product::price` means exactly the same as `p -> p.price()`; it is shorter and says the intent at a glance.
:::

A **method reference** uses the `::` operator to turn an existing method or constructor into an instance of a functional interface. It is never required (a lambda can always do the same job), but when the lambda would only call one method and pass its parameters straight through, the reference is shorter and reads better.

There are four kinds. The trick to reading them is to ask: "where do the functional interface's parameters go?"

| Kind | Syntax | Equivalent lambda | Example |
|---|---|---|---|
| Static method | `Class::staticMethod` | `(a, b) -> Class.staticMethod(a, b)` | `Integer::parseInt` is `s -> Integer.parseInt(s)` |
| Instance method of a **particular** object (bound) | `object::method` | `x -> object.method(x)` | `System.out::println` is `x -> System.out.println(x)` |
| Instance method of an **arbitrary** object of a type (unbound) | `Class::instanceMethod` | `(obj, x) -> obj.method(x)` | `String::toUpperCase` is `s -> s.toUpperCase()` |
| Constructor | `Class::new` | `x -> new Class(x)` | `ArrayList::new`, `int[]::new` |

The unbound kind is the one that confuses people. In `String::toUpperCase` there is no `String` object yet; the **first parameter** of the functional interface becomes the object the method is called on. So `Function<String, String> f = String::toUpperCase` works (one parameter: the receiver), and `BiFunction<String, String, Boolean> eq = String::equalsIgnoreCase` works too (first parameter is the receiver, second is the argument). `Comparator<String> c = String::compareTo` is the same idea.

```java
import java.math.BigDecimal;
import java.util.*;
import java.util.function.*;

public class MethodRefs {
    record Product(String sku, String name, BigDecimal price) {
        static boolean isPremium(Product p) { return p.price().compareTo(new BigDecimal("200")) >= 0; }
    }

    static class PriceFormatter {
        private final String currency;
        PriceFormatter(String currency) { this.currency = currency; }
        String format(Product p) { return p.name() + ": " + currency + " " + p.price(); }
    }

    public static void main(String[] args) {
        List<Product> catalogue = List.of(
                new Product("P1", "pen", new BigDecimal("49.00")),
                new Product("P3", "ink", new BigDecimal("210.00")));

        // 1. static method
        Function<String, Integer> parse = Integer::parseInt;
        Predicate<Product> premium = Product::isPremium;
        System.out.println(parse.apply("42") + 1);
        System.out.println(premium.test(catalogue.get(1)));

        // 2. bound: a particular object
        PriceFormatter inr = new PriceFormatter("INR");
        Function<Product, String> fmt = inr::format;
        catalogue.stream().map(fmt).forEach(System.out::println);

        // 3. unbound: the first parameter becomes the receiver
        Function<Product, String> name = Product::name;
        BiFunction<String, String, Boolean> same = String::equalsIgnoreCase;
        System.out.println(name.apply(catalogue.get(0)) + " " + same.apply("PEN", "pen"));

        // 4. constructors, including arrays
        Supplier<List<Product>> fresh = ArrayList::new;
        BiFunction<String, BigDecimal, Product> make = (sku, price) -> new Product(sku, "new", price);
        Function<String, StringBuilder> sb = StringBuilder::new;
        IntFunction<String[]> arrays = String[]::new;
        List<Product> basket = fresh.get();
        basket.add(make.apply("P9", BigDecimal.ONE));
        System.out.println(basket.size() + " " + sb.apply("abc").reverse() + " " + arrays.apply(3).length);
        String[] names = catalogue.stream().map(Product::name).toArray(String[]::new);
        System.out.println(Arrays.toString(names));
    }
}
// Output:
// 43
// true
// pen: INR 49.00
// ink: INR 210.00
// pen true
// 1 cba 3
// [pen, ink]
```

Note that `make` stays a lambda: the record's constructor takes three arguments and `BiFunction` supplies two, so a method reference cannot fill the gap. When parameters do not pass straight through, write a lambda.

**When to choose which.** Use a method reference when it names the intent (`Product::price`, `String::isBlank`, `Objects::nonNull`, `BigDecimal::add`). Use a lambda when you need to do anything else: combine two calls, pass a constant, reorder parameters. `x -> x.getAddress().getCity()` cannot be a single method reference, and that is fine.

> **Pitfall:** Overloaded methods can make a reference ambiguous. `Function<Integer, String> f = Integer::toString;` fails with "incompatible types: invalid method reference", because both the static `Integer.toString(int)` and the instance `toString()` fit. Write the lambda `i -> i.toString()` or `String::valueOf`.

> **Tip:** `this::method` and `super::method` are bound references too. In a Spring service you will often see `orders.forEach(this::notifyCustomer)`.

::: deeper
**When the receiver is evaluated.** A bound reference evaluates its receiver **once, when the reference is created**; a lambda evaluates it **each time it runs**. That difference shows up with `null` and with variables that change:

```java
import java.util.function.Supplier;

public class BoundTiming {
    public static void main(String[] args) {
        String code = null;
        Supplier<Integer> lambda = () -> code.length();     // fine: nothing runs yet
        System.out.println("lambda created");
        try {
            Supplier<Integer> ref = code::length;           // throws right here
            System.out.println("never printed");
        } catch (NullPointerException e) {
            System.out.println("reference creation threw " + e.getClass().getSimpleName());
        }
    }
}
// Output:
// lambda created
// reference creation threw NullPointerException
```

`javac` inserts a call to `Objects.requireNonNull(code)` before the `invokedynamic` for the bound reference, so the failure happens at the line that creates it, not later when it is called. In bytecode both forms end up as `invokedynamic` with `LambdaMetafactory`; for a method reference javac can often point the factory straight at the target method (for example `String.length`) instead of generating a synthetic `lambda$...` method.

**Performance.** There is no meaningful difference between a lambda and the equivalent method reference once the JIT has compiled the code. Choose by readability.

**Generic method references.** You can supply type arguments explicitly: `Collections::<String>emptyList`. You rarely need to, because inference handles it.

> **Interview:** "What are the four kinds of method reference?" Static (`Integer::parseInt`), bound instance on a particular object (`System.out::println`, `this::save`), unbound instance on an arbitrary object of a type (`String::toUpperCase`, where the first argument becomes the receiver), and constructor (`ArrayList::new`, `int[]::new`).

> **Interview:** "Is `list.forEach(System.out::println)` the same as `list.forEach(x -> System.out.println(x))`?" Functionally yes. The only difference is that the reference evaluates `System.out` once, when it is created; the lambda reads the field on every call. That only matters if someone calls `System.setOut` in between.
:::

::: check
?? Rewrite `orders.stream().map(o -> o.customerId())` and `names.forEach(n -> System.out.println(n))` with method references.
`orders.stream().map(Order::customerId)` and `names.forEach(System.out::println)`.

?? In `BiFunction<String, String, Boolean> f = String::startsWith;`, which string is the receiver?
The first parameter: `f.apply("pen-01", "pen")` calls `"pen-01".startsWith("pen")`, which is `true`.

?? Why might `Supplier<Integer> s = text::length;` throw even though `s.get()` is never called?
A bound reference checks its receiver for `null` when it is created. If `text` is `null`, a `NullPointerException` is thrown on that line. The equivalent lambda would only fail when called.
:::

::: resources
- Docs: [Method References (dev.java)](https://dev.java/learn/lambdas/method-references/) · the four kinds with examples
- Docs: [Method References (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/javaOO/methodreferences.html) · Oracle's classic page with a comparison table
- Spec: [JLS 15.13: Method Reference Expressions](https://docs.oracle.com/javase/specs/jls/se21/html/jls-15.html#jls-15.13) · including when the receiver is evaluated
- Article: [Method References in Java](https://www.baeldung.com/java-method-references) · Baeldung walkthrough
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 43: prefer method references to lambdas (when clearer)
:::

### Streams API: map, filter, reduce, collect, sorted, distinct, limit, flatMap

::: plain
A stream is an assembly line for data. Items come off a source (a list, a file, a range of numbers), pass through stations that each do one job (throw away the damaged ones, repaint, sort, keep the first ten), and at the end something collects the finished items into a box or adds up a total. You describe the stations; Java runs the belt. Nothing is stored on the belt itself, and once the belt has run, it is used up.
:::

A **stream** (`java.util.stream.Stream<T>`) is a sequence of elements that supports a pipeline of operations. It is **not a collection**: it stores nothing, it does not change its source, and it can be consumed only once. Every pipeline has three parts:

1. A **source**: `list.stream()`, `Stream.of(a, b, c)`, `Arrays.stream(array)`, `IntStream.range(0, 10)`, `Files.lines(path)`, `str.chars()`, `Stream.iterate(...)`, `Stream.generate(...)`.
2. Zero or more **intermediate operations**, each returning a new stream: `filter`, `map`, `flatMap`, `distinct`, `sorted`, `limit`, `skip`, `peek`, `mapToInt`, `boxed`, `takeWhile`, `dropWhile`.
3. One **terminal operation**, which produces a result or a side effect and closes the stream: `collect`, `toList`, `reduce`, `forEach`, `count`, `sum`, `min`, `max`, `anyMatch`, `allMatch`, `noneMatch`, `findFirst`, `findAny`, `toArray`.

[[fig:corefunc_pipe]]

The core operations, one line each:

| Operation | What it does | Example |
|---|---|---|
| `filter(Predicate)` | keep elements that pass the test | `filter(o -> o.status().equals("PAID"))` |
| `map(Function)` | turn each element into one other element | `map(Order::customer)` |
| `flatMap(Function)` | turn each element into a stream and flatten them into one | `flatMap(o -> o.lines().stream())` |
| `distinct()` | drop duplicates (uses `equals`/`hashCode`) | names without repeats |
| `sorted()`, `sorted(Comparator)` | natural order, or by a comparator | `sorted(comparing(Product::price))` |
| `limit(n)`, `skip(n)` | first n elements, or all but the first n | top 3, pagination |
| `reduce(identity, BinaryOperator)` | fold all elements into one value | `reduce(BigDecimal.ZERO, BigDecimal::add)` |
| `collect(Collector)` | build a collection, map or string | next topic |
| `toList()` (Java 16) | collect into an unmodifiable list | `.toList()` |

Here they are on ShopSphere orders. Each order has lines; each line has a SKU, a quantity and a unit price.

```java
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.*;

public class StreamBasics {
    record OrderLine(String sku, int qty, BigDecimal unitPrice) {
        BigDecimal total() { return unitPrice.multiply(BigDecimal.valueOf(qty)); }
    }
    record Order(String id, String customer, String status, List<OrderLine> lines) {
        boolean paid() { return status.equals("PAID"); }
    }

    public static void main(String[] args) {
        List<Order> orders = List.of(
                new Order("O1", "asha", "PAID", List.of(
                        new OrderLine("PEN", 3, new BigDecimal("49.00")),
                        new OrderLine("INK", 1, new BigDecimal("210.00")))),
                new Order("O2", "ravi", "CANCELLED", List.of(
                        new OrderLine("PEN", 10, new BigDecimal("49.00")))),
                new Order("O3", "asha", "PAID", List.of(
                        new OrderLine("BOOK", 2, new BigDecimal("85.00")),
                        new OrderLine("PEN", 1, new BigDecimal("49.00")))),
                new Order("O4", "meena", "PAID", List.of(
                        new OrderLine("INK", 2, new BigDecimal("210.00")))));

        // filter + map + toList
        List<String> paidIds = orders.stream().filter(Order::paid).map(Order::id).toList();
        System.out.println(paidIds);

        // map + distinct + sorted
        System.out.println(orders.stream().map(Order::customer).distinct().sorted().toList());

        // flatMap: a stream of orders becomes a stream of lines
        System.out.println(orders.stream()
                .flatMap(o -> o.lines().stream())
                .map(OrderLine::sku)
                .distinct()
                .toList());

        // reduce: revenue from paid orders
        BigDecimal revenue = orders.stream()
                .filter(Order::paid)
                .flatMap(o -> o.lines().stream())
                .map(OrderLine::total)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        System.out.println("revenue " + revenue);

        // mapToInt + sum: pens sold in paid orders
        int pens = orders.stream()
                .filter(Order::paid)
                .flatMap(o -> o.lines().stream())
                .filter(l -> l.sku().equals("PEN"))
                .mapToInt(OrderLine::qty)
                .sum();
        System.out.println("pens " + pens);

        // sorted with a comparator + limit: the two biggest lines
        orders.stream()
                .flatMap(o -> o.lines().stream())
                .sorted(Comparator.comparing(OrderLine::total).reversed())
                .limit(2)
                .forEach(l -> System.out.println(l.sku() + " x" + l.qty() + " = " + l.total()));

        // matching and finding (short-circuit terminal operations)
        System.out.println(orders.stream().anyMatch(o -> o.status().equals("CANCELLED")));
        System.out.println(orders.stream().allMatch(o -> !o.lines().isEmpty()));
        System.out.println(orders.stream().filter(o -> o.customer().equals("asha")).count());
        Optional<Order> firstMeena = orders.stream().filter(o -> o.customer().equals("meena")).findFirst();
        System.out.println(firstMeena.map(Order::id).orElse("none"));
    }
}
// Output:
// [O1, O3, O4]
// [asha, meena, ravi]
// [PEN, INK, BOOK]
// revenue 996.00
// pens 4
// PEN x10 = 490.00
// INK x2 = 420.00
// true
// true
// 2
// O4
```

**`map` vs `flatMap`.** `map` is one in, one out. If the function returns a list or a stream, `map` gives you a stream **of** lists (`Stream<List<OrderLine>>`), which is rarely what you want. `flatMap` takes a function that returns a **stream** and splices all those streams into one (`Stream<OrderLine>`). Whenever you see "for each order, for each line", think `flatMap`.

**The three forms of `reduce`.**

- `reduce(identity, op)`: returns a `T`. The identity must be a true neutral value for `op` (`0` for addition, `1` for multiplication, `BigDecimal.ZERO` for `add`), or results will be wrong, especially in parallel.
- `reduce(op)`: no identity, returns `Optional<T>`, empty for an empty stream. `Stream.of(3, 9, 4).reduce(Integer::max)` is `Optional[9]`.
- `reduce(identity, accumulator, combiner)`: the result type differs from the element type, for example summing quantities straight from lines: `reduce(0, (sum, l) -> sum + l.qty(), Integer::sum)`. In practice `mapToInt(OrderLine::qty).sum()` says the same thing more clearly.

**Primitive streams.** `IntStream`, `LongStream` and `DoubleStream` avoid boxing and add `sum()`, `average()`, `min()`, `max()`, `summaryStatistics()` and `range`/`rangeClosed`. Move between worlds with `mapToInt`/`mapToObj`/`boxed()`/`asDoubleStream()`.

#### Worked problems: the live-coding favourites

Interviewers love short stream puzzles on plain numbers and strings, because they show whether you think in pipelines. Read each problem, try it in `jshell`, then compare.

```java
import java.util.*;
import java.util.stream.*;

public class StreamDrills {
    public static void main(String[] args) {
        List<Integer> nums = List.of(5, 3, 8, 1, 8, 6, 3, 10);

        // 1. Sum of the squares of the even numbers
        System.out.println(nums.stream().filter(n -> n % 2 == 0).mapToInt(n -> n * n).sum());

        // 2. Second highest distinct number
        System.out.println(nums.stream().distinct().sorted(Comparator.reverseOrder())
                .skip(1).findFirst().orElseThrow());

        // 3. Remove duplicates, keep first-seen order
        System.out.println(nums.stream().distinct().toList());

        // 4. Max, min, average in one pass
        IntSummaryStatistics st = nums.stream().mapToInt(Integer::intValue).summaryStatistics();
        System.out.println(st.getMax() + " " + st.getMin() + " " + st.getAverage());

        // 5. Flatten a list of lists
        List<List<String>> baskets = List.of(List.of("pen", "ink"), List.of(), List.of("book", "pen"));
        System.out.println(baskets.stream().flatMap(List::stream).toList());

        // 6. Words longer than 3 letters, upper-cased, sorted by length then alphabetically
        List<String> words = List.of("gel", "notebook", "stapler", "ink", "marker", "eraser");
        System.out.println(words.stream().filter(w -> w.length() > 3).map(String::toUpperCase)
                .sorted(Comparator.comparingInt(String::length).thenComparing(Comparator.naturalOrder()))
                .toList());

        // 7. Count the vowels in a sentence
        String s = "ShopSphere ships orders";
        System.out.println(s.toLowerCase().chars().filter(c -> "aeiou".indexOf(c) >= 0).count());

        // 8. Reverse each word but keep the word order
        System.out.println(Arrays.stream("free delivery today".split(" "))
                .map(w -> new StringBuilder(w).reverse().toString())
                .collect(Collectors.joining(" ")));

        // 9. Is it a palindrome? (compare i-th char with its mirror)
        String p = "racecar";
        System.out.println(IntStream.range(0, p.length() / 2)
                .allMatch(i -> p.charAt(i) == p.charAt(p.length() - 1 - i)));

        // 10. First 10 Fibonacci numbers
        System.out.println(Stream.iterate(new long[]{0, 1}, f -> new long[]{f[1], f[0] + f[1]})
                .limit(10).map(f -> f[0]).toList());

        // 11. Product of 1..10 (factorial) with reduce
        System.out.println(LongStream.rangeClosed(1, 10).reduce(1, (a, b) -> a * b));

        // 12. Numbers 1..50 divisible by 7, as one comma-separated string
        System.out.println(IntStream.rangeClosed(1, 50).filter(n -> n % 7 == 0)
                .mapToObj(String::valueOf).collect(Collectors.joining(",")));
    }
}
// Output:
// 264
// 8
// [5, 3, 8, 1, 6, 10]
// 10 1 5.5
// [pen, ink, book, pen]
// [ERASER, MARKER, STAPLER, NOTEBOOK]
// 6
// eerf yreviled yadot
// true
// [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
// 3628800
// 7,14,21,28,35,42,49
```

Problem 1 squares 8, 8, 6 and 10: 64 + 64 + 36 + 100 = 264 (the duplicate 8 counts twice, because nothing removed it). Problem 2 is the classic "second highest salary": `distinct()` first, otherwise two copies of the highest value would make the answer the highest again.

> **Pitfall:** `stream.toList()` (Java 16) returns an **unmodifiable** list; `collect(Collectors.toList())` today returns an `ArrayList` but promises nothing. If the caller will add to the result, use `collect(Collectors.toCollection(ArrayList::new))`. Calling `add` on the `toList()` result throws `UnsupportedOperationException`.

> **Pitfall:** Do not use `forEach` to build results: `orders.stream().forEach(o -> ids.add(o.id()))` works sequentially but is a race in parallel, and it hides intent. Use `map(...).toList()` or a collector.

> **Tip:** Format pipelines one operation per line, starting each line with the dot. Diffs become one-line changes and each step can be read on its own.

::: deeper
**Streams vs loops.** A stream pipeline is not magic: for small lists a plain `for` loop is just as fast or slightly faster, because the stream sets up a chain of objects. The JIT inlines most of the overhead away for hot code. The real wins are readability, composability, and the option to go parallel. Some things remain easier with loops: early exit with complex state, checked exceptions, and updating several variables at once.

**Useful newer operations.**

- `takeWhile(pred)` / `dropWhile(pred)` (Java 9): take or skip elements while a condition holds, then stop. On sorted data they are a cheap range query.
- `Stream.iterate(seed, hasNext, next)` (Java 9): a stream version of a `for` loop, for example `Stream.iterate(1, i -> i <= 1000, i -> i * 2)`.
- `Stream.ofNullable(x)` (Java 9): an empty stream for `null`, handy inside `flatMap`.
- `mapMulti` (Java 16): like `flatMap` but pushes zero or more elements to a consumer instead of creating a stream, cheaper when each element produces few outputs.
- `toList()` (Java 16): unmodifiable, and unlike `Collectors.toUnmodifiableList()` it allows `null` elements.
- **Stream gatherers** (`Stream.gather`, finalized in Java 24 by JEP 485, so available in Java 25 LTS): custom intermediate operations such as `Gatherers.windowFixed(3)` (chunks of three) and `Gatherers.fold`. Not available in Java 21 without preview flags.

**Encounter order.** A stream from a `List` or an array is **ordered**; from a `HashSet` it is not. `sorted`, `distinct`, `limit` and `findFirst` respect encounter order when it exists, which costs extra work in parallel streams. `unordered()` tells the library you do not care.

**Primitive streams and `boxed`.** `IntStream.range(0, 5).toList()` does not compile, because `IntStream` has no `toList`. Use `boxed().toList()` or `toArray()`.

> **Interview:** "Difference between `map` and `flatMap`?" `map` applies a one-to-one function and keeps the nesting (`Stream<List<T>>` if the function returns a list). `flatMap` applies a one-to-many function that returns a stream and flattens all results into a single stream (`Stream<T>`). Use it to go from orders to order lines, or from sentences to words.

> **Interview:** "Is a stream a data structure? Can you reuse it?" No. A stream is a pipeline over a source; it holds no data and does not modify the source. It can be traversed only once: a second terminal operation throws `IllegalStateException: stream has already been operated upon or closed`. Create a new stream from the source instead, or keep a `Supplier<Stream<T>>`.
:::

::: check
?? What does `List.of(List.of(1, 2), List.of(3)).stream().map(List::size).toList()` return, and what does the `flatMap(List::stream)` version return?
`map` returns `[2, 1]` (one size per inner list). `flatMap(List::stream).toList()` returns `[1, 2, 3]`.

?? Find the second highest distinct salary from `List<Integer> salaries` with one pipeline.
`salaries.stream().distinct().sorted(Comparator.reverseOrder()).skip(1).findFirst()`, which returns an `Optional<Integer>` (empty if there are fewer than two distinct values).

?? Why does `Stream.of(1, 2, 3).reduce(10, Integer::sum)` give 16, and why is 10 a bad identity?
The identity is used as the starting value, so 10 + 1 + 2 + 3 = 16. An identity must not change the result (`0` for sum). In a parallel stream the identity may be used once per chunk, so the result would even change with the number of threads.
:::

::: resources
- Docs: [The Stream API (dev.java)](https://dev.java/learn/api/streams/) · official tutorial series on creating, mapping, reducing and collecting
- Docs: [java.util.stream package summary (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/package-summary.html) · the reference for laziness, ordering and non-interference
- Docs: [Stream (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/Stream.html) · every operation with examples
- Article: [The Java 8 Stream API Tutorial](https://www.baeldung.com/java-8-streams) · Baeldung overview
- Article: [Java Stream API Interview Questions](https://www.baeldung.com/java-8-interview-questions) · Baeldung list of common questions
- Book: *Modern Java in Action* by Raoul-Gabriel Urma, Mario Fusco and Alan Mycroft · chapters 4 and 5 on streams
:::

### Collectors: groupingBy, partitioningBy, toMap, joining, counting, summingInt

::: plain
If a stream is the assembly line, a collector is the packing station at the end. It decides what the finished goods go into: one big box (a list), a box with no duplicates (a set), a row of labelled bins (a map grouped by city), two bins marked "yes" and "no" (a partition), or a single sticker with everything written on it (a joined string). Bins can even contain smaller packing stations: "per city, count the orders" or "per category, add up the quantities".
:::

`collect(Collector)` is the most flexible terminal operation. A **collector** describes how to accumulate elements into a result: which container to start with, how to add one element, how to merge two partial containers (for parallel streams) and how to finish. You almost never write one yourself; the `Collectors` class has factories for every common case, and they **compose**: many of them take a second, **downstream** collector that says what to do with each group.

| Collector | Result | Typical use |
|---|---|---|
| `toList()`, `toSet()`, `toCollection(TreeSet::new)` | collection | gather elements |
| `toMap(keyFn, valueFn)` | `Map<K, V>` | index by id |
| `toMap(keyFn, valueFn, mergeFn)` | `Map<K, V>` | duplicate keys allowed, merged |
| `toMap(keyFn, valueFn, mergeFn, TreeMap::new)` | chosen map type | sorted or ordered keys |
| `groupingBy(classifier)` | `Map<K, List<T>>` | orders per city |
| `groupingBy(classifier, downstream)` | `Map<K, D>` | count, sum, set... per group |
| `groupingBy(classifier, mapFactory, downstream)` | chosen map type | sorted groups (`TreeMap::new`) |
| `partitioningBy(predicate[, downstream])` | `Map<Boolean, ...>` | two groups: true and false |
| `counting()` | `Long` | how many |
| `summingInt`, `summingLong`, `summingDouble` | number | totals |
| `averagingInt`, `averagingDouble` | `Double` | averages |
| `minBy(cmp)`, `maxBy(cmp)` | `Optional<T>` | best per group |
| `mapping(fn, downstream)` | downstream result | collect a field of each element |
| `filtering(pred, downstream)`, `flatMapping(fn, downstream)` (Java 9) | downstream result | filter or flatten inside a group |
| `reducing(identity, mapper, op)` | `T` | sums of `BigDecimal` per group |
| `joining(sep[, prefix, suffix])` | `String` | "pen, ink, book" |
| `collectingAndThen(downstream, finisher)` | finisher result | unwrap an `Optional`, make unmodifiable |
| `teeing(c1, c2, merger)` (Java 12) | merged result | two aggregates in one pass |

[[fig:corefunc_group]]

The tour below uses ShopSphere sale lines: which city ordered, which category and SKU, how many, and the amount.

```java
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.*;
import static java.util.stream.Collectors.*;

public class CollectorsTour {
    record Sale(String orderId, String city, String category, String sku, int qty, BigDecimal amount) { }

    static void show(Object o) { System.out.println(o); }   // see the note after the output

    public static void main(String[] args) {
        List<Sale> sales = List.of(
                new Sale("O1", "Pune", "stationery", "PEN", 3, new BigDecimal("147.00")),
                new Sale("O1", "Pune", "stationery", "INK", 1, new BigDecimal("210.00")),
                new Sale("O2", "Delhi", "books", "NOVEL", 2, new BigDecimal("598.00")),
                new Sale("O3", "Pune", "books", "ATLAS", 1, new BigDecimal("899.00")),
                new Sale("O4", "Chennai", "stationery", "PEN", 10, new BigDecimal("490.00")),
                new Sale("O5", "Delhi", "stationery", "PEN", 2, new BigDecimal("98.00")));

        // groupingBy alone: city -> list of sales (TreeMap for a stable print order)
        Map<String, List<Sale>> byCity = sales.stream().collect(groupingBy(Sale::city, TreeMap::new, toList()));
        byCity.forEach((city, list) -> System.out.println(city + " " + list.size()));

        // downstream collectors
        show(sales.stream().collect(groupingBy(Sale::city, TreeMap::new, counting())));
        show(sales.stream().collect(groupingBy(Sale::sku, TreeMap::new, summingInt(Sale::qty))));
        show(sales.stream().collect(groupingBy(Sale::category, TreeMap::new,
                mapping(Sale::city, toCollection(TreeSet::new)))));
        show(sales.stream().collect(groupingBy(Sale::city, TreeMap::new,
                reducing(BigDecimal.ZERO, Sale::amount, BigDecimal::add))));
        show(sales.stream().collect(groupingBy(Sale::category, TreeMap::new, averagingInt(Sale::qty))));

        // biggest sale per category, unwrapped from Optional
        show(sales.stream().collect(groupingBy(Sale::category, TreeMap::new,
                collectingAndThen(maxBy(Comparator.comparing(Sale::amount)), o -> o.get().sku()))));

        // partitioningBy: always has both keys, false and true
        Map<Boolean, List<String>> big = sales.stream().collect(partitioningBy(
                s -> s.amount().compareTo(new BigDecimal("500")) >= 0, mapping(Sale::sku, toList())));
        System.out.println(big);

        // toMap with a merge function: qty per SKU (duplicate keys add up)
        Map<String, Integer> qtyBySku = sales.stream()
                .collect(toMap(Sale::sku, Sale::qty, Integer::sum, LinkedHashMap::new));
        System.out.println(qtyBySku);

        // joining
        System.out.println(sales.stream().map(Sale::sku).distinct().sorted().collect(joining(", ", "[", "]")));

        // teeing: two results in one pass, merged
        String summary = sales.stream().collect(teeing(
                counting(),
                reducing(BigDecimal.ZERO, Sale::amount, BigDecimal::add),
                (n, total) -> n + " lines, total " + total));
        System.out.println(summary);
    }
}
// Output:
// Chennai 1
// Delhi 2
// Pune 3
// {Chennai=1, Delhi=2, Pune=3}
// {ATLAS=1, INK=1, NOVEL=2, PEN=15}
// {books=[Delhi, Pune], stationery=[Chennai, Delhi, Pune]}
// {Chennai=490.00, Delhi=696.00, Pune=1256.00}
// {books=1.5, stationery=4.0}
// {books=ATLAS, stationery=PEN}
// {false=[PEN, INK, PEN, PEN], true=[NOVEL, ATLAS]}
// {PEN=15, INK=1, NOVEL=2, ATLAS=1}
// [ATLAS, INK, NOVEL, PEN]
// 6 lines, total 2442.00
```

Why the little `show` helper? Writing `System.out.println(stream.collect(groupingBy(..., TreeMap::new, ...)))` directly does not compile: `println` is overloaded (`String`, `char[]`, `Object`...), and javac tries to infer the collector's result type from each overload, failing with "method println in class PrintStream cannot be applied to given types". Assigning the result to a variable first, or passing it to a method that takes only `Object`, gives inference a single target. You will meet this when experimenting.

Three details to notice. `counting()` returns a `Long`, not an `int`, so the map is `Map<String, Long>`. `groupingBy` without a map factory returns a `HashMap`, whose print order is not guaranteed; pass `TreeMap::new` for sorted keys or `LinkedHashMap::new` for first-seen order. And `partitioningBy` always contains both `false` and `true` keys, even when one side is empty, while `groupingBy` only creates keys that occur.

#### Worked problems: the collectors interview set

These are the tasks you are most likely to meet in a live-coding round. Each answer is one pipeline. The employee problems use the classic HR shape that interviewers often start from.

```java
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;
import static java.util.stream.Collectors.*;

public class CollectorDrills {
    record Employee(String name, String dept, int salary) { }

    static void show(Object o) { System.out.println(o); }

    public static void main(String[] args) {
        // 1. Word frequency, most frequent first, ties alphabetical
        String text = "the pen is on the desk and the ink is on the pen";
        Map<String, Long> freq = Arrays.stream(text.split("\\s+"))
                .collect(groupingBy(Function.identity(), counting()));
        freq.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(3)
                .forEach(e -> System.out.print(e.getKey() + "=" + e.getValue() + " "));
        System.out.println();

        // 2. Character frequency in first-seen order
        String word = "programming";
        Map<Character, Long> chars = word.chars().mapToObj(c -> (char) c)
                .collect(groupingBy(Function.identity(), LinkedHashMap::new, counting()));
        System.out.println(chars);

        // 3. First non-repeated character (reuses the LinkedHashMap idea)
        String s = "swiss";
        Character firstUnique = s.chars().mapToObj(c -> (char) c)
                .collect(groupingBy(Function.identity(), LinkedHashMap::new, counting()))
                .entrySet().stream().filter(e -> e.getValue() == 1)
                .map(Map.Entry::getKey).findFirst().orElse(null);
        System.out.println(firstUnique);

        // 4. Duplicate elements in a list
        List<String> skus = List.of("PEN", "INK", "PEN", "BOOK", "INK", "PEN");
        System.out.println(skus.stream().collect(groupingBy(Function.identity(), TreeMap::new, counting()))
                .entrySet().stream().filter(e -> e.getValue() > 1).map(Map.Entry::getKey).toList());

        // 5. Group anagrams
        List<String> ws = List.of("listen", "silent", "enlist", "pots", "stop", "tops", "ink");
        System.out.println(ws.stream().collect(groupingBy(w -> {
            char[] c = w.toCharArray(); Arrays.sort(c); return new String(c);
        }, TreeMap::new, toList())).values());

        // 6. Partition numbers into even and odd
        System.out.println(IntStream.rangeClosed(1, 8).boxed().collect(partitioningBy(n -> n % 2 == 0)));

        List<Employee> staff = List.of(
                new Employee("Asha", "IT", 95_000), new Employee("Ravi", "IT", 120_000),
                new Employee("Meena", "HR", 70_000), new Employee("John", "IT", 120_000),
                new Employee("Lata", "Sales", 65_000), new Employee("Kiran", "HR", 82_000),
                new Employee("Arun", "Sales", 91_000));

        // 7. Head count per department, sorted by department
        show(staff.stream().collect(groupingBy(Employee::dept, TreeMap::new, counting())));

        // 8. Average salary per department
        show(staff.stream().collect(groupingBy(Employee::dept, TreeMap::new,
                averagingInt(Employee::salary))));

        // 9. Highest-paid employee per department (name only)
        show(staff.stream().collect(groupingBy(Employee::dept, TreeMap::new,
                collectingAndThen(maxBy(Comparator.comparingInt(Employee::salary)), e -> e.get().name()))));

        // 10. Second highest distinct salary in the company
        System.out.println(staff.stream().map(Employee::salary).distinct()
                .sorted(Comparator.reverseOrder()).skip(1).findFirst().orElseThrow());

        // 11. Names per department, comma-separated, alphabetical
        show(staff.stream().sorted(Comparator.comparing(Employee::name))
                .collect(groupingBy(Employee::dept, TreeMap::new, mapping(Employee::name, joining(",")))));

        // 12. Top 2 earners per department
        show(staff.stream().collect(groupingBy(Employee::dept, TreeMap::new,
                collectingAndThen(toList(), list -> list.stream()
                        .sorted(Comparator.comparingInt(Employee::salary).reversed()
                                .thenComparing(Employee::name))
                        .limit(2).map(Employee::name).toList()))));

        // 13. Department with the highest total salary
        show(staff.stream().collect(groupingBy(Employee::dept, summingInt(Employee::salary)))
                .entrySet().stream().max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElseThrow());

        // 14. Name -> salary map, sorted by name
        show(staff.stream().collect(toMap(Employee::name, Employee::salary, (a, b) -> a, TreeMap::new)));
    }
}
// Output:
// the=4 is=2 on=2 
// {p=1, r=2, o=1, g=2, a=1, m=2, i=1, n=1}
// w
// [INK, PEN]
// [[listen, silent, enlist], [ink], [pots, stop, tops]]
// {false=[1, 3, 5, 7], true=[2, 4, 6, 8]}
// {HR=2, IT=3, Sales=2}
// {HR=76000.0, IT=111666.66666666667, Sales=78000.0}
// {HR=Kiran, IT=Ravi, Sales=Arun}
// 95000
// {HR=Kiran,Meena, IT=Asha,John,Ravi, Sales=Arun,Lata}
// {HR=[Kiran, Meena], IT=[John, Ravi], Sales=[Arun, Lata]}
// IT
// {Arun=91000, Asha=95000, John=120000, Kiran=82000, Lata=65000, Meena=70000, Ravi=120000}
```

Walk through the ones that trip people up:

- **Problem 1** needs `Map.Entry.<String, Long>comparingByValue()` with explicit type arguments, because `.reversed()` stops inference (the same issue the Comparator topic showed for lambdas).
- **Problem 3** works because `LinkedHashMap` keeps first-seen order, so the first entry with count 1 is the first non-repeated character. With a plain `HashMap` the answer could be any unique character.
- **Problem 9**: Ravi and John both earn 120,000. `maxBy` keeps the **first** maximum it meets, so the answer depends on encounter order. Interviewers like to ask "what about ties?": answer with a tie-breaker (`thenComparing(Employee::name)`) or return all of them (`groupingBy` on salary inside each department).
- **Problem 10**: `distinct()` is what makes 95,000 (not 120,000) the second highest.
- **Problem 5**: the key is the sorted letters; `TreeMap` makes the output order predictable.

> **Pitfall:** `toMap(keyFn, valueFn)` without a merge function throws `IllegalStateException: Duplicate key PEN (attempted merging values 3 and 10)` as soon as two elements share a key. In real data keys repeat more often than you think. Always ask "can this key repeat?" and pass a merge function (`Integer::sum`, `(a, b) -> a` to keep the first, `(a, b) -> b` to keep the last).

> **Pitfall:** `toMap` also throws `NullPointerException` if a **value** is `null`, because it uses `Map.merge` internally. `groupingBy` throws if the **key** is `null` ("element cannot be mapped to a null key"). Filter or replace nulls first.

> **Tip:** `import static java.util.stream.Collectors.*;` makes nested collectors readable: `groupingBy(Sale::city, counting())` instead of `Collectors.groupingBy(Sale::city, Collectors.counting())`.

::: deeper
**What a collector is made of.** `Collector<T, A, R>` has four functions: a `supplier` (create a container `A`), an `accumulator` (add a `T` to an `A`), a `combiner` (merge two `A`s, used by parallel streams) and a `finisher` (turn `A` into the result `R`), plus `characteristics` such as `IDENTITY_FINISH`, `UNORDERED` and `CONCURRENT`. You can build one with `Collector.of(...)`; for example a collector that sums `BigDecimal` amounts is `Collector.of(() -> new BigDecimal[]{BigDecimal.ZERO}, (a, s) -> a[0] = a[0].add(s.amount()), (a, b) -> { a[0] = a[0].add(b[0]); return a; }, a -> a[0])`, though `reducing` already does that.

**Three-argument `collect`.** `stream.collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)` takes the supplier, accumulator and combiner directly. It is the mutable-reduction counterpart of `reduce`: `reduce` builds a new value at each step, `collect` mutates a container, which is far cheaper for lists, maps and builders.

**`groupingBy` internals.** It calls `map.computeIfAbsent(key, k -> downstream.supplier().get())` and then the downstream accumulator, exactly the pattern Core Java 2 taught for manual grouping. In parallel streams each thread builds its own map and the combiner merges them key by key, which is expensive for large maps; `groupingByConcurrent` uses one shared `ConcurrentHashMap` instead, at the cost of encounter order.

**Unmodifiable results.** `toUnmodifiableList`, `toUnmodifiableSet` and `toUnmodifiableMap` (Java 10) return unmodifiable collections and reject `null`s. `collectingAndThen(toList(), Collections::unmodifiableList)` was the Java 8 way.

**Counting with a primitive.** `counting()` boxes to `Long`. If you need `Map<String, Integer>`, use `summingInt(x -> 1)` or `collectingAndThen(counting(), Long::intValue)`.

> **Interview:** "Count the occurrences of each word in a sentence using streams." `Arrays.stream(text.split("\\s+")).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))`. Mention `LinkedHashMap::new` as the map factory if order matters, `toLowerCase` and stripping punctuation if the input is real text, and sorting entries by `comparingByValue().reversed()` for a top-N list.

> **Interview:** "Difference between `groupingBy` and `partitioningBy`?" `partitioningBy` takes a `Predicate` and always returns a map with exactly two keys, `true` and `false` (both present even if empty). `groupingBy` takes any classifier function and creates one key per distinct value that actually occurs. Both accept a downstream collector.

> **Interview:** "What happens with duplicate keys in `Collectors.toMap`?" Without a merge function it throws `IllegalStateException` naming the duplicate key. Pass a third argument, a `BinaryOperator` that decides the value, and a fourth if you need a specific map type.
:::

::: check
?? Write one pipeline that returns total quantity per SKU from `List<Sale>` as a `TreeMap<String, Integer>`.
`sales.stream().collect(Collectors.groupingBy(Sale::sku, TreeMap::new, Collectors.summingInt(Sale::qty)))`, or `toMap(Sale::sku, Sale::qty, Integer::sum, TreeMap::new)`.

?? What type does `groupingBy(Employee::dept, maxBy(comparingInt(Employee::salary)))` produce, and how do you get plain employees instead?
`Map<String, Optional<Employee>>`. Wrap the downstream in `collectingAndThen(maxBy(...), Optional::get)`, or use `toMap(Employee::dept, Function.identity(), BinaryOperator.maxBy(comparingInt(Employee::salary)))`.

?? You need the first non-repeated character of a string. Why must the map be a `LinkedHashMap`?
Because you pick the first entry whose count is 1, and only a `LinkedHashMap` keeps the characters in the order they first appeared. A `HashMap` iterates in hash order, so you might return a later unique character.
:::

::: resources
- Docs: [Collectors (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/Collectors.html) · every factory with short examples
- Docs: [Using Collectors as terminal operations (dev.java)](https://dev.java/learn/api/streams/using-collectors/) · official tutorial on collectors
- Docs: [Creating your own collector (dev.java)](https://dev.java/learn/api/streams/custom-collectors/) · supplier, accumulator, combiner, finisher
- Article: [Guide to Java Collectors](https://www.baeldung.com/java-8-collectors) · Baeldung tour of every collector
- Article: [Guide to Java groupingBy Collector](https://www.baeldung.com/java-groupingby-collector) · Baeldung deep dive with downstream collectors
- Book: *Modern Java in Action* by Raoul-Gabriel Urma, Mario Fusco and Alan Mycroft · chapter 6, collecting data with streams
:::

### Lazy evaluation, intermediate vs terminal operations, parallel streams & their pitfalls

::: plain
A stream pipeline is a recipe, not a meal. Writing `filter(...).map(...)` only writes down the steps; nothing is cooked until someone orders the dish, which is the terminal operation. And the kitchen is smart: if you only ordered the first three plates, it stops cooking after three. A parallel stream is the same recipe handed to several cooks at once. That is faster for a banquet, slower for one sandwich, and a disaster if the cooks all write on the same notepad.
:::

**Intermediate operations are lazy.** `filter`, `map`, `sorted` and the rest return a new stream immediately and do no work; they only record a step. **Terminal operations are eager**: `collect`, `forEach`, `count`, `findFirst` and the rest trigger the whole pipeline. A pipeline without a terminal operation does nothing at all.

| | Intermediate | Terminal |
|---|---|---|
| Returns | another `Stream` | a result, `Optional`, or `void` |
| Runs when | never on its own | immediately, pulling elements through all steps |
| Examples | `filter`, `map`, `flatMap`, `distinct`, `sorted`, `peek`, `limit`, `skip` | `collect`, `toList`, `reduce`, `forEach`, `count`, `anyMatch`, `findFirst`, `min` |
| Stateless or stateful | stateless (`filter`, `map`) or stateful (`distinct`, `sorted`, `limit`) | n/a |
| Short-circuiting | `limit`, `takeWhile` | `anyMatch`, `allMatch`, `noneMatch`, `findFirst`, `findAny` |

**Elements travel one at a time, vertically.** A stream does not run `filter` over the whole list, then `map` over the whole result. It pushes the first element through every step, then the second, and so on. Combined with short-circuiting, that means a pipeline may look at only a few elements of a huge source. `peek` (an intermediate operation meant for debugging) makes it visible:

```java
import java.util.List;

public class Laziness {
    public static void main(String[] args) {
        List<String> skus = List.of("pen-01", "ink-02", "PAD-03", "pen-04", "BOX-05", "PIN-06");

        var pipeline = skus.stream()
                .peek(s -> System.out.println("read   " + s))
                .filter(s -> Character.isUpperCase(s.charAt(0)))
                .peek(s -> System.out.println("passed " + s))
                .map(String::toLowerCase);
        System.out.println("pipeline built, nothing read yet");

        System.out.println("first two: " + pipeline.limit(2).toList());

        // a stateful operation is a barrier: sorted must see everything first
        skus.stream()
                .peek(s -> System.out.print("[" + s.charAt(0) + "]"))
                .sorted()
                .findFirst()
                .ifPresent(s -> System.out.println(" smallest " + s));
    }
}
// Output:
// pipeline built, nothing read yet
// read   pen-01
// read   ink-02
// read   PAD-03
// passed PAD-03
// read   pen-04
// read   BOX-05
// passed BOX-05
// first two: [pad-03, box-05]
// [p][i][P][p][B][P] smallest BOX-05
```

Read the output closely. Nothing printed until `toList()` ran. Elements went through `read` and `passed` one by one. After `BOX-05` produced the second result, `limit(2)` short-circuited and `PIN-06` was never read. In the second pipeline `sorted()` had to buffer all six elements before `findFirst` could get anything: stateful operations like `sorted` and `distinct` break the one-at-a-time flow and need memory proportional to the input. (Uppercase letters sort before lowercase, so `BOX-05` is smallest.)

Laziness is what makes infinite streams usable: `Stream.iterate(1, n -> n * 2).filter(n -> n > 1000).findFirst()` is fine, because `findFirst` stops the infinite source. `Stream.iterate(1, n -> n * 2).sorted().findFirst()` never ends, because `sorted` waits for the end of an endless stream.

> **Pitfall:** A stream can be consumed once. `var s = list.stream(); s.count(); s.toList();` throws `IllegalStateException: stream has already been operated upon or closed`. Do not store streams in fields; store the collection, or a `Supplier<Stream<T>>`.

> **Pitfall:** Do not rely on side effects in `peek` or `map`. Since Java 9, `List.of(1, 2, 3).stream().peek(System.out::println).count()` prints **nothing**, because `count()` can compute the size directly from a sized source and skips the pipeline. Use `peek` only for debugging.

#### Parallel streams

`collection.parallelStream()` or `stream.parallel()` runs the same pipeline on several threads. The source is split into chunks (by its `Spliterator`), each chunk runs on a worker thread of the shared **common `ForkJoinPool`**, and partial results are combined. The number of workers defaults to the number of CPU cores minus one, with the calling thread also helping.

It looks like a free speed-up. It is not. A parallel stream is only faster when **all** of these hold:

- There is a lot of data or a lot of CPU work per element (a rough rule from the JDK team: N × Q above about 10,000, where N is elements and Q is cost per element).
- The source splits cheaply and evenly: `ArrayList`, arrays and `IntStream.range` split well; `LinkedList`, `Stream.iterate` and `Files.lines` split badly.
- The operations are stateless and do not need encounter order (`findAny` rather than `findFirst`, `unordered()` before `distinct` or `limit`).
- The work is CPU-bound, not waiting on I/O.
- Combining partial results is cheap (summing numbers yes, merging big maps less so).

The most dangerous pitfall is **shared mutable state**. The program below adds numbers to an `ArrayList` from a parallel `forEach`: a race condition that loses elements or throws `ArrayIndexOutOfBoundsException`, differently on each run. The correct version lets the collector build per-thread lists and merge them.

```java
import java.util.*;
import java.util.stream.*;

public class ParallelPitfall {
    public static void main(String[] args) {
        List<Integer> unsafe = new ArrayList<>();
        try {
            IntStream.range(0, 100_000).parallel().forEach(unsafe::add);   // WRONG: data race
            System.out.println("unsafe size is unpredictable: " + (unsafe.size() <= 100_000));
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("unsafe version crashed");
        }

        List<Integer> safe = IntStream.range(0, 100_000).parallel().boxed().toList();   // RIGHT
        System.out.println("safe size " + safe.size() + ", in order: " + safe.equals(
                IntStream.range(0, 100_000).boxed().toList()));

        long sum = IntStream.rangeClosed(1, 1_000_000).parallel().asLongStream().sum();
        System.out.println("sum " + sum);

        // forEach on a parallel stream has no order guarantee; forEachOrdered restores it
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 10).parallel().forEachOrdered(sb::append);
        System.out.println(sb);
    }
}
```

This program has no fixed output (that is the point), so it carries no output block. A typical run prints `unsafe size is unpredictable: true` or `unsafe version crashed`, then `safe size 100000, in order: true`, `sum 500000500000` and `0123456789`. Notice that `toList()` on a parallel stream still preserves encounter order: the collector merges the chunks in order.

> **Pitfall:** Every parallel stream in the JVM shares the one common pool. In a web server, one request running a slow parallel stream (worse: one that blocks on a database or HTTP call) can starve every other request's parallel streams. In server code, keep streams sequential unless you have measured a gain.

> **Tip:** Measure, do not guess. Use JMH (the Java Microbenchmark Harness) for honest numbers; a `System.nanoTime()` loop without warm-up mostly measures the JIT compiler.

::: deeper
**How the pipeline is built.** Each intermediate call creates a new stage object (`ReferencePipeline.StatelessOp` or `StatefulOp`) linked to the previous one. The terminal operation walks the chain backwards and wraps a chain of `Sink` objects, one per stage, so that `sink.accept(element)` pushes an element through `filter`, then `map`, and so on. It then asks the source's `Spliterator` to `forEachRemaining` into the first sink, or, if any stage short-circuits, to `tryAdvance` one element at a time while checking `cancellationRequested()`. That is why execution is vertical.

**Stream flags.** Each stage carries flags such as `SIZED`, `SORTED`, `DISTINCT` and `ORDERED`. The library uses them to skip work: `sorted()` on an already `SORTED` stream is a no-op, `distinct()` on a `TreeSet` stream does nothing, and `count()` on a `SIZED` stream returns the size without running the pipeline (the Java 9 `peek` surprise above).

**Parallel internals.** The terminal operation creates a `ForkJoinTask` that splits the spliterator recursively (`trySplit`) until chunks are small, runs leaf tasks on the common pool with work-stealing, and combines results up the tree. The common pool size can be set with `-Djava.util.concurrent.ForkJoinPool.common.parallelism=N`. Submitting the stream from inside your own `ForkJoinPool` (`pool.submit(() -> list.parallelStream()...)`) makes it run in that pool, a well-known but undocumented trick; a cleaner option today is an executor of virtual threads for I/O work (Advanced Java covers them).

**Non-interference and statelessness.** The `java.util.stream` package docs require behavioural parameters to be **non-interfering** (do not modify the source during the pipeline; doing so may throw `ConcurrentModificationException`) and, for predictable results, **stateless** (do not depend on state that changes while the pipeline runs).

> **Interview:** "What is lazy evaluation in streams?" Intermediate operations only build the pipeline; no element is processed until a terminal operation runs. Then elements flow one at a time through all stages, and short-circuiting operations like `limit`, `findFirst` and `anyMatch` stop the source early. That is what makes infinite streams and efficient "find the first match" queries possible.

> **Interview:** "When would you avoid parallel streams?" Small collections, sources that split badly (`LinkedList`, `iterate`, I/O lines), order-dependent operations (`findFirst`, `limit` on ordered data), blocking I/O inside the pipeline, shared mutable state, and server code where the shared common `ForkJoinPool` could be starved. Only use them after measuring a real gain on CPU-heavy work over large arrays or lists.
:::

::: check
?? How many times does the `filter` lambda run in `Stream.of(1, 2, 3, 4, 5, 6).filter(n -> n % 2 == 0).findFirst()`?
Twice: for 1 (rejected) and 2 (accepted). `findFirst` short-circuits as soon as it has a result, so 3 to 6 are never examined.

?? Why is `IntStream.range(0, n).parallel().forEach(list::add)` with an `ArrayList` a bug, and what should you write?
`ArrayList` is not thread-safe, so concurrent `add` calls lose elements or throw. Let the stream build the result: `IntStream.range(0, n).parallel().boxed().toList()` or `collect(Collectors.toList())`.

?? Name two stateful intermediate operations and explain why they matter.
`sorted` and `distinct` (also `limit` and `skip` in ordered parallel streams). They must remember elements they have already seen, so they need extra memory, break the one-at-a-time flow, and are expensive in parallel. `sorted` cannot finish on an infinite stream.
:::

::: resources
- Docs: [Package java.util.stream: laziness, parallelism, non-interference (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/package-summary.html) · the official rules in one page
- Docs: [Parallelizing streams (dev.java)](https://dev.java/learn/api/streams/parallel-streams/) · when parallel helps and how it splits
- Docs: [Parallelism (Java tutorial)](https://docs.oracle.com/javase/tutorial/collections/streams/parallelism.html) · side effects, ordering and interference
- Article: [When to Use a Parallel Stream in Java](https://www.baeldung.com/java-when-to-use-parallel-stream) · Baeldung with benchmarks and the N × Q idea
- Docs: [Aggregate Operations (Java tutorial)](https://docs.oracle.com/javase/tutorial/collections/streams/index.html) · pipelines, intermediate and terminal operations, laziness
- Book: *Modern Java in Action* by Raoul-Gabriel Urma, Mario Fusco and Alan Mycroft · chapter 7, parallel data processing
:::

### Optional: correct usage and anti-patterns

::: plain
`Optional` is a parcel that may or may not have something inside, with "may be empty" printed on the label. A method that returns a plain `Customer` might secretly hand you nothing (`null`), and you only find out when your code crashes. A method that returns `Optional<Customer>` warns you up front, and the parcel comes with tools to handle both cases: "if it is there, do this", "if not, use that instead", "if not, raise an alarm".
:::

`java.util.Optional<T>` (Java 8) is a container holding either one non-null value or nothing. Its purpose, in the words of its designers, is to be a **return type** for methods where "no result" is a normal outcome, so that the caller cannot forget the empty case. It is not a general replacement for `null` everywhere.

**Creating one.**

| Factory | Use when |
|---|---|
| `Optional.of(value)` | the value is definitely not null (throws `NullPointerException` if it is) |
| `Optional.ofNullable(value)` | the value may be null (empty if it is) |
| `Optional.empty()` | there is no value |

**Using one.** The point is to avoid `isPresent()` followed by `get()`; the API lets you say what you want in one expression.

| Method | Does |
|---|---|
| `map(fn)` | transform the value if present; empty stays empty |
| `flatMap(fn)` | same, when `fn` itself returns an `Optional` |
| `filter(pred)` | keep the value only if it passes |
| `orElse(other)` | value, or a default (default is **always evaluated**) |
| `orElseGet(supplier)` | value, or compute a default **only when empty** |
| `orElseThrow()` (Java 10), `orElseThrow(supplier)` | value, or throw `NoSuchElementException` / your exception |
| `ifPresent(consumer)`, `ifPresentOrElse(consumer, runnable)` (Java 9) | act on the value, or on its absence |
| `or(supplier)` (Java 9) | another `Optional` if empty: try the next source |
| `stream()` (Java 9) | a stream of zero or one element, great inside `flatMap` |
| `isPresent()`, `isEmpty()` (Java 11) | test; rarely needed with the methods above |

The program shows a ShopSphere repository that returns `Optional`, chained calls that navigate a possibly missing customer, address and city, and the `orElse` versus `orElseGet` difference that interviewers love.

```java
import java.util.*;

public class Optionals {
    record Address(String line1, String city) { }
    record Customer(String id, String name, Address address, String couponCode) {
        Optional<Address> addressOpt() { return Optional.ofNullable(address); }
    }

    static final Map<String, Customer> DB = Map.of(
            "C1", new Customer("C1", "Asha", new Address("12 MG Road", "Pune"), "WELCOME10"),
            "C2", new Customer("C2", "Ravi", null, null));

    static Optional<Customer> findById(String id) {          // "not found" is a normal outcome
        return Optional.ofNullable(DB.get(id));
    }

    static String defaultCity() {
        System.out.println("  (defaultCity() called)");
        return "Unknown";
    }

    public static void main(String[] args) {
        // map / flatMap chain instead of nested null checks
        for (String id : List.of("C1", "C2", "C9")) {
            String city = findById(id)
                    .flatMap(Customer::addressOpt)
                    .map(Address::city)
                    .orElse("no city");
            System.out.println(id + " -> " + city);
        }

        // filter, then ifPresentOrElse
        findById("C1").map(Customer::couponCode).filter(c -> c.startsWith("WELCOME"))
                .ifPresentOrElse(c -> System.out.println("apply " + c), () -> System.out.println("no coupon"));
        findById("C2").map(Customer::couponCode)            // map of a null result gives empty
                .ifPresentOrElse(c -> System.out.println("apply " + c), () -> System.out.println("no coupon"));

        // orElse evaluates its argument every time; orElseGet only when empty
        System.out.println("orElse on a present value:");
        String a = findById("C1").map(c -> c.address().city()).orElse(defaultCity());
        System.out.println("orElseGet on a present value:");
        String b = findById("C1").map(c -> c.address().city()).orElseGet(Optionals::defaultCity);
        System.out.println(a + " " + b);

        // orElseThrow with a meaningful exception
        try {
            findById("C9").orElseThrow(() -> new NoSuchElementException("Customer C9 not found"));
        } catch (NoSuchElementException e) {
            System.out.println(e.getMessage());
        }

        // or(): try the cache, then the database
        Optional<Customer> fromCache = Optional.empty();
        System.out.println(fromCache.or(() -> findById("C2")).map(Customer::name).orElseThrow());

        // stream(): keep only the customers that exist
        System.out.println(List.of("C1", "C9", "C2").stream()
                .map(Optionals::findById).flatMap(Optional::stream).map(Customer::name).toList());
    }
}
// Output:
// C1 -> Pune
// C2 -> no city
// C9 -> no city
// apply WELCOME10
// no coupon
// orElse on a present value:
//   (defaultCity() called)
// orElseGet on a present value:
// Pune Pune
// Customer C9 not found
// Ravi
// [Asha, Ravi]
```

`orElse(defaultCity())` called the method even though the value was present: the argument is an ordinary expression, evaluated before `orElse` runs. If the default is cheap (a constant), `orElse` is fine. If it is expensive (a database call, building an object, logging), use `orElseGet`.

**Anti-patterns to spot in code review.**

1. **`get()` without a check**, or `isPresent()` then `get()`. The first throws `NoSuchElementException: No value present`, which is no better than a `NullPointerException`. The second is a verbose `null` check. Use `orElse`, `orElseThrow`, `map` or `ifPresent`. (Since Java 10 the Javadoc of `get()` names `orElseThrow()`, which does the same thing with an honest name, as the preferred alternative.)
2. **Returning `null` from a method declared to return `Optional`.** The caller trusts the type and calls `.map` on `null`. Return `Optional.empty()`.
3. **`Optional` fields, parameters and collection elements.** `Optional` is not `Serializable` (so it breaks JPA entities and serialization), costs an extra object, and as a parameter it forces callers to wrap values. Use a nullable field with an `Optional`-returning getter, or overload the method.
4. **`Optional` of a collection.** Return an empty list, not `Optional<List<T>>`; an empty collection already means "nothing".
5. **`Optional.of(x)` when `x` may be null.** It throws at once. Use `ofNullable`.
6. **Using `Optional` to replace every `if`.** `Optional.ofNullable(x).ifPresent(...)` for a local variable is just a longer `if (x != null)`.
7. **`OptionalInt` vs `Optional<Integer>`.** For primitives the streams give you `OptionalInt`, `OptionalLong` and `OptionalDouble` (from `max()`, `average()`...); they have `getAsInt()` and `orElse(int)` but no `map`.

> **Pitfall:** `Optional` does not make `null` go away inside it: `Optional.of(customer).map(Customer::address).map(Address::city)` is safe only because `map` turns a `null` result into empty. If you write `.get().address().city()` you are back to raw nulls.

> **Tip:** In Spring Data, repository methods such as `findById` already return `Optional<T>`. The common service-layer idiom is `repo.findById(id).orElseThrow(() -> new OrderNotFoundException(id))`, which pairs with the custom exceptions from Core Java 2.

::: deeper
**Design intent.** Brian Goetz (Java language architect) has said that `Optional` was meant to be a limited mechanism for library method return types where there is a clear need to represent "no result", and that using it for fields or parameters was not the intent. That is why it is final, not `Serializable`, and has no setter.

**Value-based class.** `Optional` is documented as **value-based**: do not use `==` on it, do not synchronize on it, and do not rely on identity. `Optional.empty()` happens to return a singleton today, but code must not depend on that. Project Valhalla aims to make such classes true value objects in a future release, removing the allocation cost.

**`map` vs `flatMap`.** The rule is the same as for streams. If the function returns a plain value, use `map` and the result is wrapped for you. If the function already returns an `Optional` (like `addressOpt()`), `map` would give `Optional<Optional<Address>>`; `flatMap` flattens it to `Optional<Address>`.

**Equality.** `Optional.equals` compares contents: two empties are equal, and `Optional.of("x").equals(Optional.of("x"))` is true.

**Performance.** Each non-empty `Optional` is an allocation. In tight loops over millions of elements, prefer primitives or sentinel values; elsewhere, the JIT's escape analysis usually removes short-lived `Optional`s.

> **Interview:** "What is the difference between `orElse` and `orElseGet`?" `orElse(value)` takes an already computed value, so its argument expression is always evaluated, even when the `Optional` is present. `orElseGet(supplier)` takes a `Supplier` and calls it only when the `Optional` is empty. Use `orElseGet` when the fallback is expensive or has side effects.

> **Interview:** "Should you use `Optional` as a field or method parameter?" No. It is designed as a return type. It is not serializable, adds allocation, and as a parameter it complicates every caller. Use a nullable field with a getter that returns `Optional`, and overloads or a nullable parameter documented as such.
:::

::: check
?? What is wrong with `if (opt.isPresent()) { return opt.get().getName(); } else { return "guest"; }`, and how would you write it?
Nothing is incorrect, but it uses `Optional` like a null check and invites a bare `get()` elsewhere. Write `return opt.map(User::getName).orElse("guest");`.

?? `findById(id).orElse(createDefaultCustomer())` inserts a row into the database every time. Why, and what is the fix?
`orElse` evaluates its argument before the call, so `createDefaultCustomer()` runs even when the customer exists. Use `orElseGet(this::createDefaultCustomer)`.

?? When do you need `flatMap` instead of `map` on an `Optional`?
When the mapping function itself returns an `Optional`. `map` would produce a nested `Optional<Optional<T>>`; `flatMap` returns `Optional<T>`.
:::

::: resources
- Docs: [Optional (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Optional.html) · every method, plus the API note on intended use
- Docs: [Using Optionals (dev.java)](https://dev.java/learn/api/streams/optionals/) · official tutorial on creating and consuming optionals
- Article: [Guide to Java Optional](https://www.baeldung.com/java-optional) · Baeldung tour of the API, including `orElse` vs `orElseGet`
- Article: [Java Optional as Return Type](https://www.baeldung.com/java-optional-return) · Baeldung on where Optional belongs
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 55: return optionals judiciously
:::

### Date/Time API: LocalDate, LocalDateTime, Instant, ZonedDateTime, Duration, DateTimeFormatter

::: plain
A wall clock in Bengaluru and one in London show different times at the same moment. A birthday is a date with no time and no place. A flight departure is a time **in a specific city**. A log entry is an exact moment on a global timeline. The `java.time` API has a separate type for each of these ideas, so you cannot accidentally mix "9 a.m. somewhere" with "this exact instant", which is the source of most date bugs.
:::

Java's original `java.util.Date` and `Calendar` were mutable, not thread-safe, counted months from 0 and mixed up "an instant" with "a date in some zone". Java 8 replaced them with **`java.time`** (designed by the author of Joda-Time, JSR 310). Every class is **immutable** and thread-safe; methods like `plusDays` return a new object.

| Type | Holds | Example | Use for |
|---|---|---|---|
| `LocalDate` | date, no time, no zone | `2025-03-30` | birthdays, invoice dates, holidays |
| `LocalTime` | time of day, no zone | `09:30` | shop opening hours |
| `LocalDateTime` | date + time, no zone | `2025-03-30T09:30` | a time "on the wall", before you know where |
| `ZonedDateTime` | date + time + zone (`Asia/Kolkata`) | `2025-03-30T09:30+05:30[Asia/Kolkata]` | meetings, deliveries, anything a human schedules in a place |
| `OffsetDateTime` | date + time + fixed offset (`+05:30`) | `2025-03-30T09:30+05:30` | API payloads, database `timestamp with time zone` |
| `Instant` | a point on the UTC timeline (seconds + nanos since 1970-01-01T00:00Z) | `2025-03-30T04:00:00Z` | timestamps, logs, `createdAt` columns |
| `Duration` | an amount of time in seconds and nanos | `PT2H30M` | timeouts, "how long did it take" |
| `Period` | an amount in years, months, days | `P1M15D` | "subscription renews in 1 month" |
| `ZoneId`, `ZoneOffset` | a region with rules (`Europe/London`) or a fixed offset | `Asia/Kolkata`, `+05:30` | conversions |

[[fig:corefunc_time]]

The rule of thumb used in most backends: **store and transmit `Instant` (UTC)**, convert to `ZonedDateTime` in the user's zone only for display or for calendar maths, and use `LocalDate` for pure dates.

```java
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Locale;

public class TimeDemo {
    public static void main(String[] args) {
        // Dates: immutable, months are 1-based, maths returns new objects
        LocalDate ordered = LocalDate.of(2025, 1, 31);
        System.out.println(ordered.plusMonths(1));                  // no Feb 31: clamps to the last day
        System.out.println(ordered.plusDays(30).getDayOfWeek());
        System.out.println(ChronoUnit.DAYS.between(ordered, LocalDate.of(2025, 3, 1)));
        System.out.println(Period.between(LocalDate.of(1990, 8, 15), LocalDate.of(2025, 3, 30)));
        System.out.println(ordered.with(TemporalAdjusters.next(DayOfWeek.MONDAY)));
        System.out.println(LocalDate.of(2024, 2, 1).isLeapYear() + " " + LocalDate.of(2024, 2, 1).lengthOfMonth());

        // An order placed at an exact instant, shown in two zones
        Instant placed = Instant.parse("2025-03-30T04:00:00Z");
        ZonedDateTime india = placed.atZone(ZoneId.of("Asia/Kolkata"));
        ZonedDateTime london = placed.atZone(ZoneId.of("Europe/London"));
        System.out.println(india);
        System.out.println(london);
        System.out.println(india.toInstant().equals(london.toInstant()));

        // Daylight saving: 01:30 on 2025-03-30 does not exist in London (clocks jump 01:00 -> 02:00)
        ZonedDateTime gap = ZonedDateTime.of(LocalDateTime.of(2025, 3, 30, 1, 30), ZoneId.of("Europe/London"));
        System.out.println(gap);
        ZonedDateTime before = ZonedDateTime.of(2025, 3, 29, 12, 0, 0, 0, ZoneId.of("Europe/London"));
        System.out.println(Duration.between(before, before.plusDays(1)));    // calendar day
        System.out.println(Duration.between(before, before.plusHours(24)).toHours() + "h, lands at "
                + before.plusHours(24).toLocalTime());                     // exact 24 hours

        // Formatting and parsing
        DateTimeFormatter f = DateTimeFormatter.ofPattern("dd MMM yyyy, hh:mm a", Locale.ENGLISH);
        System.out.println(india.format(f));
        LocalDate parsed = LocalDate.parse("15/08/2025", DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        System.out.println(parsed + " " + DateTimeFormatter.ISO_LOCAL_DATE.format(parsed));

        // Duration for elapsed time and timeouts
        Duration sla = Duration.ofHours(48).plusMinutes(30);
        System.out.println(sla + " = " + sla.toMinutes() + " minutes");

        // The week-year trap: YYYY is week-based-year, not calendar year
        LocalDate newYearsEve = LocalDate.of(2024, 12, 30);
        System.out.println(newYearsEve.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + " vs "
                + newYearsEve.format(DateTimeFormatter.ofPattern("YYYY-MM-dd", Locale.UK)));
    }
}
// Output:
// 2025-02-28
// SUNDAY
// 29
// P34Y7M15D
// 2025-02-03
// true 29
// 2025-03-30T09:30+05:30[Asia/Kolkata]
// 2025-03-30T05:00+01:00[Europe/London]
// true
// 2025-03-30T02:30+01:00[Europe/London]
// PT23H
// 24h, lands at 13:00
// 30 Mar 2025, 09:30 AM
// 2025-08-15 2025-08-15
// PT48H30M = 2910 minutes
// 2024-12-30 vs 2025-12-30
```

Lines worth a second look:

- `2025-01-31` plus one month is `2025-02-28`: `java.time` clamps to the end of the month instead of overflowing into March.
- The same `Instant` is 09:30 in Pune and 05:00 in London (British Summer Time had begun at 01:00 that morning). Both `ZonedDateTime`s are the same instant.
- Asking for 01:30 in London on 30 March 2025, a time that never existed because clocks jumped forward, gives 02:30 BST: `java.time` moves forward by the length of the gap. In the autumn overlap (a time that happens twice) it picks the earlier offset by default; `withLaterOffsetAtOverlap()` picks the other.
- `plusDays(1)` keeps the wall-clock time (12:00 to 12:00), which on that weekend is only 23 hours. `plusHours(24)` adds exact time and lands at 13:00. Decide which one the business rule means.
- `YYYY` is the **week-based year**: 30 December 2024 falls in week 1 of 2025 under ISO week rules, so `YYYY` prints 2025. Always use `yyyy` (or `uuuu`). Likewise `hh` is the 12-hour clock (needs `a`), `HH` the 24-hour clock, `mm` minutes and `MM` months.

> **Pitfall:** `LocalDateTime.now()` throws away the zone, and the server's default zone (often UTC in containers, IST on your laptop) decides what you get. A `createdAt` stored as `LocalDateTime` is ambiguous forever. Store `Instant`, or `OffsetDateTime` in a `timestamp with time zone` column.

> **Pitfall:** Do not use three-letter zone IDs like `IST`: it means India, Israel or Irish Standard Time depending on context. Use region IDs (`Asia/Kolkata`, `Europe/London`) or explicit offsets.

> **Tip:** Make time testable by injecting a `java.time.Clock`. Production code calls `LocalDate.now(clock)`; tests pass `Clock.fixed(Instant.parse("2025-03-30T04:00:00Z"), ZoneOffset.UTC)` and the "current" date never changes under them.

::: deeper
**Time-zone rules change.** `ZoneId` rules come from the IANA time-zone database (tzdb) bundled in the JDK. Governments change daylight-saving rules with little notice, so JDK updates include new tzdb versions, and the `tzupdater` tool can update an older JDK. A zone ID stores a region; its offset on a given date is computed from those rules, which is why `ZonedDateTime` (region) is safer than `OffsetDateTime` (fixed offset) for future events like "a delivery slot next July".

**Legacy interop.** `Date.from(instant)`, `date.toInstant()`, `Timestamp.valueOf(localDateTime)`, `timestamp.toLocalDateTime()`, `GregorianCalendar.from(zdt)`. JDBC 4.2 drivers read and write `LocalDate`, `LocalDateTime` and `OffsetDateTime` directly with `getObject(col, LocalDate.class)`, and JPA/Hibernate map them without converters. `SimpleDateFormat` is not thread-safe (a classic production bug when stored in a static field); `DateTimeFormatter` is immutable and safe to share.

**Precision.** Since Java 9, `Instant.now()` usually has microsecond (or finer) precision on Linux and macOS, while databases may store milliseconds or microseconds; compare truncated values (`truncatedTo(ChronoUnit.MILLIS)`) in tests.

**`Duration` vs `Period`.** `Duration` is exact seconds; `Period` is calendar units whose length depends on the date (a month is 28 to 31 days). `Duration.ofDays(1)` is always 24 hours; `Period.ofDays(1)` added to a `ZonedDateTime` across a DST change can be 23 or 25 hours.

> **Interview:** "Difference between `LocalDateTime`, `ZonedDateTime` and `Instant`?" `LocalDateTime` is a date and time with no zone, so it does not identify a moment. `ZonedDateTime` adds a region zone with its daylight-saving rules, so it identifies a moment and supports calendar maths in that place. `Instant` is a moment on the UTC timeline with no human calendar attached; it is what you store and compare.

> **Interview:** "Why was `java.util.Date` replaced?" It is mutable (so not thread-safe and not safe as a map key or field), its API is confusing (years from 1900, months from 0), it conflates an instant with a local date-time, and its formatter `SimpleDateFormat` is not thread-safe. `java.time` is immutable, thread-safe, has separate types for separate concepts, and handles zones and DST properly.
:::

::: check
?? A delivery promise says "within 2 days". Should you use `Duration.ofDays(2)` or `Period.ofDays(2)` with a customer's `ZonedDateTime`, and why might they differ?
`Period.ofDays(2)` keeps the same wall-clock time two calendar days later; `Duration.ofDays(2)` adds exactly 48 hours. Across a daylight-saving change they differ by an hour. Customer-facing calendar promises usually mean the `Period` version.

?? Why does formatting `2024-12-30` with the pattern `YYYY-MM-dd` print `2025-12-30`?
`Y` is the week-based year. 30 December 2024 belongs to ISO week 1 of 2025, so the week-based year is 2025. The calendar year is `y` (or `u`).

?? What type would you use for a `created_at` column, and for a customer's date of birth?
`Instant` (or `OffsetDateTime`) for `created_at`, because it must identify an exact moment regardless of server zone. `LocalDate` for date of birth, because it has no time or zone.
:::

::: resources
- Docs: [The Date Time API (dev.java)](https://dev.java/learn/date-time/) · official tutorial covering every type
- Docs: [java.time package (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/package-summary.html) · overview and design notes
- Docs: [DateTimeFormatter (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/format/DateTimeFormatter.html) · the pattern letter table (`y` vs `Y`, `h` vs `H`)
- Article: [Introduction to the Java 8 Date/Time API](https://www.baeldung.com/java-8-date-time-intro) · Baeldung overview
- Article: [Guide to DateTimeFormatter](https://www.baeldung.com/java-datetimeformatter) · Baeldung on patterns and parsing
- Spec: [JSR 310: Date and Time API](https://jcp.org/en/jsr/detail?id=310) · the specification behind `java.time`
:::

## I/O, JSON & build tools

A program is only useful if data gets in and out. In ShopSphere, orders arrive as CSV exports from a partner, the website sends JSON, prices must be computed to the exact paisa, and the code itself has to be compiled, tested and packaged by a build tool on every laptop and CI server in the same way. This group covers those boundaries.

The first three topics are about data: files with `java.nio.file` (including a CSV parser you can write in an interview), serialization and JSON with Jackson, and `BigDecimal` for money. The last two are about the project: Maven, which most Java teams use, and Gradle, which you must at least be able to read and modify. Coming from data reconciliation, you will find the file and money topics familiar territory: they are exactly where mismatches come from.

### File I/O, NIO.2 (Path, Files), reading CSV files

::: plain
A `Path` is an address on the disk, like "Folder Orders, file march.csv", written down but not yet visited. The `Files` class is the clerk who goes to that address and does the work: reads the file, writes it, copies it, lists a folder. For big files you do not ask the clerk to bring the whole cabinet to your desk; you ask for one line at a time, so a 5 GB export never needs 5 GB of memory.
:::

Java has two file APIs. The old `java.io` package (Java 1.0) has `File`, streams (`FileInputStream`) and readers and writers (`BufferedReader`). **NIO.2**, added in Java 7 as `java.nio.file`, is the modern one: an immutable `Path` for locations and a `Files` utility class with static methods for everything else. Use NIO.2 for new code; you will still meet `java.io` readers and writers, because `Files` returns them.

| Task | NIO.2 (`java.nio.file`) |
|---|---|
| Build a path | `Path.of("data", "orders.csv")`, `dir.resolve("out.csv")`, `p.getParent()`, `p.getFileName()` |
| Read a small file | `Files.readString(p)`, `Files.readAllLines(p)` |
| Read a big file line by line | `try (Stream<String> lines = Files.lines(p)) { ... }` or `Files.newBufferedReader(p)` |
| Write | `Files.writeString(p, text)`, `Files.write(p, lines)`, `Files.newBufferedWriter(p)` |
| Append | `Files.writeString(p, text, StandardOpenOption.APPEND, StandardOpenOption.CREATE)` |
| Check | `Files.exists(p)`, `isDirectory`, `size`, `getLastModifiedTime` |
| Create | `Files.createDirectories(dir)`, `createFile`, `createTempFile`, `createTempDirectory` |
| Copy, move, delete | `Files.copy(a, b, REPLACE_EXISTING)`, `Files.move(a, b, ATOMIC_MOVE)`, `Files.deleteIfExists(p)` |
| List a folder | `try (Stream<Path> s = Files.list(dir))`, recursive: `Files.walk(dir)`, search: `Files.find` |

**Character sets.** Text files are bytes; turning bytes into characters needs a charset. `Files.readString`, `readAllLines`, `lines` and `newBufferedReader` default to **UTF-8**. Since Java 18 (JEP 400) the old `java.io` classes (`FileReader`, `FileWriter`) also default to UTF-8, but on Java 17 and earlier they used the platform charset (often Windows-1252 on Windows), a classic cause of corrupted names like "JosÃ©". Pass `StandardCharsets.UTF_8` explicitly when in doubt.

**Reading CSV.** CSV looks trivial until a customer's name contains a comma (`"Ravi, Jr."`), a quote, or a line break. `line.split(",")` breaks on the first one. For real projects use a library (Apache Commons CSV, OpenCSV, Jackson's CSV module); in an interview, show that you know the trap and write a small parser that respects quotes. The program below writes a partner's order export, parses it line by line into records (skipping and reporting bad rows, as a reconciliation job should), writes a summary file, and lists the folder.

```java
import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class CsvOrders {
    record OrderRow(String id, String customer, String city, BigDecimal amount) { }

    /** Splits one CSV line, honouring double quotes ("" inside quotes is a literal quote). */
    static List<String> splitCsv(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') { cur.append('"'); i++; }
                else inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                out.add(cur.toString()); cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out;
    }

    static Optional<OrderRow> parse(String line, int lineNo, List<String> errors) {
        List<String> f = splitCsv(line);
        if (f.size() != 4) { errors.add("line " + lineNo + ": expected 4 fields, got " + f.size()); return Optional.empty(); }
        try {
            return Optional.of(new OrderRow(f.get(0).strip(), f.get(1).strip(), f.get(2).strip(),
                    new BigDecimal(f.get(3).strip())));
        } catch (NumberFormatException e) {
            errors.add("line " + lineNo + ": bad amount '" + f.get(3) + "'");
            return Optional.empty();
        }
    }

    public static void main(String[] args) throws IOException {
        Path dir = Files.createTempDirectory("shopsphere");
        Path in = dir.resolve("orders.csv");
        Files.writeString(in, """
                order_id,customer,city,amount
                O1,Asha,Pune,357.00
                O2,"Ravi, Jr.",Delhi,490.00
                O3,Meena,Chennai,abc
                O4,"John ""JJ"" Doe",Pune,219.00
                O5,Lata,Pune
                """, StandardCharsets.UTF_8);

        // Small file: read it all at once
        System.out.println(Files.readAllLines(in).size() + " lines, " + Files.size(in) + " bytes");

        // Big-file style: stream lines lazily, close the file with try-with-resources
        List<String> errors = new ArrayList<>();
        List<OrderRow> rows;
        try (Stream<String> lines = Files.lines(in)) {
            int[] lineNo = {1};                                        // header is line 1
            rows = lines.skip(1)
                    .map(l -> parse(l, ++lineNo[0], errors))
                    .flatMap(Optional::stream)
                    .toList();
        }
        rows.forEach(System.out::println);
        errors.forEach(e -> System.out.println("SKIPPED " + e));

        // Aggregate and write a report with a BufferedWriter
        Map<String, BigDecimal> byCity = rows.stream().collect(Collectors.groupingBy(
                OrderRow::city, TreeMap::new,
                Collectors.reducing(BigDecimal.ZERO, OrderRow::amount, BigDecimal::add)));
        Path report = dir.resolve("reports").resolve("by_city.csv");
        Files.createDirectories(report.getParent());
        try (BufferedWriter w = Files.newBufferedWriter(report)) {
            w.write("city,total");
            w.newLine();
            for (var e : byCity.entrySet()) { w.write(e.getKey() + "," + e.getValue()); w.newLine(); }
        }
        System.out.print(Files.readString(report));

        // Walk the folder (paths shown relative to it)
        try (Stream<Path> all = Files.walk(dir)) {
            all.filter(Files::isRegularFile).map(p -> dir.relativize(p).toString()).sorted()
                    .forEach(System.out::println);
        }
    }
}
// Output:
// 6 lines, 145 bytes
// OrderRow[id=O1, customer=Asha, city=Pune, amount=357.00]
// OrderRow[id=O2, customer=Ravi, Jr., city=Delhi, amount=490.00]
// OrderRow[id=O4, customer=John "JJ" Doe, city=Pune, amount=219.00]
// SKIPPED line 4: bad amount 'abc'
// SKIPPED line 6: expected 4 fields, got 3
// city,total
// Delhi,490.00
// Pune,576.00
// orders.csv
// reports/by_city.csv
```

The one-element array `lineNo` is a deliberate shortcut to count lines inside a lambda in a sequential stream (a plain `for` loop over `Files.newBufferedReader(in).readLine()` is the cleaner choice when you need the line number, and is what most production parsers do). Note how errors are collected rather than thrown: one bad row in a 100,000-row partner file should produce a report entry, not abort the whole load.

> **Pitfall:** `Files.lines`, `Files.list`, `Files.walk` and `Files.find` return streams that hold an open file handle. Always use them in try-with-resources. Forgetting it works on your laptop and fails in production with "Too many open files".

> **Pitfall:** `Files.readAllLines` and `readString` load the whole file into memory. Fine for a config file, an `OutOfMemoryError` for a multi-gigabyte export. Stream large files.

> **Tip:** Write to a temporary file in the same folder and then `Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE)`. Readers then see either the old file or the complete new one, never a half-written file. Reconciliation jobs that pick up files from a shared folder depend on this.

::: deeper
**`File` vs `Path`.** `java.io.File` methods return `false` on failure without saying why (`file.delete()` just returns `false`); `Files` methods throw meaningful exceptions (`NoSuchFileException`, `AccessDeniedException`, `DirectoryNotEmptyException`, all subclasses of `IOException`). `Path` also supports symbolic links, file attributes (`Files.readAttributes(p, BasicFileAttributes.class)`, POSIX permissions) and other file systems through `FileSystem`: `FileSystems.newFileSystem(zipPath)` lets you read a ZIP file with the same `Files` API. Convert with `file.toPath()` and `path.toFile()`.

**Buffering.** Unbuffered streams make one system call per `read()`; `BufferedReader` and `BufferedWriter` read and write in 8 KB chunks (the default buffer size), which can be 100 times faster for line-oriented work. `Files.newBufferedReader` gives you one already. For binary data, `Files.newInputStream` plus `BufferedInputStream`, or `InputStream.transferTo(out)` (Java 9) to copy streams.

**Large files and NIO channels.** `FileChannel` and memory-mapped files (`channel.map(...)`) let the OS page a file into memory, which is the fastest way to scan huge binary files. `WatchService` notifies you when files appear in a folder, useful for "pick up partner files as they land" jobs, although many teams use a scheduler instead.

**Exceptions inside streams.** `Files.lines` wraps an `IOException` that happens during reading (for example a malformed UTF-8 byte) in an `UncheckedIOException`, thrown from the terminal operation. A `MalformedInputException` here usually means the file is not UTF-8; pass the right charset, such as `Files.lines(p, StandardCharsets.ISO_8859_1)`.

> **Interview:** "How do you read a 10 GB file in Java?" Never load it whole. Stream it with `Files.lines(path)` in try-with-resources, or `Files.newBufferedReader` and `readLine()` in a loop, processing and discarding each line. Keep only aggregates in memory. Mention the charset, and that malformed rows should be logged and skipped, not abort the run.

> **Interview:** "What is the difference between `java.io` and `java.nio`?" `java.io` is stream-oriented and blocking, with `File` as a weak path abstraction. `java.nio` (Java 1.4) added buffers, channels, selectors for non-blocking I/O and memory mapping; NIO.2 (Java 7) added `Path`, `Files`, file attributes, symbolic links, `WatchService` and pluggable file systems.
:::

::: check
?? Why must `Files.lines(path)` be used inside try-with-resources while `Files.readAllLines(path)` need not be?
`readAllLines` reads the file completely and closes it before returning. `Files.lines` returns a lazy stream that keeps the file open until the stream is closed.

?? Why does `"O2,\"Ravi, Jr.\",Delhi,490.00".split(",")` give five fields instead of four?
`split` knows nothing about CSV quoting, so it also splits on the comma inside the quoted name. Use a CSV-aware parser.

?? Your service reads a partner's file with `new FileReader(f)` on Java 11 and names like "José" come out garbled on the Windows server only. What happened?
Before Java 18, `FileReader` used the platform default charset (Windows-1252 on that server) instead of UTF-8. Read with an explicit charset: `Files.newBufferedReader(path, StandardCharsets.UTF_8)`.
:::

::: resources
- Docs: [Common I/O tasks in modern Java (dev.java)](https://dev.java/learn/modernio/) · reading and writing files with NIO.2
- Docs: [Files (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html) · every static method on one page
- Docs: [Basic I/O (Java tutorial)](https://docs.oracle.com/javase/tutorial/essential/io/index.html) · Oracle's tutorial on streams and the file I/O (NIO.2) trail
- Spec: [JEP 400: UTF-8 by Default](https://openjdk.org/jeps/400) · why the default charset changed in Java 18
- Article: [Reading a CSV file into an array](https://www.baeldung.com/java-csv-file-array) · Baeldung on plain Java, OpenCSV and Scanner approaches
- Docs: [File I/O featuring NIO.2 (Java tutorial)](https://docs.oracle.com/javase/tutorial/essential/io/fileio.html) · Oracle's trail on `Path`, `Files` and walking file trees
:::

### Serialization basics; JSON with Jackson (ObjectMapper, annotations)

::: plain
Objects live in memory, and memory disappears when the program stops or when data has to travel to another machine. **Serialization** packs an object into a flat sequence of bytes or text that can be saved or sent, and deserialization unpacks it on the other side, like flat-pack furniture with assembly instructions. Java's built-in serialization uses a private Java-only format. JSON is the universal flat-pack format that browsers, mobile apps and every other language understand, and Jackson is the library Java uses to pack and unpack it.
:::

**Java serialization.** A class that implements the marker interface `java.io.Serializable` can be written with `ObjectOutputStream.writeObject` and read back with `ObjectInputStream.readObject`. The JVM writes the class name, a version number and every non-`transient`, non-`static` field, following references to other objects. Fields marked **`transient`** are skipped and come back as their default value (`null`, `0`, `false`). The **`serialVersionUID`** is the class's version stamp: if the stream's stamp differs from the loaded class's, reading fails with `InvalidClassException`. If you do not declare one, the compiler-dependent default is computed from the class's structure, so adding a method can break old data; declare it explicitly.

```java
import java.io.*;
import java.math.BigDecimal;
import java.util.List;

public class JavaSerialization {
    static class Basket implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
        final String customerId;
        final List<String> skus;
        final BigDecimal total;
        transient String sessionToken;                   // not written: secrets, caches, connections

        Basket(String customerId, List<String> skus, BigDecimal total, String token) {
            this.customerId = customerId; this.skus = skus; this.total = total; this.sessionToken = token;
        }
    }

    record Coupon(String code, int percent) implements Serializable {
        Coupon {                                          // records deserialize through this constructor
            if (percent < 0 || percent > 50) throw new IllegalArgumentException("bad percent " + percent);
        }
    }

    static byte[] toBytes(Object o) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) { out.writeObject(o); }
        return bytes.toByteArray();
    }

    static Object fromBytes(byte[] data) throws IOException, ClassNotFoundException {
        try (var in = new ObjectInputStream(new ByteArrayInputStream(data))) { return in.readObject(); }
    }

    public static void main(String[] args) throws Exception {
        Basket b = new Basket("C1", List.of("PEN", "INK"), new BigDecimal("259.00"), "secret-123");
        byte[] data = toBytes(b);
        Basket copy = (Basket) fromBytes(data);
        System.out.println(copy.customerId + " " + copy.skus + " " + copy.total + " token=" + copy.sessionToken);
        System.out.println("same object? " + (copy == b));

        Coupon c = (Coupon) fromBytes(toBytes(new Coupon("WELCOME10", 10)));
        System.out.println(c);
    }
}
// Output:
// C1 [PEN, INK] 259.00 token=null
// same object? false
// Coupon[code=WELCOME10, percent=10]
```

The copy is a new object graph with the same data (a **deep copy**, in fact), and the `transient` token did not survive. Records are special: they are deserialized by calling their canonical constructor, so the validation in the compact constructor runs on incoming data too. Ordinary classes are rebuilt **without** calling their constructor, which is one reason built-in serialization is dangerous.

> **Pitfall:** Never deserialize Java-serialized data from an untrusted source. `readObject` can be tricked into instantiating "gadget" classes on the classpath whose methods run attacker-controlled code; this has caused remote-code-execution vulnerabilities in many well-known products. Modern advice, including from the Java architects, is to avoid Java serialization for new designs and use JSON or another explicit format. If you must read it, configure an `ObjectInputFilter` (Java 9, improved in Java 17 by JEP 415) to allow only expected classes.

#### JSON with Jackson

In a Spring Boot service you rarely call serialization APIs yourself: an HTTP request body arrives as JSON and Spring turns it into your `OrderRequest` record, then turns your `OrderResponse` back into JSON. The library doing that is **Jackson**, and its central class is **`ObjectMapper`**. Jackson is not part of the JDK, so the snippets below are not runnable with plain `java File.java`; they need the dependency (Spring Boot's web starter already includes it).

Build configuration (Maven; the next topics explain the file):

```xml
<!-- pom.xml: not needed in Spring Boot, where spring-boot-starter-web brings Jackson -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.19.2</version>
</dependency>
<dependency>  <!-- java.time support: LocalDate, Instant ... -->
    <groupId>com.fasterxml.jackson.datatype</groupId>
    <artifactId>jackson-datatype-jsr310</artifactId>
    <version>2.19.2</version>
</dependency>
```

Code fragment (Jackson 2.x, needs the dependencies above):

```java
// Not runnable without Jackson on the classpath
import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@JsonIgnoreProperties(ignoreUnknown = true)              // tolerate extra fields from the client
@JsonInclude(JsonInclude.Include.NON_NULL)               // leave out null fields when writing
@JsonPropertyOrder({"order_id", "customer", "placed_on", "total", "lines"})
record OrderDto(
        @JsonProperty("order_id") String id,                 // JSON name differs from Java name
        String customer,
        @JsonProperty("placed_on") @JsonFormat(pattern = "yyyy-MM-dd") LocalDate placedOn,
        BigDecimal total,
        List<LineDto> lines,
        @JsonIgnore String internalNote) { }                 // never written or read

record LineDto(String sku, int qty) { }

ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

// Java -> JSON
OrderDto order = new OrderDto("O1", "Asha", LocalDate.of(2025, 3, 30),
        new BigDecimal("357.00"), List.of(new LineDto("PEN", 3)), "VIP");
String json = mapper.writeValueAsString(order);

// JSON -> Java
OrderDto back = mapper.readValue(json, OrderDto.class);

// Generic types need a TypeReference, because of type erasure (Core Java 2)
List<OrderDto> many = mapper.readValue(jsonArray, new TypeReference<List<OrderDto>>() { });

// Tree model for JSON whose shape you do not want to map to a class
JsonNode root = mapper.readTree(json);
String firstSku = root.path("lines").path(0).path("sku").asText();     // "PEN"
```

What `writeValueAsString(order)` produces:

```text
{"order_id":"O1","customer":"Asha","placed_on":"2025-03-30","total":357.00,"lines":[{"sku":"PEN","qty":3}]}
```

The annotations you will use most:

| Annotation | Effect |
|---|---|
| `@JsonProperty("name")` | map a field to a different JSON name (also marks a constructor parameter for binding) |
| `@JsonIgnore` | leave the field out in both directions |
| `@JsonIgnoreProperties(ignoreUnknown = true)` | do not fail on JSON fields the class does not have |
| `@JsonInclude(NON_NULL)` / `NON_EMPTY` | omit nulls (or empty collections) when writing |
| `@JsonFormat(pattern = ...)` | date format, or `shape = STRING` for numbers as strings |
| `@JsonCreator` | the constructor or factory Jackson should use to build an object |
| `@JsonAlias({"orderId", "id"})` | accept alternative names when reading |
| `@JsonPropertyOrder` | the order of fields in the output |
| `@JsonNaming(SnakeCaseStrategy.class)` | `placedOn` ↔ `placed_on` for every field |

**How Jackson finds properties.** For writing it uses public getters (`getTotal()` gives `"total"`), public fields, and record components. For reading it needs a way to construct the object: a no-argument constructor plus setters or fields, or a constructor annotated with `@JsonCreator`, or (since 2.12) the canonical constructor of a record. Records are the cleanest DTOs (data transfer objects) for JSON: immutable, with a constructor Jackson understands.

> **Pitfall:** By default `ObjectMapper` fails on JSON fields your class does not know: `UnrecognizedPropertyException: Unrecognized field "coupon"`. That protects you from typos but breaks clients when the other side adds a field. Spring Boot's auto-configured mapper turns that check off; a hand-made `new ObjectMapper()` does not. Decide deliberately (`@JsonIgnoreProperties(ignoreUnknown = true)` or `mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)`).

> **Pitfall:** Without the JSR-310 module, Jackson 2 cannot serialize `LocalDate` or `Instant` ("Java 8 date/time type `java.time.LocalDate` not supported by default"). Spring Boot registers it for you; plain code must call `registerModule(new JavaTimeModule())`, and disable `WRITE_DATES_AS_TIMESTAMPS` to get ISO strings instead of arrays like `[2025,3,30]`.

> **Tip:** Create one `ObjectMapper` and share it: it is thread-safe once configured, and expensive to create because it caches serializers per class. In Spring, inject the auto-configured bean instead of calling `new ObjectMapper()`.

::: deeper
**Inside `readValue`.** For each class Jackson builds (and caches) a `BeanDeserializer` by introspecting constructors, fields, setters and annotations once. Parsing is streaming: a `JsonParser` reads tokens (`START_OBJECT`, `FIELD_NAME`, `VALUE_STRING`...) and the deserializer assigns each value as it goes, so Jackson never builds an intermediate tree unless you ask for `readTree`. That is why it is fast and memory-friendly. The same streaming API (`JsonParser`, `JsonGenerator`) is available for very large documents.

**BigDecimal and JSON numbers.** JSON numbers have no type; Jackson reads `357.00` into a `BigDecimal` field exactly, but into a `double` field approximately. For money, use `BigDecimal` fields, and consider `DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS` when reading into `Object` or `JsonNode`. Some APIs send money as strings (`"357.00"`) so JavaScript clients, whose numbers are doubles, do not lose precision.

**Polymorphism.** To read a `Payment` that may be a `CardPayment` or a `UpiPayment`, annotate the base type with `@JsonTypeInfo(use = Id.NAME, property = "type")` and `@JsonSubTypes`. Never enable "default typing" with class names from the input: it reopens the same gadget-class hole as Java serialization, and many Jackson CVEs came from it.

**Version notes.** Jackson 2.x lives in the `com.fasterxml.jackson` packages and supports Java 8 and later. **Jackson 3.0** (released in October 2025) moved to the `tools.jackson` packages and Maven group, requires Java 17, builds immutable mappers through `JsonMapper.builder()`, includes `java.time` support without a separate module, and changed some defaults; the annotations keep their `com.fasterxml.jackson.annotation` package. Spring Boot 4 uses Jackson 3, while Spring Boot 3.x projects use Jackson 2, so check which one a project is on before copying examples. Gson (Google) and JSON-B (Jakarta's standard API) are the alternatives you may also meet.

> **Interview:** "What is `serialVersionUID` and what happens if you do not declare it?" A version number written into the serialized stream and checked when reading; a mismatch throws `InvalidClassException`. Without an explicit value the JVM computes one from the class structure, so harmless changes such as adding a method can make old data unreadable. Declare `private static final long serialVersionUID = 1L;` and change it only for incompatible changes.

> **Interview:** "What does `transient` do?" It excludes a field from Java serialization; after deserialization the field holds its default value. Use it for derived values, caches, and secrets. (Jackson also skips `transient` fields, though a public getter for the same property is still used; `@JsonIgnore` is the explicit way to exclude a property from JSON.)

> **Interview:** "How does Spring convert a request body to an object?" The `@RequestBody` parameter is read by an `HttpMessageConverter`, by default `MappingJackson2HttpMessageConverter` (Jackson 2) or its Jackson 3 equivalent in Spring Boot 4, which calls the shared `ObjectMapper` to deserialize the JSON into the parameter's type. Responses go the other way.
:::

::: check
?? A `Customer` class implements `Serializable` and has a field `transient int loginCount = 5;`. What is its value after deserialization, and why?
`0`. Transient fields are not written, and deserialization of a `Serializable` class does not run its constructor or field initializers, so the field keeps the default value for `int`.

?? The mobile team adds a new field `"giftWrap": true` to the order JSON and your service starts returning 400 errors. What is the likely cause and the fix?
A plain `ObjectMapper` fails on unknown properties (`UnrecognizedPropertyException`). Add `@JsonIgnoreProperties(ignoreUnknown = true)` to the DTO, disable `FAIL_ON_UNKNOWN_PROPERTIES`, or add the field to the DTO.

?? Why do you need `new TypeReference<List<OrderDto>>() { }` instead of `List.class` to read a JSON array?
Because of type erasure: `List.class` carries no element type, so Jackson would produce a list of `LinkedHashMap`s. The anonymous `TypeReference` subclass records the full generic type in its class signature, which Jackson reads by reflection.
:::

::: resources
- Docs: [Serializable (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/io/Serializable.html) · `serialVersionUID`, special methods and records
- Docs: [Jackson Databind (GitHub)](https://github.com/FasterXML/jackson-databind) · official README with ObjectMapper usage and features
- Docs: [Jackson Annotations (GitHub)](https://github.com/FasterXML/jackson-annotations) · the annotation reference
- Article: [Jackson ObjectMapper Tutorial](https://www.baeldung.com/jackson-object-mapper-tutorial) · Baeldung on reading, writing and configuring
- Article: [Jackson Annotation Examples](https://www.baeldung.com/jackson-annotations) · Baeldung tour of every common annotation
- Article: [Introduction to Java Serialization](https://www.baeldung.com/java-serialization) · Baeldung on `Serializable`, `transient` and `serialVersionUID`
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 85: prefer alternatives to Java serialization
:::

### BigDecimal for money (never double!), rounding modes

::: plain
A `double` stores numbers in binary, the way a ruler marked only in halves, quarters and eighths measures length: it can show 0.5 or 0.25 exactly, but 0.1 lands between two marks and gets rounded. For a physics simulation that tiny error does not matter. For money it does: add enough of those errors and a reconciliation report is off by a few paise that nobody can explain. `BigDecimal` counts in decimal digits, like a cashier, so 0.10 is exactly ten paise, and it makes you say explicitly how to round.
:::

`double` and `float` are **binary floating-point** numbers (IEEE 754). Most decimal fractions (0.1, 0.2, 0.7) have no exact binary representation, just as 1/3 has no exact decimal one, so each is stored as the nearest binary value. The errors are tiny but they accumulate and they show up in comparisons:

```java
import java.math.BigDecimal;

public class DoubleTrouble {
    public static void main(String[] args) {
        System.out.println(0.1 + 0.2);
        System.out.println(0.1 + 0.2 == 0.3);
        double total = 0;
        for (int i = 0; i < 10; i++) total += 0.10;          // ten items at Rs 0.10
        System.out.println(total);
        System.out.println(1.10 * 3);
        System.out.println(new BigDecimal("1.10").multiply(new BigDecimal("3")));
        System.out.println(new BigDecimal(0.1));               // the double's true value
        System.out.println(BigDecimal.valueOf(0.1));           // uses Double.toString: "0.1"
    }
}
// Output:
// 0.30000000000000004
// false
// 0.9999999999999999
// 3.3000000000000003
// 3.30
// 0.1000000000000000055511151231257827021181583404541015625
// 0.1
```

The last two lines are the classic constructor trap. `new BigDecimal(0.1)` receives a `double` that is already wrong and faithfully preserves its every digit. Create `BigDecimal`s from **strings** (`new BigDecimal("0.10")`), from integers (`BigDecimal.valueOf(1999, 2)` is 19.99), or, if you only have a `double`, from `BigDecimal.valueOf(d)`, which goes through the shortest decimal string for that double.

**How `BigDecimal` works.** A `BigDecimal` is an arbitrary-precision integer called the **unscaled value** plus a 32-bit **scale**: the value is `unscaledValue × 10^-scale`. `new BigDecimal("357.50")` is 35750 with scale 2. **Precision** is the number of digits in the unscaled value (5 here). It is **immutable**: `add`, `subtract`, `multiply` and `divide` return new objects, so `price.add(tax);` on its own line does nothing useful; you must write `price = price.add(tax);`.

Scale follows simple rules: `add` and `subtract` give the larger of the two scales, `multiply` gives the **sum** of the scales (49.00 × 0.18 has scale 4: `8.8200`), and `divide` needs to be told what to do, because many quotients (1/3) never end.

**Rounding modes.** `setScale(2, RoundingMode.HALF_UP)` rounds to two places. The mode decides what happens to the dropped digits:

| Mode | Rule | 2.345 → | 2.355 → | -2.345 → |
|---|---|---|---|---|
| `UP` | away from zero | 2.35 | 2.36 | -2.35 |
| `DOWN` | towards zero (truncate) | 2.34 | 2.35 | -2.34 |
| `CEILING` | towards positive infinity | 2.35 | 2.36 | -2.34 |
| `FLOOR` | towards negative infinity | 2.34 | 2.35 | -2.35 |
| `HALF_UP` | nearest, ties away from zero (school rounding) | 2.35 | 2.36 | -2.35 |
| `HALF_DOWN` | nearest, ties towards zero | 2.34 | 2.35 | -2.34 |
| `HALF_EVEN` | nearest, ties to the even digit (banker's rounding) | 2.34 | 2.36 | -2.34 |
| `UNNECESSARY` | assert no rounding is needed; throws otherwise | exception | exception | exception |

`HALF_UP` is what customers expect on an invoice. `HALF_EVEN` is the default in many financial systems (and in IEEE 754) because, over many values, rounding ties up and down equally often removes the upward bias that `HALF_UP` adds. Which one you use is a **business rule**: tax authorities and payment providers specify it; ask, do not guess.

The program builds a ShopSphere invoice: line totals, 18% GST rounded per line, a discount, an exact comparison trap, a division, and a bill split three ways where the paise must still add up.

```java
import java.math.*;
import java.util.*;

public class Money {
    record Line(String sku, BigDecimal unitPrice, int qty) {
        BigDecimal net() { return unitPrice.multiply(BigDecimal.valueOf(qty)); }
    }

    static final BigDecimal GST = new BigDecimal("0.18");

    public static void main(String[] args) {
        List<Line> lines = List.of(
                new Line("PEN", new BigDecimal("49.99"), 3),
                new Line("INK", new BigDecimal("210.00"), 1),
                new Line("PAD", new BigDecimal("12.45"), 7));

        BigDecimal net = BigDecimal.ZERO, tax = BigDecimal.ZERO;
        for (Line l : lines) {
            BigDecimal lineTax = l.net().multiply(GST).setScale(2, RoundingMode.HALF_UP);
            System.out.printf("%-4s %8s  tax %6s  (exact %s)%n", l.sku(), l.net(), lineTax, l.net().multiply(GST));
            net = net.add(l.net());
            tax = tax.add(lineTax);
        }
        BigDecimal discount = net.multiply(new BigDecimal("0.05")).setScale(2, RoundingMode.HALF_EVEN);
        BigDecimal payable = net.subtract(discount).add(tax);
        System.out.println("net " + net + ", discount " + discount + ", tax " + tax + ", payable " + payable);

        // equals compares value AND scale; compareTo compares value only
        BigDecimal a = new BigDecimal("2.0"), b = new BigDecimal("2.00");
        System.out.println(a.equals(b) + " " + (a.compareTo(b) == 0) + " " + a.stripTrailingZeros().equals(b.stripTrailingZeros()));
        System.out.println(new HashSet<>(List.of(a, b)).size() + " " + new TreeSet<>(List.of(a, b)).size());

        // division must be told the scale and rounding when the result does not terminate
        BigDecimal hundred = new BigDecimal("100.00");
        try {
            hundred.divide(new BigDecimal("3"));
        } catch (ArithmeticException e) {
            System.out.println(e.getMessage());
        }
        System.out.println(hundred.divide(new BigDecimal("3"), 2, RoundingMode.HALF_UP));
        System.out.println(hundred.divide(new BigDecimal("3"), MathContext.DECIMAL64));

        // split a bill so the parts add back to the total exactly
        System.out.println(split(hundred, 3) + " -> " + split(hundred, 3).stream().reduce(BigDecimal.ZERO, BigDecimal::add));

        // rounding modes side by side
        for (RoundingMode m : List.of(RoundingMode.HALF_UP, RoundingMode.HALF_EVEN, RoundingMode.DOWN)) {
            System.out.println(m + ": " + new BigDecimal("2.345").setScale(2, m) + " "
                    + new BigDecimal("2.355").setScale(2, m) + " " + new BigDecimal("-2.345").setScale(2, m));
        }
        System.out.println(new BigDecimal("1E+3").toPlainString() + " " + new BigDecimal("1000").stripTrailingZeros());
    }

    /** Divides amount into n parts of whole paise; the first parts absorb the leftover paise. */
    static List<BigDecimal> split(BigDecimal amount, int n) {
        BigDecimal base = amount.divide(BigDecimal.valueOf(n), 2, RoundingMode.DOWN);
        BigDecimal remainder = amount.subtract(base.multiply(BigDecimal.valueOf(n)));
        int extraPaise = remainder.movePointRight(2).intValueExact();
        List<BigDecimal> parts = new ArrayList<>();
        for (int i = 0; i < n; i++) parts.add(i < extraPaise ? base.add(new BigDecimal("0.01")) : base);
        return parts;
    }
}
// Output:
// PEN    149.97  tax  26.99  (exact 26.9946)
// INK    210.00  tax  37.80  (exact 37.8000)
// PAD     87.15  tax  15.69  (exact 15.6870)
// net 447.12, discount 22.36, tax 80.48, payable 505.24
// false true true
// 2 1
// Non-terminating decimal expansion; no exact representable decimal result.
// 33.33
// 33.33333333333333
// [33.34, 33.33, 33.33] -> 100.00
// HALF_UP: 2.35 2.36 -2.35
// HALF_EVEN: 2.34 2.36 -2.34
// DOWN: 2.34 2.35 -2.34
// 1000 1E+3
```

Points to take away:

- `equals` says `2.0` and `2.00` are **different** (different scale) and `compareTo` says they are the same value. So a `HashSet` keeps both and a `TreeSet` keeps one, the inconsistency Core Java 1 warned about. Compare money with `compareTo` (`a.compareTo(b) == 0`, `a.compareTo(BigDecimal.ZERO) > 0`), or normalise the scale first.
- `divide` without a scale throws `ArithmeticException` when the result has no finite decimal form. Always pass a scale and rounding mode (or a `MathContext`, which sets significant digits instead of decimal places).
- Rounding per line and rounding the total give different answers (here per-line tax is 80.48; rounding the exact total 80.4816 also gives 80.48, but with more lines the two drift apart). Which one the invoice uses is a business rule, and it is exactly the kind of difference a reconciliation team ends up chasing.
- The split gives `33.34 + 33.33 + 33.33`: no paisa created or lost.
- `stripTrailingZeros()` on `1000` gives `1E+3` (scale -3), which surprises people when printed; `toPlainString()` never uses exponent notation.

> **Pitfall:** `BigDecimal` is immutable. `total.add(lineTotal);` compiles, runs, and changes nothing. Assign the result.

> **Pitfall:** Do not mix `double` back in through the side door: `BigDecimal.valueOf(price.doubleValue() * qty)` throws away the exactness you paid for. Keep the whole calculation in `BigDecimal`, and convert to `double` only for charts.

> **Tip:** Use the constants `BigDecimal.ZERO`, `ONE`, `TEN`, and `BigDecimal.valueOf(long)` for integers. Fix one scale for stored amounts (2 for rupees, sometimes 4 for intermediate rates) and apply it in one place, for example in a `Money` value class or record.

::: deeper
**Alternatives.** Many payment systems store money as a `long` count of the smallest unit (paise or cents): fast, exact, and immune to scale confusion, but you must track the currency's minor units yourself (the Japanese yen has none, the Bahraini dinar has three). The JSR 354 Money and Currency API (reference implementation Moneta) provides `MonetaryAmount` with currency and rounding built in. `java.util.Currency.getInstance("INR").getDefaultFractionDigits()` returns 2.

**Databases and JSON.** Map `BigDecimal` to SQL `DECIMAL(19, 4)` or `NUMERIC(p, s)`, never to `FLOAT`/`DOUBLE`. JPA: `@Column(precision = 19, scale = 4)`. JDBC's `getBigDecimal` returns the column's scale, so `10.5` stored in a scale-2 column comes back as `10.50`, which matters for `equals`. JSON was covered in the previous topic: prefer `BigDecimal` fields, or strings for JavaScript clients.

**Performance.** `BigDecimal` arithmetic is roughly ten to a hundred times slower than `double` and allocates objects. For a web shop that is irrelevant. For risk engines doing billions of operations, teams use `long` fixed-point arithmetic instead.

**`MathContext`.** `MathContext.DECIMAL64` means 16 significant digits with `HALF_EVEN`, matching IEEE 754 decimal64; `DECIMAL128` gives 34 digits. A `MathContext` limits **precision** (total significant digits), whereas `setScale` controls **decimal places**. For money you usually want `setScale`.

**Other number traps nearby.** `Math.round(-2.5)` is `-2` (rounds half up towards positive infinity), integer division truncates (`7 / 2 == 3`), and `float` has only about 7 significant digits. `0.1f + 0.2f` happens to print `0.3`, but only because the rounded `float` sum lands on the same `float` as the literal `0.3f`; neither is exactly 0.3.

> **Interview:** "Why should you not use `double` for money?" Binary floating point cannot represent most decimal fractions exactly, so values like 0.1 are approximations; errors accumulate across additions and break equality checks (`0.1 + 0.2 != 0.3`). Use `BigDecimal` created from strings, with explicit scale and `RoundingMode`, or a `long` count of the smallest currency unit.

> **Interview:** "`new BigDecimal("2.0").equals(new BigDecimal("2.00"))`?" `false`, because `equals` compares both the unscaled value and the scale. `compareTo` returns 0 because the numeric values are equal. Use `compareTo` for numeric comparison and be careful with `BigDecimal` in `HashSet` or as `HashMap` keys.

> **Interview:** "What is banker's rounding?" `RoundingMode.HALF_EVEN`: round to the nearest neighbour, and on an exact tie choose the even digit (2.345 → 2.34, 2.355 → 2.36). Over many roundings the ties go up and down equally often, so totals carry no systematic upward bias.
:::

::: check
?? What do `new BigDecimal(1.1)` and `new BigDecimal("1.1")` hold, and which should you use?
The first holds the exact binary value of the double nearest to 1.1, a long string of digits slightly above 1.1 (`1.100000000000000088817841970012523233890533447265625`). The second holds exactly 1.1. Use the string constructor, or `BigDecimal.valueOf(1.1)` when you only have a double.

?? Why does `new BigDecimal("10").divide(new BigDecimal("4"))` work but `divide(new BigDecimal("3"))` throw?
10/4 is exactly 2.5, a terminating decimal, so `divide` can return it. 10/3 is 3.333... with no end, so without a scale and rounding mode `divide` throws `ArithmeticException: Non-terminating decimal expansion`.

?? Round 2.345 and 2.355 to two places with `HALF_EVEN`.
2.34 and 2.36: on an exact tie the result goes to the even last digit (4 and 6).
:::

::: resources
- Docs: [BigDecimal (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/math/BigDecimal.html) · scale, precision, `equals` vs `compareTo`, the double constructor warning
- Docs: [RoundingMode (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/math/RoundingMode.html) · a table of every mode applied to sample values
- Article: [BigDecimal and BigInteger in Java](https://www.baeldung.com/java-bigdecimal-biginteger) · Baeldung introduction
- Article: [What Every Computer Scientist Should Know About Floating-Point Arithmetic](https://docs.oracle.com/cd/E19957-01/806-3568/ncg_goldberg.html) · David Goldberg's classic explanation of binary floating point
- Repo: [JSR 354 Moneta reference implementation](https://github.com/JavaMoney/jsr354-ri) · `MonetaryAmount` with currencies and rounding
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 60: avoid float and double if exact answers are required
:::

### Maven: pom.xml, lifecycle, dependencies & scopes, plugins, multi-module

::: plain
Building a Java project by hand means downloading the right versions of a dozen libraries, compiling in the right order, running the tests and zipping everything into a JAR, every time and on every machine. Maven is the project's recipe card plus a well-stocked pantry. The recipe (`pom.xml`) lists the ingredients by name and version; Maven fetches them from a central store, and every cook who follows the card, on any laptop or build server, gets the same dish with one command.
:::

**Apache Maven** is the most widely used build tool for Java. It does three jobs: **dependency management** (download libraries and the libraries they need, from Maven Central or a company repository), a **standard build lifecycle** (compile, test, package, install, deploy, always in that order), and a **convention for project layout** so any Java developer can find their way around:

```text
shop-service/
├── pom.xml                      the Project Object Model: what and how to build
├── mvnw, mvnw.cmd, .mvn/        Maven Wrapper: pins the Maven version, no install needed
└── src/
    ├── main/java/               production code  (com/shopsphere/order/OrderService.java)
    ├── main/resources/          config files, application.yml, SQL
    ├── test/java/               JUnit tests
    └── test/resources/          test data
target/                          build output (classes, test reports, the JAR); never commit it
```

The examples in this topic are build files and terminal commands. They need Maven and network access to Maven Central, so they are shown as labelled snippets rather than programs you run with `java`.

**The POM.** Every artifact in the Maven world is identified by **coordinates**: `groupId` (the organisation, reversed domain), `artifactId` (the module name) and `version`, often written `groupId:artifactId:version` and nicknamed GAV.

```xml
<!-- pom.xml for a small ShopSphere order service (Maven 3.9) -->
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.shopsphere</groupId>
    <artifactId>order-service</artifactId>
    <version>1.4.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.release>21</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <junit.version>5.11.4</junit.version>
    </properties>

    <dependencies>
        <dependency>                                  <!-- default scope: compile -->
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.19.2</version>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.7.5</version>
            <scope>runtime</scope>                    <!-- only needed when the app runs -->
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>1.18.36</version>
            <scope>provided</scope>                   <!-- compile-time only, not packaged -->
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>                       <!-- only for src/test -->
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.5.3</version>              <!-- runs unit tests in the test phase -->
            </plugin>
        </plugins>
    </build>
</project>
```

`SNAPSHOT` marks a version still in development: Maven may fetch a newer build of the same snapshot version each time. A version without it (`1.4.0`) is a **release** and must never change once published.

**Dependency scopes** decide on which classpath a dependency appears:

| Scope | Compile | Test | Runtime / packaged | Transitive | Example |
|---|---|---|---|---|---|
| `compile` (default) | yes | yes | yes | yes | Jackson, Guava, Spring |
| `provided` | yes | yes | no (the server or JDK provides it) | no | Servlet API in a WAR, Lombok |
| `runtime` | no | yes | yes | yes | JDBC driver, logging backend |
| `test` | no (main code) | yes | no | no | JUnit, Mockito, Testcontainers |
| `system` | like provided, from a local path | | | | avoid: not portable |
| `import` | only in `dependencyManagement` with type `pom` | | | | import a BOM |

**Transitive dependencies and conflicts.** Jackson itself depends on `jackson-core` and `jackson-annotations`; Maven pulls those in automatically. When two paths bring different versions of the same library, Maven picks the **nearest** one in the dependency tree (fewest hops from your project), and among equals, the one **declared first**. It does not pick the newest. That is why `NoSuchMethodError` at runtime is the classic Maven problem: the code was compiled against one version and a different one won. Your tools:

```bash
./mvnw dependency:tree                          # who brings in what, and which version won
./mvnw dependency:tree -Dincludes=com.fasterxml.jackson.core
./mvnw dependency:analyze                       # used-but-undeclared and declared-but-unused
```

Fix a conflict by declaring the version you want directly (nearest wins), by pinning it in `dependencyManagement`, or by excluding the unwanted transitive dependency:

```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>legacy-client</artifactId>
    <version>2.3.1</version>
    <exclusions>
        <exclusion>                                   <!-- drop its old logging binding -->
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

**The lifecycle.** Maven has three built-in lifecycles: `clean` (delete `target/`), `default` (the build) and `site` (documentation). The default lifecycle is an ordered list of **phases**. Running a phase runs **every phase before it**:

[[fig:corefunc_mvn]]

| Phase | What happens (with default plugins) |
|---|---|
| `validate` | check the POM is correct |
| `compile` | `maven-compiler-plugin` compiles `src/main/java` into `target/classes` |
| `test` | `maven-surefire-plugin` runs unit tests (`*Test.java`); failures stop the build |
| `package` | `maven-jar-plugin` builds `target/order-service-1.4.0-SNAPSHOT.jar` |
| `verify` | integration tests and checks (`maven-failsafe-plugin`, coverage, static analysis) |
| `install` | copy the JAR into your local repository `~/.m2/repository` for other local projects |
| `deploy` | upload it to a shared repository (Nexus, Artifactory, GitHub Packages) |

**Plugins and goals.** Maven itself does almost nothing; **plugins** do the work. A plugin offers **goals** (`compiler:compile`, `surefire:test`, `spring-boot:run`), and the packaging type binds default goals to phases. You add or configure plugins in `<build><plugins>`, and you can run a goal directly (`./mvnw dependency:tree`).

```bash
./mvnw clean package              # fresh build + tests + JAR: the command you will type most
./mvnw clean verify               # what CI usually runs: adds integration tests and checks
./mvnw test -Dtest=OrderServiceTest          # one test class
./mvnw package -DskipTests        # build without running tests (compiles them)
./mvnw -q spring-boot:run         # run a Spring Boot app (with the Spring Boot plugin)
./mvnw versions:display-dependency-updates   # which libraries have newer versions
```

**Multi-module projects.** Larger systems split into modules that build together: a parent POM with `pom` packaging lists the modules and shares configuration, and modules depend on each other by coordinates. Maven's **reactor** works out the build order from those dependencies.

```xml
<!-- shopsphere/pom.xml: the parent (aggregator) -->
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.shopsphere</groupId>
    <artifactId>shopsphere-parent</artifactId>
    <version>1.4.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <modules>
        <module>common</module>               <!-- DTOs, Money, shared utilities -->
        <module>order-service</module>        <!-- depends on common -->
        <module>payment-service</module>      <!-- depends on common -->
    </modules>

    <dependencyManagement>                    <!-- versions decided once, for every module -->
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>3.4.5</version>
                <type>pom</type>
                <scope>import</scope>         <!-- a BOM: a curated list of compatible versions -->
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>

<!-- order-service/pom.xml: a child module -->
<project>
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.shopsphere</groupId>
        <artifactId>shopsphere-parent</artifactId>
        <version>1.4.0-SNAPSHOT</version>
    </parent>
    <artifactId>order-service</artifactId>
    <dependencies>
        <dependency>
            <groupId>com.shopsphere</groupId>
            <artifactId>common</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>                          <!-- no version: managed by the BOM -->
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
    </dependencies>
</project>
```

```bash
./mvnw -pl order-service -am package     # build one module plus the modules it needs (-am: also make)
```

`dependencyManagement` only **declares** versions; a module still has to list the dependency (without a version) to get it. Plain `dependencies` in a parent are inherited by every child, which is rarely what you want for anything but test libraries.

> **Pitfall:** "It builds on my machine" with Maven usually means a stale `~/.m2/repository` (a locally installed SNAPSHOT nobody else has) or a different Maven version. Commit the Maven Wrapper (`mvnw`) and build with it, so CI and every developer use the same Maven.

> **Pitfall:** `-DskipTests` and `-Dmaven.test.skip=true` differ: the first compiles tests but does not run them, the second does not even compile them (and breaks modules that depend on test JARs). Neither belongs in a CI pipeline.

> **Tip:** In a Spring Boot project the parent `spring-boot-starter-parent` (or the `spring-boot-dependencies` BOM) manages versions for hundreds of libraries that are tested together. Leave the versions out and let the BOM choose, and override a single one with a property (for example `<jackson-bom.version>`) only when you must.

::: deeper
**Effective POM.** Every POM inherits from Maven's **super POM**, which defines the default directories, the Central repository and default plugin versions. `./mvnw help:effective-pom` prints the fully merged POM (super POM + parents + profiles), the first thing to look at when "where does this setting come from?".

**Repositories and settings.** Artifacts resolve from the local repository `~/.m2/repository` first, then remote repositories (Maven Central by default). Companies put a proxy such as Nexus or Artifactory in front, configured with a `<mirror>` in `~/.m2/settings.xml`, which also holds credentials for deployment. Never put passwords in `pom.xml`, which is committed.

**Profiles.** `<profiles>` activate extra configuration by flag (`-Pintegration`), by JDK version, by OS or by a property. Useful for optional integration tests; abused for environment-specific builds, which belong in runtime configuration instead (build once, deploy the same artifact everywhere).

**Executable JARs.** The plain JAR from `maven-jar-plugin` contains only your classes. To run with `java -jar`, you need the dependencies too: Spring Boot's plugin repackages a "fat JAR" with nested JARs and a launcher; `maven-shade-plugin` and `maven-assembly-plugin` do the same for non-Spring apps.

**Version notes.** Maven 3.9 is the version most projects use today. Maven 4 adds a separate consumer POM (the POM published to repositories is stripped of build-only details), a new POM model version 4.1.0 with better multi-module support, and requires Java 17 to run. Builds on Maven 3 POMs keep working, so you will see both.

> **Interview:** "Explain the Maven build lifecycle." The default lifecycle is an ordered sequence of phases: validate, compile, test, package, verify, install, deploy. Invoking a phase runs all earlier phases. Plugins bind goals to phases (compiler to compile, surefire to test, jar to package). `clean` is a separate lifecycle, which is why you type `mvn clean install`.

> **Interview:** "What are dependency scopes?" `compile` (default, everywhere, transitive), `provided` (compile and test, but supplied at runtime by the container), `runtime` (not needed to compile, needed to run, such as a JDBC driver), `test` (test code only), plus `system` (avoid) and `import` (BOMs in `dependencyManagement`).

> **Interview:** "Two libraries need different versions of the same dependency. What does Maven do?" Dependency mediation picks the nearest version in the tree, first declaration winning ties, regardless of which is newer. Inspect with `mvn dependency:tree`, then fix by declaring the version explicitly, pinning it in `dependencyManagement`, or excluding the transitive one. The Maven Enforcer plugin's `dependencyConvergence` rule can fail the build when versions disagree.
:::

::: check
?? You run `mvn install`. Which phases run, and where does the JAR end up?
All default-lifecycle phases up to `install`: validate, compile, test, package, verify, install (plus some internal ones such as `process-resources`). The JAR is in `target/` and is also copied to `~/.m2/repository/com/shopsphere/order-service/<version>/`.

?? Which scope fits each: the PostgreSQL JDBC driver, JUnit, the Servlet API in an app deployed to an external Tomcat?
`runtime`, `test` and `provided`.

?? What is the difference between `dependencies` and `dependencyManagement` in a parent POM?
`dependencies` adds the libraries to every child module. `dependencyManagement` only fixes versions (and scopes, exclusions); a child gets the library only if it declares it, and then omits the version.
:::

::: resources
- Docs: [Maven in 5 Minutes](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html) · official first steps
- Docs: [Introduction to the Build Lifecycle](https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html) · every phase and its default bindings
- Docs: [Introduction to the Dependency Mechanism](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html) · scopes, mediation, `dependencyManagement` and BOMs
- Docs: [POM Reference](https://maven.apache.org/pom.html) · every element of `pom.xml`
- Docs: [Maven Wrapper](https://maven.apache.org/wrapper/) · pin the Maven version per project
- Article: [Apache Maven Tutorial](https://www.baeldung.com/maven) · Baeldung overview with links to deeper guides
- Article: [Multi-Module Project with Maven](https://www.baeldung.com/maven-multi-module) · Baeldung walkthrough of parent and child POMs
:::

### Gradle basics (read and modify a build.gradle)

::: plain
Gradle does the same job as Maven, but its recipe card is a short script instead of a long form. Where Maven says "fill in these boxes in this fixed order", Gradle says "here is a program that describes the build". That makes it more flexible and usually faster, because it remembers what it already cooked and skips work that has not changed. Android projects and many Spring projects use it, so you must be able to read one and add a dependency.
:::

**Gradle** builds a project by running **tasks** (compile, test, jar...) arranged in a dependency graph. Plugins add tasks and conventions; the `java` plugin gives you exactly the Maven directory layout (`src/main/java`, `src/test/java`) and tasks such as `compileJava`, `test`, `jar` and `build`. It downloads dependencies from the same Maven repositories, using the same coordinates.

Build scripts come in two languages: the **Groovy DSL** (`build.gradle`) and the **Kotlin DSL** (`build.gradle.kts`, the default for new projects since Gradle 8.2). They express the same model; the Kotlin one is type-checked, so your IDE gives better completion. These are build-script snippets that need Gradle installed or its wrapper, not programs you can run with `java`.

```groovy
// build.gradle (Groovy DSL) for the ShopSphere order service
plugins {
    id 'java'
    id 'org.springframework.boot' version '3.4.5'
    id 'io.spring.dependency-management' version '1.1.7'
}

group = 'com.shopsphere'
version = '1.4.0-SNAPSHOT'

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)   // Gradle finds or downloads JDK 21
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web'      // version from the Boot BOM
    implementation 'com.fasterxml.jackson.datatype:jackson-datatype-jsr310'
    runtimeOnly 'org.postgresql:postgresql'
    compileOnly 'org.projectlombok:lombok'
    annotationProcessor 'org.projectlombok:lombok'
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}

tasks.named('test') {
    useJUnitPlatform()                                 // run JUnit 5 tests
}
```

The same in the Kotlin DSL; note double quotes, parentheses and `=`:

```text
// build.gradle.kts (Kotlin DSL)
plugins {
    java
    id("org.springframework.boot") version "3.4.5"
    id("io.spring.dependency-management") version "1.1.7"
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    runtimeOnly("org.postgresql:postgresql")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
```

**Dependency configurations** play the role of Maven scopes:

| Gradle configuration | Closest Maven scope | Meaning |
|---|---|---|
| `implementation` | `compile` | needed to compile and run; **not** exposed to consumers' compile classpath |
| `api` (needs the `java-library` plugin) | `compile` | like `implementation`, and exposed to modules that depend on you |
| `compileOnly` | `provided` | compile only, not at runtime (Lombok, annotations) |
| `runtimeOnly` | `runtime` | runtime only (JDBC driver) |
| `testImplementation` | `test` | compile and run tests |
| `testRuntimeOnly` | `test` (runtime part) | test runtime only (JUnit launcher, engines) |
| `annotationProcessor` | compiler plugin config | annotation processors such as Lombok or MapStruct |

The `implementation` vs `api` split is something Maven does not have: if `order-service` uses Jackson only inside its own code, `implementation` keeps Jackson off the compile classpath of modules that depend on it. They cannot accidentally use it, and changing it does not force them to recompile.

**Everyday commands** (always through the wrapper, `gradlew`, which pins the Gradle version just like `mvnw`):

```bash
./gradlew build                  # compile, test, assemble JARs: like mvn verify
./gradlew test --tests '*OrderServiceTest'   # one test class
./gradlew bootRun                # run a Spring Boot app
./gradlew dependencies --configuration runtimeClasspath   # the dependency tree
./gradlew dependencyInsight --dependency jackson-databind  # why this version?
./gradlew tasks                  # list available tasks
./gradlew clean build --scan     # publish a build scan to inspect timing and failures
```

**Multi-project builds.** `settings.gradle` names the root project and includes subprojects; each subproject has its own `build.gradle`, and project dependencies use `project(':common')`:

```groovy
// settings.gradle
rootProject.name = 'shopsphere'
include 'common', 'order-service', 'payment-service'

// order-service/build.gradle
dependencies {
    implementation project(':common')
}
```

**Version catalogs.** Modern Gradle builds keep versions in one file, `gradle/libs.versions.toml`, and refer to them as `libs.jackson.databind` in build scripts, the Gradle counterpart of a Maven BOM in a parent POM.

**Maven vs Gradle at a glance.**

| | Maven | Gradle |
|---|---|---|
| Build file | `pom.xml` (XML, declarative) | `build.gradle(.kts)` (Groovy/Kotlin code) |
| Model | fixed lifecycle of phases | graph of tasks |
| Speed | rebuilds what the phases cover | incremental tasks, build cache, daemon: often much faster on large builds |
| Flexibility | plugins only; custom logic is awkward | custom tasks and logic in the script |
| Learning curve | gentle, very predictable | steeper; scripts can become clever and opaque |
| Typical users | most enterprise Java and Spring back ends | Android, many newer Spring and Kotlin projects |

> **Pitfall:** `compile` and `testCompile` configurations were removed in Gradle 7. Old tutorials (and old Stack Overflow answers) still show them; use `implementation` and `testImplementation`.

> **Pitfall:** Do not add a dependency version in `build.gradle` when the Spring dependency-management plugin or a platform/BOM already manages it; you might downgrade a library that the rest of Spring Boot was tested with.

> **Tip:** To change a Gradle build at work, you usually only touch three places: `plugins`, `dependencies` and a task configuration block. Copy the style of the lines next to the one you add (quotes, parentheses) and run `./gradlew build` before committing.

::: deeper
**Build phases.** A Gradle build has three phases: **initialization** (read `settings.gradle`, decide which projects take part), **configuration** (run every build script to build the task graph) and **execution** (run only the tasks needed for the requested ones, in dependency order). Code in a build script's top level runs during configuration on every build, even `./gradlew help`; work belongs inside task actions (`doLast { }`), not at the top level.

**Why Gradle is fast.** Each task declares inputs and outputs; if they have not changed since the last run, Gradle marks the task `UP-TO-DATE` and skips it. The **build cache** reuses outputs across branches and even across machines (a remote cache shared by CI and developers). The **daemon** keeps a warmed-up JVM running between builds. The **configuration cache** (stable since Gradle 8.1) skips the configuration phase when nothing in the scripts changed.

**Toolchains.** The `java.toolchain` block decouples the JDK that runs Gradle from the JDK that compiles your code; Gradle can auto-provision the requested JDK. Maven's equivalent is `maven.compiler.release` (which only controls the target, not the compiler JDK) or the toolchains plugin.

**Version notes.** Gradle 8 is still common in existing projects; Gradle 9 (released in 2025) requires Java 17 to run Gradle itself, makes the configuration cache the preferred mode, and uses Kotlin 2 for the Kotlin DSL. Check `gradle/wrapper/gradle-wrapper.properties` to see which version a project uses.

> **Interview:** "Maven or Gradle: which and why?" Both use the same repositories and coordinates. Maven is declarative XML with a fixed lifecycle: predictable and easy to read, the enterprise default. Gradle is a task graph configured in Groovy or Kotlin, with incremental builds, build caching and a daemon, so it is faster on big builds and more flexible, at the cost of more complex scripts. Pick what the team uses; be able to read both.

> **Interview:** "Difference between `implementation` and `api` in Gradle?" Both put the dependency on your module's compile and runtime classpaths. `api` also exposes it on the compile classpath of modules that depend on yours (part of your public API); `implementation` keeps it internal, which avoids leaking dependencies and reduces recompilation.
:::

::: check
?? Translate this Maven dependency to Gradle Groovy DSL: PostgreSQL driver, scope `runtime`, version 42.7.5.
`runtimeOnly 'org.postgresql:postgresql:42.7.5'`

?? Which file decides which subprojects are part of a Gradle build, and which file pins the Gradle version?
`settings.gradle` (or `settings.gradle.kts`) lists the subprojects with `include`. `gradle/wrapper/gradle-wrapper.properties` pins the Gradle version used by `./gradlew`.

?? A task prints `UP-TO-DATE` in the build output. What does that mean?
Its declared inputs and outputs have not changed since it last ran, so Gradle skipped it and kept the previous outputs.
:::

::: resources
- Docs: [Gradle User Manual](https://docs.gradle.org/current/userguide/userguide.html) · the official documentation home
- Docs: [Building Java & JVM projects](https://docs.gradle.org/current/userguide/building_java_projects.html) · the official guide to the `java` plugin, source sets and dependencies
- Docs: [The Java Library Plugin](https://docs.gradle.org/current/userguide/java_library_plugin.html) · `api` vs `implementation` explained
- Docs: [Gradle vs Maven comparison](https://gradle.org/maven-vs-gradle/) · Gradle's own view of the differences
- Docs: [Gradle (Spring Boot plugin reference)](https://docs.spring.io/spring-boot/gradle-plugin/index.html) · `bootRun`, `bootJar` and dependency management in Spring builds
- Article: [Introduction to Gradle](https://www.baeldung.com/gradle) · Baeldung overview of tasks, plugins and dependencies
:::

## Wrap-up

### Cheat sheet

| Task | Code |
|---|---|
| Behaviour as a parameter | `Predicate<Order> paid = o -> o.status() == PAID;` then `orders.stream().filter(paid)` |
| Combine rules | `inStock.and(cheap)`, `paid.negate()`, `filter(Predicate.not(String::isBlank))`, `f.andThen(g)` |
| Method reference kinds | `Integer::parseInt`, `System.out::println`, `String::toUpperCase`, `ArrayList::new` |
| Filter, transform, collect | `list.stream().filter(...).map(...).toList()` |
| Nested lists to one stream | `orders.stream().flatMap(o -> o.lines().stream())` |
| Sum of a field | `mapToInt(Line::qty).sum()`; `BigDecimal`: `map(Line::total).reduce(BigDecimal.ZERO, BigDecimal::add)` |
| Second highest distinct | `distinct().sorted(Comparator.reverseOrder()).skip(1).findFirst()` |
| Count per key | `collect(groupingBy(Function.identity(), counting()))` |
| Sum / set / list per key | `groupingBy(Sale::city, summingInt(Sale::qty))`, `mapping(Sale::sku, toSet())` |
| Best per group | `groupingBy(E::dept, collectingAndThen(maxBy(comparingInt(E::salary)), Optional::get))` |
| Map with possible duplicate keys | `toMap(k, v, Integer::sum, LinkedHashMap::new)` |
| Two groups | `partitioningBy(n -> n % 2 == 0)` |
| Join strings | `collect(joining(", ", "[", "]"))` |
| Optional done right | `findById(id).map(Customer::name).orElse("guest")`, `orElseThrow(() -> new NotFound(id))` |
| Expensive fallback | `orElseGet(this::loadDefault)`, never `orElse(loadDefault())` |
| Store a timestamp | `Instant.now(clock)`; display with `instant.atZone(ZoneId.of("Asia/Kolkata"))` |
| Format a date | `DateTimeFormatter.ofPattern("dd MMM yyyy")`: `yyyy`, never `YYYY` |
| Read a big file | `try (Stream<String> lines = Files.lines(path)) { ... }` |
| JSON | `mapper.writeValueAsString(dto)`, `mapper.readValue(json, Dto.class)`, `new TypeReference<List<Dto>>() { }` |
| Money | `new BigDecimal("49.99")`, `setScale(2, RoundingMode.HALF_UP)`, compare with `compareTo` |
| Build | `./mvnw clean verify`, `./mvnw dependency:tree`; `./gradlew build`, `./gradlew dependencies` |

### Practice

::: try
**1. Live coding: top customers per city.** ShopSphere gives you a list of orders, each with a customer, a city and an amount (`BigDecimal`). For each city (in alphabetical order), print the two customers with the highest **total** spend in that city, highest first, breaking ties by name. Then print the overall revenue. Use streams and collectors only, no explicit loops for the aggregation.
:::

?? A solution to 1

```java
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.*;
import static java.util.stream.Collectors.*;

public class TopCustomers {
    record Order(String customer, String city, BigDecimal amount) { }

    public static void main(String[] args) {
        List<Order> orders = List.of(
                new Order("Asha", "Pune", new BigDecimal("357.00")),
                new Order("Ravi", "Pune", new BigDecimal("120.50")),
                new Order("Asha", "Pune", new BigDecimal("80.00")),
                new Order("John", "Pune", new BigDecimal("437.00")),
                new Order("Meena", "Delhi", new BigDecimal("899.00")),
                new Order("Lata", "Delhi", new BigDecimal("98.00")),
                new Order("Kiran", "Delhi", new BigDecimal("450.00")),
                new Order("Kiran", "Delhi", new BigDecimal("449.00")));

        // city -> (customer -> total)
        Map<String, Map<String, BigDecimal>> totals = orders.stream().collect(groupingBy(
                Order::city, TreeMap::new,
                groupingBy(Order::customer, reducing(BigDecimal.ZERO, Order::amount, BigDecimal::add))));

        totals.forEach((city, byCustomer) -> {
            List<String> top = byCustomer.entrySet().stream()
                    .sorted(Map.Entry.<String, BigDecimal>comparingByValue().reversed()
                            .thenComparing(Map.Entry.comparingByKey()))
                    .limit(2)
                    .map(e -> e.getKey() + " " + e.getValue())
                    .toList();
            System.out.println(city + ": " + top);
        });

        System.out.println("revenue " + orders.stream().map(Order::amount).reduce(BigDecimal.ZERO, BigDecimal::add));
    }
}
// Output:
// Delhi: [Kiran 899.00, Meena 899.00]
// Pune: [Asha 437.00, John 437.00]
// revenue 2890.50
```

The ties are deliberate: Kiran's two orders add up to Meena's single order, and Asha's two orders equal John's. Without `thenComparing(comparingByKey())` the order of tied customers would depend on `HashMap` iteration order, which an interviewer will ask about. Note also that `compareTo`-based sorting treats `899.00` and `899.0` as equal, which is what you want for money.

::: try
**2. Code review.** Find at least five problems in this service method and say how to fix each.

```java
public double monthlyRevenue(List<Order> orders) {
    List<Double> amounts = new ArrayList<>();
    orders.parallelStream()
          .filter(o -> o.getPlacedAt().getMonth() == LocalDateTime.now().getMonth())
          .forEach(o -> amounts.add(o.getAmount().doubleValue()));
    Customer vip = customerRepo.findById("C1").get();
    String label = Optional.ofNullable(vip.getTier()).orElse(loadDefaultTierFromDb());
    return amounts.stream().reduce(0.0, Double::sum);
}
```
:::

?? Answer to 2
1. **Shared mutable state in a parallel stream**: `amounts.add` from many threads on an `ArrayList` loses elements or throws. Use `map(...)` and a reduction instead of `forEach`, and drop `parallelStream` unless measured. 2. **Money as `double`**: `doubleValue()` and `Double::sum` introduce rounding errors; keep `BigDecimal` and `reduce(BigDecimal.ZERO, BigDecimal::add)`, and return `BigDecimal`. 3. **Time zone and testability**: `LocalDateTime.now()` uses the server's default zone and changes during the stream; comparing only the month also mixes up years (March 2024 and March 2025). Inject a `Clock`, compute the month once as a `YearMonth` in the business zone, and compare `YearMonth.from(order.placedAt().atZone(zone))`. 4. **`Optional.get()` without a check** throws `NoSuchElementException` when C1 does not exist; use `orElseThrow(() -> new CustomerNotFoundException("C1"))`. 5. **`orElse` with an expensive call**: `loadDefaultTierFromDb()` runs on every call even when the tier exists; use `orElseGet(this::loadDefaultTierFromDb)`. Bonus: the `vip` and `label` lines do nothing for the result (dead code with a database call), and the method mixes querying with formatting.

::: try
**3. Dates: weekend orders and delivery dates.** Orders are stored as `Instant`s. For a customer in `Asia/Kolkata`, print each order's local date and day of week, flag orders placed on a weekend **in the customer's zone**, and compute a promised delivery date three business days later (skipping Saturday and Sunday). Use `2025-03-28T20:00:00Z`, `2025-03-29T10:15:00Z` and `2025-03-30T19:00:00Z`.
:::

?? A solution to 3

```java
import java.time.*;
import java.util.List;

public class WeekendOrders {
    static boolean weekend(LocalDate d) {
        return d.getDayOfWeek() == DayOfWeek.SATURDAY || d.getDayOfWeek() == DayOfWeek.SUNDAY;
    }

    static LocalDate plusBusinessDays(LocalDate start, int days) {
        LocalDate d = start;
        while (days > 0) {
            d = d.plusDays(1);
            if (!weekend(d)) days--;
        }
        return d;
    }

    public static void main(String[] args) {
        ZoneId zone = ZoneId.of("Asia/Kolkata");
        List<Instant> placed = List.of(
                Instant.parse("2025-03-28T20:00:00Z"),
                Instant.parse("2025-03-29T10:15:00Z"),
                Instant.parse("2025-03-30T19:00:00Z"));
        for (Instant i : placed) {
            LocalDate local = i.atZone(zone).toLocalDate();
            System.out.println(i + " -> " + local + " " + local.getDayOfWeek()
                    + (weekend(local) ? " (weekend)" : "") + ", deliver by " + plusBusinessDays(local, 3));
        }
    }
}
// Output:
// 2025-03-28T20:00:00Z -> 2025-03-29 SATURDAY (weekend), deliver by 2025-04-02
// 2025-03-29T10:15:00Z -> 2025-03-29 SATURDAY (weekend), deliver by 2025-04-02
// 2025-03-30T19:00:00Z -> 2025-03-31 MONDAY, deliver by 2025-04-03
```

The first order is a Friday in UTC but already Saturday 01:30 in India, and the last one is a Sunday in UTC but Monday 00:30 in India. Evaluating "weekend" in UTC, as a server in a UTC container would by default, gets both wrong. In a real system, public holidays come from a table, and the loop checks it too.

::: try
**4. Live coding: most frequent word.** Given a paragraph of customer reviews, return the most frequent word, ignoring case and punctuation, and ignoring the stop words "the", "a", "is", "and". On a tie, return the alphabetically first word. Then return the frequency of each word length (length to count) as a sorted map.
:::

?? A solution to 4

```java
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

public class FrequentWord {
    public static void main(String[] args) {
        String reviews = "Great pen! The ink is smooth, and the pen is light. Smooth delivery; great price, great pen.";
        Set<String> stop = Set.of("the", "a", "is", "and");

        List<String> words = Arrays.stream(reviews.toLowerCase().split("[^a-z]+"))
                .filter(w -> !w.isBlank() && !stop.contains(w))
                .toList();

        String top = words.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                .max(Map.Entry.<String, Long>comparingByValue()
                        .thenComparing(Map.Entry.comparingByKey(Comparator.reverseOrder())))
                .map(Map.Entry::getKey)
                .orElseThrow();
        System.out.println(top);

        Map<Integer, Long> byLength = words.stream()
                .collect(Collectors.groupingBy(String::length, TreeMap::new, Collectors.counting()));
        System.out.println(byLength);
    }
}
// Output:
// great
// {3=4, 5=5, 6=2, 8=1}
```

"great" and "pen" both appear three times; the tie goes to the alphabetically first. With `max`, the tie-breaker must prefer the **smaller** key, hence `comparingByKey(Comparator.reverseOrder())`; with `sorted(...).findFirst()` you would write the order the other way round. Splitting on `[^a-z]+` removes punctuation, and the `isBlank` filter drops the empty string that `split` produces if the text starts with a non-letter.

::: try
**5. Build file detective.** `mvn dependency:tree` for your service shows `jackson-databind:2.13.0` coming from an old internal client library, while your code uses an API added in Jackson 2.15. The build compiles on your laptop but fails at runtime with `NoSuchMethodError`. Explain why, and give two ways to fix it in `pom.xml`, plus the Gradle command you would use to investigate the same problem in a Gradle build.
:::

?? Answer to 5
Maven's dependency mediation chose 2.13.0 because it is nearest in the tree (or declared first among equally near paths), not because it is newest. Your IDE or an earlier build may have compiled against a newer version, but the runtime classpath has 2.13.0, which lacks the method, hence `NoSuchMethodError`. Fix one: declare `jackson-databind` with the version you need directly in your `dependencies` (a direct dependency is nearest and wins). Fix two: pin the version for the whole build in `dependencyManagement` (or import the `jackson-bom`), optionally also adding an `exclusion` on the internal client. Add the Maven Enforcer `dependencyConvergence` rule to catch the next conflict at build time. In Gradle: `./gradlew dependencyInsight --dependency jackson-databind --configuration runtimeClasspath` shows which version was selected and why (Gradle picks the **highest** requested version by default, so the same conflict behaves differently there).

### What comes next

You can now write modern, functional Java: lambdas and method references, stream pipelines and collectors for any grouping or ranking question an interviewer throws at you, `Optional` used the way it was designed, and `java.time` code that survives time zones and daylight saving. You can also cross the program's boundaries safely: stream large files, parse CSV, map objects to JSON with Jackson, compute money exactly with `BigDecimal`, and read or change a Maven or Gradle build. That completes Core Java. The next section, **Advanced Java**, is a track overview of what sits on top: multithreading and the Java Memory Model (threads, locks, executors, `CompletableFuture`, `ForkJoinPool` and virtual threads), JVM internals and performance (class loading, memory areas, garbage collection, the JIT), and modern Java from 17 to 25 (pattern matching, record patterns and the newest language features). That is where the common `ForkJoinPool` behind parallel streams, and the `invokedynamic` behind lambdas, get their full explanation.
