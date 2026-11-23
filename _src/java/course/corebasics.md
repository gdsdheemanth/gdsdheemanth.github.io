This is where Core Java starts, and it is written for someone who may be meeting Java for the first time. You know SQL and you have written some code before, so you already understand ideas such as values, conditions and loops. What is new is how Java does them: every variable has a declared type, every piece of code lives inside a class, and a program is compiled before it runs. By the end of this section you will have a working Java setup and you will understand the language basics and object-oriented programming (OOP) well enough to read real Spring Boot code and to answer the classic Core Java interview questions.

**What you will be able to do afterwards**

- Install JDK 21, compile and run Java from the terminal and from IntelliJ IDEA, and explain what the JDK, JRE and JVM each do.
- Use primitive types, wrapper classes, strings, arrays, loops and the modern `switch` correctly, and avoid the traps interviewers ask about (the `Integer` cache, `==` on strings, integer overflow).
- Design classes with constructors, encapsulation, inheritance, interfaces and polymorphism, and explain dynamic dispatch.
- Write correct `equals` and `hashCode` methods, build immutable classes, and choose between enums, records and sealed classes.

**The big picture.** Java is a **compiled, statically typed, object-oriented** language that runs on a virtual machine. You write `.java` files; the compiler (`javac`) checks every type and turns them into **bytecode** (`.class` files); the **Java Virtual Machine** (JVM) loads that bytecode and runs it, compiling the busy parts into fast machine code while the program runs. Inside your program, everything you build is either a **primitive value** (a plain number, character or boolean) or an **object** on the heap that you reach through a **reference**. Hold on to that last sentence: most Java surprises, from `==` on strings to "Java is pass-by-value", follow from it.

Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more.

> **Tip:** Keep `jshell` (Java's interactive prompt, installed with the JDK) open while you read. Type `jshell` in a terminal, paste a line such as `Integer.valueOf(127) == Integer.valueOf(127)`, and see the answer. It is the quickest way to settle any doubt.

## Setup & language basics

Before writing classes you need a working toolchain and the small building blocks every program uses: types, operators, loops, arrays and strings. This group covers them in that order. If you come from SQL, the biggest change is that Java checks types **before** the program runs: a variable declared as `int` can never hold text, and the compiler refuses code that tries.

[[fig:corebasics_pipeline]]

### JDK vs JRE vs JVM; install JDK 21 (Eclipse Temurin), IntelliJ IDEA Community

::: plain
Think of a recipe written in English that must be cooked in kitchens all over the world. The **JDK** (Java Development Kit) is the full kit for the person writing recipes: it includes a translator (`javac`) that turns your recipe into a universal recipe card format called **bytecode**. The **JVM** (Java Virtual Machine) is the cook in each kitchen who can read those cards, on Windows, macOS or Linux alike. The **JRE** (Java Runtime Environment) is just the cook plus the pantry of standard ingredients (the core libraries), without the writing tools. As a developer you install the JDK, which contains everything.
:::

**The three pieces.**

| Piece | Contains | Who needs it |
|---|---|---|
| **JVM** | The engine that loads `.class` files, verifies them, interprets bytecode and compiles hot code to machine code, plus the garbage collector | Every machine that runs Java |
| **JRE** | A JVM plus the standard class library (`java.lang`, `java.util`...) | Machines that only run Java programs |
| **JDK** | A JRE plus development tools: `javac`, `jshell`, `javap`, `jar`, `jdeps`, `jlink`, a debugger | Developers (you) |

Since Java 11, Oracle and most vendors no longer ship a separate JRE download; you install a JDK, and for production you either use the JDK or build a trimmed runtime with `jlink`. You will still hear "JRE" in interviews, so know what it means.

**Installing JDK 21.** Java is open source (the OpenJDK project), and several vendors build and support it. **Eclipse Temurin** from the Adoptium project is free, well tested and the usual choice. Pick one way:

```bash
# macOS (Homebrew)
brew install --cask temurin@21

# Windows (winget)
winget install EclipseAdoptium.Temurin.21.JDK

# macOS or Linux, managing several versions with SDKMAN
sdk install java 21.0.9-tem       # the exact version string comes from: sdk list java
```

Then open a new terminal and check:

```bash
java -version      # should mention 21
javac -version     # the compiler; if this is missing you installed a JRE only
```

If `java -version` shows an older version, another Java is earlier on your `PATH` (the list of folders the shell searches for commands). Set `JAVA_HOME` to the JDK folder and put `$JAVA_HOME/bin` first on `PATH`.

**From source to running program.** Create `Hello.java`:

```java
public class Hello {
    public static void main(String[] args) {
        int total = 2 + 3;
        System.out.println("Hello, Java! total = " + total);
    }
}
// Output:
// Hello, Java! total = 5
```

Compile and run it in two steps:

```bash
javac Hello.java     # creates Hello.class (bytecode)
java Hello           # starts a JVM, loads class Hello, calls its main method
```

Note that `java` takes the **class name** (`Hello`), not the file name. Since Java 11 you can also run a single source file directly with `java Hello.java`: the launcher compiles it in memory and runs it. That is perfect for small experiments, and every complete example in this course runs this way.

**IntelliJ IDEA.** Most Java teams use IntelliJ IDEA. Download it from JetBrains (or use the JetBrains Toolbox app). The free edition is enough for this whole course, including Maven and Gradle projects; from version 2025.3 JetBrains ships a single IntelliJ IDEA download in which the former Community features stay free. Create a new project, choose "Java", select your Temurin 21 JDK as the project SDK (IntelliJ can also download one for you), and run `main` with the green arrow in the gutter. Learn three shortcuts early: run, debug, and "show intention actions" (Alt+Enter, or Option+Enter on macOS), which offers fixes for red underlines.

> **Pitfall:** "error: class Hello is public, should be declared in a file named Hello.java" means the file name and the public class name differ. A public top-level class must live in a file with exactly the same name, including capital letters.

> **Tip:** Use the debugger from week one. Click the gutter to set a breakpoint, run in debug mode, then step over (F8) and step into (F7) while watching the variables panel. It teaches you more than any `println`.

::: deeper
**What the bytecode looks like.** `javap -c Hello` disassembles the class file. The interesting part:

```text
public static void main(java.lang.String[]);
  Code:
     0: iconst_5
     1: istore_1
     2: getstatic     #7    // Field java/lang/System.out:Ljava/io/PrintStream;
     5: iload_1
     6: invokedynamic #13,  0   // InvokeDynamic #0:makeConcatWithConstants:(I)Ljava/lang/String;
    11: invokevirtual #17   // Method java/io/PrintStream.println:(Ljava/lang/String;)V
    14: return
```

Three things to notice. The compiler already computed `2 + 3` (constant folding), so the bytecode just pushes `5`. The bytecode is a **stack machine**: instructions push values onto an operand stack and pop them. And string `+` became an `invokedynamic` call (since Java 9, JEP 280), which lets the JVM choose the fastest concatenation strategy at run time. A class file starts with the magic bytes `CAFEBABE`, followed by its version: Java 21 writes major version 65, and an older JVM refuses newer class files with `UnsupportedClassVersionError`.

**What the JVM does when you run `java Hello`.**

1. **Class loading.** Three loaders cooperate: the bootstrap loader (core `java.*` classes), the platform loader, and the application loader (your classpath). Each asks its parent first (parent delegation), so nobody can replace `java.lang.String` with their own.
2. **Linking.** The **verifier** checks that the bytecode is type-safe and cannot corrupt memory; then static fields get default values and symbolic references are resolved.
3. **Initialization.** Static initializers run, once, the first time the class is actively used.
4. **Execution.** The interpreter runs bytecode immediately. Methods that run often become "hot" and the **JIT** (just-in-time) compilers turn them into native code: C1 compiles quickly with light optimization, C2 later recompiles the hottest code with aggressive optimization (inlining, escape analysis, loop unrolling). This is **tiered compilation**, and it is why Java programs get faster after a warm-up period.

> **Interview:** "Is Java compiled or interpreted?" Both. `javac` compiles source to platform-independent bytecode ahead of time. At run time the JVM interprets that bytecode and JIT-compiles hot methods to native machine code. That combination gives "write once, run anywhere" with near-native speed.

> **Interview:** "JDK vs JRE vs JVM?" The JVM runs bytecode. The JRE is the JVM plus the standard libraries, enough to run programs. The JDK is the JRE plus the developer tools (`javac`, `jshell`, `javap`, `jar`), needed to build programs.

**Version notes.** Java releases a new version every six months; every two years one becomes a long-term support (LTS) release: 8, 11, 17, 21 and 25 (September 2025). Java 22 added multi-file source launching (`java Main.java` can now use other source files in the same folder). Java 25 finalized compact source files (JEP 512): a file can contain just `void main() { IO.println("Hi"); }` with no class declaration, which makes first programs shorter. Code in this course targets 21, the version most companies run.
:::

::: check
?? What is the difference between the JDK and the JRE?
The JRE is the JVM plus the standard class library, enough to run Java programs. The JDK adds the development tools, above all the compiler `javac`, plus `jshell`, `javap`, `jar` and others.

?? What does `javac Hello.java` produce, and what does `java Hello` do with it?
It produces `Hello.class`, which holds bytecode. `java Hello` starts a JVM, loads and verifies that class, and calls its `public static void main(String[] args)` method; hot code is later JIT-compiled to native code.

?? Why can the same `.class` file run on Windows and Linux?
Bytecode targets the JVM, not a real processor. Each operating system has its own JVM that translates the same bytecode into its own machine code.
:::

::: resources
- Docs: [Getting started with Java](https://dev.java/learn/getting-started/) · the official first steps: install, compile, run
- Docs: [Eclipse Temurin downloads](https://adoptium.net/temurin/releases/) · installers for every platform and version
- Docs: [The java launcher](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html) · every option, including source-file mode
- Article: [JVM architecture explained](https://www.baeldung.com/jvm-vs-jre-vs-jdk) · Baeldung on JDK vs JRE vs JVM
- Spec: [JEP 512: Compact source files and instance main methods](https://openjdk.org/jeps/512) · the Java 25 simplification for first programs
- Docs: [IntelliJ IDEA: create your first Java application](https://www.jetbrains.com/help/idea/creating-and-running-your-first-java-application.html) · project setup, run and debug
:::

### Primitive types, wrapper classes, autoboxing, type casting

::: plain
Java has two kinds of values. **Primitives** are plain values such as the number `42` or the letter `'A'`: small, fast, stored directly in the variable, like coins in your pocket. **Objects** are bigger things stored elsewhere in memory, and a variable only holds a **reference** to them, like a coat-check ticket. Each primitive has a matching object version called a **wrapper class** (`int` has `Integer`), needed wherever Java insists on objects, such as in lists. Java converts between the two for you, which is convenient but hides a few traps.
:::

**The eight primitive types.**

| Type | Size | Range or values | Default (as a field) | Example |
|---|---|---|---|---|
| `byte` | 8 bits | -128 to 127 | `0` | `byte b = 10;` |
| `short` | 16 bits | -32,768 to 32,767 | `0` | `short s = 1000;` |
| `int` | 32 bits | about -2.1 billion to 2.1 billion | `0` | `int count = 42;` |
| `long` | 64 bits | about ±9.2 × 10^18 | `0L` | `long id = 9_000_000_000L;` |
| `float` | 32 bits | about 7 decimal digits | `0.0f` | `float f = 1.5f;` |
| `double` | 64 bits | about 15 to 16 decimal digits | `0.0` | `double price = 9.99;` |
| `char` | 16 bits | one UTF-16 code unit, 0 to 65,535 | `'\u0000'` | `char c = 'A';` |
| `boolean` | not specified | `true` or `false` | `false` | `boolean ok = true;` |

Use `int` for whole numbers and `long` when values may exceed about two billion (database IDs, milliseconds). Use `double` for scientific values, but **never for money**: use `BigDecimal`, because binary floating point cannot represent most decimal fractions exactly (`0.1 + 0.2` is `0.30000000000000004`). Underscores in literals (`1_000_000`) are only for readability.

**Wrapper classes** turn each primitive into an object: `Byte`, `Short`, `Integer`, `Long`, `Float`, `Double`, `Character`, `Boolean`. You need them for collections (`List<Integer>`, never `List<int>`), for "no value" (a wrapper can be `null`, a primitive cannot), and for helper methods such as `Integer.parseInt("42")`.

**Autoboxing** is the automatic conversion from primitive to wrapper, and **unboxing** is the reverse. The compiler inserts calls to `Integer.valueOf(...)` and `intValue()` for you.

**Type casting** converts between numeric types. **Widening** (small to large, such as `int` to `long`) is automatic and safe. **Narrowing** (large to small) needs an explicit cast and may lose information.

```java
import java.util.ArrayList;
import java.util.List;

public class Primitives {
    public static void main(String[] args) {
        int small = 100;
        long big = small;                 // widening: automatic
        double d = 9.99;
        int truncated = (int) d;          // narrowing: explicit cast, drops the fraction
        int tooBig = 300;
        byte wrapped = (byte) tooBig;     // keeps only the low 8 bits

        List<Integer> scores = new ArrayList<>();
        scores.add(85);                   // autoboxing: Integer.valueOf(85)
        int first = scores.get(0);        // unboxing: intValue()

        System.out.println(big + " " + truncated + " " + wrapped + " " + first);
        System.out.println(Integer.parseInt("42") + 1);
        System.out.println(Integer.MAX_VALUE + 1);    // overflow wraps around silently
        System.out.println('A' + 1);                  // char is a number in arithmetic
        System.out.println((char) ('A' + 1));
        System.out.println(0.1 + 0.2);
    }
}
// Output:
// 100 9 44 85
// 43
// -2147483648
// 66
// B
// 0.30000000000000004
```

`(byte) 300` is `44` because 300 in binary is `1_0010_1100`; a byte keeps the low eight bits, `0010_1100`, which is 44.

**The Integer cache.** `Integer.valueOf` returns a **cached** object for values from -128 to 127, and a new object for anything else. Since autoboxing calls `valueOf`, comparing two wrappers with `==` (which compares references) gives different answers depending on the number:

```java
public class IntegerCache {
    public static void main(String[] args) {
        Integer a = 127, b = 127;
        Integer c = 128, d = 128;
        System.out.println(a == b);        // same cached object
        System.out.println(c == d);        // two different objects
        System.out.println(c.equals(d));   // compares values
        System.out.println(c == 128);      // unboxes c, then compares numbers
    }
}
// Output:
// true
// false
// true
// true
```

> **Pitfall:** Never compare wrapper objects with `==`. It works in tests with small numbers and fails in production when an order ID reaches 128. Use `equals`, or unbox to primitives first.

> **Pitfall:** Unboxing `null` throws `NullPointerException`. `Integer qty = map.get("missing"); int n = qty;` fails on the second line, and the stack trace points at a line with no visible method call.

::: deeper
**Where the values live.** A local `int` lives directly in the method's stack frame. An `Integer` is an object on the heap with a header (12 to 16 bytes on a 64-bit JVM) plus the 4-byte value, so it costs roughly four times the memory and an extra pointer hop. An `int[]` of a million elements uses about 4 MB; a `List<Integer>` of the same size uses several times that. This is why performance-sensitive code (and libraries such as `IntStream`) prefers primitives.

**The cache in the JDK source.** `Integer.valueOf(int i)` checks `if (i >= IntegerCache.low && i <= IntegerCache.high) return IntegerCache.cache[i + 128];` and otherwise calls `new Integer(i)`. The upper bound can be raised with the JVM option `-XX:AutoBoxCacheMax=1000`, but the lower bound is fixed at -128. `Short`, `Byte`, `Long` and `Character` (0 to 127) have similar caches; `Float` and `Double` have none. The `new Integer(...)` constructors have been deprecated for removal since Java 9 (and marked for removal in 16), so always use `valueOf` or autoboxing.

**Numeric promotion.** In arithmetic, `byte`, `short` and `char` are promoted to `int` first. So `byte a = 1, b = 2; byte c = a + b;` does not compile ("incompatible types: possible lossy conversion from int to byte"), but `a += b` compiles, because compound assignment includes a hidden cast. If either operand is `long`, `float` or `double`, the other is promoted to that type.

**Overflow is silent.** `int` arithmetic wraps around without any error. When overflow would be a bug (money in cents, counters), use `Math.addExact`, `Math.multiplyExact` and friends, which throw `ArithmeticException`, or switch to `long` or `BigInteger`.

**Floating point.** `double` follows IEEE 754. Division by zero gives `Infinity` and `0.0 / 0.0` gives `NaN`, while integer division by zero throws `ArithmeticException`. `NaN != NaN` is `true`; use `Double.isNaN(x)`.

> **Interview:** "What is the output of `Integer a = 128, b = 128; a == b`?" `false`. Autoboxing uses `Integer.valueOf`, which caches only -128 to 127, so 128 creates two distinct objects and `==` compares references. With 127 the answer is `true`. Always compare wrappers with `equals`.

> **Interview:** "Why use `int` instead of `Integer`?" Primitives are faster, use less memory and cannot be `null`. Use wrappers only when an object is required (collections, generics) or when "no value" must be represented.

**Version notes.** Project Valhalla is working on value classes, which would let types like `Integer` behave more like primitives (no identity, flat in memory). As of Java 25 this is still in preview or early-access stages, so for now the rules above hold.
:::

::: check
?? Why does `Integer x = 1000, y = 1000; System.out.println(x == y);` print `false`?
Autoboxing calls `Integer.valueOf`, which returns cached objects only for -128 to 127. 1000 produces two separate objects, and `==` on objects compares references, not values.

?? What is `(int) 3.99`, and what is `(byte) 200`?
`3`: casting a floating-point number to an integer type truncates towards zero. `(byte) 200` is `-56`, because only the low eight bits are kept and they are read as a signed value.

?? Why does `byte c = a + b;` fail to compile when `a` and `b` are bytes?
Arithmetic promotes both bytes to `int`, so the result is an `int`, and assigning an `int` to a `byte` is a narrowing conversion that needs an explicit cast.
:::

::: resources
- Docs: [Primitive data types (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) · sizes, defaults and literals
- Docs: [Autoboxing and unboxing](https://docs.oracle.com/javase/tutorial/java/data/autoboxing.html) · when the compiler converts for you
- Spec: [JLS chapter 5: Conversions and contexts](https://docs.oracle.com/javase/specs/jls/se21/html/jls-5.html) · the exact widening, narrowing and boxing rules
- Docs: [Integer (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Integer.html) · `valueOf`, `parseInt`, `MAX_VALUE` and more
- Spec: [JLS 5.1.7: Boxing conversion](https://docs.oracle.com/javase/specs/jls/se21/html/jls-5.html#jls-5.1.7) · the rule that requires the -128 to 127 cache
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 61: prefer primitive types to boxed primitives
:::

### Operators, control flow, loops, switch expressions

::: plain
Operators are the symbols that compute things: `+` and `*` for arithmetic, `==` and `<` for comparisons, `&&` and `||` for "and" and "or". Control flow is how a program makes decisions and repeats work: `if` chooses a path, loops repeat, and `switch` picks one branch out of many, a bit like SQL's `CASE WHEN`. Java's versions of these look like those in C or JavaScript, with a modern `switch` that returns a value and can check the type of an object.
:::

**Operators you use every day.**

| Kind | Operators | Notes |
|---|---|---|
| Arithmetic | `+ - * / %` | `int / int` is integer division: `7 / 2` is `3` |
| Increment | `++ --` | `i++` uses the old value, then adds; `++i` adds first |
| Compound assignment | `+= -= *= /= %=` | includes a hidden cast to the left-hand type |
| Comparison | `== != < <= > >=` | `==` on objects compares references |
| Logical | `&& \|\| !` | short-circuit: the right side may not run |
| Bitwise | `& \| ^ ~ << >> >>>` | `&` and `\|` on booleans do **not** short-circuit |
| Ternary | `cond ? a : b` | an expression that picks one of two values |
| Type test | `instanceof` | with a pattern variable since Java 16 |

**Precedence** follows school maths (`*` before `+`), then comparisons, then `&&`, then `||`, then the ternary, then assignment. When in doubt, add parentheses.

**Decisions and loops.** `if`/`else` works as you expect. Java has four loops: `while` (test first), `do-while` (test after, so the body runs at least once), the classic `for` with a counter, and the **enhanced for** (`for (String s : list)`) that walks any array or collection.

```java
public class Flow {
    public static void main(String[] args) {
        int[] amounts = {120, -5, 300, 0, 75};
        int total = 0;
        for (int amount : amounts) {
            if (amount < 0) {
                System.out.println("skipping bad amount " + amount);
                continue;                       // next iteration
            }
            if (amount == 0) break;             // leave the loop
            total += amount;
        }
        System.out.println("total = " + total);

        int i = 5;
        int a = i++;                            // a gets 5, then i becomes 6
        int b = ++i;                            // i becomes 7, then b gets 7
        System.out.println(a + " " + b + " " + i);

        System.out.println(7 / 2 + " " + 7 % 2 + " " + 7 / 2.0);
        String label = total > 400 ? "large" : "small";
        System.out.println(label);

        int attempts = 0;
        do {
            attempts++;
        } while (attempts < 3);
        System.out.println("attempts = " + attempts);
    }
}
// Output:
// skipping bad amount -5
// total = 420
// 5 7 7
// 3 1 3.5
// large
// attempts = 3
```

**Short-circuiting** matters for safety: in `name != null && name.length() > 3` the right side never runs when `name` is `null`, so no `NullPointerException`.

**Labelled break.** `break` leaves only the innermost loop. To leave an outer loop, label it:

```java
public class Labelled {
    public static void main(String[] args) {
        int[][] grid = {{1, 2}, {3, 42}, {5, 6}};
        search:
        for (int r = 0; r < grid.length; r++) {
            for (int c = 0; c < grid[r].length; c++) {
                if (grid[r][c] == 42) {
                    System.out.println("found at " + r + "," + c);
                    break search;              // leaves both loops
                }
            }
        }
    }
}
// Output:
// found at 1,1
```

**`switch`: old and new.** The classic `switch` **statement** with `case X:` labels "falls through" into the next case unless you write `break`, a famous source of bugs. Java 14 made the **arrow form** standard: `case X ->` never falls through, several labels can share a branch, and the whole `switch` can be an **expression** that produces a value. Use `yield` to return a value from a block. Java 21 added **pattern matching for switch**: cases can test the type of an object and bind it to a variable, with optional `when` guards.

```java
public class Switches {
    enum Status { NEW, PAID, SHIPPED, CANCELLED }

    static String describe(Object o) {
        return switch (o) {
            case null -> "nothing";
            case Integer n when n < 0 -> "negative int " + n;
            case Integer n -> "int " + n;
            case String s -> "text of length " + s.length();
            default -> "something else: " + o;
        };
    }

    public static void main(String[] args) {
        Status st = Status.PAID;
        String next = switch (st) {
            case NEW -> "take payment";
            case PAID, SHIPPED -> "track parcel";
            case CANCELLED -> {
                String msg = "refund";
                yield msg.toUpperCase();        // yield returns a value from a block
            }
        };                                      // no default needed: all enum values covered
        System.out.println(next);

        int day = 2;
        switch (day) {                          // old statement form, falls through
            case 1:
                System.out.println("one");
            case 2:
                System.out.println("two");
            case 3:
                System.out.println("three");    // runs too: no break above
                break;
            default:
                System.out.println("other");
        }

        Object value = "hello";
        if (value instanceof String s && s.length() > 3) {   // instanceof pattern (Java 16)
            System.out.println("long string " + s);
        }
        System.out.println(describe(-4) + "; " + describe(7) + "; " + describe("abc") + "; " + describe(null));
    }
}
// Output:
// track parcel
// two
// three
// long string hello
// negative int -4; int 7; text of length 3; nothing
```

A `switch` expression must be **exhaustive**: every possible input must match some case. For an enum that means listing all constants (or a `default`); for `Object` it means a `default`. The compiler checks this and refuses incomplete expressions with "the switch expression does not cover all possible input values".

> **Pitfall:** A classic `switch` on a `null` string or enum throws `NullPointerException`. Only a pattern-matching `switch` with an explicit `case null` accepts null.

> **Tip:** Prefer the arrow form in all new code. It removes fall-through bugs and, as an expression, lets you assign the result directly with `final` variables.

::: deeper
**How `switch` compiles.** For dense integer cases `javac` emits a `tableswitch` instruction (a jump table, constant time). For sparse values it emits a `lookupswitch` (a sorted table searched by binary search). A `switch` on a `String` compiles to a switch on `s.hashCode()` followed by `equals` checks to handle collisions, which is why the string must not be `null`. A pattern-matching `switch` compiles to an `invokedynamic` call to `SwitchBootstraps.typeSwitch`, which tests the cases in order.

**Order matters for patterns.** `case Integer n -> ...` placed before `case Integer n when n < 0 -> ...` makes the guarded case unreachable, and the compiler reports "this case label is dominated by a preceding case label".

**The `x = x++` puzzle.**

```java
public class PostIncrement {
    public static void main(String[] args) {
        int x = 5;
        x = x++;
        System.out.println(x);
    }
}
// Output:
// 5
```

`x++` saves the old value (5), increments `x` to 6, and the expression yields the saved 5, which is then assigned back to `x`. In bytecode: `iload_1`, `iinc 1, 1`, `istore_1`.

**Compound assignment hides a cast.** `short s = 10; s = s + 1;` does not compile, but `s += 1;` does, because the JLS defines `s += 1` as `s = (short) (s + 1)`. The same rule lets `int n = 0; n += 3.7;` compile silently and give `3`.

**Shifts.** `>>` keeps the sign bit (`-8 >> 1` is `-4`); `>>>` fills with zeros (`-8 >>> 1` is `2147483644`). `(low + high) >>> 1` is the overflow-safe midpoint used in binary search.

> **Interview:** "What is the difference between `&` and `&&`?" Both compute logical AND on booleans, but `&&` short-circuits (skips the right operand when the left is `false`) while `&` always evaluates both. `&` is also bitwise AND on integers.

> **Interview:** "What is new in `switch` since Java 14?" The arrow form without fall-through, multiple labels per case, `switch` as an expression with `yield`, exhaustiveness checking, and (Java 21) pattern matching with type patterns, record patterns, `when` guards and `case null`.

**Version notes.** Java 21 finalized pattern matching for `switch` (JEP 441) and record patterns (JEP 440). Java 22 added unnamed variables `_` (JEP 456), handy in patterns you do not use. Primitive types in patterns, `instanceof` and `switch` (such as `case int i when i > 5`) are a preview feature in Java 23 to 25 (JEP 507 in 25), so do not rely on them yet.
:::

::: check
?? What does this print: `int i = 3; int j = i++ + ++i;`?
`j` is `8`: `i++` yields 3 (then `i` is 4), `++i` makes `i` 5 and yields 5, and 3 + 5 = 8. `i` ends as 5.

?? Why might a classic `switch` print several case bodies?
Without `break`, execution falls through into the following cases. The arrow form (`case X ->`) never falls through.

?? When must a `switch` expression have a `default` branch?
When its cases do not already cover every possible value. For an enum, listing all constants is enough; for a type such as `Object` or `String`, a `default` (or an unconditional pattern) is required.
:::

::: resources
- Docs: [Operators (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/operators.html) · every operator with precedence table
- Docs: [Branching with switch statements](https://dev.java/learn/language-basics/switch-statement/) · dev.java on classic and arrow-form switch
- Spec: [JEP 441: Pattern matching for switch](https://openjdk.org/jeps/441) · the Java 21 feature, with many examples
- Docs: [Using pattern matching](https://dev.java/learn/using-pattern-matching/) · type patterns, record patterns and guards
- Article: [Java switch statement](https://www.baeldung.com/java-switch) · Baeldung from classic switch to patterns
- Spec: [JLS 15.26.2: Compound assignment operators](https://docs.oracle.com/javase/specs/jls/se21/html/jls-15.html#jls-15.26.2) · the hidden cast in `+=`
:::

### Arrays (1D/2D), Arrays utility class

::: plain
An array is a row of numbered boxes, all holding the same type, with a length fixed when the row is built. Box numbers (indexes) start at 0, so an array of 5 has boxes 0 to 4. Java arrays are simple and fast, which is why many interview coding problems use them, but they cannot grow. For a list that grows you will use `ArrayList` in the next section. The helper class `java.util.Arrays` adds sorting, searching, copying and printing.
:::

**Creating arrays.**

```java
int[] scores = new int[5];               // five ints, all 0
String[] names = {"Asha", "Ben", "Chen"}; // initializer: length 3
double[] prices = new double[]{9.99, 4.5}; // explicit form, needed when not declaring
```

New arrays are filled with defaults: `0` for numbers, `false` for booleans, `'\u0000'` for `char`, and `null` for object types. `array.length` (a field, no parentheses) gives the size. Reading or writing outside 0 to `length - 1` throws `ArrayIndexOutOfBoundsException`.

**Two-dimensional arrays** are arrays of arrays. Rows can even have different lengths (a **jagged** array).

```java
import java.util.Arrays;

public class ArraysDemo {
    public static void main(String[] args) {
        int[] nums = {42, 7, 19, 3, 25};
        System.out.println(nums);                    // not useful: type and hash code
        System.out.println(Arrays.toString(nums));

        Arrays.sort(nums);
        System.out.println(Arrays.toString(nums));
        System.out.println(Arrays.binarySearch(nums, 19));   // index in the sorted array

        int[] firstThree = Arrays.copyOf(nums, 3);
        int[] middle = Arrays.copyOfRange(nums, 1, 4);       // from 1 inclusive to 4 exclusive
        int[] filled = new int[4];
        Arrays.fill(filled, -1);
        System.out.println(Arrays.toString(firstThree) + " " + Arrays.toString(middle) + " " + Arrays.toString(filled));

        int[] copy = nums.clone();
        System.out.println((copy == nums) + " " + copy.equals(nums) + " " + Arrays.equals(copy, nums));

        int[][] grid = new int[2][3];
        grid[1][2] = 9;
        int[][] jagged = {{1}, {2, 3}, {4, 5, 6}};
        System.out.println(Arrays.deepToString(grid));
        System.out.println(jagged[2].length + " " + Arrays.deepToString(jagged));

        int sum = 0;
        for (int[] row : jagged) {
            for (int v : row) sum += v;
        }
        System.out.println("sum = " + sum + ", stream sum = " + Arrays.stream(nums).sum());
    }
}
// Output:
// [I@...
// [42, 7, 19, 3, 25]
// [3, 7, 19, 25, 42]
// 2
// [3, 7, 19] [7, 19, 25] [-1, -1, -1, -1]
// false false true
// [[0, 0, 0], [0, 0, 9]]
// 3 [[1], [2, 3], [4, 5, 6]]
// sum = 21, stream sum = 96
```

The first line prints something like `[I@1b6d3586`: `[I` means "array of int" and the rest is a hash code that differs on every run. That is why you always print arrays with `Arrays.toString` (or `Arrays.deepToString` for 2D).

**The `Arrays` class at a glance.**

| Method | What it does |
|---|---|
| `toString(a)`, `deepToString(a)` | readable text for 1D and nested arrays |
| `sort(a)`, `sort(a, from, to)`, `sort(a, comparator)` | sort in place (comparator only for object arrays) |
| `binarySearch(a, key)` | index of `key` in a **sorted** array, or a negative number |
| `copyOf(a, n)`, `copyOfRange(a, from, to)` | new array, padded with defaults if longer |
| `fill(a, v)` | set every element |
| `equals(a, b)`, `deepEquals(a, b)` | compare contents (not references) |
| `asList(a...)` | fixed-size `List` view backed by the array |
| `stream(a)` | a stream for sums, filters and maps |

> **Pitfall:** `a.equals(b)` on arrays compares references, exactly like `==`. Use `Arrays.equals(a, b)` to compare contents, and `Arrays.deepEquals` for 2D arrays.

> **Pitfall:** `Arrays.asList(arr)` returns a fixed-size list that writes through to the array: `set` works, but `add` and `remove` throw `UnsupportedOperationException`. Wrap it in `new ArrayList<>(...)` when you need to grow it. And `Arrays.asList(intArray)` with a primitive `int[]` gives a `List<int[]>` with one element, not a list of numbers.

::: deeper
**Arrays are objects.** Every array lives on the heap, has a header that stores its length, and inherits from `Object`. The variable `nums` holds a reference, so `int[] b = nums;` creates a second name for the same array, and a method that receives an array can change its contents. `clone()` makes a **shallow** copy: for a 2D array it copies the outer array of row references, so both copies still share the rows.

**Bounds checks are always on.** Every index access is checked, which is what makes Java memory safe. The JIT removes checks it can prove unnecessary (for example in a standard `for (int i = 0; i < a.length; i++)` loop), so the cost is usually tiny.

**Sorting algorithms.** `Arrays.sort` on primitive arrays uses a **dual-pivot quicksort** (fast, not stable, which does not matter for primitives). On object arrays it uses **TimSort**, a stable merge sort that is very fast on partly sorted data. Stable means equal elements keep their original order, important when you sort orders by date and then by customer. `Arrays.parallelSort` splits large arrays across cores.

**Array covariance.** Java treats `String[]` as a subtype of `Object[]`, so this compiles but fails at run time:

```java
public class Covariance {
    public static void main(String[] args) {
        Object[] objects = new String[2];
        objects[0] = "fine";
        objects[1] = 42;               // throws ArrayStoreException
    }
}
```

Running it throws `java.lang.ArrayStoreException: java.lang.Integer`. Generic collections (`List<String>`) are not covariant, which is one reason the next section prefers them.

**Binary search on unsorted data** returns garbage, not an error. When the key is absent, the result is `-(insertionPoint) - 1`, so a negative value also tells you where the key would go.

> **Interview:** "How do you find the second largest number in an array?" Track `max` and `second` in one pass: if `v > max`, set `second = max` and `max = v`; else if `v > second && v != max`, set `second = v`. That is O(n), better than sorting (O(n log n)).

> **Interview:** "Array vs `ArrayList`?" An array has a fixed length, can hold primitives, and uses `.length`. An `ArrayList` grows automatically, holds only objects (so boxing for numbers), has `size()`, and offers rich methods. Use arrays for fixed-size, performance-critical data; use lists everywhere else.
:::

::: check
?? What is printed by `System.out.println(new int[]{1, 2})`, and how do you print the contents?
Something like `[I@6d06d69c`: the type code and a hash code, because arrays do not override `toString`. Use `Arrays.toString(array)`.

?? You need to add elements to the list returned by `Arrays.asList("a", "b")`. What happens, and what is the fix?
`add` throws `UnsupportedOperationException` because the list is a fixed-size view over the array. Copy it: `new ArrayList<>(Arrays.asList("a", "b"))`.

?? Why does `Arrays.sort` use a different algorithm for `Integer[]` than for `int[]`?
Object arrays need a **stable** sort (equal elements keep their order), so they use TimSort. Primitive values with equal keys are indistinguishable, so stability does not matter and the faster dual-pivot quicksort is used.
:::

::: resources
- Docs: [Arrays (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/arrays.html) · declaring, creating and copying arrays
- Docs: [java.util.Arrays (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Arrays.html) · every helper method
- Spec: [JLS chapter 10: Arrays](https://docs.oracle.com/javase/specs/jls/se21/html/jls-10.html) · the formal rules, including covariance
- Article: [Arrays in Java: a reference guide](https://www.baeldung.com/java-arrays-guide) · Baeldung overview with examples
- Docs: [Creating and using arrays](https://dev.java/learn/language-basics/arrays/) · dev.java, a gentle walkthrough
:::

### Strings: immutability, string pool, StringBuilder vs StringBuffer, common methods, text blocks

::: plain
A `String` is a piece of text, and in Java it is **immutable**: once created it can never change. Methods that seem to change it, such as `toUpperCase()`, actually return a brand-new string and leave the original alone, like making a photocopy with corrections rather than writing on the original. Because strings never change, Java can safely share identical string literals from a common store called the **string pool**. When you need to build text piece by piece, you use a `StringBuilder`, which is a changeable scratchpad you turn into a `String` at the end.
:::

**Creating and comparing strings.** A literal such as `"java"` is placed in the string pool, and every identical literal in the program refers to that one object. `new String("java")` deliberately creates a separate object on the heap. Because `==` compares references, it can say two equal-looking strings are different; `equals` compares the characters.

[[fig:corebasics_pool]]

```java
public class StringPool {
    public static void main(String[] args) {
        String a = "java";
        String b = "java";                 // same pooled object as a
        String c = new String("java");     // a new object outside the pool
        String d = c.intern();             // the pooled version of c
        String e = "ja" + "va";            // constant expression: computed by javac, pooled
        String part = "ja";
        String f = part + "va";            // computed at run time: a new object

        System.out.println(a == b);
        System.out.println(a == c);
        System.out.println(a.equals(c));
        System.out.println(a == d);
        System.out.println(a == e);
        System.out.println(a == f);
        System.out.println(a.equals(f));
    }
}
// Output:
// true
// false
// true
// true
// true
// false
// true
```

**Immutability in practice.**

```java
public class Immutable {
    public static void main(String[] args) {
        String s = "order-42";
        s.toUpperCase();                    // result thrown away: s is unchanged
        System.out.println(s);
        s = s.toUpperCase();                // keep the new string by reassigning
        System.out.println(s);
    }
}
// Output:
// order-42
// ORDER-42
```

The second assignment did not change the old string either: it pointed the variable `s` at a new string. The old `"order-42"` object is untouched.

**Common methods.** These cover nearly all everyday work:

```java
public class StringMethods {
    public static void main(String[] args) {
        String line = "  ACC-1001, Asha Rao, 2500.75  ";
        String t = line.strip();                          // remove leading and trailing spaces
        String[] parts = t.split(",\\s*");                // split on comma plus optional spaces
        System.out.println(parts.length + " parts: " + parts[1]);
        System.out.println(t.length() + " " + t.charAt(0) + " " + t.indexOf("Asha"));
        System.out.println(t.substring(0, 8));            // from 0 inclusive to 8 exclusive
        System.out.println(t.startsWith("ACC") + " " + t.contains("Rao") + " " + t.endsWith("75"));
        System.out.println(t.replace("ACC", "ACCOUNT").toLowerCase());
        System.out.println("abc".equalsIgnoreCase("ABC") + " " + "apple".compareTo("banana"));
        System.out.println(String.join("|", "a", "b", "c") + " " + "=".repeat(5));
        System.out.println("   ".isBlank() + " " + "".isEmpty());
        double amount = Double.parseDouble(parts[2]);
        System.out.println(String.format("%s owes %.1f", parts[1], amount));
        System.out.println("%-6s|%5d|".formatted("id", 42));
        System.out.println(String.valueOf(3.5) + String.valueOf(true));
    }
}
// Output:
// 3 parts: Asha Rao
// 27 A 10
// ACC-1001
// true true true
// account-1001, asha rao, 2500.75
// true -1
// a|b|c =====
// true true
// Asha Rao owes 2500.8
// id    |   42|
// 3.5true
```

`compareTo` returns a negative number, zero or a positive number for "before", "equal" and "after" in dictionary order; sorting uses it. `split` takes a **regular expression**, so splitting on a dot needs `split("\\.")`.

**StringBuilder vs StringBuffer.** Both are mutable sequences of characters with `append`, `insert`, `delete`, `reverse` and `toString`. The difference is thread safety:

| | `String` | `StringBuilder` | `StringBuffer` |
|---|---|---|---|
| Mutable? | No | Yes | Yes |
| Thread-safe? | Yes (because immutable) | No | Yes (synchronized methods) |
| Speed | New object per change | Fastest | Slower (locking) |
| Use for | Values, keys, constants | Building text in a method | Legacy code; rarely needed |

```java
public class Builder {
    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= 5; i++) {
            sb.append(i);
            if (i < 5) sb.append(",");
        }
        System.out.println(sb);
        sb.insert(0, "[").append("]");      // methods return the same builder, so calls chain
        System.out.println(sb.toString());
        System.out.println(new StringBuilder("level").reverse().toString().equals("level"));
    }
}
// Output:
// 1,2,3,4,5
// [1,2,3,4,5]
// true
```

**Text blocks** (Java 15) are multi-line string literals between triple quotes. They are perfect for SQL, JSON and HTML in code. The common leading indentation is removed, measured from the least-indented line, including the closing `"""`.

```java
public class TextBlocks {
    public static void main(String[] args) {
        String sql = """
            SELECT id, amount
            FROM payments
            WHERE status = 'UNMATCHED'
            """;
        System.out.print(sql);
        String json = """
            {"id": %d, "name": "%s"}""".formatted(7, "Asha");
        System.out.println(json);
    }
}
// Output:
// SELECT id, amount
// FROM payments
// WHERE status = 'UNMATCHED'
// {"id": 7, "name": "Asha"}
```

> **Pitfall:** Building a string with `+=` inside a loop creates a new string on every iteration and copies all previous characters each time, which is O(n²) for n pieces. Use a `StringBuilder` in loops. A single expression like `"Hello " + name + "!"` is fine: the compiler optimizes it.

> **Pitfall:** Compare strings with `equals`, never `==`. And put the known literal first, `"PAID".equals(status)`, so a `null` status gives `false` instead of a `NullPointerException`.

::: deeper
**Why are strings immutable?** Interviewers love this question. Five reasons:

1. **The string pool** can share one object between all identical literals only because nobody can change it.
2. **Security:** file paths, URLs, class names and database credentials are passed as strings; a caller could otherwise change them after a security check.
3. **Hash keys:** `String` caches its `hashCode` in a field after the first call. Strings are the most common `HashMap` key, and a key that changed after insertion would be lost in the map.
4. **Thread safety:** immutable objects can be shared between threads with no locking.
5. **Class loading** uses strings for names; mutating them could load the wrong class.

**Inside a String.** Since Java 9 (**compact strings**, JEP 254), a `String` stores its characters in a `byte[]` plus a `coder` flag: Latin-1 text uses one byte per character, and only text containing other characters uses two bytes (UTF-16). This roughly halves memory for typical English text. `length()` counts UTF-16 code units, so an emoji counts as 2; use `codePointCount` to count real characters.

**Where is the pool?** Since Java 7 the string pool lives in the ordinary heap (it used to be in PermGen), so pooled strings can be garbage collected. It is a fixed-size hash table inside the JVM; you can tune its size with `-XX:StringTableSize`. `intern()` returns the pooled instance, adding the string if needed. Calling it manually is rarely worth it; G1's string deduplication (`-XX:+UseStringDeduplication`) can share the underlying arrays of equal strings automatically.

**How `+` compiles.** For compile-time constants (`"ja" + "va"`, or `final String` constants) `javac` joins them itself and the result is pooled, which is why `a == e` was `true` above. Other concatenations compile to `invokedynamic` with `StringConcatFactory` (Java 9+); before Java 9 they became `new StringBuilder().append(...).append(...).toString()`. Neither optimization works across loop iterations, hence the pitfall.

**How many objects does `new String("java")` create?** Up to two: the literal `"java"` in the pool (if not already there, created when the class is loaded and the literal is resolved) and the new heap object. That is a classic interview question.

> **Interview:** "`String` vs `StringBuilder` vs `StringBuffer`?" `String` is immutable and thread-safe. `StringBuilder` is mutable and not synchronized, so it is the fast choice for building text inside one method. `StringBuffer` is the older, synchronized version; it is rarely needed because builders are almost never shared between threads.

> **Interview:** "Why is `char[]` preferred over `String` for passwords?" A `String` is immutable and may stay in memory (and the pool) until garbage collected, visible in heap dumps. A `char[]` can be overwritten with zeros immediately after use. That is why `Console.readPassword` returns `char[]`.

**Version notes.** Java 11 added `strip`, `isBlank`, `lines` and `repeat`; Java 12 added `indent` and `transform`; Java 15 finalized text blocks and added `formatted`; Java 21 added `StringBuilder.repeat` and `String.splitWithDelimiters`. String templates were previewed in Java 21 and 22 and then **withdrawn**, so they are not part of Java 25; use `formatted` or `String.format`.
:::

::: check
?? `String s1 = "hi"; String s2 = new String("hi");` What are `s1 == s2`, `s1.equals(s2)` and `s1 == s2.intern()`?
`false` (different objects), `true` (same characters), `true` (`intern` returns the pooled object that `s1` already refers to).

?? Why is `result += word` inside a loop over 100,000 words slow, and what is the fix?
Each `+=` creates a new string and copies all characters so far, giving quadratic work. Use one `StringBuilder`, `append` each word, and call `toString()` once at the end (or `String.join` / `Collectors.joining`).

?? Name three reasons `String` is immutable.
Any three of: it allows the string pool, it keeps strings safe as `HashMap` keys (cached hash code), it is thread-safe without locking, and it prevents security-sensitive values such as paths or class names from being changed after they are checked.
:::

::: resources
- Docs: [Strings (Java tutorial)](https://dev.java/learn/strings/) · dev.java guide, from basics to text blocks
- Docs: [String (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/String.html) · every method with examples
- Docs: [Programmer's guide to text blocks](https://docs.oracle.com/en/java/javase/21/text-blocks/index.html) · indentation rules and escapes
- Article: [Guide to the Java string pool](https://www.baeldung.com/java-string-pool) · Baeldung on interning and memory
- Article: [StringBuilder vs StringBuffer in Java](https://www.baeldung.com/java-string-builder-string-buffer) · differences and a benchmark
- Spec: [JEP 254: Compact strings](https://openjdk.org/jeps/254) · why strings use a `byte[]` since Java 9
:::

### Packages, imports, classpath, the main method, var (local type inference)

::: plain
A big program has thousands of classes, so Java files them in **packages**, which work like folders with labels: `com.shopsphere.orders` holds the order classes, `com.shopsphere.payments` the payment classes. An **import** is a shortcut that lets you write `List` instead of the full name `java.util.List`. The **classpath** tells the JVM where to look for compiled classes, like a list of shelves to search. The **main method** is the front door where the program starts, and `var` lets you skip writing a variable's type when the right-hand side already makes it obvious.
:::

**Packages.** The first line of a file declares its package, and the folder structure must match it:

```text
src/
  com/shopsphere/model/Product.java     ->  package com.shopsphere.model;
  com/shopsphere/app/Main.java          ->  package com.shopsphere.app;
```

Package names are lowercase and usually start with a reversed domain name (`com.shopsphere`) to stay globally unique. The full name of a class includes its package: `com.shopsphere.model.Product`. Classes in the same package see each other without imports.

```java
// file src/com/shopsphere/model/Product.java
package com.shopsphere.model;

public class Product {
    private final String name;
    public Product(String name) { this.name = name; }
    public String name() { return name; }
}
```

```java
// file src/com/shopsphere/app/Main.java
package com.shopsphere.app;

import com.shopsphere.model.Product;      // single-type import
import java.util.*;                       // on-demand (wildcard) import of java.util
import static java.lang.Math.max;         // static import: use max(...) without Math.

public class Main {
    public static void main(String[] args) {
        List<Product> products = new ArrayList<>();
        products.add(new Product("Keyboard"));
        System.out.println(products.get(0).name() + " " + max(3, 7) + " args=" + args.length);
    }
}
```

**Compiling and running with a classpath.**

```bash
javac -d out src/com/shopsphere/model/Product.java src/com/shopsphere/app/Main.java
java -cp out com.shopsphere.app.Main          # prints: Keyboard 7 args=0
java -cp out com.shopsphere.app.Main a b      # prints: Keyboard 7 args=2
jar --create --file shop.jar -C out .         # package the classes into a JAR
java -cp shop.jar com.shopsphere.app.Main     # run from the JAR
```

`-d out` writes the `.class` files into `out/com/shopsphere/...`, mirroring the packages. `-cp` (classpath) lists folders and JAR files to search, separated by `:` on macOS and Linux and `;` on Windows. A **JAR** is a zip file of classes. In real projects Maven or Gradle builds the classpath for you (the third Core Java section covers them), but when you see `ClassNotFoundException` or `NoClassDefFoundError`, the classpath is the first suspect.

**Imports** are only a naming convenience; they load nothing and cost nothing at run time. `java.lang` (home of `String`, `Integer`, `System`, `Math`) is imported automatically. If two packages have a class with the same name, such as `java.util.Date` and `java.sql.Date`, import one and write the other's full name.

**The main method.** The JVM starts a program by calling:

```java
public static void main(String[] args)
```

- `public`: the JVM must be able to call it from outside the class.
- `static`: it is called before any object exists, so it belongs to the class itself.
- `void`: it returns nothing; use `System.exit(code)` to return an exit status.
- `String[] args`: the command-line arguments (`String... args` also works).

**`var`** (Java 10) lets the compiler infer the type of a **local variable** from its initializer. The variable is still statically typed: the type is fixed at compile time, you just do not write it.

```java
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VarDemo {
    public static void main(String[] args) {
        var count = 10;                                   // int
        var name = "Asha";                                // String
        var totals = new HashMap<String, List<Integer>>(); // HashMap<String, List<Integer>>
        totals.put(name, new ArrayList<>(List.of(count, 20)));
        for (var entry : totals.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        var copy = new ArrayList<>(totals.get(name));     // ArrayList<Integer>, inferred
        copy.add(30);
        System.out.println(copy + " " + ((Object) count).getClass().getSimpleName());
    }
}
// Output:
// Asha -> [10, 20]
// [10, 20, 30] Integer
```

Where `var` is allowed and where it is not:

| Allowed | Not allowed |
|---|---|
| Local variables with an initializer | Fields, method parameters, return types |
| Enhanced `for` and classic `for` variables | `var x;` (no initializer) |
| try-with-resources variables | `var x = null;` (no type to infer) |
| Lambda parameters (Java 11), for annotations | Array initializers: `var a = {1, 2};` |

> **Pitfall:** `var list = new ArrayList<>();` compiles, but the diamond has nothing to infer from, so the type is `ArrayList<Object>`. With `var`, always write the type arguments on the right.

> **Tip:** Use `var` when the type is obvious from the right-hand side (`var orders = new ArrayList<Order>()`), and write the type when it is not (`Order o = service.find(id)` reads better than `var o = service.find(id)`).

::: deeper
**`ClassNotFoundException` vs `NoClassDefFoundError`.** The first is a checked exception thrown when code asks for a class by name (`Class.forName("...")`, JDBC drivers, reflection) and it is not on the classpath. The second is an error thrown when a class that **was** present at compile time is missing at run time (wrong JAR versions), or when its static initializer failed earlier. Both are classic production-support issues, so you have probably seen them in logs already.

**The main method in more detail.** You can overload `main` (write other methods named `main` with different parameters), but the JVM only calls the `String[]` version. Without `static`, Java 21 reports "Main method is not static in class Main" unless preview features are on. Since a class can have only one `main` with that signature, a JAR's manifest names the entry class (`Main-Class:`), which is what `java -jar app.jar` uses.

**Java 25: simpler programs.** JEP 512 (final in 25) allows **instance main methods** (`void main()` without `static`, `public` or parameters) and **compact source files** that contain only methods and fields, with no class declaration. The new class `java.lang.IO` offers `IO.println` and `IO.readln`. In Java 21 these existed only as a preview (JEP 445), so do not use them in Java 21 code.

**Modules.** Java 9 added the module system (JPMS): a `module-info.java` file names a module, lists which packages it `exports` and which modules it `requires`. The JDK itself is modular, which is what lets `jlink` build small runtimes. Most application code (including typical Spring Boot apps) still runs on the classpath without modules, so you only need to recognize the file.

**`var` is not dynamic typing.** `var x = 10; x = "text";` fails to compile with "incompatible types: String cannot be converted to int". `var` is not a keyword but a **reserved type name**, so old code that used `var` as a variable name still compiles; you just cannot name a class `var`.

> **Interview:** "Why is `main` static?" The JVM calls it before any object of the class exists, so it must not need an instance. Being static also means the JVM does not have to choose which constructor to call.

> **Interview:** "Does `var` make Java dynamically typed?" No. The compiler infers the type once from the initializer, and the variable keeps that static type. It only removes repetition in the source.
:::

::: check
?? A class is declared `package com.shop.app;` and is called `Main`. What command runs it from the compiled `out` folder?
`java -cp out com.shop.app.Main`: the classpath points at the root folder, and the fully qualified class name includes the package.

?? Do imports make a program slower or load classes?
No. Imports only let you use short names in source code; the compiled bytecode always uses fully qualified names, and classes load lazily when first used.

?? What type does `var items = new ArrayList<>();` have?
`ArrayList<Object>`, because the diamond has no target type to infer from. Write `new ArrayList<String>()` when using `var`.
:::

::: resources
- Docs: [Packages (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/package/index.html) · creating and using packages
- Docs: [Local variable type inference style guidelines](https://openjdk.org/projects/amber/guides/lvti-style-guide) · when `var` helps and when it hurts
- Docs: [The javac command](https://docs.oracle.com/en/java/javase/21/docs/specs/man/javac.html) · `-d`, `--class-path` and source layout
- Article: [ClassNotFoundException vs NoClassDefFoundError](https://www.baeldung.com/java-classnotfoundexception-and-noclassdeffounderror) · Baeldung on the two classpath failures
- Spec: [JEP 286: Local-variable type inference](https://openjdk.org/jeps/286) · the design and limits of `var`
- Spec: [JLS 12.1.4: Invoke Test.main](https://docs.oracle.com/javase/specs/jls/se21/html/jls-12.html#jls-12.1.4) · how the JVM starts a program at `main`
:::

## Object-Oriented Programming

Everything so far could live inside one `main` method. Real programs, and every Spring Boot service you will write, are built from **classes**: templates that bundle data with the code that works on it. Object-oriented programming (OOP) is the set of ideas for designing those classes well: hiding internal details, reusing behaviour, and writing code that works with many types through a common interface.

If you come from SQL, a helpful bridge: a class is a bit like a table definition (the columns are its **fields**), and an object is like one row. The difference is that a class also carries **methods**, behaviour that belongs to the data, and that objects refer to each other through references rather than foreign keys. The topics in this group build from single classes to the four pillars, interfaces, the methods every object inherits from `Object`, and the modern data types: enums, records and sealed classes.

[[fig:corebasics_heap]]

### Classes, objects, constructors, constructor chaining, this & super

::: plain
A **class** is a blueprint, like the architect's drawing of a house; an **object** is a house built from it. One drawing can produce many houses, each with its own paint colour and furniture (its own field values). A **constructor** is the building crew that sets up a new house the moment it is created. `this` means "this particular house", and `super` means "the part of me I inherited from my parent class". When one constructor calls another to avoid repeating setup work, that is **constructor chaining**.
:::

**A first class.**

```java
public class Bank {
    public static void main(String[] args) {
        Account a = new Account("ACC-1", "Asha", 500);
        Account b = new Account("ACC-2", "Ben");      // uses the shorter constructor
        a.deposit(250);
        b.deposit(100);
        System.out.println(a.describe());
        System.out.println(b.describe());
        System.out.println(Account.count() + " accounts created");
    }
}

class Account {
    private static int created = 0;         // one value shared by the whole class
    private final String id;                // fields: each object has its own copy
    private final String owner;
    private long balance;

    Account(String id, String owner, long openingBalance) {
        this.id = id;                       // this.id is the field, id is the parameter
        this.owner = owner;
        this.balance = openingBalance;
        created++;
    }

    Account(String id, String owner) {
        this(id, owner, 0);                 // constructor chaining: must be the first statement
    }

    void deposit(long amount) {
        if (amount <= 0) throw new IllegalArgumentException("amount must be positive");
        balance += amount;
    }

    String describe() {
        return id + " (" + owner + "): " + balance;
    }

    static int count() { return created; }
}
// Output:
// ACC-1 (Asha): 750
// ACC-2 (Ben): 100
// 2 accounts created
```

The key pieces:

- **Fields** hold an object's state; **methods** define its behaviour. `new Account(...)` allocates the object on the heap, runs a constructor, and returns a reference to it.
- A **constructor** has the class's name and no return type. If you write no constructor, the compiler adds a **default constructor** with no parameters. As soon as you write any constructor, that default disappears.
- `this` refers to the current object. Use `this.field` when a parameter has the same name, and `this(...)` to call another constructor of the same class.
- `super(...)` calls a constructor of the parent class, and `super.method()` calls the parent's version of a method.

**Chaining through inheritance.** Every constructor starts by calling a parent constructor. If you do not write `super(...)` or `this(...)` as the first line, the compiler inserts `super()`. So parent parts are always built before child parts:

```java
public class Chain {
    public static void main(String[] args) {
        new SavingsAccount("S-1", 2.5);
    }
}

class BaseAccount {
    protected final String id;
    BaseAccount(String id) {
        this.id = id;
        System.out.println("BaseAccount constructor: " + id);
    }
}

class SavingsAccount extends BaseAccount {
    private final double rate;
    SavingsAccount(String id, double rate) {
        super(id);                          // must come first
        this.rate = rate;
        System.out.println("SavingsAccount constructor: rate " + rate);
    }
}
// Output:
// BaseAccount constructor: S-1
// SavingsAccount constructor: rate 2.5
```

> **Pitfall:** If the parent class has only constructors with parameters, a child constructor that does not call `super(...)` explicitly fails to compile with "constructor BaseAccount in class BaseAccount cannot be applied to given types", because the implicit `super()` has nothing to call.

**Java is pass-by-value.** When you pass an argument, the method receives a **copy of the value in the variable**. For a primitive that is the number itself. For an object, the variable holds a reference, so the method gets a copy of the reference: it can change the object the reference points to, but reassigning its parameter does not affect the caller's variable.

```java
public class PassByValue {
    static void addBonus(Wallet w) { w.amount += 100; }     // changes the shared object
    static void replace(Wallet w) { w = new Wallet(); w.amount = 999; }  // changes only the local copy
    static void bump(int n) { n++; }                         // changes only the local copy

    public static void main(String[] args) {
        Wallet mine = new Wallet();
        mine.amount = 50;
        addBonus(mine);
        replace(mine);
        int x = 1;
        bump(x);
        System.out.println(mine.amount + " " + x);
    }
}

class Wallet { int amount; }
// Output:
// 150 1
```

> **Interview:** "Is Java pass-by-value or pass-by-reference?" Always pass-by-value. For object arguments the value copied is the reference, so a method can mutate the object the caller sees, but it cannot make the caller's variable point to a different object. The `swap(a, b)` method that works in C++ is impossible in Java.

::: deeper
**What `new` does.** `new Account(...)` compiles to the bytecode `new` (allocate memory and set every field to its default: `0`, `false`, `null`), `dup`, then `invokespecial Account.<init>` (the constructor, which bytecode calls `<init>`). Allocation is very cheap in modern JVMs: each thread has a **TLAB** (thread-local allocation buffer), so allocating is mostly bumping a pointer. Short-lived objects die young and are cleaned up cheaply by the garbage collector.

**Object layout.** On a 64-bit HotSpot JVM each object has a header (a mark word for locking and hashing plus a class pointer, 12 bytes with compressed class pointers) followed by its fields, padded to a multiple of 8 bytes. Java 25 made **compact object headers** a product feature (JEP 519, enabled with `-XX:+UseCompactObjectHeaders`), shrinking the header to 8 bytes.

**Initialization order inside one object**: fields get defaults, the parent constructor chain runs, then field initializers and instance initializer blocks run in source order, then the rest of the constructor body. The static topic below shows the full order including static blocks.

**`this` escaping.** Do not pass `this` to other code (register a listener, start a thread) inside the constructor: other code may see a half-built object. For the same reason, do not call overridable methods from a constructor (the overriding topic shows what goes wrong).

**Java 25 flexible constructor bodies.** JEP 513 (final in 25) allows statements **before** `super(...)` or `this(...)`, such as validating arguments, as long as they do not use the object being built. In Java 21, `super(...)` or `this(...)` must be the very first statement.

> **Interview:** "Can a constructor be `private`?" Yes. It stops other classes from creating instances, which is used for singletons, utility classes (only static methods), and static factory methods such as `List.of(...)`.

> **Interview:** "What is the difference between `this()` and `super()`?" `this(...)` calls another constructor of the same class; `super(...)` calls a constructor of the parent class. Either must be the first statement, so a constructor can contain only one of them.
:::

::: check
?? You write `class Point { Point(int x, int y) { } }`. Does `new Point()` compile?
No. Writing any constructor removes the compiler's default no-argument constructor. Add `Point() { this(0, 0); }` if you need one.

?? In what order do the constructors run for `new C()` when `C extends B` and `B extends A`?
`A`'s constructor body finishes first, then `B`'s, then `C`'s, because each constructor begins by calling its parent's constructor.

?? A method does `list = new ArrayList<>();` on its parameter. Does the caller's variable change?
No. The parameter holds a copy of the reference; reassigning it only changes the local copy. Calling `list.clear()` would change the caller's list, because both references point to the same object.
:::

::: resources
- Docs: [Classes and objects (Java tutorial)](https://dev.java/learn/classes-objects/) · dev.java on declaring classes, constructors and `this`
- Docs: [Providing constructors for your classes](https://docs.oracle.com/javase/tutorial/java/javaOO/constructors.html) · default and overloaded constructors
- Article: [Is Java pass-by-reference or pass-by-value?](https://www.baeldung.com/java-pass-by-value-or-pass-by-reference) · Baeldung with memory diagrams
- Spec: [JLS 8.8: Constructor declarations](https://docs.oracle.com/javase/specs/jls/se21/html/jls-8.html#jls-8.8) · the exact chaining rules
- Spec: [JEP 513: Flexible constructor bodies](https://openjdk.org/jeps/513) · statements before `super(...)` in Java 25
- Book: *Head First Java* (3rd edition) by Kathy Sierra, Bert Bates and Trisha Gee · the friendliest introduction to objects and memory
:::

### Encapsulation, inheritance, polymorphism, abstraction (the 4 pillars)

::: plain
The four pillars are four habits for building software that is easy to change. **Encapsulation** is a vending machine: you press buttons, but you cannot reach in and rearrange the coins. **Inheritance** is a family resemblance: a savings account *is an* account, so it gets everything an account has and adds a little more. **Polymorphism** means "many shapes": you tell any payment to `pay()`, and a card payment and a UPI payment each do it in their own way. **Abstraction** is a car's steering wheel: you use a simple control without knowing how the engine works.
:::

The pillars make the most sense together, so here is one realistic example from an online shop, ShopSphere's payment step, followed by each pillar in turn.

```java
import java.util.List;

public class Checkout {
    public static void main(String[] args) {
        List<Payment> payments = List.of(
            new CardPayment(1200, "4111111111111111"),
            new UpiPayment(450, "asha@bank"),
            new CardPayment(80, "5500005555555559"));
        long total = 0;
        for (Payment p : payments) {
            System.out.println(p.process());   // polymorphism: each type does its own thing
            total += p.amount();
        }
        System.out.println("total charged: " + total);
    }
}

abstract class Payment {                        // abstraction: the idea of "a payment"
    private final long amount;                   // encapsulation: private state

    protected Payment(long amount) {
        if (amount <= 0) throw new IllegalArgumentException("amount must be positive");
        this.amount = amount;
    }

    public long amount() { return amount; }      // read-only access

    public final String process() {              // template: same steps for every payment
        return "[" + method() + "] " + authorize() + ", amount " + amount;
    }

    protected abstract String method();          // each subclass must fill these in
    protected abstract String authorize();
}

class CardPayment extends Payment {             // inheritance: a CardPayment is a Payment
    private final String cardNumber;
    CardPayment(long amount, String cardNumber) {
        super(amount);
        this.cardNumber = cardNumber;
    }
    @Override protected String method() { return "CARD"; }
    @Override protected String authorize() {
        return "card ending " + cardNumber.substring(cardNumber.length() - 4) + " approved";
    }
}

class UpiPayment extends Payment {
    private final String vpa;
    UpiPayment(long amount, String vpa) {
        super(amount);
        this.vpa = vpa;
    }
    @Override protected String method() { return "UPI"; }
    @Override protected String authorize() { return "collect request sent to " + vpa; }
}
// Output:
// [CARD] card ending 1111 approved, amount 1200
// [UPI] collect request sent to asha@bank, amount 450
// [CARD] card ending 5559 approved, amount 80
// total charged: 1730
```

**Encapsulation** means keeping an object's data `private` and exposing only methods that keep it valid. Nobody can set `amount` to `-5`, because the field is private, it is checked once in the constructor, and there is no setter. In a bank account, `withdraw` would check the balance before changing it. The benefit is that the class guarantees its own rules (its **invariants**), and you can change the internal representation later (cents in a `long` today, `BigDecimal` tomorrow) without breaking callers.

**Inheritance** (`extends`) lets a class reuse and specialize another. `CardPayment` inherits `amount()` and `process()` and adds its own field. Java allows only **single inheritance of classes** (one parent), but a class can implement many interfaces. Use inheritance only for a true "is-a" relationship; the composition topic later shows why.

**Polymorphism** means one piece of code works with many types. The loop only knows it has `Payment` objects; at run time each call to `p.process()` reaches the right `method()` and `authorize()` for the actual object. Adding a `WalletPayment` later requires **no change** to the checkout loop. There are two kinds:

| Kind | Also called | Decided at | Example |
|---|---|---|---|
| Compile-time | static polymorphism, overloading | compile time, from declared types | `print(int)` vs `print(String)` |
| Run-time | dynamic polymorphism, overriding | run time, from the actual object | `p.authorize()` on a `Payment` variable |

**Abstraction** means showing what something does and hiding how. `Payment` is `abstract`: you cannot create a plain `Payment`, and it declares `abstract` methods that subclasses must implement. Callers depend on the abstract idea, not on card or UPI details. Interfaces are the other main tool for abstraction (next topics).

> **Pitfall:** Public fields and a setter for every field ("getters and setters everywhere") is not encapsulation. If anyone can call `setBalance(-1000)`, the class protects nothing. Expose operations that make sense in the domain (`deposit`, `withdraw`) instead.

> **Tip:** In interviews, always answer the pillars with an example. "Encapsulation is data hiding" sounds memorized; "my `Account` class keeps `balance` private and only changes it through `deposit` and `withdraw`, which validate the amount" shows you can use it.

::: deeper
**The template method pattern.** `process()` above is `final` and defines the fixed steps, while subclasses supply the varying parts (`method()`, `authorize()`). This is the **template method** design pattern, and frameworks use it heavily: Spring's `JdbcTemplate`, `OncePerRequestFilter` and many abstract base classes work this way.

**Polymorphism through the Liskov substitution principle.** Code written against `Payment` must keep working with any subclass. A subclass that throws `UnsupportedOperationException` from an inherited method, or that tightens the parent's rules (for example a `Square extends Rectangle` whose `setWidth` also changes the height), breaks callers that rely on the parent's behaviour. That is the "L" in SOLID, covered in the Software Engineering section.

**Encapsulation leaks.** Returning a reference to an internal mutable object breaks encapsulation even if the field is private:

```java
import java.util.ArrayList;
import java.util.List;

public class Leak {
    public static void main(String[] args) {
        Cart cart = new Cart();
        cart.add("pen");
        cart.items().add("free laptop");         // the caller edits private state
        System.out.println(cart.items());
        System.out.println(cart.safeItems());
    }
}

class Cart {
    private final List<String> items = new ArrayList<>();
    void add(String item) { items.add(item); }
    List<String> items() { return items; }                  // leaks the internal list
    List<String> safeItems() { return List.copyOf(items); } // read-only copy
}
// Output:
// [pen, free laptop]
// [pen, free laptop]
```

`safeItems()` returns an unmodifiable copy (or use `Collections.unmodifiableList(items)` for a read-only view). The immutability topic returns to defensive copies.

> **Interview:** "What is the difference between abstraction and encapsulation?" Abstraction is about design: exposing only the essential operations (an interface or abstract class says *what* can be done). Encapsulation is about implementation: hiding the data and internal details behind access control so the object stays valid (*how* it is protected). They work together: `Payment` abstracts the idea of paying, and its private `amount` is encapsulated.

> **Interview:** "Why does Java not support multiple inheritance of classes?" To avoid the **diamond problem**: if two parents define the same field or method, which one does the child get? Java allows multiple inheritance of *type* through interfaces, and since Java 8 of *behaviour* through default methods, with a rule that forces the class to resolve conflicts explicitly.
:::

::: check
?? Name the four pillars and give one sentence of example for each from a banking app.
Encapsulation: `balance` is private and only `deposit`/`withdraw` change it after validation. Inheritance: `SavingsAccount extends Account` and adds an interest rate. Polymorphism: a list of `Account` objects where `monthlyCharge()` behaves differently per account type. Abstraction: an abstract `Account` (or an interface) that callers use without knowing the concrete type.

?? Why can the checkout loop handle a new `WalletPayment` class without being changed?
It only calls methods declared on `Payment`. Dynamic dispatch picks the `WalletPayment` implementation at run time, so new subclasses plug in without edits to calling code (the open/closed principle).

?? Is a class with private fields and public getters and setters for all of them well encapsulated?
Usually not. Anyone can still put the object into an invalid state through the setters. Good encapsulation exposes meaningful operations that enforce the rules.
:::

::: resources
- Docs: [Object-oriented programming concepts](https://docs.oracle.com/javase/tutorial/java/concepts/index.html) · the official tutorial on objects, classes, inheritance and interfaces
- Docs: [Inheritance (Java tutorial)](https://dev.java/learn/inheritance/) · dev.java on extending classes and polymorphism
- Article: [Object-oriented programming concepts in Java](https://www.baeldung.com/java-oop) · Baeldung tour of the pillars with code
- Article: [Polymorphism in Java](https://www.baeldung.com/java-polymorphism) · static vs dynamic polymorphism
- Book: *Head First Design Patterns* (2nd edition) by Eric Freeman and Elisabeth Robson · template method, strategy and other OOP patterns
:::

### Method overloading vs overriding, dynamic dispatch, covariant return types

::: plain
**Overloading** is one name with several parameter lists in the same class, like a coffee machine with one "brew" button that accepts a cup, a mug or a flask: the compiler picks the right version by looking at what you pass. **Overriding** is a child class replacing a method it inherited, like a franchise restaurant cooking the chain's signature dish to its own local recipe. The JVM decides which overridden version runs by looking at the actual object at run time; that decision is called **dynamic dispatch**.
:::

**Overloading** happens within a class (or between a class and its parent): same method name, different parameter types or count. The return type alone cannot distinguish overloads. The **compiler** picks the overload using the **declared** (compile-time) types of the arguments.

**Overriding** happens in a subclass: same name, same parameter types, and a compatible return type. The **JVM** picks the implementation at run time using the **actual** class of the object. Always mark overrides with `@Override`, so the compiler tells you when you accidentally overloaded instead (for example by misspelling the name or changing a parameter type).

| | Overloading | Overriding |
|---|---|---|
| Where | Same class (or inherited) | Subclass |
| Parameters | Must differ | Must be the same |
| Return type | Can differ freely | Same, or a subtype (covariant) |
| Access | Any | Same or wider (not narrower) |
| Checked exceptions | Any | Same, narrower or none (not broader) |
| Chosen by | Compiler, from declared types | JVM at run time, from the actual object |
| Applies to static methods? | Yes | No: static methods are hidden, not overridden |

```java
public class Dispatch {
    static String show(Object o) { return "Object"; }
    static String show(String s) { return "String"; }
    static String show(long n) { return "long"; }
    static String show(Integer n) { return "Integer"; }
    static String show(int... ns) { return "varargs"; }

    public static void main(String[] args) {
        Object text = "hi";
        System.out.println(show("hi") + " " + show(text));   // declared type decides
        System.out.println(show(5));                         // widening int -> long wins over boxing
        System.out.println(show(Integer.valueOf(5)) + " " + show());

        Animal a = new Dog();                                // declared Animal, actual Dog
        System.out.println(a.sound());                       // overriding: actual type decides
        System.out.println(a.name);                          // fields are NOT polymorphic
        System.out.println(a.kind());                        // static methods are hidden, not overridden
        Animal copy = a.copy();
        System.out.println(copy.getClass().getSimpleName());
        Dog d = new Dog().copy();                            // covariant return: no cast needed
        System.out.println(d.fetch());
    }
}

class Animal {
    String name = "animal";
    String sound() { return "..."; }
    static String kind() { return "Animal.kind"; }
    Animal copy() { return new Animal(); }
}

class Dog extends Animal {
    String name = "dog";                                     // hides Animal.name
    @Override String sound() { return "Woof"; }
    static String kind() { return "Dog.kind"; }              // hides, does not override
    @Override Dog copy() { return new Dog(); }               // covariant return type
    String fetch() { return "fetching"; }
}
// Output:
// String Object
// long
// Integer varargs
// Woof
// animal
// Animal.kind
// Dog
// fetching
```

Read the output carefully, because each line is a common interview question:

- `show(text)` prints `Object`: the variable is declared `Object`, and overloads are chosen at compile time.
- `show(5)` prints `long`: overload resolution tries, in order, (1) exact match or **widening** (`int` to `long`), (2) **boxing** (`int` to `Integer`), (3) **varargs**. A match in an earlier phase wins.
- `a.sound()` prints `Woof`: the method is overridden, so the actual object decides.
- `a.name` prints `animal`: fields are resolved by declared type. A subclass field with the same name **hides** the parent's field rather than overriding it. Never do this on purpose.
- `a.kind()` prints `Animal.kind`: static methods belong to classes and are resolved by declared type (call them as `Animal.kind()` to avoid confusion).

**Covariant return types** (Java 5) let an overriding method return a subtype of the original return type. `Dog.copy()` returns `Dog`, so callers holding a `Dog` need no cast, while callers holding an `Animal` still get an `Animal`.

[[fig:corebasics_dispatch]]

> **Pitfall:** Overloading on related types plus `null` causes ambiguity: with `show(String)` and `show(Integer)` both present, `show(null)` fails to compile with "reference to show is ambiguous". Cast the `null` or rename the methods.

> **Pitfall:** `equals(Account other)` in your class does **not** override `Object.equals(Object)`; it overloads it. Collections call the `Object` version and ignore yours. `@Override` would have caught this at compile time.

::: deeper
**How dynamic dispatch works in the JVM.** The compiler writes `invokevirtual Animal.sound` into the bytecode, recording only the declared type:

```text
 8: aload_1
 9: invokevirtual #10    // Method Animal.sound:()V
12: invokestatic  #15    // Method Animal.describe:()V
```

At run time the JVM looks at the actual class of the object on the stack and finds the method in that class's **virtual method table** (vtable), an array of method pointers built when the class is loaded. A subclass's vtable copies its parent's entries and replaces the slots it overrides, so dispatch is a fast indexed lookup. Interface calls use `invokeinterface` and an itable. `invokestatic` (static methods) and `invokespecial` (constructors, `private` methods and `super.method()` calls) are bound without a vtable lookup.

**The JIT removes most of the cost.** If profiling shows a call site always sees `Dog` (a **monomorphic** call site), the JIT inlines `Dog.sound()` directly behind a cheap type check. Two types (**bimorphic**) are also inlined; many types (**megamorphic**) fall back to the vtable. That is why polymorphic Java code is usually as fast as direct calls. Making methods `final` does not speed things up much today; the JIT knows which classes are loaded.

**Calling an overridable method from a constructor:**

```java
public class CtorTrap {
    public static void main(String[] args) {
        new Child();
    }
}

class Parent {
    Parent() { init(); }                     // calls the overridden version
    void init() { System.out.println("Parent.init"); }
}

class Child extends Parent {
    private String greeting = "hello";
    @Override void init() { System.out.println("Child.init sees " + greeting); }
}
// Output:
// Child.init sees null
```

The parent constructor runs before `Child`'s field initializers, so `greeting` is still `null` when the overridden `init` runs. Effective Java's rule: constructors must not invoke overridable methods.

**Rules the compiler enforces on overrides.** You cannot override a `final` method, cannot reduce visibility (overriding a `public` method with a `protected` one fails with "attempting to assign weaker access privileges"), and cannot declare broader checked exceptions. `private` methods are not inherited, so a method with the same name in a subclass is a brand-new method.

> **Interview:** "Can you override a static method?" No. A static method with the same signature in a subclass **hides** the parent's method; which one runs depends on the declared type, not the object. `@Override` on it gives a compile error.

> **Interview:** "Can you overload `main`, or override a private method?" You can overload `main` (the JVM only calls the `String[]` version). You cannot override a private method, because it is not visible to subclasses; a same-named method in the subclass is unrelated.
:::

::: check
?? With overloads `f(long)` and `f(Integer)`, which runs for `f(10)`, and why?
`f(long)`. Overload resolution tries widening conversions before boxing, and `int` to `long` is a widening conversion.

?? `Animal a = new Dog(); a.sound();` where `Dog` overrides `sound`. Which version runs, and who decides?
`Dog.sound()`. The compiler only checks that `Animal` has `sound()`; the JVM chooses the implementation at run time from the object's actual class (dynamic dispatch).

?? What is a covariant return type?
An override that returns a subtype of the parent method's return type, such as `Dog copy()` overriding `Animal copy()`.
:::

::: resources
- Docs: [Overriding and hiding methods](https://docs.oracle.com/javase/tutorial/java/IandI/override.html) · the official rules, including static method hiding
- Spec: [JLS 15.12.2: Compile-time step 2, determine method signature](https://docs.oracle.com/javase/specs/jls/se21/html/jls-15.html#jls-15.12.2) · the three overload resolution phases
- Article: [Method overloading and overriding in Java](https://www.baeldung.com/java-method-overload-override) · Baeldung with examples
- Article: [Covariant return type in Java](https://www.baeldung.com/java-covariant-return-type) · how and when to use it
- Spec: [JVM specification: invokevirtual](https://docs.oracle.com/javase/specs/jvms/se21/html/jvms-6.html#jvms-6.5.invokevirtual) · how the JVM selects the method at run time
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 52: use overloading judiciously; Item 19: constructors must not invoke overridable methods
:::

### Abstract classes vs interfaces (default, static, private methods)

::: plain
An **interface** is a contract, like a job description: "anyone in this role can `charge()` and `refund()`". It says what must be done, not who you are, and one class can sign many contracts. An **abstract class** is a partly built product, like a house with the foundation and walls done but the interior left for each buyer: it can hold shared fields and working code, but you cannot move in (create an instance) until a subclass finishes it. A class can extend only one abstract class but implement many interfaces.
:::

**Interfaces** declare methods that implementing classes must provide. Since Java 8 they can also contain **default methods** (with a body, inherited by implementers), and **static methods** (helpers called on the interface itself); since Java 9 they can have **private methods** (shared helpers for the default methods). All interface fields are implicitly `public static final` constants. Interfaces cannot hold instance state.

**Abstract classes** are classes marked `abstract`. They can have constructors, instance fields of any visibility, concrete methods and `abstract` methods (no body) that subclasses must implement. You cannot instantiate them directly.

```java
import java.util.List;

public class Contracts {
    public static void main(String[] args) {
        List<PaymentGateway> gateways = List.of(new StripeGateway(), new RazorpayGateway());
        for (PaymentGateway g : gateways) {
            System.out.println(g.charge(1000));
            System.out.println(g.chargeWithFee(1000));        // default method
        }
        System.out.println(PaymentGateway.toRupees(12345));   // static interface method
        System.out.println(new RazorpayGateway().audit());    // concrete method from the abstract class
    }
}

interface PaymentGateway {
    int MAX_AMOUNT = 1_000_000;                    // implicitly public static final

    String charge(long paise);                     // abstract: implementers must write it

    default String chargeWithFee(long paise) {     // default: inherited, can be overridden
        return charge(paise + fee(paise));
    }

    private long fee(long paise) {                 // private helper (Java 9+)
        return Math.max(200, paise / 50);
    }

    static String toRupees(long paise) {           // static: PaymentGateway.toRupees(...)
        return "Rs " + paise / 100 + "." + String.format("%02d", paise % 100);
    }
}

abstract class LoggingGateway implements PaymentGateway {
    private int calls;                              // state: only possible in a class
    protected abstract String send(long paise);

    @Override public final String charge(long paise) {
        calls++;
        return getClass().getSimpleName() + " call " + calls + ": " + send(paise);
    }

    public String audit() { return "calls so far: " + calls; }
}

class StripeGateway extends LoggingGateway {
    @Override protected String send(long paise) { return "stripe charged " + paise; }
}

class RazorpayGateway extends LoggingGateway {
    @Override protected String send(long paise) { return "razorpay charged " + paise; }
}
// Output:
// StripeGateway call 1: stripe charged 1000
// StripeGateway call 2: stripe charged 1200
// RazorpayGateway call 1: razorpay charged 1000
// RazorpayGateway call 2: razorpay charged 1200
// Rs 123.45
// calls so far: 0
```

The last line is `0` because it uses a brand-new `RazorpayGateway`: each object has its own `calls` field.

**Choosing between them.**

| | Interface | Abstract class |
|---|---|---|
| Multiple inheritance | A class can implement many | A class can extend only one |
| State (instance fields) | No, only constants | Yes, any fields |
| Constructors | No | Yes |
| Method kinds | abstract, default, static, private | any, including abstract |
| Member visibility | public (private for helpers) | any |
| Expresses | a capability ("can be compared", "can charge") | a shared base ("is a kind of gateway") |
| Typical use | APIs between layers, Spring beans, lambdas | sharing code and state among close relatives |

The modern default: **start with an interface**. Add an abstract class only when implementations share state or a lot of code, often as a "skeletal implementation" next to the interface (like `List` and `AbstractList` in the JDK).

**Functional interfaces.** An interface with exactly one abstract method can be implemented with a lambda: `Comparator`, `Runnable`, `Function`. Annotate your own with `@FunctionalInterface`. The third Core Java section covers lambdas in depth.

> **Pitfall:** Adding a new **abstract** method to a published interface breaks every class that implements it. That is exactly why Java 8 introduced default methods: the JDK could add `forEach` and `stream` to `Collection` without breaking millions of classes.

::: deeper
**The diamond rule for default methods.** If a class inherits the same default method from two interfaces, the compiler forces you to choose:

```java
public class Diamond {
    public static void main(String[] args) {
        System.out.println(new Duck().move());
    }
}

interface Swimmer { default String move() { return "swim"; } }
interface Flyer { default String move() { return "fly"; } }

class Duck implements Swimmer, Flyer {
    @Override public String move() {                  // required, or it does not compile
        return Swimmer.super.move() + " and " + Flyer.super.move();
    }
}
// Output:
// swim and fly
```

Without the override, `javac` reports "types Swimmer and Flyer are incompatible; class Duck inherits unrelated defaults for move() from types Swimmer and Flyer". The full resolution rules: (1) a method in the class or a superclass always wins over an interface default ("class wins"); (2) a more specific interface (a sub-interface) wins over its parent; (3) otherwise the class must override and may call `X.super.m()`.

**Default methods cannot override `Object` methods.** A default `equals`, `hashCode` or `toString` in an interface is a compile error, because the "class wins" rule means it could never be called.

**Static interface methods are not inherited** by implementing classes: you must call `PaymentGateway.toRupees(...)`, not `new StripeGateway().toRupees(...)`. That prevents conflicts when a class implements several interfaces.

**Abstract class without abstract methods.** A class can be `abstract` even with no abstract methods, purely to prevent instantiation (for example a base class meant only for extension). And an abstract class can implement an interface without implementing its methods, leaving them to subclasses, as `LoggingGateway` does with nothing but `charge`.

**Marker interfaces** have no methods at all (`Serializable`, `Cloneable`, `RandomAccess`). They tag a class so that code can check `instanceof`. Annotations have mostly replaced new marker interfaces.

> **Interview:** "Abstract class vs interface after Java 8, when interfaces can have method bodies?" Interfaces still cannot hold instance state or constructors, and a class can implement many of them. Use an interface to define a capability or API; use an abstract class to share state and implementation among closely related classes. Many designs use both: an interface for the contract and an abstract class as a convenient base.

> **Interview:** "Why were default methods added?" For interface evolution: to add methods such as `Collection.stream()` and `Iterable.forEach()` to existing interfaces without breaking existing implementations.
:::

::: check
?? Can an interface have a constructor or an instance field?
No. Interfaces can have constants (`public static final`), abstract, default, static and private methods, but no constructors and no instance state.

?? A class implements two interfaces that both define `default void log()`. What must it do?
Override `log()` itself. Inside, it can call either version with `InterfaceName.super.log()`.

?? When would you choose an abstract class over an interface?
When related classes need to share state (instance fields), constructors or a substantial amount of implementation, and they form a true "is-a" family.
:::

::: resources
- Docs: [Interfaces (Java tutorial)](https://dev.java/learn/interfaces/) · dev.java on defining and implementing interfaces
- Docs: [Default methods](https://docs.oracle.com/javase/tutorial/java/IandI/defaultmethods.html) · evolving interfaces, conflict rules
- Docs: [Abstract methods and classes](https://docs.oracle.com/javase/tutorial/java/IandI/abstract.html) · with a comparison to interfaces
- Article: [Interface with default methods vs abstract class](https://www.baeldung.com/java-interface-default-method-vs-abstract-class) · Baeldung on choosing
- Docs: [Defining an interface](https://docs.oracle.com/javase/tutorial/java/IandI/createinterface.html) · the official tutorial on interface members
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 20: prefer interfaces to abstract classes; Item 21: design interfaces for posterity
:::

### static and final keywords; static blocks; access modifiers

::: plain
`static` means "belongs to the class, not to any one object", like the address of a school, which is the same for every student, unlike each student's name. `final` means "cannot be changed after it is set": a final variable cannot be reassigned, a final method cannot be overridden, and a final class cannot be extended. A **static block** is setup code that runs once, when the class is first used. **Access modifiers** (`public`, `protected`, `private`, or none) are the doors and locks that decide which other code may see a class member.
:::

**`static` members.** A static field has one copy shared by all instances; a static method is called on the class (`Math.max(3, 7)`) and has no `this`, so it cannot use instance fields directly. Use static for constants, counters shared across objects, factory methods (`List.of`) and utility methods (`Collections.sort`).

**`final` has three meanings.**

| Applied to | Meaning | Example |
|---|---|---|
| Variable or field | Assigned exactly once | `final int max = 10;` |
| Method | Cannot be overridden | `public final String process()` |
| Class | Cannot be extended | `public final class String` |

A **constant** is a `static final` field with an immutable value, named in `UPPER_SNAKE_CASE`: `static final int MAX_RETRIES = 3;`.

```java
import java.util.ArrayList;
import java.util.List;

public class FinalDemo {
    static final int MAX_ITEMS = 3;                    // constant
    private static int instances = 0;                  // shared counter

    private final List<String> items = new ArrayList<>();   // the reference is final, the list is not

    FinalDemo() { instances++; }

    void add(String item) {
        if (items.size() >= MAX_ITEMS) throw new IllegalStateException("cart full");
        items.add(item);                                // fine: mutating the list
        // items = new ArrayList<>();                   // would not compile: final reference
    }

    public static void main(String[] args) {
        FinalDemo c1 = new FinalDemo();
        FinalDemo c2 = new FinalDemo();
        c1.add("pen");
        c1.add("ink");
        System.out.println(c1.items + " " + c2.items + " instances=" + instances);
        try {
            c1.add("pad");
            c1.add("cap");
        } catch (IllegalStateException e) {
            System.out.println("error: " + e.getMessage());
        }
    }
}
// Output:
// [pen, ink] [] instances=2
// error: cart full
```

> **Pitfall:** `final` makes the **reference** constant, not the object. A `final List` can still be modified. For a list nobody can change, use `List.of(...)` or `List.copyOf(...)`.

**Static and instance initializer blocks.** A `static { ... }` block runs once when the class is initialized, for setup too complex for one expression (loading a lookup table, reading configuration). An instance block `{ ... }` runs on every `new`, before the constructor body. The full order is a favourite interview question:

```java
public class InitOrder {
    public static void main(String[] args) {
        System.out.println("main starts");
        new Child();
        System.out.println("---");
        new Child();
    }
}

class Parent {
    static { System.out.println("1. Parent static block"); }
    { System.out.println("3. Parent instance block"); }
    Parent() { System.out.println("4. Parent constructor"); }
}

class Child extends Parent {
    static { System.out.println("2. Child static block"); }
    { System.out.println("5. Child instance block"); }
    Child() { System.out.println("6. Child constructor"); }
}
// Output:
// main starts
// 1. Parent static block
// 2. Child static block
// 3. Parent instance block
// 4. Parent constructor
// 5. Child instance block
// 6. Child constructor
// ---
// 3. Parent instance block
// 4. Parent constructor
// 5. Child instance block
// 6. Child constructor
```

Static blocks run once per class, parent first, and only when the class is first used (not at program start). Instance blocks and constructors run on every object, parent first.

**Access modifiers**, from most to least open:

| Modifier | Same class | Same package | Subclass (other package) | Everywhere |
|---|---|---|---|---|
| `public` | yes | yes | yes | yes |
| `protected` | yes | yes | yes (through inheritance) | no |
| none (package-private) | yes | yes | no | no |
| `private` | yes | no | no | no |

Top-level classes can be only `public` or package-private. The rule of thumb is **least privilege**: make fields `private`, and widen methods only as far as needed. Package-private is underrated: it keeps helpers visible to tests in the same package but hidden from the rest of the code base.

::: deeper
**When exactly does a class initialize?** On its first **active use**: creating an instance, calling a static method, reading or writing a non-constant static field, or initializing a subclass. Merely declaring a variable of that type does not trigger it, and reading a compile-time constant (`static final int X = 5`) does not either, because `javac` copies the constant's value into the caller. Initialization is thread-safe: the JVM holds a lock while running static initializers, which is what makes the **holder idiom** for lazy singletons work:

```java
public class Config {
    private Config() { }
    private static class Holder { static final Config INSTANCE = new Config(); }
    public static Config get() { return Holder.INSTANCE; }   // Holder initializes on first call

    public static void main(String[] args) {
        System.out.println(Config.get() == Config.get());
    }
}
// Output:
// true
```

If a static initializer throws, the class is marked as failed: the first use gets `ExceptionInInitializerError`, and every later use gets `NoClassDefFoundError`, which confuses people reading logs.

**`final` and the memory model.** Final fields get a special guarantee: once a constructor finishes, every thread that sees the object sees the correct final values, without synchronization (as long as `this` did not escape during construction). That is why immutable objects with final fields are safe to share between threads; the Advanced Java section explains the memory model.

**Effectively final.** Lambdas and inner classes can only use local variables that are **effectively final**: never reassigned after initialization, even without the keyword. Otherwise javac reports "local variables referenced from a lambda expression must be final or effectively final".

**`protected` subtlety.** In another package, a subclass can access a protected member only through references of its own type (or subtypes), not through a reference to the parent type. `protected` also includes package access, which surprises people coming from C++.

> **Interview:** "Can a static method access instance variables?" Not directly: there is no `this` in a static context, so the compiler says "non-static variable balance cannot be referenced from a static context" (with your field name). It can access them through an object reference it is given.

> **Interview:** "What is the difference between `final`, `finally` and `finalize`?" `final` is a modifier (no reassignment, no override, no subclass). `finally` is the block after `try` that always runs (next section). `finalize()` was an `Object` method called before garbage collection; it has been deprecated since Java 9 and deprecated for removal since Java 18 (JEP 421). Use try-with-resources or `Cleaner` instead.
:::

::: check
?? A field is declared `private final List<String> names = new ArrayList<>();`. Can you call `names.add("x")`? Can you write `names = new ArrayList<>();`?
`add` works, because the list object is mutable. The reassignment fails to compile, because the reference is final.

?? For `class B extends A`, both with static blocks, instance blocks and constructors, what runs on the first `new B()`?
A's static block, B's static block, A's instance block, A's constructor, B's instance block, B's constructor. On later `new B()` calls the static blocks do not run again.

?? What is package-private access, and how do you declare it?
Visible only to classes in the same package. You declare it by writing no access modifier at all.
:::

::: resources
- Docs: [Understanding class members (static)](https://docs.oracle.com/javase/tutorial/java/javaOO/classvars.html) · class variables, class methods and constants
- Docs: [Controlling access to members of a class](https://docs.oracle.com/javase/tutorial/java/javaOO/accesscontrol.html) · the access table with examples
- Docs: [Initializing fields](https://docs.oracle.com/javase/tutorial/java/javaOO/initial.html) · static and instance initializer blocks
- Spec: [JLS 12.4: Initialization of classes and interfaces](https://docs.oracle.com/javase/specs/jls/se21/html/jls-12.html#jls-12.4) · exactly when and how classes initialize
- Article: [The final keyword in Java](https://www.baeldung.com/java-final) · Baeldung on final variables, methods and classes
- Article: [A guide to the static keyword in Java](https://www.baeldung.com/java-static) · static fields, methods, blocks and nested classes
:::

### Inner, static nested, local and anonymous classes

::: plain
Java lets you declare a class inside another class, the way a big company has departments inside it. A **static nested class** is a department that just happens to sit in the same building: it is grouped with the outer class for neatness but works on its own. An **inner class** is a department that belongs to one specific branch office: every inner object is tied to an outer object and can use its data. A **local class** is declared inside a method, like a temporary task force, and an **anonymous class** is a one-off helper with no name, written right where it is used.
:::

| Kind | Declared | Tied to an outer instance? | Typical use |
|---|---|---|---|
| Static nested | `static class X` inside a class | No | Builders, helper types, `Map.Entry` |
| Inner (non-static) | `class X` inside a class | Yes, holds `Outer.this` | Iterators and views over the outer object |
| Local | Inside a method body | Yes, if the method is an instance method | A helper used by one method only |
| Anonymous | `new Type() { ... }` in an expression | Yes, if created in an instance context | One-off implementations (before lambdas) |

```java
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Nested {
    public static void main(String[] args) {
        // static nested class: no outer object needed
        Order order = new Order.Builder().id("ORD-7").item("pen").item("ink").build();
        System.out.println(order);

        // inner class: created from an outer object, sees its fields
        Order.LineIterator it = order.new LineIterator();
        while (it.hasNext()) System.out.println(it.next());

        // anonymous class implementing Comparator
        List<String> names = new ArrayList<>(List.of("Chen", "Asha", "Bo"));
        names.sort(new Comparator<String>() {
            @Override public int compare(String a, String b) {
                return Integer.compare(a.length(), b.length());
            }
        });
        System.out.println(names);
        names.sort((a, b) -> a.compareTo(b));             // the same idea as a lambda
        System.out.println(names);
        System.out.println(order.summary());
    }
}

class Order {
    private final String id;
    private final List<String> items;

    private Order(Builder b) {                          // only the builder can construct
        this.id = b.id;
        this.items = List.copyOf(b.items);
    }

    static class Builder {                              // static nested class
        private String id;
        private final List<String> items = new ArrayList<>();
        Builder id(String id) { this.id = id; return this; }
        Builder item(String item) { items.add(item); return this; }
        Order build() { return new Order(this); }
    }

    class LineIterator implements Iterator<String> {   // inner class
        private int index = 0;
        public boolean hasNext() { return index < items.size(); }
        public String next() {
            String line = Order.this.id + " line " + (index + 1) + ": " + items.get(index);
            index++;
            return line;
        }
    }

    String summary() {
        class Counter {                                 // local class: only visible in this method
            int countStartingWith(char c) {
                int n = 0;
                for (String s : items) if (s.charAt(0) == c) n++;
                return n;
            }
        }
        return "items starting with p: " + new Counter().countStartingWith('p');
    }

    @Override public String toString() { return id + " " + items; }
}
// Output:
// ORD-7 [pen, ink]
// ORD-7 line 1: pen
// ORD-7 line 2: ink
// [Bo, Chen, Asha]
// [Asha, Bo, Chen]
// items starting with p: 1
```

Notice the unusual syntax `order.new LineIterator()`: an inner object needs an outer object to belong to. Inside the inner class, `Order.this` refers to that outer object, and plain `items` reaches the outer field automatically.

**The builder pattern** shown here is one of the most common uses of a static nested class: it lets you build an object with many optional parts step by step, then produce an immutable result. Lombok's `@Builder`, which you will see in Spring projects, generates exactly this code.

**Lambdas replaced most anonymous classes.** When the interface has a single abstract method (a functional interface), `(a, b) -> a.compareTo(b)` does the same job as the six-line anonymous class. Anonymous classes are still needed when you must implement an interface with several methods, extend a class, or keep state in fields.

> **Pitfall:** Prefer `static` nested classes unless the nested class truly needs the outer instance. A non-static inner class keeps a hidden reference to its outer object, which wastes memory and can keep a large outer object alive (a memory leak), for example when the inner object is stored in a cache or passed to a long-running thread.

> **Tip:** Local and anonymous classes, like lambdas, can use the enclosing method's local variables only if they are effectively final. If you need to change a counter inside one, use a field or an `AtomicInteger`.

::: deeper
**What the compiler generates.** Each nested class becomes its own `.class` file: `Order$Builder.class`, `Order$LineIterator.class`, `Order$1Counter.class` for the local class, and numbered files such as `Nested$1.class` for anonymous classes. The JVM has no special notion of "inner class" at the instruction level: `javac` adds a synthetic final field `this$0` holding the outer reference and passes it as a hidden first constructor parameter. Captured local variables are copied into synthetic fields too, which is exactly why they must be effectively final: the copy could not follow later changes.

**Private access between nested classes.** `Order`'s constructor is `private`, yet `Builder` calls it. Nested classes can access each other's private members. Before Java 11 the compiler generated hidden "access$000" bridge methods for this; since Java 11 (JEP 181, nest-based access control) the JVM understands **nests** directly, so no bridges are needed.

**Lambdas are not anonymous classes.** A lambda compiles to an `invokedynamic` instruction and a private method; no extra `.class` file is written, and inside a lambda `this` means the enclosing object, while inside an anonymous class `this` means the anonymous object itself.

**Static members in inner classes.** Before Java 16, inner classes could not declare static members (except constants). Since Java 16 (as part of the records work) they can, so you can declare a record or static method inside an inner class.

> **Interview:** "Static nested class vs inner class?" A static nested class does not need an instance of the outer class and cannot access its instance members directly; it is just a namespaced top-level class. An inner class is bound to an outer instance, can use its fields, and is created with `outer.new Inner()`. Prefer static nested unless you need that access.

> **Interview:** "Why must variables used in an anonymous class or lambda be effectively final?" The class or lambda receives a copy of the variable's value. If the variable could change afterwards, the copy and the original would disagree, so Java forbids reassignment to keep the semantics simple and thread-safe.
:::

::: check
?? How do you create an instance of a non-static inner class `Inner` declared inside `Outer`?
From an `Outer` object: `Outer o = new Outer(); Outer.Inner i = o.new Inner();`. Inside an instance method of `Outer`, plain `new Inner()` works because `this` is the outer object.

?? Why is a static nested class usually preferred over an inner class?
It does not hold a hidden reference to an outer object, so it uses less memory and cannot accidentally keep the outer object alive.

?? What can an anonymous class do that a lambda cannot?
Implement an interface with more than one abstract method, extend a class (including abstract classes), and hold its own fields. Also, `this` inside it refers to the anonymous object.
:::

::: resources
- Docs: [Nested classes (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/javaOO/nested.html) · inner, static nested, local and anonymous classes
- Docs: [Anonymous classes](https://docs.oracle.com/javase/tutorial/java/javaOO/anonymousclasses.html) · syntax and when to use them
- Article: [Nested classes in Java](https://www.baeldung.com/java-nested-classes) · Baeldung overview with shadowing rules
- Spec: [JEP 181: Nest-based access control](https://openjdk.org/jeps/181) · how private access between nested classes works since Java 11
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 24: favor static member classes over nonstatic; Item 2: consider a builder
:::

### Object class methods: equals(), hashCode() contract, toString(), clone()

::: plain
Every Java class quietly inherits from `Object`, so every object comes with a few standard methods. `equals` answers "do these two objects mean the same thing?", like two banknotes of the same value, even if they are physically different notes. `hashCode` gives each object a number used to pick a storage drawer in hash-based collections such as `HashMap`; equal objects must always pick the same drawer, or the collection will look in the wrong one. `toString` gives a readable description, and `clone` makes a copy, though it has awkward rules and better alternatives exist.
:::

**The methods every object has.** `equals(Object)`, `hashCode()`, `toString()`, `getClass()`, `clone()` (protected), the threading methods `wait()`, `notify()`, `notifyAll()`, and the deprecated `finalize()`. The first three are the ones you override in everyday code.

**Default behaviour.** `Object.equals` is identity (`this == other`). `Object.hashCode` is an "identity hash" unrelated to the fields. `Object.toString` returns `ClassName@hexHash`. For classes that represent **values** (money, customer IDs, order lines), these defaults are wrong, and that breaks collections:

[[fig:corebasics_hash]]

```java
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class EqualsDemo {
    public static void main(String[] args) {
        Set<PlainId> plain = new HashSet<>();
        plain.add(new PlainId("C-1"));
        System.out.println(plain.contains(new PlainId("C-1")));      // identity equality
        System.out.println(new PlainId("C-1"));                       // default toString

        Set<EqualsOnlyId> half = new HashSet<>();
        half.add(new EqualsOnlyId("C-1"));
        System.out.println(half.contains(new EqualsOnlyId("C-1")));  // equals without hashCode

        Set<CustomerId> good = new HashSet<>();
        good.add(new CustomerId("C-1", "IN"));
        System.out.println(good.contains(new CustomerId("C-1", "IN")));
        System.out.println(new CustomerId("C-1", "IN"));
    }
}

class PlainId {
    final String value;
    PlainId(String value) { this.value = value; }
}

class EqualsOnlyId {
    final String value;
    EqualsOnlyId(String value) { this.value = value; }
    @Override public boolean equals(Object o) {
        return o instanceof EqualsOnlyId other && value.equals(other.value);
    }
    // hashCode NOT overridden: breaks the contract
}

final class CustomerId {
    private final String value;
    private final String country;

    CustomerId(String value, String country) {
        this.value = Objects.requireNonNull(value);
        this.country = Objects.requireNonNull(country);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;                          // fast path
        if (!(o instanceof CustomerId other)) return false;  // also handles null
        return value.equals(other.value) && country.equals(other.country);
    }

    @Override public int hashCode() {
        return Objects.hash(value, country);                 // same fields as equals
    }

    @Override public String toString() {
        return "CustomerId[" + value + ", " + country + "]";
    }
}
// Output:
// false
// PlainId@...
// false
// true
// CustomerId[C-1, IN]
```

The middle case is the dangerous one: `equals` says the objects are equal, but the set computes different hash codes, looks in a different bucket, and never even calls `equals`.

**The `equals` contract** (from the `Object` documentation). For non-null references:

1. **Reflexive:** `x.equals(x)` is `true`.
2. **Symmetric:** `x.equals(y)` equals `y.equals(x)`.
3. **Transitive:** if `x.equals(y)` and `y.equals(z)`, then `x.equals(z)`.
4. **Consistent:** repeated calls give the same answer while the objects do not change.
5. **Non-null:** `x.equals(null)` is `false`, never an exception.

**The `hashCode` contract.**

1. Equal objects **must** have equal hash codes.
2. Unequal objects **may** share a hash code (a collision), but fewer collisions mean faster maps.
3. The hash code must stay the same while the fields used by `equals` do not change.

So the golden rule: **override `equals` and `hashCode` together, using the same fields.** IDEs generate both; records (last topic) generate them automatically.

**`toString`** should return a concise, informative description. It appears in logs, debugger views and exception messages, so overriding it pays off quickly. Never put passwords or card numbers in it.

**`clone()`** is the problem child. To use it, a class must implement the marker interface `Cloneable` and override the protected `clone` method; otherwise `Object.clone` throws `CloneNotSupportedException`. `Object.clone` makes a **shallow** copy: fields are copied, but referenced objects (such as a list) are shared.

```java
import java.util.ArrayList;
import java.util.List;

public class CloneDemo {
    public static void main(String[] args) throws CloneNotSupportedException {
        Basket original = new Basket();
        original.items.add("pen");
        Basket shallow = original.clone();
        Basket copy = new Basket(original);           // copy constructor: a real copy
        shallow.items.add("ink");
        System.out.println(original.items + " " + copy.items);
    }
}

class Basket implements Cloneable {
    List<String> items = new ArrayList<>();
    Basket() { }
    Basket(Basket other) { this.items = new ArrayList<>(other.items); }
    @Override public Basket clone() throws CloneNotSupportedException {
        return (Basket) super.clone();                // shallow: the list is shared
    }
}
// Output:
// [pen, ink] [pen]
```

Prefer a **copy constructor** or a static factory (`Basket.copyOf(other)`) over `clone`.

> **Pitfall:** Never use a mutable field in `hashCode` for an object that sits in a `HashSet` or is a `HashMap` key. If the field changes after insertion, the object's hash changes, it is now in the "wrong" bucket, and `contains` and `remove` can no longer find it.

::: deeper
**`instanceof` vs `getClass()` in `equals`.** With `instanceof`, a subclass instance can equal a parent instance, which breaks symmetry if the subclass adds fields to its own `equals` (`ColorPoint` vs `Point` in Effective Java). With `getClass() != o.getClass()`, only objects of exactly the same class are equal, which is safe but breaks the Liskov principle (a subclass with no new state never equals its parent). Common practice: make value classes `final` and use `instanceof`, as `CustomerId` does, or use records.

**How `Objects.hash` and `String.hashCode` compute.** Both use the polynomial `31 * h + next`. 31 is an odd prime, and `31 * h` can be computed as `(h << 5) - h`. `Objects.hash` creates a varargs array on every call, so in very hot code people write the formula by hand: `int h = value.hashCode(); h = 31 * h + country.hashCode();`.

**How `HashMap` uses both methods.** `put` computes `hashCode()`, mixes the high bits into the low bits (`h ^ (h >>> 16)`), picks a bucket with `hash & (capacity - 1)`, then walks that bucket comparing the stored hash first and calling `equals` only when the hashes match. The next section covers buckets, resizing and treeification in full.

**The identity hash code** is generated on first request (often a thread-local random number in HotSpot, not a memory address) and stored in the object header, so it never changes even when the garbage collector moves the object. `System.identityHashCode(o)` returns it even when `hashCode` is overridden.

**`compareTo` consistency.** `TreeSet` and `TreeMap` use `compareTo` instead of `equals`. If `compareTo` returns 0 for objects that are not `equals` (for example `new BigDecimal("1.0")` and `new BigDecimal("1.00")`), a `HashSet` keeps both but a `TreeSet` keeps one.

> **Interview:** "What happens if you override `equals` but not `hashCode`?" Two equal objects will usually get different identity hash codes, so hash-based collections put them in different buckets: a `HashSet` can contain "duplicates" and `map.get(equalKey)` returns `null`. The contract requires equal objects to have equal hash codes.

> **Interview:** "Can two unequal objects have the same hash code?" Yes; that is a collision, and it is allowed. `"Aa".hashCode()` and `"BB".hashCode()` are both 2112. Collisions only cost performance; maps still work because they confirm with `equals`.

> **Interview:** "Shallow vs deep copy?" A shallow copy duplicates the object but shares the objects its fields refer to; a deep copy duplicates those too, recursively. `Object.clone` is shallow; for a deep copy, copy each mutable field yourself (copy constructors), or design the class to be immutable so copying is unnecessary.
:::

::: check
?? Two `Customer` objects are `equals` but have different `hashCode` values. What goes wrong?
Hash-based collections look in the bucket chosen by the hash code, so they may never compare the two objects: a `HashSet` can hold both, and a `HashMap` lookup with one may miss the entry stored under the other. The contract is broken.

?? List the five properties of the `equals` contract.
Reflexive, symmetric, transitive, consistent, and `x.equals(null)` returns `false`.

?? Why is a copy constructor usually preferred over `clone()`?
`clone` needs the `Cloneable` marker, throws a checked exception, makes only a shallow copy, bypasses constructors and does not work with final fields that need fresh values. A copy constructor is explicit, type-safe and can copy deeply where needed.
:::

::: resources
- Docs: [Object (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Object.html) · the official `equals` and `hashCode` contracts
- Docs: [Object as a superclass (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/IandI/objectclass.html) · every `Object` method explained
- Article: [Java equals() and hashCode() contracts](https://www.baeldung.com/java-equals-hashcode-contracts) · Baeldung with common mistakes
- Article: [Guide to hashCode() in Java](https://www.baeldung.com/java-hashcode) · how hash codes drive hash tables
- Docs: [Objects (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Objects.html) · `equals`, `hash`, `requireNonNull` helpers
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Items 10 to 13: equals, hashCode, toString and clone
:::

### Immutable classes; composition vs inheritance

::: plain
An **immutable** object is like a printed receipt: once issued, nothing on it can change; if the amount is wrong you issue a new receipt. Such objects are simple to reason about and safe to share. **Composition vs inheritance** is about how classes reuse each other. Inheritance says "a car *is a* vehicle" and copies the parent's whole design, quirks included. Composition says "a car *has an* engine": the car holds an engine object and asks it to do engine work. Composition is usually the safer choice, because the car only depends on what the engine promises, not on how it is built inside.
:::

**How to write an immutable class.** `String`, `Integer`, `LocalDate` and `BigDecimal` are immutable; your value classes should be too. The recipe:

1. Make the class `final` (so no subclass can add mutable state or override methods).
2. Make every field `private final`.
3. Provide no setters; "changing" methods return a new object (`withX(...)`).
4. Make **defensive copies** of mutable inputs in the constructor and never hand out internal mutable objects.
5. Initialize everything in the constructor (validate there, too).

```java
import java.util.ArrayList;
import java.util.List;

public class ImmutableDemo {
    public static void main(String[] args) {
        List<String> lines = new ArrayList<>(List.of("pen", "ink"));
        Invoice inv = new Invoice("INV-1", 250, lines);
        lines.add("hacked");                          // caller changes its own list...
        System.out.println(inv);                      // ...the invoice is unaffected

        Invoice discounted = inv.withAmount(200);     // a new object, the old one unchanged
        System.out.println(inv.amount() + " " + discounted.amount());

        try {
            inv.lines().add("more");                  // the returned list is unmodifiable
        } catch (UnsupportedOperationException e) {
            System.out.println("cannot modify lines");
        }
    }
}

final class Invoice {
    private final String id;
    private final long amount;
    private final List<String> lines;

    Invoice(String id, long amount, List<String> lines) {
        if (amount < 0) throw new IllegalArgumentException("negative amount");
        this.id = id;
        this.amount = amount;
        this.lines = List.copyOf(lines);              // defensive, unmodifiable copy
    }

    String id() { return id; }
    long amount() { return amount; }
    List<String> lines() { return lines; }            // safe: already unmodifiable

    Invoice withAmount(long newAmount) { return new Invoice(id, newAmount, lines); }

    @Override public String toString() { return id + " " + amount + " " + lines; }
}
// Output:
// INV-1 250 [pen, ink]
// 250 200
// cannot modify lines
```

**Why bother?** Immutable objects are automatically thread-safe (no locks needed), make safe `HashMap` keys and set elements (their hash code never changes), can be shared and cached freely, and are easy to reason about: once you have checked an object is valid, it stays valid forever. The cost is creating new objects for every change, which modern garbage collectors handle well for small objects.

**Composition vs inheritance.** Inheritance couples a subclass to its parent's **implementation**, not just its interface. The classic demonstration from Effective Java: count how many elements were ever added to a set.

```java
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CompositionDemo {
    public static void main(String[] args) {
        CountingHashSet<String> inherited = new CountingHashSet<>();
        inherited.addAll(List.of("a", "b", "c"));
        System.out.println("inheritance count: " + inherited.addCount);

        CountingSet<String> composed = new CountingSet<>(new HashSet<>());
        composed.addAll(List.of("a", "b", "c"));
        System.out.println("composition count: " + composed.addCount());
    }
}

// Inheritance: depends on how HashSet works inside
class CountingHashSet<E> extends HashSet<E> {
    int addCount = 0;
    @Override public boolean add(E e) { addCount++; return super.add(e); }
    @Override public boolean addAll(Collection<? extends E> c) {
        addCount += c.size();
        return super.addAll(c);          // HashSet.addAll calls add() for each element...
    }
}

// Composition: wraps any Set and forwards calls to it
class CountingSet<E> {
    private final Set<E> inner;
    private int addCount = 0;
    CountingSet(Set<E> inner) { this.inner = inner; }
    boolean add(E e) { addCount++; return inner.add(e); }
    boolean addAll(Collection<? extends E> c) { addCount += c.size(); return inner.addAll(c); }
    int addCount() { return addCount; }
    int size() { return inner.size(); }
}
// Output:
// inheritance count: 6
// composition count: 3
```

The inherited version counts every element twice, because `HashSet.addAll` (inherited from `AbstractCollection`) happens to call `add`, which is overridden. Nothing in `HashSet`'s public contract promises that, and a future JDK could change it. The composed version only uses the wrapped set's public methods, so it does not care how they are implemented, and it works with **any** `Set` (`TreeSet`, `LinkedHashSet`). This "wrapper that forwards calls" is the **decorator** pattern.

| | Inheritance ("is-a") | Composition ("has-a") |
|---|---|---|
| Coupling | To the parent's implementation | Only to the component's interface |
| Flexibility | Fixed at compile time | Swap the component at run time |
| Reuse across types | Only one parent class | Hold as many components as you like |
| Good for | True type hierarchies you control (`Payment` → `CardPayment`) | Adding behaviour, reusing services, almost everything else |

Spring applications are built almost entirely on composition: an `OrderService` **has** an `OrderRepository` and a `PaymentClient`, injected through its constructor, rather than extending them.

> **Tip:** The rule of thumb is "favour composition over inheritance". Use inheritance when the subclass is genuinely a subtype (every `CardPayment` can be used wherever a `Payment` is expected) and the parent was designed for extension; otherwise wrap and delegate.

::: deeper
**Shallow immutability.** `final` fields guarantee nothing about the objects they point to. A `final Date` field or `final int[]` field can still be modified through the reference. That is why the recipe insists on defensive copies, and why modern code prefers immutable building blocks: `LocalDate` instead of `Date`, `List.copyOf` instead of a raw `ArrayList`.

**`List.copyOf` vs `Collections.unmodifiableList`.** `List.copyOf(list)` (Java 10) makes an independent, unmodifiable copy (and returns the same list if it is already an immutable `List.of` list); it rejects `null` elements. `Collections.unmodifiableList(list)` is only a **view**: callers cannot modify it, but changes to the underlying list show through. For immutable classes you want the copy.

**Copy before validating.** When the input is mutable, copy first and validate the copy, otherwise another thread could change the input between your check and your copy (a "time-of-check to time-of-use" attack).

**Immutability and the memory model.** The Java memory model guarantees that `final` fields are visible correctly to all threads once the constructor has finished, without `volatile` or locks. That is the formal reason immutable objects are thread-safe.

**Designing for inheritance.** If you do allow subclassing, document which overridable methods call each other ("self-use"), as the JDK does with the "Implementation Requirements" sections of its Javadoc, or make methods `final`. Otherwise, make the class `final` or give it a private constructor with static factories. Sealed classes (next topic) offer a middle ground: inheritance limited to a known list.

> **Interview:** "How do you make a class immutable?" Make the class `final`, all fields `private final`, no setters, initialize everything in the constructor, defensively copy mutable inputs and never expose mutable internals (return copies or unmodifiable views). For simple data carriers, a `record` does most of this automatically, but you still need to copy mutable components.

> **Interview:** "Why favour composition over inheritance?" Inheritance breaks encapsulation: the subclass depends on the parent's implementation details, which can change and silently break it (the counting `HashSet` example). Composition depends only on public interfaces, can be changed at run time, lets one class combine many components, and makes testing easier because components can be replaced with mocks.
:::

::: check
?? A class has `private final List<String> tags;` set with `this.tags = tags;` and a getter `return tags;`. Is it immutable? How do you fix it?
No: the caller still holds the original list and can change it, and anyone using the getter can change it too. Store `List.copyOf(tags)` in the constructor (which also makes the returned list unmodifiable).

?? Why does the inherited counting set report 6 instead of 3?
`HashSet.addAll` internally calls `add` for each element, which is overridden and counts again. The subclass depended on an undocumented implementation detail of its parent.

?? Give two benefits of immutable objects.
Any two of: thread safety without locks, safe use as `HashMap` keys and in sets, free sharing and caching, simpler reasoning (always valid once constructed), no need for defensive copying when passing them around.
:::

::: resources
- Docs: [A strategy for defining immutable objects](https://docs.oracle.com/javase/tutorial/essential/concurrency/imstrat.html) · the official recipe
- Article: [Immutable objects in Java](https://www.baeldung.com/java-immutable-object) · Baeldung on final, defensive copies and benefits
- Article: [Inheritance and composition (is-a vs has-a)](https://www.baeldung.com/java-inheritance-composition) · Baeldung comparison with examples
- Docs: [List.copyOf (Java SE 21 API)](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/List.html#copyOf(java.util.Collection)) · unmodifiable copies
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Item 17: minimize mutability; Item 18: favor composition over inheritance
:::

### Enums (with fields & methods), records, sealed classes

::: plain
These three are Java's modern tools for modelling data precisely. An **enum** is a fixed menu: an order status can only be NEW, PAID, SHIPPED or CANCELLED, nothing else. A **record** is a labelled container for data, like a form with fixed fields, where Java writes all the boring code (constructor, getters, `equals`, `hashCode`, `toString`) for you. A **sealed class** is a family with a closed guest list: you name exactly which subclasses are allowed, so the compiler knows every possibility and can check that your code handles them all.
:::

**Enums.** An enum declares a fixed set of named constants. Each constant is a single object, so you compare them with `==`. Enums are full classes: they can have fields, constructors, methods, and even a different method body per constant.

```java
import java.util.EnumMap;
import java.util.Map;

public class EnumDemo {
    enum OrderStatus {
        NEW("Order placed", false),
        PAID("Payment received", false),
        SHIPPED("On the way", false),
        DELIVERED("Delivered", true),
        CANCELLED("Cancelled", true);

        private final String label;                 // each constant has its own values
        private final boolean terminal;

        OrderStatus(String label, boolean terminal) {   // enum constructors are always private
            this.label = label;
            this.terminal = terminal;
        }

        String label() { return label; }

        boolean canMoveTo(OrderStatus next) {
            if (terminal) return false;
            return next == CANCELLED || next.ordinal() == ordinal() + 1;
        }
    }

    enum Op {
        ADD { int apply(int a, int b) { return a + b; } },     // constant-specific body
        MUL { int apply(int a, int b) { return a * b; } };
        abstract int apply(int a, int b);
    }

    public static void main(String[] args) {
        OrderStatus s = OrderStatus.valueOf("PAID");       // from a string, e.g. a database column
        System.out.println(s + " / " + s.label() + " / ordinal " + s.ordinal());
        System.out.println(s.canMoveTo(OrderStatus.SHIPPED) + " " + s.canMoveTo(OrderStatus.DELIVERED));
        System.out.println(OrderStatus.DELIVERED.canMoveTo(OrderStatus.CANCELLED));

        Map<OrderStatus, Integer> counts = new EnumMap<>(OrderStatus.class);
        for (OrderStatus st : OrderStatus.values()) counts.put(st, st.label().length());
        System.out.println(counts);
        System.out.println(Op.ADD.apply(3, 4) + " " + Op.MUL.apply(3, 4));
    }
}
// Output:
// PAID / Payment received / ordinal 1
// true false
// false
// {NEW=12, PAID=16, SHIPPED=10, DELIVERED=9, CANCELLED=9}
// 7 12
```

Useful built-ins: `values()` (all constants in order), `valueOf(String)` (throws `IllegalArgumentException` for unknown names), `name()`, `ordinal()` (position, from 0), and `compareTo`. `EnumMap` and `EnumSet` are compact, very fast collections keyed by enum constants.

> **Pitfall:** Never store `ordinal()` in a database or file. Inserting a new constant in the middle shifts every number after it. Store `name()` instead (with JPA, use `@Enumerated(EnumType.STRING)`).

**Records** (Java 16) are concise, immutable data carriers. `record Money(long paise, String currency) {}` automatically gets private final fields, a constructor with all components, accessor methods named after the components (`paise()`, not `getPaise()`), and `equals`, `hashCode` and `toString` based on all components. You can add validation in a **compact constructor**, plus static factories and extra methods.

```java
import java.util.HashSet;
import java.util.Set;

public class RecordDemo {
    record Money(long paise, String currency) {
        Money {                                          // compact constructor: validate
            if (paise < 0) throw new IllegalArgumentException("negative amount");
            currency = currency.toUpperCase();           // may normalize parameters
        }
        static Money inr(long paise) { return new Money(paise, "INR"); }
        Money plus(Money other) {
            if (!currency.equals(other.currency)) throw new IllegalArgumentException("currency mismatch");
            return new Money(paise + other.paise, currency);
        }
    }

    public static void main(String[] args) {
        Money a = new Money(1500, "inr");
        Money b = Money.inr(1500);
        System.out.println(a);
        System.out.println(a.equals(b) + " " + (a == b) + " " + a.paise());
        Set<Money> set = new HashSet<>(Set.of(a));
        System.out.println(set.contains(b));
        System.out.println(a.plus(Money.inr(250)));
        try {
            new Money(-1, "INR");
        } catch (IllegalArgumentException e) {
            System.out.println("rejected: " + e.getMessage());
        }
    }
}
// Output:
// Money[paise=1500, currency=INR]
// true false 1500
// true
// Money[paise=1750, currency=INR]
// rejected: negative amount
```

Records cannot extend another class (they implicitly extend `java.lang.Record`), cannot declare extra instance fields, and are implicitly `final`. They **can** implement interfaces, have static fields and methods, and be generic. They are ideal for DTOs (data transfer objects) in REST APIs, query results, map keys and messages.

**Sealed classes and interfaces** (Java 17) restrict which classes may extend or implement them, using `permits`. Each permitted subclass must be `final`, `sealed` (with its own `permits`), or `non-sealed` (open again). Combined with records and pattern matching, the compiler knows every case and checks that a `switch` covers them all, **without** a `default`:

```java
public class SealedDemo {
    sealed interface PaymentResult permits Success, Failure, Pending {}
    record Success(String txId) implements PaymentResult {}
    record Failure(String reason, boolean retryable) implements PaymentResult {}
    record Pending(int checksLeft) implements PaymentResult {}

    static String handle(PaymentResult r) {
        return switch (r) {                                       // exhaustive: no default needed
            case Success s -> "ship order, tx " + s.txId();
            case Failure(String reason, boolean retry) when retry -> "retry later: " + reason;
            case Failure(String reason, boolean retry) -> "cancel order: " + reason;
            case Pending(int left) -> "check again (" + left + " checks left)";
        };
    }

    public static void main(String[] args) {
        System.out.println(handle(new Success("TX-91")));
        System.out.println(handle(new Failure("bank timeout", true)));
        System.out.println(handle(new Failure("card blocked", false)));
        System.out.println(handle(new Pending(3)));
    }
}
// Output:
// ship order, tx TX-91
// retry later: bank timeout
// cancel order: card blocked
// check again (3 checks left)
```

`case Failure(String reason, boolean retry)` is a **record pattern** (Java 21): it checks the type and unpacks the components into variables in one step. If you later add a fourth result type to `permits`, every `switch` that does not handle it stops compiling, so the compiler finds every place you must update. This style is called **algebraic data types** or data-oriented programming.

| Tool | Use it for | Key property |
|---|---|---|
| `enum` | A fixed set of named values (statuses, roles, currencies) | One instance per constant; `==`, `switch`, `EnumMap` |
| `record` | Immutable data (DTOs, value objects, keys) | Generated constructor, accessors, `equals`, `hashCode`, `toString` |
| `sealed` | A closed family of types with different data | Exhaustive `switch`; the compiler knows every subtype |

::: deeper
**Enums under the hood.** `enum OrderStatus` compiles to `final class OrderStatus extends java.lang.Enum<OrderStatus>` with a `public static final` field per constant, created in a static initializer, and a synthetic `values()` that returns a **clone** of an internal array (so call it once and keep the result in hot loops). A constant with its own body (like `Op.ADD`) becomes an anonymous subclass. Enums cannot be instantiated with `new`, even through reflection, and serialization preserves their identity. That is why Effective Java calls a single-element enum (`enum Registry { INSTANCE; ... }`) the best way to write a singleton.

**Switching on enums** uses the ordinal through a synthetic lookup array, so it is as fast as an `int` switch. In a `switch` you write `case PAID`, not `case OrderStatus.PAID` (qualified names in case labels are allowed only since Java 21).

**Records under the hood.** The compiler generates the canonical constructor and accessors, and `equals`, `hashCode` and `toString` are implemented through `invokedynamic` and `ObjectMethods.bootstrap`. Records are **shallowly** immutable: `record Team(List<String> members)` still exposes a mutable list unless the compact constructor does `members = List.copyOf(members);`. Jackson (2.12+) and Spring support records out of the box, so they are the natural choice for request and response bodies. JPA entities, however, cannot be records, because JPA needs a no-argument constructor, mutable state and proxies.

**Sealed rules.** Permitted subclasses must be in the same module, or in the same package if you are not using modules. When they are in the same file, you may omit the `permits` clause and the compiler infers it. `getPermittedSubclasses()` lists them through reflection.

**Version notes.** Enums since Java 5; records final in Java 16 (JEP 395); sealed classes final in Java 17 (JEP 409); record patterns and pattern matching for `switch` final in Java 21 (JEPs 440 and 441). Java 22 added unnamed variables, so `case Failure(String reason, _)` ignores a component you do not need. Derived record creation (`withers`) is still only a proposal as of Java 25.

> **Interview:** "Record vs a class with Lombok `@Data`?" A record is immutable, final, part of the language (no annotation processor), and its accessors have no `get` prefix. `@Data` generates getters **and setters**, so the object is mutable. Use records for value-like data; use regular classes (or `@Value`) when you need inheritance, mutable state or frameworks that require a no-argument constructor (JPA).

> **Interview:** "Why use sealed classes?" To model a closed set of alternatives (payment results, shapes, events) so the compiler can check exhaustiveness: a `switch` over a sealed type needs no `default`, and adding a new subtype makes every non-exhaustive `switch` fail to compile instead of failing at run time.
:::

::: check
?? Why should you compare enum values with `==` rather than `equals`?
Each constant is a single instance, so `==` is correct, slightly faster and null-safe (`status == OrderStatus.PAID` cannot throw when `status` is null, while `status.equals(...)` can).

?? What does the compiler generate for `record Point(int x, int y) {}`?
Private final fields `x` and `y`, a canonical constructor, accessors `x()` and `y()`, and `equals`, `hashCode` and `toString` based on both components. The class is implicitly final and extends `java.lang.Record`.

?? Why does a `switch` over a sealed interface not need a `default` branch?
The compiler knows the complete list of permitted subtypes, so it can verify that every one is handled. Adding a new subtype then causes a compile error in every switch that misses it.
:::

::: resources
- Docs: [Enum types (Java tutorial)](https://docs.oracle.com/javase/tutorial/java/javaOO/enum.html) · enums with fields and methods
- Docs: [Using record to model immutable data](https://dev.java/learn/records/) · dev.java guide to records
- Docs: [Sealed classes (Java SE 21 guide)](https://docs.oracle.com/en/java/javase/21/language/sealed-classes-and-interfaces.html) · permits, final, sealed and non-sealed
- Spec: [JEP 395: Records](https://openjdk.org/jeps/395) · the design and rules of records
- Spec: [JEP 409: Sealed classes](https://openjdk.org/jeps/409) · motivation and examples
- Article: [Java 14 record keyword](https://www.baeldung.com/java-record-keyword) · Baeldung on records with examples
- Book: *Effective Java* (3rd edition) by Joshua Bloch · Items 34 to 38 on enums, and Item 3 on the enum singleton
:::

## Wrap-up

### Cheat sheet

| Task | Code |
|---|---|
| Compile and run | `javac Hello.java` then `java Hello`, or just `java Hello.java` |
| Compare wrapper values | `a.equals(b)`, never `a == b` (cache only covers -128 to 127) |
| Avoid silent overflow | `Math.addExact(a, b)`, or use `long` / `BigDecimal` for money |
| Print an array | `Arrays.toString(a)`, `Arrays.deepToString(grid)` |
| Compare strings | `"PAID".equals(status)` (null-safe), never `==` |
| Build text in a loop | `StringBuilder sb = new StringBuilder(); sb.append(x);` |
| Multi-line SQL or JSON | text block: `"""` on its own line, then the text |
| Switch that returns a value | `var x = switch (s) { case A, B -> 1; default -> 0; };` |
| Type test and cast in one step | `if (o instanceof String s && !s.isBlank())` |
| Call another constructor | `this(...)` or `super(...)` as the first statement |
| Interface helper with a body | `default`, `static` or `private` method in the interface |
| Constant | `static final int MAX_RETRIES = 3;` |
| Lazy thread-safe singleton | holder class idiom, or `enum Singleton { INSTANCE; }` |
| Value class equality | override `equals` and `hashCode` together with the same fields (`Objects.hash`) |
| Copy an object | copy constructor `new Basket(other)` rather than `clone()` |
| Immutable class | `final` class, `private final` fields, no setters, `List.copyOf` for inputs |
| Data carrier | `record Money(long paise, String currency) { Money { /* validate */ } }` |
| Closed set of types | `sealed interface R permits A, B {}` plus an exhaustive `switch` |

### Practice

::: try
**1. Predict the output.** Work it out on paper first, then run it.

```java
public class Predict {
    static void change(StringBuilder sb, String s, int n) {
        sb.append("!");
        s = s + "!";
        n = n + 1;
    }

    public static void main(String[] args) {
        Integer a = 100, b = 100, c = 1000, d = 1000;
        System.out.println((a == b) + " " + (c == d) + " " + c.equals(d));
        String s1 = "hi", s2 = "h" + "i", s3 = new String("hi");
        System.out.println((s1 == s2) + " " + (s1 == s3) + " " + s1.equals(s3));
        StringBuilder sb = new StringBuilder("wow");
        String s = "wow";
        int n = 1;
        change(sb, s, n);
        System.out.println(sb + " " + s + " " + n);
    }
}
```
:::

?? Answer to 1
`true false true`, then `true false true`, then `wow! wow 1`. The `Integer` cache covers 100 but not 1000; `"h" + "i"` is a compile-time constant so it is pooled, while `new String` is a separate object; and Java passes copies of values: the method mutated the shared `StringBuilder`, but reassigning `s` and `n` only changed its local copies.

::: try
**2. Palindrome check (live-coding classic).** Write `static boolean isPalindrome(String text)` that ignores case and every character that is not a letter or digit, so `"A man, a plan, a canal: Panama"` is a palindrome. Do it in O(n) time without building a reversed copy. Print the results for that sentence, `"race a car"` and `""`.
:::

?? A solution to 2

```java
public class Palindrome {
    static boolean isPalindrome(String text) {
        int left = 0, right = text.length() - 1;
        while (left < right) {
            char l = text.charAt(left), r = text.charAt(right);
            if (!Character.isLetterOrDigit(l)) { left++; continue; }
            if (!Character.isLetterOrDigit(r)) { right--; continue; }
            if (Character.toLowerCase(l) != Character.toLowerCase(r)) return false;
            left++;
            right--;
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(isPalindrome("A man, a plan, a canal: Panama"));
        System.out.println(isPalindrome("race a car"));
        System.out.println(isPalindrome(""));
    }
}
// Output:
// true
// false
// true
```

Two pointers walk inwards, skipping punctuation, so the check uses O(1) extra memory. In an interview, mention the edge cases (empty string, only punctuation, mixed case) before you start coding.

::: try
**3. Deduplicate employees.** An HR import produces `Employee` objects with `id`, `email` and `name`. Two rows describe the same person when the `id` matches and the email matches ignoring case. Write the class so that a `HashSet<Employee>` removes duplicates, and show that adding `(7, "asha@corp.com", "Asha")` and `(7, "ASHA@corp.com", "Asha R")` leaves one element.
:::

?? A solution to 3

```java
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public class Dedup {
    public static void main(String[] args) {
        Set<Employee> people = new HashSet<>();
        people.add(new Employee(7, "asha@corp.com", "Asha"));
        people.add(new Employee(7, "ASHA@corp.com", "Asha R"));
        people.add(new Employee(8, "ben@corp.com", "Ben"));
        System.out.println(people.size());
    }
}

final class Employee {
    private final int id;
    private final String email;
    private final String name;

    Employee(int id, String email, String name) {
        this.id = id;
        this.email = email.toLowerCase(Locale.ROOT);   // normalize once, so equals and hashCode agree
        this.name = name;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Employee e)) return false;
        return id == e.id && email.equals(e.email);
    }

    @Override public int hashCode() { return Objects.hash(id, email); }

    @Override public String toString() { return "Employee[" + id + ", " + email + ", " + name + "]"; }
}
// Output:
// 2
```

Normalizing the email in the constructor is the key step. If `equals` used `equalsIgnoreCase` while `hashCode` hashed the raw email, equal objects could get different hash codes and the contract would break. `name` is deliberately left out of both methods, because it is not part of the identity.

::: try
**4. Model notifications with sealed types.** ShopSphere sends `Email(to, subject)`, `Sms(phone, text)` and `Push(deviceId, title, silent)` notifications. Model them as records under a sealed interface, and write a `describe` method with an exhaustive `switch` (no `default`) that prints `email to ...`, `sms to ...`, and for push either `silent push to ...` or `push to ...: title`. Then add a fourth type in your head: what does the compiler do?
:::

?? A solution to 4

```java
import java.util.List;

public class Notifications {
    sealed interface Notification permits Email, Sms, Push {}
    record Email(String to, String subject) implements Notification {}
    record Sms(String phone, String text) implements Notification {}
    record Push(String deviceId, String title, boolean silent) implements Notification {}

    static String describe(Notification n) {
        return switch (n) {
            case Email e -> "email to " + e.to() + ": " + e.subject();
            case Sms s -> "sms to " + s.phone();
            case Push p when p.silent() -> "silent push to " + p.deviceId();
            case Push p -> "push to " + p.deviceId() + ": " + p.title();
        };
    }

    public static void main(String[] args) {
        List<Notification> all = List.of(
            new Email("asha@mail.com", "Order shipped"),
            new Sms("+91-9000000000", "OTP 1234"),
            new Push("dev-1", "Sale!", false),
            new Push("dev-2", "sync", true));
        all.forEach(n -> System.out.println(describe(n)));
    }
}
// Output:
// email to asha@mail.com: Order shipped
// sms to +91-9000000000
// push to dev-1: Sale!
// silent push to dev-2
```

If you add `record WhatsApp(...)` to the `permits` list, `describe` stops compiling with "the switch expression does not cover all possible input values", pointing you to exactly the code that must handle the new type.

### What comes next

You can now write and run Java programs, use its types, strings and arrays correctly, and design classes with the four pillars, interfaces, a correct `equals` and `hashCode`, immutability, enums, records and sealed types. The next section, **Core Java 2: exceptions, generics and collections**, builds directly on this: exceptions use the class hierarchy you just learned, generics make your classes type-safe for any element type, and the collections framework (`ArrayList`, `HashMap`, `TreeSet` and friends) is where `equals`, `hashCode` and `compareTo` do their real work, including how `HashMap` buckets function inside.
