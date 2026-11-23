Object.assign(FIGS, {
corebasics_pipeline:{ t:"From source code to running program", cap:"javac turns your .java file into platform-neutral bytecode once. Each JVM then loads, verifies and interprets it, and the JIT compiles hot methods to native machine code while the program runs.", svg:S("corebasics1", 250, "Java compilation and execution pipeline",
  B(10, 40, 120, 56, "box", ["Hello.java", "source code"]) +
  A("corebasics1", "M130 68 L170 68") + T(150, 58, "javac", "sm mono", "middle") +
  B(172, 40, 130, 56, "fill", ["Hello.class", "bytecode"]) +
  A("corebasics1", "M302 68 L342 68") +
  B(344, 20, 406, 200, "sp", ["", ""]) + T(547, 40, "JVM (java Hello)", "b", "middle") +
  B(360, 56, 116, 50, "box", ["Class loader", "find and load"]) +
  A("corebasics1", "M476 81 L496 81") +
  B(498, 56, 116, 50, "box", ["Verifier", "type-safe?"]) +
  A("corebasics1", "M614 81 L630 81") +
  B(632, 56, 108, 50, "box", ["Interpreter", "runs at once"]) +
  A("corebasics1", "M686 106 L686 140", "acc") + T(692, 128, "hot code", "sm", "start") +
  B(560, 142, 180, 56, "ka", ["JIT (C1, then C2)", "native machine code"]) +
  B(360, 142, 180, 56, "st", ["Heap + GC", "objects live here"]) +
  T(165, 140, "compile once, on your machine", "sm", "middle") +
  T(165, 160, "run anywhere a JVM exists", "sm", "middle")) },

corebasics_pool:{ t:"The string pool", cap:"Identical literals share one pooled object, so a == b is true. new String(\"java\") always builds a separate heap object, so a == c is false even though a.equals(c) is true; c.intern() returns the pooled one.", svg:S("corebasics2", 240, "String pool with shared literals and a separate new String object",
  B(20, 30, 110, 36, "box", ["a"]) + B(20, 90, 110, 36, "box", ["b"]) + B(20, 150, 110, 36, "box", ["c"]) +
  T(75, 210, "variables (references)", "sm", "middle") +
  B(300, 20, 260, 110, "sp", ["", ""]) + T(430, 40, "String pool (in the heap)", "b", "middle") +
  B(360, 60, 140, 44, "fill", ["\"java\""]) +
  B(300, 150, 260, 70, "box", ["", ""]) + T(430, 170, "ordinary heap", "b", "middle") +
  B(360, 180, 140, 32, "pr", ["\"java\" (copy)"], false) +
  A("corebasics2", "M130 48 L358 76") + A("corebasics2", "M130 108 L358 88") +
  A("corebasics2", "M130 168 L358 196", "kac") +
  T(600, 70, "a == b  true", "mono", "start") + T(600, 100, "a == c  false", "mono", "start") +
  T(600, 130, "a.equals(c)  true", "mono", "start") + T(600, 160, "c.intern() == a", "mono", "start")) },

corebasics_heap:{ t:"Stack, heap and pass-by-value", cap:"Local variables live in each method's stack frame. A primitive holds its value directly; an object variable holds a reference to an object on the heap. Calling a method copies those values: both frames then point at the same Wallet, but reassigning the copy cannot change the caller's variable.", svg:S("corebasics3", 260, "Stack frames holding primitives and references to heap objects",
  B(20, 20, 250, 100, "sp", ["", ""]) + T(145, 40, "main() frame", "b", "middle") +
  B(36, 54, 100, 50, "box", ["x = 1", "int value"]) + B(154, 54, 100, 50, "box", ["mine", "reference"]) +
  B(20, 140, 250, 100, "sp", ["", ""]) + T(145, 160, "addBonus(w) frame", "b", "middle") +
  B(154, 174, 100, 50, "box", ["w", "copy of ref"]) +
  T(145, 254, "stack (one frame per call)", "sm", "middle") +
  B(470, 20, 270, 220, "st", ["", ""]) + T(605, 40, "heap (shared objects)", "b", "middle") +
  B(530, 90, 150, 56, "fill", ["Wallet", "amount = 150"]) +
  A("corebasics3", "M254 79 L528 110") + A("corebasics3", "M254 199 L528 130") +
  T(380, 170, "same object", "sm", "middle")) },

corebasics_dispatch:{ t:"Dynamic dispatch through the vtable", cap:"The compiler only records Animal.sound(). At run time the JVM follows the reference to the actual object, reads its class, and calls the method in that class's table, so a Dog answers Woof.", svg:S("corebasics4", 250, "Dynamic dispatch using virtual method tables",
  B(20, 90, 170, 56, "box", ["Animal a", "declared type"]) +
  T(105, 175, "a.sound()", "mono", "middle") + T(105, 195, "invokevirtual Animal.sound", "sm", "middle") +
  A("corebasics4", "M190 118 L268 118") +
  B(270, 90, 150, 56, "fill", ["Dog object", "class pointer"]) +
  A("corebasics4", "M420 118 L478 118") +
  B(480, 20, 260, 92, "box", ["", ""]) + T(610, 40, "Animal vtable", "b", "middle") +
  T(500, 66, "sound()  -> Animal.sound", "mono", "start") + T(500, 90, "copy()   -> Animal.copy", "mono", "start") +
  B(480, 128, 260, 100, "ka", ["", ""]) + T(610, 148, "Dog vtable (actual class)", "b", "middle") +
  T(500, 174, "sound()  -> Dog.sound", "mono", "start") + T(500, 198, "copy()   -> Dog.copy", "mono", "start") +
  T(610, 220, "overridden slots replaced", "sm", "middle")) },

corebasics_hash:{ t:"Why equals needs hashCode", cap:"A HashSet first uses hashCode to choose a bucket and only then calls equals inside that bucket. If two equal objects have different hash codes, the lookup goes to the wrong bucket and never compares them.", svg:S("corebasics5", 250, "Hash buckets with equal objects landing in different buckets",
  B(20, 30, 200, 50, "box", ["new Id(\"C-1\")", "stored: hash 41"]) +
  B(20, 150, 200, 50, "pr", ["new Id(\"C-1\")", "lookup: hash 97"]) +
  B(320, 10, 160, 40, "box", ["bucket 0"], false) +
  B(320, 60, 160, 40, "fill", ["bucket 1: C-1"], false) +
  B(320, 110, 160, 40, "box", ["bucket 2"], false) +
  B(320, 160, 160, 40, "box", ["bucket 3: empty"], false) +
  A("corebasics5", "M220 55 L318 78") + A("corebasics5", "M220 175 L318 180", "kac") +
  T(560, 90, "equal objects,", "sm", "start") + T(560, 110, "different hashes:", "sm", "start") +
  T(560, 130, "contains() is false", "b", "start") +
  T(560, 170, "fix: override hashCode", "sm", "start") + T(560, 190, "with the same fields", "sm", "start")) },
});
