
var MK = function(id){ return '<defs><marker id="' + id + '" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path class="mk" d="M0,0 L10,5 L0,10 z"/></marker></defs>'; };
var FIGS = {};
/* helpers for lesson diagrams */
function B(x, y, w, h, c, lines, bold){
  var s = '<rect class="' + c + '" x="' + x + '" y="' + y + '" width="' + w + '" height="' + h + '" rx="8"/>';
  var n = lines.length, lh = 16, y0 = y + h / 2 - (n - 1) * lh / 2 + 4;
  lines.forEach(function(t, i){ s += '<text x="' + (x + w / 2) + '" y="' + (y0 + i * lh) + '" text-anchor="middle"' + (i === 0 ? (bold === false ? '' : ' class="b"') : ' class="sm"') + '>' + t + '</text>'; });
  return s;
}
function A(id, d, cls){ return '<path class="' + (cls || "ln") + '" d="' + d + '" marker-end="url(#' + id + ')"/>'; }
function T(x, y, t, c, anchor){ return '<text x="' + x + '" y="' + y + '"' + (c ? ' class="' + c + '"' : '') + (anchor ? ' text-anchor="' + anchor + '"' : '') + '>' + t + '</text>'; }
function S(id, h, label, body){ return '<svg class="dg" viewBox="0 0 760 ' + h + '" role="img" aria-label="' + label + '">' + MK(id) + body + '</svg>'; }


/* figs2: Part I (the language) diagrams, original drawings */
Object.assign(FIGS, {

names:{ t:"Names point to objects", cap:"Assignment binds a name to an object; it never copies. After b = a, both names refer to the same list, so a change made through b is visible through a. Rebinding c to a new object leaves the old one alone, and an object is freed when no names or containers refer to it.", svg:S("y1", 250, "Names in a namespace referring to objects on the heap",
  '<rect class="fill" x="20" y="20" width="200" height="210" rx="12"/>' + T(120, 44, "Namespace (names)", "b", "middle") +
  B(50, 64, 140, 40, "pr", ["a"]) + B(50, 118, 140, 40, "pr", ["b"]) + B(50, 172, 140, 40, "pr", ["c"]) +
  '<rect class="fill" x="420" y="20" width="320" height="210" rx="12"/>' + T(580, 44, "Heap (objects)", "b", "middle") +
  B(450, 70, 260, 60, "sp", ["list object", "[1, 2, 3]  refcount 2"]) + B(450, 160, 260, 50, "ka", ["int object", "42  refcount 1"]) +
  A("y1", "M190 84 L448 92") + A("y1", "M190 138 L448 110") + A("y1", "M190 192 L448 184") +
  T(320, 70, "a = [1, 2, 3]", "sm mono", "middle") + T(320, 134, "b = a", "sm mono", "middle") + T(320, 206, "c = 42", "sm mono", "middle")) },

aliasing:{ t:"Aliased rows vs independent rows", cap:"[[0] * 3] * 3 copies the reference to one inner list three times, so writing grid[0][0] changes every row. A comprehension evaluates [0] * 3 once per row and creates three independent lists.", svg:S("y2", 250, "Shared inner list versus separate inner lists",
  T(190, 26, "grid = [[0] * 3] * 3", "b mono", "middle") + T(570, 26, "grid = [[0] * 3 for _ in range(3)]", "b mono", "middle") +
  B(40, 50, 60, 36, "box", ["[0]"], false) + B(40, 100, 60, 36, "box", ["[1]"], false) + B(40, 150, 60, 36, "box", ["[2]"], false) +
  B(200, 100, 150, 36, "sp", ["[0, 0, 0]"]) +
  A("y2", "M100 68 L198 112") + A("y2", "M100 118 L198 118") + A("y2", "M100 168 L198 124") +
  T(190, 210, "one inner list, three references", "sm tR", "middle") +
  B(420, 50, 60, 36, "box", ["[0]"], false) + B(420, 100, 60, 36, "box", ["[1]"], false) + B(420, 150, 60, 36, "box", ["[2]"], false) +
  B(560, 50, 150, 36, "ka", ["[0, 0, 0]"]) + B(560, 100, 150, 36, "ka", ["[0, 0, 0]"]) + B(560, 150, 150, 36, "ka", ["[0, 0, 0]"]) +
  A("y2", "M480 68 L558 68") + A("y2", "M480 118 L558 118") + A("y2", "M480 168 L558 168") +
  T(570, 210, "three independent lists", "sm", "middle")) },

legb:{ t:"Name lookup: LEGB", cap:"Python resolves a name by searching the local scope, then each enclosing function scope, then the module's globals, then the built-ins. Whether a name is local is decided at compile time: any assignment to it inside a function makes it local throughout that function.", svg:S("y3", 270, "Local, enclosing, global and built-in scopes",
  '<rect class="pr" x="20" y="20" width="460" height="230" rx="14"/>' + T(36, 42, "Built-in: len, print, ValueError", "b") +
  '<rect class="ka" x="40" y="54" width="420" height="184" rx="12"/>' + T(56, 76, "Global (module): CONFIG, helper()", "b") +
  '<rect class="st" x="60" y="88" width="380" height="138" rx="10"/>' + T(76, 110, "Enclosing: def outer(): count = 0", "b") +
  '<rect class="sp" x="80" y="122" width="340" height="92" rx="8"/>' + T(96, 144, "Local: def inner(): x = 1", "b") + T(96, 168, "print(x, count, CONFIG, len)", "mono") + T(96, 192, "searched from the inside out", "sm") +
  T(520, 60, "Lookup order", "b") + T(520, 90, "1  Local", "") + T(520, 116, "2  Enclosing", "") + T(520, 142, "3  Global", "") + T(520, 168, "4  Built-in", "") + T(520, 200, "else NameError", "sm tR") +
  T(520, 236, "nonlocal / global change binding", "sm")) },

iterProto:{ t:"What a for loop really does", cap:"for x in obj calls iter(obj) once to get an iterator, then calls next() on it repeatedly. Each value is bound to x and the body runs; when next() raises StopIteration the loop ends quietly. Comprehensions, unpacking, sum() and list() all use this same protocol.", svg:S("y4", 220, "The iteration protocol",
  B(20, 60, 150, 60, "pr", ["iterable", "list, dict, file"]) + B(230, 60, 150, 60, "st", ["iterator", "has __next__"]) +
  B(440, 30, 140, 50, "ka", ["value", "bound to x"]) + B(440, 110, 140, 50, "sp", ["StopIteration", "loop ends"]) + B(620, 30, 120, 50, "box", ["loop body"]) +
  A("y4", "M170 90 L228 90") + T(199, 80, "iter()", "sm mono", "middle") +
  A("y4", "M380 80 L438 60") + T(400, 56, "next()", "sm mono", "middle") + A("y4", "M380 100 L438 130") + T(404, 140, "exhausted", "sm", "middle") +
  A("y4", "M580 55 L618 55") + '<path class="ln dash" d="M680 80 L680 190 L305 190 L305 122"/>' + A("y4", "M305 130 L305 122") + T(492, 206, "repeat: call next() again", "sm", "middle")) },

mro:{ t:"Diamond inheritance and the MRO", cap:"With class D(B, C) where B and C both inherit from A, C3 linearization produces D, B, C, A, object: children before parents, bases in the order written, and A only once. Cooperative super() calls follow this order, so each class runs exactly once.", svg:S("y5", 280, "Diamond class hierarchy with its method resolution order",
  B(170, 20, 120, 44, "pr", ["A"]) + B(60, 110, 120, 44, "st", ["B(A)"]) + B(280, 110, 120, 44, "st", ["C(A)"]) + B(170, 200, 120, 44, "sp", ["D(B, C)"]) +
  A("y5", "M120 110 L210 66") + A("y5", "M340 110 L250 66") + A("y5", "M210 200 L130 156") + A("y5", "M250 200 L330 156") +
  T(480, 50, "D.__mro__", "b") + B(480, 64, 50, 36, "sp", ["D"]) + B(540, 64, 50, 36, "st", ["B"]) + B(600, 64, 50, 36, "st", ["C"]) + B(660, 64, 50, 36, "pr", ["A"]) +
  T(480, 130, "super() in D calls B;", "sm") + T(480, 148, "super() in B calls C, not A;", "sm") + T(480, 166, "super() in C calls A.", "sm") +
  T(480, 200, "Rules (C3):", "b") + T(480, 220, "children before parents", "sm") + T(480, 238, "bases in written order", "sm") + T(480, 256, "each class appears once", "sm")) },

dunders:{ t:"Syntax maps to special methods", cap:"Operators and built-ins are thin layers over special methods looked up on the object's type. Implementing these methods is how your classes support +, len(), indexing, for loops, with blocks, calls and printing.", svg:S("y6", 270, "Python syntax and the special methods it calls",
  T(150, 26, "You write", "b", "middle") + T(560, 26, "Python calls", "b", "middle") +
  B(40, 40, 220, 30, "box", ["a + b"], false) + B(400, 40, 320, 30, "sp", ["type(a).__add__(a, b)  or  b.__radd__(a)"], false) +
  B(40, 78, 220, 30, "box", ["len(x)"], false) + B(400, 78, 320, 30, "sp", ["type(x).__len__(x)"], false) +
  B(40, 116, 220, 30, "box", ["x[k]   /   x[k] = v"], false) + B(400, 116, 320, 30, "sp", ["__getitem__(k)   /   __setitem__(k, v)"], false) +
  B(40, 154, 220, 30, "box", ["for item in x"], false) + B(400, 154, 320, 30, "sp", ["iter(x) → x.__iter__()"], false) +
  B(40, 192, 220, 30, "box", ["with m as v"], false) + B(400, 192, 320, 30, "sp", ["m.__enter__()  …  m.__exit__(...)"], false) +
  B(40, 230, 220, 30, "box", ["f(1)   /   print(x)"], false) + B(400, 230, 320, 30, "sp", ["f.__call__(1)   /   x.__str__()"], false) +
  A("y6", "M260 55 L398 55") + A("y6", "M260 93 L398 93") + A("y6", "M260 131 L398 131") + A("y6", "M260 169 L398 169") + A("y6", "M260 207 L398 207") + A("y6", "M260 245 L398 245")) },

decorator:{ t:"How a decorator wraps a function", cap:"@timer above def f means f = timer(f): the decorator runs once at definition time and returns a wrapper. Every later call goes to the wrapper, which runs code before and after calling the original function. functools.wraps copies the original's name, docstring and signature onto the wrapper.", svg:S("y7", 250, "Decorator wrapping and call flow",
  B(20, 30, 200, 70, "box", ["@timer", "def f(x): ...", "means f = timer(f)"]) +
  '<rect class="st" x="290" y="20" width="450" height="200" rx="14"/>' + T(306, 42, "wrapper (returned by timer, bound to the name f)", "b") +
  B(320, 60, 120, 50, "sp", ["before", "start = now()"]) + B(470, 60, 120, 50, "ka", ["original f", "result = f(x)"]) + B(620, 60, 100, 50, "sp", ["after", "log time"]) +
  A("y7", "M440 85 L468 85") + A("y7", "M590 85 L618 85") +
  B(320, 150, 400, 50, "pr", ["@functools.wraps(f): keeps __name__, __doc__,", "__wrapped__ and the signature"]) +
  A("y7", "M220 65 L288 65") + B(20, 150, 200, 50, "box", ["caller", "f(10)"]) + A("y7", "M220 170 L318 100") + T(150, 236, "decoration happens once, at import; wrapping cost is paid per call", "sm")) },

withFlow:{ t:"What a with statement does", cap:"with evaluates the manager, calls __enter__ and binds its return value, runs the block, then always calls __exit__ with the exception (or None). If __exit__ returns a true value the exception is suppressed; otherwise it propagates after cleanup.", svg:S("y8", 250, "Control flow of a with statement",
  B(20, 40, 150, 50, "box", ["with mgr as x:"]) + B(210, 40, 150, 50, "pr", ["mgr.__enter__()", "result bound to x"]) + B(400, 40, 150, 50, "ka", ["run the block"]) +
  A("y8", "M170 65 L208 65") + A("y8", "M360 65 L398 65") +
  B(590, 20, 150, 46, "okf", ["no exception", "__exit__(None, …)"]) + B(590, 100, 150, 46, "sp", ["exception raised", "__exit__(type, exc, tb)"]) +
  A("y8", "M550 55 L588 45") + A("y8", "M550 75 L588 118") +
  B(430, 180, 150, 46, "ka", ["returns True", "exception suppressed"]) + B(600, 180, 140, 46, "st", ["returns falsy", "exception re-raised"]) +
  A("y8", "M640 146 L540 178") + A("y8", "M680 146 L680 178") + T(200, 160, "__exit__ also runs on return, break and continue", "sm", "middle")) },

lookup:{ t:"Attribute lookup precedence", cap:"For obj.x, Python first checks the type's MRO for a data descriptor (one with __set__ or __delete__, such as property), then the instance __dict__, then non-data descriptors and plain class attributes (functions become bound methods here), then __getattr__ if defined, and finally raises AttributeError.", svg:S("y9", 270, "Order in which obj.x is resolved",
  B(20, 110, 120, 50, "box", ["obj.x"]) +
  B(190, 20, 330, 44, "sp", ["1  data descriptor on type(obj).__mro__", "property, __slots__ members, ORM fields"]) +
  B(190, 76, 330, 44, "pr", ["2  instance __dict__['x']", "ordinary attributes"]) +
  B(190, 132, 330, 44, "st", ["3  non-data descriptor or class attribute", "functions → bound methods, cached_property"]) +
  B(190, 188, 330, 44, "ka", ["4  type(obj).__getattr__(obj, 'x')", "only if defined"]) +
  B(570, 188, 170, 44, "box", ["AttributeError"]) +
  A("y9", "M140 120 L188 44") + A("y9", "M355 64 L355 74") + A("y9", "M355 120 L355 130") + A("y9", "M355 176 L355 186") + A("y9", "M520 210 L568 210") +
  T(560, 50, "first match wins", "b") + T(560, 72, "a data descriptor beats", "sm") + T(560, 90, "the instance dict;", "sm") + T(560, 108, "the instance dict beats", "sm") + T(560, 126, "a method of the same name", "sm")) },

classCreate:{ t:"How a class statement creates a class", cap:"Python resolves the bases and picks the metaclass, asks it for a namespace with __prepare__, executes the class body into that namespace, then calls the metaclass to build the class. Afterwards __set_name__ runs on descriptors and the parent's __init_subclass__ runs.", svg:S("y10", 230, "Steps of class creation",
  B(20, 30, 130, 56, "box", ["class C(Base,", "metaclass=M):"]) + B(180, 30, 130, 56, "pr", ["pick metaclass", "most derived"]) + B(340, 30, 130, 56, "pr", ["M.__prepare__", "namespace dict"]) + B(500, 30, 110, 56, "st", ["execute", "class body"]) + B(640, 30, 100, 56, "sp", ["M(name,", "bases, ns)"]) +
  A("y10", "M150 58 L178 58") + A("y10", "M310 58 L338 58") + A("y10", "M470 58 L498 58") + A("y10", "M610 58 L638 58") +
  B(380, 140, 170, 56, "ka", ["descriptor", "__set_name__(C, name)"]) + B(570, 140, 180, 56, "ka", ["Base.__init_subclass__", "registration hooks"]) +
  A("y10", "M690 86 L690 138") + A("y10", "M570 168 L552 168") + B(170, 140, 190, 56, "okf", ["class object C", "bound to the name C"]) + A("y10", "M380 168 L362 168")) },

importFlow:{ t:"What import does", cap:"import checks the sys.modules cache first. On a miss, finders on sys.meta_path locate the module and return a spec, the loader creates the module object, it is inserted into sys.modules before its code runs (which is why circular imports see half-initialized modules), and finally the name is bound in the importing namespace.", svg:S("y11", 250, "The import system flow",
  B(20, 40, 120, 50, "box", ["import pkg.mod"]) + B(180, 40, 150, 50, "pr", ["in sys.modules?", "cache lookup"]) + B(180, 150, 150, 50, "okf", ["yes: reuse", "module object"]) +
  B(370, 40, 170, 50, "st", ["finders on", "sys.meta_path → spec"]) + B(580, 40, 160, 50, "st", ["loader creates", "module object"]) +
  B(580, 120, 160, 50, "sp", ["insert into", "sys.modules"]) + B(580, 190, 160, 50, "sp", ["execute module", "code (once)"]) + B(370, 190, 170, 50, "ka", ["bind name in the", "importing namespace"]) +
  A("y11", "M140 65 L178 65") + A("y11", "M255 90 L255 148") + T(265, 124, "hit", "sm") + A("y11", "M330 65 L368 65") + T(349, 56, "miss", "sm", "middle") + A("y11", "M540 65 L578 65") +
  A("y11", "M660 90 L660 118") + A("y11", "M660 170 L660 188") + A("y11", "M580 215 L542 215") + A("y11", "M330 175 L368 205", "ln dash")) },

variance:{ t:"Variance in type hints", cap:"A read-only container of Dog can be used where a read-only container of Animal is expected (covariant), but a mutable list cannot, because the receiver could append a Cat (invariant). Callables are contravariant in their parameters: a function that accepts any Animal can stand in for one that needs only Dogs.", svg:S("y12", 260, "Covariance, invariance and contravariance",
  B(30, 20, 160, 40, "pr", ["Animal"]) + B(30, 100, 160, 40, "sp", ["Dog"]) + A("y12", "M110 100 L110 62") + T(120, 86, "is a", "sm") +
  B(250, 20, 220, 40, "pr", ["Sequence[Animal]"]) + B(250, 100, 220, 40, "sp", ["Sequence[Dog]"]) + A("y12", "M360 100 L360 62") + T(370, 86, "ok: covariant", "sm tS") +
  B(520, 20, 220, 40, "pr", ["list[Animal]"]) + B(520, 100, 220, 40, "sp", ["list[Dog]"]) + '<path class="bad" d="M620 98 L620 64"/><path class="bad" d="M608 72 L632 90 M632 72 L608 90"/>' + T(640, 86, "invariant", "sm tR") +
  B(130, 180, 260, 40, "pr", ["Callable[[Animal], None]"]) + B(450, 180, 260, 40, "sp", ["Callable[[Dog], None]"]) + A("y12", "M390 200 L448 200") +
  T(420, 240, "a handler for any Animal works where a Dog handler is needed: contravariant", "sm", "middle")) }
});

/* figs3: Parts II and III (internals, concurrency, performance) */
Object.assign(FIGS, {

pipeline:{ t:"From source text to running bytecode", cap:"The tokenizer splits text into tokens, the PEG parser builds an abstract syntax tree, the symbol table decides each name's scope, and the compiler produces an optimized code object. The interpreter then executes its bytecode, specializing hot instructions; very hot traces can be turned into micro-ops and, with the experimental JIT, machine code.", svg:S("z1", 250, "CPython compilation and execution pipeline",
  B(20, 30, 110, 56, "box", ["source", ".py text"]) + B(150, 30, 110, 56, "pr", ["tokenizer", "tokens"]) + B(280, 30, 110, 56, "pr", ["PEG parser", "AST"]) + B(410, 30, 110, 56, "pr", ["symtable", "scopes"]) + B(540, 30, 200, 56, "st", ["compiler + optimizer", "code object (.pyc cache)"]) +
  A("z1", "M130 58 L148 58") + A("z1", "M260 58 L278 58") + A("z1", "M390 58 L408 58") + A("z1", "M520 58 L538 58") +
  B(540, 130, 200, 56, "sp", ["evaluation loop", "bytecode, frames"]) + A("z1", "M640 86 L640 128") +
  B(290, 130, 220, 56, "sp", ["specializing interpreter", "inline caches (3.11+)"]) + A("z1", "M540 158 L512 158") +
  B(40, 130, 220, 56, "ka", ["tier 2 micro-ops (3.13+)", "optional JIT (PEP 744)"]) + A("z1", "M290 158 L262 158", "ln dash") +
  T(380, 226, "hot code is specialized; very hot traces may be optimized further", "sm", "middle")) },

refcount:{ t:"Reference counting and cycles", cap:"Every new reference increments an object's count and every dropped reference decrements it; at zero the object is freed immediately. Two objects that refer to each other keep each other's count above zero even after the program drops them, so the cyclic garbage collector periodically finds and frees such unreachable groups.", svg:S("z2", 250, "Reference counts and an unreachable cycle",
  T(190, 26, "freed by reference counting", "b", "middle") + T(570, 26, "needs the cycle collector", "b", "middle") +
  B(40, 50, 100, 40, "pr", ["name x"]) + B(200, 50, 140, 40, "sp", ["list  rc=1"]) + A("z2", "M140 70 L198 70") +
  T(190, 130, "del x  →  rc 0  →  freed at once", "sm mono", "middle") +
  B(420, 50, 140, 40, "sp", ["Node a  rc=1"]) + B(600, 50, 140, 40, "sp", ["Node b  rc=1"]) +
  A("z2", "M560 62 L598 62") + A("z2", "M598 80 L562 80") +
  T(580, 130, "no outside references, yet rc ≠ 0", "sm tR", "middle") +
  B(420, 160, 320, 56, "ka", ["gc: subtract internal references;", "what remains unreachable is freed"]) +
  B(40, 160, 300, 56, "box", ["immortal objects (3.12+): None, True,", "small ints: refcount never changes"])) },

dictLayout:{ t:"Inside a compact dict", cap:"A dict keeps a small sparse index table and a dense, insertion-ordered array of entries. Hashing a key selects a slot in the index (probing further on collisions); the slot holds the position of the entry. Iteration walks the dense entries, which is why dicts preserve insertion order and use less memory than older designs.", svg:S("z3", 260, "Index table and entries array of a dict",
  T(150, 26, "indices (sparse)", "b", "middle") + T(520, 26, "entries (dense, insertion order)", "b", "middle") +
  B(40, 40, 50, 30, "box", ["–"], false) + B(95, 40, 50, 30, "pr", ["1"], false) + B(150, 40, 50, 30, "box", ["–"], false) + B(205, 40, 50, 30, "pr", ["0"], false) +
  B(40, 75, 50, 30, "box", ["–"], false) + B(95, 75, 50, 30, "pr", ["2"], false) + B(150, 75, 50, 30, "box", ["–"], false) + B(205, 75, 50, 30, "box", ["–"], false) +
  B(330, 40, 380, 32, "sp", ["0: hash('a') | 'a' | 1"], false) + B(330, 80, 380, 32, "sp", ["1: hash('b') | 'b' | 2"], false) + B(330, 120, 380, 32, "sp", ["2: hash('c') | 'c' | 3"], false) +
  A("z3", "M230 55 L328 56") + A("z3", "M120 55 L328 96") + A("z3", "M120 90 L328 136") +
  T(150, 140, "slot = hash & mask;", "sm mono", "middle") + T(150, 158, "on collision, probe with", "sm", "middle") + T(150, 176, "perturbed hash bits", "sm", "middle") +
  T(520, 182, "resize when about two thirds full", "sm", "middle") + T(520, 202, "instances can share keys (PEP 412)", "sm", "middle") + T(380, 240, "d = {'a': 1, 'b': 2, 'c': 3}", "b mono", "middle")) },

gilTimeline:{ t:"Threads with and without the GIL", cap:"On a regular build, threads take turns holding the GIL, so two CPU-bound threads run one at a time (but I/O waits release it). On the free-threaded build (officially supported since 3.14) both threads execute Python bytecode at the same time on different cores.", svg:S("z4", 230, "Thread execution with the GIL versus free-threaded",
  T(20, 30, "GIL build", "b") + T(120, 56, "thread 1", "sm", "end") + T(120, 96, "thread 2", "sm", "end") +
  '<rect class="sp" x="130" y="42" width="90" height="22" rx="4"/><rect class="sp" x="310" y="42" width="90" height="22" rx="4"/><rect class="sp" x="490" y="42" width="90" height="22" rx="4"/>' +
  '<rect class="st" x="220" y="82" width="90" height="22" rx="4"/><rect class="st" x="400" y="82" width="90" height="22" rx="4"/><rect class="st" x="580" y="82" width="90" height="22" rx="4"/>' +
  T(700, 76, "one at a time", "sm tR", "middle") +
  T(20, 140, "free-threaded (3.13t, 3.14t)", "b") + T(120, 166, "thread 1", "sm", "end") + T(120, 206, "thread 2", "sm", "end") +
  '<rect class="sp" x="130" y="152" width="540" height="22" rx="4"/><rect class="st" x="130" y="192" width="540" height="22" rx="4"/>' + T(700, 186, "in parallel", "sm tS", "middle")) },

concModels:{ t:"Choosing a concurrency model", cap:"Waiting on I/O: use asyncio for many connections or threads for blocking libraries. Burning CPU in Python code: use processes, subinterpreters or a free-threaded build. Heavy numeric work: vectorize with NumPy or compile, which also releases the GIL.", svg:S("z5", 260, "Decision guide for concurrency in Python",
  B(280, 16, 200, 46, "box", ["What is the bottleneck?"]) +
  B(40, 100, 220, 50, "ka", ["waiting on I/O", "network, disk, APIs"]) + B(500, 100, 220, 50, "sp", ["CPU in Python code", "loops, parsing"]) +
  A("z5", "M330 62 L200 98") + A("z5", "M430 62 L560 98") +
  B(20, 190, 130, 50, "ka", ["asyncio", "1000s of sockets"]) + B(160, 190, 130, 50, "ka", ["threads", "blocking libraries"]) +
  B(330, 190, 120, 50, "sp", ["processes", "multiprocessing"]) + B(460, 190, 130, 50, "sp", ["subinterpreters", "3.14 executor"]) + B(600, 190, 140, 50, "sp", ["free-threaded", "python3.14t"]) +
  A("z5", "M110 150 L90 188") + A("z5", "M190 150 L220 188") + A("z5", "M560 150 L390 188") + A("z5", "M600 150 L525 188") + A("z5", "M650 150 L670 188")) },

eventLoop:{ t:"How the asyncio event loop runs tasks", cap:"The loop runs one ready task until it reaches an await on something not yet complete; the task is parked and the loop runs the next ready task. When the operating system reports that a socket is ready or a timer fires, the waiting task becomes ready again. Everything runs on one thread, so a blocking call stalls every task.", svg:S("z6", 260, "Ready queue, running task and waiting tasks",
  B(20, 40, 170, 110, "pr", ["ready queue", "task A", "task C", "callbacks"]) +
  B(290, 40, 180, 110, "sp", ["running", "task B", "until it awaits"]) +
  B(560, 40, 180, 110, "st", ["waiting", "socket reads, timers,", "other futures"]) +
  A("z6", "M190 80 L288 80") + T(240, 70, "pick next", "sm", "middle") +
  A("z6", "M470 80 L558 80") + T(515, 70, "await", "sm", "middle") +
  '<path class="ln" d="M650 150 L650 200 L105 200"/>' + A("z6", "M105 200 L105 152") +
  B(230, 174, 300, 48, "ka", ["OS reports a socket is ready", "epoll / kqueue / IOCP"]) +
  T(380, 248, "one thread: never call blocking functions inside a coroutine", "sm tR", "middle")) },

optLadder:{ t:"The optimization ladder", cap:"Measure first, then work up the ladder only as far as you need: better algorithms and data structures give the biggest wins, then built-ins and vectorization, then compiling hot spots, then parallelism. Each step costs more complexity than the one before.", svg:S("z7", 270, "Ordered list of optimization strategies",
  B(40, 20, 680, 34, "okf", ["0  profile: find where time actually goes (cProfile, py-spy, Tachyon in 3.15)"], false) +
  B(80, 62, 640, 34, "pr", ["1  algorithm and data structure: O(n²) → O(n log n), list → set, re-sort → heap"], false) +
  B(120, 104, 600, 34, "pr", ["2  built-ins and C-backed libraries: sum, sorted, Counter, str.join"], false) +
  B(160, 146, 560, 34, "ka", ["3  vectorize: NumPy, Polars, DuckDB"], false) +
  B(200, 188, 520, 34, "st", ["4  compile the hot spot: Numba, Cython, mypyc, Rust"], false) +
  B(240, 230, 480, 34, "sp", ["5  parallelize: processes, subinterpreters, free-threading"], false)) }
});

/* figs4: Parts IV and V (engineering practice, applied Python) */
Object.assign(FIGS, {

pyramid:{ t:"The test pyramid", cap:"Most tests should be fast, isolated unit tests; fewer should exercise real integrations such as a database; only a handful should drive the whole system end to end. Moving up the pyramid, tests get slower, more brittle and more expensive to debug, but closer to what users experience.", svg:S("x1", 260, "Unit, integration and end-to-end test proportions",
  '<path class="okf" d="M380 20 L470 90 L290 90 Z"/>' + T(380, 78, "end to end", "b", "middle") +
  '<path class="pr" d="M285 96 L475 96 L560 166 L200 166 Z"/>' + T(380, 140, "integration", "b", "middle") +
  '<path class="sp" d="M195 172 L565 172 L650 242 L110 242 Z"/>' + T(380, 214, "unit tests", "b", "middle") +
  T(690, 60, "few, slow", "sm", "middle") + T(690, 76, "realistic", "sm", "middle") + T(690, 214, "many, fast", "sm", "middle") + T(690, 230, "isolated", "sm", "middle") +
  T(70, 60, "minutes", "sm", "middle") + T(70, 140, "seconds", "sm", "middle") + T(70, 214, "milliseconds", "sm", "middle")) },

hexagonal:{ t:"Ports and adapters (hexagonal architecture)", cap:"The domain core contains business rules and depends only on ports, interfaces it defines for what it needs. Adapters implement those ports for real infrastructure (a PostgreSQL repository, a Stripe client) or for tests (in-memory fakes), and driving adapters (HTTP handlers, CLI, consumers) call the core through a service layer.", svg:S("x2", 290, "Domain core surrounded by ports and adapters",
  '<path class="fill" d="M380 30 L560 90 L560 200 L380 260 L200 200 L200 90 Z"/>' +
  B(300, 110, 160, 70, "sp", ["domain core", "entities, rules,", "use cases"]) +
  T(380, 74, "ports (interfaces)", "sm", "middle") + T(380, 226, "service layer", "sm", "middle") +
  B(20, 50, 150, 44, "pr", ["HTTP API", "FastAPI handlers"]) + B(20, 120, 150, 44, "pr", ["CLI / jobs"]) + B(20, 190, 150, 44, "pr", ["queue consumer"]) +
  B(590, 50, 150, 44, "ka", ["PostgreSQL", "repository"]) + B(590, 120, 150, 44, "ka", ["payment gateway", "client"]) + B(590, 190, 150, 44, "okf", ["in-memory fakes", "for tests"]) +
  A("x2", "M170 72 L218 110") + A("x2", "M170 142 L218 145") + A("x2", "M170 212 L218 180") +
  A("x2", "M588 72 L542 110") + A("x2", "M588 142 L542 145") + A("x2", "M588 212 L542 180", "ln dash") +
  T(95, 276, "driving adapters call in", "sm", "middle") + T(665, 276, "driven adapters implement ports", "sm", "middle")) },

webStack:{ t:"A production Python web stack", cap:"A reverse proxy or load balancer terminates TLS, serves static files and spreads requests over several application server processes. Each process (Gunicorn for WSGI, Uvicorn or Granian for ASGI) runs your framework code, which talks to the database through a connection pool, to Redis for caching, and hands slow work to background workers through a queue.", svg:S("x3", 250, "Request path through proxy, app servers, database, cache and workers",
  B(20, 100, 100, 50, "box", ["clients"]) + B(150, 100, 120, 50, "pr", ["nginx / LB", "TLS, static"]) +
  B(300, 30, 170, 50, "sp", ["worker process 1", "Uvicorn + FastAPI"]) + B(300, 100, 170, 50, "sp", ["worker process 2"]) + B(300, 170, 170, 50, "sp", ["worker process N"]) +
  B(520, 20, 220, 46, "ka", ["PostgreSQL", "via connection pool"]) + B(520, 80, 220, 46, "ka", ["Redis", "cache, rate limits"]) + B(520, 140, 220, 46, "st", ["task queue", "Celery / RQ"]) + B(520, 196, 220, 46, "st", ["background workers"]) +
  A("x3", "M120 125 L148 125") + A("x3", "M270 115 L298 60") + A("x3", "M270 125 L298 125") + A("x3", "M270 135 L298 190") +
  A("x3", "M470 55 L518 43") + A("x3", "M470 120 L518 103") + A("x3", "M470 190 L518 163") + A("x3", "M630 186 L630 194")) },

broadcast:{ t:"NumPy broadcasting", cap:"When two arrays have different shapes, NumPy compares dimensions from the right: equal sizes match, and a size of 1 is stretched (virtually, without copying) to match the other. Subtracting a row of column means from a matrix centres every column in one operation.", svg:S("x4", 230, "Broadcasting a 1 by 3 row across a 3 by 3 matrix",
  T(120, 26, "matrix (3, 3)", "b", "middle") + T(370, 26, "row (1, 3) → stretched", "b", "middle") + T(620, 26, "result (3, 3)", "b", "middle") +
  B(40, 40, 50, 34, "sp", ["1"], false) + B(95, 40, 50, 34, "sp", ["2"], false) + B(150, 40, 50, 34, "sp", ["3"], false) +
  B(40, 80, 50, 34, "sp", ["4"], false) + B(95, 80, 50, 34, "sp", ["5"], false) + B(150, 80, 50, 34, "sp", ["6"], false) +
  B(40, 120, 50, 34, "sp", ["7"], false) + B(95, 120, 50, 34, "sp", ["8"], false) + B(150, 120, 50, 34, "sp", ["9"], false) +
  T(230, 102, "-", "b", "middle") +
  B(290, 40, 50, 34, "pr", ["4"], false) + B(345, 40, 50, 34, "pr", ["5"], false) + B(400, 40, 50, 34, "pr", ["6"], false) +
  '<g opacity="0.45">' + B(290, 80, 50, 34, "pr", ["4"], false) + B(345, 80, 50, 34, "pr", ["5"], false) + B(400, 80, 50, 34, "pr", ["6"], false) +
  B(290, 120, 50, 34, "pr", ["4"], false) + B(345, 120, 50, 34, "pr", ["5"], false) + B(400, 120, 50, 34, "pr", ["6"], false) + '</g>' +
  T(490, 102, "=", "b", "middle") +
  B(540, 40, 50, 34, "ka", ["-3"], false) + B(595, 40, 50, 34, "ka", ["-3"], false) + B(650, 40, 50, 34, "ka", ["-3"], false) +
  B(540, 80, 50, 34, "ka", ["0"], false) + B(595, 80, 50, 34, "ka", ["0"], false) + B(650, 80, 50, 34, "ka", ["0"], false) +
  B(540, 120, 50, 34, "ka", ["3"], false) + B(595, 120, 50, 34, "ka", ["3"], false) + B(650, 120, 50, 34, "ka", ["3"], false) +
  T(380, 196, "m - m.mean(axis=0): shapes (3, 3) and (3,) align from the right", "sm mono", "middle")) }
});

/* figs5: Parts VI and VII (data structures, algorithms, patterns) */
function cells(x, y, vals, cls, w, h){ var s = ""; vals.forEach(function(v, i){ s += B(x + i * (w || 44), y, (w || 44) - 4, h || 34, (cls && cls[i]) || "box", [String(v)], false); }); return s; }
Object.assign(FIGS, {

growth:{ t:"How running time grows", cap:"Rough operation counts for an input of size n = 1,000. Constants matter for small inputs, but growth rate decides everything at scale: O(n log n) stays around ten thousand steps while O(n²) reaches a million and O(2ⁿ) is beyond astronomical.", svg:S("w1", 270, "Growth of common complexity classes",
  T(20, 30, "complexity", "b") + T(200, 30, "n = 1,000", "b") + T(420, 30, "typical example", "b") +
  B(20, 44, 160, 28, "okf", ["O(1)"], false) + T(200, 63, "1", "mono") + T(420, 63, "dict lookup, list index", "sm") +
  B(20, 78, 160, 28, "okf", ["O(log n)"], false) + T(200, 97, "10", "mono") + T(420, 97, "binary search, heap push", "sm") +
  B(20, 112, 160, 28, "ka", ["O(n)"], false) + T(200, 131, "1,000", "mono") + T(420, 131, "one pass, sum, max", "sm") +
  B(20, 146, 160, 28, "ka", ["O(n log n)"], false) + T(200, 165, "10,000", "mono") + T(420, 165, "sorting, heap of n items", "sm") +
  B(20, 180, 160, 28, "sp", ["O(n²)"], false) + T(200, 199, "1,000,000", "mono") + T(420, 199, "nested loops over pairs", "sm") +
  B(20, 214, 160, 28, "sp", ["O(2ⁿ)"], false) + T(200, 233, "≈ 10³⁰¹", "mono tR") + T(420, 233, "all subsets, naive recursion", "sm") +
  '<rect class="okf" x="300" y="52" width="2" height="14"/><rect class="okf" x="300" y="86" width="10" height="14"/><rect class="ka" x="300" y="120" width="40" height="14"/><rect class="ka" x="300" y="154" width="70" height="14"/><rect class="sp" x="300" y="188" width="110" height="14"/>') },

prefix:{ t:"Prefix sums turn range sums into O(1)", cap:"prefix[i] holds the sum of the first i elements, with prefix[0] = 0. The sum of nums[l..r] is prefix[r + 1] - prefix[l], so after one O(n) pass every range query is a single subtraction. Storing counts of prefix sums in a hash map also counts subarrays with a given sum.", svg:S("w2", 220, "Array and its prefix-sum array",
  T(30, 30, "nums", "b") + cells(110, 14, [3, 1, 4, 1, 5, 9], ["box","pr","pr","pr","box","box"]) +
  T(30, 92, "prefix", "b") + cells(110, 76, [0, 3, 4, 8, 9, 14, 23], ["box","pr","box","box","sp","box","box"]) +
  T(130, 136, "index 0 … 6", "sm") +
  T(30, 176, "sum(nums[1..3]) = prefix[4] - prefix[1] = 9 - 3 = 6", "mono") + T(30, 202, "(1 + 4 + 1)", "sm")) },

linkedRev:{ t:"Reversing a linked list", cap:"Walk the list with three pointers. At each step, save next, point the current node back at prev, then advance prev and curr. When curr becomes None, prev is the new head. O(n) time and O(1) extra space.", svg:S("w3", 250, "Pointer moves while reversing a singly linked list",
  T(20, 30, "before", "b") + B(100, 14, 60, 34, "pr", ["1"]) + B(200, 14, 60, 34, "pr", ["2"]) + B(300, 14, 60, 34, "pr", ["3"]) + B(400, 14, 60, 34, "pr", ["4"]) + T(500, 36, "None", "mono") +
  A("w3", "M160 31 L198 31") + A("w3", "M260 31 L298 31") + A("w3", "M360 31 L398 31") + A("w3", "M460 31 L496 31") +
  T(20, 110, "middle", "b") + B(100, 94, 60, 34, "sp", ["1"]) + B(200, 94, 60, 34, "sp", ["2"]) + B(300, 94, 60, 34, "pr", ["3"]) + B(400, 94, 60, 34, "pr", ["4"]) +
  A("w3", "M198 111 L162 111") + A("w3", "M360 111 L398 111") + T(130, 150, "prev", "sm tA", "middle") + T(330, 150, "curr", "sm tP", "middle") + T(430, 150, "nxt", "sm", "middle") +
  T(20, 205, "after", "b") + B(100, 189, 60, 34, "sp", ["1"]) + B(200, 189, 60, 34, "sp", ["2"]) + B(300, 189, 60, 34, "sp", ["3"]) + B(400, 189, 60, 34, "sp", ["4"]) +
  A("w3", "M198 206 L162 206") + A("w3", "M298 206 L262 206") + A("w3", "M398 206 L362 206") + T(70, 210, "None", "mono", "end") + A("w3", "M98 206 L74 206") + T(430, 244, "new head", "sm tA", "middle")) },

monoStack:{ t:"Monotonic stack: next greater element", cap:"Scan left to right keeping a stack of indices whose values decrease from bottom to top. When a larger value arrives, pop every smaller index: the new value is their next greater element. Each index is pushed and popped once, so the whole scan is O(n).", svg:S("w4", 240, "Stack states while finding next greater elements",
  T(20, 30, "nums", "b") + cells(90, 14, [2, 1, 5, 3, 6], ["box","box","sp","box","sp"], 60) +
  T(20, 90, "at 5:", "b") + T(90, 90, "stack [2, 1] → pop 1 (next greater 5), pop 2 (next greater 5) → push 5", "sm mono") +
  T(20, 120, "at 3:", "b") + T(90, 120, "stack [5] → 3 < 5, push → [5, 3]", "sm mono") +
  T(20, 150, "at 6:", "b") + T(90, 150, "pop 3 (6), pop 5 (6) → push 6", "sm mono") +
  T(20, 196, "answer", "b") + cells(90, 180, [5, 5, 6, 6, -1], ["okf","okf","okf","okf","box"], 60)) },

backtrack:{ t:"Backtracking as a tree of choices", cap:"Each level makes one choice (include or skip the next element here). Walking down builds a candidate; reaching a leaf records a complete solution; returning up undoes the last choice so the next branch starts clean. Pruning cuts subtrees that cannot lead to a valid answer.", svg:S("w5", 270, "Decision tree for subsets of [1, 2, 3]",
  B(320, 14, 120, 32, "box", ["[ ]"]) +
  B(150, 74, 120, 32, "sp", ["[1]"]) + B(490, 74, 120, 32, "pr", ["[ ]"]) +
  B(60, 134, 110, 32, "sp", ["[1,2]"]) + B(240, 134, 110, 32, "pr", ["[1]"]) + B(410, 134, 110, 32, "sp", ["[2]"]) + B(590, 134, 110, 32, "pr", ["[ ]"]) +
  A("w5", "M360 46 L230 72") + A("w5", "M400 46 L530 72") + A("w5", "M190 106 L120 132") + A("w5", "M230 106 L290 132") + A("w5", "M530 106 L470 132") + A("w5", "M570 106 L640 132") +
  T(20, 200, "level 3 leaves:", "b") + T(160, 200, "[1,2,3] [1,2] [1,3] [1] [2,3] [2] [3] [ ]", "mono") +
  T(20, 236, "orange = include, blue = skip; 2ⁿ leaves; undo the choice when returning", "sm")) },

binSearch:{ t:"Binary search on a sorted array", cap:"Keep a range [lo, hi) that must contain the answer. Compare the middle element with the target and discard the half that cannot contain it. Each step halves the range, so a million elements need about 20 comparisons.", svg:S("w6", 230, "Binary search narrowing the range",
  cells(40, 20, [1, 3, 5, 7, 9, 11, 13, 15, 17], ["pr","pr","pr","pr","sp","pr","pr","pr","pr"], 70) + T(20, 80, "step 1: mid = 9 < 13 → search right half", "sm") +
  cells(40, 100, [1, 3, 5, 7, 9, 11, 13, 15, 17], ["box","box","box","box","box","pr","sp","pr","pr"], 70) + T(20, 160, "step 2: mid = 15 > 13 → go left; step 3: mid = 13 → found at index 6", "sm") +
  T(20, 200, "lo, hi = 0, len(a)  ·  while lo < hi: mid = (lo + hi) // 2", "mono")) },

traversals:{ t:"Tree traversal orders", cap:"Depth-first orders differ only in when the node itself is visited: before its children (preorder), between them (inorder, which yields sorted order in a binary search tree) or after them (postorder). Breadth-first (level order) visits the tree level by level using a queue.", svg:S("w7", 260, "A binary search tree with its traversal orders",
  B(150, 14, 60, 34, "sp", ["8"]) + B(70, 84, 60, 34, "sp", ["3"]) + B(230, 84, 60, 34, "sp", ["10"]) + B(20, 154, 60, 34, "sp", ["1"]) + B(120, 154, 60, 34, "sp", ["6"]) + B(280, 154, 60, 34, "sp", ["14"]) +
  '<g class="ln"><path d="M165 48 L110 84"/><path d="M195 48 L250 84"/><path d="M85 118 L55 154"/><path d="M115 118 L145 154"/><path d="M270 118 L305 154"/></g>' +
  T(400, 50, "preorder", "b") + T(520, 50, "8 3 1 6 10 14", "mono") +
  T(400, 90, "inorder", "b") + T(520, 90, "1 3 6 8 10 14", "mono tS") +
  T(400, 130, "postorder", "b") + T(520, 130, "1 6 3 14 10 8", "mono") +
  T(400, 170, "level order", "b") + T(520, 170, "8 | 3 10 | 1 6 14", "mono") +
  T(400, 220, "inorder of a BST is sorted", "sm tS")) },

heapArray:{ t:"A binary heap stored in an array", cap:"A min-heap is a complete binary tree where every parent is at most its children, stored level by level in a list. For index i, the children are at 2i + 1 and 2i + 2 and the parent at (i - 1) // 2, so no pointers are needed and the minimum is always heap[0].", svg:S("w8", 260, "Heap as a tree and as a list",
  B(170, 14, 50, 32, "sp", ["1"]) + B(90, 74, 50, 32, "pr", ["3"]) + B(250, 74, 50, 32, "pr", ["2"]) + B(40, 134, 50, 32, "box", ["7"]) + B(130, 134, 50, 32, "box", ["4"]) + B(210, 134, 50, 32, "box", ["5"]) + B(300, 134, 50, 32, "box", ["9"]) +
  '<g class="ln"><path d="M185 46 L125 74"/><path d="M205 46 L265 74"/><path d="M105 106 L70 134"/><path d="M125 106 L150 134"/><path d="M265 106 L235 134"/><path d="M285 106 L320 134"/></g>' +
  T(420, 40, "heap list", "b") + cells(420, 54, [1, 3, 2, 7, 4, 5, 9], ["sp","pr","pr","box","box","box","box"], 44) +
  T(420, 112, "index", "sm") + T(442, 112, "0", "sm mono") + T(486, 112, "1", "sm mono") + T(530, 112, "2", "sm mono") + T(574, 112, "3", "sm mono") + T(618, 112, "4", "sm mono") + T(662, 112, "5", "sm mono") + T(706, 112, "6", "sm mono") +
  T(420, 160, "children of i: 2i + 1, 2i + 2", "mono") + T(420, 186, "parent of i: (i - 1) // 2", "mono") + T(420, 220, "push and pop: O(log n), heapify: O(n)", "sm")) },

bfsDfs:{ t:"BFS and DFS visit orders", cap:"Breadth-first search uses a queue and visits nodes in order of distance from the start, so it finds shortest paths in unweighted graphs. Depth-first search uses a stack (or recursion) and follows one branch as far as possible before backtracking, which suits cycle detection, topological sort and exhaustive search.", svg:S("w9", 260, "A small graph with BFS and DFS orders",
  B(40, 100, 50, 34, "sp", ["A"]) + B(150, 40, 50, 34, "pr", ["B"]) + B(150, 160, 50, 34, "pr", ["C"]) + B(270, 40, 50, 34, "ka", ["D"]) + B(270, 160, 50, 34, "ka", ["E"]) + B(380, 100, 50, 34, "st", ["F"]) +
  '<g class="ln"><path d="M90 110 L150 62"/><path d="M90 124 L150 172"/><path d="M200 57 L270 57"/><path d="M200 177 L270 177"/><path d="M175 74 L175 160"/><path d="M320 62 L380 110"/><path d="M320 172 L380 124"/></g>' +
  T(470, 70, "BFS (queue):", "b") + T(470, 94, "A  B C  D E  F", "mono") + T(470, 114, "level by level", "sm") +
  T(470, 160, "DFS (stack):", "b") + T(470, 184, "A B D F E C", "mono") + T(470, 204, "one branch to the end", "sm") +
  T(20, 240, "both O(V + E); mark nodes visited when they are first discovered", "sm")) },

unionFind:{ t:"Union-find with path compression", cap:"Each set is a tree whose root names the set. find(x) follows parent pointers to the root and then points every node on the way directly at the root (path compression); union attaches the smaller tree under the larger root. Together they make operations nearly O(1) amortized.", svg:S("w10", 240, "Disjoint-set trees before and after path compression",
  T(150, 26, "before find(4)", "b", "middle") + T(560, 26, "after find(4)", "b", "middle") +
  B(125, 40, 50, 32, "sp", ["1"]) + B(125, 100, 50, 32, "pr", ["2"]) + B(125, 160, 50, 32, "pr", ["3"]) + B(225, 160, 50, 32, "pr", ["4"]) +
  A("w10", "M150 100 L150 74") + A("w10", "M150 160 L150 134") + A("w10", "M240 160 L170 134") +
  B(535, 40, 50, 32, "sp", ["1"]) + B(445, 120, 50, 32, "pr", ["2"]) + B(535, 120, 50, 32, "pr", ["3"]) + B(625, 120, 50, 32, "pr", ["4"]) +
  A("w10", "M470 120 L545 74") + A("w10", "M560 120 L560 74") + A("w10", "M650 120 L575 74") +
  T(380, 222, "every node on the path now points straight at the root", "sm", "middle")) },

dpTable:{ t:"Filling a DP table: longest common subsequence", cap:"dp[i][j] is the LCS length of the first i characters of one string and the first j of the other. Matching characters extend the diagonal value by one; otherwise take the larger of the cell above and the cell to the left. The bottom-right cell is the answer, and walking back from it recovers the subsequence.", svg:S("w11", 270, "LCS table for ABCB and BDCB",
  T(170, 30, "\"\"", "mono", "middle") + T(214, 30, "B", "b", "middle") + T(258, 30, "D", "b", "middle") + T(302, 30, "C", "b", "middle") + T(346, 30, "B", "b", "middle") +
  T(110, 62, "\"\"", "mono", "middle") + T(110, 102, "A", "b", "middle") + T(110, 142, "B", "b", "middle") + T(110, 182, "C", "b", "middle") + T(110, 222, "B", "b", "middle") +
  cells(150, 44, [0, 0, 0, 0, 0]) + cells(150, 84, [0, 0, 0, 0, 0]) + cells(150, 124, [0, 1, 1, 1, 1], ["box","sp","box","box","box"]) +
  cells(150, 164, [0, 1, 1, 2, 2], ["box","box","box","sp","box"]) + cells(150, 204, [0, 1, 1, 2, 3], ["box","box","box","box","okf"]) +
  T(420, 70, "match:", "b") + T(420, 92, "dp[i][j] = dp[i-1][j-1] + 1", "mono") + T(420, 130, "no match:", "b") + T(420, 152, "max(dp[i-1][j], dp[i][j-1])", "mono") +
  T(420, 196, "answer dp[4][4] = 3", "b tS") + T(420, 218, "LCS = \"BCB\"", "mono")) },

kmp:{ t:"KMP prefix function", cap:"For each position i of the pattern, pi[i] is the length of the longest proper prefix that is also a suffix of pattern[:i + 1]. On a mismatch, KMP shifts the pattern so this border lines up, never moving backwards in the text, which gives O(n + m) matching.", svg:S("w12", 220, "Prefix function for pattern ABABCABAB",
  T(20, 36, "pattern", "b") + cells(120, 18, ["A","B","A","B","C","A","B","A","B"], ["pr","pr","pr","pr","sp","pr","pr","pr","pr"], 52) +
  T(20, 92, "pi", "b") + cells(120, 74, [0, 0, 1, 2, 0, 1, 2, 3, 4], ["box","box","ka","ka","box","ka","ka","ka","okf"], 52) +
  T(20, 150, "pi[8] = 4: \"ABAB\" is both a prefix and a suffix of \"ABABCABAB\"", "sm mono") +
  T(20, 186, "matching all occurrences: prefix function of pattern + '#' + text", "sm")) },

slidingWindow:{ t:"Variable-size sliding window", cap:"Expand the right edge one step at a time and update the window's state; while the window violates the constraint (here, a repeated character), shrink from the left. Each index enters and leaves the window at most once, so the scan is O(n).", svg:S("w13", 220, "Window over a string while finding the longest substring without repeats",
  cells(40, 30, ["a","b","c","a","b","c","b","b"], ["box","sp","sp","sp","box","box","box","box"], 60) +
  T(128, 90, "left", "sm tA", "middle") + T(248, 90, "right", "sm tA", "middle") +
  '<rect class="acc" x="96" y="24" width="184" height="46" rx="8"/>' +
  T(40, 130, "window \"bca\": no repeats, length 3", "mono") +
  T(40, 160, "add next 'b' → repeat → move left past the earlier 'b'", "sm") +
  T(40, 196, "for right in range(n): add s[right]; while invalid: remove s[left]; left += 1", "sm mono")) },

patternMap:{ t:"From problem clues to patterns", cap:"Keywords and constraints in a problem statement usually point to a small set of techniques. Use this as a starting checklist, then confirm with the constraints: n up to 20 allows exponential search, n up to 10⁵ needs O(n log n) or better.", svg:S("w14", 300, "Mapping problem clues to algorithmic patterns",
  B(20, 14, 280, 32, "box", ["sorted input, pairs, triplets"], false) + B(330, 14, 410, 32, "pr", ["two pointers, binary search"], false) +
  B(20, 52, 280, 32, "box", ["longest or shortest substring/subarray"], false) + B(330, 52, 410, 32, "pr", ["sliding window, prefix sums + hash map"], false) +
  B(20, 90, 280, 32, "box", ["top k, kth largest, merge k"], false) + B(330, 90, 410, 32, "ka", ["heap, quickselect"], false) +
  B(20, 128, 280, 32, "box", ["next greater, spans, histograms"], false) + B(330, 128, 410, 32, "ka", ["monotonic stack or queue"], false) +
  B(20, 166, 280, 32, "box", ["shortest steps, grids, levels"], false) + B(330, 166, 410, 32, "st", ["BFS (multi-source), Dijkstra"], false) +
  B(20, 204, 280, 32, "box", ["dependencies, ordering"], false) + B(330, 204, 410, 32, "st", ["topological sort, union-find"], false) +
  B(20, 242, 280, 32, "box", ["count ways, max/min with choices"], false) + B(330, 242, 410, 32, "sp", ["dynamic programming; all solutions → backtracking"], false)) }
});

/* figs6: diagrams for the in-depth topic articles, Part I */
Object.assign(FIGS, {

bindRebind:{ t:"Mutating vs rebinding", cap:"b.append(4) changes the one list both names share, so a sees it. b = [9] creates a new list and moves only the name b; a still refers to the original list.", svg:S("x1", 240, "Mutation through an alias versus rebinding a name",
  T(190, 26, "b.append(4): mutate", "b", "middle") + T(570, 26, "b = [9]: rebind", "b", "middle") +
  B(40, 50, 90, 40, "pr", ["a"]) + B(40, 120, 90, 40, "pr", ["b"]) + B(200, 80, 150, 50, "sp", ["[1, 2, 3, 4]"]) +
  A("x1", "M130 70 L198 98") + A("x1", "M130 140 L198 114") +
  T(190, 200, "one object, both names see the change", "sm", "middle") +
  B(420, 50, 90, 40, "pr", ["a"]) + B(420, 120, 90, 40, "pr", ["b"]) + B(580, 50, 150, 40, "sp", ["[1, 2, 3, 4]"]) + B(580, 120, 150, 40, "ka", ["[9]  (new)"]) +
  A("x1", "M510 70 L578 70") + A("x1", "M510 140 L578 140") +
  T(570, 200, "a is untouched; only b moved", "sm", "middle")) },

unicode:{ t:"Text, code points and bytes", cap:"A str is a sequence of code points. Encoding turns it into bytes for files and networks; UTF-8 uses one byte for ASCII and two to four for other characters. Decode bytes as soon as they arrive, work with str, and encode at the last moment.", svg:S("x2", 250, "Encoding and decoding between str and UTF-8 bytes",
  B(30, 40, 200, 60, "sp", ["str  'Aé😀'", "3 code points"]) +
  B(530, 40, 200, 60, "ka", ["bytes (UTF-8)", "7 bytes"]) +
  A("x2", "M230 60 L528 60") + T(380, 50, ".encode('utf-8')", "sm mono", "middle") +
  A("x2", "M528 86 L232 86") + T(380, 104, ".decode('utf-8')", "sm mono", "middle") +
  B(30, 140, 110, 40, "pr", ["A  U+0041"], false) + B(150, 140, 110, 40, "pr", ["é  U+00E9"], false) + B(270, 140, 120, 40, "pr", ["😀  U+1F600"], false) +
  B(430, 140, 60, 40, "box", ["41"], false) + B(500, 140, 90, 40, "box", ["C3 A9"], false) + B(600, 140, 140, 40, "box", ["F0 9F 98 80"], false) +
  T(210, 210, "1 byte, 2 bytes, 4 bytes in UTF-8", "sm", "middle") + T(585, 210, "decode early, encode late", "sm tS", "middle")) },

copies:{ t:"Shallow copy vs deep copy", cap:"A shallow copy is a new outer container holding the same inner objects, so mutating an inner list shows through both. deepcopy copies recursively, using a memo so shared objects and cycles are copied once.", svg:S("x3", 250, "Shallow and deep copies of a nested list",
  B(30, 40, 150, 40, "pr", ["original"]) + B(30, 150, 150, 40, "pr", ["shallow copy"]) +
  B(260, 95, 110, 36, "sp", ["[1, 2]"]) + B(260, 140, 110, 36, "sp", ["[3, 4]"]) +
  A("x3", "M180 60 L258 108") + A("x3", "M180 66 L258 152") + A("x3", "M180 164 L258 118") + A("x3", "M180 172 L258 160") +
  T(200, 230, "two outer lists, shared inner lists", "sm tR", "middle") +
  B(430, 40, 130, 40, "pr", ["original"]) + B(430, 150, 130, 40, "ka", ["deepcopy"]) +
  B(610, 30, 110, 30, "sp", ["[1, 2]"]) + B(610, 66, 110, 30, "sp", ["[3, 4]"]) +
  B(610, 140, 110, 30, "ka", ["[1, 2]"]) + B(610, 176, 110, 30, "ka", ["[3, 4]"]) +
  A("x3", "M560 54 L608 46") + A("x3", "M560 66 L608 80") + A("x3", "M560 164 L608 156") + A("x3", "M560 176 L608 190") +
  T(590, 230, "everything copied, fully independent", "sm tS", "middle")) },

closureCells:{ t:"How a closure keeps its variables", cap:"When make_multiplier(2) runs, factor lives in a cell object instead of an ordinary local slot. The returned function stores a reference to that cell in __closure__, so the value survives after make_multiplier returns. Each call creates a fresh cell, so double and triple are independent.", svg:S("x4", 250, "Closure functions referring to cell objects",
  B(30, 30, 220, 60, "box", ["make_multiplier(2)", "frame (finished)"]) + B(30, 150, 220, 60, "box", ["make_multiplier(3)", "frame (finished)"]) +
  B(320, 40, 150, 44, "st", ["cell: factor=2"]) + B(320, 160, 150, 44, "st", ["cell: factor=3"]) +
  '<path class="ln dash" d="M250 60 L318 62"/><path class="ln dash" d="M250 180 L318 182"/>' +
  B(560, 40, 170, 44, "sp", ["double", "__closure__"]) + B(560, 160, 170, 44, "sp", ["triple", "__closure__"]) +
  A("x4", "M560 62 L472 62") + A("x4", "M560 182 L472 182") +
  T(380, 120, "cells outlive the frame that created them", "sm", "middle") + T(380, 236, "the cell holds the variable, not a snapshot", "sm tR", "middle")) },

callStack:{ t:"The call stack during recursion", cap:"Each call pushes a frame holding its own n and its position in the code. fact(1) hits the base case and returns; each waiting frame then finishes its multiplication and pops, passing its result back to its caller.", svg:S("x5", 260, "Stack frames for fact(4)",
  B(40, 20, 250, 40, "pr", ["fact(n=4)  waits for 4 * fact(3)"], false) + B(70, 70, 250, 40, "pr", ["fact(n=3)  waits for 3 * fact(2)"], false) +
  B(100, 120, 250, 40, "pr", ["fact(n=2)  waits for 2 * fact(1)"], false) + B(130, 170, 250, 40, "okf", ["fact(n=1)  base case: return 1"], false) +
  A("x5", "M290 40 L300 70", "ln dash") + A("x5", "M320 90 L330 120", "ln dash") + A("x5", "M350 140 L360 170", "ln dash") +
  T(560, 40, "calls push frames", "b", "middle") + T(560, 70, "returns pop them:", "sm", "middle") +
  T(560, 110, "fact(1) → 1", "mono", "middle") + T(560, 135, "fact(2) → 2", "mono", "middle") + T(560, 160, "fact(3) → 6", "mono", "middle") + T(560, 185, "fact(4) → 24", "mono", "middle") +
  T(380, 245, "depth n means n frames: O(n) space; the default limit is about 1000", "sm tR", "middle")) },

eagerLazy:{ t:"List comprehension vs generator expression", cap:"A list comprehension computes every value up front and stores them all. A generator expression stores only its position and computes the next value when the consumer asks, so memory stays constant and consumers such as any() can stop early.", svg:S("x6", 230, "Eager list versus lazy generator",
  T(190, 26, "[x * x for x in data]", "b mono", "middle") + T(570, 26, "(x * x for x in data)", "b mono", "middle") +
  B(40, 50, 60, 36, "sp", ["0"], false) + B(105, 50, 60, 36, "sp", ["1"], false) + B(170, 50, 60, 36, "sp", ["4"], false) + B(235, 50, 60, 36, "sp", ["9"], false) + B(300, 50, 50, 36, "sp", ["…"], false) +
  T(190, 116, "all n values in memory before use", "sm tR", "middle") +
  B(430, 50, 120, 50, "ka", ["generator", "position: x=2"]) + A("x6", "M550 75 L608 75") + B(610, 50, 120, 50, "pr", ["consumer", "sum, any, for"]) +
  T(580, 116, "one value at a time, on request", "sm tS", "middle") +
  B(40, 150, 310, 50, "box", ["reusable, has len(), indexable"]) + B(420, 150, 310, 50, "box", ["single pass, no len(), can stop early"])) },

genStates:{ t:"Life of a generator", cap:"Calling a generator function only creates the generator. Each next() or send() runs the body until the next yield, where it suspends with its frame intact. Returning, falling off the end or close() finishes it; after that next() raises StopIteration.", svg:S("x7", 240, "Generator states and transitions",
  B(20, 80, 140, 56, "box", ["CREATED", "body not started"]) + B(240, 80, 140, 56, "pr", ["RUNNING", "executing body"]) +
  B(460, 80, 140, 56, "st", ["SUSPENDED", "paused at yield"]) + B(640, 80, 110, 56, "ka", ["CLOSED", "StopIteration"]) +
  A("x7", "M160 100 L238 100") + T(200, 90, "next()", "sm mono", "middle") +
  A("x7", "M380 100 L458 100") + T(420, 90, "yield v", "sm mono", "middle") +
  '<path class="ln" d="M530 136 L530 180 L310 180"/>' + A("x7", "M310 180 L310 138") + T(420, 198, "next() / send(x): resume after yield", "sm mono", "middle") +
  '<path class="ln" d="M310 80 L310 40 L695 40"/>' + A("x7", "M695 40 L695 78") + T(500, 32, "return, end of body, or close()", "sm", "middle")) },

genPipeline:{ t:"A lazy generator pipeline", cap:"The consumer at the end pulls one item; the request travels back through each stage to the source, and a single record flows forward through parse and filter. Only a few records exist at any moment, so memory does not depend on the file size.", svg:S("x8", 200, "Stages of a generator pipeline",
  B(20, 60, 130, 60, "box", ["read_lines", "gzip files"]) + B(185, 60, 120, 60, "pr", ["parse", "csv → dict"]) +
  B(340, 60, 130, 60, "pr", ["errors_only", "status ≥ 500"]) + B(505, 60, 100, 60, "pr", ["bytes", "r['bytes']"]) + B(640, 60, 110, 60, "sp", ["sum()", "consumer"]) +
  A("x8", "M150 80 L183 80") + A("x8", "M305 80 L338 80") + A("x8", "M470 80 L503 80") + A("x8", "M605 80 L638 80") +
  A("x8", "M638 108 L607 108", "ln dash") + A("x8", "M503 108 L472 108", "ln dash") + A("x8", "M338 108 L307 108", "ln dash") + A("x8", "M183 108 L152 108", "ln dash") +
  T(380, 40, "records flow forward one at a time →", "sm", "middle") + T(380, 160, "← each next() request flows backward", "sm", "middle")) },

c3Merge:{ t:"Computing an MRO with C3", cap:"For class D(B, C), merge the parents' linearizations plus the list of bases. At each step take the first head that does not appear in the tail of any list. A is skipped while it is still in the tail of [C, A, object], so it lands after both B and C.", svg:S("x9", 270, "Step-by-step C3 merge for a diamond",
  T(20, 28, "merge( [B, A, object], [C, A, object], [B, C] )", "b mono") +
  B(20, 46, 180, 34, "okf", ["take B"], false) + T(215, 68, "B is a head and in no tail", "sm") +
  B(20, 90, 180, 34, "bad", ["A? no, skip"], false) + T(215, 112, "A is in the tail of [C, A, object]", "sm") +
  B(20, 134, 180, 34, "okf", ["take C"], false) + T(215, 156, "next head that is in no tail", "sm") +
  B(20, 178, 180, 34, "okf", ["take A, then object"], false) + T(215, 200, "now A is free; object last", "sm") +
  B(480, 70, 250, 120, "sp", ["D.__mro__", "D → B → C → A → object", "children before parents,", "base order preserved"]) +
  T(380, 250, "if no head is ever free, class creation raises TypeError", "sm tR", "middle")) },

attrLookup:{ t:"Where __getattribute__ and __getattr__ fit", cap:"Every read of obj.name goes through type(obj).__getattribute__, which performs the normal search. Only if that search raises AttributeError does Python call __getattr__, so __getattr__ is a cheap fallback while __getattribute__ affects every access.", svg:S("x10", 250, "Attribute read path through the two hooks",
  B(20, 90, 120, 50, "box", ["obj.name"]) + A("x10", "M140 115 L178 115") +
  B(180, 70, 260, 90, "sp", ["__getattribute__  (every access)", "1 data descriptor on the type", "2 instance __dict__", "3 class attribute / method"]) +
  A("x10", "M440 95 L498 70") + B(500, 40, 230, 46, "okf", ["found: return the value"]) +
  A("x10", "M440 135 L498 160") + B(500, 140, 230, 46, "st", ["AttributeError raised"]) +
  A("x10", "M615 186 L615 204") + B(470, 206, 290, 36, "ka", ["__getattr__(name) if defined (fallback)"], false) +
  T(180, 200, "dunder lookups for operators skip both hooks", "sm tR")) },

decoWrap:{ t:"What @decorator does to a name", cap:"At definition time the decorator receives the original function and returns a wrapper; the module-level name now points at the wrapper. The original survives only inside the wrapper's closure (and as __wrapped__ when functools.wraps is used). Every call goes through the wrapper first.", svg:S("x11", 230, "Name rebinding performed by a decorator",
  T(20, 30, "definition time: add = announce(add)", "b mono") +
  B(20, 60, 110, 44, "pr", ["name: add"]) + B(220, 50, 200, 64, "sp", ["wrapper", "closure: func ="]) + B(520, 50, 210, 64, "ka", ["original add(a, b)", "__wrapped__ target"]) +
  A("x11", "M130 82 L218 82") + A("x11", "M420 82 L518 82") +
  T(20, 160, "call time: add(2, 3)", "b mono") +
  B(20, 180, 220, 36, "box", ["before: log the call"], false) + B(270, 180, 210, 36, "sp", ["result = func(2, 3)"], false) + B(510, 180, 220, 36, "box", ["after: log, return result"], false) +
  A("x11", "M240 198 L268 198") + A("x11", "M480 198 L508 198")) },

decoStack:{ t:"Stacked decorators", cap:"Decorators are applied from the bottom up, so the decorator nearest the def wraps the function first and the top decorator produces the outermost wrapper. On each call the wrappers run from the outside in, and results return from the inside out.", svg:S("x12", 250, "Nested wrappers created by three decorators",
  '<rect class="pr" x="20" y="20" width="420" height="210" rx="14"/>' + T(36, 44, "@route   (applied last: outermost)", "b") +
  '<rect class="ka" x="44" y="58" width="372" height="160" rx="12"/>' + T(60, 82, "@login_required", "b") +
  '<rect class="st" x="68" y="96" width="324" height="110" rx="10"/>' + T(84, 120, "@cache   (applied first)", "b") +
  '<rect class="sp" x="92" y="134" width="276" height="58" rx="8"/>' + T(230, 168, "def report(): ...", "mono", "middle") +
  T(480, 60, "f = route(login_required(", "mono") + T(480, 84, "      cache(report)))", "mono") +
  T(480, 130, "call: route → login →", "sm") + T(480, 150, "cache → report", "sm") + T(480, 196, "swap two lines and caching", "sm tR") + T(480, 214, "runs before the auth check", "sm tR")) },

cachedProp:{ t:"How cached_property caches", cap:"On the first access the instance dict has no entry, so lookup reaches the non-data descriptor, which computes the value and stores it in the instance dict under the same name. From then on the instance dict wins over the non-data descriptor, so the method is never called again until the entry is deleted.", svg:S("x13", 240, "First and later accesses of a cached_property",
  T(190, 26, "first access: r.summary", "b mono", "middle") + T(570, 26, "later accesses", "b mono", "middle") +
  B(40, 50, 300, 40, "box", ["1  r.__dict__ has no 'summary'"], false) +
  B(40, 100, 300, 40, "sp", ["2  descriptor __get__ computes value"], false) +
  B(40, 150, 300, 40, "ka", ["3  r.__dict__['summary'] = value"], false) +
  A("x13", "M190 90 L190 98") + A("x13", "M190 140 L190 148") +
  B(420, 50, 300, 40, "okf", ["r.__dict__['summary'] found first"], false) +
  B(420, 100, 300, 40, "box", ["descriptor not consulted (non-data)"], false) +
  B(420, 150, 300, 40, "st", ["del r.summary → recompute next time"], false) +
  T(380, 222, "speed after the first access = a plain dict lookup", "sm tS", "middle")) },

typeObject:{ t:"Instances, classes and type", cap:"Solid arrows mean 'is an instance of' and dashed arrows mean 'is a subclass of'. An instance's type is its class, a class's type is its metaclass (type by default), and type is an instance of itself. Every class, including type, ultimately subclasses object.", svg:S("x14", 264, "Instance-of and subclass-of relationships",
  B(40, 170, 140, 46, "pr", ["d = Dog()"]) + B(300, 170, 140, 46, "sp", ["class Dog"]) + B(560, 170, 140, 46, "ka", ["type"]) +
  B(300, 40, 140, 46, "box", ["object"]) +
  A("x14", "M180 193 L298 193") + T(240, 184, "instance of", "sm", "middle") +
  A("x14", "M440 193 L558 193") + T(500, 184, "instance of", "sm", "middle") +
  '<path class="ln" d="M610 216 L610 238 L680 238 L680 216"/>' + A("x14", "M680 222 L680 218") + T(645, 254, "type(type) is type", "sm", "middle") +
  A("x14", "M370 170 L370 88", "ln dash") + T(380, 130, "subclass of", "sm") +
  A("x14", "M600 170 L440 70", "ln dash") + T(540, 110, "subclass of", "sm") +
  A("x14", "M370 86 L560 175", "ln") ) },

astTree:{ t:"The AST of total = price * (1 + tax_rate)", cap:"The parser turns source text into a tree of nodes. Statements such as Assign hold expression nodes; BinOp nodes hold an operator and two operands; names carry a context saying whether they are read (Load) or written (Store). Formatting and comments are not part of the tree.", svg:S("x15", 260, "Abstract syntax tree for an assignment",
  B(300, 14, 160, 36, "box", ["Assign"]) +
  B(120, 80, 180, 36, "pr", ["Name total (Store)"], false) + B(460, 80, 160, 36, "sp", ["BinOp  Mult"]) +
  B(330, 150, 170, 36, "pr", ["Name price (Load)"], false) + B(560, 150, 160, 36, "sp", ["BinOp  Add"]) +
  B(440, 214, 120, 34, "ka", ["Constant 1"], false) + B(580, 214, 170, 34, "pr", ["Name tax_rate (Load)"], false) +
  A("x15", "M350 50 L230 78") + A("x15", "M410 50 L530 78") + A("x15", "M510 116 L420 148") + A("x15", "M560 116 L630 148") +
  A("x15", "M610 186 L510 212") + A("x15", "M660 186 L665 212") +
  T(20, 30, "targets / value", "sm") + T(20, 50, "left / op / right", "sm")) },

circularImport:{ t:"Why a circular import fails", cap:"Importing a puts an empty module object into sys.modules and starts running a.py. Its first line imports b, so b.py runs; b asks for CONFIG from a, finds a already in sys.modules but only half-executed, and the name does not exist yet.", svg:S("x16", 250, "Sequence of a circular import between two modules",
  B(20, 20, 200, 44, "pr", ["import a"]) + A("x16", "M120 64 L120 86") +
  B(20, 88, 200, 50, "sp", ["sys.modules['a'] = <empty a>", "run a.py, line 1"]) + A("x16", "M220 113 L278 113") +
  B(280, 88, 200, 50, "sp", ["from b import helper", "run b.py, line 1"]) + A("x16", "M480 113 L538 113") +
  B(540, 88, 200, 50, "bad", ["from a import CONFIG", "a found, CONFIG missing"]) +
  B(540, 170, 200, 44, "box", ["ImportError: partially", "initialized module 'a'"]) + A("x16", "M640 138 L640 168") +
  T(20, 180, "fixes: shared third module, import a + a.CONFIG", "sm") + T(20, 200, "at call time, defer the import, TYPE_CHECKING,", "sm") + T(20, 220, "or a lazy import (3.15)", "sm")) },

validateEdges:{ t:"Validate at the edges, type-check inside", cap:"Untrusted data is parsed into typed models once, where it enters the system. Inside the boundary, functions pass validated objects to each other and a static type checker guards correctness, so there is no need to re-validate on every call.", svg:S("x17", 250, "Runtime validation at trust boundaries around statically checked code",
  B(20, 30, 150, 40, "box", ["HTTP request"], false) + B(20, 80, 150, 40, "box", ["CSV / JSON file"], false) + B(20, 130, 150, 40, "box", ["queue message"], false) + B(20, 180, 150, 40, "box", ["env / config"], false) +
  B(220, 60, 120, 130, "ka", ["parse +", "validate", "(Pydantic)"]) +
  A("x17", "M170 50 L218 90") + A("x17", "M170 100 L218 110") + A("x17", "M170 150 L218 140") + A("x17", "M170 200 L218 165") +
  '<rect class="sp" x="390" y="30" width="350" height="190" rx="14"/>' + T(565, 54, "typed core: static checking (mypy, pyright)", "b", "middle") +
  B(410, 80, 140, 40, "pr", ["Order"]) + B(580, 80, 140, 40, "pr", ["pricing()"]) + B(410, 150, 140, 40, "pr", ["repository"]) + B(580, 150, 140, 40, "pr", ["report()"]) +
  A("x17", "M340 125 L408 100") + A("x17", "M550 100 L578 100") + A("x17", "M480 120 L480 148") + A("x17", "M650 120 L650 148")) },

ioLayers:{ t:"The layers behind open()", cap:"Text mode stacks a TextIOWrapper (decoding, newline handling) on a buffered stream (fewer system calls) on a raw file (the operating system file descriptor). Binary mode skips the text layer, and buffering=0 in binary mode talks to the raw layer directly.", svg:S("x18", 250, "Text, buffered and raw I/O layers",
  T(150, 26, "open('notes.txt', 'r')", "b mono", "middle") + T(400, 26, "open('a.jpg', 'rb')", "b mono", "middle") + T(630, 26, "buffering=0", "b mono", "middle") +
  B(40, 44, 220, 50, "sp", ["TextIOWrapper", "str ⇄ bytes, newlines"]) +
  B(40, 108, 220, 50, "pr", ["BufferedReader", "in-memory buffer"]) + B(290, 108, 220, 50, "pr", ["BufferedReader", "in-memory buffer"]) +
  B(40, 172, 220, 50, "ka", ["FileIO (raw)", "read()/write() syscalls"]) + B(290, 172, 220, 50, "ka", ["FileIO (raw)", "syscalls"]) + B(540, 172, 190, 50, "ka", ["FileIO (raw)", "every call is a syscall"]) +
  A("x18", "M150 94 L150 106") + A("x18", "M150 158 L150 170") + A("x18", "M400 158 L400 170") +
  T(630, 120, "unbuffered: fine for big", "sm", "middle") + T(630, 140, "reads, slow for tiny ones", "sm", "middle")) },

loggingFlow:{ t:"How a log record travels", cap:"A module logger creates a record if the level passes, then the record propagates up the logger hierarchy to the root. Each logger's handlers, with their own levels and filters, format the record and send it to a destination. Applications configure handlers once at the top; libraries only create loggers.", svg:S("x19", 250, "Logger hierarchy, handlers and formatters",
  B(20, 30, 200, 44, "pr", ["shop.billing logger"]) + A("x19", "M120 74 L120 98") +
  B(20, 100, 200, 44, "pr", ["shop logger"]) + A("x19", "M120 144 L120 168") + T(130, 90, "propagate", "sm") +
  B(20, 170, 200, 44, "pr", ["root logger"]) +
  A("x19", "M220 192 L298 120") + A("x19", "M220 192 L298 192") +
  B(300, 96, 170, 48, "sp", ["StreamHandler", "level INFO"]) + B(300, 168, 170, 48, "sp", ["RotatingFileHandler", "level DEBUG"]) +
  A("x19", "M470 120 L528 120") + A("x19", "M470 192 L528 192") +
  B(530, 96, 200, 48, "ka", ["Formatter → console"]) + B(530, 168, 200, 48, "ka", ["JSON formatter → app.log"]) +
  T(240, 30, "log.info('charged %s', id)", "sm mono") + T(240, 50, "level check happens first", "sm")) }
});

/* figs7: diagrams for the in-depth topic articles, Parts II and III */
Object.assign(FIGS, {

valueStack:{ t:"Evaluating a * (b + 1) on the value stack", cap:"Each instruction pops its operands from the frame's value stack and pushes its result. The compiler emits instructions in post-order (operands before operators), so no instruction needs to name where its inputs are.", svg:S("w1", 230, "Value stack contents after each instruction",
  B(20, 20, 130, 34, "pr", ["LOAD_FAST a"], false) + B(165, 20, 130, 34, "pr", ["LOAD_FAST b"], false) + B(310, 20, 130, 34, "pr", ["LOAD_CONST 1"], false) + B(455, 20, 130, 34, "sp", ["BINARY_OP +"], false) + B(600, 20, 140, 34, "sp", ["BINARY_OP *"], false) +
  B(45, 160, 80, 30, "box", ["a"], false) +
  B(190, 160, 80, 30, "box", ["a"], false) + B(190, 126, 80, 30, "box", ["b"], false) +
  B(335, 160, 80, 30, "box", ["a"], false) + B(335, 126, 80, 30, "box", ["b"], false) + B(335, 92, 80, 30, "box", ["1"], false) +
  B(480, 160, 80, 30, "box", ["a"], false) + B(480, 126, 80, 30, "ka", ["b+1"], false) +
  B(615, 160, 110, 30, "ka", ["a*(b+1)"], false) +
  T(380, 216, "bottom of the stack is at the bottom; each column is the state after that instruction", "sm", "middle")) },

specialize:{ t:"Specialization with guards and fallback", cap:"A generic instruction counts executions, then rewrites itself into a version specialized for the types it has seen, using an inline cache next to it. The specialized version first checks cheap guards; if they fail, it deoptimizes back to the generic instruction, which may specialize again later.", svg:S("w2", 240, "Adaptive instruction life cycle",
  B(20, 90, 190, 60, "box", ["BINARY_OP +", "generic, counts runs"]) +
  A("w2", "M210 105 L328 72") + T(270, 70, "warm: saw int + int", "sm", "middle") +
  B(330, 40, 210, 60, "sp", ["BINARY_OP_ADD_INT", "+ inline cache"]) +
  A("w2", "M540 70 L598 70") + B(600, 40, 140, 60, "okf", ["guards pass:", "fast path"]) +
  A("w2", "M435 100 L330 140") + T(400, 140, "guard fails (float!)", "sm tR", "middle") +
  B(20, 170, 190, 50, "st", ["deoptimize: run", "generic, retry later"]) + A("w2", "M330 145 L212 188") +
  T(520, 200, "type-stable code stays on the fast path", "sm tS", "middle")) },

pyObjectLayout:{ t:"Object headers in memory", cap:"Every object starts with a reference count and a type pointer, so any object can be handled through a generic PyObject pointer. Variable-size objects add a length. A list stores pointers to separate item objects, which is why a list of numbers costs far more memory than an array of raw values.", svg:S("w3", 250, "Memory layout of a float, a tuple and a list",
  T(110, 24, "float 1.5", "b mono", "middle") + B(40, 36, 140, 30, "pr", ["ob_refcnt"], false) + B(40, 66, 140, 30, "pr", ["ob_type → float"], false) + B(40, 96, 140, 30, "ka", ["ob_fval = 1.5"], false) +
  T(330, 24, "tuple (x, y)", "b mono", "middle") + B(260, 36, 140, 30, "pr", ["ob_refcnt"], false) + B(260, 66, 140, 30, "pr", ["ob_type → tuple"], false) + B(260, 96, 140, 30, "st", ["ob_size = 2"], false) + B(260, 126, 140, 30, "ka", ["ob_item[0], [1]"], false) +
  T(560, 24, "list", "b mono", "middle") + B(490, 36, 140, 30, "pr", ["ob_refcnt"], false) + B(490, 66, 140, 30, "pr", ["ob_type → list"], false) + B(490, 96, 140, 30, "st", ["ob_size, allocated"], false) + B(490, 126, 140, 30, "ka", ["ob_item ──→"], false) +
  B(650, 120, 90, 90, "sp", ["ptr", "ptr", "ptr"]) + A("w3", "M630 141 L648 141") +
  T(380, 230, "common header: refcount + type pointer (free-threaded builds add thread fields)", "sm", "middle")) },

pymalloc:{ t:"pymalloc: arenas, pools and blocks", cap:"Small allocations are served from arenas obtained from the operating system. Each arena is divided into pools, and each pool hands out blocks of a single size class. An arena can be returned to the operating system only when every pool in it is empty.", svg:S("w4", 240, "Arena containing pools of fixed-size blocks",
  '<rect class="fill" x="20" y="20" width="720" height="150" rx="12"/>' + T(36, 42, "arena (1 MiB, from mmap)", "b") +
  B(40, 56, 160, 96, "pr", ["pool: 32-byte", "blocks"]) + B(215, 56, 160, 96, "pr", ["pool: 48-byte", "blocks"]) + B(390, 56, 160, 96, "pr", ["pool: 48-byte", "blocks"]) + B(565, 56, 160, 96, "box", ["empty pool", "any size class"]) +
  '<rect class="ka" x="225" y="120" width="30" height="20"/><rect class="ka" x="260" y="120" width="30" height="20"/><rect class="box" x="295" y="120" width="30" height="20"/><rect class="ka" x="330" y="120" width="30" height="20"/>' +
  T(380, 196, "allocation = pop a free block from a pool of the right size class", "sm", "middle") +
  T(380, 220, "objects > 512 bytes go straight to the system allocator", "sm tR", "middle")) },

listGrowth:{ t:"How a list grows", cap:"A list keeps spare capacity. Appends fill free slots in O(1); when the array is full, a larger array (about 12.5% bigger plus a constant) is allocated and the pointers are copied. Because growth is geometric, the copies add up to O(n) over n appends, so append is amortized O(1).", svg:S("w5", 230, "List capacity before and after a resize",
  T(20, 30, "len 4, capacity 4: full", "b") +
  B(20, 44, 70, 34, "sp", ["p0"], false) + B(95, 44, 70, 34, "sp", ["p1"], false) + B(170, 44, 70, 34, "sp", ["p2"], false) + B(245, 44, 70, 34, "sp", ["p3"], false) +
  A("w5", "M330 61 L398 61") + T(365, 50, "append", "sm", "middle") +
  T(400, 30, "new array, capacity 8: copy 4, add 1", "b") +
  B(400, 44, 40, 34, "sp", ["p0"], false) + B(444, 44, 40, 34, "sp", ["p1"], false) + B(488, 44, 40, 34, "sp", ["p2"], false) + B(532, 44, 40, 34, "sp", ["p3"], false) + B(576, 44, 40, 34, "ka", ["p4"], false) +
  B(620, 44, 30, 34, "box", [""], false) + B(654, 44, 30, 34, "box", [""], false) + B(688, 44, 30, 34, "box", [""], false) +
  T(20, 120, "capacities: 0 → 4 → 8 → 16 → 24 → 32 → 40 → 52 → 64 → 76 → ...", "mono") +
  T(20, 160, "next three appends: O(1) each, no copying", "sm tS") +
  T(20, 190, "insert(0, x) / pop(0): shift every pointer, O(n)", "sm tR")) },

dequeBlocks:{ t:"Inside a deque", cap:"A deque is a doubly linked list of blocks of 64 slots. Appending or popping at either end touches only the end block, allocating or freeing a block occasionally, so both ends are O(1). Reaching the middle means walking blocks.", svg:S("w6", 220, "Linked blocks with free space at both ends",
  B(30, 60, 200, 60, "pr", ["block (64 slots)", "left part free"]) + B(280, 60, 200, 60, "sp", ["block", "full"]) + B(530, 60, 200, 60, "pr", ["block", "right part free"]) +
  A("w6", "M230 82 L278 82") + A("w6", "M278 100 L232 100") + A("w6", "M480 82 L528 82") + A("w6", "M528 100 L482 100") +
  T(130, 44, "leftindex", "sm mono", "middle") + T(630, 44, "rightindex", "sm mono", "middle") +
  T(130, 150, "appendleft / popleft: O(1)", "sm tS", "middle") + T(630, 150, "append / pop: O(1)", "sm tS", "middle") +
  T(380, 190, "d[i] in the middle walks blocks: O(n)", "sm tR", "middle")) },

gilIoCpu:{ t:"Threads under the GIL: I/O vs CPU", cap:"I/O-bound threads release the GIL while they wait, so their waits overlap and total time shrinks. CPU-bound threads need the GIL the whole time, so they take turns and finish no sooner than running one after another.", svg:S("w7", 250, "Timelines of I/O-bound and CPU-bound threads",
  T(20, 28, "I/O-bound: waits overlap", "b") + T(110, 54, "thread 1", "sm", "end") + T(110, 88, "thread 2", "sm", "end") +
  '<rect class="sp" x="120" y="42" width="30" height="20" rx="3"/><rect class="box" x="150" y="42" width="220" height="20" rx="3"/><rect class="sp" x="370" y="42" width="30" height="20" rx="3"/>' +
  '<rect class="st" x="150" y="76" width="30" height="20" rx="3"/><rect class="box" x="180" y="76" width="220" height="20" rx="3"/><rect class="st" x="400" y="76" width="30" height="20" rx="3"/>' +
  T(560, 70, "waiting on the network,", "sm", "middle") + T(560, 88, "GIL released", "sm tS", "middle") +
  T(20, 138, "CPU-bound: one at a time", "b") + T(110, 164, "thread 1", "sm", "end") + T(110, 198, "thread 2", "sm", "end") +
  '<rect class="sp" x="120" y="152" width="80" height="20" rx="3"/><rect class="sp" x="280" y="152" width="80" height="20" rx="3"/><rect class="sp" x="440" y="152" width="80" height="20" rx="3"/>' +
  '<rect class="st" x="200" y="186" width="80" height="20" rx="3"/><rect class="st" x="360" y="186" width="80" height="20" rx="3"/><rect class="st" x="520" y="186" width="80" height="20" rx="3"/>' +
  T(680, 182, "no speedup", "sm tR", "middle") + T(380, 236, "shaded = running Python code (holding the GIL); outline = waiting with the GIL released", "sm", "middle")) },

concParallel:{ t:"Concurrency vs parallelism", cap:"Concurrency interleaves tasks on one core so their lifetimes overlap, switching while a task waits. Parallelism runs tasks at the same instant on different cores. A program can be concurrent without being parallel, and most real systems use both.", svg:S("w8", 230, "Interleaved tasks on one core versus simultaneous tasks on two cores",
  T(20, 28, "concurrent: one core, tasks take turns", "b") + T(90, 58, "core 1", "sm", "end") +
  '<rect class="sp" x="100" y="44" width="70" height="24" rx="4"/><rect class="st" x="170" y="44" width="70" height="24" rx="4"/><rect class="ka" x="240" y="44" width="70" height="24" rx="4"/><rect class="sp" x="310" y="44" width="70" height="24" rx="4"/><rect class="st" x="380" y="44" width="70" height="24" rx="4"/><rect class="ka" x="450" y="44" width="70" height="24" rx="4"/>' +
  T(640, 60, "asyncio, threads on GIL", "sm", "middle") +
  T(20, 118, "parallel: tasks run at the same instant", "b") + T(90, 148, "core 1", "sm", "end") + T(90, 182, "core 2", "sm", "end") + T(90, 216, "core 3", "sm", "end") +
  '<rect class="sp" x="100" y="134" width="210" height="24" rx="4"/><rect class="st" x="100" y="168" width="210" height="24" rx="4"/><rect class="ka" x="100" y="202" width="210" height="24" rx="4"/>' +
  T(640, 182, "processes, free-threading", "sm", "middle")) },

backpressure:{ t:"Backpressure in a pipeline", cap:"Stages are connected by bounded queues. When the slowest stage falls behind, its input queue fills, the upstream put() blocks, and the slowdown propagates back to the source. The whole pipeline runs at the speed of its slowest stage and memory stays bounded.", svg:S("w9", 220, "Bounded queues between pipeline stages",
  B(20, 60, 120, 50, "pr", ["download", "20 workers"]) + B(220, 60, 120, 50, "pr", ["parse", "4 workers"]) + B(420, 60, 120, 50, "bad", ["write to DB", "slow"]) + B(620, 60, 120, 50, "box", ["done"]) +
  '<rect class="box" x="148" y="74" width="64" height="22" rx="4"/><rect class="box" x="348" y="74" width="64" height="22" rx="4"/>' +
  '<rect class="st" x="350" y="76" width="60" height="18" rx="3"/><rect class="st" x="150" y="76" width="44" height="18" rx="3"/>' +
  A("w9", "M540 85 L618 85") +
  T(180, 130, "filling", "sm", "middle") + T(380, 130, "full: put() blocks", "sm tR", "middle") +
  A("w9", "M470 160 L90 160", "ln dash") + T(280, 180, "slowdown propagates upstream instead of buffering forever", "sm", "middle") +
  T(380, 30, "queues have maxsize", "sm", "middle")) },

taskGroup:{ t:"Structured concurrency with TaskGroup", cap:"Tasks started in a TaskGroup cannot outlive its async with block. If one task fails, the group cancels its siblings, waits for them to finish, and raises every error together as an ExceptionGroup, so nothing keeps running unsupervised.", svg:S("w10", 240, "TaskGroup block with three tasks, one failing",
  '<rect class="fill" x="20" y="20" width="720" height="170" rx="12"/>' + T(36, 44, "async with asyncio.TaskGroup() as tg:", "b mono") +
  T(110, 82, "profile", "sm", "end") + '<rect class="sp" x="120" y="70" width="380" height="20" rx="4"/>' + T(510, 84, "cancelled", "sm tR") +
  T(110, 116, "orders", "sm", "end") + '<rect class="bad" x="120" y="104" width="240" height="20" rx="4"/>' + T(370, 118, "raises ValueError", "sm tR") +
  T(110, 150, "alerts", "sm", "end") + '<rect class="sp" x="120" y="138" width="170" height="20" rx="4"/>' + T(300, 152, "finished", "sm tS") +
  '<path class="ln dash" d="M360 60 L360 170"/>' + T(368, 176, "first failure: cancel the rest", "sm") +
  A("w10", "M380 190 L380 212") + B(200, 212, 360, 26, "ka", ["block exits: ExceptionGroup([ValueError])"], false)) },

flameGraph:{ t:"Reading a flame graph", cap:"Each box is a function; its width is the share of samples in which it was on the stack, and boxes sit on top of their callers. Wide boxes are expensive; the exposed top of a wide box is time spent in that function itself. The x-axis is not time order.", svg:S("w11", 240, "Flame graph with a wide database call",
  B(20, 190, 720, 30, "box", ["main()  100%"], false) +
  B(20, 156, 560, 30, "pr", ["build_report()  78%"], false) + B(584, 156, 156, 30, "pr", ["send_email()  22%"], false) +
  B(20, 122, 430, 30, "st", ["load_orders()  60%"], false) + B(454, 122, 126, 30, "pr", ["format_row 18%"], false) + B(584, 122, 120, 30, "sp", ["smtp.send 20%"], false) +
  B(20, 88, 400, 30, "bad", ["sqlite3 execute()  56%"], false) +
  T(600, 60, "widest bar at the top of a", "sm", "middle") + T(600, 78, "stack = where time goes", "sm", "middle") +
  T(220, 70, "← fix this first (N+1 queries?)", "sm tR", "middle") +
  T(380, 30, "y: stack depth (callers below)     x: share of samples, not time order", "sm", "middle")) }
});

/* figs8: diagrams for the in-depth topic articles, Parts IV to VII */
Object.assign(FIGS, {

layers:{ t:"A layered application", cap:"Each layer depends only on the layer below it. Requests enter at the presentation layer, use cases in the application layer coordinate domain objects, and the infrastructure layer talks to databases and external services.", svg:S("v1", 260, "Presentation, application, domain and infrastructure layers",
  B(120, 16, 520, 46, "pr", ["presentation: routes, CLI, schemas"]) +
  B(120, 76, 520, 46, "sp", ["application: use cases, transactions"]) +
  B(120, 136, 520, 46, "ka", ["domain: entities, value objects, rules"]) +
  B(120, 196, 520, 46, "st", ["infrastructure: ORM, APIs, queues"]) +
  A("v1", "M380 62 L380 74") + A("v1", "M380 122 L380 134") + A("v1", "M380 182 L380 194") +
  T(60, 130, "depends", "sm", "middle") + T(60, 146, "downward", "sm", "middle") +
  T(700, 130, "keep rules", "sm", "middle") + T(700, 146, "in the domain", "sm", "middle")) },

lruDesign:{ t:"LRU cache: hash map plus linked list", cap:"The hash map finds a key's node in O(1). The doubly linked list keeps nodes ordered by recency: an accessed node moves to the front, and when the cache is full the node just before the tail sentinel, the least recently used, is evicted.", svg:S("v2", 240, "Hash map entries pointing into a doubly linked recency list",
  B(20, 30, 150, 150, "box", ["hash map", "a → node", "b → node", "c → node"]) +
  B(220, 150, 80, 40, "pr", ["head"], false) + B(320, 150, 80, 40, "sp", ["c (MRU)"], false) + B(420, 150, 80, 40, "sp", ["a"], false) + B(520, 150, 80, 40, "ka", ["b (LRU)"], false) + B(620, 150, 80, 40, "pr", ["tail"], false) +
  A("v2", "M300 165 L318 165") + A("v2", "M400 165 L418 165") + A("v2", "M500 165 L518 165") + A("v2", "M600 165 L618 165") +
  A("v2", "M318 180 L302 180") + A("v2", "M418 180 L402 180") + A("v2", "M518 180 L502 180") + A("v2", "M618 180 L602 180") +
  '<path class="ln dash" d="M170 80 L360 148"/><path class="ln dash" d="M170 105 L460 148"/><path class="ln dash" d="M170 130 L560 148"/>' +
  T(460, 40, "get(a): move a to the front", "sm", "middle") + T(460, 62, "put(new) when full: evict b", "sm tR", "middle") + T(460, 220, "both operations O(1)", "sm tS", "middle")) },

taskQueue:{ t:"A background task queue", cap:"The web process enqueues a small message (task name and arguments) and responds immediately. The broker stores it durably; worker processes pull tasks, run them, retry failures and optionally store results. Because delivery is at least once, tasks must be idempotent.", svg:S("v3", 230, "Web app, broker and workers",
  B(20, 70, 160, 60, "pr", ["web request", "send_receipt.delay(812)"]) +
  A("v3", "M180 100 L268 100") + T(224, 90, "enqueue", "sm", "middle") +
  B(270, 60, 200, 80, "ka", ["broker", "Redis / RabbitMQ", "durable queue"]) +
  A("v3", "M470 85 L568 50") + A("v3", "M470 100 L568 100") + A("v3", "M470 115 L568 150") +
  B(570, 26, 170, 44, "sp", ["worker 1"]) + B(570, 78, 170, 44, "sp", ["worker 2"]) + B(570, 130, 170, 44, "sp", ["worker 3"]) +
  T(100, 160, "responds in ms", "sm tS", "middle") + T(370, 175, "retries, delays, dead letters", "sm", "middle") +
  T(655, 200, "ack after success; may run twice", "sm tR", "middle")) },

broadcastRules:{ t:"Broadcasting rules", cap:"Shapes are compared from the right. Each pair of dimensions must be equal or one of them must be 1; a size-1 dimension is stretched to match without copying data. If any pair differs and neither is 1, NumPy raises an error.", svg:S("v4", 230, "Aligning shapes from the right",
  T(20, 30, "m (3, 4) + row (4,)", "b mono") +
  B(20, 44, 80, 34, "pr", ["3"], false) + B(104, 44, 80, 34, "pr", ["4"], false) +
  B(20, 84, 80, 34, "box", ["1 ← added"], false) + B(104, 84, 80, 34, "pr", ["4"], false) +
  T(210, 70, "→ (3, 4) ✓", "b tS") +
  T(400, 30, "m (3, 4) + col (3, 1)", "b mono") +
  B(400, 44, 80, 34, "pr", ["3"], false) + B(484, 44, 80, 34, "pr", ["4"], false) +
  B(400, 84, 80, 34, "pr", ["3"], false) + B(484, 84, 80, 34, "ka", ["1 → 4"], false) +
  T(590, 70, "→ (3, 4) ✓", "b tS") +
  T(20, 160, "m (3, 4) + v (3,)", "b mono") +
  B(20, 174, 80, 34, "pr", ["3"], false) + B(104, 174, 80, 34, "bad", ["4"], false) + B(104, 212, 80, 0, "box", [""], false) +
  T(210, 196, "4 vs 3: error. Reshape v to (3, 1)", "sm tR")) },

recursionTree:{ t:"Recursion tree for merge sort", cap:"Each node is one call, labelled with the merging work it does. Every level of the tree sums to cn, and the problem halves until size 1, giving about log n levels, so the total is cn log n.", svg:S("v5", 250, "Levels of the merge sort recursion tree",
  B(300, 14, 120, 32, "pr", ["cn"], false) +
  B(160, 70, 120, 32, "pr", ["cn/2"], false) + B(440, 70, 120, 32, "pr", ["cn/2"], false) +
  B(80, 126, 90, 32, "pr", ["cn/4"], false) + B(220, 126, 90, 32, "pr", ["cn/4"], false) + B(410, 126, 90, 32, "pr", ["cn/4"], false) + B(550, 126, 90, 32, "pr", ["cn/4"], false) +
  A("v5", "M330 46 L240 68") + A("v5", "M390 46 L480 68") +
  A("v5", "M200 102 L130 124") + A("v5", "M240 102 L265 124") + A("v5", "M480 102 L455 124") + A("v5", "M520 102 L595 124") +
  T(360, 190, "· · ·   n leaves of size 1   · · ·", "sm", "middle") +
  T(700, 34, "= cn", "b tS") + T(700, 90, "= cn", "b tS") + T(700, 146, "= cn", "b tS") +
  T(360, 232, "log₂ n levels × cn per level = O(n log n)", "b", "middle")) },

fastPow:{ t:"Binary exponentiation of 3^13", cap:"13 is 1101 in binary, so 3^13 = 3^8 × 3^4 × 3^1. Squaring the base produces 3^1, 3^2, 3^4, 3^8 in turn; the result is multiplied by the current power only when the corresponding bit of the exponent is 1. Four squarings replace twelve multiplications.", svg:S("v6", 220, "Exponent bits, powers of the base and the running result",
  T(20, 40, "bit of 13", "b") + T(20, 90, "power", "b") + T(20, 140, "result", "b") +
  B(130, 20, 130, 34, "okf", ["bit 0 = 1"], false) + B(280, 20, 130, 34, "box", ["bit 1 = 0"], false) + B(430, 20, 130, 34, "okf", ["bit 2 = 1"], false) + B(580, 20, 130, 34, "okf", ["bit 3 = 1"], false) +
  B(130, 70, 130, 34, "pr", ["3^1 = 3"], false) + B(280, 70, 130, 34, "pr", ["3^2 = 9"], false) + B(430, 70, 130, 34, "pr", ["3^4 = 81"], false) + B(580, 70, 130, 34, "pr", ["3^8 = 6561"], false) +
  B(130, 120, 130, 34, "sp", ["1×3 = 3"], false) + B(280, 120, 130, 34, "box", ["3 (skip)"], false) + B(430, 120, 130, 34, "sp", ["3×81 = 243"], false) + B(580, 120, 130, 34, "sp", ["243×6561"], false) +
  A("v6", "M260 87 L278 87") + A("v6", "M410 87 L428 87") + A("v6", "M560 87 L578 87") +
  T(380, 190, "3^13 = 1594323 with O(log n) multiplications", "b", "middle")) },

floydCycle:{ t:"Floyd's cycle detection", cap:"The head is a steps from the cycle start, and the pointers meet b steps into the cycle. Because the fast pointer has travelled twice as far, a + b is a whole number of laps, so walking a more steps from the meeting point lands on the cycle start, exactly where a pointer from the head arrives after a steps.", svg:S("v7", 250, "List with a tail of length a leading into a cycle",
  B(20, 100, 60, 36, "pr", ["head"], false) + B(110, 100, 50, 36, "box", [""], false) + B(190, 100, 50, 36, "box", [""], false) +
  B(270, 100, 70, 36, "okf", ["start"], false) +
  A("v7", "M80 118 L108 118") + A("v7", "M160 118 L188 118") + A("v7", "M240 118 L268 118") +
  '<circle class="ln" cx="430" cy="118" r="90" fill="none"/>' +
  A("v7", "M340 118 L342 118") +
  B(480, 26, 90, 36, "ka", ["meet"], false) +
  T(175, 160, "a steps", "sm", "middle") + T(430, 46, "b", "sm") + T(545, 200, "cycle length c", "sm", "middle") +
  T(640, 90, "a + b = k·c", "b mono") + T(640, 120, "so a ≡ c − b", "mono") + T(640, 150, "(mod c)", "mono") +
  T(380, 236, "slow moves 1, fast moves 2; after meeting, restart one at head, both move 1", "sm", "middle")) },

skipList:{ t:"A skip list", cap:"Every key is in the bottom list; about half also appear one level up, a quarter two levels up, and so on, chosen by coin flips. A search starts at the top-left, moves right while the next key is smaller than the target, and drops down a level otherwise, taking O(log n) expected steps.", svg:S("v8", 230, "Three levels of forward pointers over sorted keys",
  T(20, 40, "L3", "b") + T(20, 100, "L2", "b") + T(20, 160, "L1", "b") +
  B(50, 24, 50, 30, "pr", ["head"], false) + B(330, 24, 50, 30, "sp", ["30"], false) +
  B(50, 84, 50, 30, "pr", ["head"], false) + B(190, 84, 50, 30, "sp", ["12"], false) + B(330, 84, 50, 30, "sp", ["30"], false) + B(540, 84, 50, 30, "sp", ["50"], false) +
  B(50, 144, 50, 30, "pr", ["head"], false) + B(120, 144, 50, 30, "sp", ["5"], false) + B(190, 144, 50, 30, "sp", ["12"], false) + B(260, 144, 50, 30, "sp", ["20"], false) + B(330, 144, 50, 30, "sp", ["30"], false) + B(400, 144, 50, 30, "ka", ["38"], false) + B(470, 144, 50, 30, "sp", ["44"], false) + B(540, 144, 50, 30, "sp", ["50"], false) +
  A("v8", "M100 39 L328 39") + A("v8", "M100 99 L188 99") + A("v8", "M240 99 L328 99") + A("v8", "M380 99 L538 99") +
  A("v8", "M355 54 L355 82", "ln dash") + A("v8", "M355 114 L355 142", "ln dash") + A("v8", "M380 159 L398 159") +
  T(380, 210, "search 38: right to 30 on L3, down, 50 is too big so down, then right to 38", "sm", "middle")) },

histogramStack:{ t:"Largest rectangle with a monotonic stack", cap:"When the bar of height 2 at index 4 arrives, the taller bars 6 and 5 can no longer extend right. Popping 6 gives width 1 (area 6); popping 5 gives width 2 because its left boundary is the bar of height 1 still on the stack, giving the best area of 10.", svg:S("v9", 250, "Histogram bars with the rectangle of height 5 spanning two bars",
  '<rect class="sp" x="60" y="140" width="60" height="40"/><rect class="sp" x="130" y="160" width="60" height="20"/><rect class="ka" x="200" y="80" width="60" height="100"/><rect class="ka" x="270" y="60" width="60" height="120"/><rect class="st" x="340" y="140" width="60" height="40"/><rect class="sp" x="410" y="120" width="60" height="60"/>' +
  '<rect class="bad" x="200" y="80" width="130" height="100"/>' +
  T(90, 198, "2", "sm", "middle") + T(160, 198, "1", "sm", "middle") + T(230, 198, "5", "sm", "middle") + T(300, 198, "6", "sm", "middle") + T(370, 198, "2", "sm", "middle") + T(440, 198, "3", "sm", "middle") +
  T(265, 132, "5 × 2 = 10", "b", "middle") +
  T(500, 60, "stack before index 4: [1, 2, 3]", "sm mono") + T(500, 85, "(heights 1, 5, 6: increasing)", "sm") +
  T(500, 120, "h=2 arrives: pop 6 → area 6×1", "sm") + T(500, 145, "pop 5 → left = index 1", "sm") + T(500, 170, "width = 4 − 1 − 1 = 2", "sm") +
  T(260, 235, "each bar is pushed and popped once: O(n)", "sm tS", "middle")) },

fibTree:{ t:"Recursion tree of fib(5)", cap:"Every call branches into two smaller calls, so the tree has about φⁿ nodes even though there are only n + 1 distinct arguments. The repeated subtrees (highlighted fib(3) and fib(2)) are recomputed; memoization keeps one copy of each and reduces the work to O(n). The tree's height, n, is the stack depth.", svg:S("v10", 250, "Call tree with repeated subproblems",
  B(330, 10, 80, 30, "pr", ["fib(5)"], false) +
  B(170, 62, 80, 30, "pr", ["fib(4)"], false) + B(500, 62, 80, 30, "ka", ["fib(3)"], false) +
  B(80, 114, 80, 30, "ka", ["fib(3)"], false) + B(260, 114, 80, 30, "st", ["fib(2)"], false) + B(440, 114, 80, 30, "st", ["fib(2)"], false) + B(580, 114, 70, 30, "box", ["fib(1)"], false) +
  B(30, 166, 70, 30, "st", ["fib(2)"], false) + B(120, 166, 70, 30, "box", ["fib(1)"], false) + B(230, 166, 60, 30, "box", ["f(1)"], false) + B(300, 166, 60, 30, "box", ["f(0)"], false) + B(410, 166, 60, 30, "box", ["f(1)"], false) + B(480, 166, 60, 30, "box", ["f(0)"], false) +
  B(20, 214, 50, 26, "box", ["f(1)"], false) + B(76, 214, 50, 26, "box", ["f(0)"], false) +
  A("v10", "M350 40 L220 60") + A("v10", "M390 40 L530 60") + A("v10", "M190 92 L130 112") + A("v10", "M230 92 L290 112") + A("v10", "M520 92 L480 112") + A("v10", "M560 92 L610 112") +
  A("v10", "M100 144 L70 164") + A("v10", "M140 144 L155 164") + A("v10", "M280 144 L260 164") + A("v10", "M320 144 L330 164") + A("v10", "M460 144 L440 164") + A("v10", "M500 144 L510 164") + A("v10", "M55 196 L45 212") + A("v10", "M75 196 L100 212") +
  T(700, 150, "15 calls,", "sm", "middle") + T(700, 170, "6 distinct", "sm", "middle") + T(700, 190, "arguments", "sm", "middle")) },

decisionTree:{ t:"Decision tree for sorting three items", cap:"Each internal node is one comparison and each leaf is one of the 3! = 6 possible orders. Any comparison sort corresponds to such a tree; with n! leaves, a binary tree must have height at least log₂(n!), which is Θ(n log n), so some input needs that many comparisons.", svg:S("v11", 250, "Binary tree of comparisons ending in six orderings",
  B(320, 10, 100, 32, "pr", ["a < b ?"], false) +
  B(150, 70, 100, 32, "pr", ["b < c ?"], false) + B(490, 70, 100, 32, "pr", ["a < c ?"], false) +
  B(60, 130, 90, 30, "okf", ["a b c"], false) + B(200, 130, 100, 32, "pr", ["a < c ?"], false) + B(420, 130, 90, 30, "okf", ["b a c"], false) + B(560, 130, 100, 32, "pr", ["b < c ?"], false) +
  B(150, 190, 80, 30, "okf", ["a c b"], false) + B(260, 190, 80, 30, "okf", ["c a b"], false) + B(510, 190, 80, 30, "okf", ["b c a"], false) + B(620, 190, 80, 30, "okf", ["c b a"], false) +
  A("v11", "M350 42 L210 68") + A("v11", "M390 42 L530 68") + A("v11", "M180 102 L110 128") + A("v11", "M220 102 L245 128") +
  A("v11", "M520 102 L470 128") + A("v11", "M560 102 L605 128") + A("v11", "M230 162 L195 188") + A("v11", "M270 162 L295 188") +
  A("v11", "M590 162 L555 188") + A("v11", "M630 162 L660 188") +
  T(380, 240, "6 leaves need height ≥ log₂ 6 ≈ 2.58, so 3 comparisons in the worst case", "sm", "middle")) },

medianPartition:{ t:"Partitioning two arrays for the median", cap:"Choose i elements from A and j = half − i from B for the left side. The partition is correct when the largest left values are no bigger than the smallest right values (A[i−1] ≤ B[j] and B[j−1] ≤ A[i]); binary search on i in the shorter array finds it in O(log min(m, n)).", svg:S("v12", 230, "Two sorted arrays cut into left and right halves",
  T(20, 50, "A", "b") + B(50, 30, 60, 34, "sp", ["1"], false) + B(115, 30, 60, 34, "sp", ["3"], false) + B(180, 30, 60, 34, "sp", ["8"], false) + B(260, 30, 60, 34, "ka", ["9"], false) + B(325, 30, 60, 34, "ka", ["15"], false) +
  T(20, 130, "B", "b") + B(50, 110, 60, 34, "sp", ["7"], false) + B(115, 110, 60, 34, "sp", ["11"], false) + B(195, 110, 60, 34, "ka", ["18"], false) + B(260, 110, 60, 34, "ka", ["19"], false) + B(325, 110, 60, 34, "ka", ["21"], false) + B(390, 110, 60, 34, "ka", ["25"], false) +
  '<path class="bad" d="M250 20 L250 74"/><path class="bad" d="M185 100 L185 154"/>' +
  T(470, 40, "left side: 1 3 8 | 7 11  (5 items)", "sm mono") + T(470, 64, "right side: 9 15 | 18 19 21 25", "sm mono") +
  T(470, 104, "A[i−1] = 8 ≤ B[j] = 18 ✓", "sm tS") + T(470, 128, "B[j−1] = 11 ≤ A[i] = 9 ✗", "sm tR") +
  T(470, 152, "→ move the cut in A right", "sm") +
  T(380, 210, "median = max(left) when the total is odd, else the average with min(right)", "sm", "middle")) },

avlRotation:{ t:"A right rotation", cap:"In the left-left case, node y is too heavy on the left. Rotating right makes its left child x the subtree root, y becomes x's right child, and x's former right subtree B moves to become y's left child. The in-order sequence A x B y C is unchanged, so the BST property holds, and the heights are rebalanced.", svg:S("v13", 230, "Before and after a right rotation",
  B(150, 20, 50, 34, "bad", ["y"], false) + B(80, 80, 50, 34, "pr", ["x"], false) + B(230, 80, 50, 34, "box", ["C"], false) +
  B(30, 140, 50, 34, "box", ["A"], false) + B(120, 140, 50, 34, "box", ["B"], false) +
  A("v13", "M160 54 L115 78") + A("v13", "M190 54 L245 78") + A("v13", "M95 114 L60 138") + A("v13", "M115 114 L140 138") +
  T(175, 200, "left-heavy: balance(y) = +2", "sm tR", "middle") +
  A("v13", "M330 100 L420 100") + T(375, 90, "rotate right", "sm", "middle") +
  B(560, 20, 50, 34, "okf", ["x"], false) + B(480, 80, 50, 34, "box", ["A"], false) + B(640, 80, 50, 34, "pr", ["y"], false) +
  B(590, 140, 50, 34, "box", ["B"], false) + B(690, 140, 50, 34, "box", ["C"], false) +
  A("v13", "M570 54 L515 78") + A("v13", "M600 54 L655 78") + A("v13", "M655 114 L620 138") + A("v13", "M675 114 L705 138") +
  T(600, 200, "in-order still A x B y C", "sm tS", "middle")) },

bplusTree:{ t:"A B+ tree", cap:"Internal nodes hold only separator keys that route searches, so each page fits many children and the tree stays shallow. All records live in the leaves, which are linked in key order, so a range scan descends once and then walks the leaf chain.", svg:S("v14", 220, "Root of separators, internal routing and linked leaves",
  B(300, 14, 160, 34, "pr", ["30 | 60"], false) +
  B(80, 80, 140, 34, "pr", ["10 | 20"], false) + B(310, 80, 140, 34, "pr", ["40 | 50"], false) + B(540, 80, 140, 34, "pr", ["70 | 80"], false) +
  A("v14", "M330 48 L160 78") + A("v14", "M380 48 L380 78") + A("v14", "M430 48 L610 78") +
  B(20, 150, 80, 32, "sp", ["5 8"], false) + B(110, 150, 80, 32, "sp", ["10 15"], false) + B(200, 150, 80, 32, "sp", ["20 25"], false) +
  B(290, 150, 80, 32, "sp", ["30 35"], false) + B(380, 150, 80, 32, "sp", ["40 45"], false) + B(470, 150, 80, 32, "sp", ["50 55"], false) +
  B(560, 150, 80, 32, "sp", ["60 65"], false) + B(650, 150, 90, 32, "sp", ["70 … 85"], false) +
  A("v14", "M100 166 L108 166") + A("v14", "M190 166 L198 166") + A("v14", "M280 166 L288 166") + A("v14", "M370 166 L378 166") + A("v14", "M460 166 L468 166") + A("v14", "M550 166 L558 166") + A("v14", "M640 166 L648 166") +
  T(380, 205, "leaves hold the data and link left to right: fast range scans", "sm", "middle")) },

trieDiagram:{ t:"A trie of app, apple, apply and bat", cap:"Words share the path for their common prefix: a-p-p is stored once for app, apple and apply. Marked nodes end a word. Looking up a word or a prefix walks one edge per character, regardless of how many words the trie holds.", svg:S("v15", 250, "Prefix tree with shared branches",
  B(330, 10, 60, 30, "pr", ["root"], false) +
  B(230, 60, 50, 30, "sp", ["a"], false) + B(450, 60, 50, 30, "sp", ["b"], false) +
  B(230, 110, 50, 30, "sp", ["p"], false) + B(450, 110, 50, 30, "sp", ["a"], false) +
  B(230, 160, 50, 30, "okf", ["p ●"], false) + B(450, 160, 50, 30, "okf", ["t ●"], false) +
  B(170, 210, 50, 30, "sp", ["l"], false) +
  A("v15", "M345 40 L265 58") + A("v15", "M375 40 L465 58") + A("v15", "M255 90 L255 108") + A("v15", "M475 90 L475 108") + A("v15", "M255 140 L255 158") + A("v15", "M475 140 L475 158") + A("v15", "M245 190 L200 208") +
  B(90, 160, 50, 30, "okf", ["e ●"], false) + B(10, 160, 60, 30, "okf", ["y ●"], false) +
  A("v15", "M185 210 L130 190") + A("v15", "M180 210 L50 190") +
  T(600, 120, "● = end of a word", "sm") + T(600, 145, "app, apple, apply, bat", "sm mono")) },

segmentTree:{ t:"Segment tree over [5, 3, 8, 6]", cap:"Each node stores the sum of a range: leaves hold single elements, and every parent combines its two children. A range query uses O(log n) nodes that exactly cover the range, and a point update changes the leaf and the O(log n) nodes on its path to the root.", svg:S("v16", 230, "Range sums stored in a binary tree",
  B(310, 14, 140, 34, "pr", ["[0,4) = 22"], false) +
  B(150, 76, 140, 34, "sp", ["[0,2) = 8"], false) + B(470, 76, 140, 34, "ka", ["[2,4) = 14"], false) +
  B(90, 140, 100, 34, "box", ["5"], false) + B(210, 140, 100, 34, "okf", ["3"], false) + B(450, 140, 80, 34, "ka", ["8"], false) + B(550, 140, 80, 34, "ka", ["6"], false) +
  A("v16", "M350 48 L230 74") + A("v16", "M410 48 L530 74") + A("v16", "M200 110 L150 138") + A("v16", "M240 110 L260 138") + A("v16", "M520 110 L495 138") + A("v16", "M560 110 L585 138") +
  T(380, 205, "query [1,4) = node 3 + node [2,4) = 3 + 14 = 17", "sm", "middle")) },

rerooting:{ t:"Rerooting across one edge", cap:"Moving the root from p to its child c brings the size[c] nodes in c's subtree one step closer and pushes the other n − size[c] nodes one step farther, so answer[c] = answer[p] − size[c] + (n − size[c]). One pass computes subtree sizes, a second pass applies this to every edge.", svg:S("v17", 220, "Two sides of an edge when rerooting",
  '<rect class="fill" x="20" y="30" width="300" height="160" rx="14"/>' + T(170, 54, "n − size[c] nodes", "b", "middle") + T(170, 76, "each 1 farther from c", "sm tR", "middle") +
  B(240, 100, 60, 36, "pr", ["p"], false) +
  '<rect class="sp" x="440" y="30" width="300" height="160" rx="14"/>' + T(590, 54, "size[c] nodes (c's subtree)", "b", "middle") + T(590, 76, "each 1 closer to c", "sm tS", "middle") +
  B(460, 100, 60, 36, "ka", ["c"], false) +
  '<path class="ln" d="M300 118 L460 118"/>' + T(380, 108, "edge", "sm", "middle") +
  T(380, 210, "answer[c] = answer[p] − size[c] + (n − size[c])", "b mono", "middle")) },

dijkstraSteps:{ t:"Dijkstra from A, step by step", cap:"Each step pops the unfinished vertex with the smallest tentative distance, finalizes it, and relaxes its outgoing edges. B first gets 4 through the direct edge, then improves to 3 through C; D drops from 8 to 4 once B is finalized. The highlighted path A → C → B → D costs 4.", svg:S("v19", 250, "Graph with weighted edges and a table of tentative distances per step",
  B(30, 95, 44, 34, "pr", ["A"], true) + B(170, 20, 44, 34, "sp", ["B"], true) + B(170, 170, 44, 34, "sp", ["C"], true) + B(310, 95, 44, 34, "ka", ["D"], true) +
  A("v19", "M74 104 L168 46", "ln dash") + T(112, 64, "4", "b") +
  A("v19", "M74 120 L168 180") + T(112, 168, "1", "b") +
  A("v19", "M192 170 L192 56") + T(200, 118, "2", "b") +
  A("v19", "M214 46 L308 104") + T(266, 64, "1", "b") +
  A("v19", "M214 180 L308 122", "ln dash") + T(266, 168, "7", "b") +
  T(560, 30, "pop  |  A   B   C   D", "mono b", "middle") +
  T(560, 62, "A=0  |  0   4   1   ∞", "mono", "middle") +
  T(560, 94, "C=1  |  0   3   1   8", "mono", "middle") +
  T(560, 126, "B=3  |  0   3   1   4", "mono", "middle") +
  T(560, 158, "D=4  |  0   3   1   4", "mono", "middle") +
  T(560, 196, "popped vertices are final", "sm tS", "middle") +
  T(380, 238, "solid edges form the shortest-path tree; dashed edges were relaxed but lost", "sm", "middle")) },

sccCondense:{ t:"Strongly connected components and the condensation", cap:"Inside each shaded group every vertex reaches every other through the cycle. Edges between groups only go one way, so collapsing each component to a single vertex gives a DAG that can be processed in topological order.", svg:S("v20", 230, "Directed graph grouped into SCCs",
  '<rect class="fill" x="20" y="25" width="230" height="125" rx="14"/>' + T(135, 145, "SCC {0, 1, 2}", "sm", "middle") +
  B(35, 90, 40, 30, "sp", ["0"], true) + B(115, 35, 40, 30, "sp", ["1"], true) + B(195, 90, 40, 30, "sp", ["2"], true) +
  A("v20", "M68 90 L115 62") + A("v20", "M155 62 L200 90") + A("v20", "M195 110 L77 110") +
  '<rect class="fill" x="290" y="25" width="230" height="125" rx="14"/>' + T(405, 145, "SCC {3, 4, 5}", "sm", "middle") +
  B(305, 90, 40, 30, "ka", ["3"], true) + B(385, 35, 40, 30, "ka", ["4"], true) + B(465, 90, 40, 30, "ka", ["5"], true) +
  A("v20", "M338 90 L385 62") + A("v20", "M425 62 L470 90") + A("v20", "M465 110 L347 110") +
  A("v20", "M235 100 L303 100") +
  B(600, 35, 40, 30, "pr", ["6"], true) + B(680, 100, 40, 30, "box", ["7"], true) +
  A("v20", "M600 58 L507 98") + A("v20", "M640 60 L688 98") +
  T(380, 200, "condensation (a DAG):  {0,1,2} → {3,4,5} ← {6} → {7}", "mono b", "middle")) },

residualGraph:{ t:"A flow and its residual graph", cap:"An edge carrying 7 units out of capacity 10 becomes two residual edges: 3 more units can still go forward, and the 7 already sent can be pushed back. Those backward edges let a later augmenting path reroute flow that an earlier path placed badly.", svg:S("v21", 190, "Flow network next to its residual graph",
  T(200, 30, "flow / capacity", "b", "middle") + T(580, 30, "residual graph", "b", "middle") +
  B(30, 70, 40, 34, "pr", ["s"], true) + B(190, 70, 40, 34, "sp", ["a"], true) + B(350, 70, 40, 34, "ka", ["t"], true) +
  A("v21", "M70 87 L188 87") + T(130, 78, "7 / 10", "mono", "middle") +
  A("v21", "M230 87 L348 87") + T(290, 78, "7 / 8", "mono", "middle") +
  B(420, 70, 40, 34, "pr", ["s"], true) + B(560, 70, 40, 34, "sp", ["a"], true) + B(700, 70, 40, 34, "ka", ["t"], true) +
  A("v21", "M460 79 L558 79") + T(510, 70, "3", "mono", "middle") +
  A("v21", "M600 79 L698 79") + T(650, 70, "1", "mono", "middle") +
  A("v21", "M558 96 L462 96", "ln dash") + T(510, 116, "7", "mono", "middle") +
  A("v21", "M698 96 L602 96", "ln dash") + T(650, 116, "7", "mono", "middle") +
  T(380, 165, "solid = capacity left; dashed = flow that can be undone", "sm", "middle")) },

knapsackRoll:{ t:"One-row knapsack: the loop direction decides reuse", cap:"Adding an item of weight 3 to a single dp row. Going downward, dp[w − 3] has not been touched yet for this item, so it still describes the previous items and the item is used at most once. Going upward, dp[w − 3] was already updated and may contain the item, so it can be added again.", svg:S("v22", 260, "Two dp rows with arrows showing which cell is read",
  T(20, 28, "0/1 knapsack: w goes 7, 6, 5, 4, 3 (downward)", "b") +
  B(120, 40, 66, 34, "box", ["dp[0]"], false) + B(196, 40, 66, 34, "box", ["dp[1]"], false) + B(272, 40, 66, 34, "box", ["dp[2]"], false) + B(348, 40, 66, 34, "box", ["dp[3]"], false) + B(424, 40, 66, 34, "sp", ["dp[4]"], false) + B(500, 40, 66, 34, "box", ["dp[5]"], false) + B(576, 40, 66, 34, "box", ["dp[6]"], false) + B(652, 40, 66, 34, "ka", ["dp[7]"], false) +
  A("v22", "M457 76 Q571 112 683 78") + T(570, 112, "reads old dp[4]", "sm", "middle") +
  T(20, 148, "unbounded knapsack: w goes 3, 4, 5, 6, 7 (upward)", "b") +
  B(120, 160, 66, 34, "box", ["dp[0]"], false) + B(196, 160, 66, 34, "box", ["dp[1]"], false) + B(272, 160, 66, 34, "box", ["dp[2]"], false) + B(348, 160, 66, 34, "okf", ["dp[3]"], false) + B(424, 160, 66, 34, "box", ["dp[4]"], false) + B(500, 160, 66, 34, "box", ["dp[5]"], false) + B(576, 160, 66, 34, "ka", ["dp[6]"], false) + B(652, 160, 66, 34, "box", ["dp[7]"], false) +
  A("v22", "M381 196 Q495 232 607 198") + T(494, 232, "reads dp[3], already including the item", "sm", "middle") +
  T(380, 254, "dp[w] = max(dp[w], dp[w − 3] + value)", "mono", "middle")) },

lisTails:{ t:"Patience sorting for the longest increasing subsequence", cap:"Processing 10, 9, 2, 5, 3, 7, 101, 18. Each number replaces the first tail that is greater than or equal to it (found by binary search) or is appended if it is larger than every tail. Smaller tails leave more room for future elements; the final length, 4, is the LIS length.", svg:S("v23", 250, "Tails array after each element",
  T(40, 34, "x = 10", "mono") + T(150, 34, "[10]", "mono b") + T(330, 34, "append", "sm tS") +
  T(40, 60, "x = 9", "mono") + T(150, 60, "[9]", "mono b") + T(330, 60, "replace", "sm tR") +
  T(40, 86, "x = 2", "mono") + T(150, 86, "[2]", "mono b") + T(330, 86, "replace", "sm tR") +
  T(40, 112, "x = 5", "mono") + T(150, 112, "[2, 5]", "mono b") + T(330, 112, "append", "sm tS") +
  T(40, 138, "x = 3", "mono") + T(150, 138, "[2, 3]", "mono b") + T(330, 138, "replace", "sm tR") +
  T(40, 164, "x = 7", "mono") + T(150, 164, "[2, 3, 7]", "mono b") + T(330, 164, "append", "sm tS") +
  T(40, 190, "x = 101", "mono") + T(150, 190, "[2, 3, 7, 101]", "mono b") + T(330, 190, "append", "sm tS") +
  T(40, 216, "x = 18", "mono") + T(150, 216, "[2, 3, 7, 18]", "mono b") + T(330, 216, "replace", "sm tR") +
  B(450, 60, 290, 110, "fill", ["tails[k] = smallest tail of any", "increasing subsequence of length k + 1", "", "len(tails) = 4 = LIS length"], false) +
  T(595, 200, "tails is sorted, so bisect_left finds the slot", "sm", "middle")) },

intervalOrder:{ t:"Interval DP fills the table by interval length", cap:"Cell (i, j) holds the answer for the interval i..j. Single elements (the diagonal) are base cases; each later diagonal holds intervals one longer and only reads shorter intervals: a piece to its left in the same row and a piece below it in the same column. The answer for the whole range is the top-right cell.", svg:S("v24", 250, "Upper triangular table coloured by interval length",
  B(60, 30, 58, 40, "box", ["0,0"], false) + B(124, 30, 58, 40, "sp", ["0,1"], false) + B(188, 30, 58, 40, "ka", ["0,2"], false) + B(252, 30, 58, 40, "pr", ["0,3"], false) + B(124, 76, 58, 40, "box", ["1,1"], false) + B(188, 76, 58, 40, "sp", ["1,2"], false) + B(252, 76, 58, 40, "ka", ["1,3"], false) + B(188, 122, 58, 40, "box", ["2,2"], false) + B(252, 122, 58, 40, "sp", ["2,3"], false) + B(252, 168, 58, 40, "box", ["3,3"], false) +
  T(380, 50, "length 1: base cases", "sm") + T(380, 80, "length 2", "sm") + T(380, 110, "length 3", "sm") + T(380, 140, "length 4: the whole range (answer)", "sm") +
  B(350, 38, 20, 16, "box", [""], false) + B(350, 68, 20, 16, "sp", [""], false) + B(350, 98, 20, 16, "ka", [""], false) + B(350, 128, 20, 16, "pr", [""], false) +
  T(380, 236, "dp[i][j] = best over k of dp[i][k] + dp[k+1][j] + cost(i, k, j)", "mono", "middle")) },

stockStates:{ t:"Stock trading as a state machine", cap:"Each day you are in one state and may move along an arrow: buying costs the price, selling earns it (minus any fee), resting keeps the state. The cooldown variant routes every sale through a cooldown state for one day before you can buy again. dp keeps the best profit for each state per day.", svg:S("v25", 260, "Holding and not-holding states with buy and sell transitions",
  B(80, 80, 180, 50, "sp", ["not holding"], true) + B(500, 80, 180, 50, "ka", ["holding"], true) +
  A("v25", "M260 94 L498 94") + T(380, 86, "buy: − price", "sm", "middle") +
  A("v25", "M500 116 L262 116") + T(380, 136, "sell: + price − fee", "sm", "middle") +
  A("v25", "M120 80 C100 26, 220 26, 200 78") + T(160, 26, "rest", "sm", "middle") +
  A("v25", "M540 80 C520 26, 640 26, 620 78") + T(580, 26, "keep holding", "sm", "middle") +
  B(290, 170, 180, 44, "pr", ["cooldown"], false) +
  A("v25", "M560 130 L472 176", "ln dash") + A("v25", "M290 196 L190 132", "ln dash") +
  T(380, 246, "dashed: the cooldown variant (a sale waits one day before the next buy)", "sm", "middle")) },

intervalSchedule:{ t:"Activity selection: earliest end first", cap:"The eleven intervals sorted by end time, one per row. Scanning down, an interval is taken (highlighted) when it starts at or after the end of the last one taken: [1, 4), [5, 7), [8, 11) and [12, 16). Finishing early leaves the most room, so no other choice fits more.", svg:S("v26", 250, "Intervals on a timeline with the selected ones highlighted",
  '<rect class="okf" x="82" y="18" width="126" height="12" rx="4"/>' + '<rect class="box" x="166" y="35" width="84" height="12" rx="4"/>' + '<rect class="box" x="40" y="52" width="252" height="12" rx="4"/>' + '<rect class="okf" x="250" y="69" width="84" height="12" rx="4"/>' + '<rect class="box" x="166" y="86" width="252" height="12" rx="4"/>' + '<rect class="box" x="250" y="103" width="168" height="12" rx="4"/>' + '<rect class="box" x="292" y="120" width="168" height="12" rx="4"/>' + '<rect class="okf" x="376" y="137" width="126" height="12" rx="4"/>' + '<rect class="box" x="376" y="154" width="168" height="12" rx="4"/>' + '<rect class="box" x="124" y="171" width="504" height="12" rx="4"/>' + '<rect class="okf" x="544" y="188" width="168" height="12" rx="4"/>' +
  '<path class="ln" d="M40 210 L712 210"/>' + T(40, 228, "0", "sm", "middle") + T(124, 228, "2", "sm", "middle") + T(208, 228, "4", "sm", "middle") + T(292, 228, "6", "sm", "middle") + T(376, 228, "8", "sm", "middle") + T(460, 228, "10", "sm", "middle") + T(544, 228, "12", "sm", "middle") + T(628, 228, "14", "sm", "middle") + T(712, 228, "16", "sm", "middle") +
  T(380, 246, "rows sorted by end time; highlighted rows are chosen", "sm", "middle")) },

huffmanTree:{ t:"Huffman tree for abracadabra", cap:"Frequencies a 5, b 2, r 2, c 1, d 1. The two rarest nodes are merged first (c + d, then b + r), so rare letters end up deep with long codes and the frequent a gets a one-bit code. Reading 0 for left and 1 for right gives a prefix-free code: 23 bits instead of 88 for 8-bit characters.", svg:S("v27", 290, "Binary code tree with frequencies",
  B(325, 14, 70, 32, "pr", ["11"], true) + B(165, 80, 70, 32, "okf", ["a: 5"], true) + B(485, 80, 70, 32, "pr", ["6"], false) +
  B(385, 146, 70, 32, "pr", ["2"], false) + B(585, 146, 70, 32, "pr", ["4"], false) +
  B(335, 212, 70, 32, "sp", ["c: 1"], false) + B(435, 212, 70, 32, "sp", ["d: 1"], false) + B(535, 212, 70, 32, "ka", ["b: 2"], false) + B(635, 212, 70, 32, "ka", ["r: 2"], false) +
  A("v27", "M350 46 L212 78") + T(272, 56, "0", "b") + A("v27", "M370 46 L508 78") + T(446, 56, "1", "b") +
  A("v27", "M510 112 L432 144") + T(458, 124, "0", "b") + A("v27", "M530 112 L608 144") + T(578, 124, "1", "b") +
  A("v27", "M412 178 L380 210") + T(384, 196, "0", "b") + A("v27", "M428 178 L460 210") + T(452, 196, "1", "b") +
  A("v27", "M612 178 L580 210") + T(584, 196, "0", "b") + A("v27", "M628 178 L660 210") + T(652, 196, "1", "b") +
  T(380, 276, "a = 0   c = 100   d = 101   b = 110   r = 111", "mono", "middle")) },

zBox:{ t:"The Z-box in the Z-algorithm", cap:"z[i] is how far the string starting at i agrees with the start of the string. At i = 4, s[4:7] = \"aab\" matches the prefix, so the Z-box becomes [4, 7). For positions inside the box, the value at the mirrored position i − l gives a head start, and only characters beyond r are compared again; that keeps the whole computation linear.", svg:S("v28", 200, "String cells with z-values and a highlighted Z-box",
  T(72, 74, "0", "sm", "middle") + B(47, 82, 50, 36, "sp", ["a"], true) + T(72, 140, "0", "mono", "middle") +
  T(128, 74, "1", "sm", "middle") + B(103, 82, 50, 36, "sp", ["a"], true) + T(128, 140, "1", "mono", "middle") +
  T(184, 74, "2", "sm", "middle") + B(159, 82, 50, 36, "sp", ["b"], true) + T(184, 140, "0", "mono", "middle") +
  T(240, 74, "3", "sm", "middle") + B(215, 82, 50, 36, "box", ["c"], true) + T(240, 140, "0", "mono", "middle") +
  T(296, 74, "4", "sm", "middle") + B(271, 82, 50, 36, "ka", ["a"], true) + T(296, 140, "3", "mono", "middle") +
  T(352, 74, "5", "sm", "middle") + B(327, 82, 50, 36, "ka", ["a"], true) + T(352, 140, "1", "mono", "middle") +
  T(408, 74, "6", "sm", "middle") + B(383, 82, 50, 36, "ka", ["b"], true) + T(408, 140, "0", "mono", "middle") +
  T(464, 74, "7", "sm", "middle") + B(439, 82, 50, 36, "box", ["x"], true) + T(464, 140, "0", "mono", "middle") +
  T(520, 74, "8", "sm", "middle") + B(495, 82, 50, 36, "box", ["a"], true) + T(520, 140, "2", "mono", "middle") +
  T(576, 74, "9", "sm", "middle") + B(551, 82, 50, 36, "box", ["a"], true) + T(576, 140, "2", "mono", "middle") +
  T(632, 74, "10", "sm", "middle") + B(607, 82, 50, 36, "box", ["a"], true) + T(632, 140, "1", "mono", "middle") +
  T(688, 74, "11", "sm", "middle") + B(663, 82, 50, 36, "box", ["z"], true) + T(688, 140, "0", "mono", "middle") +
  T(20, 140, "z", "b") + A("v28", "M352 80 Q240 34 132 80") + T(240, 40, "s[4:7] equals the prefix s[0:3]", "sm", "middle") +
  T(380, 186, "inside the box: z[i] ≥ min(r − i, z[i − l])", "mono", "middle")) },

ahoCorasick:{ t:"Aho-Corasick automaton for he, she, his, hers", cap:"Solid edges form the trie of the four patterns; checked nodes end a pattern. Dashed failure links point to the longest proper suffix that is also in the trie: from sh to h, and from she to he. Reaching she therefore also reports he, which is how one pass over the text finds every pattern, overlaps included.", svg:S("v29", 250, "Trie with failure links",
  B(8, 95, 64, 30, "pr", ["root"], false) + B(120, 65, 64, 30, "sp", ["h"], false) + B(228, 65, 64, 30, "okf", ["he ✓"], true) + B(338, 65, 64, 30, "sp", ["her"], false) + B(448, 65, 64, 30, "okf", ["hers ✓"], true) + B(228, 15, 64, 30, "sp", ["hi"], false) + B(338, 15, 64, 30, "okf", ["his ✓"], true) + B(120, 155, 64, 30, "sp", ["s"], false) + B(228, 155, 64, 30, "sp", ["sh"], false) + B(338, 155, 64, 30, "okf", ["she ✓"], true) +
  A("v29", "M72 104 L118 86") + A("v29", "M72 118 L118 164") + A("v29", "M184 80 L226 80") + A("v29", "M292 80 L336 80") + A("v29", "M402 80 L446 80") + A("v29", "M178 66 L232 40") + A("v29", "M292 30 L336 30") + A("v29", "M184 170 L226 170") + A("v29", "M292 170 L336 170") + A("v29", "M246 155 L168 97", "ln dash") + A("v29", "M356 155 L278 97", "ln dash") +
  T(560, 150, "dashed = failure link", "sm") + T(560, 172, "text \"ushers\" →", "sm mono") + T(560, 194, "she, he, hers", "sm b") +
  T(380, 238, "on a mismatch, follow failure links instead of restarting", "sm", "middle")) },

suffixArray:{ t:"Suffix array and LCP array of banana", cap:"The suffixes sorted lexicographically, with their start positions (the suffix array) and the longest common prefix with the previous suffix (the LCP array). Sorted order puts suffixes that share a prefix next to each other, so the maximum LCP, 3 for ana and anana, is the longest repeated substring.", svg:S("v30", 240, "Sorted suffixes table",
  '<rect class="fill" x="20" y="14" width="420" height="190" rx="12"/>' + T(60, 34, "rank", "sm b", "middle") + T(140, 34, "SA", "sm b", "middle") + T(200, 34, "suffix", "sm b") + T(400, 34, "LCP", "sm b", "middle") +
  T(60, 62, "0", "mono", "middle") + T(140, 62, "5", "mono", "middle") + T(200, 62, "a", "mono b") + T(400, 62, "0", "mono", "middle") +
  T(60, 90, "1", "mono", "middle") + T(140, 90, "3", "mono", "middle") + T(200, 90, "ana", "mono b") + T(400, 90, "1", "mono", "middle") +
  T(60, 118, "2", "mono", "middle") + T(140, 118, "1", "mono", "middle") + T(200, 118, "anana", "mono b") + T(400, 118, "3", "mono tS", "middle") +
  T(60, 146, "3", "mono", "middle") + T(140, 146, "0", "mono", "middle") + T(200, 146, "banana", "mono b") + T(400, 146, "0", "mono", "middle") +
  T(60, 174, "4", "mono", "middle") + T(140, 174, "4", "mono", "middle") + T(200, 174, "na", "mono b") + T(400, 174, "0", "mono", "middle") +
  T(60, 202, "5", "mono", "middle") + T(140, 202, "2", "mono", "middle") + T(200, 202, "nana", "mono b") + T(400, 202, "2", "mono", "middle") +
  B(480, 40, 260, 140, "sp", ["distinct substrings", "= n(n + 1)/2 − Σ LCP", "= 21 − 6 = 15", "", "longest repeated = \"ana\""], false) +
  T(380, 228, "SA = [5, 3, 1, 0, 4, 2]   LCP = [0, 1, 3, 0, 0, 2]", "mono", "middle")) },

consistentHash:{ t:"Consistent hashing on a ring", cap:"Servers and keys hash to points on the same ring, and each key belongs to the first server clockwise from it. Adding server D only takes over the keys between A and D (k1 and k2, previously owned by B); k3 and k4 keep their servers. With n servers, a change moves about 1/n of the keys instead of nearly all of them.", svg:S("v31", 290, "Ring with servers A to D and keys k1 to k4",
  '<path class="ln" d="M130 150 A100 100 0 1 0 330 150 A100 100 0 1 0 130 150"/>' + '<rect class="ka" x="274.0" y="57.4" width="12" height="12" rx="3"/>' + T(293, 45, "k1", "sm mono", "middle") + '<rect class="ka" x="323.0" y="130.1" width="12" height="12" rx="3"/>' + T(355, 136, "k2", "sm mono", "middle") + '<rect class="ka" x="215.3" y="243.6" width="12" height="12" rx="3"/>' + T(219, 280, "k3", "sm mono", "middle") + '<rect class="ka" x="133.4" y="101.7" width="12" height="12" rx="3"/>' + T(116, 101, "k4", "sm mono", "middle") + B(202.0, 36.0, 56, 28, "sp", ["A"], true) + B(288.6, 186.0, 56, 28, "sp", ["B"], true) + B(115.4, 186.0, 56, 28, "sp", ["C"], true) + B(283.9, 78.6, 56, 28, "okf", ["D"], true) +
  T(420, 70, "each key → first server clockwise", "b") +
  T(420, 104, "before D joins:  k1, k2 → B   k3 → C   k4 → A", "sm mono") +
  T(420, 130, "after D joins:   k1, k2 → D   k3 → C   k4 → A", "sm mono") +
  T(420, 170, "only the arc between A and D changes owner", "sm tS") +
  T(420, 196, "virtual nodes: many points per server", "sm") + T(420, 216, "even out the arc lengths", "sm")) },

bloomFilter:{ t:"A Bloom filter with three hash functions", cap:"Adding cat and dog sets three bits each. Querying fox finds bit 9 still 0, so fox was certainly never added. Querying emu finds all of its bits set by other items, a false positive. A Bloom filter can say no for sure but only probably yes, and more bits per item lower the false-positive rate.", svg:S("v32", 300, "Bit array with items setting bits and queries checking them",
  B(90, 26, 70, 28, "sp", ["cat"], true) + B(480, 26, 70, 28, "sp", ["dog"], true) +
  A("v32", "M125 54 L121 98") + A("v32", "M125 54 L331 98") + A("v32", "M125 54 L541 98") + A("v32", "M515 54 L205 98") + A("v32", "M515 54 L331 98") + A("v32", "M515 54 L667 98") +
  B(60, 120, 38, 34, "box", ["0"], false) + T(79, 112, "0", "sm", "middle") + B(102, 120, 38, 34, "ka", ["1"], false) + T(121, 112, "1", "sm", "middle") + B(144, 120, 38, 34, "box", ["0"], false) + T(163, 112, "2", "sm", "middle") + B(186, 120, 38, 34, "ka", ["1"], false) + T(205, 112, "3", "sm", "middle") + B(228, 120, 38, 34, "box", ["0"], false) + T(247, 112, "4", "sm", "middle") + B(270, 120, 38, 34, "box", ["0"], false) + T(289, 112, "5", "sm", "middle") + B(312, 120, 38, 34, "ka", ["1"], false) + T(331, 112, "6", "sm", "middle") + B(354, 120, 38, 34, "box", ["0"], false) + T(373, 112, "7", "sm", "middle") + B(396, 120, 38, 34, "box", ["0"], false) + T(415, 112, "8", "sm", "middle") + B(438, 120, 38, 34, "box", ["0"], false) + T(457, 112, "9", "sm", "middle") + B(480, 120, 38, 34, "box", ["0"], false) + T(499, 112, "10", "sm", "middle") + B(522, 120, 38, 34, "ka", ["1"], false) + T(541, 112, "11", "sm", "middle") + B(564, 120, 38, 34, "box", ["0"], false) + T(583, 112, "12", "sm", "middle") + B(606, 120, 38, 34, "box", ["0"], false) + T(625, 112, "13", "sm", "middle") + B(648, 120, 38, 34, "ka", ["1"], false) + T(667, 112, "14", "sm", "middle") + B(690, 120, 38, 34, "box", ["0"], false) + T(709, 112, "15", "sm", "middle") +
  B(90, 214, 70, 28, "box", ["fox?"], true) + B(560, 214, 70, 28, "box", ["emu?"], true) +
  A("v32", "M125 214 L121 158", "ln dash") + A("v32", "M125 214 L205 158", "ln dash") + A("v32", "M125 214 L457 158", "ln dash") + A("v32", "M595 214 L205 158", "ln dash") + A("v32", "M595 214 L541 158", "ln dash") + A("v32", "M595 214 L667 158", "ln dash") +
  T(125, 266, "bit 9 is 0: definitely not present", "sm tS", "middle") + T(595, 266, "all bits set: probably present", "sm tR", "middle") + T(595, 284, "(a false positive here)", "sm", "middle")) },

orientation:{ t:"The sign of the cross product", cap:"For vectors OA and OB, cross = (A − O) × (B − O). Positive means B lies to the left of the directed line from O through A (a counter-clockwise turn), negative means to the right (clockwise), and zero means the three points are collinear. The panels use the usual mathematical axes, with y pointing up.", svg:S("v33", 250, "Three orientation cases",
  B(26, 118, 28, 24, "pr", ["O"], true) + A("v33", "M54 130 L196 130") + T(206, 135, "A", "b") + A("v33", "M52 124 L140 40", "ln dash") + T(148, 44, "B", "b") + T(120, 222, "cross > 0", "b", "middle") + T(120, 240, "left turn, counter-clockwise", "sm", "middle") +
  B(281, 118, 28, 24, "pr", ["O"], true) + A("v33", "M309 130 L451 130") + T(461, 135, "A", "b") + A("v33", "M307 136 L395 195", "ln dash") + T(403, 199, "B", "b") + T(375, 222, "cross < 0", "b", "middle") + T(375, 240, "right turn, clockwise", "sm", "middle") +
  B(536, 168, 28, 24, "pr", ["O"], true) + A("v33", "M562 171 L707 62") + T(716, 65, "A", "b") + '<rect class="ka" x="624" y="114" width="12" height="12" rx="3"/>' + T(642, 138, "B", "b") + T(630, 222, "cross = 0", "b", "middle") + T(630, 240, "collinear", "sm", "middle")) },

convexHull:{ t:"Convex hull by monotone chain", cap:"The hull of the eight example points is the polygon through (0,0), (2,0), (3,3), (2,4) and (0,4); the other points lie inside. Monotone chain sorts the points, builds the lower hull left to right and the upper hull right to left, and pops any point that would make a clockwise turn.", svg:S("v34", 250, "Points with their convex hull outlined",
  '<path class="ln" d="M80 214 L220 214 L290 79 L220 34 L80 34 Z"/>' + '<rect class="ka" x="73" y="207" width="14" height="14" rx="4"/>' + '<rect class="sp" x="143" y="162" width="14" height="14" rx="4"/>' + '<rect class="sp" x="213" y="117" width="14" height="14" rx="4"/>' + '<rect class="ka" x="213" y="207" width="14" height="14" rx="4"/>' + '<rect class="ka" x="213" y="27" width="14" height="14" rx="4"/>' + '<rect class="ka" x="283" y="72" width="14" height="14" rx="4"/>' + '<rect class="ka" x="73" y="27" width="14" height="14" rx="4"/>' + '<rect class="sp" x="143" y="117" width="14" height="14" rx="4"/>' + T(90, 206, "0,0", "sm mono") + T(160, 161, "1,1", "sm mono") + T(230, 116, "2,2", "sm mono") + T(230, 206, "2,0", "sm mono") + T(230, 26, "2,4", "sm mono") + T(300, 71, "3,3", "sm mono") + T(90, 26, "0,4", "sm mono") + T(160, 116, "1,2", "sm mono") +
  T(420, 60, "1. sort points by x, then y", "sm") + T(420, 90, "2. lower hull, left to right", "sm") + T(420, 120, "3. upper hull, right to left", "sm") +
  T(420, 150, "pop while the last two points and", "sm") + T(420, 170, "the new one do not turn left", "sm") + T(420, 206, "O(n log n): the sort dominates", "sm tS")) },

twoPointerEnds:{ t:"Container with most water: two pointers from the ends", cap:"The water between two walls is limited by the shorter one. From any pair, moving the taller wall inward can only shrink the width without raising that limit, so the shorter wall is moved instead. Each step discards one wall for good, giving O(n). The best pair here holds 49.", svg:S("v35", 262, "Bar heights with the best container shaded",
  '<rect class="fill" x="172" y="70" width="464" height="140" rx="2"/>' +
  '<rect class="box" x="60" y="190" width="40" height="20" rx="3"/>' + T(80, 228, "1", "sm mono", "middle") +
  '<rect class="ka" x="132" y="50" width="40" height="160" rx="3"/>' + T(152, 228, "8", "sm mono", "middle") +
  '<rect class="box" x="204" y="90" width="40" height="120" rx="3"/>' + T(224, 228, "6", "sm mono", "middle") +
  '<rect class="box" x="276" y="170" width="40" height="40" rx="3"/>' + T(296, 228, "2", "sm mono", "middle") +
  '<rect class="box" x="348" y="110" width="40" height="100" rx="3"/>' + T(368, 228, "5", "sm mono", "middle") +
  '<rect class="box" x="420" y="130" width="40" height="80" rx="3"/>' + T(440, 228, "4", "sm mono", "middle") +
  '<rect class="box" x="492" y="50" width="40" height="160" rx="3"/>' + T(512, 228, "8", "sm mono", "middle") +
  '<rect class="box" x="564" y="150" width="40" height="60" rx="3"/>' + T(584, 228, "3", "sm mono", "middle") +
  '<rect class="ka" x="636" y="70" width="40" height="140" rx="3"/>' + T(656, 228, "7", "sm mono", "middle") +
  T(152, 250, "lo", "b", "middle") + T(656, 250, "hi", "b", "middle") + T(404, 140, "area = 7 × min(8, 7) = 49", "b", "middle")) },

prefixHash:{ t:"Subarray sum equals k with prefix sums", cap:"Prefix i is the sum of the first i numbers, so the subarray between prefixes i and j sums to prefix[j] − prefix[i]. When prefix 6 equals 14 and k = 7, the map shows that 14 − 7 = 7 was already seen as prefix 2, so nums[2:6] = 7 + 2 − 3 + 1 sums to 7. Counting how often each prefix value occurred gives every match in one pass, negatives included.", svg:S("v36", 290, "Array and its prefix sums with one matching pair",
  T(20, 84, "nums", "b") + T(20, 174, "prefix", "b") + T(20, 210, "index", "sm") +
  B(110, 60, 60, 36, "box", ["3"], false) +
  B(180, 60, 60, 36, "box", ["4"], false) +
  B(250, 60, 60, 36, "ka", ["7"], false) +
  B(320, 60, 60, 36, "ka", ["2"], false) +
  B(390, 60, 60, 36, "ka", ["-3"], false) +
  B(460, 60, 60, 36, "ka", ["1"], false) +
  B(530, 60, 60, 36, "box", ["4"], false) +
  B(600, 60, 60, 36, "box", ["2"], false) +
  B(75, 150, 60, 36, "sp", ["0"], false) + T(105, 206, "0", "sm", "middle") +
  B(145, 150, 60, 36, "sp", ["3"], false) + T(175, 206, "1", "sm", "middle") +
  B(215, 150, 60, 36, "okf", ["7"], false) + T(245, 206, "2", "sm", "middle") +
  B(285, 150, 60, 36, "sp", ["14"], false) + T(315, 206, "3", "sm", "middle") +
  B(355, 150, 60, 36, "sp", ["16"], false) + T(385, 206, "4", "sm", "middle") +
  B(425, 150, 60, 36, "sp", ["13"], false) + T(455, 206, "5", "sm", "middle") +
  B(495, 150, 60, 36, "okf", ["14"], false) + T(525, 206, "6", "sm", "middle") +
  B(565, 150, 60, 36, "sp", ["18"], false) + T(595, 206, "7", "sm", "middle") +
  B(635, 150, 60, 36, "sp", ["20"], false) + T(665, 206, "8", "sm", "middle") +
  A("v36", "M525 188 Q385 250 247 190") + T(385, 262, "14 − 7 = 7 seen before → nums[2:6] sums to 7", "sm", "middle") + T(385, 282, "keep a dict: prefix value → count", "sm mono", "middle")) },

twoHeaps:{ t:"Running median with two heaps", cap:"The max-heap holds the smaller half and the min-heap the larger half, with every value on the left no bigger than every value on the right and sizes differing by at most one. The median is read from the tops in O(1); each insertion costs O(log n).", svg:S("v18", 220, "Lower half in a max-heap, upper half in a min-heap",
  '<rect class="sp" x="40" y="40" width="300" height="120" rx="14"/>' + T(190, 64, "max-heap: lower half", "b", "middle") + T(190, 100, "1  3  [5] ← top", "mono", "middle") + T(190, 130, "largest of the small values", "sm", "middle") +
  '<rect class="ka" x="420" y="40" width="300" height="120" rx="14"/>' + T(570, 64, "min-heap: upper half", "b", "middle") + T(570, 100, "top → [8]  12  15", "mono", "middle") + T(570, 130, "smallest of the large values", "sm", "middle") +
  T(380, 105, "≤", "b", "middle") +
  T(380, 196, "6 values: median = (5 + 8) / 2 = 6.5", "b", "middle")) }
});

