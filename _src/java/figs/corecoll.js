Object.assign(FIGS, {
corecoll_exc:{ t:"The exception hierarchy", cap:"Everything you can throw extends Throwable. Errors are JVM-level problems you do not catch. Under Exception, RuntimeException and its subclasses are unchecked; every other Exception is checked, so the compiler forces you to catch or declare it.", svg:S("corecoll1", 300, "Throwable splits into Error and Exception; Exception splits into checked exceptions and RuntimeException",
  B(300, 14, 160, 44, "sp", ["Throwable", "message, cause, trace"]) +
  A("corecoll1", "M350 58 L170 96") + A("corecoll1", "M410 58 L560 96") +
  B(70, 98, 200, 48, "pr", ["Error", "unchecked: do not catch"]) +
  B(460, 98, 200, 48, "fill", ["Exception"]) +
  A("corecoll1", "M120 146 L120 186") +
  B(20, 188, 200, 84, "pr", ["OutOfMemoryError", "StackOverflowError", "NoClassDefFoundError", ""], false) +
  A("corecoll1", "M510 146 L400 186") + A("corecoll1", "M610 146 L640 186") +
  B(290, 188, 220, 84, "ka", ["Checked", "IOException, SQLException", "InterruptedException", "must catch or declare"]) +
  B(540, 188, 210, 84, "st", ["RuntimeException", "NullPointerException", "IllegalArgumentException", "unchecked: usually bugs"]) +
  T(380, 292, "javac enforces the checked rule; the JVM treats all of them alike", "sm", "middle")) },

corecoll_coll:{ t:"The collections hierarchy", cap:"List, Set and Queue extend Collection, which extends Iterable. Map stands apart and offers collection views instead. Java 21 added the Sequenced interfaces (shaded) for every type with a defined order; SequencedSet also extends SequencedCollection. The second line in each box names the main classes.", svg:S("corecoll2", 400, "Collection and Map interface hierarchy with Java 21 sequenced interfaces and main implementations",
  B(20, 14, 140, 40, "box", ["Iterable"]) + A("corecoll2", "M160 34 L218 34") +
  B(220, 14, 140, 40, "box", ["Collection"]) +
  A("corecoll2", "M300 54 L320 94") +
  B(250, 96, 190, 36, "sp", ["SequencedCollection"]) + T(450, 119, "Java 21", "sm", "start") +
  A("corecoll2", "M240 54 L90 168") + A("corecoll2", "M350 54 L640 168") +
  A("corecoll2", "M300 132 L265 168") + A("corecoll2", "M400 132 L455 168") +
  B(10, 170, 150, 52, "box", ["Set", "HashSet"]) +
  B(180, 170, 170, 52, "fill", ["List", "ArrayList, LinkedList"]) +
  B(370, 170, 170, 52, "fill", ["Deque", "ArrayDeque"]) +
  B(560, 170, 190, 52, "box", ["Queue", "PriorityQueue"]) + A("corecoll2", "M560 196 L542 196") +
  A("corecoll2", "M85 222 L85 248") +
  B(10, 250, 150, 52, "sp", ["SequencedSet", "LinkedHashSet"]) + A("corecoll2", "M85 302 L85 328") +
  B(10, 330, 150, 52, "sp", ["SortedSet", "TreeSet"]) +
  B(200, 270, 150, 52, "box", ["Map", "HashMap"]) + A("corecoll2", "M350 296 L378 296") +
  B(380, 270, 170, 52, "sp", ["SequencedMap", "LinkedHashMap"]) + A("corecoll2", "M550 296 L578 296") +
  B(580, 270, 160, 52, "sp", ["SortedMap", "TreeMap"]) +
  T(470, 356, "Map is not a Collection: keySet(), values() and", "sm", "middle") +
  T(470, 374, "entrySet() are collection views of it", "sm", "middle")) },

corecoll_lists:{ t:"ArrayList vs LinkedList in memory", cap:"An ArrayList keeps references side by side in one array with spare capacity, so get(i) jumps straight to a slot. A LinkedList wraps each element in a node with prev and next links, so reaching index i means following links one by one.", svg:S("corecoll3", 250, "ArrayList backing array compared with LinkedList nodes",
  T(20, 30, "ArrayList: elementData (size 4, capacity 6)", "b", "start") +
  B(20, 44, 70, 40, "fill", ["[0] pen"], false) + B(90, 44, 70, 40, "fill", ["[1] ink"], false) +
  B(160, 44, 70, 40, "fill", ["[2] box"], false) + B(230, 44, 70, 40, "fill", ["[3] tape"], false) +
  B(300, 44, 70, 40, "box", ["empty"], false) + B(370, 44, 70, 40, "box", ["empty"], false) +
  T(470, 60, "get(2): one address calculation, O(1)", "sm", "start") +
  T(470, 78, "full? copy into a 1.5x larger array", "sm", "start") +
  T(20, 128, "LinkedList: first, last, size", "b", "start") +
  B(20, 144, 140, 64, "st", ["Node", "prev | pen | next"]) +
  B(210, 144, 140, 64, "st", ["Node", "prev | ink | next"]) +
  B(400, 144, 140, 64, "st", ["Node", "prev | box | next"]) +
  B(590, 144, 140, 64, "st", ["Node", "prev | tape | next"]) +
  A("corecoll3", "M160 166 L208 166") + A("corecoll3", "M210 188 L162 188") +
  A("corecoll3", "M350 166 L398 166") + A("corecoll3", "M400 188 L352 188") +
  A("corecoll3", "M540 166 L588 166") + A("corecoll3", "M590 188 L542 188") +
  T(380, 236, "get(2): follow next links from the nearer end, O(n); about 24 extra bytes per node", "sm", "middle")) },

corecoll_hashmap:{ t:"Inside a HashMap", cap:"put hashes the key, spreads the high bits into the low bits, and masks with (n - 1) to pick a bucket. Colliding keys share a bucket as a linked chain; a chain of 8 in a table of at least 64 becomes a red-black tree. Past 0.75 full, the table doubles.", svg:S("corecoll4", 330, "HashMap put path: hashCode, spread, mask, buckets with a chain and a treeified bin",
  B(10, 20, 150, 50, "box", ["key.hashCode()", "\"Aa\" = 2112"]) + A("corecoll4", "M160 45 L196 45") +
  B(198, 20, 170, 50, "box", ["spread", "h ^ (h &gt;&gt;&gt; 16)"]) + A("corecoll4", "M368 45 L404 45") +
  B(406, 20, 170, 50, "box", ["bucket index", "hash &amp; (n - 1)"]) +
  A("corecoll4", "M490 70 L94 112", "acc") +
  T(600, 40, "n = 16, 32, 64...", "sm mono", "start") + T(600, 58, "always a power of 2", "sm", "start") +
  B(20, 106, 70, 34, "fill", ["[0]"], false) + B(20, 142, 70, 34, "box", ["[1]"], false) +
  B(20, 178, 70, 34, "fill", ["[2]"], false) + B(20, 214, 70, 34, "box", ["..."], false) +
  B(20, 250, 70, 34, "fill", ["[15]"], false) +
  A("corecoll4", "M90 123 L128 123") + B(130, 108, 120, 30, "st", ["Aa = 1"], false) +
  A("corecoll4", "M250 123 L288 123") + B(290, 108, 120, 30, "st", ["BB = 2"], false) +
  T(430, 128, "collision: shared bucket, equals() finds the node", "sm", "start") +
  A("corecoll4", "M90 195 L128 195") + B(130, 180, 120, 30, "st", ["PEN-01 = 7"], false) +
  A("corecoll4", "M90 267 L148 267") +
  B(150, 250, 34, 30, "ka", ["T"], false) + B(214, 250, 34, 30, "ka", ["T"], false) + B(278, 250, 34, 30, "ka", ["T"], false) +
  A("corecoll4", "M184 265 L212 265", "kac") + A("corecoll4", "M248 265 L276 265", "kac") +
  T(330, 262, "8+ nodes and n &gt;= 64: red-black tree, O(log n)", "sm", "start") +
  T(330, 280, "shrinks to 6 or fewer: back to a list", "sm", "start") +
  T(380, 316, "size &gt; 0.75 n: double n; each node stays at i or moves to i + old n", "sm", "middle")) },
});
