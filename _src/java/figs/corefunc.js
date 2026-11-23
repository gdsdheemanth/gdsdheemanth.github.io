/* corefunc: Core Java 3 diagrams, original drawings */
Object.assign(FIGS, {
corefunc_pipe:{ t:"A stream pipeline", cap:"A source feeds a chain of lazy intermediate operations; nothing runs until the terminal operation pulls elements through, one at a time, from left to right. A short-circuiting terminal such as findFirst can stop the source early.", svg:S("corefunc1", 250, "Source, then filter, map and sorted as lazy intermediate operations, then a terminal collect that triggers execution",
  B(10, 60, 130, 60, "sp", ["Source", "list.stream()"]) + A("corefunc1", "M140 90 L170 90") +
  B(172, 60, 120, 60, "fill", ["filter", "keep PAID"]) + A("corefunc1", "M292 90 L322 90") +
  B(324, 60, 120, 60, "fill", ["map", "order to id"]) + A("corefunc1", "M444 90 L474 90") +
  B(476, 60, 120, 60, "pr", ["sorted", "stateful: buffers"]) + A("corefunc1", "M596 90 L626 90") +
  B(628, 60, 122, 60, "ka", ["collect", "terminal: runs it"]) +
  T(380, 36, "intermediate operations: lazy, each returns a new Stream", "sm", "middle") +
  A("corefunc1", "M690 140 C690 200 80 200 75 128", "acc dash") +
  T(380, 216, "the terminal operation pulls each element through every stage", "sm", "middle") +
  T(380, 238, "result: List, Map, number, Optional or a side effect", "sm", "middle")) },

corefunc_group:{ t:"groupingBy with a downstream collector", cap:"groupingBy sends each element to a bucket chosen by the classifier (here the city). Each bucket has its own copy of the downstream collector (here summing the amount), and the result is a map from key to that collector's result.", svg:S("corefunc2", 290, "Sales flow into buckets per city, each bucket sums its amounts, producing a map of city to total",
  B(10, 20, 170, 34, "box", ["Pune 147"], false) + B(10, 62, 170, 34, "box", ["Delhi 598"], false) +
  B(10, 104, 170, 34, "box", ["Pune 210"], false) + B(10, 146, 170, 34, "box", ["Chennai 490"], false) +
  B(10, 188, 170, 34, "box", ["Delhi 98"], false) + B(10, 230, 170, 34, "box", ["Pune 899"], false) +
  B(230, 100, 140, 70, "sp", ["classifier", "Sale::city"]) +
  A("corefunc2", "M180 37 L230 120") + A("corefunc2", "M180 79 L230 128") + A("corefunc2", "M180 121 L230 134") +
  A("corefunc2", "M180 163 L230 140") + A("corefunc2", "M180 205 L230 146") + A("corefunc2", "M180 247 L230 152") +
  A("corefunc2", "M370 120 L420 50") + A("corefunc2", "M370 135 L420 135") + A("corefunc2", "M370 150 L420 220") +
  B(422, 20, 160, 60, "fill", ["Pune bucket", "sum: 1256"]) +
  B(422, 105, 160, 60, "fill", ["Delhi bucket", "sum: 696"]) +
  B(422, 190, 160, 60, "fill", ["Chennai bucket", "sum: 490"]) +
  A("corefunc2", "M582 135 L620 135") +
  B(622, 90, 130, 90, "ka", ["Map result", "Pune=1256", "Delhi=696", "Chennai=490"]) +
  T(502, 278, "downstream collector runs per bucket", "sm", "middle")) },

corefunc_time:{ t:"Choosing a java.time type", cap:"An Instant is one point on the UTC timeline. Add a zone and it becomes a ZonedDateTime, which a person can read on a local calendar. Drop the zone and you get a LocalDateTime, which no longer identifies a moment. Store Instants; convert at the edges.", svg:S("corefunc3", 280, "Instant converts to ZonedDateTime with a zone; ZonedDateTime drops to LocalDateTime and LocalDate; LocalDateTime needs a zone to go back",
  B(20, 100, 180, 70, "ka", ["Instant", "UTC timeline point", "store, log, compare"]) +
  A("corefunc3", "M200 120 L300 120") + T(250, 110, "atZone(zone)", "sm mono", "middle") +
  A("corefunc3", "M300 150 L200 150") + T(250, 168, "toInstant()", "sm mono", "middle") +
  B(302, 90, 190, 90, "sp", ["ZonedDateTime", "date + time + region", "DST rules applied", "display, scheduling"]) +
  A("corefunc3", "M492 120 L582 70") + T(520, 78, "toLocalDateTime()", "sm mono", "middle") +
  A("corefunc3", "M492 160 L582 210") + T(520, 210, "toLocalDate()", "sm mono", "middle") +
  B(584, 40, 166, 60, "pr", ["LocalDateTime", "no zone: not a moment"]) +
  B(584, 180, 166, 60, "fill", ["LocalDate", "birthdays, invoices"]) +
  A("corefunc3", "M660 40 C660 10 400 10 397 88", "acc dash") + T(530, 18, "atZone(zone) to go back", "sm", "middle") +
  T(110, 200, "Duration: exact seconds", "sm", "middle") + T(110, 220, "Period: calendar days, months", "sm", "middle") +
  T(400, 270, "rule of thumb: Instant in the database, ZonedDateTime at the user", "sm", "middle")) },

corefunc_mvn:{ t:"The Maven default lifecycle", cap:"Running a phase runs every phase before it, so mvn install also validates, compiles, tests, packages and verifies. Plugins bind their goals to phases; clean is a separate lifecycle you add in front.", svg:S("corefunc4", 230, "Maven phases validate, compile, test, package, verify, install, deploy with the plugin goal bound to each",
  B(10, 70, 92, 50, "box", ["validate"]) + A("corefunc4", "M102 95 L116 95") +
  B(118, 70, 92, 50, "fill", ["compile"]) + A("corefunc4", "M210 95 L224 95") +
  B(226, 70, 92, 50, "fill", ["test"]) + A("corefunc4", "M318 95 L332 95") +
  B(334, 70, 92, 50, "fill", ["package"]) + A("corefunc4", "M426 95 L440 95") +
  B(442, 70, 92, 50, "fill", ["verify"]) + A("corefunc4", "M534 95 L548 95") +
  B(550, 70, 92, 50, "ka", ["install"]) + A("corefunc4", "M642 95 L656 95") +
  B(658, 70, 92, 50, "st", ["deploy"]) +
  T(164, 140, "compiler", "sm mono", "middle") + T(272, 140, "surefire", "sm mono", "middle") +
  T(380, 140, "jar", "sm mono", "middle") + T(488, 140, "failsafe", "sm mono", "middle") +
  T(596, 140, "to ~/.m2", "sm mono", "middle") + T(704, 140, "to Nexus", "sm mono", "middle") +
  T(380, 40, "mvn install runs everything from validate up to install", "sm", "middle") +
  A("corefunc4", "M10 170 L640 170", "acc") +
  T(380, 196, "goals bound by the packaging type (jar) and by plugins you add", "sm", "middle") +
  T(380, 218, "clean lifecycle (delete target/) runs first in: mvn clean install", "sm", "middle")) },
});
