# Brief: write a Core Java section in full depth

You are writing one section of a static, single-page study guide, "Java full stack: career switch guide". It is for someone moving from a data reconciliation support role into Java full stack development, preparing for interviews. Core Java is the part of the guide taught in full depth; every article is written from scratch. The format is the one used in the companion Python course: read `model_setup.md` (a finished Python section) in full before writing and match its structure, tone and depth.

Working directory (J): `/Users/dheemanth/Desktop/projects/gdsdheemanth.github.io/_src/java`. Everything below is relative to it.

## What to produce

For the section id `<sid>` you are given: `course/<sid>.md` (required) and `figs/<sid>.js` (2 to 5 diagrams where a picture genuinely helps). Do not edit any other file, do not run `build.py` or `make_java.py`, do not commit.

Get the plan with `python3 skel.py <sid>`: groups and the **exact** topic titles in order (match them character for character), a scope note for some topics, and the previous and next sections.

## File format (same dialect as model_setup.md)

1. **Section intro** (text before the first `##`): who it is for and what it assumes (earlier sections), "What you will be able to do afterwards" (bullets), the big-picture mental model. Then one line: "Each topic follows the usual order: plain words, explanation, Go deeper, Check yourself, and resources to learn more."
2. **Groups**: `## <group title>` then a 1 to 3 paragraph bridge.
3. **Topics**: `### <topic title>`, then in this order:
   - `::: plain` ... `:::` everyday-language explanation with an analogy, 2 to 5 sentences. Required.
   - **The explanation**: what it is, why it exists, how it works inside the JVM or library, with runnable Java examples that build from simple to realistic. Define jargon on first use. `> **Pitfall:**` and `> **Tip:**` callouts. Tables for comparisons.
   - `::: deeper` ... `:::` internals (bytecode, JVM, JDK source behaviour), edge cases, performance, version notes (Java 8 → 21 → 25), and `> **Interview:**` callouts with model answers. Include in nearly every topic.
   - `::: check` ... `:::` 2 or 3 `?? question` lines, each followed directly by its answer (no blank line between them; a blank line between questions). Required.
   - `::: resources` ... `:::` 4 to 7 resources, one per line: `- Type: [Title](https://url) · short note` with Type in `Book`, `Docs`, `Course`, `Video`, `Article`, `Blog`, `Repo`, `Spec`. Prefer dev.java, docs.oracle.com (Java SE 21/25 API docs and the JLS), openjdk.org JEPs, Baeldung, Jenkov, the books in the original guide (Effective Java, Head First Java, Modern Java in Action, Java Concurrency in Practice). A book without a good URL may be `- Book: *Title* by Author · note`.
   - Length: about 1,000 to 1,300 words per topic before resources.
4. **`## Wrap-up`** with `### Cheat sheet` (two-column table, 10 to 20 rows), `### Practice` (3 to 5 exercises in `::: try` blocks, each followed by `?? Answer to N` or `?? A solution to N`; a solution may be a fenced code block after a blank line; at least one should be a typical live-coding interview task), and `### What comes next` (one paragraph naming the next section by its exact title).

Markdown supported: `##`/`###`/`####`, paragraphs, `- ` and `1. ` lists, fenced code with a language (```java, ```bash, ```xml, ```groovy, ```text), `|` tables (escape a literal pipe as `\|`), callouts, `?? question` + answer, `::: plain|deeper|check|try|resources` blocks closed by `:::`, `[[fig:key]]` alone on a line, inline `code`, **bold**, *italic*, [links](https://...). Nothing else: no HTML. In prose, write generic types inside backticks (`List<String>`), never bare angle brackets. Never write the literal text `</script`.

## Code: every example must compile and run

A JDK 21 is installed at `/opt/homebrew/opt/openjdk@21/bin` (not on PATH; use the full path). Java 21 is the target; mention Java 25 features as version notes only (do not use preview features in runnable code).

- Write examples as complete programs where practical (a class with `main`, or several top-level classes in one file) so the reader can run them. Short fragments are fine for illustration when clearly incomplete, but anything showing output must be a complete program.
- Verify every complete example: save it in a temp folder (for example `/tmp/jcheck/<sid>/`) and run it with single-file launch: `/opt/homebrew/opt/openjdk@21/bin/java Main.java` (the public class name must match the file, or use no public class). Comments that show output (`// prints 3`, or an `// Output:` block) must match the real output exactly.
- Examples that intentionally fail to compile or throw must say so, and you must confirm the error message you quote.
- Use `jshell` (`/opt/homebrew/opt/openjdk@21/bin/jshell`) if helpful, and `javap -c` to show bytecode in Go deeper.

## Diagrams

`figs/<sid>.js`:

```js
Object.assign(FIGS, {
<sid>_name:{ t:"Short title", cap:"One or two sentences on what the reader should see.", svg:S("<sid>1", 260, "aria label",
  B(20, 40, 160, 50, "sp", ["Box title", "small second line"]) + A("<sid>1", "M180 65 L300 65") + T(380, 30, "label", "sm", "middle")) },
});
```

Helpers (see `figs_helpers.js`): `S(markerId, height, ariaLabel, body)` 760-wide SVG; `B(x, y, w, h, cls, [lines], bold)` box; `A(markerId, "M.. L..", cls)` arrow (`ln`, `acc`, `kac`, `stc`, plus `dash`); `T(x, y, text, cls, anchor)` text (`sm`, `b`, `mono`, `tR`, `tA`, `tK`, `tS`, `tP`). Box classes `box`, `fill`, `sp`, `st`, `ka`, `pr`, `okf`. Unique marker id per figure (`<sid>1`, `<sid>2`...). Keys start with `<sid>_`; reference each exactly once with `[[fig:<sid>_name]]`. Keep box text short (about 7 px per character). Good candidates: JVM pipeline (javac → bytecode → class loader → interpreter/JIT), stack vs heap references, string pool, inheritance and dynamic dispatch, exception hierarchy, HashMap buckets and treeification, collection hierarchy, stream pipeline. Load test: `node -e "eval(require('fs').readFileSync('figs_helpers.js','utf8')+require('fs').readFileSync('figs/<sid>.js','utf8')); console.log(Object.keys(FIGS))"`.

## Quality rules

- Correct for Java 21 (current LTS 25, released September 2025). When unsure of a detail, check the official docs or run it; never invent APIs or defaults.
- Interview focus: these topics are asked in Java interviews (pass-by-value, String pool, equals/hashCode, HashMap internals, fail-fast iterators, PECS...). Each topic should leave the reader able to answer the common questions confidently, with Interview callouts giving model answers.
- Every URL must be real; verify with `python3 linkcheck.py course/<sid>.md` and fix every link in `bad`. `blocked` links (oreilly.com, medium and similar) are fine only if you are confident they exist.
- Style: plain, direct English; short sentences; no em dashes (use colons, commas or parentheses); no filler.
- Write the file in parts (append group by group) so nothing is lost if you are interrupted.

## Checks before finishing

1. `python3 decheck.py <sid>`: 0 problems.
2. `python3 linkcheck.py course/<sid>.md`: no `bad` links.
3. Figures load with the node command; each key referenced exactly once.
4. Every complete Java example compiled and run, output comments exact.

## Report back (short)

Topics, words, figures, check results (including how many Java programs you ran), blocked links kept, anything unresolved.
