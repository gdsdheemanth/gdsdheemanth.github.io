This is where the course starts, and it assumes you have never programmed before. By the end of the section you will have Python installed the way professionals install it, you will know how to run code three different ways, and you will understand the handful of ideas that every later section builds on: names that point to objects, what counts as true or false, how comparisons work, and how a program decides what to do next.

**What you will be able to do afterwards**

- Install Python, create a project with its own isolated packages, and run and debug code in an editor.
- Read and write simple programs: variables, printing and reading text, arithmetic, `if` decisions and loops.
- Explain why changing a list through one variable can change it "through" another, which is the most common beginner surprise in Python.
- Use the newer tools that make code shorter and clearer: f-strings, `:=` and `match`.

**How each topic is laid out**

Every topic follows the same order, so you always know where you are:

1. **In plain words**: the idea in everyday language, with an analogy. If you are new, read this first and slowly.
2. **The explanation**: how it really works, with code you can run. Type the examples in yourself rather than copying them; your fingers learn too.
3. **Pitfalls and tips** in coloured boxes: the mistakes people actually make.
4. **Go deeper** (closed by default): internals, edge cases and interview angles. Skip it on a first read and come back later.
5. **Check yourself**: two or three questions. Answer them in your head before opening the answer.

> **Tip:** Keep a Python prompt open while you read (the first group shows you how). Whenever you are unsure what something does, try it: the interpreter is always the final authority.

## Environment

Before writing any real code you need three things: **an interpreter** (the program that runs Python code), **an isolated place for each project's packages** (so projects don't break each other), and **an editor** that runs and debugs your code. This group sets up all three. It is a little dry, but you only do it once, and a good setup saves you hours of confusion later.

### Installing CPython (python.org installers, `pyenv`, `uv python install`)

::: plain
Python code is just text. To run it you need an **interpreter**: a program that reads your text and carries out the instructions. The standard interpreter is called **CPython** (because it is written in the C language), and when anyone says "install Python" this is what they mean. Installing it is like installing any other app, with one rule: leave the copy of Python that came with your operating system alone, because the system itself uses it.
:::

"Python" the language has several implementations, but **CPython**, written in C and maintained by the core developers, is the reference one: every new feature lands there first, and nearly all third-party packages are built and tested against it.

There are three sensible ways to get it:

- **python.org installers** (Windows and macOS). They are the safest choice for beginners. On Windows, tick "Add python.exe to PATH" during installation; you also get the `py` launcher, which picks between installed versions.
- **pyenv** (macOS and Linux) builds versions from source and switches between them per folder with a `.python-version` file. It is flexible but slow, and needs build tools installed.
- **uv** downloads ready-made interpreters in seconds and is now the fastest way to manage versions:

```bash
uv python install 3.15 3.14       # install two versions side by side
uv python list                    # show installed and available versions
uv run --python 3.14 script.py    # run a script with a specific version
```

After installing, check that it works. Open a terminal (Terminal on macOS, PowerShell on Windows) and run:

```bash
python3 --version                 # Windows: py --version
python3 -c "import sys; print(sys.executable)"   # which interpreter actually ran
```

> **Pitfall:** On Linux and macOS the operating system ships its own Python that system tools depend on. Never `sudo pip install` into it or upgrade it in place. Install your own interpreter alongside it and use virtual environments (below).

> **Pitfall:** Having several Pythons installed is normal, but always know which one runs. `which python3` (macOS and Linux), `py -0` (Windows) and `sys.executable` tell you.

::: deeper
**Why "which Python runs" can surprise you.** When you type `python3`, your shell searches the folders listed in the `PATH` environment variable, in order, and runs the first match. Installing a new version, activating a virtual environment or opening a different terminal can change that order. That is why the course always writes `python -m pip` and `python -m venv`: the module runs under the exact interpreter you named.

**Release cycle.** A new minor version (3.14, 3.15...) ships every October. Each gets about two years of bug fixes and then security fixes only, for five years in total. At the time of writing 3.15 is current and 3.10 is the oldest supported version. Libraries usually support the last four or five versions, so production projects often run one version behind the newest.

**Other implementations** exist: PyPy (a JIT compiler, often much faster for pure-Python loops), MicroPython (microcontrollers), GraalPy and others. Since 3.13 CPython also comes in an optional free-threaded build without the global interpreter lock; the Concurrency part covers it.
:::

::: check
?? What is CPython?
The reference implementation of Python, written in C. It is what "Python" means unless someone names another implementation, and it is where new language features land first.

?? Why should you not install packages into your operating system's Python?
The operating system's own tools depend on it. Changing its packages can break those tools, so you install your own interpreter and use virtual environments.

?? How can you find out which interpreter actually runs your code?
Print `sys.executable`, or use `which python3` on macOS and Linux and `py -0` on Windows.
:::

### The REPL (improved REPL in 3.13), IPython and Jupyter

::: plain
The **REPL** is a conversation with Python. You type one line, press Enter, and Python immediately answers. The name stands for read, evaluate, print, loop: it **reads** what you typed, **evaluates** (runs) it, **prints** the result and **loops** back for the next line. It is the fastest way to try an idea or check what something does, like a calculator that understands the whole language.
:::

Start it by running `python3` (or `py` on Windows) with no arguments. The `>>>` prompt means Python is waiting for you; `...` means it is waiting for the rest of a multi-line block. When you type an expression, the REPL shows its value; when you type a statement such as an assignment, it shows nothing.

```python
>>> 2 + 3
5
>>> name = "Ada"
>>> name.upper()
'ADA'
>>> import statistics
>>> statistics.median([3, 1, 4, 1, 5])
3
>>> help(len)        # built-in help for any function
```

Two built-ins make the REPL a learning tool: `help(thing)` shows documentation, and `dir(thing)` lists everything the thing can do. Leave with `exit` or Ctrl-D (Ctrl-Z then Enter on Windows).

Since **3.13** the default REPL is much more comfortable: you can edit multi-line blocks with the arrow keys, browse whole blocks in history, press **F1** for help, **F2** for history without prompts and **F3** for paste mode, which accepts a pasted block without mangling its indentation. 3.14 added syntax colouring and completion of module names after `import`.

**IPython** is an enhanced shell (`pip install ipython`): better completion, `thing?` for documentation, `thing??` for source code, and **magics** such as `%timeit` to time a snippet and `%debug` to inspect a crash.

**Jupyter** notebooks run Python in a browser page made of cells: code cells, their output (tables, charts) and text notes. They are excellent for exploring data and for learning.

> **Pitfall:** A notebook remembers everything you ran, in the order you ran it, not the order on the page. After a few hours the page and the program state no longer match. Use "Restart and run all" before trusting a result, and move finished code into normal `.py` files.

::: deeper
- `_` holds the last result in the REPL (`>>> _ * 2`). In IPython, `Out[3]` holds the result of cell 3.
- `python -i script.py` runs a script and then leaves you at the prompt with all its variables available: handy for poking at results or at the state after a crash.
- Set the environment variable `PYTHON_BASIC_REPL=1` to get the old, simpler REPL back (useful in some terminals and editors).
- The REPL shows `repr(value)`, the "developer" view of an object, which is why strings appear with quotes. `print` shows `str(value)`, the "user" view. The Data model section explains both methods.
- Useful IPython magics: `%run file.py`, `%load_ext autoreload` with `%autoreload 2` (re-import edited modules automatically), `%pdb on` (open the debugger on every exception), `%%time` for a whole cell.
:::

::: check
?? What does REPL stand for, and what is it for?
Read, evaluate, print, loop. It runs one line or block at a time and shows the result, so it is ideal for experiments and for checking how something behaves.

?? Why does `>>> name` show `'Ada'` with quotes while `print(name)` shows `Ada`?
The REPL displays `repr(value)`, the unambiguous developer view; `print` displays `str(value)`, the readable view.

?? What is the main risk of Jupyter notebooks?
Hidden state: cells can be run out of order, so the variables in memory may not match the code shown on the page.
:::

### Running scripts vs modules with `python -m`

::: plain
For anything longer than a few lines you write a **script**: a text file ending in `.py`, which you hand to Python to run from top to bottom. There are two ways to hand it over. `python app.py` says "run this file". `python -m some.module` says "find this module the way an `import` statement would, then run it". For a single file both work; the difference appears once your code is split into packages, and it is also how you run tools that come with Python.
:::

Create a file `hello.py`:

```python
name = "world"
print(f"Hello, {name}!")
```

and run it from the folder that contains it:

```bash
python3 hello.py           # Hello, world!
```

When Python runs a file it executes every line in order, top to bottom. There is no special "main" function you must write; the file itself is the program.

`python -m package.module` asks the **import system** to find the module, then runs it. That keeps the module's place inside its package, so imports between files of the same package work:

```bash
# project/
#   shop/__init__.py
#   shop/models.py
#   shop/cli.py        (contains: from .models import Order)
cd project
python shop/cli.py        # ImportError: attempted relative import with no known parent package
python -m shop.cli        # works
```

`-m` is also how you run tools that ship as modules. It guarantees the tool belongs to the interpreter you named, rather than whatever happens to be first on `PATH`:

```bash
python -m venv .venv            # create a virtual environment
python -m pip install requests  # install a package
python -m pytest -q             # run tests
python -m http.server 8000      # serve the current folder on http://localhost:8000
python -m json.tool data.json   # pretty-print a JSON file
python -m timeit "sum(range(1000))"
```

You will often see this pattern at the bottom of a file:

```python
def main():
    print("running as a program")

if __name__ == "__main__":
    main()
```

Python sets the variable `__name__` to `"__main__"` in the file it was asked to run, and to the module's real name when the file is imported. The `if` therefore runs `main()` only when the file is run directly, so other code can import its functions without triggering the program.

::: deeper
**What each form puts on `sys.path`** (the list of folders searched by `import`):

| Command | First entry of `sys.path` | Package context |
|---|---|---|
| `python app.py` | the folder containing `app.py` | none, so relative imports fail |
| `python -m pkg.mod` | the current working directory | kept, so relative imports work |
| `python -c "..."` | the current working directory | none |

- `python -m pkg` (a package, not a module) runs `pkg/__main__.py`. That is how `python -m pip` works.
- Under the hood `-m` uses the standard-library module `runpy`, which you can call yourself.
- Since 3.11, `python -P` (or `PYTHONSAFEPATH=1`) stops Python adding the script folder or current folder to `sys.path`. This prevents a stray local file named `random.py` or `email.py` from shadowing the standard-library module of the same name, a classic beginner bug.
:::

::: check
?? Why does `python shop/cli.py` fail with "attempted relative import with no known parent package" while `python -m shop.cli` works?
Running a file directly gives it no package context, so `from .models import ...` has nothing to be relative to. `-m` imports it as `shop.cli`, keeping its package.

?? What is `if __name__ == "__main__":` for?
It runs code only when the file is executed directly, not when it is imported, so the same file can be both a program and an importable module.

?? Why run tools as `python -m pytest` rather than `pytest`?
It guarantees the tool runs under the interpreter (and virtual environment) you named, not whichever `pytest` comes first on `PATH`.
:::

### Virtual environments with `venv` and why they matter

::: plain
A **virtual environment** is a private shelf of packages for one project. Without one, every project on your machine shares a single shelf, so if project A needs version 1 of a library and project B needs version 2, one of them breaks. With a virtual environment, each project has its own shelf and its own link to a Python interpreter, and nothing you install leaks into anything else. Create one for every project, always.
:::

```bash
cd my-project
python3 -m venv .venv               # create it (or: uv venv)
source .venv/bin/activate           # macOS/Linux. Windows: .venv\Scripts\activate
python -m pip install requests      # installs into .venv only
python -c "import sys; print(sys.prefix)"   # .../my-project/.venv
deactivate                          # back to normal
```

A virtual environment is just a folder (`.venv` by convention) containing its own `site-packages` folder for packages, a `bin` (or `Scripts`) folder with a `python` that points at a base interpreter, and a small `pyvenv.cfg` file.

**Activating** only changes `PATH` (and your prompt) so that typing `python` finds `.venv/bin/python` first. You can skip activation and call the interpreter directly, which is what scripts, scheduled jobs and Dockerfiles should do:

```bash
.venv/bin/python -m pytest
```

Rules of thumb:

- One virtual environment per project, created inside the project as `.venv`.
- Never commit it to git: it is large, machine-specific and disposable. Commit the list of dependencies (`pyproject.toml` and a lock file) and recreate the environment from it. Since 3.13, `venv` writes a `.gitignore` inside the folder so git ignores it automatically.
- Point your editor at `.venv` (see Editor and debugger setup).

> **Pitfall:** Moving or renaming a virtual environment folder breaks it, because the scripts inside contain absolute paths. Delete it and create a new one instead; that's what the dependency list is for.

::: deeper
**How Python knows it is inside a venv.** At startup the interpreter looks for `pyvenv.cfg` next to its executable (or one folder up). If it finds one, it sets `sys.prefix` to the venv folder and uses the venv's `site-packages`, while `sys.base_prefix` still points at the base installation. So `sys.prefix != sys.base_prefix` means "running in a venv".

```python
import sys
in_venv = sys.prefix != sys.base_prefix
```

- `python -m venv --system-site-packages .venv` also lets the venv see the base interpreter's packages; rarely what you want.
- `uv venv` creates the same structure much faster; tools such as `uv run` and Poetry create and manage the venv for you.
- Conda environments solve a similar problem but also manage non-Python libraries (C, CUDA); they are a different tool with their own commands.
:::

::: check
?? What problem do virtual environments solve?
Different projects needing different, conflicting package versions, and the risk of breaking the system Python. Each project gets its own isolated set of packages.

?? What does "activating" a venv actually do?
It puts the venv's `bin` (or `Scripts`) folder first on `PATH` and changes the prompt, so `python` and `pip` resolve to the venv's copies. Calling `.venv/bin/python` directly works without activating.

?? Should the `.venv` folder go into git?
No. Commit the dependency list and recreate the venv on each machine.
:::

### `pip` basics: install, freeze, requirements files

::: plain
**pip** is Python's package installer. Thousands of people publish reusable code (packages) to **PyPI**, the Python Package Index, and pip downloads them into your current environment so you can `import` them. Think of it as an app store for code libraries, where the "phone" is your virtual environment.
:::

```bash
python -m pip install requests              # latest version
python -m pip install "requests>=2.32,<3"   # any version in a range
python -m pip install -U pip                # upgrade pip itself
python -m pip list                          # what is installed
python -m pip list --outdated               # what has newer versions
python -m pip show requests                 # version, location, dependencies
python -m pip uninstall requests
```

Installing a package also installs the packages *it* needs (its **dependencies**), and pip works out compatible versions for all of them.

To let someone else (or a server) recreate your environment, record the exact versions:

```bash
python -m pip freeze > requirements.txt     # every installed package, pinned
python -m pip install -r requirements.txt   # recreate it elsewhere
```

A `requirements.txt` is a plain list such as `requests==2.32.3`, one package per line.

Always run pip as `python -m pip`. A bare `pip` command might belong to a different interpreter than `python`, and you'll install packages where your code can't see them: the classic "I installed it but `import` still fails" problem.

> **Pitfall:** Never run `sudo pip install` on Linux or macOS. Many systems now mark their Python "externally managed" (PEP 668) and pip refuses on purpose. Use a virtual environment.

> **Tip:** `uv pip install ...` is a drop-in, much faster replacement for most pip commands.

::: deeper
- **Wheels and source distributions.** A wheel (`.whl`) is a pre-built package that is simply unpacked; pip prefers it. If no wheel matches your platform and Python version, pip downloads the source distribution (`.tar.gz`) and builds it, which may need a C compiler. "Failed building wheel" errors usually mean that: try a newer Python-compatible version or install the build tools.
- **Why `freeze` is not enough for real projects.** It lists everything installed, mixing the packages you asked for with the ones that came along, so you can't tell which can be removed. Modern projects declare direct dependencies in `pyproject.toml` and generate a separate **lock file** (`uv lock`, pip-tools, Poetry) with exact versions and hashes. The Packaging section covers this workflow.
- `python -m pip install -e .` installs your own project in **editable** mode: the environment links to your source folder, so edits take effect without reinstalling.
- `pip download`, `pip wheel` and `--index-url` exist for offline installs and private package indexes.
:::

::: check
?? Why might `pip install x` succeed but `import x` then fail?
The `pip` command belonged to a different interpreter or environment than the `python` running your code. `python -m pip` prevents this.

?? What does `pip freeze > requirements.txt` produce, and what is its weakness?
A list of every installed package with exact versions. It mixes direct and indirect dependencies, so modern projects use `pyproject.toml` plus a lock file instead.
:::

### Editor and debugger setup (VS Code or PyCharm)

::: plain
You can write Python in any text editor, but a proper code editor acts like a patient assistant: it underlines mistakes as you type, suggests names, runs your program with one click, and, most importantly, lets you **pause** the program on any line and look at every variable. That pausing tool is the **debugger**, and learning it early is one of the best investments a beginner can make.
:::

The essentials are the same in **VS Code** (install the Python extension) and **PyCharm**:

1. **Select the interpreter** from your project's `.venv`. Completion, imports and "go to definition" all depend on it; if the editor uses the wrong one, it underlines installed packages as missing.
2. **Linter and formatter:** Ruff does both, very fast. Turn on "format on save" so you never think about spacing again.
3. **Type checker:** Pylance/Pyright in VS Code, or PyCharm's built-in inspections. It catches wrong argument types and forgotten `None` checks before you run anything.
4. **Test runner:** run and debug a single test from the editor.

Using the debugger instead of sprinkling `print` calls:

- Click in the margin next to a line to set a **breakpoint**; when you run in debug mode, the program pauses there.
- **Step over** runs the next line; **step into** enters the function being called; **step out** finishes the current function.
- The variables panel shows every local value; the debug console lets you type expressions and see their values right now.

You can also pause from code with the built-in `breakpoint()`, which opens `pdb`, Python's text debugger, in the terminal:

```python
def total(prices):
    breakpoint()          # pauses here: type p prices, n (next), c (continue), q (quit)
    return sum(prices)
```

> **Tip:** When a program does something you don't understand, set a breakpoint just before it and step line by line. You will learn more about Python in ten minutes of stepping than in an hour of guessing.

::: deeper
- **Conditional breakpoints** (right-click the breakpoint) stop only when an expression is true, such as `i == 999`, so you can skip straight to the interesting loop iteration. **Logpoints** print a message without stopping.
- Useful `pdb` commands: `n` next line, `s` step into, `r` return from function, `c` continue, `p expr` print, `pp expr` pretty-print, `l` list source, `w` where (call stack), `u`/`d` up and down the stack, `b 42` breakpoint at line 42, `q` quit.
- `python -m pdb script.py` starts a script under the debugger; after a crash, `python -m pdb -c continue script.py` stops at the exception (post-mortem).
- The environment variable `PYTHONBREAKPOINT` chooses what `breakpoint()` calls (`PYTHONBREAKPOINT=0` disables all of them, handy in production).
- Since 3.14 you can attach `pdb` to an already *running* process with `python -m pdb -p PID`, which helps with hung services.
:::

::: check
?? Why must the editor be pointed at the project's `.venv`?
Completion, imports, type checking and running all use the selected interpreter; with the wrong one, installed packages appear missing and code runs in the wrong environment.

?? What is the difference between "step over" and "step into"?
Step over runs the whole next line, including any function calls, and pauses after it. Step into enters the function called on that line and pauses at its first line.
:::

## Language basics

Now the language itself. This group holds the most important idea in the whole course, in the topic "Names, binding and assignment": **a variable is a name attached to an object, not a box that holds a value.** Read that topic slowly, try every example, and the rest of Python will make sense. The other topics cover the everyday tools you need on day one: how code is laid out, printing and reading text, what Python treats as true or false, and how comparisons and operators work.

[[fig:names]]

### Indentation, blocks, comments and docstrings

::: plain
Most languages group lines of code with curly braces `{ }`. Python uses **indentation** instead: the lines that belong together are pushed right by the same amount, like sub-points in a list. A line ending in a colon `:` announces "a block follows", and the block is everything indented beneath it. So in Python, the layout of the code *is* its structure, which is why Python code tends to look tidy.
:::

```python
def classify(n):
    """Return 'even' or 'odd' for an integer n."""
    if n % 2 == 0:
        return "even"      # inside the if-block (8 spaces)
    return "odd"           # back at function level (4 spaces)

print(classify(7))         # odd
```

Read the indentation like an outline: `classify` contains everything indented under it; the `if` contains only the line indented further.

The rules:

- A line ending in a colon (`if`, `elif`, `else`, `for`, `while`, `def`, `class`, `with`, `try`, `except`, `match`, `case`...) opens a block.
- Every line in the block must be indented by the same amount. Use **four spaces** per level (the PEP 8 style guide); your editor inserts them when you press Tab.
- A block can't be empty. Use `pass` as a "nothing here yet" placeholder.

```python
def todo():
    pass                   # a valid, empty function body
```

**Comments** start with `#` and run to the end of the line. Python ignores them; they're for humans. Good comments explain *why* the code does something, not *what* it does, because the code already says what.

A **docstring** is a string written as the first line inside a module, function or class. Unlike a comment, Python keeps it: `help(classify)`, your editor's hover popup and documentation tools all show it.

```python
print(classify.__doc__)    # Return 'even' or 'odd' for an integer n.
```

Write one for every function that someone else might call: a one-line summary in the imperative ("Return...", "Compute..."), then optionally a blank line and details about arguments and return value.

**Style from day one.** Follow PEP 8, Python's style guide: four-space indentation, `snake_case` for variables and functions, `CapWords` for classes, `UPPER_CASE` for constants, and lines short enough to read. Let a formatter (`ruff format`) apply it automatically.

> **Pitfall:** Never mix tabs and spaces. Python 3 raises `TabError` when the indentation is ambiguous, and the error is maddening because both look identical on screen. Configure your editor to insert spaces.

> **Tip:** Inside parentheses, brackets and braces you may break lines freely, so wrap long expressions in parentheses instead of ending lines with a backslash.

::: deeper
- The tokenizer turns increases and decreases of indentation into `INDENT` and `DEDENT` tokens, and the parser treats them exactly like `{` and `}` in C. That's why inconsistent indentation is a *syntax* error, found before any of your code runs.
- `...` (the `Ellipsis` object) is also a valid placeholder body, popular in type stubs and abstract methods.
- PEP 257 describes docstring conventions. Multi-line docstrings usually follow one of three layouts (Google, NumPy or reStructuredText) so tools such as Sphinx can render the parameters and return values.
- Docstrings are removed when Python runs with `-OO`, so never put behaviour in them.

```python
def area(width: float, height: float) -> float:
    """Return the area of a rectangle.

    Args:
        width: Width in metres.
        height: Height in metres.
    """
    return width * height
```
:::

::: check
?? How does Python know which lines belong to an `if` statement?
By indentation: the block is every line after the colon that is indented further than the `if` line, at the same level.

?? What is the difference between a comment and a docstring?
A comment (`# ...`) is thrown away when the code is read. A docstring is a string kept at run time in `__doc__`, shown by `help()` and editors.

?? What do you write in a block that has nothing to do yet?
`pass` (or `...`).
:::

### Names, binding and assignment (names point to objects)

::: plain
Think of every value in your program (a number, a piece of text, a list) as a **parcel** sitting in Python's warehouse, and a variable name as a **luggage tag** tied to a parcel. `x = 5` ties the tag `x` to the parcel holding 5. `y = x` ties a *second tag* to the *same parcel*; it does not copy the parcel. If the parcel can be changed (a list can, a number can't), changing it through one tag is visible through every tag tied to it. This one idea explains most of Python's surprises.
:::

[[fig:bindRebind]]

```python
a = [1, 2, 3]
b = a            # b is a second name for the SAME list
b.append(4)      # change the list through b...
print(a)         # [1, 2, 3, 4]   ...and a sees it: one list, two names
print(a is b)    # True

b = [9]          # rebinding: b's tag moves to a brand-new list
print(a)         # [1, 2, 3, 4]   a is unaffected
```

Two different actions look similar, so name them precisely:

- **Mutating** an object changes the object itself (`b.append(4)`, `b[0] = 10`). Every name tied to it sees the change.
- **Rebinding** a name moves the tag to a different object (`b = [9]`). Other names are untouched.

Assignment (`=`) always rebinds; it *never* copies. When you want a copy, ask for one:

```python
original = [1, 2, 3]
copy1 = original.copy()      # or list(original), or original[:]
copy1.append(4)
print(original)              # [1, 2, 3]   unaffected
```

Numbers, strings and tuples are **immutable**: no operation can change them, so sharing them is always safe. `x = x + 1` doesn't change the number 5; it computes a new number 6 and moves the tag `x` to it.

The same rule applies when you call a function: the parameter becomes another name for the object you passed. A function can therefore *mutate* a list you give it, but *rebinding* its parameter does not affect your variable.

```python
def add_item(items):
    items.append("new")      # mutates the caller's list

def replace(items):
    items = ["other"]        # only rebinds the local name

basket = ["apple"]
add_item(basket); print(basket)   # ['apple', 'new']
replace(basket);  print(basket)   # ['apple', 'new']
```

> **Interview:** "Is Python pass-by-value or pass-by-reference?" Neither exactly: it passes references to objects, often called **call by sharing**. A function can mutate a mutable argument, but rebinding the parameter does not change the caller's variable.

::: deeper
**Namespaces.** Names live in namespaces, which are essentially dictionaries: a module's globals, a function's locals, an object's attributes. `x = 1` inside a function creates or rebinds a *local* name and does not touch a global `x` unless you declare `global x` (the Functions section covers scope rules, LEGB).

**Everything that binds a name**, not just `=`: `def` and `class` bind the new function or class, `import` binds the module, `for` binds the loop variable, `with ... as` and `except ... as` bind, and parameters are bound on every call.

**Augmented assignment depends on the type.** For mutable objects `+=` usually mutates in place (`list.__iadd__`); for immutable ones it creates a new object and rebinds:

```python
t = (1, 2); u = t; t += (3,)   # new tuple; u is still (1, 2)
l = [1, 2]; m = l; l += [3]    # same list mutated; m is [1, 2, 3]
```

**Lifetime.** An object is freed when no names or containers refer to it any more; CPython notices immediately through reference counting (Memory management section).

**Shallow vs deep copies.** `list.copy()` copies the outer list only; inner lists are still shared. `copy.deepcopy` copies recursively. The Collections and Data model sections return to this.
:::

::: check
?? After `a = [1, 2]; b = a; b.append(3)`, what is `a`, and why?
`[1, 2, 3]`. `b = a` made `b` a second name for the same list, and `append` mutated that list.

?? After `a = [1, 2]; b = a; b = [9]`, what is `a`?
`[1, 2]`. `b = [9]` only moved the name `b` to a new list; the original list was not changed.

?? A function receives a list and does `items = []`. Does the caller's list become empty?
No. That rebinds the local parameter name only. `items.clear()` would empty the caller's list, because it mutates the shared object.
:::

### Dynamic but strong typing

::: plain
Every value in Python has a **type**: whole number, decimal number, text, list and so on. Two words describe how Python treats types. **Dynamic**: the type belongs to the value, not to the variable name, so the same name can point at a number now and at text later, and Python checks types while the program runs. **Strong**: Python refuses to guess when you mix types that don't belong together. Adding text to a number is an error, not a silent conversion.
:::

```python
x = 42
print(type(x))         # <class 'int'>
x = "forty-two"        # fine: the name now points at a str
print(type(x))         # <class 'str'>
```

Because types are checked at run time, a function works with any value that supports the operations it uses. This is called **duck typing** ("if it walks like a duck and quacks like a duck..."):

```python
def double(v):
    return v * 2

print(double(3), double("ab"), double([1]))   # 6 abab [1, 1]
```

Strong typing means you convert explicitly:

```python
age_text = "41"
age_text + 1          # TypeError: can only concatenate str (not "int") to str
int(age_text) + 1     # 42
str(42) + "!"         # '42!'
float("3.5")          # 3.5
```

The conversion functions are named after the types: `int()`, `float()`, `str()`, `bool()`, `list()` and so on. Converting invalid text raises `ValueError` (`int("abc")`), which you'll learn to handle in the Errors section.

> **Pitfall:** `input()` always returns text. `input("Age: ") + 1` fails; convert first with `int(...)`.

::: deeper
- **The few implicit conversions** stay inside the number family: `int` widens to `float` and `complex` in mixed arithmetic (`1 + 2.0` is `3.0`), and `bool` is a subclass of `int` (`True + True` is `2`). Everything else needs an explicit call.
- **Comparison with other languages:** JavaScript is dynamic and *weak* (`"1" + 1` is `"11"`); Java and C# are *static* and strong (types declared on variables, checked before running); C is static and fairly weak (implicit casts between integers and pointers).
- **Type hints** (`def double(v: int) -> int:`) don't change any of this: the interpreter ignores them at run time. Static checkers such as mypy and Pyright read them to find errors before you run the program, giving you static checking where you want it. The Typing section covers them.

> **Interview:** "Is Python statically or dynamically typed? Strong or weak?" Dynamic (names are untyped, types are checked at run time) and strong (no implicit coercion between unrelated types). Type hints add optional static checking without changing run-time behaviour.
:::

::: check
?? What does "dynamically typed" mean for Python?
Types belong to objects, not names. A name can refer to objects of different types over time, and type errors are found when the code runs.

?? Why does `"5" + 5` raise an error instead of giving `"55"` or `10`?
Python is strongly typed: it does not silently convert between unrelated types. Convert explicitly with `int("5") + 5` or `"5" + str(5)`.
:::

### `print`, `input` and f-strings

::: plain
`print` shows things on the screen; `input` asks the user a question and gives you their answer as text. **f-strings** let you build text with values dropped into it: put an `f` before the quotes and write any variable or expression inside curly braces, like filling in the blanks of a form letter.
:::

```python
name = "Asha"
age = 31
print("Hello", name)                 # Hello Asha       (items separated by a space)
print(f"{name} is {age} years old")  # Asha is 31 years old
print(f"Next year: {age + 1}")       # Next year: 32    (any expression works)
```

`print` converts each argument to text, joins them with `sep` (a space by default), adds `end` (a newline by default) and writes the result:

```python
print("a", "b", 3)                     # a b 3
print("a", "b", sep=", ", end="!\n")   # a, b!
print("no newline", end="")            # the next print continues on this line
```

`input(prompt)` shows the prompt, waits for the user to type a line and press Enter, and returns that line **as a string**, without the newline. Convert it when you need a number:

```python
name = input("Name: ")
age = int(input("Age: "))        # ValueError if the user types "abc"
print(f"In ten years {name} will be {age + 10}")
```

f-strings can also **format** values: after the expression, add a colon and a format specification.

```python
total = 1234.5
print(f"{total:.2f}")        # 1234.50     two decimal places
print(f"{total:,.2f}")       # 1,234.50    thousands separator
print(f"{0.256:.1%}")        # 25.6%       percentage
print(f"[{'hi':>6}]")        # [    hi]   right-aligned in 6 characters
print(f"[{'hi':<6}]")        # [hi    ]   left-aligned
print(f"{7:03d}")            # 007         zero-padded
print(f"{total=}")           # total=1234.5   debugging shortcut
```

> **Tip:** The `=` form (`f"{total=}"`) prints the expression and its value. It is the quickest way to debug with print.

::: deeper
- `print(..., file=sys.stderr)` writes to the error stream; `flush=True` forces output immediately, needed for progress messages ending in `end="\r"`.
- For programs that read lots of input (competitive programming), `sys.stdin.readline` or `sys.stdin.read().split()` is much faster than calling `input()` in a loop.
- **Format spec mini-language:** `[[fill]align][sign][width][,][.precision][type]`. Types include `d` (integer), `f` (fixed point), `e` (scientific), `%` (percentage), `x` (hex), `b` (binary). `!r` uses `repr()` instead of `str()`: `f"{name!r}"` shows `'Asha'` with quotes.
- Since 3.12 (PEP 701) f-strings are parsed as real grammar, so you can reuse the same quote type inside (`f"{d["key"]}"`), use backslashes and even put comments in multi-line expressions.
- 3.14 added **t-strings** (`t"..."`), which produce a `Template` object instead of a string, so libraries can safely escape the values (for SQL or HTML). The Strings section covers them.
- Older styles you will see in existing code: `"%s is %d" % (name, age)` and `"{} is {}".format(name, age)`. Prefer f-strings, except in `logging` calls (`log.info("user %s", uid)`), where formatting is deferred until the message is actually emitted.
:::

::: check
?? What type does `input()` return, even when the user types a number?
Always `str`. Convert with `int()` or `float()` when you need a number.

?? Write an f-string that shows `price = 3.14159` as `3.14`.
`f"{price:.2f}"`

?? What does `print("a", "b", sep="-")` output?
`a-b`
:::

### Truthiness and `None`

::: plain
`if` and `while` need a yes-or-no answer, and Python lets *any* value give one. The rule is simple: "nothing" values count as false, and everything else counts as true. "Nothing" means `None`, `False`, the number zero, and anything empty: an empty string, list, dictionary or set. So `if items:` reads naturally as "if there are any items". `None` is Python's special value meaning "no value here".
:::

The falsy values:

- `None` and `False`
- zero of any number type: `0`, `0.0`, `0j`, `Decimal(0)`
- empty containers and strings: `""`, `[]`, `()`, `{}`, `set()`, `range(0)`

Everything else is truthy, including `"0"`, `" "` (a space), `[0]` and `-1`.

```python
items = []
if not items:                    # idiomatic emptiness check
    print("nothing to do")

name = ""
display = name or "anonymous"    # or gives the first truthy value
print(display)                   # anonymous

print(bool(0), bool("0"), bool([]), bool([0]))   # False True False True
```

**`None`** is the single object of type `NoneType`. Python uses it to mean "no value": a function without a `return` statement returns `None`, optional parameters often default to `None`, and `dict.get` returns `None` for a missing key. Because there is exactly one `None`, test for it with `is`:

```python
def find(users, name):
    for u in users:
        if u == name:
            return u
    # falls off the end: returns None

result = find(["ana", "bo"], "cy")
if result is None:
    print("not found")
```

> **Pitfall:** `if x:` and `if x is not None:` are different when `0`, `""` or `[]` are valid values. `def f(limit=None): if not limit: ...` wrongly treats `limit=0` as "not given". Use `is None` whenever zero or empty is meaningful.

::: deeper
- **For your own classes**, Python calls `__bool__`; if it isn't defined it calls `__len__` and checks for zero; if neither exists, the object is always truthy. The Data model section shows how to define them.
- **`and` and `or` return one of their operands, not a bool.** `or` returns the first truthy operand (or the last one); `and` returns the first falsy operand (or the last one). They stop as soon as the answer is known (short-circuit), so later operands may never be evaluated: `[] and f()` is `[]` and `f` is not called.

```python
print(0 or "x")          # x
print("a" and "b")       # b
print(None and 1 / 0)    # None   (1 / 0 never runs)
```

- `x or default` treats every falsy value as missing. Since 0 and `""` are falsy, prefer `default if x is None else x` when they are legitimate.
- 3.15 added the built-in `sentinel("NAME")` for "not given" markers when `None` itself is a valid value.
:::

::: check
?? List four falsy values.
Any four of: `None`, `False`, `0`, `0.0`, `""`, `[]`, `()`, `{}`, `set()`, `range(0)`.

?? Is the string `"0"` truthy or falsy?
Truthy: it is a non-empty string. Only the number zero is falsy.

?? Why write `if x is None:` rather than `if not x:` when checking for a missing value?
`not x` is also true for `0`, `""` and empty containers, which may be valid values. `is None` checks only for "no value".
:::

### `id()`, `type()`, `isinstance()`

::: plain
Every object in Python carries three facts: **who** it is (its identity, a unique number for as long as it exists), **what kind** of thing it is (its type), and **what it holds** (its value). `id()` tells you the first, `type()` the second, and `isinstance()` answers the everyday question "is this a kind of X?", counting subtypes too.
:::

```python
x = [1, 2]
print(id(x))             # e.g. 4372154624, a unique number while x exists
print(type(x))           # <class 'list'>
print(type(x) is list)   # True
print(isinstance(x, list))          # True
print(isinstance(3, (int, float)))  # True: a tuple means "any of these"
print(isinstance(3.5, int | float)) # True: the | form also works (3.10+)
```

`isinstance` respects inheritance: an object of a subclass counts as an instance of its parent class. `type(x) is C` checks the *exact* class only.

```python
class Animal: ...
class Dog(Animal): ...

d = Dog()
print(type(d) is Animal)      # False: exact type is Dog
print(isinstance(d, Animal))  # True: a Dog is an Animal
print(issubclass(Dog, Animal))   # True: the class-level version
```

Prefer `isinstance` to `type(x) == C`, because it lets subclasses stand in for their parents. Better still, avoid type checks when you can and simply use the object (duck typing); type checks are best kept for validating input at the edges of a program.

> **Pitfall:** `bool` is a subclass of `int`, so `isinstance(True, int)` is `True`. Validation such as "must be an integer" may accidentally accept `True` and `False`. Reject them explicitly when it matters: `isinstance(x, int) and not isinstance(x, bool)`.

::: deeper
- In CPython, `id(obj)` is the object's memory address. Two objects whose lifetimes don't overlap may get the same id, so never store ids of temporary objects: `id([]) == id([])` can be `True`, because the first list is already freed when the second is created.
- `isinstance` also works with **abstract base classes** and runtime-checkable protocols, which check behaviour rather than ancestry:

```python
from collections.abc import Mapping, Iterable
print(isinstance({}, Mapping))       # True
print(isinstance("abc", Iterable))   # True
```

- With three arguments, `type(name, bases, namespace)` *creates* a new class at run time; the Metaprogramming section shows how classes are built this way.
- `functools.singledispatch` chooses a function implementation by argument type, a cleaner alternative to long `isinstance` chains.
:::

::: check
?? What three properties does every Python object have?
Identity (`id`), type (`type`) and value.

?? `class Dog(Animal)` and `d = Dog()`: what are `type(d) is Animal` and `isinstance(d, Animal)`?
`False` and `True`. `type` gives the exact class; `isinstance` also accepts subclasses.
:::

### `is` vs `==`

::: plain
`==` asks "do these two things have **equal values**?", like asking whether two copies of a book have the same text. `is` asks "are these **the very same object**?", like asking whether two people are holding the same physical copy. Two separate lists can be equal (`==`) without being the same list (`is`). Use `==` almost always, and save `is` for checking against `None`.
:::

```python
a = [1, 2]
b = [1, 2]
c = a
print(a == b)    # True: same contents
print(a is b)    # False: two different list objects
print(a is c)    # True: two names for one list
```

| Operator | Question it asks | Use it for |
|---|---|---|
| `==` | Do the values compare equal? | Almost everything |
| `is` | Is it the very same object? | `None`, and special one-of-a-kind objects |

The idiomatic `None` checks are `x is None` and `x is not None`.

> **Pitfall:** `is` sometimes *seems* to work for numbers and strings, because CPython reuses some objects to save memory: small integers from -5 to 256 are cached, and many short strings are shared. With other values the same code gives a different answer. Never use `is` to compare numbers or strings.

::: deeper
- `==` calls `a.__eq__(b)`, falling back to `b.__eq__(a)`, so classes decide what equality means. `is` compares identities, can't be overridden, and is therefore fast and unambiguous.
- Use `is` for **singletons**: `None`, `NotImplemented`, `Ellipsis`, and your own sentinel objects (`MISSING = object()`). Comparing booleans with `is True` is legal but rarely needed; `if flag:` is the idiom.
- Small-int caching and string interning are CPython implementation details:

```python
x = 256; y = 256; print(x is y)       # True: cached
x = int("1000"); y = int("1000"); print(x is y)   # False: two separate int objects
```

Since 3.8 the compiler warns when you write `is` with a literal: `SyntaxWarning: "is" with 'int' literal. Did you mean "=="?`.
- `float("nan") == float("nan")` is `False` (NaN is not equal to anything, even itself), but `x is x` is always true. Containers check identity *before* equality, so `nan in [nan]` is `True` when it's the same object.
:::

::: check
?? `a = [1]; b = [1]`. What are `a == b` and `a is b`?
`True` and `False`: equal contents, but two separate lists.

?? When should you use `is`?
For singletons such as `None` (`x is None`) and sentinel objects. Use `==` for comparing values.

?? Why might `x is y` be `True` for `x = y = 256` written separately but not for larger numbers?
CPython caches small integers (-5 to 256), so equal small values are often the same object. That's an implementation detail you must not rely on.
:::

### Operators and precedence, chained comparisons

::: plain
Operators are the symbols that compute things: `+ - * /` for arithmetic, `== < >` for comparisons, `and or not` for logic. **Precedence** is the order Python applies them in when you combine several, the same idea as "multiplication before addition" from school: `2 + 3 * 4` is `14`, not `20`. When you are unsure of the order, add parentheses. They cost nothing and make the meaning obvious to the next reader.
:::

Arithmetic:

```python
print(7 / 2)      # 3.5   true division always gives a float
print(7 // 2)     # 3     floor division: round down to a whole number
print(7 % 2)      # 1     remainder (modulo)
print(2 ** 10)    # 1024  power
print(-7 // 2)    # -4    floor rounds DOWN, towards minus infinity
print(-7 % 2)     # 1     the remainder takes the sign of the divisor
```

Comparison operators give `True` or `False`: `==`, `!=`, `<`, `<=`, `>`, `>=`, plus `in` (membership) and `is` (identity).

Logical operators: `and`, `or`, `not`. They **short-circuit**: `and` stops at the first false value, `or` at the first true one, so `x is not None and x.size > 0` never touches `x.size` when `x` is `None`.

**Chained comparisons** read like maths:

```python
i, items = 2, [10, 20, 30]
print(0 <= i < len(items))   # True: means 0 <= i and i < len(items)
a = b = c = 5
print(a == b == c)           # True: all equal
```

A simplified precedence order, from loosest to tightest binding: `lambda`, `if-else`, `or`, `and`, `not`, comparisons (`in`, `is`, `<`, `==`...), `|`, `^`, `&`, shifts, `+ -`, `* / // % @`, unary `-x`, `**`, then calls, indexing and attribute access.

> **Pitfall:** `**` binds tighter than a leading minus: `-2 ** 2` is `-4`, not `4`. Write `(-2) ** 2` when you mean the square of minus two.

::: deeper
More surprises that follow from the precedence table:

```python
print(2 ** -1)        # 0.5: the right operand of ** may be negative
print(2 ** 3 ** 2)    # 512: ** is right-associative, 2 ** 9
x = 6
print(x & 1 == 0)     # True: (x & 1) == 0
```

- In Python the bitwise operators `&`, `|` and `^` bind **tighter** than comparisons, the opposite of C, where `x & 1 == 0` means `x & (1 == 0)`.
- A chain evaluates each middle operand once: in `a < f() < c`, `f()` runs one time.
- Any comparisons chain, sometimes confusingly: `1 in [1] == True` means `(1 in [1]) and ([1] == True)`, which is `False`. Avoid mixing `in` or `is` into chains.
- `divmod(a, b)` returns `(a // b, a % b)` in one call; `@` is matrix multiplication (used by NumPy); `:=` has the lowest precedence of all and usually needs parentheses.
- Floor division and modulo always satisfy `a == (a // b) * b + a % b`, which is why the remainder follows the divisor's sign.
:::

::: check
?? What are `7 // 2`, `-7 // 2` and `7 / 2`?
`3`, `-4` (floor rounds towards minus infinity) and `3.5`.

?? What does `0 <= i < n` mean?
`0 <= i and i < n`, with `i` evaluated only once: a bounds check.

?? What is `-3 ** 2`?
`-9`, because `**` binds tighter than the unary minus.
:::

## Control flow

So far programs ran top to bottom, line by line. **Control flow** is how a program makes decisions and repeats work: `if` chooses between paths, `for` and `while` repeat, `break` and `continue` adjust a loop, and `match` picks a path based on the *shape* of data. With these and the basics above you can already write real programs.

### `if` / `elif` / `else` and conditional expressions

::: plain
`if` lets a program choose. "If it is raining, take an umbrella; otherwise, if it is sunny, wear a hat; otherwise, do nothing." Python checks each condition from top to bottom, runs the block under the **first** one that is true, and skips the rest. `elif` means "else if", and `else` catches everything left over.
:::

```python
def grade(score):
    if score >= 90:
        return "A"
    elif score >= 80:
        return "B"
    elif score >= 70:
        return "C"
    else:
        return "F"

print(grade(85))    # B: the first true condition wins; 85 >= 70 is never checked
```

The condition can be any value; Python uses its truthiness (see Truthiness and `None`). `elif` and `else` are optional.

The **conditional expression** picks one of two values in a single line. It is an expression, so it can go anywhere a value can:

```python
n, count = 7, 1
label = "even" if n % 2 == 0 else "odd"
print(label)                                   # odd
print(f"{count} item{'s' if count != 1 else ''}")   # 1 item
```

Read it as "this value if the condition holds, else that value".

**Guard clauses** keep code flat: handle the invalid cases first and return early, so the main logic isn't buried inside nested `if`s.

```python
def process(order):
    if order is None:
        return "nothing to do"
    if not order["items"]:
        raise ValueError("empty order")
    return f"processing {len(order['items'])} items"   # main logic, not nested

print(process({"items": [1, 2]}))   # processing 2 items
```

> **Tip:** Don't nest conditional expressions (`a if x else b if y else c`); they are hard to read. Use an `if` statement or a small function.

::: deeper
When an `elif` chain only maps a value to an action, a **dictionary of handlers** is often shorter, faster and easier to extend:

```python
def do_add(args): return f"added {args}"
def do_list(args): return "listing"
def do_unknown(args): return "unknown command"

handlers = {"add": do_add, "list": do_list}
command, args = "add", "milk"
print(handlers.get(command, do_unknown)(args))   # added milk
```

- Python had no `switch` statement before 3.10 and has no `?:` operator; the conditional expression and `match` cover both needs.
- `if x == 1 or 2:` is a classic bug: it means `(x == 1) or 2`, which is always truthy. Write `x == 1 or x == 2`, or better, `x in (1, 2)`.
:::

::: check
?? With `score = 95`, which branches of the `grade` function are checked?
Only the first: `95 >= 90` is true, so `"A"` is returned and the remaining conditions are never evaluated.

?? Rewrite `if n > 0: sign = "positive" else: sign = "not positive"` as one line.
`sign = "positive" if n > 0 else "not positive"`

?? Why is `if x == 1 or 2:` always true?
It parses as `(x == 1) or 2`, and `2` is truthy. Use `x in (1, 2)`.
:::

### `while` and `for` loops, `range`, `enumerate`, `zip`

::: plain
Loops repeat work. A **`while`** loop repeats *as long as* a condition is true, like "keep stirring while the sauce is lumpy": use it when you don't know in advance how many rounds you need. A **`for`** loop goes through a collection *one item at a time*, like "for each guest, set a plate": use it whenever you have a collection, which is most of the time.
:::

```python
countdown = 3
while countdown > 0:
    print(countdown)        # 3, 2, 1
    countdown -= 1          # without this line the loop would never end
print("lift off")

for fruit in ["apple", "banana", "cherry"]:
    print(fruit.upper())    # APPLE, BANANA, CHERRY
```

A `for` loop works on anything **iterable**: lists, strings (character by character), dictionaries (by key), files (line by line), ranges and more.

```python
for ch in "abc":
    print(ch)                         # a, b, c
prices = {"tea": 2.5, "cake": 3.0}
for item, price in prices.items():    # key and value together
    print(f"{item}: {price}")
```

**`range(start, stop, step)`** produces whole numbers, stopping *before* `stop`:

```python
print(list(range(5)))          # [0, 1, 2, 3, 4]
print(list(range(2, 10, 3)))   # [2, 5, 8]
print(list(range(10, 0, -3)))  # [10, 7, 4, 1]
```

**`enumerate`** gives you a counter along with each item, so you never need `range(len(...))`:

```python
names = ["Ada", "Grace", "Linus"]
for i, name in enumerate(names, start=1):
    print(f"{i}. {name}")      # 1. Ada  2. Grace  3. Linus
```

**`zip`** walks several collections side by side:

```python
names = ["Ada", "Grace"]
scores = [92, 88]
for name, score in zip(names, scores, strict=True):
    print(name, score)         # Ada 92, then Grace 88
```

`zip` normally stops at the shortest input and silently drops the rest; `strict=True` (3.10+) raises `ValueError` instead, which catches mismatched data.

> **Pitfall:** Don't add or remove items in a list while looping over it: items get skipped. Loop over a copy (`for x in items[:]`) or, better, build a new list. Changing a dictionary's size during iteration raises `RuntimeError`.

::: deeper
- A `for` loop calls `iter()` on the collection once, then `next()` repeatedly until it raises `StopIteration`. This **iterator protocol** is why `for` works on so many different things; the Iteration section explains it fully.
- `range` is a lazy, immutable sequence that stores only start, stop and step, so `range(10**12)` costs almost nothing. It supports `len`, indexing, slicing and fast membership: `10**11 in range(10**12)` is computed arithmetically, without looping.
- `itertools.zip_longest(a, b, fillvalue=None)` pads the shorter input instead of stopping.
- `zip` tricks: `dict(zip(keys, values))` builds a dictionary; `list(zip(*matrix))` transposes a list of rows into columns.
- Iterate over a file directly (`for line in open(path, encoding="utf-8")`) to read it lazily, line by line, without loading it all into memory.
- Loop variables survive after the loop: after `for i in range(3): pass`, `i` is `2`.
:::

::: check
?? When would you choose `while` over `for`?
When you don't know the number of iterations in advance (repeat until a condition changes: reading until there is no more input, retrying, searching). Use `for` to go through a collection.

?? What does `list(range(1, 10, 4))` produce?
`[1, 5, 9]`

?? What is wrong with `for i in range(len(names)): print(i, names[i])`, and what is the fix?
It works but is clumsy and error-prone. `for i, name in enumerate(names): print(i, name)` is the idiom.
:::

### `break`, `continue`, and `else` clauses on loops

::: plain
`break` means "stop the loop now"; `continue` means "skip the rest of this round and go on to the next one". Python also lets a loop have an `else` block, which is unusual: it runs only if the loop **finished without hitting `break`**. Read it as "no break": "search every drawer for the keys; if you never found them (never broke out), then buy new keys".
:::

```python
for n in [3, 8, -1, 5]:
    if n < 0:
        print("negative found, stopping")
        break                 # leave the loop immediately
    if n % 2 == 0:
        continue              # skip even numbers, go to the next n
    print("odd:", n)
# odd: 3
# negative found, stopping
```

The `else` clause replaces a "found" flag variable:

```python
def find_factor(n):
    for d in range(2, int(n ** 0.5) + 1):
        if n % d == 0:
            print(f"{n} = {d} * {n // d}")
            break
    else:                       # runs only if the loop never hit break
        print(f"{n} is prime")

find_factor(91)   # 91 = 7 * 13
find_factor(97)   # 97 is prime
```

It works with `while` too:

```python
attempts = 0
while attempts < 3:
    attempts += 1
    if attempts == 5:           # pretend this is a connection attempt
        break
else:
    print("gave up after 3 attempts")
```

> **Tip:** Many readers find `for ... else` surprising. Add a `# no break` comment next to the `else`, or for a simple search use `next((x for x in xs if test(x)), None)`, which returns the first match or `None`.

::: deeper
- `break` and `continue` affect only the **innermost** loop. Python has no labelled break; to leave nested loops, move them into a function and `return`, or use a flag.
- An empty loop (zero iterations) runs its `else` block, because it ended without `break`.
- `try` also has an `else` clause (it runs when no exception occurred). Same keyword, different idea; the Errors section covers it.
- A `return` inside a loop also skips the `else` block, because the loop does not finish normally.
:::

::: check
?? When does the `else` block of a `for` loop run?
When the loop finishes without hitting `break`, including when it has zero iterations.

?? What is the difference between `break` and `continue`?
`break` leaves the loop entirely; `continue` skips the rest of the current iteration and moves on to the next.
:::

### Assignment expressions with `:=` (PEP 572)

::: plain
Normally assignment (`x = ...`) is a statement on its own line. The **walrus operator** `:=` (named for its resemblance to a walrus's eyes and tusks) lets you assign a value *and* use it in the same expression. It is a small convenience for one situation: when you would otherwise have to compute something twice, or write an awkward loop.
:::

The clearest use is reading data in chunks until there is none left:

```python
import io
f = io.StringIO("abcdefghij")      # stands in for an open file

while (chunk := f.read(4)):        # assign the chunk, then test it
    print(chunk)                   # abcd, efgh, ij
```

Without `:=` you would need to call `f.read` twice, once before the loop and once at the end of it. Other good uses:

```python
import re
text = "pages 12-30"
if (m := re.search(r"(\d+)-(\d+)", text)):        # use the match only if it matched
    start, end = map(int, m.groups())
    print(start, end)                              # 12 30

items = list(range(15))
if (n := len(items)) > 10:                         # compute once, use twice
    print(f"too many items ({n})")                 # too many items (15)
```

> **Pitfall:** Always put the walrus in parentheses inside a comparison. `if n := len(items) > 10` assigns the *result of the comparison* (`True`) to `n`, because `:=` has the lowest precedence of all.

::: deeper
Rules:

- It can only assign to plain names: `self.x := 1` and `d["k"] := 1` are syntax errors.
- A bare `x := 5` as a statement is a syntax error; use `=`.
- In a comprehension, the target binds in the **enclosing** scope, so it is still visible after the comprehension finishes:

```python
def transform(x):
    return x * 10 if x % 2 else None

data = [1, 2, 3]
results = [y for x in data if (y := transform(x)) is not None]
print(results, y)        # [10, 30] 30
```

That example also shows the comprehension use: compute an expensive value once and use it in both the filter and the result.

> **Tip:** Don't use `:=` to cram logic into one line. If a normal assignment on the line before is just as clear, prefer it.
:::

::: check
?? What does `:=` do that `=` can't?
It assigns and also returns the value, so it can be used inside an expression such as a `while` condition, an `if` test or a comprehension filter.

?? What is `n` after `if n := len([1, 2, 3]) > 2:`?
`True`, not 3, because `:=` binds last. Write `if (n := len(...)) > 2:`.
:::

### Structural pattern matching `match` / `case` (PEP 634 to 636): literal, capture, sequence, mapping, class patterns and guards

::: plain
`match` compares a value against a list of **patterns** and runs the first one that fits, like a mail sorter who checks "is it a parcel? a letter with a stamp? a postcard?" and puts each item in the right bin. It looks like the `switch` statement of other languages, but it can do much more: a pattern can describe the *shape* of data (a list of two items, a dictionary with a "type" key) and pull out the pieces in the same step.
:::

Start simple, matching plain values:

```python
def http_status(code):
    match code:
        case 200:
            return "OK"
        case 301 | 302:            # | means "or"
            return "redirect"
        case 404:
            return "not found"
        case _:                    # _ matches anything: the default
            return "other"

print(http_status(302))            # redirect
```

The real power is **destructuring**: matching a structure and binding its parts to names.

```python
from dataclasses import dataclass

@dataclass
class Point:
    x: int
    y: int

def handle(event):
    match event:
        case {"type": "click", "pos": (x, y)}:          # a dict with a 2-item pos
            return f"click at {x},{y}"
        case {"type": "key", "key": "q" | "Q"}:
            return "quit"
        case {"type": "key", "key": str(k)} if len(k) == 1:   # guard with if
            return f"typed {k}"
        case [cmd, *args]:                              # a list: first item, then the rest
            return f"command {cmd} with {args}"
        case Point(x=0, y=0):                           # an object with these attributes
            return "origin"
        case None:
            return "nothing"
        case _:
            return "unknown"

print(handle({"type": "click", "pos": (3, 4)}))   # click at 3,4
print(handle({"type": "key", "key": "a"}))        # typed a
print(handle(["go", "north", "fast"]))            # command go with ['north', 'fast']
print(handle(Point(0, 0)))                        # origin
```

How to read the patterns:

- **Literals** (`200`, `"quit"`, `None`) compare with the value.
- A **bare name** (`x`, `cmd`, `k`) matches anything and **captures** it into that variable.
- `_` matches anything and captures nothing.
- `[a, b]` or `(a, *rest)` match sequences; `{"key": value}` matches dictionaries (extra keys are allowed); `Point(x=0)` checks the type and its attributes.
- `|` combines alternatives, `as` names a whole sub-pattern, and `if` adds a **guard** condition.

Cases are tried top to bottom and the first match wins, so put specific patterns before general ones.

> **Pitfall:** `case RED:` does **not** compare with a constant named `RED`. A bare name always captures, so this case matches everything and stores it in a new variable `RED`. Use a dotted name: `case Color.RED:`.

::: deeper
Pattern kinds in full:

- **Literal**: numbers, strings, `None`, `True`, `False` (singletons compare with `is`, others with `==`).
- **Capture** and **wildcard**: bare names bind; `_` never binds.
- **Value**: dotted names (`Color.RED`, `math.pi`) are looked up and compared with `==`.
- **Sequence**: matches lists, tuples and other sequences, but **not** `str`, `bytes` or `bytearray`, so a string never matches `[a, b]` by accident. `*rest` captures the remainder (at most one star per pattern).
- **Mapping**: matches any `Mapping`; keys must be literals or value patterns; `**rest` captures the other keys.
- **Class**: `Point(x=0)` runs `isinstance(subject, Point)` and then checks attributes. Positional forms such as `Point(0, 0)` use the class attribute `__match_args__`, which dataclasses define automatically. `str(k)`, `int(n)` and other built-ins match the whole subject and bind it.
- **OR** (`p1 | p2`): every alternative must bind the same names. **AS** (`case [x, y] as pair`) binds the whole match too.

> **Tip:** `match` shines for parsers, command interpreters, abstract syntax tree walkers and JSON-shaped messages. For a simple value-to-action mapping, a dictionary is still simpler.

> **Interview:** "How is `match` different from a switch statement?" It matches structure, not just values: it can check types, lengths and keys and bind sub-values in the same step, with guards for extra conditions.
:::

::: check
?? What does `case [first, *rest]:` match, and what does it bind?
Any sequence (not a string) with at least one item. `first` gets the first item and `rest` a list of the others.

?? Why is `case RED:` a bug when `RED` is a constant?
A bare name is a capture pattern: it matches anything and rebinds `RED`. Use a dotted name such as `Color.RED` to compare.

?? In what order are `case` clauses tried?
Top to bottom; the first matching pattern (whose guard is also true) wins.
:::

## Wrap-up

### Cheat sheet

| Task | Code |
|---|---|
| Create and activate a venv | `python3 -m venv .venv` then `source .venv/bin/activate` |
| Install a package | `python -m pip install requests` |
| Run a file / a module | `python app.py` / `python -m pkg.mod` |
| Pause in the debugger | `breakpoint()` |
| Copy a list instead of sharing it | `new = old.copy()` |
| Format a number | `f"{x:,.2f}"`, debug with `f"{x=}"` |
| Check for "no value" | `if x is None:` |
| Check for "empty" | `if not items:` |
| Is it a kind of X? | `isinstance(x, (int, float))` |
| Bounds check | `0 <= i < len(xs)` |
| One-line choice | `a if cond else b` |
| Loop with a counter | `for i, x in enumerate(xs, start=1):` |
| Loop over two lists | `for a, b in zip(xs, ys, strict=True):` |
| Search without a flag | `for ...: if found: break` with `else:` for "not found" |
| Read until empty | `while (chunk := f.read(4096)):` |
| Branch on data shape | `match value:` with `case [x, y]:` and `case {"k": v}:` |

### Practice

::: try
**1. Shared lists.** Predict the output, then run it.

```python
a = [1, 2]
b = a
c = a.copy()
b.append(3)
c.append(4)
print(a, b, c)
```
:::

?? Answer to 1
`[1, 2, 3] [1, 2, 3] [1, 2, 4]`. `b` is the same list as `a`; `c` is a separate copy.

::: try
**2. Number guessing.** Write a program that picks `secret = 7`, then repeatedly asks the user for a guess with `input`, prints "too low" or "too high", and stops with "correct after N guesses". Use `while`, `int()` and an f-string.
:::

?? A solution to 2

```python
secret, tries = 7, 0
while True:
    guess = int(input("Guess: "))
    tries += 1
    if guess < secret:
        print("too low")
    elif guess > secret:
        print("too high")
    else:
        print(f"correct after {tries} guesses")
        break
```

::: try
**3. FizzBuzz with `match`.** For each number from 1 to 15, print "FizzBuzz" if it is divisible by 3 and 5, "Fizz" if by 3, "Buzz" if by 5, otherwise the number. Hint: `match (n % 3, n % 5):` with patterns such as `case (0, 0):`.
:::

?? A solution to 3

```python
for n in range(1, 16):
    match (n % 3, n % 5):
        case (0, 0): print("FizzBuzz")
        case (0, _): print("Fizz")
        case (_, 0): print("Buzz")
        case _: print(n)
```

::: try
**4. Find the first negative.** Given `readings = [4, 7, 0, -2, 5]`, print the index and value of the first negative reading, or "all non-negative" if there is none. Write it once with `for`, `enumerate`, `break` and `else`, and once with `next()`.
:::

?? A solution to 4

```python
readings = [4, 7, 0, -2, 5]

for i, r in enumerate(readings):
    if r < 0:
        print(i, r)                  # 3 -2
        break
else:
    print("all non-negative")

hit = next(((i, r) for i, r in enumerate(readings) if r < 0), None)
print(hit if hit is not None else "all non-negative")   # (3, -2)
```

### What comes next

You now know how to set up a project and how Python's basic statements work. The next section, **Built-in types in depth**, looks closely at the objects your names point to: numbers, strings, lists, tuples, dictionaries and sets, and which one to choose for each job.
