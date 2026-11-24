"""Build index.html (Data engineering course) from skeleton.json, course/*.md and figs/*.js."""
import glob, os, html, json
V = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.normpath(os.path.join(V, "..", "..", "agentic-ai-guide.html"))
shell = open(os.path.join(V, "shell.html")).read()
sk = json.load(open(os.path.join(V, "skeleton.json")))
PK = {}
parts = []
for s in sk:
    pn, pk, pi = s["part"]
    if pn not in PK:
        PK[pn] = len(parts); parts.append({"n": pn, "k": pk, "i": pi, "s": []})
    parts[PK[pn]]["s"].append({"id": s["id"], "t": s["t"], "lv": ["Course"], "d": s["d"], "f": [],
        "g": [[g["t"], [[t["t"], t.get("sub", "")] for t in g["topics"]], [], []] for g in s["groups"]], "r": []})
data = "var PARTS = " + json.dumps(parts, ensure_ascii=False) + ";\n"
js = open(os.path.join(V, "figs_helpers.js")).read()
for f in sorted(glob.glob(os.path.join(V, "figs", "*.js"))):
    js += "\n" + open(f).read()
js += "\n" + data + open(os.path.join(V, "app.js")).read()
assert "</script" not in js.lower(), "script terminator inside JS"
blobs, n = "", 0
for f in sorted(glob.glob(os.path.join(V, "course", "*.md"))):
    sid = os.path.splitext(os.path.basename(f))[0]
    text = open(f).read()
    assert "</script" not in text.lower(), "script terminator in course " + sid
    blobs += '<script type="text/plain" data-course="%s">\n%s\n</script>\n' % (html.escape(sid), text); n += 1
out = shell + blobs + "<script>\n" + js + "\n</script>\n</body>\n</html>\n"
open(OUT, "w").write(out)
print("wrote", OUT, len(out), "bytes,", n, "course sections of", len(sk))
