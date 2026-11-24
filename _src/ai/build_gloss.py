"""Build agentic-ai-glossary.html from glossary/<cat>.md (intro, then ## groups with ### terms)."""
import os, re, json, html
V = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.normpath(os.path.join(V, "..", "..", "agentic-ai-glossary.html"))
order = json.load(open(os.path.join(V, "gloss_order.json")))
secs, blobs, nterms = [], "", 0
for cid, title, d in order:
    p = os.path.join(V, "glossary", cid + ".md")
    if not os.path.exists(p): continue
    t = open(p).read(); assert "</script" not in t.lower()
    groups = []
    for line in t.split("\n"):
        if line.startswith("## "): groups.append([line[3:].strip(), [], [], []])
        elif line.startswith("### ") and groups: groups[-1][1].append([line[4:].strip(), ""]); nterms += 1
    groups = [g for g in groups if g[0] != "Wrap-up"]
    secs.append({"id": cid, "t": title, "lv": ["Glossary"], "d": d, "f": [], "g": groups, "r": []})
    blobs += '<script type="text/plain" data-course="%s">\n%s\n</script>\n' % (cid, t)
parts = [{"n": "Glossary", "k": "kafka", "i": "Every agentic AI term, explained in plain words.", "s": secs}]
js = open(os.path.join(V, "figs_helpers.js")).read() + "\nvar PARTS = " + json.dumps(parts, ensure_ascii=False) + ";\n" + open(os.path.join(V, "app_gloss.js")).read()
assert "</script" not in js.lower()
open(OUT, "w").write(open(os.path.join(V, "shell_gloss.html")).read() + blobs + "<script>\n" + js + "\n</script>\n</body>\n</html>\n")
print("wrote", OUT, len(secs), "categories,", nterms, "terms")
