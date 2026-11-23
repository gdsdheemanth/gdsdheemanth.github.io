"""python3 decheck.py [sid ...]  -> structure check of course/<sid>.md against skeleton.json."""
import json, re, sys, os, glob
V = os.path.dirname(os.path.abspath(__file__))
sk = {s["id"]: s for s in json.load(open(os.path.join(V, "skeleton.json")))}
FIGKEYS = set()
for f in glob.glob(os.path.join(V, "figs", "*.js")):
    FIGKEYS |= set(re.findall(r"^\s*([A-Za-z_][\w]*)\s*:\s*\{\s*t\s*:", open(f).read(), re.M))
sids = sys.argv[1:] or sorted(os.path.basename(f)[:-3] for f in glob.glob(os.path.join(V, "course", "*.md")))
problems = 0; total_t = 0; total_w = 0
def P(*a):
    global problems; problems += 1; print("  PROBLEM", *a)
for sid in sids:
    s = sk.get(sid); t = open(os.path.join(V, "course", sid + ".md")).read()
    if not s: P(sid, "unknown section"); continue
    if t.count("\n```") % 2 and not t.startswith("```"): pass
    if len(re.findall(r"^```", t, re.M)) % 2: P(sid, "unclosed code fence")
    if "</script" in t.lower(): P(sid, "script terminator")
    for k in re.findall(r"\[\[fig:([\w-]+)\]\]", t):
        if k not in FIGKEYS: P(sid, "missing figure", k)
    fence = False; opens = closes = 0
    for l in t.split("\n"):
        if l.startswith("```"): fence = not fence
        if fence: continue
        if re.match(r"^:::\s*\w+\s*$", l): opens += 1
        elif re.match(r"^:::\s*$", l): closes += 1
    if opens != closes: P(sid, "unbalanced ::: blocks", opens, closes)
    groups = []; cur = top = None
    for l in t.split("\n"):
        if l.startswith("## "): cur = {"t": l[3:].strip(), "items": []}; groups.append(cur); top = None; continue
        if l.startswith("### ") and cur is not None: top = {"t": l[4:].strip(), "body": ""}; cur["items"].append(top); continue
        if top is not None: top["body"] += l + "\n"
    want = s["groups"]
    if len(groups) < len(want): P(sid, "group count", len(groups), "<", len(want))
    if not any(g["t"] == "Wrap-up" for g in groups[len(want):]): P(sid, "no ## Wrap-up group after the data groups")
    for gi, g in enumerate(want):
        if gi >= len(groups): break
        dg = groups[gi]
        if dg["t"] != g["t"]: P(sid, "group title", repr(dg["t"]), "!=", repr(g["t"]))
        if len(dg["items"]) != len(g["topics"]): P(sid, g["t"], "topic count", len(dg["items"]), "!=", len(g["topics"]))
        for ii, tp in enumerate(g["topics"]):
            if ii >= len(dg["items"]): break
            d = dg["items"][ii]; b = d["body"]
            if d["t"] != tp["t"]: P(sid, "topic title", repr(d["t"]), "!=", repr(tp["t"]))
            for k in ("plain", "check", "resources"):
                if not re.search(r"^:::\s*" + k + r"\s*$", b, re.M): P(sid, "missing", k, "in", tp["t"][:50])
            m = re.search(r"^:::\s*resources\s*$(.*?)^:::\s*$", b, re.M | re.S)
            if m and len(re.findall(r"^\s*-\s+", m.group(1), re.M)) < 3: P(sid, "fewer than 3 resources in", tp["t"][:50])
            if re.search(r"aws\.amazon\.com|azure\.microsoft\.com|learn\.microsoft\.com/[^ )]*azure", b): P(sid, "AWS/Azure link in", tp["t"][:50])
            w = len(b.split()); total_t += 1; total_w += w
    print(f"{sid}: {sum(len(g['items']) for g in groups[:len(want)])} topics, {len(t.split())} words")
print(json.dumps({"topics": total_t, "words": total_w, "problems": problems}))
