import json, sys, re
sys.path.insert(0, ".")
from skeleton_src import S
orig = json.load(open("de_orig.json"))
by = {}
for di, d in enumerate(orig):
    for t in d["topics"]:
        by.setdefault(t["n"], []).append(dict(t, dom=d["t"]))
REMOVED = {"Amazon Redshift — Internals & Tuning", "Azure Synapse Analytics", "AWS Data Ecosystem — Deep Dive", "Azure Data Ecosystem — Deep Dive", "Microsoft Fabric — Unified Analytics"}
used = {}
out = []
for s in S:
    groups = []
    for gt, items in s["g"]:
        tl = []
        for it in items:
            if it.startswith("+"):
                title, _, sub = it[1:].partition(" | ")
                tl.append(dict(t=title.strip(), sub=sub.strip(), res=[], new=True))
            else:
                src, _, newt = it[1:].partition(" => ")
                src = src.strip()
                assert src in by, ("missing", src)
                k = used.get(src, 0); used[src] = k + 1
                o = by[src][min(k, len(by[src]) - 1)]
                tl.append(dict(t=(newt or src).strip(), orig=src, sub=o["s"], res=o["res"], dom=o["dom"]))
        groups.append(dict(t=gt, topics=tl))
    lk = s["link"]
    out.append(dict(part=s["part"], id=s["id"], t=s["t"], d=s["d"], groups=groups, link=lk))
MERGE = {"Wasm & Portable Data Compute": "WASM for Portable Data Compute"}
for srcn, dst in MERGE.items():
    for sx in out:
        for g in sx["groups"]:
            for t in g["topics"]:
                if t.get("orig") == dst:
                    have = {r[1] for r in t["res"]}
                    t["res"] += [r for r in by[srcn][0]["res"] if r[1] not in have]
    used[srcn] = 1
all_orig = [(t["n"]) for d in orig for t in d["topics"]]
from collections import Counter
cnt = Counter(all_orig)
missing = [n for n in cnt if n not in REMOVED and used.get(n, 0) == 0]
over = [n for n in used if used[n] > cnt[n]]
titles = Counter(t["t"] for s in out for g in s["groups"] for t in g["topics"])
dupt = [t for t, c in titles.items() if c > 1]
n = sum(len(g["topics"]) for s in out for g in s["groups"])
new = sum(1 for s in out for g in s["groups"] for t in g["topics"] if t.get("new"))
print(json.dumps(dict(sections=len(out), topics=n, new=new, missing=missing, overused=over, duplicateTitles=dupt), indent=1))
json.dump(out, open("skeleton.json", "w"), indent=1, ensure_ascii=False)
for s in out: print(s["id"], sum(len(g["topics"]) for g in s["groups"]))
