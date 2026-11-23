"""python3 skel.py <sid>  -> readable skeleton of a section: groups, exact topic titles, scope, original resources, overlap links."""
import json, sys, os
V = os.path.dirname(os.path.abspath(__file__))
sk = json.load(open(os.path.join(V, "skeleton.json")))
order = [s["id"] for s in sk]
s = next(x for x in sk if x["id"] == sys.argv[1])
i = order.index(s["id"])
print(f"# {s['id']}: {s['t']}\n{s['d']}\nPart: {s['part'][0]}\nPrevious: {sk[i-1]['t'] if i else '(none, first section)'}\nNext: {sk[i+1]['t'] if i+1 < len(sk) else '(none, last section)'}")
lk = s.get("link") or {}
for g in s["groups"]:
    print(f"\n## {g['t']}")
    for t in g["topics"]:
        flag = ""
        for guide, lst in lk.items():
            if lst == "*" or (isinstance(lst, list) and t.get("orig", t["t"]) in lst): flag = f"   [OVERLAP: shorter article + ::: guide link to the {guide} guide]"
        print(f"### {t['t']}{flag}\n    scope: {t.get('sub','')}" + ("" if t.get("orig") in (None, t["t"]) else f"\n    (original title: {t['orig']})") + ("\n    NEW TOPIC" if t.get("new") else ""))
        for r in t.get("res", []):
            print(f"    orig resource: {r[0]} | {r[2]} | {r[1]}")
