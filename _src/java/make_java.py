"""Build skeleton.json and generated course files (static tabs, track overviews, interview Q&A)
from original.html. Core Java sections (corebasics, corecoll, corefunc) are written by hand/agents
and are never overwritten here."""
import json, re, html, os, subprocess
V = os.path.dirname(os.path.abspath(__file__))
src = open(os.path.join(V, "original.html")).read()
D = json.load(open(os.path.join(V, "orig_data.json")))
R, T, P, Q = D["R"], D["T"], D["P"], D["Q"]

def inline(h):
    h = re.sub(r"<br\s*/?>", " ", h)
    h = re.sub(r"<code>(.*?)</code>", lambda m: "`" + html.unescape(re.sub("<[^>]+>", "", m.group(1))) + "`", h, flags=re.S)
    h = re.sub(r'<a [^>]*href="([^"]+)"[^>]*>(.*?)</a>', lambda m: "[" + re.sub("<[^>]+>", "", m.group(2)).strip() + "](" + m.group(1) + ")", h, flags=re.S)
    h = re.sub(r"</?(b|strong)>", "**", h)
    h = re.sub(r"</?(i|em)>", "*", h)
    h = re.sub(r"<[^>]+>", "", h)
    h = html.unescape(h)
    return re.sub(r"\s+", " ", h).strip()

def to_md(frag):
    out = []
    frag = re.sub(r"<!--.*?-->", "", frag, flags=re.S)
    tokens = re.split(r"(<h[234][^>]*>.*?</h[234]>|<p[^>]*>.*?</p>|<ul[^>]*>.*?</ul>|<ol[^>]*>.*?</ol>|<table[^>]*>.*?</table>|<details[^>]*>.*?</details>|<pre[^>]*>.*?</pre>)", frag, flags=re.S)
    for t in tokens:
        t = t.strip()
        if not t: continue
        m = re.match(r"<h([234])[^>]*>(.*?)</h\1>", t, re.S)
        if m:
            lvl = {"2": None, "3": "## ", "4": "### "}[m.group(1)]
            if lvl: out.append(lvl + inline(m.group(2)))
            continue
        if t.startswith("<p"): out.append(inline(t)); continue
        if t.startswith("<ul") or t.startswith("<ol"):
            pre = "1. " if t.startswith("<ol") else "- "
            out.append("\n".join(pre + inline(li) for li in re.findall(r"<li[^>]*>(.*?)</li>", t, re.S))); continue
        if t.startswith("<table"):
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.S); lines = []
            for ri, r in enumerate(rows):
                cells = [inline(c).replace("|", "\\|") for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", r, re.S)]
                lines.append("| " + " | ".join(cells) + " |")
                if ri == 0: lines.append("|" + "---|" * len(cells))
            out.append("\n".join(lines)); continue
        if t.startswith("<details"):
            sm = re.search(r"<summary[^>]*>(.*?)</summary>(.*)</details>", t, re.S)
            out.append("### " + inline(sm.group(1)) + "\n\n" + to_md(sm.group(2))); continue
        if t.startswith("<pre"):
            out.append("```text\n" + html.unescape(re.sub(r"<[^>]+>", "", re.search(r"<pre[^>]*>(.*?)</pre>", t, re.S).group(1))).strip("\n") + "\n```"); continue
        txt = inline(t)
        if txt: out.append(txt)
    return "\n\n".join(out)

def tab_html(tid):
    m = re.search(r'<section class="tab" id="%s"[^>]*>(.*?)</section>' % tid, src, re.S)
    return m.group(1)

def resources(lst):
    ty = {"Book": "Book", "YouTube": "Video", "Paid": "Course"}
    return "::: resources\n" + "\n".join("- " + (ty[r[3]] + ": " if r[3] in ty else "") + "[" + r[0] + "](" + r[1] + ") · " + r[2] + (" (paid)" if r[3] == "Paid" else "") for r in lst) + "\n:::"

sections = []
def add(part, sid, title, d, groups):
    sections.append(dict(part=part, id=sid, t=title, d=d, groups=groups))

PS = ("Start here", "spark", "The 24-week plan, the combined project, finding roles and telling your switch story.")
add(PS, "start", "The plan: 24-week roadmap", "How to use this guide, the week-by-week roadmap, a daily routine and version notes.", [])
add(PS, "projects", "The combined project: ShopSphere", "One portfolio project that uses every skill in the guide, built in phases.", [])
add(PS, "jobs", "Roles and applying", "Job titles, where to target, resume and LinkedIn setup, referrals and offers.", [])
add(PS, "story", "The switch story", "How to explain your career switch in interviews, with answers to the hard questions.", [])

jg = T["java"]["groups"]
def grp(g): return dict(t=g[0], topics=[dict(t=it[0], sub=it[1]) for it in g[1]])
PC = ("Core Java in depth", "stream", "The foundation, taught in full depth: language basics, OOP, exceptions, generics, collections, functional Java, I/O and build tools.")
add(PC, "corebasics", "Core Java 1: language basics and OOP", "The JVM, types, strings and control flow, then classes, the four pillars of OOP, and the Object contract.", [grp(jg[0]), grp(jg[1])])
add(PC, "corecoll", "Core Java 2: exceptions, generics and collections", "Handling errors properly, writing type-safe generic code, and the collections every Java interview asks about.", [grp(jg[2]), grp(jg[3]), grp(jg[4])])
add(PC, "corefunc", "Core Java 3: functional Java, I/O and build tools", "Lambdas, streams, collectors, Optional and dates, then files, JSON, money with BigDecimal, Maven and Gradle.", [grp(jg[5]), grp(jg[6])])

tracks = [("Back end", "kafka", "Advanced Java, SQL, Spring Boot, microservices and Kafka.", ["advjava", "sql", "spring", "micro", "kafka"]),
          ("Front end", "prep", "HTML, CSS, JavaScript, TypeScript and React.", ["frontend"]),
          ("Engineering practice", "spark", "DevOps and cloud, testing, software engineering practices, DSA and system design.", ["devops", "testing", "se", "dsa"])]
for pn, pk, pi, keys in tracks:
    for k in keys:
        t = T[k]
        add((pn, pk, pi), k, t["title"], t["intro"], [grp(g) for g in t["groups"]])
add(("Interview prep", "prep", "The question bank with model answers, by topic.", ), "iq", "Interview Q&A", "Model answers to the questions Java full stack interviews ask most, from Core Java to HR.", []) if False else None
sections.append(dict(part=("Interview prep", "prep", "The question bank with model answers, by topic."), id="iq", t="Interview Q&A", d="Model answers to the questions Java full stack interviews ask most, from Core Java to HR.", groups=[]))
json.dump(sections, open(os.path.join(V, "skeleton.json"), "w"), indent=1, ensure_ascii=False)

def write(sid, text):
    open(os.path.join(V, "course", sid + ".md"), "w").write(text.strip() + "\n")

# static tabs
for sid in ("start", "projects", "jobs", "story"):
    h = tab_html(sid)
    intro_m = re.search(r'<p class="intro">(.*?)</p>', h, re.S)
    body = h.replace(intro_m.group(0), "", 1) if intro_m else h
    body = re.sub(r'<div[^>]*id="(overall|tabProg|res-start|res-jobs)[^"]*"[^>]*>.*?</div>', "", body, flags=re.S)
    md = (inline(intro_m.group(1)) + "\n\n" if intro_m else "") + to_md(body)
    md = re.sub(r"## Overall progress\n.*?(?=\n## )", "", md, flags=re.S)
    if sid == "start": md += "\n\n## Master roadmaps and references\n\n" + resources(R["start"])
    if sid == "jobs": md += "\n\n## Job boards and resources\n\n" + resources(R["jobs"])
    # first ## must exist: content before it is the intro
    write(sid, md)

# track overview pages (not core java)
for k in [x for _, _, _, ks in tracks for x in ks]:
    t = T[k]; md = [t["intro"], "", "This track lists every topic to learn, in order, with mini projects to build and the best resources. Work through the topics with the resources below, then build at least two of the projects."]
    for g in t["groups"]:
        md += ["", "## " + g[0], ""]
        for it in g[1]:
            md += ["### " + it[0], "", (it[1] + "." if it[1] and not it[1].endswith(".") else it[1]) or "Learn this from the resources at the end of the section.", ""]
    if P.get(k):
        md += ["## Hands-on mini projects", "", "Build at least two of these before moving on. Put each on GitHub with a short README.", ""]
        for p in P[k]:
            md += ["### " + p[0] + " (" + p[1] + ")", "", p[2], "", "**Practises:** " + p[3], ""]
    md += ["## Resources", "", resources(R.get(k, []))]
    write(k, "\n".join(md))

# interview Q&A
md = ["Model answers to the questions asked most often in Java full stack interviews. Read the question, answer it out loud first, then open the answer. For behavioural questions, use the STAR format (situation, task, action, result).", ""]
for cat, items in Q:
    md += ["## " + cat, ""]
    for q, a in items:
        md += ["?? " + q, a.replace("\n", " "), ""]
write("iq", "\n".join(md))
print("sections", len(sections), "topics", sum(len(g["topics"]) for s in sections for g in s["groups"]))
