(function(){
"use strict";
var KEY = "de-e2e-guide-v1";
var ROMAN = ["I","II","III","IV","V","VI","VII","VIII","IX","X","XI","XII","XIII","XIV","XV"];
function load(k, d){ try { var v = localStorage.getItem(KEY + ":" + k); return v == null ? d : JSON.parse(v); } catch (e) { return d; } }
function store(k, v){ try { localStorage.setItem(KEY + ":" + k, JSON.stringify(v)); } catch (e) {} }
var done = load("done", {}) || {};
var prefs = load("prefs", {}) || {};
prefs.mode = prefs.mode || "read"; prefs.level = prefs.level || "All";

function $(s){ return document.querySelector(s); }
function $$(s, r){ return Array.prototype.slice.call((r || document).querySelectorAll(s)); }
function esc(s){ return String(s).replace(/[&<>"]/g, function(c){ return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]; }); }
function fmt(s){ return esc(s).replace(/`([^`]+)`/g, "<code>$1</code>"); }
function plain(s){ return String(s).replace(/`/g, ""); }

/* index the data */
var SECS = [], BY = {}, TOTAL = 0;
PARTS.forEach(function(p, pi){
  p.s.forEach(function(s){
    s.pi = pi; s.k = p.k; s.ids = [];
    s.g.forEach(function(g, gi){ g[1].forEach(function(it, ii){ s.ids.push(s.id + "." + gi + "." + ii); }); });
    TOTAL += s.ids.length; SECS.push(s); BY[s.id] = s;
  });
});

var main = $("#main"), rail = $("#rail"), navParts = $("#navParts"), shell = $("#shell"), q = $("#q");
var current = null;

function lvOk(s){ return true; }
function isRead(s){ return !!done["sec:" + s.id]; }

/* nav */
function renderNav(){
  var h = "";
  PARTS.forEach(function(p, pi){
    var secs = p.s.filter(lvOk); if (!secs.length) return;
    var collapsed = (prefs.collapsed || {})[pi] ? " collapsed" : "";
    h += '<div class="np' + collapsed + '" data-k="' + p.k + '" data-pi="' + pi + '"><button class="nph" type="button" aria-expanded="' + (!collapsed) + '"><i></i><span>' + ROMAN[pi] + ". " + esc(p.n) + '</span><b data-pc="' + pi + '"></b></button><ol>';
    secs.forEach(function(s){ h += '<li><a href="#' + s.id + '" data-sec="' + s.id + '"><span>' + esc(s.t) + '</span><small data-nc="' + s.id + '"></small></a></li>'; });
    h += "</ol></div>";
  });
  navParts.innerHTML = h || '<p class="empty">No sections at this level.</p>';
  counts(); markNav();
}
navParts.addEventListener("click", function(e){
  var b = e.target.closest(".nph");
  if (b){ var np = b.parentElement, pi = np.dataset.pi; np.classList.toggle("collapsed"); b.setAttribute("aria-expanded", String(!np.classList.contains("collapsed"))); prefs.collapsed = prefs.collapsed || {}; prefs.collapsed[pi] = np.classList.contains("collapsed"); store("prefs", prefs); return; }
  if (e.target.closest("a")) document.body.classList.remove("navopen");
});
function markNav(){
  $$(".np a").forEach(function(a){ if (current && a.dataset.sec === current.id) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
  var cur = current && navParts.querySelector('a[data-sec="' + current.id + '"]');
  if (cur){ var np = cur.closest(".np"); if (np.classList.contains("collapsed")){ np.classList.remove("collapsed"); } var r = cur.getBoundingClientRect(), nr = $("#nav").getBoundingClientRect(); if (r.top < nr.top + 60 || r.bottom > nr.bottom) cur.scrollIntoView({block:"center"}); }
}

/* reading progress: whole sections */
function counts(){
  var all = 0;
  PARTS.forEach(function(p, pi){ var d = p.s.filter(isRead).length; var el = navParts.querySelector('[data-pc="' + pi + '"]'); if (el) el.textContent = d + "/" + p.s.length; });
  SECS.forEach(function(s){
    var r = isRead(s); if (r) all++;
    $$('[data-nc="' + s.id + '"]').forEach(function(el){ el.textContent = r ? "✓" : ""; el.parentElement.classList.toggle("full", r); });
    $$('[data-mini="' + s.id + '"]').forEach(function(el){ el.style.width = r ? "100%" : "0"; });
    $$('[data-mark="' + s.id + '"]').forEach(function(b){ b.setAttribute("aria-pressed", String(r)); b.textContent = r ? "✓ Read" : "Mark section as read"; });
  });
  var pct = SECS.length ? Math.round(all / SECS.length * 100) : 0;
  $("#ring").style.setProperty("--p", pct); $("#progLbl").textContent = pct + "% · " + all + "/" + SECS.length;
  var hp = $("#homePct"); if (hp) hp.textContent = pct + "%";
  var hd = $("#homeDone"); if (hd) hd.textContent = all;
}

/* resources */
function kind(u){
  if (!u) return "Book";
  if (/youtube\.com|youtu\.be/.test(u)) return "Video";
  if (/\.pdf($|\?)|vldb\.org|usenix\.org|research\.google|arxiv\.org|aphyr\.com/.test(u)) return "Paper";
  if (/github\.com/.test(u)) return "Repo";
  if (/peps\.python\.org/.test(u)) return "PEP";
  if (/leetcode\.com|neetcode\.io|hackerrank\.com|codeforces\.com|exercism\.org/.test(u)) return "Practice";
  if (/docs\.python\.org|devguide\.python\.org|packaging\.python\.org|wiki\.python\.org|readthedocs\.io|cp-algorithms\.com/.test(u)) return "Docs";
  if (/succinctly-free-ebooks|oreilly\.com\/library|oreilly\.com\/catalog|manning\.com|pragprog\.com|dataintensive\.net|amazon\.[a-z.]+\/.*\/dp\//.test(u)) return "Book";
  if (/coursera\.org|udemy\.com|cloudskillsboost\.google|skills\.google|datacamp\.com|educative\.io|edx\.org|learn\.|academy|\/courses?\//.test(u)) return "Course";
  if (/cloud\.google\.com\/[a-z-]+\/docs|cloud\.google\.com\/architecture|cloud\.google\.com\/docs|developer\.hashicorp|kubernetes\.io|postgresql\.org\/docs|redis\.io\/docs|flink\.apache|beam\.apache|trino\.io|duckdb\.org|clickhouse\.com\/docs|druid\.apache|pinot\.apache|delta\.io|paimon\.apache|arrow\.apache|avro\.apache|orc\.apache|hadoop\.apache|hive\.apache|hbase\.apache|getdbt\.com|dagster\.io|prefect\.io|datahubproject|cassandra\.apache|calcite\.apache|velox-lib|starrocks\.io|doris\.apache|pulsar\.apache|nifi\.apache|dvc\.org|lakefs\.io|mlflow\.org/.test(u)) return "Docs";
  if (/cloud\.google\.com\/blog|engineering\.|\/blog\/|medium\.com|substack\.com/.test(u)) return "Blog";
  if (/developer\.confluent\.io\/courses|databricks\.com\/learn|datavidhya\.com\/learn|certification/.test(u)) return "Course";
  if (/spark\.apache\.org|kafka\.apache\.org|cwiki\.apache\.org|issues\.apache\.org|iceberg\.apache\.org|hudi\.apache\.org|parquet\.apache\.org|airflow\.apache\.org|docs\.|openlineage\.io|datafusion\.apache|gluten\.apache|celeborn\.apache|books\.japila|graphframes\.io|debezium\.io/.test(u)) return "Docs";
  return "Article";
}
function resBox(list, title){
  if (!list || !list.length) return "";
  return '<div class="res"><h4>' + (title || "Read more") + '</h4><ul>' + list.map(function(r){
    var ty = r[3] || kind(r[1]);
    var main = r[1] ? '<a href="' + esc(r[1]) + '" target="_blank" rel="noopener">' + esc(r[0]) + '</a>' : '<cite>' + esc(r[0]) + '</cite>';
    return '<li><span class="ty">' + ty + '</span><span>' + main + (r[2] ? ' <em>· ' + fmt(r[2]) + '</em>' : '') + '</span></li>';
  }).join("") + '</ul></div>';
}

/* figures */
function figs(keys){
  return (keys || []).map(function(k){
    var f = FIGS[k]; if (!f) return "";
    var body = f.svg ? f.svg : '<img src="' + esc(f.img) + '" alt="' + esc(f.alt || f.cap) + '" loading="lazy" decoding="async"' + (f.w ? ' width="' + f.w + '" height="' + f.h + '"' : '') + '>';
    return '<figure class="fig" data-fig="' + k + '"><div class="canvas" role="button" tabindex="0" aria-label="Enlarge diagram: ' + esc(f.cap) + '">' + body + '</div><figcaption><span><b>' + esc(f.t || "Diagram") + '.</b> ' + fmt(f.cap) + '</span>' + (f.credit ? '<small>' + esc(f.credit) + '</small>' : '<small>Original diagram</small>') + '</figcaption></figure>';
  }).join("");
}
var lb = $("#lb");
function openFig(fig){
  var f = FIGS[fig.dataset.fig]; if (!f || !lb.showModal) return;
  $("#lbin").innerHTML = fig.querySelector(".canvas").innerHTML; $("#lbcap").textContent = (f.t ? f.t + ". " : "") + plain(f.cap);
  lb.showModal();
}
$("#lbclose").addEventListener("click", function(){ lb.close(); });
lb.addEventListener("click", function(e){ if (e.target === lb) lb.close(); });

/* lessons: a small markdown dialect */
var LESSON = {};
$$('script[data-lesson]').forEach(function(el){ LESSON[el.dataset.lesson] = el.textContent.replace(/^\n+/, ""); });
var DEEP = {};
$$('script[data-deep]').forEach(function(el){
  var sid = el.dataset.deep, groups = [], cur = null, topic = null;
  el.textContent.replace(/\r/g, "").split("\n").forEach(function(line){
    if (/^## /.test(line)){ cur = {title: line.slice(3).trim(), topics: []}; groups.push(cur); topic = null; return; }
    if (/^### /.test(line) && cur){ topic = {title: line.slice(4).trim(), body: []}; cur.topics.push(topic); return; }
    if (topic) topic.body.push(line); else if (cur){ cur.intro = (cur.intro || "") + line + "\n"; }
  });
  groups.forEach(function(g){ g.topics.forEach(function(t){ t.body = t.body.join("\n").trim(); }); });
  DEEP[sid] = groups;
});
var COURSE = {};
$$('script[data-course]').forEach(function(el){
  var c = {intro: "", groups: []}, cur = null, topic = null;
  el.textContent.replace(/\r/g, "").split("\n").forEach(function(line){
    if (/^## /.test(line)){ cur = {title: line.slice(3).trim(), intro: "", topics: []}; c.groups.push(cur); topic = null; return; }
    if (/^### /.test(line) && cur){ topic = {title: line.slice(4).trim(), body: []}; cur.topics.push(topic); return; }
    if (topic) topic.body.push(line); else if (cur) cur.intro += line + "\n"; else c.intro += line + "\n";
  });
  c.groups.forEach(function(g){ g.topics.forEach(function(t){ t.body = t.body.join("\n").trim(); }); });
  COURSE[el.dataset.course] = c;
});
function deepText(sid, gi, ii){
  var c = COURSE[sid]; if (c && c.groups[gi] && c.groups[gi].topics[ii]) return c.groups[gi].topics[ii].body;
  var d = DEEP[sid]; return d && d[gi] && d[gi].topics[ii] ? d[gi].topics[ii].body : "";
}
var LESSON_WORDS = 0; Object.keys(LESSON).forEach(function(k){ LESSON_WORDS += LESSON[k].split(/\s+/).length; });
Object.keys(DEEP).forEach(function(k){ DEEP[k].forEach(function(g){ g.topics.forEach(function(t){ LESSON_WORDS += t.body.split(/\s+/).length; }); }); });
Object.keys(COURSE).forEach(function(k){ LESSON_WORDS += COURSE[k].intro.split(/\s+/).length; COURSE[k].groups.forEach(function(g){ LESSON_WORDS += g.intro.split(/\s+/).length; g.topics.forEach(function(t){ LESSON_WORDS += t.body.split(/\s+/).length; }); }); });
function inl(s){
  var codes = [];
  var t = String(s).replace(/`([^`]+)`/g, function(_, c){ codes.push(c); return "\u0000" + (codes.length - 1) + "\u0000"; });
  var e = esc(t);
  e = e.replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');
  e = e.replace(/\*\*([^*]+?)\*\*/g, "<strong>$1</strong>");
  e = e.replace(/(^|[\s(])\*([^*\s][^*]*?)\*(?=[\s).,;:!?]|$)/g, "$1<em>$2</em>");
  return e.replace(/\u0000(\d+)\u0000/g, function(_, i){ return "<code>" + esc(codes[+i]) + "</code>"; });
}
function mdTable(rows){
  var cells = function(r){ return r.replace(/^\|/, "").replace(/\|\s*$/, "").split(/(?<!\\)\|/).map(function(c){ return c.trim().replace(/\\\|/g, "|"); }); };
  var head = cells(rows[0]), body = rows.slice(1).filter(function(r){ return !/^\|\s*:?-{2,}/.test(r); });
  return '<div class="tbl"><table><thead><tr>' + head.map(function(c){ return "<th>" + inl(c) + "</th>"; }).join("") + '</tr></thead><tbody>' + body.map(function(r){ return "<tr>" + cells(r).map(function(c){ return "<td>" + inl(c) + "</td>"; }).join("") + "</tr>"; }).join("") + "</tbody></table></div>";
}
var BLOCK = /^(```|#{2,4}\s|>|\?\?\s|\||- |\d+\.\s|\[\[fig:|:::)/;
var LAYER = {plain: "In plain words", deeper: "Go deeper", check: "Check yourself", try: "Try it yourself", guide: "Full depth in the companion guide"};
var RTYPES = /^(Book|Docs|Paper|Course|Video|Article|Repo|Talk|Blog|Spec|Tool):\s*/;
function resList(lines){
  var items = [];
  lines.forEach(function(l){
    var t = l.replace(/^\s*-\s+/, ""), ty = "", m2;
    if (!/^\s*-\s+/.test(l)) return;
    if ((m2 = t.match(RTYPES))){ ty = m2[1]; t = t.slice(m2[0].length); }
    var mm = t.match(/^\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)\s*(?:[·—–-]\s*(.*))?$/);
    if (mm) items.push([mm[1], mm[2], mm[3] || "", ty]); else items.push([t.replace(/^\*|\*$/g, ""), "", "", ty || "Book"]);
  });
  return resBox(items, "Resources to learn this topic in depth");
}
function layer(kind, inner){
  if (kind === "deeper") return '<details class="lay deeper"><summary><span>Go deeper</span><small>internals, edge cases and interview angles</small></summary><div class="layin">' + inner + '</div></details>';
  return '<div class="lay ' + esc(kind) + '"><div class="laytag">' + esc(LAYER[kind] || kind) + '</div>' + inner + '</div>';
}
function md(src, sid){
  var lines = src.replace(/\r/g, "").split("\n"), i = 0, h = "", heads = [], m;
  while (i < lines.length){
    var L = lines[i];
    if (/^\s*$/.test(L)){ i++; continue; }
    if ((m = L.match(/^```(\w*)/))){ var code = []; i++; while (i < lines.length && !/^```/.test(lines[i])){ code.push(lines[i]); i++; } i++; h += '<pre class="code"' + (m[1] ? ' data-lang="' + m[1] + '"' : "") + "><code>" + esc(code.join("\n")) + "</code></pre>"; continue; }
    if ((m = L.match(/^:::\s*(\w+)\s*$/))){ var inner = [], depth = 1, fence = false; i++; while (i < lines.length){ if (/^```/.test(lines[i])) fence = !fence; if (!fence){ if (/^:::\s*\w+\s*$/.test(lines[i])) depth++; else if (/^:::\s*$/.test(lines[i]) && !--depth) break; } inner.push(lines[i]); i++; } i++; if (m[1] === "resources"){ h += resList(inner); continue; } var sub = md(inner.join("\n"), sid); h += layer(m[1], sub.html); continue; }
    if ((m = L.match(/^####\s+(.*)$/))){ h += '<h4 class="lh4">' + inl(m[1]) + "</h4>"; i++; continue; }
    if ((m = L.match(/^(#{2,3})\s+(.*)$/))){ if (m[1].length === 2){ var aid = sid + "--l" + heads.length; heads.push([aid, m[2]]); h += '<h2 class="lh" id="' + aid + '">' + inl(m[2]) + "</h2>"; } else h += '<h3 class="lh3">' + inl(m[2]) + "</h3>"; i++; continue; }
    if ((m = L.match(/^\[\[fig:([\w-]+)\]\]\s*$/))){ h += figs([m[1]]); i++; continue; }
    if (/^>/.test(L)){ var q = []; while (i < lines.length && /^>/.test(lines[i])){ q.push(lines[i].replace(/^>\s?/, "")); i++; } var t = q.join(" "), kind = ((t.match(/^\*\*(\w+)/) || [])[1] || "note").toLowerCase(); h += '<div class="callout c-' + kind + '">' + inl(t) + "</div>"; continue; }
    if (/^\?\?\s/.test(L)){
      var qq = L.replace(/^\?\?\s+/, ""), ans = [], extra = ""; i++;
      while (i < lines.length && !/^\s*$/.test(lines[i]) && !/^\?\?\s/.test(lines[i])){ ans.push(lines[i]); i++; }
      if (!ans.length){ var j = i; while (j < lines.length && /^\s*$/.test(lines[j])) j++; var fm = j < lines.length && lines[j].match(/^```(\w*)/); if (fm){ var cl = []; j++; while (j < lines.length && !/^```/.test(lines[j])){ cl.push(lines[j]); j++; } i = j + 1; extra = '<pre class="code"' + (fm[1] ? ' data-lang="' + fm[1] + '"' : "") + "><code>" + esc(cl.join("\n")) + "</code></pre>"; } }
      h += '<details class="qa"><summary><span>' + inl(qq) + "</span></summary>" + (ans.length ? "<p>" + inl(ans.join(" ")) + "</p>" : "") + extra + "</details>"; continue;
    }
    if (/^\|/.test(L)){ var rows = []; while (i < lines.length && /^\|/.test(lines[i])){ rows.push(lines[i]); i++; } h += mdTable(rows); continue; }
    if (/^(- |\d+\.\s)/.test(L)){
      var ordered = /^\d+\.\s/.test(L), items = [];
      while (i < lines.length && (/^(- |\d+\.\s)/.test(lines[i]) || /^ {2,}\S/.test(lines[i]))){
        if (/^(- |\d+\.\s)/.test(lines[i])) items.push(lines[i].replace(/^(- |\d+\.\s+)/, "")); else items[items.length - 1] += " " + lines[i].trim();
        i++;
      }
      h += (ordered ? "<ol>" : "<ul>") + items.map(function(x){ return "<li>" + inl(x) + "</li>"; }).join("") + (ordered ? "</ol>" : "</ul>"); continue;
    }
    var buf = []; while (i < lines.length && !/^\s*$/.test(lines[i]) && !BLOCK.test(lines[i])){ buf.push(lines[i]); i++; }
    if (buf.length) h += "<p>" + inl(buf.join(" ")) + "</p>"; else i++;
  }
  return {html:h, heads:heads};
}

/* views */
function renderHome(){
  current = null; shell.classList.add("norail"); rail.innerHTML = "";
  var last = prefs.last && BY[prefs.last];
  var nfig = Object.keys(FIGS).length;
  var nles = SECS.length;
  var h = '<div class="col"><section class="hero"><p class="eyebrow">Complete data engineering course · Google Cloud edition</p><h1>Data engineering, <em>explained end to end</em></h1><p>A full course you can read on this page, from your first SQL query to designing petabyte-scale platforms: SQL and modeling, warehouses and lakehouses, Spark, Flink and Kafka, ingestion and orchestration, the Google Cloud data platform, distributed systems, operations, governance and data for AI. Every topic opens in plain words, then explains how it really works with examples and diagrams, keeps the internals and interview angles in an optional "Go deeper" panel, ends with questions to check yourself, and lists hand-picked resources to learn it in depth. Each section closes with a cheat sheet and practice exercises. Mark sections as read as you go; progress is saved in this browser.</p></section>';
  h += '<div class="stats"><div class="stat"><b>' + nles + '</b><span>sections · ' + Math.round(LESSON_WORDS / 1000) + 'k words</span></div><div class="stat"><b>' + TOTAL + '</b><span>topics, each fully explained</span></div><div class="stat"><b>' + nfig + '</b><span>diagrams</span></div><div class="stat"><b id="homePct">0%</b><span><span id="homeDone">0</span> sections read</span></div></div>';
  h += '<div class="cta">' + (last ? '<a class="btn pri" href="#' + last.id + '">Continue: ' + esc(last.t) + ' →</a><a class="btn" href="#foundations">Start from the beginning</a>' : '<a class="btn pri" href="#foundations">Start with the foundations →</a><a class="btn" href="#bigquery">Jump to BigQuery</a>') + '<a class="btn" href="pyspark-streaming-kafka-topic-map.html">PySpark + Kafka guide</a><a class="btn" href="python-end-to-end-topic-map.html">Python guide</a>' + '</div>';
  h += '<div class="howto"><div><b>Read in order</b>Each section builds on the previous ones. Read "In plain words" first, skip "Go deeper" on a first pass, and answer "Check yourself" before moving on.</div><div><b>Search everything</b>Press <code>/</code> and type an error, config or concept: search covers titles and explanations.</div><div><b>Move quickly</b>Use the sidebar, or <code>[</code> and <code>]</code> for the previous and next section. Click any diagram to enlarge it.</div></div>';
  h += '<div class="parts">';
  PARTS.forEach(function(p, pi){
    var secs = p.s.filter(lvOk); if (!secs.length) return;
    h += '<div class="pcard" data-k="' + p.k + '"><h3><small>' + ROMAN[pi] + '</small>' + esc(p.n) + '</h3><p>' + esc(p.i) + '</p><ul>' + secs.map(function(s){ return '<li><a href="#' + s.id + '"><span>' + esc(s.t) + '</span><span class="mini"><i data-mini="' + s.id + '"></i></span></a></li>'; }).join("") + '</ul></div>';
  });
  h += '</div>' + foot() + '</div>';
  main.innerHTML = h; document.title = "Data engineering, end to end: complete course"; markNav(); counts();
}
function foot(){ return '<footer class="foot">Explanations, examples and diagrams written for this course. Facts checked against official documentation, papers and project sources as of November 2026. Cloud services change quickly, so check the Google Cloud release notes for the services you use.</footer>'; }

function renderCourse(s, focusItem){
  var c = COURSE[s.id], h = '', used = {};
  var scan = function(t){ t.replace(/\[\[fig:([\w-]+)\]\]/g, function(_, k){ used[k] = 1; return ""; }); };
  scan(c.intro); c.groups.forEach(function(g){ scan(g.intro); g.topics.forEach(function(t){ scan(t.body); }); });
  var notUsed = function(keys){ return (keys || []).filter(function(k){ if (used[k]) return false; used[k] = 1; return true; }); };
  var words = c.intro.split(/\s+/).length, ntop = 0;
  c.groups.forEach(function(g){ words += g.intro.split(/\s+/).length; g.topics.forEach(function(t){ words += t.body.split(/\s+/).length; }); });
  s.g.forEach(function(g){ ntop += g[1].length; });
  var rails = [];
  h += '<section class="lesson course" id="' + s.id + '--course"><div class="lhead"><span class="ltag">Course</span><span class="lmeta">' + Math.max(1, Math.round(words / 200)) + ' min read · ' + s.g.length + ' parts · ' + ntop + ' topics</span></div><div class="lbody">';
  h += '<div class="overview" id="' + s.id + '--start">' + md(c.intro, s.id + '-i').html + '<div class="map"><div class="laytag">In this section</div><ol>' + s.g.map(function(g, gi){ return '<li><a href="#' + s.id + '--' + gi + '" data-jump="' + s.id + '--' + gi + '"><b>' + fmt(g[0]) + '</b><span>' + g[1].map(function(it){ return fmt(it[0]); }).join(" · ") + '</span></a></li>'; }).join("") + '</ol></div></div>';
  rails.push([s.id + '--start', "Start here"]);
  s.g.forEach(function(g, gi){
    var cg = c.groups[gi] || {intro: "", topics: []};
    h += '<h2 class="lh" id="' + s.id + '--' + gi + '"><span class="gn">' + (gi + 1) + '</span>' + fmt(g[0]) + '</h2>' + figs(notUsed(g[3])) + (cg.intro.trim() ? '<div class="bridge">' + md(cg.intro, s.id + '-b' + gi).html + '</div>' : '');
    rails.push([s.id + '--' + gi, g[0]]);
    g[1].forEach(function(it, ii){
      var t = cg.topics[ii];
      h += '<div class="art ctopic" id="t-' + s.id + '-' + gi + '-' + ii + '"><h3 class="lh3 st"><span class="tn">' + (gi + 1) + '.' + (ii + 1) + '</span><span>' + fmt(it[0]) + '</span></h3>' + (t ? md(t.body, s.id + '-d' + gi + '-' + ii).html : '<p>' + fmt(it[1] || "") + '</p>') + '</div>';
    });
    h += resBox(g[2], "Further reading");
  });
  c.groups.slice(s.g.length).forEach(function(g, k){
    var id = s.id + '--x' + k;
    h += '<h2 class="lh wrap" id="' + id + '">' + inl(g.title) + '</h2>' + md(g.intro, id).html;
    g.topics.forEach(function(t){ h += '<h3 class="lh3">' + inl(t.title) + '</h3>' + md(t.body, id).html; });
    rails.push([id, g.title]);
  });
  if (s.r && s.r.length){ h += '<h2 class="lh" id="' + s.id + '--refs">Books and references</h2>' + resBox(s.r, "For the whole section"); rails.push([s.id + '--refs', "Books and references"]); }
  var sf = figs(notUsed(s.f)); if (sf) h = h.replace('<div class="lbody">', '<div class="lbody">' + sf);
  return {html: h + '</div></section>', rails: rails};
}

function renderSection(s, focusItem){
  current = s; var p = PARTS[s.pi], idx = SECS.indexOf(s), prev = SECS[idx - 1], next = SECS[idx + 1];
  shell.classList.remove("norail");
  var h = '<div class="col"><article class="sec" data-k="' + s.k + '"><nav class="crumb" aria-label="Breadcrumb"><a href="#">Home</a><span>/</span><span>' + ROMAN[s.pi] + ". " + esc(p.n) + '</span></nav><h1>' + esc(s.t) + '</h1><div class="meta">' + s.lv.map(function(l){ return '<span class="lv lv-' + l + '">' + l + '</span>'; }).join("") + '</div><p class="lede">' + fmt(s.d) + '</p>';
  if (COURSE[s.id]){
    var cr = renderCourse(s);
    h += cr.html + '<div class="markrow"><button type="button" class="markbtn" data-mark="' + s.id + '" aria-pressed="false">Mark section as read</button></div>';
    h += '<nav class="pager" aria-label="Section navigation">' + (prev ? '<a href="#' + prev.id + '"><small>← Previous</small><b>' + esc(prev.t) + '</b></a>' : '') + (next ? '<a class="nx" href="#' + next.id + '"><small>Next →</small><b>' + esc(next.t) + '</b></a>' : '') + '</nav>' + foot() + '</article></div>';
    main.innerHTML = h;
    rail.innerHTML = '<div data-k="' + s.k + '"><h4>On this page</h4><ol>' + cr.rails.map(function(x){ return '<li><a href="#' + x[0] + '" data-jump="' + x[0] + '"><span>' + esc(plain(x[1]).replace(/\*\*/g, "")) + '</span></a></li>'; }).join("") + '</ol><h4>Keys</h4><div class="keys"><kbd>/</kbd> search<br><kbd>[</kbd> <kbd>]</kbd> previous / next section<br><kbd>Esc</kbd> clear search</div></div>';
    document.title = s.t + " · Data engineering, end to end";
    prefs.last = s.id; store("prefs", prefs);
    markNav(); counts(); observe();
    if (focusItem){ var fe = document.getElementById(focusItem); if (fe){ fe.scrollIntoView({block:"center"}); fe.classList.add("flash"); } }
    else window.scrollTo(0, 0);
    return;
  }
  var les = LESSON[s.id] ? md(LESSON[s.id], s.id) : null;
  var used = {}; (LESSON[s.id] || "").replace(/\[\[fig:([\w-]+)\]\]/g, function(_, k){ used[k] = 1; return ""; });
  (DEEP[s.id] || []).forEach(function(g){ g.topics.forEach(function(t){ t.body.replace(/\[\[fig:([\w-]+)\]\]/g, function(_, k){ used[k] = 1; return ""; }); }); });
  var notUsed = function(keys){ return (keys || []).filter(function(k){ if (used[k]) return false; used[k] = 1; return true; }); };
  var words = (LESSON[s.id] || "").split(/\s+/).length;
  s.g.forEach(function(g, gi){ g[1].forEach(function(it, ii){ words += (it[0] + " " + (deepText(s.id, gi, ii) || it[1] || "")).split(/\s+/).length; }); });
  var mins = Math.max(1, Math.round(words / 200));
  if (les){
    h += '<section class="lesson" id="' + s.id + '--lesson"><div class="lhead"><span class="ltag">Lesson</span><span class="lmeta">' + mins + ' min read in total · ' + les.heads.length + ' parts</span></div><div class="lbody">' + figs(notUsed(s.f)) + les.html + '</div></section>';
  }
  h += '<section class="lesson deep" id="' + s.id + '--deep"><div class="lhead"><span class="ltag">In depth</span><span class="lmeta">' + (les ? 'every detail of the subject, part by part' : mins + ' min read') + '</span></div><div class="lbody">' + (les ? '' : figs(notUsed(s.f)));
  s.g.forEach(function(g, gi){
    h += '<h2 class="lh" id="' + s.id + '--' + gi + '">' + fmt(g[0]) + '</h2>' + figs(notUsed(g[3]));
    g[1].forEach(function(it, ii){
      var body = deepText(s.id, gi, ii);
      h += '<div class="art" id="t-' + s.id + '-' + gi + '-' + ii + '"><h3 class="lh3 st">' + fmt(it[0]) + '</h3>' + (body ? md(body, s.id + '-d' + gi + '-' + ii).html : (it[1] ? '<p>' + fmt(it[1]) + '</p>' : '')) + '</div>';
    });
    h += resBox(g[2], "Further reading");
  });
  if (s.r && s.r.length) h += '<h2 class="lh" id="' + s.id + '--refs">Books and references</h2>' + resBox(s.r, "For the whole section");
  h += '</div></section>';
  h += '<div class="markrow"><button type="button" class="markbtn" data-mark="' + s.id + '" aria-pressed="false">Mark section as read</button></div>';
  h += '<nav class="pager" aria-label="Section navigation">' + (prev ? '<a href="#' + prev.id + '"><small>← Previous</small><b>' + esc(prev.t) + '</b></a>' : '') + (next ? '<a class="nx" href="#' + next.id + '"><small>Next →</small><b>' + esc(next.t) + '</b></a>' : '') + '</nav>' + foot() + '</article></div>';
  main.innerHTML = h;
  var lh = function(x){ return '<li><a href="#' + x[0] + '" data-jump="' + x[0] + '"><span>' + esc(plain(x[1]).replace(/\*\*/g, "")) + '</span></a></li>'; };
  rail.innerHTML = '<div data-k="' + s.k + '">' + (les ? '<h4>Lesson</h4><ol>' + les.heads.map(lh).join("") + '</ol>' : '') + '<h4>In depth</h4><ol>' + s.g.map(function(g, gi){ return lh([s.id + '--' + gi, g[0]]); }).join("") + '</ol><h4>Keys</h4><div class="keys"><kbd>/</kbd> search<br><kbd>[</kbd> <kbd>]</kbd> previous / next section<br><kbd>Esc</kbd> clear search</div></div>';
  document.title = s.t + " · Data engineering, end to end";
  prefs.last = s.id; store("prefs", prefs);
  markNav(); counts(); observe();
  if (focusItem){ var el = document.getElementById(focusItem); if (el){ el.scrollIntoView({block:"center"}); el.classList.add("flash"); } }
  else window.scrollTo(0, 0);
}

/* on-this-page highlighting */
var io;
function observe(){
  if (io) io.disconnect(); if (!("IntersectionObserver" in window)) return;
  io = new IntersectionObserver(function(es){ es.forEach(function(e){ if (e.isIntersecting){ $$(".rail a").forEach(function(a){ a.classList.toggle("on", a.dataset.jump === e.target.id); }); } }); }, {rootMargin:"-20% 0px -70% 0px"});
  $$("h2.lh[id], .overview[id]").forEach(function(g){ io.observe(g); });
}
rail.addEventListener("click", function(e){ var a = e.target.closest("a[data-jump]"); if (!a) return; e.preventDefault(); var t = document.getElementById(a.dataset.jump); if (t) t.scrollIntoView({behavior:"smooth", block:"start"}); });

/* search */
function hl(text, ql){
  var t = plain(text), i = t.toLowerCase().indexOf(ql); if (i < 0) return esc(t);
  return esc(t.slice(0, i)) + "<mark>" + esc(t.slice(i, i + ql.length)) + "</mark>" + esc(t.slice(i + ql.length));
}
function snippet(text, ql){
  var t = plain(text), i = t.toLowerCase().indexOf(ql); if (i < 0) return esc(t.length > 180 ? t.slice(0, 180) + "…" : t);
  var a = Math.max(0, i - 70), b = Math.min(t.length, i + ql.length + 110);
  return (a ? "…" : "") + hl(t.slice(a, b), ql) + (b < t.length ? "…" : "");
}
function renderSearch(raw){
  var ql = raw.trim().toLowerCase(); current = null; shell.classList.add("norail"); rail.innerHTML = "";
  var hits = [];
  SECS.forEach(function(s){
    if (!lvOk(s)) return;
    s.g.forEach(function(g, gi){ g[1].forEach(function(it, ii){
      var body = deepText(s.id, gi, ii) || it[1] || "";
      var hay = plain(it[0] + " " + body + " " + g[0] + " " + s.t).toLowerCase();
      if (hay.indexOf(ql) >= 0) hits.push({s:s, g:g, gi:gi, ii:ii, it:it, body:body.replace(/```[\s\S]*?```/g, " ").replace(/:::\s*\w*/g, " ").replace(/[#*>|]|\?\?|\[\[fig:[\w-]+\]\]/g, ""), score:(plain(it[0]).toLowerCase().indexOf(ql) >= 0 ? 0 : 1)});
    }); });
  });
  hits.sort(function(a, b){ return a.score - b.score; });
  var lhits = [];
  SECS.forEach(function(s){
    var c = COURSE[s.id]; if (!c) return;
    [[s.id + "--start", "Start here", c.intro]].concat(c.groups.slice(0, s.g.length).map(function(g, gi){ return [s.id + "--" + gi, g.title, g.intro]; })).forEach(function(x){
      var low = x[2].toLowerCase(), at = low.indexOf(ql); if (at < 0) return;
      lhits.push({s:s, href:x[0], head:x[1], kind:"Overview", snip:x[2].slice(Math.max(0, at - 90), at + ql.length + 120).replace(/\n+/g, " ").replace(/[#*>|`]|\?\?|:::\s*\w*|\[\[fig:[\w-]+\]\]/g, "")});
    });
  });
  SECS.forEach(function(s){
    var txt = LESSON[s.id]; if (!txt || !lvOk(s)) return;
    var low = txt.toLowerCase(), at = low.indexOf(ql); if (at < 0) return;
    var before = txt.slice(0, at), hn = (before.match(/^## /gm) || []).length;
    var head = hn ? (before.match(/^## (.*)$/gm) || []).pop().slice(3) : "Introduction";
    lhits.push({s:s, href:hn ? s.id + "--l" + (hn - 1) : s.id + "--lesson", head:head, kind:"Lesson", snip:txt.slice(Math.max(0, at - 90), at + ql.length + 120).replace(/\n+/g, " ").replace(/[#*>|`]|\?\?|\[\[fig:[\w-]+\]\]/g, "")});
  });
  var h = '<div class="col sres"><h1>Search: “' + esc(raw.trim()) + '”</h1><p>' + hits.length + ' topic match' + (hits.length === 1 ? "" : "es") + (lhits.length ? ' and ' + lhits.length + ' in section and group overviews' : '') + '. Press Esc to clear.</p>';
  if (!hits.length && !lhits.length) h += '<p class="empty">Nothing matches. Try a shorter word, a config name without its prefix.</p>';
  if (lhits.length){ h += '<h2 class="sh">In overviews</h2>'; lhits.slice(0, 40).forEach(function(x){ h += '<a class="hit" href="#' + x.href + '" data-k="' + x.s.k + '"><small>' + x.kind + ' · ' + esc(x.s.t) + '</small><b>' + esc(plain(x.head).replace(/\*\*/g, "")) + '</b><span>…' + hl(x.snip, ql) + '…</span></a>'; }); }
  if (hits.length && lhits.length) h += '<h2 class="sh">In topics</h2>';
  hits.slice(0, 250).forEach(function(x){
    var id = x.s.id + "." + x.gi + "." + x.ii;
    h += '<a class="hit" href="#' + x.s.id + ':' + x.gi + '.' + x.ii + '" data-k="' + x.s.k + '"><small>' + esc(x.s.t) + ' › ' + esc(plain(x.g[0])) + '' + '</small><b>' + hl(x.it[0], ql) + '</b>' + (x.body ? '<span>' + snippet(x.body, ql) + '</span>' : '') + '</a>';
  });
  if (hits.length > 250) h += '<p class="empty">Showing the first 250. Refine your search to see more.</p>';
  main.innerHTML = h + '</div>'; markNav();
}

/* routing */
function setPrac(on){ var l = document.getElementById("pracLink"); if (l){ if (on) l.setAttribute("aria-current", "page"); else l.removeAttribute("aria-current"); } }
function route(){
  var hash = decodeURIComponent(location.hash.replace(/^#/, ""));
  if (window.PRACTICE_VIEW && /^practice(\/|$)/.test(hash)){ current = null; markNav(); setPrac(true); document.body.classList.remove("navopen"); if (window.PRACTICE_VIEW) window.PRACTICE_VIEW(hash.split("/").slice(1)); return; }
  setPrac(false);
  if (!hash){ renderHome(); return; }
  var parts = hash.split(":"), s = BY[parts[0]];
  if (!s){ var grp = hash.split("--"); if (BY[grp[0]]){ renderSection(BY[grp[0]]); var g = document.getElementById(hash); if (g) g.scrollIntoView(); return; } renderHome(); return; }
  renderSection(s, parts[1] ? "t-" + s.id + "-" + parts[1].replace(".", "-") : null);
}
window.addEventListener("hashchange", function(){ if (q.value){ q.value = ""; } route(); });

/* events */
main.addEventListener("click", function(e){
  var c = e.target.closest(".fig .canvas"); if (c){ openFig(c.parentElement); return; }
  var mb = e.target.closest("[data-mark]"); if (mb){ var k = "sec:" + mb.dataset.mark; if (done[k]) delete done[k]; else done[k] = 1; store("done", done); counts(); }
});
main.addEventListener("keydown", function(e){ var c = e.target.closest && e.target.closest(".fig .canvas"); if (c && (e.key === "Enter" || e.key === " ")){ e.preventDefault(); openFig(c.parentElement); } });
var tmr;
q.addEventListener("input", function(){ clearTimeout(tmr); tmr = setTimeout(function(){ if (q.value.trim().length >= 2) renderSearch(q.value); else if (!q.value.trim()) route(); }, 140); });
q.addEventListener("keydown", function(e){ if (e.key === "Escape"){ q.value = ""; q.blur(); route(); } });
document.addEventListener("keydown", function(e){
  var typing = /input|textarea|select/i.test((document.activeElement || {}).tagName || "");
  if (e.key === "/" && !typing){ e.preventDefault(); q.focus(); q.select(); return; }
  if (typing || e.metaKey || e.ctrlKey || e.altKey) return;
  if ((e.key === "]" || e.key === "[") && current){ var i = SECS.indexOf(current) + (e.key === "]" ? 1 : -1); if (SECS[i]) location.hash = SECS[i].id; }
  if (e.key === "Escape") document.body.classList.remove("navopen");
});

$("#themeBtn").addEventListener("click", function(){
  var dark = prefs.theme ? prefs.theme === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  prefs.theme = dark ? "light" : "dark"; store("prefs", prefs); applyTheme();
});
function applyTheme(){ if (prefs.theme) document.documentElement.setAttribute("data-theme", prefs.theme); else document.documentElement.removeAttribute("data-theme"); }
$("#menuBtn").addEventListener("click", function(){ document.body.classList.toggle("navopen"); });
$("#scrim").addEventListener("click", function(){ document.body.classList.remove("navopen"); });

window.GUIDE = {md: md, inl: inl, esc: esc, fmt: fmt, LESSON: LESSON, SECS: SECS, PARTS: PARTS, BY: BY, ROMAN: ROMAN, main: main, rail: rail, shell: shell, foot: foot};
applyTheme(); renderNav(); route();
})();

