"""python3 linkcheck.py course/<sid>.md ... -> check every http(s) link; prints BAD and BLOCKED lists."""
import re, sys, subprocess, json
from concurrent.futures import ThreadPoolExecutor
urls = {}
for f in sys.argv[1:]:
    for u in re.findall(r"\]\((https?://[^)\s]+)\)", open(f).read()):
        urls.setdefault(u, f)
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
def check(u):
    for method in (["-I"], []):
        try:
            r = subprocess.run(["curl", "-s", "-L", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "20", "-A", UA] + method + [u], capture_output=True, text=True, timeout=30)
            code = int(r.stdout.strip() or 0)
        except Exception:
            code = 0
        if 200 <= code < 400: return u, code
    return u, code
with ThreadPoolExecutor(16) as ex:
    res = list(ex.map(check, urls))
bad = [(u, c) for u, c in res if c in (0, 404, 410) or (c >= 500)]
blocked = [(u, c) for u, c in res if c in (401, 403, 405, 406, 429, 999)]
other = [(u, c) for u, c in res if not (200 <= c < 400) and (u, c) not in bad and (u, c) not in blocked]
print(json.dumps({"checked": len(res), "ok": sum(1 for _, c in res if 200 <= c < 400), "bad": bad, "blocked": blocked, "other": other}, indent=1))
