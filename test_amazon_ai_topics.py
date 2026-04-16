"""
Test the updated Amazon agent with AI topic breakdown extraction on Korean Sheet Masks.

- BEFORE state: current_score, raw_score, verdict from products; repeat_purchase_mentions +
  repeat_purchase_signal from latest signals_retail (platform=amazon).
- Run AmazonAgent.scrape, print AI topic breakdown, repeat purchase detection, enrichments.
- Write signal row, run scoring engine, capture AFTER state.
- Print comparison table + delta.

Timeout intent: 300 seconds. Real Apify call (~$0.12).
"""
import os
import sys
import uuid
import logging
import io
import re
import traceback

# ─── Project root on sys.path ──────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ─── Manual UTF-8 parse of agents/.env ─────────────────────────────────────
ENV_PATH = os.path.join(PROJECT_ROOT, "agents", ".env")
if not os.path.exists(ENV_PATH):
    raise FileNotFoundError(f"agents/.env not found at {ENV_PATH}")

with open(ENV_PATH, "r", encoding="utf-8") as _f:
    for _line in _f:
        _s = _line.strip()
        if not _s or _s.startswith("#"):
            continue
        if "=" not in _s:
            continue
        _k, _v = _s.split("=", 1)
        _k = _k.strip()
        _v = _v.strip()
        # Strip surrounding quotes if present
        if len(_v) >= 2 and ((_v[0] == _v[-1] == '"') or (_v[0] == _v[-1] == "'")):
            _v = _v[1:-1]
        # Only set if not already in environment
        if _k and _k not in os.environ:
            os.environ[_k] = _v

# ─── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logging.getLogger("agents.agent_amazon").setLevel(logging.INFO)
logging.getLogger("agents.scoring_engine").setLevel(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Capture scoring_engine INFO logs for Job 3 extraction
scoring_log_stream = io.StringIO()
scoring_handler = logging.StreamHandler(scoring_log_stream)
scoring_handler.setLevel(logging.INFO)
scoring_handler.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger("agents.scoring_engine").addHandler(scoring_handler)

# ─── Imports that require env + sys.path ──────────────────────────────────
from agents.agent_amazon import AmazonAgent  # noqa: E402
from agents.scoring_engine import score_all_products  # noqa: E402


def banner(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


# ─── Product definition ───────────────────────────────────────────────────
PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"
product = {
    "id": PRODUCT_ID,
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty", "face mask", "skincare"],
}

banner("AMAZON AI-TOPIC EXTRACTION TEST — Korean Sheet Masks")
print(f"Product:     {product['name']}")
print(f"Product ID:  {product['id']}")
print(f"Keywords:    {product['keywords']}")

# ─── Create agent + Supabase handle ───────────────────────────────────────
agent = AmazonAgent()
agent.run_id = str(uuid.uuid4())
db = agent.supabase
print(f"Run ID:      {agent.run_id}")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1 — BEFORE STATE
# ═══════════════════════════════════════════════════════════════════════════
banner("STEP 1 — BEFORE STATE")

before_product = {}
try:
    resp = db.table("products") \
        .select("current_score, raw_score, current_verdict") \
        .eq("id", PRODUCT_ID) \
        .limit(1) \
        .execute()
    if resp.data:
        before_product = resp.data[0]
    print(f"  products.current_score:   {before_product.get('current_score')}")
    print(f"  products.raw_score:       {before_product.get('raw_score')}")
    print(f"  products.current_verdict: {before_product.get('current_verdict')}")
except Exception as e:
    print(f"  ERROR querying products: {e}")

before_signal = {}
try:
    resp = db.table("signals_retail") \
        .select("repeat_purchase_mentions, repeat_purchase_signal, scraped_date") \
        .eq("product_id", PRODUCT_ID) \
        .eq("platform", "amazon") \
        .order("scraped_date", desc=True) \
        .limit(1) \
        .execute()
    if resp.data:
        before_signal = resp.data[0]
    print(f"  signals_retail.repeat_purchase_mentions: {before_signal.get('repeat_purchase_mentions')}")
    print(f"  signals_retail.repeat_purchase_signal:   {before_signal.get('repeat_purchase_signal')}")
    print(f"  signals_retail.scraped_date:             {before_signal.get('scraped_date')}")
except Exception as e:
    print(f"  ERROR querying signals_retail: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2 — RUN SCRAPE
# ═══════════════════════════════════════════════════════════════════════════
banner("STEP 2 — RUN AMAZON SCRAPE (Pass 1 + Pass 2, ~$0.12)")

try:
    result = agent.scrape(product["name"], product["keywords"], product)
except Exception as e:
    print(f"\nSCRAPE FAILED: {e}")
    traceback.print_exc()
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════════
# SECTION A — ai_topic_breakdown
# ═══════════════════════════════════════════════════════════════════════════
banner("SECTION A — ai_topic_breakdown")

topics = result.get("ai_topic_breakdown", []) or []
print(f"  Number of topics extracted: {len(topics)}")
if topics:
    print(f"\n  Full topic list:")
    for i, t in enumerate(topics, 1):
        name = t.get("name", "")
        sentiment = t.get("sentiment", "")
        tot = t.get("total_mentions", 0)
        pos = t.get("positive_mentions", 0)
        neg = t.get("negative_mentions", 0)
        print(f"    {i:2d}. name={name!r:<40} sentiment={sentiment:<10} "
              f"total={tot:<4} +{pos:<4} -{neg:<4}")
else:
    print("  (No topics returned)")

# ═══════════════════════════════════════════════════════════════════════════
# SECTION B — Repeat purchase detection
# ═══════════════════════════════════════════════════════════════════════════
banner("SECTION B — Repeat Purchase Detection")

rp_signal = result.get("repeat_purchase_signal", False)
rp_mentions = result.get("repeat_purchase_mentions", 0) or 0
rp_topic = result.get("repeat_topic_name")
mpv = result.get("monthly_purchase_volume", 0) or 0

print(f"  repeat_purchase_signal: {rp_signal}")
if rp_topic:
    print(f"  Repeat topic FOUND via AI topic breakdown:")
    print(f"    repeat_topic_name: {rp_topic!r}")
    print(f"    mention count:     {rp_mentions}")
else:
    print(f"  No repeat topic in AI breakdown — used monthly volume proxy:")
    print(f"    monthly_purchase_volume: {mpv}")
    if mpv > 10000:
        tier = "Tier 5 (>10k/mo → 300)"
    elif mpv > 5000:
        tier = "Tier 4 (>5k/mo → 150)"
    elif mpv > 1000:
        tier = "Tier 3 (>1k/mo → 50)"
    else:
        tier = "No tier triggered (→ 0)"
    print(f"    proxy tier used:          {tier}")
print(f"  FINAL repeat_purchase_mentions: {rp_mentions}")

# ═══════════════════════════════════════════════════════════════════════════
# SECTION C — Product enrichments
# ═══════════════════════════════════════════════════════════════════════════
banner("SECTION C — Product Enrichments")

print(f"  brand:          {result.get('brand')!r}")
print(f"  variant_count:  {result.get('variant_count')}")
print(f"  category_path:  {result.get('category_path')!r}")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6 — Write signal row to signals_retail
# ═══════════════════════════════════════════════════════════════════════════
banner("STEP 6 — Write signal row to signals_retail")

signal_row = agent.build_signal_row(result, PRODUCT_ID)
print(f"  Signal row has {len(signal_row)} fields")

columns_to_try = dict(signal_row)
max_attempts = 8
dropped_cols = []
inserted = False
for attempt in range(1, max_attempts + 1):
    try:
        resp = db.table("signals_retail").insert(columns_to_try).execute()
        print(f"  Insert SUCCESS on attempt {attempt}")
        if resp.data:
            print(f"  Row ID: {resp.data[0].get('id', 'N/A')}")
        inserted = True
        break
    except Exception as e:
        err = str(e)
        print(f"  Attempt {attempt} failed: {err[:200]}")
        col_match = re.search(r'column\s+"?(\w+)"?\s+.*(?:does not exist|not found|schema cache)',
                              err, re.IGNORECASE)
        if col_match:
            bad = col_match.group(1)
            if bad in columns_to_try and bad not in ("product_id", "scraped_date", "platform"):
                columns_to_try.pop(bad)
                dropped_cols.append(bad)
                print(f"    → dropping column {bad!r} and retrying")
                continue
        print("    → cannot auto-fix — aborting insert")
        break

if dropped_cols:
    print(f"  Dropped columns (not in schema): {dropped_cols}")
if not inserted:
    print("  WARNING: signal row not inserted — scoring may use stale data")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 7 — Re-run scoring engine
# ═══════════════════════════════════════════════════════════════════════════
banner("STEP 7 — Re-run scoring_engine.score_all_products")

scoring_log_stream.seek(0)
scoring_log_stream.truncate()

score_all_products(db, [product], str(uuid.uuid4()))

scoring_logs = scoring_log_stream.getvalue()
print("  ── Captured scoring_engine INFO logs ──")
for line in scoring_logs.splitlines():
    print(f"    {line}")

# Parse Job 3 breakdown
job3_line = None
job3_score = None
repeat_norm_val = None
for line in scoring_logs.splitlines():
    if "Job 3" in line:
        job3_line = line
        # "[scoring] Job 3: vol=X.X(N/mo) repeat=R.R(M mentions) satisfaction=S.S bsr=B.B(trend) vel=V.V → T.T"
        m_score = re.search(r"→\s*([\d.]+)", line)
        if m_score:
            job3_score = float(m_score.group(1))
        m_rep = re.search(r"repeat=([\d.]+)", line)
        if m_rep:
            repeat_norm_val = float(m_rep.group(1))

# ═══════════════════════════════════════════════════════════════════════════
# STEP 8 — AFTER STATE
# ═══════════════════════════════════════════════════════════════════════════
banner("STEP 8 — AFTER STATE")

after_product = {}
try:
    resp = db.table("products") \
        .select("current_score, raw_score, current_verdict") \
        .eq("id", PRODUCT_ID) \
        .limit(1) \
        .execute()
    if resp.data:
        after_product = resp.data[0]
    print(f"  products.current_score:   {after_product.get('current_score')}")
    print(f"  products.raw_score:       {after_product.get('raw_score')}")
    print(f"  products.current_verdict: {after_product.get('current_verdict')}")
except Exception as e:
    print(f"  ERROR querying products: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 9 — Comparison table
# ═══════════════════════════════════════════════════════════════════════════
banner("STEP 9 — Comparison Table")


def _fmt(v):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


rows = [
    ("current_score", before_product.get("current_score"), after_product.get("current_score")),
    ("raw_score",     before_product.get("raw_score"),     after_product.get("raw_score")),
    ("repeat_norm",   None,                                 repeat_norm_val),
    ("Job 3 score",   None,                                 job3_score),
]

col1, col2, col3 = "Field", "Before", "After"
w1 = max(len(col1), max(len(r[0]) for r in rows))
w2 = max(len(col2), max(len(_fmt(r[1])) for r in rows))
w3 = max(len(col3), max(len(_fmt(r[2])) for r in rows))
sep = f"+-{'-'*w1}-+-{'-'*w2}-+-{'-'*w3}-+"
print(sep)
print(f"| {col1:<{w1}} | {col2:<{w2}} | {col3:<{w3}} |")
print(sep)
for name, b, a in rows:
    print(f"| {name:<{w1}} | {_fmt(b):<{w2}} | {_fmt(a):<{w3}} |")
print(sep)

# ═══════════════════════════════════════════════════════════════════════════
# STEP 10 — Overall delta + verdict
# ═══════════════════════════════════════════════════════════════════════════
banner("STEP 10 — Overall Delta + Verdict")


def _num(x):
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


bc, ac = _num(before_product.get("current_score")), _num(after_product.get("current_score"))
br, ar = _num(before_product.get("raw_score")),     _num(after_product.get("raw_score"))

if bc is not None and ac is not None:
    delta_current = ac - bc
    print(f"  current_score delta: {bc:.2f} → {ac:.2f}   ({delta_current:+.2f})")
else:
    print(f"  current_score delta: {_fmt(bc)} → {_fmt(ac)}")

if br is not None and ar is not None:
    delta_raw = ar - br
    print(f"  raw_score delta:     {br:.2f} → {ar:.2f}   ({delta_raw:+.2f})")
else:
    print(f"  raw_score delta:     {_fmt(br)} → {_fmt(ar)}")

print(f"  Verdict: {before_product.get('current_verdict')!r} → {after_product.get('current_verdict')!r}")
if job3_score is not None:
    print(f"  Job 3 purchase-intent score: {job3_score:.2f}")
if repeat_norm_val is not None:
    print(f"  Repeat-purchase norm contribution: {repeat_norm_val:.2f}")

banner("TEST COMPLETE")
