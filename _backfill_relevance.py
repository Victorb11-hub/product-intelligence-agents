"""Backfill relevance scores for Korean Sheet Masks — no Apify calls.
Uses concurrent updates for speed."""
import sys, os, logging, concurrent.futures, time

# Project root on sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Manual .env parse (utf-8)
env_path = os.path.join(PROJECT_ROOT, "agents", ".env")
with open(env_path, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
# Suppress HTTP request noise
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

from agents.skills.relevance_scorer import load_product_keywords, score_post_relevance
from agents.config import get_supabase, SupabaseClient, SUPABASE_URL, SUPABASE_KEY

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"

db = get_supabase()

# ── 1. Load keywords ──
keywords = load_product_keywords(db, PRODUCT_ID)
print(f"Loaded {len(keywords)} keywords")

# ── 2. Load all posts ──
all_posts = []
offset = 0
batch_size = 500
while True:
    resp = db.table("posts") \
        .select("id, post_title, post_body") \
        .eq("product_id", PRODUCT_ID) \
        .range(offset, offset + batch_size - 1) \
        .execute()
    rows = resp.data or []
    all_posts.extend(rows)
    if len(rows) < batch_size:
        break
    offset += batch_size
print(f"Loaded {len(all_posts)} posts")

# ── 3. Score all posts locally (instant, no DB) ──
scored = []
for post in all_posts:
    score = score_post_relevance(post, keywords)
    scored.append((post["id"], score))

high = sum(1 for _, s in scored if s >= 0.6)
medium = sum(1 for _, s in scored if 0.3 <= s < 0.6)
low = sum(1 for _, s in scored if s < 0.3)
print(f"Scored: {len(scored)} posts — {high} high, {medium} medium, {low} low")

# ── 4. Batch update using concurrent workers ──
def update_post(pair):
    post_id, score = pair
    client = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    try:
        client.table("posts").update({"relevance_score": score}).eq("id", post_id).execute()
        return True
    except Exception as e:
        return False

print(f"\nUpdating {len(scored)} posts with 20 concurrent workers...")
t0 = time.time()
success = 0
fail = 0
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(update_post, pair): pair for pair in scored}
    done_count = 0
    for future in concurrent.futures.as_completed(futures):
        done_count += 1
        if future.result():
            success += 1
        else:
            fail += 1
        if done_count % 500 == 0:
            elapsed = time.time() - t0
            print(f"  {done_count}/{len(scored)} done ({elapsed:.1f}s)")

elapsed = time.time() - t0
print(f"\nDone in {elapsed:.1f}s — {success} updated, {fail} failed")

# ── 5. Print stats ──
print("\n" + "=" * 60)
print("BACKFILL STATS")
print("=" * 60)
print(f"  total   = {len(scored)}")
print(f"  updated = {success}")
print(f"  high    = {high}")
print(f"  medium  = {medium}")
print(f"  low     = {low}")

# ── 6. Verify: 5 high-relevance posts ──
print("\n" + "=" * 60)
print("TOP 5 HIGH-RELEVANCE POSTS (score > 0.3)")
print("=" * 60)
high_posts = db.table("posts") \
    .select("post_title, post_body, relevance_score, platform") \
    .eq("product_id", PRODUCT_ID) \
    .gt("relevance_score", 0.3) \
    .order("relevance_score", desc=True) \
    .limit(5) \
    .execute()
for p in (high_posts.data or []):
    text = (p.get("post_title") or p.get("post_body") or "")[:80]
    print(f"  [{p['platform']:>10}] score={p['relevance_score']:.4f}  {text}")

# ── 7. Verify: 5 low-relevance posts ──
print("\n" + "=" * 60)
print("BOTTOM 5 LOW-RELEVANCE POSTS (score < 0.1)")
print("=" * 60)
low_posts = db.table("posts") \
    .select("post_title, post_body, relevance_score, platform") \
    .eq("product_id", PRODUCT_ID) \
    .lt("relevance_score", 0.1) \
    .order("relevance_score", desc=False) \
    .limit(5) \
    .execute()
for p in (low_posts.data or []):
    text = (p.get("post_title") or p.get("post_body") or "")[:80]
    print(f"  [{p['platform']:>10}] score={p['relevance_score']:.4f}  {text}")

# ── 8. Distribution ──
print("\n" + "=" * 60)
print("DISTRIBUTION")
print("=" * 60)

def count_range(col, op, val, op2=None, val2=None):
    q = db.table("posts").select("id", count="exact") \
        .eq("product_id", PRODUCT_ID)
    if op == "eq":
        q = q.eq(col, val)
    elif op == "gt":
        q = q.gt(col, val)
    elif op == "gte":
        q = q.gte(col, val)
    elif op == "lt":
        q = q.lt(col, val)
    if op2 == "lt":
        q = q.lt(col, val2)
    elif op2 == "gte":
        q = q.gte(col, val2)
    resp = q.limit(1).execute()
    return resp.count if resp.count is not None else len(resp.data or [])

zero   = count_range("relevance_score", "eq", 0)
low_c  = count_range("relevance_score", "gt", 0, "lt", 0.3)
med_c  = count_range("relevance_score", "gte", 0.3, "lt", 0.6)
high_c = count_range("relevance_score", "gte", 0.6)

print(f"  score = 0          : {zero}")
print(f"  0 < score < 0.3    : {low_c}")
print(f"  0.3 <= score < 0.6 : {med_c}")
print(f"  score >= 0.6       : {high_c}")
print(f"  TOTAL              : {zero + low_c + med_c + high_c}")
