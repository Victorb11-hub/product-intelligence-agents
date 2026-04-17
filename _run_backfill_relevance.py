"""Backfill relevance scores for Korean Sheet Masks — NO Apify calls."""
import sys, os, logging

# ── 1. Load env manually from agents/.env ──
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(PROJECT_ROOT, "agents", ".env")
with open(env_path, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip())

sys.path.insert(0, PROJECT_ROOT)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# ── 2-6. Run backfill ──
from agents.skills.relevance_scorer import backfill_relevance
from agents.config import get_supabase

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"

db = get_supabase()
result = backfill_relevance(db, PRODUCT_ID)
print("\n=== Backfill Result ===")
for k, v in result.items():
    print(f"  {k}: {v}")

# ── 7. Top 5 highest relevance ──
print("\n=== Top 5 Highest Relevance ===")
resp = db.table("posts") \
    .select("post_title, post_body, relevance_score, platform") \
    .eq("product_id", PRODUCT_ID) \
    .order("relevance_score", desc=True) \
    .limit(5) \
    .execute()
for row in (resp.data or []):
    text = (row.get("post_title") or row.get("post_body") or "")[:80]
    print(f"  [{row['platform']}] score={row['relevance_score']:.4f}  {text}")

# ── 8. Bottom 5 (score > 0) ──
print("\n=== Bottom 5 Lowest Relevance (>0) ===")
resp = db.table("posts") \
    .select("post_title, post_body, relevance_score, platform") \
    .eq("product_id", PRODUCT_ID) \
    .gt("relevance_score", 0) \
    .order("relevance_score", desc=False) \
    .limit(5) \
    .execute()
for row in (resp.data or []):
    text = (row.get("post_title") or row.get("post_body") or "")[:80]
    print(f"  [{row['platform']}] score={row['relevance_score']:.4f}  {text}")

# ── 9. Distribution (paginated counts) ──
print("\n=== Score Distribution ===")

def paginated_count(query_fn):
    """Count rows by paginating with offset/limit since PostgREST caps at 1000."""
    total = 0
    offset = 0
    PAGE = 1000
    while True:
        resp = query_fn().select("id").range(offset, offset + PAGE - 1).execute()
        batch = len(resp.data or [])
        total += batch
        if batch < PAGE:
            break
        offset += PAGE
    return total

count_zero = paginated_count(
    lambda: db.table("posts").eq("product_id", PRODUCT_ID).eq("relevance_score", 0)
)
count_low = paginated_count(
    lambda: db.table("posts").eq("product_id", PRODUCT_ID).gt("relevance_score", 0).lt("relevance_score", 0.3)
)
count_mid = paginated_count(
    lambda: db.table("posts").eq("product_id", PRODUCT_ID).gte("relevance_score", 0.3).lt("relevance_score", 0.6)
)
count_high = paginated_count(
    lambda: db.table("posts").eq("product_id", PRODUCT_ID).gte("relevance_score", 0.6)
)

print(f"  score = 0       : {count_zero}")
print(f"  0 < score < 0.3 : {count_low}")
print(f"  0.3 <= score < 0.6 : {count_mid}")
print(f"  score >= 0.6    : {count_high}")
print(f"  TOTAL           : {count_zero + count_low + count_mid + count_high}")
