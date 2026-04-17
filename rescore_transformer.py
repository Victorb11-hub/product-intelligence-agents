"""
Re-score ALL comments with transformer sentiment model.
Parts: 1) Check relevance backfill, 2) Re-score comments, 3) Show results.
"""
import sys, os, time

# Project root
ROOT = r"c:\Users\vibraca\OneDrive - Evolution Equities LLC\Personal\Business\(1) Claude\Claude Code\Social Media Scraper"
sys.path.insert(0, ROOT)

# Parse agents/.env manually
env_path = os.path.join(ROOT, "agents", ".env")
with open(env_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

from agents.config import get_supabase
from agents.skills.relevance_scorer import backfill_relevance, load_product_keywords, score_relevance

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"
db = get_supabase()

# ═══════════════════════════════════════════
# PART 1 — Check relevance backfill status
# ═══════════════════════════════════════════
print("=" * 60)
print("PART 1 — RELEVANCE BACKFILL STATUS")
print("=" * 60)

# Count posts with relevance_score > 0
resp_scored = db.table("posts").select("id", count="exact").eq("product_id", PRODUCT_ID).gt("relevance_score", 0).execute()
scored_count = resp_scored.count if hasattr(resp_scored, 'count') and resp_scored.count is not None else len(resp_scored.data or [])

# Count posts with relevance_score = 0 or null
resp_all = db.table("posts").select("id", count="exact").eq("product_id", PRODUCT_ID).execute()
total_posts = resp_all.count if hasattr(resp_all, 'count') and resp_all.count is not None else len(resp_all.data or [])

unscored = total_posts - scored_count

print(f"Total posts: {total_posts}")
print(f"Scored (relevance > 0): {scored_count}")
print(f"Unscored (0 or NULL): {unscored}")

if scored_count == 0:
    print("\nRelevance NOT backfilled yet. Running backfill now...")
    result = backfill_relevance(db, PRODUCT_ID)
    print(f"Backfill result: {result}")
else:
    # Show distribution - fetch all relevance scores
    all_scores = []
    offset = 0
    while True:
        resp = db.table("posts").select("relevance_score").eq("product_id", PRODUCT_ID).range(offset, offset + 999).execute()
        rows = resp.data or []
        all_scores.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000

    high = sum(1 for r in all_scores if (r.get("relevance_score") or 0) >= 0.6)
    medium = sum(1 for r in all_scores if 0.3 <= (r.get("relevance_score") or 0) < 0.6)
    low = sum(1 for r in all_scores if 0 < (r.get("relevance_score") or 0) < 0.3)
    zero = sum(1 for r in all_scores if (r.get("relevance_score") or 0) == 0)

    print(f"\nRelevance distribution:")
    print(f"  High (>=0.6):        {high}")
    print(f"  Medium (0.3-0.6):    {medium}")
    print(f"  Low (>0 and <0.3):   {low}")
    print(f"  Zero (=0):           {zero}")

# ═══════════════════════════════════════════
# PART 2 — Re-score comments with transformer
# ═══════════════════════════════════════════
print("\n" + "=" * 60)
print("PART 2 — RE-SCORING COMMENTS WITH TRANSFORMER")
print("=" * 60)

# Load model first to time it separately
print("Loading transformer model...")
t0 = time.time()
from agents.skills.sentiment import analyze_batch
# Force model load
analyze_batch(["test"], batch_size=1)
print(f"Model loaded in {time.time() - t0:.1f}s")

# Pull ALL comments with pagination
print("\nFetching all comments...")
all_comments = []
offset = 0
while True:
    resp = db.table("comments").select("id, comment_body, sentiment_score, platform").eq("product_id", PRODUCT_ID).range(offset, offset + 999).execute()
    rows = resp.data or []
    all_comments.extend(rows)
    if len(rows) < 1000:
        break
    offset += 1000

total_comments = len(all_comments)
print(f"Fetched {total_comments} comments")

# Track before state
zeros_before = sum(1 for c in all_comments if (c.get("sentiment_score") or 0) == 0.0)
old_scores = {c["id"]: c.get("sentiment_score", 0) for c in all_comments}

# Process in batches of 32
print(f"\nProcessing {total_comments} comments in batches of 32...")
t_start = time.time()
new_scores = {}
batch_size = 32
processed = 0

for i in range(0, total_comments, batch_size):
    batch = all_comments[i:i + batch_size]
    texts = [c.get("comment_body") or "" for c in batch]

    results = analyze_batch(texts, batch_size=batch_size)

    # Update DB for each comment
    for j, (comment, result) in enumerate(zip(batch, results)):
        cid = comment["id"]
        new_score = result["sentiment_score"]
        new_scores[cid] = new_score

        try:
            db.table("comments").update({
                "sentiment_score": new_score,
            }).eq("id", cid).execute()
        except Exception as e:
            print(f"  ERROR updating {cid}: {str(e)[:80]}")

    processed += len(batch)
    if processed % 500 < batch_size:
        elapsed = time.time() - t_start
        rate = processed / elapsed if elapsed > 0 else 0
        print(f"  {processed}/{total_comments} done ({elapsed:.1f}s, {rate:.0f} comments/s)")

elapsed_total = time.time() - t_start
print(f"\nDone! {processed} comments re-scored in {elapsed_total:.1f}s ({processed/elapsed_total:.0f} comments/s)")

# ═══════════════════════════════════════════
# PART 3 — Show results
# ═══════════════════════════════════════════
print("\n" + "=" * 60)
print("PART 3 — RESULTS")
print("=" * 60)

zeros_after = sum(1 for s in new_scores.values() if s == 0.0)

print(f"\nSENTIMENT BEFORE vs AFTER:")
print(f"  Count at 0.00 before: {zeros_before}")
print(f"  Count at 0.00 after:  {zeros_after}")

# Distribution
positive = [(cid, s) for cid, s in new_scores.items() if s > 0.2]
neutral = [(cid, s) for cid, s in new_scores.items() if -0.2 <= s <= 0.2]
negative = [(cid, s) for cid, s in new_scores.items() if s < -0.2]

n = len(new_scores)
print(f"\nDISTRIBUTION:")
print(f"  Positive (>0.2):       {len(positive)} ({len(positive)/n*100:.1f}%)")
print(f"  Neutral (-0.2 to 0.2): {len(neutral)} ({len(neutral)/n*100:.1f}%)")
print(f"  Negative (<-0.2):      {len(negative)} ({len(negative)/n*100:.1f}%)")

# AVG sentiment per platform
platform_scores = {}
for c in all_comments:
    plat = c.get("platform", "unknown")
    if plat not in platform_scores:
        platform_scores[plat] = []
    platform_scores[plat].append(new_scores.get(c["id"], 0))

print(f"\nAVG SENTIMENT PER PLATFORM:")
for plat in sorted(platform_scores.keys()):
    scores = platform_scores[plat]
    avg = sum(scores) / len(scores) if scores else 0
    print(f"  {plat}: {avg:.3f} ({len(scores)} comments)")

# TOP 5 MOST POSITIVE
print(f"\nTOP 5 MOST POSITIVE:")
comment_lookup = {c["id"]: c for c in all_comments}
sorted_pos = sorted(new_scores.items(), key=lambda x: x[1], reverse=True)[:5]
for cid, score in sorted_pos:
    c = comment_lookup[cid]
    body = (c.get("comment_body") or "")[:100]
    print(f"  [{score:+.4f}] ({c.get('platform','?')}) {body}")

# TOP 5 MOST NEGATIVE
print(f"\nTOP 5 MOST NEGATIVE:")
sorted_neg = sorted(new_scores.items(), key=lambda x: x[1])[:5]
for cid, score in sorted_neg:
    c = comment_lookup[cid]
    body = (c.get("comment_body") or "")[:100]
    print(f"  [{score:+.4f}] ({c.get('platform','?')}) {body}")

# RELEVANCE TOP 5 (from posts)
print(f"\nRELEVANCE TOP 5 (from posts):")
keywords = load_product_keywords(db, PRODUCT_ID)
resp_top = db.table("posts").select("post_title, post_body, platform, relevance_score").eq("product_id", PRODUCT_ID).order("relevance_score", desc=True).limit(5).execute()
for p in (resp_top.data or []):
    title = (p.get("post_title") or p.get("post_body") or "")[:80]
    plat = p.get("platform", "?")
    rel = p.get("relevance_score", 0)
    # Find matched keywords
    combined = f"{p.get('post_title', '')} {p.get('post_body', '')}"
    match_result = score_relevance(combined, keywords)
    matched_kws = [m["keyword"] for m in match_result["matched_keywords"]]
    print(f"  [{rel:.4f}] ({plat}) {title}")
    print(f"           matched: {', '.join(matched_kws[:6])}")

print("\n" + "=" * 60)
print("COMPLETE")
print("=" * 60)
