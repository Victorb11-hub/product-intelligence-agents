"""
Test script: Instagram agent on Korean Sheet Masks.
Runs Pass 1 + Pass 2, prints stats, writes signal row.
"""
import os
import sys
import uuid
import time
import logging
import traceback
from pathlib import Path

# Force stdout/stderr to UTF-8 on Windows to avoid cp1252 unicode errors
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Setup: load env manually, add project root to sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parent)
sys.path.insert(0, PROJECT_ROOT)

ENV_PATH = os.path.join(PROJECT_ROOT, "agents", ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip()
            # Strip surrounding quotes (single or double) if present
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            os.environ[key] = val
else:
    print(f"WARNING: env file not found at {ENV_PATH}")

# -- Logging --
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(name)s] %(levelname)s -- %(message)s",
    datefmt="%H:%M:%S",
)
for name in ("agents.agent_instagram", "agents.base_platform_agent"):
    logging.getLogger(name).setLevel(logging.INFO)

# -- Import agent (after env loaded) --
from agents.agent_instagram import InstagramAgent

# -- Product --
product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty"],
    "backfill_completed": True,
    "total_runs": 5,
}

# -- Create agent --
agent = InstagramAgent()
agent.run_id = str(uuid.uuid4())

print(f"\n{'='*60}")
print(f"Instagram Agent Test -- {product['name']}")
print(f"Run ID: {agent.run_id}")
print(f"{'='*60}\n")

# -- Run scrape --
start = time.time()
try:
    result = agent.scrape(product["name"], product["keywords"], product)
except Exception as e:
    print(f"\nFATAL: scrape() raised {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

elapsed = time.time() - start

# ===================================================
# Section 1 -- Pass 1 Stats
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 1 -- Pass 1 Stats")
print(f"{'-'*50}")
print(f"  pass1_total:      {result.get('pass1_total', 0)}")
print(f"  pass1_passed:     {result.get('pass1_passed', 0)}")
print(f"  pass1_reels:      {result.get('pass1_reels', 0)}")
print(f"  pass1_photos:     {result.get('pass1_photos', 0)}")
print(f"  reel_percentage:  {result.get('reel_percentage', 0)}%")
print(f"  pass2_posts:      {result.get('pass2_posts', 0)}")
hashtags = result.get("hashtags_searched", [])
print(f"  hashtags_searched (first 5): {hashtags[:5]}")
if len(hashtags) > 5:
    print(f"    ... and {len(hashtags)-5} more")

# ===================================================
# Section 2 -- Pass 2 Stats
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 2 -- Pass 2 Stats")
print(f"{'-'*50}")
print(f"  pass2_comments:       {result.get('pass2_comments', 0)}")
print(f"  purchase_signals:     {result.get('purchase_signals', 0)}")
print(f"  negative_signals:     {result.get('negative_signals', 0)}")
print(f"  question_signals:     {result.get('question_signals', 0)}")
print(f"  avg_weighted_intent:  {result.get('avg_weighted_intent', 0)}")

# ===================================================
# Section 3 -- Top 5 Posts
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 3 -- Top 5 Posts")
print(f"{'-'*50}")
top_posts = result.get("top_posts", [])[:5]
if not top_posts:
    print("  (none)")
for i, p in enumerate(top_posts, 1):
    url = p.get("url", "") or ""
    url_tail = url[-30:] if len(url) > 30 else url
    caption = (p.get("caption_snippet") or "")[:50]
    print(f"\n  #{i}")
    print(f"    url (last 30):    ...{url_tail}")
    print(f"    type:             {p.get('type', '?')}")
    print(f"    likes:            {p.get('likes', 0):,}")
    print(f"    comment_count:    {p.get('comment_count', 0):,}")
    print(f"    caption_snippet:  {caption}")
    print(f"    purchase_signals: {p.get('purchase_signals', 0)}")

# ===================================================
# Section 4 -- Aggregates
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 4 -- Aggregates")
print(f"{'-'*50}")
print(f"  total_likes:        {result.get('total_likes', 0):,}")
print(f"  total_comments:     {result.get('total_comments', 0):,}")
print(f"  creator_tier_score: {result.get('creator_tier_score', 0)}")

# ===================================================
# Section 5 -- Duration + Errors
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 5 -- Duration + Errors")
print(f"{'-'*50}")
print(f"  duration (result):  {result.get('duration_seconds', 0)}s")
print(f"  duration (actual):  {elapsed:.1f}s")
print(f"  error:              {result.get('error', None)}")

# ===================================================
# Section 6 -- Signal Row Write
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 6 -- Signal Row Write")
print(f"{'-'*50}")

try:
    signal_row = agent.build_signal_row(result, product["id"])
    print(f"  Signal row built with {len(signal_row)} fields")

    # Insert to signals_social
    from agents.config import get_supabase
    sb = get_supabase()
    try:
        resp = sb.table("signals_social").insert(signal_row).execute()
        if resp.data:
            print(f"  Inserted signal row ID: {resp.data[0].get('id', '?')}")
        else:
            print(f"  Insert returned no data")
    except Exception as e:
        err_str = str(e)
        print(f"  Insert error: {err_str[:400]}")
        if "column" in err_str.lower() or "null" in err_str.lower() or "schema" in err_str.lower():
            # Try removing problematic / null columns and retry
            cleaned = {k: v for k, v in signal_row.items() if v is not None}
            try:
                resp = sb.table("signals_social").insert(cleaned).execute()
                if resp.data:
                    print(f"  Retry OK -- inserted ID: {resp.data[0].get('id', '?')}")
                else:
                    print(f"  Retry returned no data")
            except Exception as e2:
                print(f"  Retry also failed: {str(e2)[:300]}")
except Exception as e:
    print(f"  FAILED to build/write signal row: {e}")
    traceback.print_exc()

print(f"\n{'='*60}")
print("DONE")
print(f"{'='*60}\n")
