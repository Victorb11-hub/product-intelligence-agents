"""
Test the rebuilt TikTok agent on Korean Sheet Masks.
Manual .env parse, run agent.scrape, print sectioned report,
then write a signals_social row.
"""
import os
import sys
import uuid
import time
import logging
import traceback
from pathlib import Path

# Force UTF-8 stdout so box-drawing characters survive on Windows cp1252.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# 1. Manual env parse + sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

ENV_PATH = PROJECT_ROOT / "agents" / ".env"
if ENV_PATH.exists():
    with ENV_PATH.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ.setdefault(key, value)
else:
    print(f"WARNING: env file not found at {ENV_PATH}")

# -- 3. Logging --
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
for name in ("agents.agent_tiktok", "agents.base_platform_agent"):
    logging.getLogger(name).setLevel(logging.INFO)

# -- 2. Import agent --
from agents.agent_tiktok import TikTokAgent  # noqa: E402

# -- 5. Product --
product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty"],
    "backfill_completed": True,
    "total_runs": 5,
}

# -- 4. Create agent --
agent = TikTokAgent()
agent.run_id = str(uuid.uuid4())

print(f"\n{'='*60}")
print(f"TikTok Agent Test - {product['name']}")
print(f"Run ID: {agent.run_id}")
print(f"{'='*60}\n")

# -- 6. Scrape --
start = time.time()
try:
    result = agent.scrape(product["name"], product["keywords"], product)
except Exception as e:
    print(f"\nFATAL: scrape() raised {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

elapsed = time.time() - start

# ===================================================
# SECTION 1 - Pass 1 Stats
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 1 - Pass 1 Stats")
print(f"{'-'*50}")
print(f"  pass1_total:       {result.get('pass1_total', 0)}")
print(f"  pass1_passed:      {result.get('pass1_passed', 0)}")
print(f"  pass2_posts:       {result.get('pass2_posts', 0)}")
hashtags = result.get("hashtags_searched", [])
print(f"  hashtags_searched: {hashtags[:5]}")
if len(hashtags) > 5:
    print(f"    ... and {len(hashtags) - 5} more")

# ===================================================
# SECTION 2 - Pass 2 Stats
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 2 - Pass 2 Stats")
print(f"{'-'*50}")
print(f"  pass2_comments:      {result.get('pass2_comments', 0)}")
print(f"  purchase_signals:    {result.get('purchase_signals', 0)}")
print(f"  negative_signals:    {result.get('negative_signals', 0)}")
print(f"  question_signals:    {result.get('question_signals', 0)}")
print(f"  avg_weighted_intent: {result.get('avg_weighted_intent', 0)}")

# ===================================================
# SECTION 3 - Top 5 Posts
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 3 - Top 5 Posts")
print(f"{'-'*50}")
top_posts = (result.get("top_posts") or [])[:5]
if not top_posts:
    print("  (none)")
for i, p in enumerate(top_posts, 1):
    url = p.get("url") or ""
    url_tail = url[-30:] if len(url) > 30 else url
    caption = (p.get("caption_snippet") or "")[:50]
    print(f"\n  #{i}")
    print(f"    url (last 30):    ...{url_tail}")
    print(f"    views:            {p.get('views', 0):,}")
    print(f"    likes:            {p.get('likes', 0):,}")
    print(f"    comment_count:    {p.get('comment_count', 0):,}")
    print(f"    engagement_rate:  {p.get('engagement_rate', 0):.2f}%")
    print(f"    caption_snippet:  {caption}")
    print(f"    purchase_signals: {p.get('purchase_signals', 0)}")

# ===================================================
# SECTION 4 - Aggregates
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 4 - Aggregates")
print(f"{'-'*50}")
print(f"  total_views:        {result.get('total_views', 0):,}")
print(f"  total_likes:        {result.get('total_likes', 0):,}")
print(f"  total_shares:       {result.get('total_shares', 0):,}")
print(f"  creator_tier_score: {result.get('creator_tier_score', 0)}")

# ===================================================
# SECTION 5 - Duration + Errors
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 5 - Duration + Errors")
print(f"{'-'*50}")
print(f"  duration (result):  {result.get('duration_seconds', 0)}s")
print(f"  duration (actual):  {elapsed:.1f}s")
print(f"  error:              {result.get('error', None)}")

# ===================================================
# SECTION 6 - Signal Row Write to signals_social
# ===================================================
print(f"\n{'-'*50}")
print("SECTION 6 - Signal Row Write")
print(f"{'-'*50}")

try:
    signal_row = agent.build_signal_row(result, product["id"])
    print(f"  Built signal row with {len(signal_row)} fields")
    print(f"  Fields: {sorted(signal_row.keys())}")

    from agents.config import get_supabase  # noqa: E402
    sb = get_supabase()

    missing_cols = []
    attempt_row = dict(signal_row)
    for attempt in range(1, 8):
        try:
            resp = sb.table("signals_social").insert(attempt_row).execute()
            if resp.data:
                print(f"  Inserted on attempt {attempt} - id: {resp.data[0].get('id', '?')}")
            else:
                print(f"  Insert on attempt {attempt} returned no data")
            break
        except Exception as e:
            err = str(e)
            # Try to detect a missing-column error like:
            #   "Could not find the 'foo' column of 'signals_social' in the schema cache"
            #   or PostgREST PGRST204 / "column \"foo\" of relation ... does not exist"
            import re
            m = (re.search(r"'([A-Za-z0-9_]+)' column", err)
                 or re.search(r'column "([A-Za-z0-9_]+)"', err)
                 or re.search(r"column ([A-Za-z0-9_]+) of", err))
            if m:
                bad = m.group(1)
                if bad in attempt_row:
                    missing_cols.append(bad)
                    del attempt_row[bad]
                    print(f"  Attempt {attempt}: column '{bad}' does not exist - dropping and retrying")
                    continue
            print(f"  Insert failed (attempt {attempt}): {err[:300]}")
            break

    if missing_cols:
        print(f"\n  Columns missing from signals_social schema: {missing_cols}")
except Exception as e:
    print(f"  FAILED to build/write signal row: {e}")
    traceback.print_exc()

print(f"\n{'='*60}")
print("DONE")
print(f"{'='*60}\n")
