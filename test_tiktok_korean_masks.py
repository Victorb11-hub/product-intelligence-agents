"""
Test script: TikTok agent on Korean Sheet Masks.
Runs Pass 1 + Pass 2, prints stats, writes signal row.
"""
import os
import sys
import uuid
import time
import logging
import traceback
from pathlib import Path
from dotenv import load_dotenv

# ── Setup ──
PROJECT_ROOT = str(Path(__file__).resolve().parent)
sys.path.insert(0, PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, "agents", ".env"), override=True)

# ── Logging ──
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
for name in ("agents.agent_tiktok", "agents.base_platform_agent"):
    logging.getLogger(name).setLevel(logging.INFO)

# ── Import agent ──
from agents.agent_tiktok import TikTokAgent

# ── Product ──
product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty"],
    "backfill_completed": True,
    "total_runs": 5,
}

# ── Create agent ──
agent = TikTokAgent()
agent.run_id = str(uuid.uuid4())

print(f"\n{'='*60}")
print(f"TikTok Agent Test — {product['name']}")
print(f"Run ID: {agent.run_id}")
print(f"{'='*60}\n")

# ── Run scrape (600s timeout) ──
start = time.time()
try:
    result = agent.scrape(product["name"], product["keywords"], product)
except Exception as e:
    print(f"\nFATAL: scrape() raised {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

elapsed = time.time() - start

# ═══════════════════════════════════════════════════
# Section 1 — Pass 1 Stats
# ═══════════════════════════════════════════════════
print(f"\n{'─'*50}")
print("SECTION 1 — Pass 1 Stats")
print(f"{'─'*50}")
print(f"  pass1_total:      {result.get('pass1_total', 0)}")
print(f"  pass1_passed:     {result.get('pass1_passed', 0)}")
print(f"  pass2_posts:      {result.get('pass2_posts', 0)}")
hashtags = result.get("hashtags_searched", [])
print(f"  hashtags_searched: {hashtags[:5]}")
if len(hashtags) > 5:
    print(f"    ... and {len(hashtags)-5} more")

# ═══════════════════════════════════════════════════
# Section 2 — Pass 2 Stats
# ═══════════════════════════════════════════════════
print(f"\n{'─'*50}")
print("SECTION 2 — Pass 2 Stats")
print(f"{'─'*50}")
print(f"  pass2_comments:       {result.get('pass2_comments', 0)}")
print(f"  purchase_signals:     {result.get('purchase_signals', 0)}")
print(f"  negative_signals:     {result.get('negative_signals', 0)}")
print(f"  question_signals:     {result.get('question_signals', 0)}")
print(f"  avg_weighted_intent:  {result.get('avg_weighted_intent', 0)}")

# ═══════════════════════════════════════════════════
# Section 3 — Top 5 Posts
# ═══════════════════════════════════════════════════
print(f"\n{'─'*50}")
print("SECTION 3 — Top 5 Posts")
print(f"{'─'*50}")
top_posts = result.get("top_posts", [])[:5]
if not top_posts:
    print("  (none)")
for i, p in enumerate(top_posts, 1):
    url = p.get("url", "")
    url_tail = url[-20:] if len(url) > 20 else url
    caption = (p.get("caption_snippet") or "")[:50]
    print(f"\n  #{i}")
    print(f"    url (tail):       ...{url_tail}")
    print(f"    views:            {p.get('views', 0):,}")
    print(f"    likes:            {p.get('likes', 0):,}")
    print(f"    comment_count:    {p.get('comment_count', 0):,}")
    print(f"    engagement_rate:  {p.get('engagement_rate', 0):.2f}%")
    print(f"    caption_snippet:  {caption}")
    print(f"    purchase_signals: {p.get('purchase_signals', 0)}")

# ═══════════════════════════════════════════════════
# Section 4 — Aggregates
# ═══════════════════════════════════════════════════
print(f"\n{'─'*50}")
print("SECTION 4 — Aggregates")
print(f"{'─'*50}")
print(f"  total_views:        {result.get('total_views', 0):,}")
print(f"  total_likes:        {result.get('total_likes', 0):,}")
print(f"  total_shares:       {result.get('total_shares', 0):,}")
print(f"  creator_tier_score: {result.get('creator_tier_score', 0)}")

# ═══════════════════════════════════════════════════
# Section 5 — Duration + Errors
# ═══════════════════════════════════════════════════
print(f"\n{'─'*50}")
print("SECTION 5 — Duration + Errors")
print(f"{'─'*50}")
print(f"  duration (result):  {result.get('duration_seconds', 0)}s")
print(f"  duration (actual):  {elapsed:.1f}s")
print(f"  error:              {result.get('error', None)}")

# ═══════════════════════════════════════════════════
# Section 6 — Signal Row Write
# ═══════════════════════════════════════════════════
print(f"\n{'─'*50}")
print("SECTION 6 — Signal Row Write")
print(f"{'─'*50}")

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
        if "column" in err_str.lower() or "null" in err_str.lower():
            print(f"  Column error: {err_str[:200]}")
            # Try removing problematic columns and retry
            for col in list(signal_row.keys()):
                if signal_row[col] is None:
                    del signal_row[col]
            try:
                resp = sb.table("signals_social").insert(signal_row).execute()
                if resp.data:
                    print(f"  Retry OK — inserted ID: {resp.data[0].get('id', '?')}")
                else:
                    print(f"  Retry returned no data")
            except Exception as e2:
                print(f"  Retry also failed: {str(e2)[:200]}")
        else:
            print(f"  Insert failed: {err_str[:200]}")
except Exception as e:
    print(f"  FAILED to build/write signal row: {e}")
    traceback.print_exc()

print(f"\n{'='*60}")
print("DONE")
print(f"{'='*60}\n")
