"""
Test Reddit agent on Korean Sheet Masks.
Validates the rebuilt Pass 1 uses subreddit-scoped search URLs.
"""
import os
import sys
import uuid
import time
import logging
from pathlib import Path
from dotenv import load_dotenv

# Fix Windows console encoding for emoji/unicode
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ── Setup ──
PROJECT_ROOT = str(Path(__file__).resolve().parent)
load_dotenv(os.path.join(PROJECT_ROOT, "agents", ".env"), override=True)
sys.path.insert(0, PROJECT_ROOT)

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logging.getLogger("agents.agent_reddit").setLevel(logging.INFO)
logging.getLogger("agents.base_platform_agent").setLevel(logging.INFO)

from agents.agent_reddit import RedditAgent

# ── Product ──
product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty", "face mask", "skincare"],
    "backfill_completed": True,
    "total_runs": 5,
}

# ── Run ──
agent = RedditAgent()
agent.run_id = str(uuid.uuid4())
print(f"Run ID: {agent.run_id}\n")

print("=" * 60)
print("  REDDIT AGENT TEST -- Korean Sheet Masks")
print("=" * 60)

start = time.time()
result = agent.scrape(product["name"], product["keywords"], product)
elapsed = time.time() - start

# ══════════════════════════════════════════════════════
# SECTION 1 -- Pass 1
# ══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  SECTION 1 -- Pass 1 (Subreddit Search)")
print("=" * 60)
print(f"  Total posts returned:    {result.get('pass1_total', 0)}")
print(f"  Passed filter:           {result.get('pass1_passed', 0)}")
print(f"  Kept for Pass 2:         {result.get('pass2_posts', 0)}")
print(f"  Subreddits searched:     {result.get('subreddits_searched', [])}")

top_posts = result.get("top_posts", [])
print(f"\n  First 3 post titles + subreddits:")
for i, p in enumerate(top_posts[:3], 1):
    title = (p.get("title") or "")[:80].encode('ascii', 'replace').decode()
    sub = p.get("subreddit", "?")
    # communityName may already have "r/" prefix
    sub_display = sub if sub.startswith("r/") else f"r/{sub}"
    print(f"    {i}. {sub_display} -- {title}")

# Relevance check
skincare_subs = {"skincareaddiction", "asianbeauty", "beauty", "skincare",
                 "kbeauty", "30plusskincare", "scacjdiscussion", "makeupaddiction"}
if top_posts:
    subs_found = {(p.get("subreddit") or "").lower() for p in top_posts[:3]}
    irrelevant = subs_found - skincare_subs - {""}
    if irrelevant:
        print(f"\n  WARNING: POSSIBLE IRRELEVANT subreddits in top 3: {irrelevant}")
    else:
        print(f"\n  OK: All top 3 posts are from skincare-related subreddits")

# ══════════════════════════════════════════════════════
# SECTION 2 -- Pass 2
# ══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  SECTION 2 -- Pass 2 (Comments + Signals)")
print("=" * 60)
print(f"  Comments pulled:         {result.get('pass2_comments', 0)}")
print(f"  Purchase signals:        {result.get('purchase_signals', 0)}")
print(f"  Negative signals:        {result.get('negative_signals', 0)}")
print(f"  Question signals:        {result.get('question_signals', 0)}")
print(f"  Avg weighted intent:     {result.get('avg_weighted_intent', 0):.4f}")
print(f"  Weighted sentiment:      {result.get('weighted_sentiment', 0):.4f}")
print(f"  High intent count:       {result.get('high_intent_count', 0)}")

# ══════════════════════════════════════════════════════
# SECTION 3 -- Top 5 Posts
# ══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  SECTION 3 -- Top 5 Posts by Discussion Score")
print("=" * 60)
for i, p in enumerate(top_posts[:5], 1):
    title = (p.get("title") or "")[:60].encode('ascii', 'replace').decode()
    sub = p.get("subreddit", "?")
    sub_display = sub if sub.startswith("r/") else f"r/{sub}"
    ups = p.get("upvotes", 0)
    cmt = p.get("comment_count", 0)
    ds = p.get("discussion_score", 0)
    print(f"  {i}. [{title}]")
    print(f"     {sub_display}  |  {ups} upvotes  |  {cmt} comments  |  disc_score={ds}")

# ══════════════════════════════════════════════════════
# SECTION 4 -- Top 5 Comments by Intent
# ══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  SECTION 4 -- Top 5 Comments by Intent")
print("=" * 60)
if top_posts:
    for i, p in enumerate(top_posts[:5], 1):
        tc = (p.get("top_comment") or "")[:100].encode('ascii', 'replace').decode()
        ps = p.get("purchase_signals", 0)
        ns = p.get("negative_signals", 0)
        if tc:
            print(f"  {i}. \"{tc}\"")
            print(f"     purchase_signals={ps}  negative_signals={ns}")
        else:
            print(f"  {i}. (no top comment)  purchase={ps}  negative={ns}")
else:
    print("  No posts available")

# ══════════════════════════════════════════════════════
# SECTION 5 -- Duration + Errors
# ══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  SECTION 5 -- Duration + Errors")
print("=" * 60)
print(f"  Total duration:          {elapsed:.1f}s")
print(f"  Agent reported duration: {result.get('duration_seconds', 0)}s")
print(f"  Error:                   {result.get('error', None)}")
print(f"  Texts for pipeline:      {len(result.get('texts', []))}")
print("=" * 60)
print("\nDone.")
