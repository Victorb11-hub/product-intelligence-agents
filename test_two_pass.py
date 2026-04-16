"""
Test script: Two-pass comment scraping for TikTok and Instagram on Korean Sheet Masks.
Tests the updated Pass 2 changes:
  - TikTok uses clockworks/tiktok-comments-scraper (separate actor)
  - TikTok uses "commentsPerPost" input key
  - Instagram engagement threshold lowered to 50
  - No filtering needed for TikTok comments
"""
import sys
import os
import uuid
import logging
import time

# ── Setup path and env ──────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Load .env from agents/.env
env_path = os.path.join(PROJECT_ROOT, "agents", ".env")
try:
    from dotenv import load_dotenv
    load_dotenv(env_path, override=True)
    print("[setup] Loaded .env via python-dotenv")
except ImportError:
    print("[setup] python-dotenv not available, parsing .env manually")
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                os.environ[key] = val

# ── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
for logger_name in ("agents.agent_tiktok", "agents.agent_instagram",
                     "agents.skills.apify_helper", "agents.scoring_engine"):
    logging.getLogger(logger_name).setLevel(logging.INFO)

# ── Imports ─────────────────────────────────────────────────────────
from datetime import date
from agents.agent_tiktok import TikTokAgent
from agents.agent_instagram import InstagramAgent
from agents.scoring_engine import score_all_products
from agents.config import get_supabase

# ── Product ─────────────────────────────────────────────────────────
PRODUCT = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["sheet mask", "k-beauty", "face mask", "skincare"],
}

RUN_ID = str(uuid.uuid4())


def print_header(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_comment_samples(platform: str):
    """Show 5 sample comments from DB with intent and sentiment."""
    try:
        sb = get_supabase()
        rows = sb.table("comments") \
            .select("comment_body, intent_level, sentiment_score, is_buy_intent") \
            .eq("product_id", PRODUCT["id"]) \
            .eq("platform", platform) \
            .order("id", desc=True) \
            .limit(5) \
            .execute()
        if rows.data:
            print(f"\n  Sample comments (latest 5 from DB):")
            for idx, row in enumerate(rows.data, 1):
                body = (row.get("comment_body") or "")[:80].replace("\n", " ")
                il = row.get("intent_level", "?")
                ss = row.get("sentiment_score", "?")
                buy = row.get("is_buy_intent", False)
                print(f"    {idx}. [{body}]")
                print(f"       intent_level={il}  intent_score={'see level'}  sentiment={ss}  buy_intent={buy}")
        else:
            print("  (no comments found in DB to sample)")
    except Exception as e:
        print(f"  (could not fetch sample comments: {e})")


def run_tiktok():
    print_header("TIKTOK -- Two-Pass Comment Scraping")
    agent = TikTokAgent()
    agent.run_id = RUN_ID

    t0 = time.time()
    try:
        result = agent.scrape(
            product_name=PRODUCT["name"],
            keywords=PRODUCT["keywords"],
            product=PRODUCT,
        )
    except Exception as e:
        print(f"\n  FAILED: {e}")
        return None
    elapsed = time.time() - t0

    print(f"\n  Time elapsed: {elapsed:.1f}s")
    print(f"\n  PASS 1 -- Video Discovery")
    print(f"    Total videos found:        {result.get('mention_count', 0)}")
    high_view = len([v for v in result.get('raw_items', []) if (v.get('playCount') or 0) >= 10_000])
    print(f"    Videos with 10K+ views:    {high_view}")
    print(f"    Top videos sent to Pass 2: {result.get('pass2_video_count', 0)}")

    print(f"\n  PASS 2 -- Comment Scraping & Scoring")
    print(f"    Comments returned & scored:  {result.get('comments_scraped', 0)}")
    print(f"    buy_intent_comment_count:    {result.get('buy_intent_comment_count', 0)}")
    print(f"    high_intent_comment_count:   {result.get('high_intent_comment_count', 0)}")
    print(f"    avg_comment_intent:          {result.get('avg_comment_intent', 0)}")
    print(f"    comment_sentiment_score:     {result.get('comment_sentiment_score', 0)}")

    print_comment_samples("tiktok")
    return result


def run_instagram():
    print_header("INSTAGRAM -- Two-Pass Comment Scraping")
    agent = InstagramAgent()
    agent.run_id = RUN_ID

    t0 = time.time()
    try:
        result = agent.scrape(
            product_name=PRODUCT["name"],
            keywords=PRODUCT["keywords"],
            product=PRODUCT,
        )
    except Exception as e:
        print(f"\n  FAILED: {e}")
        return None
    elapsed = time.time() - t0

    print(f"\n  Time elapsed: {elapsed:.1f}s")
    print(f"\n  PASS 1 -- Post Discovery")
    print(f"    Total posts found:           {result.get('mention_count', 0)}")
    high_eng = len([p for p in result.get('raw_items', [])
                    if ((p.get('likesCount') or 0) + (p.get('commentsCount') or 0)) >= 50])
    print(f"    Posts with 50+ engagement:   {high_eng}")
    print(f"    Top posts sent to Pass 2:    {result.get('pass2_post_count', 0)}")

    print(f"\n  PASS 2 -- Comment Scraping & Scoring")
    print(f"    Comments returned & scored:  {result.get('comments_scraped', 0)}")
    print(f"    buy_intent_comment_count:    {result.get('buy_intent_comment_count', 0)}")
    print(f"    high_intent_comment_count:   {result.get('high_intent_comment_count', 0)}")
    print(f"    avg_comment_intent:          {result.get('avg_comment_intent', 0)}")
    print(f"    comment_sentiment_score:     {result.get('comment_sentiment_score', 0)}")

    print_comment_samples("instagram")
    return result


def update_signal_rows(tiktok_result, instagram_result):
    """Update existing signal rows with new comment data, or insert if none exist."""
    print_header("SIGNAL ROW UPDATES")
    db = get_supabase()
    today = date.today().isoformat()

    pairs = []
    if tiktok_result:
        pairs.append(("tiktok", tiktok_result, TikTokAgent))
    if instagram_result:
        pairs.append(("instagram", instagram_result, InstagramAgent))

    for platform, result, agent_cls in pairs:
        agent = agent_cls()
        signal_row = agent.build_signal_row(result, PRODUCT["id"])

        try:
            # Try update first
            resp = db.table("signals_social") \
                .update(signal_row) \
                .eq("product_id", PRODUCT["id"]) \
                .eq("platform", platform) \
                .eq("scraped_date", today) \
                .execute()

            if resp.data:
                print(f"  [{platform}] Updated existing signal row for {today}")
                row = resp.data[0]
            else:
                # No row for today -- insert
                resp = db.table("signals_social").insert(signal_row).execute()
                print(f"  [{platform}] Inserted new signal row for {today}")
                row = resp.data[0] if resp.data else signal_row

            print(f"    mention_count:              {row.get('mention_count', 0)}")
            print(f"    buy_intent_comment_count:   {row.get('buy_intent_comment_count', 0)}")
            print(f"    high_intent_comment_count:  {row.get('high_intent_comment_count', 0)}")
            print(f"    avg_intent_score:           {row.get('avg_intent_score', 0)}")
            print(f"    total_upvotes:              {row.get('total_upvotes', 0)}")
            print(f"    total_comment_count:        {row.get('total_comment_count', 0)}")
        except Exception as e:
            print(f"  [{platform}] Signal row write FAILED: {e}")


def run_scoring():
    """Run scoring engine and print updated product row."""
    print_header("SCORING ENGINE")
    db = get_supabase()

    try:
        score_all_products(db, [PRODUCT], RUN_ID)
    except Exception as e:
        print(f"  Scoring FAILED: {e}")
        return

    try:
        resp = db.table("products") \
            .select("current_score, raw_score, coverage_pct, current_verdict, "
                    "lifecycle_phase, active_jobs, total_jobs") \
            .eq("id", PRODUCT["id"]) \
            .execute()
        if resp.data:
            row = resp.data[0]
            print(f"\n  UPDATED PRODUCT ROW:")
            print(f"    current_score:   {row.get('current_score')}")
            print(f"    raw_score:       {row.get('raw_score')}")
            print(f"    coverage_pct:    {row.get('coverage_pct')}%")
            print(f"    verdict:         {row.get('current_verdict')}")
            print(f"    lifecycle_phase: {row.get('lifecycle_phase')}")
            print(f"    active_jobs:     {row.get('active_jobs')}/{row.get('total_jobs')}")
        else:
            print("  Product row not found!")
    except Exception as e:
        print(f"  Failed to fetch product row: {e}")


# ── Main ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print_header("TWO-PASS COMMENT SCRAPING TEST")
    print(f"  Product:  {PRODUCT['name']}")
    print(f"  ID:       {PRODUCT['id']}")
    print(f"  Keywords: {PRODUCT['keywords']}")
    print(f"  Run ID:   {RUN_ID}")
    print(f"  Date:     {date.today().isoformat()}")

    # Verify credentials
    apify = os.environ.get("APIFY_API_TOKEN", "")
    supa = os.environ.get("SUPABASE_URL", "")
    print(f"\n  APIFY_API_TOKEN: {'set (' + apify[:8] + '...)' if apify else 'MISSING'}")
    print(f"  SUPABASE_URL:    {'set (' + supa[:30] + '...)' if supa else 'MISSING'}")

    if not apify or not supa:
        print("\n  ABORT: Missing required credentials. Check agents/.env")
        sys.exit(1)

    # Run both scrapers
    tk_result = run_tiktok()
    ig_result = run_instagram()

    # Update signal rows
    update_signal_rows(tk_result, ig_result)

    # Run scoring
    run_scoring()

    # Final summary
    print_header("FINAL SUMMARY")
    if tk_result:
        print(f"  TikTok:    {tk_result.get('mention_count',0)} videos, "
              f"{tk_result.get('comments_scraped',0)} comments scored, "
              f"avg_intent={tk_result.get('avg_comment_intent',0)}, "
              f"sentiment={tk_result.get('comment_sentiment_score',0)}")
    else:
        print("  TikTok:    FAILED")

    if ig_result:
        print(f"  Instagram: {ig_result.get('mention_count',0)} posts, "
              f"{ig_result.get('comments_scraped',0)} comments scored, "
              f"avg_intent={ig_result.get('avg_comment_intent',0)}, "
              f"sentiment={ig_result.get('comment_sentiment_score',0)}")
    else:
        print("  Instagram: FAILED")

    print()
