"""
Full test of the Amazon agent with two-pass architecture on Korean Sheet Masks.
"""
import sys
import os
import uuid
import logging
import json
import traceback

# Step 1: Load env from agents/.env, add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, "agents", ".env"))

# Step 2: Import AmazonAgent
from agents.agent_amazon import AmazonAgent

# Step 3: Set logging to INFO
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logging.getLogger("agents.agent_amazon").setLevel(logging.INFO)
logging.getLogger("agents.scoring_engine").setLevel(logging.INFO)

# Reduce noise from HTTP libs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

print("=" * 80)
print("AMAZON AGENT FULL TEST — Korean Sheet Masks")
print("=" * 80)

# Step 4: Create agent instance
agent = AmazonAgent()
agent.run_id = str(uuid.uuid4())
print(f"\nRun ID: {agent.run_id}")

# Step 5: Product dict
product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty", "face mask", "skincare"],
}

print(f"Product: {product['name']}")
print(f"Keywords: {product['keywords']}")

# Step 6: Run scrape
print("\n" + "=" * 80)
print("RUNNING SCRAPE (Pass 1 + Pass 2)...")
print("=" * 80)

result = agent.scrape(product["name"], product["keywords"], product)

# Step 7: Print PASS 1 results
print("\n" + "=" * 80)
print("PASS 1 RESULTS — Product Search")
print("=" * 80)

print(f"  Total products found (mention_count): {result.get('mention_count', 0)}")

# Get top products from raw_items
raw_items = result.get("raw_items", [])
print(f"  Raw items returned: {len(raw_items)}")

# Re-derive top products info from the texts
texts = result.get("texts", [])
print(f"  Product titles collected: {len(texts)}")

# Query posts table to get pass 1 products written
try:
    posts_resp = agent.supabase.table("posts") \
        .select("post_title, post_body, post_url") \
        .eq("product_id", product["id"]) \
        .eq("run_id", agent.run_id) \
        .eq("data_type", "post") \
        .execute()
    posts_data = posts_resp.data or []
    print(f"  Products written to posts table: {len(posts_data)}")

    print("\n  Top 3 Products:")
    for i, p in enumerate(posts_data[:3], 1):
        title = (p.get("post_title") or "")[:60]
        body = p.get("post_body", "")
        # Parse ASIN, rating, reviews, BSR, price from post_body
        print(f"    {i}. {title}")
        print(f"       {body}")
except Exception as e:
    print(f"  (Could not query posts table: {e})")

# Print key metrics from result
print(f"\n  Avg Rating: {result.get('avg_rating', 'N/A')}")
print(f"  Total Reviews: {result.get('review_count', 'N/A')}")
print(f"  Best BSR: {result.get('bestseller_rank', 'N/A')}")
print(f"  Avg Price: ${result.get('price', 0):.2f}" if result.get('price') else "  Avg Price: N/A")
print(f"  Price Trend: {result.get('price_trend', 'N/A')}")

# Step 8: Print PASS 2 results
print("\n" + "=" * 80)
print("PASS 2 RESULTS — Review Extraction & Scoring")
print("=" * 80)

print(f"  Total reviews extracted and scored: {result.get('reviews_scraped', 0)}")
print(f"  Verified purchase count: {result.get('verified_purchase_count', 0)}")

# Query comments table for sample reviews
try:
    comments_resp = agent.supabase.table("comments") \
        .select("comment_body, intent_level, sentiment_score, is_buy_intent") \
        .eq("product_id", product["id"]) \
        .eq("platform", "amazon") \
        .order("intent_level", desc=True) \
        .limit(5) \
        .execute()
    comments_data = comments_resp.data or []

    print(f"\n  5 Sample Reviews (from DB, ordered by intent):")
    for i, c in enumerate(comments_data[:5], 1):
        text = (c.get("comment_body") or "")[:100]
        print(f"    {i}. \"{text}...\"")
        print(f"       intent_level={c.get('intent_level')}, sentiment_score={c.get('sentiment_score')}, buy_intent={c.get('is_buy_intent')}")
except Exception as e:
    print(f"  (Could not query comments table: {e})")

print(f"\n  high_intent_count: {result.get('high_intent_count', 0)}")
print(f"  buy_intent_count: {result.get('buy_intent_count', 0)}")
print(f"  avg_review_intent: {result.get('avg_review_intent', 0)}")
print(f"  review_sentiment (avg): {result.get('review_sentiment', 0)}")
print(f"  review_velocity: {result.get('review_velocity', 0)}")

# Step 9: Write signal row to signals_retail
print("\n" + "=" * 80)
print("STEP 9 — Write Signal Row to signals_retail")
print("=" * 80)

signal_row = agent.build_signal_row(result, product["id"])
print("\n  Signal row to write:")
for k, v in signal_row.items():
    if k == "raw_json":
        print(f"    {k}: {json.dumps(v, indent=6)}")
    else:
        print(f"    {k}: {v}")

# Try inserting with retry on column issues
columns_to_try = dict(signal_row)
max_attempts = 5
for attempt in range(1, max_attempts + 1):
    try:
        resp = agent.supabase.table("signals_retail").insert(columns_to_try).execute()
        print(f"\n  SUCCESS: Signal row written to signals_retail (attempt {attempt})")
        if resp.data:
            print(f"  Row ID: {resp.data[0].get('id', 'N/A')}")
        break
    except Exception as e:
        error_str = str(e)
        print(f"\n  ATTEMPT {attempt} FAILED: {error_str[:300]}")

        # Try to identify the problematic column from the error
        # Common patterns: "column X of relation Y does not exist"
        import re
        col_match = re.search(r'column\s+"?(\w+)"?\s+.*does not exist', error_str, re.IGNORECASE)
        if col_match:
            bad_col = col_match.group(1)
            print(f"  Removing column '{bad_col}' and retrying...")
            columns_to_try.pop(bad_col, None)
        else:
            # Try removing the last non-essential column
            removable = [k for k in columns_to_try if k not in ("product_id", "scraped_date", "platform")]
            if removable and attempt < max_attempts:
                removed = removable[-1]
                columns_to_try.pop(removed)
                print(f"  Removing column '{removed}' and retrying...")
            else:
                print(f"  Cannot retry further.")
                traceback.print_exc()
                break

# Step 10: Run scoring engine
print("\n" + "=" * 80)
print("STEP 10 — Run Scoring Engine")
print("=" * 80)

from agents.scoring_engine import score_all_products

db = agent.supabase

# Score the product
print("  Running score_all_products...")
score_all_products(db, [product], "amazon-full-test")

# Query updated product
try:
    prod_resp = db.table("products") \
        .select("current_score, raw_score, coverage_pct, current_verdict, active_jobs, lifecycle_phase, fad_flag, total_jobs") \
        .eq("id", product["id"]) \
        .execute()

    if prod_resp.data:
        p = prod_resp.data[0]
        print(f"\n  Updated Product:")
        print(f"    current_score:   {p.get('current_score')}")
        print(f"    raw_score:       {p.get('raw_score')}")
        print(f"    coverage_pct:    {p.get('coverage_pct')}%")
        print(f"    current_verdict: {p.get('current_verdict')}")
        print(f"    active_jobs:     {p.get('active_jobs')}")
        print(f"    total_jobs:      {p.get('total_jobs')}")
        print(f"    lifecycle_phase: {p.get('lifecycle_phase')}")
        print(f"    fad_flag:        {p.get('fad_flag')}")
    else:
        print("  No product found in DB for this ID.")
except Exception as e:
    print(f"  Failed to query product: {e}")

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
