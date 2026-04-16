# -*- coding: utf-8 -*-
"""
Test Amazon Agent -- rating distribution extraction.
"""
import os
import sys
import uuid
import time
import json
import logging

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Load env from agents/.env
from dotenv import load_dotenv
env_path = os.path.join(os.path.dirname(__file__), "agents", ".env")
load_dotenv(env_path, override=True)

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(__file__))

from agents.agent_amazon import AmazonAgent
from agents.scoring_engine import score_all_products
from agents.config import get_supabase

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logging.getLogger("agents.agent_amazon").setLevel(logging.INFO)
logging.getLogger("agents.scoring_engine").setLevel(logging.INFO)

product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty", "face mask", "skincare"],
}

agent = AmazonAgent()
agent.run_id = str(uuid.uuid4())

print(f"\n{'='*60}")
print(f"Amazon Agent Test -- Rating Distribution")
print(f"Run ID: {agent.run_id}")
print(f"{'='*60}\n")

# --- Scrape ---
t0 = time.time()
result = agent.scrape(product["name"], product["keywords"], product)
elapsed = time.time() - t0

# --- Debug: dump starsBreakdown from raw_items ---
print(f"\n{'-'*60}")
print("DEBUG: starsBreakdown from raw_items:")
for i, item in enumerate(result.get("raw_items", [])[:3]):
    title = (item.get("title") or "?")[:40]
    bd = item.get("starsBreakdown")
    print(f"  {i+1}. {title}")
    print(f"     starsBreakdown: {bd}")
    # Check alternative keys
    for key in ["ratingBreakdown", "reviewBreakdown", "ratingsBreakdown",
                "customerReviews", "starRatings", "ratingHistogram"]:
        val = item.get(key)
        if val:
            print(f"     {key}: {val}")

# --- Results ---
print(f"\n{'-'*60}")
print(f"Elapsed: {elapsed:.1f}s")
print(f"Products found: {result.get('products_found', 0)}")
print(f"Top N kept: {result.get('mention_count', 0)}")

print(f"\nTop 3 products (from raw_items):")
for i, item in enumerate(result.get("raw_items", [])[:3]):
    title = (item.get("title") or item.get("name") or "?")[:50]
    reviews = item.get("reviewsCount") or item.get("reviews_count") or item.get("numberOfReviews") or "?"
    rating = item.get("stars") or item.get("rating") or item.get("averageRating") or "?"
    price = item.get("price") or item.get("currentPrice") or "?"
    print(f"  {i+1}. {title}  |  {reviews} reviews  |  {rating}*  |  ${price}")

print(f"\nBSR: {result.get('bestseller_rank', 'N/A')} in {result.get('best_bsr_category', 'N/A')}")

print(f"\nRating Distribution:")
print(f"  5*: {result.get('five_star_pct', 0):.1f}%")
print(f"  4*: {result.get('four_star_pct', 0):.1f}%")
print(f"  3*: {result.get('three_star_pct', 0):.1f}%")
print(f"  2*: {result.get('two_star_pct', 0):.1f}%")
print(f"  1*: {result.get('one_star_pct', 0):.1f}%")
print(f"  Total ratings: {result.get('total_ratings', 0)}")

print(f"\nSatisfaction score: {result.get('satisfaction_score', 0)}")
print(f"Review velocity: {result.get('review_velocity', 0):+d}")
print(f"BSR trend: {result.get('bsr_trend', 'unknown')}")
print(f"Rating trend: {result.get('rating_trend', 'unknown')}")

# --- Signal Row ---
print(f"\n{'-'*60}")
print("Building signal row...")
signal = agent.build_signal_row(result, product["id"])

db = get_supabase()
try:
    db.table("signals_retail").insert(signal).execute()
    print("Signal row inserted OK")
except Exception as e:
    err_msg = str(e)
    print(f"Insert failed: {err_msg[:200]}")
    # Try to identify problematic column from error
    if "column" in err_msg.lower():
        import re
        col_match = re.search(r'column\s+"?(\w+)"?', err_msg, re.IGNORECASE)
        if col_match:
            bad_col = col_match.group(1)
            print(f"Removing problematic column: {bad_col}")
            signal.pop(bad_col, None)
            try:
                db.table("signals_retail").insert(signal).execute()
                print("Signal row inserted OK (after fix)")
            except Exception as e2:
                print(f"Retry failed: {str(e2)[:200]}")

print("\nSignal row:")
for k, v in signal.items():
    print(f"  {k}: {v}")

# --- Scoring ---
print(f"\n{'-'*60}")
print("Running scoring engine...")
score_all_products(db, [product], "amazon-rating-dist-test")

# Read back the score
try:
    resp = db.table("products").select(
        "current_score, raw_score, coverage_pct, current_verdict, active_jobs"
    ).eq("id", product["id"]).execute()
    if resp.data:
        s = resp.data[0]
        print(f"\nScore results:")
        print(f"  current_score: {s.get('current_score')}")
        print(f"  raw_score: {s.get('raw_score')}")
        print(f"  coverage_pct: {s.get('coverage_pct')}")
        print(f"  verdict: {s.get('current_verdict')}")
        print(f"  active_jobs: {s.get('active_jobs')}")
    else:
        print("No score data found for product")
except Exception as e:
    print(f"Score read failed: {e}")

# --- Cost Estimate ---
print(f"\n{'-'*60}")
print("Cost estimate:")
print(f"  Pass 1 (search): ~$0.004 (30 results)")
print(f"  Pass 2 (product pages): ~${0.004 * result.get('mention_count', 10):.3f} ({result.get('mention_count', 10)} pages)")
total_cost = 0.004 + 0.004 * result.get("mention_count", 10)
print(f"  Total: ~${total_cost:.3f}")
print(f"\n{'='*60}")
