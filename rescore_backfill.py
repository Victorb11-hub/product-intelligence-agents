"""
Backfill total_views on TikTok signal row, then rescore all products.
Run from project root: python rescore_backfill.py
"""
import sys
import os
import json
import logging

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding='utf-8')

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load .env manually
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents", ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from agents.config import get_supabase
from agents.scoring_engine import score_all_products

logging.basicConfig(level=logging.INFO, format="%(message)s")

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"
TOTAL_VIEWS = 22_508_992  # Actual sum of playCount from 30 TikTok posts

db = get_supabase()

# ── Step 1: Check if total_views column exists ──
print("\n=== STEP 1: Check total_views column ===")
try:
    test = db.table("signals_social").select("total_views").limit(1).execute()
    print("✓ total_views column exists")
except Exception as e:
    err_msg = str(e)
    if "does not exist" in err_msg or "42703" in err_msg:
        print("[X] total_views column does NOT exist.")
        print("  Run in Supabase SQL Editor:")
        print("  ALTER TABLE signals_social ADD COLUMN total_views bigint DEFAULT 0;")
        print("\nAborting -- add the column first, then rerun this script.")
        sys.exit(1)
    else:
        raise

# ── Step 2: Backfill total_views ──
print("\n=== STEP 2: Backfill total_views on TikTok signal ===")
try:
    result = db.table("signals_social").update({
        "total_views": TOTAL_VIEWS
    }).eq("product_id", PRODUCT_ID).eq("platform", "tiktok").eq("scraped_date", "2026-04-13").execute()
    if result.data:
        row = result.data[0]
        print(f"✓ Updated signal row: total_views = {TOTAL_VIEWS:,}")
        print(f"  mention_count={row.get('mention_count')}, total_upvotes={row.get('total_upvotes')}, total_views={row.get('total_views')}")
    else:
        print("✗ No matching signal row found for product_id/platform/date combo")
        # Try without date filter
        resp = db.table("signals_social").select("scraped_date").eq("product_id", PRODUCT_ID).eq("platform", "tiktok").order("scraped_date", desc=True).limit(3).execute()
        if resp.data:
            dates = [r["scraped_date"] for r in resp.data]
            print(f"  Available dates: {dates}")
            print(f"  Updating most recent: {dates[0]}")
            result2 = db.table("signals_social").update({
                "total_views": TOTAL_VIEWS
            }).eq("product_id", PRODUCT_ID).eq("platform", "tiktok").eq("scraped_date", dates[0]).execute()
            if result2.data:
                print(f"✓ Updated signal row on {dates[0]}: total_views = {TOTAL_VIEWS:,}")
        else:
            print("  No TikTok signal rows found at all")
except Exception as e:
    print(f"✗ Error updating: {e}")

# ── Step 3: Rescore ──
print("\n=== STEP 3: Rescore all products ===")
products_resp = db.table("products").select("*").eq("status", "active").execute()
products = products_resp.data or []
print(f"Found {len(products)} active products")

if not products:
    # Try without status filter
    products_resp = db.table("products").select("*").execute()
    products = products_resp.data or []
    print(f"(no status filter) Found {len(products)} total products")

for p in products:
    print(f"  - {p['name']} (id={p['id'][:8]}...)")

score_all_products(db, products, "manual-rescore")

# ── Print results ──
print("\n=== RESULTS ===")
for p in products:
    pid = p["id"]
    updated = db.table("products").select("name,current_score,raw_score,coverage_pct,current_verdict,lifecycle_phase,fad_flag,active_jobs,total_jobs").eq("id", pid).execute()
    if updated.data:
        row = updated.data[0]
        print(f"\nProduct: {row['name']}")
        print(f"  current_score:  {row.get('current_score')}")
        print(f"  raw_score:      {row.get('raw_score')}")
        print(f"  coverage_pct:   {row.get('coverage_pct')}%")
        print(f"  current_verdict: {row.get('current_verdict')}")
        print(f"  lifecycle_phase: {row.get('lifecycle_phase')}")
        print(f"  fad_flag:       {row.get('fad_flag')}")
        print(f"  active_jobs:    {row.get('active_jobs')}/{row.get('total_jobs')}")

    # Latest scores_history
    hist = db.table("scores_history").select("*").eq("product_id", pid).order("created_at", desc=True).limit(1).execute()
    if hist.data:
        h = hist.data[0]
        print(f"\n  Latest scores_history:")
        print(f"    scored_date:            {h.get('scored_date')}")
        print(f"    composite_score:        {h.get('composite_score')}")
        print(f"    demand_validation:      {h.get('demand_validation_score')}")
        print(f"    purchase_intent:        {h.get('purchase_intent_score')}")
        print(f"    supply_readiness:       {h.get('supply_readiness_score')}")
        print(f"    verdict:                {h.get('verdict')}")
        print(f"    verdict_reasoning:      {h.get('verdict_reasoning')}")
        print(f"    score_change:           {h.get('score_change')}")
        print(f"    data_confidence:        {h.get('data_confidence')}")
        print(f"    platforms_used:         {h.get('platforms_used')}")

print("\nDone.")
