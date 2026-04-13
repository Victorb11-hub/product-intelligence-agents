"""One-shot script: backfill TikTok views, rescore, print results."""
import sys, os, logging
sys.stdout.reconfigure(encoding='utf-8')

# Load env vars from agents/.env
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "agents", ".env"))

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(level=logging.INFO, format="%(message)s")

from agents.config import get_supabase
from agents.scoring_engine import score_all_products

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"

db = get_supabase()

# ── 1. BACKFILL ──────────────────────────────────────────────────────────────
print("=" * 60)
print("STEP 1: BACKFILL — Setting total_views=22508992 on TikTok signal")
resp = db.table("signals_social").update({"total_views": 22508992}) \
    .eq("product_id", PRODUCT_ID) \
    .eq("platform", "tiktok") \
    .eq("scraped_date", "2026-04-13") \
    .execute()
print(f"  Updated {len(resp.data)} row(s)")
if resp.data:
    print(f"  Confirmed total_views = {resp.data[0].get('total_views')}")

# ── 2. RESCORE ───────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 2: RESCORE — Running scoring engine")
products_resp = db.table("products").select("*").eq("active", True).execute()
products = products_resp.data or []
print(f"  Found {len(products)} active products")
score_all_products(db, products, "manual-rescore-formula-fix")
print("  Scoring complete.")

# ── 3. PRINT RESULTS ────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 3: RESULTS")

# Product row
print("\n-- Product Row --")
p = db.table("products").select("name, current_score, raw_score, coverage_pct, current_verdict, lifecycle_phase, active_jobs") \
    .eq("id", PRODUCT_ID).execute()
if p.data:
    row = p.data[0]
    for k, v in row.items():
        print(f"  {k:20s}: {v}")

# Latest scores_history
print("\n-- Latest scores_history --")
sh = db.table("scores_history").select("composite_score, demand_validation_score, verdict, platforms_used, verdict_reasoning") \
    .eq("product_id", PRODUCT_ID).order("scored_date", desc=True).limit(1).execute()
if sh.data:
    row = sh.data[0]
    for k, v in row.items():
        print(f"  {k:30s}: {v}")

# Signals social
for platform in ["reddit", "tiktok", "instagram"]:
    print(f"\n-- signals_social: {platform} --")
    sig = db.table("signals_social").select("mention_count, total_views, total_upvotes, sentiment_score, velocity") \
        .eq("product_id", PRODUCT_ID).eq("platform", platform) \
        .order("scraped_date", desc=True).limit(1).execute()
    if sig.data:
        row = sig.data[0]
        for k, v in row.items():
            print(f"  {k:20s}: {v}")
    else:
        print("  (no data)")

print("\n" + "=" * 60)
print("Done.")
