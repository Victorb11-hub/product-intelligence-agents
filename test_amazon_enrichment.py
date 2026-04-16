"""
Test Amazon agent with new field enrichment on Korean Sheet Masks.
Tests: scrape, new fields, signal row write, scoring engine Job 3 breakdown.
"""
import sys
import os
import uuid
import logging
import json
import re
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
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

print("=" * 80)
print("AMAZON AGENT ENRICHMENT TEST — Korean Sheet Masks")
print("=" * 80)

# Step 4: Create agent, set run_id
agent = AmazonAgent()
agent.run_id = str(uuid.uuid4())
print(f"\nRun ID: {agent.run_id}")

# Step 5: Product definition
product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty"],
}
print(f"Product: {product['name']}")
print(f"Keywords: {product['keywords']}")

# ─── Get old score before scrape ───
old_score_data = None
try:
    old_resp = agent.supabase.table("products") \
        .select("current_score, raw_score, coverage_pct, current_verdict, lifecycle_phase") \
        .eq("id", product["id"]).execute()
    if old_resp.data:
        old_score_data = old_resp.data[0]
        print(f"\nOLD scores (before this run):")
        for k, v in old_score_data.items():
            print(f"  {k}: {v}")
except Exception as e:
    print(f"  Could not fetch old scores: {e}")

# ─── Get old signal row for comparison ───
old_signal = None
try:
    old_sig_resp = agent.supabase.table("signals_retail") \
        .select("monthly_purchase_volume, bsr_rank_actual, best_bsr_category, ai_review_summary, ai_review_sentiment, review_velocity_monthly, bestseller_rank, satisfaction_score, review_count") \
        .eq("product_id", product["id"]).eq("platform", "amazon") \
        .order("scraped_date", desc=True).limit(1).execute()
    if old_sig_resp.data:
        old_signal = old_sig_resp.data[0]
        print(f"\nOLD signal row (most recent):")
        for k, v in old_signal.items():
            val_str = str(v)[:100] if v else str(v)
            print(f"  {k}: {val_str}")
except Exception as e:
    print(f"  Could not fetch old signal: {e}")

# ═══════════════════════════════════════
# Step 6: Run scrape
# ═══════════════════════════════════════
print("\n" + "=" * 80)
print("RUNNING SCRAPE (Pass 1 + Pass 2)...")
print("=" * 80)

result = agent.scrape(product["name"], product["keywords"], product)

print("\n" + "=" * 80)
print("SCRAPE RESULTS")
print("=" * 80)
print(f"  Products found: {result.get('products_found', 0)}")
print(f"  Mention count (top N): {result.get('mention_count', 0)}")
print(f"  Avg Rating: {result.get('avg_rating', 'N/A')}")
print(f"  Total Reviews: {result.get('review_count', 'N/A')}")
print(f"  Best BSR: {result.get('bestseller_rank', 'N/A')}")
print(f"  Avg Price: ${result.get('price', 0):.2f}" if result.get('price') else "  Avg Price: N/A")

# ═══════════════════════════════════════
# Step 6b: Print NEW enrichment fields
# ═══════════════════════════════════════
print("\n" + "=" * 80)
print("NEW ENRICHMENT FIELDS")
print("=" * 80)

mpv = result.get("monthly_purchase_volume", 0)
bsr_actual = result.get("bestseller_rank", 0)
bsr_cat = result.get("best_bsr_category", "")
ai_summary = result.get("ai_review_summary") or ""
ai_sentiment = result.get("ai_review_sentiment", 0)
vel_monthly = result.get("review_velocity_monthly", 0)

print(f"  monthly_purchase_volume:  {mpv}")
print(f"  bsr_rank_actual:          {bsr_actual}")
print(f"  bsr_category:             {bsr_cat}")
print(f"  ai_review_summary:        {ai_summary[:200]}{'...' if len(ai_summary) > 200 else ''}")
print(f"  ai_review_sentiment:      {ai_sentiment}")
print(f"  review_velocity_monthly:  {vel_monthly}")

# Rating distribution
print(f"\n  Rating Distribution:")
print(f"    5-star: {result.get('five_star_pct', 0):.1f}%")
print(f"    4-star: {result.get('four_star_pct', 0):.1f}%")
print(f"    3-star: {result.get('three_star_pct', 0):.1f}%")
print(f"    2-star: {result.get('two_star_pct', 0):.1f}%")
print(f"    1-star: {result.get('one_star_pct', 0):.1f}%")
print(f"  Satisfaction Score: {result.get('satisfaction_score', 0)}")

# ═══════════════════════════════════════
# Step 7: Write signal row to signals_retail
# ═══════════════════════════════════════
print("\n" + "=" * 80)
print("WRITE SIGNAL ROW TO signals_retail")
print("=" * 80)

signal_row = agent.build_signal_row(result, product["id"])
print("\n  Signal row fields:")
for k, v in signal_row.items():
    val_str = str(v)[:120] if v else str(v)
    print(f"    {k}: {val_str}")

# Try inserting with retry on column issues
columns_to_try = dict(signal_row)
max_attempts = 5
for attempt in range(1, max_attempts + 1):
    try:
        resp = agent.supabase.table("signals_retail").insert(columns_to_try).execute()
        print(f"\n  SUCCESS: Signal row written (attempt {attempt})")
        if resp.data:
            print(f"  Row ID: {resp.data[0].get('id', 'N/A')}")
        break
    except Exception as e:
        error_str = str(e)
        print(f"\n  ATTEMPT {attempt} FAILED: {error_str[:300]}")
        col_match = re.search(r'column\s+"?(\w+)"?\s+.*does not exist', error_str, re.IGNORECASE)
        if col_match:
            bad_col = col_match.group(1)
            print(f"  Removing column '{bad_col}' and retrying...")
            columns_to_try.pop(bad_col, None)
        else:
            removable = [k for k in columns_to_try if k not in ("product_id", "scraped_date", "platform")]
            if removable and attempt < max_attempts:
                removed = removable[-1]
                columns_to_try.pop(removed)
                print(f"  Removing column '{removed}' and retrying...")
            else:
                print(f"  Cannot retry further.")
                traceback.print_exc()
                break

# ═══════════════════════════════════════
# Step 8: Run scoring engine
# ═══════════════════════════════════════
print("\n" + "=" * 80)
print("RUN SCORING ENGINE")
print("=" * 80)

from agents.scoring_engine import score_all_products, _settings_cache, _load_settings, _env_float
import math

# Score the product
print("  Running score_all_products...")
score_all_products(agent.supabase, [product], "amazon-enrichment-test")

# ─── Job 3 Breakdown (recalculate locally for display) ───
print("\n" + "-" * 60)
print("JOB 3 BREAKDOWN (Purchase Intent)")
print("-" * 60)

# Re-fetch the amazon signal we just wrote
try:
    amz_resp = agent.supabase.table("signals_retail") \
        .select("*") \
        .eq("product_id", product["id"]).eq("platform", "amazon") \
        .order("scraped_date", desc=True).limit(1).execute()
    amazon_sig = amz_resp.data[0] if amz_resp.data else {}
except Exception:
    amazon_sig = {}

if amazon_sig:
    mpv_val = amazon_sig.get("monthly_purchase_volume", 0) or 0
    t1 = _env_float("AMAZON_VOLUME_TIER_1", 100)
    t2 = _env_float("AMAZON_VOLUME_TIER_2", 500)
    t3 = _env_float("AMAZON_VOLUME_TIER_3", 1000)
    t4 = _env_float("AMAZON_VOLUME_TIER_4", 5000)
    t5 = _env_float("AMAZON_VOLUME_TIER_5", 10000)
    if mpv_val >= t5: vol_norm = 100
    elif mpv_val >= t4: vol_norm = 90
    elif mpv_val >= t3: vol_norm = 70
    elif mpv_val >= t2: vol_norm = 50
    elif mpv_val >= t1: vol_norm = 30
    elif mpv_val > 0: vol_norm = 10
    else: vol_norm = 0

    rank = amazon_sig.get("bsr_rank_actual") or amazon_sig.get("bestseller_rank") or 0
    bsr_trend_val = amazon_sig.get("bsr_trend", "unknown")
    bt1 = _env_float("AMAZON_BSR_TIER_1", 100)
    bt2 = _env_float("AMAZON_BSR_TIER_2", 1000)
    bt3 = _env_float("AMAZON_BSR_TIER_3", 10000)
    bt4 = _env_float("AMAZON_BSR_TIER_4", 50000)
    bt5 = _env_float("AMAZON_BSR_TIER_5", 100000)
    if rank and rank > 0:
        if rank < bt1: bsr_norm = 90
        elif rank < bt2: bsr_norm = 70
        elif rank < bt3: bsr_norm = 50
        elif rank < bt4: bsr_norm = 30
        elif rank < bt5: bsr_norm = 10
        else: bsr_norm = 0
        if bsr_trend_val == "rising": bsr_norm = min(100, bsr_norm + 10)
        elif bsr_trend_val == "declining": bsr_norm = max(0, bsr_norm - 10)
    else:
        bsr_norm = 40

    satisfaction_val = amazon_sig.get("satisfaction_score", 0) or 0
    satisfaction_norm = max(0, min(100, satisfaction_val))

    high_intent = amazon_sig.get("high_intent_count", 0) or 0
    total_reviews = amazon_sig.get("review_count", 1) or 1
    repeat_norm = min(100, (high_intent / max(total_reviews, 1)) * 500)

    vel_m = amazon_sig.get("review_velocity_monthly", 0) or 0
    vel_norm = min(100, max(0, math.log10(max(vel_m + 1, 1)) * 30))

    w_vol = _env_float("AMAZON_WEIGHT_MONTHLY_VOLUME", 0.30)
    w_rep = _env_float("AMAZON_WEIGHT_REPEAT_PURCHASE", 0.25)
    w_sat = _env_float("AMAZON_WEIGHT_SATISFACTION", 0.20)
    w_bsr = _env_float("AMAZON_WEIGHT_BSR", 0.15)
    w_vel = _env_float("AMAZON_WEIGHT_REVIEW_VELOCITY", 0.10)
    w_sum = w_vol + w_rep + w_sat + w_bsr + w_vel
    if abs(w_sum - 1.0) > 0.01:
        w_vol, w_rep, w_sat, w_bsr, w_vel = w_vol/w_sum, w_rep/w_sum, w_sat/w_sum, w_bsr/w_sum, w_vel/w_sum

    job3_total = vol_norm * w_vol + repeat_norm * w_rep + satisfaction_norm * w_sat + bsr_norm * w_bsr + vel_norm * w_vel

    print(f"  vol_norm:          {vol_norm:.1f}  (monthly_volume={mpv_val}, weight={w_vol:.2f})")
    print(f"  repeat_norm:       {repeat_norm:.1f}  (high_intent={high_intent}, reviews={total_reviews}, weight={w_rep:.2f})")
    print(f"  satisfaction_norm: {satisfaction_norm:.1f}  (satisfaction={satisfaction_val}, weight={w_sat:.2f})")
    print(f"  bsr_norm:          {bsr_norm:.1f}  (rank={rank}, trend={bsr_trend_val}, weight={w_bsr:.2f})")
    print(f"  vel_norm:          {vel_norm:.1f}  (vel_monthly={vel_m}, weight={w_vel:.2f})")
    print(f"  ────────────────────────────────────────")
    print(f"  JOB 3 TOTAL:       {job3_total:.1f}")
else:
    print("  No Amazon signal found — cannot compute Job 3 breakdown")

# ─── Final scores ───
print("\n" + "-" * 60)
print("FINAL PRODUCT SCORES")
print("-" * 60)

try:
    prod_resp = agent.supabase.table("products") \
        .select("current_score, raw_score, coverage_pct, current_verdict, lifecycle_phase, active_jobs, total_jobs, fad_flag") \
        .eq("id", product["id"]).execute()
    if prod_resp.data:
        p = prod_resp.data[0]
        print(f"  current_score:   {p.get('current_score')}")
        print(f"  raw_score:       {p.get('raw_score')}")
        print(f"  coverage_pct:    {p.get('coverage_pct')}%")
        print(f"  current_verdict: {p.get('current_verdict')}")
        print(f"  lifecycle_phase: {p.get('lifecycle_phase')}")
        print(f"  active_jobs:     {p.get('active_jobs')}")
        print(f"  fad_flag:        {p.get('fad_flag')}")
except Exception as e:
    print(f"  Failed to query product: {e}")

# ─── Old vs New comparison ───
if old_score_data:
    print("\n" + "-" * 60)
    print("OLD vs NEW COMPARISON")
    print("-" * 60)
    try:
        new_resp = agent.supabase.table("products") \
            .select("current_score, raw_score, coverage_pct, current_verdict, lifecycle_phase") \
            .eq("id", product["id"]).execute()
        if new_resp.data:
            new_data = new_resp.data[0]
            for key in ["current_score", "raw_score", "coverage_pct", "current_verdict", "lifecycle_phase"]:
                old_val = old_score_data.get(key, "N/A")
                new_val = new_data.get(key, "N/A")
                changed = " <<<" if old_val != new_val else ""
                print(f"  {key:20s}  OLD: {str(old_val):>10s}  →  NEW: {str(new_val):>10s}{changed}")
    except Exception as e:
        print(f"  Could not compare: {e}")

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
