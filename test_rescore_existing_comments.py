"""
Test the new comment scoring engine WITHOUT making any new Apify calls.
Re-scores existing comments for Korean Sheet Masks (product_id f0620e1e-...).

Pure DB reads + scoring — no scraping.
"""
import os
import sys
import uuid
import logging

# UTF-8 stdout for emojis / korean text
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── 1. Manual .env parsing ────────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(HERE, "agents", ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v

# Add project root to sys.path
sys.path.insert(0, HERE)

# ── 2. Logging for the two target loggers ────────────────────────────────
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
logging.getLogger("agents.base_platform_agent").setLevel(logging.INFO)
logging.getLogger("agents.scoring_engine").setLevel(logging.INFO)

# ── 3. Imports (after env + path) ────────────────────────────────────────
from agents.agent_reddit import RedditAgent
from agents.agent_tiktok import TikTokAgent
from agents.agent_instagram import InstagramAgent
from agents.scoring_engine import score_all_products
from agents.config import get_supabase

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"


def _fmt_pct(x):
    if x is None:
        return "None"
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)


def print_report(report: dict, platform: str):
    print("\n" + "=" * 72)
    print(f"REPORT — {platform.upper()}")
    print("=" * 72)

    # ─ SECTION 1 ─
    print("\n[SECTION 1 — Counts]")
    total = report.get("total_comments", 0)
    purch = report.get("purchase_signals", 0)
    neg = report.get("negative_signals", 0)
    quest = report.get("question_signals", 0)
    neutral_hi = report.get("neutral_high_intent", 0)
    ep = report.get("emoji_purchase", 0)
    epp = report.get("emoji_positive", 0)
    en = report.get("emoji_negative", 0)
    ratio = report.get("negative_ratio", 0)
    penalty = report.get("penalty_applied", 1.0)
    quality = report.get("signal_quality", "low")

    print(f"  total_comments         : {total}")
    print(f"  purchase_signals       : {purch}")
    print(f"  negative_signals       : {neg}")
    print(f"  question_signals       : {quest}")
    print(f"  neutral_high_intent    : {neutral_hi}")
    print(f"  emoji_purchase         : {ep}")
    print(f"  emoji_positive         : {epp}")
    print(f"  emoji_negative         : {en}")
    print(f"  negative_ratio         : {ratio}")
    print(f"  penalty_applied        : {penalty}")
    print(f"  signal_quality         : {quality}")

    # ─ SECTION 2 ─
    top_purch = report.get("top_purchase_comments", []) or []
    print("\n[SECTION 2 — Top 3 purchase comments]")
    if not top_purch:
        print("  (none)")
    else:
        for i, c in enumerate(top_purch[:3], 1):
            txt = (c.get("text") or "").replace("\n", " ")[:100]
            sc = c.get("score", 0)
            vw = c.get("virality_weight", 1)
            print(f"  {i}. score={sc} vw={vw}")
            print(f"     {txt}")

    # ─ SECTION 3 ─
    top_neg = report.get("top_negative_comments", []) or []
    print("\n[SECTION 3 — Top 3 negative comments]")
    if not top_neg:
        print("  (none)")
    else:
        for i, c in enumerate(top_neg[:3], 1):
            txt = (c.get("text") or "").replace("\n", " ")[:100]
            sc = c.get("score", 0)
            print(f"  {i}. score={sc}")
            print(f"     {txt}")


def main():
    print("=" * 72)
    print("STEP 8 — Re-score existing comments for Korean Sheet Masks")
    print(f"product_id = {PRODUCT_ID}")
    print("=" * 72)

    # ─ 4. Create one agent of each type ─
    reddit_agent = RedditAgent()
    tiktok_agent = TikTokAgent()
    ig_agent = InstagramAgent()

    for a in (reddit_agent, tiktok_agent, ig_agent):
        a.run_id = str(uuid.uuid4())

    # ─ 5. Generate signal reports ─
    print("\n>>> Generating signal reports from existing DB comments...")
    reddit_report = reddit_agent.generate_signal_report(PRODUCT_ID, "reddit")
    tiktok_report = tiktok_agent.generate_signal_report(PRODUCT_ID, "tiktok")
    ig_report = ig_agent.generate_signal_report(PRODUCT_ID, "instagram")

    # ─ 6. Print reports ─
    print_report(reddit_report, "reddit")
    print_report(tiktok_report, "tiktok")
    print_report(ig_report, "instagram")

    # ─ 7. Run the scoring engine ─
    print("\n" + "=" * 72)
    print("STEP 7 — Running scoring engine (watch sub-scores in logs)")
    print("=" * 72)

    db = get_supabase()
    prod_resp = db.table("products").select("*").eq("id", PRODUCT_ID).execute()
    products = prod_resp.data or []

    if not products:
        print(f"ERROR: Product {PRODUCT_ID} not found")
        return

    before = products[0]
    print("\n[Product BEFORE rescore]")
    print(f"  current_score  : {_fmt_pct(before.get('current_score'))}")
    print(f"  raw_score      : {_fmt_pct(before.get('raw_score'))}")
    print(f"  coverage_pct   : {_fmt_pct(before.get('coverage_pct'))}")
    print(f"  verdict        : {before.get('current_verdict')}")

    print("\n>>> Calling score_all_products()...")
    score_all_products(db, products, "step8-rescore")
    print(">>> Scoring complete.")

    # Re-fetch updated row
    after_resp = db.table("products") \
        .select("current_score, raw_score, coverage_pct, current_verdict, lifecycle_phase, active_jobs") \
        .eq("id", PRODUCT_ID).execute()
    after = (after_resp.data or [{}])[0]

    print("\n[Product AFTER rescore]")
    print(f"  current_score  : {_fmt_pct(after.get('current_score'))}")
    print(f"  raw_score      : {_fmt_pct(after.get('raw_score'))}")
    print(f"  coverage_pct   : {_fmt_pct(after.get('coverage_pct'))}")
    print(f"  verdict        : {after.get('current_verdict')}")

    # ─ 8. Summary of what improved ─
    print("\n" + "=" * 72)
    print("STEP 8 — Summary")
    print("=" * 72)

    def _delta(b, a):
        try:
            return f"{float(a) - float(b):+.2f}"
        except Exception:
            return "n/a"

    print(f"  current_score : {_fmt_pct(before.get('current_score'))} → "
          f"{_fmt_pct(after.get('current_score'))} ({_delta(before.get('current_score'), after.get('current_score'))})")
    print(f"  raw_score     : {_fmt_pct(before.get('raw_score'))} → "
          f"{_fmt_pct(after.get('raw_score'))} ({_delta(before.get('raw_score'), after.get('raw_score'))})")
    print(f"  coverage_pct  : {_fmt_pct(before.get('coverage_pct'))} → "
          f"{_fmt_pct(after.get('coverage_pct'))} ({_delta(before.get('coverage_pct'), after.get('coverage_pct'))})")
    print(f"  verdict       : {before.get('current_verdict')} → {after.get('current_verdict')}")

    print("\nPer-platform signal quality:")
    for label, rep in [("reddit", reddit_report), ("tiktok", tiktok_report), ("instagram", ig_report)]:
        print(f"  {label:9s}: total={rep.get('total_comments',0):<4d} "
              f"purchase={rep.get('purchase_signals',0):<3d} "
              f"negative={rep.get('negative_signals',0):<3d} "
              f"question={rep.get('question_signals',0):<3d} "
              f"quality={rep.get('signal_quality')} "
              f"penalty={rep.get('penalty_applied')}")

    print("\nDone.")


if __name__ == "__main__":
    main()
