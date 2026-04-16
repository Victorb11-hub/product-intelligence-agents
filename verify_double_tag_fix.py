"""
Verify the two recent fixes:
  1. base_platform_agent.score_comments() — purchase vs negative tie-break (purchase wins ties)
  2. scheduler.pipeline_runs insert with phase="full_pipeline" sentinel

NO Apify calls. Pure DB read/score/update + one disposable pipeline_runs row.
"""
import os
import sys
import logging
from datetime import datetime
from collections import defaultdict

# UTF-8 stdout for emojis / korean text
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── 1. Manual .env parsing (utf-8) ───────────────────────────────────────
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

# ── 2. Logging ───────────────────────────────────────────────────────────
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
logging.getLogger("agents.base_platform_agent").setLevel(logging.WARNING)
logging.getLogger("agents.scoring_engine").setLevel(logging.INFO)

# ── 3. Imports ───────────────────────────────────────────────────────────
from agents.config import get_supabase
from agents.agent_reddit import RedditAgent
from agents.scoring_engine import score_all_products

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"
PLATFORMS = ["reddit", "tiktok", "instagram", "amazon"]

# score_comments() lives on BasePlatformAgent and is platform-agnostic
# (text in / counts out). One agent instance scores all platforms.
SCORER = RedditAgent()


def get_agent(platform):
    return SCORER


def fetch_all_comments(db, product_id):
    """Page through comments table for this product across all 4 platforms."""
    all_rows = []
    for platform in PLATFORMS:
        offset = 0
        page = 1000
        while True:
            resp = (
                db.table("comments")
                .select("id, platform, comment_body, is_buy_intent, is_problem_language")
                .eq("product_id", product_id)
                .eq("platform", platform)
                .range(offset, offset + page - 1)
                .execute()
            )
            data = resp.data or []
            if not data:
                break
            all_rows.extend(data)
            if len(data) < page:
                break
            offset += page
    return all_rows


def main():
    db = get_supabase()
    print("=" * 78)
    print("VERIFY DOUBLE-TAG FIX + pipeline_runs sentinel fix")
    print(f"Product: {PRODUCT_ID}  (Korean Sheet Masks)")
    print("=" * 78)

    # === Capture product BEFORE everything ===
    prod_resp = db.table("products").select("*").eq("id", PRODUCT_ID).execute()
    if not prod_resp.data:
        print("ERROR: product not found.")
        return
    product_before = prod_resp.data[0]
    score_before = product_before.get("current_score")
    raw_before = product_before.get("raw_score")
    print(f"\n[BEFORE]  current_score={score_before}  raw_score={raw_before}")

    # =======================================================================
    # PART 1 — VERIFY DOUBLE-TAG FIX
    # =======================================================================
    print("\n" + "=" * 78)
    print("PART 1 — Re-score all comments with new logic")
    print("=" * 78)

    rows = fetch_all_comments(db, PRODUCT_ID)
    print(f"Pulled {len(rows)} comments from DB across {len(PLATFORMS)} platforms")

    # Group by platform
    by_platform = defaultdict(list)
    for r in rows:
        by_platform[r["platform"]].append(r)

    # Re-score each platform with a fresh agent
    rescored = {}  # platform -> list of scored_comment dicts (with id retained)
    db_counts = {}  # platform -> {"buy": n, "neg": n}
    new_counts = {}  # platform -> {"purchase": n, "negative": n, "question": n, "neutral_high": n}

    for platform in PLATFORMS:
        plat_rows = by_platform.get(platform, [])
        db_buy = sum(1 for r in plat_rows if r.get("is_buy_intent"))
        db_neg = sum(1 for r in plat_rows if r.get("is_problem_language"))
        db_counts[platform] = {"buy": db_buy, "neg": db_neg, "total": len(plat_rows)}

        # Build minimal dicts for scoring (text + id passthrough)
        comments_in = [
            {"text": r.get("comment_body") or "", "id": r["id"]}
            for r in plat_rows
        ]

        if not comments_in:
            new_counts[platform] = {"purchase": 0, "negative": 0, "question": 0, "neutral_high": 0}
            rescored[platform] = []
            continue

        agent = get_agent(platform)
        result = agent.score_comments(comments_in, parent_virality=1.0)

        new_counts[platform] = {
            "purchase": result["purchase_signal_count"],
            "negative": result["negative_signal_count"],
            "question": result["question_signal_count"],
            "neutral_high": result["neutral_high_intent_count"],
        }
        rescored[platform] = result["scored_comments"]

    # Side-by-side table
    print("\n| Platform   | Total | DB buy | New purch | DB neg | New neg | DB neg ratio | New neg ratio |")
    print("|------------|-------|--------|-----------|--------|---------|--------------|---------------|")
    for platform in PLATFORMS:
        dc = db_counts[platform]
        nc = new_counts[platform]
        total = max(dc["total"], 1)
        db_ratio = (dc["neg"] / max(dc["buy"] + dc["neg"], 1)) if (dc["buy"] + dc["neg"]) else 0.0
        new_ratio = (nc["negative"] / max(nc["purchase"] + nc["negative"], 1)) if (nc["purchase"] + nc["negative"]) else 0.0
        print(f"| {platform:10s} | {dc['total']:5d} | {dc['buy']:6d} | {nc['purchase']:9d} | {dc['neg']:6d} | {nc['negative']:7d} | {db_ratio:12.3f} | {new_ratio:13.3f} |")

    # Find double-tagged comments (DB has both flags TRUE)
    print("\n[Double-tagged in DB — both is_buy_intent AND is_problem_language]")
    doubles_by_platform = {}
    for platform in PLATFORMS:
        plat_rows = by_platform.get(platform, [])
        # Map id -> rescored entry
        rescored_by_id = {sc["id"]: sc for sc in rescored.get(platform, [])}
        doubles = []
        for r in plat_rows:
            if r.get("is_buy_intent") and r.get("is_problem_language"):
                sc = rescored_by_id.get(r["id"])
                if sc:
                    if sc.get("_is_purchase"):
                        resolution = "purchase"
                    elif sc.get("_is_negative"):
                        resolution = "negative"
                    else:
                        resolution = "neither"
                else:
                    resolution = "(too short / skipped)"
                doubles.append({
                    "id": r["id"],
                    "text": (r.get("comment_body") or "")[:100],
                    "resolution": resolution,
                })
        doubles_by_platform[platform] = doubles
        print(f"  {platform}: {len(doubles)} double-tagged")

    # Print up to 5 examples across all platforms
    print("\n[Up to 5 example flips]")
    examples_shown = 0
    for platform in PLATFORMS:
        for ex in doubles_by_platform[platform]:
            if examples_shown >= 5:
                break
            txt = ex["text"].replace("\n", " ")
            print(f"  [{platform}] -> {ex['resolution']}")
            print(f"    \"{txt}\"")
            examples_shown += 1
        if examples_shown >= 5:
            break
    if examples_shown == 0:
        print("  (no comments are tagged BOTH in DB)")

    # =======================================================================
    # PART 2 — UPDATE DB COMMENT FLAGS
    # =======================================================================
    print("\n" + "=" * 78)
    print("PART 2 — Write corrected flags back to DB")
    print("=" * 78)

    changed = 0
    unchanged = 0
    skipped_short = 0
    update_errors = 0

    for platform in PLATFORMS:
        plat_rows = by_platform.get(platform, [])
        rescored_by_id = {sc["id"]: sc for sc in rescored.get(platform, [])}

        for r in plat_rows:
            sc = rescored_by_id.get(r["id"])
            if sc is None:
                # Comment was too short and skipped — clear both flags if they were set
                new_buy = False
                new_neg = False
                skipped_short += 1
            else:
                new_buy = bool(sc.get("_is_purchase"))
                new_neg = bool(sc.get("_is_negative"))

            cur_buy = bool(r.get("is_buy_intent"))
            cur_neg = bool(r.get("is_problem_language"))

            if cur_buy == new_buy and cur_neg == new_neg:
                unchanged += 1
                continue

            try:
                db.table("comments").update({
                    "is_buy_intent": new_buy,
                    "is_problem_language": new_neg,
                }).eq("id", r["id"]).execute()
                changed += 1
            except Exception as e:
                update_errors += 1
                if update_errors <= 3:
                    print(f"  ! update failed for {r['id']}: {str(e)[:120]}")

    print(f"\n  changed   : {changed}")
    print(f"  unchanged : {unchanged}")
    print(f"  short skipped (rescored as both False): {skipped_short}")
    print(f"  update errors: {update_errors}")

    # =======================================================================
    # PART 3 — NEW SIGNAL SUMMARY
    # =======================================================================
    print("\n" + "=" * 78)
    print("PART 3 — Corrected signal summary (re-query DB)")
    print("=" * 78)

    rows_after = fetch_all_comments(db, PRODUCT_ID)
    by_plat_after = defaultdict(list)
    for r in rows_after:
        by_plat_after[r["platform"]].append(r)

    total_purch = 0
    total_neg = 0
    print("\n| Platform   | Total | Purchase | Negative | Neg ratio |")
    print("|------------|-------|----------|----------|-----------|")
    for platform in PLATFORMS:
        plat = by_plat_after.get(platform, [])
        p = sum(1 for r in plat if r.get("is_buy_intent"))
        n = sum(1 for r in plat if r.get("is_problem_language"))
        total_purch += p
        total_neg += n
        ratio = (n / max(p + n, 1)) if (p + n) else 0.0
        print(f"| {platform:10s} | {len(plat):5d} | {p:8d} | {n:8d} | {ratio:9.3f} |")
    overall_ratio = (total_neg / max(total_purch + total_neg, 1)) if (total_purch + total_neg) else 0.0
    print(f"\n  TOTAL purchase = {total_purch}")
    print(f"  TOTAL negative = {total_neg}")
    print(f"  Overall negative ratio = {overall_ratio:.3f}")

    # =======================================================================
    # PART 4 — VERIFY pipeline_runs FIX
    # =======================================================================
    print("\n" + "=" * 78)
    print("PART 4 — pipeline_runs insert with phase=full_pipeline")
    print("=" * 78)

    test_id = None
    try:
        test_row = db.table("pipeline_runs").insert({
            "phase": "full_pipeline",
            "run_type": "test",
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "lookback_days": 7,
            "is_backfill": False,
        }).execute()
        test_id = test_row.data[0]["id"]
        print(f"  Insert OK  — id={test_id}")

        db.table("pipeline_runs").update({
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
            "duration_seconds": 0,
            "products_processed": 0,
        }).eq("id", test_id).execute()
        print("  Update OK")

        db.table("pipeline_runs").delete().eq("id", test_id).execute()
        print("  Delete OK — test row cleaned up")
    except Exception as e:
        print(f"  FAILED: {e!r}")
        if test_id:
            try:
                db.table("pipeline_runs").delete().eq("id", test_id).execute()
                print(f"  cleaned up partial row {test_id}")
            except Exception:
                pass

    # =======================================================================
    # PART 5 — RE-RUN SCORING ENGINE
    # =======================================================================
    print("\n" + "=" * 78)
    print("PART 5 — Re-run scoring engine on Korean Sheet Masks")
    print("=" * 78)

    products = db.table("products").select("*").eq("id", PRODUCT_ID).execute().data or []
    if not products:
        print("  ERROR: product not found.")
        return

    print("  Calling score_all_products()...")
    score_all_products(db, products, "step12-double-tag-fix")
    print("  Scoring complete.")

    after = db.table("products").select("current_score, raw_score, coverage_pct, current_verdict") \
        .eq("id", PRODUCT_ID).execute().data
    after = (after or [{}])[0]
    score_after = after.get("current_score")
    raw_after = after.get("raw_score")

    def _delta(b, a):
        try:
            return f"{float(a) - float(b):+.3f}"
        except Exception:
            return "n/a"

    print("\n[Score delta]")
    print(f"  current_score : {score_before} -> {score_after}  ({_delta(score_before, score_after)})")
    print(f"  raw_score     : {raw_before} -> {raw_after}  ({_delta(raw_before, raw_after)})")
    print(f"  coverage_pct  : {after.get('coverage_pct')}")
    print(f"  verdict       : {after.get('current_verdict')}")

    print("\n" + "=" * 78)
    print("DONE")
    print("=" * 78)


if __name__ == "__main__":
    main()
