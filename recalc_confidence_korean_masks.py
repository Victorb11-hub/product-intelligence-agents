"""Recalculate confidence for Korean Sheet Masks using update_confidence() logic.
NO Apify calls. Just reads counts and updates products table."""
import os
import sys
import uuid
import logging
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# Load .env from agents/
from dotenv import load_dotenv
load_dotenv(ROOT / "agents" / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

from agents.agent_reddit import RedditAgent
from agents.config import get_supabase

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"


def fetch_product_confidence(sb):
    r = sb.table("products").select(
        "id,name,confidence_level,confidence_reason,total_comments_scored,active_platform_count"
    ).eq("id", PRODUCT_ID).execute()
    rows = r.data or []
    return rows[0] if rows else None


def fetch_underlying_counts(sb):
    # Total comments
    r = sb.table("comments").select("id").eq("product_id", PRODUCT_ID).execute()
    total_comments = len(r.data or [])

    # Active platforms
    active = set()
    per_table = {}
    for table in ("signals_social", "signals_retail", "signals_search", "signals_supply"):
        try:
            r = sb.table(table).select("platform").eq("product_id", PRODUCT_ID).execute()
            plats = [row.get("platform") for row in (r.data or []) if row.get("platform")]
            per_table[table] = sorted(set(plats))
            for p in plats:
                active.add(p)
        except Exception as e:
            per_table[table] = f"ERR: {e}"

    # Buy intent
    r = sb.table("comments").select("id").eq("product_id", PRODUCT_ID).eq("is_buy_intent", True).execute()
    buy_intent = len(r.data or [])

    # Problem language
    r = sb.table("comments").select("id").eq("product_id", PRODUCT_ID).eq("is_problem_language", True).execute()
    problem = len(r.data or [])

    neg_ratio = (problem / buy_intent) if buy_intent > 0 else 0.0

    return {
        "total_comments": total_comments,
        "active_platforms": sorted(active),
        "active_platform_count": len(active),
        "per_table_platforms": per_table,
        "is_buy_intent_count": buy_intent,
        "is_problem_language_count": problem,
        "negative_ratio": neg_ratio,
    }


def print_block(title, data):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    if isinstance(data, dict):
        for k, v in data.items():
            print(f"  {k}: {v}")
    else:
        print(data)


def main():
    sb = get_supabase()

    # Thresholds (from .env)
    high_comments = int(os.environ.get("CONFIDENCE_HIGH_COMMENTS", "5000"))
    high_platforms = int(os.environ.get("CONFIDENCE_HIGH_PLATFORMS", "3"))
    high_purchase = int(os.environ.get("CONFIDENCE_HIGH_PURCHASE", "100"))
    high_neg_ratio = float(os.environ.get("CONFIDENCE_HIGH_NEGATIVE_RATIO", "0.10"))
    med_comments = int(os.environ.get("CONFIDENCE_MEDIUM_COMMENTS", "1000"))
    med_platforms = int(os.environ.get("CONFIDENCE_MEDIUM_PLATFORMS", "2"))

    print_block("THRESHOLDS (from agents/.env)", {
        "CONFIDENCE_HIGH_COMMENTS": high_comments,
        "CONFIDENCE_HIGH_PLATFORMS": high_platforms,
        "CONFIDENCE_HIGH_PURCHASE": high_purchase,
        "CONFIDENCE_HIGH_NEGATIVE_RATIO": high_neg_ratio,
        "CONFIDENCE_MEDIUM_COMMENTS": med_comments,
        "CONFIDENCE_MEDIUM_PLATFORMS": med_platforms,
    })

    before = fetch_product_confidence(sb)
    print_block("BEFORE update_confidence() — products row", before)

    counts = fetch_underlying_counts(sb)
    print_block("UNDERLYING COUNTS (live from DB)", counts)

    # Build the agent
    agent = RedditAgent()
    agent.run_id = str(uuid.uuid4())
    print(f"\nRun ID: {agent.run_id}")

    print("\n--- Calling agent.update_confidence(product_id) ---")
    agent.update_confidence(PRODUCT_ID)
    print("--- Done ---")

    after = fetch_product_confidence(sb)
    print_block("AFTER update_confidence() — products row", after)

    # Gap to next level
    total_comments = counts["total_comments"]
    active_platforms = counts["active_platform_count"]
    purchase_count = counts["is_buy_intent_count"]
    neg_ratio = counts["negative_ratio"]
    level = (after or {}).get("confidence_level", "")

    gap_lines = []
    if level == "low":
        need_c = max(0, med_comments - total_comments)
        need_p = max(0, med_platforms - active_platforms)
        gap_lines.append(f"To reach MEDIUM (need BOTH):")
        gap_lines.append(f"  Comments:  have {total_comments:,} / need {med_comments:,}  -> need {need_c:,} more")
        gap_lines.append(f"  Platforms: have {active_platforms} / need {med_platforms}    -> need {need_p} more")
    elif level == "medium":
        need_c = max(0, high_comments - total_comments)
        need_p = max(0, high_platforms - active_platforms)
        need_b = max(0, (high_purchase + 1) - purchase_count)  # rule uses > high_purchase
        neg_ok = neg_ratio < high_neg_ratio
        gap_lines.append(f"To reach HIGH (need ALL):")
        gap_lines.append(f"  Comments:        have {total_comments:,} / need {high_comments:,}  -> need {need_c:,} more")
        gap_lines.append(f"  Platforms:       have {active_platforms} / need {high_platforms}     -> need {need_p} more")
        gap_lines.append(f"  Purchase signals: have {purchase_count} / need > {high_purchase}    -> need {need_b} more")
        gap_lines.append(f"  Negative ratio:  {neg_ratio*100:.2f}% / need < {high_neg_ratio*100:.2f}%  -> {'OK' if neg_ok else 'TOO HIGH'}")
    elif level == "high":
        gap_lines.append("Already at HIGH — no further level.")
    else:
        gap_lines.append(f"Unknown level: {level!r}")

    print_block("GAP TO NEXT LEVEL", "\n".join(gap_lines))

    # Diff summary
    if before and after:
        print_block("DELTA", {
            "confidence_level":      f"{before.get('confidence_level')} -> {after.get('confidence_level')}",
            "total_comments_scored": f"{before.get('total_comments_scored')} -> {after.get('total_comments_scored')}",
            "active_platform_count": f"{before.get('active_platform_count')} -> {after.get('active_platform_count')}",
            "confidence_reason":     f"{before.get('confidence_reason')!r} -> {after.get('confidence_reason')!r}",
        })


if __name__ == "__main__":
    main()
