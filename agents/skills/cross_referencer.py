"""
REASONING SKILL 4 — Cross-Agent Intelligence Sharing

After all agents complete, analyzes cross-platform consensus:
- Counts platforms with positive/negative/neutral signals per product
- Identifies consensus products (3+ positive platforms)
- Identifies divergence products (mixed positive/negative)
- Writes to cross_reference_runs table
- Creates CONSENSUS alerts for new consensus products
"""
import logging
from datetime import datetime

from .summarizer import generate_cross_reference_summary

logger = logging.getLogger(__name__)

POSITIVE_THRESHOLD = 0.3   # sentiment/velocity above this = positive
NEGATIVE_THRESHOLD = -0.1  # below this = negative
CONSENSUS_MIN_PLATFORMS = 3
CORROBORATION_BONUS = 5.0


async def run_cross_reference(supabase, run_id: str, product_ids: list[str]) -> list[dict]:
    """
    Run cross-platform analysis for all products in a run.

    Args:
        supabase: Supabase client.
        run_id: The orchestrator run_id to analyze.
        product_ids: List of product IDs to analyze.

    Returns:
        List of cross-reference results.
    """
    results = []

    for product_id in product_ids:
        try:
            result = await _analyze_product(supabase, run_id, product_id)
            results.append(result)

            # Write to cross_reference_runs
            supabase.table("cross_reference_runs").insert({
                "run_id": run_id,
                "product_id": product_id,
                "platforms_positive": result["platforms_positive"],
                "platforms_negative": result["platforms_negative"],
                "platforms_neutral": result["platforms_neutral"],
                "cross_platform_score": result["cross_platform_score"],
                "consensus_flag": result["consensus_flag"],
                "divergence_flag": result["divergence_flag"],
                "analysis_summary": result["analysis_summary"],
            }).execute()

            # Create CONSENSUS alert if 3+ platforms positive for first time
            if result["consensus_flag"]:
                await _check_first_consensus(supabase, product_id, result)

            # Apply corroboration bonus
            if result["consensus_flag"]:
                await _apply_corroboration_bonus(supabase, product_id)

        except Exception as e:
            logger.error("Cross-reference failed for product %s: %s", product_id, e)

    return results


async def _analyze_product(supabase, run_id: str, product_id: str) -> dict:
    """Analyze cross-platform signals for a single product."""
    platforms_positive = []
    platforms_negative = []
    platforms_neutral = []

    # Get product name for summaries
    prod_resp = supabase.table("products").select("name").eq("id", product_id).execute()
    product_name = prod_resp.data[0]["name"] if prod_resp.data else "Unknown"

    # Check social signals
    social_resp = supabase.table("signals_social") \
        .select("platform, sentiment_score, velocity_score, velocity") \
        .eq("product_id", product_id) \
        .eq("run_id", run_id) \
        .execute()

    for row in social_resp.data:
        platform = row["platform"]
        sentiment = row.get("sentiment_score", 0) or 0
        velocity = row.get("velocity", row.get("velocity_score", 0)) or 0
        avg_signal = (sentiment + velocity) / 2

        if avg_signal > POSITIVE_THRESHOLD:
            platforms_positive.append(platform)
        elif avg_signal < NEGATIVE_THRESHOLD:
            platforms_negative.append(platform)
        else:
            platforms_neutral.append(platform)

    # Check search signals
    search_resp = supabase.table("signals_search") \
        .select("platform, slope_24m, yoy_growth") \
        .eq("product_id", product_id) \
        .eq("run_id", run_id) \
        .execute()

    for row in search_resp.data:
        slope = row.get("slope_24m", 0) or 0
        if slope > 0.02:
            platforms_positive.append("google_trends")
        elif slope < -0.01:
            platforms_negative.append("google_trends")
        else:
            platforms_neutral.append("google_trends")

    # Check retail signals
    retail_resp = supabase.table("signals_retail") \
        .select("platform, review_sentiment, rank_change_wow") \
        .eq("product_id", product_id) \
        .eq("run_id", run_id) \
        .execute()

    for row in retail_resp.data:
        sentiment = row.get("review_sentiment", 0) or 0
        rank_change = row.get("rank_change_wow", 0) or 0
        if sentiment > 0.6 or rank_change > 10:
            platforms_positive.append(row["platform"])
        elif sentiment < 0.3 or rank_change < -20:
            platforms_negative.append(row["platform"])
        else:
            platforms_neutral.append(row["platform"])

    # Check supply signals
    supply_resp = supabase.table("signals_supply") \
        .select("platform, supplier_count_change, moq_trend") \
        .eq("product_id", product_id) \
        .eq("run_id", run_id) \
        .execute()

    for row in supply_resp.data:
        change = row.get("supplier_count_change", 0) or 0
        if change > 0:
            platforms_positive.append("alibaba")
        elif change < -2:
            platforms_negative.append("alibaba")
        else:
            platforms_neutral.append("alibaba")

    # Check discovery signals
    disc_resp = supabase.table("signals_discovery") \
        .select("platform, save_rate_growth, trending_category_flag") \
        .eq("product_id", product_id) \
        .eq("run_id", run_id) \
        .execute()

    for row in disc_resp.data:
        growth = row.get("save_rate_growth", 0) or 0
        trending = row.get("trending_category_flag", False)
        if growth > 0.05 or trending:
            platforms_positive.append("pinterest")
        elif growth < -0.05:
            platforms_negative.append("pinterest")
        else:
            platforms_neutral.append("pinterest")

    # Calculate scores
    cross_platform_score = len(platforms_positive) / max(
        len(platforms_positive) + len(platforms_negative) + len(platforms_neutral), 1
    )
    consensus_flag = len(platforms_positive) >= CONSENSUS_MIN_PLATFORMS
    divergence_flag = len(platforms_positive) >= 2 and len(platforms_negative) >= 2

    # Generate summary
    summary = generate_cross_reference_summary(
        product_name, platforms_positive, platforms_negative, cross_platform_score,
    )

    # Update product cross_platform_summary
    supabase.table("products") \
        .update({"cross_platform_summary": summary}) \
        .eq("id", product_id) \
        .execute()

    return {
        "product_id": product_id,
        "product_name": product_name,
        "platforms_positive": platforms_positive,
        "platforms_negative": platforms_negative,
        "platforms_neutral": platforms_neutral,
        "cross_platform_score": round(cross_platform_score, 4),
        "consensus_flag": consensus_flag,
        "divergence_flag": divergence_flag,
        "analysis_summary": summary,
    }


async def _check_first_consensus(supabase, product_id: str, result: dict):
    """Create a CONSENSUS alert if this is the product's first time hitting 3+ platforms."""
    existing = supabase.table("cross_reference_runs") \
        .select("id") \
        .eq("product_id", product_id) \
        .eq("consensus_flag", True) \
        .execute()

    # Only alert if this is the first consensus (or second, since we just wrote one)
    if len(existing.data) <= 1:
        supabase.table("alerts").insert({
            "product_id": product_id,
            "alert_type": "green_flag",
            "priority": "high",
            "message": (
                f"{result['product_name']} achieved multi-platform consensus for the first time. "
                f"Positive signals on: {', '.join(result['platforms_positive'])}. "
                f"Cross-platform score: {result['cross_platform_score']:.0%}."
            ),
            "actioned": False,
        }).execute()

        logger.info("CONSENSUS alert created for %s", result["product_name"])


async def _apply_corroboration_bonus(supabase, product_id: str):
    """Add corroboration bonus (5 points) to product's current score."""
    try:
        prod = supabase.table("products") \
            .select("current_score") \
            .eq("id", product_id) \
            .execute()

        if prod.data:
            new_score = min(100, prod.data[0]["current_score"] + CORROBORATION_BONUS)
            supabase.table("products") \
                .update({"current_score": new_score}) \
                .eq("id", product_id) \
                .execute()

    except Exception as e:
        logger.error("Failed to apply corroboration bonus: %s", e)
