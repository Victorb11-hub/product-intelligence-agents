"""
OPERATIONAL SKILL 3 — Data Quality Scoring

Audits agent output before writing to the database.
Scores quality from 0.0 to 1.0 based on:
  - Completeness: % of expected fields with non-null values (40%)
  - Sample size: how many posts/mentions derived the signal (30%)
  - Recency: % of data from last 7 days (20%)
  - Consistency: data doesn't contradict itself (10%)

If quality < 0.4, reject to signals_low_quality table.
"""
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Expected fields per signal table
EXPECTED_FIELDS = {
    "signals_social": [
        "mention_count", "growth_rate_wow", "sentiment_score",
        "velocity_score", "creator_tier_score", "buy_intent_comment_count",
    ],
    "signals_search": [
        "slope_24m", "breakout_flag", "yoy_growth",
        "seasonal_pattern", "related_rising_queries",
    ],
    "signals_retail": [
        "bestseller_rank", "rank_change_wow", "review_count",
        "review_count_growth", "review_sentiment", "search_rank", "price",
    ],
    "signals_supply": [
        "supplier_listing_count", "supplier_count_change", "moq_current",
        "price_per_unit", "competing_supplier_count",
    ],
    "signals_discovery": [
        "pin_save_rate", "save_rate_growth", "board_creation_count",
        "keyword_search_volume", "demographic_score",
    ],
}

# Minimum sample sizes for reliable signals
MIN_SAMPLE_SIZES = {
    "signals_social": 10,
    "signals_search": 1,
    "signals_retail": 5,
    "signals_supply": 3,
    "signals_discovery": 5,
}

QUALITY_THRESHOLD = 0.4


def score_quality(
    data: dict,
    table: str,
    sample_size: int = 0,
    data_dates: list[str] = None,
) -> dict:
    """
    Score the quality of a signal data row.

    Args:
        data: The signal data dict to score.
        table: Which signals table this is for.
        sample_size: How many raw posts/mentions this was derived from.
        data_dates: List of date strings for the underlying data points.

    Returns:
        {
            "data_quality_score": float (0-1),
            "completeness_score": float,
            "sample_size_score": float,
            "recency_score": float,
            "consistency_score": float,
            "passes_threshold": bool,
            "rejection_reason": str or None,
            "breakdown": dict
        }
    """
    expected = EXPECTED_FIELDS.get(table, [])

    # 1. Completeness (40%)
    if expected:
        filled = sum(1 for f in expected if data.get(f) is not None)
        completeness = filled / len(expected)
    else:
        completeness = 1.0

    # 2. Sample size (30%)
    min_sample = MIN_SAMPLE_SIZES.get(table, 5)
    if sample_size >= min_sample * 3:
        sample_score = 1.0
    elif sample_size >= min_sample:
        sample_score = 0.7
    elif sample_size >= max(1, min_sample // 2):
        sample_score = 0.4
    elif sample_size > 0:
        sample_score = 0.2
    else:
        sample_score = 0.0

    # 3. Recency (20%)
    recency = _score_recency(data_dates)

    # 4. Consistency (10%)
    consistency = _score_consistency(data, table)

    # Weighted composite
    quality_score = (
        completeness * 0.4
        + sample_score * 0.3
        + recency * 0.2
        + consistency * 0.1
    )

    passes = quality_score >= QUALITY_THRESHOLD
    rejection_reason = None
    if not passes:
        reasons = []
        if completeness < 0.3:
            reasons.append(f"Low completeness ({completeness:.0%})")
        if sample_score < 0.3:
            reasons.append(f"Insufficient sample size ({sample_size})")
        if recency < 0.3:
            reasons.append(f"Stale data ({recency:.0%} recent)")
        if consistency < 0.3:
            reasons.append(f"Data inconsistency detected")
        rejection_reason = "; ".join(reasons) if reasons else "Below quality threshold"

    return {
        "data_quality_score": round(quality_score, 4),
        "completeness_score": round(completeness, 4),
        "sample_size_score": round(sample_score, 4),
        "recency_score": round(recency, 4),
        "consistency_score": round(consistency, 4),
        "passes_threshold": passes,
        "rejection_reason": rejection_reason,
        "breakdown": {
            "fields_filled": f"{sum(1 for f in expected if data.get(f) is not None)}/{len(expected)}",
            "sample_size": sample_size,
            "min_sample_required": min_sample,
        },
    }


def _score_recency(data_dates: list[str] | None) -> float:
    """Score what percentage of data is from the last 7 days."""
    if not data_dates:
        return 0.5  # Assume moderate recency if not tracked

    now = datetime.now()
    week_ago = now - timedelta(days=7)
    recent_count = 0

    for d in data_dates:
        try:
            dt = datetime.fromisoformat(d) if isinstance(d, str) else d
            if dt >= week_ago:
                recent_count += 1
        except (ValueError, TypeError):
            continue

    return recent_count / len(data_dates) if data_dates else 0.5


def _score_consistency(data: dict, table: str) -> float:
    """Check for internal contradictions in the data."""
    score = 1.0

    if table == "signals_social":
        sentiment = data.get("sentiment_score")
        problem_count = data.get("problem_language_comment_count", 0)
        mention_count = data.get("mention_count", 0)

        # High sentiment but high problem language ratio
        if sentiment is not None and sentiment > 0.5 and mention_count > 0:
            problem_ratio = problem_count / max(mention_count, 1)
            if problem_ratio > 0.3:
                score -= 0.3

        # Very high velocity but zero mentions
        velocity = data.get("velocity_score")
        if velocity is not None and velocity > 0.8 and mention_count == 0:
            score -= 0.5

    elif table == "signals_retail":
        rank = data.get("bestseller_rank")
        reviews = data.get("review_count", 0)
        # Top rank but zero reviews is suspicious
        if rank is not None and rank < 10 and reviews == 0:
            score -= 0.3

    return max(0.0, score)
