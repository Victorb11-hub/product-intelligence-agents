"""
INTELLIGENCE SKILL 3 — Fad vs Lasting Trend Classifier

Scores product signals against three pattern profiles:
- Fad: short-lived spike driven by 1-2 creators
- Lasting: organic multi-platform adoption over 6+ weeks
- Industry shift: slow steady 12+ month climb

Outputs fad_score, lasting_score, industry_shift_score (each 0-1)
and dominant classification.
"""
import logging

logger = logging.getLogger(__name__)

# Pattern signature definitions with weighted indicators
FAD_INDICATORS = {
    "single_platform_concentration": 0.25,  # >70% of signals from one platform
    "spike_without_slope": 0.20,            # High velocity but flat Google Trends
    "peak_under_3_weeks": 0.20,             # Projected peak < 21 days
    "creator_driven": 0.15,                 # High creator_tier_score, low organic mentions
    "no_repeat_purchase_language": 0.10,    # Low repeat/routine language in comments
    "low_search_confirmation": 0.10,        # No Google Trends or search volume rise
}

LASTING_INDICATORS = {
    "multi_platform_spread": 0.20,          # 3+ platforms with positive signals
    "gradual_growth_6_weeks": 0.20,         # Consistent upward slope over 6+ weeks
    "organic_community_adoption": 0.15,     # Reddit/Facebook community discussions
    "steady_google_slope": 0.15,            # Google Trends positive slope > 0.02
    "repeat_purchase_language": 0.15,       # "been using", "on my Nth", "daily routine"
    "cross_demographic_reach": 0.15,        # Pinterest demographic score > 0.6
}

INDUSTRY_SHIFT_INDICATORS = {
    "slow_steady_climb_12m": 0.25,          # Positive slope over 12+ months
    "cross_demographic_spread": 0.20,       # High demographic diversity
    "scientific_cultural_backing": 0.20,    # News trigger flags, scientific language
    "retail_without_social": 0.20,          # Strong retail signals without social spike
    "supplier_ecosystem_growth": 0.15,      # Alibaba supplier count increasing
}


def classify(signals: dict) -> dict:
    """
    Classify a product's trend pattern.

    Args:
        signals: dict with keys:
            - platforms_active: list of platform names with data
            - velocity: float (from velocity skill)
            - acceleration: float
            - projected_peak_days: int or None
            - google_trends_slope: float or None
            - creator_tier_score: float or None
            - repeat_purchase_pct: float (0-1, pct of comments with repeat language)
            - days_tracked: int
            - demographic_score: float or None
            - news_trigger: bool
            - supplier_count_change: int or None
            - social_mention_pct: float (what % of total signals come from social)
            - retail_signal_strength: float (0-1)

    Returns:
        {
            "fad_score": float (0-1),
            "lasting_score": float (0-1),
            "industry_shift_score": float (0-1),
            "dominant": str ("fad"|"lasting"|"industry_shift"),
            "confidence": float (0-1)
        }
    """
    # If Google Trends hasn't run, we cannot make a reliable fad determination.
    # Return None values to signal insufficient data.
    gt_slope = signals.get("google_trends_slope")
    if gt_slope is None:
        return {
            "fad_score": None,
            "lasting_score": None,
            "industry_shift_score": None,
            "dominant": "insufficient_data",
            "confidence": 0.0,
        }

    fad_score = _score_fad(signals)
    lasting_score = _score_lasting(signals)
    shift_score = _score_industry_shift(signals)

    # Normalize so they sum to ~1.0
    total = fad_score + lasting_score + shift_score
    if total > 0:
        fad_norm = fad_score / total
        lasting_norm = lasting_score / total
        shift_norm = shift_score / total
    else:
        fad_norm = lasting_norm = shift_norm = 0.33

    # Dominant classification
    scores = {"fad": fad_norm, "lasting": lasting_norm, "industry_shift": shift_norm}
    dominant = max(scores, key=scores.get)

    # Confidence: how much the dominant score exceeds the second-highest
    sorted_scores = sorted(scores.values(), reverse=True)
    confidence = sorted_scores[0] - sorted_scores[1] if len(sorted_scores) > 1 else sorted_scores[0]

    return {
        "fad_score": round(fad_norm, 4),
        "lasting_score": round(lasting_norm, 4),
        "industry_shift_score": round(shift_norm, 4),
        "dominant": dominant,
        "confidence": round(min(1.0, confidence + 0.3), 4),
    }


def _score_fad(s: dict) -> float:
    score = 0.0
    platforms = s.get("platforms_active", [])
    gt_slope = s.get("google_trends_slope")

    # If Google Trends has not run, skip GT-dependent fad indicators entirely.
    # Only score on non-GT signals. GT-dependent indicators return 0.
    gt_available = gt_slope is not None

    # Single platform concentration (does NOT depend on GT)
    social_pct = s.get("social_mention_pct", 0.5)
    if social_pct > 0.7 and len(platforms) <= 2:
        score += FAD_INDICATORS["single_platform_concentration"]

    # Spike without Google Trends slope — ONLY if GT has run and shows no slope
    velocity = s.get("velocity", 0)
    if gt_available and velocity > 0.3 and gt_slope < 0.01:
        score += FAD_INDICATORS["spike_without_slope"]

    # Peak under 3 weeks
    peak_days = s.get("projected_peak_days")
    if peak_days is not None and peak_days < 14:  # Tightened from 21 to 14
        score += FAD_INDICATORS["peak_under_3_weeks"]

    # Creator driven
    creator_score = s.get("creator_tier_score")
    if creator_score is not None and creator_score > 0.7:
        score += FAD_INDICATORS["creator_driven"]

    # No repeat purchase language
    repeat_pct = s.get("repeat_purchase_pct", 0.5)
    if repeat_pct < 0.1:
        score += FAD_INDICATORS["no_repeat_purchase_language"]

    # Low search confirmation — ONLY if GT has run and shows low slope
    if gt_available and gt_slope < 0.005:
        score += FAD_INDICATORS["low_search_confirmation"]

    return score


def _score_lasting(s: dict) -> float:
    score = 0.0
    platforms = s.get("platforms_active", [])

    # Multi-platform spread
    if len(platforms) >= 3:
        score += LASTING_INDICATORS["multi_platform_spread"]

    # Gradual growth over 6+ weeks
    days = s.get("days_tracked", 0)
    velocity = s.get("velocity", 0)
    if days >= 42 and 0.02 < velocity < 0.5:
        score += LASTING_INDICATORS["gradual_growth_6_weeks"]

    # Organic community adoption (Reddit/Facebook present)
    community_platforms = [p for p in platforms if p in ("reddit", "facebook")]
    if len(community_platforms) >= 1:
        score += LASTING_INDICATORS["organic_community_adoption"]

    # Steady Google slope
    gt_slope = s.get("google_trends_slope")
    if gt_slope is not None and gt_slope > 0.02:
        score += LASTING_INDICATORS["steady_google_slope"]

    # Repeat purchase language
    repeat_pct = s.get("repeat_purchase_pct", 0)
    if repeat_pct > 0.15:
        score += LASTING_INDICATORS["repeat_purchase_language"]

    # Cross-demographic reach
    demo_score = s.get("demographic_score")
    if demo_score is not None and demo_score > 0.6:
        score += LASTING_INDICATORS["cross_demographic_reach"]

    return score


def _score_industry_shift(s: dict) -> float:
    score = 0.0

    # Slow steady climb 12+ months
    days = s.get("days_tracked", 0)
    velocity = s.get("velocity", 0)
    if days >= 365 and 0.001 < velocity < 0.1:
        score += INDUSTRY_SHIFT_INDICATORS["slow_steady_climb_12m"]

    # Cross-demographic spread
    demo_score = s.get("demographic_score")
    if demo_score is not None and demo_score > 0.75:
        score += INDUSTRY_SHIFT_INDICATORS["cross_demographic_spread"]

    # Scientific/cultural backing
    if s.get("news_trigger", False):
        score += INDUSTRY_SHIFT_INDICATORS["scientific_cultural_backing"]

    # Retail without social prompt
    retail_strength = s.get("retail_signal_strength", 0)
    social_pct = s.get("social_mention_pct", 0.5)
    if retail_strength > 0.6 and social_pct < 0.3:
        score += INDUSTRY_SHIFT_INDICATORS["retail_without_social"]

    # Supplier ecosystem growth
    supplier_change = s.get("supplier_count_change")
    if supplier_change is not None and supplier_change > 5:
        score += INDUSTRY_SHIFT_INDICATORS["supplier_ecosystem_growth"]

    return score
