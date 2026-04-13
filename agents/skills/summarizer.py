"""
REASONING SKILL 1 — Plain English Summary Generation

Uses Claude API (claude-haiku) to generate 2-3 sentence summaries
of agent findings for each product. Written for a non-technical
business owner making wholesale import decisions.
"""
import os
import logging

logger = logging.getLogger(__name__)


def generate_summary(
    product_name: str,
    platform: str,
    metrics: dict,
    category: str = "",
) -> str:
    """
    Generate a plain English summary of agent findings.

    Args:
        product_name: Name of the product.
        platform: Which platform the data came from.
        metrics: Dict of key metrics from the agent run.
        category: Product category.

    Returns:
        2-3 sentence summary string.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not api_key:
        return _fallback_summary(product_name, platform, metrics)

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)

        metrics_text = "\n".join(f"- {k}: {v}" for k, v in metrics.items() if v is not None)

        prompt = f"""You are a product intelligence analyst for a wholesale health and wellness import business.

Write a 2-3 sentence summary of what was found about "{product_name}" ({category}) on {platform}.

Key metrics:
{metrics_text}

Rules:
- State what was found in concrete terms (numbers, percentages)
- Explain why it matters for a wholesale import decision
- Flag the single most important signal and the single biggest risk
- Write for a non-technical business owner, not a data scientist
- Do NOT use bullet points — write flowing sentences
- Be specific and actionable"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )

        summary = response.content[0].text.strip()
        logger.info("Generated AI summary for %s/%s", platform, product_name)
        return summary

    except Exception as e:
        logger.warning("Claude API summary failed, using fallback: %s", e)
        return _fallback_summary(product_name, platform, metrics)


def _fallback_summary(product_name: str, platform: str, metrics: dict) -> str:
    """Template-based fallback when Claude API is unavailable."""
    mention_count = metrics.get("mention_count", 0)
    sentiment = metrics.get("sentiment_score", 0)
    velocity = metrics.get("velocity", 0)
    intent_score = metrics.get("avg_intent_score", 0)
    phase = metrics.get("phase", "unknown")

    # Build summary parts
    parts = []

    # Volume and growth
    if mention_count:
        growth_word = "surging" if velocity > 0.3 else "growing" if velocity > 0.05 else "steady" if velocity > -0.05 else "declining"
        parts.append(
            f"{product_name} has {mention_count:,} mentions on {platform} "
            f"with {growth_word} momentum (velocity: {velocity:+.1%})"
        )

    # Sentiment
    if sentiment:
        sent_word = "strongly positive" if sentiment > 0.5 else "positive" if sentiment > 0.1 else "mixed" if sentiment > -0.1 else "negative"
        parts.append(f"Sentiment is {sent_word} ({sentiment:.2f})")

    # Intent
    if intent_score:
        if intent_score >= 0.5:
            parts.append(f"Buy intent is elevated at {intent_score:.2f} — consider sourcing")
        elif intent_score >= 0.3:
            parts.append(f"Moderate interest detected (intent score: {intent_score:.2f})")

    # Phase
    if phase and phase != "unknown":
        parts.append(f"Currently in {phase} phase")

    if not parts:
        return f"Limited data collected for {product_name} on {platform}. Insufficient signals for a sourcing recommendation."

    return ". ".join(parts) + "."


def generate_cross_reference_summary(
    product_name: str,
    platforms_positive: list,
    platforms_negative: list,
    cross_platform_score: float,
) -> str:
    """Generate a summary for cross-platform analysis."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not api_key:
        pos = ", ".join(platforms_positive) if platforms_positive else "none"
        neg = ", ".join(platforms_negative) if platforms_negative else "none"
        return (
            f"{product_name} shows positive signals on {pos} and negative on {neg}. "
            f"Cross-platform score: {cross_platform_score:.1f}."
        )

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            messages=[{"role": "user", "content": (
                f"Product: {product_name}\n"
                f"Positive signals on: {', '.join(platforms_positive)}\n"
                f"Negative signals on: {', '.join(platforms_negative)}\n"
                f"Cross-platform score: {cross_platform_score}\n\n"
                "Write a 1-2 sentence cross-platform analysis for a wholesale import buyer. "
                "Is there consensus? Any red flags from divergence?"
            )}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.warning("Cross-reference summary failed: %s", e)
        pos = ", ".join(platforms_positive) if platforms_positive else "none"
        return f"{product_name} has multi-platform consensus ({pos}). Score: {cross_platform_score:.1f}."
