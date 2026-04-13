"""
REASONING SKILL 3 — Learning from Outcomes

Adjusts agent signal weights based on sourcing outcomes.
- Success: increase elevated signal weights by 5%
- Dead stock: decrease elevated signal weights by 5%
- Weights bounded between 50% and 200% of base weight
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

WEIGHT_ADJUSTMENT = 0.05  # 5% per outcome
MIN_WEIGHT_RATIO = 0.5    # 50% of base weight
MAX_WEIGHT_RATIO = 2.0    # 200% of base weight


async def load_weights(supabase, agent: str) -> dict:
    """
    Load learned weights for an agent from agent_weights table.

    Returns:
        dict mapping signal_name -> learned_weight
    """
    try:
        resp = supabase.table("agent_weights") \
            .select("signal_name, base_weight, learned_weight") \
            .eq("agent", agent) \
            .execute()

        weights = {}
        for row in resp.data:
            weights[row["signal_name"]] = {
                "base": row["base_weight"],
                "learned": row["learned_weight"],
            }
        return weights

    except Exception as e:
        logger.error("Failed to load weights for %s: %s", agent, e)
        return {}


def apply_weights(raw_scores: dict, weights: dict) -> dict:
    """
    Apply learned weights to raw signal scores.

    Args:
        raw_scores: dict of {signal_name: raw_value}
        weights: dict from load_weights()

    Returns:
        dict of {signal_name: weighted_value}
    """
    weighted = {}
    for signal, value in raw_scores.items():
        if value is None:
            weighted[signal] = None
            continue

        w = weights.get(signal, {}).get("learned", 1.0)
        weighted[signal] = value * w

    return weighted


async def update_weights_from_outcome(
    supabase,
    product_id: str,
    outcome: str,
    signals_at_decision: dict,
) -> dict:
    """
    Update agent weights based on a sourcing outcome.

    Args:
        supabase: Supabase client.
        product_id: Product that had the outcome.
        outcome: "success", "partial", or "dead_stock".
        signals_at_decision: Dict of {agent: {signal_name: was_elevated}}
            where was_elevated is True if the signal was above average at decision time.

    Returns:
        dict of adjustments made.
    """
    if outcome == "partial":
        logger.info("Partial outcome — no weight adjustment")
        return {}

    direction = 1 if outcome == "success" else -1
    adjustments = {}

    for agent, signals in signals_at_decision.items():
        for signal_name, was_elevated in signals.items():
            if not was_elevated:
                continue

            try:
                # Read current weight
                resp = supabase.table("agent_weights") \
                    .select("id, base_weight, learned_weight, adjustment_count") \
                    .eq("agent", agent) \
                    .eq("signal_name", signal_name) \
                    .execute()

                if not resp.data:
                    continue

                row = resp.data[0]
                base = row["base_weight"]
                current = row["learned_weight"]
                count = row["adjustment_count"]

                # Apply adjustment
                new_weight = current + (base * WEIGHT_ADJUSTMENT * direction)

                # Bound between 50% and 200% of base
                new_weight = max(base * MIN_WEIGHT_RATIO, min(base * MAX_WEIGHT_RATIO, new_weight))

                # Update
                supabase.table("agent_weights") \
                    .update({
                        "learned_weight": round(new_weight, 4),
                        "adjustment_count": count + 1,
                        "last_updated": datetime.now().isoformat(),
                    }) \
                    .eq("id", row["id"]) \
                    .execute()

                adjustments[f"{agent}/{signal_name}"] = {
                    "old": current,
                    "new": round(new_weight, 4),
                    "direction": "up" if direction > 0 else "down",
                }

                logger.info(
                    "Weight adjusted: %s/%s %.3f → %.3f (%s)",
                    agent, signal_name, current, new_weight,
                    "success" if direction > 0 else "dead_stock",
                )

            except Exception as e:
                logger.error("Failed to update weight %s/%s: %s", agent, signal_name, e)

    return adjustments


async def reset_all_weights(supabase) -> int:
    """Reset all learned weights back to base weights. Returns count of rows reset."""
    try:
        resp = supabase.table("agent_weights") \
            .select("id, base_weight") \
            .execute()

        count = 0
        for row in resp.data:
            supabase.table("agent_weights") \
                .update({
                    "learned_weight": row["base_weight"],
                    "adjustment_count": 0,
                    "last_updated": datetime.now().isoformat(),
                }) \
                .eq("id", row["id"]) \
                .execute()
            count += 1

        logger.info("Reset %d agent weights to base values", count)
        return count

    except Exception as e:
        logger.error("Failed to reset weights: %s", e)
        return 0
