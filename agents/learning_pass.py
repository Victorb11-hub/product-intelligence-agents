"""
Learning Pass — Adjusts council and agent weights based on sourcing outcomes.
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def run_learning_pass(db, run_id):
    """Check for new sourcing outcomes and adjust weights. Returns adjustment count."""
    adjustments = 0

    # Find sourcing outcomes not yet processed
    outcomes = db.table("sourcing_log").select("*") \
        .neq("outcome", None).execute().data

    if not outcomes:
        logger.info("[learning] No sourcing outcomes to learn from")
        return 0

    actionable = [o for o in outcomes if o.get("outcome") in ("success", "dead_stock")]
    if len(actionable) < 5:
        logger.info("[learning] Only %d actionable outcomes — need 5+ to adjust weights", len(actionable))
        return 0

    for outcome in outcomes:
        pid = outcome["product_id"]
        result = outcome["outcome"]  # success, partial, dead_stock

        if result == "partial":
            continue  # Skip partial — not enough signal

        # Find the council verdict closest to the decision date
        decision_date = outcome.get("decision_date")
        if not decision_date:
            continue

        councils = db.table("council_verdicts").select("*") \
            .eq("product_id", pid) \
            .order("verdict_date", desc=True).limit(1).execute().data

        if not councils:
            continue

        council = councils[0]
        direction = 1 if result == "success" else -1

        # Adjust each agent's weight based on whether they voted correctly
        for agent_name in ["trend_archaeologist", "demand_validator", "supply_analyst",
                           "fad_detector", "category_strategist"]:
            vote = council.get(f"{agent_name}_vote", "").lower()
            if vote == "abstain" or not vote:
                continue

            # Success: Buy votes were correct. Dead stock: Buy votes were wrong.
            if result == "success":
                correct = vote == "buy"
            else:  # dead_stock
                correct = vote != "buy"

            # Get current weight
            w_resp = db.table("council_weights").select("*") \
                .eq("agent_name", agent_name).execute()
            if not w_resp.data:
                continue

            w = w_resp.data[0]
            old_weight = w["current_weight"]
            new_weight = old_weight * (1.05 if correct else 0.95)
            new_weight = max(0.5, min(2.0, new_weight))

            total = w.get("total_decisions", 0) + 1
            correct_count = w.get("correct_decisions", 0) + (1 if correct else 0)
            accuracy = correct_count / total if total > 0 else 0

            # Update weight
            history = w.get("adjustment_history", []) or []
            history.append({
                "date": datetime.now().isoformat(),
                "product_id": pid,
                "outcome": result,
                "vote": vote,
                "correct": correct,
                "old_weight": old_weight,
                "new_weight": round(new_weight, 4),
            })

            db.table("council_weights").update({
                "current_weight": round(new_weight, 4),
                "total_decisions": total,
                "correct_decisions": correct_count,
                "accuracy_rate": round(accuracy, 4),
                "last_adjusted": datetime.now().isoformat(),
                "adjustment_history": history[-50:],  # Keep last 50
            }).eq("agent_name", agent_name).execute()

            adjustments += 1
            logger.info("[learning] %s: %s vote on %s outcome → weight %.3f → %.3f",
                        agent_name, vote, result, old_weight, new_weight)

    logger.info("[learning] Complete: %d weight adjustments", adjustments)
    return adjustments
