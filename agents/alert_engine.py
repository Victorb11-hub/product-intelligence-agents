"""
Alert Engine — Fires alerts based on scoring changes, council results, and system health.
All thresholds read from environment variables.
"""
import os
import logging
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

def _env(key, default):
    try: return float(os.environ.get(key, default))
    except (ValueError, TypeError): return float(default)


def run_alert_scan(db, products, run_id):
    """Scan for alert conditions and write to alerts table. Returns alert count."""
    alert_count = 0
    today = date.today().isoformat()

    for product in products:
        pid = product["id"]
        name = product["name"]
        score = product.get("current_score", 0)
        verdict = product.get("current_verdict", "watch")

        # Get previous day's score
        prev = db.table("scores_history").select("composite_score, verdict") \
            .eq("product_id", pid).order("scored_date", desc=True).limit(2).execute().data
        prev_score = prev[1]["composite_score"] if len(prev) > 1 else score
        prev_verdict = prev[1]["verdict"] if len(prev) > 1 else verdict

        # 1. Verdict change
        if verdict != prev_verdict:
            _fire(db, pid, "score_acceleration" if verdict == "buy" else "fad_warning",
                  "critical" if verdict == "buy" else "warning",
                  f"{name} verdict changed: {prev_verdict} -> {verdict} (score {prev_score} -> {score})")
            alert_count += 1

        # 2. Score movement > threshold points
        score_change_threshold = _env("ALERT_SCORE_CHANGE_THRESHOLD", 10)
        change = score - prev_score
        if abs(change) > score_change_threshold:
            _fire(db, pid, "score_acceleration" if change > 0 else "fad_warning",
                  "warning", f"{name} score moved {change:+.1f} points ({prev_score:.1f} -> {score:.1f})")
            alert_count += 1

        # 3. Score above 75 for first time
        try:
            all_scores = db.table("scores_history").select("composite_score") \
                .eq("product_id", pid).execute().data or []
        except Exception:
            all_scores = []
        buy_thresh = _env("BUY_THRESHOLD", 75)
        above_buy = [s for s in all_scores if (s.get("composite_score") or 0) >= buy_thresh]
        if score >= buy_thresh and len(above_buy) == 0:
            _fire(db, pid, "green_flag", "critical",
                  f"{name} crossed Buy threshold ({buy_thresh:.0f}) for the first time! Score: {score}")
            alert_count += 1

        # 4. Score drops below Watch threshold from above
        watch_thresh = _env("WATCH_THRESHOLD", 55)
        if score < watch_thresh and prev_score >= watch_thresh:
            _fire(db, pid, "fad_warning", "warning",
                  f"{name} dropped below Watch threshold ({watch_thresh:.0f}). Was {prev_score:.1f}, now {score:.1f}")
            alert_count += 1

        # 5. Council unanimous Buy
        council = db.table("council_verdicts").select("votes_for_buy, votes_for_watch, votes_for_pass") \
            .eq("product_id", pid).eq("run_id", run_id).execute().data
        if council:
            cv = council[0]
            if cv["votes_for_buy"] >= 4 and cv["votes_for_watch"] == 0 and cv["votes_for_pass"] == 0:
                _fire(db, pid, "green_flag", "critical",
                      f"{name}: Research Council UNANIMOUS BUY ({cv['votes_for_buy']}-0)")
                alert_count += 1

            # 6. Fad Detector overruled
            fad_vote = db.table("council_verdicts").select("fad_detector_vote") \
                .eq("product_id", pid).eq("run_id", run_id).execute().data
            if fad_vote and fad_vote[0].get("fad_detector_vote", "").lower() == "pass":
                other_buys = cv["votes_for_buy"]
                if other_buys >= 3:
                    _fire(db, pid, "green_flag", "info",
                          f"{name}: Fad Detector voted Pass but overruled by {other_buys} Buy votes")
                    alert_count += 1

    # 7. Formula recommendations
    recs = db.table("formula_recommendations").select("agent_name, recommendation_type, reasoning") \
        .eq("run_id", run_id).eq("status", "pending").execute().data
    for rec in recs:
        _fire(db, None, "new_sku", "info",
              f"Formula recommendation from {rec['agent_name']}: {rec['recommendation_type']} — {rec['reasoning'][:100]}")
        alert_count += 1

    # 8. Apify budget check
    month_start = f"{date.today().strftime('%Y-%m')}-01"
    costs = db.table("agent_runs").select("apify_estimated_cost").gte("created_at", month_start).execute().data
    total_cost = sum(r.get("apify_estimated_cost", 0) or 0 for r in costs)
    budget = _env("APIFY_MONTHLY_BUDGET", 29.0)
    warn_pct = _env("ALERT_BUDGET_WARNING_PCT", 75) / 100
    crit_pct = _env("ALERT_BUDGET_CRITICAL_PCT", 90) / 100
    if total_cost >= budget * warn_pct:
        severity = "critical" if total_cost >= budget * crit_pct else "warning"
        _fire(db, None, "fad_warning", severity,
              f"Apify monthly spend: ${total_cost:.2f} of ${budget:.2f} ({total_cost/budget*100:.0f}%)")
        alert_count += 1

    logger.info("[alerts] Fired %d alerts", alert_count)
    return alert_count


def _fire(db, product_id, alert_type, priority, message):
    try:
        # Dedup: skip if same alert type + product fired in last 24 hours
        yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
        existing_q = db.table("alerts").select("id") \
            .eq("alert_type", alert_type).gte("triggered_at", yesterday)
        if product_id:
            existing_q = existing_q.eq("product_id", product_id)
        existing = existing_q.limit(1).execute()
        if existing.data:
            logger.info("[alert] Dedup suppressed: %s", message[:80])
            return

        db.table("alerts").insert({
            "product_id": product_id,
            "alert_type": alert_type,
            "priority": priority,
            "message": message,
            "actioned": False,
        }).execute()
        logger.info("[alert] [%s] %s", priority, message)
    except Exception as e:
        logger.error("Failed to write alert: %s", e)
