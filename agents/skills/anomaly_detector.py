"""
REASONING SKILL 2 — Anomaly Detection and Self-Flagging

Compares current results against last 30 days of historical data.
Detects:
  - Sudden spike (3+ std devs above mean)
  - Sudden drop (2+ std devs below mean)
  - Sentiment reversal (0.4+ change in one week)
  - Pattern break (phase regression)
  - Velocity divergence (opposite direction to other platforms)
"""
import logging
from datetime import datetime, timedelta

import numpy as np

logger = logging.getLogger(__name__)

ANOMALY_TYPES = {
    "sudden_spike": "Current value is {std_devs:.1f} standard deviations above the 30-day mean",
    "sudden_drop": "Current value is {std_devs:.1f} standard deviations below the 30-day mean",
    "sentiment_reversal": "Sentiment changed by {delta:.2f} in one week (from {old:.2f} to {new:.2f})",
    "pattern_break": "Phase changed from {old_phase} to {new_phase} unexpectedly",
    "velocity_divergence": "Platform velocity ({direction}) diverges from cross-platform trend ({cross_direction})",
}

# Phase progression order (regression = going backward)
PHASE_ORDER = ["emerging", "accelerating", "peaking", "plateau", "declining"]


def detect_anomalies(
    current_data: dict,
    historical_data: list[dict],
    cross_platform_velocities: dict = None,
    platform: str = "unknown",
) -> list[dict]:
    """
    Detect anomalies by comparing current data against historical data.

    Args:
        current_data: Current run's signal data for one product.
        historical_data: List of past signal data dicts (last 30 days), ordered by date.
        cross_platform_velocities: {platform: velocity} for divergence detection.
        platform: Name of this platform.

    Returns:
        List of anomaly dicts:
        [{
            "anomaly_flag": True,
            "anomaly_type": str,
            "anomaly_description": str,
            "severity": str ("high"|"medium"|"low"),
            "metric": str
        }]
    """
    anomalies = []

    if len(historical_data) < 7:
        return anomalies  # Not enough history to detect anomalies

    # Check numeric metrics for spikes and drops
    numeric_metrics = [
        "mention_count", "velocity_score", "sentiment_score",
        "review_count", "bestseller_rank", "pin_save_rate",
        "supplier_listing_count", "keyword_search_volume",
    ]

    for metric in numeric_metrics:
        current_val = current_data.get(metric)
        if current_val is None:
            continue

        hist_values = [
            h[metric] for h in historical_data
            if h.get(metric) is not None
        ]

        if len(hist_values) < 7:
            continue

        arr = np.array(hist_values, dtype=float)
        mean = np.mean(arr)
        std = np.std(arr)

        if std == 0:
            continue

        z_score = (float(current_val) - mean) / std

        # Sudden spike: 3+ std devs above
        if z_score >= 3.0:
            anomalies.append({
                "anomaly_flag": True,
                "anomaly_type": "sudden_spike",
                "anomaly_description": ANOMALY_TYPES["sudden_spike"].format(std_devs=z_score),
                "severity": "high" if z_score >= 5.0 else "medium",
                "metric": metric,
            })

        # Sudden drop: 2+ std devs below
        elif z_score <= -2.0:
            anomalies.append({
                "anomaly_flag": True,
                "anomaly_type": "sudden_drop",
                "anomaly_description": ANOMALY_TYPES["sudden_drop"].format(std_devs=abs(z_score)),
                "severity": "high" if z_score <= -3.0 else "medium",
                "metric": metric,
            })

    # Sentiment reversal check
    current_sentiment = current_data.get("sentiment_score")
    if current_sentiment is not None and len(historical_data) >= 7:
        week_ago_sentiments = [
            h["sentiment_score"] for h in historical_data[-7:]
            if h.get("sentiment_score") is not None
        ]
        if week_ago_sentiments:
            old_sentiment = np.mean(week_ago_sentiments)
            delta = abs(float(current_sentiment) - old_sentiment)
            if delta >= 0.4:
                anomalies.append({
                    "anomaly_flag": True,
                    "anomaly_type": "sentiment_reversal",
                    "anomaly_description": ANOMALY_TYPES["sentiment_reversal"].format(
                        delta=delta, old=old_sentiment, new=current_sentiment,
                    ),
                    "severity": "high",
                    "metric": "sentiment_score",
                })

    # Pattern break: unexpected phase regression
    current_phase = current_data.get("phase")
    if current_phase and len(historical_data) >= 3:
        last_phases = [h.get("phase") for h in historical_data[-3:] if h.get("phase")]
        if last_phases:
            prev_phase = last_phases[-1]
            if prev_phase in PHASE_ORDER and current_phase in PHASE_ORDER:
                prev_idx = PHASE_ORDER.index(prev_phase)
                curr_idx = PHASE_ORDER.index(current_phase)
                # Skip of 2+ phases backward (e.g., accelerating → declining)
                if prev_idx - curr_idx >= 2 or (prev_idx <= 1 and curr_idx >= 4):
                    anomalies.append({
                        "anomaly_flag": True,
                        "anomaly_type": "pattern_break",
                        "anomaly_description": ANOMALY_TYPES["pattern_break"].format(
                            old_phase=prev_phase, new_phase=current_phase,
                        ),
                        "severity": "high",
                        "metric": "phase",
                    })

    # Velocity divergence from other platforms
    if cross_platform_velocities and len(cross_platform_velocities) >= 2:
        current_velocity = current_data.get("velocity", current_data.get("velocity_score", 0))
        if current_velocity is not None:
            other_velocities = [
                v for p, v in cross_platform_velocities.items()
                if p != platform and v is not None
            ]
            if other_velocities:
                avg_other = np.mean(other_velocities)
                # Check if moving in opposite direction
                if (current_velocity > 0.1 and avg_other < -0.1) or \
                   (current_velocity < -0.1 and avg_other > 0.1):
                    direction = "positive" if current_velocity > 0 else "negative"
                    cross_direction = "positive" if avg_other > 0 else "negative"
                    anomalies.append({
                        "anomaly_flag": True,
                        "anomaly_type": "velocity_divergence",
                        "anomaly_description": ANOMALY_TYPES["velocity_divergence"].format(
                            direction=direction, cross_direction=cross_direction,
                        ),
                        "severity": "medium",
                        "metric": "velocity",
                    })

    return anomalies


def create_alert_from_anomaly(anomaly: dict, product_id: str, platform: str) -> dict:
    """Convert an anomaly detection into an alert row for the alerts table."""
    type_to_alert = {
        "sudden_spike": "score_acceleration",
        "sudden_drop": "fad_warning",
        "sentiment_reversal": "reddit_pushback" if platform == "reddit" else "fad_warning",
        "pattern_break": "fad_warning",
        "velocity_divergence": "fad_warning",
    }

    return {
        "product_id": product_id,
        "alert_type": type_to_alert.get(anomaly["anomaly_type"], "fad_warning"),
        "priority": anomaly["severity"],
        "message": f"[{platform.upper()}] {anomaly['anomaly_description']}",
        "actioned": False,
    }
