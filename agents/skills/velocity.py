"""
INTELLIGENCE SKILL 2 — Trend Velocity Modeling

Calculates velocity (7-day rate of change), acceleration (second derivative),
projected peak using logistic growth, and phase classification.
"""
import math
import logging
from datetime import datetime, timedelta

import numpy as np

logger = logging.getLogger(__name__)


def calculate_velocity(daily_values: list[float]) -> dict:
    """
    Calculate trend velocity metrics from a time series of daily values.

    Args:
        daily_values: List of daily metric values, most recent last.
                      Minimum 7 values for meaningful results.

    Returns:
        {
            "velocity": float — rate of change over last 7 days,
            "acceleration": float — second derivative (velocity of velocity),
            "projected_peak_days": int — estimated days until peak,
            "phase": str — "emerging"|"accelerating"|"peaking"|"plateau"|"declining"
        }
    """
    if len(daily_values) < 3:
        return {
            "velocity": 0.0,
            "velocity_smoothed": 0.0,
            "acceleration": 0.0,
            "projected_peak_days": None,
            "phase": "emerging",
        }

    values = np.array(daily_values, dtype=float)

    # ── Velocity calculation ──
    # If 7+ data points: use 7-day rolling average vs previous 7-day rolling average
    # If < 7 data points: use point-to-point comparison
    if len(values) >= 14:
        # Smoothed: 7-day rolling avg (recent) vs 7-day rolling avg (previous)
        recent_avg = float(np.mean(values[-7:]))
        prev_avg = float(np.mean(values[-14:-7]))
        velocity_smoothed = (recent_avg - prev_avg) / max(prev_avg, 1)
        # Raw: last vs first of recent window
        velocity_raw = (values[-1] - values[-7]) / max(values[-7], 1)
        velocity = velocity_smoothed  # Use smoothed as primary
    elif len(values) >= 7:
        # Have 7+ but < 14: use 7-day window, no smoothing possible
        recent_avg = float(np.mean(values[-7:]))
        prev_avg = float(np.mean(values[:-7])) if len(values) > 7 else float(values[0])
        velocity_smoothed = (recent_avg - prev_avg) / max(prev_avg, 1)
        velocity_raw = (values[-1] - values[0]) / max(values[0], 1)
        velocity = velocity_smoothed
    else:
        # < 7 points: point-to-point
        if values[0] != 0:
            velocity = (values[-1] - values[0]) / values[0]
        else:
            velocity = float(values[-1]) if values[-1] != 0 else 0.0
        velocity_smoothed = velocity
        velocity_raw = velocity

    # Acceleration: change in velocity (second derivative)
    if len(values) >= 14:
        prev_window = values[-14:-7]
        if prev_window[0] != 0:
            prev_velocity = (prev_window[-1] - prev_window[0]) / prev_window[0]
        else:
            prev_velocity = 0.0
        acceleration = velocity - prev_velocity
    elif len(values) >= 6:
        half = len(values) // 2
        first_half = values[:half]
        second_half = values[half:]
        v1 = (first_half[-1] - first_half[0]) / max(first_half[0], 1)
        v2 = (second_half[-1] - second_half[0]) / max(second_half[0], 1)
        acceleration = v2 - v1
    else:
        acceleration = 0.0

    # Projected peak using logistic growth model
    projected_peak_days = _estimate_peak(values, velocity, acceleration)

    # Phase classification based on position relative to projected peak
    phase = _classify_phase(values, velocity, acceleration, projected_peak_days)

    return {
        "velocity": round(float(velocity), 4),
        "velocity_smoothed": round(float(velocity_smoothed), 4),
        "acceleration": round(float(acceleration), 4),
        "projected_peak_days": projected_peak_days,
        "phase": phase,
    }


def _estimate_peak(values: np.ndarray, velocity: float, acceleration: float) -> int | None:
    """
    Estimate days until peak. Returns None if:
    - Velocity <= 0 (already past peak or flat)
    - Less than 14 data points (insufficient history for reliable estimate)
    - Math produces unrealistic results (< 7 days)
    """
    # Never project peaks with less than 14 days of history
    if len(values) < 14:
        return None

    if velocity <= 0:
        return None

    if acceleration <= 0 and velocity > 0:
        if acceleration == 0:
            return None  # Plateau — no peak projected
        # Decelerating: estimate when velocity reaches zero
        days = abs(velocity / acceleration) * 7  # Scale to days
        if days < 7:
            return None  # Too close — unreliable
        return max(7, min(365, int(days)))

    if acceleration > 0 and velocity > 0:
        # Accelerating growth — use conservative heuristic
        # Products typically peak in 3-6x the observed growth period
        elapsed_days = len(values)
        # Conservative: assume peak in 3x elapsed time, capped at 180 days
        return max(14, min(180, int(elapsed_days * 3)))

    return None


def _classify_phase(
    values: np.ndarray, velocity: float, acceleration: float, peak_days: int | None
) -> str:
    """
    Classify the trend phase:
    - emerging: 0-20% of projected peak
    - accelerating: 20-60% of projected peak
    - peaking: 60-90% of projected peak
    - plateau: 90-100% sustained
    - declining: dropping from peak
    """
    if velocity < -0.1:
        return "declining"

    if abs(velocity) < 0.02 and abs(acceleration) < 0.01:
        # Check if we're at a high level (plateau) or low level (emerging)
        if len(values) >= 14:
            recent_mean = np.mean(values[-7:])
            overall_mean = np.mean(values)
            if recent_mean > overall_mean * 0.9:
                return "plateau"
        return "emerging"

    if peak_days is None:
        if velocity > 0:
            return "plateau"
        return "declining"

    # Estimate position on growth curve
    if peak_days > 60:
        return "emerging"
    elif peak_days > 21:
        return "accelerating"
    elif peak_days > 7:
        return "peaking"
    else:
        return "plateau"


def get_phase_from_history(historical_values: list[tuple[str, float]]) -> dict:
    """
    Convenience wrapper that accepts (date_string, value) tuples
    sorted by date ascending.
    """
    if not historical_values:
        return calculate_velocity([])
    values = [v for _, v in historical_values]
    return calculate_velocity(values)
