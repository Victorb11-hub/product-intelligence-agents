"""
OPERATIONAL SKILL 2 — Self-Healing Retry System

3-strategy retry cascade:
  Strategy 1 (immediate): retry same request after 2-5s random delay
  Strategy 2 (adapted): retry with modified parameters
  Strategy 3 (fallback): use alternative data source or method

Logs which strategy succeeded so the system learns.
"""
import time
import random
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)


class RetryResult:
    """Result of a retry attempt."""
    def __init__(self, success: bool, data: Any = None, strategy_used: int = 0,
                 error: str = None, attempts: int = 0):
        self.success = success
        self.data = data
        self.strategy_used = strategy_used
        self.error = error
        self.attempts = attempts

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "strategy_used": self.strategy_used,
            "error": self.error,
            "attempts": self.attempts,
        }


def retry_with_healing(
    primary_fn: Callable,
    adapted_fn: Callable = None,
    fallback_fn: Callable = None,
    product_name: str = "unknown",
    platform: str = "unknown",
    max_strategy1_retries: int = 2,
) -> RetryResult:
    """
    Execute a function with the 3-strategy retry cascade.

    Args:
        primary_fn: Main function to call. Should return data or raise on failure.
        adapted_fn: Modified version (different params, reduced scope). Optional.
        fallback_fn: Alternative data source. Optional.
        product_name: For logging.
        platform: For logging.
        max_strategy1_retries: How many times to retry strategy 1.

    Returns:
        RetryResult with success status, data, and strategy used.
    """
    total_attempts = 0

    # Strategy 1: Immediate retry with random delay
    for attempt in range(max_strategy1_retries):
        total_attempts += 1
        try:
            data = primary_fn()
            logger.info(
                "[%s/%s] Strategy 1 succeeded on attempt %d",
                platform, product_name, attempt + 1,
            )
            return RetryResult(
                success=True, data=data, strategy_used=1, attempts=total_attempts,
            )
        except Exception as e:
            delay = random.uniform(2, 5)
            logger.warning(
                "[%s/%s] Strategy 1 attempt %d failed: %s. Retrying in %.1fs",
                platform, product_name, attempt + 1, str(e)[:100], delay,
            )
            time.sleep(delay)

    # Strategy 2: Adapted request with modified parameters
    if adapted_fn:
        total_attempts += 1
        try:
            data = adapted_fn()
            logger.info("[%s/%s] Strategy 2 (adapted) succeeded", platform, product_name)
            return RetryResult(
                success=True, data=data, strategy_used=2, attempts=total_attempts,
            )
        except Exception as e:
            logger.warning(
                "[%s/%s] Strategy 2 failed: %s", platform, product_name, str(e)[:100],
            )

    # Strategy 3: Fallback to alternative data source
    if fallback_fn:
        total_attempts += 1
        try:
            data = fallback_fn()
            logger.info("[%s/%s] Strategy 3 (fallback) succeeded", platform, product_name)
            return RetryResult(
                success=True, data=data, strategy_used=3, attempts=total_attempts,
            )
        except Exception as e:
            logger.warning(
                "[%s/%s] Strategy 3 failed: %s", platform, product_name, str(e)[:100],
            )

    # All strategies exhausted
    error_msg = f"All retry strategies exhausted for {product_name} on {platform}"
    logger.error(error_msg)
    return RetryResult(
        success=False, error=error_msg, strategy_used=0, attempts=total_attempts,
    )


class HealingTracker:
    """
    Tracks which strategies succeed most often per platform.
    Used to optimize future retry behavior.
    """

    def __init__(self):
        self.strategy_counts = {1: 0, 2: 0, 3: 0, 0: 0}  # 0 = all failed
        self.total_calls = 0

    def record(self, result: RetryResult):
        self.total_calls += 1
        self.strategy_counts[result.strategy_used] += 1

    def get_stats(self) -> dict:
        if self.total_calls == 0:
            return {"total": 0, "success_rate": 0.0}

        success = self.total_calls - self.strategy_counts[0]
        return {
            "total": self.total_calls,
            "success_rate": round(success / self.total_calls, 4),
            "strategy_1_pct": round(self.strategy_counts[1] / max(1, self.total_calls), 4),
            "strategy_2_pct": round(self.strategy_counts[2] / max(1, self.total_calls), 4),
            "strategy_3_pct": round(self.strategy_counts[3] / max(1, self.total_calls), 4),
            "failure_pct": round(self.strategy_counts[0] / max(1, self.total_calls), 4),
        }
