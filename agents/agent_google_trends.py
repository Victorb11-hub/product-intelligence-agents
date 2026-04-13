"""
Google Trends Agent — PyTrends (free, no API key)
==================================================
Collects:
  1. 24-month interest over time → slope direction, YoY growth
  2. Related rising queries → breakout detection
  3. Seasonal pattern detection
  4. Category comparison (product vs generic terms)
  5. News trigger detection (single spike vs organic)

Writes to signals_search table.
"""
import logging
from datetime import date, datetime

import numpy as np

from .config import get_supabase, RATE_LIMITS
from .skills.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class GoogleTrendsAgent:
    PLATFORM = "google_trends"

    def __init__(self):
        self.supabase = get_supabase()
        self.run_id = None

    async def run(self, products: list[dict], run_id: str) -> dict:
        from .skills.activity_logger import post_status

        self.run_id = run_id
        rl = RateLimiter("google_trends", mean_delay=7.0, rpm=10)

        post_status("scraper-google-trends", "busy", f"Starting Google Trends for {len(products)} products")

        results = {}
        for product in products:
            try:
                post_status("scraper-google-trends", "busy", f"Analyzing: {product['name']}")
                result = self._process_product(product, rl)
                results[product["name"]] = result
            except Exception as e:
                logger.error("[google_trends] Error on %s: %s", product["name"], e)
                results[product["name"]] = {"error": str(e)[:200]}

        written = sum(1 for v in results.values() if "error" not in v)
        post_status("scraper-google-trends", "done" if written > 0 else "reporting", f"Done. {written}/{len(products)} products written.")
        post_status("scraper-google-trends", "idle", f"Google Trends idle. Last run: {written} products.")

        return {
            "status": "complete" if written > 0 else "failed",
            "products_processed": len(products),
            "rows_written": written,
            "rows_rejected": 0,
            "anomalies_detected": 0,
            "integrity_errors": [],
            "summary": f"Google Trends processed {len(products)} products, {written} written.",
        }

    def _process_product(self, product: dict, rl: RateLimiter) -> dict:
        from pytrends.request import TrendReq

        product_id = product["id"]
        product_name = product["name"]
        keywords = product.get("keywords", [product_name])
        primary_kw = keywords[0] if keywords else product_name

        logger.info("[google_trends] Processing: %s (keyword: %s)", product_name, primary_kw)

        # retries/backoff_factor removed: pytrends 4.9.x is incompatible
        # with urllib3 v2 (method_whitelist renamed to allowed_methods).
        # We handle retries ourselves via the rate limiter.
        pytrends = TrendReq(hl="en-US", tz=360)

        # ── 1. Interest over time (24 months) ──
        rl.wait()
        pytrends.build_payload([primary_kw], cat=0, timeframe="today 5-y", geo="US")
        interest_df = pytrends.interest_over_time()

        if interest_df.empty:
            raise ValueError(f"No Google Trends data for '{primary_kw}'")

        values = interest_df[primary_kw].tolist()
        dates = [d.strftime("%Y-%m-%d") for d in interest_df.index]

        # Slope calculation (linear regression)
        x = np.arange(len(values))
        slope_raw = float(np.polyfit(x, values, 1)[0]) if len(values) > 1 else 0
        mean_val = float(np.mean(values)) if values else 1
        slope_24m = round(slope_raw / max(mean_val, 1), 6)

        # Direction
        if slope_24m > 0.005:
            direction = "rising"
        elif slope_24m < -0.005:
            direction = "declining"
        else:
            direction = "flat"

        # Year over year growth
        yoy_growth = 0.0
        if len(values) >= 52:
            recent = float(np.mean(values[-4:]))
            year_ago = float(np.mean(values[-56:-52])) if len(values) >= 56 else float(np.mean(values[:4]))
            if year_ago > 0:
                yoy_growth = round((recent - year_ago) / year_ago, 4)

        # Breakout / fad detection
        median_val = float(np.median(values))
        latest_val = values[-1] if values else 0
        is_spike = latest_val > median_val * 2.5 and median_val > 0

        # Fad flag: spike with no sustained slope = true fad
        breakout_flag = is_spike and slope_24m < 0.002

        logger.info(
            "[google_trends] %s: slope=%.6f (%s), YoY=%.2f%%, breakout=%s",
            product_name, slope_24m, direction, yoy_growth * 100, breakout_flag,
        )

        # ── 2. Related rising queries ──
        rl.wait()
        try:
            pytrends.build_payload([primary_kw], cat=0, timeframe="today 12-m", geo="US")
            related = pytrends.related_queries()
            rising_queries = []
            breakout_queries = []
            if primary_kw in related and related[primary_kw].get("rising") is not None:
                rising_df = related[primary_kw]["rising"]
                if not rising_df.empty:
                    for _, row in rising_df.head(10).iterrows():
                        q = row.get("query", "")
                        val = row.get("value", 0)
                        rising_queries.append(q)
                        if str(val).lower() == "breakout" or (isinstance(val, (int, float)) and val > 5000):
                            breakout_queries.append(q)
        except Exception as e:
            logger.warning("[google_trends] Related queries failed: %s", e)
            rising_queries = []
            breakout_queries = []

        # ── 3. Seasonal pattern detection ──
        seasonal_pattern, seasonal_months = self._detect_seasonality(values)

        # ── 4. Category comparison ──
        top_format = self._compare_formats(pytrends, primary_kw, rl)

        # ── 5. News trigger detection ──
        news_trigger = self._detect_news_trigger(values)

        # ── Write to signals_search ──
        signal_row = {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": "google_trends",
            "data_confidence": 0.9,
            "run_id": self.run_id,
            "slope_24m": slope_24m,
            "breakout_flag": breakout_flag,
            "yoy_growth": yoy_growth,
            "seasonal_pattern": seasonal_pattern,
            "related_rising_queries": rising_queries[:5],
            "news_trigger_flag": news_trigger,
            "data_quality_score": 0.95,
        }

        self.supabase.table("signals_search").insert(signal_row).execute()

        logger.info("[google_trends] Written to signals_search for %s", product_name)

        return {
            "slope_24m": slope_24m,
            "direction": direction,
            "yoy_growth": yoy_growth,
            "breakout_flag": breakout_flag,
            "rising_queries": rising_queries[:5],
            "breakout_queries": breakout_queries,
            "seasonal_pattern": seasonal_pattern,
            "seasonal_months": seasonal_months,
            "top_format": top_format,
            "news_trigger": news_trigger,
            "latest_value": latest_val,
            "median_value": median_val,
            "data_points": len(values),
        }

    def _detect_seasonality(self, values: list) -> tuple[str, list]:
        """Detect seasonal patterns from weekly data."""
        if len(values) < 52:
            return "insufficient_data", []

        arr = np.array(values, dtype=float)

        # Monthly averages (approximate: 4.3 weeks per month)
        monthly = []
        for i in range(0, len(arr) - 3, 4):
            monthly.append(float(np.mean(arr[i:i + 4])))

        if len(monthly) < 12:
            return "unknown", []

        # Find which months are consistently above average
        overall_mean = np.mean(monthly)
        month_avgs = [0.0] * 12

        for i, val in enumerate(monthly):
            month_avgs[i % 12] += val

        # Normalize
        counts = [0] * 12
        for i in range(len(monthly)):
            counts[i % 12] += 1
        for i in range(12):
            if counts[i] > 0:
                month_avgs[i] /= counts[i]

        # Find peak months (>20% above average)
        peak_months = [i + 1 for i in range(12) if month_avgs[i] > overall_mean * 1.2]

        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        if not peak_months:
            return "steady", []
        elif set(peak_months) & {1, 2}:
            return "new_year_spike", [month_names[m - 1] for m in peak_months]
        elif set(peak_months) & {11, 12}:
            return "holiday_gift", [month_names[m - 1] for m in peak_months]
        elif set(peak_months) & {6, 7, 8}:
            return "summer_peak", [month_names[m - 1] for m in peak_months]
        else:
            return "variable", [month_names[m - 1] for m in peak_months]

    def _compare_formats(self, pytrends, primary_kw: str, rl: RateLimiter) -> str:
        """Compare product keyword against generic category terms."""
        # Build comparison keywords
        words = primary_kw.lower().split()
        generic_terms = []

        # Try dropping qualifiers to get broader terms
        if len(words) >= 3:
            generic_terms.append(" ".join(words[1:]))  # Drop first word
            generic_terms.append(" ".join(words[-2:]))  # Last 2 words
        elif len(words) == 2:
            generic_terms.append(words[-1])  # Last word only

        if not generic_terms:
            return primary_kw

        compare_list = [primary_kw] + generic_terms[:2]

        rl.wait()
        try:
            pytrends.build_payload(compare_list[:5], timeframe="today 3-m", geo="US")
            comparison_df = pytrends.interest_over_time()

            if comparison_df.empty:
                return primary_kw

            # Find which keyword has highest average interest
            avgs = {}
            for col in compare_list:
                if col in comparison_df.columns:
                    avgs[col] = float(comparison_df[col].mean())

            if avgs:
                winner = max(avgs, key=avgs.get)
                logger.info("[google_trends] Format comparison: %s", avgs)
                return winner
        except Exception as e:
            logger.warning("[google_trends] Format comparison failed: %s", e)

        return primary_kw

    def _detect_news_trigger(self, values: list) -> bool:
        """Detect if a spike is caused by a single news event vs organic growth."""
        if len(values) < 10:
            return False

        arr = np.array(values[-12:], dtype=float)
        mean = float(np.mean(arr))
        std = float(np.std(arr))

        if std == 0:
            return False

        # News trigger = one single data point is >3 std devs above mean
        # while surrounding points are near mean
        max_val = float(np.max(arr))
        max_idx = int(np.argmax(arr))

        if max_val > mean + 3 * std:
            # Check if surrounding points are near mean (single spike, not sustained)
            neighbors = []
            if max_idx > 0:
                neighbors.append(arr[max_idx - 1])
            if max_idx < len(arr) - 1:
                neighbors.append(arr[max_idx + 1])

            if neighbors and np.mean(neighbors) < mean + std:
                return True  # Single spike = news trigger

        return False
