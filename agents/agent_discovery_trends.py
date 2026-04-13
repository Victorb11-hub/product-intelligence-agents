"""
Discovery Agent — Google Trends
Surfaces rising queries in target health/wellness categories.
Focuses on Breakout queries (>5000% growth) and cross-references with Reddit.
"""
import logging
import time
from datetime import date

from .config import get_supabase
from .skills.activity_logger import post_status
from .skills.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


def _load_settings(db):
    resp = db.table("discovery_settings").select("setting_key, setting_value").execute()
    return {r["setting_key"]: r["setting_value"] for r in (resp.data or [])}


def _calc_confidence(mention_count, growth_rate, sentiment, signal_count):
    c = (
        min(1.0, (mention_count or 0) / 100) * 0.25 +
        min(1.0, (growth_rate or 0) / 5) * 0.25 +
        min(1.0, (signal_count or 1) / 3) * 0.30 +
        (((sentiment or 0) + 1) / 2) * 0.20
    )
    return round(min(1.0, max(0, c)), 4)


class TrendsDiscoveryAgent:
    PLATFORM = "discovery_trends"

    async def run(self, run_id=None):
        post_status("discovery-trends", "busy", "Scanning Google Trends rising queries")
        start = time.time()
        db = get_supabase()
        settings = _load_settings(db)
        rl = RateLimiter("google_trends", mean_delay=7.0, rpm=10)

        seeds = [s.strip() for s in settings.get("gt_seed_categories", "supplements").split(",")]
        exclude_kw = set(k.strip().lower() for k in settings.get("exclude_keywords", "").split(","))

        existing_products = set()
        try:
            prods = db.table("products").select("name").execute()
            existing_products = set(p["name"].lower() for p in (prods.data or []))
        except Exception:
            pass

        from pytrends.request import TrendReq
        pt = TrendReq(hl="en-US", tz=360)

        candidates_found = 0
        candidates_new = 0

        for seed in seeds[:10]:
            rl.wait()
            try:
                pt.build_payload([seed], timeframe="today 1-m", geo="US")
                related = pt.related_queries()

                if seed not in related:
                    continue

                rising = related[seed].get("rising")
                if rising is None or rising.empty:
                    continue

                import time as _time
                for _, row in rising.head(20).iterrows():
                    _time.sleep(0.3)  # Rate limit DB writes
                    query = str(row.get("query", "")).strip().lower()
                    value = row.get("value", 0)

                    if not query or len(query) < 5 or len(query.split()) < 2:
                        continue
                    if query in existing_products or query in exclude_kw:
                        continue

                    is_breakout = str(value).lower() == "breakout" or (isinstance(value, (int, float)) and value > 5000)
                    growth = float(value) / 100 if isinstance(value, (int, float)) else 5.0

                    # Check if candidate already exists
                    try:
                        existing = db.table("discovery_candidates").select("id, signal_count, confidence_score") \
                            .eq("keyword", query).execute()

                        if existing.data:
                            r = existing.data[0]
                            new_signal = r["signal_count"] + (1 if r.get("source") != "google_trends" else 0)
                            new_conf = _calc_confidence(0, growth, 0, new_signal)
                            db.table("discovery_candidates").update({
                                "gt_breakout": is_breakout,
                                "gt_rising_query": True,
                                "gt_interest_score": growth,
                                "growth_rate": max(r.get("growth_rate", 0) or 0, growth),
                                "signal_count": new_signal,
                                "confidence_score": max(r["confidence_score"], new_conf),
                                "last_updated": date.today().isoformat(),
                            }).eq("id", r["id"]).execute()
                        else:
                            conf = _calc_confidence(0, growth, 0, 1)
                            db.table("discovery_candidates").insert({
                                "keyword": query,
                                "display_name": query.title(),
                                "source": "google_trends",
                                "source_detail": f"Rising query for '{seed}'",
                                "growth_rate": round(growth, 4),
                                "gt_breakout": is_breakout,
                                "gt_rising_query": True,
                                "gt_interest_score": growth,
                                "confidence_score": conf,
                                "signal_count": 1,
                                "status": "new",
                            }).execute()
                            candidates_new += 1

                        candidates_found += 1
                    except Exception as e:
                        logger.error("[discovery_trends] Write failed for '%s': %s", query, str(e)[:100])

            except Exception as e:
                logger.warning("[discovery_trends] Failed on seed '%s': %s", seed, str(e)[:100])

        duration = int(time.time() - start)
        db.table("discovery_runs").insert({
            "source": "google_trends", "candidates_found": candidates_found,
            "candidates_new": candidates_new, "runtime_seconds": duration,
        }).execute()

        post_status("discovery-trends", "done", f"Found {candidates_found} rising queries ({candidates_new} new)")
        post_status("discovery-trends", "idle", f"Last: {candidates_found} queries")
        logger.info("[discovery_trends] Found %d candidates (%d new) in %ds", candidates_found, candidates_new, duration)
        return {"candidates_found": candidates_found, "candidates_new": candidates_new}
