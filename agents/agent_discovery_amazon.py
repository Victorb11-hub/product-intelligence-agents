"""
Discovery Agent — Amazon BSR
Monitors Amazon bestseller lists for climbing products.
Tracks BSR changes and cross-references with Reddit/GT candidates.
"""
import logging
import re
import time
from datetime import date

from .config import get_supabase, APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor
from .skills.activity_logger import post_status

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


def _clean_product_name(raw_name):
    """Remove brand names, sizes, and counts from product names."""
    clean = re.sub(r'\b\d+[\s]*(oz|ml|mg|ct|count|pack|fl|capsule|tablet|piece)s?\b', '', raw_name, flags=re.I)
    clean = re.sub(r'\s*[-–—|,]\s*.*$', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean[:80] if clean else raw_name[:80]


class AmazonDiscoveryAgent:
    PLATFORM = "discovery_amazon"

    async def run(self, run_id=None):
        if not APIFY_API_TOKEN:
            logger.info("[discovery_amazon] Skipped — no APIFY_API_TOKEN")
            return {"candidates_found": 0}

        # Budget guard — skip if monthly Apify spend is near limit
        db = get_supabase()
        try:
            from datetime import date as d
            month_start = f"{d.today().strftime('%Y-%m')}-01"
            costs = db.table("agent_runs").select("apify_estimated_cost").gte("created_at", month_start).execute()
            total_cost = sum(r.get("apify_estimated_cost", 0) or 0 for r in (costs.data or []))
            if total_cost >= 24.0:
                logger.warning("[discovery_amazon] Skipped — monthly budget near limit ($%.2f)", total_cost)
                return {"candidates_found": 0, "status": "budget_exceeded"}
        except Exception:
            pass

        post_status("discovery-amazon", "busy", "Scanning Amazon BSR for climbing products")
        start = time.time()
        settings = _load_settings(db)

        categories = [c.strip() for c in settings.get("amazon_categories", "Health & Household").split(",")]
        exclude_kw = set(k.strip().lower() for k in settings.get("exclude_keywords", "").split(","))

        existing_products = set()
        try:
            prods = db.table("products").select("name").execute()
            existing_products = set(p["name"].lower() for p in (prods.data or []))
        except Exception:
            pass

        candidates_found = 0
        candidates_new = 0

        for category in categories[:5]:
            try:
                items = run_actor(
                    actor_id=APIFY_ACTORS.get("amazon", "junglee/amazon-crawler"),
                    run_input={"keyword": f"best sellers {category}", "maxItems": 30,
                               "domain": "amazon.com", "proxy": {"useApifyProxy": True}},
                    api_token=APIFY_API_TOKEN, timeout_secs=180, max_items=50,
                )

                for item in items:
                    raw_name = item.get("title") or item.get("name") or ""
                    if not raw_name:
                        continue

                    clean_name = _clean_product_name(raw_name).lower()
                    if len(clean_name) < 5 or clean_name in existing_products or clean_name in exclude_kw:
                        continue

                    bsr = item.get("bestSellerRank") or item.get("salesRank") or 0
                    reviews = item.get("reviewsCount") or item.get("numberOfReviews") or 0

                    try:
                        existing = db.table("discovery_candidates").select("id, signal_count, confidence_score, amazon_bsr_rank") \
                            .eq("keyword", clean_name).execute()

                        if existing.data:
                            r = existing.data[0]
                            prev_bsr = r.get("amazon_bsr_rank") or bsr
                            bsr_change = prev_bsr - bsr  # Positive = rank improved (lower number = better)
                            new_signal = r["signal_count"] + (1 if r.get("source") != "amazon" else 0)
                            growth = max(0, bsr_change / max(prev_bsr, 1))  # Clamp negative to 0
                            new_conf = _calc_confidence(reviews, growth, 0, new_signal)

                            db.table("discovery_candidates").update({
                                "amazon_bsr_rank_last_week": prev_bsr,
                                "amazon_bsr_rank": bsr,
                                "amazon_bsr_change": bsr_change,
                                "signal_count": new_signal,
                                "confidence_score": max(r["confidence_score"], new_conf),
                                "last_updated": date.today().isoformat(),
                            }).eq("id", r["id"]).execute()
                        else:
                            conf = _calc_confidence(reviews, 0, 0, 1)
                            db.table("discovery_candidates").insert({
                                "keyword": clean_name,
                                "display_name": clean_name.title(),
                                "source": "amazon",
                                "source_detail": category,
                                "amazon_bsr_rank": bsr,
                                "mention_count_this_week": reviews,
                                "confidence_score": conf,
                                "signal_count": 1,
                                "status": "new",
                            }).execute()
                            candidates_new += 1

                        candidates_found += 1
                    except Exception as e:
                        logger.error("[discovery_amazon] Write failed: %s", str(e)[:100])

            except Exception as e:
                logger.warning("[discovery_amazon] Failed on '%s': %s", category, str(e)[:100])

        duration = int(time.time() - start)
        db.table("discovery_runs").insert({
            "source": "amazon", "candidates_found": candidates_found,
            "candidates_new": candidates_new, "runtime_seconds": duration,
        }).execute()

        post_status("discovery-amazon", "done", f"Found {candidates_found} BSR candidates ({candidates_new} new)")
        post_status("discovery-amazon", "idle", f"Last: {candidates_found} candidates")
        return {"candidates_found": candidates_found, "candidates_new": candidates_new}
