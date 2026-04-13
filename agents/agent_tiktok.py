"""
TikTok Agent — clockworks/tiktok-scraper ($3.00/1K)
Searches TikTok by keyword and hashtag. Extracts video engagement,
creator tiers, view velocity, and comment text for intent scoring.
Feeds Job 1 — Early Detection (40% weight).
"""
import logging
from datetime import date, datetime, timezone

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor, extract_texts, extract_dates

logger = logging.getLogger(__name__)

CREATOR_TIERS = [
    (1_000_000, 0.90, "mega"),
    (100_000,   0.75, "macro"),
    (10_000,    0.50, "micro"),
    (0,         0.20, "nano"),
]


class TikTokAgent(BaseAgent):
    PLATFORM = "tiktok"
    SIGNAL_TABLE = "signals_social"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        all_terms = [product_name] + (keywords or [])
        # Build hashtag versions (no spaces, no special chars)
        hashtags = list(set(
            t.lower().replace(" ", "").replace("-", "").replace("&", "")
            for t in all_terms[:7] if t and len(t) > 3
        ))

        logger.info("[tiktok] Searching %d hashtags: %s", len(hashtags), hashtags)

        all_items = []
        try:
            items = run_actor(
                actor_id=APIFY_ACTORS["tiktok"],
                run_input={
                    "hashtags": hashtags,
                    "resultsPerPage": 50,
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=240,
                max_items=200,
            )
            all_items.extend(items)
        except Exception as e:
            logger.warning("[tiktok] Hashtag search failed: %s", str(e)[:200])

        if not all_items:
            raise ValueError(f"No TikTok data found for {product_name}")

        # Data ingestion filter: remove old + duplicate items
        from .skills.data_ingestion import DataIngestionFilter
        ingestion = DataIngestionFilter(self.supabase)
        all_items, ingestion_stats = ingestion.get_new_items_only(
            all_items, "tiktok", product["id"], lookback_days=14
        )
        self.ingestion_stats = ingestion_stats if hasattr(self, 'ingestion_stats') else ingestion_stats

        if not all_items:
            raise ValueError(f"All TikTok items were old or duplicates for {product_name}")

        # Deduplicate by video ID
        seen = set()
        unique = []
        for item in all_items:
            vid = item.get("id", "")
            if vid and vid in seen:
                continue
            seen.add(vid)
            unique.append(item)

        # Extract texts for sentiment + intent
        texts = []
        for item in unique:
            text = (item.get("text") or item.get("desc") or item.get("description") or "").strip()
            if text:
                texts.append(text)

        dates = [item.get("createTimeISO") or item.get("createdAt") or "" for item in unique]

        # Engagement metrics
        total_views = sum(item.get("playCount", 0) or 0 for item in unique)
        total_likes = sum(item.get("diggCount", 0) or 0 for item in unique)
        total_comments = sum(item.get("commentCount", 0) or 0 for item in unique)
        total_shares = sum(item.get("shareCount", 0) or 0 for item in unique)

        # View velocity: views per hour since creation
        velocities = []
        now = datetime.now(timezone.utc)
        for item in unique:
            ct = item.get("createTime")
            views = item.get("playCount", 0) or 0
            if ct and isinstance(ct, (int, float)) and ct > 1_000_000_000 and views > 0:
                created = datetime.fromtimestamp(ct, tz=timezone.utc)
                hours = max(1, (now - created).total_seconds() / 3600)
                velocities.append(views / hours)

        avg_velocity = sum(velocities) / max(len(velocities), 1) if velocities else 0

        # Creator tier scoring
        creator_scores = []
        tier_dist = {"nano": 0, "micro": 0, "macro": 0, "mega": 0}
        for item in unique:
            author = item.get("authorMeta") or item.get("author") or {}
            if isinstance(author, dict):
                fans = author.get("fans", 0) or author.get("followers", 0) or 0
            else:
                fans = 0
            for threshold, score, tier_name in CREATOR_TIERS:
                if fans >= threshold:
                    creator_scores.append(score)
                    tier_dist[tier_name] += 1
                    break

        avg_creator = sum(creator_scores) / max(len(creator_scores), 1) if creator_scores else 0.3

        # Growth rate
        hist = self.supabase.table("signals_social") \
            .select("mention_count").eq("product_id", product["id"]) \
            .eq("platform", "tiktok").order("scraped_date", desc=True).limit(1).execute()
        prev = hist.data[0]["mention_count"] if hist.data else len(unique)
        growth = (len(unique) - prev) / max(prev, 1)

        return {
            "texts": texts,
            "raw_items": unique,
            "data_dates": dates,
            "mention_count": len(unique),
            "growth_rate_wow": round(growth, 4),
            "total_views": total_views,
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_shares": total_shares,
            "avg_view_velocity": round(avg_velocity, 2),
            "creator_tier_score": round(avg_creator, 4),
            "creator_tier_distribution": tier_dist,
            "buy_intent_comment_count": 0,
            "problem_language_comment_count": 0,
            "repeat_purchase_pct": 0,
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": "tiktok",
            "mention_count": raw_data.get("mention_count", 0),
            "growth_rate_wow": raw_data.get("growth_rate_wow", 0),
            "velocity_score": 0,
            "creator_tier_score": raw_data.get("creator_tier_score", 0),
            "buy_intent_comment_count": raw_data.get("buy_intent_comment_count", 0),
            "problem_language_comment_count": raw_data.get("problem_language_comment_count", 0),
            "total_upvotes": raw_data.get("total_likes", 0),
            "total_comment_count": raw_data.get("total_comments", 0),
            "total_views": raw_data.get("total_views", 0),
            "sample_size": raw_data.get("mention_count", 0),
        }
