"""
Instagram Agent — apify/instagram-hashtag-scraper ($2.30/1K)
Searches Instagram by hashtag. Extracts captions, engagement,
comments for intent scoring. Feeds Job 1 — Early Detection (30% weight).
"""
import logging
from datetime import date

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor, extract_texts, extract_dates

logger = logging.getLogger(__name__)


class InstagramAgent(BaseAgent):
    PLATFORM = "instagram"
    SIGNAL_TABLE = "signals_social"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        # Build hashtag list (no spaces, no special chars)
        all_terms = [product_name] + (keywords or [])
        hashtags = list(set(
            t.lower().replace(" ", "").replace("-", "").replace("&", "")
            for t in all_terms[:5] if t
        ))

        logger.info("[instagram] Searching hashtags: %s", hashtags)

        try:
            items = run_actor(
                actor_id=APIFY_ACTORS["instagram"],
                run_input={
                    "hashtags": hashtags,
                    "resultsLimit": 150,
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=240,
                max_items=200,
            )
        except Exception as e:
            raise ValueError(f"Instagram scrape failed: {str(e)[:200]}")

        # Filter out error items
        items = [i for i in items if not i.get("error")]

        if not items:
            raise ValueError(f"No Instagram data found for {product_name}")

        # Data ingestion filter
        from .skills.data_ingestion import DataIngestionFilter
        ingestion = DataIngestionFilter(self.supabase)
        items, ingestion_stats = ingestion.get_new_items_only(
            items, "instagram", product["id"], lookback_days=14
        )
        self.ingestion_stats = ingestion_stats

        if not items:
            raise ValueError(f"All Instagram items were old or duplicates for {product_name}")

        # Deduplicate by post ID
        seen = set()
        unique = []
        for item in items:
            pid = item.get("id") or item.get("shortCode") or ""
            if pid and pid in seen:
                continue
            seen.add(pid)
            unique.append(item)

        # Extract texts from captions + comments
        texts = []
        for item in unique:
            caption = (item.get("caption") or "").strip()
            if caption:
                texts.append(caption)
            # Extract latest comments for intent scoring
            comments = item.get("latestComments") or []
            if isinstance(comments, list):
                for c in comments[:5]:
                    if isinstance(c, dict):
                        ct = (c.get("text") or "").strip()
                        if ct and len(ct) > 5:
                            texts.append(ct)
                    elif isinstance(c, str) and len(c) > 5:
                        texts.append(c)

        dates = extract_dates(unique, ["timestamp", "taken_at", "createdAt"])

        # Engagement metrics
        total_likes = sum(item.get("likesCount", 0) or 0 for item in unique)
        total_comments = sum(item.get("commentsCount", 0) or 0 for item in unique)

        # Reel weighting: reels get 1.2x engagement weight
        reel_count = sum(1 for item in unique if item.get("type") in ("video", "reel", "clips"))
        photo_count = len(unique) - reel_count

        # No ownerFollowers from this actor — set neutral creator tier
        avg_creator = 0.5

        # Growth rate
        hist = self.supabase.table("signals_social") \
            .select("mention_count").eq("product_id", product["id"]) \
            .eq("platform", "instagram").order("scraped_date", desc=True).limit(1).execute()
        prev = hist.data[0]["mention_count"] if hist.data else len(unique)
        growth = (len(unique) - prev) / max(prev, 1)

        return {
            "texts": texts,
            "raw_items": unique,
            "data_dates": dates,
            "mention_count": len(unique),
            "growth_rate_wow": round(growth, 4),
            "total_likes": total_likes,
            "total_comments": total_comments,
            "reel_count": reel_count,
            "photo_count": photo_count,
            "creator_tier_score": avg_creator,
            "buy_intent_comment_count": 0,
            "problem_language_comment_count": 0,
            "repeat_purchase_pct": 0,
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": "instagram",
            "mention_count": raw_data.get("mention_count", 0),
            "growth_rate_wow": raw_data.get("growth_rate_wow", 0),
            "velocity_score": 0,
            "creator_tier_score": raw_data.get("creator_tier_score", 0.5),
            "buy_intent_comment_count": raw_data.get("buy_intent_comment_count", 0),
            "problem_language_comment_count": raw_data.get("problem_language_comment_count", 0),
            "total_upvotes": raw_data.get("total_likes", 0),
            "total_comment_count": raw_data.get("total_comments", 0),
            "sample_size": raw_data.get("mention_count", 0),
        }
