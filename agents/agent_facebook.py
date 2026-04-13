"""
Facebook Agent — Apify-powered (apify/facebook-posts-scraper)

Searches Facebook public pages and groups for product discussions.
Extracts post text, reactions, comments, shares.
Writes to signals_social.
"""
import logging
from datetime import date

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor, extract_texts, extract_dates

logger = logging.getLogger(__name__)

# Health & wellness Facebook pages/groups to monitor
DEFAULT_FB_URLS = [
    "https://www.facebook.com/search/posts/?q={query}",
]


class FacebookAgent(BaseAgent):
    PLATFORM = "facebook"
    SIGNAL_TABLE = "signals_social"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        search_terms = [product_name] + (keywords[:2] if keywords else [])
        query = " ".join(search_terms[:3])

        items = run_actor(
            actor_id=APIFY_ACTORS["facebook"],
            run_input={
                "searchQuery": query,
                "maxPosts": 60,
                "maxPostComments": 10,
                "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
            },
            api_token=APIFY_API_TOKEN,
            timeout_secs=240,
            max_items=100,
        )

        if not items:
            raise ValueError(f"No Facebook data found for {product_name}")

        texts = extract_texts(items, ["text", "message", "postText", "description"])
        for item in items:
            comments = item.get("comments", []) or []
            if isinstance(comments, list):
                for c in comments[:5]:
                    txt = c.get("text", "") if isinstance(c, dict) else ""
                    if txt and len(txt) > 5:
                        texts.append(txt)

        dates = extract_dates(items, ["time", "timestamp", "date", "createdAt"])

        total_reactions = sum(item.get("likes", 0) or item.get("reactionsCount", 0) for item in items)
        total_comments = sum(item.get("commentsCount", 0) or item.get("comments_count", 0) for item in items)
        total_shares = sum(item.get("shares", 0) or item.get("sharesCount", 0) for item in items)

        hist = self.supabase.table("signals_social") \
            .select("mention_count").eq("product_id", product["id"]) \
            .eq("platform", "facebook").order("scraped_date", desc=True).limit(1).execute()
        prev = hist.data[0]["mention_count"] if hist.data else len(items)
        growth = (len(items) - prev) / max(prev, 1)

        return {
            "texts": texts, "data_dates": dates,
            "mention_count": len(items),
            "growth_rate_wow": round(growth, 4),
            "total_reactions": total_reactions, "total_comments": total_comments,
            "total_shares": total_shares,
            "creator_tier_score": 0.4,
            "buy_intent_comment_count": 0, "problem_language_comment_count": 0,
            "repeat_purchase_pct": 0,
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id, "scraped_date": date.today().isoformat(),
            "platform": "facebook",
            "mention_count": raw_data.get("mention_count", 0),
            "growth_rate_wow": raw_data.get("growth_rate_wow", 0),
            "velocity_score": 0, "creator_tier_score": raw_data.get("creator_tier_score", 0),
            "buy_intent_comment_count": 0, "problem_language_comment_count": 0,
            "raw_json": {"total_reactions": raw_data.get("total_reactions", 0), "total_shares": raw_data.get("total_shares", 0)},
        }
