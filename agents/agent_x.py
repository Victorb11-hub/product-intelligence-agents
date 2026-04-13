"""
X (Twitter) Agent — Free official X API v2

Uses the X API v2 free tier (recent search endpoint).
Free tier: 500k tweets/month read, 10k tweets/month search.
Extracts tweet text, engagement, author metrics.
Writes to signals_social.
"""
import os
import logging
import requests
from datetime import date, datetime, timedelta

from .base_agent import BaseAgent

logger = logging.getLogger(__name__)

X_SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"


class XAgent(BaseAgent):
    PLATFORM = "x"
    SIGNAL_TABLE = "signals_social"
    REQUIRED_CREDENTIALS = ["X_BEARER_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        bearer = os.environ["X_BEARER_TOKEN"]
        headers = {"Authorization": f"Bearer {bearer}"}

        # Build query — exclude retweets, require English
        search_terms = [product_name] + (keywords[:2] if keywords else [])
        query_parts = [f'"{t}"' for t in search_terms[:3]]
        query = f"({' OR '.join(query_parts)}) lang:en -is:retweet"

        # Cap at 100 per X free tier limits
        params = {
            "query": query,
            "max_results": 100,
            "tweet.fields": "created_at,public_metrics,author_id,lang",
            "expansions": "author_id",
            "user.fields": "public_metrics,verified",
        }

        self.rate_limiter.wait()
        resp = requests.get(X_SEARCH_URL, headers=headers, params=params, timeout=30)

        if resp.status_code == 429:
            self.rate_limiter.record_failure(429)
            raise ValueError("X API rate limited")
        elif resp.status_code != 200:
            raise ValueError(f"X API error {resp.status_code}: {resp.text[:200]}")

        data = resp.json()
        tweets = data.get("data", [])
        users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}

        if not tweets:
            raise ValueError(f"No X posts found for {product_name}")

        all_texts = []
        all_dates = []
        total_likes = 0
        total_retweets = 0
        total_replies = 0
        creator_scores = []

        for tweet in tweets:
            all_texts.append(tweet.get("text", ""))
            all_dates.append(tweet.get("created_at", "")[:10])

            metrics = tweet.get("public_metrics", {})
            total_likes += metrics.get("like_count", 0)
            total_retweets += metrics.get("retweet_count", 0)
            total_replies += metrics.get("reply_count", 0)

            # Creator tier from follower count
            author_id = tweet.get("author_id", "")
            user = users.get(author_id, {})
            followers = user.get("public_metrics", {}).get("followers_count", 0)
            if followers > 500_000:
                creator_scores.append(0.95)
            elif followers > 50_000:
                creator_scores.append(0.8)
            elif followers > 5_000:
                creator_scores.append(0.6)
            else:
                creator_scores.append(0.3)

        # Growth rate
        hist = self.supabase.table("signals_social") \
            .select("mention_count").eq("product_id", product["id"]) \
            .eq("platform", "x").order("scraped_date", desc=True).limit(1).execute()
        prev = hist.data[0]["mention_count"] if hist.data else len(tweets)
        growth = (len(tweets) - prev) / max(prev, 1)

        return {
            "texts": all_texts, "data_dates": all_dates,
            "mention_count": len(tweets),
            "growth_rate_wow": round(growth, 4),
            "total_likes": total_likes,
            "total_retweets": total_retweets,
            "total_replies": total_replies,
            "creator_tier_score": round(sum(creator_scores) / max(len(creator_scores), 1), 4),
            "buy_intent_comment_count": 0,
            "problem_language_comment_count": 0,
            "repeat_purchase_pct": 0,
        }

    def scrape_adapted(self, product_name: str, keywords: list, product: dict) -> dict:
        """Strategy 2: just the product name, fewer results."""
        bearer = os.environ["X_BEARER_TOKEN"]
        headers = {"Authorization": f"Bearer {bearer}"}

        params = {
            "query": f'"{product_name}" lang:en -is:retweet',
            "max_results": 10,
            "tweet.fields": "created_at,public_metrics",
        }

        self.rate_limiter.wait()
        resp = requests.get(X_SEARCH_URL, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise ValueError(f"X adapted search failed: {resp.status_code}")

        tweets = resp.json().get("data", [])
        if not tweets:
            raise ValueError("No tweets in adapted search")

        return {
            "texts": [t.get("text", "") for t in tweets],
            "data_dates": [t.get("created_at", "")[:10] for t in tweets],
            "mention_count": len(tweets), "growth_rate_wow": 0,
            "total_likes": 0, "total_retweets": 0, "total_replies": 0,
            "creator_tier_score": 0.3, "buy_intent_comment_count": 0,
            "problem_language_comment_count": 0, "repeat_purchase_pct": 0,
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id, "scraped_date": date.today().isoformat(),
            "platform": "x",
            "mention_count": raw_data.get("mention_count", 0),
            "growth_rate_wow": raw_data.get("growth_rate_wow", 0),
            "velocity_score": 0,
            "creator_tier_score": raw_data.get("creator_tier_score", 0),
            "buy_intent_comment_count": 0, "problem_language_comment_count": 0,
            "raw_json": {
                "total_likes": raw_data.get("total_likes", 0),
                "total_retweets": raw_data.get("total_retweets", 0),
                "total_replies": raw_data.get("total_replies", 0),
            },
        }
