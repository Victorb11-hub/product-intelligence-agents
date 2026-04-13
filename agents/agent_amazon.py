"""
Amazon Agent — Apify-powered (junglee/amazon-crawler)

Searches Amazon for product listings, extracts BSR, reviews,
pricing, stock status, and review text for sentiment analysis.
Writes to signals_retail.
"""
import logging
from datetime import date

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor, extract_texts, extract_dates

logger = logging.getLogger(__name__)


class AmazonAgent(BaseAgent):
    PLATFORM = "amazon"
    SIGNAL_TABLE = "signals_retail"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        search_terms = [product_name] + (keywords[:2] if keywords else [])
        query = " ".join(search_terms[:3])

        items = run_actor(
            actor_id=APIFY_ACTORS["amazon"],
            run_input={
                "keyword": query,
                "maxItems": 20,
                "domain": "amazon.com",
                "proxy": {"useApifyProxy": True},
            },
            api_token=APIFY_API_TOKEN,
            timeout_secs=180,
            max_items=30,
        )

        if not items:
            raise ValueError(f"No Amazon data found for {product_name}")

        # Extract review texts for sentiment/intent
        texts = extract_texts(items, ["description", "title", "feature"])
        # Some actors return reviews inline
        for item in items:
            reviews = item.get("reviews", []) or []
            if isinstance(reviews, list):
                for r in reviews[:10]:
                    txt = r.get("text", "") or r.get("review", "") if isinstance(r, dict) else ""
                    if txt:
                        texts.append(txt)

        dates = extract_dates(items, ["date", "createdAt", "timestamp"])

        # Aggregate retail metrics across top listings
        prices = [item.get("price", 0) or item.get("currentPrice", 0) for item in items if item.get("price") or item.get("currentPrice")]
        avg_price = sum(prices) / len(prices) if prices else 0

        # Best seller rank — take the best (lowest) rank found
        ranks = [item.get("bestSellerRank", 0) or item.get("salesRank", 0) for item in items if item.get("bestSellerRank") or item.get("salesRank")]
        best_rank = min(ranks) if ranks else None

        total_reviews = sum(item.get("reviewsCount", 0) or item.get("reviews_count", 0) or item.get("numberOfReviews", 0) for item in items)
        avg_rating = 0
        ratings = [item.get("stars", 0) or item.get("rating", 0) for item in items if item.get("stars") or item.get("rating")]
        if ratings:
            avg_rating = sum(ratings) / len(ratings)

        # Out of stock detection
        oos = any(
            item.get("availability", "").lower() in ("out of stock", "currently unavailable")
            or item.get("inStock") is False
            for item in items
        )

        # Growth from history
        hist = self.supabase.table("signals_retail") \
            .select("review_count").eq("product_id", product["id"]) \
            .eq("platform", "amazon").order("scraped_date", desc=True).limit(1).execute()
        prev_reviews = hist.data[0]["review_count"] if hist.data else total_reviews
        review_growth = (total_reviews - prev_reviews) / max(prev_reviews, 1)

        hist_rank = self.supabase.table("signals_retail") \
            .select("bestseller_rank").eq("product_id", product["id"]) \
            .eq("platform", "amazon").order("scraped_date", desc=True).limit(1).execute()
        prev_rank = hist_rank.data[0]["bestseller_rank"] if hist_rank.data else best_rank
        rank_change = (prev_rank - best_rank) if prev_rank and best_rank else 0

        return {
            "texts": texts, "data_dates": dates,
            "mention_count": len(items),
            "bestseller_rank": best_rank,
            "rank_change_wow": rank_change,
            "review_count": total_reviews,
            "review_count_growth": round(review_growth, 4),
            "review_sentiment": round(avg_rating / 5.0, 4) if avg_rating else None,
            "search_rank": 1,
            "out_of_stock_flag": oos,
            "price": round(avg_price, 2) if avg_price else None,
            "price_history": [{"date": date.today().isoformat(), "price": round(avg_price, 2)}] if avg_price else [],
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id, "scraped_date": date.today().isoformat(),
            "platform": "amazon",
            "bestseller_rank": raw_data.get("bestseller_rank"),
            "rank_change_wow": raw_data.get("rank_change_wow", 0),
            "review_count": raw_data.get("review_count", 0),
            "review_count_growth": raw_data.get("review_count_growth", 0),
            "review_sentiment": raw_data.get("review_sentiment"),
            "search_rank": raw_data.get("search_rank"),
            "out_of_stock_flag": raw_data.get("out_of_stock_flag", False),
            "price": raw_data.get("price"),
            "price_history": raw_data.get("price_history", []),
            "raw_json": {"listings_found": raw_data.get("mention_count", 0)},
        }
