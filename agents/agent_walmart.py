"""
Walmart Agent — Apify-powered (epctex/walmart-scraper)

Searches Walmart for product listings, pricing, reviews, and stock status.
Writes to signals_retail.
"""
import logging
from datetime import date

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor, extract_texts, extract_dates

logger = logging.getLogger(__name__)


class WalmartAgent(BaseAgent):
    PLATFORM = "walmart"
    SIGNAL_TABLE = "signals_retail"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        query = product_name
        if keywords:
            query += " " + keywords[0]

        items = run_actor(
            actor_id=APIFY_ACTORS["walmart"],
            run_input={
                "search": query,
                "maxItems": 20,
                "proxy": {"useApifyProxy": True},
            },
            api_token=APIFY_API_TOKEN,
            timeout_secs=180,
            max_items=30,
        )

        if not items:
            raise ValueError(f"No Walmart data found for {product_name}")

        texts = extract_texts(items, ["name", "shortDescription", "description", "title"])
        for item in items:
            reviews = item.get("reviews", []) or []
            for r in reviews[:10]:
                txt = r.get("text", "") or r.get("reviewText", "") if isinstance(r, dict) else ""
                if txt:
                    texts.append(txt)

        dates = extract_dates(items, ["date", "createdAt"])

        prices = [item.get("price", 0) or item.get("currentPrice", 0) for item in items if item.get("price") or item.get("currentPrice")]
        avg_price = sum(prices) / len(prices) if prices else 0

        total_reviews = sum(item.get("reviewsCount", 0) or item.get("numberOfReviews", 0) for item in items)
        ratings = [item.get("rating", 0) or item.get("averageRating", 0) for item in items if item.get("rating") or item.get("averageRating")]
        avg_rating = sum(ratings) / len(ratings) if ratings else 0

        oos = any(
            item.get("availabilityStatus", "").lower() in ("out of stock", "not available")
            or item.get("inStock") is False
            for item in items
        )

        hist = self.supabase.table("signals_retail") \
            .select("review_count").eq("product_id", product["id"]) \
            .eq("platform", "walmart").order("scraped_date", desc=True).limit(1).execute()
        prev_reviews = hist.data[0]["review_count"] if hist.data else total_reviews
        review_growth = (total_reviews - prev_reviews) / max(prev_reviews, 1)

        return {
            "texts": texts, "data_dates": dates, "mention_count": len(items),
            "bestseller_rank": None, "rank_change_wow": 0,
            "review_count": total_reviews, "review_count_growth": round(review_growth, 4),
            "review_sentiment": round(avg_rating / 5.0, 4) if avg_rating else None,
            "search_rank": None, "out_of_stock_flag": oos,
            "price": round(avg_price, 2) if avg_price else None,
            "price_history": [{"date": date.today().isoformat(), "price": round(avg_price, 2)}] if avg_price else [],
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id, "scraped_date": date.today().isoformat(),
            "platform": "walmart",
            "bestseller_rank": raw_data.get("bestseller_rank"),
            "rank_change_wow": raw_data.get("rank_change_wow", 0),
            "review_count": raw_data.get("review_count", 0),
            "review_count_growth": raw_data.get("review_count_growth", 0),
            "review_sentiment": raw_data.get("review_sentiment"),
            "search_rank": raw_data.get("search_rank"),
            "out_of_stock_flag": raw_data.get("out_of_stock_flag", False),
            "price": raw_data.get("price"),
            "price_history": raw_data.get("price_history", []),
        }
