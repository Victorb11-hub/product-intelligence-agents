"""
Etsy Agent — Apify-powered (epctex/etsy-scraper)

Searches Etsy for handmade/artisan product listings.
Extracts reviews, pricing, handmade-vs-mass-market ratio.
Writes to signals_retail.
"""
import logging
from datetime import date

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor, extract_texts, extract_dates

logger = logging.getLogger(__name__)


class EtsyAgent(BaseAgent):
    PLATFORM = "etsy"
    SIGNAL_TABLE = "signals_retail"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        query = product_name
        if keywords:
            query += " " + keywords[0]

        items = run_actor(
            actor_id=APIFY_ACTORS["etsy"],
            run_input={
                "search": query,
                "maxItems": 30,
                "proxy": {"useApifyProxy": True},
            },
            api_token=APIFY_API_TOKEN,
            timeout_secs=180,
            max_items=50,
        )

        if not items:
            raise ValueError(f"No Etsy data found for {product_name}")

        texts = extract_texts(items, ["title", "description", "name"])
        for item in items:
            reviews = item.get("reviews", []) or []
            for r in reviews[:10]:
                txt = r.get("text", "") or r.get("review", "") if isinstance(r, dict) else ""
                if txt:
                    texts.append(txt)

        dates = extract_dates(items, ["date", "createdAt", "timestamp"])

        prices = [item.get("price", 0) or item.get("currentPrice", 0) for item in items if item.get("price") or item.get("currentPrice")]
        avg_price = sum(prices) / len(prices) if prices else 0

        total_reviews = sum(item.get("reviewsCount", 0) or item.get("numberOfReviews", 0) or item.get("reviews_count", 0) for item in items)
        ratings = [item.get("rating", 0) or item.get("averageRating", 0) for item in items if item.get("rating") or item.get("averageRating")]
        avg_rating = sum(ratings) / len(ratings) if ratings else 0

        # Handmade vs mass market — Etsy-specific
        # Heuristic: shops with <100 sales are likely handmade, >1000 likely mass market
        handmade_count = 0
        mass_count = 0
        for item in items:
            shop_sales = item.get("shopSales", 0) or item.get("totalSales", 0) or 0
            if shop_sales < 200:
                handmade_count += 1
            elif shop_sales > 2000:
                mass_count += 1
        total = handmade_count + mass_count
        handmade_ratio = handmade_count / total if total > 0 else 0.5

        oos = any(item.get("inStock") is False or item.get("quantity", 1) == 0 for item in items)

        hist = self.supabase.table("signals_retail") \
            .select("review_count").eq("product_id", product["id"]) \
            .eq("platform", "etsy").order("scraped_date", desc=True).limit(1).execute()
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
            "handmade_vs_mass_market_ratio": round(handmade_ratio, 4),
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id, "scraped_date": date.today().isoformat(),
            "platform": "etsy",
            "bestseller_rank": raw_data.get("bestseller_rank"),
            "rank_change_wow": 0,
            "review_count": raw_data.get("review_count", 0),
            "review_count_growth": raw_data.get("review_count_growth", 0),
            "review_sentiment": raw_data.get("review_sentiment"),
            "search_rank": raw_data.get("search_rank"),
            "out_of_stock_flag": raw_data.get("out_of_stock_flag", False),
            "price": raw_data.get("price"),
            "price_history": raw_data.get("price_history", []),
            "handmade_vs_mass_market_ratio": raw_data.get("handmade_vs_mass_market_ratio"),
        }
