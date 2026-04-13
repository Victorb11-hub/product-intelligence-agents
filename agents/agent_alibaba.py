"""
Alibaba Agent — Apify-powered (epctex/alibaba-scraper)

Searches Alibaba for supplier listings, MOQ, pricing, and supplier count.
Critical for supply readiness scoring.
Writes to signals_supply.
"""
import logging
from datetime import date

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor, extract_texts, extract_dates

logger = logging.getLogger(__name__)


class AlibabaAgent(BaseAgent):
    PLATFORM = "alibaba"
    SIGNAL_TABLE = "signals_supply"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        query = product_name
        if keywords:
            query += " " + keywords[0]

        items = run_actor(
            actor_id=APIFY_ACTORS["alibaba"],
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
            raise ValueError(f"No Alibaba data found for {product_name}")

        texts = extract_texts(items, ["title", "name", "description", "productName"])
        dates = extract_dates(items, ["date", "createdAt"])

        # Extract supply metrics
        supplier_count = len(set(
            item.get("supplierName", "") or item.get("company", "") or item.get("seller", "")
            for item in items
        ))

        # MOQ extraction
        moqs = []
        for item in items:
            moq = item.get("minOrder", 0) or item.get("moq", 0) or item.get("minimumOrder", 0)
            if isinstance(moq, str):
                # Parse "100 Pieces" style
                nums = [int(s) for s in moq.split() if s.isdigit()]
                moq = nums[0] if nums else 0
            if moq and moq > 0:
                moqs.append(moq)
        avg_moq = int(sum(moqs) / len(moqs)) if moqs else None

        # Price per unit
        prices = []
        for item in items:
            price = item.get("price", 0) or item.get("priceMin", 0) or item.get("unitPrice", 0)
            if isinstance(price, str):
                price = float(''.join(c for c in price if c.isdigit() or c == '.') or '0')
            if price and price > 0:
                prices.append(price)
        avg_price = round(sum(prices) / len(prices), 2) if prices else None

        # Supplier count change from history
        hist = self.supabase.table("signals_supply") \
            .select("supplier_listing_count").eq("product_id", product["id"]) \
            .eq("platform", "alibaba").order("scraped_date", desc=True).limit(1).execute()
        prev_count = hist.data[0]["supplier_listing_count"] if hist.data else supplier_count
        supplier_change = supplier_count - prev_count

        # MOQ trend from history
        hist_moq = self.supabase.table("signals_supply") \
            .select("moq_current").eq("product_id", product["id"]) \
            .eq("platform", "alibaba").order("scraped_date", desc=True).limit(1).execute()
        prev_moq = hist_moq.data[0]["moq_current"] if hist_moq.data else avg_moq
        if prev_moq and avg_moq:
            moq_trend = "decreasing" if avg_moq < prev_moq * 0.9 else "increasing" if avg_moq > prev_moq * 1.1 else "stable"
        else:
            moq_trend = "stable"

        # New category flag — are there new suppliers in the last week?
        new_category = supplier_change > 3

        return {
            "texts": texts, "data_dates": dates, "mention_count": len(items),
            "supplier_listing_count": len(items),
            "supplier_count": supplier_count,
            "supplier_count_change": supplier_change,
            "moq_current": avg_moq,
            "moq_trend": moq_trend,
            "price_per_unit": avg_price,
            "competing_supplier_count": supplier_count,
            "new_category_flag": new_category,
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id, "scraped_date": date.today().isoformat(),
            "platform": "alibaba",
            "supplier_listing_count": raw_data.get("supplier_listing_count", 0),
            "supplier_count_change": raw_data.get("supplier_count_change", 0),
            "moq_current": raw_data.get("moq_current"),
            "moq_trend": raw_data.get("moq_trend", "stable"),
            "price_per_unit": raw_data.get("price_per_unit"),
            "competing_supplier_count": raw_data.get("competing_supplier_count", 0),
            "new_category_flag": raw_data.get("new_category_flag", False),
            "raw_json": {"supplier_count": raw_data.get("supplier_count", 0)},
        }
