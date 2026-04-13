"""
Pinterest Agent — Apify-powered (apify/pinterest-crawler)

Searches Pinterest for product-related pins and boards.
Extracts save rates, board creation, keyword volume, demographics.
Writes to signals_discovery.
"""
import logging
from datetime import date

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor, extract_texts, extract_dates

logger = logging.getLogger(__name__)


class PinterestAgent(BaseAgent):
    PLATFORM = "pinterest"
    SIGNAL_TABLE = "signals_discovery"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        search_terms = [product_name] + (keywords[:3] if keywords else [])

        all_items = []
        for term in search_terms[:4]:
            items = run_actor(
                actor_id=APIFY_ACTORS["pinterest"],
                run_input={
                    "searchQuery": term,
                    "maxItems": 30,
                    "searchType": "pin",
                    "proxy": {"useApifyProxy": True},
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=180,
                max_items=50,
            )
            all_items.extend(items)

        if not all_items:
            raise ValueError(f"No Pinterest data found for {product_name}")

        texts = extract_texts(all_items, ["description", "title", "note", "gridTitle"])
        dates = extract_dates(all_items, ["createdAt", "created_at", "date"])

        # Pin engagement metrics
        total_saves = sum(item.get("saves", 0) or item.get("saveCount", 0) or item.get("repinCount", 0) for item in all_items)
        total_comments = sum(item.get("commentCount", 0) or item.get("comments", 0) for item in all_items)

        # Estimate save rate (saves per pin)
        pin_save_rate = total_saves / max(len(all_items), 1)

        # Board creation count — unique boards
        boards = set()
        for item in all_items:
            board = item.get("boardName", "") or item.get("board", {}).get("name", "") if isinstance(item.get("board"), dict) else ""
            if board:
                boards.add(board)
        board_count = len(boards)

        # Keyword search volume proxy — total pins found
        keyword_volume = len(all_items)

        # Trending flag — if save rate is high relative to volume
        trending = pin_save_rate > 50 or keyword_volume > 40

        # Demographic score — Pinterest skews female 25-54
        # Approximate from pin categories and descriptions
        demo_keywords = ["women", "mom", "skincare", "self care", "wellness", "beauty", "home"]
        demo_matches = sum(1 for t in texts if any(d in t.lower() for d in demo_keywords))
        demographic_score = min(1.0, demo_matches / max(len(texts), 1) + 0.4)

        # Growth from history
        hist = self.supabase.table("signals_discovery") \
            .select("pin_save_rate").eq("product_id", product["id"]) \
            .eq("platform", "pinterest").order("scraped_date", desc=True).limit(1).execute()
        prev_rate = hist.data[0]["pin_save_rate"] if hist.data else pin_save_rate
        save_rate_growth = (pin_save_rate - prev_rate) / max(prev_rate, 0.01)

        return {
            "texts": texts, "data_dates": dates, "mention_count": len(all_items),
            "pin_save_rate": round(pin_save_rate, 4),
            "save_rate_growth": round(save_rate_growth, 4),
            "board_creation_count": board_count,
            "keyword_search_volume": keyword_volume,
            "trending_category_flag": trending,
            "demographic_score": round(demographic_score, 4),
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id, "scraped_date": date.today().isoformat(),
            "platform": "pinterest",
            "pin_save_rate": raw_data.get("pin_save_rate", 0),
            "save_rate_growth": raw_data.get("save_rate_growth", 0),
            "board_creation_count": raw_data.get("board_creation_count", 0),
            "keyword_search_volume": raw_data.get("keyword_search_volume", 0),
            "trending_category_flag": raw_data.get("trending_category_flag", False),
            "demographic_score": raw_data.get("demographic_score", 0),
            "raw_json": {"total_pins": raw_data.get("mention_count", 0)},
        }
