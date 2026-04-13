"""
Universal Data Ingestion Filter
================================
Sits between every Apify actor and Supabase.
1. Filters out content older than the lookback window
2. Filters out content already in the database (dedup)
3. Logs stats and cost savings

Every agent calls get_new_items_only() after Apify returns results.
"""
import logging
from datetime import datetime, date, timedelta, timezone

logger = logging.getLogger(__name__)

# Date field names per platform
DATE_FIELDS = {
    "reddit": ["createdAt", "created_utc"],
    "tiktok": ["createTime", "createTimeISO", "createdAt"],
    "instagram": ["timestamp", "taken_at", "createdAt"],
    "youtube": ["publishedAt", "createdAt"],
    "pinterest": ["createdAt", "created_at"],
    "x": ["created_at", "createdAt"],
    "facebook": ["time", "timestamp", "createdAt"],
    "amazon": ["dateFirstAvailable", "lastReviewDate", "date"],
    "alibaba": [],  # No date filter
}

# Unique ID field per platform
ID_FIELDS = {
    "reddit": ["id"],
    "tiktok": ["id", "videoId"],
    "instagram": ["id", "shortCode"],
    "youtube": ["id", "videoId"],
    "pinterest": ["id", "pinId"],
    "x": ["id", "tweetId"],
    "facebook": ["id", "postId"],
    "amazon": ["asin", "id"],
    "alibaba": ["productId", "id"],
}

# Cost per item by platform (for savings calculation)
COST_PER_ITEM = {
    "reddit": 0.00050,
    "reddit_comments": 0.00380,
    "tiktok": 0.00300,
    "instagram": 0.00230,
    "youtube": 0.0,
    "pinterest": 0.00050,
    "x": 0.0,
    "facebook": 0.00050,
    "amazon": 0.00200,
    "alibaba": 0.00200,
}


class DataIngestionFilter:
    """Shared filter used by all platform agents."""

    def __init__(self, supabase):
        self.supabase = supabase

    def filter_by_date(self, items, platform, lookback_days):
        """Remove items older than lookback_days. Returns (kept, discarded_count)."""
        if not items or platform == "alibaba" or lookback_days <= 0:
            return items, 0

        date_keys = DATE_FIELDS.get(platform, ["createdAt"])
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        kept = []
        discarded = 0

        for item in items:
            item_date = self._parse_date(item, date_keys)
            if item_date is None:
                kept.append(item)  # No date = keep (can't filter)
                continue
            if item_date >= cutoff:
                kept.append(item)
            else:
                discarded += 1

        if discarded > 0:
            logger.info("[ingestion] %s: %d items discarded (older than %d days)", platform, discarded, lookback_days)

        return kept, discarded

    def filter_duplicates(self, items, platform, product_id):
        """Remove items already in the posts table. Returns (new_items, duplicate_count)."""
        if not items:
            return items, 0

        id_keys = ID_FIELDS.get(platform, ["id"])

        # Extract all unique IDs from incoming items
        item_ids = []
        for item in items:
            item_id = self._get_item_id(item, id_keys)
            if item_id:
                item_ids.append(item_id)

        if not item_ids:
            return items, 0  # No IDs to check

        # Batch query: which IDs already exist in posts table?
        existing_ids = set()
        try:
            # Query in batches of 50 to avoid URL length limits
            for i in range(0, len(item_ids), 50):
                batch = item_ids[i:i + 50]
                resp = self.supabase.table("posts") \
                    .select("reddit_id") \
                    .eq("product_id", product_id) \
                    .eq("platform", platform) \
                    .in_("reddit_id", batch) \
                    .execute()
                for row in (resp.data or []):
                    if row.get("reddit_id"):
                        existing_ids.add(row["reddit_id"])
        except Exception as e:
            logger.warning("[ingestion] Dedup query failed: %s — skipping dedup", str(e)[:100])
            return items, 0

        # Filter out duplicates
        new_items = []
        duplicates = 0
        for item in items:
            item_id = self._get_item_id(item, id_keys)
            if item_id and item_id in existing_ids:
                duplicates += 1
            else:
                new_items.append(item)

        if duplicates > 0:
            logger.info("[ingestion] %s: %d duplicates skipped (already in DB)", platform, duplicates)

        return new_items, duplicates

    def get_new_items_only(self, items, platform, product_id, lookback_days=30):
        """
        Combined filter: date window + dedup.
        Returns (new_items, stats_dict).
        """
        raw_count = len(items)

        # Check if this is the first run for this platform+product (no existing posts)
        is_first_run = False
        try:
            existing = self.supabase.table("posts").select("id") \
                .eq("product_id", product_id).eq("platform", platform).limit(1).execute()
            is_first_run = not existing.data
        except Exception:
            pass

        # Step 1: Date filter — skip on first run (accept all available data to establish baseline)
        if is_first_run:
            date_filtered = items
            too_old = 0
            logger.info("[ingestion] %s: First run — skipping date filter to establish baseline", platform)
        else:
            date_filtered, too_old = self.filter_by_date(items, platform, lookback_days)

        # Step 2: Dedup filter
        new_items, duplicates = self.filter_duplicates(date_filtered, platform, product_id)

        # Stats
        cost_saved = self.estimate_cost_saved(too_old + duplicates, platform)

        stats = {
            "raw_count": raw_count,
            "items_new": len(new_items),
            "items_too_old": too_old,
            "items_duplicate": duplicates,
            "cost_saved_dedup": cost_saved,
        }

        logger.info(
            "[ingestion] %s | Raw: %d | Too old: %d | Duplicates: %d | New: %d | Cost saved: $%.4f",
            platform, raw_count, too_old, duplicates, len(new_items), cost_saved,
        )

        return new_items, stats

    @staticmethod
    def estimate_cost_saved(skipped_count, platform):
        """Calculate money saved by not re-processing items."""
        cost_per = COST_PER_ITEM.get(platform, 0.001)
        return round(skipped_count * cost_per, 4)

    @staticmethod
    def _parse_date(item, date_keys):
        """Try to parse a date from an item using multiple possible field names."""
        for key in date_keys:
            val = item.get(key)
            if val is None:
                continue
            try:
                # Unix timestamp (int or float)
                if isinstance(val, (int, float)) and val > 1_000_000_000:
                    return datetime.fromtimestamp(val, tz=timezone.utc)
                # ISO string
                if isinstance(val, str) and len(val) >= 10:
                    # Handle various ISO formats
                    clean = val.replace("Z", "+00:00")
                    if "T" in clean:
                        return datetime.fromisoformat(clean)
                    else:
                        return datetime.strptime(clean[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except (ValueError, OSError):
                continue
        return None

    @staticmethod
    def _get_item_id(item, id_keys):
        """Extract the unique ID from an item."""
        for key in id_keys:
            val = item.get(key)
            if val and str(val).strip():
                return str(val).strip()
        return None
