"""
Shared configuration for all agents.
Reads Supabase credentials and API keys from environment variables.
"""
import os
from postgrest import SyncPostgrestClient

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", os.environ.get("SUPABASE_ANON_KEY", ""))
if not SUPABASE_URL:
    raise EnvironmentError("SUPABASE_URL must be set in environment variables (agents/.env)")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Apify — shared across 9 platform agents
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")

# Free API credentials (3 platforms)
X_BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN", "")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
# PyTrends needs no key

# Apify actor IDs — one per platform
APIFY_ACTORS = {
    "reddit":    "macrocosmos/reddit-scraper",
    "tiktok":    "clockworks/tiktok-scraper",
    "instagram": "apify/instagram-hashtag-scraper",
    "facebook":  "apify/facebook-posts-scraper",
    "amazon":    "junglee/amazon-crawler",
    "walmart":   "epctex/walmart-scraper",
    "etsy":      "epctex/etsy-scraper",
    "alibaba":   "epctex/alibaba-scraper",
    "pinterest": "apify/pinterest-crawler",
}

class SupabaseClient:
    """Lightweight Supabase client using postgrest directly.
    Avoids the full supabase package which has heavy dependencies (pyiceberg)
    that don't build on Python 3.14 yet."""

    def __init__(self, url: str, key: str):
        self.url = url
        self.key = key
        self.rest_url = f"{url}/rest/v1"
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        self._postgrest = SyncPostgrestClient(
            self.rest_url,
            headers=self.headers,
        )

    def table(self, name: str):
        return self._postgrest.from_(name)

def get_supabase():
    """Create and return a Supabase client."""
    return SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

# Platform rate limit configs
# For Apify agents these control delay between product searches, not HTTP requests
RATE_LIMITS = {
    "reddit":        {"rpm": 60,  "safe_pct": 0.8, "mean_delay": 1.0},
    "tiktok":        {"rpm": 30,  "safe_pct": 0.8, "mean_delay": 2.0},
    "instagram":     {"rpm": 30,  "safe_pct": 0.8, "mean_delay": 2.0},
    "x":             {"rpm": 15,  "safe_pct": 0.8, "mean_delay": 5.0},
    "facebook":      {"rpm": 30,  "safe_pct": 0.8, "mean_delay": 2.0},
    "youtube":       {"rpm": 100, "safe_pct": 0.8, "mean_delay": 0.8},
    "google_trends": {"rpm": 10,  "safe_pct": 0.8, "mean_delay": 7.0},
    "amazon":        {"rpm": 50,  "safe_pct": 0.8, "mean_delay": 1.5},
    "walmart":       {"rpm": 20,  "safe_pct": 0.8, "mean_delay": 3.0},
    "etsy":          {"rpm": 40,  "safe_pct": 0.8, "mean_delay": 1.5},
    "alibaba":       {"rpm": 20,  "safe_pct": 0.8, "mean_delay": 3.0},
    "pinterest":     {"rpm": 50,  "safe_pct": 0.8, "mean_delay": 1.5},
}

# Signal table mappings
SIGNAL_TABLES = {
    "reddit": "signals_social",
    "tiktok": "signals_social",
    "instagram": "signals_social",
    "x": "signals_social",
    "facebook": "signals_social",
    "youtube": "signals_social",
    "google_trends": "signals_search",
    "amazon": "signals_retail",
    "walmart": "signals_retail",
    "etsy": "signals_retail",
    "alibaba": "signals_supply",
    "pinterest": "signals_discovery",
}
