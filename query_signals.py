"""Quick query: latest signals_social rows for a product on TikTok and Instagram."""
import json, os, sys
from dotenv import load_dotenv

# Load env from agents/.env
load_dotenv(os.path.join(os.path.dirname(__file__), "agents", ".env"))

from postgrest import SyncPostgrestClient

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

rest_url = f"{SUPABASE_URL}/rest/v1"
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

pg = SyncPostgrestClient(rest_url, headers=headers)

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"

KEY_FIELDS = [
    "id", "product_id", "platform", "period_start", "period_end", "scraped_date",
    "mention_count", "total_views", "total_upvotes", "total_comment_count",
    "creator_tier_score", "avg_intent_score",
    "high_intent_comment_count", "buy_intent_comment_count",
    "sentiment_positive", "sentiment_negative", "sentiment_neutral",
    "engagement_rate", "created_at",
]

for platform in ("tiktok", "instagram"):
    print(f"\n{'='*60}")
    print(f"  signals_social  |  platform={platform}")
    print(f"{'='*60}")
    resp = (
        pg.from_("signals_social")
        .select("*")
        .eq("product_id", PRODUCT_ID)
        .eq("platform", platform)
        .order("scraped_date", desc=True)
        .limit(1)
        .execute()
    )
    rows = resp.data
    if not rows:
        print("  (no rows found)")
        continue
    row = rows[0]
    for k in KEY_FIELDS:
        if k in row:
            print(f"  {k:30s} = {row[k]}")
    # Print any extra columns not in KEY_FIELDS
    extras = set(row.keys()) - set(KEY_FIELDS)
    if extras:
        print(f"\n  -- extra columns --")
        for k in sorted(extras):
            print(f"  {k:30s} = {row[k]}")

print("\nDone.")
