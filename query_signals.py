"""One-shot query: Korean Sheet Masks signal data from Supabase."""
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

def dump(label, data):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(json.dumps(data, indent=2, default=str))

# 1. Product row for Korean Sheet Masks
res = pg.from_("products").select("*").ilike("name", "%korean%sheet%mask%").execute()
products = res.data
dump("PRODUCTS — Korean Sheet Masks", products)

if not products:
    # Try broader search
    res = pg.from_("products").select("*").ilike("name", "%sheet%mask%").execute()
    products = res.data
    dump("PRODUCTS — broader: Sheet Mask", products)

if not products:
    # List all products so we can find it
    res = pg.from_("products").select("id,name").execute()
    dump("ALL PRODUCTS (to find the right one)", res.data)
    sys.exit(1)

pid = products[0]["id"]
print(f"\n>>> Using product_id = {pid}")

# 2. Most recent signals_social for reddit, tiktok, instagram
for platform in ["reddit", "tiktok", "instagram"]:
    res = (
        pg.from_("signals_social")
        .select("*")
        .eq("product_id", pid)
        .eq("platform", platform)
        .order("scraped_date", desc=True)
        .limit(1)
        .execute()
    )
    dump(f"SIGNALS_SOCIAL — {platform}", res.data)

# 3. Most recent signals_search for google_trends
res = (
    pg.from_("signals_search")
    .select("*")
    .eq("product_id", pid)
    .eq("platform", "google_trends")
    .order("scraped_date", desc=True)
    .limit(1)
    .execute()
)
dump("SIGNALS_SEARCH — google_trends", res.data)

# 4. Most recent product_snapshots
res = (
    pg.from_("product_snapshots")
    .select("*")
    .eq("product_id", pid)
    .order("snapshot_date", desc=True)
    .limit(1)
    .execute()
)
dump("PRODUCT_SNAPSHOTS — latest", res.data)

# 5. Posts count by platform
# PostgREST doesn't support GROUP BY easily, so fetch counts per platform
for platform in ["reddit", "tiktok", "instagram", "x", "facebook", "youtube",
                  "google_trends", "amazon", "walmart", "etsy", "alibaba", "pinterest"]:
    res = (
        pg.from_("posts")
        .select("id", count="exact")
        .eq("product_id", pid)
        .eq("platform", platform)
        .execute()
    )
    count = res.count if res.count is not None else len(res.data)
    if count > 0:
        print(f"  posts/{platform}: {count}")

print("\nDone.")
