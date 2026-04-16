"""Test script: query product_hashtags and test BasePlatformAgent.get_hashtags()"""
import sys
import os

# 1. Setup paths and env
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, "agents", ".env"))

# 2. Query product_hashtags directly
from agents.config import get_supabase

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"
sb = get_supabase()

print("=" * 60)
print("DIRECT QUERY: product_hashtags")
print("=" * 60)

resp = sb.table("product_hashtags") \
    .select("*") \
    .eq("product_id", PRODUCT_ID) \
    .execute()

rows = resp.data or []
print(f"Total rows: {len(rows)}\n")

# 3. Group by platform
from collections import defaultdict
by_platform = defaultdict(list)
for r in rows:
    by_platform[r["platform"]].append(r)

for platform, items in sorted(by_platform.items()):
    print(f"  {platform} ({len(items)} hashtags):")
    for item in sorted(items, key=lambda x: x.get("priority", 99)):
        active = "active" if item.get("active") else "inactive"
        print(f"    - {item['hashtag']}  priority={item.get('priority')}  {active}")
    print()

# 4. Test BasePlatformAgent.get_hashtags()
print("=" * 60)
print("BasePlatformAgent.get_hashtags() TEST")
print("=" * 60)

from agents.base_platform_agent import BasePlatformAgent

product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["sheet mask", "k-beauty"],
}

# We need a concrete subclass to instantiate
class MockAgent(BasePlatformAgent):
    PLATFORM = ""
    def run_pass1(self, product, hashtags, lookback_days): return []
    def filter_pass1(self, items, lookback_days): return []
    def run_pass2(self, top_posts, product): return []
    def build_signal_row(self, raw_data, product_id): return {}
    def scrape(self, product_name, keywords, product): return {}

# Test each platform found in DB + a few known ones
platforms_to_test = sorted(set(list(by_platform.keys()) + ["tiktok", "instagram", "reddit", "x", "youtube"]))

for platform in platforms_to_test:
    agent = MockAgent()
    agent.PLATFORM = platform
    hashtags = agent.get_hashtags(product)
    print(f"\n  {platform}: {hashtags}")

print("\nDone.")
