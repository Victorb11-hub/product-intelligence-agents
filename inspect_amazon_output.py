"""Inspect raw output from junglee/amazon-crawler for a single product URL."""
import json
import os
from dotenv import load_dotenv
from apify_client import ApifyClient

# Load env
load_dotenv(os.path.join(os.path.dirname(__file__), "agents", ".env"))

token = os.environ.get("APIFY_API_TOKEN")
if not token:
    raise RuntimeError("APIFY_API_TOKEN not set")

client = ApifyClient(token)

run_input = {
    "categoryOrProductUrls": [{"url": "https://www.amazon.com/dp/B09V3KXJPB"}],
    "maxItemsPerStartUrl": 1,
}

print("Starting junglee/amazon-crawler run...")
run = client.actor("junglee/amazon-crawler").call(run_input=run_input, timeout_secs=120)
print(f"Run finished. Status: {run.get('status')}")

dataset_id = run["defaultDatasetId"]
items = list(client.dataset(dataset_id).iterate_items())

if not items:
    print("NO ITEMS RETURNED")
    raise SystemExit(1)

item = items[0]

print(f"\n{'='*80}")
print(f"TOTAL TOP-LEVEL KEYS: {len(item.keys())}")
print(f"{'='*80}\n")

review_keys = []

for key in sorted(item.keys()):
    val = item[key]
    vtype = type(val).__name__

    # Check if review-related
    lower_key = key.lower()
    is_review = any(kw in lower_key for kw in ["review", "comment", "rating", "star", "feedback"])

    marker = " *** REVIEW-RELATED ***" if is_review else ""
    if is_review:
        review_keys.append(key)

    # Format sample
    if val is None:
        sample = "None"
    elif isinstance(val, str):
        sample = repr(val[:200])
    elif isinstance(val, (int, float, bool)):
        sample = repr(val)
    elif isinstance(val, list):
        sample = f"[{len(val)} items] first 3: {json.dumps(val[:3], default=str)[:300]}"
    elif isinstance(val, dict):
        sample = f"dict with keys: {list(val.keys())[:10]}"
    else:
        sample = repr(val)[:200]

    print(f"  {key:40s} ({vtype:8s}){marker}")
    print(f"    -> {sample}")
    print()

print(f"\n{'='*80}")
print("REVIEW-RELATED KEYS SUMMARY:")
print(f"{'='*80}")
for rk in review_keys:
    print(f"  - {rk}")

# Deep-dive into list-of-objects keys (reviews, etc.)
print(f"\n{'='*80}")
print("DEEP DIVE: Lists of objects (potential review/comment structures)")
print(f"{'='*80}")
for key in sorted(item.keys()):
    val = item[key]
    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
        print(f"\n--- {key} --- (first object, full structure):")
        print(json.dumps(val[0], indent=2, default=str))
