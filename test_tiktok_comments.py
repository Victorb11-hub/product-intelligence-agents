"""
Test script - Run 6: Use the actor's own prefill URL + profiles mode to get real output.
"""
import json
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from apify_client import ApifyClient

TOKEN = "apify_api_cdNK5IM79aoy3sVsj75sOt0hgvF2oI1OrZJN"
ACTOR_ID = "clockworks/tiktok-comments-scraper"

client = ApifyClient(TOKEN)

# ── Attempt A: Use the actor's own prefill/example URL ──
print("=" * 80)
print("ATTEMPT A: Using actor's own example URL (bellapoarch)")
print("=" * 80)

test_input_a = {
    "postURLs": ["https://www.tiktok.com/@bellapoarch/video/6862153058223197445"],
    "commentsPerPost": 5
}
print(f"Input: {json.dumps(test_input_a, indent=2)}")

try:
    run = client.actor(ACTOR_ID).call(
        run_input=test_input_a,
        timeout_secs=120,
        memory_mbytes=256,
    )

    print(f"\nRun status: {run.get('status')}")
    print(f"Run message: {run.get('statusMessage', 'N/A')}")

    dataset_id = run.get("defaultDatasetId")
    if dataset_id:
        items = list(client.dataset(dataset_id).iterate_items(limit=10))
        print(f"Items returned: {len(items)}")

        errors = [i for i in items if i.get("error")]
        comments = [i for i in items if not i.get("error")]

        if errors:
            for e in errors:
                print(f"  ERROR: {e.get('url')}: {e.get('error')}")

        if comments:
            print("\n>>> FULL COMMENT ITEMS:")
            for i, item in enumerate(comments[:5]):
                print(f"\n--- Comment {i+1} ---")
                print(json.dumps(item, indent=2, ensure_ascii=False, default=str))
            print(f"\n>>> FIELD NAMES: {sorted(comments[0].keys())}")

except Exception as e:
    print(f"Error: {e}")

# ── Attempt B: Use profiles mode (scrape by username) ──
print("\n" + "=" * 80)
print("ATTEMPT B: Using profiles mode (username-based)")
print("=" * 80)

test_input_b = {
    "profiles": ["therock"],
    "resultsPerPage": 1,
    "commentsPerPost": 3
}
print(f"Input: {json.dumps(test_input_b, indent=2)}")

try:
    run = client.actor(ACTOR_ID).call(
        run_input=test_input_b,
        timeout_secs=120,
        memory_mbytes=256,
    )

    print(f"\nRun status: {run.get('status')}")
    print(f"Run message: {run.get('statusMessage', 'N/A')}")

    dataset_id = run.get("defaultDatasetId")
    if dataset_id:
        items = list(client.dataset(dataset_id).iterate_items(limit=10))
        print(f"Items returned: {len(items)}")

        errors = [i for i in items if i.get("error")]
        comments = [i for i in items if not i.get("error")]

        if errors:
            for e in errors:
                print(f"  ERROR: {json.dumps(e, default=str)}")

        if comments:
            print("\n>>> FULL COMMENT ITEMS:")
            for i, item in enumerate(comments[:5]):
                print(f"\n--- Comment {i+1} ---")
                print(json.dumps(item, indent=2, ensure_ascii=False, default=str))
            print(f"\n>>> FIELD NAMES: {sorted(comments[0].keys())}")
        else:
            print("No comments. All items:")
            for item in items:
                print(json.dumps(item, indent=2, ensure_ascii=False, default=str))

except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
