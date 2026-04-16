"""
Test 4 Apify actors for native sorting capabilities.
Runs sequentially to avoid rate limits.
"""
import os
import sys
import io
import json
import time
import logging
from dotenv import load_dotenv

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Load token from agents/.env
env_path = os.path.join(os.path.dirname(__file__), "agents", ".env")
load_dotenv(env_path)
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")

if not APIFY_TOKEN:
    print("ERROR: APIFY_API_TOKEN not found in agents/.env")
    sys.exit(1)

from apify_client import ApifyClient

client = ApifyClient(APIFY_TOKEN)

# Suppress noisy apify logs
logging.getLogger("apify_client").setLevel(logging.WARNING)

TIMEOUT = 120


def run_actor(actor_id: str, run_input: dict, label: str) -> list:
    """Run actor and return items."""
    print(f"\n{'='*70}")
    print(f"RUNNING: {actor_id}")
    print(f"Label: {label}")
    print(f"Input: {json.dumps(run_input, indent=2)}")
    print(f"{'='*70}")

    start = time.time()
    try:
        run = client.actor(actor_id).call(
            run_input=run_input,
            timeout_secs=TIMEOUT,
            memory_mbytes=1024,
        )
    except Exception as e:
        print(f"ERROR running {actor_id}: {e}")
        return []

    elapsed = time.time() - start
    status = run.get("status", "UNKNOWN")
    print(f"Status: {status} | Elapsed: {elapsed:.1f}s")

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        print("No dataset returned")
        return []

    items = list(client.dataset(dataset_id).iterate_items(limit=50))
    print(f"Items returned: {len(items)}")
    return items


def print_keys(items):
    if items:
        print(f"\nAll fields in first result ({len(items[0])} keys):")
        for k in sorted(items[0].keys()):
            v = items[0][k]
            vtype = type(v).__name__
            preview = str(v)[:80] if v is not None else "None"
            print(f"  {k} ({vtype}): {preview}")


def analyze_sort(values, label):
    """Check if a list of numeric values is sorted."""
    if not values or len(values) < 2:
        print(f"  Not enough data to analyze {label} sort")
        return
    desc = all(values[i] >= values[i+1] for i in range(len(values)-1))
    asc = all(values[i] <= values[i+1] for i in range(len(values)-1))
    if desc:
        print(f"  {label}: SORTED DESCENDING (highest first)")
    elif asc:
        print(f"  {label}: SORTED ASCENDING (lowest first)")
    else:
        print(f"  {label}: NOT SORTED (values: {values})")


# ============================================================
# TEST 1: TikTok Scraper
# ============================================================
print("\n" + "#"*70)
print("# TEST 1: clockworks/tiktok-scraper")
print("#"*70)

items1 = run_actor("clockworks/tiktok-scraper",
                    {"hashtags": ["skincare"], "resultsPerPage": 10},
                    "Default sort")

if items1:
    print_keys(items1)

    print("\n--- First 5 results ---")
    plays, diggs, dates = [], [], []
    for i, item in enumerate(items1[:5]):
        pc = item.get("playCount", item.get("play_count", item.get("stats", {}).get("playCount", "N/A")))
        dc = item.get("diggCount", item.get("digg_count", item.get("stats", {}).get("diggCount", "N/A")))
        dt = item.get("createTimeISO", item.get("createTime", item.get("created_at", "N/A")))
        print(f"  [{i+1}] playCount={pc} | diggCount={dc} | createTimeISO={dt}")
        if isinstance(pc, (int, float)): plays.append(pc)
        if isinstance(dc, (int, float)): diggs.append(dc)

    # Collect all values for sort analysis
    all_plays = [item.get("playCount", item.get("play_count", item.get("stats", {}).get("playCount"))) for item in items1]
    all_plays = [x for x in all_plays if isinstance(x, (int, float))]
    all_diggs = [item.get("diggCount", item.get("digg_count", item.get("stats", {}).get("diggCount"))) for item in items1]
    all_diggs = [x for x in all_diggs if isinstance(x, (int, float))]
    all_dates = [item.get("createTimeISO", item.get("createTime", "")) for item in items1]
    all_dates = [x for x in all_dates if x]

    print("\n--- Sort Analysis (all results) ---")
    analyze_sort(all_plays, "playCount (views)")
    analyze_sort(all_diggs, "diggCount (likes)")
    if all_dates:
        print(f"  Dates range: {all_dates[0]} ... {all_dates[-1]}")
        desc_dates = all(all_dates[i] >= all_dates[i+1] for i in range(len(all_dates)-1))
        asc_dates = all(all_dates[i] <= all_dates[i+1] for i in range(len(all_dates)-1))
        if desc_dates:
            print("  createTimeISO: SORTED DESCENDING (newest first)")
        elif asc_dates:
            print("  createTimeISO: SORTED ASCENDING (oldest first)")
        else:
            print("  createTimeISO: NOT SORTED")

# Try with sortType
print("\n--- Testing sortType parameter ---")
items1b = run_actor("clockworks/tiktok-scraper",
                     {"hashtags": ["skincare"], "resultsPerPage": 10, "sortType": "1"},
                     "With sortType=1")

if items1b:
    print("\n--- First 5 results with sortType=1 ---")
    for i, item in enumerate(items1b[:5]):
        pc = item.get("playCount", item.get("play_count", item.get("stats", {}).get("playCount", "N/A")))
        dc = item.get("diggCount", item.get("digg_count", item.get("stats", {}).get("diggCount", "N/A")))
        dt = item.get("createTimeISO", item.get("createTime", item.get("created_at", "N/A")))
        print(f"  [{i+1}] playCount={pc} | diggCount={dc} | createTimeISO={dt}")

    all_plays_b = [item.get("playCount", item.get("play_count", item.get("stats", {}).get("playCount"))) for item in items1b]
    all_plays_b = [x for x in all_plays_b if isinstance(x, (int, float))]
    analyze_sort(all_plays_b, "playCount with sortType=1")


# ============================================================
# TEST 2: Instagram Hashtag Scraper
# ============================================================
print("\n" + "#"*70)
print("# TEST 2: apify/instagram-hashtag-scraper")
print("#"*70)

items2 = run_actor("apify/instagram-hashtag-scraper",
                    {"hashtags": ["skincare"], "resultsLimit": 10},
                    "Default sort")

if items2:
    print_keys(items2)

    print("\n--- First 5 results ---")
    for i, item in enumerate(items2[:5]):
        lc = item.get("likesCount", item.get("likes", "N/A"))
        cc = item.get("commentsCount", item.get("comments", "N/A"))
        ts = item.get("timestamp", item.get("taken_at", item.get("date", "N/A")))
        print(f"  [{i+1}] likesCount={lc} | commentsCount={cc} | timestamp={ts}")

    all_likes = [item.get("likesCount", item.get("likes")) for item in items2]
    all_likes = [x for x in all_likes if isinstance(x, (int, float))]
    all_comments = [item.get("commentsCount", item.get("comments")) for item in items2]
    all_comments = [x for x in all_comments if isinstance(x, (int, float))]
    all_ts = [item.get("timestamp", item.get("taken_at", "")) for item in items2]
    all_ts = [x for x in all_ts if x]

    print("\n--- Sort Analysis ---")
    analyze_sort(all_likes, "likesCount")
    analyze_sort(all_comments, "commentsCount")
    if all_ts:
        print(f"  Timestamps range: {str(all_ts[0])[:25]} ... {str(all_ts[-1])[:25]}")

# Try searchType=top
print("\n--- Testing searchType=top ---")
items2b = run_actor("apify/instagram-hashtag-scraper",
                     {"hashtags": ["skincare"], "resultsLimit": 10, "searchType": "top"},
                     "With searchType=top")

if items2b:
    print("\n--- First 5 results with searchType=top ---")
    for i, item in enumerate(items2b[:5]):
        lc = item.get("likesCount", item.get("likes", "N/A"))
        cc = item.get("commentsCount", item.get("comments", "N/A"))
        ts = item.get("timestamp", item.get("taken_at", item.get("date", "N/A")))
        print(f"  [{i+1}] likesCount={lc} | commentsCount={cc} | timestamp={ts}")

# Also try type=top
print("\n--- Testing type=top ---")
items2c = run_actor("apify/instagram-hashtag-scraper",
                     {"hashtags": ["skincare"], "resultsLimit": 10, "type": "top"},
                     "With type=top")

if items2c:
    print("\n--- First 5 results with type=top ---")
    for i, item in enumerate(items2c[:5]):
        lc = item.get("likesCount", item.get("likes", "N/A"))
        cc = item.get("commentsCount", item.get("comments", "N/A"))
        ts = item.get("timestamp", item.get("taken_at", item.get("date", "N/A")))
        print(f"  [{i+1}] likesCount={lc} | commentsCount={cc} | timestamp={ts}")


# ============================================================
# TEST 3: Reddit Scraper
# ============================================================
print("\n" + "#"*70)
print("# TEST 3: macrocosmos/reddit-scraper")
print("#"*70)

items3 = run_actor("macrocosmos/reddit-scraper",
                    {"searches": [{"term": "sheet mask", "sort": "top", "time": "month"}], "maxItems": 10},
                    "Sort by top, time=month")

if items3:
    print_keys(items3)

    print("\n--- First 5 results ---")
    for i, item in enumerate(items3[:5]):
        title = str(item.get("title", "N/A"))[:50]
        score = item.get("score", item.get("ups", "N/A"))
        nc = item.get("num_comments", item.get("numComments", item.get("comments", "N/A")))
        ca = item.get("createdAt", item.get("created_at", item.get("created", "N/A")))
        print(f"  [{i+1}] title={title} | score={score} | num_comments={nc} | createdAt={ca}")

    all_scores = [item.get("score", item.get("ups")) for item in items3]
    all_scores = [x for x in all_scores if isinstance(x, (int, float))]

    print("\n--- Sort Analysis ---")
    analyze_sort(all_scores, "score")


# ============================================================
# TEST 4: Amazon Crawler
# ============================================================
print("\n" + "#"*70)
print("# TEST 4: junglee/amazon-crawler")
print("#"*70)

items4 = run_actor("junglee/amazon-crawler",
                    {"categoryOrProductUrls": [{"url": "https://www.amazon.com/s?k=sheet+mask"}], "maxItemsPerStartUrl": 10},
                    "Default sort (relevance)")

if items4:
    print_keys(items4)

    print("\n--- First 5 results ---")
    for i, item in enumerate(items4[:5]):
        title = str(item.get("title", "N/A"))[:50]
        rc = item.get("reviewsCount", item.get("reviews", item.get("numberOfReviews", "N/A")))
        stars = item.get("stars", item.get("rating", item.get("averageRating", "N/A")))
        price = item.get("price", item.get("currentPrice", "N/A"))
        print(f"  [{i+1}] title={title} | reviewsCount={rc} | stars={stars} | price={price}")

    all_reviews = [item.get("reviewsCount", item.get("reviews", item.get("numberOfReviews"))) for item in items4]
    all_reviews = [x for x in all_reviews if isinstance(x, (int, float))]

    print("\n--- Sort Analysis ---")
    analyze_sort(all_reviews, "reviewsCount")

# Try sort by review-rank
print("\n--- Testing sort by review-rank (URL param) ---")
items4b = run_actor("junglee/amazon-crawler",
                     {"categoryOrProductUrls": [{"url": "https://www.amazon.com/s?k=sheet+mask&s=review-rank"}], "maxItemsPerStartUrl": 10},
                     "Sort by review-rank")

if items4b:
    print("\n--- First 5 results with review-rank sort ---")
    for i, item in enumerate(items4b[:5]):
        title = str(item.get("title", "N/A"))[:50]
        rc = item.get("reviewsCount", item.get("reviews", item.get("numberOfReviews", "N/A")))
        stars = item.get("stars", item.get("rating", item.get("averageRating", "N/A")))
        price = item.get("price", item.get("currentPrice", "N/A"))
        print(f"  [{i+1}] title={title} | reviewsCount={rc} | stars={stars} | price={price}")

    all_reviews_b = [item.get("reviewsCount", item.get("reviews", item.get("numberOfReviews"))) for item in items4b]
    all_reviews_b = [x for x in all_reviews_b if isinstance(x, (int, float))]
    analyze_sort(all_reviews_b, "reviewsCount with review-rank")


print("\n" + "="*70)
print("ALL TESTS COMPLETE")
print("="*70)
