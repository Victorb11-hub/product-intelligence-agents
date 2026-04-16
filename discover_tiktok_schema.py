"""Discover the exact field names returned by clockworks/tiktok-scraper."""
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from apify_client import ApifyClient

TOKEN = "apify_api_cdNK5IM79aoy3sVsj75sOt0hgvF2oI1OrZJN"
ACTOR = "clockworks/tiktok-scraper"

client = ApifyClient(TOKEN)

run_input = {
    "hashtags": ["sheetmask"],
    "resultsPerPage": 3,
}

print("Starting actor run...")
run = client.actor(ACTOR).call(
    run_input=run_input,
    timeout_secs=60,
    max_items=5,
)
print(f"Run finished. Status: {run['status']}")

dataset_id = run["defaultDatasetId"]
items = list(client.dataset(dataset_id).iterate_items())
print(f"Total items returned: {len(items)}")

if not items:
    print("NO ITEMS RETURNED")
    exit(1)

item = items[0]

# Part 1
print("\n" + "=" * 70)
print("PART 1 - ALL TOP-LEVEL KEYS")
print("=" * 70)

for key in sorted(item.keys()):
    val = item[key]
    t = type(val).__name__
    if val is None:
        print(f"  {key:30s}  type=NoneType  value=None")
    elif isinstance(val, bool):
        print(f"  {key:30s}  type=bool      value={val}")
    elif isinstance(val, (int, float)):
        print(f"  {key:30s}  type={t:8s}  value={val}")
    elif isinstance(val, str):
        sample = val[:100].replace("\n", "\\n").encode('ascii', 'replace').decode('ascii')
        print(f"  {key:30s}  type=str       value=\"{sample}\"")
    elif isinstance(val, dict):
        print(f"  {key:30s}  type=dict      keys={list(val.keys())}")
    elif isinstance(val, list):
        inner = type(val[0]).__name__ if val else "empty"
        first_sample = ""
        if val:
            if isinstance(val[0], dict):
                first_sample = f"  first_item_keys={list(val[0].keys())}"
            else:
                s = str(val[0]).encode('ascii', 'replace').decode('ascii')
                first_sample = f"  first_item={s}"
        print(f"  {key:30s}  type=list      len={len(val)}  inner={inner}{first_sample}")
    else:
        print(f"  {key:30s}  type={t}  value={repr(val)[:100]}")

# Part 2
print("\n" + "=" * 70)
print("PART 2 - FIELD MAPPING CANDIDATES")
print("=" * 70)

mappings = {
    "post_unique_id": ["id", "videoId", "video_id", "aweme_id"],
    "video_url": ["videoUrl", "video_url", "webVideoUrl", "downloadUrl"],
    "timestamp": ["createTime", "createTimeISO", "create_time", "timestamp", "postedAt"],
    "view_count": ["playCount", "play_count", "views", "viewCount"],
    "like_count": ["diggCount", "digg_count", "likes", "likeCount", "heartCount"],
    "comment_count": ["commentCount", "comment_count", "comments"],
    "share_count": ["shareCount", "share_count", "shares"],
    "caption_text": ["text", "desc", "description", "caption"],
    "hashtag_list": ["hashtags", "challenges", "textExtra"],
}

for concept, candidates in mappings.items():
    found = False
    for c in candidates:
        if c in item:
            v = item[c]
            sample = repr(v)[:120].encode('ascii', 'replace').decode('ascii')
            print(f"  {concept:20s} -> {c:30s} = {sample}")
            found = True
            break
    if not found:
        print(f"  {concept:20s} -> NOT FOUND in candidates {candidates}")

# Part 3
print("\n" + "=" * 70)
print("PART 3 - createTime DETAILS")
print("=" * 70)

if "createTime" in item:
    ct = item["createTime"]
    print(f"  createTime exists: True")
    print(f"  type: {type(ct).__name__}")
    print(f"  value: {ct}")
    if isinstance(ct, (int, float)):
        print("  format: UNIX TIMESTAMP")
    elif isinstance(ct, str):
        if ct.isdigit():
            print("  format: UNIX TIMESTAMP (as string)")
        else:
            print("  format: ISO STRING (or other)")
else:
    print("  createTime exists: False")

if "createTimeISO" in item:
    print(f"  createTimeISO: {item['createTimeISO']}")

# Part 4
print("\n" + "=" * 70)
print("PART 4 - FULL FIRST ITEM (JSON, truncated)")
print("=" * 70)
raw = json.dumps(item, indent=2, default=str, ensure_ascii=True)
print(raw[:3000])
