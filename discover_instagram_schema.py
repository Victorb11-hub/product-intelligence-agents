"""
Discovery script for Instagram Apify actor field names.
Runs two sequential discovery calls:
  1. apify/instagram-hashtag-scraper
  2. apify/instagram-comment-scraper
"""
import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from apify_client import ApifyClient

# Load token from agents/.env
ENV_PATH = Path(__file__).parent / "agents" / ".env"
load_dotenv(ENV_PATH)

TOKEN = os.getenv("APIFY_API_TOKEN")
if not TOKEN:
    print("ERROR: APIFY_API_TOKEN not found in agents/.env")
    sys.exit(1)

client = ApifyClient(TOKEN)


def short_sample(value, max_len=120):
    """Return a short, readable sample of a value."""
    if value is None:
        return "None"
    if isinstance(value, str):
        v = value.replace("\n", " ").replace("\r", " ")
        return (v[:max_len] + "...") if len(v) > max_len else v
    if isinstance(value, (list, tuple)):
        if len(value) == 0:
            return "[] (empty)"
        head = value[0]
        if isinstance(head, (dict, list)):
            try:
                s = json.dumps(head, default=str)[:max_len]
                return f"[{len(value)} items] first={s}..."
            except Exception:
                return f"[{len(value)} items] first=<complex>"
        return f"[{len(value)} items] first={head!r}"[:max_len]
    if isinstance(value, dict):
        keys = list(value.keys())[:8]
        return f"{{{len(value)} keys}} keys={keys}"
    # primitive: int, float, bool
    s = repr(value)
    return (s[:max_len] + "...") if len(s) > max_len else s


def dump_item(item, title):
    print("=" * 80)
    print(title)
    print("=" * 80)
    if item is None:
        print("<no item returned>")
        return
    print(f"Total top-level keys: {len(item)}")
    print(f"Key list: {list(item.keys())}")
    print("-" * 80)
    for k, v in item.items():
        t = type(v).__name__
        print(f"  [{t:12s}] {k}: {short_sample(v)}")
    print("-" * 80)


def run_actor(actor_id, run_input, timeout_secs, max_items):
    print(f"\n>>> Calling actor: {actor_id}")
    print(f">>> Input: {json.dumps(run_input)}")
    print(f">>> Timeout: {timeout_secs}s  max_items: {max_items}")
    try:
        run = client.actor(actor_id).call(
            run_input=run_input,
            timeout_secs=timeout_secs,
        )
    except Exception as e:
        print(f"!!! Actor call failed: {type(e).__name__}: {e}")
        return []

    if not run or "defaultDatasetId" not in run:
        print(f"!!! No dataset returned. Run result: {run}")
        return []

    dataset_id = run["defaultDatasetId"]
    print(f">>> Dataset ID: {dataset_id}")
    try:
        items = list(
            client.dataset(dataset_id).iterate_items(limit=max_items)
        )
    except Exception as e:
        print(f"!!! Failed to iterate dataset: {type(e).__name__}: {e}")
        return []

    print(f">>> Retrieved {len(items)} items")
    return items


# ---------------------------------------------------------------------------
# DISCOVERY 1: hashtag scraper
# ---------------------------------------------------------------------------
print("\n" + "#" * 80)
print("# DISCOVERY 1: apify/instagram-hashtag-scraper")
print("#" * 80)

hashtag_items = run_actor(
    actor_id="apify/instagram-hashtag-scraper",
    run_input={"hashtags": ["sheetmask"], "resultsLimit": 3},
    timeout_secs=90,
    max_items=5,
)

first_post = hashtag_items[0] if hashtag_items else None
dump_item(first_post, "FIRST HASHTAG POST ITEM")

# Field role confirmation for Discovery 1
print("\n--- Discovery 1 field-role guesses ---")
if first_post:
    def guess(label, candidates):
        found = [c for c in candidates if c in first_post]
        vals = {c: short_sample(first_post.get(c)) for c in found}
        print(f"  {label}: found={found}  samples={vals}")

    guess("post unique ID", ["id", "postId", "shortCode"])
    guess("post URL / shortCode", ["url", "shortCode", "postUrl"])
    guess("posted timestamp", ["timestamp", "takenAtTimestamp", "takenAt"])
    guess("like count", ["likesCount", "likes", "likeCount"])
    guess("comment count", ["commentsCount", "comments", "commentCount"])
    guess("post type", ["type", "productType", "__typename", "mediaType"])
    guess("caption text", ["caption", "text", "description"])
    guess("hashtag list", ["hashtags", "tags"])
    guess("video view / play count", [
        "videoPlayCount", "videoViewCount", "viewCount", "playCount", "videoPlays",
    ])
else:
    print("  (no post item to analyze)")

# Pick a post URL for Discovery 2
post_url_for_comments = None
if first_post:
    for key in ("url", "postUrl", "shortCode"):
        if first_post.get(key):
            val = first_post[key]
            if key == "shortCode":
                post_url_for_comments = f"https://www.instagram.com/p/{val}/"
            else:
                post_url_for_comments = val
            break

if not post_url_for_comments:
    post_url_for_comments = "https://www.instagram.com/p/DEEnFBpoyoF/"
    print(f"\n!!! Falling back to known post URL: {post_url_for_comments}")
else:
    print(f"\n>>> Using post URL from Discovery 1: {post_url_for_comments}")

# ---------------------------------------------------------------------------
# DISCOVERY 2: comment scraper
# ---------------------------------------------------------------------------
print("\n" + "#" * 80)
print("# DISCOVERY 2: apify/instagram-comment-scraper")
print("#" * 80)

comment_items = run_actor(
    actor_id="apify/instagram-comment-scraper",
    run_input={"directUrls": [post_url_for_comments], "resultsLimit": 5},
    timeout_secs=90,
    max_items=8,
)

first_comment = comment_items[0] if comment_items else None
dump_item(first_comment, "FIRST COMMENT ITEM")

print("\n--- Discovery 2 field-role guesses ---")
if first_comment:
    def guess2(label, candidates):
        found = [c for c in candidates if c in first_comment]
        vals = {c: short_sample(first_comment.get(c)) for c in found}
        print(f"  {label}: found={found}  samples={vals}")

    guess2("comment unique ID", ["id", "commentId", "pk"])
    guess2("comment text", ["text", "comment", "content"])
    guess2("like count", ["likesCount", "likes", "likeCount"])
    guess2("posted timestamp", ["timestamp", "createdAt", "takenAt", "createdAtUtc"])
    guess2("reply count", ["repliesCount", "replies", "replyCount", "childCommentCount"])
    guess2("parent post URL", ["postUrl", "ownerPostUrl", "parentPostUrl", "url"])
else:
    print("  (no comment item to analyze)")

print("\n>>> Discovery complete.")
