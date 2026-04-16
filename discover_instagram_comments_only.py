"""
Re-run only DISCOVERY 2 against a known-public post so we actually get a real comment item.
The hashtag-scraped post had 0 comments, so the comment scraper returned a placeholder error row.
"""
import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from apify_client import ApifyClient

ENV_PATH = Path(__file__).parent / "agents" / ".env"
load_dotenv(ENV_PATH)
TOKEN = os.getenv("APIFY_API_TOKEN")
client = ApifyClient(TOKEN)


def short_sample(value, max_len=140):
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
    s = repr(value)
    return (s[:max_len] + "...") if len(s) > max_len else s


# Try a few well-known high-engagement posts so we definitely get comment data
candidate_urls = [
    "https://www.instagram.com/p/DEEnFBpoyoF/",                # user-supplied fallback
    "https://www.instagram.com/p/C8Z3uZ9oOdj/",                # natgeo
    "https://www.instagram.com/p/CyAFsZJsEaj/",                # nasa
]

for url in candidate_urls:
    print(f"\n>>> Trying comment scraper on: {url}")
    try:
        run = client.actor("apify/instagram-comment-scraper").call(
            run_input={"directUrls": [url], "resultsLimit": 5},
            timeout_secs=90,
        )
    except Exception as e:
        print(f"  !!! call failed: {type(e).__name__}: {e}")
        continue

    if not run or "defaultDatasetId" not in run:
        print(f"  !!! no dataset; run={run}")
        continue

    items = list(client.dataset(run["defaultDatasetId"]).iterate_items(limit=8))
    print(f"  retrieved {len(items)} items")

    # Find first item that is a real comment (not an error placeholder)
    real = next((i for i in items if "error" not in i and ("text" in i or "id" in i)), None)
    if not real:
        print(f"  !!! no real comment items, first item keys = {list(items[0].keys()) if items else 'NONE'}")
        if items:
            print(f"      first item raw: {json.dumps(items[0], default=str)[:300]}")
        continue

    print("=" * 80)
    print(f"FIRST REAL COMMENT ITEM (from {url})")
    print("=" * 80)
    print(f"Total top-level keys: {len(real)}")
    print(f"Key list: {list(real.keys())}")
    print("-" * 80)
    for k, v in real.items():
        t = type(v).__name__
        print(f"  [{t:12s}] {k}: {short_sample(v)}")
    print("-" * 80)

    print("\n--- Discovery 2 field-role guesses ---")
    def guess(label, candidates):
        found = [c for c in candidates if c in real]
        vals = {c: short_sample(real.get(c)) for c in found}
        print(f"  {label}: found={found}  samples={vals}")

    guess("comment unique ID", ["id", "commentId", "pk"])
    guess("comment text", ["text", "comment", "content"])
    guess("like count", ["likesCount", "likes", "likeCount"])
    guess("posted timestamp", ["timestamp", "createdAt", "takenAt", "createdAtUtc"])
    guess("reply count", ["repliesCount", "replies", "replyCount", "childCommentCount"])
    guess("parent post URL", ["postUrl", "ownerPostUrl", "parentPostUrl", "url", "inputUrl"])

    sys.exit(0)

print("\n!!! All candidate URLs failed to return real comments.")
