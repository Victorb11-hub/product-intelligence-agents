"""
Pull FRESH posts from a high-traffic hashtag, pick ones with commentsCount > 0,
and feed them to the comment scraper. This avoids stale-URL 404s.
"""
import os, json, sys, io
from pathlib import Path
from dotenv import load_dotenv
from apify_client import ApifyClient

# Force UTF-8 stdout so emoji in comment text don't crash on Windows cp1252
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

load_dotenv(Path(__file__).parent / "agents" / ".env")
client = ApifyClient(os.getenv("APIFY_API_TOKEN"))


def short(value, n=140):
    if value is None: return "None"
    if isinstance(value, str):
        v = value.replace("\n", " ")
        return (v[:n] + "...") if len(v) > n else v
    if isinstance(value, list):
        if not value: return "[]"
        h = value[0]
        if isinstance(h, (dict, list)):
            return f"[{len(value)}] first={json.dumps(h, default=str)[:n]}..."
        return f"[{len(value)}] first={h!r}"[:n]
    if isinstance(value, dict):
        return f"{{{len(value)} keys}} keys={list(value.keys())[:8]}"
    return repr(value)[:n]


# Get FRESH posts from a busy beauty hashtag (high comment volume on most)
print(">>> Pulling fresh posts from #skincare (high engagement)")
run = client.actor("apify/instagram-hashtag-scraper").call(
    run_input={"hashtags": ["skincare"], "resultsLimit": 30},
    timeout_secs=120,
)
posts = list(client.dataset(run["defaultDatasetId"]).iterate_items(limit=30))
print(f"  got {len(posts)} posts")

# Filter to posts with comments
candidates = [p for p in posts if (p.get("commentsCount") or 0) > 0 and p.get("url")]
candidates.sort(key=lambda p: p.get("commentsCount", 0), reverse=True)
print(f"  {len(candidates)} have commentsCount>0; top counts: "
      f"{[c.get('commentsCount') for c in candidates[:5]]}")

if not candidates:
    print("!!! no fresh posts with comments — bailing")
    sys.exit(1)

# Try the top 3 freshest
for c in candidates[:3]:
    url = c["url"]
    print(f"\n>>> Trying comment scraper on {url} (commentsCount={c.get('commentsCount')})")
    try:
        r = client.actor("apify/instagram-comment-scraper").call(
            run_input={"directUrls": [url], "resultsLimit": 5},
            timeout_secs=120,
        )
    except Exception as e:
        print(f"  call failed: {e}")
        continue
    items = list(client.dataset(r["defaultDatasetId"]).iterate_items(limit=8))
    print(f"  got {len(items)} items; first item keys: {list(items[0].keys()) if items else 'NONE'}")

    real = next((i for i in items if "error" not in i and ("text" in i or "id" in i or "ownerUsername" in i)), None)
    if not real:
        if items:
            print(f"  raw first: {json.dumps(items[0], default=str)[:300]}")
        continue

    print("=" * 80)
    print(f"FIRST REAL COMMENT (from {url})")
    print("=" * 80)
    print(f"Total top-level keys: {len(real)}")
    print(f"Key list: {list(real.keys())}")
    print("-" * 80)
    for k, v in real.items():
        print(f"  [{type(v).__name__:12s}] {k}: {short(v)}")
    print("-" * 80)

    print("\n--- Comment field-role guesses ---")
    def g(label, cands):
        f = [x for x in cands if x in real]
        print(f"  {label}: found={f}  samples={ {x: short(real[x]) for x in f} }")
    g("comment unique ID", ["id", "commentId", "pk"])
    g("comment text", ["text", "comment", "content"])
    g("like count", ["likesCount", "likes", "likeCount"])
    g("posted timestamp", ["timestamp", "createdAt", "takenAt", "createdAtUtc"])
    g("reply count", ["repliesCount", "replies", "replyCount", "childCommentCount"])
    g("parent post URL", ["postUrl", "ownerPostUrl", "parentPostUrl", "url", "inputUrl"])
    sys.exit(0)

print("\n!!! all top fresh posts also failed — comment actor or IG is blocking right now")
