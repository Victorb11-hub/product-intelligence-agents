import sys, os
sys.path.insert(0, r"C:\Users\vibraca\OneDrive - Evolution Equities LLC\Personal\Business\(1) Claude\Claude Code\Social Media Scraper")

from dotenv import load_dotenv
load_dotenv(r"C:\Users\vibraca\OneDrive - Evolution Equities LLC\Personal\Business\(1) Claude\Claude Code\Social Media Scraper\agents\.env")

import requests, json

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# --- Paginated fetch ---
all_rows = []
offset = 0
LIMIT = 1000
while True:
    url = f"{SUPABASE_URL}/rest/v1/comments?product_id=eq.{PRODUCT_ID}&select=sentiment_score,platform,comment_body&offset={offset}&limit={LIMIT}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    batch = r.json()
    if not batch:
        break
    all_rows.extend(batch)
    if len(batch) < LIMIT:
        break
    offset += LIMIT

print(f"Fetched {len(all_rows)} comments total\n")

# --- Sentiment buckets ---
zero = pos = neu = neg = null_count = 0
platform_sums = {}
platform_counts = {}

for row in all_rows:
    score = row.get("sentiment_score")
    plat = row.get("platform", "unknown")
    if score is None:
        null_count += 1
        continue
    score = float(score)
    # platform aggregation
    platform_sums[plat] = platform_sums.get(plat, 0.0) + score
    platform_counts[plat] = platform_counts.get(plat, 0) + 1

    if score == 0.0:
        zero += 1
    elif score > 0.2:
        pos += 1
    elif score < -0.2:
        neg += 1
    else:
        neu += 1

scored = zero + pos + neu + neg
print("=== Sentiment Distribution ===")
print(f"  Exactly 0.00 : {zero}")
print(f"  Positive >0.2: {pos}")
print(f"  Neutral       : {neu}")
print(f"  Negative <-0.2: {neg}")
print(f"  NULL scores   : {null_count}")
print(f"  Total rows    : {len(all_rows)}")

print("\n=== Per-Platform Average Sentiment ===")
for plat in sorted(platform_counts.keys()):
    avg = platform_sums[plat] / platform_counts[plat]
    print(f"  {plat:15s}: avg={avg:+.4f}  (n={platform_counts[plat]})")

# --- Top 3 positive ---
url_top = f"{SUPABASE_URL}/rest/v1/comments?product_id=eq.{PRODUCT_ID}&select=comment_body,platform,sentiment_score&order=sentiment_score.desc&limit=3&sentiment_score=not.is.null"
r = requests.get(url_top, headers=HEADERS, timeout=30)
r.raise_for_status()
print("\n=== Top 3 Positive ===")
for row in r.json():
    body = (row["comment_body"] or "")[:100]
    print(f"  [{row['platform']}] score={row['sentiment_score']:+.4f}  {body}")

# --- Top 3 negative ---
url_bot = f"{SUPABASE_URL}/rest/v1/comments?product_id=eq.{PRODUCT_ID}&select=comment_body,platform,sentiment_score&order=sentiment_score.asc&limit=3&sentiment_score=not.is.null"
r = requests.get(url_bot, headers=HEADERS, timeout=30)
r.raise_for_status()
print("\n=== Top 3 Negative ===")
for row in r.json():
    body = (row["comment_body"] or "")[:100]
    print(f"  [{row['platform']}] score={row['sentiment_score']:+.4f}  {body}")
