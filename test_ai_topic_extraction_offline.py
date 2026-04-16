"""
Offline test of AI topic breakdown extraction using saved raw JSON.
NO Apify calls. Pure JSON parsing that mirrors agent_amazon._fetch_product_details.

UPDATED: Uses the NEW repeat-purchase detection logic from agent_amazon.py:
  1. Topic NAME scan  (original)
  2. Snippet TEXT scan (NEW — scans topic.partialReviews text)
  3. Monthly volume proxy (NEW — uses >= not >)
  4. Expanded keyword list (added "buy again", "ordering again",
     "ordering more", "buying more", "for years", "many years",
     "been using", "keep ordering")
"""

import json
import os
import sys
from pathlib import Path

# Force UTF-8 stdout on Windows
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# --- 1. Project setup -------------------------------------------------
PROJECT_ROOT = Path(
    r"c:\Users\vibraca\OneDrive - Evolution Equities LLC\Personal\Business\(1) Claude\Claude Code\Social Media Scraper"
)
sys.path.insert(0, str(PROJECT_ROOT))

# Manual UTF-8 env parse
env_path = PROJECT_ROOT / "agents" / ".env"
if env_path.exists():
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# --- 2. Load raw JSON -------------------------------------------------
raw_path = PROJECT_ROOT / "amazon_crawler_discovery_raw.json"
with open(raw_path, "r", encoding="utf-8") as f:
    raw = json.load(f)

# Wrap in list to simulate Pass 2 items iterable
items = [raw]

# --- 3. Replicate _fetch_product_details extraction loop --------------
total_monthly_volume = 0
ai_topics_all = []
brands_seen = []
variant_counts = []
category_paths = []

first_item_info = {}

for idx, item in enumerate(items):
    # Capture raw snapshot
    if idx == 0:
        first_item_info = {
            "asin": item.get("asin") or item.get("originalAsin") or "",
            "title": (item.get("title") or "")[:80],
            "brand_raw": item.get("brand") or "",
            "variantAsins_count": len(item.get("variantAsins") or []),
            "breadCrumbs": item.get("breadCrumbs") or "",
        }

    # Monthly purchase volume extraction
    mpv = item.get("monthlyPurchaseVolume") or item.get("boughtInLastMonth") or ""
    if isinstance(mpv, str):
        mpv_lower = mpv.lower().replace(",", "").replace("+", "").strip()
        try:
            if "k" in mpv_lower:
                mpv_num = int(float(mpv_lower.replace("k", "").split()[0]) * 1000)
            else:
                import re
                nums = re.findall(r"\d+", mpv_lower)
                mpv_num = int(nums[0]) if nums else 0
        except (ValueError, IndexError):
            mpv_num = 0
    elif isinstance(mpv, (int, float)):
        mpv_num = int(mpv)
    else:
        mpv_num = 0
    total_monthly_volume += mpv_num

    # AI review summary + keywords -> ai_topics_all
    ai_summary = item.get("aiReviewsSummary") or {}
    if isinstance(ai_summary, dict):
        keywords = ai_summary.get("keywords") or []
        if isinstance(keywords, list):
            for topic in keywords:
                if not isinstance(topic, dict):
                    continue
                mentions = topic.get("customersMentionedCount") or {}
                partial = topic.get("partialReviews") or []
                snippets = []
                for p in partial[:3]:
                    if isinstance(p, dict):
                        snippets.append({
                            "text": (p.get("text") or "")[:200],
                            "highlighted": (p.get("highlightedPart") or "")[:100],
                        })
                ai_topics_all.append({
                    "name": topic.get("name") or "",
                    "sentiment": topic.get("sentiment") or "",
                    "total_mentions": mentions.get("total", 0) if isinstance(mentions, dict) else 0,
                    "positive_mentions": mentions.get("positive", 0) if isinstance(mentions, dict) else 0,
                    "negative_mentions": mentions.get("negative", 0) if isinstance(mentions, dict) else 0,
                    "snippets": snippets,
                })

    # Brand
    brand_val = item.get("brand") or ""
    if brand_val and isinstance(brand_val, str):
        brands_seen.append(brand_val.strip())

    # Variant count
    variant_asins = item.get("variantAsins") or []
    if isinstance(variant_asins, list):
        variant_counts.append(len(variant_asins))

    # Category path
    breadcrumbs = item.get("breadCrumbs") or ""
    if breadcrumbs and isinstance(breadcrumbs, str):
        category_paths.append(breadcrumbs.strip())

# --- 4. NEW Repeat purchase detection (exact copy from updated agent) -
repeat_keywords = [
    "repurchas", "reorder", "restock", "again", "monthly", "regularly",
    "always buy", "keep buying", "stock up", "subscribe", "staple",
    "holy grail", "cant live without", "can't live without", "everyday",
    "buy again", "ordering again", "ordering more", "buying more",
    "for years", "many years", "been using", "keep ordering",
]

# PASS 1: topic name scan
repeat_topic = None
repeat_mentions = 0
name_hits = []
for topic in ai_topics_all:
    name_lower = (topic.get("name") or "").lower()
    matched_kw = next((kw for kw in repeat_keywords if kw in name_lower), None)
    if matched_kw:
        mentions = topic.get("total_mentions", 0) or 0
        name_hits.append({
            "name": topic.get("name"),
            "keyword": matched_kw,
            "mentions": mentions,
        })
        if mentions > repeat_mentions:
            repeat_topic = topic.get("name")
            repeat_mentions = mentions

# PASS 2: snippet text scan (NEW)
snippet_hit_list = []  # rich info
snippet_hits = 0
for topic in ai_topics_all:
    for snip in topic.get("snippets", []):
        text = snip.get("text") or ""
        text_lower = text.lower()
        matched_kw = next((kw for kw in repeat_keywords if kw in text_lower), None)
        if matched_kw:
            snippet_hits += 1
            snippet_hit_list.append({
                "topic_name": topic.get("name"),
                "keyword": matched_kw,
                "text": text,
            })

# Final decision
path_used = None
repeat_signal = False
if repeat_topic and repeat_mentions > 0:
    repeat_signal = True
    path_used = "topic_name"
elif snippet_hits > 0:
    repeat_mentions = snippet_hits * 20
    repeat_signal = True
    path_used = "snippet_text"
else:
    path_used = "monthly_volume_proxy"
    if total_monthly_volume >= 10000:
        repeat_mentions = 300
        repeat_signal = True
    elif total_monthly_volume >= 5000:
        repeat_mentions = 150
        repeat_signal = True
    elif total_monthly_volume >= 1000:
        repeat_mentions = 50
        repeat_signal = True

# --- 5. Print sections ------------------------------------------------
def section(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


section("RAW ITEM INFO")
print(f"ASIN:              {first_item_info['asin']}")
print(f"Title (80 chars):  {first_item_info['title']}")
print(f"brand:             {first_item_info['brand_raw']}")
print(f"variantAsins cnt:  {first_item_info['variantAsins_count']}")
print(f"breadCrumbs:       {first_item_info['breadCrumbs']}")
print(f"monthly_volume:    {total_monthly_volume}")
print(f"total topics:      {len(ai_topics_all)}")


section("SECTION 1 - Topic NAME scanning")
print(f"Keywords checked ({len(repeat_keywords)}):")
print(f"  {repeat_keywords}\n")
print(f"Total topics scanned: {len(ai_topics_all)}")
for i, t in enumerate(ai_topics_all, 1):
    print(f"  [{i}] {t['name']!r}  (sentiment={t['sentiment']}, mentions={t['total_mentions']})")
print()
if name_hits:
    print(f"TOPIC NAME HITS ({len(name_hits)}):")
    for h in name_hits:
        print(f"  - name={h['name']!r} kw={h['keyword']!r} mentions={h['mentions']}")
else:
    print("NO topic NAME matched any repeat keyword. (expected)")


section("SECTION 2 - Snippet TEXT scanning (NEW)")
total_snips_scanned = sum(len(t.get("snippets", [])) for t in ai_topics_all)
print(f"Total snippets scanned across {len(ai_topics_all)} topics: {total_snips_scanned}")
print(f"Total snippet hits: {snippet_hits}\n")

if snippet_hit_list:
    for i, h in enumerate(snippet_hit_list, 1):
        print(f"  HIT [{i}]")
        print(f"    topic:   {h['topic_name']!r}")
        print(f"    keyword: {h['keyword']!r}")
        print(f"    text:    {h['text']!r}")
        print()
else:
    print("  NO snippet matched any repeat keyword.")


section("SECTION 3 - Final decision")
print(f"repeat_purchase_mentions = {repeat_mentions}")
print(f"path used                = {path_used}")
print(f"repeat_signal            = {repeat_signal}")
if path_used == "topic_name":
    print(f"  matched topic:  {repeat_topic!r}")
elif path_used == "snippet_text":
    print(f"  snippet_hits:   {snippet_hits}")
    print(f"  mentions calc:  {snippet_hits} * 20 = {repeat_mentions}")
elif path_used == "monthly_volume_proxy":
    print(f"  monthly_volume: {total_monthly_volume}")
    print(f"  tier mapping (>=10k=300, >=5k=150, >=1k=50, else=0)")


section("SECTION 4 - Scoring impact")

def new_repeat_norm(m):
    if m <= 0:        return 0
    if m <= 50:       return 20
    if m <= 150:      return 40
    if m <= 300:      return 60
    if m <= 500:      return 80
    return 100

new_norm = new_repeat_norm(repeat_mentions)
print("Tier formula: 0->0 | 1-50->20 | 51-150->40 | 151-300->60 | 301-500->80 | 500+->100")
print(f"repeat_purchase_mentions = {repeat_mentions}")
print(f"=> repeat_norm           = {new_norm}")

# Compare against OLD formula (pre-fix)
old_high_intent = 0
old_review_count = 1534
old_repeat_norm = min(100, (old_high_intent / max(old_review_count, 1)) * 500)
print(f"\nOLD formula reference: min(100, (high_intent/review_count)*500)")
print(f"  high_intent_count={old_high_intent}, review_count={old_review_count}")
print(f"  => old_repeat_norm = {old_repeat_norm}")
print(f"\nDelta: +{new_norm - old_repeat_norm} points on repeat_norm")


print("\n" + "=" * 72)
print("DONE. No Apify calls made. Pure offline parse.")
print("=" * 72)
