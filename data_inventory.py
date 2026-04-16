"""
Complete data inventory from Supabase — NO Apify calls, pure DB queries.
Reads credentials from agents/.env (manual UTF-8 parse), queries all tables,
and prints a structured report.
"""
import os
import sys
import io
import json
from pathlib import Path
from datetime import datetime
from postgrest import SyncPostgrestClient

# Force UTF-8 stdout on Windows so box-drawing chars render
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")


# ────────────────────────────────────────────────────────────
# Manual .env parse (UTF-8, tolerant to BOM and comments)
# ────────────────────────────────────────────────────────────
ENV_PATH = Path(__file__).parent / "agents" / ".env"

def load_env(path: Path):
    env = {}
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            env[k] = v
    return env

env = load_env(ENV_PATH)
SUPABASE_URL = env.get("SUPABASE_URL", "")
SUPABASE_KEY = env.get("SUPABASE_KEY") or env.get("SUPABASE_ANON_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL / SUPABASE_KEY missing in agents/.env")
    sys.exit(1)

client = SyncPostgrestClient(
    f"{SUPABASE_URL}/rest/v1",
    headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    },
)

# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────
HEADER_CHAR = "═"
SUB_CHAR = "─"
WIDTH = 60

def section(title):
    print()
    print(HEADER_CHAR * WIDTH)
    print(title)
    print(HEADER_CHAR * WIDTH)

def sub(title):
    print()
    print(SUB_CHAR * WIDTH)
    print(title)
    print(SUB_CHAR * WIDTH)

def fmt(n):
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return str(n)

def count_rows(table, filters=None):
    """Return exact row count using PostgREST count=exact header trick."""
    try:
        # Use HEAD-like tactic via small range request
        import urllib.request
        import urllib.parse
        qs = "select=id"
        if filters:
            for k, v in filters.items():
                qs += f"&{k}=eq.{urllib.parse.quote(str(v))}"
        qs += "&limit=1"
        url = f"{SUPABASE_URL}/rest/v1/{table}?{qs}"
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Prefer": "count=exact",
            "Range-Unit": "items",
            "Range": "0-0",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            cr = resp.headers.get("Content-Range", "")
            # e.g. "0-0/12345" or "*/12345"
            if "/" in cr:
                total = cr.split("/")[-1]
                if total.isdigit():
                    return int(total)
        return None
    except Exception as e:
        return f"ERR: {e}"

def fetch_all(table, select="*", filters=None, page_size=1000, order=None):
    """Paginated fetch of all rows matching filters."""
    rows = []
    offset = 0
    while True:
        q = client.from_(table).select(select)
        if filters:
            for k, v in filters.items():
                q = q.eq(k, v)
        if order:
            for col, desc in order:
                q = q.order(col, desc=desc)
        q = q.range(offset, offset + page_size - 1)
        resp = q.execute()
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
        if offset > 500_000:  # safety
            break
    return rows

def fetch_one(table, select="*", filters=None, order=None, not_null=None):
    q = client.from_(table).select(select)
    if filters:
        for k, v in filters.items():
            q = q.eq(k, v)
    if not_null:
        for col in not_null:
            q = q.not_.is_(col, "null")
    if order:
        for col, desc in order:
            q = q.order(col, desc=desc)
    q = q.limit(1)
    resp = q.execute()
    return (resp.data or [None])[0]


# ────────────────────────────────────────────────────────────
# 1. TOTAL COUNTS
# ────────────────────────────────────────────────────────────
section("1. TOTAL COUNTS (system-wide)")

tables = [
    "posts",
    "comments",
    "signals_social",
    "signals_retail",
    "signals_search",
    "signals_supply",
    "signals_discovery",
    "pipeline_runs",
    "agent_runs",
    "scores_history",
    "product_snapshots",
    "alerts",
]

counts = {}
for t in tables:
    c = count_rows(t)
    counts[t] = c
    print(f"  {t:<25} {fmt(c) if isinstance(c, int) else c}")

full_pipeline_count = count_rows("pipeline_runs", filters={"phase": "full_pipeline"})
print(f"  {'pipeline_runs (full_pipeline)':<25} {fmt(full_pipeline_count) if isinstance(full_pipeline_count, int) else full_pipeline_count}")


# ────────────────────────────────────────────────────────────
# 2. PER-PLATFORM TOTALS
# ────────────────────────────────────────────────────────────
section("2. PER-PLATFORM TOTALS")

platforms = ["reddit", "tiktok", "instagram", "amazon", "google_trends"]

# Pre-pull posts (platform, upvotes, comment_count, scraped_date) for all wanted platforms where data_type='post'
# For resilience if data_type is sometimes null, also handle the "no data_type filter" fallback per platform
print("\nFetching posts aggregates per platform ...")
platform_post_stats = {}
for p in platforms:
    # Try data_type='post' first; if zero, fall back to no data_type filter
    rows = fetch_all(
        "posts",
        select="upvotes,comment_count,scraped_date",
        filters={"platform": p, "data_type": "post"},
    )
    used_filter = "data_type=post"
    if not rows:
        rows_fb = fetch_all(
            "posts",
            select="upvotes,comment_count,scraped_date,data_type",
            filters={"platform": p},
        )
        # keep only rows where data_type is null OR 'post'
        rows = [r for r in rows_fb if (r.get("data_type") in (None, "post", ""))]
        used_filter = "platform only (data_type null/post)"
    total_posts = len(rows)
    total_upvotes = sum((r.get("upvotes") or 0) for r in rows)
    total_comment_count = sum((r.get("comment_count") or 0) for r in rows)
    dates = [r.get("scraped_date") for r in rows if r.get("scraped_date")]
    min_d = min(dates) if dates else None
    max_d = max(dates) if dates else None
    platform_post_stats[p] = {
        "posts": total_posts,
        "upvotes": total_upvotes,
        "comment_count_sum": total_comment_count,
        "min_date": min_d,
        "max_date": max_d,
        "filter_used": used_filter,
    }

# Comments aggregates
print("Fetching comments aggregates per platform ...")
platform_comment_stats = {}
for p in platforms:
    rows = fetch_all(
        "comments",
        select="upvotes,is_buy_intent,is_problem_language",
        filters={"platform": p},
    )
    platform_comment_stats[p] = {
        "comments": len(rows),
        "buy_intent": sum(1 for r in rows if r.get("is_buy_intent")),
        "problem": sum(1 for r in rows if r.get("is_problem_language")),
    }

# TikTok views from signals_social.raw_json
print("Fetching TikTok views from signals_social.raw_json ...")
tiktok_total_views = 0
try:
    tt_rows = fetch_all(
        "signals_social",
        select="raw_json",
        filters={"platform": "tiktok"},
    )
    for r in tt_rows:
        raw = r.get("raw_json") or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = {}
        # Try several common keys
        for k in ("total_views", "totalViews", "view_count", "viewCount",
                  "play_count", "playCount", "plays", "views"):
            v = raw.get(k) if isinstance(raw, dict) else None
            if isinstance(v, (int, float)):
                tiktok_total_views += int(v)
                break
        else:
            # nested: items/posts/videos arrays
            if isinstance(raw, dict):
                for arr_key in ("items", "posts", "videos", "top_videos"):
                    arr = raw.get(arr_key)
                    if isinstance(arr, list):
                        for it in arr:
                            if isinstance(it, dict):
                                for k in ("playCount", "play_count", "plays", "views", "view_count"):
                                    v = it.get(k)
                                    if isinstance(v, (int, float)):
                                        tiktok_total_views += int(v)
                                        break
except Exception as e:
    print(f"  (signals_social TikTok views fetch error: {e})")

# Fallback: also accumulate TikTok views from posts.raw_json.playCount
try:
    tt_posts = fetch_all(
        "posts",
        select="raw_json",
        filters={"platform": "tiktok"},
    )
    for r in tt_posts:
        raw = r.get("raw_json") or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = {}
        if isinstance(raw, dict):
            for k in ("playCount", "play_count", "plays", "views", "view_count", "stats"):
                v = raw.get(k)
                if k == "stats" and isinstance(v, dict):
                    pv = v.get("playCount") or v.get("play_count")
                    if isinstance(pv, (int, float)):
                        tiktok_total_views += int(pv)
                        break
                elif isinstance(v, (int, float)):
                    tiktok_total_views += int(v)
                    break
except Exception as e:
    print(f"  (posts TikTok views fetch error: {e})")

# Render table
sub("Per-Platform Table")
header = f"{'Platform':<14} {'Posts':>8} {'Comments':>9} {'Upvotes':>12} {'Views':>14} {'Purchase':>9} {'Negative':>9}  {'Date Range':<25}"
print(header)
print(SUB_CHAR * len(header))
for p in platforms:
    ps = platform_post_stats.get(p, {})
    cs = platform_comment_stats.get(p, {})
    views = tiktok_total_views if p == "tiktok" else "—"
    date_range = f"{ps.get('min_date') or '—'} → {ps.get('max_date') or '—'}"
    print(
        f"{p:<14} "
        f"{fmt(ps.get('posts', 0)):>8} "
        f"{fmt(cs.get('comments', 0)):>9} "
        f"{fmt(ps.get('upvotes', 0)):>12} "
        f"{fmt(views) if isinstance(views,int) else views:>14} "
        f"{fmt(cs.get('buy_intent', 0)):>9} "
        f"{fmt(cs.get('problem', 0)):>9}  "
        f"{date_range:<25}"
    )


# ────────────────────────────────────────────────────────────
# 3. TOP NUMBERS
# ────────────────────────────────────────────────────────────
section("3. TOP NUMBERS")

def safe_get(d, key, default=None):
    if not d:
        return default
    return d.get(key, default)

# 3a. Highest viewed TikTok post (proxy by upvotes, per prompt)
sub("Highest Viewed Post (TikTok, by upvotes=likes proxy)")
top_tt = fetch_one(
    "posts",
    select="post_url,post_title,upvotes,platform",
    filters={"platform": "tiktok"},
    order=[("upvotes", True)],
)
if top_tt:
    print(f"  upvotes:  {fmt(top_tt.get('upvotes'))}")
    print(f"  title:    {top_tt.get('post_title') or '—'}")
    print(f"  url:      {top_tt.get('post_url') or '—'}")
else:
    print("  (no tiktok posts)")

# 3b. Most liked overall
sub("Most Liked Post Overall")
top_overall = fetch_one(
    "posts",
    select="platform,upvotes,post_url,post_title",
    order=[("upvotes", True)],
)
if top_overall:
    print(f"  platform: {top_overall.get('platform')}")
    print(f"  upvotes:  {fmt(top_overall.get('upvotes'))}")
    print(f"  title:    {top_overall.get('post_title') or '—'}")
    print(f"  url:      {top_overall.get('post_url') or '—'}")

# 3c. Most commented post
sub("Most Commented Post")
top_comm = fetch_one(
    "posts",
    select="platform,comment_count,post_url,post_title",
    order=[("comment_count", True)],
)
if top_comm:
    print(f"  platform:      {top_comm.get('platform')}")
    print(f"  comment_count: {fmt(top_comm.get('comment_count'))}")
    print(f"  title:         {top_comm.get('post_title') or '—'}")
    print(f"  url:           {top_comm.get('post_url') or '—'}")

# 3d. Most upvoted Reddit post
sub("Most Upvoted Reddit Post")
top_reddit = fetch_one(
    "posts",
    select="upvotes,post_title,post_url",
    filters={"platform": "reddit", "data_type": "post"},
    order=[("upvotes", True)],
)
if not top_reddit:
    # fallback without data_type filter
    top_reddit = fetch_one(
        "posts",
        select="upvotes,post_title,post_url",
        filters={"platform": "reddit"},
        order=[("upvotes", True)],
    )
if top_reddit:
    print(f"  upvotes: {fmt(top_reddit.get('upvotes'))}")
    print(f"  title:   {top_reddit.get('post_title') or '—'}")
    print(f"  url:     {top_reddit.get('post_url') or '—'}")
else:
    print("  (no reddit posts)")

# 3e. Highest purchase signal comment
sub("Highest Purchase Signal Comment")
top_buy = fetch_one(
    "comments",
    select="platform,upvotes,comment_body,is_buy_intent",
    filters={"is_buy_intent": True},
    order=[("upvotes", True)],
)
if top_buy:
    body = (top_buy.get("comment_body") or "")[:200]
    print(f"  platform: {top_buy.get('platform')}")
    print(f"  upvotes:  {fmt(top_buy.get('upvotes'))}")
    print(f"  body:     {body}")
else:
    print("  (no buy-intent comments)")

# 3f. Most negative comment
sub("Most Negative Comment (lowest sentiment_score)")
top_neg = fetch_one(
    "comments",
    select="platform,sentiment_score,comment_body",
    order=[("sentiment_score", False)],  # ASC
    not_null=["sentiment_score"],
)
if top_neg:
    body = (top_neg.get("comment_body") or "")[:200]
    print(f"  platform:  {top_neg.get('platform')}")
    print(f"  sentiment: {top_neg.get('sentiment_score')}")
    print(f"  body:      {body}")
else:
    print("  (no scored comments)")


# ────────────────────────────────────────────────────────────
# 4. GRAND TOTALS
# ────────────────────────────────────────────────────────────
section("4. GRAND TOTALS")

total_posts = counts.get("posts") or 0
total_comments = counts.get("comments") or 0
combined = (total_posts if isinstance(total_posts, int) else 0) + \
           (total_comments if isinstance(total_comments, int) else 0)

# Sum upvotes/comment_count across ALL posts (not just the 5 platforms above)
print("Computing global upvote / comment_count sums across posts ...")
all_posts_agg = fetch_all("posts", select="upvotes,comment_count")
sum_upvotes = sum((r.get("upvotes") or 0) for r in all_posts_agg)
sum_comment_count_col = sum((r.get("comment_count") or 0) for r in all_posts_agg)

total_engagement = sum_upvotes + sum_comment_count_col + (total_comments if isinstance(total_comments, int) else 0)

print(f"  Combined rows (posts + comments):          {fmt(combined)}")
print(f"  Total upvotes/likes across all posts:      {fmt(sum_upvotes)}")
print(f"  Total comment_count across all post rows:  {fmt(sum_comment_count_col)}")
print(f"  Total comment rows (comments table):       {fmt(total_comments)}")
print(f"  Total engagement (upvotes + comment_count")
print(f"    + comment rows):                         {fmt(total_engagement)}")

print()
print(HEADER_CHAR * WIDTH)
print(f"Inventory complete at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(HEADER_CHAR * WIDTH)
