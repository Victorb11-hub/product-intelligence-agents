"""Korean Sheet Masks data inventory query — NO Apify calls."""
import sys, os, json
sys.stdout.reconfigure(encoding="utf-8")
from datetime import datetime

# ── Manual .env parse ──────────────────────────────────────────────
env_path = os.path.join(os.path.dirname(__file__), "agents", ".env")
with open(env_path, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "agents"))
from config import get_supabase

sb = get_supabase()
PID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"

def fmt(n):
    if n is None:
        return "—"
    return f"{n:,.0f}" if isinstance(n, (int, float)) else str(n)

def safe_sum(rows, key):
    return sum(r.get(key) or 0 for r in rows)

# ═══════════════════════════════════════════════════════════════════
# 1. POSTS PER PLATFORM
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. POSTS PER PLATFORM")
print("=" * 70)

# Get all post rows for this product
posts_all = sb.table("posts").select("platform,data_type,upvotes,comment_count").eq("product_id", PID).execute().data

platforms = sorted(set(r["platform"] for r in posts_all))

print(f"{'Platform':<15} {'Posts':>8} {'Comment-rows':>14} {'Total Upvotes':>15} {'Total comment_count':>20}")
print("-" * 75)
for plat in platforms:
    posts = [r for r in posts_all if r["platform"] == plat and r.get("data_type") == "post"]
    comments = [r for r in posts_all if r["platform"] == plat and r.get("data_type") == "comment"]
    upvotes = safe_sum(posts, "upvotes")
    cc = safe_sum(posts, "comment_count")
    print(f"{plat:<15} {fmt(len(posts)):>8} {fmt(len(comments)):>14} {fmt(upvotes):>15} {fmt(cc):>20}")

total_posts = len([r for r in posts_all if r.get("data_type") == "post"])
total_cr = len([r for r in posts_all if r.get("data_type") == "comment"])
print("-" * 75)
print(f"{'TOTAL':<15} {fmt(total_posts):>8} {fmt(total_cr):>14}")

# ═══════════════════════════════════════════════════════════════════
# 2. COMMENTS PER PLATFORM
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. COMMENTS PER PLATFORM (comments table)")
print("=" * 70)

comments_all = sb.table("comments").select("platform,is_buy_intent,is_problem_language").eq("product_id", PID).execute().data

c_platforms = sorted(set(r["platform"] for r in comments_all)) if comments_all else []

print(f"{'Platform':<15} {'Comments':>10} {'Purchase':>10} {'Negative':>10}")
print("-" * 50)
for plat in c_platforms:
    rows = [r for r in comments_all if r["platform"] == plat]
    buy = sum(1 for r in rows if r.get("is_buy_intent"))
    neg = sum(1 for r in rows if r.get("is_problem_language"))
    print(f"{plat:<15} {fmt(len(rows)):>10} {fmt(buy):>10} {fmt(neg):>10}")

if comments_all:
    print("-" * 50)
    print(f"{'TOTAL':<15} {fmt(len(comments_all)):>10} {fmt(sum(1 for r in comments_all if r.get('is_buy_intent'))):>10} {fmt(sum(1 for r in comments_all if r.get('is_problem_language'))):>10}")
else:
    print("  (no rows in comments table)")

# ═══════════════════════════════════════════════════════════════════
# 3. ENGAGEMENT TOTALS PER PLATFORM
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. ENGAGEMENT TOTALS PER PLATFORM")
print("=" * 70)

# TikTok views from signals_social
tiktok_views = None
try:
    sig = sb.table("signals_social").select("total_views").eq("product_id", PID).eq("platform", "tiktok").execute().data
    if sig:
        tiktok_views = safe_sum(sig, "total_views")
except Exception as e:
    tiktok_views = f"err: {e}"

print(f"{'Platform':<15} {'Total Likes/Upvotes':>20} {'Total Views (tiktok)':>22}")
print("-" * 60)
for plat in platforms:
    posts = [r for r in posts_all if r["platform"] == plat and r.get("data_type") == "post"]
    upvotes = safe_sum(posts, "upvotes")
    views = fmt(tiktok_views) if plat == "tiktok" else "—"
    print(f"{plat:<15} {fmt(upvotes):>20} {views:>22}")

# ═══════════════════════════════════════════════════════════════════
# 4. DATE RANGES PER PLATFORM
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. DATE RANGES PER PLATFORM")
print("=" * 70)

posts_dates = sb.table("posts").select("platform,scraped_date").eq("product_id", PID).execute().data

print(f"{'Platform':<15} {'First Data':>12} {'Latest Data':>13} {'Days Span':>11}")
print("-" * 55)
for plat in platforms:
    dates = [r["scraped_date"] for r in posts_dates if r["platform"] == plat and r.get("scraped_date")]
    if dates:
        mn = min(dates)[:10]
        mx = max(dates)[:10]
        span = (datetime.strptime(mx, "%Y-%m-%d") - datetime.strptime(mn, "%Y-%m-%d")).days
        print(f"{plat:<15} {mn:>12} {mx:>13} {span:>11}")
    else:
        print(f"{plat:<15} {'—':>12} {'—':>13} {'—':>11}")

# ═══════════════════════════════════════════════════════════════════
# 5. MOST RECENT RUN
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. MOST RECENT RUN")
print("=" * 70)

print(f"{'Platform':<15} {'Latest Date':>12} {'New Posts':>10} {'Older Posts':>12} {'Dedup Skips':>13}")
print("-" * 65)
for plat in platforms:
    plat_dates = [r["scraped_date"] for r in posts_dates if r["platform"] == plat and r.get("scraped_date")]
    if not plat_dates:
        print(f"{plat:<15} {'—':>12} {'—':>10} {'—':>12} {'—':>13}")
        continue
    latest = max(plat_dates)[:10]
    new_count = sum(1 for d in plat_dates if d[:10] == latest)
    older_count = sum(1 for d in plat_dates if d[:10] != latest)
    # implied dedup skips = older posts not updated (same as older count)
    print(f"{plat:<15} {latest:>12} {fmt(new_count):>10} {fmt(older_count):>12} {fmt(older_count):>13}")

# Run IDs
print("\n── Run IDs (5 most recent) ──")
try:
    run_rows = sb.table("posts").select("run_id,scraped_date").eq("product_id", PID).not_.is_("run_id", "null").execute().data
    if run_rows:
        from collections import Counter
        rid_counts = Counter()
        rid_dates = {}
        for r in run_rows:
            rid = r["run_id"]
            rid_counts[rid] += 1
            d = (r.get("scraped_date") or "")[:10]
            if rid not in rid_dates or d > rid_dates[rid]:
                rid_dates[rid] = d
        top5 = rid_counts.most_common(50)
        top5.sort(key=lambda x: rid_dates.get(x[0], ""), reverse=True)
        top5 = top5[:5]
        print(f"  {'run_id':<40} {'Posts':>8} {'Date':>12}")
        print(f"  {'-'*40} {'-'*8} {'-'*12}")
        for rid, cnt in top5:
            print(f"  {rid:<40} {fmt(cnt):>8} {rid_dates.get(rid, '—'):>12}")
    else:
        print("  No run_id values found in posts table.")
except Exception as e:
    print(f"  Error querying run_id: {e}")

print("\n✓ Done — all queries read-only, no Apify calls.")
