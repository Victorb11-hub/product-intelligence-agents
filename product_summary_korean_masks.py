"""
Comprehensive Supabase data summary for Korean Sheet Masks.
Pure DB queries — NO Apify calls.
"""
import io
import os
import sys
import datetime as dt

# Force UTF-8 stdout on Windows so emoji/CJK in titles don't crash printing
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ---- Manual .env load (utf-8) ------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(__file__), "agents", ".env")
with io.open(ENV_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        # strip optional surrounding quotes
        v = v.strip().strip('"').strip("'")
        os.environ[k.strip()] = v

# Make sure we can import agents.config
sys.path.insert(0, os.path.dirname(__file__))
from agents.config import get_supabase  # noqa: E402

PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"
LINE = "=" * 60

db = get_supabase()


def section(title: str) -> None:
    print()
    print(LINE)
    print(title)
    print(LINE)


def fmt_num(n) -> str:
    if n is None:
        return "-"
    if isinstance(n, bool):
        return str(n)
    if isinstance(n, (int,)):
        return f"{n:,}"
    if isinstance(n, float):
        if n != n:  # NaN
            return "-"
        return f"{n:,.2f}"
    return str(n)


def safe_get(d, key, default=None):
    if not d:
        return default
    return d.get(key, default)


def count_rows(table: str, filters=None) -> int:
    """Return exact row count by paging through ids (the postgrest client's
    count='exact' returns None because Prefer headers conflict). Page size 1000."""
    total = 0
    page_size = 1000
    page = 0
    try:
        while True:
            q = db.table(table).select("id")
            for col, op, val in (filters or []):
                if op == "eq":
                    q = q.eq(col, val)
                elif op == "neq":
                    q = q.neq(col, val)
                elif op == "is_":
                    q = q.is_(col, val)
                elif op == "gte":
                    q = q.gte(col, val)
            q = q.range(page * page_size, (page + 1) * page_size - 1)
            data = q.execute().data or []
            total += len(data)
            if len(data) < page_size:
                break
            page += 1
            if page > 200:  # safety cap = 200k rows
                break
        return total
    except Exception:
        return -1


def fetch_one(table: str, filters=None, columns: str = "*"):
    q = db.table(table).select(columns)
    for col, op, val in (filters or []):
        if op == "eq":
            q = q.eq(col, val)
    resp = q.limit(1).execute()
    return resp.data[0] if resp.data else None


# =========================================================================
# 1. SCORES
# =========================================================================
section("1. SCORES")

product = fetch_one("products", [("id", "eq", PRODUCT_ID)])

if not product:
    print(f"Product {PRODUCT_ID} not found.")
else:
    print(f"Product: {product.get('name')}  |  category: {product.get('category')}")
    print()
    score_fields = [
        "current_score", "raw_score", "current_verdict",
        "coverage_pct", "active_jobs", "total_jobs",
        "lifecycle_phase", "fad_flag",
        "confidence_level", "confidence_reason",
        "total_comments_scored", "active_platform_count",
        "last_scraped_at", "first_scraped_at", "total_runs", "backfill_completed",
    ]
    label_w = max(len(k) for k in score_fields)
    for k in score_fields:
        v = product.get(k, "<column missing>")
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            v_disp = fmt_num(v)
        else:
            v_disp = v if v is not None else "-"
        print(f"  {k:<{label_w}} : {v_disp}")
    if "active_jobs" in product and "total_jobs" in product:
        print(f"  {'jobs (active/total)':<{label_w}} : "
              f"{product.get('active_jobs')} / {product.get('total_jobs')}")

# scores_history (last 10)
print()
print("Scores history (last 10):")
hist_cols = "scored_date,composite_score,verdict,score_change"
try:
    hist = (
        db.table("scores_history")
        .select(hist_cols)
        .eq("product_id", PRODUCT_ID)
        .order("scored_date", desc=True)
        .limit(10)
        .execute()
    ).data or []
except Exception as e:
    hist = []
    print(f"  (error fetching scores_history: {e})")

if not hist:
    print("  (no entries)")
else:
    print(f"  {'date':<12}  {'composite':>10}  {'verdict':<8}  {'change':>10}")
    print(f"  {'-'*12}  {'-'*10}  {'-'*8}  {'-'*10}")
    for r in hist:
        print(f"  {str(r.get('scored_date') or ''):<12}  "
              f"{fmt_num(r.get('composite_score')):>10}  "
              f"{str(r.get('verdict') or '-'):<8}  "
              f"{fmt_num(r.get('score_change')):>10}")


# =========================================================================
# 2. PLATFORM COVERAGE
# =========================================================================
section("2. PLATFORM COVERAGE")

PLATFORMS = [
    ("reddit",        "signals_social"),
    ("tiktok",        "signals_social"),
    ("instagram",     "signals_social"),
    ("amazon",        "signals_retail"),
    ("google_trends", "signals_search"),
]

print(f"  {'platform':<14}  {'signals_table':<18}  {'last_scraped':<12}  "
      f"{'posts':>10}  {'comments':>10}")
print(f"  {'-'*14}  {'-'*18}  {'-'*12}  {'-'*10}  {'-'*10}")

for plat, sig_table in PLATFORMS:
    # latest scraped_date from signals table
    last_date = "-"
    try:
        r = (
            db.table(sig_table)
            .select("scraped_date")
            .eq("product_id", PRODUCT_ID)
            .eq("platform", plat)
            .order("scraped_date", desc=True)
            .limit(1)
            .execute()
        ).data
        if r:
            last_date = str(r[0].get("scraped_date") or "-")
    except Exception:
        pass

    posts = count_rows("posts", [("product_id", "eq", PRODUCT_ID), ("platform", "eq", plat)])
    comments = count_rows("comments", [("product_id", "eq", PRODUCT_ID), ("platform", "eq", plat)])

    print(f"  {plat:<14}  {sig_table:<18}  {last_date:<12}  "
          f"{fmt_num(posts):>10}  {fmt_num(comments):>10}")


# =========================================================================
# 3. SIGNAL SUMMARY
# =========================================================================
section("3. SIGNAL SUMMARY")

# Aggregate by paging through comments table for this product
def fetch_all_comments(filters_extra=None):
    """Page through comments for this product. Returns list of dicts."""
    rows = []
    page = 0
    page_size = 1000
    cols = ("platform,is_buy_intent,is_problem_language,is_repeat_purchase,"
            "intent_level,sentiment_score,upvotes")
    while True:
        q = (
            db.table("comments")
            .select(cols)
            .eq("product_id", PRODUCT_ID)
        )
        for c, o, v in (filters_extra or []):
            q = q.eq(c, v) if o == "eq" else q
        q = q.range(page * page_size, (page + 1) * page_size - 1)
        try:
            data = q.execute().data or []
        except Exception as e:
            print(f"  (error paging comments: {e})")
            break
        if not data:
            break
        rows.extend(data)
        if len(data) < page_size:
            break
        page += 1
    return rows


all_comments = fetch_all_comments()


def summarize(comments):
    total = len(comments)
    buy = sum(1 for c in comments if c.get("is_buy_intent"))
    neg = sum(1 for c in comments if c.get("is_problem_language"))
    repeat = sum(1 for c in comments if c.get("is_repeat_purchase"))
    intent_vals = [c["intent_level"] for c in comments if c.get("intent_level") is not None]
    sent_vals = [c["sentiment_score"] for c in comments if c.get("sentiment_score") is not None]
    avg_intent = sum(intent_vals) / len(intent_vals) if intent_vals else None
    avg_sent = sum(sent_vals) / len(sent_vals) if sent_vals else None
    neg_ratio = (neg / buy) if buy else None
    return total, buy, neg, repeat, avg_intent, avg_sent, neg_ratio


total, buy, neg, repeat, avg_intent, avg_sent, neg_ratio = summarize(all_comments)
print(f"  Total comments               : {fmt_num(total)}")
print(f"  is_buy_intent (purchase)     : {fmt_num(buy)}")
print(f"  is_problem_language (neg)    : {fmt_num(neg)}")
print(f"  is_repeat_purchase           : {fmt_num(repeat)}")
print(f"  Avg intent_level             : {fmt_num(avg_intent)}")
print(f"  Avg sentiment_score          : {fmt_num(avg_sent)}")
print(f"  Negative ratio (neg/buy)     : "
      f"{'-' if neg_ratio is None else f'{neg_ratio:.2f}'}")

print()
print("Per-platform breakdown:")
print(f"  {'platform':<12}  {'total':>8}  {'buy':>8}  {'neg':>8}  {'repeat':>8}  "
      f"{'avg_int':>8}  {'avg_sent':>9}  {'neg/buy':>8}")
print(f"  {'-'*12}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*9}  {'-'*8}")

platforms_seen = sorted({c.get("platform", "") for c in all_comments if c.get("platform")})
for plat in platforms_seen:
    sub = [c for c in all_comments if c.get("platform") == plat]
    t, b, n, rp, ai, asent, nr = summarize(sub)
    print(f"  {plat:<12}  {fmt_num(t):>8}  {fmt_num(b):>8}  {fmt_num(n):>8}  "
          f"{fmt_num(rp):>8}  "
          f"{('-' if ai is None else f'{ai:.2f}'):>8}  "
          f"{('-' if asent is None else f'{asent:.2f}'):>9}  "
          f"{('-' if nr is None else f'{nr:.2f}'):>8}")


# =========================================================================
# 4. TOP CONTENT
# =========================================================================
section("4. TOP CONTENT")

SOCIAL = ["reddit", "tiktok", "instagram"]
for plat in SOCIAL:
    print()
    print(f"  Top 3 {plat} posts (data_type='post', by upvotes):")
    try:
        posts = (
            db.table("posts")
            .select("post_title,post_body,post_url,upvotes,comment_count,data_type")
            .eq("product_id", PRODUCT_ID)
            .eq("platform", plat)
            .eq("data_type", "post")
            .order("upvotes", desc=True)
            .limit(3)
            .execute()
        ).data or []
    except Exception as e:
        # fallback: maybe data_type column doesn't exist for some platforms; try without it
        try:
            posts = (
                db.table("posts")
                .select("post_title,post_body,post_url,upvotes,comment_count")
                .eq("product_id", PRODUCT_ID)
                .eq("platform", plat)
                .order("upvotes", desc=True)
                .limit(3)
                .execute()
            ).data or []
        except Exception as e2:
            posts = []
            print(f"    (error: {e2})")
    if not posts:
        print("    (no posts)")
        continue
    for i, p in enumerate(posts, 1):
        title = p.get("post_title") or (p.get("post_body") or "")[:60]
        url = p.get("post_url") or "-"
        print(f"    {i}. {title}")
        print(f"       url: {url}")
        print(f"       upvotes: {fmt_num(p.get('upvotes'))}  |  "
              f"comments: {fmt_num(p.get('comment_count'))}")

print()
print("  Top 5 PURCHASE-SIGNAL comments (is_buy_intent=TRUE, all platforms):")
try:
    buys = (
        db.table("comments")
        .select("comment_body,platform,upvotes,intent_level")
        .eq("product_id", PRODUCT_ID)
        .eq("is_buy_intent", True)
        .order("upvotes", desc=True)
        .limit(5)
        .execute()
    ).data or []
except Exception as e:
    buys = []
    print(f"    (error: {e})")

if not buys:
    print("    (none)")
else:
    for i, c in enumerate(buys, 1):
        body = (c.get("comment_body") or "").replace("\n", " ")[:150]
        print(f"    {i}. [{c.get('platform')}] upvotes={fmt_num(c.get('upvotes'))}  "
              f"intent={c.get('intent_level')}")
        print(f"       {body}")

print()
print("  Top 5 NEGATIVE comments (is_problem_language=TRUE) — WARNING FLAGS:")
try:
    negs = (
        db.table("comments")
        .select("comment_body,platform,upvotes,sentiment_score")
        .eq("product_id", PRODUCT_ID)
        .eq("is_problem_language", True)
        .order("upvotes", desc=True)
        .limit(5)
        .execute()
    ).data or []
except Exception as e:
    negs = []
    print(f"    (error: {e})")

if not negs:
    print("    (none)")
else:
    for i, c in enumerate(negs, 1):
        body = (c.get("comment_body") or "").replace("\n", " ")[:150]
        sent = c.get("sentiment_score")
        sent_disp = "-" if sent is None else f"{sent:.2f}"
        print(f"    {i}. [{c.get('platform')}] upvotes={fmt_num(c.get('upvotes'))}  "
              f"sentiment={sent_disp}")
        print(f"       {body}")


# =========================================================================
# 5. PIPELINE HISTORY
# =========================================================================
section("5. PIPELINE HISTORY (last 20)")

pipeline_cols = ("started_at,run_type,status,duration_seconds,"
                 "products_processed,total_posts_found,total_comments_pulled,"
                 "total_new_records,total_dedup_skips")
try:
    runs = (
        db.table("pipeline_runs")
        .select(pipeline_cols)
        .order("started_at", desc=True)
        .limit(20)
        .execute()
    ).data or []
except Exception as e:
    # Fallback: select * and pick out matching keys
    print(f"  (narrow select failed: {e}; falling back to select *)")
    try:
        runs = (
            db.table("pipeline_runs")
            .select("*")
            .order("started_at", desc=True)
            .limit(20)
            .execute()
        ).data or []
    except Exception as e2:
        runs = []
        print(f"  (error: {e2})")

if not runs:
    print("  (no pipeline runs)")
else:
    hdr = (f"  {'started_at':<22}  {'run_type':<10}  {'status':<10}  "
           f"{'dur_s':>7}  {'prods':>5}  {'posts':>7}  {'comments':>9}  "
           f"{'new':>7}  {'dedup':>7}")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for r in runs:
        started = (r.get("started_at") or "")[:22]
        print(f"  {started:<22}  "
              f"{str(r.get('run_type') or '-'):<10}  "
              f"{str(r.get('status') or '-'):<10}  "
              f"{fmt_num(r.get('duration_seconds')):>7}  "
              f"{fmt_num(r.get('products_processed')):>5}  "
              f"{fmt_num(r.get('total_posts_found')):>7}  "
              f"{fmt_num(r.get('total_comments_pulled')):>9}  "
              f"{fmt_num(r.get('total_new_records')):>7}  "
              f"{fmt_num(r.get('total_dedup_skips')):>7}")


# =========================================================================
# 6. RAW TABLE COUNTS
# =========================================================================
section("6. RAW TABLE COUNTS")

rows = []

# products
total_products = count_rows("products")
active_products = count_rows("products", [("active", "eq", True)])
rows.append(("products", total_products, f"active={fmt_num(active_products)}"))

# signals tables
for tbl in ("signals_social", "signals_retail", "signals_search",
            "signals_supply", "signals_discovery"):
    tot = count_rows(tbl)
    this = count_rows(tbl, [("product_id", "eq", PRODUCT_ID)])
    rows.append((tbl, tot, f"this_product={fmt_num(this)}"))

# posts / comments
for tbl in ("posts", "comments"):
    tot = count_rows(tbl)
    this = count_rows(tbl, [("product_id", "eq", PRODUCT_ID)])
    rows.append((tbl, tot, f"this_product={fmt_num(this)}"))

# pipeline_runs
tot = count_rows("pipeline_runs")
done = count_rows("pipeline_runs", [("status", "eq", "completed")])
rows.append(("pipeline_runs", tot, f"completed={fmt_num(done)}"))

# agent_runs
tot = count_rows("agent_runs")
rows.append(("agent_runs", tot, ""))

# alerts
tot = count_rows("alerts")
unactioned = count_rows("alerts", [("actioned", "eq", False)])
rows.append(("alerts", tot, f"unactioned={fmt_num(unactioned)}"))

# council_verdicts
tot = count_rows("council_verdicts")
this = count_rows("council_verdicts", [("product_id", "eq", PRODUCT_ID)])
rows.append(("council_verdicts", tot, f"this_product={fmt_num(this)}"))

# product_hashtags
tot = count_rows("product_hashtags")
this = count_rows("product_hashtags", [("product_id", "eq", PRODUCT_ID)])
rows.append(("product_hashtags", tot, f"this_product={fmt_num(this)}"))

# scores_history
tot = count_rows("scores_history")
this = count_rows("scores_history", [("product_id", "eq", PRODUCT_ID)])
rows.append(("scores_history", tot, f"this_product={fmt_num(this)}"))

print(f"  {'table':<22}  {'total':>10}  detail")
print(f"  {'-'*22}  {'-'*10}  {'-'*30}")
for name, tot, detail in rows:
    tot_disp = fmt_num(tot) if tot >= 0 else "ERR"
    print(f"  {name:<22}  {tot_disp:>10}  {detail}")

print()
print(LINE)
print("END OF REPORT")
print(LINE)
