import sys, httpx, json
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
url = "https://yvzuyaemgrlhpsnfqjqv.supabase.co"
key = "sb_publishable_2i7akLDL_eQo5MUlv6lSyw_RMNnh3r7"
h = {"apikey": key, "Authorization": f"Bearer {key}"}

p = httpx.get(f"{url}/rest/v1/products?select=*&name=eq.Korean%20Sheet%20Masks", headers=h).json()[0]
pid = p["id"]

print("=" * 70)
print("PRODUCT: Korean Sheet Masks")
print("=" * 70)
print(f"Category:    {p['category']}")
print(f"Keywords:    {p['keywords']}")
print(f"Subreddits:  {p.get('target_subreddits', [])}")
print(f"Score:       {p['current_score']}")
print(f"Verdict:     {p['current_verdict']}")
print(f"Phase:       {p['lifecycle_phase']}")
print(f"Fad flag:    {p['fad_flag']}")

# Reddit signal
print()
print("=" * 70)
print("REDDIT SIGNAL (latest run)")
print("=" * 70)
sig = httpx.get(f"{url}/rest/v1/signals_social?select=*&product_id=eq.{pid}&platform=eq.reddit&order=scraped_date.desc&limit=1", headers=h).json()
s = sig[0] if sig else {}
run_id = s.get("run_id")
print(f"Mentions:        {s.get('mention_count')}")
print(f"Total upvotes:   {s.get('total_upvotes')}")
print(f"Total comments:  {s.get('total_comment_count')}")
print(f"Sentiment:       {s.get('sentiment_score')}")
print(f"Velocity:        {s.get('velocity')}")
print(f"Avg intent:      {s.get('avg_intent_score')}")
print(f"High intent:     {s.get('high_intent_comment_count')}")
print(f"Buy intent:      {s.get('buy_intent_comment_count')}")
print(f"Problem lang:    {s.get('problem_language_comment_count')}")
print(f"Repeat %:        {s.get('repeat_purchase_pct')}")
print(f"Fad / Lasting:   {s.get('fad_score')} / {s.get('lasting_score')}")
print(f"Quality:         {s.get('data_quality_score')}")

# Google Trends
print()
print("=" * 70)
print("GOOGLE TRENDS")
print("=" * 70)
gt = httpx.get(f"{url}/rest/v1/signals_search?select=*&product_id=eq.{pid}&platform=eq.google_trends&limit=1", headers=h).json()
if gt:
    g = gt[0]
    d = "RISING" if g["slope_24m"] > 0.005 else "DECLINING" if g["slope_24m"] < -0.005 else "FLAT"
    print(f"Slope:           {g['slope_24m']} ({d})")
    print(f"YoY growth:      {g['yoy_growth']:.0%}")
    print(f"Breakout:        {g['breakout_flag']}")
    print(f"Seasonal:        {g['seasonal_pattern']}")
    print(f"News trigger:    {g['news_trigger_flag']}")

# Posts by subreddit
print()
print("=" * 70)
print("POSTS BY SUBREDDIT")
print("=" * 70)
if run_id:
    posts = httpx.get(f"{url}/rest/v1/posts?select=subreddit,upvotes,intent_level,relevance_score&run_id=eq.{run_id}&data_type=eq.post", headers=h).json()
    subs = {}
    for post in posts:
        sub = post.get("subreddit", "?")
        if sub not in subs:
            subs[sub] = {"count": 0, "upvotes": 0, "hi": 0}
        subs[sub]["count"] += 1
        subs[sub]["upvotes"] += post.get("upvotes", 0) or 0
        subs[sub]["hi"] += 1 if (post.get("intent_level") or 1) >= 4 else 0
    for sub, d in sorted(subs.items(), key=lambda x: x[1]["count"], reverse=True):
        print(f"  {sub:30s} {d['count']:>4} posts  {d['upvotes']:>6} upvotes  {d['hi']:>2} high intent")
    print(f"  {'TOTAL':30s} {len(posts):>4}")

# Top 10 posts
print()
print("=" * 70)
print("TOP 10 POSTS BY UPVOTES")
print("=" * 70)
if run_id:
    top = httpx.get(f"{url}/rest/v1/posts?select=post_title,upvotes,intent_level,subreddit,relevance_score&run_id=eq.{run_id}&data_type=eq.post&order=upvotes.desc&limit=10", headers=h).json()
    for i, t in enumerate(top, 1):
        rel = f"{(t.get('relevance_score') or 0)*100:.0f}%"
        title = (t.get("post_title") or "")[:55]
        print(f"  {i:>2}. [{t['upvotes']:>5}pts] L{t.get('intent_level',1)} rel={rel:>4} {t.get('subreddit',''):20s} {title}")

# Comment analysis
print()
print("=" * 70)
print("COMMENT ANALYSIS (all stored comments)")
print("=" * 70)
comments = httpx.get(f"{url}/rest/v1/comments?select=intent_level,is_buy_intent,is_problem_language,is_repeat_purchase,sentiment_score&product_id=eq.{pid}", headers=h).json()
total = len(comments)
if total:
    buy = sum(1 for c in comments if c.get("is_buy_intent"))
    prob = sum(1 for c in comments if c.get("is_problem_language"))
    repeat = sum(1 for c in comments if c.get("is_repeat_purchase"))
    l4 = sum(1 for c in comments if (c.get("intent_level") or 1) >= 4)
    l5 = sum(1 for c in comments if (c.get("intent_level") or 1) >= 5)
    avg_s = sum(c.get("sentiment_score", 0) or 0 for c in comments) / total
    print(f"  Total:          {total}")
    print(f"  Avg sentiment:  {avg_s:+.3f}")
    print(f"  Buy intent:     {buy} ({buy/total*100:.1f}%)")
    print(f"  Problem lang:   {prob} ({prob/total*100:.1f}%)")
    print(f"  Repeat purch:   {repeat} ({repeat/total*100:.1f}%)")
    print(f"  High intent:    {l4} ({l4/total*100:.1f}%)")
    print(f"  Purchase (L5):  {l5} ({l5/total*100:.1f}%)")
    print()
    dist = {}
    for c in comments:
        lvl = c.get("intent_level", 1) or 1
        dist[lvl] = dist.get(lvl, 0) + 1
    labels = {1: "Awareness", 2: "Interest", 3: "Consideration", 4: "Intent", 5: "Purchase"}
    for lvl in sorted(dist):
        bar = "#" * (dist[lvl] // 3)
        print(f"    L{lvl} {labels.get(lvl,'?'):15s} {dist[lvl]:>4} ({dist[lvl]/total*100:>5.1f}%) {bar}")

# Top buy intent comments
print()
print("=" * 70)
print("TOP BUY INTENT COMMENTS")
print("=" * 70)
bc = httpx.get(f"{url}/rest/v1/comments?select=comment_body,intent_level,is_repeat_purchase&product_id=eq.{pid}&is_buy_intent=eq.true&order=intent_level.desc&limit=5", headers=h).json()
for c in bc:
    flags = "BUY"
    if c.get("is_repeat_purchase"): flags += "+REPEAT"
    body = (c.get("comment_body") or "")[:100]
    print(f"  L{c['intent_level']} [{flags:12s}] {body}")

# Top problem comments
print()
print("TOP PROBLEM LANGUAGE COMMENTS")
pc = httpx.get(f"{url}/rest/v1/comments?select=comment_body,intent_level,sentiment_score&product_id=eq.{pid}&is_problem_language=eq.true&limit=5", headers=h).json()
for c in pc:
    body = (c.get("comment_body") or "")[:100]
    print(f"  L{c['intent_level']} sent={c.get('sentiment_score',0):+.2f} | {body}")

# Run stats
print()
print("=" * 70)
print("LAST RUN STATS")
print("=" * 70)
runs = httpx.get(f"{url}/rest/v1/agent_runs?select=status,duration_seconds,rows_written,apify_estimated_cost,irrelevant_posts_discarded,integrity_check_passed&platform=eq.reddit&order=created_at.desc&limit=1", headers=h).json()
if runs:
    r = runs[0]
    print(f"  Status:       {r['status']}")
    print(f"  Duration:     {r['duration_seconds']:.1f}s" if r.get("duration_seconds") else "  Duration:     --")
    print(f"  Posts stored:  {r['rows_written']}")
    print(f"  Discarded:    {r.get('irrelevant_posts_discarded', 0)}")
    print(f"  Apify cost:   ${r.get('apify_estimated_cost', 0):.2f}")
    print(f"  Integrity:    {'PASSED' if r.get('integrity_check_passed') else 'FAILED'}")
