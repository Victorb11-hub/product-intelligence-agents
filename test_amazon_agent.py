"""
Test the rebuilt Amazon agent — lean two-pass, no review scraping.
"""
import sys, os, uuid, time, re, logging

# ── 1. Load env from agents/.env, add project root to sys.path ──────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, "agents", ".env"))

# ── 2. Import AmazonAgent ───────────────────────────────────────────────
from agents.agent_amazon import AmazonAgent

# ── 3. Logging ──────────────────────────────────────────────────────────
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
logging.getLogger("agents.agent_amazon").setLevel(logging.INFO)
logging.getLogger("agents.scoring_engine").setLevel(logging.INFO)

# ── 4. Create agent instance ───────────────────────────────────────────
agent = AmazonAgent()
agent.run_id = str(uuid.uuid4())
print(f"Run ID: {agent.run_id}\n")

# ── 5. Product definition ──────────────────────────────────────────────
product = {
    "id": "f0620e1e-83fd-45c9-ac92-dd922e4c674c",
    "name": "Korean Sheet Masks",
    "keywords": ["korean sheet mask", "sheet mask", "k-beauty", "face mask", "skincare"],
}

# ── 6-8. Run scrape ────────────────────────────────────────────────────
print("=" * 60)
print("SCRAPING — Korean Sheet Masks (Amazon lean two-pass)")
print("=" * 60)

t0 = time.time()
result = agent.scrape(product["name"], product["keywords"], product)
elapsed = time.time() - t0
print(f"\nElapsed: {elapsed:.1f}s")

# ── 9. PASS 1 results ──────────────────────────────────────────────────
print("\n" + "=" * 60)
print("PASS 1 RESULTS")
print("=" * 60)
print(f"Products found total: {result.get('products_found', 0)}")
print(f"Top 5 kept: {result.get('mention_count', 0)}")
print()
raw_items = result.get("raw_items", [])
for i, item in enumerate(raw_items[:5], 1):
    title = (item.get("title") or item.get("name") or "?")[:50]
    asin = item.get("asin") or item.get("ASIN") or "?"
    rating = item.get("stars") or item.get("rating") or item.get("averageRating") or "?"
    rc = item.get("reviewsCount") or item.get("reviews_count") or item.get("numberOfReviews") or item.get("ratingsCount") or "?"
    price = item.get("price") or item.get("currentPrice") or "?"
    if isinstance(price, dict):
        price = price.get("value") or price.get("amount") or "?"
    print(f"  {i}. {title}")
    print(f"     ASIN: {asin} | Rating: {rating} | Reviews: {rc} | Price: {price}")

# ── 10. PASS 2 results ─────────────────────────────────────────────────
print("\n" + "=" * 60)
print("PASS 2 RESULTS (BSR)")
print("=" * 60)
print(f"best_bsr_rank:     {result.get('bestseller_rank', 'N/A')}")
print(f"best_bsr_category: {result.get('best_bsr_category', 'N/A')}")
print(f"bsr_change:        {result.get('bsr_change', 0)}")
print(f"bsr_trend:         {result.get('bsr_trend', 'unknown')}")

# ── 11. TREND CALCULATIONS ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("TREND CALCULATIONS")
print("=" * 60)
print(f"review_velocity:      {result.get('review_velocity', 0)}")
print(f"rating_trend:         {result.get('rating_trend', 'unknown')}")
print(f"review_count_growth:  {result.get('review_count_growth', 0)}")

# ── 12. Write signal row ───────────────────────────────────────────────
print("\n" + "=" * 60)
print("SIGNAL ROW -> signals_retail")
print("=" * 60)
signal_row = agent.build_signal_row(result, product["id"])
print("Signal row fields:")
for k, v in signal_row.items():
    print(f"  {k}: {v}")

db = agent.supabase
inserted = False
row_to_insert = dict(signal_row)
for attempt in range(5):
    try:
        db.table("signals_retail").insert(row_to_insert).execute()
        print("\nInsert: SUCCESS")
        inserted = True
        break
    except Exception as e:
        err = str(e)
        print(f"\nInsert attempt {attempt+1} failed: {err[:200]}")
        col_match = re.search(r'column "(\w+)"', err)
        if col_match:
            bad_col = col_match.group(1)
            print(f"  -> Removing column: {bad_col}")
            row_to_insert.pop(bad_col, None)
        else:
            print("  -> Could not identify bad column, stopping retries.")
            break

if not inserted:
    print("Insert: FAILED after retries")

# ── 13. Scoring engine ─────────────────────────────────────────────────
print("\n" + "=" * 60)
print("SCORING ENGINE")
print("=" * 60)
from agents.scoring_engine import score_all_products
score_all_products(db, [product], "amazon-lean-test")

try:
    prod_resp = db.table("products").select(
        "current_score, raw_score, coverage_pct, current_verdict, active_jobs"
    ).eq("id", product["id"]).execute()
    if prod_resp.data:
        p = prod_resp.data[0]
        print(f"current_score:   {p.get('current_score')}")
        print(f"raw_score:       {p.get('raw_score')}")
        print(f"coverage_pct:    {p.get('coverage_pct')}")
        print(f"current_verdict: {p.get('current_verdict')}")
        print(f"active_jobs:     {p.get('active_jobs')}")
    else:
        print("Product not found in DB.")
except Exception as e:
    print(f"Failed to read scores: {e}")

# ── 14. Cost estimate ──────────────────────────────────────────────────
print("\n" + "=" * 60)
print("COST ESTIMATE")
print("=" * 60)
pass1_items = result.get("products_found", 0)
pass2_items = result.get("mention_count", 0)
total_items = pass1_items + pass2_items
cost = total_items * 4.0 / 1000
print(f"Pass 1 items: {pass1_items}")
print(f"Pass 2 items: {pass2_items}")
print(f"Total items:  {total_items}")
print(f"Est. cost:    ${cost:.4f}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
