"""One-shot scoring run: score all active products and print results."""
import sys, os, logging

# 1. Add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

# 2. Load env from agents/.env
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, "agents", ".env"))

# 3. Import scoring engine
from agents.scoring_engine import score_all_products

# 4. Get Supabase client
from agents.config import get_supabase
db = get_supabase()

# 5. Set logging to INFO for agents.scoring_engine
logging.basicConfig(level=logging.WARNING, format="%(name)s | %(levelname)s | %(message)s")
logging.getLogger("agents.scoring_engine").setLevel(logging.INFO)

# 6. Query active products
print("=" * 60)
print("Fetching active products...")
resp = db.table("products").select("*").eq("active", True).execute()
products = resp.data or []
print(f"Found {len(products)} active products\n")

for p in products:
    print(f"  - {p['name']} ({p['id'][:8]}...)")
print()

# 7. Run scoring
RUN_ID = "eng-norm-fix"
print("=" * 60)
print(f"Running score_all_products(db, products, '{RUN_ID}')")
print("=" * 60)
score_all_products(db, products, RUN_ID)
print()

# 8. Query and print results for ALL scored products
print("=" * 60)
print("Updated Product Scores")
print("=" * 60)

for p in products:
    row = db.table("products").select(
        "name, current_score, raw_score, coverage_pct, current_verdict, lifecycle_phase"
    ).eq("id", p["id"]).execute()

    if row.data:
        r = row.data[0]
        print(f"\n  {r['name']}")
        print(f"    Current Score:   {r['current_score']}")
        print(f"    Raw Score:       {r['raw_score']}")
        print(f"    Coverage %:      {r['coverage_pct']}")
        print(f"    Verdict:         {r['current_verdict']}")
        print(f"    Lifecycle Phase: {r['lifecycle_phase']}")

print()
print("Done.")
