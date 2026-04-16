"""Full rescore with 3-job breakdown logging."""
import sys, os, logging, io

# Fix Windows console encoding for Unicode arrows etc.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 1. Add project root to sys.path, load env from agents/.env
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, "agents", ".env"))

# 2. Import scoring engine
from agents.scoring_engine import score_all_products

# 3. Get Supabase client
from agents.config import get_supabase
db = get_supabase()

# 4. Enable INFO logging for agents.scoring_engine
logging.basicConfig(level=logging.WARNING, format="%(name)s | %(levelname)s | %(message)s")
scoring_logger = logging.getLogger("agents.scoring_engine")
scoring_logger.setLevel(logging.INFO)

# Capture scoring engine log lines
class LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []
    def emit(self, record):
        self.records.append(self.format(record))

capture = LogCapture()
capture.setFormatter(logging.Formatter("%(name)s | %(levelname)s | %(message)s"))
scoring_logger.addHandler(capture)

# 5. Query active products
print("=" * 60)
print("Fetching active products...")
resp = db.table("products").select("*").eq("active", True).execute()
products = resp.data or []
print(f"Found {len(products)} active products\n")

for p in products:
    print(f"  - {p['name']} ({p['id'][:8]}...)")
print()

# 6. Run scoring
RUN_ID = "full-rescore-3jobs"
print("=" * 60)
print(f"Running score_all_products(db, products, '{RUN_ID}')")
print("=" * 60)
score_all_products(db, products, RUN_ID)
print()

# 7. Print captured Job 1, Job 2, Job 3 breakdown lines
print("=" * 60)
print("Scoring Engine Log (Job Breakdowns)")
print("=" * 60)
for line in capture.records:
    print(f"  {line}")
print()

# 8. Query and print updated product rows
print("=" * 60)
print("Updated Product Scores")
print("=" * 60)

for p in products:
    row = db.table("products").select(
        "name, current_score, raw_score, coverage_pct, current_verdict, active_jobs, lifecycle_phase"
    ).eq("id", p["id"]).execute()

    if row.data:
        r = row.data[0]
        print(f"\n  {r['name']}")
        print(f"    Current Score:   {r['current_score']}")
        print(f"    Raw Score:       {r['raw_score']}")
        print(f"    Coverage %:      {r['coverage_pct']}")
        print(f"    Verdict:         {r['current_verdict']}")
        print(f"    Active Jobs:     {r['active_jobs']}")
        print(f"    Lifecycle Phase: {r['lifecycle_phase']}")

print("\nDone.")
