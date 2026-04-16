import sys, os, uuid, time, traceback
from datetime import datetime

PROJECT_ROOT = r"c:\Users\vibraca\OneDrive - Evolution Equities LLC\Personal\Business\(1) Claude\Claude Code\Social Media Scraper"
sys.path.insert(0, PROJECT_ROOT)

# Load env from agents/.env utf-8
env_file = os.path.join(PROJECT_ROOT, "agents", ".env")
with open(env_file, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

# Tee output to both file and console
class Tee:
    def __init__(self, *files): self.files = files
    def write(self, s):
        for f in self.files:
            try:
                f.write(s); f.flush()
            except Exception:
                pass
    def flush(self):
        for f in self.files:
            try:
                f.flush()
            except Exception:
                pass

logf = open(os.path.join(PROJECT_ROOT, "step12_test.log"), "w", encoding="utf-8")
sys.stdout = Tee(sys.__stdout__, logf)
sys.stderr = Tee(sys.__stderr__, logf)

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])

from agents.config import get_supabase
PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"
db = get_supabase()
products = db.table("products").select("*").eq("id", PRODUCT_ID).execute().data
if not products:
    print("ERROR: Product not found"); sys.exit(1)
product = products[0]

# --- BASELINE ---
print("=" * 60); print("BASELINE"); print("=" * 60)
baseline = {
    "score": product.get("current_score"),
    "raw_score": product.get("raw_score"),
    "verdict": product.get("current_verdict"),
    "confidence": product.get("confidence_level"),
    "confidence_reason": product.get("confidence_reason"),
    "total_comments": product.get("total_comments_scored"),
    "platforms": product.get("active_platform_count"),
    "last_scraped": product.get("last_scraped_at"),
    "posts_count": len(db.table("posts").select("id").eq("product_id", PRODUCT_ID).execute().data or []),
    "comments_count": len(db.table("comments").select("id").eq("product_id", PRODUCT_ID).execute().data or []),
}
for k, v in baseline.items(): print(f"  {k}: {v}")

# --- RUN AGENTS one at a time with full progress ---
run_id = str(uuid.uuid4())
print(f"\nrun_id: {run_id}\n")

results = {}
start_total = time.time()

agents_to_run = [
    ("reddit",    "agents.agent_reddit",    "RedditAgent"),
    ("tiktok",    "agents.agent_tiktok",    "TikTokAgent"),
    ("instagram", "agents.agent_instagram", "InstagramAgent"),
    ("amazon",    "agents.agent_amazon",    "AmazonAgent"),
]

for name, mod_path, cls_name in agents_to_run:
    print(f"\n{'=' * 60}\n{name.upper()} STARTING\n{'=' * 60}")
    sys.stdout.flush()
    t0 = time.time()
    try:
        import importlib
        mod = importlib.import_module(mod_path)
        AgentClass = getattr(mod, cls_name)
        agent = AgentClass()
        agent.run_id = run_id
        result = agent.scrape(product["name"], product.get("keywords", []), product)
        # Write signal row
        try:
            signal_row = agent.build_signal_row(result, product["id"])
            db.table(agent.SIGNAL_TABLE).insert(signal_row).execute()
            print(f"  [{name}] signal row inserted")
        except Exception as e:
            print(f"  [{name}] signal row insert: {str(e)[:200]}")
        elapsed = time.time() - t0
        results[name] = {
            "status": "ok",
            "duration": round(elapsed, 1),
            "pass1_total": result.get("pass1_total", 0),
            "pass1_passed": result.get("pass1_passed", 0),
            "pass2_comments": result.get("pass2_comments", 0),
            "purchase_signals": result.get("purchase_signals", 0),
            "negative_signals": result.get("negative_signals", 0),
        }
        print(f"\n[{name}] DONE in {elapsed:.1f}s -- {result.get('pass1_total',0)} found, {result.get('pass1_passed',0)} passed, {result.get('pass2_comments',0)} comments, {result.get('purchase_signals',0)} purchase, {result.get('negative_signals',0)} negative")
    except Exception as e:
        elapsed = time.time() - t0
        tb = traceback.format_exc()
        results[name] = {"status": "failed", "error": str(e)[:200], "duration": round(elapsed, 1)}
        print(f"\n[{name}] FAILED in {elapsed:.1f}s: {str(e)[:200]}")
        print(tb[:2000])
    sys.stdout.flush()

# --- SCORING ---
print(f"\n{'=' * 60}\nSCORING\n{'=' * 60}")
try:
    from agents.scoring_engine import score_all_products
    score_all_products(db, [product], run_id)
    print("  Scoring complete")
except Exception as e:
    print(f"  Scoring failed: {e}")
    traceback.print_exc()

# --- POST-RUN STATE ---
print(f"\n{'=' * 60}\nPOST-RUN STATE\n{'=' * 60}")
products = db.table("products").select("*").eq("id", PRODUCT_ID).execute().data
post = products[0]
after = {
    "score": post.get("current_score"),
    "raw_score": post.get("raw_score"),
    "verdict": post.get("current_verdict"),
    "confidence": post.get("confidence_level"),
    "confidence_reason": post.get("confidence_reason"),
    "total_comments": post.get("total_comments_scored"),
    "platforms": post.get("active_platform_count"),
    "last_scraped": post.get("last_scraped_at"),
    "posts_count": len(db.table("posts").select("id").eq("product_id", PRODUCT_ID).execute().data or []),
    "comments_count": len(db.table("comments").select("id").eq("product_id", PRODUCT_ID).execute().data or []),
}

print(f"\nCOMPARISON")
print(f"{'Field':<22} {'Before':<20} {'After':<20} Delta")
for k in baseline:
    b, a = baseline[k], after[k]
    if isinstance(b, (int, float)) and isinstance(a, (int, float)):
        delta = a - b
        print(f"{k:<22} {str(b):<20} {str(a):<20} {delta:+}")
    else:
        delta = "same" if b == a else f"{b} -> {a}"
        print(f"{k:<22} {str(b)[:18]:<20} {str(a)[:18]:<20} {delta}")

# --- PER-PLATFORM RESULTS ---
print(f"\n{'=' * 60}\nPER-PLATFORM RESULTS\n{'=' * 60}")
for name, r in results.items():
    print(f"  {name}: {r}")

# --- SIGNAL REPORTS ---
print(f"\n{'=' * 60}\nSIGNAL REPORTS\n{'=' * 60}")
for name, mod_path, cls_name in agents_to_run[:3]:  # just social platforms
    try:
        import importlib
        mod = importlib.import_module(mod_path)
        AgentClass = getattr(mod, cls_name)
        agent = AgentClass()
        agent.run_id = run_id
        rep = agent.generate_signal_report(PRODUCT_ID, name)
        print(f"\n[{name}] quality={rep['signal_quality']} purchase={rep['purchase_signals']} negative={rep['negative_signals']} question={rep['question_signals']} ratio={rep['negative_ratio']:.2%} penalty={rep['penalty_applied']}")
        for c in rep["top_purchase_comments"][:3]:
            print(f"    + {c['text'][:90]}  (score={c['score']:.2f})")
        for c in rep["top_negative_comments"][:3]:
            print(f"    - {c['text'][:90]}  (sent={c['score']:.2f})")
    except Exception as e:
        print(f"  [{name}] report failed: {e}")

# --- FINAL SUMMARY ---
total_dur = time.time() - start_total
print(f"\n{'=' * 60}\nFINAL SUMMARY\n{'=' * 60}")
print(f"Total duration: {total_dur:.1f}s ({total_dur/60:.1f} min)")
print(f"Score: {baseline['score']} -> {after['score']}")
print(f"Confidence: {baseline['confidence']} -> {after['confidence']}")
print(f"New posts: {after['posts_count'] - baseline['posts_count']}")
print(f"New comments: {after['comments_count'] - baseline['comments_count']}")
print(f"Verdict: {baseline['verdict']} -> {after['verdict']}")

logf.close()
print("\nDONE")
