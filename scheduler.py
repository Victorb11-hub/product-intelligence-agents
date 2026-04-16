"""
Nightly Pipeline Scheduler
===========================
1:00 AM — Parallel scraper wave 1 (GT, Reddit Pass 1, TikTok, Instagram)
         → Reddit Pass 2 after Pass 1 completes
2:00 AM — Parallel scraper wave 2 (Amazon, Alibaba, YouTube, Pinterest, X, Facebook)
3:00 AM — Hard cutoff → Scoring engine → Research council → Alerts → Email → Learning pass
"""
import os
import sys
import uuid
import asyncio
import logging
import threading
import time
from datetime import datetime, date, timedelta
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load env
env_file = PROJECT_ROOT / "agents" / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

from agents.config import get_supabase, APIFY_API_TOKEN
from agents.skills.activity_logger import post_status

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("scheduler")

# ══════════════════════════════════════════════
# PIPELINE PHASE TRACKER
# ══════════════════════════════════════════════
def log_phase(db, phase, status, started=None, completed=None, details=None, error=None):
    try:
        row = {
            "run_date": date.today().isoformat(),
            "phase": phase,
            "status": status,
            "started_at": started,
            "completed_at": completed,
            "details": details,
            "error_message": error,
        }
        if started and completed:
            s = datetime.fromisoformat(started)
            c = datetime.fromisoformat(completed)
            row["duration_seconds"] = (c - s).total_seconds()
        db.table("pipeline_runs").insert(row).execute()
    except Exception as e:
        logger.error("Failed to log pipeline phase: %s", e)


# ══════════════════════════════════════════════
# SCRAPER RUNNERS
# ══════════════════════════════════════════════
def run_agent_thread(agent_class, products, run_id, results, key):
    """Run an agent in its own thread with its own event loop. Thread is daemon so it dies with the process."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        agent = agent_class()
        result = loop.run_until_complete(agent.run(products, run_id))
        results[key] = result
    except Exception as e:
        logger.error("[%s] Thread crashed: %s", key, str(e)[:300])
        results[key] = {"status": "failed", "error": str(e)[:300]}
    finally:
        try:
            loop.close()
        except Exception:
            pass


def _run_discovery_agent(agent_class, results, key):
    """Run a discovery agent (no products param needed)."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        agent = agent_class()
        result = loop.run_until_complete(agent.run())
        results[key] = result
    except Exception as e:
        results[key] = {"status": "failed", "error": str(e)[:300]}
    finally:
        loop.close()


def has_credentials(*env_vars):
    return all(os.environ.get(v) for v in env_vars)


# ══════════════════════════════════════════════
# WAVE 1: 1:00 AM
# ══════════════════════════════════════════════
def run_wave1(db, products, run_id):
    logger.info("=== WAVE 1 START ===")
    post_status("scraper-orchestrator", "busy", "Wave 1: parallel scrapers starting")
    started = datetime.now().isoformat()
    results = {}
    threads = []

    # Google Trends — always available (free)
    from agents.agent_google_trends import GoogleTrendsAgent
    t = threading.Thread(target=run_agent_thread, args=(GoogleTrendsAgent, products, run_id, results, "google_trends"))
    threads.append(("google_trends", t))
    t.start()

    # Reddit Pass 1 + Pass 2
    from agents.agent_reddit import RedditAgent
    t = threading.Thread(target=run_agent_thread, args=(RedditAgent, products, run_id, results, "reddit"))
    threads.append(("reddit", t))
    t.start()

    # TikTok
    if has_credentials("APIFY_API_TOKEN"):
        from agents.agent_tiktok import TikTokAgent
        t = threading.Thread(target=run_agent_thread, args=(TikTokAgent, products, run_id, results, "tiktok"))
        threads.append(("tiktok", t))
        t.start()
    else:
        results["tiktok"] = {"status": "skipped", "error": "no credentials"}

    # Instagram
    if has_credentials("APIFY_API_TOKEN"):
        from agents.agent_instagram import InstagramAgent
        t = threading.Thread(target=run_agent_thread, args=(InstagramAgent, products, run_id, results, "instagram"))
        threads.append(("instagram", t))
        t.start()
    else:
        results["instagram"] = {"status": "skipped", "error": "no credentials"}

    # Discovery agents — run in parallel with wave 1
    from agents.agent_discovery_reddit import RedditDiscoveryAgent
    from agents.agent_discovery_trends import TrendsDiscoveryAgent
    t = threading.Thread(target=_run_discovery_agent, args=(RedditDiscoveryAgent, results, "discovery_reddit"))
    threads.append(("discovery_reddit", t)); t.start()
    t = threading.Thread(target=_run_discovery_agent, args=(TrendsDiscoveryAgent, results, "discovery_trends"))
    threads.append(("discovery_trends", t)); t.start()

    # Wait for all wave 1 threads (max 90 minutes)
    for name, t in threads:
        t.join(timeout=5400)
        if t.is_alive():
            logger.error("[%s] TIMED OUT — thread still running (daemon will be killed on exit)", name)
            results[name] = {"status": "timeout", "error": "Thread exceeded time limit"}

    completed = datetime.now().isoformat()
    log_phase(db, "wave1_scrapers", "complete", started, completed,
              details={k: v.get("status", "?") for k, v in results.items()})

    logger.info("=== WAVE 1 COMPLETE: %s ===", {k: v.get("status") for k, v in results.items()})
    return results


# ══════════════════════════════════════════════
# WAVE 2: 2:00 AM
# ══════════════════════════════════════════════
def run_wave2(db, products, run_id):
    logger.info("=== WAVE 2 START ===")
    post_status("scraper-orchestrator", "busy", "Wave 2: second scraper wave starting")
    started = datetime.now().isoformat()
    results = {}
    threads = []

    wave2_agents = [
        ("amazon",    "agents.agent_amazon",    "AmazonAgent",    ["APIFY_API_TOKEN"]),
        ("alibaba",   "agents.agent_alibaba",   "AlibabaAgent",   ["APIFY_API_TOKEN"]),
        ("youtube",   "agents.agent_youtube",   "YouTubeAgent",   ["YOUTUBE_API_KEY"]),
        ("pinterest", "agents.agent_pinterest", "PinterestAgent", ["APIFY_API_TOKEN"]),
        ("x",         "agents.agent_x",         "XAgent",         ["X_BEARER_TOKEN"]),
        ("facebook",  "agents.agent_facebook",  "FacebookAgent",  ["APIFY_API_TOKEN"]),
        ("walmart",   "agents.agent_walmart",   "WalmartAgent",   ["APIFY_API_TOKEN"]),
        ("etsy",      "agents.agent_etsy",      "EtsyAgent",      ["APIFY_API_TOKEN"]),
    ]

    for name, module_path, class_name, creds in wave2_agents:
        if has_credentials(*creds):
            try:
                import importlib
                mod = importlib.import_module(module_path)
                agent_cls = getattr(mod, class_name)
                t = threading.Thread(target=run_agent_thread, args=(agent_cls, products, run_id, results, name))
                threads.append((name, t))
                t.start()
            except Exception as e:
                results[name] = {"status": "failed", "error": str(e)[:200]}
        else:
            results[name] = {"status": "skipped", "error": "no credentials"}

    # Amazon discovery — runs alongside wave 2
    if has_credentials("APIFY_API_TOKEN"):
        from agents.agent_discovery_amazon import AmazonDiscoveryAgent
        t = threading.Thread(target=_run_discovery_agent, args=(AmazonDiscoveryAgent, results, "discovery_amazon"))
        threads.append(("discovery_amazon", t)); t.start()

    # Wait for wave 2 (max 55 minutes before 3 AM cutoff)
    for name, t in threads:
        t.join(timeout=3300)
        if t.is_alive():
            logger.warning("[%s] Timed out in wave 2", name)
            results[name] = {"status": "timeout"}

    completed = datetime.now().isoformat()
    log_phase(db, "wave2_scrapers", "complete", started, completed,
              details={k: v.get("status", "?") for k, v in results.items()})

    logger.info("=== WAVE 2 COMPLETE: %s ===", {k: v.get("status") for k, v in results.items()})
    return results


# ══════════════════════════════════════════════
# SCORING ENGINE PHASE
# ══════════════════════════════════════════════
def run_discovery_crossref(db):
    """Cross-reference discovery candidates and auto-add high-confidence ones."""
    logger.info("=== DISCOVERY CROSS-REFERENCE ===")
    post_status("scraper-orchestrator", "busy", "Discovery: cross-referencing candidates")
    try:
        # Get candidates with 2+ signals and high confidence
        settings_resp = db.table("discovery_settings").select("setting_key, setting_value").execute()
        settings = {r["setting_key"]: r["setting_value"] for r in (settings_resp.data or [])}
        threshold = float(settings.get("auto_add_threshold", "0.85"))
        min_signals = int(settings.get("min_signal_count", "2"))

        candidates = db.table("discovery_candidates").select("*") \
            .eq("status", "new").eq("added_to_tracking", False) \
            .gte("confidence_score", threshold).gte("signal_count", min_signals) \
            .execute()

        added = 0
        for c in (candidates.data or []):
            # Auto-add to products
            try:
                prod_resp = db.table("products").insert({
                    "name": c.get("display_name") or c["keyword"].title(),
                    "category": c.get("category") or "Wellness",
                    "keywords": [c["keyword"]],
                    "active": True,
                }).execute()

                if prod_resp.data:
                    pid = prod_resp.data[0]["id"]
                    db.table("discovery_candidates").update({
                        "added_to_tracking": True,
                        "product_id": pid,
                        "status": "added",
                        "status_changed_at": datetime.now().isoformat(),
                    }).eq("id", c["id"]).execute()

                    # Log to security_log
                    db.table("security_log").insert({
                        "event_type": "auto_discovery_add",
                        "details": f"Auto-added '{c['keyword']}' (confidence {c['confidence_score']}, {c['signal_count']} signals)",
                    }).execute()

                    added += 1
                    logger.info("[discovery] Auto-added: %s (confidence %.2f, %d signals)",
                                c["keyword"], c["confidence_score"], c["signal_count"])
            except Exception as e:
                logger.error("[discovery] Auto-add failed for '%s': %s", c["keyword"], str(e)[:100])

        logger.info("[discovery] Cross-reference complete: %d auto-added", added)
    except Exception as e:
        logger.error("[discovery] Cross-reference failed: %s", e)


def run_scoring(db, products, run_id):
    logger.info("=== SCORING ENGINE START ===")
    post_status("scraper-orchestrator", "busy", "Scoring engine running")
    started = datetime.now().isoformat()

    try:
        from agents.scoring_engine import score_all_products
        score_all_products(db, products, run_id)
        status = "complete"
        error = None
    except Exception as e:
        logger.error("Scoring engine failed: %s", e)
        status = "failed"
        error = str(e)[:500]

    completed = datetime.now().isoformat()
    log_phase(db, "scoring_engine", status, started, completed, error_message=error)
    return status


# ══════════════════════════════════════════════
# RESEARCH COUNCIL PHASE
# ══════════════════════════════════════════════
def run_council(db, products, run_id):
    logger.info("=== RESEARCH COUNCIL START ===")
    post_status("scraper-orchestrator", "busy", "Research council deliberating")
    started = datetime.now().isoformat()

    try:
        from agents.research_council import run_council_session
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        council_results = loop.run_until_complete(run_council_session(db, products, run_id))
        loop.close()
        status = "complete"
        error = None
    except Exception as e:
        logger.error("Research council failed: %s", e)
        status = "failed"
        error = str(e)[:500]
        council_results = {}

    completed = datetime.now().isoformat()
    log_phase(db, "research_council", status, started, completed, error_message=error)
    return council_results


# ══════════════════════════════════════════════
# ALERT ENGINE PHASE
# ══════════════════════════════════════════════
def run_alerts(db, products, run_id):
    logger.info("=== ALERT ENGINE START ===")
    started = datetime.now().isoformat()

    try:
        from agents.alert_engine import run_alert_scan
        alert_count = run_alert_scan(db, products, run_id)
        status = "complete"
    except Exception as e:
        logger.error("Alert engine failed: %s", e)
        alert_count = 0
        status = "failed"

    completed = datetime.now().isoformat()
    log_phase(db, "alert_engine", status, started, completed, details={"alerts_fired": alert_count})
    return alert_count


# ══════════════════════════════════════════════
# EMAIL REPORT PHASE
# ══════════════════════════════════════════════
def run_email(db, products, run_id):
    logger.info("=== EMAIL REPORT START ===")
    started = datetime.now().isoformat()

    try:
        from reporters.daily_email import send_daily_report
        sent = send_daily_report(db, products, run_id)
        status = "complete"
    except Exception as e:
        logger.error("Email report failed: %s", e)
        sent = 0
        status = "failed"

    completed = datetime.now().isoformat()
    log_phase(db, "email_report", status, started, completed, details={"emails_sent": sent})
    return sent


# ══════════════════════════════════════════════
# LEARNING PASS PHASE
# ══════════════════════════════════════════════
def run_learning(db, run_id):
    logger.info("=== LEARNING PASS START ===")
    started = datetime.now().isoformat()

    try:
        from agents.learning_pass import run_learning_pass
        adjustments = run_learning_pass(db, run_id)
        status = "complete"
    except Exception as e:
        logger.error("Learning pass failed: %s", e)
        adjustments = 0
        status = "failed"

    completed = datetime.now().isoformat()
    log_phase(db, "learning_pass", status, started, completed, details={"adjustments": adjustments})
    return adjustments


# ══════════════════════════════════════════════
# FULL NIGHTLY PIPELINE
# ══════════════════════════════════════════════
def run_full_pipeline():
    """Execute the complete pipeline (weekly, manual, or backfill)."""
    pipeline_start = datetime.now()
    run_id = str(uuid.uuid4())

    db = get_supabase()

    # Determine run type
    is_backfill = os.environ.get("BACKFILL_MODE") == "1"
    backfill_product = os.environ.get("BACKFILL_PRODUCT", "").strip() or None
    run_type = "backfill" if is_backfill else "weekly"
    lookback = int(os.environ.get("LOOKBACK_DAYS_BACKFILL", "365")) if is_backfill \
        else int(os.environ.get("LOOKBACK_DAYS_WEEKLY", "7"))

    # Insert pipeline_runs row at start
    pipeline_run_id = None
    try:
        run_row = db.table("pipeline_runs").insert({
            "run_type": run_type,
            "status": "running",
            "started_at": pipeline_start.isoformat(),
            "is_backfill": is_backfill,
            "lookback_days": lookback,
        }).execute()
        if run_row.data:
            pipeline_run_id = run_row.data[0].get("id")
    except Exception as e:
        logger.warning("Failed to write pipeline_runs start row: %s", e)

    # Prevent double-runs: skip if a run completed in the last 6 hours (skip in backfill mode)
    if not is_backfill:
        try:
            six_hours_ago = (datetime.now() - timedelta(hours=6)).isoformat()
            recent = db.table("pipeline_runs").select("id") \
                .eq("status", "completed") \
                .gte("completed_at", six_hours_ago).limit(1).execute()
            if recent.data:
                logger.info("Recent pipeline run detected (last 6 hours) — skipping duplicate")
                post_status("scraper-orchestrator", "idle", "Skipped — recent run already completed")
                if pipeline_run_id:
                    db.table("pipeline_runs").update({
                        "status": "skipped",
                        "completed_at": datetime.now().isoformat(),
                        "error": "Duplicate run within 6 hours",
                    }).eq("id", pipeline_run_id).execute()
                return
        except Exception:
            pass

    logger.info("=" * 70)
    logger.info("PIPELINE STARTED — type=%s run_id=%s lookback=%d days", run_type, run_id, lookback)
    logger.info("=" * 70)
    post_status("scraper-orchestrator", "busy", f"{run_type.title()} pipeline started — run {run_id[:8]}")

    # Load active products (filter to single product if backfill --product flag set)
    products_q = db.table("products").select("*").eq("active", True)
    if backfill_product:
        products_q = products_q.eq("name", backfill_product)
    products = products_q.execute().data
    if not products:
        msg = f"No active products" + (f" matching '{backfill_product}'" if backfill_product else "")
        logger.warning("%s — pipeline complete", msg)
        post_status("scraper-orchestrator", "idle", msg)
        if pipeline_run_id:
            db.table("pipeline_runs").update({
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "duration_seconds": int((datetime.now() - pipeline_start).total_seconds()),
                "products_processed": 0,
                "error": msg,
            }).eq("id", pipeline_run_id).execute()
        return

    logger.info("Active products: %d", len(products))

    # Phase 1: Wave 1 scrapers
    wave1_results = run_wave1(db, products, run_id)

    # Phase 2: Wave 2 scrapers
    wave2_results = run_wave2(db, products, run_id)

    # Phase 2.5: Discovery cross-reference + auto-add
    run_discovery_crossref(db)

    # Reload products in case discovery auto-added new ones
    products = db.table("products").select("*").eq("active", True).execute().data or products

    # Phase 3: Scoring engine — gate: only run if scrapers produced data
    total_written = sum(r.get("rows_written", 0) for r in wave1_results.values() if isinstance(r, dict))
    if total_written == 0:
        logger.warning("GATE: No data written in Wave 1 — skipping scoring and council")
        log_phase(db, "scoring_engine", "skipped", datetime.now().isoformat(), datetime.now().isoformat(),
                  details={"reason": "No scraper data to score"})
    else:
        scoring_status = run_scoring(db, products, run_id)

        # Phase 4: Research council — gate: only run if scoring succeeded
        if scoring_status == "failed":
            logger.warning("GATE: Scoring failed — skipping council to prevent invalid verdicts")
            log_phase(db, "research_council", "skipped", datetime.now().isoformat(), datetime.now().isoformat(),
                      details={"reason": "Scoring engine failed"})
        else:
            run_council(db, products, run_id)

    # Phase 5: Alert engine — always runs (can alert on failures)
    run_alerts(db, products, run_id)

    # Phase 6: Email report — always runs
    run_email(db, products, run_id)

    # Phase 7: Learning pass
    run_learning(db, run_id)

    # Done
    duration = (datetime.now() - pipeline_start).total_seconds()

    # Aggregate per-platform stats from wave results
    platform_stats = {}
    total_posts = 0
    total_comments = 0
    total_signals = 0
    for results in (wave1_results, wave2_results):
        for platform, r in (results or {}).items():
            if not isinstance(r, dict):
                continue
            ps = {
                "status": r.get("status"),
                "rows_written": r.get("rows_written", 0),
                "duration_seconds": r.get("duration_seconds"),
            }
            # Pull richer stats from result if present
            for k in ("pass1_total", "pass1_passed", "pass2_posts", "pass2_comments",
                      "purchase_signals", "negative_signals", "question_signals"):
                if k in r:
                    ps[k] = r[k]
            platform_stats[platform] = ps
            total_posts += r.get("pass1_total", 0) or 0
            total_comments += r.get("pass2_comments", 0) or 0
            total_signals += r.get("purchase_signals", 0) or 0

    # Write completion to pipeline_runs
    if pipeline_run_id:
        try:
            db.table("pipeline_runs").update({
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "duration_seconds": int(duration),
                "products_processed": len(products),
                "total_posts_found": total_posts,
                "total_comments_pulled": total_comments,
                "total_signals_found": total_signals,
                "platform_stats": platform_stats,
            }).eq("id", pipeline_run_id).execute()
        except Exception as e:
            logger.warning("Failed to write pipeline_runs completion: %s", e)

    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETE — %s in %.1f seconds (%d products, %d posts, %d comments)",
                run_type, duration, len(products), total_posts, total_comments)
    logger.info("=" * 70)
    post_status("scraper-orchestrator", "done", f"Pipeline complete in {duration:.0f}s")

    next_msg = "Next run: next Sunday at 1:00 AM" if run_type == "weekly" else "Backfill complete"
    post_status("scraper-orchestrator", "idle", next_msg)


# ══════════════════════════════════════════════
# SCHEDULER SETUP
# ══════════════════════════════════════════════
# Global scheduler instance — accessed by FastAPI server for pause/resume
_scheduler_instance = None


def create_scheduler():
    """Create the APScheduler with weekly cron from .env config."""
    day_of_week = os.environ.get("PIPELINE_SCHEDULE_DAY_OF_WEEK", "sun")
    hour = int(os.environ.get("PIPELINE_SCHEDULE_HOUR", "1"))
    minute = int(os.environ.get("PIPELINE_SCHEDULE_MINUTE", "0"))

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_full_pipeline, "cron",
        day_of_week=day_of_week, hour=hour, minute=minute,
        id="weekly_pipeline",
        misfire_grace_time=3600, max_instances=1,
    )
    return scheduler


def start_scheduler():
    global _scheduler_instance
    _scheduler_instance = create_scheduler()
    _scheduler_instance.start()
    job = _scheduler_instance.get_job("weekly_pipeline")
    next_run = job.next_run_time if job else "unknown"
    day = os.environ.get("PIPELINE_SCHEDULE_DAY_OF_WEEK", "sun")
    hour = os.environ.get("PIPELINE_SCHEDULE_HOUR", "1")
    minute = os.environ.get("PIPELINE_SCHEDULE_MINUTE", "0")
    logger.info("[scheduler] Weekly pipeline scheduled (%s @ %s:%02d). Next run: %s",
                day, hour, int(minute), next_run)
    post_status("scraper-orchestrator", "idle", f"Scheduler active. Next run: {next_run}")
    return _scheduler_instance


def get_scheduler():
    return _scheduler_instance


def get_scheduler_status():
    """Return scheduler state for the API."""
    s = _scheduler_instance
    if not s:
        return {"active": False, "next_run": "Scheduler not started", "state": "stopped"}

    job = s.get_job("weekly_pipeline")
    if not job:
        return {"active": False, "next_run": "No job scheduled", "state": "stopped"}

    is_paused = job.next_run_time is None
    if is_paused:
        return {"active": False, "next_run": "Paused — no runs will fire", "state": "paused"}

    next_time = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
    return {"active": True, "next_run": f"Full pipeline at {next_time}", "state": "active"}


if __name__ == "__main__":
    print("Starting nightly pipeline manually...")
    run_full_pipeline()
