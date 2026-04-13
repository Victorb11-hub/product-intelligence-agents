"""
FastAPI server for triggering agents from the React dashboard.

Endpoints:
  POST /run/all           — trigger full orchestrator run
  POST /run/{platform}    — trigger single platform agent
  GET  /status            — current run status from agent_runs
  GET  /status/{run_id}   — status for a specific run
  GET  /weights           — all learned weights
  POST /weights/reset     — reset all weights to base values
"""
import asyncio
import os
import sys
import uuid
import logging
import threading
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .config import get_supabase
from .orchestrator import run_all, run_single
from .skills.learner import reset_all_weights

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Product Intelligence Agent Server", version="1.0.0")

# Scheduler lives inside the API server process so endpoints can read it directly
_scheduler = None

@app.on_event("startup")
def _start_scheduler():
    global _scheduler
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from scheduler import create_scheduler
        _scheduler = create_scheduler()
        _scheduler.start()
        job = _scheduler.get_job("nightly_pipeline")
        next_run = job.next_run_time if job else "unknown"
        logger.info("Scheduler started inside API server. Next run: %s", next_run)
    except Exception as e:
        logger.error("Failed to start scheduler: %s", e)

# Allow dashboard to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173", "http://localhost:5174", "http://localhost:3000",
        os.environ.get("VERCEL_URL", "https://product-intelligence-dashboard.vercel.app"),
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Track active runs
_active_runs: dict[str, dict] = {}


def _run_async_in_thread(coro_func, *args, run_id: str = ""):
    """
    Run an async function in a new thread with its own event loop.
    Fixes 'no current event loop' error on Windows background threads.
    """
    def _worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(coro_func(*args))
            _active_runs[run_id] = {**result, "completed_at": datetime.now().isoformat()}
        except Exception as e:
            logger.error("Agent run %s failed: %s", run_id, e)
            _active_runs[run_id] = {
                "status": "failed",
                "error": str(e),
                "completed_at": datetime.now().isoformat(),
            }
        finally:
            loop.close()

    thread = threading.Thread(target=_worker, daemon=False)
    thread.start()


class RunResponse(BaseModel):
    run_id: str
    status: str
    message: str


@app.post("/run/all", response_model=RunResponse)
async def trigger_full_run():
    """Trigger a full orchestrator run across all enabled platforms."""
    run_id = str(uuid.uuid4())
    _active_runs[run_id] = {"status": "starting", "started_at": datetime.now().isoformat()}

    _run_async_in_thread(run_all, run_id=run_id)

    return RunResponse(
        run_id=run_id,
        status="started",
        message="Full orchestrator run started in background",
    )


@app.post("/run/{platform}", response_model=RunResponse)
async def trigger_single_run(platform: str):
    """Trigger a single platform agent run."""
    valid_platforms = [
        "reddit", "tiktok", "instagram", "x", "facebook", "youtube",
        "google_trends", "amazon", "walmart", "etsy", "alibaba", "pinterest",
    ]
    if platform not in valid_platforms:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {platform}")

    run_id = str(uuid.uuid4())
    _active_runs[run_id] = {"status": "starting", "platform": platform}

    _run_async_in_thread(run_single, platform, run_id=run_id)

    return RunResponse(
        run_id=run_id,
        status="started",
        message=f"{platform} agent started in background",
    )


@app.get("/status")
async def get_status():
    """Get status of all recent agent runs from the database."""
    supabase = get_supabase()

    # Get last 50 runs
    resp = supabase.table("agent_runs") \
        .select("*") \
        .order("created_at", desc=True) \
        .limit(50) \
        .execute()

    # Get active in-memory runs
    active = {
        rid: info for rid, info in _active_runs.items()
        if info.get("status") in ("starting", "running")
    }

    return {
        "active_runs": active,
        "recent_runs": resp.data,
        "is_running": len(active) > 0,
    }


@app.get("/status/{run_id}")
async def get_run_status(run_id: str):
    """Get status for a specific orchestrator run."""
    supabase = get_supabase()

    # Check in-memory
    if run_id in _active_runs:
        in_memory = _active_runs[run_id]
    else:
        in_memory = None

    # Check database
    resp = supabase.table("agent_runs") \
        .select("*") \
        .eq("run_id", run_id) \
        .order("platform") \
        .execute()

    # Get cross-reference results for this run
    cross_resp = supabase.table("cross_reference_runs") \
        .select("*") \
        .eq("run_id", run_id) \
        .execute()

    return {
        "run_id": run_id,
        "in_memory_status": in_memory,
        "agent_runs": resp.data,
        "cross_reference": cross_resp.data,
    }


@app.get("/weights")
async def get_weights():
    """Get all learned weights."""
    supabase = get_supabase()
    resp = supabase.table("agent_weights") \
        .select("*") \
        .order("agent") \
        .execute()

    # Group by agent
    by_agent = {}
    for row in resp.data:
        agent = row["agent"]
        if agent not in by_agent:
            by_agent[agent] = []
        by_agent[agent].append({
            "signal": row["signal_name"],
            "base": row["base_weight"],
            "learned": row["learned_weight"],
            "drift": round(row["learned_weight"] - row["base_weight"], 4),
            "adjustments": row["adjustment_count"],
        })

    return {"weights": by_agent, "total_signals": len(resp.data)}


@app.post("/weights/reset")
async def reset_weights():
    """Reset all learned weights to base values."""
    supabase = get_supabase()
    count = await reset_all_weights(supabase)
    return {"message": f"Reset {count} weights to base values", "count": count}


@app.post("/run/pipeline")
async def trigger_pipeline():
    """Trigger the full nightly pipeline manually."""
    sys.path.insert(0, str(Path(__file__).parent.parent))

    run_id = str(uuid.uuid4())
    _active_runs[run_id] = {"status": "starting", "type": "full_pipeline"}

    def _worker():
        from scheduler import run_full_pipeline
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            run_full_pipeline()
            _active_runs[run_id] = {"status": "complete", "completed_at": datetime.now().isoformat()}
        except Exception as e:
            _active_runs[run_id] = {"status": "failed", "error": str(e)}
        finally:
            loop.close()

    thread = threading.Thread(target=_worker, daemon=False)
    thread.start()

    return RunResponse(run_id=run_id, status="started", message="Full nightly pipeline started")


@app.post("/run/backtest")
async def run_backtest_endpoint(keyword: str = "", start_date: str = "", end_date: str = ""):
    """Run a backtest for a product keyword."""
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword is required")

    from .agent_backtest import run_backtest
    result = run_backtest(keyword, start_date, end_date)
    return result


@app.get("/scheduler/status")
async def scheduler_status():
    """Get the current scheduler state and next run time."""
    if not _scheduler:
        return {"active": False, "next_run": "Scheduler not started", "state": "stopped"}

    job = _scheduler.get_job("nightly_pipeline")
    if not job:
        return {"active": False, "next_run": "No job scheduled", "state": "stopped"}

    if job.next_run_time is None:
        return {"active": False, "next_run": "Paused — no runs will fire", "state": "paused"}

    next_time = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
    return {"active": True, "next_run": f"Full pipeline at {next_time}", "state": "active"}


@app.post("/scheduler/pause")
async def scheduler_pause():
    """Pause the nightly scheduler."""
    if not _scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not running")
    _scheduler.pause_job("nightly_pipeline")
    return {"status": "paused", "message": "Nightly pipeline paused. No runs will fire until resumed."}


@app.post("/scheduler/resume")
async def scheduler_resume():
    """Resume the nightly scheduler."""
    if not _scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not running")
    _scheduler.resume_job("nightly_pipeline")
    job = _scheduler.get_job("nightly_pipeline")
    next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job and job.next_run_time else "unknown"
    return {"status": "active", "message": f"Scheduler resumed. Next run: {next_run}"}


    # Auth is handled by Supabase Auth — no server-side verification needed


@app.get("/pipeline/status")
async def pipeline_status():
    """Get the latest pipeline run phases."""
    supabase = get_supabase()
    resp = supabase.table("pipeline_runs").select("*") \
        .order("created_at", desc=True).limit(20).execute()
    return {"phases": resp.data}


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


def start():
    """Entry point for running the server."""
    import uvicorn
    uvicorn.run(
        "agents.server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )


if __name__ == "__main__":
    start()
