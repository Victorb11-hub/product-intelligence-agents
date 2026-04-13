"""
Master Orchestrator Agent

Coordinates all 12 platform sub-agents:
1. Reads active products from Supabase
2. Reads schedule config
3. Triggers platform agents
4. Monitors status and logs results
5. After all agents: triggers scoring engine
6. After scoring: triggers alert engine
7. Runs cross-reference engine for consensus detection
"""
import uuid
import asyncio
import logging
from datetime import datetime

from .config import get_supabase
from .skills.cross_referencer import run_cross_reference

logger = logging.getLogger(__name__)

# Platform agent imports
AGENT_REGISTRY = {}


def _register_agents():
    """Lazy-load agent classes to avoid circular imports."""
    global AGENT_REGISTRY
    if AGENT_REGISTRY:
        return

    from .agent_reddit import RedditAgent
    from .agent_tiktok import TikTokAgent
    from .agent_instagram import InstagramAgent
    from .agent_x import XAgent
    from .agent_facebook import FacebookAgent
    from .agent_youtube import YouTubeAgent
    from .agent_google_trends import GoogleTrendsAgent
    from .agent_amazon import AmazonAgent
    from .agent_walmart import WalmartAgent
    from .agent_etsy import EtsyAgent
    from .agent_alibaba import AlibabaAgent
    from .agent_pinterest import PinterestAgent

    AGENT_REGISTRY = {
        "reddit": RedditAgent,
        "tiktok": TikTokAgent,
        "instagram": InstagramAgent,
        "x": XAgent,
        "facebook": FacebookAgent,
        "youtube": YouTubeAgent,
        "google_trends": GoogleTrendsAgent,
        "amazon": AmazonAgent,
        "walmart": WalmartAgent,
        "etsy": EtsyAgent,
        "alibaba": AlibabaAgent,
        "pinterest": PinterestAgent,
    }


async def run_all(platforms: list[str] = None) -> dict:
    """
    Execute a full orchestrator run.

    Args:
        platforms: Optional list of specific platforms to run.
                  If None, runs all enabled platforms from schedules table.

    Returns:
        Summary dict with run_id and per-platform results.
    """
    from .skills.activity_logger import post_status

    _register_agents()
    supabase = get_supabase()
    run_id = str(uuid.uuid4())
    post_status("scraper-orchestrator", "busy", f"Starting orchestrator run {run_id[:8]}")

    logger.info("=== ORCHESTRATOR RUN %s STARTED ===", run_id)

    # Load active products
    products_resp = supabase.table("products") \
        .select("*") \
        .eq("active", True) \
        .execute()
    products = products_resp.data

    if not products:
        logger.warning("No active products found")
        return {"run_id": run_id, "status": "no_products", "results": {}}

    logger.info("Loaded %d active products", len(products))

    # Determine which platforms to run
    if platforms:
        platforms_to_run = platforms
    else:
        schedules_resp = supabase.table("schedules") \
            .select("platform") \
            .eq("enabled", True) \
            .execute()
        platforms_to_run = [s["platform"] for s in schedules_resp.data]

    logger.info("Running platforms: %s", ", ".join(platforms_to_run))

    # Run each platform agent
    results = {}
    for platform in platforms_to_run:
        agent_class = AGENT_REGISTRY.get(platform)
        if not agent_class:
            logger.warning("No agent registered for platform: %s", platform)
            results[platform] = {"status": "no_agent"}
            continue

        try:
            agent = agent_class()
            result = await agent.run(products, run_id)
            results[platform] = result
            logger.info("[%s] Completed: %s", platform, result.get("status"))
        except Exception as e:
            error_msg = f"Agent crash: {str(e)[:300]}"
            logger.error("[%s] %s", platform, error_msg)
            results[platform] = {"status": "failed", "error": error_msg}

            # Log the failure
            supabase.table("agent_runs").insert({
                "run_id": run_id,
                "platform": platform,
                "status": "failed",
                "error_message": error_msg,
                "started_at": datetime.now().isoformat(),
                "completed_at": datetime.now().isoformat(),
            }).execute()

    # Update schedule last_run timestamps
    for platform in platforms_to_run:
        try:
            supabase.table("schedules") \
                .update({"last_run": datetime.now().isoformat()}) \
                .eq("platform", platform) \
                .execute()
        except Exception:
            pass

    # Run cross-reference engine
    logger.info("Running cross-reference engine...")
    product_ids = [p["id"] for p in products]
    try:
        cross_results = await run_cross_reference(supabase, run_id, product_ids)
        consensus_count = sum(1 for r in cross_results if r.get("consensus_flag"))
        divergence_count = sum(1 for r in cross_results if r.get("divergence_flag"))
        logger.info(
            "Cross-reference complete: %d consensus, %d divergence products",
            consensus_count, divergence_count,
        )
    except Exception as e:
        logger.error("Cross-reference engine failed: %s", e)

    # Summary
    total_written = sum(r.get("rows_written", 0) for r in results.values())
    total_rejected = sum(r.get("rows_rejected", 0) for r in results.values())
    failed_count = sum(1 for r in results.values() if r.get("status") == "failed")

    summary_msg = f"Run complete. {len(platforms_to_run)} platforms, {total_written} written, {failed_count} failed."
    post_status("scraper-orchestrator", "done", summary_msg)
    post_status("scraper-orchestrator", "idle", summary_msg)

    logger.info(
        "=== ORCHESTRATOR RUN %s COMPLETE === "
        "Platforms: %d, Written: %d, Rejected: %d, Failed: %d",
        run_id, len(platforms_to_run), total_written, total_rejected, failed_count,
    )

    return {
        "run_id": run_id,
        "status": "complete" if failed_count == 0 else "partial",
        "platforms_run": len(platforms_to_run),
        "total_rows_written": total_written,
        "total_rows_rejected": total_rejected,
        "failed_platforms": failed_count,
        "results": results,
    }


async def run_single(platform: str) -> dict:
    """Run a single platform agent."""
    return await run_all(platforms=[platform])
