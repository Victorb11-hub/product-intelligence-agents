"""
Activity Logger — Posts agent status to Victor's global dashboard.

Endpoint: POST http://localhost:3847/api/event
Format: {"agent":"[name]","status":"[idle|busy|reporting|walking|done]","msg":"[description]"}

Every agent must:
- POST busy at the START of every task
- POST idle at the END of every task
"""
import logging
import httpx

logger = logging.getLogger(__name__)

DASHBOARD_URL = "http://localhost:3847/api/event"


def post_status(agent: str, status: str, msg: str):
    """
    Post agent status to the global activity dashboard.
    Silently fails if dashboard is not running — never blocks the pipeline.
    """
    try:
        httpx.post(
            DASHBOARD_URL,
            json={"agent": agent, "status": status, "msg": msg},
            timeout=2,
        )
    except Exception:
        pass  # Dashboard may not be running — never block
