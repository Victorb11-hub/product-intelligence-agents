"""
Discovery Agent Stub — News
Monitors health and beauty news sites for product launch triggers
STATUS: STUB — will be activated when data source is configured.
"""
import logging
logger = logging.getLogger(__name__)

class NewsDiscoveryAgent:
    PLATFORM = "discovery_news"

    async def run(self, run_id=None):
        logger.info("[%s] Stub — not yet activated", self.PLATFORM)
        return {"candidates_found": 0, "status": "stub"}
