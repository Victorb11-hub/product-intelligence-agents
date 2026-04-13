"""
Discovery Agent Stub — TikTok
Monitors viral TikTok hashtags and sounds for emerging products
STATUS: STUB — will be activated when data source is configured.
"""
import logging
logger = logging.getLogger(__name__)

class TikTokDiscoveryAgent:
    PLATFORM = "discovery_tiktok"

    async def run(self, run_id=None):
        logger.info("[%s] Stub — not yet activated", self.PLATFORM)
        return {"candidates_found": 0, "status": "stub"}
