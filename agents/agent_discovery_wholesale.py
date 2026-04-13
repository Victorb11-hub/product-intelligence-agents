"""
Discovery Agent Stub — Wholesale
Tracks Alibaba new listings and MOQ drops for supply side signals
STATUS: STUB — will be activated when data source is configured.
"""
import logging
logger = logging.getLogger(__name__)

class WholesaleDiscoveryAgent:
    PLATFORM = "discovery_wholesale"

    async def run(self, run_id=None):
        logger.info("[%s] Stub — not yet activated", self.PLATFORM)
        return {"candidates_found": 0, "status": "stub"}
