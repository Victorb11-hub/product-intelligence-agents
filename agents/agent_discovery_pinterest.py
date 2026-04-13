"""
Discovery Agent Stub — Pinterest
Monitors Pinterest pin save rates for emerging product interest
STATUS: STUB — will be activated when data source is configured.
"""
import logging
logger = logging.getLogger(__name__)

class PinterestDiscoveryAgent:
    PLATFORM = "discovery_pinterest"

    async def run(self, run_id=None):
        logger.info("[%s] Stub — not yet activated", self.PLATFORM)
        return {"candidates_found": 0, "status": "stub"}
