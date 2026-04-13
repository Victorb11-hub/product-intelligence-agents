"""
Discovery Agent Stub — Instagram
Tracks Instagram hashtag growth and influencer product mentions
STATUS: STUB — will be activated when data source is configured.
"""
import logging
logger = logging.getLogger(__name__)

class InstagramDiscoveryAgent:
    PLATFORM = "discovery_instagram"

    async def run(self, run_id=None):
        logger.info("[%s] Stub — not yet activated", self.PLATFORM)
        return {"candidates_found": 0, "status": "stub"}
