"""
Stub Agent base — for platforms that need API credentials configured.
Logs that credentials are needed, exits cleanly.
"""
import logging
from datetime import date

from .base_agent import BaseAgent

logger = logging.getLogger(__name__)


class StubAgent(BaseAgent):
    """
    Stub agent that runs cleanly but returns no data.
    Subclasses only need to set PLATFORM, SIGNAL_TABLE, and REQUIRED_CREDENTIALS.
    """

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        raise NotImplementedError(
            f"{self.PLATFORM} agent is a stub. "
            f"Configure credentials: {', '.join(self.REQUIRED_CREDENTIALS)}"
        )

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": self.PLATFORM,
        }
