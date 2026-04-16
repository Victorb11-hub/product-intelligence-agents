"""
Shared Apify actor runner used by all 9 Apify-based agents.

Provides a single pattern for:
- Running an Apify actor with input
- Polling for completion
- Fetching results from the dataset
- Error handling and timeout management
"""
import time
import logging
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECS = 300  # 5 minutes max per actor run
POLL_INTERVAL_SECS = 5


def run_actor(
    actor_id: str,
    run_input: dict,
    api_token: str,
    timeout_secs: int = DEFAULT_TIMEOUT_SECS,
    max_items: int = 100,
    memory_mbytes: int = 1024,
) -> list[dict]:
    """
    Run an Apify actor and return the dataset items.

    Args:
        actor_id: Full actor ID (e.g., 'apify/instagram-scraper').
        run_input: Input dict for the actor.
        api_token: Apify API token.
        timeout_secs: Max seconds to wait for completion.
        max_items: Max items to fetch from the dataset.
        memory_mbytes: Memory allocation for the actor run (must be power of 2, min 512).

    Returns:
        List of result dicts from the actor's dataset.

    Raises:
        RuntimeError: If the actor fails or times out.
    """
    from apify_client import ApifyClient

    client = ApifyClient(api_token)

    logger.info("Starting Apify actor: %s", actor_id)

    # Start the actor run (suppress verbose Apify status output)
    import apify_client._logging as _al
    _prev = logging.getLogger("apify_client").level
    logging.getLogger("apify_client").setLevel(logging.WARNING)
    run = client.actor(actor_id).call(
        run_input=run_input,
        timeout_secs=timeout_secs,
        memory_mbytes=memory_mbytes,
    )
    logging.getLogger("apify_client").setLevel(_prev)

    if not run:
        raise RuntimeError(f"Actor {actor_id} returned no run object")

    status = run.get("status")
    if status not in ("SUCCEEDED", "RUNNING", "TIMED-OUT"):
        error_msg = run.get("statusMessage", "Unknown error")
        raise RuntimeError(f"Actor {actor_id} failed with status {status}: {error_msg}")

    # Fetch dataset items (including partial results on timeout)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        raise RuntimeError(f"Actor {actor_id} has no dataset")

    items = list(
        client.dataset(dataset_id).iterate_items(limit=max_items)
    )

    if status == "TIMED-OUT":
        if items:
            logger.warning("Actor %s timed out but returned %d partial items — using them",
                           actor_id, len(items))
        else:
            error_msg = run.get("statusMessage", "Unknown error")
            raise RuntimeError(f"Actor {actor_id} timed out with no results: {error_msg}")
    else:
        logger.info("Actor %s returned %d items", actor_id, len(items))

    return items


def run_actor_async_poll(
    actor_id: str,
    run_input: dict,
    api_token: str,
    timeout_secs: int = DEFAULT_TIMEOUT_SECS,
    max_items: int = 100,
) -> list[dict]:
    """
    Alternative: start actor, poll for status, then fetch results.
    Use this when .call() isn't suitable (e.g., longer-running actors).
    """
    from apify_client import ApifyClient

    client = ApifyClient(api_token)

    logger.info("Starting async Apify actor: %s", actor_id)

    run_info = client.actor(actor_id).start(
        run_input=run_input,
        memory_mbytes=256,
    )

    if not run_info:
        raise RuntimeError(f"Failed to start actor {actor_id}")

    run_id = run_info["id"]
    run_client = client.run(run_id)

    # Poll for completion
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_secs:
            # Try to abort the run
            try:
                run_client.abort()
            except Exception:
                pass
            raise RuntimeError(f"Actor {actor_id} timed out after {timeout_secs}s")

        info = run_client.get()
        status = info.get("status")

        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(
                f"Actor {actor_id} ended with status {status}: "
                f"{info.get('statusMessage', '')}"
            )

        time.sleep(POLL_INTERVAL_SECS)

    # Fetch results
    dataset_id = info.get("defaultDatasetId")
    if not dataset_id:
        return []

    items = list(
        client.dataset(dataset_id).iterate_items(limit=max_items)
    )

    logger.info("Actor %s returned %d items", actor_id, len(items))
    return items


def extract_texts(items: list[dict], text_fields: list[str]) -> list[str]:
    """
    Extract text content from actor results using multiple possible field names.

    Args:
        items: List of result dicts.
        text_fields: List of field names to try, in priority order.

    Returns:
        List of non-empty text strings.
    """
    texts = []
    for item in items:
        for field in text_fields:
            val = item.get(field)
            if val and isinstance(val, str) and len(val.strip()) > 5:
                texts.append(val.strip())
                break
    return texts


def extract_dates(items: list[dict], date_fields: list[str]) -> list[str]:
    """
    Extract date strings from actor results.
    Tries multiple field names and normalizes to YYYY-MM-DD.
    """
    dates = []
    for item in items:
        for field in date_fields:
            val = item.get(field)
            if val:
                # Handle ISO format and timestamps
                date_str = str(val)[:10]
                if len(date_str) >= 10 and date_str[4] == '-':
                    dates.append(date_str)
                    break
    return dates
