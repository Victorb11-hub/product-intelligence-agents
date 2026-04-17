"""
Relevance Scorer — Calculates how closely a post/comment relates
to a specific tracked product vs the general category.

Uses weighted keyword matching from product_keywords table.
No ML required — fast, accurate, and deterministic.

Score range: 0.0 to 1.0 (displayed as 0-100% on dashboard)
"""
import logging

logger = logging.getLogger(__name__)


def load_product_keywords(db, product_id: str) -> list[dict]:
    """Load keywords + weights from product_keywords table."""
    try:
        resp = db.table("product_keywords") \
            .select("keyword, weight") \
            .eq("product_id", product_id) \
            .execute()
        return resp.data or []
    except Exception as e:
        logger.warning("[relevance] Failed to load keywords: %s", e)
        return []


def score_relevance(text: str, keywords: list[dict]) -> dict:
    """
    Score a single text against product keywords.

    Args:
        text: post title, body, caption, or comment text
        keywords: list of {keyword: str, weight: float}

    Returns:
        {
            relevance_score: 0.0-1.0,
            matched_keywords: [{"keyword": ..., "weight": ...}],
            total_weight_matched: float,
            max_possible_weight: float,
        }
    """
    if not text or not keywords:
        return {
            "relevance_score": 0.0,
            "matched_keywords": [],
            "total_weight_matched": 0,
            "max_possible_weight": 0,
        }

    text_lower = text.lower()
    matched = []
    total_matched_weight = 0
    max_possible = sum(kw["weight"] for kw in keywords)

    for kw in keywords:
        keyword = kw["keyword"].lower()
        weight = kw.get("weight", 1.0)
        if keyword in text_lower:
            matched.append({"keyword": keyword, "weight": weight})
            total_matched_weight += weight

    # Normalize against a realistic ceiling, not the sum of ALL keywords.
    # Matching the top 3 keywords (e.g. product name + brand + category = ~6-7 weight)
    # should give a high relevance score. Dividing by ALL keywords (30+) makes
    # every post score < 0.25 which defeats the purpose.
    # Use: the sum of the top 3 keyword weights as the normalization ceiling.
    sorted_weights = sorted((kw.get("weight", 1.0) for kw in keywords), reverse=True)
    realistic_max = sum(sorted_weights[:3]) if len(sorted_weights) >= 3 else max_possible
    score = total_matched_weight / realistic_max if realistic_max > 0 else 0
    # Cap at 1.0
    score = min(1.0, score)

    return {
        "relevance_score": round(score, 4),
        "matched_keywords": matched,
        "total_weight_matched": round(total_matched_weight, 2),
        "max_possible_weight": round(max_possible, 2),
    }


def score_post_relevance(post: dict, keywords: list[dict]) -> float:
    """
    Score a post's relevance by checking both title and body.
    Returns score 0.0-1.0.
    """
    title = (post.get("post_title") or "").strip()
    body = (post.get("post_body") or "").strip()
    # Combine title + body for matching (title gets checked implicitly)
    combined = f"{title} {body}".strip()

    result = score_relevance(combined, keywords)
    return result["relevance_score"]


def backfill_relevance(db, product_id: str) -> dict:
    """
    Backfill relevance scores for all posts of a product.
    Returns stats dict.
    """
    keywords = load_product_keywords(db, product_id)
    if not keywords:
        logger.warning("[relevance] No keywords found for product %s", product_id)
        return {"total": 0, "updated": 0, "high": 0, "medium": 0, "low": 0}

    # Paginate through all posts
    all_posts = []
    offset = 0
    batch_size = 500
    while True:
        resp = db.table("posts") \
            .select("id, post_title, post_body") \
            .eq("product_id", product_id) \
            .range(offset, offset + batch_size - 1) \
            .execute()
        rows = resp.data or []
        all_posts.extend(rows)
        if len(rows) < batch_size:
            break
        offset += batch_size

    logger.info("[relevance] Scoring %d posts for product %s", len(all_posts), product_id[:8])

    updated = 0
    high = medium = low = 0

    for i, post in enumerate(all_posts):
        score = score_post_relevance(post, keywords)

        # Categorize
        if score >= 0.6:
            high += 1
        elif score >= 0.3:
            medium += 1
        else:
            low += 1

        # Write back to DB
        try:
            db.table("posts").update({
                "relevance_score": score,
            }).eq("id", post["id"]).execute()
            updated += 1
        except Exception as e:
            logger.error("[relevance] Failed to update post %s: %s", post["id"], str(e)[:100])

        if (i + 1) % 500 == 0:
            logger.info("[relevance] %d/%d complete...", i + 1, len(all_posts))

    logger.info("[relevance] Done: %d posts scored — %d high, %d medium, %d low",
                len(all_posts), high, medium, low)

    return {
        "total": len(all_posts),
        "updated": updated,
        "high": high,
        "medium": medium,
        "low": low,
        "keyword_count": len(keywords),
    }
