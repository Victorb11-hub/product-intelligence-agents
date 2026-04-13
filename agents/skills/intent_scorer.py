"""
INTELLIGENCE SKILL 4 — Buying Intent Scoring

Classifies each piece of content on a 5-level purchase intent ladder:
  Level 1 Awareness (0.1): "I heard about this"
  Level 2 Interest (0.3): "this looks interesting"
  Level 3 Consideration (0.5): "thinking about trying this"
  Level 4 Intent (0.8): "where can I buy this"
  Level 5 Purchase (1.0): "just bought", "on my third bottle"

Outputs avg_intent_score, intent_level_distribution, high_intent_comment_count.
"""
import re
import logging

logger = logging.getLogger(__name__)

# Pattern libraries for each intent level (compiled for performance)
INTENT_PATTERNS = {
    5: [  # Purchase (1.0) — confirmed buyers and repeat purchasers
        r"\bjust (bought|ordered|received|got)\b",
        r"\bbeen using (for|since)\b",
        r"\bon my (second|third|fourth|fifth|sixth|2nd|3rd|4th|5th|6th)\b",
        r"\brepurchas(e|ed|ing)\b",
        r"\breorder(ed|ing)?\b",
        r"\brefill(ed|ing)?\b",
        r"\bjust restocked\b",
        r"\bbuying again\b",
        r"\banother order\b",
        r"\blost count how many\b",
        r"\bmy (daily|morning|nightly|weekly) routine\b",
        r"\bstaple in my\b",
        r"\b(bought|ordered) (another|more|again)\b",
        r"\barrived (today|yesterday)\b",
        r"\bunboxing\b",
        r"\bjust came in\b",
        r"\b(love|loved) (this|it|mine)\b.{0,30}(month|week|year|since|for)",
        r"\busing (this|it) (for|since)\b",
        r"\bthird (bottle|tube|jar|pack)\b",
        r"\bfourth (bottle|tube|jar|pack)\b",
    ],
    4: [  # Intent (0.8) — actively planning to purchase
        r"\bwhere (can|do) (i|you) (buy|get|find|order)\b",
        r"\b(added|adding) to (my )?(cart|wishlist|list)\b",
        r"\badding to cart\b",
        r"\babout to order\b",
        r"\bchecking out\b",
        r"\bjust ordered\b",
        r"\bwaiting for delivery\b",
        r"\bshould i buy\b",
        r"\bworth buying\b",
        r"\bordering (this|it|some) (tonight|today|now|tomorrow)\b",
        r"\babout to (buy|order|purchase|get)\b",
        r"\btake my money\b",
        r"\bshut up and take\b",
        r"\blink (please|pls|\?)\b",
        r"\bwhere (did|do) you (get|buy)\b",
        r"\bdrop the link\b",
        r"\b(need|want) to (try|buy|get|order)\b",
        r"\bgoing to (order|buy|get|try)\b",
    ],
    3: [  # Consideration (0.5)
        r"\bthinking (about|of) (trying|buying|getting)\b",
        r"\bhas anyone (tried|used|bought)\b",
        r"\bworth (it|trying|buying|the (money|price|hype))\b",
        r"\bcomparing\b.{0,30}\b(vs|versus|or)\b",
        r"\bshould i (try|buy|get|switch)\b",
        r"\b(debating|considering|tempted)\b",
        r"\breviews?\b.{0,20}\b(good|bad|mixed|positive)\b",
        r"\bany(one|body) (recommend|suggest)\b",
        r"\bwhat do you (think|recommend)\b",
        r"\bpros and cons\b",
    ],
    2: [  # Interest (0.3)
        r"\blooks? (interesting|cool|amazing|great|nice|promising)\b",
        r"\bintrigued\b",
        r"\bcurious about\b",
        r"\binteresting\b.{0,20}\b(product|supplement|tool)\b",
        r"\bkeep (hearing|seeing) about\b",
        r"\beveryone('s| is) (talking|raving)\b",
        r"\bhype(d)?\b",
        r"\btrending\b",
        r"\bwhat (is|are) (this|these)\b",
        r"\bnever heard of\b.{0,20}\b(but|looks)\b",
    ],
    1: [  # Awareness (0.1)
        r"\b(heard|read|saw|seen) about\b",
        r"\bapparently\b",
        r"\bsupposedly\b",
        r"\bwhat is\b",
        r"\bwhat does .{0,20} do\b",
        r"\beli5\b",
        r"\banyone know\b",
        r"\bnew to\b.{0,15}\b(this|me)\b",
    ],
}

# Compile all patterns
_COMPILED_PATTERNS = {
    level: [re.compile(p, re.IGNORECASE) for p in patterns]
    for level, patterns in INTENT_PATTERNS.items()
}

LEVEL_SCORES = {1: 0.1, 2: 0.3, 3: 0.5, 4: 0.8, 5: 1.0}


def score_intent(text: str) -> dict:
    """
    Score a single piece of text for purchase intent.

    Returns:
        {
            "intent_level": int (1-5),
            "intent_score": float (0.1-1.0),
            "matched_patterns": list[str]
        }
    """
    if not text or not text.strip():
        return {"intent_level": 1, "intent_score": 0.1, "matched_patterns": []}

    # Check ALL levels, collect ALL matches, take highest level found
    highest_level = 1
    all_matched = []

    for level in [5, 4, 3, 2, 1]:
        for pattern in _COMPILED_PATTERNS[level]:
            match = pattern.search(text)
            if match:
                if level > highest_level:
                    highest_level = level
                all_matched.append(match.group())

    return {
        "intent_level": highest_level,
        "intent_score": LEVEL_SCORES[highest_level],
        "matched_patterns": all_matched[:5],  # Cap at 5 to avoid bloat
    }


def score_batch(texts: list[str]) -> dict:
    """
    Score a batch of texts and return aggregate intent metrics.

    Returns:
        {
            "avg_intent_score": float,
            "intent_level_distribution": dict (pct at each level),
            "high_intent_comment_count": int (levels 4 and 5),
            "sample_size": int,
            "results": list[dict] (individual scores)
        }
    """
    if not texts:
        return {
            "avg_intent_score": 0.0,
            "intent_level_distribution": {"1": 1.0, "2": 0.0, "3": 0.0, "4": 0.0, "5": 0.0},
            "high_intent_comment_count": 0,
            "sample_size": 0,
            "results": [],
        }

    results = [score_intent(t) for t in texts]
    n = len(results)

    level_counts = {str(i): 0 for i in range(1, 6)}
    for r in results:
        level_counts[str(r["intent_level"])] += 1

    distribution = {k: round(v / n, 4) for k, v in level_counts.items()}

    avg_score = sum(r["intent_score"] for r in results) / n
    high_intent = sum(1 for r in results if r["intent_level"] >= 4)

    return {
        "avg_intent_score": round(avg_score, 4),
        "intent_level_distribution": distribution,
        "high_intent_comment_count": high_intent,
        "sample_size": n,
        "results": results,
    }
