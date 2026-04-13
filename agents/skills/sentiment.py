"""
INTELLIGENCE SKILL 1 — Advanced Sentiment Analysis

Uses a pretrained transformer model (cardiffnlp/twitter-roberta-base-sentiment-latest)
that understands sarcasm, slang, hedged language, negation, and full context windows.

Outputs sentiment_score (-1.0 to 1.0) and sentiment_confidence (0.0 to 1.0).
"""
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

_model = None
_tokenizer = None


def _load_model():
    """Lazy-load the sentiment model on first use."""
    global _model, _tokenizer
    if _model is not None:
        return _model, _tokenizer

    try:
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        import torch

        model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
        _tokenizer = AutoTokenizer.from_pretrained(model_name)
        _model = AutoModelForSequenceClassification.from_pretrained(model_name)
        _model.eval()
        logger.info("Sentiment model loaded: %s", model_name)
    except Exception as e:
        logger.warning("Could not load transformer model, falling back to rule-based: %s", e)
        _model = "fallback"
        _tokenizer = None

    return _model, _tokenizer


def analyze_sentiment(text: str) -> dict:
    """
    Analyze sentiment of a single text.

    Returns:
        {
            "sentiment_score": float (-1.0 to 1.0),
            "sentiment_confidence": float (0.0 to 1.0),
            "label": str ("negative", "neutral", "positive")
        }
    """
    if not text or not text.strip():
        return {"sentiment_score": 0.0, "sentiment_confidence": 0.0, "label": "neutral"}

    model, tokenizer = _load_model()

    if model == "fallback":
        return _rule_based_sentiment(text)

    try:
        import torch

        inputs = tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            padding=True,
        )

        with torch.no_grad():
            outputs = model(**inputs)
            scores = torch.softmax(outputs.logits, dim=1)[0]

        # Model outputs: [negative, neutral, positive]
        neg, neu, pos = scores.tolist()

        # Convert to -1 to 1 scale: weighted sum
        sentiment_score = (pos - neg)
        # Confidence is how decisive the model is (1 - entropy-like measure)
        max_score = max(neg, neu, pos)
        sentiment_confidence = max_score

        if pos > neg and pos > neu:
            label = "positive"
        elif neg > pos and neg > neu:
            label = "negative"
        else:
            label = "neutral"

        return {
            "sentiment_score": round(sentiment_score, 4),
            "sentiment_confidence": round(sentiment_confidence, 4),
            "label": label,
        }

    except Exception as e:
        logger.error("Transformer inference failed, using fallback: %s", e)
        return _rule_based_sentiment(text)


def analyze_batch(texts: list[str]) -> list[dict]:
    """Analyze sentiment for a batch of texts."""
    return [analyze_sentiment(t) for t in texts]


def aggregate_sentiment(results: list[dict]) -> dict:
    """
    Aggregate multiple sentiment results into a single score.

    Returns:
        {
            "sentiment_score": float (weighted average by confidence),
            "sentiment_confidence": float (average confidence),
            "sample_size": int,
            "positive_pct": float,
            "negative_pct": float,
            "neutral_pct": float,
        }
    """
    if not results:
        return {
            "sentiment_score": 0.0,
            "sentiment_confidence": 0.0,
            "sample_size": 0,
            "positive_pct": 0.0,
            "negative_pct": 0.0,
            "neutral_pct": 0.0,
        }

    total_weight = sum(r["sentiment_confidence"] for r in results) or 1.0
    weighted_score = sum(
        r["sentiment_score"] * r["sentiment_confidence"] for r in results
    ) / total_weight

    labels = [r["label"] for r in results]
    n = len(labels)

    return {
        "sentiment_score": round(weighted_score, 4),
        "sentiment_confidence": round(
            sum(r["sentiment_confidence"] for r in results) / n, 4
        ),
        "sample_size": n,
        "positive_pct": round(labels.count("positive") / n, 4),
        "negative_pct": round(labels.count("negative") / n, 4),
        "neutral_pct": round(labels.count("neutral") / n, 4),
    }


def _rule_based_sentiment(text: str) -> dict:
    """
    Fallback rule-based sentiment when transformer model is unavailable.
    Handles sarcasm markers, slang, negation, and hedged language.
    """
    text_lower = text.lower()

    # Sarcasm/irony markers
    sarcasm_markers = ["oh great", "sure thing", "yeah right", "totally works", "what a surprise"]
    has_sarcasm = any(m in text_lower for m in sarcasm_markers)

    # Positive slang
    pos_slang = [
        "slaps", "fire", "bussin", "goated", "no cap", "lowkey obsessed",
        "game changer", "holy grail", "life changing", "obsessed", "love this",
        "amazing", "incredible", "best thing", "changed my life", "highly recommend",
        "10/10", "must have", "cant live without",
    ]
    # Negative signals (expanded with negation phrases)
    neg_signals = [
        "waste of money", "doesn't work", "does nothing", "scam", "overhyped",
        "disappointing", "terrible", "awful", "horrible", "regret", "returned it",
        "don't buy", "not worth", "rip off", "snake oil", "placebo",
        "not worth it", "don't waste your money", "wouldn't buy again",
        "not repurchasing", "didn't work for me", "don't recommend",
        "wouldn't recommend", "broke me out", "caused breakouts",
        "gave me a rash", "stopped working",
    ]
    # Hedged language (lower confidence)
    hedged = ["i think", "might work", "seems like", "could be", "not sure", "maybe"]

    pos_count = sum(1 for p in pos_slang if p in text_lower)
    neg_count = sum(1 for n in neg_signals if n in text_lower)
    hedge_count = sum(1 for h in hedged if h in text_lower)

    # Negation detection — flip sign, don't multiply
    negation_words = ["not ", "no ", "never ", "don't ", "doesn't ", "won't ", "can't "]
    has_negation = any(n in text_lower for n in negation_words)

    # Calculate score
    raw_score = (pos_count - neg_count) / max(pos_count + neg_count, 1)

    if has_sarcasm:
        raw_score = -abs(raw_score) if raw_score >= 0 else raw_score

    # Fix: flip sign on negation instead of multiplying by -0.5
    if has_negation and raw_score > 0:
        raw_score = -raw_score

    score = max(-1.0, min(1.0, raw_score))

    # Confidence based on signal strength (lowered floor from 0.2 to 0.1)
    signal_strength = pos_count + neg_count
    confidence = min(1.0, signal_strength * 0.25) if signal_strength > 0 else 0.1
    if hedge_count > 0:
        confidence *= 0.85  # Less aggressive hedge penalty (was 0.7)

    if score > 0.1:
        label = "positive"
    elif score < -0.1:
        label = "negative"
    else:
        label = "neutral"

    return {
        "sentiment_score": round(score, 4),
        "sentiment_confidence": round(confidence, 4),
        "label": label,
    }
