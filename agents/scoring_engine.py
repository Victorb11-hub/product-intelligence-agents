"""
Scoring Engine — Runs after all scrapers complete.
Computes 4-job weighted composite score with recency weighting.
Writes to scores_history and product_snapshots.
"""
import os
import math
import logging
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

def _env_float(key, default):
    try: return float(os.environ.get(key, default))
    except (ValueError, TypeError): return float(default)

def _env_int(key, default):
    try: return int(os.environ.get(key, default))
    except (ValueError, TypeError): return int(default)


def _purchase_signal_norm(count: int, platform_prefix: str) -> float:
    """Convert purchase_signal_count to 0-100 score using tier env vars."""
    t1 = _env_int(f"{platform_prefix}_PURCHASE_TIER_1", 5)
    t2 = _env_int(f"{platform_prefix}_PURCHASE_TIER_2", 20)
    t3 = _env_int(f"{platform_prefix}_PURCHASE_TIER_3", 50)
    t4 = _env_int(f"{platform_prefix}_PURCHASE_TIER_4", 100)
    if count <= 0: return 0
    if count <= t1: return 20
    if count <= t2: return 40
    if count <= t3: return 60
    if count <= t4: return 80
    return 100


def _negative_penalty(purchase_count: int, negative_count: int) -> float:
    """Compute penalty multiplier based on negative/purchase ratio."""
    if purchase_count <= 0:
        return 1.0  # No baseline to compare against
    ratio = negative_count / purchase_count
    if ratio > _env_float("NEGATIVE_RATIO_SEVERE", 0.50):
        return _env_float("NEGATIVE_PENALTY_SEVERE", 0.70)
    if ratio > _env_float("NEGATIVE_RATIO_MODERATE", 0.25):
        return _env_float("NEGATIVE_PENALTY_MODERATE", 0.85)
    if ratio > _env_float("NEGATIVE_RATIO_MILD", 0.10):
        return _env_float("NEGATIVE_PENALTY_MILD", 0.95)
    return 1.0


def _assert_weights_sum_to_one(weights: dict, label: str):
    """Log warning if weights don't sum to 1.0."""
    total = sum(weights.values())
    if abs(total - 1.0) > 0.01:
        logger.warning("[scoring] %s weights sum to %.3f, not 1.0 — normalizing", label, total)
        return {k: v / total for k, v in weights.items()}
    return weights

# Default settings — overridden by scoring_settings table in Supabase
_settings_cache = None


def _load_settings(db):
    """Load scoring settings from Supabase. Cache for the duration of one pipeline run."""
    global _settings_cache
    if _settings_cache is not None:
        return _settings_cache

    defaults = {
        'job1_weight': 0.30, 'job2_weight': 0.30, 'job3_weight': 0.25, 'job4_weight': 0.15,
        'buy_threshold': 75, 'watch_threshold': 55,
        'recency_0_7': 1.0, 'recency_8_14': 0.8, 'recency_15_21': 0.6, 'recency_22_30': 0.4,
        'lookback_days': 30, 'relevance_threshold': 0.1,
    }

    try:
        resp = db.table("scoring_settings").select("setting_key, setting_value").execute()
        for row in resp.data:
            defaults[row["setting_key"]] = row["setting_value"]
        logger.info("[scoring] Loaded %d settings from Supabase", len(resp.data))
    except Exception as e:
        logger.warning("[scoring] Failed to load settings, using defaults: %s", e)

    _settings_cache = defaults
    return defaults


def _recency_weight(scraped_date_str, settings=None):
    s = settings or {}
    try:
        d = datetime.strptime(scraped_date_str, "%Y-%m-%d").date()
        age = (date.today() - d).days
        if age <= 7: return s.get('recency_0_7', 1.0)
        if age <= 14: return s.get('recency_8_14', 0.8)
        if age <= 21: return s.get('recency_15_21', 0.6)
        if age <= 30: return s.get('recency_22_30', 0.4)
        return 0.0
    except Exception:
        return 0.5


def score_all_products(db, products, run_id):
    global _settings_cache
    _settings_cache = None  # Force reload on each pipeline run
    settings = _load_settings(db)

    for product in products:
        try:
            _score_product(db, product, run_id)
        except Exception as e:
            logger.error("Scoring failed for %s: %s", product["name"], e)


def _score_product(db, product, run_id):
    settings = _settings_cache or {}
    pid = product["id"]
    name = product["name"]

    # Gather signals with recency weighting
    reddit = _get_weighted_signal(db, "signals_social", pid, "reddit")
    tiktok = _get_weighted_signal(db, "signals_social", pid, "tiktok")
    instagram = _get_weighted_signal(db, "signals_social", pid, "instagram")
    gt = _get_weighted_signal(db, "signals_search", pid, "google_trends")
    amazon = _get_weighted_signal(db, "signals_retail", pid, "amazon")
    alibaba = _get_weighted_signal(db, "signals_supply", pid, "alibaba")

    # Job 1: Early Detection (TikTok 40%, Instagram 30%, YouTube 15%, X 10%, Pinterest 5%)
    job1 = None
    j1_parts = {}
    if tiktok:
        mentions = tiktok.get("mention_count", 0) or 0
        if mentions == 0:
            try:
                pc = db.table("posts").select("id").eq("product_id", pid).eq("platform", "tiktok").execute()
                mentions = len(pc.data or [])
            except Exception:
                pass
        likes = tiktok.get("total_upvotes", 0) or 0
        comments_raw = tiktok.get("total_comment_count", 0) or 0
        total_views = tiktok.get("total_views", 0) or 0

        # Engagement rate norm (log scale)
        avg_engagement = (likes + comments_raw) / max(mentions, 1)
        engagement_rate_norm = min(100, max(0, math.log10(max(avg_engagement, 1)) * 15))

        # Weighted comment intent (from Pass 2 — already 0-1, scale to 0-100)
        tt_intent = (tiktok.get("weighted_comment_intent") or
                     tiktok.get("avg_intent_score", 0) or 0) * 100
        tt_intent_norm = min(100, max(0, tt_intent))

        # Purchase signal normalization (tier-based)
        tt_purchase_count = tiktok.get("purchase_signal_count", 0) or 0
        tt_negative_count = tiktok.get("negative_signal_count", 0) or 0
        tt_purchase_norm = _purchase_signal_norm(tt_purchase_count, "TIKTOK")

        # Weights from env (must sum to 1.0)
        w = _assert_weights_sum_to_one({
            "intent": _env_float("TIKTOK_WEIGHT_INTENT", 0.45),
            "purchase": _env_float("TIKTOK_WEIGHT_PURCHASE", 0.25),
            "engagement": _env_float("TIKTOK_WEIGHT_ENGAGEMENT", 0.30),
        }, "TikTok")

        tiktok_sub = (tt_intent_norm * w["intent"] +
                      tt_purchase_norm * w["purchase"] +
                      engagement_rate_norm * w["engagement"])

        # Apply negative signal penalty
        penalty = _negative_penalty(tt_purchase_count, tt_negative_count)
        tiktok_sub *= penalty

        logger.info("[scoring] TikTok: intent=%.1f purchase=%.1f(%d) eng=%.1f penalty=%.2f → %.1f",
                    tt_intent_norm, tt_purchase_norm, tt_purchase_count,
                    engagement_rate_norm, penalty, tiktok_sub)
        j1_parts["tiktok"] = (tiktok_sub, 0.40)

    if instagram:
        ig_likes = instagram.get("total_upvotes", 0) or 0
        ig_comments_raw = instagram.get("total_comment_count", 0) or 0
        ig_mentions = instagram.get("mention_count", 0) or 0
        if ig_mentions == 0:
            try:
                pc = db.table("posts").select("id").eq("product_id", pid).eq("platform", "instagram").execute()
                ig_mentions = len(pc.data or [])
            except Exception:
                pass

        # Engagement per post (log scale, more aggressive)
        ig_avg_eng = (ig_likes + ig_comments_raw) / max(ig_mentions, 1)
        ig_engagement_norm = min(100, max(0, math.log10(max(ig_avg_eng, 1)) * 25))

        # Weighted comment intent (from Pass 2)
        ig_intent = (instagram.get("weighted_comment_intent") or
                     instagram.get("avg_intent_score", 0) or 0) * 100
        ig_intent_norm = min(100, max(0, ig_intent))

        # Purchase signal normalization
        ig_purchase_count = instagram.get("purchase_signal_count", 0) or 0
        ig_negative_count = instagram.get("negative_signal_count", 0) or 0
        ig_purchase_norm = _purchase_signal_norm(ig_purchase_count, "INSTAGRAM")

        # Weights from env (must sum to 1.0)
        w = _assert_weights_sum_to_one({
            "intent": _env_float("INSTAGRAM_WEIGHT_INTENT", 0.45),
            "purchase": _env_float("INSTAGRAM_WEIGHT_PURCHASE", 0.30),
            "engagement": _env_float("INSTAGRAM_WEIGHT_ENGAGEMENT", 0.25),
        }, "Instagram")

        ig_sub = (ig_intent_norm * w["intent"] +
                  ig_purchase_norm * w["purchase"] +
                  ig_engagement_norm * w["engagement"])

        # Apply negative signal penalty
        penalty = _negative_penalty(ig_purchase_count, ig_negative_count)
        ig_sub *= penalty

        logger.info("[scoring] Instagram: intent=%.1f purchase=%.1f(%d) eng=%.1f penalty=%.2f → %.1f",
                    ig_intent_norm, ig_purchase_norm, ig_purchase_count,
                    ig_engagement_norm, penalty, ig_sub)
        j1_parts["instagram"] = (ig_sub, 0.30)

    if j1_parts:
        j1_w = sum(w for _, w in j1_parts.values())
        if j1_w > 0:
            job1 = sum(s * (w / j1_w) for s, w in j1_parts.values())
            logger.info("[scoring] Job 1: %s = %.1f", {k: f"{s:.1f}" for k, (s, w) in j1_parts.items()}, job1)

    # Job 2: Demand Validation (Reddit 35%, GT 45%, Facebook 20%)
    job2 = None
    j2_parts = {}
    if reddit:
        # Weighted sentiment from Pass 2 (already in -1 to 1 range)
        weighted_sent = reddit.get("weighted_sentiment") or reddit.get("sentiment_score", 0) or 0
        weighted_sentiment_norm = max(0, min(100, (weighted_sent + 1) * 50))

        # Weighted comment intent (from Pass 2)
        r_intent = (reddit.get("weighted_comment_intent") or
                    reddit.get("avg_intent_score", 0) or 0) * 100
        r_intent_norm = min(100, max(0, r_intent))

        # Purchase signal normalization
        r_purchase_count = reddit.get("purchase_signal_count", 0) or 0
        r_negative_count = reddit.get("negative_signal_count", 0) or 0
        r_purchase_norm = _purchase_signal_norm(r_purchase_count, "REDDIT")

        # Volume (log of mentions/posts)
        r_volume_norm = min(100, max(0, math.log10(max(reddit.get("mention_count", 1), 1)) * 50))

        # Weights from env (must sum to 1.0)
        w = _assert_weights_sum_to_one({
            "intent": _env_float("REDDIT_WEIGHT_INTENT", 0.40),
            "purchase": _env_float("REDDIT_WEIGHT_PURCHASE", 0.25),
            "sentiment": _env_float("REDDIT_WEIGHT_SENTIMENT", 0.20),
            "volume": _env_float("REDDIT_WEIGHT_VOLUME", 0.15),
        }, "Reddit")

        reddit_sub = (r_intent_norm * w["intent"] +
                      r_purchase_norm * w["purchase"] +
                      weighted_sentiment_norm * w["sentiment"] +
                      r_volume_norm * w["volume"])

        # Apply negative signal penalty
        penalty = _negative_penalty(r_purchase_count, r_negative_count)
        reddit_sub *= penalty

        logger.info("[scoring] Reddit: intent=%.1f purchase=%.1f(%d) sent=%.1f vol=%.1f penalty=%.2f → %.1f",
                    r_intent_norm, r_purchase_norm, r_purchase_count,
                    weighted_sentiment_norm, r_volume_norm, penalty, reddit_sub)
        j2_parts["reddit"] = (reddit_sub, 0.35)

    if gt:
        slope = gt.get("slope_24m", 0) or 0
        yoy = gt.get("yoy_growth", 0) or 0
        breakout = gt.get("breakout_flag", False)
        s_norm = max(0, min(100, (slope + 0.01) / 0.02 * 100))
        y_norm = max(0, min(100, (yoy + 0.5) * 100))
        bonus = 0 if breakout else 20
        j2_parts["gt"] = (s_norm * 0.4 + y_norm * 0.4 + bonus, 0.45)

    if j2_parts:
        total_w = sum(w for _, w in j2_parts.values())
        if total_w > 0:
            job2 = sum(score * (w / total_w) for score, w in j2_parts.values())
        else:
            job2 = None

    # Job 3: Purchase Intent (Amazon 50%, Etsy 30%, Walmart 20%)
    job3 = None
    if amazon:
        # Monthly purchase volume — strongest purchase confirmation
        mpv = amazon.get("monthly_purchase_volume", 0) or 0
        t1 = _env_float("AMAZON_VOLUME_TIER_1", 100)
        t2 = _env_float("AMAZON_VOLUME_TIER_2", 500)
        t3 = _env_float("AMAZON_VOLUME_TIER_3", 1000)
        t4 = _env_float("AMAZON_VOLUME_TIER_4", 5000)
        t5 = _env_float("AMAZON_VOLUME_TIER_5", 10000)
        if mpv >= t5: vol_norm = 100
        elif mpv >= t4: vol_norm = 90
        elif mpv >= t3: vol_norm = 70
        elif mpv >= t2: vol_norm = 50
        elif mpv >= t1: vol_norm = 30
        elif mpv > 0: vol_norm = 10
        else: vol_norm = 0  # No monthly volume data

        # BSR score — use bsr_rank_actual if available, fall back to bestseller_rank
        rank = amazon.get("bsr_rank_actual") or amazon.get("bestseller_rank") or 0
        bsr_trend = amazon.get("bsr_trend", "unknown")
        bt1 = _env_float("AMAZON_BSR_TIER_1", 100)
        bt2 = _env_float("AMAZON_BSR_TIER_2", 1000)
        bt3 = _env_float("AMAZON_BSR_TIER_3", 10000)
        bt4 = _env_float("AMAZON_BSR_TIER_4", 50000)
        bt5 = _env_float("AMAZON_BSR_TIER_5", 100000)
        if rank and rank > 0:
            if rank < bt1: bsr_norm = 90
            elif rank < bt2: bsr_norm = 70
            elif rank < bt3: bsr_norm = 50
            elif rank < bt4: bsr_norm = 30
            elif rank < bt5: bsr_norm = 10
            else: bsr_norm = 0
            if bsr_trend == "rising": bsr_norm = min(100, bsr_norm + 10)
            elif bsr_trend == "declining": bsr_norm = max(0, bsr_norm - 10)
        else:
            bsr_norm = 40  # No BSR data — neutral

        # Satisfaction score (includes negative review penalty implicitly)
        satisfaction = amazon.get("satisfaction_score", 0) or 0
        satisfaction_norm = max(0, min(100, satisfaction))

        # Repeat purchase — use repeat_purchase_mentions from AI topic extraction
        # (falls back to monthly_volume proxy in the agent when no AI topic found)
        repeat_mentions = amazon.get("repeat_purchase_mentions", 0) or 0
        rt1 = _env_int("AMAZON_REPEAT_TIER_1", 50)
        rt2 = _env_int("AMAZON_REPEAT_TIER_2", 150)
        rt3 = _env_int("AMAZON_REPEAT_TIER_3", 300)
        rt4 = _env_int("AMAZON_REPEAT_TIER_4", 500)
        if repeat_mentions <= 0: repeat_norm = 0
        elif repeat_mentions <= rt1: repeat_norm = 20
        elif repeat_mentions <= rt2: repeat_norm = 40
        elif repeat_mentions <= rt3: repeat_norm = 60
        elif repeat_mentions <= rt4: repeat_norm = 80
        else: repeat_norm = 100

        # Review velocity — monthly rate
        vel_monthly = amazon.get("review_velocity_monthly", 0) or 0
        vel_norm = min(100, max(0, math.log10(max(vel_monthly + 1, 1)) * 30))

        # Weights from env (must sum to 1.0)
        w_vol = _env_float("AMAZON_WEIGHT_MONTHLY_VOLUME", 0.30)
        w_rep = _env_float("AMAZON_WEIGHT_REPEAT_PURCHASE", 0.25)
        w_sat = _env_float("AMAZON_WEIGHT_SATISFACTION", 0.20)
        w_bsr = _env_float("AMAZON_WEIGHT_BSR", 0.15)
        w_vel = _env_float("AMAZON_WEIGHT_REVIEW_VELOCITY", 0.10)
        w_sum = w_vol + w_rep + w_sat + w_bsr + w_vel
        if abs(w_sum - 1.0) > 0.01:
            logger.warning("[scoring] Amazon weights sum to %.3f, not 1.0 — normalizing", w_sum)
            w_vol, w_rep, w_sat, w_bsr, w_vel = w_vol/w_sum, w_rep/w_sum, w_sat/w_sum, w_bsr/w_sum, w_vel/w_sum

        job3 = vol_norm * w_vol + repeat_norm * w_rep + satisfaction_norm * w_sat + bsr_norm * w_bsr + vel_norm * w_vel
        logger.info("[scoring] Job 3: vol=%.1f(%d/mo) repeat=%.1f(%d mentions) satisfaction=%.1f bsr=%.1f(%s) vel=%.1f → %.1f",
                    vol_norm, mpv, repeat_norm, repeat_mentions, satisfaction_norm, bsr_norm, bsr_trend, vel_norm, job3)

    # Job 4: Supply Readiness (Alibaba 100%)
    job4 = None
    if alibaba:
        suppliers = alibaba.get("supplier_listing_count", 0) or 0
        moq = alibaba.get("moq_current", 1000) or 1000
        s_norm = min(100, suppliers * 3)
        m_norm = max(0, min(100, (1000 - moq) / 10))
        job4 = s_norm * 0.5 + m_norm * 0.5

    # Redistribute weights — read from settings
    j1w = settings.get('job1_weight', 0.30)
    j2w = settings.get('job2_weight', 0.30)
    j3w = settings.get('job3_weight', 0.25)
    j4w = settings.get('job4_weight', 0.15)
    buy_thresh = settings.get('buy_threshold', 75)
    watch_thresh = settings.get('watch_threshold', 55)

    total_jobs = 4
    jobs = {"early_detection": (j1w, job1), "demand_validation": (j2w, job2),
            "purchase_intent": (j3w, job3), "supply_readiness": (j4w, job4)}
    active = {k: v for k, v in jobs.items() if v[1] is not None}
    if not active:
        logger.warning("No data for %s — score 0", name)
        return

    total_w = sum(w for w, _ in active.values())
    if total_w <= 0:
        logger.warning("Total weight is 0 for %s — cannot compute score", name)
        return
    raw_score = sum(score * (w / total_w) for w, score in active.values())
    raw_score = round(max(0, min(100, raw_score)), 1)

    # Coverage penalty applied FIRST (reduces score for incomplete data)
    coverage_ratio = len(active) / total_jobs
    penalty_floor = _env_float("COVERAGE_PENALTY_FLOOR", 0.5)
    coverage_penalty = penalty_floor + (coverage_ratio * (1.0 - penalty_floor))
    adjusted = raw_score * coverage_penalty

    # Quality multiplier applied AFTER coverage
    quality = reddit.get("data_quality_score", 0.5) if reddit else 0.5
    composite = adjusted * (0.7 + quality * 0.3)
    if composite > 100 or composite < 0:
        logger.warning("[scoring] %s: composite out of bounds (%.1f) — clamping", name, composite)
    composite = round(max(0, min(100, composite)), 1)
    data_coverage_pct = round(coverage_ratio * 100)

    verdict = "buy" if composite >= buy_thresh else "watch" if composite >= watch_thresh else "pass"

    # Apply override rules from formula_rules table
    composite, verdict = _apply_override_rules(db, pid, composite, verdict, data_coverage_pct, reddit, gt)

    logger.info("[scoring] %s: raw=%.1f coverage=%d%% adjusted=%.1f → %s",
                name, raw_score, data_coverage_pct, composite, verdict)

    # Phase classification — uses GT slope as primary, Reddit velocity as secondary
    gt_slope = gt.get("slope_24m") if gt else None
    gt_yoy = gt.get("yoy_growth", 0) if gt else None
    reddit_velocity = reddit.get("velocity", 0) if reddit else 0

    # Check Reddit post volume for velocity cross-check
    reddit_dates = 0
    try:
        rd = db.table("posts").select("scraped_date").eq("product_id", pid).eq("platform", "reddit").eq("data_type", "post").execute()
        reddit_dates = len(set(r["scraped_date"] for r in (rd.data or [])))
    except Exception:
        pass
    has_reddit_velocity = reddit_dates >= 2

    # Score sustained above 75 = peak
    if composite >= 75:
        phase = "peak"
    # PRIMARY: GT slope determines phase when available
    elif gt_slope is not None:
        if gt_slope > 0.003 and (gt_yoy or 0) > 0.5:
            # Rising demand — cross-check with Reddit
            if has_reddit_velocity and reddit_velocity < -0.1:
                phase = "plateau"  # GT rising but Reddit falling = conflicting
                logger.info("[scoring] Phase: plateau (GT rising but Reddit velocity negative)")
            else:
                phase = "buy_window"
                logger.info("[scoring] Phase: buy_window (GT slope %.4f, YoY %.0f%%)", gt_slope, (gt_yoy or 0) * 100)
        elif gt_slope > 0 and (gt_yoy or 0) > 0:
            phase = "plateau"  # Positive but not strong enough for buy_window
        elif gt_slope < -0.003:
            if has_reddit_velocity and reddit_velocity > 0.1:
                phase = "plateau"  # GT declining but Reddit rising = conflicting
            else:
                phase = "declining"
                logger.info("[scoring] Phase: declining (GT slope %.4f)", gt_slope)
        else:
            phase = "plateau"  # Flat slope
    # TERTIARY: No GT data and insufficient Reddit history
    else:
        phase = "early"
        logger.info("[scoring] Phase: early (no GT data, %d Reddit dates)", reddit_dates)

    # Fad flag: only set when GT has run (Fix 2)
    fad_flag = None  # null = insufficient data
    if gt:
        fad_flag = gt.get("breakout_flag", False)

    # Get old score for change tracking
    old = db.table("products").select("current_score").eq("id", pid).execute()
    old_score = old.data[0]["current_score"] if old.data else 0

    # Update product with both raw and adjusted scores + coverage
    db.table("products").update({
        "current_score": composite, "current_verdict": verdict,
        "lifecycle_phase": phase, "fad_flag": fad_flag if fad_flag is not None else False,
        "raw_score": raw_score,
        "coverage_pct": data_coverage_pct,
        "active_jobs": len(active),
        "total_jobs": total_jobs,
    }).eq("id", pid).execute()

    # Write scores_history
    db.table("scores_history").insert({
        "product_id": pid, "scored_date": date.today().isoformat(),
        "composite_score": composite,
        "demand_validation_score": job2,
        "purchase_intent_score": job3,
        "supply_readiness_score": job4,
        "verdict": verdict,
        "verdict_reasoning": f"Score {composite} from {len(active)} active jobs. {', '.join(active.keys())}.",
        "score_change": round(composite - old_score, 1),
        "data_confidence": quality,
        "platforms_used": list(j2_parts.keys()) + (["amazon"] if job3 else []) + (["alibaba"] if job4 else []),
    }).execute()

    # Write product snapshot (delete + insert to avoid upsert issues)
    snapshot_data = {
        "product_id": pid, "snapshot_date": date.today().isoformat(),
        "composite_score": composite, "verdict": verdict, "lifecycle_phase": phase,
        "reddit_mentions": reddit.get("mention_count") if reddit else None,
        "reddit_sentiment": reddit.get("sentiment_score") if reddit else None,
        "reddit_intent": reddit.get("avg_intent_score") if reddit else None,
        "gt_slope": gt.get("slope_24m") if gt else None,
        "gt_yoy_growth": gt.get("yoy_growth") if gt else None,
        "platforms_active": len(active),
        "data_confidence": quality,
    }
    try:
        db.table("product_snapshots").delete() \
            .eq("product_id", pid).eq("snapshot_date", date.today().isoformat()).execute()
        db.table("product_snapshots").insert(snapshot_data).execute()
    except Exception as e:
        logger.warning("[scoring] Snapshot write failed: %s", str(e)[:100])

    logger.info("[scoring] %s: %.1f %s (phase=%s, jobs=%d)", name, composite, verdict, phase, len(active))


def _apply_override_rules(db, pid, composite, verdict, coverage_pct, reddit, gt):
    """Read formula_rules from Supabase and apply enabled overrides."""
    try:
        resp = db.table("formula_rules").select("*").eq("enabled", True).execute()
        rules = resp.data or []
    except Exception:
        return composite, verdict

    for rule in rules:
        name = rule.get("rule_name", "")
        rtype = rule.get("rule_type", "")
        thresh = rule.get("threshold_value", 0) or 0
        adj = rule.get("adjustment_value", 0) or 0
        triggered = False

        if name == "Fad Override":
            if gt and gt.get("breakout_flag") and gt.get("slope_24m", 0) < 0.005:
                fad_score = 0.7  # approximate — would need full fad classifier
                if fad_score > thresh:
                    verdict = "pass"
                    triggered = True

        elif name == "Insufficient Data":
            if coverage_pct < thresh:
                if verdict == "buy":
                    verdict = "watch"
                    triggered = True

        elif name == "Reddit Pushback":
            if reddit and (reddit.get("sentiment_score", 0) or 0) < thresh:
                if verdict == "buy":
                    verdict = "watch"
                    triggered = True

        elif name == "Competitor OOS Bonus":
            # Would need competitor data — skip for now
            pass

        elif name == "Multi-Platform Corroboration":
            # Would need cross-platform count — skip for now
            pass

        if rtype == "score_adjustment" and triggered:
            composite = round(min(100, composite + adj), 1)

        if triggered:
            try:
                db.table("formula_rules").update({
                    "last_triggered": datetime.now().isoformat(),
                    "trigger_count": (rule.get("trigger_count", 0) or 0) + 1,
                }).eq("id", rule["id"]).execute()
            except Exception:
                pass

    return composite, verdict


def _get_weighted_signal(db, table, product_id, platform):
    """Get most recent signal row within 30 days with actual data, applying recency weight."""
    thirty_ago = (date.today() - timedelta(days=30)).isoformat()
    try:
        # Get up to 5 recent rows and pick the first one with actual data
        resp = db.table(table).select("*") \
            .eq("product_id", product_id).eq("platform", platform) \
            .gte("scraped_date", thirty_ago) \
            .order("scraped_date", desc=True).limit(5).execute()
        for row in (resp.data or []):
            w = _recency_weight(row.get("scraped_date", ""))
            if w <= 0:
                continue
            # Skip empty signal rows (0 mentions = no actual data)
            mention = row.get("mention_count", 0) or 0
            if mention > 0 or table != "signals_social":
                return row
        # If all rows have 0 mentions, return the most recent anyway
        if resp.data:
            return resp.data[0]
        return None
    except Exception:
        return None
