"""
Amazon Agent — junglee/amazon-crawler ($4.00/1K)
Fast two-pass design (~5 min, ~$0.12/product):
  Pass 1: Keyword search → top 10 by reviews (30 results, 3 min timeout)
  Pass 2: Product pages for BSR + rating distribution (10 ASINs, 5 min timeout)
No review text scraping — review count, rating, and distribution are sufficient.
Settings read from scoring_settings table (amazon_search_results, amazon_top_n, etc).
Feeds Job 3 — Purchase Intent (50% weight).
"""
import os
import logging
from datetime import date
from urllib.parse import quote_plus

from .base_agent import BaseAgent
from .config import APIFY_API_TOKEN, APIFY_ACTORS
from .skills.apify_helper import run_actor

def _env_int(key, default):
    try: return int(os.environ.get(key, default))
    except (ValueError, TypeError): return default

logger = logging.getLogger(__name__)


class AmazonAgent(BaseAgent):
    PLATFORM = "amazon"
    SIGNAL_TABLE = "signals_retail"
    REQUIRED_CREDENTIALS = ["APIFY_API_TOKEN"]

    def _load_amazon_settings(self):
        """Load configurable settings from scoring_settings table."""
        defaults = {
            "amazon_search_results": 30,
            "amazon_top_n": 10,
            "amazon_min_reviews": 500,
            "amazon_one_star_alert_threshold": 3.0,
        }
        try:
            resp = self.supabase.table("scoring_settings").select("setting_key, setting_value") \
                .in_("setting_key", list(defaults.keys())).execute()
            for row in (resp.data or []):
                defaults[row["setting_key"]] = row["setting_value"]
        except Exception:
            pass
        return defaults

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        cfg = self._load_amazon_settings()
        search_max = int(cfg["amazon_search_results"])
        top_n = int(cfg["amazon_top_n"])
        min_reviews = int(cfg["amazon_min_reviews"])

        primary_keyword = keywords[0] if keywords else product_name
        search_query = quote_plus(primary_keyword)
        search_url = f"https://www.amazon.com/s?k={search_query}"

        logger.info("[amazon] Pass 1: searching %s (max=%d, top_n=%d, min_reviews=%d)",
                    search_url, search_max, top_n, min_reviews)

        # ═══════════════════════════════════════
        # PASS 1 — Keyword search (3 min timeout)
        # ═══════════════════════════════════════
        try:
            items = run_actor(
                actor_id=APIFY_ACTORS["amazon"],
                run_input={
                    "categoryOrProductUrls": [{"url": search_url}],
                    "maxItemsPerStartUrl": search_max,
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=_env_int("AMAZON_PASS1_TIMEOUT", 180),
                max_items=search_max + 5,
            )
        except Exception as e:
            raise ValueError(f"Amazon search failed (timeout?): {str(e)[:200]}")

        if not items:
            raise ValueError(f"No Amazon products found for {primary_keyword}")

        logger.info("[amazon] Pass 1: %d results returned", len(items))

        # Parse items
        products_found = []
        for item in items:
            asin = item.get("asin") or item.get("ASIN") or ""
            title = item.get("title") or item.get("name") or ""

            price = item.get("price") or item.get("currentPrice") or item.get("salePrice") or 0
            if isinstance(price, dict):
                price = price.get("value") or price.get("amount") or 0
            if isinstance(price, str):
                try: price = float(price.replace("$", "").replace(",", "").strip() or "0")
                except ValueError: price = 0
            if not isinstance(price, (int, float)):
                price = 0

            rating = item.get("stars") or item.get("rating") or item.get("averageRating") or 0
            if isinstance(rating, dict):
                rating = rating.get("value") or rating.get("average") or 0
            if isinstance(rating, str):
                try: rating = float(rating.split()[0])
                except (ValueError, IndexError): rating = 0

            review_count = (item.get("reviewsCount") or item.get("reviews_count") or
                           item.get("numberOfReviews") or item.get("ratingsCount") or 0)
            if isinstance(review_count, str):
                try: review_count = int(review_count.replace(",", "").strip() or "0")
                except ValueError: review_count = 0

            sponsored = item.get("sponsored") or item.get("isSponsored") or False
            url = item.get("url") or item.get("productUrl") or ""
            if not url and asin:
                url = f"https://www.amazon.com/dp/{asin}"

            if sponsored or not asin:
                continue

            products_found.append({
                "asin": asin, "title": title, "price": price,
                "rating": rating, "review_count": review_count,
                "url": url, "raw": item,
            })

        # Filter by min reviews, sort by review count, keep top N
        qualified = [p for p in products_found if p["review_count"] >= min_reviews]
        if len(qualified) < 3:
            # Relax if too few qualify
            qualified = [p for p in products_found if p["review_count"] > 0]
            logger.info("[amazon] Pass 1: relaxed min_reviews filter → %d products", len(qualified))

        qualified.sort(key=lambda p: p["review_count"], reverse=True)
        top_products = qualified[:top_n]

        if not top_products:
            raise ValueError(f"No qualifying Amazon products for {primary_keyword}")

        logger.info("[amazon] Pass 1: top %d → %s",
                    len(top_products), [(p["title"][:40], p["review_count"]) for p in top_products[:3]])

        # Write products to posts table
        for p in top_products:
            try:
                self.supabase.table("posts").insert({
                    "product_id": product["id"],
                    "run_id": self.run_id,
                    "platform": "amazon",
                    "post_title": (p["title"] or "")[:500],
                    "post_body": f"ASIN: {p['asin']} | ${p['price']:.2f} | {p['rating']}★ | {p['review_count']} reviews",
                    "post_url": p["url"][:2000] if p["url"] else None,
                    "upvotes": p["review_count"],
                    "comment_count": 0,
                    "scraped_date": date.today().isoformat(),
                    "data_type": "post",
                    "anomaly_flag": False,
                    "raw_json": {"asin": p["asin"], "price": p["price"],
                                 "rating": p["rating"], "bsr": None},
                }).execute()
            except Exception as e:
                logger.error("[amazon] Failed to write post: %s", str(e)[:200])

        # Aggregate Pass 1 metrics
        prices = [p["price"] for p in top_products if p["price"] > 0]
        avg_price = sum(prices) / len(prices) if prices else 0
        ratings = [p["rating"] for p in top_products if p["rating"] > 0]
        avg_rating = sum(ratings) / len(ratings) if ratings else 0
        total_reviews = sum(p["review_count"] for p in top_products)

        # ═══════════════════════════════════════
        # PASS 2 — Product pages for BSR + rating distribution (5 min timeout)
        # ═══════════════════════════════════════
        asin_urls = [f"https://www.amazon.com/dp/{p['asin']}" for p in top_products]
        pass2 = self._fetch_product_details(asin_urls)

        # ═══════════════════════════════════════
        # TREND CALCULATIONS (no Apify cost)
        # ═══════════════════════════════════════
        prev = self._get_previous_signal(product["id"])

        prev_reviews = prev.get("review_count", total_reviews) if prev else total_reviews
        review_velocity = total_reviews - prev_reviews

        prev_bsr = prev.get("bestseller_rank") if prev else None
        best_bsr = pass2["best_bsr"]
        bsr_change = (prev_bsr - best_bsr) if (prev_bsr and best_bsr) else 0
        if bsr_change > 0: bsr_trend = "rising"
        elif bsr_change < 0: bsr_trend = "declining"
        else: bsr_trend = "stable"

        prev_rating = prev.get("avg_rating") if prev else None
        if prev_rating and avg_rating:
            rating_delta = avg_rating - prev_rating
            rating_trend = "improving" if rating_delta > 0.05 else "declining" if rating_delta < -0.05 else "stable"
        else:
            rating_trend = "unknown"

        # Calculate satisfaction score from rating distribution
        dist = pass2["rating_dist"]
        satisfaction = (
            dist["five_star"] * 1.0 +
            dist["four_star"] * 0.75 +
            dist["three_star"] * 0.5 +
            dist["two_star"] * 0.25 +
            dist["one_star"] * 0.0
        )

        # 1★ alert check
        prev_one_star = prev.get("one_star_pct", 0) if prev else 0
        one_star_increase = dist["one_star"] - (prev_one_star or 0)
        threshold = float(cfg["amazon_one_star_alert_threshold"])
        if one_star_increase >= threshold and prev_one_star > 0:
            self._fire_one_star_alert(product, dist["one_star"], prev_one_star, one_star_increase)

        logger.info("[amazon] Done: %d products, avg_rating=%.1f, total_reviews=%d, "
                    "best_bsr=%s, satisfaction=%.1f, 1★=%.1f%%, velocity=%+d, bsr_trend=%s",
                    len(top_products), avg_rating, total_reviews,
                    best_bsr or "N/A", satisfaction, dist["one_star"],
                    review_velocity, bsr_trend)

        return {
            "texts": [],
            "raw_items": [p["raw"] for p in top_products],
            "data_dates": [date.today().isoformat()],
            "mention_count": len(top_products),
            "products_found": len(products_found),
            "bestseller_rank": best_bsr,
            "best_bsr_category": pass2["best_bsr_category"],
            "review_count": total_reviews,
            "review_count_growth": round(review_velocity / max(prev_reviews, 1), 4),
            "review_velocity": review_velocity,
            "avg_rating": round(avg_rating, 2),
            "price": round(avg_price, 2) if avg_price else None,
            "bsr_change": bsr_change,
            "bsr_trend": bsr_trend,
            "rating_trend": rating_trend,
            "review_sentiment": round(avg_rating / 5.0, 4) if avg_rating else 0,
            "satisfaction_score": round(satisfaction, 2),
            "five_star_pct": round(dist["five_star"], 1),
            "four_star_pct": round(dist["four_star"], 1),
            "three_star_pct": round(dist["three_star"], 1),
            "two_star_pct": round(dist["two_star"], 1),
            "one_star_pct": round(dist["one_star"], 1),
            "total_ratings": pass2["total_ratings"],
            "monthly_purchase_volume": pass2.get("monthly_purchase_volume", 0),
            "ai_review_summary": pass2.get("ai_review_summary"),
            "ai_review_sentiment": pass2.get("ai_review_sentiment", 0),
            "review_velocity_monthly": pass2.get("review_velocity_monthly", 0),
            "out_of_stock_flag": False,
            "search_rank": 1,
        }

    def _fetch_product_details(self, asin_urls: list[str]) -> dict:
        """Pass 2: Fetch product pages for BSR + rating distribution."""
        empty = {"best_bsr": None, "best_bsr_category": "",
                 "total_ratings": 0,
                 "rating_dist": {"five_star": 0, "four_star": 0, "three_star": 0,
                                 "two_star": 0, "one_star": 0},
                 "monthly_purchase_volume": 0,
                 "ai_review_summary": None, "ai_review_sentiment": 0,
                 "review_velocity_monthly": 0}
        if not asin_urls:
            return empty

        logger.info("[amazon] Pass 2: fetching %d product pages for BSR + ratings", len(asin_urls))

        try:
            items = run_actor(
                actor_id=APIFY_ACTORS["amazon"],
                run_input={
                    "categoryOrProductUrls": [{"url": u} for u in asin_urls],
                    "maxItemsPerStartUrl": 1,
                },
                api_token=APIFY_API_TOKEN,
                timeout_secs=_env_int("AMAZON_PASS2_TIMEOUT", 300),
                max_items=len(asin_urls) + 2,
            )
        except Exception as e:
            logger.warning("[amazon] Pass 2 failed (timeout?): %s", str(e)[:200])
            return empty

        best_bsr = None
        best_category = ""
        all_dists = []
        total_ratings_sum = 0
        total_monthly_volume = 0
        ai_summaries = []

        for item in items:
            # BSR extraction
            bsr = item.get("bestSellerRank") or item.get("salesRank") or item.get("bsr") or None
            if isinstance(bsr, dict):
                category = bsr.get("category") or bsr.get("categoryName") or ""
                bsr = bsr.get("rank") or bsr.get("value") or None
            elif isinstance(bsr, list) and bsr:
                entry = bsr[0] if isinstance(bsr[0], dict) else {"rank": bsr[0]}
                category = entry.get("category") or entry.get("categoryName") or ""
                bsr = entry.get("rank") or entry.get("value") or (bsr[0] if not isinstance(bsr[0], dict) else None)
            else:
                category = item.get("bestSellerCategory") or item.get("categoryName") or ""
            if isinstance(bsr, str):
                try: bsr = int(bsr.replace(",", "").replace("#", "").strip())
                except (ValueError, IndexError): bsr = None
            if bsr and isinstance(bsr, (int, float)):
                if best_bsr is None or bsr < best_bsr:
                    best_bsr = int(bsr)
                    best_category = category

            # Rating distribution extraction
            # junglee/amazon-crawler returns starsBreakdown as either:
            #   decimals: {"5star": 0.79, "4star": 0.09, ...}  (fractions summing to ~1.0)
            #   strings:  {"5star": "79%", "4star": "9%", ...}  (percentage strings)
            breakdown = item.get("starsBreakdown") or {}
            if breakdown:
                dist = {}
                for key, star_key in [("5star", "five_star"), ("4star", "four_star"),
                                       ("3star", "three_star"), ("2star", "two_star"),
                                       ("1star", "one_star")]:
                    val = breakdown.get(key) or 0
                    if isinstance(val, str):
                        try: val = float(val.replace("%", "").strip())
                        except ValueError: val = 0
                    dist[star_key] = val
                # Detect fraction format: if all values sum to ~1.0, multiply by 100
                total = sum(dist.values())
                if 0.5 < total < 1.5:
                    dist = {k: v * 100 for k, v in dist.items()}
                all_dists.append(dist)

            # Total ratings count
            ratings_count = item.get("reviewsCount") or item.get("ratingsCount") or 0
            if isinstance(ratings_count, str):
                try: ratings_count = int(ratings_count.replace(",", "").strip())
                except ValueError: ratings_count = 0
            total_ratings_sum += ratings_count

            # Monthly purchase volume: "1K+ bought in past month" badge
            mpv = item.get("monthlyPurchaseVolume") or item.get("boughtInLastMonth") or ""
            if isinstance(mpv, str):
                mpv_lower = mpv.lower().replace(",", "").replace("+", "").strip()
                try:
                    if "k" in mpv_lower:
                        mpv_num = int(float(mpv_lower.replace("k", "").split()[0]) * 1000)
                    else:
                        # Extract first number from string
                        import re
                        nums = re.findall(r'\d+', mpv_lower)
                        mpv_num = int(nums[0]) if nums else 0
                except (ValueError, IndexError):
                    mpv_num = 0
            elif isinstance(mpv, (int, float)):
                mpv_num = int(mpv)
            else:
                mpv_num = 0
            total_monthly_volume += mpv_num

            # AI review summary
            ai_summary = item.get("aiReviewsSummary") or {}
            if isinstance(ai_summary, dict):
                summary_text = ai_summary.get("text") or ai_summary.get("summary") or ""
            elif isinstance(ai_summary, str):
                summary_text = ai_summary
            else:
                summary_text = ""
            if summary_text:
                ai_summaries.append(summary_text)

            # Also check bestsellerRanks array (more detailed than single BSR)
            bsr_array = item.get("bestsellerRanks") or item.get("bestSellerRanks") or []
            if isinstance(bsr_array, list):
                for bsr_entry in bsr_array:
                    if isinstance(bsr_entry, dict):
                        rank_val = bsr_entry.get("rank") or bsr_entry.get("value") or None
                        cat_val = bsr_entry.get("category") or bsr_entry.get("categoryName") or ""
                        if isinstance(rank_val, str):
                            try: rank_val = int(rank_val.replace(",", "").replace("#", "").strip())
                            except (ValueError, IndexError): rank_val = None
                        if rank_val and isinstance(rank_val, (int, float)):
                            if best_bsr is None or rank_val < best_bsr:
                                best_bsr = int(rank_val)
                                best_category = cat_val

        # Average the distributions across all products
        avg_dist = {"five_star": 0, "four_star": 0, "three_star": 0, "two_star": 0, "one_star": 0}
        if all_dists:
            for key in avg_dist:
                avg_dist[key] = sum(d[key] for d in all_dists) / len(all_dists)

        # AI review sentiment
        ai_summary_combined = " ".join(ai_summaries) if ai_summaries else None
        ai_sentiment = 0
        if ai_summary_combined:
            from .skills.sentiment import analyze_sentiment as _sent
            ai_sentiment = _sent(ai_summary_combined).get("sentiment_score", 0)

        # Review velocity monthly estimate
        review_vel_monthly = total_ratings_sum / 12 if total_ratings_sum > 0 else 0

        logger.info("[amazon] Pass 2: BSR=%s, ratings=%d, monthly_vol=%d, "
                    "dist=[5★:%.0f%% 4★:%.0f%% 3★:%.0f%% 2★:%.0f%% 1★:%.0f%%], "
                    "ai_summaries=%d, ai_sentiment=%.2f",
                    best_bsr or "N/A", total_ratings_sum, total_monthly_volume,
                    avg_dist["five_star"], avg_dist["four_star"], avg_dist["three_star"],
                    avg_dist["two_star"], avg_dist["one_star"],
                    len(ai_summaries), ai_sentiment)

        return {
            "best_bsr": best_bsr,
            "best_bsr_category": best_category,
            "total_ratings": total_ratings_sum,
            "rating_dist": avg_dist,
            "monthly_purchase_volume": total_monthly_volume,
            "ai_review_summary": ai_summary_combined,
            "ai_review_sentiment": round(ai_sentiment, 4),
            "review_velocity_monthly": round(review_vel_monthly, 1),
        }

    def _fire_one_star_alert(self, product, current_pct, prev_pct, increase):
        """Fire alert when 1-star reviews increase beyond threshold."""
        try:
            self.supabase.table("alerts").insert({
                "product_id": product["id"],
                "alert_type": "fad_warning",
                "priority": "warning",
                "message": (f"{product['name']}: 1★ reviews increased from {prev_pct:.1f}% to {current_pct:.1f}% "
                           f"(+{increase:.1f}%). Possible quality issue — investigate before sourcing."),
                "actioned": False,
            }).execute()
            logger.warning("[amazon] 1★ alert fired: %s +%.1f%%", product["name"], increase)
        except Exception as e:
            logger.error("[amazon] Failed to fire 1★ alert: %s", str(e)[:200])

    def _get_previous_signal(self, product_id: str) -> dict | None:
        """Get the most recent previous signal row for trend calculations."""
        try:
            resp = self.supabase.table("signals_retail") \
                .select("*") \
                .eq("product_id", product_id) \
                .eq("platform", "amazon") \
                .order("scraped_date", desc=True) \
                .limit(1) \
                .execute()
            return resp.data[0] if resp.data else None
        except Exception:
            return None

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id,
            "scraped_date": date.today().isoformat(),
            "platform": "amazon",
            "products_found": raw_data.get("products_found", 0),
            "bestseller_rank": raw_data.get("bestseller_rank"),
            "best_bsr_category": raw_data.get("best_bsr_category", ""),
            "rank_change_wow": raw_data.get("bsr_change", 0),
            "bsr_trend": raw_data.get("bsr_trend", "unknown"),
            "review_count": raw_data.get("review_count", 0),
            "review_count_growth": raw_data.get("review_count_growth", 0),
            "review_velocity": raw_data.get("review_velocity", 0),
            "avg_rating": raw_data.get("avg_rating"),
            "rating_trend": raw_data.get("rating_trend", "unknown"),
            "review_sentiment": raw_data.get("review_sentiment"),
            "satisfaction_score": raw_data.get("satisfaction_score", 0),
            "five_star_pct": raw_data.get("five_star_pct", 0),
            "four_star_pct": raw_data.get("four_star_pct", 0),
            "three_star_pct": raw_data.get("three_star_pct", 0),
            "two_star_pct": raw_data.get("two_star_pct", 0),
            "one_star_pct": raw_data.get("one_star_pct", 0),
            "total_ratings": raw_data.get("total_ratings", 0),
            "search_rank": raw_data.get("search_rank"),
            "out_of_stock_flag": raw_data.get("out_of_stock_flag", False),
            "price": raw_data.get("price"),
            "mention_count": raw_data.get("mention_count", 0),
            "monthly_purchase_volume": raw_data.get("monthly_purchase_volume", 0),
            "bsr_rank_actual": raw_data.get("bestseller_rank") or 0,
            "ai_review_summary": raw_data.get("ai_review_summary"),
            "ai_review_sentiment": raw_data.get("ai_review_sentiment", 0),
            "review_velocity_monthly": raw_data.get("review_velocity_monthly", 0),
        }
