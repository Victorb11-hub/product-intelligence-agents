"""
YouTube Agent — Free official YouTube Data API v3

Searches YouTube for product-related videos.
Extracts view counts, comment text, engagement ratios, creator tiers.
Writes to signals_social.
"""
import os
import logging
from datetime import date, datetime, timedelta

from .base_agent import BaseAgent

logger = logging.getLogger(__name__)


class YouTubeAgent(BaseAgent):
    PLATFORM = "youtube"
    SIGNAL_TABLE = "signals_social"
    REQUIRED_CREDENTIALS = ["YOUTUBE_API_KEY"]

    def scrape(self, product_name: str, keywords: list, product: dict) -> dict:
        from googleapiclient.discovery import build

        api_key = os.environ["YOUTUBE_API_KEY"]
        youtube = build("youtube", "v3", developerKey=api_key)

        search_terms = [product_name] + (keywords[:2] if keywords else [])
        query = " ".join(search_terms[:3])

        # Search for recent videos (last 7 days)
        published_after = (datetime.utcnow() - timedelta(days=7)).isoformat() + "Z"

        self.rate_limiter.wait()
        search_response = youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=25,
            order="relevance",
            publishedAfter=published_after,
            relevanceLanguage="en",
        ).execute()

        video_items = search_response.get("items", [])
        if not video_items:
            raise ValueError(f"No YouTube videos found for {product_name}")

        video_ids = [item["id"]["videoId"] for item in video_items]

        # Get video statistics
        self.rate_limiter.wait()
        stats_response = youtube.videos().list(
            part="statistics,snippet",
            id=",".join(video_ids),
        ).execute()

        all_texts = []
        all_dates = []
        total_views = 0
        total_likes = 0
        total_comments_count = 0
        creator_scores = []

        for video in stats_response.get("items", []):
            snippet = video.get("snippet", {})
            stats = video.get("statistics", {})

            # Collect text
            title = snippet.get("title", "")
            desc = snippet.get("description", "")[:500]
            all_texts.append(f"{title} {desc}")
            all_dates.append(snippet.get("publishedAt", "")[:10])

            # Stats
            total_views += int(stats.get("viewCount", 0))
            total_likes += int(stats.get("likeCount", 0))
            total_comments_count += int(stats.get("commentCount", 0))

            # Creator tier from subscriber count (requires channel lookup)
            channel_id = snippet.get("channelId", "")
            creator_scores.append(0.5)  # Default, refine below

        # Fetch top comments from first 5 videos for sentiment/intent
        for vid_id in video_ids[:5]:
            self.rate_limiter.wait()
            try:
                comments_response = youtube.commentThreads().list(
                    part="snippet",
                    videoId=vid_id,
                    maxResults=15,
                    order="relevance",
                    textFormat="plainText",
                ).execute()

                for thread in comments_response.get("items", []):
                    comment = thread["snippet"]["topLevelComment"]["snippet"]
                    all_texts.append(comment.get("textDisplay", ""))
                    all_dates.append(comment.get("publishedAt", "")[:10])
            except Exception:
                pass  # Comments may be disabled

        # Try to get channel subscriber counts for creator tier
        channel_ids = list(set(
            v.get("snippet", {}).get("channelId", "")
            for v in stats_response.get("items", [])
            if v.get("snippet", {}).get("channelId")
        ))
        if channel_ids:
            self.rate_limiter.wait()
            try:
                channels_response = youtube.channels().list(
                    part="statistics",
                    id=",".join(channel_ids[:10]),
                ).execute()
                sub_counts = {}
                for ch in channels_response.get("items", []):
                    subs = int(ch.get("statistics", {}).get("subscriberCount", 0))
                    sub_counts[ch["id"]] = subs

                creator_scores = []
                for video in stats_response.get("items", []):
                    ch_id = video.get("snippet", {}).get("channelId", "")
                    subs = sub_counts.get(ch_id, 0)
                    if subs > 1_000_000:
                        creator_scores.append(0.95)
                    elif subs > 100_000:
                        creator_scores.append(0.8)
                    elif subs > 10_000:
                        creator_scores.append(0.6)
                    else:
                        creator_scores.append(0.3)
            except Exception:
                pass

        # Growth rate
        hist = self.supabase.table("signals_social") \
            .select("mention_count").eq("product_id", product["id"]) \
            .eq("platform", "youtube").order("scraped_date", desc=True).limit(1).execute()
        prev = hist.data[0]["mention_count"] if hist.data else len(video_items)
        growth = (len(video_items) - prev) / max(prev, 1)

        return {
            "texts": all_texts, "data_dates": all_dates,
            "mention_count": len(video_items),
            "growth_rate_wow": round(growth, 4),
            "total_views": total_views,
            "total_likes": total_likes,
            "total_comments": total_comments_count,
            "creator_tier_score": round(sum(creator_scores) / max(len(creator_scores), 1), 4),
            "buy_intent_comment_count": 0,
            "problem_language_comment_count": 0,
            "repeat_purchase_pct": 0,
        }

    def build_signal_row(self, raw_data: dict, product_id: str) -> dict:
        return {
            "product_id": product_id, "scraped_date": date.today().isoformat(),
            "platform": "youtube",
            "mention_count": raw_data.get("mention_count", 0),
            "growth_rate_wow": raw_data.get("growth_rate_wow", 0),
            "velocity_score": 0,
            "creator_tier_score": raw_data.get("creator_tier_score", 0),
            "buy_intent_comment_count": 0, "problem_language_comment_count": 0,
            "raw_json": {
                "total_views": raw_data.get("total_views", 0),
                "total_likes": raw_data.get("total_likes", 0),
                "total_comments": raw_data.get("total_comments", 0),
            },
        }
