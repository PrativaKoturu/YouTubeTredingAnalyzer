"""Thin wrapper around YouTube Data API v3 videos.list (mostPopular)."""

from __future__ import annotations

import time
from typing import Any, Iterator

import requests

from config.settings import YOUTUBE_API_KEY, YOUTUBE_MAX_RESULTS

API_URL = "https://www.googleapis.com/youtube/v3/videos"


class YouTubeApiError(RuntimeError):
    pass


def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(2 ** attempt, 30))


def fetch_page(
    region: str,
    page_token: str | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    if not YOUTUBE_API_KEY:
        raise YouTubeApiError("YOUTUBE_API_KEY is missing. Use CSV replay or set it in .env")

    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": region,
        "maxResults": min(YOUTUBE_MAX_RESULTS, 50),
        "key": YOUTUBE_API_KEY,
    }
    if page_token:
        params["pageToken"] = page_token

    http = session or requests.Session()
    last_error = None
    for attempt in range(6):
        try:
            response = http.get(API_URL, params=params, timeout=20)
        except requests.RequestException as exc:
            last_error = exc
            _sleep_backoff(attempt)
            continue

        if response.status_code == 200:
            return response.json()

        body = {}
        try:
            body = response.json()
        except ValueError:
            body = {"raw": response.text[:300]}

        reason = ""
        errors = body.get("error", {}).get("errors") or []
        if errors:
            reason = errors[0].get("reason", "")

        if response.status_code == 403 and reason == "quotaExceeded":
            raise YouTubeApiError("YouTube quota exceeded. Switch to CSV replay.")

        if response.status_code in {429, 500, 502, 503, 504} or reason in {
            "rateLimitExceeded",
            "userRateLimitExceeded",
            "backendError",
        }:
            last_error = YouTubeApiError(f"HTTP {response.status_code} {reason or body}")
            _sleep_backoff(attempt)
            continue

        raise YouTubeApiError(f"YouTube API error {response.status_code}: {body}")

    raise YouTubeApiError(f"YouTube API failed after retries: {last_error}")


def iter_most_popular(region: str, max_pages: int = 2) -> Iterator[dict[str, Any]]:
    """Yield raw video resources for one region."""
    token = None
    session = requests.Session()
    for _ in range(max_pages):
        payload = fetch_page(region, token, session=session)
        for item in payload.get("items", []):
            yield item
        token = payload.get("nextPageToken")
        if not token:
            break


def resource_to_event(item: dict[str, Any], region: str, fetched_at: str) -> dict[str, Any]:
    snippet = item.get("snippet") or {}
    stats = item.get("statistics") or {}
    details = item.get("contentDetails") or {}
    return {
        "video_id": item.get("id", ""),
        "title": snippet.get("title") or "untitled",
        "description": (snippet.get("description") or "")[:4000],
        "tags": snippet.get("tags") or [],
        "channel_id": snippet.get("channelId") or "unknown",
        "channel_title": snippet.get("channelTitle") or "unknown",
        "category_id": str(snippet.get("categoryId") or "0"),
        "region": region,
        "published_at": snippet.get("publishedAt") or fetched_at,
        "fetched_at": fetched_at,
        "duration": details.get("duration") or "PT0S",
        "views": int(stats.get("viewCount") or 0),
        "likes": int(stats.get("likeCount") or 0),
        "comments": int(stats.get("commentCount") or 0),
    }
