"""Thin wrapper around YouTube Data API v3 videos.list (mostPopular) and search.

Supports:
  - Trending/mostPopular by region (paginated)
  - Search by category keyword (paginated)
  - Comment thread fetching per video
  - Robust retry with exponential backoff
  - Quota-aware error handling
"""

from __future__ import annotations

import time
from typing import Any, Iterator

import requests

from config.settings import YOUTUBE_API_KEY, YOUTUBE_MAX_RESULTS

API_BASE = "https://www.googleapis.com/youtube/v3"
VIDEOS_URL = f"{API_BASE}/videos"
SEARCH_URL = f"{API_BASE}/search"
COMMENTS_URL = f"{API_BASE}/commentThreads"
CATEGORIES_URL = f"{API_BASE}/videoCategories"

# Search queries per category for richer data diversity
CATEGORY_QUERIES: dict[str, list[str]] = {
    "Music": ["trending music", "new music video", "top songs"],
    "Gaming": ["gaming highlights", "new game trailer", "esports finals"],
    "Sports": ["sports highlights", "match highlights", "tournament finals"],
    "News": ["breaking news today", "world news", "news update"],
    "Entertainment": ["viral video", "trending clip", "entertainment news"],
    "Science & Technology": ["tech review", "new technology", "science explained"],
    "Education": ["tutorial", "online course", "how to learn"],
    "Comedy": ["funny video", "comedy special", "stand up"],
    "Film & Animation": ["movie trailer", "animation", "film review"],
    "People & Blogs": ["vlog", "daily vlog", "lifestyle"],
}


class YouTubeApiError(RuntimeError):
    pass


class QuotaExhaustedError(YouTubeApiError):
    """Raised specifically when the daily quota is used up."""
    pass


def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(2 ** attempt, 30))


def _api_request(
    url: str,
    params: dict[str, Any],
    session: requests.Session | None = None,
    max_retries: int = 6,
) -> dict[str, Any]:
    """Make a YouTube API request with retry logic and quota handling."""
    if not YOUTUBE_API_KEY:
        raise YouTubeApiError("YOUTUBE_API_KEY is missing. Use CSV replay or set it in .env")

    params["key"] = YOUTUBE_API_KEY
    http = session or requests.Session()
    last_error = None

    for attempt in range(max_retries):
        try:
            response = http.get(url, params=params, timeout=20)
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
            raise QuotaExhaustedError("YouTube quota exceeded. Switch to CSV replay or wait.")

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


# ---------------------------------------------------------------------------
# Trending / mostPopular
# ---------------------------------------------------------------------------

def fetch_page(
    region: str,
    page_token: str | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": region,
        "maxResults": min(YOUTUBE_MAX_RESULTS, 50),
    }
    if page_token:
        params["pageToken"] = page_token
    return _api_request(VIDEOS_URL, params, session=session)


def iter_most_popular(region: str, max_pages: int = 4) -> Iterator[dict[str, Any]]:
    """Yield raw video resources for one region, paginated up to max_pages."""
    token = None
    session = requests.Session()
    for page_num in range(max_pages):
        payload = fetch_page(region, token, session=session)
        items = payload.get("items", [])
        for item in items:
            yield item
        token = payload.get("nextPageToken")
        if not token:
            break
        print(f"  [trending] region={region} page {page_num + 2}/{max_pages}")


# ---------------------------------------------------------------------------
# Search-based fetching (for volume and diversity)
# ---------------------------------------------------------------------------

def search_videos(
    query: str,
    region: str = "",
    max_results: int = 50,
    order: str = "viewCount",
    page_token: str | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Search for videos and return the API response."""
    params: dict[str, Any] = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": min(max_results, 50),
        "order": order,
        "relevanceLanguage": "en",
    }
    if region:
        params["regionCode"] = region
    if page_token:
        params["pageToken"] = page_token
    return _api_request(SEARCH_URL, params, session=session)


def fetch_video_details(
    video_ids: list[str],
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch full snippet+statistics+contentDetails for a batch of video IDs.
    YouTube allows up to 50 IDs per call.
    """
    if not video_ids:
        return []
    results: list[dict[str, Any]] = []
    # batch in groups of 50
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(batch),
        }
        data = _api_request(VIDEOS_URL, params, session=session)
        results.extend(data.get("items", []))
    return results


def iter_search_videos(
    query: str,
    region: str = "",
    max_pages: int = 3,
    order: str = "viewCount",
) -> Iterator[dict[str, Any]]:
    """Search for videos and yield full video resources (with statistics).

    The search endpoint only returns snippet data, so we do a second call
    to videos.list to get statistics and contentDetails.
    """
    token = None
    session = requests.Session()
    for page_num in range(max_pages):
        search_resp = search_videos(
            query, region=region, page_token=token, order=order, session=session,
        )

        # Extract video IDs from search results
        video_ids = [
            item["id"]["videoId"]
            for item in search_resp.get("items", [])
            if item.get("id", {}).get("videoId")
        ]

        if video_ids:
            # Fetch full details for these videos
            full_items = fetch_video_details(video_ids, session=session)
            for item in full_items:
                yield item

        token = search_resp.get("nextPageToken")
        if not token:
            break
        print(f"  [search] q={query!r} region={region} page {page_num + 2}/{max_pages}")


# ---------------------------------------------------------------------------
# Comment threads
# ---------------------------------------------------------------------------

def fetch_comment_threads(
    video_id: str,
    max_results: int = 20,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch top-level comment threads for a video."""
    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": min(max_results, 100),
        "order": "relevance",
        "textFormat": "plainText",
    }
    try:
        data = _api_request(COMMENTS_URL, params, session=session)
        return data.get("items", [])
    except YouTubeApiError:
        # Comments may be disabled; don't crash the pipeline
        return []


def extract_comments_text(threads: list[dict[str, Any]]) -> list[str]:
    """Extract plain text from comment threads."""
    texts = []
    for thread in threads:
        snippet = thread.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
        text = snippet.get("textDisplay", "").strip()
        if text:
            texts.append(text)
    return texts


# ---------------------------------------------------------------------------
# Category list
# ---------------------------------------------------------------------------

def fetch_categories(region: str = "US", session: requests.Session | None = None) -> dict[str, str]:
    """Fetch video category ID -> title mapping for a region."""
    params = {
        "part": "snippet",
        "regionCode": region,
    }
    data = _api_request(CATEGORIES_URL, params, session=session)
    return {
        item["id"]: item["snippet"]["title"]
        for item in data.get("items", [])
    }


# ---------------------------------------------------------------------------
# Event conversion
# ---------------------------------------------------------------------------

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
