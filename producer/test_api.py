"""Quick test of YouTube API connectivity."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from producer.youtube_client import fetch_page, search_videos

print("=== Testing YouTube Data API v3 ===\n")

# Test 1: Trending videos
print("[1] Fetching trending videos from IN...")
try:
    data = fetch_page("IN")
    items = data.get("items", [])
    print(f"    SUCCESS: Got {len(items)} trending videos")
    for item in items[:5]:
        s = item.get("snippet", {})
        st = item.get("statistics", {})
        vid = item.get("id", "?")
        title = s.get("title", "?")[:60]
        views = st.get("viewCount", "?")
        print(f"    {vid} | {title} | views={views}")
    next_page = data.get("nextPageToken")
    print(f"    nextPageToken: {next_page}")
except Exception as e:
    print(f"    ERROR: {e}")

# Test 2: Search
print("\n[2] Searching for 'trending music' in US...")
try:
    data = search_videos("trending music", region="US")
    items = data.get("items", [])
    print(f"    SUCCESS: Got {len(items)} search results")
    for item in items[:3]:
        s = item.get("snippet", {})
        vid = item.get("id", {}).get("videoId", "?")
        title = s.get("title", "?")[:60]
        print(f"    {vid} | {title}")
except Exception as e:
    print(f"    ERROR: {e}")

print("\n=== API test complete ===")
