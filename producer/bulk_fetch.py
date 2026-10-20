"""Bulk data fetcher: pull large volumes of YouTube data via the API.

This script orchestrates aggressive data collection across:
  1. Trending/mostPopular for 10 regions (paginated)
  2. Search queries across multiple categories for diversity
  3. Comment threads for enrichment

All fetched data is published to Kafka for downstream processing.
The script respects quota limits and saves raw JSON to data/raw/ as a backup.

Usage:
    python producer/bulk_fetch.py              # full bulk run
    python producer/bulk_fetch.py --trending   # trending only
    python producer/bulk_fetch.py --search     # search only
    python producer/bulk_fetch.py --dry-run    # fetch + save JSON, no Kafka
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from kafka import KafkaProducer  # noqa: E402

from config.settings import (  # noqa: E402
    KAFKA_BOOTSTRAP,
    KAFKA_RAW_TOPIC,
    REGIONS,
)
from producer.youtube_client import (  # noqa: E402
    CATEGORY_QUERIES,
    QuotaExhaustedError,
    YouTubeApiError,
    extract_comments_text,
    fetch_categories,
    fetch_comment_threads,
    iter_most_popular,
    iter_search_videos,
    resource_to_event,
)
from schemas.video_event import VideoEvent  # noqa: E402


RAW_DIR = ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


class BulkStats:
    """Track progress during a bulk fetch run."""

    def __init__(self) -> None:
        self.trending_fetched = 0
        self.search_fetched = 0
        self.comments_fetched = 0
        self.published_to_kafka = 0
        self.duplicates_seen: set[str] = set()
        self.errors = 0
        self.start_time = time.time()

    def summary(self) -> str:
        elapsed = time.time() - self.start_time
        return (
            f"\n{'=' * 60}\n"
            f"  BULK FETCH SUMMARY\n"
            f"{'=' * 60}\n"
            f"  Trending videos fetched : {self.trending_fetched}\n"
            f"  Search videos fetched   : {self.search_fetched}\n"
            f"  Total unique videos     : {len(self.duplicates_seen)}\n"
            f"  Comments fetched        : {self.comments_fetched}\n"
            f"  Published to Kafka      : {self.published_to_kafka}\n"
            f"  Errors / skipped        : {self.errors}\n"
            f"  Elapsed time            : {elapsed:.1f}s\n"
            f"{'=' * 60}"
        )


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,       # batch small sends for throughput
        batch_size=65536,    # 64 KB batch
        buffer_memory=67108864,  # 64 MB buffer
    )


def save_raw_json(events: list[dict[str, Any]], label: str) -> Path:
    """Persist raw events as newline-delimited JSON for backup / replay."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = RAW_DIR / f"{label}_{ts}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    print(f"  Saved {len(events)} events to {path}")
    return path


def publish_events(
    producer: KafkaProducer | None,
    events: list[dict[str, Any]],
    stats: BulkStats,
) -> None:
    """Validate and publish events to Kafka."""
    if producer is None:
        return
    for payload in events:
        try:
            event = VideoEvent.model_validate(payload)
            body = event.model_dump()
            producer.send(KAFKA_RAW_TOPIC, key=event.video_id, value=body)
            stats.published_to_kafka += 1
        except Exception as exc:  # noqa: BLE001
            stats.errors += 1
            print(f"  WARN: skipping event: {exc}")
    producer.flush()


# ---------------------------------------------------------------------------
# Trending fetch
# ---------------------------------------------------------------------------

def fetch_all_trending(
    regions: list[str],
    stats: BulkStats,
    max_pages: int = 4,
) -> list[dict[str, Any]]:
    """Fetch trending/mostPopular across all regions."""
    all_events: list[dict[str, Any]] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for region in regions:
        print(f"\n[TRENDING] Fetching region={region} (up to {max_pages} pages)")
        try:
            count = 0
            for item in iter_most_popular(region, max_pages=max_pages):
                video_id = item.get("id", "")
                dedup_key = f"{video_id}_{region}"
                if dedup_key in stats.duplicates_seen:
                    continue
                stats.duplicates_seen.add(dedup_key)

                payload = resource_to_event(item, region, fetched_at)
                all_events.append(payload)
                count += 1
                stats.trending_fetched += 1

            print(f"  -> {count} videos from {region}")

        except QuotaExhaustedError:
            print("  QUOTA EXHAUSTED. Stopping trending fetch.")
            break
        except YouTubeApiError as exc:
            print(f"  ERROR region={region}: {exc}")
            stats.errors += 1

    return all_events


# ---------------------------------------------------------------------------
# Search-based fetch
# ---------------------------------------------------------------------------

def fetch_search_bulk(
    regions: list[str],
    stats: BulkStats,
    categories: dict[str, list[str]] | None = None,
    max_pages_per_query: int = 2,
) -> list[dict[str, Any]]:
    """Fetch videos via search across categories and regions."""
    if categories is None:
        categories = CATEGORY_QUERIES

    all_events: list[dict[str, Any]] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for cat_name, queries in categories.items():
        print(f"\n[SEARCH] Category: {cat_name}")
        for query in queries:
            for region in regions:
                print(f"  Searching q={query!r} region={region}")
                try:
                    count = 0
                    for item in iter_search_videos(
                        query, region=region, max_pages=max_pages_per_query,
                    ):
                        video_id = item.get("id", "")
                        dedup_key = f"{video_id}_{region}"
                        if dedup_key in stats.duplicates_seen:
                            continue
                        stats.duplicates_seen.add(dedup_key)

                        payload = resource_to_event(item, region, fetched_at)
                        all_events.append(payload)
                        count += 1
                        stats.search_fetched += 1

                    print(f"    -> {count} new videos")

                except QuotaExhaustedError:
                    print("  QUOTA EXHAUSTED. Stopping search fetch.")
                    return all_events
                except YouTubeApiError as exc:
                    print(f"    ERROR: {exc}")
                    stats.errors += 1

    return all_events


# ---------------------------------------------------------------------------
# Comment enrichment
# ---------------------------------------------------------------------------

def enrich_with_comments(
    events: list[dict[str, Any]],
    stats: BulkStats,
    max_comments_per_video: int = 20,
    max_videos: int = 200,
) -> None:
    """Fetch top comments for a subset of videos and attach to events.

    This adds a 'top_comments' field to each event dict (not part of the
    core VideoEvent schema, so it gets stored in the raw JSON backup but
    stripped during Pydantic validation for Kafka).
    """
    print(f"\n[COMMENTS] Enriching up to {max_videos} videos with comments")
    import requests as _requests
    session = _requests.Session()

    for i, event in enumerate(events[:max_videos]):
        video_id = event.get("video_id", "")
        try:
            threads = fetch_comment_threads(
                video_id, max_results=max_comments_per_video, session=session,
            )
            texts = extract_comments_text(threads)
            event["top_comments"] = texts
            stats.comments_fetched += len(texts)
            if (i + 1) % 50 == 0:
                print(f"  ... {i + 1}/{min(len(events), max_videos)} videos enriched")
        except QuotaExhaustedError:
            print("  QUOTA EXHAUSTED. Stopping comment fetch.")
            break
        except Exception:  # noqa: BLE001
            event["top_comments"] = []

    print(f"  -> Fetched {stats.comments_fetched} comments total")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk YouTube data fetcher")
    parser.add_argument("--trending", action="store_true", help="Fetch trending only")
    parser.add_argument("--search", action="store_true", help="Fetch search results only")
    parser.add_argument("--comments", action="store_true", help="Also fetch comments")
    parser.add_argument("--dry-run", action="store_true", help="Save JSON only, no Kafka")
    parser.add_argument(
        "--regions", type=str, default="",
        help="Comma-separated region codes (overrides .env REGIONS)",
    )
    parser.add_argument(
        "--trending-pages", type=int, default=4,
        help="Max pages per region for trending (default 4 = 200 videos)",
    )
    parser.add_argument(
        "--search-pages", type=int, default=2,
        help="Max pages per search query (default 2 = 100 videos)",
    )
    parser.add_argument(
        "--max-comment-videos", type=int, default=200,
        help="Max videos to fetch comments for (default 200)",
    )
    args = parser.parse_args()

    regions = [r.strip().upper() for r in args.regions.split(",") if r.strip()] if args.regions else REGIONS
    do_trending = not args.search  # default: do trending unless --search only
    do_search = not args.trending  # default: do search unless --trending only
    # If both flags are set, do both
    if args.trending and args.search:
        do_trending = True
        do_search = True

    stats = BulkStats()
    all_events: list[dict[str, Any]] = []

    print(f"{'=' * 60}")
    print(f"  YOUTUBE BULK DATA FETCHER")
    print(f"  Regions      : {', '.join(regions)}")
    print(f"  Trending     : {'YES' if do_trending else 'NO'}")
    print(f"  Search       : {'YES' if do_search else 'NO'}")
    print(f"  Comments     : {'YES' if args.comments else 'NO'}")
    print(f"  Kafka        : {'DRY RUN' if args.dry_run else KAFKA_BOOTSTRAP}")
    print(f"{'=' * 60}")

    # Fetch trending
    if do_trending:
        trending_events = fetch_all_trending(regions, stats, max_pages=args.trending_pages)
        all_events.extend(trending_events)
        if trending_events:
            save_raw_json(trending_events, "trending")

    # Fetch search
    if do_search:
        search_events = fetch_search_bulk(
            regions, stats, max_pages_per_query=args.search_pages,
        )
        all_events.extend(search_events)
        if search_events:
            save_raw_json(search_events, "search")

    # Fetch comments
    if args.comments and all_events:
        enrich_with_comments(events=all_events, stats=stats, max_videos=args.max_comment_videos)
        # Save enriched version
        save_raw_json(all_events, "enriched")

    # Publish to Kafka
    if not args.dry_run and all_events:
        print(f"\n[KAFKA] Publishing {len(all_events)} events to {KAFKA_RAW_TOPIC}")
        producer = build_producer()
        try:
            publish_events(producer, all_events, stats)
        finally:
            producer.close()
    elif args.dry_run:
        print("\n[DRY RUN] Skipping Kafka publish. Raw data saved to data/raw/")

    print(stats.summary())


if __name__ == "__main__":
    main()
