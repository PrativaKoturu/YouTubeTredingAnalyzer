"""Poll YouTube trending videos and publish JSON to Kafka."""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import (  # noqa: E402
    KAFKA_BOOTSTRAP,
    KAFKA_RAW_TOPIC,
    POLL_INTERVAL_SEC,
    REGIONS,
)
from producer.youtube_client import (  # noqa: E402
    QuotaExhaustedError,
    YouTubeApiError,
    iter_most_popular,
    resource_to_event,
)
from schemas.video_event import VideoEvent  # noqa: E402

MAX_PAGES_PER_REGION = 4  # 4 pages × 50 results = up to 200 per region


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        batch_size=65536,
    )


def poll_once(producer: KafkaProducer, regions: list[str]) -> int:
    fetched_at = datetime.now(timezone.utc).isoformat()
    published = 0
    for region in regions:
        print(f"Fetching mostPopular region={region} (up to {MAX_PAGES_PER_REGION} pages)")
        try:
            for item in iter_most_popular(region, max_pages=MAX_PAGES_PER_REGION):
                payload = resource_to_event(item, region, fetched_at)
                event = VideoEvent.model_validate(payload)
                body = event.model_dump()
                producer.send(KAFKA_RAW_TOPIC, key=event.video_id, value=body)
                published += 1
        except QuotaExhaustedError:
            print(f"QUOTA EXHAUSTED during region {region}. Will retry next cycle.")
            break
        except YouTubeApiError as exc:
            print(f"Skipping region {region}: {exc}")
    producer.flush()
    return published


def main() -> None:
    print(f"Producer starting. bootstrap={KAFKA_BOOTSTRAP} topic={KAFKA_RAW_TOPIC}")
    print(f"Regions: {', '.join(REGIONS)} | Poll interval: {POLL_INTERVAL_SEC}s")
    producer = build_producer()
    try:
        while True:
            n = poll_once(producer, REGIONS)
            print(f"Published {n} records across {len(REGIONS)} regions. Sleeping {POLL_INTERVAL_SEC}s")
            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
