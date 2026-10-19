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
from producer.youtube_client import YouTubeApiError, iter_most_popular, resource_to_event  # noqa: E402
from schemas.video_event import VideoEvent  # noqa: E402


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )


def poll_once(producer: KafkaProducer, regions: list[str]) -> int:
    fetched_at = datetime.now(timezone.utc).isoformat()
    published = 0
    for region in regions:
        print(f"Fetching mostPopular region={region}")
        try:
            for item in iter_most_popular(region):
                payload = resource_to_event(item, region, fetched_at)
                event = VideoEvent.model_validate(payload)
                body = event.model_dump()
                producer.send(KAFKA_RAW_TOPIC, key=event.video_id, value=body)
                published += 1
        except YouTubeApiError as exc:
            print(f"Skipping region {region}: {exc}")
    producer.flush()
    return published


def main() -> None:
    print(f"Producer starting. bootstrap={KAFKA_BOOTSTRAP} topic={KAFKA_RAW_TOPIC}")
    producer = build_producer()
    try:
        while True:
            n = poll_once(producer, REGIONS)
            print(f"Published {n} records. Sleeping {POLL_INTERVAL_SEC}s")
            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
