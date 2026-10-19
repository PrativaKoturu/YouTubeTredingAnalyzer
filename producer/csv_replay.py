"""Replay a Kaggle-style trending CSV into Kafka to simulate a stream."""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import (  # noqa: E402
    KAFKA_BOOTSTRAP,
    KAFKA_RAW_TOPIC,
    REPLAY_CSV,
    REPLAY_DELAY_SEC,
)
from schemas.video_event import VideoEvent  # noqa: E402


COLUMN_ALIASES = {
    "video_id": ["video_id", "videoId"],
    "title": ["title"],
    "description": ["description"],
    "tags": ["tags"],
    "channel_id": ["channelId", "channel_id"],
    "channel_title": ["channelTitle", "channel_title"],
    "category_id": ["categoryId", "category_id"],
    "published_at": ["publishedAt", "publish_time", "published_at"],
    "views": ["view_count", "views"],
    "likes": ["likes"],
    "comments": ["comment_count", "comments"],
    "duration": ["duration"],
    "region": ["region", "country"],
}


def pick(row: dict, names: list[str], default: str = "") -> str:
    for name in names:
        if name in row and pd.notna(row[name]) and str(row[name]) != "":
            return str(row[name])
    return default


def row_to_event(row: dict, fetched_at: str, default_region: str = "US") -> VideoEvent:
    payload = {
        "video_id": pick(row, COLUMN_ALIASES["video_id"], "unknown"),
        "title": pick(row, COLUMN_ALIASES["title"], "untitled"),
        "description": pick(row, COLUMN_ALIASES["description"])[:4000],
        "tags": pick(row, COLUMN_ALIASES["tags"]),
        "channel_id": pick(row, COLUMN_ALIASES["channel_id"], "unknown"),
        "channel_title": pick(row, COLUMN_ALIASES["channel_title"], "unknown"),
        "category_id": pick(row, COLUMN_ALIASES["category_id"], "0"),
        "region": pick(row, COLUMN_ALIASES["region"], default_region)[:2].upper(),
        "published_at": pick(row, COLUMN_ALIASES["published_at"], fetched_at),
        "fetched_at": fetched_at,
        "duration": pick(row, COLUMN_ALIASES["duration"], "PT0S"),
        "views": pick(row, COLUMN_ALIASES["views"], "0"),
        "likes": pick(row, COLUMN_ALIASES["likes"], "0"),
        "comments": pick(row, COLUMN_ALIASES["comments"], "0"),
    }
    return VideoEvent.model_validate(payload)


def main() -> None:
    path = Path(REPLAY_CSV)
    if not path.exists():
        raise SystemExit(f"CSV not found: {path}")

    print(f"Replaying {path} -> {KAFKA_RAW_TOPIC} (delay={REPLAY_DELAY_SEC}s)")
    df = pd.read_csv(path)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    sent = 0
    for record in df.to_dict(orient="records"):
        try:
            event = row_to_event(record, fetched_at)
        except Exception as exc:  # noqa: BLE001
            print(f"Skipping bad CSV row: {exc}")
            continue
        producer.send(KAFKA_RAW_TOPIC, key=event.video_id, value=event.model_dump())
        sent += 1
        if REPLAY_DELAY_SEC > 0:
            time.sleep(REPLAY_DELAY_SEC)
    producer.flush()
    producer.close()
    print(f"Replay finished. Published {sent} records.")


if __name__ == "__main__":
    main()
