"""Central configuration loaded from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip() if value is not None else default


YOUTUBE_API_KEY = _env("YOUTUBE_API_KEY")
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_RAW_TOPIC = _env("KAFKA_RAW_TOPIC", "yt.trending.raw")
KAFKA_DEAD_TOPIC = _env("KAFKA_DEAD_TOPIC", "yt.trending.dead")
KAFKA_GROUP_ID = _env("KAFKA_GROUP_ID", "yt-trending-consumer")
ES_URL = _env("ES_URL", "http://localhost:9200")
ES_INDEX = _env("ES_INDEX", "yt-trending")
REGIONS = [r.strip().upper() for r in _env("REGIONS", "IN,US,GB").split(",") if r.strip()]
POLL_INTERVAL_SEC = int(_env("POLL_INTERVAL_SEC", "300"))
YOUTUBE_MAX_RESULTS = int(_env("YOUTUBE_MAX_RESULTS", "50"))
REPLAY_CSV = ROOT / _env("REPLAY_CSV", "data/sample/sample_trending.csv")
REPLAY_DELAY_SEC = float(_env("REPLAY_DELAY_SEC", "0.2"))
BULK_SIZE = int(_env("BULK_SIZE", "200"))
BLOOM_M_BITS = int(_env("BLOOM_M_BITS", "100000"))
BLOOM_K_HASHES = int(_env("BLOOM_K_HASHES", "5"))
REGIONS_FILE = ROOT / "config" / "regions.yaml"
CATEGORIES_FILE = ROOT / "config" / "categories.json"


def document_id(video_id: str, region: str, fetched_at: str) -> str:
    """Idempotent Elasticsearch _id: video + region + calendar day."""
    day = fetched_at[:10]
    return f"{video_id}_{region}_{day}"


def create_topics() -> None:
    """Create the raw and dead-letter topics if they are missing."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="yt-admin")
    topics = [
        NewTopic(name=KAFKA_RAW_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=KAFKA_DEAD_TOPIC, num_partitions=1, replication_factor=1),
    ]
    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        print(f"Created topics: {KAFKA_RAW_TOPIC} (3 partitions), {KAFKA_DEAD_TOPIC} (1 partition)")
    except TopicAlreadyExistsError:
        print("Topics already exist.")
    except Exception as exc:  # noqa: BLE001 - admin client raises several types
        # create_topics can raise a mixed error if one of two topics exists
        print(f"Topic create finished with: {exc}")
    finally:
        admin.close()


if __name__ == "__main__":
    print("KAFKA_BOOTSTRAP=", KAFKA_BOOTSTRAP)
    print("ES_URL=", ES_URL)
    print("REGIONS=", REGIONS)
    create_topics()
