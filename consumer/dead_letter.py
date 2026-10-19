"""Publish records that fail validation to the dead-letter topic."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer

from config.settings import KAFKA_BOOTSTRAP, KAFKA_DEAD_TOPIC


def build_dead_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def send_dead(
    producer: KafkaProducer,
    raw: Any,
    error: str,
    key: str = "unknown",
) -> None:
    payload = {
        "error": error,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "raw": raw if isinstance(raw, (dict, list, str, int, float, type(None))) else str(raw),
    }
    producer.send(KAFKA_DEAD_TOPIC, key=str(key), value=payload)
