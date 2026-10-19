"""Kafka consumer: validate, Bloom-dedupe, clean, sentiment, sketches, bulk index.

Offsets are committed only after a successful Elasticsearch bulk request.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from kafka import KafkaConsumer
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import (  # noqa: E402
    BLOOM_K_HASHES,
    BLOOM_M_BITS,
    BULK_SIZE,
    ES_INDEX,
    ES_URL,
    KAFKA_BOOTSTRAP,
    KAFKA_GROUP_ID,
    KAFKA_RAW_TOPIC,
    document_id,
)
from consumer.cleaning import to_video_doc  # noqa: E402
from consumer.dead_letter import build_dead_producer, send_dead  # noqa: E402
from consumer.sentiment import score_title  # noqa: E402
from schemas.video_event import VideoEvent  # noqa: E402
from streaming_algos.bloom_filter import BloomFilter  # noqa: E402
from streaming_algos.dgim import DGIM  # noqa: E402
from streaming_algos.flajolet_martin import FlajoletMartin  # noqa: E402


def bloom_key(event: VideoEvent) -> str:
    return f"{event.video_id}|{event.region}|{event.fetched_at[:10]}"


class StreamState:
    def __init__(self) -> None:
        self.bloom = BloomFilter(BLOOM_M_BITS, BLOOM_K_HASHES)
        self.fm = FlajoletMartin()
        self.dgim = DGIM(window_size=1000)
        self.processed = 0
        self.skipped_bloom = 0
        self.dead_lettered = 0
        self.last_report = time.time()

    def maybe_report(self) -> None:
        if time.time() - self.last_report < 60:
            return
        print(
            "counters "
            f"processed={self.processed} "
            f"bloom_skip={self.skipped_bloom} "
            f"dead={self.dead_lettered} "
            f"fm_channels={self.fm.estimate():.1f} "
            f"dgim_last1000={self.dgim.estimate():.1f} "
            f"bloom_theory_fpr={self.bloom.theoretical_fpr():.4f}"
        )
        self.last_report = time.time()


def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else "",
        consumer_timeout_ms=1000,
    )


def flush_bulk(es: Elasticsearch, actions: list[dict[str, Any]]) -> None:
    if not actions:
        return
    success, errors = bulk(es, actions, raise_on_error=False, refresh=False)
    if errors:
        raise RuntimeError(f"Elasticsearch bulk had errors: {errors[:3]}")
    if success != len(actions):
        raise RuntimeError(f"Bulk indexed {success}/{len(actions)}")


def process_forever() -> None:
    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        raise SystemExit(f"Cannot reach Elasticsearch at {ES_URL}")

    consumer = make_consumer()
    dead = build_dead_producer()
    state = StreamState()
    pending: list[dict[str, Any]] = []

    print(
        f"Consumer listening on {KAFKA_RAW_TOPIC} group={KAFKA_GROUP_ID} "
        f"bulk={BULK_SIZE}"
    )
    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            progressed = False
            for _tp, messages in records.items():
                for message in messages:
                    progressed = True
                    raw = message.value
                    try:
                        event = VideoEvent.model_validate(raw)
                    except ValidationError as exc:
                        send_dead(dead, raw, error=str(exc), key=message.key or "unknown")
                        state.dead_lettered += 1
                        continue

                    key = bloom_key(event)
                    if state.bloom.might_contain(key):
                        state.skipped_bloom += 1
                        continue
                    state.bloom.add(key)

                    sentiment, label = score_title(event.title)
                    doc = to_video_doc(event, sentiment, label)
                    state.fm.add(doc.channel_id)
                    state.dgim.add_bit(1)
                    state.processed += 1

                    pending.append(
                        {
                            "_op_type": "index",
                            "_index": ES_INDEX,
                            "_id": document_id(doc.video_id, doc.region, doc.fetched_at),
                            "_source": doc.to_es_source(),
                        }
                    )

                    if len(pending) >= BULK_SIZE:
                        flush_bulk(es, pending)
                        consumer.commit()
                        pending.clear()
                        progressed = False

            if pending:
                flush_bulk(es, pending)
                consumer.commit()
                pending.clear()
            elif progressed:
                # Bloom skips / dead letters still advance offsets.
                consumer.commit()

            state.maybe_report()
    except KeyboardInterrupt:
        print("Consumer stopped.")
        if pending:
            flush_bulk(es, pending)
            consumer.commit()
    finally:
        dead.flush()
        dead.close()
        consumer.close()
        print(
            "final counters "
            f"processed={state.processed} bloom_skip={state.skipped_bloom} "
            f"dead={state.dead_lettered}"
        )


if __name__ == "__main__":
    process_forever()
