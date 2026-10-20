"""Replay raw JSONL files (from bulk_fetch dry runs) into Kafka.

Usage:
    python producer/jsonl_replay.py                          # replay all files in data/raw/
    python producer/jsonl_replay.py data/raw/trending_*.jsonl  # specific files
    python producer/jsonl_replay.py --delay 0.05               # throttled replay
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from kafka import KafkaProducer  # noqa: E402

from config.settings import KAFKA_BOOTSTRAP, KAFKA_RAW_TOPIC  # noqa: E402
from schemas.video_event import VideoEvent  # noqa: E402


def replay_file(
    path: Path,
    producer: KafkaProducer,
    delay: float = 0.0,
) -> tuple[int, int]:
    """Replay a single JSONL file. Returns (sent, skipped)."""
    sent = 0
    skipped = 0
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
                # Strip extra fields that aren't in the schema (e.g. top_comments)
                event = VideoEvent.model_validate(raw)
                producer.send(
                    KAFKA_RAW_TOPIC,
                    key=event.video_id,
                    value=event.model_dump(),
                )
                sent += 1
                if delay > 0:
                    time.sleep(delay)
            except Exception as exc:  # noqa: BLE001
                skipped += 1
                if skipped <= 5:
                    print(f"  WARN line {line_no}: {exc}")
    return sent, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay JSONL files into Kafka")
    parser.add_argument(
        "files", nargs="*", default=[],
        help="JSONL file paths or glob patterns (default: data/raw/*.jsonl)",
    )
    parser.add_argument(
        "--delay", type=float, default=0.0,
        help="Delay in seconds between records (default: 0 = full speed)",
    )
    args = parser.parse_args()

    raw_dir = ROOT / "data" / "raw"
    if args.files:
        paths: list[Path] = []
        for pattern in args.files:
            paths.extend(Path(p) for p in glob.glob(pattern))
    else:
        paths = sorted(raw_dir.glob("*.jsonl"))

    if not paths:
        raise SystemExit("No JSONL files found. Run bulk_fetch.py first or specify files.")

    print(f"Replaying {len(paths)} file(s) -> {KAFKA_RAW_TOPIC} @ {KAFKA_BOOTSTRAP}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        batch_size=65536,
    )

    total_sent = 0
    total_skipped = 0
    for path in paths:
        print(f"  {path.name} ", end="", flush=True)
        sent, skipped = replay_file(path, producer, delay=args.delay)
        total_sent += sent
        total_skipped += skipped
        print(f"-> {sent} sent, {skipped} skipped")

    producer.flush()
    producer.close()
    print(f"\nDone. Total: {total_sent} sent, {total_skipped} skipped.")


if __name__ == "__main__":
    main()
