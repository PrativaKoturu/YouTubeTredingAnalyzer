"""Normalise tags, parse ISO-8601 duration, map categories, fill nulls."""

from __future__ import annotations

import json
import re
from functools import lru_cache

from isodate import parse_duration
from isodate.isoerror import ISO8601Error

from config.settings import CATEGORIES_FILE
from schemas.video_doc import VideoDoc
from schemas.video_event import VideoEvent

ISO_FALLBACK = re.compile(
    r"PT(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+)S)?",
    re.IGNORECASE,
)


@lru_cache(maxsize=1)
def category_map() -> dict[str, str]:
    with CATEGORIES_FILE.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    return {str(k): str(v) for k, v in raw.items()}


def map_category(category_id: str) -> str:
    return category_map().get(str(category_id), "Unknown")


def parse_duration_sec(value: str) -> int:
    if not value:
        return 0
    try:
        delta = parse_duration(value)
        return int(delta.total_seconds())
    except (ISO8601Error, TypeError, OverflowError, AttributeError):
        match = ISO_FALLBACK.fullmatch(value.strip())
        if not match:
            return 0
        hours = int(match.group("h") or 0)
        minutes = int(match.group("m") or 0)
        seconds = int(match.group("s") or 0)
        return hours * 3600 + minutes * 60 + seconds


def normalise_tags(tags: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for tag in tags:
        cleaned = tag.strip().lower()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def to_video_doc(event: VideoEvent, sentiment: float, sentiment_label: str) -> VideoDoc:
    return VideoDoc(
        video_id=event.video_id,
        title=(event.title or "untitled").strip(),
        description=(event.description or "")[:2000],
        tags=normalise_tags(event.tags),
        channel_id=event.channel_id or "unknown",
        channel_title=event.channel_title or "unknown",
        category_id=str(event.category_id),
        category_name=map_category(event.category_id),
        region=event.region.upper(),
        published_at=event.published_at,
        fetched_at=event.fetched_at,
        duration_sec=parse_duration_sec(event.duration),
        views=max(int(event.views), 0),
        likes=max(int(event.likes), 0),
        comments=max(int(event.comments), 0),
        sentiment=float(sentiment),
        sentiment_label=sentiment_label,
        pagerank=0.0,
        betweenness=0.0,
        community_id=-1,
    )
