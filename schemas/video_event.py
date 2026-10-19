"""Pydantic model for a raw trending record on Kafka."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class VideoEvent(BaseModel):
    """One trending video snapshot as published by the producer."""

    video_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    channel_id: str = Field(min_length=1)
    channel_title: str = Field(min_length=1)
    category_id: str = Field(min_length=1)
    region: str = Field(min_length=2, max_length=2)
    published_at: str
    fetched_at: str
    duration: str = "PT0S"
    views: int = 0
    likes: int = 0
    comments: int = 0

    @field_validator("region")
    @classmethod
    def upper_region(cls, value: str) -> str:
        return value.upper()

    @field_validator("tags", mode="before")
    @classmethod
    def split_tags(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(t).strip() for t in value if str(t).strip()]
        text = str(value).strip()
        if not text or text == "[none]":
            return []
        if "|" in text:
            return [part.strip() for part in text.split("|") if part.strip()]
        return [part.strip() for part in text.split(",") if part.strip()]

    @field_validator("views", "likes", "comments", mode="before")
    @classmethod
    def coerce_int(cls, value: Any) -> int:
        if value is None or value == "":
            return 0
        return int(float(value))
