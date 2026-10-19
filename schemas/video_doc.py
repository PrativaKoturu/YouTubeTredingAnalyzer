"""Pydantic model for a cleaned Elasticsearch document."""

from __future__ import annotations

from pydantic import BaseModel, Field


class VideoDoc(BaseModel):
    """Document stored in the yt-trending index."""

    video_id: str
    title: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    channel_id: str
    channel_title: str
    category_id: str
    category_name: str
    region: str
    published_at: str
    fetched_at: str
    duration_sec: int = 0
    views: int = 0
    likes: int = 0
    comments: int = 0
    sentiment: float = 0.0
    sentiment_label: str = "neu"
    pagerank: float = 0.0
    betweenness: float = 0.0
    community_id: int = -1

    def to_es_source(self) -> dict:
        """JSON body for Elasticsearch (no extra fields: mapping is strict)."""
        return self.model_dump()
