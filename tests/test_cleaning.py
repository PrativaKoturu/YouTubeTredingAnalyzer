from consumer.cleaning import parse_duration_sec, normalise_tags, map_category
from schemas.video_event import VideoEvent
from consumer.cleaning import to_video_doc
from consumer.sentiment import score_title


def test_parse_duration() -> None:
    assert parse_duration_sec("PT1H2M3S") == 3723
    assert parse_duration_sec("PT15M") == 900
    assert parse_duration_sec("bad") == 0


def test_normalise_tags() -> None:
    assert normalise_tags([" Cricket ", "INDIA", "cricket"]) == ["cricket", "india"]


def test_map_category() -> None:
    assert map_category("17") == "Sports"
    assert map_category("999") == "Unknown"


def test_event_to_doc() -> None:
    event = VideoEvent(
        video_id="abc",
        title="Great cricket win!",
        description="highlights",
        tags="cricket|india",
        channel_id="ch1",
        channel_title="Sports Daily",
        category_id="17",
        region="in",
        published_at="2024-01-01T00:00:00Z",
        fetched_at="2024-01-02T10:00:00Z",
        duration="PT10M",
        views=100,
        likes=10,
        comments=2,
    )
    sent, label = score_title(event.title)
    doc = to_video_doc(event, sentiment=sent, sentiment_label=label)
    assert doc.duration_sec == 600
    assert doc.category_name == "Sports"
    assert doc.region == "IN"
    assert doc.sentiment_label in {"pos", "neu", "neg"}
