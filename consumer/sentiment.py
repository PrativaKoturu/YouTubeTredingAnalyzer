"""VADER sentiment on video titles."""

from __future__ import annotations

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_ANALYZER = SentimentIntensityAnalyzer()


def label_from_compound(compound: float) -> str:
    if compound >= 0.05:
        return "pos"
    if compound <= -0.05:
        return "neg"
    return "neu"


def score_title(title: str) -> tuple[float, str]:
    compound = float(_ANALYZER.polarity_scores(title or "")["compound"])
    return compound, label_from_compound(compound)
