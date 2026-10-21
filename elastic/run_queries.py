"""Run saved Elasticsearch queries and print a visually formatted summary."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from elasticsearch import Elasticsearch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import ES_INDEX, ES_URL  # noqa: E402
from streaming_algos.flajolet_martin import FlajoletMartin  # noqa: E402

QUERY_DIR = Path(__file__).resolve().parent / "queries"

# ── ANSI colour helpers ────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
CYAN   = "\033[96m"
WHITE  = "\033[97m"
GRAY   = "\033[90m"

BG_BLUE  = "\033[44m"
BG_DARK  = "\033[48;5;236m"


def c(text: str, *codes: str) -> str:
    """Wrap text in ANSI codes."""
    return "".join(codes) + str(text) + RESET


def load(name: str) -> dict:
    with (QUERY_DIR / name).open(encoding="utf-8") as handle:
        return json.load(handle)


# ── Layout helpers ─────────────────────────────────────────────────────────────
WIDTH = 72


def banner(title: str, emoji: str = "▶") -> None:
    """Print a full-width section header."""
    inner = f" {emoji}  {title.upper()} "
    pad   = WIDTH - len(inner) - 2
    left  = pad // 2
    right = pad - left
    print()
    print(c("─" * WIDTH, GRAY))
    print(c("│" + " " * left + inner + " " * right + "│", BOLD + CYAN))
    print(c("─" * WIDTH, GRAY))


def kv(label: str, value: str, label_w: int = 24) -> None:
    label_str = c(f"{label:<{label_w}}", DIM + WHITE)
    value_str = c(value, BOLD + YELLOW)
    print(f"  {label_str} {value_str}")


def hbar(label: str, value: int | float, max_val: float,
         bar_width: int = 28, unit: str = "") -> None:
    """Print a single horizontal bar-chart row."""
    if max_val == 0:
        ratio = 0.0
    else:
        ratio = min(value / max_val, 1.0)
    filled  = int(ratio * bar_width)
    empty   = bar_width - filled
    bar     = c("█" * filled, GREEN) + c("░" * empty, GRAY)
    lbl     = f"{label:<22}"[:22]
    val_str = c(f"{value:>12,.0f}{unit}", YELLOW)
    print(f"  {c(lbl, WHITE)}  {bar}  {val_str}")


def rule(char: str = "─") -> None:
    print(c(char * WIDTH, GRAY))


def table_row(cells: list[str], widths: list[int], colors: list[str]) -> None:
    parts = []
    for cell, w, col in zip(cells, widths, colors):
        parts.append(c(str(cell)[:w].ljust(w), col))
    print("  " + c("│", GRAY) + (" " + c("│", GRAY) + " ").join(parts))


def table_divider(widths: list[int]) -> None:
    segs = ["─" * (w + 2) for w in widths]
    print("  " + c("┼".join(segs), GRAY))


# ── Individual query renderers ─────────────────────────────────────────────────

def show_category_country(body: dict) -> None:
    banner("Category × Region breakdown", "🌍")
    regions = body.get("aggregations", {}).get("by_region", {}).get("buckets", [])
    if not regions:
        print(c("  No data yet.", DIM))
        return
    for reg in regions:
        region_name = c(f"  ◆ Region: {reg['key']}", BOLD + CYAN)
        print(f"\n{region_name}  ({c(str(reg['doc_count']), YELLOW)} docs)")
        cats = reg.get("by_category", {}).get("buckets", [])
        if not cats:
            continue
        max_views = max((b["total_views"]["value"] for b in cats), default=1) or 1
        for cat in cats[:8]:
            views = cat["total_views"]["value"]
            sent  = cat["avg_sentiment"]["value"] or 0.0
            sent_str = f"  sentiment={sent:+.2f}"
            hbar(cat["key"], views, max_views, unit=" views")
            print(c(f"{'':>26}{sent_str}", DIM))


def show_hourly_views(body: dict) -> None:
    banner("Views over time  (hourly)", "📈")
    buckets = (body.get("aggregations", {})
                   .get("views_over_time", {})
                   .get("buckets", []))
    # keep only buckets with data
    buckets = [b for b in buckets if b.get("video_count", {}).get("value", 0) > 0]
    if not buckets:
        print(c("  No data yet.", DIM))
        return
    max_views = max((b["total_views"]["value"] for b in buckets), default=1) or 1
    for b in buckets[-12:]:        # show last 12 hours max
        ts  = b["key_as_string"][:16] if "key_as_string" in b else str(b["key"])
        hbar(ts, b["total_views"]["value"], max_views)


def show_cardinality(body: dict) -> None:
    banner("Index cardinality", "📊")
    total    = body.get("hits", {}).get("total", {}).get("value", "?")
    aggs     = body.get("aggregations", {})
    channels = aggs.get("distinct_channels_hll", {}).get("value", "?")
    kv("Total documents",         str(total))
    kv("Distinct channels (HLL)", str(channels))


def show_significant_tags(body: dict) -> None:
    banner("Significant / unusual tags", "🏷️")
    buckets = (body.get("aggregations", {})
                   .get("unusual_tags", {})
                   .get("buckets", []))
    if not buckets:
        print(c("  No significant tags found.", DIM))
        return
    max_score = max((b.get("score", 1) for b in buckets), default=1) or 1
    for b in buckets[:15]:
        tag   = b["key"]
        count = b["doc_count"]
        score = b.get("score", 0)
        hbar(f"#{tag}", score, max_score, unit="")
        print(c(f"{'':>26}  count={count}", DIM))


def show_filtered_search(body: dict) -> None:
    banner("BM25 filtered search  (title=cricket, region=IN)", "🔍")
    hits = body.get("hits", {}).get("hits", [])
    total = body.get("hits", {}).get("total", {}).get("value", 0)
    print(c(f"  Matched {total} documents  •  showing top {len(hits)}\n", DIM))
    if not hits:
        print(c("  No results.", DIM))
        return
    cols   = ["Score", "Title", "Views", "Sentiment"]
    widths = [6, 32, 12, 10]
    colors = [YELLOW, WHITE, CYAN, GREEN]
    table_row(cols, widths, [BOLD + GRAY] * 4)
    table_divider(widths)
    for h in hits:
        src = h.get("_source", {})
        score = f"{h.get('_score') or 0:.2f}"
        title = (src.get("title") or "")[:32]
        views = f"{src.get('views', 0):,}"
        sent  = src.get("sentiment_label", "")
        sent_c = GREEN if sent == "positive" else (RED if sent == "negative" else YELLOW)
        table_row(
            [score, title, views, sent],
            widths,
            [YELLOW, WHITE, CYAN, sent_c],
        )


def show_more_like_this(body: dict, source_id: str) -> None:
    banner(f"More-like-this  (source: {source_id[:20]}…)", "🔗")
    hits = body.get("hits", {}).get("hits", [])
    if not hits:
        print(c("  No similar documents found.", DIM))
        return
    cols   = ["Score", "Title", "Channel", "Views"]
    widths = [6, 28, 18, 10]
    colors = [BOLD + GRAY] * 4
    table_row(cols, widths, colors)
    table_divider(widths)
    for h in hits:
        src   = h.get("_source", {})
        score = f"{h.get('_score') or 0:.2f}"
        title   = (src.get("title") or "")[:28]
        channel = (src.get("channel_title") or "")[:18]
        views   = f"{src.get('views', 0):,}"
        table_row(
            [score, title, channel, views],
            widths,
            [YELLOW, WHITE, CYAN, GREEN],
        )


def compare_fm(es: Elasticsearch) -> None:
    banner("Flajolet-Martin  vs  Elasticsearch HyperLogLog++", "🧮")
    resp = es.search(
        index=ES_INDEX,
        size=10000,
        source=["channel_id"],
        query={"match_all": {}},
    )
    hits     = resp.get("hits", {}).get("hits", [])
    channels = [h["_source"]["channel_id"] for h in hits if h.get("_source")]
    fm       = FlajoletMartin()
    for ch in channels:
        fm.add(ch)
    exact = len(set(channels))
    card  = es.search(index=ES_INDEX, body=load("agg_cardinality_channels.json"))
    hll   = card["aggregations"]["distinct_channels_hll"]["value"]
    est   = fm.estimate()

    kv("Documents scanned",  str(len(channels)))
    kv("Exact distinct",     str(exact))
    print()

    # visual comparison bars
    max_val = max(exact, est, hll, 1)
    hbar("Exact",          exact, max_val)
    hbar("FM estimate",    est,   max_val)
    hbar("ES HLL++",       hll,   max_val)
    print()

    if exact:
        fm_err  = abs(est - exact) / exact * 100
        hll_err = abs(hll - exact) / exact * 100
        fm_col  = GREEN if fm_err  < 10 else YELLOW if fm_err  < 20 else RED
        hll_col = GREEN if hll_err < 10 else YELLOW if hll_err < 20 else RED
        kv("FM error %",  c(f"{fm_err:.2f}%",  fm_col))
        kv("HLL error %", c(f"{hll_err:.2f}%", hll_col))


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    # ── startup splash ──
    print()
    print(c("╔" + "═" * (WIDTH - 2) + "╗", CYAN))
    title_inner = "  📺  YouTube Trending — Elasticsearch Query Report  "
    pad = WIDTH - 2 - len(title_inner)
    print(c("║" + title_inner + " " * pad + "║", BOLD + CYAN))
    ts_inner = f"  {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}  •  index: {ES_INDEX}  "
    pad2 = WIDTH - 2 - len(ts_inner)
    print(c("║" + ts_inner + " " * pad2 + "║", DIM + CYAN))
    print(c("╚" + "═" * (WIDTH - 2) + "╝", CYAN))

    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        print(c(f"\n  ✗  Cannot reach Elasticsearch at {ES_URL}", RED + BOLD))
        raise SystemExit(1)
    if not es.indices.exists(index=ES_INDEX):
        print(c(f"\n  ✗  Index '{ES_INDEX}' not found.  Run: make index", RED + BOLD))
        raise SystemExit(1)

    print(c(f"\n  ✔  Connected to {ES_URL}", GREEN + BOLD))

    show_category_country(
        es.search(index=ES_INDEX, body=load("agg_category_country.json")).body
    )
    show_hourly_views(
        es.search(index=ES_INDEX, body=load("agg_hourly_views.json")).body
    )
    show_cardinality(
        es.search(index=ES_INDEX, body=load("agg_cardinality_channels.json")).body
    )
    show_significant_tags(
        es.search(index=ES_INDEX, body=load("agg_significant_tags.json")).body
    )
    show_filtered_search(
        es.search(index=ES_INDEX, body=load("filtered_search.json")).body
    )

    sample = es.search(index=ES_INDEX, size=1, query={"match_all": {}})
    sample_hits = sample.get("hits", {}).get("hits", [])
    if sample_hits:
        mlt = load("more_like_this.json")
        doc_id = sample_hits[0]["_id"]
        mlt["query"]["more_like_this"]["like"][0]["_id"]    = doc_id
        mlt["query"]["more_like_this"]["like"][0]["_index"] = ES_INDEX
        show_more_like_this(
            es.search(index=ES_INDEX, body=mlt).body,
            source_id=doc_id,
        )
    else:
        banner("More-like-this", "🔗")
        print(c("  Index is empty — run make replay && make consumer first.", DIM))

    compare_fm(es)

    # ── footer ──
    print()
    print(c("─" * WIDTH, GRAY))
    print(c("  Done.", BOLD + GREEN))
    print(c("─" * WIDTH, GRAY))
    print()


if __name__ == "__main__":
    main()
