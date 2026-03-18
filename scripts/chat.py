#!/usr/bin/env python3
"""
chat.py – Conversational plain-text interface to the Game Grumps transcript database.

Just run it and ask questions in plain English:

    python scripts/chat.py

Example conversation:
    You: Are there any mentions of banana or bananas?
    GG:  Found 31 results across 24 episodes …

    You: What was the context of #3?
    GG:  Here's what was happening around that moment …

    You: What episode was #1?
    GG:  #1 is from "Sonic Colors Ultimate" …

    You: Find every time they mention egoraptor and bloodborne within 30 seconds
    GG:  Co-occurrence search …
"""

import re
import sqlite3
import sys
import textwrap
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_DB = Path(__file__).resolve().parent.parent / "db" / "transcripts.sqlite"
MAX_RESULTS = 10          # default results per search
CONTEXT_WINDOW_S = 60.0  # seconds of context to show around a result
CONTEXT_PAD_S = 20.0     # seconds before/after to fetch for context view
SNIPPET_WRAP = 70         # text wrap width


# ══════════════════════════════════════════════════════════════════════════════
# Database helpers
# ══════════════════════════════════════════════════════════════════════════════

def open_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        print(
            f"\n  ERROR: Database not found at {db_path}\n"
            "  Run  python scripts/build_db.py  first.\n"
        )
        sys.exit(1)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _fts(phrase: str) -> str:
    """Wrap phrase in FTS5 double-quotes for exact matching."""
    return '"' + phrase.replace('"', '""') + '"'


def search_db(conn, terms: list[str], series_filter: str | None, limit: int) -> list:
    """Search for one or more terms, return combined deduplicated results."""
    seen_ids = set()
    rows = []
    for term in terms:
        q = _fts(term)
        if series_filter:
            cur = conn.execute(
                """
                SELECT e.series, e.youtube_id, e.relative_path,
                       s.id AS snippet_id, s.start, s.duration, s.text, rank
                FROM snippets_fts
                JOIN snippets s ON snippets_fts.rowid = s.id
                JOIN episodes e ON s.episode_id = e.id
                WHERE snippets_fts MATCH ? AND e.series = ?
                ORDER BY rank LIMIT ?
                """,
                (q, series_filter, limit * 2),
            )
        else:
            cur = conn.execute(
                """
                SELECT e.series, e.youtube_id, e.relative_path,
                       s.id AS snippet_id, s.start, s.duration, s.text, rank
                FROM snippets_fts
                JOIN snippets s ON snippets_fts.rowid = s.id
                JOIN episodes e ON s.episode_id = e.id
                WHERE snippets_fts MATCH ?
                ORDER BY rank LIMIT ?
                """,
                (q, limit * 2),
            )
        for row in cur.fetchall():
            if row["snippet_id"] not in seen_ids:
                seen_ids.add(row["snippet_id"])
                rows.append(row)
    rows.sort(key=lambda r: r["rank"])
    return rows[:limit]


def count_db(conn, terms: list[str], series_filter: str | None) -> dict[str, int]:
    counts = {}
    for term in terms:
        q = _fts(term)
        if series_filter:
            n = conn.execute(
                """SELECT COUNT(*) FROM snippets_fts
                   JOIN snippets s ON snippets_fts.rowid = s.id
                   JOIN episodes e ON s.episode_id = e.id
                   WHERE snippets_fts MATCH ? AND e.series = ?""",
                (q, series_filter),
            ).fetchone()[0]
        else:
            n = conn.execute(
                "SELECT COUNT(*) FROM snippets_fts WHERE snippets_fts MATCH ?",
                (q,),
            ).fetchone()[0]
        counts[term] = n
    return counts


def get_context(conn, snippet_id: int) -> tuple[dict, list]:
    """Return (episode_row, list_of_nearby_snippet_rows) for a snippet."""
    anchor = conn.execute(
        "SELECT * FROM snippets WHERE id = ?", (snippet_id,)
    ).fetchone()
    if not anchor:
        return None, []
    ep = conn.execute(
        "SELECT * FROM episodes WHERE id = ?", (anchor["episode_id"],)
    ).fetchone()
    nearby = conn.execute(
        """SELECT start, duration, text FROM snippets
           WHERE episode_id = ?
             AND start >= ? AND start <= ?
           ORDER BY start""",
        (anchor["episode_id"], anchor["start"] - CONTEXT_PAD_S, anchor["start"] + CONTEXT_PAD_S),
    ).fetchall()
    return ep, nearby


def episode_by_youtube_id(conn, youtube_id: str):
    return conn.execute(
        "SELECT * FROM episodes WHERE youtube_id = ?", (youtube_id,)
    ).fetchone()


def db_stats(conn) -> dict:
    return {
        "episodes": conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0],
        "snippets": conn.execute("SELECT COUNT(*) FROM snippets").fetchone()[0],
        "series":   conn.execute("SELECT COUNT(DISTINCT series) FROM episodes").fetchone()[0],
        "top_series": conn.execute(
            "SELECT series, COUNT(*) n FROM episodes GROUP BY series ORDER BY n DESC LIMIT 5"
        ).fetchall(),
    }


def cooccur_db(conn, term_a: str, term_b: str, window: float, limit: int) -> list:
    fa, fb = _fts(term_a), _fts(term_b)
    return conn.execute(
        """
        WITH a AS (
            SELECT s.episode_id, s.id sa_id, s.start sa, s.text ta
            FROM snippets_fts JOIN snippets s ON snippets_fts.rowid = s.id
            WHERE snippets_fts MATCH ?
        ),
        b AS (
            SELECT s.episode_id, s.id sb_id, s.start sb, s.text tb
            FROM snippets_fts JOIN snippets s ON snippets_fts.rowid = s.id
            WHERE snippets_fts MATCH ?
        )
        SELECT e.series, e.youtube_id, e.relative_path,
               a.sa, a.ta, b.sb, b.tb,
               ABS(a.sa - b.sb) gap
        FROM a
        JOIN b ON a.episode_id = b.episode_id AND a.sa_id != b.sb_id
                  AND ABS(a.sa - b.sb) <= ?
        JOIN episodes e ON e.id = a.episode_id
        ORDER BY gap LIMIT ?
        """,
        (fa, fb, window, limit),
    ).fetchall()


# ══════════════════════════════════════════════════════════════════════════════
# Formatting helpers
# ══════════════════════════════════════════════════════════════════════════════

def fmt_time(secs: float) -> str:
    s = int(secs)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def yt_url(youtube_id: str, start: float = 0) -> str:
    return f"https://youtu.be/{youtube_id}?t={int(start)}"


def wrap(text: str, indent: int = 4) -> str:
    pad = " " * indent
    return textwrap.fill(text, SNIPPET_WRAP, initial_indent=pad, subsequent_indent=pad)


# ══════════════════════════════════════════════════════════════════════════════
# Intent detection
# ══════════════════════════════════════════════════════════════════════════════

# Patterns ordered from most-specific to least-specific
_INTENT_PATTERNS = [
    # stats / help
    ("STATS",    re.compile(r"\b(stats|statistics|how many episodes|how many series|database info)\b", re.I)),
    ("HELP",     re.compile(r"^\s*(help|what can you do|commands|\?)\s*$", re.I)),
    # episode info for a numbered result
    ("EP_INFO",  re.compile(r"\b(what (episode|video|series) (is|was)|episode info|tell me about)\s+#?(\d+)\b", re.I)),
    # context for a numbered result
    ("CONTEXT",  re.compile(r"\b(context|surrounding|around|more from|what (were they|was happening)|expand)\b.*#?(\d+)\b", re.I)),
    # OR variant: "context of #3"
    ("CONTEXT2", re.compile(r"#(\d+).*\b(context|surrounding|more)\b", re.I)),
    # co-occurrence
    ("COOCCUR",  re.compile(r"\b(cooccur|co[.-]occur|co occur|both|together|within\s+\d+|mention.+and|episodes.+both)\b", re.I)),
    # quit
    ("QUIT",     re.compile(r"^\s*(quit|exit|bye|goodbye|q)\s*$", re.I)),
]


def detect_intent(text: str) -> str:
    for name, pat in _INTENT_PATTERNS:
        if pat.search(text):
            return name
    return "SEARCH"


def extract_series_filter(text: str) -> tuple[str, str | None]:
    """
    Look for 'in [series name]' / 'from [series name]' / 'series: ...' patterns.
    Returns (cleaned_text, series_or_None).
    """
    m = re.search(r'\b(?:in|from)\s+["\']([^"\']+)["\']', text, re.I)
    if m:
        return text[:m.start()].strip() + " " + text[m.end():].strip(), m.group(1)
    m = re.search(r'\bseries[:\s]+([A-Za-z0-9 &_\-]+?)(?:\s*$|\s*(?:and|,|\.))', text, re.I)
    if m:
        return text[:m.start()].strip(), m.group(1).strip()
    return text, None


def extract_search_terms(text: str) -> list[str]:
    """
    Pull quoted phrases and 'or'-joined terms from a plain-English query.
    e.g. "banana or bananas" → ["banana", "bananas"]
         '"kiss your dad"' → ["kiss your dad"]
    """
    # First pull anything in quotes
    quoted = re.findall(r'"([^"]+)"', text)
    if quoted:
        return quoted

    # Remove filler words so we don't search on them
    stopwords = {"are", "there", "any", "mention", "mentions", "of", "the",
                 "in", "find", "search", "for", "does", "do", "they", "say",
                 "ever", "episodes", "about", "a", "an", "all", "me", "show",
                 "transcripts", "when", "every", "time", "times", "word",
                 "words", "phrase", "look", "up", "i", "want", "please",
                 "instances", "occurrences", "how", "many", "where", "talk",
                 "together", "within", "seconds", "second", "discuss",
                 "discussed", "had", "let", "us", "using", "use", "what",
                 "which"}

    # Split on 'or' to get alternatives
    parts = re.split(r"\bor\b", text, flags=re.I)
    terms = []
    for part in parts:
        # Remove leading/trailing noise words and punctuation
        clean = re.sub(r"[\"',!?]", "", part).strip()
        words = clean.split()
        meaningful = [w for w in words if w.lower() not in stopwords]
        if meaningful:
            # If 2+ meaningful words, keep them as a phrase;
            # if single word, keep it alone
            if len(meaningful) <= 4:
                terms.append(" ".join(meaningful))
    return [t for t in terms if t] or [text.strip()]


def _clean_cooccur_term(term: str) -> str:
    """Strip filler/noise words from a co-occurrence term."""
    terms = extract_search_terms(term)
    return terms[0] if terms else term.strip()


def extract_cooccur_terms(text: str) -> tuple[str, str, float]:
    """
    Extract two terms and an optional window (seconds) from a co-occurrence query.
    Returns (term_a, term_b, window_seconds).
    """
    window = 30.0
    m = re.search(r"within\s+(\d+)\s*s(?:ec(?:ond)?s?)?", text, re.I)
    if m:
        window = float(m.group(1))
        text = text[: m.start()] + text[m.end():]

    # Pull quoted terms first
    quoted = re.findall(r'"([^"]+)"', text)
    if len(quoted) >= 2:
        return quoted[0], quoted[1], window

    # Split on 'and', then clean each half of filler words
    parts = re.split(r"\band\b", text, maxsplit=1, flags=re.I)
    if len(parts) == 2:
        a = _clean_cooccur_term(parts[0])
        b = _clean_cooccur_term(parts[1])
        if a and b:
            return a, b, window

    return _clean_cooccur_term(text), "", window


def extract_result_number(text: str) -> int | None:
    m = re.search(r"#(\d+)", text)
    if m:
        return int(m.group(1))
    m = re.search(r"\bnumber\s+(\d+)\b", text, re.I)
    if m:
        return int(m.group(1))
    # Try bare digit at end: "context of 3"
    m = re.search(r"\b(\d{1,3})\b", text)
    if m:
        return int(m.group(1))
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Response generators
# ══════════════════════════════════════════════════════════════════════════════

def respond_search(conn, terms: list[str], series_filter: str | None,
                   limit: int, session: dict) -> None:
    counts = count_db(conn, terms, series_filter)
    total = sum(counts.values())
    rows = search_db(conn, terms, series_filter, limit)

    if not rows:
        if series_filter:
            print(f'\n  No mentions of {_quote_terms(terms)} found in series "{series_filter}".\n')
        else:
            print(f"\n  No mentions of {_quote_terms(terms)} found anywhere in the transcripts.\n")
        return

    # Save numbered results for follow-up questions
    session["last_results"] = rows
    session["last_terms"] = terms

    # Header
    term_str = _quote_terms(terms)
    if series_filter:
        print(f'\n  Found {total:,} snippet(s) matching {term_str} in series "{series_filter}".')
    else:
        print(f"\n  Found {total:,} snippet(s) matching {term_str} across the transcripts.")
    if len(terms) > 1:
        for t, n in counts.items():
            print(f"    · \"{t}\" — {n:,} mention(s)")

    showing = min(limit, len(rows))
    print(f"  Showing the top {showing} result(s):\n")

    for i, row in enumerate(rows, 1):
        url = yt_url(row["youtube_id"], row["start"])
        print(f"  #{i}  Series  : {row['series']}")
        print(f"       Video   : {row['youtube_id']}  at {fmt_time(row['start'])}")
        print(f"       Link    : {url}")
        print(wrap(f'"{row["text"]}"', 7))
        print()

    print(f"  Ask \"What was the context of #3?\" or \"What episode was #1?\" for more detail.\n")


def respond_context(conn, result_n: int, session: dict) -> None:
    results = session.get("last_results", [])
    if not results or result_n < 1 or result_n > len(results):
        _no_result(result_n, len(results))
        return

    row = results[result_n - 1]
    ep, nearby = get_context(conn, row["snippet_id"])

    if not ep:
        print(f"\n  Couldn't load context for #{result_n}.\n")
        return

    anchor_start = row["start"]
    url = yt_url(ep["youtube_id"], anchor_start)

    print(f"\n  Context for #{result_n} — {ep['series']}")
    print(f"  Video: {ep['youtube_id']}  around {fmt_time(anchor_start)}")
    print(f"  Link : {url}")
    print(f"  {'─' * 60}")
    if not nearby:
        print(f"    (No surrounding snippets found in the ±{int(CONTEXT_PAD_S)}s window)")
    for s in nearby:
        marker = "  ► " if abs(s["start"] - anchor_start) < 1.0 else "    "
        print(f"{marker}[{fmt_time(s['start'])}]  {s['text']}")
    print(f"  {'─' * 60}")
    print(f"  The ► marker shows the matched line.\n")


def respond_episode_info(conn, result_n: int, session: dict) -> None:
    results = session.get("last_results", [])
    if not results or result_n < 1 or result_n > len(results):
        _no_result(result_n, len(results))
        return

    row = results[result_n - 1]
    ep = episode_by_youtube_id(conn, row["youtube_id"])
    if not ep:
        print(f"\n  Couldn't find episode info for #{result_n}.\n")
        return

    snippet_count = conn.execute(
        "SELECT COUNT(*) FROM snippets WHERE episode_id = ?", (ep["id"],)
    ).fetchone()[0]

    url = f"https://youtu.be/{ep['youtube_id']}"

    print(f"\n  Episode #{result_n}")
    print(f"  {'─' * 60}")
    print(f"  Series       : {ep['series']}")
    print(f"  YouTube ID   : {ep['youtube_id']}")
    print(f"  Watch        : {url}")
    print(f"  File         : {ep['relative_path']}")
    print(f"  Language     : {ep['language'] or 'unknown'}")
    print(f"  Transcribed  : {'Auto-generated' if ep['is_generated'] else 'Manual'}")
    print(f"  Snippets     : {snippet_count:,} caption segments")
    print(f"  {'─' * 60}\n")


def respond_cooccur(conn, text: str, session: dict, limit: int) -> None:
    term_a, term_b, window = extract_cooccur_terms(text)
    if not term_b:
        print("\n  I need two terms to search for co-occurrence.")
        print('  Try: find episodes where they mention "arin" and "danny" within 30 seconds\n')
        return

    rows = cooccur_db(conn, term_a, term_b, window, limit)
    session["last_results"] = [
        {
            "series": r["series"],
            "youtube_id": r["youtube_id"],
            "relative_path": r["relative_path"],
            "snippet_id": None,
            "start": min(r["sa"], r["sb"]),
            "duration": 0,
            "text": f'{r["ta"]}  /  {r["tb"]}',
        }
        for r in rows
    ]

    if not rows:
        print(f'\n  No moments found where "{term_a}" and "{term_b}" appear within {window:.0f} seconds of each other.\n')
        return

    print(f'\n  Found {len(rows)} moment(s) where "{term_a}" and "{term_b}" appear within {window:.0f}s of each other:\n')
    for i, r in enumerate(rows, 1):
        url = yt_url(r["youtube_id"], min(r["sa"], r["sb"]))
        print(f"  #{i}  Series : {r['series']}")
        print(f"       Video  : {r['youtube_id']}  (gap: {r['gap']:.1f}s)")
        print(f"       Link   : {url}")
        print(f"       [{fmt_time(r['sa'])}] {r['ta']}")
        print(f"       [{fmt_time(r['sb'])}] {r['tb']}")
        print()


def respond_stats(conn) -> None:
    s = db_stats(conn)
    print(f"\n  Database Overview")
    print(f"  {'─' * 60}")
    print(f"  Episodes    : {s['episodes']:,}")
    print(f"  Series      : {s['series']:,}")
    print(f"  Snippets    : {s['snippets']:,}  (caption segments with timestamps)")
    print(f"\n  Top 5 series by episode count:")
    for row in s["top_series"]:
        print(f"    · {row['series']:<45}  {row['n']} episodes")
    print(f"  {'─' * 60}\n")


def respond_help() -> None:
    print("""
  ┌─────────────────────────────────────────────────────────────┐
  │  Game Grumps Transcript Chat — What you can ask             │
  ├─────────────────────────────────────────────────────────────┤
  │  Search                                                     │
  │    Are there any mentions of banana or bananas?             │
  │    Find every time they say "kiss your dad"                 │
  │    When do they talk about Bloodborne?                      │
  │    Search for "cool cool cool" in series "Sonic Colors"     │
  │                                                             │
  │  Follow-up on numbered results                              │
  │    What episode was #2?                                     │
  │    What was the context of #3?                              │
  │    Show more from #1                                        │
  │                                                             │
  │  Co-occurrence                                              │
  │    Find episodes where they mention arin and egoraptor      │
  │    "bloodborne" and "dark souls" within 30 seconds          │
  │                                                             │
  │  Stats                                                      │
  │    Stats / how many episodes are in the database?           │
  │                                                             │
  │  Type  quit  or  exit  to leave.                            │
  └─────────────────────────────────────────────────────────────┘
""")


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

def _quote_terms(terms: list[str]) -> str:
    if len(terms) == 1:
        return f'"{terms[0]}"'
    return "  or  ".join(f'"{t}"' for t in terms)


def _no_result(n: int, total: int) -> None:
    if total == 0:
        print("\n  No results saved yet — run a search first.\n")
    else:
        print(f"\n  Result #{n} doesn't exist. Last search returned {total} result(s) (#1–#{total}).\n")


# ══════════════════════════════════════════════════════════════════════════════
# Main loop
# ══════════════════════════════════════════════════════════════════════════════

def process(conn, text: str, session: dict, limit: int) -> bool:
    """Process one user input. Returns False when the user wants to quit."""
    text = text.strip()
    if not text:
        return True

    intent = detect_intent(text)

    if intent == "QUIT":
        print("\n  Goodbye!\n")
        return False

    if intent == "HELP":
        respond_help()
        return True

    if intent == "STATS":
        respond_stats(conn)
        return True

    if intent in ("CONTEXT", "CONTEXT2"):
        n = extract_result_number(text)
        if n is None:
            print("\n  Which result? Try:  context of #2\n")
        else:
            respond_context(conn, n, session)
        return True

    if intent == "EP_INFO":
        n = extract_result_number(text)
        if n is None:
            print("\n  Which result? Try:  what episode was #1\n")
        else:
            respond_episode_info(conn, n, session)
        return True

    if intent == "COOCCUR":
        respond_cooccur(conn, text, session, limit)
        return True

    # Default: SEARCH
    cleaned, series_filter = extract_series_filter(text)
    terms = extract_search_terms(cleaned)
    respond_search(conn, terms, series_filter, limit, session)
    return True


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Conversational plain-text interface to the Game Grumps transcript database."
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB),
        help=f"Path to the SQLite database (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=MAX_RESULTS,
        help=f"Max results per search (default: {MAX_RESULTS})",
    )
    args = parser.parse_args()

    conn = open_db(Path(args.db))
    session: dict = {}  # persists numbered results between turns

    print()
    print("  ╔═══════════════════════════════════════════════════════════╗")
    print("  ║       Game Grumps Transcript Database — Chat              ║")
    print("  ╚═══════════════════════════════════════════════════════════╝")
    print()
    print("  Ask anything about the transcripts in plain English.")
    print('  Type  help  for examples, or  quit  to exit.')
    print()

    while True:
        try:
            raw = input("  You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!\n")
            break

        if not raw:
            continue

        keep_going = process(conn, raw, session, args.limit)
        if not keep_going:
            break

    conn.close()


if __name__ == "__main__":
    main()
