#!/usr/bin/env python3
"""
search.py – Search the Game Grumps transcript database.

Usage examples:
    # Phrase search (returns top matches with series, youtube_id, timestamp, snippet)
    python scripts/search.py "kiss your dad"

    # Filter by series
    python scripts/search.py "bloodborne" --series "Bloodborne"

    # Show more results
    python scripts/search.py "egoraptor" --limit 20

    # JSON output (machine-readable)
    python scripts/search.py "cool cool cool" --json

    # Co-occurrence: find snippets containing BOTH terms within N seconds
    python scripts/search.py --cooccur "arin" "danny" --window 30

    # Episode info by YouTube ID
    python scripts/search.py --episode fmJNXG_f2SY
"""

import argparse
import json as json_mod
import os
import sqlite3
import sys
import textwrap
from pathlib import Path

DEFAULT_DB = Path(__file__).resolve().parent.parent / "db" / "transcripts.sqlite"
DEFAULT_LIMIT = 10
SNIPPET_CONTEXT = 40  # characters either side of match for context display


def open_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        print(
            f"ERROR: database not found at {db_path}\n"
            "Run  python scripts/build_db.py  to generate it first.",
            file=sys.stderr,
        )
        sys.exit(1)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ── Search ────────────────────────────────────────────────────────────────────

def search(conn: sqlite3.Connection, phrase: str, series: str | None, limit: int):
    """Full-text search for *phrase*, optionally filtered by *series*."""
    # Escape special FTS5 characters in the query string
    fts_query = _fts_escape(phrase)

    if series:
        rows = conn.execute(
            """
            SELECT
                e.series,
                e.youtube_id,
                e.relative_path,
                s.start,
                s.duration,
                s.text,
                rank
            FROM snippets_fts
            JOIN snippets  s ON snippets_fts.rowid = s.id
            JOIN episodes  e ON s.episode_id = e.id
            WHERE snippets_fts MATCH ?
              AND e.series = ?
            ORDER BY rank
            LIMIT ?
            """,
            (fts_query, series, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                e.series,
                e.youtube_id,
                e.relative_path,
                s.start,
                s.duration,
                s.text,
                rank
            FROM snippets_fts
            JOIN snippets  s ON snippets_fts.rowid = s.id
            JOIN episodes  e ON s.episode_id = e.id
            WHERE snippets_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (fts_query, limit),
        ).fetchall()
    return rows


def _fts_escape(phrase: str) -> str:
    """Wrap a raw phrase in FTS5 double-quotes for exact phrase matching."""
    escaped = phrase.replace('"', '""')
    return f'"{escaped}"'


# ── Co-occurrence ─────────────────────────────────────────────────────────────

def cooccur(conn: sqlite3.Connection, term_a: str, term_b: str, window: float, limit: int):
    """
    Find episodes where *term_a* and *term_b* appear within *window* seconds
    of each other.  Returns rows of (series, youtube_id, start_a, text_a, start_b, text_b).
    """
    fts_a = _fts_escape(term_a)
    fts_b = _fts_escape(term_b)

    rows = conn.execute(
        """
        WITH a AS (
            SELECT s.episode_id, s.start AS start_a, s.text AS text_a
            FROM snippets_fts
            JOIN snippets s ON snippets_fts.rowid = s.id
            WHERE snippets_fts MATCH ?
        ),
        b AS (
            SELECT s.episode_id, s.start AS start_b, s.text AS text_b
            FROM snippets_fts
            JOIN snippets s ON snippets_fts.rowid = s.id
            WHERE snippets_fts MATCH ?
        )
        SELECT
            e.series,
            e.youtube_id,
            e.relative_path,
            a.start_a,
            a.text_a,
            b.start_b,
            b.text_b,
            ABS(a.start_a - b.start_b) AS gap
        FROM a
        JOIN b ON a.episode_id = b.episode_id
            AND a.start_a != b.start_b
            AND ABS(a.start_a - b.start_b) <= ?
        JOIN episodes e ON e.id = a.episode_id
        ORDER BY gap
        LIMIT ?
        """,
        (fts_a, fts_b, window, limit),
    ).fetchall()
    return rows


# ── Episode info ──────────────────────────────────────────────────────────────

def episode_info(conn: sqlite3.Connection, youtube_id: str):
    row = conn.execute(
        "SELECT * FROM episodes WHERE youtube_id = ?", (youtube_id,)
    ).fetchone()
    if row is None:
        return None, []
    snippets = conn.execute(
        "SELECT start, duration, text FROM snippets WHERE episode_id = ? ORDER BY start",
        (row["id"],),
    ).fetchall()
    return row, snippets


# ── Statistics ────────────────────────────────────────────────────────────────

def stats(conn: sqlite3.Connection):
    ep_count = conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
    sn_count = conn.execute("SELECT COUNT(*) FROM snippets").fetchone()[0]
    series_count = conn.execute("SELECT COUNT(DISTINCT series) FROM episodes").fetchone()[0]
    top_series = conn.execute(
        "SELECT series, COUNT(*) AS n FROM episodes GROUP BY series ORDER BY n DESC LIMIT 10"
    ).fetchall()
    return ep_count, sn_count, series_count, top_series


# ── Display helpers ───────────────────────────────────────────────────────────

def fmt_time(seconds: float) -> str:
    s = int(seconds)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def print_search_results(rows, phrase: str, as_json: bool) -> None:
    if as_json:
        out = [
            {
                "series": r["series"],
                "youtube_id": r["youtube_id"],
                "path": r["relative_path"],
                "start": r["start"],
                "duration": r["duration"],
                "text": r["text"],
            }
            for r in rows
        ]
        print(json_mod.dumps(out, indent=2, ensure_ascii=False))
        return

    if not rows:
        print(f"No results for "{phrase}".")
        return

    print(f"\n{'─'*70}")
    print(f"  Search: "{phrase}"   ({len(rows)} results shown)")
    print(f"{'─'*70}")
    for r in rows:
        url = f"https://youtu.be/{r['youtube_id']}?t={int(r['start'])}"
        print(f"\n  Series  : {r['series']}")
        print(f"  Video   : {r['youtube_id']}  @ {fmt_time(r['start'])}  →  {url}")
        print(f"  Path    : {r['relative_path']}")
        print(f"  Snippet : {textwrap.fill(r['text'], 66, initial_indent='    ', subsequent_indent='    ')}")
    print(f"\n{'─'*70}\n")


def print_cooccur_results(rows, term_a: str, term_b: str, as_json: bool) -> None:
    if as_json:
        out = [
            {
                "series": r["series"],
                "youtube_id": r["youtube_id"],
                "path": r["relative_path"],
                "term_a": {"start": r["start_a"], "text": r["text_a"]},
                "term_b": {"start": r["start_b"], "text": r["text_b"]},
                "gap_seconds": r["gap"],
            }
            for r in rows
        ]
        print(json_mod.dumps(out, indent=2, ensure_ascii=False))
        return

    if not rows:
        print(f"No co-occurrences found for "{term_a}" + "{term_b}".")
        return

    print(f"\n{'─'*70}")
    print(f"  Co-occurrence: "{term_a}" + "{term_b}"   ({len(rows)} results)")
    print(f"{'─'*70}")
    for r in rows:
        url = f"https://youtu.be/{r['youtube_id']}?t={int(min(r['start_a'], r['start_b']))}"
        print(f"\n  Series  : {r['series']}")
        print(f"  Video   : {r['youtube_id']}  →  {url}")
        print(f"  [{fmt_time(r['start_a'])}] {r['text_a']}")
        print(f"  [{fmt_time(r['start_b'])}] {r['text_b']}")
        print(f"  Gap     : {r['gap']:.1f}s")
    print(f"\n{'─'*70}\n")


def print_episode(row, snippets, as_json: bool) -> None:
    if as_json:
        out = {
            "series": row["series"],
            "youtube_id": row["youtube_id"],
            "path": row["relative_path"],
            "language": row["language"],
            "is_generated": bool(row["is_generated"]),
            "imported_at": row["imported_at"],
            "snippets": [
                {"start": s["start"], "duration": s["duration"], "text": s["text"]}
                for s in snippets
            ],
        }
        print(json_mod.dumps(out, indent=2, ensure_ascii=False))
        return

    print(f"\n{'─'*70}")
    print(f"  Episode : {row['youtube_id']}")
    print(f"  Series  : {row['series']}")
    print(f"  Path    : {row['relative_path']}")
    print(f"  URL     : https://youtu.be/{row['youtube_id']}")
    print(f"  Snippets: {len(snippets)}")
    print(f"{'─'*70}")
    for s in snippets[:20]:
        print(f"  [{fmt_time(s['start'])}]  {s['text']}")
    if len(snippets) > 20:
        print(f"  … ({len(snippets) - 20} more snippets; use --json for full output)")
    print(f"{'─'*70}\n")


def print_stats(ep_count, sn_count, series_count, top_series) -> None:
    print(f"\n{'─'*70}")
    print("  Database Statistics")
    print(f"{'─'*70}")
    print(f"  Episodes  : {ep_count:,}")
    print(f"  Snippets  : {sn_count:,}")
    print(f"  Series    : {series_count:,}")
    print(f"\n  Top 10 series by episode count:")
    for row in top_series:
        print(f"    {row['n']:4d}  {row['series']}")
    print(f"{'─'*70}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Search the Game Grumps transcript database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB),
        help=f"Path to the SQLite database (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max results to return (default: {DEFAULT_LIMIT})",
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "phrase",
        nargs="?",
        help="Phrase to search for",
    )
    mode.add_argument(
        "--cooccur",
        nargs=2,
        metavar=("TERM_A", "TERM_B"),
        help="Find snippets where both terms appear within --window seconds",
    )
    mode.add_argument(
        "--episode",
        metavar="YOUTUBE_ID",
        help="Show all snippets for a specific YouTube video ID",
    )
    mode.add_argument(
        "--stats",
        action="store_true",
        help="Print database statistics",
    )

    parser.add_argument(
        "--series",
        default=None,
        help="Filter search results by series name (exact match)",
    )
    parser.add_argument(
        "--window",
        type=float,
        default=30.0,
        help="Time window in seconds for --cooccur (default: 30)",
    )

    args = parser.parse_args()

    # If no mode flag was given but no positional phrase either, show help.
    if not (args.phrase or args.cooccur or args.episode or args.stats):
        parser.print_help()
        sys.exit(1)

    conn = open_db(Path(args.db))

    if args.stats:
        ep_count, sn_count, series_count, top_series = stats(conn)
        print_stats(ep_count, sn_count, series_count, top_series)

    elif args.episode:
        row, snippets = episode_info(conn, args.episode)
        if row is None:
            print(f"No episode found with YouTube ID: {args.episode}", file=sys.stderr)
            sys.exit(1)
        print_episode(row, snippets, args.as_json)

    elif args.cooccur:
        term_a, term_b = args.cooccur
        rows = cooccur(conn, term_a, term_b, args.window, args.limit)
        print_cooccur_results(rows, term_a, term_b, args.as_json)

    else:
        rows = search(conn, args.phrase, args.series, args.limit)
        print_search_results(rows, args.phrase, args.as_json)

    conn.close()


if __name__ == "__main__":
    main()
