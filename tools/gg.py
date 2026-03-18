#!/usr/bin/env python3
"""
gg.py - Game Grumps Transcript Search & Correlation CLI

Commands:
  build-index   Walk the transcripts/ tree and build data/gg_index.sqlite
  search        Full-text search across all transcripts
  stats         Show corpus statistics and top terms for a query
  correlate     Find episodes/snippets where two terms co-occur within a time window

Usage:
  python tools/gg.py build-index
  python tools/gg.py search "banana"
  python tools/gg.py search "kiss your dad" --limit 20 --series "Steam Train"
  python tools/gg.py stats "banana"
  python tools/gg.py correlate "banana" "cake" --window 30
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import textwrap
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
TRANSCRIPTS_DIR = REPO_ROOT / "transcripts"
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "gg_index.sqlite"

# Regex to extract video_id from filenames like "[abc123].txt"
VIDEO_ID_RE = re.compile(r"^\[(.+)\]\.txt$", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS episodes (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id  TEXT NOT NULL UNIQUE,
            series    TEXT NOT NULL,
            file_path TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS snippets (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            episode_id INTEGER NOT NULL REFERENCES episodes(id),
            start      REAL NOT NULL,
            duration   REAL NOT NULL,
            text       TEXT NOT NULL
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS snippets_fts USING fts5(
            text,
            content='snippets',
            content_rowid='id',
            tokenize='porter unicode61'
        );

        CREATE INDEX IF NOT EXISTS idx_snippets_episode ON snippets(episode_id);
        CREATE INDEX IF NOT EXISTS idx_episodes_series  ON episodes(series);
        """
    )
    conn.commit()


def rebuild_fts(conn: sqlite3.Connection) -> None:
    """Rebuild the FTS5 index from the snippets table."""
    conn.execute("INSERT INTO snippets_fts(snippets_fts) VALUES('rebuild')")
    conn.commit()


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def ingest_file(
    conn: sqlite3.Connection,
    txt_path: Path,
    series: str,
    incremental: bool = True,
) -> int:
    """
    Parse a single transcript .txt (JSON) file and insert into the DB.
    Returns the number of snippets inserted (0 if skipped in incremental mode).
    """
    m = VIDEO_ID_RE.match(txt_path.name)
    if not m:
        return 0
    video_id = m.group(1)

    rel_path = str(txt_path.relative_to(REPO_ROOT))

    if incremental:
        existing = conn.execute(
            "SELECT id FROM episodes WHERE video_id = ?", (video_id,)
        ).fetchone()
        if existing:
            return 0

    # Read + parse
    raw = txt_path.read_bytes()
    # Strip BOM if present
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    try:
        data = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return 0

    snippets_data = data.get("snippets", [])
    if not snippets_data:
        return 0

    # Upsert episode row
    conn.execute(
        """
        INSERT INTO episodes (video_id, series, file_path)
        VALUES (?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            series    = excluded.series,
            file_path = excluded.file_path
        """,
        (video_id, series, rel_path),
    )
    episode_row = conn.execute(
        "SELECT id FROM episodes WHERE video_id = ?", (video_id,)
    ).fetchone()
    episode_id = episode_row["id"]

    # Delete old snippets (for re-ingestion)
    conn.execute("DELETE FROM snippets WHERE episode_id = ?", (episode_id,))

    rows = [
        (episode_id, s.get("start", 0.0), s.get("duration", 0.0), s.get("text", ""))
        for s in snippets_data
        if isinstance(s, dict) and s.get("text", "").strip()
    ]
    conn.executemany(
        "INSERT INTO snippets (episode_id, start, duration, text) VALUES (?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def cmd_build_index(args: argparse.Namespace) -> None:
    """Walk transcripts/ and populate the SQLite database."""
    incremental = not args.full

    conn = get_connection()
    create_schema(conn)

    if args.full:
        print("Full rebuild requested — dropping existing data …")
        conn.executescript(
            """
            DELETE FROM snippets;
            DELETE FROM episodes;
            INSERT INTO snippets_fts(snippets_fts) VALUES('rebuild');
            """
        )
        conn.commit()

    total_episodes = 0
    total_snippets = 0
    skipped = 0

    series_dirs = sorted(
        p for p in TRANSCRIPTS_DIR.iterdir() if p.is_dir()
    )

    for series_dir in series_dirs:
        series_name = series_dir.name
        txt_files = sorted(series_dir.glob("*.txt"))
        for txt_path in txt_files:
            n = ingest_file(conn, txt_path, series_name, incremental=incremental)
            if n == 0:
                skipped += 1
            else:
                total_episodes += 1
                total_snippets += n

        # Commit per series for progress visibility
        conn.commit()

    # Rebuild FTS after bulk insert
    print("Rebuilding FTS index …")
    rebuild_fts(conn)
    conn.close()

    print(
        f"Done.  Episodes added: {total_episodes}  "
        f"Snippets added: {total_snippets}  "
        f"Skipped (already indexed): {skipped}"
    )
    print(f"Database: {DB_PATH}")


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

_SNIPPET_WIDTH = 120


def format_timestamp(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m}:{sec:02d}"


def cmd_search(args: argparse.Namespace) -> None:
    if not DB_PATH.exists():
        print("Database not found. Run `build-index` first.", file=sys.stderr)
        sys.exit(1)

    conn = get_connection()

    query = args.query
    limit = args.limit
    series_filter = args.series

    # Build SQL
    params: list = [query]
    series_clause = ""
    if series_filter:
        series_clause = "AND e.series LIKE ?"
        params.append(f"%{series_filter}%")

    params.append(limit)

    sql = f"""
        SELECT
            e.video_id,
            e.series,
            e.file_path,
            s.start,
            s.duration,
            s.text,
            rank
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        {series_clause}
        ORDER BY rank
        LIMIT ?
    """

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    if not rows:
        print(f"No results for: {query!r}")
        return

    print(f"\nSearch results for: {query!r}  ({len(rows)} shown)\n")
    print("=" * 80)
    for i, row in enumerate(rows, 1):
        ts = format_timestamp(row["start"])
        snippet = textwrap.fill(row["text"], width=_SNIPPET_WIDTH)
        print(
            f"[{i}] {row['series']}\n"
            f"    video_id : {row['video_id']}\n"
            f"    file     : {row['file_path']}\n"
            f"    time     : {ts}\n"
            f"    snippet  : {snippet}\n"
        )
    print("=" * 80)
    yt_prefix = "https://www.youtube.com/watch?v="
    print(
        f"\nTip: open any video with {yt_prefix}<video_id>&t=<seconds>\n"
        "     e.g. to jump to timestamp 3:45 use &t=225\n"
    )


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def cmd_stats(args: argparse.Namespace) -> None:
    if not DB_PATH.exists():
        print("Database not found. Run `build-index` first.", file=sys.stderr)
        sys.exit(1)

    conn = get_connection()

    term = args.term

    # Corpus overview
    ep_count = conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
    sn_count = conn.execute("SELECT COUNT(*) FROM snippets").fetchone()[0]
    series_count = conn.execute(
        "SELECT COUNT(DISTINCT series) FROM episodes"
    ).fetchone()[0]

    print(f"\nCorpus statistics")
    print(f"  Series  : {series_count}")
    print(f"  Episodes: {ep_count}")
    print(f"  Snippets: {sn_count}")

    if not term:
        # Top 20 series by episode count
        rows = conn.execute(
            """
            SELECT series, COUNT(*) AS cnt
            FROM episodes
            GROUP BY series
            ORDER BY cnt DESC
            LIMIT 20
            """
        ).fetchall()
        print(f"\nTop 20 series by episode count:")
        for r in rows:
            print(f"  {r['cnt']:>5}  {r['series']}")
        conn.close()
        return

    # Term-specific stats
    sql = """
        SELECT
            e.series,
            e.video_id,
            e.file_path,
            COUNT(*) AS hit_count
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        GROUP BY e.id
        ORDER BY hit_count DESC
        LIMIT 30
    """
    rows = conn.execute(sql, (term,)).fetchall()

    total_hits = conn.execute(
        """
        SELECT COUNT(*) FROM snippets_fts sf
        WHERE snippets_fts MATCH ?
        """,
        (term,),
    ).fetchone()[0]

    top_series = conn.execute(
        """
        SELECT e.series, COUNT(*) AS cnt
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        GROUP BY e.series
        ORDER BY cnt DESC
        LIMIT 10
        """,
        (term,),
    ).fetchall()

    conn.close()

    print(f"\nStats for term: {term!r}")
    print(f"  Total matching snippets : {total_hits}")
    print(f"  Episodes with match     : {len(rows)}")

    print(f"\nTop series by mention count:")
    for r in top_series:
        print(f"  {r['cnt']:>5}  {r['series']}")

    print(f"\nTop episodes by mention count:")
    for r in rows[:20]:
        print(
            f"  {r['hit_count']:>4}  [{r['video_id']}]  {r['series']}"
        )


# ---------------------------------------------------------------------------
# Correlate / Co-occurrence
# ---------------------------------------------------------------------------

def cmd_correlate(args: argparse.Namespace) -> None:
    """
    Find episodes/snippets where two terms appear within `window` seconds of
    each other.
    """
    if not DB_PATH.exists():
        print("Database not found. Run `build-index` first.", file=sys.stderr)
        sys.exit(1)

    term_a = args.term_a
    term_b = args.term_b
    window = args.window
    limit = args.limit

    conn = get_connection()

    # Fetch all matching snippets for each term, then find close pairs in Python.
    # This avoids complex SQL and works well for the corpus size.

    def fetch_snippets_for_term(term: str) -> dict[int, list[tuple[float, str]]]:
        """Returns {episode_id: [(start, text), ...]} for all FTS matches."""
        rows = conn.execute(
            """
            SELECT s.episode_id, s.start, s.text
            FROM snippets_fts sf
            JOIN snippets s ON s.id = sf.rowid
            WHERE snippets_fts MATCH ?
            ORDER BY s.episode_id, s.start
            """,
            (term,),
        ).fetchall()
        result: dict[int, list[tuple[float, str]]] = {}
        for r in rows:
            result.setdefault(r["episode_id"], []).append((r["start"], r["text"]))
        return result

    hits_a = fetch_snippets_for_term(term_a)
    hits_b = fetch_snippets_for_term(term_b)

    # Find episode_ids present in both
    common_ids = set(hits_a.keys()) & set(hits_b.keys())

    # For each common episode, find pairs within `window` seconds
    matches: list[dict] = []

    for ep_id in sorted(common_ids):
        ep_row = conn.execute(
            "SELECT video_id, series, file_path FROM episodes WHERE id = ?", (ep_id,)
        ).fetchone()

        for start_a, text_a in hits_a[ep_id]:
            for start_b, text_b in hits_b[ep_id]:
                if abs(start_a - start_b) <= window:
                    matches.append(
                        {
                            "episode_id": ep_id,
                            "video_id": ep_row["video_id"],
                            "series": ep_row["series"],
                            "file_path": ep_row["file_path"],
                            "start_a": start_a,
                            "text_a": text_a,
                            "start_b": start_b,
                            "text_b": text_b,
                        }
                    )

    conn.close()

    if not matches:
        print(
            f"No co-occurrences found for {term_a!r} and {term_b!r} "
            f"within {window}s window."
        )
        return

    # Deduplicate by episode + pair of starts (show best / first few)
    seen: set[tuple[int, int, int]] = set()
    deduped = []
    for m in matches:
        key = (m["episode_id"], round(m["start_a"]), round(m["start_b"]))
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    print(
        f"\nCo-occurrence of {term_a!r} and {term_b!r} within {window}s "
        f"— {len(deduped)} match(es) found  ({len(common_ids)} episodes)\n"
    )
    print("=" * 80)
    for i, m in enumerate(deduped[:limit], 1):
        ts_a = format_timestamp(m["start_a"])
        ts_b = format_timestamp(m["start_b"])
        print(
            f"[{i}] {m['series']}\n"
            f"    video_id : {m['video_id']}\n"
            f"    file     : {m['file_path']}\n"
            f"    [{term_a!r} @ {ts_a}] {m['text_a']}\n"
            f"    [{term_b!r} @ {ts_b}] {m['text_b']}\n"
        )
    print("=" * 80)
    yt_prefix = "https://www.youtube.com/watch?v="
    print(
        f"\nTip: open any video with {yt_prefix}<video_id>&t=<seconds>\n"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gg",
        description="Game Grumps Transcript Search & Correlation CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Examples:
              python tools/gg.py build-index
              python tools/gg.py search "banana"
              python tools/gg.py search "kiss your dad" --limit 20
              python tools/gg.py search "grump" --series "Sonic"
              python tools/gg.py stats
              python tools/gg.py stats "banana"
              python tools/gg.py correlate "banana" "cake" --window 30
            """
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # build-index
    p_build = sub.add_parser(
        "build-index",
        help="Build or update the SQLite search index from transcripts/",
    )
    p_build.add_argument(
        "--full",
        action="store_true",
        help="Drop and fully rebuild the index (default: incremental)",
    )
    p_build.set_defaults(func=cmd_build_index)

    # search
    p_search = sub.add_parser(
        "search",
        help="Full-text search across all transcripts",
    )
    p_search.add_argument("query", help="Search phrase or term")
    p_search.add_argument(
        "--limit", "-n", type=int, default=20, help="Max results (default: 20)"
    )
    p_search.add_argument(
        "--series", "-s", help="Filter by series name (partial match)"
    )
    p_search.set_defaults(func=cmd_search)

    # stats
    p_stats = sub.add_parser(
        "stats",
        help="Show corpus statistics, optionally for a specific term",
    )
    p_stats.add_argument(
        "term",
        nargs="?",
        default=None,
        help="Optional term to show per-series / per-episode stats for",
    )
    p_stats.set_defaults(func=cmd_stats)

    # correlate
    p_corr = sub.add_parser(
        "correlate",
        help="Find episodes where two terms co-occur within a time window",
    )
    p_corr.add_argument("term_a", help="First term")
    p_corr.add_argument("term_b", help="Second term")
    p_corr.add_argument(
        "--window",
        "-w",
        type=float,
        default=30.0,
        help="Time window in seconds (default: 30)",
    )
    p_corr.add_argument(
        "--limit", "-n", type=int, default=20, help="Max results (default: 20)"
    )
    p_corr.set_defaults(func=cmd_correlate)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
