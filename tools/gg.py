#!/usr/bin/env python3
"""
gg.py - Game Grumps Transcript Search

SETUP (one time):
  python tools/gg.py build-index

USAGE:
  python tools/gg.py "banana"
  python tools/gg.py "kiss your dad"
  python tools/gg.py "banana" "cake"          <- find where both appear together
  python tools/gg.py "banana" --in "Sonic"    <- limit to one series
  python tools/gg.py --stats                  <- corpus overview
  python tools/gg.py --stats "banana"         <- breakdown by series/episode
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
TRANSCRIPTS_DIR = REPO_ROOT / "transcripts"
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "gg_index.sqlite"

VIDEO_ID_RE = re.compile(r"^\[(.+)\]\.txt$", re.IGNORECASE)
YT = "https://www.youtube.com/watch?v="

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
    conn.execute("INSERT INTO snippets_fts(snippets_fts) VALUES('rebuild')")
    conn.commit()


def require_db() -> None:
    if not DB_PATH.exists():
        print("Index not found. Run this first:\n  python tools/gg.py build-index", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def ingest_file(conn: sqlite3.Connection, txt_path: Path, series: str, incremental: bool = True) -> int:
    m = VIDEO_ID_RE.match(txt_path.name)
    if not m:
        return 0
    video_id = m.group(1)
    rel_path = str(txt_path.relative_to(REPO_ROOT))

    if incremental:
        if conn.execute("SELECT id FROM episodes WHERE video_id = ?", (video_id,)).fetchone():
            return 0

    raw = txt_path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    try:
        data = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return 0

    snippets_data = data.get("snippets", [])
    if not snippets_data:
        return 0

    conn.execute(
        """
        INSERT INTO episodes (video_id, series, file_path) VALUES (?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET series = excluded.series, file_path = excluded.file_path
        """,
        (video_id, series, rel_path),
    )
    episode_id = conn.execute("SELECT id FROM episodes WHERE video_id = ?", (video_id,)).fetchone()["id"]
    conn.execute("DELETE FROM snippets WHERE episode_id = ?", (episode_id,))

    rows = [
        (episode_id, s.get("start", 0.0), s.get("duration", 0.0), s.get("text", ""))
        for s in snippets_data
        if isinstance(s, dict) and s.get("text", "").strip()
    ]
    conn.executemany("INSERT INTO snippets (episode_id, start, duration, text) VALUES (?, ?, ?, ?)", rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def ts(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def yt_link(video_id: str, start: float) -> str:
    return f"{YT}{video_id}&t={int(start)}"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_build_index(full: bool = False) -> None:
    conn = get_connection()
    create_schema(conn)

    if full:
        print("Full rebuild — clearing existing data...")
        conn.executescript(
            "DELETE FROM snippets; DELETE FROM episodes; "
            "INSERT INTO snippets_fts(snippets_fts) VALUES('rebuild');"
        )
        conn.commit()

    episodes_added = snippets_added = skipped = 0
    for series_dir in sorted(p for p in TRANSCRIPTS_DIR.iterdir() if p.is_dir()):
        for txt_path in sorted(series_dir.glob("*.txt")):
            n = ingest_file(conn, txt_path, series_dir.name, incremental=not full)
            if n:
                episodes_added += 1
                snippets_added += n
            else:
                skipped += 1
        conn.commit()

    print("Rebuilding full-text index...")
    rebuild_fts(conn)
    conn.close()
    print(f"Done. {episodes_added} episodes, {snippets_added} snippets added. ({skipped} already indexed)")
    print(f"Database: {DB_PATH}")


def cmd_search(query: str, series_filter: str | None, limit: int) -> None:
    require_db()
    conn = get_connection()

    params: list = [query]
    series_clause = ""
    if series_filter:
        series_clause = "AND e.series LIKE ?"
        params.append(f"%{series_filter}%")
    params.append(limit)

    rows = conn.execute(
        f"""
        SELECT e.video_id, e.series, s.start, s.text, rank
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        {series_clause}
        ORDER BY rank
        LIMIT ?
        """,
        params,
    ).fetchall()
    conn.close()

    if not rows:
        print(f'No results for "{query}"')
        return

    print(f'"{query}" — {len(rows)} result(s) shown\n')
    for row in rows:
        print(f'{row["series"]}  |  {ts(row["start"])}')
        print(f'  {row["text"]}')
        print(f'  {yt_link(row["video_id"], row["start"])}')
        print()


def cmd_correlate(term_a: str, term_b: str, window: float, limit: int) -> None:
    require_db()
    conn = get_connection()

    def fetch(term: str) -> dict[int, list[tuple[float, str]]]:
        out: dict[int, list[tuple[float, str]]] = {}
        for r in conn.execute(
            """
            SELECT s.episode_id, s.start, s.text
            FROM snippets_fts sf
            JOIN snippets s ON s.id = sf.rowid
            WHERE snippets_fts MATCH ?
            ORDER BY s.episode_id, s.start
            """,
            (term,),
        ).fetchall():
            out.setdefault(r["episode_id"], []).append((r["start"], r["text"]))
        return out

    hits_a = fetch(term_a)
    hits_b = fetch(term_b)
    common = set(hits_a) & set(hits_b)

    matches: list[dict] = []
    for ep_id in sorted(common):
        ep = conn.execute("SELECT video_id, series FROM episodes WHERE id = ?", (ep_id,)).fetchone()
        for sa, ta in hits_a[ep_id]:
            for sb, tb in hits_b[ep_id]:
                if abs(sa - sb) <= window:
                    matches.append({"video_id": ep["video_id"], "series": ep["series"],
                                    "start_a": sa, "text_a": ta, "start_b": sb, "text_b": tb,
                                    "ep_id": ep_id})

    conn.close()

    if not matches:
        print(f'No results: "{term_a}" and "{term_b}" never appear within {window}s of each other.')
        return

    seen: set[tuple[int, int, int]] = set()
    deduped = []
    for m in matches:
        key = (m["ep_id"], round(m["start_a"]), round(m["start_b"]))
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    print(f'"{term_a}" + "{term_b}" within {window}s — {len(deduped)} match(es) across {len(common)} episode(s)\n')
    for m in deduped[:limit]:
        print(f'{m["series"]}')
        print(f'  {ts(m["start_a"])}  {m["text_a"]}')
        print(f'  {ts(m["start_b"])}  {m["text_b"]}')
        print(f'  {yt_link(m["video_id"], min(m["start_a"], m["start_b"]))}')
        print()


def cmd_stats(term: str | None) -> None:
    require_db()
    conn = get_connection()

    ep_count  = conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
    sn_count  = conn.execute("SELECT COUNT(*) FROM snippets").fetchone()[0]
    ser_count = conn.execute("SELECT COUNT(DISTINCT series) FROM episodes").fetchone()[0]

    print(f"Corpus: {ser_count} series, {ep_count} episodes, {sn_count} snippets\n")

    if not term:
        rows = conn.execute(
            "SELECT series, COUNT(*) AS cnt FROM episodes GROUP BY series ORDER BY cnt DESC LIMIT 20"
        ).fetchall()
        print("Top 20 series by episode count:")
        for r in rows:
            print(f"  {r['cnt']:>5}  {r['series']}")
        conn.close()
        return

    total = conn.execute(
        "SELECT COUNT(*) FROM snippets_fts WHERE snippets_fts MATCH ?", (term,)
    ).fetchone()[0]

    top_series = conn.execute(
        """
        SELECT e.series, COUNT(*) AS cnt
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        GROUP BY e.series ORDER BY cnt DESC LIMIT 10
        """,
        (term,),
    ).fetchall()

    top_eps = conn.execute(
        """
        SELECT e.series, e.video_id, COUNT(*) AS cnt
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        GROUP BY e.id ORDER BY cnt DESC LIMIT 20
        """,
        (term,),
    ).fetchall()

    conn.close()

    print(f'"{term}" — {total} mentions across {len(top_eps)} episode(s)\n')
    print("Top series:")
    for r in top_series:
        print(f"  {r['cnt']:>5}  {r['series']}")
    print("\nTop episodes:")
    for r in top_eps:
        print(f"  {r['cnt']:>4}  {r['series']}  —  {YT}{r['video_id']}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="gg",
        description="Search Game Grumps transcripts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python tools/gg.py build-index\n'
            '  python tools/gg.py "banana"\n'
            '  python tools/gg.py "banana" --in "Sonic"\n'
            '  python tools/gg.py "banana" "cake"\n'
            '  python tools/gg.py --stats\n'
            '  python tools/gg.py --stats "banana"\n'
        ),
    )

    parser.add_argument("terms", nargs="*", help='Term(s) to search. One term = search. Two terms = co-occurrence.')
    parser.add_argument("--in", dest="series", metavar="SERIES", help="Limit search to a series (partial name match)")
    parser.add_argument("--window", "-w", type=float, default=30.0, metavar="SECS", help="Co-occurrence time window in seconds (default: 30)")
    parser.add_argument("--limit", "-n", type=int, default=20, metavar="N", help="Max results to show (default: 20)")
    parser.add_argument("--stats", nargs="?", const="", metavar="TERM", help="Show corpus stats, optionally for a specific term")
    parser.add_argument("--full", action="store_true", help="(build-index only) Drop and fully rebuild the index")

    args = parser.parse_args()

    # build-index is a special positional keyword
    if args.terms and args.terms[0] == "build-index":
        cmd_build_index(full=args.full)
    elif args.stats is not None:
        cmd_stats(term=args.stats or None)
    elif len(args.terms) == 2:
        cmd_correlate(args.terms[0], args.terms[1], window=args.window, limit=args.limit)
    elif len(args.terms) == 1:
        cmd_search(args.terms[0], series_filter=args.series, limit=args.limit)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
