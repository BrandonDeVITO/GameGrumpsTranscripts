#!/usr/bin/env python3
"""
gg.py — Game Grumps Transcript Search CLI

Commands:
  build-index   Scan transcripts/ and build (or rebuild) data/gg_index.sqlite
  search        Full-text search across all snippets
  cooccur       Find episodes where two phrases appear within a time window
  episode       Look up metadata for a video_id (optionally fetching YouTube title)

Usage:
  python tools/gg.py build-index [--transcripts-dir TRANSCRIPTS_DIR] [--db DB_PATH] [--dry-run]
  python tools/gg.py search QUERY [--series SERIES] [--video VIDEO_ID] [--limit N] [--json]
  python tools/gg.py cooccur PHRASE_A PHRASE_B [--window SECONDS] [--limit N] [--json]
  python tools/gg.py episode VIDEO_ID [--fetch] [--json]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import textwrap
import time
from pathlib import Path
from typing import Iterator

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TRANSCRIPTS_DIR = REPO_ROOT / "transcripts"
DEFAULT_DB_PATH = REPO_ROOT / "data" / "gg_index.sqlite"

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS episodes (
    video_id    TEXT PRIMARY KEY,
    series      TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    language    TEXT,
    is_generated INTEGER,
    snippet_count INTEGER,
    indexed_at  TEXT
);

CREATE TABLE IF NOT EXISTS snippets (
    id          INTEGER PRIMARY KEY,
    video_id    TEXT NOT NULL REFERENCES episodes(video_id),
    series      TEXT NOT NULL,
    start       REAL NOT NULL,
    duration    REAL,
    text        TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS snippets_fts USING fts5(
    text,
    video_id UNINDEXED,
    series UNINDEXED,
    start UNINDEXED,
    content='snippets',
    content_rowid='id',
    tokenize='unicode61'
);

CREATE TRIGGER IF NOT EXISTS snippets_ai AFTER INSERT ON snippets BEGIN
    INSERT INTO snippets_fts(rowid, text, video_id, series, start)
    VALUES (new.id, new.text, new.video_id, new.series, new.start);
END;
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
VIDEO_ID_RE = re.compile(r"\[(.+?)\]")


def extract_video_id(filename: str) -> str | None:
    """Extract video_id from filenames like '[abc123].txt'."""
    m = VIDEO_ID_RE.search(filename)
    return m.group(1) if m else None


def iter_transcript_files(transcripts_dir: Path) -> Iterator[tuple[Path, str, str]]:
    """Yield (file_path, series_name, video_id) for every .txt transcript."""
    for series_dir in sorted(transcripts_dir.iterdir()):
        if not series_dir.is_dir():
            continue
        series = series_dir.name
        for txt_file in sorted(series_dir.glob("*.txt")):
            video_id = extract_video_id(txt_file.name)
            if video_id is None:
                # Fall back to stem as id
                video_id = txt_file.stem
            yield txt_file, series, video_id


def read_transcript(path: Path) -> dict | None:
    """Parse a JSON transcript file, handling encoding gracefully."""
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            with open(path, encoding=enc, errors="replace") as fh:
                data = json.load(fh)
            return data
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
    return None


def get_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# build-index
# ---------------------------------------------------------------------------
def cmd_build_index(args: argparse.Namespace) -> None:
    transcripts_dir = Path(args.transcripts_dir)
    db_path = Path(args.db)
    dry_run: bool = args.dry_run

    if not transcripts_dir.exists():
        print(f"ERROR: transcripts dir not found: {transcripts_dir}", file=sys.stderr)
        sys.exit(1)

    all_files = list(iter_transcript_files(transcripts_dir))
    print(f"Found {len(all_files)} transcript files under '{transcripts_dir}'.")

    if dry_run:
        print("[dry-run] Would index:", len(all_files), "files. No DB written.")
        for path, series, vid in all_files[:5]:
            print(f"  {series} / {vid}")
        if len(all_files) > 5:
            print(f"  … and {len(all_files) - 5} more.")
        return

    conn = get_db(db_path)
    indexed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    skipped = 0
    inserted_eps = 0
    inserted_snips = 0
    BATCH = 500

    episode_rows: list[tuple] = []
    snippet_rows: list[tuple] = []
    seen_video_ids: set[str] = set()

    for i, (path, series, video_id) in enumerate(all_files):
        if i % 500 == 0:
            print(f"  Processing {i}/{len(all_files)} …")

        data = read_transcript(path)
        if data is None:
            skipped += 1
            continue

        snippets = data.get("snippets") or []
        language = data.get("language", "")
        is_generated = int(bool(data.get("is_generated", False)))
        rel_path = str(path.relative_to(REPO_ROOT))

        episode_rows.append((
            video_id, series, rel_path, language, is_generated,
            len(snippets), indexed_at,
        ))

        # Only index snippets once per video_id (same video may appear in multiple series)
        if video_id not in seen_video_ids:
            seen_video_ids.add(video_id)
            for snip in snippets:
                text = (snip.get("text") or "").strip()
                if not text:
                    continue
                snippet_rows.append((video_id, series, snip.get("start", 0.0),
                                      snip.get("duration"), text))

        if len(episode_rows) >= BATCH:
            _flush(conn, episode_rows, snippet_rows)
            inserted_eps += len(episode_rows)
            inserted_snips += len(snippet_rows)
            episode_rows, snippet_rows = [], []

    if episode_rows:
        _flush(conn, episode_rows, snippet_rows)
        inserted_eps += len(episode_rows)
        inserted_snips += len(snippet_rows)

    conn.close()
    print(f"Done. {inserted_eps} episodes, {inserted_snips} snippets indexed → {db_path}")
    if skipped:
        print(f"  Skipped {skipped} files (parse errors).")


def _flush(conn: sqlite3.Connection,
           episodes: list[tuple], snippets: list[tuple]) -> None:
    conn.executemany(
        """INSERT OR IGNORE INTO episodes
           (video_id, series, file_path, language, is_generated, snippet_count, indexed_at)
           VALUES (?,?,?,?,?,?,?)""",
        episodes,
    )
    conn.executemany(
        """INSERT INTO snippets (video_id, series, start, duration, text)
           VALUES (?,?,?,?,?)""",
        snippets,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------
def cmd_search(args: argparse.Namespace) -> None:
    db_path = Path(args.db)
    _require_db(db_path)

    query: str = args.query
    limit: int = args.limit
    series_filter: str | None = args.series
    video_filter: str | None = args.video
    as_json: bool = args.json

    conn = get_db(db_path)

    sql = """
        SELECT
            s.video_id,
            s.series,
            s.start,
            snippet(snippets_fts, 0, '>>>', '<<<', '…', 32) AS excerpt
        FROM snippets_fts
        JOIN snippets s ON snippets_fts.rowid = s.id
        WHERE snippets_fts MATCH ?
    """
    params: list = [query]

    if series_filter:
        sql += " AND s.series LIKE ?"
        params.append(f"%{series_filter}%")
    if video_filter:
        sql += " AND s.video_id = ?"
        params.append(video_filter)

    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    if not rows:
        print("No results found.")
        return

    if as_json:
        print(json.dumps([dict(r) for r in rows], indent=2))
        return

    print(f"{'#':<4} {'Video ID':<14} {'Series':<45} {'Start':>8}  Excerpt")
    print("-" * 100)
    for i, row in enumerate(rows, 1):
        series_trunc = row["series"][:44]
        excerpt = row["excerpt"].replace("\n", " ")
        print(f"{i:<4} {row['video_id']:<14} {series_trunc:<45} {row['start']:>8.1f}s  {excerpt}")


# ---------------------------------------------------------------------------
# cooccur
# ---------------------------------------------------------------------------
def cmd_cooccur(args: argparse.Namespace) -> None:
    db_path = Path(args.db)
    _require_db(db_path)

    phrase_a: str = args.phrase_a
    phrase_b: str = args.phrase_b
    window: float = args.window
    limit: int = args.limit
    as_json: bool = args.json

    conn = get_db(db_path)

    sql_a = """
        SELECT s.video_id, s.series, s.start AS start_a, s.text AS text_a
        FROM snippets_fts
        JOIN snippets s ON snippets_fts.rowid = s.id
        WHERE snippets_fts MATCH ?
    """
    sql_b = """
        SELECT s.video_id, s.start AS start_b, s.text AS text_b
        FROM snippets_fts
        JOIN snippets s ON snippets_fts.rowid = s.id
        WHERE snippets_fts MATCH ?
    """

    rows_a = conn.execute(sql_a, [phrase_a]).fetchall()
    rows_b = conn.execute(sql_b, [phrase_b]).fetchall()
    conn.close()

    # Index b by video_id
    b_by_vid: dict[str, list] = {}
    for row in rows_b:
        b_by_vid.setdefault(row["video_id"], []).append(row)

    results = []
    for row_a in rows_a:
        vid = row_a["video_id"]
        if vid not in b_by_vid:
            continue
        for row_b in b_by_vid[vid]:
            gap = abs(row_a["start_a"] - row_b["start_b"])
            if gap <= window:
                results.append({
                    "video_id": vid,
                    "series": row_a["series"],
                    "start_a": row_a["start_a"],
                    "text_a": row_a["text_a"],
                    "start_b": row_b["start_b"],
                    "text_b": row_b["text_b"],
                    "gap_seconds": round(gap, 2),
                })

    results.sort(key=lambda r: r["gap_seconds"])
    results = results[:limit]

    if not results:
        print(f"No episodes found where '{phrase_a}' and '{phrase_b}' occur within {window}s of each other.")
        return

    if as_json:
        print(json.dumps(results, indent=2))
        return

    print(f"Co-occurrences of '{phrase_a}' + '{phrase_b}' within {window}s  ({len(results)} results)")
    print("-" * 90)
    for r in results:
        print(f"  Video : {r['video_id']}  ({r['series']})")
        print(f"  A @{r['start_a']:.1f}s : {r['text_a']}")
        print(f"  B @{r['start_b']:.1f}s : {r['text_b']}")
        print(f"  Gap   : {r['gap_seconds']}s")
        print()


# ---------------------------------------------------------------------------
# episode
# ---------------------------------------------------------------------------
def cmd_episode(args: argparse.Namespace) -> None:
    db_path = Path(args.db)
    _require_db(db_path)

    video_id: str = args.video_id
    fetch: bool = args.fetch
    as_json: bool = args.json

    conn = get_db(db_path)
    row = conn.execute(
        "SELECT * FROM episodes WHERE video_id = ?", [video_id]
    ).fetchone()

    if row is None:
        print(f"No episode found for video_id '{video_id}'.")
        conn.close()
        return

    meta: dict = dict(row)
    meta["youtube_url"] = f"https://www.youtube.com/watch?v={video_id}"

    if fetch:
        yt_data = _fetch_youtube_meta(video_id)
        meta.update(yt_data)

    conn.close()

    if as_json:
        print(json.dumps(meta, indent=2))
        return

    print(f"Video ID    : {meta['video_id']}")
    print(f"Series      : {meta['series']}")
    print(f"File        : {meta['file_path']}")
    print(f"Language    : {meta['language']}  (auto={bool(meta['is_generated'])})")
    print(f"Snippets    : {meta['snippet_count']}")
    print(f"YouTube URL : {meta['youtube_url']}")
    if "yt_title" in meta:
        print(f"YT Title    : {meta['yt_title']}")
    if "yt_upload_date" in meta:
        print(f"Upload date : {meta['yt_upload_date']}")


def _fetch_youtube_meta(video_id: str) -> dict:
    """Attempt to fetch basic YouTube metadata via yt-dlp (optional)."""
    try:
        import subprocess
        result = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-playlist",
             f"https://www.youtube.com/watch?v={video_id}"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            info = json.loads(result.stdout)
            return {
                "yt_title": info.get("title"),
                "yt_upload_date": info.get("upload_date"),
                "yt_view_count": info.get("view_count"),
                "yt_duration": info.get("duration"),
            }
    except Exception:
        pass
    return {"yt_note": "yt-dlp not available or fetch failed; visit YouTube URL manually"}


# ---------------------------------------------------------------------------
# Module-level search API
# ---------------------------------------------------------------------------
def search(query: str, series: str | None = None, limit: int = 10,
           db_path: Path | str | None = None) -> list[dict]:
    """
    Python API for searching transcripts.

    Args:
        query:   FTS5 query string (phrase, boolean, prefix…)
        series:  Optional series/folder filter (substring match).
        limit:   Maximum number of results.
        db_path: Path to the SQLite DB (defaults to data/gg_index.sqlite).

    Returns:
        List of dicts with keys: video_id, series, start, excerpt.
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found at {db_path}. Run: python tools/gg.py build-index"
        )

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    sql = """
        SELECT
            s.video_id,
            s.series,
            s.start,
            snippet(snippets_fts, 0, '>>>', '<<<', '…', 32) AS excerpt
        FROM snippets_fts
        JOIN snippets s ON snippets_fts.rowid = s.id
        WHERE snippets_fts MATCH ?
    """
    params: list = [query]
    if series:
        sql += " AND s.series LIKE ?"
        params.append(f"%{series}%")
    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
def _require_db(db_path: Path) -> None:
    if not db_path.exists():
        print(
            f"ERROR: Database not found at {db_path}.\n"
            "Run: python tools/gg.py build-index",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        prog="gg",
        description="Game Grumps Transcript Search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python tools/gg.py build-index
              python tools/gg.py search "banana"
              python tools/gg.py search "kiss your dad" --limit 20
              python tools/gg.py search "bloodborne" --series "Bloodborne"
              python tools/gg.py cooccur "grumpcade" "arin" --window 60
              python tools/gg.py episode abc123XYZ --fetch
        """),
    )
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH),
                        help="Path to SQLite database (default: data/gg_index.sqlite)")

    sub = parser.add_subparsers(dest="command", required=True)

    # build-index
    p_build = sub.add_parser("build-index", help="Build (or rebuild) the search index")
    p_build.add_argument("--transcripts-dir", default=str(DEFAULT_TRANSCRIPTS_DIR),
                         help="Root transcripts directory")
    p_build.add_argument("--dry-run", action="store_true",
                         help="Scan files without writing to DB")

    # search
    p_search = sub.add_parser("search", help="Full-text search")
    p_search.add_argument("query", help="Search query (FTS5 syntax)")
    p_search.add_argument("--series", default=None,
                          help="Filter by series/folder name (substring)")
    p_search.add_argument("--video", default=None, help="Filter by video_id")
    p_search.add_argument("--limit", type=int, default=20, help="Max results (default 20)")
    p_search.add_argument("--json", action="store_true", help="Output as JSON")

    # cooccur
    p_co = sub.add_parser("cooccur",
                           help="Find episodes where two phrases appear near each other")
    p_co.add_argument("phrase_a", help="First phrase")
    p_co.add_argument("phrase_b", help="Second phrase")
    p_co.add_argument("--window", type=float, default=30.0,
                      help="Time window in seconds (default 30)")
    p_co.add_argument("--limit", type=int, default=20, help="Max results (default 20)")
    p_co.add_argument("--json", action="store_true", help="Output as JSON")

    # episode
    p_ep = sub.add_parser("episode", help="Look up episode metadata")
    p_ep.add_argument("video_id", help="YouTube video ID")
    p_ep.add_argument("--fetch", action="store_true",
                      help="Fetch title/date from YouTube via yt-dlp (requires yt-dlp)")
    p_ep.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    if args.command == "build-index":
        cmd_build_index(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "cooccur":
        cmd_cooccur(args)
    elif args.command == "episode":
        cmd_episode(args)


if __name__ == "__main__":
    main()
