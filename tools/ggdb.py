#!/usr/bin/env python3
"""
ggdb.py — Game Grumps Transcript Database CLI

Commands:
  build   Scan transcripts and (re)build the SQLite + FTS5 database.
  search  Full-text search the database with optional filters.

Usage examples:
  python tools/ggdb.py build --root transcripts --db ggtranscripts.db
  python tools/ggdb.py build --root transcripts --db ggtranscripts.db --incremental
  python tools/ggdb.py search "spider kiss" --db ggtranscripts.db --limit 20 --context 2
  python tools/ggdb.py search "banana" --db ggtranscripts.db --series "Goof Troop"
  python tools/ggdb.py search "banana" --db ggtranscripts.db --video-id JgwxuusiH2k
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BATCH_SIZE = 100          # episodes committed per transaction
PROGRESS_EVERY = 50       # print progress every N episodes

# Filename pattern: [VIDEO_ID].txt
_VIDEO_ID_RE = re.compile(r'^\[([^\]]+)\]\.txt$', re.IGNORECASE)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_video_id(filename: str) -> str | None:
    """Extract video_id from a filename like '[fmJNXG_f2SY].txt'."""
    m = _VIDEO_ID_RE.match(filename)
    return m.group(1) if m else None


def parse_series(rel_path: str) -> str:
    """Derive series name from the first path component below the root."""
    parts = Path(rel_path).parts
    # parts[0] is the series folder; parts[1] is the filename
    return parts[0] if len(parts) >= 2 else ""


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def load_transcript(path: Path) -> dict | None:
    """
    Parse a transcript JSON file. Returns a dict with keys:
      video_id, language, language_code, is_generated, snippets
    Returns None if the file cannot be parsed or is not a transcript.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict) or 'snippets' not in data:
            return None
        return data
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return None


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS episodes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id    TEXT    NOT NULL UNIQUE,
    series      TEXT    NOT NULL,
    title       TEXT,
    path        TEXT    NOT NULL,
    bytes       INTEGER NOT NULL,
    sha256      TEXT    NOT NULL,
    ingested_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS episode_lines (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id  INTEGER NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    line_no     INTEGER NOT NULL,
    start       REAL,
    duration    REAL,
    text        TEXT    NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS episode_fts USING fts5(
    video_id,
    series,
    text,
    content='episode_lines',
    content_rowid='id',
    tokenize='unicode61'
);

CREATE TRIGGER IF NOT EXISTS episode_lines_ai
AFTER INSERT ON episode_lines BEGIN
    INSERT INTO episode_fts(rowid, video_id, series, text)
    VALUES (new.id,
            (SELECT video_id FROM episodes WHERE id = new.episode_id),
            (SELECT series   FROM episodes WHERE id = new.episode_id),
            new.text);
END;

CREATE TRIGGER IF NOT EXISTS episode_lines_ad
AFTER DELETE ON episode_lines BEGIN
    INSERT INTO episode_fts(episode_fts, rowid, video_id, series, text)
    VALUES ('delete', old.id,
            (SELECT video_id FROM episodes WHERE id = old.episode_id),
            (SELECT series   FROM episodes WHERE id = old.episode_id),
            old.text);
END;
"""


def open_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    con.executescript(DDL)
    con.commit()
    return con


def rebuild_db(db_path: str) -> sqlite3.Connection:
    """Drop all data and recreate schema from scratch."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys=OFF")
    con.executescript("""
        DROP TABLE IF EXISTS episode_fts;
        DROP TRIGGER IF EXISTS episode_lines_ai;
        DROP TRIGGER IF EXISTS episode_lines_ad;
        DROP TABLE IF EXISTS episode_lines;
        DROP TABLE IF EXISTS episodes;
    """)
    con.execute("PRAGMA foreign_keys=ON")
    con.executescript(DDL)
    con.commit()
    return con


# ---------------------------------------------------------------------------
# Build command
# ---------------------------------------------------------------------------

def cmd_build(args):
    root = Path(args.root)
    db_path = args.db

    if not root.is_dir():
        print(f"ERROR: transcript root '{root}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning transcripts under: {root.resolve()}")

    # Collect all candidate transcript files
    candidates = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for fname in filenames:
            vid = parse_video_id(fname)
            if vid is None:
                continue
            full = Path(dirpath) / fname
            rel = full.relative_to(root)
            candidates.append((full, str(rel), vid))

    total = len(candidates)
    print(f"Found {total} transcript files.")

    if args.incremental and os.path.exists(db_path):
        con = open_db(db_path)
        print("Incremental mode: skipping unchanged files.")
    else:
        con = rebuild_db(db_path)
        print("Full rebuild mode.")

    t0 = time.time()
    ingested = 0
    skipped = 0
    errors = 0

    batch_eps: list[tuple] = []
    # Map video_id → list of (line_no, start, duration, text) for O(1) lookup in flush_batch
    batch_lines: dict[str, list[tuple]] = {}

    def flush_batch():
        nonlocal ingested
        if not batch_eps:
            return
        cur = con.cursor()
        for ep_row in batch_eps:
            vid_key = ep_row[0]
            cur.execute(
                "INSERT OR REPLACE INTO episodes "
                "(video_id, series, title, path, bytes, sha256, ingested_at) "
                "VALUES (?,?,?,?,?,?,?)",
                ep_row
            )
            ep_id = cur.lastrowid
            for line_no, start, duration, text in batch_lines.get(vid_key, []):
                cur.execute(
                    "INSERT INTO episode_lines (episode_id, line_no, start, duration, text) "
                    "VALUES (?,?,?,?,?)",
                    (ep_id, line_no, start, duration, text)
                )
        con.commit()
        ingested += len(batch_eps)
        batch_eps.clear()
        batch_lines.clear()

    sha_cache: dict[str, str] = {}
    if args.incremental:
        rows = con.execute("SELECT video_id, sha256 FROM episodes").fetchall()
        sha_cache = {r[0]: r[1] for r in rows}

    now_str = datetime.now(timezone.utc).isoformat()

    for i, (full_path, rel, vid) in enumerate(candidates, 1):
        try:
            file_bytes = full_path.stat().st_size
            digest = sha256_of_file(full_path)

            if args.incremental and sha_cache.get(vid) == digest:
                skipped += 1
                if i % PROGRESS_EVERY == 0:
                    elapsed = time.time() - t0
                    print(f"  [{i}/{total}] ingested={ingested} skipped={skipped} "
                          f"errors={errors} elapsed={elapsed:.1f}s")
                continue

            # If incremental + file changed, delete old record first.
            # Flush any pending batch before the delete so foreign keys stay consistent.
            if args.incremental and vid in sha_cache:
                flush_batch()
                ep_row = con.execute(
                    "SELECT id FROM episodes WHERE video_id=?", (vid,)
                ).fetchone()
                if ep_row:
                    con.execute("DELETE FROM episode_lines WHERE episode_id=?", (ep_row[0],))
                    con.execute("DELETE FROM episodes WHERE id=?", (ep_row[0],))
                    con.commit()

            data = load_transcript(full_path)
            if data is None:
                errors += 1
                continue

            series = parse_series(rel)
            snippets = data.get('snippets', [])

            batch_eps.append((vid, series, None, rel, file_bytes, digest, now_str))

            for line_no, snippet in enumerate(snippets, 1):
                text = snippet.get('text', '').strip()
                if not text:
                    continue
                start = snippet.get('start')
                duration = snippet.get('duration')
                batch_lines.setdefault(vid, []).append((line_no, start, duration, text))

        except Exception as e:
            print(f"  ERROR processing {full_path}: {e}", file=sys.stderr)
            errors += 1
            continue

        if len(batch_eps) >= BATCH_SIZE:
            flush_batch()

        if i % PROGRESS_EVERY == 0:
            elapsed = time.time() - t0
            print(f"  [{i}/{total}] ingested={ingested + len(batch_eps)} skipped={skipped} "
                  f"errors={errors} elapsed={elapsed:.1f}s")

    flush_batch()

    elapsed = time.time() - t0
    print(f"\nDone. ingested={ingested} skipped={skipped} errors={errors} "
          f"total_time={elapsed:.1f}s")
    print(f"Database: {os.path.abspath(db_path)}")

    ep_count = con.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
    line_count = con.execute("SELECT COUNT(*) FROM episode_lines").fetchone()[0]
    print(f"  episodes={ep_count}  episode_lines={line_count}")
    con.close()


# ---------------------------------------------------------------------------
# Search command
# ---------------------------------------------------------------------------

def _format_timestamp(seconds) -> str:
    if seconds is None:
        return "?"
    secs = int(seconds)
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def cmd_search(args):
    db_path = args.db
    if not os.path.exists(db_path):
        print(f"ERROR: database '{db_path}' not found. Run 'build' first.", file=sys.stderr)
        sys.exit(1)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")

    query = args.query
    limit = args.limit
    context_k = args.context

    # Build FTS query: match rows in episode_lines through episode_fts
    fts_conditions = ["episode_fts MATCH ?"]
    params: list = [query]

    # We'll use a subquery to apply series/video_id filters on the episode level
    extra_where = ""
    extra_params: list = []

    if args.series:
        extra_where += " AND e.series LIKE ?"
        extra_params.append(f"%{args.series}%")

    if args.video_id:
        extra_where += " AND e.video_id = ?"
        extra_params.append(args.video_id)

    sql = f"""
        SELECT
            el.id        AS line_id,
            el.episode_id,
            el.line_no,
            el.start,
            el.duration,
            el.text,
            e.video_id,
            e.series,
            e.path,
            rank
        FROM episode_fts
        JOIN episode_lines el ON el.id = episode_fts.rowid
        JOIN episodes e ON e.id = el.episode_id
        WHERE episode_fts MATCH ?
          {extra_where}
        ORDER BY rank
        LIMIT ?
    """

    all_params = [query] + extra_params + [limit]

    try:
        rows = con.execute(sql, all_params).fetchall()
    except sqlite3.OperationalError as exc:
        print(f"ERROR: search failed: {exc}", file=sys.stderr)
        print("Tip: make sure the query uses valid FTS5 syntax (e.g., quote phrases).",
              file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("No results found.")
        con.close()
        return

    print(f"Found {len(rows)} result(s) for: {query!r}\n")

    for row in rows:
        vid = row['video_id']
        series = row['series']
        path = row['path']
        line_no = row['line_no']
        ep_id = row['episode_id']
        start = row['start']

        ts = _format_timestamp(start)
        print(f"── [{vid}]  {series}")
        print(f"   Path: {path}  |  Line {line_no}  |  ⏱  {ts}")

        if context_k > 0:
            lo = max(1, line_no - context_k)
            hi = line_no + context_k
            ctx_rows = con.execute(
                "SELECT line_no, start, text FROM episode_lines "
                "WHERE episode_id=? AND line_no BETWEEN ? AND ? ORDER BY line_no",
                (ep_id, lo, hi)
            ).fetchall()
            for cr in ctx_rows:
                marker = ">>>" if cr[0] == line_no else "   "
                cts = _format_timestamp(cr[1])
                print(f"   {marker} [{cts}] {cr[2]}")
        else:
            print(f"       {row['text']}")

        print()

    con.close()


# ---------------------------------------------------------------------------
# Argument parsing + main
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ggdb",
        description="Game Grumps Transcript Database — build and search.",
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # --- build ---
    p_build = sub.add_parser('build', help='Build (or rebuild) the transcript database.')
    p_build.add_argument(
        '--root', default='transcripts',
        help='Path to the transcripts root directory (default: transcripts)'
    )
    p_build.add_argument(
        '--db', default='ggtranscripts.db',
        help='Path to the SQLite output database (default: ggtranscripts.db)'
    )
    p_build.add_argument(
        '--incremental', action='store_true',
        help='Only re-ingest files whose sha256 has changed (default: full rebuild)'
    )

    # --- search ---
    p_search = sub.add_parser('search', help='Full-text search the transcript database.')
    p_search.add_argument('query', help='Search query string (FTS5 syntax supported)')
    p_search.add_argument(
        '--db', default='ggtranscripts.db',
        help='Path to the SQLite database (default: ggtranscripts.db)'
    )
    p_search.add_argument(
        '--limit', type=int, default=20,
        help='Maximum number of results (default: 20)'
    )
    p_search.add_argument(
        '--context', type=int, default=2,
        help='Lines of context before/after each match (default: 2)'
    )
    p_search.add_argument(
        '--series',
        help='Filter to episodes whose series name contains this string (case-insensitive)'
    )
    p_search.add_argument(
        '--video-id',
        help='Filter to a specific video ID'
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == 'build':
        cmd_build(args)
    elif args.command == 'search':
        cmd_search(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
