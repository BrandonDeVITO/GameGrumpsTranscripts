#!/usr/bin/env python3
"""
build_db.py – Build a searchable SQLite + FTS5 database from Game Grumps transcripts.

Usage:
    python scripts/build_db.py [--transcripts-dir TRANSCRIPTS_DIR] [--db-path DB_PATH]

Defaults:
    --transcripts-dir  transcripts/   (relative to repo root)
    --db-path          db/transcripts.sqlite
"""

import argparse
import json
import os
import re
import sqlite3
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

# ── File-name pattern: [YouTubeId].txt ───────────────────────────────────────
YOUTUBE_ID_RE = re.compile(r"^\[([A-Za-z0-9_\-]+)\]\.txt$")

# Non-episode files to skip (by filename)
SKIP_FILES = {"transcripts.txt", "desktop.ini"}


def normalize_text(text: str) -> str:
    """Return a lower-cased, accent-stripped, whitespace-normalised version of *text*."""
    # Unicode normalisation → strip combining characters (accents)
    nfkd = unicodedata.normalize("NFKD", text)
    stripped = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", stripped).strip().lower()


def discover_transcripts(transcripts_dir: Path):
    """Yield (series, youtube_id, path) for every valid transcript file."""
    for entry in sorted(transcripts_dir.rglob("*.txt")):
        if entry.name in SKIP_FILES:
            continue
        m = YOUTUBE_ID_RE.match(entry.name)
        if not m:
            continue
        youtube_id = m.group(1)
        # series = the immediate parent folder name
        series = entry.parent.name
        yield series, youtube_id, entry


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS episodes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            series      TEXT    NOT NULL,
            youtube_id  TEXT    NOT NULL UNIQUE,
            relative_path TEXT  NOT NULL,
            language    TEXT,
            is_generated INTEGER,
            imported_at TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_episodes_series
            ON episodes (series);
        CREATE INDEX IF NOT EXISTS idx_episodes_youtube_id
            ON episodes (youtube_id);

        CREATE TABLE IF NOT EXISTS snippets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            episode_id  INTEGER NOT NULL REFERENCES episodes(id),
            start       REAL    NOT NULL,
            duration    REAL    NOT NULL,
            text        TEXT    NOT NULL,
            normalized  TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_snippets_episode_id
            ON snippets (episode_id);

        -- FTS5 virtual table for full-text search over snippets.
        -- content=snippets keeps text in snippets table (no duplication).
        -- We add an episode_id column for filtering by series.
        CREATE VIRTUAL TABLE IF NOT EXISTS snippets_fts USING fts5 (
            text,
            normalized,
            content = 'snippets',
            content_rowid = 'id'
        );

        -- Triggers to keep FTS index in sync with snippets table.
        CREATE TRIGGER IF NOT EXISTS snippets_ai AFTER INSERT ON snippets BEGIN
            INSERT INTO snippets_fts (rowid, text, normalized)
            VALUES (new.id, new.text, new.normalized);
        END;

        CREATE TRIGGER IF NOT EXISTS snippets_ad AFTER DELETE ON snippets BEGIN
            INSERT INTO snippets_fts (snippets_fts, rowid, text, normalized)
            VALUES ('delete', old.id, old.text, old.normalized);
        END;

        CREATE TRIGGER IF NOT EXISTS snippets_au AFTER UPDATE ON snippets BEGIN
            INSERT INTO snippets_fts (snippets_fts, rowid, text, normalized)
            VALUES ('delete', old.id, old.text, old.normalized);
            INSERT INTO snippets_fts (rowid, text, normalized)
            VALUES (new.id, new.text, new.normalized);
        END;
        """
    )


def insert_episode(
    conn: sqlite3.Connection,
    series: str,
    youtube_id: str,
    relative_path: str,
    language: str | None,
    is_generated: bool | None,
    imported_at: str,
) -> int:
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO episodes
            (series, youtube_id, relative_path, language, is_generated, imported_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (series, youtube_id, relative_path, language, int(is_generated) if is_generated is not None else None, imported_at),
    )
    if cur.lastrowid:
        return cur.lastrowid
    # Already exists (duplicate youtube_id in another series folder) – return existing id.
    row = conn.execute("SELECT id FROM episodes WHERE youtube_id = ?", (youtube_id,)).fetchone()
    return row[0] if row else None


def build_database(transcripts_dir: Path, db_path: Path, verbose: bool = True) -> None:
    transcripts_dir = transcripts_dir.resolve()
    repo_root = transcripts_dir.parent

    if not transcripts_dir.is_dir():
        raise FileNotFoundError(f"Transcripts directory not found: {transcripts_dir}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing DB (and any WAL/SHM journal files) for a clean rebuild.
    for suffix in ("", "-wal", "-shm"):
        stale = Path(str(db_path) + suffix) if suffix else db_path
        if stale.exists():
            stale.unlink()
    if verbose:
        print(f"Building fresh database at: {db_path}")

    conn = sqlite3.connect(str(db_path))
    # Performance tuning
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")  # 64 MB cache
    conn.execute("PRAGMA temp_store = MEMORY")

    create_schema(conn)

    imported_at = datetime.now(timezone.utc).isoformat()
    files = list(discover_transcripts(transcripts_dir))
    total = len(files)

    if verbose:
        print(f"Found {total} transcript files. Building database …")

    episodes_inserted = 0
    snippets_inserted = 0

    for i, (series, youtube_id, path) in enumerate(files, 1):
        relative_path = str(path.relative_to(repo_root))

        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            print(f"  WARNING: skipping {path}: {exc}")
            continue

        language = data.get("language")
        is_generated = data.get("is_generated")
        raw_snippets = data.get("snippets", [])

        with conn:
            ep_id = insert_episode(
                conn,
                series,
                youtube_id,
                relative_path,
                language,
                is_generated,
                imported_at,
            )

            # ep_id is None when this youtube_id was already inserted from another
            # series folder (duplicate/compilation).  Skip re-inserting snippets.
            if ep_id is None:
                continue

            conn.executemany(
                "INSERT INTO snippets (episode_id, start, duration, text, normalized) VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        ep_id,
                        float(s.get("start", 0)),
                        float(s.get("duration", 0)),
                        s.get("text", ""),
                        normalize_text(s.get("text", "")),
                    )
                    for s in raw_snippets
                    if s.get("text", "").strip()
                ],
            )

            episodes_inserted += 1
            snippets_inserted += len(raw_snippets)

        if verbose and (i % 500 == 0 or i == total):
            print(f"  {i}/{total} files processed …")

    # Optimise the FTS index.
    if verbose:
        print("Optimising FTS index …")
    conn.execute("INSERT INTO snippets_fts(snippets_fts) VALUES ('optimize')")
    conn.commit()
    conn.close()

    db_size_mb = db_path.stat().st_size / 1_048_576
    if verbose:
        print(
            f"\nDone.\n"
            f"  Episodes : {episodes_inserted:,}\n"
            f"  Snippets : {snippets_inserted:,}\n"
            f"  DB size  : {db_size_mb:.1f} MB  ({db_path})"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a SQLite + FTS5 database from Game Grumps transcript files."
    )
    repo_root = Path(__file__).resolve().parent.parent
    parser.add_argument(
        "--transcripts-dir",
        default=str(repo_root / "transcripts"),
        help="Root directory containing the transcript series folders (default: <repo>/transcripts)",
    )
    parser.add_argument(
        "--db-path",
        default=str(repo_root / "db" / "transcripts.sqlite"),
        help="Output path for the SQLite database (default: <repo>/db/transcripts.sqlite)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output",
    )
    args = parser.parse_args()

    build_database(
        transcripts_dir=Path(args.transcripts_dir),
        db_path=Path(args.db_path),
        verbose=not args.quiet,
    )


if __name__ == "__main__":
    main()
