#!/usr/bin/env python3
"""
validate_db.py – Verify that the committed database is consistent with the
transcript files in the repository.

Exit codes:
    0  All checks passed
    1  One or more checks failed

Usage:
    python scripts/validate_db.py [--transcripts-dir DIR] [--db-path PATH]
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path

YOUTUBE_ID_RE = re.compile(r"^\[([A-Za-z0-9_\-]+)\]\.txt$")
SKIP_FILES = {"transcripts.txt", "desktop.ini"}


def discover_transcript_ids(transcripts_dir: Path) -> set:
    ids = set()
    for entry in transcripts_dir.rglob("*.txt"):
        if entry.name in SKIP_FILES:
            continue
        m = YOUTUBE_ID_RE.match(entry.name)
        if m:
            ids.add(m.group(1))
    return ids


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Validate the transcript database against the transcript files."
    )
    parser.add_argument(
        "--transcripts-dir",
        default=str(repo_root / "transcripts"),
        help="Root directory containing transcript series folders (default: <repo>/transcripts)",
    )
    parser.add_argument(
        "--db-path",
        default=str(repo_root / "db" / "transcripts.sqlite"),
        help="Path to the SQLite database (default: <repo>/db/transcripts.sqlite)",
    )
    args = parser.parse_args()

    transcripts_dir = Path(args.transcripts_dir)
    db_path = Path(args.db_path)

    errors: list[str] = []

    # ── Check 1: DB file exists ───────────────────────────────────────────────
    print(f"[1/4] Checking database exists at {db_path} … ", end="")
    if not db_path.exists():
        print("FAIL")
        errors.append(f"Database file not found: {db_path}")
    else:
        print("OK")

    if errors:
        for e in errors:
            print(f"  ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # ── Check 2: DB is a valid SQLite file ────────────────────────────────────
    print("[2/4] Checking database is readable … ", end="")
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("SELECT COUNT(*) FROM episodes").fetchone()
        print("OK")
    except sqlite3.DatabaseError as exc:
        print("FAIL")
        errors.append(f"Cannot read database: {exc}")
        for e in errors:
            print(f"  ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # ── Check 3: Episode count matches transcript files ───────────────────────
    print("[3/4] Comparing episode count with transcript files … ", end="")
    file_ids = discover_transcript_ids(transcripts_dir)
    db_count = conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
    file_count = len(file_ids)

    if db_count != file_count:
        print("FAIL")
        errors.append(
            f"Episode count mismatch: database has {db_count:,} episodes, "
            f"but {file_count:,} transcript files were found."
        )
    else:
        print(f"OK  ({db_count:,} episodes)")

    # ── Check 4: Spot-check – every DB youtube_id exists as a file ───────────
    print("[4/4] Spot-checking YouTube IDs in database … ", end="")
    db_ids = {
        row[0]
        for row in conn.execute("SELECT youtube_id FROM episodes").fetchall()
    }
    missing_from_files = db_ids - file_ids
    missing_from_db = file_ids - db_ids

    if missing_from_files or missing_from_db:
        print("FAIL")
        if missing_from_files:
            sample = list(missing_from_files)[:5]
            errors.append(
                f"{len(missing_from_files)} IDs in DB have no matching file "
                f"(sample: {sample})"
            )
        if missing_from_db:
            sample = list(missing_from_db)[:5]
            errors.append(
                f"{len(missing_from_db)} transcript files have no DB entry "
                f"(sample: {sample})"
            )
    else:
        print("OK")

    conn.close()

    if errors:
        print("\nValidation FAILED:")
        for e in errors:
            print(f"  ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    else:
        print("\nAll checks passed ✓")


if __name__ == "__main__":
    main()
