#!/usr/bin/env python3
"""
gg.py — Game Grumps Transcript Search CLI

Commands:
  build-index   Scan transcripts/ and build (or rebuild) data/gg_index.sqlite
  ask           Ask a plain-English question and get a human-readable answer
  context N     Show the surrounding conversation for result #N from the last ask
  detail N      Show full episode info for result #N from the last ask
  search        Full-text search (programmatic, tabular output)
  cooccur       Find episodes where two phrases appear within a time window
  episode       Look up metadata for a video_id (optionally fetching YouTube title)

Usage:
  python tools/gg.py build-index [--transcripts-dir TRANSCRIPTS_DIR] [--db DB_PATH] [--dry-run]
  python tools/gg.py ask "Are there any mentions of banana?"
  python tools/gg.py context 2
  python tools/gg.py detail 2
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
LAST_RESULTS_PATH = REPO_ROOT / "data" / ".last_results.json"

# ---------------------------------------------------------------------------
# Natural-language question parsing
# ---------------------------------------------------------------------------
# Words that signal a natural-language question (not a raw FTS query)
_QUESTION_STARTERS = frozenset({
    "are", "did", "do", "does", "has", "have", "had", "is", "was", "were",
    "will", "would", "could", "should", "can", "what", "when", "where",
    "who", "which", "why", "how",
})

# Common English words + domain words that carry no search meaning
_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "if", "so", "yet", "both",
    "either", "neither", "not", "no", "nor", "such", "than", "too", "very",
    "just", "while", "all", "each", "every", "few", "more", "most", "other",
    "same", "up", "down", "out", "over", "under", "again", "once", "here",
    "there", "then", "further", "ever", "any", "some", "into", "through",
    "during", "about", "with", "from", "for", "by", "at", "on", "in", "of",
    "to", "it", "its", "this", "that", "these", "those", "i", "me", "my",
    "we", "our", "you", "your", "he", "him", "his", "she", "her", "they",
    "them", "their", "be", "been", "being", "have", "has", "had", "do",
    "does", "did", "will", "would", "could", "should", "may", "might",
    "shall", "can",
    # domain-specific filler words
    "transcripts", "transcript", "episode", "episodes",
    "mention", "mentions", "mentioned", "say", "said", "says",
    "talk", "talks", "talked", "discuss", "discussed",
    "game", "grumps", "times", "time",
})


def _parse_question(question: str) -> tuple[str, bool]:
    """
    Convert a natural-language question to an FTS5 query.

    Returns (fts_query, was_natural_language).
    If the input starts with a question word, extracts meaningful terms and
    builds an OR query with prefix matching so that e.g. "banana" also
    matches "bananas".
    If it does NOT start with a question word, it is returned unchanged so
    power users can write raw FTS5 syntax directly.
    """
    words = question.strip().split()
    if not words:
        return question, False

    first = re.sub(r"[^a-z]", "", words[0].lower())
    if first not in _QUESTION_STARTERS:
        return question, False

    terms: list[str] = []
    for word in words:
        clean = re.sub(r"[^a-z0-9]", "", word.lower())
        if clean and clean not in _STOP_WORDS and len(clean) > 1:
            terms.append(clean)

    if not terms:
        return question, True

    # Remove near-duplicates: if "bananas" is present and "banana" is also
    # present, drop "bananas" because "banana*" already covers it.
    deduped: list[str] = []
    for term in terms:
        if not any(term != other and term.startswith(other) for other in terms):
            if term not in deduped:
                deduped.append(term)

    return " OR ".join(f"{t}*" for t in deduped), True

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


def _fmt_ts(seconds: float) -> str:
    """Format seconds as M:SS (or H:MM:SS for long videos)."""
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m}:{sec:02d}"


def _yt_url(video_id: str, start: float | None = None) -> str:
    base = f"https://www.youtube.com/watch?v={video_id}"
    if start is not None:
        return f"{base}&t={int(start)}"
    return base


def _save_last_results(query: str, human_query: str, results: list[dict]) -> None:
    LAST_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "query": query,
        "human_query": human_query,
        "total_shown": len(results),
        "results": results,
    }
    with open(LAST_RESULTS_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def _load_last_results() -> dict | None:
    if not LAST_RESULTS_PATH.exists():
        return None
    try:
        with open(LAST_RESULTS_PATH, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def _get_result(n: int) -> dict | None:
    """Return the Nth result (1-based) from the last ask session."""
    data = _load_last_results()
    if data is None:
        return None
    results = data.get("results", [])
    if n < 1 or n > len(results):
        return None
    return results[n - 1]


# ---------------------------------------------------------------------------
# ask  (human-friendly search)
# ---------------------------------------------------------------------------
def cmd_ask(args: argparse.Namespace) -> None:
    db_path = Path(args.db)
    _require_db(db_path)

    human_query: str = args.question
    limit: int = args.limit

    fts_query, was_question = _parse_question(human_query)

    conn = get_db(db_path)

    # Fetch a generous pool so we can count unique episodes accurately,
    # capped to avoid fetching far more rows than needed.
    POOL = min(max(limit * 3, 100), 3000)
    sql = """
        SELECT
            s.id,
            s.video_id,
            s.series,
            s.start,
            s.text
        FROM snippets_fts
        JOIN snippets s ON snippets_fts.rowid = s.id
        WHERE snippets_fts MATCH ?
        ORDER BY rank
        LIMIT ?
    """
    try:
        pool_rows = conn.execute(sql, [fts_query, POOL]).fetchall()
    except sqlite3.OperationalError as exc:
        print(f"Search error: {exc}")
        print("Tip: wrap a phrase in double quotes, e.g.:  ask '\"kiss your dad\"'")
        conn.close()
        return

    conn.close()

    if not pool_rows:
        print(f'No results found for "{human_query}".')
        if was_question:
            print(f"  (searched for: {fts_query})")
        return

    total_snippets = len(pool_rows)
    unique_episodes = len({r["video_id"] for r in pool_rows})

    # Build top-N display rows (one per snippet, up to limit)
    display = pool_rows[:limit]

    # Persist all display rows for context/detail follow-up
    saved = [
        {
            "num": i + 1,
            "video_id": r["video_id"],
            "series": r["series"],
            "start": r["start"],
            "text": r["text"],
        }
        for i, r in enumerate(display)
    ]
    _save_last_results(fts_query, human_query, saved)

    # Human-readable summary line
    ep_word = "episode" if unique_episodes == 1 else "episodes"
    snip_word = "mention" if total_snippets == 1 else "mentions"
    term_display = f'"{human_query}"' if not was_question else f'"{fts_query}"'
    if was_question:
        intro = (
            f'Yes — found {total_snippets} {snip_word} across {unique_episodes} {ep_word}.'
        )
    else:
        intro = (
            f'Found {total_snippets} {snip_word} across {unique_episodes} {ep_word}.'
        )

    showing = min(limit, len(display))
    print(intro)
    if total_snippets > showing:
        print(f"Showing the top {showing} (of {total_snippets} total):\n")
    else:
        print()

    for i, row in enumerate(display, 1):
        ts = _fmt_ts(row["start"])
        url = _yt_url(row["video_id"], row["start"])
        print(f"  #{i:<3} {row['series']}")
        print(f"       \"{row['text']}\"")
        print(f"       ↳ at {ts}  |  {url}")
        print()

    print("To dig deeper into any result:")
    print("  python tools/gg.py context N    — show the surrounding conversation")
    print("  python tools/gg.py detail N     — full episode info")


# ---------------------------------------------------------------------------
# context N
# ---------------------------------------------------------------------------
def cmd_context(args: argparse.Namespace) -> None:
    n: int = args.number
    window: float = args.window

    result = _get_result(n)
    if result is None:
        session = _load_last_results()
        if session is None:
            print("No previous search found. Run 'ask' first.")
        else:
            total = len(session.get("results", []))
            print(f"Result #{n} not found. Last search returned {total} results.")
        return

    video_id = result["video_id"]
    series = result["series"]
    target_start: float = result["start"]
    matching_text: str = result["text"]

    # Load the transcript file to get surrounding snippets
    db_path = Path(args.db)
    conn = get_db(db_path)
    file_path_row = conn.execute(
        "SELECT file_path FROM episodes WHERE video_id = ?", [video_id]
    ).fetchone()
    conn.close()

    print(f"Context for result #{n}")
    print(f"Episode : {series}  (video: {video_id})")
    print(f"Showing conversation around {_fmt_ts(target_start)}:\n")

    if file_path_row:
        transcript_path = REPO_ROOT / file_path_row["file_path"]
        data = read_transcript(transcript_path)
        if data and data.get("snippets"):
            snippets = data["snippets"]
            nearby = [
                s for s in snippets
                if abs(s.get("start", 0) - target_start) <= window
            ]
            nearby.sort(key=lambda s: s.get("start", 0))

            for snip in nearby:
                ts = _fmt_ts(snip.get("start", 0))
                text = snip.get("text", "").strip()
                if abs(snip.get("start", 0) - target_start) < 0.5:
                    print(f"  [{ts}]  ▶ \"{text}\"     ← your result")
                else:
                    print(f"  [{ts}]    \"{text}\"")
            print()
        else:
            print(f"  (Could not load transcript file: {transcript_path})")
            print()
    else:
        print(f"  (Episode record not found in database for video_id: {video_id})")
        print()

    print(f"Watch full episode : {_yt_url(video_id)}")
    print(f"Jump to this moment: {_yt_url(video_id, target_start)}")


# ---------------------------------------------------------------------------
# detail N
# ---------------------------------------------------------------------------
def cmd_detail(args: argparse.Namespace) -> None:
    n: int = args.number

    result = _get_result(n)
    if result is None:
        session = _load_last_results()
        if session is None:
            print("No previous search found. Run 'ask' first.")
        else:
            total = len(session.get("results", []))
            print(f"Result #{n} not found. Last search returned {total} results.")
        return

    video_id = result["video_id"]
    series = result["series"]
    start: float = result["start"]
    text: str = result["text"]

    db_path = Path(args.db)
    conn = get_db(db_path)
    ep_row = conn.execute(
        "SELECT * FROM episodes WHERE video_id = ?", [video_id]
    ).fetchone()
    conn.close()

    print(f"Result #{n} from your last search:\n")
    print(f"  Series   : {series}")
    print(f"  Video ID : {video_id}")
    if ep_row:
        print(f"  Snippets : {ep_row['snippet_count']} (total lines in transcript)")
    print(f"  Watch    : {_yt_url(video_id)}")
    print()
    print(f"The matching line (at {_fmt_ts(start)}):")
    print(f"  \"{text}\"")
    print()
    print(f"Jump directly to this moment:")
    print(f"  {_yt_url(video_id, start)}")


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
              python tools/gg.py ask "Are there any mentions of banana?"
              python tools/gg.py ask "Did they ever play Bloodborne?"
              python tools/gg.py context 2
              python tools/gg.py detail 2
              python tools/gg.py search "kiss your dad" --limit 20
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

    # ask  (human-friendly)
    p_ask = sub.add_parser(
        "ask",
        help="Ask a plain-English question and get a human-readable answer",
    )
    p_ask.add_argument("question", help='Your question, e.g. "Are there any mentions of banana?"')
    p_ask.add_argument("--limit", type=int, default=20, help="Max results to show (default 20)")

    # context N
    p_ctx = sub.add_parser(
        "context",
        help="Show the surrounding conversation for result #N from the last ask",
    )
    p_ctx.add_argument("number", type=int, help="Result number from the last ask")
    p_ctx.add_argument(
        "--window", type=float, default=30.0,
        help="Seconds of context before and after the match (default 30)",
    )

    # detail N
    p_det = sub.add_parser(
        "detail",
        help="Show full episode info for result #N from the last ask",
    )
    p_det.add_argument("number", type=int, help="Result number from the last ask")

    # search (programmatic / tabular)
    p_search = sub.add_parser("search", help="Full-text search (tabular output)")
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
    p_ep = sub.add_parser("episode", help="Look up episode metadata by video_id")
    p_ep.add_argument("video_id", help="YouTube video ID")
    p_ep.add_argument("--fetch", action="store_true",
                      help="Fetch title/date from YouTube via yt-dlp (requires yt-dlp)")
    p_ep.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    if args.command == "build-index":
        cmd_build_index(args)
    elif args.command == "ask":
        cmd_ask(args)
    elif args.command == "context":
        cmd_context(args)
    elif args.command == "detail":
        cmd_detail(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "cooccur":
        cmd_cooccur(args)
    elif args.command == "episode":
        cmd_episode(args)


if __name__ == "__main__":
    main()
