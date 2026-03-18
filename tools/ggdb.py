#!/usr/bin/env python3
"""
ggdb.py — Game Grumps Transcript Database CLI

Commands:
  build   Scan transcripts and (re)build the SQLite + FTS5 database.
  search  Full-text search the database with optional filters.
  ask     Interactive plain-English REPL for querying the transcripts.

Usage examples:
  python tools/ggdb.py build --root transcripts --db ggtranscripts.db
  python tools/ggdb.py build --root transcripts --db ggtranscripts.db --incremental
  python tools/ggdb.py search "spider kiss" --db ggtranscripts.db --limit 20 --context 2
  python tools/ggdb.py search "banana" --db ggtranscripts.db --series "Goof Troop"
  python tools/ggdb.py ask --db ggtranscripts.db
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
# Plain-English ("ask") helpers
# ---------------------------------------------------------------------------

# Noise words/phrases that indicate a question structure rather than search terms.
# We strip these so the user's actual subject remains.
_NOISE_RE = re.compile(
    r'\b(are\s+there|is\s+there|were\s+there|did\s+they|do\s+they|does\s+any(one|body)?|'
    r'ever|any|all|every|each|find|search(\s+for)?|look(\s+up|\s+for)?|'
    r'show\s+me|tell\s+me\s+(about|more\s+about)?|give\s+me|'
    r'what\s+(are\s+)?all\s+the|how\s+many\s+times|how\s+often|'
    r'can\s+you\s+find|please|'
    r'mentions?\s+of|occurrences?\s+of|instances?\s+of|uses?\s+of|'
    r'times?\s+(they\s+)?(said|say|talked?\s+about|mention(ed)?|brought\s+up)|'
    r'when(\s+do|\s+did|\s+does)?\s+(they\s+)?(say|mention|talk\s+about|bring\s+up)|'
    r'in\s+the\s+transcripts?|across\s+(all\s+)?episodes?)\b',
    re.IGNORECASE,
)

# Extra filler words to strip after the main pass
_FILLER_RE = re.compile(
    r'\b(the|a|an|about|of|or|and|in|on|at|to|for|with|that|this|those|these|'
    r'they|their|it|its|is|was|has|have|had|be|been|being|'
    r'would|could|should|did|do|does|get|got|'
    r'transcripts?|episodes?|videos?|series|game\s+grumps)\b',
    re.IGNORECASE,
)

# Patterns that directly name the search subject
_SUBJECT_PATTERNS = [
    # "mentions of X", "mention of X", "any X", etc. — capture X
    re.compile(
        r'\b(?:mentions?\s+of|any\s+mention\s+of|talk(?:ing|ed|s)?\s+about|'
        r'say(?:ing|s)?\s+|said\s+|about\s+|for\s+|regarding\s+)'
        r'["\']?(.+?)["\']?\s*(?:\?|$)',
        re.IGNORECASE,
    ),
    # "find X" / "search for X" / "look for X"
    re.compile(
        r'\b(?:find|search\s+for|look\s+for|look\s+up|show\s+me|give\s+me)\s+'
        r'["\']?(.+?)["\']?\s*(?:\?|$)',
        re.IGNORECASE,
    ),
    # quoted term anywhere: "spider kiss"
    re.compile(r'["\']([^"\']+)["\']'),
]


def extract_search_terms(question: str) -> str:
    """
    Pull the key search term(s) out of a plain-English question.

    Returns a string suitable for SQLite FTS5 (may contain OR, quotes, etc.).
    Returns an empty string if nothing useful can be extracted.

    Examples:
      "Are there any mentions of banana or bananas?" -> "banana OR bananas"
      'Find every "spider kiss"' -> '"spider kiss"'
      "Did they ever talk about Bloodborne?" -> "Bloodborne"
      "banana" -> "banana"
    """
    q = question.strip()

    # 1. If the user typed a bare quoted phrase, use it directly.
    if q.startswith(('"', "'")) and q.endswith(('"', "'")):
        return q

    # 2. Try the subject-extraction patterns.
    for pat in _SUBJECT_PATTERNS:
        m = pat.search(q)
        if m:
            raw = m.group(1).strip().rstrip('?').strip()
            # Handle "X or Y" → "X OR Y" for FTS
            raw = re.sub(r'\s+or\s+', ' OR ', raw, flags=re.IGNORECASE)
            if len(raw) >= 2:
                return raw

    # 3. Strip structural noise words and filler; use whatever remains.
    stripped = _NOISE_RE.sub(' ', q)
    stripped = _FILLER_RE.sub(' ', stripped)
    stripped = re.sub(r'[?!.,;:]', ' ', stripped)
    stripped = re.sub(r'\s+', ' ', stripped).strip()

    # Handle "X or Y" → FTS OR
    stripped = re.sub(r'\s+or\s+', ' OR ', stripped, flags=re.IGNORECASE)

    return stripped if len(stripped) >= 2 else ''


# Patterns that indicate the user is asking about a specific numbered result.
_FOLLOWUP_RE = re.compile(
    r'(?:'
    r'(?:what\s+(?:was\s+the\s+)?(?:context|episode|series|game|video)\s+(?:of|was|for)\s+)?'
    r'(?:result\s+|#\s*|number\s+)'
    r'(\d+)'
    r'|'
    r'#\s*(\d+)'
    r'|'
    r'(?:context|more|details?|info)\s+(?:of|for|about|on)?\s+#?\s*(\d+)'
    r'|'
    r'(?:tell\s+me\s+more\s+about|expand\s+on|more\s+about)\s+#?\s*(\d+)'
    r'|'
    r'(?:what\s+episode|what\s+series|which\s+episode|which\s+series)\s+(?:is|was)\s+#?\s*(\d+)'
    r')',
    re.IGNORECASE,
)

# Separate pattern to determine if the follow-up is asking for episode info vs. context
_EPISODE_FOLLOWUP_RE = re.compile(
    r'\b(?:episode|series|game|video|watch|youtube|link|url|which\s+(?:episode|series|game))\b',
    re.IGNORECASE,
)


def parse_followup(question: str) -> dict | None:
    """
    Detect whether the question is a follow-up about a specific numbered result.

    Returns {'num': int, 'type': 'context' | 'episode'} or None.
    """
    m = _FOLLOWUP_RE.search(question.strip())
    if not m:
        return None
    # One of the capture groups matched
    num_str = next(g for g in m.groups() if g is not None)
    num = int(num_str)
    kind = 'episode' if _EPISODE_FOLLOWUP_RE.search(question) else 'context'
    return {'num': num, 'type': kind}


def _yt_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _fetch_context_lines(con: sqlite3.Connection, ep_id: int, line_no: int,
                         k: int = 5) -> list:
    """Return k lines before and after line_no for ep_id, ordered by line_no."""
    lo = max(1, line_no - k)
    hi = line_no + k
    return con.execute(
        "SELECT line_no, start, text FROM episode_lines "
        "WHERE episode_id=? AND line_no BETWEEN ? AND ? ORDER BY line_no",
        (ep_id, lo, hi),
    ).fetchall()


def _print_context(con: sqlite3.Connection, result: dict, k: int = 5) -> None:
    """Print a context window around a result's matched line."""
    ctx = _fetch_context_lines(con, result['ep_id'], result['line_no'], k)
    print(f"\n  Context for result #{result['num']} "
          f"— {result['series']} (⏱ {_format_timestamp(result['start'])}):\n")
    for row in ctx:
        marker = ">>>" if row[0] == result['line_no'] else "   "
        print(f"  {marker}  [{_format_timestamp(row[1])}]  {row[2]}")
    print()


def _print_episode_info(result: dict) -> None:
    """Print episode metadata for a result."""
    print(f"\n  Result #{result['num']} — Episode details:\n")
    print(f"    Series  : {result['series']}")
    print(f"    Video ID: {result['video_id']}")
    print(f"    YouTube : {_yt_url(result['video_id'])}")
    print(f"    Match at: {_format_timestamp(result['start'])}"
          f"  (line {result['line_no']})")
    print(f"    Text    : \"{result['text']}\"")
    print()


def run_human_search(con: sqlite3.Connection, terms: str,
                     limit: int = 10) -> list[dict]:
    """
    Run a FTS5 search and return a list of result dicts ready for human display.
    Each dict has: num, video_id, series, line_no, ep_id, start, text, path.
    """
    sql = """
        SELECT
            el.id        AS line_id,
            el.episode_id AS ep_id,
            el.line_no,
            el.start,
            el.text,
            e.video_id,
            e.series,
            e.path,
            rank
        FROM episode_fts
        JOIN episode_lines el ON el.id = episode_fts.rowid
        JOIN episodes e ON e.id = el.episode_id
        WHERE episode_fts MATCH ?
        ORDER BY rank
        LIMIT ?
    """
    try:
        rows = con.execute(sql, [terms, limit]).fetchall()
    except sqlite3.OperationalError:
        return []

    results = []
    for i, row in enumerate(rows, 1):
        results.append({
            'num':      i,
            'video_id': row['video_id'],
            'series':   row['series'],
            'line_no':  row['line_no'],
            'ep_id':    row['ep_id'],
            'start':    row['start'],
            'text':     row['text'],
            'path':     row['path'],
        })
    return results


def print_human_results(results: list[dict], terms: str) -> None:
    """Print a warm, numbered, human-readable result list."""
    n = len(results)
    # Count distinct episodes
    unique_eps = len({r['video_id'] for r in results})

    term_display = f'"{terms}"'
    if n == 0:
        print(f"\nHmm, I couldn't find any mentions of {term_display} "
              f"in the transcripts.\n")
        return

    if n == 1:
        print(f"\nYes! I found 1 mention of {term_display} in the transcripts.\n")
    else:
        ep_word = "episode" if unique_eps == 1 else "episodes"
        print(f"\nYes! I found {n} mention(s) of {term_display} "
              f"across {unique_eps} different {ep_word}.\n")

    for r in results:
        ts = _format_timestamp(r['start'])
        print(f"  #{r['num']:>2}  {r['series']}")
        print(f"        Video: {r['video_id']}  |  YouTube: {_yt_url(r['video_id'])}")
        print(f"        At {ts}:  \"{r['text']}\"")
        print()

    if n >= 2:
        print("  Ask me things like:")
        print("    \"What was the context of #2?\"")
        print("    \"What episode was #3?\"")
        print()


def handle_followup(followup: dict, last_results: list[dict],
                    con: sqlite3.Connection) -> None:
    """Respond to a numbered follow-up question about a previous result."""
    num = followup['num']
    kind = followup['type']

    result = next((r for r in last_results if r['num'] == num), None)
    if result is None:
        hi = max(r['num'] for r in last_results)
        print(f"\n  I don't have a result #{num}. "
              f"The last search returned results #1–#{hi}.\n")
        return

    if kind == 'episode':
        _print_episode_info(result)
    else:
        _print_context(con, result, k=5)


# ---------------------------------------------------------------------------
# Ask (interactive REPL) command
# ---------------------------------------------------------------------------

_HELP_TEXT = """
  Game Grumps Transcript Assistant — tips:
    • Ask naturally: "Are there any mentions of banana?"
    • Phrase search:  "Did they ever say 'spider kiss'?"
    • Follow-up:      "What was the context of #2?"
    • Episode detail: "What episode was #3?"
    • Narrow search:  "Find banana in Goof Troop" (series filter)
    • Exit: type  quit  or  exit
"""


def _parse_series_filter(question: str) -> tuple[str, str | None]:
    """
    If the question contains "in <Series Name>" or "from <Series Name>",
    strip it out and return (cleaned_question, series_name).
    Otherwise return (question, None).
    """
    m = re.search(
        r'\s+(?:in|from|for)\s+([A-Z][^?!.]+?)(?:\?|$)',
        question,
        re.IGNORECASE,
    )
    if m:
        series = m.group(1).strip()
        cleaned = question[:m.start()] + question[m.end():]
        return cleaned.strip(), series
    return question, None


def cmd_ask(args):
    """Interactive plain-English REPL for the transcript database."""
    db_path = args.db
    if not os.path.exists(db_path):
        print(
            f"\nThe database file '{db_path}' doesn't exist yet.\n"
            f"Build it first with:\n\n"
            f"  python tools/ggdb.py build --root transcripts --db {db_path}\n"
        )
        sys.exit(1)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")

    ep_count = con.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
    line_count = con.execute("SELECT COUNT(*) FROM episode_lines").fetchone()[0]

    print("\n" + "=" * 60)
    print("  Game Grumps Transcript Assistant")
    print("=" * 60)
    print(f"  Loaded: {ep_count:,} episodes  |  {line_count:,} transcript lines")
    print(_HELP_TEXT)

    last_results: list[dict] = []
    last_terms: str = ''

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question:
            continue

        if question.lower() in ('quit', 'exit', 'bye', 'q'):
            print("Goodbye!")
            break

        if question.lower() in ('help', '?', 'h'):
            print(_HELP_TEXT)
            continue

        # --- Follow-up about a previous numbered result? ---
        followup = parse_followup(question)
        if followup and last_results:
            handle_followup(followup, last_results, con)
            continue

        if followup and not last_results:
            print("\n  I don't have any previous results to refer to."
                  " Ask a search question first!\n")
            continue

        # --- Check for optional "in <Series>" filter ---
        cleaned_q, series_filter = _parse_series_filter(question)

        # --- Extract search terms ---
        terms = extract_search_terms(cleaned_q)
        if not terms:
            print(
                "\n  I'm not sure what to search for. Try something like:\n"
                "    \"Are there any mentions of banana?\"\n"
                "    \"Find every time they say spider kiss\"\n"
            )
            continue

        # If a series filter was found, append it to the FTS query as a column filter
        fts_terms = terms
        extra_where = ''
        # sql_params always starts with [fts_terms] as the MATCH parameter
        sql_params: list = [fts_terms]

        if series_filter:
            extra_where = ' AND e.series LIKE ?'
            sql_params.append(f'%{series_filter}%')
            print(f"\n  Searching for {terms!r} in series matching '{series_filter}'...")
        else:
            print(f"\n  Searching for {terms!r}...")

        # Run search (filtered or plain)
        if extra_where:
            sql = f"""
                SELECT el.id AS line_id, el.episode_id AS ep_id,
                       el.line_no, el.start, el.text,
                       e.video_id, e.series, e.path, rank
                FROM episode_fts
                JOIN episode_lines el ON el.id = episode_fts.rowid
                JOIN episodes e ON e.id = el.episode_id
                WHERE episode_fts MATCH ?
                  {extra_where}
                ORDER BY rank
                LIMIT ?
            """
            try:
                rows = con.execute(sql, sql_params + [args.limit]).fetchall()
            except sqlite3.OperationalError as exc:
                print(f"\n  Search error: {exc}\n"
                      "  Tip: put multi-word phrases in quotes, e.g. \"spider kiss\"\n")
                continue
            results = [
                {
                    'num': i + 1,
                    'video_id': row['video_id'],
                    'series':   row['series'],
                    'line_no':  row['line_no'],
                    'ep_id':    row['ep_id'],
                    'start':    row['start'],
                    'text':     row['text'],
                    'path':     row['path'],
                }
                for i, row in enumerate(rows)
            ]
        else:
            results = run_human_search(con, fts_terms, limit=args.limit)

        if results:
            last_results = results
            last_terms = terms

        print_human_results(results, terms)

    con.close()

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

    # --- ask (interactive REPL) ---
    p_ask = sub.add_parser(
        'ask',
        help='Interactive plain-English assistant for querying the transcripts.',
    )
    p_ask.add_argument(
        '--db', default='ggtranscripts.db',
        help='Path to the SQLite database (default: ggtranscripts.db)'
    )
    p_ask.add_argument(
        '--limit', type=int, default=10,
        help='Maximum results per search (default: 10)'
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == 'build':
        cmd_build(args)
    elif args.command == 'search':
        cmd_search(args)
    elif args.command == 'ask':
        cmd_ask(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
