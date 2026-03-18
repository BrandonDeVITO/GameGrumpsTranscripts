# Game Grumps Transcript Database

This repository includes all available Game Grumps episode transcripts and a
fully-searchable SQLite database built from them.

**7,838 unique episodes · 511 series · 6.1 million captioned snippets with timestamps**
*(counts as of initial database build; run `python scripts/validate_db.py` for current totals)*

---

## Quickstart – Chat with the transcripts

```bash
python scripts/chat.py
```

Then just ask in plain English:

```
  You: Are there any mentions of banana or bananas?
  GG:  Found 2,694 snippet(s) matching "banana" or "bananas" …

  You: What was the context of #3?
  GG:  Context for #3 — Monopoly (ALL) _ Game Grumps …

  You: What episode was #2?
  GG:  Episode #2 — Resident Evil 2 [1998] _ Game Grumps …
```

---

## What you can ask

| Question type | Example |
|---|---|
| Search | `Are there any mentions of banana or bananas?` |
| Quoted phrase | `Find every time they say "kiss your dad"` |
| Series filter | `Search for "cool" in series "Sonic Colors"` |
| Follow-up — episode info | `What episode was #2?` |
| Follow-up — context | `What was the context of #3?` |
| Co-occurrence | `mario and luigi within 20 seconds` |
| Co-occurrence | `Find every time they mention arin and danny together` |
| Stats | `stats` |
| Help | `help` |

### Options

```bash
python scripts/chat.py --limit 20        # show 20 results per search (default: 10)
python scripts/chat.py --db /path/to.db  # use a different database file
```

---

## Database file

The database lives at **`db/transcripts.sqlite`** and is tracked with
[Git LFS](https://git-lfs.github.com/) (≈ 950 MB).

> **When you clone this repo**, run:
> ```bash
> git lfs install
> git lfs pull
> ```
> to download the database file.

### Regenerating the database

If you add or update transcript files, rebuild the database with:

```bash
python scripts/build_db.py
```

Options:
```
--transcripts-dir   Path to the transcripts root folder (default: <repo>/transcripts)
--db-path           Output SQLite path              (default: <repo>/db/transcripts.sqlite)
--quiet             Suppress progress output
```

The script:
- Discovers every `[YouTubeId].txt` file under `transcripts/`
- Parses the JSON (video_id, language, snippets with start/duration/text)
- Deduplicates episodes that appear in multiple series folders
- Creates an FTS5 full-text index over all 6 million+ caption segments

A clean build takes about 4–5 minutes on a modern machine.

---

## Scripts reference

| Script | Purpose |
|---|---|
| `scripts/chat.py` | **Interactive plain-text chat** (start here) |
| `scripts/search.py` | Scriptable CLI search — JSON output, co-occurrence, episode lookup |
| `scripts/build_db.py` | Build / rebuild `db/transcripts.sqlite` from the transcript files |
| `scripts/validate_db.py` | CI check — verifies DB exists and episode count matches files |

### `scripts/search.py` (scriptable / JSON output)

```bash
# Phrase search
python scripts/search.py "kiss your dad"

# Filter by series
python scripts/search.py "bloodborne" --series "Bloodborne"

# JSON output
python scripts/search.py "cool cool cool" --json

# Co-occurrence within 30 seconds
python scripts/search.py --cooccur "arin" "danny" --window 30

# Episode info by YouTube ID
python scripts/search.py --episode fmJNXG_f2SY

# Stats
python scripts/search.py --stats
```

---

## Database schema

```sql
-- Episode metadata (one row per unique YouTube video)
CREATE TABLE episodes (
    id            INTEGER PRIMARY KEY,
    series        TEXT,      -- folder name under transcripts/
    youtube_id    TEXT UNIQUE,
    relative_path TEXT,      -- e.g. transcripts/Dead Rising 3/[qBmWK_uS2o4].txt
    language      TEXT,
    is_generated  INTEGER,   -- 1 = auto-generated captions
    imported_at   TEXT
);

-- Individual caption segments with timestamps
CREATE TABLE snippets (
    id          INTEGER PRIMARY KEY,
    episode_id  INTEGER REFERENCES episodes(id),
    start       REAL,        -- seconds from start of video
    duration    REAL,
    text        TEXT,
    normalized  TEXT         -- lowercased, accent-stripped (used by FTS)
);

-- FTS5 full-text index linked to snippets
CREATE VIRTUAL TABLE snippets_fts USING fts5(
    text, normalized,
    content = 'snippets', content_rowid = 'id'
);
```

---

## Validation / CI

```bash
python scripts/validate_db.py
```

Checks:
1. `db/transcripts.sqlite` exists
2. File is a valid, readable SQLite database
3. Episode count in DB matches unique YouTube IDs found in transcript files
4. Every ID in the DB has a corresponding transcript file (and vice-versa)

---

## Requirements

- Python 3.10+ (uses built-in `sqlite3` with FTS5 — no extra packages needed)
- Git LFS (to pull the pre-built database)
