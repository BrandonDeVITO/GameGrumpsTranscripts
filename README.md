# Game Grumps Transcripts

A repository of every Game Grumps episode transcript, organized by series/playlist.

## Repository structure

```
transcripts/
  <Series Name>/
    [YouTubeVideoId].txt    ← JSON transcript file
    ...
  ...
```

Each `.txt` file is a JSON object with the following schema:

```json
{
  "video_id": "JgwxuusiH2k",
  "language": "English (auto-generated)",
  "language_code": "en",
  "is_generated": true,
  "snippets": [
    { "text": "hey I'm Grumps I'm not so Grump", "start": 0.199, "duration": 4.481 },
    ...
  ]
}
```

---

## Searchable Transcript Database

The `tools/ggdb.py` CLI builds a local **SQLite + FTS5** search index over all transcripts so you can ask questions like:

> *"Did they ever mention a banana, and in what context?"*

### Requirements

- **Python 3.10+** (uses standard library only — no pip install needed)
- SQLite with FTS5 support (included in all standard Python distributions)

### 1. Build the database

```bash
# Full rebuild (default) — creates ggtranscripts.db in the current directory
python tools/ggdb.py build --root transcripts --db ggtranscripts.db

# Incremental rebuild — only re-ingests files whose content has changed
python tools/ggdb.py build --root transcripts --db ggtranscripts.db --incremental
```

Progress is printed every 50 episodes. A full build of ~10,000 transcripts takes a few minutes.

The generated `.db` file is listed in `.gitignore` and is **not committed to the repository** — run the build command to regenerate it locally.

### 2. Search the database

```bash
# Basic search — returns top 20 matches with ±2 lines of context
python tools/ggdb.py search "banana" --db ggtranscripts.db

# Phrase search (use quotes)
python tools/ggdb.py search '"spider kiss"' --db ggtranscripts.db

# Limit results and context window
python tools/ggdb.py search "banana" --db ggtranscripts.db --limit 50 --context 3

# Filter by series (partial match, case-insensitive)
python tools/ggdb.py search "banana" --db ggtranscripts.db --series "Goof Troop"

# Filter to a specific video
python tools/ggdb.py search "banana" --db ggtranscripts.db --video-id JgwxuusiH2k
```

#### Example output

```
Found 5 result(s) for: 'banana'

── [JgwxuusiH2k]  Goof Troop
   Path: Goof Troop/[JgwxuusiH2k].txt  |  Line 42  |  ⏱  1:37
       [1:33] what is even happening right now
       [1:35] I have no idea
   >>> [1:37] it's a banana game I love bananas
       [1:40] that's the whole plot
       [1:42] yeah okay let's go
```

### 3. FTS5 query syntax

The search query uses [SQLite FTS5 syntax](https://www.sqlite.org/fts5.html):

| Pattern | Meaning |
|---------|---------|
| `banana` | any snippet containing "banana" |
| `"spider kiss"` | exact phrase "spider kiss" |
| `banana OR apple` | either word |
| `banana NOT apple` | "banana" but not "apple" |
| `ban*` | prefix match: "ban", "banana", "bananas", … |

### Database schema

```sql
-- One row per episode
CREATE TABLE episodes (
    id          INTEGER PRIMARY KEY,
    video_id    TEXT    UNIQUE NOT NULL,   -- YouTube video ID
    series      TEXT    NOT NULL,          -- top-level folder name
    title       TEXT,                      -- reserved; null for now
    path        TEXT    NOT NULL,          -- relative path from transcripts/
    bytes       INTEGER NOT NULL,
    sha256      TEXT    NOT NULL,
    ingested_at TEXT    NOT NULL           -- ISO-8601 UTC timestamp
);

-- One row per transcript snippet (sentence-level, with timestamp)
CREATE TABLE episode_lines (
    id          INTEGER PRIMARY KEY,
    episode_id  INTEGER NOT NULL REFERENCES episodes(id),
    line_no     INTEGER NOT NULL,
    start       REAL,                      -- seconds from start of video
    duration    REAL,
    text        TEXT    NOT NULL
);

-- FTS5 virtual table for full-text search (content-table backed by episode_lines)
CREATE VIRTUAL TABLE episode_fts USING fts5(
    video_id,
    series,
    text,
    content='episode_lines',
    content_rowid='id'
);
```

The FTS index is backed by `episode_lines`, so every search result maps directly to a timestamp and episode. Results are ranked by BM25 relevance.

**Why index per-line rather than per-episode?**  
Indexing at the snippet level lets the search return exact timestamps and meaningful context windows. If the whole episode were one FTS document, you'd only know *which episode* matched, not *where* in a 30-minute video.

---

## Running tests

```bash
python -m unittest tests/test_ggdb.py -v
```

Tests cover: `parse_video_id`, `parse_series`, `load_transcript`, full build, FTS search, idempotent rebuild, and incremental ingestion. Fixture transcripts live in `tests/fixtures/`.

---

## On-demand YouTube metadata

The `video_id` in every transcript corresponds to a real YouTube video. To look up a video's title and upload date you can open:

```
https://www.youtube.com/watch?v=<video_id>
```

A future enhancement could use the [YouTube Data API](https://developers.google.com/youtube/v3) or `yt-dlp` to enrich the `episodes` table with `title` and `published_at`. This is intentionally left optional so the core tool has **zero dependencies**.
