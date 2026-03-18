# Game Grumps Transcripts — Searchable Database

This repository contains **~10,000 episode transcripts** from the Game Grumps YouTube channel, organized by series/playlist.  
The `tools/gg.py` CLI lets you build a local SQLite full-text-search index and query it with plain English terms.

---

## Quick start

```bash
# 1. Clone the repo
git clone https://github.com/BrandonDeVITO/GameGrumpsTranscripts.git
cd GameGrumpsTranscripts

# 2. Build the index (one-time; takes ~60-120 seconds on a laptop)
python tools/gg.py build-index

# 3. Search!
python tools/gg.py search "banana"
```

> **Requirements:** Python 3.8 or later.  No third-party packages needed — only the Python standard library is used.

---

## Commands

### `build-index`

Walks the `transcripts/` tree, parses every `.txt` (JSON) file, and writes a SQLite database to `data/gg_index.sqlite`.

```bash
python tools/gg.py build-index           # incremental (skips already-indexed episodes)
python tools/gg.py build-index --full    # drop and fully rebuild from scratch
```

The generated database file is excluded from version control (see `.gitignore`).  
Run `build-index` once on a fresh clone, or again after pulling new transcripts.

---

### `search`

Full-text search across all episode transcripts.

```bash
python tools/gg.py search "kiss your dad"
python tools/gg.py search "banana" --limit 30
python tools/gg.py search "egoraptor" --series "Sonic"
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `query` | *(required)* | Search phrase or term (FTS5 syntax supported) |
| `--limit / -n` | `20` | Maximum number of results to show |
| `--series / -s` | *(none)* | Filter by series name (partial, case-insensitive) |

Each result shows:
- **Series** name (folder in `transcripts/`)
- **video_id** — the YouTube video ID
- **file** — relative path to the `.txt` transcript
- **time** — timestamp in `mm:ss` or `hh:mm:ss`
- **snippet** — the matching line

A deep-link URL tip is printed so you can jump straight to the moment in the video:

```
https://www.youtube.com/watch?v=<video_id>&t=<seconds>
```

---

### `stats`

Show corpus overview or per-term statistics.

```bash
python tools/gg.py stats                  # overall corpus overview
python tools/gg.py stats "banana"         # per-series and per-episode counts
```

Without a term: lists series by episode count.  
With a term: shows total hits, episodes with matches, top series, and top episodes.

---

### `correlate`

Find episodes where **two terms appear within a time window** of each other.  
Useful for finding moments where multiple topics intersect.

```bash
python tools/gg.py correlate "banana" "cake"
python tools/gg.py correlate "egoraptor" "bloodborne" --window 60
python tools/gg.py correlate "jon" "arin" --window 10 --limit 30
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `term_a` | *(required)* | First search term |
| `term_b` | *(required)* | Second search term |
| `--window / -w` | `30` | Time window in seconds |
| `--limit / -n` | `20` | Maximum results to display |

---

## Example queries

```bash
# Did they ever mention a banana?
python tools/gg.py search "banana"

# Which series mention bananas the most?
python tools/gg.py stats "banana"

# Did they talk about Egoraptor and Bloodborne at the same time?
python tools/gg.py correlate "egoraptor" "bloodborne" --window 60

# Find episodes mentioning a specific recurring joke
python tools/gg.py search "kiss your dad"

# Search within a specific series
python tools/gg.py search "robot" --series "Sonic"

# FTS5 phrase search (quote within the query)
python tools/gg.py search '"we'll be okay"'

# FTS5 prefix search
python tools/gg.py search "grump*"
```

---

## Database schema

The SQLite database (`data/gg_index.sqlite`) contains:

```sql
-- One row per episode/video
CREATE TABLE episodes (
    id        INTEGER PRIMARY KEY,
    video_id  TEXT NOT NULL UNIQUE,  -- YouTube video ID
    series    TEXT NOT NULL,         -- folder name under transcripts/
    file_path TEXT NOT NULL          -- relative path from repo root
);

-- One row per transcript snippet (timestamped line)
CREATE TABLE snippets (
    id         INTEGER PRIMARY KEY,
    episode_id INTEGER NOT NULL REFERENCES episodes(id),
    start      REAL NOT NULL,   -- seconds from start of video
    duration   REAL NOT NULL,
    text       TEXT NOT NULL
);

-- FTS5 virtual table for full-text search (backed by snippets)
CREATE VIRTUAL TABLE snippets_fts USING fts5(
    text,
    content='snippets',
    content_rowid='id',
    tokenize='porter unicode61'
);
```

---

## Transcript format

Each `.txt` file is a JSON document:

```json
{
  "video_id": "_3L4EqFpbo0",
  "language": "English (auto-generated)",
  "language_code": "en",
  "is_generated": true,
  "snippets": [
    { "text": "welcome back to game grumps everybody", "start": 3.2, "duration": 3.3 },
    ...
  ]
}
```

Filenames follow the pattern `[VIDEO_ID].txt`, e.g. `[_3L4EqFpbo0].txt`.  
Files are located under `transcripts/<Series Name>/`.

---

## File layout

```
GameGrumpsTranscripts/
├── transcripts/
│   ├── Game Grumps Animated/
│   │   ├── [_3L4EqFpbo0].txt
│   │   └── ...
│   ├── Sonic '06/
│   │   └── ...
│   └── ... (572 series folders, ~10,000 transcripts total)
├── tools/
│   ├── gg.py             ← CLI tool (this is what you run)
│   └── requirements.txt  ← no external deps; stdlib only
├── data/
│   └── .gitkeep          ← DB generated here (not committed)
├── .gitignore
└── README.md
```

---

## Notes

- The database is **not committed** to the repository.  It is generated locally from the transcript files.
- **Incremental builds** skip episodes that are already in the database.  Use `--full` to force a complete rebuild.
- The FTS5 index uses **Porter stemming** (`tokenize='porter unicode61'`), so searching for "running" also matches "runs", "ran", etc.
- To jump to a specific moment in a video: `https://www.youtube.com/watch?v=VIDEO_ID&t=SECONDS`
