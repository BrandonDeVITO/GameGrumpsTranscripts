# Game Grumps Transcripts

A searchable archive of Game Grumps episode transcripts — one JSON `.txt` file per episode,
organized into series/playlist folders under `transcripts/`.

---

## Searching the corpus

### 1. Build the index (one-time, or after adding new transcripts)

```bash
python tools/gg.py build-index
```

This scans every `.txt` file under `transcripts/`, parses the JSON transcript, and
creates a **SQLite + FTS5** full-text index at `data/gg_index.sqlite`.

> **Note:** The generated database is gitignored (it can be several hundred MB for the full
> corpus). Re-run `build-index` whenever you add or update transcripts.

Options:

```
--transcripts-dir PATH   Path to transcripts root (default: ./transcripts)
--db PATH                Path to write the DB (default: ./data/gg_index.sqlite)
--dry-run                Scan files without writing anything
```

---

### 2. Search for a phrase

```bash
python tools/gg.py search "banana"
python tools/gg.py search "kiss your dad" --limit 30
python tools/gg.py search "bloodborne" --series "Bloodborne"
python tools/gg.py search "grumpcade" --json        # machine-readable output
```

The query uses **FTS5 syntax**: phrases in quotes, boolean `AND`/`OR`/`NOT`, prefix search
with `*`, etc.  Results are ranked by relevance and include a snippet with the matching
text highlighted between `>>>` and `<<<`.

Options:

```
QUERY              Search query (FTS5 syntax)
--series TEXT      Filter by series/folder name (substring match)
--video VIDEO_ID   Filter to a single video_id
--limit N          Max results returned (default 20)
--json             Output as JSON array
```

---

### 3. Find co-occurring phrases (within a time window)

```bash
python tools/gg.py cooccur "banana" "luigi" --window 60
python tools/gg.py cooccur "arin" "we're back" --window 30 --limit 10
```

Returns episodes (and timestamps) where both phrases appear within `--window` seconds
of each other — useful for tracking recurring bits, callbacks, and joke setups.

Options:

```
PHRASE_A           First phrase
PHRASE_B           Second phrase
--window SECONDS   Time window (default 30s)
--limit N          Max results (default 20)
--json             Output as JSON
```

---

### 4. Look up a specific episode

```bash
python tools/gg.py episode abc123XYZ
python tools/gg.py episode abc123XYZ --fetch    # also fetches YouTube title/date via yt-dlp
python tools/gg.py episode abc123XYZ --json
```

The `--fetch` flag requires [`yt-dlp`](https://github.com/yt-dlp/yt-dlp) to be installed
(`pip install yt-dlp`).  Without it, you can still visit the YouTube URL shown in the output.

---

### 5. Python module API

You can also import the search function directly:

```python
from tools.gg import search

results = search("banana", series="Bloodborne", limit=10)
for r in results:
    print(r["video_id"], r["series"], r["start"], r["excerpt"])
```

---

## File layout

```
transcripts/
  <Series Name>/
    [VIDEO_ID].txt    ← JSON transcript (snippets with timestamps)
    …
tools/
  gg.py               ← CLI + Python API
  requirements.txt
data/
  .gitkeep
  gg_index.sqlite     ← generated; gitignored
```

---

## Transcript format

Each `.txt` file is a JSON document:

```json
{
  "video_id": "abc123XYZ",
  "language": "English (auto-generated)",
  "language_code": "en",
  "is_generated": true,
  "snippets": [
    { "text": "welcome back to game grumps", "start": 3.2, "duration": 3.3 },
    …
  ]
}
```

---

## Requirements

- Python 3.8+
- No third-party packages required for core functionality
- `yt-dlp` (optional) for `episode --fetch`
