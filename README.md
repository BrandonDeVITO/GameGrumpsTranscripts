# Game Grumps Transcripts

A searchable archive of Game Grumps episode transcripts — one JSON `.txt` file per episode,
organized into series/playlist folders under `transcripts/`.

---

## Quick start

### Step 1 — Build the index (one-time setup)

```bash
python tools/gg.py build-index
```

This scans every transcript, parses the JSON, and creates a full-text search index at
`data/gg_index.sqlite`.  Takes about 2 minutes; only needs to be re-run when transcripts
are added or updated.

> The generated database is gitignored (it's several hundred MB).

---

## Asking questions (plain-English interface)

### Ask anything

```bash
python tools/gg.py ask "Are there any mentions of banana or bananas?"
python tools/gg.py ask "Did they ever play Bloodborne?"
python tools/gg.py ask "Did they ever mention kiss your dad?"
python tools/gg.py ask "Have they talked about pineapple on pizza?"
```

**Example output:**

```
Yes — found 500 mentions across 286 episodes.
Showing the top 20 (of 500 total):

  #1   Donkey Kong Country Tropical Freeze
       "the bananas right the bananas are"
       ↳ at 5:19  |  https://www.youtube.com/watch?v=3prcNROv6uc&t=319

  #2   Legend of Zelda: Breath of the Wild _ Game Grumps
       "bananas on bananas are growing out of"
       ↳ at 5:53  |  https://www.youtube.com/watch?v=cxBZH5kAEoQ&t=353
  ...

To dig deeper into any result:
  python tools/gg.py context N    — show the surrounding conversation
  python tools/gg.py detail N     — full episode info
```

---

### Get the surrounding conversation for a result

```bash
python tools/gg.py context 4
```

Shows the lines of dialogue before and after result #4, so you can see the full joke or
bit in context:

```
Context for result #4
Episode : Resident Evil 2 [1998] _ Game Grumps  (video: u3LvjgNgtgo)
Showing conversation around 3:26:

  [3:23]    "Everything's fine. [laughter]"
  [3:26]  ▶ "Banana, banana, banana, banana,"     ← your result
  [3:28]    "terracotta, banana, terracotta,"
  [3:29]    "terracotta pie."
  [3:32]    "He had he ate one of those recently on the mythical kitchen show."
  ...

Watch full episode : https://www.youtube.com/watch?v=u3LvjgNgtgo
Jump to this moment: https://www.youtube.com/watch?v=u3LvjgNgtgo&t=206
```

Use `--window` to show more or less context (default: 30 seconds either side):

```bash
python tools/gg.py context 4 --window 60
```

---

### Get full episode details for a result

```bash
python tools/gg.py detail 4
```

```
Episode #4 from your last search:

  Series   : Resident Evil 2 [1998] _ Game Grumps
  Video ID : u3LvjgNgtgo
  Snippets : 1415 (total lines in transcript)
  Watch    : https://www.youtube.com/watch?v=u3LvjgNgtgo

The matching line (at 3:26):
  "Banana, banana, banana, banana,"

Jump directly to this moment:
  https://www.youtube.com/watch?v=u3LvjgNgtgo&t=206
```

The `context` and `detail` commands always refer to the results from your **most recent
`ask`** — so you can ask a question, browse the list, then follow up as many times as
you like.

---

## More options

### Limit the number of results

```bash
python tools/gg.py ask "banana" --limit 50
```

### Exact phrase search

Wrap the phrase in double quotes (use single quotes around the whole argument on the
command line):

```bash
python tools/gg.py ask '"kiss your dad"'
```

### Advanced FTS search (programmatic / tabular output)

For power users who want raw FTS5 syntax and a tabular result:

```bash
python tools/gg.py search "banana" --series "Bloodborne" --limit 20
python tools/gg.py search '"kiss your dad"' --json
```

### Find two phrases near each other in time

```bash
python tools/gg.py cooccur "banana" "luigi" --window 60
```

Returns every episode where both phrases appear within 60 seconds of each other.

### Look up an episode directly by video ID

```bash
python tools/gg.py episode u3LvjgNgtgo
python tools/gg.py episode u3LvjgNgtgo --fetch   # also fetches YouTube title via yt-dlp
```

---

## Python module API

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
    [VIDEO_ID].txt    ← JSON transcript (timestamped snippets)
    …
tools/
  gg.py               ← CLI + Python API
  requirements.txt
data/
  .gitkeep
  gg_index.sqlite     ← generated; gitignored
  .last_results.json  ← session state; gitignored
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
