# Game Grumps Transcripts — Search

**Yes, it's ready to use.** Two steps to get started:

```bash
# Step 1 — one time only, takes about 90 seconds
python tools/gg.py build-index

# Step 2 — ask anything
python tools/gg.py "banana"
```

That's it. No subcommands to memorize. Just run the script with your question.

---

## How to ask

**Search for a word or phrase:**
```bash
python tools/gg.py "banana"
python tools/gg.py "kiss your dad"
python tools/gg.py "egoraptor"
```

**Limit to one series:**
```bash
python tools/gg.py "banana" --in "Sonic"
python tools/gg.py "robot" --in "Zelda"
```

**Find where two things are mentioned together** (within 30 seconds of each other):
```bash
python tools/gg.py "banana" "cake"
python tools/gg.py "egoraptor" "bloodborne"
python tools/gg.py "jon" "arin" --window 10
```

**See which series/episodes mention a term the most:**
```bash
python tools/gg.py --stats "banana"
```

**See the full corpus overview:**
```bash
python tools/gg.py --stats
```

---

## What the output looks like

```
"banana" — 5 result(s) shown

Resident Evil 2 [1998] _ Game Grumps  |  3:26
  Banana, banana, banana, banana,
  https://www.youtube.com/watch?v=u3LvjgNgtgo&t=206

Pokemon Art Academy  |  5:19
  banana banana Sam banana
  https://www.youtube.com/watch?v=KxSa0uNqwaM&t=319
```

Each result shows the **series**, the **timestamp**, the **line that was said**, and a **YouTube link** that jumps straight to that moment.

---

## Requirements

- Python 3.8 or later
- No packages to install — uses only Python's built-in standard library
- Runs on Windows, macOS, and Linux

---

## Setup details

The `build-index` command walks every transcript file in `transcripts/`, parses the JSON, and writes a local SQLite database to `data/gg_index.sqlite`.  
That file is not committed to the repo — you generate it locally.

Run `build-index` again any time you pull new transcripts. It's incremental by default (skips episodes already in the index).  
To force a full rebuild from scratch: `python tools/gg.py build-index --full`

---

## All options

```
python tools/gg.py "term"                 search for a word or phrase
python tools/gg.py "term" --in "Series"  search within one series
python tools/gg.py "term" -n 50          show up to 50 results (default: 20)
python tools/gg.py "a" "b"               find where two terms appear together
python tools/gg.py "a" "b" --window 60   widen the co-occurrence window to 60 seconds
python tools/gg.py --stats               corpus overview
python tools/gg.py --stats "term"        mentions by series and episode
python tools/gg.py build-index           build / update the index
python tools/gg.py build-index --full    drop and fully rebuild
```

