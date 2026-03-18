# Game Grumps Transcripts — Search

**Yes, it's ready to use.** Two steps to get started, then just ask questions in plain English.

---

## Setup (one time only)

```bash
# 1. Build the index — takes about 90 seconds, then never again
python tools/gg.py build-index

# 2. Start asking
python tools/ask.py
```

---

## How to use it

Run `python tools/ask.py` and you'll get a prompt. Just type your question naturally:

```
  Game Grumps Transcript Search  (511 series · 7,838 episodes)
  Ask me anything about the show. Type "help" for examples, "quit" to exit.

  You: Are there any mentions of banana in the transcripts?

Yes — "banana" comes up 1,894 times. Showing results 1–20:

  1.  Resident Evil 2 [1998] _ Game Grumps  —  3:26
       "Banana, banana, banana, banana,"

  2.  Pokemon Art Academy  —  5:19
       "banana banana Sam banana"
  ...

  You: What was the context of #2?

Here's what was happening around 5:19 in "Pokemon Art Academy":

     5:05   um
     5:08   smaller bananas oh that's just looks
     5:14   oh okay all right banana banana arms
  ▶  5:19   banana banana Sam banana
     5:22   Sam this is Pokemon banana
     ...

  Watch it here: https://www.youtube.com/watch?v=KxSa0uNqwaM&t=319

  You: What episode was #2?

Result #2 is from the series "Pokemon Art Academy".
  It happens at 5:19 into the episode.
  Watch it here: https://www.youtube.com/watch?v=KxSa0uNqwaM&t=319
```

---

## Things you can ask

**Search for any word or phrase:**
```
Are there any mentions of banana?
Did they ever say kiss your dad?
Find everything about egoraptor
What did they say about Bloodborne?
```

**Find where two things come up together:**
```
banana and cake
egoraptor and sonic
```

**Get stats on a topic:**
```
How many times do they mention banana?
How often do they say egoraptor?
```

**Follow up on a numbered result:**
```
What was the context of #3?      ← shows the surrounding transcript lines
What episode was #3?             ← shows series name and YouTube link
```

**Page through results:**
```
more
```

---

## Quick single-question mode

You can also ask a single question without entering interactive mode:

```bash
python tools/ask.py "Did they ever say kiss your dad?"
python tools/ask.py "Are there any mentions of banana?"
```

---

## Requirements

- Python 3.8 or later
- No packages to install — uses only Python's built-in standard library
- Runs on Windows, macOS, and Linux

---

## Setup details

The `build-index` command (from `tools/gg.py`) walks every transcript file in `transcripts/`,
parses the JSON, and writes a local SQLite database to `data/gg_index.sqlite`.
That file is not committed to the repo — you generate it locally on your machine.

Run `build-index` again any time you pull new transcripts. It skips files already indexed.
To force a full rebuild from scratch: `python tools/gg.py build-index --full`

---

## Advanced / programmatic use

`tools/gg.py` is the lower-level CLI that `ask.py` builds on. Use it directly
if you want precise control or want to pipe output to other tools:

```bash
python tools/gg.py "banana"                        # search
python tools/gg.py "banana" --in "Sonic" -n 50     # filter by series, more results
python tools/gg.py "banana" "cake"                 # co-occurrence
python tools/gg.py --stats "banana"                # stats
python tools/gg.py --stats                         # corpus overview
```

