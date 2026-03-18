#!/usr/bin/env python3
"""
ask.py — Plain-English search interface for Game Grumps transcripts.

Interactive session:
    python tools/ask.py

Single question (prints answer and exits):
    python tools/ask.py "Are there any mentions of banana?"
"""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "data" / "gg_index.sqlite"
YT = "https://www.youtube.com/watch?v="

_CONTEXT_WINDOW = 60.0   # seconds of context to show around a result
_PAGE_SIZE = 20          # results per page

_STOP = {
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can",
    "i", "me", "my", "we", "us", "our", "you", "your", "he", "she",
    "it", "its", "they", "them", "their", "this", "that", "these", "those",
    "any", "some", "all", "both", "each", "every", "either", "neither",
    "no", "not", "nor", "so", "yet", "but", "or", "for", "of",
    "in", "on", "at", "to", "up", "as", "by", "into", "out", "off",
    "over", "under", "again", "then", "once", "here", "there", "just",
    "very", "too", "also", "even", "still", "more", "most", "same",
    "own", "few", "than", "only", "such", "with", "about", "through",
    "during", "before", "after", "between", "from", "while", "if",
    "when", "where", "why", "how", "what", "who", "which", "whose",
    "mention", "mentions", "mentioned", "mentioning",
    "say", "said", "says", "saying", "talk", "talks", "talked", "talking",
    "find", "search", "look", "tell", "show", "give", "get",
    "occur", "occurs", "appear", "appears", "appeared",
    "transcript", "transcripts", "episode", "episodes", "series",
    "game", "grumps", "ever", "never", "please",
    "many", "times", "often", "much", "frequently",
}


class Session:
    def __init__(self) -> None:
        self.results: list[dict] = []
        self.last_term: str = ""
        self._offset: int = 0

    def store(self, results: list[dict], term: str) -> None:
        self.results = results
        self.last_term = term
        self._offset = 0

    def get(self, n: int) -> dict | None:
        if 1 <= n <= len(self.results):
            return self.results[n - 1]
        return None


def _conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        print(
            "\nThe search index hasn't been built yet.\n"
            "Run this one-time setup first (takes about 90 seconds):\n\n"
            "    python tools/gg.py build-index\n"
        )
        sys.exit(1)
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _ts(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _yt(video_id: str, start: float = 0.0) -> str:
    return f"{YT}{video_id}&t={int(start)}"


def _quoted(text: str) -> str | None:
    m = re.search(r'["\u201c\u201d\u2018\u2019](.+?)["\u201c\u201d\u2018\u2019]', text)
    return m.group(1).strip() if m else None


def _phrase_after_trigger(text: str) -> str | None:
    """
    Extract a phrase that follows natural-language trigger words.
    e.g. "Did they ever say kiss your dad?" -> "kiss your dad"
         "Any mentions of banana?" -> "banana"
    The phrase is preserved intact so multi-word phrases search correctly.
    """
    patterns = [
        r'\bsay(?:s|ing)?\s+(.+?)[\?\.!]*$',
        r'\bsaid\s+(.+?)[\?\.!]*$',
        r'\bmentions?\s+of\s+(.+?)(?:\s+in the.*)?[\?\.!]*$',
        r'\bany\s+(?:mentions?\s+of\s+)?(.+?)(?:\s+in the.*)?[\?\.!]*$',
        r'\b(?:find|search for|look for)\s+(.+?)[\?\.!]*$',
        r'\b(?:about|regarding)\s+(.+?)[\?\.!]*$',
        r'\bever\s+(?:mention|say|said|talk about|reference)\s+(.+?)[\?\.!]*$',
        r'\bdo they\s+(?:mention|say|talk about)\s+(.+?)[\?\.!]*$',
        r'\bdid they\s+(?:mention|say|talk about)\s+(.+?)[\?\.!]*$',
        r'^any\s+(.+?)(?:\s+in the.*)?[\?\.!]*$',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            phrase = m.group(1).strip().rstrip("?!.,")
            if phrase:
                return phrase
    return None


def _strip_to_term(text: str) -> str:
    cleaned = re.sub(r"[?!.,;:]+", " ", text)
    words = [w.lower() for w in cleaned.split()]
    content = [w for w in words if w not in _STOP and len(w) > 1]
    return " ".join(content)


def _result_number(text: str) -> int | None:
    m = re.search(r"#\s*(\d+)", text)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(?:number|result|item)\s+(\d+)\b", text, re.I)
    if m:
        return int(m.group(1))
    m = re.match(r"^\s*(\d+)\s*$", text)
    if m:
        return int(m.group(1))
    return None


def _is_context(text: str) -> bool:
    return bool(re.search(
        r"\b(context|around|surrounding|nearby|what else|more about|expand|"
        r"full|conversation|happened|what was said|before|after)\b", text, re.I
    ))


def _is_stats(text: str) -> bool:
    return bool(re.search(
        r"\b(how many|count|times|often|frequently|most|stats|statistics|"
        r"breakdown|summary|how much)\b", text, re.I
    ))


def _is_more(text: str) -> bool:
    return bool(re.match(
        r"^\s*(more|next|continue|keep going|show more|more results)\s*$", text, re.I
    ))


def _cooccurrence_pair(text: str) -> tuple[str, str] | None:
    clean = re.sub(
        r"^(?:did they ever|do they|are there|find|show me|were there)\s+", "", text, flags=re.I
    )
    patterns = [
        r"(?:both\s+)?(.+?)\s+and\s+(.+?)(?:\s+(?:together|at the same|simultaneously).*)?$",
        r"(.+?)\s+(?:together with|alongside|with)\s+(.+)",
    ]
    for pat in patterns:
        m = re.search(pat, clean, re.I)
        if m:
            a = m.group(1).strip().rstrip("?!.,")
            b = m.group(2).strip().rstrip("?!.,")
            a_clean = _strip_to_term(a)
            b_clean = _strip_to_term(b)
            if a_clean and b_clean and a_clean != b_clean:
                return a_clean, b_clean
    return None


def _fts_query(term: str, is_phrase: bool = False) -> str:
    if is_phrase and " " in term:
        escaped = term.replace('"', '""')
        return f'"{escaped}"'
    return term


def _search(term: str, limit: int = 100, is_phrase: bool = False) -> list[dict]:
    conn = _conn()
    q = _fts_query(term, is_phrase)
    rows = conn.execute(
        """
        SELECT e.video_id, e.series, s.start, s.text,
               s.id AS snippet_id, e.id AS episode_id
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (q, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _context(result: dict, window: float = _CONTEXT_WINDOW) -> list[dict]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT start, text FROM snippets
        WHERE episode_id = ? AND start BETWEEN ? AND ?
        ORDER BY start
        """,
        (result["episode_id"], result["start"] - window, result["start"] + window),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _correlate(term_a: str, term_b: str, window: float = 30.0, limit: int = 50) -> list[dict]:
    conn = _conn()

    def fetch(term: str) -> dict[int, list[tuple[float, str]]]:
        out: dict[int, list[tuple[float, str]]] = {}
        for r in conn.execute(
            """
            SELECT s.episode_id, s.start, s.text
            FROM snippets_fts sf
            JOIN snippets s ON s.id = sf.rowid
            WHERE snippets_fts MATCH ?
            ORDER BY s.episode_id, s.start
            """,
            (term,),
        ).fetchall():
            out.setdefault(r["episode_id"], []).append((r["start"], r["text"]))
        return out

    hits_a = fetch(term_a)
    hits_b = fetch(term_b)
    common = set(hits_a) & set(hits_b)

    matches: list[dict] = []
    seen: set[tuple[int, int, int]] = set()
    for ep_id in sorted(common):
        ep = conn.execute(
            "SELECT video_id, series FROM episodes WHERE id = ?", (ep_id,)
        ).fetchone()
        for sa, ta in hits_a[ep_id]:
            for sb, tb in hits_b[ep_id]:
                if abs(sa - sb) <= window:
                    key = (ep_id, round(sa), round(sb))
                    if key not in seen:
                        seen.add(key)
                        matches.append({
                            "video_id": ep["video_id"],
                            "series": ep["series"],
                            "start": min(sa, sb),
                            "text": ta,
                            "episode_id": ep_id,
                            "start_a": sa, "text_a": ta,
                            "start_b": sb, "text_b": tb,
                        })
    conn.close()
    return matches[:limit]


def _corpus_summary() -> str:
    conn = _conn()
    series  = conn.execute("SELECT COUNT(DISTINCT series) FROM episodes").fetchone()[0]
    eps     = conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
    conn.close()
    return f"{series:,} series · {eps:,} episodes"


def _print_results(results: list[dict], term: str, total: int, offset: int = 0) -> None:
    shown = len(results)
    if shown == 0:
        print(f'\nNo, "{term}" doesn\'t appear anywhere in the transcripts.\n')
        return

    if total == 1:
        print(f'\nYes — "{term}" comes up once in the transcripts:\n')
    elif total <= _PAGE_SIZE:
        print(f'\nYes — "{term}" comes up {total:,} time(s) across the transcripts:\n')
    else:
        showing = f"{offset + 1}–{offset + shown}"
        print(f'\nYes — "{term}" comes up {total:,} time(s). Showing results {showing}:\n')

    for i, r in enumerate(results, offset + 1):
        print(f"  {i}.  {r['series']}  —  {_ts(r['start'])}")
        print(f"       \"{r['text']}\"")
        print()

    remaining = total - offset - shown
    if remaining > 0:
        print(f"  There are {remaining} more result(s). Just say \"more\" to continue.\n")


def _print_context(n: int, result: dict, snippets: list[dict]) -> None:
    target = result["start"]
    print(f"\nHere's what was happening around {_ts(target)} in \"{result['series']}\":\n")
    # Mark only the single closest snippet
    closest = min(snippets, key=lambda s: abs(s["start"] - target)) if snippets else None
    for s in snippets:
        marker = "  ▶" if s is closest else "   "
        print(f"{marker}  {_ts(s['start'])}   {s['text']}")
    print(f"\n  Watch it here: {_yt(result['video_id'], target)}\n")


def _print_episode(n: int, result: dict) -> None:
    print(f"\nResult #{n} is from the series \"{result['series']}\".")
    print(f"  It happens at {_ts(result['start'])} into the episode.")
    print(f"  Watch it here: {_yt(result['video_id'], result['start'])}\n")


def _print_correlate(matches: list[dict], term_a: str, term_b: str) -> None:
    if not matches:
        print(
            f'\nNo — "{term_a}" and "{term_b}" never appear within '
            f"30 seconds of each other in any episode.\n"
        )
        return
    ep_count = len({m["episode_id"] for m in matches})
    print(
        f'\nYes — "{term_a}" and "{term_b}" come up close together '
        f"in {ep_count} episode(s). Here are the moments:\n"
    )
    for i, m in enumerate(matches, 1):
        print(f"  {i}.  {m['series']}")
        print(f"       {_ts(m['start_a'])}   \"{m['text_a']}\"")
        print(f"       {_ts(m['start_b'])}   \"{m['text_b']}\"")
        print(f"       {_yt(m['video_id'], m['start'])}")
        print()


def _print_stats(term: str) -> None:
    conn = _conn()
    total = conn.execute(
        "SELECT COUNT(*) FROM snippets_fts WHERE snippets_fts MATCH ?", (term,)
    ).fetchone()[0]
    if total == 0:
        print(f'\n  "{term}" doesn\'t appear anywhere in the transcripts.\n')
        conn.close()
        return

    ep_count = conn.execute(
        """
        SELECT COUNT(DISTINCT e.id)
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        """,
        (term,),
    ).fetchone()[0]

    top_series = conn.execute(
        """
        SELECT e.series, COUNT(*) AS cnt
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        GROUP BY e.series ORDER BY cnt DESC LIMIT 10
        """,
        (term,),
    ).fetchall()

    top_eps = conn.execute(
        """
        SELECT e.series, e.video_id, COUNT(*) AS cnt
        FROM snippets_fts sf
        JOIN snippets s ON s.id = sf.rowid
        JOIN episodes e ON e.id = s.episode_id
        WHERE snippets_fts MATCH ?
        GROUP BY e.id ORDER BY cnt DESC LIMIT 10
        """,
        (term,),
    ).fetchall()
    conn.close()

    print(f'\n"{term}" is mentioned {total:,} time(s) across {ep_count} episode(s).\n')

    print("  Series that mention it the most:")
    for r in top_series:
        times = "time" if r["cnt"] == 1 else "times"
        print(f"    {r['cnt']:>5}  {times}   {r['series']}")

    print("\n  Episodes that mention it the most:")
    for r in top_eps:
        times = "time" if r["cnt"] == 1 else "times"
        print(f"    {r['cnt']:>4}  {times}   {r['series']}")
        print(f"           {_yt(r['video_id'])}")
    print()


_HELP = """
  Just ask in plain English. For example:

    Are there any mentions of banana in the transcripts?
    Did they ever say kiss your dad?
    Find everything about egoraptor
    How many times do they mention banana?
    banana and cake                   <- find where both come up close together
    What was the context of #2?
    What episode was #3?
    more                              <- see more results from the last search
    quit                              <- exit
"""


def answer(text: str, session: Session) -> None:
    text = text.strip()
    if not text:
        return

    lower = text.lower()

    if re.match(r"^(quit|exit|bye|goodbye|q)$", lower):
        print("\n  See you later!\n")
        sys.exit(0)

    if re.match(r"^(help|\?|h)$", lower):
        print(_HELP)
        return

    if _is_more(lower):
        if not session.results:
            print("\n  I haven't searched for anything yet — ask me a question first.\n")
            return
        offset = session._offset + _PAGE_SIZE
        session._offset = offset
        page = session.results[offset : offset + _PAGE_SIZE]
        if not page:
            print("\n  No more results — that was everything.\n")
            return
        _print_results(page, session.last_term, len(session.results), offset)
        return

    ref_n = _result_number(text)
    if ref_n is not None:
        result = session.get(ref_n)
        if result is None:
            if session.results:
                print(
                    f"\n  I don't have a result #{ref_n}. "
                    f"The last search returned {len(session.results)} result(s).\n"
                )
            else:
                print("\n  I haven't searched for anything yet — ask me something first.\n")
            return

        if _is_context(lower):
            snippets = _context(result)
            _print_context(ref_n, result, snippets)
        else:
            _print_episode(ref_n, result)
        return

    if _is_stats(lower):
        term = _quoted(text) or _phrase_after_trigger(text) or _strip_to_term(text)
        if not term:
            print('\n  What would you like stats on? Try: "How many times do they say banana?"\n')
            return
        _print_stats(term)
        return

    pair = _cooccurrence_pair(lower)
    if pair:
        term_a, term_b = pair
        matches = _correlate(term_a, term_b)
        session.store(matches, f"{term_a} and {term_b}")
        _print_correlate(matches, term_a, term_b)
        return

    # Default: search
    # Priority: explicit quotes > phrase after trigger word > stop-word strip
    term = _quoted(text) or _phrase_after_trigger(text) or _strip_to_term(text)
    is_phrase = bool(_quoted(text) or _phrase_after_trigger(text))

    if not term:
        print(
            "\n  I'm not sure what to search for. Try something like:\n"
            '  "Are there any mentions of banana?"\n'
        )
        return

    results = _search(term, limit=100, is_phrase=is_phrase)

    # If phrase search found nothing, fall back to keyword AND search
    if not results and is_phrase and " " in term:
        results = _search(term, limit=100, is_phrase=False)

    session.store(results, term)
    _print_results(results[:_PAGE_SIZE], term, len(results))


def main() -> None:
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        s = Session()
        answer(question, s)
        return

    try:
        summary = _corpus_summary()
    except SystemExit:
        return

    print(f"\n  Game Grumps Transcript Search  ({summary})")
    print('  Ask me anything about the show. Type "help" for examples, "quit" to exit.\n')

    session = Session()
    while True:
        try:
            text = input("  You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n\n  See you later!\n")
            break
        answer(text, session)


if __name__ == "__main__":
    main()
