"""
Unit tests for tools/ggdb.py

Tests cover:
- parse_video_id: extracting video ID from filenames
- parse_series: extracting series name from relative paths
- load_transcript: loading and validating JSON transcript files
- Database build: creating episodes and episode_lines from fixture transcripts
- Search: full-text search returns expected results
- Incremental build: unchanged files are skipped; changed files are re-ingested
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

# Allow importing from tools/ without installing the package
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / 'tools'))

from ggdb import (  # noqa: E402
    parse_video_id,
    parse_series,
    load_transcript,
    open_db,
    rebuild_db,
    cmd_build,
    extract_search_terms,
    parse_followup,
    run_human_search,
    print_human_results,
)

FIXTURES_DIR = Path(__file__).resolve().parent / 'fixtures'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _BuildArgs:
    """Minimal stand-in for argparse.Namespace for cmd_build."""
    def __init__(self, root, db, incremental=False):
        self.root = root
        self.db = db
        self.incremental = incremental


# ---------------------------------------------------------------------------
# parse_video_id
# ---------------------------------------------------------------------------

class TestParseVideoId(unittest.TestCase):

    def test_standard_filename(self):
        self.assertEqual(parse_video_id('[fmJNXG_f2SY].txt'), 'fmJNXG_f2SY')

    def test_underscore_in_id(self):
        self.assertEqual(parse_video_id('[_3L4EqFpbo0].txt'), '_3L4EqFpbo0')

    def test_hyphen_in_id(self):
        self.assertEqual(parse_video_id('[i-irl7VnIec].txt'), 'i-irl7VnIec')

    def test_alphanumeric(self):
        self.assertEqual(parse_video_id('[JgwxuusiH2k].txt'), 'JgwxuusiH2k')

    def test_no_brackets(self):
        self.assertIsNone(parse_video_id('fmJNXG_f2SY.txt'))

    def test_ico_file(self):
        self.assertIsNone(parse_video_id('some_icon.ico'))

    def test_empty_brackets(self):
        self.assertIsNone(parse_video_id('[].txt'))

    def test_wrong_extension(self):
        self.assertIsNone(parse_video_id('[fmJNXG_f2SY].json'))

    def test_case_insensitive_extension(self):
        # .TXT should also match
        self.assertEqual(parse_video_id('[fmJNXG_f2SY].TXT'), 'fmJNXG_f2SY')


# ---------------------------------------------------------------------------
# parse_series
# ---------------------------------------------------------------------------

class TestParseSeries(unittest.TestCase):

    def test_simple_series(self):
        self.assertEqual(parse_series('Goof Troop/[JgwxuusiH2k].txt'), 'Goof Troop')

    def test_series_with_special_chars(self):
        self.assertEqual(
            parse_series('$1,000,000 Pyramid _ Game Grumps/[abc123].txt'),
            '$1,000,000 Pyramid _ Game Grumps'
        )

    def test_posix_separator(self):
        self.assertEqual(parse_series('Some Series/[vid].txt'), 'Some Series')

    def test_windows_style_path(self):
        # pathlib handles both separators on all platforms
        rel = str(Path('Sonic Colors Ultimate _ Game Grumps') / '[OwK5-aBtqKI].txt')
        self.assertEqual(parse_series(rel), 'Sonic Colors Ultimate _ Game Grumps')


# ---------------------------------------------------------------------------
# load_transcript
# ---------------------------------------------------------------------------

class TestLoadTranscript(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _write(self, name, content):
        p = Path(self.tmp) / name
        p.write_text(content, encoding='utf-8')
        return p

    def test_valid_transcript(self):
        p = self._write('t.txt', json.dumps({
            "video_id": "abc",
            "snippets": [{"text": "hello", "start": 0.0, "duration": 1.0}]
        }))
        data = load_transcript(p)
        self.assertIsNotNone(data)
        self.assertEqual(data['video_id'], 'abc')
        self.assertEqual(len(data['snippets']), 1)

    def test_missing_snippets_key(self):
        p = self._write('t.txt', json.dumps({"video_id": "abc"}))
        self.assertIsNone(load_transcript(p))

    def test_invalid_json(self):
        p = self._write('t.txt', 'this is not json {{}}')
        self.assertIsNone(load_transcript(p))

    def test_empty_file(self):
        p = self._write('t.txt', '')
        self.assertIsNone(load_transcript(p))

    def test_array_json(self):
        p = self._write('t.txt', json.dumps([1, 2, 3]))
        self.assertIsNone(load_transcript(p))


# ---------------------------------------------------------------------------
# Database build + search (integration-style unit tests)
# ---------------------------------------------------------------------------

class TestBuildAndSearch(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, 'test.db')

    def _build(self, root=None, incremental=False):
        root = root or str(FIXTURES_DIR)
        args = _BuildArgs(root=root, db=self.db, incremental=incremental)
        cmd_build(args)

    def _con(self):
        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row
        return con

    def test_episodes_ingested(self):
        self._build()
        con = self._con()
        count = con.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
        self.assertGreaterEqual(count, 2)
        con.close()

    def test_episode_lines_ingested(self):
        self._build()
        con = self._con()
        count = con.execute("SELECT COUNT(*) FROM episode_lines").fetchone()[0]
        self.assertGreater(count, 0)
        con.close()

    def test_video_id_stored(self):
        self._build()
        con = self._con()
        row = con.execute(
            "SELECT * FROM episodes WHERE video_id=?", ('testVideoId1',)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row['video_id'], 'testVideoId1')
        self.assertEqual(row['series'], 'Test Series')
        con.close()

    def test_series_stored(self):
        self._build()
        con = self._con()
        series = {r['series'] for r in con.execute("SELECT series FROM episodes").fetchall()}
        self.assertIn('Test Series', series)
        self.assertIn('Another Series', series)
        con.close()

    def test_timestamps_stored(self):
        self._build()
        con = self._con()
        ep = con.execute(
            "SELECT id FROM episodes WHERE video_id='testVideoId1'"
        ).fetchone()
        lines = con.execute(
            "SELECT start, duration FROM episode_lines WHERE episode_id=? ORDER BY line_no",
            (ep['id'],)
        ).fetchall()
        self.assertAlmostEqual(lines[0]['start'], 0.199, places=3)
        con.close()

    def test_fts_search_banana(self):
        self._build()
        con = self._con()
        rows = con.execute(
            "SELECT rowid FROM episode_fts WHERE episode_fts MATCH 'banana'"
        ).fetchall()
        self.assertGreater(len(rows), 0)
        con.close()

    def test_fts_search_no_result(self):
        self._build()
        con = self._con()
        rows = con.execute(
            "SELECT rowid FROM episode_fts WHERE episode_fts MATCH 'zzz_not_in_any_transcript'"
        ).fetchall()
        self.assertEqual(len(rows), 0)
        con.close()

    def test_fts_phrase_search(self):
        self._build()
        con = self._con()
        rows = con.execute(
            "SELECT rowid FROM episode_fts WHERE episode_fts MATCH '\"spider kiss\"'"
        ).fetchall()
        self.assertGreater(len(rows), 0)
        con.close()

    def test_rebuild_is_idempotent(self):
        self._build()
        count1 = self._con().execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
        self._build()  # full rebuild again
        count2 = self._con().execute("SELECT COUNT(*) FROM episodes").fetchone()[0]
        self.assertEqual(count1, count2)

    def test_sha256_stored(self):
        self._build()
        con = self._con()
        row = con.execute(
            "SELECT sha256 FROM episodes WHERE video_id='testVideoId1'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(len(row['sha256']), 64)  # hex SHA-256
        con.close()


# ---------------------------------------------------------------------------
# Incremental build
# ---------------------------------------------------------------------------

class TestIncrementalBuild(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, 'inc.db')
        # Create a temporary transcript root with one episode
        self.root = os.path.join(self.tmp, 'transcripts')
        self.series_dir = os.path.join(self.root, 'My Series')
        os.makedirs(self.series_dir)
        self.transcript_path = os.path.join(self.series_dir, '[incVid1].txt')
        self._write_transcript(self.transcript_path, 'hello world')

    def _write_transcript(self, path, text_content):
        data = {
            "video_id": "incVid1",
            "language": "English (auto-generated)",
            "language_code": "en",
            "is_generated": True,
            "snippets": [{"text": text_content, "start": 0.0, "duration": 2.0}]
        }
        Path(path).write_text(json.dumps(data), encoding='utf-8')

    def _build(self, incremental=False):
        args = _BuildArgs(root=self.root, db=self.db, incremental=incremental)
        cmd_build(args)

    def _con(self):
        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row
        return con

    def test_incremental_skips_unchanged(self):
        self._build()  # full build
        sha_before = self._con().execute(
            "SELECT sha256 FROM episodes WHERE video_id='incVid1'"
        ).fetchone()['sha256']

        # Incremental build without changing the file should skip it
        self._build(incremental=True)
        sha_after = self._con().execute(
            "SELECT sha256 FROM episodes WHERE video_id='incVid1'"
        ).fetchone()['sha256']

        self.assertEqual(sha_before, sha_after)

    def test_incremental_reingest_changed(self):
        self._build()  # full build

        # Change the transcript
        self._write_transcript(self.transcript_path, 'completely different content')
        self._build(incremental=True)

        con = self._con()
        line = con.execute(
            "SELECT el.text FROM episode_lines el "
            "JOIN episodes e ON e.id=el.episode_id "
            "WHERE e.video_id='incVid1' ORDER BY el.line_no LIMIT 1"
        ).fetchone()
        self.assertIn('different', line['text'])
        con.close()


# ---------------------------------------------------------------------------
# extract_search_terms
# ---------------------------------------------------------------------------

class TestExtractSearchTerms(unittest.TestCase):

    def test_bare_word(self):
        self.assertEqual(extract_search_terms('banana'), 'banana')

    def test_mentions_of(self):
        result = extract_search_terms('Are there any mentions of banana in the transcripts?')
        self.assertIn('banana', result.lower())

    def test_or_variants(self):
        result = extract_search_terms('Are there any mentions of banana or bananas?')
        self.assertIn('banana', result.lower())
        self.assertIn('OR', result)

    def test_find_phrase(self):
        result = extract_search_terms('Find every time they say spider kiss')
        self.assertIn('spider', result.lower())

    def test_quoted_phrase_passthrough(self):
        result = extract_search_terms('"spider kiss"')
        self.assertEqual(result, '"spider kiss"')

    def test_did_they_ever_mention(self):
        result = extract_search_terms('Did they ever mention Bloodborne?')
        self.assertIn('bloodborne', result.lower())

    def test_talk_about(self):
        result = extract_search_terms('When do they talk about pizza?')
        self.assertIn('pizza', result.lower())

    def test_empty_question_returns_empty(self):
        result = extract_search_terms('?')
        self.assertEqual(result, '')

    def test_show_me(self):
        result = extract_search_terms('Show me banana')
        self.assertIn('banana', result.lower())

    def test_how_many_times(self):
        result = extract_search_terms('How many times did they say "oops"?')
        self.assertIn('oops', result.lower())


# ---------------------------------------------------------------------------
# parse_followup
# ---------------------------------------------------------------------------

class TestParseFollowup(unittest.TestCase):

    def test_hash_number(self):
        r = parse_followup('#2')
        self.assertIsNotNone(r)
        self.assertEqual(r['num'], 2)

    def test_hash_with_space(self):
        r = parse_followup('# 3')
        self.assertIsNotNone(r)
        self.assertEqual(r['num'], 3)

    def test_context_of(self):
        r = parse_followup('What was the context of #2?')
        self.assertIsNotNone(r)
        self.assertEqual(r['num'], 2)
        self.assertEqual(r['type'], 'context')

    def test_what_episode_was(self):
        r = parse_followup('What episode was #3?')
        self.assertIsNotNone(r)
        self.assertEqual(r['num'], 3)
        self.assertEqual(r['type'], 'episode')

    def test_what_series_was(self):
        r = parse_followup('What series was #1?')
        self.assertIsNotNone(r)
        self.assertEqual(r['num'], 1)
        self.assertEqual(r['type'], 'episode')

    def test_more_about(self):
        r = parse_followup('Tell me more about #4')
        self.assertIsNotNone(r)
        self.assertEqual(r['num'], 4)

    def test_result_number(self):
        r = parse_followup('result 5')
        self.assertIsNotNone(r)
        self.assertEqual(r['num'], 5)

    def test_not_a_followup(self):
        self.assertIsNone(parse_followup('Are there any mentions of banana?'))

    def test_plain_search_not_followup(self):
        self.assertIsNone(parse_followup('Find every time they say Bloodborne'))

    def test_youtube_link_implies_episode(self):
        r = parse_followup('Give me the YouTube link for #2')
        self.assertIsNotNone(r)
        self.assertEqual(r['num'], 2)
        self.assertEqual(r['type'], 'episode')


# ---------------------------------------------------------------------------
# run_human_search + print_human_results (integration)
# ---------------------------------------------------------------------------

class TestHumanSearch(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, 'human.db')
        args = _BuildArgs(root=str(FIXTURES_DIR), db=self.db)
        cmd_build(args)
        self.con = sqlite3.connect(self.db)
        self.con.row_factory = sqlite3.Row

    def tearDown(self):
        self.con.close()

    def test_search_returns_results(self):
        results = run_human_search(self.con, 'banana', limit=10)
        self.assertGreater(len(results), 0)

    def test_results_are_numbered(self):
        results = run_human_search(self.con, 'banana', limit=5)
        nums = [r['num'] for r in results]
        self.assertEqual(nums, list(range(1, len(nums) + 1)))

    def test_results_have_required_keys(self):
        results = run_human_search(self.con, 'banana', limit=3)
        for r in results:
            for key in ('num', 'video_id', 'series', 'line_no', 'ep_id', 'start', 'text'):
                self.assertIn(key, r)

    def test_no_results(self):
        results = run_human_search(self.con, 'zzznomatch99999', limit=10)
        self.assertEqual(results, [])

    def test_print_human_results_output(self):
        import io
        results = run_human_search(self.con, 'banana', limit=5)
        buf = io.StringIO()
        import sys as _sys
        old_stdout = _sys.stdout
        _sys.stdout = buf
        try:
            print_human_results(results, 'banana')
        finally:
            _sys.stdout = old_stdout
        output = buf.getvalue()
        self.assertIn('banana', output.lower())
        self.assertIn('# 1', output)
        self.assertIn('youtube.com', output.lower())

    def test_print_human_results_no_results(self):
        import io
        import sys as _sys
        buf = io.StringIO()
        old_stdout = _sys.stdout
        _sys.stdout = buf
        try:
            print_human_results([], 'zzznomatch')
        finally:
            _sys.stdout = old_stdout
        self.assertIn("couldn't find", buf.getvalue().lower())


if __name__ == '__main__':
    unittest.main()
