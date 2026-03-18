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


if __name__ == '__main__':
    unittest.main()
