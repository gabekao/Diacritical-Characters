from __future__ import annotations

from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from diacritical_characters import corpus


class NormalizationTests(unittest.TestCase):
    def test_normalize_token_basic(self) -> None:
        value = corpus.normalize_token("  École!! ")
        self.assertIsNotNone(value)
        assert value is not None
        self.assertEqual(value.word, "école")
        self.assertEqual(value.word_casefold, "école")
        self.assertTrue(value.has_diacritic)
        self.assertTrue(value.has_combining_mark)

    def test_normalize_token_strips_edges_preserves_internal(self) -> None:
        value = corpus.normalize_token("'meme-like-'")
        self.assertIsNotNone(value)
        assert value is not None
        self.assertEqual(value.word, "meme-like")

    def test_normalize_token_rejects_non_alpha(self) -> None:
        self.assertIsNone(corpus.normalize_token("!!!"))

    def test_casefold_collision(self) -> None:
        left = corpus.normalize_token("Straße")
        right = corpus.normalize_token("strasse")
        self.assertIsNotNone(left)
        self.assertIsNotNone(right)
        assert left is not None and right is not None
        self.assertEqual(left.word_casefold, right.word_casefold)


class ParserTests(unittest.TestCase):
    def test_hunspell_parser(self) -> None:
        lines = ["3", "naive/AB", "cafe", "#comment"]
        words = [entry.word for entry in corpus.parse_hunspell_stream(lines)]
        self.assertEqual(words, ["naive", "cafe"])

    def test_freq_tsv_parser(self) -> None:
        lines = ["the\t10\t8", "cafe\t3\t2"]
        entries = list(corpus.parse_freq_tsv_stream(lines))
        self.assertEqual(entries[0].word, "the")
        self.assertEqual(entries[0].raw_freq, 10.0)
        self.assertEqual(entries[0].rank, 1)

    def test_wiktionary_html_parser(self) -> None:
        html = '<a href="/wiki/doge">doge</a><a href="/wiki/Category:Foo">skip</a>'
        words = [entry.word for entry in corpus.parse_wiktionary_html(html)]
        self.assertEqual(words, ["doge"])

    def test_wiktextract_line_parser(self) -> None:
        line = '{"word": "meme", "lang_code": "en", "lang": "English"}'
        entry = corpus._parse_wiktextract_line(line)
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry.word, "meme")


class FilenameTests(unittest.TestCase):
    def test_safe_filename_from_url_strips_windows_invalid_chars(self) -> None:
        value = corpus.safe_filename_from_url("https://en.wiktionary.org/wiki/Category:en:Internet_memes")
        self.assertEqual(value, "Category_en_Internet_memes")


class QueryTests(unittest.TestCase):
    def test_store_query_filters_by_length_prefix_and_letters(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "corpus.sqlite3"
            with closing(sqlite3.connect(db_path)) as conn:
                corpus.initialize_database(conn)
                conn.execute(
                    """
                    INSERT INTO words (
                        word, word_casefold, length, initial, ascii_mask, has_diacritic, has_combining_mark, first_seen_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("meme", "meme", 4, "m", (1 << (ord("m") - 97)) | (1 << (ord("e") - 97)), 0, 0, "test"),
                )
                conn.execute(
                    """
                    INSERT INTO words (
                        word, word_casefold, length, initial, ascii_mask, has_diacritic, has_combining_mark, first_seen_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "code",
                        "code",
                        4,
                        "c",
                        (1 << (ord("c") - 97)) | (1 << (ord("o") - 97)) | (1 << (ord("d") - 97)) | (1 << (ord("e") - 97)),
                        0,
                        0,
                        "test",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO words (
                        word, word_casefold, length, initial, ascii_mask, has_diacritic, has_combining_mark, first_seen_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "me-me",
                        "me-me",
                        5,
                        "m",
                        (1 << (ord("m") - 97)) | (1 << (ord("e") - 97)),
                        0,
                        0,
                        "test",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO words (
                        word, word_casefold, length, initial, ascii_mask, has_diacritic, has_combining_mark, first_seen_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "me'me",
                        "me'me",
                        5,
                        "m",
                        (1 << (ord("m") - 97)) | (1 << (ord("e") - 97)),
                        0,
                        0,
                        "test",
                    ),
                )
                conn.commit()

            store = corpus.CorpusStore(db_path)
            values = store.suggest_words(target_length=4, prefix="m", limit=20, allowed_letters="acdehimortuvx")
            self.assertEqual(values, ["meme"])
            values_len5 = store.suggest_words(target_length=5, prefix="m", limit=20, allowed_letters="acdehimortuvx")
            self.assertEqual(values_len5, [])


class BuildPipelineTests(unittest.TestCase):
    def _fake_entries(self, *_args, **_kwargs):
        yield corpus.WordEntry("meme")
        yield corpus.WordEntry("doge")

    def test_incremental_skips_unchanged_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "data" / "corpus.sqlite3"
            download_dir = Path(temp_dir) / "downloads"
            with patch("diacritical_characters.corpus._iter_source_entries", self._fake_entries):
                first = corpus.build_data(
                    db_path=db_path,
                    download_dir=download_dir,
                    full_rebuild=True,
                    workers=1,
                    min_success=1,
                    include_sources=[corpus.SOURCE_NLTK_WORDS],
                )
                second = corpus.build_data(
                    db_path=db_path,
                    download_dir=download_dir,
                    full_rebuild=False,
                    workers=1,
                    min_success=1,
                    include_sources=[corpus.SOURCE_NLTK_WORDS],
                )

            self.assertEqual(first.successful_sources, 1)
            self.assertEqual(second.skipped_sources, 1)

    def test_min_success_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "data" / "corpus.sqlite3"
            download_dir = Path(temp_dir) / "downloads"
            with patch("diacritical_characters.corpus._iter_source_entries", self._fake_entries):
                with self.assertRaises(RuntimeError):
                    corpus.build_data(
                        db_path=db_path,
                        download_dir=download_dir,
                        full_rebuild=True,
                        workers=1,
                        min_success=2,
                        include_sources=[corpus.SOURCE_NLTK_WORDS],
                    )

    def test_smoke_build_and_query(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "data" / "corpus.sqlite3"
            download_dir = Path(temp_dir) / "downloads"
            with patch("diacritical_characters.corpus._iter_source_entries", self._fake_entries):
                result = corpus.build_data(
                    db_path=db_path,
                    download_dir=download_dir,
                    full_rebuild=True,
                    workers=1,
                    min_success=1,
                    include_sources=[corpus.SOURCE_NLTK_WORDS],
                )
            self.assertGreater(result.total_words, 0)
            store = corpus.CorpusStore(db_path)
            suggestions = store.suggest_words(
                target_length=4,
                prefix="m",
                limit=10,
                allowed_letters="acdehimortuvx",
            )
            self.assertEqual(suggestions, ["meme"])


if __name__ == "__main__":
    unittest.main()
