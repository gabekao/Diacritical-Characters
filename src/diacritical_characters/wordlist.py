"""Bundled suggestion word list.

The apps only suggest words spelled entirely with letters that have a
combining superscript form. Out of the multi-million-word corpus that is a
manageable number, so instead of querying SQLite at runtime we ship them as
plain text files, ranked most-common-first, and filter in memory:

- ``words.txt``: words using only the 13 well-supported letters.
- ``words_extended.txt``: every word in ``words.txt`` plus the most common
  words that need one of the 10 extra letters (b f g k l n p s w z), in one
  ranking. Most English words qualify, so the extras are capped.

Regenerate both files from a built corpus with:

    python -m diacritical_characters.corpus export-wordlist
"""

from __future__ import annotations

from contextlib import closing
from functools import lru_cache
from pathlib import Path
import sqlite3
import sys
from typing import Iterable

SUPERSCRIPT_LETTERS = "acdehimortuvx"
EXTENDED_LETTERS = "abcdefghiklmnoprstuvwxz"

# How many words needing an extra letter go into words_extended.txt. Around
# this rank the list is still real words; it keeps the file near 1 MB.
EXTRA_WORD_LIMIT = 100_000

WORDLIST_FILES = {False: "words.txt", True: "words_extended.txt"}

# Sources that are curated dictionaries (as opposed to raw web frequency lists).
_DICTIONARY_SOURCES = (
    "hunspell_en_us",
    "hunspell_en_gb",
    "nltk_words",
    "wiktextract_raw",
    "wiktionary_internet_lists",
)
_ZIPF_SOURCE = "wordfreq_en"


def data_dir() -> Path:
    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
        return base / "diacritical_characters" / "data"
    return Path(__file__).resolve().parent / "data"


def wordlist_path(extended: bool = False) -> Path:
    return data_dir() / WORDLIST_FILES[extended]


class WordList:
    """Words grouped by length, each group ranked most-common-first."""

    def __init__(self, words: Iterable[str]) -> None:
        self._by_length: dict[int, list[str]] = {}
        for word in words:
            self._by_length.setdefault(len(word), []).append(word)
        self._sorted_cache: dict[int, list[str]] = {}

    def __len__(self) -> int:
        return sum(len(group) for group in self._by_length.values())

    def lengths(self) -> list[int]:
        return sorted(self._by_length)

    def words_of_length(self, length: int, *, alphabetical: bool = False) -> list[str]:
        ranked = self._by_length.get(length, [])
        if not alphabetical:
            return ranked
        cached = self._sorted_cache.get(length)
        if cached is None:
            cached = self._sorted_cache[length] = sorted(ranked)
        return cached

    def suggest(
        self,
        length: int,
        query: str = "",
        *,
        alphabetical: bool = False,
        exclude: Iterable[str] = (),
    ) -> list[str]:
        """Words of ``length`` matching ``query``.

        Words starting with the query come first, followed by words that
        merely contain it. Within each group the list order is preserved.
        """
        words = self.words_of_length(length, alphabetical=alphabetical)
        excluded = set(exclude)
        if excluded:
            words = [word for word in words if word not in excluded]
        query = query.strip().casefold()
        if not query:
            return list(words)
        starts = [word for word in words if word.startswith(query)]
        contains = [word for word in words if query in word and not word.startswith(query)]
        return starts + contains


@lru_cache(maxsize=2)
def load_wordlist(extended: bool = False) -> WordList:
    path = wordlist_path(extended)
    with path.open("r", encoding="utf-8") as handle:
        return WordList(line.strip() for line in handle if line.strip())


def export_wordlist(db_path: Path, output_dir: Path | None = None) -> tuple[int, int]:
    """Write both ranked suggestion lists from a built corpus database.

    A word is kept when it uses only superscript-capable letters and is either
    in a curated dictionary or has a wordfreq frequency. Words that appear only
    in raw web frequency lists are mostly noise ("aaaaah", typos) and dropped.

    Returns the word counts of ``words.txt`` and ``words_extended.txt``.
    """
    output_dir = output_dir or (Path(__file__).resolve().parent / "data")
    disallowed_mask = 0
    for offset in range(26):
        if chr(ord("a") + offset) not in EXTENDED_LETTERS:
            disallowed_mask |= 1 << offset

    dictionary_marks = ",".join("?" for _ in _DICTIONARY_SOURCES)
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute(
            f"""
            SELECT w.word_casefold,
                   MAX(CASE WHEN ws.source_id = ? THEN ws.raw_freq END) AS zipf,
                   SUM(CASE WHEN ws.source_id IN ({dictionary_marks}) THEN 1 ELSE 0 END) AS dictionaries,
                   COUNT(*) AS sources
            FROM words w
            JOIN word_source ws ON ws.word_id = w.id
            WHERE (w.ascii_mask & ?) = 0
              AND w.length > 0
              AND w.word_casefold NOT GLOB '*[^a-z]*'
            GROUP BY w.id
            """,
            (_ZIPF_SOURCE, *_DICTIONARY_SOURCES, disallowed_mask),
        ).fetchall()

    scored: list[tuple[float, str]] = []
    for word, zipf, dictionaries, sources in rows:
        if not dictionaries and zipf is None:
            continue
        score = float(zipf or 0.0) + (1.5 if dictionaries else 0.0) + 0.25 * sources
        scored.append((score, word))
    scored.sort(key=lambda pair: (-pair[0], pair[1]))

    basic_letters = set(SUPERSCRIPT_LETTERS)
    basic: list[str] = []
    extended: list[str] = []
    extras = 0
    for _score, word in scored:
        if set(word) <= basic_letters:
            basic.append(word)
            extended.append(word)
        elif extras < EXTRA_WORD_LIMIT:
            extended.append(word)
            extras += 1

    output_dir.mkdir(parents=True, exist_ok=True)
    for words, is_extended in ((basic, False), (extended, True)):
        with (output_dir / WORDLIST_FILES[is_extended]).open("w", encoding="utf-8", newline="\n") as handle:
            handle.writelines(word + "\n" for word in words)
    load_wordlist.cache_clear()
    return len(basic), len(extended)
