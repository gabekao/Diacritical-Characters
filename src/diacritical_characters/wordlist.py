"""Bundled suggestion word list.

The GUI only ever suggests words spelled entirely with letters that have a
combining superscript form (13 letters). Out of the multi-million-word corpus
that is a few tens of thousands of words, so instead of querying SQLite at
runtime we ship them as a plain text file, ranked most-common-first, and
filter in memory.

Regenerate the file from a built corpus with:

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

# Sources that are curated dictionaries (as opposed to raw web frequency lists).
_DICTIONARY_SOURCES = (
    "hunspell_en_us",
    "hunspell_en_gb",
    "nltk_words",
    "wiktextract_raw",
    "wiktionary_internet_lists",
)
_ZIPF_SOURCE = "wordfreq_en"


def wordlist_path() -> Path:
    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
        return base / "diacritical_characters" / "data" / "words.txt"
    return Path(__file__).resolve().parent / "data" / "words.txt"


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


@lru_cache(maxsize=1)
def load_wordlist() -> WordList:
    path = wordlist_path()
    with path.open("r", encoding="utf-8") as handle:
        return WordList(line.strip() for line in handle if line.strip())


def export_wordlist(db_path: Path, output_path: Path | None = None) -> int:
    """Write the ranked suggestion list from a built corpus database.

    A word is kept when it uses only superscript-capable letters and is either
    in a curated dictionary or has a wordfreq frequency. Words that appear only
    in raw web frequency lists are mostly noise ("aaaaah", typos) and dropped.
    """
    output_path = output_path or (Path(__file__).resolve().parent / "data" / "words.txt")
    disallowed_mask = 0
    for offset in range(26):
        if chr(ord("a") + offset) not in SUPERSCRIPT_LETTERS:
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

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for _score, word in scored:
            handle.write(word + "\n")
    load_wordlist.cache_clear()
    return len(scored)
