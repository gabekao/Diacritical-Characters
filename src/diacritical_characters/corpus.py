from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
import gzip
import hashlib
from html import unescape
import importlib.metadata
import json
import lzma
from pathlib import Path
from queue import Empty, Queue
import re
import sqlite3
import threading
import time
from typing import Callable, Iterable, Iterator
import unicodedata
import urllib.parse
import urllib.request

from tqdm import tqdm

USER_AGENT = "diacritical-characters-corpus-builder/1.0"
DEFAULT_WORKERS = 8
DEFAULT_MIN_SUCCESS = 6
WORD_FREQ_LIMIT = 400_000
DOWNLOAD_CHUNK_SIZE = 128 * 1024
PROCESS_BATCH_SIZE = 2000
PROCESS_QUEUE_MULTIPLIER = 4

SOURCE_NLTK_WORDS = "nltk_words"
SOURCE_WORDFREQ_EN = "wordfreq_en"
SOURCE_HUNSPELL_EN_US = "hunspell_en_us"
SOURCE_HUNSPELL_EN_GB = "hunspell_en_gb"
SOURCE_WIKTEXTRACT_RAW = "wiktextract_raw"
SOURCE_WIKTIONARY_INTERNET = "wiktionary_internet_lists"
SOURCE_CCNET_EN_FREQ = "ccnet_en_freq"
SOURCE_OPENWEBTEXT_FREQ = "openwebtext_freq"

SOURCE_IDS = (
    SOURCE_NLTK_WORDS,
    SOURCE_WORDFREQ_EN,
    SOURCE_HUNSPELL_EN_US,
    SOURCE_HUNSPELL_EN_GB,
    SOURCE_WIKTEXTRACT_RAW,
    SOURCE_WIKTIONARY_INTERNET,
    SOURCE_CCNET_EN_FREQ,
    SOURCE_OPENWEBTEXT_FREQ,
)


@dataclass(frozen=True)
class BuildProgress:
    source_id: str
    phase: str
    status: str
    bytes_downloaded: int = 0
    bytes_total: int | None = None
    records: int = 0
    elapsed_seconds: float = 0.0
    message: str = ""


@dataclass(frozen=True)
class SourceResult:
    source_id: str
    status: str
    records: int = 0
    error: str = ""
    etag: str = ""
    last_modified: str = ""
    sha256: str = ""
    bytes_downloaded: int = 0
    elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class BuildResult:
    db_path: Path
    total_words: int
    total_word_sources: int
    successful_sources: int
    skipped_sources: int
    failed_sources: int
    source_results: tuple[SourceResult, ...]


@dataclass(frozen=True)
class WordEntry:
    word: str
    raw_freq: float | None = None
    rank: int | None = None
    meta: dict[str, object] | None = None


@dataclass(frozen=True)
class NormalizedWord:
    word: str
    word_casefold: str
    length: int
    initial: str
    ascii_mask: int
    has_diacritic: bool
    has_combining_mark: bool


@dataclass(frozen=True)
class SourceDefinition:
    source_id: str
    urls: tuple[str, ...] = ()


@dataclass(frozen=True)
class DownloadTask:
    source_id: str
    url: str
    destination: Path


@dataclass(frozen=True)
class DownloadResult:
    source_id: str
    url: str
    destination: Path
    bytes_downloaded: int
    sha256: str
    etag: str
    last_modified: str
    bytes_total: int | None
    error: str = ""


ProgressCallback = Callable[[BuildProgress], None]
_WINDOWS_INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_db_path(root: Path | None = None) -> Path:
    return (root or project_root()) / "data" / "corpus.sqlite3"


def default_download_directory(root: Path | None = None) -> Path:
    return (root or project_root()) / "downloads"


def source_registry() -> dict[str, SourceDefinition]:
    return {
        SOURCE_NLTK_WORDS: SourceDefinition(SOURCE_NLTK_WORDS),
        SOURCE_WORDFREQ_EN: SourceDefinition(SOURCE_WORDFREQ_EN),
        SOURCE_HUNSPELL_EN_US: SourceDefinition(
            SOURCE_HUNSPELL_EN_US,
            urls=("https://raw.githubusercontent.com/LibreOffice/dictionaries/master/en/en_US.dic",),
        ),
        SOURCE_HUNSPELL_EN_GB: SourceDefinition(
            SOURCE_HUNSPELL_EN_GB,
            urls=("https://raw.githubusercontent.com/LibreOffice/dictionaries/master/en/en_GB.dic",),
        ),
        SOURCE_WIKTEXTRACT_RAW: SourceDefinition(
            SOURCE_WIKTEXTRACT_RAW,
            urls=("https://kaikki.org/dictionary/raw-wiktextract-data.jsonl.gz",),
        ),
        SOURCE_WIKTIONARY_INTERNET: SourceDefinition(
            SOURCE_WIKTIONARY_INTERNET,
            urls=(
                "https://en.wiktionary.org/wiki/Category:en:Internet_memes",
                "https://en.wiktionary.org/wiki/Appendix:English_internet_slang",
            ),
        ),
        SOURCE_CCNET_EN_FREQ: SourceDefinition(
            SOURCE_CCNET_EN_FREQ,
            urls=("https://ssharoff.github.io/frqc/ccnet-en-200-clean2-biwt.tsv.xz",),
        ),
        SOURCE_OPENWEBTEXT_FREQ: SourceDefinition(
            SOURCE_OPENWEBTEXT_FREQ,
            urls=("https://ssharoff.github.io/frqc/openwebtext-clean2.tsv.xz",),
        ),
    }


def available_source_ids() -> tuple[str, ...]:
    return SOURCE_IDS


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _emit(callback: ProgressCallback | None, progress: BuildProgress) -> None:
    if callback is not None:
        callback(progress)


def initialize_database(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            url TEXT,
            etag TEXT,
            last_modified TEXT,
            sha256 TEXT,
            status TEXT NOT NULL,
            error TEXT,
            updated_at TEXT NOT NULL,
            records INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS words (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL,
            word_casefold TEXT NOT NULL UNIQUE,
            length INTEGER NOT NULL,
            initial TEXT NOT NULL,
            ascii_mask INTEGER NOT NULL,
            has_diacritic INTEGER NOT NULL,
            has_combining_mark INTEGER NOT NULL,
            first_seen_source TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS word_source (
            word_id INTEGER NOT NULL,
            source_id TEXT NOT NULL,
            raw_freq REAL,
            rank INTEGER,
            meta_json TEXT,
            PRIMARY KEY (word_id, source_id),
            FOREIGN KEY (word_id) REFERENCES words(id) ON DELETE CASCADE,
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_words_casefold ON words(word_casefold);
        CREATE INDEX IF NOT EXISTS idx_words_length_initial ON words(length, initial, word_casefold);
        CREATE INDEX IF NOT EXISTS idx_words_ascii_mask ON words(ascii_mask);
        CREATE INDEX IF NOT EXISTS idx_words_diacritics ON words(has_diacritic, has_combining_mark);
        CREATE INDEX IF NOT EXISTS idx_word_source_source ON word_source(source_id);
        """
    )
    conn.commit()


def _load_source_rows(conn: sqlite3.Connection) -> dict[str, dict[str, object]]:
    rows = conn.execute(
        "SELECT source_id, etag, last_modified, sha256, status, records FROM sources"
    ).fetchall()
    data: dict[str, dict[str, object]] = {}
    for source_id, etag, last_modified, sha256_value, status, records in rows:
        data[source_id] = {
            "etag": etag or "",
            "last_modified": last_modified or "",
            "sha256": sha256_value or "",
            "status": status or "",
            "records": int(records or 0),
        }
    return data


def _upsert_source_row(
    conn: sqlite3.Connection,
    source_id: str,
    urls: Iterable[str],
    status: str,
    records: int,
    *,
    etag: str = "",
    last_modified: str = "",
    sha256_value: str = "",
    error: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO sources (
            source_id, url, etag, last_modified, sha256, status, error, updated_at, records
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            url = excluded.url,
            etag = excluded.etag,
            last_modified = excluded.last_modified,
            sha256 = excluded.sha256,
            status = excluded.status,
            error = excluded.error,
            updated_at = excluded.updated_at,
            records = excluded.records
        """,
        (
            source_id,
            "\n".join(urls),
            etag,
            last_modified,
            sha256_value,
            status,
            error,
            _utc_now(),
            records,
        ),
    )


def _json_token(values: dict[str, str]) -> str:
    return json.dumps(dict(sorted(values.items())), ensure_ascii=False, separators=(",", ":"))


def _package_version_token(package_name: str) -> str:
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return "missing"


def _head_request(url: str) -> tuple[str, str, int | None]:
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=45) as response:
        headers = response.headers
        etag = headers.get("ETag", "") or ""
        last_modified = headers.get("Last-Modified", "") or ""
        length_header = headers.get("Content-Length")
        bytes_total = int(length_header) if length_header and length_header.isdigit() else None
        return etag, last_modified, bytes_total


def _discover_remote_tokens(urls: Iterable[str]) -> tuple[str, str, dict[str, int | None]]:
    etags: dict[str, str] = {}
    last_modified: dict[str, str] = {}
    sizes: dict[str, int | None] = {}
    for url in urls:
        key = Path(urllib.parse.urlparse(url).path).name or url
        try:
            etag, lm, size = _head_request(url)
        except Exception:
            etag, lm, size = "", "", None
        etags[key] = etag
        last_modified[key] = lm
        sizes[key] = size
    return _json_token(etags), _json_token(last_modified), sizes


def safe_filename_from_url(url: str) -> str:
    path_name = Path(urllib.parse.urlparse(url).path).name
    candidate = path_name or "download"
    candidate = _WINDOWS_INVALID_FILENAME_CHARS.sub("_", candidate)
    candidate = candidate.rstrip(". ").strip()
    if not candidate:
        candidate = "download"
    return candidate


def normalize_token(token: str) -> NormalizedWord | None:
    if not token:
        return None

    raw = unicodedata.normalize("NFC", token.strip())
    if not raw:
        return None

    start = 0
    end = len(raw)
    while start < end and not raw[start].isalpha():
        start += 1
    while end > start and not raw[end - 1].isalpha():
        end -= 1
    if start >= end:
        return None

    trimmed = raw[start:end]
    cleaned_chars: list[str] = []
    for char in trimmed:
        if char.isalpha() or char in {"'", "-"}:
            cleaned_chars.append(char)
    candidate = "".join(cleaned_chars).strip("'-")
    if not candidate:
        return None
    if not any(char.isalpha() for char in candidate):
        return None

    word = unicodedata.normalize("NFC", candidate.lower())
    casefold = word.casefold()
    if not casefold:
        return None

    ascii_mask = 0
    for char in casefold:
        if "a" <= char <= "z":
            ascii_mask |= 1 << (ord(char) - ord("a"))

    nfd = unicodedata.normalize("NFD", word)
    has_combining_mark = any(unicodedata.category(char) == "Mn" for char in nfd)
    has_diacritic = has_combining_mark or any(ord(char) > 127 for char in word if char.isalpha())

    return NormalizedWord(
        word=word,
        word_casefold=casefold,
        length=len(word),
        initial=casefold[:1],
        ascii_mask=ascii_mask,
        has_diacritic=has_diacritic,
        has_combining_mark=has_combining_mark,
    )


def parse_hunspell_stream(lines: Iterable[str]) -> Iterator[WordEntry]:
    first_line = True
    for line in lines:
        text = line.strip()
        if not text:
            continue
        if first_line:
            first_line = False
            if text.isdigit():
                continue
        first_line = False
        if text.startswith("#"):
            continue
        token = text.split("/", 1)[0].strip()
        if token:
            yield WordEntry(word=token)


_WIKTIONARY_LINK_RE = re.compile(r'href="/wiki/([^"#?]+)"')


def parse_wiktionary_html(html_text: str) -> Iterator[WordEntry]:
    seen: set[str] = set()
    for match in _WIKTIONARY_LINK_RE.finditer(html_text):
        raw = urllib.parse.unquote(match.group(1))
        if ":" in raw:
            continue
        token = unescape(raw.replace("_", " ")).strip()
        if token and token not in seen:
            seen.add(token)
            yield WordEntry(word=token)


def parse_freq_tsv_stream(lines: Iterable[str]) -> Iterator[WordEntry]:
    for rank, line in enumerate(lines, start=1):
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        parts = text.split("\t")
        if len(parts) < 2:
            continue
        raw_freq: float | None = None
        try:
            raw_freq = float(parts[1])
        except ValueError:
            raw_freq = None
        meta: dict[str, object] = {}
        if len(parts) > 2:
            try:
                meta["adjusted_freq"] = float(parts[2])
            except ValueError:
                meta["adjusted_freq"] = parts[2]
        yield WordEntry(word=parts[0], raw_freq=raw_freq, rank=rank, meta=meta or None)


def _parse_wiktextract_line(line: str) -> WordEntry | None:
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    word = data.get("word")
    if not isinstance(word, str) or not word.strip():
        return None
    lang_code = data.get("lang_code")
    lang_name = data.get("lang")
    if lang_code != "en" and lang_name != "English":
        return None
    return WordEntry(word=word)


def _download_file(
    task: DownloadTask,
    progress_callback: Callable[[str, int, int | None], None] | None = None,
) -> DownloadResult:
    task.destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = task.destination.with_suffix(task.destination.suffix + ".part")
    sha = hashlib.sha256()
    bytes_downloaded = 0
    last_emit = 0.0
    etag = ""
    last_modified = ""
    bytes_total: int | None = None

    request = urllib.request.Request(task.url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            headers = response.headers
            etag = headers.get("ETag", "") or ""
            last_modified = headers.get("Last-Modified", "") or ""
            length_header = headers.get("Content-Length")
            if length_header and length_header.isdigit():
                bytes_total = int(length_header)

            with temp_path.open("wb") as handle:
                while True:
                    chunk = response.read(DOWNLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    handle.write(chunk)
                    sha.update(chunk)
                    bytes_downloaded += len(chunk)
                    if progress_callback is not None:
                        now = time.perf_counter()
                        if now - last_emit >= 0.15:
                            progress_callback(task.source_id, bytes_downloaded, bytes_total)
                            last_emit = now
        temp_path.replace(task.destination)
        if progress_callback is not None:
            progress_callback(task.source_id, bytes_downloaded, bytes_total)
        return DownloadResult(
            source_id=task.source_id,
            url=task.url,
            destination=task.destination,
            bytes_downloaded=bytes_downloaded,
            sha256=sha.hexdigest(),
            etag=etag,
            last_modified=last_modified,
            bytes_total=bytes_total,
        )
    except Exception as exc:  # noqa: BLE001
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        return DownloadResult(
            source_id=task.source_id,
            url=task.url,
            destination=task.destination,
            bytes_downloaded=bytes_downloaded,
            sha256="",
            etag=etag,
            last_modified=last_modified,
            bytes_total=bytes_total,
            error=f"{type(exc).__name__}: {exc}",
        )


def _lookup_or_create_word_id(
    conn: sqlite3.Connection,
    cache: dict[str, int],
    normalized: NormalizedWord,
    source_id: str,
) -> int:
    cached = cache.get(normalized.word_casefold)
    if cached is not None:
        return cached

    conn.execute(
        """
        INSERT INTO words (
            word, word_casefold, length, initial, ascii_mask, has_diacritic, has_combining_mark, first_seen_source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(word_casefold) DO NOTHING
        """,
        (
            normalized.word,
            normalized.word_casefold,
            normalized.length,
            normalized.initial,
            normalized.ascii_mask,
            int(normalized.has_diacritic),
            int(normalized.has_combining_mark),
            source_id,
        ),
    )
    row = conn.execute(
        "SELECT id FROM words WHERE word_casefold = ?",
        (normalized.word_casefold,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Could not resolve word id for {normalized.word_casefold}")
    word_id = int(row[0])
    cache[normalized.word_casefold] = word_id
    return word_id


def _iter_source_entries(source_id: str, downloaded_files: list[Path]) -> Iterator[WordEntry]:
    if source_id == SOURCE_NLTK_WORDS:
        from nltk.corpus import words

        for token in words.words():
            text = str(token).strip()
            if text:
                yield WordEntry(word=text)
        return

    if source_id == SOURCE_WORDFREQ_EN:
        from wordfreq import top_n_list, zipf_frequency

        words_list = top_n_list("en", WORD_FREQ_LIMIT)
        for rank, token in enumerate(words_list, start=1):
            freq = zipf_frequency(token, "en")
            yield WordEntry(word=str(token), raw_freq=freq, rank=rank)
        return

    if source_id in {SOURCE_HUNSPELL_EN_US, SOURCE_HUNSPELL_EN_GB}:
        if not downloaded_files:
            return
        with downloaded_files[0].open("r", encoding="utf-8", errors="ignore") as handle:
            yield from parse_hunspell_stream(handle)
        return

    if source_id == SOURCE_WIKTEXTRACT_RAW:
        if not downloaded_files:
            return
        with gzip.open(downloaded_files[0], "rt", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                entry = _parse_wiktextract_line(line)
                if entry is not None:
                    yield entry
        return

    if source_id == SOURCE_WIKTIONARY_INTERNET:
        for path in sorted(downloaded_files):
            html_text = path.read_text(encoding="utf-8", errors="ignore")
            yield from parse_wiktionary_html(html_text)
        return

    if source_id in {SOURCE_CCNET_EN_FREQ, SOURCE_OPENWEBTEXT_FREQ}:
        if not downloaded_files:
            return
        with lzma.open(downloaded_files[0], "rt", encoding="utf-8", errors="ignore") as handle:
            yield from parse_freq_tsv_stream(handle)
        return

    raise ValueError(f"Unsupported source: {source_id}")

def _parse_source_to_queue(
    source_id: str,
    downloaded_files: list[Path],
    output_queue: Queue,
    progress_callback: ProgressCallback | None,
    batch_size: int = PROCESS_BATCH_SIZE,
) -> None:
    started = time.perf_counter()
    parsed_count = 0
    output_queue.put(("start", source_id, None))
    _emit(
        progress_callback,
        BuildProgress(source_id=source_id, phase="parse", status="running", records=0),
    )

    batch: list[tuple[NormalizedWord, float | None, int | None, str | None]] = []
    try:
        for entry in _iter_source_entries(source_id, downloaded_files):
            normalized = normalize_token(entry.word)
            if normalized is None:
                continue
            meta_json = json.dumps(entry.meta, ensure_ascii=False) if entry.meta else None
            batch.append((normalized, entry.raw_freq, entry.rank, meta_json))
            parsed_count += 1

            if len(batch) >= batch_size:
                output_queue.put(("batch", source_id, batch))
                batch = []
                _emit(
                    progress_callback,
                    BuildProgress(
                        source_id=source_id,
                        phase="parse",
                        status="running",
                        records=parsed_count,
                        elapsed_seconds=time.perf_counter() - started,
                    ),
                )

        if batch:
            output_queue.put(("batch", source_id, batch))

        output_queue.put(("parse_done", source_id, parsed_count))
        _emit(
            progress_callback,
            BuildProgress(
                source_id=source_id,
                phase="parse",
                status="done",
                records=parsed_count,
                elapsed_seconds=time.perf_counter() - started,
            ),
        )
    except Exception as exc:  # noqa: BLE001
        message = f"{type(exc).__name__}: {exc}"
        output_queue.put(("parse_error", source_id, message))
        _emit(
            progress_callback,
            BuildProgress(
                source_id=source_id,
                phase="parse",
                status="failed",
                records=parsed_count,
                elapsed_seconds=time.perf_counter() - started,
                message=message,
            ),
        )


def _process_sources_concurrently(
    *,
    db_path: Path,
    sources: list[SourceDefinition],
    artifact_paths: dict[str, list[Path]],
    workers: int,
    progress_callback: ProgressCallback | None,
) -> tuple[dict[str, int], dict[str, str], dict[str, float]]:
    records_by_source: dict[str, int] = {source.source_id: 0 for source in sources}
    errors_by_source: dict[str, str] = {}
    elapsed_by_source: dict[str, float] = {}
    if not sources:
        return records_by_source, errors_by_source, elapsed_by_source

    queue_size = max(16, max(1, workers) * PROCESS_QUEUE_MULTIPLIER)
    output_queue: Queue = Queue(maxsize=queue_size)
    source_ids = [source.source_id for source in sources]

    with closing(sqlite3.connect(db_path, timeout=60)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 60000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        cache: dict[str, int] = {}
        source_started: dict[str, float] = {}
        source_completed: set[str] = set()

        def ensure_started(source_id: str) -> None:
            if source_id in source_started:
                return
            source_started[source_id] = time.perf_counter()
            conn.execute("DELETE FROM word_source WHERE source_id = ?", (source_id,))
            conn.commit()
            _emit(
                progress_callback,
                BuildProgress(source_id=source_id, phase="upsert", status="running", records=0),
            )

        with ThreadPoolExecutor(max_workers=max(1, workers)) as parser_executor:
            futures = {
                source.source_id: parser_executor.submit(
                    _parse_source_to_queue,
                    source.source_id,
                    artifact_paths[source.source_id],
                    output_queue,
                    progress_callback,
                )
                for source in sources
            }

            while len(source_completed) < len(source_ids):
                try:
                    event, source_id, payload = output_queue.get(timeout=0.25)
                except Empty:
                    for source_id, future in futures.items():
                        if source_id in source_completed or not future.done():
                            continue
                        error = future.exception()
                        if error is None:
                            continue
                        ensure_started(source_id)
                        message = f"{type(error).__name__}: {error}"
                        errors_by_source[source_id] = message
                        conn.execute("DELETE FROM word_source WHERE source_id = ?", (source_id,))
                        conn.commit()
                        records_by_source[source_id] = 0
                        started = source_started.get(source_id, time.perf_counter())
                        elapsed_by_source[source_id] = time.perf_counter() - started
                        source_completed.add(source_id)
                        _emit(
                            progress_callback,
                            BuildProgress(
                                source_id=source_id,
                                phase="upsert",
                                status="failed",
                                records=0,
                                elapsed_seconds=elapsed_by_source[source_id],
                                message=message,
                            ),
                        )
                    continue
                ensure_started(source_id)

                if event == "start":
                    continue

                if event == "batch":
                    rows = payload
                    if not isinstance(rows, list):
                        continue
                    for normalized, raw_freq, rank, meta_json in rows:
                        word_id = _lookup_or_create_word_id(conn, cache, normalized, source_id)
                        conn.execute(
                            """
                            INSERT INTO word_source (word_id, source_id, raw_freq, rank, meta_json)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(word_id, source_id) DO UPDATE SET
                                raw_freq = excluded.raw_freq,
                                rank = excluded.rank,
                                meta_json = excluded.meta_json
                            """,
                            (word_id, source_id, raw_freq, rank, meta_json),
                        )
                    records_by_source[source_id] += len(rows)
                    conn.commit()
                    started = source_started.get(source_id, time.perf_counter())
                    _emit(
                        progress_callback,
                        BuildProgress(
                            source_id=source_id,
                            phase="upsert",
                            status="running",
                            records=records_by_source[source_id],
                            elapsed_seconds=time.perf_counter() - started,
                        ),
                    )
                    continue

                if event == "parse_error":
                    message = str(payload)
                    errors_by_source[source_id] = message
                    conn.execute("DELETE FROM word_source WHERE source_id = ?", (source_id,))
                    conn.commit()
                    records_by_source[source_id] = 0
                    started = source_started.get(source_id, time.perf_counter())
                    elapsed_by_source[source_id] = time.perf_counter() - started
                    source_completed.add(source_id)
                    _emit(
                        progress_callback,
                        BuildProgress(
                            source_id=source_id,
                            phase="upsert",
                            status="failed",
                            records=0,
                            elapsed_seconds=elapsed_by_source[source_id],
                            message=message,
                        ),
                    )
                    continue

                if event == "parse_done":
                    conn.commit()
                    started = source_started.get(source_id, time.perf_counter())
                    elapsed_by_source[source_id] = time.perf_counter() - started
                    source_completed.add(source_id)
                    if source_id not in errors_by_source:
                        _emit(
                            progress_callback,
                            BuildProgress(
                                source_id=source_id,
                                phase="upsert",
                                status="done",
                                records=records_by_source[source_id],
                                elapsed_seconds=elapsed_by_source[source_id],
                            ),
                        )
                    continue

            for future in futures.values():
                future.result()

    return records_by_source, errors_by_source, elapsed_by_source


def _prune_orphan_words(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        DELETE FROM words
        WHERE id NOT IN (SELECT DISTINCT word_id FROM word_source)
        """
    )
    conn.commit()


def _integrity_ok(conn: sqlite3.Connection) -> bool:
    row = conn.execute("PRAGMA integrity_check").fetchone()
    if row is None:
        return False
    return str(row[0]).lower() == "ok"


class CorpusStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    def exists(self) -> bool:
        return self.db_path.exists()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _disallowed_mask(allowed_letters: Iterable[str]) -> int:
        allowed = {char.lower() for char in allowed_letters if len(char) == 1 and "a" <= char.lower() <= "z"}
        disallowed_mask = 0
        for index in range(26):
            char = chr(ord("a") + index)
            if char not in allowed:
                disallowed_mask |= 1 << index
        return disallowed_mask

    def suggest_words(
        self,
        *,
        target_length: int,
        prefix: str = "",
        limit: int = 100,
        allowed_letters: Iterable[str],
    ) -> list[str]:
        if not self.exists() or target_length <= 0 or limit <= 0:
            return []

        prefix_cf = prefix.casefold()
        disallowed_mask = self._disallowed_mask(allowed_letters)
        with closing(self._connect()) as conn:
            if prefix_cf:
                rows = conn.execute(
                    """
                    SELECT word
                    FROM words
                    WHERE length = ?
                      AND word_casefold LIKE ? || '%'
                      AND word_casefold GLOB '[a-z]*'
                      AND word_casefold NOT GLOB '*[^a-z]*'
                      AND (ascii_mask & ?) = 0
                    ORDER BY word_casefold
                    LIMIT ?
                    """,
                    (target_length, prefix_cf, disallowed_mask, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT word
                    FROM words
                    WHERE length = ?
                      AND word_casefold GLOB '[a-z]*'
                      AND word_casefold NOT GLOB '*[^a-z]*'
                      AND (ascii_mask & ?) = 0
                    ORDER BY word_casefold
                    LIMIT ?
                    """,
                    (target_length, disallowed_mask, limit),
                ).fetchall()
        return [str(row["word"]) for row in rows]


def _resolve_selected_sources(
    include_sources: Iterable[str] | None,
    exclude_sources: Iterable[str] | None,
) -> list[SourceDefinition]:
    registry = source_registry()
    include_set = set(include_sources or SOURCE_IDS)
    exclude_set = set(exclude_sources or [])
    unknown = sorted((include_set | exclude_set) - set(SOURCE_IDS))
    if unknown:
        raise ValueError(f"Unknown source ids: {', '.join(unknown)}")

    selected_ids = [source_id for source_id in SOURCE_IDS if source_id in include_set and source_id not in exclude_set]
    return [registry[source_id] for source_id in selected_ids]


def build_data(
    *,
    db_path: Path | None = None,
    download_dir: Path | None = None,
    full_rebuild: bool = False,
    workers: int = DEFAULT_WORKERS,
    min_success: int = DEFAULT_MIN_SUCCESS,
    include_sources: Iterable[str] | None = None,
    exclude_sources: Iterable[str] | None = None,
    progress_callback: ProgressCallback | None = None,
) -> BuildResult:
    selected_sources = _resolve_selected_sources(include_sources, exclude_sources)
    if not selected_sources:
        raise ValueError("No sources selected.")

    db_path = Path(db_path) if db_path is not None else default_db_path()
    download_dir = Path(download_dir) if download_dir is not None else default_download_directory()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    with closing(sqlite3.connect(db_path)) as conn:
        initialize_database(conn)
        existing_rows = _load_source_rows(conn)

        source_results: dict[str, SourceResult] = {}
        to_process: list[SourceDefinition] = []
        discovered_tokens: dict[str, tuple[str, str]] = {}

        for source in selected_sources:
            _emit(
                progress_callback,
                BuildProgress(source_id=source.source_id, phase="discover", status="running"),
            )
            if source.source_id == SOURCE_NLTK_WORDS:
                etag = _package_version_token("nltk")
                last_modified = ""
            elif source.source_id == SOURCE_WORDFREQ_EN:
                etag = _package_version_token("wordfreq")
                last_modified = ""
            else:
                etag, last_modified, _ = _discover_remote_tokens(source.urls)

            discovered_tokens[source.source_id] = (etag, last_modified)

            previous = existing_rows.get(source.source_id)
            can_skip = (
                not full_rebuild
                and previous is not None
                and previous.get("records", 0) > 0
                and previous.get("status") in {"success", "skipped"}
                and bool(etag or last_modified)
                and previous.get("etag", "") == etag
                and previous.get("last_modified", "") == last_modified
            )
            if can_skip:
                result = SourceResult(
                    source_id=source.source_id,
                    status="skipped",
                    records=int(previous.get("records", 0)),
                    etag=etag,
                    last_modified=last_modified,
                    sha256=str(previous.get("sha256", "")),
                )
                source_results[source.source_id] = result
                _upsert_source_row(
                    conn,
                    source.source_id,
                    source.urls,
                    "skipped",
                    result.records,
                    etag=etag,
                    last_modified=last_modified,
                    sha256_value=result.sha256,
                )
                _emit(
                    progress_callback,
                    BuildProgress(source_id=source.source_id, phase="discover", status="skipped"),
                )
            else:
                to_process.append(source)
                _emit(
                    progress_callback,
                    BuildProgress(source_id=source.source_id, phase="discover", status="ready"),
                )

        conn.commit()

        artifact_paths: dict[str, list[Path]] = {source.source_id: [] for source in selected_sources}
        planned_artifacts: dict[str, list[Path]] = {source.source_id: [] for source in selected_sources}
        source_failures: dict[str, str] = {}
        source_bytes_downloaded: dict[str, int] = {source.source_id: 0 for source in selected_sources}
        source_bytes_total: dict[str, int] = {source.source_id: 0 for source in selected_sources}
        artifact_progress: dict[tuple[str, str], int] = {}
        artifact_totals: dict[tuple[str, str], int | None] = {}
        progress_lock = threading.Lock()

        def on_download_progress(source_id: str, url: str, downloaded: int, total: int | None) -> None:
            key = (source_id, url)
            with progress_lock:
                artifact_progress[key] = downloaded
                if total is not None:
                    artifact_totals[key] = total
                source_downloaded = sum(value for (sid, _), value in artifact_progress.items() if sid == source_id)
                source_total_values = [value for (sid, _), value in artifact_totals.items() if sid == source_id and value]
                source_total = sum(source_total_values) if source_total_values else None
                source_bytes_downloaded[source_id] = source_downloaded
                if source_total is not None:
                    source_bytes_total[source_id] = source_total
            _emit(
                progress_callback,
                BuildProgress(
                    source_id=source_id,
                    phase="download",
                    status="running",
                    bytes_downloaded=source_downloaded,
                    bytes_total=source_total,
                ),
            )

        download_tasks: list[DownloadTask] = []
        for source in to_process:
            previous = existing_rows.get(source.source_id)
            for url in source.urls:
                filename = safe_filename_from_url(url)
                destination = download_dir / source.source_id / filename
                planned_artifacts[source.source_id].append(destination)
                if (
                    full_rebuild
                    and previous is not None
                    and previous.get("etag", "") == discovered_tokens[source.source_id][0]
                    and previous.get("last_modified", "") == discovered_tokens[source.source_id][1]
                    and destination.exists()
                ):
                    artifact_paths[source.source_id].append(destination)
                    file_size = destination.stat().st_size
                    source_bytes_downloaded[source.source_id] += file_size
                    source_bytes_total[source.source_id] += file_size
                    _emit(
                        progress_callback,
                        BuildProgress(
                            source_id=source.source_id,
                            phase="download",
                            status="cached",
                            bytes_downloaded=source_bytes_downloaded[source.source_id],
                            bytes_total=source_bytes_total[source.source_id],
                            message=f"Reused cached artifact: {destination.name}",
                        ),
                    )
                else:
                    download_tasks.append(DownloadTask(source.source_id, url, destination))

        if download_tasks:
            with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
                future_map = {
                    executor.submit(
                        _download_file,
                        task,
                        lambda sid, downloaded, total, current_url=task.url: on_download_progress(
                            sid, current_url, downloaded, total
                        ),
                    ): task
                    for task in download_tasks
                }
                for future in as_completed(future_map):
                    result = future.result()
                    if result.error:
                        source_failures[result.source_id] = result.error
                        _emit(
                            progress_callback,
                            BuildProgress(
                                source_id=result.source_id,
                                phase="download",
                                status="failed",
                                bytes_downloaded=result.bytes_downloaded,
                                bytes_total=result.bytes_total,
                                message=result.error,
                            ),
                        )
                    else:
                        artifact_paths[result.source_id].append(result.destination)
                        _emit(
                            progress_callback,
                            BuildProgress(
                                source_id=result.source_id,
                                phase="download",
                                status="done",
                                bytes_downloaded=result.bytes_downloaded,
                                bytes_total=result.bytes_total,
                            ),
                        )

        for source in to_process:
            expected_files = planned_artifacts[source.source_id]
            if expected_files and not artifact_paths[source.source_id]:
                source_failures[source.source_id] = (
                    source_failures.get(source.source_id)
                    or "No source artifacts were available after download/caching."
                )

        processable_sources = [source for source in to_process if source.source_id not in source_failures]
        for source in processable_sources:
            _upsert_source_row(
                conn,
                source.source_id,
                source.urls,
                "running",
                0,
                etag=discovered_tokens[source.source_id][0],
                last_modified=discovered_tokens[source.source_id][1],
            )
        conn.commit()

        processed_records, processing_errors, processed_elapsed = _process_sources_concurrently(
            db_path=db_path,
            sources=processable_sources,
            artifact_paths=artifact_paths,
            workers=workers,
            progress_callback=progress_callback,
        )
        source_failures.update(processing_errors)

        for source in to_process:
            started = time.perf_counter()
            if source.source_id in source_failures:
                error = source_failures[source.source_id]
                result = SourceResult(
                    source_id=source.source_id,
                    status="failed",
                    error=error,
                    records=0,
                    etag=discovered_tokens[source.source_id][0],
                    last_modified=discovered_tokens[source.source_id][1],
                    bytes_downloaded=source_bytes_downloaded.get(source.source_id, 0),
                    elapsed_seconds=processed_elapsed.get(source.source_id, 0.0),
                )
                source_results[source.source_id] = result
                _upsert_source_row(
                    conn,
                    source.source_id,
                    source.urls,
                    "failed",
                    0,
                    etag=result.etag,
                    last_modified=result.last_modified,
                    error=result.error,
                )
                _emit(
                    progress_callback,
                    BuildProgress(
                        source_id=source.source_id,
                        phase="finalize",
                        status="failed",
                        message=error,
                    ),
                )
                continue

            try:
                records = processed_records.get(source.source_id, 0)
                files_hash = hashlib.sha256()
                for path in sorted(artifact_paths[source.source_id]):
                    files_hash.update(str(path).encode("utf-8"))
                    with path.open("rb") as handle:
                        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                            files_hash.update(chunk)
                if not artifact_paths[source.source_id]:
                    files_hash.update(discovered_tokens[source.source_id][0].encode("utf-8"))
                    files_hash.update(discovered_tokens[source.source_id][1].encode("utf-8"))
                sha256_value = files_hash.hexdigest()
                elapsed = max(processed_elapsed.get(source.source_id, 0.0), time.perf_counter() - started)
                result = SourceResult(
                    source_id=source.source_id,
                    status="success",
                    records=records,
                    etag=discovered_tokens[source.source_id][0],
                    last_modified=discovered_tokens[source.source_id][1],
                    sha256=sha256_value,
                    bytes_downloaded=source_bytes_downloaded.get(source.source_id, 0),
                    elapsed_seconds=elapsed,
                )
                source_results[source.source_id] = result
                _upsert_source_row(
                    conn,
                    source.source_id,
                    source.urls,
                    "success",
                    records,
                    etag=result.etag,
                    last_modified=result.last_modified,
                    sha256_value=result.sha256,
                )
                _emit(
                    progress_callback,
                    BuildProgress(
                        source_id=source.source_id,
                        phase="finalize",
                        status="success",
                        records=records,
                        elapsed_seconds=elapsed,
                        bytes_downloaded=result.bytes_downloaded,
                        bytes_total=source_bytes_total.get(source.source_id) or None,
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                elapsed = time.perf_counter() - started
                message = f"{type(exc).__name__}: {exc}"
                result = SourceResult(
                    source_id=source.source_id,
                    status="failed",
                    error=message,
                    records=0,
                    etag=discovered_tokens[source.source_id][0],
                    last_modified=discovered_tokens[source.source_id][1],
                    bytes_downloaded=source_bytes_downloaded.get(source.source_id, 0),
                    elapsed_seconds=elapsed,
                )
                source_results[source.source_id] = result
                _upsert_source_row(
                    conn,
                    source.source_id,
                    source.urls,
                    "failed",
                    0,
                    etag=result.etag,
                    last_modified=result.last_modified,
                    error=result.error,
                )
                _emit(
                    progress_callback,
                    BuildProgress(
                        source_id=source.source_id,
                        phase="finalize",
                        status="failed",
                        message=message,
                    ),
                )

        conn.commit()
        _prune_orphan_words(conn)

        total_words = int(conn.execute("SELECT COUNT(*) FROM words").fetchone()[0])
        total_word_sources = int(conn.execute("SELECT COUNT(*) FROM word_source").fetchone()[0])
        if not _integrity_ok(conn):
            raise RuntimeError("SQLite integrity check failed after build.")

        ordered_results = tuple(source_results[source_id] for source_id in SOURCE_IDS if source_id in source_results)
        successful_sources = sum(1 for result in ordered_results if result.status == "success")
        skipped_sources = sum(1 for result in ordered_results if result.status == "skipped")
        failed_sources = sum(1 for result in ordered_results if result.status == "failed")

        if successful_sources + skipped_sources < min_success:
            failed_list = ", ".join(f"{result.source_id}: {result.error}" for result in ordered_results if result.status == "failed")
            raise RuntimeError(
                "Build finished below minimum success threshold. "
                f"required={min_success}, completed={successful_sources + skipped_sources}. "
                f"Failures: {failed_list}"
            )

        return BuildResult(
            db_path=db_path,
            total_words=total_words,
            total_word_sources=total_word_sources,
            successful_sources=successful_sources,
            skipped_sources=skipped_sources,
            failed_sources=failed_sources,
            source_results=ordered_results,
        )

class _CliProgressReporter:
    def __init__(self, selected_sources: Iterable[str]) -> None:
        self._lock = threading.Lock()
        self._completed: set[str] = set()
        self._source_bar = tqdm(total=len(list(selected_sources)), desc="Sources", unit="src")
        self._download_bar = tqdm(
            total=0,
            desc="Downloads",
            unit="B",
            unit_scale=True,
            leave=False,
            position=1,
        )
        self._source_downloaded: dict[str, int] = {}
        self._source_totals: dict[str, int] = {}

    def close(self) -> None:
        self._download_bar.close()
        self._source_bar.close()

    def __call__(self, progress: BuildProgress) -> None:
        with self._lock:
            if progress.phase == "download":
                previous = self._source_downloaded.get(progress.source_id, 0)
                delta = max(0, progress.bytes_downloaded - previous)
                self._source_downloaded[progress.source_id] = progress.bytes_downloaded
                if progress.bytes_total is not None:
                    self._source_totals[progress.source_id] = progress.bytes_total
                total_target = sum(self._source_totals.values())
                if total_target > self._download_bar.total:
                    self._download_bar.total = total_target
                    self._download_bar.refresh()
                if delta:
                    self._download_bar.update(delta)

            if progress.status in {"success", "failed", "skipped"} and progress.source_id not in self._completed:
                self._completed.add(progress.source_id)
                self._source_bar.update(1)
                summary = progress.message or f"{progress.phase}: {progress.status}"
                self._source_bar.write(f"[{progress.source_id}] {summary}")


def _build_command(args: argparse.Namespace) -> int:
    include = args.source if args.source else None
    exclude = args.exclude_source if args.exclude_source else None
    selected_ids = [
        source_id
        for source_id in SOURCE_IDS
        if source_id in set(include or SOURCE_IDS) and source_id not in set(exclude or [])
    ]
    reporter = _CliProgressReporter(selected_ids)
    try:
        result = build_data(
            db_path=Path(args.db_path) if args.db_path else None,
            download_dir=Path(args.download_dir) if args.download_dir else None,
            full_rebuild=args.full_rebuild,
            workers=args.workers,
            min_success=args.min_success,
            include_sources=include,
            exclude_sources=exclude,
            progress_callback=reporter,
        )
    finally:
        reporter.close()

    print(f"Corpus DB: {result.db_path}")
    print(
        "Sources -> "
        f"success={result.successful_sources}, skipped={result.skipped_sources}, failed={result.failed_sources}"
    )
    print(f"Rows -> words={result.total_words}, word_source={result.total_word_sources}")
    for source in result.source_results:
        detail = source.error if source.error else f"records={source.records}"
        print(f" - {source.source_id}: {source.status} ({detail})")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the SQLite corpus datastore.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Build or update corpus datastore.")
    build_parser.add_argument("--full-rebuild", action="store_true", help="Ignore incremental source checks.")
    build_parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrent download workers.")
    build_parser.add_argument(
        "--min-success",
        type=int,
        default=DEFAULT_MIN_SUCCESS,
        help="Minimum successful+skipped sources required.",
    )
    build_parser.add_argument(
        "--source",
        action="append",
        choices=SOURCE_IDS,
        help="Include only this source id (repeatable).",
    )
    build_parser.add_argument(
        "--exclude-source",
        action="append",
        choices=SOURCE_IDS,
        help="Exclude this source id (repeatable).",
    )
    build_parser.add_argument("--db-path", help="SQLite output path.")
    build_parser.add_argument("--download-dir", help="Directory used for downloaded source artifacts.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.command == "build":
        return _build_command(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
