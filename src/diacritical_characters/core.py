from __future__ import annotations

from pathlib import Path
import pickle
import sys
from typing import Callable, Iterable

from . import corpus

DEFAULT_ALLOWED_LETTERS = "acdehimortuvx"
DEFAULT_WORD_LIMIT = 100

BuildResult = corpus.BuildResult
BuildProgress = corpus.BuildProgress


def project_root() -> Path:
    # When packaged with PyInstaller, keep runtime data next to the built executable.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def pickle_directory(root: Path | None = None) -> Path:
    return (root or project_root()) / "pickle"


def text_directory(root: Path | None = None) -> Path:
    return (root or project_root()) / "txt"


def data_directory(root: Path | None = None) -> Path:
    return (root or project_root()) / "data"


def download_directory(root: Path | None = None) -> Path:
    return (root or project_root()) / "downloads"


def corpus_database_path(root: Path | None = None) -> Path:
    return data_directory(root) / "corpus.sqlite3"


def _read_pickle(path: Path):
    with path.open("rb") as handle:
        return pickle.load(handle)


def load_superscript_dict(root: Path | None = None) -> dict[str, str]:
    path = pickle_directory(root) / "superscript_dict.p"
    data = _read_pickle(path)
    if not isinstance(data, dict):
        raise ValueError("superscript_dict.p must contain a dictionary.")

    cleaned: dict[str, str] = {}
    for key, value in data.items():
        key_str = str(key).lower()
        value_str = str(value)
        if len(key_str) != 1 or len(value_str) != 1:
            continue
        cleaned[key_str] = value_str

    if not cleaned:
        raise ValueError("superscript_dict.p does not contain valid mappings.")
    return cleaned


def open_corpus_store(root: Path | None = None) -> corpus.CorpusStore:
    return corpus.CorpusStore(corpus_database_path(root))


def build_data(
    root: Path | None = None,
    *,
    full_rebuild: bool = False,
    workers: int = corpus.DEFAULT_WORKERS,
    min_success: int = corpus.DEFAULT_MIN_SUCCESS,
    include_sources: Iterable[str] | None = None,
    exclude_sources: Iterable[str] | None = None,
    progress_callback: Callable[[BuildProgress], None] | None = None,
) -> BuildResult:
    root = root or project_root()
    data_directory(root).mkdir(parents=True, exist_ok=True)
    download_directory(root).mkdir(parents=True, exist_ok=True)
    return corpus.build_data(
        db_path=corpus_database_path(root),
        download_dir=download_directory(root),
        full_rebuild=full_rebuild,
        workers=workers,
        min_success=min_success,
        include_sources=include_sources,
        exclude_sources=exclude_sources,
        progress_callback=progress_callback,
    )


def validate_input_pair(
    base_text: str,
    superscript_text: str,
    superscript_dict: dict[str, str],
) -> list[str]:
    errors = []
    if len(base_text) != len(superscript_text):
        errors.append("Lengths do not match.")

    invalid_chars = sorted({char for char in superscript_text.lower() if char not in superscript_dict})
    if invalid_chars:
        errors.append(f"Unsupported superscript characters: {', '.join(invalid_chars)}")
    return errors


def validate_layer_stack(
    base_text: str,
    superscript_layers: list[str],
    superscript_dict: dict[str, str],
) -> list[str]:
    errors: list[str] = []
    if not superscript_layers:
        errors.append("Add at least one superscript layer.")
        return errors

    for index, layer in enumerate(superscript_layers, start=1):
        layer_errors = validate_input_pair(base_text, layer, superscript_dict)
        for error in layer_errors:
            errors.append(f"Layer {index}: {error}")
    return errors


def compose_diacritical_layers(
    base_text: str,
    superscript_layers: list[str],
    superscript_dict: dict[str, str],
) -> str:
    errors = validate_layer_stack(base_text, superscript_layers, superscript_dict)
    if errors:
        raise ValueError(" | ".join(errors))

    output = []
    for char_index, base_char in enumerate(base_text):
        output.append(base_char)
        for layer in superscript_layers:
            output.append(superscript_dict[layer[char_index].lower()])
    return "".join(output)


def compose_diacritical_string(base_text: str, superscript_text: str, superscript_dict: dict[str, str]) -> str:
    return compose_diacritical_layers(base_text, [superscript_text], superscript_dict)


def compose_or_errors(
    base_text: str,
    superscript_text: str,
    superscript_dict: dict[str, str],
) -> tuple[str, list[str]]:
    return compose_layers_or_errors(base_text, [superscript_text], superscript_dict)


def compose_layers_or_errors(
    base_text: str,
    superscript_layers: list[str],
    superscript_dict: dict[str, str],
) -> tuple[str, list[str]]:
    errors = validate_layer_stack(base_text, superscript_layers, superscript_dict)
    if errors:
        return "", errors
    return compose_diacritical_layers(base_text, superscript_layers, superscript_dict), []


def suggest_superscript_words(
    store: corpus.CorpusStore | None,
    target_length: int,
    prefix: str = "",
    limit: int = DEFAULT_WORD_LIMIT,
    allowed_letters: Iterable[str] = DEFAULT_ALLOWED_LETTERS,
) -> list[str]:
    if store is None or target_length <= 0:
        return []
    if not store.exists():
        return []
    return store.suggest_words(
        target_length=target_length,
        prefix=prefix,
        limit=max(0, limit),
        allowed_letters=allowed_letters,
    )
