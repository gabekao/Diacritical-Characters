from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import pickle
import string
import sys
from typing import Iterable

DEFAULT_ALLOWED_LETTERS = "acdehimortuvx"
DEFAULT_WORD_LIMIT = 100


@dataclass(frozen=True)
class BuildResult:
    total_words: int
    filtered_words: int
    length_buckets: int
    output_pickle: Path


def project_root() -> Path:
    # When packaged with PyInstaller, keep runtime data next to the built executable.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def pickle_directory(root: Path | None = None) -> Path:
    return (root or project_root()) / "pickle"


def text_directory(root: Path | None = None) -> Path:
    return (root or project_root()) / "txt"


def _read_pickle(path: Path):
    with path.open("rb") as handle:
        return pickle.load(handle)


def _write_pickle(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        pickle.dump(obj, handle)


def dump_output(lines: Iterable[str], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as handle:
        for line in lines:
            handle.write(f"{line}\n")


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


def _generate_word_list_from_nltk() -> list[str]:
    try:
        from nltk.corpus import words
    except ModuleNotFoundError as exc:
        raise RuntimeError("NLTK is required to generate word data. Install with: pip install nltk") from exc

    try:
        source_words = words.words()
    except LookupError as exc:
        raise RuntimeError(
            "NLTK words corpus is missing. Run: python -m nltk.downloader words"
        ) from exc

    return sorted({str(word).lower() for word in source_words if str(word).strip()})


def load_or_generate_word_list(root: Path | None = None, force_generate: bool = False) -> list[str]:
    root = root or project_root()
    word_pickle = pickle_directory(root) / "word_list.p"
    raw_txt = text_directory(root) / "raw_list.txt"

    words: list[str]
    if force_generate or not word_pickle.exists():
        words = _generate_word_list_from_nltk()
        _write_pickle(word_pickle, words)
        dump_output(words, raw_txt)
        return words

    loaded = _read_pickle(word_pickle)
    if not isinstance(loaded, list):
        raise ValueError("word_list.p must contain a list of words.")

    words = sorted({str(word).lower() for word in loaded if str(word).strip()})
    if words != loaded:
        _write_pickle(word_pickle, words)
    return words


def filter_words_to_allowed_letters(words: Iterable[str], allowed_letters: Iterable[str]) -> list[str]:
    allowed = {char.lower() for char in allowed_letters}
    disallowed = set(string.ascii_lowercase) - allowed
    filtered = {
        word.lower()
        for word in words
        if word and not any(char in disallowed for char in word.lower())
    }
    return sorted(filtered, key=str.casefold)


def build_sorted_index(words: Iterable[str]) -> dict[int, dict[str, list[str]]]:
    temp: defaultdict[int, defaultdict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

    for word in words:
        normalized = str(word).lower()
        if not normalized:
            continue
        temp[len(normalized)][normalized[0]].add(normalized)

    sorted_index: dict[int, dict[str, list[str]]] = {}
    for length in sorted(temp):
        by_initial: dict[str, list[str]] = {}
        for initial in sorted(temp[length]):
            by_initial[initial] = sorted(temp[length][initial], key=str.casefold)
        sorted_index[length] = by_initial
    return sorted_index


def _write_index_text(index: dict[int, dict[str, list[str]]], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    max_length = max(index.keys(), default=0)
    with destination.open("w", encoding="utf-8") as handle:
        for length in range(1, max_length + 1):
            handle.write(f"{length}: {index.get(length, {})}\n")


def normalize_sorted_index(raw_index) -> dict[int, dict[str, list[str]]]:
    if not isinstance(raw_index, dict):
        raise ValueError("Sorted source pickle must contain a dictionary-like object.")

    normalized: dict[int, dict[str, list[str]]] = {}
    for length_key, by_initial in raw_index.items():
        try:
            length = int(length_key)
        except (TypeError, ValueError):
            continue
        if length <= 0 or not isinstance(by_initial, dict):
            continue

        normalized[length] = {}
        for initial_key, words in by_initial.items():
            initial = str(initial_key).lower()[:1]
            if not initial:
                continue

            if isinstance(words, str):
                sequence = [words]
            elif isinstance(words, Iterable):
                sequence = [str(word) for word in words]
            else:
                continue

            clean_words = sorted(
                {word.lower() for word in sequence if word},
                key=str.casefold,
            )
            if clean_words:
                normalized[length][initial] = clean_words

        if not normalized[length]:
            normalized.pop(length, None)

    return dict(sorted(normalized.items(), key=lambda item: item[0]))


def load_suggestion_index(root: Path | None = None) -> dict[int, dict[str, list[str]]]:
    path = pickle_directory(root) / "sorted_source.p"
    raw = _read_pickle(path)
    return normalize_sorted_index(raw)


def build_data(root: Path | None = None, force_generate_words: bool = False) -> BuildResult:
    root = root or project_root()
    p_dir = pickle_directory(root)
    t_dir = text_directory(root)
    p_dir.mkdir(parents=True, exist_ok=True)
    t_dir.mkdir(parents=True, exist_ok=True)

    superscript_dict = load_superscript_dict(root)
    words = load_or_generate_word_list(root, force_generate=force_generate_words)
    filtered_words = filter_words_to_allowed_letters(words, superscript_dict.keys())
    sorted_index = build_sorted_index(filtered_words)

    sorted_source_pickle = p_dir / "sorted_source.p"
    _write_pickle(sorted_source_pickle, sorted_index)
    _write_index_text(sorted_index, t_dir / "filtered_set.txt")
    _write_index_text(sorted_index, t_dir / "sorted_source.txt")
    dump_output(filtered_words, t_dir / "filtered_list.txt")

    return BuildResult(
        total_words=len(words),
        filtered_words=len(filtered_words),
        length_buckets=len(sorted_index),
        output_pickle=sorted_source_pickle,
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
    index: dict[int, dict[str, list[str]]] | None,
    target_length: int,
    prefix: str = "",
    limit: int = DEFAULT_WORD_LIMIT,
) -> list[str]:
    if not index or target_length <= 0:
        return []

    by_initial = index.get(target_length, {})
    if not by_initial:
        return []

    prefix = prefix.lower()
    candidates: list[str]
    if prefix:
        initial = prefix[0]
        candidates = [
            word
            for word in by_initial.get(initial, [])
            if word.startswith(prefix)
        ]
    else:
        combined = []
        for words in by_initial.values():
            combined.extend(words)
        candidates = sorted(set(combined), key=str.casefold)

    return candidates[: max(0, limit)]
