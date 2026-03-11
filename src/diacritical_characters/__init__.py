"""Core package for the Diacritical Characters project."""

from __future__ import annotations

from typing import Any

__all__ = [
    "BuildProgress",
    "BuildResult",
    "build_data",
    "compose_diacritical_layers",
    "compose_diacritical_string",
    "compose_layers_or_errors",
    "compose_or_errors",
    "load_superscript_dict",
    "open_corpus_store",
    "suggest_superscript_words",
    "validate_layer_stack",
    "validate_input_pair",
]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from . import core

        return getattr(core, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
