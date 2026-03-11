"""Core package for the Diacritical Characters project."""

from .core import (
    BuildResult,
    build_data,
    compose_diacritical_layers,
    compose_diacritical_string,
    compose_layers_or_errors,
    compose_or_errors,
    load_suggestion_index,
    load_superscript_dict,
    suggest_superscript_words,
    validate_layer_stack,
    validate_input_pair,
)

__all__ = [
    "BuildResult",
    "build_data",
    "compose_diacritical_layers",
    "compose_diacritical_string",
    "compose_layers_or_errors",
    "compose_or_errors",
    "load_suggestion_index",
    "load_superscript_dict",
    "suggest_superscript_words",
    "validate_layer_stack",
    "validate_input_pair",
]
