from __future__ import annotations

import argparse
from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from diacritical_characters import core


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a diacritical string from stacked superscript layers.")
    parser.add_argument("--base", required=True, help="Base string shown as regular characters.")
    parser.add_argument(
        "--superscript",
        action="append",
        dest="superscripts",
        required=True,
        help="One superscript layer. Repeat this flag to stack multiple layers.",
    )
    parser.add_argument(
        "--extended",
        action="store_true",
        help="Also allow b f g k l n p s w z, which many fonts cannot display.",
    )
    return parser.parse_args()


def _safe_print(value: str) -> None:
    try:
        print(value)
    except UnicodeEncodeError:
        # Fall back to UTF-8 bytes for terminals using narrow encodings.
        sys.stdout.buffer.write(value.encode("utf-8") + b"\n")


def main() -> int:
    args = parse_args()
    layers = args.superscripts
    superscript_dict = core.load_superscript_dict(extended=args.extended)
    output, errors = core.compose_layers_or_errors(args.base, layers, superscript_dict)

    if errors:
        for error in errors:
            print(error)
        return 1

    _safe_print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
