from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from diacritical_characters import core


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build filtered word data for diacritical suggestions.")
    parser.add_argument(
        "--force-generate-words",
        action="store_true",
        help="Regenerate word_list.p from the NLTK corpus instead of using existing data.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = datetime.now()
    result = core.build_data(force_generate_words=args.force_generate_words)
    elapsed = datetime.now() - start
    print(f"Time elapsed: {elapsed}")
    print(
        "Built data with "
        f"{result.filtered_words} filtered words across {result.length_buckets} length buckets.\n"
        f"Output pickle: {result.output_pickle}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

