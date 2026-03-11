from __future__ import annotations

import argparse
from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from diacritical_characters import corpus
from diacritical_characters.build_popup import BuildGuiOptions, run_build_popup


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compatibility wrapper for the corpus builder CLI."
    )
    parser.add_argument(
        "--force-generate-words",
        action="store_true",
        help="Legacy flag mapped to --full-rebuild.",
    )
    parser.add_argument("--full-rebuild", action="store_true", help="Rebuild all selected sources.")
    parser.add_argument("--workers", type=int, default=corpus.DEFAULT_WORKERS)
    parser.add_argument("--min-success", type=int, default=corpus.DEFAULT_MIN_SUCCESS)
    parser.add_argument("--source", action="append", choices=corpus.available_source_ids())
    parser.add_argument("--exclude-source", action="append", choices=corpus.available_source_ids())
    parser.add_argument("--db-path")
    parser.add_argument("--download-dir")
    parser.add_argument(
        "--cli-progress",
        action="store_true",
        help="Use terminal-only progress output instead of the popup monitor.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    forwarded = ["build"]
    if args.full_rebuild or args.force_generate_words:
        forwarded.append("--full-rebuild")
    forwarded.extend(["--workers", str(args.workers)])
    forwarded.extend(["--min-success", str(args.min_success)])
    for source in args.source or []:
        forwarded.extend(["--source", source])
    for source in args.exclude_source or []:
        forwarded.extend(["--exclude-source", source])
    if args.db_path:
        forwarded.extend(["--db-path", args.db_path])
    if args.download_dir:
        forwarded.extend(["--download-dir", args.download_dir])
    if args.cli_progress:
        return corpus.main(forwarded)

    options = BuildGuiOptions(
        full_rebuild=args.full_rebuild or args.force_generate_words,
        workers=args.workers,
        min_success=args.min_success,
        include_sources=tuple(args.source) if args.source else None,
        exclude_sources=tuple(args.exclude_source) if args.exclude_source else None,
        db_path=Path(args.db_path) if args.db_path else None,
        download_dir=Path(args.download_dir) if args.download_dir else None,
    )
    return run_build_popup(options)


if __name__ == "__main__":
    raise SystemExit(main())

