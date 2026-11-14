from __future__ import annotations

import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from diacritical_characters import corpus
from diacritical_characters.build_popup import BuildGuiOptions, BuildRunner


class BuildRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # A full QApplication, so widget tests in the same run can share it.
        cls.app = QApplication.instance() or QApplication([])

    def test_runner_emits_progress_and_finished(self) -> None:
        progress_events: list[corpus.BuildProgress] = []
        finished_events: list[object] = []
        failed_events: list[str] = []

        def fake_build_data(*_args, **kwargs):
            callback = kwargs.get("progress_callback")
            if callback is not None:
                callback(
                    corpus.BuildProgress(
                        source_id="nltk_words",
                        phase="discover",
                        status="running",
                    )
                )
            return corpus.BuildResult(
                db_path=Path("data/corpus.sqlite3"),
                total_words=1,
                total_word_sources=1,
                successful_sources=1,
                skipped_sources=0,
                failed_sources=0,
                source_results=(),
            )

        runner = BuildRunner(BuildGuiOptions(full_rebuild=False, workers=1, min_success=1))
        runner.progress.connect(lambda event: progress_events.append(event))
        runner.finished.connect(lambda result: finished_events.append(result))
        runner.failed.connect(lambda message: failed_events.append(message))

        with patch("diacritical_characters.build_popup.corpus.build_data", fake_build_data):
            runner.run()

        self.assertEqual(len(progress_events), 1)
        self.assertEqual(len(finished_events), 1)
        self.assertEqual(len(failed_events), 0)


if __name__ == "__main__":
    unittest.main()
