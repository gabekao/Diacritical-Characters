from __future__ import annotations

import os
from pathlib import Path
import sys
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from diacritical_characters import core
from diacritical_characters.gui import MainWindow
from diacritical_characters.wordlist import SUPERSCRIPT_LETTERS, WordList, load_wordlist


class WordListTests(unittest.TestCase):
    def test_suggest_puts_prefix_matches_before_substring_matches(self) -> None:
        words = WordList(["earth", "tread", "death", "trade", "cat"])
        self.assertEqual(words.suggest(5), ["earth", "tread", "death", "trade"])
        self.assertEqual(words.suggest(5, "ea"), ["earth", "tread", "death"])
        self.assertEqual(words.suggest(5, "tr", alphabetical=True), ["trade", "tread"])
        self.assertEqual(words.suggest(4), [])

    def test_bundled_list_only_uses_superscript_letters(self) -> None:
        bundled = load_wordlist()
        self.assertGreater(len(bundled), 10_000)
        allowed = set(SUPERSCRIPT_LETTERS)
        self.assertEqual(allowed, set(core.SUPERSCRIPT_MAP))
        for length in bundled.lengths():
            for word in bundled.words_of_length(length):
                self.assertEqual(len(word), length)
                self.assertLessEqual(set(word), allowed, word)


class MainWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QApplication.instance() or QApplication([])

    def setUp(self) -> None:
        self.window = MainWindow(WordList(["erotic", "meteor", "editor", "tomato"]))

    def test_filter_and_stack_layers(self) -> None:
        window = self.window
        window.base_input.setText("jordan")
        self.assertEqual(window.word_model.rowCount(), 4)

        window.filter_input.setText("e")
        self.assertEqual(window.word_model.words(), ["erotic", "editor", "meteor"])

        window._on_filter_return()  # not a full word, so the top match is added
        self.assertEqual(window.layers, ["erotic"])
        self.assertEqual(window.filter_input.text(), "")

        self.assertTrue(window.add_layer("meteor"))
        expected = core.compose_diacritical_layers("jordan", ["erotic", "meteor"], core.SUPERSCRIPT_MAP)
        self.assertEqual(window.result_view.text(), expected)
        self.assertTrue(window.copy_button.isEnabled())

    def test_rejects_layers_that_cannot_be_stacked(self) -> None:
        window = self.window
        window.base_input.setText("jordan")
        self.assertFalse(window.add_layer("orbita"))  # "b" has no superscript form
        self.assertFalse(window.add_layer("tomatoes"))  # wrong length
        self.assertEqual(window.layers, [])
        self.assertFalse(window.copy_button.isEnabled())

    def test_typed_custom_layer_is_accepted(self) -> None:
        window = self.window
        window.base_input.setText("jordan")
        window.filter_input.setText("xxxxxx")
        self.assertEqual(window.word_model.rowCount(), 0)
        window._on_filter_return()
        self.assertEqual(window.layers, ["xxxxxx"])

    def test_preview_does_not_commit(self) -> None:
        window = self.window
        window.base_input.setText("jordan")
        window._set_preview_word("erotic")
        self.assertIn(core.SUPERSCRIPT_MAP["e"], window.result_view.text())
        self.assertEqual(window.layers, [])
        window._set_preview_word("")
        self.assertEqual(window.result_view.text(), "jordan")


if __name__ == "__main__":
    unittest.main()
