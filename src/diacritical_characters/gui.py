from __future__ import annotations

import random
import sys
import time

from PySide6.QtCore import QAbstractListModel, QModelIndex, QSize, Qt, QTimer, Signal
from PySide6.QtGui import QFont, QFontMetrics, QKeyEvent, QPainter
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListView,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from . import core
from .wordlist import WordList, load_wordlist

PREVIEW_MAX_POINT_SIZE = 34
PREVIEW_MIN_POINT_SIZE = 14
RANDOM_POOL_SIZE = 300

STYLESHEET = """
QFrame#previewFrame {
    background: palette(base);
    border: 1px solid palette(mid);
    border-radius: 10px;
}
QLabel#hint { color: palette(placeholder-text); }
QLabel#sectionLabel { font-weight: 600; }
QLineEdit { padding: 6px 8px; }
QPushButton { padding: 6px 14px; }
QListView#wordGrid, QListWidget#layerChips {
    border: 1px solid palette(mid);
    border-radius: 6px;
    background: palette(base);
}
QListView#wordGrid::item { padding: 3px 8px; border-radius: 4px; }
QListView#wordGrid::item:hover { background: palette(midlight); }
QListView#wordGrid::item:selected { background: palette(highlight); color: palette(highlighted-text); }
QListWidget#layerChips { padding: 3px; }
QListWidget#layerChips::item {
    padding: 3px 10px;
    margin: 2px;
    border: 1px solid palette(mid);
    border-radius: 11px;
    background: palette(button);
    color: palette(button-text);
}
QListWidget#layerChips::item:hover { border-color: palette(highlight); }
"""


class WordModel(QAbstractListModel):
    """Flat list of words; the view only asks for rows it actually paints."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._words: list[str] = []
        self._item_size = QSize()

    def set_item_size(self, size: QSize) -> None:
        self._item_size = size

    def set_words(self, words: list[str]) -> None:
        self.beginResetModel()
        self._words = words
        self.endResetModel()

    def words(self) -> list[str]:
        return self._words

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self._words)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if role == Qt.DisplayRole and index.isValid() and 0 <= index.row() < len(self._words):
            return self._words[index.row()]
        if role == Qt.SizeHintRole and self._item_size.isValid():
            return self._item_size
        return None


class WordGrid(QListView):
    """Wrapping grid of words. Hover or arrow keys preview, click or Enter picks."""

    wordPicked = Signal(str)
    wordPreviewed = Signal(str)  # empty string clears the preview

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("wordGrid")
        self.setFlow(QListView.LeftToRight)
        self.setWrapping(True)
        self.setResizeMode(QListView.Adjust)
        self.setUniformItemSizes(True)
        self.setLayoutMode(QListView.Batched)
        self.setBatchSize(400)
        self.setMovement(QListView.Static)
        self.setSelectionMode(QListView.SingleSelection)
        self.setEditTriggers(QListView.NoEditTriggers)
        self.setVerticalScrollMode(QListView.ScrollPerPixel)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setMouseTracking(True)
        self.entered.connect(self._preview_index)
        self.clicked.connect(self._pick_index)

    def set_word_length(self, length: int) -> None:
        metrics = QFontMetrics(self.font())
        # Sized for the widest plausible word so nothing is ever elided.
        size = QSize(metrics.horizontalAdvance("m" * max(1, length)) + 22, metrics.height() + 10)
        model = self.model()
        if isinstance(model, WordModel):
            model.set_item_size(size)
        self.setGridSize(size + QSize(4, 2))

    def _preview_index(self, index: QModelIndex) -> None:
        self.wordPreviewed.emit(index.data() or "")

    def _pick_index(self, index: QModelIndex) -> None:
        word = index.data()
        if word:
            self.wordPicked.emit(word)

    def currentChanged(self, current: QModelIndex, previous: QModelIndex) -> None:  # noqa: N802
        super().currentChanged(current, previous)
        if self.hasFocus():
            self._preview_index(current)

    def keyPressEvent(self, event: QKeyEvent) -> None:  # noqa: N802
        if event.key() in (Qt.Key_Return, Qt.Key_Enter) and self.currentIndex().isValid():
            self._pick_index(self.currentIndex())
            return
        super().keyPressEvent(event)

    def leaveEvent(self, event) -> None:  # noqa: N802
        super().leaveEvent(event)
        self.wordPreviewed.emit("")

    def focusOutEvent(self, event) -> None:  # noqa: N802
        super().focusOutEvent(event)
        self.wordPreviewed.emit("")


class ResultView(QWidget):
    """Paints the stacked text itself.

    QLabel clips each line to its normal height, which cuts off anything past
    the first layer of combining marks. Painting directly lets the stack grow
    upward into the free space above the baseline.
    """

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._text = ""
        self._base = ""
        self._dimmed = False
        self.setMinimumHeight(150)
        self.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Expanding)

    def text(self) -> str:
        return self._text

    def set_content(self, text: str, base: str, *, dimmed: bool = False) -> None:
        self._text = text
        self._base = base
        self._dimmed = dimmed
        self.update()

    def _fitted_font(self) -> QFont:
        # Combining marks have no advance width, so the base text drives the fit.
        font = QFont(self.font())
        available = max(50, self.width() - 24)
        size = PREVIEW_MAX_POINT_SIZE
        while True:
            font.setPointSize(size)
            if size <= PREVIEW_MIN_POINT_SIZE or QFontMetrics(font).horizontalAdvance(self._base) <= available:
                return font
            size -= 2

    def paintEvent(self, event) -> None:  # noqa: N802
        if not self._text:
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.TextAntialiasing)
        font = self._fitted_font()
        painter.setFont(font)
        role = self.foregroundRole()
        color = self.palette().color(role)
        if self._dimmed:
            color.setAlphaF(0.45)
        painter.setPen(color)
        metrics = QFontMetrics(font)
        x = (self.width() - metrics.horizontalAdvance(self._base)) / 2
        baseline = self.height() - metrics.descent() - 10
        painter.drawText(int(x), int(baseline), self._text)


class FilterEdit(QLineEdit):
    """Line edit that hands Down-arrow off to the word grid."""

    downPressed = Signal()

    def keyPressEvent(self, event: QKeyEvent) -> None:  # noqa: N802
        if event.key() == Qt.Key_Down:
            self.downPressed.emit()
            return
        super().keyPressEvent(event)


class MainWindow(QMainWindow):
    def __init__(self, wordlist: WordList | None = None) -> None:
        super().__init__()
        self.setWindowTitle("Diacritical Characters")
        self.resize(860, 680)
        self.setMinimumSize(520, 460)

        self.superscript_dict = core.load_superscript_dict()
        self.wordlist = wordlist if wordlist is not None else load_wordlist()
        self.layers: list[str] = []
        self._preview_word = ""
        self._last_pick: tuple[str, float] = ("", 0.0)

        # --- preview -------------------------------------------------------
        self.result_view = ResultView()

        self.copy_button = QPushButton("Copy")
        self.copy_button.setDefault(False)
        self.copy_button.setShortcut("Ctrl+Shift+C")
        self.copy_button.setToolTip("Copy the result to the clipboard (Ctrl+Shift+C)")
        self._copy_reset_timer = QTimer(self)
        self._copy_reset_timer.setSingleShot(True)
        self._copy_reset_timer.setInterval(1400)
        self._copy_reset_timer.timeout.connect(lambda: self.copy_button.setText("Copy"))

        self.preview_hint = QLabel()
        self.preview_hint.setObjectName("hint")

        preview_footer = QHBoxLayout()
        preview_footer.addWidget(self.preview_hint, 1)
        preview_footer.addWidget(self.copy_button)

        preview_frame = QFrame()
        preview_frame.setObjectName("previewFrame")
        preview_layout = QVBoxLayout(preview_frame)
        preview_layout.setContentsMargins(14, 10, 14, 10)
        preview_layout.addWidget(self.result_view, 1)
        preview_layout.addLayout(preview_footer)

        # --- base text + layers ---------------------------------------------
        self.base_input = QLineEdit()
        self.base_input.setPlaceholderText("Base text, e.g. jordan")
        self.base_input.setClearButtonEnabled(True)

        self.layer_chips = QListWidget()
        self.layer_chips.setObjectName("layerChips")
        self.layer_chips.setFlow(QListWidget.LeftToRight)
        self.layer_chips.setWrapping(True)
        self.layer_chips.setResizeMode(QListWidget.Adjust)
        self.layer_chips.setSelectionMode(QListWidget.NoSelection)
        self.layer_chips.setFocusPolicy(Qt.NoFocus)
        self.layer_chips.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.layer_chips.setFixedHeight(50)
        self.layer_chips.setToolTip("Click a layer to remove it")
        self.layer_chips.setCursor(Qt.PointingHandCursor)

        self.clear_button = QPushButton("Clear")
        self.clear_button.setToolTip("Remove all layers")

        base_label = QLabel("Base")
        base_label.setObjectName("sectionLabel")
        layers_label = QLabel("Layers")
        layers_label.setObjectName("sectionLabel")
        label_width = max(base_label.sizeHint().width(), layers_label.sizeHint().width()) + 6
        base_label.setFixedWidth(label_width)
        layers_label.setFixedWidth(label_width)

        base_row = QHBoxLayout()
        base_row.addWidget(base_label)
        base_row.addWidget(self.base_input, 1)

        layers_row = QHBoxLayout()
        layers_row.addWidget(layers_label)
        layers_row.addWidget(self.layer_chips, 1)
        layers_row.addWidget(self.clear_button)

        # --- word picker ------------------------------------------------------
        self.filter_input = FilterEdit()
        self.filter_input.setPlaceholderText("Filter words, or type your own layer and press Enter")
        self.filter_input.setClearButtonEnabled(True)

        self.sort_combo = QComboBox()
        self.sort_combo.addItem("Common first", False)
        self.sort_combo.addItem("A to Z", True)

        self.random_button = QPushButton("Random")
        self.random_button.setToolTip("Add a random common word as a layer")

        picker_row = QHBoxLayout()
        picker_row.addWidget(self.filter_input, 1)
        picker_row.addWidget(self.sort_combo)
        picker_row.addWidget(self.random_button)

        self.word_model = WordModel(self)
        self.word_grid = WordGrid()
        self.word_grid.setModel(self.word_model)
        grid_font = QFont(self.word_grid.font())
        grid_font.setPointSizeF(grid_font.pointSizeF() + 1)
        self.word_grid.setFont(grid_font)

        self.status_label = QLabel()
        self.status_label.setObjectName("hint")
        self.status_label.setWordWrap(True)

        layout = QVBoxLayout()
        layout.setContentsMargins(14, 14, 14, 10)
        layout.setSpacing(10)
        layout.addWidget(preview_frame, 2)
        layout.addLayout(base_row)
        layout.addLayout(layers_row)
        layout.addLayout(picker_row)
        layout.addWidget(self.word_grid, 5)
        layout.addWidget(self.status_label)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)
        self.setStyleSheet(STYLESHEET)

        self.base_input.textChanged.connect(self._on_base_changed)
        self.filter_input.textChanged.connect(self._refresh_words)
        self.filter_input.returnPressed.connect(self._on_filter_return)
        self.filter_input.downPressed.connect(self._focus_grid)
        self.sort_combo.currentIndexChanged.connect(self._refresh_words)
        self.random_button.clicked.connect(self._add_random_layer)
        self.clear_button.clicked.connect(self._clear_layers)
        self.copy_button.clicked.connect(self._copy_result)
        self.layer_chips.itemClicked.connect(self._remove_layer_item)
        self.word_grid.wordPicked.connect(self._on_word_picked)
        self.word_grid.wordPreviewed.connect(self._set_preview_word)

        self._on_base_changed()
        self.base_input.setFocus()

    # --- state helpers -----------------------------------------------------

    def _base_text(self) -> str:
        return self.base_input.text()

    def _target_length(self) -> int:
        return len(self._base_text())

    def _layer_errors(self, layer: str) -> list[str]:
        return core.validate_input_pair(self._base_text(), layer, self.superscript_dict)

    def _mismatched_layers(self) -> list[str]:
        return [layer for layer in self.layers if len(layer) != self._target_length()]

    # --- refresh -----------------------------------------------------------

    def _on_base_changed(self) -> None:
        self.word_grid.set_word_length(self._target_length())
        self._refresh_words()
        self._refresh_result()

    def _refresh_words(self) -> None:
        length = self._target_length()
        query = self.filter_input.text().strip().casefold()
        if length <= 0:
            words: list[str] = []
        else:
            words = self.wordlist.suggest(length, query, alphabetical=bool(self.sort_combo.currentData()))
        self.word_model.set_words(words)
        self.word_grid.scrollToTop()
        self.random_button.setEnabled(bool(words))
        self._set_preview_word("")
        self._refresh_status()

    def _refresh_status(self) -> None:
        length = self._target_length()
        query = self.filter_input.text().strip().casefold()
        count = self.word_model.rowCount()
        if length <= 0:
            self.status_label.setText("Type some base text to see words that fit on top of it.")
            return

        unsupported = sorted({char for char in query if char not in self.superscript_dict})
        letters = f"{length} letter" + ("" if length == 1 else "s")
        if unsupported:
            allowed = " ".join(sorted(self.superscript_dict))
            self.status_label.setText(
                f"No superscript form exists for: {' '.join(unsupported)}. Usable letters: {allowed}"
            )
        elif query and len(query) == length:
            self.status_label.setText(f"Press Enter to add “{query}” as a layer.")
        elif count:
            noun = "word" if count == 1 else "words"
            suffix = f" matching “{query}”" if query else ""
            self.status_label.setText(
                f"{count:,} {noun} with {letters}{suffix}. Hover to preview, click to add."
            )
        elif query:
            self.status_label.setText(
                f"No {letters} words match “{query}”. "
                f"Keep typing to {length} letters and press Enter to use it anyway."
            )
        else:
            self.status_label.setText(f"No suggestions with {letters}. Type your own layer and press Enter.")

    def _refresh_result(self) -> None:
        base = self._base_text()
        mismatched = self._mismatched_layers()
        layers = list(self.layers)
        previewing = bool(self._preview_word) and not mismatched and not self._layer_errors(self._preview_word)
        if previewing:
            layers.append(self._preview_word)

        self.clear_button.setEnabled(bool(self.layers))

        if not base:
            self._show_result("", "Your stacked text will appear here.")
            self.copy_button.setEnabled(False)
            return
        if mismatched:
            self._show_result(
                base,
                f"Layers are {len(mismatched[0])} letters but the base is now {len(base)}. "
                "Fix the base text or clear the layers.",
            )
            self.copy_button.setEnabled(False)
            return
        if not layers:
            self._show_result(base, "Add a layer below to stack letters on top.")
            self.copy_button.setEnabled(False)
            return

        result = core.compose_diacritical_layers(base, layers, self.superscript_dict)
        if previewing:
            hint = f"Preview with “{self._preview_word}”"
        else:
            count = len(self.layers)
            hint = f"{count} layer" + ("" if count == 1 else "s")
        self._show_result(result, hint, dimmed=False)
        self.copy_button.setEnabled(bool(self.layers))

    def _show_result(self, text: str, hint: str, *, dimmed: bool = True) -> None:
        self.result_view.set_content(text, self._base_text(), dimmed=dimmed)
        self.preview_hint.setText(hint)

    # --- actions -----------------------------------------------------------

    def _set_preview_word(self, word: str) -> None:
        if word == self._preview_word:
            return
        self._preview_word = word
        self._refresh_result()

    def _focus_grid(self) -> None:
        if self.word_model.rowCount() == 0:
            return
        self.word_grid.setFocus()
        if not self.word_grid.currentIndex().isValid():
            self.word_grid.setCurrentIndex(self.word_model.index(0))
        else:
            self._set_preview_word(self.word_grid.currentIndex().data() or "")

    def _on_word_picked(self, word: str) -> None:
        # A double-click delivers two clicks; treat it as one pick.
        now = time.monotonic()
        last_word, last_time = self._last_pick
        if word == last_word and now - last_time < QApplication.doubleClickInterval() / 1000:
            return
        self._last_pick = (word, now)
        self.add_layer(word)

    def _on_filter_return(self) -> None:
        typed = self.filter_input.text().strip().casefold()
        if typed and not self._layer_errors(typed):
            candidate = typed
        elif self.word_model.rowCount():
            candidate = self.word_model.words()[0]
        else:
            return
        if self.add_layer(candidate):
            self.filter_input.clear()

    def add_layer(self, word: str) -> bool:
        word = word.strip().casefold()
        if not word or self._layer_errors(word) or self._mismatched_layers():
            return False
        self.layers.append(word)
        self.layer_chips.addItem(QListWidgetItem(f"{word}  ×"))
        self._preview_word = ""
        self._refresh_result()
        return True

    def _add_random_layer(self) -> None:
        pool = self.word_model.words()[:RANDOM_POOL_SIZE]
        if pool:
            self.add_layer(random.choice(pool))

    def _remove_layer_item(self, item: QListWidgetItem) -> None:
        row = self.layer_chips.row(item)
        if row < 0:
            return
        self.layer_chips.takeItem(row)
        self.layers.pop(row)
        self._refresh_result()

    def _clear_layers(self) -> None:
        self.layer_chips.clear()
        self.layers.clear()
        self._refresh_result()

    def _copy_result(self) -> None:
        if not self.layers or self._mismatched_layers():
            return
        text = core.compose_diacritical_layers(self._base_text(), self.layers, self.superscript_dict)
        QApplication.clipboard().setText(text)
        self.copy_button.setText("Copied")
        self._copy_reset_timer.start()


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Diacritical Characters")
    window = MainWindow()
    window.show()
    if "--smoke-test" in sys.argv[1:]:
        # Used by CI to prove the packaged build starts: open, then quit.
        QTimer.singleShot(500, app.quit)
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
