from __future__ import annotations

import sys

from PySide6.QtCore import QObject, QThread, Signal, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QFormLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from . import core

CANDIDATE_LIMIT = 400


class BuildWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)

    def run(self) -> None:
        try:
            result = core.build_data()
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))
            return
        self.finished.emit(result)


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Diacritical String Builder")
        self.resize(820, 640)

        self.superscript_dict: dict[str, str] = {}
        self.suggestion_index: dict[int, dict[str, list[str]]] = {}
        self.stacked_layers: list[str] = []
        self.worker_thread: QThread | None = None
        self.worker: BuildWorker | None = None

        self.base_input = QLineEdit()
        self.base_input.setPlaceholderText("Base text (example: jordan)")

        self.layer_input = QLineEdit()
        self.layer_input.setPlaceholderText("Type to filter candidate list (example: e...)")

        self.result_output = QLineEdit()
        self.result_output.setReadOnly(True)

        self.allowed_label = QLabel()
        self.layer_info_label = QLabel("Stacked layers: 0")
        self.layer_info_label.setWordWrap(True)
        self.candidate_info_label = QLabel()
        self.candidate_info_label.setWordWrap(True)
        self.status_label = QLabel()
        self.status_label.setWordWrap(True)

        self.copy_button = QPushButton("Copy")
        self.copy_button.setEnabled(False)
        self.add_layer_button = QPushButton("Add Layer")
        self.remove_layer_button = QPushButton("Remove Selected Layer")
        self.clear_layers_button = QPushButton("Clear Layers")
        self.build_button = QPushButton("Build Data")
        self.build_button.hide()

        self.layer_list = QListWidget()
        self.layer_list.setMinimumHeight(120)

        self.candidate_table = QTableWidget(0, 1)
        self.candidate_table.setMinimumHeight(260)
        self.candidate_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.candidate_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.candidate_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.candidate_table.setShowGrid(False)
        self.candidate_table.setWordWrap(False)
        self.candidate_table.verticalHeader().setVisible(False)
        self.candidate_table.horizontalHeader().setVisible(False)
        self.candidate_table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.candidate_table.verticalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.candidate_table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.candidate_table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.candidate_table.setStyleSheet("QTableWidget::item { padding: 1px 4px; }")

        form = QFormLayout()
        form.addRow("Base text", self.base_input)
        form.addRow("Layer filter / input", self.layer_input)
        form.addRow("Result", self.result_output)

        actions = QHBoxLayout()
        actions.addWidget(self.add_layer_button)
        actions.addWidget(self.remove_layer_button)
        actions.addWidget(self.clear_layers_button)
        actions.addWidget(self.copy_button)
        actions.addWidget(self.build_button)
        actions.addStretch(1)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addWidget(self.allowed_label)
        layout.addLayout(actions)
        layout.addWidget(self.layer_info_label)
        layout.addWidget(self.layer_list)
        layout.addWidget(self.candidate_info_label)
        layout.addWidget(self.candidate_table)
        layout.addWidget(self.status_label)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        self.base_input.textChanged.connect(self._on_context_changed)
        self.layer_input.textChanged.connect(self._on_filter_changed)
        self.layer_input.returnPressed.connect(self._add_current_layer)
        self.add_layer_button.clicked.connect(self._add_current_layer)
        self.remove_layer_button.clicked.connect(self._remove_selected_layer)
        self.clear_layers_button.clicked.connect(self._clear_layers)
        self.copy_button.clicked.connect(self._copy_result)
        self.build_button.clicked.connect(self._start_build_data)
        self.candidate_table.cellClicked.connect(self._on_candidate_clicked)
        self.candidate_table.cellDoubleClicked.connect(self._on_candidate_double_clicked)

        self._load_initial_data()
        self._on_context_changed()

    def _set_status(self, message: str, color: str = "#8a0000") -> None:
        self.status_label.setStyleSheet(f"color: {color};")
        self.status_label.setText(message)

    def _load_initial_data(self) -> None:
        try:
            self.superscript_dict = core.load_superscript_dict()
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"Failed to load superscript mapping: {exc}")
            self.base_input.setEnabled(False)
            self.layer_input.setEnabled(False)
            self.copy_button.setEnabled(False)
            self.add_layer_button.setEnabled(False)
            return

        allowed = "".join(sorted(self.superscript_dict.keys()))
        self.allowed_label.setText(f"Allowed superscript letters: {allowed}")

        try:
            self.suggestion_index = core.load_suggestion_index()
            self._set_status("Ready.", "#006400")
        except FileNotFoundError:
            self.suggestion_index = {}
            self._set_status("Suggestion data not found. Click Build Data to generate it.", "#8a4d00")
            self.build_button.show()
        except Exception as exc:  # noqa: BLE001
            self.suggestion_index = {}
            self._set_status(f"Could not load suggestion data: {exc}. Click Build Data to rebuild.", "#8a4d00")
            self.build_button.show()

    def _on_context_changed(self) -> None:
        self._update_candidates()
        self._update_result()

    def _on_filter_changed(self) -> None:
        self._update_candidates()

    def _update_candidates(self) -> None:
        self.candidate_table.clearContents()
        self.candidate_table.setRowCount(0)
        self.candidate_table.setColumnCount(1)

        if not self.suggestion_index:
            self.candidate_info_label.setText("Candidates unavailable until data is built.")
            return

        target_length = len(self.base_input.text())
        if target_length <= 0:
            self.candidate_info_label.setText("Enter base text to load candidate layers.")
            return

        prefix = self.layer_input.text().strip()
        candidates = core.suggest_superscript_words(
            self.suggestion_index,
            target_length=target_length,
            prefix=prefix,
            limit=CANDIDATE_LIMIT,
        )

        if not candidates:
            if prefix:
                self.candidate_info_label.setText(
                    f"No matches for prefix '{prefix}' at length {target_length}."
                )
            else:
                self.candidate_info_label.setText(f"No candidates for length {target_length}.")
            return

        metrics = self.candidate_table.fontMetrics()
        row_height = max(16, metrics.height() + 2)
        sample = "m" * max(1, target_length)
        col_width = max(50, metrics.horizontalAdvance(sample) + 16)
        viewport_width = max(1, self.candidate_table.viewport().width())
        col_count = max(1, min(len(candidates), viewport_width // col_width))
        row_count = (len(candidates) + col_count - 1) // col_count

        self.candidate_table.setColumnCount(col_count)
        self.candidate_table.setRowCount(row_count)

        for idx, word in enumerate(candidates):
            row = idx // col_count
            col = idx % col_count
            item = QTableWidgetItem(word)
            item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            self.candidate_table.setItem(row, col, item)

        for col in range(col_count):
            self.candidate_table.setColumnWidth(col, col_width)
        for row in range(row_count):
            self.candidate_table.setRowHeight(row, row_height)

        if prefix:
            detail = f"prefix '{prefix}'"
        else:
            detail = "all starts"
        self.candidate_info_label.setText(
            (
                f"Candidates for length {target_length} ({detail}). "
                f"Showing up to {CANDIDATE_LIMIT}; loaded {len(candidates)} in {col_count} columns."
            )
        )

    def _update_result(self) -> None:
        if not self.superscript_dict:
            self.result_output.clear()
            self.copy_button.setEnabled(False)
            return

        self.layer_info_label.setText(f"Stacked layers: {len(self.stacked_layers)}")

        base_text = self.base_input.text()
        result, errors = core.compose_layers_or_errors(base_text, self.stacked_layers, self.superscript_dict)

        if errors:
            self.result_output.clear()
            self.copy_button.setEnabled(False)
            if self.stacked_layers:
                self._set_status(" | ".join(errors), "#8a0000")
            else:
                self._set_status("Add at least one layer to generate output.", "#8a4d00")
            if not self.suggestion_index:
                self.build_button.show()
            return

        self.result_output.setText(result)
        self.copy_button.setEnabled(bool(result))
        self._set_status(f"Ready. Active layers: {len(self.stacked_layers)}.", "#006400")

    def _add_current_layer(self) -> None:
        if not self.superscript_dict:
            return

        candidate = self.layer_input.text().strip().lower()
        if not candidate:
            self._set_status("Type or choose a layer before adding.", "#8a4d00")
            return

        errors = core.validate_input_pair(self.base_input.text(), candidate, self.superscript_dict)
        if errors:
            self._set_status(" | ".join(errors), "#8a0000")
            return

        self.stacked_layers.append(candidate)
        self.layer_list.addItem(candidate)
        self.layer_input.clear()
        self._update_candidates()
        self._update_result()

    def _remove_selected_layer(self) -> None:
        row = self.layer_list.currentRow()
        if row < 0:
            self._set_status("Select a stacked layer to remove.", "#8a4d00")
            return
        self.layer_list.takeItem(row)
        self.stacked_layers.pop(row)
        self._update_result()

    def _clear_layers(self) -> None:
        self.layer_list.clear()
        self.stacked_layers.clear()
        self._update_result()

    def _on_candidate_clicked(self, row: int, col: int) -> None:
        item = self.candidate_table.item(row, col)
        if item is not None:
            self.layer_input.setText(item.text())

    def _on_candidate_double_clicked(self, row: int, col: int) -> None:
        item = self.candidate_table.item(row, col)
        if item is not None:
            self.layer_input.setText(item.text())
            self._add_current_layer()

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._update_candidates()

    def _copy_result(self) -> None:
        text = self.result_output.text()
        if not text:
            return
        QApplication.clipboard().setText(text)
        self._set_status("Copied to clipboard.", "#006400")

    def _start_build_data(self) -> None:
        if self.worker_thread is not None:
            return

        self.build_button.setEnabled(False)
        self._set_status("Building data... this may take a while.", "#005c80")

        self.worker_thread = QThread(self)
        self.worker = BuildWorker()
        self.worker.moveToThread(self.worker_thread)

        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self._on_build_finished)
        self.worker.failed.connect(self._on_build_failed)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.failed.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self._cleanup_worker)

        self.worker_thread.start()

    def _on_build_finished(self, result: object) -> None:
        try:
            self.suggestion_index = core.load_suggestion_index()
        except Exception as exc:  # noqa: BLE001
            self._set_status(f"Build completed but reloading suggestions failed: {exc}", "#8a4d00")
            self.build_button.setEnabled(True)
            self.build_button.show()
            return

        if isinstance(result, core.BuildResult):
            self._set_status(
                (
                    f"Data built: {result.filtered_words} filtered words "
                    f"across {result.length_buckets} length buckets."
                ),
                "#006400",
            )
        else:
            self._set_status("Data built successfully.", "#006400")

        self.build_button.hide()
        self._on_context_changed()

    def _on_build_failed(self, message: str) -> None:
        self._set_status(f"Build failed: {message}", "#8a0000")
        self.build_button.setEnabled(True)
        self.build_button.show()

    def _cleanup_worker(self) -> None:
        if self.worker is not None:
            self.worker.deleteLater()
        if self.worker_thread is not None:
            self.worker_thread.deleteLater()
        self.worker = None
        self.worker_thread = None
        self.build_button.setEnabled(True)


def main() -> int:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
