from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys

from PySide6.QtCore import QObject, QThread, Signal, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QHeaderView,
)

from . import corpus


@dataclass(frozen=True)
class BuildGuiOptions:
    full_rebuild: bool
    workers: int
    min_success: int
    include_sources: tuple[str, ...] | None = None
    exclude_sources: tuple[str, ...] | None = None
    db_path: Path | None = None
    download_dir: Path | None = None


class BuildRunner(QObject):
    progress = Signal(object)
    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, options: BuildGuiOptions) -> None:
        super().__init__()
        self.options = options

    def run(self) -> None:
        try:
            result = corpus.build_data(
                db_path=self.options.db_path,
                download_dir=self.options.download_dir,
                full_rebuild=self.options.full_rebuild,
                workers=self.options.workers,
                min_success=self.options.min_success,
                include_sources=self.options.include_sources,
                exclude_sources=self.options.exclude_sources,
                progress_callback=self._on_progress,
            )
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))
            return
        self.finished.emit(result)

    def _on_progress(self, progress: corpus.BuildProgress) -> None:
        self.progress.emit(progress)


class BuildMonitorWindow(QMainWindow):
    def __init__(self, options: BuildGuiOptions) -> None:
        super().__init__()
        self.options = options
        self.setWindowTitle("Corpus Build Monitor")
        self.resize(980, 620)

        self.thread: QThread | None = None
        self.runner: BuildRunner | None = None
        self.progress_rows: dict[str, int] = {}
        self.completed_sources: set[str] = set()

        self.status_label = QLabel("Preparing build...")
        self.summary_label = QLabel("")

        selected = set(options.include_sources or corpus.available_source_ids())
        excluded = set(options.exclude_sources or [])
        self.selected_sources = [sid for sid in corpus.available_source_ids() if sid in selected and sid not in excluded]

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, max(1, len(self.selected_sources)))
        self.progress_bar.setValue(0)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["Source", "Phase", "Status", "Bytes", "Records", "Elapsed"])
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionMode(QAbstractItemView.NoSelection)

        self.log_output = QPlainTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumBlockCount(3000)

        self.close_button = QPushButton("Close")
        self.close_button.setEnabled(False)
        self.close_button.clicked.connect(self.close)

        button_row = QHBoxLayout()
        button_row.addStretch(1)
        button_row.addWidget(self.close_button)

        layout = QVBoxLayout()
        layout.addWidget(self.status_label)
        layout.addWidget(self.summary_label)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.table)
        layout.addWidget(QLabel("Build log"))
        layout.addWidget(self.log_output)
        layout.addLayout(button_row)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

    def start(self) -> None:
        self.thread = QThread(self)
        self.runner = BuildRunner(self.options)
        self.runner.moveToThread(self.thread)

        self.thread.started.connect(self.runner.run)
        self.runner.progress.connect(self._on_progress)
        self.runner.finished.connect(self._on_finished)
        self.runner.failed.connect(self._on_failed)
        self.runner.finished.connect(self.thread.quit)
        self.runner.failed.connect(self.thread.quit)
        self.thread.finished.connect(self._on_thread_finished)

        self._log("Build started.")
        self.thread.start()

    def _log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_output.appendPlainText(f"[{timestamp}] {message}")

    def _format_bytes(self, size: int) -> str:
        value = float(size)
        units = ["B", "KB", "MB", "GB", "TB"]
        for unit in units:
            if value < 1024 or unit == units[-1]:
                return f"{value:.1f} {unit}"
            value /= 1024
        return f"{size} B"

    def _ensure_row(self, source_id: str) -> int:
        row = self.progress_rows.get(source_id)
        if row is not None:
            return row
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.progress_rows[source_id] = row
        for col, value in enumerate([source_id, "", "", "", "", ""]):
            self.table.setItem(row, col, QTableWidgetItem(value))
        return row

    def _on_progress(self, progress_obj: object) -> None:
        if not isinstance(progress_obj, corpus.BuildProgress):
            return
        progress = progress_obj

        row = self._ensure_row(progress.source_id)
        self.table.item(row, 1).setText(progress.phase)
        self.table.item(row, 2).setText(progress.status)

        if progress.bytes_total:
            bytes_text = f"{self._format_bytes(progress.bytes_downloaded)} / {self._format_bytes(progress.bytes_total)}"
        elif progress.bytes_downloaded:
            bytes_text = self._format_bytes(progress.bytes_downloaded)
        else:
            bytes_text = ""
        self.table.item(row, 3).setText(bytes_text)

        self.table.item(row, 4).setText(str(progress.records) if progress.records else "")
        self.table.item(row, 5).setText(f"{progress.elapsed_seconds:.1f}s" if progress.elapsed_seconds else "")

        status_line = f"{progress.source_id}: {progress.phase}/{progress.status}"
        if progress.message:
            status_line = f"{status_line} - {progress.message}"
        self.status_label.setText(status_line)

        if progress.status in {"success", "failed", "skipped"} and progress.source_id not in self.completed_sources:
            self.completed_sources.add(progress.source_id)
            self.progress_bar.setValue(len(self.completed_sources))
            self._log(status_line)
        elif progress.status in {"failed", "cached"} and progress.message:
            self._log(status_line)

    def _on_finished(self, result_obj: object) -> None:
        if isinstance(result_obj, corpus.BuildResult):
            result = result_obj
            self.summary_label.setText(
                (
                    f"words={result.total_words}, links={result.total_word_sources}, "
                    f"sources success={result.successful_sources}, skipped={result.skipped_sources}, failed={result.failed_sources}"
                )
            )
            self._log("Build completed successfully.")
        else:
            self.summary_label.setText("Build completed.")
            self._log("Build completed.")

        self.status_label.setText("Build finished.")
        self.progress_bar.setValue(self.progress_bar.maximum())
        self.close_button.setEnabled(True)

    def _on_failed(self, error: str) -> None:
        self.status_label.setText(f"Build failed: {error}")
        self.summary_label.setText("Build failed.")
        self._log(f"Build failed: {error}")
        self.close_button.setEnabled(True)

    def _on_thread_finished(self) -> None:
        if self.runner is not None:
            self.runner.deleteLater()
        if self.thread is not None:
            self.thread.deleteLater()
        self.runner = None
        self.thread = None


def run_build_popup(options: BuildGuiOptions) -> int:
    app = QApplication.instance()
    owns_app = app is None
    if app is None:
        app = QApplication(sys.argv)

    window = BuildMonitorWindow(options)
    window.show()
    window.start()

    if owns_app:
        return app.exec()
    return 0
