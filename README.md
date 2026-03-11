# Diacritical-Characters

Create goofy words like `tͭͫͩrͣͣͤyͨͭͫpͪͪͦoͦͤͨgͫͫͬrͤͣͣaͭͭͭpͤͥͥhͬͨͨ` using a GUI or CLI.

## Project Layout

- `src/diacritical_characters/core.py`: shared data loading, filtering, suggestions, and composition logic.
- `src/diacritical_characters/corpus.py`: concurrent corpus ingestion pipeline and SQLite datastore/query logic.
- `src/diacritical_characters/gui.py`: PySide6 GUI app.
- `setup.py`: compatibility CLI wrapper for corpus builds.
- `string_maker.py`: CLI wrapper to compose a diacritical string.
- `gui.py`: root launcher for the GUI.

## Quick Start

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Launch GUI

```powershell
python gui.py
```

GUI flow:
- Enter your base text.
- Use the layer input to filter candidates by prefix (for example type `e`).
- Browse the compact, scrollable multi-column candidate table.
- Pick a candidate and add layers to the stack.
- Stack as many layers as you want; output updates from stacked layers.

### Build / Update Corpus Datastore

```powershell
python -m diacritical_characters.corpus build
```

Compatibility wrapper:

```powershell
python setup.py
```

`setup.py` now opens a lightweight build monitor popup (per-source progress table + live log).
Use terminal-only mode instead:

```powershell
python setup.py --cli-progress
```

Full rebuild (ignore incremental skip checks):

```powershell
python -m diacritical_characters.corpus build --full-rebuild
```

Build behavior:
- Downloads run concurrently.
- Processing runs concurrently (parallel parse/normalize workers), with a single serialized SQLite writer.
- `--workers` controls both download and processing concurrency.
- Default runs (`python setup.py` or `python -m diacritical_characters.corpus build`) skip unchanged sources using metadata.
- `--full-rebuild` rebuilds selected sources but reuses cached download artifacts when source metadata is unchanged.

Useful options:

```powershell
python setup.py --workers 8 --min-success 6
python setup.py --source nltk_words --source wordfreq_en
python setup.py --exclude-source wiktextract_raw
python setup.py --db-path data\corpus.sqlite3 --download-dir downloads
```

Available source IDs:
- `nltk_words`
- `wordfreq_en`
- `hunspell_en_us`
- `hunspell_en_gb`
- `wiktextract_raw`
- `wiktionary_internet_lists`
- `ccnet_en_freq`
- `openwebtext_freq`

If NLTK words are missing:

```powershell
python -m nltk.downloader words
```

### CLI Compose

```powershell
python string_maker.py --base jordan --superscript erotic
```

Stack multiple layers in CLI:

```powershell
python string_maker.py --base jordan --superscript erotic --superscript orbita
```
