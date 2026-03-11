# Diacritical-Characters

Create goofy words like `tͭͫͩrͣͣͤyͨͭͫpͪͪͦoͦͤͨgͫͫͬrͤͣͣaͭͭͭpͤͥͥhͬͨͨ` using a GUI or CLI.

## Project Layout

- `src/diacritical_characters/core.py`: shared data loading, filtering, suggestions, and composition logic.
- `src/diacritical_characters/gui.py`: PySide6 GUI app.
- `setup.py`: CLI wrapper to (re)build suggestion data.
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

### Rebuild Data (if suggestions are missing)

```powershell
python setup.py
```

Optional full regeneration from NLTK corpus:

```powershell
python setup.py --force-generate-words
```

If NLTK corpus is missing:

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
