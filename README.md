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

## GitHub Actions EXE Build

- Workflow: `.github/workflows/build-gui-exe.yml`
- Trigger: manual (`workflow_dispatch`) or push to `main` touching app files.
- Output: `DiacriticalCharacters-windows` artifact containing the Windows GUI build from PyInstaller.

## Versioned Release Publishing

- Workflow: `.github/workflows/release-gui-exe.yml`
- Trigger: push a version tag like `v1.0.0`
- Output: GitHub Release named by the tag, with `DiacriticalCharacters-<tag>-windows.zip` attached.

Example:

```powershell
git tag v1.0.0
git push origin v1.0.0
```
