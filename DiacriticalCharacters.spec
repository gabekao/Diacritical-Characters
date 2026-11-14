# PyInstaller build recipe. Run from the repository root:
#
#     pyinstaller --noconfirm --clean DiacriticalCharacters.spec
#
# Output lands in dist/DiacriticalCharacters/.

a = Analysis(
    ["gui.py"],
    # The package lives under src/, which PyInstaller does not search by default.
    pathex=["src"],
    datas=[("src/diacritical_characters/data/words.txt", "diacritical_characters/data")],
    # The GUI only needs the bundled word list. The corpus build pipeline and its
    # dependencies stay out of the executable.
    excludes=["nltk", "wordfreq", "tqdm", "diacritical_characters.corpus", "diacritical_characters.build_popup"],
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="DiacriticalCharacters",
    console=False,
)
coll = COLLECT(exe, a.binaries, a.datas, name="DiacriticalCharacters")
