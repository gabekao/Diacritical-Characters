"use strict";

// Browser port of the desktop app. The logic mirrors the Python package:
//   *_SUPERSCRIPTS / compose  <-> core.py
//   WordList                  <-> wordlist.py
// Both front ends read the same words.txt / words_extended.txt, ranked
// most-common-first.

// Latin letters that exist as combining superscripts in the original block
// (U+0363..U+036F). Nearly every font that handles combining marks has these.
const BASIC_SUPERSCRIPTS = {
  a: "\u0363", e: "\u0364", i: "\u0365", o: "\u0366", u: "\u0367",
  c: "\u0368", d: "\u0369", h: "\u036a", m: "\u036b", r: "\u036c",
  t: "\u036d", v: "\u036e", x: "\u036f",
};

// Added later in Combining Diacritical Marks Supplement. Font support is
// patchy, so these are opt-in. j, q and y have no combining form at all.
const EXTRA_SUPERSCRIPTS = {
  b: "\u1de8", f: "\u1deb", g: "\u1dda", k: "\u1ddc", l: "\u1ddd",
  n: "\u1de0", p: "\u1dee", s: "\u1de4", w: "\u1df1", z: "\u1de6",
};

const ALL_SUPERSCRIPTS = { ...BASIC_SUPERSCRIPTS, ...EXTRA_SUPERSCRIPTS };

// On GitHub Pages the deploy workflow copies the lists next to this file.
// The second path lets the page run straight from a checkout of the repo.
const WORDLIST_FILES = { basic: "words.txt", extended: "words_extended.txt" };
const wordListUrls = (file) => [file, `../src/diacritical_characters/data/${file}`];
const STORAGE_KEY = "diacritical-characters:extended";

const PAGE_SIZE = 600; // words rendered at a time; more load as the grid scrolls
const RANDOM_POOL_SIZE = 300;
const MAX_FONT_PX = 54;
const MIN_FONT_PX = 20;
const DOUBLE_CLICK_MS = 400;

const chars = (text) => Array.from(text);

// Composing always knows every letter; which letters may be *added* is
// decided by the More letters toggle (see unsupportedLetters).
function compose(base, layers) {
  return chars(base)
    .map((char, index) => char + layers.map((layer) => ALL_SUPERSCRIPTS[layer[index]]).join(""))
    .join("");
}

const superscripts = () => (state.extended ? ALL_SUPERSCRIPTS : BASIC_SUPERSCRIPTS);

function unsupportedLetters(text) {
  const allowed = superscripts();
  return [...new Set(chars(text).filter((char) => !(char in allowed)))].sort();
}

const onlyExtraLetters = (letters) => letters.length > 0 && letters.every((char) => char in EXTRA_SUPERSCRIPTS);

class WordList {
  constructor(words) {
    this.byLength = new Map();
    for (const word of words) {
      if (!this.byLength.has(word.length)) this.byLength.set(word.length, []);
      this.byLength.get(word.length).push(word);
    }
    this.sortedCache = new Map();
  }

  wordsOfLength(length, alphabetical) {
    const ranked = this.byLength.get(length) || [];
    if (!alphabetical) return ranked;
    if (!this.sortedCache.has(length)) this.sortedCache.set(length, [...ranked].sort());
    return this.sortedCache.get(length);
  }

  // Words starting with the query first, then words that merely contain it.
  suggest(length, query, alphabetical) {
    const words = this.wordsOfLength(length, alphabetical);
    if (!query) return words;
    const starts = [];
    const contains = [];
    for (const word of words) {
      const at = word.indexOf(query);
      if (at === 0) starts.push(word);
      else if (at > 0) contains.push(word);
    }
    return starts.concat(contains);
  }
}

async function loadWordList(file) {
  for (const url of wordListUrls(file)) {
    try {
      const response = await fetch(url);
      if (!response.ok) continue;
      const text = await response.text();
      return new WordList(text.split(/\r?\n/).filter(Boolean));
    } catch (error) {
      // Try the next location.
    }
  }
  throw new Error("Could not load the word list.");
}

// --- UI ---------------------------------------------------------------------

const el = {
  result: document.getElementById("result"),
  stage: document.querySelector(".result-stage"),
  previewHint: document.getElementById("preview-hint"),
  copy: document.getElementById("copy"),
  base: document.getElementById("base"),
  layers: document.getElementById("layers"),
  clear: document.getElementById("clear"),
  filter: document.getElementById("filter"),
  sort: document.getElementById("sort"),
  random: document.getElementById("random"),
  extended: document.getElementById("extended"),
  warning: document.getElementById("extended-warning"),
  warningSample: document.getElementById("extended-sample"),
  words: document.getElementById("words"),
  status: document.getElementById("status"),
};

const state = {
  extended: false,
  // A list is fetched the first time it is needed, so the extended one costs
  // nothing for people who never turn it on.
  lists: {
    basic: { words: null, error: "", loading: null },
    extended: { words: null, error: "", loading: null },
  },
  layers: [],
  previewWord: "",
  matches: [],
  rendered: 0,
  lastPick: { word: "", time: 0 },
};

const baseText = () => el.base.value;
const targetLength = () => chars(baseText()).length;
const query = () => el.filter.value.trim().toLowerCase();
const plural = (count, noun) => `${count.toLocaleString()} ${noun}${count === 1 ? "" : "s"}`;

const currentList = () => state.lists[state.extended ? "extended" : "basic"];

function layerIsValid(word) {
  return word.length === targetLength() && unsupportedLetters(word).length === 0;
}

function mismatchedLayers() {
  return state.layers.filter((layer) => layer.length !== targetLength());
}

// Layers that use extra letters after More letters was switched back off.
function unsupportedLayers() {
  return state.layers.filter((layer) => unsupportedLetters(layer).length > 0);
}

const hasInvalidLayers = () => mismatchedLayers().length > 0 || unsupportedLayers().length > 0;

function refreshWords() {
  const length = targetLength();
  const list = currentList().words;
  state.matches = length > 0 && list ? list.suggest(length, query(), el.sort.value === "alpha") : [];
  state.rendered = 0;
  state.previewWord = "";
  el.words.replaceChildren();
  el.words.style.setProperty("--word-length", Math.max(1, length));
  el.words.scrollTop = 0;
  renderMoreWords();
  el.random.disabled = state.matches.length === 0;
  refreshStatus();
  refreshResult();
}

function renderMoreWords() {
  const fragment = document.createDocumentFragment();
  const end = Math.min(state.matches.length, state.rendered + PAGE_SIZE);
  for (let index = state.rendered; index < end; index += 1) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "word";
    button.textContent = state.matches[index];
    fragment.appendChild(button);
  }
  state.rendered = end;
  el.words.appendChild(fragment);
}

function refreshStatus() {
  const length = targetLength();
  const typed = query();
  const count = state.matches.length;
  const letters = plural(length, "letter");
  const list = currentList();
  const unsupported = unsupportedLetters(typed);
  let message;
  if (list.error) {
    message = `${list.error} You can still type your own layers.`;
  } else if (length === 0) {
    message = "Type some base text to see words that fit on top of it.";
  } else if (!list.words) {
    message = "Loading words…";
  } else if (unsupported.length && !state.extended && onlyExtraLetters(unsupported)) {
    message = `${unsupported.join(" ")} only work with More letters turned on.`;
  } else if (unsupported.length) {
    message = `No superscript form exists for: ${unsupported.join(" ")}. ` +
      `Usable letters: ${Object.keys(superscripts()).sort().join(" ")}`;
  } else if (typed && typed.length === length) {
    message = `Press Enter to add “${typed}” as a layer.`;
  } else if (count) {
    const suffix = typed ? ` matching “${typed}”` : "";
    message = `${plural(count, "word")} with ${letters}${suffix}. Hover to preview, click to add.`;
  } else if (typed) {
    message = `No words with ${letters} match “${typed}”. ` +
      `Keep typing to ${letters} and press Enter to use it anyway.`;
  } else {
    message = `No suggestions with ${letters}. Type your own layer and press Enter.`;
  }
  el.status.textContent = message;
}

function refreshResult() {
  const base = baseText();
  const mismatched = mismatchedLayers();
  const unsupported = unsupportedLayers();
  const layers = [...state.layers];
  const previewing = Boolean(state.previewWord) && !hasInvalidLayers() && layerIsValid(state.previewWord);
  if (previewing) layers.push(state.previewWord);

  el.clear.disabled = state.layers.length === 0;
  let text = base;
  let hint;
  let dimmed = true;

  if (!base) {
    hint = "Your stacked text will appear here.";
  } else if (mismatched.length) {
    hint = `Layers are ${plural(mismatched[0].length, "letter")} but the base is now ${targetLength()}. ` +
      "Fix the base text or clear the layers.";
  } else if (unsupported.length) {
    hint = `“${unsupported[0]}” uses ${unsupportedLetters(unsupported[0]).join(" ")}. ` +
      "Turn More letters back on or remove that layer.";
  } else if (!layers.length) {
    hint = "Add a layer below to stack letters on top.";
  } else {
    text = compose(base, layers);
    hint = previewing ? `Preview with “${state.previewWord}”` : plural(state.layers.length, "layer");
    dimmed = false;
  }

  el.result.textContent = text;
  el.result.classList.toggle("dimmed", dimmed);
  // Always size for one more layer than is committed, so the text is the same
  // size whether or not a word is being previewed.
  el.result.style.setProperty("--layers", state.layers.length + 1);
  el.previewHint.textContent = hint;
  el.copy.disabled = !(base && state.layers.length && !hasInvalidLayers());
  fitResult();
}

// The stage has a fixed size so that previewing a word never moves the grid
// under the pointer. Shrink the text until it fits: long base text limits the
// width (combining marks add none), tall stacks limit the height.
function fitResult() {
  const style = getComputedStyle(el.stage);
  const maxWidth = el.stage.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
  const maxHeight = el.stage.clientHeight - parseFloat(style.paddingTop) - parseFloat(style.paddingBottom);
  let size = MAX_FONT_PX;
  el.result.style.fontSize = `${size}px`;
  while (size > MIN_FONT_PX && (el.result.scrollWidth > maxWidth || el.result.offsetHeight > maxHeight)) {
    size -= 2;
    el.result.style.fontSize = `${size}px`;
  }
}

function renderLayers() {
  el.layers.replaceChildren(
    ...state.layers.map((layer, index) => {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "chip";
      chip.dataset.index = String(index);
      chip.setAttribute("role", "listitem");
      chip.setAttribute("aria-label", `Remove layer ${layer}`);
      chip.title = "Click to remove";
      chip.append(layer);
      const cross = document.createElement("span");
      cross.textContent = "×";
      chip.appendChild(cross);
      return chip;
    }),
  );
  el.layers.scrollTop = el.layers.scrollHeight; // keep the newest chip in view
}

function addLayer(word) {
  const layer = word.trim().toLowerCase();
  if (!layer || !layerIsValid(layer) || hasInvalidLayers()) return false;
  state.layers.push(layer);
  state.previewWord = "";
  renderLayers();
  refreshResult();
  return true;
}

function setPreview(word) {
  if (word === state.previewWord) return;
  state.previewWord = word;
  refreshResult();
}

const wordOf = (target) => (target instanceof Element && target.closest(".word")?.textContent) || "";

el.base.addEventListener("input", refreshWords);
el.filter.addEventListener("input", refreshWords);
el.sort.addEventListener("change", refreshWords);
el.words.addEventListener("scroll", () => {
  const nearBottom = el.words.scrollTop + el.words.clientHeight > el.words.scrollHeight - 300;
  if (nearBottom && state.rendered < state.matches.length) renderMoreWords();
});

el.filter.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    const typed = query();
    const candidate = typed && layerIsValid(typed) ? typed : state.matches[0];
    if (candidate && addLayer(candidate)) {
      el.filter.value = "";
      refreshWords();
    }
  } else if (event.key === "ArrowDown") {
    el.words.querySelector(".word")?.focus();
    event.preventDefault();
  }
});

el.words.addEventListener("click", (event) => {
  const word = wordOf(event.target);
  if (!word) return;
  // A double-click delivers two clicks; treat it as one pick.
  const now = performance.now();
  if (word === state.lastPick.word && now - state.lastPick.time < DOUBLE_CLICK_MS) return;
  state.lastPick = { word, time: now };
  addLayer(word);
});
el.words.addEventListener("mouseover", (event) => setPreview(wordOf(event.target)));
el.words.addEventListener("mouseleave", () => setPreview(""));
el.words.addEventListener("focusin", (event) => setPreview(wordOf(event.target)));
el.words.addEventListener("focusout", () => setPreview(""));

el.layers.addEventListener("click", (event) => {
  const chip = event.target instanceof Element ? event.target.closest(".chip") : null;
  if (!chip) return;
  state.layers.splice(Number(chip.dataset.index), 1);
  renderLayers();
  refreshResult();
});

el.clear.addEventListener("click", () => {
  state.layers = [];
  renderLayers();
  refreshResult();
});

el.random.addEventListener("click", () => {
  const pool = state.matches.slice(0, RANDOM_POOL_SIZE);
  if (pool.length) addLayer(pool[Math.floor(Math.random() * pool.length)]);
});

el.copy.addEventListener("click", async () => {
  const text = compose(baseText(), state.layers);
  try {
    await navigator.clipboard.writeText(text);
  } catch (error) {
    // Clipboard API unavailable (e.g. not a secure context): fall back.
    const scratch = document.createElement("textarea");
    scratch.value = text;
    document.body.appendChild(scratch);
    scratch.select();
    document.execCommand("copy");
    scratch.remove();
  }
  el.copy.textContent = "Copied";
  setTimeout(() => { el.copy.textContent = "Copy"; }, 1400);
});

window.addEventListener("resize", fitResult);

// --- More letters ---------------------------------------------------------

function ensureList(name) {
  const entry = state.lists[name];
  if (entry.words || entry.loading) return;
  entry.loading = loadWordList(WORDLIST_FILES[name])
    .then((words) => { entry.words = words; })
    .catch((error) => { entry.error = error.message; })
    .finally(() => {
      entry.loading = null;
      refreshWords();
    });
}

function setExtended(on) {
  state.extended = on;
  el.extended.checked = on;
  try {
    localStorage.setItem(STORAGE_KEY, on ? "1" : "0");
  } catch (error) {
    // Storage blocked (private window etc.); the toggle just won't persist.
  }
  if (on) ensureList("extended");
  refreshWords();
}

// The warning shows each extra letter stacked on itself, so people can see
// whether their own device draws them.
el.warningSample.replaceChildren(
  ...Object.entries(EXTRA_SUPERSCRIPTS).map(([letter, mark]) => {
    const span = document.createElement("span");
    span.textContent = letter + mark;
    return span;
  }),
);

el.extended.addEventListener("change", () => {
  if (!el.extended.checked) {
    setExtended(false);
    return;
  }
  el.extended.checked = false; // stays off unless the warning is confirmed
  if (typeof el.warning.showModal === "function") {
    el.warning.returnValue = "";
    el.warning.showModal();
  } else if (window.confirm("The extra letters do not display on many devices. Turn them on anyway?")) {
    setExtended(true);
  }
});

el.warning.addEventListener("close", () => {
  if (el.warning.returnValue === "on") setExtended(true);
  el.extended.focus();
});

let savedExtended = false;
try {
  savedExtended = localStorage.getItem(STORAGE_KEY) === "1";
} catch (error) {
  // Storage blocked; start with the basic letters.
}

ensureList("basic");
setExtended(savedExtended);
