"""Map file paths to user-friendly categories by extension.

Used by the Run-tab Log widget to group skipped paths under
human-readable headings (Documents / Photos / Videos / Music / …)
instead of by-reason headings (permission denied / excluded by
pattern / …) — the user thinks in "did my photos get backed up?",
not in "what kind of error stopped my files?".

Categorization is purely lexical: the extension is split on the
last ``.`` and looked up in a static dict. No magic-byte sniffing,
no MIME parsing — fast and deterministic on tens of thousands of
paths. Files without a recognized extension (or without an
extension at all, including bare directories accumulated by
exclude-pattern matches) fall into ``OTHER``.

The category set is intentionally small and stable:

- ``DOCUMENTS`` — Office, PDF, e-books, plain text, notes.
- ``PHOTOS`` — JPEG/PNG/HEIC/AVIF, RAW (all camera makers), vector
  & illustration sources (.psd, .ai, .svg).
- ``VIDEOS`` — common containers, MPEG family, pro RAW vidéo
  (.mxf, .braw, .r3d, .ari).
- ``MUSIC`` — lossy/lossless audio, DSD, MIDI, trackers, playlists,
  DAW project files (.als, .flp, etc.).
- ``ARCHIVES`` — zip/rar/7z/tar/iso family.
- ``CODE_DATA`` — programming source, config, markup, shell.
- ``OTHER`` — fallback.

Adding an extension here is a one-line change. Adding a category is
a deliberate decision — keep the count low so the Run-tab Log stays
readable.
"""

from __future__ import annotations

from pathlib import Path

# Category labels — exposed as module constants so callers compare
# against ``CATEGORY_PHOTOS`` rather than the literal string ``"Photos"``,
# which keeps a future rename to a localized label cheap.
CATEGORY_DOCUMENTS = "Documents"
CATEGORY_PHOTOS = "Photos"
CATEGORY_VIDEOS = "Videos"
CATEGORY_MUSIC = "Music"
CATEGORY_ARCHIVES = "Archives"
CATEGORY_CODE_DATA = "Code & data"
CATEGORY_OTHER = "Other"

# Display order in the Run-tab Log. Documents / Photos / Videos /
# Music first because that is the typical user mental model
# ("are my documents safe? are my photos safe?"). Code & data near
# the end because it is mostly a developer concern. Other last as
# the catch-all.
CATEGORY_ORDER: tuple[str, ...] = (
    CATEGORY_DOCUMENTS,
    CATEGORY_PHOTOS,
    CATEGORY_VIDEOS,
    CATEGORY_MUSIC,
    CATEGORY_ARCHIVES,
    CATEGORY_CODE_DATA,
    CATEGORY_OTHER,
)


# Extension → category. Keys are lowercase including the leading dot.
# A file whose extension is missing from this dict falls into ``OTHER``;
# files without any extension also fall into ``OTHER``.
#
# Sources for the lists were validated against the spec discussion in
# the project — when adding extensions consider whether the user
# actually has them in their ``Documents``/``Pictures``/``Music``/
# ``Videos`` library; obscure formats are better left in ``OTHER``
# than scattered as one-offs across multiple categories.
_EXTENSION_TO_CATEGORY: dict[str, str] = {
    # ---- Documents ----
    # Microsoft Word
    ".doc": CATEGORY_DOCUMENTS,
    ".docx": CATEGORY_DOCUMENTS,
    ".docm": CATEGORY_DOCUMENTS,
    ".dot": CATEGORY_DOCUMENTS,
    ".dotx": CATEGORY_DOCUMENTS,
    ".dotm": CATEGORY_DOCUMENTS,
    # Microsoft Excel
    ".xls": CATEGORY_DOCUMENTS,
    ".xlsx": CATEGORY_DOCUMENTS,
    ".xlsm": CATEGORY_DOCUMENTS,
    ".xlsb": CATEGORY_DOCUMENTS,
    ".xlt": CATEGORY_DOCUMENTS,
    ".xltx": CATEGORY_DOCUMENTS,
    ".xltm": CATEGORY_DOCUMENTS,
    # Microsoft PowerPoint
    ".ppt": CATEGORY_DOCUMENTS,
    ".pptx": CATEGORY_DOCUMENTS,
    ".pptm": CATEGORY_DOCUMENTS,
    ".pps": CATEGORY_DOCUMENTS,
    ".ppsx": CATEGORY_DOCUMENTS,
    ".pot": CATEGORY_DOCUMENTS,
    ".potx": CATEGORY_DOCUMENTS,
    # OpenDocument
    ".odt": CATEGORY_DOCUMENTS,
    ".ods": CATEGORY_DOCUMENTS,
    ".odp": CATEGORY_DOCUMENTS,
    ".odg": CATEGORY_DOCUMENTS,
    ".odf": CATEGORY_DOCUMENTS,
    ".fodt": CATEGORY_DOCUMENTS,
    ".fods": CATEGORY_DOCUMENTS,
    ".fodp": CATEGORY_DOCUMENTS,
    # Apple iWork
    ".pages": CATEGORY_DOCUMENTS,
    ".numbers": CATEGORY_DOCUMENTS,
    ".key": CATEGORY_DOCUMENTS,
    # PDF & e-books
    ".pdf": CATEGORY_DOCUMENTS,
    ".epub": CATEGORY_DOCUMENTS,
    ".mobi": CATEGORY_DOCUMENTS,
    ".azw": CATEGORY_DOCUMENTS,
    ".azw3": CATEGORY_DOCUMENTS,
    ".kfx": CATEGORY_DOCUMENTS,
    ".djvu": CATEGORY_DOCUMENTS,
    ".fb2": CATEGORY_DOCUMENTS,
    # Plain & rich text
    ".txt": CATEGORY_DOCUMENTS,
    ".text": CATEGORY_DOCUMENTS,
    ".rtf": CATEGORY_DOCUMENTS,
    ".markdown": CATEGORY_DOCUMENTS,
    ".rst": CATEGORY_DOCUMENTS,
    ".adoc": CATEGORY_DOCUMENTS,
    ".tex": CATEGORY_DOCUMENTS,
    ".latex": CATEGORY_DOCUMENTS,
    # Notes
    ".one": CATEGORY_DOCUMENTS,
    ".onetoc2": CATEGORY_DOCUMENTS,
    ".enex": CATEGORY_DOCUMENTS,
    ".org": CATEGORY_DOCUMENTS,
    ".notes": CATEGORY_DOCUMENTS,
    # Tabular data
    ".csv": CATEGORY_DOCUMENTS,
    ".tsv": CATEGORY_DOCUMENTS,
    # Microsoft Publisher / Visio
    ".pub": CATEGORY_DOCUMENTS,
    ".vsd": CATEGORY_DOCUMENTS,
    ".vsdx": CATEGORY_DOCUMENTS,
    ".vss": CATEGORY_DOCUMENTS,
    ".vssx": CATEGORY_DOCUMENTS,
    ".vst": CATEGORY_DOCUMENTS,
    ".vstx": CATEGORY_DOCUMENTS,
    # Microsoft Project / Access
    ".mpp": CATEGORY_DOCUMENTS,
    ".mppx": CATEGORY_DOCUMENTS,
    ".mdb": CATEGORY_DOCUMENTS,
    ".accdb": CATEGORY_DOCUMENTS,
    # WordPerfect (legacy)
    ".wpd": CATEGORY_DOCUMENTS,
    ".wps": CATEGORY_DOCUMENTS,
    # ---- Photos ----
    # JPEG family
    ".jpg": CATEGORY_PHOTOS,
    ".jpeg": CATEGORY_PHOTOS,
    ".jpe": CATEGORY_PHOTOS,
    ".jfif": CATEGORY_PHOTOS,
    ".jif": CATEGORY_PHOTOS,
    # PNG / GIF / BMP
    ".png": CATEGORY_PHOTOS,
    ".apng": CATEGORY_PHOTOS,
    ".gif": CATEGORY_PHOTOS,
    ".bmp": CATEGORY_PHOTOS,
    ".dib": CATEGORY_PHOTOS,
    # TIFF
    ".tif": CATEGORY_PHOTOS,
    ".tiff": CATEGORY_PHOTOS,
    # Modern web
    ".webp": CATEGORY_PHOTOS,
    ".avif": CATEGORY_PHOTOS,
    ".heic": CATEGORY_PHOTOS,
    ".heif": CATEGORY_PHOTOS,
    ".jxl": CATEGORY_PHOTOS,
    # RAW (all camera makers, displayed as a single "RAW" subgroup
    # in the UI per the design — keep all entries here so a
    # ``categorize(...)`` lookup stays O(1) regardless of make)
    ".dng": CATEGORY_PHOTOS,
    ".cr2": CATEGORY_PHOTOS,
    ".cr3": CATEGORY_PHOTOS,
    ".crw": CATEGORY_PHOTOS,
    ".nef": CATEGORY_PHOTOS,
    ".nrw": CATEGORY_PHOTOS,
    ".arw": CATEGORY_PHOTOS,
    ".srf": CATEGORY_PHOTOS,
    ".sr2": CATEGORY_PHOTOS,
    ".raf": CATEGORY_PHOTOS,
    ".rw2": CATEGORY_PHOTOS,
    ".rwl": CATEGORY_PHOTOS,
    ".orf": CATEGORY_PHOTOS,
    ".pef": CATEGORY_PHOTOS,
    ".ptx": CATEGORY_PHOTOS,
    ".srw": CATEGORY_PHOTOS,
    ".x3f": CATEGORY_PHOTOS,
    ".3fr": CATEGORY_PHOTOS,
    ".fff": CATEGORY_PHOTOS,
    ".iiq": CATEGORY_PHOTOS,
    ".mef": CATEGORY_PHOTOS,
    ".dcr": CATEGORY_PHOTOS,
    ".kdc": CATEGORY_PHOTOS,
    ".k25": CATEGORY_PHOTOS,
    ".erf": CATEGORY_PHOTOS,
    ".mrw": CATEGORY_PHOTOS,
    ".raw": CATEGORY_PHOTOS,
    ".rwz": CATEGORY_PHOTOS,
    # Vector & illustration
    ".svg": CATEGORY_PHOTOS,
    ".svgz": CATEGORY_PHOTOS,
    ".eps": CATEGORY_PHOTOS,
    ".ai": CATEGORY_PHOTOS,
    ".cdr": CATEGORY_PHOTOS,
    # Source design
    ".psd": CATEGORY_PHOTOS,
    ".psb": CATEGORY_PHOTOS,
    ".xcf": CATEGORY_PHOTOS,
    ".sketch": CATEGORY_PHOTOS,
    ".fig": CATEGORY_PHOTOS,
    ".afphoto": CATEGORY_PHOTOS,
    ".afdesign": CATEGORY_PHOTOS,
    # Sidecar (RAW metadata)
    ".xmp": CATEGORY_PHOTOS,
    # Other photo-adjacent
    ".ico": CATEGORY_PHOTOS,
    ".icns": CATEGORY_PHOTOS,
    ".cur": CATEGORY_PHOTOS,
    ".pcx": CATEGORY_PHOTOS,
    ".tga": CATEGORY_PHOTOS,
    ".exr": CATEGORY_PHOTOS,
    ".hdr": CATEGORY_PHOTOS,
    # ---- Videos ----
    # Common containers
    ".mp4": CATEGORY_VIDEOS,
    ".m4v": CATEGORY_VIDEOS,
    ".mov": CATEGORY_VIDEOS,
    ".mkv": CATEGORY_VIDEOS,
    ".avi": CATEGORY_VIDEOS,
    ".webm": CATEGORY_VIDEOS,
    ".flv": CATEGORY_VIDEOS,
    ".f4v": CATEGORY_VIDEOS,
    # MPEG family
    ".mpg": CATEGORY_VIDEOS,
    ".mpeg": CATEGORY_VIDEOS,
    ".mp2": CATEGORY_VIDEOS,
    ".m2v": CATEGORY_VIDEOS,
    ".m2p": CATEGORY_VIDEOS,
    ".mts": CATEGORY_VIDEOS,
    ".m2ts": CATEGORY_VIDEOS,
    # NOTE: ``.ts`` is intentionally NOT registered here as a video
    # extension. It is registered in the Code & data block below
    # (TypeScript) because in a typical user's home tree TypeScript
    # files vastly outnumber HLS .ts segments. See the module
    # docstring for the rationale.
    # Windows Media
    ".wmv": CATEGORY_VIDEOS,
    ".asf": CATEGORY_VIDEOS,
    ".dvr-ms": CATEGORY_VIDEOS,
    # DVD / VOB
    ".vob": CATEGORY_VIDEOS,
    ".ifo": CATEGORY_VIDEOS,
    # Mobile
    ".3gp": CATEGORY_VIDEOS,
    ".3g2": CATEGORY_VIDEOS,
    # Ogg / WebM family
    ".ogv": CATEGORY_VIDEOS,
    ".ogm": CATEGORY_VIDEOS,
    # Pro / RAW vidéo
    ".mxf": CATEGORY_VIDEOS,
    ".braw": CATEGORY_VIDEOS,
    ".r3d": CATEGORY_VIDEOS,
    ".ari": CATEGORY_VIDEOS,
    ".dv": CATEGORY_VIDEOS,
    ".dvr": CATEGORY_VIDEOS,
    ".prores": CATEGORY_VIDEOS,
    # Legacy
    ".rm": CATEGORY_VIDEOS,
    ".rmvb": CATEGORY_VIDEOS,
    ".swf": CATEGORY_VIDEOS,
    # ---- Music ----
    # Lossy compressed
    ".mp3": CATEGORY_MUSIC,
    ".aac": CATEGORY_MUSIC,
    ".m4a": CATEGORY_MUSIC,
    ".m4b": CATEGORY_MUSIC,
    ".ogg": CATEGORY_MUSIC,
    ".oga": CATEGORY_MUSIC,
    ".opus": CATEGORY_MUSIC,
    ".wma": CATEGORY_MUSIC,
    # Lossless
    ".flac": CATEGORY_MUSIC,
    ".alac": CATEGORY_MUSIC,
    ".ape": CATEGORY_MUSIC,
    ".wv": CATEGORY_MUSIC,
    ".tak": CATEGORY_MUSIC,
    ".tta": CATEGORY_MUSIC,
    # Uncompressed
    ".wav": CATEGORY_MUSIC,
    ".wave": CATEGORY_MUSIC,
    ".aiff": CATEGORY_MUSIC,
    ".aif": CATEGORY_MUSIC,
    ".aifc": CATEGORY_MUSIC,
    ".au": CATEGORY_MUSIC,
    # DSD / SACD
    ".dsd": CATEGORY_MUSIC,
    ".dsf": CATEGORY_MUSIC,
    ".dff": CATEGORY_MUSIC,
    # MIDI / partitions
    ".mid": CATEGORY_MUSIC,
    ".midi": CATEGORY_MUSIC,
    ".kar": CATEGORY_MUSIC,
    ".musicxml": CATEGORY_MUSIC,
    ".mxl": CATEGORY_MUSIC,
    # Tracker
    ".mod": CATEGORY_MUSIC,
    ".it": CATEGORY_MUSIC,
    ".s3m": CATEGORY_MUSIC,
    ".xm": CATEGORY_MUSIC,
    ".stm": CATEGORY_MUSIC,
    # Playlists
    ".m3u": CATEGORY_MUSIC,
    ".m3u8": CATEGORY_MUSIC,
    ".pls": CATEGORY_MUSIC,
    ".wpl": CATEGORY_MUSIC,
    ".xspf": CATEGORY_MUSIC,
    ".cue": CATEGORY_MUSIC,
    # DAW projects
    ".als": CATEGORY_MUSIC,
    ".flp": CATEGORY_MUSIC,
    ".logicx": CATEGORY_MUSIC,
    ".cpr": CATEGORY_MUSIC,
    ".rpp": CATEGORY_MUSIC,
    # Audio book / voice
    ".aax": CATEGORY_MUSIC,
    ".aa": CATEGORY_MUSIC,
    ".amr": CATEGORY_MUSIC,
    ".gsm": CATEGORY_MUSIC,
    # ---- Archives ----
    ".zip": CATEGORY_ARCHIVES,
    ".rar": CATEGORY_ARCHIVES,
    ".7z": CATEGORY_ARCHIVES,
    ".tar": CATEGORY_ARCHIVES,
    ".gz": CATEGORY_ARCHIVES,
    ".tgz": CATEGORY_ARCHIVES,
    ".bz2": CATEGORY_ARCHIVES,
    ".xz": CATEGORY_ARCHIVES,
    ".iso": CATEGORY_ARCHIVES,
    # ---- Code & data ----
    ".py": CATEGORY_CODE_DATA,
    ".pyw": CATEGORY_CODE_DATA,
    ".js": CATEGORY_CODE_DATA,
    ".mjs": CATEGORY_CODE_DATA,
    ".cjs": CATEGORY_CODE_DATA,
    ".ts": CATEGORY_CODE_DATA,  # NB: clashes with MPEG ``.ts`` in priority — see note below
    ".tsx": CATEGORY_CODE_DATA,
    ".jsx": CATEGORY_CODE_DATA,
    ".html": CATEGORY_CODE_DATA,
    ".htm": CATEGORY_CODE_DATA,
    ".css": CATEGORY_CODE_DATA,
    ".scss": CATEGORY_CODE_DATA,
    ".sass": CATEGORY_CODE_DATA,
    ".json": CATEGORY_CODE_DATA,
    ".java": CATEGORY_CODE_DATA,
    ".c": CATEGORY_CODE_DATA,
    ".cpp": CATEGORY_CODE_DATA,
    ".cc": CATEGORY_CODE_DATA,
    ".h": CATEGORY_CODE_DATA,
    ".hpp": CATEGORY_CODE_DATA,
    ".cs": CATEGORY_CODE_DATA,
    ".rb": CATEGORY_CODE_DATA,
    ".go": CATEGORY_CODE_DATA,
    ".rs": CATEGORY_CODE_DATA,
    ".php": CATEGORY_CODE_DATA,
    ".sql": CATEGORY_CODE_DATA,
    ".yml": CATEGORY_CODE_DATA,
    ".yaml": CATEGORY_CODE_DATA,
    ".toml": CATEGORY_CODE_DATA,
    ".xml": CATEGORY_CODE_DATA,
    ".md": CATEGORY_CODE_DATA,
    ".sh": CATEGORY_CODE_DATA,
    ".bash": CATEGORY_CODE_DATA,
    ".zsh": CATEGORY_CODE_DATA,
    ".bat": CATEGORY_CODE_DATA,
    ".cmd": CATEGORY_CODE_DATA,
    ".ps1": CATEGORY_CODE_DATA,
    ".ini": CATEGORY_CODE_DATA,
    ".cfg": CATEGORY_CODE_DATA,
    ".conf": CATEGORY_CODE_DATA,
    ".log": CATEGORY_CODE_DATA,
}

# Ambiguous extension note: ``.ts`` is both TypeScript source and an
# MPEG transport stream container. The map above resolves it to
# ``Code & data`` because the file frequency in a typical user's
# home tree skews overwhelmingly that way (a developer with a JS
# project has hundreds of .ts files; a videographer with HLS streams
# has at most a handful and they live in a media folder). If a future
# user hits the wrong bucket they can rename the category mentally
# at the read site — categorization is purely cosmetic, the file is
# still in the skipped list either way.


def categorize(path: str | Path) -> str:
    """Return the category label for ``path`` based on its extension.

    Args:
        path: A filesystem path. Only the extension is consulted —
            the file does not need to exist.

    Returns:
        One of the ``CATEGORY_*`` constants. Paths without a
        recognized extension (or no extension at all, including bare
        directories) return ``CATEGORY_OTHER``.
    """
    if isinstance(path, str):
        # ``Path(path).suffix`` is the cheapest correct way to handle
        # multi-dot names ("foo.tar.gz" → ".gz", which is what we want
        # for categorization). A plain ``rsplit('.', 1)`` would work
        # too but Path also handles trailing separators on directories.
        path = Path(path)
    suffix = path.suffix.lower()
    if not suffix:
        return CATEGORY_OTHER
    return _EXTENSION_TO_CATEGORY.get(suffix, CATEGORY_OTHER)


def extension_of(path: str | Path) -> str:
    """Return the lowercase extension of ``path`` (with leading dot).

    Empty string when the path has no extension. Used by the Run-tab
    Log to sub-group skipped paths under their category by extension
    (Documents → .pdf, .docx, .xlsx, ...).
    """
    if isinstance(path, str):
        path = Path(path)
    return path.suffix.lower()
