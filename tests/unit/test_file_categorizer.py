"""Tests for ``file_categorizer.categorize`` and ``extension_of``.

The categorizer maps file paths to user-friendly categories
(Documents / Photos / Videos / Music / Archives / Code & data / Other)
based purely on the extension. Used by the Run-tab Log widget to
group skipped paths under category headings the user thinks in
("did my photos get backed up?") rather than by-reason headings.

Tests pin the mapping for representative files in every category,
verify that unknown / extensionless paths fall into ``Other``, and
guard the ambiguous ``.ts`` choice (TypeScript vs MPEG transport
stream) against accidental flips.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.core.file_categorizer import (
    CATEGORY_ARCHIVES,
    CATEGORY_CODE_DATA,
    CATEGORY_DOCUMENTS,
    CATEGORY_MUSIC,
    CATEGORY_ORDER,
    CATEGORY_OTHER,
    CATEGORY_PHOTOS,
    CATEGORY_VIDEOS,
    categorize,
    extension_of,
)


class TestDocuments:
    """Office, PDF, plain text, notes."""

    @pytest.mark.parametrize(
        "ext",
        [
            ".docx", ".doc", ".odt", ".pages", ".pdf", ".epub", ".mobi",
            ".txt", ".rtf", ".markdown", ".csv", ".tsv",
            ".xlsx", ".xls", ".ods", ".numbers",
            ".pptx", ".ppt", ".key", ".odp",
            ".vsdx", ".pub", ".mpp", ".mdb",
        ],
    )
    def test_extension_routes_to_documents(self, ext: str) -> None:
        assert categorize(f"any/path/file{ext}") == CATEGORY_DOCUMENTS


class TestPhotos:
    """Raster, vector, RAW (all camera makers), source design."""

    @pytest.mark.parametrize(
        "ext",
        [
            ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff",
            ".webp", ".avif", ".heic", ".heif", ".jxl",
            ".svg", ".eps", ".ai", ".psd", ".xcf",
        ],
    )
    def test_common_image_formats(self, ext: str) -> None:
        assert categorize(f"any/photo{ext}") == CATEGORY_PHOTOS

    @pytest.mark.parametrize(
        "ext",
        [
            # Canon
            ".cr2", ".cr3", ".crw",
            # Nikon
            ".nef", ".nrw",
            # Sony
            ".arw", ".srf", ".sr2",
            # Adobe (universal)
            ".dng",
            # Other makers (Fuji, Olympus, Pentax, Hasselblad, Phase One...)
            ".raf", ".orf", ".pef", ".rw2", ".x3f", ".3fr", ".iiq", ".srw",
        ],
    )
    def test_raw_formats_all_map_to_photos(self, ext: str) -> None:
        """RAW from any maker goes to Photos — the UI then sub-groups
        them under a single 'RAW' subgroup per the design."""
        assert categorize(f"DCIM/IMG_1234{ext}") == CATEGORY_PHOTOS

    def test_xmp_sidecar_lives_in_photos(self) -> None:
        """XMP is metadata for a RAW; user expects it next to its photo."""
        assert categorize("photo.xmp") == CATEGORY_PHOTOS


class TestVideos:
    """Containers + MPEG family + pro RAW vidéo."""

    @pytest.mark.parametrize(
        "ext",
        [
            ".mp4", ".mov", ".mkv", ".avi", ".webm", ".wmv",
            ".mpg", ".mpeg", ".m2ts", ".mts",
            ".vob", ".3gp", ".ogv",
            ".mxf", ".braw", ".r3d", ".ari",
        ],
    )
    def test_common_video_formats(self, ext: str) -> None:
        assert categorize(f"any/video{ext}") == CATEGORY_VIDEOS


class TestMusic:
    """Lossy + lossless + DSD + MIDI + DAW projects."""

    @pytest.mark.parametrize(
        "ext",
        [
            # Lossy
            ".mp3", ".aac", ".m4a", ".ogg", ".opus", ".wma",
            # Lossless
            ".flac", ".alac", ".ape",
            # Uncompressed
            ".wav", ".aiff",
            # DSD / MIDI
            ".dsd", ".mid", ".midi",
            # Tracker
            ".mod", ".it", ".s3m",
            # Playlists
            ".m3u", ".m3u8", ".pls",
            # DAW projects
            ".als", ".flp", ".cpr", ".rpp",
            # Audio book
            ".aax",
        ],
    )
    def test_common_audio_formats(self, ext: str) -> None:
        assert categorize(f"any/song{ext}") == CATEGORY_MUSIC


class TestArchives:
    @pytest.mark.parametrize(
        "ext", [".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".iso"]
    )
    def test_archive_formats(self, ext: str) -> None:
        assert categorize(f"backup{ext}") == CATEGORY_ARCHIVES


class TestCodeData:
    @pytest.mark.parametrize(
        "ext",
        [
            ".py", ".js", ".tsx", ".java", ".c", ".cpp", ".cs", ".go", ".rs",
            ".html", ".css", ".json", ".yml", ".toml", ".xml", ".md",
            ".sh", ".bat", ".ps1",
        ],
    )
    def test_source_files_route_to_code_data(self, ext: str) -> None:
        assert categorize(f"src/file{ext}") == CATEGORY_CODE_DATA

    def test_ts_resolves_to_code_data(self) -> None:
        """``.ts`` is ambiguous (TypeScript vs MPEG transport stream).

        We pin it to ``Code & data`` because in a typical user's home
        tree the TypeScript files vastly outnumber the rare HLS .ts
        segments. A user who really has an MPEG .ts in their video
        folder will find it under 'Code & data' — a minor surprise,
        but the file is still listed and findable via the search box.
        """
        assert categorize("project/app.ts") == CATEGORY_CODE_DATA


class TestOtherFallback:
    """Unknown extensions or extensionless paths fall into Other."""

    def test_unknown_extension(self) -> None:
        assert categorize("file.xyz_unknown") == CATEGORY_OTHER

    def test_no_extension(self) -> None:
        assert categorize("Makefile") == CATEGORY_OTHER

    def test_directory_path(self) -> None:
        # Bare directories (e.g. ``node_modules`` recorded as a single
        # excluded entry) have no suffix and fall into Other.
        assert categorize("/path/to/some_directory") == CATEGORY_OTHER

    def test_path_with_dot_but_no_extension(self) -> None:
        # ``.bashrc`` has no real extension — Path.suffix returns "".
        assert categorize(".bashrc") == CATEGORY_OTHER


class TestCaseInsensitive:
    """Extension matching is case-insensitive (Windows produces .JPG)."""

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("PHOTO.JPG", CATEGORY_PHOTOS),
            ("Doc.PDF", CATEGORY_DOCUMENTS),
            ("Song.MP3", CATEGORY_MUSIC),
            ("Movie.MOV", CATEGORY_VIDEOS),
        ],
    )
    def test_uppercase_extensions(self, path: str, expected: str) -> None:
        assert categorize(path) == expected


class TestAcceptsPathObject:
    """``categorize`` accepts both str and Path."""

    def test_str_input(self) -> None:
        assert categorize("photo.jpg") == CATEGORY_PHOTOS

    def test_path_input(self) -> None:
        assert categorize(Path("photo.jpg")) == CATEGORY_PHOTOS

    def test_windows_path_str(self) -> None:
        assert categorize(r"C:\Users\loica\Photos\IMG_1023.cr2") == CATEGORY_PHOTOS


class TestExtensionOf:
    """``extension_of`` returns the lowercase suffix for sub-grouping."""

    def test_normal_extension(self) -> None:
        assert extension_of("photo.JPG") == ".jpg"

    def test_double_extension_keeps_only_last(self) -> None:
        # ``foo.tar.gz`` has Path.suffix == ".gz" — the right behaviour
        # for our use (group with other .gz, not with imaginary .tar.gz).
        assert extension_of("backup.tar.gz") == ".gz"

    def test_no_extension_returns_empty(self) -> None:
        assert extension_of("Makefile") == ""


class TestCategoryOrder:
    """Display order is documented and stable."""

    def test_order_starts_with_user_relevant_categories(self) -> None:
        # Documents / Photos / Videos / Music come first because that
        # is the typical user mental model.
        assert CATEGORY_ORDER[:4] == (
            CATEGORY_DOCUMENTS,
            CATEGORY_PHOTOS,
            CATEGORY_VIDEOS,
            CATEGORY_MUSIC,
        )

    def test_other_is_last(self) -> None:
        assert CATEGORY_ORDER[-1] == CATEGORY_OTHER

    def test_all_categories_unique(self) -> None:
        assert len(set(CATEGORY_ORDER)) == len(CATEGORY_ORDER)
