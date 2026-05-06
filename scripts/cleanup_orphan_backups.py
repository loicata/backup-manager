"""Best-effort cleanup of orphan backup directories.

Removes leftover backup folders that the legacy rotation logic
silently failed to delete (Windows long-path issues, read-only
attributes, transient locks). Uses ``safe_remove_tree`` from the
storage helpers so long paths and read-only files are handled
correctly.

Usage::

    # List orphan candidates (FULL/DIFF directories without a paired
    # .wbverify manifest):
    python scripts/cleanup_orphan_backups.py <destination>

    # Remove specific entries (the script asks for confirmation):
    python scripts/cleanup_orphan_backups.py <destination> <name> [name ...]

Examples::

    python scripts/cleanup_orphan_backups.py "G:/Backup Manager"
    python scripts/cleanup_orphan_backups.py "G:/Backup Manager" \\
        Backup_Loic_FULL_2026-04-20_100017 \\
        Backup_Loic_FULL_2026-04-21_100007 \\
        Backup_Loic_FULL_2026-04-21_100954
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Allow running as ``python scripts/cleanup_orphan_backups.py`` from
# the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.storage._fs_utils import safe_remove_tree  # noqa: E402

_BACKUP_NAME_RE = re.compile(r"_(?:FULL|DIFF)_\d{4}-\d{2}-\d{2}_\d{6}(?:\.tar\.wbenc)?$")


def find_orphans(destination: Path) -> list[Path]:
    """Return backup directories that have no paired ``.wbverify`` manifest.

    A valid backup leaves both a directory (or .tar.wbenc archive)
    AND a sibling ``<name>.wbverify`` file. A directory without the
    manifest is most likely a partial-delete residue from the legacy
    rotation bug — but the user must confirm before deletion.
    """
    if not destination.is_dir():
        raise NotADirectoryError(f"Not a directory: {destination}")

    orphans: list[Path] = []
    for entry in destination.iterdir():
        if not entry.is_dir():
            continue
        if not _BACKUP_NAME_RE.search(entry.name):
            continue
        manifest = destination / f"{entry.name}.wbverify"
        if not manifest.exists():
            orphans.append(entry)
    return sorted(orphans, key=lambda p: p.name)


def remove_one(target: Path) -> bool:
    """Remove a single backup folder. Returns True on full success."""
    print(f"Removing {target.name} ...", flush=True)
    result = safe_remove_tree(target)
    if result.success:
        print(
            f"  ok — {result.removed_files} file(s), "
            f"{result.removed_dirs} dir(s) removed",
            flush=True,
        )
        return True
    print(
        f"  FAILED — {len(result.residuals)} residual(s) remain:",
        flush=True,
    )
    for r in result.residuals[:5]:
        print(f"    {r}", flush=True)
    if len(result.residuals) > 5:
        print(f"    ... (+{len(result.residuals) - 5} more)", flush=True)
    return False


def confirm(prompt: str) -> bool:
    """Interactive yes/no prompt — defaults to no."""
    answer = input(f"{prompt} [y/N] ").strip().lower()
    return answer in {"y", "yes"}


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2

    destination = Path(argv[1])
    if not destination.exists():
        print(f"Destination does not exist: {destination}", file=sys.stderr)
        return 2

    targets = argv[2:]

    if not targets:
        # List mode: surface candidates and exit
        orphans = find_orphans(destination)
        if not orphans:
            print(f"No orphan backup candidates under {destination}")
            return 0
        print(f"Orphan candidates under {destination}:")
        for o in orphans:
            print(f"  {o.name}")
        print()
        print(
            "Re-run with names to delete, e.g.:\n"
            f"  python {Path(__file__).name} \"{destination}\" "
            f"{orphans[0].name}"
        )
        return 0

    # Removal mode: confirm and delete each named entry
    print(f"About to remove {len(targets)} entries under {destination}:")
    for name in targets:
        print(f"  {name}")
    if not confirm("Proceed?"):
        print("Aborted.")
        return 1

    failed = 0
    for name in targets:
        target = destination / name
        if not target.exists():
            print(f"Skipping {name}: not found", flush=True)
            continue
        if not remove_one(target):
            failed += 1
        # Also remove the paired manifest if the main entry is gone
        manifest = destination / f"{name}.wbverify"
        if manifest.exists():
            if remove_one(manifest):
                pass

    if failed:
        print(f"\nDone with {failed} failure(s).")
        return 1
    print("\nAll requested entries removed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
