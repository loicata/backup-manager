"""Read-only diagnostic for the [Errno 22] backup failures (2026-06-16).

Walks the forensic-dump tree that aborts the 'Loic15062026' backup and
counts, per errno, how many files the manifest's open()-to-hash step
would choke on. Every probed path is on C: (NOT the failing F: drive).

Run: .venv/Scripts/python.exe scripts/diagnose-forensic-errno22.py
"""

from __future__ import annotations

import os
from collections import Counter

FORENSIC_ROOT = (
    r"C:\Users\loica\Documents\Documents Loic\Cloughjordan Ecovillage"
    r"\Vine\VINE_RaspberryPi5_SDcard-Python\forensic\[root]"
)

_MAX_LISTED = 25


def _long(p: str) -> str:
    """Mirror hashing._long_path (Windows long-path prefix)."""
    if os.name == "nt" and not p.startswith("\\\\?\\"):
        return "\\\\?\\" + p
    return p


def walk_and_probe(root: str) -> None:
    """os.walk the tree, open() every file as compute_sha256 would."""
    total = 0
    ok = 0
    errno_counts: Counter[int | None] = Counter()
    listed = 0
    walk_errors = 0

    def on_walk_error(err: OSError) -> None:
        nonlocal walk_errors
        walk_errors += 1
        if walk_errors <= 10:
            print(f"  WALK ERR errno={err.errno}: {err!r}")

    for dirpath, _dirs, files in os.walk(_long(root), onerror=on_walk_error):
        for name in files:
            total += 1
            full = os.path.join(dirpath, name)
            try:
                with open(full, "rb") as f:
                    f.read(1)
                ok += 1
            except OSError as e:
                errno_counts[e.errno] += 1
                if listed < _MAX_LISTED:
                    listed += 1
                    # Strip the long prefix + root for readable output.
                    short = full.replace(_long(root), "").lstrip("\\")
                    print(f"  BAD errno={e.errno}: {short}  ({e.strerror})")

    print("\n--- summary ---")
    print(f"  files seen        : {total}")
    print(f"  readable (open ok): {ok}")
    print(f"  unreadable        : {sum(errno_counts.values())}")
    print(f"  walk errors       : {walk_errors}")
    for errno_val, count in errno_counts.most_common():
        print(f"    errno {errno_val}: {count}")


def main() -> None:
    print(f"forensic root exists: {os.path.isdir(_long(FORENSIC_ROOT))}")
    print(f"scanning: {FORENSIC_ROOT}\n")
    walk_and_probe(FORENSIC_ROOT)


if __name__ == "__main__":
    main()
