"""Compact a bloated run_history JSONL by dropping giant skip-list blobs.

The collector writes one 'Skipped N file(s)' event per run whose
``details`` embeds every skipped path (tens of MB). Repeated across a
retry/crash-recovery storm this grew the 'Loic15062026' history to
722 MB. This tool streams the file, replaces any oversized line with a
compact placeholder, and atomically rewrites it (``.tmp`` + os.replace).
Only the target JSONL is modified.

Run: .venv/Scripts/python.exe scripts/compact-run-history.py [profile_id]
"""

from __future__ import annotations

import os
import sys

# Normal run-history events are well under 1 KB. The collector's
# skip-list blob is megabytes. Anything above this threshold is the
# blob and gets replaced by a one-line placeholder.
_BIG_LINE_CHARS = 100_000

_PLACEHOLDER = (
    '{"ts":"","msg":"[run-history compacted: oversized skip-list removed]",'
    '"level":"info","phase":"collector"}\n'
)


def compact(path: str) -> None:
    """Stream-rewrite ``path``, stripping oversized lines."""
    if not os.path.isfile(path):
        print(f"not found: {path}")
        return

    before = os.path.getsize(path)
    tmp = path + ".compact.tmp"
    kept = 0
    stripped = 0

    with (
        open(path, encoding="utf-8", errors="replace") as fin,
        open(tmp, "w", encoding="utf-8") as fout,
    ):
        for line in fin:
            if len(line) > _BIG_LINE_CHARS:
                fout.write(_PLACEHOLDER)
                stripped += 1
            else:
                if not line.endswith("\n"):
                    line += "\n"
                fout.write(line)
                kept += 1

    os.replace(tmp, path)
    after = os.path.getsize(path)
    print(f"compacted {path}")
    print(
        f"  {before:,} -> {after:,} bytes  "
        f"({before / 1e6:.1f} MB -> {after / 1e6:.1f} MB)"
    )
    print(f"  kept {kept} normal lines, stripped {stripped} oversized blob(s)")


def main() -> None:
    profile_id = sys.argv[1] if len(sys.argv) > 1 else "912c3f02b8c843c78a374681a178b93a"
    appdata = os.environ.get("APPDATA", "")
    path = os.path.join(appdata, "BackupManager", "run_history", f"{profile_id}.jsonl")
    compact(path)


if __name__ == "__main__":
    main()
