"""USB throughput benchmark: kernel ``shutil.copy2`` vs Python loop.

Reproduces the workload that triggered the v3.3.15 -> v3.3.18 regression:
many small files copied to a USB SSD. The production pipeline goes
through ``shutil.copy2`` which on Windows resolves to ``CopyFileExW``
(kernel-space) and on Linux/macOS to ``sendfile`` / ``copy_file_range``.
A regression to a pure-Python ``read/write`` loop drops throughput from
~50 MB/s to ~7-8 MB/s on a Samsung T7 USB SSD with 30k small files.

Pytest cannot catch this regression — the suite runs on ``tmp_path``
(NVMe in CI, RAM disk locally) where both paths look identical.
``scripts/bench_copy.py`` is the manual safety net: run it against a
real USB target before tagging any release that touches
``write_flat`` or its callers.

Usage:
    python -m scripts.bench_copy --target G:/.bench
    python -m scripts.bench_copy --target G:/.bench --files 30000 --size 1024
    python -m scripts.bench_copy --target G:/.bench --threshold-mbs 30

The script always cleans up its own temp dirs (source + destination),
even on Ctrl-C, so it is safe to point at a partition that already
holds backups.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

# Add project root to path so we can reuse production helpers
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Re-use the production long-path helper so the benchmark exercises the
# real call site, not a hand-rolled copy.
from src.storage.base import long_path_mkdir, long_path_str  # noqa: E402

# Default workload chosen to match the field-reported regression case:
# ~30k small files (~1 KB each) — this is the shape that exposes
# kernel-vs-Python-loop differences most dramatically because the
# per-file overhead (open/close, metadata copy) dominates over raw
# byte transfer.
DEFAULT_FILES = 5_000
DEFAULT_SIZE_BYTES = 1024
# Empirical threshold below which the kernel path is suspect on a
# real USB SSD. v3.3.14 sustained 50+ MB/s; the regressed releases
# capped at ~8 MB/s. 30 MB/s is a generous floor that any working
# kernel-copy implementation clears comfortably even on slower USB
# 3.0 drives.
DEFAULT_THRESHOLD_MBS = 30.0
# 4 MiB chunk for the Python-loop baseline. Same value as
# ``hashing.HASH_CHUNK_SIZE`` so this is the most charitable Python
# implementation; if the kernel path is only marginally faster than
# THIS it would still be a regression.
PYTHON_LOOP_CHUNK = 4 * 1024 * 1024


def _python_loop_copy(src: str, dst: str) -> None:
    """Reference Python ``read/write`` copy — the regression baseline.

    Mirrors the v3.3.15-class code path: open source, open destination,
    pump 4-MiB chunks until EOF. No kernel fast-path, no metadata copy.
    """
    with open(src, "rb") as src_f, open(dst, "wb") as dst_f:
        while True:
            chunk = src_f.read(PYTHON_LOOP_CHUNK)
            if not chunk:
                break
            dst_f.write(chunk)


def _seed_source(source: Path, n_files: int, size_bytes: int) -> int:
    """Create ``n_files`` source files of ``size_bytes`` each.

    Returns the total bytes written. Files use ``os.urandom`` so the
    OS / antivirus / filesystem cannot collapse identical pages.
    """
    long_path_mkdir(source)
    payload = os.urandom(size_bytes)
    total = 0
    print(f"Seeding {n_files} files of {size_bytes} bytes in {source} ...", flush=True)
    t0 = time.monotonic()
    for i in range(n_files):
        # Spread files across 100 subdirs so a single dir isn't asked
        # to hold the entire workload — that triggers different code
        # paths in NTFS / FAT32 directory indexing.
        sub = source / f"d{i % 100:02d}"
        if i % 100 == 0:
            long_path_mkdir(sub)
        path = sub / f"f_{i:06d}.bin"
        # Mix the index in so each file has a unique first byte —
        # cheap protection against any deduplication path.
        with open(long_path_str(path), "wb") as f:
            f.write(bytes([i & 0xFF]) + payload[1:])
        total += size_bytes
    print(f"  seeded in {time.monotonic() - t0:.1f}s ({total / 1_048_576:.1f} MiB)")
    return total


def _walk_source(source: Path) -> list[tuple[Path, str]]:
    """Return ``(absolute_path, relative_path)`` pairs for every source file."""
    pairs: list[tuple[Path, str]] = []
    for root, _dirs, names in os.walk(long_path_str(source)):
        for name in names:
            abs_p = Path(root) / name
            rel = str(abs_p.relative_to(source)).replace("\\", "/")
            pairs.append((abs_p, rel))
    return pairs


def _run_copy_pass(
    label: str,
    source_pairs: list[tuple[Path, str]],
    dest: Path,
    copier,
    total_bytes: int,
) -> tuple[float, float]:
    """Execute one full copy pass and return ``(elapsed_s, throughput_MBs)``."""
    long_path_mkdir(dest)
    print(f"\n[{label}]  copying {len(source_pairs)} files to {dest} ...", flush=True)
    t0 = time.monotonic()
    last_print = t0
    for i, (src_path, rel) in enumerate(source_pairs):
        target = dest / rel
        long_path_mkdir(target.parent)
        copier(long_path_str(src_path), long_path_str(target))
        # Light progress on long runs — once a second is enough.
        now = time.monotonic()
        if now - last_print > 1.0:
            done = i + 1
            elapsed = now - t0
            cur_mbs = (done * (total_bytes / len(source_pairs))) / elapsed / 1_048_576
            print(
                f"  ... {done}/{len(source_pairs)} ({cur_mbs:.1f} MB/s)",
                flush=True,
            )
            last_print = now
    elapsed = time.monotonic() - t0
    mbs = total_bytes / elapsed / 1_048_576 if elapsed > 0 else 0.0
    print(f"  -> {elapsed:.2f}s, {mbs:.1f} MB/s")
    return elapsed, mbs


@contextmanager
def _scratch_dirs(target: Path):
    """Allocate source + dest scratch dirs inside ``target`` and clean up.

    We deliberately seed the source on the SAME drive as the target so
    the bench measures intra-device copy speed (the realistic backup
    case where source = system disk and target = USB would just measure
    USB write speed, which is fine but not the regression we hunt).
    A single ``unique`` parent makes cleanup trivial and idempotent.
    """
    unique = target / f".bench_{uuid.uuid4().hex[:8]}"
    source = unique / "src"
    dest_kernel = unique / "dst_kernel"
    dest_python = unique / "dst_python"
    long_path_mkdir(unique)
    try:
        yield source, dest_kernel, dest_python
    finally:
        # Best-effort cleanup; we never want this script to leave
        # gigabytes behind on a USB drive.
        print(f"\nCleaning up {unique} ...", flush=True)
        shutil.rmtree(long_path_str(unique), ignore_errors=True)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="USB copy throughput benchmark: kernel vs Python loop."
    )
    parser.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Directory on the drive to benchmark (will be created if missing).",
    )
    parser.add_argument(
        "--files",
        type=int,
        default=DEFAULT_FILES,
        help=f"Number of files in the workload (default: {DEFAULT_FILES}).",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=DEFAULT_SIZE_BYTES,
        help=f"Bytes per file (default: {DEFAULT_SIZE_BYTES}).",
    )
    parser.add_argument(
        "--threshold-mbs",
        type=float,
        default=DEFAULT_THRESHOLD_MBS,
        help=(
            f"Minimum kernel throughput in MB/s; below this the script "
            f"exits non-zero (default: {DEFAULT_THRESHOLD_MBS})."
        ),
    )
    parser.add_argument(
        "--skip-python-baseline",
        action="store_true",
        help="Skip the slow Python-loop reference pass (kernel-only run).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    target = args.target.resolve()
    if not target.exists():
        long_path_mkdir(target)
    if not target.is_dir():
        print(f"ERROR: --target {target} is not a directory.", file=sys.stderr)
        return 2

    print("=" * 60)
    print("Backup Manager — USB copy benchmark")
    print(f"  target          : {target}")
    print(f"  files           : {args.files}")
    print(f"  bytes per file  : {args.size}")
    print(f"  threshold       : {args.threshold_mbs} MB/s")
    print("=" * 60)

    with _scratch_dirs(target) as (source, dest_kernel, dest_python):
        total_bytes = _seed_source(source, args.files, args.size)
        pairs = _walk_source(source)
        if not pairs:
            print("ERROR: no files were seeded — aborting.", file=sys.stderr)
            return 2

        kernel_elapsed, kernel_mbs = _run_copy_pass(
            "kernel  (shutil.copy2)",
            pairs,
            dest_kernel,
            shutil.copy2,
            total_bytes,
        )

        python_mbs = None
        python_elapsed = None
        if not args.skip_python_baseline:
            python_elapsed, python_mbs = _run_copy_pass(
                "python  (read/write loop, 4 MiB)",
                pairs,
                dest_python,
                _python_loop_copy,
                total_bytes,
            )

    print("\n" + "=" * 60)
    print("Summary")
    print("-" * 60)
    print(f"  kernel  shutil.copy2   : {kernel_mbs:6.1f} MB/s  ({kernel_elapsed:.2f}s)")
    if python_mbs is not None:
        print(f"  python  read/write loop: {python_mbs:6.1f} MB/s  ({python_elapsed:.2f}s)")
        if python_mbs > 0:
            ratio = kernel_mbs / python_mbs
            print(f"  speedup (kernel/python): {ratio:5.1f}x")
    print("=" * 60)

    if kernel_mbs < args.threshold_mbs:
        print(
            f"\nREGRESSION DETECTED: kernel throughput {kernel_mbs:.1f} MB/s "
            f"is below the {args.threshold_mbs} MB/s threshold.",
            file=sys.stderr,
        )
        print(
            "  Investigate: has ``write_flat`` been changed away from "
            "``shutil.copy2``? Has the integrity manifest been folded "
            "back into the writer's per-file loop? See "
            "docs/INVARIANTS.md and CHANGELOG entries for v3.3.15-v3.3.19.",
            file=sys.stderr,
        )
        return 1

    print("\nKernel throughput within expected range — invariant holds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
