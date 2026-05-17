"""Bench three copy strategies on a real workload.

Compares:
- single  : shutil.copy2() sequential — current v3.7.0 baseline.
- pool4   : ThreadPoolExecutor(4) wrapping shutil.copy2().
- pool8   : ThreadPoolExecutor(8) wrapping shutil.copy2().

Why these three: shutil.copy2 already routes to Win32 CopyFileExW
(kernel zero-copy), so per-file throughput is already optimal. The
271 k-file BLoic workload is dominated by per-file overhead
(open/close + NTFS metadata, ~5-10 ms each) which parallelises well
even on a single HDD spindle because the I/O queue keeps the head
busy while the CPU handles metadata for the next file.

Cache eviction between runs: Windows unified Memory Manager caches
file data aggressively. Without eviction, runs 2 and 3 read source
files from RAM and look 5-10x faster than they really are. Before
each run we write a 20 GB dummy file (configurable) on the
destination drive to push everything out of standby cache. The
copy phase that follows reads sources fresh from disk.

Usage:
    .venv/Scripts/python.exe scripts/bench_copy_strategies.py \\
        --source F:\\Documents\\Divers \\
        --dest-root E:\\BMtest \\
        --max-bytes 10737418240

Output: JSON results in bench_results_<timestamp>.json + console table.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class BenchResult:
    """Single-mode benchmark outcome."""

    mode: str
    files: int
    bytes_total: int
    elapsed_seconds: float
    rate_mb_s: float
    evict_seconds: float


def evict_cache(drive_path: Path, size_gb: int) -> float:
    """Saturate OS file cache by writing a dummy file.

    Windows Memory Manager uses a unified standby list across all
    volumes — writing N GB on the destination volume pushes N GB out
    of cache regardless of which volume held it.

    Args:
        drive_path: Any writable path on the volume to use for the dummy.
        size_gb: Total bytes to write (in gibibytes).

    Returns:
        Wall-clock seconds spent in the eviction phase.
    """
    dummy = drive_path / "_bench_cache_evict.tmp"
    if dummy.exists():
        dummy.unlink()

    block = b"\x00" * (1024 * 1024)  # 1 MiB
    blocks_total = size_gb * 1024

    t0 = time.perf_counter()
    try:
        with open(dummy, "wb", buffering=0) as f:
            for _ in range(blocks_total):
                f.write(block)
            f.flush()
            os.fsync(f.fileno())
    finally:
        elapsed = time.perf_counter() - t0
        if dummy.exists():
            dummy.unlink()
    return elapsed


def collect_files(source: Path, max_bytes: int) -> tuple[list[Path], int]:
    """Walk source and collect files until total size reaches max_bytes.

    Returns:
        (file_list, total_bytes)
    """
    files: list[Path] = []
    total = 0
    for f in source.rglob("*"):
        try:
            if not f.is_file():
                continue
            sz = f.stat().st_size
        except OSError:
            # Skip files that disappear mid-walk or refuse stat
            continue
        files.append(f)
        total += sz
        if total >= max_bytes:
            break
    return files, total


def copy_one(pair: tuple[Path, Path]) -> None:
    """Standard shutil.copy2 for a single (src, dst) pair."""
    src, dst = pair
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def run_single(pairs: list[tuple[Path, Path]]) -> None:
    """Sequential baseline."""
    for p in pairs:
        copy_one(p)


def run_pool(pairs: list[tuple[Path, Path]], workers: int) -> None:
    """ThreadPoolExecutor wrapping copy_one."""
    with ThreadPoolExecutor(max_workers=workers) as ex:
        # list() drains the iterator so exceptions surface here.
        list(ex.map(copy_one, pairs))


MODES = (
    ("single", lambda pairs: run_single(pairs)),
    ("pool4", lambda pairs: run_pool(pairs, 4)),
    ("pool8", lambda pairs: run_pool(pairs, 8)),
)


def run_bench(
    source: Path,
    dest_root: Path,
    max_bytes: int,
    evict_gb: int,
) -> list[BenchResult]:
    """Run all three modes back to back, with cache eviction between."""
    print(f"Source     : {source}")
    print(f"Dest root  : {dest_root}")
    print(f"Budget     : {max_bytes / 1024**3:.2f} GB")
    print(f"Evict size : {evict_gb} GB (between runs)")
    print()

    print("Collecting source file list...")
    files, total_bytes = collect_files(source, max_bytes)
    print(f"  {len(files):,} files, {total_bytes / 1024**3:.2f} GB")
    print()

    results: list[BenchResult] = []
    dest_root.mkdir(parents=True, exist_ok=True)

    for mode_name, runner in MODES:
        # Fresh destination per mode so previous mode does not skew the
        # next via partial metadata that NTFS may cache.
        dst_mode = dest_root / mode_name
        if dst_mode.exists():
            print(f"[{mode_name}] Cleaning existing {dst_mode}...")
            shutil.rmtree(dst_mode)
        dst_mode.mkdir(parents=True)

        # Build (src, dst) pairs preserving relative tree structure
        pairs = [(f, dst_mode / f.relative_to(source)) for f in files]

        # Evict OS cache so this run reads sources fresh from disk
        print(f"[{mode_name}] Evicting OS cache ({evict_gb} GB)...")
        ev_dur = evict_cache(dest_root, evict_gb)
        print(f"  eviction: {ev_dur:.1f} s")

        # Run the copy phase
        print(f"[{mode_name}] Copying {len(pairs):,} files...")
        t0 = time.perf_counter()
        try:
            runner(pairs)
        except Exception as e:  # noqa: BLE001 — print, then continue with other modes
            print(f"  ERROR in {mode_name}: {type(e).__name__}: {e}")
            elapsed = float("nan")
            rate = 0.0
        else:
            elapsed = time.perf_counter() - t0
            rate = (total_bytes / elapsed) / (1024**2)

        print(f"[{mode_name}] {elapsed:.1f} s — {rate:.1f} MB/s")
        print()
        results.append(
            BenchResult(
                mode=mode_name,
                files=len(files),
                bytes_total=total_bytes,
                elapsed_seconds=elapsed,
                rate_mb_s=rate,
                evict_seconds=ev_dur,
            )
        )

    return results


def print_summary(results: list[BenchResult]) -> None:
    """Render the comparison table on stdout."""
    print("=" * 70)
    print(
        f"{'Mode':<10} {'Files':<10} {'Elapsed (s)':<14} "
        f"{'Rate (MB/s)':<14} {'Speedup':<10}"
    )
    print("-" * 70)
    if not results:
        print("(no results)")
        return
    baseline = results[0].elapsed_seconds
    for r in results:
        if baseline > 0 and r.elapsed_seconds > 0:
            speedup = baseline / r.elapsed_seconds
            speedup_s = f"{speedup:.2f}x"
        else:
            speedup_s = "—"
        print(
            f"{r.mode:<10} {r.files:<10,} {r.elapsed_seconds:<14.1f} "
            f"{r.rate_mb_s:<14.1f} {speedup_s:<10}"
        )
    print("=" * 70)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", required=True, type=Path, help="Source directory")
    ap.add_argument("--dest-root", required=True, type=Path, help="Destination root")
    ap.add_argument(
        "--max-bytes",
        default=10 * 1024**3,
        type=int,
        help="File budget in bytes (default 10 GB)",
    )
    ap.add_argument(
        "--evict-gb",
        default=20,
        type=int,
        help="Cache-eviction dummy size in GB (default 20)",
    )
    args = ap.parse_args()

    if not args.source.is_dir():
        print(f"ERROR: --source {args.source} is not a directory", file=sys.stderr)
        return 2

    args.dest_root.mkdir(parents=True, exist_ok=True)

    start = datetime.now()
    results = run_bench(args.source, args.dest_root, args.max_bytes, args.evict_gb)
    end = datetime.now()

    print_summary(results)

    # Persist results so the user can compare across days
    out_file = Path(
        f"bench_results_{start.strftime('%Y%m%d_%H%M%S')}.json"
    )
    payload = {
        "started_at": start.isoformat(),
        "ended_at": end.isoformat(),
        "source": str(args.source),
        "dest_root": str(args.dest_root),
        "max_bytes": args.max_bytes,
        "evict_gb": args.evict_gb,
        "modes": [asdict(r) for r in results],
    }
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nResults persisted to {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
