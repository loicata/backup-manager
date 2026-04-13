"""S3 bandwidth benchmark: compare throughput at different sample sizes.

Uploads test files of 32 MB and 64 MB to the S3 bucket configured in the
first S3 profile found.  Verifies Object Lock retention before writing.

Usage:
    python -m scripts.s3_bandwidth_bench
"""

import io
import os
import sys
import time
import uuid

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import ConfigManager, StorageType
from src.storage.s3 import S3Storage

TEMP_PREFIX = ".bm_speedtest_"
SAMPLE_SIZES_MB = [128, 256]


def find_s3_profile():
    """Find the first S3 profile in ConfigManager."""
    cm = ConfigManager()
    profiles = cm.get_all_profiles()
    for p in profiles:
        if p.storage.storage_type == StorageType.S3:
            return p
    return None


def check_object_lock(backend: S3Storage, bucket: str) -> dict | None:
    """Check Object Lock configuration on the bucket.

    Returns:
        Lock config dict or None if not enabled.
    """
    client = backend._get_client()
    try:
        resp = client.get_object_lock_configuration(Bucket=bucket)
        return resp.get("ObjectLockConfiguration", {})
    except client.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ObjectLockConfigurationNotFoundError":
            return None
        raise


def write_sample(backend: S3Storage, size: int) -> float:
    """Upload a sample file and return speed in bytes/sec."""
    temp_name = f"{TEMP_PREFIX}{uuid.uuid4().hex[:8]}_{size}"
    data = os.urandom(size)

    start = time.monotonic()
    backend.upload_file(io.BytesIO(data), temp_name, size=size)
    elapsed = time.monotonic() - start

    if elapsed <= 0:
        return 0.0
    return size / elapsed


def main():
    # 1. Find S3 profile
    profile = find_s3_profile()
    if profile is None:
        print("ERROR: No S3 profile found.")
        sys.exit(1)

    s = profile.storage
    print(f"Profile: {profile.name}")
    print(f"Bucket:  {s.s3_bucket}")
    print(f"Region:  {s.s3_region}")
    print(f"Prefix:  {s.s3_prefix or '(none)'}")
    print()

    # 2. Create backend
    backend = S3Storage(
        bucket=s.s3_bucket,
        prefix=s.s3_prefix,
        region=s.s3_region,
        access_key=s.s3_access_key,
        secret_key=s.s3_secret_key,
        endpoint_url=s.s3_endpoint_url,
        provider=s.s3_provider,
    )

    # 3. Test connection
    ok, msg = backend.test_connection()
    if not ok:
        print(f"ERROR: Connection failed: {msg}")
        sys.exit(1)
    print(f"Connection OK: {msg}")
    print()

    # 4. Check Object Lock
    lock_config = check_object_lock(backend, s.s3_bucket)
    if lock_config:
        rule = lock_config.get("Rule", {})
        retention = rule.get("DefaultRetention", {})
        mode = retention.get("Mode", "?")
        days = retention.get("Days", "?")
        print(f"Object Lock: {mode}, retention = {days} days")

        if isinstance(days, int) and days > 31:
            print(f"ERROR: Retention is {days} days (> 31). Aborting.")
            sys.exit(1)
    else:
        print("Object Lock: not enabled")

    # 5. Confirm
    total_mb = sum(SAMPLE_SIZES_MB)
    print()
    print(f"Will upload {total_mb} MB of test data.")
    if lock_config:
        print(f"WARNING: Files will be LOCKED for {days} days (not deletable).")
    if "--yes" not in sys.argv:
        answer = input("Continue? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            sys.exit(0)

    print()
    print("=" * 60)
    print(f"{'Size':>8}  {'Speed (MB/s)':>12}  {'Time (s)':>10}")
    print("-" * 60)

    # 6. Run tests
    results = []
    for size_mb in SAMPLE_SIZES_MB:
        size_bytes = size_mb * 1024 * 1024
        try:
            speed_bps = write_sample(backend, size_bytes)
            speed_mbps = speed_bps / (1024 * 1024)
            elapsed = size_bytes / speed_bps if speed_bps > 0 else 0
            print(f"{size_mb:>6} MB  {speed_mbps:>12.2f}  {elapsed:>10.1f}")
            results.append((size_mb, speed_mbps, elapsed))
        except Exception as e:
            print(f"{size_mb:>6} MB  {'FAILED':>12}  {str(e)}")
            results.append((size_mb, 0, 0))

    print("=" * 60)
    print()

    # 7. Summary
    if len(results) >= 2 and results[0][1] > 0 and results[1][1] > 0:
        diff_pct = ((results[1][1] - results[0][1]) / results[0][1]) * 100
        print(f"Difference: {diff_pct:+.1f}% between {results[0][0]} MB and {results[1][0]} MB")

    print()
    print("Done. Test files remain on S3 (prefix: .bm_speedtest_)")


if __name__ == "__main__":
    main()
